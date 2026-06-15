package rating

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	// pgx stdlib driver registers itself as "pgx" with database/sql. Same choice as
	// internal/drain: standard database/sql so the store is a thin, mockable seam
	// (sqlmock) and pool tuning is the familiar API.
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Store is rating's data seam. It is an interface so the rater orchestration can be
// unit-tested against a fake and the Postgres SQL tested in isolation via sqlmock.
//
// The contract is YAML-priced and money-in-SQL (E1): there is no price TABLE. The
// caller loads the price file into a PriceBook and passes it to RateWindow; the
// store PROJECTS the book's already-premium-applied per-token rates into a
// transient (TEMP) price table for the window, then rates the whole window in SQL —
// resolves the effective rate, computes per-event cost, sums it per (auth_id,
// model_id, hour) into rated_usage idempotently, AND counts the fail-loud anomalies,
// all in one snapshot so the rollups and the anomaly counts always agree on what
// "priced" means.
type Store interface {
	// RateWindow rates [start, end) against the prices in book. book carries the
	// FINAL per-token rates (the global fine-tune premium already applied in exact
	// Dec); the store binds them as NUMERIC and does all cost MULTIPLY-and-SUM in SQL.
	RateWindow(ctx context.Context, book *PriceBook, start, end time.Time) (RateResult, error)
	Ping(ctx context.Context) error
	Close() error
}

// RateResult is what the rating run reports back: the priced traffic
// (rollups/events/total) AND the anomaly counts from the SAME snapshot.
// TotalCost is a NUMERIC carried as a string (money never becomes a Go number).
type RateResult struct {
	// int64 (not int): these are COUNT/SUM over an arbitrary backfill window, so a
	// wide window can exceed 2^31. Widened with the ::bigint SQL casts to avoid a
	// silent 32-bit overflow.
	RollupsWritten int64
	EventsRated    int64
	// TotalCost is the window's summed cost as NUMERIC text (money never becomes a Go
	// number). The SQL COALESCEs the SUM to 0, so an empty window returns "0", not ""
	// — never an empty string.
	TotalCost string

	// Fail-loud counts, from the same statement/snapshot as the upsert, so they can
	// never disagree with what the rollups excluded.
	UnpricedEvents       int64
	UnattributableEvents int64
	// AmbiguousBaseEvents counts events under rollups where a single ft: model_id
	// resolved through MORE THAN ONE distinct base_model in a window — the E3
	// ft-uniqueness violation (a uuid4 checkpoint id can't carry two bases). Those
	// rollups are excluded from the upsert and screamed about, never silently billed at
	// the MIN (cheaper) rate.
	AmbiguousBaseEvents int64
}

// Anomalies are the fail-loud counts for a window: events that could not be priced
// and rows that could not be attributed. Both drive the exit-nonzero path. int64 to
// match RateResult's widened counts.
type Anomalies struct {
	UnpricedEvents       int64
	UnattributableEvents int64
	AmbiguousBaseEvents  int64
}

// PostgresStore reads billing_event and writes rated_usage in the shared Atlas
// Postgres. PRICES ARE NOT IN THE DB — they ride in from the PriceBook (the YAML
// file) per call. Like the drainer it does NOT run migrations; it assumes
// billing_event + rated_usage exist (owned by the Atlas Alembic chain).
type PostgresStore struct {
	db *sql.DB
}

// OpenPostgres opens a *sql.DB against the DSN using the pgx stdlib driver, applies
// pool settings, and Pings once so a bad DSN fails fast at job start.
func OpenPostgres(ctx context.Context, cfg Config) (*PostgresStore, error) {
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("rating: DATABASE_URL is empty (Postgres holds billing_event and rated_usage; the rater cannot run without it)")
	}

	db, err := sql.Open("pgx", ensureUTCTimeZone(cfg.DatabaseURL))
	if err != nil {
		return nil, fmt.Errorf("rating: open postgres: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	s := &PostgresStore{db: db}
	if err := s.Ping(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("rating: postgres ping: %w", err)
	}
	return s, nil
}

// ensureUTCTimeZone pins the session TimeZone to UTC via the DSN, unless the DSN
// already sets one. Belt-and-braces only: the rating SQL is written to be
// session-TZ-independent (see the bucketing expression in rateWindowSQL), so this
// is defense in depth, not the load-bearing fix.
func ensureUTCTimeZone(dsn string) string {
	if strings.Contains(strings.ToLower(dsn), "timezone") {
		return dsn // the operator pinned a TZ explicitly; don't fight it
	}
	if strings.Contains(dsn, "://") { // URL form
		if strings.Contains(dsn, "?") {
			return dsn + "&timezone=UTC"
		}
		return dsn + "?timezone=UTC"
	}
	return dsn + " timezone=UTC" // keyword=value form
}

// NewPostgresStore wraps an existing *sql.DB. Used by tests (sqlmock) and callers
// owning the pool lifecycle.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }
func (s *PostgresStore) Close() error                   { return s.db.Close() }

// createPriceTempSQL creates the per-transaction TEMP price table. It is dropped at
// COMMIT (ON COMMIT DROP), so the prices never persist in the DB — the YAML file is
// the source of truth and a re-run with a different file simply projects different
// rows. NUMERIC(20,9) matches the rated_usage money columns exactly, so the applied
// rate stored on the row is bit-for-bit the rate the cost was computed from.
const createPriceTempSQL = `
CREATE TEMP TABLE rating_price (
    model_id         text PRIMARY KEY,
    prompt_price     NUMERIC(20,9) NOT NULL,
    cached_price     NUMERIC(20,9) NOT NULL,
    completion_price NUMERIC(20,9) NOT NULL
) ON COMMIT DROP`

// createDerivedTempSQL creates the per-transaction DERIVED price table, keyed on the
// BASE model id. It carries the per-token rate a fine-tune deriving from that base
// pays — the global premium applied to the base in exact Dec, quantized to 9dp at
// projection (same NUMERIC(20,9) as rating_price). A fine-tune event arrives with an
// ft:<checkpoint> model_id the file never names, but it carries its base_model (E3);
// the rater joins that base_model here to price it at base x premium. ON COMMIT DROP
// so it never persists. Empty when the file declares no base models (impossible: the
// loader rejects an empty base_models).
const createDerivedTempSQL = `
CREATE TEMP TABLE rating_derived (
    base_model       text PRIMARY KEY,
    prompt_price     NUMERIC(20,9) NOT NULL,
    cached_price     NUMERIC(20,9) NOT NULL,
    completion_price NUMERIC(20,9) NOT NULL
) ON COMMIT DROP`

// rateWindowSQL resolves, sums, upserts, and counts in ONE statement over the
// transient rating_price table (populated from the YAML PriceBook for this run).
//
// RESOLUTION (two tables, direct wins): a billing_event's model NAME is aliased to
// model_id (the price key) and LEFT JOINed to rating_price (the direct rate). If that
// misses AND model_id is an ft:<checkpoint> id carrying a base_model (E3 fine-tune),
// it is LEFT JOINed to rating_derived on base_model — the base-x-premium rate. Both
// tables already carry the FINAL per-token rate (premium applied + quantized in exact
// Dec at projection), so the SQL does NO premium math; it COALESCEs direct-over-derived
// and multiplies. An event that resolves through NEITHER is UNPRICED (NULLs) and is
// COUNTED, never $0-billed — including an ft: id with an EMPTY base_model, which is a
// propagation bug (Atlas guarantees base_model at deploy), not a free model, so it
// MUST scream rather than silently mis-price. (E4's create-time gate should prevent
// any unpriced traffic; the rater keeps the fail-loud backstop.)
//
// THE BILLABLE-PROMPT FORMULA (highest-risk line; mirror of Rate() in the oracle):
//
//	billable_prompt = GREATEST(prompt_tokens - cached_tokens, 0)   -- cached ⊆ prompt
//	cost = billable_prompt   * prompt_price
//	     + cached_tokens     * cached_price
//	     + completion_tokens * completion_price
//
// cached_tokens are the SUBSET of prompt_tokens served from cache; charging them at
// BOTH rates would OVER-bill every cache hit. GREATEST(_,0) clamps a malformed
// cached>prompt so we never CREDIT phantom tokens.
//
// APPLIED RATE STORED ON THE ROW (E1 self-auditing rollup): the rated_usage row
// carries applied_prompt_rate / applied_cached_rate / applied_completion_rate — the
// exact per-token rates this rollup was billed at. The row is then immutable and
// self-auditing: "we never reprice traffic you've already served" holds by
// construction, because the row froze its own rate. A rollup mixes only one
// model_id, so a single applied-rate triple per row is well-defined.
//
// HOUR BUCKET IS SESSION-TZ-INDEPENDENT (date_trunc on a UTC wall-clock timestamp),
// so rollup keys can never disagree across sessions and re-rates can't overlap.
//
// IDEMPOTENCY: ON CONFLICT (auth_id, model_id, window_start) DO UPDATE replaces the
// stored sums/cost/applied-rates with the freshly recomputed ones. The surrogate id
// is DETERMINISTIC (md5 of the LENGTH-PREFIXED natural key — injective, so no '|' in a
// field can collide two keys), so a re-run regenerates the SAME id.
//
// ONE STATEMENT, ONE SNAPSHOT: the upsert and the anomaly counts are CTEs of a
// single statement, so a billing_event the drainer commits mid-run is visible to
// BOTH the rollups and the counts or to NEITHER — never excluded-but-uncounted.
//
// $1 = window start (inclusive), $2 = window end (exclusive), $3 = the fine-tune
// LIKE pattern (fineTunePrefix + '%'), single-sourcing the ft: marker from Go.
const rateWindowSQL = `
WITH ev AS (
    SELECT
        auth_id,
        -- billing_event stores the engine-reported model NAME in its model column;
        -- that name IS phoebe's stable price key, model_id. A NULL model is
        -- unattributable.
        model AS model_id,
        -- base_model: the HF base id a fine-tune derives from (E3), stamped by Atlas.
        -- NULL for a base model. Used only to price an ft:<checkpoint> model_id.
        base_model,
        prompt_tokens,
        cached_tokens,
        completion_tokens,
        COALESCE(event_ts, created_at) AS ev_ts
    FROM billing_event
    WHERE COALESCE(event_ts, created_at) >= $1
      AND COALESCE(event_ts, created_at) <  $2
),
resolved AS (
    SELECT
        ev.auth_id,
        ev.model_id,
        ev.base_model,
        ev.ev_ts,
        ev.prompt_tokens,
        ev.cached_tokens,
        ev.completion_tokens,
        GREATEST(ev.prompt_tokens - ev.cached_tokens, 0) AS billable_prompt,
        -- Direct price wins; else the derived (base x premium) price for an ft: id
        -- carrying a base_model. A miss on BOTH → NULL → UNPRICED (never $0). An ft:
        -- id with a NULL base_model can only miss the derived join (NULL = NULL is
        -- never true), so it correctly falls through to UNPRICED and screams.
        COALESCE(rp.prompt_price,     rd.prompt_price)     AS prompt_price,
        COALESCE(rp.cached_price,     rd.cached_price)     AS cached_price,
        COALESCE(rp.completion_price, rd.completion_price) AS completion_price,
        -- Whether this row priced through the DERIVED (base_model) path: an ft: id that
        -- missed the direct table and hit rating_derived. Drives the ft-uniqueness
        -- enforcement below — only a derived-priced row's base_model matters.
        (rp.model_id IS NULL AND rd.base_model IS NOT NULL) AS via_derived
    FROM ev
    -- The YAML-projected DIRECT price table (keyed on model_id).
    LEFT JOIN rating_price rp ON rp.model_id = ev.model_id
    -- The DERIVED price table (keyed on base_model): consulted ONLY for an ft:
    -- model_id that missed the direct join — base_model prices the fine-tune at
    -- base x premium. The rp.model_id-IS-NULL guard keeps direct-over-derived
    -- precedence (a fine-tune with its own in-file rate is never re-derived); the
    -- ft: prefix guard keeps a base model from ever resolving through the derived
    -- table by accident.
    LEFT JOIN rating_derived rd
        ON rd.base_model = ev.base_model
       AND rp.model_id IS NULL
       -- The ft: prefix is SINGLE-SOURCED from the Go fineTunePrefix constant, bound as
       -- $3 (ftLikePattern), so the money path has ONE source of truth for what marks a
       -- fine-tune — never a literal 'ft:%' that could drift from the constant.
       AND ev.model_id LIKE $3
),
-- grouped: the per-(auth_id, model_id, hour) rollup BEFORE the ft-uniqueness gate.
-- It carries ambiguous_base = COUNT(DISTINCT base_model among DERIVED-priced rows) > 1.
-- E3 mints ft:<checkpoint_artifact_id> as a globally-unique uuid4, so one ft: model_id
-- can NEVER legitimately carry two different base_models. If it does, the derived rates
-- differ and a blind MIN()-applied-rate would silently bill the rollup at the CHEAPER
-- base — under-billing, counted as rated. That is a base_model PROPAGATION/UNIQUENESS
-- violation: it must SCREAM, not silently pick a rate. So ambiguous rollups are split
-- out below (counted as an anomaly, never upserted).
grouped AS (
    SELECT
        auth_id,
        model_id,
        -- Session-TZ-independent hour bucket; see the statement comment.
        date_trunc('hour', ev_ts AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'                     AS window_start,
        date_trunc('hour', ev_ts AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + interval '1 hour' AS window_end,
        SUM(prompt_tokens)::bigint                       AS prompt_tokens,
        SUM(cached_tokens)::bigint                       AS cached_tokens,
        SUM(completion_tokens)::bigint                   AS completion_tokens,
        SUM(billable_prompt)::bigint                     AS billable_prompt_tokens,
        -- THE MONEY: per-event cost summed in SQL. NUMERIC throughout, no float.
        SUM(
            billable_prompt   * prompt_price
          + cached_tokens     * cached_price
          + completion_tokens * completion_price
        )                                                AS cost,
        -- The applied per-token rates frozen onto the row. A rollup is single-model and
        -- (by the ft-uniqueness invariant enforced below) single derived-base, so all
        -- rows share ONE rate; MIN picks it deterministically. The ambiguous_base guard
        -- guarantees MIN is not silently masking a second, different rate.
        MIN(prompt_price)                                AS applied_prompt_rate,
        MIN(cached_price)                                AS applied_cached_rate,
        MIN(completion_price)                            AS applied_completion_rate,
        COUNT(*)::bigint                                 AS event_count,
        -- > 1 distinct base_model among the DERIVED-priced rows → ambiguous (the E3
        -- ft-uniqueness violation). DERIVED rows only: a direct-priced row's base_model
        -- never affects its rate, so it must not trip the gate.
        COUNT(DISTINCT base_model) FILTER (WHERE via_derived) > 1 AS ambiguous_base
    FROM resolved
    WHERE prompt_price IS NOT NULL          -- priced only
      AND auth_id  IS NOT NULL              -- attributable only
      AND model_id IS NOT NULL
    GROUP BY auth_id, model_id, date_trunc('hour', ev_ts AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
),
priced AS (
    SELECT * FROM grouped WHERE NOT ambiguous_base
),
upserted AS (
    INSERT INTO rated_usage (
        id, auth_id, model_id, window_start, window_end,
        prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens,
        cost, applied_prompt_rate, applied_cached_rate, applied_completion_rate,
        event_count
    )
    SELECT
        -- DETERMINISTIC 32-char hex surrogate: md5 of the natural key, so re-rating
        -- regenerates the SAME id. The fields are LENGTH-PREFIXED (len || ':' || value)
        -- so the encoding is INJECTIVE — a '|' inside auth_id or model_id can never
        -- shift the boundary and collide two different keys onto one id (e.g. auth 'a|b'
        -- + model 'c' vs auth 'a' + model 'b|c'). epoch (a bounded integer, no
        -- separator hazard) keeps the hash input session-TZ-independent.
        md5(length(auth_id)::text || ':' || auth_id
          || '|' || length(model_id)::text || ':' || model_id
          || '|' || extract(epoch FROM window_start)::bigint::text),
        auth_id, model_id, window_start, window_end,
        prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens,
        cost, applied_prompt_rate, applied_cached_rate, applied_completion_rate,
        event_count
    FROM priced
    -- Deterministic lock order across concurrent raters (no ABBA deadlock).
    ORDER BY auth_id, model_id, window_start
    ON CONFLICT (auth_id, model_id, window_start) DO UPDATE SET
        window_end              = EXCLUDED.window_end,
        prompt_tokens           = EXCLUDED.prompt_tokens,
        cached_tokens           = EXCLUDED.cached_tokens,
        completion_tokens       = EXCLUDED.completion_tokens,
        billable_prompt_tokens  = EXCLUDED.billable_prompt_tokens,
        cost                    = EXCLUDED.cost,
        applied_prompt_rate     = EXCLUDED.applied_prompt_rate,
        applied_cached_rate     = EXCLUDED.applied_cached_rate,
        applied_completion_rate = EXCLUDED.applied_completion_rate,
        event_count             = EXCLUDED.event_count,
        rated_at                = now()
    RETURNING event_count, cost
)
SELECT
    (SELECT COUNT(*)::bigint                      FROM upserted) AS rollups_written,
    (SELECT COALESCE(SUM(event_count), 0)::bigint FROM upserted) AS events_rated,
    (SELECT COALESCE(SUM(cost), 0)::numeric       FROM upserted) AS total_cost,
    -- Anomaly counts from the SAME snapshot as the upsert. An unattributable row is
    -- counted ONLY as unattributable (the more specific signal), never also as
    -- unpriced, so the counts partition the in-window rows:
    --   events_rated + unpriced + unattributable + ambiguous_base == total in-window events.
    (SELECT COUNT(*)::bigint FROM resolved
      WHERE prompt_price IS NULL
        AND auth_id  IS NOT NULL
        AND model_id IS NOT NULL)                             AS unpriced_events,
    (SELECT COUNT(*)::bigint FROM ev
      WHERE auth_id IS NULL OR model_id IS NULL)              AS unattributable_events,
    -- AMBIGUOUS-BASE events: the EVENT count under ambiguous rollups (a single ft:
    -- model_id resolving through >1 distinct base_model in one window — the E3
    -- ft-uniqueness violation). These rollups are NOT upserted (excluded from priced),
    -- so their events are neither rated nor $0-billed; they are counted here, from the
    -- SAME snapshot, and drive the fail-loud exit. SUM(event_count) (not COUNT(*) of
    -- rollups) so the partition identity above stays in EVENT units.
    (SELECT COALESCE(SUM(event_count), 0)::bigint FROM grouped
      WHERE ambiguous_base)                                  AS ambiguous_base_events`

// RateWindow runs the price-projection + the single resolve→sum→upsert→count
// statement for [start, end) in ONE transaction, and reports the rollups written,
// events rated, total cost (NUMERIC text), and the fail-loud anomaly counts — all
// from one snapshot. All money math is in SQL.
//
// The TEMP price table (ON COMMIT DROP) and the rating statement share the
// transaction, so the rates the cost is computed from are exactly the rates
// projected from the file this run loaded — there is no window where another run's
// prices could leak in.
func (s *PostgresStore) RateWindow(ctx context.Context, book *PriceBook, start, end time.Time) (RateResult, error) {
	if book == nil {
		return RateResult{}, fmt.Errorf("rating: nil price book (the rater must load a price file before rating)")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RateResult{}, fmt.Errorf("rating: begin tx: %w", err)
	}
	// Roll back on any error path; the successful path Commits and returns before this.
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx, createPriceTempSQL); err != nil {
		return RateResult{}, fmt.Errorf("rating: create temp price table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, createDerivedTempSQL); err != nil {
		return RateResult{}, fmt.Errorf("rating: create temp derived-price table: %w", err)
	}
	if err := insertPrices(ctx, tx, book.resolvedRates()); err != nil {
		return RateResult{}, err
	}
	if err := insertDerived(ctx, tx, book.derivedRates()); err != nil {
		return RateResult{}, err
	}

	var res RateResult
	var total string
	err = tx.QueryRowContext(ctx, rateWindowSQL, start.UTC(), end.UTC(), ftLikePattern).
		Scan(&res.RollupsWritten, &res.EventsRated, &total,
			&res.UnpricedEvents, &res.UnattributableEvents, &res.AmbiguousBaseEvents)
	if err != nil {
		return RateResult{}, fmt.Errorf("rating: rate window [%s,%s): %w",
			start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339), err)
	}

	if err := tx.Commit(); err != nil {
		return RateResult{}, fmt.Errorf("rating: commit: %w", err)
	}
	committed = true

	res.TotalCost = total
	return res, nil
}

// insertPrices bulk-loads the projected rates into the TEMP rating_price table with
// a single multi-row INSERT (one round-trip). The rates are canonical decimal
// strings bound as NUMERIC — money never becomes a Go float, even in transit.
func insertPrices(ctx context.Context, tx *sql.Tx, rates []resolvedRate) error {
	if len(rates) == 0 {
		// An empty price book would $0/UNPRICE everything; the loader already rejects
		// an empty base_models, so this is a belt-and-braces guard.
		return fmt.Errorf("rating: price book projected zero rates (would price nothing)")
	}
	var sb strings.Builder
	sb.WriteString("INSERT INTO rating_price (model_id, prompt_price, cached_price, completion_price) VALUES ")
	args := make([]any, 0, len(rates)*4)
	for i, r := range rates {
		if i > 0 {
			sb.WriteString(", ")
		}
		n := i * 4
		fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d)", n+1, n+2, n+3, n+4)
		args = append(args, r.ModelID, r.Prompt, r.Cached, r.Completion)
	}
	if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("rating: load prices into temp table: %w", err)
	}
	return nil
}

// insertDerived bulk-loads the projected DERIVED rates (base_model x premium) into the
// TEMP rating_derived table with a single multi-row INSERT. Same NUMERIC-as-string
// discipline as insertPrices — money never becomes a Go float in transit. The slice is
// non-empty in practice (the loader rejects an empty base_models, and every base
// yields one derived row), but an empty slice is tolerated: a file with only own-rate
// fine-tunes and no derivable base simply has no derived rows, and any ft: event then
// falls through to UNPRICED (fail loud).
func insertDerived(ctx context.Context, tx *sql.Tx, rates []derivedRate) error {
	if len(rates) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString("INSERT INTO rating_derived (base_model, prompt_price, cached_price, completion_price) VALUES ")
	args := make([]any, 0, len(rates)*4)
	for i, r := range rates {
		if i > 0 {
			sb.WriteString(", ")
		}
		n := i * 4
		fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d)", n+1, n+2, n+3, n+4)
		args = append(args, r.BaseModel, r.Prompt, r.Cached, r.Completion)
	}
	if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("rating: load derived prices into temp table: %w", err)
	}
	return nil
}
