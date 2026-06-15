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
// Mirrors internal/drain.Store.
//
// The v2 contract is SQL-SIDE and money-in-SQL: there is no LoadPrices /
// per-event Rate loop. The whole window is rated, summed, and upserted by the DB.
//
//   - RateWindow runs the SINGLE statement that resolves the effective price (own
//     rate, or base-via-derived_from through the global policy), computes per-event
//     cost AND sums it per (auth_id, model_id, hour) into rated_usage, idempotently
//     (ON CONFLICT DO UPDATE recomputes from scratch) — AND, in the same statement,
//     counts the fail-loud anomalies: events that could NOT be priced (no resolvable
//     rate / chain > 1 hop) and rows that are unattributable (NULL auth_id/model_id).
//     ONE statement means ONE snapshot: a row the drainer commits mid-run can never
//     be excluded from the rollups yet missed by the anomaly counts (two separate
//     READ COMMITTED statements could disagree exactly that way). Anomalous rows are
//     NEVER summed into a rollup; they are counted and surfaced.
//   - CountAnomalies re-counts the anomalies for an arbitrary window (ad-hoc / ops
//     use). The rater's fail-loud contract does NOT depend on it — Run gets its
//     counts from RateWindow's single snapshot.
type Store interface {
	RateWindow(ctx context.Context, start, end time.Time) (RateResult, error)
	CountAnomalies(ctx context.Context, start, end time.Time) (Anomalies, error)
	Ping(ctx context.Context) error
	Close() error
}

// RateResult is what the single rating statement reports back: the priced traffic
// (rollups/events/total) AND the anomaly counts from the SAME snapshot.
// TotalCost is a NUMERIC carried as a string (money never becomes a Go number).
type RateResult struct {
	// int64 (not int): these are COUNT/SUM over an arbitrary backfill window, so a
	// wide window can exceed 2^31. Widened with the ::bigint SQL casts to avoid a
	// silent 32-bit overflow.
	RollupsWritten int64
	EventsRated    int64
	TotalCost      string // NUMERIC as text; "" when no rollups

	// Fail-loud counts, from the same statement/snapshot as the upsert above, so
	// they can never disagree with what the rollups excluded.
	UnpricedEvents       int64
	UnattributableEvents int64
}

// Anomalies are the fail-loud counts for a window: events that could not be priced
// and rows that could not be attributed. Both drive the exit-nonzero path. int64
// to match RateResult's widened counts (a wide backfill window can exceed 2^31).
type Anomalies struct {
	UnpricedEvents       int64
	UnattributableEvents int64
}

// PostgresStore reads billing_event + model_price + derivation_policy and writes
// rated_usage in the shared Atlas Postgres. Like the drainer it does NOT run
// migrations — it assumes the tables exist (owned by the Atlas Alembic chain; see
// migrations/README.md).
type PostgresStore struct {
	db *sql.DB
}

// OpenPostgres opens a *sql.DB against the DSN using the pgx stdlib driver, applies
// pool settings, and Pings once so a bad DSN fails fast at job start.
func OpenPostgres(ctx context.Context, cfg Config) (*PostgresStore, error) {
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("rating: DATABASE_URL is empty (Postgres holds billing_event and the price book; the rater cannot run without it)")
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
// is defense in depth, not the load-bearing fix. pgx passes unrecognized
// parameters (URL query or keyword form) to the server as run-time parameters.
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

// resolvedEventsCTE is the heart of the v2 rater: a CTE that, for every
// billing_event row in [$1, $2), resolves the effective per-token rate and the
// per-event cost ENTIRELY IN SQL. It is shared verbatim by rateWindowSQL (which
// SUMs and upserts the priced rows AND counts the rows that did NOT resolve, in
// one statement/snapshot) and countAnomaliesSQL (the ad-hoc/ops counting query),
// so the two ALWAYS agree on what "priced" means.
//
// Resolution, AS-OF each event's rating instant ev_ts = COALESCE(event_ts,
// created_at):
//
//	own  : the model's OWN model_price row effective at ev_ts that carries a rate
//	       (prompt_price NOT NULL). If present, it WINS — the derivation policy is
//	       bypassed (the operator escape hatch).
//	base : else the model's effective row whose derived_from is set; we then look up
//	       the BASE model_id's effective row that carries a rate, and the
//	       derivation_policy effective at ev_ts, and apply policy(base_rate).
//	       ONE HOP ONLY: if the base row has NO own rate (it is itself derived /
//	       rate-less), the LEFT JOIN to base-with-rate yields NULL and the event is
//	       UNPRICED — we never recurse. (A multiplier/markup of a NULL base rate is
//	       NULL, so it falls through to unpriced too.)
//	none : neither resolves → rate columns are NULL → the event is UNPRICED.
//
// The effective-window predicate is the half-open [effective_from, effective_to):
//
//	effective_from <= ev_ts AND (effective_to IS NULL OR ev_ts < effective_to)
//
// The model_price GiST exclusion constraint guarantees AT MOST ONE row matches per
// (model_id, instant), so these scalar subselects cannot fan out / double-count.
//
// THE BILLABLE-PROMPT FORMULA (highest-risk line; mirror of Rate() in rate.go):
//
//	billable_prompt = GREATEST(prompt_tokens - cached_tokens, 0)   -- cached ⊆ prompt
//	cost = billable_prompt   * prompt_price
//	     + cached_tokens     * cached_price
//	     + completion_tokens * completion_price
//
// cached_tokens are the SUBSET of prompt_tokens served from cache; charging them at
// BOTH the prompt and cached rate would OVER-bill every cache hit. GREATEST(_,0)
// clamps a malformed cached>prompt so we never CREDIT phantom tokens.
//
// $1 = window start (inclusive), $2 = window end (exclusive).
//
// The own / base / policy lookups are inlined as per-event LATERAL joins (one row
// per event by construction, so no CTE re-join key is needed). The model_price GiST
// exclusion constraint guarantees each LATERAL matches AT MOST ONE row.
const resolvedEventsCTE = `
WITH ev AS (
    SELECT
        auth_id,
        -- billing_event stores the engine-reported model NAME in its model column
        -- (untouched v1 metering schema); that name IS phoebe's stable price key,
        -- model_id, so we alias it here. (model_id deliberately is NOT resource_id,
        -- which is the ephemeral deployment id.) A NULL model is unattributable.
        model AS model_id,
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
        ev.ev_ts,
        ev.prompt_tokens,
        ev.cached_tokens,
        ev.completion_tokens,
        GREATEST(ev.prompt_tokens - ev.cached_tokens, 0) AS billable_prompt,
        -- OWN rate wins (escape hatch); else the DERIVED rate (base via policy);
        -- else NULL → unpriced. Each component COALESCEs own over derived.
        COALESCE(o.prompt_price,
            CASE pol.function
                WHEN 'multiplier' THEN base.prompt_price * pol.factor
                WHEN 'markup'     THEN base.prompt_price + pol.markup
                ELSE base.prompt_price                     -- identity / no policy row
            END) AS prompt_price,
        COALESCE(o.cached_price,
            CASE pol.function
                WHEN 'multiplier' THEN base.cached_price * pol.factor
                WHEN 'markup'     THEN base.cached_price + pol.markup
                ELSE base.cached_price
            END) AS cached_price,
        COALESCE(o.completion_price,
            CASE pol.function
                WHEN 'multiplier' THEN base.completion_price * pol.factor
                WHEN 'markup'     THEN base.completion_price + pol.markup
                ELSE base.completion_price
            END) AS completion_price
    FROM ev
    -- OWN rate: the model's own effective rate row at ev_ts (escape hatch).
    LEFT JOIN LATERAL (
        SELECT mp.prompt_price, mp.cached_price, mp.completion_price
        FROM model_price mp
        WHERE mp.model_id = ev.model_id
          AND mp.prompt_price IS NOT NULL                  -- carries a rate
          AND mp.effective_from <= ev.ev_ts
          AND (mp.effective_to IS NULL OR ev.ev_ts < mp.effective_to)
        LIMIT 1                                             -- GiST guarantees <= 1
    ) o ON TRUE
    -- DERIVED: the model's effective row that derives (derived_from, no own rate).
    LEFT JOIN LATERAL (
        SELECT mp.derived_from
        FROM model_price mp
        WHERE mp.model_id = ev.model_id
          AND mp.prompt_price IS NULL                      -- no own rate
          AND mp.derived_from IS NOT NULL
          AND mp.effective_from <= ev.ev_ts
          AND (mp.effective_to IS NULL OR ev.ev_ts < mp.effective_to)
        LIMIT 1
    ) der ON TRUE
    -- BASE: the base's effective row that carries a rate. ONE HOP ONLY — the base
    -- must have its OWN rate; if the base is itself derived (rate-less) this is NULL
    -- and the event falls through to unpriced. We never recurse.
    LEFT JOIN LATERAL (
        SELECT mp.prompt_price, mp.cached_price, mp.completion_price
        FROM model_price mp
        WHERE mp.model_id = der.derived_from
          AND mp.prompt_price IS NOT NULL
          AND mp.effective_from <= ev.ev_ts
          AND (mp.effective_to IS NULL OR ev.ev_ts < mp.effective_to)
        LIMIT 1
    ) base ON TRUE
    -- POLICY: the single global derivation policy effective at ev_ts (identity if
    -- none). Only consulted for the derived branch; own rate ignores it.
    LEFT JOIN LATERAL (
        SELECT dp.function, dp.factor, dp.markup
        FROM derivation_policy dp
        WHERE dp.effective_from <= ev.ev_ts
          AND (dp.effective_to IS NULL OR ev.ev_ts < dp.effective_to)
        LIMIT 1
    ) pol ON TRUE
)`

// rateWindowSQL appends, to the resolution CTE, the INSERT ... SELECT that SUMs the
// PRICED, ATTRIBUTABLE events per (auth_id, model_id, hour) into rated_usage and
// upserts idempotently — AND the anomaly counts over the SAME CTEs.
//
// PRICED  = prompt_price IS NOT NULL (resolution succeeded).
// ATTRIB. = auth_id IS NOT NULL AND model_id IS NOT NULL.
// Unpriced / unattributable rows are EXCLUDED from the rollups (never $0-billed)
// and counted by the final SELECT below.
//
// ONE STATEMENT, ONE SNAPSHOT (the fail-loud guarantee): the upsert and the anomaly
// counts are CTEs of a single statement, and in Postgres every CTE of a statement —
// including a data-modifying CTE — sees the same snapshot. So a billing_event row
// the drainer commits while the rater runs is either visible to BOTH the rollups
// and the counts, or to NEITHER; it can never be excluded-from-rollups-but-uncounted
// (which two separate READ COMMITTED statements allowed, silently defeating the
// exit-nonzero contract).
//
// MONEY IS COMPUTED AND SUMMED IN SQL — the cost expression multiplies token
// counts by the NUMERIC rates and SUM()s, so Go never touches a money value except
// to read the resulting NUMERIC text.
//
// THE HOUR BUCKET MUST BE SESSION-TZ-INDEPENDENT: date_trunc('hour', tstz) truncates
// in the session TimeZone, so a fractional-offset session (e.g. +05:30) would shift
// bucket boundaries — window_start off by 30 minutes and ON CONFLICT keys that
// DISAGREE across sessions, i.e. overlapping rollups and a double-bill on re-rate.
// Same hazard class the schema already closed for the exclusion constraint (see the
// tstzrange-not-tsrange comment in migrations/0002_rating.sql). The expression
//
//	date_trunc('hour', ev_ts AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
//
// converts to a UTC wall-clock timestamp, truncates, and converts back — identical
// on every session TZ. (Preferred over the 3-arg date_trunc(..., 'UTC'), which is
// PG14+ only; this form works on all supported Postgres versions.)
//
// IDEMPOTENCY: ON CONFLICT (auth_id, model_id, window_start) DO UPDATE replaces the
// stored sums/cost with the freshly recomputed ones, so a re-run reconciles to the
// correct totals and never doubles. The surrogate id is DETERMINISTIC: md5 of the
// natural key (auth_id|model_id|epoch(window_start)), so a re-run inserts the SAME
// id for the same rollup — there is no random id that could PK-collide differently
// across runs. epoch() rather than ::text keeps the hash input session-TZ-
// independent (timestamptz::text renders in the session TZ). A cross-key md5
// collision (or a crafted '|' embedding) would abort the statement with a PK error
// and exit the run loudly — never a wrong bill.
//
// The upsert's SELECT is ORDERed BY the natural key so concurrent raters lock the
// conflicting rows in the same order — no ABBA deadlock between overlapping runs.
//
// RETURNING feeds the final SELECT so one round-trip reports rollups written, rated
// event count, total cost (NUMERIC text), and both anomaly counts.
const rateWindowSQL = resolvedEventsCTE + `,
priced AS (
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
        COUNT(*)::int                                    AS event_count
    FROM resolved
    WHERE prompt_price IS NOT NULL          -- priced only
      AND auth_id  IS NOT NULL              -- attributable only
      AND model_id IS NOT NULL
    GROUP BY auth_id, model_id, date_trunc('hour', ev_ts AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
),
upserted AS (
    INSERT INTO rated_usage (
        id, auth_id, model_id, window_start, window_end,
        prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens,
        cost, event_count
    )
    SELECT
        -- DETERMINISTIC 32-char hex surrogate: md5 of the natural key, so re-rating
        -- a window regenerates the SAME id (no random component to collide
        -- differently across runs; no extension dependency — gen_random_bytes needs
        -- pgcrypto). On conflict the existing row's id is kept anyway; see the
        -- statement comment for the (statement-aborting, never wrong-bill)
        -- cross-key-collision caveat.
        md5(auth_id || '|' || model_id || '|' || extract(epoch FROM window_start)::bigint::text),
        auth_id, model_id, window_start, window_end,
        prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens,
        cost, event_count
    FROM priced
    -- Deterministic lock order across concurrent raters (no ABBA deadlock).
    ORDER BY auth_id, model_id, window_start
    ON CONFLICT (auth_id, model_id, window_start) DO UPDATE SET
        window_end             = EXCLUDED.window_end,
        prompt_tokens          = EXCLUDED.prompt_tokens,
        cached_tokens          = EXCLUDED.cached_tokens,
        completion_tokens      = EXCLUDED.completion_tokens,
        billable_prompt_tokens = EXCLUDED.billable_prompt_tokens,
        cost                   = EXCLUDED.cost,
        event_count            = EXCLUDED.event_count,
        rated_at               = now()
    RETURNING event_count, cost
)
SELECT
    -- ::bigint, not ::int: a very wide backfill window can sum more than 2^31
    -- events (or write more than 2^31 rollup rows), which a 32-bit cast would
    -- silently overflow into a negative/garbage count and defeat the rated-count
    -- reconciliation. The cost is already NUMERIC. Scanned into int64 in Go.
    (SELECT COUNT(*)::bigint                      FROM upserted) AS rollups_written,
    (SELECT COALESCE(SUM(event_count), 0)::bigint FROM upserted) AS events_rated,
    (SELECT COALESCE(SUM(cost), 0)::numeric       FROM upserted) AS total_cost,
    -- Anomaly counts from the SAME snapshot as the upsert. An unattributable row is
    -- counted ONLY as unattributable (the more specific signal), never also as
    -- unpriced, so the counts partition the in-window rows:
    --   events_rated + unpriced + unattributable == total in-window events.
    (SELECT COUNT(*)::bigint FROM resolved
      WHERE prompt_price IS NULL
        AND auth_id  IS NOT NULL
        AND model_id IS NOT NULL)                             AS unpriced_events,
    (SELECT COUNT(*)::bigint FROM ev
      WHERE auth_id IS NULL OR model_id IS NULL)              AS unattributable_events`

// RateWindow runs the single resolve→sum→upsert→count statement for [start, end)
// and reports the rollups written, events rated, total cost (NUMERIC text), and the
// fail-loud anomaly counts — all from one snapshot. All money math is in SQL.
func (s *PostgresStore) RateWindow(ctx context.Context, start, end time.Time) (RateResult, error) {
	var res RateResult
	var total string
	err := s.db.QueryRowContext(ctx, rateWindowSQL, start.UTC(), end.UTC()).
		Scan(&res.RollupsWritten, &res.EventsRated, &total,
			&res.UnpricedEvents, &res.UnattributableEvents)
	if err != nil {
		return RateResult{}, fmt.Errorf("rating: rate window [%s,%s): %w",
			start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339), err)
	}
	res.TotalCost = total
	return res, nil
}

// countAnomaliesSQL appends, to the resolution CTE, a single counting query over
// the SAME window and SAME resolution: events that could NOT be priced (resolution
// returned NULL rates) among ATTRIBUTABLE rows, and rows that are UNATTRIBUTABLE
// (NULL auth_id/model_id). It shares resolvedEventsCTE with the rating insert so
// "unpriced" means exactly the same thing in both.
//
// NOTE: the rater's fail-loud contract does NOT use this — Run gets its anomaly
// counts from rateWindowSQL's single snapshot (a separate statement could miss a
// row committed between the two). This query exists for ad-hoc / ops inspection of
// a window without writing rollups.
//
// An unattributable row cannot be priced either, but it is counted ONLY as
// unattributable (the distinct, more-specific signal) so the two counts don't
// double-attribute the same row.
const countAnomaliesSQL = resolvedEventsCTE + `
SELECT
    COUNT(*) FILTER (
        WHERE prompt_price IS NULL
          AND auth_id  IS NOT NULL
          AND model_id IS NOT NULL
    )::int AS unpriced,
    COUNT(*) FILTER (
        WHERE auth_id IS NULL OR model_id IS NULL
    )::int AS unattributable
FROM resolved`

// CountAnomalies counts the fail-loud signals for [start, end): unpriced events and
// unattributable rows. Nonzero either way drives the exit-nonzero / loud-log path.
func (s *PostgresStore) CountAnomalies(ctx context.Context, start, end time.Time) (Anomalies, error) {
	var a Anomalies
	err := s.db.QueryRowContext(ctx, countAnomaliesSQL, start.UTC(), end.UTC()).
		Scan(&a.UnpricedEvents, &a.UnattributableEvents)
	if err != nil {
		return Anomalies{}, fmt.Errorf("rating: count anomalies [%s,%s): %w",
			start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339), err)
	}
	return a, nil
}
