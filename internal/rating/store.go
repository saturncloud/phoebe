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
// resource_id, model_id, hour) into rated_usage idempotently, AND counts the fail-loud anomalies,
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
	// ReconciledDeletions is how many stale rated_usage rows this run DELETED because
	// they billed in a prior run but fell out of the current priced set (re-rate
	// convergence — "what the latest run says is what bills"). 0 on a first run or a
	// clean identical re-run; nonzero only when a re-rate supersedes prior billing.
	ReconciledDeletions int64
	// TotalCost is the window's summed cost as NUMERIC text (money never becomes a Go
	// number). The SQL COALESCEs the SUM to 0, so an empty window returns "0", not ""
	// — never an empty string.
	TotalCost string

	// Fail-loud counts, from the same statement/snapshot as the upsert, so they can
	// never disagree with what the rollups excluded.
	UnpricedEvents       int64
	UnattributableEvents int64
	// MissingUsageEvents are execution-attempt records for which the serving engine
	// supplied no authoritative usage block. They are retained as zero-charge audit
	// evidence and excluded from rated_usage. This is the TOTAL; it is reported for
	// reconciliation and is deliberately NOT the paging signal, because it is
	// dominated by routine client aborts and upstream failures.
	MissingUsageEvents int64
	// ExpectedMissingUsageEvents is the routine share of MissingUsageEvents: the
	// client disconnected, or the attempt terminated with a non-success status.
	// Correctly billed zero; reviewed in the reconciliation view, never paged.
	ExpectedMissingUsageEvents int64
	// UnexplainedMissingUsageEvents is the alarming share: NOT aborted and NOT
	// failed, so the engine reported success while reporting no tokens — work we
	// may have served and cannot bill. This is the fail-loud missing-usage signal.
	// A NULL status_code counts here (fail closed: an attempt we cannot prove
	// failed is not silently excused).
	UnexplainedMissingUsageEvents int64
	// InvalidUsageEvents are authoritative rows that violate token
	// invariants. They remain raw evidence but are excluded from money.
	InvalidUsageEvents int64
	// AmbiguousBaseEvents counts events under rollups whose base_model-priced rows
	// (derived OR plain-base) did not share ONE rate in the window: a single model_id
	// resolving through more than one distinct base_model (the E3 ft-uniqueness
	// violation for an ft: id; the same hazard for a reused endpoint name under C4),
	// or a mix of premium and plain-base pricing on one endpoint name (the adapter
	// header flapping). Those rollups are excluded from the upsert and screamed
	// about, never silently billed at the MIN (cheaper) rate.
	AmbiguousBaseEvents int64
	// AmbiguousOrgEvents counts events under rollups carrying >1 distinct non-NULL
	// org_id — an E2 attribution propagation bug (one resource resolving to two orgs in
	// a window). Those rollups are excluded from the upsert and screamed about, never
	// billed to a guessed org. A partial-NULL org (real org + missing-header rows) is
	// NOT ambiguous and does not count here.
	AmbiguousOrgEvents int64
	// OwnerConflictEvents counts events under rollups where some event carried BOTH a
	// user_id and a group_id. Upstream an identity is a user XOR a group, so both set
	// is a producer bug and the owner cannot be determined. Those rollups are excluded
	// from the upsert and screamed about, never billed to a guessed owner; the raw
	// events stay in billing_event as evidence.
	OwnerConflictEvents int64
	// AmbiguousGraphRollups counts ROLLUPS (not events) whose traffic came from more
	// than one serving graph. Unlike the buckets above this is NOT a withholding
	// signal: the graph is cost-attribution evidence, not identity, so the rollup bills
	// normally with a NULL graph rather than a guessed one. Counted in rollup units
	// because these events are already inside EventsRated — counting them in event
	// units too would break the anomaly partition.
	AmbiguousGraphRollups int64
}

// Anomalies are the fail-loud counts for a window: events that could not be priced
// and rows that could not be attributed. Both drive the exit-nonzero path. int64 to
// match RateResult's widened counts.
type Anomalies struct {
	UnpricedEvents                int64
	UnattributableEvents          int64
	MissingUsageEvents            int64
	ExpectedMissingUsageEvents    int64
	UnexplainedMissingUsageEvents int64
	InvalidUsageEvents            int64
	AmbiguousBaseEvents           int64
	AmbiguousOrgEvents            int64
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
// RESOLUTION (the C4 ladder, precedence a > b > c > d; the Go mirror is
// PriceBook.ResolveEvent): vLLM serves Token Factory endpoints under the ENDPOINT
// NAME, so billing_event.model (aliased to model_id) is usually NOT a price-file
// key; the event's base_model (X-Saturn-Base-Model, on ALL TF deployments) is the
// catalog price key and a non-null adapter (X-Saturn-Adapter, ONLY on fine-tune
// checkpoint deployments) marks fine-tune traffic.
//
//	a. rating_price on model_id (the direct rate — a price-file key, incl. any
//	   per-endpoint override) wins;
//	b. else, for FINE-TUNE traffic (adapter non-null OR an ft: model_id) carrying a
//	   base_model: rating_derived on base_model — the base-x-premium rate;
//	c. else, for a BASE-MODEL endpoint (no adapter, no ft: prefix) carrying a
//	   base_model: rating_price keyed on base_model — the PLAIN base rate, no
//	   premium;
//	d. an event that resolves through NONE is UNPRICED (NULLs) and is COUNTED,
//	   never $0-billed — including fine-tune traffic with a NULL base_model, which
//	   is a propagation bug (Atlas guarantees base_model at deploy), not a free
//	   model, so it MUST scream rather than silently mis-price. The (b) and (c)
//	   join guards are mutually exclusive on the fine-tune marker, so fine-tune
//	   traffic with an unpriced base can never slip into the plain-base rate.
//
// All tables already carry the FINAL per-token rate (premium applied + quantized in
// exact Dec at projection), so the SQL does NO premium math; it COALESCEs
// direct-over-derived-over-plain-base and multiplies. (E4's create-time gate should
// prevent any unpriced traffic; the rater keeps the fail-loud backstop.)
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
// exact per-token rates this rollup was billed at, so the row is auditable on its
// own. A rollup mixes only one model_id, so one rate triple is well-defined.
//
// "NEVER REPRICE SERVED TRAFFIC" holds WITHOUT a local price-freeze table: the
// caller rates each hour against the book EFFECTIVE DURING that hour (the manager
// owns the effective-dated series), so re-rating an old hour resolves the same
// rates it originally did. Deleting and recreating an anomalous rollup therefore
// cannot pick up a newer rate — the price is a function of the hour, not of when
// the rater ran. phoebe keeps NO price history of its own.
//
// HOUR BUCKET IS SESSION-TZ-INDEPENDENT (date_trunc on a UTC wall-clock timestamp),
// so rollup keys can never disagree across sessions and re-rates can't overlap.
//
// IDEMPOTENCY IS RECONCILE, NOT UPSERT-ONLY: a re-run of a window makes rated_usage
// EXACTLY what the latest run says. ON CONFLICT (auth_id, resource_id, model_id,
// window_start) DO UPDATE replaces a surviving rollup's sums/cost/applied-rates with
// the freshly recomputed ones (the surrogate id is DETERMINISTIC — md5 of the LENGTH-PREFIXED
// natural key, injective, so no '|' in a field can collide two keys — so a re-run
// regenerates the SAME id). AND the `deleted` CTE removes any in-window rated_usage
// row this run did NOT reproduce in priced (it became ambiguous/unpriced, or its
// events vanished), so a rollup that billed clean in a prior run cannot keep billing
// at its stale cost. A clean re-run with identical data reproduces every prior row, so
// the delete matches nothing — a no-op. (Was upsert-only; reconcile is Hugo's decision,
// "what the latest run says is what bills.")
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
        -- resource_id: the deployment id (E2 customer attribution — the owning org is
        -- captured at meter time into org_id; see that column below, no push-time join).
        -- It is part of the rated_usage grain: a NULL resource_id row CANNOT name its
        -- deployment/org, so it is unattributable (counted, never billed to a NULL org) —
        -- see the unattributable filter below.
        resource_id,
        -- org_id: the deployment-owning org (E2 attribution), captured at meter time
        -- from X-Saturn-Org-Id. Carried onto the rollup so push reads org off the row
        -- (no resource_name join). NULL when the producer header was absent — carried
        -- through and surfaced (a NULL-org rollup is held at push, never billed to a
        -- guessed org); it does NOT enter the rollup grain (org is a function of
        -- resource_id, so it must not split a rollup).
        org_id,
        -- billing_event stores the engine-reported model NAME in its model column;
        -- that name IS phoebe's stable price key, model_id. A NULL model is
        -- unattributable.
        model AS model_id,
        -- base_model: the HF base id — the catalog price key (C4), stamped by Atlas
        -- on every Token Factory deployment. Prices both a base-model endpoint
        -- (plain base rate) and a fine-tune (base x premium).
        base_model,
        -- serving_mode: the serving-mode SKU axis (X-Saturn-Serving-Mode). NULL/''
        -- = dedicated; 'shared' = shared traffic, priced from the SKU base key below.
        serving_mode,
        -- sku_base: the MODE-PREFIXED base price key (design D1, mirrors the Go
        -- servingModeKey). Shared traffic (serving_mode = 'shared') prices from a
        -- DISTINCT 'shared:'||base_model row; dedicated (NULL/''/anything else = the
        -- absence-of-prefix contract) keys on the bare base_model. The (b) derived
        -- and (c) plain-base joins below key on THIS, so shared and dedicated of the
        -- same base resolve to independent rate rows.
        CASE WHEN serving_mode = 'shared' THEN 'shared:' || base_model ELSE base_model END
            AS sku_base,
        -- adapter: the fine-tune checkpoint artifact id, non-NULL ONLY on fine-tune
        -- checkpoint deployments. Its PRESENCE is the premium trigger (C4).
        adapter,
        -- OWNER IDENTITY, collapsed from billing_event's two nullable columns into the
        -- single (type, id) pair the upstream model actually has: an identity is a user
        -- XOR a group, never both. NULL/'' on both sides means the producer supplied no
        -- owner -- attribution is then by auth_id alone, which is not an error.
        --
        -- BOTH set is a producer bug (it contradicts the upstream ownership model). It
        -- is NOT silently resolved to one side: owner_conflict below flags it, the
        -- rollup is withheld from money, and the raw evidence stays in billing_event
        -- for diagnosis (Hugo, 2026-09-23: retain raw, exclude from money, alarm).
        CASE
            WHEN COALESCE(user_id, '')  <> '' AND COALESCE(group_id, '') <> '' THEN ''
            WHEN COALESCE(user_id, '')  <> '' THEN 'user'
            WHEN COALESCE(group_id, '') <> '' THEN 'group'
            ELSE ''
        END AS owner_type,
        CASE
            WHEN COALESCE(user_id, '')  <> '' AND COALESCE(group_id, '') <> '' THEN ''
            WHEN COALESCE(user_id, '')  <> '' THEN user_id
            WHEN COALESCE(group_id, '') <> '' THEN group_id
            ELSE ''
        END AS owner_id,
        -- owner_conflict: both a user AND a group on one event. Carried so the grouped
        -- gate below can withhold the whole rollup rather than bill a guessed owner.
        (COALESCE(user_id, '') <> '' AND COALESCE(group_id, '') <> '') AS owner_conflict,
        -- graph_k8s_name: the DynamoGraphDeployment that served the request -- the cost
        -- centre. Carried as EVIDENCE onto the rollup, never a grain key: a shared graph
        -- serves many orgs and has no database row, so without it shared-mode cost is
        -- unattributable. NULL is normal and never withholds money.
        graph_k8s_name,
        usage_found,
        -- aborted / status_code partition the missing-usage bucket by CAUSE (see
        -- the counts below). A client disconnect or an upstream failure with no
        -- usage block is EXPECTED; a SUCCESSFUL response with no usage block is
        -- the alarming case, because the engine served work we cannot bill.
        aborted,
        status_code,
        (prompt_tokens >= 0 AND cached_tokens >= 0 AND completion_tokens >= 0
         AND cached_tokens <= prompt_tokens) AS valid_usage,
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
        ev.resource_id,
        ev.org_id,
        ev.model_id,
        ev.base_model,
        -- serving_mode: a GRAIN key column from here on (see the grouped GROUP BY).
        -- Normalized to '' for dedicated so the key column is never NULL -- UNIQUE
        -- treats NULLs as distinct, so a NULL key column would let one logical rollup
        -- be written twice and double-bill.
        COALESCE(ev.serving_mode, '') AS serving_mode,
        ev.owner_type,
        ev.owner_id,
        ev.owner_conflict,
        ev.graph_k8s_name,
        ev.ev_ts,
        ev.usage_found,
        ev.valid_usage,
        ev.prompt_tokens,
        ev.cached_tokens,
        ev.completion_tokens,
        -- Widen BEFORE subtracting. prompt_tokens and cached_tokens are INTEGER,
        -- so individually valid engine evidence can overflow int32 on the
        -- difference (e.g. 2147483647 - (-2147483648)). This projection runs over
        -- EVERY event in the window before valid_usage filters anything, so an
        -- int32 subtraction here fails the whole hour's rating with 22003 —
        -- one malformed row would block all billing for that window, not just
        -- its own. BIGINT operands keep the row computable; it is then excluded
        -- from money by valid_usage and reported as an invalid-usage attempt.
        GREATEST(ev.prompt_tokens::bigint - ev.cached_tokens::bigint, 0) AS billable_prompt,
        -- The C4 ladder: direct (a) wins; else derived base x premium (b) for
        -- fine-tune traffic; else the plain base rate (c) for a base-model endpoint.
        -- The rd and rpb join guards are mutually exclusive on the fine-tune marker,
        -- so at most one of them is non-NULL and the COALESCE order between them is
        -- documentation, not a tiebreak. A miss on ALL → NULL → UNPRICED (never $0).
        -- Fine-tune traffic with a NULL base_model can only miss its join (NULL =
        -- NULL is never true) and is BARRED from the plain-base join by the marker
        -- guard, so it correctly falls through to UNPRICED and screams.
        -- The book passed in is the one EFFECTIVE DURING THIS HOUR (the caller
        -- rates hour by hour against the manager's effective-dated series), so
        -- re-rating an old hour resolves the same rates it originally did. There is
        -- no local price-freeze table: "never reprice served traffic" holds because
        -- the price series is a function of TIME, not of when the rater last ran.
        COALESCE(rp.prompt_price,     rd.prompt_price,     rpb.prompt_price)     AS prompt_price,
        COALESCE(rp.cached_price,     rd.cached_price,     rpb.cached_price)     AS cached_price,
        COALESCE(rp.completion_price, rd.completion_price, rpb.completion_price) AS completion_price,
        -- Whether this row priced through the DERIVED (base x premium) path (b), or
        -- the PLAIN-BASE path (c). Both key the rate on base_model, so both feed the
        -- single-rate ambiguity gate below.
        (rp.model_id IS NULL AND rd.base_model IS NOT NULL) AS via_derived,
        (rp.model_id IS NULL AND rpb.model_id IS NOT NULL)  AS via_base
    FROM ev
    -- (a) The YAML-projected DIRECT price table (keyed on model_id).
    LEFT JOIN rating_price rp ON rp.model_id = ev.model_id
    -- (b) The DERIVED price table (keyed on base_model): consulted ONLY for
    -- FINE-TUNE traffic — an injected adapter, or the ft: model prefix — that missed
    -- the direct join; base_model prices the fine-tune at base x premium. The
    -- rp.model_id-IS-NULL guard keeps direct-over-derived precedence (a fine-tune
    -- with its own in-file rate is never re-derived); the fine-tune-marker guard
    -- keeps a base-model endpoint from ever resolving through the derived table.
    LEFT JOIN rating_derived rd
        ON rd.base_model = ev.sku_base
       AND rp.model_id IS NULL
       -- The ft: prefix is SINGLE-SOURCED from the Go fineTunePrefix constant, bound as
       -- $3 (ftLikePattern), so the money path has ONE source of truth for what marks a
       -- fine-tune — never a literal 'ft:%' that could drift from the constant. The
       -- adapter presence is the OTHER fine-tune marker (C4): the endpoint serves under
       -- its endpoint name, so the model id alone cannot mark it.
       AND (ev.model_id LIKE $3 OR ev.adapter IS NOT NULL)
    -- (c) The PLAIN-BASE rate for a BASE-MODEL endpoint serving under its endpoint
    -- name (C4): the same direct price table, keyed on ev.base_model — NO premium.
    -- Guarded to NON-fine-tune traffic only (the exact negation of rd's marker
    -- guard), so a fine-tune whose base misses rating_derived can never fall through
    -- to an un-premiumed plain-base rate — it must scream as UNPRICED instead.
    LEFT JOIN rating_price rpb
        ON rpb.model_id = ev.sku_base
       AND rp.model_id IS NULL
       AND NOT (ev.model_id LIKE $3 OR ev.adapter IS NOT NULL)
),
-- grouped: the per-(auth_id, resource_id, model_id, hour) rollup BEFORE the
-- single-rate gate.
-- A rollup stores ONE applied-rate triple, so every priced row in it must have
-- resolved to the SAME rate. Rows that priced through base_model (via_derived or
-- via_base) can violate that two ways, both stamped ambiguous_base:
--   - >1 DISTINCT base_model among them: for an ft: id that is the E3 ft-uniqueness
--     violation (a globally-unique uuid4 checkpoint id can't carry two bases); for
--     an endpoint-name model_id it is the same hazard via C4 (e.g. an endpoint name
--     reused across deployments with different bases inside one window). Either
--     way the rates differ and a blind MIN()-applied-rate would silently bill the
--     rollup at the CHEAPER base — under-billing, counted as rated.
--   - BOTH paths present (some rows derived, some plain-base — the adapter header
--     flapping on one endpoint name): premium and no-premium rates differ even on a
--     SINGLE base_model, and MIN() would again silently pick the cheaper.
-- Both are base_model/adapter PROPAGATION violations: they must SCREAM, not
-- silently pick a rate. Ambiguous rollups are split out below (counted as an
-- anomaly, never upserted). Direct-priced (via model_id) rows never trip the gate —
-- their base_model/adapter never affected their rate.
grouped AS (
    SELECT
        auth_id,
        resource_id,
        model_id,
        -- GRAIN KEY: shared and dedicated price from different SKUs, so they must never
        -- merge into one rollup (a merge would let MIN() below apply the cheaper rate
        -- to both). Already normalized to '' for dedicated in resolved.
        serving_mode,
        -- GRAIN KEYS: the owner pair, so charges are presentable per person/team as
        -- well as per API key. '' / '' means the producer supplied no owner.
        owner_type,
        owner_id,
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
        -- (by the single-rate gate enforced below) single base-and-path, so all
        -- rows share ONE rate; MIN picks it deterministically. The ambiguous_base guard
        -- guarantees MIN is not silently masking a second, different rate.
        MIN(prompt_price)                                AS applied_prompt_rate,
        MIN(cached_price)                                AS applied_cached_rate,
        MIN(completion_price)                            AS applied_completion_rate,
        COUNT(*)::bigint                                 AS event_count,
        -- org_id carried onto the rollup via MAX (NOT a GROUP BY key — org is a function
        -- of resource_id, so it must never split a rollup). MAX ignores NULLs, so a
        -- deployment that metered some events before its org header was wired and some
        -- after collapses to the one non-NULL org (a partial-NULL is NOT ambiguity). The
        -- result is NULL only if EVERY event in the rollup lacked org — then push holds
        -- + screams the rollup, never bills a guessed org. The ambiguous_org guard below
        -- guarantees MAX is not silently masking a SECOND, distinct non-NULL org.
        MAX(org_id)                                      AS org_id,
        -- THE SINGLE-RATE GATE (see the grouped comment): among rows whose rate came
        -- from base_model (derived OR plain-base), >1 distinct base_model — or a MIX
        -- of the two paths (premium vs no-premium on one endpoint name) — means more
        -- than one rate in the rollup → ambiguous. Rows priced directly on model_id
        -- never affect it.
        (COUNT(DISTINCT base_model) FILTER (WHERE via_derived OR via_base) > 1
         OR (bool_or(via_derived) AND bool_or(via_base)))          AS ambiguous_base,
        -- > 1 distinct NON-NULL org_id for one (auth, resource, model, hour) rollup →
        -- ambiguous org. Org is a deployment property, so a resource resolving to two
        -- distinct orgs in one window is an attribution PROPAGATION bug (Atlas injected
        -- conflicting org labels). A blind MAX(org_id) would silently bill the whole
        -- rollup to ONE of them — mis-attribution counted as rated. So ambiguous-org
        -- rollups are split out (counted as an anomaly, never upserted), exactly like
        -- ambiguous_base. A partial-NULL (one real org + missing-header rows) is NOT
        -- ambiguous (DISTINCT over non-NULLs is 1).
        COUNT(DISTINCT org_id) > 1 AS ambiguous_org,
        -- graph_k8s_name carried onto the rollup via MAX (NOT a GROUP BY key -- the
        -- graph is evidence, not identity, and keying on it could split a rollup on a
        -- value that merely propagated late). MAX ignores NULLs, so a rollup whose
        -- events partly predate graph propagation collapses to the one known graph.
        -- NULL only when NO event in the rollup named a graph, which is normal and
        -- never withholds money -- it only means the cost is not pool-attributable.
        --
        -- On a genuine two-graph conflict the value is forced to NULL rather than
        -- letting MAX pick one: an unattributable rollup must not LOOK attributable.
        -- "No graph" and "a guessed graph" read identically downstream, so the honest
        -- one is chosen.
        CASE WHEN COUNT(DISTINCT graph_k8s_name) > 1 THEN NULL
             ELSE MAX(graph_k8s_name) END                AS graph_k8s_name,
        -- > 1 distinct graph for one rollup. Unlike a partial-NULL (which MAX resolves
        -- cleanly), two DIFFERENT non-NULL graphs in one bucket means the traffic was
        -- served by two cost centres and a single MAX would silently attribute all of
        -- it to one. Evidence-only, so this does NOT withhold the money -- the rollup
        -- bills normally with a NULL graph rather than a guessed one, and the count
        -- below surfaces it. (Contrast ambiguous_org, which DOES withhold: org decides
        -- WHO is billed, graph only decides what the cost is attributed against.)
        -- NOTE: there is deliberately NO owner_conflict aggregate here. Conflicted
        -- events are dropped by the WHERE below, per event, so none survives to be
        -- aggregated -- see the comment there for why a group-level gate was wrong.
        COUNT(DISTINCT graph_k8s_name) > 1 AS ambiguous_graph
    FROM resolved
    WHERE usage_found                       -- authoritative engine counts only
      AND valid_usage                       -- malformed legacy evidence never enters money
      AND prompt_price IS NOT NULL          -- priced only
      AND auth_id     IS NOT NULL           -- attributable only
      AND model_id    IS NOT NULL
      -- resource_id is a NON-NULL key column AND the E2 customer-attribution key. A
      -- NULL resource_id row can't name its deployment/org, so it is NEVER billed; it
      -- is excluded here and COUNTED as unattributable below (fail closed).
      AND resource_id IS NOT NULL
      -- OWNER CONFLICT is excluded PER EVENT, here, NOT via a bool_or gate over the
      -- group. This is load-bearing and was a bug once: a conflicted event collapses to
      -- owner_type='' / owner_id='' (the CASE has nowhere else to put it), which is the
      -- SAME bucket as genuine no-owner traffic. A group-level bool_or therefore
      -- withheld every legitimate no-owner rollup that merely SHARED a bucket with one
      -- malformed event -- one bad row zeroing other people's revenue -- and counted
      -- the whole group's events as conflicted, over-reporting the blast radius.
      -- Dropping the row here keeps the damage to exactly the offending event, and the
      -- count below is taken from ev so it reports that one event, not its neighbours.
      AND NOT owner_conflict
    GROUP BY auth_id, owner_type, owner_id, resource_id, model_id, serving_mode,
             date_trunc('hour', ev_ts AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
),
-- owner_conflict is NOT a gate here: conflicted events never reach grouped (they are
-- dropped per event in its WHERE), so there is no group left to filter. ambiguous_graph
-- is not here either -- the graph is evidence, so a conflict nulls the column rather
-- than withholding the money.
priced AS (
    SELECT * FROM grouped
    WHERE NOT ambiguous_base AND NOT ambiguous_org
),
-- RECONCILE (re-rate deletes superseded rollups): a rated_usage row whose
-- (auth_id, resource_id, model_id, window_start) falls IN this run's window but is
-- NOT in the current priced set is STALE — it billed CLEAN in a prior run, but the latest run
-- now excludes it (it became ambiguous-base, or unpriced, or its events vanished).
-- "What the latest run says is what bills," so it is DELETED, atomically with the
-- upsert below, in the SAME snapshot. Without this, an upsert-only re-run leaves the
-- stale row billing at its old cost forever.
--
-- WINDOW PREDICATE: cmd/rater passes HOUR-ALIGNED [$1,$2) bounds (enforced there),
-- and a rollup buckets to the UTC hour of ev_ts for ev_ts in [$1,$2), so every
-- in-scope bucket lies in [$1,$2). Delete ONLY window_start in that half-open range —
-- never an adjacent hour the run did not rate. The run rates the FULL window, so any
-- in-window rated_usage row this run did not reproduce in priced IS superseded; the
-- NOT EXISTS against priced is the exact "not reproduced" test (keyed on the same
-- natural key as the unique constraint). A clean re-run with identical data reproduces
-- every row in priced, so NOTHING matches the delete — idempotent no-op.
deleted AS (
    DELETE FROM rated_usage ru
    WHERE ru.window_start >= $1
      AND ru.window_start <  $2
      AND NOT EXISTS (
          SELECT 1 FROM priced p
          WHERE p.auth_id      = ru.auth_id
            AND p.owner_type   = ru.owner_type
            AND p.owner_id     = ru.owner_id
            AND p.resource_id  = ru.resource_id
            AND p.model_id     = ru.model_id
            AND p.serving_mode = ru.serving_mode
            AND p.window_start = ru.window_start
      )
    RETURNING ru.id
),
upserted AS (
    INSERT INTO rated_usage (
        id, auth_id, owner_type, owner_id, resource_id, org_id, model_id,
        serving_mode, graph_k8s_name, window_start, window_end,
        prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens,
        cost, applied_prompt_rate, applied_cached_rate, applied_completion_rate,
        event_count
    )
    SELECT
        -- DETERMINISTIC 32-char hex surrogate: md5 of the natural key, so re-rating
        -- regenerates the SAME id. The fields are LENGTH-PREFIXED (len || ':' || value)
        -- so the encoding is INJECTIVE — a '|' inside any field can never shift the
        -- boundary and collide two different keys onto one id (e.g. auth 'a|b' +
        -- resource 'c' vs auth 'a' + resource 'b|c'). The field ORDER is FIXED and MUST
        -- equal the unique key (auth_id, owner_type, owner_id, resource_id, model_id,
        -- serving_mode, window_start); epoch (a bounded integer, no separator hazard)
        -- keeps the hash input session-TZ-independent.
        --
        -- owner_type, owner_id and serving_mode are NOT NULL (defaulted to '' upstream),
        -- so length() is never NULL here -- a NULL anywhere in this expression would
        -- make the whole md5 NULL and violate the PK.
        --
        -- WIDENING THIS KEY CHANGES EVERY ID. Migration 0006 widened it from 4 fields to
        -- 7; ids minted before that are not reproducible and were discarded with the
        -- clean cutover. Do not add a field here without the same reckoning.
        md5(length(auth_id)::text || ':' || auth_id
          || '|' || length(owner_type)::text || ':' || owner_type
          || '|' || length(owner_id)::text || ':' || owner_id
          || '|' || length(resource_id)::text || ':' || resource_id
          || '|' || length(model_id)::text || ':' || model_id
          || '|' || length(serving_mode)::text || ':' || serving_mode
          || '|' || extract(epoch FROM window_start)::bigint::text),
        -- org_id is NOT part of the md5 natural key (the key is auth/resource/model/
        -- window): org is DERIVED from resource_id, so a NULL→value org transition must
        -- NOT mint a new id and double-write. It is carried as a data column only.
        auth_id, owner_type, owner_id, resource_id, org_id, model_id,
        serving_mode, graph_k8s_name, window_start, window_end,
        prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens,
        cost, applied_prompt_rate, applied_cached_rate, applied_completion_rate,
        event_count
    FROM priced
    -- Deterministic upsert order so a SINGLE rater never self-deadlocks against its
    -- own rows (it takes row locks in one consistent order). Cross-rater
    -- deadlock-safety (this ordered upsert vs. the deleted CTE's DELETE — a
    -- potential ABBA pair) holds ONLY UNDER SINGLE-FLIGHT: the deployment forbids two
    -- raters over overlapping windows (Atlas CronJob concurrencyPolicy: Forbid), so
    -- the cross-rater hazard is unreachable and no delete-lock-ordering machinery is
    -- added here. See cmd/rater's package doc for the single-flight contract.
    ORDER BY auth_id, owner_type, owner_id, resource_id, model_id, serving_mode, window_start
    ON CONFLICT (auth_id, owner_type, owner_id, resource_id, model_id, serving_mode, window_start) DO UPDATE SET
        -- Refresh org_id on re-rate, but NEVER erase a known org: COALESCE prefers the
        -- new snapshot's org and FALLS BACK to the existing row's org when the new one is
        -- NULL. So a rollup first written with a NULL org (header not yet wired) picks up
        -- the real org on a later re-rate (NULL -> real, convergence), but a re-rate over
        -- a window whose snapshot has since LOST its org headers (e.g. a forced replay
        -- from a stale billing_event range) can NOT overwrite a prior good org with NULL
        -- (real -> NULL would silently un-attribute already-billed usage). The only
        -- legitimate "org changed" case (real A -> real B for one resource) is NOT a
        -- silent re-rate flip: it is a distinct-org collision the ambiguous_org guard
        -- above already withholds + screams, so it never reaches this UPDATE.
        org_id                  = COALESCE(EXCLUDED.org_id, rated_usage.org_id),
        -- PLAIN OVERWRITE, deliberately NOT the COALESCE-never-erase treatment org_id
        -- gets one line above. The asymmetry is the whole point.
        --
        -- org_id can COALESCE safely because a real -> NULL org transition never reaches
        -- this UPDATE: the only way one org becomes another is a distinct-org collision,
        -- and ambiguous_org WITHHOLDS that rollup from priced entirely. So a NULL in
        -- EXCLUDED.org_id can only ever mean "header not wired yet", never "we now know
        -- this is unattributable".
        --
        -- graph is different precisely BECAUSE ambiguous_graph does not withhold: a
        -- rollup that spans two graphs still bills, with the column nulled (see the
        -- CASE in grouped) so an unattributable cost does not LOOK attributable. That
        -- NULL therefore DOES reach this UPDATE, and it is a real finding, not a gap.
        -- COALESCE here would restore the stale single-graph value and silently undo
        -- the nulling -- leaving the rollup asserting a cost centre the rater has just
        -- determined it cannot name.
        --
        -- The cost of the plain overwrite is that a re-rate over a window whose events
        -- no longer carry a graph downgrades a known graph to NULL. That is the honest
        -- direction to fail: the rollup then says "unknown" rather than asserting a
        -- graph this run could not confirm. Evidence only -- it never changes the money.
        graph_k8s_name          = EXCLUDED.graph_k8s_name,
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
    -- Stale rollups DELETED by the reconcile (re-rate convergence). Rows that billed
    -- in a prior run but fell out of priced this run; surfaced so a re-rate that
    -- supersedes prior billing is observable, not silent.
    (SELECT COUNT(*)::bigint FROM deleted)                       AS reconciled_deletions,
    -- Anomaly counts from the SAME snapshot as the upsert. An unattributable row is
    -- counted ONLY as unattributable (the more specific signal), never also as
    -- unpriced; likewise an ambiguous_org rollup that is ALSO ambiguous_base is counted
    -- ONLY as ambiguous_base. So the counts strictly PARTITION the in-window rows:
    --   events_rated + missing_usage + invalid_usage + unpriced + unattributable + ambiguous_base + ambiguous_org
    --     == total in-window events.
    -- The unpriced count requires FULL attribution (auth_id, resource_id, model_id all
    -- NON-NULL) for exactly this exclusivity: a NULL-resource_id row that is also
    -- unpriced must be counted ONLY as unattributable, never double-counted here.
    (SELECT COUNT(*)::bigint FROM resolved
      WHERE usage_found
        AND valid_usage
        AND prompt_price  IS NULL
        AND auth_id     IS NOT NULL
        AND resource_id IS NOT NULL
        AND model_id    IS NOT NULL)                          AS unpriced_events,
    (SELECT COUNT(*)::bigint FROM ev
      WHERE usage_found
        AND valid_usage
        AND (auth_id IS NULL OR resource_id IS NULL OR model_id IS NULL)) AS unattributable_events,
    -- Missing engine usage is its own exclusive audit bucket. It must not become a
    -- zero-token rated rollup or be misreported as an attribution/price failure.
    -- This is the TOTAL, reported for reconciliation; the paging decision uses the
    -- cause-partitioned counts below, not this one.
    (SELECT COUNT(*)::bigint FROM ev WHERE NOT usage_found)            AS missing_usage_events,
    -- EXPECTED missing usage: the client disconnected (aborted) or the attempt
    -- terminated with a non-success status. Routine internet, correctly billed
    -- zero, reviewed in billing_reconciliation_hourly — NOT paged.
    (SELECT COUNT(*)::bigint FROM ev
      WHERE NOT usage_found
        AND (aborted OR (status_code IS NOT NULL AND status_code >= 400)))
                                                                       AS expected_missing_usage_events,
    -- UNEXPLAINED missing usage: the attempt was NOT aborted and did NOT fail, so
    -- the engine reported success while telling us nothing about tokens. We may
    -- have served real work that cannot be billed. This is the fail-loud bucket.
    -- A NULL status_code counts here: an attempt we cannot prove failed is not
    -- allowed to be silently excused (fail closed).
    (SELECT COUNT(*)::bigint FROM ev
      WHERE NOT usage_found
        AND NOT aborted
        AND (status_code IS NULL OR status_code < 400))
                                                                       AS unexplained_missing_usage_events,
    -- Retain invalid authoritative engine evidence in billing_event for repair,
    -- but never let malformed counts enter
    -- money or overlap another anomaly bucket.
    (SELECT COUNT(*)::bigint FROM ev
      WHERE usage_found AND NOT valid_usage)                           AS invalid_usage_events,
    -- AMBIGUOUS-BASE events: the EVENT count under ambiguous rollups (a single
    -- model_id whose base_model-priced rows carried >1 rate in one window — >1
    -- distinct base_model, or mixed premium/plain-base pricing; see the grouped
    -- comment). These rollups are NOT upserted (excluded from priced),
    -- so their events are neither rated nor $0-billed; they are counted here, from the
    -- SAME snapshot, and drive the fail-loud exit. SUM(event_count) (not COUNT(*) of
    -- rollups) so the partition identity above stays in EVENT units.
    (SELECT COALESCE(SUM(event_count), 0)::bigint FROM grouped
      WHERE ambiguous_base)                                  AS ambiguous_base_events,
    -- AMBIGUOUS-ORG events: the EVENT count under rollups carrying >1 distinct non-NULL
    -- org_id (an E2 attribution propagation bug — one resource resolving to two orgs in
    -- a window). Excluded from priced (NOT billed to a guessed org), counted here from
    -- the same snapshot to drive the fail-loud exit. Same EVENT-unit convention as
    -- ambiguous_base. EXCLUSIVE of ambiguous_base (AND NOT ambiguous_base) so the
    -- anomaly counts stay a strict PARTITION: a rollup that is BOTH base- and
    -- org-ambiguous is counted ONLY as ambiguous_base (the more specific E3 signal),
    -- exactly as unattributable takes precedence over unpriced above. So
    --   events_rated + missing_usage + invalid_usage + unpriced + unattributable
    --     + ambiguous_base + ambiguous_org + owner_conflict == total in-window events
    -- holds with no double-count. Both still drive exit-nonzero, so precedence changes
    -- only which bucket reports the overlap, never whether it screams.
    (SELECT COALESCE(SUM(event_count), 0)::bigint FROM grouped
      WHERE ambiguous_org AND NOT ambiguous_base)           AS ambiguous_org_events,
    -- OWNER-CONFLICT events: the EVENT count under rollups where some event carried
    -- BOTH a user and a group. Upstream an identity is a user XOR a group, so both
    -- set is a producer bug and the owner cannot be determined. Excluded from priced
    -- (never billed to a guessed owner), counted here from the same snapshot to drive
    -- the fail-loud exit. The raw events remain in billing_event as evidence — retain
    -- raw, exclude from money, alarm (Hugo, 2026-09-23).
    --
    -- LAST in the precedence chain (AND NOT the two above) so the partition stays
    -- strict: a rollup that is both owner-conflicted and base-ambiguous is counted
    -- ONLY as ambiguous_base. Same EVENT-unit convention throughout.
    -- Counted from ev (PER EVENT), not from grouped: a conflicted event is dropped
    -- before grouping, so it has no rollup to be summed under. This also keeps the
    -- count honest -- exactly the events that carried both owners, never the innocent
    -- neighbours that happened to share their bucket.
    --
    -- The attribution filters mirror the unattributable/unpriced buckets so the
    -- partition stays strict: a conflicted row that ALSO lacks auth/resource/model is
    -- counted once, as unattributable (the more specific upstream failure).
    (SELECT COUNT(*)::bigint FROM ev
      WHERE owner_conflict
        AND usage_found
        AND valid_usage
        AND auth_id     IS NOT NULL
        AND resource_id IS NOT NULL
        AND model_id    IS NOT NULL)                        AS owner_conflict_events,
    -- AMBIGUOUS-GRAPH rollups: >1 distinct serving graph in one rollup. NOT part of
    -- the event partition above and NOT a withholding gate — the graph is evidence,
    -- so the rollup BILLS NORMALLY with a NULL graph rather than a guessed one. Counted
    -- as ROLLUPS (not events) precisely because these rows ARE rated: their events are
    -- already inside events_rated, and counting them again in event units would break
    -- the partition identity. Surfaced so lost cost attribution is observable.
    (SELECT COUNT(*)::bigint FROM priced
      WHERE ambiguous_graph)                                AS ambiguous_graph_rollups`

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
		Scan(&res.RollupsWritten, &res.EventsRated, &total, &res.ReconciledDeletions,
			&res.UnpricedEvents, &res.UnattributableEvents, &res.MissingUsageEvents,
			&res.ExpectedMissingUsageEvents, &res.UnexplainedMissingUsageEvents,
			&res.InvalidUsageEvents, &res.AmbiguousBaseEvents,
			&res.AmbiguousOrgEvents, &res.OwnerConflictEvents,
			&res.AmbiguousGraphRollups)
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
