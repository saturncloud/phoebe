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

// Store is the aggregator's data seam: an interface so the orchestration can be
// unit-tested against a fake and the Postgres SQL tested in isolation.
type Store interface {
	// AggregateWindow rolls billing_event in [start, end) into rated_usage and
	// reports the rollups and the anomaly counts, all from one statement.
	AggregateWindow(ctx context.Context, start, end time.Time) (AggregateResult, error)
	Ping(ctx context.Context) error
	Close() error
}

// AggregateResult is what one aggregation statement reports: what it wrote and,
// from the SAME snapshot, every in-window event it kept out of rated_usage and
// why. The event counts partition the window:
//
//	EventsAggregated + MissingUsage + InvalidUsage + Unattributable
//	  + InvalidServingMode + OwnerConflict
//	  + AmbiguousBase + AmbiguousOrg == in-window billing_event rows
//
// int64 throughout: these are COUNT/SUM over an arbitrary backfill window.
type AggregateResult struct {
	RollupsWritten   int64
	EventsAggregated int64
	// ReconciledDeletions counts in-window rated_usage rows this run DELETED
	// because it no longer produces them (they became withheld, or their events
	// vanished). "What the latest run says is what bills."
	ReconciledDeletions int64

	// MissingUsageEvents are attempts for which the engine supplied no usage
	// block; zero-quantity audit evidence, never aggregated. The TOTAL is
	// reported, not paged; the two shares below split it by cause.
	MissingUsageEvents int64
	// ExpectedMissingUsageEvents: the client aborted, or the attempt failed.
	ExpectedMissingUsageEvents int64
	// UnexplainedMissingUsageEvents: NOT aborted and NOT failed, so the engine
	// reported success with no tokens. The paging signal. A NULL status_code
	// counts here (fail closed).
	UnexplainedMissingUsageEvents int64
	// InvalidUsageEvents: authoritative counts that break the token invariants
	// (negative, or cached > prompt). Retained, never aggregated.
	InvalidUsageEvents int64
	// UnattributableEvents: no auth_id, resource_id or model_id (NULL or ''). The
	// record could not name who used what, so it is never sent.
	UnattributableEvents int64
	// InvalidServingModeEvents: serving_mode is neither 'shared' nor 'dedicated'.
	InvalidServingModeEvents int64
	// OwnerConflictEvents: the event carried BOTH a user and a group. An identity
	// is one or the other (Hugo, 2026-09-23: retain raw, exclude, alarm).
	OwnerConflictEvents int64
	// AmbiguousBaseEvents: events of an identity (auth, owner, resource, model,
	// serving mode, hour) whose events named more than one base_model or adapter,
	// or mixed a value with none (base model on some events and not others, or
	// adapter on some and not others). A record carries one of each, and the
	// manager prices by them, so picking one would misprice the rest.
	AmbiguousBaseEvents int64
	// AmbiguousOrgEvents: events of an identity whose events named more than one
	// non-NULL org. The org is the payer; a guess would bill the wrong customer.
	AmbiguousOrgEvents int64

	// EmptyBaseModelEvents counts AGGREGATED events with no base_model. Not
	// withheld and not an anomaly: Atlas stamps X-Saturn-Base-Model as
	// `base_model | default('')`, and a dedicated endpoint may serve a model with
	// no catalog base (D3). The record is sent with base_model "" and the manager
	// prices it (dedicated: the plan's default rate; shared: reported unpriced).
	// Withholding it would hold back every token record of the hour. Part of
	// EventsAggregated, so it is not a partition bucket.
	EmptyBaseModelEvents int64

	// UnknownRevisionRollups counts written rollups that name a graph but carry
	// topology revision 0. Not an anomaly: the quantities and payer are right;
	// only margin attribution of that slice of the hour is lost.
	UnknownRevisionRollups int64
}

// PostgresStore reads billing_event and writes rated_usage in phoebe's own
// Postgres. It does not run migrations.
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
// already sets one. Belt-and-braces only: the SQL is written to be
// session-TZ-independent (see the bucketing expression in aggregateWindowSQL).
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

// NewPostgresStore wraps an existing *sql.DB. Used by tests and callers owning
// the pool lifecycle.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }
func (s *PostgresStore) Close() error                   { return s.db.Close() }

// aggregateWindowSQL filters, groups, upserts, reconciles and counts in ONE
// statement, so a billing_event the drainer commits mid-run is visible to both
// the rollups and the counts or to neither, never kept out but uncounted.
//
// THE GRAIN is billing-design.md section 3's token grain, unchanged from
// migration 0009: (auth_id, owner_type, owner_id, resource_id, model_id,
// serving_mode, graph_k8s_name, topology_revision, hour). It is rated_usage's
// unique key. org_id, region, base_model and adapter are carried per rollup,
// not keyed.
//
// NO MONEY. The manager rates (D1). cost, applied_*_rate and price_key are
// written NULL (migration 0011), on insert and on update, so a row the
// previous rater priced does not keep a stale price after it is re-aggregated.
//
// EVERY FILTER IS PER EVENT (billable below), and the payer and pricing-fact
// gates (gate below) are computed over the identity WITHOUT graph and revision,
// both as the rater has done since 0009: who pays and what the tokens are
// priced by are properties of the deployment, not of the graph revision that
// served it, so a conflict across a graph boundary must still be caught.
//
// RECONCILE: an in-window rated_usage row this run does not reproduce is
// DELETED in the same statement, so a rollup that later became withheld (or
// whose events vanished) stops being sent. token-push then omits it and the
// manager deletes it by absence.
//
// HOUR BUCKET is session-TZ-independent (date_trunc on a UTC wall clock).
//
// $1 = window start (inclusive), $2 = window end (exclusive), both hour-aligned.
const aggregateWindowSQL = `
WITH ev AS (
    SELECT
        -- '' is as unattributable as NULL: the wire requires each of these to be
        -- non-empty, so an empty one could never be sent.
        NULLIF(auth_id, '')     AS auth_id,
        NULLIF(resource_id, '') AS resource_id,
        org_id,
        -- billing_event.model is the engine-reported model name: the record's model_id.
        NULLIF(model, '')       AS model_id,
        -- base_model and adapter are the facts the manager prices tokens by (with
        -- serving_mode). '' means absent, exactly like NULL. An absent base_model
        -- is NOT withheld: it is sent as "" and the manager prices it (D3).
        NULLIF(base_model, '')  AS base_model,
        NULLIF(adapter, '')     AS adapter,
        serving_mode,
        -- Since the 2026-09-29 ruling 'shared' and 'dedicated' are the only legal
        -- values (migration 0007 rewrote pre-cutover NULL/''). Anything else is a
        -- missed backfill or a producer bug: withheld per event and counted.
        COALESCE(serving_mode IN ('shared', 'dedicated'), false) AS valid_serving_mode,
        -- The owner pair. An identity is a user XOR a group; both set is a producer
        -- bug, collapsed to ('', '') here and withheld per event in billable.
        CASE
            WHEN COALESCE(user_id, '') <> '' AND COALESCE(group_id, '') <> '' THEN ''
            WHEN COALESCE(user_id, '') <> '' THEN 'user'
            WHEN COALESCE(group_id, '') <> '' THEN 'group'
            ELSE ''
        END AS owner_type,
        CASE
            WHEN COALESCE(user_id, '') <> '' AND COALESCE(group_id, '') <> '' THEN ''
            WHEN COALESCE(user_id, '') <> '' THEN user_id
            WHEN COALESCE(group_id, '') <> '' THEN group_id
            ELSE ''
        END AS owner_id,
        (COALESCE(user_id, '') <> '' AND COALESCE(group_id, '') <> '') AS owner_conflict,
        -- Grain keys since 0009, normalized to the '' / 0 "unknown" sentinels
        -- because a key column may not be NULL. Unknown is normal, never withheld.
        COALESCE(graph_k8s_name, '') AS graph_k8s_name,
        COALESCE(topology_revision, 0) AS topology_revision,
        region,
        usage_found,
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
-- The events that may be aggregated, filtered ONCE so the gates and the rollups
-- read exactly the same rows. Every filter is per event: a group-level gate would
-- withhold innocent neighbours that merely share a bucket with a bad row.
billable AS (
    SELECT *,
        date_trunc('hour', ev_ts AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS window_start
    FROM ev
    WHERE usage_found
      AND valid_usage
      AND auth_id     IS NOT NULL
      AND resource_id IS NOT NULL
      AND model_id    IS NOT NULL
      AND valid_serving_mode
      AND NOT owner_conflict
),
-- Gates over the identity WITHOUT graph and revision (see the statement comment).
gate AS (
    SELECT
        auth_id, owner_type, owner_id, resource_id, model_id, serving_mode, window_start,
        -- org_id is carried, never keyed (it is a function of the resource). MAX
        -- ignores NULLs, so events from before the org header collapse onto the
        -- one real org; ambiguous_org guarantees MAX is not hiding a second one.
        MAX(org_id) AS org_id,
        -- A record carries ONE base_model and ONE adapter, and the manager prices
        -- by them. Two bases, two adapters, or a value on some events and none on
        -- others (catalog base versus default rate; premium versus no premium)
        -- mean the identity's tokens would be priced two ways: withheld and
        -- counted, never sent with one of them. A base_model that is absent on
        -- EVERY event of the identity is not ambiguous; it is sent as "".
        (COUNT(DISTINCT base_model) > 1
         OR (bool_or(base_model IS NULL) AND bool_or(base_model IS NOT NULL))
         OR COUNT(DISTINCT adapter) > 1
         OR (bool_or(adapter IS NULL) AND bool_or(adapter IS NOT NULL))) AS ambiguous_base,
        -- Two distinct non-NULL orgs for one deployment: an attribution bug. A
        -- partial NULL (one real org plus header-less events) is not ambiguous.
        COUNT(DISTINCT org_id) > 1 AS ambiguous_org
    FROM billable
    GROUP BY auth_id, owner_type, owner_id, resource_id, model_id, serving_mode, window_start
),
grouped_raw AS (
    SELECT
        auth_id, resource_id, model_id, serving_mode, owner_type, owner_id,
        graph_k8s_name, topology_revision, window_start,
        window_start + interval '1 hour'                 AS window_end,
        SUM(prompt_tokens)::bigint                       AS prompt_tokens,
        SUM(cached_tokens)::bigint                       AS cached_tokens,
        SUM(completion_tokens)::bigint                   AS completion_tokens,
        -- Kept for the reconciliation view (rated_fresh_input_tokens); not sent.
        -- BIGINT before subtracting: INTEGER operands can overflow on the difference.
        SUM(GREATEST(prompt_tokens::bigint - cached_tokens::bigint, 0))::bigint
                                                         AS billable_prompt_tokens,
        -- request_count on the wire: one billing_event row per forwarded inference
        -- attempt (ruling 2026-10-15), so the row count is the attempt count.
        COUNT(*)::bigint                                 AS event_count,
        -- region is operator-editable and can change inside one revision. When the
        -- events disagree it is NULL (sent as ''), never a guess.
        CASE WHEN COUNT(DISTINCT region) > 1 THEN NULL ELSE MAX(region) END AS region,
        -- Single-valued by the gate (ambiguous identities are withheld below).
        MAX(base_model)                                  AS base_model,
        MAX(adapter)                                     AS adapter
    FROM billable
    GROUP BY auth_id, owner_type, owner_id, resource_id, model_id, serving_mode,
             graph_k8s_name, topology_revision, window_start
),
grouped AS (
    SELECT r.*, g.org_id, g.ambiguous_base, g.ambiguous_org
    FROM grouped_raw r
    JOIN gate g
      ON  g.auth_id      = r.auth_id
      AND g.owner_type   = r.owner_type
      AND g.owner_id     = r.owner_id
      AND g.resource_id  = r.resource_id
      AND g.model_id     = r.model_id
      AND g.serving_mode = r.serving_mode
      AND g.window_start = r.window_start
),
kept AS (
    SELECT * FROM grouped
    WHERE NOT ambiguous_base AND NOT ambiguous_org
),
deleted AS (
    DELETE FROM rated_usage ru
    WHERE ru.window_start >= $1
      AND ru.window_start <  $2
      AND NOT EXISTS (
          SELECT 1 FROM kept k
          WHERE k.auth_id           = ru.auth_id
            AND k.owner_type        = ru.owner_type
            AND k.owner_id          = ru.owner_id
            AND k.resource_id       = ru.resource_id
            AND k.model_id          = ru.model_id
            AND k.serving_mode      = ru.serving_mode
            AND k.graph_k8s_name    = ru.graph_k8s_name
            AND k.topology_revision = ru.topology_revision
            AND k.window_start      = ru.window_start
      )
    RETURNING ru.id
),
upserted AS (
    INSERT INTO rated_usage (
        id, auth_id, owner_type, owner_id, resource_id, org_id, model_id,
        serving_mode, graph_k8s_name, topology_revision, region, window_start, window_end,
        prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens,
        cost, applied_prompt_rate, applied_cached_rate, applied_completion_rate,
        event_count, base_model, adapter, price_key
    )
    SELECT
        -- Deterministic local surrogate: md5 of the LENGTH-PREFIXED natural key
        -- (injective, so a '|' inside a field cannot collide two keys), unchanged
        -- since 0009. It is phoebe's row id only; the wire's usage_id is computed
        -- by token-push from the record's own grain (internal/meteredusage).
        md5(length(auth_id)::text || ':' || auth_id
          || '|' || length(owner_type)::text || ':' || owner_type
          || '|' || length(owner_id)::text || ':' || owner_id
          || '|' || length(resource_id)::text || ':' || resource_id
          || '|' || length(model_id)::text || ':' || model_id
          || '|' || length(serving_mode)::text || ':' || serving_mode
          || '|' || length(graph_k8s_name)::text || ':' || graph_k8s_name
          || '|' || topology_revision::text
          || '|' || extract(epoch FROM window_start)::bigint::text),
        auth_id, owner_type, owner_id, resource_id, org_id, model_id,
        serving_mode, graph_k8s_name, topology_revision, region, window_start, window_end,
        prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens,
        NULL, NULL, NULL, NULL,
        event_count, base_model, adapter, NULL
    FROM kept
    -- Deterministic order so one aggregator never self-deadlocks against its own
    -- rows. Two concurrent aggregators are excluded by the deployment (CronJob
    -- concurrencyPolicy: Forbid); see cmd/rater's single-flight note.
    ORDER BY auth_id, owner_type, owner_id, resource_id, model_id, serving_mode,
             graph_k8s_name, topology_revision, window_start
    ON CONFLICT (auth_id, owner_type, owner_id, resource_id, model_id, serving_mode,
                 graph_k8s_name, topology_revision, window_start) DO UPDATE SET
        -- Never erase a known org: a re-run whose events lost their org header
        -- keeps the org already recorded. A real org CHANGE is ambiguous_org and
        -- never reaches here.
        org_id                  = COALESCE(EXCLUDED.org_id, rated_usage.org_id),
        -- Plain overwrites: the latest run says what served the hour.
        region                  = EXCLUDED.region,
        window_end              = EXCLUDED.window_end,
        prompt_tokens           = EXCLUDED.prompt_tokens,
        cached_tokens           = EXCLUDED.cached_tokens,
        completion_tokens       = EXCLUDED.completion_tokens,
        billable_prompt_tokens  = EXCLUDED.billable_prompt_tokens,
        event_count             = EXCLUDED.event_count,
        base_model              = EXCLUDED.base_model,
        adapter                 = EXCLUDED.adapter,
        -- Clear what the previous rater priced: no stale money on an
        -- aggregated row.
        cost                    = NULL,
        applied_prompt_rate     = NULL,
        applied_cached_rate     = NULL,
        applied_completion_rate = NULL,
        price_key               = NULL,
        rated_at                = now()
    RETURNING event_count, graph_k8s_name, topology_revision, base_model
)
SELECT
    (SELECT COUNT(*)::bigint                      FROM upserted) AS rollups_written,
    (SELECT COALESCE(SUM(event_count), 0)::bigint FROM upserted) AS events_aggregated,
    (SELECT COUNT(*)::bigint                      FROM deleted)  AS reconciled_deletions,
    -- The withheld buckets, in precedence order so they PARTITION the window
    -- (see AggregateResult): each event is counted in the first bucket it fits.
    (SELECT COUNT(*)::bigint FROM ev WHERE NOT usage_found)       AS missing_usage_events,
    (SELECT COUNT(*)::bigint FROM ev
      WHERE NOT usage_found
        AND (aborted OR (status_code IS NOT NULL AND status_code >= 400)))
                                                                  AS expected_missing_usage_events,
    (SELECT COUNT(*)::bigint FROM ev
      WHERE NOT usage_found
        AND NOT aborted
        AND (status_code IS NULL OR status_code < 400))           AS unexplained_missing_usage_events,
    (SELECT COUNT(*)::bigint FROM ev WHERE usage_found AND NOT valid_usage)
                                                                  AS invalid_usage_events,
    (SELECT COUNT(*)::bigint FROM ev
      WHERE usage_found AND valid_usage
        AND (auth_id IS NULL OR resource_id IS NULL OR model_id IS NULL))
                                                                  AS unattributable_events,
    (SELECT COUNT(*)::bigint FROM ev
      WHERE usage_found AND valid_usage
        AND auth_id IS NOT NULL AND resource_id IS NOT NULL AND model_id IS NOT NULL
        AND NOT valid_serving_mode)                               AS invalid_serving_mode_events,
    (SELECT COUNT(*)::bigint FROM ev
      WHERE usage_found AND valid_usage
        AND auth_id IS NOT NULL AND resource_id IS NOT NULL AND model_id IS NOT NULL
        AND valid_serving_mode
        AND owner_conflict)                                       AS owner_conflict_events,
    -- Event units (SUM(event_count)), so the partition holds. A rollup that is
    -- both base- and org-ambiguous is counted once, as ambiguous_base.
    (SELECT COALESCE(SUM(event_count), 0)::bigint FROM grouped WHERE ambiguous_base)
                                                                  AS ambiguous_base_events,
    (SELECT COALESCE(SUM(event_count), 0)::bigint FROM grouped
      WHERE ambiguous_org AND NOT ambiguous_base)                 AS ambiguous_org_events,
    (SELECT COALESCE(SUM(event_count), 0)::bigint FROM upserted
      WHERE base_model IS NULL)                                   AS empty_base_model_events,
    (SELECT COUNT(*)::bigint FROM upserted
      WHERE graph_k8s_name <> '' AND topology_revision = 0)      AS unknown_revision_rollups`

// AggregateWindow runs aggregateWindowSQL for [start, end) in one transaction.
func (s *PostgresStore) AggregateWindow(ctx context.Context, start, end time.Time) (AggregateResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AggregateResult{}, fmt.Errorf("rating: begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	var res AggregateResult
	err = tx.QueryRowContext(ctx, aggregateWindowSQL, start.UTC(), end.UTC()).
		Scan(&res.RollupsWritten, &res.EventsAggregated, &res.ReconciledDeletions,
			&res.MissingUsageEvents, &res.ExpectedMissingUsageEvents, &res.UnexplainedMissingUsageEvents,
			&res.InvalidUsageEvents, &res.UnattributableEvents, &res.InvalidServingModeEvents,
			&res.OwnerConflictEvents,
			&res.AmbiguousBaseEvents, &res.AmbiguousOrgEvents,
			&res.EmptyBaseModelEvents, &res.UnknownRevisionRollups)
	if err != nil {
		return AggregateResult{}, fmt.Errorf("rating: aggregate window [%s,%s): %w",
			start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339), err)
	}
	if err := tx.Commit(); err != nil {
		return AggregateResult{}, fmt.Errorf("rating: commit: %w", err)
	}
	committed = true
	return res, nil
}
