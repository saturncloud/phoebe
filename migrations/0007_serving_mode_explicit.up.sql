-- Serving mode is spelled 'shared' or 'dedicated' everywhere (Hugo's 2026-09-29
-- ruling, made in the pre-production window). Before this migration a dedicated
-- rollup stored serving_mode = '' (the absence of the shared: price-key prefix).
-- From here on the empty string is not a serving mode: the proxy resolves a
-- dedicated route to 'dedicated' before metering, the rater withholds any event
-- whose serving mode is not one of the two values, the push sends the explicit
-- value, and saturn-aws-manager rejects '' on the wire.
--
-- The price-key grammar is NOT changed: a dedicated price row is still the bare
-- base id and a shared one is 'shared:<base>'. Only the serving_mode VALUE moves.
--
-- WHY THE ID IS RECOMPUTED. rated_usage.id is md5 of the natural key, and
-- serving_mode is part of that key (see internal/rating/store.go, the upserted
-- CTE). A row rewritten to 'dedicated' must therefore also get the id the rater
-- would now compute for it; otherwise a later re-rate of the same window would
-- update the row in place (ON CONFLICT on the natural key) but keep an id no
-- other code can reproduce. The expression below is the rater's, character for
-- character. There are no production rows (the ruling's premise), so changing
-- dedicated ids is allowed; saturn-aws-manager sees the new id on its next push
-- of the window and replaces the old record by delete-by-absence.
--
-- TWO CLEAN-UPS HAPPEN FIRST:
--  1. A '' row whose natural-key twin already exists as 'dedicated' cannot be
--     renamed without violating the unique key. The '' row is deleted; the
--     'dedicated' twin stays. Twins are EXPECTED, not hypothetical: the ratified
--     deploy order ships Atlas #6709 first, and Atlas then stamps
--     X-Saturn-Serving-Mode: dedicated while the old phoebe is still running. The
--     old rater groups the NULL (pre-stamp) events as '' and the stamped events as
--     'dedicated', so every hour that straddles the Atlas rollout, and every hour
--     after it until this migration runs, can hold both rows. The surviving
--     'dedicated' row covers only PART of its hour until that hour is re-rated:
--     the deleted '' row's events (backfilled to 'dedicated' below) are counted
--     again only by a re-rate of that hour.
--  2. A row with any other value (neither '', 'shared' nor 'dedicated') has no
--     legal meaning and is deleted.
-- The raw evidence for both remains in billing_event.
-- Before deleting, the migration prints a RAISE NOTICE with the row count, SUM(cost),
-- SUM(event_count) and the earliest window_start for each clean-up that finds rows;
-- that NOTICE output is the audit trail for the deleted rows. The routine rater
-- rebuilds them only for hours inside its trailing window (24h by default). Hours
-- older than that MUST be re-rated explicitly with `rater --since <earliest
-- window_start> --until <now>`, then pushed with token-push over the same window
-- (see "Rollout order for migration 0007" in migrations/README.md).
--
-- PRE-CUTOVER EVIDENCE IS DEDICATED (Hugo, 2026-09-30: "assume it's all dedicated
-- (which is true)"). Before this migration the proxy stored dedicated traffic with a
-- NULL serving_mode (the header is absent on dedicated routes), and '' is the other
-- historical spelling. Every such billing_event row IS dedicated traffic, so the
-- first statement below rewrites it to 'dedicated'. This is a deliberate, one-time,
-- ratified exception to "billing_event is never edited": it records what the row
-- always meant in the vocabulary the rater now reads. Without it the rater would
-- withhold that evidence, the trailing re-rate would reconcile-delete the rows
-- renamed below, and the rater would page for a day.
--
-- With the backfill, a re-rate of any pre-cutover window reproduces exactly the
-- rows this migration renames: same serving_mode, same id, no reconcile deletion.
--
-- The UPDATE is idempotent. Events metered by old proxy pods between this
-- migration and the end of the rollout also carry NULL; re-running the same
-- statement once the rollout finishes covers them (see "Serving-mode cutover
-- (migration 0007)" in docs/billing-reconciliation.md).

-- ROLLBACK: the down migration maps 'dedicated' back to the pre-0007 spellings in
-- both tables (NULL in billing_event, '' in rated_usage). See its header.

UPDATE billing_event
SET serving_mode = 'dedicated'
WHERE serving_mode IS NULL OR serving_mode = '';

DO $$
DECLARE
    n       bigint;
    cost_s  numeric;
    evts_s  bigint;
    first_w timestamptz;
BEGIN
    SELECT COUNT(*), COALESCE(SUM(ru.cost), 0), COALESCE(SUM(ru.event_count), 0),
           MIN(ru.window_start)
      INTO n, cost_s, evts_s, first_w
      FROM rated_usage ru
     WHERE ru.serving_mode = ''
       AND EXISTS (
           SELECT 1 FROM rated_usage d
           WHERE d.serving_mode = 'dedicated'
             AND d.auth_id      = ru.auth_id
             AND d.owner_type   = ru.owner_type
             AND d.owner_id     = ru.owner_id
             AND d.resource_id  = ru.resource_id
             AND d.model_id     = ru.model_id
             AND d.window_start = ru.window_start
       );
    IF n > 0 THEN
        RAISE NOTICE '0007: deleting % rated_usage rows with serving_mode '''' that have a ''dedicated'' twin (sum cost %, sum event_count %, earliest window_start %); the ''dedicated'' twins are partial until re-rated, so every affected hour MUST be re-rated: rater --since <earliest window_start> --until <now>, then token-push over the same window',
            n, cost_s, evts_s, to_char(first_w AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
    END IF;

    SELECT COUNT(*), COALESCE(SUM(cost), 0), COALESCE(SUM(event_count), 0),
           MIN(window_start)
      INTO n, cost_s, evts_s, first_w
      FROM rated_usage
     WHERE serving_mode NOT IN ('', 'shared', 'dedicated');
    IF n > 0 THEN
        RAISE NOTICE '0007: deleting % rated_usage rows with an unknown serving_mode (sum cost %, sum event_count %, earliest window_start %)',
            n, cost_s, evts_s, to_char(first_w AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
    END IF;
END
$$;

DELETE FROM rated_usage ru
WHERE ru.serving_mode = ''
  AND EXISTS (
      SELECT 1 FROM rated_usage d
      WHERE d.serving_mode = 'dedicated'
        AND d.auth_id      = ru.auth_id
        AND d.owner_type   = ru.owner_type
        AND d.owner_id     = ru.owner_id
        AND d.resource_id  = ru.resource_id
        AND d.model_id     = ru.model_id
        AND d.window_start = ru.window_start
  );

DELETE FROM rated_usage
WHERE serving_mode NOT IN ('', 'shared', 'dedicated');

UPDATE rated_usage
SET serving_mode = 'dedicated',
    id = md5(length(auth_id)::text || ':' || auth_id
          || '|' || length(owner_type)::text || ':' || owner_type
          || '|' || length(owner_id)::text || ':' || owner_id
          || '|' || length(resource_id)::text || ':' || resource_id
          || '|' || length(model_id)::text || ':' || model_id
          || '|' || length('dedicated')::text || ':' || 'dedicated'
          || '|' || extract(epoch FROM window_start)::bigint::text)
WHERE serving_mode = '';

-- No default: every writer (the rater) supplies the value, and a default of
-- either spelling would hide a writer that forgot it.
ALTER TABLE rated_usage ALTER COLUMN serving_mode DROP DEFAULT;

-- A pre-0007 rater writes '' and is rejected here; see "Rollout order for
-- migration 0007" in migrations/README.md.
ALTER TABLE rated_usage
    ADD CONSTRAINT rated_usage_serving_mode_ck CHECK (serving_mode IN ('shared', 'dedicated'));

COMMENT ON COLUMN rated_usage.serving_mode IS
    'Serving-mode SKU axis: ''shared'' or ''dedicated'' (CHECK-enforced; the empty string was retired on 2026-09-29). KEY COLUMN: the two price from different SKUs and must never merge into one rollup.';

-- billing_event is the evidence ledger and deliberately has no CHECK. Its
-- description changes: NULL no longer means dedicated.
COMMENT ON COLUMN billing_event.serving_mode IS
    'Serving mode captured at meter time: ''shared'' or ''dedicated''. Rows metered before migration 0007 were backfilled to ''dedicated'' (all pre-cutover traffic was dedicated). Any other value, including NULL, is withheld by the rater as invalid_serving_mode_events.';

-- Reconciliation view: add invalid_serving_mode_attempts. Before this migration
-- every dedicated event was rated; from here on the rater withholds any event
-- whose serving_mode is not 'shared' or 'dedicated' (invalid_serving_mode_events).
-- Without a column for that cause, those withheld attempts would show up as an
-- unexplained attempt_delta and token deltas. The predicate matches the rater's.
-- The `rated` CTE now aggregates rated_usage to the view's grain (see the CTE's
-- comment); every output column keeps its 0005 name and type. Everything else is
-- the 0005 definition unchanged; the new column is appended last because CREATE
-- OR REPLACE VIEW can only add columns at the end. The down migration drops and
-- recreates the 0005 column set with the same aggregated `rated` CTE.
CREATE OR REPLACE VIEW billing_reconciliation_hourly AS
WITH raw AS (
    SELECT
        date_trunc('hour', COALESCE(event_ts, created_at) AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS window_start,
        auth_id, resource_id, model AS model_id,
        MAX(org_id) AS org_id,
        COUNT(*) FILTER (WHERE org_id IS NULL)::bigint AS missing_org_attempts,
        COUNT(DISTINCT org_id)::bigint AS distinct_org_ids,
        COUNT(*)::bigint AS raw_attempts,
        COUNT(*) FILTER (WHERE usage_found)::bigint AS metered_attempts,
        COUNT(*) FILTER (WHERE NOT usage_found)::bigint AS missing_usage_attempts,
        COUNT(*) FILTER (WHERE prompt_tokens < 0 OR cached_tokens < 0
                              OR completion_tokens < 0 OR cached_tokens > prompt_tokens)::bigint
            AS invalid_usage_attempts,
        COUNT(*) FILTER (WHERE aborted)::bigint AS aborted_attempts,
        -- Same predicate as the rater's invalid_serving_mode_events bucket: an
        -- invalid serving mode on an authoritative (usage_found), valid-usage,
        -- attributable event. A missing-usage or invalid-usage attempt is already
        -- explained by its own column above, and an unattributable one (NULL
        -- auth/resource/model) by the rater's unattributable bucket, so counting
        -- it here as well would explain the same delta twice.
        COUNT(*) FILTER (WHERE (serving_mode IS NULL OR serving_mode NOT IN ('shared','dedicated'))
                              AND usage_found
                              AND prompt_tokens >= 0 AND cached_tokens >= 0
                              AND completion_tokens >= 0 AND cached_tokens <= prompt_tokens
                              AND auth_id IS NOT NULL
                              AND resource_id IS NOT NULL
                              AND model IS NOT NULL)::bigint
            AS invalid_serving_mode_attempts,
        -- "FAILED" is status_code >= 400, the SAME threshold the rater uses to
        -- decide whether a zero-usage attempt is routine (expected) or alarming
        -- (unexplained). The two must agree: a 4xx zero-usage attempt excused by
        -- the rater but shown here as un-failed would make the operator's audit
        -- view disagree with the paging decision about the very same rows.
        COUNT(*) FILTER (WHERE status_code >= 400)::bigint AS failed_attempts,
        SUM(prompt_tokens)::bigint AS raw_prompt_tokens,
        SUM(fresh_input_tokens)::bigint AS raw_fresh_input_tokens,
        SUM(cached_tokens)::bigint AS raw_cached_tokens,
        SUM(completion_tokens)::bigint AS raw_completion_tokens
    FROM billing_event
    GROUP BY 1, auth_id, resource_id, model
), rated AS (
    -- Since 0006, rated_usage holds one row per (serving_mode, owner) inside the
    -- view's grain. Aggregate to exactly one row per (window, auth, resource,
    -- model) so the LEFT JOIN below cannot repeat a raw row (and its
    -- raw_attempts / invalid_serving_mode_attempts) once per rated row.
    SELECT
        window_start, auth_id, resource_id, model_id,
        MAX(org_id) AS org_id,
        SUM(event_count)::bigint AS rated_attempts,
        SUM(prompt_tokens)::bigint AS rated_prompt_tokens,
        SUM(billable_prompt_tokens)::bigint AS rated_fresh_input_tokens,
        SUM(cached_tokens)::bigint AS rated_cached_tokens,
        SUM(completion_tokens)::bigint AS rated_completion_tokens,
        SUM(cost) AS cost
    FROM rated_usage
    GROUP BY window_start, auth_id, resource_id, model_id
), reconciliation_keys AS (
    -- UNION uses PostgreSQL set semantics (NULLs compare equal) to produce one
    -- row per null-safe natural key without relying on FULL JOIN conditions that
    -- PostgreSQL 16 cannot always plan for filtered queries.
    SELECT window_start, auth_id, resource_id, model_id FROM raw
    UNION
    SELECT window_start, auth_id, resource_id, model_id FROM rated
)
SELECT
    reconciliation_keys.window_start AS window_start,
    reconciliation_keys.auth_id AS auth_id,
    reconciliation_keys.resource_id AS resource_id,
    COALESCE(raw.org_id, rated.org_id) AS org_id,
    reconciliation_keys.model_id AS model_id,
    COALESCE(raw.raw_attempts, 0) AS raw_attempts,
    COALESCE(raw.metered_attempts, 0) AS metered_attempts,
    COALESCE(raw.missing_usage_attempts, 0) AS missing_usage_attempts,
    COALESCE(raw.invalid_usage_attempts, 0) AS invalid_usage_attempts,
    COALESCE(raw.aborted_attempts, 0) AS aborted_attempts,
    COALESCE(raw.failed_attempts, 0) AS failed_attempts,
    COALESCE(raw.missing_org_attempts, 0) AS missing_org_attempts,
    COALESCE(raw.distinct_org_ids, 0) AS distinct_org_ids,
    COALESCE(rated.rated_attempts, 0) AS rated_attempts,
    COALESCE(raw.raw_prompt_tokens, 0) AS raw_prompt_tokens,
    COALESCE(rated.rated_prompt_tokens, 0) AS rated_prompt_tokens,
    COALESCE(raw.raw_fresh_input_tokens, 0) AS raw_fresh_input_tokens,
    COALESCE(rated.rated_fresh_input_tokens, 0) AS rated_fresh_input_tokens,
    COALESCE(raw.raw_cached_tokens, 0) AS raw_cached_tokens,
    COALESCE(rated.rated_cached_tokens, 0) AS rated_cached_tokens,
    COALESCE(raw.raw_completion_tokens, 0) AS raw_completion_tokens,
    COALESCE(rated.rated_completion_tokens, 0) AS rated_completion_tokens,
    COALESCE(rated.cost, 0::numeric) AS rated_cost,
    COALESCE(raw.raw_attempts, 0) - COALESCE(rated.rated_attempts, 0) AS attempt_delta,
    COALESCE(raw.raw_prompt_tokens, 0) - COALESCE(rated.rated_prompt_tokens, 0) AS prompt_token_delta,
    COALESCE(raw.raw_fresh_input_tokens, 0) - COALESCE(rated.rated_fresh_input_tokens, 0) AS fresh_input_token_delta,
    COALESCE(raw.raw_cached_tokens, 0) - COALESCE(rated.rated_cached_tokens, 0) AS cached_token_delta,
    COALESCE(raw.raw_completion_tokens, 0) - COALESCE(rated.rated_completion_tokens, 0) AS completion_token_delta,
    -- Last: CREATE OR REPLACE VIEW may only append columns.
    COALESCE(raw.invalid_serving_mode_attempts, 0) AS invalid_serving_mode_attempts
FROM reconciliation_keys
LEFT JOIN raw
  ON raw.window_start = reconciliation_keys.window_start
 AND raw.auth_id IS NOT DISTINCT FROM reconciliation_keys.auth_id
 AND raw.resource_id IS NOT DISTINCT FROM reconciliation_keys.resource_id
 AND raw.model_id IS NOT DISTINCT FROM reconciliation_keys.model_id
LEFT JOIN rated
  ON rated.window_start = reconciliation_keys.window_start
 AND rated.auth_id IS NOT DISTINCT FROM reconciliation_keys.auth_id
 AND rated.resource_id IS NOT DISTINCT FROM reconciliation_keys.resource_id
 AND rated.model_id IS NOT DISTINCT FROM reconciliation_keys.model_id;
