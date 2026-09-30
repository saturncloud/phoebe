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
-- TWO CLEAN-UPS HAPPEN FIRST, both on staging data only:
--  1. A '' row whose natural-key twin already exists as 'dedicated' (possible if
--     a trusted header ever stamped the explicit spelling before the cutover)
--     cannot be renamed without violating the unique key. The '' row is deleted;
--     the 'dedicated' twin stays.
--  2. A row with any other value (neither '', 'shared' nor 'dedicated') has no
--     legal meaning and is deleted.
-- The raw evidence for both remains in billing_event.
--
-- NOTE ON RE-RATING: pre-cutover dedicated evidence in billing_event has a NULL
-- serving_mode. The rater now withholds such events (invalid_serving_mode_events),
-- so a re-rate of a pre-cutover window deletes the rows rewritten here through the
-- normal reconcile path. That is intended: there is nothing in production to keep.
-- The routine trailing-window rater hits this on its own for up to
-- rateTrailingHours after the deploy (exit 2 each run, dedicated rows deleted and
-- then removed from saturn-aws-manager by token-push). See "Serving-mode cutover
-- (migration 0007)" in docs/billing-reconciliation.md for what to expect and the
-- optional explicit backfill.
--
-- ROLLBACK LIMIT: the down migration cannot be reversed exactly for windows
-- metered after the cutover. The pre-0007 rater keeps billing_event's explicit
-- 'dedicated', so re-rating such a window after a rollback writes 'dedicated'
-- rollups next to the '' rows the down migration restores. See the header of
-- 0007_serving_mode_explicit.down.sql.

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

-- billing_event is the evidence ledger and deliberately has no CHECK. Only its
-- description changes: NULL no longer means dedicated.
COMMENT ON COLUMN billing_event.serving_mode IS
    'Serving mode captured at meter time: ''shared'' or ''dedicated''. NULL or empty only on events metered before the 2026-09-29 serving-mode cutover (when it meant dedicated); the rater withholds those from money and counts them as invalid_serving_mode_events.';

-- Reconciliation view: add invalid_serving_mode_attempts. Before this migration
-- every dedicated event was rated; from here on the rater withholds any event
-- whose serving_mode is not 'shared' or 'dedicated' (invalid_serving_mode_events).
-- Without a column for that cause, those withheld attempts would show up as an
-- unexplained attempt_delta and token deltas. The predicate matches the rater's.
-- Everything else is the 0005 definition unchanged; the new column is appended
-- last because CREATE OR REPLACE VIEW can only add columns at the end. The down
-- migration drops and recreates the 0005 view.
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
    SELECT
        window_start, auth_id, resource_id, org_id, model_id,
        event_count AS rated_attempts,
        prompt_tokens AS rated_prompt_tokens,
        billable_prompt_tokens AS rated_fresh_input_tokens,
        cached_tokens AS rated_cached_tokens,
        completion_tokens AS rated_completion_tokens,
        cost
    FROM rated_usage
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
