-- Reverse of 0007: every 'dedicated' row in rated_usage goes back to being
-- spelled '', with the id recomputed by the pre-0007 formula (the same md5
-- expression hashing the empty string). The rename cannot collide with the
-- unique key: while the CHECK held, no '' row could exist.
--
-- WHAT THIS DOES AND DOES NOT RESTORE. The rewrite is lossless for the rated_usage
-- rows that exist at rollback time. The rewritten rows match what the pre-0007
-- rater would write only for windows whose billing_event evidence predates the
-- cutover (billing_event.serving_mode NULL or ''). Events metered after the
-- cutover were stamped serving_mode = 'dedicated' by the proxy, and the pre-0007
-- rater keeps that value, because its COALESCE(ev.serving_mode, '') maps only
-- NULL to ''. So after a rollback, the next re-rate of any window that holds
-- post-cutover evidence writes a 'dedicated' rollup with a different id and
-- deletes, through the normal reconcile path, the '' row this migration restored.
-- Expect reconcile-deletion counts and new ids pushed to saturn-aws-manager for
-- those windows. An hour that also holds events from the rolled-back proxy splits
-- into a '' rollup and a 'dedicated' rollup, so a rolled-back install holds both
-- spellings of dedicated and a rolled-back manager receives both for one grain.
--
-- This migration deliberately does NOT rewrite billing_event to hide that: it is
-- the append-only evidence ledger (contracts/billing-event-ledger.md) and is never
-- edited. The limitation is accepted only because rated_usage rows are disposable
-- in the pre-production window (no production rows; the 2026-09-24 ruling, ledger
-- item 2), where they can be thrown away and rebuilt from billing_event. Once
-- production rows exist, do not roll 0007 back.
ALTER TABLE rated_usage DROP CONSTRAINT IF EXISTS rated_usage_serving_mode_ck;

UPDATE rated_usage
SET serving_mode = '',
    id = md5(length(auth_id)::text || ':' || auth_id
          || '|' || length(owner_type)::text || ':' || owner_type
          || '|' || length(owner_id)::text || ':' || owner_id
          || '|' || length(resource_id)::text || ':' || resource_id
          || '|' || length(model_id)::text || ':' || model_id
          || '|' || length('')::text || ':' || ''
          || '|' || extract(epoch FROM window_start)::bigint::text)
WHERE serving_mode = 'dedicated';

ALTER TABLE rated_usage ALTER COLUMN serving_mode SET DEFAULT '';

COMMENT ON COLUMN rated_usage.serving_mode IS
    'Serving-mode SKU axis; '''' = dedicated (the absence-of-prefix contract), ''shared'' = shared. KEY COLUMN: the two price from different SKUs and must never merge into one rollup.';

COMMENT ON COLUMN billing_event.serving_mode IS NULL;

-- Restore the 0005 reconciliation view. CREATE OR REPLACE cannot remove the
-- invalid_serving_mode_attempts column 0007 added, so drop and recreate.
DROP VIEW billing_reconciliation_hourly;

-- Operator-facing raw -> rated reconciliation.  This deliberately stops at
-- rated_usage: the central billing system is outside Phoebe's trust boundary,
-- and must reconcile the pushed rated_usage.id values to its invoice lines.
--
-- Raw evidence is grouped at rated_usage's natural grain — (hour, auth_id,
-- resource_id, model) — because the rater derives org_id with MAX and does not
-- group by it.  Grouping raw by org_id would split a rollout-era NULL org and a
-- real org into two false mismatch rows; missing_org_attempts and
-- distinct_org_ids expose that org evidence instead.
CREATE VIEW billing_reconciliation_hourly AS
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
    COALESCE(raw.raw_completion_tokens, 0) - COALESCE(rated.rated_completion_tokens, 0) AS completion_token_delta
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
