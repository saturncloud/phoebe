-- Separate the trusted billable attempt identity (request_id, retained for wire
-- compatibility) from the caller's untrusted logical/correlation id.  Also keep
-- outcome evidence for every execution attempt. Raw engine evidence remains
-- insertable even when token counts are invalid; the rater excludes it from
-- money and reconciliation exposes it for repair.
ALTER TABLE billing_event ADD COLUMN client_request_id VARCHAR(255);
ALTER TABLE billing_event ADD COLUMN usage_found BOOLEAN;
ALTER TABLE billing_event ADD COLUMN status_code INTEGER;
ALTER TABLE billing_event ADD COLUMN streamed BOOLEAN NOT NULL DEFAULT FALSE;
-- BIGINT from explicitly widened operands: prompt_tokens and cached_tokens are
-- individually valid int32 engine evidence, but their difference can exceed
-- int32 (e.g. prompt_tokens = 2147483647 with cached_tokens = -1). An INTEGER
-- generated column would make PostgreSQL reject the INSERT outright, destroying
-- the raw invalid evidence this table exists to retain. Widening before the
-- subtraction keeps the row insertable; the rater still excludes it from money
-- and reconciliation still counts it as an invalid-usage attempt.
ALTER TABLE billing_event ADD COLUMN fresh_input_tokens BIGINT
    GENERATED ALWAYS AS (prompt_tokens::BIGINT - cached_tokens::BIGINT) STORED;

-- The supported rollout is a verified-empty clean cutover: no legacy database,
-- Valkey, or WAL events and no mixed-version drainer (see migrations/README.md).
-- Keep this defensive development-database backfill deterministic, but do not
-- treat it as a rolling-version compatibility mechanism.
UPDATE billing_event
SET usage_found = NOT (
    aborted AND prompt_tokens = 0 AND cached_tokens = 0 AND completion_tokens = 0
);
ALTER TABLE billing_event ALTER COLUMN usage_found SET DEFAULT FALSE;
ALTER TABLE billing_event ALTER COLUMN usage_found SET NOT NULL;

ALTER TABLE billing_event
    ADD CONSTRAINT billing_event_status_code_ck CHECK (
        status_code IS NULL OR status_code BETWEEN 100 AND 599
    );

COMMENT ON COLUMN billing_event.request_id IS
    'Trusted Phoebe-minted billable execution-attempt id; deduplication key.';
COMMENT ON COLUMN billing_event.client_request_id IS
    'Untrusted caller correlation/idempotency value; ingress requires printable ASCII shorter than 255 bytes; never used for billing deduplication.';
COMMENT ON COLUMN billing_event.usage_found IS
    'True only when the serving engine supplied an authoritative OpenAI usage block.';

CREATE INDEX billing_event_client_request_id_ix
    ON billing_event (client_request_id)
    WHERE client_request_id IS NOT NULL;

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
