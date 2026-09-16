DROP VIEW IF EXISTS billing_reconciliation_hourly;
DROP TABLE IF EXISTS rating_price_lock;
DROP INDEX IF EXISTS billing_event_client_request_id_ix;

ALTER TABLE billing_event
    DROP CONSTRAINT IF EXISTS billing_event_status_code_ck,
    DROP CONSTRAINT IF EXISTS billing_event_cached_subset_ck,
    DROP CONSTRAINT IF EXISTS billing_event_token_counts_nonnegative_ck,
    DROP COLUMN IF EXISTS fresh_input_tokens,
    DROP COLUMN IF EXISTS streamed,
    DROP COLUMN IF EXISTS status_code,
    DROP COLUMN IF EXISTS usage_found,
    DROP COLUMN IF EXISTS client_request_id;
