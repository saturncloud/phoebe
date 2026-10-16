-- Reverse 0008. Drops the evidence columns; nothing in money reads them.
COMMENT ON COLUMN billing_event.gpu_type IS NULL;
ALTER TABLE billing_event DROP COLUMN serving_duration_ms;
ALTER TABLE billing_event DROP COLUMN request_started_at;
ALTER TABLE billing_event DROP COLUMN region;
ALTER TABLE billing_event DROP COLUMN topology_revision;
