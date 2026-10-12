-- Serving topology, region and serving time on every usage event (build item
-- B24; owner ruling 2026-10-12: usage events record the serving topology by
-- revision). All four columns are platform-set evidence for margin analysis:
-- nullable, no CHECK (billing_event is the evidence ledger, see
-- contracts/billing-event-ledger.md). 0009 makes topology_revision a rollup grain
-- key; none of them can withhold or reprice a row.
--
--   topology_revision   Atlas tf_topology_revision.revision of the serving
--                       graph; with graph_k8s_name it names each pool's role,
--                       instance type, GPU type and GPUs per replica.
--   region              the serving cluster's region.
--   request_started_at  when phoebe received the request.
--   serving_duration_ms phoebe's time from receipt to the attempt's outcome.
--
-- gpu_type is left in place but no longer written: no code ever filled it,
-- and one GPU type cannot describe a disaggregated graph whose pools may use
-- different GPUs. The revision replaces it. Rows stay NULL.
--
-- Rollout: additive. A pre-0008 drainer keeps working against this schema
-- (it names its columns), so apply 0008 before rolling out the new drainer;
-- the migrate Job already runs first.
ALTER TABLE billing_event ADD COLUMN topology_revision INTEGER;
ALTER TABLE billing_event ADD COLUMN region VARCHAR(64);
ALTER TABLE billing_event ADD COLUMN request_started_at TIMESTAMPTZ;
ALTER TABLE billing_event ADD COLUMN serving_duration_ms BIGINT;
COMMENT ON COLUMN billing_event.gpu_type IS
  'Unused since 0008: never written. See topology_revision.';
