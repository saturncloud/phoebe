-- Reverse 0006. LOSSY BY CONSTRUCTION, in the direction of money: collapsing the
-- grain merges rollups that were deliberately split because they priced differently.
-- Two rows for one (auth, resource, model, hour) that differ only in serving_mode
-- cannot both survive a narrower unique constraint, and re-adding it would fail on
-- the duplicate rather than silently pick one. The same applies to owner splits.
--
-- This down migration therefore DELETES the rows that cannot be represented in the old
-- grain, keeping one per old-grain key, rather than failing or merging costs. It is
-- only safe on a development database -- which the clean-cutover contract
-- (migrations/README.md) guarantees is the only place it can run.

DROP INDEX IF EXISTS rated_usage_owner_window_start_ix;

-- Keep one row per OLD grain key. Preferring the dedicated/no-owner row makes the
-- choice deterministic rather than arbitrary; any other row for that key is a split
-- the old shape cannot hold.
DELETE FROM rated_usage ru
WHERE EXISTS (
    SELECT 1 FROM rated_usage other
    WHERE other.auth_id      = ru.auth_id
      AND other.resource_id  = ru.resource_id
      AND other.model_id     = ru.model_id
      AND other.window_start = ru.window_start
      AND (other.serving_mode, other.owner_type, other.owner_id, other.id)
        < (ru.serving_mode,    ru.owner_type,    ru.owner_id,    ru.id)
);

ALTER TABLE rated_usage DROP CONSTRAINT IF EXISTS rated_usage_grain_uq;

ALTER TABLE rated_usage
    ADD CONSTRAINT rated_usage_auth_resource_model_window_uq
    UNIQUE (auth_id, resource_id, model_id, window_start);

ALTER TABLE rated_usage
    DROP CONSTRAINT IF EXISTS rated_usage_owner_pair_ck,
    DROP CONSTRAINT IF EXISTS rated_usage_owner_type_ck,
    DROP COLUMN IF EXISTS graph_k8s_name,
    DROP COLUMN IF EXISTS owner_id,
    DROP COLUMN IF EXISTS owner_type,
    DROP COLUMN IF EXISTS serving_mode;

ALTER TABLE billing_event DROP COLUMN IF EXISTS graph_k8s_name;
