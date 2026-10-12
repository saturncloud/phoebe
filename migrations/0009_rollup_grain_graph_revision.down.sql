-- Reverse 0009. LOSSY BY CONSTRUCTION, like 0006's down: rollups that differ only in
-- graph or revision cannot both survive the narrower key. Keep one per old-grain key
-- (development databases only; see migrations/README.md), then restore the 0007 key,
-- ids and the nullable evidence graph.
--
-- Roll the code back first: deploy the pre-0009 rater and token-push before running
-- this, then re-rate (`rater --since ... --until ...`) and re-push the hours whose
-- rows were deleted here. See "Rollout order for migration 0009" in the README.
DELETE FROM rated_usage ru
USING rated_usage keep
WHERE ru.auth_id = keep.auth_id
  AND ru.owner_type = keep.owner_type
  AND ru.owner_id = keep.owner_id
  AND ru.resource_id = keep.resource_id
  AND ru.model_id = keep.model_id
  AND ru.serving_mode = keep.serving_mode
  AND ru.window_start = keep.window_start
  AND ru.id > keep.id;

ALTER TABLE rated_usage DROP CONSTRAINT rated_usage_topology_revision_ck;
ALTER TABLE rated_usage DROP CONSTRAINT rated_usage_grain_uq;
ALTER TABLE rated_usage
    ADD CONSTRAINT rated_usage_grain_uq
    UNIQUE (auth_id, owner_type, owner_id, resource_id, model_id, serving_mode, window_start);

UPDATE rated_usage
SET id = md5(length(auth_id)::text || ':' || auth_id
          || '|' || length(owner_type)::text || ':' || owner_type
          || '|' || length(owner_id)::text || ':' || owner_id
          || '|' || length(resource_id)::text || ':' || resource_id
          || '|' || length(model_id)::text || ':' || model_id
          || '|' || length(serving_mode)::text || ':' || serving_mode
          || '|' || extract(epoch FROM window_start)::bigint::text);

ALTER TABLE rated_usage ALTER COLUMN graph_k8s_name DROP NOT NULL;
ALTER TABLE rated_usage ALTER COLUMN graph_k8s_name DROP DEFAULT;
UPDATE rated_usage SET graph_k8s_name = NULL WHERE graph_k8s_name = '';
ALTER TABLE rated_usage DROP COLUMN region;
ALTER TABLE rated_usage DROP COLUMN topology_revision;
