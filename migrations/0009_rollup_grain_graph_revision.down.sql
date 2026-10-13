-- Reverse 0009. LOSSY BY CONSTRUCTION, like 0006's down: rollups that differ only in
-- graph or revision cannot both survive the narrower key. Keep one per old-grain key
-- (development databases only; see migrations/README.md), then restore the 0007 key,
-- ids and the nullable evidence graph.
--
-- Roll the code back first: deploy the pre-0009 rater and token-push before running
-- this, then re-rate (`rater --since ... --until ...`) and re-push the hours whose
-- rows were deleted here. See "Rollout order for migration 0009" in the README.
--
-- Before the lossy DELETE, print the row count, SUM(cost), SUM(event_count) and
-- earliest window_start of exactly the rows it removes (the same audit trail 0007 up
-- prints), so the operator knows which hours to re-rate.
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
     WHERE EXISTS (
           SELECT 1 FROM rated_usage keep
           WHERE keep.auth_id      = ru.auth_id
             AND keep.owner_type   = ru.owner_type
             AND keep.owner_id     = ru.owner_id
             AND keep.resource_id  = ru.resource_id
             AND keep.model_id     = ru.model_id
             AND keep.serving_mode = ru.serving_mode
             AND keep.window_start = ru.window_start
             AND keep.id < ru.id
       );
    IF n > 0 THEN
        RAISE NOTICE '0009 down: deleting % rated_usage rows that differ from a kept row only in graph or topology revision (sum cost %, sum event_count %, earliest window_start %); the kept rows are partial until re-rated, so every affected hour MUST be re-rated: rater --since <earliest window_start> --until <now>, then token-push over the same window',
            n, cost_s, evts_s, to_char(first_w AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"');
    END IF;
END
$$;

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
