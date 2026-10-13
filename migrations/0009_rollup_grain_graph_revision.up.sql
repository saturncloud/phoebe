-- The rollup grain gains the serving graph and its topology revision (owner ruling
-- 2026-10-12, metering-margin Q-1: "make the graph and the topology revision part of
-- the rollup grain, so every rollup belongs to exactly one graph revision"). Margin is
-- computed per graph per time window, from the tokens and revenue on the rollups
-- against GPU-hours per pool (Atlas); that needs every rollup to name one graph
-- revision, where 0006 carried the graph only as evidence that went NULL whenever a
-- rollup spanned two graphs.
--
-- New grain:
--   (auth_id, owner_type, owner_id, resource_id, model_id, serving_mode,
--    graph_k8s_name, topology_revision, window_start)
--
-- KEY COLUMNS MAY NOT BE NULL (UNIQUE treats NULLs as distinct, so a NULL key would
-- let one logical rollup be written twice and double-bill). The unknown values are
-- therefore sentinels, as for the owner pair: graph_k8s_name '' = the request named
-- no graph; topology_revision 0 = no published revision (an event from before 0008,
-- or a graph with no Atlas topology entry yet). Unknown never withholds money.
--
-- region is carried as an attribute, not a key: a revision names one cluster, and a
-- cluster has one region, so region is a function of the key.
--
-- WHY THE ID IS RECOMPUTED. rated_usage.id is md5 of the natural key; widening the key
-- changes every id, exactly as 0006 and 0007 did. Existing rows are rewritten below
-- with the rater's new formula so a re-rate reproduces them. Pre-production: no
-- settled rows hold the old ids (same reckoning as 0007).
--
-- Rollout: NOT additive. A pre-0009 rater must not run against this schema. Its
-- ON CONFLICT target (the 7-column 0006 grain) no longer matches any unique
-- constraint, and it can write graph_k8s_name NULL, which now violates NOT NULL.
-- Either error rolls back the whole RateWindow transaction, so the failure is
-- closed and nothing is written; the new rater re-rates the window on its next
-- run. Order:
--   1. suspend the rater CronJob (and token-push), or let any in-flight run
--      finish;
--   2. run cmd/migrate up (0008 and 0009);
--   3. deploy the new drainer, interceptor and rater images;
--   4. deploy the saturn-aws-manager release whose TokenRollupSchema accepts
--      graph_k8s_name, topology_revision, region and request_count BEFORE the
--      new token-push image (the old manager rejects the new keys with a 400);
--   5. resume the rater (and token-push).
-- If the gap was longer than the rater's trailing re-rate window, run
-- `rater --since <gap start> --until <current hour start>` and then token-push
-- over the same range.
--
-- Rollback: the down migration is LOSSY (rollups that differ only in graph or
-- revision collapse to one row per 0007 key; the others are deleted). Roll the
-- code back before the schema: deploy the pre-0009 rater and token-push, then
-- run migrate down, then re-rate and re-push the affected hours. Pre-production
-- only. See "Rollout order for migration 0009" in migrations/README.md.

ALTER TABLE rated_usage ADD COLUMN topology_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE rated_usage ADD COLUMN region VARCHAR(64);

UPDATE rated_usage SET graph_k8s_name = '' WHERE graph_k8s_name IS NULL;
ALTER TABLE rated_usage ALTER COLUMN graph_k8s_name SET DEFAULT '';
ALTER TABLE rated_usage ALTER COLUMN graph_k8s_name SET NOT NULL;

UPDATE rated_usage
SET id = md5(length(auth_id)::text || ':' || auth_id
          || '|' || length(owner_type)::text || ':' || owner_type
          || '|' || length(owner_id)::text || ':' || owner_id
          || '|' || length(resource_id)::text || ':' || resource_id
          || '|' || length(model_id)::text || ':' || model_id
          || '|' || length(serving_mode)::text || ':' || serving_mode
          || '|' || length(graph_k8s_name)::text || ':' || graph_k8s_name
          || '|' || topology_revision::text
          || '|' || extract(epoch FROM window_start)::bigint::text);

ALTER TABLE rated_usage DROP CONSTRAINT rated_usage_grain_uq;
ALTER TABLE rated_usage
    ADD CONSTRAINT rated_usage_grain_uq
    UNIQUE (auth_id, owner_type, owner_id, resource_id, model_id, serving_mode,
            graph_k8s_name, topology_revision, window_start);

ALTER TABLE rated_usage
    ADD CONSTRAINT rated_usage_topology_revision_ck CHECK (topology_revision >= 0);

COMMENT ON COLUMN rated_usage.graph_k8s_name IS
    'The DynamoGraphDeployment that served this rollup (the cost centre). KEY COLUMN since 0009; '''' = the requests named no graph.';
COMMENT ON COLUMN rated_usage.topology_revision IS
    'Atlas tf_topology_revision.revision of the serving graph (pool roles, instance types, GPU types, GPUs per replica). KEY COLUMN; 0 = no published revision.';
COMMENT ON COLUMN rated_usage.region IS
    'Region of the serving cluster, as Atlas published it. Attribute, not a key (a revision names one cluster).';
