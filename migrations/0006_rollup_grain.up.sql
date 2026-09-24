-- Widen the rated_usage grain: serving mode and owner identity join the natural key,
-- and the serving graph rides along as evidence.
--
-- WHY serving_mode IS A KEY COLUMN. Shared and dedicated traffic price from DIFFERENT
-- SKUs -- 'shared:'||base_model versus the bare base_model (the absence-of-prefix
-- contract). Before this change serving_mode was NOT part of the grain, so a bucket
-- containing both modes collapsed into ONE rollup and MIN(prompt_price) silently
-- applied the CHEAPER of the two rates to all of it. The existing ambiguous_base gate
-- could not see it: both rows carry the SAME base_model and both resolve via the same
-- pricing path, so COUNT(DISTINCT base_model) is 1 and the gate stays false.
--
-- That mixing is not hypothetical. serving_mode is a deploy-time property (a tf_model
-- column, or the anti-spoof X-Saturn-Serving-Mode header), so it cannot vary per
-- request -- but it CAN change across an hour boundary: Atlas flipping a deployment's
-- mode, or the header rollout landing mid-hour (an absent header reads as dedicated,
-- so pre-rollout events on an already-shared deployment price as dedicated). Both put
-- two rates in one bucket. Keying on it splits them into two correctly-priced rollups
-- instead of merging them at the cheaper rate.
--
-- WHY OWNER IDENTITY IS IN THE KEY. auth_id is the API-KEY identity. It answers "which
-- key incurred this" but not "which person or team" -- and it does not survive key
-- rotation, so a rotated key breaks attribution continuity. Owner answers the second
-- question and survives rotation, but alone it cannot present per-key charges. Both
-- are needed, so both are in the grain (Hugo, 2026-09-23).
--
-- OWNER IS ONE (type, id) PAIR, NOT TWO INDEPENDENT COLUMNS. Upstream, an identity is
-- a user XOR a group -- never both (Atlas's ownership model; usage-statistics already
-- encodes exactly this as owner_type/owner_id). billing_event carries user_id and
-- group_id as two nullable columns with nothing enforcing the invariant, so this
-- migration adds the CHECK that makes the illegal state unrepresentable, and the rater
-- collapses the pair into (owner_type, owner_id).
--
-- An event with BOTH set is a producer bug. Per the established treatment of invalid
-- evidence (invalid token counts, NULL org): it is RETAINED RAW, EXCLUDED FROM MONEY,
-- and ALARMED -- never guessed at, never silently attributed to one of the two.
-- Hence the CHECK lives on rated_usage (money) and NOT on billing_event (evidence):
-- constraining the raw ledger would make the bad row un-insertable and destroy the
-- very evidence needed to diagnose the producer.
--
-- WHY graph_k8s_name IS CARRIED BUT NOT KEYED. It names the DynamoGraphDeployment that
-- actually served the traffic -- the thing that consumes GPUs. In DEDICATED mode a
-- deployment owns its graph (1:1). In SHARED mode MANY tf_model rows, across MANY
-- orgs, ride one platform-owned base graph, and that graph has NO row in any database:
-- it is keyed only by a deterministic name and reference-counted. So a rollup's
-- resource_id cannot identify the cost centre in shared mode, and grouping rollups by
-- resource_id to obtain pool cost is wrong there.
--
-- Carrying the graph name keeps that attributable. It is free at meter time (already a
-- data key on the registry ConfigMap phoebe reads) and UNRECOVERABLE later. It is NOT
-- a key column: a model does not move between graphs within an hour, and keying on it
-- would risk splitting a rollup on a value that is evidence rather than identity.
-- Margin analysis itself is out of scope -- this only avoids precluding it.

-- ---------------------------------------------------------------------------
-- billing_event: owner identity evidence
-- ---------------------------------------------------------------------------

-- The serving graph, captured at meter time. NULLABLE: dedicated-path events that
-- predate graph propagation, and any event whose producer did not supply it, simply
-- carry no graph. A NULL here is not an error and never withholds money -- it only
-- means that rollup's cost cannot be attributed to a pool.
ALTER TABLE billing_event ADD COLUMN graph_k8s_name VARCHAR(253);

COMMENT ON COLUMN billing_event.graph_k8s_name IS
    'The DynamoGraphDeployment serving this request (the cost centre). Dedicated: the deployment''s own graph. Shared: the platform base graph many orgs ride. NULL when the producer did not supply it. Evidence only -- never part of the billing grain.';

-- NO CHECK constraint on (user_id, group_id) here, deliberately. billing_event is the
-- RAW EVIDENCE ledger: a both-set row is a producer bug whose evidence must survive so
-- the bug is diagnosable. A constraint would reject the INSERT and destroy it. The
-- rater excludes such rows from money and counts them; see internal/rating/store.go.

-- ---------------------------------------------------------------------------
-- rated_usage: the widened grain
-- ---------------------------------------------------------------------------

-- serving_mode: NOT NULL with a '' default meaning DEDICATED, mirroring the
-- absence-of-prefix pricing contract. A key column may not be NULL -- PostgreSQL
-- UNIQUE treats NULLs as distinct, so a nullable key column would let the same
-- logical rollup be written twice and double-bill. The rater normalizes NULL/''
-- to '' before this table sees it.
ALTER TABLE rated_usage ADD COLUMN serving_mode VARCHAR(32) NOT NULL DEFAULT '';

-- owner_type / owner_id: the single owner pair. Both NOT NULL with '' defaults for
-- the same key-column reason as serving_mode. '' means "the producer supplied no
-- owner" -- attribution is then by auth_id alone, which is the pre-existing
-- behaviour and is not an error.
ALTER TABLE rated_usage ADD COLUMN owner_type VARCHAR(16) NOT NULL DEFAULT '';
ALTER TABLE rated_usage ADD COLUMN owner_id   VARCHAR(32) NOT NULL DEFAULT '';

-- graph_k8s_name: evidence, not identity. NULLABLE (unlike the key columns above)
-- precisely because it is not keyed, so a NULL cannot split or duplicate a rollup.
ALTER TABLE rated_usage ADD COLUMN graph_k8s_name VARCHAR(253);

-- serving_mode is a closed enum: '' (dedicated, the absence-of-prefix pricing
-- contract) or 'shared'. Constrained for the SAME reason as owner_type below, and
-- with more force: this column SELECTS THE PRICE SKU. An unconstrained value is not
-- merely uninterpretable, it is a rollup priced from the wrong rate row, or -- for a
-- near-miss like 'SHARED', 'shared ' or the spelled-out 'dedicated' -- a grain SPLIT
-- that mints a second rated_usage_id for traffic that should have been one rollup.
--
-- 'dedicated' is the specific trap worth naming: the manager's own pricing module
-- defines SERVING_MODE_DEDICATED = "dedicated" (pricing/lookup.py), so the spelled-out
-- form already exists in the system as a legitimate value in a DIFFERENT vocabulary.
-- Here it is illegal, because dedicated is the ABSENCE of a prefix. Without this CHECK
-- the two vocabularies silently coexist in one column and the first breakdown view
-- that groups on it reports dedicated spend split across two buckets.
ALTER TABLE rated_usage
    ADD CONSTRAINT rated_usage_serving_mode_ck CHECK (serving_mode IN ('', 'shared'));

-- owner_type is a closed two-valued enum plus the empty sentinel. Anything else is a
-- producer or rater bug; reject it at the money boundary rather than billing an
-- uninterpretable owner.
ALTER TABLE rated_usage
    ADD CONSTRAINT rated_usage_owner_type_ck CHECK (owner_type IN ('', 'user', 'group'));

-- The pair moves together: a type with no id, or an id with no type, is a half-written
-- owner that no consumer can interpret.
ALTER TABLE rated_usage
    ADD CONSTRAINT rated_usage_owner_pair_ck CHECK (
        (owner_type = '' AND owner_id = '') OR (owner_type <> '' AND owner_id <> '')
    );

COMMENT ON COLUMN rated_usage.serving_mode IS
    'Serving-mode SKU axis; '''' = dedicated (the absence-of-prefix contract), ''shared'' = shared. KEY COLUMN: the two price from different SKUs and must never merge into one rollup.';
COMMENT ON COLUMN rated_usage.owner_type IS
    'Owner discriminator: ''user'', ''group'', or '''' when the producer supplied no owner. KEY COLUMN. A user XOR a group -- never both.';
COMMENT ON COLUMN rated_usage.owner_id IS
    'The owning user or group id, interpreted per owner_type. KEY COLUMN, so charges are presentable per person/team as well as per API key.';
COMMENT ON COLUMN rated_usage.graph_k8s_name IS
    'The DynamoGraphDeployment that served this rollup (the cost centre). NOT a key column -- evidence carried so shared-mode cost stays attributable, since a shared graph has no database row and serves many orgs.';

-- ---------------------------------------------------------------------------
-- The natural key
-- ---------------------------------------------------------------------------

-- Replace the old 4-column grain with the widened 7-column one. The clean-cutover
-- contract (migrations/README.md) means there are no production rollups to migrate:
-- the new columns default to '' on any development rows, which is exactly the
-- dedicated/no-owner reading they had implicitly.
--
-- NOTE FOR ANYONE HOLDING A rated_usage.id: the surrogate id is an md5 over this
-- natural key, so EVERY id changes with this migration. That is intended and is free
-- only because nothing is deployed. See internal/rating/store.go.
ALTER TABLE rated_usage DROP CONSTRAINT rated_usage_auth_resource_model_window_uq;

ALTER TABLE rated_usage
    ADD CONSTRAINT rated_usage_grain_uq
    UNIQUE (auth_id, owner_type, owner_id, resource_id, model_id, serving_mode, window_start);

-- Owner-scoped billing queries (per-user / per-group spend over a window) mirror the
-- existing per-auth_id index. Partial: rows with no owner are not owner-queryable and
-- would only bloat the index.
CREATE INDEX rated_usage_owner_window_start_ix
    ON rated_usage (owner_type, owner_id, window_start)
    WHERE owner_type <> '';
