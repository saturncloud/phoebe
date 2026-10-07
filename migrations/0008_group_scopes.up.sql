-- Membership-aware group quotas (ruled 2026-10-07, Q-T1(b) + Q-T2(b)).
--
-- Two additions, both in service of group-level admission enforcement:
--
--  1. billing_event.member_group_ids — the groups the CALLER belongs to
--     (X-Saturn-Group-Scopes membership), captured at meter time as evidence.
--     EVIDENCE ONLY: it never enters the money grain. rated_usage stays keyed
--     (auth_id, owner_type, owner_id, resource_id, model_id, serving_mode,
--     window_start) and the money row is still written exactly once per window.
--
--  2. group_usage — the GROUP ATTRIBUTION rollup the monthly spend cap reads:
--     per (group_id, hour) token sums and cost, written by the rater WHILE it
--     rates a window into money. Attribution sources per event: the token's own
--     group (billing_event.group_id, the group-token case) PLUS every group in
--     member_group_ids (the membership case). A user in N groups contributes to
--     N group rows. This is ATTRIBUTION, not money: the same event cost appears
--     under every group it belongs to, and withheld (non-money) events
--     contribute nothing.
--
-- ROLLOUT: the rater's statement references member_group_ids and group_usage,
-- so a pre-0008 rater cannot run against this schema and a post-0008 rater
-- cannot run against the old one (SQLSTATE 42703) — roll code and schema
-- together, in the order used for migration 0007 (migrate up, then deploy).

-- ---------------------------------------------------------------------------
-- billing_event: membership evidence
-- ---------------------------------------------------------------------------

-- The caller's group memberships, captured at meter time from the trusted
-- X-Saturn-Group-Scopes envelope. NULLABLE: an event whose caller belongs to no
-- group (or whose edge does not stamp the envelope) carries NULL, exactly like
-- every other identity column. NULL means "no membership evidence", never
-- "zero groups" as a chargeable fact.
ALTER TABLE billing_event ADD COLUMN member_group_ids TEXT[];

COMMENT ON COLUMN billing_event.member_group_ids IS
    'Groups the caller belongs to (X-Saturn-Group-Scopes membership), captured at meter time. Evidence only -- never part of the billing grain. NULL when the caller has no group memberships.';

-- ---------------------------------------------------------------------------
-- group_usage: the group attribution rollup
-- ---------------------------------------------------------------------------

CREATE TABLE group_usage (
    group_id               TEXT NOT NULL,
    -- The UTC hour bucket, same bucketing expression the rater uses for
    -- rated_usage.window_start, so a group's monthly spend sum reads
    -- window_start >= date_trunc('month', now()) over aligned hours.
    window_start           TIMESTAMPTZ NOT NULL,
    prompt_tokens          BIGINT NOT NULL DEFAULT 0,
    cached_tokens          BIGINT NOT NULL DEFAULT 0,
    completion_tokens      BIGINT NOT NULL DEFAULT 0,
    billable_prompt_tokens BIGINT NOT NULL DEFAULT 0,
    -- NUMERIC(20,9), the rated_usage.cost unit, so the admission spend check
    -- compares in the same unit caps are authored in. Attribution cost: the
    -- summed per-event cost of the events attributed to this group in this
    -- hour. The same event cost legitimately appears under several groups.
    cost                   NUMERIC(20,9) NOT NULL DEFAULT 0,
    event_count            BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT pk_group_usage PRIMARY KEY (group_id, window_start)
);

COMMENT ON TABLE group_usage IS
    'Group attribution rollup written by the rater while rating a window into money: per (group_id, hour) token sums and cost. Attribution, not money -- an event attributed to N groups contributes to N rows, and withheld (non-money) events contribute nothing. The admission group spend check reads SUM(cost) here; rated_usage remains the only money rollup.';
