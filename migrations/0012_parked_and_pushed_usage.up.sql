-- Conflicted usage is PARKED per identity, never dropped and never allowed to
-- hold back the rest of an install-hour (coordination, 2026-10-27).
--
-- Three new tables, all phoebe-local. Nothing here is sent to the manager as is.
--
-- parked_usage: written by the rater (cmd/rater). One row per (hour, identity,
-- reason) whose usage the rater could not turn into a record:
--   owner_conflict  events carrying both a user and a group (identity owner '')
--   ambiguous_org   more than one non-NULL org for the identity in the hour
--   ambiguous_base  more than one base model or adapter, or a value on some
--                   events and none on others
--   missing_org     no org on any event of the identity (the payer is unknown)
-- The identity is the rater's gate identity: (auth_id, owner_type, owner_id,
-- resource_id, model_id, serving_mode). The counts are the parked usage, so the
-- record of it survives after billing_event is pruned (30 days); rated_usage
-- holds no row for a parked identity. resolved_at is set when a later run finds
-- the conflict gone (and cleared again if it comes back); resolved rows stay as
-- history. NEVER pruned.
CREATE TABLE IF NOT EXISTS parked_usage (
    window_start      TIMESTAMPTZ NOT NULL,
    auth_id           VARCHAR(64) NOT NULL,
    owner_type        VARCHAR(16) NOT NULL,
    owner_id          VARCHAR(64) NOT NULL,
    resource_id       VARCHAR(64) NOT NULL,
    model_id          VARCHAR(255) NOT NULL,
    serving_mode      VARCHAR(32) NOT NULL,
    reason            VARCHAR(32) NOT NULL,
    event_count       BIGINT NOT NULL,
    prompt_tokens     BIGINT NOT NULL,
    cached_tokens     BIGINT NOT NULL,
    completion_tokens BIGINT NOT NULL,
    first_parked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at       TIMESTAMPTZ,
    CONSTRAINT parked_usage_pk PRIMARY KEY
        (window_start, auth_id, owner_type, owner_id, resource_id, model_id, serving_mode, reason),
    CONSTRAINT parked_usage_reason_ck CHECK
        (reason IN ('owner_conflict', 'ambiguous_org', 'ambiguous_base', 'missing_org'))
);

-- The rater and token-push look up unresolved and recently resolved rows.
CREATE INDEX IF NOT EXISTS parked_usage_unresolved_ix
    ON parked_usage (window_start) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS parked_usage_resolved_at_ix
    ON parked_usage (resolved_at) WHERE resolved_at IS NOT NULL;

-- pushed_usage: written by token-push. The usage_ids the manager holds for an
-- hour after token-push's last request for it (records sent plus held), with
-- the gate identity of each, so a later push of the hour can list in `held`
-- the ids of an identity that has since been parked. Without it, sending the
-- hour without that identity would make the manager delete, by absence, usage
-- it already accepted.
CREATE TABLE IF NOT EXISTS pushed_usage (
    window_start  TIMESTAMPTZ NOT NULL,
    usage_id      CHAR(64) NOT NULL,
    auth_id       VARCHAR(64) NOT NULL,
    owner_type    VARCHAR(16) NOT NULL,
    owner_id      VARCHAR(64) NOT NULL,
    resource_id   VARCHAR(64) NOT NULL,
    model_id      VARCHAR(255) NOT NULL,
    serving_mode  VARCHAR(32) NOT NULL,
    pushed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pushed_usage_pk PRIMARY KEY (window_start, usage_id)
);

-- pushed_hour: written by token-push. When each hour was last accepted by the
-- manager. A parked identity resolved after that instant makes token-push
-- re-send the hour, however old it is.
CREATE TABLE IF NOT EXISTS pushed_hour (
    window_start TIMESTAMPTZ NOT NULL,
    pushed_at    TIMESTAMPTZ NOT NULL,
    CONSTRAINT pushed_hour_pk PRIMARY KEY (window_start)
);
