-- Hourly video usage (video models design, approved 2026-11-18, sections 7.2
-- and 7.3). The rater (cmd/rater) rolls video_billing_event (0013) up by the
-- hour, and cmd/token-push sends each hour's rollups to the manager as
-- kind "video" records. Quantities only: the manager prices (billing-design.md
-- D1).
--
-- EXPAND ONLY. Two new tables, and one column with a default on pushed_usage,
-- so the previous release's rater and token-push keep working unchanged.

-- rated_video_usage: one row per grain
--   (auth_id, owner_type, owner_id, resource_id, model_id, serving_mode,
--    base_model, task, resolution, region, hour)
-- carrying org_id. Every grain column is an attribute of the wire record and
-- enters its usage_id, so none may be NULL: an unknown base model or region is
-- '' (the wire sends ""). Written by the rater, which converges an hour to its
-- latest run (re-running an hour replaces its rows and deletes the ones it no
-- longer produces). NEVER pruned, like rated_usage.
--
-- Failed and cancelled jobs are counted in event_count and gpu_seconds (margin
-- evidence) and contribute nothing else: their output and request_count are 0
-- (0013's video_billing_event_unbilled_ck). request_count counts completed jobs.
CREATE TABLE IF NOT EXISTS rated_video_usage (
    -- Deterministic local surrogate: md5 of the length-prefixed grain. phoebe's
    -- row id only; the wire's usage_id is computed by token-push.
    id              CHAR(32)      NOT NULL PRIMARY KEY,
    auth_id         VARCHAR(64)   NOT NULL,
    owner_type      VARCHAR(16)   NOT NULL,
    owner_id        VARCHAR(64)   NOT NULL,
    resource_id     VARCHAR(64)   NOT NULL,
    org_id          VARCHAR(64)   NOT NULL,
    model_id        VARCHAR(255)  NOT NULL,
    serving_mode    VARCHAR(32)   NOT NULL,
    base_model      VARCHAR(255)  NOT NULL,
    task            VARCHAR(8)    NOT NULL,
    resolution      VARCHAR(8)    NOT NULL,
    region          VARCHAR(64)   NOT NULL,
    window_start    TIMESTAMPTZ   NOT NULL,
    window_end      TIMESTAMPTZ   NOT NULL,
    -- Exact sums of video_billing_event's NUMERIC(18,9) and NUMERIC(14,3):
    -- nine fractional digits, the manager's decimal limit.
    output_seconds  NUMERIC(30,9) NOT NULL,
    output_frames   BIGINT        NOT NULL,
    request_count   BIGINT        NOT NULL,
    gpu_seconds     NUMERIC(24,3) NOT NULL,
    -- Every finished job of the grain, whatever its status.
    event_count     BIGINT        NOT NULL,
    rated_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT rated_video_usage_grain_uq UNIQUE (auth_id, owner_type, owner_id, resource_id, model_id,
        serving_mode, base_model, task, resolution, region, window_start),
    CONSTRAINT rated_video_usage_serving_mode_ck CHECK (serving_mode IN ('shared', 'dedicated')),
    CONSTRAINT rated_video_usage_task_ck CHECK (task IN ('t2v', 'i2v')),
    CONSTRAINT rated_video_usage_resolution_ck CHECK (resolution IN ('480p', '720p', '1080p', '4k')),
    CONSTRAINT rated_video_usage_quantities_ck CHECK (output_seconds >= 0 AND output_frames >= 0
        AND request_count >= 0 AND gpu_seconds >= 0 AND event_count > 0 AND request_count <= event_count)
);

-- token-push reads one hour at a time, and the reconcile deletes by hour.
CREATE INDEX IF NOT EXISTS rated_video_usage_window_ix ON rated_video_usage (window_start);

-- parked_video_usage: conflicted video usage parked per (hour, identity,
-- reason), never dropped and never holding back the rest of the hour: the
-- video twin of parked_usage (0012), with video quantities. The identity is
-- the same gate identity as tokens: (auth_id, owner_type, owner_id,
-- resource_id, model_id, serving_mode). Reasons:
--   owner_conflict  jobs carrying both a user and a group (identity owner '')
--   ambiguous_org   more than one org for the identity in the hour
--   missing_org     every job of the identity has org_id '' (0013 makes org_id
--                   NOT NULL, so only an empty string can land here)
-- There is no ambiguous_base: base_model is part of the video grain, so two base
-- models are two records, not a conflict. NEVER pruned.
CREATE TABLE IF NOT EXISTS parked_video_usage (
    window_start     TIMESTAMPTZ   NOT NULL,
    auth_id          VARCHAR(64)   NOT NULL,
    owner_type       VARCHAR(16)   NOT NULL,
    owner_id         VARCHAR(64)   NOT NULL,
    resource_id      VARCHAR(64)   NOT NULL,
    model_id         VARCHAR(255)  NOT NULL,
    serving_mode     VARCHAR(32)   NOT NULL,
    reason           VARCHAR(32)   NOT NULL,
    event_count      BIGINT        NOT NULL,
    output_seconds   NUMERIC(30,9) NOT NULL,
    output_frames    BIGINT        NOT NULL,
    request_count    BIGINT        NOT NULL,
    gpu_seconds      NUMERIC(24,3) NOT NULL,
    first_parked_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_seen_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    resolved_at      TIMESTAMPTZ,
    CONSTRAINT parked_video_usage_pk PRIMARY KEY
        (window_start, auth_id, owner_type, owner_id, resource_id, model_id, serving_mode, reason),
    CONSTRAINT parked_video_usage_reason_ck CHECK
        (reason IN ('owner_conflict', 'ambiguous_org', 'missing_org'))
);

CREATE INDEX IF NOT EXISTS parked_video_usage_unresolved_ix
    ON parked_video_usage (window_start) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS parked_video_usage_resolved_at_ix
    ON parked_video_usage (resolved_at) WHERE resolved_at IS NOT NULL;

-- pushed_usage.kind: which kind each usage_id the manager holds is, so a parked
-- or held-back identity holds only the records of its own kind. A token
-- identity and a video identity can share (auth, owner, resource, model,
-- serving mode); without the kind, parking one would hold the other's records
-- that should be deleted by absence. The previous release's token-push writes
-- token rows without naming the column, so the default is 'token'.
ALTER TABLE pushed_usage ADD COLUMN IF NOT EXISTS kind VARCHAR(16) NOT NULL DEFAULT 'token';
