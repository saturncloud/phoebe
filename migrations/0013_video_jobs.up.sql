-- Video generation jobs (video models design, approved 2026-11-18, sections 5-7).
--
-- Saturn owns the job, not the engine: the engine keeps jobs in memory and
-- writes files to the pod's disk, which loses work on a restart. Every job a
-- customer creates with POST /v1/videos is a row here, written and read only by
-- the phoebe-videos service (cmd/videos). New tables only: nothing the previous
-- release writes is touched.

-- video_job: one row per job. The row is the source of truth for the job's
-- status; the Valkey stream per endpoint only orders the queue and wakes the
-- dispatcher, so a lost stream entry is re-added from here, never lost.
--
-- Access is decided on (org_id, resource_id): a request reads or changes a job
-- only when both match the trusted identity of the request (another org's or
-- another endpoint's job answers 404).
--
-- Retention: the files (input image, video, thumbnail) are deleted when
-- expires_at passes (24 hours after completion by default) and files_deleted_at
-- is set; the prompt is erased at the same time. The row itself is deleted 30
-- days after it was created, like billing_event.
CREATE TABLE IF NOT EXISTS video_job (
    id                VARCHAR(40)  NOT NULL PRIMARY KEY,   -- video_<26-char ULID>
    org_id            VARCHAR(64)  NOT NULL,
    auth_id           VARCHAR(64)  NOT NULL,
    user_id           VARCHAR(64),
    group_id          VARCHAR(64),
    resource_id       VARCHAR(64)  NOT NULL,
    served_model      VARCHAR(255) NOT NULL,               -- the request's model= (the endpoint name)
    base_model        VARCHAR(255) NOT NULL,
    serving_mode      VARCHAR(32)  NOT NULL,
    upstream          TEXT         NOT NULL,               -- host:port of the engine, from X-Saturn-Upstream
    region            VARCHAR(64),
    task              VARCHAR(8)   NOT NULL,
    prompt            TEXT,                                -- NULL once the files expire
    seconds           INTEGER      NOT NULL,
    size              VARCHAR(16)  NOT NULL,
    fps               INTEGER      NOT NULL,
    gpus              INTEGER      NOT NULL,
    params            JSONB        NOT NULL DEFAULT '{}',  -- negative_prompt, seed, num_inference_steps, guidance_scale
    status            VARCHAR(16)  NOT NULL,
    progress          SMALLINT     NOT NULL DEFAULT 0,
    error_code        VARCHAR(32),
    error_message     TEXT,
    attempts          SMALLINT     NOT NULL DEFAULT 0,
    lease_until       TIMESTAMPTZ,                         -- set while in_progress; renewed by the dispatcher
    enqueued_at       TIMESTAMPTZ  NOT NULL DEFAULT now(), -- last time the job id was added to the stream
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    started_at        TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    expires_at        TIMESTAMPTZ,
    files_deleted_at  TIMESTAMPTZ,
    input_key         TEXT,
    video_key         TEXT,
    thumbnail_key     TEXT,
    video_bytes       BIGINT,
    output_frames     INTEGER,
    output_seconds    NUMERIC(12,4),
    gpu_seconds       NUMERIC(14,3),
    CONSTRAINT video_job_status_ck CHECK
        (status IN ('queued', 'in_progress', 'completed', 'failed', 'cancelled')),
    CONSTRAINT video_job_task_ck CHECK (task IN ('t2v', 'i2v')),
    CONSTRAINT video_job_error_code_ck CHECK (error_code IS NULL OR error_code IN
        ('engine_error', 'timeout', 'queue_timeout', 'output_too_large', 'cancelled')),
    CONSTRAINT video_job_progress_ck CHECK (progress BETWEEN 0 AND 100)
);

-- GET /v1/videos lists one endpoint's jobs for one org, newest first, with
-- cursor pagination on (created_at, id).
CREATE INDEX IF NOT EXISTS video_job_list_ix ON video_job (resource_id, org_id, created_at, id);
-- The dispatcher and the reaper scan open jobs only.
CREATE INDEX IF NOT EXISTS video_job_open_ix ON video_job (resource_id, status)
    WHERE status IN ('queued', 'in_progress');
-- File expiry scans completed jobs whose files are still stored.
CREATE INDEX IF NOT EXISTS video_job_expiry_ix ON video_job (expires_at)
    WHERE files_deleted_at IS NULL AND expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS video_job_created_ix ON video_job (created_at);

-- video_billing_event: the raw metering record of one finished job, keyed by the
-- job id. Written in the same transaction that moves the job to a final status,
-- so every finished job has exactly one row and no row exists for an unfinished
-- job. Failed and cancelled jobs carry zero quantities and their status: every
-- job is accounted for, and only completed jobs are billed (request_count 1).
--
-- A separate table from billing_event because the units differ: billing_event's
-- columns and checks are token shaped, and one table holding two units invites
-- a query that sums the wrong thing (design 7.2). Pruned with the job rows (30
-- days on created_at); the hourly rollup outlives it.
CREATE TABLE IF NOT EXISTS video_billing_event (
    job_id          VARCHAR(40)   NOT NULL PRIMARY KEY,
    created_at      TIMESTAMPTZ   NOT NULL,                -- when the job finished; the rollup hour
    org_id          VARCHAR(64)   NOT NULL,
    auth_id         VARCHAR(64)   NOT NULL,
    user_id         VARCHAR(64),
    group_id        VARCHAR(64),
    resource_id     VARCHAR(64)   NOT NULL,
    model_id        VARCHAR(255)  NOT NULL,                -- the served model (endpoint name)
    base_model      VARCHAR(255)  NOT NULL,
    serving_mode    VARCHAR(32)   NOT NULL,
    task            VARCHAR(8)    NOT NULL,
    resolution      VARCHAR(8)    NOT NULL,
    region          VARCHAR(64),
    status          VARCHAR(16)   NOT NULL,
    output_seconds  NUMERIC(12,4) NOT NULL DEFAULT 0,
    output_frames   INTEGER       NOT NULL DEFAULT 0,
    request_count   INTEGER       NOT NULL DEFAULT 0,
    gpu_seconds     NUMERIC(14,3) NOT NULL DEFAULT 0,
    CONSTRAINT video_billing_event_status_ck CHECK (status IN ('completed', 'failed', 'cancelled')),
    CONSTRAINT video_billing_event_task_ck CHECK (task IN ('t2v', 'i2v')),
    CONSTRAINT video_billing_event_resolution_ck CHECK (resolution IN ('480p', '720p', '1080p', '4k')),
    CONSTRAINT video_billing_event_serving_mode_ck CHECK (serving_mode IN ('shared', 'dedicated')),
    -- Only a completed job carries usage.
    CONSTRAINT video_billing_event_unbilled_ck CHECK
        (status = 'completed' OR (output_seconds = 0 AND output_frames = 0 AND request_count = 0)),
    CONSTRAINT video_billing_event_count_ck CHECK (request_count IN (0, 1))
);

CREATE INDEX IF NOT EXISTS video_billing_event_created_ix ON video_billing_event (created_at);
