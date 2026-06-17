-- billing_event: phoebe's system-of-record for RAW metering records.
--
-- One row per LLM request, written by the Postgres drainer (cmd/drainer) as it
-- consumes the Valkey metering stream. This holds pre-rating token counts only;
-- pricing/rating is applied downstream, out of band.
--
-- request_id is the idempotency key: the drainer does INSERT ... ON CONFLICT
-- (request_id) DO NOTHING, so at-least-once stream delivery yields exactly one
-- row (effectively-once). It is an engine/OpenAI request id (not an Atlas 32-char
-- hex), hence varchar(255).
--
-- NOTE: this .sql is for reference and local dev only. In the shared Atlas
-- Postgres the table is created by the Alembic chain — see
-- migrations/atlas/<rev>_add_billing_event.py and migrations/README.md. Keep the
-- two in sync.

CREATE TABLE billing_event (
    request_id        VARCHAR(255) NOT NULL,

    -- Identity, captured verbatim from atlas-auth headers (nullable: not every
    -- token carries every field). auth_id is the stable attribution key; rating
    -- resolves auth_id -> user/group/org out of band.
    auth_id           VARCHAR(64),
    user_id           VARCHAR(32),
    group_id          VARCHAR(32),
    resource_id       VARCHAR(64),
    resource_type     VARCHAR(64),

    -- Workload.
    model             VARCHAR(255),
    -- base_model: the HF base id a fine-tune derives from (E3 derived_from),
    -- stamped at deploy time by Atlas (which enforces base_model is present to
    -- deploy a fine-tune). NULL for a base-model deployment. The rater prices an
    -- ft:<checkpoint> model at base_price x premium by joining this to the price
    -- file's base rates; an ft: model with a NULL base_model fails loud (never $0).
    base_model        VARCHAR(255),
    adapter           VARCHAR(255),

    -- Raw token counts (the engine's own usage block; never re-tokenized).
    prompt_tokens     INTEGER NOT NULL DEFAULT 0,
    cached_tokens     INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,

    finish_reason     VARCHAR(64),
    gpu_type          VARCHAR(64),
    aborted           BOOLEAN NOT NULL DEFAULT FALSE,

    -- event_ts: when the interceptor stamped the event (from TimestampUnixMs).
    -- created_at: when the drainer wrote the row. They differ by the stream lag.
    event_ts          TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_billing_event PRIMARY KEY (request_id)
);

-- Per-API-key billing queries scan by auth_id over a time window.
CREATE INDEX billing_event_auth_id_created_at_ix
    ON billing_event (auth_id, created_at);

-- Time-range scans (e.g. "all events in this hour") for batch rating.
CREATE INDEX billing_event_created_at_ix
    ON billing_event (created_at);
