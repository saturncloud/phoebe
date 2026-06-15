-- io_log: phoebe's M5 I/O-logging store for request/response BODIES.
--
-- One row per SAMPLED, OPTED-IN request, written by the interceptor's iolog
-- PostgresSink (internal/iolog). This is a SEPARATE store from billing_event
-- (the metering system-of-record) with its own retention and access posture:
--   - billing_event is durable money data, retained long-term.
--   - io_log is best-effort, sampled, SHORT-RETENTION debug telemetry. A dropped
--     row is acceptable (the sink drops-and-counts on overflow), and rows are
--     pruned aggressively by a retention job scanning created_at.
--
-- Bodies are sensitive (prompts/completions/PII), which is why capture is
-- opt-in + sampled at the policy gate (internal/iolog/policy.go) and OFF by
-- default. This table only ever holds what that gate let through.
--
-- body_tsv is a tsvector over the request+response bodies with a GIN index, so
-- operators can full-text "grep inside bodies" (e.g. find requests that produced
-- a given error string). pgvector/semantic search is a LATER milestone and is
-- intentionally NOT added here.
--
-- request_id is an engine/OpenAI request id (not an Atlas 32-char hex), hence
-- varchar(255). It is NOT a primary key here: unlike billing_event (one durable
-- row per request, upserted ON CONFLICT), io_log is append-only best-effort and
-- a request may legitimately produce zero or one sampled rows. We index it for
-- joins back to billing_event rather than constraining uniqueness.
--
-- NOTE: this .sql is for reference and local dev only. In the shared Atlas
-- Postgres the table is created by the Alembic chain — see
-- migrations/atlas/<rev>_add_io_log.py and migrations/README.md. Keep the two
-- (and internal/iolog/postgres.go's insert column list) in sync.

CREATE TABLE io_log (
    -- Surrogate key: io_log is append-only, so a generated id is cleaner than
    -- overloading request_id (which is not unique here).
    id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    request_id         VARCHAR(255) NOT NULL,

    -- Identity, captured verbatim from atlas-auth headers (nullable: not every
    -- token carries every field). auth_id is the attribution key.
    auth_id            VARCHAR(64),
    user_id            VARCHAR(32),
    group_id           VARCHAR(32),
    resource_id        VARCHAR(64),
    resource_type      VARCHAR(64),

    -- Workload.
    model              VARCHAR(255),

    -- The captured bodies. TEXT (unbounded at the column level); the BUFFERED
    -- size is capped in the sink (default 256 KiB for the response) so rows stay
    -- bounded. response_truncated flags a response cut at that cap.
    request_body       TEXT,
    response_body      TEXT,
    response_truncated BOOLEAN NOT NULL DEFAULT FALSE,

    status_code        INTEGER,
    streamed           BOOLEAN NOT NULL DEFAULT FALSE,
    latency_ms         BIGINT,

    -- created_at: when the request completed / the row was stamped. Drives the
    -- retention scan.
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Full-text vector over the two bodies, populated at INSERT time by the sink
    -- (to_tsvector('simple', request_body || ' ' || response_body)).
    body_tsv           TSVECTOR
);

-- GIN index over the body tsvector: the "grep inside bodies" capability.
CREATE INDEX io_log_body_tsv_ix
    ON io_log USING GIN (body_tsv);

-- Per-API-key troubleshooting: "show me this key's recent requests".
CREATE INDEX io_log_auth_id_created_at_ix
    ON io_log (auth_id, created_at);

-- Retention scans (prune rows older than the short retention window).
CREATE INDEX io_log_created_at_ix
    ON io_log (created_at);

-- Join back to billing_event by request id.
CREATE INDEX io_log_request_id_ix
    ON io_log (request_id);
