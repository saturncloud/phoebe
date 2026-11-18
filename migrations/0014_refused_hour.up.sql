-- refused_hour: written by token-push. An hour the manager refused for its
-- content (400 validation failure, 413, 422), or that token-push itself found
-- breaks a request-level wire rule (meteredusage.Validate; status 0), is
-- PARKED here. Sending the same payload again would get the same answer, so a
-- parked hour is not re-sent on every run and does not count against the
-- per-run cap on missed hours. It becomes eligible again when the payload
-- token-push would build for it changes: each run rebuilds a bounded number of
-- parked hours (oldest checked_at first) and sends any whose payload_sha256
-- differs. An accepted push of the hour deletes its row.
--
-- Whole-install refusals (401, 403, a 409 from a manager older than the
-- 2026-11-17 rule) are never recorded here: they are about the install, not the
-- hour, and token-push keeps re-sending once they are fixed.
--
-- status          the HTTP status of the last refusal; 0 = refused locally
-- message         the manager's response body (or the local validation error)
-- payload_sha256  hex SHA-256 of the refused request body
-- refusals        how many times the hour has been refused
-- checked_at      when a run last rebuilt the hour and found the payload unchanged
CREATE TABLE IF NOT EXISTS refused_hour (
    window_start     TIMESTAMPTZ NOT NULL,
    status           INTEGER NOT NULL,
    message          TEXT NOT NULL,
    payload_sha256   CHAR(64) NOT NULL,
    refusals         BIGINT NOT NULL DEFAULT 1,
    first_refused_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_refused_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    checked_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT refused_hour_pk PRIMARY KEY (window_start)
);

CREATE INDEX IF NOT EXISTS refused_hour_checked_at_ix ON refused_hour (checked_at);
