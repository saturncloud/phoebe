-- deferred_hour: written by token-push. An hour the manager refused because it
-- does not accept, from this install, a usage kind the hour lists: a 409
-- {"error": "install billing path is legacy"} (a manager older than the rule
-- that accepts video from installs on the legacy billing path,
-- saturn-aws-manager#334), or a 400 whose validation path is `kinds` (a
-- manager older than the video kind). Such a refusal is about the manager's
-- version and the kinds the hour lists, not about the hour's records or the
-- whole install: the same manager keeps accepting this install's token-only
-- hours. The manager stores nothing for a refused request, so it still holds
-- what it held for the hour before.
--
-- A deferred hour is re-sent by every run, its payload changed or not, until
-- the manager accepts it (after the manager is upgraded). An accepted push of
-- the hour deletes its row, and so does parking it (refused_hour): an hour is
-- deferred or parked, never both. Without this table an hour accepted earlier
-- with token records only, then refused once video was rated into it, would be
-- re-sent only while it is in the trailing window, and its video would never
-- reach the manager.
--
-- status            the HTTP status of the last refusal (409 or 400)
-- message           the manager's response body
-- refusals          how many times the hour has been refused this way
-- last_deferred_at  when a run last sent the hour and was refused; runs retry
--                   the least recently refused hours first
--
-- Expand step: a new table. The previous release's token-push never reads or
-- writes it.
CREATE TABLE IF NOT EXISTS deferred_hour (
    window_start      TIMESTAMPTZ NOT NULL,
    status            INTEGER NOT NULL,
    message           TEXT NOT NULL,
    refusals          BIGINT NOT NULL DEFAULT 1,
    first_deferred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_deferred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT deferred_hour_pk PRIMARY KEY (window_start)
);

CREATE INDEX IF NOT EXISTS deferred_hour_last_deferred_at_ix ON deferred_hour (last_deferred_at);
