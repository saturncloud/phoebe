-- token_push_state: token-push's own single-row state.
--
-- auto_floor is the first hour accepted by a SCHEDULED token-push run (a run
-- with no -since/-until). A scheduled run re-sends, without an operator, every
-- hour after auto_floor that has usage and was never accepted. Hours at or
-- before it (usage from before the install was connected, and hours refused or
-- failed before that first acceptance) are sent only by an explicit
-- -since/-until run. An explicit run never writes auto_floor, so a backfill
-- cannot pull older usage into the automatic re-send.
--
-- Before this table, the floor was min(pushed_hour.window_start), which an
-- explicit backfill lowered. The backfill below keeps that floor for installs
-- that already pushed; nothing is inserted on an install that never pushed.
CREATE TABLE IF NOT EXISTS token_push_state (
    singleton  BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    auto_floor TIMESTAMPTZ NOT NULL
);

INSERT INTO token_push_state (auto_floor)
SELECT min(window_start) FROM pushed_hour HAVING min(window_start) IS NOT NULL
ON CONFLICT (singleton) DO NOTHING;
