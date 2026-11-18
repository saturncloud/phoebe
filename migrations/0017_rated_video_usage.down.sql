-- Reverse 0017. rated_video_usage and parked_video_usage are the only durable
-- record of video usage once video_billing_event is pruned (30 days), so this
-- REFUSES while either holds usage the manager does not have:
--   - an unresolved parked_video_usage row (unbilled, conflicted usage), or
--   - a rated_video_usage row the manager has not accepted since it was last
--     written: its own usage_id has no video pushed_usage row recorded at or
--     after rated_at by an accepted push (pushed_at at or before the hour's
--     pushed_hour.pushed_at; an accepted push writes both in one transaction,
--     so they are equal, while an id recorded only as intent before a send
--     that failed carries a later pushed_at). Matched per usage_id, so a
--     sibling grain's id cannot stand in for it. A row with no usage_id was
--     never stamped and refuses, or
--   - a video record the rater removed (a tombstone) whose id the manager
--     still holds (pushed_usage has it).
-- Stop token-push and the rater first (suspend their CronJobs) so neither
-- writes these tables; do not run the previous release's token-push before
-- this has run (migrations/README.md, "Rollout order for migration 0017").
-- A database that applied 0017 before usage_id existed has no such column:
-- add it so the guard below runs (every such row is then unstamped and
-- refuses, as it should: nothing proves the manager accepted it).
ALTER TABLE rated_video_usage ADD COLUMN IF NOT EXISTS usage_id CHAR(64);

DO $$
DECLARE
    parked bigint;
    unsent bigint;
    removed bigint;
BEGIN
    SELECT COUNT(*) INTO parked FROM parked_video_usage WHERE resolved_at IS NULL;
    IF parked > 0 THEN
        RAISE EXCEPTION '0017 down: % parked_video_usage rows are unresolved (unbilled video usage). Resolve them or export them before migrating down.', parked;
    END IF;
    SELECT COUNT(*) INTO unsent FROM rated_video_usage v
      LEFT JOIN pushed_hour h ON h.window_start = v.window_start
     WHERE v.usage_id IS NULL
        OR NOT EXISTS (SELECT 1 FROM pushed_usage u
                        WHERE u.kind = 'video' AND u.window_start = v.window_start
                          AND u.usage_id = v.usage_id
                          AND u.pushed_at >= v.rated_at AND u.pushed_at <= h.pushed_at);
    IF unsent > 0 THEN
        RAISE EXCEPTION '0017 down: % rated_video_usage row(s) were never accepted by the manager since they were last rated. Run token-push over their hours (or export the table) before migrating down.', unsent;
    END IF;
    -- A video record the rater removed (tombstone) that the manager still
    -- holds (its id is still in pushed_usage): the previous release never
    -- lists video, so the record would stay at the manager for good.
    -- (A database from before the tombstones has no tombstone table.)
    IF to_regclass('video_reconcile_deletion') IS NOT NULL THEN
        SELECT COUNT(*) INTO removed FROM pushed_usage u
          JOIN video_reconcile_deletion t ON t.window_start = u.window_start AND t.usage_id = u.usage_id
         WHERE u.kind = 'video';
    END IF;
    IF removed > 0 THEN
        RAISE EXCEPTION '0017 down: % video record(s) the rater removed are still held by the manager. Run token-push over their hours so the manager deletes them before migrating down.', removed;
    END IF;
END $$;

-- The previous release's token-push knows only token records. Video usage_ids
-- are dropped from its bookkeeping: it sends kinds ["token"] only, so the
-- manager's video records for those hours are never deleted by its pushes.
DELETE FROM pushed_usage WHERE kind <> 'token';
ALTER TABLE pushed_usage DROP COLUMN IF EXISTS kind;

DROP TABLE IF EXISTS video_reconcile_deletion;
DROP TABLE IF EXISTS parked_video_usage;
DROP TABLE IF EXISTS rated_video_usage;
