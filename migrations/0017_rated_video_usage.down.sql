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
--     never stamped and refuses.
-- Stop token-push and the rater first (suspend their CronJobs) so neither
-- writes these tables; do not run the previous release's token-push before
-- this has run (migrations/README.md, "Rollout order for migration 0017").
DO $$
DECLARE
    parked bigint;
    unsent bigint;
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
END $$;

-- The previous release's token-push knows only token records. Video usage_ids
-- are dropped from its bookkeeping: it sends kinds ["token"] only, so the
-- manager's video records for those hours are never deleted by its pushes.
DELETE FROM pushed_usage WHERE kind <> 'token';
ALTER TABLE pushed_usage DROP COLUMN IF EXISTS kind;

DROP TABLE IF EXISTS video_reconcile_deletion;
DROP TABLE IF EXISTS parked_video_usage;
DROP TABLE IF EXISTS rated_video_usage;
