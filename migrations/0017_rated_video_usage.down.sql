-- Reverse 0017. rated_video_usage and parked_video_usage are the only durable
-- record of video usage once video_billing_event is pruned (30 days), so this
-- REFUSES while either holds usage the manager does not have:
--   - an unresolved parked_video_usage row (unbilled, conflicted usage), or
--   - a rated_video_usage row the manager has not accepted since the row was
--     last written: no video pushed_usage row of its identity and hour that
--     was recorded at or after rated_at. pushed_hour is not enough: it is
--     stamped by any accepted push, including a token-only one (the previous
--     release's token-push, or a run that left video out), which says nothing
--     about the hour's video.
-- Roll back token-push and the rater first, so neither writes these tables.
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
     WHERE NOT EXISTS (
         SELECT 1 FROM pushed_usage u
          WHERE u.kind = 'video'
            AND u.window_start = v.window_start
            AND u.auth_id = v.auth_id AND u.owner_type = v.owner_type AND u.owner_id = v.owner_id
            AND u.resource_id = v.resource_id AND u.model_id = v.model_id
            AND u.serving_mode = v.serving_mode
            AND u.pushed_at >= v.rated_at);
    IF unsent > 0 THEN
        RAISE EXCEPTION '0017 down: % rated_video_usage rows were never accepted by the manager since they were last rated. Run token-push over their hours (or export the table) before migrating down.', unsent;
    END IF;
END $$;

-- The previous release's token-push knows only token records. Video usage_ids
-- are dropped from its bookkeeping: it sends kinds ["token"] only, so the
-- manager's video records for those hours are never deleted by its pushes.
DELETE FROM pushed_usage WHERE kind <> 'token';
ALTER TABLE pushed_usage DROP COLUMN IF EXISTS kind;

DROP TABLE IF EXISTS parked_video_usage;
DROP TABLE IF EXISTS rated_video_usage;
