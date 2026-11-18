-- Reverse 0017. rated_video_usage and parked_video_usage are the only durable
-- record of video usage once video_billing_event is pruned (30 days), so this
-- REFUSES while either holds usage the manager does not have:
--   - an unresolved parked_video_usage row (unbilled, conflicted usage), or
--   - rated_video_usage the manager has not accepted since it was last
--     written. Per identity-hour (auth, owner, resource, model, serving mode,
--     hour): the hour's last accepted push (pushed_hour.pushed_at) must be at
--     or after the identity's latest rated_at, and that push must have
--     recorded at least one video pushed_usage id per rated row of the
--     identity. An accepted push writes pushed_usage and pushed_hour in one
--     transaction, so its ids carry pushed_at = pushed_hour.pushed_at; an id
--     recorded later (RecordIntent before a send that failed) has a later
--     pushed_at and does not count. pushed_hour alone is not enough: any
--     accepted push stamps it, including a token-only one (the previous
--     release's token-push), which says nothing about the hour's video. The
--     count is per identity because rated_video_usage does not store the
--     usage_id; a grain held back by token-push leaves the identity short of
--     ids and refuses.
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
    SELECT COUNT(*) INTO unsent
      FROM (SELECT window_start, auth_id, owner_type, owner_id, resource_id, model_id, serving_mode,
                   COUNT(*) AS n_rated, MAX(rated_at) AS rated_at
              FROM rated_video_usage
             GROUP BY window_start, auth_id, owner_type, owner_id, resource_id, model_id, serving_mode) r
      LEFT JOIN pushed_hour h ON h.window_start = r.window_start
     WHERE h.pushed_at IS NULL
        OR h.pushed_at < r.rated_at
        OR (SELECT COUNT(*) FROM pushed_usage u
             WHERE u.kind = 'video' AND u.window_start = r.window_start
               AND u.auth_id = r.auth_id AND u.owner_type = r.owner_type AND u.owner_id = r.owner_id
               AND u.resource_id = r.resource_id AND u.model_id = r.model_id
               AND u.serving_mode = r.serving_mode
               AND u.pushed_at >= r.rated_at AND u.pushed_at <= h.pushed_at) < r.n_rated;
    IF unsent > 0 THEN
        RAISE EXCEPTION '0017 down: % rated_video_usage identity-hour(s) were never accepted by the manager since they were last rated. Run token-push over their hours (or export the table) before migrating down.', unsent;
    END IF;
END $$;

-- The previous release's token-push knows only token records. Video usage_ids
-- are dropped from its bookkeeping: it sends kinds ["token"] only, so the
-- manager's video records for those hours are never deleted by its pushes.
DELETE FROM pushed_usage WHERE kind <> 'token';
ALTER TABLE pushed_usage DROP COLUMN IF EXISTS kind;

DROP TABLE IF EXISTS parked_video_usage;
DROP TABLE IF EXISTS rated_video_usage;
