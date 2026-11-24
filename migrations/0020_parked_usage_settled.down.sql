-- Reverse 0020. A row settled by hand keeps resolved_at, so the previous
-- release treats it as resolved and does not page for it; only the record of
-- who settled it and why is lost. Copy it out first if it is needed:
--   SELECT * FROM parked_usage WHERE settled_at IS NOT NULL;
--   SELECT * FROM parked_video_usage WHERE settled_at IS NOT NULL;
ALTER TABLE parked_usage DROP COLUMN IF EXISTS settle_reason;
ALTER TABLE parked_usage DROP COLUMN IF EXISTS settled_by;
ALTER TABLE parked_usage DROP COLUMN IF EXISTS settled_at;
ALTER TABLE parked_video_usage DROP COLUMN IF EXISTS settle_reason;
ALTER TABLE parked_video_usage DROP COLUMN IF EXISTS settled_by;
ALTER TABLE parked_video_usage DROP COLUMN IF EXISTS settled_at;
