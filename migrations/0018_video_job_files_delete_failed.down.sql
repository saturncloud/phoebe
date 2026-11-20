-- Reverse 0018. The column only orders the reaper's retries; dropping it loses
-- nothing the previous release needs.
ALTER TABLE video_job DROP COLUMN IF EXISTS files_delete_failed_at;
