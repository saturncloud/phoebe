-- video_job.files_delete_failed_at: when the phoebe-videos reaper last failed
-- to delete a job's files. The reaper lists expired files with jobs never
-- tried first and then the ones that failed longest ago, so a batch of files
-- that cannot be deleted (the store or Atlas keeps refusing) does not hold back
-- the files of newer jobs. NULL means no delete of this job's files has failed.
--
-- Expand step: a nullable column with no default. The previous release's
-- phoebe-videos never reads or writes it.
ALTER TABLE video_job ADD COLUMN IF NOT EXISTS files_delete_failed_at TIMESTAMPTZ;
