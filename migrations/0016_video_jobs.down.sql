-- Drops the video job tables. The files the jobs stored in object storage are
-- not touched here; they expire on their own retention in the bucket only if
-- the phoebe-videos reaper runs, so delete the tf-videos/ prefix by hand after
-- a downgrade if any jobs completed.
DROP TABLE IF EXISTS video_billing_event;
DROP TABLE IF EXISTS video_job;
