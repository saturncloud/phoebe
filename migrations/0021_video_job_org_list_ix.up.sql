-- The org host's GET /v1/videos (issue #71) lists every video of one org,
-- across all its endpoints, newest first, with cursor pagination on
-- (created_at, id). video_job_list_ix leads with resource_id, so it cannot
-- serve that list; this index does. The per-endpoint list (and the org
-- host's model filter, which resolves to one endpoint) keeps using
-- video_job_list_ix.
--
-- Expand step: an index only. The previous release's phoebe-videos never
-- depends on it.
CREATE INDEX IF NOT EXISTS video_job_org_list_ix ON video_job (org_id, created_at, id);
