-- Parked usage settled by hand (Hugo, 2026-10-27: parked usage follows the
-- 30-day pruning rule unchanged and is settled by hand). Until now the only
-- way to record a hand settlement was an UPDATE that set resolved_at, which
-- left no trace of who settled the row or why, and looked the same as a
-- conflict the rater found gone.
--
-- `phoebe-rater -settle-parked` sets resolved_at (so the row stops paging and
-- token-push re-sends the hour without it, as for any resolved row) and these
-- three columns in the same UPDATE. The row is kept: it is the record of the
-- unbilled usage and of the settlement.
--
-- settled_at      when an operator settled the row by hand (NULL: not settled
--                 by hand; a non-NULL resolved_at without it was resolved by a
--                 rater run)
-- settled_by      who settled it (free text, required by the tool)
-- settle_reason   why, and what was done in the manager (free text, required)
--
-- Expand step: nullable columns only. The previous release never reads them,
-- and its upsert of a re-parked conflict leaves them unchanged.
ALTER TABLE parked_usage ADD COLUMN IF NOT EXISTS settled_at TIMESTAMPTZ;
ALTER TABLE parked_usage ADD COLUMN IF NOT EXISTS settled_by TEXT;
ALTER TABLE parked_usage ADD COLUMN IF NOT EXISTS settle_reason TEXT;
ALTER TABLE parked_video_usage ADD COLUMN IF NOT EXISTS settled_at TIMESTAMPTZ;
ALTER TABLE parked_video_usage ADD COLUMN IF NOT EXISTS settled_by TEXT;
ALTER TABLE parked_video_usage ADD COLUMN IF NOT EXISTS settle_reason TEXT;
