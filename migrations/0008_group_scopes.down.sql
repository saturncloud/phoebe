-- Roll back 0008. group_usage is fully owned by this migration (drop the
-- table); member_group_ids is evidence only (drop the column). Rated windows
-- keep their rated_usage money rows either way -- this rollback never touches
-- money.
--
-- THIS ROLLBACK IS LOSSY. member_group_ids is the only copy of the membership
-- evidence: dropping the column destroys it, and re-applying the up migration
-- only re-adds an empty (NULL) column. After a down/up cycle, re-rating a
-- window rebuilds group_usage from group-token usage (billing_event.group_id)
-- only -- membership attribution is permanently lost, so month-to-date group
-- spend restarts lower than it really is and spend caps can over-admit until
-- real usage re-accumulates. Operators should avoid a mid-month rollback.

DROP TABLE IF EXISTS group_usage;

ALTER TABLE billing_event DROP COLUMN IF EXISTS member_group_ids;
