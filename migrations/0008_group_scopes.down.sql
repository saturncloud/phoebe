-- Roll back 0008. group_usage is fully owned by this migration (drop the
-- table); member_group_ids is evidence only (drop the column). Rated windows
-- keep their rated_usage money rows either way -- this rollback never touches
-- money. Re-rating a window after a down/up cycle rebuilds the group rows.

DROP TABLE IF EXISTS group_usage;

ALTER TABLE billing_event DROP COLUMN IF EXISTS member_group_ids;
