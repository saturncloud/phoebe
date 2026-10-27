-- Reverse 0011. Restoring NOT NULL on the money columns needs a value in every
-- row, and rows the aggregator wrote have none. Inventing $0 would make those
-- hours look served-but-free to the previous rater's token-push, so this
-- REFUSES while any such row exists.
--
-- To roll back: deploy the previous rater (it needs the manager price routes),
-- re-rate the affected hours with --since/--until so every row carries real
-- money again, then run migrate down.
DO $$
DECLARE
    unpriced bigint;
BEGIN
    SELECT COUNT(*) INTO unpriced FROM rated_usage
     WHERE cost IS NULL OR applied_prompt_rate IS NULL
        OR applied_cached_rate IS NULL OR applied_completion_rate IS NULL;
    IF unpriced > 0 THEN
        RAISE EXCEPTION '0011 down: % rated_usage rows have no money columns (written by the quantities-only aggregator). Re-rate those hours with the previous rater before migrating down; this migration will not invent a $0 cost.', unpriced;
    END IF;
END $$;

COMMENT ON COLUMN rated_usage.cost IS NULL;
ALTER TABLE rated_usage ALTER COLUMN applied_completion_rate SET DEFAULT 0;
ALTER TABLE rated_usage ALTER COLUMN applied_completion_rate SET NOT NULL;
ALTER TABLE rated_usage ALTER COLUMN applied_cached_rate SET DEFAULT 0;
ALTER TABLE rated_usage ALTER COLUMN applied_cached_rate SET NOT NULL;
ALTER TABLE rated_usage ALTER COLUMN applied_prompt_rate SET DEFAULT 0;
ALTER TABLE rated_usage ALTER COLUMN applied_prompt_rate SET NOT NULL;
ALTER TABLE rated_usage ALTER COLUMN cost SET NOT NULL;
