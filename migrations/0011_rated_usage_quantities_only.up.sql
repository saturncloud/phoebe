-- The manager prices every hour (billing-design.md D1, ruled by Hugo
-- 2026-10-22). phoebe's rater no longer fetches prices or computes money: it
-- aggregates billing_event into rated_usage at the same grain, and token-push
-- sends the quantities to POST /customer/metered-usage.
--
-- So the money columns stop being written. This is the EXPAND step: they become
-- nullable and lose their $0 defaults, so the new rater writes NULL (never a $0
-- that would read as "served, but free"), while the previous release's rater
-- (which may still run once during an upgrade) keeps writing real values.
-- price_key (0010) is already nullable and is likewise written NULL from now on.
--
-- The CONTRACT step (dropping cost, applied_*_rate and price_key, and the
-- reconciliation view's rated_cost column that reads cost) is one release later,
-- per "Upgrade order and backward compatibility" in README.md.
ALTER TABLE rated_usage ALTER COLUMN cost DROP NOT NULL;
ALTER TABLE rated_usage ALTER COLUMN applied_prompt_rate DROP NOT NULL;
ALTER TABLE rated_usage ALTER COLUMN applied_prompt_rate DROP DEFAULT;
ALTER TABLE rated_usage ALTER COLUMN applied_cached_rate DROP NOT NULL;
ALTER TABLE rated_usage ALTER COLUMN applied_cached_rate DROP DEFAULT;
ALTER TABLE rated_usage ALTER COLUMN applied_completion_rate DROP NOT NULL;
ALTER TABLE rated_usage ALTER COLUMN applied_completion_rate DROP DEFAULT;

COMMENT ON COLUMN rated_usage.cost IS
    'Unused since 0011: the manager prices usage. NULL on rows the aggregator wrote; dropped in a later release.';
