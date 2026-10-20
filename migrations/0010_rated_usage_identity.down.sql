-- Reverse 0010. The three usage-identity columns are attributes (no money, not
-- grain), so dropping them loses only labels. Roll back token-push first, so it
-- stops reading them.
ALTER TABLE rated_usage DROP COLUMN IF EXISTS price_key;
ALTER TABLE rated_usage DROP COLUMN IF EXISTS adapter;
ALTER TABLE rated_usage DROP COLUMN IF EXISTS base_model;
