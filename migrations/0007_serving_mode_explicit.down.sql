-- Reverse of 0007: dedicated goes back to being spelled '' in rated_usage, with
-- the id recomputed by the pre-0007 formula (the same md5 expression hashing the
-- empty string), so the rows match what the pre-0007 rater would write for them.
-- Lossless for the rows present: while the CHECK held, no '' row could exist, so
-- the rename cannot collide with the unique key.
ALTER TABLE rated_usage DROP CONSTRAINT IF EXISTS rated_usage_serving_mode_ck;

UPDATE rated_usage
SET serving_mode = '',
    id = md5(length(auth_id)::text || ':' || auth_id
          || '|' || length(owner_type)::text || ':' || owner_type
          || '|' || length(owner_id)::text || ':' || owner_id
          || '|' || length(resource_id)::text || ':' || resource_id
          || '|' || length(model_id)::text || ':' || model_id
          || '|' || length('')::text || ':' || ''
          || '|' || extract(epoch FROM window_start)::bigint::text)
WHERE serving_mode = 'dedicated';

ALTER TABLE rated_usage ALTER COLUMN serving_mode SET DEFAULT '';

COMMENT ON COLUMN rated_usage.serving_mode IS
    'Serving-mode SKU axis; '''' = dedicated (the absence-of-prefix contract), ''shared'' = shared. KEY COLUMN: the two price from different SKUs and must never merge into one rollup.';

COMMENT ON COLUMN billing_event.serving_mode IS NULL;
