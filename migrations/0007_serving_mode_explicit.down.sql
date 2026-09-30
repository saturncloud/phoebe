-- Reverse of 0007: every 'dedicated' row in rated_usage goes back to being
-- spelled '', with the id recomputed by the pre-0007 formula (the same md5
-- expression hashing the empty string). The rename cannot collide with the
-- unique key: while the CHECK held, no '' row could exist.
--
-- WHAT THIS DOES AND DOES NOT RESTORE. The rewrite is lossless for the rated_usage
-- rows that exist at rollback time. The rewritten rows match what the pre-0007
-- rater would write only for windows whose billing_event evidence predates the
-- cutover (billing_event.serving_mode NULL or ''). Events metered after the
-- cutover were stamped serving_mode = 'dedicated' by the proxy, and the pre-0007
-- rater keeps that value, because its COALESCE(ev.serving_mode, '') maps only
-- NULL to ''. So after a rollback, the next re-rate of any window that holds
-- post-cutover evidence writes a 'dedicated' rollup with a different id and
-- deletes, through the normal reconcile path, the '' row this migration restored.
-- Expect reconcile-deletion counts and new ids pushed to saturn-aws-manager for
-- those windows. An hour that also holds events from the rolled-back proxy splits
-- into a '' rollup and a 'dedicated' rollup, so a rolled-back install holds both
-- spellings of dedicated and a rolled-back manager receives both for one grain.
--
-- This migration deliberately does NOT rewrite billing_event to hide that: it is
-- the append-only evidence ledger (contracts/billing-event-ledger.md) and is never
-- edited. The limitation is accepted only because rated_usage rows are disposable
-- in the pre-production window (no production rows; the 2026-09-24 ruling, ledger
-- item 2), where they can be thrown away and rebuilt from billing_event. Once
-- production rows exist, do not roll 0007 back.
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
