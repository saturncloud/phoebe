-- Serving mode is spelled 'shared' or 'dedicated' everywhere (Hugo's 2026-09-29
-- ruling, made in the pre-production window). Before this migration a dedicated
-- rollup stored serving_mode = '' (the absence of the shared: price-key prefix).
-- From here on the empty string is not a serving mode: the proxy resolves a
-- dedicated route to 'dedicated' before metering, the rater withholds any event
-- whose serving mode is not one of the two values, the push sends the explicit
-- value, and saturn-aws-manager rejects '' on the wire.
--
-- The price-key grammar is NOT changed: a dedicated price row is still the bare
-- base id and a shared one is 'shared:<base>'. Only the serving_mode VALUE moves.
--
-- WHY THE ID IS RECOMPUTED. rated_usage.id is md5 of the natural key, and
-- serving_mode is part of that key (see internal/rating/store.go, the upserted
-- CTE). A row rewritten to 'dedicated' must therefore also get the id the rater
-- would now compute for it; otherwise a later re-rate of the same window would
-- update the row in place (ON CONFLICT on the natural key) but keep an id no
-- other code can reproduce. The expression below is the rater's, character for
-- character. There are no production rows (the ruling's premise), so changing
-- dedicated ids is allowed; saturn-aws-manager sees the new id on its next push
-- of the window and replaces the old record by delete-by-absence.
--
-- TWO CLEAN-UPS HAPPEN FIRST, both on staging data only:
--  1. A '' row whose natural-key twin already exists as 'dedicated' (possible if
--     a trusted header ever stamped the explicit spelling before the cutover)
--     cannot be renamed without violating the unique key. The '' row is deleted;
--     the 'dedicated' twin stays.
--  2. A row with any other value (neither '', 'shared' nor 'dedicated') has no
--     legal meaning and is deleted.
-- The raw evidence for both remains in billing_event.
--
-- NOTE ON RE-RATING: pre-cutover dedicated evidence in billing_event has a NULL
-- serving_mode. The rater now withholds such events (invalid_serving_mode_events),
-- so a re-rate of a pre-cutover window deletes the rows rewritten here through the
-- normal reconcile path. That is intended: there is nothing in production to keep.

DELETE FROM rated_usage ru
WHERE ru.serving_mode = ''
  AND EXISTS (
      SELECT 1 FROM rated_usage d
      WHERE d.serving_mode = 'dedicated'
        AND d.auth_id      = ru.auth_id
        AND d.owner_type   = ru.owner_type
        AND d.owner_id     = ru.owner_id
        AND d.resource_id  = ru.resource_id
        AND d.model_id     = ru.model_id
        AND d.window_start = ru.window_start
  );

DELETE FROM rated_usage
WHERE serving_mode NOT IN ('', 'shared', 'dedicated');

UPDATE rated_usage
SET serving_mode = 'dedicated',
    id = md5(length(auth_id)::text || ':' || auth_id
          || '|' || length(owner_type)::text || ':' || owner_type
          || '|' || length(owner_id)::text || ':' || owner_id
          || '|' || length(resource_id)::text || ':' || resource_id
          || '|' || length(model_id)::text || ':' || model_id
          || '|' || length('dedicated')::text || ':' || 'dedicated'
          || '|' || extract(epoch FROM window_start)::bigint::text)
WHERE serving_mode = '';

-- No default: every writer (the rater) supplies the value, and a default of
-- either spelling would hide a writer that forgot it.
ALTER TABLE rated_usage ALTER COLUMN serving_mode DROP DEFAULT;

ALTER TABLE rated_usage
    ADD CONSTRAINT rated_usage_serving_mode_ck CHECK (serving_mode IN ('shared', 'dedicated'));

COMMENT ON COLUMN rated_usage.serving_mode IS
    'Serving-mode SKU axis: ''shared'' or ''dedicated'' (CHECK-enforced; the empty string was retired on 2026-09-29). KEY COLUMN: the two price from different SKUs and must never merge into one rollup.';

-- billing_event is the evidence ledger and deliberately has no CHECK. Only its
-- description changes: NULL no longer means dedicated.
COMMENT ON COLUMN billing_event.serving_mode IS
    'Serving mode captured at meter time: ''shared'' or ''dedicated''. NULL or empty only on events metered before the 2026-09-29 serving-mode cutover (when it meant dedicated); the rater withholds those from money and counts them as invalid_serving_mode_events.';
