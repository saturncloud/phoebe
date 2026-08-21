-- serving_mode: the serving-mode SKU axis ("shared" | "dedicated"), captured at
-- meter time from the trusted X-Saturn-Serving-Mode header (or gateway
-- resolution) and written by the drainer alongside base_model/adapter.
--
-- NULLABLE, and NULL (or '') = dedicated — the absence-of-prefix pricing
-- contract: every event written before this column existed is a dedicated
-- event and prices exactly as before (the rater's CASE treats NULL/'' /
-- anything-but-'shared' as the bare base price key). 'shared' prices from the
-- distinct 'shared:'||base_model rate row. The drainer stores '' as NULL
-- (nullStr), keeping the column faithful to "absence = dedicated".
--
-- Mirrors the Atlas-side migration d9e2f3a41b58 (same name/type/semantics) so
-- an install pointing the drainer at either database sees one contract.
--
-- No index: serving_mode is never a scan predicate — the rater reads it inside
-- its window CTE (filtered by the existing rating-instant index) only to
-- compose the price key.
ALTER TABLE billing_event
    ADD COLUMN serving_mode VARCHAR(32);
