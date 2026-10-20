-- Usage identity (usage-identity contract, proposal A; coordination 2026-10-19):
-- rated_usage keeps what served each rollup, so usage still names a deleted
-- endpoint's base model and LoRA adapter after billing_event is pruned (30 days).
--
--   base_model  the catalog base model id, from billing_event.base_model.
--   adapter     the fine-tune checkpoint artifact id, from billing_event.adapter;
--               NULL when the rollup has no adapter.
--   price_key   the price key the rollup was rated at, as rating resolved it:
--               '<mode prefix>ft:<base>' on the fine-tune (base x premium) path,
--               the mode-prefixed base ('shared:<base>' or '<base>') on the
--               plain-base path, NULL when the rollup's own model_id entry priced it.
--
-- ATTRIBUTE columns, not grain: the unique key and rated_usage.id are unchanged,
-- and none of them affects money. Nullable with no default, so the previous
-- release's rater keeps writing (expand step). Existing rows keep NULL until the
-- hour is re-rated.
ALTER TABLE rated_usage ADD COLUMN IF NOT EXISTS base_model TEXT;
ALTER TABLE rated_usage ADD COLUMN IF NOT EXISTS adapter TEXT;
ALTER TABLE rated_usage ADD COLUMN IF NOT EXISTS price_key TEXT;
