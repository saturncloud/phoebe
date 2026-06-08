-- Rating v1 schema: the REVENUE path. Turns raw metered token counts
-- (billing_event) into money.
--
-- Two tables:
--   model_price  — the price book. Per-token prices in micro-USD, effective-dated.
--   rated_usage  — the rollup. Per-(auth_id, model, hour) cost, idempotently upserted.
--
-- MONEY UNIT (read before touching any number here):
--   All money is stored as INTEGER micro-USD (1e-6 USD). NEVER float — float
--   rounding silently corrupts money. Atlas's hourly_usage_record uses units of
--   1e-4 USD; we use a finer 1e-6 base so per-token prices like "$0.0000005/token"
--   are exact integers rather than rounded. Relationship: 1 Atlas unit (1e-4 USD)
--   = 100 micro-USD. Converting our cost_micro_usd -> Atlas units is a divide by
--   100 (done downstream, deliberately, where the rounding policy is decided).
--
-- NOTE: this .sql is for reference and local dev only. In the shared Atlas
-- Postgres the tables are created by the Alembic chain — see
-- migrations/atlas/c2f1a3b4d5e6_add_rating.py and migrations/README.md. Keep the
-- two in sync.

-- model_price: the price book. Per-token prices, effective-dated so a price change
-- never retroactively reprices already-served traffic.
--
-- *** PLACEHOLDER / NON-BINDING ***: this migration creates the TABLE only. No
-- prices ship in the schema. Real prices are DATA, set by Hugo (see
-- migrations/seed_example_prices.sql for an illustrative, non-binding seed).
CREATE TABLE model_price (
    id                      VARCHAR(32) NOT NULL,   -- Atlas-style 32-char hex id
    model                   VARCHAR(255) NOT NULL,

    -- Per-token prices in micro-USD (1e-6 USD), stored as integers.
    -- e.g. "$3.00 per 1M prompt tokens" = 3000000 micro-USD / 1e6 tokens
    --      = 3 micro-USD per token.
    -- cached tokens are a DISTINCT (usually discounted) rate from prompt tokens;
    -- see the billable-prompt formula in internal/rating/rate.go.
    prompt_price_micro      BIGINT NOT NULL,
    cached_price_micro      BIGINT NOT NULL,
    completion_price_micro  BIGINT NOT NULL,

    -- Effective window. effective_to NULL == open-ended (current price).
    -- An event is rated with the price whose [effective_from, effective_to)
    -- window contains the event's event_ts (fall back to created_at).
    effective_from          TIMESTAMPTZ NOT NULL,
    effective_to            TIMESTAMPTZ,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_model_price PRIMARY KEY (id)
);

-- Price lookups resolve (model, at-time) → newest price with effective_from <= at.
CREATE INDEX model_price_model_effective_from_ix
    ON model_price (model, effective_from);

-- rated_usage: the per-(auth_id, model, hour) cost rollup.
--
-- Grain is HOURLY (window_start truncated to the hour), matching Atlas's
-- hourly_usage_record. The natural key (auth_id, model, window_start) is UNIQUE so
-- re-rating a window UPSERTS the same rows instead of duplicating them — re-runs
-- are idempotent and never double-count.
CREATE TABLE rated_usage (
    id                      VARCHAR(32) NOT NULL,   -- surrogate; natural key is the unique constraint below

    auth_id                 VARCHAR(64) NOT NULL,
    model                   VARCHAR(255) NOT NULL,

    -- [window_start, window_end) is the hour this rollup covers.
    window_start            TIMESTAMPTZ NOT NULL,
    window_end              TIMESTAMPTZ NOT NULL,

    -- Raw token sums over the window (audit trail behind the cost).
    prompt_tokens           BIGINT NOT NULL,
    cached_tokens           BIGINT NOT NULL,
    completion_tokens       BIGINT NOT NULL,
    -- billable_prompt_tokens = prompt_tokens - cached_tokens (the non-cached
    -- subset charged at the prompt rate). Stored so the cost is reconstructable.
    billable_prompt_tokens  BIGINT NOT NULL,

    cost_micro_usd          BIGINT NOT NULL,        -- the money, in micro-USD
    event_count             INTEGER NOT NULL,

    rated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_rated_usage PRIMARY KEY (id),
    -- Idempotency key: one rollup row per (auth_id, model, hour).
    CONSTRAINT rated_usage_auth_model_window_uq
        UNIQUE (auth_id, model, window_start)
);

-- Per-API-key billing queries scan by auth_id over a time window.
CREATE INDEX rated_usage_auth_id_window_start_ix
    ON rated_usage (auth_id, window_start);
