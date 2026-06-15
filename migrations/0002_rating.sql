-- Rating (E1) schema: the REVENUE path. Turns raw metered token counts
-- (billing_event) into money.
--
-- PRICES ARE A YAML CONFIG FILE, NOT A DB TABLE (E1). There is no model_price and
-- no derivation_policy table, no GiST exclusion constraint, no effective-dating.
-- The operator authors a versioned price YAML (base per-token rates keyed on the HF
-- model id, the single global fine-tune premium policy, and per-GPU floor rates);
-- the file's history IS the price audit trail. The hourly rater loads the CURRENT
-- file, rates the last complete hour, and FREEZES the applied per-token rate onto
-- each rated_usage row. See config/prices.example.yaml and internal/rating.
--
-- ONE table here:
--   rated_usage — the rollup. Per-(auth_id, model_id, hour) cost, idempotently
--                 upserted, carrying the applied per-token rates so the row is
--                 self-auditing and immutable ("we never reprice served traffic").
--
-- MONEY UNIT (read before touching any number here):
--   All money is stored as NUMERIC(20,9) — exact base-10 decimal, 9 fractional
--   digits (nano-USD resolution), 11 integer digits. NEVER float (float rounding
--   silently corrupts money) and NEVER an integer micro/nano scalar. A per-token
--   price like $0.15 / 1,000,000 tokens = 0.000000150 USD/token is EXACT.
--
--   ALL MONEY MATH HAPPENS IN SQL, never in Go. The rater projects the YAML prices
--   into a transient TEMP table, then computes per-event cost AND sums it in a
--   single statement; Go only carries NUMERIC values to/from the DB as strings.
--   The fine-tune premium is applied in exact decimal when the prices are projected.
--   See internal/rating.
--
-- NOTE: this .sql is for reference and local dev only. In the shared Atlas Postgres
-- the table is created by the Alembic chain — see
-- migrations/atlas/c2f1a3b4d5e6_add_rating.py and migrations/README.md. Keep the
-- two in sync.

-- rated_usage: the per-(auth_id, model_id, hour) cost rollup.
--
-- Grain is HOURLY (window_start truncated to the hour), matching Atlas's
-- hourly_usage_record. The natural key (auth_id, model_id, window_start) is UNIQUE
-- so re-rating a window UPSERTS the same rows instead of duplicating them — re-runs
-- are idempotent and never double-count.
CREATE TABLE rated_usage (
    id                      VARCHAR(32) NOT NULL,   -- surrogate; natural key is the unique constraint below

    auth_id                 VARCHAR(64) NOT NULL,
    model_id                VARCHAR(255) NOT NULL,

    -- [window_start, window_end) is the hour this rollup covers.
    window_start            TIMESTAMPTZ NOT NULL,
    window_end              TIMESTAMPTZ NOT NULL,

    -- Raw token sums over the window (audit trail behind the cost).
    prompt_tokens           BIGINT NOT NULL,
    cached_tokens           BIGINT NOT NULL,
    completion_tokens       BIGINT NOT NULL,
    -- billable_prompt_tokens = SUM(prompt_tokens - cached_tokens) (clamped >= 0):
    -- the non-cached subset charged at the prompt rate. Stored so cost is
    -- reconstructable.
    billable_prompt_tokens  BIGINT NOT NULL,

    -- The money, as exact NUMERIC. Computed and summed in SQL (never in Go).
    cost                    NUMERIC(20, 9) NOT NULL,

    -- THE APPLIED PER-TOKEN RATES (E1 self-auditing rollup): the exact rates this
    -- rollup was billed at, frozen onto the row from the price file the run loaded.
    -- A rollup mixes only one model_id, so a single applied-rate triple is
    -- well-defined. With these on the row, cost is fully reconstructable and the row
    -- never needs to be repriced — "we never reprice traffic you've already served"
    -- holds by construction. Defaulted to 0 so an ALTER on an existing table is
    -- backfill-free; every rater-written row sets them explicitly.
    applied_prompt_rate     NUMERIC(20, 9) NOT NULL DEFAULT 0,
    applied_cached_rate     NUMERIC(20, 9) NOT NULL DEFAULT 0,
    applied_completion_rate NUMERIC(20, 9) NOT NULL DEFAULT 0,

    event_count             INTEGER NOT NULL,

    rated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_rated_usage PRIMARY KEY (id),
    -- Idempotency key: one rollup row per (auth_id, model_id, hour).
    CONSTRAINT rated_usage_auth_model_window_uq
        UNIQUE (auth_id, model_id, window_start)
);

-- Per-API-key billing queries scan by auth_id over a time window.
CREATE INDEX rated_usage_auth_id_window_start_ix
    ON rated_usage (auth_id, window_start);

-- The rater scans billing_event by its RATING INSTANT, COALESCE(event_ts,
-- created_at), over the rating window. The index must be on that EXACT
-- expression: Postgres matches index expressions structurally, so an index on
-- bare (event_ts) — partial or not — can never serve a COALESCE(event_ts,
-- created_at) predicate, and the rater would seq-scan billing_event (a table
-- that only grows) on every run.
CREATE INDEX IF NOT EXISTS billing_event_rating_instant_ix
    ON billing_event ((COALESCE(event_ts, created_at)));

-- base_model on billing_event (E3 fine-tune linkage): the HF base id a fine-tune
-- derives from, stamped by Atlas at deploy. The rater prices an ft:<checkpoint>
-- model at base x premium via this column; an ft: model with a NULL base_model
-- fails loud (never $0). Added idempotently: the billing_event create migration
-- (0001) now declares it directly, so on a fresh DB this is a no-op; a
-- billing_event created before the column existed still gets it here. Mirrors the
-- Alembic rating migration (migrations/atlas/c2f1a3b4d5e6_add_rating.py).
ALTER TABLE billing_event ADD COLUMN IF NOT EXISTS base_model VARCHAR(255);
