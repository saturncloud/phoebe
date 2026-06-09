-- Rating v2 schema: the REVENUE path. Turns raw metered token counts
-- (billing_event) into money.
--
-- Three tables:
--   model_price       — the price book. Per-token prices, effective-dated, keyed
--                       on a STABLE model_id (not a deployment id, not a name).
--   derivation_policy — the single GLOBAL rule that derives a fine-tune's price
--                       from its base model's price. Effective-dated.
--   rated_usage       — the rollup. Per-(auth_id, model_id, hour) cost, idempotently
--                       upserted.
--
-- MONEY UNIT (read before touching any number here):
--   All money is stored as NUMERIC(20,9) — exact base-10 decimal, 9 fractional
--   digits (nano-USD resolution), 11 integer digits. NEVER float (float rounding
--   silently corrupts money) and NEVER an integer micro/nano scalar (that forces
--   a rounding decision at write time and leaks sub-unit revenue). NUMERIC keeps a
--   per-token price like $0.15 / 1,000,000 tokens = 0.000000150 USD/token EXACT.
--
--   ALL MONEY MATH HAPPENS IN SQL, never in Go. The rater computes per-event cost
--   AND sums it in a single statement; Go never holds a running money total. Go
--   only carries NUMERIC values to/from the DB as strings. See internal/rating.
--
-- PRICE KEY (model_id, NOT model name, NOT deployment id):
--   model_id is a stable model identity. A fine-tune that has no own price points
--   at its base via derived_from (a self-FK) and inherits the base's effective
--   price transformed by the global derivation_policy. This is a POINTER, not a
--   copy: changing the base's price auto-propagates to every derived model.
--
-- NOTE: this .sql is for reference and local dev only. In the shared Atlas
-- Postgres the tables are created by the Alembic chain — see
-- migrations/atlas/c2f1a3b4d5e6_add_rating.py and migrations/README.md. Keep the
-- two in sync.

-- btree_gist lets a GiST exclusion constraint mix an equality column (model_id)
-- with a range overlap (&&) — the mechanism that makes overlapping effective
-- windows IMPOSSIBLE (see the EXCLUDE constraints below). Without it the &&
-- operator class for the scalar key is unavailable.
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- model_price: the price book. Per-token prices, effective-dated so a price change
-- never retroactively reprices already-served traffic.
--
-- RESOLUTION at rating time, AS-OF the event's event_ts:
--   1. own rate present (prompt_price NOT NULL) → use it (the escape hatch; a model
--      with an explicit rate BYPASSES the derivation policy).
--   2. else derived_from set → resolve the BASE model_id's effective rate (ONE hop
--      only) and transform it by the effective derivation_policy.
--   3. else → NO PRICE → fail loud (counted, logged ERROR, never silently $0).
--
-- A model whose derived_from points at another DERIVED model (a chain > 1 hop) is
-- out of scope for v1: the rater treats it as unpriced (an error), never recursing.
--
-- *** PLACEHOLDER / NON-BINDING ***: this migration creates the TABLE only. No
-- prices ship in the schema. Real prices are DATA, set by an operator (see
-- migrations/seed_example_prices.sql for an illustrative, non-binding seed).
CREATE TABLE model_price (
    id                      VARCHAR(32) NOT NULL,   -- Atlas-style 32-char hex surrogate id

    -- model_id is the STABLE price key (a model identity, not a deployment id).
    model_id                VARCHAR(255) NOT NULL,

    -- derived_from: NULL for a base model; else the model_id of the BASE this model
    -- inherits its price from (one hop). A self-reference in the price-key space —
    -- NOT a row FK, because the base may have many effective rows over time and the
    -- inheritance is resolved AS-OF the event, not pinned to one row.
    derived_from            VARCHAR(255),

    -- Per-token prices as NUMERIC (exact decimal), NULLABLE: a derived model with a
    -- null rate inherits from its base via derived_from + derivation_policy. A base
    -- model (or a model with the escape-hatch own rate) has these set.
    -- cached_price is a DISTINCT (usually discounted) rate; cached_tokens are the
    -- SUBSET of prompt_tokens served from cache. See the billable-prompt formula in
    -- the rater SQL (internal/rating/store.go) and the Rate() oracle.
    prompt_price            NUMERIC(20, 9),
    cached_price            NUMERIC(20, 9),
    completion_price        NUMERIC(20, 9),

    -- Effective window. effective_to NULL == open-ended (current price).
    -- An event is rated with the row whose [effective_from, effective_to) window
    -- contains the event's event_ts (fall back to created_at).
    effective_from          TIMESTAMPTZ NOT NULL,
    effective_to            TIMESTAMPTZ,

    -- AUDIT: model_price is append-only-effective-dated — you never UPDATE a price,
    -- you INSERT a new effective row and close the old one's effective_to. That
    -- history IS the audit trail. created_by records WHO set the price (the
    -- operator identity; write-path authz is an Atlas/control-plane concern, out of
    -- scope for phoebe — phoebe's DB merely records it). created_at records when.
    created_by              VARCHAR(255),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_model_price PRIMARY KEY (id),

    -- A model either has a derived_from (inherits) or its own rate (or both, where
    -- the own rate wins). A row with NEITHER a rate NOR a derived_from is a useless
    -- "no price" row that would silently make the model unpriced; reject it at write
    -- time so the price book can never contain a dead entry.
    --
    -- An own rate is ALL-OR-NOTHING across the three components: either all three of
    -- prompt/cached/completion are set, or none are (a pure derived row). This is a
    -- money-correctness guard: a partially-NULL rate would make one cost term NULL
    -- in the SQL sum and silently UNDER-bill that component. (A model that wants to
    -- charge $0 for, say, cached tokens sets cached_price = 0, not NULL.)
    CONSTRAINT model_price_rate_or_derived_ck
        CHECK (derived_from IS NOT NULL OR prompt_price IS NOT NULL),
    CONSTRAINT model_price_rate_all_or_none_ck CHECK (
        (prompt_price IS NULL     AND cached_price IS NULL     AND completion_price IS NULL) OR
        (prompt_price IS NOT NULL AND cached_price IS NOT NULL AND completion_price IS NOT NULL)
    ),

    -- effective_from must strictly precede effective_to. An equal-bound row is an
    -- EMPTY tstzrange, which overlaps nothing — so the no-overlap exclusion below
    -- would NOT reject it even sitting inside another row's window, yet the rater's
    -- [from, to) predicate never matches it either: a silently inert "dead" price
    -- row that can mask an operator's intended price. Reject it at write time.
    CONSTRAINT model_price_effective_order_ck
        CHECK (effective_to IS NULL OR effective_from < effective_to),

    -- FORWARD-ONLY, NON-OVERLAPPING effective-dating, enforced in the DB: for any
    -- (model_id, instant) AT MOST ONE row matches. Two overlapping price rows for a
    -- model would let the rating join FAN OUT and silently OVER-bill (double-count
    -- an event); this GiST exclusion makes that data state IMPOSSIBLE to insert.
    -- tstzrange is half-open [from, to); a NULL effective_to is the unbounded upper.
    -- MUST be tstzrange, not tsrange: the columns are TIMESTAMPTZ, and tsrange would
    -- coerce them to local timestamp using the session TimeZone, making the overlap
    -- check (and thus this whole no-overlap guarantee) session-TZ-dependent. The
    -- rater's price lookups use LIMIT 1 with no ORDER BY, trusting this constraint
    -- for at-most-one-match — that bedrock cannot be TZ-sensitive. tstzrange compares
    -- in absolute time.
    CONSTRAINT model_price_no_overlap
        EXCLUDE USING gist (
            model_id WITH =,
            tstzrange(effective_from, effective_to) WITH &&
        )
);

-- Price lookups resolve (model_id, at-time) → the row whose window contains `at`.
CREATE INDEX model_price_model_id_effective_from_ix
    ON model_price (model_id, effective_from);

-- derivation_policy: the SINGLE GLOBAL rule that turns a base model's per-token
-- price into a derived (fine-tune) model's per-token price. Operators NEVER set
-- per-fine-tune prices (there may be thousands); they set ONE policy here.
--
-- GLOBAL SCOPE ONLY for v1: one policy applies to ALL fine-tunes. A per-base
-- override is a deliberate v1 NON-GOAL (documented). The policy is effective-dated
-- with the SAME forward-only, non-overlapping machinery as model_price, so the
-- policy in effect at the event's event_ts is the one applied.
--
-- function:
--   'identity'   → derived price = base price                      (default)
--   'multiplier' → derived price = base price * factor             (factor in `factor`)
--   'markup'     → derived price = base price + markup (per-token)  (amount in `markup`)
-- Exactly the parameter for the chosen function is set; the others are NULL.
CREATE TABLE derivation_policy (
    id              VARCHAR(32) NOT NULL,
    function        VARCHAR(32) NOT NULL,

    -- 'multiplier' parameter: dimensionless factor (e.g. 1.500000000 = +50%).
    factor          NUMERIC(20, 9),
    -- 'markup' parameter: additive per-token amount, same money unit as prices.
    markup          NUMERIC(20, 9),

    effective_from  TIMESTAMPTZ NOT NULL,
    effective_to    TIMESTAMPTZ,

    created_by      VARCHAR(255),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT pk_derivation_policy PRIMARY KEY (id),

    CONSTRAINT derivation_policy_function_ck
        CHECK (function IN ('identity', 'multiplier', 'markup')),
    -- Each function carries exactly its own parameter (and only it). This keeps the
    -- SQL CASE that applies the policy unambiguous and rejects malformed rows.
    CONSTRAINT derivation_policy_params_ck CHECK (
        (function = 'identity'   AND factor IS NULL     AND markup IS NULL) OR
        (function = 'multiplier' AND factor IS NOT NULL AND markup IS NULL) OR
        (function = 'markup'     AND markup IS NOT NULL AND factor IS NULL)
    ),

    -- Single global policy per instant: forward-only, non-overlapping, like
    -- model_price. The constant 0 is the "all rows are the same group" equality key
    -- (there is no per-base scope in v1), so any two overlapping policy windows are
    -- rejected — exactly one policy is in effect at any instant.
    CONSTRAINT derivation_policy_no_overlap
        EXCLUDE USING gist (
            (0) WITH =,
            tstzrange(effective_from, effective_to) WITH &&
        )
);

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

-- The rater scans billing_event by its rating instant COALESCE(event_ts,
-- created_at) over a one-hour window. billing_event already indexes created_at;
-- the rater filters on event_ts too, so index it (partial: event_ts is nullable
-- and the COALESCE falls back to created_at when null).
CREATE INDEX IF NOT EXISTS billing_event_event_ts_ix
    ON billing_event (event_ts)
    WHERE event_ts IS NOT NULL;
