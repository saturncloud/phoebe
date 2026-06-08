-- =====================================================================
--  PLACEHOLDER — NON-BINDING EXAMPLE PRICE BOOK. DO NOT SHIP TO PROD.
-- =====================================================================
--
-- These are ILLUSTRATIVE round numbers so the v2 rating job has something to join
-- against in local dev. They are NOT real prices and carry no commercial meaning.
-- An operator sets the real price book as DATA (separate, deliberate INSERTs),
-- never in a schema migration and never from this file in production.
--
-- MONEY UNIT: prices are NUMERIC(20,9) PER-TOKEN USD — EXACT decimal, not float,
-- not an integer micro/nano scalar.
--   "$X per 1,000,000 tokens"  ==  X / 1e6  USD per token.
--   e.g. $3.00 / 1M = 0.000003000 ; $0.15 / 1M = 0.000000150 (exact at 9dp).
--
-- KEY: prices are keyed on a STABLE model_id (not a deployment id, not a display
-- name). cached_price is a DISTINCT, discounted rate: vLLM's cached_tokens are the
-- SUBSET of prompt_tokens served from cache; rating charges the NON-cached prompt
-- subset (prompt_tokens - cached_tokens) at prompt_price and the cached subset at
-- cached_price. See internal/rating/rate.go (Rate oracle) and the rater SQL.
--
-- INHERITANCE: a fine-tune has NO own rate; it sets derived_from to its base's
-- model_id and inherits the base's effective rate transformed by the global
-- derivation_policy below (a POINTER, not a copy — change the base price and every
-- derived model re-prices automatically). A model with its OWN rate bypasses the
-- policy (the escape hatch).
--
-- effective_from is far in the past and effective_to is NULL (open) so these
-- placeholders apply to all dev traffic. id is an arbitrary 32-char hex.

-- --- BASE models (own rate) -------------------------------------------------
INSERT INTO model_price
    (id, model_id, prompt_price, cached_price, completion_price,
     effective_from, effective_to, created_by)
VALUES
    -- "gpt-4o-ish":               $2.50/1M prompt, $0.25/1M cached, $10/1M completion
    ('00000000000000000000000000000001', 'gpt-4o',
        0.000002500, 0.000000250, 0.000010000,
        TIMESTAMPTZ '2000-01-01 00:00:00+00', NULL, 'seed:example'),
    -- "gpt-4o-mini-ish":          $0.15/1M prompt, $0.075/1M cached, $0.60/1M completion
    --   (sub-$1/1M → fractional per-token; EXACT in NUMERIC — the reason v2 is not
    --    an integer micro/nano unit, which would round these to zero or coarsen them)
    ('00000000000000000000000000000002', 'gpt-4o-mini',
        0.000000150, 0.000000075, 0.000000600,
        TIMESTAMPTZ '2000-01-01 00:00:00+00', NULL, 'seed:example'),
    -- "llama-70b-ish" (a BASE that fine-tunes derive from): $0.90/1M prompt,
    --   no cache discount modelled, $0.90/1M completion
    ('00000000000000000000000000000003', 'meta-llama/Llama-3.1-70B-Instruct',
        0.000000900, 0.000000900, 0.000000900,
        TIMESTAMPTZ '2000-01-01 00:00:00+00', NULL, 'seed:example');

-- --- DERIVED fine-tune (NO own rate; inherits base via derived_from) ---------
-- This row has NULL rate columns and derived_from = the llama base's model_id.
-- At rating time it resolves to the base's effective rate, transformed by the
-- global derivation_policy below. Set thousands of these and NONE need a price.
INSERT INTO model_price
    (id, model_id, derived_from, effective_from, effective_to, created_by)
VALUES
    ('00000000000000000000000000000010',
        'org-acme/llama-3.1-70b-customer-support-ft',
        'meta-llama/Llama-3.1-70B-Instruct',
        TIMESTAMPTZ '2000-01-01 00:00:00+00', NULL, 'seed:example');

-- --- GLOBAL derivation policy ------------------------------------------------
-- ONE policy for ALL fine-tunes (global scope; per-base override is a v1 non-goal).
-- Here: a 1.5x multiplier — a fine-tune costs 1.5x its base per token. Switch the
-- function to 'identity' (factor/markup NULL) for "fine-tune == base", or 'markup'
-- (markup set, factor NULL) for "base + fixed per-token amount".
INSERT INTO derivation_policy
    (id, function, factor, markup, effective_from, effective_to, created_by)
VALUES
    ('00000000000000000000000000000100', 'multiplier', 1.500000000, NULL,
        TIMESTAMPTZ '2000-01-01 00:00:00+00', NULL, 'seed:example');
