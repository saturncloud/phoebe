-- =====================================================================
--  PLACEHOLDER — NON-BINDING EXAMPLE PRICE BOOK. DO NOT SHIP TO PROD.
-- =====================================================================
--
-- These are ILLUSTRATIVE round numbers so the rating job has something to join
-- against in local dev. They are NOT real prices and carry no commercial
-- meaning. Hugo sets the real price book as DATA (separate, deliberate INSERTs),
-- never in a schema migration and never from this file in production.
--
-- UNIT: prices are micro-USD (1e-6 USD) PER TOKEN, stored as integers.
--   "$X per 1,000,000 tokens"  ==  X micro-USD per token.
--   (because $X / 1e6 tokens = X*1e6 micro-USD / 1e6 tokens = X micro-USD/token)
--
-- cached_price_micro is a DISTINCT, discounted rate: vLLM's cached_tokens are the
-- subset of prompt_tokens served from cache. Rating charges the NON-cached prompt
-- subset (prompt_tokens - cached_tokens) at prompt_price_micro and the cached
-- subset at cached_price_micro. See internal/rating/rate.go for the formula.
--
-- effective_from is set far in the past and effective_to is NULL (open) so these
-- placeholders apply to all dev traffic. id is an arbitrary 32-char hex.

INSERT INTO model_price
    (id, model, prompt_price_micro, cached_price_micro, completion_price_micro,
     effective_from, effective_to)
VALUES
    -- model                       prompt  cached  completion   ($/1M tokens equiv)
    -- "gpt-4o-ish":               $3.00   $0.30   $10.00
    ('00000000000000000000000000000001', 'gpt-4o',
        3, 0, 10, TIMESTAMPTZ '2000-01-01 00:00:00+00', NULL),
    -- "gpt-4o-mini-ish":          $0.15   $0.015  $0.60  (sub-$1/1M → fractional;
    --                             rounded to whole micro-USD/token here, which is
    --                             exactly why the integer base is 1e-6 not 1e-4)
    ('00000000000000000000000000000002', 'gpt-4o-mini',
        1, 0, 1, TIMESTAMPTZ '2000-01-01 00:00:00+00', NULL),
    -- "llama-70b-ish":            $0.90   $0.00   $0.90  (no cache discount modelled)
    ('00000000000000000000000000000003', 'meta-llama/Llama-3.1-70B-Instruct',
        1, 0, 1, TIMESTAMPTZ '2000-01-01 00:00:00+00', NULL),
    -- "claude-ish":               $3.00   $0.30   $15.00
    ('00000000000000000000000000000004', 'claude-3-5-sonnet',
        3, 0, 15, TIMESTAMPTZ '2000-01-01 00:00:00+00', NULL);
