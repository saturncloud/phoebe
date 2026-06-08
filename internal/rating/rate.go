// Package rating is phoebe's REVENUE path. It turns the raw token counts in
// billing_event into money: it reads a metering window, joins an effective-dated
// price book, computes per-event cost, and writes per-(auth_id, model, hour) cost
// rollups to rated_usage.
//
// MONEY CORRECTNESS is the entire product. The invariants enforced here, each of
// which is a way to silently get a customer's bill wrong:
//
//   - Money is INTEGER micro-USD (1e-6 USD). NEVER float — float rounding
//     corrupts money. (cost_micro_usd int64.) See microUSD docs below.
//   - Prices are per-token, in micro-USD, integers, from the price book.
//   - cached tokens are a DISTINCT rate and a SUBSET of prompt tokens — the
//     billable-prompt formula (Rate) must not double-count them. This is the
//     single highest-risk line in the codebase.
//   - Prices are effective-dated: an event is rated with the price in effect at
//     the event's time, never retroactively repriced.
//   - A model with no price for the event's time FAILS LOUD (ErrNoPrice) — it is
//     NEVER silently billed $0 (that is lost revenue). This is the fail-closed rule.
package rating

import (
	"errors"
	"fmt"
	"time"
)

// microUSD is the money base unit: 1 micro-USD = 1e-6 USD. All prices and costs
// in this package are integer counts of micro-USD.
//
// Why 1e-6 and not Atlas's hourly_usage_record base of 1e-4 USD: per-token prices
// are tiny (e.g. $0.15 / 1M tokens = 0.15 micro-USD/token). A 1e-4 base would
// force such prices to round to zero or to a coarse integer BEFORE the multiply,
// losing revenue on every token. 1e-6 keeps per-token prices as exact integers.
//
// Relationship to Atlas: 1 Atlas unit (1e-4 USD) == 100 micro-USD. Converting a
// cost_micro_usd to Atlas units is a divide-by-100, done downstream where the
// rounding policy is owned — deliberately NOT here.
//
// Overflow headroom: cost is int64 micro-USD. int64 max ≈ 9.2e18 micro-USD ≈
// $9.2e12. At, say, $50/1M tokens (50 micro-USD/token) that is ~1.8e17 tokens in a
// single rollup before overflow — far beyond any realistic hourly per-key volume.
// We still keep every multiply/sum in int64 and never widen to float.
const _ = "micro-USD = 1e-6 USD; 1 Atlas unit = 100 micro-USD" // doc anchor

// ErrNoPrice is the fail-closed sentinel: the price book has no entry for a
// model at the event's time. The caller MUST surface this (count it, log it
// loudly) and MUST NOT bill the event at $0. Returned by PriceBook.Price and
// propagated by Rate.
var ErrNoPrice = errors.New("rating: no price for model at event time")

// Price is one resolved per-token price, in micro-USD. It is the price book row
// already selected for a specific (model, time).
type Price struct {
	Model                string
	PromptPriceMicro     int64 // micro-USD per non-cached prompt token
	CachedPriceMicro     int64 // micro-USD per cached prompt token (distinct rate)
	CompletionPriceMicro int64 // micro-USD per completion token
	EffectiveFrom        time.Time
	EffectiveTo          time.Time // zero value == open-ended (current price)
}

// PriceResolver resolves the price for a model effective at a given instant, or
// ErrNoPrice. PriceBook implements it; tests use a fake. Kept as an interface so
// Rate depends only on the lookup contract, not on how prices are stored.
type PriceResolver interface {
	Price(model string, at time.Time) (Price, error)
}

// RatedEvent is the minimal slice of a billing_event that rating needs. It is
// decoupled from metering.Event so the pure money math has no dependency on the
// capture/emit side and can be tested in isolation.
type RatedEvent struct {
	AuthID           string
	Model            string
	PromptTokens     int64 // TOTAL prompt tokens (cached + non-cached), per vLLM
	CachedTokens     int64 // SUBSET of PromptTokens that was a cache hit
	CompletionTokens int64
	Aborted          bool
	// At is the instant used for price selection: event_ts, or created_at if
	// event_ts is null. Resolved by the caller (the Store read) before rating.
	At time.Time
}

// Rate computes the cost of a single event in micro-USD using the price in
// effect at the event's time.
//
// THE BILLABLE-PROMPT FORMULA (the highest-risk line in the codebase — read this
// before changing anything):
//
//	vLLM reports prompt_tokens as the TOTAL prompt tokens and cached_tokens as the
//	SUBSET of those that were served from cache. They OVERLAP: cached_tokens ⊆
//	prompt_tokens. So the non-cached prompt subset is (prompt_tokens -
//	cached_tokens), and the cost is:
//
//	    billable_prompt = prompt_tokens - cached_tokens
//	    cost = billable_prompt * prompt_price
//	         + cached_tokens   * cached_price
//	         + completion_tokens * completion_price
//
//	We charge each prompt token EXACTLY ONCE: non-cached at the prompt rate, cached
//	at the (usually discounted) cached rate. Charging cached_tokens at BOTH rates —
//	i.e. prompt_tokens*prompt_price + cached_tokens*cached_price — would OVERBILL
//	the customer for every cache hit. Do not do that.
//
// Defensive clamp: if cached_tokens > prompt_tokens (malformed engine usage
// block), billable_prompt would go negative and we'd UNDERBILL (credit) the
// non-existent negative tokens. We clamp billable_prompt at 0 and charge the
// reported cached_tokens at the cached rate — the conservative, never-credit
// reading of a malformed record.
//
// Aborted events are rated NORMALLY: an aborted stream still served real tokens
// (the engine's usage block reflects them), and those tokens cost real GPU time.
// Rating reflects tokens served, not whether the client hung up. The `aborted`
// flag is preserved on billing_event for audit/analytics, not for zero-rating.
//
// Zero tokens → zero cost (and zero is a legitimate rated value here — the model
// HAD a price; contrast ErrNoPrice, which means the model had NO price at all).
func Rate(e RatedEvent, prices PriceResolver) (costMicroUSD int64, err error) {
	p, err := prices.Price(e.Model, e.At)
	if err != nil {
		// Propagate ErrNoPrice (fail-closed) verbatim; wrap others with context.
		if errors.Is(err, ErrNoPrice) {
			return 0, fmt.Errorf("%w: model=%q at=%s", ErrNoPrice, e.Model, e.At.UTC().Format(time.RFC3339))
		}
		return 0, err
	}

	billablePrompt := e.PromptTokens - e.CachedTokens
	if billablePrompt < 0 {
		// Malformed usage (cached > prompt): never credit phantom tokens.
		billablePrompt = 0
	}

	// All-int64 multiply/add: no float anywhere on the money path.
	cost := billablePrompt*p.PromptPriceMicro +
		e.CachedTokens*p.CachedPriceMicro +
		e.CompletionTokens*p.CompletionPriceMicro

	return cost, nil
}

// BillablePromptTokens returns prompt - cached, clamped at 0. Exposed so the
// aggregator can store billable_prompt_tokens consistently with Rate's math (one
// definition of the subtraction, not two).
func BillablePromptTokens(promptTokens, cachedTokens int64) int64 {
	b := promptTokens - cachedTokens
	if b < 0 {
		return 0
	}
	return b
}
