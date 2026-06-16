// THE Rate() ORACLE — test-support code (this is a _test.go file; it never ships).
//
// This file holds a PURE Go reference implementation of the cost formula (Rate).
// The SQL rater in store.go is the PRODUCTION path; Rate() is the SPEC, and the
// conformance test asserts the SQL output matches Rate() row-for-row over a
// fixture. Rate() builds on the production exact-decimal type Dec (decimal.go) —
// big.Rat, no float error — so it is the authority on what the SQL must compute.
// See doc.go for the production-vs-oracle split.
//
// The exact-decimal type (Dec), the per-token rate triple (Rate3), the fine-tune
// premium policy (PolicyFunc / ApplyPolicy), and the ErrNoPrice sentinel are
// PRODUCTION types now (the YAML price loader parses into them) and live in
// decimal.go / policy.go. This file holds only the oracle-specific surface:
// RatedEvent, the billable-prompt formula, and the Rate cost function.
//
// ONE-HOP DERIVATION is enforced at LOAD, not at rate time: buildPriceBook
// (pricebook.go) rejects a derived_from that points at a base which is itself derived
// (a multi-hop chain) and a dangling derived_from, and ResolveEvent never derives from
// a base that is itself a derived fine-tune. So a chain longer than one hop can never
// reach the oracle — there is no runtime "derivation chain" sentinel to return (an
// earlier ErrDerivationChain was documented as returned but never was; it was dead
// surface and is removed).
package rating

import (
	"time"
)

// RatedEvent is the minimal slice of a billing_event that rating needs. It is
// decoupled from metering.Event so the pure money math has no dependency on the
// capture/emit side and can be tested in isolation.
type RatedEvent struct {
	AuthID string
	// ResourceID is the deployment id (E2 customer attribution — billing resolves the
	// org via resource_id→org_id). Part of the rollup grain. Empty → unattributable
	// (the row can't name its deployment/org), mirroring the SQL's resource_id-IS-NULL
	// handling: counted, never billed.
	ResourceID string
	ModelID    string
	// BaseModel is the HF base id a fine-tune derives from (E3), carried on the event
	// from billing_event.base_model. Empty for a base model. The oracle prices an ft:
	// ModelID via base x premium keyed on BaseModel — mirroring the SQL.
	BaseModel        string
	PromptTokens     int64 // TOTAL prompt tokens (cached + non-cached), per vLLM
	CachedTokens     int64 // SUBSET of PromptTokens that was a cache hit
	CompletionTokens int64
	Aborted          bool
	// At is the instant used for price selection: event_ts, or created_at if
	// event_ts is null. Resolved by the caller (the Store read) before rating.
	At time.Time
}

// Rate computes the cost of a single event given the already-resolved per-token
// rate (base rate, or base-rate-through-premium). Resolution — including the
// fine-tune premium — happens BEFORE Rate, in the price loader / the SQL join; Rate
// is purely the multiply-and-sum.
//
// THE BILLABLE-PROMPT FORMULA (the highest-risk line in the codebase — read this
// before changing anything):
//
//	vLLM reports prompt_tokens as the TOTAL prompt tokens and cached_tokens as the
//	SUBSET of those served from cache. They OVERLAP: cached_tokens ⊆ prompt_tokens.
//	So the non-cached prompt subset is (prompt_tokens - cached_tokens), and:
//
//	    billable_prompt = max(prompt_tokens - cached_tokens, 0)
//	    cost = billable_prompt    * prompt_price
//	         + cached_tokens      * cached_price
//	         + completion_tokens  * completion_price
//
//	We charge each prompt token EXACTLY ONCE: non-cached at the prompt rate, cached
//	at the (usually discounted) cached rate. Charging cached_tokens at BOTH rates
//	would OVERBILL every cache hit. The same formula is implemented in SQL in
//	store.go; the conformance test pins them together.
//
// Defensive clamp: if cached_tokens > prompt_tokens (malformed engine usage),
// billable_prompt would go negative and we'd UNDERBILL (credit) phantom tokens. We
// clamp billable_prompt at 0 and charge the reported cached_tokens at the cached
// rate — the conservative, never-credit reading of a malformed record.
//
// Aborted events are rated NORMALLY: an aborted stream still served real tokens.
//
// ROUNDING — quantize-then-multiply (the ratified spec; see doc.go's ROUNDING
// section for why). Production stores the applied per-token rate on the
// rated_usage row (E1), so the rate must be the 9dp NUMERIC(20,9) value the row
// can hold — the premium is applied to the EXACT base rate, then the FINAL
// per-token rate is QUANTIZED to 9dp, and cost = quantized-rate × tokens. So the
// oracle's caller passes an ALREADY-QUANTIZED rate (r.Quantized()): Rate then
// multiplies that 9dp rate by integer token counts (an exact product at 9dp) and
// sums. There is no sub-nano residue to round away — the trailing Round here is a
// no-op on a quantized rate, kept only so a caller that (incorrectly) passes an
// un-quantized rate still lands on a representable money value rather than a
// big.Rat with an enormous denominator.
//
// IMPORTANT: Rate is faithful to production ONLY when fed a quantized rate. The
// conformance tests pass r.Quantized(); see TestConformance_PremiumQuantizedBeforeBilling
// for the residue case (1-nano base × 1.5 → 0.000000002 bills) that distinguishes
// quantize-then-multiply from the old sum-then-round.
func Rate(e RatedEvent, r Rate3) Dec {
	return rateExact(e, r).Round(moneyScale)
}

// rateExact is the exact (unrounded) per-event cost — the addend the rollup sums
// before its single final rounding. No float anywhere.
func rateExact(e RatedEvent, r Rate3) Dec {
	billablePrompt := BillablePromptTokens(e.PromptTokens, e.CachedTokens)
	return r.Prompt.MulInt(billablePrompt).
		Add(r.Cached.MulInt(e.CachedTokens)).
		Add(r.Completion.MulInt(e.CompletionTokens))
}

// BillablePromptTokens returns prompt - cached, clamped at 0. Exposed so the same
// subtraction is used everywhere (one definition, not two).
func BillablePromptTokens(promptTokens, cachedTokens int64) int64 {
	b := promptTokens - cachedTokens
	if b < 0 {
		return 0
	}
	return b
}
