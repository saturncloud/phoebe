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
// RatedEvent, the billable-prompt formula, the Rate cost function, and the
// derivation-chain sentinel.
package rating

import (
	"errors"
	"time"
)

// ErrDerivationChain is returned by the oracle when a fine-tune's derived_from
// points at a base that is itself a fine-tune (a chain longer than one hop). v1
// supports ONE hop only; a deeper chain is treated as an error (and counted as
// unpriced), never recursed. Distinct from ErrNoPrice so the anomaly can be
// reported specifically, but it drives the same fail-loud path.
var ErrDerivationChain = errors.New("rating: derived_from chain exceeds one hop (unsupported in v1)")

// RatedEvent is the minimal slice of a billing_event that rating needs. It is
// decoupled from metering.Event so the pure money math has no dependency on the
// capture/emit side and can be tested in isolation.
type RatedEvent struct {
	AuthID           string
	ModelID          string
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
// ROUNDING — match production exactly (sum-then-round, NOT round-then-sum):
// the SQL rater SUMs the exact per-event products across the whole rollup and
// rounds the SUM once when it lands in NUMERIC(20,9). So the oracle must NOT
// round per event and then sum. rateExact returns the UNROUNDED exact cost; the
// rollup sums those and rounds once. Rate() is the single-event convenience form
// (sum of one = round once), kept for per-event unit tests.
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
