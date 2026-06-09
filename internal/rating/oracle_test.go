// THE Rate() ORACLE — test-support code (this is a _test.go file; it never ships).
//
// This file holds a PURE Go reference implementation of the cost formula (Rate)
// plus an exact decimal helper (Dec). The SQL rater in store.go is the PRODUCTION
// path; Rate() is the SPEC, and the conformance test asserts the SQL output
// matches Rate() row-for-row over a fixture. Rate() uses big.Rat — exact rational
// arithmetic — so it has NO float error and is the authority on what the SQL must
// compute. See doc.go for the production-vs-oracle split.
package rating

import (
	"errors"
	"fmt"
	"math/big"
	"time"
)

// moneyScale is the number of fractional decimal digits money is stored and
// compared at, matching the NUMERIC(20,9) columns in migrations/0002_rating.sql.
// 9 digits == nano-USD resolution. Rate() rounds its exact rational result to
// this scale (half-up) so the oracle and the DB's NUMERIC arithmetic agree.
const moneyScale = 9

// ErrNoPrice is the fail-closed sentinel: the price book has no resolvable entry
// for a model at the event's time (no own rate, and no one-hop derived base rate).
// The caller MUST surface this (count it, log it loudly) and MUST NOT bill the
// event at $0. Returned by ResolvePrice and propagated by Rate.
var ErrNoPrice = errors.New("rating: no price for model at event time")

// ErrDerivationChain is returned when a model's derived_from points at another
// DERIVED model (a chain longer than one hop). v1 supports ONE hop only; a deeper
// chain is treated as an error (and counted as unpriced), never recursed — see the
// one-hop-only rule. Distinct from ErrNoPrice so the anomaly can be reported
// specifically, but it drives the same fail-loud, exit-nonzero path.
var ErrDerivationChain = errors.New("rating: derived_from chain exceeds one hop (unsupported in v1)")

// PolicyFunc is the derivation-policy function type. It mirrors the
// derivation_policy.function CHECK constraint in the schema.
type PolicyFunc string

const (
	PolicyIdentity   PolicyFunc = "identity"   // derived = base
	PolicyMultiplier PolicyFunc = "multiplier" // derived = base * factor
	PolicyMarkup     PolicyFunc = "markup"     // derived = base + markup (per-token)
)

// Rate is the per-token rate triple a model resolves to at a given instant, AFTER
// any derivation policy has been applied. These are the prices Rate() multiplies
// the token counts by. Carried as exact Dec (never float).
type Rate3 struct {
	Prompt     Dec
	Cached     Dec
	Completion Dec
}

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
// rate (own rate, or base-rate-through-policy). Resolution — including
// effective-dating and the derivation policy — happens BEFORE Rate, in ResolvePrice
// (oracle) / the SQL join (production); Rate is purely the multiply-and-sum.
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
//	at the (usually discounted) cached rate. Charging cached_tokens at BOTH rates —
//	prompt_tokens*prompt_price + cached_tokens*cached_price — would OVERBILL for
//	every cache hit. Do not do that. The same formula is implemented in SQL in
//	store.go; the conformance test pins them together.
//
// Defensive clamp: if cached_tokens > prompt_tokens (malformed engine usage),
// billable_prompt would go negative and we'd UNDERBILL (credit) phantom tokens. We
// clamp billable_prompt at 0 and charge the reported cached_tokens at the cached
// rate — the conservative, never-credit reading of a malformed record.
//
// Aborted events are rated NORMALLY: an aborted stream still served real tokens.
// The result is rounded to moneyScale (9dp) half-up so it equals the DB NUMERIC.
func Rate(e RatedEvent, r Rate3) Dec {
	billablePrompt := BillablePromptTokens(e.PromptTokens, e.CachedTokens)

	// Exact rational accumulation: tokens (integers) times per-token NUMERIC rates,
	// summed with no rounding until the final quantize. No float anywhere.
	cost := r.Prompt.MulInt(billablePrompt).
		Add(r.Cached.MulInt(e.CachedTokens)).
		Add(r.Completion.MulInt(e.CompletionTokens))

	return cost.Round(moneyScale)
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

// ApplyPolicy transforms a base per-token rate triple by the derivation policy,
// producing the derived (fine-tune) rate. This is the Go oracle for the policy
// math that the SQL applies in-join (a CASE on function). identity is the default;
// multiplier scales every component by factor; markup adds a per-token amount to
// every component. Exact decimal throughout.
func ApplyPolicy(base Rate3, fn PolicyFunc, factor, markup Dec) (Rate3, error) {
	switch fn {
	case PolicyIdentity:
		return base, nil
	case PolicyMultiplier:
		return Rate3{
			Prompt:     base.Prompt.Mul(factor),
			Cached:     base.Cached.Mul(factor),
			Completion: base.Completion.Mul(factor),
		}, nil
	case PolicyMarkup:
		return Rate3{
			Prompt:     base.Prompt.Add(markup),
			Cached:     base.Cached.Add(markup),
			Completion: base.Completion.Add(markup),
		}, nil
	default:
		return Rate3{}, fmt.Errorf("rating: unknown derivation policy function %q", fn)
	}
}

// Dec is an EXACT decimal money value backed by a big.Rat. It powers the Rate()
// oracle's exact arithmetic — phoebe does NOT do money math in Go on the
// production path (the SQL rater does), and Dec is test-only by virtue of living
// in a _test.go file. Dec uses big.Rat (exact rationals), so multiplies and adds
// have zero rounding error; rounding happens once, explicitly, at Round(moneyScale).
//
// The zero Dec is exact 0.
type Dec struct {
	// r is nil for the zero value (treated as 0). Always accessed via rat().
	r *big.Rat
}

// rat returns the underlying rational, treating the nil zero-value as 0.
func (d Dec) rat() *big.Rat {
	if d.r == nil {
		return new(big.Rat) // 0/1
	}
	return d.r
}

// MustDec parses a decimal string (e.g. "0.000000150") into an exact Dec, panicking
// on a malformed input. For test fixtures and constants where the literal is known
// good.
func MustDec(s string) Dec {
	d, err := ParseDec(s)
	if err != nil {
		panic(err)
	}
	return d
}

// ParseDec parses a decimal string into an exact Dec. It accepts the forms Postgres
// emits for a NUMERIC ("3", "0.300000000", "-0.5"); it rejects float-style
// exponents to keep the money path free of any float round-trip.
func ParseDec(s string) (Dec, error) {
	if s == "" {
		return Dec{}, fmt.Errorf("rating: empty decimal string")
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return Dec{}, fmt.Errorf("rating: invalid decimal %q", s)
	}
	return Dec{r: r}, nil
}

// Add returns d + o (exact).
func (d Dec) Add(o Dec) Dec {
	return Dec{r: new(big.Rat).Add(d.rat(), o.rat())}
}

// Mul returns d * o (exact).
func (d Dec) Mul(o Dec) Dec {
	return Dec{r: new(big.Rat).Mul(d.rat(), o.rat())}
}

// MulInt returns d * n (exact), the per-token-price × token-count operation.
func (d Dec) MulInt(n int64) Dec {
	return Dec{r: new(big.Rat).Mul(d.rat(), new(big.Rat).SetInt64(n))}
}

// Round returns d rounded to `scale` fractional decimal digits, half-up (round
// half away from zero), matching the rounding the DB applies when storing into a
// NUMERIC(_, scale). This is the ONLY place the oracle rounds.
func (d Dec) Round(scale int) Dec {
	// scaled = d * 10^scale, then round to nearest integer half-up, then / 10^scale.
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	powRat := new(big.Rat).SetInt(pow)
	scaled := new(big.Rat).Mul(d.rat(), powRat)

	num := scaled.Num()
	den := scaled.Denom()
	// Integer division with half-up rounding on |num/den|.
	q := new(big.Int)
	rem := new(big.Int)
	q.QuoRem(num, den, rem)
	// 2*|rem| >= |den| → round away from zero.
	twiceRem := new(big.Int).Abs(rem)
	twiceRem.Lsh(twiceRem, 1)
	if twiceRem.Cmp(new(big.Int).Abs(den)) >= 0 {
		if num.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	rounded := new(big.Rat).SetFrac(q, pow)
	return Dec{r: rounded}
}

// String renders the Dec at moneyScale fixed decimal places — the canonical form
// used to bind a money value into a parameterised SQL statement and to compare
// against a value Postgres returned. Fixed-scale so "3" and "3.000000000" compare
// equal as strings after both pass through here.
func (d Dec) String() string {
	return d.StringScale(moneyScale)
}

// StringScale renders the Dec at exactly `scale` fixed decimal places (half-up).
func (d Dec) StringScale(scale int) string {
	return d.Round(scale).rat().FloatString(scale)
}

// Equal reports exact equality of the rational values (scale-independent: 3 == 3.0).
func (d Dec) Equal(o Dec) bool {
	return d.rat().Cmp(o.rat()) == 0
}

// IsZero reports whether d is exactly zero.
func (d Dec) IsZero() bool {
	return d.rat().Sign() == 0
}
