package rating

import (
	"errors"
	"testing"
	"time"
)

// fixedPrices is a PriceResolver returning one price for "m", ErrNoPrice for any
// other model — the minimal resolver for unit-testing Rate in isolation.
type fixedPrices struct {
	p Price
}

func (f fixedPrices) Price(model string, _ time.Time) (Price, error) {
	if model == f.p.Model {
		return f.p, nil
	}
	return Price{}, ErrNoPrice
}

// TestRate is the money core. Each case names the billing rule it defends.
func TestRate(t *testing.T) {
	// Prompt $3/1M = 3 micro/token; cached $0.30/1M ≈ but here use 1 micro/token
	// to keep the discount distinct and the arithmetic checkable; completion 10.
	price := Price{Model: "m", PromptPriceMicro: 3, CachedPriceMicro: 1, CompletionPriceMicro: 10}
	prices := fixedPrices{p: price}

	cases := []struct {
		name     string
		rule     string
		event    RatedEvent
		wantCost int64
		wantErr  error
	}{
		{
			// THE headline case from the spec: prompt=100, cached=30 →
			// 70*prompt + 30*cached, NOT 100*prompt + 30*cached.
			name:  "cached-subset-no-double-count",
			rule:  "billable_prompt = (prompt-cached) at prompt rate + cached at cached rate; cached charged ONCE",
			event: RatedEvent{Model: "m", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 0},
			// (100-30)*3 + 30*1 + 0*10 = 210 + 30 = 240
			wantCost: 240,
		},
		{
			name:  "cached-subset-with-completion",
			rule:  "all three token classes summed with their own rates",
			event: RatedEvent{Model: "m", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50},
			// 70*3 + 30*1 + 50*10 = 210 + 30 + 500 = 740
			wantCost: 740,
		},
		{
			name:  "no-cache-hits-all-prompt-at-prompt-rate",
			rule:  "cached=0 → entire prompt billed at prompt rate, none at cached",
			event: RatedEvent{Model: "m", PromptTokens: 100, CachedTokens: 0, CompletionTokens: 0},
			// 100*3 = 300
			wantCost: 300,
		},
		{
			name:  "all-prompt-cached",
			rule:  "cached==prompt → billable_prompt=0, whole prompt at cached rate",
			event: RatedEvent{Model: "m", PromptTokens: 100, CachedTokens: 100, CompletionTokens: 0},
			// 0*3 + 100*1 = 100
			wantCost: 100,
		},
		{
			name:     "zero-token",
			rule:     "zero tokens → zero cost (and zero is legitimate; the model HAD a price)",
			event:    RatedEvent{Model: "m", PromptTokens: 0, CachedTokens: 0, CompletionTokens: 0},
			wantCost: 0,
		},
		{
			name:  "aborted-event-rated-normally",
			rule:  "aborted streams served real tokens; rate them — do NOT zero-rate on abort",
			event: RatedEvent{Model: "m", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, Aborted: true},
			// same as cached-subset-with-completion: abort flag does not change cost
			wantCost: 740,
		},
		{
			name:  "malformed-cached-gt-prompt-clamps-no-credit",
			rule:  "cached>prompt (malformed) clamps billable_prompt to 0; never credit phantom tokens",
			event: RatedEvent{Model: "m", PromptTokens: 10, CachedTokens: 40, CompletionTokens: 0},
			// billable_prompt clamped 0; charge reported cached at cached rate: 0*3 + 40*1 = 40
			wantCost: 40,
		},
		{
			name:    "missing-price-fails-loud-not-zero",
			rule:    "model with no price → ErrNoPrice, NOT a $0 rating (fail-closed / lost-revenue guard)",
			event:   RatedEvent{Model: "unpriced-model", PromptTokens: 100, CompletionTokens: 100},
			wantErr: ErrNoPrice,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Rate(tc.event, prices)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("[%s] err = %v, want Is(%v)", tc.rule, err, tc.wantErr)
				}
				if got != 0 {
					t.Fatalf("[%s] cost = %d on error, want 0 (must NOT bill on missing price)", tc.rule, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("[%s] unexpected err: %v", tc.rule, err)
			}
			if got != tc.wantCost {
				t.Fatalf("[%s] cost = %d, want %d", tc.rule, got, tc.wantCost)
			}
		})
	}
}

// TestRate_IntegerNoFloat asserts the result type is integer and that a price
// expressible only as a sub-cent-per-token fraction is exact in micro-USD — the
// reason we use a 1e-6 integer base instead of float or a coarser unit.
func TestRate_IntegerNoFloat(t *testing.T) {
	// $0.50 / 1M tokens = 0.5 micro-USD/token. We store integer micro-USD, so the
	// finest representable per-token price is 1 micro-USD. 1 micro/token * 1,000,000
	// tokens = 1,000,000 micro-USD = $1.00 — exact, no float drift.
	price := Price{Model: "m", PromptPriceMicro: 1, CachedPriceMicro: 0, CompletionPriceMicro: 1}
	got, err := Rate(RatedEvent{Model: "m", PromptTokens: 1_000_000, CompletionTokens: 1_000_000}, fixedPrices{p: price})
	if err != nil {
		t.Fatal(err)
	}
	var want int64 = 1_000_000 + 1_000_000
	if got != want {
		t.Fatalf("cost = %d, want %d (exact integer micro-USD, no float)", got, want)
	}
	// Compile-time guard that cost is int64 (a float would not satisfy this).
	var _ int64 = got
}

// TestBillablePromptTokens locks the clamp used by both Rate and the aggregator.
func TestBillablePromptTokens(t *testing.T) {
	cases := []struct {
		prompt, cached, want int64
	}{
		{100, 30, 70},
		{100, 0, 100},
		{100, 100, 0},
		{10, 40, 0}, // malformed clamp
	}
	for _, c := range cases {
		if got := BillablePromptTokens(c.prompt, c.cached); got != c.want {
			t.Fatalf("BillablePromptTokens(%d,%d) = %d, want %d", c.prompt, c.cached, got, c.want)
		}
	}
}
