package rating

import (
	"testing"
)

// TestRate is the money core. Each case names the billing rule it defends. The
// per-token rates here are the RESOLVED rates (own, or base-through-policy);
// resolution itself is exercised in pricebook_test.go. Rate is the multiply-and-sum
// over the billable-prompt formula.
func TestRate(t *testing.T) {
	// prompt $3/1M = 0.000003/token; cached $0.30/1M = 0.0000003/token (DISTINCT,
	// discounted); completion $10/1M = 0.00001/token. Real sub-cent prices, exact in
	// NUMERIC — the bug that drove the move off integer micro-USD.
	r := rate3("0.000003", "0.0000003", "0.00001")

	cases := []struct {
		name string
		rule string
		ev   RatedEvent
		want string // expected cost, NUMERIC at moneyScale (9dp)
	}{
		{
			name: "cached-subset-no-double-count",
			rule: "billable_prompt=(prompt-cached) at prompt rate + cached at cached rate; cached charged ONCE",
			ev:   RatedEvent{PromptTokens: 100, CachedTokens: 30, CompletionTokens: 0},
			// (100-30)*0.000003 + 30*0.0000003 = 0.000210 + 0.000009 = 0.000219
			want: "0.000219000",
		},
		{
			name: "cached-subset-with-completion",
			rule: "all three token classes summed with their own rates",
			ev:   RatedEvent{PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50},
			// 0.000219 + 50*0.00001 = 0.000219 + 0.0005 = 0.000719
			want: "0.000719000",
		},
		{
			name: "no-cache-hits-all-prompt-at-prompt-rate",
			rule: "cached=0 → entire prompt billed at prompt rate, none at cached",
			ev:   RatedEvent{PromptTokens: 100, CachedTokens: 0, CompletionTokens: 0},
			// 100*0.000003 = 0.0003
			want: "0.000300000",
		},
		{
			name: "all-prompt-cached",
			rule: "cached==prompt → billable_prompt=0, whole prompt at cached rate",
			ev:   RatedEvent{PromptTokens: 100, CachedTokens: 100, CompletionTokens: 0},
			// 0 + 100*0.0000003 = 0.00003
			want: "0.000030000",
		},
		{
			name: "zero-token",
			rule: "zero tokens → zero cost (legitimate; the model HAD a price)",
			ev:   RatedEvent{PromptTokens: 0, CachedTokens: 0, CompletionTokens: 0},
			want: "0.000000000",
		},
		{
			name: "aborted-event-rated-normally",
			rule: "aborted streams served real tokens; rate them — do NOT zero-rate on abort",
			ev:   RatedEvent{PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, Aborted: true},
			want: "0.000719000", // identical to cached-subset-with-completion
		},
		{
			name: "malformed-cached-gt-prompt-clamps-no-credit",
			rule: "cached>prompt (malformed) clamps billable_prompt to 0; never credit phantom tokens",
			ev:   RatedEvent{PromptTokens: 10, CachedTokens: 40, CompletionTokens: 0},
			// billable_prompt clamped 0; charge reported cached at cached rate: 40*0.0000003 = 0.000012
			want: "0.000012000",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Rate(tc.ev, r).String()
			if got != tc.want {
				t.Fatalf("[%s] cost = %s, want %s", tc.rule, got, tc.want)
			}
		})
	}
}

// TestRate_NumericExactnessNoFloat: a sub-$1/1M price ($0.15/1M = 0.00000015/token)
// is EXACT in NUMERIC and survives a large token count with no float drift — the
// exact reason v2 stores money as NUMERIC, not float and not a coarse integer unit.
func TestRate_NumericExactnessNoFloat(t *testing.T) {
	// $0.15 / 1,000,000 tokens = 0.000000150 USD/token. In float64 this is not
	// exactly representable; in NUMERIC/big.Rat it is exact.
	r := rate3("0.00000015", "0", "0.0000006") // gpt-4o-mini-ish prompt/cached/completion
	got := Rate(RatedEvent{PromptTokens: 1_000_000, CompletionTokens: 1_000_000}, r).String()
	// 1e6 * 0.00000015 + 1e6 * 0.0000006 = 0.15 + 0.60 = 0.75 exactly.
	if got != "0.750000000" {
		t.Fatalf("cost = %s, want 0.750000000 (exact NUMERIC, no float drift)", got)
	}

	// And the per-token price round-trips exactly at the column scale (9dp).
	if s := MustDec("0.00000015").String(); s != "0.000000150" {
		t.Fatalf("price string = %s, want 0.000000150 (representable at 9dp)", s)
	}
}

// TestDec_RoundHalfUp pins the oracle's rounding to the DB's NUMERIC half-up so
// they never disagree on the last digit.
func TestDec_RoundHalfUp(t *testing.T) {
	cases := []struct{ in, want string }{
		{"0.0000000005", "0.000000001"},   // half → away from zero (up)
		{"0.0000000004", "0.000000000"},   // below half → down
		{"-0.0000000005", "-0.000000001"}, // negative half → away from zero
		{"1.2345678905", "1.234567891"},
	}
	for _, c := range cases {
		if got := MustDec(c.in).String(); got != c.want {
			t.Fatalf("Round(%s) = %s, want %s", c.in, got, c.want)
		}
	}
}

// TestApplyPolicy covers the three derivation functions, exact.
func TestApplyPolicy(t *testing.T) {
	base := rate3("0.000003", "0.0000003", "0.00001")

	t.Run("derivation-identity", func(t *testing.T) {
		got, err := ApplyPolicy(base, PolicyIdentity, Dec{}, Dec{})
		if err != nil {
			t.Fatal(err)
		}
		if !got.Prompt.Equal(base.Prompt) || !got.Cached.Equal(base.Cached) || !got.Completion.Equal(base.Completion) {
			t.Fatalf("identity changed the rate: %+v", got)
		}
	})

	t.Run("derivation-multiplier-1.5x", func(t *testing.T) {
		got, err := ApplyPolicy(base, PolicyMultiplier, MustDec("1.5"), Dec{})
		if err != nil {
			t.Fatal(err)
		}
		// 0.000003 * 1.5 = 0.0000045 ; 0.0000003*1.5=0.00000045 ; 0.00001*1.5=0.000015
		if got.Prompt.String() != "0.000004500" || got.Cached.String() != "0.000000450" || got.Completion.String() != "0.000015000" {
			t.Fatalf("multiplier wrong: %s/%s/%s", got.Prompt, got.Cached, got.Completion)
		}
	})

	t.Run("derivation-markup-additive", func(t *testing.T) {
		got, err := ApplyPolicy(base, PolicyMarkup, Dec{}, MustDec("0.000001"))
		if err != nil {
			t.Fatal(err)
		}
		// +0.000001 per token to each component.
		if got.Prompt.String() != "0.000004000" || got.Cached.String() != "0.000001300" || got.Completion.String() != "0.000011000" {
			t.Fatalf("markup wrong: %s/%s/%s", got.Prompt, got.Cached, got.Completion)
		}
	})

	t.Run("unknown-function-errors", func(t *testing.T) {
		if _, err := ApplyPolicy(base, PolicyFunc("nope"), Dec{}, Dec{}); err == nil {
			t.Fatal("expected error for unknown policy function")
		}
	})
}

// TestBillablePromptTokens locks the clamp used by both Rate and the SQL GREATEST.
func TestBillablePromptTokens(t *testing.T) {
	cases := []struct{ prompt, cached, want int64 }{
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

// TestParseDec_RejectsGarbage proves the money parser fails closed on non-decimal
// input rather than silently yielding zero (a $0 bill).
func TestParseDec_RejectsGarbage(t *testing.T) {
	for _, s := range []string{"", "abc", "1.2.3", "$3"} {
		if _, err := ParseDec(s); err == nil {
			t.Fatalf("ParseDec(%q) = nil err, want parse error (must not silently become 0)", s)
		}
	}
}
