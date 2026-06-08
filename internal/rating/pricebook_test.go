package rating

import (
	"errors"
	"testing"
	"time"
)

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// baseRow is a model with its OWN rate, open-ended from 2026-01-01.
func baseRow(modelID, prompt, cached, completion string) PriceRow {
	return PriceRow{
		ModelID:       modelID,
		HasRate:       true,
		Prompt:        MustDec(prompt),
		Cached:        MustDec(cached),
		Completion:    MustDec(completion),
		EffectiveFrom: mustTime("2026-01-01T00:00:00Z"),
	}
}

// derivedRow is a fine-tune: NO own rate, derived_from a base, open-ended.
func derivedRow(modelID, base string) PriceRow {
	return PriceRow{
		ModelID:       modelID,
		DerivedFrom:   base,
		HasRate:       false,
		EffectiveFrom: mustTime("2026-01-01T00:00:00Z"),
	}
}

// TestResolve_EffectiveDatedSelection: an event before a price change uses the OLD
// price; at/after uses the new one; before any price fails loud. The half-open
// [from,to) boundary belongs to the new window.
func TestResolve_EffectiveDatedSelection(t *testing.T) {
	old := PriceRow{
		ModelID: "m", HasRate: true,
		Prompt: MustDec("0.000003"), Cached: MustDec("0"), Completion: MustDec("0"),
		EffectiveFrom: mustTime("2026-01-01T00:00:00Z"),
		EffectiveTo:   mustTime("2026-06-01T00:00:00Z"),
	}
	current := PriceRow{
		ModelID: "m", HasRate: true,
		Prompt: MustDec("0.000005"), Cached: MustDec("0"), Completion: MustDec("0"),
		EffectiveFrom: mustTime("2026-06-01T00:00:00Z"),
	}
	book := NewPriceBook([]PriceRow{current, old}, nil) // out of order on purpose

	cases := []struct {
		name, at, wantPrompt string
		wantErr              error
	}{
		{name: "event-before-change-uses-old-price", at: "2026-03-15T12:00:00Z", wantPrompt: "0.000003000"},
		{name: "event-at-new-effective_from-uses-new-price", at: "2026-06-01T00:00:00Z", wantPrompt: "0.000005000"},
		{name: "event-after-change-uses-new-price", at: "2026-09-01T00:00:00Z", wantPrompt: "0.000005000"},
		{name: "event-before-any-price-fails-loud", at: "2025-01-01T00:00:00Z", wantErr: ErrNoPrice},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := book.ResolvePrice("m", mustTime(tc.at))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want Is(%v)", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if r.Prompt.String() != tc.wantPrompt {
				t.Fatalf("prompt = %s, want %s (wrong effective price)", r.Prompt, tc.wantPrompt)
			}
		})
	}
}

// TestResolve_MissingPriceFailsLoud: an unknown model and a model with no row at
// `at` both fail loud (ErrNoPrice) — NEVER a $0 resolution.
func TestResolve_MissingPriceFailsLoud(t *testing.T) {
	book := NewPriceBook([]PriceRow{baseRow("m", "0.000003", "0", "0")}, nil)
	if _, err := book.ResolvePrice("does-not-exist", mustTime("2026-06-01T00:00:00Z")); !errors.Is(err, ErrNoPrice) {
		t.Fatalf("unknown model: err = %v, want ErrNoPrice", err)
	}
}

// TestResolve_HalfOpenWindow: an event exactly at effective_to is excluded (belongs
// to the next window or is unpriced).
func TestResolve_HalfOpenWindow(t *testing.T) {
	row := PriceRow{
		ModelID: "m", HasRate: true, Prompt: MustDec("0.000003"), Cached: MustDec("0"), Completion: MustDec("0"),
		EffectiveFrom: mustTime("2026-01-01T00:00:00Z"),
		EffectiveTo:   mustTime("2026-06-01T00:00:00Z"),
	}
	book := NewPriceBook([]PriceRow{row}, nil)
	if _, err := book.ResolvePrice("m", mustTime("2026-06-01T00:00:00Z")); !errors.Is(err, ErrNoPrice) {
		t.Fatalf("at upper bound: err = %v, want ErrNoPrice (half-open)", err)
	}
	if _, err := book.ResolvePrice("m", mustTime("2026-05-31T23:59:59Z")); err != nil {
		t.Fatalf("just before upper bound: unexpected err %v", err)
	}
}

// TestResolve_DerivedFromInheritance_Identity: a fine-tune (rate=null, derived_from)
// with the default identity policy inherits its base's rate exactly. This is the
// pointer-not-copy rule — a base price change auto-propagates.
func TestResolve_DerivedFromInheritance_Identity(t *testing.T) {
	base := baseRow("base", "0.000003", "0.0000003", "0.00001")
	ft := derivedRow("ft", "base")
	book := NewPriceBook([]PriceRow{base, ft}, nil) // no policy row → identity default

	r, err := book.ResolvePrice("ft", mustTime("2026-06-01T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Prompt.String() != "0.000003000" || r.Cached.String() != "0.000000300" || r.Completion.String() != "0.000010000" {
		t.Fatalf("inherited rate = %s/%s/%s, want base rate (identity)", r.Prompt, r.Cached, r.Completion)
	}
}

// TestResolve_DerivationMultiplier: fine-tune price = base × factor (1.5×).
func TestResolve_DerivationMultiplier(t *testing.T) {
	base := baseRow("base", "0.000003", "0.0000003", "0.00001")
	ft := derivedRow("ft", "base")
	pol := []PolicyRow{{
		Func: PolicyMultiplier, Factor: MustDec("1.5"),
		EffectiveFrom: mustTime("2026-01-01T00:00:00Z"),
	}}
	book := NewPriceBook([]PriceRow{base, ft}, pol)

	r, err := book.ResolvePrice("ft", mustTime("2026-06-01T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Prompt.String() != "0.000004500" || r.Cached.String() != "0.000000450" || r.Completion.String() != "0.000015000" {
		t.Fatalf("multiplier-derived = %s/%s/%s, want base×1.5", r.Prompt, r.Cached, r.Completion)
	}
}

// TestResolve_DerivationMarkup: fine-tune price = base + per-token markup.
func TestResolve_DerivationMarkup(t *testing.T) {
	base := baseRow("base", "0.000003", "0.0000003", "0.00001")
	ft := derivedRow("ft", "base")
	pol := []PolicyRow{{
		Func: PolicyMarkup, Markup: MustDec("0.000001"),
		EffectiveFrom: mustTime("2026-01-01T00:00:00Z"),
	}}
	book := NewPriceBook([]PriceRow{base, ft}, pol)

	r, err := book.ResolvePrice("ft", mustTime("2026-06-01T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Prompt.String() != "0.000004000" || r.Cached.String() != "0.000001300" || r.Completion.String() != "0.000011000" {
		t.Fatalf("markup-derived = %s/%s/%s, want base+0.000001", r.Prompt, r.Cached, r.Completion)
	}
}

// TestResolve_OwnRateBypassesPolicy: a model with its OWN explicit rate ignores the
// derivation policy entirely (the escape hatch), even if a multiplier is in effect.
func TestResolve_OwnRateBypassesPolicy(t *testing.T) {
	// "m" has its own rate AND (hypothetically) a derived_from; the own rate wins
	// and the 1000× multiplier must NOT touch it.
	own := PriceRow{
		ModelID: "m", HasRate: true, DerivedFrom: "base",
		Prompt: MustDec("0.000003"), Cached: MustDec("0.0000003"), Completion: MustDec("0.00001"),
		EffectiveFrom: mustTime("2026-01-01T00:00:00Z"),
	}
	base := baseRow("base", "0.000009", "0", "0")
	pol := []PolicyRow{{
		Func: PolicyMultiplier, Factor: MustDec("1000"),
		EffectiveFrom: mustTime("2026-01-01T00:00:00Z"),
	}}
	book := NewPriceBook([]PriceRow{own, base}, pol)

	r, err := book.ResolvePrice("m", mustTime("2026-06-01T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Prompt.String() != "0.000003000" {
		t.Fatalf("own-rate model prompt = %s, want 0.000003000 (policy must NOT apply)", r.Prompt)
	}
}

// TestResolve_OneHopOnly: a model deriving from another DERIVED model (chain > 1
// hop) is an ERROR (ErrDerivationChain), never recursed. v1 supports one hop only.
func TestResolve_OneHopOnly(t *testing.T) {
	base := baseRow("base", "0.000003", "0", "0")
	mid := derivedRow("mid", "base")  // mid derives from base (1 hop, fine on its own)
	leaf := derivedRow("leaf", "mid") // leaf derives from mid (a derived model) → 2 hops
	book := NewPriceBook([]PriceRow{base, mid, leaf}, nil)

	// mid resolves fine (one hop).
	if _, err := book.ResolvePrice("mid", mustTime("2026-06-01T00:00:00Z")); err != nil {
		t.Fatalf("mid (1 hop) should resolve, got %v", err)
	}
	// leaf is a 2-hop chain → error, not recursion.
	_, err := book.ResolvePrice("leaf", mustTime("2026-06-01T00:00:00Z"))
	if !errors.Is(err, ErrDerivationChain) {
		t.Fatalf("leaf (2 hops): err = %v, want ErrDerivationChain", err)
	}
}

// TestResolve_DerivedBaseMissing: a fine-tune whose base has no effective rate at
// `at` is unpriced (fail loud), not $0.
func TestResolve_DerivedBaseMissing(t *testing.T) {
	// base's rate only opens in June; an April event for the fine-tune can't resolve.
	base := PriceRow{
		ModelID: "base", HasRate: true, Prompt: MustDec("0.000003"), Cached: MustDec("0"), Completion: MustDec("0"),
		EffectiveFrom: mustTime("2026-06-01T00:00:00Z"),
	}
	ft := derivedRow("ft", "base") // ft effective from Jan, base only from June
	book := NewPriceBook([]PriceRow{base, ft}, nil)
	if _, err := book.ResolvePrice("ft", mustTime("2026-04-01T00:00:00Z")); !errors.Is(err, ErrNoPrice) {
		t.Fatalf("derived with no base rate: err = %v, want ErrNoPrice", err)
	}
}

// TestResolve_DerivationPolicyEffectiveDated: the policy in effect at the event's
// time is the one applied — a later policy change does not retroactively reprice.
func TestResolve_DerivationPolicyEffectiveDated(t *testing.T) {
	base := baseRow("base", "0.000004", "0", "0")
	ft := derivedRow("ft", "base")
	pol := []PolicyRow{
		{Func: PolicyMultiplier, Factor: MustDec("1.5"),
			EffectiveFrom: mustTime("2026-01-01T00:00:00Z"), EffectiveTo: mustTime("2026-06-01T00:00:00Z")},
		{Func: PolicyMultiplier, Factor: MustDec("2"),
			EffectiveFrom: mustTime("2026-06-01T00:00:00Z")},
	}
	book := NewPriceBook([]PriceRow{base, ft}, pol)

	// March → 1.5× policy: 0.000004*1.5 = 0.000006
	r, err := book.ResolvePrice("ft", mustTime("2026-03-01T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Prompt.String() != "0.000006000" {
		t.Fatalf("March prompt = %s, want 0.000006000 (1.5× policy)", r.Prompt)
	}
	// July → 2× policy: 0.000004*2 = 0.000008
	r, err = book.ResolvePrice("ft", mustTime("2026-07-01T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Prompt.String() != "0.000008000" {
		t.Fatalf("July prompt = %s, want 0.000008000 (2× policy)", r.Prompt)
	}
}
