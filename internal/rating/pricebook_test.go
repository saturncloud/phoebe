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

// TestPriceBook_EffectiveDating is the effective-dated-price-selection rule: an
// event is rated with the price in effect at the event's time, never
// retroactively repriced by a newer price.
func TestPriceBook_EffectiveDating(t *testing.T) {
	// Two prices for "m": an OLD one (prompt=3) open from 2026-01-01 to
	// 2026-06-01, and a NEW one (prompt=5) open from 2026-06-01 onward.
	old := Price{
		Model: "m", PromptPriceMicro: 3, CachedPriceMicro: 1, CompletionPriceMicro: 9,
		EffectiveFrom: mustTime("2026-01-01T00:00:00Z"),
		EffectiveTo:   mustTime("2026-06-01T00:00:00Z"),
	}
	current := Price{
		Model: "m", PromptPriceMicro: 5, CachedPriceMicro: 2, CompletionPriceMicro: 15,
		EffectiveFrom: mustTime("2026-06-01T00:00:00Z"),
		// EffectiveTo zero == open-ended.
	}
	// Insert out of order to prove sorting.
	book := NewPriceBook([]Price{current, old})

	cases := []struct {
		name       string
		at         string
		wantPrompt int64
		wantErr    error
	}{
		{
			name:       "event-before-price-change-uses-old-price",
			at:         "2026-03-15T12:00:00Z",
			wantPrompt: 3,
		},
		{
			name:       "event-at-new-effective_from-uses-new-price",
			at:         "2026-06-01T00:00:00Z", // boundary: [from,to) → belongs to new
			wantPrompt: 5,
		},
		{
			name:       "event-after-change-uses-new-price",
			at:         "2026-09-01T00:00:00Z",
			wantPrompt: 5,
		},
		{
			name:    "event-before-any-price-fails-loud",
			at:      "2025-01-01T00:00:00Z", // predates the model's first price
			wantErr: ErrNoPrice,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := book.Price("m", mustTime(tc.at))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want Is(%v)", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if p.PromptPriceMicro != tc.wantPrompt {
				t.Fatalf("prompt price = %d, want %d (wrong effective price selected)", p.PromptPriceMicro, tc.wantPrompt)
			}
		})
	}
}

// TestPriceBook_UnknownModel proves an unknown model fails loud (ErrNoPrice), the
// fail-closed path for a model that has no price-book entry at all.
func TestPriceBook_UnknownModel(t *testing.T) {
	book := NewPriceBook([]Price{{Model: "m", EffectiveFrom: mustTime("2026-01-01T00:00:00Z")}})
	_, err := book.Price("does-not-exist", mustTime("2026-06-01T00:00:00Z"))
	if !errors.Is(err, ErrNoPrice) {
		t.Fatalf("err = %v, want Is(ErrNoPrice)", err)
	}
}

// TestPriceBook_ClosedWindowExcludesAtUpperBound proves the window is half-open:
// an event exactly at effective_to is NOT in that closed window (it belongs to
// the next one, or is unpriced if none follows).
func TestPriceBook_ClosedWindowExcludesAtUpperBound(t *testing.T) {
	p := Price{
		Model: "m", PromptPriceMicro: 3,
		EffectiveFrom: mustTime("2026-01-01T00:00:00Z"),
		EffectiveTo:   mustTime("2026-06-01T00:00:00Z"),
	}
	book := NewPriceBook([]Price{p})
	// at == effective_to → excluded (half-open) → unpriced.
	if _, err := book.Price("m", mustTime("2026-06-01T00:00:00Z")); !errors.Is(err, ErrNoPrice) {
		t.Fatalf("at upper bound: err = %v, want Is(ErrNoPrice) (half-open window)", err)
	}
	// Just before effective_to → priced.
	if _, err := book.Price("m", mustTime("2026-05-31T23:59:59Z")); err != nil {
		t.Fatalf("just before upper bound: unexpected err %v", err)
	}
}
