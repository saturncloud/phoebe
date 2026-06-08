package rating

import (
	"sort"
	"time"
)

// PriceBook is an in-memory, effective-dated price book. It is loaded once from
// model_price (Store.LoadPrices) at the start of a rating run and then resolves
// prices for thousands of events without re-querying — the book is small (one row
// per model per price change) and immutable for the duration of the run, so a
// snapshot is correct and fast.
//
// EFFECTIVE-DATING is the core invariant: Price(model, at) returns the price whose
// [effective_from, effective_to) window contains `at`. Rating "now" therefore uses
// the price that was in effect WHEN THE EVENT HAPPENED, never the current price —
// so a price change never retroactively reprices already-served traffic.
type PriceBook struct {
	// byModel holds each model's prices sorted by EffectiveFrom ASC, so a reverse
	// scan finds the newest price effective at-or-before `at`.
	byModel map[string][]Price
}

// NewPriceBook builds a price book from price rows (any order). It does not query
// the DB; the Store loads the rows and hands them here, so the book is unit
// testable without Postgres.
func NewPriceBook(prices []Price) *PriceBook {
	byModel := make(map[string][]Price)
	for _, p := range prices {
		byModel[p.Model] = append(byModel[p.Model], p)
	}
	for model := range byModel {
		ps := byModel[model]
		sort.Slice(ps, func(i, j int) bool {
			return ps[i].EffectiveFrom.Before(ps[j].EffectiveFrom)
		})
		byModel[model] = ps
	}
	return &PriceBook{byModel: byModel}
}

// Price resolves the price for model effective at `at`, or ErrNoPrice.
//
// A row matches when effective_from <= at AND (effective_to is open OR at <
// effective_to) — i.e. the half-open window [from, to). If multiple rows match
// (overlapping windows, which the data shouldn't have but we don't assume), the
// one with the LATEST effective_from wins — the most specific/recent price.
//
// Returns ErrNoPrice (the fail-closed sentinel) when the model is unknown OR
// known but has no row covering `at` (e.g. the event predates the model's first
// price). The caller MUST NOT treat ErrNoPrice as $0.
func (b *PriceBook) Price(model string, at time.Time) (Price, error) {
	ps, ok := b.byModel[model]
	if !ok {
		return Price{}, ErrNoPrice
	}
	// Scan newest-first (slice is ASC by EffectiveFrom) and take the first whose
	// window contains `at`.
	for i := len(ps) - 1; i >= 0; i-- {
		p := ps[i]
		if at.Before(p.EffectiveFrom) {
			continue // price not yet in effect at `at`
		}
		// effective_to zero value == open-ended.
		if !p.EffectiveTo.IsZero() && !at.Before(p.EffectiveTo) {
			continue // `at` is at/after this window's close
		}
		return p, nil
	}
	return Price{}, ErrNoPrice
}
