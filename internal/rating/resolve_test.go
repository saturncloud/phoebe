package rating

import (
	"sort"
	"time"
)

// PriceRow is one row of the model_price table, as loaded for the oracle /
// conformance test. Rate fields are nil-able (a derived model inherits), carried
// as exact Dec. This is the in-memory shape the resolution oracle walks; the
// PRODUCTION rater never loads these — it resolves prices in the SQL join.
type PriceRow struct {
	ModelID     string
	DerivedFrom string // "" == base model (no inheritance)
	HasRate     bool   // true when this row carries its own prompt/cached/completion rate
	Prompt      Dec
	Cached      Dec
	Completion  Dec

	EffectiveFrom time.Time
	EffectiveTo   time.Time // zero == open-ended
}

// PolicyRow is one row of the derivation_policy table for the oracle.
type PolicyRow struct {
	Func          PolicyFunc
	Factor        Dec // set iff Func == multiplier
	Markup        Dec // set iff Func == markup
	EffectiveFrom time.Time
	EffectiveTo   time.Time // zero == open-ended
}

// PriceBook is the in-memory effective-dated price book + derivation policy used by
// the Rate() ORACLE and the conformance test. It mirrors, in Go, exactly the
// resolution the SQL rater performs in its join:
//
//	own rate at `at`  →  else base's rate at `at` via derived_from (ONE hop) through
//	the policy at `at`  →  else ErrNoPrice.
//
// It is NOT used on the production rating path (that is pure SQL); it is the spec
// the SQL is pinned to.
type PriceBook struct {
	byModel  map[string][]PriceRow // each model's rows, sorted by EffectiveFrom ASC
	policies []PolicyRow           // sorted by EffectiveFrom ASC
}

// NewPriceBook builds a price book from rows (any order) and the policy rows.
func NewPriceBook(rows []PriceRow, policies []PolicyRow) *PriceBook {
	byModel := make(map[string][]PriceRow)
	for _, r := range rows {
		byModel[r.ModelID] = append(byModel[r.ModelID], r)
	}
	for m := range byModel {
		rs := byModel[m]
		sort.Slice(rs, func(i, j int) bool { return rs[i].EffectiveFrom.Before(rs[j].EffectiveFrom) })
		byModel[m] = rs
	}
	ps := append([]PolicyRow(nil), policies...)
	sort.Slice(ps, func(i, j int) bool { return ps[i].EffectiveFrom.Before(ps[j].EffectiveFrom) })
	return &PriceBook{byModel: byModel, policies: ps}
}

// ResolvePrice resolves the effective per-token rate triple for modelID at `at`,
// applying the derivation policy when the model inherits from a base. Returns
// ErrNoPrice when nothing resolves, or ErrDerivationChain when the base is itself
// a derived model (chain > 1 hop, unsupported in v1).
func (b *PriceBook) ResolvePrice(modelID string, at time.Time) (Rate3, error) {
	row, ok := b.rowAt(modelID, at)
	if !ok {
		return Rate3{}, ErrNoPrice
	}

	// 1. Own rate wins (the escape hatch): bypass the derivation policy entirely.
	if row.HasRate {
		return Rate3{Prompt: row.Prompt, Cached: row.Cached, Completion: row.Completion}, nil
	}

	// 2. Inherit from the base via derived_from — ONE hop only.
	if row.DerivedFrom == "" {
		// No own rate AND no base: nothing to inherit. (The schema CHECK forbids
		// this row, but the oracle fails closed rather than trusting the constraint.)
		return Rate3{}, ErrNoPrice
	}
	base, ok := b.rowAt(row.DerivedFrom, at)
	if !ok {
		return Rate3{}, ErrNoPrice
	}
	if !base.HasRate {
		// The base is itself derived (or rate-less): a chain longer than one hop.
		// v1 does not recurse — treat as an error, never unbounded recursion.
		return Rate3{}, ErrDerivationChain
	}
	baseRate := Rate3{Prompt: base.Prompt, Cached: base.Cached, Completion: base.Completion}

	// 3. Transform the base rate by the policy effective at `at`. Absent any policy
	//    row, identity is the default.
	fn, factor, markup := b.policyAt(at)
	return ApplyPolicy(baseRate, fn, factor, markup)
}

// rowAt returns the model's price row whose [effective_from, effective_to) window
// contains `at` (latest effective_from wins), or ok=false.
func (b *PriceBook) rowAt(modelID string, at time.Time) (PriceRow, bool) {
	rs, ok := b.byModel[modelID]
	if !ok {
		return PriceRow{}, false
	}
	for i := len(rs) - 1; i >= 0; i-- {
		r := rs[i]
		if at.Before(r.EffectiveFrom) {
			continue
		}
		if !r.EffectiveTo.IsZero() && !at.Before(r.EffectiveTo) {
			continue
		}
		return r, true
	}
	return PriceRow{}, false
}

// policyAt returns the derivation function + parameters effective at `at`. With no
// policy row in effect, the default is identity (a fine-tune costs what its base
// costs).
func (b *PriceBook) policyAt(at time.Time) (PolicyFunc, Dec, Dec) {
	for i := len(b.policies) - 1; i >= 0; i-- {
		p := b.policies[i]
		if at.Before(p.EffectiveFrom) {
			continue
		}
		if !p.EffectiveTo.IsZero() && !at.Before(p.EffectiveTo) {
			continue
		}
		return p.Func, p.Factor, p.Markup
	}
	return PolicyIdentity, Dec{}, Dec{}
}
