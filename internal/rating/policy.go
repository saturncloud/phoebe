package rating

import (
	"errors"
	"fmt"
)

// ErrNoPrice is the fail-closed sentinel: the price file has no resolvable entry
// for a model (no base entry under that model_id, and — for an ft: id — no usable
// derived_from base). The caller MUST surface this (count it, log it loudly) and
// MUST NOT bill the event at $0. This is the defensive backstop behind the E4
// create-time price gate (a priced deploy should never reach the rater unpriced; if
// one does, the rater screams rather than billing $0).
var ErrNoPrice = errors.New("rating: no price for model in the price file")

// PolicyFunc is the fine-tune derivation-policy function. It mirrors the
// `fine_tune_premium.policy` field in the price YAML.
type PolicyFunc string

const (
	PolicyIdentity   PolicyFunc = "identity"   // derived = base
	PolicyMultiplier PolicyFunc = "multiplier" // derived = base * factor
	PolicyMarkup     PolicyFunc = "markup"     // derived = base + markup (per-token)
)

// Rate3 is the per-token rate triple a model resolves to, AFTER any fine-tune
// premium has been applied. Carried as exact Dec (never float).
type Rate3 struct {
	Prompt     Dec
	Cached     Dec
	Completion Dec
}

// Quantized returns the rate triple rounded to the money scale (9dp) — the EXACT
// rate that is projected into the SQL price table (NUMERIC(20,9)), stored as the
// applied rate on the rated_usage row, and multiplied to compute cost. Because the
// stored rate is 9dp and token counts are integers, cost = quantized-rate × tokens
// is itself exact at 9dp: there is no sub-nano residue, and the cost on a row is
// always reconstructable from the applied rate the row carries (self-auditing).
//
// The premium (multiplier/markup) is applied to the EXACT base rate first; only the
// final per-token rate is quantized. So a 1.5× premium on a 1-nano base yields an
// exact 0.0000000015 that rounds to 0.000000002 — and that 9dp rate is what bills.
func (r Rate3) Quantized() Rate3 {
	return Rate3{
		Prompt:     r.Prompt.Round(moneyScale),
		Cached:     r.Cached.Round(moneyScale),
		Completion: r.Completion.Round(moneyScale),
	}
}

// ApplyPolicy transforms a base per-token rate triple by the global fine-tune
// premium policy, producing the derived (fine-tune) rate. identity is the default;
// multiplier scales every component by factor; markup adds a per-token amount to
// every component. Exact decimal throughout — this is the same math the SQL applies
// (a CASE on function) and the Rate() oracle mirrors.
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
		return Rate3{}, fmt.Errorf("rating: unknown fine-tune premium policy %q", fn)
	}
}
