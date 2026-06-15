package rating

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v2"
)

// fineTunePrefix is the reserved prefix that marks a model_id as a fine-tune
// (E3: `ft:<checkpoint_artifact_id>`). It can never collide with a Hugging Face
// base id (an HF id is `org/name`, no `ft:` prefix), so a base-model entry keyed on
// an HF id and a fine-tune keyed on an ft: id live in disjoint namespaces.
const fineTunePrefix = "ft:"

// --- THE PRICE FILE SCHEMA (the operator-facing contract) -------------------
//
// The price file is the SINGLE source of truth for what every model costs (E1).
// An operator authors and version-controls it; the file's history IS the price
// audit trail (no DB price table, no effective-dating, no audit table). The hourly
// rater loads the CURRENT file at run start and rates the last complete hour with
// whatever rate the file carries — and freezes that rate onto the rated_usage row.
//
// All rates are STRINGS in YAML and parsed to the exact decimal Dec, NEVER a float:
// a per-token price like $0.15/1M = 0.000000150 is exact as a decimal string and
// would lose precision through a YAML float. The loader REJECTS a float-shaped or
// negative value (fail closed) — see validate().
//
// Schema (see config/prices.example.yaml for a fully-commented example):
//
//	version: 1
//	base_models:
//	  "meta-llama/Llama-3.1-8B-Instruct":     # key = the Hugging Face model id
//	    prompt:     "0.000000200"             # per-token USD, exact decimal string
//	    cached:     "0.000000050"
//	    completion: "0.000000600"
//	fine_tune_premium:                         # the SINGLE global premium policy
//	  policy: multiplier                       # identity | multiplier | markup
//	  factor: "1.5"                            # set iff policy == multiplier
//	  markup: "0.000000100"                   # set iff policy == markup (per-token USD)
//	gpu_floor_rates:                           # per-GPU floor (uptime meter, not the
//	  "A100-80GB": "0.000000000"               # token rater — parsed + validated now,
//	  "H100-80GB": "0.000000000"               # not yet wired)

// PriceFile is the on-disk YAML shape. It is decoded as-is (strings) and then
// parsed/validated into a PriceBook. Kept separate from PriceBook so the wire
// format and the in-memory exact-decimal model don't entangle.
type PriceFile struct {
	// Version pins the schema. Only version 1 is understood; any other value fails
	// the load (fail closed) so an operator can evolve the format without a silent
	// mis-parse on an old binary.
	Version int `yaml:"version"`

	// BaseModels maps a Hugging Face base model id → its per-token rates.
	BaseModels map[string]rateYAML `yaml:"base_models"`

	// FineTunePremium is the single global fine-tune premium policy.
	FineTunePremium fineTunePremiumYAML `yaml:"fine_tune_premium"`

	// GPUFloorRates maps a GPU type → a per-GPU floor rate (per-token USD). Consumed
	// by the uptime meter later; the token rater PARSES and VALIDATES it (so the file
	// is complete and well-formed) but does not yet USE it.
	GPUFloorRates map[string]string `yaml:"gpu_floor_rates"`
}

// rateYAML is one base model's three per-token rates, as exact-decimal STRINGS.
type rateYAML struct {
	Prompt     string `yaml:"prompt"`
	Cached     string `yaml:"cached"`
	Completion string `yaml:"completion"`
}

// fineTunePremiumYAML is the global premium policy, as strings.
type fineTunePremiumYAML struct {
	Policy string `yaml:"policy"` // identity | multiplier | markup
	Factor string `yaml:"factor"` // multiplier parameter
	Markup string `yaml:"markup"` // markup parameter (per-token USD)
}

// PriceBook is the loaded, validated, exact-decimal price book the rater bills
// from. It is IMMUTABLE after load. Resolution mirrors, in Go, exactly what the SQL
// join does (store.go): a base model id resolves to its own rate; an ft: id
// resolves to its derived_from base's rate transformed by the global premium; an
// unknown id is ErrNoPrice (never $0).
//
// FINE-TUNE BASE LINKAGE — KNOWN GAP (flagged): billing_event today carries only the
// engine-reported model NAME (migrations/0001_billing_event.sql: no derived_from /
// base_model column). So an event whose model_id is an `ft:<checkpoint>` id has NO
// base linkage available at rating time. This PriceBook supports a fine-tune's
// linkage being carried IN THE FILE — a base_models or fine_tunes entry can name a
// derived_from — but until the metering path plumbs the base (saturn.io/...base_model)
// through to billing_event OR a fine-tune→base map ships in the file, an ft: id that
// is not itself a key in the file resolves to ErrNoPrice (fail loud, never $0). A
// base-direct event (model_id IS a key) prices fully today.
type PriceBook struct {
	// base maps model_id → its own per-token rate (base models AND any fine-tune
	// that carries its own explicit rate in the file).
	base map[string]Rate3
	// derivedFrom maps a fine-tune model_id → the base model_id it inherits from.
	// Populated only for fine-tune entries that declare a derived_from in the file.
	derivedFrom map[string]string

	// The single global fine-tune premium policy.
	policyFn     PolicyFunc
	policyFactor Dec
	policyMarkup Dec

	// gpuFloor maps GPU type → per-token floor rate. Parsed + validated; the token
	// rater does not yet consume it (the uptime meter will).
	gpuFloor map[string]Dec
}

// fineTuneEntry is an optional fine-tune declaration in the file: a fine-tune
// model_id and the base it derives from (and/or its own rate). Carried under a
// `fine_tunes` map so the file CAN express fine-tune base linkage today, ahead of
// the metering path plumbing derived_from through billing_event. (Base-direct
// pricing — model_id is a base_models key — works without any fine_tunes entry.)
type fineTuneEntry struct {
	DerivedFrom string    `yaml:"derived_from"`
	Rate        *rateYAML `yaml:"rate"` // optional own rate (escape hatch; bypasses premium)
}

// LoadPriceBook reads, parses, and validates the price file at path, returning an
// immutable PriceBook. It FAILS CLOSED: a missing file, malformed YAML, an unknown
// schema version, a non-decimal/negative rate, or an inconsistent premium policy is
// an error — the rater refuses to run rather than rate at $0 or a wrong rate.
//
// SEAM FOR S3 (out of scope here): the file is loaded from a local path. To fetch
// from S3, fetch-to-local then call LoadPriceBook(localPath) — the create-time price
// gate (E4) and the rater MUST read the same file/version, so a single fetched copy
// is the natural shared artifact.
func LoadPriceBook(path string) (*PriceBook, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("rating: read price file %q: %w", path, err)
	}
	return ParsePriceBook(data)
}

// ParsePriceBook parses+validates raw YAML bytes into a PriceBook. Split from
// LoadPriceBook so tests can exercise the parser without touching the filesystem.
// UnmarshalStrict rejects unknown keys, so a typo'd field (e.g. `promt:`) fails the
// load rather than silently pricing a token at $0.
func ParsePriceBook(data []byte) (*PriceBook, error) {
	// A second, strict view to capture the optional fine_tunes map without bloating
	// the primary PriceFile struct's documented surface.
	var raw struct {
		PriceFile `yaml:",inline"`
		FineTunes map[string]fineTuneEntry `yaml:"fine_tunes"`
	}
	if err := yaml.UnmarshalStrict(data, &raw); err != nil {
		return nil, fmt.Errorf("rating: parse price file: %w", err)
	}
	return buildPriceBook(raw.PriceFile, raw.FineTunes)
}

func buildPriceBook(f PriceFile, fineTunes map[string]fineTuneEntry) (*PriceBook, error) {
	if f.Version != 1 {
		return nil, fmt.Errorf("rating: price file version %d unsupported (want 1)", f.Version)
	}
	if len(f.BaseModels) == 0 {
		return nil, fmt.Errorf("rating: price file has no base_models (an empty price book would $0-rate everything)")
	}

	pb := &PriceBook{
		base:        map[string]Rate3{},
		derivedFrom: map[string]string{},
		gpuFloor:    map[string]Dec{},
	}

	// Base models: keyed on the HF id. A base id must NOT carry the ft: prefix
	// (that namespace is for fine-tunes) — reject it so a base can never masquerade
	// as a fine-tune key (or vice versa).
	for id, r := range f.BaseModels {
		if strings.HasPrefix(id, fineTunePrefix) {
			return nil, fmt.Errorf("rating: base_models key %q must not use the %q fine-tune prefix", id, fineTunePrefix)
		}
		rate, err := parseRate3(id, r)
		if err != nil {
			return nil, err
		}
		pb.base[id] = rate
	}

	// Optional fine-tune entries: each names a derived_from base (and/or an own
	// rate). The key SHOULD be an ft: id (E3); we don't hard-require the prefix here
	// (the operator owns the namespace), but the derived_from MUST resolve to a base.
	for id, fe := range fineTunes {
		if _, dup := pb.base[id]; dup {
			return nil, fmt.Errorf("rating: model_id %q appears in both base_models and fine_tunes", id)
		}
		hasOwn := fe.Rate != nil
		if !hasOwn && fe.DerivedFrom == "" {
			return nil, fmt.Errorf("rating: fine_tunes[%q] has neither a derived_from nor an own rate (it would be unpriceable)", id)
		}
		if hasOwn {
			rate, err := parseRate3(id, *fe.Rate)
			if err != nil {
				return nil, err
			}
			pb.base[id] = rate // own rate wins; bypasses the premium (escape hatch)
		}
		if fe.DerivedFrom != "" {
			pb.derivedFrom[id] = fe.DerivedFrom
		}
	}

	// Every declared derived_from must point at a known base — one hop only. A
	// fine-tune deriving from another fine-tune (a base that is itself derived and
	// has no own rate) is rejected at load: v1 never recurses, and a dangling base
	// would silently make the fine-tune unpriceable.
	for ft, base := range pb.derivedFrom {
		if _, ok := pb.base[base]; !ok {
			return nil, fmt.Errorf("rating: fine_tunes[%q] derived_from %q which is not a priced base model", ft, base)
		}
	}

	// The global fine-tune premium policy.
	fn, factor, markup, err := parsePremium(f.FineTunePremium)
	if err != nil {
		return nil, err
	}
	pb.policyFn, pb.policyFactor, pb.policyMarkup = fn, factor, markup

	// Per-GPU floor rates (validated now, used by the uptime meter later).
	for gpu, s := range f.GPUFloorRates {
		d, err := ParseDec(s)
		if err != nil {
			return nil, fmt.Errorf("rating: gpu_floor_rates[%q]: %w", gpu, err)
		}
		if d.Sign() < 0 {
			return nil, fmt.Errorf("rating: gpu_floor_rates[%q] is negative (%s)", gpu, s)
		}
		pb.gpuFloor[gpu] = d
	}

	return pb, nil
}

// parseRate3 parses and validates one model's three per-token rates. All three are
// REQUIRED (all-or-nothing: a missing component would NULL a cost term and silently
// under-bill) and must be non-negative (a negative rate would credit the customer).
func parseRate3(modelID string, r rateYAML) (Rate3, error) {
	if r.Prompt == "" || r.Cached == "" || r.Completion == "" {
		return Rate3{}, fmt.Errorf("rating: model %q must set all of prompt/cached/completion (got %q/%q/%q)",
			modelID, r.Prompt, r.Cached, r.Completion)
	}
	prompt, err := parseNonNegRate(modelID, "prompt", r.Prompt)
	if err != nil {
		return Rate3{}, err
	}
	cached, err := parseNonNegRate(modelID, "cached", r.Cached)
	if err != nil {
		return Rate3{}, err
	}
	completion, err := parseNonNegRate(modelID, "completion", r.Completion)
	if err != nil {
		return Rate3{}, err
	}
	return Rate3{Prompt: prompt, Cached: cached, Completion: completion}, nil
}

func parseNonNegRate(modelID, field, s string) (Dec, error) {
	d, err := ParseDec(s)
	if err != nil {
		return Dec{}, fmt.Errorf("rating: model %q %s rate: %w", modelID, field, err)
	}
	if d.Sign() < 0 {
		return Dec{}, fmt.Errorf("rating: model %q %s rate is negative (%s) — a price must never credit the customer", modelID, field, s)
	}
	// FAIL CLOSED on a sub-9dp rate that quantizes to ZERO. Money is stored and
	// billed at 9dp (moneyScale / NUMERIC(20,9)); a finer nonzero rate is silently
	// rounded, and one that rounds to 0 (e.g. "0.0000000001") would serve the model
	// for FREE — exactly the lost-revenue outcome this package exists to prevent. An
	// operator who writes a nonzero number clearly intends a nonzero price, so a
	// round-to-zero is a MIS-PRICED model, not a $0 model. A literal $0 (d.Sign()==0
	// here) is a legitimate, intentional free rate and is allowed.
	if d.Sign() > 0 && d.Round(moneyScale).IsZero() {
		return Dec{}, fmt.Errorf("rating: model %q %s rate %s is nonzero but rounds to $0 at %d-decimal (nano-USD) precision — it would bill the model for FREE; write a rate >= 0.000000001 or 0 for an intentional free rate",
			modelID, field, s, moneyScale)
	}
	return d, nil
}

// parsePremium parses+validates the global premium policy. Exactly the parameter
// for the chosen policy is set; an absent fine_tune_premium block defaults to
// identity (a fine-tune costs exactly what its base costs).
func parsePremium(p fineTunePremiumYAML) (PolicyFunc, Dec, Dec, error) {
	switch PolicyFunc(p.Policy) {
	case "", PolicyIdentity:
		if p.Factor != "" || p.Markup != "" {
			return "", Dec{}, Dec{}, fmt.Errorf("rating: identity premium must not set factor/markup")
		}
		return PolicyIdentity, Dec{}, Dec{}, nil
	case PolicyMultiplier:
		if p.Factor == "" || p.Markup != "" {
			return "", Dec{}, Dec{}, fmt.Errorf("rating: multiplier premium must set factor (and not markup)")
		}
		factor, err := ParseDec(p.Factor)
		if err != nil {
			return "", Dec{}, Dec{}, fmt.Errorf("rating: premium factor: %w", err)
		}
		if factor.Sign() < 0 {
			return "", Dec{}, Dec{}, fmt.Errorf("rating: premium factor is negative (%s)", p.Factor)
		}
		return PolicyMultiplier, factor, Dec{}, nil
	case PolicyMarkup:
		if p.Markup == "" || p.Factor != "" {
			return "", Dec{}, Dec{}, fmt.Errorf("rating: markup premium must set markup (and not factor)")
		}
		markup, err := ParseDec(p.Markup)
		if err != nil {
			return "", Dec{}, Dec{}, fmt.Errorf("rating: premium markup: %w", err)
		}
		if markup.Sign() < 0 {
			return "", Dec{}, Dec{}, fmt.Errorf("rating: premium markup is negative (%s)", p.Markup)
		}
		return PolicyMarkup, Dec{}, markup, nil
	default:
		return "", Dec{}, Dec{}, fmt.Errorf("rating: unknown fine_tune_premium.policy %q (want identity|multiplier|markup)", p.Policy)
	}
}

// Resolve returns the effective per-token rate triple for modelID, applying the
// global fine-tune premium when the model inherits from a base. Returns ErrNoPrice
// when nothing resolves — NEVER a $0 rate. This is the Go mirror of the SQL join
// (store.go) and the spec the conformance test pins the SQL to.
func (pb *PriceBook) Resolve(modelID string) (Rate3, error) {
	// 1. Own rate wins (a base model, or a fine-tune with its own rate): bypass the
	//    premium entirely (the escape hatch).
	if r, ok := pb.base[modelID]; ok {
		return r, nil
	}
	// 2. A fine-tune that declares a derived_from inherits the base rate transformed
	//    by the global premium — ONE hop (the base must have its own rate; the loader
	//    guarantees this).
	if base, ok := pb.derivedFrom[modelID]; ok {
		baseRate := pb.base[base] // guaranteed present by buildPriceBook's validation
		return ApplyPolicy(baseRate, pb.policyFn, pb.policyFactor, pb.policyMarkup)
	}
	// 3. Unknown id (including an ft: id with no in-file linkage — the flagged gap).
	return Rate3{}, ErrNoPrice
}

// PolicyFn / PolicyFactor / PolicyMarkup expose the loaded global premium so the
// store can project it into the SQL resolution. Read-only.
func (pb *PriceBook) PolicyFn() PolicyFunc { return pb.policyFn }
func (pb *PriceBook) PolicyFactor() Dec    { return pb.policyFactor }
func (pb *PriceBook) PolicyMarkup() Dec    { return pb.policyMarkup }

// resolvedRate is one row projected into the SQL rater's transient price table: a
// model_id and the FINAL per-token rate (premium already applied), as canonical
// decimal strings. Building this in Go keeps the premium math in exact Dec and lets
// the SQL join a flat (model_id → rate) table — the SQL still does all the cost
// MULTIPLY-and-SUM money math over NUMERIC (see store.go).
type resolvedRate struct {
	ModelID    string
	Prompt     string
	Cached     string
	Completion string
}

// resolvedRates returns every priced model_id with its FINAL per-token rate
// (premium applied), sorted by model_id for a deterministic projection. This is the
// flat price table the rater materialises and joins.
func (pb *PriceBook) resolvedRates() []resolvedRate {
	out := make([]resolvedRate, 0, len(pb.base)+len(pb.derivedFrom))
	for id := range pb.base {
		r := pb.base[id]
		out = append(out, resolvedRate{
			ModelID: id, Prompt: r.Prompt.String(), Cached: r.Cached.String(), Completion: r.Completion.String(),
		})
	}
	for ft := range pb.derivedFrom {
		if _, hasOwn := pb.base[ft]; hasOwn {
			continue // own rate already emitted; premium does not apply
		}
		r, err := pb.Resolve(ft)
		if err != nil {
			continue // unreachable: loader validated the linkage
		}
		out = append(out, resolvedRate{
			ModelID: ft, Prompt: r.Prompt.String(), Cached: r.Cached.String(), Completion: r.Completion.String(),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ModelID < out[j].ModelID })
	return out
}
