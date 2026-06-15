package rating

import (
	"errors"
	"strings"
	"testing"
)

// TestLoad_BasePriceApplied (yaml-base-price-applied): a base model keyed on its HF
// id resolves to exactly the per-token rates the file declares, parsed as exact
// decimal (no float drift).
func TestLoad_BasePriceApplied(t *testing.T) {
	const y = `
version: 1
base_models:
  "meta-llama/Llama-3.1-8B-Instruct":
    prompt:     "0.000000200"
    cached:     "0.000000050"
    completion: "0.000000600"
`
	pb, err := ParsePriceBook([]byte(y))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	r, err := pb.Resolve("meta-llama/Llama-3.1-8B-Instruct")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if r.Prompt.String() != "0.000000200" || r.Cached.String() != "0.000000050" || r.Completion.String() != "0.000000600" {
		t.Fatalf("rates = %s/%s/%s, want the file's exact values", r.Prompt, r.Cached, r.Completion)
	}
}

// TestLoad_FineTunePremiumMultiplier (yaml-fine-tune-premium-multiplier): an ft: id
// with an in-file derived_from inherits base × factor.
func TestLoad_FineTunePremiumMultiplier(t *testing.T) {
	const y = `
version: 1
base_models:
  "base":
    prompt:     "0.000003"
    cached:     "0.0000003"
    completion: "0.00001"
fine_tunes:
  "ft:abc123":
    derived_from: "base"
fine_tune_premium:
  policy: multiplier
  factor: "1.5"
`
	pb, err := ParsePriceBook([]byte(y))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	r, err := pb.Resolve("ft:abc123")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// base × 1.5
	if r.Prompt.String() != "0.000004500" || r.Cached.String() != "0.000000450" || r.Completion.String() != "0.000015000" {
		t.Fatalf("ft rates = %s/%s/%s, want base×1.5", r.Prompt, r.Cached, r.Completion)
	}
}

// TestLoad_FineTunePremiumMarkup (yaml-fine-tune-premium-markup): an ft: id inherits
// base + per-token markup.
func TestLoad_FineTunePremiumMarkup(t *testing.T) {
	const y = `
version: 1
base_models:
  "base":
    prompt:     "0.000003"
    cached:     "0.0000003"
    completion: "0.00001"
fine_tunes:
  "ft:abc123":
    derived_from: "base"
fine_tune_premium:
  policy: markup
  markup: "0.000001"
`
	pb, err := ParsePriceBook([]byte(y))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	r, err := pb.Resolve("ft:abc123")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// base + 0.000001 per token
	if r.Prompt.String() != "0.000004000" || r.Cached.String() != "0.000001300" || r.Completion.String() != "0.000011000" {
		t.Fatalf("ft rates = %s/%s/%s, want base+0.000001", r.Prompt, r.Cached, r.Completion)
	}
}

// TestLoad_FineTuneIdentityPremium: with no premium block (identity default), an ft:
// inherits its base exactly — the pointer-not-copy rule.
func TestLoad_FineTuneIdentityPremium(t *testing.T) {
	const y = `
version: 1
base_models:
  "base":
    prompt:     "0.000003"
    cached:     "0.0000003"
    completion: "0.00001"
fine_tunes:
  "ft:abc123":
    derived_from: "base"
`
	pb, err := ParsePriceBook([]byte(y))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	r, err := pb.Resolve("ft:abc123")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if r.Prompt.String() != "0.000003000" || r.Cached.String() != "0.000000300" || r.Completion.String() != "0.000010000" {
		t.Fatalf("identity ft rates = %s/%s/%s, want = base", r.Prompt, r.Cached, r.Completion)
	}
}

// TestLoad_FineTuneOwnRateBypassesPremium: a fine-tune with its OWN rate in the file
// ignores the premium entirely (the escape hatch).
func TestLoad_FineTuneOwnRateBypassesPremium(t *testing.T) {
	const y = `
version: 1
base_models:
  "base":
    prompt:     "0.000009"
    cached:     "0.000009"
    completion: "0.000009"
fine_tunes:
  "ft:own":
    derived_from: "base"
    rate:
      prompt:     "0.000003"
      cached:     "0.0000003"
      completion: "0.00001"
fine_tune_premium:
  policy: multiplier
  factor: "1000"
`
	pb, err := ParsePriceBook([]byte(y))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	r, err := pb.Resolve("ft:own")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if r.Prompt.String() != "0.000003000" {
		t.Fatalf("own-rate ft prompt = %s, want 0.000003000 (premium must NOT apply)", r.Prompt)
	}
}

// TestResolve_MissingPriceFailsLoud (missing-price-fails-loud-not-zero): an unknown
// model id (and an ft: id with no in-file linkage — the flagged gap) resolves to
// ErrNoPrice, NEVER a $0 rate.
func TestResolve_MissingPriceFailsLoud(t *testing.T) {
	const y = `
version: 1
base_models:
  "base":
    prompt:     "0.000003"
    cached:     "0"
    completion: "0"
`
	pb, err := ParsePriceBook([]byte(y))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := pb.Resolve("does-not-exist"); !errors.Is(err, ErrNoPrice) {
		t.Fatalf("unknown model: err = %v, want ErrNoPrice", err)
	}
	// An ft: id whose base linkage is NOT in the file is the flagged gap: it must
	// fail loud, never silently $0.
	if _, err := pb.Resolve("ft:not-plumbed-yet"); !errors.Is(err, ErrNoPrice) {
		t.Fatalf("unlinked ft id: err = %v, want ErrNoPrice (flagged gap must fail loud)", err)
	}
}

// TestLoad_MalformedYAMLFailsClosed (malformed-yaml-fails-closed): a bad price file —
// not YAML, wrong version, unknown key, float-shaped rate, negative rate, missing
// rate component, empty book, dangling derived_from — is an ERROR. The rater refuses
// to run rather than rate at $0 or a wrong rate.
func TestLoad_MalformedYAMLFailsClosed(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{"not-yaml", "::: this is not yaml :::"},
		{"wrong-version", "version: 2\nbase_models:\n  m: {prompt: \"1\", cached: \"1\", completion: \"1\"}"},
		{"no-version", "base_models:\n  m: {prompt: \"1\", cached: \"1\", completion: \"1\"}"},
		{"empty-base-models", "version: 1\nbase_models: {}"},
		{"unknown-key", "version: 1\nbase_models:\n  m: {prompt: \"1\", cached: \"1\", completion: \"1\"}\nbogus: 3"},
		{"typoed-rate-key", "version: 1\nbase_models:\n  m: {promt: \"1\", cached: \"1\", completion: \"1\"}"},
		{"float-exponent-rate", "version: 1\nbase_models:\n  m: {prompt: \"1e-6\", cached: \"0\", completion: \"0\"}"},
		{"negative-rate", "version: 1\nbase_models:\n  m: {prompt: \"-0.000001\", cached: \"0\", completion: \"0\"}"},
		{"missing-component", "version: 1\nbase_models:\n  m: {prompt: \"0.000001\", completion: \"0\"}"},
		{"base-with-ft-prefix", "version: 1\nbase_models:\n  \"ft:x\": {prompt: \"1\", cached: \"1\", completion: \"1\"}"},
		{"dangling-derived-from", "version: 1\nbase_models:\n  base: {prompt: \"1\", cached: \"1\", completion: \"1\"}\nfine_tunes:\n  \"ft:x\": {derived_from: \"nope\"}"},
		{"ft-no-linkage", "version: 1\nbase_models:\n  base: {prompt: \"1\", cached: \"1\", completion: \"1\"}\nfine_tunes:\n  \"ft:x\": {}"},
		{"multiplier-no-factor", "version: 1\nbase_models:\n  m: {prompt: \"1\", cached: \"1\", completion: \"1\"}\nfine_tune_premium:\n  policy: multiplier"},
		{"markup-with-factor", "version: 1\nbase_models:\n  m: {prompt: \"1\", cached: \"1\", completion: \"1\"}\nfine_tune_premium:\n  policy: markup\n  factor: \"1.5\"\n  markup: \"1\""},
		{"unknown-policy", "version: 1\nbase_models:\n  m: {prompt: \"1\", cached: \"1\", completion: \"1\"}\nfine_tune_premium:\n  policy: bogus"},
		{"negative-gpu-floor", "version: 1\nbase_models:\n  m: {prompt: \"1\", cached: \"1\", completion: \"1\"}\ngpu_floor_rates:\n  A100: \"-1\""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParsePriceBook([]byte(tc.yaml)); err == nil {
				t.Fatalf("malformed price file %q parsed cleanly; want a fail-closed error (never rate at $0/wrong)", tc.name)
			}
		})
	}
}

// TestLoad_NumericExactnessNoFloat (numeric-exactness-no-float): a sub-$1/1M price
// declared as a decimal string survives the load EXACTLY (the reason rates are
// strings in YAML, not floats).
func TestLoad_NumericExactnessNoFloat(t *testing.T) {
	const y = `
version: 1
base_models:
  m:
    prompt:     "0.000000150"
    cached:     "0"
    completion: "0.000000600"
`
	pb, err := ParsePriceBook([]byte(y))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	r, err := pb.Resolve("m")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if r.Prompt.String() != "0.000000150" || r.Completion.String() != "0.000000600" {
		t.Fatalf("exact sub-$1/1M rate drifted: %s/%s", r.Prompt, r.Completion)
	}
}

// TestLoad_GPUFloorRatesParsed: per-GPU floor rates are parsed and validated (so the
// file is complete) even though the token rater does not yet consume them.
func TestLoad_GPUFloorRatesParsed(t *testing.T) {
	const y = `
version: 1
base_models:
  m: {prompt: "0.000001", cached: "0", completion: "0"}
gpu_floor_rates:
  "A100-80GB": "0.000123000"
  "H100-80GB": "0.000456000"
`
	pb, err := ParsePriceBook([]byte(y))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := pb.gpuFloor["A100-80GB"].String(); got != "0.000123000" {
		t.Fatalf("A100 floor = %s, want 0.000123000", got)
	}
	if got := pb.gpuFloor["H100-80GB"].String(); got != "0.000456000" {
		t.Fatalf("H100 floor = %s, want 0.000456000", got)
	}
}

// TestLoadPriceBook_MissingFileFailsClosed: a missing price file is an error — the
// rater can't run without prices (never default to $0).
func TestLoadPriceBook_MissingFileFailsClosed(t *testing.T) {
	if _, err := LoadPriceBook("/no/such/prices.yaml"); err == nil {
		t.Fatal("missing price file loaded cleanly; want a fail-closed error")
	}
}

// TestLoadPriceBook_ExampleFileIsValid: the shipped example price file must parse and
// validate — it is the operator-facing contract and a broken example is a footgun.
func TestLoadPriceBook_ExampleFileIsValid(t *testing.T) {
	pb, err := LoadPriceBook("../../config/prices.example.yaml")
	if err != nil {
		t.Fatalf("example price file does not load: %v", err)
	}
	// It should price at least one concrete base model end-to-end.
	if _, err := pb.Resolve("meta-llama/Llama-3.1-8B-Instruct"); err != nil {
		t.Fatalf("example file does not price the documented base model: %v", err)
	}
}

// TestResolvedRates_PremiumAppliedOnce: the flat projection the store joins carries
// the FINAL rate (premium already applied) for both base and derived ids, and a
// fine-tune with an own rate is emitted once at its own rate (premium not applied).
func TestResolvedRates_PremiumAppliedOnce(t *testing.T) {
	pb := newTestBook(
		map[string]Rate3{"base": rate3("0.000004", "0", "0")},
		map[string]string{"ft:x": "base"},
		PolicyMultiplier, MustDec("1.5"), Dec{},
	)
	rows := pb.resolvedRates()
	got := map[string]string{}
	for _, r := range rows {
		got[r.ModelID] = r.Prompt
	}
	if len(rows) != 2 {
		t.Fatalf("projected %d rows, want 2 (base + derived)", len(rows))
	}
	if got["base"] != "0.000004000" {
		t.Fatalf("base projected prompt = %s, want 0.000004000", got["base"])
	}
	if got["ft:x"] != "0.000006000" { // 0.000004 × 1.5
		t.Fatalf("derived projected prompt = %s, want 0.000006000 (premium applied)", got["ft:x"])
	}
	// Deterministic ordering for a stable projection.
	if !strings.HasPrefix(rows[0].ModelID, "base") {
		t.Fatalf("rows not sorted by model_id: %+v", rows)
	}
}
