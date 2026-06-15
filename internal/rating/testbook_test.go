package rating

import "time"

// Test-only constructors for the production PriceBook. The production loader builds
// a book from YAML; these let tests assemble one directly (and via YAML round-trips
// in pricebook_test.go) without a file. They mirror the loader's invariants (own
// rate wins; a fine-tune's derived_from must point at a known base).

// mustTime parses an RFC3339 instant or panics (test fixtures).
func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// newTestBook builds a PriceBook from base rates, fine-tune→base linkages, and a
// premium policy. base maps model_id → {prompt,cached,completion} decimal strings;
// ftDerivedFrom maps fine-tune model_id → base model_id.
func newTestBook(base map[string]Rate3, ftDerivedFrom map[string]string, fn PolicyFunc, factor, markup Dec) *PriceBook {
	pb := &PriceBook{
		base:         map[string]Rate3{},
		derivedFrom:  map[string]string{},
		gpuFloor:     map[string]Dec{},
		policyFn:     fn,
		policyFactor: factor,
		policyMarkup: markup,
	}
	for id, r := range base {
		pb.base[id] = r
	}
	for ft, b := range ftDerivedFrom {
		pb.derivedFrom[ft] = b
	}
	if pb.policyFn == "" {
		pb.policyFn = PolicyIdentity
	}
	return pb
}

// rate3 builds a Rate3 from decimal strings (terse fixtures).
func rate3(prompt, cached, completion string) Rate3 {
	return Rate3{Prompt: MustDec(prompt), Cached: MustDec(cached), Completion: MustDec(completion)}
}

// bookM is a one-base-model book ("m": prompt 0.000003 / cached 0.0000003 /
// completion 0.00001), identity premium.
func bookM() *PriceBook {
	return newTestBook(
		map[string]Rate3{"m": rate3("0.000003", "0.0000003", "0.00001")},
		nil, PolicyIdentity, Dec{}, Dec{},
	)
}
