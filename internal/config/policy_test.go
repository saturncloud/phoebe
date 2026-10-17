package config

import (
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/saturncloud/phoebe/internal/testutil"
)

func helmAdmission(t *testing.T) AdmissionSettings {
	t.Helper()
	a := AdmissionSettings{
		Enabled:    true,
		ValkeyAddr: "valkey:6379",
		Graph:      AdmissionLimits{MaxActiveRequests: testutil.Ptr[int64](40)},
		Organization: AdmissionLimits{
			MaxActiveRequests: testutil.Ptr[int64](10),
			MaxColdHolds:      testutil.Ptr[int64](4),
		},
	}
	if err := a.parse(true); err != nil {
		t.Fatalf("helm settings: %v", err)
	}
	return a
}

func TestParseAdmissionPolicyEntryAcceptsTierFieldKeysWithPositiveIntegers(t *testing.T) {
	v, err := ParseAdmissionPolicyEntry("organizationModel.maxReservedKvTokens", " 50000 ")
	if err != nil || v != 50000 {
		t.Fatalf("got %d %v", v, err)
	}
}

func TestParseAdmissionPolicyEntryRefusesMalformedEntries(t *testing.T) {
	cases := map[string][2]string{
		"no dot":        {"maxActiveRequests", "1"},
		"unknown tier":  {"lanes.maxActiveRequests", "1"},
		"unknown field": {"organization.maxThings", "1"},
		"window":        {"organization.window", "2m"},
		"zero":          {"organization.maxActiveRequests", "0"},
		"negative":      {"organization.maxActiveRequests", "-1"},
		"float":         {"organization.maxActiveRequests", "1.5"},
		"empty":         {"organization.maxActiveRequests", ""},
		"word":          {"organization.maxActiveRequests", "unlimited"},
		"overflow":      {"organization.maxActiveRequests", "99999999999999999999"},
	}
	for name, kv := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseAdmissionPolicyEntry(kv[0], kv[1]); err == nil {
				t.Fatalf("accepted %s=%q", kv[0], kv[1])
			}
		})
	}
}

func TestWithLimitOverridesReplacesPresentFieldsAndKeepsHelmValuesForAbsentOnes(t *testing.T) {
	base := helmAdmission(t)
	got, err := base.WithLimitOverrides(map[string]int64{
		"organization.maxActiveRequests":     6,
		"graph.wakesPerWindow":               3,
		"organizationModel.maxPrefillTokens": 50000,
	})
	if err != nil {
		t.Fatalf("refused: %v", err)
	}
	if *got.Organization.MaxActiveRequests != 6 || *got.Graph.WakesPerWindow != 3 || *got.OrganizationModel.MaxPrefillTokens != 50000 {
		t.Fatalf("overrides not applied: %+v %+v %+v", got.Organization, got.Graph, got.OrganizationModel)
	}
	if *got.Organization.MaxColdHolds != 4 || *got.Graph.MaxActiveRequests != 40 {
		t.Fatalf("absent fields must keep the Helm values: %+v %+v", got.Organization, got.Graph)
	}
	if *base.Organization.MaxActiveRequests != 10 || base.Graph.WakesPerWindow != nil {
		t.Fatalf("baseline modified through a shared pointer: %+v %+v", base.Organization, base.Graph)
	}
}

func TestWithLimitOverridesWithNothingReturnsTheHelmSettings(t *testing.T) {
	base := helmAdmission(t)
	got, err := base.WithLimitOverrides(nil)
	if err != nil || *got.Organization.MaxActiveRequests != 10 || *got.Graph.MaxActiveRequests != 40 {
		t.Fatalf("got %+v %+v %v", got.Organization, got.Graph, err)
	}
}

func TestWithLimitOverridesRefusesACombinationThatFailsCrossFieldValidation(t *testing.T) {
	base := helmAdmission(t)
	// Valid alone, but the tier's prefill cap would exceed its active cap.
	got, err := base.WithLimitOverrides(map[string]int64{"organization.maxConcurrentPrefills": 20})
	if err == nil {
		t.Fatalf("accepted maxConcurrentPrefills 20 above maxActiveRequests 10")
	}
	if got.Organization.MaxConcurrentPrefills != nil {
		t.Fatalf("a refused update changed the settings: %+v", got.Organization)
	}
}

func TestWithLimitOverridesRevalidatesLaneShares(t *testing.T) {
	base := AdmissionSettings{
		Enabled:    true,
		ValkeyAddr: "valkey:6379",
		Platform:   AdmissionLimits{MaxActiveRequests: testutil.Ptr[int64](100)},
		Lanes: map[string]AdmissionLane{
			"default": {Weight: 1, Limits: AdmissionLimits{MaxActiveRequests: testutil.Ptr[int64](80)}},
		},
	}
	if err := base.parse(true); err != nil {
		t.Fatalf("helm settings: %v", err)
	}
	_, err := base.WithLimitOverrides(map[string]int64{"platform.maxActiveRequests": 50})
	if err == nil || !strings.Contains(err.Error(), "lane shares") {
		t.Fatalf("lowering the platform cap under the lane shares must be refused, got %v", err)
	}
}

func TestLimitFieldCoversExactlyTheLimitFieldsKeys(t *testing.T) {
	// Every tier can set every LimitFields key.
	for _, tier := range admissionPolicyTiers {
		for _, field := range LimitFields(AdmissionLimits{}) {
			if _, err := ParseAdmissionPolicyEntry(tier.name+"."+field.Name, "1"); err != nil {
				t.Fatalf("admission policy cannot set %s.%s: %v", tier.name, field.Name, err)
			}
		}
	}
	// limitField points at the same struct field LimitFields names.
	for i, field := range LimitFields(AdmissionLimits{}) {
		var l AdmissionLimits
		target := limitField(&l, field.Name)
		if target == nil {
			t.Fatalf("limitField does not resolve LimitFields key %q", field.Name)
		}
		*target = testutil.Ptr[int64](1)
		if got := LimitFields(l)[i]; got.Name != field.Name || got.Value == nil {
			t.Fatalf("limitField(%q) set a different field than LimitFields reports", field.Name)
		}
	}
	// The other direction: every *int64 field of AdmissionLimits is either a
	// LimitFields key (limitField resolves it) or a removed key that the
	// policy must refuse (limitField returns nil). A new struct field that is
	// in neither list fails here.
	names := map[string]bool{}
	for _, field := range LimitFields(AdmissionLimits{}) {
		names[field.Name] = true
	}
	removed := map[string]bool{"maxPromptBytes": true}
	int64Ptr := reflect.TypeOf((*int64)(nil))
	typ := reflect.TypeOf(AdmissionLimits{})
	seen := map[string]bool{}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.Type != int64Ptr {
			continue
		}
		tag, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		switch {
		case names[tag]:
			if limitField(&AdmissionLimits{}, tag) == nil {
				t.Fatalf("limitField misses limit field %s (yaml %q)", f.Name, tag)
			}
			seen[tag] = true
		case removed[tag]:
			if limitField(&AdmissionLimits{}, tag) != nil {
				t.Fatalf("limitField resolves removed key %q; the policy must refuse it", tag)
			}
		default:
			t.Fatalf("AdmissionLimits field %s (yaml %q) is neither a LimitFields key nor a known removed key", f.Name, tag)
		}
	}
	if len(seen) != len(names) {
		t.Fatalf("LimitFields names %v but the struct's *int64 limit tags are %v", names, seen)
	}
}

func TestWithLimitOverridesRefusesUnknownKeysWithoutPanicking(t *testing.T) {
	cases := map[string]string{
		"unknown tier, known field": "lanes.maxActiveRequests",
		"known tier, unknown field": "organization.maxThings",
		"no dot":                    "organization",
	}
	for reason, key := range cases {
		t.Run(reason, func(t *testing.T) {
			base := helmAdmission(t)
			got, err := base.WithLimitOverrides(map[string]int64{key: 1})
			if err == nil || !strings.Contains(err.Error(), "unknown admission policy key") {
				t.Fatalf("WithLimitOverrides(%q) = %v, want an unknown admission policy key error", key, err)
			}
			if !reflect.DeepEqual(got, base) {
				t.Fatalf("a refused key changed the settings: %+v", got)
			}
		})
	}
}

func TestWithLimitOverridesRefusesAnUnknownTierWithAKnownField(t *testing.T) {
	base := helmAdmission(t)
	got, err := base.WithLimitOverrides(map[string]int64{"lanes.maxActiveRequests": 5})
	if err == nil || !strings.Contains(err.Error(), "unknown admission policy key") {
		t.Fatalf("got %v, want an unknown admission policy key error", err)
	}
	if !reflect.DeepEqual(got, base) {
		t.Fatalf("a refused key changed the settings: %+v", got)
	}
}

// cloneLanes returns s with its Lanes and OrganizationLanes maps copied, so a
// full parse (which writes to Lanes) cannot touch the maps of the original.
func cloneLanes(s AdmissionSettings) AdmissionSettings {
	s.Lanes = maps.Clone(s.Lanes)
	s.OrganizationLanes = maps.Clone(s.OrganizationLanes)
	return s
}

func TestWithLimitOverridesOutputPassesTheFullHelmValidation(t *testing.T) {
	base := AdmissionSettings{
		Enabled:    true,
		ValkeyAddr: "valkey:6379",
		Platform: AdmissionLimits{
			MaxActiveRequests:   testutil.Ptr[int64](100),
			MaxReservedKVTokens: testutil.Ptr[int64](1_000_000),
		},
		Organization: AdmissionLimits{MaxActiveRequests: testutil.Ptr[int64](10)},
		Lanes: map[string]AdmissionLane{
			"default": {Weight: 1, Limits: AdmissionLimits{
				MaxActiveRequests:   testutil.Ptr[int64](60),
				MaxReservedKVTokens: testutil.Ptr[int64](600_000),
			}},
			"premium": {Weight: 1, Limits: AdmissionLimits{
				MaxActiveRequests:   testutil.Ptr[int64](40),
				MaxReservedKVTokens: testutil.Ptr[int64](400_000),
			}},
		},
		OrganizationLanes: map[string]string{"org-1": "premium"},
	}
	if err := base.parse(true); err != nil {
		t.Fatalf("helm settings: %v", err)
	}
	overrides := []map[string]int64{
		nil,
		{"platform.maxActiveRequests": 120},
		{"platform.maxReservedKvTokens": 2_000_000, "graph.maxActiveRequests": 30},
		{"organization.maxActiveRequests": 5, "organization.maxConcurrentPrefills": 5},
		{"organizationModel.requestsPerWindow": 50, "graph.wakesPerWindow": 3},
	}
	for i, values := range overrides {
		got, err := base.WithLimitOverrides(values)
		if err != nil {
			t.Fatalf("override %d %v refused: %v", i, values, err)
		}
		full := cloneLanes(got)
		if err := full.parse(true); err != nil {
			t.Fatalf("override %d %v passed WithLimitOverrides but fails the Helm validation: %v", i, values, err)
		}
	}
}
