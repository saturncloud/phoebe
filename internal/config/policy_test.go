package config

import (
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
	path, v, err := ParseAdmissionPolicyEntry("organizationModel.maxReservedKvTokens", " 50000 ")
	if err != nil || path != "organizationModel.maxReservedKvTokens" || v != 50000 {
		t.Fatalf("got %q %d %v", path, v, err)
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
			if _, _, err := ParseAdmissionPolicyEntry(kv[0], kv[1]); err == nil {
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

func TestAdmissionPolicyCanSetEveryLimitField(t *testing.T) {
	for _, field := range LimitFields(AdmissionLimits{}) {
		if _, _, err := ParseAdmissionPolicyEntry("organization."+field.Name, "1"); err != nil {
			t.Fatalf("admission policy cannot set limit field %q: %v", field.Name, err)
		}
	}
}
