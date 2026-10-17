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

func TestAdmissionPolicyOverlaysPresentFieldsAndKeepsHelmValuesForAbsentOnes(t *testing.T) {
	base := helmAdmission(t)
	got, revision, err := base.WithAdmissionPolicy([]byte(`
version: 1
revision: "rev-7"
organization:
  maxActiveRequests: 6
  wakesPerWindow: 3
  window: 2m
organizationModel:
  maxPrefillTokens: 50000
`))
	if err != nil {
		t.Fatalf("policy refused: %v", err)
	}
	if revision != "rev-7" {
		t.Fatalf("revision = %q", revision)
	}
	if *got.Organization.MaxActiveRequests != 6 || *got.Organization.WakesPerWindow != 3 {
		t.Fatalf("organization overlay not applied: %+v", got.Organization)
	}
	if *got.Organization.MaxColdHolds != 4 {
		t.Fatalf("absent maxColdHolds must keep the Helm value 4, got %d", *got.Organization.MaxColdHolds)
	}
	if got.Organization.Window.String() != "2m0s" {
		t.Fatalf("window = %s", got.Organization.Window)
	}
	if *got.Graph.MaxActiveRequests != 40 || *got.OrganizationModel.MaxPrefillTokens != 50000 {
		t.Fatalf("other tiers wrong: graph %+v orgModel %+v", got.Graph, got.OrganizationModel)
	}
	// The baseline is not modified through shared pointers.
	if *base.Organization.MaxActiveRequests != 10 || base.Organization.WakesPerWindow != nil {
		t.Fatalf("baseline modified: %+v", base.Organization)
	}
}

func TestAdmissionPolicyWithNoTiersReturnsTheHelmSettings(t *testing.T) {
	base := helmAdmission(t)
	got, _, err := base.WithAdmissionPolicy([]byte("version: 1\nrevision: reset\n"))
	if err != nil {
		t.Fatalf("empty policy refused: %v", err)
	}
	if *got.Organization.MaxActiveRequests != 10 || *got.Graph.MaxActiveRequests != 40 {
		t.Fatalf("empty policy changed limits: %+v %+v", got.Organization, got.Graph)
	}
}

func TestAdmissionPolicyRefusesAnythingMalformedWhole(t *testing.T) {
	cases := map[string]string{
		"not yaml":             "version: [1",
		"no version":           "organization:\n  maxActiveRequests: 1\n",
		"unknown version":      "version: 2\n",
		"string version":       "version: \"1\"\n",
		"unknown top key":      "version: 1\nlanes: {}\n",
		"revision not string":  "version: 1\nrevision: 7\n",
		"tier not mapping":     "version: 1\norganization: 5\n",
		"unknown field":        "version: 1\norganization:\n  maxThings: 1\n",
		"null removes a cap":   "version: 1\norganization:\n  maxActiveRequests: null\n",
		"float":                "version: 1\norganization:\n  maxActiveRequests: 1.5\n",
		"string number":        "version: 1\norganization:\n  maxActiveRequests: \"5\"\n",
		"boolean":              "version: 1\norganization:\n  maxActiveRequests: true\n",
		"negative":             "version: 1\norganization:\n  maxActiveRequests: -1\n",
		"bad window":           "version: 1\norganization:\n  window: soon\n",
		"zero window":          "version: 1\norganization:\n  window: 0s\n",
		"repeated field":       "version: 1\norganization:\n  maxActiveRequests: 1\n  maxActiveRequests: 2\n",
		"repeated tier":        "version: 1\ngraph: {}\ngraph: {}\n",
		"prefills over active": "version: 1\norganization:\n  maxConcurrentPrefills: 20\n",
		"uncached over total":  "version: 1\nplatform:\n  totalPromptTokensPerWindow: 10\n  uncachedPromptTokensPerWindow: 20\n",
	}
	base := helmAdmission(t)
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			got, revision, err := base.WithAdmissionPolicy([]byte(doc))
			if err == nil {
				t.Fatalf("accepted malformed policy: %+v", got.Organization)
			}
			if revision != "" {
				t.Fatalf("refused policy returned revision %q", revision)
			}
			if *got.Organization.MaxActiveRequests != 10 {
				t.Fatalf("refused policy changed the settings: %+v", got.Organization)
			}
		})
	}
}

func TestAdmissionPolicyRevalidatesLaneShares(t *testing.T) {
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
	_, _, err := base.WithAdmissionPolicy([]byte("version: 1\nplatform:\n  maxActiveRequests: 50\n"))
	if err == nil || !strings.Contains(err.Error(), "lane shares") {
		t.Fatalf("lowering the platform cap under the lane shares must be refused, got %v", err)
	}
}

func TestAdmissionPolicyCanSetEveryLimitField(t *testing.T) {
	for _, field := range LimitFields(AdmissionLimits{}) {
		if limitField(&AdmissionLimits{}, field.Name) == nil {
			t.Fatalf("admission policy cannot set limit field %q", field.Name)
		}
	}
}
