package admission

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/saturncloud/phoebe/internal/config"
)

func loadSettingsYAML(t *testing.T, yaml string) *config.Settings {
	t.Helper()
	path := filepath.Join(t.TempDir(), "settings.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := config.Load(path)
	if err != nil {
		t.Fatalf("load settings: %v", err)
	}
	return s
}

func TestFromSettingsWithoutStoreBuildsNoAdmitter(t *testing.T) {
	admitter, client, ok := FromSettings(loadSettingsYAML(t, "admission:\n  enabled: false\n"))
	if ok || admitter != nil || client != nil {
		t.Fatalf("FromSettings with no store = (%v, %v, %v), want (nil, nil, false)", admitter, client, ok)
	}
}

func TestFromSettingsMeteringStoreAloneBuildsContractOnlyAdmitter(t *testing.T) {
	s := loadSettingsYAML(t, "emit:\n  valkeyAddr: metering:6379\nadmission:\n  enabled: false\n  organization:\n    requestsPerWindow: 1\n    window: 1m\n")
	admitter, client, ok := FromSettings(s)
	if !ok || admitter == nil || client == nil {
		t.Fatal("no admitter built although emit.valkeyAddr names a store: contract limits would not be enforced")
	}
	t.Cleanup(func() { _ = client.Close() })
	cfg := admitter.Settings()
	if cfg.ValkeyAddr != "metering:6379" || client.Options().Addr != "metering:6379" {
		t.Fatalf("store=%q client=%q, want the metering Valkey", cfg.ValkeyAddr, client.Options().Addr)
	}
	if cfg.Organization.RequestsPerWindow != nil {
		t.Fatalf("operator organization tier survived admission.enabled=false: %+v", cfg.Organization)
	}
}

func TestFromSettingsAdmissionStoreTakesPrecedenceOverMeteringStore(t *testing.T) {
	s := loadSettingsYAML(t, "emit:\n  valkeyAddr: metering:6379\nadmission:\n  enabled: true\n  valkeyAddr: admission:6379\n")
	admitter, client, ok := FromSettings(s)
	if !ok {
		t.Fatal("no admitter built")
	}
	t.Cleanup(func() { _ = client.Close() })
	if admitter.Settings().ValkeyAddr != "admission:6379" || client.Options().Addr != "admission:6379" {
		t.Fatalf("store=%q, want admission.valkeyAddr", admitter.Settings().ValkeyAddr)
	}
}

// A binding operator tier under admission.enabled=false must not reach the
// admitter: this fails if FromSettings passes s.Admission instead of the
// EffectiveAdmission settings to New.
func TestFromSettingsAdmissionFlagOffIgnoresBindingOperatorTiers(t *testing.T) {
	mr := miniredis.RunT(t)
	s := loadSettingsYAML(t, "emit:\n  valkeyAddr: "+mr.Addr()+"\nadmission:\n  enabled: false\n"+
		"  platform:\n    requestsPerWindow: 1\n    maxActiveRequests: 1\n    window: 1m\n"+
		"  organization:\n    requestsPerWindow: 1\n    window: 1m\n")
	admitter, client, ok := FromSettings(s)
	if !ok {
		t.Fatal("no admitter built")
	}
	t.Cleanup(func() { _ = client.Close() })
	for i := 0; i < 3; i++ {
		lease, err := admitter.Admit(context.Background(), request("org-a", "m"))
		if err != nil {
			t.Fatalf("request %d: %v, want admitted (operator tiers are off under admission.enabled=false)", i+1, err)
		}
		_ = lease // held open: a maxActiveRequests=1 tier would bind on the 2nd
	}
}

// Every operator scope, the lane included, is checked before the contract
// scopes, so at an exact tie the operator scope answers.
func TestOperatorScopesPrecedeContractScopes(t *testing.T) {
	cfg := config.AdmissionSettings{
		Lanes:             map[string]config.AdmissionLane{"default": {Weight: 1}, "gold": {Weight: 1}},
		OrganizationLanes: map[string]string{"org-a": "gold"},
	}
	a, _ := testAdmitter(t, cfg)
	req := request("org-a", "m")
	req.OrganizationLimits = RateLimits{Requests: ptr64(1)}
	req.OwnerLimits = RateLimits{Requests: ptr64(1)}
	var names []string
	for _, s := range a.scopes(req) {
		names = append(names, s.Name)
	}
	want := []string{"platform", "graph", "organization", "organization_model", "lane", "contract_organization", "contract_owner"}
	if len(names) != len(want) {
		t.Fatalf("scopes=%v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("scopes=%v, want %v", names, want)
		}
	}
}

// A contract request that alone exceeds a scope's whole per-window limit can
// never be admitted; it is rejected as unsatisfiable, with no retry hint,
// instead of a retryable 429 that repeats forever.
func TestSingleRequestAboveContractWindowLimitIsUnsatisfiable(t *testing.T) {
	for _, tc := range []struct {
		name      string
		limits    RateLimits
		dimension string
	}{
		{"generated", RateLimits{GeneratedTokens: ptr64(10)}, "generated_tokens_exceeds_limit"},
		{"total prompt", RateLimits{TotalPromptTokens: ptr64(5)}, "total_prompt_tokens_exceeds_limit"},
		{"uncached prompt", RateLimits{UncachedPromptTokens: ptr64(5)}, "uncached_prompt_tokens_exceeds_limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := testAdmitter(t, config.AdmissionSettings{})
			req := request("org-a", "m") // EstimatedInputTokens 10, ReservedOutputTokens 20
			req.OrganizationLimits = tc.limits
			_, err := a.Admit(context.Background(), req)
			var rejected *Rejected
			if !errors.As(err, &rejected) || !rejected.Unsatisfiable || !rejected.Contractual ||
				rejected.Dimension != tc.dimension || rejected.RetryAfter != 0 {
				t.Fatalf("err=%#v, want unsatisfiable contract rejection %s with no retry", err, tc.dimension)
			}
		})
	}
	t.Run("explicit zero cap stays retryable", func(t *testing.T) {
		a, _ := testAdmitter(t, config.AdmissionSettings{})
		req := request("org-a", "m")
		req.OrganizationLimits = RateLimits{GeneratedTokens: ptr64(0)}
		_, err := a.Admit(context.Background(), req)
		var rejected *Rejected
		if !errors.As(err, &rejected) || rejected.Unsatisfiable || rejected.Dimension != "generated_tokens" || rejected.RetryAfter <= 0 {
			t.Fatalf("err=%#v, want a retryable generated_tokens rejection", err)
		}
	})
	t.Run("operator generated tier keeps its retryable answer", func(t *testing.T) {
		l := limits(5)
		l.GeneratedTokensPerWindow = ptr64(10)
		a, _ := testAdmitter(t, config.AdmissionSettings{Platform: l})
		_, err := a.Admit(context.Background(), request("org-a", "m"))
		var rejected *Rejected
		if !errors.As(err, &rejected) || rejected.Unsatisfiable || rejected.Contractual || rejected.RetryAfter <= 0 {
			t.Fatalf("err=%#v, want a retryable operator rejection", err)
		}
	})
}
