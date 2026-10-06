package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/logging"
)

func loadTestSettings(t *testing.T, yaml string) *config.Settings {
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

// The 2026-10-06 QA fix: with admission.enabled=false and only the metering
// Valkey configured, the interceptor still builds an admitter, so contract
// limits are enforced.
func TestBuildAdmissionContractOnlyFromMeteringStore(t *testing.T) {
	mr := miniredis.RunT(t)
	admitter, closeAdmission := buildAdmission(loadTestSettings(t, "emit:\n  valkeyAddr: "+mr.Addr()+"\n"), logging.New(logging.ERROR))
	defer closeAdmission()
	if admitter == nil {
		t.Fatal("no admitter with admission.enabled=false and emit.valkeyAddr set: contract limits would not be enforced")
	}
}

func TestBuildAdmissionWithoutStoreReturnsNil(t *testing.T) {
	admitter, closeAdmission := buildAdmission(loadTestSettings(t, "admission:\n  enabled: false\n"), logging.New(logging.ERROR))
	defer closeAdmission()
	if admitter != nil {
		t.Fatalf("admitter=%v with no store configured, want nil", admitter)
	}
}

func TestBuildAdmissionEnabledFalseDropsOperatorTiers(t *testing.T) {
	mr := miniredis.RunT(t)
	s := loadTestSettings(t, "emit:\n  valkeyAddr: "+mr.Addr()+"\nadmission:\n  enabled: false\n"+
		"  platform:\n    requestsPerWindow: 1\n    maxActiveRequests: 1\n    window: 1m\n")
	admitter, closeAdmission := buildAdmission(s, logging.New(logging.ERROR))
	defer closeAdmission()
	if admitter == nil {
		t.Fatal("no admitter built")
	}
	for i := 0; i < 3; i++ {
		_, err := admitter.Admit(context.Background(), admission.Request{
			Graph: "graph-a", Organization: "org-a", Owner: "owner-a", Model: "m",
			PromptBytes: 10, EstimatedInputTokens: 10, ReservedOutputTokens: 20,
		})
		if err != nil {
			t.Fatalf("request %d: %v, want admitted (operator tiers are off under admission.enabled=false)", i+1, err)
		}
	}
}
