package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
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

// When admission.valkeyAddr is empty the admission store falls back to the
// metering Valkey, which couples admission to the metering stream's memory
// cap. Startup must say so, naming both settings.
func TestBuildAdmissionLogsMeteringStoreFallback(t *testing.T) {
	mr := miniredis.RunT(t)
	logs := func(yaml string) string {
		var buf bytes.Buffer
		logger := &logging.Logger{Debug: log.New(io.Discard, "", 0), Info: log.New(&buf, "", 0), Warn: log.New(&buf, "", 0), Error: log.New(&buf, "", 0)}
		_, closeAdmission := buildAdmission(loadTestSettings(t, yaml), logger)
		closeAdmission()
		return buf.String()
	}
	fallback := logs("emit:\n  valkeyAddr: " + mr.Addr() + "\n")
	if !strings.Contains(fallback, "admission.valkeyAddr is empty") || !strings.Contains(fallback, "emit.valkeyAddr ("+mr.Addr()+")") {
		t.Fatalf("metering-store fallback not logged with both settings named: %q", fallback)
	}
	explicit := logs("emit:\n  valkeyAddr: " + mr.Addr() + "\nadmission:\n  valkeyAddr: " + mr.Addr() + "\n")
	if strings.Contains(explicit, "admission.valkeyAddr is empty") {
		t.Fatalf("fallback logged although admission.valkeyAddr is set: %q", explicit)
	}
}

const spendWiringGID = "a1b2c3d4e5f60718293a4b5c6d7e8f90"

// spendWiringRequest is a shared-shaped admit request carrying one group
// scope; scope carries either the rate limit or the spend cap under test.
func spendWiringRequest(scope admission.GroupScope) admission.Request {
	return admission.Request{
		Graph: "graph-a", Organization: "org-a", Owner: "owner-a", Model: "m",
		PromptBytes: 10, EstimatedInputTokens: 10, ReservedOutputTokens: 20,
		GroupScopes: []admission.GroupScope{scope},
	}
}

// TestBuildAdmissionGroupSpendCapsDatabaseURL pins the production DATABASE_URL
// switch that turns the monthly group spend cap on. This wiring is the
// feature's only production call site, and every other spend test wires the
// store by hand — so a regression here (WithGroupSpend not called on a
// successful open, or the open error made fatal) disables group spend caps on
// every interceptor, or crash-loops it, with the rest of the suite green.
func TestBuildAdmissionGroupSpendCapsDatabaseURL(t *testing.T) {
	build := func(t *testing.T) (admission.Admitter, *bytes.Buffer) {
		t.Helper()
		mr := miniredis.RunT(t)
		var buf bytes.Buffer
		logger := &logging.Logger{Debug: log.New(io.Discard, "", 0), Info: log.New(&buf, "", 0), Warn: log.New(&buf, "", 0), Error: log.New(&buf, "", 0)}
		admitter, closeAdmission := buildAdmission(loadTestSettings(t, "emit:\n  valkeyAddr: "+mr.Addr()+"\n"), logger)
		t.Cleanup(closeAdmission)
		return admitter, &buf
	}

	t.Run("unset logs NOT enforced and group rate limits still enforce", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "")
		admitter, buf := build(t)
		if admitter == nil {
			t.Fatal("no admitter with DATABASE_URL unset: contract and group rate limits would not be enforced")
		}
		if out := buf.String(); !strings.Contains(out, "group spend caps are NOT enforced") {
			t.Fatalf("startup log missing the loud NOT-enforced line:\n%s", out)
		}

		// The startup line's promise, pinned behaviorally: group rate limits
		// still enforce — a capped window rejects the second request with the
		// contractual 429 mapping.
		one := int64(1)
		rateScope := admission.GroupScope{GroupID: spendWiringGID, Limits: admission.RateLimits{Requests: &one}}
		lease, err := admitter.Admit(context.Background(), spendWiringRequest(rateScope))
		if err != nil {
			t.Fatalf("first group-rate admit: %v, want admitted", err)
		}
		_ = lease.Complete(context.Background(), 0)
		_, err = admitter.Admit(context.Background(), spendWiringRequest(rateScope))
		var rejected *admission.Rejected
		if !errors.As(err, &rejected) || !rejected.Contractual {
			t.Fatalf("second group-rate admit: %v, want a contractual rejection (group rate limits still enforce without DATABASE_URL)", err)
		}

		// ... and the spend cap does not: a spend-capped group admits
		// fail-open with no store behind it.
		cappedScope := admission.GroupScope{GroupID: spendWiringGID, SpendCap: "0"}
		if _, err := admitter.Admit(context.Background(), spendWiringRequest(cappedScope)); err != nil {
			t.Fatalf("spend-capped admit with no store: %v, want admitted (spend caps are NOT enforced without DATABASE_URL)", err)
		}
	})

	t.Run("unroutable DSN logs DISABLED and serving continues", func(t *testing.T) {
		// 127.0.0.1:1 answers with connection refused, so the 5s open budget
		// is not what bounds this test.
		t.Setenv("DATABASE_URL", "postgres://postgres:pw@127.0.0.1:1/postgres?sslmode=disable")
		admitter, buf := build(t)
		if admitter == nil {
			t.Fatal("no admitter with an unroutable DATABASE_URL: serving must continue without spend caps")
		}
		out := buf.String()
		if !strings.Contains(out, "group spend caps DISABLED") || !strings.Contains(out, "group rate limits still enforce") {
			t.Fatalf("startup log missing the loud DISABLED line naming the surviving rate limits:\n%s", out)
		}
	})
}
