package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/proxy"
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
			PrefillTokens: 10, EstimatedInputTokens: 10, ReservedOutputTokens: 20,
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

func TestBuildWakerSharedOnlyDoesNotLogAtlasUnavailable(t *testing.T) {
	var buf bytes.Buffer
	logger := &logging.Logger{
		Debug: log.New(io.Discard, "", 0),
		Info:  log.New(io.Discard, "", 0),
		Warn:  log.New(&buf, "", 0),
		Error: log.New(&buf, "", 0),
	}
	s := &config.Settings{}
	s.Wake.Enabled = true
	s.Gateway.Namespace = "shared"
	_ = buildWaker(s, logger)
	if strings.Contains(buf.String(), "Atlas actuator unavailable") {
		t.Fatalf("valid shared-only wake logged a false Atlas error: %q", buf.String())
	}
}

func TestBuildWakerConnectsDedicatedAtlasActuator(t *testing.T) {
	var wakeCalls int
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/internal/token-factory/inference-endpoints/wake":
			wakeCalls++
			if got := r.Header.Get("X-Saturn-Internal-Wake-Token"); got != "secret" {
				t.Fatalf("wake token = %q, want secret", got)
			}
			w.WriteHeader(http.StatusNoContent)
		case "/v1/models":
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"model-a"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer atlas.Close()
	atlasURL, err := url.Parse(atlas.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(internalWakeTokenEnv, "secret")

	s := &config.Settings{}
	s.Wake.Enabled = true
	s.Wake.AtlasURL = atlas.URL
	built := buildWaker(s, logging.New(logging.ERROR))
	if built == nil {
		t.Fatal("buildWaker returned nil for valid dedicated Atlas configuration")
	}
	err = built.Wake(context.Background(), proxy.WakeTarget{
		ResourceID: "dep-1", UpstreamHost: atlasURL.Host, ServedModel: "model-a",
		ServingMode: identity.ServingModeDedicated,
	})
	if err != nil {
		t.Fatalf("dedicated Wake: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("Atlas wake calls = %d, want 1", wakeCalls)
	}
}

func TestBuildWakerLogsMissingDedicatedToken(t *testing.T) {
	t.Setenv(internalWakeTokenEnv, "")
	var buf bytes.Buffer
	logger := &logging.Logger{
		Debug: log.New(io.Discard, "", 0),
		Info:  log.New(io.Discard, "", 0),
		Warn:  log.New(&buf, "", 0),
		Error: log.New(&buf, "", 0),
	}
	s := &config.Settings{}
	s.Wake.Enabled = true
	s.Wake.AtlasURL = "http://atlas.internal"
	if built := buildWaker(s, logger); built != nil {
		t.Fatalf("buildWaker returned %T without %s, want nil", built, internalWakeTokenEnv)
	}
	if got := buf.String(); !strings.Contains(got, "Atlas actuator unavailable") || !strings.Contains(got, "internal wake token is empty") {
		t.Fatalf("missing-token startup log = %q", got)
	}
}
