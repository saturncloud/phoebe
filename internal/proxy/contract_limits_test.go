package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

// These tests reproduce the 2026-10-06 QA finding "Contract rate limits are
// stamped on the envelope but never enforced": an Atlas UsageLimit of 30
// requests/minute on the trusted envelope must admit 30 requests in the
// window and answer the 31st with 429 + Retry-After, whatever the operator
// capacity tiers are — absent, an empty `organization: {}`, or admission.enabled
// false (ruling R12 clarification: there is no enablement gate for contract
// limits). The admitter is built exactly as cmd/interceptor does, from the
// loaded settings through Settings.EffectiveAdmission.

const contractBurst = 35

var allScopedLimitHeaders = []string{
	identity.HeaderOrgRateLimitRequests,
	identity.HeaderOrgRateLimitTotalPromptTokens,
	identity.HeaderOrgRateLimitUncachedPromptTokens,
	identity.HeaderOrgRateLimitGeneratedTokens,
	identity.HeaderOwnerRateLimitRequests,
	identity.HeaderOwnerRateLimitTotalPromptTokens,
	identity.HeaderOwnerRateLimitUncachedPromptTokens,
	identity.HeaderOwnerRateLimitGeneratedTokens,
}

// frozenMiniredis returns a store whose TIME is pinned mid-minute, so a burst
// can never straddle a fixed-window boundary and flake.
func frozenMiniredis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	mr := miniredis.RunT(t)
	mr.SetTime(time.Date(2026, 10, 6, 17, 10, 15, 0, time.UTC))
	return mr
}

// loadSettings loads YAML through the production loader, so the test sees the
// same defaults and parse as a deployed interceptor.
func loadSettings(t *testing.T, yaml string) *config.Settings {
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

// contractTestServer wires a gateway Server the way cmd/interceptor does:
// the admitter exists iff Settings.EffectiveAdmission reports a store.
func contractTestServer(t *testing.T, settings *config.Settings, backend *url.URL) *Server {
	t.Helper()
	s := New(settings, logging.New(logging.ERROR), &recordingEmitter{}).WithGateway(&mapResolver{m: map[[2]string]gateway.Resolution{
		{"org-1", "model-a"}: {ResourceID: "resource-a", BaseModel: "model-a", ServingMode: "shared", GraphK8sName: "graph-a"},
	}}, "tf-shared", 8000)
	s.gateway.upstreamFor = func(string, int) string { return backend.Host }
	if cfg, ok := settings.EffectiveAdmission(); ok {
		s = s.WithAdmitter(admission.New(admission.NewValkeyClient(cfg.ValkeyAddr), cfg))
	}
	return s
}

func countingBackend(t *testing.T) (*url.URL, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	t.Cleanup(backend.Close)
	up, _ := url.Parse(backend.URL)
	return up, &hits
}

// contractGatewayRequest is a gateway request whose envelope carries the
// owner-id anchor and exactly the given limit headers; every other limit
// header is ABSENT (R4: unlimited).
func contractGatewayRequest(limits map[string]string) *http.Request {
	req := gatewayRequest("org-1", `{"model":"model-a","max_tokens":20}`)
	for _, header := range allScopedLimitHeaders {
		req.Header.Del(header)
	}
	for header, value := range limits {
		req.Header.Set(header, value)
	}
	return req
}

type burstResult struct {
	codes       []int
	retryAfters []string
	bodies      []string
}

func runBurst(s *Server, n int, mk func() *http.Request) burstResult {
	var out burstResult
	for i := 0; i < n; i++ {
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, mk())
		out.codes = append(out.codes, rr.Code)
		out.retryAfters = append(out.retryAfters, rr.Header().Get("Retry-After"))
		out.bodies = append(out.bodies, strings.TrimSpace(rr.Body.String()))
	}
	return out
}

// assertCutoff asserts the first `admitted` requests got 200 and every later
// one got `rejectStatus` with a Retry-After inside the 1-minute window.
func assertCutoff(t *testing.T, got burstResult, admitted, rejectStatus int, rejectBody string) {
	t.Helper()
	for i, code := range got.codes {
		if i < admitted {
			if code != http.StatusOK {
				t.Fatalf("request %d: status=%d body=%q, want 200 (within the limit of %d)", i+1, code, got.bodies[i], admitted)
			}
			continue
		}
		if code != rejectStatus {
			t.Fatalf("request %d: status=%d body=%q, want %d past the limit of %d (codes=%v)", i+1, code, got.bodies[i], rejectStatus, admitted, got.codes)
		}
		if got.bodies[i] != rejectBody {
			t.Fatalf("request %d: body=%q, want %q", i+1, got.bodies[i], rejectBody)
		}
		retry, err := strconv.Atoi(got.retryAfters[i])
		if err != nil || retry < 1 || retry > 60 {
			t.Fatalf("request %d: Retry-After=%q, want 1..60 seconds", i+1, got.retryAfters[i])
		}
	}
}

const (
	contractRejectBody = "shared inference rate limit exceeded"
	capacityRejectBody = "shared inference capacity is temporarily unavailable"
)

func TestContractOrgLimitEnforcedRegardlessOfOperatorTiers(t *testing.T) {
	for _, tc := range []struct {
		name string
		yaml func(addr string) string
	}{
		{
			// QA case (a): no admission block at all, as the chart renders
			// admission.enabled=false. The store is the metering Valkey.
			name: "admission disabled, store from emit.valkeyAddr",
			yaml: func(addr string) string { return "emit:\n  valkeyAddr: " + addr + "\n" },
		},
		{
			// QA case (b): admission enabled with every operator tier absent.
			name: "admission enabled, all operator tiers absent",
			yaml: func(addr string) string {
				return "emit:\n  valkeyAddr: " + addr + "\nadmission:\n  enabled: true\n  valkeyAddr: " + addr + "\n"
			},
		},
		{
			// QA case (c): admission enabled with an empty organization tier.
			name: "admission enabled, empty organization tier",
			yaml: func(addr string) string {
				return "emit:\n  valkeyAddr: " + addr + "\nadmission:\n  enabled: true\n  valkeyAddr: " + addr + "\n  organization: {}\n"
			},
		},
		{
			// The chart's R5 shape: enabled with every operator limit null.
			name: "admission enabled, chart-null operator tiers",
			yaml: func(addr string) string {
				return "admission:\n  enabled: true\n  valkeyAddr: " + addr +
					"\n  platform:\n    requestsPerWindow: null\n    maxActiveRequests: null\n    window: 1m\n  organization:\n    requestsPerWindow: null\n    window: 1m\n"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mr := frozenMiniredis(t)
			up, hits := countingBackend(t)
			s := contractTestServer(t, loadSettings(t, tc.yaml(mr.Addr())), up)
			if s.admitter == nil {
				t.Fatal("no admitter built although an admission store is configured: contract limits cannot be enforced")
			}
			got := runBurst(s, contractBurst, func() *http.Request {
				return contractGatewayRequest(map[string]string{identity.HeaderOrgRateLimitRequests: "30"})
			})
			assertCutoff(t, got, 30, http.StatusTooManyRequests, contractRejectBody)
			if hits.Load() != 30 {
				t.Fatalf("upstream hits=%d, want 30 (rejected requests must not reach the engine)", hits.Load())
			}
		})
	}
}

func TestContractOwnerLimitEnforcedRegardlessOfOperatorTiers(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run("admission.enabled="+strconv.FormatBool(enabled), func(t *testing.T) {
			mr := frozenMiniredis(t)
			up, hits := countingBackend(t)
			yaml := "emit:\n  valkeyAddr: " + mr.Addr() + "\n"
			if enabled {
				yaml += "admission:\n  enabled: true\n  valkeyAddr: " + mr.Addr() + "\n"
			}
			s := contractTestServer(t, loadSettings(t, yaml), up)
			got := runBurst(s, contractBurst, func() *http.Request {
				return contractGatewayRequest(map[string]string{identity.HeaderOwnerRateLimitRequests: "30"})
			})
			assertCutoff(t, got, 30, http.StatusTooManyRequests, contractRejectBody)
			if hits.Load() != 30 {
				t.Fatalf("upstream hits=%d, want 30", hits.Load())
			}
		})
	}
}

// The header-routed (non-gateway) shared path engages the same contract
// scopes under admission.enabled=false when its route carries the envelope.
func TestContractLimitEnforcedOnHeaderRoutedSharedPathWithAdmissionFlagOff(t *testing.T) {
	mr := frozenMiniredis(t)
	up, hits := countingBackend(t)
	s := contractTestServer(t, loadSettings(t, "emit:\n  valkeyAddr: "+mr.Addr()+"\n"), up)
	got := runBurst(s, 5, func() *http.Request {
		req := sharedRequest(up)
		req.Header.Set(identity.HeaderOrgRateLimitRequests, "3")
		return req
	})
	assertCutoff(t, got, 3, http.StatusTooManyRequests, contractRejectBody)
	if hits.Load() != 3 {
		t.Fatalf("upstream hits=%d, want 3", hits.Load())
	}
}

// Operator capacity scopes keep their documented 503 answer.
func TestOperatorCapacityLimitStillAnswers503(t *testing.T) {
	mr := frozenMiniredis(t)
	up, _ := countingBackend(t)
	s := contractTestServer(t, loadSettings(t, "admission:\n  enabled: true\n  valkeyAddr: "+mr.Addr()+
		"\n  organization:\n    requestsPerWindow: 30\n    window: 1m\n"), up)
	got := runBurst(s, contractBurst, func() *http.Request { return contractGatewayRequest(nil) })
	assertCutoff(t, got, 30, http.StatusServiceUnavailable, capacityRejectBody)
}

// Operator scopes and contract scopes are checked in ONE transaction, operator
// scopes first (platform, graph, organization, organizationModel, then
// contract_organization, contract_owner, then the lane). Whichever limit is
// reached first decides the answer; at an exact tie the operator scope is
// checked first and the answer is 503.
func TestContractAndOperatorLimitPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name               string
		operator, contract string
		admitted, status   int
		body               string
	}{
		{name: "contract tighter", operator: "30", contract: "20", admitted: 20, status: http.StatusTooManyRequests, body: contractRejectBody},
		{name: "operator tighter", operator: "20", contract: "30", admitted: 20, status: http.StatusServiceUnavailable, body: capacityRejectBody},
		{name: "tie answers operator 503", operator: "25", contract: "25", admitted: 25, status: http.StatusServiceUnavailable, body: capacityRejectBody},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mr := frozenMiniredis(t)
			up, _ := countingBackend(t)
			s := contractTestServer(t, loadSettings(t, "admission:\n  enabled: true\n  valkeyAddr: "+mr.Addr()+
				"\n  organization:\n    requestsPerWindow: "+tc.operator+"\n    window: 1m\n"), up)
			got := runBurst(s, contractBurst, func() *http.Request {
				return contractGatewayRequest(map[string]string{identity.HeaderOrgRateLimitRequests: tc.contract})
			})
			assertCutoff(t, got, tc.admitted, tc.status, tc.body)
		})
	}
}

// R4: an absent limit header is unlimited, with the flag on or off.
func TestAbsentContractLimitHeadersAreUnlimited(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run("admission.enabled="+strconv.FormatBool(enabled), func(t *testing.T) {
			mr := frozenMiniredis(t)
			up, hits := countingBackend(t)
			yaml := "emit:\n  valkeyAddr: " + mr.Addr() + "\n"
			if enabled {
				yaml += "admission:\n  enabled: true\n  valkeyAddr: " + mr.Addr() + "\n"
			}
			s := contractTestServer(t, loadSettings(t, yaml), up)
			got := runBurst(s, contractBurst, func() *http.Request { return contractGatewayRequest(nil) })
			assertCutoff(t, got, contractBurst, 0, "")
			if hits.Load() != contractBurst {
				t.Fatalf("upstream hits=%d, want %d", hits.Load(), contractBurst)
			}
		})
	}
}

// R4: an explicit 0 is a zero cap that blocks the scope, also with the flag off.
func TestExplicitZeroContractLimitBlocksWithAdmissionFlagOff(t *testing.T) {
	for _, header := range []string{identity.HeaderOrgRateLimitRequests, identity.HeaderOwnerRateLimitRequests} {
		t.Run(header, func(t *testing.T) {
			mr := frozenMiniredis(t)
			up, hits := countingBackend(t)
			s := contractTestServer(t, loadSettings(t, "emit:\n  valkeyAddr: "+mr.Addr()+"\n"), up)
			got := runBurst(s, 3, func() *http.Request { return contractGatewayRequest(map[string]string{header: "0"}) })
			assertCutoff(t, got, 0, http.StatusTooManyRequests, contractRejectBody)
			if hits.Load() != 0 {
				t.Fatalf("upstream hits=%d, want 0 at a zero cap", hits.Load())
			}
		})
	}
}

// Under admission.enabled=false a shared request with NO envelope at all (a
// historical per-resource route) is still served, as before; under
// admission.enabled=true it fails closed. The flag keeps exactly this meaning.
func TestEnvelopeLessSharedRequestFollowsAdmissionFlag(t *testing.T) {
	for _, tc := range []struct {
		enabled bool
		status  int
	}{
		{enabled: false, status: http.StatusOK},
		{enabled: true, status: http.StatusServiceUnavailable},
	} {
		t.Run("admission.enabled="+strconv.FormatBool(tc.enabled), func(t *testing.T) {
			mr := frozenMiniredis(t)
			up, _ := countingBackend(t)
			yaml := "emit:\n  valkeyAddr: " + mr.Addr() + "\n"
			if tc.enabled {
				yaml += "admission:\n  enabled: true\n  valkeyAddr: " + mr.Addr() + "\n"
			}
			s := contractTestServer(t, loadSettings(t, yaml), up)
			req := sharedRequest(up)
			req.Header.Del(identity.HeaderOwnerID)
			for _, header := range allScopedLimitHeaders {
				req.Header.Del(header)
			}
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, req)
			if rr.Code != tc.status {
				t.Fatalf("status=%d body=%q, want %d", rr.Code, rr.Body.String(), tc.status)
			}
		})
	}
}

// A partial envelope (a limit header without its owner-id anchor) fails
// closed even with admission.enabled=false: a stamped limit is never dropped.
func TestPartialEnvelopeFailsClosedWithAdmissionFlagOff(t *testing.T) {
	mr := frozenMiniredis(t)
	up, hits := countingBackend(t)
	s := contractTestServer(t, loadSettings(t, "emit:\n  valkeyAddr: "+mr.Addr()+"\n"), up)
	req := contractGatewayRequest(map[string]string{identity.HeaderOrgRateLimitRequests: "30"})
	req.Header.Del(identity.HeaderOwnerID)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable || hits.Load() != 0 {
		t.Fatalf("status=%d hits=%d, want 503 before upstream", rr.Code, hits.Load())
	}
}
