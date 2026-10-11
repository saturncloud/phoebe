package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

func TestIsColdWakeable(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"404 model not found -> cold", 404, `{"message":"Model not found"}`, true},
		{"503 not-ready marker -> cold", 503, `Model X is not ready to serve requests yet. Retry.`, true},
		{"503 generic overload -> NOT cold", 503, `{"error":"server overloaded"}`, false},
		{"200 -> not cold", 200, `{"ok":true}`, false},
		{"500 -> not cold", 500, `boom`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newBufferingResponseWriter()
			b.WriteHeader(tc.status)
			_, _ = b.Write([]byte(tc.body))
			if b.isColdWakeable() != tc.want {
				t.Fatalf("isColdWakeable(status=%d)=%v want %v", tc.status, b.isColdWakeable(), tc.want)
			}
		})
	}
}

// TestGraphFromUpstreamHost pins the header-routed graph derivation: first DNS
// label, `-frontend` Service suffix stripped, port ignored; a label without the
// suffix is the k8s name itself.
func TestGraphFromUpstreamHost(t *testing.T) {
	cases := map[string]string{
		"graph-llama31-frontend.tf-shared.svc.cluster.local:8000":     "graph-llama31",
		"pd-abcde-mymodel-r123.main-namespace.svc.cluster.local:8000": "pd-abcde-mymodel-r123",
		"solo-frontend:8000": "solo",
		"bare-name":          "bare-name",
	}
	for host, want := range cases {
		if got := graphFromUpstreamHost(host); got != want {
			t.Errorf("graphFromUpstreamHost(%q) = %q, want %q", host, got, want)
		}
	}
}

func TestIsWakeable(t *testing.T) {
	if !isWakeable(identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "shared"}) {
		t.Fatal("shared route with resource id should be wakeable")
	}
	if isWakeable(identity.Identity{ResourceID: "r1", ServedModel: "m"}) {
		t.Fatal("dedicated route must NOT be wakeable even with a model binding")
	}
	if isWakeable(identity.Identity{ServedModel: "m", ServingMode: "shared"}) {
		t.Fatal("no resource id (unauthorized) must NOT be wakeable")
	}
	// An empty ServingMode is not a valid serving mode. The route gate refuses a
	// header-routed request without one (ruling #19), and the gateway registry
	// parser (gateway.parseRegistryConfigMap) rejects rows with a blank
	// serving_mode. If one ever arrives here it must not be
	// wakeable, because only the exact value "shared" is (fail closed).
	if isWakeable(identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: ""}) {
		t.Fatal("empty serving mode is invalid and must NOT be wakeable")
	}
	if isWakeable(identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "dedicated"}) {
		t.Fatal("dedicated route must NOT be wakeable")
	}
}

type fakeWaker struct {
	calls   int32
	err     error
	warmsAt int32 // after this many wake calls, the upstream goes warm
	backend *coldToWarmBackend

	mu         sync.Mutex
	lastTarget WakeTarget
}

func (f *fakeWaker) Wake(_ context.Context, target WakeTarget) error {
	n := atomic.AddInt32(&f.calls, 1)
	f.mu.Lock()
	f.lastTarget = target
	f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if f.backend != nil && n >= f.warmsAt {
		f.backend.warm.Store(true)
	}
	return nil
}

func (f *fakeWaker) last() WakeTarget {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastTarget
}

// coldToWarmBackend serves cold (404) until warm is set, then 200.
type coldToWarmBackend struct {
	warm      atomic.Bool
	requests  atomic.Int32
	successes atomic.Int32
	warmBody  string
}

func (c *coldToWarmBackend) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	c.requests.Add(1)
	if c.warm.Load() {
		c.successes.Add(1)
		w.WriteHeader(200)
		body := c.warmBody
		if body == "" {
			body = `{"served":true}`
		}
		_, _ = w.Write([]byte(body))
		return
	}
	w.WriteHeader(404)
	_, _ = w.Write([]byte(`{"message":"Model not found"}`))
}

// TestWithWakerDefaults pins the default wake budget: 300s — deliberately
// ABOVE vLLM's measured ~2.5min cold reload (a smaller default made every real
// wake time out and serve the cold response after holding the client anyway).
func TestWithWakerDefaults(t *testing.T) {
	s := New(&config.Settings{}, logging.New(logging.ERROR), nil).WithWaker(&fakeWaker{}, 0, 0)
	if s.wakeTimeout != 300*time.Second {
		t.Fatalf("default wakeTimeout = %v, want 300s (must exceed the real cold start)", s.wakeTimeout)
	}
	if s.wakeMaxTries != 3 {
		t.Fatalf("default wakeMaxTries = %d, want 3", s.wakeMaxTries)
	}
}

func testServerWithWaker(waker Waker) *Server {
	// A non-nil emitter is required: the served=true wake exits record a raw
	// reconciliation row, and these legacy tests exercise exactly those exits.
	s := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{})
	return s.WithWaker(waker, 5*time.Second, 3)
}

func TestServeWithWake_ColdThenWarm(t *testing.T) {
	backend := &coldToWarmBackend{}
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)

	waker := &fakeWaker{warmsAt: 1, backend: backend}
	s := testServerWithWaker(waker)

	req := httptest.NewRequest("POST", "http://x/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	id := identity.Identity{ResourceID: "r1", ServedModel: "m"}
	rec := httptest.NewRecorder()

	served := s.serveWithWake(rec, req, up, id, "req-1", "client-req-1", nil)
	// Cold-then-warm: serveWithWake wakes, sees warm on re-probe, returns false
	// (caller does the real forward). Waker called exactly once.
	if served {
		t.Fatalf("expected served=false (warm -> caller forwards), got true")
	}
	if got := atomic.LoadInt32(&waker.calls); got != 1 {
		t.Fatalf("waker called %d times, want 1", got)
	}
	if backend.requests.Load() != 2 || backend.successes.Load() != 1 {
		t.Fatalf("backend requests=%d successes=%d, want one cold probe plus one warm re-probe",
			backend.requests.Load(), backend.successes.Load())
	}
}

// errScaleForbidden stands in for a wake failure that never started a
// scale-up (an RBAC or API error on the DGDSA patch, a missing adapter).
var errScaleForbidden = errors.New("waker: scale DGDSA tf-shared/g-worker to 1: forbidden")

// TestServeWithWake_HeldColdOutcomes pins what a client receives after its
// cold request was held for a wake (QA 2026-10-10, uk2: a cold node needed
// ~6.5 min, the 300 s hold expired, and the client got the cold 404 with no
// Retry-After — read as "model not found" and never retried).
func TestServeWithWake_HeldColdOutcomes(t *testing.T) {
	notReady := fmt.Errorf("waker: upstream h not ready before deadline: %w: %w",
		ErrWakeNotReady, context.DeadlineExceeded)
	cases := []struct {
		name string
		// waker setup
		wakeErr  error
		warmsAt  int32 // 0 = never warms
		maxTries int
		// expectations
		wantServed     bool
		wantStatus     int
		wantRetryAfter string
		wantStarting   bool // JSON "model is starting" body
		wantColdBody   bool // upstream cold body passed through
	}{
		{
			name:    "wake timed out after scale-up -> 503 + Retry-After, not the cold 404",
			wakeErr: notReady, maxTries: 3,
			wantServed: true, wantStatus: http.StatusServiceUnavailable,
			wantRetryAfter: "30", wantStarting: true,
		},
		{
			name:    "wake ran out of retries but stayed cold -> 503 + Retry-After",
			warmsAt: 99, maxTries: 2,
			wantServed: true, wantStatus: http.StatusServiceUnavailable,
			wantRetryAfter: "30", wantStarting: true,
		},
		{
			name:    "wake failed without starting a scale-up -> cold 404 passed through",
			wakeErr: errScaleForbidden, maxTries: 3,
			wantServed: true, wantStatus: http.StatusNotFound, wantColdBody: true,
		},
		{
			name:    "bare deadline error from a waker that never said it scaled -> cold 404 passed through",
			wakeErr: context.DeadlineExceeded, maxTries: 3,
			wantServed: true, wantStatus: http.StatusNotFound, wantColdBody: true,
		},
		{
			name:    "wake completes -> caller forwards normally",
			warmsAt: 1, maxTries: 3,
			wantServed: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := &coldToWarmBackend{}
			be := httptest.NewServer(backend)
			defer be.Close()
			up, _ := url.Parse(be.URL)

			waker := &fakeWaker{err: tc.wakeErr}
			if tc.warmsAt > 0 {
				waker.warmsAt, waker.backend = tc.warmsAt, backend
			}
			em := &recordingEmitter{}
			s := New(&config.Settings{}, logging.New(logging.ERROR), em).
				WithWaker(waker, 5*time.Second, tc.maxTries)

			req := httptest.NewRequest("POST", "http://x/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
			id := identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "shared"}
			rec := httptest.NewRecorder()

			served := s.serveWithWake(rec, req, up, id, "req-1", "client-req-1", nil)
			if served != tc.wantServed {
				t.Fatalf("served = %v, want %v", served, tc.wantServed)
			}
			if !tc.wantServed {
				if n := len(em.all()); n != 0 {
					t.Fatalf("wake path emitted %d rows, want 0 (the caller's forward meters it)", n)
				}
				return
			}
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if got := rec.Header().Get("Retry-After"); got != tc.wantRetryAfter {
				t.Fatalf("Retry-After = %q, want %q", got, tc.wantRetryAfter)
			}
			if got := rec.Header().Get(requestIDHeader); got != "req-1" {
				t.Fatalf("X-Request-Id = %q, want the minted attempt id", got)
			}
			if tc.wantStarting {
				if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
					t.Fatalf("Content-Type = %q, want application/json", ct)
				}
				var body map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatalf("503 body is not JSON: %q (%v)", rec.Body.String(), err)
				}
				if want := "The model is starting from zero; retry in about 30 seconds."; body["error"] != want {
					t.Fatalf("503 body error = %q, want %q", body["error"], want)
				}
				if strings.Contains(rec.Body.String(), "Model not found") {
					t.Fatalf("503 body leaked the upstream cold body: %q", rec.Body.String())
				}
			}
			if tc.wantColdBody && !strings.Contains(rec.Body.String(), "Model not found") {
				t.Fatalf("body = %q, want the upstream cold body passed through", rec.Body.String())
			}
			events := em.waitForEvents(1, 2*time.Second)
			if len(events) != 1 {
				t.Fatalf("emitted %d rows, want exactly 1 raw reconciliation row: %+v", len(events), events)
			}
			if ev := events[0]; ev.Aborted || ev.UsageFound || ev.StatusCode != tc.wantStatus {
				t.Fatalf("row = {Aborted:%v UsageFound:%v StatusCode:%d}, want {false false %d}",
					ev.Aborted, ev.UsageFound, ev.StatusCode, tc.wantStatus)
			}
		})
	}
}

// TestNonWakeableColdPassesThrough pins that the 503 rewrite is confined to
// held wakeable requests: a cold 404 on a route that is not wakeable (a
// dedicated route, or a shared route with no waker configured) reaches the
// client unchanged, and the waker is never called.
func TestNonWakeableColdPassesThrough(t *testing.T) {
	cases := []struct {
		name        string
		servingMode string
		withWaker   bool
	}{
		{"dedicated route with a waker configured", identity.ServingModeDedicated, true},
		{"shared route with no waker configured", identity.ServingModeShared, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := &coldToWarmBackend{} // stays cold
			be := httptest.NewServer(backend)
			defer be.Close()
			up, _ := url.Parse(be.URL)

			waker := &fakeWaker{err: fmt.Errorf("x: %w", ErrWakeNotReady)}
			s := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{})
			if tc.withWaker {
				s = s.WithWaker(waker, 5*time.Second, 3)
			}

			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
				strings.NewReader(`{"model":"m","messages":[]}`))
			setUpstream(req, up)
			req.Header.Set(identity.HeaderAuthID, "auth-1")
			req.Header.Set(identity.HeaderResourceID, "r1")
			req.Header.Set(identity.HeaderServedModel, "m")
			req.Header.Set(identity.HeaderServingMode, tc.servingMode)
			s.Handler().ServeHTTP(rr, req)

			if rr.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want the cold 404 passed through", rr.Code)
			}
			if got := rr.Header().Get("Retry-After"); got != "" {
				t.Fatalf("Retry-After = %q on a non-wakeable cold 404, want none", got)
			}
			if !strings.Contains(rr.Body.String(), "Model not found") {
				t.Fatalf("body = %q, want the upstream cold body unchanged", rr.Body.String())
			}
			if got := atomic.LoadInt32(&waker.calls); got != 0 {
				t.Fatalf("waker called %d times on a non-wakeable route", got)
			}
		})
	}
}

func TestServeWithWake_WarmRequestNotServed(t *testing.T) {
	backend := &coldToWarmBackend{}
	backend.warm.Store(true)
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)

	waker := &fakeWaker{}
	s := testServerWithWaker(waker)

	req := httptest.NewRequest("POST", "http://x/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	id := identity.Identity{ResourceID: "r1", ServedModel: "m"}
	rec := httptest.NewRecorder()

	served := s.serveWithWake(rec, req, up, id, "req-1", "client-req-1", nil)
	if served {
		t.Fatal("warm request must not be served by serveWithWake (caller forwards)")
	}
	if backend.requests.Load() != 1 || backend.successes.Load() != 1 {
		t.Fatalf("backend requests=%d successes=%d, want exactly one probe", backend.requests.Load(), backend.successes.Load())
	}
	if waker.calls != 0 {
		t.Fatalf("waker called %d times for warm request", waker.calls)
	}
	// The caller re-forwards the request, so the body must be restored: the
	// probe consumed it, and serveWithWake returns with it readable.
	body, err := io.ReadAll(req.Body)
	if err != nil || string(body) != `{"model":"m"}` {
		t.Fatalf("request body not restored for the metered forward: %q, %v", body, err)
	}
}

func TestWakeErrorColdEmitsReconciliationRow(t *testing.T) {
	backend := &coldToWarmBackend{} // stays cold
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)

	em := &recordingEmitter{}
	s := New(&config.Settings{}, logging.New(logging.ERROR), em).
		WithWaker(&fakeWaker{err: errScaleForbidden}, 5*time.Second, 3)

	req := httptest.NewRequest("POST", "http://x/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	id := identity.Identity{ResourceID: "r1", ServedModel: "m"}
	rec := httptest.NewRecorder()

	served := s.serveWithWake(rec, req, up, id, "req-1", "client-req-1", nil)
	if !served {
		t.Fatal("wake error should serve the cold response (served=true)")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("client got %d, want the cold 404 (no scale-up started, so no 503)", rec.Code)
	}
	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("wake failure emitted %d billing events, want exactly 1 raw reconciliation row: %+v",
			len(events), events)
	}
	ev := events[0]
	if ev.Aborted || ev.UsageFound || ev.StatusCode != http.StatusNotFound {
		t.Fatalf("wake-failure row = {Aborted:%v UsageFound:%v StatusCode:%d}, "+
			"want {false false 404} (visible to reconciliation, charges $0)", ev.Aborted, ev.UsageFound, ev.StatusCode)
	}
	if ev.RequestID != "req-1" || ev.ResourceID != "r1" {
		t.Fatalf("wake-failure row attribution = {RequestID:%q ResourceID:%q}, want {req-1 r1}",
			ev.RequestID, ev.ResourceID)
	}
	if got := rec.Header().Get(requestIDHeader); got != "req-1" {
		t.Fatalf("wake-failure response X-Request-Id = %q, want the minted attempt id (client correlation handle)", got)
	}
}

func TestWakeExhaustedWarmFinalProbeEmitsReconciliationRow(t *testing.T) {
	backend := &coldToWarmBackend{}
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)

	// warmsAt=1: the single in-loop wake succeeds and flips the backend warm;
	// maxTries=1 ends the loop there, so the post-loop final probe sees the 200.
	em := &recordingEmitter{}
	s := New(&config.Settings{}, logging.New(logging.ERROR), em).
		WithWaker(&fakeWaker{warmsAt: 1, backend: backend}, 5*time.Second, 1)

	req := httptest.NewRequest("POST", "http://x/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	id := identity.Identity{ResourceID: "r1", ServedModel: "m"}
	rec := httptest.NewRecorder()

	// Merged #50×#51 contract (decc49b): a tries-exhausted final probe that is
	// NOT cold must NOT be served from the buffer. Serving it here would hand
	// the client a real response that the wake path never meters (a billing
	// hole) — or, for a non-cold error, an unclassified 502 settled
	// never-served zero. serveWithWake returns false so the caller's normal
	// metered forward owns the response, the metering row, and the settlement.
	served := s.serveWithWake(rec, req, up, id, "req-1", "client-req-1", nil)
	if served {
		t.Fatal("warm final probe must not be served by serveWithWake (caller forwards and meters)")
	}
	if n := len(em.all()); n != 0 {
		t.Fatalf("warm final probe emitted %d rows from the wake path, want 0 — the caller's metered forward owns the row", n)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil || string(body) != `{"model":"m"}` {
		t.Fatalf("request body not restored for the metered forward: %q, %v", body, err)
	}
}
func TestWakeColdHoldRejectionEmitsReconciliationRow(t *testing.T) {
	backend := &coldToWarmBackend{} // stays cold
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)

	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(10)
	cfg.Platform.MaxColdHolds = ptr64(1)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	admitter := admission.New(c, cfg)

	// Positive work estimate: #51's admission API validates the request's work
	// estimate against the configured floors.
	holdReq := admission.Request{Graph: "g1", Organization: "org-a", Model: "m", PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1}
	holder, err := admitter.Admit(context.Background(), holdReq)
	if err != nil {
		t.Fatalf("holder admit: %v", err)
	}
	t.Cleanup(func() { _ = holder.Complete(context.Background(), 0) })
	// Consume the single platform cold hold so the request under test is rejected.
	if err := holder.BeginColdHold(context.Background()); err != nil {
		t.Fatalf("holder begin cold hold: %v", err)
	}
	lease, err := admitter.Admit(context.Background(), holdReq)
	if err != nil {
		t.Fatalf("request admit: %v", err)
	}
	t.Cleanup(func() { _ = lease.Complete(context.Background(), 0) })

	em := &recordingEmitter{}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), em).
		WithWaker(&fakeWaker{}, 5*time.Second, 3)

	req := httptest.NewRequest("POST", "http://x/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	id := identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "shared"}
	rec := httptest.NewRecorder()

	served := s.serveWithWake(rec, req, up, id, "req-1", "client-req-1", lease)
	if !served {
		t.Fatal("cold-hold rejection should serve the admission error (served=true)")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("client got %d, want the admission 503 (platform cold-hold cap is non-contractual)", rec.Code)
	}
	if got := rec.Header().Get(requestIDHeader); got != "req-1" {
		t.Fatalf("cold-hold rejection response X-Request-Id = %q, want the minted attempt id", got)
	}
	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("cold-hold rejection emitted %d billing events, want exactly 1 raw reconciliation row: %+v",
			len(events), events)
	}
	ev := events[0]
	if ev.Aborted || ev.UsageFound || ev.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("cold-hold row = {Aborted:%v UsageFound:%v StatusCode:%d}, "+
			"want {false false 503} (visible to reconciliation, charges $0)", ev.Aborted, ev.UsageFound, ev.StatusCode)
	}
	if ev.RequestID != "req-1" || ev.ResourceID != "r1" {
		t.Fatalf("cold-hold row attribution = {RequestID:%q ResourceID:%q}, want {req-1 r1}",
			ev.RequestID, ev.ResourceID)
	}
}
func TestWakeWarmFallThroughMetersNormalRowOnly(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"model":"m","choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)

	em := &recordingEmitter{}
	s := New(&config.Settings{}, logging.New(logging.ERROR), em).
		WithWaker(&fakeWaker{}, 5*time.Second, 3)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","messages":[]}`))
	setUpstream(req, up)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	req.Header.Set(identity.HeaderServedModel, "m")
	req.Header.Set(identity.HeaderServingMode, "shared")
	s.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("warm fall-through emitted %d billing events, want exactly 1: %+v", len(events), events)
	}
	ev := events[0]
	if !ev.UsageFound || ev.Aborted {
		t.Fatalf("warm fall-through row = {UsageFound:%v Aborted:%v}, want the usage-bearing row {true false}",
			ev.UsageFound, ev.Aborted)
	}
	if ev.StatusCode != http.StatusOK || ev.PromptTokens != 1 || ev.CompletionTokens != 1 {
		t.Fatalf("warm fall-through row = {StatusCode:%d Prompt:%d Completion:%d}, want {200 1 1}",
			ev.StatusCode, ev.PromptTokens, ev.CompletionTokens)
	}
}

// TestWakeSkippedOnControlRoutes (wake-scoping negative pin): the wake path
// runs ONLY for model-bearing inference requests. A bound shared GET or HEAD
// on a cold control route (/health, /live, /v1/models) is served by the normal
// forward — the readiness sanitizer and model-list filters reduce the cold
// upstream response — and must never call the waker: a monitoring probe must
// not trigger a 0->1 scale of a cold base. Without this test, deleting
// inferenceRequestPathAllowed(routePath) from the wake condition leaves the
// whole suite green while cold control probes scale the graph.
func TestWakeSkippedOnControlRoutes(t *testing.T) {
	backend := &coldToWarmBackend{} // stays cold
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)

	em := &recordingEmitter{}
	waker := &fakeWaker{}
	s := New(&config.Settings{}, logging.New(logging.ERROR), em).
		WithWaker(waker, 5*time.Second, 3)

	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/health"},
		{http.MethodGet, "/live"},
		{http.MethodGet, "/v1/models"},
		{http.MethodHead, "/health"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			setUpstream(req, up)
			req.Header.Set(identity.HeaderAuthID, "auth-1")
			req.Header.Set(identity.HeaderResourceID, "r1")
			req.Header.Set(identity.HeaderServedModel, "m")
			req.Header.Set(identity.HeaderServingMode, "shared")
			s.Handler().ServeHTTP(rr, req)

			if got := atomic.LoadInt32(&waker.calls); got != 0 {
				t.Fatalf("control-route %s %s called the waker %d times, want 0 "+
					"(monitoring probes must not scale a cold base)", tc.method, tc.path, got)
			}
			if rr.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want the upstream cold 404 served by the normal forward", rr.Code)
			}
		})
	}

	// Positive control: the identical bound shared route DOES wake on the
	// inference POST — proves the waker is functional in this test and the
	// negative assertions above are meaningful.
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","messages":[]}`))
	setUpstream(req, up)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	req.Header.Set(identity.HeaderServedModel, "m")
	req.Header.Set(identity.HeaderServingMode, "shared")
	s.Handler().ServeHTTP(rr, req)
	if got := atomic.LoadInt32(&waker.calls); got == 0 {
		t.Fatal("inference POST on the same cold route did not call the waker — the positive control must wake")
	}
}
