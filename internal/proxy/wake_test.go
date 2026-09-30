package proxy

import (
	"context"
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
// label, `-frontend` Service suffix stripped, port ignored; a label without
// the suffix is the k8s name itself.
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
	// An empty ServingMode is not a valid serving mode. identity.FromRequest
	// never produces it: an absent header resolves to ServingModeDedicated, and
	// the gateway registry parser (gateway.parseRegistryConfigMap) rejects rows
	// with a blank serving_mode. If one ever arrives here it must not be
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
type coldToWarmBackend struct{ warm atomic.Bool }

func (c *coldToWarmBackend) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if c.warm.Load() {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"served":true}`))
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
}

func TestServeWithWake_WakeErrorReturnsCold(t *testing.T) {
	backend := &coldToWarmBackend{} // stays cold
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)

	waker := &fakeWaker{err: context.DeadlineExceeded}
	s := testServerWithWaker(waker)

	req := httptest.NewRequest("POST", "http://x/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	id := identity.Identity{ResourceID: "r1", ServedModel: "m"}
	rec := httptest.NewRecorder()

	served := s.serveWithWake(rec, req, up, id, "req-1", "client-req-1", nil)
	if !served {
		t.Fatal("wake error should serve the cold response (served=true)")
	}
	if rec.Code != 404 {
		t.Fatalf("expected the cold 404 flushed to client, got %d", rec.Code)
	}
}

// TestWakeErrorColdEmitsReconciliationRow (merged billing contract #48): the
// waker failed (deadline), so serveWithWake serves the buffered cold response
// and returns served=true — but the probe above WAS a real forwarded attempt of
// the customer's request, and the normal metered forward never runs. Exactly
// one raw reconciliation row must be recorded: Aborted=false, UsageFound=false,
// at the status actually written to the client (the cold 404), charging $0.
func TestWakeErrorColdEmitsReconciliationRow(t *testing.T) {
	backend := &coldToWarmBackend{} // stays cold
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)

	em := &recordingEmitter{}
	s := New(&config.Settings{}, logging.New(logging.ERROR), em).
		WithWaker(&fakeWaker{err: context.DeadlineExceeded}, 5*time.Second, 3)

	req := httptest.NewRequest("POST", "http://x/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	id := identity.Identity{ResourceID: "r1", ServedModel: "m"}
	rec := httptest.NewRecorder()

	served := s.serveWithWake(rec, req, up, id, "req-1", "client-req-1", nil)
	if !served {
		t.Fatal("wake error should serve the cold response (served=true)")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("client got %d, want the cold 404", rec.Code)
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

// TestWakeExhaustedWarmFinalProbeEmitsReconciliationRow (merged billing
// contract #48): tries are exhausted but the post-loop final probe is WARM, so
// the client is served the real 200 inference body from serveWithWake
// (served=true) and the normal metered forward never runs. Exactly one raw
// reconciliation row at the status actually written to the client (200) — NOT
// zero, and NOT an extra row beyond it.
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

	served := s.serveWithWake(rec, req, up, id, "req-1", "client-req-1", nil)
	if !served {
		t.Fatal("tries-exhausted should serve the final probe response (served=true)")
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"served":true`) {
		t.Fatalf("client got %d body %q, want the warm 200 inference body", rec.Code, rec.Body.String())
	}
	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("tries-exhausted emitted %d billing events, want exactly 1 raw reconciliation row: %+v",
			len(events), events)
	}
	ev := events[0]
	if ev.Aborted || ev.UsageFound || ev.StatusCode != http.StatusOK {
		t.Fatalf("tries-exhausted row = {Aborted:%v UsageFound:%v StatusCode:%d}, "+
			"want {false false 200} (visible to reconciliation, charges $0)", ev.Aborted, ev.UsageFound, ev.StatusCode)
	}
	if got := rec.Header().Get(requestIDHeader); got != "req-1" {
		t.Fatalf("tries-exhausted response X-Request-Id = %q, want the minted attempt id (client correlation handle)", got)
	}
}

// TestWakeColdHoldRejectionEmitsReconciliationRow (merged billing contract #48,
// exactly-once): the BeginColdHold-failure exit is the third and last served=true
// exit; nothing else pins it. With the platform cold-hold cap already consumed by
// another lease, a cold probe's BeginColdHold is rejected, the client gets the
// admission error, and exactly one raw reconciliation row is recorded at the
// status actually written — the regression class these tests exist to catch on
// a money path (wrong status, or a double-emit, would pass the suite without
// this test).
func TestWakeColdHoldRejectionEmitsReconciliationRow(t *testing.T) {
	backend := &coldToWarmBackend{} // stays cold
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)

	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(10)
	cfg.Platform.MaxColdHolds = 1
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	admitter := admission.New(c, cfg)

	holdReq := admission.Request{Graph: "g1", Organization: "org-a", Model: "m", ReservedOutputTokens: 1}
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

// TestWakeWarmFallThroughMetersNormalRowOnly (merged billing contract #48,
// exactly-once): a warm first probe returns false and the caller's normal
// metered forward runs, emitting the usual single usage-bearing completion row.
// serveWithWake must NOT add a raw row of its own — the attempt is metered
// exactly once, by the forward.
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
