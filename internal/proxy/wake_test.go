package proxy

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	// An EMPTY ServingMode is dedicated by the absence-of-prefix contract
	// (identity.ServingMode: "Empty = dedicated"), so it is NOT wakeable even
	// on a fully-resolved gateway route. The gateway registry parser
	// (gateway.parseRegistryConfigMap) rejects rows with a blank serving_mode
	// precisely so a shared row can never arrive here with "" and silently
	// lose wake-from-zero.
	if isWakeable(identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: ""}) {
		t.Fatal("empty serving mode is dedicated by contract and must NOT be wakeable")
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

	served := s.serveWithWake(rec, req, up, id, "req-1", nil)
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

	served := s.serveWithWake(rec, req, up, id, "req-1", nil)
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

	served := s.serveWithWake(rec, req, up, id, "req-1", nil)
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

	served := s.serveWithWake(rec, req, up, id, "req-1", nil)
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

// TestWakeEnabledWarnsOnBoundRouteMissingServingMode pins the diagnosability
// fix for the producer-rollout ordering window: the middleware injects
// X-Saturn-Served-Model but not yet X-Saturn-Serving-Mode, so a bound route
// silently loses wake-from-zero (empty serving mode is dedicated by the
// absence-of-prefix contract — the behavior is intentional and unchanged). The
// only addition is this WARN, fired when wake is configured and the route has
// the bound shape without a serving mode.
func TestWakeEnabledWarnsOnBoundRouteMissingServingMode(t *testing.T) {
	newServer := func() (*Server, *bytes.Buffer) {
		var buf bytes.Buffer
		logger := &logging.Logger{
			Debug: log.New(io.Discard, "", 0),
			Info:  log.New(io.Discard, "", 0),
			Warn:  log.New(&buf, "", 0),
			Error: log.New(io.Discard, "", 0),
		}
		return New(&config.Settings{}, logger, nil).WithWaker(&fakeWaker{}, time.Second, 1), &buf
	}

	cases := []struct {
		name     string
		id       identity.Identity
		wantWake bool
		wantLog  bool
	}{
		{"bound shape without serving mode warns", identity.Identity{ResourceID: "r1", ServedModel: "m"}, false, true},
		{"shared mode wakes and does not warn", identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "shared"}, true, false},
		{"explicit dedicated does not warn", identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "dedicated"}, false, false},
		{"unbound route does not warn", identity.Identity{ResourceID: "r1"}, false, false},
		{"no resource id does not warn", identity.Identity{ServedModel: "m"}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, buf := newServer()
			if got := s.wakeEnabled(tc.id); got != tc.wantWake {
				t.Fatalf("wakeEnabled = %v, want %v", got, tc.wantWake)
			}
			if got := strings.Contains(buf.String(), "edge contract not fully rolled out"); got != tc.wantLog {
				t.Fatalf("warn present = %v, want %v (log: %q)", got, tc.wantLog, buf.String())
			}
		})
	}

	// Wake unconfigured: the predicate short-circuits before the diagnostic, so
	// no WARN either (a dedicated install has no wake rollout to diagnose).
	s, buf := newServer()
	s.waker = nil
	if s.wakeEnabled(identity.Identity{ResourceID: "r1", ServedModel: "m"}) {
		t.Fatal("wakeEnabled = true with nil waker, want false")
	}
	if buf.Len() != 0 {
		t.Fatalf("warn logged with wake unconfigured: %q", buf.String())
	}
}
