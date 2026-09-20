package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

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
	if !isWakeable(identity.Identity{ResourceID: "r1", ServedModel: "m"}) {
		t.Fatal("shared route with resource id should be wakeable")
	}
	if isWakeable(identity.Identity{ResourceID: "r1"}) {
		t.Fatal("no served-model allow-list (dedicated) must NOT be wakeable")
	}
	if isWakeable(identity.Identity{ServedModel: "m"}) {
		t.Fatal("no resource id (unauthorized) must NOT be wakeable")
	}
}

type fakeWaker struct {
	calls int32
	err   error
	wake  func(context.Context, WakeTarget) error

	mu         sync.Mutex
	lastTarget WakeTarget
}

func (f *fakeWaker) Wake(ctx context.Context, target WakeTarget) error {
	atomic.AddInt32(&f.calls, 1)
	f.mu.Lock()
	f.lastTarget = target
	f.mu.Unlock()
	if f.wake != nil {
		return f.wake(ctx, target)
	}
	return f.err
}

func (f *fakeWaker) last() WakeTarget {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastTarget
}

// inferenceBackend counts only customer inference executions. Readiness is the
// waker's responsibility and therefore never reaches this POST handler.
type inferenceBackend struct {
	calls int32
	warm  atomic.Bool
}

func (b *inferenceBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&b.calls, 1)
	if r.Method != http.MethodPost {
		http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		return
	}
	if !b.warm.Load() {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Model not found"}`))
		return
	}
	_, _ = w.Write([]byte(`{"model":"m","choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`))
}

// TestWithWakerDefaults pins the default wake budget: 300s — deliberately
// above vLLM's measured ~2.5min cold reload.
func TestWithWakerDefaults(t *testing.T) {
	s := New(&config.Settings{}, logging.New(logging.ERROR), nil).WithWaker(&fakeWaker{}, 0)
	if s.wakeTimeout != 300*time.Second {
		t.Fatalf("default wakeTimeout = %v, want 300s", s.wakeTimeout)
	}
}

func wakeableRequest(backend *httptest.Server) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	req.Header.Set(identity.HeaderServedModel, "m")
	req.Header.Set(identity.HeaderUpstream, strings.TrimPrefix(backend.URL, "http://"))
	req.Header.Set(requestIDHeader, "client-request")
	return req
}

func TestWakeThenSingleMeteredInference(t *testing.T) {
	backendHandler := &inferenceBackend{}
	backend := httptest.NewServer(backendHandler)
	defer backend.Close()
	em := &recordingEmitter{}
	waker := &fakeWaker{wake: func(context.Context, WakeTarget) error {
		backendHandler.warm.Store(true)
		return nil
	}}
	s := New(&config.Settings{}, logging.New(logging.ERROR), em).WithWaker(waker, time.Second)

	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, wakeableRequest(backend))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rr.Code, rr.Body.String())
	}
	if got := atomic.LoadInt32(&backendHandler.calls); got != 1 {
		t.Fatalf("inference POST executions = %d, want exactly 1", got)
	}
	if got := atomic.LoadInt32(&waker.calls); got != 1 {
		t.Fatalf("waker calls = %d, want 1", got)
	}
	if target := waker.last(); target.ServedModel != "m" || target.ResourceID != "r1" {
		t.Fatalf("wake target = %+v, want requested model and authorized resource", target)
	}
	events := em.waitForEvents(1, time.Second)
	if len(events) != 1 || !events[0].UsageFound || events[0].PromptTokens != 7 {
		t.Fatalf("events = %+v, want one usage-bearing event", events)
	}
}

func TestWakeFailureFallsThroughToOneHonestMeteredResponse(t *testing.T) {
	backendHandler := &inferenceBackend{} // remains cold
	backend := httptest.NewServer(backendHandler)
	defer backend.Close()
	em := &recordingEmitter{}
	waker := &fakeWaker{err: errors.New("cluster unavailable")}
	s := New(&config.Settings{}, logging.New(logging.ERROR), em).WithWaker(waker, time.Second)

	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, wakeableRequest(backend))

	if rr.Code != http.StatusNotFound || !strings.Contains(rr.Body.String(), "Model not found") {
		t.Fatalf("response = %d %q, want honest upstream 404", rr.Code, rr.Body.String())
	}
	if got := atomic.LoadInt32(&backendHandler.calls); got != 1 {
		t.Fatalf("inference POST executions = %d, want exactly 1", got)
	}
	events := em.waitForEvents(1, time.Second)
	if len(events) != 1 || events[0].UsageFound || events[0].StatusCode != http.StatusNotFound {
		t.Fatalf("events = %+v, want one zero-charge 404 attempt", events)
	}
}

func TestClientDisconnectWhileWakingNeverExecutesInference(t *testing.T) {
	backendHandler := &inferenceBackend{}
	backend := httptest.NewServer(backendHandler)
	defer backend.Close()
	started := make(chan struct{})
	waker := &fakeWaker{wake: func(ctx context.Context, _ WakeTarget) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}}
	em := &recordingEmitter{}
	s := New(&config.Settings{}, logging.New(logging.ERROR), em).WithWaker(waker, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	req := wakeableRequest(backend).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		s.Handler().ServeHTTP(httptest.NewRecorder(), req)
		close(done)
	}()
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not return after client cancellation")
	}
	if got := atomic.LoadInt32(&backendHandler.calls); got != 0 {
		t.Fatalf("inference POST executions = %d, want 0 after disconnect", got)
	}
	events := em.waitForEvents(1, time.Second)
	if len(events) != 1 || !events[0].Aborted || events[0].UsageFound {
		t.Fatalf("events = %+v, want one aborted zero-charge attempt", events)
	}
}
