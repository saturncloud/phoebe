package proxy

import (
	"context"
	"io"
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
			resp := &http.Response{
				StatusCode: tc.status,
				Body:       io.NopCloser(strings.NewReader(tc.body)),
			}
			got, err := isColdResponse(resp)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("isColdResponse(status=%d)=%v want %v", tc.status, got, tc.want)
			}
			preserved, err := io.ReadAll(resp.Body)
			if err != nil || string(preserved) != tc.body {
				t.Fatalf("response body not preserved: %q, %v", preserved, err)
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
	s := New(&config.Settings{}, logging.New(logging.ERROR), nil)
	return s.WithWaker(waker, 5*time.Second, 3)
}

func TestWakeRoundTripper_ColdThenWarmExecutesOneSuccessfulInference(t *testing.T) {
	backend := &coldToWarmBackend{}
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)

	waker := &fakeWaker{warmsAt: 1, backend: backend}
	s := testServerWithWaker(waker)

	req := httptest.NewRequest("POST", "http://x/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	replaceRequestBody(req, []byte(`{"model":"m"}`))
	id := identity.Identity{ResourceID: "r1", ServedModel: "m"}
	req.URL = up
	resp, err := s.newWakeRoundTripper(up.Host, "req-1", id, nil).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want 200", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&waker.calls); got != 1 {
		t.Fatalf("waker called %d times, want 1", got)
	}
	if backend.requests.Load() != 2 || backend.successes.Load() != 1 {
		t.Fatalf("backend requests=%d successes=%d, want one cold + one successful inference", backend.requests.Load(), backend.successes.Load())
	}
}

func TestWakeRoundTripper_WarmRequestExecutesExactlyOnce(t *testing.T) {
	backend := &coldToWarmBackend{}
	backend.warm.Store(true)
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)
	waker := &fakeWaker{}
	s := testServerWithWaker(waker)
	req := httptest.NewRequest("POST", up.String(), strings.NewReader(`{"model":"m"}`))
	replaceRequestBody(req, []byte(`{"model":"m"}`))
	id := identity.Identity{ResourceID: "r1", ServedModel: "m"}

	resp, err := s.newWakeRoundTripper(up.Host, "req-1", id, nil).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if backend.requests.Load() != 1 || backend.successes.Load() != 1 {
		t.Fatalf("backend requests=%d successes=%d, want exactly one inference", backend.requests.Load(), backend.successes.Load())
	}
	if waker.calls != 0 {
		t.Fatalf("waker called %d times for warm request", waker.calls)
	}
}

func TestWakeRoundTripper_WakeErrorReturnsCold(t *testing.T) {
	backend := &coldToWarmBackend{} // stays cold
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)

	waker := &fakeWaker{err: context.DeadlineExceeded}
	s := testServerWithWaker(waker)

	req := httptest.NewRequest("POST", "http://x/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	replaceRequestBody(req, []byte(`{"model":"m"}`))
	id := identity.Identity{ResourceID: "r1", ServedModel: "m"}
	req.URL = up
	resp, err := s.newWakeRoundTripper(up.Host, "req-1", id, nil).RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected cold 404, got %d", resp.StatusCode)
	}
}
