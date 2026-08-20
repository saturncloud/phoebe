package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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
}

func (f *fakeWaker) Wake(ctx context.Context, upstream, resourceID string) error {
	n := atomic.AddInt32(&f.calls, 1)
	if f.err != nil {
		return f.err
	}
	if f.backend != nil && n >= f.warmsAt {
		f.backend.warm.Store(true)
	}
	return nil
}

// coldToWarmBackend serves cold (404) until warm is set, then 200.
type coldToWarmBackend struct{ warm atomic.Bool }

func (c *coldToWarmBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if c.warm.Load() {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"served":true}`))
		return
	}
	w.WriteHeader(404)
	_, _ = w.Write([]byte(`{"message":"Model not found"}`))
}

func testServerWithWaker(waker Waker) *Server {
	s := New(&config.Settings{}, logging.New(logging.ERROR), nil)
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

	served := s.serveWithWake(rec, req, up, id, "req-1")
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

	served := s.serveWithWake(rec, req, up, id, "req-1")
	if !served {
		t.Fatal("wake error should serve the cold response (served=true)")
	}
	if rec.Code != 404 {
		t.Fatalf("expected the cold 404 flushed to client, got %d", rec.Code)
	}
}
