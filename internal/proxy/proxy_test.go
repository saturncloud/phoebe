package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
	"github.com/saturncloud/phoebe/internal/registry"
)

// recordingEmitter captures emitted events for assertions.
type recordingEmitter struct{ events []metering.Event }

func (r *recordingEmitter) Emit(_ context.Context, e metering.Event) {
	r.events = append(r.events, e)
}

func newTestServer(t *testing.T, upstream *url.URL) *Server {
	t.Helper()
	s := &config.Settings{ListenAddr: ":0"}
	log := logging.New(logging.ERROR)
	resolver := registry.NewStatic(upstream)
	return New(s, log, resolver, &recordingEmitter{})
}

func TestHealthz(t *testing.T) {
	srv := newTestServer(t, &url.URL{Scheme: "http", Host: "localhost:1"})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("healthz: got %d, want 200", rr.Code)
	}
}

func TestProxyMissingResourceID(t *testing.T) {
	srv := newTestServer(t, &url.URL{Scheme: "http", Host: "localhost:1"})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("missing resource id: got %d, want 400", rr.Code)
	}
}

func TestProxyForwardsToUpstream(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer backend.Close()

	upstream, _ := url.Parse(backend.URL)
	srv := newTestServer(t, upstream)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set(identity.HeaderResourceID, "model-abc")
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("proxy: got %d, want 200", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if string(body) != `{"ok":true}` {
		t.Fatalf("proxy body: got %q", string(body))
	}
}

func TestProxyNotFound(t *testing.T) {
	// Resolver with no fallback → ErrNotFound → clean 404.
	s := &config.Settings{ListenAddr: ":0"}
	log := logging.New(logging.ERROR)
	srv := New(s, log, registry.NewStatic(nil), &recordingEmitter{})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set(identity.HeaderResourceID, "gone")
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("torn-down model: got %d, want 404", rr.Code)
	}
}
