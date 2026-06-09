package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
	"github.com/saturncloud/phoebe/internal/registry"
)

// recordingEmitter captures emitted events for assertions. Safe for concurrent
// use: the tee fires Emit from the response-read path.
type recordingEmitter struct {
	mu     sync.Mutex
	events []metering.Event
}

func (r *recordingEmitter) Emit(_ context.Context, e metering.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recordingEmitter) all() []metering.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]metering.Event(nil), r.events...)
}

func (r *recordingEmitter) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// waitForEvents polls until at least n events have been emitted, or the timeout
// elapses, and returns the events. Emit happens ASYNCHRONOUSLY from the abort-
// watcher / onDone goroutines, so a test must not read events immediately after
// ServeHTTP returns — a fixed sleep is flaky under CI load. Poll instead so the
// test is deterministic regardless of scheduling. Returns whatever was captured
// if it times out, letting the caller assert and report the shortfall.
func (r *recordingEmitter) waitForEvents(n int, timeout time.Duration) []metering.Event {
	deadline := time.Now().Add(timeout)
	for {
		if r.count() >= n || time.Now().After(deadline) {
			return r.all()
		}
		time.Sleep(time.Millisecond)
	}
}

func newTestServer(t *testing.T, upstream *url.URL) *Server {
	t.Helper()
	return newTestServerE(t, upstream, &recordingEmitter{})
}

func newTestServerE(t *testing.T, upstream *url.URL, em metering.Emitter) *Server {
	t.Helper()
	s := &config.Settings{ListenAddr: ":0"}
	log := logging.New(logging.ERROR)
	resolver := registry.NewStatic(upstream)
	return New(s, log, resolver, em)
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

// TestProxyBillingGate verifies the fail-closed billing-identity gate: a
// request missing the auth-id and/or resource-id headers is rejected with 400
// (we never serve traffic we can't attribute), and the error names what's
// missing. An emitter is checked to ensure nothing is billed for a reject.
func TestProxyBillingGate(t *testing.T) {
	tests := []struct {
		name       string
		authID     string
		resourceID string
		wantStatus int
		wantInBody string
	}{
		{"missing both", "", "", http.StatusBadRequest, identity.HeaderAuthID},
		{"missing auth-id", "", "model-abc", http.StatusBadRequest, identity.HeaderAuthID},
		{"missing resource-id", "auth-1", "", http.StatusBadRequest, identity.HeaderResourceID},
		{"both present", "auth-1", "model-abc", http.StatusOK, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			defer backend.Close()
			upstream, _ := url.Parse(backend.URL)
			em := &recordingEmitter{}
			srv := newTestServerE(t, upstream, em)

			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			if tt.authID != "" {
				req.Header.Set(identity.HeaderAuthID, tt.authID)
			}
			if tt.resourceID != "" {
				req.Header.Set(identity.HeaderResourceID, tt.resourceID)
			}
			srv.Handler().ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rr.Code, tt.wantStatus)
			}
			if tt.wantInBody != "" && !strings.Contains(rr.Body.String(), tt.wantInBody) {
				t.Fatalf("body %q does not name missing field %q", rr.Body.String(), tt.wantInBody)
			}
			if tt.wantStatus == http.StatusBadRequest && len(em.all()) != 0 {
				t.Fatalf("rejected request should emit no billing event, got %d", len(em.all()))
			}
		})
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
	req.Header.Set(identity.HeaderAuthID, "auth-key-7")
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
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "gone")
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("torn-down model: got %d, want 404", rr.Code)
	}
}

// TestProxyStreamingEndToEnd drives the full path through a real fake-vLLM
// backend: rewrite → forward → tee → emit. It asserts (1) the client receives
// the SSE bytes verbatim, (2) the backend actually saw include_usage forced,
// and (3) a metering event with the right counts was emitted.
func TestProxyStreamingEndToEnd(t *testing.T) {
	var gotIncludeUsage bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Inspect the forwarded request body for the forced flag.
		body, _ := io.ReadAll(r.Body)
		gotIncludeUsage = strings.Contains(string(body), `"include_usage":true`)

		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		for _, chunk := range strings.SplitAfter(vllmStream, "\n\n") {
			if chunk == "" {
				continue
			}
			_, _ = io.WriteString(w, chunk)
			if fl != nil {
				fl.Flush()
			}
		}
	}))
	defer backend.Close()

	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	srv := newTestServerE(t, upstream, em)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	req.Header.Set(identity.HeaderResourceID, "model-abc")
	req.Header.Set(identity.HeaderResourceType, "deployment")
	req.Header.Set(identity.HeaderGroupID, "org-1")
	req.Header.Set(identity.HeaderUserID, "user-1")
	req.Header.Set(identity.HeaderAuthID, "auth-key-7")
	req.Header.Set("X-Request-Id", "req-123")

	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !gotIncludeUsage {
		t.Fatal("backend did not receive include_usage:true — forcing failed")
	}
	if rr.Body.String() != vllmStream {
		t.Fatalf("client did not receive SSE verbatim:\n%q", rr.Body.String())
	}

	events := em.all()
	if len(events) != 1 {
		t.Fatalf("expected 1 metering event, got %d", len(events))
	}
	e := events[0]
	if e.RequestID != "req-123" || e.GroupID != "org-1" || e.UserID != "user-1" {
		t.Fatalf("event identity wrong: %+v", e)
	}
	if e.AuthID != "auth-key-7" {
		t.Fatalf("event AuthID = %q, want auth-key-7", e.AuthID)
	}
	if e.ResourceID != "model-abc" || e.ResourceType != "deployment" {
		t.Fatalf("event resource fields wrong: id=%q type=%q", e.ResourceID, e.ResourceType)
	}
	// Model is the ENGINE-REPORTED name from the response body ("llama-3-8b"),
	// NOT the routing resource id ("model-abc"). Pricing keys on this; getting
	// it from the resource id would leave every event unpriced. This assertion
	// is the regression guard for that bug.
	if e.Model != "llama-3-8b" {
		t.Fatalf("event Model = %q, want llama-3-8b (engine name, not resource id)", e.Model)
	}
	if e.Model == e.ResourceID {
		t.Fatal("event Model must not equal ResourceID — the price key is the engine model name, not the deployment id")
	}
	if e.PromptTokens != 2006 || e.CompletionTokens != 300 || e.CachedTokens != 1920 {
		t.Fatalf("event token counts wrong: %+v", e)
	}
	if e.FinishReason != "stop" {
		t.Fatalf("event finish_reason = %q, want stop", e.FinishReason)
	}
}
