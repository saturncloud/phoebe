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

// TestProxyRequestID_GeneratedWhenAbsent guards the served-but-never-billed
// hole: X-Request-Id is client-controlled and the drainer poison-drops events
// with an empty request_id, so a client that simply omits the header must NOT
// escape billing. The proxy generates an id, uses it on the metering event,
// forwards it upstream, and echoes it to the client for correlation.
func TestProxyRequestID_GeneratedWhenAbsent(t *testing.T) {
	var mu sync.Mutex
	var upstreamSaw string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		upstreamSaw = r.Header.Get("X-Request-Id")
		mu.Unlock()
		_, _ = w.Write([]byte(`{"model":"m1","choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`))
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	srv := newTestServerE(t, upstream, em)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","messages":[]}`))
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "model-abc")
	// Deliberately NO X-Request-Id.
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (absent id must be generated, not rejected)", rr.Code)
	}
	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("expected 1 metering event, got %d", len(events))
	}
	id := events[0].RequestID
	if !strings.HasPrefix(id, "phoebe-") || len(id) != len("phoebe-")+32 {
		t.Fatalf("generated request id = %q, want phoebe-<32 hex>", id)
	}
	if !validRequestID(id) {
		t.Fatalf("generated id %q fails our own validity gate", id)
	}
	mu.Lock()
	saw := upstreamSaw
	mu.Unlock()
	if saw != id {
		t.Fatalf("upstream saw request id %q, event has %q — correlation broken", saw, id)
	}
	if got := rr.Header().Get("X-Request-Id"); got != id {
		t.Fatalf("response X-Request-Id = %q, want %q (client must learn the generated id)", got, id)
	}
}

// TestProxyRequestID_RejectsInvalid is the fail-closed gate on the billing
// idempotency PK: an oversize id would poison the drainer's batch INSERT
// (VARCHAR(255)), and non-printable bytes are a billing-integrity attack, not
// a normal request. Invalid ids get a 400 before any upstream work and emit
// no billing event.
func TestProxyRequestID_RejectsInvalid(t *testing.T) {
	tests := []struct {
		name       string
		requestID  string
		wantStatus int
	}{
		{"valid passes", "req-abc.123_OK", http.StatusOK},
		{"max length passes", strings.Repeat("a", 200), http.StatusOK},
		{"over max length rejected", strings.Repeat("a", 201), http.StatusBadRequest},
		{"space rejected", "req 123", http.StatusBadRequest},
		{"control byte rejected", "req\x01id", http.StatusBadRequest},
		{"DEL byte rejected", "req\x7fid", http.StatusBadRequest},
		{"non-ASCII rejected", "rëq-1", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var upstreamHits int32
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				atomic.AddInt32(&upstreamHits, 1)
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			defer backend.Close()
			upstream, _ := url.Parse(backend.URL)
			em := &recordingEmitter{}
			srv := newTestServerE(t, upstream, em)

			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			req.Header.Set(identity.HeaderAuthID, "auth-1")
			req.Header.Set(identity.HeaderResourceID, "model-abc")
			req.Header.Set("X-Request-Id", tt.requestID)
			srv.Handler().ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rr.Code, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusBadRequest {
				if n := atomic.LoadInt32(&upstreamHits); n != 0 {
					t.Fatalf("invalid id reached upstream %d times, want 0 (reject before forwarding)", n)
				}
				if got := em.count(); got != 0 {
					t.Fatalf("rejected request emitted %d billing events, want 0", got)
				}
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

// TestProxyUpstreamHeaderRoutes is THE routing-seam test: when Atlas injects
// X-Saturn-Upstream (a Token Factory inference deployment), phoebe MUST forward to that
// backend and NOT resolve — because the real k8s Service (a `pd-...` name) is not
// derivable by the resolver's convention. Proven by pointing the RESOLVER at a
// "wrong" backend that fails the test if hit, and the HEADER at the "right" one.
func TestProxyUpstreamHeaderRoutes(t *testing.T) {
	// The resolver's target — MUST NOT be reached when the header is present.
	wrong := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("resolver backend was hit — phoebe ignored X-Saturn-Upstream and resolved by convention")
		w.WriteHeader(http.StatusTeapot)
	}))
	defer wrong.Close()
	// The header's target — the real deployment Service; MUST be reached.
	right := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"routed":"via-upstream-header"}`))
	}))
	defer right.Close()

	wrongURL, _ := url.Parse(wrong.URL)
	srv := newTestServer(t, wrongURL) // resolver → wrong backend

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "828402f0deadbeef") // a `pd-...` deploy; convention can't reach it
	// Atlas injects the real backend as host:port (no scheme), as it does in production.
	req.Header.Set(identity.HeaderUpstream, strings.TrimPrefix(right.URL, "http://"))
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("got %d, want 200 (should forward via the upstream header)", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if string(body) != `{"routed":"via-upstream-header"}` {
		t.Fatalf("body = %q, want the header-target's response (phoebe forwarded to the wrong backend)", string(body))
	}
}

// TestProxyUpstreamHeaderAbsentFallsBackToResolver: with NO X-Saturn-Upstream (the
// normal, non-inference path), phoebe resolves as before — the header is a preference,
// not a requirement.
func TestProxyUpstreamHeaderAbsentFallsBackToResolver(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"routed":"via-resolver"}`))
	}))
	defer backend.Close()
	u, _ := url.Parse(backend.URL)
	srv := newTestServer(t, u)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "model-abc")
	// No X-Saturn-Upstream.
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("got %d, want 200 (resolver fallback)", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if string(body) != `{"routed":"via-resolver"}` {
		t.Fatalf("body = %q, want the resolver target's response", string(body))
	}
}

// TestProxyUpstreamHeaderMalformedFailsClosed: a broken trusted header (Atlas injected a
// bad value) is a broken edge contract, not a normal request — fail closed (502), never
// forward to a guessed/empty target.
func TestProxyUpstreamHeaderMalformedFailsClosed(t *testing.T) {
	unused, _ := url.Parse("http://unused")
	srv := newTestServer(t, unused)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r")
	req.Header.Set(identity.HeaderUpstream, "://:") // no host, unparseable target
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("got %d, want 502 (a malformed upstream header must fail closed)", rr.Code)
	}
}

// TestProxyBillingGate_OrgIDNotGated asserts the Q2 ruling by name: org_id is
// captured best-effort, NOT a hot-path gate. A request carrying X-Saturn-Org-Id has
// it stamped onto the metering event; a request MISSING it is still served (200) and
// still emits an event (org held + screamed at push, never here) — so a per-install
// producer-rollout gap can never black-hole inference.
func TestProxyBillingGate_OrgIDNotGated(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"model":"m","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)

	t.Run("org present is carried onto the event", func(t *testing.T) {
		em := &recordingEmitter{}
		srv := newTestServerE(t, upstream, em)
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		req.Header.Set(identity.HeaderAuthID, "auth-1")
		req.Header.Set(identity.HeaderResourceID, "model-abc")
		req.Header.Set(identity.HeaderOrgID, "org-42")
		srv.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("got %d, want 200", rr.Code)
		}
		evs := em.waitForEvents(1, time.Second)
		if len(evs) != 1 {
			t.Fatalf("emitted %d events, want 1", len(evs))
		}
		if evs[0].OrgID != "org-42" {
			t.Errorf("event OrgID = %q, want org-42", evs[0].OrgID)
		}
	})

	t.Run("org absent is served and still emits (not gated)", func(t *testing.T) {
		em := &recordingEmitter{}
		srv := newTestServerE(t, upstream, em)
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		req.Header.Set(identity.HeaderAuthID, "auth-1")
		req.Header.Set(identity.HeaderResourceID, "model-abc")
		// No X-Saturn-Org-Id.
		srv.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("missing org_id must NOT gate: got %d, want 200", rr.Code)
		}
		evs := em.waitForEvents(1, time.Second)
		if len(evs) != 1 {
			t.Fatalf("missing org_id must still emit: emitted %d events, want 1", len(evs))
		}
		if evs[0].OrgID != "" {
			t.Errorf("event OrgID = %q, want empty", evs[0].OrgID)
		}
	})
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
