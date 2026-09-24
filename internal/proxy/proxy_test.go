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

func newTestServerE(t *testing.T, _ *url.URL, em metering.Emitter) *Server {
	t.Helper()
	s := &config.Settings{ListenAddr: ":0"}
	log := logging.New(logging.ERROR)
	return New(s, log, em)
}

// setUpstream stamps the X-Saturn-Upstream routing header onto a test request,
// exactly as Atlas's per-route injection does in production. Routing now comes
// solely from this header (phoebe resolves no upstream of its own), so every
// request a test expects to be FORWARDED must carry it.
func setUpstream(req *http.Request, upstream *url.URL) {
	req.Header.Set(identity.HeaderUpstream, upstream.Host)
	req.Header.Set("X-Request-Id", "saturn-test-request-id")
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

func TestProxyBindsDedicatedEndpointToServedModel(t *testing.T) {
	var upstreamCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		// Emit the graph-wide headers the sanitizers exist to strip. Without
		// these the "no X-Graph-Debug / ETag leaked" assertions below are
		// vacuous — they would pass whether or not the sanitizer ran.
		w.Header().Set("X-Graph-Debug", "adapter-b")
		w.Header().Set("ETag", "graph-wide")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"adapter-a","object":"model"},{"id":"adapter-b","object":"model"},{"id":"base-internal","object":"model"}]}`))
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	srv := newTestServer(t, upstream)

	request := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		setUpstream(req, upstream)
		req.Header.Set(identity.HeaderAuthID, "auth-1")
		req.Header.Set(identity.HeaderResourceID, "deployment-a")
		req.Header.Set(identity.HeaderServedModel, "adapter-a")
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, req)
		return rr
	}

	if rr := request(http.MethodPost, "/v1/chat/completions", `{"model":"adapter-b"}`); rr.Code != http.StatusForbidden {
		t.Fatalf("cross-model status = %d, want 403", rr.Code)
	}
	if upstreamCalls != 0 {
		t.Fatalf("cross-model request reached Dynamo (%d calls)", upstreamCalls)
	}
	if rr := request(http.MethodPost, "/v1/chat/completions", `{"model":"adapter-a"}`); rr.Code != http.StatusOK {
		t.Fatalf("bound model status = %d, want 200", rr.Code)
	}
	if rr := request(http.MethodGet, "/v1/models", ""); rr.Code != http.StatusOK {
		t.Fatalf("model-list status = %d, want 200", rr.Code)
	} else {
		body := rr.Body.String()
		if !strings.Contains(body, `"id":"adapter-a"`) {
			t.Fatalf("model list omitted authorized model: %s", body)
		}
		if strings.Contains(body, "adapter-b") || strings.Contains(body, "base-internal") {
			t.Fatalf("model list disclosed graph-wide names: %s", body)
		}
	}
	if rr := request(http.MethodGet, "/v1/models/adapter-b", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("sibling model metadata status = %d, want 404", rr.Code)
	}
	if rr := request(http.MethodGet, "/v1/models/adapter-b/ready", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("sibling model readiness status = %d, want 404", rr.Code)
	}
	if rr := request(http.MethodHead, "/v1/models/adapter-b", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("sibling HEAD metadata status = %d, want 404", rr.Code)
	}
	if rr := request(http.MethodHead, "/v1/models/adapter-b/ready", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("sibling HEAD readiness status = %d, want 404", rr.Code)
	}
	for _, path := range []string{"/metrics", "/busy_threshold", "/docs", "/openapi.json", "/future-admin"} {
		if rr := request(http.MethodGet, path, ""); rr.Code != http.StatusNotFound {
			t.Fatalf("bound GET %s status = %d, want 404", path, rr.Code)
		}
	}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		for _, path := range []string{"/future-admin", "/v1/models", "/metrics", "/busy_threshold"} {
			if rr := request(method, path, `{"model":"adapter-a"}`); rr.Code != http.StatusNotFound {
				t.Fatalf("bound %s %s status = %d, want 404", method, path, rr.Code)
			}
		}
	}
	if rr := request(http.MethodGet, "/v1/models/adapter-a", ""); rr.Code != http.StatusOK {
		t.Fatalf("bound model metadata status = %d, want 200", rr.Code)
	}
	if rr := request(http.MethodGet, "/v1/models/adapter-a/ready", ""); rr.Code != http.StatusNotFound {
		t.Fatalf("ambiguous model readiness status = %d, want 404", rr.Code)
	}
	if rr := request(http.MethodHead, "/v1/models/adapter-a", ""); rr.Code != http.StatusOK {
		t.Fatalf("bound HEAD metadata status = %d, want 200", rr.Code)
	}
	if rr := request(http.MethodHead, "/v1/models", ""); rr.Code != http.StatusOK {
		t.Fatalf("bound HEAD list status = %d, want 200", rr.Code)
	} else if rr.Header().Get("Content-Length") != "" || rr.Header().Get("X-Graph-Debug") != "" ||
		rr.Header().Get("ETag") != "" {
		t.Fatalf("HEAD list leaked graph-wide representation headers: %v", rr.Header())
	}
	// OPTIONS used to bypass the path switch entirely and reach Dynamo, whose
	// framework answers preflight with an Allow header enumerating a graph-wide
	// admin route's methods. It is now refused on admin paths and answered
	// LOCALLY (204, no body, no Allow echo) on the inference surface. The
	// load-bearing assertion is the upstream call counter below: no OPTIONS
	// reaches Dynamo on ANY path.
	for _, path := range []string{"/metrics", "/busy_threshold", "/docs", "/openapi.json", "/future-admin", "/v1/models"} {
		rr := request(http.MethodOptions, path, "")
		if rr.Code != http.StatusNotFound {
			t.Fatalf("bound OPTIONS %s status = %d, want 404", path, rr.Code)
		}
		if rr.Header().Get("Allow") != "" {
			t.Fatalf("bound OPTIONS %s echoed an upstream method enumeration: %q", path, rr.Header().Get("Allow"))
		}
	}
	for _, path := range []string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings"} {
		rr := request(http.MethodOptions, path, "")
		if rr.Code != http.StatusNoContent {
			t.Fatalf("bound OPTIONS %s status = %d, want 204", path, rr.Code)
		}
		if rr.Body.Len() != 0 || rr.Header().Get("Allow") != "" || rr.Header().Get("X-Graph-Debug") != "" {
			t.Fatalf("bound OPTIONS %s returned upstream data: body=%q headers=%v", path, rr.Body.String(), rr.Header())
		}
	}
	// Encoded targets decode to an allowlisted path but are FORWARDED raw, so
	// an upstream that normalizes differently would resolve a path the
	// allow-list never approved. They are refused, and never reach Dynamo.
	for _, target := range []string{"/v1%2Fchat/completions", "/v1/models/%61dapter-a", "/v1/models/%61dapter-b"} {
		if rr := request(http.MethodPost, target, `{"model":"adapter-a"}`); rr.Code != http.StatusNotFound {
			t.Fatalf("encoded target %s status = %d, want 404", target, rr.Code)
		}
	}
	if upstreamCalls != 5 {
		t.Fatalf("authorized requests made %d upstream calls, want 5", upstreamCalls)
	}
}

// A real client round-trip, not a ResponseRecorder: trailers are only
// transported over the wire, and net/http sends none at all for HEAD — so the
// recorder-based version of this test could not fail on the HEAD iteration. The
// backend here announces AND emits a real trailer plus a plain leak header set
// BEFORE WriteHeader (the previous Set-after-WriteHeader was a no-op, so the
// header the test is named around never existed on the wire).
func TestProxySanitizesModelListTrailers(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Trailer", "X-Graph-Debug")
		w.Header().Set("X-Graph-Debug-Hdr", "adapter-b")
		w.Header().Set("ETag", "graph-wide")
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"adapter-b failed"}`))
		} else {
			w.WriteHeader(http.StatusOK)
		}
		w.Header().Set("X-Graph-Debug", "adapter-b")
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	srv := newTestServer(t, upstream)

	front := httptest.NewServer(srv.Handler())
	defer front.Close()

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		req, err := http.NewRequestWithContext(context.Background(), method, front.URL+"/v1/models", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(identity.HeaderUpstream, upstream.Host)
		req.Header.Set(identity.HeaderAuthID, "auth-1")
		req.Header.Set(identity.HeaderResourceID, "deployment-a")
		req.Header.Set(identity.HeaderServedModel, "adapter-a")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		// Go populates resp.Trailer only after the body is drained.
		_, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.Header.Get("Trailer") != "" || len(resp.Trailer) != 0 {
			t.Fatalf("%s model list leaked upstream trailers: headers=%v trailers=%v", method, resp.Header, resp.Trailer)
		}
		if resp.Header.Get("X-Graph-Debug-Hdr") != "" || resp.Header.Get("X-Graph-Debug") != "" ||
			resp.Header.Get("ETag") != "" {
			t.Fatalf("%s model list leaked upstream extension headers: %v", method, resp.Header)
		}
	}
}

func TestProxyModelListFilterFailureKeepsRequestID(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":"sibling","id":"adapter-a"}]}`))
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	srv := newTestServer(t, upstream)
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	setUpstream(req, upstream)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "deployment-a")
	req.Header.Set(identity.HeaderServedModel, "adapter-a")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
	if id := rr.Header().Get(requestIDHeader); !strings.HasPrefix(id, "phoebe-") {
		t.Fatalf("response request id = %q, want generated correlation id", id)
	}
	if strings.Contains(rr.Body.String(), "sibling") {
		t.Fatalf("error leaked upstream model data: %s", rr.Body.String())
	}
}

func TestDedicatedBoundRouteDoesNotUseSharedWakeProbe(t *testing.T) {
	var upstreamCalls int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&upstreamCalls, 1)
		_, _ = w.Write([]byte(`{"model":"adapter-a","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	waker := &fakeWaker{}
	srv := newTestServer(t, upstream).WithWaker(waker, time.Second, 2)
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		strings.NewReader(`{"model":"adapter-a","messages":[]}`),
	)
	setUpstream(req, upstream)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "deployment-a")
	req.Header.Set(identity.HeaderServedModel, "adapter-a")
	// Empty ServingMode is the dedicated SKU contract.
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got := atomic.LoadInt32(&upstreamCalls); got != 1 {
		t.Fatalf("dedicated inference reached upstream %d times, want 1", got)
	}
	if got := atomic.LoadInt32(&waker.calls); got != 0 {
		t.Fatalf("dedicated inference invoked shared waker %d times, want 0", got)
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
			setUpstream(req, upstream)
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

// TestProxyRequestID_GeneratedWhenAbsent verifies Phoebe mints the billing id
// itself and uses that one value for upstream, response, and metering.
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
	setUpstream(req, upstream)
	req.Header.Del("X-Request-Id")
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

// Reusing a client-controlled correlation id must still produce two distinct
// billing attempts. This is the regression test for served-but-unbilled replay.
func TestProxyRequestID_ClientReplayCannotReuseBillingID(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"model":"m1","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	srv := newTestServerE(t, upstream, em)

	responseIDs := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		setUpstream(req, upstream)
		req.Header.Set(identity.HeaderAuthID, "auth-1")
		req.Header.Set(identity.HeaderResourceID, "model-abc")
		req.Header.Set(requestIDHeader, "client-replayed-id")
		srv.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("attempt %d status = %d, want 200", i, rr.Code)
		}
		got := rr.Header().Get(requestIDHeader)
		if !strings.HasPrefix(got, "phoebe-") || got == "client-replayed-id" {
			t.Fatalf("attempt %d response id = %q, want Phoebe-generated id", i, got)
		}
		responseIDs = append(responseIDs, got)
	}
	events := em.waitForEvents(2, 2*time.Second)
	if len(events) != 2 || events[0].RequestID == events[1].RequestID {
		t.Fatalf("billing ids = %#v, want two distinct Phoebe attempts", events)
	}
	if events[0].RequestID != responseIDs[0] || events[1].RequestID != responseIDs[1] {
		t.Fatalf("event ids do not match response ids: events=%#v responses=%#v", events, responseIDs)
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
	setUpstream(req, upstream)
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

// TestProxyUpstreamHeaderMalformedFailsClosed: a broken trusted header (Atlas injected a
// bad value) is a broken edge contract, not a normal request — fail closed (502), never
// forward to a guessed/empty target.
func TestProxyUpstreamHeaderMalformedFailsClosed(t *testing.T) {
	unused, _ := url.Parse("http://unused")
	// The grammar is strictly host:port with an implicit (or explicit) http scheme.
	// A value with no host, one carrying a path/query/fragment (which
	// NewSingleHostReverseProxy would silently prepend to every request → a
	// whole-deployment 404), or a NON-HTTP scheme (SSRF surface: phoebe would speak
	// that transport to the target) is a broken edge contract → fail closed.
	for _, bad := range []string{
		"://:", // no host, unparseable
		"pd-x.main-namespace.svc.cluster.local:8000/v1",  // path → would double-prefix routes
		"pd-x.main-namespace.svc.cluster.local:8000?a=b", // query
		"pd-x:8000#frag", // fragment
		"https://pd-x.main-namespace.svc.cluster.local:8000", // non-http scheme (TLS to a plaintext engine)
		"ftp://pd-x:8000", // non-http scheme
	} {
		t.Run(bad, func(t *testing.T) {
			srv := newTestServer(t, unused)
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			req.Header.Set(identity.HeaderAuthID, "auth-1")
			req.Header.Set(identity.HeaderResourceID, "r")
			req.Header.Set(identity.HeaderUpstream, bad)
			srv.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusBadGateway {
				t.Fatalf("upstream %q: got %d, want 502 (non-bare-host:port must fail closed)", bad, rr.Code)
			}
		})
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
		setUpstream(req, upstream)
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
		setUpstream(req, upstream)
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

// TestProxyNoUpstreamFailsClosed verifies the fail-closed routing contract: a
// request that passes the billing-identity gate but carries NO X-Saturn-Upstream
// header has no forward target, so phoebe must refuse it (502) rather than invent
// a default — there is no resolver and no fallback upstream anymore.
func TestProxyNoUpstreamFailsClosed(t *testing.T) {
	s := &config.Settings{ListenAddr: ":0"}
	log := logging.New(logging.ERROR)
	em := &recordingEmitter{}
	srv := New(s, log, em)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "gone")
	// Deliberately NO X-Saturn-Upstream header.
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("missing upstream: got %d, want 502 (fail closed, never forward to a default)", rr.Code)
	}
	if got := em.count(); got != 0 {
		t.Fatalf("refused (no-upstream) request emitted %d billing events, want 0", got)
	}
}

// parseUpstreamHeader is the routing-authority parse boundary; it must accept
// exactly an in-cluster engine host:port and reject everything else (fail closed).
// Tested by attack: non-http scheme (SSRF transport), path/query (request-path
// smuggling via ReverseProxy path-join), and malformed/empty input.
func TestParseUpstreamHeader(t *testing.T) {
	accept := []struct{ in, wantURL string }{
		{"engine.svc:8000", "http://engine.svc:8000"},
		{"pd-a.main-namespace.svc.cluster.local:8000", "http://pd-a.main-namespace.svc.cluster.local:8000"},
		{"http://engine.svc:8000", "http://engine.svc:8000"},
		{"  engine.svc:8000  ", "http://engine.svc:8000"}, // trimmed
	}
	for _, c := range accept {
		u, err := parseUpstreamHeader(c.in)
		if err != nil {
			t.Errorf("parseUpstreamHeader(%q): unexpected error %v", c.in, err)
			continue
		}
		if u.String() != c.wantURL {
			t.Errorf("parseUpstreamHeader(%q) = %q, want %q", c.in, u.String(), c.wantURL)
		}
	}

	reject := []string{
		"",                            // empty
		"   ",                         // whitespace only
		"https://engine.svc:8000",     // non-http scheme (would TLS to a plain-HTTP engine)
		"gopher://host:70",            // exotic scheme
		"ftp://host:21",               // exotic scheme
		"engine.svc:8000/foo",         // path smuggling (ReverseProxy prepends it)
		"http://engine.svc:8000/foo",  // path smuggling, explicit scheme
		"engine.svc:8000?k=v",         // query smuggling
		"http://engine.svc:8000#frag", // fragment
		"http://user@engine.svc:8000", // userinfo
		"http://",                     // hostless
	}
	for _, in := range reject {
		if u, err := parseUpstreamHeader(in); err == nil {
			t.Errorf("parseUpstreamHeader(%q) = %q, want error (fail closed)", in, u.String())
		}
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
	setUpstream(req, upstream)
	req.Header.Set(identity.HeaderResourceID, "model-abc")
	req.Header.Set(identity.HeaderResourceType, "deployment")
	req.Header.Set(identity.HeaderGroupID, "org-1")
	req.Header.Set(identity.HeaderUserID, "user-1")
	req.Header.Set(identity.HeaderAuthID, "auth-key-7")
	// The two per-deployment C4 headers the Atlas-rendered Traefik middleware
	// injects: the catalog price key and the fine-tune checkpoint artifact id.
	req.Header.Set(identity.HeaderBaseModel, "meta-llama/Llama-3.1-8B-Instruct")
	req.Header.Set(identity.HeaderAdapter, "ckpt-artifact-42")
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
	if !strings.HasPrefix(e.RequestID, "phoebe-") || e.RequestID == "req-123" || e.GroupID != "org-1" || e.UserID != "user-1" {
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
	// BaseModel and Adapter are the C4 pricing seam: the trusted per-deployment
	// headers must ride onto the metering event VERBATIM — base_model is the
	// catalog price key and adapter presence is the fine-tune premium trigger.
	// Dropping either silently mis-prices every endpoint-name event downstream.
	if e.BaseModel != "meta-llama/Llama-3.1-8B-Instruct" {
		t.Fatalf("event BaseModel = %q, want the X-Saturn-Base-Model header value", e.BaseModel)
	}
	if e.Adapter != "ckpt-artifact-42" {
		t.Fatalf("event Adapter = %q, want the X-Saturn-Adapter header value", e.Adapter)
	}
	if e.FinishReason != "stop" {
		t.Fatalf("event finish_reason = %q, want stop", e.FinishReason)
	}
}

// Documents an INTENTIONAL fail-open: a non-gateway route that reaches phoebe
// with X-Saturn-Served-Model ABSENT gets none of the three protections keyed on
// that header — no route gate, no body binding, no /v1/models filter — so it
// forwards Dynamo's graph-wide surface. That is tolerable only because the
// header is injected and anti-spoof overwritten server-side by the
// Atlas-rendered Traefik middleware, so a client cannot cause its absence.
// Pinned by name so any future change to it is deliberate rather than silent.
func TestProxyUnboundRouteSkipsAllServedModelGates(t *testing.T) {
	var upstreamCalls int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&upstreamCalls, 1)
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"adapter-b"}]}`))
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	srv := newTestServer(t, upstream)

	request := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		setUpstream(req, upstream)
		req.Header.Set(identity.HeaderAuthID, "auth-1")
		req.Header.Set(identity.HeaderResourceID, "deployment-a")
		// deliberately NO identity.HeaderServedModel
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, req)
		return rr
	}

	if rr := request(http.MethodPost, "/v1/chat/completions", `{"model":"anything"}`); rr.Code != http.StatusOK {
		t.Fatalf("unbound POST status = %d, want 200 (binding not enforced without the header)", rr.Code)
	}
	if rr := request(http.MethodGet, "/metrics", ""); rr.Code == http.StatusNotFound {
		t.Fatal("unbound GET /metrics is NOT route-gated today; update this test deliberately if that changes")
	}
	if atomic.LoadInt32(&upstreamCalls) != 2 {
		t.Fatalf("unbound requests made %d upstream calls, want 2", atomic.LoadInt32(&upstreamCalls))
	}
	// OPTIONS is the one gate that is NOT keyed on the header: it must be
	// answered locally even on an unbound route, so no Dynamo response data
	// can reach the caller through preflight.
	if rr := request(http.MethodOptions, "/metrics", ""); rr.Code != http.StatusNoContent {
		t.Fatalf("unbound OPTIONS status = %d, want 204 (answered locally)", rr.Code)
	}
	if atomic.LoadInt32(&upstreamCalls) != 2 {
		t.Fatalf("OPTIONS reached Dynamo: %d upstream calls", atomic.LoadInt32(&upstreamCalls))
	}
}

// A PRESENT but empty-parsing allow-list authorizes no model at all and must
// fail CLOSED at the route gate — before the request reaches Dynamo — not late
// inside the response filter, which made the request hit the graph and surfaced
// as an opaque 502. Either way no sibling name may appear in the body.
func TestBoundRouteWithUnparseableAllowListFailsClosed(t *testing.T) {
	var upstreamCalls int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&upstreamCalls, 1)
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"adapter-b"},{"id":"base-internal"}]}`))
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	srv := newTestServer(t, upstream)

	request := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		setUpstream(req, upstream)
		req.Header.Set(identity.HeaderAuthID, "auth-1")
		req.Header.Set(identity.HeaderResourceID, "deployment-a")
		req.Header.Set(identity.HeaderServedModel, " ,")
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, req)
		return rr
	}

	if rr := request(http.MethodPost, "/v1/chat/completions", `{"model":"adapter-b"}`); rr.Code != http.StatusForbidden {
		t.Fatalf("POST with empty-parsing allow-list = %d, want 403", rr.Code)
	}
	rr := request(http.MethodGet, "/v1/models", "")
	if rr.Code != http.StatusNotFound {
		t.Fatalf("GET /v1/models with empty-parsing allow-list = %d, want 404 at the route gate", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "adapter-b") || strings.Contains(rr.Body.String(), "base-internal") {
		t.Fatalf("empty allow-list disclosed graph-wide names: %s", rr.Body.String())
	}
	if n := atomic.LoadInt32(&upstreamCalls); n != 0 {
		t.Fatalf("empty-allow-list requests reached Dynamo (%d calls)", n)
	}
}

// Dynamo's /health and /live are GRAPH-WIDE: they enumerate the whole graph's
// components and workers, i.e. sibling tenants' attached adapters. On a
// deployment-scoped subdomain they stay routable but must disclose nothing
// beyond the status class, or readiness re-opens the enumeration /v1/models was
// hardened against.
func TestProxySanitizesBoundReadinessResponses(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", "graph-wide")
		w.Header().Set("X-Graph-Debug", "adapter-b")
		_, _ = w.Write([]byte(`{"components":[{"model":"org/sibling","workers":2}],"instances":["base-internal"]}`))
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	srv := newTestServer(t, upstream)

	for _, path := range []string{"/health", "/live"} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			req := httptest.NewRequest(method, path, nil)
			setUpstream(req, upstream)
			req.Header.Set(identity.HeaderAuthID, "auth-1")
			req.Header.Set(identity.HeaderResourceID, "deployment-a")
			req.Header.Set(identity.HeaderServedModel, "adapter-a")
			rr := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("%s %s status = %d, want 200 (liveness stays truthful)", method, path, rr.Code)
			}
			body := rr.Body.String()
			if strings.Contains(body, "sibling") || strings.Contains(body, "base-internal") ||
				strings.Contains(body, "components") {
				t.Fatalf("%s %s disclosed graph-wide readiness: %s", method, path, body)
			}
			if rr.Header().Get("ETag") != "" || rr.Header().Get("X-Graph-Debug") != "" {
				t.Fatalf("%s %s leaked upstream headers: %v", method, path, rr.Header())
			}
			if method == http.MethodGet && body != `{"status":"ok"}` {
				t.Fatalf("GET %s body = %s, want status-only document", path, body)
			}
		}
	}
}

// Phoebe emits NO Access-Control-* headers on the gateway preflight: browser-
// origin clients are not a supported gateway client (Atlas proxies them
// server-side), so the 204 exists only so a preflight does not 404. Pinned so
// the non-support stays a decision rather than an accident — adding a
// permissive allow-origin would be a security-posture change.
func TestGatewayPreflightEmitsNoCORSHeaders(t *testing.T) {
	srv := newTestServer(t, &url.URL{Scheme: "http", Host: "localhost:1"})
	req := httptest.NewRequest(http.MethodOptions, "/v1/chat/completions", nil)
	req.Header.Set(identity.HeaderGateway, "true")
	req.Header.Set(identity.HeaderOrgID, "org-1")
	req.Header.Set("Origin", "https://example.test")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("gateway preflight status = %d, want 204", rr.Code)
	}
	for k := range rr.Header() {
		if strings.HasPrefix(http.CanonicalHeaderKey(k), "Access-Control-") {
			t.Fatalf("gateway preflight emitted %s; browser clients are not supported here "+
				"and a permissive origin would be a security-posture change", k)
		}
	}
}

// The errorHandler's no-double-emit invariant: a ModifyResponse fault is NOT a
// client abort, so it releases the admission lease but writes no bogus
// zero-token billing row. Named for the invariant it guards, because the
// comment that used to justify it ("ModifyResponse always returns nil") is no
// longer true.
func TestModelListFilterErrorEmitsNoBillingEvent(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":"sibling","id":"adapter-a"}]}`))
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	srv := newTestServerE(t, upstream, em)
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	setUpstream(req, upstream)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "deployment-a")
	req.Header.Set(identity.HeaderServedModel, "adapter-a")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
	if events := em.waitForEvents(1, 100*time.Millisecond); len(events) != 0 {
		t.Fatalf("filter failure emitted %d billing events, want 0: %+v", len(events), events)
	}
}
