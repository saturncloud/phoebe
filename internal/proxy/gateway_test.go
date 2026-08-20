package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

// mapResolver is a fake gateway.Resolver keyed on (org, model). A missing key
// is ErrNotFound; err (when set) trumps everything (the DB-down case).
type mapResolver struct {
	calls int32
	m     map[[2]string]gateway.Resolution
	err   error
}

func (f *mapResolver) Resolve(_ context.Context, org, model string) (gateway.Resolution, error) {
	atomic.AddInt32(&f.calls, 1)
	if f.err != nil {
		return gateway.Resolution{}, f.err
	}
	if r, ok := f.m[[2]string{org, model}]; ok {
		return r, nil
	}
	return gateway.Resolution{}, gateway.ErrNotFound
}

// newGatewayTestServer builds a Server with the gateway configured and — the
// test seam — every resolved upstream pointed at the given backend, standing in
// for the production <graph>-frontend.<ns>.svc.cluster.local:<port> host that a
// unit test cannot dial. The graph name still reaches upstreamFor, so
// TestGateway_UpstreamHostShape covers the production composition separately.
func newGatewayTestServer(t *testing.T, em *recordingEmitter, resolver gateway.Resolver, backend *url.URL) *Server {
	t.Helper()
	s := New(&config.Settings{ListenAddr: ":0"}, logging.New(logging.ERROR), em).
		WithGateway(resolver, "tf-shared", 8000)
	if backend != nil {
		s.gateway.upstreamFor = func(string) string { return backend.Host }
	}
	return s
}

// gatewayRequest builds a gateway-marked request: the trusted middleware
// markers (X-Saturn-Gateway, X-Saturn-Org-Id) plus the auth id every billed
// request carries — and NONE of the per-resource routing headers, exactly as
// the gateway route contract specifies.
func gatewayRequest(org, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set(identity.HeaderGateway, "true")
	if org != "" {
		req.Header.Set(identity.HeaderOrgID, org)
	}
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	return req
}

// usageBackend returns an httptest server answering like a vLLM engine (model
// name + usage block), so the full metering path runs.
func usageBackend(t *testing.T) (*httptest.Server, *url.URL) {
	t.Helper()
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"model":"served-m","choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`))
	}))
	u, _ := url.Parse(be.URL)
	return be, u
}

// TestGateway_ResolvesBaseModel is the happy path for a BASE-MODEL row: the
// request forwards to the resolved upstream and the emitted billing event
// carries the tf_model row's identity — indistinguishable from a
// header-injected one (resource id, base model, serving mode, empty adapter,
// org).
func TestGateway_ResolvesBaseModel(t *testing.T) {
	be, beURL := usageBackend(t)
	defer be.Close()

	resolver := &mapResolver{m: map[[2]string]gateway.Resolution{
		{"org-1", "meta-llama/Llama-3.1-8B-Instruct"}: {
			ResourceID:   "tfm-base-1",
			BaseModel:    "meta-llama/Llama-3.1-8B-Instruct",
			ServingMode:  "shared",
			GraphK8sName: "graph-llama31",
		},
	}}
	em := &recordingEmitter{}
	srv := newGatewayTestServer(t, em, resolver, beURL)

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, gatewayRequest("org-1", `{"model":"meta-llama/Llama-3.1-8B-Instruct"}`))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rr.Code, rr.Body.String())
	}
	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want 1", len(events))
	}
	e := events[0]
	if e.ResourceID != "tfm-base-1" {
		t.Fatalf("event.ResourceID = %q, want the tf_model row id", e.ResourceID)
	}
	if e.BaseModel != "meta-llama/Llama-3.1-8B-Instruct" || e.ServingMode != "shared" || e.Adapter != "" {
		t.Fatalf("event pricing identity wrong: base=%q mode=%q adapter=%q", e.BaseModel, e.ServingMode, e.Adapter)
	}
	if e.OrgID != "org-1" {
		t.Fatalf("event.OrgID = %q, want org-1", e.OrgID)
	}
	if e.PromptTokens != 7 || e.CompletionTokens != 3 {
		t.Fatalf("usage not metered: %+v", e)
	}
}

// TestGateway_ResolvesFineTune: an adapter row's checkpoint id rides the event
// (the fine-tune premium trigger at rating).
func TestGateway_ResolvesFineTune(t *testing.T) {
	be, beURL := usageBackend(t)
	defer be.Close()

	resolver := &mapResolver{m: map[[2]string]gateway.Resolution{
		{"org-1", "support-bot"}: {
			ResourceID:   "tfm-ft-1",
			BaseModel:    "meta-llama/Llama-3.1-8B-Instruct",
			Adapter:      "ckpt-42",
			ServingMode:  "shared",
			GraphK8sName: "graph-llama31",
		},
	}}
	em := &recordingEmitter{}
	srv := newGatewayTestServer(t, em, resolver, beURL)

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, gatewayRequest("org-1", `{"model":"support-bot"}`))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rr.Code, rr.Body.String())
	}
	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want 1", len(events))
	}
	if e := events[0]; e.Adapter != "ckpt-42" || e.ResourceID != "tfm-ft-1" {
		t.Fatalf("fine-tune identity wrong: %+v", e)
	}
}

// TestGateway_UnknownModel404NoEcho: an unresolvable model is a generic 404 —
// the body must not echo the attempted model (no oracle for probing which
// models exist) — and nothing is billed.
func TestGateway_UnknownModel404NoEcho(t *testing.T) {
	resolver := &mapResolver{m: map[[2]string]gateway.Resolution{}}
	em := &recordingEmitter{}
	srv := newGatewayTestServer(t, em, resolver, nil)

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, gatewayRequest("org-1", `{"model":"super-secret-probe"}`))

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "super-secret-probe") {
		t.Fatalf("404 body echoes the attempted model: %q", rr.Body.String())
	}
	if em.count() != 0 {
		t.Fatalf("unresolved request must not bill, emitted %d", em.count())
	}
}

// TestGateway_CrossTenantModel404: org B naming org A's model gets the SAME
// generic 404 as a nonexistent model — the org filter is the tenancy boundary,
// and a cross-tenant probe learns nothing.
func TestGateway_CrossTenantModel404(t *testing.T) {
	resolver := &mapResolver{m: map[[2]string]gateway.Resolution{
		{"org-a", "victim-bot"}: {
			ResourceID: "tfm-a", BaseModel: "b", ServingMode: "shared", GraphK8sName: "g",
		},
	}}
	em := &recordingEmitter{}
	srv := newGatewayTestServer(t, em, resolver, nil)

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, gatewayRequest("org-b", `{"model":"victim-bot"}`))

	if rr.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant status = %d, want 404", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "victim-bot") {
		t.Fatalf("cross-tenant 404 echoes the model: %q", rr.Body.String())
	}
}

// TestGateway_MissingOrg403: a gateway-marked request without X-Saturn-Org-Id
// means the edge contract is broken — 403 before any resolution is attempted.
func TestGateway_MissingOrg403(t *testing.T) {
	resolver := &mapResolver{}
	srv := newGatewayTestServer(t, &recordingEmitter{}, resolver, nil)

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, gatewayRequest("", `{"model":"m"}`))

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rr.Code)
	}
	if atomic.LoadInt32(&resolver.calls) != 0 {
		t.Fatal("resolver must not be consulted without an org")
	}
}

// TestGateway_HeaderAbsent_LegacyPathUnchanged: with the gateway CONFIGURED, a
// request without the gateway marker takes today's header-routed path exactly
// — forwarded via X-Saturn-Upstream without ever consulting the resolver, and
// still failing closed (400 missing identity) without the legacy headers.
func TestGateway_HeaderAbsent_LegacyPathUnchanged(t *testing.T) {
	be, beURL := usageBackend(t)
	defer be.Close()

	resolver := &mapResolver{}
	em := &recordingEmitter{}
	srv := newGatewayTestServer(t, em, resolver, beURL)

	// Legacy request: full identity + upstream header, no gateway marker.
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "dep-1")
	setUpstream(req, beURL)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("legacy request status = %d, want 200", rr.Code)
	}

	// Legacy fail-closed unchanged: no upstream header → 502, no resolution.
	req2 := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	req2.Header.Set(identity.HeaderAuthID, "auth-1")
	req2.Header.Set(identity.HeaderResourceID, "dep-1")
	rr2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusBadGateway {
		t.Fatalf("legacy no-upstream status = %d, want 502 (fail closed)", rr2.Code)
	}

	if atomic.LoadInt32(&resolver.calls) != 0 {
		t.Fatal("resolver must never run for non-gateway requests")
	}
}

// TestGateway_DBDown503: a resolver failure (DB unreachable) is 503 — phoebe
// never serves traffic it cannot attribute — and bills nothing.
func TestGateway_DBDown503(t *testing.T) {
	resolver := &mapResolver{err: errors.New("dial tcp: connection refused")}
	em := &recordingEmitter{}
	srv := newGatewayTestServer(t, em, resolver, nil)

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, gatewayRequest("org-1", `{"model":"m"}`))

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if em.count() != 0 {
		t.Fatalf("failed resolution must not bill, emitted %d", em.count())
	}
}

// TestGateway_DuplicateModelKeys400: duplicate top-level "model" keys are
// rejected BEFORE resolution — the same ambiguity rejection as the binding
// path (phoebe must not resolve one duplicate while Dynamo routes the other).
func TestGateway_DuplicateModelKeys400(t *testing.T) {
	resolver := &mapResolver{m: map[[2]string]gateway.Resolution{
		{"org-1", "mine"}: {ResourceID: "tfm-1", BaseModel: "b", GraphK8sName: "g"},
	}}
	srv := newGatewayTestServer(t, &recordingEmitter{}, resolver, nil)

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, gatewayRequest("org-1", `{"model":"victim","model":"mine"}`))

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if atomic.LoadInt32(&resolver.calls) != 0 {
		t.Fatal("an ambiguous body must not reach the resolver")
	}
}

// TestGateway_MissingModel400: a body with no model= (or no body) cannot be
// resolved — 400, resolver untouched.
func TestGateway_MissingModel400(t *testing.T) {
	resolver := &mapResolver{}
	srv := newGatewayTestServer(t, &recordingEmitter{}, resolver, nil)

	for _, body := range []string{`{}`, ``, `not json`} {
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, gatewayRequest("org-1", body))
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("body %q: status = %d, want 400", body, rr.Code)
		}
	}
	if atomic.LoadInt32(&resolver.calls) != 0 {
		t.Fatal("unreadable bodies must not reach the resolver")
	}
}

// TestGateway_Unconfigured503FailClosed: a gateway-marked request on an
// interceptor with NO gateway configured is refused 503 — it never falls
// through to header routing (which it cannot satisfy) or a guessed route.
func TestGateway_Unconfigured503FailClosed(t *testing.T) {
	srv := New(&config.Settings{ListenAddr: ":0"}, logging.New(logging.ERROR), &recordingEmitter{})

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, gatewayRequest("org-1", `{"model":"m"}`))

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (fail closed)", rr.Code)
	}
}

// TestWithGateway_PartialWiringFailsClosed: WithGateway with a nil resolver or
// empty namespace must leave the gateway OFF (503), never compose upstreams
// into a guessed namespace.
func TestWithGateway_PartialWiringFailsClosed(t *testing.T) {
	base := func() *Server {
		return New(&config.Settings{ListenAddr: ":0"}, logging.New(logging.ERROR), &recordingEmitter{})
	}
	if s := base().WithGateway(nil, "ns", 8000); s.gateway != nil {
		t.Fatal("nil resolver must leave gateway unconfigured")
	}
	if s := base().WithGateway(&mapResolver{}, "", 8000); s.gateway != nil {
		t.Fatal("empty namespace must leave gateway unconfigured")
	}
}

// TestGateway_UpstreamHostShape pins the production upstream composition:
// <graph>-frontend.<namespace>.svc.cluster.local:<port>, with port defaulting
// to 8000 when unset.
func TestGateway_UpstreamHostShape(t *testing.T) {
	s := New(&config.Settings{ListenAddr: ":0"}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithGateway(&mapResolver{}, "tf-shared", 0)
	got := s.gateway.upstreamFor("graph-llama31")
	want := "graph-llama31-frontend.tf-shared.svc.cluster.local:8000"
	if got != want {
		t.Fatalf("upstreamFor = %q, want %q", got, want)
	}
	if _, err := parseUpstreamHeader(got); err != nil {
		t.Fatalf("composed upstream must satisfy the upstream grammar: %v", err)
	}
}

// TestGateway_WakeEligible: a resolved gateway route is wake-eligible —
// resolution succeeded IS the wakeability signal (ResourceID + ServedModel are
// populated by resolveGateway) — so a cold (scaled-to-zero) upstream triggers
// the waker and the request is served after warm-up rather than 404ing.
func TestGateway_WakeEligible(t *testing.T) {
	backend := &coldToWarmBackend{}
	be := httptest.NewServer(backend)
	defer be.Close()
	beURL, _ := url.Parse(be.URL)

	resolver := &mapResolver{m: map[[2]string]gateway.Resolution{
		{"org-1", "sleepy-bot"}: {
			ResourceID:   "tfm-cold-1",
			BaseModel:    "meta-llama/Llama-3.1-8B-Instruct",
			ServingMode:  "shared",
			GraphK8sName: "graph-llama31",
		},
	}}
	waker := &fakeWaker{warmsAt: 1, backend: backend}
	em := &recordingEmitter{}
	srv := newGatewayTestServer(t, em, resolver, beURL).WithWaker(waker, 5*time.Second, 3)

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, gatewayRequest("org-1", `{"model":"sleepy-bot"}`))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 after wake (body %q)", rr.Code, rr.Body.String())
	}
	if got := atomic.LoadInt32(&waker.calls); got != 1 {
		t.Fatalf("waker called %d times, want 1 (gateway route must be wakeable)", got)
	}
	// The RESOLVED graph name is threaded onto the wake target verbatim —
	// never re-derived from the upstream host the gateway composed from it.
	if tgt := waker.last(); tgt.GraphK8sName != "graph-llama31" || tgt.ResourceID != "tfm-cold-1" {
		t.Fatalf("wake target = %+v, want the resolved graph/resource", tgt)
	}
}
