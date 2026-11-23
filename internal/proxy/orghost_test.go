package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

// fakeTierResolver serves both tiers from memory, like the registry: shared
// rows through Resolve, dedicated rows through ResolveDedicated.
type fakeTierResolver struct {
	shared    map[[2]string]gateway.Resolution
	dedicated []gateway.DedicatedEndpoint
	err       error
}

func (f *fakeTierResolver) Resolve(_ context.Context, org, model string) (gateway.Resolution, error) {
	if f.err != nil {
		return gateway.Resolution{}, f.err
	}
	if r, ok := f.shared[[2]string{org, model}]; ok {
		return r, nil
	}
	return gateway.Resolution{}, gateway.ErrNotFound
}

func (f *fakeTierResolver) ResolveDedicated(_ context.Context, org, model string) (gateway.DedicatedEndpoint, error) {
	if f.err != nil {
		return gateway.DedicatedEndpoint{}, f.err
	}
	for _, ep := range f.dedicated {
		if ep.OrgID == org && ep.Name == model {
			return ep, nil
		}
	}
	for _, ep := range f.dedicated {
		if ep.OrgID != org {
			continue
		}
		for _, a := range ep.Aliases {
			if a == model {
				return ep, nil
			}
		}
	}
	return gateway.DedicatedEndpoint{}, gateway.ErrNotFound
}

func (f *fakeTierResolver) ListDedicated(_ context.Context, org string) ([]gateway.DedicatedEndpoint, error) {
	if f.err != nil {
		return nil, f.err
	}
	var out []gateway.DedicatedEndpoint
	for _, ep := range f.dedicated {
		if ep.OrgID == org {
			out = append(out, ep)
		}
	}
	return out, nil
}

const testSharedBaseURL = "https://inference.example.com/v1"

func newOrgHostTestServer(t *testing.T, em *recordingEmitter, res *fakeTierResolver, sharedBackend *url.URL) *Server {
	t.Helper()
	if em == nil {
		em = &recordingEmitter{}
	}
	s := New(&config.Settings{ListenAddr: ":0"}, logging.New(logging.ERROR), em).
		WithGateway(res, "tf-shared", 8000).
		WithOrgHosts(res, testSharedBaseURL)
	if sharedBackend != nil {
		s.gateway.upstreamFor = func(string, int) string { return sharedBackend.Host }
	}
	return s
}

// orgHostRequest is a request as the tf-org-hosts route delivers it: the
// org-host mark, the host's org, the key; no per-route stamps.
func orgHostRequest(method, path, org, body string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set(identity.HeaderOrgHost, "true")
	if org != "" {
		req.Header.Set(identity.HeaderOrgID, org)
	}
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderOwnerID, "owner-1")
	req.Header.Set("Authorization", "Bearer sk-test")
	req.Header.Set("Content-Type", "application/json")
	return req
}

func chat(model string) string {
	return `{"model":"` + model + `","messages":[{"role":"user","content":"hi"}]}`
}

// orgEngine answers like a vLLM engine with usage and records requests.
type orgEngine struct {
	hits    atomic.Int32
	mu      sync.Mutex
	headers []http.Header
	bodies  [][]byte
	paths   []string
	status  int
	body    string
}

func (b *orgEngine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.hits.Add(1)
	body, _ := io.ReadAll(r.Body)
	b.mu.Lock()
	b.headers = append(b.headers, r.Header.Clone())
	b.bodies = append(b.bodies, body)
	b.paths = append(b.paths, r.URL.RequestURI())
	b.mu.Unlock()
	status := b.status
	if status == 0 {
		status = http.StatusOK
	}
	out := b.body
	if out == "" {
		out = `{"model":"m","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2}}`
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(out))
}

func dedicatedRow(org, name, resource, upstream string) gateway.DedicatedEndpoint {
	return gateway.DedicatedEndpoint{
		OrgID: org, Name: name, ResourceID: resource, BaseModel: "Qwen/Qwen3-8B",
		Upstream: upstream, ServingProfile: "aggregated", State: gateway.EndpointStateRunning,
		Cluster: "local", OrgHostURL: "https://" + org + "-inference.example.com/v1",
	}
}

// --- tier resolution and 404 rules (design 3.2, 4.4) ---

func TestOrgHostResolvesOnlyDedicatedRows(t *testing.T) {
	be := &orgEngine{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)
	em := &recordingEmitter{}
	res := &fakeTierResolver{
		dedicated: []gateway.DedicatedEndpoint{dedicatedRow("org-1", "my-endpoint", "dep-1", up.Host)},
		shared: map[[2]string]gateway.Resolution{
			{"org-1", "shared-model"}: {ResourceID: "tfm-1", BaseModel: "b", ServingMode: "shared", GraphK8sName: "g"},
		},
	}
	s := newOrgHostTestServer(t, em, res, up)

	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("my-endpoint")))
	if rr.Code != http.StatusOK {
		t.Fatalf("dedicated on org host: status %d body %s", rr.Code, rr.Body.String())
	}
	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 || events[0].ResourceID != "dep-1" || events[0].ServingMode != identity.ServingModeDedicated ||
		events[0].BaseModel != "Qwen/Qwen3-8B" || events[0].OrgID != "org-1" {
		t.Fatalf("metering event = %+v, want the row's identity", events)
	}
	// No X-Saturn-* header reaches the engine.
	for name := range be.headers[0] {
		if strings.HasPrefix(strings.ToLower(name), "x-saturn") {
			t.Fatalf("engine received %s", name)
		}
	}

	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("shared-model")))
	if rr.Code != http.StatusNotFound || be.hits.Load() != 1 {
		t.Fatalf("shared model on org host: status %d, engine hits %d", rr.Code, be.hits.Load())
	}
}

func TestOrgHostSharedNameIsWrongEndpointHost(t *testing.T) {
	res := &fakeTierResolver{shared: map[[2]string]gateway.Resolution{
		{"org-1", "llama-shared"}: {ResourceID: "tfm-1", BaseModel: "b", ServingMode: "shared", GraphK8sName: "g"},
	}}
	s := newOrgHostTestServer(t, nil, res, nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("llama-shared")))
	e := errorBody(t, rr.Body.Bytes())
	if rr.Code != http.StatusNotFound || e.Code != "wrong_endpoint_host" || e.Type != "invalid_request_error" ||
		!strings.Contains(e.Message, testSharedBaseURL) {
		t.Fatalf("status %d body %s, want 404 wrong_endpoint_host naming the shared URL", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "llama-shared") {
		t.Fatalf("body echoes the model: %s", rr.Body.String())
	}
}

func TestSharedHostOwnOrgDedicatedNameIsWrongEndpointHost(t *testing.T) {
	res := &fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{dedicatedRow("org-1", "my-endpoint", "dep-1", "x:1")}}
	s := newOrgHostTestServer(t, nil, res, nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, gatewayRequest("org-1", chat("my-endpoint")))
	e := errorBody(t, rr.Body.Bytes())
	if rr.Code != http.StatusNotFound || e.Code != "wrong_endpoint_host" ||
		!strings.Contains(e.Message, "https://org-1-inference.example.com/v1") {
		t.Fatalf("status %d body %s, want 404 wrong_endpoint_host naming the org host", rr.Code, rr.Body.String())
	}
}

// Another org's model is the same lookup miss as a typo: same status, code
// and body bytes, on both hosts.
func TestOtherOrgsModelIsIdenticalToUnknownOnBothHosts(t *testing.T) {
	res := &fakeTierResolver{
		dedicated: []gateway.DedicatedEndpoint{dedicatedRow("org-2", "theirs", "dep-2", "x:1")},
		shared: map[[2]string]gateway.Resolution{
			{"org-2", "their-shared"}: {ResourceID: "tfm-2", BaseModel: "b", ServingMode: "shared", GraphK8sName: "g"},
		},
	}
	s := newOrgHostTestServer(t, nil, res, nil)
	for _, tc := range []struct {
		name      string
		other     *http.Request
		typo      *http.Request
		otherName string
	}{
		{"org host, other org's dedicated", orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("theirs")),
			orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("nope")), "theirs"},
		{"org host, other org's shared", orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("their-shared")),
			orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("nope")), "their-shared"},
		{"shared host, other org's dedicated", gatewayRequest("org-1", chat("theirs")), gatewayRequest("org-1", chat("nope")), "theirs"},
		{"shared host, other org's shared", gatewayRequest("org-1", chat("their-shared")), gatewayRequest("org-1", chat("nope")), "their-shared"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := httptest.NewRecorder(), httptest.NewRecorder()
			s.Handler().ServeHTTP(a, tc.other)
			s.Handler().ServeHTTP(b, tc.typo)
			if a.Code != http.StatusNotFound || a.Code != b.Code || !bytes.Equal(a.Body.Bytes(), b.Body.Bytes()) {
				t.Fatalf("other org: %d %s; typo: %d %s — want identical 404s", a.Code, a.Body.String(), b.Code, b.Body.String())
			}
			if errorCode(t, a.Body.Bytes()) != "model_not_found" || strings.Contains(a.Body.String(), tc.otherName) {
				t.Fatalf("body %s", a.Body.String())
			}
		})
	}
}

func TestOrgHostUnknownModelDoesNotEchoTheName(t *testing.T) {
	s := newOrgHostTestServer(t, nil, &fakeTierResolver{}, nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("super-secret-probe")))
	e := errorBody(t, rr.Body.Bytes())
	if rr.Code != http.StatusNotFound || e.Code != "model_not_found" || e.Message != orgHostModelNotFound.Message {
		t.Fatalf("status %d body %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "super-secret-probe") {
		t.Fatal("404 echoes the model")
	}
}

func TestNameThatIsBothDedicatedAndSharedServesThisHostsTier(t *testing.T) {
	dedicatedBE := &orgEngine{}
	dsrv := httptest.NewServer(dedicatedBE)
	defer dsrv.Close()
	dup, _ := url.Parse(dsrv.URL)
	sharedBE := &orgEngine{}
	ssrv := httptest.NewServer(sharedBE)
	defer ssrv.Close()
	sup, _ := url.Parse(ssrv.URL)
	res := &fakeTierResolver{
		dedicated: []gateway.DedicatedEndpoint{dedicatedRow("org-1", "both", "dep-1", dup.Host)},
		shared: map[[2]string]gateway.Resolution{
			{"org-1", "both"}: {ResourceID: "tfm-1", BaseModel: "b", ServingMode: "shared", GraphK8sName: "g"},
		},
	}
	s := newOrgHostTestServer(t, nil, res, sup)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("both")))
	if rr.Code != http.StatusOK || dedicatedBE.hits.Load() != 1 || sharedBE.hits.Load() != 0 {
		t.Fatalf("org host: %d dedicated=%d shared=%d", rr.Code, dedicatedBE.hits.Load(), sharedBE.hits.Load())
	}
	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, gatewayRequest("org-1", chat("both")))
	if rr.Code != http.StatusOK || dedicatedBE.hits.Load() != 1 || sharedBE.hits.Load() != 1 {
		t.Fatalf("shared host: %d %s dedicated=%d shared=%d", rr.Code, rr.Body.String(), dedicatedBE.hits.Load(), sharedBE.hits.Load())
	}
}

func TestOrgHostStoppedEndpointIs409(t *testing.T) {
	be := &orgEngine{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)
	row := dedicatedRow("org-1", "my-endpoint", "dep-1", up.Host)
	row.State = gateway.EndpointStateStopped
	s := newOrgHostTestServer(t, nil, &fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{row}}, nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("my-endpoint")))
	if rr.Code != http.StatusConflict || errorCode(t, rr.Body.Bytes()) != "endpoint_not_running" || be.hits.Load() != 0 {
		t.Fatalf("status %d body %s hits %d", rr.Code, rr.Body.String(), be.hits.Load())
	}
}

func TestEndpointScopeRefusesOtherEndpoints(t *testing.T) {
	be := &orgEngine{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)
	stopped := dedicatedRow("org-1", "stopped-one", "dep-3", up.Host)
	stopped.State = gateway.EndpointStateStopped
	res := &fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{
		dedicatedRow("org-1", "mine", "dep-1", up.Host),
		dedicatedRow("org-1", "other", "dep-2", up.Host),
		stopped,
	}}
	s := newOrgHostTestServer(t, nil, res, nil)
	send := func(model string) *httptest.ResponseRecorder {
		req := orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat(model))
		req.Header.Set(identity.HeaderEndpointScope, "dep-1")
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		return rr
	}
	if rr := send("mine"); rr.Code != http.StatusOK {
		t.Fatalf("own endpoint: %d %s", rr.Code, rr.Body.String())
	}
	if rr := send("other"); rr.Code != http.StatusForbidden || errorCode(t, rr.Body.Bytes()) != "model_not_authorized" {
		t.Fatalf("other endpoint: %d %s", rr.Code, rr.Body.String())
	}
	// The scope check comes before the state check: another endpoint's state
	// is not revealed.
	if rr := send("stopped-one"); rr.Code != http.StatusForbidden {
		t.Fatalf("other stopped endpoint: %d %s, want 403", rr.Code, rr.Body.String())
	}
	if be.hits.Load() != 1 {
		t.Fatalf("engine hits %d, want 1", be.hits.Load())
	}
}

func TestEndpointScopeMalformedHeaderRefuses(t *testing.T) {
	be := &orgEngine{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)
	s := newOrgHostTestServer(t, nil, &fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{
		dedicatedRow("org-1", "mine", "dep-1", up.Host),
	}}, nil)
	for _, v := range []string{"", "dep-1,dep-2", "dep 1", strings.Repeat("a", 65)} {
		req := orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("mine"))
		req.Header[identity.HeaderEndpointScope] = []string{v}
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusForbidden {
			t.Fatalf("scope %q: status %d, want 403", v, rr.Code)
		}
	}
	// On a request that is not an org-host request the scope header is an
	// edge-contract break and refuses too.
	req := gatewayRequest("org-1", chat("mine"))
	req.Header.Set(identity.HeaderEndpointScope, "dep-1")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("scope on gateway request: %d", rr.Code)
	}
	if be.hits.Load() != 0 {
		t.Fatalf("engine hits %d", be.hits.Load())
	}
}

func TestOrgHostRefusesGatewayAndOrgHostMarkersTogether(t *testing.T) {
	be := &orgEngine{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)
	s := newOrgHostTestServer(t, nil, &fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{
		dedicatedRow("org-1", "mine", "dep-1", up.Host),
	}}, up)
	for name, extra := range map[string][2]string{
		"gateway":         {identity.HeaderGateway, "true"},
		"video retrieval": {identity.HeaderVideoRetrieval, "true"},
		"served model":    {identity.HeaderServedModel, "mine"},
		"resource id":     {identity.HeaderResourceID, "dep-9"},
		"serving mode":    {identity.HeaderServingMode, "dedicated"},
		"video limits":    {identity.HeaderVideoLimits, "{}"},
	} {
		t.Run(name, func(t *testing.T) {
			req := orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("mine"))
			req.Header.Set(extra[0], extra[1])
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusForbidden {
				t.Fatalf("status %d body %s, want 403", rr.Code, rr.Body.String())
			}
		})
	}
	if be.hits.Load() != 0 {
		t.Fatalf("engine hits %d", be.hits.Load())
	}
}

// A client-supplied upstream never routes: if the edge failed to strip it,
// the request is refused; the row's upstream is the only target.
func TestOrgHostIgnoresClientSuppliedUpstream(t *testing.T) {
	rowBE, evilBE := &orgEngine{}, &orgEngine{}
	rs, es := httptest.NewServer(rowBE), httptest.NewServer(evilBE)
	defer rs.Close()
	defer es.Close()
	rup, _ := url.Parse(rs.URL)
	eup, _ := url.Parse(es.URL)
	s := newOrgHostTestServer(t, nil, &fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{
		dedicatedRow("org-1", "mine", "dep-1", rup.Host),
	}}, nil)
	req := orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("mine"))
	req.Header.Set(identity.HeaderUpstream, eup.Host)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden || evilBE.hits.Load() != 0 {
		t.Fatalf("client upstream: status %d, evil hits %d", rr.Code, evilBE.hits.Load())
	}
	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("mine")))
	if rr.Code != http.StatusOK || rowBE.hits.Load() != 1 || evilBE.hits.Load() != 0 {
		t.Fatalf("row upstream: status %d row=%d evil=%d", rr.Code, rowBE.hits.Load(), evilBE.hits.Load())
	}
}

func TestOrgHostWithoutConfigurationOrOrgFailsClosed(t *testing.T) {
	// No WithOrgHosts.
	s := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{})
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("m")))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured: %d", rr.Code)
	}
	s = newOrgHostTestServer(t, nil, &fakeTierResolver{}, nil)
	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "", chat("m")))
	if rr.Code != http.StatusForbidden || errorCode(t, rr.Body.Bytes()) != "wrong_organization" {
		t.Fatalf("no org: %d %s", rr.Code, rr.Body.String())
	}
	s = newOrgHostTestServer(t, nil, &fakeTierResolver{err: gateway.ErrNotSynced}, nil)
	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("m")))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("not synced: %d", rr.Code)
	}
	// Unknown path and a non-POST on an inference path.
	for _, req := range []*http.Request{
		orgHostRequest(http.MethodGet, "/metrics", "org-1", ""),
		orgHostRequest(http.MethodGet, "/v1/chat/completions", "org-1", ""),
		orgHostRequest(http.MethodGet, "/health", "org-1", ""),
	} {
		rr = httptest.NewRecorder()
		newOrgHostTestServer(t, nil, &fakeTierResolver{}, nil).Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("%s %s: %d", req.Method, req.URL.Path, rr.Code)
		}
	}
}

// With the endpoint-scope header outside the trusted set, a key narrowed to
// one endpoint cannot be recognized, so every org-host request refuses.
func TestOrgHostRefusesWhenEndpointScopeIsNotTrusted(t *testing.T) {
	t.Setenv(identity.TrustedHeadersEnv, strings.Join([]string{
		identity.HeaderGateway, identity.HeaderOrgID, identity.HeaderOwnerID, identity.HeaderServingMode,
		identity.HeaderServingProfile, identity.HeaderServedModel, identity.HeaderOrgHost,
	}, ","))
	identity.LoadTrustedHeaders(logging.New(logging.ERROR))
	t.Cleanup(func() {
		t.Setenv(identity.TrustedHeadersEnv, "")
		identity.LoadTrustedHeaders(logging.New(logging.ERROR))
	})
	s := newOrgHostTestServer(t, nil, &fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{
		dedicatedRow("org-1", "mine", "dep-1", "x:1"),
	}}, nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("mine")))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", rr.Code)
	}
}

func TestOrgHostVideoEndpointServesOnlyTheVideoAPI(t *testing.T) {
	row := dedicatedRow("org-1", "vid", "dep-1", "x:1")
	row.VideoLimits = `{"sizes":["1280x720"]}`
	s := newOrgHostTestServer(t, nil, &fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{row}}, nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("vid")))
	if rr.Code != http.StatusNotFound || errorCode(t, rr.Body.Bytes()) != "video_endpoint" {
		t.Fatalf("status %d body %s", rr.Code, rr.Body.String())
	}
}

func TestOrgHostLegacyAliasServes(t *testing.T) {
	be := &orgEngine{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)
	row := dedicatedRow("org-1", "ft", "dep-1", up.Host)
	row.Aliases = []string{"ft-base"}
	s := newOrgHostTestServer(t, nil, &fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{row}}, nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("ft-base")))
	if rr.Code != http.StatusOK {
		t.Fatalf("alias: %d %s", rr.Code, rr.Body.String())
	}
	var sent struct{ Model string }
	_ = json.Unmarshal(be.bodies[0], &sent)
	if sent.Model != "ft-base" {
		t.Fatalf("engine got model %q, want the alias unchanged", sent.Model)
	}
}

// --- /v1/models ---

func modelIDs(t *testing.T, body []byte) []string {
	t.Helper()
	var list struct {
		Object string `json:"object"`
		Data   []struct {
			ID     string `json:"id"`
			Object string `json:"object"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &list); err != nil || list.Object != "list" {
		t.Fatalf("not a model list: %s", body)
	}
	var ids []string
	for _, m := range list.Data {
		ids = append(ids, m.ID)
	}
	return ids
}

func TestOrgHostModelsListsOnlyTheOrgsDedicatedEndpoints(t *testing.T) {
	stopped := dedicatedRow("org-1", "stopped", "dep-3", "x:1")
	stopped.State = gateway.EndpointStateStopped
	remote := dedicatedRow("org-1", "on-spoke", "dep-4", "")
	remote.ForwardURL = "https://org-1-inference.spoke.example.com"
	res := &fakeTierResolver{
		dedicated: []gateway.DedicatedEndpoint{
			dedicatedRow("org-1", "alpha", "dep-1", "x:1"), dedicatedRow("org-1", "beta", "dep-2", "x:1"),
			stopped, remote, dedicatedRow("org-2", "theirs", "dep-9", "x:1"),
		},
		shared: map[[2]string]gateway.Resolution{{"org-1", "shared"}: {ServingMode: "shared"}},
	}
	s := newOrgHostTestServer(t, nil, res, nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodGet, "/v1/models", "org-1", ""))
	got := strings.Join(modelIDs(t, rr.Body.Bytes()), ",")
	if rr.Code != http.StatusOK || got != "alpha,beta,on-spoke" {
		t.Fatalf("models = %q (status %d), want alpha,beta,on-spoke", got, rr.Code)
	}
}

func TestEndpointScopedModelsListsOnlyItsEndpoint(t *testing.T) {
	res := &fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{
		dedicatedRow("org-1", "alpha", "dep-1", "x:1"), dedicatedRow("org-1", "beta", "dep-2", "x:1"),
	}}
	s := newOrgHostTestServer(t, nil, res, nil)
	req := orgHostRequest(http.MethodGet, "/v1/models", "org-1", "")
	req.Header.Set(identity.HeaderEndpointScope, "dep-2")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if got := strings.Join(modelIDs(t, rr.Body.Bytes()), ","); got != "beta" {
		t.Fatalf("scoped models = %q, want beta", got)
	}
}

// --- shared gateway refusals are JSON ---

func TestGatewayErrorsAreOpenAIJSON(t *testing.T) {
	res := &fakeTierResolver{}
	configured := newOrgHostTestServer(t, nil, res, nil)
	unconfigured := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{})
	failing := newOrgHostTestServer(t, nil, &fakeTierResolver{err: errors.New("boom")}, nil)
	cases := []struct {
		name   string
		srv    *Server
		req    *http.Request
		status int
	}{
		{"not configured", unconfigured, gatewayRequest("org-1", chat("m")), http.StatusServiceUnavailable},
		{"no org", configured, gatewayRequest("", chat("m")), http.StatusForbidden},
		{"no org preflight", configured, gatewayRequestFor(http.MethodOptions, "/v1/chat/completions", "", ""), http.StatusForbidden},
		{"unknown path", configured, gatewayRequestFor(http.MethodGet, "/metrics", "org-1", ""), http.StatusNotFound},
		{"no model", configured, gatewayRequest("org-1", `{"messages":[]}`), http.StatusBadRequest},
		{"invalid json", configured, gatewayRequest("org-1", `{"model":`), http.StatusBadRequest},
		{"unknown model", configured, gatewayRequest("org-1", chat("m")), http.StatusNotFound},
		{"resolver failure", failing, gatewayRequest("org-1", chat("m")), http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			tc.srv.Handler().ServeHTTP(rr, tc.req)
			if rr.Code != tc.status {
				t.Fatalf("status %d, want %d (%s)", rr.Code, tc.status, rr.Body.String())
			}
			if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type %q", ct)
			}
			e := errorBody(t, rr.Body.Bytes())
			if e.Message == "" || e.Type == "" || e.Code == "" {
				t.Fatalf("incomplete error object %s", rr.Body.String())
			}
			var raw map[string]map[string]any
			_ = json.Unmarshal(rr.Body.Bytes(), &raw)
			if _, ok := raw["error"]["param"]; !ok {
				t.Fatalf("error object has no param key: %s", rr.Body.String())
			}
		})
	}
}

// --- forwarding (design section 5) ---

// tlsServing is a stand-in serving cluster's org host over TLS.
func tlsServing(t *testing.T, h http.Handler) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewTLSServer(h)
	t.Cleanup(srv.Close)
	return srv, srv.URL // https://127.0.0.1:port
}

func forwardingServer(t *testing.T, em *recordingEmitter, serving *httptest.Server, rows ...gateway.DedicatedEndpoint) *Server {
	t.Helper()
	s := newOrgHostTestServer(t, em, &fakeTierResolver{dedicated: rows}, nil)
	if serving != nil {
		s.orgHosts.forward.transport = serving.Client().Transport
	}
	return s
}

func forwardRow(org, name, resource, forwardURL, cluster string) gateway.DedicatedEndpoint {
	ep := dedicatedRow(org, name, resource, "")
	ep.ForwardURL = forwardURL
	ep.Cluster = cluster
	return ep
}

func TestForwardSendsOnlyAuthorizationAndBody(t *testing.T) {
	be := &orgEngine{}
	serving, base := tlsServing(t, be)
	s := forwardingServer(t, nil, serving, forwardRow("org-1", "remote", "dep-1", base, "spoke"))
	req := orgHostRequest(http.MethodPost, "/v1/chat/completions?x=1", "org-1", chat("remote"))
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cookie", "session=secret")
	req.Header.Set("X-Request-Id", "client-id")
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || be.hits.Load() != 1 {
		t.Fatalf("status %d hits %d body %s", rr.Code, be.hits.Load(), rr.Body.String())
	}
	got := be.headers[0]
	allowed := map[string]bool{"Authorization": true, "Content-Type": true, "Accept": true,
		http.CanonicalHeaderKey(orgHostForwardedHeader): true, "Content-Length": true, "User-Agent": true, "Accept-Encoding": true}
	for name := range got {
		if !allowed[name] {
			t.Fatalf("forward carried header %s", name)
		}
	}
	if got.Get("Authorization") != "Bearer sk-test" || got.Get("Accept") != "text/event-stream" {
		t.Fatalf("forward lost the customer's credential or Accept: %v", got)
	}
	if string(be.bodies[0]) != chat("remote") || be.paths[0] != "/v1/chat/completions?x=1" {
		t.Fatalf("body %q path %q", be.bodies[0], be.paths[0])
	}
}

func TestForwardTargetComesOnlyFromTheRow(t *testing.T) {
	be := &orgEngine{}
	serving, base := tlsServing(t, be)
	s := forwardingServer(t, nil, serving, forwardRow("org-1", "remote", "dep-1", base, "spoke"))
	req := orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("remote"))
	req.Host = "evil.example.com"
	req.Header.Set("X-Forwarded-Host", "evil.example.com")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || be.hits.Load() != 1 {
		t.Fatalf("status %d hits %d", rr.Code, be.hits.Load())
	}
	if h := be.headers[0].Get("X-Forwarded-Host"); h != "" {
		t.Fatalf("forward carried X-Forwarded-Host %q", h)
	}
}

func TestForwardVerifiesTLS(t *testing.T) {
	be := &orgEngine{}
	_, base := tlsServing(t, be) // self-signed: the default transport must refuse it
	s := forwardingServer(t, nil, nil, forwardRow("org-1", "remote", "dep-1", base, "spoke"))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("remote")))
	if rr.Code != http.StatusBadGateway || be.hits.Load() != 0 {
		t.Fatalf("untrusted certificate: status %d hits %d, want 502 and no request", rr.Code, be.hits.Load())
	}
	tr, ok := s.orgHosts.forward.transport.(*http.Transport)
	if !ok || tr.TLSClientConfig == nil || tr.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("forward transport must verify certificates")
	}
}

func TestForwardedRequestIsNotMeteredOnTheEntry(t *testing.T) {
	be := &orgEngine{}
	serving, base := tlsServing(t, be)
	em := &recordingEmitter{}
	s := forwardingServer(t, em, serving, forwardRow("org-1", "remote", "dep-1", base, "spoke"))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("remote")))
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	time.Sleep(100 * time.Millisecond)
	if em.count() != 0 {
		t.Fatalf("entry emitted %d metering events, want 0 (the serving cluster meters)", em.count())
	}
}

func TestForwardOutlastsTheServingHoldBudget(t *testing.T) {
	s := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithWaker(&fakeWaker{}, 10*time.Second).
		WithOrgHosts(&fakeTierResolver{}, "")
	f := s.orgHosts.forward
	if f.firstByteTimeout <= config.DefaultWakeTimeout {
		t.Fatalf("first-byte timeout %s must exceed the serving hold budget %s", f.firstByteTimeout, config.DefaultWakeTimeout)
	}
	tr := f.transport.(*http.Transport)
	if tr.ResponseHeaderTimeout != f.firstByteTimeout {
		t.Fatalf("transport header timeout %s != %s", tr.ResponseHeaderTimeout, f.firstByteTimeout)
	}
	long := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithWaker(&fakeWaker{}, 600*time.Second).
		WithOrgHosts(&fakeTierResolver{}, "")
	if long.orgHosts.forward.firstByteTimeout <= 600*time.Second {
		t.Fatalf("a longer local hold must lengthen the forward timeout: %s", long.orgHosts.forward.firstByteTimeout)
	}
}

func TestForwardPassesServingErrorsThrough(t *testing.T) {
	be := &orgEngine{status: http.StatusConflict, body: `{"error":{"message":"This endpoint is stopped.","type":"invalid_request_error","code":"endpoint_not_running","param":null}}`}
	serving, base := tlsServing(t, be)
	s := forwardingServer(t, nil, serving, forwardRow("org-1", "remote", "dep-1", base, "spoke"))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("remote")))
	if rr.Code != http.StatusConflict || rr.Body.String() != be.body {
		t.Fatalf("status %d body %s, want the serving cluster's answer unchanged", rr.Code, rr.Body.String())
	}
}

func TestForwardStreamsChunkByChunk(t *testing.T) {
	release := make(chan struct{})
	serving, base := tlsServing(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: one\n\n"))
		w.(http.Flusher).Flush()
		<-release
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	s := forwardingServer(t, nil, serving, forwardRow("org-1", "remote", "dep-1", base, "spoke"))
	front := httptest.NewServer(s.Handler())
	defer front.Close()
	req, _ := http.NewRequest(http.MethodPost, front.URL+"/v1/chat/completions", strings.NewReader(chat("remote")))
	req.Header = orgHostRequest(http.MethodPost, "/", "org-1", "").Header
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || line != "data: one\n" {
		t.Fatalf("first chunk %q (%v) not delivered before the stream ended", line, err)
	}
	close(release)
}

func TestForwardedRequestIsNeverForwardedAgain(t *testing.T) {
	be := &orgEngine{}
	serving, base := tlsServing(t, be)
	local := &orgEngine{}
	lsrv := httptest.NewServer(local)
	defer lsrv.Close()
	lup, _ := url.Parse(lsrv.URL)
	s := forwardingServer(t, nil, serving,
		forwardRow("org-1", "remote", "dep-1", base, "spoke"),
		dedicatedRow("org-1", "here", "dep-2", lup.Host),
	)
	req := orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("remote"))
	req.Header.Set(orgHostForwardedHeader, "1")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusLoopDetected || errorCode(t, rr.Body.Bytes()) != "forward_loop" || be.hits.Load() != 0 {
		t.Fatalf("second forward: status %d hits %d", rr.Code, be.hits.Load())
	}
	// A forwarded request for an endpoint this cluster serves is served, and
	// the forward marker does not reach the engine.
	req = orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("here"))
	req.Header.Set(orgHostForwardedHeader, "1")
	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || local.hits.Load() != 1 {
		t.Fatalf("forwarded local: status %d hits %d", rr.Code, local.hits.Load())
	}
	if local.headers[0].Get(orgHostForwardedHeader) != "" {
		t.Fatal("forward marker reached the engine")
	}
	// And the forward the entry sends carries the marker.
	s.Handler().ServeHTTP(httptest.NewRecorder(), orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("remote")))
	if be.hits.Load() != 1 || be.headers[0].Get(orgHostForwardedHeader) == "" {
		t.Fatalf("entry forward hits %d marker %q", be.hits.Load(), be.headers[0].Get(orgHostForwardedHeader))
	}
}

// A spoke holding a copy of a HUB endpoint's row forwards to the hub exactly
// as the hub forwards to a spoke: nothing in the code names the hub.
func TestEntryDoesNotAssumeItIsTheHub(t *testing.T) {
	hubBE := &orgEngine{}
	hub, hubBase := tlsServing(t, hubBE)
	spokeEntry := forwardingServer(t, nil, hub, forwardRow("org-1", "on-hub", "dep-1", hubBase, "hub"))
	rr := httptest.NewRecorder()
	spokeEntry.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("on-hub")))
	if rr.Code != http.StatusOK || hubBE.hits.Load() != 1 {
		t.Fatalf("spoke entry to hub: status %d hits %d", rr.Code, hubBE.hits.Load())
	}
}

// --- wake (design section 7) ---

func TestOrgHostWakeUsesTheRowsResourceId(t *testing.T) {
	backend := &coldToWarmBackend{warmBody: `{"model":"m","usage":{"prompt_tokens":1,"completion_tokens":1}}`}
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)
	row := dedicatedRow("org-1", "m", "dep-cold", up.Host)
	row.ServingProfile = "disaggregated"
	waker := &fakeWaker{backend: backend, warmsAt: 1}
	s := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithGateway(&fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{row}}, "tf-shared", 8000).
		WithWaker(waker, 5*time.Second)
	s = s.WithOrgHosts(&fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{row}}, "")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("m")))
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rr.Code, rr.Body.String())
	}
	waker.mu.Lock()
	target := waker.lastTarget
	waker.mu.Unlock()
	if atomic.LoadInt32(&waker.calls) != 1 || target.ResourceID != "dep-cold" || target.ServingMode != identity.ServingModeDedicated ||
		target.ServedModel != "m" {
		t.Fatalf("wake calls %d target %+v, want the row's resource", atomic.LoadInt32(&waker.calls), target)
	}
}

func TestOrgHostColdHoldLimitPerEndpoint(t *testing.T) {
	backend := &coldToWarmBackend{warmBody: `{"model":"m","usage":{"prompt_tokens":1,"completion_tokens":1}}`}
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)
	rowA := dedicatedRow("org-1", "a", "endpoint-a", up.Host)
	rowA.ServingProfile = "disaggregated"
	rowB := dedicatedRow("org-1", "b", "endpoint-b", up.Host)
	rowB.ServingProfile = "disaggregated"
	res := &fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{rowA, rowB}}
	waker := &blockingWaker{started: make(chan struct{}, 4), release: make(chan struct{}), backend: backend}
	s := newProtectedServer(config.ProtectionSettings{MaxDedicatedColdHoldsPerEndpoint: 1}, &recordingEmitter{}).
		WithGateway(res, "tf-shared", 8000).
		WithWaker(waker, 5*time.Second)
	s = s.WithOrgHosts(res, "")

	var wg sync.WaitGroup
	held := httptest.NewRecorder()
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Handler().ServeHTTP(held, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("a")))
	}()
	<-waker.started
	over := httptest.NewRecorder()
	s.Handler().ServeHTTP(over, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("a")))
	if over.Code != http.StatusServiceUnavailable || over.Header().Get("Retry-After") == "" {
		t.Fatalf("over-limit: %d retry-after %q", over.Code, over.Header().Get("Retry-After"))
	}
	other := httptest.NewRecorder()
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Handler().ServeHTTP(other, orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", chat("b")))
	}()
	select {
	case <-waker.started:
	case <-time.After(5 * time.Second):
		t.Fatal("another endpoint must get its own hold")
	}
	close(waker.release)
	wg.Wait()
	if held.Code != http.StatusOK || other.Code != http.StatusOK {
		t.Fatalf("held=%d other=%d", held.Code, other.Code)
	}
}
