package proxy

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
)

// Ruling Q-R8STRIP2: no X-Saturn-* header reaches the upstream, on any route.
// Phoebe parses identity first and strips the whole namespace before
// forwarding; its routing and metering decisions use only the parsed identity.

// retiredLegacyHeaders are the five quota headers ruling R8 removed from the
// trusted set.
var retiredLegacyHeaders = []string{
	"X-Saturn-Service-Tier",
	"X-Saturn-Rate-Limit-Requests",
	"X-Saturn-Rate-Limit-Total-Prompt-Tokens",
	"X-Saturn-Rate-Limit-Uncached-Prompt-Tokens",
	"X-Saturn-Rate-Limit-Generated-Tokens",
}

// keptLookalikes are headers outside the X-Saturn-* namespace (look-alike
// names included) that must reach the upstream unchanged.
var keptLookalikes = map[string]string{
	"X-Saturnine":      "kept-1",
	"X-SaturnX":        "kept-2",
	"X-Saturn":         "kept-3",
	"X-Saturnalia-Foo": "kept-4",
	"X-Client-Custom":  "kept-5",
}

// addClientSaturnHeaders stamps client-sent X-Saturn-* junk (retired, unknown,
// mixed-case, repeated) plus the look-alike and non-Saturn headers that must
// survive. Mixed-case keys are written straight into the map so they bypass
// Header.Set canonicalization, as a raw key would.
func addClientSaturnHeaders(req *http.Request) {
	for _, name := range retiredLegacyHeaders {
		req.Header.Set(name, "client-forged")
	}
	req.Header.Set("X-Saturn-Foo", "client-forged")
	req.Header["x-saturn-MIXED-case"] = []string{"client-forged"}
	req.Header["X-SATURN-SERVICE-TIER-SHOUT"] = []string{"client-forged"}
	req.Header.Add("X-Saturn-Repeated", "one")
	req.Header.Add("X-Saturn-Repeated", "two")
	for name, value := range keptLookalikes {
		req.Header[name] = []string{value}
	}
}

// headerRecorder is an upstream that records the headers of EVERY request it
// receives (wake probes included) and delegates the response to next.
type headerRecorder struct {
	mu   sync.Mutex
	seen []http.Header
	next http.Handler
}

func (h *headerRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.seen = append(h.seen, r.Header.Clone())
	h.mu.Unlock()
	h.next.ServeHTTP(w, r)
}

func (h *headerRecorder) all() []http.Header {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]http.Header(nil), h.seen...)
}

func usageHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`))
	})
}

func isSaturnName(name string) bool {
	return strings.HasPrefix(strings.ToLower(name), "x-saturn-")
}

// assertNoUpstreamSaturnHeaders checks every request the upstream saw: it
// carries no X-Saturn-* header in any spelling, every look-alike and
// non-Saturn client header arrives with its value, and phoebe's own request
// id arrives.
func assertNoUpstreamSaturnHeaders(t *testing.T, seen []http.Header) {
	t.Helper()
	if len(seen) == 0 {
		t.Fatal("upstream received no request")
	}
	for i, got := range seen {
		for name, values := range got {
			if isSaturnName(name) {
				t.Errorf("upstream request %d received X-Saturn-* header %s=%q", i, name, values)
			}
		}
		for name, want := range keptLookalikes {
			if g := got.Get(name); g != want {
				t.Errorf("upstream request %d: non-Saturn header %s = %q, want %q", i, name, g, want)
			}
		}
		if got.Get(requestIDHeader) == "" {
			t.Errorf("upstream request %d lost phoebe's %s", i, requestIDHeader)
		}
	}
}

// requireSent fails unless the client request actually carried each named
// X-Saturn-* header, so a pass proves those headers were stripped rather than
// never sent.
func requireSent(t *testing.T, sent http.Header, names ...string) {
	t.Helper()
	for _, name := range names {
		if sent.Get(name) == "" {
			t.Fatalf("fixture must carry %s", name)
		}
	}
}

// TestNoSaturnHeadersReachUpstreamOnEveryRoute covers every route type that
// forwards upstream: header-routed dedicated, header-routed shared (phoebe's
// own policy headers added), the single-host gateway, and the shared wake path
// (cold probe, warm re-probe, then the metered forward). On each, the
// upstream sees ZERO X-Saturn-* headers, including the trusted envelope and
// the edge-contract identity headers (X-Saturn-Upstream and friends), while
// the request still reaches the right upstream and is metered with the
// identity phoebe parsed before the strip.
func TestNoSaturnHeadersReachUpstreamOnEveryRoute(t *testing.T) {
	type outcome struct {
		code     int
		requests int // upstream requests expected (wake: probe, re-probe, forward)
		want     metering.Event
	}
	cases := []struct {
		name string
		// run builds the server and the request for an upstream at u and a
		// decoy at decoy, adds the client headers, serves, and returns what
		// the test must observe.
		run       func(t *testing.T, rec *headerRecorder, u, decoy *url.URL, em *recordingEmitter) outcome
		backend   func() http.Handler
		sharedPol bool // phoebe adds the shared Dynamo policy headers
	}{
		{
			name:    "dedicated header-routed",
			backend: usageHandler,
			run: func(t *testing.T, _ *headerRecorder, u, _ *url.URL, em *recordingEmitter) outcome {
				req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a"}`))
				setUpstream(req, u)
				req.Header.Set(identity.HeaderAuthID, "auth-a")
				req.Header.Set(identity.HeaderUserID, "user-a")
				req.Header.Set(identity.HeaderResourceID, "resource-a")
				req.Header.Set(identity.HeaderResourceType, "deployment")
				req.Header.Set(identity.HeaderOrgID, "org-a")
				req.Header.Set(identity.HeaderBaseModel, "base/model")
				req.Header.Set(identity.HeaderServedModel, "model-a")
				addClientSaturnHeaders(req)
				requireSent(t, req.Header, identity.HeaderUpstream, identity.HeaderServingMode, identity.HeaderResourceID, identity.HeaderAuthID, identity.HeaderOrgID)
				rr := httptest.NewRecorder()
				newTestServerE(t, u, em).Handler().ServeHTTP(rr, req)
				return outcome{rr.Code, 1, metering.Event{
					AuthID: "auth-a", UserID: "user-a", ResourceID: "resource-a", ResourceType: "deployment",
					OrgID: "org-a", BaseModel: "base/model", ServingMode: identity.ServingModeDedicated,
				}}
			},
		},
		{
			name:      "shared header-routed",
			backend:   usageHandler,
			sharedPol: true,
			run: func(t *testing.T, _ *headerRecorder, u, _ *url.URL, em *recordingEmitter) outcome {
				req := sharedRequest(u)
				addClientSaturnHeaders(req)
				requireSent(t, req.Header, identity.HeaderUpstream, identity.HeaderServingMode, identity.HeaderOwnerID, identity.HeaderOrgID, identity.HeaderOwnerRateLimitRequests)
				rr := httptest.NewRecorder()
				newTestServerE(t, u, em).Handler().ServeHTTP(rr, req)
				return outcome{rr.Code, 1, metering.Event{
					AuthID: "auth-a", ResourceID: "resource-a", OrgID: "org-a", ServingMode: identity.ServingModeShared,
				}}
			},
		},
		{
			// A client on the gateway route forges per-resource routing
			// headers pointing at a decoy. Gateway resolution overwrites the
			// parsed identity, so the request reaches the resolved upstream
			// and is metered as the resolved resource; the forged headers are
			// stripped like every other X-Saturn-* header.
			name:      "gateway",
			backend:   usageHandler,
			sharedPol: true,
			run: func(t *testing.T, _ *headerRecorder, u, decoy *url.URL, em *recordingEmitter) outcome {
				resolver := &mapResolver{m: map[[2]string]gateway.Resolution{
					{"org-1", "model-a"}: {ResourceID: "tfm-1", BaseModel: "base/model", ServingMode: "shared", GraphK8sName: "graph-a"},
				}}
				srv := newGatewayTestServer(t, em, resolver, u)
				req := gatewayRequest("org-1", `{"model":"model-a","max_tokens":20}`)
				req.Header.Set(identity.HeaderUpstream, decoy.Host)
				req.Header.Set(identity.HeaderResourceID, "forged-resource")
				addClientSaturnHeaders(req)
				requireSent(t, req.Header, identity.HeaderGateway, identity.HeaderOrgID, identity.HeaderOwnerID, identity.HeaderUpstream, identity.HeaderResourceID)
				rr := httptest.NewRecorder()
				srv.Handler().ServeHTTP(rr, req)
				return outcome{rr.Code, 1, metering.Event{
					AuthID: "auth-1", ResourceID: "tfm-1", OrgID: "org-1", BaseModel: "base/model", ServingMode: identity.ServingModeShared,
				}}
			},
		},
		{
			name:      "shared header-routed through wake",
			sharedPol: true,
			run: func(t *testing.T, rec *headerRecorder, u, _ *url.URL, em *recordingEmitter) outcome {
				cold := rec.next.(*coldToWarmBackend)
				waker := &fakeWaker{warmsAt: 1, backend: cold}
				srv := newTestServerE(t, u, em).WithWaker(waker, 5*time.Second, 3)
				req := sharedRequest(u)
				addClientSaturnHeaders(req)
				requireSent(t, req.Header, identity.HeaderUpstream, identity.HeaderServingMode, identity.HeaderResourceID)
				rr := httptest.NewRecorder()
				srv.Handler().ServeHTTP(rr, req)
				if got := waker.last().ResourceID; got != "resource-a" {
					t.Errorf("wake target resource = %q, want resource-a (from the parsed identity)", got)
				}
				// cold probe, warm re-probe after the wake, metered forward
				return outcome{rr.Code, 3, metering.Event{
					AuthID: "auth-a", ResourceID: "resource-a", OrgID: "org-a", ServingMode: identity.ServingModeShared,
				}}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &headerRecorder{}
			if tc.backend != nil {
				rec.next = tc.backend()
			} else {
				rec.next = &coldToWarmBackend{warmBody: `{"model":"model-a","usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`}
			}
			be := httptest.NewServer(rec)
			defer be.Close()
			u, _ := url.Parse(be.URL)
			decoyRec := &headerRecorder{next: usageHandler()}
			decoyServer := httptest.NewServer(decoyRec)
			defer decoyServer.Close()
			decoy, _ := url.Parse(decoyServer.URL)
			em := &recordingEmitter{}

			got := tc.run(t, rec, u, decoy, em)
			if got.code != http.StatusOK {
				t.Fatalf("status = %d, want 200", got.code)
			}
			seen := rec.all()
			if len(seen) != got.requests {
				t.Fatalf("upstream saw %d requests, want %d", len(seen), got.requests)
			}
			if n := len(decoyRec.all()); n != 0 {
				t.Fatalf("decoy upstream saw %d requests, want 0", n)
			}
			assertNoUpstreamSaturnHeaders(t, seen)
			if tc.sharedPol {
				// The final forward carries phoebe's own shared-policy headers.
				last := seen[len(seen)-1]
				if last.Get("X-Tenant-ID") == "" || last.Get("X-Dynamo-Request-Priority") == "" {
					t.Errorf("phoebe-added shared policy headers missing upstream: %v", last)
				}
			}
			events := em.waitForEvents(1, 2*time.Second)
			if len(events) != 1 {
				t.Fatalf("metering events = %d, want 1", len(events))
			}
			ev := events[0]
			want := got.want
			for _, f := range []struct{ field, got, want string }{
				{"AuthID", ev.AuthID, want.AuthID},
				{"UserID", ev.UserID, want.UserID},
				{"ResourceID", ev.ResourceID, want.ResourceID},
				{"ResourceType", ev.ResourceType, want.ResourceType},
				{"OrgID", ev.OrgID, want.OrgID},
				{"BaseModel", ev.BaseModel, want.BaseModel},
				{"ServingMode", ev.ServingMode, want.ServingMode},
			} {
				if f.got != f.want {
					t.Errorf("metered %s = %q, want %q", f.field, f.got, f.want)
				}
			}
			if !ev.UsageFound {
				t.Errorf("metered event has no usage: %+v", ev)
			}
		})
	}
}

// TestSaturnHeadersStrippedWithConfiguredTrustedSet: an explicitly configured
// PHOEBE_TRUSTED_HEADERS changes what phoebe reads, not what it forwards. With
// a configured list, the trusted names are still stripped and the gateway
// request is still served from the parsed identity.
func TestSaturnHeadersStrippedWithConfiguredTrustedSet(t *testing.T) {
	var keep []string
	for name := range identity.ActiveTrustedHeaders() {
		keep = append(keep, name)
	}
	// t.Setenv restores the variable itself, but the active trusted set is
	// package-global, so the registry must be reloaded after the variable is
	// restored. Cleanups run last-in first-out: registering the reload before
	// t.Setenv makes it run after t.Setenv's restore, so it reloads from the
	// original value.
	t.Cleanup(func() { identity.LoadTrustedHeaders(logging.New(logging.ERROR)) })
	t.Setenv(identity.TrustedHeadersEnv, strings.Join(keep, ","))
	identity.LoadTrustedHeaders(logging.New(logging.ERROR))

	rec := &headerRecorder{next: usageHandler()}
	be := httptest.NewServer(rec)
	defer be.Close()
	u, _ := url.Parse(be.URL)

	resolver := &mapResolver{m: map[[2]string]gateway.Resolution{
		{"org-1", "model-a"}: {ResourceID: "tfm-1", BaseModel: "base/model", ServingMode: "shared", GraphK8sName: "graph-a"},
	}}
	srv := newGatewayTestServer(t, &recordingEmitter{}, resolver, u)
	req := gatewayRequest("org-1", `{"model":"model-a","max_tokens":20}`)
	addClientSaturnHeaders(req)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	assertNoUpstreamSaturnHeaders(t, rec.all())
}

// trailerRecorder is a backend that reads each request body to the end and
// then records the request's trailers. The server fills in r.Trailer only once
// the body reaches EOF, so the copy must be taken after the drain.
type trailerRecorder struct {
	mu       sync.Mutex
	trailers []http.Header
}

func (rec *trailerRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	rec.mu.Lock()
	rec.trailers = append(rec.trailers, r.Trailer.Clone())
	rec.mu.Unlock()
	_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`))
}

func (rec *trailerRecorder) all() []http.Header {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]http.Header(nil), rec.trailers...)
}

// rawChunkedPost writes a chunked HTTP/1.1 POST byte for byte to addr, with
// the given trailer lines after the terminating zero-length chunk, and returns
// the response status. Writing raw bytes is what makes the server parse the
// trailers off the wire and merge them into r.Trailer at body EOF, which an
// in-process httptest.NewRequest fixture never does.
func rawChunkedPost(t *testing.T, addr string, header http.Header, declared, body string, trailerLines []string) int {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	var b strings.Builder
	b.WriteString("POST /v1/chat/completions HTTP/1.1\r\nHost: phoebe.test\r\n")
	for name, values := range header {
		for _, v := range values {
			fmt.Fprintf(&b, "%s: %s\r\n", name, v)
		}
	}
	b.WriteString("Content-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n")
	if declared != "" {
		fmt.Fprintf(&b, "Trailer: %s\r\n", declared)
	}
	b.WriteString("\r\n")
	if body != "" {
		fmt.Fprintf(&b, "%x\r\n%s\r\n", len(body), body)
	}
	b.WriteString("0\r\n")
	for _, line := range trailerLines {
		b.WriteString(line + "\r\n")
	}
	b.WriteString("\r\n")
	if _, err := conn.Write([]byte(b.String())); err != nil {
		t.Fatalf("write request: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

// TestNoSaturnTrailersReachUpstreamOverWire: no X-Saturn-* request trailer
// reaches the upstream (ruling Q-R8STRIP2 covers headers and trailers), on a
// real connection where the server fills in trailer values only after the body
// has been read. The trailers include trusted and edge-contract names, a
// mixed-case name, and look-alikes that must survive. Covered:
// a declared and an undeclared trailer name (the server merges both), a
// non-empty and an empty body, and the dedicated and gateway routes. A
// harmless trailer must still arrive, proving the trailer channel is live.
func TestNoSaturnTrailersReachUpstreamOverWire(t *testing.T) {
	const allTrailers = "X-Saturn-Service-Tier, X-Saturn-Foo, X-Saturn-Upstream, X-Saturn-Org-Id, x-SATURN-mixed, X-Saturnine, X-Other-Trailer"
	trailerLines := []string{
		"X-Saturn-Service-Tier: premium", "X-Saturn-Foo: bar",
		"X-Saturn-Upstream: decoy.invalid:80", "X-Saturn-Org-Id: org-forged",
		"x-SATURN-mixed: forged", "X-Saturnine: ok", "X-Other-Trailer: ok",
	}

	dedicatedHeader := func(u *url.URL) http.Header {
		h := http.Header{}
		h.Set(identity.HeaderAuthID, "auth-a")
		h.Set(identity.HeaderResourceID, "resource-a")
		h.Set(identity.HeaderUpstream, u.Host)
		h.Set(identity.HeaderServingMode, identity.ServingModeDedicated)
		h.Set("X-Request-Id", "saturn-test-request-id")
		return h
	}
	gatewayHeader := func(*url.URL) http.Header {
		return gatewayRequest("org-1", "").Header
	}
	dedicatedServer := func(t *testing.T, u *url.URL) *Server { return newTestServer(t, u) }
	gatewayServer := func(t *testing.T, u *url.URL) *Server {
		resolver := &mapResolver{m: map[[2]string]gateway.Resolution{
			{"org-1", "model-a"}: {ResourceID: "tfm-1", BaseModel: "base/model", ServingMode: "shared", GraphK8sName: "graph-a"},
		}}
		return newGatewayTestServer(t, &recordingEmitter{}, resolver, u)
	}

	for _, tc := range []struct {
		name     string
		server   func(*testing.T, *url.URL) *Server
		header   func(*url.URL) http.Header
		declared string
		body     string
		// noControl: an empty body is forwarded with no body at all
		// (ReverseProxy drops a zero-length body), so no trailer, not even
		// the harmless one, can travel. The case still proves the empty
		// chunked request is served (200, not 502) and leaks nothing.
		noControl bool
	}{
		{"dedicated/declared", dedicatedServer, dedicatedHeader, allTrailers, `{"model":"model-a","stream":true,"max_tokens":5}`, false},
		{"dedicated/undeclared", dedicatedServer, dedicatedHeader, "X-Other-Trailer", `{"model":"model-a","max_tokens":5}`, false},
		{"dedicated/empty-body", dedicatedServer, dedicatedHeader, allTrailers, "", true},
		{"gateway/declared", gatewayServer, gatewayHeader, allTrailers, `{"model":"model-a","stream":true,"max_tokens":5}`, false},
		{"gateway/undeclared", gatewayServer, gatewayHeader, "X-Other-Trailer", `{"model":"model-a","max_tokens":5}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &trailerRecorder{}
			be := httptest.NewServer(rec)
			defer be.Close()
			u, _ := url.Parse(be.URL)
			front := httptest.NewServer(tc.server(t, u).Handler())
			defer front.Close()

			status := rawChunkedPost(t, front.Listener.Addr().String(), tc.header(u), tc.declared, tc.body, trailerLines)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200", status)
			}
			seen := rec.all()
			if len(seen) == 0 {
				t.Fatal("upstream saw no request")
			}
			for _, tr := range seen {
				if !tc.noControl && (tr.Get("X-Other-Trailer") != "ok" || tr.Get("X-Saturnine") != "ok") {
					t.Fatalf("control trailers did not arrive (%v); the test cannot observe trailers", tr)
				}
				for name := range tr {
					if isSaturnName(name) {
						t.Fatalf("upstream received X-Saturn-* trailer %s=%v", name, tr[name])
					}
				}
			}
		})
	}
}
