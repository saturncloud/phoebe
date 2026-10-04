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
)

// Ruling Q-R8STRIP (option b): phoebe strips every X-Saturn-* request header
// outside the active trusted set before forwarding upstream, on every route.

// retiredLegacyHeaders are the five quota headers ruling R8 removed from the
// trusted set. The edge no longer strips them, so a client copy reaches phoebe.
var retiredLegacyHeaders = []string{
	"X-Saturn-Service-Tier",
	"X-Saturn-Rate-Limit-Requests",
	"X-Saturn-Rate-Limit-Total-Prompt-Tokens",
	"X-Saturn-Rate-Limit-Uncached-Prompt-Tokens",
	"X-Saturn-Rate-Limit-Generated-Tokens",
}

// forwardableSaturn is every X-Saturn-* name phoebe may forward with the
// pinned trusted set active: the pinned 13 plus the edge-contract identity
// headers.
var forwardableSaturn = map[string]bool{
	identity.HeaderGateway: true, identity.HeaderOrgID: true, identity.HeaderOwnerID: true,
	identity.HeaderServingMode: true, identity.HeaderServedModel: true,
	identity.HeaderOrgRateLimitRequests: true, identity.HeaderOrgRateLimitTotalPromptTokens: true,
	identity.HeaderOrgRateLimitUncachedPromptTokens: true, identity.HeaderOrgRateLimitGeneratedTokens: true,
	identity.HeaderOwnerRateLimitRequests: true, identity.HeaderOwnerRateLimitTotalPromptTokens: true,
	identity.HeaderOwnerRateLimitUncachedPromptTokens: true, identity.HeaderOwnerRateLimitGeneratedTokens: true,
	identity.HeaderAuthID: true, identity.HeaderUserID: true, identity.HeaderGroupID: true,
	identity.HeaderResourceID: true, identity.HeaderResourceType: true, identity.HeaderBaseModel: true,
	identity.HeaderAdapter: true, identity.HeaderUpstream: true,
}

// addUntrustedSaturnHeaders stamps the client-sent junk every route case must
// strip, and returns the names (as the client spelled them) that must not
// reach the upstream. Mixed-case keys are written straight into the map so
// they bypass Header.Set canonicalization, as a raw key would.
func addUntrustedSaturnHeaders(req *http.Request) []string {
	var names []string
	for _, name := range retiredLegacyHeaders {
		req.Header.Set(name, "client-forged")
		names = append(names, name)
	}
	req.Header.Set("X-Saturn-Foo", "client-forged")
	req.Header["x-saturn-MIXED-case"] = []string{"client-forged"}
	req.Header["X-SATURN-SERVICE-TIER-SHOUT"] = []string{"client-forged"}
	req.Header.Add("X-Saturn-Repeated", "one")
	req.Header.Add("X-Saturn-Repeated", "two")
	return append(names, "X-Saturn-Foo", "x-saturn-MIXED-case", "X-SATURN-SERVICE-TIER-SHOUT", "X-Saturn-Repeated")
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

// assertUpstreamSaturnHeaders checks every request the upstream saw: no
// stripped name arrives in any spelling, every X-Saturn-* name that does
// arrive is forwardable, every forwardable header the client request carried
// arrives with its exact values, and phoebe's own request id arrives.
func assertUpstreamSaturnHeaders(t *testing.T, seen []http.Header, sent http.Header, stripped []string, forwardable map[string]bool) {
	t.Helper()
	if len(seen) == 0 {
		t.Fatal("upstream received no request")
	}
	for i, got := range seen {
		for _, name := range stripped {
			if v := got.Values(name); len(v) > 0 {
				t.Errorf("upstream request %d received stripped header %q", i, name)
			}
		}
		for name := range got {
			if strings.HasPrefix(strings.ToLower(name), "x-saturn-") && !forwardable[http.CanonicalHeaderKey(name)] {
				t.Errorf("upstream request %d received non-forwardable header %q", i, name)
			}
		}
		for name, values := range sent {
			canonical := http.CanonicalHeaderKey(name)
			if !forwardable[canonical] {
				continue
			}
			if g := got.Values(canonical); strings.Join(g, ",") != strings.Join(values, ",") {
				t.Errorf("upstream request %d: trusted header %s = %q, want %q", i, canonical, g, values)
			}
		}
		if got.Get(requestIDHeader) == "" {
			t.Errorf("upstream request %d lost phoebe's %s", i, requestIDHeader)
		}
	}
}

// TestUntrustedSaturnHeadersStrippedOnEveryRoute covers every route type that
// forwards upstream: header-routed dedicated, header-routed shared (phoebe's
// own policy headers added), the single-host gateway, and the wake path (cold
// probe, warm re-probe, then the metered forward).
func TestUntrustedSaturnHeadersStrippedOnEveryRoute(t *testing.T) {
	cases := []struct {
		name string
		// run builds the server and request for an upstream at u, adds the
		// untrusted headers, and serves; it returns the status, the headers the
		// client sent, the stripped names, and the expected upstream request count.
		run       func(t *testing.T, rec *headerRecorder, u *url.URL) (int, http.Header, []string, int)
		backend   func() http.Handler
		sharedPol bool // phoebe adds the shared Dynamo policy headers
	}{
		{
			name:    "dedicated header-routed",
			backend: usageHandler,
			run: func(t *testing.T, _ *headerRecorder, u *url.URL) (int, http.Header, []string, int) {
				req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a"}`))
				setUpstream(req, u)
				req.Header.Set(identity.HeaderAuthID, "auth-a")
				req.Header.Set(identity.HeaderResourceID, "resource-a")
				req.Header.Set(identity.HeaderBaseModel, "base/model")
				req.Header.Set(identity.HeaderServedModel, "model-a")
				stripped := addUntrustedSaturnHeaders(req)
				sent := req.Header.Clone()
				rr := httptest.NewRecorder()
				newTestServer(t, u).Handler().ServeHTTP(rr, req)
				return rr.Code, sent, stripped, 1
			},
		},
		{
			name:      "shared header-routed",
			backend:   usageHandler,
			sharedPol: true,
			run: func(t *testing.T, _ *headerRecorder, u *url.URL) (int, http.Header, []string, int) {
				req := sharedRequest(u)
				stripped := addUntrustedSaturnHeaders(req)
				sent := req.Header.Clone()
				rr := httptest.NewRecorder()
				newTestServer(t, u).Handler().ServeHTTP(rr, req)
				return rr.Code, sent, stripped, 1
			},
		},
		{
			name:      "gateway",
			backend:   usageHandler,
			sharedPol: true,
			run: func(t *testing.T, _ *headerRecorder, u *url.URL) (int, http.Header, []string, int) {
				resolver := &mapResolver{m: map[[2]string]gateway.Resolution{
					{"org-1", "model-a"}: {ResourceID: "tfm-1", BaseModel: "base/model", ServingMode: "shared", GraphK8sName: "graph-a"},
				}}
				srv := newGatewayTestServer(t, &recordingEmitter{}, resolver, u)
				req := gatewayRequest("org-1", `{"model":"model-a","max_tokens":20}`)
				stripped := addUntrustedSaturnHeaders(req)
				sent := req.Header.Clone()
				rr := httptest.NewRecorder()
				srv.Handler().ServeHTTP(rr, req)
				return rr.Code, sent, stripped, 1
			},
		},
		{
			name:      "shared header-routed through wake",
			sharedPol: true,
			run: func(t *testing.T, rec *headerRecorder, u *url.URL) (int, http.Header, []string, int) {
				cold := rec.next.(*coldToWarmBackend)
				waker := &fakeWaker{warmsAt: 1, backend: cold}
				srv := newTestServer(t, u).WithWaker(waker, 5*time.Second, 3)
				req := sharedRequest(u)
				stripped := addUntrustedSaturnHeaders(req)
				sent := req.Header.Clone()
				rr := httptest.NewRecorder()
				srv.Handler().ServeHTTP(rr, req)
				// cold probe, warm re-probe after the wake, metered forward
				return rr.Code, sent, stripped, 3
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

			code, sent, stripped, wantRequests := tc.run(t, rec, u)
			if code != http.StatusOK {
				t.Fatalf("status = %d, want 200", code)
			}
			seen := rec.all()
			if len(seen) != wantRequests {
				t.Fatalf("upstream saw %d requests, want %d", len(seen), wantRequests)
			}
			assertUpstreamSaturnHeaders(t, seen, sent, stripped, forwardableSaturn)
			if tc.sharedPol {
				// The final forward carries phoebe's own shared-policy headers.
				last := seen[len(seen)-1]
				if last.Get("X-Tenant-ID") == "" || last.Get("X-Dynamo-Request-Priority") == "" {
					t.Errorf("phoebe-added shared policy headers missing upstream: %v", last)
				}
			}
		})
	}
}

// TestConfiguredTrustedHeadersOmissionIsStripped: a name left out of a
// configured PHOEBE_TRUSTED_HEADERS (a future retirement) is stripped before
// forwarding even though the request carries it, while the remaining trusted
// names still arrive.
func TestConfiguredTrustedHeadersOmissionIsStripped(t *testing.T) {
	retired := identity.HeaderOwnerRateLimitRequests
	var keep []string
	for name := range identity.ActiveTrustedHeaders() {
		if name != retired {
			keep = append(keep, name)
		}
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
	if req.Header.Get(retired) == "" {
		t.Fatalf("fixture must carry %s", retired)
	}
	sent := req.Header.Clone()
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}

	forwardable := make(map[string]bool, len(forwardableSaturn))
	for name := range forwardableSaturn {
		forwardable[name] = name != retired
	}
	assertUpstreamSaturnHeaders(t, rec.all(), sent, []string{retired}, forwardable)
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

// TestUntrustedSaturnTrailersStrippedOverWire: a client's X-Saturn-* request
// trailer outside the forwardable set never reaches the upstream, on a real
// connection where the server fills in trailer values only after the body has
// been read (ruling Q-R8STRIP option b covers headers and trailers). Covered:
// a declared and an undeclared trailer name (the server merges both), a
// non-empty and an empty body, and the dedicated and gateway routes. A
// harmless trailer must still arrive, proving the trailer channel is live.
func TestUntrustedSaturnTrailersStrippedOverWire(t *testing.T) {
	const allTrailers = "X-Saturn-Service-Tier, X-Saturn-Foo, X-Other-Trailer"
	trailerLines := []string{"X-Saturn-Service-Tier: premium", "X-Saturn-Foo: bar", "X-Other-Trailer: ok"}

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
				if !tc.noControl && tr.Get("X-Other-Trailer") != "ok" {
					t.Fatalf("control trailer did not arrive (%v); the test cannot observe trailers", tr)
				}
				for name := range tr {
					canon := http.CanonicalHeaderKey(name)
					if strings.HasPrefix(canon, "X-Saturn-") && !forwardableSaturn[canon] {
						t.Fatalf("upstream received untrusted X-Saturn-* trailer %s=%v", name, tr[name])
					}
				}
			}
		})
	}
}
