package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/saturncloud/phoebe/internal/logging"
)

// fakeAtlasCheck records what the relay sent and answers as configured.
type fakeAtlasCheck struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   []string
	status   int
	headers  map[string]string
	body     string
}

func (f *fakeAtlasCheck) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.requests = append(f.requests, r.Clone(r.Context()))
	f.bodies = append(f.bodies, string(b))
	f.mu.Unlock()
	for k, v := range f.headers {
		w.Header().Set(k, v)
	}
	status := f.status
	if status == 0 {
		status = http.StatusNoContent
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(f.body))
}

func (f *fakeAtlasCheck) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func relayFor(t *testing.T, atlas http.Handler) (*OrgHostCheckRelay, *httptest.Server) {
	t.Helper()
	srv := httptest.NewTLSServer(atlas)
	t.Cleanup(srv.Close)
	relay, err := NewOrgHostCheckRelay(srv.URL+"/api/auth/org-host/check", srv.Client().Transport, logging.New(logging.ERROR))
	if err != nil {
		t.Fatal(err)
	}
	return relay, srv
}

func checkRequest(hosts ...string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, OrgHostCheckPath, strings.NewReader("should never be sent"))
	for _, h := range hosts {
		req.Header.Add("X-Forwarded-Host", h)
	}
	req.Header.Set("Authorization", "Bearer sk-org")
	req.Header.Set("Cookie", "session=secret")
	req.Header.Set("X-Saturn-Org-Id", "forged")
	req.Header.Set("X-Forwarded-Uri", "/v1/chat/completions")
	return req
}

func TestOrgHostCheckRelaySendsOnlyTheHostAndTheKey(t *testing.T) {
	atlas := &fakeAtlasCheck{}
	relay, _ := relayFor(t, atlas)
	rr := httptest.NewRecorder()
	relay.Handler().ServeHTTP(rr, checkRequest("acme&x=1-inference.spoke.example.com"))
	if rr.Code != http.StatusNoContent || atlas.count() != 1 {
		t.Fatalf("status %d atlas hits %d", rr.Code, atlas.count())
	}
	got := atlas.requests[0]
	if got.Method != http.MethodGet || got.URL.Path != "/api/auth/org-host/check" {
		t.Fatalf("relay sent %s %s", got.Method, got.URL.Path)
	}
	if q := got.URL.Query(); len(q) != 1 || q.Get("host") != "acme&x=1-inference.spoke.example.com" {
		t.Fatalf("query %v: the host must arrive URL-encoded as the only parameter", got.URL.RawQuery)
	}
	allowed := map[string]bool{"Authorization": true, "User-Agent": true, "Accept-Encoding": true}
	for name := range got.Header {
		if !allowed[name] {
			t.Fatalf("relay sent header %s", name)
		}
	}
	if got.Header.Get("Authorization") != "Bearer sk-org" || atlas.bodies[0] != "" {
		t.Fatalf("authorization %q body %q", got.Header.Get("Authorization"), atlas.bodies[0])
	}
}

func TestOrgHostCheckRelayRelaysOnlyTheIdentityHeaders(t *testing.T) {
	atlas := &fakeAtlasCheck{headers: map[string]string{
		"X-Saturn-Org-Id": "org-1", "X-Saturn-Auth-Id": "auth-1", "X-Saturn-Owner-Id": "owner-1",
		"X-Saturn-Endpoint-Scope": "dep-1", "X-Saturn-Upstream": "evil:1", "Set-Cookie": "a=b",
	}}
	relay, _ := relayFor(t, atlas)
	rr := httptest.NewRecorder()
	relay.Handler().ServeHTTP(rr, checkRequest("acme-inference.spoke.example.com"))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("status %d", rr.Code)
	}
	for _, name := range []string{"X-Saturn-Org-Id", "X-Saturn-Auth-Id", "X-Saturn-Owner-Id", "X-Saturn-Endpoint-Scope"} {
		if rr.Header().Get(name) == "" {
			t.Fatalf("missing %s", name)
		}
	}
	if rr.Header().Get("X-Saturn-Upstream") != "" || rr.Header().Get("Set-Cookie") != "" {
		t.Fatalf("relayed a header outside the allowlist: %v", rr.Header())
	}
}

func TestOrgHostCheckRelayPassesAtlasRefusalsThrough(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		body := `{"error": {"message": "m", "type": "permission_error", "code": "wrong_organization", "param": null}}`
		atlas := &fakeAtlasCheck{status: status, body: body, headers: map[string]string{
			"Content-Type": "application/json", "X-Saturn-Org-Id": "org-1",
		}}
		relay, _ := relayFor(t, atlas)
		rr := httptest.NewRecorder()
		relay.Handler().ServeHTTP(rr, checkRequest("acme-inference.spoke.example.com"))
		if rr.Code != status || rr.Body.String() != body || rr.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("status %d body %q", rr.Code, rr.Body.String())
		}
		if rr.Header().Get("X-Saturn-Org-Id") != "" {
			t.Fatal("a refusal must not carry identity headers")
		}
	}
}

func TestOrgHostCheckRelayFailsClosed(t *testing.T) {
	cases := map[string]*fakeAtlasCheck{
		"redirect to login": {status: http.StatusFound, headers: map[string]string{"Location": "/login", "X-Saturn-Org-Id": "org-1"}},
		"atlas 500":         {status: http.StatusInternalServerError},
		"atlas 503":         {status: http.StatusServiceUnavailable},
		"oversized answer":  {status: http.StatusOK, body: strings.Repeat("x", orgHostCheckMaxBody+10)},
	}
	for name, atlas := range cases {
		t.Run(name, func(t *testing.T) {
			relay, _ := relayFor(t, atlas)
			rr := httptest.NewRecorder()
			relay.Handler().ServeHTTP(rr, checkRequest("acme-inference.spoke.example.com"))
			if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("X-Saturn-Org-Id") != "" {
				t.Fatalf("status %d headers %v, want 503 with no identity", rr.Code, rr.Header())
			}
			if !strings.Contains(rr.Body.String(), `"error"`) {
				t.Fatalf("refusal is not JSON: %s", rr.Body.String())
			}
		})
	}
	t.Run("atlas unreachable", func(t *testing.T) {
		relay, srv := relayFor(t, &fakeAtlasCheck{})
		srv.Close()
		rr := httptest.NewRecorder()
		relay.Handler().ServeHTTP(rr, checkRequest("acme-inference.spoke.example.com"))
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("status %d", rr.Code)
		}
	})
	t.Run("untrusted certificate", func(t *testing.T) {
		atlas := &fakeAtlasCheck{}
		srv := httptest.NewTLSServer(atlas)
		defer srv.Close()
		relay, err := NewOrgHostCheckRelay(srv.URL+"/api/auth/org-host/check", nil, logging.New(logging.ERROR))
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		relay.Handler().ServeHTTP(rr, checkRequest("acme-inference.spoke.example.com"))
		if rr.Code != http.StatusServiceUnavailable || atlas.count() != 0 {
			t.Fatalf("status %d atlas hits %d: a self-signed Atlas must be refused", rr.Code, atlas.count())
		}
	})
}

func TestOrgHostCheckRelayNeedsExactlyOneForwardedHost(t *testing.T) {
	for name, hosts := range map[string][]string{
		"none":         nil,
		"two values":   {"a-inference.x.com", "b-inference.x.com"},
		"comma joined": {"a-inference.x.com, b-inference.x.com"},
		"empty":        {""},
	} {
		t.Run(name, func(t *testing.T) {
			atlas := &fakeAtlasCheck{}
			relay, _ := relayFor(t, atlas)
			rr := httptest.NewRecorder()
			relay.Handler().ServeHTTP(rr, checkRequest(hosts...))
			if rr.Code != http.StatusForbidden || atlas.count() != 0 {
				t.Fatalf("status %d atlas hits %d", rr.Code, atlas.count())
			}
			if rr.Body.String() != string(orgHostCheckWrongOrganization) {
				t.Fatalf("body %s, want Atlas's own wrong_organization body", rr.Body.String())
			}
		})
	}
}

func TestOrgHostCheckRelayServesOnlyItsPathAndNeverCaches(t *testing.T) {
	atlas := &fakeAtlasCheck{}
	relay, _ := relayFor(t, atlas)
	for _, req := range []*http.Request{
		httptest.NewRequest(http.MethodPost, OrgHostCheckPath, nil),
		httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil),
		httptest.NewRequest(http.MethodGet, OrgHostCheckPath+"/x", nil),
	} {
		req.Header.Set("X-Forwarded-Host", "acme-inference.spoke.example.com")
		rr := httptest.NewRecorder()
		relay.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("%s %s: status %d", req.Method, req.URL.Path, rr.Code)
		}
	}
	if atlas.count() != 0 {
		t.Fatalf("atlas hits %d for refused paths", atlas.count())
	}
	for i := 0; i < 2; i++ {
		rr := httptest.NewRecorder()
		relay.Handler().ServeHTTP(rr, checkRequest("acme-inference.spoke.example.com"))
	}
	if atlas.count() != 2 {
		t.Fatalf("atlas hits %d, want 2 (no cache)", atlas.count())
	}
}
