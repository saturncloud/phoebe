package proxy

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/videos"
)

const testVideoLimits = `{"tasks":["t2v"],"sizes":["1280x704"],"seconds":[5],"fps":24,"max_steps":50,"max_concurrency":1,"jobs_in_flight":20,"gpus":1}`

type videoBackend struct {
	mu   sync.Mutex
	reqs []*http.Request
}

func (b *videoBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	b.reqs = append(b.reqs, r.Clone(r.Context()))
	b.mu.Unlock()
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"object":"video"}`))
}

func videoRequest(method, path string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(`{"model":"my-wan","prompt":"x"}`))
	req.Header.Set(identity.HeaderUpstream, "pd-wan.ns.svc.cluster.local:8000")
	req.Header.Set(identity.HeaderServingMode, identity.ServingModeDedicated)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "res-1")
	req.Header.Set(identity.HeaderOrgID, "org-1")
	req.Header.Set(identity.HeaderBaseModel, "Wan-AI/Wan2.2-TI2V-5B-Diffusers")
	req.Header.Set(identity.HeaderServedModel, "my-wan")
	req.Header.Set(identity.HeaderVideoLimits, testVideoLimits)
	return req
}

func newVideoServer(t *testing.T) (*Server, *videoBackend) {
	t.Helper()
	backend := &videoBackend{}
	ts := httptest.NewServer(backend)
	t.Cleanup(ts.Close)
	srv, err := newTestServer(t, nil).WithVideos(ts.URL, "shared-token")
	if err != nil {
		t.Fatal(err)
	}
	return srv, backend
}

func TestVideoRouteHandsTheTrustedIdentityToPhoebeVideos(t *testing.T) {
	srv, backend := newVideoServer(t)
	req := videoRequest(http.MethodPost, "/v1/videos")
	// A client that tries to speak for phoebe itself is overwritten.
	req.Header.Set(videos.HeaderHandoff, "forged")
	req.Header.Set(videos.HeaderToken, "forged")
	req.Header.Set("X-Phoebe-Anything", "forged")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || len(backend.reqs) != 1 {
		t.Fatalf("status %d, backend calls %d", rr.Code, len(backend.reqs))
	}
	got := backend.reqs[0]
	if got.Header.Get(videos.HeaderToken) != "shared-token" {
		t.Error("the shared token was not set")
	}
	if got.Header.Get("X-Phoebe-Anything") != "" {
		t.Error("a client X-Phoebe-* header reached phoebe-videos")
	}
	for name := range got.Header {
		if strings.HasPrefix(name, "X-Saturn-") {
			t.Errorf("%s reached phoebe-videos", name)
		}
	}
	if got.URL.Path != "/v1/videos" {
		t.Errorf("path %q", got.URL.Path)
	}
	if rr.Header().Get(requestIDHeader) == "" {
		t.Error("no request id on the response")
	}
}

func TestVideoRouteServesOnlyTheVideoSurface(t *testing.T) {
	srv, backend := newVideoServer(t)
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{http.MethodPost, "/v1/videos", 200},
		{http.MethodGet, "/v1/videos", 200},
		{http.MethodGet, "/v1/videos/video_01ARZ3NDEKTSV4RRFFQ69G5FAV", 200},
		{http.MethodDelete, "/v1/videos/video_01ARZ3NDEKTSV4RRFFQ69G5FAV", 200},
		{http.MethodGet, "/v1/videos/video_01ARZ3NDEKTSV4RRFFQ69G5FAV/content", 200},
		{http.MethodPost, "/v1/chat/completions", 404},
		{http.MethodPost, "/v1/videos/sync", 404},
		{http.MethodGet, "/v1/models", 404},
		{http.MethodGet, "/health", 404},
		{http.MethodGet, "/metrics", 404},
		{http.MethodPut, "/v1/videos", 404},
		{http.MethodPost, "/v1/videos/video_01ARZ3NDEKTSV4RRFFQ69G5FAV/remix", 404},
		{http.MethodGet, "/v1/videos/x/content/extra", 404},
		{http.MethodDelete, "/v1/videos", 404},
		{http.MethodOptions, "/v1/videos", 404},
	} {
		before := len(backend.reqs)
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, videoRequest(tc.method, tc.path))
		if rr.Code != tc.want {
			t.Errorf("%s %s: %d, want %d", tc.method, tc.path, rr.Code, tc.want)
		}
		if tc.want == 404 && len(backend.reqs) != before {
			t.Errorf("%s %s reached phoebe-videos", tc.method, tc.path)
		}
	}
	// A percent-encoded path is refused, like every other route gate.
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, videoRequest(http.MethodGet, "/v1%2Fvideos"))
	if rr.Code != 404 {
		t.Errorf("encoded path: %d", rr.Code)
	}
}

func TestVideoRouteFailsClosed(t *testing.T) {
	srv, backend := newVideoServer(t)
	for _, tc := range []struct {
		reason string
		mut    func(*http.Request)
	}{
		{"shared serving mode", func(r *http.Request) { r.Header.Set(identity.HeaderServingMode, identity.ServingModeShared) }},
		{"no served model", func(r *http.Request) { r.Header.Del(identity.HeaderServedModel) }},
		{"no org", func(r *http.Request) { r.Header.Del(identity.HeaderOrgID) }},
		{"no base model", func(r *http.Request) { r.Header.Del(identity.HeaderBaseModel) }},
	} {
		req := videoRequest(http.MethodPost, "/v1/videos")
		tc.mut(req)
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, req)
		if rr.Code != 404 {
			t.Errorf("%s: %d, want 404", tc.reason, rr.Code)
		}
	}
	// Missing billing identity keeps the existing 400.
	req := videoRequest(http.MethodPost, "/v1/videos")
	req.Header.Del(identity.HeaderAuthID)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 400 {
		t.Errorf("no auth id: %d", rr.Code)
	}
	if len(backend.reqs) != 0 {
		t.Errorf("%d refused requests reached phoebe-videos", len(backend.reqs))
	}

	// Video generation off on this phoebe: 404.
	off := newTestServer(t, nil)
	rr = httptest.NewRecorder()
	off.Handler().ServeHTTP(rr, videoRequest(http.MethodPost, "/v1/videos"))
	if rr.Code != 404 {
		t.Errorf("videos off: %d", rr.Code)
	}
}

func TestChatRouteServesNoVideoPaths(t *testing.T) {
	srv, backend := newVideoServer(t)
	req := videoRequest(http.MethodPost, "/v1/videos")
	req.Header.Del(identity.HeaderVideoLimits)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 404 || len(backend.reqs) != 0 {
		t.Fatalf("a chat route reached phoebe-videos: %d", rr.Code)
	}
}

func TestWithVideosRefusesABadServiceURL(t *testing.T) {
	for _, u := range []string{"https://x:1", "x:1", "http://x:1/path", "ftp://x"} {
		if _, err := newTestServer(t, nil).WithVideos(u, "t"); err == nil {
			t.Errorf("accepted %q", u)
		}
	}
	srv, err := newTestServer(t, nil).WithVideos("http://x:1", "")
	if err != nil || srv.videos != nil {
		t.Error("an empty token must leave videos off")
	}
}

const testVideoID = "video_01ARZ3NDEKTSV4RRFFQ69G5FAV"

// orgHostVideoRequest is what the tf-org-hosts route delivers for a video
// request (issue #71): the org-host mark, the host's org and the caller's key.
// No upstream, serving mode, served model or video limits.
func orgHostVideoRequest(method, target, body string) *http.Request {
	req := orgHostRequest(method, target, "org-1", body)
	if body == "" {
		req.Header.Del("Content-Type")
	}
	return req
}

func videoRow(name, resource, upstream string) gateway.DedicatedEndpoint {
	ep := dedicatedRow("org-1", name, resource, upstream)
	ep.BaseModel = "Wan-AI/Wan2.2-TI2V-5B-Diffusers"
	ep.VideoLimits = testVideoLimits
	return ep
}

func testOrgHostVideoRows() []gateway.DedicatedEndpoint {
	stopped := videoRow("stopped-wan", "res-5", "pd-wan5.ns.svc.cluster.local:8000")
	stopped.State = gateway.EndpointStateStopped
	legacy := videoRow("legacy-wan", "res-6", "pd-wan6.ns.svc.cluster.local:8000")
	legacy.Aliases = []string{"legacy-wan-base"}
	return []gateway.DedicatedEndpoint{
		videoRow("my-wan", "res-1", "pd-wan.ns.svc.cluster.local:8000"),
		videoRow("other-wan", "res-2", "pd-wan2.ns.svc.cluster.local:8000"),
		dedicatedRow("org-1", "my-chat", "res-3", "pd-chat.ns.svc.cluster.local:8000"),
		stopped, legacy,
		videoRow("their-wan", "res-9", "pd-x.ns.svc.cluster.local:8000"),
	}
}

// newOrgHostVideoServer is an interceptor with org hosts and videos on, over
// the given dedicated rows (another org's row is moved to org-2).
func newOrgHostVideoServer(t *testing.T, rows []gateway.DedicatedEndpoint) (*Server, *videoBackend) {
	t.Helper()
	for i := range rows {
		if rows[i].Name == "their-wan" {
			rows[i].OrgID = "org-2"
		}
	}
	backend := &videoBackend{}
	ts := httptest.NewServer(backend)
	t.Cleanup(ts.Close)
	srv, err := newOrgHostTestServer(t, nil, &fakeTierResolver{dedicated: rows}, nil).WithVideos(ts.URL, "shared-token")
	if err != nil {
		t.Fatal(err)
	}
	return srv, backend
}

func serveOrgHost(srv *Server, req *http.Request) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	return rr
}

func scoped(req *http.Request, endpoint string) *http.Request {
	req.Header.Set(identity.HeaderEndpointScope, endpoint)
	return req
}

func handoffOf(t *testing.T, r *http.Request) videos.Handoff {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(r.Header.Get(videos.HeaderHandoff))
	if err != nil {
		t.Fatal(err)
	}
	var h videos.Handoff
	if err := json.Unmarshal(raw, &h); err != nil {
		t.Fatal(err)
	}
	return h
}

func clientErrorCode(rr *httptest.ResponseRecorder) string {
	var e struct{ Error clientError }
	_ = json.Unmarshal(rr.Body.Bytes(), &e)
	return e.Error.Code
}

func TestOrgHostVideoCreatePicksEndpointByModel(t *testing.T) {
	srv, backend := newOrgHostVideoServer(t, testOrgHostVideoRows())
	for _, tc := range []struct{ model, resource, upstream, served string }{
		{"my-wan", "res-1", "pd-wan.ns.svc.cluster.local:8000", "my-wan"},
		{"other-wan", "res-2", "pd-wan2.ns.svc.cluster.local:8000", "other-wan"},
		// A legacy alias picks its endpoint; phoebe-videos gets every served name.
		{"legacy-wan-base", "res-6", "pd-wan6.ns.svc.cluster.local:8000", "legacy-wan,legacy-wan-base"},
	} {
		before := len(backend.reqs)
		body := `{"model":"` + tc.model + `","prompt":"a cat"}`
		req := orgHostVideoRequest(http.MethodPost, "/v1/videos", body)
		// Client forgeries of the handoff are replaced.
		req.Header.Set(videos.HeaderHandoff, "forged")
		rr := serveOrgHost(srv, req)
		if rr.Code != http.StatusOK || len(backend.reqs) != before+1 {
			t.Fatalf("%s: status %d %s", tc.model, rr.Code, rr.Body)
		}
		got := backend.reqs[len(backend.reqs)-1]
		h := handoffOf(t, got)
		want := videos.Handoff{OrgID: "org-1", AuthID: "auth-1", ResourceID: tc.resource, ServedModel: tc.served,
			BaseModel: "Wan-AI/Wan2.2-TI2V-5B-Diffusers", ServingMode: "dedicated", Upstream: tc.upstream, Limits: testVideoLimits}
		if h != want {
			t.Fatalf("%s: handoff %+v, want %+v", tc.model, h, want)
		}
		// phoebe-videos gets the whole body back to parse itself.
		if got.ContentLength != int64(len(body)) {
			t.Fatalf("%s: forwarded %d body bytes, want %d", tc.model, got.ContentLength, len(body))
		}
		for name := range got.Header {
			if strings.HasPrefix(name, "X-Saturn-") {
				t.Errorf("%s reached phoebe-videos", name)
			}
		}
		if got.Header.Get("Authorization") != "" {
			t.Error("the caller's key reached phoebe-videos")
		}
	}
	// Multipart (the OpenAI SDK with an uploaded input_reference), model last.
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("prompt", "a cat")
	fw, _ := mw.CreateFormFile("input_reference", "x.png")
	_, _ = fw.Write([]byte("\x89PNG...."))
	_ = mw.WriteField("model", "other-wan")
	_ = mw.Close()
	req := orgHostVideoRequest(http.MethodPost, "/v1/videos", "")
	req.Body = io.NopCloser(bytes.NewReader(buf.Bytes()))
	req.ContentLength = int64(buf.Len())
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rr := serveOrgHost(srv, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("multipart: %d %s", rr.Code, rr.Body)
	}
	if h := handoffOf(t, backend.reqs[len(backend.reqs)-1]); h.ResourceID != "res-2" {
		t.Fatalf("multipart picked %q", h.ResourceID)
	}
}

func TestOrgHostVideoCreateRefusals(t *testing.T) {
	srv, backend := newOrgHostVideoServer(t, testOrgHostVideoRows())
	for _, tc := range []struct {
		name, body, scope string
		want              int
		code              string
	}{
		{"unknown model", `{"model":"nope","prompt":"x"}`, "", 404, "model_not_found"},
		{"another org's endpoint", `{"model":"their-wan","prompt":"x"}`, "", 404, "model_not_found"},
		{"chat endpoint", `{"model":"my-chat","prompt":"x"}`, "", 404, "not_a_video_endpoint"},
		{"stopped endpoint", `{"model":"stopped-wan","prompt":"x"}`, "", 409, "endpoint_not_running"},
		{"no model", `{"prompt":"x"}`, "", 400, "invalid_request"},
		{"model not a string", `{"model":3,"prompt":"x"}`, "", 400, "invalid_request"},
		{"not JSON", `nope`, "", 400, "invalid_request"},
		{"scoped key, other endpoint", `{"model":"other-wan","prompt":"x"}`, "0123456789abcdef0123456789abcdef", 403, "model_not_authorized"},
	} {
		req := orgHostVideoRequest(http.MethodPost, "/v1/videos", tc.body)
		if tc.scope != "" {
			scoped(req, tc.scope)
		}
		rr := serveOrgHost(srv, req)
		if rr.Code != tc.want || clientErrorCode(rr) != tc.code {
			t.Errorf("%s: %d %s, want %d %s", tc.name, rr.Code, rr.Body, tc.want, tc.code)
		}
	}
	if len(backend.reqs) != 0 {
		t.Fatalf("%d refused creates reached phoebe-videos", len(backend.reqs))
	}
	// Another org's endpoint answers exactly like an unknown name.
	a := serveOrgHost(srv, orgHostVideoRequest(http.MethodPost, "/v1/videos", `{"model":"their-wan","prompt":"x"}`))
	b := serveOrgHost(srv, orgHostVideoRequest(http.MethodPost, "/v1/videos", `{"model":"nope","prompt":"x"}`))
	if a.Code != b.Code || a.Body.String() != b.Body.String() {
		t.Fatalf("another org's model %d %s differs from an unknown one %d %s", a.Code, a.Body, b.Code, b.Body)
	}
}

// Endpoint ids in these tests are 32-hex, as the scope header requires.
const (
	scopeRes1 = "11111111111111111111111111111111"
	scopeRes2 = "22222222222222222222222222222222"
)

func scopedRows() []gateway.DedicatedEndpoint {
	return []gateway.DedicatedEndpoint{
		videoRow("my-wan", scopeRes1, "pd-wan.ns.svc.cluster.local:8000"),
		videoRow("other-wan", scopeRes2, "pd-wan2.ns.svc.cluster.local:8000"),
	}
}

func TestOrgHostVideoScopedKeyCreatesOnlyOnItsEndpoint(t *testing.T) {
	srv, backend := newOrgHostVideoServer(t, scopedRows())
	if rr := serveOrgHost(srv, scoped(orgHostVideoRequest(http.MethodPost, "/v1/videos", `{"model":"my-wan","prompt":"x"}`), scopeRes1)); rr.Code != 200 {
		t.Fatalf("own endpoint: %d %s", rr.Code, rr.Body)
	}
	rr := serveOrgHost(srv, scoped(orgHostVideoRequest(http.MethodPost, "/v1/videos", `{"model":"other-wan","prompt":"x"}`), scopeRes1))
	if rr.Code != 403 || clientErrorCode(rr) != "model_not_authorized" || len(backend.reqs) != 1 {
		t.Fatalf("other endpoint: %d %s", rr.Code, rr.Body)
	}
}

func TestOrgHostVideoReadsListAndDeletesCarryOnlyTheOrgAndScope(t *testing.T) {
	srv, backend := newOrgHostVideoServer(t, scopedRows())
	for _, tc := range []struct {
		method, target, scope string
		want                  videos.Handoff
	}{
		{http.MethodGet, "/v1/videos/" + testVideoID, "", videos.Handoff{OrgHost: true, OrgID: "org-1", AuthID: "auth-1"}},
		{http.MethodGet, "/v1/videos/" + testVideoID + "/content", scopeRes1, videos.Handoff{OrgHost: true, OrgID: "org-1", AuthID: "auth-1", ResourceID: scopeRes1}},
		{http.MethodDelete, "/v1/videos/" + testVideoID, "", videos.Handoff{OrgHost: true, OrgID: "org-1", AuthID: "auth-1"}},
		{http.MethodGet, "/v1/videos", "", videos.Handoff{OrgHost: true, OrgID: "org-1", AuthID: "auth-1"}},
		{http.MethodGet, "/v1/videos?limit=5", scopeRes2, videos.Handoff{OrgHost: true, OrgID: "org-1", AuthID: "auth-1", ResourceID: scopeRes2}},
	} {
		before := len(backend.reqs)
		req := orgHostVideoRequest(tc.method, tc.target, "")
		if tc.scope != "" {
			scoped(req, tc.scope)
		}
		rr := serveOrgHost(srv, req)
		if rr.Code != 200 || len(backend.reqs) != before+1 {
			t.Fatalf("%s %s: %d %s", tc.method, tc.target, rr.Code, rr.Body)
		}
		got := backend.reqs[len(backend.reqs)-1]
		if h := handoffOf(t, got); h != tc.want {
			t.Errorf("%s %s scope %q: handoff %+v, want %+v", tc.method, tc.target, tc.scope, h, tc.want)
		}
		for name := range got.Header {
			if strings.HasPrefix(name, "X-Saturn-") {
				t.Errorf("%s reached phoebe-videos", name)
			}
		}
	}
}

func TestOrgHostVideoListModelFilter(t *testing.T) {
	rows := scopedRows()
	rows = append(rows, videoRow("their-wan", "33333333333333333333333333333333", "pd-x.ns.svc.cluster.local:8000"))
	srv, backend := newOrgHostVideoServer(t, rows)
	for _, tc := range []struct {
		query, scope string
		resource     string
		noMatch      bool
	}{
		{"model=my-wan", "", scopeRes1, false},
		{"model=other-wan&limit=3", "", scopeRes2, false},
		{"model=my-wan", scopeRes1, scopeRes1, false},
		// TestOrgHostVideoListUnknownModelIsEmptyNot404 at the interceptor: an
		// unknown name, another org's name, and another endpoint's name for a
		// scoped key all become an empty list in phoebe-videos, never a 404.
		{"model=nope", "", "", true},
		{"model=their-wan", "", "", true},
		{"model=other-wan", scopeRes1, "", true},
		{"model=", "", "", true},
	} {
		before := len(backend.reqs)
		req := orgHostVideoRequest(http.MethodGet, "/v1/videos?"+tc.query, "")
		if tc.scope != "" {
			scoped(req, tc.scope)
		}
		rr := serveOrgHost(srv, req)
		if rr.Code != 200 || len(backend.reqs) != before+1 {
			t.Fatalf("%s: %d %s", tc.query, rr.Code, rr.Body)
		}
		h := handoffOf(t, backend.reqs[len(backend.reqs)-1])
		if h.ResourceID != tc.resource || h.NoMatch != tc.noMatch || !h.OrgHost {
			t.Errorf("%s scope %q: handoff %+v", tc.query, tc.scope, h)
		}
	}
	before := len(backend.reqs)
	rr := serveOrgHost(srv, orgHostVideoRequest(http.MethodGet, "/v1/videos?model=my-wan&model=other-wan", ""))
	if rr.Code != 400 || len(backend.reqs) != before {
		t.Fatalf("model twice: %d", rr.Code)
	}
}

func TestOrgHostVideoFailsClosed(t *testing.T) {
	srv, backend := newOrgHostVideoServer(t, testOrgHostVideoRows())
	for _, tc := range []struct {
		reason string
		mut    func(*http.Request)
	}{
		{"no org", func(r *http.Request) { r.Header.Del(identity.HeaderOrgID) }},
		{"no auth id", func(r *http.Request) { r.Header.Del(identity.HeaderAuthID) }},
		{"gateway mark", func(r *http.Request) { r.Header.Set(identity.HeaderGateway, "true") }},
		{"upstream stamp", func(r *http.Request) { r.Header.Set(identity.HeaderUpstream, "x.ns.svc:8000") }},
		{"serving mode stamp", func(r *http.Request) { r.Header.Set(identity.HeaderServingMode, "dedicated") }},
		{"served model stamp", func(r *http.Request) { r.Header.Set(identity.HeaderServedModel, "my-wan") }},
		{"video limits stamp", func(r *http.Request) { r.Header.Set(identity.HeaderVideoLimits, testVideoLimits) }},
		{"resource id stamp", func(r *http.Request) { r.Header.Set(identity.HeaderResourceID, "res-1") }},
		{"empty scope", func(r *http.Request) { r.Header.Set(identity.HeaderEndpointScope, "") }},
		{"malformed scope", func(r *http.Request) { r.Header.Set(identity.HeaderEndpointScope, "res-1,res-2") }},
	} {
		req := orgHostVideoRequest(http.MethodGet, "/v1/videos/"+testVideoID, "")
		tc.mut(req)
		if rr := serveOrgHost(srv, req); rr.Code < 400 || rr.Code == http.StatusOK {
			t.Errorf("%s: %d, want a refusal", tc.reason, rr.Code)
		}
	}
	for _, tc := range []struct{ method, path string }{
		{http.MethodPut, "/v1/videos"}, {http.MethodDelete, "/v1/videos"},
		{http.MethodPost, "/v1/videos/" + testVideoID + "/remix"},
		{http.MethodGet, "/v1/videos/" + testVideoID + "%2Fcontent"},
		{http.MethodGet, "/v1%2Fvideos/" + testVideoID},
		{http.MethodGet, "/v1/videos/x/content/extra"},
	} {
		rr := serveOrgHost(srv, orgHostVideoRequest(tc.method, tc.path, ""))
		if rr.Code != 404 || clientErrorCode(rr) != "not_found" {
			t.Errorf("%s %s: %d %s, want the JSON 404", tc.method, tc.path, rr.Code, rr.Body)
		}
	}
	if len(backend.reqs) != 0 {
		t.Errorf("%d refused requests reached phoebe-videos", len(backend.reqs))
	}
	// Video generation off on this phoebe: reads and creates are 404.
	off := newOrgHostTestServer(t, nil, &fakeTierResolver{dedicated: testOrgHostVideoRows()}, nil)
	for _, req := range []*http.Request{
		orgHostVideoRequest(http.MethodGet, "/v1/videos", ""),
		orgHostVideoRequest(http.MethodPost, "/v1/videos", `{"model":"my-wan","prompt":"x"}`),
	} {
		if rr := serveOrgHost(off, req); rr.Code != 404 {
			t.Errorf("videos off: %d %s", rr.Code, rr.Body)
		}
	}
}

// A create for an endpoint on another cluster is forwarded to that
// cluster's org host with the caller's key; nothing reaches the local
// phoebe-videos, and a request that was already forwarded is not forwarded
// again.
func TestOrgHostVideoCreateForwardsToTheServingCluster(t *testing.T) {
	be := &orgEngine{}
	serving, base := tlsServing(t, be)
	remote := forwardRow("org-1", "remote-wan", "res-4", base, "spoke")
	remote.VideoLimits = testVideoLimits
	srv, backend := newOrgHostVideoServer(t, []gateway.DedicatedEndpoint{remote})
	srv.orgHosts.forward.transport = serving.Client().Transport
	body := `{"model":"remote-wan","prompt":"x"}`
	rr := serveOrgHost(srv, orgHostVideoRequest(http.MethodPost, "/v1/videos", body))
	if rr.Code != http.StatusOK || be.hits.Load() != 1 || len(backend.reqs) != 0 {
		t.Fatalf("status %d, serving hits %d, local phoebe-videos %d: %s", rr.Code, be.hits.Load(), len(backend.reqs), rr.Body)
	}
	be.mu.Lock()
	path, hdr, sent := be.paths[0], be.headers[0], string(be.bodies[0])
	be.mu.Unlock()
	if path != "/v1/videos" || sent != body || hdr.Get("Authorization") != "Bearer sk-test" || hdr.Get(orgHostForwardedHeader) == "" {
		t.Fatalf("forwarded %s %q headers %v", path, sent, hdr)
	}
	for name := range hdr {
		if strings.HasPrefix(name, "X-Saturn-") {
			t.Errorf("%s was forwarded", name)
		}
	}
	again := orgHostVideoRequest(http.MethodPost, "/v1/videos", body)
	again.Header.Set(orgHostForwardedHeader, "1")
	if rr := serveOrgHost(srv, again); rr.Code != http.StatusLoopDetected || be.hits.Load() != 1 {
		t.Fatalf("second forward: %d hits %d", rr.Code, be.hits.Load())
	}
}

// The org-host handoff stops the caller's key and cookies at the interceptor
// and replaces the client's request id.
func TestOrgHostVideoHandoffDropsCredentialsAndReplacesRequestID(t *testing.T) {
	srv, backend := newOrgHostVideoServer(t, testOrgHostVideoRows())
	req := orgHostVideoRequest(http.MethodGet, "/v1/videos/"+testVideoID, "")
	req.Header.Set("Cookie", "a=b")
	req.Header.Set(requestIDHeader, "client-chosen")
	rr := serveOrgHost(srv, req)
	if rr.Code != 200 || len(backend.reqs) != 1 {
		t.Fatalf("status %d", rr.Code)
	}
	got := backend.reqs[0]
	if got.Header.Get("Authorization") != "" || got.Header.Get("Cookie") != "" {
		t.Error("caller credentials reached phoebe-videos")
	}
	if got.Header.Get(videos.HeaderToken) != "shared-token" {
		t.Error("the shared token was not set")
	}
	if sent := got.Header.Get(requestIDHeader); sent == "client-chosen" || sent == "" || sent != rr.Header().Get(requestIDHeader) {
		t.Errorf("request id %q (response %q)", sent, rr.Header().Get(requestIDHeader))
	}
}

// The issue #13 retrieval mark on the shared host is retired: a request that
// carries it (with the org, endpoint and key the old check stamped) is not a
// video request any more and never reaches phoebe-videos.
func TestRetiredVideoRetrievalMarkReachesNothing(t *testing.T) {
	srv, backend := newVideoServer(t)
	for _, path := range []string{"/v1/videos/" + testVideoID, "/v1/videos/" + testVideoID + "/content"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		// Retired in issue #71; no constant names it any more.
		req.Header.Set("X-Saturn-Video-Retrieval", "true")
		req.Header.Set(identity.HeaderOrgID, "org-1")
		req.Header.Set(identity.HeaderResourceID, "res-1")
		req.Header.Set(identity.HeaderAuthID, "auth-1")
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, req)
		if rr.Code == http.StatusOK {
			t.Errorf("%s: retired mark served", path)
		}
	}
	if len(backend.reqs) != 0 {
		t.Errorf("%d retired-mark requests reached phoebe-videos", len(backend.reqs))
	}
}

// captureWarn sends the server's warnings to a buffer the test can read.
func captureWarn(srv *Server) *lockedBuffer {
	buf := &lockedBuffer{}
	srv.log.Warn = log.New(buf, "", 0)
	return buf
}

// C13: a refusal names the path the client sent. Go leaves URL.RawPath empty
// for an ordinary path, so logging it showed "" for almost every refusal.
func TestVideoRefusalLogsThePath(t *testing.T) {
	srv, _ := newOrgHostVideoServer(t, testOrgHostVideoRows())
	warn := captureWarn(srv)
	path := "/v1/videos/" + testVideoID
	if rr := serveOrgHost(srv, orgHostVideoRequest(http.MethodPost, path, "")); rr.Code != 404 {
		t.Fatalf("status %d", rr.Code)
	}
	if got := warn.String(); !strings.Contains(got, fmt.Sprintf("path=%q", path)) {
		t.Fatalf("org-host refusal log %q does not name the path %q", got, path)
	}
	warn = captureWarn(srv)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, videoRequest(http.MethodPost, "/v1/videos/sync"))
	if got := warn.String(); rr.Code != 404 || !strings.Contains(got, `path="/v1/videos/sync"`) {
		t.Fatalf("status %d, refusal log %q does not name the path", rr.Code, got)
	}
}

// C14: the caller's Saturn key and cookies stop at the interceptor on the
// endpoint video route; phoebe-videos is reached with the shared token only.
func TestVideoHandoffDropsCallerCredentials(t *testing.T) {
	srv, backend := newVideoServer(t)
	req := videoRequest(http.MethodPost, "/v1/videos")
	req.Header.Set("Authorization", "Bearer k")
	req.Header.Set("Cookie", "a=b")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || len(backend.reqs) != 1 {
		t.Fatalf("status %d, backend calls %d", rr.Code, len(backend.reqs))
	}
	got := backend.reqs[0]
	if got.Header.Get("Authorization") != "" || got.Header.Get("Cookie") != "" {
		t.Error("caller credentials reached phoebe-videos")
	}
	if got.Header.Get(videos.HeaderToken) != "shared-token" {
		t.Error("the shared token was not set")
	}
}
