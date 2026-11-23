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

// orgHostVideoRequest is what the org-host route delivers (issue #71):
// Atlas's org-host ForwardAuth stamped the host's org and the caller's key,
// and the route's header middleware the org-host mark. No upstream, serving
// mode, served model or video limits.
func orgHostVideoRequest(method, target, body string) *http.Request {
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, target, nil)
	} else {
		req = httptest.NewRequest(method, target, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(identity.HeaderOrgID, "org-1")
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	return req
}

// fakeOrgHostEndpoints is the org-host path's registry view for one org:
// endpoint name -> row.
type fakeOrgHostEndpoints map[string]orgHostVideoEndpoint

func (f fakeOrgHostEndpoints) resolvers(scope string) orgHostVideoResolvers {
	return orgHostVideoResolvers{
		Create: func(w http.ResponseWriter, model string) (orgHostVideoEndpoint, bool) {
			ep, ok := f[model]
			if !ok {
				writeClientError(w, "", http.StatusNotFound, clientError{Message: "This organization has no dedicated endpoint with that name.", Type: "invalid_request_error", Code: "model_not_found"})
				return orgHostVideoEndpoint{}, false
			}
			if scope != "" && ep.ResourceID != scope {
				writeClientError(w, "", http.StatusForbidden, orgHostVideoModelNotAuthorized)
				return orgHostVideoEndpoint{}, false
			}
			return ep, true
		},
		EndpointID: func(name string) (string, bool) {
			ep, ok := f[name]
			return ep.ResourceID, ok
		},
	}
}

var testOrgHostEndpoints = fakeOrgHostEndpoints{
	"my-wan": {ResourceID: "res-1", ServedModels: []string{"my-wan"}, BaseModel: "Wan-AI/Wan2.2-TI2V-5B-Diffusers",
		Upstream: "pd-wan.ns.svc.cluster.local:8000", VideoLimits: testVideoLimits},
	"other-wan": {ResourceID: "res-2", ServedModels: []string{"other-wan"}, BaseModel: "Wan-AI/Wan2.2-TI2V-5B-Diffusers",
		Upstream: "pd-wan2.ns.svc.cluster.local:8000", VideoLimits: testVideoLimits},
	"my-chat": {ResourceID: "res-3", ServedModels: []string{"my-chat"}, BaseModel: "Qwen/Qwen3-8B",
		Upstream: "pd-chat.ns.svc.cluster.local:8000"},
	"remote-wan": {ResourceID: "res-4", ServedModels: []string{"remote-wan"}, BaseModel: "Wan-AI/Wan2.2-TI2V-5B-Diffusers",
		VideoLimits: testVideoLimits},
}

// serveOrgHost runs a request through the org-host video hook the way the
// org-host path does: identity parsed, X-Saturn-* stripped, canonical path.
func serveOrgHost(srv *Server, req *http.Request, scope string) *httptest.ResponseRecorder {
	id := identity.FromRequest(req)
	identity.StripSaturnHeaders(req.Header)
	routePath, canonical := canonicalRequestPath(req.URL)
	rr := httptest.NewRecorder()
	srv.serveOrgHostVideo(rr, req, id, scope, routePath, canonical, testOrgHostEndpoints.resolvers(scope))
	return rr
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

func TestOrgHostVideoCreatePicksEndpointByModel(t *testing.T) {
	srv, backend := newVideoServer(t)
	for _, tc := range []struct{ model, resource, upstream string }{
		{"my-wan", "res-1", "pd-wan.ns.svc.cluster.local:8000"},
		{"other-wan", "res-2", "pd-wan2.ns.svc.cluster.local:8000"},
	} {
		before := len(backend.reqs)
		body := `{"model":"` + tc.model + `","prompt":"a cat"}`
		req := orgHostVideoRequest(http.MethodPost, "/v1/videos", body)
		// Client forgeries of the handoff or the routing headers are ignored.
		req.Header.Set(videos.HeaderHandoff, "forged")
		rr := serveOrgHost(srv, req, "")
		if rr.Code != http.StatusOK || len(backend.reqs) != before+1 {
			t.Fatalf("%s: status %d %s", tc.model, rr.Code, rr.Body)
		}
		got := backend.reqs[len(backend.reqs)-1]
		h := handoffOf(t, got)
		want := videos.Handoff{OrgID: "org-1", AuthID: "auth-1", ResourceID: tc.resource, ServedModel: tc.model,
			BaseModel: "Wan-AI/Wan2.2-TI2V-5B-Diffusers", ServingMode: "dedicated", Upstream: tc.upstream, Limits: testVideoLimits}
		if h != want {
			t.Fatalf("%s: handoff %+v, want %+v", tc.model, h, want)
		}
		// phoebe-videos gets the whole body back to parse itself.
		if got.ContentLength != int64(len(body)) {
			t.Fatalf("%s: forwarded %d body bytes, want %d", tc.model, got.ContentLength, len(body))
		}
	}
	// Multipart (the OpenAI SDK with an uploaded input_reference).
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("prompt", "a cat")
	fw, _ := mw.CreateFormFile("input_reference", "x.png")
	_, _ = fw.Write([]byte("\x89PNG...."))
	_ = mw.WriteField("model", "other-wan")
	_ = mw.Close()
	req := orgHostVideoRequest(http.MethodPost, "/v1/videos", "")
	req.Body = io.NopCloser(bytes.NewReader(buf.Bytes()))
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rr := serveOrgHost(srv, req, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("multipart: %d %s", rr.Code, rr.Body)
	}
	if h := handoffOf(t, backend.reqs[len(backend.reqs)-1]); h.ResourceID != "res-2" {
		t.Fatalf("multipart picked %q", h.ResourceID)
	}
}

func TestOrgHostVideoCreateRefusals(t *testing.T) {
	srv, backend := newVideoServer(t)
	for _, tc := range []struct {
		name, body, scope string
		want              int
		code              string
	}{
		{"unknown model", `{"model":"nope","prompt":"x"}`, "", 404, "model_not_found"},
		{"chat endpoint", `{"model":"my-chat","prompt":"x"}`, "", 404, "not_found"},
		{"forwarding copy", `{"model":"remote-wan","prompt":"x"}`, "", 404, "not_found"},
		{"no model", `{"prompt":"x"}`, "", 400, "invalid_request"},
		{"model not a string", `{"model":3,"prompt":"x"}`, "", 400, "invalid_request"},
		{"not JSON", `nope`, "", 400, "invalid_request"},
		{"scoped key, other endpoint", `{"model":"other-wan","prompt":"x"}`, "res-1", 403, "model_not_authorized"},
	} {
		rr := serveOrgHost(srv, orgHostVideoRequest(http.MethodPost, "/v1/videos", tc.body), tc.scope)
		var e struct{ Error clientError }
		_ = json.Unmarshal(rr.Body.Bytes(), &e)
		if rr.Code != tc.want || e.Error.Code != tc.code {
			t.Errorf("%s: %d %s, want %d %s", tc.name, rr.Code, rr.Body, tc.want, tc.code)
		}
	}
	if len(backend.reqs) != 0 {
		t.Fatalf("%d refused creates reached phoebe-videos", len(backend.reqs))
	}
	// The scoped key's own endpoint is allowed.
	if rr := serveOrgHost(srv, orgHostVideoRequest(http.MethodPost, "/v1/videos", `{"model":"my-wan","prompt":"x"}`), "res-1"); rr.Code != 200 {
		t.Fatalf("scoped key, own endpoint: %d", rr.Code)
	}
}

func TestOrgHostVideoReadsListAndDeletesCarryOnlyTheOrgAndScope(t *testing.T) {
	srv, backend := newVideoServer(t)
	for _, tc := range []struct {
		method, target, scope string
		want                  videos.Handoff
	}{
		{http.MethodGet, "/v1/videos/" + testVideoID, "", videos.Handoff{OrgHost: true, OrgID: "org-1", AuthID: "auth-1"}},
		{http.MethodGet, "/v1/videos/" + testVideoID + "/content", "res-1", videos.Handoff{OrgHost: true, OrgID: "org-1", AuthID: "auth-1", ResourceID: "res-1"}},
		{http.MethodDelete, "/v1/videos/" + testVideoID, "", videos.Handoff{OrgHost: true, OrgID: "org-1", AuthID: "auth-1"}},
		{http.MethodGet, "/v1/videos", "", videos.Handoff{OrgHost: true, OrgID: "org-1", AuthID: "auth-1"}},
		{http.MethodGet, "/v1/videos?limit=5", "res-2", videos.Handoff{OrgHost: true, OrgID: "org-1", AuthID: "auth-1", ResourceID: "res-2"}},
	} {
		before := len(backend.reqs)
		rr := serveOrgHost(srv, orgHostVideoRequest(tc.method, tc.target, ""), tc.scope)
		if rr.Code != 200 || len(backend.reqs) != before+1 {
			t.Fatalf("%s %s: %d", tc.method, tc.target, rr.Code)
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
	srv, backend := newVideoServer(t)
	for _, tc := range []struct {
		query, scope string
		resource     string
		noMatch      bool
	}{
		{"model=my-wan", "", "res-1", false},
		{"model=other-wan&limit=3", "", "res-2", false},
		{"model=my-wan", "res-1", "res-1", false},
		// TestOrgHostVideoListUnknownModelIsEmptyNot404 at the interceptor: an
		// unknown name, and another endpoint's name for a scoped key, both
		// become an empty list in phoebe-videos (never a 404 here).
		{"model=nope", "", "", true},
		{"model=other-wan", "res-1", "", true},
		{"model=", "", "", true},
	} {
		before := len(backend.reqs)
		rr := serveOrgHost(srv, orgHostVideoRequest(http.MethodGet, "/v1/videos?"+tc.query, ""), tc.scope)
		if rr.Code != 200 || len(backend.reqs) != before+1 {
			t.Fatalf("%s: %d", tc.query, rr.Code)
		}
		h := handoffOf(t, backend.reqs[len(backend.reqs)-1])
		if h.ResourceID != tc.resource || h.NoMatch != tc.noMatch || !h.OrgHost {
			t.Errorf("%s scope %q: handoff %+v", tc.query, tc.scope, h)
		}
	}
	before := len(backend.reqs)
	rr := serveOrgHost(srv, orgHostVideoRequest(http.MethodGet, "/v1/videos?model=my-wan&model=other-wan", ""), "")
	if rr.Code != 400 || len(backend.reqs) != before {
		t.Fatalf("model twice: %d", rr.Code)
	}
}

func TestOrgHostVideoFailsClosed(t *testing.T) {
	srv, backend := newVideoServer(t)
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
	} {
		req := orgHostVideoRequest(http.MethodGet, "/v1/videos/"+testVideoID, "")
		tc.mut(req)
		if rr := serveOrgHost(srv, req, ""); rr.Code != 404 {
			t.Errorf("%s: %d, want 404", tc.reason, rr.Code)
		}
	}
	for _, tc := range []struct{ method, path string }{
		{http.MethodPut, "/v1/videos"}, {http.MethodDelete, "/v1/videos"},
		{http.MethodPost, "/v1/videos/" + testVideoID + "/remix"},
		{http.MethodGet, "/v1/videos/" + testVideoID + "%2Fcontent"},
		{http.MethodGet, "/v1%2Fvideos/" + testVideoID},
		{http.MethodGet, "/v1/videos/x/content/extra"},
	} {
		rr := serveOrgHost(srv, orgHostVideoRequest(tc.method, tc.path, ""), "")
		var e struct{ Error clientError }
		if rr.Code != 404 || json.Unmarshal(rr.Body.Bytes(), &e) != nil || e.Error.Code != "not_found" {
			t.Errorf("%s %s: %d %s, want the JSON 404", tc.method, tc.path, rr.Code, rr.Body)
		}
	}
	if len(backend.reqs) != 0 {
		t.Errorf("%d refused requests reached phoebe-videos", len(backend.reqs))
	}
	off := newTestServer(t, nil)
	if rr := serveOrgHost(off, orgHostVideoRequest(http.MethodGet, "/v1/videos", ""), ""); rr.Code != 404 {
		t.Errorf("videos off: %d", rr.Code)
	}
}

// The org-host handoff, like the endpoint one, stops the caller's key and
// cookies at the interceptor and replaces the client's request id.
func TestOrgHostVideoHandoffDropsCredentialsAndReplacesRequestID(t *testing.T) {
	srv, backend := newVideoServer(t)
	req := orgHostVideoRequest(http.MethodGet, "/v1/videos/"+testVideoID, "")
	req.Header.Set("Authorization", "Bearer k")
	req.Header.Set("Cookie", "a=b")
	req.Header.Set(requestIDHeader, "client-chosen")
	rr := serveOrgHost(srv, req, "")
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
		req.Header.Set(identity.HeaderVideoRetrieval, "true")
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
	srv, _ := newVideoServer(t)
	warn := captureWarn(srv)
	path := "/v1/videos/" + testVideoID
	if rr := serveOrgHost(srv, orgHostVideoRequest(http.MethodPost, path, ""), ""); rr.Code != 404 {
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
