package proxy

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
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

// retrievalRequest is what the always-on retrieval route delivers (issue
// #13): Atlas's retrieval ForwardAuth stamped the mark, the video's org and
// endpoint, and the caller's key. No upstream, serving mode or served model.
func retrievalRequest(method, path string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set(identity.HeaderVideoRetrieval, "true")
	req.Header.Set(identity.HeaderOrgID, "org-1")
	req.Header.Set(identity.HeaderResourceID, "res-1")
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	return req
}

func TestVideoRetrievalRouteHandsAnOrgAndEndpointOnlyIdentity(t *testing.T) {
	srv, backend := newVideoServer(t)
	req := retrievalRequest(http.MethodGet, "/v1/videos/"+testVideoID+"/content")
	req.Header.Set(videos.HeaderHandoff, "forged")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || len(backend.reqs) != 1 {
		t.Fatalf("status %d, backend calls %d", rr.Code, len(backend.reqs))
	}
	got := backend.reqs[0]
	if got.Header.Get(videos.HeaderToken) != "shared-token" {
		t.Error("the shared token was not set")
	}
	raw, err := base64.RawURLEncoding.DecodeString(got.Header.Get(videos.HeaderHandoff))
	if err != nil {
		t.Fatal(err)
	}
	var h videos.Handoff
	if err := json.Unmarshal(raw, &h); err != nil {
		t.Fatal(err)
	}
	want := videos.Handoff{Retrieval: true, OrgID: "org-1", AuthID: "auth-1", ResourceID: "res-1"}
	if h != want {
		t.Fatalf("handoff %+v, want %+v", h, want)
	}
	for name := range got.Header {
		if strings.HasPrefix(name, "X-Saturn-") {
			t.Errorf("%s reached phoebe-videos", name)
		}
	}
}

func TestVideoRetrievalRouteServesOnlyReadDownloadAndDelete(t *testing.T) {
	srv, backend := newVideoServer(t)
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/v1/videos/" + testVideoID, 200},
		{http.MethodDelete, "/v1/videos/" + testVideoID, 200},
		{http.MethodGet, "/v1/videos/" + testVideoID + "/content", 200},
		{http.MethodGet, "/v1/videos/" + testVideoID + "/content?variant=thumbnail", 200},
		{http.MethodPost, "/v1/videos", 404},
		{http.MethodGet, "/v1/videos", 404},
		{http.MethodPost, "/v1/chat/completions", 404},
		{http.MethodGet, "/v1/models", 404},
		{http.MethodGet, "/v1/videos/not-a-video-id", 404},
		{http.MethodDelete, "/v1/videos/" + testVideoID + "/content", 404},
		{http.MethodGet, "/v1/videos/" + testVideoID + "/", 404},
		{http.MethodPost, "/v1/videos/" + testVideoID + "/remix", 404},
	} {
		before := len(backend.reqs)
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, retrievalRequest(tc.method, tc.path))
		if rr.Code != tc.want {
			t.Errorf("%s %s: %d, want %d", tc.method, tc.path, rr.Code, tc.want)
		}
		if tc.want == 404 && len(backend.reqs) != before {
			t.Errorf("%s %s reached phoebe-videos", tc.method, tc.path)
		}
	}
}

func TestVideoRetrievalRouteFailsClosed(t *testing.T) {
	srv, backend := newVideoServer(t)
	path := "/v1/videos/" + testVideoID
	for _, tc := range []struct {
		reason string
		mut    func(*http.Request)
	}{
		{"no org", func(r *http.Request) { r.Header.Del(identity.HeaderOrgID) }},
		{"no endpoint", func(r *http.Request) { r.Header.Del(identity.HeaderResourceID) }},
		{"no key", func(r *http.Request) { r.Header.Del(identity.HeaderAuthID) }},
		{"with the gateway mark", func(r *http.Request) { r.Header.Set(identity.HeaderGateway, "true") }},
		{"with endpoint limits", func(r *http.Request) { r.Header.Set(identity.HeaderVideoLimits, testVideoLimits) }},
	} {
		req := retrievalRequest(http.MethodGet, path)
		tc.mut(req)
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, req)
		if rr.Code != 404 {
			t.Errorf("%s: %d, want 404", tc.reason, rr.Code)
		}
	}
	// Only the exact value "true" marks a retrieval request; anything else is
	// an ordinary header-routed request, refused for its missing upstream.
	req := retrievalRequest(http.MethodGet, path)
	req.Header.Set(identity.HeaderVideoRetrieval, "yes")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code == http.StatusOK {
		t.Errorf("mark %q was accepted", "yes")
	}
	if len(backend.reqs) != 0 {
		t.Errorf("%d refused requests reached phoebe-videos", len(backend.reqs))
	}
	// Video generation off on this phoebe: 404.
	off := newTestServer(t, nil)
	rr = httptest.NewRecorder()
	off.Handler().ServeHTTP(rr, retrievalRequest(http.MethodGet, path))
	if rr.Code != 404 {
		t.Errorf("videos off: %d", rr.Code)
	}
}

// With the mark left out of PHOEBE_TRUSTED_HEADERS (an old chart), a client
// copy is never read: the request is not a retrieval request.
func TestVideoRetrievalMarkIsReadOnlyWhenTrusted(t *testing.T) {
	// Reload after t.Setenv restores the variable (cleanups run last in,
	// first out), as TestSaturnHeadersStrippedWithConfiguredTrustedSet does.
	t.Cleanup(func() { identity.LoadTrustedHeaders(logging.New(logging.ERROR)) })
	t.Setenv(identity.TrustedHeadersEnv, "X-Saturn-Gateway,X-Saturn-Org-Id,X-Saturn-Serving-Mode")
	identity.LoadTrustedHeaders(logging.New(logging.ERROR))
	srv, backend := newVideoServer(t)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, retrievalRequest(http.MethodGet, "/v1/videos/"+testVideoID))
	if rr.Code == http.StatusOK || len(backend.reqs) != 0 {
		t.Fatalf("an untrusted mark reached phoebe-videos: %d", rr.Code)
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
func TestVideoRetrievalRefusalLogsThePath(t *testing.T) {
	srv, backend := newVideoServer(t)
	warn := captureWarn(srv)
	path := "/v1/videos/" + testVideoID
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, retrievalRequest(http.MethodPost, path))
	if rr.Code != 404 || len(backend.reqs) != 0 {
		t.Fatalf("status %d, backend calls %d", rr.Code, len(backend.reqs))
	}
	if got := warn.String(); !strings.Contains(got, fmt.Sprintf("path=%q", path)) {
		t.Fatalf("refusal log %q does not name the path %q", got, path)
	}
	// The endpoint route's refusal names it too.
	warn = captureWarn(srv)
	rr = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, videoRequest(http.MethodPost, "/v1/videos/sync"))
	if got := warn.String(); rr.Code != 404 || !strings.Contains(got, `path="/v1/videos/sync"`) {
		t.Fatalf("status %d, refusal log %q does not name the path", rr.Code, got)
	}
}
