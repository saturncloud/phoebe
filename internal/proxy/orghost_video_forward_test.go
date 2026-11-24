package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/saturncloud/phoebe/internal/gateway"
)

// localVideoStore stands in for this cluster's phoebe-videos: it holds the
// ids in have and answers its own 404 for anything else.
type localVideoStore struct {
	mu   sync.Mutex
	have map[string]bool
	hits int
}

const localVideo404 = `{"error":{"message":"video not found","type":"invalid_request_error","code":"not_found","param":null}}`

func (l *localVideoStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.mu.Lock()
	l.hits++
	l.mu.Unlock()
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/videos/"), "/content")
	w.Header().Set("Content-Type", "application/json")
	if !l.have[id] {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(localVideo404))
		return
	}
	_, _ = w.Write([]byte(`{"object":"video","id":"` + id + `","where":"local"}`))
}

// remoteCluster stands in for another cluster's org host.
type remoteCluster struct {
	name    string
	have    map[string]bool
	mu      sync.Mutex
	reqs    []*http.Request
	content string
}

func (c *remoteCluster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	c.reqs = append(c.reqs, r.Clone(r.Context()))
	c.mu.Unlock()
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/videos/"), "/content")
	if !c.have[id] {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"message":"remote 404","code":"not_found"}}`))
		return
	}
	if r.Method == http.MethodDelete {
		_, _ = w.Write([]byte(`{"id":"` + id + `","deleted":true}`))
		return
	}
	if strings.HasSuffix(r.URL.Path, "/content") {
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = w.Write([]byte(c.content))
		return
	}
	_, _ = w.Write([]byte(`{"object":"video","id":"` + id + `","where":"` + c.name + `"}`))
}

func (c *remoteCluster) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.reqs)
}

// videoForwardSetup: local store, two other clusters each serving one of
// org-1's video endpoints, one local video endpoint and one of org-2.
func videoForwardSetup(t *testing.T, local *localVideoStore, a, b *remoteCluster) *Server {
	t.Helper()
	ts := httptest.NewServer(local)
	t.Cleanup(ts.Close)
	srvA := httptest.NewTLSServer(a)
	t.Cleanup(srvA.Close)
	srvB := httptest.NewTLSServer(b)
	t.Cleanup(srvB.Close)
	rowA := videoRow("wan-a", "res-a", "")
	rowA.ForwardURL, rowA.Cluster = srvA.URL, "spoke-a"
	rowB := videoRow("wan-b", "res-b", "")
	rowB.ForwardURL, rowB.Cluster = srvB.URL, "spoke-b"
	rowB2 := videoRow("wan-b2", "res-b2", "") // same cluster as wan-b: asked once
	rowB2.ForwardURL, rowB2.Cluster = srvB.URL, "spoke-b"
	chatA := dedicatedRow("org-1", "chat-a", "res-c", "")
	chatA.ForwardURL, chatA.Cluster = srvA.URL+"/chat", "spoke-a" // not a video endpoint: never asked
	localRow := videoRow("wan-local", "res-l", "pd-wan.ns.svc.cluster.local:8000")
	other := videoRow("their-wan", "res-9", "")
	other.OrgID, other.ForwardURL = "org-2", srvA.URL
	rows := []gateway.DedicatedEndpoint{rowA, rowB, rowB2, chatA, localRow, other}
	srv, err := newOrgHostTestServer(t, nil, &fakeTierResolver{dedicated: rows}, nil).WithVideos(ts.URL, "shared-token")
	if err != nil {
		t.Fatal(err)
	}
	// Both fake clusters use httptest's one test certificate.
	srv.orgHosts.forward.transport = srvA.Client().Transport
	return srv
}

func TestOrgHostVideoRetrievalForwardsToTheClusterThatHasIt(t *testing.T) {
	local := &localVideoStore{have: map[string]bool{"video_local": true}}
	a := &remoteCluster{name: "a", have: map[string]bool{}}
	b := &remoteCluster{name: "b", have: map[string]bool{"video_b": true}, content: "MP4BYTES"}
	srv := videoForwardSetup(t, local, a, b)

	rr := serveOrgHost(srv, orgHostVideoRequest(http.MethodGet, "/v1/videos/video_b", ""))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"where":"b"`) {
		t.Fatalf("retrieve: %d %s", rr.Code, rr.Body)
	}
	if a.count() > 1 || b.count() != 1 {
		t.Fatalf("each other cluster asked at most once: a=%d b=%d", a.count(), b.count())
	}
	got := b.reqs[0]
	if got.Header.Get("Authorization") != "Bearer sk-test" || got.Header.Get(orgHostForwardedHeader) == "" {
		t.Fatalf("forward must carry the key and the forwarded marker: %v", got.Header)
	}
	for name := range got.Header {
		if strings.HasPrefix(name, "X-Saturn-") || strings.HasPrefix(name, "X-Phoebe-") {
			t.Fatalf("forward carried %s", name)
		}
	}
	rr = serveOrgHost(srv, orgHostVideoRequest(http.MethodGet, "/v1/videos/video_b/content", ""))
	if rr.Code != http.StatusOK || rr.Body.String() != "MP4BYTES" || rr.Header().Get("Content-Type") != "video/mp4" {
		t.Fatalf("content: %d %q %v", rr.Code, rr.Body, rr.Header())
	}
	// A local video is served locally; no other cluster is asked.
	before := a.count() + b.count()
	rr = serveOrgHost(srv, orgHostVideoRequest(http.MethodGet, "/v1/videos/video_local", ""))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"where":"local"`) || a.count()+b.count() != before {
		t.Fatalf("local: %d %s", rr.Code, rr.Body)
	}
}

func TestOrgHostVideoDeleteIsForwarded(t *testing.T) {
	local := &localVideoStore{have: map[string]bool{}}
	a := &remoteCluster{name: "a", have: map[string]bool{"video_a": true}}
	b := &remoteCluster{name: "b", have: map[string]bool{}}
	srv := videoForwardSetup(t, local, a, b)
	rr := serveOrgHost(srv, orgHostVideoRequest(http.MethodDelete, "/v1/videos/video_a", ""))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"deleted":true`) {
		t.Fatalf("delete: %d %s", rr.Code, rr.Body)
	}
	if a.reqs[0].Method != http.MethodDelete {
		t.Fatalf("forwarded method %s", a.reqs[0].Method)
	}
}

func TestOrgHostVideoAllMissesGiveTheIdenticalLocal404(t *testing.T) {
	local := &localVideoStore{have: map[string]bool{}}
	a := &remoteCluster{name: "a", have: map[string]bool{}}
	b := &remoteCluster{name: "b", have: map[string]bool{}}
	srv := videoForwardSetup(t, local, a, b)
	rr := serveOrgHost(srv, orgHostVideoRequest(http.MethodGet, "/v1/videos/video_nowhere", ""))
	if rr.Code != http.StatusNotFound || rr.Body.String() != localVideo404 {
		t.Fatalf("all misses must give this cluster's own 404: %d %q", rr.Code, rr.Body)
	}
	if a.count() != 1 || b.count() != 1 {
		t.Fatalf("each distinct cluster asked once: a=%d b=%d", a.count(), b.count())
	}
	// Exactly the same bytes as a cluster with no other cluster to ask.
	solo := &localVideoStore{have: map[string]bool{}}
	ts := httptest.NewServer(solo)
	defer ts.Close()
	alone, err := newOrgHostTestServer(t, nil, &fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{videoRow("wan-local", "res-l", "pd:8000")}}, nil).WithVideos(ts.URL, "shared-token")
	if err != nil {
		t.Fatal(err)
	}
	rr2 := serveOrgHost(alone, orgHostVideoRequest(http.MethodGet, "/v1/videos/video_nowhere", ""))
	if rr2.Code != rr.Code || rr2.Body.String() != rr.Body.String() {
		t.Fatalf("bodies differ: %q vs %q", rr2.Body, rr.Body)
	}
}

func TestOrgHostVideoRetrievalIsNeverForwardedTwice(t *testing.T) {
	local := &localVideoStore{have: map[string]bool{}}
	a := &remoteCluster{name: "a", have: map[string]bool{"video_a": true}}
	b := &remoteCluster{name: "b", have: map[string]bool{}}
	srv := videoForwardSetup(t, local, a, b)
	req := orgHostVideoRequest(http.MethodGet, "/v1/videos/video_a", "")
	req.Header.Set(orgHostForwardedHeader, "1")
	rr := serveOrgHost(srv, req)
	if rr.Code != http.StatusNotFound || rr.Body.String() != localVideo404 {
		t.Fatalf("a forwarded miss is the local 404: %d %s", rr.Code, rr.Body)
	}
	if a.count()+b.count() != 0 {
		t.Fatalf("a forwarded request was forwarded again: a=%d b=%d", a.count(), b.count())
	}
}

func TestOrgHostVideoScopedKeyReachesOnlyItsEndpointsCluster(t *testing.T) {
	local := &localVideoStore{have: map[string]bool{}}
	a := &remoteCluster{name: "a", have: map[string]bool{"video_x": true}}
	b := &remoteCluster{name: "b", have: map[string]bool{"video_x": true}}
	srv := videoForwardSetup(t, local, a, b)
	rr := serveOrgHost(srv, scoped(orgHostVideoRequest(http.MethodGet, "/v1/videos/video_x", ""), "res-b"))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"where":"b"`) || a.count() != 0 {
		t.Fatalf("scoped to res-b: %d %s a=%d", rr.Code, rr.Body, a.count())
	}
	// A key scoped to a local endpoint asks no other cluster.
	rr = serveOrgHost(srv, scoped(orgHostVideoRequest(http.MethodGet, "/v1/videos/video_x", ""), "res-l"))
	if rr.Code != http.StatusNotFound || a.count() != 0 || b.count() != 1 {
		t.Fatalf("scoped to a local endpoint: %d a=%d b=%d", rr.Code, a.count(), b.count())
	}
}

func TestOrgHostVideoListIsNotForwarded(t *testing.T) {
	local := &localVideoStore{have: map[string]bool{}}
	a := &remoteCluster{name: "a", have: map[string]bool{}}
	b := &remoteCluster{name: "b", have: map[string]bool{}}
	srv := videoForwardSetup(t, local, a, b)
	_ = serveOrgHost(srv, orgHostVideoRequest(http.MethodGet, "/v1/videos", ""))
	if a.count()+b.count() != 0 {
		t.Fatalf("the list must stay this cluster's: a=%d b=%d", a.count(), b.count())
	}
}
