package videos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testOrg   = "org1"
	testJob   = "video_01JD5Z8Q7M4W3X2Y1V0T9S8R7P"
	testToken = "store-token"
)

// fakeAtlas answers the internal video store routes; fakeStore is the object
// store the URLs point at. Together they stand in for Atlas signing and the
// store checking, so these tests pin what phoebe sends and how it reads each
// refusal. Atlas's own refusals (wrong org, wrong video id, a key outside the
// prefix, a job not in progress) are tested in saturn
// tests/routes/test_tf_video_store_internal.py; here they arrive as statuses.
type fakeAtlas struct {
	mu        sync.Mutex
	calls     []map[string]any
	paths     []string
	status    int    // the answer to object-urls (default 200)
	message   string // its message when not 200
	expiresIn time.Duration
	headers   map[string]string
	method    string // the method to claim it signed (default: the one asked)
	store     *httptest.Server
}

func (a *fakeAtlas) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(atlasVideoStoreHeader) != testToken {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		a.mu.Lock()
		a.calls = append(a.calls, body)
		a.paths = append(a.paths, r.URL.Path)
		a.mu.Unlock()
		switch {
		case r.URL.Path == atlasVideoJobsPath:
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{}`)
		case strings.HasSuffix(r.URL.Path, "/close"):
			_, _ = io.WriteString(w, `{}`)
		case strings.HasSuffix(r.URL.Path, "/object-urls"):
			if a.status != 0 && a.status != http.StatusOK {
				w.WriteHeader(a.status)
				_ = json.NewEncoder(w).Encode(map[string]string{"message": a.message})
				return
			}
			method, _ := body["method"].(string)
			headers := a.headers
			if headers == nil && method == http.MethodPut {
				headers = map[string]string{"Content-Type": "video/mp4",
					"Content-Length": strconv.FormatFloat(body["content_length"].(float64), 'f', 0, 64)}
			}
			expires := a.expiresIn
			if expires == 0 {
				expires = 5 * time.Minute
			}
			signed := method
			if a.method != "" {
				signed = a.method
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"method": signed, "url": a.store.URL + "/bucket/" + body["key"].(string) + "?X-Amz-Signature=x",
				"expires_at": time.Now().Add(expires).UTC().Format(time.RFC3339Nano), "headers": headers,
			})
		default:
			t.Errorf("unexpected Atlas path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

type storeReq struct {
	method, path, ctype string
	length              int64
	body                []byte
}

type fakeStore struct {
	mu     sync.Mutex
	reqs   []storeReq
	status int // default 200
	reply  string
	objs   map[string][]byte
}

func (s *fakeStore) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reqs = append(s.reqs, storeReq{r.Method, r.URL.Path, r.Header.Get("Content-Type"), r.ContentLength, body})
	if s.status != 0 {
		w.WriteHeader(s.status)
		_, _ = io.WriteString(w, s.reply)
		return
	}
	switch r.Method {
	case http.MethodPut:
		s.objs[r.URL.Path] = body
	case http.MethodGet:
		d, ok := s.objs[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(d)))
		_, _ = w.Write(d)
	case http.MethodDelete:
		delete(s.objs, r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	}
}

func newTestBlob(t *testing.T) (*AtlasBlob, *fakeAtlas, *fakeStore) {
	t.Helper()
	st := &fakeStore{objs: map[string][]byte{}}
	storeSrv := httptest.NewServer(http.HandlerFunc(st.handler))
	t.Cleanup(storeSrv.Close)
	at := &fakeAtlas{store: storeSrv}
	atlasSrv := httptest.NewServer(at.handler(t))
	t.Cleanup(atlasSrv.Close)
	b, err := NewAtlasBlob(atlasSrv.URL, testToken, "tf-videos")
	if err != nil {
		t.Fatal(err)
	}
	return b, at, st
}

func videoKey() string { return Keys{Prefix: "tf-videos"}.Video(testOrg, testJob) }

func TestAtlasBlobPutSendsTheSignedRequestExactly(t *testing.T) {
	b, at, st := newTestBlob(t)
	data := []byte("an mp4 file")
	if err := b.Put(context.Background(), videoKey(), bytes.NewReader(data), int64(len(data)), "video/mp4"); err != nil {
		t.Fatal(err)
	}
	if len(at.calls) != 1 || at.paths[0] != atlasVideoJobsPath+"/"+testJob+"/object-urls" {
		t.Fatalf("Atlas calls %v %v", at.paths, at.calls)
	}
	want := map[string]any{"org_id": testOrg, "key": videoKey(), "method": "PUT", "content_length": float64(len(data))}
	for k, v := range want {
		if at.calls[0][k] != v {
			t.Fatalf("Atlas asked %v, want %v", at.calls[0], want)
		}
	}
	if len(st.reqs) != 1 {
		t.Fatalf("store requests %v", st.reqs)
	}
	got := st.reqs[0]
	if got.method != http.MethodPut || got.path != "/bucket/"+videoKey() || got.ctype != "video/mp4" ||
		got.length != int64(len(data)) || !bytes.Equal(got.body, data) {
		t.Fatalf("store got %+v", got)
	}
}

func TestAtlasBlobGetStreamsAndDeleteRemoves(t *testing.T) {
	b, _, st := newTestBlob(t)
	st.objs["/bucket/"+videoKey()] = []byte("video bytes")
	r, size, err := b.Get(context.Background(), videoKey())
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	r.Close()
	if string(got) != "video bytes" || size != 11 {
		t.Fatalf("got %q %d", got, size)
	}
	if err := b.Delete(context.Background(), videoKey()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Get(context.Background(), videoKey()); !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("get after delete: %v", err)
	}
	// Deleting what is already gone is not an error (the reaper retries).
	if err := b.Delete(context.Background(), videoKey()); err != nil {
		t.Fatal(err)
	}
}

// A key outside <prefix>/<org>/<video id>/<name> is refused before Atlas or
// the store is asked.
func TestAtlasBlobRefusesAKeyOutsideThePrefix(t *testing.T) {
	b, at, st := newTestBlob(t)
	for _, key := range []string{
		"tf-adapters/org1/adapter_model.safetensors",
		"tf-videos/org1/" + testJob,
		"tf-videos/org1/not-a-video-id/video.mp4",
		"tf-videos/org1/" + testJob + "/nested/video.mp4",
		"tf-videos//" + testJob + "/video.mp4",
		"tf-videos-x/org1/" + testJob + "/video.mp4",
	} {
		if err := b.Put(context.Background(), key, strings.NewReader("x"), 1, "video/mp4"); err == nil {
			t.Errorf("put %q accepted", key)
		}
		if _, _, err := b.Get(context.Background(), key); err == nil {
			t.Errorf("get %q accepted", key)
		}
		if err := b.Delete(context.Background(), key); err == nil {
			t.Errorf("delete %q accepted", key)
		}
	}
	if len(at.calls) != 0 || len(st.reqs) != 0 {
		t.Fatalf("Atlas %v, store %v", at.calls, st.reqs)
	}
}

// An expired URL is never sent: one that has expired by the time it arrives
// is dropped here, and the store's own refusal of an expired URL (SeaweedFS:
// 403 AccessDenied "Request has expired") is an error, never a success.
func TestAtlasBlobExpiredURL(t *testing.T) {
	b, at, st := newTestBlob(t)
	at.expiresIn = -time.Second
	err := b.Put(context.Background(), videoKey(), strings.NewReader("x"), 1, "video/mp4")
	if !errors.Is(err, errURLExpired) || len(st.reqs) != 0 {
		t.Fatalf("expired URL: err %v, store %v", err, st.reqs)
	}
	at.expiresIn = 0
	st.status = http.StatusForbidden
	st.reply = `<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>Request has expired</Message></Error>`
	err = b.Put(context.Background(), videoKey(), strings.NewReader("x"), 1, "video/mp4")
	if err == nil || !strings.Contains(err.Error(), "AccessDenied: Request has expired") {
		t.Fatalf("store refusal: %v", err)
	}
	if _, _, err := b.Get(context.Background(), videoKey()); err == nil || errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("get with an expired URL: %v", err)
	}
	if err := b.Delete(context.Background(), videoKey()); err == nil {
		t.Fatal("delete with an expired URL succeeded")
	}
}

// Atlas's refusals arrive as statuses: a wrong org or video id (404), a key
// outside the prefix (403) and a job that is not in progress (409) fail the
// upload; a 409 on a read (the files can no longer exist) reads as gone.
func TestAtlasBlobAtlasRefusals(t *testing.T) {
	for _, tc := range []struct {
		status int
		reason string
	}{
		{http.StatusNotFound, "wrong org or video id"},
		{http.StatusForbidden, "key outside the prefix"},
		{http.StatusConflict, "job not in progress"},
	} {
		b, at, st := newTestBlob(t)
		at.status, at.message = tc.status, tc.reason
		err := b.Put(context.Background(), videoKey(), strings.NewReader("x"), 1, "video/mp4")
		var ae *atlasError
		if !errors.As(err, &ae) || ae.status != tc.status || len(st.reqs) != 0 {
			t.Errorf("%s: put err %v, store %v", tc.reason, err, st.reqs)
		}
		_, _, err = b.Get(context.Background(), videoKey())
		if tc.status == http.StatusConflict {
			if !errors.Is(err, ErrBlobNotFound) {
				t.Errorf("%s: get err %v", tc.reason, err)
			}
		} else if err == nil || errors.Is(err, ErrBlobNotFound) {
			t.Errorf("%s: get err %v", tc.reason, err)
		}
	}
}

// phoebe sends only what Atlas signed: a URL for another method, another
// length or another content type is refused before the store sees it.
func TestAtlasBlobRefusesASigningMismatch(t *testing.T) {
	for _, tc := range []struct {
		headers map[string]string
		method  string
		reason  string
	}{
		{map[string]string{"Content-Type": "video/mp4", "Content-Length": "2"}, "", "length"},
		{map[string]string{"Content-Type": "image/png", "Content-Length": "1"}, "", "content type"},
		{nil, http.MethodGet, "method"},
	} {
		b, at, st := newTestBlob(t)
		at.headers, at.method = tc.headers, tc.method
		if err := b.Put(context.Background(), videoKey(), strings.NewReader("x"), 1, "video/mp4"); err == nil {
			t.Errorf("%s mismatch accepted", tc.reason)
		}
		if len(st.reqs) != 0 {
			t.Errorf("%s mismatch reached the store", tc.reason)
		}
	}
}

func TestAtlasBlobRegisterAndClose(t *testing.T) {
	b, at, _ := newTestBlob(t)
	if err := b.Register(context.Background(), testOrg, testJob, "dep1", "png"); err != nil {
		t.Fatal(err)
	}
	if err := b.Register(context.Background(), testOrg, testJob, "dep1", ""); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(context.Background(), testOrg, testJob); err != nil {
		t.Fatal(err)
	}
	if at.calls[0]["input_ext"] != "png" || at.calls[1]["input_ext"] != nil ||
		at.calls[0]["resource_id"] != "dep1" || at.calls[0]["video_id"] != testJob {
		t.Fatalf("register bodies %v", at.calls)
	}
	if at.paths[2] != atlasVideoJobsPath+"/"+testJob+"/close" || at.calls[2]["org_id"] != testOrg {
		t.Fatalf("close %v %v", at.paths, at.calls)
	}
}

func TestAtlasBlobWrongTokenIsRefused(t *testing.T) {
	b, _, _ := newTestBlob(t)
	b.token = "wrong"
	var ae *atlasError
	if err := b.Register(context.Background(), testOrg, testJob, "dep1", ""); !errors.As(err, &ae) ||
		ae.status != http.StatusForbidden {
		t.Fatalf("got %v", err)
	}
}

func TestNewAtlasBlobRefusesBadConfig(t *testing.T) {
	for _, tc := range []struct{ url, token, prefix string }{
		{"http://atlas:8888", "", "tf-videos"},
		{"atlas:8888", "t", "tf-videos"},
		{"http://user:pw@atlas:8888", "t", "tf-videos"},
		{"http://atlas:8888", "t", "/"},
	} {
		if _, err := NewAtlasBlob(tc.url, tc.token, tc.prefix); err == nil {
			t.Errorf("accepted %+v", tc)
		}
	}
}
