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
	tokens    []string // the internal token header of every call
	status    int      // the answer to every call when set and not 200
	message   string   // its message
	expiresIn time.Duration
	headers   map[string]string
	method    string // the method to claim it signed (default: the one asked)
	store     *httptest.Server
	// signedURL, when set, is the URL signed for a key (default: the store's
	// URL for the key, with a placeholder signature).
	signedURL func(key string) string
}

func (a *fakeAtlas) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		a.mu.Lock()
		a.calls = append(a.calls, body)
		a.paths = append(a.paths, r.URL.Path)
		a.tokens = append(a.tokens, r.Header.Get(atlasVideoStoreHeader))
		a.mu.Unlock()
		if a.status != 0 && a.status != http.StatusOK {
			w.WriteHeader(a.status)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": a.message})
			return
		}
		switch {
		case r.URL.Path == atlasVideoJobsPath:
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{}`)
		case strings.HasSuffix(r.URL.Path, "/close"):
			_, _ = io.WriteString(w, `{}`)
		case strings.HasSuffix(r.URL.Path, "/object-urls"):
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
			signedURL := a.store.URL + "/bucket/" + body["key"].(string) + "?X-Amz-Signature=x"
			if a.signedURL != nil {
				signedURL = a.signedURL(body["key"].(string))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"method": signed, "url": signedURL,
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
	// token is whether the request carried the Atlas internal token header.
	token bool
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
	s.reqs = append(s.reqs, storeReq{r.Method, r.URL.Path, r.Header.Get("Content-Type"), r.ContentLength, body,
		r.Header.Get(atlasVideoStoreHeader) != ""})
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

func videoKey() string { return Keys{Prefix: "tf-videos"}.Video(testOrg, testJob, 1) }

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
		"tf-videos/org1/" + testJob + "/attempt-0/video.mp4",
		"tf-videos/org1/" + testJob + "/attempt-01/video.mp4",
		"tf-videos/org1/" + testJob + "/attempt-1000/video.mp4",
		"tf-videos/org1/" + testJob + "/attempt-x/video.mp4",
		"tf-videos/org1/" + testJob + "/attempt-1/",
		"tf-videos/org1/" + testJob + "/attempt-1/nested/video.mp4",
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

// TestAtlasBlobMapsAtlasRefusalStatuses pins how each Atlas refusal status
// reads here, not the conditions behind it: those are proved in saturn
// tests/routes/test_tf_video_store_internal.py (test_url_for_a_wrong_org_is_404,
// test_url_for_a_wrong_video_id_is_404,
// test_url_for_a_key_outside_the_jobs_prefix_is_403,
// test_video_store_refuses_a_missing_or_wrong_token,
// test_put_for_a_job_that_is_not_in_progress_is_409,
// test_get_after_the_files_can_no_longer_exist_is_409).
//
// Every refusal is an *atlasError carrying the status, and no request reaches
// the store. On a read, 409 (the files can no longer exist) and 404 (Atlas no
// longer has the job) read as ErrBlobNotFound; 403, which is also Atlas's
// answer to a wrong internal token, stays an error.
func TestAtlasBlobMapsAtlasRefusalStatuses(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		getMissing bool // Get reads as ErrBlobNotFound
	}{
		{"atlas_404_on_get_reads_as_not_found", http.StatusNotFound, true},
		{"atlas_403_is_an_error_not_not_found", http.StatusForbidden, false},
		{"atlas_409_on_get_reads_as_not_found", http.StatusConflict, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, at, st := newTestBlob(t)
			at.status, at.message = tc.status, "refused"
			isRefusal := func(op string, err error) {
				t.Helper()
				var ae *atlasError
				if !errors.As(err, &ae) || ae.status != tc.status {
					t.Errorf("%s: err %v, want an Atlas %d", op, err, tc.status)
				}
			}
			isRefusal("register", b.Register(context.Background(), testOrg, testJob, "dep1", ""))
			isRefusal("close", b.Close(context.Background(), testOrg, testJob, 0))
			isRefusal("discard", b.Discard(context.Background(), videoKey()))
			isRefusal("put", b.Put(context.Background(), videoKey(), strings.NewReader("x"), 1, "video/mp4"))
			isRefusal("delete", b.Delete(context.Background(), videoKey()))
			_, _, err := b.Get(context.Background(), videoKey())
			if tc.getMissing {
				if !errors.Is(err, ErrBlobNotFound) {
					t.Errorf("get: err %v, want ErrBlobNotFound", err)
				}
			} else {
				if errors.Is(err, ErrBlobNotFound) {
					t.Errorf("get: Atlas %d read as not found", tc.status)
				}
				isRefusal("get", err)
			}
			if len(st.reqs) != 0 {
				t.Errorf("the store got %v", st.reqs)
			}
		})
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

// Each attempt's output keys are its own; the input image belongs to no
// attempt. Parse reads all of them back to the job.
func TestKeysScopeOutputsToTheAttempt(t *testing.T) {
	k := Keys{Prefix: "/tf-videos/"}
	for _, tc := range []struct{ got, want string }{
		{k.Video(testOrg, testJob, 1), "tf-videos/org1/" + testJob + "/attempt-1/video.mp4"},
		{k.Thumbnail(testOrg, testJob, 2), "tf-videos/org1/" + testJob + "/attempt-2/thumbnail.webp"},
		{k.Input(testOrg, testJob, "png"), "tf-videos/org1/" + testJob + "/input.png"},
	} {
		if tc.got != tc.want {
			t.Errorf("key %q, want %q", tc.got, tc.want)
		}
		org, job, err := k.Parse(tc.got)
		if err != nil || org != testOrg || job != testJob {
			t.Errorf("parse %q: %s %s %v", tc.got, org, job, err)
		}
	}
	if k.Video(testOrg, testJob, 1) == k.Video(testOrg, testJob, 2) {
		t.Fatal("two attempts share a video key")
	}
}

// Discard is a DELETE that tells Atlas it removes a superseded attempt's own
// file (Atlas refuses it for the recorded output); Delete never says so.
func TestAtlasBlobDiscardMarksTheDelete(t *testing.T) {
	b, at, st := newTestBlob(t)
	ctx := context.Background()
	if err := b.Delete(ctx, videoKey()); err != nil {
		t.Fatal(err)
	}
	if err := b.Discard(ctx, videoKey()); err != nil {
		t.Fatal(err)
	}
	if _, marked := at.calls[0]["discard"]; marked || at.calls[0]["method"] != "DELETE" {
		t.Fatalf("delete body %v", at.calls[0])
	}
	if at.calls[1]["discard"] != true || at.calls[1]["method"] != "DELETE" {
		t.Fatalf("discard body %v", at.calls[1])
	}
	if len(st.reqs) != 2 || st.reqs[1].method != http.MethodDelete {
		t.Fatalf("store %v", st.reqs)
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
	if err := b.Close(context.Background(), testOrg, testJob, 0); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(context.Background(), testOrg, testJob, 2); err != nil {
		t.Fatal(err)
	}
	if _, named := at.calls[2]["output_attempt"]; named {
		t.Fatalf("a close without an output named one: %v", at.calls[2])
	}
	if at.calls[3]["output_attempt"] != float64(2) {
		t.Fatalf("close of a completed job %v", at.calls[3])
	}
	if at.calls[0]["input_ext"] != "png" || at.calls[1]["input_ext"] != nil ||
		at.calls[0]["resource_id"] != "dep1" || at.calls[0]["video_id"] != testJob {
		t.Fatalf("register bodies %v", at.calls)
	}
	if at.paths[2] != atlasVideoJobsPath+"/"+testJob+"/close" || at.calls[2]["org_id"] != testOrg {
		t.Fatalf("close %v %v", at.paths, at.calls)
	}
}

// Every call to Atlas carries the internal token, and no request to the store
// does (the store is another host; the token is a credential).
func TestAtlasBlobSendsItsTokenOnEveryAtlasCall(t *testing.T) {
	b, at, st := newTestBlob(t)
	ctx := context.Background()
	if err := b.Register(ctx, testOrg, testJob, "dep1", ""); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, videoKey(), strings.NewReader("x"), 1, "video/mp4"); err != nil {
		t.Fatal(err)
	}
	r, _, err := b.Get(ctx, videoKey())
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if err := b.Delete(ctx, videoKey()); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(ctx, testOrg, testJob, 1); err != nil {
		t.Fatal(err)
	}
	if len(at.tokens) != 5 {
		t.Fatalf("Atlas calls %v", at.paths)
	}
	for i, tok := range at.tokens {
		if tok != testToken {
			t.Errorf("Atlas call %s carried token %q", at.paths[i], tok)
		}
	}
	if len(st.reqs) != 3 {
		t.Fatalf("store requests %v", st.reqs)
	}
	for _, r := range st.reqs {
		if r.token {
			t.Errorf("store request %s %s carried the Atlas token", r.method, r.path)
		}
	}
}

// A store that answers with a redirect is not followed: the signed request
// was signed for one URL, and each operation fails instead.
func TestAtlasBlobNeverFollowsAStoreRedirect(t *testing.T) {
	var followed int
	var mu sync.Mutex
	elsewhere := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		followed++
		mu.Unlock()
	}))
	t.Cleanup(elsewhere.Close)
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirecting.Close)
	b, at, _ := newTestBlob(t)
	at.store = redirecting
	ctx := context.Background()
	if err := b.Put(ctx, videoKey(), strings.NewReader("x"), 1, "video/mp4"); err == nil {
		t.Error("put through a redirect succeeded")
	}
	if _, _, err := b.Get(ctx, videoKey()); err == nil || errors.Is(err, ErrBlobNotFound) {
		t.Errorf("get through a redirect: %v", err)
	}
	if err := b.Delete(ctx, videoKey()); err == nil {
		t.Error("delete through a redirect succeeded")
	}
	mu.Lock()
	defer mu.Unlock()
	if followed != 0 {
		t.Fatalf("the redirect target got %d request(s)", followed)
	}
}

// Atlas answering with a redirect is not followed either, so the internal
// token never reaches the host the redirect names.
func TestAtlasBlobNeverSendsTheTokenAcrossARedirect(t *testing.T) {
	var reached []string
	var mu sync.Mutex
	elsewhere := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reached = append(reached, r.Header.Get(atlasVideoStoreHeader))
		mu.Unlock()
	}))
	t.Cleanup(elsewhere.Close)
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(atlas.Close)
	b, err := NewAtlasBlob(atlas.URL, testToken, "tf-videos")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	isRedirect := func(op string, err error) {
		t.Helper()
		var ae *atlasError
		if !errors.As(err, &ae) || ae.status != http.StatusTemporaryRedirect {
			t.Errorf("%s: err %v, want the 307 as an Atlas refusal", op, err)
		}
	}
	isRedirect("register", b.Register(ctx, testOrg, testJob, "dep1", ""))
	isRedirect("close", b.Close(ctx, testOrg, testJob, 0))
	isRedirect("put", b.Put(ctx, videoKey(), strings.NewReader("x"), 1, "video/mp4"))
	_, _, err = b.Get(ctx, videoKey())
	isRedirect("get", err)
	isRedirect("delete", b.Delete(ctx, videoKey()))
	mu.Lock()
	defer mu.Unlock()
	if len(reached) != 0 {
		t.Fatalf("the redirect target got %d request(s), tokens %q", len(reached), reached)
	}
}

// An HTTP proxy in the pod environment is never used, by either client.
// net/http reads the proxy variables once per process and never proxies a
// loopback address, so the counting proxy alone cannot prove this; the
// transports' Proxy being nil is what does.
func TestAtlasBlobIgnoresAmbientProxy(t *testing.T) {
	var proxied int
	var mu sync.Mutex
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		proxied++
		mu.Unlock()
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(proxy.Close)
	for _, k := range []string{"HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"} {
		t.Setenv(k, proxy.URL)
	}
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	b, _, _ := newTestBlob(t)
	for name, c := range map[string]*http.Client{"atlas": b.atlas, "store": b.store} {
		tr, ok := c.Transport.(*http.Transport)
		if !ok || tr.Proxy != nil {
			t.Errorf("the %s client's transport uses a proxy function", name)
		}
	}
	data := []byte("x")
	if err := b.Put(context.Background(), videoKey(), bytes.NewReader(data), 1, "video/mp4"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if proxied != 0 {
		t.Fatalf("the proxy got %d request(s)", proxied)
	}
}

// A store that takes a request and never answers is cut off (the store
// client has no whole-request timeout, so the response header timeout is what
// stops it), and the error says so instead of hanging.
func TestAtlasBlobStoreThatNeverAnswersTimesOut(t *testing.T) {
	release := make(chan struct{})
	hung := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-release
	}))
	t.Cleanup(hung.Close)
	t.Cleanup(func() { close(release) })
	b, at, _ := newTestBlob(t)
	at.store = hung
	tr := b.store.Transport.(*http.Transport)
	if tr.ResponseHeaderTimeout != storeResponseHeaderTimeout || !tr.DisableCompression {
		t.Fatalf("store transport: header timeout %s, compression disabled %v",
			tr.ResponseHeaderTimeout, tr.DisableCompression)
	}
	tr.ResponseHeaderTimeout = 100 * time.Millisecond
	done := make(chan error, 1)
	go func() { done <- b.Put(context.Background(), videoKey(), strings.NewReader("x"), 1, "video/mp4") }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "timeout awaiting response headers") {
			t.Fatalf("put to a store that never answers: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("put to a store that never answers did not time out")
	}
}

// A transport error on a signed request never carries the URL's query, which
// holds the signature; it still names the key.
func TestAtlasBlobStoreTransportErrorNeverCarriesTheSignature(t *testing.T) {
	ln := httptest.NewServer(http.NotFoundHandler())
	closedURL := ln.URL
	ln.Close() // nothing listens there any more
	b, at, _ := newTestBlob(t)
	at.signedURL = func(key string) string {
		return closedURL + "/bucket/" + key + "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=SECRETSIG"
	}
	ctx := context.Background()
	_, _, getErr := b.Get(ctx, videoKey())
	for op, err := range map[string]error{
		"put":    b.Put(ctx, videoKey(), strings.NewReader("x"), 1, "video/mp4"),
		"get":    getErr,
		"delete": b.Delete(ctx, videoKey()),
	} {
		if err == nil {
			t.Errorf("%s to a closed listener succeeded", op)
			continue
		}
		msg := err.Error()
		if strings.Contains(msg, "SECRETSIG") || strings.Contains(msg, "X-Amz-") {
			t.Errorf("%s error carries the signature: %s", op, msg)
		}
		if !strings.Contains(msg, videoKey()) {
			t.Errorf("%s error does not name the key: %s", op, msg)
		}
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
