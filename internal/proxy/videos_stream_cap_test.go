package proxy

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/logging"
)

// slowVideosBackend stands in for phoebe-videos: it waits before the
// headers, then sends the body in chunks with a pause between each, so the
// whole response takes several times the test's maxStreamDuration while no
// single write ever waits long.
func slowVideosBackend(t *testing.T, chunks int, pause time.Duration) (*httptest.Server, []byte) {
	t.Helper()
	chunk := bytes.Repeat([]byte("v"), 1024)
	want := bytes.Repeat(chunk, chunks)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(pause):
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "video/mp4")
		w.WriteHeader(http.StatusOK)
		for i := 0; i < chunks; i++ {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			select {
			case <-time.After(pause):
			case <-r.Context().Done():
				return
			}
		}
	}))
	t.Cleanup(backend.Close)
	return backend, want
}

func newCappedVideoServer(t *testing.T, p config.ProtectionSettings, backendURL string) *Server {
	t.Helper()
	srv, err := New(&config.Settings{Protection: p}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithVideos(backendURL, "shared-token")
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// X4 (battery on phoebe#148): a video content download is the one request
// exempt from protection.maxStreamDuration, on the endpoint's route and on
// the always-on retrieval route alike. The download here takes about six
// times the limit and arrives whole.
func TestVideoContentDownloadOutlivesMaxStreamDuration(t *testing.T) {
	const limit = 100 * time.Millisecond
	backend, want := slowVideosBackend(t, 5, limit)
	srv := newCappedVideoServer(t, config.ProtectionSettings{MaxStreamDuration: limit}, backend.URL)
	for name, req := range map[string]*http.Request{
		"endpoint route":  videoRequest(http.MethodGet, "/v1/videos/"+testVideoID+"/content"),
		"retrieval route": retrievalRequest(http.MethodGet, "/v1/videos/"+testVideoID+"/content"),
		"thumbnail":       retrievalRequest(http.MethodGet, "/v1/videos/"+testVideoID+"/content?variant=thumbnail"),
	} {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			rr := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("status %d, want 200", rr.Code)
			}
			if !bytes.Equal(rr.Body.Bytes(), want) {
				t.Fatalf("got %d bytes, want the whole %d", rr.Body.Len(), len(want))
			}
			if elapsed := time.Since(start); elapsed < 3*limit {
				t.Fatalf("download took %v; the test needs it to outlive the %v limit", elapsed, limit)
			}
		})
	}
}

// Every other video request keeps the bound: an upstream that answers
// nothing within maxStreamDuration is cut there for a job read, a delete, a
// create and a list, exactly as before (the video proxy answers that cut with
// its 502).
func TestVideoRequestsOtherThanContentKeepMaxStreamDuration(t *testing.T) {
	const limit = 100 * time.Millisecond
	backend, _ := slowVideosBackend(t, 1, 5*limit)
	srv := newCappedVideoServer(t, config.ProtectionSettings{MaxStreamDuration: limit}, backend.URL)
	for name, req := range map[string]*http.Request{
		"read on the endpoint route":    videoRequest(http.MethodGet, "/v1/videos/"+testVideoID),
		"delete on the endpoint route":  videoRequest(http.MethodDelete, "/v1/videos/"+testVideoID),
		"create":                        videoRequest(http.MethodPost, "/v1/videos"),
		"list":                          videoRequest(http.MethodGet, "/v1/videos"),
		"read on the retrieval route":   retrievalRequest(http.MethodGet, "/v1/videos/"+testVideoID),
		"delete on the retrieval route": retrievalRequest(http.MethodDelete, "/v1/videos/"+testVideoID),
	} {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			rr := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusBadGateway {
				t.Fatalf("status %d, want the cut's 502", rr.Code)
			}
			if elapsed := time.Since(start); elapsed > 3*limit {
				t.Fatalf("ended after %v, want within a small multiple of the %v limit", elapsed, limit)
			}
		})
	}
}

// The stall guard still ends a download whose client stops reading: the
// exemption removes only the whole-request bound. A live server is needed,
// because the guard works through connection write deadlines.
func TestVideoContentDownloadKeepsTheStallGuard(t *testing.T) {
	const stall = 200 * time.Millisecond
	// Large enough that the kernel's socket buffers cannot absorb it, so the
	// proxy's writes block once the client stops reading.
	const size = 64 << 20
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, io.LimitReader(zeroReader{}, size))
	}))
	t.Cleanup(backend.Close)
	srv := newCappedVideoServer(t, config.ProtectionSettings{
		MaxStreamDuration: time.Hour, StreamStallTimeout: stall,
	}, backend.URL)
	proxySrv := liveProxy(t, srv)

	req := retrievalRequest(http.MethodGet, proxySrv.URL+"/v1/videos/"+testVideoID+"/content")
	req.RequestURI = ""
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	// The client stops reading for well past the stall timeout...
	time.Sleep(10 * stall)
	// ...and the download has been cut short.
	n, err := io.Copy(io.Discard, resp.Body)
	if err == nil && n >= size {
		t.Fatalf("the whole %d-byte download arrived after the client stalled; the stall guard did not fire", size)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
