package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

// idleTailBackend streams one SSE chunk, flushes, then stays silent for idle
// before ending the response: an upstream that pauses longer than the stall
// timeout after its last chunk.
func idleTailBackend(t *testing.T, idle time.Duration) *url.URL {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n")
		w.(http.Flusher).Flush()
		time.Sleep(idle)
	}))
	t.Cleanup(backend.Close)
	up, _ := url.Parse(backend.URL)
	return up
}

// B2: the stall guard's per-write deadline must not break the writes net/http
// makes after the handler returns (the chunked terminator and final flush),
// even when the deadline armed by the last guarded write has already expired;
// and it must not leak into the next request on a keep-alive connection.
// Without the handler's deferred re-arm, the terminator write fails with
// i/o timeout and the client sees a truncated chunked body (unexpected EOF).
func TestStallGuardDeadlineDoesNotAffectPostHandlerWritesOrNextRequest(t *testing.T) {
	const stallTimeout = 100 * time.Millisecond
	up := idleTailBackend(t, 250*time.Millisecond) // idles past the deadline
	proxySrv := liveProxy(t, New(&config.Settings{Protection: config.ProtectionSettings{StreamStallTimeout: stallTimeout}},
		logging.New(logging.ERROR), &recordingEmitter{}))

	conn, err := net.Dial("tcp", proxySrv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)

	for i, resource := range []string{"tail-1", "tail-2"} {
		if i > 0 {
			// The keep-alive gap is longer than the stall timeout, so a write
			// deadline left over from the first request would already have
			// passed when the second request's response is written.
			time.Sleep(2 * stallTimeout)
		}
		if _, err := io.WriteString(conn, rawStreamingRequest(up, resource)); err != nil {
			t.Fatalf("request %d: write: %v", i+1, err)
		}
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("request %d: read response: %v", i+1, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status=%d, want 200", i+1, resp.StatusCode)
		}
		// A client that reads slowly: start reading the body only once the
		// per-write deadline from the streamed chunk has expired.
		time.Sleep(stallTimeout + 50*time.Millisecond)
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("request %d: body was truncated (%v); got %q", i+1, err, body)
		}
		if !strings.Contains(string(body), `"content":"hi"`) {
			t.Fatalf("request %d: body=%q, want the streamed chunk", i+1, body)
		}
		if resp.Close {
			t.Fatalf("request %d: connection was not kept alive", i+1)
		}
	}
}

// B1: with the stall guard in front of the handler, a chunked body over the
// request limit still answers 413 AND closes the connection. MaxBytesReader
// can mark the connection close-after-reply only when given the connection's
// own ResponseWriter (net/http's unexported requestTooLarge hook), so the
// handler passes it the raw writer, not the guard.
func TestOversizeRequestBodyReturns413WithStallGuardEnabled(t *testing.T) {
	const limit = 256
	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("an oversized body reached the upstream")
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	proxySrv := liveProxy(t, New(&config.Settings{Protection: config.ProtectionSettings{
		MaxRequestBodyBytes: limit, StreamStallTimeout: time.Second,
	}}, logging.New(logging.ERROR), &recordingEmitter{}))

	conn, err := net.Dial("tcp", proxySrv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	body := bodyOfSize(limit + 1)
	raw := "POST /v1/chat/completions HTTP/1.1\r\nHost: phoebe\r\n" +
		identity.HeaderUpstream + ": " + up.Host + "\r\n" +
		identity.HeaderServingMode + ": dedicated\r\n" +
		identity.HeaderAuthID + ": auth-1\r\n" +
		identity.HeaderResourceID + ": dedicated-1\r\n" +
		identity.HeaderServedModel + ": m\r\n" +
		"Content-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n" +
		fmt.Sprintf("%x", len(body)) + "\r\n" + body + "\r\n0\r\n\r\n"
	if _, err := io.WriteString(conn, raw); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d, want 413", resp.StatusCode)
	}
	// ReadResponse consumes the "Connection: close" header into resp.Close.
	if !resp.Close {
		t.Fatal("413 for an oversize body did not carry Connection: close")
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if n, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("server kept the connection open after an oversize body (read %v, %v)", n, err)
	}
}

// The stall guard keeps http.ResponseController working for code behind it
// (ReverseProxy's per-chunk flush, deadline control): Flush and
// SetWriteDeadline reach the real connection through the guard and Unwrap.
func TestStallGuardKeepsResponseControllerFlushAndDeadlineControl(t *testing.T) {
	results := make(chan [2]error, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		g := newStallGuardWriter(w, time.Second, func(error) {})
		rc := http.NewResponseController(g)
		_, _ = io.WriteString(g, "x")
		results <- [2]error{rc.Flush(), rc.SetWriteDeadline(time.Now().Add(time.Second))}
	}))
	defer srv.Close()
	getReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	got := <-results
	if got[0] != nil || got[1] != nil {
		t.Fatalf("flush=%v setWriteDeadline=%v through the guard, want both supported", got[0], got[1])
	}
	if g := newStallGuardWriter(httptest.NewRecorder(), time.Second, func(error) {}); g.Unwrap() == nil {
		t.Fatal("Unwrap must return the wrapped ResponseWriter")
	}
}
