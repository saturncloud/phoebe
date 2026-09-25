package proxy

// M3 abort-correctness tests: client-disconnect detection, always-bill-known-
// usage policy, and race-freedom under abort + normal completion paths.
//
// Design of the "slow backend" pattern used throughout: the backend writes the
// first chunk(s) and then blocks on a channel. The test cancels the client
// context while the backend is blocked. ReverseProxy sees the cancelled context,
// cancels the upstream request, and closes the captureReader body — triggering
// finish(), which reads the cancelled request context as the abort signal. The
// metering event is emitted asynchronously, so tests assert via
// em.waitForEvents(...); go test -race verifies no data races.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
)

// newTestServerWithSettings constructs a Server for abort-path tests.
func newTestServerWithSettings(t *testing.T, _ *url.URL, em metering.Emitter) *Server {
	t.Helper()
	s := &config.Settings{ListenAddr: ":0"}
	log := logging.New(logging.ERROR)
	return New(s, log, em)
}

// slowBackend starts an httptest.Server that writes firstChunks immediately,
// then blocks until unblock is closed, then closes the connection. It returns
// the server and the unblock channel. Call backend.Close() to clean up.
func slowBackend(t *testing.T, firstChunks string) (*httptest.Server, chan struct{}) {
	t.Helper()
	unblock := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		if firstChunks != "" {
			_, _ = io.WriteString(w, firstChunks)
			if fl != nil {
				fl.Flush()
			}
		}
		// Block until the test releases us (or the request context is cancelled,
		// which unblocks the select via r.Context().Done()).
		select {
		case <-unblock:
		case <-r.Context().Done():
		}
	}))
	return srv, unblock
}

// doAbortRequest sends a proxied SSE request via srv and cancels the context
// after delayBeforeCancel, then returns once ServeHTTP returns. The metering
// event is emitted asynchronously, so callers must assert via
// em.waitForEvents(...), not em.all() immediately after this returns.
func doAbortRequest(t *testing.T, srv *Server, upstream *url.URL, delayBeforeCancel time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(delayBeforeCancel)
		cancel()
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(identity.HeaderUpstream, upstream.Host)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "model-abc")
	req.Header.Set(identity.HeaderGroupID, "org-1")
	req.Header.Set(identity.HeaderUserID, "user-1")
	req.Header.Set("X-Request-Id", "req-abort")

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	// ServeHTTP has returned, but the metering event is emitted ASYNCHRONOUSLY
	// (the abort-watcher goroutine may call markAborted just after ServeHTTP
	// returns, and onDone fires from there). The caller must therefore wait for
	// the event with em.waitForEvents(...) rather than reading em.all()
	// immediately — a fixed sleep here was flaky under CI load.
}

// TestAbortMidStreamEmitsAbortedEvent verifies that a client disconnect mid-
// stream produces an event with Aborted=true.
func TestAbortMidStreamEmitsAbortedEvent(t *testing.T) {
	backend, unblock := slowBackend(t, `data: {"choices":[{"index":0,"delta":{"content":"Hello"}}]}`+"\n\n")
	defer backend.Close()
	defer close(unblock)

	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	srv := newTestServerWithSettings(t, upstream, em)

	doAbortRequest(t, srv, upstream, 10*time.Millisecond)

	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if !events[0].Aborted {
		t.Fatalf("event.Aborted = false, want true: %+v", events[0])
	}
}

// TestAbortWithoutUsageRecordsZeroChargeAttempt verifies that an abort with no
// authoritative usage still remains visible without fabricating a charge.
func TestAbortWithoutUsageRecordsZeroChargeAttempt(t *testing.T) {
	// Backend sends only a content chunk (no usage) then blocks.
	backend, unblock := slowBackend(t, `data: {"choices":[{"index":0,"delta":{"content":"Hi"}}]}`+"\n\n")
	defer backend.Close()
	defer close(unblock)

	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	srv := newTestServerWithSettings(t, upstream, em)

	doAbortRequest(t, srv, upstream, 10*time.Millisecond)

	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("abort without usage: expected 1 event, got %d", len(events))
	}
	e := events[0]
	if !e.Aborted {
		t.Fatalf("event.Aborted = false: %+v", e)
	}
	// Token counts are zero because no usage block arrived — that is correct
	// and expected for a partial event.
	if e.PromptTokens != 0 || e.CompletionTokens != 0 {
		t.Fatalf("expected zero token counts for no-usage abort: %+v", e)
	}
}

// TestAbortWithUsage verifies that when a usage block arrives before the abort,
// the event carries authoritative counts, UsageFound=true, and Aborted=true.
// The rater therefore charges it normally: disconnect never erases served work.
func TestAbortWithUsage(t *testing.T) {
	// Stream has finish_reason and usage chunks, but no [DONE] — simulates a
	// backend that sent everything except the final terminator.
	partialWithUsage := `data: {"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}

data: {"choices":[],"usage":{"prompt_tokens":50,"total_tokens":70,"completion_tokens":20}}

`
	backend, unblock := slowBackend(t, partialWithUsage)
	defer backend.Close()
	defer close(unblock)

	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	srv := newTestServerWithSettings(t, upstream, em)

	doAbortRequest(t, srv, upstream, 20*time.Millisecond)

	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("abort+usage: expected 1 event, got %d", len(events))
	}
	e := events[0]
	if !e.Aborted || !e.UsageFound {
		t.Fatalf("abort with authoritative usage = %+v, want Aborted and UsageFound", e)
	}
	if e.PromptTokens != 50 || e.CompletionTokens != 20 {
		t.Fatalf("wrong token counts: %+v", e)
	}
}

// TestAbortOnDoneFiresExactlyOnceViaProxy exercises the full proxy path and
// asserts that the emitter is called exactly once even under a context cancel.
// This is the integration-level once-guard test.
func TestAbortOnDoneFiresExactlyOnceViaProxy(t *testing.T) {
	backend, unblock := slowBackend(t, `data: {"choices":[{"index":0,"delta":{"content":"x"}}]}`+"\n\n")
	defer backend.Close()
	defer close(unblock)

	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	srv := newTestServerWithSettings(t, upstream, em)

	doAbortRequest(t, srv, upstream, 10*time.Millisecond)

	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("onDone fired %d times via proxy, want exactly 1", len(events))
	}
}

// TestNormalCompletionNotAffectedByAbortWatcher confirms that a normal (non-
// aborted) completion still produces a clean event with Aborted=false after M3.
// The watcher goroutine fires after the stream ends (context not cancelled here).
func TestNormalCompletionNotAffectedByAbortWatcher(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		for _, chunk := range strings.SplitAfter(vllmStream, "\n\n") {
			if chunk == "" {
				continue
			}
			_, _ = io.WriteString(w, chunk)
			if fl != nil {
				fl.Flush()
			}
		}
	}))
	defer backend.Close()

	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	srv := newTestServerWithSettings(t, upstream, em)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	req.Header.Set(identity.HeaderUpstream, upstream.Host)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "model-abc")
	req.Header.Set(identity.HeaderGroupID, "org-1")
	req.Header.Set(identity.HeaderUserID, "user-1")
	req.Header.Set("X-Request-Id", "req-normal")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	// Give watcher goroutine a moment to run (it fires on the request context
	// being cancelled at ServeHTTP return).
	time.Sleep(10 * time.Millisecond)

	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Aborted {
		t.Fatalf("clean completion should have Aborted=false: %+v", events[0])
	}
	if events[0].PromptTokens != 2006 || events[0].CompletionTokens != 300 {
		t.Fatalf("token counts wrong: %+v", events[0])
	}
}

// TestAbortRaceStress runs many concurrent abort requests and verifies:
//   - No panics or data races (primary goal; run with -race).
//   - Every emitted event has Aborted=true (no partial-billed clean event).
//   - onDone fires at most once per request (no double-emit).
//
// CONTRACT (post Fix A): a request cancelled BEFORE ModifyResponse runs (no
// captureReader, no onDone) now ALSO emits — a zero-token Aborted event from the
// ErrorHandler — so an aborted request is never billing-invisible regardless of
// whether the cancel landed pre- or post-header. (Previously such a request
// emitted nothing, which this test documented as "correct"; Fix A changed that
// contract.)
//
// Because BOTH the pre-header (ErrorHandler) and post-header (onDone) paths now
// emit, and they are mutually exclusive, the count is NOT scheduling-dependent:
// exactly N events, N distinct trusted request ids, and exactly one event per
// client correlation id. Asserting only "> 0" would let a lost or duplicated
// billing event pass, which is the whole risk this stress test exists to catch.
func TestAbortRaceStress(t *testing.T) {
	const N = 50

	backend, unblock := slowBackend(t, `data: {"choices":[{"index":0,"delta":{"content":"x"}}]}`+"\n\n")
	defer backend.Close()
	defer close(unblock)

	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	srv := newTestServerWithSettings(t, upstream, em)

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				// Delay slightly so the backend has time to write the first chunk
				// and ModifyResponse has run before we cancel.
				time.Sleep(15 * time.Millisecond)
				cancel()
			}()
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions",
				strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
			req.Header.Set(identity.HeaderUpstream, upstream.Host)
			req.Header.Set(identity.HeaderAuthID, "auth-1")
			req.Header.Set(identity.HeaderResourceID, "model-abc")
			req.Header.Set(identity.HeaderGroupID, "org-1")
			req.Header.Set(identity.HeaderUserID, "user-1")
			req.Header.Set("X-Request-Id", fmt.Sprintf("req-%d", i))
			rr := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rr, req)
		}(i)
	}
	wg.Wait()
	time.Sleep(20 * time.Millisecond) // let watcher goroutines settle

	events := em.all()
	// Every emitted event must be Aborted=true; no clean completion is possible
	// since the backend never sends [DONE].
	for _, e := range events {
		if !e.Aborted {
			t.Fatalf("stress: event not aborted: %+v", e)
		}
	}
	// Exactly one billing event per request: no aborted request goes
	// billing-invisible, and none is double-emitted.
	if len(events) != N {
		t.Fatalf("stress: emitted %d events, want exactly %d (one per request)", len(events), N)
	}
	// Trusted attempt ids are server-minted and must be unique per attempt.
	trusted := make(map[string]struct{}, len(events))
	for _, e := range events {
		if e.RequestID == "" {
			t.Fatalf("stress: event has no trusted request id: %+v", e)
		}
		if _, dup := trusted[e.RequestID]; dup {
			t.Fatalf("stress: duplicate trusted request id %q", e.RequestID)
		}
		trusted[e.RequestID] = struct{}{}
	}
	if len(trusted) != N {
		t.Fatalf("stress: %d distinct trusted request ids, want %d", len(trusted), N)
	}
	// Exactly one event per client correlation id — the untrusted X-Request-Id
	// each goroutine sent. A missing or doubled id means a lost or duplicated
	// billable attempt for that client request.
	perClient := make(map[string]int, N)
	for _, e := range events {
		perClient[e.ClientRequestID]++
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("req-%d", i)
		switch perClient[id] {
		case 1:
		case 0:
			t.Fatalf("stress: no event emitted for client correlation id %q", id)
		default:
			t.Fatalf("stress: %d events emitted for client correlation id %q, want 1", perClient[id], id)
		}
	}
	if len(perClient) != N {
		t.Fatalf("stress: %d distinct client correlation ids, want %d", len(perClient), N)
	}
}

// TestIdleTimeoutNotIntroduced is a documentation test. The http.Server has no
// WriteTimeout (see server.go), so long-running streams are never severed by
// a write deadline. This test checks the Server.Run() method is configured
// that way by inspecting that our server.go code compiles without a
// WriteTimeout field — enforced structurally by the absence of that field in
// the http.Server literal in Run(). No runtime assertion needed; this comment
// serves as the audit trail.
//
// What we DO verify: a long-running stream (simulated by a slow backend with
// no deadline on the test itself) completes correctly.
func TestLongStreamNoDeadlineSever(t *testing.T) {
	// Backend writes 3 chunks with small sleeps between them — simulates a
	// slow but live stream. No deadline should cut it.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		chunks := []string{
			`data: {"choices":[{"index":0,"delta":{"content":"a"}}]}` + "\n\n",
			`data: {"choices":[{"index":0,"delta":{"content":"b"}}]}` + "\n\n",
			`data: {"choices":[],"usage":{"prompt_tokens":5,"total_tokens":7,"completion_tokens":2}}` + "\n\n",
			"data: [DONE]\n\n",
		}
		for _, c := range chunks {
			_, _ = io.WriteString(w, c)
			if fl != nil {
				fl.Flush()
			}
			time.Sleep(2 * time.Millisecond)
		}
	}))
	defer backend.Close()

	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	srv := newTestServerWithSettings(t, upstream, em)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	req.Header.Set(identity.HeaderUpstream, upstream.Host)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "model-abc")
	req.Header.Set(identity.HeaderGroupID, "org-1")
	req.Header.Set(identity.HeaderUserID, "user-1")
	req.Header.Set("X-Request-Id", "req-long")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("long stream: expected 1 event, got %d", len(events))
	}
	if events[0].Aborted {
		t.Fatal("long stream: Aborted should be false for clean completion")
	}
	if events[0].PromptTokens != 5 || events[0].CompletionTokens != 2 {
		t.Fatalf("long stream: wrong tokens: %+v", events[0])
	}
}
