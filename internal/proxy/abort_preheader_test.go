package proxy

// Fix A: pre-header abort emits a zero-token attributable event.
//
// The post-header abort path (client disconnects AFTER the upstream wrote its
// response headers) is covered by abort_test.go: ModifyResponse ran, installed
// the captureReader, and onDone emits an Aborted event. This file covers the
// PRE-header case: the client disconnects BEFORE the upstream writes any headers,
// so RoundTrip fails, ModifyResponse never runs, and onDone never fires. Without
// Fix A such a request — though it passed the billing-identity gate — would emit
// NOTHING. The ErrorHandler now emits one zero-token Aborted event so the request
// is attributable for billing/reconciliation.
//
// Invariant under test: every request past the billing-identity gate emits
// exactly one attributable event — usage on completion, or zero-token Aborted on
// disconnect (pre- or post-header).

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/identity"
)

// blockBeforeHeadersBackend accepts the connection but blocks WITHOUT ever
// writing response headers, until either the request context is cancelled (the
// client abort — the path under test) OR the returned unblock channel is closed
// (the test's cleanup, so backend.Close() can never hang on a lingering conn).
// It never calls WriteHeader/Write, so the proxy's RoundTrip is still in flight
// when the client cancels, failing RoundTrip and routing through ErrorHandler
// (ModifyResponse never runs).
func blockBeforeHeadersBackend(t *testing.T) (*httptest.Server, chan struct{}) {
	t.Helper()
	unblock := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done(): // the abort cancels the proxied (outbound) request
		case <-unblock: // cleanup release
		}
	}))
	return srv, unblock
}

// TestPreHeaderAbortEmitsAttributableEvent: a client that cancels BEFORE the
// upstream writes headers must still produce EXACTLY ONE event — Aborted=true,
// zero token counts, identity populated. This is the contract Fix A establishes.
func TestPreHeaderAbortEmitsAttributableEvent(t *testing.T) {
	backend, unblock := blockBeforeHeadersBackend(t)
	defer backend.Close()
	defer close(unblock)

	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	// BillPartialOnAbort=true so a no-usage abort emits a (zero-token) partial
	// event rather than logging only — that is the path Fix A must drive.
	srv := newTestServerWithSettings(t, upstream, em, true /* billPartial */)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond) // let RoundTrip reach the blocked backend
		cancel()
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "model-abc")
	req.Header.Set(identity.HeaderGroupID, "org-1")
	req.Header.Set(identity.HeaderUserID, "user-1")
	req.Header.Set("X-Request-Id", "req-preheader-abort")

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("pre-header abort: expected EXACTLY 1 attributable event, got %d: %+v", len(events), events)
	}
	e := events[0]
	if !e.Aborted {
		t.Fatalf("pre-header abort event.Aborted = false, want true: %+v", e)
	}
	if e.PromptTokens != 0 || e.CompletionTokens != 0 || e.CachedTokens != 0 {
		t.Fatalf("pre-header abort must carry zero token counts: %+v", e)
	}
	if e.AuthID != "auth-1" || e.ResourceID != "model-abc" {
		t.Fatalf("pre-header abort event must be attributable (AuthID+ResourceID): %+v", e)
	}
	if e.RequestID != "req-preheader-abort" {
		t.Fatalf("pre-header abort RequestID = %q, want req-preheader-abort", e.RequestID)
	}
}

// TestPreHeaderAbortBillPartialFalseNoEvent: with BillPartialOnAbort=false a
// pre-header abort with no usage must NOT emit a billable event — the abort-emit
// obeys the SAME bill-partial policy as the completion path (it does not invent a
// second, policy-bypassing emit). It is logged for reconciliation instead.
func TestPreHeaderAbortBillPartialFalseNoEvent(t *testing.T) {
	backend, unblock := blockBeforeHeadersBackend(t)
	defer backend.Close()
	defer close(unblock)

	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	srv := newTestServerWithSettings(t, upstream, em, false /* billPartial */)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "model-abc")
	req.Header.Set("X-Request-Id", "req-preheader-nobill")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	events := em.waitForEvents(1, 200*time.Millisecond)
	if len(events) != 0 {
		t.Fatalf("BillPartialOnAbort=false, pre-header abort, no usage: expected 0 events, got %d: %+v", len(events), events)
	}
}

// TestNormalCompletionEmitsExactlyOnce guards against double-emit on the NORMAL
// path: a clean completion must produce EXACTLY ONE event from onDone, and the
// ErrorHandler abort-emit must not also fire (ModifyResponse returns nil in
// phoebe, so ErrorHandler never runs on a successful RoundTrip).
func TestNormalCompletionEmitsExactlyOnce(t *testing.T) {
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
	srv := newTestServerWithSettings(t, upstream, em, true)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "model-abc")
	req.Header.Set("X-Request-Id", "req-once")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	// Wait for the one expected event, then confirm a second never appears.
	if got := em.waitForEvents(1, 2*time.Second); len(got) != 1 {
		t.Fatalf("normal completion: expected 1 event, got %d", len(got))
	}
	time.Sleep(50 * time.Millisecond) // window for a spurious second emit
	if got := em.count(); got != 1 {
		t.Fatalf("normal completion double-emitted: count=%d, want exactly 1", got)
	}
	if em.all()[0].Aborted {
		t.Fatalf("clean completion event must have Aborted=false: %+v", em.all()[0])
	}
}
