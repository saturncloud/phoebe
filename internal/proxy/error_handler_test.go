package proxy

// Fix B: client-cancel classification in the ReverseProxy ErrorHandler.
//
// Go delivers a mid-flight client cancellation WRAPPED (a *url.Error around
// context.Canceled is the common shape on the RoundTrip path). The old identity
// compare (err == context.Canceled) never matched the wrapped form, so a real
// abort was misclassified as an upstream fault: logged at error AND 502'd over a
// connection that was already gone. isClientAbort uses errors.Is, which sees
// through the wrapper. It matches context.Canceled ONLY — context.DeadlineExceeded
// is an upstream fault in this proxy (no client deadline exists), so it must 502,
// not abort. These tests pin both directions.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/saturncloud/phoebe/internal/identity"
)

func TestIsClientAbort(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"bare canceled", context.Canceled, true},
		{"wrapped canceled (url.Error)", &url.Error{Op: "Post", URL: "http://up", Err: context.Canceled}, true},
		// DeadlineExceeded is NOT a client abort in this proxy: there is no
		// client-side request deadline, so a DeadlineExceeded reaching the
		// ErrorHandler is ALWAYS an upstream/transport fault (dial timeout,
		// response-header timeout). Classifying it as an abort would drop the
		// 502, log the outage at debug, and emit a spurious zero-token Aborted
		// billing row. A prior change asserted these as `true` — that pinned a
		// real regression; they MUST be false.
		{"bare deadline is an upstream fault", context.DeadlineExceeded, false},
		{"wrapped deadline (url.Error) is an upstream fault", &url.Error{Op: "Post", URL: "http://up", Err: context.DeadlineExceeded}, false},
		{"genuine upstream error", &url.Error{Op: "Post", URL: "http://up", Err: http.ErrServerClosed}, false},
		{"plain error", http.ErrHandlerTimeout, false},
		{"nil", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isClientAbort(tc.err); got != tc.want {
				t.Fatalf("isClientAbort(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestErrorHandlerClassifiesWrappedCancel drives the ErrorHandler with a WRAPPED
// cancellation (*url.Error{Err: context.Canceled}) — the exact shape Go's
// transport produces on a mid-flight client disconnect — and asserts the
// client-disconnect treatment: NO 502 is written. A 502 here is the regression
// (an identity compare would fall through and 502 a dead connection).
func TestErrorHandlerClassifiesWrappedCancel(t *testing.T) {
	upstream, _ := url.Parse("http://upstream.invalid")
	em := &recordingEmitter{}
	srv := newTestServerE(t, upstream, em)
	id := identity.Identity{AuthID: "auth-1", ResourceID: "model-abc"}

	// Only context.Canceled is a client abort (DeadlineExceeded is an upstream
	// fault here — covered by TestErrorHandlerUpstreamFaultStill502). A wrapped
	// cancel must NOT 502 over the already-dead connection. (The abort's
	// zero-token emit and its BillPartialOnAbort gating are covered by the
	// dedicated TestPreHeaderAbort* tests, not re-asserted here.)
	h := srv.errorHandler(upstream.String(), id, "req-1")
	rr := httptest.NewRecorder()
	h(rr, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		&url.Error{Op: "Post", URL: upstream.String(), Err: context.Canceled})

	if rr.Code == http.StatusBadGateway {
		t.Fatal("wrapped client abort 502'd: a real abort must not write a 502 over a dead connection")
	}
	_ = em // emitter wired for parity with the server constructor; emit policy tested in TestPreHeaderAbort*
}

// TestErrorHandlerUpstreamFaultStill502 guards the negative: a GENUINE upstream
// fault (not a client abort) must still be 502'd, so the abort special-case did
// not swallow real errors.
func TestErrorHandlerUpstreamFaultStill502(t *testing.T) {
	upstream, _ := url.Parse("http://upstream.invalid")
	id := identity.Identity{AuthID: "auth-1", ResourceID: "model-abc"}

	// Both a plain upstream error AND a wrapped DeadlineExceeded (a dial/header
	// timeout — an upstream fault in this proxy, NOT a client abort) must 502 and
	// must NOT emit a spurious zero-token Aborted billing row.
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"upstream error", &url.Error{Op: "Post", URL: upstream.String(), Err: http.ErrServerClosed}},
		{"upstream deadline (dial/header timeout)", &url.Error{Op: "Post", URL: upstream.String(), Err: context.DeadlineExceeded}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			emTC := &recordingEmitter{}
			srvTC := newTestServerE(t, upstream, emTC)
			h := srvTC.errorHandler(upstream.String(), id, "req-1")
			rr := httptest.NewRecorder()
			h(rr, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil), tc.err)

			if rr.Code != http.StatusBadGateway {
				t.Fatalf("upstream fault Code=%d, want 502", rr.Code)
			}
			if n := len(emTC.all()); n != 0 {
				t.Fatalf("upstream fault emitted %d events, want 0 (no spurious billing row)", n)
			}
		})
	}
}
