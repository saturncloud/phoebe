package proxy

// Fix B: client-cancel classification in the ReverseProxy ErrorHandler.
//
// Go delivers a mid-flight client cancellation WRAPPED (a *url.Error around
// context.Canceled is the common shape on the RoundTrip path). The old identity
// compare (err == context.Canceled) never matched the wrapped form, so a real
// abort was misclassified as an upstream fault: logged at error AND 502'd over a
// connection that was already gone. isClientAbort uses errors.Is, which sees
// through the wrapper, and also covers context.DeadlineExceeded. These tests pin
// that regression.

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
		{"bare deadline", context.DeadlineExceeded, true},
		{"wrapped canceled (url.Error)", &url.Error{Op: "Post", URL: "http://up", Err: context.Canceled}, true},
		{"wrapped deadline (url.Error)", &url.Error{Op: "Post", URL: "http://up", Err: context.DeadlineExceeded}, true},
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

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"wrapped canceled", &url.Error{Op: "Post", URL: upstream.String(), Err: context.Canceled}},
		{"wrapped deadline", &url.Error{Op: "Post", URL: upstream.String(), Err: context.DeadlineExceeded}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := srv.errorHandler(upstream.String(), id, "req-1")
			rr := httptest.NewRecorder()
			h(rr, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil), tc.err)

			if rr.Code == http.StatusBadGateway {
				t.Fatalf("wrapped client abort 502'd (Code=%d): a real abort must not write a 502 over a dead connection", rr.Code)
			}
		})
	}
}

// TestErrorHandlerUpstreamFaultStill502 guards the negative: a GENUINE upstream
// fault (not a client abort) must still be 502'd, so the abort special-case did
// not swallow real errors.
func TestErrorHandlerUpstreamFaultStill502(t *testing.T) {
	upstream, _ := url.Parse("http://upstream.invalid")
	em := &recordingEmitter{}
	srv := newTestServerE(t, upstream, em)
	id := identity.Identity{AuthID: "auth-1", ResourceID: "model-abc"}

	h := srv.errorHandler(upstream.String(), id, "req-1")
	rr := httptest.NewRecorder()
	h(rr, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil),
		&url.Error{Op: "Post", URL: upstream.String(), Err: http.ErrServerClosed})

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("genuine upstream fault Code=%d, want 502", rr.Code)
	}
}
