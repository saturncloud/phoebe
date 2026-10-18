package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

// refusedDial is the error the transport returns when kube-proxy refuses a
// connection to a Service with no ready endpoints.
func refusedDial() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}
}

// wakeTransportWith returns a wake transport whose upstream answers cold first,
// then runs after(attempt, req) for every later attempt (attempt 1 is the first
// one after the wake). It records each attempt's request body.
func wakeTransportWith(t *testing.T, hold time.Duration, after func(int32, *http.Request) (*http.Response, error)) (*wakeRoundTripper, *atomic.Int32, *[]string) {
	t.Helper()
	var calls atomic.Int32
	bodies := []string{}
	server := New(&config.Settings{}, logging.New(logging.ERROR), nil).WithWaker(&fakeWaker{}, hold)
	transport := server.newWakeTransport(
		roundTripFunc(func(req *http.Request) (*http.Response, error) {
			n := calls.Add(1)
			if req.Body != nil {
				b, _ := io.ReadAll(req.Body)
				bodies = append(bodies, string(b))
			}
			if n == 1 {
				return &http.Response{
					StatusCode: http.StatusNotFound, Header: http.Header{}, Request: req,
					Body: io.NopCloser(strings.NewReader(`{"message":"Model not found"}`)),
				}, nil
			}
			return after(n-1, req)
		}),
		&url.URL{Host: "graph-frontend.default.svc:8000"},
		identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "shared"},
		"m", "request-1", nil,
	)
	return transport, &calls, &bodies
}

func held(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://graph-frontend.default.svc:8000/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func ok(req *http.Request) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Request: req, Body: io.NopCloser(strings.NewReader(`{"served":true}`))}
}

// TestWakeRetriesARefusedConnectionUntilTheFrontendIsBack: right after a wake
// the frontend can leave its Service for a few seconds (its readiness check
// fails while the woken worker registers), and the forward is refused. The held
// request is resent, body included, and served once the frontend is back
// instead of returning a 502 (demo-qa, GB300 build 9).
func TestWakeRetriesARefusedConnectionUntilTheFrontendIsBack(t *testing.T) {
	transport, calls, bodies := wakeTransportWith(t, 10*time.Second, func(n int32, req *http.Request) (*http.Response, error) {
		if n <= 2 {
			return nil, refusedDial()
		}
		return ok(req), nil
	})
	resp, err := transport.RoundTrip(held(t))
	if err != nil {
		t.Fatalf("RoundTrip error = %v, want the served response", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("upstream attempts = %d, want 4 (cold, two refusals, served)", got)
	}
	for i, body := range *bodies {
		if body != `{"model":"m"}` {
			t.Fatalf("attempt %d sent body %q, want the original request body", i+1, body)
		}
	}
}

// TestWakeRefusedConnectionPastTheHoldDeadlineIsReturned: the retries stop at
// the wake hold's deadline, so a frontend that stays away still fails the
// request rather than holding it longer than the hold allows.
func TestWakeRefusedConnectionPastTheHoldDeadlineIsReturned(t *testing.T) {
	transport, calls, _ := wakeTransportWith(t, wakeRefusedRetryInterval/2, func(int32, *http.Request) (*http.Response, error) {
		return nil, refusedDial()
	})
	_, err := transport.RoundTrip(held(t))
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("RoundTrip error = %v, want the refusal", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("upstream attempts = %d, want 2 (cold, one refusal, no retry past the deadline)", got)
	}
}

// TestWakeOtherForwardErrorsAreNotRetried: only a refused connection is known
// to carry no request bytes; any other transport error is returned at once.
func TestWakeOtherForwardErrorsAreNotRetried(t *testing.T) {
	boom := errors.New("connection reset by peer")
	transport, calls, _ := wakeTransportWith(t, 10*time.Second, func(int32, *http.Request) (*http.Response, error) {
		return nil, boom
	})
	_, err := transport.RoundTrip(held(t))
	if !errors.Is(err, boom) {
		t.Fatalf("RoundTrip error = %v, want %v", err, boom)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("upstream attempts = %d, want 2 (cold, one failed forward)", got)
	}
}

// TestWakeRefusedRetryStopsWhenTheClientGoesAway: a client that disconnects
// while a refused forward waits to be resent ends the retries at once, without
// waiting out the hold.
func TestWakeRefusedRetryStopsWhenTheClientGoesAway(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport, calls, _ := wakeTransportWith(t, 10*time.Second, func(int32, *http.Request) (*http.Response, error) {
		// The client goes away during the first wait between resends.
		time.AfterFunc(wakeRefusedRetryInterval/5, cancel)
		return nil, refusedDial()
	})
	start := time.Now()
	_, err := transport.RoundTrip(held(t).WithContext(ctx))
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("RoundTrip error = %v, want the refusal", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("upstream attempts = %d, want 2 (cold, one refusal, no resend after the client left)", got)
	}
	if elapsed := time.Since(start); elapsed >= wakeRefusedRetryInterval {
		t.Fatalf("returned after %v, want before the %v resend interval", elapsed, wakeRefusedRetryInterval)
	}
}

// refusedFirstTransport returns a wake transport whose upstream runs
// answer(attempt, req) for every attempt (attempt 1 is the first forward) and
// whose waker is waker. It counts attempts.
func refusedFirstTransport(t *testing.T, waker *fakeWaker, hold time.Duration, answer func(int32, *http.Request) (*http.Response, error)) (*wakeRoundTripper, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	server := New(&config.Settings{}, logging.New(logging.ERROR), nil).WithWaker(waker, hold)
	transport := server.newWakeTransport(
		roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return answer(calls.Add(1), req)
		}),
		&url.URL{Host: "graph-frontend.default.svc:8000"},
		identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "shared"},
		"m", "request-1", nil,
	)
	return transport, &calls
}

// TestWakeHoldsARefusedFirstForwardUntilTheFrontendIsBack: Grove recreates a
// shared graph's whole replica, frontend included, after its worker has sat at
// zero for terminationDelay (demo-qa, GB300: every 4 h). A request arriving
// while the frontend is away is refused on its first forward. It is held like a
// cold response, the graph is woken, and the request is served once the
// frontend is back, where before it got a 502.
func TestWakeHoldsARefusedFirstForwardUntilTheFrontendIsBack(t *testing.T) {
	waker := &fakeWaker{}
	transport, calls := refusedFirstTransport(t, waker, 10*time.Second, func(n int32, req *http.Request) (*http.Response, error) {
		if n <= 2 {
			return nil, refusedDial()
		}
		return ok(req), nil
	})
	resp, err := transport.RoundTrip(held(t))
	if err != nil {
		t.Fatalf("RoundTrip error = %v, want the served response", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&waker.calls); got != 1 {
		t.Fatalf("wake calls = %d, want 1", got)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("upstream attempts = %d, want 3 (refused first forward, refused resend, served)", got)
	}
}

// TestWakeRefusedFirstForwardWithADefiniteWakeFailureReturnsTheRefusal: when
// Atlas definitely refuses the wake, nothing is starting, so the client gets
// the original refusal, exactly as before this change.
func TestWakeRefusedFirstForwardWithADefiniteWakeFailureReturnsTheRefusal(t *testing.T) {
	waker := &fakeWaker{err: errors.New("definite wake failure")}
	transport, calls := refusedFirstTransport(t, waker, 10*time.Second, func(int32, *http.Request) (*http.Response, error) {
		return nil, refusedDial()
	})
	_, err := transport.RoundTrip(held(t))
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("RoundTrip error = %v, want the refusal", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream attempts = %d, want 1", got)
	}
}

// TestWakeRefusedFirstForwardWhileTheWakeIsStillRunningIsARetryable503: a
// wake that is still in progress at the end of the hold answers the usual
// "starting" 503 with Retry-After, not a transport error.
func TestWakeRefusedFirstForwardWhileTheWakeIsStillRunningIsARetryable503(t *testing.T) {
	waker := &fakeWaker{err: ErrWakeNotReady}
	transport, _ := refusedFirstTransport(t, waker, 10*time.Second, func(int32, *http.Request) (*http.Response, error) {
		return nil, refusedDial()
	})
	resp, err := transport.RoundTrip(held(t))
	if err != nil {
		t.Fatalf("RoundTrip error = %v, want a 503", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("status = %d Retry-After = %q, want a retryable 503", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
}
