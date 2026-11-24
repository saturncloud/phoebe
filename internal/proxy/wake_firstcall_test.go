package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

// timedOutDial is the error the transport returns when a Service with no
// ready endpoint drops the connection attempt instead of refusing it (uk2,
// Nebius, 2026-11-23: "dial tcp 10.81.105.254:8000: i/o timeout").
func timedOutDial() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}
}

// TestWakeFirstForwardDialTimeoutStartsTheWake: issue #111. The first request
// to a new shared endpoint, whose frontend was still pulling its image, timed
// out dialling and got a 502 with no wake started. A dial that times out is a
// frontend that is not up, like a refusal: the request is held, the wake
// starts, and the request is served once the frontend answers.
func TestWakeFirstForwardDialTimeoutStartsTheWake(t *testing.T) {
	waker := &fakeWaker{}
	transport, calls := refusedFirstTransport(t, waker, 10*time.Second, func(n int32, req *http.Request) (*http.Response, error) {
		if n <= 2 {
			return nil, timedOutDial()
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
		t.Fatalf("wake calls = %d, want 1 (the first request starts the wake)", got)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("upstream attempts = %d, want 3 (timed-out first forward, timed-out resend, served)", got)
	}
}

// TestWakeFirstForwardDialTimeoutWhileStartingIsA503NamingThePhase: the hold
// ends while Atlas reports the workers pulling their image. The client gets
// the retryable 503 saying so, never a 502 or "could not confirm whether
// startup began".
func TestWakeFirstForwardDialTimeoutWhileStartingIsA503NamingThePhase(t *testing.T) {
	waker := &fakeWaker{err: fmt.Errorf("%w: %w", ErrWakeNotReady, &StartupPhaseError{Phase: StartupPhasePullingImage})}
	transport, _ := refusedFirstTransport(t, waker, 10*time.Second, func(int32, *http.Request) (*http.Response, error) {
		return nil, timedOutDial()
	})
	resp, err := transport.RoundTrip(held(t))
	if err != nil {
		t.Fatalf("RoundTrip error = %v, want a 503", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "30" {
		t.Fatalf("Retry-After = %q, want 30", got)
	}
	var body map[string]string
	raw, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("body %q: %v", raw, err)
	}
	if want := "The model is starting: pulling its container image; retry in about 30 seconds."; body["error"] != want {
		t.Fatalf("error = %q, want %q", body["error"], want)
	}
}

func TestWakeStartingMessageNamesEachPhase(t *testing.T) {
	for phase, want := range map[string]string{
		StartupPhaseWaitingForGPUNode: "The model is starting: waiting for a GPU node; retry in about 30 seconds.",
		StartupPhasePullingImage:      "The model is starting: pulling its container image; retry in about 30 seconds.",
		StartupPhaseLoadingModel:      "The model is starting: loading the model; retry in about 30 seconds.",
		"starting":                    "The model is starting from zero; retry in about 30 seconds.",
		"":                            "The model is starting from zero; retry in about 30 seconds.",
		"something_new":               "The model is starting from zero; retry in about 30 seconds.",
	} {
		if got := wakeStartingMessage(phase); got != want {
			t.Errorf("wakeStartingMessage(%q) = %q, want %q", phase, got, want)
		}
	}
}

func TestIsDialFailure(t *testing.T) {
	live := context.Background()
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"refused", live, refusedDial(), true},
		{"dial timeout", live, timedOutDial(), true},
		{"no route", live, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: no route to host")}, true},
		{"read after connecting", live, &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}, false},
		{"other", live, errors.New("boom"), false},
		{"client went away", gone, timedOutDial(), false},
	}
	for _, c := range cases {
		if got := isDialFailure(c.ctx, c.err); got != c.want {
			t.Errorf("%s: isDialFailure = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestWakeUpstreamTransportDialsWithAShortTimeout: the default transport waits
// 30 s on a dial that is dropped, which delayed the wake by 30 s on uk2.
func TestWakeUpstreamTransportDialsWithAShortTimeout(t *testing.T) {
	server := New(&config.Settings{}, logging.New(logging.ERROR), nil).WithWaker(&fakeWaker{}, time.Second)
	transport := server.newWakeTransport(nil, &url.URL{Host: "graph-frontend.default.svc:8000"},
		identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "shared"}, "m", "request-1", nil)
	if transport.base != wakeUpstreamTransport {
		t.Fatal("a wake route without its own transport does not use wakeUpstreamTransport")
	}
	if wakeDialTimeout > 5*time.Second {
		t.Fatalf("wakeDialTimeout = %s, want at most 5s", wakeDialTimeout)
	}
}
