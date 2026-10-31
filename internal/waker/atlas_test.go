package waker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/proxy"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestNewAtlasValidatesURLScheme(t *testing.T) {
	for _, rawURL := range []string{"http://atlas.internal", "https://atlas.internal"} {
		if _, err := NewAtlas(rawURL, "secret", logging.New(logging.ERROR)); err != nil {
			t.Errorf("NewAtlas(%q): %v", rawURL, err)
		}
	}
	for _, rawURL := range []string{
		"ftp://atlas.internal",
		"file://atlas.internal",
		"atlas.internal",
		"http://atlas.internal?debug=1",
		"http://atlas.internal#fragment",
		"http://user:pass@atlas.internal",
	} {
		if _, err := NewAtlas(rawURL, "secret", logging.New(logging.ERROR)); err == nil {
			t.Errorf("NewAtlas(%q) succeeded, want unsupported URL error", rawURL)
		}
	}
}

func TestAtlasWakerComposesBasePath(t *testing.T) {
	var wakeCalls atomic.Int32
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/prefix" + internalWakePath:
			wakeCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case "/v1/models":
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"model-a"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer atlas.Close()
	u, err := url.Parse(atlas.URL)
	if err != nil {
		t.Fatal(err)
	}
	waker, err := NewAtlas(atlas.URL+"/prefix/", "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	if err := waker.Wake(context.Background(), proxy.WakeTarget{
		ResourceID: "dep-1", UpstreamHost: u.Host, ServedModel: "model-a",
	}); err != nil {
		t.Fatalf("Wake: %v", err)
	}
	if got := wakeCalls.Load(); got != 1 {
		t.Fatalf("wake calls = %d, want 1 at prefixed internal path", got)
	}
}

func TestAtlasWakerAuthenticatesAndSendsOnlyResourceID(t *testing.T) {
	var wakeCalls int
	var modelCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case internalWakePath:
			wakeCalls++
			if got := r.Header.Get(internalWakeHeader); got != "secret" {
				t.Fatalf("wake token = %q, want secret", got)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode wake body: %v", err)
			}
			if len(body) != 1 || body["resource_id"] != "dep-1" {
				t.Fatalf("wake body = %#v, want only resource_id", body)
			}
			w.WriteHeader(http.StatusNoContent)
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			switch modelCalls.Add(1) {
			case 1:
				_, _ = w.Write([]byte(`{"object":"list","data":[]}`))
				return
			case 2:
				_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"model-b"}]}`))
				return
			}
			_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"model-a"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	waker, err := NewAtlas(srv.URL, "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.pollInterval = time.Millisecond
	waker.loopWindow = 500 * time.Millisecond
	if err := waker.Wake(context.Background(), proxy.WakeTarget{
		ResourceID: "dep-1", UpstreamHost: u.Host, ServedModel: "model-a",
	}); err != nil {
		t.Fatalf("Wake: %v", err)
	}
	if wakeCalls != 1 {
		t.Fatalf("wake calls = %d, want 1", wakeCalls)
	}
	if modelCalls.Load() < 3 {
		t.Fatalf("model discovery calls = %d, want empty, wrong-model, then requested-model responses", modelCalls.Load())
	}
}

func TestAtlasWakerTreatsLostWakeResponseAsInProgress(t *testing.T) {
	waker, err := NewAtlas("http://atlas.example", "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	var applied atomic.Bool
	waker.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost || req.URL.Path != internalWakePath {
			t.Fatalf("request = %s %s, want POST %s", req.Method, req.URL.Path, internalWakePath)
		}
		trace := httptrace.ContextClientTrace(req.Context())
		if trace == nil || trace.GotConn == nil {
			t.Fatal("Atlas request has no GotConn trace")
		}
		trace.GotConn(httptrace.GotConnInfo{})
		applied.Store(true)
		return nil, errors.New("response connection lost")
	})}

	waker.pollInterval = time.Millisecond
	waker.loopWindow = 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = waker.Wake(ctx, proxy.WakeTarget{
		ResourceID: "dep-1", UpstreamHost: "model.example", ServedModel: "model-a",
	})
	if !applied.Load() {
		t.Fatal("wake request was not applied")
	}
	if !errors.Is(err, proxy.ErrWakeStatusUnknown) {
		t.Fatalf("Wake error = %v, want ErrWakeStatusUnknown", err)
	}
}

func TestAtlasWakerTreatsServerFailureAfterWakeAsInProgress(t *testing.T) {
	var applied atomic.Bool
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != internalWakePath {
			http.NotFound(w, r)
			return
		}
		applied.Store(true)
		http.Error(w, "response failed after restore", http.StatusInternalServerError)
	}))
	defer atlas.Close()

	waker, err := NewAtlas(atlas.URL, "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.pollInterval = time.Millisecond
	waker.loopWindow = 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = waker.Wake(ctx, proxy.WakeTarget{
		ResourceID: "dep-1", UpstreamHost: "model.example", ServedModel: "model-a",
	})
	if !applied.Load() {
		t.Fatal("wake request was not applied")
	}
	if !errors.Is(err, proxy.ErrWakeStatusUnknown) {
		t.Fatalf("Wake error = %v, want ErrWakeStatusUnknown", err)
	}
}

func TestAtlasWakerBypassesAmbientHTTPProxy(t *testing.T) {
	var proxyCalls atomic.Int32
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxyCalls.Add(1)
		http.Error(w, "unexpected proxy request", http.StatusBadGateway)
	}))
	defer proxyServer.Close()
	t.Setenv("HTTP_PROXY", proxyServer.URL)
	t.Setenv("NO_PROXY", "")

	var atlasCalls atomic.Int32
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atlasCalls.Add(1)
		if got := r.Header.Get(internalWakeHeader); got != "secret" {
			t.Fatalf("wake token = %q, want secret", got)
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer atlas.Close()
	atlasURL, err := url.Parse(atlas.URL)
	if err != nil {
		t.Fatalf("parse Atlas test URL: %v", err)
	}

	waker, err := NewAtlas("http://atlas.internal", "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	transport, ok := waker.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Atlas transport = %T, want *http.Transport", waker.client.Transport)
	}
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, atlasURL.Host)
	}

	err = waker.Wake(context.Background(), proxy.WakeTarget{
		ResourceID: "dep-1", UpstreamHost: "model.example", ServedModel: "model-a",
	})
	if err == nil || errors.Is(err, proxy.ErrWakeNotReady) {
		t.Fatalf("Wake error = %v, want definite Atlas client error", err)
	}
	if got := atlasCalls.Load(); got != 1 {
		t.Fatalf("Atlas received %d requests, want 1", got)
	}
	if got := proxyCalls.Load(); got != 0 {
		t.Fatalf("ambient HTTP proxy received %d requests, want 0", got)
	}
}

// TestAtlasWakerRetriesWhileAtlasIsUnreachable (F2): a call that fails before
// connecting never reached Atlas (restarting during a deploy), so the loop
// asks again; a hold that ends while Atlas is still unreachable is
// ErrWakeUnavailable, which is retryable, not a definite "nothing started".
func TestAtlasWakerRetriesWhileAtlasIsUnreachable(t *testing.T) {
	waker, err := NewAtlas("http://atlas.example", "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.pollInterval = time.Millisecond
	waker.loopWindow = 200 * time.Millisecond
	var calls atomic.Int32
	waker.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("dial refused")
	})}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = waker.Wake(ctx, proxy.WakeTarget{
		ResourceID: "dep-1", UpstreamHost: "model.example", ServedModel: "model-a",
	})
	if !errors.Is(err, proxy.ErrWakeUnavailable) || errors.Is(err, proxy.ErrWakeStatusUnknown) {
		t.Fatalf("Wake error = %v, want ErrWakeUnavailable", err)
	}
	if !strings.Contains(err.Error(), "dial refused") {
		t.Fatalf("Wake error = %v, want the underlying cause in the message", err)
	}
	if calls.Load() < 2 {
		t.Fatalf("Atlas was tried %d times, want the loop to ask again", calls.Load())
	}
}

// TestAtlasWakerGivesUpAfterTheUnreachableBudget (#104 battery): a flight
// that never reaches Atlas ends after unreachableBudget, well before the hold
// or the loop window, and makes no further calls, so held requests release
// their cold-hold slots and no graph is woken after every caller has left.
func TestAtlasWakerGivesUpAfterTheUnreachableBudget(t *testing.T) {
	waker, err := NewAtlas("http://atlas.example", "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.pollInterval = time.Millisecond
	waker.unreachableBudget = 20 * time.Millisecond
	var calls atomic.Int32
	waker.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("dial refused")
	})}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	err = waker.Wake(ctx, proxy.WakeTarget{ResourceID: "dep-1", UpstreamHost: "model.example", ServedModel: "m"})
	if !errors.Is(err, proxy.ErrWakeUnavailable) {
		t.Fatalf("Wake error = %v, want ErrWakeUnavailable", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Wake held for %s, want it to end after the unreachable budget", elapsed)
	}
	after := calls.Load()
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != after {
		t.Fatalf("the flight kept calling Atlas after giving up (%d -> %d calls)", after, calls.Load())
	}
}

// TestAtlasWakerFailsFastOnAMisconfiguredAtlasURL (#104 battery): an Atlas
// host that does not resolve is not a restart. The flight ends at once as
// ErrWakeUnavailable carrying the cause, instead of retrying for the hold.
func TestAtlasWakerFailsFastOnAMisconfiguredAtlasURL(t *testing.T) {
	waker, err := NewAtlas("http://atlas.invalid", "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.pollInterval = time.Millisecond
	var calls atomic.Int32
	waker.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, &net.DNSError{Err: "no such host", Name: "atlas.invalid", IsNotFound: true}
	})}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = waker.Wake(ctx, proxy.WakeTarget{ResourceID: "dep-1", UpstreamHost: "model.example", ServedModel: "m"})
	if !errors.Is(err, proxy.ErrWakeUnavailable) || !strings.Contains(err.Error(), "no such host") {
		t.Fatalf("Wake error = %v, want ErrWakeUnavailable naming the lookup failure", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("Atlas was tried %d times, want 1 for a misconfigured URL", calls.Load())
	}
}

// TestAtlasWakerStaysStatusUnknownOnceAtlasWasReached (#104 battery): after a
// call reached Atlas (which may have started the wake), later calls that fail
// before connecting cannot prove nothing started: the hold ends as "status
// unknown", never "unavailable".
func TestAtlasWakerStaysStatusUnknownOnceAtlasWasReached(t *testing.T) {
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "worker is still starting", http.StatusServiceUnavailable)
	}))
	defer atlas.Close()
	waker, err := NewAtlas(atlas.URL, "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.pollInterval = time.Millisecond
	waker.loopWindow = 200 * time.Millisecond
	var calls atomic.Int32
	base := waker.client.Transport
	waker.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return base.RoundTrip(req)
		}
		return nil, errors.New("dial refused")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = waker.Wake(ctx, proxy.WakeTarget{ResourceID: "dep-1", UpstreamHost: "model.example", ServedModel: "m"})
	if !errors.Is(err, proxy.ErrWakeStatusUnknown) || errors.Is(err, proxy.ErrWakeUnavailable) {
		t.Fatalf("Wake error = %v, want ErrWakeStatusUnknown once Atlas was reached", err)
	}
}

// TestAtlasWakerHoldEndingDuringTheFirstDialIsStatusUnknown (#104 battery):
// nothing has failed yet, so the hold must not claim Atlas was unreachable.
func TestAtlasWakerHoldEndingDuringTheFirstDialIsStatusUnknown(t *testing.T) {
	waker, err := NewAtlas("http://atlas.example", "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.loopWindow = 200 * time.Millisecond
	waker.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done() // the first dial never finishes within the hold
		return nil, req.Context().Err()
	})}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err = waker.Wake(ctx, proxy.WakeTarget{ResourceID: "dep-1", UpstreamHost: "model.example", ServedModel: "m"})
	if !errors.Is(err, proxy.ErrWakeStatusUnknown) || errors.Is(err, proxy.ErrWakeUnavailable) {
		t.Fatalf("Wake error = %v, want ErrWakeStatusUnknown while the first call is still dialing", err)
	}
}

// TestAtlasWakerTreatsAStalePooledConnectionAsUnreached (#104 battery): after
// a successful wake leaves a keep-alive connection in the pool, Atlas goes
// away (a deploy). The next wake must not count that pooled connection as
// having reached Atlas: it ends as "unavailable", not "status unknown".
func TestAtlasWakerTreatsAStalePooledConnectionAsUnreached(t *testing.T) {
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":"m"}]}`))
	}))
	defer upstream.Close()
	upstreamURL, _ := url.Parse(upstream.URL)
	waker, err := NewAtlas(atlas.URL, "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.pollInterval = time.Millisecond
	waker.loopWindow = 300 * time.Millisecond
	if err := waker.Wake(context.Background(), proxy.WakeTarget{ResourceID: "dep-1", UpstreamHost: upstreamURL.Host, ServedModel: "m"}); err != nil {
		t.Fatalf("first Wake: %v", err)
	}
	atlas.Close() // Atlas restarts: its connections and listener are gone

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = waker.Wake(ctx, proxy.WakeTarget{ResourceID: "dep-2", UpstreamHost: upstreamURL.Host, ServedModel: "m"})
	if !errors.Is(err, proxy.ErrWakeUnavailable) {
		t.Fatalf("Wake error = %v, want ErrWakeUnavailable after Atlas went away", err)
	}
}

// TestAtlasWakerWakesOnceAtlasIsBackWithinTheHold (F2): Atlas refuses
// connections for the first calls, then comes back; the same hold completes
// the wake instead of returning the cold response.
func TestAtlasWakerWakesOnceAtlasIsBackWithinTheHold(t *testing.T) {
	var atlasUp atomic.Bool
	var wakeCalls atomic.Int32
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		wakeCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer atlas.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":"model-a"}]}`))
	}))
	defer upstream.Close()
	upstreamURL, _ := url.Parse(upstream.URL)

	waker, err := NewAtlas(atlas.URL, "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.pollInterval = time.Millisecond
	var refused atomic.Int32
	base := waker.client.Transport
	waker.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == upstreamURL.Host || atlasUp.Load() {
			return base.RoundTrip(req)
		}
		if refused.Add(1) >= 3 {
			atlasUp.Store(true)
		}
		return nil, errors.New("dial refused")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := waker.Wake(ctx, proxy.WakeTarget{
		ResourceID: "dep-1", UpstreamHost: upstreamURL.Host, ServedModel: "model-a",
	}); err != nil {
		t.Fatalf("Wake error = %v, want the wake to complete once Atlas is back", err)
	}
	if wakeCalls.Load() != 1 || refused.Load() < 3 {
		t.Fatalf("Atlas wake calls = %d after %d refused dials, want 1 after 3", wakeCalls.Load(), refused.Load())
	}
}

func TestAtlasWakerDoesNotFollowRedirects(t *testing.T) {
	for _, status := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var redirected atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				redirected.Add(1)
				if token := r.Header.Get(internalWakeHeader); token != "" {
					t.Errorf("redirect target received internal wake token %q", token)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer target.Close()
			atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target.URL+internalWakePath, status)
			}))
			defer atlas.Close()

			waker, err := NewAtlas(atlas.URL, "secret", logging.New(logging.ERROR))
			if err != nil {
				t.Fatalf("NewAtlas: %v", err)
			}
			err = waker.Wake(context.Background(), proxy.WakeTarget{
				ResourceID: "dep-1", UpstreamHost: "model.example", ServedModel: "model-a",
			})
			if err == nil || errors.Is(err, proxy.ErrWakeNotReady) {
				t.Fatalf("Wake error = %v, want a definite Atlas redirect status error", err)
			}
			if got := redirected.Load(); got != 0 {
				t.Fatalf("redirect target received %d request(s), want 0", got)
			}
		})
	}
}

// TestAtlasWakerAsksAgainWhileWorkersStart is the regression for a cold wake
// that outlasts Atlas's own readiness wait: Atlas answers 503 until the
// workers are ready and only then restores the Planner, so the waker must
// keep the request held and ask again instead of giving up after one call.
func TestAtlasWakerAsksAgainWhileWorkersStart(t *testing.T) {
	var wakeCalls atomic.Int32
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case internalWakePath:
			if wakeCalls.Add(1) < 3 {
				http.Error(w, "worker is still starting; retry the request", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{"graph_k8s_name":"g","worker_replicas":1}`))
		case "/v1/models":
			_, _ = w.Write([]byte(`{"data":[{"id":"model-a"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer atlas.Close()
	u, _ := url.Parse(atlas.URL)

	waker, err := NewAtlas(atlas.URL, "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.pollInterval = time.Millisecond
	waker.loopWindow = 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := waker.Wake(ctx, proxy.WakeTarget{
		ResourceID: "tfm-1", UpstreamHost: u.Host, ServedModel: "model-a",
		ServingMode: identity.ServingModeShared,
	}); err != nil {
		t.Fatalf("Wake: %v", err)
	}
	if got := wakeCalls.Load(); got != 3 {
		t.Fatalf("Atlas wake calls = %d, want 3 (two still-starting answers, then success)", got)
	}
}

// TestAtlasWakerFinishesWakeAfterClientLeaves: Atlas restores the Planner
// only on the call that finds the workers ready. If the only held client
// disconnects first, the wake loop must keep calling Atlas until that call
// happens; otherwise the graph serves without its Planner.
func TestAtlasWakerFinishesWakeAfterClientLeaves(t *testing.T) {
	var wakeCalls atomic.Int32
	firstCall := make(chan struct{})
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := wakeCalls.Add(1)
		if n == 1 {
			close(firstCall)
		}
		if n < 3 {
			http.Error(w, "worker is still starting", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer atlas.Close()
	waker, err := NewAtlas(atlas.URL, "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.pollInterval = time.Millisecond
	waker.loopWindow = 500 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	go func() { <-firstCall; cancel() }()
	err = waker.Wake(ctx, proxy.WakeTarget{ResourceID: "tfm-1", UpstreamHost: "model.example", ServedModel: "m"})
	if !errors.Is(err, proxy.ErrWakeStatusUnknown) {
		t.Fatalf("Wake error = %v, want ErrWakeStatusUnknown after the client left", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for wakeCalls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := wakeCalls.Load(); got != 3 {
		t.Fatalf("Atlas wake calls = %d, want 3: the loop must continue after the client leaves", got)
	}
}

// TestAtlasWakerSharesOneCallLoopPerEndpoint: many requests held for one
// cold endpoint make one Atlas call, not one each.
func TestAtlasWakerSharesOneCallLoopPerEndpoint(t *testing.T) {
	var wakeCalls atomic.Int32
	release := make(chan struct{})
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case internalWakePath:
			wakeCalls.Add(1)
			<-release
			w.WriteHeader(http.StatusOK)
		case "/v1/models":
			_, _ = w.Write([]byte(`{"data":[{"id":"m"}]}`))
		}
	}))
	defer atlas.Close()
	u, _ := url.Parse(atlas.URL)
	waker, err := NewAtlas(atlas.URL, "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}

	const held = 10
	errs := make(chan error, held)
	for i := 0; i < held; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			errs <- waker.Wake(ctx, proxy.WakeTarget{ResourceID: "tfm-1", UpstreamHost: u.Host, ServedModel: "m"})
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for wakeCalls.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let every request join before Atlas answers
	close(release)
	for i := 0; i < held; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("Wake: %v", err)
		}
	}
	if got := wakeCalls.Load(); got != 1 {
		t.Fatalf("Atlas wake calls = %d, want 1 for %d held requests", got, held)
	}
}

// TestAtlasWakerKeepsUnknownStatusAfterLaterFailure: once Atlas answered
// "still starting", a later refusal or an unreachable Atlas must not turn
// into a definite failure (which would hand the client the cold 404).
func TestAtlasWakerKeepsUnknownStatusAfterLaterFailure(t *testing.T) {
	for _, tc := range []struct {
		name  string
		later func(*httptest.Server, http.ResponseWriter)
	}{
		{"later 4xx", func(_ *httptest.Server, w http.ResponseWriter) {
			http.Error(w, "graph assignment changed", http.StatusBadRequest)
		}},
		{"Atlas goes away", func(s *httptest.Server, w http.ResponseWriter) {
			http.Error(w, "still starting", http.StatusServiceUnavailable)
			go s.CloseClientConnections()
			go s.Listener.Close()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			var srv *httptest.Server
			srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) == 1 {
					http.Error(w, "still starting", http.StatusServiceUnavailable)
					return
				}
				tc.later(srv, w)
			}))
			defer srv.Close()
			waker, err := NewAtlas(srv.URL, "secret", logging.New(logging.ERROR))
			if err != nil {
				t.Fatalf("NewAtlas: %v", err)
			}
			waker.pollInterval = time.Millisecond
			waker.loopWindow = 500 * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			err = waker.Wake(ctx, proxy.WakeTarget{ResourceID: "tfm-1", UpstreamHost: "model.example", ServedModel: "m"})
			if !errors.Is(err, proxy.ErrWakeStatusUnknown) {
				t.Fatalf("Wake error = %v, want ErrWakeStatusUnknown", err)
			}
		})
	}
}

// TestAtlasWakerFirstRefusalIsDefinite: a 4xx on the first call means nothing
// started; the client gets the cold response after exactly one call.
func TestAtlasWakerFirstRefusalIsDefinite(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "Inference endpoint not found", http.StatusNotFound)
	}))
	defer srv.Close()
	waker, err := NewAtlas(srv.URL, "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	err = waker.Wake(context.Background(), proxy.WakeTarget{ResourceID: "tfm-1", UpstreamHost: "model.example", ServedModel: "m"})
	if err == nil || errors.Is(err, proxy.ErrWakeStatusUnknown) || errors.Is(err, proxy.ErrWakeNotReady) {
		t.Fatalf("Wake error = %v, want a definite refusal", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("Atlas wake calls = %d, want 1", got)
	}
}

// TestAtlasWakerLoopOutlivesAShortHold is the regression for uk2's 3 s
// wake.timeout: the client's hold ended long before the worker was ready, the
// loop ended with it, and the Planner came back only with the reaper's
// repair pass. The loop's lifetime must not depend on the hold.
func TestAtlasWakerLoopOutlivesAShortHold(t *testing.T) {
	start := time.Now()
	var restored atomic.Bool
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if time.Since(start) < 200*time.Millisecond {
			http.Error(w, "worker is still starting", http.StatusServiceUnavailable)
			return
		}
		restored.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer atlas.Close()
	waker, err := NewAtlas(atlas.URL, "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.pollInterval = 5 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = waker.Wake(ctx, proxy.WakeTarget{ResourceID: "tfm-1", UpstreamHost: "model.example", ServedModel: "m"})
	if !errors.Is(err, proxy.ErrWakeStatusUnknown) {
		t.Fatalf("Wake error = %v, want ErrWakeStatusUnknown when the hold ends first", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for !restored.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !restored.Load() {
		t.Fatal("the wake loop stopped with the client's hold; Atlas never got the call that restores the Planner")
	}
}

// graphWakeServers is an Atlas that records which endpoints it was asked to
// wake (refusing those in refuse with a 404, after delay) and an upstream that
// lists every model, so a successful wake is immediately ready.
func graphWakeServers(t *testing.T, refuse map[string]bool, delay time.Duration) (*AtlasWaker, string, *sync.Map) {
	t.Helper()
	var calls sync.Map // resource id -> *atomic.Int32
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ResourceID string `json:"resource_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		counter, _ := calls.LoadOrStore(body.ResourceID, &atomic.Int32{})
		counter.(*atomic.Int32).Add(1)
		time.Sleep(delay)
		if refuse[body.ResourceID] {
			http.Error(w, "Inference endpoint not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(atlas.Close)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":"ep-a"},{"id":"ep-b"}]}`))
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, _ := url.Parse(upstream.URL)
	waker, err := NewAtlas(atlas.URL, "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.pollInterval = time.Millisecond
	return waker, upstreamURL.Host, &calls
}

func wakeCalls(calls *sync.Map, resourceID string) int32 {
	if counter, ok := calls.Load(resourceID); ok {
		return counter.(*atomic.Int32).Load()
	}
	return 0
}

// TestAtlasWakerSharesOneLoopPerGraph (F1): two endpoints on one shared graph
// waking at once share one Atlas loop (each Atlas call holds a database
// connection under the graph's lock), and both are served.
func TestAtlasWakerSharesOneLoopPerGraph(t *testing.T) {
	waker, host, calls := graphWakeServers(t, nil, 50*time.Millisecond)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, ep := range []string{"ep-a", "ep-b"} {
		wg.Add(1)
		go func(i int, ep string) {
			defer wg.Done()
			errs[i] = waker.Wake(context.Background(), proxy.WakeTarget{
				GraphK8sName: "tf-shared-g", ResourceID: ep, UpstreamHost: host, ServedModel: ep,
			})
		}(i, ep)
		time.Sleep(10 * time.Millisecond) // the second joins the first's flight
	}
	wg.Wait()
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("Wake errors = %v, want both served", errs)
	}
	if got := wakeCalls(calls, "ep-a") + wakeCalls(calls, "ep-b"); got != 1 {
		t.Fatalf("Atlas wake calls = %d (a=%d b=%d), want 1 shared loop", got, wakeCalls(calls, "ep-a"), wakeCalls(calls, "ep-b"))
	}
}

// TestAtlasWakerJoinerDoesNotInheritAnotherEndpointsRefusal (F1): Atlas
// refuses the leader's endpoint (deleted, or moved off the graph). That
// refusal is about the leader, so a joiner for another endpoint on the same
// graph runs its own loop and is served.
func TestAtlasWakerJoinerDoesNotInheritAnotherEndpointsRefusal(t *testing.T) {
	waker, host, calls := graphWakeServers(t, map[string]bool{"ep-a": true}, 50*time.Millisecond)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, ep := range []string{"ep-a", "ep-b"} {
		wg.Add(1)
		go func(i int, ep string) {
			defer wg.Done()
			errs[i] = waker.Wake(context.Background(), proxy.WakeTarget{
				GraphK8sName: "tf-shared-g", ResourceID: ep, UpstreamHost: host, ServedModel: ep,
			})
		}(i, ep)
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()
	if errs[0] == nil || errors.Is(errs[0], proxy.ErrWakeStatusUnknown) || errors.Is(errs[0], proxy.ErrWakeUnavailable) {
		t.Fatalf("leader Wake error = %v, want the definite refusal", errs[0])
	}
	if errs[1] != nil {
		t.Fatalf("joiner Wake error = %v, want served by its own loop", errs[1])
	}
	if wakeCalls(calls, "ep-a") != 1 || wakeCalls(calls, "ep-b") != 1 {
		t.Fatalf("Atlas wake calls a=%d b=%d, want one each", wakeCalls(calls, "ep-a"), wakeCalls(calls, "ep-b"))
	}
}

// TestAtlasWakerSameEndpointJoinerSharesTheRefusal (F1): a joiner for the
// SAME refused endpoint takes the refusal; it does not ask Atlas again.
func TestAtlasWakerSameEndpointJoinerSharesTheRefusal(t *testing.T) {
	waker, host, calls := graphWakeServers(t, map[string]bool{"ep-a": true}, 50*time.Millisecond)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = waker.Wake(context.Background(), proxy.WakeTarget{
				GraphK8sName: "tf-shared-g", ResourceID: "ep-a", UpstreamHost: host, ServedModel: "ep-a",
			})
		}(i)
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()
	if errs[0] == nil || errs[1] == nil {
		t.Fatalf("Wake errors = %v, want both refused", errs)
	}
	if got := wakeCalls(calls, "ep-a"); got != 1 {
		t.Fatalf("Atlas wake calls = %d, want 1", got)
	}
}
