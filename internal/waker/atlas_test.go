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

	err = waker.Wake(context.Background(), proxy.WakeTarget{
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
	err = waker.Wake(context.Background(), proxy.WakeTarget{
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

func TestAtlasWakerTreatsPreConnectionFailureAsNotStarted(t *testing.T) {
	waker, err := NewAtlas("http://atlas.example", "secret", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial refused")
	})}

	err = waker.Wake(context.Background(), proxy.WakeTarget{
		ResourceID: "dep-1", UpstreamHost: "model.example", ServedModel: "model-a",
	})
	if err == nil || errors.Is(err, proxy.ErrWakeNotReady) {
		t.Fatalf("Wake error = %v, want a definite pre-connection failure", err)
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

type recordingWaker struct{ calls int }

func (w *recordingWaker) Wake(context.Context, proxy.WakeTarget) error {
	w.calls++
	return nil
}

func TestModeWakerKeepsSharedAndDedicatedActuatorsSeparate(t *testing.T) {
	shared := &recordingWaker{}
	dedicated := &recordingWaker{}
	w := ModeWaker{Shared: shared, Dedicated: dedicated}
	if err := w.Wake(context.Background(), proxy.WakeTarget{ServingMode: identity.ServingModeShared}); err != nil {
		t.Fatal(err)
	}
	if err := w.Wake(context.Background(), proxy.WakeTarget{ServingMode: identity.ServingModeDedicated}); err != nil {
		t.Fatal(err)
	}
	if shared.calls != 1 || dedicated.calls != 1 {
		t.Fatalf("calls shared=%d dedicated=%d, want one each", shared.calls, dedicated.calls)
	}
}

func TestModeWakerReportsAvailableActuatorByServingMode(t *testing.T) {
	shared := &recordingWaker{}
	dedicated := &recordingWaker{}
	sharedOnly := ModeWaker{Shared: shared}
	dedicatedOnly := ModeWaker{Dedicated: dedicated}

	if !sharedOnly.SupportsWake(proxy.WakeTarget{ServingMode: identity.ServingModeShared}) {
		t.Fatal("shared-only waker does not report shared support")
	}
	if sharedOnly.SupportsWake(proxy.WakeTarget{ServingMode: identity.ServingModeDedicated}) {
		t.Fatal("shared-only waker reports dedicated support")
	}
	if !dedicatedOnly.SupportsWake(proxy.WakeTarget{ServingMode: identity.ServingModeDedicated}) {
		t.Fatal("dedicated-only waker does not report dedicated support")
	}
	if dedicatedOnly.SupportsWake(proxy.WakeTarget{ServingMode: identity.ServingModeShared}) {
		t.Fatal("dedicated-only waker reports shared support")
	}
	if dedicatedOnly.SupportsWake(proxy.WakeTarget{ServingMode: "unknown"}) {
		t.Fatal("waker reports support for an unknown serving mode")
	}
}
