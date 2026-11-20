package waker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/atlastoken"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/proxy"
)

// staticToken is a Source that always has the same token.
type staticToken string

func (s staticToken) Token() (string, error) { return string(s), nil }

// noToken is a Source with no usable token (missing, expired, malformed).
type noToken struct{}

func (noToken) Token() (string, error) {
	return "", atlastoken.ErrUnavailable
}

// switchableToken has a token until it is withdrawn.
type switchableToken struct{ ok atomic.Bool }

func (s *switchableToken) Token() (string, error) {
	if s.ok.Load() {
		return "svc-token", nil
	}
	return "", atlastoken.ErrUnavailable
}

func TestNewAtlasRequiresATokenSource(t *testing.T) {
	if _, err := NewAtlas("http://atlas.internal", nil, logging.New(logging.ERROR)); err == nil {
		t.Fatal("NewAtlas succeeded with no token source")
	}
}

func captureHeaders(t *testing.T, w *AtlasWaker) http.Header {
	t.Helper()
	var got http.Header
	w.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		got = r.Header.Clone()
		return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody, Request: r}, nil
	})
	if err := w.requestWake(context.Background(), "dep-1", func() {}); err != nil {
		t.Fatalf("requestWake: %v", err)
	}
	return got
}

func TestWakeSendsTheServiceTokenInAuthorization(t *testing.T) {
	w, _ := NewAtlas("http://atlas.internal", staticToken("svc-token"), logging.New(logging.ERROR))
	h := captureHeaders(t, w)
	if h.Get("Authorization") != "token svc-token" {
		t.Fatalf("Authorization = %q", h.Get("Authorization"))
	}
	if h.Get("X-Saturn-Internal-Wake-Token") != "" {
		t.Fatalf("the retired legacy header was sent: %q", h.Get("X-Saturn-Internal-Wake-Token"))
	}
}

// With no usable token, the wake is not sent at all and
// the flight ends as unavailable: no unauthenticated call, no fallback.
func TestWakeWithNoUsableTokenIsNotSent(t *testing.T) {
	var calls atomic.Int32
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer atlas.Close()
	w, _ := NewAtlas(atlas.URL, noToken{}, logging.New(logging.ERROR))
	err := w.requestWake(context.Background(), "dep-1", func() {})
	if !errors.Is(err, errNoCredential) || !errors.Is(err, proxy.ErrWakeUnavailable) {
		t.Fatalf("requestWake = %v; want errNoCredential and ErrWakeUnavailable", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("Atlas was called %d times without a credential", calls.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f := &wakeFlight{done: make(chan struct{}), resourceID: "dep-1"}
	if err := w.callUntilWoken(ctx, "dep-1", f); !errors.Is(err, proxy.ErrWakeUnavailable) {
		t.Fatalf("callUntilWoken = %v; want ErrWakeUnavailable at once", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("Atlas was called %d times without a credential", calls.Load())
	}
}

// A token that becomes usable again (Atlas's job wrote a new one) is picked
// up by the next call, with no restart.
func TestWakeUsesTheTokenAgainOnceItIsBack(t *testing.T) {
	src := &switchableToken{}
	w, _ := NewAtlas("http://atlas.internal", src, logging.New(logging.ERROR))
	if err := w.requestWake(context.Background(), "dep-1", func() {}); !errors.Is(err, errNoCredential) {
		t.Fatalf("requestWake = %v", err)
	}
	src.ok.Store(true)
	if h := captureHeaders(t, w); h.Get("Authorization") != "token svc-token" {
		t.Fatalf("Authorization = %q", h.Get("Authorization"))
	}
}

// Atlas refusing the token (revoked, wrong scope) is a refusal, not a retry.
func TestWakeRefusedTokenIsARefusal(t *testing.T) {
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"message":"Invalid internal wake credential"}`, http.StatusForbidden)
	}))
	defer atlas.Close()
	w, _ := NewAtlas(atlas.URL, staticToken("revoked"), logging.New(logging.ERROR))
	if err := w.requestWake(context.Background(), "dep-1", func() {}); !errors.Is(err, errAtlasRefused) {
		t.Fatalf("requestWake = %v; want errAtlasRefused", err)
	}
}
