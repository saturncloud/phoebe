package waker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/proxy"
)

// startingAtlas answers every wake call with Atlas's wake_starting 503 and
// the phase phaseFor returns for that call.
func startingAtlas(t *testing.T, phaseFor func(n int32) string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != internalWakePath {
			http.NotFound(w, r)
			return
		}
		phase := phaseFor(calls.Add(1))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"message":"Shared graph g worker is still starting; retry the request",` +
			`"error_code":"wake_starting","phase":"` + phase + `"}`))
	}))
	t.Cleanup(atlas.Close)
	return atlas, &calls
}

func startingPhaseOf(err error) string {
	var phaseErr *proxy.StartupPhaseError
	if errors.As(err, &phaseErr) {
		return phaseErr.Phase
	}
	return ""
}

// TestAtlasWakerHoldEndingWhileWorkersStartIsNotReadyWithThePhase: issue
// #111. Atlas answered "still starting" for the whole hold while the worker
// pulled its image, and the client was told the wake service "could not
// confirm whether startup began". Atlas's wake_starting answer proves the
// workers are starting: the hold ends with ErrWakeNotReady and the latest
// phase, never ErrWakeStatusUnknown.
func TestAtlasWakerHoldEndingWhileWorkersStartIsNotReadyWithThePhase(t *testing.T) {
	atlas, calls := startingAtlas(t, func(n int32) string {
		if n < 3 {
			return "waiting_for_gpu_node"
		}
		return "pulling_image"
	})
	waker, err := NewAtlas(atlas.URL, staticToken("svc-token"), "", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.pollInterval = time.Millisecond
	waker.loopWindow = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = waker.Wake(ctx, proxy.WakeTarget{
		ResourceID: "tfm-1", GraphK8sName: "g", UpstreamHost: "model.example", ServedModel: "model-a",
	})
	if !errors.Is(err, proxy.ErrWakeNotReady) || errors.Is(err, proxy.ErrWakeStatusUnknown) {
		t.Fatalf("Wake error = %v, want ErrWakeNotReady only", err)
	}
	if got := startingPhaseOf(err); got != "pulling_image" {
		t.Fatalf("phase = %q, want pulling_image (the latest Atlas reported)", got)
	}
	if calls.Load() < 3 {
		t.Fatalf("Atlas wake calls = %d, want the loop to keep asking", calls.Load())
	}
}

// TestAtlasWakerLoopEndingWhileWorkersStartStaysStarting: a held request that
// joins a loop which ran out its window while the workers were still starting
// gets the same starting answer.
func TestAtlasWakerLoopEndingWhileWorkersStartStaysStarting(t *testing.T) {
	atlas, _ := startingAtlas(t, func(int32) string { return "loading_model" })
	waker, err := NewAtlas(atlas.URL, staticToken("svc-token"), "", logging.New(logging.ERROR))
	if err != nil {
		t.Fatalf("NewAtlas: %v", err)
	}
	waker.pollInterval = time.Millisecond
	waker.loopWindow = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = waker.Wake(ctx, proxy.WakeTarget{
		ResourceID: "tfm-1", GraphK8sName: "g", UpstreamHost: "model.example", ServedModel: "model-a",
	})
	if !errors.Is(err, proxy.ErrWakeNotReady) || startingPhaseOf(err) != "loading_model" {
		t.Fatalf("Wake error = %v, want ErrWakeNotReady with phase loading_model", err)
	}
}

// TestAtlasWakerOtherAtlas503StaysStatusUnknown: only Atlas's wake_starting
// code proves startup began; any other 503 keeps the honest unknown answer.
func TestAtlasWakerOtherAtlas503StaysStatusUnknown(t *testing.T) {
	for _, body := range []string{
		`{"message":"cluster unreachable"}`,
		`{"message":"x","error_code":"something_else","phase":"pulling_image"}`,
		`not json`,
	} {
		atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(body))
		}))
		waker, err := NewAtlas(atlas.URL, staticToken("svc-token"), "", logging.New(logging.ERROR))
		if err != nil {
			t.Fatalf("NewAtlas: %v", err)
		}
		waker.pollInterval = time.Millisecond
		waker.loopWindow = time.Second
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		err = waker.Wake(ctx, proxy.WakeTarget{ResourceID: "tfm-1", GraphK8sName: "g", UpstreamHost: "model.example", ServedModel: "model-a"})
		cancel()
		atlas.Close()
		if !errors.Is(err, proxy.ErrWakeStatusUnknown) || errors.Is(err, proxy.ErrWakeNotReady) {
			t.Fatalf("body %s: Wake error = %v, want ErrWakeStatusUnknown", strings.TrimSpace(body), err)
		}
	}
}
