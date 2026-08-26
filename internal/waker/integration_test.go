package waker

// The serveWithWake <-> KubeWaker integration seam, exercised through the
// proxy's PUBLIC surface (this lives in package waker because the proxy's
// in-package tests cannot import waker without a cycle — waker implements the
// proxy's Waker interface).

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
	"github.com/saturncloud/phoebe/internal/proxy"
)

// TestServeWithWake_DGDSAMissing_ServesColdResponse: end to end through the
// real proxy handler — a wakeable cold request whose graph has NO DGDSA gets
// the honest cold response (the wake errors, nothing hangs, nothing 500s).
func TestServeWithWake_DGDSAMissing_ServesColdResponse(t *testing.T) {
	// A permanently-cold Dynamo-shaped backend (model scaled to 0 -> 404).
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Model not found"}`))
	}))
	defer backend.Close()

	// KubeWaker over an EMPTY fake cluster: the DGDSA read fails -> wake error.
	w, client := newFakeWaker(t)

	srv := proxy.New(&config.Settings{ListenAddr: ":0"}, logging.New(logging.ERROR), &metering.LogEmitter{Log: logging.New(logging.ERROR)}).
		WithWaker(w, time.Second, 3)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "tfm-1")
	req.Header.Set(identity.HeaderServedModel, "m") // wakeable route
	req.Header.Set(identity.HeaderUpstream, strings.TrimPrefix(backend.URL, "http://"))
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	// The client sees the honest cold 404 (flushed by serveWithWake after the
	// wake failed), not a hang and not a phoebe-made 5xx.
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want the cold 404 passed through", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Model not found") {
		t.Fatalf("body = %q, want the upstream's own cold body", rr.Body.String())
	}
	// The waker really was consulted (one DGDSA read) and wrote nothing.
	if patches := patchActions(client); len(patches) != 0 {
		t.Fatalf("issued %d patches, want 0", len(patches))
	}
	gets := 0
	for _, a := range client.Actions() {
		if a.GetVerb() == "get" {
			gets++
		}
	}
	if gets == 0 {
		t.Fatal("expected the waker to have read the (missing) DGDSA")
	}
}
