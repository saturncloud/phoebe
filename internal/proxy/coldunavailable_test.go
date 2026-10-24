package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/identity"
)

// coldDedicated drives one POST at a bound dedicated (aggregated, not
// wakeable) route whose upstream answers status/body.
func coldDedicated(t *testing.T, status int, body, requestModel string) (*httptest.ResponseRecorder, int32, *recordingEmitter) {
	t.Helper()
	var calls int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(backend.Close)
	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	srv := newTestServerE(t, upstream, em)
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		strings.NewReader(`{"model":"`+requestModel+`","messages":[]}`),
	)
	setUpstream(req, upstream)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "deployment-a")
	req.Header.Set(identity.HeaderServedModel, "endpoint-a")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	return rr, atomic.LoadInt32(&calls), em
}

func assertRetryLater(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %q)", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Fatalf("503 has no Retry-After")
	}
	if !strings.Contains(rr.Body.String(), coldUnavailableMessage) {
		t.Fatalf("body = %q, want phoebe's retry-later body", rr.Body.String())
	}
}

func TestDedicatedColdModelNotFoundIsARetryable503(t *testing.T) {
	rr, calls, em := coldDedicated(t, http.StatusNotFound,
		`{"message":"Model not found","type":"Not Found","code":404}`, "endpoint-a")

	assertRetryLater(t, rr)
	if calls != 1 {
		t.Fatalf("upstream called %d times, want 1 (no wake, no retry)", calls)
	}
	// The attempt is recorded like a wake's own 503: no tokens.
	for _, e := range em.waitForEvents(1, time.Second) {
		if e.PromptTokens != 0 || e.CompletionTokens != 0 {
			t.Fatalf("a never-served attempt was billed tokens: %+v", e)
		}
	}
}

func TestDedicatedZeroWorker503IsTheStandardRetryable503(t *testing.T) {
	rr, _, _ := coldDedicated(t, http.StatusServiceUnavailable,
		`{"message":"Model endpoint-a is not ready to serve requests yet"}`, "endpoint-a")

	assertRetryLater(t, rr)
}

func TestDedicatedOther404IsPassedThrough(t *testing.T) {
	rr, _, _ := coldDedicated(t, http.StatusNotFound, `{"message":"no such route"}`, "endpoint-a")

	if rr.Code != http.StatusNotFound || !strings.Contains(rr.Body.String(), "no such route") {
		t.Fatalf("status %d body %q, want the upstream 404 unchanged", rr.Code, rr.Body.String())
	}
}

func TestDedicatedOverload503IsPassedThrough(t *testing.T) {
	rr, _, _ := coldDedicated(t, http.StatusServiceUnavailable, `{"message":"queue full"}`, "endpoint-a")

	if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "queue full") {
		t.Fatalf("status %d body %q, want the upstream 503 unchanged", rr.Code, rr.Body.String())
	}
}

func TestDedicatedServingResponseIsUntouched(t *testing.T) {
	rr, _, _ := coldDedicated(t, http.StatusOK,
		`{"model":"endpoint-a","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`,
		"endpoint-a")

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
}

func TestWrongModelStillGets403AndNeverReachesTheEngine(t *testing.T) {
	// An empty or foreign model fails the binding before any forward, whatever
	// the engine would have said.
	for _, model := range []string{"", "endpoint-b"} {
		rr, calls, _ := coldDedicated(t, http.StatusNotFound, `{"message":"Model not found"}`, model)
		if rr.Code != http.StatusForbidden {
			t.Fatalf("model %q: status = %d, want 403", model, rr.Code)
		}
		if calls != 0 {
			t.Fatalf("model %q reached the engine %d times", model, calls)
		}
	}
}
