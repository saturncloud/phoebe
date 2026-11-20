package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/identity"
)

// /v1/rerank is a dedicated endpoint's API and nothing else: open on bound and
// unbound dedicated routes, closed on the shared gateway. vLLM's other
// spellings of the same APIs stay closed everywhere.
func TestRerankRouteIsOpenOnDedicatedRoutesOnly(t *testing.T) {
	allow := "my-reranker"
	for _, method := range []string{"POST", "OPTIONS"} {
		if !boundRequestAllowed(method, "/v1/rerank", allow) {
			t.Fatalf("bound %s /v1/rerank must be allowed", method)
		}
		if !unboundRequestAllowed(method, "/v1/rerank") {
			t.Fatalf("unbound %s /v1/rerank must be allowed", method)
		}
		if gatewayRequestAllowed(method, "/v1/rerank") {
			t.Fatalf("gateway %s /v1/rerank must be refused: no shared graph serves rerank", method)
		}
	}
	if !sharedTierPathAllowed("/v1/embeddings") || sharedTierPathAllowed("/v1/rerank") {
		t.Fatal("the shared tier serves embeddings and never rerank")
	}
	for _, path := range []string{"/rerank", "/v2/rerank", "/score", "/v1/score", "/v1/classify", "/classify", "/v1/pooling", "/pooling"} {
		for _, method := range []string{"POST", "OPTIONS"} {
			if boundRequestAllowed(method, path, allow) || unboundRequestAllowed(method, path) || gatewayRequestAllowed(method, path) {
				t.Fatalf("%s %s must stay closed (one customer route per API)", method, path)
			}
		}
	}
}

// A dedicated reranker's response (vLLM 0.31 RerankResponse) is forwarded
// unchanged and metered from usage.prompt_tokens, with no completion tokens:
// rerank bills input tokens only, exactly like embeddings.
func TestDedicatedRerankIsForwardedAndMeteredByPromptTokens(t *testing.T) {
	const response = `{"id":"rerank-1","model":"my-reranker","usage":{"prompt_tokens":37,"total_tokens":37},` +
		`"results":[{"index":1,"document":{"text":"Paris is the capital of France."},"relevance_score":0.98},` +
		`{"index":0,"document":{"text":"Bananas are yellow."},"relevance_score":0.01}]}`
	var gotPath string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, response)
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	srv := newTestServerE(t, upstream, em)

	body := `{"model":"my-reranker","query":"capital of France","documents":["Bananas are yellow.","Paris is the capital of France."],"top_n":2}`
	req := httptest.NewRequest(http.MethodPost, "/v1/rerank", strings.NewReader(body))
	setUpstream(req, upstream)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "deployment-r")
	req.Header.Set(identity.HeaderServedModel, "my-reranker")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK || rr.Body.String() != response {
		t.Fatalf("status=%d body=%q, want the engine's 200 body unchanged", rr.Code, rr.Body.String())
	}
	if gotPath != "/v1/rerank" {
		t.Fatalf("engine saw path %q, want /v1/rerank unchanged", gotPath)
	}
	events := em.waitForEvents(1, 5*time.Second)
	if len(events) != 1 {
		t.Fatalf("events=%+v, want exactly one metering event", events)
	}
	e := events[0]
	if !e.UsageFound || e.PromptTokens != 37 || e.CompletionTokens != 0 || e.Model != "my-reranker" || e.Aborted {
		t.Fatalf("event=%+v, want usage found, 37 prompt tokens, 0 completion tokens, model my-reranker", e)
	}
}

// A rerank request bound to the wrong model is refused like any other
// inference request: the binding applies to /v1/rerank too.
func TestDedicatedRerankEnforcesModelBinding(t *testing.T) {
	var calls int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	srv := newTestServer(t, upstream)

	req := httptest.NewRequest(http.MethodPost, "/v1/rerank",
		strings.NewReader(`{"model":"someone-else","query":"q","documents":["d"]}`))
	setUpstream(req, upstream)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "deployment-r")
	req.Header.Set(identity.HeaderServedModel, "my-reranker")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("cross-model rerank status = %d, want 403", rr.Code)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("cross-model rerank reached the engine (%d calls)", got)
	}
}

// A shared route (header-routed, with its allow-list) never forwards a rerank
// request: no shared graph can serve it, and admission must not count it as
// shared work. Same generic 404 as every other unauthorized route.
func TestSharedRouteRefusesRerank(t *testing.T) {
	var calls int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	srv := newTestServer(t, upstream)

	for _, method := range []string{http.MethodPost, http.MethodOptions} {
		req := httptest.NewRequest(method, "/v1/rerank",
			strings.NewReader(`{"model":"shared-model","query":"q","documents":["d"]}`))
		req.Header.Set(identity.HeaderServingMode, identity.ServingModeShared)
		setUpstream(req, upstream)
		req.Header.Set(identity.HeaderAuthID, "auth-1")
		req.Header.Set(identity.HeaderResourceID, "model-shared")
		req.Header.Set(identity.HeaderServedModel, "shared-model")
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("shared %s /v1/rerank status = %d, want 404", method, rr.Code)
		}
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Fatalf("shared rerank reached the graph (%d calls)", got)
	}
}
