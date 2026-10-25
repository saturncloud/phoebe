package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

// Dynamo v1.6.0's no-backend 503 (error.rs, ErrorClass::Unavailable), as a
// GB300 endpoint answered right after its engine was killed (demo-qa,
// 2026-10-24, build 29; /home/hugo/work/gb300-qa/phoebe503/run.log).
const dynamoWorkerGone503 = `{"message":"Service temporarily unavailable","type":"Service Unavailable","code":503}`

type wakeRouteResult struct {
	rr       *httptest.ResponseRecorder
	upstream int32
	wakes    int32
	em       *recordingEmitter
}

// wakeRoute drives one POST through a wakeable route (shared, or dedicated
// disaggregated when profile is set) whose upstream answers the given
// responses in order, then 200 once they run out.
func wakeRoute(t *testing.T, mode, profile, requestModel string, responses ...[2]string) wakeRouteResult {
	t.Helper()
	var calls int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := int(atomic.AddInt32(&calls, 1))
		if n > len(responses) {
			_, _ = w.Write([]byte(`{"served":true}`))
			return
		}
		status := http.StatusNotFound
		if responses[n-1][0] == "503" {
			status = http.StatusServiceUnavailable
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(responses[n-1][1]))
	}))
	t.Cleanup(backend.Close)
	upstream, _ := url.Parse(backend.URL)
	waker := &fakeWaker{}
	em := &recordingEmitter{}
	srv := New(&config.Settings{}, logging.New(logging.ERROR), em).WithWaker(waker, time.Second)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"`+requestModel+`","messages":[]}`))
	setUpstream(req, upstream)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	req.Header.Set(identity.HeaderServedModel, "m")
	req.Header.Set(identity.HeaderServingMode, mode)
	if profile != "" {
		req.Header.Set(identity.HeaderServingProfile, profile)
	}
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	return wakeRouteResult{rr: rr, upstream: atomic.LoadInt32(&calls), wakes: atomic.LoadInt32(&waker.calls), em: em}
}

func TestWakeRouteWorkerGone503IsTheRetryable503WithoutAWake(t *testing.T) {
	for _, route := range []struct{ mode, profile string }{
		{identity.ServingModeShared, ""},
		{identity.ServingModeDedicated, "disaggregated"},
	} {
		got := wakeRoute(t, route.mode, route.profile, "m", [2]string{"503", dynamoWorkerGone503})

		assertRetryLater(t, got.rr)
		if got.wakes != 0 || got.upstream != 1 {
			t.Fatalf("%s %s: wakes=%d upstream=%d, want 0 and 1 (a worker exists; no wake, no retry)",
				route.mode, route.profile, got.wakes, got.upstream)
		}
		for _, e := range got.em.waitForEvents(1, time.Second) {
			if e.PromptTokens != 0 || e.CompletionTokens != 0 {
				t.Fatalf("a never-served attempt was billed tokens: %+v", e)
			}
		}
	}
}

func TestWakeRouteDrainingFrontend503IsTheRetryable503(t *testing.T) {
	got := wakeRoute(t, identity.ServingModeShared, "", "m",
		[2]string{"503", `{"message":"Service is not ready","type":"Service Unavailable","code":503}`})

	assertRetryLater(t, got.rr)
	if got.wakes != 0 {
		t.Fatalf("wakes=%d, want 0", got.wakes)
	}
}

func TestWakeRouteWorkerGone503AfterTheWakeIsTheRetryable503(t *testing.T) {
	got := wakeRoute(t, identity.ServingModeShared, "", "m",
		[2]string{"404", `{"message":"Model not found","type":"Not Found","code":404}`},
		[2]string{"503", dynamoWorkerGone503})

	assertRetryLater(t, got.rr)
	if got.wakes != 1 || got.upstream != 2 {
		t.Fatalf("wakes=%d upstream=%d, want 1 and 2", got.wakes, got.upstream)
	}
}

func TestWakeRouteDynamoOverload503IsPassedThrough(t *testing.T) {
	body := `{"message":"Service temporarily overloaded","type":"Service Unavailable","code":503}`
	got := wakeRoute(t, identity.ServingModeShared, "", "m", [2]string{"503", body})

	if got.rr.Code != http.StatusServiceUnavailable || got.rr.Body.String() != body || got.rr.Header().Get("Retry-After") != "" {
		t.Fatalf("status %d body %q, want Dynamo's overload 503 unchanged", got.rr.Code, got.rr.Body.String())
	}
	if got.wakes != 0 {
		t.Fatalf("an overload 503 invoked the waker %d times", got.wakes)
	}
}

func TestWakeRouteOversizedNoBackend503IsPassedThrough(t *testing.T) {
	body := `{"message":"Service temporarily unavailable","pad":"` + strings.Repeat("x", coldCandidatePrefixLimit) + `"}`
	got := wakeRoute(t, identity.ServingModeShared, "", "m", [2]string{"503", body})

	if got.rr.Code != http.StatusServiceUnavailable || got.rr.Body.String() != body {
		t.Fatalf("status %d (body %d bytes), want the upstream 503 unchanged", got.rr.Code, got.rr.Body.Len())
	}
}

func TestWakeRouteWrongModelStillGets403(t *testing.T) {
	got := wakeRoute(t, identity.ServingModeShared, "", "other", [2]string{"503", dynamoWorkerGone503})

	if got.rr.Code != http.StatusForbidden || got.upstream != 0 {
		t.Fatalf("status=%d upstream=%d, want 403 and 0", got.rr.Code, got.upstream)
	}
}
