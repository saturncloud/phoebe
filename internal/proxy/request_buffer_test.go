package proxy

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

func TestBudgetedBodyChargesAndReleases(t *testing.T) {
	var budget requestBufferBudget
	a := budget.wrap(io.NopCloser(strings.NewReader(strings.Repeat("a", 60))), 100)
	if b, err := io.ReadAll(a); err != nil || len(b) != 60 {
		t.Fatalf("read %d err=%v", len(b), err)
	}
	if got := budget.held.Load(); got != 60 {
		t.Fatalf("held=%d, want 60 while the request runs", got)
	}
	b := budget.wrap(io.NopCloser(strings.NewReader(strings.Repeat("b", 41))), 100)
	if _, err := io.ReadAll(b); !errors.Is(err, errRequestBufferFull) {
		t.Fatalf("err=%v, want errRequestBufferFull past the total", err)
	}
	a.release()
	a.release()
	b.release()
	if got := budget.held.Load(); got != 0 {
		t.Fatalf("held=%d after release, want 0", got)
	}
}

// Q6: with the replica's request-buffer budget full, a request is refused 503
// with Retry-After before it reaches the engine; once the holder finishes, a
// request is served again.
func TestRequestBufferBudgetFullIsRetryable503(t *testing.T) {
	var upstreamHits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits++
		_, _ = io.ReadAll(r.Body)
		_, _ = io.WriteString(w, `{"model":"m","usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	s := New(&config.Settings{Protection: config.ProtectionSettings{
		MaxRequestBodyBytes: 1024, MaxRequestBufferBytesTotal: 1024,
	}}, logging.New(logging.ERROR), &recordingEmitter{})

	// Another request holds most of the budget.
	s.requestBuffers.held.Store(1000)
	req := dedicatedBoundRequest(up, bodyOfSize(200))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") == "" {
		t.Fatalf("status=%d retry-after=%q, want 503 with Retry-After", rr.Code, rr.Header().Get("Retry-After"))
	}
	if upstreamHits != 0 {
		t.Fatal("a refused request reached the engine")
	}
	if got := s.requestBuffers.held.Load(); got != 1000 {
		t.Fatalf("held=%d after the refusal, want the other request's 1000 only", got)
	}

	s.requestBuffers.held.Store(0)
	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, dedicatedBoundRequest(up, bodyOfSize(200)))
	if rr.Code != http.StatusOK || upstreamHits != 1 {
		t.Fatalf("status=%d hits=%d, want served once the budget has room", rr.Code, upstreamHits)
	}
	if got := s.requestBuffers.held.Load(); got != 0 {
		t.Fatalf("held=%d after the request finished, want 0", got)
	}
}

// Q7: an overflowed non-streamed body with no usage in its tail is an
// unmetered attempt, logged with a fixed marker so it can be counted.
func TestOverflowedBodyWithoutUsageLogsAnomaly(t *testing.T) {
	full := `{"model":"m","data":"` + strings.Repeat("x", 1<<20) + `","note":"no usage here"}`
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, full)
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	var warnings lockedBuffer
	log := logging.New(logging.WARN)
	log.Warn.SetOutput(&warnings)
	em := &recordingEmitter{}
	s := New(&config.Settings{Protection: config.ProtectionSettings{MaxResponseBufferBytesPerStream: 64 << 10}}, log, em)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	setUpstream(req, up)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Body.Len() != len(full) {
		t.Fatalf("body len=%d, want the whole response", rr.Body.Len())
	}
	if events := em.waitForEvents(1, 5*time.Second); len(events) != 1 || events[0].UsageFound {
		t.Fatalf("events=%+v, want one unmetered attempt", events)
	}
	if !strings.Contains(warnings.String(), "metering anomaly: overflowed non-streamed response had no usage") {
		t.Fatalf("anomaly not logged; warnings=%q", warnings.String())
	}
}
