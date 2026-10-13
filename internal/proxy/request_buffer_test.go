package proxy

import (
	"context"
	"encoding/json"
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
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") != "1" {
		t.Fatalf("status=%d retry-after=%q, want 503 with Retry-After 1", rr.Code, rr.Header().Get("Retry-After"))
	}
	// Phoebe's retryable 503 shape (writeRetryLater): the gateway-minted
	// request id (never the client's), and a JSON {"error": ...} body.
	if got := rr.Header().Get("X-Request-Id"); got == "" || got == "saturn-test-request-id" {
		t.Fatalf("X-Request-Id=%q, want the gateway-assigned attempt id", got)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type=%q, want application/json", ct)
	}
	var refusal map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &refusal); err != nil || refusal["error"] == "" {
		t.Fatalf("body=%q err=%v, want a JSON error field", rr.Body.String(), err)
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

// Q7/D44: the fixed "metering anomaly" marker counts only a successful (2xx)
// non-streamed body whose retained head and tail missed part of the body and
// held no usage. It stays silent for a body with usage in its tail, an error
// body (which never carries usage), a client abort, and a body small enough
// for head and tail to cover it whole even when replica-total pressure forced
// the overflow.
func TestOverflowedBodyAnomalyLogOnlyForUnmeteredSuccess(t *testing.T) {
	const anomaly = "metering anomaly: overflowed non-streamed response had no usage"
	big := strings.Repeat("x", 1<<20)
	usage := `"usage":{"prompt_tokens":3,"completion_tokens":5,"total_tokens":8}`
	perStream := config.ProtectionSettings{MaxResponseBufferBytesPerStream: 64 << 10}
	// A replica total this small refuses the first reserve, so even a tiny
	// body switches to head-and-tail mode.
	tinyTotal := config.ProtectionSettings{MaxResponseBufferBytesPerStream: 16, MaxResponseBufferBytesTotal: 16}
	for _, tc := range []struct {
		name      string
		prot      config.ProtectionSettings
		status    int
		body      string
		abort     bool
		wantWarn  bool
		wantUsage bool
	}{
		{name: "200 without usage", prot: perStream, status: 200,
			body: `{"model":"m","data":"` + big + `","note":"no usage here"}`, wantWarn: true},
		{name: "200 with usage in the tail", prot: perStream, status: 200,
			body: `{"model":"m","data":"` + big + `",` + usage + `}`, wantUsage: true},
		{name: "200 with usage only in the middle", prot: perStream, status: 200,
			body: `{"model":"m","a":"` + big[:600<<10] + `",` + usage + `,"b":"` + big[:600<<10] + `"}`, wantWarn: true},
		{name: "502 error body without usage", prot: perStream, status: http.StatusBadGateway,
			body: `{"error":"` + big + `"}`},
		{name: "client aborts mid-body", prot: perStream, status: 200,
			body: `{"model":"m","data":"` + big + `"`, abort: true},
		{name: "small 200 overflowed by the replica total", prot: tinyTotal, status: 200,
			body: `{"model":"m","choices":[],"note":"no usage, but small"}`},
		{name: "small 500 overflowed by the replica total", prot: tinyTotal, status: http.StatusInternalServerError,
			body: `{"error":"engine failed"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			overflowed := make(chan struct{})
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				if !tc.abort {
					_, _ = io.WriteString(w, tc.body)
					return
				}
				// Past the 64 KiB allowance and past what head and tail
				// can cover, then hold the rest until the
				// client goes away.
				_, _ = io.WriteString(w, tc.body[:768<<10])
				w.(http.Flusher).Flush()
				close(overflowed)
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
				}
			}))
			defer backend.Close()
			up, _ := url.Parse(backend.URL)
			var warnings lockedBuffer
			log := logging.New(logging.WARN)
			log.Warn.SetOutput(&warnings)
			em := &recordingEmitter{}
			s := New(&config.Settings{Protection: tc.prot}, log, em)

			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
			setUpstream(req, up)
			req.Header.Set(identity.HeaderAuthID, "auth-1")
			req.Header.Set(identity.HeaderResourceID, "r1")
			if tc.abort {
				ctx, cancel := context.WithCancel(req.Context())
				defer cancel()
				req = req.WithContext(ctx)
				go func() {
					<-overflowed
					time.Sleep(50 * time.Millisecond)
					cancel()
				}()
			}
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, req)
			if !tc.abort && rr.Body.Len() != len(tc.body) {
				t.Fatalf("body len=%d, want the whole response (%d)", rr.Body.Len(), len(tc.body))
			}
			events := em.waitForEvents(1, 5*time.Second)
			if len(events) != 1 {
				t.Fatalf("events=%d, want one", len(events))
			}
			if events[0].UsageFound != tc.wantUsage || events[0].Aborted != tc.abort {
				t.Fatalf("event UsageFound=%v Aborted=%v, want %v %v", events[0].UsageFound, events[0].Aborted, tc.wantUsage, tc.abort)
			}
			if got := strings.Contains(warnings.String(), anomaly); got != tc.wantWarn {
				t.Fatalf("anomaly logged=%v, want %v; warnings=%q", got, tc.wantWarn, warnings.String())
			}
		})
	}
}

// D43: a full request-buffer budget is routine load shedding. Refusals are
// logged at WARN through a sampler (an onset line, then 1 in
// admissionErrorLogEvery), never at ERROR, so a saturated replica does not
// flood its error log.
func TestRequestBufferFullRefusalsLogSampledWarnNotError(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("a refused request reached the engine")
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	var warnings, errorsOut lockedBuffer
	log := logging.New(logging.WARN)
	log.Warn.SetOutput(&warnings)
	log.Error.SetOutput(&errorsOut)
	s := New(&config.Settings{Protection: config.ProtectionSettings{
		MaxRequestBodyBytes: 1024, MaxRequestBufferBytesTotal: 1024,
	}}, log, &recordingEmitter{})
	s.requestBuffers.held.Store(1000)

	const n = 20
	for i := 0; i < n; i++ {
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, dedicatedBoundRequest(up, bodyOfSize(200)))
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("request %d: status=%d, want 503", i, rr.Code)
		}
	}
	if got := errorsOut.String(); got != "" {
		t.Fatalf("budget-full refusals logged at ERROR: %q", got)
	}
	lines := strings.Count(warnings.String(), "replica request-buffer budget full; refused 503")
	if lines < 1 || lines >= n {
		t.Fatalf("WARN refusal lines=%d for %d refusals, want an onset line and sampling below one per request", lines, n)
	}
}

// The request-buffer budget charges each received byte once, so a buffered
// body must not be copied again on every later full read: later reads share
// its one buffer.
func TestReadAndRestoreBodyReusesTheBufferedBody(t *testing.T) {
	var budget requestBufferBudget
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Body = budget.wrap(io.NopCloser(strings.NewReader(`{"model":"m"}`)), 1<<20)
	a, err := readAndRestoreBody(req)
	if err != nil || len(a) == 0 {
		t.Fatalf("first read: %q err=%v", a, err)
	}
	b, err := readAndRestoreBody(req)
	if err != nil || string(b) != string(a) {
		t.Fatalf("second read: %q err=%v, want %q", b, err, a)
	}
	if &a[0] != &b[0] {
		t.Fatal("second read copied the buffered body, want the same backing array")
	}
	// The restored body still forwards every byte, and GetBody replays it.
	if fwd, _ := io.ReadAll(req.Body); string(fwd) != string(a) {
		t.Fatalf("forwarded %q, want %q", fwd, a)
	}
	if rc, _ := req.GetBody(); rc != nil {
		if replay, _ := io.ReadAll(rc); string(replay) != string(a) {
			t.Fatalf("GetBody replayed %q, want %q", replay, a)
		}
	}
	// A partly read body is not reused: the caller gets only what is left.
	replaceRequestBody(req, a)
	one := make([]byte, 1)
	_, _ = req.Body.Read(one)
	rest, err := readAndRestoreBody(req)
	if err != nil || string(rest) != string(a[1:]) {
		t.Fatalf("after a partial read got %q err=%v, want %q", rest, err, a[1:])
	}
	if got := budget.held.Load(); got != int64(len(a)) {
		t.Fatalf("budget held=%d, want each received byte charged once (%d)", got, len(a))
	}
}
