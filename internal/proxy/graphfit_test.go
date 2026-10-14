package proxy

import (
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/testutil"
)

func kvCap(kv int64) gateway.GraphCapacity {
	var c gateway.GraphCapacity
	if kv >= 0 {
		c.MaxReservedKVTokens = &kv
	}
	return c
}

// Q24: the only "can never fit" check is one request's KV (prompt + output x
// sequences) against one worker's KV value; nothing else is refused here.
func TestGraphFitRejection(t *testing.T) {
	perWorker := kvCap(1000)
	cases := []struct {
		name string
		est  admissionEstimate
		want bool
	}{
		{"fits one worker", admissionEstimate{InputTokens: 300, OutputTokens: 500, OutputSequences: 1}, false},
		{"exactly one worker", admissionEstimate{InputTokens: 500, OutputTokens: 500, OutputSequences: 1}, false},
		{"over one worker", admissionEstimate{InputTokens: 300, OutputTokens: 800, OutputSequences: 1}, true},
		{"every output sequence counts", admissionEstimate{InputTokens: 100, OutputTokens: 300, OutputSequences: 4}, true},
		{"a huge prompt alone is over one worker", admissionEstimate{InputTokens: 5000}, true},
		{"overflow saturates instead of wrapping", admissionEstimate{InputTokens: 10, OutputTokens: math.MaxInt64 / 2, OutputSequences: 4}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := graphFitRejection(perWorker, tc.est)
			if (got != nil) != tc.want {
				t.Fatalf("got %+v, want rejected=%v", got, tc.want)
			}
			if got != nil && (got.Dimension != "kv_exceeds_limit" || !got.Unsatisfiable || got.Contractual) {
				t.Fatalf("a rejection must be the per-worker KV 413, got %+v", got)
			}
		})
	}
	if got := graphFitRejection(kvCap(-1), admissionEstimate{InputTokens: 1 << 40}); got != nil {
		t.Fatalf("no KV cap: got %+v", got)
	}
	if got := graphFitRejection(kvCap(0), admissionEstimate{InputTokens: 5}); got != nil {
		t.Fatalf("an explicit zero cap is admission's retryable refusal, got %+v", got)
	}
}

type perRequestCapacity map[string]gateway.GraphCapacity

func (p perRequestCapacity) GraphRequestCapacity(graph string) gateway.GraphCapacity { return p[graph] }

// End to end (Q24): with Atlas publishing per-worker KV 3000 and the graph
// scaled to 4 workers (enforced KV 12000), a declared request needing 5000 KV
// is a permanent 413 (one worker cannot hold it), while a prompt larger than
// the scaled prefill budget is admitted when nothing else is in flight.
func TestPerWorkerKVIs413AtAnyGraphSize(t *testing.T) {
	be := &oslRecordingBackend{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)
	cfg := kvTestConfig()
	cfg.Enabled = false
	s := kvTestServer(t, cfg)
	graph := graphFromUpstreamHost(up.Host)
	s.graphCapacity = staticGraphCapacity{graph: {
		MaxReservedKVTokens: testutil.Ptr[int64](12000),
		MaxPrefillTokens:    testutil.Ptr[int64](100),
	}}
	s.graphRequestCapacity = perRequestCapacity{graph: {
		MaxReservedKVTokens: testutil.Ptr[int64](3000),
		MaxPrefillTokens:    testutil.Ptr[int64](25),
	}}

	rr := serveChat(s, up, chatBody(4*1000, 4000)) // 1000 prompt + 4000 output = 5000 KV
	if rr.Code != http.StatusRequestEntityTooLarge || rr.Header().Get("Retry-After") != "" {
		t.Fatalf("status=%d retry-after=%q, want 413 without Retry-After", rr.Code, rr.Header().Get("Retry-After"))
	}
	if !strings.Contains(rr.Body.String(), "this model's KV-cache capacity") {
		t.Fatalf("body=%q, want the model KV-cache wording", rr.Body.String())
	}
	// 1000 prompt tokens > the scaled prefill budget of 100, but it fits one
	// worker's KV and nothing else is in flight: admitted.
	if rr := serveChat(s, up, chatBody(4*1000, 100)); rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q, want the lone oversized prompt admitted", rr.Code, rr.Body.String())
	}
	if hits, _ := be.snapshot(); hits != 1 {
		t.Fatalf("upstream hits=%d, want only the admitted request", hits)
	}
}
