package proxy

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/logging"
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

// splitGraphCapacity publishes a scaled capacity (what admission enforces)
// and a separate per-request (one worker's) capacity, as the registry does
// for a per-worker entry with several ready workers.
type splitGraphCapacity struct{ scaled, perRequest staticGraphCapacity }

func (c splitGraphCapacity) GraphCapacity(graph string) gateway.GraphCapacity { return c.scaled[graph] }

func (c splitGraphCapacity) GraphRequestCapacity(graph string) gateway.GraphCapacity {
	return c.perRequest[graph]
}

// perWorkerTestServer serves the recording backend behind a server whose
// graph publishes per-worker KV 3000 and prefill 25, scaled to scaledKV and
// scaledPrefill, with admission enabled only by the published capacity.
func perWorkerTestServer(t *testing.T, cfg config.AdmissionSettings, scaledKV, scaledPrefill int64) (*Server, *oslRecordingBackend, *url.URL) {
	t.Helper()
	be := &oslRecordingBackend{}
	srv := httptest.NewServer(be)
	t.Cleanup(srv.Close)
	up, _ := url.Parse(srv.URL)
	s, _ := kvTestServerWithStore(t, cfg)
	graph := graphFromUpstreamHost(up.Host)
	s.graphCapacity = splitGraphCapacity{
		scaled: staticGraphCapacity{graph: {
			MaxReservedKVTokens: testutil.Ptr(scaledKV),
			MaxPrefillTokens:    testutil.Ptr(scaledPrefill),
		}},
		perRequest: staticGraphCapacity{graph: {
			MaxReservedKVTokens: testutil.Ptr[int64](3000),
			MaxPrefillTokens:    testutil.Ptr[int64](25),
		}},
	}
	return s, be, up
}

// assertPerWorker413 checks the per-worker KV refusal: 413 without
// Retry-After, the billing-correlation request id echoed, the model KV-cache
// wording, and nothing sent to the engine.
func assertPerWorker413(t *testing.T, rr *httptest.ResponseRecorder, be *oslRecordingBackend) {
	t.Helper()
	if rr.Code != http.StatusRequestEntityTooLarge || rr.Header().Get("Retry-After") != "" {
		t.Fatalf("status=%d retry-after=%q body=%q, want 413 without Retry-After", rr.Code, rr.Header().Get("Retry-After"), rr.Body.String())
	}
	if rr.Header().Get(requestIDHeader) == "" {
		t.Fatalf("413 without the %s header", requestIDHeader)
	}
	if !strings.Contains(rr.Body.String(), "this model's KV-cache capacity") {
		t.Fatalf("body=%q, want the model KV-cache wording", rr.Body.String())
	}
	if hits, _ := be.snapshot(); hits != 0 {
		t.Fatalf("upstream hits=%d, a 413 must not reach the engine", hits)
	}
}

// Q24 end to end: with Atlas publishing per-worker KV 3000, a declared
// request needing 5000 KV is a permanent 413 however many workers the graph
// is scaled to (a scaled KV of one worker, four workers, or a saturated
// multiplier): one worker can never hold it.
func TestPerWorkerKVOverflowIs413WithoutRetryAfterOrUpstreamHit(t *testing.T) {
	for _, c := range []struct {
		name     string
		scaledKV int64
	}{
		{"one worker", 3000},
		{"four workers", 12000},
		{"saturated multiplier", math.MaxInt64},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := kvTestConfig()
			cfg.Enabled = false
			s, be, up := perWorkerTestServer(t, cfg, c.scaledKV, 1<<40)
			assertPerWorker413(t, serveChat(s, up, chatBody(4*1000, 4000)), be) // 1000 prompt + 4000 output = 5000 KV
		})
	}
}

// The per-worker 413 runs before admission's shared counters, so it does not
// depend on whether an earlier scope is full: with the platform's only
// active slot taken, the oversized request is still the 413 (not a retryable
// 503), while a small request in the same state is the retryable 503,
// proving the scope really is full.
func TestPerWorkerKV413WinsWhenEarlierScopeFull(t *testing.T) {
	cfg := kvTestConfig()
	cfg.Enabled = false
	cfg.Platform = config.AdmissionLimits{MaxActiveRequests: testutil.Ptr[int64](1)}
	s, be, up := perWorkerTestServer(t, cfg, 12000, 1<<40)
	ctx := context.Background()
	holder, err := s.admitter.Admit(ctx, admission.Request{Graph: "holder", Organization: "org-h", Owner: "u", Model: "m",
		PrefillTokens: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Complete(ctx, 0) }()

	assertPerWorker413(t, serveChat(s, up, chatBody(4*1000, 4000)), be)

	rr := serveChat(s, up, chatBody(4*10, 10))
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") == "" {
		t.Fatalf("control: status=%d retry-after=%q, want the retryable 503 of a full platform scope", rr.Code, rr.Header().Get("Retry-After"))
	}
	if hits, _ := be.snapshot(); hits != 0 {
		t.Fatalf("upstream hits=%d, want none while the platform scope is full", hits)
	}
}

// The 413 wording through the full handler names the sequence count when n
// pushes the reservation over one worker, and names max_tokens only when the
// client declared it.
func TestPerWorkerKV413WordingNamesSequencesForNAboveOne(t *testing.T) {
	prompt := strings.Repeat("x", 4*2990)
	for _, c := range []struct {
		name, body, want, mustNot string
	}{
		{"undeclared max_tokens, n=16", `{"model":"model-a","n":16,"messages":[{"role":"user","content":"` + prompt + `"}]}`,
			"plus one output token per requested sequence", "max_tokens"},
		{"declared max_tokens, n=4", `{"model":"model-a","n":4,"max_tokens":800,"messages":[{"role":"user","content":"x"}]}`,
			"plus max_tokens times the number of output sequences", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := kvTestConfig()
			cfg.Enabled = false
			s, be, up := perWorkerTestServer(t, cfg, 12000, 1<<40)
			rr := serveChat(s, up, []byte(c.body))
			assertPerWorker413(t, rr, be)
			if b := rr.Body.String(); !strings.Contains(b, c.want) || (c.mustNot != "" && strings.Contains(b, c.mustNot)) {
				t.Fatalf("body=%q, want %q and not %q", b, c.want, c.mustNot)
			}
		})
	}
}

// Q24: a published prefill capacity is a load limit. A prompt larger than
// the scaled prefill budget (1000 > 100) that fits one worker's KV is
// admitted when no other request is prefilling on the graph; while another
// request holds prefill tokens on that graph the same prompt is refused as
// retryable (Retry-After), never 413 and never sent to the engine.
func TestLoneOversizedPromptIsAdmittedUnderScaledPrefillLimit(t *testing.T) {
	t.Run("no other request prefilling: admitted", func(t *testing.T) {
		cfg := kvTestConfig()
		cfg.Enabled = false
		s, be, up := perWorkerTestServer(t, cfg, 12000, 100)
		if rr := serveChat(s, up, chatBody(4*1000, 100)); rr.Code != http.StatusOK {
			t.Fatalf("status=%d body=%q, want the lone oversized prompt admitted", rr.Code, rr.Body.String())
		}
		if hits, _ := be.snapshot(); hits != 1 {
			t.Fatalf("upstream hits=%d, want 1", hits)
		}
	})
	t.Run("another request prefilling: retryable", func(t *testing.T) {
		cfg := kvTestConfig()
		cfg.Enabled = false
		s, be, up := perWorkerTestServer(t, cfg, 12000, 100)
		ctx := context.Background()
		holder, err := s.admitter.Admit(ctx, admission.Request{Graph: graphFromUpstreamHost(up.Host), Organization: "org-h", Owner: "u", Model: "m",
			PrefillTokens: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
			GraphCapacity: s.graphCapacity.GraphCapacity(graphFromUpstreamHost(up.Host)).Limits()})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Complete(ctx, 0) }()
		rr := serveChat(s, up, chatBody(4*1000, 100))
		if (rr.Code != http.StatusServiceUnavailable && rr.Code != http.StatusTooManyRequests) || rr.Header().Get("Retry-After") == "" {
			t.Fatalf("status=%d retry-after=%q body=%q, want a retryable 503/429 with Retry-After", rr.Code, rr.Header().Get("Retry-After"), rr.Body.String())
		}
		if hits, _ := be.snapshot(); hits != 0 {
			t.Fatalf("upstream hits=%d, a refused request must not reach the engine", hits)
		}
	})
}

type pairCapacity struct{ scaled, perRequest gateway.GraphCapacity }

func (p pairCapacity) GraphCapacities(string) (gateway.GraphCapacity, gateway.GraphCapacity) {
	return p.scaled, p.perRequest
}

// publishedGraphCapacities takes both values from the one-read pair source
// when it is wired (so a request never mixes two registry states), falls
// back to the capacity source's two reads otherwise, and returns nothing for
// an empty graph or no source.
func TestPublishedGraphCapacitiesPrefersTheOneReadPair(t *testing.T) {
	s := &Server{}
	if scaled, perRequest := s.publishedGraphCapacities("g"); scaled.Any() || perRequest.Any() {
		t.Fatalf("no source: got (%+v, %+v), want no limits", scaled, perRequest)
	}
	s.graphCapacity = splitGraphCapacity{scaled: staticGraphCapacity{"g": kvCap(12000)}, perRequest: staticGraphCapacity{"g": kvCap(3000)}}
	scaled, perRequest := s.publishedGraphCapacities("g")
	if !reflect.DeepEqual(scaled, kvCap(12000)) || !reflect.DeepEqual(perRequest, kvCap(3000)) {
		t.Fatalf("capacity source: got (%+v, %+v), want (12000, 3000)", scaled, perRequest)
	}
	s.graphCapacityPair = pairCapacity{scaled: kvCap(8000), perRequest: kvCap(2000)}
	scaled, perRequest = s.publishedGraphCapacities("g")
	if !reflect.DeepEqual(scaled, kvCap(8000)) || !reflect.DeepEqual(perRequest, kvCap(2000)) {
		t.Fatalf("pair source: got (%+v, %+v), want the pair's (8000, 2000)", scaled, perRequest)
	}
	scaled, perRequest = s.publishedGraphCapacities("")
	if scaled.Any() || perRequest.Any() {
		t.Fatalf("empty graph: got (%+v, %+v), want no limits", scaled, perRequest)
	}
}

// shrinkingGraphCapacity reports a large scaled capacity on its first read
// and a small one on every later read, as when the graph scales down while a
// client is still uploading its body.
type shrinkingGraphCapacity struct {
	reads              int
	first, later, unit gateway.GraphCapacity
}

func (c *shrinkingGraphCapacity) GraphCapacity(string) gateway.GraphCapacity {
	c.reads++
	if c.reads == 1 {
		return c.first
	}
	return c.later
}

func (c *shrinkingGraphCapacity) GraphRequestCapacity(string) gateway.GraphCapacity { return c.unit }

// D54/D59: the scaled graph capacity Admit enforces is read after the body,
// so a ready-worker count that changed during a slow upload is reflected:
// the admitter receives the later, smaller limit, not the snapshot taken
// before the body read.
func TestAdmitUsesGraphCapacityReadAfterTheBody(t *testing.T) {
	be := &oslRecordingBackend{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)
	fake := &recordingAdmitter{}
	s := New(&config.Settings{Admission: config.AdmissionSettings{DefaultMaxOutputTokens: 4096}}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithAdmitter(fake)
	s.graphCapacity = &shrinkingGraphCapacity{first: kvCap(90000), later: kvCap(20000), unit: kvCap(10000)}
	if rr := serveChat(s, up, chatBody(4*100, 50)); rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q, want 200", rr.Code, rr.Body.String())
	}
	if fake.calls != 1 {
		t.Fatalf("Admit calls=%d, want 1", fake.calls)
	}
	if got := fake.last.GraphCapacity.MaxReservedKVTokens; got == nil || *got != 20000 {
		t.Fatalf("Admit GraphCapacity.MaxReservedKVTokens=%v, want the post-body 20000", got)
	}
}

// The proxy's per-worker check and admission's reserved-KV charge are one
// formula (prompt + output x sequences; scripts.go q.kv) over one estimate:
// the request the proxy sends to Admit carries exactly the estimate
// graphFitRejection judged, and for KV at a cap and one token above it the
// proxy refuses exactly the request whose KV, as the script records it,
// exceeds the cap.
func TestGraphFitRejectionMatchesScriptReservedKV(t *testing.T) {
	t.Run("the proxy sends Admit the estimate it checked", func(t *testing.T) {
		be := &oslRecordingBackend{}
		srv := httptest.NewServer(be)
		defer srv.Close()
		up, _ := url.Parse(srv.URL)
		fake := &recordingAdmitter{}
		s := New(&config.Settings{Admission: config.AdmissionSettings{DefaultMaxOutputTokens: 4096}}, logging.New(logging.ERROR), &recordingEmitter{}).
			WithAdmitter(fake)
		s.graphCapacity = staticGraphCapacity{graphFromUpstreamHost(up.Host): kvCap(1 << 30)}
		body := []byte(`{"model":"model-a","n":3,"max_tokens":70,"messages":[{"role":"user","content":"` + strings.Repeat("x", 400) + `"}]}`)
		est, ok := admissionWork(body, 4096, "/v1/chat/completions")
		if !ok {
			t.Fatal("admissionWork refused the body")
		}
		if rr := serveChat(s, up, body); rr.Code != http.StatusOK {
			t.Fatalf("status=%d body=%q, want 200", rr.Code, rr.Body.String())
		}
		got := fake.last
		if got.EstimatedInputTokens != est.InputTokens || got.PrefillTokens != est.InputTokens ||
			got.ReservedOutputTokens != est.OutputTokens || got.OutputSequences != est.OutputSequences {
			t.Fatalf("Admit got input=%d prefill=%d output=%d sequences=%d, want the checked estimate %+v",
				got.EstimatedInputTokens, got.PrefillTokens, got.ReservedOutputTokens, got.OutputSequences, est)
		}
	})
	const limit = 1000
	for _, c := range []struct {
		name string
		est  admissionEstimate
	}{
		{"KV exactly at the cap", admissionEstimate{InputTokens: 200, OutputTokens: 200, OutputSequences: 4}},
		{"KV one token above the cap", admissionEstimate{InputTokens: 201, OutputTokens: 200, OutputSequences: 4}},
	} {
		t.Run(c.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			cfg := config.AdmissionSettings{Enabled: true, KeyPrefix: "fit-vs-script", LeaseTTL: time.Minute}
			ctx := context.Background()
			lease, err := admission.New(client, cfg).Admit(ctx, admission.Request{Graph: "g", Organization: "o", Owner: "u", Model: "m",
				PrefillTokens: c.est.InputTokens, EstimatedInputTokens: c.est.InputTokens,
				ReservedOutputTokens: c.est.OutputTokens, OutputSequences: c.est.OutputSequences,
				GraphCapacity: kvCap(1 << 30).Limits()})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lease.Complete(ctx, 0) }()
			key := "{" + cfg.KeyPrefix + "}:counters"
			fields, err := mr.HKeys(key)
			if err != nil {
				t.Fatal(err)
			}
			var recorded int64 = -1
			for _, f := range fields {
				if !strings.HasSuffix(f, "|kv") {
					continue
				}
				v, err := strconv.ParseInt(mr.HGet(key, f), 10, 64)
				if err != nil {
					t.Fatalf("counter %s: %v", f, err)
				}
				if recorded >= 0 && v != recorded {
					t.Fatalf("scopes record different KV charges: %d and %d", recorded, v)
				}
				recorded = v
			}
			if recorded < 0 {
				t.Fatalf("no kv counter recorded in %v", fields)
			}
			refused := graphFitRejection(kvCap(limit), c.est) != nil
			if refused != (recorded > limit) {
				t.Fatalf("script recorded KV %d against cap %d, proxy refused=%v; they must agree", recorded, limit, refused)
			}
		})
	}
}
