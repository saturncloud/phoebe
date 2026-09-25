package proxy

// Settlement matrix for the admission lease (battery escalation R1 ruling):
// determinate never-served outcomes (the cold-hold capacity rejection, the
// cold response served final after the wake gave up) settle ZERO token-window
// charges while KEEPING the requests-window +1 (that window measures demand,
// not work). Genuinely indeterminate outcomes (engine 4xx/5xx after a real
// dispatch, aborts, mid-stream faults) keep the conservative estimate. These
// tests pin both directions end to end against a real admission store, plus
// the closed settlementKind classifiers that decide between them.

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/capture"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

func TestClassifyRoundTripSettlement(t *testing.T) {
	dialFailure := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	tests := []struct {
		name string
		err  error
		want settlementKind
	}{
		// Aborts and indeterminate faults keep the conservative estimate.
		{"client abort", context.Canceled, settlementUnknown},
		{"wrapped client abort", &url.Error{Op: "Post", URL: "http://up", Err: context.Canceled}, settlementUnknown},
		{"upstream reset (EOF)", io.ErrUnexpectedEOF, settlementUnknown},
		{"wrapped upstream deadline", &url.Error{Op: "Post", URL: "http://up", Err: context.DeadlineExceeded}, settlementUnknown},
		{"plain upstream error", errors.New("upstream exploded"), settlementUnknown},
		// A verified pre-write dial failure proves the request never left the
		// process, wrapped or bare.
		{"dial failure", dialFailure, settlementZeroPreWriteDial},
		{"wrapped dial failure", &url.Error{Op: "Post", URL: "http://up", Err: dialFailure}, settlementZeroPreWriteDial},
		{"nil", nil, settlementUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyRoundTripSettlement(tc.err); got != tc.want {
				t.Fatalf("classifyRoundTripSettlement(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}

func TestClassifyCaptureSettlement(t *testing.T) {
	withUsage := capture.Result{UsageFound: true}
	noUsage := capture.Result{UsageFound: false}
	abortedNoUsage := capture.Result{UsageFound: false, Aborted: true}
	tests := []struct {
		name string
		res  capture.Result
		want settlementKind
	}{
		{"usage found", withUsage, settlementActual},
		{"no usage, ordinary response", noUsage, settlementUnknown},
		// An aborted stream with no usage stays conservative: the engine may
		// have done work before the disconnect.
		{"aborted, no usage", abortedNoUsage, settlementUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyCaptureSettlement(tc.res); got != tc.want {
				t.Fatalf("classifyCaptureSettlement(%+v) = %d, want %d", tc.res, got, tc.want)
			}
		})
	}
}

// TestSettleAdmissionLeaseRejectsUnsetKind pins the enum guard: the zero-value
// kind names no settlement path, so settleAdmissionLease must fail closed at
// the switch's default branch rather than take a real settlement's lease
// calls. Callers always classify first, so this is a guard against the most
// common Go enum mistake (an uninitialized settlementKind), not a live path.
func TestSettleAdmissionLeaseRejectsUnsetKind(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	lease, err := admission.New(c, cfg).Admit(context.Background(), admission.Request{
		Graph: "graph", Organization: "org-a", Model: "m",
		PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = settleAdmissionLease(context.Background(), lease, settlementUnset, admission.Usage{})
	if err == nil {
		t.Fatal("settleAdmissionLease with the zero-value kind must error, not settle")
	}
	if !strings.Contains(err.Error(), "unhandled settlement kind") {
		t.Fatalf("settleAdmissionLease error = %v, want the default branch's fail-closed message", err)
	}
	if err := lease.Complete(context.Background(), 0); err != nil {
		t.Fatalf("lease left unsettled-but-invalid by the rejected zero kind: %v", err)
	}
}

// chargeThenProbeAdmit runs charge (the admission-affecting request section),
// then issues each probe Admit and returns the probe errors. The admission
// windows are fixed 1-minute buckets: if the bucket rolls between the charge
// and the probes, the probes land in a fresh (empty) bucket — so on a roll it
// resets the store to empty and retries the whole section once. A probe that
// admits is settled immediately (Complete(0)) so it holds no physical slot and
// leaves no window residue for the probes after it. Mirrors the rollover
// handling of the pre-existing chargeAndProbe tests.
func chargeThenProbeAdmit(t *testing.T, a *admission.RedisAdmitter, reset func(), charge func(), probes []admission.Request) []error {
	t.Helper()
	run := func() (bool, []error) {
		bucketBefore := fixedWindowBucket()
		charge()
		errs := make([]error, len(probes))
		for i, p := range probes {
			lease, err := a.Admit(context.Background(), p)
			if err == nil {
				_ = lease.Complete(context.Background(), 0)
			}
			errs[i] = err
		}
		return bucketBefore != fixedWindowBucket(), errs
	}
	rolled, errs := run()
	if rolled {
		reset()
		rolled, errs = run()
		if rolled {
			t.Skip("fixed 1-minute admission window rolled twice during the test; the probes cannot be made deterministic — retry")
		}
	}
	return errs
}

// rejectedScopeDimension unwraps an Admit error to its rejection scope and
// dimension, reporting false for non-rejection errors.
func rejectedScopeDimension(err error) (scope, dimension string, ok bool) {
	var rejected *admission.Rejected
	if !errors.As(err, &rejected) {
		return "", "", false
	}
	return rejected.Scope, rejected.Dimension, true
}

// contractProbe builds an Admit probe against the proxy request's own
// organization contract windows (org-a): the windows are per scope+dimension,
// shared by every request of that org regardless of each request's own limits.
func contractProbe(estInput, reservedOutput int64, limits admission.RateLimits) admission.Request {
	return admission.Request{
		Graph: "graph", Organization: "org-a", Model: "m",
		PromptBytes: 1, EstimatedInputTokens: estInput, ReservedOutputTokens: reservedOutput,
		OrganizationLimits: limits,
	}
}

// sharedRequestEstimate recomputes the admission estimate handleProxy derives
// from the request body, so a probe can reserve EXACTLY the rejected request's
// conservative charge and thereby detect any phantom residue in the window.
func sharedRequestEstimate(t *testing.T, req *http.Request) admissionEstimate {
	t.Helper()
	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	req.Body = io.NopCloser(strings.NewReader(string(body)))
	est, ok := admissionWork(body, 20) // 20 = proxyAdmissionConfig's DefaultMaxOutputTokens
	if !ok {
		t.Fatal("shared request body must parse for an admission estimate")
	}
	return est
}

// withContractEnvelope stamps the shared request with an organization contract
// (requests 1/min, generated 20/min, prompt 100/min) so its Admit creates the
// contract_organization scope: the rejections-under-test settle against those
// windows, and the requests probe can observe the admit-time RPM charge.
func withContractEnvelope(req *http.Request) *http.Request {
	req.Header.Set(identity.HeaderOrgRateLimitRequests, "1")
	req.Header.Set(identity.HeaderOrgRateLimitGeneratedTokens, "20")
	req.Header.Set(identity.HeaderOrgRateLimitTotalPromptTokens, "100")
	req.Header.Set(identity.HeaderOrgRateLimitUncachedPromptTokens, "100")
	return req
}

// settleZeroProbes is the shared assertion set for a determinate never-served
// outcome: token windows uncharged (generated/prompt probes reserving exactly
// the conservative estimate are ADMITTED), the requests-window +1 kept (the
// requests probe is REJECTED).
func settleZeroProbes(t *testing.T, errs []error) {
	t.Helper()
	if len(errs) != 3 {
		t.Fatalf("got %d probe errors, want 3", len(errs))
	}
	if scope, dimension, ok := rejectedScopeDimension(errs[0]); !ok || scope != "contract_organization" || dimension != "requests" {
		t.Errorf("requests probe: err=%v, want contract_organization requests rejection (the admit-time +1 must be kept)", errs[0])
	}
	if errs[1] != nil {
		t.Errorf("generated probe: err=%v, want admission (a phantom conservative charge would fill the window and reject it)", errs[1])
	}
	if errs[2] != nil {
		t.Errorf("prompt probe: err=%v, want admission (a phantom conservative charge would fill the window and reject it)", errs[2])
	}
}

func zeroSettlementProbeSet(est admissionEstimate) []admission.Request {
	return []admission.Request{
		contractProbe(1, 1, admission.RateLimits{Requests: 1}),
		contractProbe(1, 20, admission.RateLimits{GeneratedTokens: 20}),
		contractProbe(est.InputTokens, 1, admission.RateLimits{
			TotalPromptTokens: est.InputTokens, UncachedPromptTokens: est.InputTokens,
		}),
	}
}

// TestWakeColdHoldRejectionSettlesZeroNeverServed pins the cold-hold capacity
// rejection end to end: the platform cold-hold slot is occupied by a contending
// holder, the wakeable request's BeginColdHold is rejected (503 + Retry-After,
// waker never invoked — pinned by TestWakeColdHoldRejectionFailsClosedAndReleasesLease),
// and the rejection settles ZERO: token windows uncharged, the requests-window
// +1 kept, the physical platform reservation released. serveWithWake does not
// settle this path itself; the handler's deferred release fallback owns the
// lease release, and it lands on exactly the R1 never-served settlement:
// Complete(0). (The 429 contractual rendering would take the same path through
// the same fallback — no current config produces a contractual cold-holds
// scope, so it cannot be driven end to end.)
func TestWakeColdHoldRejectionSettlesZeroNeverServed(t *testing.T) {
	backend := &coldToWarmBackend{} // stays cold: every response is the cold 404
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(2)
	cfg.Platform.MaxColdHolds = 1
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	a := admission.New(c, cfg)
	waker := &fakeWaker{}
	em := &recordingEmitter{}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), em).
		WithAdmitter(a).
		WithWaker(waker, time.Second, 3)

	var holder *admission.Lease
	charge := func() {
		callsBefore := atomic.LoadInt32(&waker.calls)
		// The contending hold occupies the platform's single cold-hold slot.
		// Created inside the charge section so a window-roll retry (which
		// flushes the store) re-establishes it.
		h, err := a.Admit(context.Background(), admission.Request{
			Graph: "graph", Organization: "holder", Model: "m",
			PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := h.BeginColdHold(context.Background()); err != nil {
			t.Fatal(err)
		}
		holder = h

		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, withContractEnvelope(sharedRequest(up)))
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d, want the cold-hold rejection's 503", rr.Code)
		}
		if rr.Header().Get("Retry-After") == "" {
			t.Fatal("cold-hold rejection must carry Retry-After")
		}
		if got := atomic.LoadInt32(&waker.calls) - callsBefore; got != 0 {
			t.Fatalf("waker invoked %d times despite the rejected cold hold", got)
		}
	}
	defer func() {
		if holder != nil {
			_ = holder.EndColdHold(context.Background())
			_ = holder.Complete(context.Background(), 0)
		}
	}()

	req := sharedRequest(up)
	est := sharedRequestEstimate(t, req)
	errs := chargeThenProbeAdmit(t, a, mr.FlushAll, charge, zeroSettlementProbeSet(est))
	settleZeroProbes(t, errs)

	// Physical capacity: the rejected request's platform slot was released, so
	// a plain Admit fits alongside the holder's.
	physical, err := a.Admit(context.Background(), admission.Request{
		Graph: "graph", Organization: "org-c", Model: "m",
		PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
	})
	if err != nil {
		t.Fatalf("cold-hold rejection leaked its platform reservation: %v", err)
	}
	_ = physical.Complete(context.Background(), 0)

	// Determinate never-served: nothing is metered either.
	if n := len(em.all()); n != 0 {
		t.Fatalf("cold-hold rejection emitted %d events, want 0", n)
	}
}

// TestWakeExhaustedColdSettlesZeroNeverServed pins the wake-exhausted cold 404
// (serveWithWake's give-up after maxTries cold attempts): the engine refused
// every dispatch before any inference, so the final cold 404 settles ZERO —
// token windows uncharged, the requests-window +1 kept, the physical
// reservation released — rather than the conservative estimate a usage-less
// ordinary response would retain.
func TestWakeExhaustedColdSettlesZeroNeverServed(t *testing.T) {
	backend := &coldToWarmBackend{} // never warms
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	cfg.Platform.MaxColdHolds = 1
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	a := admission.New(c, cfg)
	waker := &fakeWaker{warmsAt: 99, backend: backend} // wake succeeds, graph stays cold
	em := &recordingEmitter{}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), em).
		WithAdmitter(a).
		WithWaker(waker, time.Second, 2)

	charge := func() {
		callsBefore := atomic.LoadInt32(&waker.calls)
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, withContractEnvelope(sharedRequest(up)))
		if rr.Code != http.StatusNotFound {
			t.Fatalf("status=%d, want the final cold 404 after wake exhaustion", rr.Code)
		}
		// serveWithWake wakes once per cold attempt: maxTries=2 means two wakes
		// (each followed by a still-cold re-probe), then the honest final cold
		// forward.
		if got := atomic.LoadInt32(&waker.calls) - callsBefore; got != 2 {
			t.Fatalf("waker delta=%d, want 2 for maxTries=2 (one wake per cold attempt)", got)
		}
	}

	req := sharedRequest(up)
	est := sharedRequestEstimate(t, req)
	errs := chargeThenProbeAdmit(t, a, mr.FlushAll, charge, zeroSettlementProbeSet(est))
	settleZeroProbes(t, errs)

	physical, err := a.Admit(context.Background(), admission.Request{
		Graph: "graph", Organization: "org-c", Model: "m",
		PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
	})
	if err != nil {
		t.Fatalf("exhausted wake leaked its platform reservation: %v", err)
	}
	_ = physical.Complete(context.Background(), 0)

	if n := len(em.all()); n != 0 {
		t.Fatalf("exhausted cold response emitted %d events, want 0", n)
	}
}

// TestWakeErrorColdSettlesZeroNeverServed pins the waker-failure give-up: the
// waker errored, the engine's cold response is final, and the same never-served
// settlement applies — zero token-window charges, the requests-window +1 kept,
// physical capacity released.
func TestWakeErrorColdSettlesZeroNeverServed(t *testing.T) {
	backend := &coldToWarmBackend{} // never warms
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	cfg.Platform.MaxColdHolds = 1
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	a := admission.New(c, cfg)
	waker := &fakeWaker{err: context.DeadlineExceeded}
	em := &recordingEmitter{}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), em).
		WithAdmitter(a).
		WithWaker(waker, time.Second, 3)

	charge := func() {
		callsBefore := atomic.LoadInt32(&waker.calls)
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, withContractEnvelope(sharedRequest(up)))
		if rr.Code != http.StatusNotFound {
			t.Fatalf("status=%d, want the final cold 404 after the waker failed", rr.Code)
		}
		if got := atomic.LoadInt32(&waker.calls) - callsBefore; got != 1 {
			t.Fatalf("waker delta=%d, want 1 (a failed wake must not retry)", got)
		}
	}

	req := sharedRequest(up)
	est := sharedRequestEstimate(t, req)
	errs := chargeThenProbeAdmit(t, a, mr.FlushAll, charge, zeroSettlementProbeSet(est))
	settleZeroProbes(t, errs)

	physical, err := a.Admit(context.Background(), admission.Request{
		Graph: "graph", Organization: "org-c", Model: "m",
		PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
	})
	if err != nil {
		t.Fatalf("waker-failure cold response leaked its platform reservation: %v", err)
	}
	_ = physical.Complete(context.Background(), 0)

	if n := len(em.all()); n != 0 {
		t.Fatalf("waker-failure cold response emitted %d events, want 0", n)
	}
}

// TestWakeExhaustedWarmFinalSettlesActualUsage pins the tries-exhausted tail's
// NON-cold final axis: the in-loop probes stay cold (each triggering a wake)
// but the FINAL dispatch comes back warm — the engine DID real work. The tail
// must NOT settle Complete(0) and flush the buffer: it returns false so the
// caller's metered forward serves the response, emits exactly one usage-bearing
// metering row, and settles the lease with the engine-authoritative usage.
// Under the old unconditional-zero settlement the generated/prompt probes below
// would be ADMITTED (nothing charged); with actual usage settled they must be
// REJECTED.
func TestWakeExhaustedWarmFinalSettlesActualUsage(t *testing.T) {
	var requests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) > 2 { // in-loop probes (2) stay cold; the final dispatch is warm
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":5,"completion_tokens":7}}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Model not found"}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	cfg.Platform.MaxColdHolds = 1
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	a := admission.New(c, cfg)
	// The waker succeeds but never warms the backend: only the counting handler
	// above decides which dispatch sees the warm body.
	waker := &fakeWaker{}
	em := &recordingEmitter{}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), em).
		WithAdmitter(a).
		WithWaker(waker, time.Second, 2)

	charge := func() {
		callsBefore := atomic.LoadInt32(&waker.calls)
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, withContractEnvelope(sharedRequest(up)))
		if rr.Code != http.StatusOK {
			t.Fatalf("status=%d, want the warm 200 served by the caller's metered forward", rr.Code)
		}
		if !strings.Contains(rr.Body.String(), `"completion_tokens":7`) {
			t.Fatalf("body=%q, want the warm response body served to the client", rr.Body.String())
		}
		if got := atomic.LoadInt32(&waker.calls) - callsBefore; got != 2 {
			t.Fatalf("waker delta=%d, want 2 for maxTries=2 (one wake per cold in-loop probe)", got)
		}
		// Two cold in-loop probes + the warm final probe + the caller's metered
		// forward: four dispatches total, two warm.
		if got := requests.Load(); got != 4 {
			t.Fatalf("backend requests=%d, want 4 (2 cold probes + warm final probe + warm forward)", got)
		}
	}

	req := sharedRequest(up)
	est := sharedRequestEstimate(t, req)
	errs := chargeThenProbeAdmit(t, a, mr.FlushAll, charge, zeroSettlementProbeSet(est))
	if len(errs) != 3 {
		t.Fatalf("got %d probe errors, want 3", len(errs))
	}
	// The requests-window +1 is kept under every settlement (demand, not work).
	if scope, dimension, ok := rejectedScopeDimension(errs[0]); !ok || scope != "contract_organization" || dimension != "requests" {
		t.Errorf("requests probe: err=%v, want contract_organization requests rejection", errs[0])
	}
	// The generated/prompt probes reserve exactly the conservative estimate
	// against a limit equal to it. They must be REJECTED: the lease settled with
	// ACTUAL usage (generated=7, prompt=5) still occupying the windows. A
	// Complete(0) settlement would leave the windows empty and both probes
	// would be ADMITTED — this is the billing-hole pin.
	for i, wantDimension := range []string{"generated_tokens", "total_prompt_tokens"} {
		if scope, dimension, ok := rejectedScopeDimension(errs[i+1]); !ok || scope != "contract_organization" || dimension != wantDimension {
			t.Errorf("probe %d: err=%v, want contract_organization %s rejection (actual usage settled, not zero)", i+1, errs[i+1], wantDimension)
		}
	}

	// Exactly one metering event, carrying the engine's usage block.
	events := em.waitForEvents(1, 5*time.Second)
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want exactly 1 (the caller's metered forward)", len(events))
	}
	e := events[0]
	if !e.UsageFound || e.PromptTokens != 5 || e.CompletionTokens != 7 || e.StatusCode != http.StatusOK {
		t.Fatalf("event=%+v, want one usage-bearing 200 event (prompt=5 completion=7)", e)
	}

	// Physical capacity is released by the usage settlement.
	physical, err := a.Admit(context.Background(), admission.Request{
		Graph: "graph", Organization: "org-c", Model: "m",
		PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
	})
	if err != nil {
		t.Fatalf("warm-final usage settlement leaked its platform reservation: %v", err)
	}
	_ = physical.Complete(context.Background(), 0)
}

// TestWakeExhaustedTransportErrorSettlesUnknownUsage pins the tries-exhausted
// tail's indeterminate-fault axis: the in-loop probes stay cold, then the
// engine dies before the final dispatch — the connection drops mid-retry. The
// tail must NOT settle never-served zero and flush an unclassified 502: it
// returns false so the caller's error handler classifies the fault — one
// UsageFound=false 502 metering row and the conservative reservation retained
// (CompleteUnknownUsage), exactly the R1 ruling for indeterminate faults.
func TestWakeExhaustedTransportErrorSettlesUnknownUsage(t *testing.T) {
	var requests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) > 2 { // engine dies after the last in-loop probe
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("backend ResponseWriter is not a Hijacker")
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			// Drop the connection without a byte: the dispatch fails at the
			// transport layer, deterministically pre-response.
			_ = conn.Close()
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Model not found"}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	cfg.Platform.MaxColdHolds = 1
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	a := admission.New(c, cfg)
	waker := &fakeWaker{} // succeeds; the backend dies on its own count
	em := &recordingEmitter{}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), em).
		WithAdmitter(a).
		WithWaker(waker, time.Second, 2)

	charge := func() {
		callsBefore := atomic.LoadInt32(&waker.calls)
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, withContractEnvelope(sharedRequest(up)))
		if rr.Code != http.StatusBadGateway {
			t.Fatalf("status=%d, want the classified 502 from the caller's error handler", rr.Code)
		}
		if got := atomic.LoadInt32(&waker.calls) - callsBefore; got != 2 {
			t.Fatalf("waker delta=%d, want 2 for maxTries=2 (one wake per cold in-loop probe)", got)
		}
		// Two cold in-loop probes + the failed final probe + the caller's failed
		// forward: four dispatches, none warm.
		if got := requests.Load(); got != 4 {
			t.Fatalf("backend requests=%d, want 4 (2 cold probes + failed final probe + failed forward)", got)
		}
	}

	req := sharedRequest(up)
	est := sharedRequestEstimate(t, req)
	errs := chargeThenProbeAdmit(t, a, mr.FlushAll, charge, zeroSettlementProbeSet(est))
	if len(errs) != 3 {
		t.Fatalf("got %d probe errors, want 3", len(errs))
	}
	// Indeterminate fault: the conservative reservation is RETAINED in every
	// contract window (the requests-window +1 kept, and the generated/prompt
	// probes reserving exactly the conservative estimate are REJECTED) — the
	// same expectations as a dispatched engine error. A never-served-zero
	// settlement would ADMIT both token probes.
	for i, wantDimension := range []string{"requests", "generated_tokens", "total_prompt_tokens"} {
		if scope, dimension, ok := rejectedScopeDimension(errs[i]); !ok || scope != "contract_organization" || dimension != wantDimension {
			t.Errorf("probe %d: err=%v, want contract_organization %s rejection (conservative settlement retained)", i, errs[i], wantDimension)
		}
	}

	// Exactly one metering row: the classified UpstreamFault 502, no usage.
	events := em.waitForEvents(1, 5*time.Second)
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want exactly 1 (the caller's error-handler row)", len(events))
	}
	e := events[0]
	if e.UsageFound || e.StatusCode != http.StatusBadGateway || e.Aborted {
		t.Fatalf("event=%+v, want one UsageFound=false 502 row (UpstreamFault, not an abort)", e)
	}

	// Physical capacity is released even though the conservative charges stand.
	physical, err := a.Admit(context.Background(), admission.Request{
		Graph: "graph", Organization: "org-c", Model: "m",
		PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
	})
	if err != nil {
		t.Fatalf("transport-error final leaked its platform reservation: %v", err)
	}
	_ = physical.Complete(context.Background(), 0)
}

// TestAdmissionEngineErrorResponseChargesUnknownUsage pins the conservative
// side of the ruling: an engine 4xx/5xx AFTER A REAL DISPATCH, with no usage
// block, cannot be distinguished from a prompt-processed-then-failed request,
// so the conservative estimate is RETAINED in the org contract windows (and
// the requests-window +1 is kept). Physical capacity is still released.
func TestAdmissionEngineErrorResponseChargesUnknownUsage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
	}{
		{"engine 400", http.StatusBadRequest},
		{"engine 500", http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"error":"engine refused"}`))
			}))
			defer backend.Close()
			up, _ := url.Parse(backend.URL)
			mr := miniredis.RunT(t)
			cfg := proxyAdmissionConfig(1)
			c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = c.Close() })
			a := admission.New(c, cfg)
			s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(a)

			charge := func() {
				rr := httptest.NewRecorder()
				s.Handler().ServeHTTP(rr, withContractEnvelope(sharedRequest(up)))
				if rr.Code != tc.status {
					t.Fatalf("status=%d, want %d", rr.Code, tc.status)
				}
			}

			req := sharedRequest(up)
			est := sharedRequestEstimate(t, req)
			errs := chargeThenProbeAdmit(t, a, mr.FlushAll, charge, zeroSettlementProbeSet(est))
			if len(errs) != 3 {
				t.Fatalf("got %d probe errors, want 3", len(errs))
			}
			for i, wantDimension := range []string{"requests", "generated_tokens", "total_prompt_tokens"} {
				if scope, dimension, ok := rejectedScopeDimension(errs[i]); !ok || scope != "contract_organization" || dimension != wantDimension {
					t.Errorf("probe %d: err=%v, want contract_organization %s rejection (conservative settlement retained)", i, errs[i], wantDimension)
				}
			}

			// Physical capacity is released even though the conservative
			// contract charges stand.
			physical, err := a.Admit(context.Background(), admission.Request{
				Graph: "graph", Organization: "org-c", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
			})
			if err != nil {
				t.Fatalf("engine error response leaked its platform reservation: %v", err)
			}
			_ = physical.Complete(context.Background(), 0)
		})
	}
}
