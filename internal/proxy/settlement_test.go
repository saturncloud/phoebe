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
	"fmt"
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
	coldHoldRejection := &admissionRoundTripError{err: &admission.Rejected{
		Scope: "platform", Dimension: "cold_holds", RetryAfter: time.Second,
	}}
	coldHoldRejection429 := &admissionRoundTripError{err: &admission.Rejected{
		Scope: "contract_organization", Dimension: "cold_holds", RetryAfter: time.Second, Contractual: true,
	}}
	dialFailure := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	tests := []struct {
		name string
		err  error
		want settlementKind
	}{
		// The cold-hold capacity rejection is determinate never-served: the
		// engine answered cold and no inference ran. The settlement kind is the
		// same whether writeAdmissionError renders the rejection 503
		// (non-contractual scope) or 429 (contractual scope) — the axis is
		// never-served, not the status code.
		{"cold-hold rejection (503 rendering)", coldHoldRejection, settlementZeroNeverServed},
		{"cold-hold rejection (429 rendering)", coldHoldRejection429, settlementZeroNeverServed},
		{"wrapped cold-hold rejection", fmt.Errorf("roundtrip: %w", coldHoldRejection), settlementZeroNeverServed},
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
		name          string
		res           capture.Result
		wakeColdFinal bool
		want          settlementKind
	}{
		{"usage found", withUsage, false, settlementActual},
		// A captured usage block always wins, even on a final cold response.
		{"usage found on final cold response", withUsage, true, settlementActual},
		{"no usage, final cold response", noUsage, true, settlementZeroNeverServed},
		{"no usage, ordinary response", noUsage, false, settlementUnknown},
		// An aborted stream with no usage stays conservative: the engine may
		// have done work before the disconnect.
		{"aborted, no usage, ordinary response", abortedNoUsage, false, settlementUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyCaptureSettlement(tc.res, tc.wakeColdFinal); got != tc.want {
				t.Fatalf("classifyCaptureSettlement(%+v, %t) = %d, want %d", tc.res, tc.wakeColdFinal, got, tc.want)
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

// TestWakeRoundTripperMarksColdFinal pins the plumbing: the round tripper flags
// the response as final-cold exactly when it gives up and serves the engine's
// cold response (wake budget exhausted OR waker failed), and never for a warm
// or successfully re-warmed response.
func TestWakeRoundTripperMarksColdFinal(t *testing.T) {
	roundTrip := func(t *testing.T, backend *coldToWarmBackend, waker Waker, maxTries int) (*http.Response, *atomic.Bool) {
		t.Helper()
		be := httptest.NewServer(backend)
		t.Cleanup(be.Close)
		up, _ := url.Parse(be.URL)
		s := New(&config.Settings{}, logging.New(logging.ERROR), nil).WithWaker(waker, 5*time.Second, maxTries)
		req := httptest.NewRequest(http.MethodPost, "http://x/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
		replaceRequestBody(req, []byte(`{"model":"m"}`))
		req.URL = up
		id := identity.Identity{ResourceID: "r1", ServedModel: "m"}
		coldFinal := new(atomic.Bool)
		resp, err := s.newWakeRoundTripper(up.Host, "req-1", id, nil, coldFinal).RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp, coldFinal
	}

	t.Run("exhausted budget flags final cold", func(t *testing.T) {
		backend := &coldToWarmBackend{}
		resp, coldFinal := roundTrip(t, backend, &fakeWaker{warmsAt: 99, backend: backend}, 2)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status=%d, want the cold 404", resp.StatusCode)
		}
		if !coldFinal.Load() {
			t.Fatal("coldFinal not set after the wake budget was exhausted")
		}
	})
	t.Run("waker failure flags final cold", func(t *testing.T) {
		backend := &coldToWarmBackend{}
		resp, coldFinal := roundTrip(t, backend, &fakeWaker{err: context.DeadlineExceeded}, 3)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status=%d, want the cold 404", resp.StatusCode)
		}
		if !coldFinal.Load() {
			t.Fatal("coldFinal not set after the waker failed")
		}
	})
	t.Run("warm response does not flag", func(t *testing.T) {
		backend := &coldToWarmBackend{}
		backend.warm.Store(true)
		resp, coldFinal := roundTrip(t, backend, &fakeWaker{}, 3)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status=%d, want 200", resp.StatusCode)
		}
		if coldFinal.Load() {
			t.Fatal("coldFinal set for a warm response")
		}
	})
	t.Run("successful re-warm does not flag", func(t *testing.T) {
		backend := &coldToWarmBackend{}
		resp, coldFinal := roundTrip(t, backend, &fakeWaker{warmsAt: 1, backend: backend}, 3)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status=%d, want the warm 200", resp.StatusCode)
		}
		if coldFinal.Load() {
			t.Fatal("coldFinal set for a successfully re-warmed response")
		}
	})
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
// +1 kept, the physical platform reservation released. (The 429 contractual
// rendering is pinned at the classifier level in TestClassifyRoundTripSettlement:
// no current config produces a contractual cold-holds scope, so it cannot be
// driven end to end.)
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
// (the round tripper's give-up at maxTries): the engine refused every dispatch
// before any inference, so the final cold 404 settles ZERO — token windows
// uncharged, the requests-window +1 kept, the physical reservation released —
// rather than the conservative estimate a usage-less ordinary response would
// retain.
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
		if got := atomic.LoadInt32(&waker.calls) - callsBefore; got != 1 {
			t.Fatalf("waker delta=%d, want 1 for maxTries=2", got)
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
