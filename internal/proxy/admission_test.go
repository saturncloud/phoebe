package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

func proxyAdmissionConfig(active int64) config.AdmissionSettings {
	return config.AdmissionSettings{Enabled: true, KeyPrefix: "proxy-test", LeaseTTL: time.Minute,
		DefaultMaxOutputTokens: 20, Platform: config.AdmissionLimits{MaxActiveRequests: active,
			MaxConcurrentPrefills: active, MaxReservedDecodeSlots: active, MaxPromptBytes: 1024, MaxReservedOutputTokens: 100,
			RequestsPerWindow: 100, GeneratedTokensPerWindow: 1000, Window: time.Minute}}
}

func sharedRequest(upstream *url.URL) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","max_tokens":20}`))
	setUpstream(r, upstream)
	r.Header.Set(identity.HeaderAuthID, "auth-a")
	r.Header.Set(identity.HeaderOwnerID, "owner-a")
	r.Header.Set(identity.HeaderResourceID, "resource-a")
	r.Header.Set(identity.HeaderOrgID, "org-a")
	r.Header.Set(identity.HeaderServingMode, "shared")
	r.Header.Set(identity.HeaderServedModel, "model-a")
	// Admission-enabled shared requests require the complete authenticated
	// policy envelope regardless of whether they use the single-host gateway or
	// a transitional per-resource route. Explicit zero means unlimited.
	for _, header := range []string{
		identity.HeaderOrgRateLimitRequests,
		identity.HeaderOrgRateLimitTotalPromptTokens,
		identity.HeaderOrgRateLimitUncachedPromptTokens,
		identity.HeaderOrgRateLimitGeneratedTokens,
		identity.HeaderOwnerRateLimitRequests,
		identity.HeaderOwnerRateLimitTotalPromptTokens,
		identity.HeaderOwnerRateLimitUncachedPromptTokens,
		identity.HeaderOwnerRateLimitGeneratedTokens,
	} {
		r.Header.Set(header, "0")
	}
	return r
}

type countingAdmitter struct{ calls int }

func (a *countingAdmitter) Admit(context.Context, admission.Request) (*admission.Lease, error) {
	a.calls++
	return nil, nil
}

func sharedRequestBodyOfSize(t *testing.T, upstream *url.URL, size int) *http.Request {
	t.Helper()
	prefix := `{"model":"model-a","max_tokens":20,"padding":"`
	suffix := `"}`
	if size < len(prefix)+len(suffix) {
		t.Fatalf("body size %d too small", size)
	}
	body := prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix
	req := sharedRequest(upstream)
	replaceRequestBody(req, []byte(body))
	return req
}

func TestSharedRequestBodyBoundariesBeforeAdmission(t *testing.T) {
	const limit = 128
	var upstreamHits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHits++
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)

	newServer := func(a *countingAdmitter) *Server {
		cfg := proxyAdmissionConfig(2)
		cfg.Platform.MaxPromptBytes = limit
		return New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).
			WithAdmitter(a)
	}

	t.Run("exact limit reaches admission and upstream", func(t *testing.T) {
		a := &countingAdmitter{}
		rr := httptest.NewRecorder()
		newServer(a).Handler().ServeHTTP(rr, sharedRequestBodyOfSize(t, up, limit))
		if rr.Code != http.StatusOK || a.calls != 1 || upstreamHits != 1 {
			t.Fatalf("status=%d admission=%d upstream=%d", rr.Code, a.calls, upstreamHits)
		}
	})

	for _, tc := range []struct {
		name    string
		chunked bool
	}{
		{name: "known content length plus one"},
		{name: "chunked plus one", chunked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &countingAdmitter{}
			req := sharedRequestBodyOfSize(t, up, limit+1)
			if tc.chunked {
				req.ContentLength = -1
				req.Header.Del("Content-Length")
			}
			rr := httptest.NewRecorder()
			newServer(a).Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status=%d, want 413", rr.Code)
			}
			if a.calls != 0 || upstreamHits != 1 {
				t.Fatalf("oversized request reached admission=%d or upstream total=%d", a.calls, upstreamHits)
			}
		})
	}
}

func TestSharedRequestAtExactBodyLimitAdmitsWithRedisAdmitter(t *testing.T) {
	const limit = 128
	var upstreamHits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHits++
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(2)
	cfg.Platform.MaxPromptBytes = limit
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithAdmitter(admission.New(client, cfg))

	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, sharedRequestBodyOfSize(t, up, limit))
	if rr.Code != http.StatusOK || upstreamHits != 1 {
		t.Fatalf("status=%d upstream=%d, exact original JSON limit must admit", rr.Code, upstreamHits)
	}
}

func TestSharedRequestBodyLimitUsesSafeUnlimitedFallback(t *testing.T) {
	if got := sharedRequestBodyLimit(config.AdmissionSettings{}, config.AdmissionLane{}); got != defaultSharedRequestBodyLimit {
		t.Fatalf("unlimited fallback=%d, want %d", got, defaultSharedRequestBodyLimit)
	}
}

func TestAdmissionAcrossProxyReplicasAndLifecycleRelease(t *testing.T) {
	release := make(chan struct{})
	hit := make(chan struct{}, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit <- struct{}{}
		<-release
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":2,"completion_tokens":3}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	newReplica := func() *Server {
		c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = c.Close() })
		s := &config.Settings{Admission: cfg, BillPartialOnAbort: true}
		return New(s, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
	}
	one, two := newReplica(), newReplica()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { rr := httptest.NewRecorder(); one.Handler().ServeHTTP(rr, sharedRequest(up)); done <- rr }()
	select {
	case <-hit:
	case <-time.After(time.Second):
		t.Fatal("first request never reached backend")
	}
	rr2 := httptest.NewRecorder()
	two.Handler().ServeHTTP(rr2, sharedRequest(up))
	if rr2.Code != http.StatusServiceUnavailable {
		t.Fatalf("contending replica status=%d, want 503", rr2.Code)
	}
	close(release)
	if rr := <-done; rr.Code != http.StatusOK {
		t.Fatalf("first status=%d", rr.Code)
	}
	// The completion callback released all active/output reservations and charged
	// only the engine-reported three generated tokens.
	rr3 := httptest.NewRecorder()
	two.Handler().ServeHTTP(rr3, sharedRequest(up))
	if rr3.Code != http.StatusOK {
		t.Fatalf("post-completion status=%d, want 200", rr3.Code)
	}
}

func TestAdmissionStateFailureBypassesGate(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
	var hits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":7,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":2}}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	cfg := proxyAdmissionConfig(1)
	client := admission.NewValkeyClient(listener.Addr().String())
	t.Cleanup(func() { _ = client.Close() })
	em := &recordingEmitter{}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), em).WithAdmitter(admission.New(client, cfg))
	rr := httptest.NewRecorder()
	started := time.Now()
	s.Handler().ServeHTTP(rr, sharedRequest(up))
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("fail-open admission took %s against accept/no-reply Valkey", elapsed)
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rr.Code)
	}
	if hits != 1 {
		t.Fatalf("fail-open request reached upstream %d times, want 1", hits)
	}
	events := em.waitForEvents(1, time.Second)
	if len(events) != 1 || events[0].PromptTokens != 7 || events[0].CachedTokens != 2 || events[0].CompletionTokens != 3 {
		t.Fatalf("fail-open request was not independently metered: %+v", events)
	}
}

func TestWakeEnabledWarmRequestExecutesMetersAndSettlesOnce(t *testing.T) {
	var hits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":7,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":2}}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	a := admission.New(client, cfg)
	em := &recordingEmitter{}
	waker := &fakeWaker{}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), em).
		WithAdmitter(a).
		WithWaker(waker, time.Second, 3)

	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, sharedRequest(up))
	if rr.Code != http.StatusOK || hits != 1 || waker.calls != 0 {
		t.Fatalf("status=%d backend hits=%d wake calls=%d", rr.Code, hits, waker.calls)
	}
	events := em.waitForEvents(1, time.Second)
	if len(events) != 1 || events[0].PromptTokens != 7 || events[0].CompletionTokens != 3 {
		t.Fatalf("metering events=%+v, want exactly one authoritative event", events)
	}
	lease, err := a.Admit(context.Background(), admission.Request{
		Graph: "graph-a", Organization: "org-b", Model: "model-a",
		PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
	})
	if err != nil {
		t.Fatalf("warm request did not settle its admission lease: %v", err)
	}
	_ = lease.Complete(context.Background(), 0)
}

// A wakeable request that loses the cold-hold race must get the admission
// rejection (503 + Retry-After), never invoke the waker, and release its lease
// completely.
func TestWakeColdHoldRejectionFailsClosedAndReleasesLease(t *testing.T) {
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
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithAdmitter(a).
		WithWaker(waker, time.Second, 3)

	// A contending hold occupies the platform's single cold-hold slot.
	holder, err := a.Admit(context.Background(), admission.Request{
		Graph: "graph", Organization: "holder", Model: "m",
		PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.BeginColdHold(context.Background()); err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, sharedRequest(up))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want the cold-hold rejection's 503", rr.Code)
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Fatal("cold-hold rejection must carry Retry-After")
	}
	if calls := atomic.LoadInt32(&waker.calls); calls != 0 {
		t.Fatalf("waker invoked %d times despite the rejected cold hold", calls)
	}

	// The rejected request's lease must be fully released: of the two platform
	// active slots the holder owns exactly one, so the next Admit succeeds and
	// the one after fails. A leaked lease would reject the first.
	lease, err := a.Admit(context.Background(), admission.Request{
		Graph: "graph", Organization: "org-b", Model: "m",
		PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
	})
	if err != nil {
		t.Fatalf("cold-hold rejection leaked its lease: %v", err)
	}
	if _, err := a.Admit(context.Background(), admission.Request{
		Graph: "graph", Organization: "org-c", Model: "m",
		PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
	}); err == nil {
		t.Fatal("platform active limit not enforced after cold-hold rejection")
	}
	_ = lease.Complete(context.Background(), 0)
	_ = holder.EndColdHold(context.Background())
	_ = holder.Complete(context.Background(), 0)
}

// The prefill reservation must release at the first response BODY byte, not at
// header arrival: headers can arrive long before the engine finishes prefill,
// and releasing there would under-protect prefill bursts. The first request
// goes through a real HTTP client so "response headers arrived" is observed
// AFTER the proxy's ModifyResponse ran, with no backend-to-proxy race.
func TestPrefillReservationReleasesAtFirstBodyByte(t *testing.T) {
	writeFirstByte := make(chan struct{})
	firstByteWritten := make(chan struct{})
	finishFirst := make(chan struct{})
	var startOnce, finishOnce sync.Once
	startBody := func() { startOnce.Do(func() { close(writeFirstByte) }) }
	releaseFirst := func() { finishOnce.Do(func() { close(finishFirst) }) }
	var requests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			// First request: headers immediately, then withhold the body.
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-writeFirstByte
			_, _ = w.Write([]byte(`{"model"`))
			w.(http.Flusher).Flush()
			close(firstByteWritten)
			<-finishFirst
			_, _ = w.Write([]byte(`:"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
			return
		}
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(2)
	cfg.Platform.MaxConcurrentPrefills = 1 // prefill is the only binding dimension
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
	front := httptest.NewServer(s.Handler())
	defer front.Close()
	// Registered after the servers so it runs before their blocking Close():
	// never leave the first backend handler stuck on a failure path.
	defer func() { startBody(); releaseFirst() }()

	// Real client request: Do returns once response headers arrive, i.e. after
	// the proxy received headers and ran ModifyResponse.
	first, err := http.NewRequestWithContext(context.Background(), http.MethodPost, front.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"model-a","max_tokens":20}`))
	if err != nil {
		t.Fatal(err)
	}
	for k, vs := range sharedRequest(up).Header {
		first.Header[k] = vs
	}
	resp, err := front.Client().Do(first)
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	defer resp.Body.Close()

	// While the first response's body is withheld, its prefill slot is still
	// held: a second request at the same platform scope is rejected at the
	// prefill dimension (the only dimension that can bind here).
	second := httptest.NewRecorder()
	s.Handler().ServeHTTP(second, sharedRequest(up))
	if second.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d while the first body was withheld, want 503 prefill rejection", second.Code)
	}

	// The first body byte is the prefill→decode boundary: capacity opens even
	// though the first response has not completed.
	startBody()
	select {
	case <-firstByteWritten:
	case <-time.After(2 * time.Second):
		t.Fatal("backend never wrote the first body byte")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, sharedRequest(up))
		if rr.Code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("prefill capacity did not open at the first body byte; last status=%d", rr.Code)
		}
		time.Sleep(5 * time.Millisecond)
	}

	releaseFirst()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("read first response body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first request status=%d, want 200", resp.StatusCode)
	}
}

// When every wake attempt stays cold, the client receives the final cold
// response and the request's lease is fully settled — no capacity leaks even
// though no warm response ever arrived.
func TestWakeExhaustedReturnsColdAndReleasesCapacity(t *testing.T) {
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
	waker := &fakeWaker{warmsAt: 99, backend: backend}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithAdmitter(a).
		WithWaker(waker, time.Second, 2)

	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, sharedRequest(up))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want the final cold 404 after wake exhaustion", rr.Code)
	}
	if calls := atomic.LoadInt32(&waker.calls); calls != 1 {
		t.Fatalf("waker calls=%d, want 1 for maxTries=2", calls)
	}
	lease, err := a.Admit(context.Background(), admission.Request{
		Graph: "graph", Organization: "org-b", Model: "m",
		PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
	})
	if err != nil {
		t.Fatalf("exhausted wake leaked its lease: %v", err)
	}
	_ = lease.Complete(context.Background(), 0)
}

// A store outage at BeginColdHold degrades to a logged bypass: the wake flow
// and the request complete normally instead of failing closed.
func TestWakeColdHoldStoreOutageDegradesToBypass(t *testing.T) {
	mr := miniredis.RunT(t)
	var killOnce sync.Once
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Kill the admission store after Admit succeeded but before the cold
		// response drives BeginColdHold.
		killOnce.Do(mr.Close)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Model not found"}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	cfg := proxyAdmissionConfig(1)
	cfg.Platform.MaxColdHolds = 1
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	a := admission.New(c, cfg)
	waker := &fakeWaker{}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithAdmitter(a).
		WithWaker(waker, time.Second, 2)

	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, sharedRequest(up))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want the cold 404 — a cold-hold store outage must bypass, not reject", rr.Code)
	}
	if calls := atomic.LoadInt32(&waker.calls); calls != 1 {
		t.Fatalf("waker calls=%d, want 1 (the bypass must not skip the wake)", calls)
	}
}

func TestAdmissionImpossibleOutputRejectedBeforeUpstream(t *testing.T) {
	var hits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hits++; w.WriteHeader(200) }))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(2)
	cfg.Platform.MaxReservedOutputTokens = 10
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, sharedRequest(up))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", rr.Code)
	}
	if hits != 0 {
		t.Fatal("impossible request reached upstream")
	}
}

func TestContractRateLimitReturns429BeforeUpstream(t *testing.T) {
	var hits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hits++; w.WriteHeader(200) }))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(2)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
	req := sharedRequest(up)
	req.Header.Set(identity.HeaderOwnerRateLimitGeneratedTokens, "10")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d, want 429", rr.Code)
	}
	if hits != 0 {
		t.Fatal("contractually over-limit request reached upstream")
	}
}

func TestAdmissionRetryAfterCeilsRemainingFixedWindow(t *testing.T) {
	s := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{})
	rr := httptest.NewRecorder()
	s.writeAdmissionError(rr, &admission.Rejected{
		Scope: "contract_organization", Dimension: "requests", Contractual: true,
		RetryAfter: 1500 * time.Millisecond,
	})
	if got := rr.Header().Get("Retry-After"); got != "2" {
		t.Fatalf("Retry-After=%q, want ceil(1.5s)=2", got)
	}
}

func TestMalformedTrustedRateLimitFailsClosed(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	cfg := proxyAdmissionConfig(2)
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
	req := sharedRequest(up)
	req.Header.Set(identity.HeaderOrgRateLimitRequests, "not-a-number")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", rr.Code)
	}
}

func TestTrustedRateLimitParserBoundaries(t *testing.T) {
	var hits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	cfg := proxyAdmissionConfig(2)
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))

	t.Run("negative limit fails closed", func(t *testing.T) {
		req := sharedRequest(up)
		req.Header.Set(identity.HeaderOrgRateLimitRequests, "-1")
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d, want 503", rr.Code)
		}
	})
	t.Run("uncached above total fails closed", func(t *testing.T) {
		req := sharedRequest(up)
		req.Header.Set(identity.HeaderOrgRateLimitTotalPromptTokens, "100")
		req.Header.Set(identity.HeaderOrgRateLimitUncachedPromptTokens, "101")
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d, want 503", rr.Code)
		}
	})
	t.Run("uncached equal to total is valid", func(t *testing.T) {
		req := sharedRequest(up)
		req.Header.Set(identity.HeaderOrgRateLimitTotalPromptTokens, "100")
		req.Header.Set(identity.HeaderOrgRateLimitUncachedPromptTokens, "100")
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status=%d, want 200", rr.Code)
		}
	})
	if hits != 1 {
		t.Fatalf("upstream hits=%d, want exactly the valid boundary request", hits)
	}
}

func TestMissingOrPartialTrustedRateLimitPolicyFailsClosed(t *testing.T) {
	_, _, err := parseTrustedRateLimits(identity.Identity{Gateway: true})
	if err == nil {
		t.Fatal("gateway request without Atlas policy was accepted as unlimited")
	}
	_, _, err = parseTrustedRateLimits(identity.Identity{OwnerID: "owner-1"})
	if err == nil {
		t.Fatal("per-resource shared request without policy was accepted as unlimited")
	}
	organization, owner, err := parseTrustedRateLimits(identity.Identity{
		Gateway: true, OwnerID: "owner-1",
		OrgRateLimitRequests: "0", OrgRateLimitTotalPromptTokens: "0",
		OrgRateLimitUncachedPromptTokens: "0", OrgRateLimitGeneratedTokens: "0",
		OwnerRateLimitRequests: "0", OwnerRateLimitTotalPromptTokens: "0",
		OwnerRateLimitUncachedPromptTokens: "0", OwnerRateLimitGeneratedTokens: "0",
	})
	if err != nil || organization != (admission.RateLimits{}) || owner != (admission.RateLimits{}) {
		t.Fatalf("explicit unlimited gateway policy = org=%+v owner=%+v, %v", organization, owner, err)
	}
	legacyOrg, legacyOwner, err := parseTrustedRateLimits(identity.Identity{
		Gateway: true, LegacyServiceTier: "default", LegacyRateLimitRequests: "7",
		LegacyRateLimitTotalPromptTokens: "100", LegacyRateLimitUncachedPromptTokens: "25",
		LegacyRateLimitGeneratedTokens: "50",
	})
	if err != nil || legacyOrg != (admission.RateLimits{Requests: 7, TotalPromptTokens: 100, UncachedPromptTokens: 25, GeneratedTokens: 50}) ||
		legacyOwner != (admission.RateLimits{}) {
		t.Fatalf("legacy gateway policy = org=%+v owner=%+v, %v", legacyOrg, legacyOwner, err)
	}
	_, _, err = parseTrustedRateLimits(identity.Identity{
		Gateway: true, OwnerID: "partial-new", LegacyServiceTier: "default",
		LegacyRateLimitRequests: "0", LegacyRateLimitTotalPromptTokens: "0",
		LegacyRateLimitUncachedPromptTokens: "0", LegacyRateLimitGeneratedTokens: "0",
	})
	if err == nil {
		t.Fatal("partial new envelope incorrectly fell back to the legacy policy")
	}
}

func TestAdmissionEnabledPerResourceRequestWithoutPolicyFailsClosed(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	cfg := proxyAdmissionConfig(1)
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))

	for _, tc := range []struct {
		name   string
		mutate func(*http.Request)
	}{
		{
			name: "missing",
			mutate: func(req *http.Request) {
				for _, header := range []string{
					identity.HeaderOrgRateLimitRequests,
					identity.HeaderOrgRateLimitTotalPromptTokens,
					identity.HeaderOrgRateLimitUncachedPromptTokens,
					identity.HeaderOrgRateLimitGeneratedTokens,
					identity.HeaderOwnerRateLimitRequests,
					identity.HeaderOwnerRateLimitTotalPromptTokens,
					identity.HeaderOwnerRateLimitUncachedPromptTokens,
					identity.HeaderOwnerRateLimitGeneratedTokens,
				} {
					req.Header.Del(header)
				}
			},
		},
		{
			name: "partial new plus complete legacy",
			mutate: func(req *http.Request) {
				req.Header.Del(identity.HeaderOwnerRateLimitGeneratedTokens)
				req.Header.Set(identity.HeaderLegacyServiceTier, "default")
				req.Header.Set(identity.HeaderLegacyRateLimitRequests, "0")
				req.Header.Set(identity.HeaderLegacyRateLimitTotalPromptTokens, "0")
				req.Header.Set(identity.HeaderLegacyRateLimitUncachedPromptTokens, "0")
				req.Header.Set(identity.HeaderLegacyRateLimitGeneratedTokens, "0")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := sharedRequest(up)
			tc.mutate(req)
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d, want 503", rr.Code)
			}
		})
	}
}

func TestSharedRequestMissingOrganizationIdentity(t *testing.T) {
	const requestBody = `{"model":"model-a","max_tokens":20}`
	var upstreamHits int
	var observedTenant string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits++
		observedTenant = r.Header.Get("X-Tenant-ID")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)

	t.Run("enabled admission fails closed before upstream", func(t *testing.T) {
		cfg := proxyAdmissionConfig(1)
		mr := miniredis.RunT(t)
		c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
		req := sharedRequest(up)
		req.Header.Del(identity.HeaderOrgID)
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d, want 503", rr.Code)
		}
		if upstreamHits != 0 {
			t.Fatalf("missing organization reached upstream %d times", upstreamHits)
		}
	})

	t.Run("disabled admission uses trusted resource isolation", func(t *testing.T) {
		cfg := proxyAdmissionConfig(1)
		s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{})
		req := sharedRequest(up)
		req.Header.Del(identity.HeaderOrgID)
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status=%d, want 200", rr.Code)
		}
		_, expectedTenant, err := prepareSharedDynamoRequest(
			[]byte(requestBody), "resource:resource-a", 20, config.AdmissionLane{},
		)
		if err != nil {
			t.Fatal(err)
		}
		if observedTenant != expectedTenant {
			t.Fatalf("tenant=%q, want resource-isolated %q", observedTenant, expectedTenant)
		}
	})
}

// A degenerate trusted upstream (":8000") parses but yields no graph scope.
// The request must fail closed with 503 — never enter the fail-open admission
// bypass (which would forward it with every limit disabled).
func TestDegenerateUpstreamGraphFailsClosedNoBypass(t *testing.T) {
	up, _ := url.Parse("http://127.0.0.1:1") // target irrelevant; request never forwards
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
	req := sharedRequest(up)
	req.Header.Set(identity.HeaderUpstream, ":8000")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503 (a fail-open bypass would attempt to forward and 502)", rr.Code)
	}
	// Zero bypass: no lease was admitted, so a contending request at the same
	// platform scope (capacity 1) must succeed immediately.
	lease, err := admission.New(c, cfg).Admit(context.Background(), admission.Request{
		Graph: "graph-a", Organization: "org-a", Owner: "owner-a", Model: "model-a",
		PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
	})
	if err != nil {
		t.Fatalf("degenerate upstream request leaked into admission state: %v", err)
	}
	_ = lease.Complete(context.Background(), 0)
}

func TestAdmissionReleasesOnUpstreamFailure(t *testing.T) {
	up, _ := url.Parse("http://127.0.0.1:1")
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
	for i := 0; i < 2; i++ {
		rr := httptest.NewRecorder()
		req := sharedRequest(up)
		req = req.WithContext(context.Background())
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusBadGateway {
			t.Fatalf("attempt %d status=%d, want 502 (a leaked lease would surface as 503)", i, rr.Code)
		}
	}
}

func TestAdmissionReleasesAbortedStream(t *testing.T) {
	started := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("data: {\"model\":\"model-a\",\"choices\":[]}\n\n"))
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	a := admission.New(c, cfg)
	s := New(&config.Settings{Admission: cfg, BillPartialOnAbort: true}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(a)
	ctx, cancel := context.WithCancel(context.Background())
	req := sharedRequest(up)
	req.Header.Set(identity.HeaderOrgRateLimitGeneratedTokens, "20")
	req.Header.Set(identity.HeaderOwnerRateLimitGeneratedTokens, "20")
	req = req.WithContext(ctx)
	done := make(chan struct{})
	go func() { defer close(done); s.Handler().ServeHTTP(httptest.NewRecorder(), req) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("stream never started")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("aborted proxy did not return")
	}
	for _, tc := range []struct {
		name string
		req  admission.Request
		want string
	}{
		{
			name: "organization contract",
			req: admission.Request{Graph: "graph", Organization: "org-a", Owner: "other-owner", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
				OrganizationLimits: admission.RateLimits{GeneratedTokens: 20}},
			want: "contract_organization",
		},
		{
			name: "owner contract",
			req: admission.Request{Graph: "graph", Organization: "other-org", Owner: "owner-a", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
				OwnerLimits: admission.RateLimits{GeneratedTokens: 20}},
			want: "contract_owner",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := a.Admit(context.Background(), tc.req)
			var rejected *admission.Rejected
			if !errors.As(err, &rejected) || rejected.Scope != tc.want || rejected.Dimension != "generated_tokens" {
				t.Fatalf("err=%v, want %s generated_tokens rejection", err, tc.want)
			}
		})
	}
	lease, err := a.Admit(context.Background(), admission.Request{Graph: "graph", Organization: "other", Model: "m", PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1})
	if err != nil {
		t.Fatalf("aborted stream leaked reservation: %v", err)
	}
	_ = lease.Complete(context.Background(), 0)
}

func TestAdmissionChargesUnknownUsageOnPreHeaderAbort(t *testing.T) {
	started := make(chan struct{})
	unblock := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
		case <-unblock:
		}
	}))
	defer backend.Close()
	defer close(unblock)
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	a := admission.New(c, cfg)
	s := New(&config.Settings{Admission: cfg, BillPartialOnAbort: true}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(a)
	ctx, cancel := context.WithCancel(context.Background())
	req := sharedRequest(up)
	req.Header.Set(identity.HeaderOrgRateLimitGeneratedTokens, "20")
	req.Header.Set(identity.HeaderOwnerRateLimitGeneratedTokens, "20")
	req = req.WithContext(ctx)
	done := make(chan struct{})
	go func() { defer close(done); s.Handler().ServeHTTP(httptest.NewRecorder(), req) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request never reached backend")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pre-header abort did not return")
	}

	for _, tc := range []struct {
		name string
		req  admission.Request
		want string
	}{
		{
			name: "organization contract",
			req: admission.Request{Graph: "graph", Organization: "org-a", Owner: "other-owner", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
				OrganizationLimits: admission.RateLimits{GeneratedTokens: 20}},
			want: "contract_organization",
		},
		{
			name: "owner contract",
			req: admission.Request{Graph: "graph", Organization: "other-org", Owner: "owner-a", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
				OwnerLimits: admission.RateLimits{GeneratedTokens: 20}},
			want: "contract_owner",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := a.Admit(context.Background(), tc.req)
			var rejected *admission.Rejected
			if !errors.As(err, &rejected) || rejected.Scope != tc.want || rejected.Dimension != "generated_tokens" {
				t.Fatalf("err=%v, want %s generated_tokens rejection", err, tc.want)
			}
		})
	}
}

// An upstream that consumes the full request and then resets before writing
// response headers fails RoundTrip with EOF — not a client cancel. The engine's
// usage is indeterminate, so the conservative reservation must be retained in
// the org/owner contract windows and the request must be metered exactly like
// the pre-header abort path.
func TestAdmissionChargesUnknownUsageOnUpstreamReset(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // consume the full request, then vanish
		conn, _, herr := w.(http.Hijacker).Hijack()
		if herr != nil {
			return
		}
		_ = conn.Close()
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	a := admission.New(c, cfg)
	em := &recordingEmitter{}
	s := New(&config.Settings{Admission: cfg, BillPartialOnAbort: true}, logging.New(logging.ERROR), em).WithAdmitter(a)
	req := sharedRequest(up)
	req.Header.Set(identity.HeaderOrgRateLimitGeneratedTokens, "20")
	req.Header.Set(identity.HeaderOwnerRateLimitGeneratedTokens, "20")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status=%d, want 502", rr.Code)
	}

	events := em.waitForEvents(1, time.Second)
	if len(events) != 1 || !events[0].Aborted || events[0].PromptTokens != 0 || events[0].CompletionTokens != 0 {
		t.Fatalf("metering events=%+v, want the same zero-token attributable event as the abort path", events)
	}

	for _, tc := range []struct {
		name string
		req  admission.Request
		want string
	}{
		{
			name: "organization contract",
			req: admission.Request{Graph: "graph", Organization: "org-a", Owner: "other-owner", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
				OrganizationLimits: admission.RateLimits{GeneratedTokens: 20}},
			want: "contract_organization",
		},
		{
			name: "owner contract",
			req: admission.Request{Graph: "graph", Organization: "other-org", Owner: "owner-a", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
				OwnerLimits: admission.RateLimits{GeneratedTokens: 20}},
			want: "contract_owner",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := a.Admit(context.Background(), tc.req)
			var rejected *admission.Rejected
			if !errors.As(err, &rejected) || rejected.Scope != tc.want || rejected.Dimension != "generated_tokens" {
				t.Fatalf("err=%v, want %s generated_tokens rejection", err, tc.want)
			}
		})
	}
	// Physical capacity itself is released; only the conservative contract
	// charges are retained.
	lease, err := a.Admit(context.Background(), admission.Request{Graph: "graph", Organization: "other", Model: "m", PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1})
	if err != nil {
		t.Fatalf("reset upstream leaked physical reservation: %v", err)
	}
	_ = lease.Complete(context.Background(), 0)
}

func TestAdmissionRenewalFailureDoesNotCancelUpstream(t *testing.T) {
	started := make(chan struct{})
	releaseBackend := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
		case <-releaseBackend:
		}
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	cfg.LeaseTTL = 30 * time.Millisecond
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(), DialTimeout: 20 * time.Millisecond, ReadTimeout: 20 * time.Millisecond,
		WriteTimeout: 20 * time.Millisecond, MaxRetries: 0,
	})
	defer client.Close()
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithAdmitter(admission.New(client, cfg))
	rr := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Handler().ServeHTTP(rr, sharedRequest(up))
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request never reached upstream")
	}
	mr.Close()
	time.Sleep(100 * time.Millisecond)
	close(releaseBackend)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not complete after backend release")
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200 after admission authority loss", rr.Code)
	}
}

func TestDedicatedTrafficBypassesSharedAdmission(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	cfg := proxyAdmissionConfig(1)
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 20 * time.Millisecond, MaxRetries: 0})
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(client, cfg))
	req := sharedRequest(up)
	req.Header.Set(identity.HeaderServingMode, "dedicated")
	req.Header.Del(identity.HeaderServedModel)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("dedicated status=%d, want 200", rr.Code)
	}
}

func TestPerResourceServingIdentityFailsClosedWhenInconsistent(t *testing.T) {
	var upstreamHits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHits++
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)

	tests := []struct {
		name        string
		servingMode string
		servedModel string
	}{
		{name: "model with absent mode", servedModel: "model-a"},
		{name: "model with dedicated mode", servingMode: "dedicated", servedModel: "model-a"},
		{name: "model with unknown mode", servingMode: "shraed", servedModel: "model-a"},
		{name: "shared mode without model", servingMode: "shared"},
		{name: "unknown mode without model", servingMode: "shraed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := &countingAdmitter{}
			s := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(a)
			req := sharedRequest(up)
			if tc.servingMode == "" {
				req.Header.Del(identity.HeaderServingMode)
			} else {
				req.Header.Set(identity.HeaderServingMode, tc.servingMode)
			}
			if tc.servedModel == "" {
				req.Header.Del(identity.HeaderServedModel)
			} else {
				req.Header.Set(identity.HeaderServedModel, tc.servedModel)
			}
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d, want 503", rr.Code)
			}
			if a.calls != 0 {
				t.Fatalf("admission calls=%d, want 0", a.calls)
			}
			if upstreamHits != 0 {
				t.Fatalf("upstream hits=%d, want 0", upstreamHits)
			}
		})
	}
}

func TestSharedPolicyOverwritesClientPriorityWhenAdmissionDisabled(t *testing.T) {
	seen := make(chan *http.Request, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Clone(r.Context())
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	s := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{})
	req := sharedRequest(up)
	req.Header.Set("X-Tenant-ID", "attacker")
	req.Header.Set("X-Dynamo-Request-Priority", "2147483647")
	req.Header.Set("X-Dynamo-Request-Strict-Priority", "4294967295")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d", rr.Code)
	}
	forwarded := <-seen
	if got := forwarded.Header.Get("X-Tenant-ID"); got == "" || got == "attacker" {
		t.Fatalf("forwarded tenant header=%q", got)
	}
	if got := forwarded.Header.Get("X-Dynamo-Request-Priority"); got != "0" {
		t.Fatalf("forwarded priority header=%q", got)
	}
	if got := forwarded.Header.Get("X-Dynamo-Request-Strict-Priority"); got != "0" {
		t.Fatalf("forwarded strict-priority header=%q", got)
	}
}

func TestAdmissionWorkRejectsAmbiguousOrImpossibleRequests(t *testing.T) {
	if _, ok := admissionWork([]byte(`{"model":"m","max_tokens":2,"max_completion_tokens":3}`), 10); ok {
		t.Fatal("conflicting output limits accepted")
	}
	body := []byte(`{"model":"m"}`)
	if estimate, ok := admissionWork(body, 10); !ok || estimate.Model != "m" || estimate.OutputTokens != 10 || estimate.InputTokens != int64((len(body)+3)/4) {
		t.Fatalf("estimate=(%+v,%t)", estimate, ok)
	}
	for _, body := range []string{
		`{"model":"m","model":"other","max_tokens":2}`,
		`{"model":"m","max_tokens":200,"max_tokens":2}`,
		`{"model":"m","max_completion_tokens":200,"max_completion_tokens":2}`,
		`{"model":"m","max_tokens":4294967296}`,
	} {
		if _, ok := admissionWork([]byte(body), 10); ok {
			t.Fatalf("ambiguous admission work accepted: %s", body)
		}
	}
}

func TestPrepareSharedDynamoRequestOverwritesUntrustedHints(t *testing.T) {
	body := []byte(`{"model":"m","max_tokens":41,"stream":true,"stream_options":{"include_usage":false},"cache_salt":"attacker","nvext":{"cache_salt":"attacker","keep":"yes","backend_instance_id":99,"token_data":[1,2],"agent_hints":{"priority":2147483647,"strict_priority":4294967295,"osl":1,"speculative_prefill":true}}}`)
	lane := config.AdmissionLane{DynamoPriority: 9, DynamoStrictPriority: 2}
	out, tenant, err := prepareSharedDynamoRequest(body, "org-a", 41, lane)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		CacheSalt     string `json:"cache_salt"`
		StreamOptions struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
		Nvext struct {
			CacheSalt         string           `json:"cache_salt"`
			Keep              string           `json:"keep"`
			BackendInstanceID *json.RawMessage `json:"backend_instance_id"`
			TokenData         *json.RawMessage `json:"token_data"`
			Hints             struct {
				Priority           int64 `json:"priority"`
				StrictPriority     int64 `json:"strict_priority"`
				OSL                int64 `json:"osl"`
				SpeculativePrefill bool  `json:"speculative_prefill"`
			} `json:"agent_hints"`
		} `json:"nvext"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	if tenant == "" || got.Nvext.CacheSalt != tenant || got.CacheSalt != tenant {
		t.Fatalf("cache salts=(%q,%q) tenant=%q", got.CacheSalt, got.Nvext.CacheSalt, tenant)
	}
	if got.Nvext.Hints.Priority != 9 || got.Nvext.Hints.StrictPriority != 2 || got.Nvext.Hints.OSL != 41 {
		t.Fatalf("trusted hints not applied: %+v", got.Nvext.Hints)
	}
	if got.Nvext.Keep != "yes" || got.Nvext.BackendInstanceID != nil || got.Nvext.TokenData != nil || got.Nvext.Hints.SpeculativePrefill || !got.StreamOptions.IncludeUsage {
		t.Fatalf("unrelated extensions or usage flag lost: %+v", got)
	}
	_, otherTenant, err := prepareSharedDynamoRequest(body, "org-b", 41, lane)
	if err != nil {
		t.Fatal(err)
	}
	if tenant == otherTenant {
		t.Fatal("distinct organizations received the same Dynamo tenant namespace")
	}
}

func TestProxyForwardsOperatorAdmissionLaneDynamoHints(t *testing.T) {
	seen := make(chan *http.Request, 1)
	seenBody := make(chan []byte, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := readAndRestoreBody(r)
		seen <- r.Clone(r.Context())
		seenBody <- body
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":2,"completion_tokens":3}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	cfg.Lanes = map[string]config.AdmissionLane{
		"default": {Weight: 1},
		"gold":    {Weight: 1, DynamoPriority: 11, DynamoStrictPriority: 4},
	}
	cfg.OrganizationLanes = map[string]string{"org-a": "gold"}
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
	req := sharedRequest(up)
	req.Header.Set("X-Tenant-ID", "attacker")
	req.Header.Set("X-Dynamo-Request-Priority", "2147483647")
	req.Header.Set("X-Dynamo-Request-Strict-Priority", "4294967295")
	req.Header.Set("X-Dynamo-Worker-Instance-ID", "99")
	req.Body = http.NoBody
	req.Body = io.NopCloser(strings.NewReader(`{"model":"model-a","max_tokens":20,"nvext":{"cache_salt":"attacker","agent_hints":{"priority":999}}}`))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d", rr.Code)
	}
	forwarded := <-seen
	if got := forwarded.Header.Get("X-Tenant-ID"); got == "" || got == "attacker" {
		t.Fatalf("forwarded tenant header=%q", got)
	}
	if got := forwarded.Header.Get("X-Dynamo-Request-Priority"); got != "11" {
		t.Fatalf("forwarded priority header=%q", got)
	}
	if got := forwarded.Header.Get("X-Dynamo-Request-Strict-Priority"); got != "4" {
		t.Fatalf("forwarded strict-priority header=%q", got)
	}
	if got := forwarded.Header.Get("X-Dynamo-Worker-Instance-ID"); got != "" {
		t.Fatalf("forwarded direct-worker header=%q", got)
	}
	var payload struct {
		Nvext struct {
			CacheSalt string `json:"cache_salt"`
			Hints     struct {
				Priority       int64 `json:"priority"`
				StrictPriority int64 `json:"strict_priority"`
				OSL            int64 `json:"osl"`
			} `json:"agent_hints"`
		} `json:"nvext"`
	}
	if err := json.Unmarshal(<-seenBody, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Nvext.CacheSalt != forwarded.Header.Get("X-Tenant-ID") || payload.Nvext.Hints.Priority != 11 || payload.Nvext.Hints.StrictPriority != 4 || payload.Nvext.Hints.OSL != 20 {
		t.Fatalf("forwarded trusted policy mismatch: %+v headers=%v", payload, forwarded.Header)
	}
}
