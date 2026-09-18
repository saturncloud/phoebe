package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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
	r.Header.Set(identity.HeaderResourceID, "resource-a")
	r.Header.Set(identity.HeaderOrgID, "org-a")
	r.Header.Set(identity.HeaderServingMode, "shared")
	r.Header.Set(identity.HeaderServedModel, "model-a")
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

func TestSharedRequestBodyLimitUsesSafeUnlimitedFallback(t *testing.T) {
	if got := sharedRequestBodyLimit(config.AdmissionSettings{}, config.AdmissionTier{}); got != defaultSharedRequestBodyLimit {
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
	var hits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":7,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":2}}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	cfg := proxyAdmissionConfig(1)
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 20 * time.Millisecond, ReadTimeout: 20 * time.Millisecond, WriteTimeout: 20 * time.Millisecond, MaxRetries: 0})
	em := &recordingEmitter{}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), em).WithAdmitter(admission.New(client, cfg))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, sharedRequest(up))
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
	req.Header.Set(identity.HeaderRateLimitGeneratedTokens, "10")
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
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{})
	req := sharedRequest(up)
	req.Header.Set(identity.HeaderRateLimitRequests, "not-a-number")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", rr.Code)
	}
}

func TestGatewayMissingTrustedRateLimitPolicyFailsClosed(t *testing.T) {
	_, err := parseTrustedRateLimits(identity.Identity{Gateway: true})
	if err == nil {
		t.Fatal("gateway request without Atlas policy was accepted as unlimited")
	}
	limits, err := parseTrustedRateLimits(identity.Identity{
		Gateway: true, ServiceTier: "default", RateLimitRequests: "0",
		RateLimitTotalPromptTokens: "0", RateLimitUncachedPromptTokens: "0",
		RateLimitGeneratedTokens: "0",
	})
	if err != nil || limits != (admission.RateLimits{}) {
		t.Fatalf("explicit unlimited gateway policy = %+v, %v", limits, err)
	}
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
			t.Fatalf("attempt %d status=%d, want 502 (a leaked lease would be 429)", i, rr.Code)
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
	req := sharedRequest(up).WithContext(ctx)
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
	lease, err := a.Admit(context.Background(), admission.Request{Graph: "graph", Organization: "other", Model: "m", PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1})
	if err != nil {
		t.Fatalf("aborted stream leaked reservation: %v", err)
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
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("dedicated status=%d, want 200", rr.Code)
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
	tier := config.AdmissionTier{DynamoPriority: 9, DynamoStrictPriority: 2}
	out, tenant, err := prepareSharedDynamoRequest(body, "org-a", 41, tier)
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
	_, otherTenant, err := prepareSharedDynamoRequest(body, "org-b", 41, tier)
	if err != nil {
		t.Fatal(err)
	}
	if tenant == otherTenant {
		t.Fatal("distinct organizations received the same Dynamo tenant namespace")
	}
}

func TestProxyForwardsAtlasServiceTierDynamoHints(t *testing.T) {
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
	cfg.Tiers = map[string]config.AdmissionTier{
		"default": {Weight: 1},
		"gold":    {Weight: 1, DynamoPriority: 11, DynamoStrictPriority: 4},
	}
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
	req := sharedRequest(up)
	req.Header.Set(identity.HeaderServiceTier, "gold")
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
