package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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

func ptr64(v int64) *int64 { return &v }

func proxyAdmissionConfig(active int64) config.AdmissionSettings {
	return config.AdmissionSettings{Enabled: true, KeyPrefix: "proxy-test", LeaseTTL: time.Minute,
		DefaultMaxOutputTokens: 20, Platform: config.AdmissionLimits{MaxActiveRequests: ptr64(active),
			MaxConcurrentPrefills: ptr64(active), MaxReservedDecodeSlots: ptr64(active), MaxPromptBytes: ptr64(1024), MaxReservedOutputTokens: ptr64(100),
			RequestsPerWindow: ptr64(100), GeneratedTokensPerWindow: ptr64(1000), Window: time.Minute}}
}

// fixedWindowBucket mirrors the admission store's fixed 1-minute window bucket
// (floor(now_ms/window_ms), internal/admission/scripts.go) so a test can
// detect a minute rollover between charging a window and probing it.
func fixedWindowBucket() int64 {
	return time.Now().UnixMilli() / time.Minute.Milliseconds()
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
	// Admission-enabled shared requests require the authenticated identity
	// anchor (X-Saturn-Owner-Id) regardless of whether they use the
	// single-host gateway or a transitional per-resource route; the limit
	// headers themselves are per-field — absent is unlimited, explicit "0" is
	// a zero cap. This base helper stamps large, non-binding caps and tests
	// that need an absent, binding, or zero-capped dimension override the
	// headers explicitly.
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
		r.Header.Set(header, "1000000")
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
		cfg.Platform.MaxPromptBytes = ptr64(limit)
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
	cfg.Platform.MaxPromptBytes = ptr64(limit)
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
		s := &config.Settings{Admission: cfg}
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
	// serveWithWake costs one readiness probe on the wake path (the backend is
	// warm, so the probe returns not-cold and the caller performs the real
	// metered forward): two backend hits, one wake-eligible probe and one
	// inference, zero wakes.
	if rr.Code != http.StatusOK || hits != 2 || waker.calls != 0 {
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
	cfg.Platform.MaxColdHolds = ptr64(1)
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
	cfg.Platform.MaxConcurrentPrefills = ptr64(1) // prefill is the only binding dimension
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

// prefillTransitionHook intercepts the admission prefill transition — the
// only Lua call whose ARGV carries the bare action string "prefill" — to
// block or fail it, simulating a slow or dead Valkey at the prefill→decode
// boundary. It matches both EVALSHA and EVAL (miniredis starts with no
// scripts loaded, so every call falls back from the former to the latter).
type prefillTransitionHook struct {
	entered chan struct{}
	release chan struct{}
	fail    error
	once    sync.Once
}

func (h *prefillTransitionHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *prefillTransitionHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *prefillTransitionHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() != "evalsha" && cmd.Name() != "eval" {
			return next(ctx, cmd)
		}
		// The prefill transition is the only script whose ARGV carries the
		// bare action string "prefill" (finish carries "finish", renew and
		// admit carry none). Exact-match per arg: admit's JSON body merely
		// contains the substring "prefills".
		prefill := false
		for _, arg := range cmd.Args() {
			if fmt.Sprint(arg) == "prefill" {
				prefill = true
				break
			}
		}
		if !prefill {
			return next(ctx, cmd)
		}
		h.once.Do(func() { close(h.entered) })
		if h.fail != nil {
			return h.fail
		}
		select {
		case <-h.release:
			return next(ctx, cmd)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// The prefill release is a Valkey round-trip and must stay off the first-byte
// Read path: with the store blocked or failing, the first byte still flows to
// the client. Settlement is unchanged — once the store answers, the transition
// records the release; if it never does, the lease's finish releases the
// prefill reservation itself (release_record), so capacity cannot leak.
func TestPrefillReleaseOffFirstBytePath(t *testing.T) {
	newBackend := func(requests *atomic.Int32, writeFirstByte <-chan struct{}, firstByteWritten chan<- struct{}, finishFirst <-chan struct{}) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
	}

	t.Run("blocked store does not delay the first byte", func(t *testing.T) {
		writeFirstByte := make(chan struct{})
		firstByteWritten := make(chan struct{})
		finishFirst := make(chan struct{})
		var startOnce, finishOnce sync.Once
		startBody := func() { startOnce.Do(func() { close(writeFirstByte) }) }
		releaseFirst := func() { finishOnce.Do(func() { close(finishFirst) }) }
		var requests atomic.Int32
		backend := newBackend(&requests, writeFirstByte, firstByteWritten, finishFirst)
		defer backend.Close()
		up, _ := url.Parse(backend.URL)
		mr := miniredis.RunT(t)
		cfg := proxyAdmissionConfig(2)
		cfg.Platform.MaxConcurrentPrefills = ptr64(1) // prefill is the only binding dimension
		c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = c.Close() })
		hook := &prefillTransitionHook{entered: make(chan struct{}), release: make(chan struct{})}
		c.AddHook(hook)
		var releaseHookOnce sync.Once
		releaseHook := func() { releaseHookOnce.Do(func() { close(hook.release) }) }
		s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
		front := httptest.NewServer(s.Handler())
		defer front.Close()
		// Registered after the servers so it runs before their blocking Close():
		// never leave the first backend handler stuck on a failure path.
		defer func() { startBody(); releaseFirst(); releaseHook() }()

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

		startBody()
		select {
		case <-firstByteWritten:
		case <-time.After(2 * time.Second):
			t.Fatal("backend never wrote the first body byte")
		}
		// The prefill transition is now in flight against the blocked store.
		select {
		case <-hook.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("prefill transition never reached the store")
		}
		// The first byte must reach the client while that round-trip is still
		// blocked; inline settlement would hold it until the store answered.
		readDone := make(chan error, 1)
		go func() {
			one := make([]byte, 1)
			_, rerr := resp.Body.Read(one)
			readDone <- rerr
		}()
		select {
		case err := <-readDone:
			if err != nil {
				t.Fatalf("first byte read: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("first byte blocked on the in-flight prefill release")
		}

		// Unblock the store: the release lands and the prefill slot opens even
		// though the first response is still streaming.
		releaseHook()
		deadline := time.Now().Add(2 * time.Second)
		for {
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, sharedRequest(up))
			if rr.Code == http.StatusOK {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("prefill release never settled; last status=%d", rr.Code)
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
	})

	t.Run("failing store defers the release to settlement", func(t *testing.T) {
		writeFirstByte := make(chan struct{})
		firstByteWritten := make(chan struct{})
		finishFirst := make(chan struct{})
		close(writeFirstByte)
		close(finishFirst)
		var requests atomic.Int32
		backend := newBackend(&requests, writeFirstByte, firstByteWritten, finishFirst)
		defer backend.Close()
		up, _ := url.Parse(backend.URL)
		mr := miniredis.RunT(t)
		cfg := proxyAdmissionConfig(2)
		cfg.Platform.MaxConcurrentPrefills = ptr64(1)
		c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = c.Close() })
		hook := &prefillTransitionHook{entered: make(chan struct{}), fail: errors.New("valkey unavailable")}
		c.AddHook(hook)
		s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
		front := httptest.NewServer(s.Handler())
		defer front.Close()

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
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			t.Fatalf("response body with a failing prefill release: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("first request status=%d, want 200", resp.StatusCode)
		}
		select {
		case <-hook.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("prefill transition never reached the store")
		}
		// The failed transition left rec.prefill set, so the lease's finish
		// releases the prefill reservation itself — capacity cannot leak.
		lease, err := admission.New(c, cfg).Admit(context.Background(), admission.Request{
			Graph: "graph", Organization: "org-b", Model: "m",
			PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
		})
		if err != nil {
			t.Fatalf("failed prefill release leaked the reservation past settlement: %v", err)
		}
		_ = lease.Complete(context.Background(), 0)
	})
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
	cfg.Platform.MaxColdHolds = ptr64(1)
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
	// serveWithWake wakes once per cold attempt: maxTries=2 means two probes
	// (both cold) with a wake between them, then the honest final cold forward.
	if calls := atomic.LoadInt32(&waker.calls); calls != 2 {
		t.Fatalf("waker calls=%d, want 2 for maxTries=2 (one wake per cold attempt)", calls)
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

// A store outage between Admit and the cold probe fails closed at the cold
// hold: the request gets the admission-unavailable error (503), the waker is
// never invoked, and the handler's deferred release fallback — not a cold-hold
// bypass — owns the lease release. (The bypass lives at Admit time: when the
// store is already down at Admit, ErrUnavailable bypasses the whole gate and
// the wake completes normally; a mid-request outage is a broken contract, not
// a soft limit, and fails closed.)
func TestWakeColdHoldStoreOutageFailsClosed(t *testing.T) {
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
	cfg.Platform.MaxColdHolds = ptr64(1)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	a := admission.New(c, cfg)
	waker := &fakeWaker{}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithAdmitter(a).
		WithWaker(waker, time.Second, 2)

	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, sharedRequest(up))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503 — a mid-request store outage must fail closed, not bypass", rr.Code)
	}
	if calls := atomic.LoadInt32(&waker.calls); calls != 0 {
		t.Fatalf("waker calls=%d, want 0 (a failed cold hold must not invoke the waker)", calls)
	}
}

func TestAdmissionImpossibleOutputRejectedBeforeUpstream(t *testing.T) {
	var hits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hits++; w.WriteHeader(200) }))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(2)
	cfg.Platform.MaxReservedOutputTokens = ptr64(10)
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

// R4 sentinel semantics end to end: an explicit "0" in the trusted envelope is
// a zero cap — every request is blocked with the contractual 429 before
// upstream — and a forged all-zeros envelope parses as zero-quota and is
// blocked the same way instead of minting unlimited traffic. Table-driven;
// run under -race.
func TestExplicitZeroRateLimitCapsBlockBeforeUpstream(t *testing.T) {
	zeroHeaders := []string{
		identity.HeaderOrgRateLimitRequests,
		identity.HeaderOrgRateLimitTotalPromptTokens,
		identity.HeaderOrgRateLimitUncachedPromptTokens,
		identity.HeaderOrgRateLimitGeneratedTokens,
		identity.HeaderOwnerRateLimitRequests,
		identity.HeaderOwnerRateLimitTotalPromptTokens,
		identity.HeaderOwnerRateLimitUncachedPromptTokens,
		identity.HeaderOwnerRateLimitGeneratedTokens,
	}
	for _, tc := range []struct {
		name   string
		mutate func(*http.Request)
	}{
		{
			name: "zero organization requests cap",
			mutate: func(req *http.Request) {
				req.Header.Set(identity.HeaderOrgRateLimitRequests, "0")
			},
		},
		{
			name: "zero organization generated-tokens cap",
			mutate: func(req *http.Request) {
				req.Header.Set(identity.HeaderOrgRateLimitGeneratedTokens, "0")
			},
		},
		{
			name: "zero owner requests cap",
			mutate: func(req *http.Request) {
				req.Header.Set(identity.HeaderOwnerRateLimitRequests, "0")
			},
		},
		{
			// The forged-zeros hazard: a complete envelope of all zeros used to
			// parse as unlimited; it must now block (fail-closed).
			name: "forged all-zeros envelope",
			mutate: func(req *http.Request) {
				for _, header := range zeroHeaders {
					req.Header.Set(header, "0")
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits int
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hits++; w.WriteHeader(200) }))
			defer backend.Close()
			up, _ := url.Parse(backend.URL)
			mr := miniredis.RunT(t)
			cfg := proxyAdmissionConfig(2)
			c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = c.Close() })
			s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
			req := sharedRequest(up)
			tc.mutate(req)
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusTooManyRequests {
				t.Fatalf("status=%d, want the contractual 429 at the zero cap", rr.Code)
			}
			if hits != 0 {
				t.Fatal("zero-capped request reached upstream")
			}
		})
	}
}

func TestAdmissionRetryAfterCeilsRemainingFixedWindow(t *testing.T) {
	s := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{})
	rr := httptest.NewRecorder()
	s.writeAdmissionError(rr, "req-retry-after", &admission.Rejected{
		Scope: "contract_organization", Dimension: "requests", Contractual: true,
		RetryAfter: 1500 * time.Millisecond,
	})
	if got := rr.Header().Get("Retry-After"); got != "2" {
		t.Fatalf("Retry-After=%q, want ceil(1.5s)=2", got)
	}
	if got := rr.Header().Get("X-Request-Id"); got != "req-retry-after" {
		t.Fatalf("X-Request-Id=%q, want the request id echoed on the rejection response", got)
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

// rateLimitsEqual compares two parsed quota envelopes field-by-field: a nil
// field (absent header = unlimited) equals only nil, and an explicit zero cap
// equals an explicit zero cap. (RateLimits holds *int64, so != would compare
// pointer identity and never match.)
func rateLimitsEqual(a, b admission.RateLimits) bool {
	equal := func(x, y *int64) bool {
		if x == nil || y == nil {
			return x == y
		}
		return *x == *y
	}
	return equal(a.Requests, b.Requests) && equal(a.TotalPromptTokens, b.TotalPromptTokens) &&
		equal(a.UncachedPromptTokens, b.UncachedPromptTokens) && equal(a.GeneratedTokens, b.GeneratedTokens)
}

// R4 x R7 reconciliation for the trusted quota envelope, pinned at the
// parser. R4: every limit header is per-field — absent parses as nil =
// unlimited, explicit "0" parses as a zero cap (an all-zeros envelope is a
// forged zero-quota envelope, blocked fail-closed by admission, never
// unlimited). R7: the completeness gate covers the IDENTITY anchor only —
// X-Saturn-Owner-Id; limit headers without that anchor, a malformed present
// value, and an absent policy all fail closed. R8: there is no legacy
// envelope (see TestLegacyQuotaHeadersAreIgnored).
func TestTrustedRateLimitPolicyParsing(t *testing.T) {
	// R7 structural pins — fail closed.
	for _, tc := range []struct {
		name string
		id   identity.Identity
	}{
		{
			name: "no anchor and no headers",
			id:   identity.Identity{Gateway: true},
		},
		{
			name: "scoped headers without owner id",
			id: identity.Identity{Gateway: true,
				OrgRateLimitRequests: "5", OrgRateLimitTotalPromptTokens: "100",
				OrgRateLimitUncachedPromptTokens: "100", OrgRateLimitGeneratedTokens: "20",
				OwnerRateLimitRequests: "5", OwnerRateLimitTotalPromptTokens: "100",
				OwnerRateLimitUncachedPromptTokens: "100", OwnerRateLimitGeneratedTokens: "20"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := parseTrustedRateLimits(tc.id); err == nil {
				t.Fatal("structurally incomplete envelope was accepted")
			}
		})
	}

	// R4 per-field pins — the identity anchor present, limit headers absent
	// where unset: each absent header parses to nil (unlimited)
	// independently. An owner id with NO headers is the all-unset org.
	organization, owner, err := parseTrustedRateLimits(identity.Identity{OwnerID: "owner-1"})
	if err != nil || !rateLimitsEqual(organization, admission.RateLimits{}) || !rateLimitsEqual(owner, admission.RateLimits{}) {
		t.Fatalf("owner identity with all limits unset = org=%+v owner=%+v, %v; absent headers must parse as unlimited", organization, owner, err)
	}
	organization, owner, err = parseTrustedRateLimits(identity.Identity{
		OwnerID:              "owner-1",
		OrgRateLimitRequests: "5", OwnerRateLimitGeneratedTokens: "7",
	})
	if err != nil ||
		!rateLimitsEqual(organization, admission.RateLimits{Requests: ptr64(5)}) ||
		!rateLimitsEqual(owner, admission.RateLimits{GeneratedTokens: ptr64(7)}) {
		t.Fatalf("per-field parse = org=%+v owner=%+v, %v; absent fields must be nil (unlimited)", organization, owner, err)
	}

	// FLIPPED by R4: the all-zeros envelope used to parse as "explicit
	// unlimited" (every field 0); it now parses as zero-quota — every field an
	// explicit zero cap — so admission blocks it.
	organization, owner, err = parseTrustedRateLimits(identity.Identity{
		Gateway: true, OwnerID: "owner-1",
		OrgRateLimitRequests: "0", OrgRateLimitTotalPromptTokens: "0",
		OrgRateLimitUncachedPromptTokens: "0", OrgRateLimitGeneratedTokens: "0",
		OwnerRateLimitRequests: "0", OwnerRateLimitTotalPromptTokens: "0",
		OwnerRateLimitUncachedPromptTokens: "0", OwnerRateLimitGeneratedTokens: "0",
	})
	zeroCap := admission.RateLimits{Requests: ptr64(0), TotalPromptTokens: ptr64(0),
		UncachedPromptTokens: ptr64(0), GeneratedTokens: ptr64(0)}
	if err != nil || !rateLimitsEqual(organization, zeroCap) || !rateLimitsEqual(owner, zeroCap) {
		t.Fatalf("all-zeros envelope must parse as zero-quota: org=%+v owner=%+v, %v", organization, owner, err)
	}

	// Malformed PRESENT values fail closed (R7's malformed clause); the per-field rule governs absence, not garbage.
	for _, tc := range []struct {
		name string
		id   identity.Identity
	}{
		{
			name: "non-numeric scoped header",
			id: identity.Identity{Gateway: true, OwnerID: "owner-1",
				OrgRateLimitRequests: "not-a-number"},
		},
		{
			name: "negative scoped header",
			id: identity.Identity{Gateway: true, OwnerID: "owner-1",
				OrgRateLimitRequests: "-1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := parseTrustedRateLimits(tc.id); err == nil {
				t.Fatal("malformed present limit value was accepted")
			}
		})
	}
}

// R7 structural pin, end to end: the completeness gate covers the identity
// anchor, so limit headers without their anchor are refused (503) before
// admission. Absent limit headers with the anchor present are NOT a violation
// — they parse as unlimited (see TestPartialRateLimitHeadersAdmitAsUnlimited).
func TestTrustedRateLimitPolicyWithoutIdentityAnchorFailsClosed(t *testing.T) {
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
			name: "no anchor and no headers",
			mutate: func(req *http.Request) {
				req.Header.Del(identity.HeaderOwnerID)
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
			name: "scoped headers without owner id",
			mutate: func(req *http.Request) {
				req.Header.Del(identity.HeaderOwnerID)
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

// legacyQuotaTestHeaders are the five single-scope quota headers R8 removed.
// They are spelled out literally because phoebe defines no trusted-header
// constants for them.
var legacyQuotaTestHeaders = []string{
	"X-Saturn-Service-Tier",
	"X-Saturn-Rate-Limit-Requests",
	"X-Saturn-Rate-Limit-Total-Prompt-Tokens",
	"X-Saturn-Rate-Limit-Uncached-Prompt-Tokens",
	"X-Saturn-Rate-Limit-Generated-Tokens",
}

func deleteScopedRateLimitHeaders(req *http.Request) {
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
}

// R8 hard cut, end to end: the five legacy single-scope quota headers
// (X-Saturn-Service-Tier and X-Saturn-Rate-Limit-*) are not an envelope.
// Legacy headers neither admit a request nor bind or break its limits:
//   - legacy-only (no X-Saturn-Owner-Id, no scoped header) has no trusted
//     policy and fails closed (503) before reaching the upstream;
//   - with the owner-id anchor and no scoped limits, legacy zero caps are
//     ignored and the request is unlimited and forwarded;
//   - with the owner-id anchor and scoped limits, legacy zero or malformed
//     values neither refuse the request nor change the parsed scoped limits.
//
// The last two cases are the ones that would fail if phoebe read the legacy
// headers; the first only shows that a missing anchor fails closed.
func TestLegacyQuotaHeadersAreIgnored(t *testing.T) {
	newServer := func(t *testing.T) (*Server, *url.URL, *int) {
		t.Helper()
		hits := new(int)
		backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			*hits++
			_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
		}))
		t.Cleanup(backend.Close)
		up, _ := url.Parse(backend.URL)
		cfg := proxyAdmissionConfig(1)
		mr := miniredis.RunT(t)
		c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = c.Close() })
		s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
		return s, up, hits
	}

	t.Run("legacy only without owner anchor fails closed", func(t *testing.T) {
		s, up, hits := newServer(t)
		req := sharedRequest(up)
		req.Header.Del(identity.HeaderOwnerID)
		deleteScopedRateLimitHeaders(req)
		req.Header.Set("X-Saturn-Service-Tier", "default")
		for _, header := range legacyQuotaTestHeaders[1:] {
			req.Header.Set(header, "1000000")
		}
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("status=%d, want 503 — legacy-only quota headers must not satisfy the policy gate", rr.Code)
		}
		if *hits != 0 {
			t.Fatalf("legacy-only request reached upstream %d times, want 0", *hits)
		}
	})

	t.Run("owner anchor without scoped limits ignores legacy zero caps", func(t *testing.T) {
		s, up, hits := newServer(t)
		req := sharedRequest(up)
		deleteScopedRateLimitHeaders(req)
		req.Header.Set("X-Saturn-Service-Tier", "default")
		for _, header := range legacyQuotaTestHeaders[1:] {
			req.Header.Set(header, "0")
		}
		organization, owner, err := parseTrustedRateLimits(identity.FromRequest(req))
		if err != nil {
			t.Fatalf("parseTrustedRateLimits: %v — legacy headers must not affect an anchored request", err)
		}
		for name, limits := range map[string]admission.RateLimits{"organization": organization, "owner": owner} {
			if limits.Requests != nil || limits.TotalPromptTokens != nil || limits.UncachedPromptTokens != nil || limits.GeneratedTokens != nil {
				t.Fatalf("%s limits=%+v, want all unlimited (nil) — legacy zero caps were read", name, limits)
			}
		}
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status=%d, want 200 — legacy zero caps must not block the request", rr.Code)
		}
		if *hits != 1 {
			t.Fatalf("upstream hits=%d, want 1", *hits)
		}
	})

	t.Run("owner anchor with scoped limits ignores legacy zero and malformed values", func(t *testing.T) {
		s, up, hits := newServer(t)
		req := sharedRequest(up)
		req.Header.Set("X-Saturn-Service-Tier", "gold")
		req.Header.Set("X-Saturn-Rate-Limit-Requests", "0")
		req.Header.Set("X-Saturn-Rate-Limit-Total-Prompt-Tokens", "0")
		req.Header.Set("X-Saturn-Rate-Limit-Uncached-Prompt-Tokens", "-1")
		req.Header.Set("X-Saturn-Rate-Limit-Generated-Tokens", "not-a-number")
		organization, owner, err := parseTrustedRateLimits(identity.FromRequest(req))
		if err != nil {
			t.Fatalf("parseTrustedRateLimits: %v — malformed legacy headers must not break an anchored request", err)
		}
		for name, limits := range map[string]admission.RateLimits{"organization": organization, "owner": owner} {
			for field, got := range map[string]*int64{
				"Requests": limits.Requests, "TotalPromptTokens": limits.TotalPromptTokens,
				"UncachedPromptTokens": limits.UncachedPromptTokens, "GeneratedTokens": limits.GeneratedTokens,
			} {
				if got == nil || *got != 1000000 {
					t.Fatalf("%s %s=%v, want the scoped 1000000 — legacy values leaked into the parsed limits", name, field, got)
				}
			}
		}
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status=%d, want 200 — legacy zero/malformed values must not refuse the request", rr.Code)
		}
		if *hits != 1 {
			t.Fatalf("upstream hits=%d, want 1", *hits)
		}
	})
}

// R8 cutover diagnostic: a legacy-only request still gets the unchanged,
// opaque 503, but the log names the likely root cause (a producer that still
// stamps only the removed legacy headers). A request with no policy and no
// legacy headers must not carry the marker. The legacy headers only ever
// reach a log line, never a trust or limit decision.
func TestLegacyOnlyQuotaHeadersLogPreR8ProducerMarker(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	cfg := proxyAdmissionConfig(1)
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	for _, tc := range []struct {
		name       string
		legacy     bool
		wantMarker bool
	}{
		{name: "legacy headers present", legacy: true, wantMarker: true},
		{name: "no policy at all", legacy: false, wantMarker: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var warnBuf, errBuf bytes.Buffer
			logger := &logging.Logger{Warn: log.New(&warnBuf, "", 0), Error: log.New(&errBuf, "", 0)}
			s := New(&config.Settings{Admission: cfg}, logger, &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
			req := sharedRequest(up)
			req.Header.Del(identity.HeaderOwnerID)
			deleteScopedRateLimitHeaders(req)
			if tc.legacy {
				req.Header.Set("X-Saturn-Service-Tier", "default")
				for _, header := range legacyQuotaTestHeaders[1:] {
					req.Header.Set(header, "1000000")
				}
			}
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d, want 503", rr.Code)
			}
			if body := strings.TrimSpace(rr.Body.String()); body != "shared inference policy unavailable" {
				t.Fatalf("response body=%q, want the unchanged opaque body", body)
			}
			if !strings.Contains(errBuf.String(), "incomplete trusted shared-inference rate-limit policy") {
				t.Fatalf("error log=%q, want the policy error", errBuf.String())
			}
			if got := strings.Contains(warnBuf.String(), "legacy_envelope_present=true"); got != tc.wantMarker {
				t.Fatalf("legacy marker in warn log=%v, want %v; warn log=%q", got, tc.wantMarker, warnBuf.String())
			}
		})
	}
}

// R4 x R7 reconciliation, end to end: with the identity anchor present, Atlas
// may omit any limit header whose UsageLimit is unset — the absent fields
// parse as unlimited and the request is admitted and forwarded. This is the
// shape the sister Atlas branch (hugo/r4-atlas-omit-unset) produces; under an
// all-or-nothing gate every shared request of such an org 503'd.
func TestPartialRateLimitHeadersAdmitAsUnlimited(t *testing.T) {
	scopedHeaders := []string{
		identity.HeaderOrgRateLimitRequests,
		identity.HeaderOrgRateLimitTotalPromptTokens,
		identity.HeaderOrgRateLimitUncachedPromptTokens,
		identity.HeaderOrgRateLimitGeneratedTokens,
		identity.HeaderOwnerRateLimitRequests,
		identity.HeaderOwnerRateLimitTotalPromptTokens,
		identity.HeaderOwnerRateLimitUncachedPromptTokens,
		identity.HeaderOwnerRateLimitGeneratedTokens,
	}
	for _, tc := range []struct {
		name   string
		mutate func(*http.Request)
	}{
		{
			name: "owner id only, every limit unset",
			mutate: func(req *http.Request) {
				for _, header := range scopedHeaders {
					req.Header.Del(header)
				}
			},
		},
		{
			name: "single org header present",
			mutate: func(req *http.Request) {
				for _, header := range scopedHeaders {
					req.Header.Del(header)
				}
				req.Header.Set(identity.HeaderOrgRateLimitRequests, "1000000")
			},
		},
		{
			name: "org and owner subsets",
			mutate: func(req *http.Request) {
				for _, header := range scopedHeaders {
					req.Header.Del(header)
				}
				req.Header.Set(identity.HeaderOrgRateLimitRequests, "1000000")
				req.Header.Set(identity.HeaderOrgRateLimitGeneratedTokens, "1000000")
				req.Header.Set(identity.HeaderOwnerRateLimitRequests, "1000000")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits int
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits++
				_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
			}))
			defer backend.Close()
			up, _ := url.Parse(backend.URL)
			mr := miniredis.RunT(t)
			cfg := proxyAdmissionConfig(1)
			c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			t.Cleanup(func() { _ = c.Close() })
			s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))

			req := sharedRequest(up)
			tc.mutate(req)
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("status=%d, want 200 — absent limit headers must admit as unlimited", rr.Code)
			}
			if hits != 1 {
				t.Fatalf("upstream hits=%d, want 1", hits)
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
	// A buffered signal, not a close: the charge+probe section below re-issues
	// the stream request once on a window rollover, and the backend handler
	// signals once per request.
	started := make(chan struct{}, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("data: {\"model\":\"model-a\",\"choices\":[]}\n\n"))
		w.(http.Flusher).Flush()
		started <- struct{}{}
		<-r.Context().Done()
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	a := admission.New(c, cfg)
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(a)

	// Release half of this test (spliced at the merge): a plain aborted stream
	// must start, return promptly once the client cancels, and release its
	// lease. The probe half below then pins the conservative reservation the
	// abort retains in the contract windows.
	//
	// R4: the base envelope now stamps large (non-binding) caps instead of
	// zeros, so a complete envelope always creates contract scopes and this
	// abort's conservative charge would land in org-a/owner-a's windows and
	// pollute the probes below. The release half pins PHYSICAL lease release
	// only, so give its request a disjoint org/owner identity.
	ctx, cancel := context.WithCancel(context.Background())
	req := sharedRequest(up).WithContext(ctx)
	req.Header.Set(identity.HeaderOrgID, "org-release-check")
	req.Header.Set(identity.HeaderOwnerID, "owner-release-check")
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

	// The aborted stream's retained reservation was charged to the org/owner
	// generated_tokens windows, so each probe reserving one more token must
	// be rejected. That window is a fixed 1-minute bucket (floor(now/60000)):
	// a minute tick between the aborted request's charge and the rejection
	// probes empties the bucket and would spuriously ADMIT them. Capture the
	// bucket around the charge+probe section and, on roll, re-charge and
	// retry the section once.
	probes := []struct {
		name string
		req  admission.Request
		want string
	}{
		{
			name: "organization contract",
			req: admission.Request{Graph: "graph", Organization: "org-a", Owner: "other-owner", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
				OrganizationLimits: admission.RateLimits{GeneratedTokens: ptr64(20)}},
			want: "contract_organization",
		},
		{
			name: "owner contract",
			req: admission.Request{Graph: "graph", Organization: "other-org", Owner: "owner-a", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
				OwnerLimits: admission.RateLimits{GeneratedTokens: ptr64(20)}},
			want: "contract_owner",
		},
	}
	chargeAndProbe := func() (bool, []error) {
		bucketBefore := fixedWindowBucket()
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
		errs := make([]error, len(probes))
		for i, tc := range probes {
			_, errs[i] = a.Admit(context.Background(), tc.req)
		}
		return bucketBefore != fixedWindowBucket(), errs
	}
	rolled, errs := chargeAndProbe()
	if rolled {
		// The window ticked between charge and probes: the probes' admits
		// landed in the fresh bucket and charged it themselves. Flush the
		// store back to the exact post-rollover empty-window state, then
		// re-charge and retry once.
		mr.FlushAll()
		rolled, errs = chargeAndProbe()
		if rolled {
			t.Skip("fixed 1-minute admission window rolled twice during the test; the probes cannot be made deterministic — retry")
		}
	}
	for i, tc := range probes {
		var rejected *admission.Rejected
		if !errors.As(errs[i], &rejected) || rejected.Scope != tc.want || rejected.Dimension != "generated_tokens" {
			t.Errorf("%s: err=%v, want %s generated_tokens rejection", tc.name, errs[i], tc.want)
		}
	}
	lease, err := a.Admit(context.Background(), admission.Request{Graph: "graph", Organization: "other", Model: "m", PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1})
	if err != nil {
		t.Fatalf("aborted stream leaked reservation: %v", err)
	}
	_ = lease.Complete(context.Background(), 0)
}

func TestAdmissionChargesUnknownUsageOnPreHeaderAbort(t *testing.T) {
	// A buffered signal, not a close: the charge+probe section below re-issues
	// the request once on a window rollover, and the backend handler signals
	// once per request.
	started := make(chan struct{}, 1)
	unblock := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
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
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(a)

	// The pre-header abort retained the conservative reservation in the
	// org/owner generated_tokens windows, so each probe reserving one more
	// token must be rejected. That window is a fixed 1-minute bucket
	// (floor(now/60000)): a minute tick between the aborted request's charge
	// and the rejection probes empties the bucket and would spuriously ADMIT
	// them. Capture the bucket around the charge+probe section and, on roll,
	// re-charge and retry the section once.
	probes := []struct {
		name string
		req  admission.Request
		want string
	}{
		{
			name: "organization contract",
			req: admission.Request{Graph: "graph", Organization: "org-a", Owner: "other-owner", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
				OrganizationLimits: admission.RateLimits{GeneratedTokens: ptr64(20)}},
			want: "contract_organization",
		},
		{
			name: "owner contract",
			req: admission.Request{Graph: "graph", Organization: "other-org", Owner: "owner-a", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
				OwnerLimits: admission.RateLimits{GeneratedTokens: ptr64(20)}},
			want: "contract_owner",
		},
	}
	chargeAndProbe := func() (bool, []error) {
		bucketBefore := fixedWindowBucket()
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
		errs := make([]error, len(probes))
		for i, tc := range probes {
			_, errs[i] = a.Admit(context.Background(), tc.req)
		}
		return bucketBefore != fixedWindowBucket(), errs
	}
	rolled, errs := chargeAndProbe()
	if rolled {
		// The window ticked between charge and probes: the probes' admits
		// landed in the fresh bucket and charged it themselves. Flush the
		// store back to the exact post-rollover empty-window state, then
		// re-charge and retry once.
		mr.FlushAll()
		rolled, errs = chargeAndProbe()
		if rolled {
			t.Skip("fixed 1-minute admission window rolled twice during the test; the probes cannot be made deterministic — retry")
		}
	}
	for i, tc := range probes {
		var rejected *admission.Rejected
		if !errors.As(errs[i], &rejected) || rejected.Scope != tc.want || rejected.Dimension != "generated_tokens" {
			t.Errorf("%s: err=%v, want %s generated_tokens rejection", tc.name, errs[i], tc.want)
		}
	}
}

// An upstream that consumes the full request and then resets before writing
// response headers fails RoundTrip with EOF — not a client cancel. The engine's
// usage is indeterminate, so the conservative reservation must be retained in
// the org/owner contract windows and the request must be metered like the
// pre-header abort path — a zero-token attributable event — but classified as
// an upstream fault, never as a client abort.
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
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), em).WithAdmitter(a)

	// The reset retained the conservative reservation in the org/owner
	// generated_tokens windows, so each probe reserving one more token must
	// be rejected. That window is a fixed 1-minute bucket (floor(now/60000)):
	// a minute tick between the reset request's charge and the rejection
	// probes empties the bucket and would spuriously ADMIT them. Capture the
	// bucket around the charge+probe section and, on roll, re-charge and
	// retry the section once.
	probes := []struct {
		name string
		req  admission.Request
		want string
	}{
		{
			name: "organization contract",
			req: admission.Request{Graph: "graph", Organization: "org-a", Owner: "other-owner", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
				OrganizationLimits: admission.RateLimits{GeneratedTokens: ptr64(20)}},
			want: "contract_organization",
		},
		{
			name: "owner contract",
			req: admission.Request{Graph: "graph", Organization: "other-org", Owner: "owner-a", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
				OwnerLimits: admission.RateLimits{GeneratedTokens: ptr64(20)}},
			want: "contract_owner",
		},
	}
	chargeAndProbe := func() (bool, []error) {
		bucketBefore := fixedWindowBucket()
		// The emitter accumulates across attempts (and this attempt's emit
		// lands synchronously inside ServeHTTP), so snapshot the count BEFORE
		// the request and assert on the events THIS attempt produced.
		before := em.count()
		req := sharedRequest(up)
		req.Header.Set(identity.HeaderOrgRateLimitGeneratedTokens, "20")
		req.Header.Set(identity.HeaderOwnerRateLimitGeneratedTokens, "20")
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusBadGateway {
			t.Fatalf("status=%d, want 502", rr.Code)
		}

		events := em.waitForEvents(before+1, time.Second)
		mine := events[before:]
		if len(mine) != 1 || mine[0].PromptTokens != 0 || mine[0].CompletionTokens != 0 {
			t.Fatalf("metering events=%+v, want the same zero-token attributable event as the abort path", mine)
		}
		// The always-record policy persists the attempt: explicit UsageFound=false
		// and the 502 status the client saw, so reconciliation can distinguish it
		// from a legitimate zero-token completion.
		if mine[0].UsageFound || mine[0].StatusCode != http.StatusBadGateway {
			t.Fatalf("upstream fault event must be a usage-missing 502 attempt: %+v", mine[0])
		}
		// The reset is an upstream fault, not a client abort: the event must be
		// attributable without billing_event.aborted misrecording it as one.
		if mine[0].Aborted {
			t.Fatalf("upstream fault event must have Aborted=false: %+v", mine[0])
		}

		errs := make([]error, len(probes))
		for i, tc := range probes {
			_, errs[i] = a.Admit(context.Background(), tc.req)
		}
		return bucketBefore != fixedWindowBucket(), errs
	}
	rolled, errs := chargeAndProbe()
	if rolled {
		// The window ticked between charge and probes: the probes' admits
		// landed in the fresh bucket and charged it themselves. Flush the
		// store back to the exact post-rollover empty-window state, then
		// re-charge and retry once.
		mr.FlushAll()
		rolled, errs = chargeAndProbe()
		if rolled {
			t.Skip("fixed 1-minute admission window rolled twice during the test; the probes cannot be made deterministic — retry")
		}
	}
	for i, tc := range probes {
		var rejected *admission.Rejected
		if !errors.As(errs[i], &rejected) || rejected.Scope != tc.want || rejected.Dimension != "generated_tokens" {
			t.Errorf("%s: err=%v, want %s generated_tokens rejection", tc.name, errs[i], tc.want)
		}
	}
	// Physical capacity itself is released; only the conservative contract
	// charges are retained.
	lease, err := a.Admit(context.Background(), admission.Request{Graph: "graph", Organization: "other", Model: "m", PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1})
	if err != nil {
		t.Fatalf("reset upstream leaked physical reservation: %v", err)
	}
	_ = lease.Complete(context.Background(), 0)
}

// The emit-matrix cell the sibling test cannot see: under the always-record
// policy (BillPartialOnAbort is gone) the same upstream reset (UpstreamFault,
// no usage) MUST emit exactly one zero-token, usage-missing row — status
// preserved at 502, Aborted=false — so reconciliation sees the attempt, and
// the Warn reconciliation log must say the unmetered attempt was recorded,
// not that it was skipped.
func TestAdmissionUpstreamResetAlwaysEmitsZeroTokenRow(t *testing.T) {
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
	t.Cleanup(func() { _ = c.Close() })
	a := admission.New(c, cfg)
	em := &recordingEmitter{}
	var warnBuf, errBuf bytes.Buffer
	logger := &logging.Logger{
		Warn:  log.New(&warnBuf, "", 0),
		Error: log.New(&errBuf, "", 0),
	}
	s := New(&config.Settings{Admission: cfg}, logger, em).WithAdmitter(a)
	req := sharedRequest(up)
	req.Header.Set(identity.HeaderOrgRateLimitGeneratedTokens, "20")
	req.Header.Set(identity.HeaderOwnerRateLimitGeneratedTokens, "20")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status=%d, want 502", rr.Code)
	}
	events := em.waitForEvents(1, time.Second)
	if len(events) != 1 {
		t.Fatalf("always-record policy emitted %d events for an upstream fault with no usage, want 1", len(events))
	}
	e := events[0]
	if e.UsageFound || e.Aborted || e.StatusCode != http.StatusBadGateway || e.PromptTokens != 0 || e.CompletionTokens != 0 {
		t.Fatalf("upstream fault event = %+v, want a zero-token usage-missing non-aborted 502 attempt", e)
	}
	if !strings.Contains(warnBuf.String(), "no usage captured; recording unmetered attempt") {
		t.Fatalf("the emit must log the reconciliation Warn, got: %q", warnBuf.String())
	}
}

// A verifiable pre-write dial failure (connection refused) proves the request
// never left the process: the lease settles Complete(0) — physical capacity is
// released and the generated/prompt contract windows are NOT charged. The
// requests window IS charged: admitScript charges RPM at admit time and only
// the compensating abandon (an Admit-path cleanup) ever subtracts it, so a
// dial-failed request counts against the contract's requests-per-window. This
// pins the contract as built; changing the settlement is a separate decision.
func TestAdmissionDialFailureChargesOnlyRequestsWindow(t *testing.T) {
	up, _ := url.Parse("http://127.0.0.1:1") // nothing listening: dial is refused
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(1)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	a := admission.New(c, cfg)
	em := &recordingEmitter{}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), em).WithAdmitter(a)

	// The requests window was charged at admit time and never refunded: the
	// dial-failed request's own org (resp. owner) is already at its
	// requests-per-minute limit of 1.
	probes := []struct {
		name string
		req  admission.Request
		want string
	}{
		{
			name: "organization requests window",
			req: admission.Request{Graph: "graph", Organization: "org-a", Owner: "other-owner", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
				OrganizationLimits: admission.RateLimits{Requests: ptr64(1)}},
			want: "contract_organization",
		},
		{
			name: "owner requests window",
			req: admission.Request{Graph: "graph", Organization: "other-org", Owner: "owner-a", Model: "m",
				PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
				OwnerLimits: admission.RateLimits{Requests: ptr64(1)}},
			want: "contract_owner",
		},
	}
	// The window is a fixed 1-minute bucket (floor(now/60000)): a minute tick
	// between the dial-failed request's charge and the rejection probes empties
	// the bucket and would spuriously ADMIT them. Capture the bucket around the
	// charge+probe section and, on roll, re-charge and retry the section once.
	chargeAndProbe := func() (bool, []error) {
		bucketBefore := fixedWindowBucket()
		// The emitter accumulates across attempts (and this attempt's emit
		// lands synchronously inside ServeHTTP), so snapshot the count BEFORE
		// the request and assert on the events THIS attempt produced.
		before := em.count()
		req := sharedRequest(up)
		req.Header.Set(identity.HeaderOrgRateLimitRequests, "1")
		req.Header.Set(identity.HeaderOwnerRateLimitRequests, "1")
		req.Header.Set(identity.HeaderOrgRateLimitGeneratedTokens, "20")
		req.Header.Set(identity.HeaderOwnerRateLimitGeneratedTokens, "20")
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusBadGateway {
			t.Fatalf("status=%d, want 502", rr.Code)
		}
		errs := make([]error, len(probes))
		for i, tc := range probes {
			_, errs[i] = a.Admit(context.Background(), tc.req)
		}
		// No engine work was possible, so the lease settles zero — but the
		// attempt itself is still persisted: the always-record policy emits a
		// zero-token, usage-missing 502 row for every transport failure
		// (including a verified pre-write dial failure), keeping it visible to
		// reconciliation.
		events := em.waitForEvents(before+1, time.Second)
		mine := events[before:]
		if len(mine) != 1 || mine[0].UsageFound || mine[0].Aborted || mine[0].StatusCode != http.StatusBadGateway {
			t.Fatalf("dial failure events=%+v, want one zero-token usage-missing non-aborted 502 attempt", mine)
		}
		return bucketBefore != fixedWindowBucket(), errs
	}
	rolled, errs := chargeAndProbe()
	if rolled {
		// The window ticked between charge and probes: the probes' admits
		// landed in the fresh bucket and charged it themselves. Flush the
		// store back to the exact post-rollover empty-window state, then
		// re-charge and retry once.
		mr.FlushAll()
		rolled, errs = chargeAndProbe()
		if rolled {
			t.Skip("fixed 1-minute admission window rolled twice during the test; the probes cannot be made deterministic — retry")
		}
	}
	for i, tc := range probes {
		var rejected *admission.Rejected
		if !errors.As(errs[i], &rejected) || rejected.Scope != tc.want || rejected.Dimension != "requests" {
			t.Errorf("%s: err=%v, want %s requests rejection", tc.name, errs[i], tc.want)
		}
	}

	// The generated and prompt windows were NOT charged: the same organization
	// reserving exactly the dial-failed request's conservative estimate (7
	// estimated input tokens, 20 reserved output tokens) still fits its
	// contract. A phantom conservative charge would have filled the window and
	// rejected this probe.
	lease, err := a.Admit(context.Background(), admission.Request{Graph: "graph", Organization: "org-a", Owner: "other-owner", Model: "m",
		PromptBytes: 1, EstimatedInputTokens: 7, ReservedOutputTokens: 20,
		OrganizationLimits: admission.RateLimits{GeneratedTokens: ptr64(20), TotalPromptTokens: ptr64(7), UncachedPromptTokens: ptr64(7)}})
	if err != nil {
		t.Fatalf("dial failure left a phantom prompt/generated charge: %v", err)
	}
	_ = lease.Complete(context.Background(), 0)
}

// Broken trusted identity/policy fails closed BEFORE the Valkey fail-open
// boundary — and must keep failing closed when the admission store is down.
// Only a complete, valid envelope may bypass a dead store.
func TestStoreDownStillFailsClosedOnBrokenIdentityAndPolicy(t *testing.T) {
	var hits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":7,"completion_tokens":3}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	cfg := proxyAdmissionConfig(1)
	// The admitter points at a closed port: every Admit returns ErrUnavailable,
	// the exact condition the fail-open bypass exists for.
	newDeadStoreServer := func() *Server {
		client := admission.NewValkeyClient("127.0.0.1:1")
		t.Cleanup(func() { _ = client.Close() })
		return New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).
			WithAdmitter(admission.New(client, cfg))
	}

	for _, tc := range []struct {
		name   string
		mutate func(*http.Request)
	}{
		{
			name:   "missing organization",
			mutate: func(req *http.Request) { req.Header.Del(identity.HeaderOrgID) },
		},
		{
			name:   "malformed policy envelope",
			mutate: func(req *http.Request) { req.Header.Set(identity.HeaderOrgRateLimitRequests, "not-a-number") },
		},
		{
			// R4 x R7: a single ABSENT limit header is unlimited, not a
			// violation — the structural 503 shape is headers without their
			// identity anchor.
			name:   "headers without identity anchor",
			mutate: func(req *http.Request) { req.Header.Del(identity.HeaderOwnerID) },
		},
		{
			name:   "degenerate upstream graph",
			mutate: func(req *http.Request) { req.Header.Set(identity.HeaderUpstream, ":8000") },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := sharedRequest(up)
			tc.mutate(req)
			rr := httptest.NewRecorder()
			newDeadStoreServer().Handler().ServeHTTP(rr, req)
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("status=%d with the admission store down, want the fail-closed 503 — a fail-open bypass would forward", rr.Code)
			}
		})
	}
	if hits != 0 {
		t.Fatalf("broken identity/policy reached the upstream %d times under a store outage; the fail-open bypass was taken", hits)
	}

	// Positive control: a complete valid envelope with the store down still
	// bypasses the distributed gate and is served.
	rr := httptest.NewRecorder()
	newDeadStoreServer().Handler().ServeHTTP(rr, sharedRequest(up))
	if rr.Code != http.StatusOK {
		t.Fatalf("control status=%d, want 200 (valid request bypasses a dead store)", rr.Code)
	}
	if hits != 1 {
		t.Fatalf("upstream hits=%d, want exactly the valid control request", hits)
	}
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

// TestPerResourceServingIdentityFailsClosedWhenInconsistent — merged #50×#51
// contract. phoebe#51's standalone coherence gate used to 503 every
// serving-metadata combination it considered inconsistent. phoebe#50 made the
// served-model allow-list a legitimate DEDICATED-route shape (bound
// endpoints), and its model-binding route gate is now the single checker: it
// validates the mode, binds the request body to the injected allow-list, and
// fails malformed modes and allow-list-less shared routes closed with the
// generic 404 — before admission and before upstream. Those two shapes are
// legitimate now and must forward normally.
func TestPerResourceServingIdentityFailsClosedWhenInconsistent(t *testing.T) {
	tests := []struct {
		name        string
		servingMode string
		servedModel string
		wantStatus  int
	}{
		// Ruling #19: an absent serving mode is an edge-contract bug, not
		// dedicated — refused at the route gate like a malformed one.
		{name: "model with absent mode", servedModel: "model-a", wantStatus: http.StatusNotFound},
		{name: "model with dedicated mode", servingMode: "dedicated", servedModel: "model-a", wantStatus: http.StatusOK},
		{name: "model with unknown mode", servingMode: "shraed", servedModel: "model-a", wantStatus: http.StatusNotFound},
		{name: "shared mode without model", servingMode: "shared", wantStatus: http.StatusNotFound},
		{name: "unknown mode without model", servingMode: "shraed", wantStatus: http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var upstreamHits int
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				upstreamHits++
				w.WriteHeader(http.StatusOK)
			}))
			defer backend.Close()
			up, _ := url.Parse(backend.URL)
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
			if rr.Code != tc.wantStatus {
				t.Fatalf("status=%d, want %d", rr.Code, tc.wantStatus)
			}
			if a.calls != 0 {
				t.Fatalf("admission calls=%d, want 0 — no shape here reaches admission", a.calls)
			}
			if tc.wantStatus != http.StatusOK && upstreamHits != 0 {
				t.Fatalf("upstream hits=%d, want 0 for refused shapes", upstreamHits)
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
	// Every client-controllable worker/rank selection header — direct
	// routing bypasses load- and cache-aware scheduling.
	for _, header := range []string{
		"X-Dynamo-Worker-Instance-ID", "X-Dynamo-Prefill-Instance-ID",
		"X-Dynamo-DP-Rank", "X-Dynamo-Prefill-DP-Rank",
		// Dynamo 1.4 retains these aliases for compatibility.
		"X-Worker-Instance-ID", "X-Prefill-Instance-ID",
		"X-DP-Rank", "X-Data-Parallel-Rank", "X-Prefill-DP-Rank",
	} {
		req.Header.Set(header, "99")
	}
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
	for _, header := range []string{
		"X-Dynamo-Worker-Instance-ID", "X-Dynamo-Prefill-Instance-ID",
		"X-Dynamo-DP-Rank", "X-Dynamo-Prefill-DP-Rank",
		"X-Worker-Instance-ID", "X-Prefill-Instance-ID",
		"X-DP-Rank", "X-Data-Parallel-Rank", "X-Prefill-DP-Rank",
	} {
		if got := forwarded.Header.Get(header); got != "" {
			t.Fatalf("forwarded direct-worker header %s=%q, want it stripped", header, got)
		}
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

// Only operator config selects a lane: an org with no OrganizationLanes entry
// stays on the default lane even when a higher-priority lane is configured,
// another org is mapped to it, and the client asks for that priority.
func TestUnmappedOrgStaysOnDefaultLaneDynamoHints(t *testing.T) {
	seen := make(chan *http.Request, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Clone(r.Context())
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	mr := miniredis.RunT(t)
	cfg := proxyAdmissionConfig(2)
	cfg.Lanes = map[string]config.AdmissionLane{
		"default": {Weight: 1},
		"gold":    {Weight: 1, DynamoPriority: 11, DynamoStrictPriority: 4},
	}
	cfg.OrganizationLanes = map[string]string{"org-z": "gold"}
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(c, cfg))
	req := sharedRequest(up) // org-a, full scoped envelope
	req.Header.Set("X-Dynamo-Request-Priority", "11")
	req.Header.Set("X-Dynamo-Request-Strict-Priority", "4")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d", rr.Code)
	}
	forwarded := <-seen
	if got := forwarded.Header.Get("X-Dynamo-Request-Priority"); got != "0" {
		t.Fatalf("forwarded priority header=%q, want 0 — an unmapped org must not get the gold lane's 11", got)
	}
	if got := forwarded.Header.Get("X-Dynamo-Request-Strict-Priority"); got != "0" {
		t.Fatalf("forwarded strict-priority header=%q, want 0 — an unmapped org must not get the gold lane's 4", got)
	}
}

// The per-request admission error logs must aggregate, not flood: the first
// occurrence logs at onset, then every 100th, each carrying the number of
// suppressed occurrences since the previous line.
func TestSampledErrorLog(t *testing.T) {
	var buf bytes.Buffer
	logger := &logging.Logger{Error: log.New(&buf, "", 0)}
	var site sampledErrorLog
	for i := 0; i < 250; i++ {
		site.logf(logger, "admission: distributed gate unavailable; bypassing request_id=%d", i)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("logged %d lines for 250 occurrences, want 3 (1st + every 100th): %q", len(lines), lines)
	}
	if !strings.Contains(lines[0], "request_id=0") || strings.Contains(lines[0], "suppressed") {
		t.Fatalf("first occurrence must log plainly: %q", lines[0])
	}
	if !strings.Contains(lines[1], "+98 similar suppressed") {
		t.Fatalf("100th occurrence must report 98 suppressed: %q", lines[1])
	}
	if !strings.Contains(lines[2], "+99 similar suppressed") {
		t.Fatalf("200th occurrence must report 99 suppressed: %q", lines[2])
	}
}

// The settlement and cold-hold release failure sites route through their own
// sampledErrorLog samplers on Server, with the same contract as the bypass
// sampler: a store outage that fails every request's settlement still logs its
// onset (1st + every 100th occurrence) instead of flooding one ERROR per request.
func TestSampledErrorLogAdmissionReleaseSites(t *testing.T) {
	var buf bytes.Buffer
	logger := &logging.Logger{Error: log.New(&buf, "", 0)}
	s := New(&config.Settings{}, logger, nil)
	storeErr := errors.New("valkey: connection refused")
	sites := []struct {
		name string
		logf func()
	}{
		{"release fallback", func() { s.admissionReleaseFallbackLog.logf(s.log, "admission: release fallback failed: %v", storeErr) }},
		{"completion release", func() {
			s.admissionCompletionReleaseLog.logf(s.log, "admission: completion release failed: %v", storeErr)
		}},
		{"prefill release", func() { s.admissionPrefillReleaseLog.logf(s.log, "admission: prefill release failed: %v", storeErr) }},
		{"upstream-failure release", func() {
			s.admissionUpstreamReleaseLog.logf(s.log, "admission: upstream-failure release failed: %v", storeErr)
		}},
		{"cold-hold release", func() { s.admissionColdHoldReleaseLog.logf(s.log, "admission: cold-hold release failed: %v", storeErr) }},
	}
	for _, site := range sites {
		t.Run(site.name, func(t *testing.T) {
			buf.Reset()
			for i := 0; i < 250; i++ {
				site.logf()
			}
			lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
			if len(lines) != 3 {
				t.Fatalf("logged %d lines for 250 occurrences, want 3 (1st + every 100th): %q", len(lines), lines)
			}
			if strings.Contains(lines[0], "suppressed") {
				t.Fatalf("first occurrence must log plainly: %q", lines[0])
			}
			if !strings.Contains(lines[1], "+98 similar suppressed") || !strings.Contains(lines[2], "+99 similar suppressed") {
				t.Fatalf("100th/200th occurrences must report the suppressed counts: %q %q", lines[1], lines[2])
			}
		})
	}
}

// Real-path proof that the admission bypass site emits through the Server
// sampler under the merged serveWithWake architecture. With the store dead
// from the start, every request's Admit fails with ErrUnavailable and the
// request bypasses the whole gate (admitted == nil) — the wake path then runs
// with no lease, so the cold-hold sites are never reached. 250 bypassed
// requests = 250 occurrences at the bypass site, so sampling must yield
// exactly the 1st + every 100th line — not 250 ERROR lines.
func TestWakeColdHoldStoreOutageLogsSampled(t *testing.T) {
	mr := miniredis.RunT(t)
	backend := &coldToWarmBackend{} // stays cold: the wake never warms it
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)
	cfg := proxyAdmissionConfig(1)
	cfg.Platform.MaxColdHolds = ptr64(1)
	// MaxRetries=-1: against a dead store every op otherwise pays go-redis's
	// retry backoff (~85ms), which would stretch this 250-request test to
	// minutes. The sampling behavior under test is unaffected.
	c := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = c.Close() })
	a := admission.New(c, cfg)

	// Admit the lease while the store is up, then kill the store: every
	// request's Admit fails with ErrUnavailable and bypasses the gate.
	lease, err := a.Admit(context.Background(), admission.Request{
		Graph: "graph", Organization: "org-a", Model: "m",
		PromptBytes: 1, EstimatedInputTokens: 1, ReservedOutputTokens: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = lease.Complete(context.Background(), 0)
	mr.Close()

	var buf bytes.Buffer
	// Warn must be sinked too: the merged emit() logs unmetered-attempt rows
	// at Warn, and the cold-hold rejection path now records a reconciliation row.
	logger := &logging.Logger{Error: log.New(&buf, "", 0), Warn: log.New(io.Discard, "", 0)}
	s := New(&config.Settings{Admission: cfg}, logger, &recordingEmitter{}).
		WithAdmitter(a).
		WithWaker(&fakeWaker{}, time.Second, 3)

	const requests = 250
	for i := 0; i < requests; i++ {
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, sharedRequest(up))
		if i == 0 && rr.Code != http.StatusNotFound {
			t.Fatalf("status=%d, want the cold 404 — a bypassed request must still complete the wake path", rr.Code)
		}
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	// 250 occurrences at the bypass site, logged at n=1,100,200.
	if len(lines) != 3 {
		t.Fatalf("logged %d lines for %d bypass occurrences, want 3 (1st + every 100th): %q", len(lines), requests, lines)
	}
	if got := strings.Count(buf.String(), "distributed gate unavailable; bypassing"); got != 3 {
		t.Fatalf("bypass site logged %d lines, want 3 (1st + every 100th of %d)", got, requests)
	}
	if strings.Contains(lines[0], "suppressed") {
		t.Fatalf("first occurrence must log the bypass onset plainly: %q", lines[0])
	}
	if !strings.Contains(lines[1], "+98 similar suppressed") || !strings.Contains(lines[2], "+99 similar suppressed") {
		t.Fatalf("100th/200th occurrences must report the suppressed counts: %q %q", lines[1], lines[2])
	}
}

// A second incident after a quiet gap must log its own onset: sampledErrorLog
// resets its counter when no occurrence has been seen for over a minute, so
// the 1st + every-100th cadence applies within an incident, not across
// incidents. A sub-gap pause does not reset anything.
func TestSampledErrorLogQuietGapReset(t *testing.T) {
	var buf bytes.Buffer
	logger := &logging.Logger{Error: log.New(&buf, "", 0)}
	var site sampledErrorLog
	base := time.Unix(1_700_000_000, 0)
	now := base
	site.now = func() time.Time { return now }

	// Incident one: 3 occurrences, only the onset logs.
	for i := 0; i < 3; i++ {
		site.logf(logger, "incident-one occurrence %d", i)
	}
	// Over a minute of silence, then a short second incident: its onset must
	// log even though the previous incident's counter (n=3) would suppress it.
	now = base.Add(2 * time.Minute)
	site.logf(logger, "incident-two onset")
	// The cadence still applies within the second incident: 98 suppressed
	// occurrences, then the 100th logs with its suppressed count.
	for i := 0; i < 99; i++ {
		now = now.Add(time.Millisecond)
		site.logf(logger, "incident-two occurrence %d", i)
	}
	// A sub-gap pause is NOT a new incident: the next occurrence is still
	// cadence-suppressed.
	now = now.Add(30 * time.Second)
	site.logf(logger, "incident-two still going")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("logged %d lines, want 3 (incident-one onset, incident-two onset, incident-two 100th): %q", len(lines), lines)
	}
	if !strings.Contains(lines[0], "incident-one occurrence 0") {
		t.Fatalf("line 0 must be incident one's onset: %q", lines[0])
	}
	if !strings.Contains(lines[1], "incident-two onset") || strings.Contains(lines[1], "suppressed") {
		t.Fatalf("line 1 must be incident two's onset, logged plainly: %q", lines[1])
	}
	if !strings.Contains(lines[2], "+98 similar suppressed") {
		t.Fatalf("line 2 must be incident two's 100th occurrence with 98 suppressed: %q", lines[2])
	}
}

// A concurrent burst at an incident's onset must log exactly ONE onset line:
// the quiet-gap reset is gated on winning the lastUnixNano CAS, so only the
// winner resets the counters. Without the gate, every goroutine that observed
// the gap would Store(0) and log its own onset line — a bounded burst at
// exactly the moment an incident starts.
func TestSampledErrorLogQuietGapConcurrentOnset(t *testing.T) {
	var buf bytes.Buffer
	logger := &logging.Logger{Error: log.New(&buf, "", 0)}
	var site sampledErrorLog
	base := time.Unix(1_700_000_000, 0)
	now := base
	site.now = func() time.Time { return now }
	site.quietGap = time.Minute

	// Incident one: 3 occurrences, only the onset logs.
	for i := 0; i < 3; i++ {
		site.logf(logger, "incident-one occurrence %d", i)
	}

	// Over a minute of silence, then a concurrent second incident.
	now = base.Add(2 * time.Minute)

	// Park every worker inside logf between its lastUnixNano load and the
	// gap check/CAS: the arrival barrier holds each worker at the hook until
	// all of them have loaded the pre-reset timestamp (incident one's wall
	// clock), so every worker enters the gap branch acting on the same
	// stale last — the collision the CAS gate exists to break. Releasing
	// them together then makes the outcome scheduling-independent: without
	// the gate each worker resets the counters and logs its own onset line
	// (deterministic red); with it exactly one CAS winner resets and logs
	// (deterministic green). The losers return without counting or logging
	// (their dropped occurrence is the acknowledged under-count).
	const workers = 16
	var arrived sync.WaitGroup
	arrived.Add(workers)
	release := make(chan struct{})
	site.afterLoad = func() {
		arrived.Done()
		<-release
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			site.logf(logger, "incident-two occurrence %d", i)
		}(i)
	}
	// Fail fast if a regression keeps workers from reaching the hook, rather
	// than blocking until the go test panic timeout.
	arrivedDone := make(chan struct{})
	go func() {
		arrived.Wait()
		close(arrivedDone)
	}()
	select {
	case <-arrivedDone:
	case <-time.After(5 * time.Second):
		t.Fatal("workers did not reach the afterLoad hook within 5s")
	}
	close(release)
	wg.Wait()

	// Losers must neither count nor log: only the CAS winner's increment
	// survives the reset. A refactor that lets a loser fall through to the
	// increment shows up here as n > 1 with a phantom suppressed count,
	// even when the log-line assertion below still passes (battery
	// finding 928773741a12).
	if got := site.n.Load(); got != 1 {
		t.Fatalf("site.n = %d, want 1 (only the CAS winner counts after the reset)", got)
	}
	if got := site.suppressed.Load(); got != 0 {
		t.Fatalf("site.suppressed = %d, want 0 (losers never reach the suppressed counter)", got)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("logged %d lines, want 2 (incident-one onset + exactly one incident-two onset): %q", len(lines), lines)
	}
	if !strings.Contains(lines[0], "incident-one occurrence 0") || strings.Contains(lines[0], "suppressed") {
		t.Fatalf("line 0 must be incident one's onset, logged plainly: %q", lines[0])
	}
	if !strings.Contains(lines[1], "incident-two occurrence") {
		t.Fatalf("line 1 must be the single incident-two onset: %q", lines[1])
	}
}

// The afterLoad barrier alone cannot force the one interleaving that logs a
// duplicate onset: a loser's increment landing between the winner's counter
// reset and the winner's own increment. The winner must be parked in that
// window while a loser falls through. afterReset exists for exactly that
// park: the winner stops between the zeroing stores and its first increment,
// every loser has by then already lost the CAS (a failed CAS implies the
// winner's swap happened), so against the old fall-through the first loser
// increment returns n==1 and logs its own onset line — deterministically,
// not scheduling-dependently (battery finding 1987a183d457).
func TestSampledErrorLogQuietGapWinnerPreemption(t *testing.T) {
	var buf bytes.Buffer
	logger := &logging.Logger{Error: log.New(&buf, "", 0)}
	var site sampledErrorLog
	base := time.Unix(1_700_000_000, 0)
	now := base
	site.now = func() time.Time { return now }
	site.quietGap = time.Minute

	// Incident one: 3 occurrences, only the onset logs.
	for i := 0; i < 3; i++ {
		site.logf(logger, "incident-one occurrence %d", i)
	}

	// Over a minute of silence, then a concurrent second incident.
	now = base.Add(2 * time.Minute)

	const workers = 16
	var arrived sync.WaitGroup
	arrived.Add(workers)
	release := make(chan struct{})
	site.afterLoad = func() {
		arrived.Done()
		<-release
	}
	resetParked := make(chan struct{})
	resetRelease := make(chan struct{})
	var resetOnce sync.Once
	site.afterReset = func() {
		resetOnce.Do(func() { close(resetParked) })
		<-resetRelease
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			site.logf(logger, "incident-two occurrence %d", i)
		}(i)
	}
	arrivedDone := make(chan struct{})
	go func() {
		arrived.Wait()
		close(arrivedDone)
	}()
	select {
	case <-arrivedDone:
	case <-time.After(5 * time.Second):
		t.Fatal("workers did not reach the afterLoad hook within 5s")
	}
	close(release)
	select {
	case <-resetParked:
	case <-time.After(5 * time.Second):
		t.Fatal("CAS winner did not reach the afterReset hook within 5s")
	}
	// The winner is parked in the fall-through window; with the loser
	// early-return in place none of the 15 losers counts or logs.
	close(resetRelease)
	wg.Wait()

	if got := site.n.Load(); got != 1 {
		t.Fatalf("site.n = %d, want 1 (a fall-through loser increments after the reset)", got)
	}
	if got := site.suppressed.Load(); got != 0 {
		t.Fatalf("site.suppressed = %d, want 0", got)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("logged %d lines, want 2 (incident-one onset + exactly one incident-two onset): %q", len(lines), lines)
	}
}

// Billing-contract pin for the admission-cause ErrorHandler branch: when a
// mid-flight lease renewal failure cancels the in-flight upstream request,
// errorHandler writes the 503 AND records exactly one raw reconciliation row
// (Aborted=false, UsageFound=false, StatusCode=503) via the same s.emit path
// as every other failure. Before the fix this branch was the one ErrorHandler
// exit with no metering row — the request passed the billing-identity gate,
// consumed shared capacity, and was invisible to billing. Exactly-once holds
// for the same structural reason as the 502 path: ModifyResponse never ran, so
// the completion emit was never armed.
// TestAdmissionRenewalFailureEmitsReconciliationRow — merged #50×#51 contract.
// phoebe#50 cancelled the in-flight upstream on mid-stream lease-renewal loss
// and recorded a raw 503 reconciliation row. phoebe#51's fail-open posture
// (the R2 ruling is pending Hugo) instead treats renewal failure as a bypass:
// the sampled log records it, and the already-admitted stream runs to
// completion — metering is independent of the fairness store, and the
// engine's real usage is what billing needs. This test now pins the merged
// behavior: no cancellation, the engine response reaches the client, and the
// normal completion path records exactly one usage-bearing row. The
// errorHandler's 503 admission-cause branch stays as a fail-safe for any
// future cancellation-with-cause, but nothing in the current tree cancels the
// proxy on renewal loss.
func TestAdmissionRenewalFailureEmitsReconciliationRow(t *testing.T) {
	started := make(chan struct{})
	var startedOnce, releaseOnce sync.Once
	releaseBackend := make(chan struct{})
	defer func() { releaseOnce.Do(func() { close(releaseBackend) }) }()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedOnce.Do(func() { close(started) })
		select {
		case <-r.Context().Done():
		case <-releaseBackend:
		}
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":2,"completion_tokens":3}}`))
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
	em := &recordingEmitter{}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), em).
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
	mr.Close() // mid-stream store loss: lease renewal fails from here on
	releaseOnce.Do(func() { close(releaseBackend) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("admitted stream did not complete after renewal failure (bypass posture)")
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200 — renewal failure must not cancel the admitted stream", rr.Code)
	}
	if got := rr.Header().Get(requestIDHeader); !strings.HasPrefix(got, "phoebe-") {
		t.Fatalf("response X-Request-Id = %q, want the generated billing attempt id", got)
	}
	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("completion emitted %d billing events, want exactly 1 usage-bearing row: %+v", len(events), events)
	}
	ev := events[0]
	if ev.Aborted || !ev.UsageFound || ev.StatusCode != http.StatusOK {
		t.Fatalf("completion row = {Aborted:%v UsageFound:%v StatusCode:%d}, "+
			"want {false true 200} (the engine's real usage, recorded by the normal path)", ev.Aborted, ev.UsageFound, ev.StatusCode)
	}
}
