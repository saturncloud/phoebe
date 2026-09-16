package proxy

import (
	"context"
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
			MaxConcurrentPrefills: active, MaxPromptBytes: 1024, MaxReservedOutputTokens: 100,
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
	if rr2.Code != http.StatusTooManyRequests {
		t.Fatalf("contending replica status=%d, want 429", rr2.Code)
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

func TestAdmissionStateFailureReturns503BeforeUpstream(t *testing.T) {
	var hits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hits++; w.WriteHeader(200) }))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	cfg := proxyAdmissionConfig(1)
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 20 * time.Millisecond, ReadTimeout: 20 * time.Millisecond, WriteTimeout: 20 * time.Millisecond, MaxRetries: 0})
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(client, cfg))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, sharedRequest(up))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", rr.Code)
	}
	if hits != 0 {
		t.Fatal("failed-closed request reached upstream")
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
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d, want 429", rr.Code)
	}
	if hits != 0 {
		t.Fatal("impossible request reached upstream")
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
	lease, err := a.Admit(context.Background(), admission.Request{Graph: "graph", Organization: "other", Model: "m", PromptBytes: 1, ReservedOutputTokens: 1})
	if err != nil {
		t.Fatalf("aborted stream leaked reservation: %v", err)
	}
	_ = lease.Complete(context.Background(), 0)
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

func TestAdmissionWorkRejectsAmbiguousOrImpossibleRequests(t *testing.T) {
	if _, _, ok := admissionWork([]byte(`{"model":"m","max_tokens":2,"max_completion_tokens":3}`), 10); ok {
		t.Fatal("conflicting output limits accepted")
	}
	if model, n, ok := admissionWork([]byte(`{"model":"m"}`), 10); !ok || model != "m" || n != 10 {
		t.Fatalf("defaults=(%q,%d,%t)", model, n, ok)
	}
}
