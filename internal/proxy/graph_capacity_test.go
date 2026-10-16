package proxy

import (
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
	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/testutil"
)

// staticGraphCapacity publishes the same values as the scaled and the
// per-request capacity (a graph-level entry, or one ready worker).
type staticGraphCapacity map[string]gateway.GraphCapacity

func (s staticGraphCapacity) GraphCapacity(graph string) gateway.GraphCapacity { return s[graph] }

func (s staticGraphCapacity) GraphRequestCapacity(graph string) gateway.GraphCapacity {
	return s[graph]
}

// bareSharedRequest is a shared request with the trusted org but NO limit
// envelope: with admission.enabled=false, nothing but published graph
// capacity can engage admission for it.
func bareSharedRequest(up *url.URL, org string) *http.Request {
	r := sharedRequest(up)
	for _, header := range identity.ScopedRateLimitHeaders {
		r.Header.Del(header)
	}
	r.Header.Del(identity.HeaderOwnerID)
	if org == "" {
		r.Header.Del(identity.HeaderOrgID)
	} else {
		r.Header.Set(identity.HeaderOrgID, org)
	}
	return r
}

// capacityFixture is a server with an admission store but admission.enabled
// off, whose graph (the upstream's) has a published capacity of one active
// request. The upstream holds every request until releaseAll.
type capacityFixture struct {
	s          *Server
	up         *url.URL
	graph      string
	hit        chan struct{}
	releaseAll func()
}

func newCapacityFixture(t *testing.T) *capacityFixture {
	t.Helper()
	release := make(chan struct{})
	hit := make(chan struct{}, 16)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit <- struct{}{}
		<-release
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	t.Cleanup(backend.Close)
	// Cleanups run last-registered first, so a failed assertion releases any
	// held handler before backend.Close, which would otherwise hang.
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)
	up, _ := url.Parse(backend.URL)
	graph := graphFromUpstreamHost(up.Host)

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	settings := &config.Settings{Admission: config.AdmissionSettings{KeyPrefix: "b8", LeaseTTL: time.Minute, DefaultMaxOutputTokens: 20}}
	eff, ok := (&config.Settings{Admission: settings.Admission, Emit: config.EmitSettings{ValkeyAddr: mr.Addr()}}).EffectiveAdmission()
	if !ok {
		t.Fatal("store must resolve")
	}
	s := New(settings, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(client, eff))
	s.graphCapacity = staticGraphCapacity{graph: {MaxActiveRequests: testutil.Ptr[int64](1)}}
	return &capacityFixture{s: s, up: up, graph: graph, hit: hit, releaseAll: releaseAll}
}

// serveAsync serves a bare shared request for org in the background and
// sends its status code to done.
func (f *capacityFixture) serveAsync(org string, done chan int) {
	go func() {
		rr := httptest.NewRecorder()
		f.s.Handler().ServeHTTP(rr, bareSharedRequest(f.up, org))
		done <- rr.Code
	}()
}

// fill sends one request from org-a and waits until the upstream holds it,
// so the graph's single slot is taken. The returned channel receives its
// status once released.
func (f *capacityFixture) fill(t *testing.T) chan int {
	t.Helper()
	done := make(chan int, 4)
	f.serveAsync("org-a", done)
	select {
	case <-f.hit:
	case code := <-done:
		t.Fatalf("first request finished early with %d", code)
	case <-time.After(5 * time.Second):
		t.Fatal("first request never reached the engine")
	}
	return done
}

// B8 / D5: capacity Atlas publishes for a graph is enforced whenever an
// admission store exists, without admission.enabled and without any contract
// envelope, as an operator capacity rejection (503 + Retry-After, not a 429).
func TestPublishedGraphCapacityRejectsWith503WithoutAdmissionEnabled(t *testing.T) {
	f := newCapacityFixture(t)
	f.fill(t)
	rr := httptest.NewRecorder()
	f.s.Handler().ServeHTTP(rr, bareSharedRequest(f.up, "org-b"))
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") == "" {
		t.Fatalf("second request on a full graph: status=%d retry-after=%q, want 503 with Retry-After", rr.Code, rr.Header().Get("Retry-After"))
	}
}

// While graph A is full, a graph with no published capacity stays unlimited.
// Two concurrent requests are held there, so a capacity lookup that ignored
// its graph (and capped this graph at one) is caught too, not only one
// sharing graph A's counter.
func TestPublishedGraphCapacityDoesNotAffectOtherGraphs(t *testing.T) {
	f := newCapacityFixture(t)
	f.fill(t)

	release2 := make(chan struct{})
	hit2 := make(chan struct{}, 2)
	backend2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit2 <- struct{}{}
		<-release2
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend2.Close()
	var release2Once sync.Once
	release2All := func() { release2Once.Do(func() { close(release2) }) }
	defer release2All()
	up2, _ := url.Parse(backend2.URL)
	// Both test servers listen on 127.0.0.1 (graph "127"); "localhost" is a
	// different graph with no published capacity.
	up2.Host = "localhost:" + up2.Port()
	if graphFromUpstreamHost(up2.Host) == f.graph {
		t.Fatal("precondition: the second upstream must map to a different graph")
	}
	done2 := make(chan int, 2)
	for i := 0; i < 2; i++ {
		go func() {
			rr := httptest.NewRecorder()
			f.s.Handler().ServeHTTP(rr, bareSharedRequest(up2, "org-b"))
			done2 <- rr.Code
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-hit2:
		case code := <-done2:
			t.Fatalf("an unpublished graph must not be limited by another graph's capacity: status=%d", code)
		case <-time.After(5 * time.Second):
			t.Fatal("an unpublished graph must not be limited by another graph's capacity: request never reached the engine")
		}
	}
	release2All()
	for i := 0; i < 2; i++ {
		if code := <-done2; code != http.StatusOK {
			t.Fatalf("an unpublished graph must not be limited by another graph's capacity: status=%d", code)
		}
	}
}

// D5 applies published capacity to requests with a trusted organization. A
// route with no trusted org keeps its historical behavior: published capacity
// alone does not start failing it closed, even while the graph is full.
func TestPublishedGraphCapacityNotEnforcedForOrglessRoute(t *testing.T) {
	f := newCapacityFixture(t)
	done := f.fill(t)
	f.serveAsync("", done)
	select {
	case <-f.hit:
	case code := <-done:
		t.Fatalf("an org-less historical route must not be refused by published capacity: status=%d", code)
	case <-time.After(5 * time.Second):
		t.Fatal("an org-less historical route must not be refused by published capacity")
	}
	f.releaseAll()
	for i := 0; i < 2; i++ {
		if code := <-done; code != http.StatusOK {
			t.Fatalf("held request status=%d", code)
		}
	}
}

// Once the request holding a full graph's only slot finishes, the graph
// admits again.
func TestPublishedGraphCapacityReadmitsAfterRelease(t *testing.T) {
	f := newCapacityFixture(t)
	done := f.fill(t)
	full := httptest.NewRecorder()
	f.s.Handler().ServeHTTP(full, bareSharedRequest(f.up, "org-b"))
	if full.Code != http.StatusServiceUnavailable {
		t.Fatalf("precondition: second request on a full graph status=%d, want 503", full.Code)
	}
	f.releaseAll()
	if code := <-done; code != http.StatusOK {
		t.Fatalf("held request status=%d", code)
	}
	// The upstream no longer holds requests once released.
	afterRelease := httptest.NewRecorder()
	f.s.Handler().ServeHTTP(afterRelease, bareSharedRequest(f.up, "org-b"))
	if afterRelease.Code != http.StatusOK {
		t.Fatalf("after release status=%d, want 200", afterRelease.Code)
	}
}

// With nothing published (no profile, no override) a bare shared request
// never touches the admission store.
func TestNoPublishedCapacityMeansNoAdmission(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	a := &countingAdmitter{}
	s := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(a)
	s.graphCapacity = staticGraphCapacity{}
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, bareSharedRequest(up, "org-a"))
	if rr.Code != http.StatusOK || a.calls != 0 {
		t.Fatalf("status=%d admit calls=%d, want 200 and no admission", rr.Code, a.calls)
	}
}

// D12 / B8: a request larger than the capacity Atlas published for its
// graph can never be admitted there, so it is refused 413 with the model's
// KV-cache wording, without Retry-After and before the engine. The control:
// a published zero cap stays a retryable 503.
func TestPublishedGraphCapacityOversizeIs413(t *testing.T) {
	var hits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	graph := graphFromUpstreamHost(up.Host)

	serve := func(c gateway.GraphCapacity) *httptest.ResponseRecorder {
		mr := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		settings := &config.Settings{Admission: config.AdmissionSettings{KeyPrefix: "b8-oversize", LeaseTTL: time.Minute, DefaultMaxOutputTokens: 20}}
		eff, ok := (&config.Settings{Admission: settings.Admission, Emit: config.EmitSettings{ValkeyAddr: mr.Addr()}}).EffectiveAdmission()
		if !ok {
			t.Fatal("store must resolve")
		}
		s := New(settings, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(client, eff))
		s.graphCapacity = staticGraphCapacity{graph: c}
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, bareSharedRequest(up, "org-a"))
		return rr
	}

	// bareSharedRequest declares max_tokens=20, so it needs at least 21 KV tokens.
	rr := serve(gateway.GraphCapacity{MaxReservedKVTokens: testutil.Ptr[int64](10)})
	if rr.Code != http.StatusRequestEntityTooLarge || rr.Header().Get("Retry-After") != "" {
		t.Fatalf("status=%d retry-after=%q, want 413 without Retry-After", rr.Code, rr.Header().Get("Retry-After"))
	}
	if !strings.Contains(rr.Body.String(), "this model's KV-cache capacity") {
		t.Fatalf("body=%q, want the model KV-cache wording", rr.Body.String())
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("upstream hits=%d, an oversize request must not reach the engine", n)
	}

	rr = serve(gateway.GraphCapacity{MaxReservedKVTokens: testutil.Ptr[int64](0), MaxPrefillTokens: testutil.Ptr[int64](0)})
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") == "" {
		t.Fatalf("published zero caps: status=%d retry-after=%q, want a retryable 503", rr.Code, rr.Header().Get("Retry-After"))
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("upstream hits=%d after a zero-cap refusal, want 0", n)
	}

	// A zero reserved-KV cap on its own (no zero prefill cap to answer
	// first) is also the retryable 503, never the 413 kv_exceeds_limit.
	rr = serve(gateway.GraphCapacity{MaxReservedKVTokens: testutil.Ptr[int64](0)})
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") == "" {
		t.Fatalf("published zero KV cap alone: status=%d retry-after=%q, want a retryable 503", rr.Code, rr.Header().Get("Retry-After"))
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("upstream hits=%d after a zero KV cap refusal, want 0", n)
	}
}

// R-C (with D5): when published graph capacity is the only reason admission
// runs (admission.enabled=false, no limit envelope) and the admission store
// is unavailable, shared requests fail open: they reach the engine with 200
// and the published capacity is not enforced while the store is down. Both a
// refused connection and a store that accepts but never replies are covered.
func TestPublishedGraphCapacityFailsOpenWhenStoreUnavailable(t *testing.T) {
	refused := func(t *testing.T) string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		_ = l.Close()
		return addr
	}
	silent := func(t *testing.T) string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = l.Close() })
		go func() {
			for {
				conn, err := l.Accept()
				if err != nil {
					return
				}
				go func() {
					defer conn.Close()
					_, _ = io.Copy(io.Discard, conn)
				}()
			}
		}()
		return l.Addr().String()
	}
	for name, storeAddr := range map[string]func(*testing.T) string{"refused": refused, "accepts but never replies": silent} {
		t.Run(name, func(t *testing.T) {
			var hits atomic.Int64
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				<-release
				_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
			}))
			t.Cleanup(backend.Close)
			t.Cleanup(releaseAll)
			up, _ := url.Parse(backend.URL)

			settings := &config.Settings{Admission: config.AdmissionSettings{KeyPrefix: "b8", LeaseTTL: time.Minute, DefaultMaxOutputTokens: 20}}
			client := admission.NewValkeyClient(storeAddr(t))
			t.Cleanup(func() { _ = client.Close() })
			s := New(settings, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(admission.New(client, settings.Admission))
			s.graphCapacity = staticGraphCapacity{graphFromUpstreamHost(up.Host): {MaxActiveRequests: testutil.Ptr[int64](1)}}

			// Two concurrent requests against a published capacity of one:
			// both reach the engine, so the capacity is not enforced.
			done := make(chan int, 2)
			started := time.Now()
			for range 2 {
				go func() {
					rr := httptest.NewRecorder()
					s.Handler().ServeHTTP(rr, bareSharedRequest(up, "org-a"))
					done <- rr.Code
				}()
			}
			deadline := time.Now().Add(2 * time.Second)
			for hits.Load() < 2 {
				if time.Now().After(deadline) {
					t.Fatalf("upstream hits=%d after %s, want both requests to fail open to the engine", hits.Load(), time.Since(started))
				}
				time.Sleep(5 * time.Millisecond)
			}
			releaseAll()
			for range 2 {
				if code := <-done; code != http.StatusOK {
					t.Fatalf("status=%d, want 200 (fail open)", code)
				}
			}
		})
	}
}
