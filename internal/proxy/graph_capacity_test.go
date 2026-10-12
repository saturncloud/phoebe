package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

type staticGraphCapacity map[string]gateway.GraphCapacity

func (s staticGraphCapacity) GraphCapacity(graph string) gateway.GraphCapacity { return s[graph] }

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

// B8: capacity Atlas publishes for a graph is enforced whenever an admission
// store exists — without admission.enabled and without any contract envelope
// — as an operator capacity rejection (503 + Retry-After, not a 429). Other
// graphs are unaffected.
func TestPublishedGraphCapacityEnforcedWithoutEnablement(t *testing.T) {
	release := make(chan struct{})
	hit := make(chan struct{}, 4)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit <- struct{}{}
		<-release
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
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
	s.graphCapacity = staticGraphCapacity{graph: {MaxActiveRequests: ptr64(1)}}

	done := make(chan int, 1)
	go func() {
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, bareSharedRequest(up, "org-a"))
		done <- rr.Code
	}()
	select {
	case <-hit:
	case code := <-done:
		t.Fatalf("first request finished early with %d", code)
	case <-time.After(5 * time.Second):
		t.Fatal("first request never reached the engine")
	}

	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, bareSharedRequest(up, "org-b"))
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") == "" {
		t.Fatalf("second request on a full graph: status=%d retry-after=%q, want 503 with Retry-After", rr.Code, rr.Header().Get("Retry-After"))
	}

	// A route with no trusted org keeps its historical behavior: published
	// capacity alone does not start failing it closed.
	go func() {
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, bareSharedRequest(up, ""))
		done <- rr.Code
	}()
	select {
	case <-hit:
	case <-time.After(5 * time.Second):
		t.Fatal("an org-less historical route must not be refused by published capacity")
	}
	close(release)
	for i := 0; i < 2; i++ {
		if code := <-done; code != http.StatusOK {
			t.Fatalf("held request status=%d", code)
		}
	}

	// Freed: the graph admits again.
	release2 := httptest.NewRecorder()
	backend.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	})
	s.Handler().ServeHTTP(release2, bareSharedRequest(up, "org-b"))
	if release2.Code != http.StatusOK {
		t.Fatalf("after release status=%d, want 200", release2.Code)
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
