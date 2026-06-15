package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/iolog"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/registry"
)

// recordingSink captures iolog Records for assertions. Safe for concurrent use.
type recordingSink struct {
	mu      sync.Mutex
	records []iolog.Record
}

func (s *recordingSink) Log(_ context.Context, rec iolog.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, rec)
}
func (s *recordingSink) Close(context.Context) error { return nil }
func (s *recordingSink) all() []iolog.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]iolog.Record(nil), s.records...)
}

// spyPolicy returns a fixed decision and records that it was consulted exactly
// once per request (the proxy must not call ShouldLog more than once).
type spyPolicy struct {
	decision bool
	mu       sync.Mutex
	calls    int
}

func (p *spyPolicy) ShouldLog(identity.Identity, string) bool {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	return p.decision
}
func (p *spyPolicy) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func newIOLogServer(t *testing.T, upstream *url.URL, policy iolog.Policy, sink iolog.Sink, maxBody int) *Server {
	t.Helper()
	s := &config.Settings{ListenAddr: ":0"}
	log := logging.New(logging.ERROR)
	resolver := registry.NewStatic(upstream)
	return NewWithIOLog(s, log, resolver, &recordingEmitter{}, policy, sink, maxBody)
}

func iologRequest(method, body string) *http.Request {
	req := httptest.NewRequest(method, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set(identity.HeaderAuthID, "auth-key-7")
	req.Header.Set(identity.HeaderResourceID, "model-abc")
	req.Header.Set(identity.HeaderResourceType, "deployment")
	req.Header.Set(identity.HeaderGroupID, "org-1")
	req.Header.Set(identity.HeaderUserID, "user-1")
	req.Header.Set("X-Request-Id", "req-iolog-1")
	return req
}

// TestIOLog_DisabledByDefault verifies the New() constructor (no iolog wiring)
// captures NOTHING — the fail-closed default. A spy sink would see no records.
func TestIOLog_DisabledByDefault(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)

	sink := &recordingSink{}
	s := &config.Settings{ListenAddr: ":0"}
	// Plain New(): denyAllPolicy + NopSink. We pass our recording sink only to
	// prove it's NOT used — wire it via NewWithIOLog with a deny-all policy.
	srv := New(s, logging.New(logging.ERROR), registry.NewStatic(upstream), &recordingEmitter{})
	srv.ioSink = sink // even if a sink is present, deny-all policy means no Log

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, iologRequest(http.MethodPost, `{"model":"m"}`))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got := len(sink.all()); got != 0 {
		t.Fatalf("default (logging off) must capture nothing, got %d records", got)
	}
}

// TestIOLog_ShouldLogFalse_NoBuffering verifies that when the policy says no,
// no Record is produced and the response is forwarded normally. The spy policy
// also asserts ShouldLog is consulted exactly once.
func TestIOLog_ShouldLogFalse_NoBuffering(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)

	policy := &spyPolicy{decision: false}
	sink := &recordingSink{}
	srv := newIOLogServer(t, upstream, policy, sink, 0)

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, iologRequest(http.MethodPost, `{"model":"m"}`))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if rr.Body.String() != `{"ok":true}` {
		t.Fatalf("response not forwarded verbatim: %q", rr.Body.String())
	}
	if got := len(sink.all()); got != 0 {
		t.Fatalf("ShouldLog=false must produce no records, got %d", got)
	}
	if c := policy.callCount(); c != 1 {
		t.Fatalf("ShouldLog called %d times, want exactly 1 (hot-path gate)", c)
	}
}

// TestIOLog_ShouldLogTrue_ProducesRecord verifies a full Record with request +
// response bodies, identity, and status is produced when the gate passes.
func TestIOLog_ShouldLogTrue_ProducesRecord(t *testing.T) {
	const reqBody = `{"model":"m","messages":[{"role":"user","content":"hi"}]}`
	const respBody = `{"id":"x","model":"llama-3-8b","choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2}}`
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Confirm upstream still receives a readable body (no double-read break).
		got, _ := io.ReadAll(r.Body)
		if len(got) == 0 {
			t.Error("upstream received empty body after capture")
		}
		_, _ = io.WriteString(w, respBody)
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)

	policy := &spyPolicy{decision: true}
	sink := &recordingSink{}
	srv := newIOLogServer(t, upstream, policy, sink, 0)

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, iologRequest(http.MethodPost, reqBody))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	recs := sink.all()
	if len(recs) != 1 {
		t.Fatalf("ShouldLog=true must produce exactly 1 record, got %d", len(recs))
	}
	rec := recs[0]
	if rec.RequestID != "req-iolog-1" {
		t.Errorf("RequestID = %q", rec.RequestID)
	}
	if rec.AuthID != "auth-key-7" || rec.GroupID != "org-1" || rec.UserID != "user-1" {
		t.Errorf("identity wrong: %+v", rec)
	}
	if rec.ResourceID != "model-abc" || rec.ResourceType != "deployment" {
		t.Errorf("resource fields wrong: %+v", rec)
	}
	// Model is the engine-reported name from the response body, not the resource id.
	if rec.Model != "llama-3-8b" {
		t.Errorf("Model = %q, want llama-3-8b (engine name, not resource id)", rec.Model)
	}
	// Request body is the ORIGINAL client body (no include_usage injection).
	if rec.RequestBody != reqBody {
		t.Errorf("RequestBody = %q, want original %q", rec.RequestBody, reqBody)
	}
	if rec.ResponseBody != respBody {
		t.Errorf("ResponseBody = %q, want %q", rec.ResponseBody, respBody)
	}
	if rec.ResponseTruncated {
		t.Error("small response should not be truncated")
	}
	if rec.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want 200", rec.StatusCode)
	}
}

// TestIOLog_StreamingForwardedVerbatim verifies that enabling body capture does
// NOT break streaming: the client still receives the SSE bytes unchanged, and a
// record with the concatenated stream is produced.
func TestIOLog_StreamingForwardedVerbatim(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		for _, chunk := range strings.SplitAfter(vllmStream, "\n\n") {
			if chunk == "" {
				continue
			}
			_, _ = io.WriteString(w, chunk)
			if fl != nil {
				fl.Flush()
			}
		}
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)

	policy := &spyPolicy{decision: true}
	sink := &recordingSink{}
	srv := newIOLogServer(t, upstream, policy, sink, 0)

	rr := httptest.NewRecorder()
	req := iologRequest(http.MethodPost, `{"model":"m","stream":true,"messages":[]}`)
	srv.Handler().ServeHTTP(rr, req)

	if rr.Body.String() != vllmStream {
		t.Fatalf("streaming not forwarded verbatim under logging:\n%q", rr.Body.String())
	}
	recs := sink.all()
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	if !recs[0].Streamed {
		t.Error("record should be marked Streamed")
	}
	if recs[0].ResponseBody != vllmStream {
		t.Errorf("captured response body != stream:\n%q", recs[0].ResponseBody)
	}
}

// TestIOLog_ResponseCapTruncates verifies the response-body buffering cap:
// the client still gets the FULL body, but the logged copy is truncated + flagged.
func TestIOLog_ResponseCapTruncates(t *testing.T) {
	big := strings.Repeat("A", 10000)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, big)
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)

	const bodyCap = 100
	policy := &spyPolicy{decision: true}
	sink := &recordingSink{}
	srv := newIOLogServer(t, upstream, policy, sink, bodyCap)

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, iologRequest(http.MethodPost, `{"model":"m"}`))

	// Client receives the FULL body — the cap only bounds the logged COPY.
	if rr.Body.Len() != len(big) {
		t.Fatalf("client body len = %d, want %d (cap must not truncate client stream)", rr.Body.Len(), len(big))
	}
	recs := sink.all()
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	if len(recs[0].ResponseBody) != bodyCap {
		t.Errorf("logged body len = %d, want %d (capped)", len(recs[0].ResponseBody), bodyCap)
	}
	if !recs[0].ResponseTruncated {
		t.Error("ResponseTruncated must be true when body exceeds cap")
	}
}

// TestIOLog_SinkReceivesRecordViaOnDone tests what the proxy actually guarantees
// about the sink: for an opted-in request it hands EXACTLY ONE record to the sink,
// from the onDone completion callback, with the response forwarded verbatim. It
// does NOT claim the sink runs off the response path — the proxy calls Sink.Log
// SYNCHRONOUSLY from onDone (and onDone can fire inside the final body Read, before
// the last bytes are copied to the client), so a sink that blocks in Log WOULD
// stall the response. The non-blocking guarantee is therefore the SINK's
// responsibility, asserted on the real PostgresSink in
// iolog.TestPostgresSink_LogIsNonBlocking (a buffered-channel send that drops
// rather than blocks).
//
// The previous test was named "NonBlockingSink" but wired a sink that never
// blocked — so it asserted nothing about non-blocking. This name matches what the
// proxy-level test can honestly verify; the real non-blocking property is pinned
// where it actually lives.
func TestIOLog_SinkReceivesRecordViaOnDone(t *testing.T) {
	const respBody = `{"ok":true,"model":"m"}`
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, respBody)
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)

	policy := &spyPolicy{decision: true}
	sink := &recordingSink{}
	srv := newIOLogServer(t, upstream, policy, sink, 0)

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, iologRequest(http.MethodPost, `{"model":"m"}`))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if rr.Body.String() != respBody {
		t.Fatalf("response not forwarded verbatim: %q", rr.Body.String())
	}
	if got := len(sink.all()); got != 1 {
		t.Fatalf("opted-in request must hand exactly 1 record to the sink, got %d", got)
	}
}
