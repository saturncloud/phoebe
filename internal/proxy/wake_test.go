package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

func TestIsColdWakeable(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"404 model not found -> cold", 404, `{"message":"Model not found"}`, true},
		{"503 not-ready marker -> cold", 503, `Model X is not ready to serve requests yet. Retry.`, true},
		{"503 generic overload -> NOT cold", 503, `{"error":"server overloaded"}`, false},
		{"200 -> not cold", 200, `{"ok":true}`, false},
		{"500 -> not cold", 500, `boom`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newBufferingResponseWriter()
			b.WriteHeader(tc.status)
			_, _ = b.Write([]byte(tc.body))
			if b.isColdWakeable() != tc.want {
				t.Fatalf("isColdWakeable(status=%d)=%v want %v", tc.status, b.isColdWakeable(), tc.want)
			}
		})
	}
}

// TestGraphFromUpstreamHost pins the header-routed graph derivation: first DNS
// label, `-frontend` Service suffix stripped, port ignored; a label without the
// suffix is the k8s name itself.
func TestGraphFromUpstreamHost(t *testing.T) {
	cases := map[string]string{
		"graph-llama31-frontend.tf-shared.svc.cluster.local:8000":     "graph-llama31",
		"pd-abcde-mymodel-r123.main-namespace.svc.cluster.local:8000": "pd-abcde-mymodel-r123",
		"solo-frontend:8000": "solo",
		"bare-name":          "bare-name",
	}
	for host, want := range cases {
		if got := graphFromUpstreamHost(host); got != want {
			t.Errorf("graphFromUpstreamHost(%q) = %q, want %q", host, got, want)
		}
	}
}

func TestIsWakeable(t *testing.T) {
	if !isWakeable(identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "shared"}) {
		t.Fatal("shared route with resource id should be wakeable")
	}
	if isWakeable(identity.Identity{ResourceID: "r1", ServedModel: "m"}) {
		t.Fatal("dedicated route must NOT be wakeable even with a model binding")
	}
	if isWakeable(identity.Identity{ServedModel: "m", ServingMode: "shared"}) {
		t.Fatal("no resource id (unauthorized) must NOT be wakeable")
	}
	// An empty ServingMode is not a valid serving mode. The route gate refuses a
	// header-routed request without one (ruling #19), and the gateway registry
	// parser (gateway.parseRegistryConfigMap) rejects rows with a blank
	// serving_mode. If one ever arrives here it must not be
	// wakeable, because only the exact value "shared" is (fail closed).
	if isWakeable(identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: ""}) {
		t.Fatal("empty serving mode is invalid and must NOT be wakeable")
	}
	if isWakeable(identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "dedicated"}) {
		t.Fatal("dedicated route without a disaggregated profile must NOT be wakeable")
	}
	if isWakeable(identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "dedicated", ServingProfile: "aggregated"}) {
		t.Fatal("aggregated dedicated route must NOT be wakeable")
	}
	if !isWakeable(identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "dedicated", ServingProfile: "disaggregated"}) {
		t.Fatal("disaggregated dedicated route with an authorized resource must be wakeable")
	}
}

type fakeWaker struct {
	calls   int32
	err     error
	warmsAt int32 // after this many wake calls, the upstream goes warm
	backend *coldToWarmBackend
	onWake  func()

	mu         sync.Mutex
	lastTarget WakeTarget
}

type unsupportedWaker struct {
	fakeWaker
}

func (*unsupportedWaker) SupportsWake(WakeTarget) bool { return false }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type readTrackingBody struct {
	reads atomic.Int32
}

func (b *readTrackingBody) Read([]byte) (int, error) {
	b.reads.Add(1)
	return 0, io.EOF
}

func (*readTrackingBody) Close() error { return nil }

type countingReadCloser struct {
	reader *strings.Reader
	bytes  atomic.Int64
	closed atomic.Bool
}

func (b *countingReadCloser) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.bytes.Add(int64(n))
	return n, err
}

func (b *countingReadCloser) Close() error {
	b.closed.Store(true)
	return nil
}

type bytesAndErrorBody struct {
	body   []byte
	err    error
	offset int
	done   bool
	closed atomic.Bool
}

type closeUnblocksBody struct {
	prefix []byte
	sent   bool
	closed chan struct{}
	once   sync.Once
}

type delayedChunksBody struct {
	chunks  [][]byte
	release <-chan struct{}
	chunk   int
	offset  int
	waited  bool
	closed  atomic.Bool
}

func (b *delayedChunksBody) Read(p []byte) (int, error) {
	if b.chunk >= len(b.chunks) {
		return 0, io.EOF
	}
	if b.chunk == 1 && !b.waited {
		<-b.release
		b.waited = true
	}
	n := copy(p, b.chunks[b.chunk][b.offset:])
	b.offset += n
	if b.offset == len(b.chunks[b.chunk]) {
		b.chunk++
		b.offset = 0
	}
	return n, nil
}

func (b *delayedChunksBody) Close() error {
	b.closed.Store(true)
	return nil
}

func (b *closeUnblocksBody) Read(p []byte) (int, error) {
	if !b.sent {
		b.sent = true
		return copy(p, b.prefix), nil
	}
	<-b.closed
	return 0, errors.New("body closed")
}

func (b *closeUnblocksBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

func (b *bytesAndErrorBody) Read(p []byte) (int, error) {
	if b.done {
		return 0, io.EOF
	}
	n := copy(p, b.body[b.offset:])
	b.offset += n
	if b.offset == len(b.body) {
		b.done = true
		return n, b.err
	}
	return n, nil
}

func (b *bytesAndErrorBody) Close() error {
	b.closed.Store(true)
	return nil
}

func (f *fakeWaker) Wake(_ context.Context, target WakeTarget) error {
	n := atomic.AddInt32(&f.calls, 1)
	if f.onWake != nil {
		f.onWake()
	}
	f.mu.Lock()
	f.lastTarget = target
	f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if f.backend != nil && n >= f.warmsAt {
		f.backend.warm.Store(true)
	}
	return nil
}

func (f *fakeWaker) last() WakeTarget {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastTarget
}

// coldToWarmBackend serves cold (404) until warm is set, then 200.
type coldToWarmBackend struct {
	warm      atomic.Bool
	requests  atomic.Int32
	successes atomic.Int32
	warmBody  string
}

func (c *coldToWarmBackend) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	c.requests.Add(1)
	if c.warm.Load() {
		c.successes.Add(1)
		w.WriteHeader(200)
		body := c.warmBody
		if body == "" {
			body = `{"served":true}`
		}
		_, _ = w.Write([]byte(body))
		return
	}
	w.WriteHeader(404)
	_, _ = w.Write([]byte(`{"message":"Model not found"}`))
}

// TestWithWakerDefaults pins the default wake budget: 300s — deliberately
// ABOVE vLLM's measured ~2.5min cold reload (a smaller default made every real
// wake time out and serve the cold response after holding the client anyway).
func TestWithWakerDefaults(t *testing.T) {
	s := New(&config.Settings{}, logging.New(logging.ERROR), nil).WithWaker(&fakeWaker{}, 0)
	if s.wakeTimeout != 300*time.Second {
		t.Fatalf("default wakeTimeout = %v, want 300s (must exceed the real cold start)", s.wakeTimeout)
	}
}

func TestWakeWarmFallThroughMetersNormalRowOnly(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"model":"m","choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)

	em := &recordingEmitter{}
	s := New(&config.Settings{}, logging.New(logging.ERROR), em).
		WithWaker(&fakeWaker{}, 5*time.Second)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","messages":[]}`))
	setUpstream(req, up)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	req.Header.Set(identity.HeaderServedModel, "m")
	req.Header.Set(identity.HeaderServingMode, "shared")
	s.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("warm fall-through emitted %d billing events, want exactly 1: %+v", len(events), events)
	}
	ev := events[0]
	if !ev.UsageFound || ev.Aborted {
		t.Fatalf("warm fall-through row = {UsageFound:%v Aborted:%v}, want the usage-bearing row {true false}",
			ev.UsageFound, ev.Aborted)
	}
	if ev.StatusCode != http.StatusOK || ev.PromptTokens != 1 || ev.CompletionTokens != 1 {
		t.Fatalf("warm fall-through row = {StatusCode:%d Prompt:%d Completion:%d}, want {200 1 1}",
			ev.StatusCode, ev.PromptTokens, ev.CompletionTokens)
	}
}

func TestColdWakeRetryPreservesRewrittenStreamingBody(t *testing.T) {
	var mu sync.Mutex
	var bodies [][]byte
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read upstream request body: %v", err)
		}
		mu.Lock()
		bodies = append(bodies, append([]byte(nil), body...))
		attempt := len(bodies)
		mu.Unlock()
		if attempt == 1 {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"Model not found"}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n"))
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)

	em := &recordingEmitter{}
	s := New(&config.Settings{}, logging.New(logging.ERROR), em).
		WithWaker(&fakeWaker{}, time.Second)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hello"}]}`))
	setUpstream(req, up)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	req.Header.Set(identity.HeaderServedModel, "m")
	req.Header.Set(identity.HeaderServingMode, identity.ServingModeDedicated)
	req.Header.Set(identity.HeaderServingProfile, "disaggregated")

	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	mu.Lock()
	gotBodies := append([][]byte(nil), bodies...)
	mu.Unlock()
	if len(gotBodies) != 2 {
		t.Fatalf("upstream attempts = %d, want exactly 2", len(gotBodies))
	}
	if !bytes.Equal(gotBodies[0], gotBodies[1]) {
		t.Fatalf("retry body differs from first attempt:\nfirst: %s\nretry: %s", gotBodies[0], gotBodies[1])
	}
	var rewritten struct {
		Model    string `json:"model"`
		Stream   bool   `json:"stream"`
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
		StreamOptions struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	if err := json.Unmarshal(gotBodies[1], &rewritten); err != nil {
		t.Fatalf("decode retry body: %v", err)
	}
	if rewritten.Model != "m" || !rewritten.Stream || len(rewritten.Messages) != 1 ||
		rewritten.Messages[0].Content != "hello" || !rewritten.StreamOptions.IncludeUsage {
		t.Fatalf("retry body lost request fields or usage rewrite: %+v", rewritten)
	}
	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("metering events = %d, want exactly 1: %+v", len(events), events)
	}
}

func TestWakeTransportDoesNotBufferSuccessfulResponse(t *testing.T) {
	body := &readTrackingBody{}
	waker := &fakeWaker{}
	server := New(&config.Settings{}, logging.New(logging.ERROR), nil).
		WithWaker(waker, time.Second)
	transport := server.newWakeTransport(
		roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"text/event-stream"}},
				Body:       body,
				Request:    req,
			}, nil
		}),
		&url.URL{Host: "graph-frontend.default.svc:8000"},
		identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "shared"},
		"m",
		"request-1",
		nil,
	)

	resp, err := transport.RoundTrip(httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Body != body || body.reads.Load() != 0 {
		t.Fatalf("successful response body was replaced or read before streaming: body_same=%v reads=%d", resp.Body == body, body.reads.Load())
	}
	if got := waker.calls; got != 0 {
		t.Fatalf("successful response invoked waker %d times, want 0", got)
	}
}

func TestBufferColdCandidateBounds503InspectionAndPreservesBody(t *testing.T) {
	body404 := &countingReadCloser{reader: strings.NewReader(`{"message":"Model not found"}`)}
	resp404 := &http.Response{StatusCode: http.StatusNotFound, Body: body404}
	cold, err := bufferColdCandidate(resp404)
	if err != nil || !cold || !body404.closed.Load() {
		t.Fatalf("404 classification: cold=%v err=%v closed=%v, want a closed buffered cold response", cold, err, body404.closed.Load())
	}

	original := strings.Repeat("x", coldCandidatePrefixLimit*2)
	body := &countingReadCloser{reader: strings.NewReader(original)}
	resp503 := &http.Response{StatusCode: http.StatusServiceUnavailable, Body: body}
	cold, err = bufferColdCandidate(resp503)
	if err != nil || cold {
		t.Fatalf("generic 503 classification: cold=%v err=%v", cold, err)
	}
	if got := body.bytes.Load(); got > coldCandidatePrefixLimit+1 {
		t.Fatalf("503 classification read %d bytes, limit is %d", got, coldCandidatePrefixLimit+1)
	}
	replayed, err := io.ReadAll(resp503.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(replayed) != original {
		t.Fatalf("restored 503 body length=%d, want %d", len(replayed), len(original))
	}
	_ = resp503.Body.Close()
}

func TestBufferColdCandidatePreserves503BodyReadError(t *testing.T) {
	readErr := io.ErrUnexpectedEOF
	for _, tc := range []struct {
		name string
		body string
		cold bool
	}{
		{name: "generic", body: `{"error":"partial overload"}`, cold: false},
		{name: "cold marker", body: "Model X is not ready to serve requests yet", cold: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := &bytesAndErrorBody{body: []byte(tc.body), err: readErr}
			resp := &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       original,
			}
			cold, err := bufferColdCandidate(resp)
			if err != nil || cold != tc.cold {
				t.Fatalf("classification: cold=%v err=%v, want cold=%v nil", cold, err, tc.cold)
			}
			got, err := io.ReadAll(resp.Body)
			if string(got) != tc.body || !errors.Is(err, readErr) {
				t.Fatalf("replayed body=%q err=%v, want %q and %v", got, err, tc.body, readErr)
			}
			if !original.closed.Load() {
				t.Fatal("original 503 body was not closed after bounded classification")
			}
		})
	}
}

func TestBufferColdCandidatePreservesOversized503BodyReadError(t *testing.T) {
	readErr := io.ErrUnexpectedEOF
	body := bytes.Repeat([]byte("x"), coldCandidatePrefixLimit+1)
	original := &bytesAndErrorBody{body: body, err: readErr}
	resp := &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Body:       original,
	}

	cold, err := bufferColdCandidate(resp)
	if err != nil || cold {
		t.Fatalf("classification: cold=%v err=%v, want false and nil", cold, err)
	}
	got, err := io.ReadAll(resp.Body)
	if !bytes.Equal(got, body) || !errors.Is(err, readErr) {
		t.Fatalf("replayed body length=%d err=%v, want length=%d and %v", len(got), err, len(body), readErr)
	}
	if !original.closed.Load() {
		t.Fatal("original oversized 503 body was not closed after terminal read error")
	}
}

func TestBufferColdCandidateTimesOutIncompleteBody(t *testing.T) {
	secondChunk := make(chan struct{})
	delayed := strings.Repeat("x", coldCandidatePrefixLimit+100)
	body := &delayedChunksBody{
		chunks:  [][]byte{[]byte(`{"error":"partial"}`), []byte(delayed)},
		release: secondChunk,
	}
	resp := &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Body:       body,
	}
	started := time.Now()
	cold, err := bufferColdCandidateWithin(context.Background(), resp, 20*time.Millisecond)
	if cold || err != nil {
		t.Fatalf("classification: cold=%v err=%v, want false and nil", cold, err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("classification timeout took %s", elapsed)
	}
	close(secondChunk)
	replayed, err := io.ReadAll(resp.Body)
	want := `{"error":"partial"}` + delayed
	if err != nil || string(replayed) != want {
		t.Fatalf("replayed body=%q err=%v, want both chunks", replayed, err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close handed-off body: %v", err)
	}
	if !body.closed.Load() {
		t.Fatal("closing handed-off body did not close the upstream body")
	}
}

func TestWakeCandidateInspectionTimeoutPreservesDelayed404StatusAndBody(t *testing.T) {
	testWakeCandidateInspectionTimeoutPreservesResponse(
		t,
		http.StatusNotFound,
		`{"message":"delayed missing model"}`,
	)
}

func TestWakeCandidateInspectionTimeoutPreservesDelayedGeneric503StatusAndBody(t *testing.T) {
	testWakeCandidateInspectionTimeoutPreservesResponse(
		t,
		http.StatusServiceUnavailable,
		`{"error":"delayed overload"}`,
	)
}

func testWakeCandidateInspectionTimeoutPreservesResponse(t *testing.T, status int, responseBody string) {
	t.Helper()
	firstChunk := responseBody[:len(responseBody)/2]
	secondChunk := responseBody[len(responseBody)/2:]
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = io.WriteString(w, firstChunk)
		w.(http.Flusher).Flush()
		time.Sleep(150 * time.Millisecond)
		_, _ = io.WriteString(w, secondChunk)
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	waker := &fakeWaker{}
	server := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithWaker(waker, 50*time.Millisecond)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	setUpstream(req, upstream)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	req.Header.Set(identity.HeaderServedModel, "m")
	req.Header.Set(identity.HeaderServingMode, identity.ServingModeShared)
	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != status || recorder.Body.String() != responseBody {
		t.Fatalf("response = status %d body %q, want status %d body %q",
			recorder.Code, recorder.Body.String(), status, responseBody)
	}
	if got := atomic.LoadInt32(&waker.calls); got != 0 {
		t.Fatalf("incomplete candidate invoked the waker %d times, want 0", got)
	}
}

func TestWakeUnsupportedModeSlowResponseBypassesInspection(t *testing.T) {
	body := &closeUnblocksBody{
		prefix: []byte(`{"error":"slow unsupported mode"}`),
		closed: make(chan struct{}),
	}
	waker := &unsupportedWaker{}
	server := New(&config.Settings{}, logging.New(logging.ERROR), nil).
		WithWaker(waker, 20*time.Millisecond)
	transport := &wakeRoundTripper{
		server: server,
		base: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: body}, nil
		}),
		target: WakeTarget{ServingMode: identity.ServingModeDedicated},
	}

	started := time.Now()
	resp, err := transport.RoundTrip(httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("unsupported response was held for inspection for %s", elapsed)
	}
	if resp.Body != body || body.sent {
		t.Fatalf("unsupported response body was inspected: same=%v read=%v", resp.Body == body, body.sent)
	}
	if got := atomic.LoadInt32(&waker.calls); got != 0 {
		t.Fatalf("unsupported response invoked the waker %d times, want 0", got)
	}
	_ = resp.Body.Close()
}

func TestWakeCompleted503ColdMarkerStillWakes(t *testing.T) {
	var requests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "Model m is not ready to serve requests yet")
			return
		}
		_, _ = io.WriteString(w, `{"served":true}`)
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)
	waker := &fakeWaker{}
	server := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithWaker(waker, time.Second)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	setUpstream(req, upstream)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	req.Header.Set(identity.HeaderServedModel, "m")
	req.Header.Set(identity.HeaderServingMode, identity.ServingModeShared)
	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK || recorder.Body.String() != `{"served":true}` {
		t.Fatalf("response = status %d body %q, want 200 warm response", recorder.Code, recorder.Body.String())
	}
	if got := atomic.LoadInt32(&waker.calls); got != 1 {
		t.Fatalf("completed marker-bearing 503 invoked the waker %d times, want 1", got)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("upstream requests = %d, want cold attempt plus one retry", got)
	}
}

func TestWakeClosesFirstColdResponseBeforeWaiting(t *testing.T) {
	original := &countingReadCloser{reader: strings.NewReader(`{"message":"Model not found"}`)}
	waker := &fakeWaker{
		err: errors.New("definite wake failure"),
		onWake: func() {
			if !original.closed.Load() {
				t.Error("cold upstream body remained open when Wake began")
			}
		},
	}
	server := New(&config.Settings{}, logging.New(logging.ERROR), nil).WithWaker(waker, time.Second)
	transport := server.newWakeTransport(
		roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}, Body: original, Request: req}, nil
		}),
		&url.URL{Host: "graph-frontend.default.svc:8000"},
		identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "shared"},
		"m", "request-1", nil,
	)
	resp, err := transport.RoundTrip(httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != `{"message":"Model not found"}` {
		t.Fatalf("returned cold body=%q err=%v", body, err)
	}
}

func TestWakeUnknownStatusReturnsNeutralRetryableResponse(t *testing.T) {
	original := &countingReadCloser{reader: strings.NewReader(`{"message":"Model not found"}`)}
	waker := &fakeWaker{err: fmt.Errorf("Atlas response unavailable: %w", ErrWakeStatusUnknown)}
	server := New(&config.Settings{}, logging.New(logging.ERROR), nil).WithWaker(waker, time.Second)
	transport := server.newWakeTransport(
		roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}, Body: original, Request: req}, nil
		}),
		&url.URL{Host: "graph-frontend.default.svc:8000"},
		identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "dedicated"},
		"m", "request-1", nil,
	)
	resp, err := transport.RoundTrip(httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("status=%d Retry-After=%q, want retryable 503", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if strings.Contains(string(body), "starting from zero") || !strings.Contains(string(body), "could not confirm") {
		t.Fatalf("body=%q, want neutral indeterminate-wake guidance", body)
	}
	if !original.closed.Load() {
		t.Fatal("original cold response body was not closed")
	}
}

// TestWakeUnavailableReturnsRetryableResponseNotTheCold404 (F2): Atlas could
// not be reached for the whole hold (restarting during a deploy). The model
// exists, so the caller gets a retryable 503 saying the wake service is
// unavailable, never the frontend's cold 404 "Model not found".
func TestWakeUnavailableReturnsRetryableResponseNotTheCold404(t *testing.T) {
	original := &countingReadCloser{reader: strings.NewReader(`{"message":"Model not found"}`)}
	waker := &fakeWaker{err: fmt.Errorf("atlas unreachable: %w", ErrWakeUnavailable)}
	server := New(&config.Settings{}, logging.New(logging.ERROR), nil).WithWaker(waker, time.Second)
	transport := server.newWakeTransport(
		roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}, Body: original, Request: req}, nil
		}),
		&url.URL{Host: "graph-frontend.default.svc:8000"},
		identity.Identity{ResourceID: "r1", ServedModel: "m", ServingMode: "shared"},
		"m", "request-1", nil,
	)
	resp, err := transport.RoundTrip(httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("status=%d Retry-After=%q, want retryable 503", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if !strings.Contains(string(body), "wake service is unavailable") || strings.Contains(string(body), "Model not found") {
		t.Fatalf("body=%q, want the wake-unavailable guidance, not the cold 404", body)
	}
	if !original.closed.Load() {
		t.Fatal("original cold response body was not closed")
	}
}

func TestDedicatedWakeTargetsRequestedAllowListMember(t *testing.T) {
	backend := &coldToWarmBackend{warmBody: `{"model":"adapter-a","usage":{"prompt_tokens":2,"completion_tokens":1}}`}
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)
	waker := &fakeWaker{backend: backend, warmsAt: 1}
	s := New(&config.Settings{}, logging.New(logging.ERROR), &recordingEmitter{}).
		WithWaker(waker, time.Second)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"adapter-a","messages":[]}`))
	setUpstream(req, up)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	req.Header.Set(identity.HeaderServedModel, "base-model, adapter-a")
	req.Header.Set(identity.HeaderServingMode, identity.ServingModeDedicated)
	req.Header.Set(identity.HeaderServingProfile, "disaggregated")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK || backend.requests.Load() != 2 || backend.successes.Load() != 1 || waker.calls != 1 {
		t.Fatalf("status=%d requests=%d successes=%d wakes=%d, want 200/2/1/1", rr.Code, backend.requests.Load(), backend.successes.Load(), waker.calls)
	}
	if got := waker.last().ServedModel; got != "adapter-a" {
		t.Fatalf("wake target served model=%q, want the requested allow-list member", got)
	}
}

// TestWakeSkippedOnControlRoutes (wake-scoping negative pin): the wake path
// runs ONLY for model-bearing inference requests. A bound shared GET or HEAD
// on a cold control route (/health, /live, /v1/models) is served by the normal
// forward — the readiness sanitizer and model-list filters reduce the cold
// upstream response — and must never call the waker: a monitoring probe must
// not trigger a 0->1 scale of a cold base. Without this test, deleting
// inferenceRequestPathAllowed(routePath) from the wake condition leaves the
// whole suite green while cold control probes scale the graph.
func TestWakeSkippedOnControlRoutes(t *testing.T) {
	backend := &coldToWarmBackend{} // stays cold
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)

	em := &recordingEmitter{}
	waker := &fakeWaker{}
	s := New(&config.Settings{}, logging.New(logging.ERROR), em).
		WithWaker(waker, 5*time.Second)

	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/health"},
		{http.MethodGet, "/live"},
		{http.MethodGet, "/v1/models"},
		{http.MethodHead, "/health"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			setUpstream(req, up)
			req.Header.Set(identity.HeaderAuthID, "auth-1")
			req.Header.Set(identity.HeaderResourceID, "r1")
			req.Header.Set(identity.HeaderServedModel, "m")
			req.Header.Set(identity.HeaderServingMode, "shared")
			s.Handler().ServeHTTP(rr, req)

			if got := atomic.LoadInt32(&waker.calls); got != 0 {
				t.Fatalf("control-route %s %s called the waker %d times, want 0 "+
					"(monitoring probes must not scale a cold base)", tc.method, tc.path, got)
			}
			if rr.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want the upstream cold 404 served by the normal forward", rr.Code)
			}
		})
	}

	// Positive control: the identical bound shared route DOES wake on the
	// inference POST — proves the waker is functional in this test and the
	// negative assertions above are meaningful.
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m","messages":[]}`))
	setUpstream(req, up)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	req.Header.Set(identity.HeaderServedModel, "m")
	req.Header.Set(identity.HeaderServingMode, "shared")
	s.Handler().ServeHTTP(rr, req)
	if got := atomic.LoadInt32(&waker.calls); got == 0 {
		t.Fatal("inference POST on the same cold route did not call the waker — the positive control must wake")
	}
}
