package proxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

func newProtectedServer(p config.ProtectionSettings, em *recordingEmitter) *Server {
	return New(&config.Settings{Protection: p}, logging.New(logging.ERROR), em)
}

// dedicatedBoundRequest is a header-routed dedicated request on its
// per-resource route WITH an injected served-model allow-list: the route whose
// body phoebe used to read whole with no limit (model binding).
func dedicatedBoundRequest(up *url.URL, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	setUpstream(req, up)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "dedicated-1")
	req.Header.Set(identity.HeaderServedModel, "m")
	return req
}

func bodyOfSize(size int) string {
	prefix := `{"model":"m","messages":[],"padding":"`
	suffix := `"}`
	return prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix
}

// B1: every route is bounded, including the dedicated bound route and the
// dedicated route without an allow-list, for declared and chunked bodies.
func TestRequestBodyCapAppliesToDedicatedRoutes(t *testing.T) {
	const limit = 256
	var upstreamHits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHits.Add(1)
		_, _ = io.WriteString(w, `{"model":"m","usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	s := newProtectedServer(config.ProtectionSettings{MaxRequestBodyBytes: limit}, &recordingEmitter{})

	for _, tc := range []struct {
		name    string
		bound   bool
		size    int
		chunked bool
		want    int
	}{
		{"bound route at the limit is served", true, limit, false, http.StatusOK},
		{"bound route one byte over is refused", true, limit + 1, false, http.StatusRequestEntityTooLarge},
		{"bound route chunked one byte over is refused", true, limit + 1, true, http.StatusRequestEntityTooLarge},
		{"unbound route one byte over is refused", false, limit + 1, false, http.StatusRequestEntityTooLarge},
		{"unbound route chunked one byte over is refused", false, limit + 1, true, http.StatusRequestEntityTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := upstreamHits.Load()
			req := dedicatedBoundRequest(up, bodyOfSize(tc.size))
			if !tc.bound {
				req.Header.Del(identity.HeaderServedModel)
			}
			if tc.chunked {
				req.ContentLength = -1
				req.Header.Del("Content-Length")
			}
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, req)
			if rr.Code != tc.want {
				t.Fatalf("status=%d, want %d", rr.Code, tc.want)
			}
			if tc.want != http.StatusOK && upstreamHits.Load() != before {
				t.Fatal("an oversized body reached the upstream")
			}
		})
	}
}

func TestProtectionSettingsAbsentSelectsDefaults(t *testing.T) {
	eff := config.ProtectionSettings{}.Effective()
	if eff.MaxRequestBodyBytes != config.DefaultMaxRequestBodyBytes ||
		eff.StreamStallTimeout != config.DefaultStreamStallTimeout ||
		eff.MaxStreamDuration != config.DefaultMaxStreamDuration ||
		eff.MaxResponseBufferBytesPerStream != config.DefaultMaxResponseBufferBytesPerStream ||
		eff.MaxResponseBufferBytesTotal != config.DefaultMaxResponseBufferBytesTotal ||
		eff.MaxDedicatedColdHoldsPerEndpoint != config.DefaultMaxDedicatedColdHoldsPerEndpoint {
		t.Fatalf("absent settings must select every default, got %+v", eff)
	}
}

// liveProxy runs the server on a real listener: the stall guard and the
// keep-alive deadline reset only exist on a real connection.
func liveProxy(t *testing.T, s *Server) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return srv
}

func rawStreamingRequest(up *url.URL, resource string) string {
	body := `{"model":"m","stream":true,"messages":[]}`
	return "POST /v1/chat/completions HTTP/1.1\r\nHost: phoebe\r\n" +
		identity.HeaderUpstream + ": " + up.Host + "\r\n" +
		identity.HeaderServingMode + ": dedicated\r\n" +
		identity.HeaderAuthID + ": auth-1\r\n" +
		identity.HeaderResourceID + ": " + resource + "\r\n" +
		"Content-Type: application/json\r\nContent-Length: " + fmt.Sprint(len(body)) + "\r\n\r\n" + body
}

// B2: a client that stops reading mid-stream is cut off after the stall
// timeout, the upstream request is cancelled, and the attempt is recorded as
// aborted. Before the stall guard this request was held until the client went
// away, which a stalled client never does.
func TestStalledClientIsCutOffAndRecordedAsAborted(t *testing.T) {
	upstreamDone := make(chan struct{})
	chunk := "data: " + strings.Repeat("x", 32<<10) + "\n\n"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(upstreamDone)
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for r.Context().Err() == nil {
			if _, err := io.WriteString(w, chunk); err != nil {
				return
			}
			fl.Flush()
		}
	}))
	defer backend.Close()
	up, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	var warnings lockedBuffer
	log := logging.New(logging.WARN)
	log.Warn.SetOutput(&warnings)
	proxySrv := liveProxy(t, New(&config.Settings{Protection: config.ProtectionSettings{StreamStallTimeout: 200 * time.Millisecond}}, log, em))

	conn, err := net.Dial("tcp", proxySrv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// A tiny receive window makes the proxy's writes block quickly once the
	// client stops reading.
	_ = conn.(*net.TCPConn).SetReadBuffer(4096)
	if _, err := io.WriteString(conn, rawStreamingRequest(up, "stall-1")); err != nil {
		t.Fatal(err)
	}
	// Read the status line, then stop reading entirely.
	if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
		t.Fatal(err)
	}

	select {
	case <-upstreamDone:
	case <-time.After(10 * time.Second):
		t.Fatal("upstream request was never cancelled for a client that stopped reading")
	}
	events := em.waitForEvents(1, 5*time.Second)
	if len(events) != 1 || !events[0].Aborted {
		t.Fatalf("events=%+v, want one aborted attempt", events)
	}
	// The cut must be attributed to the stall guard, not logged as an
	// ordinary client disconnect: operators need to see that B2 fired.
	if got := warnings.String(); !strings.Contains(got, "protection: ended response") ||
		!strings.Contains(got, "protection.streamStallTimeout") {
		t.Fatalf("stall cut was not recorded as a protection cut; warnings=%q", got)
	}
}

// lockedBuffer is a bytes.Buffer safe to write from the handler goroutine
// while the test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// B3: a stream that outlives maxStreamDuration is ended and recorded as aborted.
func TestMaxStreamDurationEndsStream(t *testing.T) {
	backend, unblock := slowBackend(t, `data: {"model":"m","choices":[{"index":0,"delta":{"content":"hi"}}]}`+"\n\n")
	defer backend.Close()
	defer close(unblock)
	up, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	s := newProtectedServer(config.ProtectionSettings{MaxStreamDuration: 200 * time.Millisecond}, em)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	setUpstream(req, up)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = recover() }() // ReverseProxy aborts the handler on a cut copy
		s.Handler().ServeHTTP(httptest.NewRecorder(), req)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stream was not ended by maxStreamDuration")
	}
	events := em.waitForEvents(1, 5*time.Second)
	if len(events) != 1 || !events[0].Aborted {
		t.Fatalf("events=%+v, want one aborted attempt", events)
	}
}

// B3: no response headers within maxStreamDuration is a gateway timeout, not
// a client abort: the client gets 504 and the row is an upstream fault.
func TestMaxStreamDurationBeforeHeadersIsGatewayTimeout(t *testing.T) {
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer backend.Close()
	defer close(release)
	up, _ := url.Parse(backend.URL)
	em := &recordingEmitter{}
	s := newProtectedServer(config.ProtectionSettings{MaxStreamDuration: 200 * time.Millisecond}, em)

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	setUpstream(req, up)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusGatewayTimeout {
		t.Fatalf("status=%d, want 504", rr.Code)
	}
	events := em.waitForEvents(1, 5*time.Second)
	if len(events) != 1 || events[0].Aborted || events[0].StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("events=%+v, want one non-aborted 504 attempt", events)
	}
}

// B4: a non-streamed body larger than the per-stream buffer limit is cut
// BECAUSE of that limit: the client sees a cut response, the attempt is
// recorded as aborted, the protection log names the buffer-limit cause, and
// every byte returns to the replica budget. The control case (a body under the
// same limit) passes whole and is not aborted, so a limit that cut everything
// would fail here.
func TestResponseBufferPerStreamLimitAbortsWithBufferLimitCause(t *testing.T) {
	const perStream = 4 << 10
	for _, tc := range []struct {
		name    string
		payload int
		cut     bool
	}{
		{"over the limit is cut with the buffer-limit cause", 64 << 10, true},
		{"under the limit passes whole", 2 << 10, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			full := `{"model":"m","data":"` + strings.Repeat("x", tc.payload) + `"}`
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, full)
			}))
			defer backend.Close()
			up, _ := url.Parse(backend.URL)
			em := &recordingEmitter{}
			var warnings lockedBuffer
			log := logging.New(logging.WARN)
			log.Warn.SetOutput(&warnings)
			s := New(&config.Settings{Protection: config.ProtectionSettings{MaxResponseBufferBytesPerStream: perStream}}, log, em)
			proxySrv := liveProxy(t, s)

			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, proxySrv.URL+"/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
			if err != nil {
				t.Fatal(err)
			}
			setUpstream(req, up)
			req.Header.Set(identity.HeaderAuthID, "auth-1")
			req.Header.Set(identity.HeaderResourceID, "r1")
			resp, err := http.DefaultClient.Do(req)
			var body []byte
			var readErr error
			if err == nil {
				body, readErr = io.ReadAll(resp.Body)
				resp.Body.Close()
			}

			events := em.waitForEvents(1, 5*time.Second)
			if tc.cut {
				if err == nil && readErr == nil && len(body) == len(full) {
					t.Fatalf("response of %d bytes was delivered whole past the buffer limit", len(body))
				}
				if len(events) != 1 || !events[0].Aborted {
					t.Fatalf("events=%+v, want one aborted attempt", events)
				}
				if got := warnings.String(); !strings.Contains(got, "protection: ended response") ||
					!strings.Contains(got, errResponseBufferLimit.Error()) {
					t.Fatalf("cut was not attributed to the buffer limit; warnings=%q", got)
				}
			} else {
				if err != nil || readErr != nil || string(body) != full {
					t.Fatalf("under-limit body not passed whole: err=%v readErr=%v len=%d", err, readErr, len(body))
				}
				if len(events) != 1 || events[0].Aborted {
					t.Fatalf("events=%+v, want one non-aborted attempt", events)
				}
				if got := warnings.String(); strings.Contains(got, "protection: ended response") {
					t.Fatalf("an under-limit response fired a protection bound; warnings=%q", got)
				}
			}
			if held := s.responseBuffers.held.Load(); held != 0 {
				t.Fatalf("replica budget still holds %d bytes after the request finished", held)
			}
		})
	}
}

func TestResponseBufferAccountLimits(t *testing.T) {
	var budget responseBufferBudget
	p := config.ProtectionSettings{MaxResponseBufferBytesPerStream: 100, MaxResponseBufferBytesTotal: 150}
	var cancelled []error
	cancel := func(err error) { cancelled = append(cancelled, err) }
	a := budget.newAccount(p, cancel)
	b := budget.newAccount(p, cancel)

	if err := a.set(bufferSlotCapture, 60); err != nil {
		t.Fatal(err)
	}
	if err := a.set(bufferSlotFilter, 41); err == nil {
		t.Fatal("two slots summing over the per-stream limit must fail")
	}
	if err := b.set(bufferSlotCapture, 91); err == nil {
		t.Fatal("exceeding the replica total must fail")
	}
	if got := budget.held.Load(); got != 60 {
		t.Fatalf("failed reservations must charge nothing: held=%d, want 60", got)
	}
	if err := b.set(bufferSlotCapture, 90); err != nil {
		t.Fatalf("exactly at the total must pass: %v", err)
	}
	if err := a.set(bufferSlotCapture, 10); err != nil {
		t.Fatal(err)
	}
	a.close()
	a.close()
	b.close()
	if got := budget.held.Load(); got != 0 {
		t.Fatalf("held=%d after close, want 0", got)
	}
	if len(cancelled) != 2 {
		t.Fatalf("each refused reservation cancels its request: %d cancels, want 2", len(cancelled))
	}
}

// B4: a refused oversized reservation never inflates the replica total, so a
// concurrent small request that fits is never cut by a false over-total.
func TestResponseBufferRefusedReservationNeverBlocksOneThatFits(t *testing.T) {
	var budget responseBufferBudget
	p := config.ProtectionSettings{MaxResponseBufferBytesPerStream: 100, MaxResponseBufferBytesTotal: 100}
	noop := func(error) {}
	holder := budget.newAccount(p, noop)
	if err := holder.set(bufferSlotCapture, 90); err != nil {
		t.Fatal(err)
	}
	big := budget.newAccount(p, noop)
	small := budget.newAccount(p, noop)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := big.set(bufferSlotCapture, 50); err == nil {
				t.Error("an oversized reservation must be refused")
				return
			}
		}
	}()
	for i := 0; i < 20000; i++ {
		n := int64(i%5 + 1)
		if err := small.set(bufferSlotCapture, n); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("iteration %d: a reservation that fits (%d bytes on 90 of 100) was refused", i, n)
		}
	}
	close(stop)
	wg.Wait()
	// small's last reservation was (19999%5)+1 = 5 bytes on top of 90.
	if got := budget.held.Load(); got != 95 {
		t.Fatalf("held=%d, want 95", got)
	}
	small.close()
	big.close()
	holder.close()
	if got := budget.held.Load(); got != 0 {
		t.Fatalf("held=%d after close, want 0", got)
	}
}

// blockingWaker holds every wake until released, so the test can fill the
// dedicated cold-hold limit.
type blockingWaker struct {
	started chan struct{}
	release chan struct{}
	backend *coldToWarmBackend

	mu    sync.Mutex
	calls map[string]int // wakes per resource
}

func (b *blockingWaker) Wake(ctx context.Context, target WakeTarget) error {
	b.mu.Lock()
	if b.calls == nil {
		b.calls = map[string]int{}
	}
	b.calls[target.ResourceID]++
	b.mu.Unlock()
	b.started <- struct{}{}
	select {
	case <-b.release:
		b.backend.warm.Store(true)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *blockingWaker) wakes(resource string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls[resource]
}

func dedicatedColdRequest(up *url.URL, resource string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	setUpstream(req, up)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, resource)
	req.Header.Set(identity.HeaderServedModel, "m")
	req.Header.Set(identity.HeaderServingMode, identity.ServingModeDedicated)
	req.Header.Set(identity.HeaderServingProfile, "disaggregated")
	return req
}

// B5: one cold dedicated endpoint can hold at most the configured number of
// requests per replica; the next gets 503 + Retry-After without a wake, and
// another endpoint is unaffected.
func TestDedicatedColdHoldsCappedPerEndpoint(t *testing.T) {
	backend := &coldToWarmBackend{warmBody: `{"model":"m","usage":{"prompt_tokens":1,"completion_tokens":1}}`}
	be := httptest.NewServer(backend)
	defer be.Close()
	up, _ := url.Parse(be.URL)
	waker := &blockingWaker{started: make(chan struct{}, 4), release: make(chan struct{}), backend: backend}
	s := newProtectedServer(config.ProtectionSettings{MaxDedicatedColdHoldsPerEndpoint: 1}, &recordingEmitter{}).
		WithWaker(waker, 5*time.Second)

	var wg sync.WaitGroup
	held := httptest.NewRecorder()
	wg.Add(1)
	go func() { defer wg.Done(); s.Handler().ServeHTTP(held, dedicatedColdRequest(up, "endpoint-a")) }()
	<-waker.started

	over := httptest.NewRecorder()
	s.Handler().ServeHTTP(over, dedicatedColdRequest(up, "endpoint-a"))
	if over.Code != http.StatusServiceUnavailable || over.Header().Get("Retry-After") == "" {
		t.Fatalf("over-limit status=%d retry-after=%q, want 503 with Retry-After", over.Code, over.Header().Get("Retry-After"))
	}
	// The refused request never reached Wake. Checking started is empty also
	// means the endpoint-b wait below can only be satisfied by endpoint-b's
	// own wake, not by a stray one from the refused request.
	if n := waker.wakes("endpoint-a"); n != 1 {
		t.Fatalf("endpoint-a wakes=%d after the over-limit request, want 1 (refused without a wake)", n)
	}
	select {
	case <-waker.started:
		t.Fatal("over-limit request must be refused without a wake")
	default:
	}

	other := httptest.NewRecorder()
	wg.Add(1)
	go func() { defer wg.Done(); s.Handler().ServeHTTP(other, dedicatedColdRequest(up, "endpoint-b")) }()
	select {
	case <-waker.started:
	case <-time.After(5 * time.Second):
		t.Fatal("a different endpoint must still get its own cold hold")
	}
	if a, b := waker.wakes("endpoint-a"), waker.wakes("endpoint-b"); a != 1 || b != 1 {
		t.Fatalf("wakes endpoint-a=%d endpoint-b=%d, want 1 each", a, b)
	}
	close(waker.release)
	wg.Wait()
	if held.Code != http.StatusOK || other.Code != http.StatusOK {
		t.Fatalf("held=%d other=%d, want both served after the wake", held.Code, other.Code)
	}
	s.dedicatedHolds.mu.Lock()
	defer s.dedicatedHolds.mu.Unlock()
	if len(s.dedicatedHolds.held) != 0 {
		t.Fatalf("holds not released: %v", s.dedicatedHolds.held)
	}
}

// timeoutWriter is a ResponseWriter whose Write and FlushError fail the way a
// real connection does when the write deadline passes.
type timeoutWriter struct {
	http.ResponseWriter
	err error
}

func (w *timeoutWriter) Write([]byte) (int, error) { return 0, w.err }
func (w *timeoutWriter) FlushError() error         { return w.err }

// B2: a write that runs past its deadline (a *net.OpError wrapping
// os.ErrDeadlineExceeded, which does not match context.DeadlineExceeded) is
// recorded as a stall, through both Write and FlushError.
func TestStallGuardRecordsNetWriteDeadlineAsStall(t *testing.T) {
	opErr := &net.OpError{Op: "write", Net: "tcp", Err: os.ErrDeadlineExceeded}
	for _, tc := range []struct {
		name string
		do   func(g *stallGuardWriter)
	}{
		{"write", func(g *stallGuardWriter) { _, _ = g.Write([]byte("x")) }},
		{"flush", func(g *stallGuardWriter) { _ = g.FlushError() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			g := newStallGuardWriter(&timeoutWriter{ResponseWriter: httptest.NewRecorder(), err: opErr}, time.Second, cancel)
			tc.do(g)
			if !g.stalled.Load() || !errors.Is(g.stallCause(), errStreamStalled) {
				t.Fatal("a write past its deadline must set the stall flag")
			}
			if !errors.Is(context.Cause(ctx), errStreamStalled) || protectionCause(ctx) == nil {
				t.Fatalf("cause=%v, want errStreamStalled", context.Cause(ctx))
			}
		})
	}
}

// B2: the stall is still recorded when net/http has already cancelled the
// parent context (as it does on a failed connection write) before the guard
// runs, so the context cause can no longer be set.
func TestStallGuardRecordsStallAfterParentCancelled(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(context.Canceled)
	g := newStallGuardWriter(&timeoutWriter{ResponseWriter: httptest.NewRecorder(),
		err: &net.OpError{Op: "write", Err: os.ErrDeadlineExceeded}}, time.Second, cancel)
	_, _ = g.Write([]byte("x"))
	if protectionCause(ctx) != nil {
		t.Fatal("precondition: the parent cause should stay context.Canceled")
	}
	if !errors.Is(g.stallCause(), errStreamStalled) {
		t.Fatal("stall must be recorded on the guard when the context cause is already set")
	}
}

// A write that fails for any reason other than its deadline is not a stall.
func TestStallGuardIgnoresNonTimeoutWriteErrors(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	g := newStallGuardWriter(&timeoutWriter{ResponseWriter: httptest.NewRecorder(),
		err: &net.OpError{Op: "write", Err: errors.New("connection reset by peer")}}, time.Second, cancel)
	_, _ = g.Write([]byte("x"))
	if g.stallCause() != nil || ctx.Err() != nil {
		t.Fatal("a non-timeout write error must not be recorded as a stall")
	}
}

// blockingWarmBackend answers cold (404) until the wake warms it, then holds
// each warm request before sending headers until release is closed, the way
// an engine holds a request until its first token.
type blockingWarmBackend struct {
	warm     atomic.Bool
	inWarm   chan struct{}
	release  chan struct{}
	warmBody string
}

func (b *blockingWarmBackend) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if !b.warm.Load() {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Model not found"}`))
		return
	}
	b.inWarm <- struct{}{}
	<-b.release
	_, _ = w.Write([]byte(b.warmBody))
}

type warmingWaker struct{ backend *blockingWarmBackend }

func (w *warmingWaker) Wake(context.Context, WakeTarget) error {
	w.backend.warm.Store(true)
	return nil
}

// B5: a dedicated cold hold covers only the time the endpoint is starting.
// It is released when the wake completes, before the retried request waits
// for the warm engine's headers, so a second cold request to the same
// endpoint can take the single slot while the first is still waiting.
func TestDedicatedColdHoldReleasedWhenWakeCompletes(t *testing.T) {
	backend := &blockingWarmBackend{
		inWarm:   make(chan struct{}, 1),
		release:  make(chan struct{}),
		warmBody: `{"model":"m","usage":{"prompt_tokens":1,"completion_tokens":1}}`,
	}
	be := httptest.NewServer(backend)
	defer be.Close()
	// Unblock the warm backend before be.Close on every exit, so a failed
	// assertion ends the test instead of hanging it.
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(backend.release) }) }
	defer unblock()
	up, _ := url.Parse(be.URL)
	s := newProtectedServer(config.ProtectionSettings{MaxDedicatedColdHoldsPerEndpoint: 1}, &recordingEmitter{}).
		WithWaker(&warmingWaker{backend: backend}, 5*time.Second)

	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); s.Handler().ServeHTTP(rec, dedicatedColdRequest(up, "endpoint-a")) }()
	select {
	case <-backend.inWarm:
	case <-time.After(5 * time.Second):
		t.Fatal("the retried request never reached the warm backend")
	}

	s.dedicatedHolds.mu.Lock()
	inUse := s.dedicatedHolds.held["endpoint-a"]
	s.dedicatedHolds.mu.Unlock()
	if inUse != 0 {
		t.Fatalf("endpoint-a holds in use while the retry waits for headers = %d, want 0", inUse)
	}
	second, ok := s.dedicatedHolds.acquire("endpoint-a", s.protection().MaxDedicatedColdHoldsPerEndpoint)
	if !ok {
		t.Fatal("a second cold request could not take the only slot while the first waited on the warm engine")
	}
	second()

	unblock()
	<-done
	if rec.Code != http.StatusOK {
		t.Fatalf("held request status=%d, want 200 after the wake", rec.Code)
	}
}
