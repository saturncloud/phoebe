package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/identity"
)

// WakeTarget identifies what a wake actuates. The proxy resolves it once so
// the actuator never re-parses strings the proxy composed.
type WakeTarget struct {
	// UpstreamHost is the trusted upstream host:port the cold response came
	// from. The waker also uses it for readiness checks.
	UpstreamHost string
	// GraphK8sName is the Dynamo graph (DGD) k8s name whose worker DGDSA the
	// waker scales. On the gateway path it is the tf_model row's
	// graph_k8s_name, threaded through resolution verbatim; on header-routed
	// requests it is derived once from the upstream host (see
	// graphFromUpstreamHost). Never empty for a target the proxy hands to a
	// waker.
	GraphK8sName string
	// ResourceID is the atlas-authorized resource id (authorization proof at
	// wake time + audit).
	ResourceID string
	// ServedModel is the trusted deployment-scoped model name Atlas stamped on
	// the route. Dedicated wake does not report ready until this model appears
	// in frontend discovery.
	ServedModel string
	// ServingMode selects the actuator. Shared graphs keep their narrow DGDSA
	// patch; dedicated disaggregated graphs call Atlas, which validates the
	// endpoint and restores both persisted worker-pool counts.
	ServingMode string
}

// Waker triggers a shared base graph's 0->1 scale-up and blocks until the
// graph's worker is ready to serve (or the context/deadline is exceeded).
// Abstracted so the ACTUATION is pluggable (the concrete client-go DGDSA
// waker lives in internal/waker; a future actuator could POST to an Atlas
// wake endpoint instead). The proxy's cold-detect + hold-and-retry logic is
// identical either way.
//
// Wake returns nil once the worker is ready, or an error if wake could not
// complete. The caller never hangs forever; what the client then receives
// depends on WHY the wake did not complete:
//
//   - The scale-up was triggered (or was already in progress) but the worker
//     was not ready before ctx expired: Wake returns an error wrapping
//     ErrWakeNotReady. The caller answers 503 Service Unavailable with a
//     Retry-After header and a JSON error body saying the model is starting.
//     The scale-up keeps going after the hold expires, so a retry is served
//     once the worker is ready.
//   - The wake request may have reached its actuator, but the response does not
//     prove whether scale-up began: Wake wraps ErrWakeStatusUnknown. The caller
//     answers a neutral retryable 503 without claiming that workers are starting.
//   - Any other failure (no scalable adapter for the graph, an RBAC or API
//     error on the scale patch, a malformed target): Wake returns an error that
//     does NOT wrap ErrWakeNotReady. Nothing is starting, so telling the client
//     to retry would be false; the caller returns the original cold upstream
//     response unchanged.
type Waker interface {
	Wake(ctx context.Context, target WakeTarget) error
}

// wakeTargetSupporter lets a composite waker report that one serving mode has
// no actuator. The check happens before admission starts a cold hold. Simple
// wakers need not implement it because they support every target handed to
// them.
type wakeTargetSupporter interface {
	SupportsWake(target WakeTarget) bool
}

// ErrWakeNotReady is the error a Waker wraps when it triggered the 0->1
// scale-up (or found the graph already scaled up) but the worker did not
// become ready before the context expired. It is the only wake error that
// turns the held cold response into a 503 + Retry-After (see Waker).
var ErrWakeNotReady = errors.New("wake triggered but the worker was not ready before the deadline")

// ErrWakeStatusUnknown marks a wake request whose result is indeterminate. It
// remains retryable, but unlike ErrWakeNotReady it does not prove that worker
// restoration began.
var ErrWakeStatusUnknown = errors.New("wake status is unknown")

// wakeRetryAfterSeconds is the Retry-After value (in seconds) phoebe sends on
// the 503 it returns when a scale-up from zero is in progress but the worker
// was not ready within the wake hold (wake.timeout). Long enough not to hammer
// a graph that is still pulling its image or loading weights, short enough
// that a client retrying on it is served soon after the worker becomes ready.
const wakeRetryAfterSeconds = 30

// wakeNeverServedHeader is reserved so an upstream cannot expose the obsolete
// marker to clients. Settlement uses private wakeRoundTripper state instead.
const wakeNeverServedHeader = "X-Phoebe-Wake-Never-Served"

// writeWakeStarting answers a held cold request whose scale-up was triggered
// but whose worker was not ready before the hold expired: 503 + Retry-After +
// a JSON error body in phoebe's {"error": "..."} shape. The cold upstream
// response (a Dynamo 404 "Model not found", or the not-ready 503) is NOT
// passed through: a 404 tells API clients the model does not exist and they
// do not retry it, which is wrong while the model is starting.
func writeWakeStarting(w http.ResponseWriter, requestID string) {
	body, _ := json.Marshal(map[string]string{
		"error": "The model is starting from zero; retry in about " +
			strconv.Itoa(wakeRetryAfterSeconds) + " seconds.",
	})
	h := w.Header()
	h.Set(requestIDHeader, requestID)
	h.Set("Content-Type", "application/json")
	h.Set("Retry-After", strconv.Itoa(wakeRetryAfterSeconds))
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write(body)
}

func writeWakeStatusUnknown(w http.ResponseWriter, requestID string) {
	body, _ := json.Marshal(map[string]string{
		"error": "The wake service could not confirm whether startup began; retry in about " +
			strconv.Itoa(wakeRetryAfterSeconds) + " seconds.",
	})
	h := w.Header()
	h.Set(requestIDHeader, requestID)
	h.Set("Content-Type", "application/json")
	h.Set("Retry-After", strconv.Itoa(wakeRetryAfterSeconds))
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write(body)
}

// Shared-mode wake-from-zero (the 0->1 leg). Dynamo has NO wake-from-zero: when
// a shared base graph's worker is scaled to 0 (by the Atlas reaper), the
// frontend drops the model and a request for it returns a COLD response. Phoebe
// — the only actor in the request path — supplies the external wake signal:
// detect the cold response, trigger a scale 0->1 on the graph's DGDSA, hold the
// request, and retry once the worker is back.
//
// THE COLD SIGNAL (verified against Dynamo v1.4.0): a scaled-to-0 base returns
// EITHER a 404 "Model not found" (the model object is fully removed from
// discovery) OR, transiently on the way down/up, a 503 whose body carries the
// fixed "not ready to serve requests yet" phrase. Both are treated as cold.
// A 404 alone is AMBIGUOUS (a genuinely-nonexistent model 404s identically), so
// phoebe never wakes on the response ALONE — it wakes only when the request also
// carries a valid, atlas-authorized X-Saturn-Resource-Id (proof this is a real
// deployed endpoint) AND the route is a shared-mode route (a served-model
// allow-list is present). A cold response without those is returned to the
// client unchanged.

// the fixed substring Dynamo's frontend puts in the 503 body when a model is
// registered but has zero ready workers (v1.4.0). Machine-distinguishable from a
// generic overload 503 (which comes via a separate queue-rejection path).
const dynamoNotReadyBodyMarker = "is not ready to serve requests yet"

// isWakeable reports whether a request is eligible for wake-from-zero. Shared
// routes are eligible under the established behavior. A dedicated route is
// eligible only when Atlas explicitly stamps it as disaggregated. Both paths
// also require the Atlas-authorized resource id and served-model binding that
// distinguish a parked graph from a model that does not exist.
func isWakeable(id identity.Identity) bool {
	modeWakeable := id.ServingMode == identity.ServingModeShared ||
		(id.ServingMode == identity.ServingModeDedicated && id.ServingProfile == "disaggregated")
	return modeWakeable && id.ResourceID != "" && id.ServedModel != ""
}

// graphFromUpstreamHost derives the Dynamo graph (DGD) k8s name from a
// trusted upstream host for HEADER-ROUTED wakes (the gateway path threads the
// graph name from resolution instead). The upstream's
// first DNS label names the graph's Service: for a Dynamo frontend Service
// that is `<graph>-frontend` (exactly what the gateway's upstreamFor
// composes, and what Atlas injects for Dynamo-served deployments), so the
// `-frontend` suffix is stripped; a label without the suffix IS the k8s name.
func graphFromUpstreamHost(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	first, _, _ := strings.Cut(host, ".")
	return strings.TrimSuffix(first, "-frontend")
}

// bodyLooksNotReady reports whether a 503 body carries Dynamo's fixed
// zero-worker "not ready" marker — distinguishing a cold-worker 503 from a
// generic overload/queue-rejection 503 (which must NOT trigger a wake+hold).
func bodyLooksNotReady(body []byte) bool {
	return strings.Contains(string(body), dynamoNotReadyBodyMarker)
}

// bufferingResponseWriter constructs local admission and starting responses
// that the wake transport returns through the normal reverse proxy path.
type bufferingResponseWriter struct {
	header http.Header
	status int
	body   []byte
	wrote  bool
}

func newBufferingResponseWriter() *bufferingResponseWriter {
	return &bufferingResponseWriter{header: http.Header{}, status: http.StatusOK}
}

func (b *bufferingResponseWriter) Header() http.Header { return b.header }

func (b *bufferingResponseWriter) WriteHeader(status int) {
	if b.wrote {
		return
	}
	b.status = status
	b.wrote = true
}

func (b *bufferingResponseWriter) Write(p []byte) (int, error) {
	if !b.wrote {
		b.WriteHeader(http.StatusOK)
	}
	b.body = append(b.body, p...)
	return len(p), nil
}

// isColdWakeable reports whether the captured response is a wakeable cold signal:
// a 404 (model removed from discovery at 0 workers), or a 503 carrying Dynamo's
// fixed "not ready" marker (registered-but-zero-workers). A generic overload 503
// (no marker) is NOT wakeable.
func (b *bufferingResponseWriter) isColdWakeable() bool {
	switch b.status {
	case http.StatusNotFound:
		return true
	case http.StatusServiceUnavailable:
		return bodyLooksNotReady(b.body)
	default:
		return false
	}
}

// wakeEnabled reports whether this request should go through the wake path: a
// waker is configured and the route is eligible for one of the supported wake
// modes. Everything else streams directly with zero wake overhead.
func (s *Server) wakeEnabled(id identity.Identity) bool {
	return s.waker != nil && isWakeable(id)
}

type wakeRoundTripper struct {
	server    *Server
	base      http.RoundTripper
	target    WakeTarget
	requestID string
	lease     *admission.Lease
	// neverServed is private per-request state. It must not come from a response
	// header because the upstream controls those headers.
	neverServed bool
}

func (t *wakeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if supporter, ok := t.server.waker.(wakeTargetSupporter); ok && !supporter.SupportsWake(t.target) {
		return t.base.RoundTrip(req)
	}
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	cold, err := bufferColdCandidateWithin(req.Context(), resp, t.candidateReadTimeout())
	if err != nil || !cold {
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	if t.lease != nil {
		if err := t.lease.BeginColdHold(req.Context()); err != nil {
			_ = resp.Body.Close()
			t.neverServed = true
			buf := newBufferingResponseWriter()
			t.server.writeAdmissionError(buf, t.requestID, err)
			return bufferedHTTPResponse(req, buf, true), nil
		}
	}

	if t.target.ServingMode == identity.ServingModeDedicated {
		// Dedicated endpoints have no admission lease, so the shared tier's
		// maxColdHolds never applies to them; this per-endpoint cap bounds how
		// many requests one cold endpoint can park in this replica.
		release, ok := t.server.dedicatedHolds.acquire(t.target.ResourceID, t.server.protection().MaxDedicatedColdHoldsPerEndpoint)
		if !ok {
			_ = resp.Body.Close()
			t.neverServed = true
			t.server.log.Warn.Printf("wake: dedicated cold-hold limit reached request_id=%s resource_id=%s",
				t.requestID, t.target.ResourceID)
			buf := newBufferingResponseWriter()
			writeDedicatedColdHoldsFull(buf, t.requestID)
			return bufferedHTTPResponse(req, buf, true), nil
		}
		defer release()
	}

	ctx := req.Context()
	if t.server.wakeTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.server.wakeTimeout)
		defer cancel()
	}
	wakeErr := t.server.waker.Wake(ctx, t.target)
	if t.lease != nil {
		if err := t.lease.EndColdHold(context.WithoutCancel(req.Context())); err != nil {
			t.server.admissionColdHoldReleaseLog.logf(
				t.server.log, "admission: cold-hold release failed: %v", err,
			)
		}
	}
	if wakeErr != nil {
		t.server.log.Warn.Printf(
			"wake: could not warm base for request_id=%s resource_id=%s: %v",
			t.requestID, t.target.ResourceID, wakeErr,
		)
		if errors.Is(wakeErr, ErrWakeStatusUnknown) {
			_ = resp.Body.Close()
			t.neverServed = true
			return wakeStatusUnknownResponse(req, t.requestID), nil
		}
		if errors.Is(wakeErr, ErrWakeNotReady) {
			_ = resp.Body.Close()
			t.neverServed = true
			return wakeStartingResponse(req, t.requestID), nil
		}
		t.neverServed = true
		return resp, nil
	}

	_ = resp.Body.Close()
	retry := req.Clone(req.Context())
	if req.GetBody != nil {
		retry.Body, err = req.GetBody()
		if err != nil {
			return nil, fmt.Errorf("wake: restore request body: %w", err)
		}
	}
	resp, err = t.base.RoundTrip(retry)
	if err != nil {
		return nil, err
	}
	cold, err = bufferColdCandidateWithin(req.Context(), resp, t.candidateReadTimeout())
	if err != nil {
		return nil, err
	}
	if cold {
		_ = resp.Body.Close()
		t.neverServed = true
		return wakeStartingResponse(req, t.requestID), nil
	}
	return resp, nil
}

const (
	coldCandidatePrefixLimit = 64 << 10
	coldCandidateReadTimeout = 5 * time.Second
)

func (t *wakeRoundTripper) candidateReadTimeout() time.Duration {
	if t.server.wakeTimeout > 0 && t.server.wakeTimeout < coldCandidateReadTimeout {
		return t.server.wakeTimeout
	}
	return coldCandidateReadTimeout
}

type replayReadCloser struct {
	io.Reader
	closer io.Closer
}

func (r *replayReadCloser) Close() error { return r.closer.Close() }

type terminalErrorReader struct {
	err error
}

func (r *terminalErrorReader) Read([]byte) (int, error) {
	err := r.err
	r.err = nil
	if err == nil {
		return 0, io.EOF
	}
	return 0, err
}

type candidateReadResult struct {
	data []byte
	err  error
}

// candidateBody keeps exactly one goroutine reading the network body. During
// classification, reads can be abandoned at the inspection deadline without
// abandoning the in-flight network read. The response consumer then receives
// that result and all later bytes through the same reader.
type candidateBody struct {
	body     io.ReadCloser
	requests chan int
	results  chan candidateReadResult
	done     chan struct{}

	readMu      sync.Mutex
	pending     bool
	terminal    bool
	buffered    []byte
	bufferedErr error
	closeOnce   sync.Once
	closeResult error
}

func newCandidateBody(body io.ReadCloser) *candidateBody {
	b := &candidateBody{
		body:     body,
		requests: make(chan int),
		results:  make(chan candidateReadResult, 1),
		done:     make(chan struct{}),
	}
	go b.readNetworkBody()
	return b
}

func (b *candidateBody) readNetworkBody() {
	for {
		var size int
		select {
		case size = <-b.requests:
		case <-b.done:
			return
		}
		buf := make([]byte, size)
		n, err := b.body.Read(buf)
		result := candidateReadResult{data: buf[:n], err: err}
		select {
		case b.results <- result:
		case <-b.done:
			return
		}
		if err != nil {
			return
		}
	}
}

func (b *candidateBody) Read(p []byte) (int, error) {
	return b.readWithin(context.Background(), p)
}

func (b *candidateBody) readWithin(ctx context.Context, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.readMu.Lock()
	defer b.readMu.Unlock()
	if len(b.buffered) > 0 {
		n := copy(p, b.buffered)
		b.buffered = b.buffered[n:]
		if len(b.buffered) > 0 {
			return n, nil
		}
		err := b.bufferedErr
		b.bufferedErr = nil
		if err != nil {
			b.terminal = true
		}
		return n, err
	}
	if b.terminal {
		return 0, io.EOF
	}
	if !b.pending {
		select {
		case b.requests <- len(p):
			b.pending = true
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-b.done:
			return 0, io.ErrClosedPipe
		}
	}
	select {
	case result := <-b.results:
		b.pending = false
		n := copy(p, result.data)
		if n < len(result.data) {
			b.buffered = result.data[n:]
			b.bufferedErr = result.err
			return n, nil
		}
		if result.err != nil {
			b.terminal = true
		}
		return n, result.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-b.done:
		return 0, io.ErrClosedPipe
	}
}

func (b *candidateBody) Close() error {
	b.closeOnce.Do(func() {
		close(b.done)
		b.closeResult = b.body.Close()
	})
	return b.closeResult
}

// bufferColdCandidate inspects only statuses that can be cold. A response may
// enter a long wake hold only when its complete body fits within the fixed
// bound. Then the network body is closed and replaced with an in-memory replay
// before Wake starts. Oversized candidates are restored and returned as
// non-cold without waiting. If the inspection deadline expires before the
// body is complete, the bytes received so far and the still-live body reader
// are handed to the response consumer. An incomplete candidate is not enough
// evidence to wake, and the inspection deadline must not truncate or rewrite
// the upstream response.
func bufferColdCandidate(resp *http.Response) (bool, error) {
	return bufferColdCandidateWithin(context.Background(), resp, coldCandidateReadTimeout)
}

func bufferColdCandidateWithin(ctx context.Context, resp *http.Response, timeout time.Duration) (bool, error) {
	if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusServiceUnavailable {
		return false, nil
	}
	body := newCandidateBody(resp.Body)
	readCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	prefix := make([]byte, 0, coldCandidatePrefixLimit+1)
	var readErr error
	for len(prefix) <= coldCandidatePrefixLimit && readErr == nil {
		remaining := coldCandidatePrefixLimit + 1 - len(prefix)
		buf := make([]byte, remaining)
		var n int
		n, readErr = body.readWithin(readCtx, buf)
		prefix = append(prefix, buf[:n]...)
	}
	if readCtx.Err() != nil {
		if ctx.Err() != nil {
			_ = body.Close()
			return false, fmt.Errorf("wake: read cold-candidate response body: %w", ctx.Err())
		}
		resp.Body = &replayReadCloser{Reader: io.MultiReader(bytes.NewReader(prefix), body), closer: body}
		return false, nil
	}
	if len(prefix) > coldCandidatePrefixLimit {
		if readErr != nil {
			_ = body.Close()
			resp.Body = &replayReadCloser{Reader: io.MultiReader(bytes.NewReader(prefix), &terminalErrorReader{err: readErr}), closer: body}
			return false, nil
		}
		resp.Body = &replayReadCloser{Reader: io.MultiReader(bytes.NewReader(prefix), body), closer: body}
		return false, nil
	}
	_ = body.Close()
	if readErr != nil {
		resp.Body = &replayReadCloser{Reader: io.MultiReader(bytes.NewReader(prefix), &terminalErrorReader{err: readErr}), closer: body}
	} else {
		resp.Body = &replayReadCloser{Reader: bytes.NewReader(prefix), closer: body}
	}
	return resp.StatusCode == http.StatusNotFound || bodyLooksNotReady(prefix), nil
}

func bufferedHTTPResponse(req *http.Request, buf *bufferingResponseWriter, neverServed bool) *http.Response {
	header := buf.header.Clone()
	if neverServed {
		header.Del(wakeNeverServedHeader)
	}
	return &http.Response{
		StatusCode:    buf.status,
		Status:        strconv.Itoa(buf.status) + " " + http.StatusText(buf.status),
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(buf.body)),
		ContentLength: int64(len(buf.body)),
		Request:       req,
	}
}

func wakeStartingResponse(req *http.Request, requestID string) *http.Response {
	buf := newBufferingResponseWriter()
	writeWakeStarting(buf, requestID)
	return bufferedHTTPResponse(req, buf, true)
}

func wakeStatusUnknownResponse(req *http.Request, requestID string) *http.Response {
	buf := newBufferingResponseWriter()
	writeWakeStatusUnknown(buf, requestID)
	return bufferedHTTPResponse(req, buf, true)
}

func (s *Server) newWakeTransport(
	base http.RoundTripper,
	upstream *url.URL,
	id identity.Identity,
	servedModel string,
	requestID string,
	lease *admission.Lease,
) *wakeRoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	target := WakeTarget{
		UpstreamHost: upstream.Host,
		GraphK8sName: id.GraphK8sName,
		ResourceID:   id.ResourceID,
		ServedModel:  servedModel,
		ServingMode:  id.ServingMode,
	}
	if target.GraphK8sName == "" {
		target.GraphK8sName = graphFromUpstreamHost(upstream.Host)
	}
	return &wakeRoundTripper{
		server: s, base: base, target: target, requestID: requestID, lease: lease,
	}
}
