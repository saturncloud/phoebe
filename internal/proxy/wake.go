package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/capture"
	"github.com/saturncloud/phoebe/internal/identity"
)

// WakeTarget identifies WHAT a wake actuates, resolved by the proxy ONCE (see
// serveWithWake) so the actuator never re-parses strings the proxy composed.
type WakeTarget struct {
	// UpstreamHost is the trusted upstream host:port the cold response came
	// from (the readiness-probe address).
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
//   - Any other failure (no scalable adapter for the graph, an RBAC or API
//     error on the scale patch, a malformed target): Wake returns an error that
//     does NOT wrap ErrWakeNotReady. Nothing is starting, so telling the client
//     to retry would be false; the caller returns the original cold upstream
//     response unchanged.
type Waker interface {
	Wake(ctx context.Context, target WakeTarget) error
}

// ErrWakeNotReady is the error a Waker wraps when it triggered the 0->1
// scale-up (or found the graph already scaled up) but the worker did not
// become ready before the context expired. It is the only wake error that
// turns the held cold response into a 503 + Retry-After (see Waker).
var ErrWakeNotReady = errors.New("wake triggered but the worker was not ready before the deadline")

// wakeRetryAfterSeconds is the Retry-After value (in seconds) phoebe sends on
// the 503 it returns when a scale-up from zero is in progress but the worker
// was not ready within the wake hold (wake.timeout). Long enough not to hammer
// a graph that is still pulling its image or loading weights, short enough
// that a client retrying on it is served soon after the worker becomes ready.
const wakeRetryAfterSeconds = 30

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

// isWakeable reports whether a request is eligible for wake-from-zero: it must be
// a shared-mode route (a served-model allow-list was injected by Atlas) AND carry
// a valid atlas-authorized resource id. This is what disambiguates "cold parked
// base, wake it" from "model genuinely doesn't exist" — the latter has no such
// authorized resource. Dedicated routes also carry a served-model binding now,
// so ServingMode is the authoritative discriminator; dedicated capacity never
// scales to zero through this path.
func isWakeable(id identity.Identity) bool {
	return id.ServingMode == identity.ServingModeShared && id.ResourceID != "" && id.ServedModel != ""
}

// graphFromUpstreamHost derives the Dynamo graph (DGD) k8s name from a
// trusted upstream host for HEADER-ROUTED wakes (the gateway path threads the
// graph name from resolution instead — see serveWithWake). The upstream's
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

// bufferingResponseWriter captures an upstream response in memory instead of
// streaming it to the client, so a wakeable-cold response can be DISCARDED and
// the request retried after a wake — without the client ever seeing the cold
// 404/503. Used ONLY on the (rare) wake path; the normal path streams directly.
//
// A cold response is small (a JSON error body), so buffering it is cheap. If a
// response turns out NOT to be a cold-and-wakeable case, its captured bytes are
// flushed to the real client verbatim (flushTo), preserving status + headers.
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

// flushTo writes the captured status, headers, and body to the real client.
// The request-correlation header is never copied from the buffer: the caller
// Sets the authoritative minted value on w before flushing (an upstream echo
// must not duplicate it — same "Set, not Add" contract as ModifyResponse).
func (b *bufferingResponseWriter) flushTo(w http.ResponseWriter) {
	dst := w.Header()
	for k, vs := range b.header {
		if strings.EqualFold(k, requestIDHeader) {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
	w.WriteHeader(b.status)
	if len(b.body) > 0 {
		_, _ = w.Write(b.body)
	}
}

// wakeEnabled reports whether this request should go through the wake path: a
// waker is configured AND the route is wakeable (shared-mode + authorized
// resource id). Everything else streams directly with zero wake overhead.
func (s *Server) wakeEnabled(id identity.Identity) bool {
	return s.waker != nil && isWakeable(id)
}

// statusRecorder wraps the client ResponseWriter to capture the status code a
// helper actually writes, so a served response that bypassed the normal
// metered forward can be recorded at the status the client really received.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(p)
}

// emitWakeFailureRow records a forwarded-but-unmetered wake attempt. Every
// exit path below that returns served=true has already forwarded at least one
// real probe of the customer's request to the upstream (probe.ServeHTTP /
// last.ServeHTTP) and then serves the response directly, so the normal metered
// forward — and errorHandler with it — never runs. The merged billing
// contract (#48) requires exactly one row per forwarded attempt: Aborted=false,
// UsageFound=false, at the status actually written to the client. Same shape
// as errorHandler's fall-through row, same WithoutCancel ctx (the emit must
// survive a cancelled client ctx), and $0 by construction (no authoritative
// counts). The fall-through (return false) path must NOT call this — the
// caller's normal forward meters that attempt. clientRequestID is captured by
// handleProxy BEFORE it replaces the X-Request-Id header with the minted
// attempt id, so it is threaded in explicitly; the column is forensic only.
func (s *Server) emitWakeFailureRow(r *http.Request, id identity.Identity, requestID, clientRequestID string, statusCode int) {
	s.emit(context.WithoutCancel(r.Context()), id, requestID,
		clientRequestID, statusCode, capture.Result{UsageFound: false})
}

// serveWithWake probes the upstream and, on a cold (scaled-to-zero) response,
// triggers a 0->1 wake and retries. Returns true if it produced the client's
// FINAL response here (a genuine error; a 503 + Retry-After because the
// scale-up is in progress but the worker was not ready within the hold; or the
// cold response passed through because the wake failed without starting a
// scale-up) — the caller then returns. Returns false when
// the base is warm and the caller should perform the normal metered streaming
// forward (the request body has been restored for that attempt).
//
// Every served=true exit below has already forwarded at least one real probe,
// so each emits exactly one raw reconciliation row (Aborted=false,
// UsageFound=false) at the status the client received — see emitWakeFailureRow.
// The return-false path emits nothing here; the caller's forward meters it.
//
// Only the cold-probe attempts are buffered here (a cold body is tiny). The
// moment a NON-cold response is seen we DON'T serve it from the buffer — we
// return false so the caller re-forwards it with full streaming + metering
// intact. Cost: one extra round-trip on the (rare) first-request-after-idle.
//
// SETTLEMENT (R1 ruling): when this function returns true with a FINAL
// answer to a cold probe (the wake-error give-up, or tries exhausted with the
// final probe STILL cold), the engine provably did no inference work — every
// dispatch was refused before any token ran — so the admitted lease is settled
// to zero here (settlementZeroNeverServed), in addition to the 503 (or the
// passed-through cold response) being written to the client. A tries-exhausted final probe that is NOT cold (warm, or a
// non-cold transport/overload error) returns false instead: the R1
// never-served precondition does not hold for it, so the caller's metered
// forward owns both the metering row and the settlement (actual usage, or
// conservative-unknown for an indeterminate fault). The rejection path
// (BeginColdHold error) does NOT settle: the engine answered cold but the
// request is refused pre-dispatch, and the handler's deferred release fallback
// owns that lease release. A successful wake returns false without settling:
// the caller's metered forward owns settlement.
func (s *Server) serveWithWake(
	w http.ResponseWriter,
	r *http.Request,
	upstream *url.URL,
	id identity.Identity,
	requestID, clientRequestID string,
	lease *admission.Lease,
) (served bool) {
	// Snapshot the (already include-usage-rewritten) request body so it can be
	// replayed on each probe + the final forward. nil body is fine.
	body, err := readAndRestoreBody(r)
	if err != nil {
		s.log.Error.Printf("wake: snapshot request body: %v (request_id=%s)", err, requestID)
		http.Error(w, "bad request body", http.StatusBadRequest)
		return true
	}
	restoreBody := func() {
		if body != nil {
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.ContentLength = int64(len(body))
		}
	}

	// Resolve the wake target ONCE. The gateway path already knows the graph
	// (tf_model.graph_k8s_name, carried on the identity by resolveGateway);
	// header-routed requests derive it from the upstream host here — the
	// single parse site, never re-parsed by the actuator.
	target := WakeTarget{
		UpstreamHost: upstream.Host,
		GraphK8sName: id.GraphK8sName,
		ResourceID:   id.ResourceID,
	}
	if target.GraphK8sName == "" {
		target.GraphK8sName = graphFromUpstreamHost(upstream.Host)
	}

	maxTries := s.wakeMaxTries
	if maxTries < 1 {
		maxTries = 1
	}
	for attempt := 0; attempt < maxTries; attempt++ {
		restoreBody()
		buf := newBufferingResponseWriter()
		probe := newUpstreamProxy(upstream)
		// A probe failure (upstream unreachable) is not a cold signal — surface
		// it as a bad gateway, same as the normal error handler would.
		probe.ErrorHandler = func(pw http.ResponseWriter, _ *http.Request, e error) {
			s.log.Warn.Printf("wake: probe upstream error: %v (request_id=%s)", e, requestID)
			pw.WriteHeader(http.StatusBadGateway)
		}
		probe.ServeHTTP(buf, r)

		if !buf.isColdWakeable() {
			// Warm (or a non-cold error): let the caller do the real metered
			// streaming forward. Restore the body for that attempt.
			restoreBody()
			return false
		}

		// Cold. Trigger the wake and block until ready (bounded), then retry.
		if lease != nil {
			if aerr := lease.BeginColdHold(r.Context()); aerr != nil {
				// A probe was already forwarded above; this served=true exit
				// bypasses the normal metered forward, so record the attempt.
				// The minted id is the client's only correlation handle to the
				// reconciliation row — same contract as every errorHandler exit
				// (writeAdmissionError stamps it on the response).
				//
				// The cold-hold gate refused to extend the lease for a wake
				// (capacity rejection or unavailable store). The engine
				// answered cold — no inference ran — but this is a rejection,
				// not a completed response: fail closed without usage-settling
				// here; the handler's deferred release fallback owns the lease
				// release on this exit path.
				rec := &statusRecorder{ResponseWriter: w}
				s.writeAdmissionError(rec, requestID, aerr)
				s.emitWakeFailureRow(r, id, requestID, clientRequestID, rec.status)
				return true
			}
		}
		ctx := r.Context()
		if s.wakeTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, s.wakeTimeout)
			defer cancel()
		}
		werr := s.waker.Wake(ctx, target)
		if lease != nil {
			if aerr := lease.EndColdHold(context.WithoutCancel(r.Context())); aerr != nil {
				s.admissionColdHoldReleaseLog.logf(s.log, "admission: cold-hold release failed: %v", aerr)
			}
		}
		if werr != nil {
			// Wake couldn't complete: answer now rather than hang. Either way
			// the outcome is determinate never-served (no inference ran), so
			// the lease settles zero before the response is written. The probe
			// above was a real forwarded attempt and this served=true exit
			// bypasses the normal metered forward, so the attempt is recorded
			// at the status the client received.
			s.log.Warn.Printf("wake: could not warm base for request_id=%s resource_id=%s: %v",
				requestID, id.ResourceID, werr)
			s.settleFinalColdLease(r, lease)
			if errors.Is(werr, ErrWakeNotReady) {
				// The scale-up is in progress but the worker was not ready
				// within the hold: 503 + Retry-After, never the cold 404.
				writeWakeStarting(w, requestID)
				s.emitWakeFailureRow(r, id, requestID, clientRequestID, http.StatusServiceUnavailable)
				return true
			}
			// No scale-up is in progress (no adapter, RBAC/API error): pass
			// the cold upstream response through unchanged.
			w.Header().Set(requestIDHeader, requestID)
			buf.flushTo(w)
			s.emitWakeFailureRow(r, id, requestID, clientRequestID, buf.status)
			return true
		}
		// Woken: loop and re-probe (the next attempt should be warm).
	}

	// Tries exhausted — dispatch ONE more buffered probe and classify it.
	// Still genuinely cold: same determinate never-served axis as the wake-error
	// give-up — every dispatch was refused before any inference, so the lease
	// settles zero before the flush.
	//
	// NOT cold (a warm response, or a non-cold error such as a transport
	// 502/500): the R1 "engine provably did no work" precondition does NOT
	// hold. Serving the buffer here would hand the client a real response
	// settled Complete(0) and never metered (a billing hole), or an
	// unclassified 502 settled never-served zero (indeterminate faults must
	// keep the conservative reservation). Return false so the caller's normal
	// metered streaming forward classifies the outcome — exactly the in-loop
	// not-cold handling above.
	restoreBody()
	buf := newBufferingResponseWriter()
	last := newUpstreamProxy(upstream)
	last.ServeHTTP(buf, r)
	if !buf.isColdWakeable() {
		restoreBody()
		return false
	}
	// Still cold after every wake reported success: the scale-up was triggered
	// (Wake returned nil), so the model is starting, not missing. Same answer
	// as a wake that ran out of time: 503 + Retry-After, never the cold 404.
	s.settleFinalColdLease(r, lease)
	writeWakeStarting(w, requestID)
	s.emitWakeFailureRow(r, id, requestID, clientRequestID, http.StatusServiceUnavailable)
	return true
}

// settleFinalColdLease settles an admitted lease to zero for a final cold
// response (the engine provably did no work). Settlement is first-writer-wins:
// the handler's deferred release fallback becomes a no-op once this lands, and
// vice versa. The context is decoupled from the client request: the settlement
// must survive a client disconnect racing the cold flush (mirrors the error
// handler and onDone).
func (s *Server) settleFinalColdLease(r *http.Request, lease *admission.Lease) {
	if lease == nil {
		return
	}
	ctx := context.WithoutCancel(r.Context())
	if err := settleAdmissionLease(ctx, lease, settlementZeroNeverServed, admission.Usage{}); err != nil {
		s.admissionReleaseFallbackLog.logf(s.log, "admission: release fallback failed: %v", err)
	}
}
