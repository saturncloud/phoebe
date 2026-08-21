package proxy

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

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
// complete within the deadline (the caller then returns the original cold
// response to the client rather than hanging forever).
type Waker interface {
	Wake(ctx context.Context, target WakeTarget) error
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
// authorized resource. Dedicated routes (no allow-list) are never woken here
// (they don't scale to zero via this path).
func isWakeable(id identity.Identity) bool {
	return id.ResourceID != "" && id.ServedModel != ""
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
func (b *bufferingResponseWriter) flushTo(w http.ResponseWriter) {
	dst := w.Header()
	for k, vs := range b.header {
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

// serveWithWake probes the upstream and, on a cold (scaled-to-zero) response,
// triggers a 0->1 wake and retries. Returns true if it produced the client's
// FINAL response here (a genuine error, or the wake deadline was exceeded and
// the cold response was returned) — the caller then returns. Returns false when
// the base is warm and the caller should perform the normal metered streaming
// forward (the request body has been restored for that attempt).
//
// Only the cold-probe attempts are buffered here (a cold body is tiny). The
// moment a NON-cold response is seen we DON'T serve it from the buffer — we
// return false so the caller re-forwards it with full streaming + metering
// intact. Cost: one extra round-trip on the (rare) first-request-after-idle.
func (s *Server) serveWithWake(
	w http.ResponseWriter,
	r *http.Request,
	upstream *url.URL,
	id identity.Identity,
	requestID string,
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
		probe := httputil.NewSingleHostReverseProxy(upstream)
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
		ctx := r.Context()
		if s.wakeTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, s.wakeTimeout)
			defer cancel()
		}
		if werr := s.waker.Wake(ctx, target); werr != nil {
			// Wake couldn't complete (deadline/scale error): return the cold
			// response to the client rather than hang. It's a real, honest 503/404
			// for a base we couldn't bring up in time.
			s.log.Warn.Printf("wake: could not warm base for request_id=%s resource_id=%s: %v",
				requestID, id.ResourceID, werr)
			buf.flushTo(w)
			return true
		}
		// Woken: loop and re-probe (the next attempt should be warm).
	}

	// Tries exhausted and still cold — serve the last cold response honestly.
	restoreBody()
	buf := newBufferingResponseWriter()
	last := httputil.NewSingleHostReverseProxy(upstream)
	last.ServeHTTP(buf, r)
	buf.flushTo(w)
	return true
}
