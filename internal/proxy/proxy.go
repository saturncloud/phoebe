// Package proxy is the core of the interceptor: a thin, tenant-aware reverse
// proxy that sits behind Traefik and in front of the inference router/engine.
//
// M1 implemented the forward-then-inspect streaming tee. M3 (this milestone)
// wires client-abort detection and applies the bill-partial-on-abort policy:
//   - read trusted identity headers (does NOT authenticate)
//   - resolve the target model's upstream from X-Saturn-Resource-Id
//   - force stream_options.include_usage=true on every streaming request
//   - stream each SSE chunk to the client immediately (per-chunk flush, never
//     buffer-then-forward), capturing the trailing usage block and finish_reason
//     as the bytes pass through
//   - detect client disconnect via the request context and call markAborted()
//     on the captureReader before its onDone fires, so the emitted event has
//     Aborted=true
//   - apply BillPartialOnAbort policy in emit: if aborted and usage present,
//     always bill; if aborted and no usage, bill only if BillPartialOnAbort;
//     if not aborted and no usage, log for reconciliation only
package proxy

import (
	"context"
	"net/http"
	"net/http/httputil"
	"strings"

	"github.com/saturncloud/phoebe/internal/capture"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
	"github.com/saturncloud/phoebe/internal/registry"
)

// requestIDHeader is the per-request idempotency key. vLLM/the router echo a
// request id; we also accept an inbound one. Captured for the metering event.
const requestIDHeader = "X-Request-Id"

// Server is the interceptor HTTP server.
type Server struct {
	settings *config.Settings
	log      *logging.Logger
	resolver registry.Resolver
	emitter  metering.Emitter
}

// New constructs a Server from its dependencies.
func New(s *config.Settings, log *logging.Logger, resolver registry.Resolver, emitter metering.Emitter) *Server {
	return &Server{
		settings: s,
		log:      log,
		resolver: resolver,
		emitter:  emitter,
	}
}

// Handler returns the http.Handler for the interceptor.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/", s.handleProxy)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleProxy is the single capture point: identity → registry → upstream,
// with the streaming tee capturing usage as the response flows to the client.
//
// Abort detection (M3): after ModifyResponse wraps the body in a captureReader,
// a watcher goroutine blocks on r.Context().Done(). On cancellation it calls
// cr.markAborted(), which sets Aborted=true and triggers finish() if it hasn't
// fired yet — guaranteeing onDone sees Aborted=true. ReverseProxy cancels the
// upstream request context on client disconnect, which also causes the upstream
// body reads to fail and Close() to be called; the once-guard in finish()
// ensures onDone fires exactly once regardless of which path reaches it first.
func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	id := identity.FromRequest(r)

	// Billing-identity gate: fail closed if we lack what we need to attribute
	// consumption. A billing product must not serve traffic it can't bill — a
	// missing identity header means the edge contract is broken (auth-server
	// not emitting it, or Traefik not allowlisting it), not a normal request.
	// Report every missing field at once so the misconfiguration is obvious.
	if missing := missingBillingFields(id); len(missing) > 0 {
		s.log.Warn.Printf("rejecting unbillable request: missing %s (request_id=%s)",
			strings.Join(missing, ", "), r.Header.Get(requestIDHeader))
		http.Error(w, "missing required billing identity: "+strings.Join(missing, ", "),
			http.StatusBadRequest)
		return
	}

	upstream, err := s.resolver.Resolve(id.ResourceID)
	if err == registry.ErrNotFound {
		// Torn-down or unknown model: fail cleanly, never hang or misroute.
		http.Error(w, "model upstream not found", http.StatusNotFound)
		return
	}
	if err != nil {
		s.log.Error.Printf("resolve %q: %v", id.ResourceID, err)
		http.Error(w, "upstream resolution error", http.StatusBadGateway)
		return
	}

	// Force streaming usage so we never under-bill a streamed response.
	if err := forceIncludeUsage(r); err != nil {
		s.log.Error.Printf("rewrite request body: %v", err)
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	requestID := r.Header.Get(requestIDHeader)

	rp := httputil.NewSingleHostReverseProxy(upstream)

	// FlushInterval=-1 flushes every write immediately — per-chunk SSE
	// delivery with no buffering. This is the streaming-correctness linchpin.
	// No WriteTimeout is set on the http.Server (see server.go) — long streams
	// must not be cut by a write deadline.
	rp.FlushInterval = -1

	rp.ModifyResponse = func(resp *http.Response) error {
		streamed := isEventStream(resp)

		onDone := func(res capture.Result) {
			s.emit(r.Context(), id, requestID, res)
		}

		// The captureReader reads r.Context() to decide Aborted at finalisation
		// time — so a client disconnect is detected without a separate watcher
		// goroutine racing the body's Close(). On abort, ReverseProxy cancels
		// this context, so by the time finish() runs ctx.Err() is non-nil.
		resp.Body = newCaptureReader(r.Context(), resp.Body, streamed, onDone)
		return nil
	}

	rp.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		// context.Canceled means the client disconnected — an abort, not an
		// upstream fault. Don't log it as an error or write a 502 over a
		// connection that's already gone.
		if err == context.Canceled {
			s.log.Debug.Printf("client disconnected for %s", upstream)
			return
		}
		s.log.Error.Printf("upstream %s error: %v", upstream, err)
		http.Error(w, "upstream error", http.StatusBadGateway)
	}

	rp.ServeHTTP(w, r)
}

// emit builds the metering event from the captured result and hands it to the
// emitter. It must not block the client response — the emitter is responsible
// for async/durable delivery.
//
// Policy (M3):
//   - Aborted + usage captured:    always emit (we have real counts).
//   - Aborted + no usage:          emit only if BillPartialOnAbort; otherwise
//     log for reconciliation.
//   - Not aborted + no usage:      log for reconciliation; never bill.
//   - Not aborted + usage:         always emit (normal completion).
func (s *Server) emit(ctx context.Context, id identity.Identity, requestID string, res capture.Result) {
	if res.Aborted && !res.UsageFound {
		if !s.settings.BillPartialOnAbort {
			// Policy: don't bill partial aborts with no token data. Log for
			// reconciliation so the event is not silently lost.
			s.log.Warn.Printf("aborted, no usage, not billing (BillPartialOnAbort=false) resource=%s request_id=%s streamed=%t",
				id.ResourceID, requestID, res.Streamed)
			return
		}
		// BillPartialOnAbort=true: emit a partial event with zero counts so
		// downstream knows we attempted to bill and can reconcile if needed.
		s.log.Debug.Printf("aborted, no usage, emitting partial event resource=%s request_id=%s streamed=%t",
			id.ResourceID, requestID, res.Streamed)
	}

	if !res.UsageFound && !res.Aborted {
		// No usage and not an abort: a non-OpenAI response or an upstream we
		// can't meter. Log for reconciliation; emit nothing billable.
		s.log.Warn.Printf("no usage captured for resource=%s request_id=%s streamed=%t",
			id.ResourceID, requestID, res.Streamed)
		return
	}

	e := metering.Event{
		RequestID: requestID,
		// Identity captured verbatim — attribution resolved downstream.
		AuthID:       id.AuthID,
		UserID:       id.UserID,
		GroupID:      id.GroupID,
		ResourceID:   id.ResourceID,
		ResourceType: id.ResourceType,
		Model:        id.ResourceID,

		PromptTokens:     res.Usage.PromptTokens,
		CachedTokens:     res.Usage.CachedTokens(),
		CompletionTokens: res.Usage.CompletionTokens,
		FinishReason:     res.FinishReason,
		Aborted:          res.Aborted,
	}
	s.emitter.Emit(ctx, e)
}

// missingBillingFields returns the names of the identity headers required to
// bill a request that are absent. Empty result means the request is billable.
//
// AuthID (token / API-key id) is the attribution key; ResourceID identifies
// the model being billed. Both are mandatory. UserID/GroupID are resolved
// downstream from AuthID and are NOT required here.
func missingBillingFields(id identity.Identity) []string {
	var missing []string
	if id.AuthID == "" {
		missing = append(missing, identity.HeaderAuthID)
	}
	if id.ResourceID == "" {
		missing = append(missing, identity.HeaderResourceID)
	}
	return missing
}

// isEventStream reports whether the response is an SSE stream.
func isEventStream(resp *http.Response) bool {
	ct := resp.Header.Get("Content-Type")
	return strings.HasPrefix(ct, "text/event-stream")
}
