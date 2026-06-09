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
//   - detect client disconnect by reading the request context at finalization
//     (captureReader.finish reads ctx.Err()), so the emitted event has
//     Aborted=true without a separate watcher goroutine racing the body Close
//   - capture the engine-reported model name from the response body as the
//     stable price key (Event.Model), distinct from the routing resource id
//   - apply BillPartialOnAbort policy in emit: if aborted and usage present,
//     always bill; if aborted and no usage, bill only if BillPartialOnAbort;
//     if not aborted and no usage, log for reconciliation only
package proxy

import (
	"context"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/saturncloud/phoebe/internal/capture"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/iolog"
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

	// ioPolicy gates M5 body capture (opt-in + sampling). ioSink receives the
	// captured Records. Both default to the inert pair (deny-all policy +
	// NopSink) when not supplied, so the hot path is unchanged unless I/O
	// logging is explicitly wired in main.go.
	ioPolicy     iolog.Policy
	ioSink       iolog.Sink
	ioMaxBodyLen int
}

// New constructs a Server from its dependencies. I/O logging is OFF: the policy
// denies every request and the sink is a NopSink, so no bodies are buffered.
// Use NewWithIOLog to enable M5 body capture.
func New(s *config.Settings, log *logging.Logger, resolver registry.Resolver, emitter metering.Emitter) *Server {
	return &Server{
		settings: s,
		log:      log,
		resolver: resolver,
		emitter:  emitter,
		// denyAllPolicy + NopSink = logging fully inert; ShouldLog is never true
		// so no request ever buffers a body. This is the fail-closed default.
		ioPolicy:     denyAllPolicy{},
		ioSink:       iolog.NopSink{},
		ioMaxBodyLen: iolog.DefaultMaxBodyBytes,
	}
}

// NewWithIOLog constructs a Server with M5 I/O logging wired in. policy decides
// per request whether to capture bodies; sink receives the Records. maxBodyLen
// caps the buffered response-body copy (<=0 uses the default). When logging is
// disabled, callers pass denyAllPolicy/NopSink via New instead.
func NewWithIOLog(s *config.Settings, log *logging.Logger, resolver registry.Resolver, emitter metering.Emitter,
	policy iolog.Policy, sink iolog.Sink, maxBodyLen int) *Server {
	srv := New(s, log, resolver, emitter)
	if policy != nil {
		srv.ioPolicy = policy
	}
	if sink != nil {
		srv.ioSink = sink
	}
	if maxBodyLen > 0 {
		srv.ioMaxBodyLen = maxBodyLen
	}
	return srv
}

// denyAllPolicy is the default Policy when I/O logging is off: ShouldLog is
// always false, so the proxy never buffers a request or response body. Keeping
// this as a real Policy (rather than a nil check on the hot path) means the
// gate is a single uniform call site.
type denyAllPolicy struct{}

func (denyAllPolicy) ShouldLog(identity.Identity, string) bool { return false }

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

	requestID := r.Header.Get(requestIDHeader)

	// M5 I/O-logging gate — computed ONCE. Everything that adds hot-path cost
	// (capturing the request body, buffering the response) is guarded by this
	// single boolean. When false (the default, and the common case), the proxy
	// behaves exactly as it did before M5: no extra read, no extra allocation.
	shouldLog := s.ioPolicy.ShouldLog(id, requestID)

	// Capture the ORIGINAL client request body for fidelity — BEFORE
	// forceIncludeUsage rewrites it. We log what the tenant actually sent, not
	// phoebe's internal include_usage injection: when a tenant is debugging a
	// bad response, "what did my client send?" is the useful question, and the
	// rewrite is an implementation detail they never wrote. captureRequestBody
	// reads the body once and restores it so forceIncludeUsage re-reads the
	// same bytes (no double-read). Only runs when shouldLog is true.
	var reqBody string
	var startTime time.Time
	if shouldLog {
		startTime = time.Now()
		reqBody, err = captureRequestBody(r)
		if err != nil {
			s.log.Error.Printf("capture request body: %v", err)
			http.Error(w, "bad request body", http.StatusBadRequest)
			return
		}
	}

	// Force streaming usage so we never under-bill a streamed response.
	if err := forceIncludeUsage(r); err != nil {
		s.log.Error.Printf("rewrite request body: %v", err)
		http.Error(w, "bad request body", http.StatusBadRequest)
		return
	}

	rp := httputil.NewSingleHostReverseProxy(upstream)

	// FlushInterval=-1 flushes every write immediately — per-chunk SSE
	// delivery with no buffering. This is the streaming-correctness linchpin.
	// No WriteTimeout is set on the http.Server (see server.go) — long streams
	// must not be cut by a write deadline.
	rp.FlushInterval = -1

	rp.ModifyResponse = func(resp *http.Response) error {
		streamed := isEventStream(resp)
		statusCode := resp.StatusCode

		// Declared before onDone so the callback can capture it by reference
		// (the closure reads cr.capturedBody() for M5 logging).
		var cr *captureReader

		onDone := func(res capture.Result) {
			// Metering (durable) always fires. Use a context DECOUPLED from the
			// client request: onDone runs on the abort path precisely BECAUSE
			// r.Context() was cancelled (that is how Aborted is detected), and
			// an aborted request is the one we most need to durably bill. A
			// cancelled context must never be able to drop that emit.
			s.emit(context.WithoutCancel(r.Context()), id, requestID, res)
			// M5 I/O logging (best-effort) only when this request opted in.
			if shouldLog {
				respBody, truncated := cr.capturedBody()
				rec := iolog.Record{
					RequestID:         requestID,
					AuthID:            id.AuthID,
					UserID:            id.UserID,
					GroupID:           id.GroupID,
					ResourceID:        id.ResourceID,
					ResourceType:      id.ResourceType,
					// Engine-reported model name, not the routing resource id.
					Model: res.Model,
					RequestBody:       reqBody,
					ResponseBody:      respBody,
					ResponseTruncated: truncated,
					StatusCode:        statusCode,
					Streamed:          res.Streamed,
					LatencyMs:         time.Since(startTime).Milliseconds(),
					Timestamp:         time.Now(),
				}
				// Sink.Log is async/non-blocking and best-effort — it never
				// blocks this completion callback or the client response.
				s.ioSink.Log(r.Context(), rec)
			}
		}

		// The captureReader reads r.Context() to decide Aborted at finalisation
		// time — so a client disconnect is detected without a separate watcher
		// goroutine racing the body's Close(). On abort, ReverseProxy cancels
		// this context, so by the time finish() runs ctx.Err() is non-nil.
		cr = newCaptureReader(r.Context(), resp.Body, streamed, onDone)
		// Enable bounded response-body capture ONLY for opted-in requests (M5).
		// For everything else logBuf stays nil and Read pays no extra cost.
		if shouldLog {
			cr.enableBodyLog(s.ioMaxBodyLen)
		}
		resp.Body = cr
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
		// Model is the ENGINE-REPORTED name (rating's stable price key),
		// captured from the response body — NOT id.ResourceID, which is the
		// ephemeral deployment id and prices nothing. Empty when the upstream
		// emitted no parseable model; rating then fails the event loud rather
		// than billing it wrong.
		Model: res.Model,

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
