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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/saturncloud/phoebe/internal/capture"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/iolog"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
)

// requestIDHeader is the per-request idempotency key. vLLM/the router echo a
// request id; we also accept an inbound one. Captured for the metering event.
//
// SECURITY: unlike the X-Saturn-* identity headers, X-Request-Id is NOT on the
// Traefik auth-server allowlist — it is client-controlled. The value becomes
// billing_event's PRIMARY KEY (the billing idempotency key), so garbage here is
// a billing-integrity attack surface, the same class as the identity gate in
// handleProxy: an omitted id would make the event undecodable downstream
// (served-but-never-billed), and an oversize/binary one would poison the
// drainer's batch INSERT. handleProxy therefore generates an id when absent and
// fails closed (400) on an invalid one.
//
// KNOWN LIMITATION: server-side generation does not stop a client deliberately
// RESENDING a previously billed valid id — the billing_event PK dedups it, so
// the replayed request is served but stores no new row (free inference). The
// drainer cannot distinguish client replay from at-least-once stream
// redelivery, so the true fix lives at the auth/edge layer (an allowlisted,
// edge-stamped id). Documented in DESIGN.md §1 (trust model).
const requestIDHeader = "X-Request-Id"

// maxRequestIDLen bounds an inbound X-Request-Id. The billing_event PK column
// is VARCHAR(255); 200 leaves headroom so a valid id can never fail the
// drainer's INSERT on length.
const maxRequestIDLen = 200

// validRequestID reports whether a client-supplied X-Request-Id is safe to use
// as the billing idempotency key: at most maxRequestIDLen bytes, every byte in
// [\x21-\x7e] (printable ASCII, no spaces/control bytes). Anything else is
// rejected fail-closed — see the requestIDHeader comment for why.
func validRequestID(id string) bool {
	if len(id) > maxRequestIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] < 0x21 || id[i] > 0x7e {
			return false
		}
	}
	return true
}

// generateRequestID mints a server-side request id (16 bytes crypto/rand, hex)
// for requests that arrive without one, so omitting the header can never dodge
// billing. The "phoebe-" prefix makes generated ids recognisable downstream.
func generateRequestID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "phoebe-" + hex.EncodeToString(b[:]), nil
}

// Server is the interceptor HTTP server.
type Server struct {
	settings *config.Settings
	log      *logging.Logger
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
func New(s *config.Settings, log *logging.Logger, emitter metering.Emitter) *Server {
	return &Server{
		settings: s,
		log:      log,
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
func NewWithIOLog(s *config.Settings, log *logging.Logger, emitter metering.Emitter,
	policy iolog.Policy, sink iolog.Sink, maxBodyLen int) *Server {
	srv := New(s, log, emitter)
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
// Abort detection (M3): the captureReader reads r.Context().Err() at
// finalization (finish()) to decide Aborted — no separate watcher goroutine.
// ReverseProxy cancels the upstream request context on client disconnect, which
// fails the body reads and triggers Close(); by the time finish() runs, ctx.Err()
// is non-nil, so onDone sees Aborted=true. The once-guard in finish() ensures
// onDone fires exactly once regardless of whether EOF or Close reaches it first.
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

	// Request-id gate: the id is the billing idempotency PK and X-Request-Id is
	// client-controlled (not on the Traefik allowlist), so it gets the same
	// fail-closed treatment as the identity gate above. Absent → generate (a
	// client must never be served-but-unbilled by simply omitting the header);
	// invalid → 400 (an oversize or non-printable id is a billing-integrity
	// attack, not a normal request). See the requestIDHeader comment for the
	// full threat model, including the deliberate-reuse limitation.
	requestID := r.Header.Get(requestIDHeader)
	switch {
	case requestID == "":
		generated, err := generateRequestID()
		if err != nil {
			// crypto/rand failing means we cannot mint the billing key; fail
			// closed rather than serve an unbillable request.
			s.log.Error.Printf("generate request id: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		requestID = generated
		// Propagate the generated id onto the forwarded request so upstream
		// logs correlate; the response header is set in ModifyResponse below.
		r.Header.Set(requestIDHeader, requestID)
	case !validRequestID(requestID):
		s.log.Warn.Printf("rejecting invalid %s (len=%d) resource=%s auth=%s",
			requestIDHeader, len(requestID), id.ResourceID, id.AuthID)
		http.Error(w, "invalid "+requestIDHeader+": must be at most 200 printable ASCII (no spaces) characters",
			http.StatusBadRequest)
		return
	}

	// The forward target comes ONLY from the X-Saturn-Upstream header that Atlas
	// injected on this deployment's per-subdomain route (see identity.HeaderUpstream).
	// phoebe makes no routing decision of its own — it forwards to the address on
	// the envelope. FAIL CLOSED: an absent or unparseable upstream is refused, never
	// forwarded to a default or a guess. There is no other source of a target, so
	// misrouting authenticated traffic is structurally impossible, not just avoided.
	upstream, err := parseUpstream(r.Header.Get(identity.HeaderUpstream))
	if err != nil {
		// No usable upstream on a request that passed auth. Either the route wasn't
		// built for phoebe (misconfiguration) or the header was stripped — refuse,
		// don't invent a target. Log loudly with the resource for diagnosis.
		s.log.Error.Printf("refusing %q: %v (resource_id=%q)", identity.HeaderUpstream, err, id.ResourceID)
		http.Error(w, "no upstream for this request", http.StatusBadGateway)
		return
	}

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
	var reqTruncated bool
	var startTime time.Time
	if shouldLog {
		startTime = time.Now()
		// Cap the LOGGED request-body copy at the same bound as the response copy
		// (s.ioMaxBodyLen) — an uncapped body flows into to_tsvector and fails the
		// INSERT past ~1 MiB. The forwarded request keeps the full body.
		var reqOrigLen int
		reqBody, reqTruncated, reqOrigLen, err = captureRequestBody(r, s.ioMaxBodyLen)
		if err != nil {
			s.log.Error.Printf("capture request body: %v", err)
			http.Error(w, "bad request body", http.StatusBadRequest)
			return
		}
		// Hard truncation is intentional (an uncapped body fails the to_tsvector
		// INSERT), but make it OBSERVABLE: an operator should be able to see that
		// a captured body was cut, and by how much.
		if reqTruncated {
			s.log.Warn.Printf("iolog: request body truncated for capture request_id=%s (%d → %d bytes)",
				requestID, reqOrigLen, s.ioMaxBodyLen)
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
		// Echo the request id to the client (Set, not Add, so an upstream echo
		// can't duplicate it) — with a generated id this is the client's only
		// handle for correlating a support question to its billing record.
		resp.Header.Set(requestIDHeader, requestID)

		streamed := isEventStream(resp)
		statusCode := resp.StatusCode

		// Declared before onDone so the callback can capture it by reference
		// (the closure reads cr.capturedBody() for M5 logging).
		var cr *captureReader

		onDone := func(res capture.Result) {
			// Everything downstream of onDone gets a context DECOUPLED from the
			// client request: onDone runs on the abort path precisely BECAUSE
			// r.Context() was cancelled (that is how Aborted is detected), and
			// an aborted request is the one we most need to durably bill — and
			// whose bodies we most want logged. A cancelled context must never
			// be able to drop the emit OR the I/O-log record (a sink that
			// honours ctx would otherwise lose every aborted request).
			ctx := context.WithoutCancel(r.Context())
			// Metering (durable) always fires.
			s.emit(ctx, id, requestID, res)
			// M5 I/O logging (best-effort) only when this request opted in.
			if shouldLog {
				respBody, truncated := cr.capturedBody()
				rec := iolog.Record{
					RequestID:    requestID,
					AuthID:       id.AuthID,
					UserID:       id.UserID,
					GroupID:      id.GroupID,
					ResourceID:   id.ResourceID,
					ResourceType: id.ResourceType,
					// Engine-reported model name, not the routing resource id.
					Model:             res.Model,
					RequestBody:       reqBody,
					RequestTruncated:  reqTruncated,
					ResponseBody:      respBody,
					ResponseTruncated: truncated,
					StatusCode:        statusCode,
					Streamed:          res.Streamed,
					LatencyMs:         time.Since(startTime).Milliseconds(),
					Timestamp:         time.Now(),
				}
				// Sink.Log is async/non-blocking and best-effort — it never
				// blocks this completion callback or the client response.
				s.ioSink.Log(ctx, rec)
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

	rp.ErrorHandler = s.errorHandler(upstream.String(), id, requestID)

	rp.ServeHTTP(w, r)
}

// errorHandler builds the ReverseProxy ErrorHandler for one request. The handler
// fires for BOTH RoundTrip errors and ModifyResponse-returned errors, but only
// CLIENT ABORTS (disconnect / deadline) are special-cased: an abort is logged at
// debug and must not 502 a connection that is already gone, while a genuine
// upstream/transport fault is logged at error and 502'd. The single isClientAbort
// predicate decides which.
//
// THE ABORT-EMIT (Fix A) — money-path observability. ModifyResponse installs the
// captureReader whose onDone emits the metering event; but a client that aborts
// BEFORE the upstream writes its response headers fails RoundTrip, so
// ModifyResponse never runs and onDone never fires — the request would pass the
// billing-identity gate yet emit NOTHING, leaving served-or-attempted traffic
// completely invisible to billing/reconciliation. So on an abort we emit exactly
// ONE zero-token, attributable event (Aborted=true, no usage) with the already-
// resolved identity, via the SAME s.emit path the completion path uses — so the
// pre-header abort obeys the SAME BillPartialOnAbort policy (no second policy).
//
// Invariant: every request past the billing-identity gate emits exactly one
// attributable event — real usage on completion, or a zero-token Aborted event on
// disconnect (pre- OR post-header).
//
// NO double-emit: in phoebe ModifyResponse always returns nil, so ErrorHandler
// fires ONLY on a RoundTrip error (pre-header) — mutually exclusive with the
// onDone path (post-header), which needs ModifyResponse to have run. The emit is
// gated on isClientAbort, so a genuine upstream/ModifyResponse fault never writes
// a bogus zero-token billing row. The context is decoupled from the cancelled
// client ctx (WithoutCancel) — the abort is precisely WHY we are here, so a
// cancelled ctx must not be able to drop the emit (mirrors onDone).
func (s *Server) errorHandler(upstream string, id identity.Identity, requestID string) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		if isClientAbort(err) {
			s.log.Debug.Printf("client disconnected for %s", upstream)
			// Pre-header abort: ModifyResponse never ran, so onDone will not emit.
			// Emit a zero-token attributable event so the request is not invisible
			// to billing. Best-effort, non-blocking — like onDone's emit.
			ctx := context.WithoutCancel(r.Context())
			s.emit(ctx, id, requestID, capture.Result{Aborted: true, UsageFound: false})
			return
		}
		s.log.Error.Printf("upstream %s error: %v", upstream, err)
		http.Error(w, "upstream error", http.StatusBadGateway)
	}
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
		// BaseModel is the fine-tune's HF base id (E3 derived_from), injected by
		// atlas-auth at deploy time and carried verbatim. Empty for a base model;
		// for an ft:<checkpoint> Model the rater prices via base x premium. Stamped
		// from the trusted identity header, never from the engine response (the
		// engine doesn't know the deployment's base).
		BaseModel: id.BaseModel,

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

// parseUpstream turns the X-Saturn-Upstream header value into a forwardable URL.
// The value is the backend Atlas addressed for this deployment's route — either a
// bare `host:port` (the common form Atlas injects) or a full `scheme://host:port`.
// A bare host:port defaults to http (engines are plain HTTP inside the cluster).
// Returns an error (→ fail closed) on empty, unparseable, or hostless input — phoebe
// must never forward to a default when the routing authority is missing.
func parseUpstream(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty upstream header")
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("unparseable upstream %q: %w", raw, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("upstream %q has no host", raw)
	}
	return u, nil
}

// isEventStream reports whether the response is an SSE stream.
func isEventStream(resp *http.Response) bool {
	ct := resp.Header.Get("Content-Type")
	return strings.HasPrefix(ct, "text/event-stream")
}

// isClientAbort reports whether a ReverseProxy error is a client-side abort (the
// client disconnected → context.Canceled) as opposed to an upstream/transport
// fault. errors.Is (never ==) because the transport delivers the sentinel WRAPPED
// on the common mid-flight paths (a *url.Error or *net.OpError around it), so an
// identity compare would misclassify a real abort as an upstream error.
//
// DELIBERATELY context.Canceled ONLY — NOT context.DeadlineExceeded. In this
// proxy's config a DeadlineExceeded is ALWAYS an upstream/transport fault, never
// a client one: the request context carries no client deadline (the only
// WithTimeout is the server's graceful-shutdown ctx), and DeadlineExceeded is
// exactly what an upstream dial timeout (the transport's built-in 30s dial
// Timeout) or a response-header timeout returns. Treating it as an abort would
// SILENTLY downgrade an upstream outage to a debug "client disconnected" log,
// drop the 502 the client should get, and emit a spurious zero-token Aborted
// billing row attributing the outage to the tenant. (A prior change widened this
// to include DeadlineExceeded; that was a regression — see TestIsClientAbort.)
//
// This is the SINGLE predicate that decides both ErrorHandler branches: an abort
// is logged at debug + emits a zero-token attributable event (see handleProxy's
// ErrorHandler), while a genuine upstream fault is logged at error + 502'd. The
// two must agree on what "abort" means, hence one helper.
func isClientAbort(err error) bool {
	return errors.Is(err, context.Canceled)
}
