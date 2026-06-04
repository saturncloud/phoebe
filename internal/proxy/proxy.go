// Package proxy is the core of the interceptor: a thin, tenant-aware reverse
// proxy that sits behind Traefik and in front of the inference router/engine.
//
// M1 (this milestone) implements the forward-then-inspect streaming tee:
//   - read trusted identity headers (does NOT authenticate)
//   - resolve the target model's upstream from X-Saturn-Resource-Id
//   - force stream_options.include_usage=true on every streaming request
//   - stream each SSE chunk to the client immediately (per-chunk flush, never
//     buffer-then-forward), capturing the trailing usage block and finish_reason
//     as the bytes pass through
//   - hand the captured result to the metering emitter, off the hot path
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
func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	id := identity.FromRequest(r)

	if id.ResourceID == "" {
		http.Error(w, "missing "+identity.HeaderResourceID, http.StatusBadRequest)
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
	rp.FlushInterval = -1

	rp.ModifyResponse = func(resp *http.Response) error {
		streamed := isEventStream(resp)

		// onDone fires once when the response body is fully read OR closed.
		// Capturing happens on the SAME bytes streamed to the client; emit is
		// handed off off the hot path.
		//
		// NOTE: on a client disconnect, ReverseProxy cancels the upstream and
		// closes this body, so onDone still fires — but with whatever partial
		// usage we'd captured (often none) and Aborted=false. Proper abort
		// handling (mark aborted, emit partial-count event per policy) is M3;
		// the capture.Result already carries the Aborted field for it.
		onDone := func(res capture.Result) {
			s.emit(r.Context(), id, requestID, res)
		}

		resp.Body = newCaptureReader(resp.Body, streamed, onDone)
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
func (s *Server) emit(ctx context.Context, id identity.Identity, requestID string, res capture.Result) {
	if !res.UsageFound && !res.Aborted {
		// No usage and not an abort: a non-OpenAI response or an upstream we
		// can't meter. Log for reconciliation; emit nothing billable.
		s.log.Warn.Printf("no usage captured for resource=%s request_id=%s streamed=%t",
			id.ResourceID, requestID, res.Streamed)
		return
	}

	e := metering.Event{
		RequestID:        requestID,
		GroupID:          id.GroupID,
		UserID:           id.UserID,
		Model:            id.ResourceID,
		PromptTokens:     res.Usage.PromptTokens,
		CachedTokens:     res.Usage.CachedTokens(),
		CompletionTokens: res.Usage.CompletionTokens,
		FinishReason:     res.FinishReason,
		Aborted:          res.Aborted,
	}
	s.emitter.Emit(ctx, e)
}

// isEventStream reports whether the response is an SSE stream.
func isEventStream(resp *http.Response) bool {
	ct := resp.Header.Get("Content-Type")
	return strings.HasPrefix(ct, "text/event-stream")
}
