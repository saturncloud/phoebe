// Package proxy is the core of the interceptor: a thin, tenant-aware reverse
// proxy that sits behind Traefik and in front of the inference router/engine.
//
// Responsibilities (scaffold; bodies filled in over subsequent milestones):
//   - read trusted identity headers (does NOT authenticate)
//   - resolve the target model's upstream from X-Saturn-Resource-Id
//   - force stream_options.include_usage=true on every request
//   - forward-then-inspect SSE: stream each chunk immediately, capture the
//     trailing usage block, never buffer-then-forward
//   - emit one idempotent metering event per request, off the hot path
package proxy

import (
	"net/http"
	"net/http/httputil"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
	"github.com/saturncloud/phoebe/internal/registry"
)

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

// handleProxy is the single capture point. The streaming tee and metering
// emit are stubbed here; this milestone wires identity → registry → upstream.
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

	rp := httputil.NewSingleHostReverseProxy(upstream)

	// TODO(milestone-streaming): force stream_options.include_usage=true on
	// the request body, and set ModifyResponse to tee SSE chunks
	// forward-then-inspect, capturing the trailing usage block and emitting a
	// metering event keyed by request_id. FlushInterval=-1 ensures per-chunk
	// flushing (no buffering) for streamed responses.
	rp.FlushInterval = -1

	rp.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		s.log.Error.Printf("upstream %s error: %v", upstream, err)
		http.Error(w, "upstream error", http.StatusBadGateway)
	}

	rp.ServeHTTP(w, r)
}
