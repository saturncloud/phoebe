package proxy

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
)

// gatewayRoute is the proxy's gateway-resolution wiring: the (org, model) →
// tf_model resolver plus the upstream composer for resolved graphs. nil on a
// Server means the gateway is not configured — gateway-marked requests then
// FAIL CLOSED with 503 (never fall through to header routing, which they
// cannot satisfy, and never guess).
type gatewayRoute struct {
	resolver gateway.Resolver
	// upstreamFor composes the forward target for a resolved graph. In
	// production this is always <graph>-frontend.<namespace>.svc.cluster.local
	// :<port> over the configured gateway namespace/port; it is a func field so
	// tests can point resolved requests at a live httptest backend.
	upstreamFor func(graphK8sName string) string
}

// WithGateway enables gateway resolution: requests carrying the trusted
// X-Saturn-Gateway marker resolve their (org, body model=) against Atlas's
// tf_model via resolver, and forward to the resolved graph's frontend Service
// in namespace on port (<=0 uses 8000, matching vLLM's serve port).
//
// FAIL CLOSED on partial wiring: a nil resolver or empty namespace leaves the
// gateway unconfigured (gateway requests 503) rather than composing upstreams
// into a guessed namespace. config.GatewaySettings validation makes that
// unreachable from main; this is the last line.
func (srv *Server) WithGateway(resolver gateway.Resolver, namespace string, port int) *Server {
	if resolver == nil || namespace == "" {
		return srv
	}
	if port <= 0 {
		port = 8000
	}
	srv.gateway = &gatewayRoute{
		resolver: resolver,
		upstreamFor: func(graphK8sName string) string {
			return fmt.Sprintf("%s-frontend.%s.svc.cluster.local:%d", graphK8sName, namespace, port)
		},
	}
	return srv
}

// resolveGateway performs gateway model resolution for a request the trusted
// middleware marked with X-Saturn-Gateway (id.Gateway). On success it mutates
// id in place so the rest of handleProxy — the billing gate, upstream parse,
// wake, metering — runs EXACTLY as it would for a header-routed request:
//
//	ResourceID / BaseModel / Adapter / ServingMode ← the tf_model row (the
//	    billing event is indistinguishable from a header-injected one)
//	Upstream    ← <graph>-frontend.<ns>.svc.cluster.local:<port>
//	ServedModel ← the resolved request model (the route is a shared-style
//	    single-model binding, which also makes it wake-eligible: resolution
//	    succeeded == the route is wakeable)
//
// It returns false after writing the client response when the request must not
// proceed. Every refusal fails closed and echoes NOTHING the caller could use
// as an oracle for which models exist (same discipline as checkModelBinding):
//
//	503 gateway not configured — the marker arrived but this phoebe has no
//	    resolver/namespace; serving would mean guessing a route.
//	403 missing org — the gateway middleware contract guarantees
//	    X-Saturn-Org-Id; its absence means the edge contract is broken, and
//	    without an org, resolution (the tenancy boundary) is impossible.
//	400 unreadable model — no parseable body model=, including DUPLICATE
//	    top-level "model" keys (same ambiguity rejection as the model-binding
//	    path: phoebe must not resolve one duplicate while Dynamo routes the
//	    other).
//	404 unknown model — no tf_model row for (org, model): generic body.
//	503 resolver failure — the DB couldn't answer; phoebe never serves
//	    traffic it can't attribute.
func (s *Server) resolveGateway(w http.ResponseWriter, r *http.Request, id *identity.Identity) bool {
	requestID := r.Header.Get(requestIDHeader)

	if s.gateway == nil {
		s.log.Error.Printf("gateway: refusing gateway-marked request: gateway resolution is not configured on this interceptor (set gateway.enabled/namespace/databaseUrl) org_id=%q request_id=%q",
			id.OrgID, requestID)
		http.Error(w, "gateway not configured", http.StatusServiceUnavailable)
		return false
	}

	// The gateway middleware injects the org alongside the marker; a marked
	// request without one means the edge contract is broken. The org is the
	// tenancy boundary of the lookup, so there is nothing safe to resolve —
	// fail closed, generic body.
	if id.OrgID == "" {
		s.log.Warn.Printf("gateway: refusing gateway-marked request with no %s (edge contract broken) request_id=%q",
			identity.HeaderOrgID, requestID)
		http.Error(w, "forbidden", http.StatusForbidden)
		return false
	}

	body, err := readAndRestoreBody(r)
	if err != nil {
		s.log.Error.Printf("gateway: read request body: %v (request_id=%q)", err, requestID)
		http.Error(w, "bad request body", http.StatusBadRequest)
		return false
	}
	model, ok := extractRequestModel(body)
	if !ok {
		// No body, not a JSON object, no/empty model, or duplicate top-level
		// "model" keys (see extractRequestModel — the same duplicate-key
		// rejection the binding check applies, for the same reason: phoebe must
		// not validate one key while the downstream router honors another).
		s.log.Warn.Printf("gateway: refusing request with no unambiguous body model= org_id=%s request_id=%q",
			id.OrgID, requestID)
		http.Error(w, "request body must carry a single model field", http.StatusBadRequest)
		return false
	}

	res, err := s.gateway.resolver.Resolve(r.Context(), id.OrgID, model)
	switch {
	case errors.Is(err, gateway.ErrNotFound):
		// Unknown (org, model) — the org has no such addressable model. The
		// body is GENERIC: no model echo, exactly like the model-binding 403
		// (no oracle for probing which models exist). Server-side the model is
		// logged for ops (a customer's "my model 404s" is undebuggable without
		// it); logs are not client-visible.
		s.log.Warn.Printf("gateway: no model for org_id=%s model=%q request_id=%q", id.OrgID, model, requestID)
		http.Error(w, "model not found", http.StatusNotFound)
		return false
	case err != nil:
		// Resolver (DB) failure: fail closed — phoebe must never forward
		// traffic it cannot attribute, and must never guess a route.
		s.log.Error.Printf("gateway: resolve failed org_id=%s request_id=%q: %v", id.OrgID, requestID, err)
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return false
	}

	id.ResourceID = res.ResourceID
	id.BaseModel = res.BaseModel
	id.Adapter = res.Adapter
	id.ServingMode = res.ServingMode
	// The resolved model is definitionally the single model this request is
	// bound to — recorded on the identity both as documentation-of-binding and
	// because wake eligibility (isWakeable) keys on ResourceID+ServedModel:
	// "resolution succeeded" is exactly what makes a gateway route wakeable.
	id.ServedModel = model
	id.Upstream = s.gateway.upstreamFor(res.GraphK8sName)
	return true
}
