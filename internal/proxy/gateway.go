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
	// :<port> over the configured gateway namespace — the port is the
	// RESOLUTION's port when it carries one (the registry ConfigMaps do;
	// multi-backend graphs may serve on different ports) and the configured
	// gateway.port otherwise. A func field so tests can point resolved
	// requests at a live httptest backend.
	upstreamFor func(graphK8sName string, port int) string
}

// graphCapacitySource is the per-graph admission capacity Atlas publishes in
// the served-model registry (gateway.RegistryResolver implements it; the
// deprecated Postgres resolver does not, so it publishes no limits).
// GraphCapacity is the scaled capacity admission enforces; GraphRequestCapacity
// is the capacity ONE request is checked against to decide that it can never
// fit (Q24): the published values, unscaled by ready workers. Both are
// required, so a resolver that publishes capacity can never leave the 413
// fit check to fall back to the looser scaled value.
type graphCapacitySource interface {
	GraphCapacity(graph string) gateway.GraphCapacity
	GraphRequestCapacity(graph string) gateway.GraphCapacity
}

// graphCapacityPairSource returns GraphCapacity and GraphRequestCapacity from
// one consistent registry read (gateway.RegistryResolver implements it).
type graphCapacityPairSource interface {
	GraphCapacities(graph string) (gateway.GraphCapacity, gateway.GraphCapacity)
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
func (s *Server) WithGateway(resolver gateway.Resolver, namespace string, port int) *Server {
	if resolver == nil || namespace == "" {
		return s
	}
	if port <= 0 {
		port = 8000
	}
	if source, ok := resolver.(graphCapacitySource); ok {
		s.graphCapacity = source
	}
	if source, ok := resolver.(graphCapacityPairSource); ok {
		s.graphCapacityPair = source
	}
	s.gateway = &gatewayRoute{
		resolver: resolver,
		upstreamFor: func(graphK8sName string, resolvedPort int) string {
			p := resolvedPort
			if p <= 0 {
				p = port // resolution carried no port: the configured default
			}
			return fmt.Sprintf("%s-frontend.%s.svc.cluster.local:%d", graphK8sName, namespace, p)
		},
	}
	return s
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
		s.logRequestBodyError("gateway: read request body", requestID, err)
		writeRequestBodyError(w, r, requestID, err)
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
	if res.ServingMode != identity.ServingModeShared {
		// The gateway is exclusively the shared-inference entry point. Treat a
		// non-shared registry row as a broken trusted contract; otherwise the
		// request would skip tenant isolation, scheduler sanitization, and quota
		// admission after resolving onto a shared graph.
		s.log.Error.Printf("gateway: resolved non-shared model org_id=%s model=%q serving_mode=%q request_id=%q",
			id.OrgID, model, res.ServingMode, requestID)
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return false
	}

	id.ResourceID = res.ResourceID
	id.BaseModel = res.BaseModel
	id.Adapter = res.Adapter
	id.ServingMode = res.ServingMode
	// Defense in depth at the seam: Resolver is an exported interface and
	// WithGateway accepts any implementation, so the mode is validated HERE in
	// code, not only asserted below. A resolver returning a value outside the
	// closed enum would otherwise silently disable the shared-policy block and
	// wake on what may be a genuinely shared graph — the same fail-open the
	// header-routed gate refuses. Both shipped resolvers (parseRegistryConfigMap,
	// PGResolver.Resolve) already reject such rows; this makes the proxy fail
	// closed even for a non-conforming in-process impl, and rejects "" too: a
	// resolved row must name its mode explicitly.
	if !identity.ValidServingMode(res.ServingMode) {
		s.log.Error.Printf("gateway: resolver returned invalid serving_mode %q org_id=%s request_id=%q",
			res.ServingMode, id.OrgID, requestID)
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return false
	}
	// The resolved model is definitionally the single model this request is
	// bound to — recorded on the identity both as documentation-of-binding and
	// because wake eligibility (isWakeable) keys on
	// ServingMode+ResourceID+ServedModel. Resolution alone is NOT sufficient:
	// a dedicated row resolves and is deliberately not wakeable (dedicated
	// capacity never scales to zero through this path). The serving-mode
	// check above enforces the closed enum (shipped resolvers reject bad rows
	// themselves — the registry parser and PGResolver.Resolve — the proxy
	// check covers a non-conforming Resolver impl), so the shared case is
	// never silently lost.
	id.ServedModel = model
	// The graph name rides the identity to the wake target verbatim, so a wake
	// on this route actuates exactly the resolved graph — never a re-parse of
	// the upstream host composed from it one line below.
	id.GraphK8sName = res.GraphK8sName
	id.Upstream = s.gateway.upstreamFor(res.GraphK8sName, res.Port)
	if res.Adapter != "" && !res.Ready {
		// Atlas has registered the endpoint but has not observed its
		// DynamoModel ready: after a reconcile, a new adapter, or a load
		// failure. On a parked shared graph the adapter cannot load until a
		// worker runs, and refusing here kept every request away from the
		// wake path, so the endpoint stayed stuck (gateway-admission Q69).
		// A wakeable shared route therefore goes down the cold-wake path like
		// a base-model request: the frontend answers a model it does not list
		// as cold, the graph is woken, and the waker holds the request until
		// the upstream's /v1/models lists this endpoint's served name (the
		// adapter), then forwards it; at the hold deadline the caller gets the
		// usual retryable "starting" 503. Atlas publishes ready=true at its
		// next registry sweep. Atlas publishes no separate "failed" state, so
		// a failed adapter also takes this path, bounded by the wake hold.
		// Every outcome other than a served response (hold expiry, a failed
		// wake, a still-unlisted adapter) answers 503 adapter_not_loaded with
		// Retry-After, as before (wake.go, AdapterNotReady), so callers that
		// script against that body keep seeing it.
		if id.ServingMode == identity.ServingModeShared && s.wakeEnabled(*id) && s.wakerSupports(*id) {
			s.log.Info.Printf("gateway: adapter not ready; holding for a wake org_id=%s model=%q request_id=%q",
				id.OrgID, model, requestID)
			id.AdapterNotReady = true
			return true
		}
		// The graph cannot be woken here (wake disabled, or the waker does not
		// support this target). Keep this distinct from 404: the model exists
		// and the caller may retry. A dedicated row never reaches this point:
		// the non-shared check above refuses it; the ServingMode test in the
		// condition is defense in depth.
		s.log.Warn.Printf("gateway: adapter not loaded org_id=%s model=%q request_id=%q",
			id.OrgID, model, requestID)
		http.Error(w, "adapter_not_loaded", http.StatusServiceUnavailable)
		return false
	}
	return true
}

// wakerSupports reports whether the configured waker can wake the graph id
// resolves to: a waker that declares support per target (SupportsWake) is
// asked; any other waker supports every target.
func (s *Server) wakerSupports(id identity.Identity) bool {
	supporter, ok := s.waker.(wakeTargetSupporter)
	if !ok {
		return true
	}
	return supporter.SupportsWake(WakeTarget{
		UpstreamHost: id.Upstream,
		GraphK8sName: id.GraphK8sName,
		ResourceID:   id.ResourceID,
		ServedModel:  id.ServedModel,
		ServingMode:  id.ServingMode,
	})
}
