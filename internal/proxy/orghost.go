package proxy

// ORG HOSTS (issue #71, tenant-domains design sections 3, 4.4, 5 and 7).
//
// <label>-inference.<domain> serves one org's dedicated endpoints and their
// videos. The saturn-k8s tf-org-hosts route strips every X-Saturn-* header at
// the edge, runs Atlas's /api/auth/org-host/check ForwardAuth (which pins the
// host's org in X-Saturn-Org-Id, the key in X-Saturn-Auth-Id and, for a key
// narrowed to one endpoint, X-Saturn-Endpoint-Scope), then sets
// X-Saturn-Org-Host: true and blanks every per-route header. phoebe never
// reads the Host header: the mark is the only way a request takes this path.
//
// On this path phoebe resolves the request-body model= against the org's
// DEDICATED registry rows only (gateway.DedicatedResolver), takes every
// routing and billing field from the row, and then runs the existing
// dedicated path unchanged (model binding, metering, wake, videos). A row
// that lives on another cluster is forwarded to that cluster's org host
// (orghost_forward.go). Every refusal is OpenAI-shaped JSON (orghost_errors.go)
// and none echoes the requested model.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
)

// orgHostRoute is the org-host wiring. nil on a Server means org hosts are
// not configured here: every org-host request is refused with 503.
type orgHostRoute struct {
	resolver gateway.DedicatedResolver
	// shared is the shared-tier resolver, consulted only to tell "a shared
	// model of your org" (wrong_endpoint_host) from "no such model".
	shared gateway.Resolver
	// sharedBaseURL is named in the org host's wrong_endpoint_host message
	// (config gateway.sharedBaseURL). Empty: the message names no URL.
	sharedBaseURL string
	forward       *orgHostForwarder
}

// WithOrgHosts enables the org-host path. resolver must implement
// gateway.DedicatedResolver (the registry resolver does); otherwise org hosts
// stay off and org-host requests are refused (fail closed).
func (s *Server) WithOrgHosts(resolver gateway.Resolver, sharedBaseURL string) *Server {
	dedicated, ok := resolver.(gateway.DedicatedResolver)
	if !ok || resolver == nil {
		return s
	}
	s.orgHosts = &orgHostRoute{
		resolver:      dedicated,
		shared:        resolver,
		sharedBaseURL: strings.TrimRight(sharedBaseURL, "/"),
		forward:       newOrgHostForwarder(),
	}
	return s
}

// endpointScopeRE is the grammar of an endpoint id in X-Saturn-Endpoint-Scope:
// Atlas deployment ids (32 hex). Letters, digits, '-' and '_' up to 64 bytes
// are accepted so the check stays a grammar check, not an Atlas id format
// dependency; anything else (empty, a list, spaces) refuses.
var endpointScopeRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// orgHostConflictingMarkers reports whether an org-host request also carries
// another route's marker or a per-route stamp. The org-host route blanks all
// of them, so their presence means the edge contract is broken.
func orgHostConflictingMarkers(id identity.Identity) bool {
	return id.Gateway || id.Upstream != "" || id.ResourceID != "" ||
		id.ServedModel != "" || id.ServingMode != "" || id.ServingProfile != "" ||
		id.VideoLimits != "" || id.BaseModel != "" || id.Adapter != "" || id.ResourceType != ""
}

// orgHostForwardedHeader marks a request one entry cluster forwarded to the
// serving cluster's org host. It is not an X-Saturn-* header, so the edge
// strip leaves it in place for the serving phoebe to see. A request that
// carries it is never forwarded again (508): only a cluster's own rows can
// serve it. A client that sets it only stops its own request from being
// forwarded.
const orgHostForwardedHeader = "Saturn-Org-Host-Forwarded"

// serveOrgHost handles a request marked X-Saturn-Org-Host. It returns true
// when it filled id from a local dedicated row and handleProxy must continue
// down the dedicated path; false when it has already answered (a refusal, a
// local answer such as /v1/models, a video request or a forward).
func (s *Server) serveOrgHost(w http.ResponseWriter, r *http.Request, id *identity.Identity, routePath string, pathCanonical bool) bool {
	forwarded := r.Header.Get(orgHostForwardedHeader) != ""
	r.Header.Del(orgHostForwardedHeader)
	switch {
	case orgHostConflictingMarkers(*id):
		s.log.Error.Printf("org-host: refusing request that also carries another route's marker or stamps (edge contract broken) org_id=%q", id.OrgID)
		writeClientError(w, "", http.StatusForbidden, orgHostConflictingRoute)
		return false
	case s.orgHosts == nil:
		s.log.Error.Printf("org-host: refusing org-host request: org hosts are not configured on this interceptor (gateway.enabled with the registry resolver)")
		writeClientError(w, "", http.StatusServiceUnavailable, orgHostUnavailable)
		return false
	case !identity.EndpointScopeTrusted():
		s.log.Error.Printf("org-host: refusing org-host request: %s is not in the trusted header set, so a key narrowed to one endpoint cannot be told from an org key", identity.HeaderEndpointScope)
		writeClientError(w, "", http.StatusServiceUnavailable, orgHostUnavailable)
		return false
	case id.OrgID == "":
		s.log.Warn.Printf("org-host: refusing org-host request with no %s (edge contract broken)", identity.HeaderOrgID)
		writeClientError(w, "", http.StatusForbidden, orgHostWrongOrganization)
		return false
	case id.EndpointScopePresent && !endpointScopeRE.MatchString(id.EndpointScope):
		s.log.Warn.Printf("org-host: refusing malformed %s org_id=%s", identity.HeaderEndpointScope, id.OrgID)
		writeClientError(w, "", http.StatusForbidden, bindingModelNotAuthorized)
		return false
	case !pathCanonical:
		writeClientError(w, "", http.StatusNotFound, orgHostUnknownPath)
		return false
	}

	if strings.HasPrefix(routePath, "/v1/videos") && (routePath == "/v1/videos" || strings.HasPrefix(routePath, "/v1/videos/")) {
		s.serveOrgHostVideos(w, r, *id, routePath, forwarded)
		return false
	}
	if routePath == "/v1/models" {
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return false
		}
		if r.Method != http.MethodGet {
			writeClientError(w, "", http.StatusNotFound, orgHostUnknownPath)
			return false
		}
		s.serveOrgHostModels(w, r, *id)
		return false
	}
	if !inferenceRequestPathAllowed(routePath) {
		writeClientError(w, "", http.StatusNotFound, orgHostUnknownPath)
		return false
	}
	switch r.Method {
	case http.MethodOptions:
		// Answered locally, as on the gateway: no Access-Control-* headers.
		w.WriteHeader(http.StatusNoContent)
		return false
	case http.MethodPost:
	default:
		writeClientError(w, "", http.StatusNotFound, orgHostUnknownPath)
		return false
	}

	body, err := readAndRestoreBody(r)
	if err != nil {
		s.logRequestBodyError("org-host: read request body", "", err)
		writeRequestBodyError(w, r, "", err)
		return false
	}
	model, ok := extractRequestModel(body)
	if !ok {
		if len(body) > 0 && !validJSON(body) {
			writeClientError(w, "", http.StatusBadRequest, bindingInvalidJSON)
			return false
		}
		writeClientError(w, "", http.StatusBadRequest, modelFieldRequired)
		return false
	}
	ep, ok := s.resolveOrgHostModel(w, r, *id, model)
	if !ok {
		return false
	}
	if ep.VideoLimits != "" {
		// A video endpoint serves the video API only (its route did the same).
		writeClientError(w, "", http.StatusNotFound, orgHostVideoEndpointOnly)
		return false
	}
	if ep.Forwards() {
		// The forward needs the key: the serving cluster's edge checks it again.
		s.forwardOrgHost(w, r, *id, ep, forwarded)
		return false
	}
	if !s.applyDedicatedEndpoint(w, id, ep, model) {
		return false
	}
	stripCallerCredentials(r)
	return true
}

// stripCallerCredentials removes the caller's Saturn credential before an
// org-host request is proxied to the local engine. On the per-endpoint routes
// atlas-auth removed it (issue #26: a Saturn token never reaches the app);
// on the org host the ForwardAuth is Atlas's check, which cannot remove
// request headers, so phoebe does it here. The key may be an org API key or
// the playground relay token, which carries the session's audiences. It runs
// only after the forward decision, because a forward needs the key. The
// X-Saturn-* strip at handler entry already removed X-Saturn-Authorization;
// it is deleted again here so this function alone states the rule.
func stripCallerCredentials(r *http.Request) {
	r.Header.Del("Authorization")
	r.Header.Del("Cookie")
	r.Header.Del("X-Saturn-Authorization")
}

// resolveOrgHostModel resolves the body model= of an org-host request to one
// of the org's dedicated endpoints, applying design 3.2 and 4.4 in order. It
// writes the refusal and returns false when the request must not proceed:
//
//	503 registry not synced or lookup failure;
//	404 model_not_found     — no dedicated endpoint of THIS org has that name
//	                          (a typo and another org's endpoint are the same
//	                          lookup miss, so the same bytes);
//	404 wrong_endpoint_host — the name is a shared model of this org;
//	403 model_not_authorized — a key narrowed to another endpoint;
//	409 endpoint_not_running — the endpoint is stopped.
//
// The scope check runs before the state check, so a narrowed key learns
// nothing about other endpoints' state. The video sibling (POST /v1/videos)
// uses it unchanged.
func (s *Server) resolveOrgHostModel(w http.ResponseWriter, r *http.Request, id identity.Identity, model string) (gateway.DedicatedEndpoint, bool) {
	ep, err := s.orgHosts.resolver.ResolveDedicated(r.Context(), id.OrgID, model)
	switch {
	case errors.Is(err, gateway.ErrNotFound):
		if s.orgHostSharedModelExists(r.Context(), id.OrgID, model) {
			s.log.Info.Printf("org-host: shared model sent to the org host org_id=%s model=%q", id.OrgID, model)
			writeClientError(w, "", http.StatusNotFound, orgHostWrongEndpointHost(s.orgHosts.sharedBaseURL))
			return gateway.DedicatedEndpoint{}, false
		}
		s.log.Warn.Printf("org-host: no dedicated endpoint for org_id=%s model=%q", id.OrgID, model)
		writeClientError(w, "", http.StatusNotFound, orgHostModelNotFound)
		return gateway.DedicatedEndpoint{}, false
	case err != nil:
		s.log.Error.Printf("org-host: resolve failed org_id=%s: %v", id.OrgID, err)
		writeClientError(w, "", http.StatusServiceUnavailable, orgHostUnavailable)
		return gateway.DedicatedEndpoint{}, false
	}
	if ep.OrgID != id.OrgID {
		// Impossible by the lookup key; a resolver that broke it must not serve.
		s.log.Error.Printf("org-host: resolver returned org %s for org %s", ep.OrgID, id.OrgID)
		writeClientError(w, "", http.StatusServiceUnavailable, orgHostUnavailable)
		return gateway.DedicatedEndpoint{}, false
	}
	if id.EndpointScopePresent && ep.ResourceID != id.EndpointScope {
		s.log.Warn.Printf("org-host: endpoint-scoped key for %s asked for resource %s org_id=%s", id.EndpointScope, ep.ResourceID, id.OrgID)
		writeClientError(w, "", http.StatusForbidden, bindingModelNotAuthorized)
		return gateway.DedicatedEndpoint{}, false
	}
	if ep.State == gateway.EndpointStateStopped {
		writeClientError(w, "", http.StatusConflict, orgHostEndpointNotRunning)
		return gateway.DedicatedEndpoint{}, false
	}
	return ep, true
}

// orgHostSharedModelExists reports whether model is a shared model available
// to org. Any lookup failure reads as "no" (the caller then answers
// model_not_found, which reveals nothing).
func (s *Server) orgHostSharedModelExists(ctx context.Context, orgID, model string) bool {
	if s.orgHosts.shared == nil {
		return false
	}
	res, err := s.orgHosts.shared.Resolve(ctx, orgID, model)
	return err == nil && res.ServingMode == identity.ServingModeShared
}

// applyDedicatedEndpoint fills the routing and billing fields of id from a
// local dedicated row, so the rest of handleProxy runs the dedicated path
// exactly as for a per-endpoint route. Every field comes from the row; none
// from the request. Returns false (after answering 502) when the row has no
// upstream, which only a forwarding copy may lack.
func (s *Server) applyDedicatedEndpoint(w http.ResponseWriter, id *identity.Identity, ep gateway.DedicatedEndpoint, model string) bool {
	if ep.Upstream == "" {
		s.log.Error.Printf("org-host: dedicated row for resource %s has no upstream", ep.ResourceID)
		writeClientError(w, "", http.StatusBadGateway, orgHostUnavailable)
		return false
	}
	id.ResourceID = ep.ResourceID
	id.BaseModel = ep.BaseModel
	id.Adapter = ep.Adapter
	id.ServingMode = identity.ServingModeDedicated
	id.ServingProfile = ep.ServingProfile
	id.ServedModel = model
	id.Upstream = ep.Upstream
	id.VideoLimits = ep.VideoLimits
	return true
}

// orgHostModel is one entry of the org host's GET /v1/models.
type orgHostModel struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// serveOrgHostModels answers GET /v1/models on the org host from the
// registry: the org's deployed dedicated endpoints (running or scaled to
// zero; stopped ones are left out), one entry per endpoint, id = endpoint
// name. A key narrowed to one endpoint sees only that endpoint. Nothing is
// forwarded, so no engine's graph-wide list can leak.
func (s *Server) serveOrgHostModels(w http.ResponseWriter, r *http.Request, id identity.Identity) {
	rows, err := s.orgHosts.resolver.ListDedicated(r.Context(), id.OrgID)
	if err != nil {
		s.log.Error.Printf("org-host: list failed org_id=%s: %v", id.OrgID, err)
		writeClientError(w, "", http.StatusServiceUnavailable, orgHostUnavailable)
		return
	}
	data := make([]orgHostModel, 0, len(rows))
	for _, ep := range rows {
		if ep.State != gateway.EndpointStateRunning {
			continue
		}
		if id.EndpointScopePresent && ep.ResourceID != id.EndpointScope {
			continue
		}
		data = append(data, orgHostModel{ID: ep.Name, Object: "model", OwnedBy: "organization"})
	}
	body, _ := json.Marshal(map[string]any{"object": "list", "data": data})
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
