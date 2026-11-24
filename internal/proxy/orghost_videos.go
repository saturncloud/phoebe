package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/videos"
)

// VIDEOS ON THE ORG HOST (issue #71, tenant-domains design section 6). All
// five video operations of an org's dedicated endpoints are served on its org
// host, <label>-inference.<domain>:
//
//	POST   /v1/videos               body model= picks the endpoint
//	GET    /v1/videos               org-wide list; ?model= filters to one endpoint
//	GET    /v1/videos/{id}          looked up by id under the host's org
//	GET    /v1/videos/{id}/content  same; the bytes stream from phoebe-videos
//	DELETE /v1/videos/{id}          same
//
// The org comes only from the edge (Atlas's org-host ForwardAuth stamps
// X-Saturn-Org-Id), and an endpoint-scoped key's endpoint only from
// X-Saturn-Endpoint-Scope; the endpoint a create runs on comes only from the
// registry row its model resolved to. Nothing here reads a per-route header.
// Reads, downloads, deletes and lists need no endpoint state, so they work
// for an endpoint that is stopped or deleted.
//
// Other clusters: a create whose endpoint runs on another cluster is
// forwarded to that cluster's org host (forwardOrgHost), which checks the key
// again and runs the job there. Reads, downloads and deletes of one video are
// forwarded the same way when this cluster does not hold it, as design
// section 6 says (orghost_video_forward.go). Only the org-wide list answers
// from this cluster's phoebe-videos alone: design section 6 defers merging
// per-cluster lists (video generation runs on one cluster today).

var (
	orgHostVideoNotFound   = clientError{Message: "Not found.", Type: "invalid_request_error", Code: "not_found"}
	orgHostVideoModelTwice = clientError{
		Message: "The model query parameter may be given at most once.",
		Type:    "invalid_request_error", Code: "invalid_request",
	}
	orgHostVideoBadBody = clientError{
		Message: "The request body could not be read as JSON or multipart/form-data.",
		Type:    "invalid_request_error", Code: "invalid_request",
	}
	orgHostVideoNoModel = clientError{
		Message: "model is required: set it to the name of one of your organization's video endpoints.",
		Type:    "invalid_request_error", Code: "invalid_request",
	}
	orgHostNotVideoEndpoint = clientError{
		Message: "This endpoint does not generate videos. Set model to one of your organization's video endpoints.",
		Type:    "invalid_request_error", Code: "not_a_video_endpoint",
	}
)

// serveOrgHostVideos is the org host's /v1/videos surface. serveOrgHost
// calls it for every path under /v1/videos after its own checks: the org-host
// mark is trusted and alone (no other route's marker or stamps), the
// interceptor is configured for org hosts, id.OrgID is the host's org, a
// present X-Saturn-Endpoint-Scope is one well-formed endpoint id, and the
// path is canonical. alreadyForwarded reports that another cluster
// forwarded this request here; such a request is never forwarded again.
func (s *Server) serveOrgHostVideos(w http.ResponseWriter, r *http.Request, id identity.Identity, routePath string, alreadyForwarded bool) {
	requestID, err := generateRequestID()
	if err != nil {
		s.log.Error.Printf("videos: generate request id: %v", err)
		writeClientError(w, "", http.StatusInternalServerError, clientError{Message: "Internal error.", Type: "server_error", Code: "internal_error"})
		return
	}
	scope := ""
	if id.EndpointScopePresent {
		scope = id.EndpointScope
	}
	refuse := func(reason string) {
		s.log.Warn.Printf("videos: refused org-host request request_id=%s org_id=%q scope=%q method=%s path=%q: %s",
			requestID, id.OrgID, scope, r.Method, r.URL.EscapedPath(), reason)
		writeClientError(w, requestID, http.StatusNotFound, orgHostVideoNotFound)
	}
	switch {
	case !id.OrgHost || orgHostConflictingMarkers(id):
		// serveOrgHost refuses these already; this is the backstop for the
		// video path, which must never take a per-route stamp.
		refuse("org-host video request without the mark or with another route's stamps")
		return
	case s.orgHosts == nil:
		refuse("org hosts are not configured on this interceptor")
		return
	case id.OrgID == "" || id.AuthID == "":
		refuse("org-host video request without org or auth id")
		return
	case id.EndpointScopePresent && scope == "":
		refuse("empty endpoint scope")
		return
	case !videoRequestAllowed(r.Method, routePath):
		refuse("outside the video surface")
		return
	}
	if routePath == "/v1/videos" && r.Method == http.MethodPost {
		s.orgHostVideoCreate(w, r, id, requestID, alreadyForwarded)
		return
	}
	h := videos.Handoff{
		OrgHost: true, OrgID: id.OrgID, AuthID: id.AuthID, UserID: id.UserID, GroupID: id.GroupID,
		ResourceID: scope,
	}
	if routePath != "/v1/videos" {
		// Read, download or delete one video: this cluster's store first,
		// then the other clusters that serve the org's video endpoints
		// (orghost_video_forward.go). With video generation off here only
		// the other clusters are asked.
		var local func(http.ResponseWriter, *http.Request)
		if s.videos != nil {
			local = func(w http.ResponseWriter, r *http.Request) { s.handOffVideo(w, r, requestID, h) }
		}
		s.serveOrgHostVideoByID(w, r, id, requestID, scope, local, alreadyForwarded)
		return
	}
	// Video generation off here: checked after the create branch, because a
	// create for an endpoint on another cluster is forwarded whatever this
	// cluster runs.
	if s.videos == nil {
		refuse("video generation is not enabled on this phoebe (videos.serviceUrl / PHOEBE_VIDEOS_TOKEN)")
		return
	}
	{
		models := r.URL.Query()["model"]
		if len(models) > 1 {
			writeClientError(w, requestID, http.StatusBadRequest, orgHostVideoModelTwice)
			return
		}
		if len(models) == 1 {
			// The filter reveals nothing: a name no dedicated endpoint of the
			// org has, and another endpoint's name for an endpoint-scoped
			// key, both list nothing, exactly like an endpoint with no
			// videos.
			ep, err := s.orgHosts.resolver.ResolveDedicated(r.Context(), id.OrgID, models[0])
			switch {
			case errors.Is(err, gateway.ErrNotFound) || (err == nil && (ep.OrgID != id.OrgID || ep.ResourceID == "")):
				h.ResourceID, h.NoMatch = "", true
			case err != nil:
				s.log.Error.Printf("videos: org-host list filter request_id=%s org_id=%s: %v", requestID, id.OrgID, err)
				writeClientError(w, requestID, http.StatusServiceUnavailable, orgHostUnavailable)
				return
			case scope != "" && ep.ResourceID != scope:
				h.ResourceID, h.NoMatch = "", true
			default:
				h.ResourceID = ep.ResourceID
			}
		}
	}
	// The list: the org's videos on this cluster, restricted to the scoped
	// endpoint for an endpoint-scoped key. No endpoint is bound, so the
	// request's metrics carry no endpoint label.
	s.handOffVideo(w, r, requestID, h)
}

// orgHostVideoCreate resolves a create's body model to one of the org's
// dedicated endpoints (the same 404, 403 and 409 rules as inference) and
// either forwards it to the cluster that serves the endpoint or hands it to
// phoebe-videos with that endpoint's row values.
func (s *Server) orgHostVideoCreate(w http.ResponseWriter, r *http.Request, id identity.Identity, requestID string, alreadyForwarded bool) {
	model, err := videoCreateModel(r)
	if err != nil {
		s.log.Warn.Printf("videos: org-host create request_id=%s org_id=%q: %v", requestID, id.OrgID, err)
		writeClientError(w, requestID, http.StatusBadRequest, orgHostVideoBadBody)
		return
	}
	if model == "" {
		writeClientError(w, requestID, http.StatusBadRequest, orgHostVideoNoModel)
		return
	}
	ep, ok := s.resolveOrgHostModel(w, r, id, model)
	if !ok {
		return
	}
	if ep.VideoLimits == "" {
		// A chat endpoint: the inference path's mirror image of
		// orgHostVideoEndpointOnly.
		writeClientError(w, requestID, http.StatusNotFound, orgHostNotVideoEndpoint)
		return
	}
	if ep.Forwards() {
		s.forwardOrgHost(w, r, id, ep, alreadyForwarded)
		return
	}
	if s.videos == nil {
		s.log.Warn.Printf("videos: org-host create request_id=%s resource_id=%s: video generation is not enabled on this phoebe", requestID, ep.ResourceID)
		writeClientError(w, requestID, http.StatusNotFound, orgHostVideoNotFound)
		return
	}
	bound := id
	if !s.applyDedicatedEndpoint(w, &bound, ep, model) {
		return
	}
	// phoebe-videos checks the create's model against the endpoint's
	// served names, as on the endpoint's own route.
	served := strings.Join(append([]string{ep.Name}, ep.Aliases...), ",")
	recordFrom(r.Context()).bind(bound)
	s.handOffVideo(w, r, requestID, videos.Handoff{
		OrgID: id.OrgID, AuthID: id.AuthID, UserID: id.UserID, GroupID: id.GroupID,
		ResourceID: bound.ResourceID, ServedModel: served, BaseModel: bound.BaseModel,
		ServingMode: identity.ServingModeDedicated, Upstream: bound.Upstream, Limits: bound.VideoLimits,
	})
}

// videoCreateModel reads the model field of a create body, JSON or
// multipart/form-data (the OpenAI SDK sends multipart when it uploads
// input_reference as a file), and puts the body back for phoebe-videos,
// which parses and validates the whole request itself. The body is already
// bounded by the gateway-wide request ceiling.
func videoCreateModel(r *http.Request) (string, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return "", nil
	}
	body, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		return "", err
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	mediaType, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	switch mediaType {
	case "multipart/form-data":
		mr := multipart.NewReader(bytes.NewReader(body), params["boundary"])
		model, found := "", false
		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				return model, nil
			}
			if err != nil {
				return "", err
			}
			if name := part.FormName(); name != "model" {
				// A case variant of model ("Model") is refused rather
				// than skipped, so phoebe and phoebe-videos can never
				// read two different values (battery round 2, #71).
				if strings.EqualFold(name, "model") {
					return "", errors.New("model given in another letter case")
				}
				continue
			}
			if found {
				return "", errors.New("model given twice")
			}
			b, err := io.ReadAll(io.LimitReader(part, 4096))
			if err != nil {
				return "", err
			}
			model, found = string(b), true
		}
	case "application/json", "":
		// A duplicate model key, or one in another letter case, is
		// refused, as on every other body phoebe reads model from.
		if _, err := checkTopLevelKeyAmbiguity(body, []string{"model"}); err != nil {
			return "", err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(body, &fields); err != nil {
			return "", err
		}
		raw, ok := fields["model"]
		if !ok {
			return "", nil
		}
		var model string
		if err := json.Unmarshal(raw, &model); err != nil {
			return "", errors.New("model is not a string")
		}
		return model, nil
	default:
		return "", errors.New("unsupported content type")
	}
}
