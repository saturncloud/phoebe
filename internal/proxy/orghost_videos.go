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

// orgHostVideoEndpoint is the dedicated endpoint a create's model resolved
// to: the registry row's values, never the request's.
type orgHostVideoEndpoint struct {
	ResourceID string
	// ServedModels is every name the endpoint answers to (its name, then any
	// legacy alias); the create's model must be one of them.
	ServedModels []string
	BaseModel    string
	// Upstream is the engine (host:port). Empty on a row that forwards to
	// another cluster: the caller forwards those before calling the hook.
	Upstream    string
	VideoLimits string
}

// orgHostVideoResolvers is what the org-host path gives the video hook.
type orgHostVideoResolvers struct {
	// Create resolves a create's body model to a dedicated endpoint of the
	// host's org. When ok is false it has already written the refusal
	// (model_not_found, wrong_endpoint_host, endpoint_not_running,
	// model_not_authorized), exactly as for an inference request.
	Create func(w http.ResponseWriter, model string) (ep orgHostVideoEndpoint, ok bool)
	// EndpointID maps a list's ?model= name to the endpoint id of the
	// host's org that has it, false when no dedicated endpoint of the org
	// has that name. Stopped and scaled-to-zero endpoints count.
	EndpointID func(name string) (resourceID string, ok bool)
}

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
	orgHostVideoModelNotAuthorized = clientError{
		Message: "The requested model is not served by this endpoint. Set model to this endpoint's model name.",
		Type:    "permission_error", Code: "model_not_authorized",
	}
)

// serveOrgHostVideo is the org-host /v1/videos hook. The org-host path calls
// it for every request whose path starts with /v1/videos, after its own
// checks: the org-host mark is trusted and alone (no gateway, retrieval or
// per-route stamps), id.OrgID is the host's org, and scope is the validated
// X-Saturn-Endpoint-Scope value ("" for an org key; a present but empty or
// malformed header was refused before this point). A create whose row
// forwards to another cluster is forwarded by the caller, not handed here.
func (s *Server) serveOrgHostVideo(w http.ResponseWriter, r *http.Request, id identity.Identity, scope, routePath string, pathCanonical bool, res orgHostVideoResolvers) {
	requestID, err := generateRequestID()
	if err != nil {
		s.log.Error.Printf("videos: generate request id: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	refuse := func(reason string) {
		s.log.Warn.Printf("videos: refused org-host request request_id=%s org_id=%q scope=%q method=%s path=%q: %s",
			requestID, id.OrgID, scope, r.Method, r.URL.EscapedPath(), reason)
		writeClientError(w, requestID, http.StatusNotFound, orgHostVideoNotFound)
	}
	switch {
	case s.videos == nil:
		refuse("video generation is not enabled on this phoebe (videos.serviceUrl / PHOEBE_VIDEOS_TOKEN)")
		return
	case id.Gateway || id.VideoLimits != "" || id.Upstream != "" || id.ServingMode != "" || id.ServedModel != "":
		// The org-host route blanks every per-route header; a stamp next to
		// the mark means the edge contract is broken.
		refuse("org-host video request with another route's stamps")
		return
	case id.OrgID == "" || id.AuthID == "":
		refuse("org-host video request without org or auth id")
		return
	case !pathCanonical || !videoRequestAllowed(r.Method, routePath):
		refuse("outside the video surface")
		return
	}
	h := videos.Handoff{
		OrgHost: true, OrgID: id.OrgID, AuthID: id.AuthID, UserID: id.UserID, GroupID: id.GroupID,
		ResourceID: scope,
	}
	switch {
	case routePath == "/v1/videos" && r.Method == http.MethodPost:
		s.orgHostVideoCreate(w, r, id, scope, requestID, res)
		return
	case routePath == "/v1/videos" && r.Method == http.MethodGet:
		models := r.URL.Query()["model"]
		if len(models) > 1 {
			writeClientError(w, requestID, http.StatusBadRequest, orgHostVideoModelTwice)
			return
		}
		if len(models) == 1 {
			// The filter reveals nothing: a name no endpoint of the org has,
			// and another endpoint's name for an endpoint-scoped key, both
			// list nothing, exactly like a real endpoint with no videos.
			resourceID, ok := "", false
			if res.EndpointID != nil {
				resourceID, ok = res.EndpointID(models[0])
			}
			switch {
			case !ok || resourceID == "":
				h.ResourceID, h.NoMatch = "", true
			case scope != "" && resourceID != scope:
				h.ResourceID, h.NoMatch = "", true
			default:
				h.ResourceID = resourceID
			}
		}
	}
	// Reads, downloads, deletes and lists: the org's videos, restricted to
	// the scoped endpoint for an endpoint-scoped key. No endpoint is bound,
	// so the request's metrics carry the org only.
	s.handOffVideo(w, r, requestID, h)
}

// orgHostVideoCreate hands a create to phoebe-videos as the endpoint its
// body model resolved to, with that endpoint's row values.
func (s *Server) orgHostVideoCreate(w http.ResponseWriter, r *http.Request, id identity.Identity, scope, requestID string, res orgHostVideoResolvers) {
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
	if res.Create == nil {
		s.log.Error.Printf("videos: org-host create request_id=%s: no endpoint resolver wired", requestID)
		writeClientError(w, requestID, http.StatusServiceUnavailable, clientError{
			Message: "Video generation is unavailable on this host right now.", Type: "server_error", Code: "service_unavailable",
		})
		return
	}
	ep, ok := res.Create(w, model)
	if !ok {
		return
	}
	switch {
	case scope != "" && ep.ResourceID != scope:
		// The resolver applies the scope too; this is the backstop.
		writeClientError(w, requestID, http.StatusForbidden, orgHostVideoModelNotAuthorized)
		return
	case ep.ResourceID == "" || ep.BaseModel == "" || ep.Upstream == "" || ep.VideoLimits == "" || len(ep.ServedModels) == 0:
		// A chat endpoint (no video limits), a forwarding copy (no
		// upstream) or an incomplete row: nothing here can run the job.
		s.log.Warn.Printf("videos: org-host create request_id=%s org_id=%q resource_id=%q: endpoint row cannot serve videos here",
			requestID, id.OrgID, ep.ResourceID)
		writeClientError(w, requestID, http.StatusNotFound, orgHostVideoNotFound)
		return
	}
	bound := id
	bound.ResourceID, bound.BaseModel, bound.ServingMode = ep.ResourceID, ep.BaseModel, identity.ServingModeDedicated
	bound.Upstream, bound.VideoLimits = ep.Upstream, ep.VideoLimits
	bound.ServedModel = strings.Join(ep.ServedModels, ",")
	recordFrom(r.Context()).bind(bound)
	s.handOffVideo(w, r, requestID, videos.Handoff{
		OrgID: id.OrgID, AuthID: id.AuthID, UserID: id.UserID, GroupID: id.GroupID,
		ResourceID: ep.ResourceID, ServedModel: bound.ServedModel, BaseModel: ep.BaseModel,
		ServingMode: identity.ServingModeDedicated, Upstream: ep.Upstream, Limits: ep.VideoLimits,
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
			if part.FormName() != "model" {
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
