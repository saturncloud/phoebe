package proxy

import (
	"net/http"

	"github.com/saturncloud/phoebe/internal/identity"
)

// serveOrgHostVideos is the org host's /v1/videos surface (create, org-wide
// list, retrieve, content, delete; tenant-domains design section 6). OWNED BY
// the td71-phoebe-videos work (branch td71/phoebe-videos), which replaces
// this body. Contract for the replacement:
//
//   - id is the trusted identity: OrgID is the host's org, AuthID the key,
//     EndpointScopePresent/EndpointScope a key narrowed to one endpoint.
//   - POST /v1/videos picks the endpoint with
//     s.resolveOrgHostModel(w, r, id, model) (writes its own refusal and
//     returns false), then requires ep.VideoLimits != "", then either
//     s.forwardOrgHost(w, r, id, ep, alreadyForwarded) when ep.Forwards(), or
//     s.applyDedicatedEndpoint(w, &id, ep, model) followed by the existing
//     dedicated video path.
//   - The list's model filter maps a name to its resource id with
//     s.orgHosts.resolver.ResolveDedicated / ListDedicated.
//   - alreadyForwarded reports that another cluster forwarded this request.
//
// Until then every org-host video request is refused as an unknown path, so
// nothing is served without the org checks.
func (s *Server) serveOrgHostVideos(w http.ResponseWriter, _ *http.Request, id identity.Identity, routePath string, alreadyForwarded bool) {
	_ = alreadyForwarded
	s.log.Warn.Printf("org-host: video request on the org host before the videos path is built org_id=%s path=%q", id.OrgID, routePath)
	writeClientError(w, "", http.StatusNotFound, orgHostUnknownPath)
}
