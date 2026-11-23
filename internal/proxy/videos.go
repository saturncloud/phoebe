package proxy

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/videos"
)

// videosRoute hands a video endpoint's /v1/videos requests to the
// phoebe-videos service, which owns the jobs (video models design 5.1). The
// interceptor keeps what it is for: it checks the trusted identity of the
// request at its gates, then passes that identity on; it holds no job state.
type videosRoute struct {
	proxy *httputil.ReverseProxy
	token string
}

// WithVideos enables the video route. serviceURL is the phoebe-videos
// Service (http://host:port); token is the shared PHOEBE_VIDEOS_TOKEN. With
// either missing, video routes answer 404 (video generation off here).
func (s *Server) WithVideos(serviceURL, token string) (*Server, error) {
	if serviceURL == "" || token == "" {
		return s, nil
	}
	u, err := url.Parse(serviceURL)
	if err != nil || u.Host == "" || u.Scheme != "http" || (u.Path != "" && u.Path != "/") {
		return s, fmt.Errorf("videos.serviceUrl %q must be http://host:port", serviceURL)
	}
	s.videos = &videosRoute{proxy: newUpstreamProxy(u), token: token}
	return s, nil
}

// videoRequestAllowed is a video endpoint's whole public surface (design 3.3).
func videoRequestAllowed(method, path string) bool {
	if path == "/v1/videos" {
		return method == http.MethodPost || method == http.MethodGet
	}
	rest, ok := strings.CutPrefix(path, "/v1/videos/")
	if !ok || rest == "" {
		return false
	}
	id, sub, hasSub := strings.Cut(rest, "/")
	if id == "" || strings.Contains(sub, "/") {
		return false
	}
	if !hasSub {
		return method == http.MethodGet || method == http.MethodDelete
	}
	return sub == "content" && method == http.MethodGet
}

// serveVideo answers a request on a route that carries X-Saturn-Video-Limits.
// Every refusal is the same generic 404 the other route gates use.
func (s *Server) serveVideo(w http.ResponseWriter, r *http.Request, id identity.Identity, requestID, routePath string, pathCanonical bool) {
	refuse := func(reason string) {
		s.log.Warn.Printf("videos: refused request_id=%s resource_id=%s method=%s path=%q: %s",
			requestID, id.ResourceID, r.Method, r.URL.EscapedPath(), reason)
		http.Error(w, "not found", http.StatusNotFound)
	}
	switch {
	case s.videos == nil:
		refuse("video generation is not enabled on this phoebe (videos.serviceUrl / PHOEBE_VIDEOS_TOKEN)")
		return
	case id.ServingMode != identity.ServingModeDedicated:
		// v1 is dedicated only (design 4.4); a video route on anything else is
		// an Atlas producer bug.
		refuse(fmt.Sprintf("serving mode %q on a video route", id.ServingMode))
		return
	case id.ServedModel == "" || id.OrgID == "" || id.BaseModel == "":
		refuse("video route without served model, org or base model")
		return
	case !pathCanonical || !videoRequestAllowed(r.Method, routePath):
		refuse("outside the video surface")
		return
	}
	s.handOffVideo(w, r, requestID, videos.Handoff{
		OrgID: id.OrgID, AuthID: id.AuthID, UserID: id.UserID, GroupID: id.GroupID,
		ResourceID: id.ResourceID, ServedModel: id.ServedModel, BaseModel: id.BaseModel,
		ServingMode: id.ServingMode, Upstream: id.Upstream, Limits: id.VideoLimits,
	})
}

// handOffVideo passes a checked request to phoebe-videos with the trusted
// identity and the shared token, replacing any client copy of either.
func (s *Server) handOffVideo(w http.ResponseWriter, r *http.Request, requestID string, h videos.Handoff) {
	handoff, err := videos.EncodeHandoff(h)
	if err != nil {
		s.log.Error.Printf("videos: encode identity: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	// A client cannot set the internal headers: they are replaced here.
	for name := range r.Header {
		if strings.HasPrefix(http.CanonicalHeaderKey(name), "X-Phoebe-") {
			r.Header.Del(name)
		}
	}
	// phoebe-videos authenticates by the shared token; the caller's
	// credentials never go past the interceptor.
	r.Header.Del("Authorization")
	r.Header.Del("Cookie")
	r.Header.Set(videos.HeaderHandoff, handoff)
	r.Header.Set(videos.HeaderToken, s.videos.token)
	// Replace, never preserve, the client correlation id, as the endpoint
	// path does before it reaches here.
	r.Header.Set(requestIDHeader, requestID)
	w.Header().Set(requestIDHeader, requestID)
	// Every caller has checked the path against its surface, so a GET
	// ending in /content is a download of a stored file: the one request
	// exempt from protection.maxStreamDuration (stall guards still apply).
	if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/content") {
		exemptFromMaxStreamDuration(r.Context())
	}
	s.videos.proxy.ServeHTTP(w, r)
}
