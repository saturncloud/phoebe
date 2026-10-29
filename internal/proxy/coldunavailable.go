package proxy

import (
	"bytes"
	"net/http"

	"github.com/saturncloud/phoebe/internal/identity"
)

// A bound dedicated endpoint that cannot be woken (an aggregated graph, or any
// dedicated route when no waker is configured) has no wake path, so before
// this its cold response reached the client unchanged: Dynamo's 404 "Model not
// found" once the model left discovery, or its zero-worker 503. A 404 tells an
// API client the model does not exist, and it is not retryable. On GB300 a
// multi-node aggregated endpoint whose leader restarted answered 404 for the
// whole outage (demo-qa, 2026-10-24).
//
// The request has already passed the model binding (proxy.go), so the model
// it names IS this endpoint's: a cold answer means "not serving right now",
// and phoebe answers its standard retryable 503 instead. No wake is attempted,
// the attempt is marked never-served like a wake's own 503, and every other
// response (including a 404 for something other than a missing model) streams
// through unchanged.
//
// Dynamo v1.6.0 has three cold answers on a dedicated frontend, seen in this
// order when only the engine of a 1-GPU Qwen2.5-0.5B was killed on GB300
// (demo-qa, 2026-10-24, build 29):
//   - while the model is still registered but its worker is gone, a 503
//     "Service temporarily unavailable" (error.rs, ErrorClass::Unavailable);
//   - once the model leaves discovery, the frontend turns its chat and
//     completions routes off, and every request to them gets a 404 with an
//     EMPTY body (service_v2.rs: the endpoint-flag middleware returns a bare
//     StatusCode::NOT_FOUND). This lasted the whole restart, about 95% of the
//     outage;
//   - a 404 "Model not found" (openai.rs model_not_found), or the 503 "not
//     ready to serve requests yet", when the route is on but this model is not
//     servable.
// A frontend that is draining answers 503 "Service is not ready". All of
// these become the retryable 503. Dynamo's overload 503 ("Service temporarily
// overloaded"), its JSON "Route not found" 404 for an unknown path, and any
// other body pass through unchanged.

// dynamoModelNotFoundBodyMarker is the message of Dynamo's frontend 404 for a
// model with no registration (lib/llm/src/http/service/openai.rs
// model_not_found, v1.5.0 and v1.6.0).
const dynamoModelNotFoundBodyMarker = "Model not found"

// dynamoNoBackendBodyMarkers are the messages of Dynamo's frontend 503s that
// mean "no worker can take this request now", as opposed to overload:
// ErrorClass::Unavailable (error.rs) and the frontend's own not-ready answer
// while draining (openai.rs _service_unavailable), v1.6.0.
var dynamoNoBackendBodyMarkers = []string{
	"Service temporarily unavailable",
	"Service is not ready",
}

const coldUnavailableMessage = "the endpoint is not serving right now; retry later"

// coldUnavailableEligible reports whether a request's cold answer is turned
// into a retryable 503: a POST to a bound dedicated (not gateway) route that
// carries an authorized resource and is not going through the wake path.
func (s *Server) coldUnavailableEligible(id identity.Identity, method string) bool {
	dedicated := id.ServingMode == identity.ServingModeDedicated || id.ServingMode == ""
	return method == http.MethodPost && !id.Gateway && dedicated &&
		id.ServedModel != "" && id.ResourceID != "" && !s.wakeEnabled(id)
}

type coldUnavailableRoundTripper struct {
	base      http.RoundTripper
	requestID string
	// neverServed is private per-request state (as on wakeRoundTripper): the
	// response phoebe substituted was never served by the engine.
	neverServed bool
}

func (t *coldUnavailableRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusServiceUnavailable {
		return resp, nil
	}
	// The whole (bounded) body is read and resp.Body replaced with a replay,
	// so a response that is not cold is still returned unchanged.
	body, complete, err := bufferCandidateBody(req.Context(), resp, coldCandidateReadTimeout)
	if err != nil {
		return nil, err
	}
	if !complete || !dedicatedColdAnswer(resp.StatusCode, body) {
		return resp, nil
	}
	_ = resp.Body.Close()
	t.neverServed = true
	buf := newBufferingResponseWriter()
	writeRetryLater(buf, t.requestID, coldUnavailableMessage)
	return bufferedHTTPResponse(req, buf, true), nil
}

// dedicatedColdAnswer reports whether a complete upstream 404 or 503 body is
// one of Dynamo's "not serving right now" answers (see the top of this file).
func dedicatedColdAnswer(status int, body []byte) bool {
	switch status {
	case http.StatusNotFound:
		return len(bytes.TrimSpace(body)) == 0 || bytes.Contains(body, []byte(dynamoModelNotFoundBodyMarker))
	case http.StatusServiceUnavailable:
		return bodyLooksNotReady(body) || bodyLooksNoBackend(body)
	}
	return false
}

// bodyLooksNoBackend reports whether a 503 body is one of Dynamo's "no worker
// can take this request now" answers (dynamoNoBackendBodyMarkers), as opposed
// to its overload 503.
func bodyLooksNoBackend(body []byte) bool {
	for _, marker := range dynamoNoBackendBodyMarkers {
		if bytes.Contains(body, []byte(marker)) {
			return true
		}
	}
	return false
}
