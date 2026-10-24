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

// dynamoModelNotFoundBodyMarker is the message of Dynamo's frontend 404 for a
// model with no registration (lib/llm/src/http/service/openai.rs
// model_not_found, v1.5.0 and v1.6.0). A 404 without it is some other 404 and
// is passed through.
const dynamoModelNotFoundBodyMarker = "Model not found"

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
	cold, err := bufferColdCandidateWithin(req.Context(), resp, coldCandidateReadTimeout)
	if err != nil {
		return nil, err
	}
	if !cold {
		return resp, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		// bufferColdCandidate replaced the body with an in-memory replay of the
		// whole (bounded) body, so it can be read here and handed back.
		body := readReplay(resp)
		if !bytes.Contains(body, []byte(dynamoModelNotFoundBodyMarker)) {
			return resp, nil
		}
	}
	_ = resp.Body.Close()
	t.neverServed = true
	buf := newBufferingResponseWriter()
	writeRetryLater(buf, t.requestID, coldUnavailableMessage)
	return bufferedHTTPResponse(req, buf, true), nil
}

// readReplay reads a body bufferColdCandidate already replaced with an
// in-memory replay and restores it, so the response can still be returned.
func readReplay(resp *http.Response) []byte {
	var b bytes.Buffer
	_, _ = b.ReadFrom(resp.Body)
	resp.Body = &replayReadCloser{Reader: bytes.NewReader(b.Bytes()), closer: resp.Body}
	return b.Bytes()
}
