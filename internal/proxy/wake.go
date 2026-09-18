package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/identity"
)

type WakeTarget struct {
	UpstreamHost string
	GraphK8sName string
	ResourceID   string
}

type Waker interface {
	Wake(ctx context.Context, target WakeTarget) error
}

const dynamoNotReadyBodyMarker = "is not ready to serve requests yet"

func isWakeable(id identity.Identity) bool {
	return id.ResourceID != "" && id.ServedModel != ""
}

func graphFromUpstreamHost(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	first, _, _ := strings.Cut(host, ".")
	return strings.TrimSuffix(first, "-frontend")
}

func bodyLooksNotReady(body []byte) bool {
	return bytes.Contains(body, []byte(dynamoNotReadyBodyMarker))
}

func (s *Server) wakeEnabled(id identity.Identity) bool {
	return s.waker != nil && isWakeable(id)
}

// Dynamo's cold error is tiny. A larger 503 is an ordinary upstream response
// and is streamed after restoring the bounded prefix inspected here.
const coldInspectionBytes = 64 << 10

type prefixReadCloser struct {
	io.Reader
	closer io.Closer
}

func (r *prefixReadCloser) Close() error { return r.closer.Close() }

// isColdResponse never reads a warm/success response, preserving streaming and
// making the first warm inference the one returned to the client.
func isColdResponse(resp *http.Response) (bool, error) {
	if resp.StatusCode == http.StatusNotFound {
		return true, nil
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		return false, nil
	}
	original := resp.Body
	prefix, err := io.ReadAll(io.LimitReader(original, coldInspectionBytes+1))
	if err != nil {
		resp.Body = &prefixReadCloser{
			Reader: io.MultiReader(bytes.NewReader(prefix), original),
			closer: original,
		}
		return false, err
	}
	if len(prefix) > coldInspectionBytes {
		resp.Body = &prefixReadCloser{
			Reader: io.MultiReader(bytes.NewReader(prefix), original),
			closer: original,
		}
		return false, nil
	}
	_ = original.Close()
	resp.Body = io.NopCloser(bytes.NewReader(prefix))
	resp.ContentLength = int64(len(prefix))
	return bodyLooksNotReady(prefix), nil
}

type admissionRoundTripError struct{ err error }

func (e *admissionRoundTripError) Error() string { return e.err.Error() }
func (e *admissionRoundTripError) Unwrap() error { return e.err }

// wakeRoundTripper retries only a positively identified cold response. A warm
// response reaches ReverseProxy untouched and is streamed, captured, and
// settled exactly once.
type wakeRoundTripper struct {
	server    *Server
	base      http.RoundTripper
	target    WakeTarget
	requestID string
	lease     *admission.Lease
}

func (t *wakeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	maxTries := t.server.wakeMaxTries
	if maxTries < 1 {
		maxTries = 1
	}
	current := req
	for attempt := 0; attempt < maxTries; attempt++ {
		resp, err := t.base.RoundTrip(current)
		if err != nil {
			return nil, err
		}
		cold, inspectErr := isColdResponse(resp)
		if inspectErr != nil || !cold {
			// An inspection read failure is not evidence of a cold graph. Let
			// ReverseProxy handle the restored body as an ordinary response.
			return resp, nil
		}
		if attempt == maxTries-1 {
			return resp, nil
		}

		if t.lease != nil {
			if aerr := t.lease.BeginColdHold(req.Context()); aerr != nil {
				if errors.Is(aerr, admission.ErrUnavailable) {
					t.server.log.Error.Printf("admission: cold-hold state unavailable; bypassing distributed gate for request_id=%s: %v", t.requestID, aerr)
				} else {
					_ = resp.Body.Close()
					return nil, &admissionRoundTripError{err: aerr}
				}
			}
		}

		ctx := req.Context()
		cancel := func() {}
		if t.server.wakeTimeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, t.server.wakeTimeout)
		}
		werr := t.server.waker.Wake(ctx, t.target)
		cancel()
		if t.lease != nil {
			if aerr := t.lease.EndColdHold(context.WithoutCancel(req.Context())); aerr != nil {
				t.server.log.Error.Printf("admission: cold-hold release failed: %v", aerr)
			}
		}
		if werr != nil {
			t.server.log.Warn.Printf("wake: could not warm base for request_id=%s resource_id=%s: %v", t.requestID, t.target.ResourceID, werr)
			return resp, nil
		}

		_ = resp.Body.Close()
		if req.GetBody == nil {
			return nil, fmt.Errorf("wake: request body is not replayable")
		}
		body, err := req.GetBody()
		if err != nil {
			return nil, fmt.Errorf("wake: replay request body: %w", err)
		}
		current = req.Clone(req.Context())
		current.Body = body
		current.GetBody = req.GetBody
	}
	panic("unreachable")
}

func (s *Server) newWakeRoundTripper(upstreamHost, requestID string, id identity.Identity, lease *admission.Lease) http.RoundTripper {
	graph := id.GraphK8sName
	if graph == "" {
		graph = graphFromUpstreamHost(upstreamHost)
	}
	return &wakeRoundTripper{
		server: s,
		base:   http.DefaultTransport,
		target: WakeTarget{
			UpstreamHost: upstreamHost,
			GraphK8sName: graph,
			ResourceID:   id.ResourceID,
		},
		requestID: requestID,
		lease:     lease,
	}
}
