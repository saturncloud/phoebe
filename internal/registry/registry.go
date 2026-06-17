// Package registry resolves a model (from X-Saturn-Resource-Id) to its
// upstream URL. The resolution must support new models without a redeploy
// and fail cleanly (404/410) for torn-down models.
//
// This file defines the contract plus a trivial static implementation. A
// later implementation will resolve via a Service-naming convention or a
// cached control-plane lookup.
package registry

import (
	"errors"
	"net/url"
)

// ErrNotFound indicates the model/resource has no live upstream. Callers
// should translate this to a clean 404/410, never a hang or misroute.
var ErrNotFound = errors.New("model upstream not found")

// Resolver maps a resource ID to an upstream base URL.
type Resolver interface {
	// Resolve returns the upstream base URL for the given resource ID.
	// Returns ErrNotFound if the model is unknown or torn down.
	Resolve(resourceID string) (*url.URL, error)
}

// Static is a fixed map of resource ID → upstream, with an optional default.
// Useful for the walking skeleton and tests. Topology-independent: the
// upstream may be an engine (Shape A) or a router (Shape B); the interceptor
// behaves identically either way.
type Static struct {
	upstreams map[string]*url.URL
	fallback  *url.URL
}

// NewStatic builds a Static resolver. fallback may be nil.
func NewStatic(fallback *url.URL) *Static {
	return &Static{
		upstreams: make(map[string]*url.URL),
		fallback:  fallback,
	}
}

// Set registers an upstream for a resource ID.
func (s *Static) Set(resourceID string, upstream *url.URL) {
	s.upstreams[resourceID] = upstream
}

func (s *Static) Resolve(resourceID string) (*url.URL, error) {
	if u, ok := s.upstreams[resourceID]; ok {
		return u, nil
	}
	if s.fallback != nil {
		return s.fallback, nil
	}
	return nil, ErrNotFound
}
