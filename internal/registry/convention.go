package registry

import (
	"fmt"
	"net/url"
	"strings"
)

// ConventionResolver maps a resource ID to an upstream URL via a naming
// convention template. In Kubernetes, every deployed model Service follows a
// predictable name (e.g. "model-<id>.<namespace>.svc.cluster.local"), so
// resolution requires no control-plane round-trip and no redeploy — the
// Service exists by naming convention as soon as the deployment is live.
//
// Template substitution: {id} is replaced with the resource ID. Example:
//
//	http://model-{id}.{namespace}.svc.cluster.local:{port}
//
// This is the zero-lookup, zero-redeploy path; it degrades gracefully when
// the control plane is unavailable (used as fallback in ChainResolver).
type ConventionResolver struct {
	cfg ConventionConfig
}

// ConventionConfig holds the naming-convention resolver settings.
type ConventionConfig struct {
	// Template is the URL template with {id} replaced by the resource ID.
	// Must produce a valid URL after substitution. Required.
	// Example: "http://model-{id}.inference.svc.cluster.local:8000"
	Template string

	// Scheme overrides the URL scheme (default: "http"). Ignored when
	// Template already contains a full URL with scheme.
	Scheme string
}

// NewConventionResolver creates a ConventionResolver from cfg.
// Returns an error if Template is empty.
func NewConventionResolver(cfg ConventionConfig) (*ConventionResolver, error) {
	if cfg.Template == "" {
		return nil, fmt.Errorf("registry: ConventionResolver requires a non-empty Template")
	}
	return &ConventionResolver{cfg: cfg}, nil
}

// Resolve substitutes {id} into the template and returns the resulting URL.
// Returns ErrNotFound if the substituted host is empty (misconfiguration guard).
func (r *ConventionResolver) Resolve(resourceID string) (*url.URL, error) {
	raw := strings.ReplaceAll(r.cfg.Template, "{id}", resourceID)
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("registry: convention template produced invalid URL %q: %w", raw, err)
	}
	if u.Host == "" {
		return nil, ErrNotFound
	}
	return u, nil
}
