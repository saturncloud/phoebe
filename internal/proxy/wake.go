package proxy

import (
	"context"
	"net"
	"strings"

	"github.com/saturncloud/phoebe/internal/identity"
)

// WakeTarget identifies what a wake actuates. The proxy resolves every field
// once so the actuator never has to re-parse request or routing data.
type WakeTarget struct {
	// UpstreamHost is the trusted upstream host:port whose /v1/models endpoint
	// is polled for readiness.
	UpstreamHost string
	// GraphK8sName is the Dynamo graph (DGD) k8s name whose worker DGDSA the
	// waker scales. On the gateway path it is the tf_model row's
	// graph_k8s_name, threaded through resolution verbatim; on header-routed
	// requests it is derived once from the upstream host.
	GraphK8sName string
	// ResourceID is the atlas-authorized resource id (authorization proof at
	// wake time + audit).
	ResourceID string
	// ServedModel is the exact model that must appear in /v1/models before the
	// customer's inference request is forwarded.
	ServedModel string
}

// Waker triggers a shared base graph's 0->1 scale-up and blocks until the
// requested model is ready to serve (or the context/deadline is exceeded).
// Readiness probes must be non-billable: Wake must never forward or replay the
// customer's inference request.
type Waker interface {
	Wake(ctx context.Context, target WakeTarget) error
}

// isWakeable reports whether a request is eligible for proactive wake-from-zero:
// it must be a shared-mode route and carry an atlas-authorized resource id.
// Dedicated routes (no served-model allow-list) are never woken here.
func isWakeable(id identity.Identity) bool {
	return id.ResourceID != "" && id.ServedModel != ""
}

// graphFromUpstreamHost derives the Dynamo graph (DGD) k8s name from a trusted
// upstream host for header-routed wakes. The gateway path threads the resolved
// graph name directly. A Dynamo frontend Service is `<graph>-frontend`, so the
// suffix is stripped; a label without it is already the k8s name.
func graphFromUpstreamHost(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	first, _, _ := strings.Cut(host, ".")
	return strings.TrimSuffix(first, "-frontend")
}

// wakeEnabled reports whether this request should wait for proactive
// wake/readiness before its single inference forward.
func (s *Server) wakeEnabled(id identity.Identity) bool {
	return s.waker != nil && isWakeable(id)
}
