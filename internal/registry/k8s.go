package registry

import (
	"context"
	"fmt"
	"net/url"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// k8s.go implements the REAL control-plane LookupFunc: resolve a deployment's
// upstream by reading its in-cluster Service, selected by the
// saturncloud.io/resource-id label.
//
// WHY this and not a naming convention or an Atlas API call:
//   - Saturn stamps `saturncloud.io/resource-id: <deployment-id>` on every
//     inference Service (pdc basic_resource_labels), and phoebe receives that
//     exact id as X-Saturn-Resource-Id. So the label IS the join key — no name
//     reconstruction (the Service name embeds the group prefix + endpoint name,
//     which phoebe never receives) and no new Atlas API/headers.
//   - The Service name template can change on the Atlas side without breaking
//     phoebe; the label is set by the same resource-labels path that labels
//     every Saturn object, so this resolution is self-correcting.
//
// ROUTING CONTRACT (the one-way door phoebe now depends on — surfaced for Hugo):
//   1. inference Services carry `saturncloud.io/resource-id: <id>` and
//      `saturncloud.io/service-type: internal`.
//   2. the served (vLLM) port is 8000 (== Deployment.proxy_port); the Service
//      port is named "8000" (Route.port_name = str(container_port)).
// A deployment also has an ssh Service sharing the resource-id label, so the
// service-type=internal selector is REQUIRED to pick the model endpoint, not ssh.

const (
	// labelResourceID is the Saturn label whose value equals the deployment id
	// phoebe receives as X-Saturn-Resource-Id (pdc labels.py Labels.RESOURCE_ID).
	labelResourceID = "saturncloud.io/resource-id"
	// labelServiceType distinguishes the model Service ("internal") from the
	// deployment's ssh Service ("ssh"), both of which carry the resource-id label.
	labelServiceType    = "saturncloud.io/service-type"
	serviceTypeInternal = "internal"
	// servedPortName is the Service port name for the vLLM serve port. Route
	// port names are str(container_port); the served port is 8000.
	servedPortName = "8000"
)

// K8sLookupConfig configures the label-based Service resolver.
type K8sLookupConfig struct {
	// Namespace is the namespace inference Services live in (Saturn: "main-namespace").
	Namespace string
	// Client is the Kubernetes clientset. Injected so the lookup is unit-testable
	// against a fake clientset; production builds it from the in-cluster config.
	Client kubernetes.Interface
}

// NewK8sLookup returns a LookupFunc that resolves a deployment id to its model
// Service's cluster-local URL via the saturncloud.io/resource-id label.
//
// It returns ErrNotFound when no internal Service carries the id (unknown or
// torn-down model — negative-cached by the CachedResolver with a short TTL). Any
// other error (API failure, ambiguous match, no served port) is transient and is
// NOT cached, so the next request retries.
func NewK8sLookup(cfg K8sLookupConfig) (LookupFunc, error) {
	if cfg.Namespace == "" {
		return nil, fmt.Errorf("registry: K8sLookup requires a non-empty Namespace")
	}
	if cfg.Client == nil {
		return nil, fmt.Errorf("registry: K8sLookup requires a non-nil Client")
	}
	ns := cfg.Namespace
	client := cfg.Client
	return func(ctx context.Context, resourceID string) (*url.URL, error) {
		// Select the model's internal Service. The service-type=internal clause is
		// load-bearing: the deployment's ssh Service shares the resource-id label.
		selector := fmt.Sprintf("%s=%s,%s=%s",
			labelResourceID, resourceID, labelServiceType, serviceTypeInternal)
		list, err := client.CoreV1().Services(ns).List(ctx, metav1.ListOptions{
			LabelSelector: selector,
			Limit:         2, // 0/1 expected; >1 is an error, so 2 is enough to detect it.
		})
		if err != nil {
			// Transient: kube API unreachable/throttled. Not ErrNotFound, so the
			// CachedResolver treats it as a retryable error (not negative-cached).
			return nil, fmt.Errorf("registry: list services for %s: %w", resourceID, err)
		}
		switch len(list.Items) {
		case 0:
			return nil, ErrNotFound
		case 1:
			// ok
		default:
			return nil, fmt.Errorf("registry: ambiguous upstream for %s: %d internal services match",
				resourceID, len(list.Items))
		}

		svc := list.Items[0]
		port, err := servedPort(&svc)
		if err != nil {
			return nil, fmt.Errorf("registry: %s: %w", resourceID, err)
		}
		raw := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", svc.Name, ns, port)
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("registry: resolved invalid URL %q for %s: %w", raw, resourceID, err)
		}
		return u, nil
	}, nil
}

// servedPort picks the vLLM serve port (8000) from a model Service. It prefers
// the port NAMED "8000" (Route.port_name == str(container_port)); if the Service
// has exactly one port it uses that; otherwise it errors rather than guess (a
// deployment can expose multiple route ports, and silently picking the wrong one
// would route metered traffic to the wrong listener).
func servedPort(svc *corev1.Service) (int32, error) {
	ports := svc.Spec.Ports
	for _, p := range ports {
		if p.Name == servedPortName {
			return p.Port, nil
		}
	}
	if len(ports) == 1 {
		return ports[0].Port, nil
	}
	return 0, fmt.Errorf("no served port: want a port named %q among %d ports on service %s",
		servedPortName, len(ports), svc.Name)
}

// InClusterClient builds a Kubernetes clientset from the in-cluster service
// account config. phoebe's interceptor runs as a pod, so this is the production
// path; it requires an RBAC Role granting get/list on services in the namespace.
func InClusterClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("registry: in-cluster config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("registry: build clientset: %w", err)
	}
	return cs, nil
}
