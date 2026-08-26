// Package waker is the concrete wake-from-zero actuator: it implements the
// proxy's Waker seam (internal/proxy/wake.go) by scaling the cold graph's
// DynamoGraphDeploymentScalingAdapter (DGDSA) directly with client-go, from
// the MAIN phoebe container (ratified design: augmented RBAC on phoebe's own
// ServiceAccount, no separate waker pod — see deploy/rbac-waker.yaml).
//
// THE SETTLED SCALING REGIME (three DISJOINT actors, all driving the DGDSA —
// the operator's single source of truth for the worker replica count):
//
//	0->1 wake  = phoebe (THIS package), on a cold response for a wakeable route.
//	1->N load  = KEDA (ScaledObject -> DGDSA, minReplicaCount: 1, never 0).
//	1->0 reap  = the Atlas reaper (idle detection off the metering stream).
//
// THE KEDA PAUSE HANDOFF (order is LOAD-BEARING, verified against KEDA 2.20.2
// semantics): the reaper parks a graph by annotating its ScaledObject
// `autoscaling.keda.sh/paused: "true"` FIRST (KEDA deletes its HPA and stops
// touching replicas) and THEN scaling the DGDSA to 0. The waker runs the exact
// inverse: patch the DGDSA 0->1 FIRST (the pod starts NOW — never wait on
// KEDA's laggy unpause reconcile), THEN annotate paused: "false" to hand the
// 1->N range back to KEDA (whose recreated HPA immediately holds >=1, so the
// unpause can never undo the wake). Names mirror what Atlas renders:
// DGDSA `<graph>-vllmworker` (Dynamo v1.4.0 generateAdapterName), ScaledObject
// `<graph>-scaler` (token_factory._render_keda_scaledobject); the annotation
// value "false" (not removal) matches the Atlas reaper's own unpause.
//
// FAIL CLOSED / NEVER DOWN: a missing DGDSA or any read/patch failure on it
// errors the wake — the proxy then serves the honest cold response. The waker
// only ever writes replicas: 1, and only after reading 0; it can NEVER scale
// down (the reaper alone owns 1->0). The ScaledObject unpause is best-effort:
// a KEDA-less install (no ScaledObject) still wakes — the scale-up is the
// critical step.
package waker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/proxy"
)

// The two CRDs the waker touches — via the dynamic client, so phoebe carries
// no nvidia/keda typed API dependency.
var (
	dgdsaGVR = schema.GroupVersionResource{
		Group: "nvidia.com", Version: "v1alpha1", Resource: "dynamographdeploymentscalingadapters",
	}
	scaledObjectGVR = schema.GroupVersionResource{
		Group: "keda.sh", Version: "v1alpha1", Resource: "scaledobjects",
	}
)

// kedaPausedAnnotation is KEDA's pause switch. PLAIN paused — deliberately NOT
// `paused-replicas`, which re-asserts a pinned count and has a stuck-state
// race. Value semantics ("true"/"false") are parsed by KEDA since v2.13.0;
// the Atlas reaper writes the same annotation from the other side.
const kedaPausedAnnotation = "autoscaling.keda.sh/paused"

// DGDSAName returns the DGDSA that scales a graph's vLLM worker:
// `<graph>-vllmworker` — Dynamo v1.4.0 generateAdapterName
// ("<dgd>-<lowercase(component)>" for the VllmWorker component), the same
// name Atlas's shared_base_dgdsa_name computes for KEDA and the reaper.
func DGDSAName(graphK8sName string) string { return graphK8sName + "-vllmworker" }

// ScaledObjectName returns the graph's KEDA ScaledObject: `<graph>-scaler`,
// exactly as Atlas renders it (token_factory._render_keda_scaledobject).
func ScaledObjectName(graphK8sName string) string { return graphK8sName + "-scaler" }

// Config wires a KubeWaker.
type Config struct {
	// Namespace the DGDSAs and ScaledObjects live in (the shared-graph
	// namespace — the same value as the gateway config's namespace). Required.
	Namespace string
	// Kubeconfig is a kubeconfig file path for dev/tests. Empty (production)
	// uses in-cluster config.
	Kubeconfig string
	// PollInterval between readiness probes while holding a woken request.
	// <=0 uses 2s.
	PollInterval time.Duration
}

// KubeWaker implements proxy.Waker by patching the graph's DGDSA and
// unpausing its ScaledObject, then holding until the upstream is ready.
type KubeWaker struct {
	client       dynamic.Interface
	namespace    string
	log          *logging.Logger
	pollInterval time.Duration

	// ready reports whether the upstream serves again after a wake. The
	// default probes GET /v1/models (see upstreamServesModels); a func field
	// so tests drive readiness deterministically.
	ready func(ctx context.Context, upstreamHost string) bool

	// mu guards graphLocks; each per-graph lock serializes the read-then-patch
	// of one graph inside THIS process, so N concurrent cold requests for one
	// graph issue ONE effective scale patch (the rest observe replicas>=1 and
	// skip). Cross-process races (multiple phoebe replicas) are benign by
	// construction: everyone writes the same replicas: 1.
	mu         sync.Mutex
	graphLocks map[string]*sync.Mutex
}

// compile-time: KubeWaker satisfies the proxy's Waker seam.
var _ proxy.Waker = (*KubeWaker)(nil)

// New builds a KubeWaker from in-cluster config, or from cfg.Kubeconfig when
// set (dev/tests). An unavailable cluster config is an error — the caller
// (main) logs it and runs WITHOUT a waker (cold responses pass through);
// it must never crash the proxy.
func New(cfg Config, log *logging.Logger) (*KubeWaker, error) {
	if cfg.Namespace == "" {
		return nil, errors.New("waker: namespace is required")
	}
	var (
		rc  *rest.Config
		err error
	)
	if cfg.Kubeconfig != "" {
		rc, err = clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	} else {
		rc, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("waker: kubernetes config: %w", err)
	}
	client, err := dynamic.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("waker: kubernetes client: %w", err)
	}
	return NewWithClient(client, cfg, log), nil
}

// NewWithClient builds a KubeWaker over an existing dynamic client (the seam
// tests use with client-go's fake dynamic client).
func NewWithClient(client dynamic.Interface, cfg Config, log *logging.Logger) *KubeWaker {
	pi := cfg.PollInterval
	if pi <= 0 {
		pi = 2 * time.Second
	}
	w := &KubeWaker{
		client:       client,
		namespace:    cfg.Namespace,
		log:          log,
		pollInterval: pi,
		graphLocks:   map[string]*sync.Mutex{},
	}
	w.ready = w.upstreamServesModels
	return w
}

// Wake implements proxy.Waker: scale the target graph's worker 0->1 (KEDA
// pause-handoff order), then HOLD until the upstream serves again or ctx
// expires. The Waker contract requires blocking-until-ready: the proxy's
// retry loop is bounded by attempts, not time, so a Wake that returned at
// patch time would exhaust the retries in milliseconds while the worker
// spends minutes loading weights.
func (w *KubeWaker) Wake(ctx context.Context, target proxy.WakeTarget) error {
	if target.GraphK8sName == "" {
		return errors.New("waker: no graph name on wake target")
	}
	if err := w.scaleUp(ctx, target.GraphK8sName, target.ResourceID); err != nil {
		return err
	}
	return w.waitReady(ctx, target.UpstreamHost)
}

// scaleUp performs the actual 0->1, serialized per graph within this process:
//
//  1. READ the DGDSA. Any failure (including NotFound) errors the wake — the
//     DGDSA is the proof a scalable graph exists; without it there is nothing
//     safe to actuate (and the proxy then serves the honest cold response).
//  2. spec.replicas >= 1 → the graph is already awake (another request/replica
//     won the race, or the cold response was transient churn): SUCCESS with no
//     writes. This branch is also the never-scale-down invariant — the waker
//     writes only when the current count is 0.
//  3. Patch spec.replicas: 1 (merge patch on the DGDSA object, the same write
//     the Atlas reaper uses in the other direction). Idempotent by value.
//  4. THEN unpause KEDA (annotation "false") — best-effort: a missing
//     ScaledObject (KEDA-less install, or not yet rendered) is logged and
//     ignored; the scale-up above is the critical step. Order matters — see
//     the package comment.
func (w *KubeWaker) scaleUp(ctx context.Context, graph, resourceID string) error {
	lock := w.graphLock(graph)
	lock.Lock()
	defer lock.Unlock()

	name := DGDSAName(graph)
	dgdsa, err := w.client.Resource(dgdsaGVR).Namespace(w.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("waker: read DGDSA %s/%s: %w", w.namespace, name, err)
	}
	replicas, found, err := unstructured.NestedInt64(dgdsa.Object, "spec", "replicas")
	if err != nil {
		return fmt.Errorf("waker: DGDSA %s/%s spec.replicas unreadable: %w", w.namespace, name, err)
	}
	if found && replicas >= 1 {
		// Already awake — nothing to write (and NEVER write downward).
		return nil
	}

	patch := []byte(`{"spec":{"replicas":1}}`)
	if _, err := w.client.Resource(dgdsaGVR).Namespace(w.namespace).
		Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("waker: scale DGDSA %s/%s to 1: %w", w.namespace, name, err)
	}
	w.log.Info.Printf("waker: scaled DGDSA %s/%s 0->1 (resource_id=%s)", w.namespace, name, resourceID)

	w.unpauseKEDA(ctx, graph)
	return nil
}

// unpauseKEDA hands the 1->N range back to KEDA by annotating the graph's
// ScaledObject paused: "false" (the value the reaper's own unpause writes; the
// recreated HPA holds minReplicaCount: 1, so this can never undo the wake).
// Best-effort by design: the wake already succeeded.
func (w *KubeWaker) unpauseKEDA(ctx context.Context, graph string) {
	name := ScaledObjectName(graph)
	patch := []byte(`{"metadata":{"annotations":{"` + kedaPausedAnnotation + `":"false"}}}`)
	_, err := w.client.Resource(scaledObjectGVR).Namespace(w.namespace).
		Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
	switch {
	case err == nil:
	case apierrors.IsNotFound(err):
		// KEDA-less install or ScaledObject not rendered yet — fine; without a
		// ScaledObject there is no HPA to hand anything back to.
		w.log.Info.Printf("waker: no ScaledObject %s/%s to unpause (KEDA-less?)", w.namespace, name)
	default:
		w.log.Warn.Printf("waker: unpause ScaledObject %s/%s failed (wake still succeeded): %v", w.namespace, name, err)
	}
}

// graphLock returns the per-graph mutex, creating it on first use.
func (w *KubeWaker) graphLock(graph string) *sync.Mutex {
	w.mu.Lock()
	defer w.mu.Unlock()
	if l, ok := w.graphLocks[graph]; ok {
		return l
	}
	l := &sync.Mutex{}
	w.graphLocks[graph] = l
	return l
}

// waitReady polls w.ready until the upstream serves again or ctx expires.
// Returns ctx.Err() on expiry — the proxy then flushes the honest cold
// response rather than hanging forever.
func (w *KubeWaker) waitReady(ctx context.Context, upstreamHost string) error {
	if w.ready(ctx, upstreamHost) {
		return nil
	}
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("waker: upstream %s not ready before deadline: %w", upstreamHost, ctx.Err())
		case <-ticker.C:
			if w.ready(ctx, upstreamHost) {
				return nil
			}
		}
	}
}

// upstreamServesModels is the default readiness probe: GET /v1/models on the
// upstream and report ready when at least one model is registered.
//
// WHY THIS SIGNAL (verified against Dynamo v1.4.0): at 0 workers the frontend
// process stays up (its /health is 200 — useless as a wake signal) but
// DELETES every model object from discovery, which is exactly why the cold
// request 404s. The model reappearing in /v1/models is therefore the same
// discovery event that ends the 404 — the earliest moment a re-probe can
// succeed — and it needs no extra RBAC. A transient post-registration "not
// ready yet" 503 is handled by the proxy's own probe/retry loop above us.
func (w *KubeWaker) upstreamServesModels(ctx context.Context, upstreamHost string) bool {
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, "http://"+upstreamHost+"/v1/models", nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var body struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return false
	}
	return len(body.Data) > 0
}
