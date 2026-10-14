package gateway

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

// NewKubeClient builds the typed clientset the registry resolver watches
// with: in-cluster config in production, a kubeconfig path for dev/tests
// (mirrors the waker's config loading).
func NewKubeClient(kubeconfig string) (kubernetes.Interface, error) {
	var (
		rc  *rest.Config
		err error
	)
	if kubeconfig != "" {
		rc, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		rc, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("gateway: kubernetes config: %w", err)
	}
	return kubernetes.NewForConfig(rc)
}

// NewDynamicClient is NewKubeClient's dynamic client, for the custom
// resources phoebe reads (the DynamoGraphDeployments' ready workers).
func NewDynamicClient(kubeconfig string) (dynamic.Interface, error) {
	var (
		rc  *rest.Config
		err error
	)
	if kubeconfig != "" {
		rc, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		rc, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("gateway: kubernetes config: %w", err)
	}
	return dynamic.NewForConfig(rc)
}

// THE REGISTRY CONTRACT (contract of record, coordinated with the Atlas side
// on saturn branch hugo/tf-served-model-registry — Atlas renders these
// objects; phoebe only watches them):
//
//	Kind:      ConfigMap, in the WORKLOAD namespace (gateway.namespace)
//	Label:     saturncloud.io/tf-served-model: "true"
//	Name:      tf-model-<model_id>
//	Data keys: org_id            — tenant org (the tenancy boundary)
//	           served_model_name — the request-body model= string, matched
//	                               BYTE-EXACT like every other binding
//	           resource_id       — the tf_model row id (billing resource id)
//	           base_model        — HF base id (the catalog price key)
//	           adapter           — fine-tune checkpoint id; MAY BE EMPTY
//	                               (base-model endpoint)
//	           ready             — "true" only after Atlas observes an
//	                               adapter DynamoModel ready; missing or any
//	                               other value fails closed for adapters
//	           serving_mode      — REQUIRED, exactly "shared" | "dedicated"
//	                               (SKU axis). It is load-bearing at request
//	                               time — it selects the shared price row, the
//	                               shared admission gate, and wake-from-zero
//	                               eligibility (proxy.isWakeable) — so an
//	                               empty or unrecognized value is REJECTED,
//	                               never defaulted.
//	           graph_k8s_name    — the serving DGD k8s name
//	           port              — the graph's OpenAI-compatible serve port
//	           admission_max_active_requests,
//	           admission_max_reserved_kv_tokens,
//	           admission_max_prefill_tokens
//	                             — OPTIONAL PER-WORKER admission capacity
//	                               (B8; per worker since Q22): non-negative
//	                               integers, empty or absent = no limit.
//	                               Phoebe enforces each graph at the value
//	                               times (ready workers + 1); see
//	                               GraphCapacity and ReadyWorkers
//
// Atlas guarantees (org_id, served_model_name) uniqueness across live
// registrations; the resolver still tolerates duplicates deterministically
// (see resolveClaimants) because a watch cache must never panic on data it
// didn't author.
const (
	// RegistryLabel selects the served-model registry ConfigMaps.
	RegistryLabel = "saturncloud.io/tf-served-model"
	// registryLabelSelector is the informer's server-side filter.
	registryLabelSelector = RegistryLabel + "=true"
)

// ErrNotSynced reports that the registry informer has not completed its
// initial list yet. The proxy fails CLOSED on it (503, like any resolver
// failure): before the first sync phoebe cannot distinguish "model doesn't
// exist" from "haven't loaded the registry", and a 404 would be a lie.
var ErrNotSynced = errors.New("gateway: served-model registry not synced yet")

// RegistryResolver resolves gateway (org, model) pairs from a client-go
// informer over the served-model registry ConfigMaps: a label-selected,
// namespace-scoped watch feeding an in-memory index keyed byte-exactly on
// (org_id, served_model_name). Lookups are PURE MEMORY READS — no per-request
// I/O, so no TTL cache in front (the informer IS the cache, kept fresh by the
// watch instead of by expiry).
//
// AVAILABILITY TRADEOFF (deliberate, data-plane autonomy): before the FIRST
// sync the resolver fails closed (ErrNotSynced → 503) — it has no state worth
// trusting. AFTER the first sync it never un-syncs: if the API server / hub
// connection is lost, the resolver KEEPS SERVING the last-known state while
// client-go's reflector retries in the background (each failed re-list/watch
// logs loudly via the watch error handler). A hub or API-server flap
// therefore cannot kill warm serving; the cost is that registrations made
// DURING an outage are invisible until the watch reconnects (new models 404,
// deleted models keep resolving) — bounded staleness, never a dead data
// plane.
type RegistryResolver struct {
	log      *logging.Logger
	informer cache.SharedIndexInformer
	synced   atomic.Bool

	mu sync.RWMutex
	// byKey holds every live claimant of an (org, served name) key, indexed by
	// ConfigMap name — a map, not a single row, so a transient duplicate
	// claim and its later deletion both resolve correctly (delete the winner
	// and the survivor takes over).
	byKey map[cacheKey]map[string]Resolution
	// cmKey remembers which key each ConfigMap currently claims, so an UPDATE
	// that moves a ConfigMap to a different (org, name) — or breaks it — also
	// removes the old claim.
	cmKey map[string]cacheKey
	// byGraph holds each graph's published capacity per ConfigMap, and
	// cmGraph which graph each ConfigMap last published for, so an update or
	// delete withdraws exactly that entry's contribution.
	byGraph map[string]map[string]GraphCapacity
	cmGraph map[string]string
	// workers, when set, holds each graph's ready workers: Atlas publishes
	// PER-WORKER limits and GraphCapacity multiplies them by (ready workers +
	// GraphLimitHeadroomWorkers) (Q4(b), option B).
	workers readyWorkerSource
}

// WithReadyWorkers makes GraphCapacity scale the published per-worker limits
// by each graph's ready workers plus GraphLimitHeadroomWorkers.
func (r *RegistryResolver) WithReadyWorkers(w readyWorkerSource) *RegistryResolver {
	r.workers = w
	return r
}

// compile-time: the registry implements the same seam the PG resolver does.
var _ Resolver = (*RegistryResolver)(nil)

// NewRegistryResolver builds the resolver and its informer over client. Call
// Run to start watching; Resolve fails closed (ErrNotSynced) until the
// initial sync completes.
func NewRegistryResolver(client kubernetes.Interface, namespace string, log *logging.Logger) *RegistryResolver {
	factory := informers.NewSharedInformerFactoryWithOptions(
		client,
		0, // no forced resync: the watch keeps the index fresh; re-lists happen on reconnect
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.LabelSelector = registryLabelSelector
		}),
	)
	r := &RegistryResolver{
		log:      log,
		informer: factory.Core().V1().ConfigMaps().Informer(),
		byKey:    map[cacheKey]map[string]Resolution{},
		cmKey:    map[string]cacheKey{},
		byGraph:  map[string]map[string]GraphCapacity{},
		cmGraph:  map[string]string{},
	}

	// Loud, periodic connectivity logging: client-go invokes this on every
	// failed list/watch (with its own backoff cadence), so an API-server
	// outage is visible in the logs for as long as it lasts — while the
	// index above keeps serving last-known state (see the type comment).
	_ = r.informer.SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
		if r.synced.Load() {
			r.log.Error.Printf("gateway: served-model registry watch failed (%v) — SERVING LAST-KNOWN registrations until the watch reconnects (new/deleted models invisible)", err)
		} else {
			r.log.Error.Printf("gateway: served-model registry initial sync failing (%v) — gateway requests 503 until the first sync completes", err)
		}
	})

	_, _ = r.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { r.upsert(obj) },
		UpdateFunc: func(_, obj any) { r.upsert(obj) },
		DeleteFunc: func(obj any) { r.remove(obj) },
	})
	return r
}

// Run starts the informer and flips the resolver to synced once the initial
// list completes. It does NOT block on the sync (serving must start — gateway
// requests 503 until sync); it returns when ctx is cancelled.
func (r *RegistryResolver) Run(ctx context.Context) {
	go func() {
		if cache.WaitForCacheSync(ctx.Done(), r.informer.HasSynced) {
			r.synced.Store(true)
			r.log.Info.Printf("gateway: served-model registry synced (%d registrations)", r.size())
		}
	}()
	r.informer.Run(ctx.Done())
}

// Resolve implements Resolver from the in-memory index. No I/O.
func (r *RegistryResolver) Resolve(_ context.Context, orgID, model string) (Resolution, error) {
	if !r.synced.Load() {
		return Resolution{}, ErrNotSynced
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	claimants := r.byKey[cacheKey{org: orgID, model: model}]
	if len(claimants) == 0 {
		return Resolution{}, ErrNotFound
	}
	return r.resolveClaimants(orgID, model, claimants), nil
}

// resolveClaimants picks the winner among ConfigMaps claiming one (org, name)
// key: the lexicographically-lowest resource_id — DETERMINISTIC, so every
// request (and every phoebe replica) agrees, and a duplicate can never make
// resolution flap between two resources per-request. More than one claimant
// should be impossible (Atlas enforces uniqueness on live registrations) —
// it is logged loudly as the Atlas-side bug it would be, but the resolver
// must not panic or fail serving over data it didn't author.
func (r *RegistryResolver) resolveClaimants(orgID, model string, claimants map[string]Resolution) Resolution {
	if len(claimants) == 1 {
		for _, res := range claimants {
			return res
		}
	}
	winner := Resolution{}
	names := make([]string, 0, len(claimants))
	for name, res := range claimants {
		names = append(names, name)
		if winner.ResourceID == "" || res.ResourceID < winner.ResourceID {
			winner = res
		}
	}
	sort.Strings(names)
	r.log.Error.Printf("gateway: %d registry ConfigMaps claim (org=%s, model=%q): %v — Atlas uniqueness violated; deterministically serving resource_id=%s (lowest)",
		len(claimants), orgID, model, names, winner.ResourceID)
	return winner
}

// upsert indexes a ConfigMap (add or update). A row that fails validation is
// logged and DE-indexed (an update can break a previously-valid row; leaving
// the stale claim would serve a registration Atlas no longer asserts).
func (r *RegistryResolver) upsert(obj any) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return
	}
	res, key, err := parseRegistryConfigMap(cm)

	r.mu.Lock()
	defer r.mu.Unlock()
	if old, had := r.cmKey[cm.Name]; had {
		r.dropClaimLocked(cm.Name, old)
	}
	if err != nil {
		r.log.Error.Printf("gateway: ignoring registry ConfigMap %s/%s: %v", cm.Namespace, cm.Name, err)
		return
	}
	capacity, problems := parseGraphCapacity(cm.Data)
	if len(problems) > 0 {
		r.log.Error.Printf("gateway: registry ConfigMap %s/%s publishes malformed admission capacity %v — ignoring those limits (no limit)", cm.Namespace, cm.Name, problems)
	}
	if capacity.Any() {
		if r.byGraph[res.GraphK8sName] == nil {
			r.byGraph[res.GraphK8sName] = map[string]GraphCapacity{}
		}
		r.byGraph[res.GraphK8sName][cm.Name] = capacity
		r.cmGraph[cm.Name] = res.GraphK8sName
	}
	if r.byKey[key] == nil {
		r.byKey[key] = map[string]Resolution{}
	}
	r.byKey[key][cm.Name] = res
	if n := len(r.byKey[key]); n > 1 {
		r.log.Error.Printf("gateway: registry ConfigMap %s joins %d existing claim(s) on (org=%s, model=%q) — Atlas uniqueness violated", cm.Name, n-1, key.org, key.model)
	}
	r.cmKey[cm.Name] = key
}

// remove de-indexes a deleted ConfigMap (handles the tombstone the informer
// delivers when a delete was observed late).
func (r *RegistryResolver) remove(obj any) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		tomb, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		if cm, ok = tomb.Obj.(*corev1.ConfigMap); !ok {
			return
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if key, had := r.cmKey[cm.Name]; had {
		r.dropClaimLocked(cm.Name, key)
	}
}

// dropClaimLocked removes one ConfigMap's claim and its published capacity.
// Caller holds mu.
func (r *RegistryResolver) dropClaimLocked(cmName string, key cacheKey) {
	if claims := r.byKey[key]; claims != nil {
		delete(claims, cmName)
		if len(claims) == 0 {
			delete(r.byKey, key)
		}
	}
	delete(r.cmKey, cmName)
	if graph, had := r.cmGraph[cmName]; had {
		if caps := r.byGraph[graph]; caps != nil {
			delete(caps, cmName)
			if len(caps) == 0 {
				delete(r.byGraph, graph)
			}
		}
		delete(r.cmGraph, cmName)
	}
}

// GraphCapacity returns the admission capacity phoebe enforces for graph: the
// tightest per-worker value per limit across the graph's registry entries
// that win their (org, served name) key, times the graph's ready workers plus
// GraphLimitHeadroomWorkers when a ReadyWorkers source is attached (Q22). A ConfigMap that loses a duplicate claim (see
// resolveClaimants) does not serve traffic, so its capacity values do not
// count either: a stale duplicate must not cap the graph for as long as it
// lives. Before the first sync, or for a graph with nothing published, it
// returns no limits: "no profile and no override" means no capacity limit
// (ruling 2026-10-12). Pure memory read.
func (r *RegistryResolver) GraphCapacity(graph string) GraphCapacity {
	r.mu.RLock()
	defer r.mu.RUnlock()
	caps := r.byGraph[graph]
	winners := make(map[string]GraphCapacity, len(caps))
	for cmName, c := range caps {
		if r.winsClaimLocked(cmName) {
			winners[cmName] = c
		}
	}
	capacity := tightest(winners)
	if r.workers == nil {
		return capacity
	}
	return capacity.Scaled(r.workers.Ready(graph) + GraphLimitHeadroomWorkers)
}

// winsClaimLocked reports whether cmName is the claimant resolveClaimants
// serves for its (org, served name) key: no other claimant on that key has a
// lower resource_id. Claimants that tie on the lowest resource_id all count
// as winners, so the answer never depends on map iteration order. Caller
// holds mu (read or write).
func (r *RegistryResolver) winsClaimLocked(cmName string) bool {
	key, ok := r.cmKey[cmName]
	if !ok {
		return false
	}
	claims := r.byKey[key]
	mine, ok := claims[cmName]
	if !ok {
		return false
	}
	for _, other := range claims {
		if other.ResourceID < mine.ResourceID {
			return false
		}
	}
	return true
}

func (r *RegistryResolver) size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byKey)
}

// parseRegistryConfigMap validates a registry ConfigMap against the contract
// above. org_id, served_model_name, resource_id, base_model, and
// graph_k8s_name are REQUIRED (a row missing any of them cannot be routed
// AND billed correctly — indexing it would serve unbillable or unroutable
// traffic); serving_mode is REQUIRED and must be exactly "shared" or
// "dedicated", because phoebe derives the SKU price row, the shared admission
// gate, and wake-from-zero eligibility (proxy.isWakeable) from it — a row
// phoebe cannot attribute to an SKU must not serve traffic, so the fail-closed
// choice is to reject the ConfigMap (upsert logs it and de-indexes the row)
// rather than to silently default. Only adapter may be empty (a base-model
// endpoint). An unparseable port is tolerated as 0 (the proxy falls back to
// the configured gateway.port) — wrong-but-recoverable, unlike the identity
// fields.
func parseRegistryConfigMap(cm *corev1.ConfigMap) (Resolution, cacheKey, error) {
	d := cm.Data
	for _, req := range []string{"org_id", "served_model_name", "resource_id", "base_model", "graph_k8s_name"} {
		if d[req] == "" {
			return Resolution{}, cacheKey{}, fmt.Errorf("missing required data key %q", req)
		}
	}
	if !identity.ValidServingMode(d["serving_mode"]) {
		return Resolution{}, cacheKey{}, fmt.Errorf("invalid serving_mode %q (must be %q or %q)",
			d["serving_mode"], identity.ServingModeShared, identity.ServingModeDedicated)
	}
	res := Resolution{
		ResourceID:   d["resource_id"],
		BaseModel:    d["base_model"],
		Adapter:      d["adapter"],
		Ready:        d["adapter"] == "" || d["ready"] == "true",
		ServingMode:  d["serving_mode"],
		GraphK8sName: d["graph_k8s_name"],
	}
	if p := d["port"]; p != "" {
		port, err := strconv.Atoi(p)
		if err != nil || port < 1 || port > 65535 {
			// Recoverable: route on the configured default port instead.
			res.Port = 0
		} else {
			res.Port = port
		}
	}
	return res, cacheKey{org: d["org_id"], model: d["served_model_name"]}, nil
}

// WaitSynced blocks until the initial sync completes or the timeout elapses,
// reporting whether it synced. For startup logging/tests only — Resolve does
// its own fail-closed check and never blocks.
func (r *RegistryResolver) WaitSynced(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r.synced.Load() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return r.synced.Load()
}
