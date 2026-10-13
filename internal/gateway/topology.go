package gateway

// Graph topology entries (build item B24; owner ruling 2026-10-12: usage
// events record the serving topology by revision).
//
// Atlas publishes one ConfigMap per running Token Factory graph, shared or
// dedicated, carrying the graph's topology revision (Atlas's
// tf_topology_revision.revision: each pool's role, instance type, GPU type
// and GPUs per replica) and its cluster's region. phoebe looks the entry up
// by the graph_k8s_name it already resolved for the request and stamps both
// on the usage event, so the values are platform-set, never echoed by the
// engine.
//
// CROSS-REPO CONTRACT (Atlas writes these: pdc/tf_metering.py; change only in
// lockstep):
//   - kind ConfigMap in the workload namespace (gateway.namespace), name
//     tf-graph-topology-<graph_k8s_name>.
//   - label saturncloud.io/tf-graph-topology: "true" (this informer's
//     selector).
//   - data keys (strings): graph_k8s_name (the lookup key; the ConfigMap
//     name must be tf-graph-topology-<graph_k8s_name>, so the namespace's
//     name uniqueness gives at most one entry per graph),
//     topology_revision (decimal, 1..2147483647), region (may be "", at most
//     64 characters).
//
// The fields are EVIDENCE for margin analysis, not billing inputs: a missing
// or malformed entry only leaves them NULL on the event. An entry whose name
// does not match its graph, or whose revision is out of range, is ignored
// whole. A region that is not valid UTF-8, contains a NUL byte, or is longer than 64 characters is
// dropped (logged) and the revision is kept. Startup waits up to 10s for the
// first sync so the first requests after a pod start carry their revision,
// but serving never fails or waits beyond that on this index, unlike the
// served-model registry, which fails closed before its first sync.

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
)

const (
	topologyLabelSelector = "saturncloud.io/tf-graph-topology=true"
	// topologyConfigMapPrefix + graph_k8s_name is the required ConfigMap name
	// (Atlas: pdc/tf_metering.py topology_configmap_name).
	topologyConfigMapPrefix = "tf-graph-topology-"
)

// Topology is what Atlas published for one graph.
type Topology struct {
	Revision int
	Region   string
}

// TopologyIndex is an in-memory index of the graph topology entries, kept
// fresh by a label-selected ConfigMap informer. Lookups are memory reads.
type TopologyIndex struct {
	log      *logging.Logger
	informer cache.SharedIndexInformer
	// reg is the event-handler registration. Its HasSynced is true only once
	// the handler has processed the initial list, which the informer's own
	// HasSynced does not promise. nil if registration failed.
	reg cache.ResourceEventHandlerRegistration

	mu sync.RWMutex
	// byGraph maps graph_k8s_name to its topology. byCM remembers which graph
	// each ConfigMap last indexed, so an update that renames the graph or
	// breaks the entry drops the old key.
	byGraph map[string]Topology
	byCM    map[string]string
}

// NewTopologyIndex builds the index and its informer over client in namespace.
// Call Run to start watching.
func NewTopologyIndex(client kubernetes.Interface, namespace string, log *logging.Logger) *TopologyIndex {
	factory := informers.NewSharedInformerFactoryWithOptions(
		client,
		0,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.LabelSelector = topologyLabelSelector
		}),
	)
	t := &TopologyIndex{
		log:      log,
		informer: factory.Core().V1().ConfigMaps().Informer(),
		byGraph:  map[string]Topology{},
		byCM:     map[string]string{},
	}
	_ = t.informer.SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
		t.log.Warn.Printf("gateway: graph topology watch failed (%v); usage events keep the last-known topology", err)
	})
	reg, err := t.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { t.upsert(obj) },
		UpdateFunc: func(_, obj any) { t.upsert(obj) },
		DeleteFunc: func(obj any) { t.remove(obj) },
	})
	if err != nil {
		t.log.Warn.Printf("gateway: registering the graph topology handler failed (%v); usage events will carry no topology", err)
	} else {
		t.reg = reg
	}
	return t
}

// Run starts the informer; it returns when ctx is cancelled.
func (t *TopologyIndex) Run(ctx context.Context) {
	t.informer.Run(ctx.Done())
}

// WaitSynced blocks until the event handler has indexed the informer's
// initial list or ctx is done, and reports whether it synced. It waits on the
// handler registration, not the informer: the informer reports synced once
// its store is filled, possibly before the handler has run upsert for every
// entry.
func (t *TopologyIndex) WaitSynced(ctx context.Context) bool {
	if t.reg == nil {
		return false
	}
	return cache.WaitForCacheSync(ctx.Done(), t.reg.HasSynced)
}

// Lookup returns the topology Atlas published for graph, if any.
func (t *TopologyIndex) Lookup(graph string) (Topology, bool) {
	if t == nil || graph == "" {
		return Topology{}, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	topo, ok := t.byGraph[graph]
	return topo, ok
}

func (t *TopologyIndex) upsert(obj any) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return
	}
	graph, topo, regionDropped, err := parseTopologyConfigMap(cm)
	t.mu.Lock()
	defer t.mu.Unlock()
	if old, had := t.byCM[cm.Name]; had {
		delete(t.byGraph, old)
		delete(t.byCM, cm.Name)
	}
	if err != nil {
		t.log.Warn.Printf("gateway: ignoring graph topology ConfigMap %s/%s: %v", cm.Namespace, cm.Name, err)
		return
	}
	if regionDropped {
		t.log.Warn.Printf("gateway: graph topology ConfigMap %s/%s: region is not valid UTF-8, contains a NUL byte, or is longer than %d characters; keeping the revision and recording no region", cm.Namespace, cm.Name, metering.MaxRegionChars)
	}
	t.byGraph[graph] = topo
	t.byCM[cm.Name] = graph
}

func (t *TopologyIndex) remove(obj any) {
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
	t.mu.Lock()
	defer t.mu.Unlock()
	if graph, had := t.byCM[cm.Name]; had {
		delete(t.byGraph, graph)
		delete(t.byCM, cm.Name)
	}
}

// parseTopologyConfigMap validates an entry against the contract above. It
// returns an error when the entry must be ignored whole; regionDropped
// reports a bad region that was cleared while the revision was kept.
func parseTopologyConfigMap(cm *corev1.ConfigMap) (graph string, topo Topology, regionDropped bool, err error) {
	d := cm.Data
	graph = d["graph_k8s_name"]
	if graph == "" {
		return "", Topology{}, false, fmt.Errorf("missing required data key %q", "graph_k8s_name")
	}
	// One ConfigMap per graph: the name is derived from the graph, so two
	// ConfigMaps can never index (and on delete, un-index) the same graph.
	if cm.Name != topologyConfigMapPrefix+graph {
		return "", Topology{}, false, fmt.Errorf("name %q does not match graph_k8s_name %q (want %s<graph>)", cm.Name, graph, topologyConfigMapPrefix)
	}
	// Bounded to int32: billing_event.topology_revision and
	// rated_usage.topology_revision are Postgres INTEGER, and a larger value
	// would make every event for this graph drain as poison (dropped usage).
	r64, perr := strconv.ParseInt(d["topology_revision"], 10, 32)
	if perr != nil || r64 < 1 || r64 > math.MaxInt32 {
		return "", Topology{}, false, fmt.Errorf("invalid topology_revision %q", d["topology_revision"])
	}
	region := d["region"]
	// NUL is rejected because PostgreSQL text cannot store it: the drainer's
	// insert would fail and drop the batch.
	if !utf8.ValidString(region) || strings.ContainsRune(region, 0) || utf8.RuneCountInString(region) > metering.MaxRegionChars {
		region = ""
		regionDropped = true
	}
	return graph, Topology{Revision: int(r64), Region: region}, regionDropped, nil
}
