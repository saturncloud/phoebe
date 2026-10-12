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
//   - data keys (strings): graph_k8s_name (the lookup key),
//     topology_revision (decimal, >= 1), region (may be "").
//
// The fields are EVIDENCE for margin analysis, not billing inputs: a missing
// or malformed entry only leaves them NULL on the event. Nothing fails or
// waits on this index, unlike the served-model registry, which fails closed
// before its first sync.

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/saturncloud/phoebe/internal/logging"
)

const topologyLabelSelector = "saturncloud.io/tf-graph-topology=true"

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
	_, _ = t.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { t.upsert(obj) },
		UpdateFunc: func(_, obj any) { t.upsert(obj) },
		DeleteFunc: func(obj any) { t.remove(obj) },
	})
	return t
}

// Run starts the informer; it returns when ctx is cancelled.
func (t *TopologyIndex) Run(ctx context.Context) {
	t.informer.Run(ctx.Done())
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
	graph, topo, err := parseTopologyConfigMap(cm)
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

// parseTopologyConfigMap validates an entry against the contract above.
func parseTopologyConfigMap(cm *corev1.ConfigMap) (string, Topology, error) {
	d := cm.Data
	graph := d["graph_k8s_name"]
	if graph == "" {
		return "", Topology{}, fmt.Errorf("missing required data key %q", "graph_k8s_name")
	}
	revision, err := strconv.Atoi(d["topology_revision"])
	if err != nil || revision < 1 {
		return "", Topology{}, fmt.Errorf("invalid topology_revision %q", d["topology_revision"])
	}
	region := d["region"]
	if len(region) > 64 {
		return "", Topology{}, fmt.Errorf("region longer than 64 bytes")
	}
	return graph, Topology{Revision: revision, Region: region}, nil
}
