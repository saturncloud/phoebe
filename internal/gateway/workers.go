package gateway

import (
	"context"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	"github.com/saturncloud/phoebe/internal/logging"
)

// GraphLimitHeadroomWorkers is the workers' worth of headroom a shared
// graph's admission limit allows above its ready workers (Q4(b)): the
// published per-worker limit times (ready workers + this). The extra worker
// lets load build past the running workers, which is what the autoscaler
// scales out on, and a graph at zero workers still admits one worker's worth
// for requests held through a wake.
const GraphLimitHeadroomWorkers = 1

// dynamoGraphResource is the DynamoGraphDeployment API phoebe reads ready
// workers from (read-only: get, list, watch).
var dynamoGraphResource = schema.GroupVersionResource{
	Group: "nvidia.com", Version: "v1beta1", Resource: "dynamographdeployments",
}

// sharedWorkerComponents are the DGD status component names of a shared
// graph's worker, in preference order: the uniform Worker, then the
// pre-rename VllmWorker (compared case-insensitively).
var sharedWorkerComponents = []string{"worker", "vllmworker"}

// atlasManagedSelector is the label selector phoebe lists DynamoGraphDeployments
// with: only DGDs Atlas manages (labelled saturncloud.io/managed-by=Atlas) are
// read. This label is part of the registry contract (see registry.go).
const atlasManagedSelector = "saturncloud.io/managed-by=Atlas"

// readyWorkersStaleAfter bounds how long a last-known ready worker count is
// trusted while the DGD watch keeps failing. After it, every graph reads 0
// ready workers (one worker's worth, per D54) until the watch recovers.
const readyWorkersStaleAfter = 60 * time.Second

// ReadyWorkers is an informer over the Atlas-managed DynamoGraphDeployments
// in the workload namespace, keeping each graph's ready worker count
// (status.components.Worker.readyReplicas) in memory. Every scaler (KEDA,
// the Dynamo Planner, phoebe's waker, Atlas's reaper) moves that number, so
// reading it here needs no hook in any of them. Lookups are pure memory
// reads. Before the first sync, and for a graph it has not seen, the count
// is zero: the graph then admits one worker's worth, never no limit. While
// the watch is failing, a last-known count is trusted for at most
// readyWorkersStaleAfter; after that every count reads as zero (one worker's
// worth, per D54) until an event from a recovered watch arrives.
type ReadyWorkers struct {
	log      *logging.Logger
	informer cache.SharedIndexInformer
	synced   atomic.Bool
	// failingSince is when the watch started failing, in unix nanoseconds;
	// zero while the watch is healthy.
	failingSince atomic.Int64
	now          func() time.Time

	mu    sync.RWMutex
	ready map[string]int64
}

// NewReadyWorkers builds the informer over client in namespace. Call Run.
func NewReadyWorkers(client dynamic.Interface, namespace string, log *logging.Logger) *ReadyWorkers {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		client, 0, namespace,
		func(o *metav1.ListOptions) { o.LabelSelector = atlasManagedSelector },
	)
	w := &ReadyWorkers{
		log:      log,
		informer: factory.ForResource(dynamoGraphResource).Informer(),
		ready:    map[string]int64{},
		now:      time.Now,
	}
	_ = w.informer.SetWatchErrorHandler(func(_ *cache.Reflector, err error) { w.watchFailed(err) })
	_, _ = w.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { w.upsert(obj) },
		UpdateFunc: func(_, obj any) { w.upsert(obj) },
		DeleteFunc: func(obj any) { w.remove(obj) },
	})
	return w
}

// Run starts the informer; it returns when ctx is cancelled.
func (w *ReadyWorkers) Run(ctx context.Context) {
	go func() {
		if cache.WaitForCacheSync(ctx.Done(), w.informer.HasSynced) {
			w.synced.Store(true)
			w.log.Info.Printf("gateway: ready-worker counts synced")
		}
	}()
	w.informer.Run(ctx.Done())
}

// watchFailed records that the DGD list or watch failed and logs what that
// does to the graph admission limits.
func (w *ReadyWorkers) watchFailed(err error) {
	w.failingSince.CompareAndSwap(0, w.now().UnixNano())
	if !w.synced.Load() {
		w.log.Error.Printf("gateway: ready-worker list failed before first sync (%v); every graph reads 0 ready workers and admits one worker's worth until the list succeeds (check phoebe RBAC on dynamographdeployments.nvidia.com/v1beta1 and that the CRD exists)", err)
		return
	}
	w.log.Error.Printf("gateway: ready-worker watch failed (%v); graph admission limits use the last-known worker counts for up to %s, then every graph reads 0 ready workers (one worker's worth)", err, readyWorkersStaleAfter)
}

// Synced reports whether the informer has completed its first list.
func (w *ReadyWorkers) Synced() bool { return w.synced.Load() }

// Ready is graph's ready worker count: zero when unknown, and zero for every
// graph once the watch has been failing for longer than
// readyWorkersStaleAfter.
func (w *ReadyWorkers) Ready(graph string) int64 {
	if f := w.failingSince.Load(); f != 0 && w.now().Sub(time.Unix(0, f)) > readyWorkersStaleAfter {
		return 0
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.ready[graph]
}

func (w *ReadyWorkers) upsert(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	n := readyWorkersOf(u.Object)
	w.failingSince.Store(0)
	w.mu.Lock()
	w.ready[u.GetName()] = n
	w.mu.Unlock()
}

func (w *ReadyWorkers) remove(obj any) {
	if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	w.failingSince.Store(0)
	w.mu.Lock()
	delete(w.ready, u.GetName())
	w.mu.Unlock()
}

// readyWorkersOf reads a DGD's ready worker count. A missing or malformed
// status reads as zero. Worker is preferred; VllmWorker is read when Worker
// has none ready.
func readyWorkersOf(obj map[string]any) int64 {
	components, _, _ := unstructured.NestedMap(obj, "status", "components")
	for _, want := range sharedWorkerComponents {
		for name, raw := range components {
			if strings.ToLower(name) != want {
				continue
			}
			c, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			switch v := c["readyReplicas"].(type) {
			case int64:
				if v > 0 {
					return v
				}
			case float64:
				if v > 0 && v < math.MaxInt64 {
					return int64(v)
				}
			}
		}
	}
	return 0
}

// Scaled is the capacity times workers: every set limit multiplied,
// saturating at the int64 maximum; no limit stays no limit and a zero cap
// stays zero. A workers value below 1 is treated as 1 (values unchanged).
func (c GraphCapacity) Scaled(workers int64) GraphCapacity {
	var out GraphCapacity
	for _, f := range capacityFields {
		v := *f.get(&c)
		if v == nil {
			continue
		}
		x := *v
		if workers > 0 && x > 0 {
			if x > math.MaxInt64/workers {
				x = math.MaxInt64
			} else {
				x *= workers
			}
		}
		*f.get(&out) = &x
	}
	return out
}

// readyWorkerSource is what the registry needs from ReadyWorkers.
type readyWorkerSource interface {
	Ready(graph string) int64
}
