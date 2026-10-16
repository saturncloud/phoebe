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
// graph's worker, in preference order (compared case-insensitively): the
// uniform Worker, then the pre-rename VllmWorker. The first one present
// decides the count, so VllmWorker is read only when the DGD has no Worker
// component; a stale VllmWorker status never raises a Worker reading of 0.
var sharedWorkerComponents = []string{"worker", "vllmworker"}

// atlasManagedSelector is the label selector phoebe lists DynamoGraphDeployments
// with: only DGDs Atlas manages (labelled saturncloud.io/managed-by=Atlas) are
// read. This label is part of the registry contract (see registry.go).
const atlasManagedSelector = "saturncloud.io/managed-by=Atlas"

// ReadyWorkers is an informer over the Atlas-managed DynamoGraphDeployments
// in the workload namespace, keeping each graph's ready worker count
// (status.components.Worker.readyReplicas) in memory. Every scaler (KEDA,
// the Dynamo Planner, phoebe's waker, Atlas's reaper) moves that number, so
// reading it here needs no hook in any of them. Lookups are pure memory
// reads. Before the first sync every graph reads zero, even one the initial
// list has already delivered, and after it a graph it has not seen reads
// zero: the graph then admits one worker's worth, never no limit.
//
// After the first sync the counts are trusted while phoebe can still read
// the DGDs (a 15 s health probe); after 60 s with no successful read (API
// server down, or the grant revoked) every graph reads 0, one worker's worth,
// until a probe succeeds again (Hugo's Q23 condition, D58: losing the read
// fails tight).
type ReadyWorkers struct {
	log      *logging.Logger
	informer cache.SharedIndexInformer
	synced   atomic.Bool

	// The health probe (Q23): a one-item list of the same DGDs every
	// probeEvery. lastReachable is when the API server last answered one
	// (nil until the first sync). It is kept as a time.Time from now, so
	// with time.Now the comparison uses the monotonic clock and a wall-clock
	// step (NTP, VM resume) cannot shift the window. When it is older than
	// staleAfter every graph reads 0 ready workers (one worker's worth), and
	// the next successful probe restores the counts. Health comes from this
	// list, not from watch errors, so a watch that reconnects without
	// delivering events is never mistaken for a failing one.
	client        dynamic.Interface
	namespace     string
	probeEvery    time.Duration
	staleAfter    time.Duration
	now           func() time.Time
	lastReachable atomic.Pointer[time.Time]
	unreachable   atomic.Bool

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
		log:        log,
		informer:   factory.ForResource(dynamoGraphResource).Informer(),
		ready:      map[string]int64{},
		client:     client,
		namespace:  namespace,
		probeEvery: readyWorkersProbeEvery,
		staleAfter: readyWorkersStaleAfter,
		now:        time.Now,
	}
	_ = w.informer.SetWatchErrorHandler(func(_ *cache.Reflector, err error) { w.watchFailed(err) })
	_, _ = w.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { w.upsert(obj) },
		UpdateFunc: func(_, obj any) { w.upsert(obj) },
		DeleteFunc: func(obj any) { w.remove(obj) },
	})
	return w
}

// readyWorkersProbeEvery and readyWorkersStaleAfter: phoebe checks that it
// can still read the DGDs every 15 s, and after 60 s without a successful read
// every graph falls back to one worker's worth (Hugo's Q23 condition: losing
// the read must fail tight).
const (
	readyWorkersProbeEvery = 15 * time.Second
	readyWorkersStaleAfter = 60 * time.Second
)

// Run starts the informer and the health probe; it returns when ctx is
// cancelled.
func (w *ReadyWorkers) Run(ctx context.Context) {
	go func() {
		if cache.WaitForCacheSync(ctx.Done(), w.informer.HasSynced) {
			w.markReachable()
			w.synced.Store(true)
			w.log.Info.Printf("gateway: ready-worker counts synced")
		}
	}()
	go func() {
		t := time.NewTicker(w.probeEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				w.probe(ctx)
			}
		}
	}()
	w.informer.Run(ctx.Done())
}

// probe lists at most one Atlas-managed DGD and records whether the API
// server answered.
func (w *ReadyWorkers) probe(ctx context.Context) {
	pctx, cancel := context.WithTimeout(ctx, w.probeEvery)
	defer cancel()
	_, err := w.client.Resource(dynamoGraphResource).Namespace(w.namespace).List(pctx,
		metav1.ListOptions{LabelSelector: atlasManagedSelector, Limit: 1})
	if err != nil {
		if w.synced.Load() && !w.unreachable.Load() && w.stale() {
			w.unreachable.Store(true)
			w.log.Error.Printf("gateway: ready-worker counts not confirmed for over %s (%v); every graph admits one worker's worth until phoebe can read the DynamoGraphDeployments again", w.staleAfter, err)
		}
		return
	}
	w.markReachable()
	if w.unreachable.Swap(false) {
		w.log.Info.Printf("gateway: ready-worker counts confirmed again; graph admission limits follow ready workers")
	}
}

// markReachable records that the API server answered a read of the DGDs now.
func (w *ReadyWorkers) markReachable() {
	t := w.now()
	w.lastReachable.Store(&t)
}

// stale reports whether the last successful read is older than staleAfter.
// Before the first sync there is no last read, so nothing is stale.
func (w *ReadyWorkers) stale() bool {
	last := w.lastReachable.Load()
	return last != nil && w.now().Sub(*last) > w.staleAfter
}

// watchFailed records that the DGD list or watch failed and logs what that
// does to the graph admission limits.
func (w *ReadyWorkers) watchFailed(err error) {
	if !w.synced.Load() {
		w.log.Error.Printf("gateway: ready-worker list failed before first sync (%v); every graph reads 0 ready workers and admits one worker's worth until the list succeeds (check phoebe RBAC on dynamographdeployments.nvidia.com/v1beta1 and that the CRD exists)", err)
		return
	}
	w.log.Error.Printf("gateway: ready-worker watch failed (%v); graph admission limits use the last-known worker counts while phoebe can still read the DynamoGraphDeployments, and one worker's worth after %s without a successful read", err, w.staleAfter)
}

// Synced reports whether the informer has completed its first list. It is
// for tests and diagnostics: admission does not wait for it, because an
// unsynced count reads as 0, one worker's worth (D54).
func (w *ReadyWorkers) Synced() bool { return w.synced.Load() }

// Ready is graph's ready worker count: zero before the first sync, zero when
// unknown, and zero for every graph once no read of the DGDs has succeeded
// for staleAfter.
func (w *ReadyWorkers) Ready(graph string) int64 {
	if !w.synced.Load() || w.stale() {
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
	w.mu.Lock()
	delete(w.ready, u.GetName())
	w.mu.Unlock()
}

// readyWorkersOf reads a DGD's ready worker count. A missing or malformed
// status reads as zero. Worker is preferred; VllmWorker is read only when the
// DGD has no Worker component. A Worker component whose readyReplicas is 0,
// missing or malformed reads as zero rather than falling back, so a stale
// VllmWorker status cannot raise the limit while the real workers are down.
// A component entry that is not a map is treated as absent. Names match
// case-insensitively; when several entries differ only by case (for example
// "Worker" and "worker"), the smallest ready count among them wins, so the
// result never depends on map iteration order and fails tight.
func readyWorkersOf(obj map[string]any) int64 {
	components, _, _ := unstructured.NestedMap(obj, "status", "components")
	for _, want := range sharedWorkerComponents {
		found := false
		var minReady int64
		for name, raw := range components {
			if strings.ToLower(name) != want {
				continue
			}
			c, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			var n int64
			switch v := c["readyReplicas"].(type) {
			case int64:
				if v > 0 {
					n = v
				}
			case float64:
				if v > 0 && v < math.MaxInt64 {
					n = int64(v)
				}
			}
			if !found || n < minReady {
				minReady = n
			}
			found = true
		}
		if found {
			return minReady
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
