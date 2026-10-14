package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/saturncloud/phoebe/internal/logging"
)

func dgd(name string, components map[string]any) *unstructured.Unstructured {
	obj := map[string]any{
		"apiVersion": "nvidia.com/v1beta1",
		"kind":       "DynamoGraphDeployment",
		"metadata": map[string]any{
			"name":      name,
			"namespace": registryNS,
			"labels":    map[string]any{"saturncloud.io/managed-by": "Atlas"},
		},
	}
	if components != nil {
		obj["status"] = map[string]any{"components": components}
	}
	return &unstructured.Unstructured{Object: obj}
}

func TestReadyWorkersOf(t *testing.T) {
	cases := []struct {
		name       string
		components map[string]any
		want       int64
	}{
		{"worker", map[string]any{"Frontend": map[string]any{"readyReplicas": int64(1)}, "Worker": map[string]any{"readyReplicas": int64(3)}}, 3},
		{"legacy VllmWorker", map[string]any{"VllmWorker": map[string]any{"readyReplicas": int64(2)}}, 2},
		{"Worker present at 0 ignores a stale VllmWorker", map[string]any{"Worker": map[string]any{"readyReplicas": int64(0)}, "VllmWorker": map[string]any{"readyReplicas": int64(3)}}, 0},
		{"Worker with no readyReplicas ignores a stale VllmWorker", map[string]any{"Worker": map[string]any{}, "VllmWorker": map[string]any{"readyReplicas": int64(3)}}, 0},
		{"Worker with a malformed count ignores a stale VllmWorker", map[string]any{"Worker": map[string]any{"readyReplicas": "3"}, "VllmWorker": map[string]any{"readyReplicas": int64(3)}}, 0},
		{"VllmWorker read only when Worker is absent", map[string]any{"Frontend": map[string]any{"readyReplicas": int64(1)}, "VllmWorker": map[string]any{"readyReplicas": int64(2)}}, 2},
		{"a Worker entry that is not a map is treated as absent", map[string]any{"Worker": "x", "VllmWorker": map[string]any{"readyReplicas": int64(2)}}, 2},
		{"float count", map[string]any{"Worker": map[string]any{"readyReplicas": float64(4)}}, 4},
		{"desired replicas are not ready workers", map[string]any{"Worker": map[string]any{"replicas": int64(4)}}, 0},
		{"a string count reads as zero", map[string]any{"Worker": map[string]any{"readyReplicas": "3"}}, 0},
		{"malformed component", map[string]any{"Worker": "x"}, 0},
		{"no status", nil, 0},
		{"case-insensitive component names: lowercase worker at 0 ignores VLLMWorker", map[string]any{"worker": map[string]any{"readyReplicas": int64(0)}, "VLLMWorker": map[string]any{"readyReplicas": int64(5)}}, 0},
		{"case-insensitive VLLMWorker read when Worker is absent", map[string]any{"VLLMWorker": map[string]any{"readyReplicas": int64(5)}}, 5},
		{"lowercase worker", map[string]any{"worker": map[string]any{"readyReplicas": int64(3)}}, 3},
		{"uppercase WORKER", map[string]any{"WORKER": map[string]any{"readyReplicas": int64(3)}}, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := readyWorkersOf(dgd("g", tc.components).Object); got != tc.want {
				t.Fatalf("readyWorkersOf = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestGraphCapacityScaled(t *testing.T) {
	ptr := func(v int64) *int64 { return &v }
	c := GraphCapacity{MaxActiveRequests: ptr(16), MaxReservedKVTokens: ptr(0)}
	got := c.Scaled(3)
	if *got.MaxActiveRequests != 48 || *got.MaxReservedKVTokens != 0 || got.MaxPrefillTokens != nil {
		t.Fatalf("Scaled(3) = %+v", got)
	}
	if *c.MaxActiveRequests != 16 {
		t.Fatal("Scaled mutated its receiver")
	}
	for _, workers := range []int64{0, -1} {
		got := c.Scaled(workers)
		if *got.MaxActiveRequests != 16 || *got.MaxReservedKVTokens != 0 || got.MaxPrefillTokens != nil {
			t.Fatalf("Scaled(%d) must treat workers < 1 as 1 and leave values unchanged: %+v", workers, got)
		}
	}
	big := GraphCapacity{MaxPrefillTokens: ptr(math.MaxInt64/2 + 1)}
	if *big.Scaled(2).MaxPrefillTokens != math.MaxInt64 {
		t.Fatal("Scaled must saturate at the int64 maximum")
	}
}

type fixedWorkers map[string]int64

func (f fixedWorkers) Ready(graph string) int64 { return f[graph] }

// The registry multiplies the per-worker limit Atlas publishes by the graph's
// ready workers plus one, as reported by a stub worker source (a graph the
// stub does not know reads 0 workers). The real-informer fail-closed case is
// TestRegistryGraphCapacityFailsClosedWhenWorkersUnreadable.
func TestRegistryGraphCapacityScalesStubReadyWorkersPlusOne(t *testing.T) {
	r, _, _ := startedRegistry(t, registryCM("tf-model-a", map[string]string{capacityKeyUnit: capacityUnitPerWorker,
		"admission_max_active_requests": "16",
	}))
	deadline := time.Now().Add(5 * time.Second)
	for !r.GraphCapacity("graph-llama31").Any() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := *r.GraphCapacity("graph-llama31").MaxActiveRequests; got != 16 {
		t.Fatalf("without a worker source the published value is used as is: %d", got)
	}
	r.WithReadyWorkers(fixedWorkers{"graph-llama31": 2})
	if got := *r.GraphCapacity("graph-llama31").MaxActiveRequests; got != 48 {
		t.Fatalf("16 per worker x (2 ready + 1) = 48, got %d", got)
	}
	r.WithReadyWorkers(fixedWorkers{})
	if got := *r.GraphCapacity("graph-llama31").MaxActiveRequests; got != 16 {
		t.Fatalf("a graph with no known workers admits one worker's worth, got %d", got)
	}
}

// Scaling by ready workers keeps the registry's capacity rules: an explicit
// zero cap stays zero (R-K), an absent key stays no limit, a negative ready
// count is treated as zero, and a huge ready count saturates the limit at
// the int64 maximum instead of wrapping to an unscaled value. The rule that
// only the winning duplicate claimant counts is covered in
// TestRegistry_GraphCapacityOnlyFromWinningClaimant.
func TestRegistryGraphCapacityScaledKeepsRegistryInvariants(t *testing.T) {
	cases := []struct {
		name    string
		cms     []*corev1.ConfigMap
		workers int64
		check   func(t *testing.T, c GraphCapacity)
	}{
		{
			name: "explicit zero cap stays zero (R-K)",
			cms: []*corev1.ConfigMap{registryCM("tf-model-a", map[string]string{capacityKeyUnit: capacityUnitPerWorker,
				"admission_max_active_requests": "0",
			})},
			workers: 5,
			check: func(t *testing.T, c GraphCapacity) {
				if c.MaxActiveRequests == nil || *c.MaxActiveRequests != 0 {
					t.Fatalf("MaxActiveRequests = %v, want an explicit 0", c.MaxActiveRequests)
				}
			},
		},
		{
			name: "absent key stays no limit",
			cms: []*corev1.ConfigMap{registryCM("tf-model-a", map[string]string{capacityKeyUnit: capacityUnitPerWorker,
				"admission_max_prefill_tokens": "4096",
			})},
			workers: 5,
			check: func(t *testing.T, c GraphCapacity) {
				if c.MaxActiveRequests != nil {
					t.Fatalf("MaxActiveRequests = %d, want nil (no limit)", *c.MaxActiveRequests)
				}
				if c.MaxReservedKVTokens != nil {
					t.Fatalf("MaxReservedKVTokens = %d, want nil", *c.MaxReservedKVTokens)
				}
				if val(c.MaxPrefillTokens) != 4096*6 {
					t.Fatalf("MaxPrefillTokens = %d, want 4096 x 6", val(c.MaxPrefillTokens))
				}
			},
		},
		{
			name: "negative ready count is treated as zero",
			cms: []*corev1.ConfigMap{registryCM("tf-model-a", map[string]string{capacityKeyUnit: capacityUnitPerWorker,
				"admission_max_active_requests": "10",
			})},
			workers: -5,
			check: func(t *testing.T, c GraphCapacity) {
				if val(c.MaxActiveRequests) != 10*GraphLimitHeadroomWorkers {
					t.Fatalf("MaxActiveRequests = %d, want one worker's worth (10 x %d)", val(c.MaxActiveRequests), GraphLimitHeadroomWorkers)
				}
			},
		},
		{
			name: "a huge ready count saturates instead of wrapping",
			cms: []*corev1.ConfigMap{registryCM("tf-model-a", map[string]string{capacityKeyUnit: capacityUnitPerWorker,
				"admission_max_active_requests": "10", "admission_max_reserved_kv_tokens": "0",
			})},
			workers: math.MaxInt64,
			check: func(t *testing.T, c GraphCapacity) {
				if val(c.MaxActiveRequests) != math.MaxInt64 {
					t.Fatalf("MaxActiveRequests = %d, want the int64 maximum", val(c.MaxActiveRequests))
				}
				if c.MaxReservedKVTokens == nil || *c.MaxReservedKVTokens != 0 {
					t.Fatalf("MaxReservedKVTokens = %v, want an explicit 0", c.MaxReservedKVTokens)
				}
				if c.MaxPrefillTokens != nil {
					t.Fatal("an unset limit must stay unset")
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, _, _ := startedRegistry(t, tc.cms...)
			r.WithReadyWorkers(fixedWorkers{"graph-llama31": tc.workers})
			tc.check(t, waitCapacity(t, r, "graph-llama31", func(c GraphCapacity) bool { return c.Any() }))
		})
	}
}

// forbiddenDGDClient is a dynamic client whose DGD list is forbidden, as
// with missing RBAC.
func forbiddenDGDClient() *dynamicfake.FakeDynamicClient {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{dynamoGraphResource: "DynamoGraphDeploymentList"})
	client.PrependReactor("list", "dynamographdeployments", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(dynamoGraphResource.GroupResource(), "", nil)
	})
	return client
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// With a forbidden DGD list the worker source never syncs and reads 0, and a
// registry with this real source attached admits exactly one worker's worth:
// the published per-worker value, never no limit (D54 fails closed).
func TestRegistryGraphCapacityFailsClosedWhenWorkersUnreadable(t *testing.T) {
	w := NewReadyWorkers(forbiddenDGDClient(), registryNS, logging.New(logging.ERROR))
	w.log.Error.SetOutput(io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	r, _, _ := startedRegistry(t, registryCM("tf-model-a", map[string]string{
		"admission_max_active_requests": "16", capacityKeyUnit: capacityUnitPerWorker,
	}))
	r.WithReadyWorkers(w)
	c := waitCapacity(t, r, "graph-llama31", func(c GraphCapacity) bool { return c.Any() })
	if c.MaxActiveRequests == nil || *c.MaxActiveRequests != 16*GraphLimitHeadroomWorkers {
		t.Fatalf("MaxActiveRequests = %v, want the per-worker 16 x (0 + 1)", c.MaxActiveRequests)
	}
	if w.Ready("graph-llama31") != 0 {
		t.Fatalf("Ready = %d, want 0 with the list forbidden", w.Ready("graph-llama31"))
	}
	if w.Synced() {
		t.Fatal("Synced() = true with the list forbidden")
	}
}

// A registry with a real ReadyWorkers attached drops a per-worker graph's
// limit to one worker's worth once the DGDs have not been readable for
// staleAfter (D58), and restores it when a probe succeeds again.
func TestRegistryGraphCapacityFailsTightWhenWorkerReadGoesStale(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{dynamoGraphResource: "DynamoGraphDeploymentList"})
	w := NewReadyWorkers(client, registryNS, logging.New(logging.ERROR))
	w.log.Error.SetOutput(io.Discard)
	w.log.Info.SetOutput(io.Discard)
	now := time.Unix(1_000_000, 0)
	w.now = func() time.Time { return now }
	w.markReachable()
	w.synced.Store(true)
	w.upsert(dgd("graph-llama31", map[string]any{"Worker": map[string]any{"readyReplicas": int64(3)}}))

	r, _, _ := startedRegistry(t, registryCM("tf-model-a", map[string]string{
		"admission_max_active_requests": "16", capacityKeyUnit: capacityUnitPerWorker,
	}))
	r.WithReadyWorkers(w)
	c := waitCapacity(t, r, "graph-llama31", func(c GraphCapacity) bool { return c.Any() })
	if val(c.MaxActiveRequests) != 16*(3+GraphLimitHeadroomWorkers) {
		t.Fatalf("readable: MaxActiveRequests = %d, want 16 x (3 + 1)", val(c.MaxActiveRequests))
	}

	failing := true
	client.PrependReactor("list", "dynamographdeployments", func(clienttesting.Action) (bool, runtime.Object, error) {
		if failing {
			return true, nil, errors.New("forbidden")
		}
		return false, nil, nil
	})
	now = now.Add(w.staleAfter + time.Second)
	w.probe(context.Background())
	if got := val(r.GraphCapacity("graph-llama31").MaxActiveRequests); got != 16*GraphLimitHeadroomWorkers {
		t.Fatalf("stale: MaxActiveRequests = %d, want one worker's worth 16 x (0 + 1)", got)
	}

	failing = false
	w.probe(context.Background())
	if got := val(r.GraphCapacity("graph-llama31").MaxActiveRequests); got != 16*(3+GraphLimitHeadroomWorkers) {
		t.Fatalf("readable again: MaxActiveRequests = %d, want 16 x (3 + 1)", got)
	}
}

// A list that fails before the first sync is logged as such (not as "using
// the last-known counts", of which there are none), and every graph reads 0.
func TestReadyWorkersUnsyncedListFailureReadsZero(t *testing.T) {
	w := NewReadyWorkers(forbiddenDGDClient(), registryNS, logging.New(logging.ERROR))
	logs := &syncBuffer{}
	w.log.Error.SetOutput(logs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), "before first sync") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.Contains(logs.String(), "before first sync") {
		t.Fatalf("log = %q, want the before-first-sync message", logs.String())
	}
	if strings.Contains(logs.String(), "last-known") {
		t.Fatalf("log = %q claims last-known counts before any sync", logs.String())
	}
	if w.Synced() {
		t.Fatal("Synced() = true with the list forbidden")
	}
	if w.Ready("graph-a") != 0 {
		t.Fatalf("Ready = %d, want 0", w.Ready("graph-a"))
	}
}

// Q23: after staleAfter without a successful read of the DGDs (API server
// down, or the grant revoked) every graph reads 0 ready workers, so it admits
// one worker's worth; the next successful probe restores the last-known
// counts. Health comes from the probe's list, so watch errors alone change
// nothing.
func TestReadyWorkersFailTightWhenTheDGDsCannotBeRead(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{dynamoGraphResource: "DynamoGraphDeploymentList"})
	w := NewReadyWorkers(client, registryNS, logging.New(logging.ERROR))
	w.log.Error.SetOutput(&syncBuffer{})
	w.log.Info.SetOutput(&syncBuffer{})
	now := time.Unix(1_000_000, 0)
	w.now = func() time.Time { return now }
	w.markReachable()
	w.synced.Store(true)
	w.upsert(dgd("graph-a", map[string]any{"Worker": map[string]any{"readyReplicas": int64(8)}}))

	// A watch error with the API server still answering changes nothing.
	w.watchFailed(errors.New("watch closed"))
	now = now.Add(2 * w.staleAfter)
	w.probe(context.Background())
	if got := w.Ready("graph-a"); got != 8 {
		t.Fatalf("API server reachable: Ready = %d, want the last-known 8", got)
	}

	// The API server stops answering (or the grant is revoked).
	failing := true
	client.PrependReactor("list", "dynamographdeployments", func(clienttesting.Action) (bool, runtime.Object, error) {
		if failing {
			return true, nil, errors.New("forbidden")
		}
		return false, nil, nil
	})
	now = now.Add(w.staleAfter)
	w.probe(context.Background())
	if got := w.Ready("graph-a"); got != 8 {
		t.Fatalf("exactly at the bound Ready = %d, want the last-known 8", got)
	}
	now = now.Add(time.Second)
	w.probe(context.Background())
	if got := w.Ready("graph-a"); got != 0 {
		t.Fatalf("past the bound Ready = %d, want 0 (one worker's worth)", got)
	}

	// A successful probe restores the counts, with no DGD event needed.
	failing = false
	w.probe(context.Background())
	if got := w.Ready("graph-a"); got != 8 {
		t.Fatalf("after a successful probe Ready = %d, want 8", got)
	}
}

// Before the first sync there is nothing stale to expire: unknown counts are
// already 0, and a probe that never succeeded must not misreport staleness.
func TestReadyWorkersNotStaleBeforeFirstSync(t *testing.T) {
	w := NewReadyWorkers(dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{dynamoGraphResource: "DynamoGraphDeploymentList"}),
		registryNS, logging.New(logging.ERROR))
	if w.stale() {
		t.Fatal("stale() before any successful read")
	}
}

// A probe that fails before the first sync has no last read to expire: it
// does not mark the source unreachable, does not log the stale message, and
// does not record a successful read.
func TestReadyWorkersProbeFailureBeforeFirstSyncDoesNotMarkUnreachable(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{dynamoGraphResource: "DynamoGraphDeploymentList"})
	w := NewReadyWorkers(client, registryNS, logging.New(logging.ERROR))
	errs, infos := &syncBuffer{}, &syncBuffer{}
	w.log.Error.SetOutput(errs)
	w.log.Info.SetOutput(infos)
	now := time.Unix(1_000_000, 0)
	w.now = func() time.Time { return now }
	client.PrependReactor("list", "dynamographdeployments", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("forbidden")
	})
	w.upsert(dgd("graph-a", map[string]any{"Worker": map[string]any{"readyReplicas": int64(8)}}))
	for i := 0; i < 3; i++ {
		w.probe(context.Background())
		now = now.Add(2 * w.staleAfter)
	}
	if w.lastReachable.Load() != nil {
		t.Fatal("a failed probe recorded a successful read")
	}
	if w.stale() {
		t.Fatal("stale() before any successful read")
	}
	if w.unreachable.Load() {
		t.Fatal("unreachable set before the first sync")
	}
	if strings.Contains(errs.String(), "not confirmed") {
		t.Fatalf("error log = %q, want no stale message before the first sync", errs.String())
	}
	if got := w.Ready("graph-a"); got != 0 {
		t.Fatalf("Ready(graph-a) = %d, want 0 before the first sync", got)
	}
	if got := w.Ready("graph-unknown"); got != 0 {
		t.Fatalf("Ready(graph-unknown) = %d, want 0", got)
	}
}

// Before the first sync every graph reads 0 (one worker's worth), even one
// the initial list has already delivered; the sync then exposes the count.
func TestReadyWorkersUnsyncedReadsZeroEvenWithDeliveredObjects(t *testing.T) {
	w := NewReadyWorkers(dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{dynamoGraphResource: "DynamoGraphDeploymentList"}),
		registryNS, logging.New(logging.ERROR))
	w.upsert(dgd("graph-a", map[string]any{"Worker": map[string]any{"readyReplicas": int64(8)}}))
	if got := w.Ready("graph-a"); got != 0 {
		t.Fatalf("unsynced Ready = %d, want 0", got)
	}
	w.synced.Store(true)
	if got := w.Ready("graph-a"); got != 8 {
		t.Fatalf("synced Ready = %d, want 8", got)
	}
}

// The informer follows live status changes of the Atlas-managed DGDs.
func TestReadyWorkersInformerFollowsStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{dynamoGraphResource: "DynamoGraphDeploymentList"},
		dgd("graph-a", map[string]any{"Worker": map[string]any{"readyReplicas": int64(1)}}),
	)
	w := NewReadyWorkers(client, registryNS, logging.New(logging.ERROR))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	waitFor := func(graph string, want int64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if w.Ready(graph) == want {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("Ready(%s) = %d, want %d", graph, w.Ready(graph), want)
	}
	waitFor("graph-a", 1)
	updated := dgd("graph-a", map[string]any{"Worker": map[string]any{"readyReplicas": int64(4)}})
	if _, err := client.Resource(dynamoGraphResource).Namespace(registryNS).Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor("graph-a", 4)
	if err := client.Resource(dynamoGraphResource).Namespace(registryNS).Delete(ctx, "graph-a", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor("graph-a", 0)
}

// Only Atlas-managed DGDs are read: an unlabeled DGD reads 0 ready workers
// while a labelled one created later is seen.
func TestReadyWorkersIgnoresUnmanagedGraphs(t *testing.T) {
	unmanaged := dgd("graph-unmanaged", map[string]any{"Worker": map[string]any{"readyReplicas": int64(7)}})
	unmanaged.SetLabels(nil)
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{dynamoGraphResource: "DynamoGraphDeploymentList"},
		dgd("graph-a", map[string]any{"Worker": map[string]any{"readyReplicas": int64(1)}}),
		unmanaged,
	)
	w := NewReadyWorkers(client, registryNS, logging.New(logging.ERROR))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	waitFor := func(graph string, want int64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if w.Ready(graph) == want {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("Ready(%s) = %d, want %d", graph, w.Ready(graph), want)
	}
	waitFor("graph-a", 1)
	if got := w.Ready("graph-unmanaged"); got != 0 {
		t.Fatalf("Ready(graph-unmanaged) = %d, want 0 (not Atlas-managed)", got)
	}
	labelled := dgd("graph-b", map[string]any{"Worker": map[string]any{"readyReplicas": int64(2)}})
	if _, err := client.Resource(dynamoGraphResource).Namespace(registryNS).Create(ctx, labelled, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor("graph-b", 2)
	if got := w.Ready("graph-unmanaged"); got != 0 {
		t.Fatalf("Ready(graph-unmanaged) = %d, want 0 (not Atlas-managed)", got)
	}
}

// The unit marker makes either rollout order safe (Q22): an entry without
// admission_capacity_unit (an Atlas older than per-worker publication) is
// enforced as published, never multiplied; and on a graph whose entries
// disagree during a rollout, each entry is read in its own unit before the
// tightest wins. The unknown-unit case is
// TestRegistryGraphCapacityEnforcesUnknownUnitAsPublished.
func TestRegistryGraphCapacityScalesOnlyPerWorkerEntries(t *testing.T) {
	r, client, _ := startedRegistry(t, registryCM("tf-model-old", map[string]string{
		"admission_max_active_requests": "40",
	}))
	r.WithReadyWorkers(fixedWorkers{"graph-llama31": 3})
	c := waitCapacity(t, r, "graph-llama31", func(c GraphCapacity) bool { return c.Any() })
	if val(c.MaxActiveRequests) != 40 {
		t.Fatalf("an unmarked (graph-level) entry must be enforced as published, got %d", val(c.MaxActiveRequests))
	}

	// A per-worker entry from a newer Atlas on the same graph: 8 x (3 + 1) =
	// 32 is tighter than the old entry's 40, so it wins.
	if _, err := client.CoreV1().ConfigMaps(registryNS).Create(context.Background(), registryCM("tf-model-new", map[string]string{
		"org_id": "org-b", capacityKeyUnit: capacityUnitPerWorker, "admission_max_active_requests": "8",
	}), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	c = waitCapacity(t, r, "graph-llama31", func(c GraphCapacity) bool { return val(c.MaxActiveRequests) == 32 })
	if val(c.MaxActiveRequests) != 32 {
		t.Fatalf("mixed units: want min(40, 8 x 4) = 32, got %d", val(c.MaxActiveRequests))
	}
}

// An unknown admission_capacity_unit (D55(a)) is enforced as published, as
// a graph-level value multiplied by nothing, and the log says so: it must not
// claim the limits are ignored.
func TestRegistryGraphCapacityEnforcesUnknownUnitAsPublished(t *testing.T) {
	r, client, _ := startedRegistry(t)
	logs := &syncBuffer{}
	r.log.Error.SetOutput(logs)
	r.WithReadyWorkers(fixedWorkers{"graph-llama31": 3})
	if _, err := client.CoreV1().ConfigMaps(registryNS).Create(context.Background(), registryCM("tf-model-odd", map[string]string{
		capacityKeyUnit: "bogus", "admission_max_active_requests": "16",
	}), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	c := waitCapacity(t, r, "graph-llama31", func(c GraphCapacity) bool { return c.Any() })
	if val(c.MaxActiveRequests) != 16 {
		t.Fatalf("MaxActiveRequests = %d, want 16 as published (not 16 x 4 = 64)", val(c.MaxActiveRequests))
	}
	out := logs.String()
	if !strings.Contains(out, "tf-model-odd") || !strings.Contains(out, capacityKeyUnit) || !strings.Contains(out, "enforcing") {
		t.Fatalf("log = %q, want the ConfigMap, the unit key and that the limits are enforced", out)
	}
	if strings.Contains(out, "no limit") {
		t.Fatalf("log = %q claims no limit for an unknown unit whose limits are enforced", out)
	}
}

func TestParseGraphCapacityUnit(t *testing.T) {
	c, badValues, badUnit := parseGraphCapacity(map[string]string{"admission_max_active_requests": "5", capacityKeyUnit: " per_worker "})
	if !c.PerWorker || len(badValues) != 0 || badUnit != "" {
		t.Fatalf("per_worker marker: PerWorker=%v badValues=%v badUnit=%q", c.PerWorker, badValues, badUnit)
	}
	c, badValues, badUnit = parseGraphCapacity(map[string]string{"admission_max_active_requests": "5"})
	if c.PerWorker || len(badValues) != 0 || badUnit != "" {
		t.Fatalf("no marker means graph-level: PerWorker=%v badValues=%v badUnit=%q", c.PerWorker, badValues, badUnit)
	}
}

// An unknown unit keeps its values as graph-level limits and is reported
// only through the unit channel, never as an ignored limit value.
func TestParseGraphCapacityUnknownUnitKeepsValuesAndReportsOnlyTheUnit(t *testing.T) {
	c, badValues, badUnit := parseGraphCapacity(map[string]string{"admission_max_active_requests": "8", capacityKeyUnit: "per-worker"})
	if c.PerWorker {
		t.Fatal("an unknown unit must not be read as per-worker")
	}
	if val(c.MaxActiveRequests) != 8 {
		t.Fatalf("MaxActiveRequests = %d, want 8 kept", val(c.MaxActiveRequests))
	}
	if len(badValues) != 0 {
		t.Fatalf("badValues = %v, want none", badValues)
	}
	if badUnit != capacityKeyUnit+`="per-worker"` {
		t.Fatalf("badUnit = %q", badUnit)
	}
}
