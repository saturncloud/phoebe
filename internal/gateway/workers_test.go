package gateway

import (
	"bytes"
	"context"
	"errors"
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
	k8stesting "k8s.io/client-go/testing"

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
		{"Worker with none ready falls back to VllmWorker", map[string]any{"Worker": map[string]any{"readyReplicas": int64(0)}, "VllmWorker": map[string]any{"readyReplicas": int64(2)}}, 2},
		{"desired replicas are not ready workers", map[string]any{"Worker": map[string]any{"replicas": int64(4)}}, 0},
		{"a string count reads as zero", map[string]any{"Worker": map[string]any{"readyReplicas": "3"}}, 0},
		{"malformed component", map[string]any{"Worker": "x"}, 0},
		{"no status", nil, 0},
		{"case-insensitive component names", map[string]any{"worker": map[string]any{"readyReplicas": int64(0)}, "VLLMWorker": map[string]any{"readyReplicas": int64(5)}}, 5},
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
	r, _, _ := startedRegistry(t, registryCM("tf-model-a", map[string]string{
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
// zero cap stays zero (R-K), an absent key stays no limit, only the winning
// duplicate claimant counts, and a huge ready count saturates the limit at
// the int64 maximum instead of wrapping to an unscaled value.
func TestRegistryGraphCapacityScaledKeepsRegistryInvariants(t *testing.T) {
	cases := []struct {
		name    string
		cms     []*corev1.ConfigMap
		workers int64
		ready   func(GraphCapacity) bool
		check   func(t *testing.T, c GraphCapacity)
	}{
		{
			name: "explicit zero cap stays zero (R-K)",
			cms: []*corev1.ConfigMap{registryCM("tf-model-a", map[string]string{
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
			cms: []*corev1.ConfigMap{registryCM("tf-model-a", map[string]string{
				"admission_max_prefill_tokens": "4096",
			})},
			workers: 5,
			check: func(t *testing.T, c GraphCapacity) {
				if c.MaxActiveRequests != nil {
					t.Fatalf("MaxActiveRequests = %d, want nil (no limit)", *c.MaxActiveRequests)
				}
				if val(c.MaxPrefillTokens) != 4096*6 {
					t.Fatalf("MaxPrefillTokens = %d, want 4096 x 6", val(c.MaxPrefillTokens))
				}
			},
		},
		{
			name: "only the winning duplicate claimant is scaled",
			cms: []*corev1.ConfigMap{
				registryCM("tf-model-winner", map[string]string{"resource_id": "tfm-aaa", "admission_max_active_requests": "10"}),
				registryCM("tf-model-stale", map[string]string{"resource_id": "tfm-bbb", "admission_max_active_requests": "2"}),
			},
			workers: 2,
			ready:   func(c GraphCapacity) bool { return val(c.MaxActiveRequests) == 30 },
			check: func(t *testing.T, c GraphCapacity) {
				if val(c.MaxActiveRequests) != 30 {
					t.Fatalf("MaxActiveRequests = %d, want the winner's 10 x 3, not the loser's 2 x 3", val(c.MaxActiveRequests))
				}
			},
		},
		{
			name: "a huge ready count saturates instead of wrapping",
			cms: []*corev1.ConfigMap{registryCM("tf-model-a", map[string]string{
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
			ready := tc.ready
			if ready == nil {
				ready = func(c GraphCapacity) bool { return c.Any() }
			}
			tc.check(t, waitCapacity(t, r, "graph-llama31", ready))
		})
	}
}

// Scaling keeps an explicit zero cap at zero and an absent key at no limit
// while multiplying the set limits.
func TestRegistryGraphCapacityScaledKeepsZeroAndNil(t *testing.T) {
	r, _, _ := startedRegistry(t, registryCM("tf-model-a", map[string]string{
		"admission_max_active_requests": "0", "admission_max_prefill_tokens": "4096",
	}))
	waitCapacity(t, r, "graph-llama31", func(c GraphCapacity) bool { return c.Any() })
	r.WithReadyWorkers(fixedWorkers{"graph-llama31": 4})
	c := r.GraphCapacity("graph-llama31")
	if c.MaxActiveRequests == nil || *c.MaxActiveRequests != 0 {
		t.Fatalf("MaxActiveRequests = %v, want an explicit 0", c.MaxActiveRequests)
	}
	if c.MaxReservedKVTokens != nil {
		t.Fatalf("MaxReservedKVTokens = %d, want nil", *c.MaxReservedKVTokens)
	}
	if val(c.MaxPrefillTokens) != 20480 {
		t.Fatalf("MaxPrefillTokens = %d, want 4096 x 5 = 20480", val(c.MaxPrefillTokens))
	}
}

// forbiddenDGDClient is a dynamic client whose DGD list is forbidden, as
// with missing RBAC.
func forbiddenDGDClient() *dynamicfake.FakeDynamicClient {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{dynamoGraphResource: "DynamoGraphDeploymentList"})
	client.PrependReactor("list", "dynamographdeployments", func(k8stesting.Action) (bool, runtime.Object, error) {
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
	logs := &syncBuffer{}
	w.log.Error.SetOutput(logs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	r, _, _ := startedRegistry(t, registryCM("tf-model-a", map[string]string{
		"admission_max_active_requests": "16",
	}))
	r.WithReadyWorkers(w)
	c := waitCapacity(t, r, "graph-llama31", func(c GraphCapacity) bool { return c.Any() })
	if c.MaxActiveRequests == nil || *c.MaxActiveRequests != 16 {
		t.Fatalf("MaxActiveRequests = %v, want the per-worker 16 x (0 + 1)", c.MaxActiveRequests)
	}
	if w.Ready("graph-llama31") != 0 {
		t.Fatalf("Ready = %d, want 0 with the list forbidden", w.Ready("graph-llama31"))
	}
	if w.Synced() {
		t.Fatal("Synced() = true with the list forbidden")
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

// While the watch fails, a last-known count is trusted for at most
// readyWorkersStaleAfter and then reads as 0; a later event clears the
// failure and the counts are trusted again.
func TestReadyWorkersLastKnownCountExpiresWhileWatchFails(t *testing.T) {
	w := NewReadyWorkers(dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{dynamoGraphResource: "DynamoGraphDeploymentList"}),
		registryNS, logging.New(logging.ERROR))
	w.log.Error.SetOutput(&syncBuffer{})
	now := time.Unix(1_000_000, 0)
	w.now = func() time.Time { return now }
	w.synced.Store(true)
	w.upsert(dgd("graph-a", map[string]any{"Worker": map[string]any{"readyReplicas": int64(8)}}))

	w.watchFailed(errors.New("forbidden"))
	now = now.Add(readyWorkersStaleAfter)
	if got := w.Ready("graph-a"); got != 8 {
		t.Fatalf("within the bound Ready = %d, want the last-known 8", got)
	}
	w.watchFailed(errors.New("forbidden again")) // must not restart the clock
	now = now.Add(time.Second)
	if got := w.Ready("graph-a"); got != 0 {
		t.Fatalf("past the bound Ready = %d, want 0 (one worker's worth)", got)
	}

	w.upsert(dgd("graph-a", map[string]any{"Worker": map[string]any{"readyReplicas": int64(2)}}))
	if got := w.Ready("graph-a"); got != 2 {
		t.Fatalf("after a recovered event Ready = %d, want 2", got)
	}
	if w.failingSince.Load() != 0 {
		t.Fatal("an event must clear the watch failure")
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
