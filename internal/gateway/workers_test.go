package gateway

import (
	"context"
	"math"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

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
	big := GraphCapacity{MaxPrefillTokens: ptr(math.MaxInt64/2 + 1)}
	if *big.Scaled(2).MaxPrefillTokens != math.MaxInt64 {
		t.Fatal("Scaled must saturate at the int64 maximum")
	}
}

type fixedWorkers map[string]int64

func (f fixedWorkers) Ready(graph string) int64 { return f[graph] }

// The registry multiplies the per-worker limit Atlas publishes by the graph's
// ready workers plus one; an unknown graph admits one worker's worth.
func TestRegistryGraphCapacityScalesByReadyWorkersPlusOne(t *testing.T) {
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
