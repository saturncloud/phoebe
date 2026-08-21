package waker

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/proxy"
)

const testNS = "tf-shared"

func dgdsaObj(graph string, replicas int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "nvidia.com/v1alpha1",
		"kind":       "DynamoGraphDeploymentScalingAdapter",
		"metadata":   map[string]any{"name": DGDSAName(graph), "namespace": testNS},
		"spec": map[string]any{
			"replicas": replicas,
			"dgdRef":   map[string]any{"name": graph, "serviceName": "VllmWorker"},
		},
	}}
}

func scaledObjectObj(graph string, paused string) *unstructured.Unstructured {
	meta := map[string]any{"name": ScaledObjectName(graph), "namespace": testNS}
	if paused != "" {
		meta["annotations"] = map[string]any{kedaPausedAnnotation: paused}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "keda.sh/v1alpha1",
		"kind":       "ScaledObject",
		"metadata":   meta,
	}}
}

// newFakeWaker builds a KubeWaker over client-go's fake dynamic client seeded
// with objs, with readiness stubbed to always-ready (readiness has its own
// tests). Returns the waker and the fake for action assertions.
func newFakeWaker(t *testing.T, objs ...runtime.Object) (*KubeWaker, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	scheme := runtime.NewScheme()
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		dgdsaGVR:        "DynamoGraphDeploymentScalingAdapterList",
		scaledObjectGVR: "ScaledObjectList",
	}, objs...)
	w := NewWithClient(client, Config{Namespace: testNS, PollInterval: time.Millisecond}, logging.New(logging.ERROR))
	w.ready = func(context.Context, string) bool { return true }
	return w, client
}

func target(graph string) proxy.WakeTarget {
	return proxy.WakeTarget{
		UpstreamHost: DGDSAName(graph) + "-frontend." + testNS + ".svc.cluster.local:8000",
		GraphK8sName: graph,
		ResourceID:   "tfm-1",
	}
}

// patchActions returns the PATCH actions issued, in order.
func patchActions(client *dynamicfake.FakeDynamicClient) []k8stesting.PatchAction {
	var out []k8stesting.PatchAction
	for _, a := range client.Actions() {
		if p, ok := a.(k8stesting.PatchAction); ok {
			out = append(out, p)
		}
	}
	return out
}

// TestWake_ColdPatchesReplicasThenUnpausesKEDA locks the LOAD-BEARING order of
// the KEDA pause handoff (KEDA 2.20.2 semantics): the DGDSA is scaled 0->1
// FIRST (the pod starts now), THEN the ScaledObject is unpaused
// (paused: "false") to hand 1->N back to KEDA. Also pins the patch payloads:
// replicas exactly 1, annotation exactly "false".
func TestWake_ColdPatchesReplicasThenUnpausesKEDA(t *testing.T) {
	w, client := newFakeWaker(t, dgdsaObj("g1", 0), scaledObjectObj("g1", "true"))

	if err := w.Wake(context.Background(), target("g1")); err != nil {
		t.Fatalf("Wake: %v", err)
	}

	patches := patchActions(client)
	if len(patches) != 2 {
		t.Fatalf("issued %d patches, want 2 (DGDSA scale, then SO unpause)", len(patches))
	}
	// ORDER: DGDSA first, ScaledObject second.
	if patches[0].GetResource() != dgdsaGVR || patches[0].GetName() != DGDSAName("g1") {
		t.Fatalf("first patch = %s %q, want the DGDSA scale-up FIRST", patches[0].GetResource().Resource, patches[0].GetName())
	}
	if patches[1].GetResource() != scaledObjectGVR || patches[1].GetName() != ScaledObjectName("g1") {
		t.Fatalf("second patch = %s %q, want the ScaledObject unpause SECOND", patches[1].GetResource().Resource, patches[1].GetName())
	}
	// PAYLOADS.
	var scale struct {
		Spec struct {
			Replicas int `json:"replicas"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(patches[0].GetPatch(), &scale); err != nil || scale.Spec.Replicas != 1 {
		t.Fatalf("scale patch %s: replicas=%d err=%v, want exactly 1", patches[0].GetPatch(), scale.Spec.Replicas, err)
	}
	var unpause struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(patches[1].GetPatch(), &unpause); err != nil ||
		unpause.Metadata.Annotations[kedaPausedAnnotation] != "false" {
		t.Fatalf("unpause patch %s (err=%v), want %s=\"false\"", patches[1].GetPatch(), err, kedaPausedAnnotation)
	}

	// The stored DGDSA now has replicas 1 (the fake applies the merge patch).
	got, err := client.Resource(dgdsaGVR).Namespace(testNS).Get(context.Background(), DGDSAName("g1"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get DGDSA: %v", err)
	}
	replicas, _, _ := unstructured.NestedInt64(got.Object, "spec", "replicas")
	if replicas != 1 {
		t.Fatalf("DGDSA replicas = %d, want 1", replicas)
	}
}

// TestWake_AlreadyAwakeIsNoOpSuccess: spec.replicas >= 1 means another
// actor/request already woke the graph — success with ZERO writes (no scale
// patch, no unpause).
func TestWake_AlreadyAwakeIsNoOpSuccess(t *testing.T) {
	w, client := newFakeWaker(t, dgdsaObj("g1", 1), scaledObjectObj("g1", "true"))

	if err := w.Wake(context.Background(), target("g1")); err != nil {
		t.Fatalf("Wake: %v", err)
	}
	if patches := patchActions(client); len(patches) != 0 {
		t.Fatalf("issued %d patches, want 0 (already awake)", len(patches))
	}
}

// TestDGDSANames pins the literal name contract: the uniform multi-backend
// name `<graph>-worker` (what Atlas's renamed Worker component yields) and the
// pre-multibackend legacy `<graph>-vllmworker` the fallback still serves.
func TestDGDSANames(t *testing.T) {
	if got := DGDSAName("g1"); got != "g1-worker" {
		t.Fatalf("DGDSAName = %q, want g1-worker", got)
	}
	if got := legacyDGDSAName("g1"); got != "g1-vllmworker" {
		t.Fatalf("legacyDGDSAName = %q, want g1-vllmworker", got)
	}
}

// TestWake_UniformWorkerNameHappyPath: a post-rename graph (only
// `<graph>-worker` exists) wakes via the uniform name — one read, the scale
// patch lands on `<graph>-worker`, and the legacy name is never consulted.
func TestWake_UniformWorkerNameHappyPath(t *testing.T) {
	w, client := newFakeWaker(t, dgdsaObj("g1", 0), scaledObjectObj("g1", "true"))

	if err := w.Wake(context.Background(), target("g1")); err != nil {
		t.Fatalf("Wake: %v", err)
	}
	patches := patchActions(client)
	if len(patches) != 2 || patches[0].GetName() != "g1-worker" {
		t.Fatalf("patches = %v, want the scale patch on g1-worker first", patches)
	}
	for _, a := range client.Actions() {
		if a.GetVerb() == "get" && a.(k8stesting.GetAction).GetName() == "g1-vllmworker" {
			t.Fatal("uniform-name graph must never consult the legacy name")
		}
	}
}

// TestWake_LegacyVllmWorkerFallback: a pre-multibackend graph (only
// `<graph>-vllmworker` exists) still wakes — the uniform name 404s, the
// fallback reads the legacy adapter, and the scale patch lands on the SAME
// name that was read. The ScaledObject unpause (graph-named `<graph>-scaler`,
// not component-coupled) fires as usual.
func TestWake_LegacyVllmWorkerFallback(t *testing.T) {
	legacy := dgdsaObj("g1", 0)
	legacy.Object["metadata"].(map[string]any)["name"] = legacyDGDSAName("g1")
	w, client := newFakeWaker(t, legacy, scaledObjectObj("g1", "true"))

	if err := w.Wake(context.Background(), target("g1")); err != nil {
		t.Fatalf("Wake via legacy name: %v", err)
	}
	patches := patchActions(client)
	if len(patches) != 2 {
		t.Fatalf("issued %d patches, want 2 (legacy scale + SO unpause)", len(patches))
	}
	if patches[0].GetName() != "g1-vllmworker" {
		t.Fatalf("scale patch on %q, want the legacy g1-vllmworker (patch what was read)", patches[0].GetName())
	}
	if patches[1].GetName() != ScaledObjectName("g1") {
		t.Fatalf("unpause on %q, want %s (graph-named, unaffected by the component rename)", patches[1].GetName(), ScaledObjectName("g1"))
	}
}

// TestWake_BothDGDSANamesMissingErrors: neither the uniform nor the legacy
// adapter exists — the wake errors (naming BOTH candidates for diagnosis) and
// writes nothing; the proxy then serves the honest cold response (see the
// serveWithWake integration test below).
func TestWake_BothDGDSANamesMissingErrors(t *testing.T) {
	w, client := newFakeWaker(t) // empty cluster

	err := w.Wake(context.Background(), target("g1"))
	if err == nil {
		t.Fatal("Wake with no DGDSA under either name must error")
	}
	for _, name := range []string{"g1-worker", "g1-vllmworker"} {
		if !strings.Contains(err.Error(), name) {
			t.Fatalf("error %q does not name candidate %q", err, name)
		}
	}
	if patches := patchActions(client); len(patches) != 0 {
		t.Fatalf("issued %d patches, want 0 (nothing to actuate)", len(patches))
	}
}

// TestWake_ScaledObjectMissingScaleStillSucceeds: the unpause is best-effort —
// a KEDA-less install (no ScaledObject) still wakes; the scale-up is the
// critical step.
func TestWake_ScaledObjectMissingScaleStillSucceeds(t *testing.T) {
	w, client := newFakeWaker(t, dgdsaObj("g1", 0)) // no ScaledObject

	if err := w.Wake(context.Background(), target("g1")); err != nil {
		t.Fatalf("Wake must succeed without a ScaledObject: %v", err)
	}
	got, err := client.Resource(dgdsaGVR).Namespace(testNS).Get(context.Background(), DGDSAName("g1"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get DGDSA: %v", err)
	}
	replicas, _, _ := unstructured.NestedInt64(got.Object, "spec", "replicas")
	if replicas != 1 {
		t.Fatalf("DGDSA replicas = %d, want 1 (scale-up must land)", replicas)
	}
}

// TestWake_ConcurrentWakesSingleEffectivePatch: N racing cold requests for one
// graph issue exactly ONE scale patch — the per-graph lock serializes
// read-then-patch, so the losers observe replicas=1 and no-op.
func TestWake_ConcurrentWakesSingleEffectivePatch(t *testing.T) {
	w, client := newFakeWaker(t, dgdsaObj("g1", 0), scaledObjectObj("g1", "true"))

	const n = 16
	var wg sync.WaitGroup
	var errs int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Wake(context.Background(), target("g1")); err != nil {
				atomic.AddInt32(&errs, 1)
			}
		}()
	}
	wg.Wait()

	if errs != 0 {
		t.Fatalf("%d concurrent wakes errored, want 0", errs)
	}
	var dgdsaPatches int
	for _, p := range patchActions(client) {
		if p.GetResource() == dgdsaGVR {
			dgdsaPatches++
		}
	}
	if dgdsaPatches != 1 {
		t.Fatalf("issued %d DGDSA scale patches, want exactly 1 effective patch", dgdsaPatches)
	}
}

// TestWake_NeverScalesDown: whatever the current count (loaded graph at N>1),
// the waker never writes a smaller number — it writes nothing at all.
func TestWake_NeverScalesDown(t *testing.T) {
	for _, replicas := range []int64{1, 3, 7} {
		w, client := newFakeWaker(t, dgdsaObj("g1", replicas), scaledObjectObj("g1", ""))
		if err := w.Wake(context.Background(), target("g1")); err != nil {
			t.Fatalf("replicas=%d Wake: %v", replicas, err)
		}
		if patches := patchActions(client); len(patches) != 0 {
			t.Fatalf("replicas=%d: issued %d patches, want 0 (never scale down, never touch a warm graph)", replicas, len(patches))
		}
	}
}

// TestWake_HoldsUntilReadyOrDeadline: after the patch, Wake HOLDS the request
// until readiness (proxy's retry budget is attempts, not time) and returns the
// ctx error when the worker never comes up in time.
func TestWake_HoldsUntilReadyOrDeadline(t *testing.T) {
	// Never-ready: Wake must return the deadline error, not hang, not succeed.
	w, _ := newFakeWaker(t, dgdsaObj("g1", 0))
	w.ready = func(context.Context, string) bool { return false }
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := w.Wake(ctx, target("g1")); err == nil || !strings.Contains(err.Error(), "not ready before deadline") {
		t.Fatalf("never-ready Wake err = %v, want deadline error", err)
	}

	// Ready-after-a-few-polls: Wake returns nil once the probe flips.
	w2, _ := newFakeWaker(t, dgdsaObj("g2", 0))
	var polls int32
	w2.ready = func(context.Context, string) bool { return atomic.AddInt32(&polls, 1) >= 3 }
	if err := w2.Wake(context.Background(), target("g2")); err != nil {
		t.Fatalf("eventually-ready Wake: %v", err)
	}
	if atomic.LoadInt32(&polls) < 3 {
		t.Fatalf("readiness polled %d times, want >= 3 (must hold until ready)", polls)
	}
}

// TestWake_EmptyGraphNameErrors: a target with no graph identity is a
// programming error upstream — refuse rather than patch a guessed name.
func TestWake_EmptyGraphNameErrors(t *testing.T) {
	w, client := newFakeWaker(t, dgdsaObj("g1", 0))
	if err := w.Wake(context.Background(), proxy.WakeTarget{UpstreamHost: "h:1", ResourceID: "r"}); err == nil {
		t.Fatal("Wake with empty GraphK8sName must error")
	}
	if patches := patchActions(client); len(patches) != 0 {
		t.Fatalf("issued %d patches, want 0", len(patches))
	}
}
