package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/saturncloud/phoebe/internal/logging"
)

const registryNS = "tf-shared"

// registryCM builds a served-model registry ConfigMap per the contract of
// record (see registry.go). Pass overrides to mutate/delete data keys.
func registryCM(name string, data map[string]string) *corev1.ConfigMap {
	base := map[string]string{
		"org_id":            "org-1",
		"served_model_name": "support-bot",
		"resource_id":       "tfm-1",
		"base_model":        "meta-llama/Llama-3.1-8B-Instruct",
		"adapter":           "",
		"serving_mode":      "shared",
		"graph_k8s_name":    "graph-llama31",
		"port":              "8000",
	}
	for k, v := range data {
		if v == "" && k != "adapter" && k != "serving_mode" && k != "port" {
			delete(base, k)
			continue
		}
		base[k] = v
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: registryNS,
			Labels:    map[string]string{RegistryLabel: "true"},
		},
		Data: base,
	}
}

// startedRegistry builds a RegistryResolver over a fake clientset seeded with
// objs, starts its informer, and waits for the initial sync. Returns the
// resolver, the fake clientset (for live add/update/delete), and a cancel.
func startedRegistry(t *testing.T, objs ...*corev1.ConfigMap) (*RegistryResolver, *fake.Clientset, context.CancelFunc) {
	t.Helper()
	client := fake.NewSimpleClientset()
	for _, cm := range objs {
		if _, err := client.CoreV1().ConfigMaps(registryNS).Create(context.Background(), cm, metav1.CreateOptions{}); err != nil {
			t.Fatalf("seed ConfigMap %s: %v", cm.Name, err)
		}
	}
	r := NewRegistryResolver(client, registryNS, logging.New(logging.ERROR))
	ctx, cancel := context.WithCancel(context.Background())
	go r.Run(ctx)
	t.Cleanup(cancel)
	if !r.WaitSynced(5 * time.Second) {
		t.Fatal("registry informer never synced")
	}
	return r, client, cancel
}

// waitResolve polls until (org, model) resolves (or times out), absorbing the
// informer's asynchronous event delivery.
func waitResolve(t *testing.T, r *RegistryResolver, org, model string) (Resolution, bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if res, err := r.Resolve(context.Background(), org, model); err == nil {
			return res, true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return Resolution{}, false
}

// waitNotFound polls until (org, model) stops resolving.
func waitNotFound(t *testing.T, r *RegistryResolver, org, model string) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := r.Resolve(context.Background(), org, model); errors.Is(err, ErrNotFound) {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// TestRegistry_InformerFedHappyPath: seeded base-model and adapter
// registrations resolve from memory with every contract field carried —
// including the registration's port.
func TestRegistry_InformerFedHappyPath(t *testing.T) {
	baseRow := registryCM("tf-model-base1", map[string]string{
		"served_model_name": "meta-llama/Llama-3.1-8B-Instruct",
		"resource_id":       "tfm-base-1",
		"adapter":           "",
	})
	ftRow := registryCM("tf-model-ft1", map[string]string{
		"served_model_name": "support-bot",
		"resource_id":       "tfm-ft-1",
		"adapter":           "ckpt-42",
		"port":              "9099",
	})
	r, _, _ := startedRegistry(t, baseRow, ftRow)

	res, err := r.Resolve(context.Background(), "org-1", "meta-llama/Llama-3.1-8B-Instruct")
	if err != nil {
		t.Fatalf("base row: %v", err)
	}
	if res.ResourceID != "tfm-base-1" || res.Adapter != "" || res.ServingMode != "shared" ||
		res.GraphK8sName != "graph-llama31" || res.Port != 8000 {
		t.Fatalf("base resolution = %+v", res)
	}

	res, err = r.Resolve(context.Background(), "org-1", "support-bot")
	if err != nil {
		t.Fatalf("adapter row: %v", err)
	}
	if res.ResourceID != "tfm-ft-1" || res.Adapter != "ckpt-42" || res.Port != 9099 {
		t.Fatalf("adapter resolution = %+v", res)
	}
}

// TestRegistry_UpdateAndDeletePropagation: a live update re-keys the entry
// (old name stops resolving, new one starts) and a delete removes it.
func TestRegistry_UpdateAndDeletePropagation(t *testing.T) {
	cm := registryCM("tf-model-1", nil)
	r, client, _ := startedRegistry(t, cm)

	if _, ok := waitResolve(t, r, "org-1", "support-bot"); !ok {
		t.Fatal("seeded row must resolve")
	}

	// UPDATE: the served name changes; the old key must stop resolving and
	// the new one must start (cmKey re-keying, not just an add).
	updated := registryCM("tf-model-1", map[string]string{"served_model_name": "support-bot-v2"})
	if _, err := client.CoreV1().ConfigMaps(registryNS).Update(context.Background(), updated, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, ok := waitResolve(t, r, "org-1", "support-bot-v2"); !ok {
		t.Fatal("updated served name must resolve")
	}
	if !waitNotFound(t, r, "org-1", "support-bot") {
		t.Fatal("old served name must stop resolving after the update")
	}

	// DELETE: the registration disappears entirely.
	if err := client.CoreV1().ConfigMaps(registryNS).Delete(context.Background(), "tf-model-1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !waitNotFound(t, r, "org-1", "support-bot-v2") {
		t.Fatal("deleted registration must stop resolving")
	}
}

// TestRegistry_UnsyncedAtStartupFailsClosed: before the informer's first sync
// the resolver returns ErrNotSynced — NOT ErrNotFound — so the proxy 503s
// (any non-NotFound resolver error is the 503 path, pinned by the proxy's
// TestGateway_DBDown503) instead of lying with a 404.
func TestRegistry_UnsyncedAtStartupFailsClosed(t *testing.T) {
	client := fake.NewSimpleClientset()
	r := NewRegistryResolver(client, registryNS, logging.New(logging.ERROR)) // never Run

	_, err := r.Resolve(context.Background(), "org-1", "support-bot")
	if !errors.Is(err, ErrNotSynced) {
		t.Fatalf("unsynced Resolve err = %v, want ErrNotSynced", err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatal("unsynced must NOT report NotFound (that would 404 instead of 503)")
	}
}

// TestRegistry_KeepsServingAfterInformerStops: once synced, losing the watch
// (API-server flap, modeled by stopping the informer) does NOT un-sync the
// resolver — lookups keep answering from last-known state, because they are
// pure memory reads. The availability tradeoff documented on the type.
func TestRegistry_KeepsServingAfterInformerStops(t *testing.T) {
	r, _, cancel := startedRegistry(t, registryCM("tf-model-1", nil))
	if _, ok := waitResolve(t, r, "org-1", "support-bot"); !ok {
		t.Fatal("row must resolve while the watch runs")
	}

	cancel() // the "API server is gone" moment
	time.Sleep(20 * time.Millisecond)

	res, err := r.Resolve(context.Background(), "org-1", "support-bot")
	if err != nil || res.ResourceID != "tfm-1" {
		t.Fatalf("post-outage Resolve = %+v, %v — must keep serving last-known state", res, err)
	}
	if _, err := r.Resolve(context.Background(), "org-1", "never-existed"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown model after outage err = %v, want ErrNotFound (state intact, not degraded)", err)
	}
}

// TestRegistry_UnknownModelNotFound: a synced registry with no such key is
// ErrNotFound (the proxy's generic 404-no-echo path), and org-scoping holds —
// another org's registration never resolves.
func TestRegistry_UnknownModelNotFound(t *testing.T) {
	r, _, _ := startedRegistry(t, registryCM("tf-model-1", nil))

	if _, err := r.Resolve(context.Background(), "org-1", "no-such-model"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown model err = %v, want ErrNotFound", err)
	}
	if _, err := r.Resolve(context.Background(), "org-2", "support-bot"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-org err = %v, want ErrNotFound", err)
	}
}

// TestRegistry_DuplicateKeyDeterministic: two ConfigMaps claiming one
// (org, served name) — impossible per Atlas's uniqueness constraint, but the
// resolver must not panic or flap: it serves the lexicographically-lowest
// resource_id on every lookup, and when the winner is deleted the survivor
// takes over.
func TestRegistry_DuplicateKeyDeterministic(t *testing.T) {
	a := registryCM("tf-model-a", map[string]string{"resource_id": "tfm-bbb", "graph_k8s_name": "graph-b"})
	b := registryCM("tf-model-b", map[string]string{"resource_id": "tfm-aaa", "graph_k8s_name": "graph-a"})
	r, client, _ := startedRegistry(t, a, b)

	for i := 0; i < 10; i++ {
		res, err := r.Resolve(context.Background(), "org-1", "support-bot")
		if err != nil {
			t.Fatalf("duplicate-claim Resolve: %v", err)
		}
		if res.ResourceID != "tfm-aaa" {
			t.Fatalf("duplicate pick = %s, want the lexicographically-lowest tfm-aaa (deterministic, no flap)", res.ResourceID)
		}
	}

	// Deleting the winner promotes the survivor — the claims map, not a
	// single overwritten row.
	if err := client.CoreV1().ConfigMaps(registryNS).Delete(context.Background(), "tf-model-b", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete winner: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		res, err := r.Resolve(context.Background(), "org-1", "support-bot")
		if err == nil && res.ResourceID == "tfm-bbb" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("survivor never took over: res=%+v err=%v", res, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestRegistry_InvalidRowsSkippedNeverServed: a registration missing a
// REQUIRED key (base_model — routable but unbillable) is never indexed, and
// an update that BREAKS a valid row de-indexes it (a stale claim must not
// keep serving what Atlas no longer asserts). Invalid port is recoverable:
// the row serves with Port 0 (proxy falls back to the configured port).
func TestRegistry_InvalidRowsSkippedNeverServed(t *testing.T) {
	broken := registryCM("tf-model-broken", map[string]string{"base_model": ""}) // deletes the key
	badPort := registryCM("tf-model-badport", map[string]string{"served_model_name": "porty", "port": "eighty"})
	r, client, _ := startedRegistry(t, broken, badPort)

	if _, err := r.Resolve(context.Background(), "org-1", "support-bot"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing-base_model row err = %v, want ErrNotFound (never indexed)", err)
	}
	res, ok := waitResolve(t, r, "org-1", "porty")
	if !ok || res.Port != 0 {
		t.Fatalf("bad-port row = %+v ok=%v, want served with Port 0 (config fallback)", res, ok)
	}

	// A valid row updated into an invalid one must stop resolving.
	fixed := registryCM("tf-model-broken", nil)
	if _, err := client.CoreV1().ConfigMaps(registryNS).Update(context.Background(), fixed, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update to valid: %v", err)
	}
	if _, ok := waitResolve(t, r, "org-1", "support-bot"); !ok {
		t.Fatal("fixed row must resolve")
	}
	rebroken := registryCM("tf-model-broken", map[string]string{"resource_id": ""})
	if _, err := client.CoreV1().ConfigMaps(registryNS).Update(context.Background(), rebroken, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update to invalid: %v", err)
	}
	if !waitNotFound(t, r, "org-1", "support-bot") {
		t.Fatal("row broken by update must be de-indexed")
	}
}
