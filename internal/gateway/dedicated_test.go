package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// dedicatedCM builds a dedicated endpoint row per the #71 contract. An empty
// override value deletes the key.
func dedicatedCM(name string, data map[string]string) *corev1.ConfigMap {
	base := map[string]string{
		"serving_mode":      "dedicated",
		"org_id":            "org-1",
		"served_model_name": "my-endpoint",
		"resource_id":       "dep-1",
		"base_model":        "Qwen/Qwen3-8B",
		"adapter":           "",
		"upstream":          "pd-tf-x-dep-1.org-ns.svc.cluster.local:8000",
		"serving_profile":   "aggregated",
		"video_limits":      "",
		"state":             "running",
		"cluster":           "hub",
		"org_host_url":      "https://acme-inference.example.com/v1",
	}
	for k, v := range data {
		if v == "" {
			delete(base, k)
			continue
		}
		base[k] = v
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: registryNS,
			Labels: map[string]string{
				RegistryLabel:          "true",
				DedicatedEndpointLabel: "true",
			},
		},
		Data: base,
	}
}

func waitDedicated(t *testing.T, r *RegistryResolver, org, model string, want bool) DedicatedEndpoint {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ep, err := r.ResolveDedicated(context.Background(), org, model)
		if want && err == nil {
			return ep
		}
		if !want && errors.Is(err, ErrNotFound) {
			return DedicatedEndpoint{}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("ResolveDedicated(%s,%s) never reached want=%v", org, model, want)
	return DedicatedEndpoint{}
}

func TestDedicatedRowResolvesWithEveryField(t *testing.T) {
	r, _, _ := startedRegistry(t, dedicatedCM("tf-endpoint-dep-1", map[string]string{
		"served_model_aliases": "my-endpoint-base, ,my-endpoint",
		"adapter":              "ckpt-1",
		"video_limits":         `{"sizes":["1280x720"]}`,
	}))
	ep, err := r.ResolveDedicated(context.Background(), "org-1", "my-endpoint")
	if err != nil {
		t.Fatal(err)
	}
	if ep.ResourceID != "dep-1" || ep.BaseModel != "Qwen/Qwen3-8B" || ep.Adapter != "ckpt-1" ||
		ep.Upstream == "" || ep.ForwardURL != "" || ep.ServingProfile != "aggregated" ||
		ep.VideoLimits == "" || ep.State != "running" || ep.OrgHostURL == "" || ep.Cluster != "hub" {
		t.Fatalf("row = %+v", ep)
	}
	if len(ep.Aliases) != 1 || ep.Aliases[0] != "my-endpoint-base" {
		t.Fatalf("aliases = %v, want [my-endpoint-base] (blank and self dropped)", ep.Aliases)
	}
	if _, err := r.ResolveDedicated(context.Background(), "org-1", "my-endpoint-base"); err != nil {
		t.Fatalf("alias: %v", err)
	}
}

// The tier boundary inside the registry: a dedicated row is never a shared
// resolution, and a shared row is never a dedicated one.
func TestDedicatedAndSharedRowsNeverCross(t *testing.T) {
	r, _, _ := startedRegistry(t,
		dedicatedCM("tf-endpoint-dep-1", map[string]string{"served_model_name": "name-a"}),
		registryCM("tf-model-1", map[string]string{"served_model_name": "name-b"}),
	)
	if _, err := r.Resolve(context.Background(), "org-1", "name-a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("shared Resolve of a dedicated row: err=%v, want ErrNotFound", err)
	}
	if _, err := r.ResolveDedicated(context.Background(), "org-1", "name-b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolveDedicated of a shared row: err=%v, want ErrNotFound", err)
	}
}

func TestDedicatedLookupIsOrgScoped(t *testing.T) {
	r, _, _ := startedRegistry(t, dedicatedCM("tf-endpoint-dep-1", nil))
	if _, err := r.ResolveDedicated(context.Background(), "org-2", "my-endpoint"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other org: err=%v, want ErrNotFound", err)
	}
}

func TestDedicatedExactNameWinsOverAlias(t *testing.T) {
	r, _, _ := startedRegistry(t,
		dedicatedCM("tf-endpoint-legacy", map[string]string{
			"served_model_name": "foo", "resource_id": "dep-a", "served_model_aliases": "foo-base",
		}),
		dedicatedCM("tf-endpoint-other", map[string]string{
			"served_model_name": "foo-base", "resource_id": "dep-z",
		}),
	)
	ep, err := r.ResolveDedicated(context.Background(), "org-1", "foo-base")
	if err != nil || ep.ResourceID != "dep-z" {
		t.Fatalf("foo-base resolved to %+v (%v), want the endpoint named foo-base (dep-z)", ep, err)
	}
}

func TestDedicatedInvalidRowsAreNotIndexed(t *testing.T) {
	cases := map[string]map[string]string{
		"no org":             {"org_id": ""},
		"no name":            {"served_model_name": ""},
		"no resource":        {"resource_id": ""},
		"no base model":      {"base_model": ""},
		"shared mode":        {"serving_mode": "shared"},
		"bad state":          {"state": "paused"},
		"no state":           {"state": ""},
		"both targets":       {"forward_url": "https://acme-inference.spoke.example.com"},
		"no target":          {"upstream": ""},
		"http forward":       {"upstream": "", "forward_url": "http://acme-inference.spoke.example.com"},
		"forward with path":  {"upstream": "", "forward_url": "https://acme-inference.spoke.example.com/v1"},
		"forward with query": {"upstream": "", "forward_url": "https://h.example.com?x=1"},
		"forward with user":  {"upstream": "", "forward_url": "https://u:p@h.example.com"},
	}
	for name, override := range cases {
		t.Run(name, func(t *testing.T) {
			r, _, _ := startedRegistry(t, dedicatedCM("tf-endpoint-dep-1", override))
			model := "my-endpoint"
			if _, err := r.ResolveDedicated(context.Background(), "org-1", model); !errors.Is(err, ErrNotFound) {
				t.Fatalf("invalid row indexed: err=%v", err)
			}
		})
	}
}

func TestDedicatedForwardCopyParses(t *testing.T) {
	r, _, _ := startedRegistry(t, dedicatedCM("tf-endpoint-dep-1", map[string]string{
		"upstream": "", "forward_url": "https://acme-inference.spoke.example.com/", "cluster": "spoke",
	}))
	ep, err := r.ResolveDedicated(context.Background(), "org-1", "my-endpoint")
	if err != nil || !ep.Forwards() || ep.ForwardURL != "https://acme-inference.spoke.example.com" {
		t.Fatalf("forward copy = %+v (%v)", ep, err)
	}
}

func TestDedicatedLiveUpdateAndDelete(t *testing.T) {
	r, client, _ := startedRegistry(t, dedicatedCM("tf-endpoint-dep-1", nil))
	cm := dedicatedCM("tf-endpoint-dep-1", map[string]string{"state": "stopped"})
	if _, err := client.CoreV1().ConfigMaps(registryNS).Update(context.Background(), cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ep, _ := r.ResolveDedicated(context.Background(), "org-1", "my-endpoint"); ep.State == "stopped" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if ep, _ := r.ResolveDedicated(context.Background(), "org-1", "my-endpoint"); ep.State != "stopped" {
		t.Fatalf("update not applied: %+v", ep)
	}
	if err := client.CoreV1().ConfigMaps(registryNS).Delete(context.Background(), cm.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitDedicated(t, r, "org-1", "my-endpoint", false)
	if rows, _ := r.ListDedicated(context.Background(), "org-1"); len(rows) != 0 {
		t.Fatalf("list after delete = %v", rows)
	}
}

func TestDedicatedListIsSortedAndOrgScoped(t *testing.T) {
	r, _, _ := startedRegistry(t,
		dedicatedCM("tf-endpoint-b", map[string]string{"served_model_name": "zeta", "resource_id": "dep-b"}),
		dedicatedCM("tf-endpoint-a", map[string]string{"served_model_name": "alpha", "resource_id": "dep-a"}),
		dedicatedCM("tf-endpoint-c", map[string]string{"served_model_name": "other", "resource_id": "dep-c", "org_id": "org-2"}),
	)
	rows, err := r.ListDedicated(context.Background(), "org-1")
	if err != nil || len(rows) != 2 || rows[0].Name != "alpha" || rows[1].Name != "zeta" {
		t.Fatalf("list = %+v (%v)", rows, err)
	}
}

func TestDedicatedNotSyncedFailsClosed(t *testing.T) {
	r := &RegistryResolver{dedicated: newDedicatedIndex()}
	if _, err := r.ResolveDedicated(context.Background(), "o", "m"); !errors.Is(err, ErrNotSynced) {
		t.Fatalf("err=%v, want ErrNotSynced", err)
	}
	if _, err := r.ListDedicated(context.Background(), "o"); !errors.Is(err, ErrNotSynced) {
		t.Fatalf("err=%v, want ErrNotSynced", err)
	}
}

// Two different endpoints of one org claiming one name (a stale row after a
// failed delete, then the name reused) is refused, never served from either.
func TestDedicatedDuplicateNameOfTwoEndpointsIsAmbiguous(t *testing.T) {
	r, _, _ := startedRegistry(t,
		dedicatedCM("tf-endpoint-old", map[string]string{"resource_id": "dep-a"}),
		dedicatedCM("tf-endpoint-new", map[string]string{"resource_id": "dep-b"}),
	)
	if _, err := r.ResolveDedicated(context.Background(), "org-1", "my-endpoint"); !errors.Is(err, ErrAmbiguousDedicated) {
		t.Fatalf("err=%v, want ErrAmbiguousDedicated", err)
	}
}

// Two rows of the same endpoint (one resource id) are one claimant.
func TestDedicatedTwoRowsOfOneEndpointResolve(t *testing.T) {
	r, _, _ := startedRegistry(t,
		dedicatedCM("tf-endpoint-a", map[string]string{"resource_id": "dep-a"}),
		dedicatedCM("tf-endpoint-b", map[string]string{"resource_id": "dep-a"}),
	)
	ep, err := r.ResolveDedicated(context.Background(), "org-1", "my-endpoint")
	if err != nil || ep.ResourceID != "dep-a" {
		t.Fatalf("ep=%+v err=%v", ep, err)
	}
}
