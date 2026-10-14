package gateway

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/config"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func waitCapacity(t *testing.T, r *RegistryResolver, graph string, ok func(GraphCapacity) bool) GraphCapacity {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c := r.GraphCapacity(graph)
		if ok(c) || time.Now().After(deadline) {
			return c
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func val(p *int64) int64 {
	if p == nil {
		return -1
	}
	return *p
}

func TestParseGraphCapacity(t *testing.T) {
	for _, tc := range []struct {
		name         string
		data         map[string]string
		active, kv   int64 // -1 = no limit
		prefill      int64
		wantProblems int
	}{
		{"nothing published", map[string]string{}, -1, -1, -1, 0},
		{"all published", map[string]string{
			"admission_max_active_requests": "12", "admission_max_reserved_kv_tokens": "400000", "admission_max_prefill_tokens": "65536",
		}, 12, 400000, 65536, 0},
		{"empty means no limit", map[string]string{"admission_max_active_requests": " "}, -1, -1, -1, 0},
		{"explicit zero is a zero cap", map[string]string{"admission_max_active_requests": "0"}, 0, -1, -1, 0},
		{"negative is ignored", map[string]string{"admission_max_active_requests": "-3"}, -1, -1, -1, 1},
		{"non-integer is ignored, others kept", map[string]string{
			"admission_max_active_requests": "12.5", "admission_max_prefill_tokens": "100",
		}, -1, -1, 100, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, problems := parseGraphCapacity(tc.data)
			if val(c.MaxActiveRequests) != tc.active || val(c.MaxReservedKVTokens) != tc.kv || val(c.MaxPrefillTokens) != tc.prefill {
				t.Fatalf("capacity=(%d,%d,%d)", val(c.MaxActiveRequests), val(c.MaxReservedKVTokens), val(c.MaxPrefillTokens))
			}
			if len(problems) != tc.wantProblems {
				t.Fatalf("problems=%v, want %d", problems, tc.wantProblems)
			}
		})
	}
}

// Capacity is graph-level and stamped on each tenant entry of the graph: the
// tightest published value wins, an update re-publishes, and deleting the
// last entry withdraws it. A malformed value never de-registers the model.
func TestRegistry_GraphCapacityLifecycle(t *testing.T) {
	a := registryCM("tf-model-a", map[string]string{"admission_max_active_requests": "10"})
	b := registryCM("tf-model-b", map[string]string{
		"served_model_name": "other", "resource_id": "tfm-2", "admission_max_active_requests": "8",
	})
	r, client, _ := startedRegistry(t, a, b)

	if c := waitCapacity(t, r, "graph-llama31", func(c GraphCapacity) bool { return val(c.MaxActiveRequests) == 8 }); val(c.MaxActiveRequests) != 8 {
		t.Fatalf("tightest across entries = %d, want 8", val(c.MaxActiveRequests))
	}
	if c := r.GraphCapacity("graph-other"); c.Any() {
		t.Fatal("another graph must publish nothing")
	}

	b.Data["admission_max_active_requests"] = "not-a-number"
	if _, err := client.CoreV1().ConfigMaps(registryNS).Update(context.Background(), b, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if c := waitCapacity(t, r, "graph-llama31", func(c GraphCapacity) bool { return val(c.MaxActiveRequests) == 10 }); val(c.MaxActiveRequests) != 10 {
		t.Fatalf("after b's value became malformed = %d, want a's 10", val(c.MaxActiveRequests))
	}
	if _, ok := waitResolve(t, r, "org-1", "other"); !ok {
		t.Fatal("a malformed capacity value must not de-register the model")
	}

	for _, name := range []string{"tf-model-a", "tf-model-b"} {
		if err := client.CoreV1().ConfigMaps(registryNS).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if c := waitCapacity(t, r, "graph-llama31", func(c GraphCapacity) bool { return !c.Any() }); c.Any() {
		t.Fatalf("capacity still published after every entry was deleted: %+v", c)
	}
}

// A ConfigMap that loses a duplicate (org, served name) claim does not serve
// traffic, so its capacity must not cap the graph: only the claimant
// resolveClaimants serves (lowest resource_id) publishes. Deleting the winner
// promotes the survivor's capacity along with its resolution.
func TestRegistry_GraphCapacityOnlyFromWinningClaimant(t *testing.T) {
	winner := registryCM("tf-model-winner", map[string]string{
		"resource_id": "tfm-aaa", "admission_max_active_requests": "10", "admission_max_prefill_tokens": "4096",
	})
	stale := registryCM("tf-model-stale", map[string]string{
		"resource_id": "tfm-bbb", "admission_max_active_requests": "2", "admission_max_prefill_tokens": "512",
	})
	r, client, _ := startedRegistry(t, winner, stale)

	if res, ok := waitResolve(t, r, "org-1", "support-bot"); !ok || res.ResourceID != "tfm-aaa" {
		t.Fatalf("resolution = %+v (ok=%v), want winner tfm-aaa", res, ok)
	}
	c := waitCapacity(t, r, "graph-llama31", func(c GraphCapacity) bool { return val(c.MaxActiveRequests) == 10 })
	if val(c.MaxActiveRequests) != 10 || val(c.MaxPrefillTokens) != 4096 {
		t.Fatalf("capacity = (active %d, prefill %d), want the winner's (10, 4096), not the losing duplicate's (2, 512)",
			val(c.MaxActiveRequests), val(c.MaxPrefillTokens))
	}

	// Scaling by ready workers (2 ready + 1) multiplies the winner's values,
	// not the losing duplicate's.
	r.WithReadyWorkers(fixedWorkers{"graph-llama31": 2})
	c = r.GraphCapacity("graph-llama31")
	if val(c.MaxActiveRequests) != 30 || val(c.MaxPrefillTokens) != 12288 {
		t.Fatalf("scaled capacity = (active %d, prefill %d), want the winner's x 3 (30, 12288), not the duplicate's (6, 1536)",
			val(c.MaxActiveRequests), val(c.MaxPrefillTokens))
	}

	if err := client.CoreV1().ConfigMaps(registryNS).Delete(context.Background(), "tf-model-winner", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	c = waitCapacity(t, r, "graph-llama31", func(c GraphCapacity) bool { return val(c.MaxActiveRequests) == 6 })
	if val(c.MaxActiveRequests) != 6 || val(c.MaxPrefillTokens) != 1536 {
		t.Fatalf("after the winner was deleted capacity = (active %d, prefill %d), want the survivor's x 3 (6, 1536)",
			val(c.MaxActiveRequests), val(c.MaxPrefillTokens))
	}
}

// GraphCapacity.Limits is the one conversion of published capacity into the
// graph_capacity admission scope. Every GraphCapacity field must reach the
// AdmissionLimits field of the same name, or a published key would engage
// admission (Any) without being enforced.
func TestGraphCapacityLimitsCopiesEveryField(t *testing.T) {
	var c GraphCapacity
	cv := reflect.ValueOf(&c).Elem()
	want := map[string]*int64{}
	for i := 0; i < cv.NumField(); i++ {
		p := new(int64)
		*p = int64(i + 1)
		cv.Field(i).Set(reflect.ValueOf(p))
		want[cv.Type().Field(i).Name] = p
	}
	limits := c.Limits()
	lv := reflect.ValueOf(limits)
	for name, p := range want {
		f := lv.FieldByName(name)
		if !f.IsValid() {
			t.Fatalf("config.AdmissionLimits has no field %s", name)
		}
		if got, _ := f.Interface().(*int64); got != p {
			t.Fatalf("Limits().%s = %v, want the GraphCapacity pointer %v", name, got, p)
		}
	}
	var set int
	for _, v := range config.LimitValues(limits) {
		if v != nil {
			set++
		}
	}
	if set != cv.NumField() {
		t.Fatalf("LimitValues(Limits()) has %d values set, want %d (one per GraphCapacity field)", set, cv.NumField())
	}
}

// capacityFields is the one table that Any, Limits, parseGraphCapacity and
// tightest walk. Every *int64 field of GraphCapacity must have exactly one
// entry, with a distinct non-empty registry key, whose limit accessor names
// the config.AdmissionLimits field of the same name. A field added to the
// struct without an entry would be silently never parsed, merged or enforced.
func TestCapacityFieldsCoverEveryGraphCapacityField(t *testing.T) {
	var c GraphCapacity
	cv := reflect.ValueOf(&c).Elem()
	byAddr := map[uintptr]string{}
	for i := 0; i < cv.NumField(); i++ {
		f := cv.Type().Field(i)
		if f.Type != reflect.TypeOf((*int64)(nil)) {
			continue
		}
		byAddr[cv.Field(i).Addr().Pointer()] = f.Name
	}
	var l config.AdmissionLimits
	lv := reflect.ValueOf(&l).Elem()
	covered := map[string]bool{}
	keys := map[string]bool{}
	for i, f := range capacityFields {
		name, ok := byAddr[reflect.ValueOf(f.get(&c)).Pointer()]
		if !ok {
			t.Fatalf("capacityFields[%d].get does not point at a GraphCapacity *int64 field", i)
		}
		if covered[name] {
			t.Fatalf("GraphCapacity.%s has more than one capacityFields entry", name)
		}
		covered[name] = true
		if f.key == "" || keys[f.key] {
			t.Fatalf("capacityFields[%d] (%s) has an empty or duplicate key %q", i, name, f.key)
		}
		keys[f.key] = true
		lf := lv.FieldByName(name)
		if !lf.IsValid() || reflect.ValueOf(f.limit(&l)).Pointer() != lf.Addr().Pointer() {
			t.Fatalf("capacityFields[%d].limit does not point at config.AdmissionLimits.%s", i, name)
		}
	}
	for _, name := range byAddr {
		if !covered[name] {
			t.Fatalf("GraphCapacity.%s has no capacityFields entry", name)
		}
	}
}
