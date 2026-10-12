package gateway

import (
	"context"
	"testing"
	"time"

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
