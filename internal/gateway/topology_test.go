package gateway

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/saturncloud/phoebe/internal/logging"
)

func topologyCM(name string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: registryNS,
			Labels:    map[string]string{"saturncloud.io/tf-graph-topology": "true"},
		},
		Data: data,
	}
}

func waitTopology(t *testing.T, idx *TopologyIndex, graph string, want bool) Topology {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if topo, ok := idx.Lookup(graph); ok == want {
			return topo
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("Lookup(%q) never reached ok=%t", graph, want)
	return Topology{}
}

// TestTopologyIndex_FollowsTheAtlasEntries: entries are indexed by graph,
// updates replace them, deletes remove them, and malformed entries are
// ignored rather than indexed.
func TestTopologyIndex_FollowsTheAtlasEntries(t *testing.T) {
	client := fake.NewSimpleClientset()
	cms := client.CoreV1().ConfigMaps(registryNS)
	ctx := context.Background()
	if _, err := cms.Create(ctx, topologyCM("tf-graph-topology-a", map[string]string{
		"graph_k8s_name": "graph-a", "topology_revision": "2", "region": "eu-north1",
	}), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cms.Create(ctx, topologyCM("tf-graph-topology-bad", map[string]string{
		"graph_k8s_name": "graph-bad", "topology_revision": "zero",
	}), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	idx := NewTopologyIndex(client, registryNS, logging.New(logging.ERROR))
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go idx.Run(runCtx)

	if got := waitTopology(t, idx, "graph-a", true); got != (Topology{Revision: 2, Region: "eu-north1"}) {
		t.Fatalf("graph-a = %+v", got)
	}
	if _, ok := idx.Lookup("graph-bad"); ok {
		t.Fatal("malformed entry was indexed")
	}

	if _, err := cms.Update(ctx, topologyCM("tf-graph-topology-a", map[string]string{
		"graph_k8s_name": "graph-a", "topology_revision": "3", "region": "",
	}), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got, _ := idx.Lookup("graph-a"); got.Revision == 3 {
			if got.Region != "" {
				t.Fatalf("region = %q, want empty", got.Region)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("update never reached the index")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := cms.Delete(ctx, "tf-graph-topology-a", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitTopology(t, idx, "graph-a", false)
}

func TestParseTopologyConfigMap(t *testing.T) {
	for _, tc := range []struct {
		name string
		data map[string]string
		ok   bool
	}{
		{"valid", map[string]string{"graph_k8s_name": "g", "topology_revision": "1"}, true},
		{"missing graph", map[string]string{"topology_revision": "1"}, false},
		{"revision zero", map[string]string{"graph_k8s_name": "g", "topology_revision": "0"}, false},
		{"revision not a number", map[string]string{"graph_k8s_name": "g", "topology_revision": "x"}, false},
		{"region too long", map[string]string{"graph_k8s_name": "g", "topology_revision": "1", "region": string(make([]byte, 65))}, false},
	} {
		_, _, err := parseTopologyConfigMap(topologyCM("cm", tc.data))
		if (err == nil) != tc.ok {
			t.Errorf("%s: err = %v, want ok=%t", tc.name, err, tc.ok)
		}
	}
}

func TestTopologyIndex_NilLookupIsAMiss(t *testing.T) {
	var idx *TopologyIndex
	if _, ok := idx.Lookup("g"); ok {
		t.Fatal("nil index returned a topology")
	}
}
