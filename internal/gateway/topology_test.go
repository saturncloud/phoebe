package gateway

import (
	"context"
	"strings"
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
	if _, err := cms.Create(ctx, topologyCM("tf-graph-topology-graph-a", map[string]string{
		"graph_k8s_name": "graph-a", "topology_revision": "2", "region": "eu-north1",
	}), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cms.Create(ctx, topologyCM("tf-graph-topology-graph-bad", map[string]string{
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

	if _, err := cms.Update(ctx, topologyCM("tf-graph-topology-graph-a", map[string]string{
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

	if err := cms.Delete(ctx, "tf-graph-topology-graph-a", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitTopology(t, idx, "graph-a", false)
}

func TestParseTopologyConfigMap(t *testing.T) {
	ok := func(rev int, region string) *Topology { return &Topology{Revision: rev, Region: region} }
	for _, tc := range []struct {
		name          string
		cmName        string // default tf-graph-topology-g
		data          map[string]string
		want          *Topology // nil = the entry is rejected
		regionDropped bool
	}{
		{"valid", "", map[string]string{"graph_k8s_name": "g", "topology_revision": "1"}, ok(1, ""), false},
		{"missing graph", "", map[string]string{"topology_revision": "1"}, nil, false},
		{"name does not match graph", "copy-of-g", map[string]string{"graph_k8s_name": "g", "topology_revision": "1"}, nil, false},
		{"revision zero", "", map[string]string{"graph_k8s_name": "g", "topology_revision": "0"}, nil, false},
		{"revision negative", "", map[string]string{"graph_k8s_name": "g", "topology_revision": "-1"}, nil, false},
		{"revision not a number", "", map[string]string{"graph_k8s_name": "g", "topology_revision": "x"}, nil, false},
		{"revision at int32 max", "", map[string]string{"graph_k8s_name": "g", "topology_revision": "2147483647"}, ok(2147483647, ""), false},
		{"revision above int32 max", "", map[string]string{"graph_k8s_name": "g", "topology_revision": "2147483648"}, nil, false},
		{"revision overflows int32", "", map[string]string{"graph_k8s_name": "g", "topology_revision": "3000000000"}, nil, false},
		{"multibyte region of 64 characters", "", map[string]string{"graph_k8s_name": "g", "topology_revision": "1", "region": strings.Repeat("é", 64)}, ok(1, strings.Repeat("é", 64)), false},
		{"region too long keeps revision", "", map[string]string{"graph_k8s_name": "g", "topology_revision": "1", "region": strings.Repeat("a", 65)}, ok(1, ""), true},
		{"region invalid utf8 keeps revision", "", map[string]string{"graph_k8s_name": "g", "topology_revision": "1", "region": "\xff"}, ok(1, ""), true},
	} {
		name := tc.cmName
		if name == "" {
			name = "tf-graph-topology-g"
		}
		graph, topo, dropped, err := parseTopologyConfigMap(topologyCM(name, tc.data))
		if tc.want == nil {
			if err == nil {
				t.Errorf("%s: accepted %+v, want rejected", tc.name, topo)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: err = %v, want accepted", tc.name, err)
			continue
		}
		if graph != "g" || topo != *tc.want || dropped != tc.regionDropped {
			t.Errorf("%s: got (%q, %+v, dropped=%t), want (g, %+v, dropped=%t)", tc.name, graph, topo, dropped, *tc.want, tc.regionDropped)
		}
	}
}

// TestTopologyIndex_MisnamedDuplicateNeverUnindexesTheGraph: only the
// ConfigMap named tf-graph-topology-<graph> may index a graph, so a copy
// naming the same graph is ignored, and creating then deleting it leaves the
// real entry in place.
func TestTopologyIndex_MisnamedDuplicateNeverUnindexesTheGraph(t *testing.T) {
	client := fake.NewSimpleClientset()
	cms := client.CoreV1().ConfigMaps(registryNS)
	ctx := context.Background()
	if _, err := cms.Create(ctx, topologyCM("tf-graph-topology-graph-a", map[string]string{
		"graph_k8s_name": "graph-a", "topology_revision": "2", "region": "eu-north1",
	}), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	idx := NewTopologyIndex(client, registryNS, logging.New(logging.ERROR))
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go idx.Run(runCtx)
	want := Topology{Revision: 2, Region: "eu-north1"}
	if got := waitTopology(t, idx, "graph-a", true); got != want {
		t.Fatalf("graph-a = %+v", got)
	}

	if _, err := cms.Create(ctx, topologyCM("copy-of-graph-a", map[string]string{
		"graph_k8s_name": "graph-a", "topology_revision": "9",
	}), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitInformerHas(t, idx, "copy-of-graph-a", true)
	if got, _ := idx.Lookup("graph-a"); got != want {
		t.Fatalf("after misnamed copy added, graph-a = %+v, want %+v", got, want)
	}
	if err := cms.Delete(ctx, "copy-of-graph-a", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitInformerHas(t, idx, "copy-of-graph-a", false)
	if got, ok := idx.Lookup("graph-a"); !ok || got != want {
		t.Fatalf("after misnamed copy deleted, graph-a = %+v ok=%t, want %+v", got, ok, want)
	}
}

// waitInformerHas waits until the informer's store has (or lacks) the named
// ConfigMap, which means its event handler has already run.
func waitInformerHas(t *testing.T, idx *TopologyIndex, name string, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, exists, _ := idx.informer.GetStore().GetByKey(registryNS + "/" + name)
		if exists == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("informer store has %q never reached %t", name, want)
}

// TestTopologyIndex_WaitSyncedIndexesTheInitialList: after WaitSynced
// returns true, the initial list is indexed, so the first requests after a
// pod start carry their revision instead of 0.
func TestTopologyIndex_WaitSyncedIndexesTheInitialList(t *testing.T) {
	client := fake.NewSimpleClientset(topologyCM("tf-graph-topology-graph-a", map[string]string{
		"graph_k8s_name": "graph-a", "topology_revision": "4",
	}))
	idx := NewTopologyIndex(client, registryNS, logging.New(logging.ERROR))
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go idx.Run(runCtx)
	wctx, wcancel := context.WithTimeout(runCtx, 5*time.Second)
	defer wcancel()
	if !idx.WaitSynced(wctx) {
		t.Fatal("WaitSynced = false, want true")
	}
	if got, ok := idx.Lookup("graph-a"); !ok || got.Revision != 4 {
		t.Fatalf("Lookup right after sync = %+v ok=%t, want revision 4", got, ok)
	}
}

func TestTopologyIndex_NilLookupIsAMiss(t *testing.T) {
	var idx *TopologyIndex
	if _, ok := idx.Lookup("g"); ok {
		t.Fatal("nil index returned a topology")
	}
}
