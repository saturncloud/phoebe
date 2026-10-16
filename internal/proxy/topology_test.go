package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/metering"
)

type mapTopology map[string]gateway.Topology

func (m mapTopology) Lookup(graph string) (gateway.Topology, bool) {
	t, ok := m[graph]
	return t, ok
}

// TestTopology_GatewayEventCarriesPlatformSetServingFields: a gateway request
// is stamped with its graph's published revision and region and with phoebe's
// own start time and duration (owner ruling 2026-10-12: platform-set, never
// engine-reported).
func TestTopology_GatewayEventCarriesPlatformSetServingFields(t *testing.T) {
	be, beURL := usageBackend(t)
	defer be.Close()

	resolver := &mapResolver{m: map[[2]string]gateway.Resolution{
		{"org-1", "m"}: {ResourceID: "tfm-1", BaseModel: "m", ServingMode: "shared", GraphK8sName: "graph-a"},
	}}
	em := &recordingEmitter{}
	srv := newGatewayTestServer(t, em, resolver, beURL).
		WithTopology(mapTopology{"graph-a": {Revision: 4, Region: "eu-north1"}})

	before := time.Now().UnixMilli()
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, gatewayRequest("org-1", `{"model":"m"}`))
	after := time.Now().UnixMilli()
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", rr.Code, rr.Body.String())
	}
	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want 1", len(events))
	}
	e := events[0]
	if e.TopologyRevision != 4 || e.Region != "eu-north1" {
		t.Fatalf("topology = (%d, %q), want (4, eu-north1)", e.TopologyRevision, e.Region)
	}
	if e.RequestStartedUnixMs < before || e.RequestStartedUnixMs > after {
		t.Fatalf("request start %d outside [%d, %d]", e.RequestStartedUnixMs, before, after)
	}
	if e.ServingDurationMs < 0 || e.ServingDurationMs > after-before {
		t.Fatalf("serving duration %d outside [0, %d]", e.ServingDurationMs, after-before)
	}
}

// TestTopology_MissingEntryLeavesFieldsEmptyAndStillMeters: no published
// entry for the graph changes nothing but the topology fields.
func TestTopology_MissingEntryLeavesFieldsEmptyAndStillMeters(t *testing.T) {
	be, beURL := usageBackend(t)
	defer be.Close()

	resolver := &mapResolver{m: map[[2]string]gateway.Resolution{
		{"org-1", "m"}: {ResourceID: "tfm-1", BaseModel: "m", ServingMode: "shared", GraphK8sName: "graph-a"},
	}}
	em := &recordingEmitter{}
	srv := newGatewayTestServer(t, em, resolver, beURL).WithTopology(mapTopology{})

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, gatewayRequest("org-1", `{"model":"m"}`))
	events := em.waitForEvents(1, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("emitted %d events, want 1", len(events))
	}
	e := events[0]
	if e.TopologyRevision != 0 || e.Region != "" {
		t.Fatalf("topology = (%d, %q), want empty", e.TopologyRevision, e.Region)
	}
	if e.PromptTokens != 7 || e.RequestStartedUnixMs == 0 {
		t.Fatalf("event not metered as usual: %+v", e)
	}
}

func TestStampServing_NoStartInContextLeavesTimesEmpty(t *testing.T) {
	s := &Server{}
	e := metering.Event{GraphK8sName: "g"}
	s.stampServing(context.Background(), &e)
	if e.RequestStartedUnixMs != 0 || e.ServingDurationMs != 0 || e.TopologyRevision != 0 {
		t.Fatalf("stamped without inputs: %+v", e)
	}
}

// changingTopology answers the first lookup with first and every later one
// with later (ok=laterOK): a revision published, or the ConfigMap deleted,
// while the request is in flight.
type changingTopology struct {
	mu      sync.Mutex
	calls   int
	first   gateway.Topology
	later   gateway.Topology
	laterOK bool
}

func (c *changingTopology) Lookup(string) (gateway.Topology, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls == 1 {
		return c.first, true
	}
	return c.later, c.laterOK
}

// TestTopology_EventCarriesTopologyAtRequestStartNotAtEmit: the event records
// the topology that was published when the request's graph was resolved. A
// new revision published, or the graph's entry deleted, before the emit must
// not relabel the tokens the earlier topology served.
func TestTopology_EventCarriesTopologyAtRequestStartNotAtEmit(t *testing.T) {
	cases := []struct {
		name    string
		later   gateway.Topology
		laterOK bool
	}{
		{name: "new revision published mid-request", later: gateway.Topology{Revision: 5, Region: "us-central1"}, laterOK: true},
		{name: "entry deleted mid-request", laterOK: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			be, beURL := usageBackend(t)
			defer be.Close()

			resolver := &mapResolver{m: map[[2]string]gateway.Resolution{
				{"org-1", "m"}: {ResourceID: "tfm-1", BaseModel: "m", ServingMode: "shared", GraphK8sName: "graph-a"},
			}}
			em := &recordingEmitter{}
			topo := &changingTopology{
				first:   gateway.Topology{Revision: 4, Region: "eu-north1"},
				later:   tc.later,
				laterOK: tc.laterOK,
			}
			srv := newGatewayTestServer(t, em, resolver, beURL).WithTopology(topo)

			rr := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rr, gatewayRequest("org-1", `{"model":"m"}`))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 (body %q)", rr.Code, rr.Body.String())
			}
			events := em.waitForEvents(1, 2*time.Second)
			if len(events) != 1 {
				t.Fatalf("emitted %d events, want 1", len(events))
			}
			if e := events[0]; e.TopologyRevision != 4 || e.Region != "eu-north1" {
				t.Fatalf("topology = (%d, %q), want the request-time (4, eu-north1)", e.TopologyRevision, e.Region)
			}
		})
	}
}

// TestStampServing_SnapshotMissIsNotRelookedUp: a miss recorded at request
// time stays a miss; the emit does not pick up an entry published later.
func TestStampServing_SnapshotMissIsNotRelookedUp(t *testing.T) {
	s := &Server{topology: mapTopology{"g": {Revision: 9, Region: "r"}}}
	ctx := withTopologySnapshot(context.Background(), "g", gateway.Topology{}, false)
	e := metering.Event{GraphK8sName: "g"}
	s.stampServing(ctx, &e)
	if e.TopologyRevision != 0 || e.Region != "" {
		t.Fatalf("topology = (%d, %q), want the request-time miss", e.TopologyRevision, e.Region)
	}
}

// TestStampServing_NoSnapshotFallsBackToLookup: an emit before the graph was
// resolved (no snapshot) still looks the graph up at emit time.
func TestStampServing_NoSnapshotFallsBackToLookup(t *testing.T) {
	s := &Server{topology: mapTopology{"g": {Revision: 9, Region: "r"}}}
	e := metering.Event{GraphK8sName: "g"}
	s.stampServing(context.Background(), &e)
	if e.TopologyRevision != 9 || e.Region != "r" {
		t.Fatalf("topology = (%d, %q), want (9, r)", e.TopologyRevision, e.Region)
	}
}
