package proxy

import (
	"context"
	"time"

	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/metering"
)

// TopologyLookup resolves the topology Atlas published for a graph
// (gateway.TopologyIndex). The values only annotate usage events; a miss
// leaves the event's topology fields empty and changes nothing else.
type TopologyLookup interface {
	Lookup(graph string) (gateway.Topology, bool)
}

// WithTopology stamps each usage event with its graph's topology revision and
// region from lookup. nil leaves them empty.
func (s *Server) WithTopology(lookup TopologyLookup) *Server {
	s.topology = lookup
	return s
}

type requestStartKey struct{}

// withRequestStart records when phoebe received the request. Every emit runs
// on a context derived from the request's (the outbound clones and
// context.WithoutCancel keep values), so it reads the instant back.
func withRequestStart(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, requestStartKey{}, t)
}

type topologySnapshotKey struct{}

// topologySnapshot is the graph's topology as published when phoebe resolved
// the serving graph for the request. ok=false records a miss at that instant.
type topologySnapshot struct {
	graph string
	topo  gateway.Topology
	ok    bool
}

// withTopologySnapshot records the serving graph's topology at request time,
// so a revision published (or a ConfigMap deleted) while the request is in
// flight never relabels the tokens the earlier topology served.
func withTopologySnapshot(ctx context.Context, graph string, topo gateway.Topology, ok bool) context.Context {
	return context.WithValue(ctx, topologySnapshotKey{}, topologySnapshot{graph: graph, topo: topo, ok: ok})
}

// stampServing fills the platform-set serving fields of e: the graph's
// published topology and the request's start time and duration on phoebe's
// clock. Owner ruling 2026-10-12: topology, region and serving time come from
// the platform, never from what the engine echoes back.
//
// The topology is the one snapshotted when the request's graph was resolved
// (handleProxy), not the one current at emit time: a long stream or a cold
// wake can outlive a revision change or the graph's teardown. Only an emit
// with no snapshot (an early return before the graph was resolved) looks the
// graph up now.
func (s *Server) stampServing(ctx context.Context, e *metering.Event) {
	if snap, ok := ctx.Value(topologySnapshotKey{}).(topologySnapshot); ok && snap.graph == e.GraphK8sName {
		if snap.ok {
			e.TopologyRevision = snap.topo.Revision
			e.Region = snap.topo.Region
		}
	} else if s.topology != nil {
		if topo, ok := s.topology.Lookup(e.GraphK8sName); ok {
			e.TopologyRevision = topo.Revision
			e.Region = topo.Region
		}
	}
	if start, ok := ctx.Value(requestStartKey{}).(time.Time); ok && !start.IsZero() {
		e.RequestStartedUnixMs = start.UnixMilli()
		if d := time.Since(start); d > 0 {
			e.ServingDurationMs = d.Milliseconds()
		}
	}
}
