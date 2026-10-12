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

// stampServing fills the platform-set serving fields of e: the graph's
// published topology and the request's start time and duration on phoebe's
// clock. Owner ruling 2026-10-12: topology, region and serving time come from
// the platform, never from what the engine echoes back.
func (s *Server) stampServing(ctx context.Context, e *metering.Event) {
	if s.topology != nil {
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
