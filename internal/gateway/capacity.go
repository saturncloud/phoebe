package gateway

import (
	"strconv"
	"strings"

	"github.com/saturncloud/phoebe/internal/config"
)

// GraphCapacity is the per-graph admission capacity Atlas publishes for a
// shared serving graph (B8, ruling 2026-10-12). Atlas chooses each value as
// the operator's override if one exists, else the profiled value times the
// install's safety margin, else nothing. R4 semantics: nil is no limit, an
// explicit 0 is a zero cap.
type GraphCapacity struct {
	MaxActiveRequests   *int64
	MaxReservedKVTokens *int64
	MaxPrefillTokens    *int64
	// PerWorker is true when the entry marks its values as per-worker
	// (capacityKeyUnit = capacityUnitPerWorker, Q22): the registry then
	// enforces them times the graph's ready workers plus the headroom
	// growthInfo.headroom allows (one, or zero when the graph cannot grow,
	// Q63). An entry
	// without the marker carries graph-level values (what Atlas published
	// before Q22), enforced as they are. Not a limit: tightest and Scaled
	// leave it false on their results.
	PerWorker bool
	// Disaggregated is true when the entry marks its graph as disaggregated
	// (capacityKeyTopology = capacityTopologyDisaggregated, Q50): each
	// per-worker value is then per worker of the pool that bounds it (Decode
	// for active requests and reserved KV, Prefill for prefill tokens), and
	// the registry scales it by that pool's ready workers plus that pool's
	// headroom (growthInfo.headroom, Q63). Only
	// meaningful with PerWorker. Not a limit: tightest and Scaled leave it
	// false on their results.
	Disaggregated bool
	// WorkerGrowth, PrefillGrowth and DecodeGrowth say whether the graph's
	// (aggregated) Worker pool, or each pool of a disaggregated graph, can
	// add a worker (Q63, Hugo 2026-10-19). They decide whether the one
	// worker of headroom applies (growthInfo.headroom). Not limits: tightest
	// and Scaled leave them zero on their results.
	WorkerGrowth  growthInfo
	PrefillGrowth growthInfo
	DecodeGrowth  growthInfo
}

// growthFor returns the growth facts of the pool that bounds a field: the
// disaggregated pool name, or the Worker pool of an aggregated graph.
func (c GraphCapacity) growthFor(pool string) growthInfo {
	if c.Disaggregated {
		switch pool {
		case PoolPrefill:
			return c.PrefillGrowth
		case PoolDecode:
			return c.DecodeGrowth
		}
		// No other pool exists on a disaggregated graph; an unknown one reads
		// as unknown growth (headroom kept), never as the aggregated keys,
		// which Atlas does not publish for a disaggregated graph.
		return growthInfo{}
	}
	return c.WorkerGrowth
}

// workerCapacity is Atlas's answer to "can one more worker of this pool's
// size be placed now" (admission_worker_capacity): unknown (absent, empty or
// unreadable), yes or no.
type workerCapacity int8

const (
	workerCapacityUnknown workerCapacity = iota
	workerCapacityYes
	workerCapacityNo
)

// growthInfo is what Atlas publishes about one pool's room to grow (Q63):
// the Planner's maximum workers (0 = unknown) and whether one more worker
// can be placed now.
type growthInfo struct {
	MaxWorkers int64
	Capacity   workerCapacity
}

// headroom is the workers' worth of headroom a pool with ready workers gets
// (Q63, amending Q4(b)/D50): GraphLimitHeadroomWorkers while the pool can
// actually add a worker, else 0, so a graph that cannot grow is never
// admitted past the slots its workers have.
//   - ready <= 0 keeps the headroom: a scaled-to-zero graph must let
//     requests through for the wake (a worker that cannot be placed fails
//     the wake on its own path).
//   - at or above the Planner maximum, or with Atlas reporting no room for
//     one more worker, there is no headroom.
//   - unknown (a key absent, empty or unreadable) keeps today's headroom:
//     unknown is a transient read failure, and dropping the headroom then
//     would also silence the Planner's scale-up signal (ruling: fail open
//     on quota-state loss).
//
// A "yes" is not a reservation: several graphs may each see yes for one
// free slot and each keep a worker of headroom, at most one worker per
// graph, the same as before Q63.
func (g growthInfo) headroom(ready int64) int64 {
	if ready <= 0 {
		return GraphLimitHeadroomWorkers
	}
	if g.MaxWorkers > 0 && ready >= g.MaxWorkers {
		return 0
	}
	if g.Capacity == workerCapacityNo {
		return 0
	}
	return GraphLimitHeadroomWorkers
}

// The registry data keys carrying GraphCapacity. They are PER-WORKER values
// (Q22, ruling 2026-10-14) stamped on every entry of the graph (each entry is
// one tenant's model on it); RegistryResolver.GraphCapacity multiplies them by
// the graph's ready workers plus one. All optional; empty or absent publishes
// no limit.
const (
	capacityKeyMaxActiveRequests   = "admission_max_active_requests"
	capacityKeyMaxReservedKVTokens = "admission_max_reserved_kv_tokens"
	capacityKeyMaxPrefillTokens    = "admission_max_prefill_tokens"

	// capacityKeyUnit says what the values above are. Only
	// capacityUnitPerWorker is defined; absent means graph-level values. No
	// rollout order over-admits. A phoebe that predates the marker ignores it
	// and enforces a per-worker value as the whole graph's limit (tighter,
	// never looser). This phoebe enforces an unmarked entry from an older
	// Atlas as it was published. Atlas first is not free, though: deploying
	// Atlas first, or an old Atlas replica republishing during a rolling
	// deploy (which drops the marker), holds a shared graph to one worker's
	// worth until both sides are new. Deploy order (D57): phoebe (with the
	// saturn-k8s RBAC change) first, then Atlas.
	capacityKeyUnit       = "admission_capacity_unit"
	capacityUnitPerWorker = "per_worker"

	// capacityKeyTopology says how the graph's workers are pooled (Q50,
	// 2026-10-17): absent or capacityTopologyAggregated keeps one Worker pool;
	// capacityTopologyDisaggregated scales each per-worker value by the pool
	// that bounds it. An internal Atlas-to-phoebe contract; both change in
	// lockstep.
	capacityKeyTopology           = "admission_topology"
	capacityTopologyAggregated    = "aggregated"
	capacityTopologyDisaggregated = "disaggregated"

	// Room to grow (Q63, 2026-10-19), per pool: the Planner's maximum workers
	// and whether one more worker can be placed now ("yes" or "no"; absent or
	// empty = unknown). The aggregated keys describe the Worker pool; a
	// disaggregated graph carries the _prefill and _decode pairs. Internal
	// Atlas-to-phoebe contract; absent keys keep the pre-Q63 headroom, so no
	// deploy order is needed.
	capacityKeyMaxWorkers     = "admission_max_workers"
	capacityKeyWorkerCapacity = "admission_worker_capacity"
	workerCapacityValueYes    = "yes"
	workerCapacityValueNo     = "no"
)

// growthKeySuffixes maps each pool's registry key suffix to its growth field.
var growthKeySuffixes = []struct {
	suffix string
	get    func(*GraphCapacity) *growthInfo
}{
	{"", func(c *GraphCapacity) *growthInfo { return &c.WorkerGrowth }},
	{"_" + PoolPrefill, func(c *GraphCapacity) *growthInfo { return &c.PrefillGrowth }},
	{"_" + PoolDecode, func(c *GraphCapacity) *growthInfo { return &c.DecodeGrowth }},
}

// parseGrowth reads the room-to-grow keys of one registry entry. A malformed
// value reads as unknown (which keeps the headroom) and is reported with the
// other malformed values.
func parseGrowth(d map[string]string, c *GraphCapacity) (bad []string) {
	for _, g := range growthKeySuffixes {
		info := g.get(c)
		maxKey := capacityKeyMaxWorkers + g.suffix
		if raw := strings.TrimSpace(d[maxKey]); raw != "" {
			v, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || v < 1 {
				bad = append(bad, maxKey+"="+strconv.Quote(raw))
			} else {
				info.MaxWorkers = v
			}
		}
		capKey := capacityKeyWorkerCapacity + g.suffix
		switch raw := strings.TrimSpace(d[capKey]); raw {
		case "":
		case workerCapacityValueYes:
			info.Capacity = workerCapacityYes
		case workerCapacityValueNo:
			info.Capacity = workerCapacityNo
		default:
			bad = append(bad, capKey+"="+strconv.Quote(raw))
		}
	}
	return bad
}

// capacityFields is the one list of GraphCapacity fields. Each entry names
// the registry key carrying the field, the field itself, and the
// config.AdmissionLimits field it becomes in the graph_capacity scope. Any,
// Limits, parseGraphCapacity and tightest all walk this table, so a new
// published limit is a struct field plus one entry here;
// TestCapacityFieldsCoverEveryGraphCapacityField fails when the entry is
// missing.
//
// pool is the disaggregated pool whose workers bound the field (Q50): a
// decode worker holds each sequence and its KV for the whole generation; the
// prompt tokens in prefill are what the prefill workers process.
var capacityFields = []struct {
	key   string
	pool  string
	get   func(*GraphCapacity) **int64
	limit func(*config.AdmissionLimits) **int64
}{
	{capacityKeyMaxActiveRequests, PoolDecode,
		func(c *GraphCapacity) **int64 { return &c.MaxActiveRequests },
		func(l *config.AdmissionLimits) **int64 { return &l.MaxActiveRequests }},
	{capacityKeyMaxReservedKVTokens, PoolDecode,
		func(c *GraphCapacity) **int64 { return &c.MaxReservedKVTokens },
		func(l *config.AdmissionLimits) **int64 { return &l.MaxReservedKVTokens }},
	{capacityKeyMaxPrefillTokens, PoolPrefill,
		func(c *GraphCapacity) **int64 { return &c.MaxPrefillTokens },
		func(l *config.AdmissionLimits) **int64 { return &l.MaxPrefillTokens }},
}

// Limits is the capacity as the admission limits of the graph_capacity scope.
func (c GraphCapacity) Limits() config.AdmissionLimits {
	var out config.AdmissionLimits
	for _, f := range capacityFields {
		*f.limit(&out) = *f.get(&c)
	}
	return out
}

// Any reports whether at least one limit is published.
func (c GraphCapacity) Any() bool {
	for _, f := range capacityFields {
		if *f.get(&c) != nil {
			return true
		}
	}
	return false
}

// parseGraphCapacity reads the capacity keys of one registry entry and
// reports the two kinds of Atlas producer bug separately, because phoebe
// handles them in opposite directions:
//
//   - badValues lists limit values that are not non-negative integers. Each
//     is treated as unpublished (no limit) rather than rejecting the entry,
//     because refusing the registration would take the tenant's model
//     offline over a missing capacity hint (the same direction as having no
//     profile at all).
//   - badGrowth lists malformed room-to-grow keys (admission_max_workers*
//     below 1 or not an integer, admission_worker_capacity* other than yes
//     or no; Q63). Each reads as unknown, which keeps one worker of
//     headroom (D65); the limits themselves are still enforced.
//   - badUnit names an unknown admission_capacity_unit. The entry's values
//     are kept and enforced as published, as graph-level limits (PerWorker
//     stays false): that is the tighter reading of a per-worker value and
//     the published reading of a graph-level one (D55(a)).
func parseGraphCapacity(d map[string]string) (c GraphCapacity, badValues []string, badUnit string, badGrowth []string) {
	for _, f := range capacityFields {
		key := f.key
		raw := strings.TrimSpace(d[key])
		if raw == "" {
			continue
		}
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			badValues = append(badValues, key+"="+strconv.Quote(raw))
			continue
		}
		*f.get(&c) = &v
	}
	badGrowth = parseGrowth(d, &c)
	switch unit := strings.TrimSpace(d[capacityKeyUnit]); unit {
	case "":
	case capacityUnitPerWorker:
		c.PerWorker = true
	default:
		badUnit = capacityKeyUnit + "=" + strconv.Quote(unit)
	}
	// An unknown topology is reported like an unknown unit and read the same
	// tight way: the values are enforced as published, as graph-level limits.
	switch topology := strings.TrimSpace(d[capacityKeyTopology]); topology {
	case "", capacityTopologyAggregated:
	case capacityTopologyDisaggregated:
		c.Disaggregated = true
	default:
		c.PerWorker = false
		if badUnit == "" {
			badUnit = capacityKeyTopology + "=" + strconv.Quote(topology)
		}
	}
	return c, badValues, badUnit, badGrowth
}

// tightest combines the capacity published on several entries of one graph:
// for each limit the smallest published value wins. Entries of one graph
// should agree (Atlas stamps one per-worker value on all of them); during a
// change they briefly may not, and the tighter value is the safe reading.
func tightest(all map[string]GraphCapacity) GraphCapacity {
	var out GraphCapacity
	pick := func(dst **int64, v *int64) {
		if v != nil && (*dst == nil || *v < **dst) {
			x := *v
			*dst = &x
		}
	}
	for _, c := range all {
		for _, f := range capacityFields {
			pick(f.get(&out), *f.get(&c))
		}
	}
	return out
}
