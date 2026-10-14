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
	// enforces them times the graph's ready workers plus one. An entry
	// without the marker carries graph-level values (what Atlas published
	// before Q22), enforced as they are. Not a limit: tightest and Scaled
	// leave it false on their results.
	PerWorker bool
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
)

// capacityFields is the one list of GraphCapacity fields. Each entry names
// the registry key carrying the field, the field itself, and the
// config.AdmissionLimits field it becomes in the graph_capacity scope. Any,
// Limits, parseGraphCapacity and tightest all walk this table, so a new
// published limit is a struct field plus one entry here;
// TestCapacityFieldsCoverEveryGraphCapacityField fails when the entry is
// missing.
var capacityFields = []struct {
	key   string
	get   func(*GraphCapacity) **int64
	limit func(*config.AdmissionLimits) **int64
}{
	{capacityKeyMaxActiveRequests,
		func(c *GraphCapacity) **int64 { return &c.MaxActiveRequests },
		func(l *config.AdmissionLimits) **int64 { return &l.MaxActiveRequests }},
	{capacityKeyMaxReservedKVTokens,
		func(c *GraphCapacity) **int64 { return &c.MaxReservedKVTokens },
		func(l *config.AdmissionLimits) **int64 { return &l.MaxReservedKVTokens }},
	{capacityKeyMaxPrefillTokens,
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
//   - badUnit names an unknown admission_capacity_unit. The entry's values
//     are kept and enforced as published, as graph-level limits (PerWorker
//     stays false): that is the tighter reading of a per-worker value and
//     the published reading of a graph-level one (D55(a)).
func parseGraphCapacity(d map[string]string) (c GraphCapacity, badValues []string, badUnit string) {
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
	switch unit := strings.TrimSpace(d[capacityKeyUnit]); unit {
	case "":
	case capacityUnitPerWorker:
		c.PerWorker = true
	default:
		badUnit = capacityKeyUnit + "=" + strconv.Quote(unit)
	}
	return c, badValues, badUnit
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
