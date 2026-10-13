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
}

// Limits is the capacity as the admission limits of the graph_capacity scope.
// A new published field must be copied here; TestGraphCapacityLimitsCopiesEveryField
// fails when one is missing.
func (c GraphCapacity) Limits() config.AdmissionLimits {
	return config.AdmissionLimits{
		MaxActiveRequests:   c.MaxActiveRequests,
		MaxReservedKVTokens: c.MaxReservedKVTokens,
		MaxPrefillTokens:    c.MaxPrefillTokens,
	}
}

// Any reports whether at least one limit is published. It goes through
// Limits, so it reads the canonical config.LimitValues list.
func (c GraphCapacity) Any() bool { return c.Limits().Any() }

// fields lists the capacity fields in the order of capacityKeys. parse and
// tightest both walk it, so a new published key is added to this list and to
// capacityKeys only.
func (c *GraphCapacity) fields() []**int64 {
	return []**int64{&c.MaxActiveRequests, &c.MaxReservedKVTokens, &c.MaxPrefillTokens}
}

// The registry data keys carrying GraphCapacity. They are graph-level values
// stamped on every entry of the graph (each entry is one tenant's model on
// it). All optional; empty or absent publishes no limit.
const (
	capacityKeyMaxActiveRequests   = "admission_max_active_requests"
	capacityKeyMaxReservedKVTokens = "admission_max_reserved_kv_tokens"
	capacityKeyMaxPrefillTokens    = "admission_max_prefill_tokens"
)

// capacityKeys are the registry keys of GraphCapacity.fields, in that order.
var capacityKeys = []string{capacityKeyMaxActiveRequests, capacityKeyMaxReservedKVTokens, capacityKeyMaxPrefillTokens}

// parseGraphCapacity reads the capacity keys of one registry entry. A value
// that is not a non-negative integer is an Atlas producer bug; it is reported
// in problems and treated as unpublished (no limit) rather than rejecting the
// entry, because refusing the registration would take the tenant's model
// offline over a missing capacity hint (the same direction as having no
// profile at all).
func parseGraphCapacity(d map[string]string) (c GraphCapacity, problems []string) {
	for i, dst := range c.fields() {
		key := capacityKeys[i]
		raw := strings.TrimSpace(d[key])
		if raw == "" {
			continue
		}
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			problems = append(problems, key+"="+strconv.Quote(raw))
			continue
		}
		*dst = &v
	}
	return c, problems
}

// tightest combines the capacity published on several entries of one graph:
// for each limit the smallest published value wins. Entries of one graph
// should agree (Atlas stamps one graph-level value on all of them); during a
// change they briefly may not, and the tighter value is the safe reading.
func tightest(all map[string]GraphCapacity) GraphCapacity {
	var out GraphCapacity
	pick := func(dst **int64, v *int64) {
		if v != nil && (*dst == nil || *v < **dst) {
			x := *v
			*dst = &x
		}
	}
	dst := out.fields()
	for _, c := range all {
		for i, src := range c.fields() {
			pick(dst[i], *src)
		}
	}
	return out
}
