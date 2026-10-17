package config

import (
	"fmt"
	"strconv"
	"strings"
)

// admissionPolicyTiers are the operator tiers an admission-policy ConfigMap
// may set, by the key prefix it uses (the same key as in the Helm values).
var admissionPolicyTiers = []string{"platform", "graph", "organization", "organizationModel"}

// ParseAdmissionPolicyEntry checks one data entry of the Atlas-written
// admission-policy ConfigMap (shared-tier admin page, Part B): a key
// "<tier>.<field>" naming one of the four operator tiers and an
// AdmissionLimits field by its Helm key, and a decimal integer value >= 1.
// It returns the key unchanged as the path and the value. The policy can set
// a cap but never remove one or set it to zero (removing a cap the Helm values
// set stays a Helm change).
func ParseAdmissionPolicyEntry(key, value string) (string, int64, error) {
	tier, field, ok := strings.Cut(key, ".")
	if !ok || !isAdmissionPolicyTier(tier) || limitField(&AdmissionLimits{}, field) == nil {
		return "", 0, fmt.Errorf("unknown admission policy key %q (want <tier>.<field>, tier one of %s)", key, strings.Join(admissionPolicyTiers, ", "))
	}
	v, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || v < 1 {
		return "", 0, fmt.Errorf("admission policy %s must be a decimal integer >= 1, got %q", key, value)
	}
	return key, v, nil
}

// WithLimitOverrides returns a copy of a with each value (a path from
// ParseAdmissionPolicyEntry -> its value) replacing the Helm value of that
// tier and field; a path that is absent keeps the Helm value. The result goes
// through the same validation as the Helm config (per-tier rules such as
// maxConcurrentPrefills <= maxActiveRequests, and lane shares against the
// platform caps). On an error the caller keeps the settings it had: the
// policy never fails open. a must be the Helm settings the admitter was built
// with, so each update is laid over the Helm values, never over the previous
// update.
func (a AdmissionSettings) WithLimitOverrides(values map[string]int64) (AdmissionSettings, error) {
	out := a
	targets := map[string]*AdmissionLimits{
		"platform": &out.Platform, "graph": &out.Graph,
		"organization": &out.Organization, "organizationModel": &out.OrganizationModel,
	}
	for _, tier := range admissionPolicyTiers {
		*targets[tier] = copyLimits(*targets[tier])
	}
	for path, value := range values {
		tier, field, _ := strings.Cut(path, ".")
		limits, ok := targets[tier]
		target := limitField(limits, field)
		if !ok || target == nil {
			return a, fmt.Errorf("unknown admission policy key %q", path)
		}
		v := value
		*target = &v
	}
	for _, tier := range admissionPolicyTiers {
		if err := targets[tier].parse("admission policy " + tier); err != nil {
			return a, err
		}
	}
	if len(out.Lanes) > 0 {
		if err := out.validateLaneShares(); err != nil {
			return a, fmt.Errorf("admission policy: %w", err)
		}
	}
	return out, nil
}

func isAdmissionPolicyTier(tier string) bool {
	for _, t := range admissionPolicyTiers {
		if t == tier {
			return true
		}
	}
	return false
}

// copyLimits copies l so that setting a field of the copy never writes
// through a pointer shared with l (AdmissionLimits fields are *int64).
func copyLimits(l AdmissionLimits) AdmissionLimits {
	out := l
	for _, field := range LimitFields(AdmissionLimits{}) {
		if value := limitField(&l, field.Name); *value != nil {
			v := **value
			*limitField(&out, field.Name) = &v
		}
	}
	return out
}

// limitField returns a pointer to l's field with YAML key name, or nil for an
// unknown name. It covers exactly the LimitFields keys (a test pins that).
func limitField(l *AdmissionLimits, name string) **int64 {
	switch name {
	case "maxActiveRequests":
		return &l.MaxActiveRequests
	case "maxConcurrentPrefills":
		return &l.MaxConcurrentPrefills
	case "maxReservedDecodeSlots":
		return &l.MaxReservedDecodeSlots
	case "maxPrefillTokens":
		return &l.MaxPrefillTokens
	case "maxReservedOutputTokens":
		return &l.MaxReservedOutputTokens
	case "maxReservedKvTokens":
		return &l.MaxReservedKVTokens
	case "maxActiveAdapters":
		return &l.MaxActiveAdapters
	case "requestsPerWindow":
		return &l.RequestsPerWindow
	case "totalPromptTokensPerWindow":
		return &l.TotalPromptTokensPerWindow
	case "uncachedPromptTokensPerWindow":
		return &l.UncachedPromptTokensPerWindow
	case "generatedTokensPerWindow":
		return &l.GeneratedTokensPerWindow
	case "maxColdHolds":
		return &l.MaxColdHolds
	case "wakesPerWindow":
		return &l.WakesPerWindow
	}
	return nil
}
