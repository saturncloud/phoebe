package config

import (
	"fmt"
	"sort"

	"gopkg.in/yaml.v2"
)

// AdmissionPolicyVersion is the only admission-policy document version this
// phoebe understands. A document with any other version is refused whole.
const AdmissionPolicyVersion = 1

// admissionPolicyTiers are the operator tiers an admission-policy document may
// set, by their key in the document (the same key as in the Helm values).
var admissionPolicyTiers = []string{"platform", "graph", "organization", "organizationModel"}

// WithAdmissionPolicy returns a copy of a with the operator-tier limits from
// an Atlas-written admission-policy document (shared-tier admin page, Part B)
// laid over it, and the document's revision.
//
// Precedence is per field: a field present in the document replaces the Helm
// value for that tier and field; an absent field keeps the Helm value. The
// document can set a cap but never remove one: a present field must be an
// integer >= 0 (window: a positive duration), and null is malformed.
//
// The document is validated whole, then the result goes through the same
// validation as the Helm config (per-tier rules and lane shares). Any error
// refuses the whole document, so the caller keeps the config it had: an
// admission policy never fails open. a must be the settings the admitter was
// built with (EffectiveAdmission) and must have admission enabled; with
// admission disabled there are no operator tiers to tune and the caller
// ignores the document.
func (a AdmissionSettings) WithAdmissionPolicy(doc []byte) (AdmissionSettings, string, error) {
	var raw yaml.MapSlice
	if err := yaml.Unmarshal(doc, &raw); err != nil {
		return a, "", fmt.Errorf("admission policy is not valid YAML: %w", err)
	}
	top := map[string]interface{}{}
	for _, item := range raw {
		key, ok := item.Key.(string)
		if !ok {
			return a, "", fmt.Errorf("admission policy has a non-string key %v", item.Key)
		}
		if _, dup := top[key]; dup {
			return a, "", fmt.Errorf("admission policy repeats key %q", key)
		}
		top[key] = item.Value
	}
	version, ok := top["version"].(int)
	if !ok || version != AdmissionPolicyVersion {
		return a, "", fmt.Errorf("admission policy version must be %d, got %v", AdmissionPolicyVersion, top["version"])
	}
	revision := ""
	if value, present := top["revision"]; present {
		if revision, ok = value.(string); !ok {
			return a, "", fmt.Errorf("admission policy revision must be a string, got %T", value)
		}
	}
	allowed := map[string]bool{"version": true, "revision": true}
	for _, tier := range admissionPolicyTiers {
		allowed[tier] = true
	}
	for key := range top {
		if !allowed[key] {
			return a, "", fmt.Errorf("admission policy has unknown key %q", key)
		}
	}

	out := a
	targets := map[string]*AdmissionLimits{
		"platform": &out.Platform, "graph": &out.Graph,
		"organization": &out.Organization, "organizationModel": &out.OrganizationModel,
	}
	for _, tier := range admissionPolicyTiers {
		value, present := top[tier]
		if !present {
			continue
		}
		fields, ok := value.(yaml.MapSlice)
		if !ok {
			return a, "", fmt.Errorf("admission policy %s must be a mapping, got %T", tier, value)
		}
		limits := copyLimits(*targets[tier])
		if err := overlayLimits(&limits, tier, fields); err != nil {
			return a, "", err
		}
		*targets[tier] = limits
	}
	for _, tier := range admissionPolicyTiers {
		if err := targets[tier].parse("admission policy " + tier); err != nil {
			return a, "", err
		}
	}
	if len(out.Lanes) > 0 {
		if err := out.validateLaneShares(); err != nil {
			return a, "", fmt.Errorf("admission policy: %w", err)
		}
	}
	return out, revision, nil
}

// copyLimits copies l so that setting a field of the copy never writes
// through a pointer shared with l (AdmissionLimits fields are *int64).
func copyLimits(l AdmissionLimits) AdmissionLimits {
	out := l
	for _, name := range limitFieldNames() {
		if value := limitField(&l, name); *value != nil {
			v := **value
			*limitField(&out, name) = &v
		}
	}
	return out
}

// overlayLimits sets each field present in fields on l, refusing unknown
// fields, repeats, null and anything that is not a non-negative integer (or,
// for window, a duration string; parse validates it afterwards).
func overlayLimits(l *AdmissionLimits, tier string, fields yaml.MapSlice) error {
	seen := map[string]bool{}
	for _, item := range fields {
		name, ok := item.Key.(string)
		if !ok {
			return fmt.Errorf("admission policy %s has a non-string key %v", tier, item.Key)
		}
		if seen[name] {
			return fmt.Errorf("admission policy %s repeats %q", tier, name)
		}
		seen[name] = true
		if name == "window" {
			window, ok := item.Value.(string)
			if !ok || window == "" {
				return fmt.Errorf("admission policy %s.window must be a duration string such as \"1m\"", tier)
			}
			l.WindowStr = window
			continue
		}
		target := limitField(l, name)
		if target == nil {
			return fmt.Errorf("admission policy %s has unknown field %q", tier, name)
		}
		value, ok := item.Value.(int)
		if !ok {
			// null, floats, strings and booleans are refused: the policy can
			// set a cap, never remove one (a Helm value change does that).
			return fmt.Errorf("admission policy %s.%s must be an integer >= 0, got %v", tier, name, item.Value)
		}
		if value < 0 {
			return fmt.Errorf("admission policy %s.%s cannot be negative", tier, name)
		}
		v := int64(value)
		*target = &v
	}
	return nil
}

// limitFieldNames is LimitFields' names, sorted, for the policy overlay.
func limitFieldNames() []string {
	var names []string
	for _, field := range LimitFields(AdmissionLimits{}) {
		names = append(names, field.Name)
	}
	sort.Strings(names)
	return names
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
