package meteredusage

import (
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"
)

// ValidationError names the first field that breaks a wire rule, in the same
// JSON-path style as the manager's 400 body ({"error", "path"}), so a log line
// from either side points at the same field.
type ValidationError struct {
	Path string
	Msg  string
}

func (e *ValidationError) Error() string { return e.Path + ": " + e.Msg }

func invalid(path, format string, args ...any) error {
	return &ValidationError{Path: path, Msg: fmt.Sprintf(format, args...)}
}

var (
	hex32 = regexp.MustCompile(`^[0-9a-f]{32}$`)
	hex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// knownKinds is the manager's closed set of kinds.
var knownKinds = map[string]bool{"token": true, "compute": true, "storage": true, "seats": true}

// Validate applies the manager's request rules (wire-conventions.md) on the
// install, before sending. The manager rejects a bad request whole with a 400
// and stores nothing, and a 400 is not retried, so a request that fails here
// would fail there too; refusing it locally keeps the failure on the install,
// where the evidence is, and names the bad field.
//
// It also recomputes every token record's usage_id from the record's own
// fields. The manager treats usage_id as opaque, so it cannot catch a record
// whose id does not match its grain; this check is the only place that can.
func Validate(req Request) error {
	ws, err := time.Parse(time.RFC3339, req.WindowStart)
	if err != nil {
		return invalid("window_start", "not ISO-8601 with an explicit offset: %v", err)
	}
	if !ws.Truncate(time.Hour).Equal(ws) || ws.Nanosecond() != 0 {
		return invalid("window_start", "%q is not a whole hour", req.WindowStart)
	}
	if FormatWindowStart(ws) != req.WindowStart {
		// The hash input pins this exact spelling; a different spelling of the
		// same instant would mint different usage_ids for the same hour.
		return invalid("window_start", "%q is not spelled YYYY-MM-DDTHH:00:00+00:00", req.WindowStart)
	}

	if len(req.Kinds) == 0 {
		return invalid("kinds", "must not be empty")
	}
	kinds := make(map[string]bool, len(req.Kinds))
	for i, k := range req.Kinds {
		if !knownKinds[k] {
			return invalid(fmt.Sprintf("kinds[%d]", i), "unknown kind %q", k)
		}
		if kinds[k] {
			return invalid(fmt.Sprintf("kinds[%d]", i), "duplicate kind %q", k)
		}
		kinds[k] = true
	}

	if req.Records == nil {
		return invalid("records", "must be a list (an empty hour is [], never null)")
	}
	if len(req.Records) > MaxRecordsPerRequest {
		return invalid("records", "%d records exceed the %d-record cap", len(req.Records), MaxRecordsPerRequest)
	}
	if req.Held == nil {
		return invalid("held", "must be a list ([] when nothing is held)")
	}
	for i, id := range req.Held {
		if !hex64.MatchString(id) {
			return invalid(fmt.Sprintf("held[%d]", i), "not a usage_id (64 lowercase hex)")
		}
	}

	seen := make(map[string]int, len(req.Records))
	for i, rec := range req.Records {
		path := fmt.Sprintf("records[%d]", i)
		if !kinds[rec.Kind] {
			return invalid(path+".kind", "%q is not listed in kinds", rec.Kind)
		}
		if rec.Kind != KindToken {
			return invalid(path+".kind", "phoebe only produces %q records, got %q", KindToken, rec.Kind)
		}
		if err := validateTokenRecord(path, req.WindowStart, rec); err != nil {
			return err
		}
		if j, dup := seen[rec.UsageID]; dup {
			return invalid(path+".usage_id", "duplicates records[%d].usage_id", j)
		}
		seen[rec.UsageID] = i
	}
	return nil
}

func validateTokenRecord(path, windowStart string, rec Record) error {
	if !hex64.MatchString(rec.UsageID) {
		return invalid(path+".usage_id", "not 64 lowercase hex")
	}
	if !hex32.MatchString(rec.OrgID) {
		return invalid(path+".org_id", "not 32 lowercase hex")
	}
	switch rec.OwnerType {
	case OwnerTypeUser, OwnerTypeGroup:
		if !hex32.MatchString(rec.OwnerID) {
			return invalid(path+".owner_id", "not 32 lowercase hex (owner_type %q)", rec.OwnerType)
		}
	case OwnerTypeNone:
		if rec.OwnerID != "" {
			return invalid(path+".owner_id", "must be empty when owner_type is empty")
		}
	default:
		return invalid(path+".owner_type", "%q is not user, group or empty", rec.OwnerType)
	}
	// A token record always names the endpoint that served it.
	if !hex32.MatchString(rec.ResourceID) {
		return invalid(path+".resource_id", "not 32 lowercase hex")
	}

	a := rec.Attributes
	for _, kv := range a.attributePairs() {
		if !utf8.ValidString(kv[1]) {
			return invalid(path+".attributes."+kv[0], "not valid UTF-8")
		}
		if n := utf8.RuneCountInString(kv[1]); n > MaxAttributeChars {
			return invalid(path+".attributes."+kv[0], "%d characters exceed the %d cap", n, MaxAttributeChars)
		}
	}
	// base_model may be "" (corrected conventions, 2026-10-27): a dedicated
	// endpoint may serve a model with no catalog base, and the manager prices it.
	for _, kv := range [][2]string{{"auth_id", a.AuthID}, {"model_id", a.ModelID}} {
		if kv[1] == "" {
			return invalid(path+".attributes."+kv[0], "must not be empty")
		}
	}
	if a.ServingMode != ServingModeShared && a.ServingMode != ServingModeDedicated {
		return invalid(path+".attributes.serving_mode", "%q is not shared or dedicated", a.ServingMode)
	}

	q := rec.Quantities
	for _, kv := range []struct {
		k string
		v int64
	}{{"prompt_tokens", q.PromptTokens}, {"cached_tokens", q.CachedTokens}, {"completion_tokens", q.CompletionTokens}, {"request_count", q.RequestCount}} {
		if kv.v < 0 {
			return invalid(path+".quantities."+kv.k, "negative (%d)", kv.v)
		}
	}

	if want := TokenUsageID(windowStart, rec.OrgID, rec.OwnerType, rec.OwnerID, rec.ResourceID, a); rec.UsageID != want {
		return invalid(path+".usage_id", "does not match the record's grain (want %s)", want)
	}
	return nil
}
