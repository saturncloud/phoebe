package meteredusage

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Video generation records (kind "video"), video-models-design.md section 7.3,
// approved by Hugo 2026-11-18. The attribute and quantity names, and the enum
// values of task and resolution, are one-way doors: the manager stores them and
// prices by them.

// Video tasks: text-to-video and image-to-video.
const (
	TaskT2V = "t2v"
	TaskI2V = "i2v"
)

// Video resolutions, bucketed by the short side of the frame.
const (
	Resolution480p  = "480p"
	Resolution720p  = "720p"
	Resolution1080p = "1080p"
	Resolution4K    = "4k"
)

var (
	videoTasks       = map[string]bool{TaskT2V: true, TaskI2V: true}
	videoResolutions = map[string]bool{Resolution480p: true, Resolution720p: true, Resolution1080p: true, Resolution4K: true}
)

// decimalQuantity is the manager's rule for a decimal-string quantity (the
// same rule as storage's gib_seconds): non-negative, at most 30 integer digits
// and 9 fractional digits, no sign, no exponent.
var decimalQuantity = regexp.MustCompile(`^[0-9]{1,30}(\.[0-9]{1,9})?$`)

// VideoRecord is one hourly video record. The JSON shape is Record's, with
// video attributes and quantities.
type VideoRecord struct {
	UsageID    string          `json:"usage_id"`
	Kind       string          `json:"kind"`
	OrgID      string          `json:"org_id"`
	OwnerType  string          `json:"owner_type"`
	OwnerID    string          `json:"owner_id"`
	ResourceID string          `json:"resource_id"`
	Attributes VideoAttributes `json:"attributes"`
	Quantities VideoQuantities `json:"quantities"`
}

// VideoAttributes are the facts a video price can depend on. Together with the
// hour, org, owner and resource they are the record's grain, and all of them
// enter usage_id.
type VideoAttributes struct {
	AuthID      string `json:"auth_id"`
	ModelID     string `json:"model_id"`
	ServingMode string `json:"serving_mode"`
	// BaseModel is the catalog base model; "" when the endpoint has none.
	BaseModel string `json:"base_model"`
	// Task is "t2v" or "i2v".
	Task string `json:"task"`
	// Resolution is "480p", "720p", "1080p" or "4k".
	Resolution string `json:"resolution"`
	// Region is the serving cluster's region; "" when unknown (the usual case
	// in v1: a video endpoint is not a Dynamo graph, so no region is stamped).
	Region string `json:"region"`
}

// VideoQuantities are the per-record sums.
type VideoQuantities struct {
	// OutputSeconds is the delivered video length (frames / fps), summed
	// exactly, as a decimal string.
	OutputSeconds string `json:"output_seconds"`
	OutputFrames  int64  `json:"output_frames"`
	// RequestCount counts completed jobs only.
	RequestCount int64 `json:"request_count"`
	// GPUSeconds is GPU count x wall time on the engine, failed jobs included,
	// as a decimal string.
	GPUSeconds string `json:"gpu_seconds"`
}

// attributePairs names the video attribute keys for the hash.
// TestVideoAttributeKeysMatchJSONTags keeps them in step with the JSON tags.
func (a VideoAttributes) attributePairs() [][2]string {
	return [][2]string{
		{"auth_id", a.AuthID},
		{"model_id", a.ModelID},
		{"serving_mode", a.ServingMode},
		{"base_model", a.BaseModel},
		{"task", a.Task},
		{"resolution", a.Resolution},
		{"region", a.Region},
	}
}

// VideoUsageID is a video record's usage_id: the same rule as TokenUsageID
// with kind "video" and the video attributes.
func VideoUsageID(windowStart, orgID, ownerType, ownerID, resourceID string, attrs VideoAttributes) string {
	return usageID(KindVideo, windowStart, orgID, ownerType, ownerID, resourceID, attrs.attributePairs())
}

// CanonicalDecimal turns a non-negative Postgres NUMERIC rendering ("10.1250",
// "0.000") into the shortest exact spelling ("10.125", "0"). The value is
// unchanged; only trailing fractional zeros go, so a re-sent hour spells the
// same sum the same way whatever the column's scale.
func CanonicalDecimal(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}

// ValidateVideoRecord applies the per-record rules to one video record of the
// hour windowStart. A producer uses it to hold back one bad record instead of
// the whole hour.
func ValidateVideoRecord(windowStart string, rec VideoRecord) error {
	return validateVideoRecord("record", windowStart, rec)
}

func validateVideoRecord(path, windowStart string, rec VideoRecord) error {
	if rec.Kind != KindVideo {
		return invalid(path+".kind", "a video record must have kind %q, got %q", KindVideo, rec.Kind)
	}
	if err := validateEnvelope(path, rec.UsageID, rec.OrgID, rec.OwnerType, rec.OwnerID, rec.ResourceID); err != nil {
		return err
	}
	a := rec.Attributes
	if err := validateStringAttributes(path, a.attributePairs()); err != nil {
		return err
	}
	for _, kv := range [][2]string{{"auth_id", a.AuthID}, {"model_id", a.ModelID}} {
		if kv[1] == "" {
			return invalid(path+".attributes."+kv[0], "must not be empty")
		}
	}
	if a.ServingMode != ServingModeShared && a.ServingMode != ServingModeDedicated {
		return invalid(path+".attributes.serving_mode", "%q is not shared or dedicated", a.ServingMode)
	}
	if !videoTasks[a.Task] {
		return invalid(path+".attributes.task", "%q is not t2v or i2v", a.Task)
	}
	if !videoResolutions[a.Resolution] {
		return invalid(path+".attributes.resolution", "%q is not 480p, 720p, 1080p or 4k", a.Resolution)
	}

	q := rec.Quantities
	for _, kv := range [][2]string{{"output_seconds", q.OutputSeconds}, {"gpu_seconds", q.GPUSeconds}} {
		if !decimalQuantity.MatchString(kv[1]) {
			return invalid(path+".quantities."+kv[0], "%q is not a non-negative decimal string with at most 30 integer and 9 fractional digits", kv[1])
		}
	}
	for _, kv := range []struct {
		k string
		v int64
	}{{"output_frames", q.OutputFrames}, {"request_count", q.RequestCount}} {
		if kv.v < 0 {
			return invalid(path+".quantities."+kv.k, "negative (%d)", kv.v)
		}
	}

	if want := VideoUsageID(windowStart, rec.OrgID, rec.OwnerType, rec.OwnerID, rec.ResourceID, a); rec.UsageID != want {
		return invalid(path+".usage_id", "does not match the record's grain (want %s)", want)
	}
	return nil
}

// validateEnvelope checks the fields every phoebe record shares.
func validateEnvelope(path, usageID, orgID, ownerType, ownerID, resourceID string) error {
	if !hex64.MatchString(usageID) {
		return invalid(path+".usage_id", "not 64 lowercase hex")
	}
	if !hex32.MatchString(orgID) {
		return invalid(path+".org_id", "not 32 lowercase hex")
	}
	switch ownerType {
	case OwnerTypeUser, OwnerTypeGroup:
		if !hex32.MatchString(ownerID) {
			return invalid(path+".owner_id", "not 32 lowercase hex (owner_type %q)", ownerType)
		}
	case OwnerTypeNone:
		if ownerID != "" {
			return invalid(path+".owner_id", "must be empty when owner_type is empty")
		}
	default:
		return invalid(path+".owner_type", "%q is not user, group or empty", ownerType)
	}
	// A phoebe record always names the endpoint that served it.
	if !hex32.MatchString(resourceID) {
		return invalid(path+".resource_id", "not 32 lowercase hex")
	}
	return nil
}

// validateStringAttributes checks every string attribute is valid UTF-8 and
// within the manager's length cap.
func validateStringAttributes(path string, pairs [][2]string) error {
	for _, kv := range pairs {
		if !utf8.ValidString(kv[1]) {
			return invalid(path+".attributes."+kv[0], "not valid UTF-8")
		}
		if n := utf8.RuneCountInString(kv[1]); n > MaxAttributeChars {
			return invalid(path+".attributes."+kv[0], "%d characters exceed the %d cap", n, MaxAttributeChars)
		}
	}
	return nil
}

// requestWire is Request's JSON shape: one "records" list holding every kind.
type requestWire struct {
	WindowStart string            `json:"window_start"`
	Kinds       []string          `json:"kinds"`
	Records     []json.RawMessage `json:"records"`
	Held        []string          `json:"held"`
}

// MarshalJSON writes the token records, then the video records, as one
// "records" list. A Request whose Records is nil and has no video records
// marshals "records": null, which Validate refuses before any send.
func (r Request) MarshalJSON() ([]byte, error) {
	w := requestWire{WindowStart: r.WindowStart, Kinds: r.Kinds, Held: r.Held}
	if r.Records != nil || len(r.VideoRecords) > 0 {
		w.Records = make([]json.RawMessage, 0, len(r.Records)+len(r.VideoRecords))
	}
	for _, rec := range r.Records {
		b, err := json.Marshal(rec)
		if err != nil {
			return nil, err
		}
		w.Records = append(w.Records, b)
	}
	for _, rec := range r.VideoRecords {
		b, err := json.Marshal(rec)
		if err != nil {
			return nil, err
		}
		w.Records = append(w.Records, b)
	}
	return json.Marshal(w)
}

// UnmarshalJSON is MarshalJSON's inverse: each record goes to Records or
// VideoRecords by its kind. Tests and tools use it to read a sent body back.
func (r *Request) UnmarshalJSON(data []byte) error {
	var w requestWire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	*r = Request{WindowStart: w.WindowStart, Kinds: w.Kinds, Held: w.Held}
	if w.Records != nil {
		r.Records = make([]Record, 0, len(w.Records))
	}
	for i, raw := range w.Records {
		var probe struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(raw, &probe); err != nil {
			return fmt.Errorf("records[%d]: %w", i, err)
		}
		switch probe.Kind {
		case KindVideo:
			var v VideoRecord
			if err := json.Unmarshal(raw, &v); err != nil {
				return fmt.Errorf("records[%d]: %w", i, err)
			}
			r.VideoRecords = append(r.VideoRecords, v)
		default:
			var t Record
			if err := json.Unmarshal(raw, &t); err != nil {
				return fmt.Errorf("records[%d]: %w", i, err)
			}
			r.Records = append(r.Records, t)
		}
	}
	return nil
}

// RecordCount is the number of records of every kind in the request.
func (r Request) RecordCount() int { return len(r.Records) + len(r.VideoRecords) }
