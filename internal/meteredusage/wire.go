// Package meteredusage is phoebe's side of the manager's metered-usage wire:
// POST /customer/metered-usage (billing-design.md section 3, ACCEPTED by Hugo
// 2026-10-22; details pinned in agent-wip/evidence/billing-unification/
// wire-conventions.md).
//
// The install sends QUANTITIES, never money (design D1): the manager owns every
// price and rates each hour as of that hour. phoebe produces kind "token" only.
//
// One request is the COMPLETE set of records for one hour and the listed kinds
// from this install. Records of that (install, hour, kind) scope that are absent
// from the request and not listed in "held" are deleted by the manager
// (delete-by-absence). So a request is a snapshot, a re-sent hour replaces the
// previous one, and an empty "records" list is how an hour with no usage says so.
//
// This package holds what must agree byte for byte with the manager: the JSON
// shape, the usage_id hash, and the validation rules. validate.go mirrors the
// manager's 400 rules so the install refuses to send a request the manager
// would reject, and client.go carries the retry policy.
package meteredusage

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
)

// Path is the manager route. The base URL is configured per install.
const Path = "/customer/metered-usage"

// KindToken is the only kind phoebe produces. Every request phoebe sends lists
// exactly this kind, so phoebe's delete-by-absence can never reach the compute,
// storage or seats records other producers send for the same hour.
const KindToken = "token"

// Serving modes: a closed enum on the wire (wire-conventions.md).
const (
	ServingModeShared    = "shared"
	ServingModeDedicated = "dedicated"
)

// Owner types. "" means the producer supplied no owner, and then owner_id is "".
const (
	OwnerTypeUser  = "user"
	OwnerTypeGroup = "group"
	OwnerTypeNone  = ""
)

// MaxRecordsPerRequest is the manager's per-request cap. One request must carry
// the whole hour, so an hour above the cap cannot be sent at all.
const MaxRecordsPerRequest = 50000

// MaxAttributeChars is the manager's cap on an attribute string, in characters
// (Unicode code points, Python len()).
const MaxAttributeChars = 256

// windowLayout is "YYYY-MM-DDTHH:00:00+00:00" for a whole UTC hour. The numeric
// offset, not "Z", because the conventions pin this exact spelling inside the
// usage_id hash input, and a different spelling of the same instant would hash
// to a different id.
const windowLayout = "2006-01-02T15:04:05+00:00"

// FormatWindowStart renders a whole UTC hour the way the wire and the usage_id
// hash both spell it. Callers must pass an hour-aligned instant; Validate
// rejects a request whose window_start is not one.
func FormatWindowStart(t time.Time) string {
	return t.UTC().Format(windowLayout)
}

// Request is the POST body. Field order is the JSON order.
type Request struct {
	WindowStart string   `json:"window_start"`
	Kinds       []string `json:"kinds"`
	// Records must be non-nil: an empty hour marshals to [], which tells the
	// manager the hour has no token usage (delete-by-absence), never to null.
	Records []Record `json:"records"`
	// Held lists usage_ids the manager must keep although they are absent.
	// phoebe sends [] (it withholds a whole hour instead; see cmd/token-push).
	Held []string `json:"held"`
}

// Record is one hourly token record.
type Record struct {
	UsageID    string          `json:"usage_id"`
	Kind       string          `json:"kind"`
	OrgID      string          `json:"org_id"`
	OwnerType  string          `json:"owner_type"`
	OwnerID    string          `json:"owner_id"`
	ResourceID string          `json:"resource_id"`
	Attributes TokenAttributes `json:"attributes"`
	Quantities TokenQuantities `json:"quantities"`
}

// TokenAttributes are the facts a token price can depend on, exactly the keys
// of billing-design.md section 3. Together with the hour, org, owner and
// resource they are the record's grain, and all of them enter usage_id.
//
// The optional model_params / model_active_params attributes are NOT sent:
// phoebe has no reliable source for a model's parameter count (Atlas does not
// stamp it on the request), and the conventions allow them to be absent.
type TokenAttributes struct {
	AuthID      string `json:"auth_id"`
	ModelID     string `json:"model_id"`
	ServingMode string `json:"serving_mode"`
	BaseModel   string `json:"base_model"`
	// Adapter is the fine-tune checkpoint artifact id; "" when there is none.
	Adapter string `json:"adapter"`
	// GraphK8sName is the serving graph; "" when the requests named none.
	GraphK8sName string `json:"graph_k8s_name"`
	// TopologyRevision is Atlas's topology revision as a decimal string; ""
	// when unknown (phoebe stores unknown as 0, see TopologyRevisionAttr).
	TopologyRevision string `json:"topology_revision"`
	// Region is the serving cluster's region; "" when unknown or when the
	// hour's events disagreed.
	Region string `json:"region"`
}

// TopologyRevisionAttr maps phoebe's stored revision (0 = unknown, the
// rated_usage key sentinel) to the wire attribute ("" = unknown). Revisions
// are positive integers in Atlas, so the mapping is one-to-one.
func TopologyRevisionAttr(rev int64) string {
	if rev == 0 {
		return ""
	}
	return strconv.FormatInt(rev, 10)
}

// TokenQuantities are the per-record sums. All are required and non-negative.
type TokenQuantities struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CachedTokens     int64 `json:"cached_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	RequestCount     int64 `json:"request_count"`
}

// Response is the manager's 200 body.
type Response struct {
	Accepted    int64    `json:"accepted"`
	Deleted     int64    `json:"deleted"`
	WindowStart string   `json:"window_start"`
	Kinds       []string `json:"kinds"`
}

// attributePairs lists the token attributes as (key, value) pairs. It is the
// single place that names the keys for the hash; the JSON tags above name them
// for the body. TestTokenAttributeKeysMatchJSONTags keeps the two in step.
func (a TokenAttributes) attributePairs() [][2]string {
	return [][2]string{
		{"auth_id", a.AuthID},
		{"model_id", a.ModelID},
		{"serving_mode", a.ServingMode},
		{"base_model", a.BaseModel},
		{"adapter", a.Adapter},
		{"graph_k8s_name", a.GraphK8sName},
		{"topology_revision", a.TopologyRevision},
		{"region", a.Region},
	}
}

// TokenUsageID computes a token record's usage_id as wire-conventions.md
// defines it:
//
//	sha256("|".join([kind, window_start_utc_iso, org_id, owner_type, owner_id,
//	                 resource_id, json.dumps(attributes, sort_keys=True,
//	                                         separators=(",", ":"))]))
//
// lowercase hex. For kind token every attribute defines the grain, so all of
// them are hashed. The JSON is Python's json.dumps output exactly (see
// pythonJSONString), because the manager and the other producers compute the
// same function in Python; Go's encoding/json would differ on non-ASCII text
// and on <, > and &.
//
// The same hour re-sent yields the same ids, so a re-send replaces rather than
// duplicates. Any attribute change (a region relabel, for instance) yields a
// new id; the old record is then deleted by absence and the new one inserted.
func TokenUsageID(windowStart, orgID, ownerType, ownerID, resourceID string, attrs TokenAttributes) string {
	var b strings.Builder
	for _, part := range []string{KindToken, windowStart, orgID, ownerType, ownerID, resourceID} {
		b.WriteString(part)
		b.WriteByte('|')
	}
	writeSortedStringObject(&b, attrs.attributePairs())
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

// writeSortedStringObject writes a JSON object of string values with keys
// sorted and no whitespace: json.dumps(obj, sort_keys=True,
// separators=(",", ":")). Python sorts str keys by code point; for valid
// UTF-8, byte order is code point order, so sort.Strings matches.
func writeSortedStringObject(b *strings.Builder, pairs [][2]string) {
	sorted := make([][2]string, len(pairs))
	copy(sorted, pairs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i][0] < sorted[j][0] })
	b.WriteByte('{')
	for i, kv := range sorted {
		if i > 0 {
			b.WriteByte(',')
		}
		writePythonJSONString(b, kv[0])
		b.WriteByte(':')
		writePythonJSONString(b, kv[1])
	}
	b.WriteByte('}')
}

// writePythonJSONString writes s as Python's json.dumps writes a str with the
// default ensure_ascii=True: '"' and '\\' backslash-escaped; \b \f \n \r \t
// as their short escapes; every other character outside ' '..'~' (so control
// characters, DEL and all non-ASCII) as \uXXXX with lowercase hex, and
// characters above U+FFFF as a UTF-16 surrogate pair. '/' is not escaped.
//
// s must be valid UTF-8 (Validate refuses anything else): an invalid byte has
// no Python str equivalent, so no hash could match the manager's view of it.
func writePythonJSONString(b *strings.Builder, s string) {
	const hexDigits = "0123456789abcdef"
	writeU := func(r rune) {
		b.WriteString(`\u`)
		b.WriteByte(hexDigits[(r>>12)&0xf])
		b.WriteByte(hexDigits[(r>>8)&0xf])
		b.WriteByte(hexDigits[(r>>4)&0xf])
		b.WriteByte(hexDigits[r&0xf])
	}
	b.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"':
			b.WriteString(`\"`)
		case r == '\\':
			b.WriteString(`\\`)
		case r == '\b':
			b.WriteString(`\b`)
		case r == '\f':
			b.WriteString(`\f`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r >= ' ' && r <= '~':
			b.WriteRune(r)
		case r > 0xffff:
			hi, lo := utf16.EncodeRune(r)
			writeU(hi)
			writeU(lo)
		default:
			writeU(r)
		}
	}
	b.WriteByte('"')
}
