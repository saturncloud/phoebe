package meteredusage

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// pythonReference holds usage_ids computed by the conventions file's formula in
// Python, verbatim:
//
//	import hashlib, json
//	def usage_id(kind, ws, org, ot, oid, rid, attrs):
//	    s = "|".join([kind, ws, org, ot, oid, rid,
//	                  json.dumps(attrs, sort_keys=True, separators=(",", ":"))])
//	    return hashlib.sha256(s.encode("utf-8")).hexdigest()
//
// (agent-wip/evidence/billing-unification/wire-conventions.md, "Record").
// The cases cover the places Go's encoding/json and Python's json.dumps
// disagree: non-ASCII (Python escapes to \uXXXX), characters above U+FFFF
// (surrogate pairs), control characters, DEL, and <, > and & (Go escapes them,
// Python does not).
var pythonReference = []struct {
	name       string
	ownerType  string
	ownerID    string
	attrs      TokenAttributes
	wantUsage  string
	wantJSONIn string // a fragment of the canonical attribute JSON, to make a mismatch readable
}{
	{
		name: "plain", ownerType: "user", ownerID: strings.Repeat("b", 32),
		attrs:     refAttrs(func(*TokenAttributes) {}),
		wantUsage: "523e4ca670fea8a74415c4ee1ce845ea9a034ba093ebba1bd2d6f014f6900b4f",
	},
	{
		name: "shared_no_owner", ownerType: "", ownerID: "",
		attrs: refAttrs(func(a *TokenAttributes) {
			a.ServingMode, a.GraphK8sName, a.TopologyRevision, a.Region = "shared", "", "", ""
		}),
		wantUsage: "ef2e9a28e247e71691f1f4dd6513b2681c5e0a9cd46948b90d34d4d851b2909b",
	},
	{
		name: "non_ascii", ownerType: "group", ownerID: strings.Repeat("c", 32),
		attrs: refAttrs(func(a *TokenAttributes) {
			a.ModelID, a.Region = "modèle-ü", "東京"
		}),
		wantUsage:  "1df98f1677cd89431ba32a8fd8793dee42a369c242c893047e53738f5268084c",
		wantJSONIn: `"model_id":"mod\u00e8le-\u00fc"`,
	},
	{
		name: "astral", ownerType: "user", ownerID: strings.Repeat("b", 32),
		attrs:      refAttrs(func(a *TokenAttributes) { a.Adapter = "ckpt-\U0001F600" }),
		wantUsage:  "0bb01d8148a3c0e731b5c7ab97a0a5e99af7875a5b3b996dd93887da38ce0997",
		wantJSONIn: `"adapter":"ckpt-\ud83d\ude00"`,
	},
	{
		name: "escapes", ownerType: "user", ownerID: strings.Repeat("b", 32),
		attrs:      refAttrs(func(a *TokenAttributes) { a.ModelID = "q\"uo\\te\n\t\x01\x7f<>&/" }),
		wantUsage:  "d93ca574be1d939a236cd04665ba0824a79bfb61d0511dd2b9fdb17d749cbee9",
		wantJSONIn: `"model_id":"q\"uo\\te\n\t\u0001\u007f<>&/"`,
	},
}

const (
	refWindow   = "2026-10-22T14:00:00+00:00"
	refOrg      = "dddddddddddddddddddddddddddddddd"
	refResource = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
)

func refAttrs(mod func(*TokenAttributes)) TokenAttributes {
	a := TokenAttributes{
		AuthID: strings.Repeat("a", 32), ModelID: "qwen-endpoint", ServingMode: "dedicated",
		BaseModel: "Qwen/Qwen2.5-32B-Instruct", Adapter: "", GraphK8sName: "pd-qwen",
		TopologyRevision: "3", Region: "us-central1",
	}
	mod(&a)
	return a
}

func TestTokenUsageID_EqualsThePythonReferenceFromTheConventions(t *testing.T) {
	for _, tc := range pythonReference {
		t.Run(tc.name, func(t *testing.T) {
			got := TokenUsageID(refWindow, refOrg, tc.ownerType, tc.ownerID, refResource, tc.attrs)
			if got != tc.wantUsage {
				var b strings.Builder
				writeSortedStringObject(&b, tc.attrs.attributePairs())
				t.Fatalf("usage_id = %s, Python reference = %s\ncanonical attributes: %s", got, tc.wantUsage, b.String())
			}
			if tc.wantJSONIn != "" {
				var b strings.Builder
				writeSortedStringObject(&b, tc.attrs.attributePairs())
				if !strings.Contains(b.String(), tc.wantJSONIn) {
					t.Fatalf("canonical attributes %s do not contain Python's spelling %s", b.String(), tc.wantJSONIn)
				}
			}
		})
	}
}

func TestTokenUsageID_IsDeterministic(t *testing.T) {
	a := refAttrs(func(*TokenAttributes) {})
	first := TokenUsageID(refWindow, refOrg, "user", strings.Repeat("b", 32), refResource, a)
	for i := 0; i < 100; i++ {
		if got := TokenUsageID(refWindow, refOrg, "user", strings.Repeat("b", 32), refResource, a); got != first {
			t.Fatalf("run %d: %s != %s", i, got, first)
		}
	}
}

// Every field that defines a token record's grain must move the id; otherwise
// two different records in one hour would collide (a 400 on the whole hour) or
// a changed record would silently keep its old id.
func TestTokenUsageID_ChangesWhenAnyGrainFieldChanges(t *testing.T) {
	base := refAttrs(func(*TokenAttributes) {})
	owner := strings.Repeat("b", 32)
	baseID := TokenUsageID(refWindow, refOrg, "user", owner, refResource, base)

	other := strings.Repeat("f", 32)
	variants := map[string]string{
		"window":   TokenUsageID("2026-10-22T15:00:00+00:00", refOrg, "user", owner, refResource, base),
		"org":      TokenUsageID(refWindow, other, "user", owner, refResource, base),
		"ownerTyp": TokenUsageID(refWindow, refOrg, "group", owner, refResource, base),
		"owner":    TokenUsageID(refWindow, refOrg, "user", other, refResource, base),
		"resource": TokenUsageID(refWindow, refOrg, "user", owner, other, base),
	}
	for _, f := range []string{"auth_id", "model_id", "serving_mode", "base_model", "adapter", "graph_k8s_name", "topology_revision", "region"} {
		a := base
		switch f {
		case "auth_id":
			a.AuthID += "x"
		case "model_id":
			a.ModelID += "x"
		case "serving_mode":
			a.ServingMode = "shared"
		case "base_model":
			a.BaseModel += "x"
		case "adapter":
			a.Adapter = "ckpt"
		case "graph_k8s_name":
			a.GraphK8sName += "x"
		case "topology_revision":
			a.TopologyRevision = "4"
		case "region":
			a.Region += "x"
		}
		variants["attr:"+f] = TokenUsageID(refWindow, refOrg, "user", owner, refResource, a)
	}
	seen := map[string]string{baseID: "base"}
	for name, id := range variants {
		if prev, dup := seen[id]; dup {
			t.Fatalf("changing %s produced the same id as %s", name, prev)
		}
		seen[id] = name
	}
}

// The hash names attribute keys in attributePairs; the body names them in the
// JSON tags. If the two lists drift, the manager would store attributes under
// one set of names while the id was computed over another.
func TestTokenAttributeKeysMatchJSONTags(t *testing.T) {
	var fromTags []string
	typ := reflect.TypeOf(TokenAttributes{})
	for i := 0; i < typ.NumField(); i++ {
		fromTags = append(fromTags, typ.Field(i).Tag.Get("json"))
	}
	var fromPairs []string
	for _, kv := range (TokenAttributes{}).attributePairs() {
		fromPairs = append(fromPairs, kv[0])
	}
	if !reflect.DeepEqual(fromTags, fromPairs) {
		t.Fatalf("json tags %v != hashed keys %v", fromTags, fromPairs)
	}
	want := []string{"auth_id", "model_id", "serving_mode", "base_model", "adapter", "graph_k8s_name", "topology_revision", "region"}
	if !reflect.DeepEqual(fromTags, want) {
		t.Fatalf("token attribute keys %v, section 3 says %v", fromTags, want)
	}
}

func TestTopologyRevisionAttr_ZeroMeansUnknownAndIsEmpty(t *testing.T) {
	if got := TopologyRevisionAttr(0); got != "" {
		t.Fatalf("revision 0 (unknown) = %q, want empty", got)
	}
	if got := TopologyRevisionAttr(12); got != "12" {
		t.Fatalf("revision 12 = %q", got)
	}
}

func TestFormatWindowStart_SpellsTheHourWithANumericUTCOffset(t *testing.T) {
	loc := time.FixedZone("x", 2*3600)
	got := FormatWindowStart(time.Date(2026, 10, 22, 16, 0, 0, 0, loc))
	if got != refWindow {
		t.Fatalf("got %q, want %q", got, refWindow)
	}
}

// The body must keep an empty hour as [] (delete-by-absence), never null, and
// must carry held as a list.
func TestRequestJSON_EmptyHourIsAnEmptyListNotNull(t *testing.T) {
	b, err := json.Marshal(Request{WindowStart: refWindow, Kinds: []string{KindToken}, Records: []Record{}, Held: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"window_start":"2026-10-22T14:00:00+00:00","kinds":["token"],"records":[],"held":[]}`
	if string(b) != want {
		t.Fatalf("got %s, want %s", b, want)
	}
}
