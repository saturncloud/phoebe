package meteredusage

import (
	"errors"
	"strings"
	"testing"
)

func validRecord(mod func(*Record)) Record {
	r := Record{
		Kind: KindToken, OrgID: refOrg, OwnerType: "user", OwnerID: strings.Repeat("b", 32),
		ResourceID: refResource, Attributes: refAttrs(func(*TokenAttributes) {}),
		Quantities: TokenQuantities{PromptTokens: 100, CachedTokens: 40, CompletionTokens: 20, RequestCount: 3},
	}
	if mod != nil {
		mod(&r)
	}
	r.UsageID = TokenUsageID(refWindow, r.OrgID, r.OwnerType, r.OwnerID, r.ResourceID, r.Attributes)
	return r
}

func validRequest(records ...Record) Request {
	if records == nil {
		records = []Record{}
	}
	return Request{WindowStart: refWindow, Kinds: []string{KindToken}, Records: records, Held: []string{}}
}

func TestValidate_AcceptsAWellFormedHour(t *testing.T) {
	r2 := validRecord(func(r *Record) { r.Attributes.ServingMode = "shared"; r.OwnerType, r.OwnerID = "", "" })
	if err := Validate(validRequest(validRecord(nil), r2)); err != nil {
		t.Fatal(err)
	}
}

func TestValidate_AcceptsAnEmptyHour(t *testing.T) {
	if err := Validate(validRequest()); err != nil {
		t.Fatal(err)
	}
}

// One row per wire rule in wire-conventions.md. Each request must be refused
// on the install, naming the path the manager's 400 would name.
func TestValidate_RefusesWhatTheManagerWouldRejectWith400(t *testing.T) {
	long := strings.Repeat("x", MaxAttributeChars+1)
	cases := []struct {
		name     string
		req      Request
		wantPath string
	}{
		{"window not ISO", func() Request { r := validRequest(); r.WindowStart = "yesterday"; return r }(), "window_start"},
		{"window not whole hour", func() Request { r := validRequest(); r.WindowStart = "2026-10-22T14:30:00+00:00"; return r }(), "window_start"},
		{"window spelled with Z", func() Request { r := validRequest(); r.WindowStart = "2026-10-22T14:00:00Z"; return r }(), "window_start"},
		{"no kinds", func() Request { r := validRequest(); r.Kinds = nil; return r }(), "kinds"},
		{"unknown kind", func() Request { r := validRequest(); r.Kinds = []string{"vm"}; return r }(), "kinds[0]"},
		{"duplicate kind", func() Request { r := validRequest(); r.Kinds = []string{"token", "token"}; return r }(), "kinds[1]"},
		{"records null", func() Request { r := validRequest(); r.Records = nil; return r }(), "records"},
		{"held null", func() Request { r := validRequest(); r.Held = nil; return r }(), "held"},
		{"held not a usage_id", func() Request { r := validRequest(); r.Held = []string{"abc"}; return r }(), "held[0]"},
		{"record kind not listed", func() Request {
			r := validRequest(validRecord(nil))
			r.Kinds = []string{"compute"}
			return r
		}(), "records[0].kind"},
		{"org not hex", validRequest(validRecord(func(r *Record) { r.OrgID = "ORG" })), "records[0].org_id"},
		{"org empty", validRequest(validRecord(func(r *Record) { r.OrgID = "" })), "records[0].org_id"},
		{"owner type unknown", validRequest(validRecord(func(r *Record) { r.OwnerType = "team" })), "records[0].owner_type"},
		{"owner id missing for user", validRequest(validRecord(func(r *Record) { r.OwnerID = "" })), "records[0].owner_id"},
		{"owner id without type", validRequest(validRecord(func(r *Record) { r.OwnerType = "" })), "records[0].owner_id"},
		{"resource empty", validRequest(validRecord(func(r *Record) { r.ResourceID = "" })), "records[0].resource_id"},
		{"auth_id empty", validRequest(validRecord(func(r *Record) { r.Attributes.AuthID = "" })), "records[0].attributes.auth_id"},
		{"model_id empty", validRequest(validRecord(func(r *Record) { r.Attributes.ModelID = "" })), "records[0].attributes.model_id"},
		{"base_model empty", validRequest(validRecord(func(r *Record) { r.Attributes.BaseModel = "" })), "records[0].attributes.base_model"},
		{"serving mode outside enum", validRequest(validRecord(func(r *Record) { r.Attributes.ServingMode = "SHARED" })), "records[0].attributes.serving_mode"},
		{"attribute too long", validRequest(validRecord(func(r *Record) { r.Attributes.Region = long })), "records[0].attributes.region"},
		{"attribute not UTF-8", validRequest(validRecord(func(r *Record) { r.Attributes.ModelID = "m\xff" })), "records[0].attributes.model_id"},
		{"negative quantity", validRequest(validRecord(func(r *Record) { r.Quantities.CachedTokens = -1 })), "records[0].quantities.cached_tokens"},
		{"duplicate usage_id", validRequest(validRecord(nil), validRecord(nil)), "records[1].usage_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.req)
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("Validate = %v, want a ValidationError at %s", err, tc.wantPath)
			}
			if ve.Path != tc.wantPath {
				t.Fatalf("path = %s (%s), want %s", ve.Path, ve.Msg, tc.wantPath)
			}
		})
	}
}

// 256 characters is the cap in characters, not bytes: a 256-character
// non-ASCII string is legal although it is longer than 256 bytes.
func TestValidate_CountsTheAttributeCapInCharactersNotBytes(t *testing.T) {
	r := validRecord(func(r *Record) { r.Attributes.Region = strings.Repeat("é", MaxAttributeChars) })
	if err := Validate(validRequest(r)); err != nil {
		t.Fatal(err)
	}
}

func TestValidate_RefusesARecordWhoseUsageIDDoesNotMatchItsGrain(t *testing.T) {
	r := validRecord(nil)
	r.Attributes.Region = "elsewhere" // grain changed after the id was computed
	err := Validate(validRequest(r))
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Path != "records[0].usage_id" {
		t.Fatalf("Validate = %v, want a usage_id mismatch", err)
	}
}

func TestValidate_RefusesAnHourAboveTheRecordCap(t *testing.T) {
	recs := make([]Record, MaxRecordsPerRequest+1)
	err := Validate(validRequest(recs...))
	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Path != "records" {
		t.Fatalf("Validate = %v, want the records cap", err)
	}
}
