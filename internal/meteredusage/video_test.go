package meteredusage

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func refVideoAttrs(mod func(*VideoAttributes)) VideoAttributes {
	a := VideoAttributes{
		AuthID: strings.Repeat("a", 32), ModelID: "wan-endpoint", ServingMode: "dedicated",
		BaseModel: "Wan-AI/Wan2.2-T2V-A14B-Diffusers", Task: "t2v", Resolution: "720p", Region: "",
	}
	if mod != nil {
		mod(&a)
	}
	return a
}

func validVideoRecord(mod func(*VideoRecord)) VideoRecord {
	r := VideoRecord{
		Kind: KindVideo, OrgID: refOrg, OwnerType: "user", OwnerID: strings.Repeat("b", 32),
		ResourceID: refResource, Attributes: refVideoAttrs(nil),
		Quantities: VideoQuantities{OutputSeconds: "10.125", OutputFrames: 162, RequestCount: 2, GPUSeconds: "361.5"},
	}
	if mod != nil {
		mod(&r)
	}
	r.UsageID = VideoUsageID(refWindow, r.OrgID, r.OwnerType, r.OwnerID, r.ResourceID, r.Attributes)
	return r
}

func videoRequest(tokens []Record, videos ...VideoRecord) Request {
	if tokens == nil {
		tokens = []Record{}
	}
	return Request{WindowStart: refWindow, Kinds: []string{KindToken, KindVideo}, Records: tokens, VideoRecords: videos, Held: []string{}}
}

// The Python reference (the conventions formula, as in pythonReference) with
// kind "video":
//
//	usage_id("video", "2026-10-22T14:00:00+00:00", "d"*32, "user", "b"*32, "e"*32,
//	         {"auth_id": "a"*32, "model_id": "wan-endpoint", "serving_mode": "dedicated",
//	          "base_model": "Wan-AI/Wan2.2-T2V-A14B-Diffusers", "task": "t2v",
//	          "resolution": "720p", "region": ""})
func TestVideoUsageID_EqualsThePythonReference(t *testing.T) {
	for _, tc := range []struct {
		name               string
		ownerType, ownerID string
		attrs              VideoAttributes
		want               string
	}{
		{"plain", "user", strings.Repeat("b", 32), refVideoAttrs(nil),
			"7b9a6abc7e607d51617bb232b4c86b2cf7536d6cb9b6be34b0b144029ff59afe"},
		{"shared_no_owner_non_ascii", "", "", refVideoAttrs(func(a *VideoAttributes) {
			a.ServingMode, a.Task, a.Resolution, a.Region, a.BaseModel = "shared", "i2v", "4k", "東京", ""
		}), "091dc31130380a31ef87ced0778a46711839a76e0539a8eae4f11e15c22b52f3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := VideoUsageID(refWindow, refOrg, tc.ownerType, tc.ownerID, refResource, tc.attrs); got != tc.want {
				t.Fatalf("usage_id = %s, Python reference = %s", got, tc.want)
			}
		})
	}
}

// The kind is part of the hash: a video record never collides with a token
// record of the same grain strings.
func TestVideoUsageID_DiffersFromTheTokenIDOfTheSameStrings(t *testing.T) {
	a := refVideoAttrs(nil)
	video := VideoUsageID(refWindow, refOrg, "user", strings.Repeat("b", 32), refResource, a)
	asToken := usageID(KindToken, refWindow, refOrg, "user", strings.Repeat("b", 32), refResource, a.attributePairs())
	if video == asToken || asToken != "dce5b26974c2114248463a66569f0e3042e805cad71b5dd082172e4c3d2bc59e" {
		t.Fatalf("video %s, same strings as kind token %s", video, asToken)
	}
}

func TestVideoUsageID_ChangesWhenAnyAttributeChanges(t *testing.T) {
	base := VideoUsageID(refWindow, refOrg, "user", strings.Repeat("b", 32), refResource, refVideoAttrs(nil))
	for name, mod := range map[string]func(*VideoAttributes){
		"auth_id":      func(a *VideoAttributes) { a.AuthID = strings.Repeat("f", 32) },
		"model_id":     func(a *VideoAttributes) { a.ModelID = "other" },
		"serving_mode": func(a *VideoAttributes) { a.ServingMode = "shared" },
		"base_model":   func(a *VideoAttributes) { a.BaseModel = "" },
		"task":         func(a *VideoAttributes) { a.Task = "i2v" },
		"resolution":   func(a *VideoAttributes) { a.Resolution = "480p" },
		"region":       func(a *VideoAttributes) { a.Region = "us-central1" },
	} {
		if VideoUsageID(refWindow, refOrg, "user", strings.Repeat("b", 32), refResource, refVideoAttrs(mod)) == base {
			t.Errorf("changing %s did not change the usage_id", name)
		}
	}
}

// The wire names are one-way doors (design 7.3 and section 11): the JSON tags,
// the hashed keys and the design's list must be the same, in the same order.
func TestVideoAttributeKeysMatchJSONTags(t *testing.T) {
	var fromTags []string
	typ := reflect.TypeOf(VideoAttributes{})
	for i := 0; i < typ.NumField(); i++ {
		fromTags = append(fromTags, typ.Field(i).Tag.Get("json"))
	}
	var fromPairs []string
	for _, kv := range (VideoAttributes{}).attributePairs() {
		fromPairs = append(fromPairs, kv[0])
	}
	want := []string{"auth_id", "model_id", "serving_mode", "base_model", "task", "resolution", "region"}
	if !reflect.DeepEqual(fromTags, fromPairs) || !reflect.DeepEqual(fromTags, want) {
		t.Fatalf("json tags %v, hashed keys %v, design 7.3 says %v", fromTags, fromPairs, want)
	}
}

// The record on the wire, byte for byte: decimal quantities are strings,
// integer quantities are numbers, and an unknown region is "".
func TestVideoRecordJSON_SpellsTheDesignsWireRecord(t *testing.T) {
	rec := validVideoRecord(nil)
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"usage_id":"` + rec.UsageID + `","kind":"video","org_id":"` + refOrg + `","owner_type":"user","owner_id":"` +
		strings.Repeat("b", 32) + `","resource_id":"` + refResource + `","attributes":{"auth_id":"` + strings.Repeat("a", 32) +
		`","model_id":"wan-endpoint","serving_mode":"dedicated","base_model":"Wan-AI/Wan2.2-T2V-A14B-Diffusers","task":"t2v","resolution":"720p","region":""},` +
		`"quantities":{"output_seconds":"10.125","output_frames":162,"request_count":2,"gpu_seconds":"361.5"}}`
	if string(b) != want {
		t.Fatalf("got\n%s\nwant\n%s", b, want)
	}
}

// Token and video records share one "records" list on the wire, and a body
// read back splits them by kind.
func TestRequestJSON_CarriesBothKindsInOneRecordsListAndRoundTrips(t *testing.T) {
	req := videoRequest([]Record{validRecord(nil)}, validVideoRecord(nil))
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var generic struct {
		Kinds   []string         `json:"kinds"`
		Records []map[string]any `json:"records"`
		Held    []string         `json:"held"`
	}
	if err := json.Unmarshal(b, &generic); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(generic.Kinds, []string{"token", "video"}) || len(generic.Records) != 2 ||
		generic.Records[0]["kind"] != "token" || generic.Records[1]["kind"] != "video" || generic.Held == nil {
		t.Fatalf("body %s", b)
	}
	var back Request
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, req) {
		t.Fatalf("round trip %+v, want %+v", back, req)
	}
	if err := Validate(back); err != nil {
		t.Fatal(err)
	}
}

// An hour with video listed and no usage of either kind is "records": [].
func TestRequestJSON_EmptyHourWithVideoListedIsAnEmptyList(t *testing.T) {
	b, err := json.Marshal(videoRequest(nil))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"records":[]`) || !strings.Contains(string(b), `"kinds":["token","video"]`) {
		t.Fatalf("body %s", b)
	}
}

func TestValidate_AcceptsAnHourWithBothKinds(t *testing.T) {
	if err := Validate(videoRequest([]Record{validRecord(nil)}, validVideoRecord(nil))); err != nil {
		t.Fatal(err)
	}
	// Video only, token set empty.
	if err := Validate(videoRequest(nil, validVideoRecord(nil))); err != nil {
		t.Fatal(err)
	}
}

func TestValidate_RefusesBadVideoRecords(t *testing.T) {
	for _, c := range []struct {
		name string
		req  Request
		path string
	}{
		{"video not listed in kinds", func() Request {
			r := videoRequest(nil, validVideoRecord(nil))
			r.Kinds = []string{KindToken}
			return r
		}(), "records[0].kind"},
		{"path continues after the token records", videoRequest([]Record{validRecord(nil)},
			validVideoRecord(func(r *VideoRecord) { r.Attributes.Task = "v2v" })), "records[1].attributes.task"},
		{"empty auth", videoRequest(nil, validVideoRecord(func(r *VideoRecord) { r.Attributes.AuthID = "" })), "records[0].attributes.auth_id"},
		{"empty model", videoRequest(nil, validVideoRecord(func(r *VideoRecord) { r.Attributes.ModelID = "" })), "records[0].attributes.model_id"},
		{"bad serving mode", videoRequest(nil, validVideoRecord(func(r *VideoRecord) { r.Attributes.ServingMode = "" })), "records[0].attributes.serving_mode"},
		{"bad resolution", videoRequest(nil, validVideoRecord(func(r *VideoRecord) { r.Attributes.Resolution = "720" })), "records[0].attributes.resolution"},
		{"uppercase 4K", videoRequest(nil, validVideoRecord(func(r *VideoRecord) { r.Attributes.Resolution = "4K" })), "records[0].attributes.resolution"},
		{"negative seconds", videoRequest(nil, validVideoRecord(func(r *VideoRecord) { r.Quantities.OutputSeconds = "-1" })), "records[0].quantities.output_seconds"},
		{"exponent", videoRequest(nil, validVideoRecord(func(r *VideoRecord) { r.Quantities.GPUSeconds = "1e3" })), "records[0].quantities.gpu_seconds"},
		{"empty decimal", videoRequest(nil, validVideoRecord(func(r *VideoRecord) { r.Quantities.GPUSeconds = "" })), "records[0].quantities.gpu_seconds"},
		{"ten fractional digits", videoRequest(nil, validVideoRecord(func(r *VideoRecord) { r.Quantities.OutputSeconds = "1.0000000001" })), "records[0].quantities.output_seconds"},
		{"negative frames", videoRequest(nil, validVideoRecord(func(r *VideoRecord) { r.Quantities.OutputFrames = -1 })), "records[0].quantities.output_frames"},
		{"no resource", videoRequest(nil, validVideoRecord(func(r *VideoRecord) { r.ResourceID = "" })), "records[0].resource_id"},
		{"no org", videoRequest(nil, validVideoRecord(func(r *VideoRecord) { r.OrgID = "" })), "records[0].org_id"},
		{"wrong id", func() Request {
			v := validVideoRecord(nil)
			v.Attributes.Region = "us-central1"
			return videoRequest(nil, v)
		}(), "records[0].usage_id"},
		{"duplicate across kinds", func() Request {
			v := validVideoRecord(nil)
			return videoRequest(nil, v, v)
		}(), "records[1].usage_id"},
	} {
		err := Validate(c.req)
		ve, ok := err.(*ValidationError)
		if !ok || ve.Path != c.path {
			t.Errorf("%s: err = %v, want a ValidationError at %s", c.name, err, c.path)
		}
	}
}

// The record cap counts both kinds: one request carries the whole hour.
func TestValidate_TheRecordCapCountsBothKinds(t *testing.T) {
	tokens := make([]Record, MaxRecordsPerRequest)
	req := videoRequest(tokens, validVideoRecord(nil))
	if err := Validate(req); err == nil || !strings.Contains(err.Error(), "cap") {
		t.Fatalf("err = %v, want the cap", err)
	}
}

func TestCanonicalDecimal_DropsOnlyTrailingFractionalZeros(t *testing.T) {
	for in, want := range map[string]string{
		"10.1250": "10.125", "0.0000": "0", "0.000": "0", "5.0625": "5.0625", "100": "100", "120.500": "120.5", "10.0": "10",
	} {
		if got := CanonicalDecimal(in); got != want {
			t.Errorf("CanonicalDecimal(%q) = %q, want %q", in, got, want)
		}
	}
}
