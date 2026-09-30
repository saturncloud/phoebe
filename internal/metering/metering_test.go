package metering

import "testing"

// TestUnmarshalEvent_OnlyAbsentServingModeDefaultsToDedicated pins the one
// pre-cutover default for stored and queued event JSON: an ABSENT serving_mode
// key (the pre-cutover producer omitted it for dedicated traffic) decodes as
// "dedicated", while an explicit "" or null (a post-cutover producer bug)
// decodes as "" so the rater withholds it instead of billing it as dedicated.
// Known modes decode as sent. This matches internal/recovery.
func TestUnmarshalEvent_OnlyAbsentServingModeDefaultsToDedicated(t *testing.T) {
	cases := []struct {
		name string
		json string
		want string
	}{
		{"absent key is pre-cutover dedicated", `{"request_id":"r"}`, "dedicated"},
		{"explicit empty is withheld", `{"request_id":"r","serving_mode":""}`, ""},
		{"explicit null is withheld", `{"request_id":"r","serving_mode":null}`, ""},
		{"shared as sent", `{"request_id":"r","serving_mode":"shared"}`, "shared"},
		{"dedicated as sent", `{"request_id":"r","serving_mode":"dedicated"}`, "dedicated"},
		{"unknown as sent", `{"request_id":"r","serving_mode":"bogus"}`, "bogus"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev, err := UnmarshalEvent([]byte(tc.json))
			if err != nil {
				t.Fatalf("UnmarshalEvent: %v", err)
			}
			if ev.RequestID != "r" {
				t.Fatalf("request_id = %q, want r", ev.RequestID)
			}
			if ev.ServingMode != tc.want {
				t.Fatalf("serving_mode = %q, want %q", ev.ServingMode, tc.want)
			}
		})
	}
}

func TestUnmarshalEvent_RejectsInvalidJSON(t *testing.T) {
	if _, err := UnmarshalEvent([]byte(`{"request_id":`)); err == nil {
		t.Fatal("UnmarshalEvent accepted truncated JSON")
	}
}
