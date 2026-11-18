package main

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/meteredusage"
)

// idVideo is a dedicated video endpoint's identity. idVideoOnFT shares every
// identity field with the token fine-tune endpoint idFT, which a real endpoint
// cannot, so the per-kind holding is tested at its hardest.
var (
	idVideo     = identity{authA, "user", userA, deployC, "acme-wan", "dedicated"}
	idVideoOnFT = idFT
)

func videoRollup(id string, idn identity, resolution, seconds, gpu string, frames, requests int64) videoRollupRow {
	return videoRollupRow{ID: id, identity: idn, OrgID: orgA, BaseModel: "Wan-AI/Wan2.2-T2V-A14B-Diffusers",
		Task: "t2v", Resolution: resolution, Region: "", OutputSeconds: seconds, GPUSeconds: gpu,
		OutputFrames: frames, Requests: requests}
}

func videoStore() *fakeStore {
	f := fixtureStore()
	f.hasVideo = true
	f.videoRollups[win] = []videoRollupRow{
		videoRollup("vid-1", idVideo, "720p", "10.1250", "361.500", 162, 2),
		// Failed jobs only: zero output, request_count 0, GPU time kept.
		videoRollup("vid-2", idVideo, "480p", "0.0000", "30.000", 0, 0),
	}
	return f
}

func push1(t *testing.T, f *fakeStore, m *recordingManager, hours ...time.Time) int {
	t.Helper()
	return newPusher(f, m.client(t)).pushWindows(context.Background(), hours, now)
}

// Video recorded: the hour lists kinds ["token", "video"] and carries both
// kinds' records, the decimals in their shortest exact spelling and each video
// usage_id from the video hash rule.
func TestPushWindows_SendsTokenAndVideoRecordsInOneRequest(t *testing.T) {
	m := &recordingManager{}
	if code := push1(t, videoStore(), m, win); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	req := sentRequest(t, m.bodies[0])
	if !reflect.DeepEqual(req.Kinds, []string{"token", "video"}) || len(req.Records) != 4 || len(req.VideoRecords) != 2 {
		t.Fatalf("kinds %v, %d token and %d video records", req.Kinds, len(req.Records), len(req.VideoRecords))
	}
	v := req.VideoRecords[0]
	want := meteredusage.VideoAttributes{AuthID: authA, ModelID: "acme-wan", ServingMode: "dedicated",
		BaseModel: "Wan-AI/Wan2.2-T2V-A14B-Diffusers", Task: "t2v", Resolution: "720p", Region: ""}
	if v.Kind != "video" || v.OrgID != orgA || v.ResourceID != deployC || v.Attributes != want {
		t.Fatalf("video record %+v", v)
	}
	if v.Quantities != (meteredusage.VideoQuantities{OutputSeconds: "10.125", OutputFrames: 162, RequestCount: 2, GPUSeconds: "361.5"}) {
		t.Fatalf("quantities %+v", v.Quantities)
	}
	if q := req.VideoRecords[1].Quantities; q.OutputSeconds != "0" || q.RequestCount != 0 || q.GPUSeconds != "30" {
		t.Fatalf("failed-only rollup quantities %+v, want zero output and its GPU time", q)
	}
	for _, r := range req.VideoRecords {
		if r.UsageID != meteredusage.VideoUsageID(req.WindowStart, r.OrgID, r.OwnerType, r.OwnerID, r.ResourceID, r.Attributes) {
			t.Fatalf("usage_id %s does not follow the video hash rule", r.UsageID)
		}
	}
	if !strings.Contains(string(m.bodies[0]), `"output_seconds":"10.125"`) || !strings.Contains(string(m.bodies[0]), `"output_frames":162`) {
		t.Fatalf("body does not spell the decimals as strings and frames as integers: %s", m.bodies[0])
	}
}

// Once video is recorded, an hour with token usage and no video usage still
// lists "video" (so the manager deletes, by absence, video records the rater
// reconciled away), and an hour with only video usage sends an empty token
// set.
func TestPushWindows_AnHourWithOnlyOneKindStillListsBoth(t *testing.T) {
	f := videoStore()
	videoOnly := win.Add(time.Hour)
	f.videoRollups[videoOnly] = f.videoRollups[win]
	delete(f.videoRollups, win)
	m := &recordingManager{}
	if code := push1(t, f, m, win, videoOnly); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	tokensOnly, onlyVideo := sentRequest(t, m.bodies[0]), sentRequest(t, m.bodies[1])
	for _, r := range []meteredusage.Request{tokensOnly, onlyVideo} {
		if !reflect.DeepEqual(r.Kinds, []string{"token", "video"}) {
			t.Fatalf("kinds %v, want both", r.Kinds)
		}
	}
	if len(tokensOnly.Records) != 4 || len(tokensOnly.VideoRecords) != 0 || len(onlyVideo.Records) != 0 || len(onlyVideo.VideoRecords) != 2 {
		t.Fatalf("token hour %d/%d, video hour %d/%d", len(tokensOnly.Records), len(tokensOnly.VideoRecords),
			len(onlyVideo.Records), len(onlyVideo.VideoRecords))
	}
	if !strings.Contains(string(m.bodies[0]), `"records":[{`) {
		t.Fatalf("token-only hour body %s", m.bodies[0])
	}
}

// No video recorded on the install: requests are exactly what they were
// before video existed (kinds ["token"]), so an install that never ran video
// keeps pushing against a manager that does not know the video kind.
func TestPushWindows_NoVideoRecordedSendsTokenKindOnly(t *testing.T) {
	m := &recordingManager{}
	if code := push1(t, fixtureStore(), m, win); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	if req := sentRequest(t, m.bodies[0]); !reflect.DeepEqual(req.Kinds, []string{"token"}) || len(req.VideoRecords) != 0 {
		t.Fatalf("kinds %v with %d video records", req.Kinds, len(req.VideoRecords))
	}
}

// A video identity the rater parks after it was sent: its video usage_ids go
// in held. Holding is per kind, so the token records of an identity with the
// same (auth, owner, resource, model, serving mode) are not held, and a parked
// token identity does not hold video records.
func TestPushWindows_AParkedVideoIdentityHoldsOnlyItsVideoRecords(t *testing.T) {
	f := videoStore()
	f.videoRollups[win] = []videoRollupRow{videoRollup("vid-ft", idVideoOnFT, "720p", "5.0625", "60.000", 81, 1)}
	m := &recordingManager{}
	if code := push1(t, f, m, win); code != exitOK {
		t.Fatalf("first push: exit %d", code)
	}
	first := sentRequest(t, m.bodies[0])
	videoID := first.VideoRecords[0].UsageID

	// The rater parks the video identity; idFT's token rollups are unchanged
	// but one of them was reconciled away (it must NOT be held).
	f.videoRollups[win] = nil
	f.videoParked[win] = []identity{idVideoOnFT}
	f.rollups[win] = fixtureRollups()[1:]
	if code := push1(t, f, m, win); code != exitOK {
		t.Fatalf("second push: exit %d", code)
	}
	second := sentRequest(t, m.bodies[1])
	if !reflect.DeepEqual(second.Held, []string{videoID}) || len(second.VideoRecords) != 0 {
		t.Fatalf("held %v with %d video records, want only the parked video record [%s]", second.Held, len(second.VideoRecords), videoID)
	}

	// The reverse: the token identity parked, the video identity sent again.
	f.videoParked[win] = nil
	f.videoRollups[win] = []videoRollupRow{videoRollup("vid-ft", idVideoOnFT, "720p", "5.0625", "60.000", 81, 1)}
	f.parked[win] = []identity{idFT}
	f.rollups[win] = fixtureRollups()[2:]
	if code := push1(t, f, m, win); code != exitOK {
		t.Fatalf("third push: exit %d", code)
	}
	third := sentRequest(t, m.bodies[2])
	for _, h := range third.Held {
		if h == videoID {
			t.Fatalf("held %v includes the video record, which is being sent", third.Held)
		}
	}
	if len(third.Held) != 1 || len(third.VideoRecords) != 1 {
		t.Fatalf("held %v, %d video records; want idFT's one remaining token record held and the video record sent", third.Held, len(third.VideoRecords))
	}
}

// A video rollup token-push cannot send holds back only its own video
// identity; everything else of the hour is sent and the run exits 2.
func TestPushWindows_AnUnsendableVideoRollupHoldsBackOnlyItsIdentity(t *testing.T) {
	f := videoStore()
	bad := videoRollup("vid-bad", identity{authB, "", "", deployA, "other-wan", "shared"}, "720p", "5.0625", "60.000", 81, 1)
	bad.OrgID = "not-hex"
	f.videoRollups[win] = append(f.videoRollups[win], bad)
	m := &recordingManager{}
	if code := push1(t, f, m, win); code != exitWithheld {
		t.Fatalf("exit %d, want %d", code, exitWithheld)
	}
	req := sentRequest(t, m.bodies[0])
	if len(req.Records) != 4 || len(req.VideoRecords) != 2 {
		t.Fatalf("%d token and %d video records, want the clean ones sent", len(req.Records), len(req.VideoRecords))
	}
}

// pushed_usage records each id's kind, and accepting a request trims only the
// kinds it listed.
func TestPushWindows_RecordsTheKindOfEverySentID(t *testing.T) {
	f := videoStore()
	m := &recordingManager{}
	if code := push1(t, f, m, win); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	kinds := map[string]int{}
	for _, r := range f.pushed[win] {
		kinds[r.kind()]++
	}
	if kinds["token"] != 4 || kinds["video"] != 2 {
		t.Fatalf("pushed kinds %v, want 4 token and 2 video", kinds)
	}
}

// Video was sent before and the video tables are now empty (a wipe): video is
// left out of the request so the manager's video records are not deleted by
// absence, tokens are still sent, the video bookkeeping is kept, and the run
// exits 1.
func TestPushWindows_WipedVideoTablesSendTokensOnlyAndFail(t *testing.T) {
	f := videoStore()
	m := &recordingManager{}
	if code := push1(t, f, m, win); code != exitOK {
		t.Fatalf("first push: exit %d", code)
	}
	f.hasVideo = false
	f.videoRollups = map[time.Time][]videoRollupRow{}
	if code := push1(t, f, m, win); code != exitFatal {
		t.Fatalf("exit %d, want %d", code, exitFatal)
	}
	req := sentRequest(t, m.bodies[1])
	if !reflect.DeepEqual(req.Kinds, []string{"token"}) || len(req.Records) != 4 || len(req.Held) != 0 {
		t.Fatalf("kinds %v, %d records, held %v; want the token hour only", req.Kinds, len(req.Records), req.Held)
	}
	videoLeft := 0
	for _, r := range f.pushed[win] {
		if r.kind() == "video" {
			videoLeft++
		}
	}
	if videoLeft != 2 {
		t.Fatalf("%d video ids left in pushed_usage, want both kept (the request did not list video)", videoLeft)
	}
}

// An install whose only usage is video: the liveness guard counts video.
func TestPushWindows_AVideoOnlyInstallIsNotTreatedAsEmpty(t *testing.T) {
	f := newFake()
	f.hasVideo = true
	f.videoRollups[win] = []videoRollupRow{videoRollup("vid-1", idVideo, "720p", "5.0625", "60.000", 81, 1)}
	m := &recordingManager{}
	if code := push1(t, f, m, win); code != exitOK || len(m.bodies) != 1 {
		t.Fatalf("exit %d with %d requests", code, len(m.bodies))
	}
	if req := sentRequest(t, m.bodies[0]); len(req.Records) != 0 || len(req.VideoRecords) != 1 {
		t.Fatalf("%d token and %d video records", len(req.Records), len(req.VideoRecords))
	}
}

func TestVideoHourQuery_ReadsQuantitiesAsExactText(t *testing.T) {
	for _, want := range []string{"output_seconds::text", "gpu_seconds::text"} {
		if !strings.Contains(videoHourQuery, want) {
			t.Fatalf("videoHourQuery does not read %s:\n%s", want, videoHourQuery)
		}
	}
}
