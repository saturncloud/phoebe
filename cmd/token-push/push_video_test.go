package main

import (
	"context"
	"reflect"
	"sort"
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
	return newPusher(f, m.client(t)).pushWindows(context.Background(), hours, now, false)
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

// Once video is rated, an hour lists "video" only if it has video of its own
// or this install sent the manager video records for it before:
//   - a token-only hour that never had video lists ["token"], so the manager's
//     video records for that hour that did not come from this database are
//     not deleted by absence (the per-kind form of the empty-hour rule);
//   - a token-only hour whose video rollups the rater reconciled away still
//     lists "video" with an empty set, so the manager deletes those records;
//   - a video-only hour sends an empty token set beside its video records.
func TestPushWindows_ListsVideoOnlyForHoursWithVideoOrSentVideo(t *testing.T) {
	f := videoStore()
	videoOnly := win.Add(time.Hour)
	videoGone := win.Add(2 * time.Hour)
	f.videoRollups[videoOnly] = f.videoRollups[win]
	delete(f.videoRollups, win)
	f.rollups[videoGone] = f.rollups[win]
	f.pushed[videoGone] = []pushedRow{{UsageID: strings.Repeat("e", 64), Kind: meteredusage.KindVideo, identity: idVideo}}
	f.tombstones = map[time.Time][]string{videoGone: {strings.Repeat("e", 64)}} // the rater removed it
	m := &recordingManager{}
	if code := push1(t, f, m, win, videoOnly, videoGone); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	tokensOnly, onlyVideo, gone := sentRequest(t, m.bodies[0]), sentRequest(t, m.bodies[1]), sentRequest(t, m.bodies[2])
	for _, tc := range []struct {
		name          string
		req           meteredusage.Request
		kinds         []string
		token, videos int
	}{
		{"token-only hour, never sent video", tokensOnly, []string{"token"}, 4, 0},
		{"video-only hour", onlyVideo, []string{"token", "video"}, 0, 2},
		{"token-only hour whose sent video vanished", gone, []string{"token", "video"}, 4, 0},
	} {
		if !reflect.DeepEqual(tc.req.Kinds, tc.kinds) || len(tc.req.Records) != tc.token || len(tc.req.VideoRecords) != tc.videos {
			t.Errorf("%s: kinds %v, %d token and %d video records; want %v, %d and %d",
				tc.name, tc.req.Kinds, len(tc.req.Records), len(tc.req.VideoRecords), tc.kinds, tc.token, tc.videos)
		}
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

// Video was sent before and the video tables are now empty (a wipe): there is
// no tombstone for the sent ids, so they are held and the manager keeps its
// video records. Tokens are still sent, the video bookkeeping is kept, and the
// run exits 1.
func TestPushWindows_WipedVideoTablesHoldEverySentVideoRecord(t *testing.T) {
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
	if !reflect.DeepEqual(req.Kinds, []string{"token", "video"}) || len(req.Records) != 4 ||
		len(req.VideoRecords) != 0 || len(req.Held) != 2 {
		t.Fatalf("kinds %v, %d token and %d video records, held %v; want the tokens sent and both video ids held",
			req.Kinds, len(req.Records), len(req.VideoRecords), req.Held)
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

// An install whose only usage is video: the liveness guard counts video, both
// before its first accepted hour and after (pushed_hour then has rows although
// rated_usage is empty, which for a token install means a wiped table).
func TestPushWindows_AVideoOnlyInstallIsNotTreatedAsEmpty(t *testing.T) {
	f := newFake()
	f.empty = true
	f.hasVideo = true
	f.videoRollups[win] = []videoRollupRow{videoRollup("vid-1", idVideo, "720p", "5.0625", "60.000", 81, 1)}
	m := &recordingManager{}
	if code := push1(t, f, m, win); code != exitOK || len(m.bodies) != 1 {
		t.Fatalf("exit %d with %d requests", code, len(m.bodies))
	}
	if req := sentRequest(t, m.bodies[0]); len(req.Records) != 0 || len(req.VideoRecords) != 1 {
		t.Fatalf("%d token and %d video records", len(req.Records), len(req.VideoRecords))
	}
	f.everPushed = true
	if code := push1(t, f, m, win); code != exitOK || len(m.bodies) != 2 {
		t.Fatalf("second run: exit %d with %d requests, want the video hour sent again", code, len(m.bodies))
	}
}

// rated_usage wiped on an install that also has video: token records were
// sent before (pushed_usage has token ids), so the run refuses to push at all,
// as on a token-only install, instead of sending hours with an empty token set.
func TestPushWindows_WipedTokenTablesRefuseEvenWithVideo(t *testing.T) {
	f := videoStore()
	m := &recordingManager{}
	if code := push1(t, f, m, win); code != exitOK {
		t.Fatalf("first push: exit %d", code)
	}
	f.empty, f.everPushed = true, true
	f.rollups = map[time.Time][]rollupRow{}
	if code := push1(t, f, m, win); code != exitFatal || len(m.bodies) != 1 {
		t.Fatalf("exit %d with %d requests, want %d and no new request", code, len(m.bodies), exitFatal)
	}
}

func TestVideoHourQuery_ReadsQuantitiesAsExactText(t *testing.T) {
	for _, want := range []string{"output_seconds::text", "gpu_seconds::text"} {
		if !strings.Contains(videoHourQuery, want) {
			t.Fatalf("videoHourQuery does not read %s:\n%s", want, videoHourQuery)
		}
	}
}

// Nine-decimal sums from rated_video_usage reach the wire in their shortest
// exact spelling: trailing zeros go, significant digits never do.
func TestPushWindows_NineDecimalSecondsAreSentShortestAndExact(t *testing.T) {
	f := newFake()
	f.hasVideo = true
	f.videoRollups[win] = []videoRollupRow{
		videoRollup("vid-24fps", idVideo, "720p", "15.125000001", "181.500", 363, 3),
		videoRollup("vid-16fps", idVideo, "480p", "10.125000000", "0.000", 162, 2),
	}
	m := &recordingManager{}
	if code := push1(t, f, m, win); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	body := string(m.bodies[0])
	for _, want := range []string{`"output_seconds":"15.125000001"`, `"output_seconds":"10.125"`, `"gpu_seconds":"181.5"`, `"gpu_seconds":"0"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("body lacks %s: %s", want, body)
		}
	}
}

// A video record this install sent for an hour, with no rollup now, is let
// go (the hour's video set no longer names it, so the manager deletes it) ONLY
// when the rater recorded removing it: a tombstone for its usage_id. Without
// one it was lost, whatever the hour's age or the state of the other tables:
// its id is held so the manager keeps the record, and the run exits 1. A
// parked identity's id is held even with a tombstone (the park holds it until
// it resolves). Decided per id: another grain of the same identity, live in
// the same hour, does not release it.
func TestPushWindows_AVanishedVideoRecordIsDeletedOnlyWithATombstone(t *testing.T) {
	sentID := strings.Repeat("e", 64)
	sentVideo := []pushedRow{{UsageID: sentID, Kind: meteredusage.KindVideo, identity: idVideo}}
	for _, tc := range []struct {
		name       string
		tombstone  bool
		parked     bool
		otherGrain bool
		wantHeld   []string
		wantExit   int
	}{
		{"the rater removed it: tombstone, deleted by absence", true, false, false, nil, exitOK},
		{"lost: no tombstone, held", false, false, false, []string{sentID}, exitFatal},
		{"lost while the same identity keeps another grain: held", false, false, true, []string{sentID}, exitFatal},
		{"tombstone but the identity is parked: held by the park", true, true, false, []string{sentID}, exitOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := videoStore()
			delete(f.videoRollups, win)
			f.pushed[win] = sentVideo
			if tc.tombstone {
				f.tombstones = map[time.Time][]string{win: {sentID}}
			}
			if tc.parked {
				f.videoParked[win] = []identity{idVideo}
			}
			if tc.otherGrain {
				f.videoRollups[win] = videoStore().videoRollups[win][:1] // idVideo at 720p
			}
			m := &recordingManager{}
			if code := push1(t, f, m, win); code != tc.wantExit {
				t.Fatalf("exit %d, want %d", code, tc.wantExit)
			}
			req := sentRequest(t, m.bodies[0])
			if !reflect.DeepEqual(req.Kinds, []string{"token", "video"}) || len(req.Held) != len(tc.wantHeld) ||
				(len(tc.wantHeld) > 0 && !reflect.DeepEqual(req.Held, tc.wantHeld)) {
				t.Fatalf("kinds %v held %v, want [token video] and %v", req.Kinds, req.Held, tc.wantHeld)
			}
			// A held id is kept through RecordAccepted's trim; a released id is not.
			kept := false
			for _, r := range f.pushed[win] {
				kept = kept || r.UsageID == sentID
			}
			if kept != (len(tc.wantHeld) > 0) {
				t.Fatalf("sent id kept in pushed_usage = %v, want %v", kept, len(tc.wantHeld) > 0)
			}
		})
	}
}

// A rollup whose stored usage_id is not the id of its own fields (edited
// outside the rater, or rated under another usage_id rule, or never stamped)
// is held back, not sent under its new id: sending it would leave the old
// record at the manager with no tombstone, a double charge.
func TestPushWindows_ARollupWhoseStoredUsageIDDiffersIsHeldBack(t *testing.T) {
	for _, stored := range []string{strings.Repeat("d", 64), unstampedVideoID} {
		f := videoStore()
		f.videoRollups[win][0].UsageID = stored
		m := &recordingManager{}
		if code := push1(t, f, m, win); code != exitWithheld {
			t.Fatalf("stored %q: exit %d, want exitWithheld", stored, code)
		}
		req := sentRequest(t, m.bodies[0])
		for _, r := range req.VideoRecords {
			if r.Attributes.Resolution == "720p" {
				t.Fatalf("stored %q: the mismatched rollup was sent", stored)
			}
		}
	}
}

// Wiped video tables fail every run (exit 1) even when no hour the run sends
// had video: the scheduled run otherwise exits 0 and the loss of the only
// durable video record goes unpaged.
func TestPushWindows_WipedVideoTablesFailEvenWithoutAVideoHourInTheRun(t *testing.T) {
	f := fixtureStore()
	f.everPushedVideo = true // video was sent for some older hour
	m := &recordingManager{}
	if code := push1(t, f, m, win); code != exitFatal {
		t.Fatalf("exit %d, want %d", code, exitFatal)
	}
	if req := sentRequest(t, m.bodies[0]); len(req.Records) != 4 || len(req.Held) != 0 {
		t.Fatalf("%d token records, held %v; want the token hour sent unchanged", len(req.Records), req.Held)
	}
}

// A video-only install (never sent a token record) whose video rollups were
// all removed is not refused as a wiped token table: it pushes, so the
// tombstoned records are released.
func TestPushWindows_AVideoOnlyInstallWithEveryRollupRemovedStillPushes(t *testing.T) {
	sentID := strings.Repeat("e", 64)
	f := newFake()
	f.empty, f.everPushed = true, true
	f.pushed[win] = []pushedRow{{UsageID: sentID, Kind: meteredusage.KindVideo, identity: idVideo}}
	f.tombstones = map[time.Time][]string{win: {sentID}}
	m := &recordingManager{}
	if code := push1(t, f, m, win); code != exitOK || len(m.bodies) != 1 {
		t.Fatalf("exit %d with %d requests, want 0 and the hour sent (an all-removed install is not wiped)", code, len(m.bodies))
	}
	if req := sentRequest(t, m.bodies[0]); !reflect.DeepEqual(req.Kinds, []string{"token", "video"}) || len(req.Held) != 0 {
		t.Fatalf("kinds %v held %v, want video listed empty so the tombstoned record is deleted", req.Kinds, req.Held)
	}
}

// A video rollup token-push holds back holds its whole identity, tombstones
// or not: while one of its rollups cannot be sent, the manager keeps every
// record of the identity. Releasing a regrained predecessor here would leave
// the grain with no record at all until a person fixes the rollup.
func TestPushWindows_AHeldBackVideoRollupHoldsItsIdentityEvenWithTombstones(t *testing.T) {
	f := videoStore()
	rows := f.videoRollups[win]
	bad := rows[0]
	bad.OrgID = "not-hex" // fails the wire rule
	f.videoRollups[win] = []videoRollupRow{bad}
	badID := stampedVideoID(win, bad)
	predecessor := strings.Repeat("e", 64) // the grain's old id, regrained by the rater
	f.pushed[win] = []pushedRow{
		{UsageID: badID, Kind: meteredusage.KindVideo, identity: idVideo},
		{UsageID: predecessor, Kind: meteredusage.KindVideo, identity: idVideo},
	}
	f.tombstones = map[time.Time][]string{win: {predecessor}}
	m := &recordingManager{}
	if code := push1(t, f, m, win); code != exitWithheld {
		t.Fatalf("exit %d, want exitWithheld", code)
	}
	held := sentRequest(t, m.bodies[0]).Held
	sort.Strings(held)
	want := []string{badID, predecessor}
	sort.Strings(want)
	if !reflect.DeepEqual(held, want) {
		t.Fatalf("held %v, want both of the identity's records %v", held, want)
	}
}

// A live video rollup whose id has a tombstone was restored outside the rater
// without deleting its tombstone (the rater clears a live id's tombstone in
// the transaction that makes it live). The record is sent, and the run exits
// 1 so a person deletes the tombstone before a later loss would release it.
func TestPushWindows_ALiveVideoRecordWithATombstoneIsSentAndAlarms(t *testing.T) {
	f := videoStore()
	live := stampedVideoID(win, f.videoRollups[win][0])
	f.tombstones = map[time.Time][]string{win: {live}}
	m := &recordingManager{}
	if code := push1(t, f, m, win); code != exitFatal {
		t.Fatalf("exit %d, want %d", code, exitFatal)
	}
	if req := sentRequest(t, m.bodies[0]); len(req.VideoRecords) != 2 {
		t.Fatalf("%d video records, want both sent", len(req.VideoRecords))
	}
}

// A live rollup with a tombstone anywhere fails every run (exit 1), not only
// runs that rebuild its hour, so the alarm does not stop once the hour leaves
// the routine window.
func TestPushWindows_AStaleTombstoneAnywhereFailsTheRun(t *testing.T) {
	f := fixtureStore()
	f.staleTombstone = true
	m := &recordingManager{}
	if code := push1(t, f, m, win); code != exitFatal {
		t.Fatalf("exit %d, want %d", code, exitFatal)
	}
}
