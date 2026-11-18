//go:build integration

package main

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/meteredusage"
)

const itVideoRes = "8293a4b5c6d7e8f90a1b2c3d4e5f6071"

// videoJob inserts one finished job into video_billing_event: a completed
// 81-frame 720p t2v clip (5.0625 s), or a failed one with zero output.
func videoJob(t *testing.T, db *sql.DB, at time.Time, org, status string) {
	t.Helper()
	itSeq++
	secs, frames, reqs := "5.0625", 81, 1
	if status != "completed" {
		secs, frames, reqs = "0", 0, 0
	}
	if _, err := db.Exec(`INSERT INTO video_billing_event (job_id, created_at, org_id, auth_id, user_id, resource_id,
		model_id, base_model, serving_mode, task, resolution, region, status, output_seconds, output_frames,
		request_count, gpu_seconds)
		VALUES ($1,$2,$3,$4,$5,$6,'acme-wan','Wan-AI/Wan2.2-T2V-A14B-Diffusers','dedicated','t2v','720p',NULL,$7,
		        $8::numeric,$9,$10,60.5)`,
		fmt.Sprintf("video_%026d", itSeq), at, org, itAuth, itUser, itVideoRes, status, secs, frames, reqs); err != nil {
		t.Fatal(err)
	}
}

// The rater rolls the jobs up and token-push sends them with the hour's token
// records: kinds ["token", "video"], output_seconds summed exactly, the failed
// job's GPU time included, request_count the completed jobs, region "".
// pushed_usage records each id's kind.
func TestIntegration_VideoJobsAreRatedAndPushedWithTheHoursTokens(t *testing.T) {
	db := itDB(t, "phoebe_push_video_it")
	h := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	event(t, db, h.Add(time.Minute), itRes1, itOrg1)
	videoJob(t, db, h.Add(2*time.Minute), itOrg1, "completed")
	videoJob(t, db, h.Add(3*time.Minute), itOrg1, "completed")
	videoJob(t, db, h.Add(4*time.Minute), itOrg1, "failed")
	if res := rate(t, db, h, h.Add(time.Hour)); res.Video.RollupsWritten != 1 || res.Video.EventsAggregated != 3 {
		t.Fatalf("%+v, want one video rollup of three jobs", res.Video)
	}
	m := &recordingManager{}
	if code := push(t, db, m, h); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	req := lastRequest(t, m)
	if !reflect.DeepEqual(req.Kinds, []string{"token", "video"}) || len(req.Records) != 1 || len(req.VideoRecords) != 1 {
		t.Fatalf("kinds %v, %d token and %d video records", req.Kinds, len(req.Records), len(req.VideoRecords))
	}
	v := req.VideoRecords[0]
	if v.Quantities != (meteredusage.VideoQuantities{OutputSeconds: "10.125", OutputFrames: 162, RequestCount: 2, GPUSeconds: "181.5"}) {
		t.Fatalf("quantities %+v", v.Quantities)
	}
	if v.OrgID != itOrg1 || v.OwnerType != "user" || v.OwnerID != itUser || v.ResourceID != itVideoRes || v.Attributes.Region != "" {
		t.Fatalf("record %+v", v)
	}
	var kinds string
	if err := db.QueryRow(`SELECT string_agg(kind, ',' ORDER BY kind) FROM pushed_usage WHERE window_start = $1`, h).Scan(&kinds); err != nil || kinds != "token,video" {
		t.Fatalf("pushed_usage kinds %q (err %v), want token,video", kinds, err)
	}
}

// A sent video identity that becomes ambiguous is parked by the rater and its
// usage_id is held on every push while parked; the token record of the hour
// is sent and not held. Resolving it re-sends the hour.
func TestIntegration_AnAlreadySentVideoRecordIsHeldWhileParkedThenResent(t *testing.T) {
	db := itDB(t, "phoebe_push_video_held_it")
	h := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	event(t, db, h.Add(time.Minute), itRes1, itOrg1)
	videoJob(t, db, h.Add(2*time.Minute), itOrg1, "completed")
	rate(t, db, h, h.Add(time.Hour))
	m := &recordingManager{}
	if code := push(t, db, m, h); code != exitOK {
		t.Fatalf("first push: exit %d", code)
	}
	videoID := lastRequest(t, m).VideoRecords[0].UsageID

	videoJob(t, db, h.Add(30*time.Minute), itOrg2, "completed") // late, another org
	if res := rate(t, db, h, h.Add(time.Hour)); res.Video.ParkedDeletions != 1 || res.Video.AmbiguousOrgEvents != 2 {
		t.Fatalf("%+v, want the video rollup moved to parked", res.Video)
	}
	for i := 0; i < 2; i++ {
		if code := push(t, db, m, h); code != exitOK {
			t.Fatalf("push %d: exit %d", i, code)
		}
		req := lastRequest(t, m)
		if len(req.Records) != 1 || len(req.VideoRecords) != 0 || !reflect.DeepEqual(req.Held, []string{videoID}) {
			t.Fatalf("push %d: %d token, %d video, held %v; want the token record sent and [%s] held", i,
				len(req.Records), len(req.VideoRecords), req.Held, videoID)
		}
	}

	if _, err := db.Exec(`UPDATE video_billing_event SET org_id = $1`, itOrg1); err != nil {
		t.Fatal(err)
	}
	if res := rate(t, db, h, h.Add(time.Hour)); res.Video.ParkedResolved != 1 {
		t.Fatalf("%+v, want the parked video row resolved", res.Video)
	}
	if hours, err := (pgStore{db: db}).ResolvedHours(context.Background(), time.Now().UTC().Truncate(time.Hour)); err != nil || len(hours) != 1 || !hours[0].Equal(h) {
		t.Fatalf("resolved hours %v (err %v), want [%s]", hours, err, h)
	}
	if code := push(t, db, m, h); code != exitOK {
		t.Fatal("push after resolve failed")
	}
	if req := lastRequest(t, m); len(req.VideoRecords) != 1 || req.VideoRecords[0].Quantities.RequestCount != 2 || len(req.Held) != 0 {
		t.Fatalf("%+v, want the resolved video identity sent with both jobs and nothing held", req)
	}
}

// A video-only hour the manager never accepted is re-sent like a token hour,
// and an install whose only usage is video passes the liveness guard.
func TestIntegration_AVideoOnlyHourNeverAcceptedIsReSent(t *testing.T) {
	db := itDB(t, "phoebe_push_video_unsent_it")
	now := time.Now().UTC().Truncate(time.Hour)
	h1, h2, h3 := now.Add(-80*time.Hour), now.Add(-60*time.Hour), now.Add(-time.Hour)
	for _, h := range []time.Time{h1, h2} {
		videoJob(t, db, h.Add(time.Minute), itOrg1, "completed")
		rate(t, db, h, h.Add(time.Hour))
	}
	m := &recordingManager{}
	if code := push(t, db, m, h1); code != exitOK || len(lastRequest(t, m).VideoRecords) != 1 {
		t.Fatalf("exit %d", code)
	}
	if code := push(t, db, &recordingManager{statuses: []int{503}}, h2); code != exitFatal {
		t.Fatalf("exit %d, want the 503 to fail the hour", code)
	}
	m2 := &recordingManager{}
	if code := push(t, db, m2, h3); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	// h3 has no usage and was never sent, so it is skipped (an empty snapshot
	// goes only to an hour this install sent before).
	if len(m2.bodies) != 1 || sentRequest(t, m2.bodies[0]).WindowStart != meteredusage.FormatWindowStart(h2) ||
		len(sentRequest(t, m2.bodies[0]).VideoRecords) != 1 {
		t.Fatalf("sent %d hours, want only h2 re-sent with its video record", len(m2.bodies))
	}
}

// A wiped rated_video_usage is caught even though parked_video_usage still
// holds a RESOLVED row (parked rows are never pruned): video is left out of
// the request and the run exits 1, instead of every re-sent hour listing
// video with no records and deleting the manager's video records.
func TestIntegration_AWipedVideoRollupIsCaughtDespiteAResolvedParkedRow(t *testing.T) {
	db := itDB(t, "phoebe_push_video_wipe_parked_it")
	h := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	event(t, db, h.Add(time.Minute), itRes1, itOrg1)
	videoJob(t, db, h.Add(2*time.Minute), itOrg1, "completed")
	rate(t, db, h, h.Add(time.Hour))
	m := &recordingManager{}
	if code := push(t, db, m, h); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	if _, err := db.Exec(`INSERT INTO parked_video_usage (window_start, auth_id, owner_type, owner_id, resource_id,
		model_id, serving_mode, reason, event_count, output_seconds, output_frames, request_count, gpu_seconds, resolved_at)
		VALUES ($1, 'old-auth', '', '', 'old-res', 'old-model', 'dedicated', 'ambiguous_org', 1, 0, 0, 0, 0, now())`,
		h.Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM rated_video_usage`); err != nil {
		t.Fatal(err)
	}
	if code := push(t, db, m, h); code != exitFatal {
		t.Fatalf("exit %d, want %d for a wiped rated_video_usage", code, exitFatal)
	}
	if req := lastRequest(t, m); !reflect.DeepEqual(req.Kinds, []string{"token"}) {
		t.Fatalf("kinds %v, want token only so the manager's video records are kept", req.Kinds)
	}
}

// The empty-database guards behave as before: nothing at all is a fresh
// install (exit 0, nothing sent); video bookkeeping with the video tables
// emptied sends tokens only and exits 1.
func TestIntegration_TheEmptyDatabaseGuardsCoverVideo(t *testing.T) {
	db := itDB(t, "phoebe_push_video_guard_it")
	h := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	m := &recordingManager{}
	if code := push(t, db, m, h); code != exitOK || len(m.bodies) != 0 {
		t.Fatalf("fresh install: exit %d with %d requests, want 0 and none", code, len(m.bodies))
	}
	event(t, db, h.Add(time.Minute), itRes1, itOrg1)
	videoJob(t, db, h.Add(2*time.Minute), itOrg1, "completed")
	rate(t, db, h, h.Add(time.Hour))
	if code := push(t, db, m, h); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	if _, err := db.Exec(`DELETE FROM rated_video_usage`); err != nil {
		t.Fatal(err)
	}
	if code := push(t, db, m, h); code != exitFatal {
		t.Fatalf("exit %d, want %d for wiped video tables", code, exitFatal)
	}
	req := lastRequest(t, m)
	if !reflect.DeepEqual(req.Kinds, []string{"token"}) || len(req.Records) != 1 {
		t.Fatalf("kinds %v with %d records, want the token hour only", req.Kinds, len(req.Records))
	}
	var videoRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pushed_usage WHERE kind = 'video'`).Scan(&videoRows); err != nil || videoRows != 1 {
		t.Fatalf("video pushed_usage rows %d (err %v), want the one kept", videoRows, err)
	}
}
