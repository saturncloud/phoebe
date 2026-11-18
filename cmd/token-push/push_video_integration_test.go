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
	var sentAt time.Time
	if err := db.QueryRow(`SELECT pushed_at FROM pushed_usage WHERE usage_id = $1`, videoID).Scan(&sentAt); err != nil {
		t.Fatal(err)
	}

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
		// A held id keeps the pushed_at of the push that sent the record:
		// pushed_at says when the manager last received it (0017 down).
		var heldAt time.Time
		if err := db.QueryRow(`SELECT pushed_at FROM pushed_usage WHERE usage_id = $1`, videoID).Scan(&heldAt); err != nil || !heldAt.Equal(sentAt) {
			t.Fatalf("push %d: held id pushed_at %s (err %v), want %s kept", i, heldAt, err, sentAt)
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

// BOTH DIRECTIONS of a vanished video record, end to end through the real
// rater and token-push:
//   - the rater removed it (its job vanished, or its org changed): the rater
//     wrote a tombstone in the same transaction, so token-push lets the
//     manager delete the old record by absence, and the run exits 0;
//   - it was lost (the rollup row deleted outside the rater): no tombstone,
//     so token-push holds its id, the manager keeps the record, and the run
//     exits 1.
func TestIntegration_AVanishedVideoRecordIsDeletedOnlyWhenTheRaterRemovedIt(t *testing.T) {
	setup := func(t *testing.T, name string) (*sql.DB, time.Time, *recordingManager, string) {
		db := itDB(t, name)
		h := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
		event(t, db, h.Add(time.Minute), itRes1, itOrg1)
		videoJob(t, db, h.Add(2*time.Minute), itOrg1, "completed")
		rate(t, db, h, h.Add(time.Hour))
		m := &recordingManager{}
		if code := push(t, db, m, h); code != exitOK {
			t.Fatalf("first push: exit %d", code)
		}
		return db, h, m, lastRequest(t, m).VideoRecords[0].UsageID
	}
	t.Run("the rater removed it: its job vanished", func(t *testing.T) {
		db, h, m, id := setup(t, "phoebe_push_video_removed_it")
		if _, err := db.Exec(`DELETE FROM video_billing_event`); err != nil {
			t.Fatal(err)
		}
		rate(t, db, h, h.Add(time.Hour))
		if code := push(t, db, m, h); code != exitOK {
			t.Fatalf("exit %d, want 0", code)
		}
		req := lastRequest(t, m)
		if !reflect.DeepEqual(req.Kinds, []string{"token", "video"}) || len(req.VideoRecords) != 0 || len(req.Held) != 0 {
			t.Fatalf("kinds %v, %d video records, held %v; want video listed empty so the manager deletes %s",
				req.Kinds, len(req.VideoRecords), req.Held, id)
		}
	})
	t.Run("the rater removed it: its org changed", func(t *testing.T) {
		db, h, m, id := setup(t, "phoebe_push_video_regrained_it")
		if _, err := db.Exec(`UPDATE video_billing_event SET org_id = $1`, itOrg2); err != nil {
			t.Fatal(err)
		}
		rate(t, db, h, h.Add(time.Hour))
		if code := push(t, db, m, h); code != exitOK {
			t.Fatalf("exit %d, want 0", code)
		}
		req := lastRequest(t, m)
		if len(req.VideoRecords) != 1 || req.VideoRecords[0].UsageID == id || req.VideoRecords[0].OrgID != itOrg2 || len(req.Held) != 0 {
			t.Fatalf("%d video records, held %v; want the new org's record sent and the old id %s let go", len(req.VideoRecords), req.Held, id)
		}
	})
	t.Run("an explicit re-rate of an older hour is re-sent automatically", func(t *testing.T) {
		db, h, m, id := setup(t, "phoebe_push_video_rerate_resent_it")
		if _, err := db.Exec(`DELETE FROM video_billing_event`); err != nil {
			t.Fatal(err)
		}
		rate(t, db, h, h.Add(time.Hour))
		hours, err := (pgStore{db: db}).ResolvedHours(context.Background(), time.Now().UTC().Truncate(time.Hour))
		if err != nil || len(hours) != 1 || !hours[0].Equal(h) {
			t.Fatalf("hours to re-send %v (err %v), want [%s]: its record %s was tombstoned after its last push", hours, err, h, id)
		}
		_ = m
	})
	t.Run("restored outside the rater, sent again, then lost again: held", func(t *testing.T) {
		db, h, m, id := setup(t, "phoebe_push_video_restored_it")
		// The rater removes it (tombstone), the manager deletes it.
		if _, err := db.Exec(`CREATE TABLE saved AS SELECT * FROM rated_video_usage`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`DELETE FROM video_billing_event`); err != nil {
			t.Fatal(err)
		}
		rate(t, db, h, h.Add(time.Hour))
		if code := push(t, db, m, h); code != exitOK {
			t.Fatalf("release push: exit %d", code)
		}
		// Restored by hand (the tombstone stays), and sent again.
		if _, err := db.Exec(`INSERT INTO rated_video_usage SELECT * FROM saved`); err != nil {
			t.Fatal(err)
		}
		if code := push(t, db, m, h); code != exitOK || len(lastRequest(t, m).VideoRecords) != 1 {
			t.Fatalf("re-send after restore: exit %d", code)
		}
		// Lost again: the old tombstone predates the re-send, so it is held.
		if _, err := db.Exec(`DELETE FROM rated_video_usage`); err != nil {
			t.Fatal(err)
		}
		if code := push(t, db, m, h); code != exitFatal {
			t.Fatalf("exit %d, want %d", code, exitFatal)
		}
		if req := lastRequest(t, m); !reflect.DeepEqual(req.Held, []string{id}) {
			t.Fatalf("held %v, want [%s]: a stale tombstone must not release it", req.Held, id)
		}
	})
	t.Run("lost: the rollup row deleted outside the rater", func(t *testing.T) {
		db, h, m, id := setup(t, "phoebe_push_video_lost_it")
		if _, err := db.Exec(`DELETE FROM rated_video_usage`); err != nil {
			t.Fatal(err)
		}
		if code := push(t, db, m, h); code != exitFatal {
			t.Fatalf("exit %d, want %d", code, exitFatal)
		}
		if req := lastRequest(t, m); !reflect.DeepEqual(req.Held, []string{id}) {
			t.Fatalf("held %v, want [%s] so the manager keeps the record", req.Held, id)
		}
	})
}

// The empty-database guards: nothing at all is a fresh install (exit 0,
// nothing sent); with the video tables emptied after video was sent, the
// tokens are sent, every sent video id is held (no tombstone), and the run
// exits 1.
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
	if !reflect.DeepEqual(req.Kinds, []string{"token", "video"}) || len(req.Records) != 1 || len(req.Held) != 1 {
		t.Fatalf("kinds %v with %d records, held %v; want the token record sent and the video id held",
			req.Kinds, len(req.Records), req.Held)
	}
	var videoRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pushed_usage WHERE kind = 'video'`).Scan(&videoRows); err != nil || videoRows != 1 {
		t.Fatalf("video pushed_usage rows %d (err %v), want the one kept", videoRows, err)
	}
}
