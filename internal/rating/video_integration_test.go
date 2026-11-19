//go:build integration

package rating

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/meteredusage"
)

// vbRow is one video_billing_event row. The defaults (see vrow) describe one
// completed 81-frame Wan A14B clip at 16 fps (5.0625 s) on a dedicated 720p
// t2v endpoint.
type vbRow struct {
	id                                     string
	auth, resource, org, model, base, user string
	group, mode, task, resolution, region  string
	status                                 string
	outputSeconds, gpuSeconds              string
	outputFrames, requestCount             int
	at                                     time.Time
}

var vrowSeq int

func vrow(mod func(*vbRow)) vbRow {
	vrowSeq++
	r := vbRow{
		id: fmt.Sprintf("video_%026d", vrowSeq), auth: "auth-a", resource: "res-v", org: "org-1",
		model: "wan-ep", base: "Wan-AI/Wan2.2-T2V-A14B-Diffusers", user: "user-1", mode: "dedicated",
		task: "t2v", resolution: "720p", status: "completed",
		outputSeconds: "5.0625", gpuSeconds: "120.500", outputFrames: 81, requestCount: 1,
		at: itHour.Add(10 * time.Minute),
	}
	if mod != nil {
		mod(&r)
	}
	return r
}

// failed turns a row into a failed job: zero output, request_count 0, GPU time kept.
func failed(r *vbRow) {
	r.status, r.outputSeconds, r.outputFrames, r.requestCount = "failed", "0", 0, 0
}

func vseed(t *testing.T, db *sql.DB, rows ...vbRow) {
	t.Helper()
	for _, r := range rows {
		_, err := db.Exec(`INSERT INTO video_billing_event (job_id, created_at, org_id, auth_id, user_id, group_id,
			resource_id, model_id, base_model, serving_mode, task, resolution, region, status,
			output_seconds, output_frames, request_count, gpu_seconds)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::numeric,$16,$17,$18::numeric)`,
			r.id, r.at, r.org, r.auth, nullableStr(r.user), nullableStr(r.group), r.resource, r.model, r.base,
			r.mode, r.task, r.resolution, nullableStr(r.region), r.status,
			r.outputSeconds, r.outputFrames, r.requestCount, r.gpuSeconds)
		if err != nil {
			t.Fatalf("seed %s: %v", r.id, err)
		}
	}
}

func quiet() *logging.Logger { l, _, _ := captureLogger(); return l }

func vaggregate(t *testing.T, db *sql.DB, start, end time.Time) VideoAggregateResult {
	t.Helper()
	return aggregate(t, db, start, end).Video
}

func vwithheld(r VideoAggregateResult) int64 {
	return r.InvalidUsageEvents + r.UnattributableEvents + r.OwnerConflictEvents + r.AmbiguousOrgEvents + r.MissingOrgEvents
}

func assertVideoPartition(t *testing.T, r VideoAggregateResult, total int) {
	t.Helper()
	if got := r.EventsAggregated + vwithheld(r); got != int64(total) {
		t.Fatalf("aggregated %d + withheld %d = %d, want every one of the %d jobs exactly once (%+v)",
			r.EventsAggregated, vwithheld(r), got, total, r)
	}
}

type videoRollup struct {
	auth, ownerType, ownerID, resource, org, model, mode, base, task, resolution, region string
	outputSeconds, gpuSeconds                                                            string
	frames, requests, events                                                             int64
}

func readVideoRollups(t *testing.T, db *sql.DB) []videoRollup {
	t.Helper()
	rows, err := db.Query(`SELECT auth_id, owner_type, owner_id, resource_id, org_id, model_id, serving_mode,
		base_model, task, resolution, region, output_seconds::text, gpu_seconds::text,
		output_frames, request_count, event_count
		FROM rated_video_usage
		ORDER BY auth_id, owner_id, resource_id, model_id, serving_mode, base_model, task, resolution, region`)
	if err != nil {
		t.Fatalf("read video rollups: %v", err)
	}
	defer rows.Close()
	var out []videoRollup
	for rows.Next() {
		var r videoRollup
		if err := rows.Scan(&r.auth, &r.ownerType, &r.ownerID, &r.resource, &r.org, &r.model, &r.mode,
			&r.base, &r.task, &r.resolution, &r.region, &r.outputSeconds, &r.gpuSeconds,
			&r.frames, &r.requests, &r.events); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// Two completed 81-frame clips and one failed job in one grain: output seconds
// sum exactly (5.0625 + 5.0625 = 10.125000000, never a float), request_count counts
// the two completed jobs only, and the failed job adds its GPU time and its
// event. The rollup carries the org.
func TestIntegration_VideoJobsRollUpExactlyAndCountCompletedRequestsOnly(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_sums_it")
	vseed(t, db, vrow(nil), vrow(nil), vrow(failed))
	res := vaggregate(t, db, itHour, itHour.Add(time.Hour))
	assertVideoPartition(t, res, 3)
	if res.RollupsWritten != 1 || res.EventsAggregated != 3 || vwithheld(res) != 0 {
		t.Fatalf("%+v, want one rollup of three jobs, nothing withheld", res)
	}
	got := readVideoRollups(t, db)
	if len(got) != 1 {
		t.Fatalf("%d rollups, want 1", len(got))
	}
	r := got[0]
	if r.outputSeconds != "10.125000000" || r.frames != 162 || r.requests != 2 || r.gpuSeconds != "361.500" || r.events != 3 {
		t.Fatalf("rollup %+v, want 10.125000000 s, 162 frames, 2 requests, 361.500 GPU s over 3 jobs", r)
	}
	if r.org != "org-1" || r.ownerType != "user" || r.ownerID != "user-1" || r.region != "" || r.task != "t2v" || r.resolution != "720p" {
		t.Fatalf("rollup identity %+v", r)
	}
}

// A frame count the frame rate does not divide (121 frames at 24 fps,
// 5.041666667 s at nine decimals) sums with all nine digits kept: the rollup
// column has the same scale as video_billing_event, so nothing is rounded.
func TestIntegration_VideoSecondsKeepNineDecimalsThroughTheRollup(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_nine_it")
	at24 := func(r *vbRow) { r.outputSeconds, r.outputFrames = "5.041666667", 121 }
	vseed(t, db, vrow(at24), vrow(at24), vrow(at24))
	vaggregate(t, db, itHour, itHour.Add(time.Hour))
	got := readVideoRollups(t, db)
	if len(got) != 1 || got[0].outputSeconds != "15.125000001" || got[0].frames != 363 {
		t.Fatalf("rollups %+v, want 15.125000001 s over 363 frames", got)
	}
}

// Every grain key splits the rollup: base model, task, resolution, region and
// serving mode are all record attributes, so one hour of one endpoint in two
// values of any of them is two records, never a conflict.
func TestIntegration_VideoEachGrainKeySplitsTheRollup(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_grain_it")
	vseed(t, db,
		vrow(nil),
		vrow(func(r *vbRow) { r.base = "Wan-AI/Wan2.2-I2V-A14B-Diffusers" }),
		vrow(func(r *vbRow) { r.task = "i2v" }),
		vrow(func(r *vbRow) { r.resolution = "480p" }),
		vrow(func(r *vbRow) { r.region = "us-central1" }),
		vrow(func(r *vbRow) { r.mode = "shared" }),
	)
	res := vaggregate(t, db, itHour, itHour.Add(time.Hour))
	assertVideoPartition(t, res, 6)
	if res.RollupsWritten != 6 || res.ParkedWritten != 0 {
		t.Fatalf("%+v, want six rollups and nothing parked", res)
	}
}

// Re-running an hour converges: the same rows and ids, no deletions; a late
// job is folded in; a job that vanishes from the evidence reconciles its
// rollup away and is counted as an unexplained deletion.
func TestIntegration_VideoReRunIsIdempotentAndReconciles(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_idem_it")
	gone := vrow(func(r *vbRow) { r.resolution = "1080p" })
	vseed(t, db, vrow(nil), gone)
	first := vaggregate(t, db, itHour, itHour.Add(time.Hour))
	ids := func() string {
		var s string
		if err := db.QueryRow(`SELECT string_agg(id, ',' ORDER BY id) FROM rated_video_usage`).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	before, beforeRows := ids(), fmt.Sprint(readVideoRollups(t, db))

	second := vaggregate(t, db, itHour, itHour.Add(time.Hour))
	if second.RollupsWritten != first.RollupsWritten || second.ReconciledDeletions != 0 {
		t.Fatalf("re-run %+v after %+v, want the same rollups and no deletions", second, first)
	}
	if ids() != before || fmt.Sprint(readVideoRollups(t, db)) != beforeRows {
		t.Fatal("re-run changed the rollups or their ids")
	}

	vseed(t, db, vrow(func(r *vbRow) { r.at = itHour.Add(50 * time.Minute) }))
	exec(t, db, `DELETE FROM video_billing_event WHERE job_id = '`+gone.id+`'`)
	third := vaggregate(t, db, itHour, itHour.Add(time.Hour))
	if third.RollupsWritten != 1 || third.ReconciledDeletions != 1 || third.ParkedDeletions != 0 {
		t.Fatalf("%+v, want the 720p rollup rewritten and the vanished 1080p rollup reconciled away", third)
	}
	got := readVideoRollups(t, db)
	if len(got) != 1 || got[0].requests != 2 || got[0].outputSeconds != "10.125000000" {
		t.Fatalf("rollups %+v, want one 720p rollup with the late job folded in", got)
	}
}

// Hours outside the window are never touched by a re-run.
func TestIntegration_VideoReRunLeavesOtherHoursAlone(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_hours_it")
	vseed(t, db, vrow(nil), vrow(func(r *vbRow) { r.at = itHour.Add(-30 * time.Minute) }))
	vaggregate(t, db, itHour.Add(-time.Hour), itHour.Add(time.Hour))
	exec(t, db, `DELETE FROM video_billing_event`)
	res := vaggregate(t, db, itHour, itHour.Add(time.Hour))
	if res.ReconciledDeletions != 1 || len(readVideoRollups(t, db)) != 1 {
		t.Fatalf("%+v, want only the window's rollup deleted", res)
	}
}

// Two orgs for one endpoint identity in an hour, across two resolutions: the
// payer is ambiguous, so the whole identity is PARKED (both resolutions), the
// rollup it already had is deleted as parked (not unexplained), and an
// unrelated endpoint in the same hour is written. Fixing the evidence resolves
// the parked row and writes the identity again.
func TestIntegration_VideoAmbiguousOrgParksTheIdentityThenResolves(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_amborg_it")
	vseed(t, db, vrow(nil), vrow(func(r *vbRow) { r.resource = "res-other" }))
	if res := vaggregate(t, db, itHour, itHour.Add(time.Hour)); res.RollupsWritten != 2 {
		t.Fatalf("%+v, want two clean rollups first", res)
	}
	bad := vrow(func(r *vbRow) { r.org = "org-2"; r.resolution = "480p" })
	vseed(t, db, bad)
	res := vaggregate(t, db, itHour, itHour.Add(time.Hour))
	assertVideoPartition(t, res, 3)
	if res.AmbiguousOrgEvents != 2 || res.ParkedWritten != 1 || res.ParkedDeletions != 1 || res.ReconciledDeletions != 0 || res.RollupsWritten != 1 {
		t.Fatalf("%+v, want res-v parked as ambiguous_org (2 jobs), its rollup deleted as parked, res-other written", res)
	}
	var reason, secs string
	var events, requests int64
	if err := db.QueryRow(`SELECT reason, event_count, request_count, output_seconds::text FROM parked_video_usage
		WHERE resource_id = 'res-v' AND resolved_at IS NULL`).Scan(&reason, &events, &requests, &secs); err != nil {
		t.Fatal(err)
	}
	if reason != "ambiguous_org" || events != 2 || requests != 2 || secs != "10.125000000" {
		t.Fatalf("parked %s events=%d requests=%d seconds=%s, want ambiguous_org with both jobs' usage", reason, events, requests, secs)
	}

	exec(t, db, `UPDATE video_billing_event SET org_id = 'org-1' WHERE job_id = '`+bad.id+`'`)
	res = vaggregate(t, db, itHour, itHour.Add(time.Hour))
	if res.ParkedResolved != 1 || res.RollupsWritten != 3 {
		t.Fatalf("%+v, want the parked row resolved and three rollups written", res)
	}
}

// org_id ” on every job of an identity: the payer is unknown, so it is parked
// as missing_org and never written with a blank payer.
func TestIntegration_VideoMissingOrgIsParkedNotSent(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_noorg_it")
	vseed(t, db, vrow(func(r *vbRow) { r.org = "" }), vrow(func(r *vbRow) { r.resource = "res-other" }))
	res := vaggregate(t, db, itHour, itHour.Add(time.Hour))
	assertVideoPartition(t, res, 2)
	if res.MissingOrgEvents != 1 || res.ParkedWritten != 1 || res.RollupsWritten != 1 {
		t.Fatalf("%+v, want the org-less identity parked and its neighbour written", res)
	}
	for _, r := range readVideoRollups(t, db) {
		if r.org == "" {
			t.Fatalf("rollup %+v written with no org", r)
		}
	}
}

// A job carrying both a user and a group is parked per job under owner
// (”, ”); the clean job of the same grain is still aggregated.
func TestIntegration_VideoOwnerConflictIsParkedWithoutPoisoningItsBucket(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_owner_it")
	vseed(t, db, vrow(nil), vrow(func(r *vbRow) { r.group = "group-1" }))
	res := vaggregate(t, db, itHour, itHour.Add(time.Hour))
	assertVideoPartition(t, res, 2)
	if res.OwnerConflictEvents != 1 || res.ParkedWritten != 1 || res.RollupsWritten != 1 || res.EventsAggregated != 1 {
		t.Fatalf("%+v, want the conflicted job parked and the clean one aggregated", res)
	}
	var ownerType, ownerID string
	if err := db.QueryRow(`SELECT owner_type, owner_id FROM parked_video_usage WHERE reason = 'owner_conflict'`).Scan(&ownerType, &ownerID); err != nil || ownerType != "" || ownerID != "" {
		t.Fatalf("owner_conflict parked under %q/%q (err %v), want ''/''", ownerType, ownerID, err)
	}
}

// Evidence that cannot become a record is withheld per job and counted (exit
// 2), never parked and never aggregated: an empty identifier, a negative
// quantity, and a completed job with request_count 0.
func TestIntegration_VideoInvalidAndUnattributableJobsAreWithheldAndCounted(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_invalid_it")
	vseed(t, db,
		vrow(nil),
		vrow(func(r *vbRow) { r.auth = "" }),
		vrow(func(r *vbRow) { r.model = "" }),
		vrow(func(r *vbRow) { r.gpuSeconds = "-1" }),
		vrow(func(r *vbRow) { r.requestCount = 0 }),
	)
	res := vaggregate(t, db, itHour, itHour.Add(time.Hour))
	assertVideoPartition(t, res, 5)
	if res.UnattributableEvents != 2 || res.InvalidUsageEvents != 2 || res.EventsAggregated != 1 || res.ParkedWritten != 0 {
		t.Fatalf("%+v, want 2 unattributable, 2 invalid, 1 aggregated, nothing parked", res)
	}
	if !res.HasAnomaly() {
		t.Fatal("withheld video jobs must be an anomaly (cmd/rater exit 2)")
	}
}

// One run aggregates both kinds in one transaction, and neither kind's counts
// include the other's events.
func TestIntegration_TokenAndVideoAggregateInOneRunWithSeparateCounts(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_both_it")
	seed(t, db, row(nil))
	vseed(t, db, vrow(nil), vrow(nil))
	res := aggregate(t, db, itHour, itHour.Add(time.Hour))
	assertPartition(t, res, 1)
	assertVideoPartition(t, res.Video, 2)
	if res.RollupsWritten != 1 || res.Video.RollupsWritten != 1 || res.Video.EventsAggregated != 2 {
		t.Fatalf("%+v, want one token rollup and one video rollup of two jobs", res)
	}
}

// -list-parked shows video rows with kind "video" and their video quantities,
// and flags a parked video hour whose raw jobs were pruned. The aggregator
// then refuses to re-aggregate that hour.
func TestIntegration_VideoParkedRowsAreListedAndAPrunedHourIsNeverReAggregated(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_list_it")
	vseed(t, db, vrow(nil), vrow(func(r *vbRow) { r.org = "org-2" }))
	seed(t, db, row(nil), row(func(r *beRow) { r.org = "org-2" }))
	vaggregate(t, db, itHour, itHour.Add(time.Hour))
	store := itStore(db)
	parked, err := store.ListParked(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(parked) != 2 {
		t.Fatalf("parked %+v, want one token and one video row", parked)
	}
	var v Parked
	for _, p := range parked {
		if p.Kind == KindVideo {
			v = p
		}
	}
	if v.Reason != "ambiguous_org" || v.EventCount != 2 || v.RequestCount != 2 || v.OutputSeconds != "10.125000000" ||
		v.OutputFrames != 162 || v.GPUSeconds != "241.000" || v.RawEvents != 2 || v.RawEventsPruned {
		t.Fatalf("video parked row %+v", v)
	}
	if !strings.Contains(v.UsageLabel(), "output_seconds=10.125000000") {
		t.Fatalf("usage label %q", v.UsageLabel())
	}
	hours, err := store.ParkedHours(context.Background(), itHour.Add(-time.Hour))
	if err != nil || len(hours) != 1 || !hours[0].Equal(itHour) {
		t.Fatalf("parked hours %v (err %v), want the one hour once across both kinds", hours, err)
	}

	exec(t, db, `DELETE FROM video_billing_event WHERE org_id = 'org-2'`)
	parked, _ = store.ListParked(context.Background())
	for _, p := range parked {
		if p.Kind == KindVideo && !p.RawEventsPruned {
			t.Fatalf("%+v, want the video row marked pruned", p)
		}
	}
	agg := New(store, quiet())
	agg.now = itNow
	if _, err := agg.Run(context.Background(), itHour, itHour.Add(time.Hour), true); err == nil ||
		!strings.Contains(err.Error(), "pruned") {
		t.Fatalf("Run over a pruned parked video hour: err = %v, want the refusal", err)
	}
}

// 0017 down refuses while video usage is parked or not yet accepted by the
// manager, and otherwise round-trips (down, then up again). A token-only push
// of the hour (pushed_hour stamped, no video pushed_usage row) does not count
// as the video being accepted.
func TestIntegration_Migration0017DownRefusesWhileVideoUsageIsParkedOrUnsent(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_m0017_it")
	vseed(t, db, vrow(nil), vrow(func(r *vbRow) { r.resolution = "480p" }),
		vrow(func(r *vbRow) { r.resource = "res-2"; r.org = "org-2" }),
		vrow(func(r *vbRow) { r.resource = "res-2" }))
	vaggregate(t, db, itHour, itHour.Add(time.Hour))
	down := readMigration(t, "0017_rated_video_usage.down.sql")
	if _, err := db.Exec(down); err == nil || !strings.Contains(err.Error(), "unresolved") {
		t.Fatalf("down with parked video usage: err = %v, want the refusal", err)
	}
	exec(t, db, `UPDATE parked_video_usage SET resolved_at = now()`)
	if _, err := db.Exec(down); err == nil || !strings.Contains(err.Error(), "never accepted") {
		t.Fatalf("down with unsent video usage: err = %v, want the refusal", err)
	}
	// A token-only accepted push: the previous release's token-push stamps
	// pushed_hour and records only token ids.
	exec(t, db, `INSERT INTO pushed_hour (window_start, pushed_at) VALUES ('`+itHour.Format(time.RFC3339)+`', now() + interval '1 minute')`)
	exec(t, db, `INSERT INTO pushed_usage (window_start, usage_id, kind, auth_id, owner_type, owner_id, resource_id, model_id, serving_mode)
		VALUES ('`+itHour.Format(time.RFC3339)+`', repeat('b', 64), 'token', 'a', '', '', 'r', 'm', 'dedicated')`)
	if _, err := db.Exec(down); err == nil || !strings.Contains(err.Error(), "never accepted") {
		t.Fatalf("down after a token-only push: err = %v, want the refusal", err)
	}
	// Ids recorded only as intent (RecordIntent before a send that failed):
	// pushed_at after the hour's last acceptance. Not accepted.
	insertVideoIDs := func(at, limit string) {
		exec(t, db, `INSERT INTO pushed_usage (window_start, usage_id, kind, auth_id, owner_type, owner_id, resource_id,
			model_id, serving_mode, pushed_at)
			SELECT window_start, usage_id, 'video', auth_id, owner_type, owner_id, resource_id, model_id,
			       serving_mode, `+at+`
			  FROM rated_video_usage ORDER BY id LIMIT `+limit+`
			ON CONFLICT ON CONSTRAINT pushed_usage_pk DO UPDATE SET pushed_at = EXCLUDED.pushed_at`)
	}
	insertVideoIDs("now() + interval '1 hour'", "ALL")
	if _, err := db.Exec(down); err == nil || !strings.Contains(err.Error(), "never accepted") {
		t.Fatalf("down with intent-only video ids: err = %v, want the refusal", err)
	}
	// Accepted with the hour's push, but only one of the identity's two
	// grains (720p and 480p): the other was never sent.
	exec(t, db, `DELETE FROM pushed_usage WHERE kind = 'video'`)
	insertVideoIDs("(SELECT pushed_at FROM pushed_hour)", "1")
	if _, err := db.Exec(down); err == nil || !strings.Contains(err.Error(), "never accepted") {
		t.Fatalf("down with one of two grains accepted: err = %v, want the refusal", err)
	}
	// Accepted with the hour's push: one grain's own id plus some OTHER id of
	// the same identity (a held older record). The other grain's id is
	// missing, and a per-identity count would have let it pass.
	exec(t, db, `INSERT INTO pushed_usage (window_start, usage_id, kind, auth_id, owner_type, owner_id, resource_id,
		model_id, serving_mode, pushed_at)
		SELECT window_start, repeat('f', 64), 'video', auth_id, owner_type, owner_id, resource_id, model_id,
		       serving_mode, (SELECT pushed_at FROM pushed_hour)
		  FROM rated_video_usage ORDER BY id LIMIT 1`)
	if _, err := db.Exec(down); err == nil || !strings.Contains(err.Error(), "never accepted") {
		t.Fatalf("down with a sibling id standing in for a grain: err = %v, want the refusal", err)
	}
	// Every grain accepted with the hour's push, but the stand-in id is a
	// record the rater removed (tombstone) that the manager still holds.
	insertVideoIDs("(SELECT pushed_at FROM pushed_hour)", "ALL")
	exec(t, db, `INSERT INTO video_reconcile_deletion (window_start, usage_id, auth_id, owner_type, owner_id,
		resource_id, model_id, serving_mode, reason)
		SELECT window_start, usage_id, auth_id, owner_type, owner_id, resource_id, model_id, serving_mode, 'reconcile'
		  FROM pushed_usage WHERE usage_id = repeat('f', 64)`)
	if _, err := db.Exec(down); err == nil || !strings.Contains(err.Error(), "still held by the manager") {
		t.Fatalf("down with a removed record still at the manager: err = %v, want the refusal", err)
	}
	exec(t, db, `DELETE FROM pushed_usage WHERE usage_id = repeat('f', 64)`)
	exec(t, db, down)
	var left int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pushed_usage`).Scan(&left); err != nil || left != 1 {
		t.Fatalf("pushed_usage rows after down = %d (err %v), want only the token row", left, err)
	}
	exec(t, db, readMigration(t, "0017_rated_video_usage.up.sql"))
	var kind string
	if err := db.QueryRow(`SELECT kind FROM pushed_usage`).Scan(&kind); err != nil || kind != "token" {
		t.Fatalf("kind after up = %q (err %v), want the 'token' default", kind, err)
	}
}

// An hour older than the video reach is never re-rated for video: its jobs
// may be pruned, so re-running it would reconcile-delete the rollup the
// manager bills from (and token-push would then delete the manager's record).
// An explicit re-rate of that hour, for instance to fix its tokens, keeps the
// video rollup as it is and reports the hour as out of reach.
func TestIntegration_VideoOlderThanTheReachIsNeverReRatedOrDeleted(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_reach_it")
	vseed(t, db, vrow(nil))
	seed(t, db, row(nil))
	if res := vaggregate(t, db, itHour, itHour.Add(time.Hour)); res.RollupsWritten != 1 {
		t.Fatalf("%+v, want the hour rated while its jobs are retained", res)
	}
	before := readVideoRollups(t, db)

	// Forty days on, phoebe-videos has pruned the hour's jobs.
	exec(t, db, `DELETE FROM video_billing_event`)
	store := itStore(db)
	store.now = func() time.Time { return itHour.Add(40 * 24 * time.Hour) }
	res, err := store.AggregateWindow(context.Background(), itHour, itHour.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if res.Video.OutOfReachHours != 1 || res.Video.ReconciledDeletions != 0 || res.RollupsWritten != 1 {
		t.Fatalf("%+v, want tokens re-rated, video left alone and the hour reported out of reach", res)
	}
	if after := readVideoRollups(t, db); len(after) != 1 || after[0] != before[0] {
		t.Fatalf("video rollups %+v, want %+v kept", after, before)
	}
	// Out-of-reach hours are reported by kind: itHour holds a rollup (kept);
	// itHour-2h a completed job never rated (unbilled); itHour-3h only a
	// failed job (nothing to bill, not reported); itHour-1h nothing.
	vseed(t, db, vrow(func(r *vbRow) { r.at = itHour.Add(-2*time.Hour + 5*time.Minute) }),
		vrow(func(r *vbRow) { r.at = itHour.Add(-3*time.Hour + 5*time.Minute); failed(r) }))
	res, err = store.AggregateWindow(context.Background(), itHour.Add(-3*time.Hour), itHour.Add(time.Hour))
	if err != nil || res.Video.OutOfReachHours != 1 || res.Video.UnratedOutOfReachHours != 1 || !res.Video.HasAnomaly() {
		t.Fatalf("out-of-reach hours %d with rollups and %d unrated (err %v), want 1 and 1 and an anomaly (exit 2)",
			res.Video.OutOfReachHours, res.Video.UnratedOutOfReachHours, err)
	}

	// The reach is one day inside the 30-day row retention.
	if got := itHour.Add(40 * 24 * time.Hour).Sub(VideoFloor(itHour.Add(40 * 24 * time.Hour))); got != 29*24*time.Hour {
		t.Fatalf("reach %s, want 29 days", got)
	}
}

// The one-time catch-up rates a video-only hour (video jobs, no token
// events) that the trailing window never reached, so its usage is pushed.
// An hour with video jobs older than the video reach is not listed.
func TestIntegration_CatchUpRatesVideoOnlyHours(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_catchup_it")
	exec(t, db, readMigration(t, "0015_rater_state.up.sql"))
	old := itHour.Add(-30 * 24 * time.Hour)
	// Video in three hours with a gap (itHour, +1h, +3h), token events
	// interleaved at +2h, and one video hour older than the reach.
	vseed(t, db, vrow(nil),
		vrow(func(r *vbRow) { r.at = itHour.Add(70 * time.Minute) }),
		vrow(func(r *vbRow) { r.at = itHour.Add(3*time.Hour + 5*time.Minute) }),
		vrow(func(r *vbRow) { r.at = old.Add(5 * time.Minute) }))
	seed(t, db, row(func(r *beRow) { r.at = itHour.Add(2*time.Hour + 5*time.Minute) }))
	agg := New(itStore(db), quiet())
	agg.now = itNow
	cu, err := agg.CatchUp(context.Background(), itHour.Add(5*time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Time{itHour, itHour.Add(time.Hour), itHour.Add(2 * time.Hour), itHour.Add(3 * time.Hour)}
	if fmt.Sprint(cu.Hours) != fmt.Sprint(want) || !cu.Completed {
		t.Fatalf("caught up %v (completed %v), want %v", cu.Hours, cu.Completed, want)
	}
	if cu.Video.RollupsWritten != 3 || cu.Video.EventsAggregated != 3 || cu.EventsAggregated != 1 {
		t.Fatalf("catch-up result %+v, want three video hours and one token hour rolled up", cu.AggregateResult)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM rated_video_usage`).Scan(&n); err != nil || n != 3 {
		t.Fatalf("rated_video_usage rows %d (err %v), want one per video hour", n, err)
	}
}

// A NaN quantity is invalid evidence (withheld, counted, exit 2), never summed:
// Postgres orders NaN above every number, so NaN >= 0 alone would pass it.
func TestIntegration_VideoNaNQuantitiesAreInvalidUsage(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_nan_it")
	vseed(t, db, vrow(nil),
		vrow(func(r *vbRow) { r.outputSeconds = "NaN" }),
		vrow(func(r *vbRow) { r.gpuSeconds = "NaN" }))
	res := vaggregate(t, db, itHour, itHour.Add(time.Hour))
	assertVideoPartition(t, res, 3)
	if res.InvalidUsageEvents != 2 || res.EventsAggregated != 1 || !res.HasAnomaly() {
		t.Fatalf("%+v, want both NaN jobs withheld as invalid", res)
	}
	if got := readVideoRollups(t, db); len(got) != 1 || got[0].outputSeconds != "5.062500000" {
		t.Fatalf("rollups %+v, want the clean job only", got)
	}
}

// A no-owner rollup whose jobs vanish is a reconcile deletion (it pages on a
// routine run) even when an owner-conflict job of the same endpoint is parked
// under the same (”, ”) owner in that hour: owner-conflict jobs never formed
// a record, so their park explains no deletion.
func TestIntegration_VideoOwnerConflictParkDoesNotExplainAVanishedNoOwnerRollup(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_ownerless_it")
	vseed(t, db, vrow(func(r *vbRow) { r.id = "video_noowner"; r.user = "" }))
	if res := vaggregate(t, db, itHour, itHour.Add(time.Hour)); res.RollupsWritten != 1 {
		t.Fatalf("%+v, want the no-owner rollup", res)
	}
	exec(t, db, `DELETE FROM video_billing_event WHERE job_id = 'video_noowner'`)
	vseed(t, db, vrow(func(r *vbRow) { r.group = "group-1" }))
	res := vaggregate(t, db, itHour, itHour.Add(time.Hour))
	if res.ReconciledDeletions != 1 || res.ParkedDeletions != 0 || res.OwnerConflictEvents != 1 {
		t.Fatalf("%+v, want one reconcile deletion and none explained by the owner-conflict park", res)
	}
}

// Never-rated video older than the reach is found per identity: a rated
// identity in the same hour does not hide another identity's unrated
// completed jobs, an unresolved park does cover its own identity, a resolved
// park does not, and jobs that were never rateable (unattributable) are not
// reported as unbilled usage.
func TestIntegration_UnratedOutOfReachVideoIsCountedPerIdentity(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_unrated_it")
	h := itHour.Add(-5 * time.Hour)
	at := func(r *vbRow) { r.at = h.Add(10 * time.Minute) }
	// Identity A (res-v) is rated while inside the reach.
	vseed(t, db, vrow(at))
	store := itStore(db)
	if _, err := store.AggregateWindow(context.Background(), h, h.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	old := func() time.Time { return itHour.Add(40 * 24 * time.Hour) }
	store.now = old
	count := func() int64 {
		t.Helper()
		res, err := store.AggregateWindow(context.Background(), h, h.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		return res.Video.UnratedOutOfReachHours
	}
	// An unattributable completed job (never rateable) is not unbilled usage.
	vseed(t, db, vrow(func(r *vbRow) { at(r); r.auth = "" }))
	if n := count(); n != 0 {
		t.Fatalf("unrated hours %d, want 0 with A rated and only an unattributable job", n)
	}
	// Identity B (res-b) has a completed job and no rollup: unbilled.
	vseed(t, db, vrow(func(r *vbRow) { at(r); r.resource = "res-b" }))
	if n := count(); n != 1 {
		t.Fatalf("unrated hours %d, want 1: A's rollup must not hide B", n)
	}
	// An unresolved park of B covers it; a resolved one does not.
	exec(t, db, `INSERT INTO parked_video_usage (window_start, auth_id, owner_type, owner_id, resource_id, model_id,
		serving_mode, reason, event_count, output_seconds, output_frames, request_count, gpu_seconds)
		VALUES ('`+h.Format(time.RFC3339)+`', 'auth-a', 'user', 'user-1', 'res-b', 'wan-ep', 'dedicated',
		        'ambiguous_org', 1, 0, 0, 0, 0)`)
	if n := count(); n != 0 {
		t.Fatalf("unrated hours %d, want 0 with B parked", n)
	}
	exec(t, db, `UPDATE parked_video_usage SET resolved_at = now()`)
	if n := count(); n != 1 {
		t.Fatalf("unrated hours %d, want 1 once B's park is resolved and B is still not rated", n)
	}
}

// TOMBSTONES at the rater: every rollup carries its wire usage_id, and every
// usage_id the rater removes gets a tombstone in the same run (reconcile,
// parked, regrained), cleared when the id is live again. The other direction
// (a rollup lost outside the rater leaves none, and token-push holds it) is
// TestIntegration_AVanishedVideoRecordIsDeletedOnlyWhenTheRaterRemovedIt.
func TestIntegration_TheRaterTombstonesEveryVideoUsageIDItRemoves(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_tombstone_it")
	tombstones := func() map[string]string {
		t.Helper()
		rows, err := db.Query(`SELECT usage_id, reason FROM video_reconcile_deletion`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]string{}
		for rows.Next() {
			var id, reason string
			if err := rows.Scan(&id, &reason); err != nil {
				t.Fatal(err)
			}
			out[id] = reason
		}
		return out
	}
	stamped := func() string {
		t.Helper()
		var id, org string
		if err := db.QueryRow(`SELECT COALESCE(usage_id, ''), org_id FROM rated_video_usage`).Scan(&id, &org); err != nil {
			t.Fatal(err)
		}
		return id
	}
	vseed(t, db, vrow(func(r *vbRow) { r.id = "video_t1" }))
	vaggregate(t, db, itHour, itHour.Add(time.Hour))
	first := stamped()
	want := meteredusage.VideoUsageID(meteredusage.FormatWindowStart(itHour), "org-1", "user", "user-1", "res-v",
		meteredusage.VideoAttributes{AuthID: "auth-a", ModelID: "wan-ep", ServingMode: "dedicated",
			BaseModel: "Wan-AI/Wan2.2-T2V-A14B-Diffusers", Task: "t2v", Resolution: "720p"})
	if first != want {
		t.Fatalf("stamped usage_id %s, want token-push's id %s", first, want)
	}

	// Regrained: the org changes, the row keeps its grain and gets a new id.
	exec(t, db, `UPDATE video_billing_event SET org_id = 'org-2'`)
	if res := vaggregate(t, db, itHour, itHour.Add(time.Hour)); res.TombstonesWritten != 1 {
		t.Fatalf("%+v, want one tombstone", res)
	}
	second := stamped()
	if ts := tombstones(); second == first || ts[first] != "regrained" || len(ts) != 1 {
		t.Fatalf("tombstones %v, new id %s; want the old id %s tombstoned as regrained", ts, second, first)
	}

	// Live again: the org changes back, the old id's tombstone is cleared and
	// the second id is tombstoned.
	exec(t, db, `UPDATE video_billing_event SET org_id = 'org-1'`)
	vaggregate(t, db, itHour, itHour.Add(time.Hour))
	if ts := tombstones(); stamped() != first || ts[first] != "" || ts[second] != "regrained" {
		t.Fatalf("tombstones %v; want %s cleared (live again) and %s tombstoned", ts, first, second)
	}

	// Parked: a second org for the identity parks it and deletes its rollup.
	vseed(t, db, vrow(func(r *vbRow) { r.id = "video_t2"; r.org = "org-3" }))
	vaggregate(t, db, itHour, itHour.Add(time.Hour))
	if ts := tombstones(); ts[first] != "parked" {
		t.Fatalf("tombstones %v; want %s tombstoned as parked", ts, first)
	}

	// The park resolves: the rollup is back with its old id, whose tombstone
	// is cleared (a tombstone never releases an id the rater produces).
	exec(t, db, `DELETE FROM video_billing_event WHERE job_id = 'video_t2'`)
	vaggregate(t, db, itHour, itHour.Add(time.Hour))
	if ts := tombstones(); stamped() != first || ts[first] != "" {
		t.Fatalf("tombstones %v; want %s cleared once its rollup is back", ts, first)
	}

	// Reconcile: the jobs vanish.
	exec(t, db, `DELETE FROM video_billing_event`)
	vaggregate(t, db, itHour, itHour.Add(time.Hour))
	if ts := tombstones(); ts[first] != "reconcile" {
		t.Fatalf("tombstones %v; want %s tombstoned as reconcile", ts, first)
	}
}

// A database that applied 0017 before usage_id and the tombstones existed
// (dev installs before #143): 0017 down still runs its guard (it adds the
// column first; the unstamped rows refuse), and 0017 up applied again by hand
// brings the schema up to date.
func TestIntegration_Migration0017OnADatabaseFromBeforeTheTombstones(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_m0017_old_it")
	exec(t, db, `DROP TABLE video_reconcile_deletion`)
	exec(t, db, `DROP INDEX rated_video_usage_usage_id_uq`)
	exec(t, db, `ALTER TABLE rated_video_usage DROP COLUMN usage_id`)
	exec(t, db, `INSERT INTO rated_video_usage (id, auth_id, owner_type, owner_id, resource_id, org_id, model_id,
		serving_mode, base_model, task, resolution, region, window_start, window_end, output_seconds, output_frames,
		request_count, gpu_seconds, event_count)
		VALUES (repeat('a', 32), 'a', '', '', 'r', 'o', 'm', 'dedicated', '', 't2v', '720p', '', now(), now(), 1, 1, 1, 1, 1)`)
	if _, err := db.Exec(readMigration(t, "0017_rated_video_usage.down.sql")); err == nil || !strings.Contains(err.Error(), "never accepted") {
		t.Fatalf("down on the old schema: err = %v, want the guard's refusal (not a missing column)", err)
	}
	exec(t, db, readMigration(t, "0017_rated_video_usage.up.sql"))
	// With no rows, down runs every guard on the old schema (no tombstone
	// table) and succeeds.
	exec(t, db, `DELETE FROM rated_video_usage`)
	exec(t, db, `DROP TABLE video_reconcile_deletion`)
	exec(t, db, readMigration(t, "0017_rated_video_usage.down.sql"))
	exec(t, db, readMigration(t, "0017_rated_video_usage.up.sql"))
	exec(t, db, `INSERT INTO rated_video_usage (id, auth_id, owner_type, owner_id, resource_id, org_id, model_id,
		serving_mode, base_model, task, resolution, region, window_start, window_end, output_seconds, output_frames,
		request_count, gpu_seconds, event_count)
		VALUES (repeat('a', 32), 'a', '', '', 'r', 'o', 'm', 'dedicated', '', 't2v', '720p', '', now(), now(), 1, 1, 1, 1, 1)`)
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM video_reconcile_deletion`).Scan(&n); err != nil {
		t.Fatalf("tombstone table after re-applying up: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM rated_video_usage WHERE usage_id IS NULL`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("unstamped rows %d (err %v), want the old row with usage_id NULL", n, err)
	}
}

// A job with '|' in an envelope field is invalid usage (withheld, counted,
// exit 2), never a rollup: the wire usage_id joins those fields with '|'
// unescaped, so two such grains could share one id.
func TestIntegration_VideoJobsWithAPipeInAnEnvelopeFieldAreInvalidUsage(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_pipe_it")
	vseed(t, db,
		vrow(func(r *vbRow) { r.user = "u|x"; r.resource = "res" }),
		vrow(func(r *vbRow) { r.user = "u"; r.resource = "x|res" }),
		vrow(func(r *vbRow) { r.org = "org|1" }),
		vrow(nil))
	res := vaggregate(t, db, itHour, itHour.Add(time.Hour))
	assertVideoPartition(t, res, 4)
	if res.InvalidUsageEvents != 3 || res.EventsAggregated != 1 || !res.HasAnomaly() {
		t.Fatalf("%+v, want the three jobs with a '|' withheld as invalid", res)
	}
	// Nor are they never-rated usage: they were never rateable.
	exec(t, db, `DELETE FROM rated_video_usage`)
	exec(t, db, `DELETE FROM video_billing_event WHERE strpos(org_id || COALESCE(user_id, '') || resource_id, '|') = 0`)
	if n, err := itStore(db).UnratedVideoHours(context.Background(), itHour, itHour.Add(time.Hour)); err != nil || n != 0 {
		t.Fatalf("never-rated hours %d (err %v), want 0 for jobs with a '|'", n, err)
	}
}
