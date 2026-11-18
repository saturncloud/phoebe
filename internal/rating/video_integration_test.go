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
// sum exactly (5.0625 + 5.0625 = 10.1250, never a float), request_count counts
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
	if r.outputSeconds != "10.1250" || r.frames != 162 || r.requests != 2 || r.gpuSeconds != "361.500" || r.events != 3 {
		t.Fatalf("rollup %+v, want 10.1250 s, 162 frames, 2 requests, 361.500 GPU s over 3 jobs", r)
	}
	if r.org != "org-1" || r.ownerType != "user" || r.ownerID != "user-1" || r.region != "" || r.task != "t2v" || r.resolution != "720p" {
		t.Fatalf("rollup identity %+v", r)
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
	if len(got) != 1 || got[0].requests != 2 || got[0].outputSeconds != "10.1250" {
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
	if reason != "ambiguous_org" || events != 2 || requests != 2 || secs != "10.1250" {
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
	store := NewPostgresStore(db)
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
	if v.Reason != "ambiguous_org" || v.EventCount != 2 || v.RequestCount != 2 || v.OutputSeconds != "10.1250" ||
		v.OutputFrames != 162 || v.GPUSeconds != "241.000" || v.RawEvents != 2 || v.RawEventsPruned {
		t.Fatalf("video parked row %+v", v)
	}
	if !strings.Contains(v.UsageLabel(), "output_seconds=10.1250") {
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
	if _, err := New(store, quiet()).Run(context.Background(), itHour, itHour.Add(time.Hour), true); err == nil ||
		!strings.Contains(err.Error(), "pruned") {
		t.Fatalf("Run over a pruned parked video hour: err = %v, want the refusal", err)
	}
}

// 0014 down refuses while video usage is parked or not yet accepted by the
// manager, and otherwise round-trips (down, then up again).
func TestIntegration_Migration0014DownRefusesWhileVideoUsageIsParkedOrUnsent(t *testing.T) {
	db := openRatingSchema(t, "phoebe_video_m0014_it")
	vseed(t, db, vrow(nil), vrow(func(r *vbRow) { r.resource = "res-2"; r.org = "org-2" }),
		vrow(func(r *vbRow) { r.resource = "res-2" }))
	vaggregate(t, db, itHour, itHour.Add(time.Hour))
	down := readMigration(t, "0014_rated_video_usage.down.sql")
	if _, err := db.Exec(down); err == nil || !strings.Contains(err.Error(), "unresolved") {
		t.Fatalf("down with parked video usage: err = %v, want the refusal", err)
	}
	exec(t, db, `UPDATE parked_video_usage SET resolved_at = now()`)
	if _, err := db.Exec(down); err == nil || !strings.Contains(err.Error(), "never accepted") {
		t.Fatalf("down with unsent video usage: err = %v, want the refusal", err)
	}
	exec(t, db, `INSERT INTO pushed_hour (window_start, pushed_at) VALUES ('`+itHour.Format(time.RFC3339)+`', now() + interval '1 minute')`)
	exec(t, db, `INSERT INTO pushed_usage (window_start, usage_id, kind, auth_id, owner_type, owner_id, resource_id, model_id, serving_mode)
		VALUES ('`+itHour.Format(time.RFC3339)+`', repeat('a', 64), 'video', 'a', '', '', 'r', 'm', 'dedicated'),
		       ('`+itHour.Format(time.RFC3339)+`', repeat('b', 64), 'token', 'a', '', '', 'r', 'm', 'dedicated')`)
	exec(t, db, down)
	var left int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pushed_usage`).Scan(&left); err != nil || left != 1 {
		t.Fatalf("pushed_usage rows after down = %d (err %v), want only the token row", left, err)
	}
	exec(t, db, readMigration(t, "0014_rated_video_usage.up.sql"))
	var kind string
	if err := db.QueryRow(`SELECT kind FROM pushed_usage`).Scan(&kind); err != nil || kind != "token" {
		t.Fatalf("kind after up = %q (err %v), want the 'token' default", kind, err)
	}
}
