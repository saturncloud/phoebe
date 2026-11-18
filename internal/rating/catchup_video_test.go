package rating

import "testing"

// The catch-up's summed result carries every video counter, so withheld video
// jobs, video reconcile deletions and parked video in caught-up hours reach
// the rater's exit code.
func TestAddResults_SumsTheVideoCounters(t *testing.T) {
	a := AggregateResult{Video: VideoAggregateResult{InvalidUsageEvents: 1, ParkedWritten: 1}}
	b := AggregateResult{Video: VideoAggregateResult{UnattributableEvents: 2, ReconciledDeletions: 3, OutOfReachHours: 1}}
	sum := addResults(a, b)
	want := VideoAggregateResult{InvalidUsageEvents: 1, UnattributableEvents: 2, ReconciledDeletions: 3,
		ParkedWritten: 1, OutOfReachHours: 1}
	if sum.Video != want {
		t.Fatalf("summed video %+v, want %+v", sum.Video, want)
	}
	r := Result{AggregateResult: sum}
	if !r.HasAnomaly() || r.AllReconciledDeletions() != 3 {
		t.Fatalf("HasAnomaly %v, AllReconciledDeletions %d; want true and 3", r.HasAnomaly(), r.AllReconciledDeletions())
	}
}
