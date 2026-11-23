package rating

import (
	"reflect"
	"testing"
)

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

// VideoAggregateResult.add sums every counter: a field added later and
// forgotten in add would silently drop out of the catch-up and re-check
// totals (the bug this sum replaced).
func TestVideoAggregateResultAddSumsEveryField(t *testing.T) {
	var one VideoAggregateResult
	v := reflect.ValueOf(&one).Elem()
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).Kind() != reflect.Int64 {
			t.Fatalf("field %s is %s; extend this test", v.Type().Field(i).Name, v.Field(i).Kind())
		}
		v.Field(i).SetInt(1)
	}
	sum := reflect.ValueOf(one.add(one))
	for i := 0; i < sum.NumField(); i++ {
		if sum.Field(i).Int() != 2 {
			t.Errorf("add does not sum %s", sum.Type().Field(i).Name)
		}
	}
}
