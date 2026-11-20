package videos

import (
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/rating"
)

// The rater re-rates video only inside VideoRateReach, so the reaper must keep
// video_billing_event at least that long plus the day of margin the reach
// assumes; otherwise a routine re-rate would read a partly pruned hour and
// reconcile-delete billed video.
func TestReaperDefaultRetentionCoversTheRatersVideoReach(t *testing.T) {
	var c ReaperConfig
	c.defaults()
	if c.RowRetention < rating.VideoRateReach+24*time.Hour {
		t.Fatalf("row retention %s is shorter than the rater's video reach %s plus a day", c.RowRetention, rating.VideoRateReach)
	}
}
