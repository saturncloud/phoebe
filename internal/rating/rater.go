package rating

import (
	"context"
	"fmt"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

// Rater is ORCHESTRATION ONLY. It holds the loaded price book (from the YAML file),
// resolves the window, runs the SINGLE SQL statement (project prices → resolve → sum
// → upsert, with the anomaly counts in the same snapshot), surfaces anomalies, and
// reports a Result. It does NO per-event money math — the billable-prompt formula
// and the cost summation happen in SQL (see Store / store.go); the fine-tune premium
// is applied in exact Dec when the book is projected. Money in the database, Go as
// the conductor.
//
// THE PRICE BOOK IS LOADED ONCE, AT CONSTRUCTION, and frozen for the run: a single
// rater run rates against one immutable snapshot of the file, so a mid-run file edit
// can never split a window across two price sets (E1: the row freezes its own rate).
type Rater struct {
	store Store
	book  *PriceBook
	log   *logging.Logger
}

// New constructs a Rater over a Store and a loaded PriceBook (the YAML price file).
func New(store Store, book *PriceBook, log *logging.Logger) *Rater {
	return &Rater{store: store, book: book, log: log}
}

// Result summarises one rating run. It is returned to the caller AND logged, so an
// operator / CronJob can assert on it.
//
// TotalCost is the window's total as NUMERIC TEXT — money never becomes a Go
// number. UnpricedEvents / UnattributableEvents are the fail-loud signals: real
// traffic the price book could not price, and rows that could not be attributed.
type Result struct {
	WindowStart time.Time
	WindowEnd   time.Time
	// int64: COUNT/SUM over an arbitrary backfill window can exceed 2^31; the SQL
	// casts these as ::bigint to avoid a silent 32-bit overflow (see store.go).
	EventsRated          int64
	UnpricedEvents       int64  // events whose model had NO resolvable price (NOT $0-billed)
	UnattributableEvents int64  // in-window rows with NULL auth_id/model_id (upstream leak)
	RollupsWritten       int64  // distinct (auth_id, model_id, hour) rows upserted
	TotalCost            string // sum of all rollup costs, NUMERIC as text
}

// HasUnpriced reports whether any event could not be priced (a loud outcome even
// though the run "succeeded").
func (r Result) HasUnpriced() bool { return r.UnpricedEvents > 0 }

// HasUnattributable reports whether any in-window row was skipped for a NULL
// auth_id/model_id — like HasUnpriced, a loud, exit-nonzero outcome.
func (r Result) HasUnattributable() bool { return r.UnattributableEvents > 0 }

// HasAnomaly reports whether the run rated cleanly but something leaked: events
// that could not be priced OR rows that could not be attributed. Both are the same
// class of fail-loud signal, so cmd/rater exits non-zero on either.
func (r Result) HasAnomaly() bool { return r.HasUnpriced() || r.HasUnattributable() }

// Run rates [windowStart, windowEnd): it runs the SINGLE SQL statement that
// resolves, sums, upserts AND counts the anomalies in one snapshot, then surfaces
// the outcome.
//
// FAIL-LOUD ON MISSING PRICE / UNATTRIBUTABLE (the fail-closed rule): an event
// whose model has no resolvable price at its time, and a row with a NULL
// auth_id/model_id, are NOT summed into any rollup — the SQL excludes them. They
// are COUNTED by the SAME statement that writes the rollups (one snapshot, so a
// row the drainer commits mid-run can never be excluded-but-uncounted), logged
// loudly (ERROR), and drive cmd/rater's exit-nonzero path. They never become $0
// rollups (a $0 rollup is indistinguishable from "served, but free" and silently
// loses revenue / hides an upstream leak).
//
// IDEMPOTENCY: the SQL recomputes each rollup from scratch and upserts ON CONFLICT
// DO UPDATE, so re-running a window reconciles to the correct totals and never
// double-counts. See store.go.
func (r *Rater) Run(ctx context.Context, windowStart, windowEnd time.Time) (Result, error) {
	windowStart = windowStart.UTC()
	windowEnd = windowEnd.UTC()
	res := Result{WindowStart: windowStart, WindowEnd: windowEnd}

	if !windowStart.Before(windowEnd) {
		return res, fmt.Errorf("rating: empty/inverted window [%s,%s)", windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339))
	}

	rr, err := r.store.RateWindow(ctx, r.book, windowStart, windowEnd)
	if err != nil {
		return res, err
	}
	res.EventsRated = rr.EventsRated
	res.RollupsWritten = rr.RollupsWritten
	res.TotalCost = rr.TotalCost
	res.UnpricedEvents = rr.UnpricedEvents
	res.UnattributableEvents = rr.UnattributableEvents

	if res.HasUnattributable() {
		r.log.Error.Printf("rating: window [%s,%s) has %d UNATTRIBUTABLE billing_event rows (NULL auth_id/model_id) — these cannot be rated; the interceptor's billing gate should reject them before metering, so a nonzero count means revenue is leaking upstream",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), res.UnattributableEvents)
	}
	if res.HasUnpriced() {
		r.log.Error.Printf("rating: window [%s,%s) has %d UNPRICED events (model_id absent from the price file — no base entry; or an ft: id whose base_model is empty/unpriced, which for a fine-tune is a base_model PROPAGATION BUG, not a free model) — these are NOT billed; the create-time price gate should prevent this, so a nonzero count means an unpriced model was served (or base_model stopped propagating). Add the price/fix the header and re-rate this window",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), res.UnpricedEvents)
	}

	if res.HasAnomaly() {
		r.log.Error.Printf("rating: window [%s,%s) rated %d events into %d rollups, total=%s USD; %d UNPRICED events dropped (backfill prices and re-rate), %d UNATTRIBUTABLE rows skipped (NULL auth_id/model_id — upstream billing-gate leak)",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339),
			res.EventsRated, res.RollupsWritten, res.TotalCost, res.UnpricedEvents, res.UnattributableEvents)
	} else {
		r.log.Info.Printf("rating: window [%s,%s) rated %d events into %d rollups, total=%s USD",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339),
			res.EventsRated, res.RollupsWritten, res.TotalCost)
	}
	return res, nil
}
