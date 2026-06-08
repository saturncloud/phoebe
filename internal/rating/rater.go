package rating

import (
	"context"
	"fmt"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

// Rater is ORCHESTRATION ONLY. It resolves the window, runs the SQL statements
// (the resolve→sum→upsert insert, and the anomaly count), surfaces anomalies, and
// reports a Result. It holds NO money value and does NO per-event math — all
// pricing, the billable-prompt formula, the derivation policy, and the summation
// happen in SQL (see Store / store.go). This is the deliberate v2 shape: money in
// the database, Go as the conductor.
type Rater struct {
	store Store
	log   *logging.Logger
}

// New constructs a Rater over a Store.
func New(store Store, log *logging.Logger) *Rater {
	return &Rater{store: store, log: log}
}

// Result summarises one rating run. It is returned to the caller AND logged, so an
// operator / CronJob can assert on it.
//
// TotalCost is the window's total as NUMERIC TEXT — money never becomes a Go
// number. UnpricedEvents / UnattributableEvents are the fail-loud signals: real
// traffic the price book could not price, and rows that could not be attributed.
type Result struct {
	WindowStart          time.Time
	WindowEnd            time.Time
	EventsRated          int
	UnpricedEvents       int    // events whose model had NO resolvable price (NOT $0-billed)
	UnattributableEvents int    // in-window rows with NULL auth_id/model_id (upstream leak)
	RollupsWritten       int    // distinct (auth_id, model_id, hour) rows upserted
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

// Run rates [windowStart, windowEnd): it counts anomalies and runs the SQL
// resolve→sum→upsert, then surfaces the outcome.
//
// FAIL-LOUD ON MISSING PRICE / UNATTRIBUTABLE (the fail-closed rule): an event
// whose model has no resolvable price at its time, and a row with a NULL
// auth_id/model_id, are NOT summed into any rollup — the SQL excludes them. They
// are COUNTED (CountAnomalies, sharing the SAME resolution as the insert) and
// logged loudly (ERROR), and drive cmd/rater's exit-nonzero path. They never
// become $0 rollups (a $0 rollup is indistinguishable from "served, but free" and
// silently loses revenue / hides an upstream leak).
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

	// Count anomalies first (over the same resolution the insert uses), so the loud
	// signals are known even if the insert writes zero rows.
	anomalies, err := r.store.CountAnomalies(ctx, windowStart, windowEnd)
	if err != nil {
		return res, err
	}
	res.UnpricedEvents = anomalies.UnpricedEvents
	res.UnattributableEvents = anomalies.UnattributableEvents

	if res.HasUnattributable() {
		r.log.Error.Printf("rating: window [%s,%s) has %d UNATTRIBUTABLE billing_event rows (NULL auth_id/model_id) — these cannot be rated; the interceptor's billing gate should reject them before metering, so a nonzero count means revenue is leaking upstream",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), res.UnattributableEvents)
	}
	if res.HasUnpriced() {
		r.log.Error.Printf("rating: window [%s,%s) has %d UNPRICED events (no resolvable price at event time — own rate, or base via derived_from through the global policy, all absent; or a derived_from chain > 1 hop) — these are NOT billed; set/backfill a price and re-rate this window",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), res.UnpricedEvents)
	}

	rr, err := r.store.RateWindow(ctx, windowStart, windowEnd)
	if err != nil {
		return res, err
	}
	res.EventsRated = rr.EventsRated
	res.RollupsWritten = rr.RollupsWritten
	res.TotalCost = rr.TotalCost

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
