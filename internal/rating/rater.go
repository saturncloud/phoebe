package rating

import (
	"context"
	"fmt"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

// Rater runs the batch rating job: read a window of billing_event, rate each
// event against the price book, aggregate into per-(auth_id, model, hour)
// rollups, and upsert them into rated_usage.
type Rater struct {
	store Store
	log   *logging.Logger
}

// New constructs a Rater over a Store.
func New(store Store, log *logging.Logger) *Rater {
	return &Rater{store: store, log: log}
}

// Result summarises one rating run. It is returned to the caller AND logged, so
// an operator / CronJob can assert on it. UnpricedEvents > 0 is the fail-loud
// signal: real traffic the price book could not price.
type Result struct {
	WindowStart    time.Time
	WindowEnd      time.Time
	EventsRead     int
	EventsRated    int
	UnpricedEvents int // events whose model had NO price at their time (NOT $0-billed)
	// UnattributableEvents counts billing_event rows in-window with a NULL
	// auth_id/model — cannot be attributed; counted, never rated. Nonzero =
	// upstream billing-gate leak (the interceptor should reject these before they
	// are metered), so it is loud and exit-nonzero, exactly like UnpricedEvents.
	UnattributableEvents int
	RollupsWritten       int   // distinct (auth_id, model, hour) rows upserted
	TotalCostMicro       int64 // sum of all rollup costs, micro-USD
}

// HasUnpriced reports whether any event could not be priced. The caller treats
// this as a non-zero (loud) outcome even though the run "succeeded".
func (r Result) HasUnpriced() bool { return r.UnpricedEvents > 0 }

// HasUnattributable reports whether any in-window row was skipped for a NULL
// auth_id/model. Like HasUnpriced, a true value is a loud, exit-nonzero outcome.
func (r Result) HasUnattributable() bool { return r.UnattributableEvents > 0 }

// HasAnomaly reports whether the run rated cleanly but something leaked: events
// that could not be priced OR rows that could not be attributed. Both are the
// same class of fail-loud signal (the window "succeeded" yet revenue/data went
// uncaptured), so cmd/rater exits non-zero on either.
func (r Result) HasAnomaly() bool { return r.HasUnpriced() || r.HasUnattributable() }

// rollupKey is the aggregation grain: one cost bucket per API key, per model, per
// hour — matching rated_usage's unique constraint and Atlas's hourly grain.
type rollupKey struct {
	authID      string
	model       string
	windowStart time.Time // hour-truncated UTC
}

// Run rates [windowStart, windowEnd) and upserts the resulting rollups.
//
// FAIL-LOUD ON MISSING PRICE (the fail-closed rule): an event whose model has no
// price at its time is NOT aggregated into any rollup — it is counted as an
// unpriced event and logged loudly (ERROR, with model + time). It does NOT become
// a $0 rollup row, because a $0 rollup is indistinguishable from "served, but free"
// and silently loses revenue. The run still completes and writes the rollups it
// COULD price; the unpriced count surfaces the gap for backfill once a price is set.
//
// IDEMPOTENCY: aggregation recomputes each rollup's totals from scratch from the
// current window, and UpsertRollups REPLACES on conflict — so re-running a window
// reconciles to the correct totals and never double-counts. See Store.UpsertRollups.
func (r *Rater) Run(ctx context.Context, windowStart, windowEnd time.Time) (Result, error) {
	windowStart = windowStart.UTC()
	windowEnd = windowEnd.UTC()
	res := Result{WindowStart: windowStart, WindowEnd: windowEnd}

	if !windowStart.Before(windowEnd) {
		return res, fmt.Errorf("rating: empty/inverted window [%s,%s)", windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339))
	}

	prices, err := r.store.LoadPrices(ctx)
	if err != nil {
		return res, err
	}
	book := NewPriceBook(prices)

	events, unattributable, err := r.store.ReadWindow(ctx, windowStart, windowEnd)
	if err != nil {
		return res, err
	}
	res.EventsRead = len(events)
	// Unattributable rows (NULL auth_id/model) are skipped at the store seam but
	// must be surfaced, not dropped: count them so the loud summary + exit-nonzero
	// path below fires. Nonzero here means the upstream billing gate leaked.
	res.UnattributableEvents = unattributable
	if res.HasUnattributable() {
		r.log.Error.Printf("rating: window [%s,%s) has %d UNATTRIBUTABLE billing_event rows (NULL auth_id/model) — these cannot be rated; the interceptor's billing gate should reject them before metering, so a nonzero count means revenue is leaking upstream",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), res.UnattributableEvents)
	}

	// Aggregate priced events into per-key rollups.
	rollups := make(map[rollupKey]*Rollup)
	for _, e := range events {
		cost, err := Rate(e, book)
		if err != nil {
			// ErrNoPrice (and any wrap of it) is the fail-loud case: count, log
			// loudly, and DROP the event from the rollups — never bill it $0.
			// (Rate returns ErrNoPrice wrapped with model+time; errors.Is matches.)
			res.UnpricedEvents++
			r.log.Error.Printf("rating: UNPRICED event dropped (not billed): auth_id=%s model=%s at=%s — set a price-book entry and re-rate this window",
				e.AuthID, e.Model, e.At.UTC().Format(time.RFC3339))
			continue
		}

		hour := e.At.UTC().Truncate(time.Hour)
		key := rollupKey{authID: e.AuthID, model: e.Model, windowStart: hour}
		ru := rollups[key]
		if ru == nil {
			ru = &Rollup{
				AuthID:      e.AuthID,
				Model:       e.Model,
				WindowStart: hour,
				WindowEnd:   hour.Add(time.Hour),
			}
			rollups[key] = ru
		}
		billablePrompt := BillablePromptTokens(e.PromptTokens, e.CachedTokens)
		ru.PromptTokens += e.PromptTokens
		ru.CachedTokens += e.CachedTokens
		ru.CompletionTokens += e.CompletionTokens
		ru.BillablePromptTokens += billablePrompt
		ru.CostMicroUSD += cost
		ru.EventCount++

		res.EventsRated++
		res.TotalCostMicro += cost
	}

	out := make([]Rollup, 0, len(rollups))
	for _, ru := range rollups {
		out = append(out, *ru)
	}
	if err := r.store.UpsertRollups(ctx, out); err != nil {
		return res, err
	}
	res.RollupsWritten = len(out)

	// Loud summary. Unpriced AND unattributable events are ERRORs (lost-revenue /
	// lost-data signals), not info — both surface here and drive the exit-nonzero
	// path in cmd/rater so a CronJob alerts.
	if res.HasAnomaly() {
		r.log.Error.Printf("rating: window [%s,%s) rated %d/%d events into %d rollups, total=%d micro-USD; %d UNPRICED events dropped (backfill prices and re-rate), %d UNATTRIBUTABLE rows skipped (NULL auth_id/model — upstream billing-gate leak)",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339),
			res.EventsRated, res.EventsRead, res.RollupsWritten, res.TotalCostMicro, res.UnpricedEvents, res.UnattributableEvents)
	} else {
		r.log.Info.Printf("rating: window [%s,%s) rated %d events into %d rollups, total=%d micro-USD",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339),
			res.EventsRated, res.RollupsWritten, res.TotalCostMicro)
	}
	return res, nil
}
