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
	// bookForHour, when set, supplies the price book EFFECTIVE DURING a given hour
	// instead of using the single static book. See RunWindow.
	bookForHour BookForHour
	log         *logging.Logger
}

// BookForHour returns the price book effective during the hour starting at
// hourStart. The manager owns the effective-dated price series, so this is how a
// rating run prices each hour at the rates that were in force during it —
// which is what makes re-rating an old window idempotent, however many times
// prices have changed since.
//
// It returns an error rather than a fallback book when the prices for that hour
// cannot be obtained: rating an hour at the WRONG prices is worse than not rating
// it, because the resulting rollup looks authoritative.
type BookForHour func(ctx context.Context, hourStart time.Time) (*PriceBook, error)

// New constructs a Rater over a Store and a loaded PriceBook (the YAML price file).
// Rating uses this one book for every hour — correct only when the book is known to
// be the right one for the window (an operator-authored file, or a single-hour run).
// Prefer WithBookForHour for multi-hour windows against the manager.
func New(store Store, book *PriceBook, log *logging.Logger) *Rater {
	return &Rater{store: store, book: book, log: log}
}

// WithBookForHour makes the rater price each hour from the book effective during
// that hour, rather than from one snapshot for the whole window.
func (r *Rater) WithBookForHour(f BookForHour) *Rater {
	r.bookForHour = f
	return r
}

// Result summarises one rating run. It is returned to the caller AND logged, so an
// operator / CronJob can assert on it.
//
// TotalCost is the window's total as NUMERIC TEXT — money never becomes a Go
// number. UnpricedEvents, UnattributableEvents, MissingUsageEvents, and InvalidUsageEvents are
// fail-loud signals: traffic the book could not price, rows that could not be
// attributed, and attempts without authoritative engine counts.
type Result struct {
	WindowStart time.Time
	WindowEnd   time.Time
	// int64: COUNT/SUM over an arbitrary backfill window can exceed 2^31; the SQL
	// casts these as ::bigint to avoid a silent 32-bit overflow (see store.go).
	EventsRated          int64
	UnpricedEvents       int64 // events whose model had NO resolvable price (NOT $0-billed)
	UnattributableEvents int64 // in-window rows with NULL auth_id/resource_id/model_id (upstream leak)
	MissingUsageEvents   int64 // attempts with no authoritative engine usage block (zero-charge) — TOTAL, reported not paged
	// ExpectedMissingUsageEvents is the routine share of MissingUsageEvents: the
	// client aborted, or the attempt terminated non-success. Reported, never paged.
	ExpectedMissingUsageEvents int64
	// UnexplainedMissingUsageEvents is the alarming share: a SUCCESSFUL response
	// carrying no usage block, i.e. served work we cannot bill. This is the
	// missing-usage signal that pages.
	UnexplainedMissingUsageEvents int64
	InvalidUsageEvents            int64  // authoritative evidence with malformed token counts (never money)
	AmbiguousBaseEvents           int64  // events under an ft: rollup spanning >1 base_model (E3 violation)
	AmbiguousOrgEvents            int64  // events under a rollup spanning >1 non-NULL org_id (E2 attribution bug)
	RollupsWritten                int64  // distinct (auth_id, resource_id, model_id, hour) rows upserted
	ReconciledDeletions           int64  // stale in-window rollups DELETED because this re-run no longer produces them
	TotalCost                     string // sum of all rollup costs, NUMERIC as text
}

// HasUnpriced reports whether any event could not be priced (a loud outcome even
// though the run "succeeded").
func (r Result) HasUnpriced() bool { return r.UnpricedEvents > 0 }

// HasUnattributable reports whether any in-window row was skipped for a NULL
// auth_id/resource_id/model_id — like HasUnpriced, a loud, exit-nonzero outcome. A
// NULL resource_id means the row can't name its deployment/org (E2), so it can't be
// billed and is counted here rather than attributed to a NULL org.
func (r Result) HasUnattributable() bool { return r.UnattributableEvents > 0 }

// HasMissingUsage reports attempts retained for audit but excluded from money
// because the serving engine did not supply authoritative token counts. This is
// the TOTAL across both causes and is REPORTED, not paged — see
// HasUnexplainedMissingUsage for the paging signal and HasAnomaly for why.
func (r Result) HasMissingUsage() bool { return r.MissingUsageEvents > 0 }

// HasUnexplainedMissingUsage reports the alarming share of missing usage: the
// attempt was neither aborted by the client nor terminated with a failure status,
// so the engine reported SUCCESS while reporting no tokens. That is work we may
// have served and cannot bill, so it is the fail-loud, exit-nonzero signal.
//
// The routine share (client aborts, upstream failures) is deliberately excluded:
// this branch made zero-usage rows a NORMAL product of every abort and every 5xx,
// so paging on the total would fire hourly on any install with real traffic and
// would bury the rare anomalies (unpriced, unattributable, ambiguous) that share
// the exit-2 channel. Ratified with Hugo, 2026-09-21.
func (r Result) HasUnexplainedMissingUsage() bool { return r.UnexplainedMissingUsageEvents > 0 }

// HasInvalidUsage reports authoritative raw evidence whose token counts violate
// the billing invariants. It is retained for repair but excluded from money.
func (r Result) HasInvalidUsage() bool { return r.InvalidUsageEvents > 0 }

// HasAmbiguousBase reports whether any ft: rollup spanned more than one base_model in a
// window — the E3 ft-uniqueness violation. A uuid4 checkpoint id cannot carry two
// bases, so a nonzero count means base_model propagation is broken upstream; the rollup
// is NOT billed (it would silently bill at the cheaper base), it screams. Loud,
// exit-nonzero, like the other anomalies.
func (r Result) HasAmbiguousBase() bool { return r.AmbiguousBaseEvents > 0 }

// HasAmbiguousOrg reports whether any rollup spanned more than one distinct non-NULL
// org_id in a window — an E2 attribution propagation bug (one resource resolving to two
// orgs). The rollup is NOT billed (it would silently mis-attribute to one org), it
// screams. Loud, exit-nonzero, like the other anomalies. A partial-NULL org (real org +
// missing-header rows) is NOT ambiguous and never trips this.
func (r Result) HasAmbiguousOrg() bool { return r.AmbiguousOrgEvents > 0 }

// HasAnomaly reports whether something leaked: events that could not be priced,
// rows that could not be attributed, a SUCCESSFUL attempt that reported no engine
// usage, malformed authoritative counts, an ft: rollup spanning multiple
// base_models, or a rollup spanning multiple orgs. All are RARE and WRONG, so
// cmd/rater exits non-zero on any of them and an operator should be paged.
//
// Deliberately NOT here: the missing-usage TOTAL. Client aborts and upstream
// failures legitimately produce zero-usage rows on every install with traffic, so
// including them would make exit 2 fire hourly and destroy its meaning for the
// conditions above. Those are reported (and land in
// billing_reconciliation_hourly) rather than paged.
func (r Result) HasAnomaly() bool {
	return r.HasUnpriced() || r.HasUnattributable() || r.HasUnexplainedMissingUsage() || r.HasInvalidUsage() || r.HasAmbiguousBase() || r.HasAmbiguousOrg()
}

// Run rates [windowStart, windowEnd): it runs the SINGLE SQL statement that
// resolves, sums, upserts AND counts the anomalies in one snapshot, then surfaces
// the outcome.
//
// FAIL-LOUD ON MISSING PRICE / UNATTRIBUTABLE (the fail-closed rule): an event
// whose model has no resolvable price at its time, and a row with a NULL
// auth_id/resource_id/model_id, are NOT summed into any rollup — the SQL excludes them. They
// are COUNTED by the SAME statement that writes the rollups (one snapshot, so a
// row the drainer commits mid-run can never be excluded-but-uncounted), logged
// loudly (ERROR), and drive cmd/rater's exit-nonzero path. They never become $0
// rollups (a $0 rollup is indistinguishable from "served, but free" and silently
// loses revenue / hides an upstream leak).
//
// IDEMPOTENCY IS RECONCILE (Hugo's decision — "what the latest run says is what
// bills"): the SQL recomputes each rollup from scratch and upserts ON CONFLICT DO
// UPDATE, AND deletes any in-window rated_usage row this run did NOT reproduce in
// priced (one that fell out to ambiguous/unpriced, or whose events vanished). So a
// re-run converges rated_usage to exactly the latest run's output — never
// double-counts, and never leaves a superseded rollup billing at its stale cost. A
// clean identical re-run is a no-op (same rows upserted, nothing deleted). See store.go.
//
// windowExplicit threads the routine-vs-backfill distinction into the rating-side
// observability so the LOG severity of a reconcile-delete matches the EXIT-code
// contract (option (c), see cmd/rater). On a ROUTINE run (default trailing-hours
// window, windowExplicit == false) a reconcile-delete rewrote a prior bill with no
// operator behind it — data vanished from billing_event or an upstream regression
// dropped events — so it is logged at ERROR (page). On an EXPLICIT backfill
// (--since/--until, windowExplicit == true) the same delete is intended convergence,
// logged at INFO. The reconcile SEMANTICS are identical either way; only the log
// severity (and, in cmd/rater, the exit code) turns on windowExplicit. The flag is
// passed in rather than recomputed here because only cmd/rater knows how the window
// was chosen, mirroring the exit-code gate.
func (r *Rater) Run(ctx context.Context, windowStart, windowEnd time.Time, windowExplicit bool) (Result, error) {
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
	res.ReconciledDeletions = rr.ReconciledDeletions
	res.TotalCost = rr.TotalCost
	res.UnpricedEvents = rr.UnpricedEvents
	res.UnattributableEvents = rr.UnattributableEvents
	res.MissingUsageEvents = rr.MissingUsageEvents
	res.ExpectedMissingUsageEvents = rr.ExpectedMissingUsageEvents
	res.UnexplainedMissingUsageEvents = rr.UnexplainedMissingUsageEvents
	res.InvalidUsageEvents = rr.InvalidUsageEvents
	res.AmbiguousBaseEvents = rr.AmbiguousBaseEvents
	res.AmbiguousOrgEvents = rr.AmbiguousOrgEvents

	if res.HasAmbiguousBase() {
		r.log.Error.Printf("rating: window [%s,%s) has %d events under AMBIGUOUS-BASE rollups (a single model_id whose base_model-priced events carried MORE THAN ONE rate in a window: >1 distinct base_model, or mixed premium/plain-base pricing from an X-Saturn-Adapter flap) — a base_model/adapter PROPAGATION violation, NOT a priceable rollup; these rollups are NOT billed (billing the MIN rate would silently under-charge). Fix header propagation and re-rate this window",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), res.AmbiguousBaseEvents)
	}
	if res.HasAmbiguousOrg() {
		r.log.Error.Printf("rating: window [%s,%s) has %d events under AMBIGUOUS-ORG rollups (a single (auth, resource, model, hour) rollup carried MORE THAN ONE distinct non-NULL org_id) — a deployment owns exactly one org, so this is an E2 attribution PROPAGATION bug (Atlas injected conflicting X-Saturn-Org-Id values for one resource); these rollups are NOT billed (a guessed org would mis-attribute revenue). Fix org_id propagation and re-rate this window",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), res.AmbiguousOrgEvents)
	}
	if res.HasUnattributable() {
		r.log.Error.Printf("rating: window [%s,%s) has %d UNATTRIBUTABLE billing_event rows (NULL auth_id/resource_id/model_id — a NULL resource_id can't name the deployment/org for E2 billing) — these cannot be rated; the interceptor's billing gate should reject them before metering, so a nonzero count means revenue is leaking upstream",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), res.UnattributableEvents)
	}
	if res.HasUnpriced() {
		r.log.Error.Printf("rating: window [%s,%s) has %d UNPRICED events (nothing resolved: model_id absent from the price file AND base_model empty/unpriced — which for fine-tune traffic (X-Saturn-Adapter present or an ft: id) is a base_model PROPAGATION BUG, not a free model) — these are NOT billed; the create-time price gate should prevent this, so a nonzero count means an unpriced model was served (or a header stopped propagating). Add the price/fix the header and re-rate this window",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), res.UnpricedEvents)
	}
	if res.HasUnexplainedMissingUsage() {
		r.log.Error.Printf("rating: window [%s,%s) has %d UNEXPLAINED MISSING-USAGE attempts — the response was NOT aborted and did NOT fail, so the engine reported success while supplying no authoritative usage block: work may have been served that cannot be billed. Reconcile against engine logs before settling the invoice",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), res.UnexplainedMissingUsageEvents)
	}
	if res.ExpectedMissingUsageEvents > 0 {
		// Routine: client aborts and upstream failures. Zero-charge by design and
		// reviewed in billing_reconciliation_hourly — reported at INFO so it never
		// competes with the fail-loud channel above.
		r.log.Info.Printf("rating: window [%s,%s) has %d expected missing-usage attempts (client aborted or upstream failed) — retained at zero charge, excluded from rated_usage, no action required",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), res.ExpectedMissingUsageEvents)
	}
	if res.HasInvalidUsage() {
		r.log.Error.Printf("rating: window [%s,%s) has %d INVALID-USAGE events — authoritative raw evidence was retained but excluded from rated_usage because token counts were negative or cached tokens exceeded prompt tokens. Repair or quarantine the evidence before settling the invoice",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), res.InvalidUsageEvents)
	}

	// A re-rate that SUPERSEDES prior billing (deleted stale rollups) is significant —
	// not a leaked-anomaly (it does not flip HasAnomaly), but it changes a customer's
	// bill, so surface it. 0 on a first run or a clean identical re-run. The SEVERITY
	// turns on windowExplicit (the routine-vs-backfill contract, option (c)):
	//   - ROUTINE (default trailing-hours window, !windowExplicit): no operator chose
	//     this window, so rewriting a prior bill is alarming — events vanished from
	//     billing_event (data loss) or an upstream regression dropped them (e.g.
	//     base_model stopped propagating → rollups went unpriced/ambiguous). ERROR:
	//     page/investigate. This is the loud half that matches cmd/rater's exit 2.
	//   - EXPLICIT backfill (--since/--until, windowExplicit): convergence is exactly
	//     what the operator asked for (e.g. a late price fix) — INFO, not a page.
	// The deletion count + window appear in both; only the level and the wording differ.
	if res.ReconciledDeletions > 0 {
		if windowExplicit {
			r.log.Info.Printf("rating: EXPLICIT backfill window [%s,%s) reconcile DELETED %d stale rollup(s) that prior runs billed but this run no longer produces (became ambiguous/unpriced, or their events vanished) — intended convergence, 'what the latest run says is what bills'",
				windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), res.ReconciledDeletions)
		} else {
			r.log.Error.Printf("rating: ROUTINE window [%s,%s) reconcile DELETED %d previously-billed rollup(s) — a routine run REWROTE A PRIOR BILL with no operator behind it, meaning those events VANISHED from billing_event (data loss) or an upstream regression now drops them (e.g. base_model stopped propagating → rollups went unpriced/ambiguous). This is NOT a backfill; page/investigate (run with --since/--until only after confirming the deletion is intended)",
				windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), res.ReconciledDeletions)
		}
	}

	if res.HasAnomaly() {
		r.log.Error.Printf("rating: window [%s,%s) rated %d events into %d rollups, total=%s USD; %d UNPRICED events dropped (backfill prices and re-rate), %d UNATTRIBUTABLE rows skipped (NULL auth_id/resource_id/model_id — upstream billing-gate leak), %d UNEXPLAINED MISSING-USAGE attempts (success with no usage block — reconcile engine logs) of %d missing-usage total, %d INVALID-USAGE events excluded from money, %d AMBIGUOUS-BASE events dropped (one model_id, more than one base_model-derived rate — fix base_model/adapter propagation and re-rate), %d AMBIGUOUS-ORG events dropped (one resource spanning multiple orgs — fix org_id propagation and re-rate)",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339),
			res.EventsRated, res.RollupsWritten, res.TotalCost, res.UnpricedEvents, res.UnattributableEvents,
			res.UnexplainedMissingUsageEvents, res.MissingUsageEvents, res.InvalidUsageEvents, res.AmbiguousBaseEvents, res.AmbiguousOrgEvents)
	} else {
		r.log.Info.Printf("rating: window [%s,%s) rated %d events into %d rollups, total=%s USD",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339),
			res.EventsRated, res.RollupsWritten, res.TotalCost)
	}
	return res, nil
}

// RunWindow rates [windowStart, windowEnd) HOUR BY HOUR, pricing each hour from
// the book effective DURING that hour, and returns the aggregate.
//
// THE PRICING INSTANT IS THE HOUR START (Hugo, 2026-09-22). Each hour is priced
// from the book effective at hourStart, so a price change takes effect at the NEXT
// HOUR BOUNDARY: a reprice landing at 10:30 applies to the 11:00 hour, not to any
// part of 10:00. That is the deliberate quantum, not an accident of the loop.
//
// The alternative — splitting an hour at the reprice instant — was rejected: the
// rating SQL buckets on date_trunc('hour', ...), so sub-hour pricing cannot be
// expressed without a schema change, and the precision is not worth a one-way
// door. Operators scheduling a price change should therefore pick an hour
// boundary; anything else silently rounds forward to one.
//
// WHY PER HOUR (the correctness reason): prices are effective-dated in the manager,
// but one PriceBook is a flat snapshot with no time dimension. The default run
// covers 24 trailing hours, so pricing the whole span from any single snapshot
// would misprice every hour on the far side of a mid-window price change — silently,
// since the resulting rollups look authoritative. Rating each hour against its own
// book removes that class of error entirely, and makes a re-rate idempotent by
// construction: an hour always resolves to the rates that were in force during it,
// however many times prices have changed since. This is what replaced phoebe's local
// price-freeze table (rating_price_lock): the freeze was a local workaround for a
// time dimension the wire call used to discard.
//
// The rating SQL is UNCHANGED: each hour is still one RateWindow call with one flat
// book, so the money path keeps its single-snapshot semantics. Only the number of
// calls and which book each gets are new.
//
// FAIL CLOSED PER HOUR: if an hour's prices cannot be obtained, that hour is not
// rated and the run returns the error. A partially-rated window is reported through
// the aggregate (hours already committed keep their rollups — each hour's SQL is its
// own transaction), so a retry converges rather than double-counting. That report is
// SELF-CONSISTENT: agg.TotalCost is kept in step with the counters as each hour is
// folded in, so an error return never claims N events rated at a total of $0.
//
// HOUR-ALIGNED ONLY: the rating SQL buckets on date_trunc('hour') and REPLACES a
// bucket, so a partial hour would overwrite a complete rollup with a partial sum
// (and its reconcile-delete would erase a neighbouring hour's rows). Unaligned
// bounds are therefore refused, not clamped.
//
// Anomaly counts, reconcile deletions and cost SUM across the hours, so the caller's
// exit-code contract (see cmd/rater) is unchanged: any hour leaking an anomaly makes
// the aggregate report it.
func (r *Rater) RunWindow(ctx context.Context, windowStart, windowEnd time.Time, windowExplicit bool) (Result, error) {
	windowStart = windowStart.UTC()
	windowEnd = windowEnd.UTC()
	agg := Result{WindowStart: windowStart, WindowEnd: windowEnd, TotalCost: "0"}

	if !windowStart.Before(windowEnd) {
		return agg, fmt.Errorf("rating: empty/inverted window [%s,%s)", windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339))
	}
	if !windowStart.Truncate(time.Hour).Equal(windowStart) || !windowEnd.Truncate(time.Hour).Equal(windowEnd) {
		return agg, fmt.Errorf("rating: window [%s,%s) is not hour-aligned; the rating SQL buckets on date_trunc('hour') and REPLACES a bucket, so a partial hour would overwrite a complete rollup",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339))
	}
	// A per-hour provider is mandatory: rating a multi-hour window from one
	// snapshot is exactly the mispricing RunWindow exists to prevent, and the
	// manager is the only price source, so there is no book to fall back to.
	if r.bookForHour == nil {
		return agg, fmt.Errorf("rating: no per-hour price provider configured (refusing to rate [%s,%s) from a single snapshot)",
			windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339))
	}

	// The zero Dec is exact 0 (see decimal.go); money is summed as exact decimal,
	// never a float.
	var total Dec
	for hourStart := windowStart; hourStart.Before(windowEnd); hourStart = hourStart.Add(time.Hour) {
		hourEnd := hourStart.Add(time.Hour)
		book, err := r.bookForHour(ctx, hourStart)
		if err != nil {
			return agg, fmt.Errorf("rating: prices for hour %s: %w (refusing to rate this hour at the wrong prices)",
				hourStart.Format(time.RFC3339), err)
		}
		if book == nil {
			return agg, fmt.Errorf("rating: no price book for hour %s (refusing to rate at $0)", hourStart.Format(time.RFC3339))
		}
		hourRater := &Rater{store: r.store, book: book, log: r.log}
		hourRes, err := hourRater.Run(ctx, hourStart, hourEnd, windowExplicit)
		agg.accumulate(hourRes)
		if err != nil {
			return agg, err
		}
		hourCost, err := ParseDec(hourRes.TotalCost)
		if err != nil {
			// This hour's counters are already folded in but its cost is NOT: an
			// unparseable total is precisely the case where the hour's cost is
			// unknown, so it must not be invented. agg.TotalCost keeps the sum of
			// the hours whose cost IS known.
			return agg, fmt.Errorf("rating: hour %s total %q: %w", hourStart.Format(time.RFC3339), hourRes.TotalCost, err)
		}
		total = total.Add(hourCost)
		// Keep the aggregate's cost in step with the counters accumulate() already
		// folded in, so an error return below reports the cost actually committed
		// rather than a self-contradicting "0".
		agg.TotalCost = total.String()
	}
	return agg, nil
}

// accumulate folds one hour's outcome into the window aggregate. Costs are summed
// separately (exact decimal, never a float).
func (r *Result) accumulate(hour Result) {
	r.EventsRated += hour.EventsRated
	r.RollupsWritten += hour.RollupsWritten
	r.ReconciledDeletions += hour.ReconciledDeletions
	r.UnpricedEvents += hour.UnpricedEvents
	r.UnattributableEvents += hour.UnattributableEvents
	r.MissingUsageEvents += hour.MissingUsageEvents
	r.ExpectedMissingUsageEvents += hour.ExpectedMissingUsageEvents
	r.UnexplainedMissingUsageEvents += hour.UnexplainedMissingUsageEvents
	r.InvalidUsageEvents += hour.InvalidUsageEvents
	r.AmbiguousBaseEvents += hour.AmbiguousBaseEvents
	r.AmbiguousOrgEvents += hour.AmbiguousOrgEvents
}
