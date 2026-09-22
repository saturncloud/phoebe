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
