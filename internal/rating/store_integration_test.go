//go:build integration

// Package rating integration test: runs the REAL rating SQL (rateWindowSQL) against
// a LIVE Postgres, pricing from a YAML PriceBook (E1), then asserts the rated_usage
// rows the SQL wrote — including the APPLIED per-token rates frozen onto each row —
// equal the pure Rate() oracle row-for-row over the same fixture. This is the
// production-path half of the conformance pair (the in-Go oracle self-consistency
// check lives in rater_test.go's TestOracleModel_SelfConsistent).
//
// Gated behind the `integration` build tag AND a non-empty PHOEBE_TEST_DATABASE_URL.
// Run with:
//
//	PHOEBE_TEST_DATABASE_URL=postgres://... go test -tags=integration ./internal/rating/...
package rating

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// schemaDDL is the rating schema (just billing_event + rated_usage now — prices are
// a YAML file, not DB tables), created in an isolated schema per test run so the
// test is self-contained and leaves no residue.
const schemaDDL = `
-- billing_event mirrors the v1 metering schema (migration 0001): the model NAME
-- lives in the model column, which the rater aliases to model_id (the price key).
CREATE TABLE billing_event (
    request_id        VARCHAR(255) PRIMARY KEY,
    auth_id           VARCHAR(64),
    -- resource_id (the deployment id) is NULLABLE here, mirroring migration 0001: the
    -- rater fails closed on a NULL (counts it unattributable), it does not reject it.
    resource_id       VARCHAR(64),
    -- org_id (the deployment-owning org) is NULLABLE here, mirroring migration
    -- d3a2b4c5e6f7: captured at meter time, carried onto rated_usage; a NULL is held
    -- + screamed at push, never billed to a guessed org.
    org_id            VARCHAR(64),
    model             VARCHAR(255),
    base_model        VARCHAR(255),
    prompt_tokens     INTEGER NOT NULL DEFAULT 0,
    cached_tokens     INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    event_ts          TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX billing_event_rating_instant_ix
    ON billing_event ((COALESCE(event_ts, created_at)));

CREATE TABLE rated_usage (
    id                      VARCHAR(32) PRIMARY KEY,
    auth_id                 VARCHAR(64) NOT NULL,
    resource_id             VARCHAR(64) NOT NULL,
    -- org_id NULLABLE (unlike resource_id): carried from billing_event by the rater;
    -- a NULL is the held-not-billed signal at push (migration d3a2b4c5e6f7).
    org_id                  VARCHAR(64),
    model_id                VARCHAR(255) NOT NULL,
    window_start            TIMESTAMPTZ NOT NULL,
    window_end              TIMESTAMPTZ NOT NULL,
    prompt_tokens           BIGINT NOT NULL,
    cached_tokens           BIGINT NOT NULL,
    completion_tokens       BIGINT NOT NULL,
    billable_prompt_tokens  BIGINT NOT NULL,
    cost                    NUMERIC(20,9) NOT NULL,
    applied_prompt_rate     NUMERIC(20,9) NOT NULL DEFAULT 0,
    applied_cached_rate     NUMERIC(20,9) NOT NULL DEFAULT 0,
    applied_completion_rate NUMERIC(20,9) NOT NULL DEFAULT 0,
    event_count             BIGINT NOT NULL,
    rated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT rated_usage_auth_resource_model_window_uq UNIQUE (auth_id, resource_id, model_id, window_start)
);

-- Mirror the production indexes (migrations/0002_rating.sql): the auth-leading index
-- for billing queries and the window_start-leading index the reconcile DELETE needs (it
-- filters window_start alone; every auth-leading index leaves it trailing). No standalone
-- (resource_id, window_start) index — production ships none until the E2 per-deployment
-- reader exists (see the migration's NOTE), so the fixture omits it too.
CREATE INDEX rated_usage_auth_id_window_start_ix ON rated_usage (auth_id, window_start);
CREATE INDEX rated_usage_window_start_ix ON rated_usage (window_start);`

// conformanceBook is the fixture price book shared by the conformance tests: base
// "b" with its own rate, fine-tune "f" derived from "b", 1.5× premium.
func conformanceBook() *PriceBook {
	return newTestBook(
		map[string]Rate3{"b": rate3("0.000005", "0.0000005", "0.00002")},
		map[string]string{"f": "b"},
		PolicyMultiplier, MustDec("1.5"), Dec{},
	)
}

func TestIntegration_RateWindow_ConformsToOracle(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const sch = "phoebe_rating_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")
	book := conformanceBook()

	// Events: priced base, priced derived, unpriced, unattributable. Each priced/unpriced
	// event carries a resource_id (E2 grain); the unattributable one has none.
	events := []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "b", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: hour.Add(5 * time.Minute)},
		{AuthID: "a", ResourceID: "r", ModelID: "f", PromptTokens: 100, CachedTokens: 0, CompletionTokens: 0, At: hour.Add(15 * time.Minute)},
		{AuthID: "a", ResourceID: "r", ModelID: "unpriced", PromptTokens: 9, At: hour.Add(1 * time.Minute)},
		{AuthID: "", ResourceID: "r", ModelID: "b", PromptTokens: 9, At: hour.Add(2 * time.Minute)},
	}
	for i, e := range events {
		_, err := db.ExecContext(ctx,
			`INSERT INTO billing_event (request_id, auth_id, resource_id, model, prompt_tokens, cached_tokens, completion_tokens, event_ts)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			fmt.Sprintf("req-%d", i), nullableStr(e.AuthID), nullableStr(e.ResourceID), nullableStr(e.ModelID),
			e.PromptTokens, e.CachedTokens, e.CompletionTokens, e.At)
		if err != nil {
			t.Fatalf("seed event %d: %v", i, err)
		}
	}

	store := NewPostgresStore(db)

	// Run the REAL rating SQL, priced from the YAML PriceBook. The anomaly counts
	// ride the SAME statement.
	res, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}
	if res.RollupsWritten != 2 {
		t.Fatalf("rollups = %d, want 2", res.RollupsWritten)
	}
	if res.UnpricedEvents != 1 || res.UnattributableEvents != 1 {
		t.Fatalf("RateWindow anomaly counts = %d/%d, want 1/1 (single-snapshot accounting)", res.UnpricedEvents, res.UnattributableEvents)
	}
	if got := res.EventsRated + res.UnpricedEvents + res.UnattributableEvents; got != int64(len(events)) {
		t.Fatalf("rated+unpriced+unattributable = %d, want %d", got, len(events))
	}

	// Oracle: independent Rate() over the priced+attributable events; also assert the
	// applied-rate columns equal the resolved rate (applied-rate-stored-on-row).
	//
	// QUANTIZE-THEN-MULTIPLY: production bills the 9dp-QUANTIZED per-token rate (the
	// NUMERIC(20,9) projected into rating_price and frozen onto the row), so the
	// oracle MUST be fed rate.Quantized() — feeding the un-quantized resolved rate
	// would silently mis-calibrate the conformance guard against the day a sub-nano
	// premium residue appears (see TestConformance_PremiumQuantizedBeforeBilling and
	// the residue fixture below).
	for _, e := range []RatedEvent{events[0], events[1]} {
		rate, err := book.Resolve(e.ModelID)
		if err != nil {
			t.Fatalf("oracle resolve %s: %v", e.ModelID, err)
		}
		billed := rate.Quantized() // the rate production actually bills and stores
		wantCost := Rate(e, billed).String()
		var gotCost, gotPrompt, gotCached, gotCompletion string
		err = db.QueryRowContext(ctx,
			`SELECT cost::text, applied_prompt_rate::text, applied_cached_rate::text, applied_completion_rate::text
			   FROM rated_usage WHERE auth_id=$1 AND model_id=$2 AND window_start=$3`,
			e.AuthID, e.ModelID, hour).Scan(&gotCost, &gotPrompt, &gotCached, &gotCompletion)
		if err != nil {
			t.Fatalf("read rated_usage (%s): %v", e.ModelID, err)
		}
		if MustDec(gotCost).String() != wantCost {
			t.Errorf("model %s: SQL cost = %s, oracle Rate() = %s", e.ModelID, gotCost, wantCost)
		}
		// The row carries the EXACT 9dp rate it was billed at (premium applied, then
		// quantized).
		if MustDec(gotPrompt).String() != billed.Prompt.String() ||
			MustDec(gotCached).String() != billed.Cached.String() ||
			MustDec(gotCompletion).String() != billed.Completion.String() {
			t.Errorf("model %s applied rates = %s/%s/%s, want %s/%s/%s (frozen rate must equal quantized resolved rate)",
				e.ModelID, gotPrompt, gotCached, gotCompletion,
				billed.Prompt, billed.Cached, billed.Completion)
		}
	}

	// E2 ATTRIBUTION: every written rollup carries the event's resource_id (the
	// deployment id billing resolves the org from). The two priced rollups were seeded
	// with resource_id 'r'.
	var nWithResource, nTotal int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FILTER (WHERE resource_id = 'r'), COUNT(*) FROM rated_usage`).
		Scan(&nWithResource, &nTotal); err != nil {
		t.Fatalf("read resource_id: %v", err)
	}
	if nWithResource != nTotal || nTotal != 2 {
		t.Fatalf("rated_usage resource_id: %d of %d rows carry 'r', want all 2 (E2 attribution must be on every row)", nWithResource, nTotal)
	}

	// Deterministic surrogate ids: capture before the re-run.
	idsBefore := readRatedUsageIDs(t, db)

	// Idempotency (idempotent-rerun): re-run, totals unchanged, still 2 rows.
	res2, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow re-run: %v", err)
	}
	if res2.RollupsWritten != 2 || MustDec(res2.TotalCost).String() != MustDec(res.TotalCost).String() {
		t.Fatalf("re-run not idempotent: %+v vs %+v", res2, res)
	}

	idsAfter := readRatedUsageIDs(t, db)
	if len(idsBefore) != len(idsAfter) {
		t.Fatalf("row count changed across re-run: %d → %d", len(idsBefore), len(idsAfter))
	}
	for k, id := range idsBefore {
		if idsAfter[k] != id {
			t.Errorf("rollup %s id changed across re-run: %s → %s (id must be deterministic)", k, id, idsAfter[k])
		}
	}
}

// TestIntegration_ResourceIDGrainAndFailClosed runs the REAL SQL to pin the E2
// resource_id grain in Postgres:
//   - TWO deployments (distinct resource_id) of the SAME model by the SAME auth in the
//     SAME hour produce TWO distinct rated_usage rows (NOT one summed row) — they may
//     bill to different orgs, so collapsing them would mis-attribute revenue;
//   - a NULL-resource_id event is UNATTRIBUTABLE: counted, never written (a row that
//     can't name its deployment/org must never be billed). This is the live-Postgres
//     proof of the fail-closed attribution partition.
func TestIntegration_ResourceIDGrainAndFailClosed(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const sch = "phoebe_rating_resource_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{"b": rate3("0.000005", "0", "0")},
		nil, PolicyIdentity, Dec{}, Dec{},
	)

	// Two deployments of model "b" by auth "a" in the same hour, plus a NULL-resource_id
	// event that must fail closed.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, model, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('d1','a','deploy-1','b',100,0,$1),
		        ('d2','a','deploy-2','b',100,0,$1),
		        ('dnull','a',NULL,'b',100,0,$1)`, hour.Add(5*time.Minute)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	store := NewPostgresStore(db)
	res, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}

	// Two deployments → two rollups; the NULL-resource_id event is unattributable.
	if res.RollupsWritten != 2 || res.EventsRated != 2 {
		t.Fatalf("rollups/events = %d/%d, want 2/2 (distinct deployments bill separately)", res.RollupsWritten, res.EventsRated)
	}
	if res.UnattributableEvents != 1 {
		t.Fatalf("unattributable = %d, want 1 (the NULL-resource_id event must be counted, never billed)", res.UnattributableEvents)
	}
	// PARTITION holds with resource_id in the mix (all five buckets; org is 0 here).
	if got := res.EventsRated + res.UnpricedEvents + res.UnattributableEvents +
		res.AmbiguousBaseEvents + res.AmbiguousOrgEvents; got != 3 {
		t.Fatalf("rated+unpriced+unattr+ambiguous_base+ambiguous_org = %d, want 3 (all seeded events)", got)
	}

	// Exactly two rows, one per deployment; NONE with a NULL/empty resource_id.
	var nRows, nNull int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*), COUNT(*) FILTER (WHERE resource_id IS NULL OR resource_id = '') FROM rated_usage`).
		Scan(&nRows, &nNull); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if nRows != 2 {
		t.Fatalf("rated_usage rows = %d, want 2 (one per deployment)", nRows)
	}
	if nNull != 0 {
		t.Fatalf("rated_usage has %d NULL/empty-resource_id rows — a row that can't name its deployment/org must NEVER be written", nNull)
	}
	for _, rid := range []string{"deploy-1", "deploy-2"} {
		var n int
		if err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM rated_usage WHERE auth_id='a' AND resource_id=$1 AND model_id='b' AND window_start=$2`,
			rid, hour).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", rid, err)
		}
		if n != 1 {
			t.Fatalf("deployment %s rollups = %d, want exactly 1", rid, n)
		}
	}
}

// TestIntegration_AmbiguousOrgFailsLoud is the org twin of the ambiguous_base
// fail-loud test: it exercises the three net-new org-rollup boundaries against real
// Postgres (the oracle store can't model org, so this is their only executing coverage):
//
//   - TWO DISTINCT non-NULL orgs under one (auth,resource,model,hour) rollup → an E2
//     attribution propagation bug: the rollup is WITHHELD (never billed to a guessed
//     MAX org), counted in AmbiguousOrgEvents, and drives HasAnomaly/exit-nonzero.
//   - PARTIAL-NULL (real org on some events, NULL on others — the header-rollout window)
//     → MAX(org_id)/COUNT(DISTINCT) ignore the NULL, so the rollup is NOT ambiguous: it
//     bills, carrying the single real org. This is the precise behavior the PR exists to
//     make safe.
//   - A CLEAN single-org rollup rates normally alongside.
//
// The base+org BOTH-ambiguous interaction (the strict-partition exclusivity clause) is
// covered separately by TestIntegration_BothAmbiguousCountedOnceAsBase.
func TestIntegration_AmbiguousOrgFailsLoud(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const sch = "phoebe_rating_ambigorg_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{"b": rate3("0.000005", "0", "0")},
		nil, PolicyIdentity, Dec{}, Dec{},
	)

	// Seed three rollups in one hour, INCLUDING the org_id column (the other tests omit
	// it, so org is NULL there; here it is load-bearing):
	//   resource 'amb'   : two events, DISTINCT non-NULL orgs 'org-1'/'org-2' → ambiguous.
	//   resource 'clean' : one event, org 'org-3' → bills normally.
	//   resource 'mixed' : two events, org 'org-4' and NULL → partial-NULL, bills as org-4.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('a1','a','amb','org-1','b',100,0,$1),
		        ('a2','a','amb','org-2','b',100,0,$1),
		        ('c1','a','clean','org-3','b',100,0,$1),
		        ('m1','a','mixed','org-4','b',100,0,$1),
		        ('m2','a','mixed',NULL,'b',100,0,$1)`, hour.Add(5*time.Minute)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	store := NewPostgresStore(db)
	res, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}

	// The two conflicting-org events are counted ambiguous (a nonzero count is what
	// drives the rater's exit-nonzero / HasAnomaly path — that predicate wiring is pinned
	// separately by TestResult_HasAmbiguousOrgDrivesAnomaly; here we assert the count the
	// SQL produces, since RateWindow returns the store-level RateResult, not Result).
	if res.AmbiguousOrgEvents != 2 {
		t.Fatalf("AmbiguousOrgEvents = %d, want 2 (the two distinct-org events)", res.AmbiguousOrgEvents)
	}
	// clean (1 event) + mixed (2 events) rate; amb (2 events) is withheld.
	if res.RollupsWritten != 2 || res.EventsRated != 3 {
		t.Fatalf("rollups/events = %d/%d, want 2/3 (clean + mixed bill; amb withheld)", res.RollupsWritten, res.EventsRated)
	}
	// PARTITION over all five buckets (org now nonzero).
	if got := res.EventsRated + res.UnpricedEvents + res.UnattributableEvents +
		res.AmbiguousBaseEvents + res.AmbiguousOrgEvents; got != 5 {
		t.Fatalf("partition sum = %d, want 5 (all seeded events accounted exactly once)", got)
	}

	// The ambiguous rollup must NOT be billed to a guessed org.
	var nAmb int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM rated_usage WHERE resource_id='amb'`).Scan(&nAmb); err != nil {
		t.Fatalf("count amb: %v", err)
	}
	if nAmb != 0 {
		t.Fatalf("rated_usage has %d rows for the two-org rollup — it must NEVER be billed to a guessed org", nAmb)
	}

	// The partial-NULL rollup bills, carrying the single REAL org (MAX ignored the NULL).
	var mixedOrg sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT org_id FROM rated_usage WHERE resource_id='mixed'`).Scan(&mixedOrg); err != nil {
		t.Fatalf("read mixed org: %v", err)
	}
	if !mixedOrg.Valid || mixedOrg.String != "org-4" {
		t.Fatalf("mixed rollup org_id = %v, want 'org-4' (partial-NULL must collapse to the real org, not split or null)", mixedOrg)
	}
	// And the clean rollup carries its org.
	var cleanOrg sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT org_id FROM rated_usage WHERE resource_id='clean'`).Scan(&cleanOrg); err != nil {
		t.Fatalf("read clean org: %v", err)
	}
	if !cleanOrg.Valid || cleanOrg.String != "org-3" {
		t.Fatalf("clean rollup org_id = %v, want 'org-3'", cleanOrg)
	}
}

// TestIntegration_BothAmbiguousCountedOnceAsBase pins the strict-partition exclusivity
// clause `WHERE ambiguous_org AND NOT ambiguous_base` (store.go) against real Postgres: a
// single rollup that is SIMULTANEOUSLY base-ambiguous (one ft: id over two base_models)
// AND org-ambiguous (two distinct non-NULL orgs) must be counted EXACTLY ONCE — as
// ambiguous_base (the more specific E3 signal), NOT also as ambiguous_org. Without the
// exclusivity clause this rollup would be double-counted and the partition identity
// (rated + unpriced + unattr + ambiguous_base + ambiguous_org == total) would break. The
// existing single-axis tests can't catch a dropped/inverted exclusivity guard because
// neither trips both flags at once.
func TestIntegration_BothAmbiguousCountedOnceAsBase(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const sch = "phoebe_rating_bothambig_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{
			"cheap/base":     rate3("0.000001", "0", "0"),
			"expensive/base": rate3("0.000009", "0", "0"),
		},
		nil, PolicyMultiplier, MustDec("1.5"), Dec{},
	)

	// One ft:dupe rollup whose two events disagree on BOTH base_model AND org_id.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, base_model, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('b1','a','d1','org-1','ft:dupe','cheap/base',1000,0,$1),
		        ('b2','a','d1','org-2','ft:dupe','expensive/base',1000,0,$1)`, hour.Add(5*time.Minute)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	store := NewPostgresStore(db)
	res, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}

	// Counted ONCE as base (the more specific signal), NOT also as org.
	if res.AmbiguousBaseEvents != 2 {
		t.Fatalf("AmbiguousBaseEvents = %d, want 2 (both events of the dupe rollup)", res.AmbiguousBaseEvents)
	}
	if res.AmbiguousOrgEvents != 0 {
		t.Fatalf("AmbiguousOrgEvents = %d, want 0 (a both-ambiguous rollup counts ONLY as base — the exclusivity clause)", res.AmbiguousOrgEvents)
	}
	// Strict partition holds: no double-count.
	if got := res.EventsRated + res.UnpricedEvents + res.UnattributableEvents +
		res.AmbiguousBaseEvents + res.AmbiguousOrgEvents; got != 2 {
		t.Fatalf("partition sum = %d, want 2 (the rollup must be counted exactly once, not double)", got)
	}
	// And nothing was billed (the rollup is withheld).
	if res.RollupsWritten != 0 {
		t.Fatalf("RollupsWritten = %d, want 0 (a both-ambiguous rollup is never billed)", res.RollupsWritten)
	}
}

// TestIntegration_OrgReRateConvergesNeverErases pins the ON CONFLICT org_id =
// COALESCE(EXCLUDED.org_id, rated_usage.org_id) behavior against real Postgres — the
// re-rate one-way-door the battery flagged:
//
//   - NULL -> real (CONVERGENCE): a rollup first rated before its org header was wired
//     (org NULL) picks up the real org on a later re-rate. This is the intended rollout
//     recovery.
//   - real -> NULL (NEVER ERASE): a re-rate over a window whose snapshot has since LOST
//     its org (e.g. a stale/forced replay) must NOT overwrite the prior good org with
//     NULL — that would silently un-attribute already-billed usage. COALESCE keeps the
//     existing org.
func TestIntegration_OrgReRateConvergesNeverErases(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const sch = "phoebe_rating_orgrerate_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{"b": rate3("0.000005", "0", "0")},
		nil, PolicyIdentity, Dec{}, Dec{},
	)
	store := NewPostgresStore(db)
	orgOf := func() sql.NullString {
		var o sql.NullString
		if err := db.QueryRowContext(ctx,
			`SELECT org_id FROM rated_usage WHERE resource_id='d1'`).Scan(&o); err != nil {
			t.Fatalf("read org: %v", err)
		}
		return o
	}

	// Run 1: the org header isn't wired yet → org_id NULL on the event.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('r1','a','d1',NULL,'b',100,0,$1)`, hour.Add(5*time.Minute)); err != nil {
		t.Fatalf("seed run1: %v", err)
	}
	if _, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour)); err != nil {
		t.Fatalf("RateWindow run1: %v", err)
	}
	if o := orgOf(); o.Valid {
		t.Fatalf("after run1 org_id = %v, want NULL (header not yet wired)", o)
	}

	// Run 2: the producer is now injecting → the SAME hour re-rates with a real org.
	// NULL -> real: COALESCE prefers the new non-NULL org.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('r2','a','d1','org-real','b',100,0,$1)`, hour.Add(6*time.Minute)); err != nil {
		t.Fatalf("seed run2: %v", err)
	}
	if _, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour)); err != nil {
		t.Fatalf("RateWindow run2: %v", err)
	}
	if o := orgOf(); !o.Valid || o.String != "org-real" {
		t.Fatalf("after run2 org_id = %v, want 'org-real' (NULL->real convergence)", o)
	}

	// Run 3: a stale replay drops the org headers again (only the NULL-org event is in
	// range). real -> NULL must NOT erase the prior good org. We re-rate with ONLY the
	// original NULL-org event present for this hour by deleting the real-org event first.
	if _, err := db.ExecContext(ctx, `DELETE FROM billing_event WHERE request_id='r2'`); err != nil {
		t.Fatalf("delete r2: %v", err)
	}
	if _, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour)); err != nil {
		t.Fatalf("RateWindow run3: %v", err)
	}
	if o := orgOf(); !o.Valid || o.String != "org-real" {
		t.Fatalf("after run3 org_id = %v, want 'org-real' preserved (a stale NULL replay must NEVER erase a known org)", o)
	}
}

// readRatedUsageIDs returns natural-key → id for every rated_usage row.
func readRatedUsageIDs(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.Query(`SELECT auth_id || '|' || resource_id || '|' || model_id || '|' || extract(epoch FROM window_start)::bigint::text, id FROM rated_usage`)
	if err != nil {
		t.Fatalf("read rated_usage ids: %v", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, id string
		if err := rows.Scan(&k, &id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[k] = id
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// TestConformance_PremiumQuantizedBeforeBilling is the applied-rate self-audit
// guard. The fine-tune premium is applied to the EXACT base rate, then the FINAL
// per-token rate is quantized to 9dp (the NUMERIC(20,9) the row can store and bills
// from) — there is no sub-nano residue left to diverge on. This fixture uses a base
// 1-nano rate × 1.5 = 0.0000000015 → rounds to 0.000000002, the rate that bills.
//
// It asserts three things that together mean the row is self-auditing:
//   - the APPLIED rate stored on the row is the 9dp-quantized premium rate
//     (0.000000002), NOT the un-storable exact 0.0000000015;
//   - the cost equals that stored rate × tokens (reconstructable from the row);
//   - the oracle (premium-then-quantize) matches the SQL row-for-row.
func TestConformance_PremiumQuantizedBeforeBilling(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const sch = "phoebe_rating_quant_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")

	// Base "b": prompt_price = 1 nano. Derived "f": 1.5× → exact 0.0000000015 per
	// prompt token, which quantizes to 0.000000002 (half-up) before billing.
	book := newTestBook(
		map[string]Rate3{"b": rate3("0.000000001", "0", "0")},
		map[string]string{"f": "b"},
		PolicyMultiplier, MustDec("1.5"), Dec{},
	)

	// Three single-prompt-token events for "f" in ONE rollup. Cost = stored rate
	// (0.000000002) × 3 = 0.000000006 — exact, reconstructable from the row.
	events := []RatedEvent{
		{AuthID: "a", ModelID: "f", PromptTokens: 1, At: hour.Add(1 * time.Minute)},
		{AuthID: "a", ModelID: "f", PromptTokens: 1, At: hour.Add(2 * time.Minute)},
		{AuthID: "a", ModelID: "f", PromptTokens: 1, At: hour.Add(3 * time.Minute)},
	}
	for i, e := range events {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO billing_event (request_id, auth_id, resource_id, model, prompt_tokens, cached_tokens, completion_tokens, event_ts)
			 VALUES ($1,$2,'r',$3,$4,$5,$6,$7)`,
			fmt.Sprintf("q-req-%d", i), e.AuthID, e.ModelID,
			e.PromptTokens, e.CachedTokens, e.CompletionTokens, e.At); err != nil {
			t.Fatalf("seed event %d: %v", i, err)
		}
	}

	store := NewPostgresStore(db)
	if _, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour)); err != nil {
		t.Fatalf("RateWindow: %v", err)
	}

	var gotCost, gotAppliedPrompt string
	if err := db.QueryRowContext(ctx,
		`SELECT cost::text, applied_prompt_rate::text FROM rated_usage WHERE auth_id='a' AND model_id='f' AND window_start=$1`,
		hour).Scan(&gotCost, &gotAppliedPrompt); err != nil {
		t.Fatalf("read rated_usage: %v", err)
	}

	// Oracle: premium-then-quantize is what bills.
	resolved, err := book.Resolve("f")
	if err != nil {
		t.Fatalf("oracle resolve: %v", err)
	}
	billed := resolved.Quantized()
	if billed.Prompt.String() != "0.000000002" {
		t.Fatalf("quantized premium rate = %s, want 0.000000002 (exact 0.0000000015 rounds half-up)", billed.Prompt)
	}
	// Applied rate frozen on the row must be the 9dp-quantized rate.
	if MustDec(gotAppliedPrompt).String() != "0.000000002" {
		t.Errorf("applied_prompt_rate = %s, want 0.000000002 (the rate that bills, stored on the row)", gotAppliedPrompt)
	}
	// Cost = stored rate × 3 tokens = 0.000000006, reconstructable from the row.
	wantCost := billed.Prompt.MulInt(3).Round(moneyScale).String()
	if MustDec(gotCost).String() != wantCost {
		t.Errorf("SQL cost = %s, want %s (stored 9dp rate × tokens)", gotCost, wantCost)
	}
}

// TestConformance_OracleQuantizesBeforeMultiply_OnResidue is the proven-teeth guard
// for the ratified quantize-then-multiply spec. It rates a sub-nano-residue
// fine-tune through the REAL SQL, then asserts the SQL cost equals the QUANTIZED
// oracle — AND demonstrates the test has teeth by computing the UN-quantized oracle
// cost the same way the old (mis-calibrated) oracle did and proving it DISAGREES
// with what the SQL bills. If the oracle ever reverts to feeding Rate() the
// un-quantized rate, this test goes RED, so the latent miscalibration can never be
// reintroduced silently.
//
// Residue: base prompt 0.000000001 (1 nano) × 1.5 premium = 0.0000000015 (exact).
//   - quantize-then-multiply (production + ratified oracle): rate → 0.000000002,
//     cost over N tokens = 0.000000002 × N.
//   - sum-then-round (the OLD oracle, un-quantized rate): cost = 0.0000000015 × N
//     rounded once. For N=3: 0.0000000045 → 0.000000005 (rounds DOWN to 5 nano),
//     vs production's 0.000000006 (6 nano). They DIFFER — that gap is the teeth.
func TestConformance_OracleQuantizesBeforeMultiply_OnResidue(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const sch = "phoebe_rating_residue_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{"b": rate3("0.000000001", "0", "0")},
		map[string]string{"f": "b"},
		PolicyMultiplier, MustDec("1.5"), Dec{},
	)

	// Three single-prompt-token "f" events in one rollup (N=3, where the two
	// rounding models DIVERGE: 6 nano vs 5 nano).
	events := []RatedEvent{
		{AuthID: "a", ModelID: "f", PromptTokens: 1, At: hour.Add(1 * time.Minute)},
		{AuthID: "a", ModelID: "f", PromptTokens: 1, At: hour.Add(2 * time.Minute)},
		{AuthID: "a", ModelID: "f", PromptTokens: 1, At: hour.Add(3 * time.Minute)},
	}
	for i, e := range events {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO billing_event (request_id, auth_id, resource_id, model, prompt_tokens, cached_tokens, completion_tokens, event_ts)
			 VALUES ($1,$2,'r',$3,$4,$5,$6,$7)`,
			fmt.Sprintf("res-req-%d", i), e.AuthID, e.ModelID,
			e.PromptTokens, e.CachedTokens, e.CompletionTokens, e.At); err != nil {
			t.Fatalf("seed event %d: %v", i, err)
		}
	}

	store := NewPostgresStore(db)
	if _, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour)); err != nil {
		t.Fatalf("RateWindow: %v", err)
	}

	var gotCost string
	if err := db.QueryRowContext(ctx,
		`SELECT cost::text FROM rated_usage WHERE auth_id='a' AND model_id='f' AND window_start=$1`,
		hour).Scan(&gotCost); err != nil {
		t.Fatalf("read rated_usage: %v", err)
	}

	resolved, err := book.Resolve("f")
	if err != nil {
		t.Fatalf("oracle resolve: %v", err)
	}

	// The RATIFIED oracle: quantize the per-token rate, THEN multiply. This is what
	// the SQL must match.
	quantized := Rate(events[0], resolved.Quantized()).
		Add(Rate(events[1], resolved.Quantized())).
		Add(Rate(events[2], resolved.Quantized())).String()
	if quantized != "0.000000006" {
		t.Fatalf("quantized oracle cost = %s, want 0.000000006 (0.000000002 × 3)", quantized)
	}
	if MustDec(gotCost).String() != quantized {
		t.Errorf("SQL cost = %s, quantized oracle = %s — production and the ratified oracle must agree", gotCost, quantized)
	}

	// TEETH: the OLD, mis-calibrated oracle fed Rate() the UN-quantized resolved
	// rate (sum-then-round). Recompute it that way and prove it DISAGREES with what
	// the SQL bills — so a revert to the un-quantized oracle would flip this test RED.
	unquantized := rateExact(events[0], resolved).
		Add(rateExact(events[1], resolved)).
		Add(rateExact(events[2], resolved)).Round(moneyScale).String()
	if unquantized != "0.000000005" {
		t.Fatalf("un-quantized oracle cost = %s, want 0.000000005 (0.0000000045 rounds half-up to 5 nano)", unquantized)
	}
	if unquantized == MustDec(gotCost).String() {
		t.Fatal("un-quantized oracle MATCHES the SQL on a residue fixture — the conformance guard has no teeth (the spec divergence is undetectable)")
	}
}

// TestIntegration_FineTunePricesViaBaseModel runs the REAL SQL over fine-tune events
// that price through the event-carried base_model (E3): an ft:<checkpoint> model the
// price file never names, but whose base_model IS a priced base, bills at base ×
// premium. It also pins the fail-loud invariant in SQL — an ft: event with a NULL
// base_model lands in UNPRICED (never $0). This is the production fine-tune path now
// that base_model rides on billing_event.
func TestIntegration_FineTunePricesViaBaseModel(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const sch = "phoebe_rating_basemodel_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")
	// File declares ONLY the base; the ft: checkpoint id is not listed. 1.5× premium.
	book := newTestBook(
		map[string]Rate3{"meta-llama/Llama-3.1-8B-Instruct": rate3("0.000004", "0", "0")},
		nil, PolicyMultiplier, MustDec("1.5"), Dec{},
	)

	type seed struct {
		req, model, baseModel string
		prompt                int64
	}
	seeds := []seed{
		// Priced via base_model: ft: id + a known base → base × 1.5.
		{"ft-ok", "ft:9f8e7d6c5b4a", "meta-llama/Llama-3.1-8B-Instruct", 1000},
		// FAIL LOUD: ft: id with NULL base_model → unpriced (propagation bug, never $0).
		{"ft-nobase", "ft:cafebabe", "", 1000},
		// FAIL LOUD: ft: id with an unknown base_model → unpriced.
		{"ft-badbase", "ft:0badf00d", "some/unpriced-base", 1000},
	}
	for _, s := range seeds {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO billing_event (request_id, auth_id, resource_id, model, base_model, prompt_tokens, completion_tokens, event_ts)
			 VALUES ($1,'a','r',$2,$3,$4,0,$5)`,
			s.req, s.model, nullableStr(s.baseModel), s.prompt, hour.Add(5*time.Minute)); err != nil {
			t.Fatalf("seed %s: %v", s.req, err)
		}
	}

	store := NewPostgresStore(db)
	res, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}

	// Exactly one rollup (the priced fine-tune); two unpriced (no/unknown base_model).
	if res.RollupsWritten != 1 || res.EventsRated != 1 {
		t.Fatalf("rollups/events = %d/%d, want 1/1 (only ft-ok prices)", res.RollupsWritten, res.EventsRated)
	}
	if res.UnpricedEvents != 2 {
		t.Fatalf("unpriced = %d, want 2 (ft-nobase + ft-badbase must scream, never $0)", res.UnpricedEvents)
	}

	// The priced fine-tune billed at base × premium, with the derived rate on the row.
	var gotCost, gotApplied string
	if err := db.QueryRowContext(ctx,
		`SELECT cost::text, applied_prompt_rate::text FROM rated_usage WHERE model_id='ft:9f8e7d6c5b4a' AND window_start=$1`,
		hour).Scan(&gotCost, &gotApplied); err != nil {
		t.Fatalf("read rated_usage: %v", err)
	}
	if MustDec(gotApplied).String() != "0.000006000" {
		t.Errorf("applied_prompt_rate = %s, want 0.000006000 (0.000004 × 1.5)", gotApplied)
	}
	// 1000 × 0.000006 = 0.006.
	if MustDec(gotCost).String() != "0.006000000" {
		t.Errorf("cost = %s, want 0.006000000 (base × premium via base_model)", gotCost)
	}

	// Cross-check against the oracle (ResolveEvent → quantize → Rate).
	rate, err := book.ResolveEvent("ft:9f8e7d6c5b4a", "meta-llama/Llama-3.1-8B-Instruct")
	if err != nil {
		t.Fatalf("oracle ResolveEvent: %v", err)
	}
	wantCost := Rate(RatedEvent{PromptTokens: 1000}, rate.Quantized()).String()
	if MustDec(gotCost).String() != wantCost {
		t.Errorf("SQL cost %s != oracle %s (base_model derived path must conform)", gotCost, wantCost)
	}
}

// TestIntegration_FineTuneAmbiguousBaseModelFailsLoud runs the REAL SQL over the E3
// ft-uniqueness violation (FIX 2): a single ft: model_id resolving through TWO distinct
// base_models in one window. E3 mints ft:<checkpoint_artifact_id> as a globally-unique
// uuid4, so this can't happen legitimately; if it does, a blind MIN()-applied-rate would
// silently bill the rollup at the cheaper base. The SQL must instead SPLIT the ambiguous
// rollup out: not upsert it, and count its events as ambiguous_base_events. A clean
// single-base ft: rollup in the same window must still rate normally.
func TestIntegration_FineTuneAmbiguousBaseModelFailsLoud(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const sch = "phoebe_rating_ambig_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{
			"cheap/base":     rate3("0.000001", "0", "0"),
			"expensive/base": rate3("0.000009", "0", "0"),
		},
		nil, PolicyMultiplier, MustDec("1.5"), Dec{},
	)

	type seed struct {
		req, model, baseModel string
		prompt                int64
	}
	seeds := []seed{
		// SAME ft: id, TWO different base_models → ambiguous (must NOT bill at MIN rate).
		{"ambig-1", "ft:dupe", "cheap/base", 1000},
		{"ambig-2", "ft:dupe", "expensive/base", 1000},
		// A clean single-base ft: rollup that MUST still rate.
		{"clean", "ft:clean", "cheap/base", 1000},
	}
	for _, s := range seeds {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO billing_event (request_id, auth_id, resource_id, model, base_model, prompt_tokens, completion_tokens, event_ts)
			 VALUES ($1,'a','r',$2,$3,$4,0,$5)`,
			s.req, s.model, nullableStr(s.baseModel), s.prompt, hour.Add(5*time.Minute)); err != nil {
			t.Fatalf("seed %s: %v", s.req, err)
		}
	}

	store := NewPostgresStore(db)
	res, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}

	// Only the clean rollup rated; the two ambiguous events are counted, not billed.
	if res.RollupsWritten != 1 || res.EventsRated != 1 {
		t.Fatalf("rollups/events = %d/%d, want 1/1 (only the single-base ft: rollup)", res.RollupsWritten, res.EventsRated)
	}
	if res.AmbiguousBaseEvents != 2 {
		t.Fatalf("ambiguous = %d, want 2 (the two-base ft: rollup must scream, never MIN-billed)", res.AmbiguousBaseEvents)
	}
	// NO rated_usage row for the ambiguous ft: id (not even at the cheaper rate).
	var nDupe int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM rated_usage WHERE model_id='ft:dupe'`).Scan(&nDupe); err != nil {
		t.Fatalf("count ft:dupe rollups: %v", err)
	}
	if nDupe != 0 {
		t.Fatalf("rated_usage has %d rows for the ambiguous ft:dupe — it must NOT be billed (silent MIN under-charge)", nDupe)
	}
	// The clean fine-tune billed normally at its base × premium.
	var cost string
	if err := db.QueryRowContext(ctx,
		`SELECT cost::text FROM rated_usage WHERE model_id='ft:clean'`).Scan(&cost); err != nil {
		t.Fatalf("read ft:clean rollup: %v", err)
	}
	if MustDec(cost).String() != "0.001500000" { // 1000 × (0.000001 × 1.5)
		t.Errorf("ft:clean cost = %s, want 0.001500000 (base × premium)", cost)
	}
}

// TestIntegration_ReRateReconciles runs the REAL SQL to pin the reconcile semantics:
// re-rate RECONCILES (deletes superseded rollups), it is NOT upsert-only. Run A bills a CLEAN single-base
// ft: rollup. Then a second, distinct base_model arrives for the SAME ft: id in the same
// window, making it ambiguous; run B excludes it from priced and must DELETE the stale
// rated_usage row in the SAME statement — never leave it billing at its run-A cost. A
// third, identical re-run must be a no-op (deletes nothing). This is the live-Postgres
// proof that the `deleted` CTE fires atomically with the upsert.
func TestIntegration_ReRateReconciles(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const sch = "phoebe_rating_reconcile_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{
			"cheap/base":     rate3("0.000001", "0", "0"),
			"expensive/base": rate3("0.000009", "0", "0"),
		},
		nil, PolicyMultiplier, MustDec("1.5"), Dec{},
	)
	store := NewPostgresStore(db)

	// Run A: a single-base ft: rollup (ft:dupe) PLUS a co-window survivor (ft:keep).
	// Both clean → billed. ft:keep must survive run B's reconcile untouched.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, model, base_model, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('rc-1','a','r','ft:dupe','cheap/base',1000,0,$1),
		        ('rc-keep','a','r','ft:keep','cheap/base',1000,0,$1)`, hour.Add(5*time.Minute)); err != nil {
		t.Fatalf("seed rc-1/rc-keep: %v", err)
	}
	resA, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("run A: %v", err)
	}
	if resA.RollupsWritten != 2 || resA.ReconciledDeletions != 0 {
		t.Fatalf("run A: rollups=%d deletions=%d, want 2/0", resA.RollupsWritten, resA.ReconciledDeletions)
	}
	var nA int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rated_usage WHERE model_id='ft:dupe'`).Scan(&nA); err != nil {
		t.Fatalf("count after A: %v", err)
	}
	if nA != 1 {
		t.Fatalf("after run A, ft:dupe rollups = %d, want 1 (clean rollup must bill)", nA)
	}

	// Mutate: a SECOND base_model for the SAME ft: id → ambiguous.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, model, base_model, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('rc-2','a','r','ft:dupe','expensive/base',1000,0,$1)`, hour.Add(10*time.Minute)); err != nil {
		t.Fatalf("seed rc-2: %v", err)
	}
	resB, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("run B: %v", err)
	}
	if resB.AmbiguousBaseEvents != 2 {
		t.Fatalf("run B: ambiguous = %d, want 2 (two-base ft:dupe)", resB.AmbiguousBaseEvents)
	}
	if resB.ReconciledDeletions != 1 {
		t.Fatalf("run B: reconciled deletions = %d, want 1 (the stale clean rollup must be deleted)", resB.ReconciledDeletions)
	}
	// THE INVARIANT: NO rated_usage row survives for the now-ambiguous ft:dupe.
	var nB int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rated_usage WHERE model_id='ft:dupe'`).Scan(&nB); err != nil {
		t.Fatalf("count after B: %v", err)
	}
	if nB != 0 {
		t.Fatalf("after run B, ft:dupe rollups = %d, want 0 — the stale rollup is STILL BILLING (upsert-only bug; reconcile must delete it)", nB)
	}
	// The co-window ft:keep rollup must SURVIVE run B (delete-set and upsert-set are
	// disjoint — the two modifying CTEs must not clobber a surviving co-window row).
	var nKeep int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rated_usage WHERE model_id='ft:keep'`).Scan(&nKeep); err != nil {
		t.Fatalf("count ft:keep after B: %v", err)
	}
	if nKeep != 1 {
		t.Fatalf("after run B, ft:keep rollups = %d, want 1 (a surviving co-window rollup must be UPDATED, not deleted)", nKeep)
	}

	// Run C: re-run with IDENTICAL data → no-op (deletes nothing; nothing to reconcile).
	resC, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("run C: %v", err)
	}
	if resC.ReconciledDeletions != 0 {
		t.Fatalf("run C (identical re-run): deletions = %d, want 0 (no spurious deletes)", resC.ReconciledDeletions)
	}
}

// TestIntegration_ReRateReconcileLeavesOtherWindowsUntouched proves the reconcile's
// window predicate is correct: a re-rate of ONE hour must DELETE only superseded rollups
// IN that hour, never touch a clean rollup in an adjacent hour. Guards the [start,end)
// half-open window_start predicate against deleting out-of-scope billing.
func TestIntegration_ReRateReconcileLeavesOtherWindowsUntouched(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const sch = "phoebe_rating_reconcile_win_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour10 := mustTime("2026-06-08T10:00:00Z")
	hour12 := mustTime("2026-06-08T12:00:00Z")
	book := newTestBook(
		map[string]Rate3{"b": rate3("0.000005", "0", "0")},
		nil, PolicyIdentity, Dec{}, Dec{},
	)
	store := NewPostgresStore(db)

	// Seed a clean rollup in BOTH the 10:00 and 12:00 hours; rate each window.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, model, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('w10','a','r','b',100,0,$1), ('w12','a','r','b',100,0,$2)`,
		hour10.Add(5*time.Minute), hour12.Add(5*time.Minute)); err != nil {
		t.Fatalf("seed windows: %v", err)
	}
	if _, err := store.RateWindow(ctx, book, hour10, hour10.Add(time.Hour)); err != nil {
		t.Fatalf("rate 10:00: %v", err)
	}
	if _, err := store.RateWindow(ctx, book, hour12, hour12.Add(time.Hour)); err != nil {
		t.Fatalf("rate 12:00: %v", err)
	}

	// Now DELETE the 10:00 event upstream so a re-rate of [10:00,11:00) supersedes its
	// rollup, and re-rate ONLY that window.
	if _, err := db.ExecContext(ctx, `DELETE FROM billing_event WHERE request_id='w10'`); err != nil {
		t.Fatalf("delete w10: %v", err)
	}
	resRe, err := store.RateWindow(ctx, book, hour10, hour10.Add(time.Hour))
	if err != nil {
		t.Fatalf("re-rate 10:00: %v", err)
	}
	if resRe.ReconciledDeletions != 1 {
		t.Fatalf("re-rate 10:00: deletions = %d, want 1 (the vanished 10:00 rollup)", resRe.ReconciledDeletions)
	}
	// The 12:00 rollup must be UNTOUCHED.
	var n12, n10 int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rated_usage WHERE window_start=$1`, hour12).Scan(&n12); err != nil {
		t.Fatalf("count 12:00: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rated_usage WHERE window_start=$1`, hour10).Scan(&n10); err != nil {
		t.Fatalf("count 10:00: %v", err)
	}
	if n10 != 0 {
		t.Fatalf("10:00 rollups = %d, want 0 (superseded)", n10)
	}
	if n12 != 1 {
		t.Fatalf("12:00 rollups = %d, want 1 (an adjacent-window rollup must NOT be deleted by a 10:00 re-rate)", n12)
	}
}

// TestIntegration_OneHopFineTuneCannotDeriveFromFineTune runs the REAL SQL to pin E3's
// one-hop rule (FIX 3): an own-rate fine-tune is NOT a derivation base. An ft: event
// whose base_model points at another own-rate ft: must NOT price (a second hop) — it
// lands UNPRICED, matching the oracle's ResolveEvent. Proves the SQL projection
// (rating_derived) excludes ft:-prefixed own-rate entries, so SQL and oracle agree.
func TestIntegration_OneHopFineTuneCannotDeriveFromFineTune(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const sch = "phoebe_rating_onehop_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")
	// A true base AND an own-rate fine-tune (ft:ownrate) priced directly in the file.
	book := newTestBook(
		map[string]Rate3{
			"meta-llama/Llama-3.1-8B-Instruct": rate3("0.000004", "0", "0"),
			"ft:ownrate":                       rate3("0.00001", "0", "0"),
		},
		nil, PolicyMultiplier, MustDec("1.5"), Dec{},
	)

	type seed struct {
		req, model, baseModel string
	}
	seeds := []seed{
		// One legitimate hop: ft: deriving from the TRUE base → priced.
		{"hop-ok", "ft:abc", "meta-llama/Llama-3.1-8B-Instruct"},
		// SECOND hop forbidden: ft: whose base_model is the own-rate fine-tune → UNPRICED.
		{"hop-bad", "ft:def", "ft:ownrate"},
	}
	for _, s := range seeds {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO billing_event (request_id, auth_id, resource_id, model, base_model, prompt_tokens, completion_tokens, event_ts)
			 VALUES ($1,'a','r',$2,$3,1000,0,$4)`,
			s.req, s.model, s.baseModel, hour.Add(5*time.Minute)); err != nil {
			t.Fatalf("seed %s: %v", s.req, err)
		}
	}

	store := NewPostgresStore(db)
	res, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}

	// hop-ok prices (one rollup); hop-bad is UNPRICED (the second hop is forbidden).
	if res.RollupsWritten != 1 || res.EventsRated != 1 {
		t.Fatalf("rollups/events = %d/%d, want 1/1 (only the one-hop ft: prices)", res.RollupsWritten, res.EventsRated)
	}
	if res.UnpricedEvents != 1 {
		t.Fatalf("unpriced = %d, want 1 (ft deriving from an own-rate ft: must fail loud — no second hop)", res.UnpricedEvents)
	}
	// Cross-check the oracle agrees: ResolveEvent fails for the second hop.
	if _, err := book.ResolveEvent("ft:def", "ft:ownrate"); err == nil {
		t.Fatal("oracle ResolveEvent priced a fine-tune-of-fine-tune — SQL and oracle must BOTH forbid the second hop")
	}
}

// TestIntegration_UTCBucketing_SessionTZIndependent: the hour bucket must NOT depend
// on the session TimeZone. A fractional-offset session (Asia/Kolkata, +05:30) must
// produce window_start on EXACT UTC hour boundaries, and a re-run from a UTC session
// must hit the SAME ON CONFLICT keys — no duplicate/overlapping rollups.
func TestIntegration_UTCBucketing_SessionTZIndependent(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1) // One connection so SET TIME ZONE / search_path stick.

	const sch = "phoebe_rating_tz_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{"b": rate3("0.000005", "0.0000005", "0.00002")},
		nil, PolicyIdentity, Dec{}, Dec{},
	)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, model, prompt_tokens, cached_tokens, completion_tokens, event_ts)
		 VALUES ('tz-req-0','a','r','b',100,0,50,$1)`, hour.Add(30*time.Minute)); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	store := NewPostgresStore(db)

	// Rate from a FRACTIONAL-OFFSET session.
	exec(t, db, "SET TIME ZONE 'Asia/Kolkata'")
	if _, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour)); err != nil {
		t.Fatalf("RateWindow (IST session): %v", err)
	}

	var ws time.Time
	if err := db.QueryRowContext(ctx,
		`SELECT window_start FROM rated_usage WHERE auth_id='a' AND model_id='b'`).Scan(&ws); err != nil {
		t.Fatalf("read window_start: %v", err)
	}
	if !ws.UTC().Equal(hour) {
		t.Fatalf("window_start = %s, want exact UTC hour boundary %s (session-TZ leaked into bucketing)",
			ws.UTC().Format(time.RFC3339), hour.Format(time.RFC3339))
	}

	idsIST := readRatedUsageIDs(t, db)
	exec(t, db, "SET TIME ZONE 'UTC'")
	if _, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour)); err != nil {
		t.Fatalf("RateWindow (UTC session): %v", err)
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rated_usage`).Scan(&n); err != nil {
		t.Fatalf("count rated_usage: %v", err)
	}
	if n != 1 {
		t.Fatalf("rated_usage has %d rows after IST-then-UTC re-rate, want 1 (duplicate buckets double-bill)", n)
	}
	idsUTC := readRatedUsageIDs(t, db)
	for k, id := range idsIST {
		if idsUTC[k] != id {
			t.Errorf("rollup %s id changed across sessions: %s → %s", k, id, idsUTC[k])
		}
	}
}

// TestIntegration_RatingInstantIndexServesScan: the expression index on
// COALESCE(event_ts, created_at) must actually serve the rater's window predicate.
func TestIntegration_RatingInstantIndexServesScan(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	const sch = "phoebe_rating_ix_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	exec(t, db, "SET enable_seqscan = off")
	plan := explainPlan(t, db, `EXPLAIN SELECT * FROM billing_event
		WHERE COALESCE(event_ts, created_at) >= '2026-06-08T10:00:00Z'
		  AND COALESCE(event_ts, created_at) <  '2026-06-08T11:00:00Z'`)
	if !strings.Contains(plan, "billing_event_rating_instant_ix") {
		t.Fatalf("plan does not use billing_event_rating_instant_ix:\n%s", plan)
	}
}

// TestIntegration_ReconcileDeleteCanUseWindowStartIndex pins ONE structural
// invariant: the reconcile DELETE's window_start-only predicate CAN be served by
// rated_usage_window_start_ix (rather than falling back to a seqscan or to the
// auth-leading composite index, neither of which can serve a window_start-only
// range as a tight slice — every other index leads with auth_id).
//
// This is an index-USABILITY sanity check, NOT a money assertion and NOT a
// planner-cost-preference or performance guarantee. Whether the planner PREFERS
// this index at default cost is a version- and GUC-fragile cost-model decision
// whose failure mode is a slow reconcile, not a wrong bill — so we deliberately do
// NOT gate on it. We force the planner's hand with `enable_seqscan=off` and assert
// only that the dedicated index CAN serve the predicate at all.
//
// The single hard assertion accepts ANY plan node that uses the index by NAME —
// Index Scan / Index Only Scan / Bitmap Index Scan — since all of those prove the
// invariant; matching only the `using …` forms would flake when Postgres renders a
// bitmap index scan ("Bitmap Index Scan on rated_usage_window_start_ix"). We
// EXPLAIN the ACTUAL reconcile DELETE from store.go (the `deleted` CTE's rated_usage
// table-access: the window_start range plus the NOT EXISTS anti-join against this
// run's priced rows) against an ANALYZE'd population so it is a real plan, not a
// vacuous SELECT against an empty table.
func TestIntegration_ReconcileDeleteCanUseWindowStartIndex(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	const sch = "phoebe_rating_winix_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	// POPULATE a small, ANALYZE'd population: 10 distinct auth_ids × 24 hours = 240
	// rated_usage rows across a couple dozen windows. That is enough for the
	// window_start range predicate to be a genuine, non-trivial index-served slice
	// (the reconcile targets a single hour → ~10 rows), so the EXPLAIN'd plan is real
	// rather than a degenerate seqscan-an-empty-table. We do NOT need 10k rows: we
	// only prove the index CAN serve the predicate under seqscan-off, not that the
	// planner prefers it at default cost, so a large population would back nothing.
	exec(t, db, `INSERT INTO rated_usage
		(id, auth_id, resource_id, model_id, window_start, window_end,
		 prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens,
		 cost, applied_prompt_rate, applied_cached_rate, applied_completion_rate, event_count)
		SELECT
		    md5(a::text || ':' || h::text),
		    'auth' || a::text,
		    'deploy' || a::text,
		    'm',
		    '2026-01-01T00:00:00Z'::timestamptz + (h || ' hours')::interval,
		    '2026-01-01T01:00:00Z'::timestamptz + (h || ' hours')::interval,
		    100, 0, 0, 100, 0.001, 0.00001, 0, 0, 1
		FROM generate_series(0, 9) AS a, generate_series(0, 23) AS h`)
	// ANALYZE so the planner has row-count + distribution stats rather than defaults,
	// making the EXPLAIN'd plan a real plan over the populated table.
	exec(t, db, "ANALYZE rated_usage")

	// EXPLAIN the ACTUAL reconcile DELETE statement (store.go's `deleted` CTE shape):
	// the rated_usage range on window_start, anti-joined against this run's priced
	// rows. `priced` is empty here (a re-run that reproduces nothing for this hour →
	// the whole slice is deleted), which is exactly the worst-case reconcile that
	// must still go through the index rather than seqscanning the whole table.
	const reconcileDelete = `EXPLAIN
		WITH priced AS (
		    SELECT auth_id, resource_id, model_id, window_start FROM rated_usage WHERE false
		)
		DELETE FROM rated_usage ru
		WHERE ru.window_start >= '2026-01-01T04:00:00Z'
		  AND ru.window_start <  '2026-01-01T05:00:00Z'
		  AND NOT EXISTS (
		      SELECT 1 FROM priced p
		      WHERE p.auth_id      = ru.auth_id
		        AND p.resource_id  = ru.resource_id
		        AND p.model_id     = ru.model_id
		        AND p.window_start = ru.window_start
		  )`

	// The ONE hard gate: with seqscan forbidden, the plan MUST reach rated_usage
	// through rated_usage_window_start_ix. That proves the dedicated index CAN serve
	// the window_start-only predicate at all (a remaining seqscan or a fall-through to
	// the auth-leading composite would mean no usable index for the reconcile's exact
	// predicate — a real defect). We accept any node that names the index — Index Scan,
	// Index Only Scan, or Bitmap Index Scan — because all three satisfy the invariant;
	// requiring the "using …" rendering alone would flake on the bitmap form.
	exec(t, db, "SET enable_seqscan = off")
	plan := explainPlan(t, db, reconcileDelete)
	if !strings.Contains(plan, "using rated_usage_window_start_ix") &&
		!strings.Contains(plan, "on rated_usage_window_start_ix") {
		t.Fatalf("reconcile DELETE predicate cannot be served by rated_usage_window_start_ix even with seqscan off (no usable window_start index for the window-only predicate):\n%s", plan)
	}
}

// explainPlan runs an EXPLAIN query and returns the joined plan text.
func explainPlan(t *testing.T, db *sql.DB, explainQuery string) string {
	t.Helper()
	rows, err := db.Query(explainQuery)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	var plan string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan: %v", err)
		}
		plan += line + "\n"
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return plan
}

func exec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func nullableStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
