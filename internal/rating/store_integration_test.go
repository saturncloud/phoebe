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
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/saturncloud/phoebe/internal/logging"
)

// ratingSchemaDDL returns the rating schema applied before each integration test
// (billing_event + rated_usage — prices are a YAML file, not DB tables). It is
// loaded from the REAL migration .sql files rather than a hand-copied constant, so
// the conformance test runs against EXACTLY the production schema and can never
// silently drift from it: a column-type or index-expression change in the
// migration that the test didn't track would otherwise pass green here and break
// in prod. Loading 0001 then 0002_rating reproduces the production apply order;
// base_model is declared in 0001 and re-added IF NOT EXISTS in 0002_rating, so the
// overlap is a harmless no-op. The DDL runs inside the per-test isolated schema
// (search_path is set by the caller), leaving no residue. io_log (0003_io_log) is
// intentionally not loaded — the rater never touches it.
func ratingSchemaDDL(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	for _, f := range []string{
		"../../migrations/0001_billing_event.up.sql",
		"../../migrations/0002_rating.up.sql",
		// 0004 adds billing_event.serving_mode, which rateWindowSQL reads (the
		// serving-mode SKU axis). Skipping it reproduces the staging 42703.
		"../../migrations/0004_billing_event_serving_mode.up.sql",
		"../../migrations/0005_invoice_grade_attempts.up.sql",
		// 0006 widens the rated_usage grain (serving_mode/owner_type/owner_id join
		// the natural key) and adds billing_event.graph_k8s_name, both of which
		// rateWindowSQL reads and writes.
		"../../migrations/0006_rollup_grain.up.sql",
	} {
		ddl, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read migration %s: %v (the integration test applies the REAL "+
				"migration DDL so it can't drift from production)", f, err)
		}
		b.Write(ddl)
		b.WriteString("\n")
	}
	// Existing fixtures predate usage_found and all represent authoritative usage
	// unless a test explicitly writes false. Keep their INSERTs readable while the
	// production migration's false default remains covered by migration/E2E tests.
	b.WriteString("ALTER TABLE billing_event ALTER COLUMN usage_found SET DEFAULT TRUE;\n")
	return b.String()
}

// schemaDDLReference is the previously hand-maintained schema, kept ONLY as a
// readable in-file description of what the loaded migrations produce. It is NOT
// applied (ratingSchemaDDL loads the real files); if you edit it, you are editing
// a comment. Drift between this and the migrations is now harmless.
const schemaDDLReference = `
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
    adapter           VARCHAR(255),
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
	exec(t, db, ratingSchemaDDL(t))

	hour := mustTime("2026-06-08T10:00:00Z")
	book := conformanceBook()

	// Events: priced aborted base, priced derived, unpriced, unattributable. The
	// aborted event proves disconnect never removes authoritative usage from
	// money. Each priced/unpriced event carries a resource_id (E2 grain); the
	// unattributable one has none.
	events := []RatedEvent{
		{AuthID: "a", ResourceID: "r", ModelID: "b", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, Aborted: true, At: hour.Add(5 * time.Minute)},
		{AuthID: "a", ResourceID: "r", ModelID: "f", PromptTokens: 100, CachedTokens: 0, CompletionTokens: 0, At: hour.Add(15 * time.Minute)},
		{AuthID: "a", ResourceID: "r", ModelID: "unpriced", PromptTokens: 9, At: hour.Add(1 * time.Minute)},
		{AuthID: "", ResourceID: "r", ModelID: "b", PromptTokens: 9, At: hour.Add(2 * time.Minute)},
	}
	for i, e := range events {
		_, err := db.ExecContext(ctx,
			`INSERT INTO billing_event (request_id, auth_id, resource_id, model, prompt_tokens, cached_tokens, completion_tokens, aborted, event_ts)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			fmt.Sprintf("req-%d", i), nullableStr(e.AuthID), nullableStr(e.ResourceID), nullableStr(e.ModelID),
			e.PromptTokens, e.CachedTokens, e.CompletionTokens, e.Aborted, e.At)
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
	// Full 6-bucket partition (ambiguous buckets are 0 in this fixture, but named so the
	// invariant holds by construction, not by coincidence).
	if got := res.EventsRated + res.UnpricedEvents + res.UnattributableEvents +
		res.AmbiguousBaseEvents + res.AmbiguousOrgEvents + res.OwnerConflictEvents; got != int64(len(events)) {
		t.Fatalf("rated+unpriced+unattr+ambiguous_base+ambiguous_org+owner_conflict = %d, want %d", got, len(events))
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
	exec(t, db, ratingSchemaDDL(t))

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
		res.AmbiguousBaseEvents + res.AmbiguousOrgEvents + res.OwnerConflictEvents; got != 3 {
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
	exec(t, db, ratingSchemaDDL(t))

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
		res.AmbiguousBaseEvents + res.AmbiguousOrgEvents + res.OwnerConflictEvents; got != 5 {
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
	exec(t, db, ratingSchemaDDL(t))

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
		res.AmbiguousBaseEvents + res.AmbiguousOrgEvents + res.OwnerConflictEvents; got != 2 {
		t.Fatalf("partition sum = %d, want 2 (the rollup must be counted exactly once, not double)", got)
	}
	// And nothing was billed (the rollup is withheld).
	if res.RollupsWritten != 0 {
		t.Fatalf("RollupsWritten = %d, want 0 (a both-ambiguous rollup is never billed)", res.RollupsWritten)
	}
}

// TestIntegration_CleanThenAmbiguousOrgReconciles pins the re-rate path where a rollup
// billed CLEAN in run A becomes org-ambiguous in run B (a second deployment-org appeared
// for the same resource in-window). Run B must: (a) reconcile-DELETE the prior clean row
// (it can no longer be billed to a single org) AND (b) count it as ambiguous_org +
// exit-nonzero. Both signals fire — the prior bill is removed and the conflict screams.
// This documents the current "both fire" alert contract (see the open question on whether
// a reconcile-delete should suppress the anomaly exit).
func TestIntegration_CleanThenAmbiguousOrgReconciles(t *testing.T) {
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

	const sch = "phoebe_rating_clean2ambig_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, ratingSchemaDDL(t))

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{"b": rate3("0.000005", "0", "0")},
		nil, PolicyIdentity, Dec{}, Dec{},
	)
	store := NewPostgresStore(db)

	// Run A: one clean single-org rollup → billed.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('c1','a','d1','org-1','b',100,0,$1)`, hour.Add(5*time.Minute)); err != nil {
		t.Fatalf("seed A: %v", err)
	}
	resA, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow A: %v", err)
	}
	if resA.RollupsWritten != 1 || resA.AmbiguousOrgEvents != 0 {
		t.Fatalf("run A: rollups=%d ambiguous_org=%d, want 1/0 (clean)", resA.RollupsWritten, resA.AmbiguousOrgEvents)
	}

	// Run B: a SECOND org appears for the same resource in the same hour → ambiguous.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('c2','a','d1','org-2','b',100,0,$1)`, hour.Add(6*time.Minute)); err != nil {
		t.Fatalf("seed B: %v", err)
	}
	resB, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow B: %v", err)
	}
	// The prior clean row is reconcile-DELETED (no longer billable to one org)...
	if resB.ReconciledDeletions != 1 {
		t.Fatalf("run B: reconciled deletions = %d, want 1 (the prior clean rollup must be removed)", resB.ReconciledDeletions)
	}
	// ...AND the conflict is counted + screams (both signals fire — current contract).
	if resB.AmbiguousOrgEvents != 2 {
		t.Fatalf("run B: ambiguous_org = %d, want 2 (the two conflicting-org events)", resB.AmbiguousOrgEvents)
	}
	// Nothing remains billed for the now-ambiguous rollup.
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rated_usage WHERE resource_id='d1'`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("rated_usage still has %d rows for the now-ambiguous rollup — it must be un-billed", n)
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
	exec(t, db, ratingSchemaDDL(t))

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

	// Reconciliation must use the same org-independent grain as the rater. The
	// rollout-era NULL and real org are one accurately rated rollup, while the
	// missing header remains visible as evidence rather than a false raw-only row.
	var viewRows, rawAttempts, ratedAttempts, missingOrg, distinctOrgs int64
	var attemptDelta, promptDelta, freshDelta, cachedDelta, completionDelta int64
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), MAX(raw_attempts), MAX(rated_attempts),
		       MAX(missing_org_attempts), MAX(distinct_org_ids),
		       MAX(attempt_delta), MAX(prompt_token_delta),
		       MAX(fresh_input_token_delta), MAX(cached_token_delta),
		       MAX(completion_token_delta)
		FROM billing_reconciliation_hourly
		WHERE window_start=$1 AND auth_id='a' AND resource_id='d1' AND model_id='b'`, hour).
		Scan(&viewRows, &rawAttempts, &ratedAttempts, &missingOrg, &distinctOrgs,
			&attemptDelta, &promptDelta, &freshDelta, &cachedDelta, &completionDelta); err != nil {
		t.Fatalf("read reconciliation view: %v", err)
	}
	if viewRows != 1 || rawAttempts != 2 || ratedAttempts != 2 || missingOrg != 1 || distinctOrgs != 1 {
		t.Fatalf("reconciliation grain = rows/raw/rated/missing-org/distinct-orgs %d/%d/%d/%d/%d, want 1/2/2/1/1",
			viewRows, rawAttempts, ratedAttempts, missingOrg, distinctOrgs)
	}
	if attemptDelta != 0 || promptDelta != 0 || freshDelta != 0 || cachedDelta != 0 || completionDelta != 0 {
		t.Fatalf("reconciliation deltas = attempts/prompt/fresh/cached/completion %d/%d/%d/%d/%d, want all zero",
			attemptDelta, promptDelta, freshDelta, cachedDelta, completionDelta)
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

// TestIntegration_ReRatePreservesHistoricalPrice proves the invoice-grade price
// one-way door: changing the current YAML book after an hour was first rated may
// incorporate late events, but every token in that existing rollup continues to
// use the originally applied rates.
func TestIntegration_ReRatePreservesHistoricalPrice(t *testing.T) {
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

	const sch = "phoebe_rating_hourly_book_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, ratingSchemaDDL(t))

	hour := mustTime("2026-06-08T10:00:00Z")
	store := NewPostgresStore(db)
	// The prices EFFECTIVE DURING `hour`. Re-rating that hour always resolves these,
	// because the caller asks the manager for the book effective during the hour it
	// is rating — not for today's book. newBook is what the price list says LATER;
	// it must never touch this hour, and the mechanism that guarantees that is the
	// per-hour lookup, not a local freeze table (which no longer exists).
	oldBook := newTestBook(map[string]Rate3{"b": rate3("0.000001", "0", "0")}, nil, PolicyIdentity, Dec{}, Dec{})
	newBook := newTestBook(map[string]Rate3{"b": rate3("0.000009", "0", "0")}, nil, PolicyIdentity, Dec{}, Dec{})

	// bookForHour models the manager's effective-dated series: this hour always
	// resolves to the rates in force during it.
	bookForHour := func(_ context.Context, hourStart time.Time) (*PriceBook, error) {
		if hourStart.UTC().Equal(hour) {
			return oldBook, nil
		}
		return newBook, nil
	}

	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, prompt_tokens, event_ts)
		 VALUES ('p1','a','d1','org-1','b',100,$1)`, hour.Add(5*time.Minute)); err != nil {
		t.Fatalf("seed first event: %v", err)
	}
	if _, err := store.RateWindow(ctx, oldBook, hour, hour.Add(time.Hour)); err != nil {
		t.Fatalf("initial rate: %v", err)
	}

	// Price changes, then a delayed event from the already-rated hour arrives.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, prompt_tokens, event_ts)
		 VALUES ('p2','a','d1','org-1','b',100,$1)`, hour.Add(6*time.Minute)); err != nil {
		t.Fatalf("seed late event: %v", err)
	}
	// Re-rate AFTER the price list changed. The rater asks for this hour's prices,
	// so the late event bills at the hour's own rate — today's higher rate never
	// reaches it.
	rater := New(store, nil, logging.New(logging.ERROR)).WithBookForHour(bookForHour)
	if _, err := rater.RunWindow(ctx, hour, hour.Add(time.Hour), true); err != nil {
		t.Fatalf("re-rate after a price change: %v", err)
	}

	var tokens int64
	var rate, cost string
	if err := db.QueryRowContext(ctx,
		`SELECT prompt_tokens, applied_prompt_rate::text, cost::text
		 FROM rated_usage WHERE auth_id='a' AND resource_id='d1' AND model_id='b' AND window_start=$1`,
		hour).Scan(&tokens, &rate, &cost); err != nil {
		t.Fatalf("read frozen rollup: %v", err)
	}
	if tokens != 200 || MustDec(rate).String() != "0.000001000" || MustDec(cost).String() != "0.000200000" {
		t.Fatalf("frozen rollup tokens/rate/cost = %d/%s/%s, want 200/0.000001000/0.000200000", tokens, rate, cost)
	}

	// Even a reconcile deletion must not erase the historical price decision.
	if _, err := db.ExecContext(ctx, `DELETE FROM billing_event WHERE request_id IN ('p1','p2')`); err != nil {
		t.Fatalf("remove raw events: %v", err)
	}
	if res, err := rater.RunWindow(ctx, hour, hour.Add(time.Hour), true); err != nil {
		t.Fatalf("reconcile delete: %v", err)
	} else if res.ReconciledDeletions != 1 {
		t.Fatalf("reconcile deletions = %d, want 1", res.ReconciledDeletions)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, prompt_tokens, event_ts)
		 VALUES ('p3','a','d1','org-1','b',100,$1)`, hour.Add(7*time.Minute)); err != nil {
		t.Fatalf("seed recovered event: %v", err)
	}
	// Recreate the rollup after the reconcile delete. Without a local price freeze,
	// the hour STILL prices at its own rates — the deleted-and-recreated rollup
	// cannot pick up the newer rate, because the price is a function of the hour.
	if _, err := rater.RunWindow(ctx, hour, hour.Add(time.Hour), true); err != nil {
		t.Fatalf("recreate after reconcile delete: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT prompt_tokens, applied_prompt_rate::text, cost::text
		 FROM rated_usage WHERE auth_id='a' AND resource_id='d1' AND model_id='b' AND window_start=$1`,
		hour).Scan(&tokens, &rate, &cost); err != nil {
		t.Fatalf("read recreated frozen rollup: %v", err)
	}
	if tokens != 100 || MustDec(rate).String() != "0.000001000" || MustDec(cost).String() != "0.000100000" {
		t.Fatalf("recreated rollup tokens/rate/cost = %d/%s/%s, want 100/0.000001000/0.000100000", tokens, rate, cost)
	}
}

// TestIntegration_MissingUsageAttemptIsNeverBilled: a failed attempt with no engine
// usage is retained for audit, but must never become money — it is counted in the
// missing-usage partition, not as unattributable, and writes no rollup.
func TestIntegration_MissingUsageAttemptIsNeverBilled(t *testing.T) {
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

	const sch = "phoebe_rating_missingusage_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, ratingSchemaDDL(t))

	hour := mustTime("2026-06-08T10:00:00Z")
	store := NewPostgresStore(db)
	book := newTestBook(map[string]Rate3{"b": rate3("0.000009", "0", "0")}, nil, PolicyIdentity, Dec{}, Dec{})

	// A failed attempt with no engine usage is retained for audit, but is neither
	// billed as a zero-token rollup nor mislabeled as unattributable when model is
	// unavailable because no upstream response arrived.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event
		 (request_id, auth_id, resource_id, org_id, model, usage_found, status_code, event_ts)
		 VALUES ('failed-no-usage','a','d2','org-1',NULL,FALSE,502,$1)`, hour.Add(8*time.Minute)); err != nil {
		t.Fatalf("seed missing-usage attempt: %v", err)
	}
	missingRes, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("rate missing-usage attempt: %v", err)
	}
	// The anomaly counts strictly PARTITION the window's events (see store.go:
	// events_rated + missing_usage + ... == total in-window events), and
	// events_rated sums event_count over the UPSERTED rollups. A missing-usage
	// attempt writes no rollup, so it is counted ONCE, as missing usage, and
	// rated is 0. Expecting rated=1 here would double-count the same event in two
	// buckets and contradict the "no rollups written" assertion just below.
	if missingRes.MissingUsageEvents != 1 || missingRes.UnattributableEvents != 0 || missingRes.EventsRated != 0 {
		t.Fatalf("missing-usage partition = missing %d / unattributable %d / rated %d, want 1/0/0",
			missingRes.MissingUsageEvents, missingRes.UnattributableEvents, missingRes.EventsRated)
	}
	var failedRollups int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rated_usage WHERE resource_id='d2'`).Scan(&failedRollups); err != nil {
		t.Fatalf("count failed-attempt rollups: %v", err)
	}
	if failedRollups != 0 {
		t.Fatalf("failed-attempt rollups = %d, want 0 (missing usage must not become money)", failedRollups)
	}
}

// TestIntegration_InvalidUsageEvidenceNeverEntersMoney proves malformed engine
// evidence remains insertable and queryable after the invoice-grade migration,
// while the rater partitions it into InvalidUsageEvents and writes no money.
func TestIntegration_InvalidUsageEvidenceNeverEntersMoney(t *testing.T) {
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

	const sch = "phoebe_rating_invalid_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()

	apply := func(name string) {
		t.Helper()
		ddl, readErr := os.ReadFile("../../migrations/" + name)
		if readErr != nil {
			t.Fatalf("read migration %s: %v", name, readErr)
		}
		exec(t, db, string(ddl))
	}
	for _, name := range []string{
		"0001_billing_event.up.sql",
		"0002_rating.up.sql",
		"0004_billing_event_serving_mode.up.sql",
		"0005_invoice_grade_attempts.up.sql",
		"0006_rollup_grain.up.sql",
	} {
		apply(name)
	}

	hour := mustTime("2026-06-08T10:00:00Z")
	// This newly received authoritative row deliberately violates cached <=
	// prompt. The raw ledger must retain it rather than rejecting the attempt.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event
		 (request_id, auth_id, resource_id, org_id, model, prompt_tokens, cached_tokens, completion_tokens, usage_found, event_ts)
		 VALUES ('engine-invalid','a','d1','org-1','b',10,40,0,TRUE,$1)`, hour.Add(5*time.Minute)); err != nil {
		t.Fatalf("insert invalid engine evidence: %v", err)
	}

	book := newTestBook(map[string]Rate3{"b": rate3("0.000005", "0.000001", "0")}, nil, PolicyIdentity, Dec{}, Dec{})
	res, err := NewPostgresStore(db).RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}
	if res.InvalidUsageEvents != 1 || res.EventsRated != 0 || res.RollupsWritten != 0 {
		t.Fatalf("invalid result = invalid/rated/rollups %d/%d/%d, want 1/0/0",
			res.InvalidUsageEvents, res.EventsRated, res.RollupsWritten)
	}
	var rawRows, invalidAttempts, ratedRows int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM billing_event`).Scan(&rawRows); err != nil {
		t.Fatalf("count raw evidence: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(SUM(invalid_usage_attempts),0) FROM billing_reconciliation_hourly`).Scan(&invalidAttempts); err != nil {
		t.Fatalf("read invalid reconciliation evidence: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rated_usage`).Scan(&ratedRows); err != nil {
		t.Fatalf("count rated rows: %v", err)
	}
	if rawRows != 1 || invalidAttempts != 1 || ratedRows != 0 {
		t.Fatalf("invalid persistence = raw/invalid/rated %d/%d/%d, want 1/1/0", rawRows, invalidAttempts, ratedRows)
	}
}

// readRatedUsageIDs returns natural-key → id for every rated_usage row.
func readRatedUsageIDs(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.Query(`SELECT auth_id || '|' || owner_type || '|' || owner_id || '|' || resource_id || '|' || model_id || '|' || serving_mode || '|' || extract(epoch FROM window_start)::bigint::text, id FROM rated_usage`)
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
	exec(t, db, ratingSchemaDDL(t))

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
	exec(t, db, ratingSchemaDDL(t))

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
	exec(t, db, ratingSchemaDDL(t))

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
	rate, err := book.ResolveEvent("ft:9f8e7d6c5b4a", "meta-llama/Llama-3.1-8B-Instruct", "", "")
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
	exec(t, db, ratingSchemaDDL(t))

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
	exec(t, db, ratingSchemaDDL(t))

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
	exec(t, db, ratingSchemaDDL(t))

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
	exec(t, db, ratingSchemaDDL(t))

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
	if _, err := book.ResolveEvent("ft:def", "ft:ownrate", "", ""); err == nil {
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
	exec(t, db, ratingSchemaDDL(t))

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
	exec(t, db, ratingSchemaDDL(t))

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
	exec(t, db, ratingSchemaDDL(t))

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

// TestIntegration_C4ResolutionLadderConformsToOracle runs the REAL rating SQL over
// one fixture per rung of the C4 resolution ladder and pins it to the Go oracle
// (ResolveEvent → Quantized → Rate) case by case. The contract under test: vLLM
// serves under the ENDPOINT NAME, X-Saturn-Base-Model (billing_event.base_model) is
// the catalog price key, and X-Saturn-Adapter (billing_event.adapter) presence is
// the fine-tune premium trigger. Precedence a > b > c > d:
//
//	base-endpoint-no-premium            (c) base_model set, no adapter, non-ft name → plain base rate
//	adapter-triggers-premium            (b) endpoint name + adapter + base_model → base x premium
//	ft-prefix-still-premium             (b) ft: model_id + base_model → base x premium (unchanged)
//	direct-entry-wins-over-derivation   (a) model_id in the file + base_model + adapter → the direct rate
//	adapter-with-empty-base-model-unpriced (d) adapter, NULL base_model → UNPRICED (fail closed)
//	plain-unknown-model-unpriced        (d) nothing resolves → UNPRICED (unchanged)
func TestIntegration_C4ResolutionLadderConformsToOracle(t *testing.T) {
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

	const sch = "phoebe_rating_c4_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, ratingSchemaDDL(t))

	hour := mustTime("2026-06-08T10:00:00Z")
	// One priced base (the catalog key), one direct per-endpoint override entry,
	// 1.5x premium. Endpoint names are deliberately NOT file keys.
	book := newTestBook(
		map[string]Rate3{
			"meta-llama/Llama-3.1-8B-Instruct": rate3("0.000004", "0.0000004", "0.00001"),
			"tf-ep-override":                   rate3("0.000099", "0", "0"),
		},
		nil, PolicyMultiplier, MustDec("1.5"), Dec{},
	)

	const base = "meta-llama/Llama-3.1-8B-Instruct"
	cases := []struct {
		name              string
		model, baseModel  string
		adapter           string
		prompt            int64
		priced            bool
		wantAppliedPrompt string // the 9dp rate that must be frozen on the row
	}{
		{"base-endpoint-no-premium", "tf-ep-base", base, "", 100, true, "0.000004000"},
		{"adapter-triggers-premium", "tf-ep-ft", base, "ckpt-artifact-1", 200, true, "0.000006000"},
		{"ft-prefix-still-premium", "ft:9f8e7d6c5b4a", base, "", 300, true, "0.000006000"},
		{"direct-entry-wins-over-derivation", "tf-ep-override", base, "ckpt-artifact-2", 400, true, "0.000099000"},
		{"adapter-with-empty-base-model-unpriced", "tf-ep-orphan", "", "ckpt-artifact-3", 500, false, ""},
		{"plain-unknown-model-unpriced", "tf-ep-unknown", "", "", 600, false, ""},
	}
	for i, c := range cases {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO billing_event (request_id, auth_id, resource_id, model, base_model, adapter, prompt_tokens, completion_tokens, event_ts)
			 VALUES ($1,'a','r',$2,$3,$4,$5,0,$6)`,
			fmt.Sprintf("c4-req-%d", i), c.model, nullableStr(c.baseModel), nullableStr(c.adapter),
			c.prompt, hour.Add(5*time.Minute)); err != nil {
			t.Fatalf("seed %s: %v", c.name, err)
		}
	}

	store := NewPostgresStore(db)
	res, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}
	if res.RollupsWritten != 4 || res.EventsRated != 4 {
		t.Fatalf("rollups/events = %d/%d, want 4/4 (the four priced rungs)", res.RollupsWritten, res.EventsRated)
	}
	if res.UnpricedEvents != 2 {
		t.Fatalf("unpriced = %d, want 2 (the two fail-closed rungs must scream, never $0)", res.UnpricedEvents)
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rate, oerr := book.ResolveEvent(c.model, c.baseModel, c.adapter, "")
			if !c.priced {
				// Oracle agrees it is unpriced, and the SQL wrote NO rollup for it.
				if oerr == nil {
					t.Fatalf("oracle priced %s — SQL and oracle must BOTH refuse", c.name)
				}
				var n int
				if err := db.QueryRowContext(ctx,
					`SELECT COUNT(*) FROM rated_usage WHERE model_id=$1`, c.model).Scan(&n); err != nil {
					t.Fatalf("count: %v", err)
				}
				if n != 0 {
					t.Fatalf("rated_usage has %d rows for %s — an unpriced event must never be billed", n, c.model)
				}
				return
			}
			if oerr != nil {
				t.Fatalf("oracle ResolveEvent(%s): %v", c.name, oerr)
			}
			billed := rate.Quantized()
			wantCost := Rate(RatedEvent{PromptTokens: c.prompt}, billed).String()
			var gotCost, gotApplied string
			if err := db.QueryRowContext(ctx,
				`SELECT cost::text, applied_prompt_rate::text FROM rated_usage WHERE model_id=$1 AND window_start=$2`,
				c.model, hour).Scan(&gotCost, &gotApplied); err != nil {
				t.Fatalf("read rated_usage (%s): %v", c.model, err)
			}
			if MustDec(gotCost).String() != wantCost {
				t.Errorf("SQL cost = %s, oracle = %s (SQL and Go must agree on the C4 ladder)", gotCost, wantCost)
			}
			if MustDec(gotApplied).String() != c.wantAppliedPrompt {
				t.Errorf("applied_prompt_rate = %s, want %s (the ladder rung's rate frozen on the row)", gotApplied, c.wantAppliedPrompt)
			}
			if MustDec(gotApplied).String() != billed.Prompt.String() {
				t.Errorf("applied_prompt_rate = %s != oracle quantized rate %s", gotApplied, billed.Prompt)
			}
		})
	}
}

// TestIntegration_C4AmbiguityFailsLoud runs the REAL SQL over the two NEW single-rate
// violations C4's plain-base path makes possible, proving the extended gate splits
// them out (counted ambiguous, never MIN-billed) while a clean endpoint still rates:
//
//   - an endpoint-name model_id resolving through TWO distinct base_models on the
//     PLAIN-BASE path (an endpoint name reused over a different base in one window);
//   - ONE endpoint name whose adapter FLAPS (some events premium-derived, some
//     plain-base) — two rates even on a single base_model.
func TestIntegration_C4AmbiguityFailsLoud(t *testing.T) {
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

	const sch = "phoebe_rating_c4ambig_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, ratingSchemaDDL(t))

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{
			"cheap/base":     rate3("0.000001", "0", "0"),
			"expensive/base": rate3("0.000009", "0", "0"),
		},
		nil, PolicyMultiplier, MustDec("1.5"), Dec{},
	)

	type seed struct {
		req, model, baseModel, adapter string
	}
	seeds := []seed{
		// Two distinct bases on the PLAIN-BASE path for one endpoint name → ambiguous.
		{"pb-1", "tf-ep-reused", "cheap/base", ""},
		{"pb-2", "tf-ep-reused", "expensive/base", ""},
		// Adapter FLAP on one endpoint name (same base): premium vs plain → ambiguous.
		{"flap-1", "tf-ep-flap", "cheap/base", "ckpt-artifact-9"},
		{"flap-2", "tf-ep-flap", "cheap/base", ""},
		// A clean single-base, no-adapter endpoint that MUST still rate (plain rate).
		{"clean", "tf-ep-clean", "cheap/base", ""},
	}
	for _, s := range seeds {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO billing_event (request_id, auth_id, resource_id, model, base_model, adapter, prompt_tokens, completion_tokens, event_ts)
			 VALUES ($1,'a','r',$2,$3,$4,1000,0,$5)`,
			s.req, s.model, s.baseModel, nullableStr(s.adapter), hour.Add(5*time.Minute)); err != nil {
			t.Fatalf("seed %s: %v", s.req, err)
		}
	}

	store := NewPostgresStore(db)
	res, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}
	if res.RollupsWritten != 1 || res.EventsRated != 1 {
		t.Fatalf("rollups/events = %d/%d, want 1/1 (only tf-ep-clean bills)", res.RollupsWritten, res.EventsRated)
	}
	if res.AmbiguousBaseEvents != 4 {
		t.Fatalf("ambiguous = %d, want 4 (two-base reuse + adapter flap must scream, never MIN-bill)", res.AmbiguousBaseEvents)
	}
	var nAmbig int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM rated_usage WHERE model_id IN ('tf-ep-reused','tf-ep-flap')`).Scan(&nAmbig); err != nil {
		t.Fatalf("count ambiguous rollups: %v", err)
	}
	if nAmbig != 0 {
		t.Fatalf("rated_usage has %d rows for ambiguous endpoints — they must NOT be billed", nAmbig)
	}
	// The clean endpoint billed at its PLAIN base rate: 1000 x 0.000001 = 0.001.
	var cost string
	if err := db.QueryRowContext(ctx,
		`SELECT cost::text FROM rated_usage WHERE model_id='tf-ep-clean'`).Scan(&cost); err != nil {
		t.Fatalf("read tf-ep-clean rollup: %v", err)
	}
	if MustDec(cost).String() != "0.001000000" {
		t.Errorf("tf-ep-clean cost = %s, want 0.001000000 (plain base rate, no premium)", cost)
	}
}

// TestIntegration_FreshInputTokensSurvivesInt32Overflow pins the generated
// fresh_input_tokens column against int32 overflow of its own subtraction.
//
// prompt_tokens and cached_tokens are INTEGER, so each value below is
// individually valid engine evidence, but `prompt_tokens - cached_tokens`
// exceeds int32 range. Declared as INTEGER over unwidened operands, PostgreSQL
// raises 22003 and rejects the INSERT — destroying the raw invalid evidence the
// ledger exists to retain, and turning an engine bug into lost billing
// forensics. Declared BIGINT over explicitly cast BIGINT operands, the row
// persists, reconciliation counts it as an invalid-usage attempt, and it still
// never reaches money.
func TestIntegration_FreshInputTokensSurvivesInt32Overflow(t *testing.T) {
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

	const sch = "phoebe_rating_overflow_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()

	for _, name := range []string{
		"0001_billing_event.up.sql",
		"0002_rating.up.sql",
		"0004_billing_event_serving_mode.up.sql",
		"0005_invoice_grade_attempts.up.sql",
		"0006_rollup_grain.up.sql",
	} {
		ddl, readErr := os.ReadFile("../../migrations/" + name)
		if readErr != nil {
			t.Fatalf("read migration %s: %v", name, readErr)
		}
		exec(t, db, string(ddl))
	}

	// The generated column must be wide enough to hold the difference.
	var dataType string
	if err := db.QueryRowContext(ctx,
		`SELECT data_type FROM information_schema.columns
		 WHERE table_schema = $1 AND table_name = 'billing_event'
		   AND column_name = 'fresh_input_tokens'`, sch).Scan(&dataType); err != nil {
		t.Fatalf("read fresh_input_tokens type: %v", err)
	}
	if dataType != "bigint" {
		t.Fatalf("fresh_input_tokens is %s, want bigint: an INTEGER generated column "+
			"overflows on valid int32 operands and rejects raw evidence", dataType)
	}

	hour := mustTime("2026-06-08T10:00:00Z")
	const (
		maxInt32 = 2147483647
		minInt32 = -2147483648
	)
	// Each operand is a valid INTEGER; both differences exceed int32 range.
	cases := []struct {
		requestID string
		prompt    int64
		cached    int64
		wantFresh int64
	}{
		{"engine-overflow-positive", maxInt32, minInt32, int64(maxInt32) - int64(minInt32)},
		{"engine-overflow-negative", minInt32, maxInt32, int64(minInt32) - int64(maxInt32)},
	}
	for _, tc := range cases {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO billing_event
			 (request_id, auth_id, resource_id, org_id, model, prompt_tokens, cached_tokens, completion_tokens, usage_found, event_ts)
			 VALUES ($1,'a','d1','org-1','b',$2,$3,0,TRUE,$4)`,
			tc.requestID, tc.prompt, tc.cached, hour.Add(5*time.Minute)); err != nil {
			t.Fatalf("insert %s: %v (raw invalid evidence must remain persistable)", tc.requestID, err)
		}
		var fresh int64
		if err := db.QueryRowContext(ctx,
			`SELECT fresh_input_tokens FROM billing_event WHERE request_id = $1`, tc.requestID).Scan(&fresh); err != nil {
			t.Fatalf("read fresh_input_tokens for %s: %v", tc.requestID, err)
		}
		if fresh != tc.wantFresh {
			t.Fatalf("%s fresh_input_tokens = %d, want %d", tc.requestID, fresh, tc.wantFresh)
		}
	}

	// Both rows are invalid evidence (cached > prompt, or negative counts), so
	// they are reported for repair and excluded from money.
	book := newTestBook(map[string]Rate3{"b": rate3("0.000005", "0.000001", "0")}, nil, PolicyIdentity, Dec{}, Dec{})
	res, err := NewPostgresStore(db).RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}
	if res.InvalidUsageEvents != 2 || res.EventsRated != 0 || res.RollupsWritten != 0 {
		t.Fatalf("overflow result = invalid/rated/rollups %d/%d/%d, want 2/0/0",
			res.InvalidUsageEvents, res.EventsRated, res.RollupsWritten)
	}
	var rawRows, invalidAttempts, ratedRows int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM billing_event`).Scan(&rawRows); err != nil {
		t.Fatalf("count raw evidence: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(invalid_usage_attempts),0) FROM billing_reconciliation_hourly`).Scan(&invalidAttempts); err != nil {
		t.Fatalf("read invalid reconciliation evidence: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rated_usage`).Scan(&ratedRows); err != nil {
		t.Fatalf("count rated rows: %v", err)
	}
	if rawRows != 2 || invalidAttempts != 2 || ratedRows != 0 {
		t.Fatalf("overflow persistence = raw/invalid/rated %d/%d/%d, want 2/2/0", rawRows, invalidAttempts, ratedRows)
	}
}

// TestIntegration_MissingUsagePartitionedByCause proves the paging partition
// against live Postgres: routine zero-usage attempts (client abort, upstream
// failure) are counted as EXPECTED and must not page, while a SUCCESSFUL response
// carrying no usage block is counted as UNEXPLAINED and must page. Ratified with
// Hugo 2026-09-21 — paging on the cause-blind total fired hourly on any install
// with real traffic and buried the rare anomalies sharing the exit-2 channel.
func TestIntegration_MissingUsagePartitionedByCause(t *testing.T) {
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

	const sch = "phoebe_rating_missing_cause_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	for _, name := range []string{
		"0001_billing_event.up.sql",
		"0002_rating.up.sql",
		"0004_billing_event_serving_mode.up.sql",
		"0005_invoice_grade_attempts.up.sql",
		"0006_rollup_grain.up.sql",
	} {
		ddl, readErr := os.ReadFile("../../migrations/" + name)
		if readErr != nil {
			t.Fatalf("read migration %s: %v", name, readErr)
		}
		exec(t, db, string(ddl))
	}

	hour := mustTime("2026-06-08T10:00:00Z")
	insert := func(id string, aborted bool, status any) {
		t.Helper()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO billing_event
			 (request_id, auth_id, resource_id, org_id, model, prompt_tokens, cached_tokens,
			  completion_tokens, usage_found, aborted, status_code, event_ts)
			 VALUES ($1,'a','d1','org-1','b',0,0,0,FALSE,$2,$3,$4)`,
			id, aborted, status, hour.Add(5*time.Minute)); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}
	// Routine: a client disconnect (499) and an upstream failure (502).
	insert("abort-499", true, 499)
	insert("upstream-502", false, 502)
	// Alarming: the engine returned 200 and reported no tokens.
	insert("success-no-usage", false, 200)

	book := newTestBook(map[string]Rate3{"b": rate3("0.000005", "0.000001", "0")}, nil, PolicyIdentity, Dec{}, Dec{})
	res, err := NewPostgresStore(db).RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}
	if res.MissingUsageEvents != 3 {
		t.Fatalf("missing-usage total = %d, want 3", res.MissingUsageEvents)
	}
	if res.ExpectedMissingUsageEvents != 2 {
		t.Fatalf("expected(routine) missing-usage = %d, want 2 (the 499 abort and the 502)",
			res.ExpectedMissingUsageEvents)
	}
	if res.UnexplainedMissingUsageEvents != 1 {
		t.Fatalf("unexplained missing-usage = %d, want 1 (the 200 with no usage block)",
			res.UnexplainedMissingUsageEvents)
	}
	if res.ExpectedMissingUsageEvents+res.UnexplainedMissingUsageEvents != res.MissingUsageEvents {
		t.Fatalf("causes %d+%d do not partition the total %d",
			res.ExpectedMissingUsageEvents, res.UnexplainedMissingUsageEvents, res.MissingUsageEvents)
	}
	// None of them is money.
	if res.EventsRated != 0 || res.RollupsWritten != 0 {
		t.Fatalf("rated/rollups = %d/%d, want 0/0: zero-usage attempts never become money",
			res.EventsRated, res.RollupsWritten)
	}

	// A window of ONLY routine attempts must not page.
	if _, err := db.ExecContext(ctx, `DELETE FROM billing_event WHERE request_id = 'success-no-usage'`); err != nil {
		t.Fatalf("delete unexplained row: %v", err)
	}
	routine, err := NewPostgresStore(db).RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow (routine only): %v", err)
	}
	if routine.UnexplainedMissingUsageEvents != 0 {
		t.Fatalf("unexplained = %d on a routine-only window, want 0", routine.UnexplainedMissingUsageEvents)
	}
	rr := Result{
		MissingUsageEvents:            routine.MissingUsageEvents,
		ExpectedMissingUsageEvents:    routine.ExpectedMissingUsageEvents,
		UnexplainedMissingUsageEvents: routine.UnexplainedMissingUsageEvents,
	}
	if rr.HasAnomaly() {
		t.Fatal("a window of only client aborts and upstream failures must NOT page")
	}
}

// TestIntegration_AmbiguousBaseIsIndistinguishableFromUnratedInTheView pins the
// blind spot that docs/billing-reconciliation.md warns about, so the warning can
// never silently drift from what the view actually exposes.
//
// THE INVARIANT: a rollup WITHHELD by the rater's ambiguous_base gate appears in
// billing_reconciliation_hourly as raw_attempts > 0 with rated_attempts = 0 and
// rated_cost = 0 — byte-identical in shape to an hour the rater never rated — and
// the view carries NO column that distinguishes the two. The base gate keys on
// rating_price/rating_derived join outcomes (via_derived/via_base) that exist only
// inside the rater, not as billing_event columns, so the view CANNOT compute it;
// the only signal is the run report's AmbiguousBaseEvents. An operator auditing
// deltas must therefore consult the run report before calling such a row lost
// rating. If a future migration ever DOES surface an ambiguous/withheld column,
// this test fails and the doc paragraph must be rewritten to point at it.
//
// This is deliberately asymmetric with the org case, which the view DOES explain
// via distinct_org_ids > 1 (asserted below as the contrast).
func TestIntegration_AmbiguousBaseIsIndistinguishableFromUnratedInTheView(t *testing.T) {
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

	const sch = "phoebe_rating_ambig_view_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, ratingSchemaDDL(t))

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{
			"cheap/base":     rate3("0.000001", "0", "0"),
			"expensive/base": rate3("0.000009", "0", "0"),
		},
		nil, PolicyMultiplier, MustDec("1.5"), Dec{},
	)

	// One ft: id under TWO base_models in one hour → the base gate withholds it.
	for _, s := range []struct{ req, base string }{
		{"ab-1", "cheap/base"}, {"ab-2", "expensive/base"},
	} {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, base_model, prompt_tokens, completion_tokens, event_ts)
			 VALUES ($1,'a','r-ambig','org-1','ft:dupe',$2,1000,0,$3)`,
			s.req, s.base, hour.Add(5*time.Minute)); err != nil {
			t.Fatalf("seed %s: %v", s.req, err)
		}
	}
	// A second resource the rater is simply never asked to rate: the "rater has not
	// run for this hour" cause, seeded in an hour outside the rated window.
	unratedHour := hour.Add(time.Hour)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, base_model, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('ur-1','a','r-unrated','org-1','ft:clean','cheap/base',1000,0,$1)`,
		unratedHour.Add(5*time.Minute)); err != nil {
		t.Fatalf("seed unrated: %v", err)
	}

	store := NewPostgresStore(db)
	res, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}
	if res.AmbiguousBaseEvents != 2 {
		t.Fatalf("AmbiguousBaseEvents = %d, want 2 — the run report is the ONLY place this cause is visible",
			res.AmbiguousBaseEvents)
	}

	type viewRow struct {
		raw, rated, distinctOrgs, delta int64
		cost                            string
	}
	read := func(resource string, win time.Time) viewRow {
		var r viewRow
		if err := db.QueryRowContext(ctx, `
			SELECT raw_attempts, rated_attempts, distinct_org_ids, attempt_delta, rated_cost::text
			FROM billing_reconciliation_hourly
			WHERE window_start=$1 AND auth_id='a' AND resource_id=$2`, win, resource).
			Scan(&r.raw, &r.rated, &r.distinctOrgs, &r.delta, &r.cost); err != nil {
			t.Fatalf("read view for %s: %v", resource, err)
		}
		return r
	}

	withheld := read("r-ambig", hour)
	neverRated := read("r-unrated", unratedHour)

	// The withheld rollup looks exactly like lost rating.
	if withheld.raw != 2 || withheld.rated != 0 || withheld.delta != 2 || MustDec(withheld.cost).String() != "0.000000000" {
		t.Fatalf("withheld row = raw/rated/delta/cost %d/%d/%d/%s, want 2/0/2/0 (the gate excludes it from rated_usage entirely)",
			withheld.raw, withheld.rated, withheld.delta, withheld.cost)
	}
	// And the never-rated hour is the SAME shape, per raw attempt — that sameness
	// IS the blind spot the docs tell the operator to resolve via the run report.
	if neverRated.rated != 0 || neverRated.delta != neverRated.raw {
		t.Fatalf("never-rated row = raw/rated/delta %d/%d/%d, want rated 0 and delta == raw",
			neverRated.raw, neverRated.rated, neverRated.delta)
	}
	// Neither row carries an org-ambiguity signal, so distinct_org_ids cannot be
	// mistaken for a base-ambiguity explanation.
	if withheld.distinctOrgs != 1 || neverRated.distinctOrgs != 1 {
		t.Fatalf("distinct_org_ids withheld/never-rated = %d/%d, want 1/1 (single org on both; base ambiguity is invisible here)",
			withheld.distinctOrgs, neverRated.distinctOrgs)
	}

	// The view exposes NO ambiguous/withheld column. If one is ever added, the
	// documented "the view does not surface it" guidance is stale — fail here.
	var explanatory int64
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name='billing_reconciliation_hourly'
		  AND (column_name LIKE '%ambiguous%' OR column_name LIKE '%withheld%'
		       OR column_name LIKE '%distinct_base%')`).Scan(&explanatory); err != nil {
		t.Fatalf("inspect view columns: %v", err)
	}
	if explanatory != 0 {
		t.Fatalf("billing_reconciliation_hourly now has %d ambiguity/withheld column(s); docs/billing-reconciliation.md still tells operators the view does not surface base ambiguity — update the doc", explanatory)
	}
}

// TestIntegration_MigrationsCreateOrgGrainViewWithoutReplacement guards the
// invariant that the ENTIRE embedded migration set, applied in version order
// exactly as cmd/migrate applies it, creates billing_reconciliation_hourly ONCE
// at the rated natural grain — it is never created in a known-wrong org-grouped
// shape and then dropped and replaced by a later migration. The reconciliation
// view's shape is the operator's audit contract; an operator applying the
// migrations must never materialize a view shape nobody intends to run.
//
// It asserts three things: exactly one CREATE VIEW of that name exists across
// every up migration and no up migration DROPs it; the applied view exposes the
// org-evidence columns (missing_org_attempts, distinct_org_ids); and raw
// evidence is grouped at the rater's grain, so a rollout-era NULL org_id and a
// real org_id on the same (hour, auth, resource, model) collapse into ONE row
// rather than two false mismatch rows.
func TestIntegration_MigrationsCreateOrgGrainViewWithoutReplacement(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}

	ups, err := filepath.Glob("../../migrations/*.up.sql")
	if err != nil || len(ups) == 0 {
		t.Fatalf("glob up migrations: %v (found %d)", err, len(ups))
	}
	sort.Strings(ups)
	creates, drops := 0, 0
	for _, f := range ups {
		body, readErr := os.ReadFile(f)
		if readErr != nil {
			t.Fatalf("read %s: %v", f, readErr)
		}
		creates += strings.Count(string(body), "CREATE VIEW billing_reconciliation_hourly")
		drops += strings.Count(string(body), "DROP VIEW billing_reconciliation_hourly")
		drops += strings.Count(string(body), "DROP VIEW IF EXISTS billing_reconciliation_hourly")
	}
	if creates != 1 || drops != 0 {
		t.Fatalf("up migrations CREATE billing_reconciliation_hourly %d time(s) and DROP it %d time(s); want exactly 1 create and 0 drops — operators must not apply a view shape that a later migration immediately replaces", creates, drops)
	}

	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const sch = "phoebe_migration_view_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()

	// Apply EVERY up migration in version order — the real cmd/migrate sequence,
	// io_log included, so nothing about the ordering is hand-curated here.
	for _, f := range ups {
		ddl, readErr := os.ReadFile(f)
		if readErr != nil {
			t.Fatalf("read %s: %v", f, readErr)
		}
		exec(t, db, string(ddl))
	}

	for _, col := range []string{"missing_org_attempts", "distinct_org_ids"} {
		var n int64
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.columns
			WHERE table_schema=$1 AND table_name='billing_reconciliation_hourly'
			  AND column_name=$2`, sch, col).Scan(&n); err != nil {
			t.Fatalf("inspect view column %s: %v", col, err)
		}
		if n != 1 {
			t.Fatalf("billing_reconciliation_hourly is missing %s after applying all migrations; the org-grain view did not survive the migration set", col)
		}
	}

	// Two attempts on the same natural key, one carrying a rollout-era NULL org.
	// At the rater's grain they are ONE reconciliation row; grouping raw by
	// org_id would split them into two rows that each look like a mismatch.
	hour := mustTime("2026-06-08T10:00:00Z")
	for i, org := range []interface{}{nil, "org-1"} {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO billing_event
			 (request_id, auth_id, resource_id, org_id, model, prompt_tokens, cached_tokens, completion_tokens, usage_found, event_ts)
			 VALUES ($1,'a','d1',$2,'b',10,0,5,TRUE,$3)`,
			fmt.Sprintf("org-grain-%d", i), org, hour.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("insert attempt %d: %v", i, err)
		}
	}
	var rows, rawAttempts, missingOrg, distinctOrgs int64
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MAX(raw_attempts),0), COALESCE(MAX(missing_org_attempts),0),
		       COALESCE(MAX(distinct_org_ids),0)
		FROM billing_reconciliation_hourly`).Scan(&rows, &rawAttempts, &missingOrg, &distinctOrgs); err != nil {
		t.Fatalf("query view: %v", err)
	}
	if rows != 1 || rawAttempts != 2 {
		t.Fatalf("view has %d row(s) with max raw_attempts=%d, want 1 row of 2 attempts — a NULL org and a real org on one natural key must not split into false mismatch rows", rows, rawAttempts)
	}
	if missingOrg != 1 || distinctOrgs != 1 {
		t.Fatalf("missing_org_attempts/distinct_org_ids = %d/%d, want 1/1 — the org evidence the grain change replaced org grouping with", missingOrg, distinctOrgs)
	}
}

// TestIntegration_ServingModeSplitsRollupAndPricesEachMode is THE regression test for
// the defect migration 0006 exists to fix, against real Postgres.
//
// THE BUG (pre-0006): serving_mode was NOT part of the rollup grain, but it IS part of
// the price key — shared traffic prices from 'shared:'||base_model, dedicated from the
// bare base_model. So a bucket containing both modes collapsed into ONE rollup, and
// MIN(prompt_price) applied the CHEAPER of the two rates to ALL of it. Silent
// under-billing (or over-billing, depending which way the rates differ).
//
// The pre-existing ambiguous_base gate could not catch it: BOTH rows carry the SAME
// base_model and BOTH resolve via the same pricing path, so COUNT(DISTINCT base_model)
// is 1 and bool_or(via_derived)/bool_or(via_base) never mix. The gate stays false.
//
// REACHABILITY: serving_mode is a deploy-time property (a tf_model column, or the
// anti-spoof X-Saturn-Serving-Mode header), so it cannot vary per request. But it CAN
// change across an hour: Atlas flipping a deployment's mode, or the header rollout
// landing mid-hour — an ABSENT header reads as dedicated, so pre-rollout events on an
// already-shared deployment price as dedicated. That is the scenario seeded below.
//
// WHAT THIS PINS: one resource, one model, one hour, both modes → TWO rollups, each
// priced at ITS OWN rate, with no withholding and no anomaly. Deliberately asserts the
// per-row cost, not just the row count: a split that still priced both rows the same
// would pass a count-only test while leaving the money wrong.
func TestIntegration_ServingModeSplitsRollupAndPricesEachMode(t *testing.T) {
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

	const sch = "phoebe_rating_servingmode_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, ratingSchemaDDL(t))

	hour := mustTime("2026-06-08T10:00:00Z")
	// Two DISTINCT rates for one base: the bare key (dedicated) and the mode-prefixed
	// key (shared). Shared is 10x cheaper here, so a collapse would be visible as
	// under-billing — and MIN() would pick the shared rate for the dedicated traffic.
	book := newTestBook(
		map[string]Rate3{
			"b":        rate3("0.000010", "0", "0"),
			"shared:b": rate3("0.000001", "0", "0"),
		},
		nil, PolicyIdentity, Dec{}, Dec{},
	)

	// ONE resource, ONE model, ONE hour, both modes — the mid-hour flip. 'd1'/'d2' are
	// dedicated (NULL and '' respectively: both spellings of "absence = dedicated",
	// which must land in the SAME rollup); 's1'/'s2' are shared.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, base_model, serving_mode, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('d1','a','res','org-1','m','b',NULL,100,0,$1),
		        ('d2','a','res','org-1','m','b','',  100,0,$1),
		        ('s1','a','res','org-1','m','b','shared',100,0,$1),
		        ('s2','a','res','org-1','m','b','shared',100,0,$1)`, hour.Add(5*time.Minute)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	store := NewPostgresStore(db)
	res, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}

	// TWO rollups from one (auth, resource, model, hour) — the split. All four events
	// rate; nothing is withheld, because differing modes are now a legitimate split
	// rather than an ambiguity.
	if res.RollupsWritten != 2 || res.EventsRated != 4 {
		t.Fatalf("rollups/events = %d/%d, want 2/4 (one rollup per serving mode, all events rated)",
			res.RollupsWritten, res.EventsRated)
	}
	if res.AmbiguousBaseEvents != 0 {
		t.Fatalf("AmbiguousBaseEvents = %d, want 0 (a mode split is not a base ambiguity)", res.AmbiguousBaseEvents)
	}

	// THE MONEY. Each rollup must carry ITS OWN rate:
	//   dedicated: 200 prompt tokens x 0.000010 = 0.002000000
	//   shared   : 200 prompt tokens x 0.000001 = 0.000200000
	// Pre-0006 this was ONE row of 400 tokens at MIN() = 0.000001 → 0.000400000,
	// i.e. the dedicated traffic billed at the shared rate.
	for _, want := range []struct {
		mode string
		cost string
		rate string
	}{
		{"", "0.002000000", "0.000010000"},
		{"shared", "0.000200000", "0.000001000"},
	} {
		var cost, rate string
		var tokens int64
		if err := db.QueryRowContext(ctx,
			`SELECT cost::text, applied_prompt_rate::text, prompt_tokens
			   FROM rated_usage
			  WHERE resource_id = 'res' AND serving_mode = $1 AND window_start = $2`,
			want.mode, hour).Scan(&cost, &rate, &tokens); err != nil {
			t.Fatalf("read serving_mode=%q rollup: %v", want.mode, err)
		}
		if cost != want.cost || rate != want.rate || tokens != 200 {
			t.Fatalf("serving_mode=%q: cost/rate/tokens = %s/%s/%d, want %s/%s/200",
				want.mode, cost, rate, tokens, want.cost, want.rate)
		}
	}

	// The two rollups must have DISTINCT ids: rated_usage_id is an md5 over the grain,
	// so if serving_mode had not entered the hash they would collide and the second
	// upsert would overwrite the first — the split would be undone at the id level even
	// with a correct GROUP BY.
	var distinctIDs int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT id) FROM rated_usage WHERE resource_id = 'res' AND window_start = $1`,
		hour).Scan(&distinctIDs); err != nil {
		t.Fatalf("count distinct ids: %v", err)
	}
	if distinctIDs != 2 {
		t.Fatalf("distinct rated_usage ids = %d, want 2 (serving_mode must be in the md5 grain)", distinctIDs)
	}
}

// TestIntegration_OwnerConflictWithheldAndGraphCarried pins the two other net-new 0006
// behaviours against real Postgres, which the oracle store cannot model:
//
//   - OWNER CONFLICT: an event carrying BOTH user_id and group_id contradicts the
//     upstream user-XOR-group model. The owner cannot be determined, so the rollup is
//     WITHHELD (never billed to a guessed owner) and counted. Note auth-server emits
//     these headers under an else-if and so cannot produce this today — the gate guards
//     against a FUTURE producer regression.
//   - OWNER SPLITS the grain: two different owners on one (auth, resource, model, hour)
//     are two rollups, not one, so per-person charges are presentable.
//   - GRAPH is EVIDENCE, not grain: it is carried onto the rollup but never splits it,
//     and a rollup spanning TWO graphs still BILLS (with a NULL graph rather than a
//     guess) — unlike every other ambiguity, which withholds.
func TestIntegration_OwnerConflictWithheldAndGraphCarried(t *testing.T) {
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

	const sch = "phoebe_rating_owner_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, ratingSchemaDDL(t))

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{"b": rate3("0.000005", "0", "0")},
		nil, PolicyIdentity, Dec{}, Dec{},
	)

	//   'bad'   : one event with BOTH user and group  → withheld.
	//   'split' : two events, different owners        → TWO rollups.
	//   'graph' : two events, TWO distinct graphs     → ONE rollup, BILLED, NULL graph.
	//   'one'   : two events, one graph               → ONE rollup carrying that graph.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, user_id, group_id, resource_id, org_id, model, base_model, graph_k8s_name, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('b1','a','u-1','g-1','bad',  'org-1','m','b',NULL,     100,0,$1),
		        ('p1','a','u-1',NULL, 'split','org-1','m','b',NULL,     100,0,$1),
		        ('p2','a',NULL, 'g-2','split','org-1','m','b',NULL,     100,0,$1),
		        ('g1','a','u-3',NULL, 'graph','org-1','m','b','dgd-a',  100,0,$1),
		        ('g2','a','u-3',NULL, 'graph','org-1','m','b','dgd-b',  100,0,$1),
		        ('o1','a','u-4',NULL, 'one',  'org-1','m','b','dgd-c',  100,0,$1),
		        ('o2','a','u-4',NULL, 'one',  'org-1','m','b',NULL,     100,0,$1)`,
		hour.Add(5*time.Minute)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	store := NewPostgresStore(db)
	res, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}

	if res.OwnerConflictEvents != 1 {
		t.Fatalf("OwnerConflictEvents = %d, want 1 (the both-owner event)", res.OwnerConflictEvents)
	}
	// split(2) + graph(2) + one(2) = 6 rated; bad(1) withheld.
	// Rollups: split is TWO (one per owner), graph is one, one is one = 4.
	if res.RollupsWritten != 4 || res.EventsRated != 6 {
		t.Fatalf("rollups/events = %d/%d, want 4/6 (owner splits 'split' into two; 'bad' withheld)",
			res.RollupsWritten, res.EventsRated)
	}
	// The two-graph rollup is BILLED, not withheld — the whole point of graph being
	// evidence rather than identity.
	if res.AmbiguousGraphRollups != 1 {
		t.Fatalf("AmbiguousGraphRollups = %d, want 1 (the two-graph rollup, which still bills)", res.AmbiguousGraphRollups)
	}

	// 'bad' never reached rated_usage.
	var badRows int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM rated_usage WHERE resource_id = 'bad'`).Scan(&badRows); err != nil {
		t.Fatalf("count bad: %v", err)
	}
	if badRows != 0 {
		t.Fatalf("rated_usage rows for the owner-conflict resource = %d, want 0 (withheld, never billed to a guessed owner)", badRows)
	}

	// The owner pair split 'split' into one rollup per owner, each carrying its own
	// (type, id) — this is what makes per-person / per-team charges presentable.
	rows, err := db.QueryContext(ctx,
		`SELECT owner_type, owner_id FROM rated_usage WHERE resource_id = 'split' ORDER BY owner_type`)
	if err != nil {
		t.Fatalf("read split rollups: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var owners []string
	for rows.Next() {
		var ot, oid string
		if err := rows.Scan(&ot, &oid); err != nil {
			t.Fatalf("scan owner: %v", err)
		}
		owners = append(owners, ot+":"+oid)
	}
	if len(owners) != 2 || owners[0] != "group:g-2" || owners[1] != "user:u-1" {
		t.Fatalf("split owners = %v, want [group:g-2 user:u-1] (one rollup per owner)", owners)
	}

	// A two-graph rollup carries NULL rather than a guessed graph: an unattributable
	// cost must not LOOK attributable.
	var graph sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT graph_k8s_name FROM rated_usage WHERE resource_id = 'graph'`).Scan(&graph); err != nil {
		t.Fatalf("read graph rollup: %v", err)
	}
	if graph.Valid {
		t.Fatalf("two-graph rollup carries graph_k8s_name = %q, want NULL (never guess a cost centre)", graph.String)
	}

	// A partial-NULL graph resolves to the one known graph — same MAX()-ignores-NULL
	// convergence org_id already relies on, so a late-propagating graph is not lost.
	var oneGraph sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT graph_k8s_name FROM rated_usage WHERE resource_id = 'one'`).Scan(&oneGraph); err != nil {
		t.Fatalf("read one-graph rollup: %v", err)
	}
	if !oneGraph.Valid || oneGraph.String != "dgd-c" {
		t.Fatalf("partial-NULL graph rollup = %v/%q, want dgd-c (MAX ignores NULLs)", oneGraph.Valid, oneGraph.String)
	}
}

// TestIntegration_ReRateNullsAGraphThatBecameAmbiguous pins the ONE place where graph
// and org_id deliberately behave DIFFERENTLY on re-rate, against real Postgres.
//
// org_id's upsert COALESCEs (never erase a known org), and that is safe only because a
// real->NULL org transition cannot reach the UPDATE: ambiguous_org WITHHOLDS such a
// rollup. graph has no such protection -- ambiguous_graph deliberately does NOT
// withhold, because the graph decides what a cost is attributed AGAINST, not WHO is
// billed. So its NULL genuinely reaches the UPDATE and must overwrite.
//
// The failure this guards: run A records one graph; run B (a late event arrives, or a
// deployment moved) finds TWO and nulls the column to avoid asserting a cost centre it
// can no longer name. Under a COALESCE the stale graph would be restored, silently
// undoing the nulling and leaving the rollup claiming hardware the rater has just
// determined it cannot identify.
func TestIntegration_ReRateNullsAGraphThatBecameAmbiguous(t *testing.T) {
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

	const sch = "phoebe_rating_graphrerate_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, ratingSchemaDDL(t))

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{"b": rate3("0.000005", "0", "0")},
		nil, PolicyIdentity, Dec{}, Dec{},
	)
	store := NewPostgresStore(db)

	// RUN A: one event, one graph. The rollup records it.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, base_model, graph_k8s_name, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('e1','a','res','org-1','m','b','dgd-a',100,0,$1)`, hour.Add(5*time.Minute)); err != nil {
		t.Fatalf("seed run A: %v", err)
	}
	if _, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour)); err != nil {
		t.Fatalf("RateWindow A: %v", err)
	}
	var graphA sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT graph_k8s_name FROM rated_usage WHERE resource_id = 'res'`).Scan(&graphA); err != nil {
		t.Fatalf("read run A: %v", err)
	}
	if !graphA.Valid || graphA.String != "dgd-a" {
		t.Fatalf("run A graph = %v/%q, want dgd-a", graphA.Valid, graphA.String)
	}

	// RUN B: a second event on a DIFFERENT graph lands in the same hour. The rollup is
	// now two-graph: it still BILLS (graph is evidence, not identity) but can no longer
	// name its cost centre.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, base_model, graph_k8s_name, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('e2','a','res','org-1','m','b','dgd-b',100,0,$1)`, hour.Add(6*time.Minute)); err != nil {
		t.Fatalf("seed run B: %v", err)
	}
	resB, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow B: %v", err)
	}
	if resB.AmbiguousGraphRollups != 1 {
		t.Fatalf("AmbiguousGraphRollups = %d, want 1", resB.AmbiguousGraphRollups)
	}
	// It still bills -- BOTH events, at the full rate. Ambiguity here costs attribution,
	// never revenue.
	if resB.EventsRated != 2 || resB.RollupsWritten != 1 {
		t.Fatalf("run B events/rollups = %d/%d, want 2/1 (a two-graph rollup still bills)",
			resB.EventsRated, resB.RollupsWritten)
	}

	// THE ASSERTION: the previously-recorded graph is GONE, not restored by a COALESCE.
	var graphB sql.NullString
	var cost string
	if err := db.QueryRowContext(ctx,
		`SELECT graph_k8s_name, cost::text FROM rated_usage WHERE resource_id = 'res'`).
		Scan(&graphB, &cost); err != nil {
		t.Fatalf("read run B: %v", err)
	}
	if graphB.Valid {
		t.Fatalf("after re-rate the rollup still claims graph %q — a COALESCE restored a cost centre the rater determined it cannot name", graphB.String)
	}
	if cost != "0.001000000" {
		t.Fatalf("run B cost = %s, want 0.001000000 (200 tokens x 0.000005; ambiguity must not touch the money)", cost)
	}
}

// TestIntegration_OwnerConflictDoesNotPoisonItsBucket is the regression test for a bug
// the FIRST owner-conflict test missed, because that test isolated the malformed event
// on its own resource_id and so never made it share a bucket with anyone.
//
// THE BUG: a both-owner event has nowhere to go in the owner CASE, so it collapses to
// owner_type=” / owner_id=” -- the SAME bucket as genuine no-owner traffic. The gate
// was a group-level bool_or(owner_conflict) in `grouped`, which therefore withheld
// EVERY legitimate no-owner rollup that merely shared a bucket with one malformed
// event. One bad row zeroed other people's revenue, and the alarm counted the whole
// group's events as conflicted, over-reporting the blast radius 3x on this fixture.
//
// THE FIX: drop conflicted events PER EVENT in grouped's WHERE (like the
// unattributable filters) and count them from `ev`, so the damage is exactly the
// offending event and the count names only it.
//
// Note this is a FUTURE-producer guard, not a live one: auth-server emits the two
// identity headers under an else-if and structurally cannot send both. That is
// precisely why the gate must not over-withhold -- when a new producer does regress,
// the blast radius should be one event, not everyone who shared its hour.
func TestIntegration_OwnerConflictDoesNotPoisonItsBucket(t *testing.T) {
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

	const sch = "phoebe_rating_conflictbucket_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, ratingSchemaDDL(t))

	hour := mustTime("2026-06-08T10:00:00Z")
	book := newTestBook(
		map[string]Rate3{"b": rate3("0.000005", "0", "0")},
		nil, PolicyIdentity, Dec{}, Dec{},
	)

	// ALL THREE share one (auth, resource, model, serving_mode, hour) bucket, and the
	// two good ones have NO owner -- so they land in the same '' / '' owner bucket the
	// conflicted event collapses into. That collision is the whole point of the test.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, user_id, group_id, resource_id, org_id, model, base_model, prompt_tokens, completion_tokens, event_ts)
		 VALUES ('ok1','a',NULL, NULL, 'res','org-1','m','b',100,0,$1),
		        ('ok2','a',NULL, NULL, 'res','org-1','m','b',100,0,$1),
		        ('bad','a','u-1','g-1','res','org-1','m','b',100,0,$1)`,
		hour.Add(5*time.Minute)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	store := NewPostgresStore(db)
	res, err := store.RateWindow(ctx, book, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}

	// The two legitimate events STILL BILL. Under the old group-level gate this was 0.
	if res.EventsRated != 2 || res.RollupsWritten != 1 {
		t.Fatalf("events/rollups = %d/%d, want 2/1 — one malformed event must not withhold the revenue of legitimate events sharing its bucket",
			res.EventsRated, res.RollupsWritten)
	}
	// 200 tokens x 0.000005. The conflicted event contributes nothing.
	if res.TotalCost != "0.001000000" {
		t.Fatalf("TotalCost = %s, want 0.001000000 (the two good events only)", res.TotalCost)
	}
	// EXACTLY the offending event — not its innocent neighbours. Under the old gate
	// this reported 3, sending an operator after 3x the real blast radius.
	if res.OwnerConflictEvents != 1 {
		t.Fatalf("OwnerConflictEvents = %d, want 1 (only one event carried both owners)", res.OwnerConflictEvents)
	}

	// The surviving rollup is the no-owner one, carrying only the good events.
	var ownerType, ownerID string
	var events int64
	if err := db.QueryRowContext(ctx,
		`SELECT owner_type, owner_id, event_count FROM rated_usage WHERE resource_id = 'res'`).
		Scan(&ownerType, &ownerID, &events); err != nil {
		t.Fatalf("read rollup: %v", err)
	}
	if ownerType != "" || ownerID != "" || events != 2 {
		t.Fatalf("rollup = (%q,%q) with %d events, want ('','') with 2 (the conflicted event must not be counted into it)",
			ownerType, ownerID, events)
	}
}
