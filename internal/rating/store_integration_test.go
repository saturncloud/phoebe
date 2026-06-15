//go:build integration

// Package rating integration test: runs the REAL v2 rating SQL (resolvedEventsCTE
// + rateWindowSQL + countAnomaliesSQL) against a LIVE Postgres, then asserts the
// rated_usage rows the SQL wrote equal the pure Rate() oracle row-for-row over the
// same fixture. This is the production-path half of the conformance pair (the
// in-Go model lives in rater_test.go's TestConformance_SQLModelMatchesRateOracle).
//
// It is gated behind the `integration` build tag AND a non-empty
// PHOEBE_TEST_DATABASE_URL so the default `go test ./...` (and CI's unit lane)
// never need a database. Run it with:
//
//	PHOEBE_TEST_DATABASE_URL=postgres://... go test -tags=integration ./internal/rating/...
//
// The Postgres must have the btree_gist extension available (the migration's GiST
// exclusion constraints need it).
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

// schemaDDL is the v2 rating schema (model_price + derivation_policy + rated_usage,
// with the GiST exclusion constraints), created in an isolated schema per test run
// so the integration test is self-contained and leaves no residue.
const schemaDDL = `
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- billing_event mirrors the v1 metering schema (migration 0001): the model NAME
-- lives in the model column, which the rater aliases to model_id (the price key).
CREATE TABLE billing_event (
    request_id        VARCHAR(255) PRIMARY KEY,
    auth_id           VARCHAR(64),
    model             VARCHAR(255),
    prompt_tokens     INTEGER NOT NULL DEFAULT 0,
    cached_tokens     INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    event_ts          TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Expression index on the rater's scan predicate, mirroring migration 0002: an
-- index on bare (event_ts) cannot serve COALESCE(event_ts, created_at).
CREATE INDEX billing_event_rating_instant_ix
    ON billing_event ((COALESCE(event_ts, created_at)));

CREATE TABLE model_price (
    id               VARCHAR(32) PRIMARY KEY,
    model_id         VARCHAR(255) NOT NULL,
    derived_from     VARCHAR(255),
    prompt_price     NUMERIC(20,9),
    cached_price     NUMERIC(20,9),
    completion_price NUMERIC(20,9),
    effective_from   TIMESTAMPTZ NOT NULL,
    effective_to     TIMESTAMPTZ,
    created_by       VARCHAR(255),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT model_price_rate_or_derived_ck
        CHECK (derived_from IS NOT NULL OR prompt_price IS NOT NULL),
    CONSTRAINT model_price_rate_all_or_none_ck CHECK (
        (prompt_price IS NULL     AND cached_price IS NULL     AND completion_price IS NULL) OR
        (prompt_price IS NOT NULL AND cached_price IS NOT NULL AND completion_price IS NOT NULL)),
    CONSTRAINT model_price_no_overlap EXCLUDE USING gist (
        model_id WITH =, tstzrange(effective_from, effective_to) WITH &&)
);

CREATE TABLE derivation_policy (
    id             VARCHAR(32) PRIMARY KEY,
    function       VARCHAR(32) NOT NULL,
    factor         NUMERIC(20,9),
    markup         NUMERIC(20,9),
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to   TIMESTAMPTZ,
    created_by     VARCHAR(255),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT derivation_policy_no_overlap EXCLUDE USING gist (
        (0) WITH =, tstzrange(effective_from, effective_to) WITH &&)
);

CREATE TABLE rated_usage (
    id                     VARCHAR(32) PRIMARY KEY,
    auth_id                VARCHAR(64) NOT NULL,
    model_id               VARCHAR(255) NOT NULL,
    window_start           TIMESTAMPTZ NOT NULL,
    window_end             TIMESTAMPTZ NOT NULL,
    prompt_tokens          BIGINT NOT NULL,
    cached_tokens          BIGINT NOT NULL,
    completion_tokens      BIGINT NOT NULL,
    billable_prompt_tokens BIGINT NOT NULL,
    cost                   NUMERIC(20,9) NOT NULL,
    event_count            INTEGER NOT NULL,
    rated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT rated_usage_auth_model_window_uq UNIQUE (auth_id, model_id, window_start)
);`

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

	// Isolated schema so the test is hermetic and self-cleaning.
	const sch = "phoebe_rating_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")

	// Price book: base "b" (own rate), fine-tune "f" derived from "b"; 1.5×
	// multiplier policy. Mirrors the in-Go conformance fixture.
	exec(t, db, `INSERT INTO model_price (id, model_id, prompt_price, cached_price, completion_price, effective_from) VALUES
		('p1','b',0.000005,0.0000005,0.00002,'2026-01-01T00:00:00Z')`)
	exec(t, db, `INSERT INTO model_price (id, model_id, derived_from, effective_from) VALUES
		('p2','f','b','2026-01-01T00:00:00Z')`)
	exec(t, db, `INSERT INTO derivation_policy (id, function, factor, effective_from) VALUES
		('d1','multiplier',1.5,'2026-01-01T00:00:00Z')`)

	// Events: priced base, priced derived, unpriced, unattributable.
	events := []RatedEvent{
		{AuthID: "a", ModelID: "b", PromptTokens: 100, CachedTokens: 30, CompletionTokens: 50, At: hour.Add(5 * time.Minute)},
		{AuthID: "a", ModelID: "f", PromptTokens: 100, CachedTokens: 0, CompletionTokens: 0, At: hour.Add(15 * time.Minute)},
		{AuthID: "a", ModelID: "unpriced", PromptTokens: 9, At: hour.Add(1 * time.Minute)},
		{AuthID: "", ModelID: "b", PromptTokens: 9, At: hour.Add(2 * time.Minute)},
	}
	for i, e := range events {
		_, err := db.ExecContext(ctx,
			`INSERT INTO billing_event (request_id, auth_id, model, prompt_tokens, cached_tokens, completion_tokens, event_ts)
			 VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			fmt.Sprintf("req-%d", i), nullableStr(e.AuthID), nullableStr(e.ModelID),
			e.PromptTokens, e.CachedTokens, e.CompletionTokens, e.At)
		if err != nil {
			t.Fatalf("seed event %d: %v", i, err)
		}
	}

	store := NewPostgresStore(db)

	// CountAnomalies (the ad-hoc/ops query) still agrees: 1 unpriced + 1 unattr.
	an, err := store.CountAnomalies(ctx, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("CountAnomalies: %v", err)
	}
	if an.UnpricedEvents != 1 || an.UnattributableEvents != 1 {
		t.Fatalf("anomalies = %+v, want 1/1", an)
	}

	// Run the REAL rating SQL. The anomaly counts ride the SAME statement.
	res, err := store.RateWindow(ctx, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}
	if res.RollupsWritten != 2 {
		t.Fatalf("rollups = %d, want 2", res.RollupsWritten)
	}
	if res.UnpricedEvents != 1 || res.UnattributableEvents != 1 {
		t.Fatalf("RateWindow anomaly counts = %d/%d, want 1/1 (single-snapshot accounting)", res.UnpricedEvents, res.UnattributableEvents)
	}
	// Single-snapshot accounting invariant: every in-window event lands in exactly
	// one bucket of rated / unpriced / unattributable.
	if got := res.EventsRated + res.UnpricedEvents + res.UnattributableEvents; got != len(events) {
		t.Fatalf("rated+unpriced+unattributable = %d, want %d", got, len(events))
	}

	// Oracle: independent Rate() over the priced+attributable events.
	book := NewPriceBook(
		[]PriceRow{
			{ModelID: "b", HasRate: true, Prompt: MustDec("0.000005"), Cached: MustDec("0.0000005"), Completion: MustDec("0.00002"),
				EffectiveFrom: mustTime("2026-01-01T00:00:00Z")},
			derivedRow("f", "b"),
		},
		[]PolicyRow{{Func: PolicyMultiplier, Factor: MustDec("1.5"), EffectiveFrom: mustTime("2026-01-01T00:00:00Z")}},
	)
	for _, e := range []RatedEvent{events[0], events[1]} {
		rate, err := book.ResolvePrice(e.ModelID, e.At)
		if err != nil {
			t.Fatalf("oracle resolve %s: %v", e.ModelID, err)
		}
		wantCost := Rate(e, rate).String()
		var gotCost string
		err = db.QueryRowContext(ctx,
			`SELECT cost::text FROM rated_usage WHERE auth_id=$1 AND model_id=$2 AND window_start=$3`,
			e.AuthID, e.ModelID, hour).Scan(&gotCost)
		if err != nil {
			t.Fatalf("read rated_usage (%s): %v", e.ModelID, err)
		}
		// Normalize both through the oracle's fixed scale for a string compare.
		if MustDec(gotCost).String() != wantCost {
			t.Errorf("model %s: SQL cost = %s, oracle Rate() = %s", e.ModelID, gotCost, wantCost)
		}
	}

	// Deterministic surrogate ids: capture before the re-run.
	idsBefore := readRatedUsageIDs(t, db)

	// Idempotency: re-run, totals unchanged, still 2 rows.
	res2, err := store.RateWindow(ctx, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow re-run: %v", err)
	}
	if res2.RollupsWritten != 2 || MustDec(res2.TotalCost).String() != MustDec(res.TotalCost).String() {
		t.Fatalf("re-run not idempotent: %+v vs %+v", res2, res)
	}

	// deterministic-id: a re-run regenerates the SAME ids (md5 of the natural key,
	// no random component), so id stability is part of the idempotency contract.
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

// readRatedUsageIDs returns natural-key → id for every rated_usage row.
func readRatedUsageIDs(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.Query(`SELECT auth_id || '|' || model_id || '|' || extract(epoch FROM window_start)::bigint::text, id FROM rated_usage`)
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

// TestConformance_SubNanoRoundingMatchesSQL is the rounding-order conformance
// guard. It builds a rollup of MULTIPLE events whose per-token cost has a
// sub-nano residue (a derived price = base 0.000000001 × 1.5 = 0.0000000015 per
// token), so round-then-sum and sum-then-round DISAGREE. It asserts the SQL's
// rated_usage.cost equals the oracle computed as sum-of-exact-then-round-once —
// the production behavior — and that this differs from the naive
// round-each-event-then-sum, proving the test actually exercises the divergence.
func TestConformance_SubNanoRoundingMatchesSQL(t *testing.T) {
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

	const sch = "phoebe_rating_subnano_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")

	// Base "b": prompt_price = 1 nano (0.000000001). Derived "f": 1.5× → 0.0000000015
	// per prompt token — a sub-nano residue. cached/completion priced 0 so the cost
	// is purely prompt × 1.5-nano.
	exec(t, db, `INSERT INTO model_price (id, model_id, prompt_price, cached_price, completion_price, effective_from) VALUES
		('p1','b',0.000000001,0,0,'2026-01-01T00:00:00Z')`)
	exec(t, db, `INSERT INTO model_price (id, model_id, derived_from, effective_from) VALUES
		('p2','f','b','2026-01-01T00:00:00Z')`)
	exec(t, db, `INSERT INTO derivation_policy (id, function, factor, effective_from) VALUES
		('d1','multiplier',1.5,'2026-01-01T00:00:00Z')`)

	// Three single-prompt-token events for "f" in ONE rollup. Per event the exact
	// cost is 0.0000000015; round-EACH-then-sum = 0.000000002×3 = 0.000000006.
	// Sum-exact-then-round = round(0.0000000045) = 0.000000005 (half-up, 4.5→5).
	// 0.000000005 != 0.000000006 — the orders genuinely diverge here.
	events := []RatedEvent{
		{AuthID: "a", ModelID: "f", PromptTokens: 1, At: hour.Add(1 * time.Minute)},
		{AuthID: "a", ModelID: "f", PromptTokens: 1, At: hour.Add(2 * time.Minute)},
		{AuthID: "a", ModelID: "f", PromptTokens: 1, At: hour.Add(3 * time.Minute)},
	}
	for i, e := range events {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO billing_event (request_id, auth_id, model, prompt_tokens, cached_tokens, completion_tokens, event_ts)
			 VALUES ($1,$2,$3,$4,$5,$6,$7)`,
			fmt.Sprintf("sn-req-%d", i), e.AuthID, e.ModelID,
			e.PromptTokens, e.CachedTokens, e.CompletionTokens, e.At); err != nil {
			t.Fatalf("seed event %d: %v", i, err)
		}
	}

	store := NewPostgresStore(db)
	if _, err := store.RateWindow(ctx, hour, hour.Add(time.Hour)); err != nil {
		t.Fatalf("RateWindow: %v", err)
	}

	var gotCost string
	if err := db.QueryRowContext(ctx,
		`SELECT cost::text FROM rated_usage WHERE auth_id='a' AND model_id='f' AND window_start=$1`,
		hour).Scan(&gotCost); err != nil {
		t.Fatalf("read rated_usage: %v", err)
	}

	// Oracle: sum the EXACT per-event costs, round ONCE (production behavior).
	book := NewPriceBook(
		[]PriceRow{
			{ModelID: "b", HasRate: true, Prompt: MustDec("0.000000001"), Cached: MustDec("0"), Completion: MustDec("0"),
				EffectiveFrom: mustTime("2026-01-01T00:00:00Z")},
			derivedRow("f", "b"),
		},
		[]PolicyRow{{Func: PolicyMultiplier, Factor: MustDec("1.5"), EffectiveFrom: mustTime("2026-01-01T00:00:00Z")}},
	)
	exactSum := Dec{}
	roundEachSum := Dec{}
	for _, e := range events {
		rate, err := book.ResolvePrice(e.ModelID, e.At)
		if err != nil {
			t.Fatalf("oracle resolve: %v", err)
		}
		exactSum = exactSum.Add(rateExact(e, rate))
		roundEachSum = roundEachSum.Add(Rate(e, rate)) // round-per-event (the WRONG order)
	}
	wantRoundOnce := exactSum.Round(moneyScale).String()

	if MustDec(gotCost).String() != wantRoundOnce {
		t.Errorf("SQL cost = %s, sum-exact-then-round-once oracle = %s", gotCost, wantRoundOnce)
	}
	// Guard the guard: confirm the fixture actually distinguishes the two orders,
	// so this test can't silently stop testing what it claims to.
	if roundEachSum.String() == wantRoundOnce {
		t.Fatalf("fixture does not exercise the rounding divergence (round-each=%s == round-once=%s); pick rates with a real sub-nano residue",
			roundEachSum.String(), wantRoundOnce)
	}
}

// TestIntegration_UTCBucketing_SessionTZIndependent: utc-bucketing. The hour
// bucket must NOT depend on the session TimeZone. A fractional-offset session
// (Asia/Kolkata, +05:30) running the rater must produce window_start values on
// EXACT UTC hour boundaries, and a re-run from a UTC session must hit the SAME
// ON CONFLICT keys — no duplicate/overlapping rollups (which would double-bill on
// re-rate). The schema fixed this class for the exclusion constraint (tstzrange);
// this pins the bucketing expression.
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
	// One connection so SET TIME ZONE / search_path stick for every statement.
	db.SetMaxOpenConns(1)

	const sch = "phoebe_rating_tz_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	hour := mustTime("2026-06-08T10:00:00Z")
	exec(t, db, `INSERT INTO model_price (id, model_id, prompt_price, cached_price, completion_price, effective_from) VALUES
		('p1','b',0.000005,0.0000005,0.00002,'2026-01-01T00:00:00Z')`)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, model, prompt_tokens, cached_tokens, completion_tokens, event_ts)
		 VALUES ('tz-req-0','a','b',100,0,50,$1)`, hour.Add(30*time.Minute)); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	store := NewPostgresStore(db)

	// Rate from a FRACTIONAL-OFFSET session. The broken expression
	// date_trunc('hour', ev_ts) would truncate in IST and write
	// window_start = 10:30Z (16:00 IST) instead of 10:00Z.
	exec(t, db, "SET TIME ZONE 'Asia/Kolkata'")
	if _, err := store.RateWindow(ctx, hour, hour.Add(time.Hour)); err != nil {
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

	// Re-run from a UTC session: must hit the SAME conflict key — exactly one row,
	// same id, no duplicate/overlapping rollup.
	idsIST := readRatedUsageIDs(t, db)
	exec(t, db, "SET TIME ZONE 'UTC'")
	if _, err := store.RateWindow(ctx, hour, hour.Add(time.Hour)); err != nil {
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
// COALESCE(event_ts, created_at) must actually serve the rater's window
// predicate (the bare event_ts index it replaced could not, leaving a seq scan
// on an ever-growing table). enable_seqscan=off forces the planner to use an
// index if one is usable, so a seq scan in the plan means no index matched.
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
	db.SetMaxOpenConns(1) // SET/search_path must stick

	const sch = "phoebe_rating_ix_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, schemaDDL)

	exec(t, db, "SET enable_seqscan = off")
	rows, err := db.Query(`EXPLAIN SELECT * FROM billing_event
		WHERE COALESCE(event_ts, created_at) >= '2026-06-08T10:00:00Z'
		  AND COALESCE(event_ts, created_at) <  '2026-06-08T11:00:00Z'`)
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
	if !strings.Contains(plan, "billing_event_rating_instant_ix") {
		t.Fatalf("plan does not use billing_event_rating_instant_ix (the index cannot serve the rater's predicate):\n%s", plan)
	}
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
