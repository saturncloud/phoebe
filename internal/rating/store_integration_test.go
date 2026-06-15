//go:build integration

// Package rating integration test: runs the REAL rating SQL (rateWindowSQL) against
// a LIVE Postgres, pricing from a YAML PriceBook (E1), then asserts the rated_usage
// rows the SQL wrote — including the APPLIED per-token rates frozen onto each row —
// equal the pure Rate() oracle row-for-row over the same fixture. This is the
// production-path half of the conformance pair (the in-Go model lives in
// rater_test.go's TestConformance_SQLModelMatchesRateOracle).
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
    event_count             INTEGER NOT NULL,
    rated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT rated_usage_auth_model_window_uq UNIQUE (auth_id, model_id, window_start)
);`

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
			`INSERT INTO billing_event (request_id, auth_id, model, prompt_tokens, cached_tokens, completion_tokens, event_ts)
			 VALUES ($1,$2,$3,$4,$5,$6,$7)`,
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
			`INSERT INTO billing_event (request_id, auth_id, model, prompt_tokens, cached_tokens, completion_tokens, event_ts)
			 VALUES ($1,$2,$3,$4,$5,$6,$7)`,
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
			`INSERT INTO billing_event (request_id, auth_id, model, base_model, prompt_tokens, completion_tokens, event_ts)
			 VALUES ($1,'a',$2,$3,$4,0,$5)`,
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
		`INSERT INTO billing_event (request_id, auth_id, model, prompt_tokens, cached_tokens, completion_tokens, event_ts)
		 VALUES ('tz-req-0','a','b',100,0,50,$1)`, hour.Add(30*time.Minute)); err != nil {
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
		t.Fatalf("plan does not use billing_event_rating_instant_ix:\n%s", plan)
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
