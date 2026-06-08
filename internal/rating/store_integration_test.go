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
        model_id WITH =, tsrange(effective_from, effective_to) WITH &&)
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
        (0) WITH =, tsrange(effective_from, effective_to) WITH &&)
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

	// Anomalies: 1 unpriced + 1 unattributable.
	an, err := store.CountAnomalies(ctx, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("CountAnomalies: %v", err)
	}
	if an.UnpricedEvents != 1 || an.UnattributableEvents != 1 {
		t.Fatalf("anomalies = %+v, want 1/1", an)
	}

	// Run the REAL rating SQL.
	res, err := store.RateWindow(ctx, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow: %v", err)
	}
	if res.RollupsWritten != 2 {
		t.Fatalf("rollups = %d, want 2", res.RollupsWritten)
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

	// Idempotency: re-run, totals unchanged, still 2 rows.
	res2, err := store.RateWindow(ctx, hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("RateWindow re-run: %v", err)
	}
	if res2.RollupsWritten != 2 || MustDec(res2.TotalCost).String() != MustDec(res.TotalCost).String() {
		t.Fatalf("re-run not idempotent: %+v vs %+v", res2, res)
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
