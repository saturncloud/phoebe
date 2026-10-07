//go:build integration

// Package admission integration test: runs the REAL group spend check
// (groupSpendExhaustedSQL, through PostgresSpendStore) against a LIVE Postgres
// loaded with the production migrations, and pins the verdicts the unit tests
// can only assume: the sqlmock test pins the query text and the fakes hard-code
// the boundary verdicts, so neither would notice the comparison drifting from
// >= to > or the COALESCE being dropped.
//
// Gated behind the `integration` build tag AND a non-empty PHOEBE_TEST_DATABASE_URL.
// Run with:
//
//	PHOEBE_TEST_DATABASE_URL=postgres://... go test -tags=integration ./internal/admission/...
package admission

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"
)

// spendSchemaDDL returns the production schema applied before the spend test.
// It is loaded from the REAL migration .sql files (0001 through 0008, in apply
// order) rather than a hand-copied group_usage table, so the test runs against
// exactly the column types production has (group_usage.cost is NUMERIC(20,9))
// and cannot silently drift from them. The DDL runs inside a per-test isolated
// schema (search_path is set by the caller), leaving no residue.
func spendSchemaDDL(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	for _, f := range []string{
		"../../migrations/0001_billing_event.up.sql",
		"../../migrations/0002_rating.up.sql",
		"../../migrations/0003_io_log.up.sql",
		"../../migrations/0004_billing_event_serving_mode.up.sql",
		"../../migrations/0005_invoice_grade_attempts.up.sql",
		"../../migrations/0006_rollup_grain.up.sql",
		"../../migrations/0007_serving_mode_explicit.up.sql",
		// 0008 adds group_usage, the rollup the spend check sums.
		"../../migrations/0008_group_scopes.up.sql",
	} {
		ddl, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read migration %s: %v (the integration test applies the REAL "+
				"migration DDL so it can't drift from production)", f, err)
		}
		b.Write(ddl)
		b.WriteString("\n")
	}
	return b.String()
}

// TestIntegration_GroupSpendExhausted pins the monthly spend verdict as
// Postgres computes it. Every verdict is read through the store only — the
// test never sums spend in Go — so it proves the comparison itself runs in
// Postgres:
//
//   - a zero cap with no group_usage rows is exhausted (the empty month sums
//     to NULL, COALESCE makes it 0, and 0 >= 0 denies all paid work);
//   - month spend exactly equal to the cap is exhausted (the >= boundary);
//   - month spend below the cap is not exhausted;
//   - a row in the PRIOR UTC month is excluded from the sum, even when it is
//     the last hour before the boundary;
//   - a row in the FIRST hour of the current UTC month is counted.
//
// The session runs with TimeZone set to UTC+14 (Pacific/Kiritimati), so a
// month boundary taken in the session's time zone instead of UTC lands 14
// hours away from the UTC boundary: it either pulls the prior month's last
// hour into the sum or, during the last 14 hours of a UTC month, pushes the
// first hour of the current month out of it. Postgres in CI runs with
// TimeZone=UTC, where the two boundaries coincide and neither mistake shows.
func TestIntegration_GroupSpendExhausted(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres spend check")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	// search_path is a per-connection setting, so the pool is pinned to one
	// connection: the store's query must see the same isolated schema the
	// fixture was written into.
	db.SetMaxOpenConns(1)

	const sch = "phoebe_admission_spend_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	// Like search_path, TimeZone is per connection, so it holds for every
	// query below on the single pinned connection.
	exec(t, db, "SET TIME ZONE 'Pacific/Kiritimati'")
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	exec(t, db, spendSchemaDDL(t))

	// UTC hour buckets, the same bucketing the rater writes window_start with.
	// monthStart is the first hour of the current UTC month; the hour before it
	// is the last hour of the prior month.
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	thisHour := now.Truncate(time.Hour)
	priorMonthLastHour := monthStart.Add(-time.Hour)

	const (
		gEmpty = "00000000000000000000000000000000"
		gSpend = "a1b2c3d4e5f60718293a4b5c6d7e8f90"
		gOther = "00112233445566778899aabbccddeeff"
		gStart = "ffeeddccbbaa99887766554433221100"
	)
	seed := func(group string, windowStart time.Time, cost string) {
		t.Helper()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO group_usage (group_id, window_start, cost, event_count)
			 VALUES ($1, $2, $3::numeric, 1)`,
			group, windowStart, cost); err != nil {
			t.Fatalf("seed group_usage (%s, %s): %v", group, windowStart, err)
		}
	}
	// gSpend's current-month spend is 1.500000001, split across two hours so
	// the check must sum rows (when the test runs in the month's first hour
	// both land in one bucket, so the second is folded into the first). The
	// prior-month row is large enough that counting it would flip every verdict
	// below. gOther's spend must not leak into gSpend's sum.
	if thisHour.Equal(monthStart) {
		seed(gSpend, monthStart, "1.500000001")
	} else {
		seed(gSpend, monthStart, "1.000000000")
		seed(gSpend, thisHour, "0.500000001")
	}
	seed(gSpend, priorMonthLastHour, "1000")
	seed(gOther, monthStart, "1000")
	// gStart has a single row, in the first hour of the current UTC month.
	seed(gStart, monthStart, "2.5")

	store := NewPostgresSpendStore(db)
	for _, tc := range []struct {
		name  string
		group string
		spendCap string
		want  bool
	}{
		// 1. Zero cap, no rows at all for the group: exhausted.
		{"zero cap, empty month denies", gEmpty, "0", true},
		// A positive cap over an empty month is unreached (COALESCE to 0).
		{"positive cap, empty month admits", gEmpty, "0.000000001", false},
		// 2. Spend equal to the cap, to the ninth decimal: exhausted. Equality
		// at this scale only holds if the comparison is NUMERIC in Postgres.
		{"spend == cap denies", gSpend, "1.500000001", true},
		// 3. Spend below the cap by the smallest NUMERIC(20,9) step: admitted.
		{"spend < cap admits", gSpend, "1.500000002", false},
		{"spend > cap denies", gSpend, "1.5", true},
		// 4. The prior-month row (1000) is excluded: were it counted, spend
		// would be 1001.500000001 and this cap would be exhausted.
		{"prior month excluded", gSpend, "1001", false},
		// 5. The first hour of the current UTC month is counted: spend equal
		// to the cap is exhausted. A boundary taken in the session's UTC+14
		// time zone would drop this row late in the month and admit.
		{"current month first hour counted", gStart, "2.5", true},
	} {
		got, err := store.GroupSpendExhausted(ctx, tc.group, tc.spendCap)
		if err != nil {
			t.Fatalf("%s: GroupSpendExhausted(%s, %s): %v", tc.name, tc.group, tc.spendCap, err)
		}
		if got != tc.want {
			t.Errorf("%s: GroupSpendExhausted(%s, %s) = %v, want %v",
				tc.name, tc.group, tc.spendCap, got, tc.want)
		}
	}
}

func exec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}
