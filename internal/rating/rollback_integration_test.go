//go:build integration

package rating

import (
	"context"
	"database/sql"
	"os"
	"testing"
)

// TestIntegration_GroupScopesRollbackIsLossy pins the LOSSY claim in the
// header of migrations/0008_group_scopes.down.sql against real Postgres, so
// the comment can never be "fixed" back to the false claim that re-rating
// after a down/up cycle rebuilds the group rows:
//
//   - the schema comes back: after 0008 down then up again,
//     billing_event.member_group_ids exists again and group_usage exists
//     again;
//   - the evidence does NOT come back: a row seeded with membership evidence
//     reads member_group_ids = NULL after the cycle -- the down migration
//     dropped the only copy of the membership evidence, and the up migration
//     only re-adds an empty column. Re-rating afterwards attributes group
//     usage from group-token usage (billing_event.group_id) only, so
//     month-to-date group spend restarts lower than it really is.
//
// It loads the REAL migration files 0001-0008 from disk (like
// TestIntegration_InvalidUsageEvidenceNeverEntersMoney's apply loop) rather
// than a hand-copied DDL constant, so it tests exactly what cmd/migrate ships.
func TestIntegration_GroupScopesRollbackIsLossy(t *testing.T) {
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

	const sch = "phoebe_rating_rollback_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()

	apply := func(name string) {
		t.Helper()
		ddl, readErr := os.ReadFile("../../migrations/" + name)
		if readErr != nil {
			t.Fatalf("read migration %s: %v (the integration test applies the REAL "+
				"migration DDL so it can't drift from production)", name, readErr)
		}
		exec(t, db, string(ddl))
	}
	// The full production chain 0001-0008, in cmd/migrate apply order.
	for _, name := range []string{
		"0001_billing_event.up.sql",
		"0002_rating.up.sql",
		"0003_io_log.up.sql",
		"0004_billing_event_serving_mode.up.sql",
		"0005_invoice_grade_attempts.up.sql",
		"0006_rollup_grain.up.sql",
		"0007_serving_mode_explicit.up.sql",
		"0008_group_scopes.up.sql",
	} {
		apply(name)
	}

	// Seed one row carrying membership evidence -- the column 0008 added.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, member_group_ids)
		 VALUES ('member-evidence','a', ARRAY['g1','g2']::text[])`); err != nil {
		t.Fatalf("seed membership evidence: %v", err)
	}
	var seeded int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM billing_event
		 WHERE request_id = 'member-evidence' AND member_group_ids = ARRAY['g1','g2']::text[]`).
		Scan(&seeded); err != nil {
		t.Fatalf("read seeded evidence: %v", err)
	}
	if seeded != 1 {
		t.Fatalf("seeded membership evidence rows = %d, want 1 (the fixture must carry evidence before the rollback)", seeded)
	}

	// The rollback cycle: 0008 down, then 0008 up again.
	apply("0008_group_scopes.down.sql")
	apply("0008_group_scopes.up.sql")

	// The schema is restored: the column and the table both exist again.
	var colExists bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS (
		    SELECT 1 FROM information_schema.columns
		    WHERE table_schema = current_schema()
		      AND table_name = 'billing_event'
		      AND column_name = 'member_group_ids')`).Scan(&colExists); err != nil {
		t.Fatalf("check member_group_ids column: %v", err)
	}
	if !colExists {
		t.Fatal("billing_event.member_group_ids is missing after the down/up cycle -- the schema must be restored")
	}
	var tblExists bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS (
		    SELECT 1 FROM information_schema.tables
		    WHERE table_schema = current_schema()
		      AND table_name = 'group_usage')`).Scan(&tblExists); err != nil {
		t.Fatalf("check group_usage table: %v", err)
	}
	if !tblExists {
		t.Fatal("group_usage is missing after the down/up cycle -- the schema must be restored")
	}

	// The evidence is NOT restored: the seeded row survives (the raw ledger is
	// never touched by 0008), but its membership evidence reads back NULL.
	// This is the lossy part: if the down migration ever "preserved" the
	// column (or the header reverted to claiming it did), this assertion goes
	// red.
	var rowCount, evidenceRows int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*), COUNT(*) FILTER (WHERE member_group_ids IS NOT NULL)
		   FROM billing_event WHERE request_id = 'member-evidence'`).
		Scan(&rowCount, &evidenceRows); err != nil {
		t.Fatalf("read evidence after rollback: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("billing_event rows for the seeded event = %d, want 1 (the raw row must survive the cycle)", rowCount)
	}
	if evidenceRows != 0 {
		t.Fatalf("member_group_ids still holds evidence on %d row(s) after the down/up cycle -- "+
			"the rollback is LOSSY and must read NULL (see 0008_group_scopes.down.sql's header)", evidenceRows)
	}
}
