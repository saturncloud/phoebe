//go:build integration

// Package rating integration tests: run the REAL aggregation SQL
// (aggregateWindowSQL) and the real migrations against a LIVE Postgres.
//
// Gated behind the `integration` build tag AND a non-empty
// PHOEBE_TEST_DATABASE_URL. Run with:
//
//	PHOEBE_TEST_DATABASE_URL=postgres://... go test -tags=integration ./internal/rating/...
//
// `go test ./...` and `go vet ./...` skip this file, so "unit tests green" does
// not mean this SQL compiles. Run the integration lane for any change here.
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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

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
		// 0004 adds billing_event.serving_mode, which aggregateWindowSQL reads (the
		// serving-mode SKU axis). Skipping it reproduces the staging 42703.
		"../../migrations/0004_billing_event_serving_mode.up.sql",
		"../../migrations/0005_invoice_grade_attempts.up.sql",
		// 0006 widens the rated_usage grain (serving_mode/owner_type/owner_id join
		// the natural key) and adds billing_event.graph_k8s_name, both of which
		// aggregateWindowSQL reads and writes.
		"../../migrations/0006_rollup_grain.up.sql",
		// 0007 makes rated_usage.serving_mode 'shared'/'dedicated' only (CHECK, no
		// default), which the rater's upsert must satisfy.
		"../../migrations/0007_serving_mode_explicit.up.sql",
		// 0008 adds the billing_event serving fields the rater reads (revision,
		// region); 0009 makes graph and revision rollup grain keys.
		"../../migrations/0008_billing_event_topology.up.sql",
		"../../migrations/0009_rollup_grain_graph_revision.up.sql",
		// 0010 adds the usage-identity attributes the rater writes.
		"../../migrations/0010_rated_usage_identity.up.sql",
		// 0011 makes the money columns nullable: the aggregator writes NULL.
		"../../migrations/0011_rated_usage_quantities_only.up.sql",
		// 0012: parked_usage (the rater parks conflicts), pushed_usage, pushed_hour.
		"../../migrations/0012_parked_and_pushed_usage.up.sql",
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
	// Same fixture convenience for the serving mode: most tests here are about
	// something other than serving mode and write dedicated traffic without naming
	// the column. Production billing_event has NO default (the proxy always writes
	// 'shared' or 'dedicated'); tests that exercise the serving mode, including the
	// NULL/'' pre-cutover evidence, write the column explicitly.
	b.WriteString("ALTER TABLE billing_event ALTER COLUMN serving_mode SET DEFAULT 'dedicated';\n")
	return b.String()
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

// openRatingSchema opens the integration database with the rating schema applied in
// a fresh schema named sch, dropped again when the test ends. It skips the test when
// PHOEBE_TEST_DATABASE_URL is unset.
func openRatingSchema(t *testing.T, sch string) *sql.DB {
	t.Helper()
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	t.Cleanup(func() {
		exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
		_ = db.Close()
	})
	exec(t, db, ratingSchemaDDL(t))
	return db
}

// readMigration returns the text of one real migration file.
func readMigration(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("../../migrations", name))
	if err != nil {
		t.Fatalf("read migration %s: %v", name, err)
	}
	return string(b)
}

// TestIntegration_Migration0007NoticesEveryRatedRowItDeletes pins the audit trail
// for the two rated_usage clean-ups in 0007 up: before deleting a ” row that has a
// 'dedicated' twin, or a row with an unknown serving_mode, the migration must RAISE
// NOTICE with the row count, SUM(cost), SUM(event_count) and earliest window_start
// of exactly the rows it deletes; the twin NOTICE must also say the hours MUST be
// re-rated. A clean database (nothing to delete) must produce no such NOTICE.
func TestIntegration_Migration0007NoticesEveryRatedRowItDeletes(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	var notices []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		if strings.HasPrefix(n.Message, "0007:") {
			notices = append(notices, n.Message)
		}
	}
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	// One pooled connection so SET search_path sticks and every NOTICE reaches
	// the callback above.
	db.SetMaxOpenConns(1)

	setup := func(sch string) {
		t.Helper()
		exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
		exec(t, db, "CREATE SCHEMA "+sch)
		exec(t, db, "SET search_path TO "+sch)
		for _, f := range []string{"0001_billing_event", "0002_rating", "0004_billing_event_serving_mode",
			"0005_invoice_grade_attempts", "0006_rollup_grain"} {
			exec(t, db, readMigration(t, f+".up.sql"))
		}
	}
	hour := mustTime("2026-06-08T10:00:00Z")
	seed := func(auth, mode, cost string, events int) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `INSERT INTO rated_usage
			(id, auth_id, resource_id, org_id, model_id, serving_mode, window_start, window_end,
			 prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens,
			 cost, applied_prompt_rate, applied_cached_rate, applied_completion_rate, event_count)
			VALUES (md5($1 || $2), $1, 'res', 'org-1', 'm', $2, $3::timestamptz, $3::timestamptz + interval '1 hour',
			        100, 0, 0, 100, $4::numeric, 0.00001, 0, 0, $5)`, auth, mode, hour, cost, events); err != nil {
			t.Fatalf("seed rated_usage (%s, %q): %v", auth, mode, err)
		}
	}

	const sch = "phoebe_migration_0007_notice_it"
	setup(sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	seed("a-ded", "", "0.500000000", 7)       // renamed, not deleted: no NOTICE
	seed("a-shr", "shared", "0.250000000", 3) // untouched
	seed("a-twin", "", "1.250000000", 4)      // deleted: 'dedicated' twin exists
	seed("a-twin2", "", "0.750000000", 6)     // deleted: 'dedicated' twin exists
	seed("a-twin", "dedicated", "9.000000000", 9)
	seed("a-twin2", "dedicated", "9.000000000", 9)
	seed("a-bogus", "bogus", "2.000000000", 5)   // deleted: unknown value
	seed("a-bogus2", "Shared", "0.125000000", 1) // deleted: unknown value (case matters)

	exec(t, db, readMigration(t, "0007_serving_mode_explicit.up.sql"))

	want := []string{
		"0007: deleting 2 rated_usage rows with serving_mode '' that have a 'dedicated' twin (sum cost 2.000000000, sum event_count 10, earliest window_start 2026-06-08T10:00:00Z); " +
			"the 'dedicated' twins are partial until re-rated, so every affected hour MUST be re-rated: rater --since <earliest window_start> --until <now>, then token-push over the same window",
		"0007: deleting 2 rated_usage rows with an unknown serving_mode (sum cost 2.125000000, sum event_count 6, earliest window_start 2026-06-08T10:00:00Z)",
	}
	if strings.Join(notices, "\n") != strings.Join(want, "\n") {
		t.Fatalf("0007 notices =\n%s\nwant\n%s", strings.Join(notices, "\n"), strings.Join(want, "\n"))
	}
	var left int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM rated_usage`).Scan(&left); err != nil {
		t.Fatalf("count after up: %v", err)
	}
	if left != 4 {
		t.Fatalf("rated_usage rows after up = %d, want 4 (a-ded, a-shr and the two 'dedicated' twins)", left)
	}

	// Nothing to delete: no audit NOTICE at all.
	notices = nil
	const clean = "phoebe_migration_0007_notice_clean_it"
	setup(clean)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+clean+" CASCADE") }()
	seed("a-ded", "", "0.500000000", 7)
	seed("a-shr", "shared", "0.250000000", 3)
	exec(t, db, readMigration(t, "0007_serving_mode_explicit.up.sql"))
	if len(notices) != 0 {
		t.Fatalf("0007 on a database with nothing to delete raised %v, want no NOTICE", notices)
	}
}

// TestIntegration_Migration0007ServingModeExplicit applies the real 0001–0006 DDL,
// seeds rated_usage the way the pre-0007 rater wrote it, then runs 0007 up and down.
//
// Up must: rename ” to 'dedicated' with the id the CURRENT rater computes (proved
// by deleting the row and re-rating the same billing_event evidence: the rater must
// mint the identical id); drop a ” row whose 'dedicated' twin already exists; drop
// a row with an unknown value; leave 'shared' untouched; then reject ” via the
// CHECK and reject a missing value (no default).
// Down must: rename 'dedicated' back to ” with the pre-0007 id and allow ” again.
func TestIntegration_Migration0007ServingModeExplicit(t *testing.T) {
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
	// One pooled connection so SET search_path sticks for every statement.
	db.SetMaxOpenConns(1)

	const sch = "phoebe_migration_0007_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	for _, f := range []string{"0001_billing_event", "0002_rating", "0004_billing_event_serving_mode",
		"0005_invoice_grade_attempts", "0006_rollup_grain"} {
		exec(t, db, readMigration(t, f+".up.sql"))
	}
	exec(t, db, "ALTER TABLE billing_event ALTER COLUMN usage_found SET DEFAULT TRUE")

	hour := mustTime("2026-06-08T10:00:00Z")
	// idExpr is the rater's md5 natural-key expression over a given serving_mode.
	idExpr := func(mode string) string {
		return `md5(length(auth_id)::text || ':' || auth_id
		  || '|' || length(owner_type)::text || ':' || owner_type
		  || '|' || length(owner_id)::text || ':' || owner_id
		  || '|' || length(resource_id)::text || ':' || resource_id
		  || '|' || length(model_id)::text || ':' || model_id
		  || '|' || length('` + mode + `')::text || ':' || '` + mode + `'
		  || '|' || extract(epoch FROM window_start)::bigint::text)`
	}
	seed := func(auth, mode string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `INSERT INTO rated_usage
			(id, auth_id, resource_id, org_id, model_id, serving_mode, window_start, window_end,
			 prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens,
			 cost, applied_prompt_rate, applied_cached_rate, applied_completion_rate, event_count)
			VALUES (md5($1 || $2), $1, 'res', 'org-1', 'm', $2, $3::timestamptz, $3::timestamptz + interval '1 hour',
			        100, 0, 0, 100, 0.001, 0.00001, 0, 0, 1)`, auth, mode, hour); err != nil {
			t.Fatalf("seed rated_usage (%s, %q): %v", auth, mode, err)
		}
	}
	seed("a-ded", "") // renamed to 'dedicated'
	seed("a-shr", "shared")
	seed("a-twin", "") // dropped: its 'dedicated' twin exists
	seed("a-twin", "dedicated")
	seed("a-bogus", "bogus") // dropped: no legal meaning
	// Give the renamed row an old-formula id, exactly as the pre-0007 rater wrote it.
	exec(t, db, "UPDATE rated_usage SET id = "+idExpr("")+" WHERE auth_id = 'a-ded'")

	exec(t, db, readMigration(t, "0007_serving_mode_explicit.up.sql"))

	rows := map[string]string{}
	r, err := db.QueryContext(ctx, `SELECT auth_id || '/' || serving_mode, id FROM rated_usage`)
	if err != nil {
		t.Fatalf("read after up: %v", err)
	}
	for r.Next() {
		var k, id string
		if err := r.Scan(&k, &id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		rows[k] = id
	}
	r.Close()
	if len(rows) != 3 || rows["a-ded/dedicated"] == "" || rows["a-shr/shared"] == "" || rows["a-twin/dedicated"] == "" {
		t.Fatalf("rows after up = %v, want exactly a-ded/dedicated, a-shr/shared, a-twin/dedicated", rows)
	}
	migratedID := rows["a-ded/dedicated"]

	// The migrated id must equal the 7-field natural-key formula the rater minted at
	// 0007. (The rater itself has since moved to 0009's 9-field key; that migration's
	// own test checks the rater-equals-migration property against today's rater.)
	var formulaID string
	if err := db.QueryRowContext(ctx, `SELECT md5(length(auth_id)::text || ':' || auth_id
          || '|' || length(owner_type)::text || ':' || owner_type
          || '|' || length(owner_id)::text || ':' || owner_id
          || '|' || length(resource_id)::text || ':' || resource_id
          || '|' || length(model_id)::text || ':' || model_id
          || '|' || length(serving_mode)::text || ':' || serving_mode
          || '|' || extract(epoch FROM window_start)::bigint::text)
		FROM rated_usage WHERE auth_id = 'a-ded'`).Scan(&formulaID); err != nil {
		t.Fatalf("compute formula id: %v", err)
	}
	if formulaID != migratedID {
		t.Fatalf("migration wrote %s, the 0007 natural-key formula gives %s", migratedID, formulaID)
	}

	// The CHECK rejects the retired spelling, and there is no default to fall back on.
	if _, err := db.ExecContext(ctx, `INSERT INTO rated_usage
		(id, auth_id, resource_id, model_id, serving_mode, window_start, window_end,
		 prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens, cost, event_count)
		VALUES ('x1', 'z', 'res', 'm', '', $1::timestamptz, $1::timestamptz + interval '1 hour', 0, 0, 0, 0, 0, 0)`, hour); err == nil ||
		!strings.Contains(err.Error(), "rated_usage_serving_mode_ck") {
		t.Fatalf("insert serving_mode='' err = %v, want the rated_usage_serving_mode_ck violation", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO rated_usage
		(id, auth_id, resource_id, model_id, window_start, window_end,
		 prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens, cost, event_count)
		VALUES ('x2', 'z', 'res', 'm', $1::timestamptz, $1::timestamptz + interval '1 hour', 0, 0, 0, 0, 0, 0)`, hour); err == nil ||
		!strings.Contains(err.Error(), "serving_mode") {
		t.Fatalf("insert without serving_mode err = %v, want a NOT NULL violation (no default)", err)
	}

	exec(t, db, readMigration(t, "0007_serving_mode_explicit.down.sql"))
	var downMode, downID, wantID string
	if err := db.QueryRowContext(ctx,
		`SELECT serving_mode, id, `+idExpr("")+` FROM rated_usage WHERE auth_id = 'a-ded'`).
		Scan(&downMode, &downID, &wantID); err != nil {
		t.Fatalf("read after down: %v", err)
	}
	if downMode != "" || downID != wantID {
		t.Fatalf("after down: mode=%q id=%s, want '' with the pre-0007 id %s", downMode, downID, wantID)
	}
	// Down restores the 0005 view shape: the 0007 column is gone.
	var viewCols int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema=$1 AND table_name='billing_reconciliation_hourly'
		  AND column_name='invalid_serving_mode_attempts'`, sch).Scan(&viewCols); err != nil {
		t.Fatalf("inspect view after down: %v", err)
	}
	if viewCols != 0 {
		t.Fatalf("billing_reconciliation_hourly still has invalid_serving_mode_attempts after 0007 down")
	}
	// '' is legal again after down.
	exec(t, db, `INSERT INTO rated_usage
		(id, auth_id, resource_id, model_id, window_start, window_end,
		 prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens, cost, event_count)
		VALUES ('x3', 'z', 'res', 'm', '2026-06-08T10:00:00Z', '2026-06-08T11:00:00Z', 0, 0, 0, 0, 0, 0)`)
}

// TestIntegration_Migration0009DownNoticesEveryRatedRowItDeletes pins the audit
// trail for 0009 down's lossy DELETE: before removing the rollups that differ from a
// kept row only in graph or topology revision, the migration must RAISE NOTICE with
// the row count, SUM(cost), SUM(event_count) and earliest window_start of exactly the
// rows it deletes, and say the hours MUST be re-rated. A database with nothing to
// delete must produce no such NOTICE.
func TestIntegration_Migration0009DownNoticesEveryRatedRowItDeletes(t *testing.T) {
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres conformance")
	}
	ctx := context.Background()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	var notices []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		if strings.HasPrefix(n.Message, "0009 down:") {
			notices = append(notices, n.Message)
		}
	}
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	// One pooled connection so SET search_path sticks and every NOTICE reaches
	// the callback above.
	db.SetMaxOpenConns(1)

	setup := func(sch string) {
		t.Helper()
		exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
		exec(t, db, "CREATE SCHEMA "+sch)
		exec(t, db, "SET search_path TO "+sch)
		for _, f := range []string{"0001_billing_event", "0002_rating", "0004_billing_event_serving_mode",
			"0005_invoice_grade_attempts", "0006_rollup_grain", "0007_serving_mode_explicit",
			"0008_billing_event_topology", "0009_rollup_grain_graph_revision",
			"0010_rated_usage_identity"} {
			exec(t, db, readMigration(t, f+".up.sql"))
		}
	}
	hour := mustTime("2026-06-08T10:00:00Z")
	// idKey fixes which row survives: down keeps the lowest id per 0007 key.
	seed := func(idKey, auth, graph string, revision int, at time.Time, cost string, events int) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `INSERT INTO rated_usage
			(id, auth_id, resource_id, org_id, model_id, serving_mode, graph_k8s_name, topology_revision, window_start, window_end,
			 prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens,
			 cost, applied_prompt_rate, applied_cached_rate, applied_completion_rate, event_count)
			VALUES ($1, $2, 'res', 'org-1', 'm', 'dedicated', $3, $4, $5::timestamptz, $5::timestamptz + interval '1 hour',
			        100, 0, 0, 100, $6::numeric, 0.00001, 0, 0, $7)`, idKey, auth, graph, revision, at, cost, events); err != nil {
			t.Fatalf("seed rated_usage %s: %v", idKey, err)
		}
	}

	const sch = "phoebe_migration_0009_down_notice_it"
	setup(sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	// Same hour, same 0007 key, differ only in graph: 'b' is deleted.
	seed("g-a", "a-graph", "dgd-a", 0, hour, "0.500000000", 5)
	seed("g-b", "a-graph", "dgd-b", 0, hour, "1.250000000", 4)
	// Next hour, differ only in revision: 'r2' is deleted.
	seed("r-1", "a-rev", "dgd-a", 1, hour.Add(time.Hour), "0.100000000", 1)
	seed("r-2", "a-rev", "dgd-a", 2, hour.Add(time.Hour), "0.750000000", 6)
	// Alone in its 0007 key: untouched.
	seed("solo", "a-solo", "dgd-c", 3, hour, "9.000000000", 9)

	exec(t, db, readMigration(t, "0009_rollup_grain_graph_revision.down.sql"))

	want := []string{
		"0009 down: deleting 2 rated_usage rows that differ from a kept row only in graph or topology revision " +
			"(sum cost 2.000000000, sum event_count 10, earliest window_start 2026-06-08T10:00:00Z); " +
			"the kept rows are partial until re-rated, so every affected hour MUST be re-rated: " +
			"rater --since <earliest window_start> --until <now>, then token-push over the same window",
	}
	if strings.Join(notices, "\n") != strings.Join(want, "\n") {
		t.Fatalf("0009 down notices =\n%s\nwant\n%s", strings.Join(notices, "\n"), strings.Join(want, "\n"))
	}
	var left int64
	var leftCost string
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), SUM(cost)::text FROM rated_usage`).Scan(&left, &leftCost); err != nil {
		t.Fatalf("count after down: %v", err)
	}
	// The NOTICE must describe exactly the rows removed: 12.6 seeded - 2.0 noticed.
	if left != 3 || leftCost != "9.600000000" {
		t.Fatalf("rated_usage after down = %d rows, sum cost %s; want 3 rows, 9.600000000", left, leftCost)
	}

	// Nothing to delete: no audit NOTICE at all.
	notices = nil
	const clean = "phoebe_migration_0009_down_notice_clean_it"
	setup(clean)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+clean+" CASCADE") }()
	seed("g-a", "a-graph", "dgd-a", 0, hour, "0.500000000", 5)
	seed("solo", "a-solo", "dgd-c", 3, hour, "9.000000000", 9)
	exec(t, db, readMigration(t, "0009_rollup_grain_graph_revision.down.sql"))
	if len(notices) != 0 {
		t.Fatalf("0009 down on a database with nothing to delete raised %v, want no NOTICE", notices)
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
		 cost, applied_prompt_rate, applied_cached_rate, applied_completion_rate, event_count,
		 serving_mode)
		SELECT
		    md5(a::text || ':' || h::text),
		    'auth' || a::text,
		    'deploy' || a::text,
		    'm',
		    '2026-01-01T00:00:00Z'::timestamptz + (h || ' hours')::interval,
		    '2026-01-01T01:00:00Z'::timestamptz + (h || ' hours')::interval,
		    100, 0, 0, 100, 0.001, 0.00001, 0, 0, 1,
		    'dedicated'
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

// TestIntegration_ReconciliationViewOneRowPerGrainAcrossServingModes guards the
// invariant that billing_reconciliation_hourly returns exactly one row per
// (window, auth, resource, model) even though rated_usage (since 0006) can hold
// several rows inside that grain, one per serving_mode and owner. Without the
// aggregation in the view's `rated` CTE, the LEFT JOIN repeats the raw row once per
// rated row, double-counting raw_attempts and invalid_serving_mode_attempts. The
// invariant must hold after 0007 up and after 0007 down.
func TestIntegration_ReconciliationViewOneRowPerGrainAcrossServingModes(t *testing.T) {
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
	db.SetMaxOpenConns(1)

	const sch = "phoebe_reconciliation_view_grain_it"
	exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
	exec(t, db, "CREATE SCHEMA "+sch)
	exec(t, db, "SET search_path TO "+sch)
	defer func() { exec(t, db, "DROP SCHEMA IF EXISTS "+sch+" CASCADE") }()
	for _, f := range []string{"0001_billing_event", "0002_rating", "0004_billing_event_serving_mode",
		"0005_invoice_grade_attempts", "0006_rollup_grain"} {
		exec(t, db, readMigration(t, f+".up.sql"))
	}
	// The 0005 view's column types: 0007 up (CREATE OR REPLACE) must keep them,
	// and 0007 down must restore them.
	colTypes := func() string {
		t.Helper()
		r, err := db.QueryContext(ctx, `SELECT a.attname, format_type(a.atttypid, a.atttypmod)
			FROM pg_attribute a WHERE a.attrelid = 'billing_reconciliation_hourly'::regclass
			  AND a.attnum > 0 AND NOT a.attisdropped
			  AND a.attname <> 'invalid_serving_mode_attempts'
			ORDER BY a.attnum`)
		if err != nil {
			t.Fatalf("read view columns: %v", err)
		}
		defer r.Close()
		var b strings.Builder
		for r.Next() {
			var n, ty string
			if err := r.Scan(&n, &ty); err != nil {
				t.Fatalf("scan: %v", err)
			}
			b.WriteString(n + " " + ty + "\n")
		}
		return b.String()
	}
	types0005 := colTypes()

	exec(t, db, readMigration(t, "0007_serving_mode_explicit.up.sql"))
	if got := colTypes(); got != types0005 {
		t.Fatalf("view columns after 0007 up =\n%s\nwant the 0005 shape\n%s", got, types0005)
	}

	hour := mustTime("2026-06-08T10:00:00Z")
	if _, err := db.ExecContext(ctx,
		`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, serving_mode, usage_found, prompt_tokens, event_ts)
		 VALUES ('e1', 'a', 'res', 'org-1', 'm', 'dedicated', TRUE, 100, $1)`, hour.Add(time.Minute)); err != nil {
		t.Fatalf("seed billing_event: %v", err)
	}
	for _, row := range []struct {
		mode   string
		events int
	}{{"shared", 2}, {"dedicated", 3}} {
		if _, err := db.ExecContext(ctx, `INSERT INTO rated_usage
			(id, auth_id, resource_id, org_id, model_id, serving_mode, window_start, window_end,
			 prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens,
			 cost, applied_prompt_rate, applied_cached_rate, applied_completion_rate, event_count)
			VALUES (md5('a' || $1::text), 'a', 'res', 'org-1', 'm', $1::text, $2::timestamptz,
			        $2::timestamptz + interval '1 hour', $3, 0, 0, $3, 0.001, 0.00001, 0, 0, $4)`,
			row.mode, hour, 100*row.events, row.events); err != nil {
			t.Fatalf("seed rated_usage %q: %v", row.mode, err)
		}
	}

	check := func(stage string) {
		t.Helper()
		var rows, raw, rated, delta int64
		var cost string
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*), SUM(raw_attempts), SUM(rated_attempts),
			SUM(attempt_delta), SUM(rated_cost)::text
			FROM billing_reconciliation_hourly
			WHERE window_start=$1 AND auth_id='a' AND resource_id='res' AND model_id='m'`, hour).
			Scan(&rows, &raw, &rated, &delta, &cost); err != nil {
			t.Fatalf("%s: read view: %v", stage, err)
		}
		if rows != 1 || raw != 1 || rated != 5 || delta != -4 || cost != "0.002000000" {
			t.Fatalf("%s: rows=%d raw_attempts=%d rated_attempts=%d attempt_delta=%d rated_cost=%s, want 1/1/5/-4/0.002000000",
				stage, rows, raw, rated, delta, cost)
		}
	}
	check("after 0007 up")

	exec(t, db, readMigration(t, "0007_serving_mode_explicit.down.sql"))
	if got := colTypes(); got != types0005 {
		t.Fatalf("view columns after 0007 down =\n%s\nwant the 0005 shape\n%s", got, types0005)
	}
	// Down renames 'dedicated' to '': still two rated rows inside the grain.
	check("after 0007 down")
}

// ---------------------------------------------------------------------------
// Aggregator behaviour (billing unification: quantities only, no money).
// ---------------------------------------------------------------------------

var itHour = mustTime("2026-10-22T14:00:00Z")

// beRow is one billing_event row. Empty strings are written as NULL; the
// defaults (see row) describe one clean dedicated attempt.
type beRow struct {
	id                                        string
	auth, resource, org, model, base, adapter string
	user, group, mode, graph, region          string
	rev                                       int
	prompt, cached, completion                int
	usageFound                                bool
	aborted                                   bool
	status                                    int // 0 = NULL
	at                                        time.Time
}

var rowSeq int

func row(mod func(*beRow)) beRow {
	rowSeq++
	r := beRow{
		id: fmt.Sprintf("req-%d", rowSeq), auth: "auth-a", resource: "res-1", org: "org-1",
		model: "qwen-ep", base: "Qwen/Qwen2.5-32B-Instruct", user: "user-1", mode: "dedicated",
		graph: "pd-qwen", region: "us-central1", rev: 3,
		prompt: 100, cached: 10, completion: 20, usageFound: true, status: 200,
		at: itHour.Add(5 * time.Minute),
	}
	if mod != nil {
		mod(&r)
	}
	return r
}

func seed(t *testing.T, db *sql.DB, rows ...beRow) {
	t.Helper()
	for _, r := range rows {
		var status any
		if r.status != 0 {
			status = r.status
		}
		var rev any
		if r.rev != 0 {
			rev = r.rev
		}
		_, err := db.Exec(`INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, base_model, adapter,
			user_id, group_id, serving_mode, graph_k8s_name, topology_revision, region,
			prompt_tokens, cached_tokens, completion_tokens, usage_found, aborted, status_code, event_ts)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`,
			r.id, nullableStr(r.auth), nullableStr(r.resource), nullableStr(r.org), nullableStr(r.model),
			nullableStr(r.base), nullableStr(r.adapter), nullableStr(r.user), nullableStr(r.group),
			nullableStr(r.mode), nullableStr(r.graph), rev, nullableStr(r.region),
			r.prompt, r.cached, r.completion, r.usageFound, r.aborted, status, r.at)
		if err != nil {
			t.Fatalf("seed %s: %v", r.id, err)
		}
	}
}

func aggregate(t *testing.T, db *sql.DB, start, end time.Time) AggregateResult {
	t.Helper()
	res, err := NewPostgresStore(db).AggregateWindow(context.Background(), start, end)
	if err != nil {
		t.Fatalf("AggregateWindow: %v", err)
	}
	return res
}

// withheld sums every bucket an event can be kept out in.
func withheld(r AggregateResult) int64 {
	return r.MissingUsageEvents + r.InvalidUsageEvents + r.UnattributableEvents + r.InvalidServingModeEvents +
		r.OwnerConflictEvents + r.AmbiguousBaseEvents + r.AmbiguousOrgEvents + r.MissingOrgEvents
}

// assertPartition: every in-window billing_event row lands in exactly one
// place, aggregated or one withheld bucket.
func assertPartition(t *testing.T, r AggregateResult, total int) {
	t.Helper()
	if got := r.EventsAggregated + withheld(r); got != int64(total) {
		t.Fatalf("aggregated %d + withheld %d = %d, want every one of the %d events exactly once (%+v)",
			r.EventsAggregated, withheld(r), got, total, r)
	}
}

type rollupRow struct {
	auth, ownerType, ownerID, resource, model, mode, graph string
	rev                                                    int
	org, region, base, adapter                             sql.NullString
	prompt, cached, completion, billablePrompt, requests   int64
	cost, priceKey                                         sql.NullString
}

func readRollups(t *testing.T, db *sql.DB) []rollupRow {
	t.Helper()
	rows, err := db.Query(`SELECT auth_id, owner_type, owner_id, resource_id, model_id, serving_mode, graph_k8s_name,
		topology_revision, org_id, region, base_model, adapter, prompt_tokens, cached_tokens, completion_tokens,
		billable_prompt_tokens, event_count, cost::text, price_key
		FROM rated_usage ORDER BY auth_id, owner_id, resource_id, model_id, serving_mode, graph_k8s_name, topology_revision`)
	if err != nil {
		t.Fatalf("read rollups: %v", err)
	}
	defer rows.Close()
	var out []rollupRow
	for rows.Next() {
		var r rollupRow
		if err := rows.Scan(&r.auth, &r.ownerType, &r.ownerID, &r.resource, &r.model, &r.mode, &r.graph, &r.rev,
			&r.org, &r.region, &r.base, &r.adapter, &r.prompt, &r.cached, &r.completion, &r.billablePrompt,
			&r.requests, &r.cost, &r.priceKey); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestIntegration_AggregatorSumsQuantitiesPerRollupAndWritesNoMoney(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_sums_it")
	seed(t, db,
		row(func(r *beRow) { r.prompt, r.cached, r.completion = 100, 10, 20 }),
		row(func(r *beRow) { r.prompt, r.cached, r.completion = 50, 50, 5; r.at = itHour.Add(59 * time.Minute) }),
		row(func(r *beRow) { r.prompt, r.cached, r.completion = 7, 0, 3; r.adapter = "ckpt-1"; r.model = "ft-ep" }),
	)
	res := aggregate(t, db, itHour, itHour.Add(time.Hour))
	assertPartition(t, res, 3)
	if res.RollupsWritten != 2 || res.EventsAggregated != 3 || withheld(res) != 0 {
		t.Fatalf("result %+v, want 2 rollups from 3 events and nothing withheld", res)
	}
	got := readRollups(t, db)
	if len(got) != 2 {
		t.Fatalf("%d rollups, want 2", len(got))
	}
	ft, qwen := got[0], got[1]
	if qwen.model != "qwen-ep" || qwen.prompt != 150 || qwen.cached != 60 || qwen.completion != 25 || qwen.requests != 2 {
		t.Fatalf("qwen rollup %+v, want prompt 150 cached 60 completion 25 over 2 requests", qwen)
	}
	if qwen.billablePrompt != 90 {
		t.Fatalf("billable prompt %d, want 90 (100-10 + 50-50)", qwen.billablePrompt)
	}
	if ft.adapter.String != "ckpt-1" || qwen.adapter.Valid {
		t.Fatalf("adapters ft=%v qwen=%v, want ckpt-1 and NULL", ft.adapter, qwen.adapter)
	}
	for _, r := range got {
		if r.cost.Valid || r.priceKey.Valid {
			t.Fatalf("rollup %s carries money (cost %v, price_key %v); the manager prices, phoebe must write NULL", r.model, r.cost, r.priceKey)
		}
		if r.org.String != "org-1" || r.base.String != "Qwen/Qwen2.5-32B-Instruct" || r.region.String != "us-central1" ||
			r.ownerType != "user" || r.ownerID != "user-1" || r.mode != "dedicated" || r.graph != "pd-qwen" || r.rev != 3 {
			t.Fatalf("rollup carries the wrong identity: %+v", r)
		}
	}
}

// The task's grain test: the topology revision is a grain key, so one
// deployment's hour served by two revisions is two records.
func TestIntegration_TwoTopologyRevisionsAreTwoRollups(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_rev_it")
	seed(t, db,
		row(func(r *beRow) { r.rev = 3 }),
		row(func(r *beRow) { r.rev = 4 }),
		row(func(r *beRow) { r.rev = 4 }),
	)
	res := aggregate(t, db, itHour, itHour.Add(time.Hour))
	got := readRollups(t, db)
	if res.RollupsWritten != 2 || len(got) != 2 || got[0].rev != 3 || got[1].rev != 4 || got[0].requests != 1 || got[1].requests != 2 {
		t.Fatalf("rollups %+v, want revision 3 (1 request) and revision 4 (2 requests)", got)
	}
}

// Every other grain key also splits: auth (API key), owner, resource, model,
// serving mode, graph and hour.
func TestIntegration_EachGrainKeySplitsTheRollup(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_grain_it")
	seed(t, db,
		row(nil),
		row(func(r *beRow) { r.auth = "auth-b" }),
		row(func(r *beRow) { r.user, r.group = "", "group-1" }),
		row(func(r *beRow) { r.user = "" }),
		row(func(r *beRow) { r.resource = "res-2" }),
		row(func(r *beRow) { r.model = "other-ep" }),
		row(func(r *beRow) { r.mode = "shared" }),
		row(func(r *beRow) { r.graph = "pd-other" }),
		row(func(r *beRow) { r.at = itHour.Add(time.Hour + time.Minute) }),
	)
	res := aggregate(t, db, itHour, itHour.Add(2*time.Hour))
	assertPartition(t, res, 9)
	if res.RollupsWritten != 9 {
		t.Fatalf("%d rollups, want 9 (one per distinct grain)", res.RollupsWritten)
	}
}

// Ruling 2026-10-15: every forwarded inference attempt emits exactly one
// billing row (enforced in internal/proxy, TestControlRoutesWriteNoBillingRow).
// The aggregator's half of that invariant: every row is accounted for exactly
// once, and request_count is the number of attempt rows aggregated.
func TestIntegration_EveryAttemptRowIsAccountedForExactlyOnce(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_partition_it")
	rows := []beRow{
		row(nil), row(nil), // aggregated
		row(func(r *beRow) { r.usageFound, r.aborted, r.prompt, r.cached, r.completion = false, true, 0, 0, 0 }), // expected missing
		row(func(r *beRow) { r.usageFound, r.prompt, r.cached, r.completion = false, 0, 0, 0 }),                  // unexplained missing
		row(func(r *beRow) { r.cached = 500 }),      // invalid usage
		row(func(r *beRow) { r.auth = "" }),         // unattributable
		row(func(r *beRow) { r.mode = "SHARED" }),   // invalid serving mode
		row(func(r *beRow) { r.group = "group-1" }), // owner conflict
		row(func(r *beRow) { r.resource = "res-amb-base" }), row(func(r *beRow) { r.resource = "res-amb-base"; r.base = "other/base" }),
		row(func(r *beRow) { r.resource = "res-amb-org" }), row(func(r *beRow) { r.resource = "res-amb-org"; r.org = "org-2" }),
		row(func(r *beRow) { r.resource = "res-no-org"; r.org = "" }), // missing org
	}
	seed(t, db, rows...)
	res := aggregate(t, db, itHour, itHour.Add(time.Hour))
	assertPartition(t, res, len(rows))
	want := AggregateResult{
		RollupsWritten: 1, EventsAggregated: 2, MissingUsageEvents: 2, ExpectedMissingUsageEvents: 1,
		UnexplainedMissingUsageEvents: 1, InvalidUsageEvents: 1, UnattributableEvents: 1, InvalidServingModeEvents: 1,
		OwnerConflictEvents: 1, AmbiguousBaseEvents: 2, AmbiguousOrgEvents: 2, MissingOrgEvents: 1,
		ParkedWritten: 4, // one per conflicted identity-hour: owner conflict, ambiguous base, ambiguous org, missing org
	}
	if res != want {
		t.Fatalf("got  %+v\nwant %+v", res, want)
	}
	if got := readRollups(t, db); got[0].requests != 2 {
		t.Fatalf("request_count %d, want 2 (one per aggregated attempt row)", got[0].requests)
	}
}

// The wire requires auth_id, resource_id and model_id to be non-empty, so ”
// is as unattributable as NULL.
func TestIntegration_EmptyStringIdentifiersAreUnattributable(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_emptyid_it")
	exec(t, db, `INSERT INTO billing_event (request_id, auth_id, resource_id, org_id, model, base_model, user_id,
		serving_mode, prompt_tokens, usage_found, status_code, event_ts) VALUES
		('e1','','res-1','org-1','m','B','u','dedicated',5,true,200,'2026-10-22T14:05:00Z'),
		('e2','a','','org-1','m','B','u','dedicated',5,true,200,'2026-10-22T14:05:00Z'),
		('e3','a','res-1','org-1','','B','u','dedicated',5,true,200,'2026-10-22T14:05:00Z'),
		('e4','a','res-1','org-1','m','','u','dedicated',5,true,200,'2026-10-22T14:05:00Z')`)
	res := aggregate(t, db, itHour, itHour.Add(time.Hour))
	if res.UnattributableEvents != 3 || res.RollupsWritten != 1 || res.EmptyBaseModelEvents != 1 {
		t.Fatalf("%+v, want 3 unattributable and the empty-base-model event aggregated", res)
	}
}

// A record carries one base_model and one adapter, and the manager prices by
// them. Two of either, or adapter on some events and not others, withholds
// the identity's hour, even across a graph-revision boundary; a neighbouring
// identity is untouched.
func TestIntegration_AmbiguousBaseModelOrAdapterWithholdsTheIdentity(t *testing.T) {
	cases := map[string][]beRow{
		"two base models":           {row(nil), row(func(r *beRow) { r.base = "other/base" })},
		"two adapters":              {row(func(r *beRow) { r.adapter = "ckpt-1" }), row(func(r *beRow) { r.adapter = "ckpt-2" })},
		"adapter and no adapter":    {row(nil), row(func(r *beRow) { r.adapter = "ckpt-1" })},
		"base model and no base":    {row(nil), row(func(r *beRow) { r.base = "" })},
		"across topology revisions": {row(func(r *beRow) { r.rev = 3 }), row(func(r *beRow) { r.rev = 4; r.base = "other/base" })},
	}
	for name, rows := range cases {
		t.Run(name, func(t *testing.T) {
			db := openRatingSchema(t, "phoebe_agg_ambbase_it")
			clean := row(func(r *beRow) { r.resource = "res-clean" })
			seed(t, db, append(rows, clean)...)
			res := aggregate(t, db, itHour, itHour.Add(time.Hour))
			assertPartition(t, res, len(rows)+1)
			if res.AmbiguousBaseEvents != int64(len(rows)) || res.RollupsWritten != 1 {
				t.Fatalf("%+v, want %d ambiguous-base events withheld and only the clean neighbour written", res, len(rows))
			}
			if got := readRollups(t, db); got[0].resource != "res-clean" {
				t.Fatalf("written %+v, want only res-clean", got)
			}
		})
	}
}

// The org pays. Two non-NULL orgs for one identity-hour withhold it (even
// across revisions); a partial NULL (events from before the org header) is not
// ambiguous and resolves to the one real org.
func TestIntegration_AmbiguousOrgWithholdsButAPartialNullOrgResolves(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_org_it")
	seed(t, db,
		row(func(r *beRow) { r.resource = "res-two-orgs"; r.rev = 3 }),
		row(func(r *beRow) { r.resource = "res-two-orgs"; r.rev = 4; r.org = "org-2" }),
		row(func(r *beRow) { r.resource = "res-partial"; r.org = "" }),
		row(func(r *beRow) { r.resource = "res-partial"; r.rev = 4 }),
	)
	res := aggregate(t, db, itHour, itHour.Add(time.Hour))
	assertPartition(t, res, 4)
	if res.AmbiguousOrgEvents != 2 || res.RollupsWritten != 2 {
		t.Fatalf("%+v, want 2 ambiguous-org events and the 2 partial-org rollups written", res)
	}
	for _, r := range readRollups(t, db) {
		if r.resource != "res-partial" || r.org.String != "org-1" {
			t.Fatalf("rollup %+v, want res-partial carrying org-1 in both revisions", r)
		}
	}
}

// Both-owner events are withheld per event: an innocent no-owner event in the
// same (”, ”) bucket still aggregates (one bad row must not zero someone
// else's usage).
func TestIntegration_OwnerConflictIsWithheldPerEventWithoutPoisoningItsBucket(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_owner_it")
	seed(t, db,
		row(func(r *beRow) { r.user, r.group = "user-1", "group-1" }),
		row(func(r *beRow) { r.user = "" }),
	)
	res := aggregate(t, db, itHour, itHour.Add(time.Hour))
	assertPartition(t, res, 2)
	got := readRollups(t, db)
	if res.OwnerConflictEvents != 1 || len(got) != 1 || got[0].ownerType != "" || got[0].requests != 1 {
		t.Fatalf("%+v rollups %+v, want 1 conflict withheld and the no-owner event aggregated alone", res, got)
	}
}

func TestIntegration_ConflictingRegionsAggregateToNullNotAGuess(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_region_it")
	seed(t, db,
		row(func(r *beRow) { r.region = "us-central1" }),
		row(func(r *beRow) { r.region = "eu-west1" }),
		row(func(r *beRow) { r.resource = "res-2"; r.region = "" }),
		row(func(r *beRow) { r.resource = "res-2"; r.region = "us-central1" }),
	)
	aggregate(t, db, itHour, itHour.Add(time.Hour))
	got := readRollups(t, db)
	if got[0].region.Valid || got[1].region.String != "us-central1" {
		t.Fatalf("regions %v / %v, want NULL for the disagreeing rollup and us-central1 where only NULLs disagree", got[0].region, got[1].region)
	}
}

// Re-running an hour is idempotent: same rows, same ids, nothing deleted.
func TestIntegration_ReRunningAnHourIsIdempotent(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_idem_it")
	seed(t, db, row(nil), row(func(r *beRow) { r.rev = 4 }), row(func(r *beRow) { r.mode = "shared" }))
	first := aggregate(t, db, itHour, itHour.Add(time.Hour))
	ids := map[string]bool{}
	rows, _ := db.Query(`SELECT id FROM rated_usage`)
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		ids[id] = true
	}
	rows.Close()
	before := readRollups(t, db)

	second := aggregate(t, db, itHour, itHour.Add(time.Hour))
	if second.ReconciledDeletions != 0 || second.RollupsWritten != first.RollupsWritten {
		t.Fatalf("re-run %+v after %+v, want the same rollups and no deletions", second, first)
	}
	after := readRollups(t, db)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("re-run changed rollups:\n%v\n%v", before, after)
	}
	var same int
	if err := db.QueryRow(`SELECT COUNT(*) FROM rated_usage WHERE id = ANY($1)`, keys(ids)).Scan(&same); err != nil || same != len(ids) {
		t.Fatalf("ids changed across the re-run (%d of %d kept, err %v)", same, len(ids), err)
	}
}

func keys(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// A late event is folded into its hour on the next run, and a rollup that
// becomes withheld is DELETED (so token-push stops sending it and the manager
// deletes it by absence). Hours outside the window are never touched.
func TestIntegration_ReRunFoldsLateEventsAndDeletesARollupThatBecameWithheld(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_reconcile_it")
	seed(t, db,
		row(nil),
		row(func(r *beRow) { r.resource = "res-2" }),
		row(func(r *beRow) { r.at = itHour.Add(-30 * time.Minute) }), // previous hour, outside the window
	)
	aggregate(t, db, itHour.Add(-time.Hour), itHour.Add(time.Hour))
	if n := len(readRollups(t, db)); n != 3 {
		t.Fatalf("%d rollups after the first run, want 3", n)
	}

	seed(t, db,
		row(func(r *beRow) { r.prompt = 1000 }),                       // late event, folded in
		row(func(r *beRow) { r.resource = "res-2"; r.org = "org-2" }), // res-2 becomes ambiguous
	)
	res := aggregate(t, db, itHour, itHour.Add(time.Hour))
	if res.ParkedDeletions != 1 || res.ReconciledDeletions != 0 || res.AmbiguousOrgEvents != 2 || res.ParkedWritten != 1 {
		t.Fatalf("%+v, want res-2 deleted because it is now parked as ambiguous, not as an unexplained deletion", res)
	}
	got := readRollups(t, db)
	if len(got) != 2 {
		t.Fatalf("%d rollups, want the previous hour's and res-1's", len(got))
	}
	for _, r := range got {
		if r.resource == "res-2" {
			t.Fatal("res-2 still present after becoming ambiguous")
		}
	}
	var prompt int64
	if err := db.QueryRow(`SELECT prompt_tokens FROM rated_usage WHERE resource_id='res-1' AND window_start=$1`, itHour).Scan(&prompt); err != nil || prompt != 1100 {
		t.Fatalf("res-1 prompt %d (err %v), want 1100 with the late event folded in", prompt, err)
	}
}

// No event names an org: the payer is unknown, so the identity is PARKED as
// missing_org (not written to rated_usage, so never sent with a blank payer).
// When an event with the org arrives, the next run resolves the parked row and
// aggregates the identity, carrying the org.
func TestIntegration_MissingOrgIsParkedThenAggregatedWhenTheOrgArrives(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_orgconv_it")
	seed(t, db, row(func(r *beRow) { r.org = "" }), row(func(r *beRow) { r.resource = "res-2" }))
	res := aggregate(t, db, itHour, itHour.Add(time.Hour))
	if res.MissingOrgEvents != 1 || res.ParkedWritten != 1 || res.RollupsWritten != 1 {
		t.Fatalf("%+v, want the org-less identity parked and its neighbour written", res)
	}
	seed(t, db, row(nil))
	res = aggregate(t, db, itHour, itHour.Add(time.Hour))
	if res.ParkedResolved != 1 || res.RollupsWritten != 2 {
		t.Fatalf("%+v, want the parked row resolved and both identities written", res)
	}
	for _, r := range readRollups(t, db) {
		if r.org.String != "org-1" {
			t.Fatalf("rollup %+v, want org-1", r)
		}
	}
}

// The upgrade path: rows the previous rater priced lose their money the first
// time the aggregator re-runs their hour, so no stale price lingers.
func TestIntegration_ReAggregatingAPricedRowClearsItsMoney(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_upgrade_it")
	seed(t, db, row(nil))
	aggregate(t, db, itHour, itHour.Add(time.Hour))
	exec(t, db, `UPDATE rated_usage SET cost = 1.5, applied_prompt_rate = 0.1, applied_cached_rate = 0.01,
		applied_completion_rate = 0.2, price_key = 'Qwen/Qwen2.5-32B-Instruct'`)
	aggregate(t, db, itHour, itHour.Add(time.Hour))
	var priced int
	if err := db.QueryRow(`SELECT COUNT(*) FROM rated_usage WHERE cost IS NOT NULL OR applied_prompt_rate IS NOT NULL
		OR applied_cached_rate IS NOT NULL OR applied_completion_rate IS NOT NULL OR price_key IS NOT NULL`).Scan(&priced); err != nil || priced != 0 {
		t.Fatalf("%d rows still carry money after re-aggregation (err %v)", priced, err)
	}
}

// Hour buckets do not depend on the session time zone.
func TestIntegration_HourBucketsAreSessionTimeZoneIndependent(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_tz_it")
	seed(t, db, row(func(r *beRow) { r.at = itHour.Add(59*time.Minute + 59*time.Second) }))
	exec(t, db, `SET TIME ZONE 'Asia/Kolkata'`) // +05:30: a half-hour offset
	aggregate(t, db, itHour, itHour.Add(time.Hour))
	var ws time.Time
	if err := db.QueryRow(`SELECT window_start FROM rated_usage`).Scan(&ws); err != nil || !ws.Equal(itHour) {
		t.Fatalf("window_start %v (err %v), want %v", ws, err, itHour)
	}
}

// Individually valid INTEGER counts can overflow int32 on prompt - cached;
// that must not fail the whole hour.
func TestIntegration_Int32OverflowOnFreshInputDoesNotFailTheHour(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_overflow_it")
	seed(t, db,
		row(func(r *beRow) { r.prompt, r.cached = 2147483647, -2147483648 }),
		row(func(r *beRow) { r.resource = "res-2" }),
	)
	res := aggregate(t, db, itHour, itHour.Add(time.Hour))
	if res.InvalidUsageEvents != 1 || res.RollupsWritten != 1 {
		t.Fatalf("%+v, want the malformed row withheld as invalid and the neighbour aggregated", res)
	}
}

// 0011 down refuses while rows without money exist (it will not invent $0),
// and succeeds once none are left.
func TestIntegration_Migration0011DownRefusesToInventMoney(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_m0011_it")
	seed(t, db, row(nil))
	aggregate(t, db, itHour, itHour.Add(time.Hour))

	down := readMigration(t, "0011_rated_usage_quantities_only.down.sql")
	if _, err := db.Exec(down); err == nil || !strings.Contains(err.Error(), "will not invent a $0 cost") {
		t.Fatalf("down with unpriced rows: err = %v, want the refusal", err)
	}
	exec(t, db, `UPDATE rated_usage SET cost = 1, applied_prompt_rate = 0, applied_cached_rate = 0, applied_completion_rate = 0`)
	exec(t, db, down)
	var nullable string
	if err := db.QueryRow(`SELECT is_nullable FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'rated_usage' AND column_name = 'cost'`).Scan(&nullable); err != nil || nullable != "NO" {
		t.Fatalf("cost nullable = %q (err %v) after down, want NO", nullable, err)
	}
	exec(t, db, readMigration(t, "0011_rated_usage_quantities_only.up.sql"))
}

// D3: a dedicated endpoint may serve a model with no catalog base, and Atlas
// stamps the header as `base_model | default(”)`. Such usage is aggregated
// (base_model NULL, sent as "") with a warning count, never withheld, and it
// does not hold back anything else in the hour.
func TestIntegration_EmptyBaseModelDedicatedEventIsAggregatedNotWithheld(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_emptybase_it")
	seed(t, db,
		row(func(r *beRow) { r.resource = "res-custom"; r.model = "my-own-model"; r.base = "" }),
		row(func(r *beRow) { r.resource = "res-custom"; r.model = "my-own-model"; r.base = "" }),
		row(nil),
	)
	res := aggregate(t, db, itHour, itHour.Add(time.Hour))
	assertPartition(t, res, 3)
	if res.RollupsWritten != 2 || res.EventsAggregated != 3 || res.EmptyBaseModelEvents != 2 || withheld(res) != 0 {
		t.Fatalf("%+v, want both rollups written, 2 empty-base events counted, nothing withheld", res)
	}
	for _, r := range readRollups(t, db) {
		if r.resource == "res-custom" && (r.base.Valid || r.requests != 2) {
			t.Fatalf("custom rollup %+v, want base_model NULL over 2 requests", r)
		}
	}
}

// 0012 down refuses while unresolved parked usage exists: parked_usage is the
// only record of that unbilled usage.
func TestIntegration_Migration0012DownRefusesWhileUsageIsParked(t *testing.T) {
	db := openRatingSchema(t, "phoebe_agg_m0012_it")
	seed(t, db, row(nil), row(func(r *beRow) { r.org = "org-2" }))
	if res := aggregate(t, db, itHour, itHour.Add(time.Hour)); res.ParkedWritten != 1 {
		t.Fatalf("%+v, want one parked row", res)
	}
	down := readMigration(t, "0012_parked_and_pushed_usage.down.sql")
	if _, err := db.Exec(down); err == nil || !strings.Contains(err.Error(), "unresolved") {
		t.Fatalf("down with parked usage: err = %v, want the refusal", err)
	}
	exec(t, db, `UPDATE parked_usage SET resolved_at = now()`)
	exec(t, db, down)
	exec(t, db, readMigration(t, "0012_parked_and_pushed_usage.up.sql"))
}
