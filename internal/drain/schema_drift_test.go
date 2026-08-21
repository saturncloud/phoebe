package drain

import (
	"regexp"
	"strings"
	"testing"

	"github.com/saturncloud/phoebe/migrations"
)

// TestUpsertColumnsExistInMigrationDDL pins insert-columns ⊆ migration-DDL
// columns for billing_event, against the SAME embedded migrations cmd/migrate
// applies. This is the static guard for a live-verified failure class: the
// drainer's INSERT referenced serving_mode before any phoebe migration created
// it, so on a fresh phoebe DB EVERY event poison-dropped with SQLSTATE 42703
// (undefined column) — served-but-never-billed — until the column was
// hand-ALTERed in on staging. The full-pipeline proof lives in internal/e2e
// (real migrations + real INSERT against Postgres), but that lane is
// integration-gated; THIS test keeps the contract in the unit lane, where the
// drift would have been caught at commit time.
func TestUpsertColumnsExistInMigrationDDL(t *testing.T) {
	ddl := billingEventDDLColumns(t)
	for _, col := range upsertColumns {
		if !ddl[col] {
			t.Errorf("drainer inserts %q but no embedded migration creates billing_event.%q — a fresh DB poison-drops every event with SQLSTATE 42703; add the column to the migration chain", col, col)
		}
	}
}

// billingEventDDLColumns extracts every billing_event column the embedded up
// migrations create — the CREATE TABLE block plus any ALTER TABLE ... ADD
// COLUMN. The parser is deliberately pinned to this repo's migration style
// (one column per line, `--` comments, uppercase type keywords) and fails the
// test loudly if it stops recognizing the DDL, rather than silently matching
// nothing.
func billingEventDDLColumns(t *testing.T) map[string]bool {
	t.Helper()
	cols := map[string]bool{}

	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		t.Fatalf("read embedded migrations: %v", err)
	}
	createRe := regexp.MustCompile(`(?is)CREATE TABLE billing_event\s*\((.*?)\);`)
	// A column definition line: identifier then a type keyword. CONSTRAINT/
	// comment lines don't match (their first token is uppercase or --).
	colRe := regexp.MustCompile(`(?m)^\s*([a-z_]+)\s+(?:VARCHAR|CHAR|TEXT|INTEGER|BIGINT|SMALLINT|BOOLEAN|NUMERIC|TIMESTAMPTZ|TIMESTAMP|DATE|JSONB|TSVECTOR)`)
	alterRe := regexp.MustCompile(`(?i)ALTER TABLE billing_event\s+ADD COLUMN\s+(?:IF NOT EXISTS\s+)?([a-z_]+)`)

	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".up.sql") {
			continue
		}
		b, err := migrations.FS.ReadFile(e.Name())
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		sql := string(b)
		for _, m := range createRe.FindAllStringSubmatch(sql, -1) {
			for _, c := range colRe.FindAllStringSubmatch(m[1], -1) {
				cols[c[1]] = true
			}
		}
		for _, m := range alterRe.FindAllStringSubmatch(sql, -1) {
			cols[strings.ToLower(m[1])] = true
		}
	}

	// Parser sanity: the CREATE TABLE must have been found and recognizably
	// parsed (billing_event has had >= 15 columns since 0001). A style change
	// that breaks the parser must fail HERE, not let drift through unmatched.
	if !cols["request_id"] || len(cols) < 15 {
		t.Fatalf("migration DDL parser recognized only %d billing_event columns (%v) — the parser no longer matches the migration style; fix it rather than trusting a silent pass", len(cols), cols)
	}
	return cols
}
