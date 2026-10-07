//go:build integration

// Package main integration test: runs buildAdmission's DATABASE_URL spend
// wiring against a LIVE Postgres loaded with the production migrations, so the
// "group spend caps enabled" startup line is proven to describe a working
// check — the unit test pins the two failure logs, but only this half
// exercises OpenPostgresSpendStore + WithGroupSpend together.
//
// Gated behind the `integration` build tag AND a non-empty
// PHOEBE_TEST_DATABASE_URL. Run with:
//
//	PHOEBE_TEST_DATABASE_URL=postgres://... go test -tags=integration ./cmd/interceptor/...
package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/migrations"
)

// newSpendWiringHarness creates an isolated schema — named per test PROCESS so
// concurrent runs against a shared database stop stomping each other — with
// ALL production migrations applied (discovered from the embedded
// migrations.FS, ordered by filename; a hand-maintained list could go stale
// while green), and returns a pool pinned to it plus the schema-pinned DSN for
// handing to buildAdmission as DATABASE_URL.
func newSpendWiringHarness(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping interceptor spend-wiring integration test")
	}
	schema := fmt.Sprintf("phoebe_interceptor_spend_it_%d", os.Getpid())

	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
		_ = admin.Close()
	})
	execSpendWiring(t, admin, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	execSpendWiring(t, admin, "CREATE SCHEMA "+schema)

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	pinned := dsn + sep + "search_path=" + schema
	db, err := sql.Open("pgx", pinned)
	if err != nil {
		t.Fatalf("open schema pool: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		t.Fatalf("read migrations FS: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".up.sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		b, err := migrations.FS.ReadFile(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", err, name)
		}
		execSpendWiring(t, db, string(b))
	}
	return db, pinned
}

func execSpendWiring(t *testing.T, db *sql.DB, stmt string) {
	t.Helper()
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("exec failed: %v\nstatement: %s", err, stmt)
	}
}

// TestIntegration_BuildAdmissionGroupSpendCapsEnabled is the positive half of
// the DATABASE_URL wiring: against live Postgres, buildAdmission logs the
// "enabled" line AND the admitter it returns enforces the monthly spend cap —
// a spend-capped Admit whose group_usage spend reached the cap is the
// contractual monthly_spend rejection. A regression deleting the
// WithGroupSpend call admits this request, so the wiring's production switch
// is now mutation-covered, not just logged.
func TestIntegration_BuildAdmissionGroupSpendCapsEnabled(t *testing.T) {
	db, pinned := newSpendWiringHarness(t)
	t.Setenv("DATABASE_URL", pinned)
	mr := miniredis.RunT(t)
	var buf bytes.Buffer
	logger := &logging.Logger{Debug: log.New(io.Discard, "", 0), Info: log.New(&buf, "", 0), Warn: log.New(&buf, "", 0), Error: log.New(&buf, "", 0)}
	admitter, closeAdmission := buildAdmission(loadTestSettings(t, "emit:\n  valkeyAddr: "+mr.Addr()+"\n"), logger)
	defer closeAdmission()
	if admitter == nil {
		t.Fatal("no admitter built")
	}
	if out := buf.String(); !strings.Contains(out, "group spend caps enabled") {
		t.Fatalf("startup log missing the enabled line:\n%s", out)
	}

	// The group's month-to-date attribution spend is 10 — over the cap of 1 —
	// in the current UTC hour bucket, so the month predicate cannot skip it.
	_, err := db.Exec(`INSERT INTO group_usage (group_id, window_start, cost, event_count)
		VALUES ($1, $2, '10', 1)`, spendWiringGID, time.Now().UTC().Truncate(time.Hour))
	if err != nil {
		t.Fatalf("seed group_usage: %v", err)
	}

	req := spendWiringRequest(admission.GroupScope{GroupID: spendWiringGID, SpendCap: "1"})
	_, err = admitter.Admit(context.Background(), req)
	var rejected *admission.Rejected
	if !errors.As(err, &rejected) || !rejected.Contractual || rejected.Dimension != "monthly_spend" {
		t.Fatalf("spend-capped admit = %v, want a contractual monthly_spend rejection (the wired spend check reads group_usage)", err)
	}
}
