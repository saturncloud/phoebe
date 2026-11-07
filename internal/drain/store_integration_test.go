//go:build integration

// Live-Postgres tests for the probe write. Gated behind the `integration` build
// tag AND PHOEBE_TEST_DATABASE_URL, like the other integration lanes. Run with:
//
//	PHOEBE_TEST_DATABASE_URL=postgres://... go test -tags=integration ./internal/drain/...
package drain

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/saturncloud/phoebe/internal/metering"
	"github.com/saturncloud/phoebe/migrations"
)

// migratedStore returns a PostgresStore on a fresh schema with every embedded
// up migration applied, and the pool it uses. The schema is dropped at the end.
func migratedStore(t *testing.T, schema string) (*PostgresStore, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping live-Postgres probe test")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
		_ = admin.Close()
	})
	// btree_gist lives in public so dropping this schema cannot remove it from
	// under another package's integration test.
	for _, q := range []string{"CREATE EXTENSION IF NOT EXISTS btree_gist", "DROP SCHEMA IF EXISTS " + schema + " CASCADE", "CREATE SCHEMA " + schema} {
		if _, err := admin.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	db, err := sql.Open("pgx", dsn+sep+"search_path="+schema+",public")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ups, err := fs.Glob(migrations.FS, "*.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(ups)
	for _, name := range ups {
		b, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(string(b)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	return NewPostgresStore(db), db
}

func billingEventRows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM billing_event`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestIntegration_ProbeWriteLeavesNoRowAndFailsOnARejectingConstraint: against
// the real schema the probe succeeds and leaves billing_event empty. Once a
// constraint that rejects every new row is added, the probe fails, so the
// drainer would pause rather than drop rows.
func TestIntegration_ProbeWriteLeavesNoRowAndFailsOnARejectingConstraint(t *testing.T) {
	store, db := migratedStore(t, "drain_probe_it")
	ctx := context.Background()

	for range 3 {
		if err := store.ProbeWrite(ctx); err != nil {
			t.Fatalf("ProbeWrite on the migrated schema: %v", err)
		}
	}
	if n := billingEventRows(t, db); n != 0 {
		t.Fatalf("billing_event has %d rows after probes, want 0", n)
	}

	if _, err := db.Exec(`ALTER TABLE billing_event ADD CONSTRAINT reject_all_ck CHECK (false) NOT VALID`); err != nil {
		t.Fatal(err)
	}
	err := store.ProbeWrite(ctx)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("ProbeWrite with a rejecting constraint = %v, want check_violation (23514)", err)
	}
	if n := billingEventRows(t, db); n != 0 {
		t.Fatalf("billing_event has %d rows, want 0", n)
	}
}

// TestIntegration_RowAtATimeUsesTheProbeAgainstRealPostgres: with a real store, a
// lone row the table cannot hold (a resource_id longer than VARCHAR(64)) is
// dropped as poison because the probe write succeeds; with a constraint that
// rejects every row, the same kind of lone row is kept and the batch is a store
// failure.
func TestIntegration_RowAtATimeUsesTheProbeAgainstRealPostgres(t *testing.T) {
	store, db := migratedStore(t, "drain_probe_rows_it")
	ctx := context.Background()
	d := New(DefaultConfig(), testLogger(), nil, store)

	bad := metering.Event{RequestID: "too-long-resource", ResourceID: strings.Repeat("r", 65), Model: "m", ServingMode: "dedicated"}
	ackable, stored, err := d.storeBatch(ctx, []metering.Event{bad}, []string{"1-0"})
	if err != nil || stored != 0 || len(ackable) != 1 || d.Poisoned() != 1 {
		t.Fatalf("bad row with a working store: ackable=%v stored=%d err=%v poisoned=%d, want dropped as poison", ackable, stored, err, d.Poisoned())
	}

	if _, err := db.Exec(`ALTER TABLE billing_event ADD CONSTRAINT reject_all_ck CHECK (false) NOT VALID`); err != nil {
		t.Fatal(err)
	}
	good := metering.Event{RequestID: "good", Model: "m", ServingMode: "dedicated", StatusCode: 200}
	ackable, stored, err = d.storeBatch(ctx, []metering.Event{good}, []string{"2-0"})
	if err == nil || stored != 0 || len(ackable) != 0 || d.Poisoned() != 1 {
		t.Fatalf("row with a rejecting store: ackable=%v stored=%d err=%v poisoned=%d, want a store failure that keeps the row", ackable, stored, err, d.Poisoned())
	}
	if n := billingEventRows(t, db); n != 0 {
		t.Fatalf("billing_event has %d rows, want 0", n)
	}
}
