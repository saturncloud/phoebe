package drain

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/saturncloud/phoebe/internal/metering"
	"github.com/saturncloud/phoebe/migrations"
)

// schemaGapStore fails every Upsert with Postgres's undefined_column until
// migrated is set, like a new drainer writing a column the old table lacks,
// and reports a schema version for the startup gate.
type schemaGapStore struct {
	*fakeStore
	mu       sync.Mutex
	migrated bool
	version  uint
	dirty    bool
}

func (s *schemaGapStore) Upsert(ctx context.Context, events []metering.Event) error {
	s.mu.Lock()
	migrated := s.migrated
	s.mu.Unlock()
	if !migrated {
		return fmt.Errorf("drain: upsert %d events: %w", len(events), &pgconn.PgError{Code: "42703", Message: `column "region" does not exist`})
	}
	return s.fakeStore.Upsert(ctx, events)
}

func (s *schemaGapStore) SchemaVersion(context.Context) (uint, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version, s.dirty, nil
}

func (s *schemaGapStore) migrate(version uint) {
	s.mu.Lock()
	s.migrated, s.version = true, version
	s.mu.Unlock()
}

// The severity 2 finding (uk2, phoebe #69): a new drainer that runs before
// the migration must not drop events as poison when the table lacks a column.
// They stay pending and are stored once the schema catches up.
func TestDrain_SchemaGapRetriesInsteadOfPoisoning(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := &schemaGapStore{fakeStore: newFakeStore()}
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("e1"))
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("e2"))

	d := New(cfg, testLogger(), rdb, store)
	ctx := context.Background()
	if err := d.ensureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := d.readAndStore(ctx); err == nil {
		t.Fatal("a schema gap must surface as a store error, not succeed")
	}
	if d.Poisoned() != 0 {
		t.Fatalf("Poisoned() = %d, want 0: a schema gap is not a bad row", d.Poisoned())
	}
	if p := pendingCount(t, rdb, cfg.StreamName, cfg.Group); p != 2 {
		t.Fatalf("pending = %d, want both events left un-ACK'd for redelivery", p)
	}

	store.migrate(migrations.LatestVersion())
	d.cfg.ClaimMinIdle = 0
	d.reclaim(ctx)
	if store.rowCount() != 2 || pendingCount(t, rdb, cfg.StreamName, cfg.Group) != 0 {
		t.Fatalf("rows=%d pending=%d after the migration, want both stored and ACK'd",
			store.rowCount(), pendingCount(t, rdb, cfg.StreamName, cfg.Group))
	}
}

// The drainer consumes nothing until the database is at the schema version
// it writes, so an upgrade never writes new columns into the old table.
func TestDrain_WaitsForRequiredSchemaVersion(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	cfg.RequiredSchemaVersion = 9
	store := &schemaGapStore{fakeStore: newFakeStore(), version: 8}
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("e1"))

	d := New(cfg, testLogger(), rdb, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	time.Sleep(300 * time.Millisecond)
	store.mu.Lock()
	calls := store.calls
	store.mu.Unlock()
	if calls != 0 || store.rowCount() != 0 {
		t.Fatalf("calls=%d rows=%d while the schema is behind, want nothing written", calls, store.rowCount())
	}

	store.migrate(9)
	deadline := time.Now().Add(5 * time.Second)
	for store.rowCount() != 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if store.rowCount() != 1 {
		t.Fatal("the event was not drained after the schema reached the required version")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v after cancel", err)
	}
}

// A dirty schema (a failed migration) is not ready either.
func TestDrain_DirtySchemaIsNotReady(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	cfg.RequiredSchemaVersion = 9
	store := &schemaGapStore{fakeStore: newFakeStore(), version: 9, dirty: true}
	d := New(cfg, testLogger(), rdb, store)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := d.waitForSchema(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForSchema = %v, want it still waiting on a dirty schema", err)
	}
}

// QA's B22 drill: Valkey restarted without persistence, wiping the stream and
// its consumer group; the drainer looped on NOGROUP until a pod restart. It
// now recreates the group and keeps draining.
func TestDrain_RecreatesMissingConsumerGroup(t *testing.T) {
	mr, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := newFakeStore()
	d := New(cfg, testLogger(), rdb, store)
	ctx := context.Background()
	if err := d.ensureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	mr.Del(cfg.StreamName) // the wipe: stream and group gone

	if _, err := d.readAndStore(ctx); err != nil {
		t.Fatalf("readAndStore after the wipe = %v, want the group recreated", err)
	}
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("after-wipe"))
	if _, err := d.readAndStore(ctx); err != nil {
		t.Fatal(err)
	}
	if store.rowCount() != 1 {
		t.Fatalf("rows=%d, want the event emitted after the wipe drained", store.rowCount())
	}
}

func TestLatestVersionMatchesEmbeddedMigrations(t *testing.T) {
	if got := migrations.LatestVersion(); got < 9 {
		t.Fatalf("LatestVersion() = %d, want the highest embedded NNNN prefix (>= 9)", got)
	}
}

func TestIsSchemaNotReady(t *testing.T) {
	for code, want := range map[string]bool{"42703": true, "42P01": true, "42P10": true, "23505": false, "22001": false} {
		err := fmt.Errorf("drain: upsert 1 events: %w", &pgconn.PgError{Code: code})
		if got := isSchemaNotReady(err); got != want {
			t.Errorf("code %s: isSchemaNotReady = %v, want %v", code, got, want)
		}
	}
	if isSchemaNotReady(errors.New("plain")) {
		t.Error("a non-Postgres error is not a schema gap")
	}
}
