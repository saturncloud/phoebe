package drain

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
	"github.com/saturncloud/phoebe/migrations"
)

// schemaGapStore fails every Upsert with Postgres's undefined_column until
// migrated is set, like a new drainer writing a column the old table lacks,
// and reports a schema version for the startup gate. attempts counts every
// Upsert call, including the ones that fail, so a test can prove the gate
// held without relying on fakeStore's counter (which a failed call never
// reaches).
type schemaGapStore struct {
	*fakeStore
	mu       sync.Mutex
	migrated bool
	version  uint
	dirty    bool
	err      error
	attempts int
}

func (s *schemaGapStore) Upsert(ctx context.Context, events []metering.Event) error {
	s.mu.Lock()
	s.attempts++
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
	return s.version, s.dirty, s.err
}

func (s *schemaGapStore) migrate(version uint) {
	s.mu.Lock()
	s.migrated, s.version = true, version
	s.mu.Unlock()
}

func (s *schemaGapStore) attemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

// lockedBuffer is a goroutine-safe log sink.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// capturingLogger returns a logger whose WARN and INFO lines go to the
// returned buffer.
func capturingLogger() (*logging.Logger, *lockedBuffer) {
	buf := &lockedBuffer{}
	return &logging.Logger{
		Debug: log.New(io.Discard, "", 0),
		Info:  log.New(buf, "INFO: ", 0),
		Warn:  log.New(buf, "WARN: ", 0),
		Error: log.New(buf, "ERROR: ", 0),
	}, buf
}

// newGatedDrainer builds a drainer whose schema probe polls quickly.
func newGatedDrainer(cfg Config, logger *logging.Logger, rdb redis.Cmdable, store Store) *Drainer {
	d := New(cfg, logger, rdb, store)
	d.schemaPoll = 10 * time.Millisecond
	return d
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

	latest, err := migrations.LatestVersion()
	if err != nil {
		t.Fatal(err)
	}
	store.migrate(latest)
	d.cfg.ClaimMinIdle = 0
	d.reclaim(ctx)
	if store.rowCount() != 2 || pendingCount(t, rdb, cfg.StreamName, cfg.Group) != 0 {
		t.Fatalf("rows=%d pending=%d after the migration, want both stored and ACK'd",
			store.rowCount(), pendingCount(t, rdb, cfg.StreamName, cfg.Group))
	}
}

// The drainer consumes nothing until the database is at the schema version
// it writes, so an upgrade never writes new columns into an old table. The
// store accepts writes from the start (migrated) while reporting version 8, so
// a broken gate would store a row here, and the consumer group must not even
// exist yet (the gate runs before ensureGroup), so nothing was read.
func TestDrain_WaitsForRequiredSchemaVersion(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	cfg.RequiredSchemaVersion = 9
	store := &schemaGapStore{fakeStore: newFakeStore(), version: 8, migrated: true}
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("e1"))

	d := newGatedDrainer(cfg, testLogger(), rdb, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	time.Sleep(300 * time.Millisecond)
	if attempts := store.attemptCount(); attempts != 0 || store.rowCount() != 0 {
		t.Fatalf("upsert attempts=%d rows=%d while the schema is behind, want no write attempted", attempts, store.rowCount())
	}
	groups, err := rdb.XInfoGroups(context.Background(), cfg.StreamName).Result()
	if err != nil {
		t.Fatalf("xinfo groups: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("consumer groups = %v while the schema is behind, want none (nothing read from the stream)", groups)
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

func TestSchemaReady(t *testing.T) {
	for _, c := range []struct {
		version uint
		dirty   bool
		want    bool
	}{
		{8, false, false}, // behind
		{9, false, true},  // at the required version
		{9, true, false},  // the migration this binary needs is half-applied
		{10, true, true},  // a LATER migration failed; 9 is intact
		{10, false, true}, // ahead
		{8, true, false},  // behind and dirty
	} {
		if got := schemaReady(c.version, c.dirty, 9); got != c.want {
			t.Errorf("schemaReady(%d, dirty=%v, need 9) = %v, want %v", c.version, c.dirty, got, c.want)
		}
	}
}

// A dirty row AT the required version means the migration this binary needs
// is half-applied: the drainer must not write.
func TestDrain_DirtyAtRequiredVersionWaits(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	cfg.RequiredSchemaVersion = 9
	store := &schemaGapStore{fakeStore: newFakeStore(), version: 9, dirty: true, migrated: true}
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("e1"))
	d := newGatedDrainer(cfg, testLogger(), rdb, store)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := d.Run(ctx); err != nil {
		t.Fatalf("Run = %v, want a clean stop at the context deadline", err)
	}
	if attempts := store.attemptCount(); attempts != 0 || store.rowCount() != 0 {
		t.Fatalf("upsert attempts=%d rows=%d on a dirty schema at the required version, want nothing written", attempts, store.rowCount())
	}
}

// A failed LATER migration (golang-migrate leaves the version it attempted
// dirty) does not stop the running release's drainer: every version it needs
// is fully applied.
func TestDrain_DirtyAboveRequiredVersionConsumes(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	cfg.RequiredSchemaVersion = 9
	store := &schemaGapStore{fakeStore: newFakeStore(), version: 10, dirty: true, migrated: true}
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("e1"))
	logger, logs := capturingLogger()
	d := newGatedDrainer(cfg, logger, rdb, store)

	runUntil(t, d, func() bool { return store.rowCount() == 1 }, 5*time.Second)
	if !strings.Contains(logs.String(), "WARN: drainer: database schema at version 10 is dirty") {
		t.Fatalf("want a WARN naming the dirty version 10, got logs:\n%s", logs.String())
	}
}

// Behind-and-clean, and a probe error, both keep the gate closed: the
// drainer fails closed until the context ends.
func TestWaitForSchema_BehindSchemaKeepsWaiting(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RequiredSchemaVersion = 9
	store := &schemaGapStore{fakeStore: newFakeStore(), version: 8}
	d := newGatedDrainer(cfg, testLogger(), nil, store)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := d.waitForSchema(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForSchema = %v, want it still waiting on version 8 (need 9)", err)
	}
}

func TestDrain_SchemaVersionErrorIsNotReady(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RequiredSchemaVersion = 9
	store := &schemaGapStore{fakeStore: newFakeStore(), version: 9, err: errors.New("connection refused")}
	d := newGatedDrainer(cfg, testLogger(), nil, store)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := d.waitForSchema(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForSchema = %v, want it still waiting while SchemaVersion errors", err)
	}
}

// hangingSchemaStore's SchemaVersion blocks until its context ends, like a
// query stuck behind a lock or a stalled connection.
type hangingSchemaStore struct {
	*fakeStore
	probes atomic.Int32
}

func (s *hangingSchemaStore) SchemaVersion(ctx context.Context) (uint, bool, error) {
	s.probes.Add(1)
	<-ctx.Done()
	return 0, false, ctx.Err()
}

// Each probe has its own deadline, so a hung query becomes an error that the
// wait loop logs and retries, rather than a silent stall with no log line.
func TestWaitForSchema_HungProbeTimesOutAndIsLogged(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RequiredSchemaVersion = 9
	store := &hangingSchemaStore{fakeStore: newFakeStore()}
	logger, logs := capturingLogger()
	d := newGatedDrainer(cfg, logger, nil, store)
	d.schemaProbeTimeout = 20 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := d.waitForSchema(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForSchema = %v, want it still waiting", err)
	}
	if n := store.probes.Load(); n < 2 {
		t.Fatalf("probes = %d, want the hung probe abandoned and retried", n)
	}
	if !strings.Contains(logs.String(), "err=context deadline exceeded") {
		t.Fatalf("want the probe timeout in the wait log, got:\n%s", logs.String())
	}
}

// The wait is bounded: once SchemaWaitTimeout passes, Run returns an error
// (main exits non-zero, the pod restarts visibly) instead of idling while the
// emitter's stream cap trims un-drained events.
func TestDrain_RunFailsWhenSchemaNeverArrives(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RequiredSchemaVersion = 9
	cfg.SchemaWaitTimeout = 10 * time.Minute
	store := &schemaGapStore{fakeStore: newFakeStore(), version: 8}
	d := newGatedDrainer(cfg, testLogger(), nil, store)
	d.schemaPoll = time.Millisecond
	var clock atomic.Int64
	base := time.Unix(1_700_000_000, 0)
	d.now = func() time.Time { return base.Add(time.Duration(clock.Add(int64(time.Minute)))) }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := d.Run(ctx)
	if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want a schema-timeout error", err)
	}
	if !strings.Contains(err.Error(), "not at version 9") {
		t.Fatalf("Run error = %q, want it to name the missing schema version", err)
	}
}

// Every Store reports a schema version (it is part of the interface), so the
// gate can never be skipped silently by a store that cannot answer.
func TestDrain_SchemaGateAppliesToEveryStore(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RequiredSchemaVersion = 9
	store := newFakeStore()
	d := newGatedDrainer(cfg, testLogger(), nil, store)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := d.waitForSchema(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForSchema = %v with a plain store at version 0, want it waiting", err)
	}
	store.schemaVersion = 9
	if err := d.waitForSchema(context.Background()); err != nil {
		t.Fatalf("waitForSchema = %v with the store at version 9, want the gate open", err)
	}
}

// QA's B22 drill: Valkey restarted without persistence, wiping the stream and
// its consumer group; the drainer looped on NOGROUP until a pod restart. It
// now recreates the group from the start of the stream, so an event the
// emitter wrote between the wipe and the recreate (the stream exists again
// but has no group) is drained too, not skipped.
func TestDrain_RecreatedGroupDrainsEventsEmittedDuringTheWipe(t *testing.T) {
	mr, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	store := newFakeStore()
	d := New(cfg, testLogger(), rdb, store)
	ctx := context.Background()
	if err := d.ensureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	mr.Del(cfg.StreamName) // the wipe: stream and group gone
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("during-wipe"))

	if _, err := d.readAndStore(ctx); err != nil {
		t.Fatalf("readAndStore after the wipe = %v, want the group recreated", err)
	}
	xaddEvent(t, rdb, cfg.StreamName, sampleEvent("after-recreate"))
	for i := 0; i < 3 && store.rowCount() < 2; i++ {
		if _, err := d.readAndStore(ctx); err != nil {
			t.Fatal(err)
		}
	}
	store.mu.Lock()
	_, during := store.rows["during-wipe"]
	_, after := store.rows["after-recreate"]
	store.mu.Unlock()
	if !during || !after {
		t.Fatalf("stored during-wipe=%v after-recreate=%v, want both: the event emitted between the wipe and the group being recreated, and the one emitted afterwards, must be drained", during, after)
	}
}

// golangMigrateUpName is the pattern golang-migrate's source.Parse accepts,
// restricted to up files. Written out here so the test does not share the
// implementation's parser.
var golangMigrateUpName = regexp.MustCompile(`^([0-9]+)_(.*)\.(up)\.sql$`)

// LatestVersion must equal the highest up-migration version on disk, computed
// here independently; every up file must be a name golang-migrate parses and
// have its down file.
func TestLatestVersionMatchesEmbeddedMigrations(t *testing.T) {
	entries, err := os.ReadDir("../../migrations")
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name()] = true
	}
	var want uint
	for name := range names {
		base, ok := strings.CutSuffix(name, ".up.sql")
		if !ok {
			continue
		}
		m := golangMigrateUpName.FindStringSubmatch(name)
		if m == nil {
			t.Fatalf("migration %s does not match golang-migrate's NNNN_name.up.sql pattern", name)
		}
		v, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			t.Fatalf("migration %s: version %q does not parse: %v", name, m[1], err)
		}
		if uint(v) > want {
			want = uint(v)
		}
		if !names[base+".down.sql"] {
			t.Errorf("migration %s has no matching %s.down.sql", name, base)
		}
	}
	if want == 0 {
		t.Fatal("no .up.sql migrations found")
	}
	got, err := migrations.LatestVersion()
	if err != nil {
		t.Fatalf("LatestVersion() error = %v", err)
	}
	if got != want {
		t.Fatalf("LatestVersion() = %d, want %d (the highest .up.sql version on disk)", got, want)
	}
}

// The required schema version must be exactly the last version cmd/migrate
// applies: walk migrations.FS through the same golang-migrate iofs source
// cmd/migrate uses and compare with its final version.
func TestLatestVersionIsLastVersionMigrateApplies(t *testing.T) {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	last, err := src.First()
	if err != nil {
		t.Fatalf("iofs First: %v", err)
	}
	for {
		next, err := src.Next(last)
		if errors.Is(err, fs.ErrNotExist) {
			break
		}
		if err != nil {
			t.Fatalf("iofs Next(%d): %v", last, err)
		}
		last = next
	}
	got, err := migrations.LatestVersion()
	if err != nil {
		t.Fatalf("LatestVersion() error = %v", err)
	}
	if got != last {
		t.Fatalf("LatestVersion() = %d, want %d (the last version golang-migrate applies from migrations.FS)", got, last)
	}
}

// RequiredSchemaVersion 0 disables the gate (tests only; main never sets it),
// and says so in the log so a disabled gate is never silent.
func TestWaitForSchema_ZeroDisablesGateAndWarns(t *testing.T) {
	_, rdb := startMiniredis(t)
	cfg := testConfig(rdb.Options().Addr)
	cfg.RequiredSchemaVersion = 0
	store := &schemaGapStore{fakeStore: newFakeStore(), version: 0, migrated: true}
	logger, buf := capturingLogger()
	d := newGatedDrainer(cfg, logger, rdb, store)
	if err := d.waitForSchema(context.Background()); err != nil {
		t.Fatalf("waitForSchema = %v, want nil with the gate disabled", err)
	}
	if out := buf.String(); !strings.Contains(out, "WARN: ") || !strings.Contains(out, "schema gate is disabled") {
		t.Fatalf("log = %q, want a WARN line saying the schema gate is disabled", out)
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
