package iolog

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/saturncloud/phoebe/internal/logging"
)

func testLogger() *logging.Logger { return logging.New(logging.DEBUG) }

func sampleRecord(id string) Record {
	return Record{
		RequestID:    id,
		AuthID:       "auth-1",
		GroupID:      "grp-1",
		ResourceID:   "model-1",
		Model:        "model-1",
		RequestBody:  `{"prompt":"hi"}`,
		ResponseBody: `{"choices":[]}`,
		StatusCode:   200,
		Streamed:     false,
		LatencyMs:    12,
		Timestamp:    time.UnixMilli(1_700_000_000_000),
	}
}

// newMockSink builds a PostgresSink wired to a sqlmock DB. It uses 1 worker so
// inserts are ordered, which makes expectations deterministic.
func newMockSink(t *testing.T, cfg Config) (*PostgresSink, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	if cfg.WorkerCount == 0 {
		cfg.WorkerCount = 1
	}
	s, err := newPostgresSinkWithDB(cfg, testLogger(), db)
	if err != nil {
		t.Fatalf("newPostgresSinkWithDB: %v", err)
	}
	return s, mock, db
}

// ---- NopSink ----------------------------------------------------------------

// TestNopSink_DoesNothing verifies the off-default sink is inert and satisfies
// the interface.
func TestNopSink_DoesNothing(t *testing.T) {
	var s Sink = NopSink{}
	// Must not panic or block.
	s.Log(context.Background(), sampleRecord("r1"))
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("NopSink.Close: %v", err)
	}
}

// ---- PostgresSink: SQL shape via sqlmock -------------------------------------

// TestPostgresSink_InsertSQLShape verifies the INSERT targets io_log with the
// expected columns and the tsvector expression, and that identity args map
// correctly (empty -> NULL).
func TestPostgresSink_InsertSQLShape(t *testing.T) {
	cfg := DefaultConfig()
	s, mock, db := newMockSink(t, cfg)
	defer db.Close()

	rec := sampleRecord("req-shape")
	// Expect an INSERT INTO io_log ... with to_tsvector over the bodies.
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO io_log")).
		WithArgs(
			rec.RequestID,
			rec.AuthID,
			sqlmock.AnyArg(), // user_id (empty -> NULL)
			rec.GroupID,
			rec.ResourceID,
			sqlmock.AnyArg(), // resource_type (empty -> NULL)
			rec.Model,
			rec.RequestBody,
			rec.ResponseBody,
			rec.ResponseTruncated,
			rec.StatusCode,
			rec.Streamed,
			rec.LatencyMs,
			sqlmock.AnyArg(), // created_at (UTC time)
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	s.Log(context.Background(), rec)

	// Close drains the worker, so the insert has executed by the time it returns.
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestPostgresSink_QueryMentionsTsvector verifies the generated INSERT includes
// the full-text vector population, the "grep inside bodies" mechanism.
func TestPostgresSink_QueryMentionsTsvector(t *testing.T) {
	q := insertQuery("io_log")
	for _, want := range []string{"INSERT INTO io_log", "to_tsvector", "body_tsv", "$14"} {
		if !regexp.MustCompile(regexp.QuoteMeta(want)).MatchString(q) {
			t.Errorf("insert query missing %q:\n%s", want, q)
		}
	}
}

// TestPostgresSink_InsertErrorDoesNotPanic verifies a DB error on INSERT is
// swallowed (best-effort) — the sink logs and moves on, never crashes.
func TestPostgresSink_InsertErrorDoesNotPanic(t *testing.T) {
	cfg := DefaultConfig()
	s, mock, db := newMockSink(t, cfg)
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO io_log")).
		WillReturnError(fmt.Errorf("boom"))

	s.Log(context.Background(), sampleRecord("req-err"))
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// No panic, no crash; expectation met (the error path consumed it).
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestPostgresSink_NulAndInvalidUTF8BodySanitized verifies bodies are made
// safe for Postgres TEXT before INSERT: PG rejects NUL bytes and invalid
// UTF-8, and an insert that failed over tenant-controlled bytes would drop the
// record. NULs are removed; invalid sequences become U+FFFD.
func TestPostgresSink_NulAndInvalidUTF8BodySanitized(t *testing.T) {
	cfg := DefaultConfig()
	s, mock, db := newMockSink(t, cfg)
	defer db.Close()

	rec := sampleRecord("req-nul")
	rec.RequestBody = "a\x00b"      // NUL byte: PG TEXT rejects it outright
	rec.ResponseBody = "x\xff\xfey" // invalid UTF-8
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO io_log")).
		WithArgs(
			rec.RequestID,
			rec.AuthID,
			sqlmock.AnyArg(),
			rec.GroupID,
			rec.ResourceID,
			sqlmock.AnyArg(),
			rec.Model,
			"ab",  // NUL removed
			"x�y", // the run of invalid bytes replaced with one U+FFFD
			rec.ResponseTruncated,
			rec.StatusCode,
			rec.Streamed,
			rec.LatencyMs,
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	s.Log(context.Background(), rec)
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestPostgresSink_InsertFailureCountsDropped verifies a failed INSERT is a
// counted loss, not just a log line — an invisible drop is how capture gaps
// hide. The dropped counter covers BOTH overflow and insert failure.
func TestPostgresSink_InsertFailureCountsDropped(t *testing.T) {
	cfg := DefaultConfig()
	s, mock, db := newMockSink(t, cfg)
	defer db.Close()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO io_log")).
		WillReturnError(fmt.Errorf("invalid byte sequence for encoding \"UTF8\""))

	s.Log(context.Background(), sampleRecord("req-fail"))
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := s.Dropped(); got != 1 {
		t.Fatalf("Dropped() = %d after failed insert, want 1", got)
	}
}

// ---- PostgresSink: async / non-blocking --------------------------------------

// TestPostgresSink_LogIsNonBlocking verifies Log returns fast even when the
// underlying DB is slow: the channel decouples Log from the insert.
func TestPostgresSink_LogIsNonBlocking(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ChanBuf = 64
	cfg.WorkerCount = 1
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Every insert sleeps; Log must NOT wait for it.
	mock.MatchExpectationsInOrder(false)
	for range 64 {
		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO io_log")).
			WillDelayFor(50 * time.Millisecond).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	s, err := newPostgresSinkWithDB(cfg, testLogger(), db)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	for i := range 50 {
		s.Log(context.Background(), sampleRecord(fmt.Sprintf("r%d", i)))
	}
	elapsed := time.Since(start)
	if elapsed > 200*time.Millisecond {
		t.Errorf("Log blocked: 50 calls took %v (slow insert leaked into hot path)", elapsed)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.Close(ctx)
}

// TestPostgresSink_OverflowDropsAndCounts verifies that once the buffer is full
// (the single worker is stuck on a slow insert), further Log calls are dropped
// and counted rather than blocking. This is the best-effort contract: lose the
// sampled record, never backpressure the caller.
func TestPostgresSink_OverflowDropsAndCounts(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ChanBuf = 4
	cfg.WorkerCount = 1
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// The first insert blocks for the duration of the test, so the lone worker
	// is parked and the channel stays full once we've filled it. A second
	// expectation (no delay) absorbs whatever the worker pulls after unblocking
	// at Close, so sqlmock doesn't error on an unexpected call.
	mock.MatchExpectationsInOrder(false)
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO io_log")).
		WillDelayFor(time.Hour). // effectively blocks the worker for the test
		WillReturnResult(sqlmock.NewResult(1, 1))
	for range 8 {
		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO io_log")).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	s, err := newPostgresSinkWithDB(cfg, testLogger(), db)
	if err != nil {
		t.Fatal(err)
	}

	// Worker takes 1 record and parks on the slow insert; ChanBuf(4) then fill.
	// So 1 (in-worker) + 4 (buffered) = 5 accepted before drops begin. We poll
	// until drops are observed rather than assuming exact timing.
	const n = 100
	start := time.Now()
	for i := range n {
		s.Log(context.Background(), sampleRecord(fmt.Sprintf("r%d", i)))
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("Log blocked on overflow: %v", elapsed)
	}

	dropped := s.Dropped()
	if dropped == 0 {
		t.Fatal("expected some records dropped on overflow, got 0")
	}
	// At most 5 can be accepted (1 in-flight + 4 buffered); the rest drop.
	if dropped < uint64(n-5) {
		t.Errorf("dropped = %d, want >= %d (accepted at most 5)", dropped, n-5)
	}

	// Don't wait for the hour-long insert; bound Close so the test ends.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = s.Close(ctx)
}

// TestPostgresSink_CloseDrainsInFlight verifies graceful Close flushes buffered
// records (best-effort flush, not a drop).
func TestPostgresSink_CloseDrainsInFlight(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ChanBuf = 64
	cfg.WorkerCount = 2
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.MatchExpectationsInOrder(false)
	const n = 20
	for range n {
		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO io_log")).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	s, err := newPostgresSinkWithDB(cfg, testLogger(), db)
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		s.Log(context.Background(), sampleRecord(fmt.Sprintf("r%d", i)))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("Close did not drain all in-flight records: %v", err)
	}
}

// TestPostgresSink_ConcurrentLogNoRace exercises Log from many goroutines for
// the race detector.
func TestPostgresSink_ConcurrentLogNoRace(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ChanBuf = 256
	cfg.WorkerCount = 4
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.MatchExpectationsInOrder(false)
	// Allow any number of inserts.
	for range 1000 {
		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO io_log")).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	s, err := newPostgresSinkWithDB(cfg, testLogger(), db)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for g := range 10 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 20 {
				s.Log(context.Background(), sampleRecord(fmt.Sprintf("g%d-r%d", g, i)))
			}
		}(g)
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.Close(ctx)
}

// TestNewPostgresSink_RequiresDatabaseURL verifies fail-fast on missing DSN.
func TestNewPostgresSink_RequiresDatabaseURL(t *testing.T) {
	_, err := NewPostgresSink(DefaultConfig(), testLogger())
	if err == nil {
		t.Fatal("expected error for empty DatabaseURL")
	}
}

// TestSink_InterfaceSatisfied is a runtime check that NopSink satisfies Sink and
// is inert. (*PostgresSink's interface conformance is enforced by the
// package-level `var _ Sink = (*PostgresSink)(nil)` in postgres.go.)
func TestSink_InterfaceSatisfied(t *testing.T) {
	var nop Sink = NopSink{}
	nop.Log(context.Background(), Record{}) // must not panic
	if err := nop.Close(context.Background()); err != nil {
		t.Errorf("NopSink.Close: %v", err)
	}
}
