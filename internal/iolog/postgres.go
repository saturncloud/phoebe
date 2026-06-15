package iolog

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	// pgx stdlib driver: registers "pgx" with database/sql. Mirrors the
	// drainer's Postgres approach (database/sql + pgx stdlib + DATABASE_URL).
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/saturncloud/phoebe/internal/logging"
)

// PostgresSink writes Records to the io_log table via a worker pool draining a
// buffered channel. It is the built M5 store.
//
// # Best-effort, NOT durable (contrast with metering)
//
// Unlike emit.DurableEmitter, PostgresSink has NO WAL and NO log floor. On
// channel overflow it DROPS the record and increments a counter; on INSERT
// error it logs and moves on. This is deliberate: I/O logging is sampled debug
// telemetry, so a dropped body is acceptable and far cheaper than adding
// backpressure or disk I/O to the request path. (Metering cannot drop — it is
// money — which is why emit climbs a durability ladder and this does not.)
//
// Log never blocks: it does a non-blocking channel send and, if the buffer is
// full, drops + counts and returns immediately.
type PostgresSink struct {
	cfg     Config
	log     *logging.Logger
	db      *sql.DB
	ownsDB  bool // true if we opened db and must close it
	insertQ string

	ch     chan Record
	wg     sync.WaitGroup
	stopCh chan struct{}

	// dropped counts records LOST for any reason — channel overflow OR a failed
	// INSERT (both lose the record just as completely). Exposed via Dropped()
	// for observability; a steadily rising value means the sink can't keep up
	// (raise WorkerCount/ChanBuf or lower the sample rate) or the inserts are
	// failing (check the warn logs for the error).
	dropped atomic.Uint64
}

// NewPostgresSink opens the database from cfg.DatabaseURL and starts the worker
// pool. The caller owns Close.
func NewPostgresSink(cfg Config, log *logging.Logger) (*PostgresSink, error) {
	if log == nil {
		return nil, fmt.Errorf("iolog.NewPostgresSink: logger must not be nil")
	}
	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("iolog.NewPostgresSink: DatabaseURL must not be empty")
	}
	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("iolog.NewPostgresSink: open: %w", err)
	}
	s, err := newPostgresSinkWithDB(cfg, log, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	s.ownsDB = true
	return s, nil
}

// newPostgresSinkWithDB builds a sink around an existing *sql.DB. It is the seam
// the tests use to inject a sqlmock DB without a real Postgres. It does NOT take
// ownership of db (ownsDB stays false) unless the caller sets it.
func newPostgresSinkWithDB(cfg Config, log *logging.Logger, db *sql.DB) (*PostgresSink, error) {
	c := withDefaults(cfg)
	s := &PostgresSink{
		cfg:     c,
		log:     log,
		db:      db,
		insertQ: insertQuery(c.Table),
		ch:      make(chan Record, c.ChanBuf),
		stopCh:  make(chan struct{}),
	}
	for range c.WorkerCount {
		s.wg.Add(1)
		go s.worker()
	}
	return s, nil
}

// withDefaults fills zero-valued config fields with their defaults so a
// caller-supplied Config (e.g. just DatabaseURL) is usable.
func withDefaults(cfg Config) Config {
	if cfg.Table == "" {
		cfg.Table = "io_log"
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 2
	}
	if cfg.ChanBuf <= 0 {
		cfg.ChanBuf = 2048
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	return cfg
}

// insertQuery builds the parameterized INSERT. The tsvector column is populated
// by a generated/trigger-free expression at write time so search works without
// a separate maintenance job: we compute to_tsvector over the two bodies in SQL.
// Column order matches insertArgs below.
func insertQuery(table string) string {
	return fmt.Sprintf(`INSERT INTO %s (
		request_id, auth_id, user_id, group_id, resource_id, resource_type,
		model, request_body, response_body, response_truncated,
		status_code, streamed, latency_ms, created_at, body_tsv
	) VALUES (
		$1, $2, $3, $4, $5, $6,
		$7, $8, $9, $10,
		$11, $12, $13, $14,
		to_tsvector('simple', coalesce($8,'') || ' ' || coalesce($9,''))
	)`, table)
}

func insertArgs(rec Record) []any {
	ts := rec.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	return []any{
		rec.RequestID,
		nullStr(rec.AuthID),
		nullStr(rec.UserID),
		nullStr(rec.GroupID),
		nullStr(rec.ResourceID),
		nullStr(rec.ResourceType),
		nullStr(rec.Model),
		sanitizeBody(rec.RequestBody),
		sanitizeBody(rec.ResponseBody),
		rec.ResponseTruncated,
		rec.StatusCode,
		rec.Streamed,
		rec.LatencyMs,
		ts.UTC(),
	}
}

// nullStr maps an empty string to SQL NULL so optional identity columns are
// NULL rather than ” — cleaner for retention/analytics queries.
func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// sanitizeBody makes a captured body storable in a Postgres TEXT column:
// Postgres rejects invalid UTF-8 outright and additionally rejects NUL (0x00)
// even though it is a valid UTF-8 code point. Bodies are tenant-controlled
// bytes, so either would fail the INSERT and silently lose the whole record —
// sanitize instead. Invalid sequences become U+FFFD (a visible marker of where
// bytes were mangled); NULs are removed (Postgres has no representable form).
func sanitizeBody(s string) string {
	s = strings.ToValidUTF8(s, "�")
	return strings.ReplaceAll(s, "\x00", "")
}

// Log hands the record to the worker channel without blocking. On a full buffer
// the record is DROPPED and counted — best-effort, never backpressure the proxy.
func (s *PostgresSink) Log(_ context.Context, rec Record) {
	select {
	case s.ch <- rec:
	default:
		// Buffer full: drop + count. Losing a sampled debug body is acceptable;
		// blocking the client response is not.
		n := s.dropped.Add(1)
		// Log sparsely (powers of two) so a sustained overflow doesn't itself
		// become a log-spam problem.
		if n&(n-1) == 0 {
			s.log.Warn.Printf("iolog: buffer full, dropped record request_id=%s (total dropped=%d)", rec.RequestID, n)
		}
	}
}

// Dropped returns the number of records lost since start — dropped on channel
// overflow or lost to a failed INSERT.
func (s *PostgresSink) Dropped() uint64 { return s.dropped.Load() }

// worker drains the channel and inserts each record.
func (s *PostgresSink) worker() {
	defer s.wg.Done()
	for {
		select {
		case rec, ok := <-s.ch:
			if !ok {
				return
			}
			s.insert(rec)
		case <-s.stopCh:
			// Drain remaining buffered records before exiting so a graceful
			// shutdown flushes what's in flight.
			for {
				select {
				case rec := <-s.ch:
					s.insert(rec)
				default:
					return
				}
			}
		}
	}
}

// insert writes one record. On error it logs and drops (best-effort) — and
// COUNTS the drop: a failed INSERT loses the record just as completely as a
// channel overflow, and an uncounted loss is how capture gaps stay invisible.
func (s *PostgresSink) insert(rec Record) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.WriteTimeout)
	defer cancel()
	if _, err := s.db.ExecContext(ctx, s.insertQ, insertArgs(rec)...); err != nil {
		n := s.dropped.Add(1)
		s.log.Warn.Printf("iolog: insert request_id=%s: %v (dropped, total dropped=%d)", rec.RequestID, err, n)
	}
}

// Close stops the workers, drains in-flight records, and closes the DB if owned.
// The context bounds the drain wait.
func (s *PostgresSink) Close(ctx context.Context) error {
	close(s.stopCh)
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		// Drain timed out; still close the DB below so we don't leak it.
		if s.ownsDB {
			_ = s.db.Close()
		}
		return ctx.Err()
	}
	if s.ownsDB {
		return s.db.Close()
	}
	return nil
}

// compile-time interface check.
var _ Sink = (*PostgresSink)(nil)
