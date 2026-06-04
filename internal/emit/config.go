// Package emit provides a durable metering.Emitter that ships events via a
// three-level durability ladder:
//
//  1. Valkey/Redis Streams (hot buffer, async, low-latency)
//  2. Local-disk WAL (append-only JSONL, fsync'd, for when Valkey is down)
//  3. Structured log (last resort if even the WAL fails)
//
// Emit is always non-blocking from the caller's perspective; background workers
// handle I/O. Graceful shutdown is available via Close.
package emit

import "time"

// Config holds all tunable knobs for the durable emitter.
// It is intentionally separate from config.Settings — emit is an independent
// subsystem and its config should be wired at the call site, not bloat the
// global settings struct.
type Config struct {
	// Valkey / Redis settings
	ValkeyAddr   string // e.g. "localhost:6379"
	StreamName   string // XADD target key, e.g. "phoebe:metering"
	StreamMaxLen int64  // MAXLEN trim (0 = no trim)

	// Worker pool
	WorkerCount int // number of goroutines draining the event channel
	ChanBuf     int // buffered channel capacity before falling to WAL

	// XADDTimeout is the per-XADD context deadline. Defaults to 2s if zero.
	// Set lower in tests to avoid slow retries when Valkey is down.
	XADDTimeout time.Duration

	// WAL settings
	WALPath       string        // path of the append-only JSONL file
	ShipInterval  time.Duration // how often the shipper retries draining the WAL
	ShipBatchSize int           // max events per drain pass
}

// DefaultConfig returns a Config with safe, production-ready defaults.
func DefaultConfig() Config {
	return Config{
		ValkeyAddr:    "localhost:6379",
		StreamName:    "phoebe:metering",
		StreamMaxLen:  1_000_000,
		WorkerCount:   4,
		ChanBuf:       8192,
		WALPath:       "/tmp/phoebe-metering-wal.jsonl",
		ShipInterval:  5 * time.Second,
		ShipBatchSize: 256,
	}
}
