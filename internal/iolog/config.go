package iolog

import "time"

// Config holds the tunable knobs for the I/O-logging subsystem. Like
// emit.Config it is intentionally separate from config.Settings — iolog is an
// independent subsystem wired at the call site, not bloat on the global struct.
type Config struct {
	// DatabaseURL is the Postgres DSN for the PostgresSink (pgx stdlib /
	// database/sql form, e.g. "postgres://user:pass@host:5432/db"). Empty means
	// no Postgres sink can be constructed.
	DatabaseURL string

	// Table is the destination table name. Defaults to "io_log".
	Table string

	// WorkerCount is the number of goroutines draining the record channel.
	WorkerCount int

	// ChanBuf is the buffered-channel capacity. On overflow records are DROPPED
	// (and counted) — best-effort, see the package doc.
	ChanBuf int

	// MaxBodyBytes caps how many bytes of the response body are BUFFERED for
	// logging. Bodies larger than this are truncated (and flagged) in the
	// Record; the full body still streams to the client verbatim. This bounds
	// memory under large/streamed responses. Defaults to 256 KiB.
	//
	// NOTE: this caps the buffered LOG copy only. It does NOT cap what the
	// client receives — streaming correctness is unaffected.
	MaxBodyBytes int

	// WriteTimeout is the per-INSERT context deadline. Defaults to 5s if zero.
	WriteTimeout time.Duration
}

// DefaultMaxBodyBytes is the default response-body buffering cap (256 KiB).
// Chosen as a balance: large enough to capture a typical completion plus its
// SSE framing, small enough that a burst of opted-in large responses cannot
// exhaust memory. Document any change to this in DESIGN §8.
const DefaultMaxBodyBytes = 256 * 1024

// DefaultConfig returns a Config with safe defaults. DatabaseURL is left empty
// (the caller supplies it only when I/O logging is enabled).
func DefaultConfig() Config {
	return Config{
		Table:        "io_log",
		WorkerCount:  2,
		ChanBuf:      2048,
		MaxBodyBytes: DefaultMaxBodyBytes,
		WriteTimeout: 5 * time.Second,
	}
}
