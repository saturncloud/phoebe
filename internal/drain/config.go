// Package drain is the CONSUMER side of the metering durability ladder. The
// emitter (internal/emit) XADDs metering events to a Valkey Stream as the hot
// buffer; the drainer reads them via a consumer group and writes each event
// durably to Postgres, the system of record (pre-rating raw counts).
//
// Delivery is at-least-once: an entry is XACK'd only AFTER its Postgres write
// commits. A crash before ACK re-delivers the entry, which is safe because the
// Postgres upsert is idempotent on request_id (INSERT ... ON CONFLICT DO
// NOTHING). At-least-once + idempotent write = effectively-once.
package drain

import "time"

// Config holds the drainer's tunable knobs. It mirrors the YAML-free,
// wire-at-call-site pattern used by emit.Config: the binary's main.go reads a
// settings file (or env) and translates it into this struct, so this package
// has no dependency on internal/config.
type Config struct {
	// --- Valkey consumer ---

	// ValkeyAddr is the Valkey/Redis address, e.g. "localhost:6379". Required:
	// the drainer has no fallback source — it is the consumer of the stream.
	ValkeyAddr string

	// StreamName is the stream the emitter XADDs to. MUST match emit.Config's
	// StreamName, e.g. "phoebe:metering".
	StreamName string

	// Group is the consumer-group name. All drainer replicas share one group so
	// each stream entry is delivered to exactly one consumer in the group.
	Group string

	// Consumer is this replica's unique name within the group (e.g. the pod
	// name). Pending entries are tracked per-consumer; XAUTOCLAIM reclaims
	// entries stranded by a dead consumer.
	Consumer string

	// BatchSize is the max entries read per XREADGROUP / written per upsert
	// batch. Larger batches amortise the Postgres round-trip.
	BatchSize int

	// BlockTimeout is how long XREADGROUP blocks waiting for new entries before
	// returning empty (so the loop can check for shutdown / run reclaim).
	BlockTimeout time.Duration

	// --- Pending-entry reclaim (dead-consumer recovery) ---

	// ClaimMinIdle is the minimum time an entry must sit unacknowledged in
	// another consumer's PEL before XAUTOCLAIM may steal it. Set comfortably
	// above the expected processing time so we don't reclaim in-flight work.
	ClaimMinIdle time.Duration

	// ClaimInterval is how often (in drain-loop iterations elapsed) reclaim is
	// attempted. Reclaim runs opportunistically; it need not be on every pass.
	ClaimInterval time.Duration

	// --- Postgres ---

	// DatabaseURL is the Postgres DSN. Read from the DATABASE_URL env var by the
	// binary (Atlas convention). The pgx stdlib driver accepts the standard
	// libpq URL form, e.g. "postgres://user:pass@host:5432/db?sslmode=require".
	DatabaseURL string

	// MaxOpenConns / MaxIdleConns / ConnMaxLifetime tune the database/sql pool.
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// DefaultConfig returns a Config with safe, production-ready defaults. The
// caller MUST set ValkeyAddr and DatabaseURL; everything else has a sane value.
func DefaultConfig() Config {
	return Config{
		StreamName:      "phoebe:metering",
		Group:           "phoebe-drainer",
		Consumer:        "drainer-1",
		BatchSize:       256,
		BlockTimeout:    5 * time.Second,
		ClaimMinIdle:    30 * time.Second,
		ClaimInterval:   15 * time.Second,
		MaxOpenConns:    90,
		MaxIdleConns:    20,
		ConnMaxLifetime: 30 * time.Minute,
	}
}
