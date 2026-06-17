package rating

import "time"

// Config holds the rater's tunable knobs. It mirrors drain.Config's YAML-free,
// wire-at-call-site pattern: cmd/rater reads a settings file (or env) and
// translates it into this struct, so this package has no dependency on a shared
// config package.
//
// The rater is a BATCH job (cron / k8s CronJob), not a daemon: it rates one
// window and exits. The window itself is passed to Run, not held here.
type Config struct {
	// DatabaseURL is the Postgres DSN. Read from the DATABASE_URL env var by the
	// binary (Atlas convention). Same shared Atlas Postgres the drainer writes
	// billing_event into. Required.
	DatabaseURL string

	// MaxOpenConns / MaxIdleConns / ConnMaxLifetime tune the database/sql pool.
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// DefaultConfig returns a Config with safe defaults. The caller MUST set
// DatabaseURL. A batch job needs a small pool — it reads one window, upserts the
// rollups, and exits.
func DefaultConfig() Config {
	return Config{
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
	}
}
