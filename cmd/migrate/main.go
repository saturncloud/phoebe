// Command migrate applies phoebe's SQL schema to phoebe's OWN Postgres.
//
// phoebe owns its billing schema (billing_event, rated_usage, io_log) and, as of
// the own-Postgres cutover, its own database — no longer the shared Atlas DB. This
// binary is the schema applier: it runs the embedded, version-tracked migrations
// (migrations.FS, golang-migrate) against DATABASE_URL. It runs as a one-shot Job /
// init-container in the phoebe chart before the drainer starts.
//
// phoebe is self-contained: no query joins any Atlas-owned table, so its tables do
// not need to be co-located with the Atlas schema — they live in phoebe's own DB.
//
// Config: DATABASE_URL (same convention as the drainer/rater/token-push). "up"
// (default) applies all pending migrations; "down" rolls back one step; "version"
// prints the current version.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/saturncloud/phoebe/migrations"
)

// migrateDSN adapts the shared DATABASE_URL (a standard postgres:// DSN, same as
// the drainer/rater/token-push consume via sql.Open("pgx", ...)) to the scheme
// golang-migrate's pgx/v5 database driver registers under ("pgx5://"). The rest of
// the DSN (host, creds, params) is unchanged, so one DATABASE_URL serves every
// phoebe component; only the scheme differs for the migrate driver's registry.
func migrateDSN(dsn string) string {
	for _, p := range []string{"postgres://", "postgresql://"} {
		if strings.HasPrefix(dsn, p) {
			return "pgx5://" + strings.TrimPrefix(dsn, p)
		}
	}
	return dsn
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatalf("migrate: DATABASE_URL is empty (phoebe's own Postgres DSN; cannot apply schema without it)")
	}

	cmd := "up"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		log.Fatalf("migrate: load embedded migrations: %v", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", src, migrateDSN(dsn))
	if err != nil {
		log.Fatalf("migrate: init: %v", err)
	}
	defer func() { _, _ = m.Close() }()

	switch cmd {
	case "up":
		if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			log.Fatalf("migrate: up: %v", err)
		}
		printVersion(m, "up complete")
	case "down":
		// Single-step rollback (down migrations are destructive; never all-at-once).
		if err := m.Steps(-1); err != nil && !errors.Is(err, migrate.ErrNoChange) {
			log.Fatalf("migrate: down: %v", err)
		}
		printVersion(m, "down one step complete")
	case "version":
		printVersion(m, "current")
	default:
		log.Fatalf("migrate: unknown command %q (want: up | down | version)", cmd)
	}
}

func printVersion(m *migrate.Migrate, note string) {
	v, dirty, err := m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		fmt.Printf("migrate: %s — no migrations applied yet\n", note)
		return
	}
	if err != nil {
		log.Fatalf("migrate: read version: %v", err)
	}
	fmt.Printf("migrate: %s — version=%d dirty=%v\n", note, v, dirty)
}
