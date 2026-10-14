// Package migrations embeds phoebe's SQL schema migrations so cmd/migrate can
// apply them to phoebe's own Postgres without shipping the .sql files separately.
//
// The migrations are golang-migrate up/down pairs (NNNN_name.up.sql /
// NNNN_name.down.sql), applied in version order. phoebe OWNS this schema in its
// OWN database (not the shared Atlas DB); it is self-contained (no query joins any
// Atlas-owned table), so the tables need not be co-located with the Atlas schema.
package migrations

import (
	"embed"
	"fmt"
	"io/fs"
	"strings"

	"github.com/golang-migrate/migrate/v4/source"
)

// FS holds the embedded up/down SQL migration files.
//
//go:embed *.sql
var FS embed.FS

// LatestVersion is the highest up-migration version embedded in FS: the schema
// version this binary expects. The drainer waits for the database to reach it
// before writing (an upgrade must never write new columns into an old table).
//
// It fails closed: a version of 0 would disable the drainer's schema gate, so
// LatestVersion returns an error instead of 0 when FS cannot be read, when any
// .sql file name is not one golang-migrate parses (cmd/migrate would then apply
// a different set than this function counts), or when there is no up migration.
func LatestVersion() (uint, error) {
	return latestVersion(FS)
}

// latestVersion is LatestVersion over any directory, so tests can feed it
// malformed names. Names are parsed with golang-migrate's own parser
// (source.Parse, 64-bit versions), the one cmd/migrate's iofs source uses.
func latestVersion(fsys fs.ReadDirFS) (uint, error) {
	entries, err := fsys.ReadDir(".")
	if err != nil {
		return 0, fmt.Errorf("migrations: read embedded migrations: %w", err)
	}
	var latest uint
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".sql") {
			continue
		}
		m, err := source.Parse(name)
		if err != nil {
			return 0, fmt.Errorf("migrations: %s is not a golang-migrate NNNN_name.(up|down).sql file: %w", name, err)
		}
		if m.Direction == source.Up && m.Version > latest {
			latest = m.Version
		}
	}
	if latest == 0 {
		return 0, fmt.Errorf("migrations: no up migration with a version above 0 found")
	}
	return latest, nil
}
