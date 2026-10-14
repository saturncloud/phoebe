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
	"strconv"
	"strings"
)

// FS holds the embedded up/down SQL migration files.
//
//go:embed *.sql
var FS embed.FS

// LatestVersion is the highest migration version embedded in FS: the schema
// version this binary expects. The drainer waits for the database to reach it
// before writing (an upgrade must never write new columns into an old table).
func LatestVersion() uint {
	entries, err := FS.ReadDir(".")
	if err != nil {
		return 0
	}
	var latest uint
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		prefix, _, ok := strings.Cut(name, "_")
		if !ok {
			continue
		}
		v, err := strconv.ParseUint(prefix, 10, 32)
		if err == nil && uint(v) > latest {
			latest = uint(v)
		}
	}
	return latest
}
