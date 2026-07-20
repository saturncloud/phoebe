// Package migrations embeds phoebe's SQL schema migrations so cmd/migrate can
// apply them to phoebe's own Postgres without shipping the .sql files separately.
//
// The migrations are golang-migrate up/down pairs (NNNN_name.up.sql /
// NNNN_name.down.sql), applied in version order. phoebe OWNS this schema in its
// OWN database (not the shared Atlas DB); it is self-contained (no query joins any
// Atlas-owned table), so the tables need not be co-located with the Atlas schema.
package migrations

import "embed"

// FS holds the embedded up/down SQL migration files.
//
//go:embed *.sql
var FS embed.FS
