// Command prune is phoebe's evidence-retention batch job, implementing the
// RULED policy (rulings.md 2026-09-30, queue #5 + follow-ons 1-3):
//
//	billing_event   pruned on created_at, default horizon 30 days (hard
//	                floor 7 — below the floor a later rater re-rate can
//	                reconcile-DELETE previously billed rated_usage rows).
//	                ONE horizon for every row class: withheld/invalid rows
//	                prune the same as clean rows; the pruner does NOT
//	                reimplement the rater's withholding logic.
//	io_log          pruned on created_at at its OWN period, default 7 days.
//	archive         OUT OF SCOPE by ruling — the manager's money rollup
//	                outlives the trailing window; the WAL keeps recent
//	                evidence.
//
// It is a ONE-SHOT BATCH job (a k8s CronJob), NOT a daemon: it prunes both
// tables once and exits. Shape (A) by ruling: batched DELETEs, NO schema
// change — the table, the PK, and the drainer's ON CONFLICT (request_id)
// dedup stay untouched, and rated_usage (money) is never referenced.
//
// Exit codes:
//
//	0  both tables pruned per policy (zero eligible rows is success)
//	1  fatal (config, floor violation, DB) — nothing or partial; safe to re-run
//
// Re-running is safe and idempotent: the cutoff is truncated to the second
// and each batch loop converges to zero, so cron overlap or a manual re-run
// never double-deletes or errors.
//
// SINGLE-FLIGHT: like the rater, the deployment MUST forbid concurrency
// (the chart sets concurrencyPolicy: Forbid). Concurrent pruners are not a
// correctness hazard (batched DELETEs of disjoint-by-time rows), but
// single-flight keeps the WAL story simple.
//
// Config: a YAML settings file (flag -f) for the horizons and batch size,
// the DATABASE_URL env var for Postgres (same phoebe-database secret as the
// drainer/rater/token-push). The pruner does NOT run migrations.
package main

import (
	"context"
	"database/sql"
	"flag"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"gopkg.in/yaml.v2"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/prune"
)

// pruneSettings is the YAML shape for the pruner. Zero values mean "use the
// ruled default"; the floors are enforced by prune.Config.Validate (below the
// billing_event floor is refused, not clamped — see the package doc).
type pruneSettings struct {
	Debug bool `yaml:"debug"`

	// BillingEventRetentionDays: billing_event horizon. Default 30, floor 7.
	BillingEventRetentionDays int `yaml:"billingEventRetentionDays"`
	// IoLogRetentionDays: io_log horizon, its OWN period. Default 7, floor 1.
	IoLogRetentionDays int `yaml:"ioLogRetentionDays"`
	// BatchSize: rows per DELETE statement. Default 5000.
	BatchSize int `yaml:"batchSize"`

	MaxOpenConns    int    `yaml:"maxOpenConns"`
	MaxIdleConns    int    `yaml:"maxIdleConns"`
	ConnMaxLifetime string `yaml:"connMaxLifetime"`
}

const (
	exitOK    = 0
	exitFatal = 1
)

func main() {
	configPath := flag.String("f", "/etc/saturn/config/prune.yaml", "path to the prune YAML settings file")
	flag.Parse()

	log := logging.New(logging.INFO)

	raw, err := os.ReadFile(*configPath)
	if err != nil {
		log.Error.Printf("prune: read config: %v", err)
		os.Exit(exitFatal)
	}
	var settings pruneSettings
	if err := yaml.Unmarshal(raw, &settings); err != nil {
		log.Error.Printf("prune: parse config: %v", err)
		os.Exit(exitFatal)
	}
	if settings.Debug {
		log = logging.New(logging.DEBUG)
	}

	cfg := prune.Config{
		BillingEventRetentionDays: settings.BillingEventRetentionDays,
		IoLogRetentionDays:        settings.IoLogRetentionDays,
		BatchSize:                 settings.BatchSize,
	}.WithDefaults()
	if err := cfg.Validate(); err != nil {
		log.Error.Printf("prune: invalid config: %v", err)
		os.Exit(exitFatal)
	}

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Error.Printf("prune: DATABASE_URL is required")
		os.Exit(exitFatal)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Error.Printf("prune: open database: %v", err)
		os.Exit(exitFatal)
	}
	defer db.Close()
	// One connection is plenty for a batch DELETE loop; default small so the
	// job does not hold pool capacity, overridable via settings.
	maxOpen := settings.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 2
	}
	db.SetMaxOpenConns(maxOpen)
	if settings.MaxIdleConns > 0 {
		db.SetMaxIdleConns(settings.MaxIdleConns)
	}
	if settings.ConnMaxLifetime != "" {
		d, err := time.ParseDuration(settings.ConnMaxLifetime)
		if err != nil {
			log.Error.Printf("prune: connMaxLifetime: %v", err)
			os.Exit(exitFatal)
		}
		db.SetConnMaxLifetime(d)
	}
	if err := db.Ping(); err != nil {
		log.Error.Printf("prune: ping database: %v", err)
		os.Exit(exitFatal)
	}

	store := prune.NewStore(db)
	ctx := context.Background()

	// billing_event first: it is the unbounded-growth table (the finding).
	// io_log second. Each failure is fatal-on-exit but leaves the other
	// table's completed work standing (idempotent re-run catches up).
	failed := false
	for _, job := range []struct {
		table         prune.Table
		retentionDays int
	}{
		{prune.BillingEvent, cfg.BillingEventRetentionDays},
		{prune.IoLog, cfg.IoLogRetentionDays},
	} {
		res, err := store.Prune(ctx, job.table, job.retentionDays, cfg.BatchSize)
		if err != nil {
			log.Error.Printf("prune: %s: %v", job.table.Name(), err)
			failed = true
			continue
		}
		log.Info.Printf("pruned %s: deleted %d rows older than %s (%dd horizon)",
			res.Table.Name(), res.RowsDeleted, res.Cutoff.UTC().Format(time.RFC3339), job.retentionDays)
	}
	if failed {
		os.Exit(exitFatal)
	}
	os.Exit(exitOK)
}
