// Command rater is phoebe's hourly usage AGGREGATOR. It reads a window of raw
// metering rows from billing_event and rolls them into rated_usage at the token
// grain of billing-design.md section 3, keeping the identity checks that need
// raw events (see internal/rating). It fetches no prices and computes no money:
// the manager rates every hour (design D1, ruled by Hugo 2026-10-22).
// cmd/token-push sends the rollups to the manager.
//
// The binary keeps the name "rater" (and its settings file, chart keys and the
// rated_usage table keep theirs) so the saturn-k8s chart needs no change.
//
// It is a ONE-SHOT BATCH job (a k8s CronJob), not a daemon. Exit codes:
//
//	0  window aggregated, nothing withheld
//	1  fatal (config, DB, ...): nothing or partial; safe to re-run
//	2  window aggregated BUT usage was withheld for a reason that needs a person
//	   (unattributable, owner conflict, ambiguous org, ambiguous base model or
//	   adapter, missing base model, invalid serving mode, invalid usage, or a
//	   successful attempt with no usage block), OR a ROUTINE run reconcile-deleted
//	   a rollup an earlier run wrote (usage vanished with no operator behind it).
//
// Exit 3 (an hour skipped because its prices were unavailable) no longer exists:
// there are no prices to wait for.
//
// THE DEFAULT WINDOW IS THE TRAILING N COMPLETE HOURS, [floor(now)-N*1h,
// floor(now)), N = rateTrailingHours (default 24). Events can be drained late (a
// Valkey outage recovered from the WAL) into an hour already aggregated; the
// trailing window picks them up on a later run, and cmd/token-push re-sends the
// same trailing hours, so the manager converges. Re-aggregating is idempotent
// (the upsert REPLACES each bucket). An event more than N hours late still needs
// an explicit --since/--until run.
//
// SINGLE-FLIGHT (deployment contract): two aggregators must never run over
// overlapping windows at once; the reconcile DELETE and the ordered upsert are
// deadlock-safe only then. The chart's CronJob sets concurrencyPolicy: Forbid.
//
// Config: a YAML settings file (flag -f) for pool knobs and rateTrailingHours,
// and DATABASE_URL for Postgres. The aggregator does not run migrations.
package main

import (
	"context"
	"flag"
	"os"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/rating"
)

// raterSettings is the YAML shape for the rater. Durations are strings parsed
// with time.ParseDuration; "" means "use default".
type raterSettings struct {
	Debug bool `yaml:"debug"`

	// RateTrailingHours is N in the default window [floor(now)-N*1h, floor(now)).
	// Default 24; must be >= 1. A pointer so an explicit 0 fails loud instead of
	// silently meaning "default".
	RateTrailingHours *int `yaml:"rateTrailingHours"`

	// ManagerURL is no longer read: the aggregator never calls the manager. The
	// key is still accepted so a chart that sets it keeps working; run() logs a
	// warning so the operator removes it.
	ManagerURL string `yaml:"managerURL"`

	MaxOpenConns    int    `yaml:"maxOpenConns"`
	MaxIdleConns    int    `yaml:"maxIdleConns"`
	ConnMaxLifetime string `yaml:"connMaxLifetime"`
}

// legacyTokenEnv is the customer auth token the rater needed to fetch prices.
// It no longer uses it; run() warns if it is still mounted, because a pod
// should not hold a credential it does not use.
const legacyTokenEnv = "SATURN_TOKEN"

// defaultRateTrailingHours is the default N for the trailing window.
const defaultRateTrailingHours = 24

// exit codes (see package doc).
const (
	exitOK      = 0
	exitFatal   = 1
	exitAnomaly = 2
)

// exitCode maps a completed run to its exit code. Withheld usage always exits
// exitAnomaly. A reconcile-delete exits exitAnomaly only on a ROUTINE run
// (windowExplicit == false); on an explicit --since/--until backfill it is
// the convergence the operator asked for. Only cmd/rater knows which it was.
func exitCode(reconciledDeletions int64, windowExplicit, hasAnomaly bool) int {
	if hasAnomaly {
		return exitAnomaly
	}
	if reconciledDeletions > 0 && !windowExplicit {
		return exitAnomaly
	}
	return exitOK
}

func main() {
	os.Exit(run())
}

// run is main's body returning an exit code, so deferred Close runs before exit.
func run() int {
	settingsFile := flag.String("f", "/etc/saturn/config/rater.yaml", "Settings YAML file path")
	// -manager-url is accepted and ignored, for the same reason as the managerURL
	// settings key: an older CronJob spec that passes it must not crash-loop.
	managerFlag := flag.String("manager-url", "", "Ignored: the aggregator no longer calls the manager")
	since := flag.String("since", "", "Window start, RFC3339 (default: floor(now) minus rateTrailingHours)")
	until := flag.String("until", "", "Window end, RFC3339 (default: start of the current hour)")
	flag.Parse()

	log := logging.New(logging.INFO)

	cfg, opts, err := loadConfig(*settingsFile)
	if err != nil {
		log.Error.Printf("rater: load config: %v", err)
		return exitFatal
	}
	if opts.debug {
		log.SetLevel(logging.DEBUG)
	}
	if opts.managerURL != "" || *managerFlag != "" {
		log.Warn.Printf("rater: managerURL is set but no longer used (the manager prices usage; the rater only aggregates); remove it from the rater settings")
	}
	if os.Getenv(legacyTokenEnv) != "" {
		log.Warn.Printf("rater: %s is set but no longer used (the rater makes no manager calls); remove it from the rater pod so it does not hold an unused credential", legacyTokenEnv)
	}

	windowStart, windowEnd, windowExplicit, err := resolveWindow(*since, *until, opts.trailingHours, time.Now())
	if err != nil {
		log.Error.Printf("rater: %v", err)
		return exitFatal
	}

	ctx, stop := signalContext()
	defer stop()

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	store, err := rating.OpenPostgres(pingCtx, cfg)
	cancel()
	if err != nil {
		log.Error.Printf("rater: %v", err)
		return exitFatal
	}
	defer func() { _ = store.Close() }()

	res, err := rating.New(store, log).Run(ctx, windowStart, windowEnd, windowExplicit)
	if err != nil {
		log.Error.Printf("rater: run: %v", err)
		return exitFatal
	}
	return exitCode(res.ReconciledDeletions, windowExplicit, res.HasAnomaly())
}

// resolveWindow computes [start, end). Defaults (both flags empty) cover the
// TRAILING trailingHours COMPLETE hours: [floor(now)-N*1h, floor(now)). Either
// flag may be given explicitly (RFC3339) and wins over the default; both must
// parse, be hour-aligned, and start must be before end.
//
// The third return is windowExplicit: TRUE if EITHER --since or --until was set,
// i.e. the operator named the window. It decides whether a reconcile-delete is
// alarming (routine) or intended (backfill); see exitCode.
func resolveWindow(since, until string, trailingHours int, now time.Time) (time.Time, time.Time, bool, error) {
	if trailingHours < 1 {
		return time.Time{}, time.Time{}, false, errInvalidTrailingHours(trailingHours)
	}
	now = now.UTC()
	currentHour := now.Truncate(time.Hour)

	start := currentHour.Add(-time.Duration(trailingHours) * time.Hour)
	end := currentHour

	windowExplicit := since != "" || until != ""

	if since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return time.Time{}, time.Time{}, false, errBadWindow("since", since, err)
		}
		start = t.UTC()
	}
	if until != "" {
		t, err := time.Parse(time.RFC3339, until)
		if err != nil {
			return time.Time{}, time.Time{}, false, errBadWindow("until", until, err)
		}
		end = t.UTC()
	}
	if !start.Before(end) {
		return time.Time{}, time.Time{}, false, errInvertedWindow(start, end)
	}
	// Hour-aligned only: the upsert REPLACES a bucket, so a sub-hour bound would
	// overwrite an hour's complete rollup with a partial sum. Fail loud rather
	// than snap, so an operator never silently re-aggregates hours they did not name.
	if !start.Truncate(time.Hour).Equal(start) {
		return time.Time{}, time.Time{}, false, errUnalignedWindow("since", start)
	}
	if !end.Truncate(time.Hour).Equal(end) {
		return time.Time{}, time.Time{}, false, errUnalignedWindow("until", end)
	}
	return start, end, windowExplicit, nil
}

// raterOptions are the non-pool knobs loadConfig resolves from the settings file.
type raterOptions struct {
	debug         bool
	trailingHours int    // validated >= 1, defaulted to defaultRateTrailingHours
	managerURL    string // read only to warn that it is unused
}

// loadConfig reads the YAML settings file, applies rating.DefaultConfig, then
// overlays DATABASE_URL from the environment. A missing settings file is
// tolerated (all defaults + env).
func loadConfig(path string) (rating.Config, raterOptions, error) {
	cfg := rating.DefaultConfig()
	opts := raterOptions{trailingHours: defaultRateTrailingHours}

	var s raterSettings
	if data, err := os.ReadFile(path); err == nil {
		if err := yaml.Unmarshal(data, &s); err != nil {
			return cfg, opts, err
		}
	} else if !os.IsNotExist(err) {
		return cfg, opts, err
	}

	if s.RateTrailingHours != nil {
		if *s.RateTrailingHours < 1 {
			return cfg, opts, errInvalidTrailingHours(*s.RateTrailingHours)
		}
		opts.trailingHours = *s.RateTrailingHours
	}
	if s.MaxOpenConns > 0 {
		cfg.MaxOpenConns = s.MaxOpenConns
	}
	if s.MaxIdleConns > 0 {
		cfg.MaxIdleConns = s.MaxIdleConns
	}
	if s.ConnMaxLifetime != "" {
		v, err := time.ParseDuration(s.ConnMaxLifetime)
		if err != nil {
			return cfg, opts, errInvalidDuration("connMaxLifetime", err)
		}
		cfg.ConnMaxLifetime = v
	}

	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	opts.managerURL = s.ManagerURL
	opts.debug = s.Debug
	return cfg, opts, nil
}
