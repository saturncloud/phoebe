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
//	2  window aggregated BUT evidence could not become any record (unattributable,
//	   invalid serving mode, invalid usage, or a successful attempt with no usage
//	   block), OR a ROUTINE run reconcile-deleted a rollup an earlier run wrote
//	   for a reason other than parking (usage vanished with no operator behind it).
//	4  window aggregated, nothing in exit 2's list, BUT unbilled usage is PARKED
//	   (owner conflict, ambiguous org, ambiguous base model or adapter, missing
//	   org; any hour, any age). Returned by EVERY run until the parked usage is
//	   resolved, so a Job alert keeps firing. List it with -list-parked.
//
// Precedence: 1 > 2 > 4 > 0. Exit 3 (an hour skipped because its prices were
// unavailable) no longer exists: there are no prices to wait for.
//
// PARKED USAGE. A conflict holds back only the conflicted identity (auth,
// owner, resource, model, serving mode) in that hour: its usage is recorded in
// parked_usage with the reason and token counts, every other identity of the
// hour is aggregated and sent on time, and token-push lists the conflicted
// identity's already-sent usage_ids in `held` so the manager keeps them.
// Parked usage is never dropped: every run re-checks parked hours from the last
// ParkedRescanReach (about 7 days, inside the pruner's hard floor), logs every
// unresolved row at ERROR, and when a conflict is gone the hour is aggregated
// and token-push re-sends it, however old. Older parked hours are listed until
// an operator re-runs them with --since/--until.
//
// `phoebe-rater -list-parked` prints the unresolved parked usage and exits 0
// (4 when there is any), without aggregating. Its RAW_EVENTS column marks a row
// whose billing_event rows the pruner has removed (30-day horizon, no exception
// for parked usage; Hugo, 2026-10-27): "raw_events: pruned — settle manually via
// manager credit". Such an hour is never re-aggregated; a window containing it is
// refused.
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
	"fmt"
	"io"
	"os"
	"text/tabwriter"
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

	// ManagerURL is not used: the aggregator never calls the manager (Hugo
	// approved its removal from the rater, 2026-10-27). The key is still parsed
	// only so an old ConfigMap that carries it during an upgrade (chart and image
	// do not roll out at the same instant) gets one warning, never a failure.
	ManagerURL string `yaml:"managerURL"`

	MaxOpenConns    int    `yaml:"maxOpenConns"`
	MaxIdleConns    int    `yaml:"maxIdleConns"`
	ConnMaxLifetime string `yaml:"connMaxLifetime"`
}

// defaultRateTrailingHours is the default N for the trailing window.
const defaultRateTrailingHours = 24

// exit codes (see package doc).
const (
	exitOK      = 0
	exitFatal   = 1
	exitAnomaly = 2
	exitParked  = 4
)

// exitCode maps a completed run to its exit code. Withheld usage always exits
// exitAnomaly. A reconcile-delete exits exitAnomaly only on a ROUTINE run
// (windowExplicit == false); on an explicit --since/--until backfill it is
// the convergence the operator asked for. Only cmd/rater knows which it was.
// Parked usage exits exitParked when nothing outranks it.
func exitCode(reconciledDeletions int64, windowExplicit, hasAnomaly, hasParked bool) int {
	if hasAnomaly {
		return exitAnomaly
	}
	if reconciledDeletions > 0 && !windowExplicit {
		return exitAnomaly
	}
	if hasParked {
		return exitParked
	}
	return exitOK
}

func main() {
	os.Exit(run())
}

// run is main's body returning an exit code, so deferred Close runs before exit.
func run() int {
	return runWith(os.Args[1:], logging.New(logging.INFO))
}

// runWith parses argv and runs one aggregation; tests drive it directly. The
// rater reads no credential: it makes no manager call, so SATURN_TOKEN is not
// read from the environment at all.
func runWith(argv []string, log *logging.Logger) int {
	fs := flag.NewFlagSet("rater", flag.ContinueOnError)
	settingsFile := fs.String("f", "/etc/saturn/config/rater.yaml", "Settings YAML file path")
	since := fs.String("since", "", "Window start, RFC3339 (default: floor(now) minus rateTrailingHours)")
	until := fs.String("until", "", "Window end, RFC3339 (default: start of the current hour)")
	listParked := fs.Bool("list-parked", false, "Print the unresolved parked (unbilled) usage and exit; no aggregation")
	if err := fs.Parse(argv); err != nil {
		return exitFatal
	}

	cfg, opts, err := loadConfig(*settingsFile)
	if err != nil {
		log.Error.Printf("rater: load config: %v", err)
		return exitFatal
	}
	if opts.debug {
		log.SetLevel(logging.DEBUG)
	}
	if opts.managerURL != "" {
		log.Warn.Printf("rater: ignoring managerURL in %s; the rater no longer calls the manager (an old ConfigMap during an upgrade; the current chart does not set it)", *settingsFile)
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

	if *listParked {
		parked, err := store.ListParked(ctx)
		if err != nil {
			log.Error.Printf("rater: %v", err)
			return exitFatal
		}
		printParked(os.Stdout, parked)
		if len(parked) > 0 {
			return exitParked
		}
		return exitOK
	}

	res, err := rating.New(store, log).Run(ctx, windowStart, windowEnd, windowExplicit)
	if err != nil {
		log.Error.Printf("rater: run: %v", err)
		return exitFatal
	}
	return exitCode(res.ReconciledDeletions, windowExplicit, res.HasAnomaly(), res.HasParked())
}

// printParked writes one tab-separated line per parked identity-hour, with a
// header, for an operator.
func printParked(w io.Writer, parked []rating.Parked) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "HOUR\tREASON\tAUTH_ID\tOWNER\tRESOURCE_ID\tMODEL_ID\tMODE\tEVENTS\tPROMPT\tCACHED\tCOMPLETION\tFIRST_PARKED\tRAW_EVENTS")
	for _, p := range parked {
		owner := p.OwnerType + "/" + p.OwnerID
		if p.OwnerType == "" {
			owner = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%s\t%s\n",
			p.WindowStart.Format(time.RFC3339), p.Reason, p.AuthID, owner, p.ResourceID, p.ModelID, p.ServingMode,
			p.EventCount, p.PromptTokens, p.CachedTokens, p.CompletionTokens, p.FirstParkedAt.UTC().Format(time.RFC3339),
			p.RawEventsLabel())
	}
	_ = tw.Flush()
	fmt.Fprintf(w, "%d parked identity-hour(s)\n", len(parked))
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
