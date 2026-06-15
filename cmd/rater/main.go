// Command rater is phoebe's REVENUE path batch job. It loads a YAML PRICE FILE (E1)
// — base per-token rates keyed on the HF model id, the single global fine-tune
// premium policy, and per-GPU floor rates — reads a window of raw metering rows from
// billing_event, projects the prices into a transient TEMP table, and ENTIRELY IN
// SQL computes per-event cost as exact NUMERIC, sums it, and upserts
// per-(auth_id, model_id, hour) cost rollups into rated_usage (with the applied
// per-token rates frozen onto each row). No money math happens in Go — the rater
// binary is orchestration only (the fine-tune premium is applied in exact decimal
// when the prices are projected, then handed to SQL as NUMERIC).
//
// It is a ONE-SHOT BATCH job (run by cron / a k8s CronJob), NOT a daemon: it
// rates one window and exits. Exit codes:
//
//	0  window rated, every event priced and attributed
//	1  fatal error (config, DB, etc.) — nothing or partial; safe to re-run
//	2  window rated BUT an anomaly leaked (fail-loud): some events were unpriced
//	   (backfill the price book and re-rate), some rows were unattributable — NULL
//	   auth_id/model_id, which the interceptor's billing gate should reject before
//	   metering — and/or an ft: rollup spanned more than one base_model in a window
//	   (the E3 ft-uniqueness violation: a uuid4 checkpoint id cannot carry two bases,
//	   so this is broken base_model propagation, never a priceable rollup). Any of
//	   these means revenue is leaking upstream. Distinct code so a CronJob can alert
//	   on "lost revenue / lost data" separately from "job broke". A ROUTINE run (the
//	   default trailing-hours window, no --since/--until) that RECONCILE-DELETED a
//	   previously-billed rated_usage row ALSO exits 2: on a routine cadence a prior
//	   bill vanishing means data was lost / an upstream regression dropped events —
//	   page someone (see exitCode and the reconcile-exit contract below).
//
// Re-running any window is safe and idempotent (rollups upsert on the natural
// key), so cron overlap or manual re-rating never double-counts.
//
// THE RECONCILE-DELETE EXIT CONTRACT (option (c): loud on routine, quiet on
// backfill). A run that DELETES a previously-billed rated_usage row
// (ReconciledDeletions > 0) is a REVENUE CHANGE — a customer's prior bill just got
// rewritten. Whether that is alarming depends on WHY the window was chosen:
//   - DEFAULT trailing-hours window (no --since/--until): a routine cron run should
//     NOT be rewriting prior bills. If it does, events that billed before have
//     vanished from billing_event (data loss) or an upstream regression now drops
//     them (e.g. base_model stopped propagating → rollups went unpriced/ambiguous).
//     That is an ERROR + exit 2 — treat it like the other anomalies and page.
//   - EXPLICIT --since/--until window (operator backfill, e.g. after a late price
//     fix): convergence is exactly what the operator asked for — INFO + exit 0.
//
// The gate is ReconciledDeletions > 0 && !windowExplicit. The semantics are
// UNCHANGED either way ("what the latest run says is what bills" — store.go always
// reconciles); this is purely the observability/exit contract. The decision lives
// HERE, in cmd/rater, because only here is it known whether the window was explicit;
// it is deliberately NOT folded into Result.HasAnomaly() (that would make explicit
// backfills exit nonzero too).
//
// SINGLE-FLIGHT ASSUMPTION (deployment contract, enforced OUTSIDE this repo). The
// rater MUST run single-flight: no two rater processes rating overlapping windows
// concurrently. The reconcile DELETE (the `deleted` CTE in store.go) and the
// ordered upsert are deadlock-safe ONLY under single-flight — the upsert's
// ORDER BY prevents a rater self-deadlocking against its own rows, but two
// concurrent raters could still take row locks in opposing orders across the
// DELETE/INSERT pair (a classic ABBA hazard). The deployment MUST forbid
// concurrency: the Atlas CronJob sets `concurrencyPolicy: Forbid` so a slow run is
// never overlapped by the next tick. That manifest lives in the Atlas repo, not
// here; this rater does NOT add delete-lock-ordering machinery because single-flight
// makes the hazard unreachable. If you ever run the rater outside that CronJob,
// preserve single-flight some other way (an advisory lock, a queue) before doing so.
//
// THE DEFAULT WINDOW IS THE TRAILING N COMPLETE HOURS, [floor(now)-N*1h,
// floor(now)), N = rateTrailingHours (default 24). It is deliberately NOT just the
// last hour: events can be DRAINED LATE (a Valkey outage recovered from the WAL)
// with an event_ts in an hour that was already rated, and a last-hour-only cron
// would never revisit that hour — silent revenue loss. Re-rating closed hours is
// safe BY CONSTRUCTION: the upsert REPLACES each (auth_id, model_id, hour) bucket
// with a freshly recomputed total, so the late event is folded in and nothing
// doubles. RESIDUAL RISK, stated honestly: an event arriving MORE than N hours
// late still slips past the default window (the DESIGN.md reconciliation backstop
// is the eventual answer; until then, widen rateTrailingHours or re-rate the hour
// explicitly with --since/--until).
//
// Config, like the drainer: a YAML settings file (flag -f) for pool knobs and
// rateTrailingHours, the DATABASE_URL env var (Atlas convention) for Postgres, and a
// price-file path (settings `priceFile` or flag -prices) for the YAML price book. The
// rater does NOT run migrations — it assumes billing_event/rated_usage exist (see
// migrations/README.md). It FAILS CLOSED if the price file is missing or malformed:
// it refuses to rate rather than bill at $0.
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

// raterSettings is the YAML shape for the rater. Mirrors rating.Config without
// importing it into a shared config package (same rationale as drainSettings).
// Durations are strings parsed with time.ParseDuration; "" means "use default".
type raterSettings struct {
	Debug bool `yaml:"debug"`

	// RateTrailingHours is N in the default window [floor(now)-N*1h, floor(now)):
	// how many complete trailing hours each run (re-)rates, to catch late-drained
	// events (see the package doc). Default 24; must be >= 1. A pointer so an
	// explicit 0 fails loud instead of silently meaning "default".
	RateTrailingHours *int `yaml:"rateTrailingHours"`

	// PriceFile is the path to the YAML price book (E1). Required (via settings or
	// the -prices flag): the rater cannot price without it. A local path now; the
	// S3-fetch is out of scope (fetch-to-local then point this at the local copy —
	// the create-time price gate and the rater must read the same file/version).
	PriceFile string `yaml:"priceFile"`

	MaxOpenConns    int    `yaml:"maxOpenConns"`
	MaxIdleConns    int    `yaml:"maxIdleConns"`
	ConnMaxLifetime string `yaml:"connMaxLifetime"`
}

// defaultRateTrailingHours is the default N for the trailing window. 24 trades a
// cheap re-rate of already-correct hours (the upsert is a no-op REPLACE) for a full
// day of late-drain tolerance.
const defaultRateTrailingHours = 24

// exit codes (see package doc).
const (
	exitOK      = 0
	exitFatal   = 1
	exitAnomaly = 2 // window rated but something leaked: unpriced and/or unattributable
)

// exitCode maps a completed rating run to its process exit code, encoding the
// reconcile-delete contract (see the package doc). A run that rated cleanly but
// leaked an anomaly (unpriced / unattributable / ambiguous-base) ALWAYS exits
// exitAnomaly. A reconcile-delete (reconciledDeletions > 0) exits exitAnomaly TOO —
// but ONLY when the window was the default trailing-hours window (windowExplicit ==
// false): on a routine run a prior bill vanishing is alarming (data loss / upstream
// regression). When the operator chose the window explicitly (--since/--until),
// reconcile-deletes are intended convergence (a backfill) and do NOT raise the exit
// code on their own. This is the single place the routine-vs-backfill decision is
// made, because only cmd/rater knows whether the window was explicit.
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

// run is main's body returning an exit code, so deferred Close runs before exit
// (os.Exit skips defers).
func run() int {
	settingsFile := flag.String("f", "/etc/saturn/config/rater.yaml", "Settings YAML file path")
	pricesFlag := flag.String("prices", "", "Price YAML file path (overrides settings priceFile)")
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

	// The -prices flag overrides the settings priceFile. The price file is REQUIRED:
	// without it the rater cannot price (it must never default to $0). Load+validate
	// up front so a bad file fails the job before any DB work — fail closed.
	pricePath := opts.priceFile
	if *pricesFlag != "" {
		pricePath = *pricesFlag
	}
	if pricePath == "" {
		log.Error.Printf("rater: no price file configured (set priceFile in the settings file or pass -prices); the rater cannot rate without prices")
		return exitFatal
	}
	book, err := rating.LoadPriceBook(pricePath)
	if err != nil {
		log.Error.Printf("rater: load price file %q: %v (refusing to rate — never bill at $0)", pricePath, err)
		return exitFatal
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

	r := rating.New(store, book, log)
	res, err := r.Run(ctx, windowStart, windowEnd)
	if err != nil {
		log.Error.Printf("rater: run: %v", err)
		return exitFatal
	}

	// Exit code encodes BOTH the leaked-anomaly signal (unpriced / unattributable /
	// ambiguous-base — always nonzero) AND the reconcile-delete contract: a routine
	// run (default window) that rewrote a prior bill is alarming and exits nonzero,
	// while an explicit backfill (--since/--until) that did so is intended and exits
	// 0. See exitCode and the package doc.
	return exitCode(res.ReconciledDeletions, windowExplicit, res.HasAnomaly())
}

// resolveWindow computes [start, end). Defaults (both flags empty) rate the
// TRAILING trailingHours COMPLETE hours: [floor(now)-N*1h, floor(now)). The
// natural CronJob cadence is still hourly (run at :05 past the hour); each run
// re-rates the last N closed hours so an event DRAINED LATE into an already-rated
// hour (Valkey outage → WAL recovery) is picked up by a later run instead of being
// lost forever. Re-rating a closed hour is safe by construction: the upsert
// REPLACES each (auth_id, model_id, hour) bucket with a recomputed total, so
// re-runs reconcile and never double-count. An event arriving more than N hours
// late still slips (residual risk; see the package doc — reconciliation is the
// backstop). Either flag may be given explicitly (RFC3339) and WINS over the
// default; both must parse, be hour-aligned, and start must be before end.
//
// The third return is windowExplicit: TRUE if EITHER --since or --until was set, i.e.
// the operator named the window rather than taking the trailing-hours default. It is
// the single source of truth for the routine-vs-backfill distinction the
// reconcile-delete exit contract turns on (see exitCode + the package doc): a
// reconcile-delete on a routine (default) window is alarming; on an explicit backfill
// it is intended convergence.
func resolveWindow(since, until string, trailingHours int, now time.Time) (time.Time, time.Time, bool, error) {
	if trailingHours < 1 {
		return time.Time{}, time.Time{}, false, errInvalidTrailingHours(trailingHours)
	}
	now = now.UTC()
	currentHour := now.Truncate(time.Hour)

	start := currentHour.Add(-time.Duration(trailingHours) * time.Hour)
	end := currentHour

	// Either flag set means the operator chose the window — an explicit backfill, not
	// the routine trailing-hours cadence. Captured BEFORE parsing so it reflects
	// operator INTENT (a flag was passed) independent of the parsed values.
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
	// Both bounds MUST be hour-aligned. The rollup buckets by date_trunc('hour')
	// and the upsert REPLACES a bucket's totals (not additive — that is what makes
	// a re-run idempotent), so a sub-hour bound would rate only part of a
	// date-trunc'd hour and overwrite that hour's complete rollup with a partial
	// sum → silent under-bill. The default window is hour-aligned by construction;
	// this fences the explicit --since/--until path. Fail loud rather than snap,
	// so an operator never silently re-rates hours they did not name.
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
	priceFile     string // path to the YAML price book (may be overridden by -prices)
}

// loadConfig reads the YAML settings file, applies rating.DefaultConfig, then
// overlays DATABASE_URL from the environment. A missing settings file is
// tolerated (all defaults + env), so the rater can run env-only.
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

	// DATABASE_URL (Atlas convention) is the authoritative Postgres source.
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")

	opts.priceFile = s.PriceFile
	opts.debug = s.Debug
	return cfg, opts, nil
}
