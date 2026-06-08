// Command rater is phoebe's REVENUE path batch job. It reads a window of raw
// metering rows from billing_event, joins the effective-dated price book
// (model_price), computes per-event cost in integer micro-USD, and upserts
// per-(auth_id, model, hour) cost rollups into rated_usage.
//
// It is a ONE-SHOT BATCH job (run by cron / a k8s CronJob), NOT a daemon: it
// rates one window and exits. Exit codes:
//
//	0  window rated, every event priced and attributed
//	1  fatal error (config, DB, etc.) — nothing or partial; safe to re-run
//	2  window rated BUT an anomaly leaked (fail-loud): some events were unpriced
//	   (backfill the price book and re-rate) and/or some rows were unattributable
//	   — NULL auth_id/model, which the interceptor's billing gate should reject
//	   before metering, so a nonzero count means revenue is leaking upstream.
//	   Distinct code so a CronJob can alert on "lost revenue / lost data"
//	   separately from "job broke".
//
// Re-running any window is safe and idempotent (rollups upsert on the natural
// key), so cron overlap or manual re-rating never double-counts.
//
// Config, like the drainer: a YAML settings file (flag -f) for pool knobs, and
// the DATABASE_URL env var (Atlas convention) for Postgres. The rater does NOT
// run migrations — it assumes billing_event/model_price/rated_usage exist (see
// migrations/README.md).
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

	MaxOpenConns    int    `yaml:"maxOpenConns"`
	MaxIdleConns    int    `yaml:"maxIdleConns"`
	ConnMaxLifetime string `yaml:"connMaxLifetime"`
}

// exit codes (see package doc).
const (
	exitOK      = 0
	exitFatal   = 1
	exitAnomaly = 2 // window rated but something leaked: unpriced and/or unattributable
)

func main() {
	os.Exit(run())
}

// run is main's body returning an exit code, so deferred Close runs before exit
// (os.Exit skips defers).
func run() int {
	settingsFile := flag.String("f", "/etc/saturn/config/rater.yaml", "Settings YAML file path")
	since := flag.String("since", "", "Window start, RFC3339 (default: start of the last complete hour)")
	until := flag.String("until", "", "Window end, RFC3339 (default: start of the current hour)")
	flag.Parse()

	log := logging.New(logging.INFO)

	cfg, debug, err := loadConfig(*settingsFile)
	if err != nil {
		log.Error.Printf("rater: load config: %v", err)
		return exitFatal
	}
	if debug {
		log.SetLevel(logging.DEBUG)
	}

	windowStart, windowEnd, err := resolveWindow(*since, *until, time.Now())
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

	r := rating.New(store, log)
	res, err := r.Run(ctx, windowStart, windowEnd)
	if err != nil {
		log.Error.Printf("rater: run: %v", err)
		return exitFatal
	}

	if res.HasAnomaly() {
		// The window was rated and rollups were written, but something leaked:
		// events that could not be priced and/or rows that could not be attributed
		// (NULL auth_id/model). Non-zero exit so a CronJob surfaces the lost
		// revenue / lost data.
		return exitAnomaly
	}
	return exitOK
}

// resolveWindow computes [start, end). Defaults (both flags empty) rate the LAST
// COMPLETE hour: [floor(now)-1h, floor(now)). This is the natural CronJob cadence
// — run at :05 past the hour to rate the hour that just closed. Either flag may be
// given explicitly (RFC3339); both must parse and start must be before end.
func resolveWindow(since, until string, now time.Time) (time.Time, time.Time, error) {
	now = now.UTC()
	currentHour := now.Truncate(time.Hour)

	start := currentHour.Add(-time.Hour)
	end := currentHour

	if since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return time.Time{}, time.Time{}, errBadWindow("since", since, err)
		}
		start = t.UTC()
	}
	if until != "" {
		t, err := time.Parse(time.RFC3339, until)
		if err != nil {
			return time.Time{}, time.Time{}, errBadWindow("until", until, err)
		}
		end = t.UTC()
	}
	if !start.Before(end) {
		return time.Time{}, time.Time{}, errInvertedWindow(start, end)
	}
	return start, end, nil
}

// loadConfig reads the YAML settings file, applies rating.DefaultConfig, then
// overlays DATABASE_URL from the environment. A missing settings file is
// tolerated (all defaults + env), so the rater can run env-only.
func loadConfig(path string) (rating.Config, bool, error) {
	cfg := rating.DefaultConfig()

	var s raterSettings
	if data, err := os.ReadFile(path); err == nil {
		if err := yaml.Unmarshal(data, &s); err != nil {
			return cfg, false, err
		}
	} else if !os.IsNotExist(err) {
		return cfg, false, err
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
			return cfg, false, errInvalidDuration("connMaxLifetime", err)
		}
		cfg.ConnMaxLifetime = v
	}

	// DATABASE_URL (Atlas convention) is the authoritative Postgres source.
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")

	return cfg, s.Debug, nil
}
