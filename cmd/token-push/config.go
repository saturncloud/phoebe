package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/saturncloud/phoebe/internal/rating"
)

// ensureUTCTimeZone pins the session TimeZone to UTC via the DSN unless one is already
// set. Mirrors internal/rating's unexported helper; the pusher's window_start equality
// filter compares absolute instants, so this is defense-in-depth, not load-bearing.
func ensureUTCTimeZone(dsn string) string {
	if strings.Contains(strings.ToLower(dsn), "timezone") {
		return dsn
	}
	if strings.Contains(dsn, "://") {
		if strings.Contains(dsn, "?") {
			return dsn + "&timezone=UTC"
		}
		return dsn + "?timezone=UTC"
	}
	return dsn + " timezone=UTC"
}

// pushOptions are the resolved knobs from the settings file (+ flag/env overrides).
type pushOptions struct {
	debug          bool
	managerURL     string
	trailingHours  int
	requestTimeout time.Duration
}

// pushSettings is the YAML shape for the settings file. Mirrors the rater's
// settings style (a thin struct per binary, no shared config package). The customer
// auth token is NOT here — it comes from SATURN_TOKEN (a mounted secret).
type pushSettings struct {
	Debug bool `yaml:"debug"`

	// ManagerURL is the base URL of the central manager. The endpoint path
	// (/customer/token-usage) is appended by this binary.
	ManagerURL string `yaml:"managerURL"`

	// PushTrailingHours is N in the default window: how many complete trailing hours
	// each run snapshots. Default 24; must be >= 1. A pointer so an explicit 0 fails
	// loud instead of silently meaning "default".
	PushTrailingHours *int `yaml:"pushTrailingHours"`

	// RequestTimeout bounds a single window POST. "" means defaultRequestTimeout.
	RequestTimeout string `yaml:"requestTimeout"`

	// Pool knobs for the read-only DB connection.
	MaxOpenConns    int    `yaml:"maxOpenConns"`
	MaxIdleConns    int    `yaml:"maxIdleConns"`
	ConnMaxLifetime string `yaml:"connMaxLifetime"`
}

// loadConfig reads the YAML settings file, applies rating.DefaultConfig for the pool,
// overlays DATABASE_URL from the env, and resolves the push knobs. A missing settings
// file is tolerated (all defaults + env), so the binary can run env-only.
func loadConfig(path string) (rating.Config, pushOptions, error) {
	cfg := rating.DefaultConfig()
	opts := pushOptions{
		trailingHours:  defaultPushTrailingHours,
		requestTimeout: defaultRequestTimeout,
	}

	var s pushSettings
	if data, err := os.ReadFile(path); err == nil {
		if err := yaml.UnmarshalStrict(data, &s); err != nil {
			return cfg, opts, err
		}
	} else if !os.IsNotExist(err) {
		return cfg, opts, err
	}

	if s.PushTrailingHours != nil {
		if *s.PushTrailingHours < 1 {
			return cfg, opts, fmt.Errorf("pushTrailingHours must be >= 1, got %d", *s.PushTrailingHours)
		}
		opts.trailingHours = *s.PushTrailingHours
	}
	if s.RequestTimeout != "" {
		d, err := time.ParseDuration(s.RequestTimeout)
		if err != nil {
			return cfg, opts, fmt.Errorf("invalid requestTimeout %q: %w", s.RequestTimeout, err)
		}
		if d <= 0 {
			return cfg, opts, fmt.Errorf("requestTimeout must be positive, got %q", s.RequestTimeout)
		}
		opts.requestTimeout = d
	}
	if s.MaxOpenConns > 0 {
		cfg.MaxOpenConns = s.MaxOpenConns
	}
	if s.MaxIdleConns > 0 {
		cfg.MaxIdleConns = s.MaxIdleConns
	}
	if s.ConnMaxLifetime != "" {
		d, err := time.ParseDuration(s.ConnMaxLifetime)
		if err != nil {
			return cfg, opts, fmt.Errorf("invalid connMaxLifetime %q: %w", s.ConnMaxLifetime, err)
		}
		cfg.ConnMaxLifetime = d
	}

	// DATABASE_URL (Atlas convention) is the authoritative Postgres source.
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	opts.debug = s.Debug
	opts.managerURL = s.ManagerURL
	return cfg, opts, nil
}

// resolveWindows returns the list of hour-aligned window_start values to snapshot,
// one per hour in [start, end). The default (both flags empty) is the trailing
// trailingHours complete hours: [floor(now)-N*1h, floor(now)). Each hour is pushed as
// its own snapshot (the manager keys delete-by-absence per window_start), so a re-rate
// or reconcile-delete in any covered hour reconverges on the next run.
//
// Both bounds MUST be hour-aligned, same reason as the rater: a snapshot for a
// partial hour would tell the manager to delete-by-absence the rollups in the rest of
// that hour. Fail loud rather than snap, so an operator never silently mis-scopes a
// backfill.
func resolveWindows(since, until string, trailingHours int, now time.Time) ([]time.Time, error) {
	if trailingHours < 1 {
		return nil, fmt.Errorf("pushTrailingHours must be >= 1, got %d", trailingHours)
	}
	now = now.UTC()
	currentHour := now.Truncate(time.Hour)

	start := currentHour.Add(-time.Duration(trailingHours) * time.Hour)
	end := currentHour

	if since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return nil, fmt.Errorf("invalid -since %q: %w", since, err)
		}
		start = t.UTC()
	}
	if until != "" {
		t, err := time.Parse(time.RFC3339, until)
		if err != nil {
			return nil, fmt.Errorf("invalid -until %q: %w", until, err)
		}
		end = t.UTC()
	}
	// Alignment BEFORE ordering: a misaligned -since that also happens to land after
	// the default end would otherwise report a misleading "not before end" instead of
	// the real "not hour-aligned" cause.
	if !start.Truncate(time.Hour).Equal(start) {
		return nil, fmt.Errorf("-since %s is not hour-aligned", start.Format(time.RFC3339))
	}
	if !end.Truncate(time.Hour).Equal(end) {
		return nil, fmt.Errorf("-until %s is not hour-aligned", end.Format(time.RFC3339))
	}
	if !start.Before(end) {
		return nil, fmt.Errorf("window start %s is not before end %s", start.Format(time.RFC3339), end.Format(time.RFC3339))
	}
	// Never snapshot a FUTURE window: future hours have no rated_usage rows, so an
	// empty snapshot for them would signal delete-all to the manager for hours that
	// should not exist, and a far-future -until would also generate an unbounded number
	// of windows. The end is clamped to the current hour (the rater only rates closed
	// hours, so there is never anything to push past it).
	if end.After(currentHour) {
		return nil, fmt.Errorf("-until %s is in the future (past the current hour %s); the rater only rates closed hours and a future snapshot would signal delete-all", end.Format(time.RFC3339), currentHour.Format(time.RFC3339))
	}
	// Bound the span on the DEEP-PAST side too (symmetric to the future clamp above): a
	// mistyped or over-broad -since (e.g. a stray year) would enumerate tens of
	// thousands of historical hours, each an EMPTY snapshot = delete-all for that hour,
	// silently un-billing historical revenue the manager still holds — plus a query +
	// POST per hour. Fail loud rather than enumerate: an over-wide range is almost
	// certainly an operator typo, and a legitimate wide backfill should be deliberate
	// (narrow the range, or raise the cap on purpose). The cap is far above any real
	// backfill (90 days) and far below the 56k+ a stray year produces.
	if n := int(end.Sub(start) / time.Hour); n > maxPushWindows {
		return nil, fmt.Errorf("window range %s .. %s spans %d hours, exceeding the %d-hour (%d-day) cap; a range this wide is almost certainly a mistyped -since and would push %d empty historical snapshots, each a delete-by-absence (un-bill) for that hour — narrow -since/-until or raise the cap deliberately",
			start.Format(time.RFC3339), end.Format(time.RFC3339), n, maxPushWindows, maxPushWindows/24, n)
	}

	var windows []time.Time
	for w := start; w.Before(end); w = w.Add(time.Hour) {
		windows = append(windows, w)
	}
	return windows, nil
}
