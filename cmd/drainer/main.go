// Command drainer is the consumer side of phoebe's metering durability ladder.
// It reads metering events from the Valkey Stream the interceptor's emitter
// writes to (via a consumer group) and writes each event durably to Postgres —
// the system of record for raw, pre-rating token counts.
//
// Config comes from two places, matching their respective conventions:
//   - a YAML settings file (flag -f), like the interceptor, for Valkey/stream
//     and consumer-group knobs;
//   - the DATABASE_URL environment variable (Atlas convention) for Postgres.
//
// The drainer does NOT run migrations: it assumes the billing_event table
// already exists in the shared Atlas Postgres (see migrations/README.md). If
// the table is missing, the first upsert fails with a clear error.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v2"

	"github.com/saturncloud/phoebe/internal/drain"
	"github.com/saturncloud/phoebe/internal/logging"
)

// drainSettings is the YAML shape for the drainer. It mirrors drain.Config
// without importing it into a shared config package — the drainer is an
// independent binary and keeps its own config local (same rationale as
// emit.Config / config.EmitSettings).
//
// Durations are strings parsed with time.ParseDuration; "" means "use default".
type drainSettings struct {
	Debug bool `yaml:"debug"`

	ValkeyAddr   string `yaml:"valkeyAddr"`
	StreamName   string `yaml:"streamName"`
	Group        string `yaml:"group"`
	Consumer     string `yaml:"consumer"`
	BatchSize    int    `yaml:"batchSize"`
	BlockTimeout string `yaml:"blockTimeout"`

	ClaimMinIdle  string `yaml:"claimMinIdle"`
	ClaimInterval string `yaml:"claimInterval"`

	MaxOpenConns    int    `yaml:"maxOpenConns"`
	MaxIdleConns    int    `yaml:"maxIdleConns"`
	ConnMaxLifetime string `yaml:"connMaxLifetime"`
}

func main() {
	settingsFile := flag.String("f", "/etc/saturn/config/drainer.yaml", "Settings YAML file path")
	flag.Parse()

	log := logging.New(logging.INFO)

	cfg, debug, err := loadConfig(*settingsFile)
	if err != nil {
		log.Error.Fatalf("drainer: load config: %v", err)
	}
	if debug {
		log.SetLevel(logging.DEBUG)
	}

	if cfg.ValkeyAddr == "" {
		log.Error.Fatalf("drainer: valkeyAddr is required (the drainer is the stream consumer)")
	}

	// Root context cancelled on SIGTERM/SIGINT for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Postgres: open + Ping (pool_pre_ping equivalent) before consuming, so a
	// bad DSN fails fast rather than after we've claimed stream entries.
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	store, err := drain.OpenPostgres(pingCtx, cfg)
	cancel()
	if err != nil {
		log.Error.Fatalf("drainer: %v", err)
	}
	defer func() { _ = store.Close() }()
	log.Info.Printf("drainer: postgres connected (maxOpen=%d maxIdle=%d)", cfg.MaxOpenConns, cfg.MaxIdleConns)

	rdb := redis.NewClient(&redis.Options{Addr: cfg.ValkeyAddr})
	defer func() { _ = rdb.Close() }()

	d := drain.New(cfg, log, rdb, store)
	if err := d.Run(ctx); err != nil {
		log.Error.Fatalf("drainer: run: %v", err)
	}
	log.Info.Printf("drainer: stopped")
}

// loadConfig reads the YAML settings file, applies drain.DefaultConfig, then
// overlays DATABASE_URL from the environment. Returns the assembled
// drain.Config and the debug flag. A missing settings file is tolerated (all
// defaults + env), so the drainer can run env-only in minimal deployments.
func loadConfig(path string) (drain.Config, bool, error) {
	cfg := drain.DefaultConfig()

	var s drainSettings
	if data, err := os.ReadFile(path); err == nil {
		if err := yaml.Unmarshal(data, &s); err != nil {
			return cfg, false, err
		}
	} else if !os.IsNotExist(err) {
		return cfg, false, err
	}

	if s.ValkeyAddr != "" {
		cfg.ValkeyAddr = s.ValkeyAddr
	}
	if s.StreamName != "" {
		cfg.StreamName = s.StreamName
	}
	if s.Group != "" {
		cfg.Group = s.Group
	}
	if s.Consumer != "" {
		cfg.Consumer = s.Consumer
	}
	if s.BatchSize > 0 {
		cfg.BatchSize = s.BatchSize
	}
	if s.MaxOpenConns > 0 {
		cfg.MaxOpenConns = s.MaxOpenConns
	}
	if s.MaxIdleConns > 0 {
		cfg.MaxIdleConns = s.MaxIdleConns
	}

	for _, d := range []struct {
		raw string
		dst *time.Duration
		key string
	}{
		{s.BlockTimeout, &cfg.BlockTimeout, "blockTimeout"},
		{s.ClaimMinIdle, &cfg.ClaimMinIdle, "claimMinIdle"},
		{s.ClaimInterval, &cfg.ClaimInterval, "claimInterval"},
		{s.ConnMaxLifetime, &cfg.ConnMaxLifetime, "connMaxLifetime"},
	} {
		if d.raw == "" {
			continue
		}
		v, err := time.ParseDuration(d.raw)
		if err != nil {
			return cfg, false, errInvalidDuration(d.key, err)
		}
		*d.dst = v
	}

	// DATABASE_URL (Atlas convention) is the authoritative Postgres source; it
	// overrides anything and is required.
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")

	return cfg, s.Debug, nil
}

func errInvalidDuration(key string, err error) error {
	return &configError{key: key, err: err}
}

type configError struct {
	key string
	err error
}

func (e *configError) Error() string { return "invalid " + e.key + ": " + e.err.Error() }
func (e *configError) Unwrap() error { return e.err }
