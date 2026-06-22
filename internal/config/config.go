// Package config loads interceptor settings from a YAML file, applying
// defaults then parsing/validating. The load → defaults → unmarshal → parse
// flow mirrors auth-server's util.Settings.
package config

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v2"
)

// Settings stores configuration for the interceptor.
type Settings struct {
	// --- YAML file settings ---

	Debug bool `yaml:"debug"`

	// Port the interceptor listens on for inbound (post-Traefik) traffic.
	ListenPort int `yaml:"listenPort"`

	// IdleTimeout bounds how long an idle streaming connection may stay open.
	// Token streams can idle between chunks, so this is intentionally long.
	IdleTimeoutStr string `yaml:"idleTimeout"`

	// BillPartialOnAbort decides whether a client-aborted request still emits
	// a metering event for the partial token count. Explicit policy, not a
	// silent default.
	BillPartialOnAbort bool `yaml:"billPartialOnAbort"`

	// Emit configures the durable metering emitter (M2). main.go translates
	// these into an emit.Config for the same reason.
	Emit EmitSettings `yaml:"emit"`

	// IOLog configures the M5 I/O-logging subsystem (opt-in body capture).
	// OFF by default — see IOLogSettings. main.go translates these into an
	// iolog.Config + iolog.StaticPolicy, keeping config free of an iolog
	// dependency (same pattern as Registry/Emit).
	IOLog IOLogSettings `yaml:"ioLog"`

	// --- Parsed settings (populated by parse) ---

	ListenAddr  string        `yaml:"-"`
	IdleTimeout time.Duration `yaml:"-"`

	// configDir is the directory the settings file was loaded from; relative
	// paths in the YAML are resolved against it.
	configDir string
}

// EmitSettings is the YAML shape for the durable emitter. Mirrors emit.Config
// without importing it.
type EmitSettings struct {
	// ValkeyAddr is the Valkey/Redis address. Empty disables Valkey (WAL-only).
	ValkeyAddr string `yaml:"valkeyAddr"`
	StreamName string `yaml:"streamName"`
	// WALPath is the WAL DIRECTORY (the WAL is a tidwall/wal segment log). A
	// legacy single-file JSONL WAL from a pre-upgrade release found at this
	// exact path is imported on startup and renamed aside to
	// "<walPath>.imported" — do not change the configured path across the
	// upgrade, or the old file's events won't be found.
	WALPath string `yaml:"walPath"`
}

// IOLogSettings is the YAML shape for the M5 I/O-logging subsystem. Mirrors
// iolog.Config + iolog.StaticPolicy without importing them (avoids a
// config→iolog dependency).
//
// FAIL CLOSED: Enabled defaults to false and SampleRate to 0.0, so I/O logging
// captures nothing unless an operator explicitly turns it on. Bodies are
// sensitive; capturing them is always a deliberate opt-in.
type IOLogSettings struct {
	// Enabled is the global kill switch for body capture. Default false.
	Enabled bool `yaml:"enabled"`

	// SampleRate is the fraction of opted-in requests to capture, [0,1].
	// Default 0.0 (capture nothing even when Enabled).
	SampleRate float64 `yaml:"sampleRate"`

	// AllowAuthIDs / AllowGroupIDs are the per-tenant opt-in allowlists. Empty
	// opts in NO ONE (fail-closed) — forgetting the allowlist must not capture
	// every tenant's bodies. For deliberate fleet-wide debug capture, set
	// allowAllTenants: true explicitly.
	AllowAuthIDs  []string `yaml:"allowAuthIds"`
	AllowGroupIDs []string `yaml:"allowGroupIds"`

	// AllowAllTenants is the EXPLICIT fleet-wide opt-in (debug only). An empty
	// allowlist never means "everyone"; this flag must be set to capture across
	// all tenants (still subject to Enabled + SampleRate).
	AllowAllTenants bool `yaml:"allowAllTenants"`

	// DatabaseURL is the Postgres DSN for the io_log store. Required when
	// Enabled is true; ignored otherwise.
	DatabaseURL string `yaml:"databaseUrl"`

	// MaxBodyBytes caps the buffered response-body copy (default 256 KiB if 0).
	MaxBodyBytes int `yaml:"maxBodyBytes"`
}

// Load reads, defaults, and parses a settings YAML file.
func Load(settingsFile string) (*Settings, error) {
	s := &Settings{
		Debug:              false,
		ListenPort:         8080,
		IdleTimeoutStr:     "10m",
		BillPartialOnAbort: true,
		Emit: EmitSettings{
			StreamName: "phoebe:metering",
			WALPath:    "/var/lib/phoebe/metering-wal.jsonl",
		},
	}

	absFilePath, err := filepath.Abs(settingsFile)
	if err != nil {
		return nil, err
	}
	s.configDir = path.Dir(absFilePath)

	settingsYAML, err := os.ReadFile(absFilePath)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(settingsYAML, s); err != nil {
		return nil, err
	}
	if err := s.parse(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Settings) parse() error {
	var err error

	if s.IdleTimeout, err = time.ParseDuration(s.IdleTimeoutStr); err != nil {
		return fmt.Errorf("invalid idleTimeout: %w", err)
	}

	s.ListenAddr = fmt.Sprintf(":%d", s.ListenPort)

	if err := s.IOLog.parse(); err != nil {
		return err
	}
	return nil
}

// parse validates the I/O-logging settings. It fails closed: an enabled logger
// with no DatabaseURL or an out-of-range sample rate is a misconfiguration we
// reject at startup rather than silently capturing nothing (or everything).
func (i *IOLogSettings) parse() error {
	if !i.Enabled {
		return nil // off: nothing to validate
	}
	if i.DatabaseURL == "" {
		return fmt.Errorf("ioLog.enabled=true requires ioLog.databaseUrl")
	}
	if i.SampleRate < 0 || i.SampleRate > 1 {
		return fmt.Errorf("ioLog.sampleRate %.3f out of range [0,1]", i.SampleRate)
	}
	return nil
}
