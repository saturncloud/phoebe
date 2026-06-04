// Package config loads interceptor settings from a YAML file, applying
// defaults then parsing/validating. The load → defaults → unmarshal → parse
// flow mirrors auth-server's util.Settings.
package config

import (
	"fmt"
	"net/url"
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

	// DefaultUpstream is the fallback upstream base URL used when a model's
	// upstream cannot be resolved from the registry. In the walking-skeleton
	// phase this is the single hardcoded engine/router URL.
	DefaultUpstream string `yaml:"defaultUpstream"`

	// IdleTimeout bounds how long an idle streaming connection may stay open.
	// Token streams can idle between chunks, so this is intentionally long.
	IdleTimeoutStr string `yaml:"idleTimeout"`

	// BillPartialOnAbort decides whether a client-aborted request still emits
	// a metering event for the partial token count. Explicit policy, not a
	// silent default.
	BillPartialOnAbort bool `yaml:"billPartialOnAbort"`

	// Registry configures model→upstream dispatch (M4). These are plain
	// fields here; main.go translates them into a registry.Resolver so config
	// stays free of a dependency on the registry package.
	Registry RegistrySettings `yaml:"registry"`

	// Emit configures the durable metering emitter (M2). main.go translates
	// these into an emit.Config for the same reason.
	Emit EmitSettings `yaml:"emit"`

	// --- Parsed settings (populated by parse) ---

	ListenAddr  string        `yaml:"-"`
	IdleTimeout time.Duration `yaml:"-"`
	Default     *url.URL      `yaml:"-"`

	// configDir is the directory the settings file was loaded from; relative
	// paths in the YAML are resolved against it.
	configDir string
}

// RegistrySettings is the YAML shape for model dispatch. Mirrors
// registry.Config without importing it (avoids a config→registry dependency).
type RegistrySettings struct {
	// Strategy: "static" (default), "convention", "cached", or "chain".
	Strategy string `yaml:"strategy"`

	// ConventionTemplate is the URL template with {id} substituted, e.g.
	// "http://model-{id}.inference.svc.cluster.local:8000".
	ConventionTemplate string `yaml:"conventionTemplate"`

	// CacheSize, and the positive/negative TTLs for the cached resolver.
	CacheSize        int    `yaml:"cacheSize"`
	CachePositiveTTL string `yaml:"cachePositiveTTL"`
	CacheNegativeTTL string `yaml:"cacheNegativeTTL"`

	// Parsed durations.
	PositiveTTL time.Duration `yaml:"-"`
	NegativeTTL time.Duration `yaml:"-"`
}

// EmitSettings is the YAML shape for the durable emitter. Mirrors emit.Config
// without importing it.
type EmitSettings struct {
	// ValkeyAddr is the Valkey/Redis address. Empty disables Valkey (WAL-only).
	ValkeyAddr string `yaml:"valkeyAddr"`
	StreamName string `yaml:"streamName"`
	WALPath    string `yaml:"walPath"`
}

// Load reads, defaults, and parses a settings YAML file.
func Load(settingsFile string) (*Settings, error) {
	s := &Settings{
		Debug:              false,
		ListenPort:         8080,
		DefaultUpstream:    "http://localhost:8000",
		IdleTimeoutStr:     "10m",
		BillPartialOnAbort: true,
		Registry: RegistrySettings{
			Strategy:         "static",
			CacheSize:        4096,
			CachePositiveTTL: "5m",
			CacheNegativeTTL: "10s",
		},
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

	if s.DefaultUpstream == "" {
		return fmt.Errorf("missing defaultUpstream")
	}
	if s.Default, err = url.Parse(s.DefaultUpstream); err != nil {
		return fmt.Errorf("invalid defaultUpstream: %w", err)
	}

	s.ListenAddr = fmt.Sprintf(":%d", s.ListenPort)

	if err := s.Registry.parse(); err != nil {
		return err
	}
	return nil
}

func (r *RegistrySettings) parse() error {
	var err error
	if r.CachePositiveTTL != "" {
		if r.PositiveTTL, err = time.ParseDuration(r.CachePositiveTTL); err != nil {
			return fmt.Errorf("invalid registry.cachePositiveTTL: %w", err)
		}
	}
	if r.CacheNegativeTTL != "" {
		if r.NegativeTTL, err = time.ParseDuration(r.CacheNegativeTTL); err != nil {
			return fmt.Errorf("invalid registry.cacheNegativeTTL: %w", err)
		}
	}
	switch r.Strategy {
	case "", "static", "convention", "cached", "chain":
		// ok
	default:
		return fmt.Errorf("invalid registry.strategy %q (want static|convention|cached|chain)", r.Strategy)
	}
	if (r.Strategy == "convention" || r.Strategy == "chain") && r.ConventionTemplate == "" {
		return fmt.Errorf("registry.strategy %q requires conventionTemplate", r.Strategy)
	}
	return nil
}
