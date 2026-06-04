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

	// --- Parsed settings (populated by parse) ---

	ListenAddr  string        `yaml:"-"`
	IdleTimeout time.Duration `yaml:"-"`
	Default     *url.URL      `yaml:"-"`

	// configDir is the directory the settings file was loaded from; relative
	// paths in the YAML are resolved against it.
	configDir string
}

// Load reads, defaults, and parses a settings YAML file.
func Load(settingsFile string) (*Settings, error) {
	s := &Settings{
		Debug:              false,
		ListenPort:         8080,
		DefaultUpstream:    "http://localhost:8000",
		IdleTimeoutStr:     "10m",
		BillPartialOnAbort: true,
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
	return nil
}
