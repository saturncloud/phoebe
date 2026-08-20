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
	// dependency (same pattern as Emit).
	IOLog IOLogSettings `yaml:"ioLog"`

	// Gateway configures the TF single-host gateway resolution path (requests
	// marked X-Saturn-Gateway resolve their body model= against Atlas's
	// tf_model). OFF by default — a phoebe without it refuses gateway-marked
	// requests fail-closed (503). main.go translates these into the
	// internal/gateway resolver wiring (same pattern as Emit/IOLog).
	Gateway GatewaySettings `yaml:"gateway"`

	// Wake configures wake-from-zero actuation (the client-go DGDSA waker,
	// internal/waker). OFF by default — without it a cold response passes
	// through to the client exactly as before. main.go builds the waker; an
	// unavailable kubernetes config degrades to wake-off (logged), never a
	// crash.
	Wake WakeSettings `yaml:"wake"`

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

// GatewaySettings is the YAML shape for the TF gateway resolution path.
//
// FAIL CLOSED: Enabled defaults to false, and enabling without a Namespace is
// a startup error — the namespace is half of every composed upstream
// (<graph>-frontend.<namespace>.svc.cluster.local:<port>), and phoebe never
// guesses a forward target. OPERATING ASSUMPTION (documented, load-bearing):
// every serving graph reachable through this phoebe's gateway host lives in
// this ONE namespace (the platform shared-graph namespace); a tf_model row
// whose graph lives elsewhere fails at dial, never misroutes.
type GatewaySettings struct {
	// Enabled turns gateway resolution on. Default false: gateway-marked
	// requests are refused (503) rather than resolved.
	Enabled bool `yaml:"enabled"`

	// Namespace is the k8s namespace the shared serving graphs' frontend
	// Services live in. REQUIRED when Enabled (fail closed, see above).
	Namespace string `yaml:"namespace"`

	// Port is the graphs' OpenAI-compatible serve port. Default 8000 (vLLM's
	// serve port, the same value Atlas's header-injected upstreams carry).
	Port int `yaml:"port"`

	// DatabaseURL is the Atlas Postgres DSN for tf_model lookups — the same
	// database phoebe's drainer/rater already use for billing_event /
	// rated_usage. Empty falls back to the DATABASE_URL env var (Atlas
	// convention, resolved in main); enabled with NEITHER set is a startup
	// error.
	DatabaseURL string `yaml:"databaseUrl"`
}

// WakeSettings is the YAML shape for the wake-from-zero actuator.
//
// The DGDSAs/ScaledObjects the waker patches live in the SAME namespace as
// the gateway's serving graphs, so enabling wake requires gateway.namespace
// (validated in Settings.parse — fail closed rather than actuate in a guessed
// namespace).
type WakeSettings struct {
	// Enabled turns the DGDSA waker on. Default false: cold responses pass
	// through unchanged.
	Enabled bool `yaml:"enabled"`

	// Kubeconfig is a kubeconfig file path for dev/tests. Empty (production)
	// uses in-cluster config.
	Kubeconfig string `yaml:"kubeconfig"`
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
	if err := s.Gateway.parse(); err != nil {
		return err
	}
	// The waker patches DGDSAs in the gateway namespace; without it there is
	// no safe namespace to actuate in (fail closed at startup, not a guessed
	// patch at wake time). gateway.enabled itself is NOT required — wake also
	// serves header-routed shared subdomains.
	if s.Wake.Enabled && s.Gateway.Namespace == "" {
		return fmt.Errorf("wake.enabled=true requires gateway.namespace (the namespace the serving graphs' DGDSAs live in)")
	}
	return nil
}

// parse validates the gateway settings and applies the port default. It fails
// closed: an enabled gateway with no namespace is a misconfiguration rejected
// at startup — serving gateway requests would require guessing where the
// graphs live, which phoebe never does. (DatabaseURL is validated in main,
// after the DATABASE_URL env fallback.)
func (g *GatewaySettings) parse() error {
	if !g.Enabled {
		return nil // off: nothing to validate
	}
	if g.Namespace == "" {
		return fmt.Errorf("gateway.enabled=true requires gateway.namespace (the shared-graph namespace; phoebe never guesses a forward target)")
	}
	if g.Port == 0 {
		g.Port = 8000
	}
	if g.Port < 1 || g.Port > 65535 {
		return fmt.Errorf("gateway.port %d out of range [1,65535]", g.Port)
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
