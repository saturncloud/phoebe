package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTemp(t *testing.T, yaml string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "settings.yaml")
	if err := os.WriteFile(p, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadDefaults(t *testing.T) {
	s, err := Load(writeTemp(t, "debug: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !s.Debug {
		t.Fatal("debug not loaded")
	}
	if s.ListenAddr != ":8080" {
		t.Fatalf("ListenAddr = %q, want :8080", s.ListenAddr)
	}
	if !s.BillPartialOnAbort {
		t.Fatal("BillPartialOnAbort should default true")
	}
}

// TestLoadIOLogOffByDefault verifies M5 I/O logging is OFF unless configured:
// no ioLog block means Enabled stays false and parse imposes no requirements.
func TestLoadIOLogOffByDefault(t *testing.T) {
	s, err := Load(writeTemp(t, "debug: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.IOLog.Enabled {
		t.Fatal("ioLog must be disabled by default")
	}
	if s.IOLog.SampleRate != 0 {
		t.Fatalf("default sampleRate = %v, want 0", s.IOLog.SampleRate)
	}
}

// TestLoadIOLogEnabledRequiresDatabaseURL verifies the fail-closed validation:
// enabling logging without a DatabaseURL is a startup error, not silent.
func TestLoadIOLogEnabledRequiresDatabaseURL(t *testing.T) {
	_, err := Load(writeTemp(t, "ioLog:\n  enabled: true\n  sampleRate: 0.1\n"))
	if err == nil {
		t.Fatal("expected error: ioLog.enabled without databaseUrl")
	}
}

// TestLoadIOLogRejectsOutOfRangeRate verifies sampleRate is bounded to [0,1].
func TestLoadIOLogRejectsOutOfRangeRate(t *testing.T) {
	_, err := Load(writeTemp(t, `
ioLog:
  enabled: true
  databaseUrl: "postgres://x/y"
  sampleRate: 1.5
`))
	if err == nil {
		t.Fatal("expected error: sampleRate out of [0,1]")
	}
}

// TestLoadIOLogParsesEnabled verifies a full enabled config round-trips.
func TestLoadIOLogParsesEnabled(t *testing.T) {
	s, err := Load(writeTemp(t, `
ioLog:
  enabled: true
  sampleRate: 0.25
  databaseUrl: "postgres://u:p@h:5432/db"
  allowAuthIds: ["auth-a", "auth-b"]
  allowGroupIds: ["grp-1"]
  maxBodyBytes: 4096
`))
	if err != nil {
		t.Fatal(err)
	}
	if !s.IOLog.Enabled || s.IOLog.SampleRate != 0.25 {
		t.Fatalf("ioLog settings wrong: %+v", s.IOLog)
	}
	if s.IOLog.DatabaseURL != "postgres://u:p@h:5432/db" {
		t.Fatalf("databaseUrl = %q", s.IOLog.DatabaseURL)
	}
	if len(s.IOLog.AllowAuthIDs) != 2 || len(s.IOLog.AllowGroupIDs) != 1 {
		t.Fatalf("allowlists wrong: %+v", s.IOLog)
	}
	if s.IOLog.MaxBodyBytes != 4096 {
		t.Fatalf("maxBodyBytes = %d, want 4096", s.IOLog.MaxBodyBytes)
	}
}

// TestLoadGatewayOffByDefault: no gateway block means disabled — and parse
// imposes no requirements (a legacy config file keeps loading unchanged).
func TestLoadGatewayOffByDefault(t *testing.T) {
	s, err := Load(writeTemp(t, "debug: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Gateway.Enabled {
		t.Fatal("gateway must be disabled by default")
	}
}

// TestLoadGatewayEnabledRequiresNamespace: the fail-closed validation —
// enabling the gateway without the shared-graph namespace is a startup error
// (phoebe never guesses a forward target), not a silent misroute.
func TestLoadGatewayEnabledRequiresNamespace(t *testing.T) {
	_, err := Load(writeTemp(t, "gateway:\n  enabled: true\n"))
	if err == nil {
		t.Fatal("expected error: gateway.enabled without namespace")
	}
}

// TestLoadGatewayPortDefaults8000: an enabled gateway without a port takes
// 8000 (vLLM's serve port); an explicit port is honored; out-of-range is
// rejected.
func TestLoadGatewayPortDefaults8000(t *testing.T) {
	s, err := Load(writeTemp(t, "gateway:\n  enabled: true\n  namespace: \"tf-shared\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Gateway.Port != 8000 {
		t.Fatalf("port = %d, want default 8000", s.Gateway.Port)
	}

	s, err = Load(writeTemp(t, "gateway:\n  enabled: true\n  namespace: \"tf-shared\"\n  port: 9000\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Gateway.Port != 9000 {
		t.Fatalf("port = %d, want 9000", s.Gateway.Port)
	}

	if _, err := Load(writeTemp(t, "gateway:\n  enabled: true\n  namespace: \"tf-shared\"\n  port: 70000\n")); err == nil {
		t.Fatal("expected error: gateway.port out of range")
	}
}

// TestLoadGatewayParsesEnabled: a full gateway block round-trips.
func TestLoadGatewayParsesEnabled(t *testing.T) {
	s, err := Load(writeTemp(t, `
gateway:
  enabled: true
  namespace: "tf-shared"
  port: 8000
  databaseUrl: "postgres://u:p@h:5432/atlas"
`))
	if err != nil {
		t.Fatal(err)
	}
	if !s.Gateway.Enabled || s.Gateway.Namespace != "tf-shared" || s.Gateway.DatabaseURL != "postgres://u:p@h:5432/atlas" {
		t.Fatalf("gateway settings wrong: %+v", s.Gateway)
	}
}

// TestLoadGatewayRegistryIsDefault: an enabled gateway with no resolver
// selection uses the registry resolver (legacy flag false) and needs NO
// databaseUrl — the registry watches ConfigMaps, not Postgres.
func TestLoadGatewayRegistryIsDefault(t *testing.T) {
	s, err := Load(writeTemp(t, "gateway:\n  enabled: true\n  namespace: \"tf-shared\"\n"))
	if err != nil {
		t.Fatalf("registry-mode gateway without databaseUrl must load: %v", err)
	}
	if s.Gateway.LegacyPostgresResolver {
		t.Fatal("legacyPostgresResolver must default false (registry is the default)")
	}
	if s.Gateway.DatabaseURL != "" {
		t.Fatalf("databaseUrl = %q, want empty (not required in registry mode)", s.Gateway.DatabaseURL)
	}
}

// TestLoadGatewayLegacyResolverFlag: the deprecated PG path stays selectable
// for one release behind the explicit flag (rollback seam); databaseUrl is
// still not a PARSE requirement (main enforces it after the DATABASE_URL env
// fallback, as before).
func TestLoadGatewayLegacyResolverFlag(t *testing.T) {
	s, err := Load(writeTemp(t, `
gateway:
  enabled: true
  namespace: "tf-shared"
  legacyPostgresResolver: true
  databaseUrl: "postgres://u:p@h:5432/atlas"
`))
	if err != nil {
		t.Fatal(err)
	}
	if !s.Gateway.LegacyPostgresResolver || s.Gateway.DatabaseURL == "" {
		t.Fatalf("legacy settings wrong: %+v", s.Gateway)
	}
}

// TestLoadWakeOffByDefault: no wake block means disabled, no requirements.
func TestLoadWakeOffByDefault(t *testing.T) {
	s, err := Load(writeTemp(t, "debug: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Wake.Enabled {
		t.Fatal("wake must be disabled by default")
	}
}

// TestLoadWakeEnabledRequiresGatewayNamespace: the waker patches DGDSAs in the
// gateway namespace; enabling wake without one is a startup error (fail closed
// — never actuate in a guessed namespace). gateway.enabled itself is NOT
// required (wake also serves header-routed shared subdomains).
func TestLoadWakeEnabledRequiresGatewayNamespace(t *testing.T) {
	if _, err := Load(writeTemp(t, "wake:\n  enabled: true\n")); err == nil {
		t.Fatal("expected error: wake.enabled without gateway.namespace")
	}
	s, err := Load(writeTemp(t, "wake:\n  enabled: true\ngateway:\n  namespace: \"tf-shared\"\n"))
	if err != nil {
		t.Fatalf("wake with gateway.namespace (gateway disabled) must load: %v", err)
	}
	if !s.Wake.Enabled || s.Gateway.Enabled {
		t.Fatalf("settings wrong: wake=%+v gateway=%+v", s.Wake, s.Gateway)
	}
}

// TestLoadWakeTimeout: wake.timeout parses to a duration; empty means "proxy
// default" (0); garbage and non-positive values are startup errors.
func TestLoadWakeTimeout(t *testing.T) {
	base := "gateway:\n  namespace: \"tf-shared\"\nwake:\n  enabled: true\n"

	s, err := Load(writeTemp(t, base))
	if err != nil {
		t.Fatal(err)
	}
	if s.Wake.Timeout != 0 {
		t.Fatalf("unset wake.timeout = %v, want 0 (proxy default)", s.Wake.Timeout)
	}

	s, err = Load(writeTemp(t, base+"  timeout: \"7m\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Wake.Timeout != 7*time.Minute {
		t.Fatalf("wake.timeout = %v, want 7m", s.Wake.Timeout)
	}

	if _, err := Load(writeTemp(t, base+"  timeout: \"soon\"\n")); err == nil {
		t.Fatal("expected error: unparseable wake.timeout")
	}
	if _, err := Load(writeTemp(t, base+"  timeout: \"-30s\"\n")); err == nil {
		t.Fatal("expected error: non-positive wake.timeout")
	}
}

func TestLoadEmitSettings(t *testing.T) {
	s, err := Load(writeTemp(t, `
emit:
  valkeyAddr: "valkey:6379"
  streamName: "custom:stream"
`))
	if err != nil {
		t.Fatal(err)
	}
	if s.Emit.ValkeyAddr != "valkey:6379" || s.Emit.StreamName != "custom:stream" {
		t.Fatalf("emit settings wrong: %+v", s.Emit)
	}
}
