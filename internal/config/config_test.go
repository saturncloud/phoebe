package config

import (
	"os"
	"path/filepath"
	"testing"
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
