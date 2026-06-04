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
	if s.Registry.Strategy != "static" {
		t.Fatalf("default strategy = %q, want static", s.Registry.Strategy)
	}
	if !s.BillPartialOnAbort {
		t.Fatal("BillPartialOnAbort should default true")
	}
}

func TestLoadRegistryConventionRequiresTemplate(t *testing.T) {
	_, err := Load(writeTemp(t, "registry:\n  strategy: convention\n"))
	if err == nil {
		t.Fatal("expected error: convention strategy without template")
	}
}

func TestLoadRegistryParsesTTLs(t *testing.T) {
	s, err := Load(writeTemp(t, `
registry:
  strategy: cached
  conventionTemplate: "http://model-{id}.svc:8000"
  cachePositiveTTL: "2m"
  cacheNegativeTTL: "15s"
`))
	if err != nil {
		t.Fatal(err)
	}
	if s.Registry.PositiveTTL.Minutes() != 2 {
		t.Fatalf("PositiveTTL = %v, want 2m", s.Registry.PositiveTTL)
	}
	if s.Registry.NegativeTTL.Seconds() != 15 {
		t.Fatalf("NegativeTTL = %v, want 15s", s.Registry.NegativeTTL)
	}
}

func TestLoadInvalidStrategy(t *testing.T) {
	_, err := Load(writeTemp(t, "registry:\n  strategy: bogus\n"))
	if err == nil {
		t.Fatal("expected error for invalid strategy")
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
