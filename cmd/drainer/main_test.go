package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/drain"
)

func writeSettings(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "drainer.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// The schema wait is always bounded: unset or "0" keeps the default, a
// negative value is rejected, and a positive value overrides.
func TestLoadConfig_SchemaWaitTimeoutCannotBeDisabled(t *testing.T) {
	def := drain.DefaultConfig().SchemaWaitTimeout
	if def <= 0 {
		t.Fatalf("default SchemaWaitTimeout = %s, want a positive bound", def)
	}
	for body, want := range map[string]time.Duration{
		"":                       def,
		"schemaWaitTimeout: 0":   def,
		"schemaWaitTimeout: 0s":  def,
		"schemaWaitTimeout: 30m": 30 * time.Minute,
	} {
		cfg, _, err := loadConfig(writeSettings(t, body))
		if err != nil {
			t.Fatalf("%q: %v", body, err)
		}
		if cfg.SchemaWaitTimeout != want {
			t.Errorf("%q: SchemaWaitTimeout = %s, want %s", body, cfg.SchemaWaitTimeout, want)
		}
	}
	if _, _, err := loadConfig(writeSettings(t, "schemaWaitTimeout: -1m")); err == nil {
		t.Error("a negative schemaWaitTimeout must be rejected")
	}
}
