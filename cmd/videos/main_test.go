package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "videos.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

var fullEnv = env(map[string]string{"DATABASE_URL": "postgres://x", "PHOEBE_VIDEOS_TOKEN": "t"})

func TestLoadAppliesTheDesignDefaults(t *testing.T) {
	c, err := load(writeConfig(t, "valkeyAddr: v:6379\n"), fullEnv)
	if err != nil {
		t.Fatal(err)
	}
	if c.retention != 24*time.Hour || c.queueTimeout != time.Hour || c.runTimeout != 30*time.Minute ||
		c.s.Store.Prefix != "tf-videos" || c.s.ListenPort != 8090 {
		t.Fatalf("defaults: %+v", c)
	}
}

func TestLoadRefusesBadConfig(t *testing.T) {
	for _, tc := range []struct {
		yaml   string
		env    func(string) string
		reason string
	}{
		{"valkeyAddr: v\nretentionHours: 169\n", fullEnv, "retention above 7 days"},
		{"valkeyAddr: v\nretentionHours: -1\n", fullEnv, "negative retention"},
		{"valkeyAddr: v\nqueueTimeout: soon\n", fullEnv, "bad duration"},
		{"valkeyAddr: v\nunknownKey: 1\n", fullEnv, "unknown key"},
		{"retentionHours: 24\n", fullEnv, "no valkey"},
		{"valkeyAddr: v\n", env(map[string]string{"DATABASE_URL": "x"}), "no token"},
		{"valkeyAddr: v\n", env(map[string]string{"PHOEBE_VIDEOS_TOKEN": "t"}), "no database"},
	} {
		if _, err := load(writeConfig(t, tc.yaml), tc.env); err == nil {
			t.Errorf("accepted %s", tc.reason)
		}
	}
}
