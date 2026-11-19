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

var fullEnv = env(map[string]string{"DATABASE_URL": "postgres://x", "PHOEBE_VIDEOS_TOKEN": "t",
	"TF_INTERNAL_VIDEO_STORE_TOKEN": "s"})

const atlas = "atlasURL: http://atlas:8888\n"

func TestLoadAppliesTheDesignDefaults(t *testing.T) {
	c, err := load(writeConfig(t, "valkeyAddr: v:6379\n"+atlas), fullEnv)
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
		{atlas + "valkeyAddr: v\nretentionHours: 169\n", fullEnv, "retention above 7 days"},
		{atlas + "valkeyAddr: v\nretentionHours: -1\n", fullEnv, "negative retention"},
		{atlas + "valkeyAddr: v\nqueueTimeout: soon\n", fullEnv, "bad duration"},
		{atlas + "valkeyAddr: v\nunknownKey: 1\n", fullEnv, "unknown key"},
		{atlas + "retentionHours: 24\n", fullEnv, "no valkey"},
		{atlas + "valkeyAddr: v\n", env(map[string]string{"DATABASE_URL": "x", "TF_INTERNAL_VIDEO_STORE_TOKEN": "s"}), "no token"},
		{atlas + "valkeyAddr: v\n", env(map[string]string{"PHOEBE_VIDEOS_TOKEN": "t", "TF_INTERNAL_VIDEO_STORE_TOKEN": "s"}), "no database"},
		{"valkeyAddr: v\n", fullEnv, "no Atlas URL"},
		{atlas + "valkeyAddr: v\n", env(map[string]string{"DATABASE_URL": "x", "PHOEBE_VIDEOS_TOKEN": "t"}), "no video store token"},
		// The store credential and location settings are gone: an old
		// config that still carries them fails rather than being ignored.
		{atlas + "valkeyAddr: v\nstore:\n  bucket: b\n", fullEnv, "a store bucket"},
		{atlas + "valkeyAddr: v\nstore:\n  endpoint: https://s3\n", fullEnv, "a store endpoint"},
	} {
		if _, err := load(writeConfig(t, tc.yaml), tc.env); err == nil {
			t.Errorf("accepted %s", tc.reason)
		}
	}
}

// Atlas signs uploads for a job until at most 1440 minutes after it
// registered. Timeouts that let a job wait and run twice past that (with 30
// minutes kept to store the video) are refused at start.
func TestLoadRefusesTimeoutsNoUploadWindowCovers(t *testing.T) {
	// 60m + 2*675m + 30m = 1440m: the largest that fits.
	if _, err := load(writeConfig(t, atlas+"valkeyAddr: v\nqueueTimeout: 60m\nrunTimeout: 675m\n"), fullEnv); err != nil {
		t.Fatalf("timeouts that fit the window refused: %v", err)
	}
	for _, yaml := range []string{
		"queueTimeout: 61m\nrunTimeout: 675m\n",
		"queueTimeout: 60m\nrunTimeout: 676m\n",
		"queueTimeout: 1400m\n",
	} {
		if _, err := load(writeConfig(t, atlas+"valkeyAddr: v\n"+yaml), fullEnv); err == nil {
			t.Errorf("accepted %q", yaml)
		}
	}
}

// Atlas signs a video upload of at most 5 GiB; a larger maxOutputBytes would
// let a job generate a video no upload can store.
func TestLoadRefusesMaxOutputBytesAboveStoreLimit(t *testing.T) {
	if _, err := load(writeConfig(t, atlas+"valkeyAddr: v\nmaxOutputBytes: 5368709120\n"), fullEnv); err != nil {
		t.Fatalf("5 GiB refused: %v", err)
	}
	if _, err := load(writeConfig(t, atlas+"valkeyAddr: v\nmaxOutputBytes: 5368709121\n"), fullEnv); err == nil {
		t.Fatal("accepted maxOutputBytes above 5 GiB")
	}
}
