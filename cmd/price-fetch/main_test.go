package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

// validPriceYAML is a minimal price file the rater's loader (rating.ParsePriceBook)
// accepts: one base model with exact-decimal string rates and an identity premium.
// Mirrors the fixtures in internal/rating/pricebook_test.go so a drift in the loader
// surfaces here too.
const validPriceYAML = `
version: 1
base_models:
  "meta-llama/Llama-3.1-8B-Instruct":
    prompt:     "0.000000200"
    cached:     "0.000000050"
    completion: "0.000000600"
fine_tune_premium:
  policy: "identity"
`

// invalidPriceYAML parses as YAML but the rater REJECTS it: empty base_models would
// $0-rate everything. The fetcher must never install this.
const invalidPriceYAML = `
version: 1
base_models: {}
fine_tune_premium:
  policy: "identity"
`

func quietLog() *logging.Logger { return logging.New(logging.ERROR) }

// priceServer returns a test server that serves body with the given status and
// version header, and records how many times it was hit.
func priceServer(t *testing.T, status int, version, body string, hits *int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits != nil {
			*hits++
		}
		if r.URL.Path != tokenPricesPath {
			t.Errorf("path = %q, want %q", r.URL.Path, tokenPricesPath)
		}
		if got := r.Header.Get("Authorization"); got != "token test-token" {
			t.Errorf("Authorization = %q, want %q", got, "token test-token")
		}
		if version != "" {
			w.Header().Set(priceVersionHeader, version)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func optsFor(srv *httptest.Server, dest string) fetchOptions {
	return fetchOptions{
		managerURL:     srv.URL,
		priceFile:      dest,
		requestTimeout: 5 * time.Second,
	}
}

// TestFetchAndInstall_Success (price-fetch-success): a valid file is fetched,
// validated, written to the destination, and its version recorded in the sidecar.
func TestFetchAndInstall_Success(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")
	srv := priceServer(t, http.StatusOK, "hash-v1", validPriceYAML, nil)

	if err := fetchAndInstall(context.Background(), quietLog(), optsFor(srv, dest), "test-token"); err != nil {
		t.Fatalf("fetchAndInstall: %v", err)
	}

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read installed file: %v", err)
	}
	if string(got) != validPriceYAML {
		t.Fatalf("installed bytes != served bytes")
	}
	ver, ok := readInstalledVersion(dest)
	if !ok || ver != "hash-v1" {
		t.Fatalf("sidecar version = %q (ok=%v), want %q", ver, ok, "hash-v1")
	}
}

// TestFetchAndInstall_Idempotent (price-fetch-idempotent): when the served version
// matches the installed sidecar, the file is NOT rewritten (no churn, no rename a
// rater could race). We detect "not rewritten" by pre-seeding the destination with
// DIFFERENT bytes under the same version and asserting they survive.
func TestFetchAndInstall_Idempotent(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")

	// Seed: a marker body + a sidecar recording the version the server will serve.
	const marker = "PRE-EXISTING-BYTES\n"
	if err := os.WriteFile(dest, []byte(marker), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest+versionSuffix, []byte("hash-v1"), 0o644); err != nil {
		t.Fatal(err)
	}

	srv := priceServer(t, http.StatusOK, "hash-v1", validPriceYAML, nil)
	if err := fetchAndInstall(context.Background(), quietLog(), optsFor(srv, dest), "test-token"); err != nil {
		t.Fatalf("fetchAndInstall: %v", err)
	}

	got, _ := os.ReadFile(dest)
	if string(got) != marker {
		t.Fatalf("file was rewritten on a matching version; got %q want the seeded marker", string(got))
	}
}

// TestFetchAndInstall_VersionChanged (price-fetch-version-changed): a DIFFERENT
// served version replaces the installed file and sidecar.
func TestFetchAndInstall_VersionChanged(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")
	if err := os.WriteFile(dest, []byte("OLD\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest+versionSuffix, []byte("hash-OLD"), 0o644); err != nil {
		t.Fatal(err)
	}

	srv := priceServer(t, http.StatusOK, "hash-NEW", validPriceYAML, nil)
	if err := fetchAndInstall(context.Background(), quietLog(), optsFor(srv, dest), "test-token"); err != nil {
		t.Fatalf("fetchAndInstall: %v", err)
	}
	got, _ := os.ReadFile(dest)
	if string(got) != validPriceYAML {
		t.Fatalf("file not replaced on version change")
	}
	if ver, _ := readInstalledVersion(dest); ver != "hash-NEW" {
		t.Fatalf("sidecar = %q, want hash-NEW", ver)
	}
}

// TestFetchAndInstall_InvalidBodyNeverInstalled (price-fetch-reject-invalid): a body
// the rater's loader rejects (empty base_models) is NEVER written, and a prior good
// file is left intact. This is the core fail-closed guarantee.
func TestFetchAndInstall_InvalidBodyNeverInstalled(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")
	if err := os.WriteFile(dest, []byte(validPriceYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest+versionSuffix, []byte("hash-good"), 0o644); err != nil {
		t.Fatal(err)
	}

	srv := priceServer(t, http.StatusOK, "hash-bad", invalidPriceYAML, nil)
	err := fetchAndInstall(context.Background(), quietLog(), optsFor(srv, dest), "test-token")
	if err == nil {
		t.Fatalf("expected an error installing an invalid price file, got nil")
	}

	// The prior good file and version must be untouched.
	got, _ := os.ReadFile(dest)
	if string(got) != validPriceYAML {
		t.Fatalf("prior good file was clobbered by an invalid fetch")
	}
	if ver, _ := readInstalledVersion(dest); ver != "hash-good" {
		t.Fatalf("prior sidecar clobbered: %q", ver)
	}
}

// TestFetchAndInstall_Non200FailsClosed (price-fetch-503-fails-closed): the
// endpoint's own 503 (no plan / incomplete card) is an error and installs nothing.
func TestFetchAndInstall_Non200FailsClosed(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")
	srv := priceServer(t, http.StatusServiceUnavailable, "", "plan not priced\n", nil)

	err := fetchAndInstall(context.Background(), quietLog(), optsFor(srv, dest), "test-token")
	if err == nil {
		t.Fatalf("expected an error on 503, got nil")
	}
	if _, statErr := os.Stat(dest); statErr == nil {
		t.Fatalf("a file was installed despite a 503")
	}
}

// TestFetchAndInstall_UnreachableLeavesExisting (price-fetch-unreachable-stale): when
// the manager is unreachable, a prior good file is left in place (stale-but-priced).
func TestFetchAndInstall_UnreachableLeavesExisting(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")
	if err := os.WriteFile(dest, []byte(validPriceYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	// Point at a closed server.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // now unreachable

	opts := fetchOptions{managerURL: url, priceFile: dest, requestTimeout: time.Second}
	err := fetchAndInstall(context.Background(), quietLog(), opts, "test-token")
	if err == nil {
		t.Fatalf("expected an error against an unreachable manager, got nil")
	}
	got, _ := os.ReadFile(dest)
	if string(got) != validPriceYAML {
		t.Fatalf("existing file was disturbed by a failed fetch")
	}
}

// TestRun_NoTokenFatal (price-fetch-no-token): a missing auth token is fatal (exit 1),
// distinct from a fetch failure (exit 2) — nothing to call the manager with.
func TestRun_NoTokenFatal(t *testing.T) {
	t.Setenv(tokenEnv, "")
	dir := t.TempDir()
	// Provide url+dest via flags so only the token is missing.
	srv := priceServer(t, http.StatusOK, "v", validPriceYAML, nil)
	code := runWith([]string{
		"-manager-url", srv.URL,
		"-out", filepath.Join(dir, "prices.yaml"),
		"-f", filepath.Join(dir, "does-not-exist.yaml"),
	})
	if code != exitFatal {
		t.Fatalf("exit code = %d, want exitFatal(%d)", code, exitFatal)
	}
}

// TestRun_FetchBadWithExistingExits2 (price-fetch-exit-2): a fetch failure when a
// local file exists exits exitFetchBad(2), not fatal(1).
func TestRun_FetchBadWithExistingExits2(t *testing.T) {
	t.Setenv(tokenEnv, "test-token")
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")
	if err := os.WriteFile(dest, []byte(validPriceYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	srv := priceServer(t, http.StatusServiceUnavailable, "", "no plan\n", nil)
	code := runWith([]string{
		"-manager-url", srv.URL,
		"-out", dest,
		"-f", filepath.Join(dir, "does-not-exist.yaml"),
	})
	if code != exitFetchBad {
		t.Fatalf("exit code = %d, want exitFetchBad(%d)", code, exitFetchBad)
	}
}

// TestRun_Success (price-fetch-run-success): the full run() path installs and exits 0.
func TestRun_Success(t *testing.T) {
	t.Setenv(tokenEnv, "test-token")
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")
	srv := priceServer(t, http.StatusOK, "hash-v1", validPriceYAML, nil)
	code := runWith([]string{
		"-manager-url", srv.URL,
		"-out", dest,
		"-f", filepath.Join(dir, "does-not-exist.yaml"),
	})
	if code != exitOK {
		t.Fatalf("exit code = %d, want exitOK(%d)", code, exitOK)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Fatalf("price file not installed: %v", err)
	}
}
