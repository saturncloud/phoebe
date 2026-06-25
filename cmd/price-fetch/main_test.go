package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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

// inodeOf returns the filesystem inode number of the file at path. A rewrite via the
// temp-file+rename install path lands a NEW inode, so an unchanged inode proves the
// file was not rewritten — robust where mtime is too coarse.
func inodeOf(t *testing.T, path string) uint64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat %s: Sys() is not *syscall.Stat_t", path)
	}
	return st.Ino
}

// TestFetchAndInstall_Idempotent (price-fetch-idempotent): when the served version
// matches the installed sidecar AND the on-disk body already equals the served body,
// the file is NOT rewritten (no churn, no rename a rater could race). With the
// body-content check (Fix 2) a true no-op REQUIRES the on-disk body to already match,
// so we seed the destination with the EXACT served bytes and assert the file's inode
// is unchanged (the install path would rename a fresh inode into place).
func TestFetchAndInstall_Idempotent(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")

	// Seed: the exact served body + a sidecar recording the served version.
	if err := os.WriteFile(dest, []byte(validPriceYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest+versionSuffix, []byte("hash-v1"), 0o644); err != nil {
		t.Fatal(err)
	}
	inoBefore := inodeOf(t, dest)

	srv := priceServer(t, http.StatusOK, "hash-v1", validPriceYAML, nil)
	if err := fetchAndInstall(context.Background(), quietLog(), optsFor(srv, dest), "test-token"); err != nil {
		t.Fatalf("fetchAndInstall: %v", err)
	}

	if inoAfter := inodeOf(t, dest); inoAfter != inoBefore {
		t.Fatalf("file was rewritten on a true match (inode %d -> %d)", inoBefore, inoAfter)
	}
	got, _ := os.ReadFile(dest)
	if string(got) != validPriceYAML {
		t.Fatalf("file content changed on a no-op; got %q", string(got))
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

// TestInstall_BodyFirst_RecoversFromSidecarAheadCrash
// (price-fetch-recover-from-stranded-state): reproduces the post-crash stranded state
// where the sidecar already records the NEW version but the body on disk is still OLD
// (the dangerous {sidecar:V_new, body:B_old}). A version-only idempotency no-op would
// strand the old bytes forever. With the body-content check (Fix 2) the no-op does NOT
// fire — fetchAndInstall re-installs the NEW body. This is the invariant that makes
// recovery REAL; body-first ordering (Fix 1) only stops CREATING this state going
// forward.
func TestInstall_BodyFirst_RecoversFromSidecarAheadCrash(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")

	// Stranded state: OLD body, sidecar already AHEAD at hash-NEW.
	if err := os.WriteFile(dest, []byte("OLD-STALE-BYTES\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest+versionSuffix, []byte("hash-NEW"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Server serves version hash-NEW with the valid NEW body.
	srv := priceServer(t, http.StatusOK, "hash-NEW", validPriceYAML, nil)
	if err := fetchAndInstall(context.Background(), quietLog(), optsFor(srv, dest), "test-token"); err != nil {
		t.Fatalf("fetchAndInstall: %v", err)
	}

	got, _ := os.ReadFile(dest)
	if string(got) != validPriceYAML {
		t.Fatalf("stranded OLD bytes were not recovered; on-disk body = %q, want the NEW body", string(got))
	}
}

// TestFetchAndInstall_OversizedBodyRejected (price-fetch-reject-oversized): a 200 body
// larger than maxPriceBodyBytes is REJECTED, not silently truncated to a parseable
// prefix. Nothing is installed; the error mentions the cap.
func TestFetchAndInstall_OversizedBodyRejected(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")

	// A valid-looking prefix padded with a long YAML comment line to overrun the cap by
	// one byte. (A truncated prefix of this WOULD parse — that's exactly the danger.)
	prefix := "version: 1\nbase_models:\n"
	pad := maxPriceBodyBytes + 1 - len(prefix)
	body := prefix + "#" + strings.Repeat("x", pad-1)
	if len(body) != maxPriceBodyBytes+1 {
		t.Fatalf("test body len = %d, want %d", len(body), maxPriceBodyBytes+1)
	}

	srv := priceServer(t, http.StatusOK, "hash-big", body, nil)
	err := fetchAndInstall(context.Background(), quietLog(), optsFor(srv, dest), "test-token")
	if err == nil {
		t.Fatalf("expected an error on an oversized body, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error = %v, want it to mention the size cap (\"exceeds\")", err)
	}
	if _, statErr := os.Stat(dest); statErr == nil {
		t.Fatalf("a file was installed despite an oversized body")
	}
}

// TestFetchAndInstall_BoundaryBodyAccepted (price-fetch-accept-at-cap): a body exactly
// maxPriceBodyBytes long is at the cap, not over it, so it is read in full. (We use a
// padding YAML comment so the body still parses; it must install.)
func TestFetchAndInstall_BoundaryBodyAccepted(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")

	pad := maxPriceBodyBytes - len(validPriceYAML) - len("\n#")
	body := validPriceYAML + "\n#" + strings.Repeat("x", pad)
	if len(body) != maxPriceBodyBytes {
		t.Fatalf("test body len = %d, want %d", len(body), maxPriceBodyBytes)
	}

	srv := priceServer(t, http.StatusOK, "hash-cap", body, nil)
	if err := fetchAndInstall(context.Background(), quietLog(), optsFor(srv, dest), "test-token"); err != nil {
		t.Fatalf("a body exactly at the cap should install, got: %v", err)
	}
	got, _ := os.ReadFile(dest)
	if string(got) != body {
		t.Fatalf("installed bytes != served bytes at the boundary")
	}
}

// TestInstall_FileMode0644 (price-fetch-world-readable): the installed price file and
// its sidecar are 0644 (world-readable), not CreateTemp's 0600, so the rater running
// under a DIFFERENT uid can read them. Drives the real install path.
func TestInstall_FileMode0644(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")
	srv := priceServer(t, http.StatusOK, "hash-v1", validPriceYAML, nil)

	if err := fetchAndInstall(context.Background(), quietLog(), optsFor(srv, dest), "test-token"); err != nil {
		t.Fatalf("fetchAndInstall: %v", err)
	}

	for _, p := range []string{dest, dest + versionSuffix} {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		if perm := fi.Mode().Perm(); perm != 0o644 {
			t.Fatalf("%s mode = %o, want 0644", p, perm)
		}
	}
}

// TestIdempotent_TrimsSidecarWhitespace (price-fetch-trim-sidecar): an
// externally-seeded sidecar with a trailing newline (e.g. `echo V > file`) still
// compares equal to the served version, so a true match (same body too) is a no-op.
func TestIdempotent_TrimsSidecarWhitespace(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")

	if err := os.WriteFile(dest, []byte(validPriceYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	// Sidecar seeded WITH a trailing newline (the externally-seeded shape).
	if err := os.WriteFile(dest+versionSuffix, []byte("hash-v1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	inoBefore := inodeOf(t, dest)

	srv := priceServer(t, http.StatusOK, "hash-v1", validPriceYAML, nil)
	if err := fetchAndInstall(context.Background(), quietLog(), optsFor(srv, dest), "test-token"); err != nil {
		t.Fatalf("fetchAndInstall: %v", err)
	}

	if inoAfter := inodeOf(t, dest); inoAfter != inoBefore {
		t.Fatalf("a whitespace-only sidecar difference forced a rewrite (inode %d -> %d)", inoBefore, inoAfter)
	}
}

// TestFetchAndInstall_MissingVersionFailsClosed (price-fetch-missing-version) pins
// the missing-version contract: the manager ALWAYS sets X-Saturn-Price-Version on a
// 200 (it is a deterministic content hash, set unconditionally), so a 200 with no
// version header is a contract violation, not a best-effort omission. The fetcher
// refuses it and leaves any prior good file + sidecar untouched (stale-but-priced) —
// never installs a price file it could not attribute in a billing reconcile.
func TestFetchAndInstall_MissingVersionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")

	// Seed a prior good install: a body + a sidecar naming its version.
	if err := os.WriteFile(dest, []byte(validPriceYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dest+versionSuffix, []byte("hash-OLD"), 0o644); err != nil {
		t.Fatal(err)
	}

	// A genuinely DIFFERENT valid body served with NO version header (priceServer
	// omits the header when version == "").
	const newBody = `
version: 1
base_models:
  "meta-llama/Llama-3.1-8B-Instruct":
    prompt:     "0.000000200"
    cached:     "0.000000050"
    completion: "0.000000999"
fine_tune_premium:
  policy: "identity"
`
	srv := priceServer(t, http.StatusOK, "", newBody, nil)
	err := fetchAndInstall(context.Background(), quietLog(), optsFor(srv, dest), "test-token")
	if err == nil {
		t.Fatalf("expected an error on a 200 with no version header, got nil")
	}

	// The prior good body and sidecar must be UNTOUCHED — never replaced by an
	// unversioned body.
	got, _ := os.ReadFile(dest)
	if string(got) != validPriceYAML {
		t.Fatalf("prior good body was replaced by an unversioned fetch")
	}
	if ver, ok := readInstalledVersion(dest); !ok || ver != "hash-OLD" {
		t.Fatalf("prior sidecar disturbed: ver=%q ok=%v, want hash-OLD", ver, ok)
	}
}

// TestRun_MissingVersionExits2 (price-fetch-missing-version-exit-2): through the full
// run() path, a 200 with no version header exits exitFetchBad(2) — the
// prices-not-refreshing signal — not fatal(1) and not success(0).
func TestRun_MissingVersionExits2(t *testing.T) {
	t.Setenv(tokenEnv, "test-token")
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")
	if err := os.WriteFile(dest, []byte(validPriceYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	srv := priceServer(t, http.StatusOK, "", validPriceYAML, nil)
	code := runWith([]string{
		"-manager-url", srv.URL,
		"-out", dest,
		"-f", filepath.Join(dir, "does-not-exist.yaml"),
	})
	if code != exitFetchBad {
		t.Fatalf("exit code = %d, want exitFetchBad(%d)", code, exitFetchBad)
	}
}

// TestInstallAtomically_EmptyVersionAsserts (price-fetch-install-empty-version-asserts):
// installAtomically fails loud on an empty version. This precondition is unreachable from
// production (fetchPrices refuses a 200 with no version header), but the assertion keeps a
// future caller from silently installing an unversioned, unattributable price file.
func TestInstallAtomically_EmptyVersionAsserts(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "prices.yaml")
	if err := installAtomically(dest, []byte("x"), ""); err == nil {
		t.Fatalf("expected installAtomically to error on an empty version")
	}
	// Nothing should have been written.
	if _, err := os.Stat(dest); err == nil {
		t.Fatalf("a price file was installed despite the empty-version assertion")
	}
}
