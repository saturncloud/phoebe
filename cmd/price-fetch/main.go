// Command price-fetch is phoebe's price-book SYNC step: it pulls the customer's
// effective token prices from the central pricing service (saturn-aws-manager's
// GET /customer/token-prices) and installs them as the local YAML price file the
// rater (cmd/rater) loads. It is the fetch-to-local half of the seam that
// rating.LoadPriceBook documents: phoebe rates against a LOCAL file; this binary
// keeps that file current from the authoritative central source.
//
// WHY A SEPARATE BINARY (not folded into the rater): the rater is a money-path
// batch job that must be able to run against the LAST-GOOD prices even when the
// manager is unreachable. Decoupling the fetch lets a manager outage delay a price
// UPDATE without ever blocking RATING of already-priced traffic. The two share only
// the file on disk.
//
// AUTHORSHIP vs RATING (the design this rests on): the central pricing service owns
// price authorship AND effective-dating/history. /customer/token-prices serves a
// POINT-IN-TIME effective snapshot for the customer's plan plus an
// X-Saturn-Price-Version content-hash header. phoebe does NOT keep a local price
// history; it fetches the current effective snapshot and rates against it. "Rate an
// old event at the old price" is the manager's effective-window responsibility, not
// phoebe's — so this binary keeps NO effective-dated tables, just the current file.
//
// It is a ONE-SHOT job (run by cron / a k8s CronJob), NOT a daemon: it fetches once
// and exits. Exit codes:
//
//	0  a fresh price file was fetched, validated, and installed (or the served
//	   version already matched what is installed — a no-op, nothing rewritten)
//	1  fatal: bad config / no auth token / unusable destination path
//	2  fetch FAILED (manager unreachable, non-200, malformed/invalid price file, or a
//	   200 missing the X-Saturn-Price-Version header — the manager always sets it, so
//	   its absence is treated as a contract violation, not a best-effort omission)
//	   BUT a previously-good local file is still in place. The rater can keep
//	   rating against it; this code says "prices are STALE, investigate" without
//	   conflating it with "the job is broken" (code 1) — distinct so a CronJob can
//	   alert on "prices not refreshing" separately from "fetcher crashed".
//
// FAIL-CLOSED CONTRACT (the one-way doors):
//   - The fetched bytes are validated by the rater's OWN loader (rating.ParsePriceBook)
//     before they are installed. A file the rater would reject is NEVER written into
//     place — the rater can never be handed a file it chokes on or that would $0-rate.
//   - The install is ATOMIC: write a temp file in the destination dir, fsync the file
//     AND the parent directory, then rename(2) over the destination. The rater loads
//     the file in one read; it can never observe a half-written file. The BODY is
//     written and renamed FIRST, then the version sidecar. If the process dies between
//     the two renames, the worst case is {body:B_new, sidecar:V_old-or-absent}: the
//     next run sees served-version != installed-version (or no sidecar), so the
//     idempotency guard does NOT short-circuit and the install correctly re-runs. The
//     guard ALSO verifies the on-disk body equals the freshly-fetched bytes, so a
//     sidecar that is somehow AHEAD of a stale body still re-installs rather than
//     billing stale prices — recovery is real, not just claimed. A 200 with no
//     X-Saturn-Price-Version header is refused at fetch (see below), so an installed
//     price file ALWAYS records a version — the sidecar names the installed body's
//     version, never a different one.
//   - When the fetch fails and a good local file exists, the file is left UNTOUCHED
//     (stale-but-priced) and the job exits 2. When the fetch fails and NO local file
//     exists, there is nothing to rate against and the job exits 2 as well — the
//     rater will then fail closed on its own missing-file check. Either way the
//     fetcher never installs an empty or partial book.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/rating"
)

// priceVersionHeader is the response header the pricing endpoint sets to the
// content-hash version of the served price file. It is the reconcile key: a later
// audit proves "phoebe billed window W against price-version V" by matching this to
// the sidecar this binary writes. It rides OUT-OF-BAND (not inside the YAML) by
// design — the YAML's own `version:` field is the SCHEMA version, a different thing.
const priceVersionHeader = "X-Saturn-Price-Version"

// versionSuffix is appended to the price-file path to name the sidecar that records
// the installed file's X-Saturn-Price-Version. e.g. prices.yaml -> prices.yaml.version.
const versionSuffix = ".version"

// tokenEnv is the env var carrying the customer auth token used to call the manager.
// Same install->manager direction (customer token) as the /customer/usage push; not
// a new auth surface.
const tokenEnv = "SATURN_TOKEN"

// exit codes (see package doc).
const (
	exitOK       = 0
	exitFatal    = 1
	exitFetchBad = 2 // fetch failed; any prior local file is left in place (stale).
)

// defaultRequestTimeout bounds the whole fetch (connect + read). The price file is
// small; a long stall means the manager is unhealthy and we should fall back to the
// last-good file rather than hang a CronJob pod.
const defaultRequestTimeout = 30 * time.Second

// tokenPricesPath is the endpoint path on the manager. The customer is identified by
// the auth token, so there is no per-customer path component.
const tokenPricesPath = "/customer/token-prices"

// maxPriceBodyBytes caps the served price file. The file is small (KBs); 8 MiB is a
// generous ceiling. We read one byte PAST the cap and REJECT an over-cap body rather
// than silently truncating: a truncated-but-parseable prefix could install a PARTIAL
// price book (a truncated valid YAML still parses under UnmarshalStrict), so we fail
// closed instead and leave the prior file in place.
const maxPriceBodyBytes = 8 << 20

func main() {
	os.Exit(run())
}

// run is main's entrypoint body; it parses the process args. Delegates to runWith so
// tests can drive the full flag/config/exit-code path without touching global flag
// state. Returns an exit code so deferred cleanup runs before exit (os.Exit skips it).
func run() int {
	return runWith(os.Args[1:])
}

// runWith is run() parameterized on the argument slice (everything after the program
// name). Mirrors cmd/rater's structure.
func runWith(argv []string) int {
	fs := flag.NewFlagSet("price-fetch", flag.ContinueOnError)
	settingsFile := fs.String("f", "/etc/saturn/config/price-fetch.yaml", "Settings YAML file path")
	urlFlag := fs.String("manager-url", "", "Manager base URL (overrides settings managerURL)")
	destFlag := fs.String("out", "", "Destination price-file path (overrides settings priceFile)")
	if err := fs.Parse(argv); err != nil {
		return exitFatal
	}

	log := logging.New(logging.INFO)

	opts, err := loadConfig(*settingsFile)
	if err != nil {
		log.Error.Printf("price-fetch: load config: %v", err)
		return exitFatal
	}
	if opts.debug {
		log.SetLevel(logging.DEBUG)
	}

	if *urlFlag != "" {
		opts.managerURL = *urlFlag
	}
	if *destFlag != "" {
		opts.priceFile = *destFlag
	}

	if opts.managerURL == "" {
		log.Error.Printf("price-fetch: no manager URL configured (set managerURL in the settings file or pass -manager-url)")
		return exitFatal
	}
	if opts.priceFile == "" {
		log.Error.Printf("price-fetch: no destination price file configured (set priceFile in the settings file or pass -out)")
		return exitFatal
	}
	// The token is REQUIRED: the endpoint authenticates the customer by it. Read it
	// from the env (a mounted secret), never a flag/settings file (it would land in
	// logs / a configmap).
	token := os.Getenv(tokenEnv)
	if token == "" {
		log.Error.Printf("price-fetch: %s is empty; the manager authenticates the customer by this token and will not serve prices without it", tokenEnv)
		return exitFatal
	}

	ctx, stop := signalContext()
	defer stop()

	if err := fetchAndInstall(ctx, log, opts, token); err != nil {
		// A fetch/validation failure is NOT fatal-to-the-system: a prior good file may
		// still be in place for the rater. Report it as "prices stale" (code 2). Probe
		// the local file with the RATER'S OWN loader (not a bare os.Stat) so the operator
		// learns whether rating can actually proceed, not just whether a file exists.
		if _, loadErr := rating.LoadPriceBook(opts.priceFile); loadErr == nil {
			log.Error.Printf("price-fetch: %v — leaving the existing price file %q in place and the rater CAN load it (stale-but-priced)", err, opts.priceFile)
		} else if errors.Is(loadErr, os.ErrNotExist) {
			log.Error.Printf("price-fetch: %v — NO local price file exists at %q (the rater will fail closed)", err, opts.priceFile)
		} else {
			log.Error.Printf("price-fetch: %v — existing price file %q is UNLOADABLE: %v (the rater will fail closed)", err, opts.priceFile, loadErr)
		}
		return exitFetchBad
	}
	return exitOK
}

// fetchAndInstall performs the whole fetch->validate->atomic-install. It returns an
// error WITHOUT touching the destination file on any failure before the final
// rename, so a failure always leaves the prior file (if any) intact.
func fetchAndInstall(ctx context.Context, log *logging.Logger, opts fetchOptions, token string) error {
	body, version, err := fetchPrices(ctx, opts, token)
	if err != nil {
		return err
	}

	// Validate with the RATER'S OWN loader before installing: if the rater would
	// reject these bytes, do not put them where the rater will read them. This is the
	// fail-closed guarantee — the rater can never be handed a file it chokes on.
	if _, err := rating.ParsePriceBook(body); err != nil {
		return fmt.Errorf("served price file failed validation (rater would reject it): %w", err)
	}

	// Idempotency: if the served version matches what is already installed AND the
	// on-disk body already equals the freshly-fetched bytes, do not rewrite the file.
	// Avoids churn (and a needless rename the rater could race) when prices have not
	// changed. A missing/empty sidecar means "unknown" -> install. The body check is
	// what makes recovery real: a sidecar that is AHEAD of a stale body (e.g. a crash
	// between the two renames, before body-first ordering existed) no longer
	// short-circuits — we re-install instead of billing stale prices forever.
	if version != "" {
		if installed, ok := readInstalledVersion(opts.priceFile); ok && installed == version {
			if onDisk, err := os.ReadFile(opts.priceFile); err == nil && bytes.Equal(onDisk, body) {
				log.Info.Printf("price-fetch: served version %q already installed at %q; no-op", version, opts.priceFile)
				return nil
			}
		}
	}

	if err := installAtomically(opts.priceFile, body, version); err != nil {
		return err
	}
	log.Info.Printf("price-fetch: installed price file %q (version %q, %d bytes)", opts.priceFile, version, len(body))
	return nil
}

// fetchPrices GETs the price file from the manager and returns the body and the
// X-Saturn-Price-Version header. A non-2xx status (including the endpoint's own 503
// for no-plan / incomplete-card) is an error: phoebe never installs a non-OK body.
func fetchPrices(ctx context.Context, opts fetchOptions, token string) ([]byte, string, error) {
	url := opts.managerURL + tokenPricesPath
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", fmt.Errorf("build request: %w", err)
	}
	// Same Authorization scheme as the rest of the install->manager direction.
	req.Header.Set("Authorization", "token "+token)

	client := &http.Client{Timeout: opts.requestTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("GET %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Cap the read so a misbehaving/huge response cannot exhaust memory, reading ONE
	// byte past the cap so we can DISTINGUISH a body that is exactly at the cap from one
	// that overran it. An over-cap body is rejected (not silently truncated): a
	// truncated valid-YAML prefix would install a partial book, so we fail closed.
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPriceBodyBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("read response from %s: %w", url, err)
	}
	if len(body) > maxPriceBodyBytes {
		return nil, "", fmt.Errorf("price file from %s exceeds %d bytes (refusing a possibly-truncated body)", url, maxPriceBodyBytes)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("GET %s: status %d (not serving prices)", url, resp.StatusCode)
	}
	// The manager ALWAYS sets X-Saturn-Price-Version on a 200: it is a deterministic
	// content hash of the served prices, set unconditionally (the endpoint raises an
	// error rather than serve a body without it). So an empty header on a 200 is not a
	// "best-effort omission" — it means something is wrong (a bug, a proxy stripping the
	// header, or the wrong endpoint answering). Fail closed: refuse the body and leave
	// the prior good file in place, rather than install prices we could never attribute
	// in a billing reconcile ("phoebe billed window W against price-version V" needs V).
	version := strings.TrimSpace(resp.Header.Get(priceVersionHeader))
	if version == "" {
		return nil, "", fmt.Errorf("GET %s: 200 response is missing the %s header (the manager always sets it; refusing an unversioned price file)", url, priceVersionHeader)
	}
	return body, version, nil
}

// installAtomically writes body to the destination via a temp file + rename(2), so a
// concurrent rater read can never observe a partial file. The BODY is written and
// renamed FIRST, then the sidecar: if the process dies between the two renames, the
// worst case is {body:B_new, sidecar:V_old}. The next run then sees served-version !=
// installed-version, so the idempotency guard does NOT short-circuit and the install
// correctly re-runs — making recovery real. The opposite (sidecar-first) ordering was
// dangerous: it could leave a sidecar AHEAD of stale body bytes, and the version-only
// idempotency no-op would then bill stale prices forever.
//
// PRECONDITION: version is non-empty. The caller (fetchPrices) fails closed on a 200
// with no X-Saturn-Price-Version header (a missing header is treated as a manager
// contract violation, not a best-effort omission), so an unversioned body never reaches
// here. We assert that precondition rather than silently branch on it: an empty version
// reaching this point is a programming error, and a price file installed with no recorded
// version would be unattributable in a billing reconcile.
func installAtomically(dest string, body []byte, version string) error {
	if version == "" {
		// Unreachable from production (fetchPrices guarantees a non-empty version); a
		// fail-loud assertion so a future caller can't silently install an unversioned,
		// unattributable price file.
		return fmt.Errorf("installAtomically: empty version (a price file must always record its X-Saturn-Price-Version)")
	}

	dir := filepath.Dir(dest)

	// Body first: the recorded version must never advance ahead of the bytes on disk.
	if err := writeFileSync(dir, dest, body); err != nil {
		return fmt.Errorf("install price file: %w", err)
	}
	if err := writeFileSync(dir, dest+versionSuffix, []byte(version)); err != nil {
		return fmt.Errorf("write version sidecar: %w", err)
	}
	return nil
}

// writeFileSync writes data to a uniquely-named temp file in dir, fsyncs it, renames
// it over dest, and then fsyncs the parent DIRECTORY so the rename itself is durable
// (POSIX: a rename is not crash-safe until the containing directory is fsynced). Both
// files are on the same filesystem (dir), so the rename is atomic. The temp file is
// cleaned up on any error before the rename. The file is installed world-readable
// (0644) — price data is NON-secret (the SATURN_TOKEN is never written here) and the
// rater may run under a DIFFERENT uid, so CreateTemp's default 0600 would lock it out.
func writeFileSync(dir, dest string, data []byte) error {
	tmp, err := os.CreateTemp(dir, ".price-fetch-*")
	if err != nil {
		return fmt.Errorf("create temp file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	// Clean up the temp file unless the rename below succeeds (which moves it).
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fsync temp file: %w", err)
	}
	// CreateTemp makes the file 0600; relax to 0644 BEFORE close so the rater (a
	// possibly-different uid) can read the installed file. *os.File.Chmod has no
	// TOCTOU window — it operates on the open descriptor.
	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmpName, dest); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmpName, dest, err)
	}
	// Fsync the parent directory so the rename survives a crash. Until this returns,
	// POSIX permits the directory entry to be lost even though the file data is synced.
	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir %s for fsync: %w", dir, err)
	}
	if err := dirFile.Sync(); err != nil {
		_ = dirFile.Close()
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	if err := dirFile.Close(); err != nil {
		return fmt.Errorf("close dir %s: %w", dir, err)
	}
	committed = true
	return nil
}

// readInstalledVersion reads the version sidecar next to dest. Returns ("", false)
// when the sidecar is absent or unreadable (treated as "unknown" -> re-install).
func readInstalledVersion(dest string) (string, bool) {
	data, err := os.ReadFile(dest + versionSuffix)
	if err != nil {
		return "", false
	}
	// Trim whitespace so an externally-seeded sidecar (e.g. `echo V > file`, which adds
	// a trailing newline) still compares equal to the trimmed header. We always WRITE
	// the sidecar without a trailing newline; this only hardens the read side.
	return strings.TrimSpace(string(data)), true
}

// fetchOptions are the resolved knobs from the settings file (+ flag/env overrides).
type fetchOptions struct {
	debug          bool
	managerURL     string
	priceFile      string
	requestTimeout time.Duration
}

// fetchSettings is the YAML shape for the settings file. Mirrors the rater's
// settings style (a thin struct per binary, no shared config package).
type fetchSettings struct {
	Debug bool `yaml:"debug"`

	// ManagerURL is the base URL of the central pricing service (saturn-aws-manager).
	// The endpoint path (/customer/token-prices) is appended by this binary.
	ManagerURL string `yaml:"managerURL"`

	// PriceFile is the LOCAL destination path — the same file the rater's priceFile
	// points at. This binary writes it; the rater reads it.
	PriceFile string `yaml:"priceFile"`

	// RequestTimeout bounds the whole fetch. "" means defaultRequestTimeout.
	RequestTimeout string `yaml:"requestTimeout"`
}

// loadConfig reads the YAML settings file and resolves defaults. A missing settings
// file is tolerated (all defaults + flag/env overrides), so the binary can run
// flag-only.
func loadConfig(path string) (fetchOptions, error) {
	opts := fetchOptions{requestTimeout: defaultRequestTimeout}

	var s fetchSettings
	if data, err := os.ReadFile(path); err == nil {
		if err := yaml.UnmarshalStrict(data, &s); err != nil {
			return opts, err
		}
	} else if !os.IsNotExist(err) {
		return opts, err
	}

	opts.debug = s.Debug
	opts.managerURL = s.ManagerURL
	opts.priceFile = s.PriceFile
	if s.RequestTimeout != "" {
		d, err := time.ParseDuration(s.RequestTimeout)
		if err != nil {
			return opts, fmt.Errorf("invalid requestTimeout %q: %w", s.RequestTimeout, err)
		}
		if d <= 0 {
			return opts, fmt.Errorf("requestTimeout must be positive, got %q", s.RequestTimeout)
		}
		opts.requestTimeout = d
	}
	return opts, nil
}

// signalContext returns a context cancelled on SIGTERM/SIGINT, so a CronJob pod
// shutdown aborts an in-flight fetch cleanly. Mirrors cmd/rater.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
}
