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
//	2  fetch FAILED (manager unreachable, non-200, malformed/invalid price file)
//	   BUT a previously-good local file is still in place. The rater can keep
//	   rating against it; this code says "prices are STALE, investigate" without
//	   conflating it with "the job is broken" (code 1) — distinct so a CronJob can
//	   alert on "prices not refreshing" separately from "fetcher crashed".
//
// FAIL-CLOSED CONTRACT (the one-way doors):
//   - The fetched bytes are validated by the rater's OWN loader (rating.ParsePriceBook)
//     before they are installed. A file the rater would reject is NEVER written into
//     place — the rater can never be handed a file it chokes on or that would $0-rate.
//   - The install is ATOMIC: write a temp file in the destination dir, fsync, then
//     rename(2) over the destination. The rater loads the file in one read; it can
//     never observe a half-written file. The version sidecar is written BEFORE the
//     rename so the recorded version can never describe bytes that are not yet (or
//     never) in place.
//   - When the fetch fails and a good local file exists, the file is left UNTOUCHED
//     (stale-but-priced) and the job exits 2. When the fetch fails and NO local file
//     exists, there is nothing to rate against and the job exits 2 as well — the
//     rater will then fail closed on its own missing-file check. Either way the
//     fetcher never installs an empty or partial book.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
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
		// still be in place for the rater. Report it as "prices stale" (code 2), and
		// say whether a local file exists so the operator knows if rating can proceed.
		if _, statErr := os.Stat(opts.priceFile); statErr == nil {
			log.Error.Printf("price-fetch: %v — leaving the existing price file %q in place (rater will rate against STALE prices)", err, opts.priceFile)
		} else {
			log.Error.Printf("price-fetch: %v — and NO local price file exists at %q (the rater will fail closed)", err, opts.priceFile)
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

	// Idempotency: if the served version matches what is already installed, do not
	// rewrite the file. Avoids churn (and a needless rename the rater could race) when
	// prices have not changed. A missing/empty sidecar means "unknown" -> install.
	if version != "" {
		if installed, ok := readInstalledVersion(opts.priceFile); ok && installed == version {
			log.Info.Printf("price-fetch: served version %q already installed at %q; no-op", version, opts.priceFile)
			return nil
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

	// Cap the read so a misbehaving/huge response cannot exhaust memory. The price
	// file is small (KBs); 8 MiB is a generous ceiling that still fails closed on a
	// runaway body.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, "", fmt.Errorf("read response from %s: %w", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("GET %s: status %d (not serving prices)", url, resp.StatusCode)
	}
	return body, resp.Header.Get(priceVersionHeader), nil
}

// installAtomically writes body to the destination via a temp file + rename(2), so a
// concurrent rater read can never observe a partial file. The version sidecar is
// written and fsynced BEFORE the destination rename: if the process dies between the
// two, the worst case is a sidecar describing a version whose bytes are not yet in
// place — caught next run by the version mismatch (re-install), never a file whose
// recorded version is AHEAD of its content during rating.
func installAtomically(dest string, body []byte, version string) error {
	dir := filepath.Dir(dest)

	// Sidecar first (best-effort ordering described above). Only when we have a
	// version to record; a served file with no version header installs without a
	// sidecar (idempotency then always re-installs, which is safe).
	if version != "" {
		if err := writeFileSync(dir, dest+versionSuffix, []byte(version)); err != nil {
			return fmt.Errorf("write version sidecar: %w", err)
		}
	}

	if err := writeFileSync(dir, dest, body); err != nil {
		return fmt.Errorf("install price file: %w", err)
	}
	return nil
}

// writeFileSync writes data to a uniquely-named temp file in dir, fsyncs it, and
// renames it over dest. Both file are on the same filesystem (dir), so the rename is
// atomic. The temp file is cleaned up on any error before the rename.
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
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmpName, dest); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmpName, dest, err)
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
	return string(data), true
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
