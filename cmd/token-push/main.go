// Command token-push is phoebe's install-side hourly billing PUSH job. It reads the
// rater's rated_usage rollups for a window, resolves each rollup's deployment to the
// org that owns it (resource_id → resource_name.org_id, a LOCAL join in the shared
// Atlas Postgres), and POSTs an hourly SNAPSHOT to the central manager's
// POST /customer/token-usage. The manager turns those rollups into Stripe charges; this
// binary is the trust-boundary crossing that gets the priced usage OUT of the install.
//
// See token-factory-stripe-consumer-design.md (Ben). This binary is that design's
// "install-side hourly push job" upstream prerequisite. The contract points it must
// honor:
//
//   - SNAPSHOT, not increments (Decision 3). Each push carries the COMPLETE current set
//     of rated_usage rows for one window_start, across all orgs/resources/models. The
//     manager treats a rated_usage_id it has on file for that (customer, window) but
//     that is ABSENT from the snapshot as a reconcile-DELETE. So a re-rate or a
//     reconcile-delete in phoebe self-corrects across the boundary — a bare incremental
//     delete would silently keep billing a vanished rollup forever.
//   - ORG RESOLVED INSTALL-SIDE (Decision 2 / C1). resource_id → org_id is joined HERE,
//     where the resource_name FK lives; the manager never reads the install DB. The
//     org_id rides the payload. A rollup whose resource_id resolves to NO org makes its
//     WHOLE WINDOW be WITHHELD (not pushed) and screamed about — because the snapshot is
//     delete-by-absence, pushing a window with a row omitted would silently DELETE that
//     row's prior (possibly already-billed) charge. Withholding leaves the manager's
//     prior good state for the window standing (stale-but-billed); the next run re-pushes
//     once the resource_name mapping is restored. NEVER pushed with a guessed/empty org
//     (C2/C7 fail-closed). Because rated_usage.resource_id is NOT NULL and the rater only
//     writes attributable rows, an unresolved org means a deployment row vanished from
//     resource_name: a real anomaly worth a non-zero exit.
//   - MONEY IS AN EXACT DECIMAL STRING end to end (C8). cost and the applied rates cross
//     the wire as NUMERIC text, never a float — read as ::text from Postgres, emitted as
//     JSON strings.
//   - AUTH = the install's customer auth_token (SATURN_TOKEN env), the same
//     install→manager direction as /customer/usage and /customer/token-prices. The token
//     authorizes the install to report its OWN usage; it carries no central authority.
//
// It is a ONE-SHOT batch job (cron / k8s CronJob), NOT a daemon: it pushes the windows it
// covers and exits. Exit codes:
//
//	0  every covered window's snapshot was accepted by the manager (or there was
//	   nothing to push — an empty window snapshots to an empty rollup set, which the
//	   manager still needs to apply delete-by-absence)
//	1  fatal: bad config / no auth token / DB unreachable / a window failed to push
//	2  some window was WITHHELD (not pushed) because it contained a rated_usage row
//	   that could NOT be resolved to an org (a deployment missing from resource_name);
//	   every window that WAS pushed succeeded. The withheld window's prior state on the
//	   manager is left untouched (stale-but-billed). Distinct code so a CronJob alerts on
//	   "usage we metered and priced but cannot attribute to an org" (held revenue / lost
//	   attribution) separately from "the job broke".
//
// WINDOW SELECTION mirrors the rater: default is the trailing N complete hours, so a
// rollup the rater re-rated or reconcile-deleted in an already-covered hour is re-
// snapshotted on a later run and the manager reconverges. A snapshot is idempotent on
// the manager (UPSERT by (customer_id, rated_usage_id) + delete-by-absence), so
// re-pushing a window never double-bills.
package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

// tokenEnv carries the install's customer auth token (a mounted secret), same as
// price-fetch. Never a flag/config — it must not land in logs or a configmap.
const tokenEnv = "SATURN_TOKEN"

// tokenUsagePath is the manager endpoint that ingests the hourly snapshot.
const tokenUsagePath = "/customer/token-usage"

// exit codes (see package doc).
const (
	exitOK       = 0
	exitFatal    = 1
	exitUnattrib = 2 // a window was WITHHELD (not pushed) — some row had no resolvable org.
)

// defaultPushTrailingHours is how many complete trailing hours each run snapshots,
// matching the rater's re-rate window so a late re-rate/delete is carried across the
// boundary. 24 = a day of reconcile tolerance for a cheap idempotent re-push.
const defaultPushTrailingHours = 24

// defaultRequestTimeout bounds a single window POST.
const defaultRequestTimeout = 60 * time.Second

func main() {
	os.Exit(run())
}

// run is main's entrypoint; it parses process args and delegates to runWith so tests
// can drive the full path without touching global flag state.
func run() int {
	return runWith(os.Args[1:])
}

func runWith(argv []string) int {
	fs := flag.NewFlagSet("token-push", flag.ContinueOnError)
	settingsFile := fs.String("f", "/etc/saturn/config/token-push.yaml", "Settings YAML file path")
	urlFlag := fs.String("manager-url", "", "Manager base URL (overrides settings managerURL)")
	since := fs.String("since", "", "Window start, RFC3339 (default: floor(now) minus trailingHours)")
	until := fs.String("until", "", "Window end, RFC3339 (default: start of the current hour)")
	if err := fs.Parse(argv); err != nil {
		return exitFatal
	}

	log := logging.New(logging.INFO)

	cfg, opts, err := loadConfig(*settingsFile)
	if err != nil {
		log.Error.Printf("token-push: load config: %v", err)
		return exitFatal
	}
	if opts.debug {
		log.SetLevel(logging.DEBUG)
	}
	if *urlFlag != "" {
		opts.managerURL = *urlFlag
	}
	if opts.managerURL == "" {
		log.Error.Printf("token-push: no manager URL configured (set managerURL in the settings file or pass -manager-url)")
		return exitFatal
	}

	token := os.Getenv(tokenEnv)
	if token == "" {
		log.Error.Printf("token-push: %s is empty; the manager authenticates the install by this token and will not accept usage without it", tokenEnv)
		return exitFatal
	}

	// The push window is hour-aligned, same rules as the rater (a snapshot must cover
	// whole hours — a partial-hour snapshot would delete-by-absence the rollups in the
	// rest of that hour).
	windows, err := resolveWindows(*since, *until, opts.trailingHours, time.Now())
	if err != nil {
		log.Error.Printf("token-push: %v", err)
		return exitFatal
	}

	ctx, stop := signalContext()
	defer stop()

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	db, err := openDB(pingCtx, cfg)
	cancel()
	if err != nil {
		log.Error.Printf("token-push: %v", err)
		return exitFatal
	}
	defer func() { _ = db.Close() }()

	p := &pusher{
		db:         db,
		log:        log,
		managerURL: opts.managerURL,
		token:      token,
		client:     &http.Client{Timeout: opts.requestTimeout},
	}

	return p.pushWindows(ctx, windows)
}

// signalContext returns a context cancelled on SIGTERM/SIGINT so a CronJob pod
// shutdown aborts an in-flight push cleanly. Mirrors cmd/rater and cmd/price-fetch.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
}
