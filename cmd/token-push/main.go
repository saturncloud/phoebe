// Command token-push is phoebe's install-side hourly usage PUSH job. For each
// hour in its window it reads the aggregator's rated_usage rollups and sends the
// complete set of that hour's token records to the manager's
// POST /customer/metered-usage with kinds ["token"] (billing-design.md section
// 3; wire details in internal/meteredusage). The manager prices and settles;
// phoebe sends quantities and the facts a price depends on, never money.
//
// It replaces the old POST /customer/token-usage push outright (section 3: no
// production phoebe existed, so there is no transition period and no flag).
//
// The contract points it honors:
//
//   - ONE REQUEST PER HOUR, A SNAPSHOT. The manager deletes, by absence, any
//     token record it holds for this install and hour that the request omits.
//     So an hour with no usage is sent as records: [] and a re-sent hour
//     replaces the previous one. usage_id is a deterministic hash of the
//     record's grain, so re-sending never duplicates.
//   - THE SETTLE HORIZON IS THE TRAILING WINDOW. Each run re-sends the last
//     pushTrailingHours complete hours (default 24, matching the rater's
//     rateTrailingHours), so late-drained events the rater folds into an old
//     hour reach the manager on the next run.
//   - FAIL CLOSED. An hour with a rollup that cannot become a valid record (no
//     org, or a wire-rule violation) is withheld whole, never sent short.
//   - RETRIES. 5xx, 429 and network errors are retried with capped exponential
//     backoff; a 429's Retry-After is honoured up to the backoff cap. Every
//     other status (400/401/409/422, other 4xx) is final.
//   - AUTH = the install's existing customer auth token (SATURN_TOKEN env), sent
//     as "Authorization: token <t>", the same credential and header as the old
//     push and /customer/usage. No new credential, no new scope.
//
// It is a ONE-SHOT batch job (a k8s CronJob). Exit codes:
//
//	0  every hour in the window was accepted by the manager
//	1  fatal: bad config, no token, DB unreachable, an hour that could not be
//	   read, a 5xx/429/network failure that outlasted the retries, a 200 that
//	   disagreed with the request, an entirely empty rated_usage table, or a
//	   window range wider than the backfill cap
//	2  some hour was WITHHELD (not sent) because a rollup could not become a
//	   valid record; every hour that was sent succeeded
//	3  the manager REJECTED some hour with a final status (400 wire rule, 401
//	   token, 409 install on the legacy billing path, 422 billing period
//	   already settled, any other non-429 4xx). Not retried; needs a person
//
// Precedence when several apply: 1 > 3 > 2.
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
	"github.com/saturncloud/phoebe/internal/meteredusage"
)

// tokenEnv carries the install's customer auth token (a mounted secret). Never a
// flag or config value: it must not land in logs or a configmap.
const tokenEnv = "SATURN_TOKEN"

// exit codes (see package doc).
const (
	exitOK       = 0
	exitFatal    = 1
	exitWithheld = 2 // an hour was not sent: a rollup could not become a valid record
	exitRejected = 3 // the manager answered 4xx for an hour; not retried
)

// maxPushWindows caps how many hour-windows a single run may enumerate (a backfill
// span guard). 90 days — far above any legitimate backfill, far below the tens of
// thousands a mistyped multi-year -since would produce. Bounds the deep-past edge of
// resolveWindows, symmetric to the future-hour clamp.
const maxPushWindows = 24 * 90

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
		db:  db,
		log: log,
		client: &meteredusage.Client{
			BaseURL: opts.managerURL,
			Token:   token,
			// One client for every hour of the run, so connections are reused.
			HTTP: &http.Client{Timeout: opts.requestTimeout},
		},
	}

	return p.pushWindows(ctx, windows)
}

// signalContext returns a context cancelled on SIGTERM/SIGINT so a CronJob pod
// shutdown aborts an in-flight push cleanly. Mirrors cmd/rater.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
}
