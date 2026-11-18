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
//     So a re-sent hour replaces the previous one, and an hour with no usage
//     is sent as records: [] only if this install sent that hour before
//     (pushed_hour or pushed_usage has it). An empty hour this install never
//     sent is skipped: the manager's records for it did not come from this
//     database (a wiped and restarted one, for instance), so an empty request
//     must not delete them. usage_id is a deterministic hash of the record's
//     grain, so re-sending never duplicates.
//
//   - THE SETTLE HORIZON IS THE TRAILING WINDOW. Each run re-sends the last
//     pushTrailingHours complete hours (default 24, matching the rater's
//     rateTrailingHours), so late-drained events the rater folds into an old
//     hour reach the manager on the next run.
//
//   - CONFLICTS HOLD BACK ONE IDENTITY, NEVER THE HOUR. A rollup that cannot
//     become a valid record (no org, or a wire-rule violation), and any identity
//     the rater has parked (cmd/rater), is left out of the request; every other
//     record of the hour is sent on time. The usage_ids the manager already
//     holds for that identity (recorded in pushed_usage) are listed in `held`,
//     so delete-by-absence cannot remove usage the manager accepted.
//
//   - RESOLVED HOURS ARE RE-SENT. Besides the trailing window, each run sends
//     every hour with parked usage resolved since that hour's last accepted push
//     (pushed_hour), however old.
//
//   - RETRIES. 5xx, 429 and network errors are retried with capped exponential
//     backoff; a 429's Retry-After is honoured up to the backoff cap. Every
//     other status is final for the run.
//
//   - AN UNREACHABLE MANAGER STOPS THE RUN. Connecting is bounded by 10 s dial
//     and 10 s TLS-handshake timeouts (the request timeout still bounds a slow
//     answer). When maxConsecutiveUnreachable (3) hours in a row fail because
//     the manager cannot be reached at all (DNS failure, connection refused or
//     timed out, TLS handshake failure or a certificate that does not verify),
//     the run logs "cannot reach manager at <url>: <cause>" at ERROR and stops
//     with exit 1, within minutes rather than at the job deadline. Blocked
//     egress is a misconfiguration, never a way to stop sending. Any answer
//     from the manager, including a 5xx, resets the count.
//
//   - REFUSED HOURS ARE PARKED. A 400 with the manager's validation body
//     ({"error", "path"}) will be given again for the same payload, so the hour
//     is recorded in refused_hour (status, body, SHA-256 of the request, count,
//     first and last refusal) and logged at ERROR. So is an hour whose request
//     breaks a request-level wire rule token-push checks itself (status 0). A
//     parked hour is rebuilt but not re-sent while its payload is unchanged,
//     and it does not count against maxUnsentHoursPerRun; each run re-checks up
//     to maxParkedRechecksPerRun of them, so one whose usage changes is sent
//     again. An explicit -since/-until run sends parked hours too (the way to
//     retry after fixing the cause). Every run with parked hours logs at WARN
//     how many there are. 401, 403 and 409 are refusals for the whole install,
//     never parked: the run stops at the first one. Any other 4xx (including a
//     400 without the manager's validation body) is not parked either.
//
//   - AUTH = the install's existing customer auth token (SATURN_TOKEN env), sent
//     as "Authorization: token <t>", the same credential and header as the old
//     push and /customer/usage. No new credential, no new scope.
//
//   - IT RUNS ON EVERY INSTALL (rulings.md 2026-11-17: usage is always recorded
//     and sent; whether it is charged is the manager's decision, by plan or
//     flag, never by switching collection off). Two states every install can be
//     in are therefore normal, not failures:
//
//   - NOT CONNECTED: the settings file sets managerURL to "" and no
//     -manager-url is given (an air-gapped install, or managerURI set to "").
//     A missing settings file is not this state: it is fatal. If the install
//     never pushed (pushed_hour is empty, or DATABASE_URL is not set at all),
//     nothing is sent and the run exits 0. The usage stays recorded in phoebe's
//     rated_usage, which the pruner never deletes, and is back-filled once a
//     manager URL is configured (MISSED HOURS below). If it HAS pushed before,
//     the manager URL was lost and the run exits 1, like the wiped-database
//     guard; so does a DATABASE_URL that is set but cannot be read, since the
//     run then cannot tell the two apart.
//
//   - NO USAGE YET: rated_usage, parked_usage and pushed_hour are all empty (a
//     fresh install that has served no Token Factory traffic). Nothing is
//     sent and the run exits 0. Sending would be wrong: every hour would go
//     as an empty snapshot and delete the manager's records for it.
//
//   - MISSED HOURS ARE RE-SENT, AND EARLIER USAGE IS BACK-FILLED. After the
//     trailing window and the resolved hours, each run sends up to
//     maxUnsentHoursPerRun never-accepted, unparked hours with usage: first
//     the oldest after the first hour accepted by a scheduled run
//     (token_push_state.auto_floor; for instance a manager outage longer than
//     the window), then, with what is left of the cap, the oldest before it
//     (usage from before the install was connected, and hours that failed
//     before that first acceptance). rated_usage is never pruned, so the
//     back-fill reaches the install's first hour of usage; each run logs how
//     many hours are left. Accepted hours (pushed_hour) are never back-filled
//     again. -since/-until re-sends any range on demand. An explicit run never
//     moves the floor.
//
// It is a ONE-SHOT batch job (a k8s CronJob). Exit codes:
//
//	0  every hour sent was accepted by the manager (hours already parked with
//	   unchanged usage are not sent and do not count); or the install was
//	   never connected to a manager; or it has no usage yet (see above)
//	1  fatal: a missing or bad settings file (-f), no manager URL on an
//	   install that has pushed before (a lost URL), a manager URL with no
//	   token, a manager URL that is not https, DATABASE_URL set but
//	   unreachable, an hour that could not be read or recorded, a
//	   5xx/429/network failure that outlasted the retries, a manager that could
//	   not be reached for 3 hours in a row (the run stops there), a
//	   200 that disagreed with the request, rated_usage empty after usage was
//	   already sent (a wiped table), or a window range wider than the backfill
//	   cap
//	2  token-push HELD BACK a rollup it could not turn into a valid record (no
//	   org, or a wire rule; the rest of its hour was sent), or parked an hour
//	   whose request breaks a request-level wire rule. Parked usage is the
//	   rater's signal (its exit 4), not this one
//	3  the manager REJECTED some hour with a final status: a 400 validation
//	   answer (the hour is parked; later runs do not fail for it while its
//	   usage is unchanged), 401 token, 403 inactive or uninstalled install, 409
//	   a manager older than the 2026-11-17 rule that accepts token usage from
//	   installs on the legacy billing path, or any other non-429 4xx. Not
//	   retried; needs a person
//
// Precedence when several apply: 1 > 3 > 2.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/meteredusage"
	"github.com/saturncloud/phoebe/internal/rating"
)

// tokenEnv carries the install's customer auth token (a mounted secret). Never a
// flag or config value: it must not land in logs or a configmap.
const tokenEnv = "SATURN_TOKEN"

// exit codes (see package doc).
const (
	exitOK       = 0
	exitFatal    = 1
	exitWithheld = 2 // token-push held back a rollup it could not turn into a valid record
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
		if err := checkManagerURL(*urlFlag, opts.allowInsecureLocalhost); err != nil {
			log.Error.Printf("token-push: -manager-url: %v", err)
			return exitFatal
		}
		opts.managerURL = *urlFlag
	}
	ctx, stop := signalContext()
	defer stop()

	if opts.managerURL == "" {
		return notConnected(ctx, log, cfg)
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

	store, closeStore, err := openStore(ctx, cfg)
	if err != nil {
		log.Error.Printf("token-push: %v", err)
		return exitFatal
	}
	defer func() { _ = closeStore() }()

	p := &pusher{
		store:      store,
		log:        log,
		managerURL: opts.managerURL,
		client: &meteredusage.Client{
			BaseURL: opts.managerURL,
			Token:   token,
			// One client for every hour of the run, so connections are reused.
			// Connecting is bounded by short dial and TLS-handshake timeouts,
			// so an unreachable manager fails in seconds.
			HTTP: meteredusage.NewHTTPClient(opts.requestTimeout),
		},
	}

	return p.pushWindows(ctx, windows, time.Now(), *since != "" || *until != "")
}

// openStore opens phoebe's Postgres (10 s to answer a ping) as token-push's
// store. A variable so the run tests can substitute an in-memory store.
var openStore = func(ctx context.Context, cfg rating.Config) (pushStore, func() error, error) {
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	db, err := openDB(pingCtx, cfg)
	if err != nil {
		return nil, nil, err
	}
	return pgStore{db: db}, db.Close, nil
}

// notConnected handles a run with no manager URL (package doc, NOT CONNECTED).
// An install that never pushed is not connected yet: nothing is sent and the
// run succeeds. An install that has pushed before has LOST its manager URL (a
// values change, a broken settings render): its usage would silently stop
// reaching the manager, so the run fails, the same discipline as the
// wiped-database guard. Telling the two apart needs the database:
//
//   - DATABASE_URL not set at all: the install has no phoebe database this job
//     can read, so it cannot have pushed from it; treated as never connected
//     (exit 0).
//   - DATABASE_URL set but the database unreachable, or the check fails: exit
//     1. The run cannot tell a lost URL from a never-connected install, and
//     guessing "never connected" would hide a lost URL behind a database
//     outage.
func notConnected(ctx context.Context, log *logging.Logger, cfg rating.Config) int {
	if cfg.DatabaseURL == "" {
		log.Info.Printf("token-push: no manager URL configured and no DATABASE_URL; this install is not connected to a manager, so nothing is sent")
		return exitOK
	}
	store, closeStore, err := openStore(ctx, cfg)
	if err != nil {
		log.Error.Printf("token-push: no manager URL configured, and the database that says whether this install has pushed before cannot be read: %v", err)
		return exitFatal
	}
	defer func() { _ = closeStore() }()
	everPushed, err := store.EverPushed(ctx)
	if err != nil {
		log.Error.Printf("token-push: no manager URL configured, and whether this install has pushed before cannot be read: %v", err)
		return exitFatal
	}
	if everPushed {
		log.Error.Printf("token-push: NO MANAGER URL, but this install has pushed usage to a manager before (pushed_hour has rows). The manager URL was lost (settings managerURL is \"\"), so usage is no longer sent. Restore managerURL (or pass -manager-url)")
		return exitFatal
	}
	log.Info.Printf("token-push: no manager URL configured; this install is not connected to a manager, so nothing is sent. Token usage stays recorded in phoebe's rated_usage table. Once managerURL is set, the first accepted run starts sending it, the trailing window first and then the earlier usage, %d hours per run at most", maxUnsentHoursPerRun)
	return exitOK
}

// signalContext returns a context cancelled on SIGTERM/SIGINT so a CronJob pod
// shutdown aborts an in-flight push cleanly. Mirrors cmd/rater.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
}
