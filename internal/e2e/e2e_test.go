//go:build integration

// Package e2e wires the REAL pipeline components together, in one process, and
// proves a token stream becomes money:
//
//	httptest fake vLLM → proxy.Server (real tee/rewrite/identity gates)
//	  → emit.DurableEmitter (real, async, WAL-backed) → miniredis stream
//	  → drain.Drainer (real consumer group) → Postgres billing_event
//	  → rating.Rater (real single-statement SQL rater) → rated_usage
//
// WHY THIS TEST EXISTS: the three worst bugs found in review were
// producer/consumer CONTRACT mismatches between components whose unit tests
// each passed — Event.Model carried the deployment id instead of the engine
// name (every event unpriceable), an empty request_id was poison-dropped by
// the drainer (served-but-never-billed), and an empty model string landed in
// the UNPRICED anomaly bucket instead of UNATTRIBUTABLE (wrong runbook).
// Nothing tested the actual pipe. This file does, and each of those three
// contracts is asserted by name below.
//
// Gated behind the `integration` build tag AND PHOEBE_TEST_DATABASE_URL, like
// internal/rating's conformance tests, so the unit lane never needs a
// database. Each test runs in its own isolated Postgres schema (DROP/CREATE)
// so it cannot collide with the rating integration tests' schemas. Run with:
//
//	PHOEBE_TEST_DATABASE_URL=postgres://... go test -tags=integration ./internal/e2e/...
package e2e

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/drain"
	"github.com/saturncloud/phoebe/internal/emit"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/metering"
	"github.com/saturncloud/phoebe/internal/proxy"
	"github.com/saturncloud/phoebe/internal/rating"
	"github.com/saturncloud/phoebe/internal/registry"
)

// vllmStream mirrors the realistic vLLM SSE fixture in internal/proxy/tee_test.go
// (that copy lives in package proxy's test files, so it can't be imported):
// content chunks, a finish_reason chunk, THEN the trailing usage chunk, then
// [DONE]. Every chunk carries the ENGINE-reported "model" — the stable price
// key — and the usage chunk reports cached tokens under
// prompt_tokens_details.cached_tokens, exactly as vLLM emits them.
const vllmStream = `data: {"id":"c1","object":"chat.completion.chunk","model":"llama-3-8b","choices":[{"index":0,"delta":{"role":"assistant","content":"Hello"}}]}

data: {"id":"c1","object":"chat.completion.chunk","model":"llama-3-8b","choices":[{"index":0,"delta":{"content":" world"}}]}

data: {"id":"c1","object":"chat.completion.chunk","model":"llama-3-8b","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

data: {"id":"c1","object":"chat.completion.chunk","model":"llama-3-8b","choices":[],"usage":{"prompt_tokens":2006,"total_tokens":2306,"completion_tokens":300,"prompt_tokens_details":{"cached_tokens":1920}}}

data: [DONE]

`

const (
	// The routing resource id is DELIBERATELY different from the engine model
	// name: the price key must be the latter, and the deployment-id-as-Model
	// bug is exactly the case where they get conflated.
	testResourceID = "deploy-abc123"
	testModelName  = "llama-3-8b"
	testAuthID     = "auth-key-e2e"

	streamName = "phoebe:metering:e2e"
)

// harness holds the real, wired pipeline components for one test: an isolated
// Postgres schema with the production migrations applied, a miniredis stream,
// and a started DurableEmitter. The drainer and rater are constructed per test
// from the same handles.
type harness struct {
	db       *sql.DB // pool pinned (via DSN search_path) to the isolated schema
	rdb      *redis.Client
	emitter  *emit.DurableEmitter
	drainCfg drain.Config
	log      *logging.Logger
}

// newHarness creates the isolated schema (DROP/CREATE so reruns are clean and
// it can never collide with the rating integration tests' schemas), applies
// the REAL migration files from migrations/ (read from disk so this test
// tracks the schema, not a divergent inline copy), and starts miniredis plus a
// real DurableEmitter pointed at it.
func newHarness(t *testing.T, schema string) *harness {
	t.Helper()
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set; skipping end-to-end pipeline test")
	}

	// Admin pool with the DEFAULT search_path: owns schema create/drop, and
	// installs btree_gist into a stable schema (public) so dropping OUR schema
	// can never cascade the extension out from under a concurrently running
	// rating integration test (packages run in parallel under `go test ./...`).
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
		_ = admin.Close()
	})
	mustExec(t, admin, "CREATE EXTENSION IF NOT EXISTS btree_gist")
	mustExec(t, admin, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	mustExec(t, admin, "CREATE SCHEMA "+schema)

	// Working pool: search_path pinned via the DSN so EVERY pooled connection
	// (drain store, rating store, assertions) lands in the isolated schema —
	// a per-connection `SET search_path` would silently not stick across a
	// database/sql pool.
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	db, err := sql.Open("pgx", dsn+sep+"search_path="+schema)
	if err != nil {
		t.Fatalf("open schema pool: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// The production migrations, from disk. 0002 is multi-statement (and its
	// CREATE EXTENSION is a no-op after the admin install above); pgx's stdlib
	// driver execs an argument-less query over the simple protocol, which
	// accepts multiple statements — same approach as the files' psql usage.
	mustExec(t, db, readMigration(t, "0001_billing_event.sql"))
	mustExec(t, db, readMigration(t, "0002_rating.sql"))

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	log := logging.New(logging.ERROR)

	emitCfg := emit.DefaultConfig()
	emitCfg.ValkeyAddr = mr.Addr()
	emitCfg.StreamName = streamName
	emitCfg.WALPath = filepath.Join(t.TempDir(), "wal.jsonl")
	emitCfg.ShipInterval = 50 * time.Millisecond
	emitCfg.WorkerCount = 2
	emitCfg.ChanBuf = 64
	emitter, err := emit.New(emitCfg, log, rdb)
	if err != nil {
		t.Fatalf("emit.New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = emitter.Close(ctx)
	})

	drainCfg := drain.DefaultConfig()
	drainCfg.ValkeyAddr = mr.Addr()
	drainCfg.StreamName = streamName
	drainCfg.Group = "phoebe-drainer-e2e"
	drainCfg.Consumer = "drainer-e2e-1"
	drainCfg.BatchSize = 16
	drainCfg.BlockTimeout = 50 * time.Millisecond

	return &harness{db: db, rdb: rdb, emitter: emitter, drainCfg: drainCfg, log: log}
}

// proxyServer builds a real proxy.Server routing every resource id to the
// given upstream, emitting through the harness's real DurableEmitter.
func (h *harness) proxyServer(t *testing.T, upstream string) *proxy.Server {
	t.Helper()
	u, err := url.Parse(upstream)
	if err != nil {
		t.Fatalf("parse upstream url: %v", err)
	}
	settings := &config.Settings{ListenAddr: ":0", BillPartialOnAbort: true}
	return proxy.New(settings, h.log, registry.NewStatic(u), h.emitter)
}

// waitForStreamLen polls the miniredis stream until it holds at least n
// entries: the emitter ships ASYNCHRONOUSLY (channel → worker → XADD), so the
// stream must be polled with a deadline, never read immediately after Emit.
func (h *harness) waitForStreamLen(t *testing.T, n int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got, err := h.rdb.XLen(context.Background(), streamName).Result()
		if err != nil {
			t.Fatalf("xlen: %v", err)
		}
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %s has %d entries after %s, want >= %d (emitter never shipped)",
				streamName, got, timeout, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// drainUntilRows runs a REAL drain.Drainer via its production Run loop
// (consumer group create, XREADGROUP, store-then-ACK) against a real Postgres
// store, until billing_event holds n rows or the deadline passes, then cancels
// the loop and waits for it to exit cleanly.
func (h *harness) drainUntilRows(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	store := drain.NewPostgresStore(h.db)
	d := drain.New(h.drainCfg, h.log, h.rdb, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()

	deadline := time.Now().Add(timeout)
	for {
		var rows int
		if err := h.db.QueryRow("SELECT COUNT(*) FROM billing_event").Scan(&rows); err != nil {
			t.Fatalf("count billing_event: %v", err)
		}
		if rows >= n {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("billing_event has %d rows after %s, want %d (drainer never stored the event)",
				rows, timeout, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("drainer Run returned error: %v", err)
	}
	if got := d.Poisoned(); got != 0 {
		t.Fatalf("drainer poison-dropped %d events; a real event must never be poison", got)
	}
}

// priceBookYAML is the YAML price file for the e2e rater (E1): prices are a config
// file now, not DB rows. It prices the ENGINE model name with the same rates the
// old model_price seed used, so the expected cost is unchanged.
const priceBookYAML = `
version: 1
base_models:
  "llama-3-8b":
    prompt:     "0.000005"
    cached:     "0.0000005"
    completion: "0.00002"
`

// priceBook parses the e2e YAML price file into a PriceBook (fail-closed if the
// fixture is malformed).
func (h *harness) priceBook(t *testing.T) *rating.PriceBook {
	t.Helper()
	pb, err := rating.ParsePriceBook([]byte(priceBookYAML))
	if err != nil {
		t.Fatalf("parse e2e price book: %v", err)
	}
	return pb
}

// rateEventHour reads the stored event's rating instant back from billing_event,
// truncates it to its UTC hour, and runs a real rating.Rater (priced from the YAML
// PriceBook) over exactly that hour window — so the test never races a wall-clock
// hour boundary.
func (h *harness) rateEventHour(t *testing.T, book *rating.PriceBook) rating.Result {
	t.Helper()
	var evTS time.Time
	if err := h.db.QueryRow(
		"SELECT MIN(COALESCE(event_ts, created_at)) FROM billing_event").Scan(&evTS); err != nil {
		t.Fatalf("read event_ts: %v", err)
	}
	hour := evTS.UTC().Truncate(time.Hour)

	rater := rating.New(rating.NewPostgresStore(h.db), book, h.log)
	res, err := rater.Run(context.Background(), hour, hour.Add(time.Hour))
	if err != nil {
		t.Fatalf("rater.Run: %v", err)
	}
	return res
}

// assertNumericEqual compares two NUMERIC values IN Postgres — money never
// becomes a Go number, so equality is delegated to the database too.
func (h *harness) assertNumericEqual(t *testing.T, got, want, what string) {
	t.Helper()
	var equal bool
	if err := h.db.QueryRow("SELECT $1::numeric = $2::numeric", got, want).Scan(&equal); err != nil {
		t.Fatalf("numeric compare %s: %v", what, err)
	}
	if !equal {
		t.Errorf("%s = %s, want %s", what, got, want)
	}
}

func mustExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %.80q...: %v", q, err)
	}
}

// readMigration loads a production migration file from migrations/ relative to
// this package (go test runs with the package dir as cwd).
func readMigration(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
	if err != nil {
		t.Fatalf("read migration %s: %v", name, err)
	}
	return string(b)
}

// TestE2E_StreamedRequestBecomesMoney is THE pipeline test: one streamed
// request through the real proxy → real emitter → miniredis → real drainer →
// real Postgres → real rater, asserting at the end that the money is right and
// every cross-component contract held:
//
//   - model contract: rated_usage.model_id is the ENGINE-reported name
//     ("llama-3-8b"), never the routing resource id ("deploy-abc123") the
//     request was addressed to — the deployment-id-as-Model bug made every
//     event unpriceable;
//   - request_id contract: the request is sent WITHOUT X-Request-Id, so the
//     proxy must mint a "phoebe-" id that survives to billing_event verbatim —
//     the empty-request_id bug had the drainer poison-dropping such events
//     (served-but-never-billed);
//   - the money: cost equals the independently hand-computed NUMERIC, with
//     zero unpriced and zero unattributable events.
func TestE2E_StreamedRequestBecomesMoney(t *testing.T) {
	h := newHarness(t, "phoebe_e2e_pipeline")

	// 1. Upstream: a fake vLLM streaming the SSE fixture chunk by chunk.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		for _, chunk := range strings.SplitAfter(vllmStream, "\n\n") {
			if chunk == "" {
				continue
			}
			_, _ = io.WriteString(w, chunk)
			if fl != nil {
				fl.Flush()
			}
		}
	}))
	defer backend.Close()

	// 2. Proxy: real Server + real DurableEmitter. Full identity headers,
	//    resource id != model name, and deliberately NO X-Request-Id — this
	//    exercises the generated-id path end to end.
	srv := h.proxyServer(t, backend.URL)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"whatever-the-client-said","stream":true,"messages":[]}`))
	req.Header.Set(identity.HeaderAuthID, testAuthID)
	req.Header.Set(identity.HeaderResourceID, testResourceID)
	req.Header.Set(identity.HeaderResourceType, "deployment")
	req.Header.Set(identity.HeaderUserID, "user-e2e")
	req.Header.Set(identity.HeaderGroupID, "group-e2e")
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("proxy status = %d, want 200 (body: %s)", rr.Code, rr.Body.String())
	}
	if rr.Body.String() != vllmStream {
		t.Fatalf("client did not receive the SSE stream verbatim:\n%q", rr.Body.String())
	}
	clientRequestID := rr.Header().Get("X-Request-Id")
	if !strings.HasPrefix(clientRequestID, "phoebe-") {
		t.Fatalf("response X-Request-Id = %q, want generated phoebe-<hex> (the client's only billing handle)", clientRequestID)
	}

	// 3. Drainer: the emit is async — wait for the stream entry, then run the
	//    real drain loop until the row is durable in Postgres.
	h.waitForStreamLen(t, 1, 5*time.Second)
	h.drainUntilRows(t, 1, 10*time.Second)

	var (
		nRows                         int
		requestID, authID, resourceID string
		model                         sql.NullString
		prompt, cached, completion    int
		aborted                       bool
	)
	if err := h.db.QueryRow("SELECT COUNT(*) FROM billing_event").Scan(&nRows); err != nil {
		t.Fatalf("count billing_event: %v", err)
	}
	if nRows != 1 {
		t.Fatalf("billing_event rows = %d, want exactly 1", nRows)
	}
	if err := h.db.QueryRow(
		`SELECT request_id, auth_id, resource_id, model, prompt_tokens, cached_tokens, completion_tokens, aborted
		 FROM billing_event`).
		Scan(&requestID, &authID, &resourceID, &model, &prompt, &cached, &completion, &aborted); err != nil {
		t.Fatalf("read billing_event: %v", err)
	}

	// REQUEST-ID CONTRACT: non-empty, phoebe-generated, and the SAME id the
	// client was given — empty here is the poison-drop bug.
	if requestID == "" || !strings.HasPrefix(requestID, "phoebe-") {
		t.Errorf("billing_event.request_id = %q, want non-empty phoebe-<hex> (generated-id path)", requestID)
	}
	if requestID != clientRequestID {
		t.Errorf("billing_event.request_id = %q but client was told %q — correlation broken", requestID, clientRequestID)
	}

	// MODEL CONTRACT: the engine name, not the deployment id.
	if !model.Valid || model.String != testModelName {
		t.Errorf("billing_event.model = %v, want %q (the ENGINE-reported name)", model, testModelName)
	}
	if model.String == testResourceID {
		t.Errorf("billing_event.model = %q equals the routing resource id — the price key must be the engine model name", model.String)
	}
	if resourceID != testResourceID {
		t.Errorf("billing_event.resource_id = %q, want %q", resourceID, testResourceID)
	}
	if authID != testAuthID {
		t.Errorf("billing_event.auth_id = %q, want %q", authID, testAuthID)
	}
	if prompt != 2006 || cached != 1920 || completion != 300 {
		t.Errorf("billing_event tokens = %d/%d/%d, want 2006/1920/300 (from the trailing usage chunk)", prompt, cached, completion)
	}
	if aborted {
		t.Error("billing_event.aborted = true for a completed stream")
	}

	// 4. Rater: price the ENGINE name from the YAML price file, rate the event's hour.
	res := h.rateEventHour(t, h.priceBook(t))

	// 5. The money. Expected cost, hand-derived from the fixture and the seeded
	//    prices (mirrors the Rate() oracle's billable-prompt formula, computed
	//    independently here since the oracle is test-only inside package rating):
	//
	//	billable_prompt = prompt - cached = 2006 - 1920 = 86
	//	cost = 86   * 0.000005  = 0.000430   (non-cached prompt)
	//	     + 1920 * 0.0000005 = 0.000960   (cached prompt)
	//	     + 300  * 0.00002   = 0.006000   (completion)
	//	     = 0.007390
	const wantCost = "0.00739"

	if res.EventsRated != 1 || res.RollupsWritten != 1 {
		t.Fatalf("rater Result = %+v, want exactly 1 event rated into 1 rollup", res)
	}
	if res.UnpricedEvents != 0 || res.UnattributableEvents != 0 {
		t.Fatalf("rater anomalies = %d unpriced / %d unattributable, want 0/0 — the pipeline leaked the price key or identity",
			res.UnpricedEvents, res.UnattributableEvents)
	}
	h.assertNumericEqual(t, res.TotalCost, wantCost, "Result.TotalCost")

	var (
		nRollups                                     int
		ruAuthID, ruModelID, cost                    string
		ruPrompt, ruCached, ruCompletion, ruBillable int64
		eventCount                                   int
	)
	if err := h.db.QueryRow("SELECT COUNT(*) FROM rated_usage").Scan(&nRollups); err != nil {
		t.Fatalf("count rated_usage: %v", err)
	}
	if nRollups != 1 {
		t.Fatalf("rated_usage rows = %d, want exactly 1", nRollups)
	}
	if err := h.db.QueryRow(
		`SELECT auth_id, model_id, prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens, cost::text, event_count
		 FROM rated_usage`).
		Scan(&ruAuthID, &ruModelID, &ruPrompt, &ruCached, &ruCompletion, &ruBillable, &cost, &eventCount); err != nil {
		t.Fatalf("read rated_usage: %v", err)
	}
	if ruAuthID != testAuthID {
		t.Errorf("rated_usage.auth_id = %q, want %q (the X-Saturn-Auth-Id header value)", ruAuthID, testAuthID)
	}
	// THE deployment-id-bug guard, at the far end of the pipe: the money is
	// keyed on the engine name the upstream reported, not the id we routed on.
	if ruModelID != testModelName {
		t.Errorf("rated_usage.model_id = %q, want %q (engine name, not resource id)", ruModelID, testModelName)
	}
	if ruPrompt != 2006 || ruCached != 1920 || ruCompletion != 300 || ruBillable != 86 {
		t.Errorf("rated_usage tokens = %d/%d/%d billable=%d, want 2006/1920/300 billable=86",
			ruPrompt, ruCached, ruCompletion, ruBillable)
	}
	if eventCount != 1 {
		t.Errorf("rated_usage.event_count = %d, want 1", eventCount)
	}
	h.assertNumericEqual(t, cost, wantCost, "rated_usage.cost")
}

// ftVllmStream is the vLLM SSE fixture for a FINE-TUNE deployment: the engine reports
// the ft:<checkpoint> id as its model name (the stable price key for a fine-tune, E3).
const ftVllmStream = `data: {"id":"c2","object":"chat.completion.chunk","model":"ft:9f8e7d6c5b4a","choices":[{"index":0,"delta":{"role":"assistant","content":"Hi"}}]}

data: {"id":"c2","object":"chat.completion.chunk","model":"ft:9f8e7d6c5b4a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

data: {"id":"c2","object":"chat.completion.chunk","model":"ft:9f8e7d6c5b4a","choices":[],"usage":{"prompt_tokens":1000,"total_tokens":1000,"completion_tokens":0,"prompt_tokens_details":{"cached_tokens":0}}}

data: [DONE]

`

// ftPriceBookYAML prices ONLY the base model + a 1.5× premium. The fine-tune's
// ft:<checkpoint> id is NOT listed — it prices through the event-carried base_model.
const ftPriceBookYAML = `
version: 1
base_models:
  "meta-llama/Llama-3.1-8B-Instruct":
    prompt:     "0.000004"
    cached:     "0"
    completion: "0"
fine_tune_premium:
  policy: multiplier
  factor: "1.5"
`

// TestE2E_FineTuneBillsAtBaseTimesPremium is the fine-tune pipeline test: a request to
// a fine-tune deployment carries the X-Saturn-Base-Model header (as Atlas injects it
// at deploy), the engine reports the ft:<checkpoint> id as its model, and the whole
// pipe must end with the fine-tune billed at base × premium — proving base_model rides
// end to end (proxy → emit → drain → billing_event.base_model → rater derived join).
//
//   - base_model contract: billing_event.base_model is the HF base id from the header,
//     NOT empty — the propagation seam the rater needs to price an ft: id;
//   - the money: the ft: id (absent from the price file) bills at base × 1.5 via its
//     base_model, with zero unpriced events.
func TestE2E_FineTuneBillsAtBaseTimesPremium(t *testing.T) {
	h := newHarness(t, "phoebe_e2e_finetune")

	const (
		ftModelName = "ft:9f8e7d6c5b4a"
		ftBaseModel = "meta-llama/Llama-3.1-8B-Instruct"
	)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		for _, chunk := range strings.SplitAfter(ftVllmStream, "\n\n") {
			if chunk == "" {
				continue
			}
			_, _ = io.WriteString(w, chunk)
			if fl != nil {
				fl.Flush()
			}
		}
	}))
	defer backend.Close()

	srv := h.proxyServer(t, backend.URL)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"my-finetune","stream":true,"messages":[]}`))
	req.Header.Set(identity.HeaderAuthID, testAuthID)
	req.Header.Set(identity.HeaderResourceID, testResourceID)
	req.Header.Set(identity.HeaderResourceType, "deployment")
	req.Header.Set(identity.HeaderUserID, "user-e2e")
	req.Header.Set(identity.HeaderGroupID, "group-e2e")
	// The base_model header Atlas injects at deploy for a fine-tune endpoint.
	req.Header.Set(identity.HeaderBaseModel, ftBaseModel)
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("proxy status = %d, want 200 (body: %s)", rr.Code, rr.Body.String())
	}

	h.waitForStreamLen(t, 1, 5*time.Second)
	h.drainUntilRows(t, 1, 10*time.Second)

	// base_model contract: stored verbatim, alongside the ft: model name.
	var model, baseModel sql.NullString
	if err := h.db.QueryRow("SELECT model, base_model FROM billing_event").Scan(&model, &baseModel); err != nil {
		t.Fatalf("read billing_event: %v", err)
	}
	if !model.Valid || model.String != ftModelName {
		t.Errorf("billing_event.model = %v, want %q (the ft: checkpoint id)", model, ftModelName)
	}
	if !baseModel.Valid || baseModel.String != ftBaseModel {
		t.Fatalf("billing_event.base_model = %v, want %q — the header must ride to billing_event for the rater to price the fine-tune", baseModel, ftBaseModel)
	}

	// Rate from the YAML book that prices ONLY the base; the ft: id must resolve via
	// base_model at base × premium.
	book, err := rating.ParsePriceBook([]byte(ftPriceBookYAML))
	if err != nil {
		t.Fatalf("parse ft price book: %v", err)
	}
	res := h.rateEventHour(t, book)

	if res.UnpricedEvents != 0 || res.UnattributableEvents != 0 {
		t.Fatalf("anomalies = %d unpriced / %d unattributable, want 0/0 — base_model failed to plumb the fine-tune's price", res.UnpricedEvents, res.UnattributableEvents)
	}
	if res.EventsRated != 1 || res.RollupsWritten != 1 {
		t.Fatalf("rater Result = %+v, want 1 event / 1 rollup", res)
	}
	// 1000 prompt tokens × (0.000004 base × 1.5 premium) = 0.006.
	const wantCost = "0.006"
	h.assertNumericEqual(t, res.TotalCost, wantCost, "Result.TotalCost (base × premium)")

	var ruModelID, cost, appliedPrompt string
	if err := h.db.QueryRow(
		`SELECT model_id, cost::text, applied_prompt_rate::text FROM rated_usage`).
		Scan(&ruModelID, &cost, &appliedPrompt); err != nil {
		t.Fatalf("read rated_usage: %v", err)
	}
	if ruModelID != ftModelName {
		t.Errorf("rated_usage.model_id = %q, want %q (the fine-tune's ft: id)", ruModelID, ftModelName)
	}
	h.assertNumericEqual(t, appliedPrompt, "0.000006", "rated_usage.applied_prompt_rate (base × premium frozen on row)")
	h.assertNumericEqual(t, cost, wantCost, "rated_usage.cost")
}

// TestE2E_ModellessEventIsUnattributable pins the nullStr(model) contract end
// to end: an event whose upstream never reported a model (e.g. an abort before
// the first chunk) must reach Postgres with model = NULL and be counted by the
// rater as UNATTRIBUTABLE — never UNPRICED. A stored ” would dodge the
// `model_id IS NULL` predicate and misreport as unpriced, pointing operators
// at the wrong runbook ("backfill prices" instead of "fix the capture gap").
func TestE2E_ModellessEventIsUnattributable(t *testing.T) {
	h := newHarness(t, "phoebe_e2e_modelless")

	// Emit directly through the REAL emitter, exactly as the proxy does for a
	// BillPartialOnAbort=true abort with no usage chunk: empty Model, zero
	// counts, Aborted. (See proxy.Server.emit.)
	h.emitter.Emit(context.Background(), metering.Event{
		RequestID:    "phoebe-e2e-modelless-0001",
		AuthID:       testAuthID,
		UserID:       "user-e2e",
		GroupID:      "group-e2e",
		ResourceID:   testResourceID,
		ResourceType: "deployment",
		Model:        "", // upstream emitted no parseable model
		Aborted:      true,
	})

	h.waitForStreamLen(t, 1, 5*time.Second)
	h.drainUntilRows(t, 1, 10*time.Second)

	// The drain store must write NULL, not '' (the nullStr(model) contract).
	var modelIsNull bool
	if err := h.db.QueryRow(
		"SELECT model IS NULL FROM billing_event WHERE request_id = 'phoebe-e2e-modelless-0001'").
		Scan(&modelIsNull); err != nil {
		t.Fatalf("read billing_event.model: %v", err)
	}
	if !modelIsNull {
		t.Fatal("billing_event.model is not NULL for a model-less event — '' dodges the rater's unattributable predicate")
	}

	// Price book loaded so an event misfiled as "unpriced" could only mean the
	// bucket logic itself is wrong, not a missing price.
	res := h.rateEventHour(t, h.priceBook(t))

	if res.UnattributableEvents != 1 {
		t.Errorf("UnattributableEvents = %d, want 1 (the model-less event)", res.UnattributableEvents)
	}
	if res.UnpricedEvents != 0 {
		t.Errorf("UnpricedEvents = %d, want 0 — a model-less event must land in UNATTRIBUTABLE, not UNPRICED (wrong runbook)", res.UnpricedEvents)
	}
	if res.EventsRated != 0 || res.RollupsWritten != 0 {
		t.Errorf("rater billed a model-less event: %+v (must never be rated, let alone $0-billed)", res)
	}
	if !res.HasAnomaly() {
		t.Error("Result.HasAnomaly() = false — the leak must drive the exit-nonzero path")
	}
	h.assertNumericEqual(t, res.TotalCost, "0", "Result.TotalCost")
}
