//go:build integration

// Package e2e wires the REAL pipeline components together, in one process, and
// proves a token stream becomes a usage record:
//
//	httptest fake vLLM → proxy.Server (real tee/rewrite/identity gates)
//	  → emit.DurableEmitter (real, async, WAL-backed) → miniredis stream
//	  → drain.Drainer (real consumer group) → Postgres billing_event
//	  → rating.Aggregator (real single-statement SQL aggregator) → rated_usage
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
	"github.com/saturncloud/phoebe/migrations"
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
	testOrgID      = "org-e2e"
	testBaseModel  = "meta-llama/Meta-Llama-3-8B-Instruct"

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

	// The production migrations, from the embedded migrations.FS (same source
	// cmd/migrate applies). 0002 is multi-statement (and its CREATE EXTENSION is a
	// no-op after the admin install above); pgx's stdlib driver execs an
	// argument-less query over the simple protocol, which accepts multiple
	// statements — same approach as the files' psql usage.
	mustExec(t, db, readMigration(t, "0001_billing_event.up.sql"))
	mustExec(t, db, readMigration(t, "0002_rating.up.sql"))
	// 0003 (io_log) is not needed by this pipeline. 0004 adds
	// billing_event.serving_mode, which the drainer's INSERT references — the
	// exact drift this harness exists to catch (a fresh staging DB without it
	// poison-dropped every event with SQLSTATE 42703).
	mustExec(t, db, readMigration(t, "0004_billing_event_serving_mode.up.sql"))
	mustExec(t, db, readMigration(t, "0005_invoice_grade_attempts.up.sql"))
	// 0006 adds billing_event.graph_k8s_name (in the drainer's INSERT) and widens
	// the rated_usage grain the rater upserts on — same drift class as 0004.
	mustExec(t, db, readMigration(t, "0006_rollup_grain.up.sql"))
	// 0007 makes rated_usage.serving_mode 'shared'/'dedicated' only (CHECK, no
	// default); the rater's upsert must satisfy it.
	mustExec(t, db, readMigration(t, "0007_serving_mode_explicit.up.sql"))
	// 0008 adds the serving fields the drainer inserts; 0009 makes graph and
	// revision rollup grain keys, which the rater upserts on.
	mustExec(t, db, readMigration(t, "0008_billing_event_topology.up.sql"))
	mustExec(t, db, readMigration(t, "0009_rollup_grain_graph_revision.up.sql"))
	// 0010 adds the usage-identity attributes the rater writes.
	mustExec(t, db, readMigration(t, "0010_rated_usage_identity.up.sql"))
	// 0011: the money columns are nullable; the aggregator writes NULL.
	mustExec(t, db, readMigration(t, "0011_rated_usage_quantities_only.up.sql"))

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

// proxyServer builds a real proxy.Server emitting through the harness's real
// DurableEmitter. The proxy no longer resolves upstreams — each test request
// carries the forward target in the X-Saturn-Upstream header (as Atlas injects
// it per route), so this helper takes no upstream.
func (h *harness) proxyServer(t *testing.T) *proxy.Server {
	t.Helper()
	settings := &config.Settings{ListenAddr: ":0"}
	return proxy.New(settings, h.log, h.emitter)
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

// aggregateEventHour reads the stored event's instant back from billing_event,
// truncates it to its UTC hour, and runs the real rating.Aggregator over exactly
// that hour, so the test never races a wall-clock hour boundary.
func (h *harness) aggregateEventHour(t *testing.T) rating.Result {
	t.Helper()
	var evTS time.Time
	if err := h.db.QueryRow(
		"SELECT MIN(COALESCE(event_ts, created_at)) FROM billing_event").Scan(&evTS); err != nil {
		t.Fatalf("read event_ts: %v", err)
	}
	hour := evTS.UTC().Truncate(time.Hour)

	res, err := rating.New(rating.NewPostgresStore(h.db), h.log).Run(context.Background(), hour, hour.Add(time.Hour), false)
	if err != nil {
		t.Fatalf("aggregator.Run: %v", err)
	}
	return res
}

// assertNoMoney fails if any rollup carries a price: the manager prices usage
// (billing-design.md D1) and phoebe must write NULL.
func (h *harness) assertNoMoney(t *testing.T) {
	t.Helper()
	var priced int
	if err := h.db.QueryRow(`SELECT COUNT(*) FROM rated_usage WHERE cost IS NOT NULL OR applied_prompt_rate IS NOT NULL
		OR applied_cached_rate IS NOT NULL OR applied_completion_rate IS NOT NULL OR price_key IS NOT NULL`).Scan(&priced); err != nil {
		t.Fatalf("read money columns: %v", err)
	}
	if priced != 0 {
		t.Fatalf("%d rollups carry money; phoebe no longer prices", priced)
	}
}

func mustExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("exec %.80q...: %v", q, err)
	}
}

// readMigration loads a production migration file from the SAME embedded FS that
// cmd/migrate applies (migrations.FS), so this test can never drift from the real
// apply path or a filename rename. It applies the up migrations directly (rather
// than via golang-migrate) so it can target an isolated per-test schema.
func readMigration(t *testing.T, name string) string {
	t.Helper()
	b, err := migrations.FS.ReadFile(name)
	if err != nil {
		t.Fatalf("read migration %s: %v", name, err)
	}
	return string(b)
}

// TestE2E_StreamedRequestBecomesAUsageRecord is THE pipeline test: one streamed
// request through the real proxy → real emitter → miniredis → real drainer →
// real Postgres → real rater, asserting at the end that the money is right and
// every cross-component contract held:
//
//   - model contract: rated_usage.model_id is the ENGINE-reported name
//     ("llama-3-8b"), never the routing resource id ("deploy-abc123") the
//     request was addressed to — the deployment-id-as-Model bug made every
//     event unpriceable;
//   - request_id contract: Phoebe's internal attempt id survives to billing_event verbatim —
//     the empty-request_id bug had the drainer poison-dropping such events
//     (served-but-never-billed);
//   - the quantities: the rollup carries the engine's exact token counts, the
//     payer, owner, serving mode and base model, and no money (the manager prices).
func TestE2E_StreamedRequestBecomesAUsageRecord(t *testing.T) {
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
	//    resource id != model name, with a replayable client correlation id.
	srv := h.proxyServer(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"whatever-the-client-said","stream":true,"messages":[]}`))
	req.Header.Set(identity.HeaderUpstream, backend.URL)
	if req.Header.Get(identity.HeaderServingMode) == "" { // Atlas stamps it on every TF route (ruling #19)
		req.Header.Set(identity.HeaderServingMode, identity.ServingModeDedicated)
	}
	req.Header.Set(identity.HeaderAuthID, testAuthID)
	req.Header.Set(identity.HeaderResourceID, testResourceID)
	req.Header.Set(identity.HeaderResourceType, "deployment")
	// A user XOR a group, never both (DESIGN §2: identity_auth joins org via
	// user_id OR group_id). Setting both is a producer bug the rater now treats
	// as an owner conflict and withholds from money, so the fixture carries the
	// realistic user-owned identity.
	req.Header.Set(identity.HeaderUserID, "user-e2e")
	req.Header.Set(identity.HeaderOrgID, testOrgID)
	// Atlas stamps the base model on every Token Factory endpoint; the manager
	// prices tokens by it, so a record cannot be sent without it.
	req.Header.Set(identity.HeaderBaseModel, testBaseModel)
	req.Header.Set("X-Request-Id", "saturn-e2e-streamed-request")
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("proxy status = %d, want 200 (body: %s)", rr.Code, rr.Body.String())
	}
	clientStream := rr.Body.String()
	var wantClientStream strings.Builder
	for _, event := range strings.SplitAfter(vllmStream, "\n\n") {
		if strings.Contains(event, `"usage"`) {
			// The response filter removes the data line while retaining the
			// event terminator. An empty SSE event has no client-visible data.
			wantClientStream.WriteString("\n")
			continue
		}
		wantClientStream.WriteString(event)
	}
	if clientStream != wantClientStream.String() {
		t.Errorf(
			"client stream differs from the upstream stream after removing unrequested usage:\ngot  %q\nwant %q",
			clientStream,
			wantClientStream.String(),
		)
	}
	clientRequestID := rr.Header().Get("X-Request-Id")
	if !strings.HasPrefix(clientRequestID, "phoebe-") || clientRequestID == "saturn-e2e-streamed-request" {
		t.Fatalf("response X-Request-Id = %q, want fresh Phoebe billing id", clientRequestID)
	}

	// 3. Drainer: the emit is async — wait for the stream entry, then run the
	//    real drain loop until the row is durable in Postgres.
	h.waitForStreamLen(t, 1, 5*time.Second)
	h.drainUntilRows(t, 1, 10*time.Second)

	var (
		nRows                         int
		requestID, authID, resourceID string
		beOrgID                       sql.NullString // billing_event.org_id (nullable) — asserted at the drain layer
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
		`SELECT request_id, auth_id, resource_id, org_id, model, prompt_tokens, cached_tokens, completion_tokens, aborted
		 FROM billing_event`).
		Scan(&requestID, &authID, &resourceID, &beOrgID, &model, &prompt, &cached, &completion, &aborted); err != nil {
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
	// ORG captured at the DRAIN layer (not only transitively via rated_usage): a drainer
	// regression that drops org to NULL is caught HERE, at the layer that produced it.
	if !beOrgID.Valid || beOrgID.String != testOrgID {
		t.Errorf("billing_event.org_id = %v, want %q (captured from X-Saturn-Org-Id at meter time)", beOrgID, testOrgID)
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

	// 4. Aggregator: roll the event's hour into rated_usage.
	res := h.aggregateEventHour(t)
	if res.EventsAggregated != 1 || res.RollupsWritten != 1 || res.HasAnomaly() {
		t.Fatalf("aggregator Result = %+v, want exactly 1 event aggregated into 1 rollup and nothing withheld", res)
	}

	var (
		nRollups                                     int
		ruAuthID, ruResourceID, ruModelID, ruBase    string
		ruOrgID                                      sql.NullString // nullable column; a NULL must surface as a clear assertion, not a scan error
		ruPrompt, ruCached, ruCompletion, ruBillable int64
		eventCount                                   int64 // BIGINT column
	)
	if err := h.db.QueryRow("SELECT COUNT(*) FROM rated_usage").Scan(&nRollups); err != nil {
		t.Fatalf("count rated_usage: %v", err)
	}
	if nRollups != 1 {
		t.Fatalf("rated_usage rows = %d, want exactly 1", nRollups)
	}
	if err := h.db.QueryRow(
		`SELECT auth_id, resource_id, org_id, model_id, prompt_tokens, cached_tokens, completion_tokens, billable_prompt_tokens, base_model, event_count
		 FROM rated_usage`).
		Scan(&ruAuthID, &ruResourceID, &ruOrgID, &ruModelID, &ruPrompt, &ruCached, &ruCompletion, &ruBillable, &ruBase, &eventCount); err != nil {
		t.Fatalf("read rated_usage: %v", err)
	}
	if ruAuthID != testAuthID {
		t.Errorf("rated_usage.auth_id = %q, want %q (the X-Saturn-Auth-Id header value)", ruAuthID, testAuthID)
	}
	// E2 customer attribution: the rollup carries the deployment id the request routed
	// to (X-Saturn-Resource-Id), end-to-end from the proxy through billing_event into
	// the rated_usage grain — this is the key billing resolves the org from.
	if ruResourceID != testResourceID {
		t.Errorf("rated_usage.resource_id = %q, want %q (the X-Saturn-Resource-Id header value, for E2 org attribution)", ruResourceID, testResourceID)
	}
	// E2 org attribution, end-to-end: the deployment-owning org (X-Saturn-Org-Id) is
	// CAPTURED at the proxy and carried through billing_event into the rated_usage rollup
	// — the whole point of this change (org rides the request, not a push-time
	// resource_name join). This is the composed-path assertion for the header→rollup carry.
	if !ruOrgID.Valid || ruOrgID.String != testOrgID {
		t.Errorf("rated_usage.org_id = %v, want %q (the X-Saturn-Org-Id header value, carried meter→rate)", ruOrgID, testOrgID)
	}
	// SERVING MODE, end to end (2026-09-29 ruling): this request models a dedicated
	// route, which carries X-Saturn-Serving-Mode: dedicated (Atlas stamps it
	// explicitly, ruling #19). It must be stored as the explicit 'dedicated' in
	// billing_event AND rated_usage — never NULL or the retired ''.
	var beMode, ruMode sql.NullString
	if err := h.db.QueryRow(`SELECT serving_mode FROM billing_event`).Scan(&beMode); err != nil {
		t.Fatalf("read billing_event.serving_mode: %v", err)
	}
	if err := h.db.QueryRow(`SELECT serving_mode FROM rated_usage`).Scan(&ruMode); err != nil {
		t.Fatalf("read rated_usage.serving_mode: %v", err)
	}
	if !beMode.Valid || beMode.String != "dedicated" || !ruMode.Valid || ruMode.String != "dedicated" {
		t.Errorf("serving_mode billing_event=%v rated_usage=%v, want 'dedicated' in both", beMode, ruMode)
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
	if ruBase != testBaseModel {
		t.Errorf("rated_usage.base_model = %q, want %q (the pricing fact the manager needs)", ruBase, testBaseModel)
	}
	h.assertNoMoney(t)
}

// ftVllmStream is the vLLM SSE fixture for a FINE-TUNE deployment: the engine reports
// the ft:<checkpoint> id as its model name (the stable price key for a fine-tune, E3).
const ftVllmStream = `data: {"id":"c2","object":"chat.completion.chunk","model":"ft:9f8e7d6c5b4a","choices":[{"index":0,"delta":{"role":"assistant","content":"Hi"}}]}

data: {"id":"c2","object":"chat.completion.chunk","model":"ft:9f8e7d6c5b4a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

data: {"id":"c2","object":"chat.completion.chunk","model":"ft:9f8e7d6c5b4a","choices":[],"usage":{"prompt_tokens":1000,"total_tokens":1000,"completion_tokens":0,"prompt_tokens_details":{"cached_tokens":0}}}

data: [DONE]

`

// TestE2E_FineTuneCarriesItsBaseModelToTheRollup is the fine-tune pipeline test: a request to
// a fine-tune deployment carries the X-Saturn-Base-Model header (as Atlas injects it
// at deploy), the engine reports the ft:<checkpoint> id as its model, and the whole
// pipe must end with the base model on the rollup, where the manager prices from it — proving base_model rides
// end to end (proxy → emit → drain → billing_event.base_model → rater derived join).
//
//   - base_model contract: billing_event.base_model is the HF base id from the header,
//     NOT empty — the propagation seam the rater needs to price an ft: id;
//   - the money: the ft: id (absent from the price file) bills at base × 1.5 via its
//     base_model, with zero unpriced events.
func TestE2E_FineTuneCarriesItsBaseModelToTheRollup(t *testing.T) {
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

	srv := h.proxyServer(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"my-finetune","stream":true,"messages":[]}`))
	req.Header.Set(identity.HeaderUpstream, backend.URL)
	if req.Header.Get(identity.HeaderServingMode) == "" { // Atlas stamps it on every TF route (ruling #19)
		req.Header.Set(identity.HeaderServingMode, identity.ServingModeDedicated)
	}
	req.Header.Set(identity.HeaderAuthID, testAuthID)
	req.Header.Set(identity.HeaderResourceID, testResourceID)
	req.Header.Set(identity.HeaderResourceType, "deployment")
	// A user XOR a group, never both (DESIGN §2: identity_auth joins org via
	// user_id OR group_id). Setting both is a producer bug the rater now treats
	// as an owner conflict and withholds from money, so the fixture carries the
	// realistic user-owned identity.
	req.Header.Set(identity.HeaderUserID, "user-e2e")
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

	res := h.aggregateEventHour(t)
	if res.EventsAggregated != 1 || res.RollupsWritten != 1 || res.HasAnomaly() {
		t.Fatalf("aggregator Result = %+v, want 1 event / 1 rollup and nothing withheld", res)
	}
	var ruModelID, ruBase string
	if err := h.db.QueryRow(`SELECT model_id, base_model FROM rated_usage`).Scan(&ruModelID, &ruBase); err != nil {
		t.Fatalf("read rated_usage: %v", err)
	}
	if ruModelID != ftModelName || ruBase != ftBaseModel {
		t.Errorf("rated_usage model/base = %q/%q, want %q/%q", ruModelID, ruBase, ftModelName, ftBaseModel)
	}
	h.assertNoMoney(t)
}

// TestE2E_MissingBaseModelHeaderIsWithheldNotSent is the fail-CLOSED end of the
// fine-tune pipe (the mirror of TestE2E_FineTuneCarriesItsBaseModelToTheRollup). A request to
// a fine-tune deployment that arrives WITHOUT the X-Saturn-Base-Model header — the
// propagation seam broken (Traefik not allowlisting it, Atlas not stamping it) — must
// NOT become a record: the manager prices tokens by base_model and the wire requires
// it, so the whole pipe must end with the event withheld as MISSING-BASE-MODEL (an
// anomaly that screams), never sent with a guessed base.
func TestE2E_MissingBaseModelHeaderIsWithheldNotSent(t *testing.T) {
	h := newHarness(t, "phoebe_e2e_ft_nobase")

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

	srv := h.proxyServer(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"my-finetune","stream":true,"messages":[]}`))
	req.Header.Set(identity.HeaderUpstream, backend.URL)
	if req.Header.Get(identity.HeaderServingMode) == "" { // Atlas stamps it on every TF route (ruling #19)
		req.Header.Set(identity.HeaderServingMode, identity.ServingModeDedicated)
	}
	req.Header.Set(identity.HeaderAuthID, testAuthID)
	req.Header.Set(identity.HeaderResourceID, testResourceID)
	req.Header.Set(identity.HeaderResourceType, "deployment")
	// A user XOR a group, never both (DESIGN §2: identity_auth joins org via
	// user_id OR group_id). Setting both is a producer bug the rater now treats
	// as an owner conflict and withholds from money, so the fixture carries the
	// realistic user-owned identity.
	req.Header.Set(identity.HeaderUserID, "user-e2e")
	// DELIBERATELY no X-Saturn-Base-Model header — the propagation bug under test.
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("proxy status = %d, want 200 (body: %s)", rr.Code, rr.Body.String())
	}

	h.waitForStreamLen(t, 1, 5*time.Second)
	h.drainUntilRows(t, 1, 10*time.Second)

	// base_model must be NULL on the row — the header never rode, so the rater has
	// nothing to derive through.
	var baseModel sql.NullString
	if err := h.db.QueryRow("SELECT base_model FROM billing_event").Scan(&baseModel); err != nil {
		t.Fatalf("read billing_event.base_model: %v", err)
	}
	if baseModel.Valid {
		t.Fatalf("billing_event.base_model = %q, want NULL (no header was sent)", baseModel.String)
	}

	// The event must FAIL LOUD: withheld, not aggregated.
	res := h.aggregateEventHour(t)
	if res.MissingBaseModelEvents != 1 || res.EventsAggregated != 0 || res.RollupsWritten != 0 || !res.HasAnomaly() {
		t.Fatalf("result = %+v, want 1 missing-base-model event withheld, nothing aggregated, and an anomaly", res)
	}
	// And no rated_usage row was written for it.
	var nRollups int
	if err := h.db.QueryRow("SELECT COUNT(*) FROM rated_usage").Scan(&nRollups); err != nil {
		t.Fatalf("count rated_usage: %v", err)
	}
	if nRollups != 0 {
		t.Fatalf("rated_usage rows = %d, want 0 (no record without a base model)", nRollups)
	}
}

// TestE2E_ModellessEventWithoutUsageIsMissingUsage pins the no-response contract
// end to end: an attempt whose upstream never reported either a model or a usage
// block (for example, an abort before the first chunk) reaches Postgres with model
// = NULL and is counted once as MISSING-USAGE. It must be neither UNATTRIBUTABLE
// nor UNPRICED, because those categories describe usage-bearing events whose
// attribution or pricing failed.
func TestE2E_ModellessEventWithoutUsageIsMissingUsage(t *testing.T) {
	h := newHarness(t, "phoebe_e2e_modelless")

	// Emit directly through the REAL emitter, exactly as the proxy does for a
	// An abort with no usage chunk: empty Model, zero counts, Aborted. Phoebe
	// always records the attempt but has no authoritative counts to charge.
	h.emitter.Emit(context.Background(), metering.Event{
		RequestID: "phoebe-e2e-modelless-0001",
		AuthID:    testAuthID,
		UserID:    "user-e2e", // a user XOR a group, never both

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

	res := h.aggregateEventHour(t)

	if res.MissingUsageEvents != 1 {
		t.Errorf("MissingUsageEvents = %d, want 1 (the no-usage attempt)", res.MissingUsageEvents)
	}
	// Cause partition: this attempt was ABORTED, so it is routine — reported for
	// reconciliation, never paged (ratified with Hugo 2026-09-21). Paging on the
	// cause-blind total fired hourly on any install with traffic.
	if res.ExpectedMissingUsageEvents != 1 {
		t.Errorf("ExpectedMissingUsageEvents = %d, want 1 (an abort is a routine zero-usage cause)", res.ExpectedMissingUsageEvents)
	}
	if res.UnexplainedMissingUsageEvents != 0 {
		t.Errorf("UnexplainedMissingUsageEvents = %d, want 0 (the attempt aborted; it did not report success)", res.UnexplainedMissingUsageEvents)
	}
	if res.UnattributableEvents != 0 {
		t.Errorf("UnattributableEvents = %d, want 0 — a model-less attempt with no usage is MISSING-USAGE; that bucket is exclusive and more specific than UNATTRIBUTABLE", res.UnattributableEvents)
	}
	if res.MissingBaseModelEvents != 0 {
		t.Errorf("MissingBaseModelEvents = %d, want 0 — a model-less attempt with no usage is MISSING-USAGE (wrong runbook otherwise)", res.MissingBaseModelEvents)
	}
	if res.EventsAggregated != 0 || res.RollupsWritten != 0 {
		t.Errorf("aggregator recorded a model-less event: %+v", res)
	}
	// An aborted attempt is correctly billed zero and is NOT a pageable anomaly:
	// exit 2 is reserved for rare, wrong conditions. The attempt is still fully
	// recorded (asserted above) and surfaces in billing_reconciliation_hourly.
	if res.HasAnomaly() {
		t.Error("Result.HasAnomaly() = true for a routine client abort — exit 2 must stay reserved for rare, wrong conditions")
	}
}

// TestE2E_SuccessWithoutUsagePages is the other half of the missing-usage cause
// partition: an attempt the engine reported as SUCCESSFUL while supplying no usage
// block means work may have been served that cannot be billed. That is the
// unexplained case and it MUST drive the exit-nonzero path, end to end through the
// emitter, stream, drainer and rater.
func TestE2E_SuccessWithoutUsagePages(t *testing.T) {
	h := newHarness(t, "phoebe_e2e_success_no_usage")

	h.emitter.Emit(context.Background(), metering.Event{
		RequestID: "phoebe-e2e-success-no-usage-0001",
		AuthID:    testAuthID,
		UserID:    "user-e2e", // a user XOR a group, never both

		ResourceID:   testResourceID,
		ResourceType: "deployment",
		Model:        testModelName,
		// Not aborted, HTTP 200, yet the engine reported no usage block.
		Aborted:    false,
		StatusCode: 200,
		UsageFound: false,
	})

	h.waitForStreamLen(t, 1, 5*time.Second)
	h.drainUntilRows(t, 1, 10*time.Second)

	res := h.aggregateEventHour(t)

	if res.MissingUsageEvents != 1 {
		t.Errorf("MissingUsageEvents = %d, want 1", res.MissingUsageEvents)
	}
	if res.UnexplainedMissingUsageEvents != 1 {
		t.Errorf("UnexplainedMissingUsageEvents = %d, want 1 (success with no usage block)", res.UnexplainedMissingUsageEvents)
	}
	if res.ExpectedMissingUsageEvents != 0 {
		t.Errorf("ExpectedMissingUsageEvents = %d, want 0 (neither aborted nor failed)", res.ExpectedMissingUsageEvents)
	}
	if res.EventsAggregated != 0 || res.RollupsWritten != 0 {
		t.Errorf("aggregator recorded an attempt with no authoritative usage: %+v", res)
	}
	if !res.HasAnomaly() {
		t.Error("Result.HasAnomaly() = false — a success with no usage block must drive the exit-nonzero path")
	}
}

// TestE2E_InvalidEngineEvidenceIsRetainedButNeverRated proves an authoritative
// malformed usage block survives emitter -> stream -> drainer -> Postgres. It
// must be a loud reconciliation anomaly, never a poison drop and never money.
func TestE2E_InvalidEngineEvidenceIsRetainedButNeverRated(t *testing.T) {
	h := newHarness(t, "phoebe_e2e_invalid_usage")
	h.emitter.Emit(context.Background(), metering.Event{
		RequestID:        "phoebe-e2e-invalid-0001",
		AuthID:           testAuthID,
		ResourceID:       testResourceID,
		ResourceType:     "deployment",
		OrgID:            testOrgID,
		Model:            testModelName,
		PromptTokens:     10,
		CachedTokens:     20, // impossible: cached input cannot exceed prompt input
		CompletionTokens: 1,
		UsageFound:       true,
		StatusCode:       http.StatusOK,
	})

	h.waitForStreamLen(t, 1, 5*time.Second)
	h.drainUntilRows(t, 1, 10*time.Second) // also asserts drainer Poisoned() == 0

	res := h.aggregateEventHour(t)
	if res.InvalidUsageEvents != 1 || res.EventsAggregated != 0 || res.RollupsWritten != 0 {
		t.Fatalf("invalid evidence result = invalid/aggregated/rollups %d/%d/%d, want 1/0/0",
			res.InvalidUsageEvents, res.EventsAggregated, res.RollupsWritten)
	}
	if !res.HasAnomaly() {
		t.Fatal("invalid engine evidence must drive the fail-loud anomaly path")
	}

	var rawRows, invalidAttempts, ratedRows int64
	if err := h.db.QueryRow("SELECT COUNT(*) FROM billing_event WHERE request_id='phoebe-e2e-invalid-0001'").Scan(&rawRows); err != nil {
		t.Fatalf("count raw invalid evidence: %v", err)
	}
	if err := h.db.QueryRow("SELECT COALESCE(SUM(invalid_usage_attempts),0) FROM billing_reconciliation_hourly").Scan(&invalidAttempts); err != nil {
		t.Fatalf("read invalid reconciliation evidence: %v", err)
	}
	if err := h.db.QueryRow("SELECT COUNT(*) FROM rated_usage").Scan(&ratedRows); err != nil {
		t.Fatalf("count rated rows: %v", err)
	}
	if rawRows != 1 || invalidAttempts != 1 || ratedRows != 0 {
		t.Fatalf("invalid evidence persistence = raw/invalid/rated %d/%d/%d, want 1/1/0",
			rawRows, invalidAttempts, ratedRows)
	}
}

// epVllmStream is the vLLM SSE fixture for a C4 Token Factory deployment: the engine
// reports the ENDPOINT NAME as its model (the ratified serving contract — vLLM serves
// under the endpoint name, so the model is NOT a price-file key and NOT ft:-prefixed).
const epVllmStream = `data: {"id":"c3","object":"chat.completion.chunk","model":"tf-ep-my-finetune","choices":[{"index":0,"delta":{"role":"assistant","content":"Hi"}}]}

data: {"id":"c3","object":"chat.completion.chunk","model":"tf-ep-my-finetune","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

data: {"id":"c3","object":"chat.completion.chunk","model":"tf-ep-my-finetune","choices":[],"usage":{"prompt_tokens":1000,"total_tokens":1000,"completion_tokens":0,"prompt_tokens_details":{"cached_tokens":0}}}

data: [DONE]

`

// TestE2E_AdapterHeaderRidesToTheRollup is the C4 fine-tune
// pipeline test: a request to a fine-tune checkpoint endpoint carries BOTH
// per-deployment headers the Atlas-rendered Traefik middleware injects —
// X-Saturn-Base-Model (the catalog price key) and X-Saturn-Adapter (the checkpoint
// artifact id, whose PRESENCE is the premium trigger) — while the engine reports the
// ENDPOINT NAME (not an ft: id) as its model. The whole pipe must end with:
//
//   - adapter contract: billing_event.adapter is the checkpoint artifact id from the
//     header, stored VERBATIM (forensic: which checkpoint served the request);
//   - the rollup: the endpoint-name model carries both the base model and the
//     adapter, the facts the manager prices a fine-tune by (base x premium).
func TestE2E_AdapterHeaderRidesToTheRollup(t *testing.T) {
	h := newHarness(t, "phoebe_e2e_adapter")

	const (
		epModelName = "tf-ep-my-finetune"
		epBaseModel = "meta-llama/Llama-3.1-8B-Instruct"
		epAdapter   = "ckpt-9f8e7d6c5b4a"
	)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		for _, chunk := range strings.SplitAfter(epVllmStream, "\n\n") {
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

	srv := h.proxyServer(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"my-finetune","stream":true,"messages":[]}`))
	// The forward target rides on the request (X-Saturn-Upstream, as Atlas
	// injects per deployment); phoebe has no resolver.
	req.Header.Set(identity.HeaderUpstream, backend.URL)
	if req.Header.Get(identity.HeaderServingMode) == "" { // Atlas stamps it on every TF route (ruling #19)
		req.Header.Set(identity.HeaderServingMode, identity.ServingModeDedicated)
	}
	req.Header.Set(identity.HeaderAuthID, testAuthID)
	req.Header.Set(identity.HeaderResourceID, testResourceID)
	req.Header.Set(identity.HeaderResourceType, "deployment")
	// A user XOR a group, never both (DESIGN §2: identity_auth joins org via
	// user_id OR group_id). Setting both is a producer bug the rater now treats
	// as an owner conflict and withholds from money, so the fixture carries the
	// realistic user-owned identity.
	req.Header.Set(identity.HeaderUserID, "user-e2e")
	// The two per-deployment headers the Atlas middleware injects on a fine-tune
	// checkpoint endpoint (C4).
	req.Header.Set(identity.HeaderBaseModel, epBaseModel)
	req.Header.Set(identity.HeaderAdapter, epAdapter)
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("proxy status = %d, want 200 (body: %s)", rr.Code, rr.Body.String())
	}

	h.waitForStreamLen(t, 1, 5*time.Second)
	h.drainUntilRows(t, 1, 10*time.Second)

	// The adapter + base_model contracts: both header values ride to billing_event
	// verbatim, alongside the endpoint-name model.
	var model, baseModel, adapter sql.NullString
	if err := h.db.QueryRow("SELECT model, base_model, adapter FROM billing_event").Scan(&model, &baseModel, &adapter); err != nil {
		t.Fatalf("read billing_event: %v", err)
	}
	if !model.Valid || model.String != epModelName {
		t.Errorf("billing_event.model = %v, want %q (the endpoint name the engine serves under)", model, epModelName)
	}
	if !baseModel.Valid || baseModel.String != epBaseModel {
		t.Fatalf("billing_event.base_model = %v, want %q (the catalog price key must ride to billing_event)", baseModel, epBaseModel)
	}
	if !adapter.Valid || adapter.String != epAdapter {
		t.Fatalf("billing_event.adapter = %v, want %q — X-Saturn-Adapter must land in billing_event.adapter (the premium trigger + checkpoint forensics)", adapter, epAdapter)
	}

	res := h.aggregateEventHour(t)
	if res.EventsAggregated != 1 || res.RollupsWritten != 1 || res.HasAnomaly() {
		t.Fatalf("aggregator Result = %+v, want 1 event / 1 rollup and nothing withheld", res)
	}
	// The adapter is the manager's fine-tune premium trigger: it must reach the
	// rollup next to the base model.
	var ruModelID, ruBase, ruAdapter string
	if err := h.db.QueryRow(`SELECT model_id, base_model, adapter FROM rated_usage`).Scan(&ruModelID, &ruBase, &ruAdapter); err != nil {
		t.Fatalf("read rated_usage: %v", err)
	}
	if ruModelID != epModelName || ruBase != epBaseModel || ruAdapter != epAdapter {
		t.Errorf("rated_usage model/base/adapter = %q/%q/%q, want %q/%q/%q", ruModelID, ruBase, ruAdapter, epModelName, epBaseModel, epAdapter)
	}
	h.assertNoMoney(t)
}
