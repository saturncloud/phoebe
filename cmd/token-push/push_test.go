package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/meteredusage"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/token-request.json from the current code")

func quietLog() *logging.Logger { return logging.New(logging.ERROR) }

// hourCols is the column set hourQuery returns, in order.
var hourCols = []string{
	"id", "org_id", "owner_type", "owner_id", "resource_id", "auth_id", "model_id", "serving_mode",
	"base_model", "adapter", "graph_k8s_name", "topology_revision", "region",
	"prompt_tokens", "cached_tokens", "completion_tokens", "event_count",
}

var win = time.Date(2026, 10, 22, 14, 0, 0, 0, time.UTC)

const (
	orgA     = "0a1b2c3d4e5f60718293a4b5c6d7e8f9"
	userA    = "1f2e3d4c5b6a79880716a5b4c3d2e1f0"
	groupA   = "2c3d4e5f60718293a4b5c6d7e8f90a1b"
	deployA  = "3d4e5f60718293a4b5c6d7e8f90a1b2c"
	tfModelB = "4e5f60718293a4b5c6d7e8f90a1b2c3d"
	deployC  = "718293a4b5c6d7e8f90a1b2c3d4e5f60"
	authA    = "5f60718293a4b5c6d7e8f90a1b2c3d4e"
	authB    = "60718293a4b5c6d7e8f90a1b2c3d4e5f"
)

// The fixture hour: three rollups covering the shapes the wire must carry.
//   - a dedicated fine-tune endpoint owned by a user, served by graph revision 3;
//   - the same endpoint in revision 4 (a second record: revision is grain);
//   - a shared base-model call by a group-owned key, no graph named, revision
//     unknown, region unknown (NULLs become "");
//   - a dedicated endpoint serving a model outside the catalog: no base model
//     (sent as ""; the manager prices it at the plan's default dedicated rate).
func fixtureRows() *sqlmock.Rows {
	return sqlmock.NewRows(hourCols).
		AddRow("rid-1", orgA, "user", userA, deployA, authA, "acme-qwen-ft", "dedicated",
			"Qwen/Qwen2.5-32B-Instruct", "ckpt-7a1c", "pd-acme-qwen-ft", int64(3), "us-central1",
			int64(120000), int64(80000), int64(9000), int64(42)).
		AddRow("rid-2", orgA, "user", userA, deployA, authA, "acme-qwen-ft", "dedicated",
			"Qwen/Qwen2.5-32B-Instruct", "ckpt-7a1c", "pd-acme-qwen-ft", int64(4), "us-central1",
			int64(3000), int64(0), int64(500), int64(2)).
		AddRow("rid-3", orgA, "group", groupA, tfModelB, authB, "openai/gpt-oss-120b", "shared",
			"openai/gpt-oss-120b", nil, "", int64(0), nil,
			int64(5000), int64(1000), int64(700), int64(9)).
		AddRow("rid-4", orgA, "user", userA, deployC, authA, "acme-custom-llm", "dedicated",
			nil, nil, "pd-acme-custom-llm", int64(1), "us-central1",
			int64(800), int64(0), int64(64), int64(4))
}

func newMockPusher(t *testing.T, client poster) (*pusher, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &pusher{db: db, log: quietLog(), client: client}, mock
}

func expectLiveness(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT 1 FROM rated_usage LIMIT 1`).WillReturnRows(sqlmock.NewRows([]string{"x"}).AddRow(1))
}

// recordingManager is a stub manager: it answers with the given statuses in
// order (the last repeats), records every body, and answers 200 with the
// contract's body.
type recordingManager struct {
	mu       sync.Mutex
	statuses []int
	bodies   [][]byte
	auth     []string
	paths    []string
}

func (m *recordingManager) server(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.bodies = append(m.bodies, body)
		m.auth = append(m.auth, r.Header.Get("Authorization"))
		m.paths = append(m.paths, r.URL.Path)
		n := len(m.bodies)
		status := http.StatusOK
		if len(m.statuses) > 0 {
			status = m.statuses[len(m.statuses)-1]
			if n <= len(m.statuses) {
				status = m.statuses[n-1]
			}
		}
		m.mu.Unlock()
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"stub","path":"records[0]"}`))
			return
		}
		var req meteredusage.Request
		_ = json.Unmarshal(body, &req)
		_ = json.NewEncoder(w).Encode(meteredusage.Response{Accepted: int64(len(req.Records)), WindowStart: req.WindowStart, Kinds: req.Kinds})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (m *recordingManager) client(t *testing.T) *meteredusage.Client {
	return &meteredusage.Client{
		BaseURL: m.server(t).URL + "/", Token: "tok-123", HTTP: &http.Client{Timeout: 5 * time.Second},
		Sleep: func(context.Context, time.Duration) error { return nil },
	}
}

func TestHourQuery_ReadsQuantitiesAndNoMoney(t *testing.T) {
	for _, money := range []string{"cost", "applied_", "price_key", "resource_name"} {
		if strings.Contains(hourQuery, money) {
			t.Fatalf("hourQuery reads %q; the manager prices, phoebe sends quantities only:\n%s", money, hourQuery)
		}
	}
}

func TestBuildRequest_MapsEachRollupToATokenRecord(t *testing.T) {
	p, mock := newMockPusher(t, nil)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(fixtureRows())

	req, unsendable, err := p.buildRequest(context.Background(), win)
	if err != nil || unsendable != 0 {
		t.Fatalf("buildRequest: unsendable=%d err=%v", unsendable, err)
	}
	if req.WindowStart != "2026-10-22T14:00:00+00:00" || len(req.Kinds) != 1 || req.Kinds[0] != "token" || len(req.Records) != 4 {
		t.Fatalf("request header %q %v with %d records", req.WindowStart, req.Kinds, len(req.Records))
	}
	shared := req.Records[2]
	wantAttrs := meteredusage.TokenAttributes{
		AuthID: authB, ModelID: "openai/gpt-oss-120b", ServingMode: "shared", BaseModel: "openai/gpt-oss-120b",
		Adapter: "", GraphK8sName: "", TopologyRevision: "", Region: "",
	}
	if shared.Attributes != wantAttrs {
		t.Fatalf("shared attributes %+v, want %+v (NULL adapter/region and revision 0 are sent as \"\")", shared.Attributes, wantAttrs)
	}
	if shared.Quantities != (meteredusage.TokenQuantities{PromptTokens: 5000, CachedTokens: 1000, CompletionTokens: 700, RequestCount: 9}) {
		t.Fatalf("quantities %+v", shared.Quantities)
	}
	if req.Records[0].Attributes.TopologyRevision != "3" || req.Records[1].Attributes.TopologyRevision != "4" {
		t.Fatal("topology revisions not carried as decimal strings")
	}
	for i, r := range req.Records {
		want := meteredusage.TokenUsageID(req.WindowStart, r.OrgID, r.OwnerType, r.OwnerID, r.ResourceID, r.Attributes)
		if r.UsageID != want {
			t.Fatalf("records[%d].usage_id %s, want %s", i, r.UsageID, want)
		}
	}
	if req.Records[0].UsageID == req.Records[1].UsageID {
		t.Fatal("two topology revisions minted the same usage_id")
	}
	if err := meteredusage.Validate(req); err != nil {
		t.Fatalf("built request breaks a wire rule: %v", err)
	}
}

// The golden fixture: the full body for the fixture hour, byte for byte. A
// copy lives in saturn-architecture-docs
// (agent-wip/evidence/billing-unification/fixtures/token-request.json) so the
// manager's ingest can be tested against exactly what phoebe sends.
// Regenerate with: go test ./cmd/token-push -run TestGoldenRequest -update
func TestGoldenRequest_FullHourBody(t *testing.T) {
	p, mock := newMockPusher(t, nil)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(fixtureRows())
	req, _, err := p.buildRequest(context.Background(), win)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(req) // exactly what meteredusage.Client sends
	if err != nil {
		t.Fatal(err)
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, wire, "", "  "); err != nil {
		t.Fatal(err)
	}
	pretty.WriteByte('\n')

	path := filepath.Join("testdata", "token-request.json")
	if *updateGolden {
		if err := os.WriteFile(path, pretty.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	golden, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (run with -update to create it): %v", err)
	}
	if !bytes.Equal(golden, pretty.Bytes()) {
		t.Fatalf("request body drifted from %s:\n%s", path, pretty.String())
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, golden); err != nil || !bytes.Equal(compact.Bytes(), wire) {
		t.Fatalf("golden compacts to %s, wire is %s (err %v)", compact.Bytes(), wire, err)
	}
}

func TestBuildRequest_EmptyHourIsAnEmptyRecordList(t *testing.T) {
	p, mock := newMockPusher(t, nil)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(sqlmock.NewRows(hourCols))
	req, _, err := p.buildRequest(context.Background(), win)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(req)
	if !strings.Contains(string(b), `"records":[]`) || !strings.Contains(string(b), `"held":[]`) {
		t.Fatalf("empty hour marshals to %s; want records [] (delete-by-absence), never null", b)
	}
}

// An empty hour is still SENT: that is how the manager learns usage it holds for
// the hour is gone (for example, a rollup that became withheld after a re-run).
func TestPushWindows_SendsAnEmptyHourSoTheManagerDeletesByAbsence(t *testing.T) {
	m := &recordingManager{}
	p, mock := newMockPusher(t, m.client(t))
	expectLiveness(mock)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(sqlmock.NewRows(hourCols))
	if code := p.pushWindows(context.Background(), []time.Time{win}); code != exitOK {
		t.Fatalf("exit %d, want 0", code)
	}
	if len(m.bodies) != 1 || !strings.Contains(string(m.bodies[0]), `"records":[]`) {
		t.Fatalf("bodies %q, want one empty-hour request", m.bodies)
	}
	if m.auth[0] != "token tok-123" || m.paths[0] != meteredusage.Path {
		t.Fatalf("auth %q path %q, want the install's customer token on %s", m.auth[0], m.paths[0], meteredusage.Path)
	}
}

// Re-sending an hour sends identical bytes (same usage_ids), which is what makes
// the manager replace rather than duplicate.
func TestPushWindows_ResendingAnHourIsByteIdentical(t *testing.T) {
	m := &recordingManager{}
	p, mock := newMockPusher(t, m.client(t))
	for i := 0; i < 2; i++ {
		expectLiveness(mock)
		mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(fixtureRows())
		if code := p.pushWindows(context.Background(), []time.Time{win}); code != exitOK {
			t.Fatalf("run %d: exit %d", i, code)
		}
	}
	if len(m.bodies) != 2 || !bytes.Equal(m.bodies[0], m.bodies[1]) {
		t.Fatalf("re-sent hour differs:\n%s\n%s", m.bodies[0], m.bodies[1])
	}
}

func TestPushWindows_WithholdsAnHourWithARollupWithoutOrgButSendsTheOthers(t *testing.T) {
	m := &recordingManager{}
	p, mock := newMockPusher(t, m.client(t))
	expectLiveness(mock)
	bad := sqlmock.NewRows(hourCols).
		AddRow("rid-1", orgA, "user", userA, deployA, authA, "m", "dedicated", "B", nil, "", int64(0), nil, int64(1), int64(0), int64(1), int64(1)).
		AddRow("rid-2", nil, "user", userA, deployA, authB, "m", "dedicated", "B", nil, "", int64(0), nil, int64(1), int64(0), int64(1), int64(1))
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(bad)
	next := win.Add(time.Hour)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(next).WillReturnRows(fixtureRows())

	if code := p.pushWindows(context.Background(), []time.Time{win, next}); code != exitWithheld {
		t.Fatalf("exit %d, want exitWithheld (%d)", code, exitWithheld)
	}
	if len(m.bodies) != 1 || !strings.Contains(string(m.bodies[0]), "2026-10-22T15:00:00+00:00") {
		t.Fatalf("sent %d bodies, want only the clean 15:00 hour", len(m.bodies))
	}
}

// A rollup that would break a wire rule (here an owner_id that is not 32 hex)
// withholds the hour instead of earning a 400.
func TestPushWindows_WithholdsAnHourThatBreaksAWireRule(t *testing.T) {
	m := &recordingManager{}
	p, mock := newMockPusher(t, m.client(t))
	expectLiveness(mock)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(sqlmock.NewRows(hourCols).
		AddRow("rid-1", orgA, "user", "not-hex", deployA, authA, "m", "dedicated", "B", nil, "", int64(0), nil, int64(1), int64(0), int64(1), int64(1)))
	if code := p.pushWindows(context.Background(), []time.Time{win}); code != exitWithheld {
		t.Fatalf("exit %d, want exitWithheld", code)
	}
	if len(m.bodies) != 0 {
		t.Fatalf("sent %d bodies, want none", len(m.bodies))
	}
}

func TestPushWindows_NeverRetries400_409_422AndExits3(t *testing.T) {
	for _, status := range []int{400, 409, 422} {
		m := &recordingManager{statuses: []int{status, 200}}
		p, mock := newMockPusher(t, m.client(t))
		expectLiveness(mock)
		mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(fixtureRows())
		if code := p.pushWindows(context.Background(), []time.Time{win}); code != exitRejected {
			t.Fatalf("status %d: exit %d, want exitRejected (%d)", status, code, exitRejected)
		}
		if len(m.bodies) != 1 {
			t.Fatalf("status %d: %d attempts, want exactly 1", status, len(m.bodies))
		}
	}
}

func TestPushWindows_Retries5xxUntilTheManagerAccepts(t *testing.T) {
	m := &recordingManager{statuses: []int{503, 502, 200}}
	p, mock := newMockPusher(t, m.client(t))
	expectLiveness(mock)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(fixtureRows())
	if code := p.pushWindows(context.Background(), []time.Time{win}); code != exitOK {
		t.Fatalf("exit %d, want 0 after retries", code)
	}
	if len(m.bodies) != 3 || !bytes.Equal(m.bodies[0], m.bodies[2]) {
		t.Fatalf("%d attempts, want 3 identical ones", len(m.bodies))
	}
}

func TestPushWindows_Persistent5xxIsFatalAndDominates(t *testing.T) {
	m := &recordingManager{statuses: []int{503}}
	p, mock := newMockPusher(t, m.client(t))
	expectLiveness(mock)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(fixtureRows())
	// A second hour that is withheld: fatal still wins.
	next := win.Add(time.Hour)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(next).WillReturnRows(sqlmock.NewRows(hourCols).
		AddRow("rid-9", nil, "", "", deployA, authA, "m", "dedicated", "B", nil, "", int64(0), nil, int64(1), int64(0), int64(1), int64(1)))
	if code := p.pushWindows(context.Background(), []time.Time{win, next}); code != exitFatal {
		t.Fatalf("exit %d, want exitFatal", code)
	}
	if len(m.bodies) != meteredusage.DefaultMaxAttempts {
		t.Fatalf("%d attempts, want %d", len(m.bodies), meteredusage.DefaultMaxAttempts)
	}
}

func TestPushWindows_RejectedOutranksWithheld(t *testing.T) {
	m := &recordingManager{statuses: []int{422}}
	p, mock := newMockPusher(t, m.client(t))
	expectLiveness(mock)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(fixtureRows())
	next := win.Add(time.Hour)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(next).WillReturnRows(sqlmock.NewRows(hourCols).
		AddRow("rid-9", nil, "", "", deployA, authA, "m", "dedicated", "B", nil, "", int64(0), nil, int64(1), int64(0), int64(1), int64(1)))
	if code := p.pushWindows(context.Background(), []time.Time{win, next}); code != exitRejected {
		t.Fatalf("exit %d, want exitRejected", code)
	}
}

// An entirely empty table would send every hour as empty and delete the
// manager's records; refuse instead.
func TestPushWindows_EmptyTableRefusesToSendAnything(t *testing.T) {
	m := &recordingManager{}
	p, mock := newMockPusher(t, m.client(t))
	mock.ExpectQuery(`SELECT 1 FROM rated_usage LIMIT 1`).WillReturnRows(sqlmock.NewRows([]string{"x"}))
	if code := p.pushWindows(context.Background(), []time.Time{win}); code != exitFatal {
		t.Fatalf("exit %d, want exitFatal", code)
	}
	if len(m.bodies) != 0 {
		t.Fatal("sent a request from an empty table")
	}
}

// D3 and the corrected conventions: a dedicated endpoint with no catalog base
// is sent with base_model "" in the same request as the rest of the hour. It
// neither withholds itself nor holds back anyone else's records.
func TestPushWindows_EmptyBaseModelDedicatedRecordIsSentAndDoesNotBlockTheHour(t *testing.T) {
	m := &recordingManager{}
	p, mock := newMockPusher(t, m.client(t))
	expectLiveness(mock)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(fixtureRows())
	if code := p.pushWindows(context.Background(), []time.Time{win}); code != exitOK {
		t.Fatalf("exit %d, want 0", code)
	}
	var sent meteredusage.Request
	if err := json.Unmarshal(m.bodies[0], &sent); err != nil {
		t.Fatal(err)
	}
	if len(sent.Records) != 4 {
		t.Fatalf("sent %d records, want all 4", len(sent.Records))
	}
	custom := sent.Records[3]
	if custom.Attributes.ModelID != "acme-custom-llm" || custom.Attributes.BaseModel != "" || custom.Attributes.ServingMode != "dedicated" {
		t.Fatalf("custom record %+v, want dedicated with base_model \"\"", custom.Attributes)
	}
}

func TestPushWindows_Retries429UntilTheManagerAccepts(t *testing.T) {
	m := &recordingManager{statuses: []int{429, 200}}
	p, mock := newMockPusher(t, m.client(t))
	expectLiveness(mock)
	mock.ExpectQuery(`FROM rated_usage ru`).WithArgs(win).WillReturnRows(fixtureRows())
	if code := p.pushWindows(context.Background(), []time.Time{win}); code != exitOK {
		t.Fatalf("exit %d, want 0 after a 429 retry", code)
	}
	if len(m.bodies) != 2 {
		t.Fatalf("%d attempts, want 2", len(m.bodies))
	}
}
