package main

import (
	"bytes"
	"context"
	"database/sql"
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

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/meteredusage"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/token-request.json from the current code")

func quietLog() *logging.Logger { return logging.New(logging.ERROR) }

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

func ns(v string) sql.NullString { return sql.NullString{String: v, Valid: v != ""} }

func rollup(id string, idn identity, org, base, adapter, graph string, rev int64, region string, p, c, comp, n int64) rollupRow {
	return rollupRow{ID: id, identity: idn, OrgID: ns(org), BaseModel: ns(base), Adapter: ns(adapter), Region: ns(region),
		GraphK8sName: graph, TopologyRevision: rev, Prompt: p, Cached: c, Completion: comp, Requests: n}
}

var (
	idFT     = identity{authA, "user", userA, deployA, "acme-qwen-ft", "dedicated"}
	idShared = identity{authB, "group", groupA, tfModelB, "openai/gpt-oss-120b", "shared"}
	idCustom = identity{authA, "user", userA, deployC, "acme-custom-llm", "dedicated"}
)

// The fixture hour: four rollups covering the shapes the wire must carry.
//   - a dedicated fine-tune endpoint owned by a user, served by graph revision 3;
//   - the same endpoint in revision 4 (a second record: revision is grain);
//   - a shared base-model call by a group-owned key, no graph named, revision
//     unknown, region unknown (NULLs become "");
//   - a dedicated endpoint serving a model outside the catalog: no base model
//     (sent as ""; the manager prices it at the plan's default dedicated rate).
func fixtureRollups() []rollupRow {
	return []rollupRow{
		rollup("rid-1", idFT, orgA, "Qwen/Qwen2.5-32B-Instruct", "ckpt-7a1c", "pd-acme-qwen-ft", 3, "us-central1", 120000, 80000, 9000, 42),
		rollup("rid-2", idFT, orgA, "Qwen/Qwen2.5-32B-Instruct", "ckpt-7a1c", "pd-acme-qwen-ft", 4, "us-central1", 3000, 0, 500, 2),
		rollup("rid-3", idShared, orgA, "openai/gpt-oss-120b", "", "", 0, "", 5000, 1000, 700, 9),
		rollup("rid-4", idCustom, orgA, "", "", "pd-acme-custom-llm", 1, "us-central1", 800, 0, 64, 4),
	}
}

// fakeStore is an in-memory pushStore.
type fakeStore struct {
	empty    bool
	rollups  map[time.Time][]rollupRow
	parked   map[time.Time][]identity
	pushed   map[time.Time][]pushedRow
	resolved []time.Time
	accepted map[time.Time]int
}

func newFake() *fakeStore {
	return &fakeStore{rollups: map[time.Time][]rollupRow{}, parked: map[time.Time][]identity{},
		pushed: map[time.Time][]pushedRow{}, accepted: map[time.Time]int{}}
}

func (f *fakeStore) HasAnyUsage(context.Context) (bool, error) { return !f.empty, nil }
func (f *fakeStore) Rollups(_ context.Context, h time.Time) ([]rollupRow, error) {
	return f.rollups[h], nil
}
func (f *fakeStore) ParkedIdentities(_ context.Context, h time.Time) ([]identity, error) {
	return f.parked[h], nil
}
func (f *fakeStore) Pushed(_ context.Context, h time.Time) ([]pushedRow, error) {
	return f.pushed[h], nil
}
func (f *fakeStore) ResolvedHours(_ context.Context, before time.Time) ([]time.Time, error) {
	var out []time.Time
	for _, h := range f.resolved {
		if h.Before(before) {
			out = append(out, h)
		}
	}
	return out, nil
}
func (f *fakeStore) RecordIntent(_ context.Context, h time.Time, rows []pushedRow) error {
	seen := map[string]bool{}
	for _, r := range f.pushed[h] {
		seen[r.UsageID] = true
	}
	for _, r := range rows {
		if !seen[r.UsageID] {
			f.pushed[h] = append(f.pushed[h], r)
		}
	}
	return nil
}
func (f *fakeStore) RecordAccepted(_ context.Context, h time.Time, rows []pushedRow) error {
	f.pushed[h] = append([]pushedRow(nil), rows...)
	f.accepted[h]++
	return nil
}

func newPusher(store pushStore, client poster) *pusher {
	return &pusher{store: store, log: quietLog(), client: client}
}

// now is a fixed run instant after every fixture hour.
var now = win.Add(3 * time.Hour)

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

func fixtureStore() *fakeStore {
	f := newFake()
	f.rollups[win] = fixtureRollups()
	return f
}

func TestBuildHour_MapsEachRollupToATokenRecord(t *testing.T) {
	plan, err := newPusher(fixtureStore(), nil).buildHour(context.Background(), win)
	if err != nil || plan.heldBack != 0 {
		t.Fatalf("buildHour: heldBack=%d err=%v", plan.heldBack, err)
	}
	req := plan.req
	if req.WindowStart != "2026-10-22T14:00:00+00:00" || len(req.Kinds) != 1 || req.Kinds[0] != "token" || len(req.Records) != 4 {
		t.Fatalf("request header %q %v with %d records", req.WindowStart, req.Kinds, len(req.Records))
	}
	shared := req.Records[2]
	wantAttrs := meteredusage.TokenAttributes{
		AuthID: authB, ModelID: "openai/gpt-oss-120b", ServingMode: "shared", BaseModel: "openai/gpt-oss-120b",
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
		if want := meteredusage.TokenUsageID(req.WindowStart, r.OrgID, r.OwnerType, r.OwnerID, r.ResourceID, r.Attributes); r.UsageID != want {
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
	plan, err := newPusher(fixtureStore(), nil).buildHour(context.Background(), win)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(plan.req) // exactly what meteredusage.Client sends
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

func TestBuildHour_EmptyHourIsAnEmptyRecordList(t *testing.T) {
	plan, err := newPusher(newFake(), nil).buildHour(context.Background(), win)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(plan.req)
	if !strings.Contains(string(b), `"records":[]`) || !strings.Contains(string(b), `"held":[]`) {
		t.Fatalf("empty hour marshals to %s; want records [] (delete-by-absence), never null", b)
	}
}

// An empty hour is still SENT: that is how the manager learns usage it holds for
// the hour is gone.
func TestPushWindows_SendsAnEmptyHourSoTheManagerDeletesByAbsence(t *testing.T) {
	m := &recordingManager{}
	if code := newPusher(newFake(), m.client(t)).pushWindows(context.Background(), []time.Time{win}, now); code != exitOK {
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
	p := newPusher(fixtureStore(), m.client(t))
	for i := 0; i < 2; i++ {
		if code := p.pushWindows(context.Background(), []time.Time{win}, now); code != exitOK {
			t.Fatalf("run %d: exit %d", i, code)
		}
	}
	if len(m.bodies) != 2 || !bytes.Equal(m.bodies[0], m.bodies[1]) {
		t.Fatalf("re-sent hour differs:\n%s\n%s", m.bodies[0], m.bodies[1])
	}
}

func sentRequest(t *testing.T, body []byte) meteredusage.Request {
	t.Helper()
	var r meteredusage.Request
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

// Coordination 2026-10-27: a conflict holds back only the conflicted identity.
// Here the rater has parked idShared for the hour (its usage is not in
// rated_usage); every other record of the hour is still sent.
func TestPushWindows_AConflictInOneIdentityStillSendsEveryCleanRecordOfTheHour(t *testing.T) {
	f := newFake()
	all := fixtureRollups()
	f.rollups[win] = []rollupRow{all[0], all[1], all[3]} // idShared parked by the rater
	f.parked[win] = []identity{idShared}
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now); code != exitOK {
		t.Fatalf("exit %d, want 0 (parked usage is the rater's signal)", code)
	}
	req := sentRequest(t, m.bodies[0])
	if len(req.Records) != 3 || len(req.Held) != 0 {
		t.Fatalf("sent %d records and held %v, want the 3 clean records and nothing held (idShared was never sent)", len(req.Records), req.Held)
	}
}

// If records of the now-conflicted identity were accepted by an earlier push of
// the hour, their usage_ids go in held, or delete-by-absence would remove them.
func TestPushWindows_AnAlreadySentConflictedRecordIsHeld(t *testing.T) {
	f := fixtureStore()
	m := &recordingManager{}
	p := newPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{win}, now); code != exitOK {
		t.Fatalf("first push: exit %d", code)
	}
	first := sentRequest(t, m.bodies[0])
	sharedID := first.Records[2].UsageID

	// A late event makes idShared ambiguous: the rater parks it and removes its row.
	all := fixtureRollups()
	f.rollups[win] = []rollupRow{all[0], all[1], all[3]}
	f.parked[win] = []identity{idShared}
	if code := p.pushWindows(context.Background(), []time.Time{win}, now); code != exitOK {
		t.Fatalf("second push: exit %d", code)
	}
	second := sentRequest(t, m.bodies[1])
	if len(second.Records) != 3 || len(second.Held) != 1 || second.Held[0] != sharedID {
		t.Fatalf("second push: %d records, held %v; want 3 records and held [%s]", len(second.Records), second.Held, sharedID)
	}
	// And it stays held on every later push while parked.
	if code := p.pushWindows(context.Background(), []time.Time{win}, now); code != exitOK {
		t.Fatal("third push failed")
	}
	if third := sentRequest(t, m.bodies[2]); len(third.Held) != 1 || third.Held[0] != sharedID {
		t.Fatalf("third push held %v, want [%s] still", third.Held, sharedID)
	}
}

// A rollup token-push itself cannot send (here: no org, a row from before the
// rater parked missing orgs) holds back only its identity; its earlier record
// is held; the hour goes out; exit 2.
func TestPushWindows_AnUnsendableRollupHoldsBackOnlyItsIdentity(t *testing.T) {
	f := fixtureStore()
	m := &recordingManager{}
	p := newPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{win}, now); code != exitOK {
		t.Fatal("first push failed")
	}
	firstFT := sentRequest(t, m.bodies[0]).Records[0].UsageID
	f.rollups[win][0].OrgID = sql.NullString{}
	if code := p.pushWindows(context.Background(), []time.Time{win}, now); code != exitWithheld {
		t.Fatalf("exit %d, want exitWithheld (%d)", code, exitWithheld)
	}
	req := sentRequest(t, m.bodies[1])
	if len(req.Records) != 3 || len(req.Held) != 1 || req.Held[0] != firstFT {
		t.Fatalf("%d records, held %v; want the other 3 and held [%s]", len(req.Records), req.Held, firstFT)
	}
}

// A parked hour that resolves is re-sent however old, even outside the window.
func TestPushWindows_ResolvedParkedHourIsSentEvenOutsideTheWindow(t *testing.T) {
	old := win.Add(-72 * time.Hour)
	f := newFake()
	f.rollups[old] = fixtureRollups()
	f.resolved = []time.Time{old}
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	if len(m.bodies) != 2 || sentRequest(t, m.bodies[0]).WindowStart != meteredusage.FormatWindowStart(old) {
		t.Fatalf("sent %d hours, want the resolved old hour first and then the window", len(m.bodies))
	}
	if f.accepted[old] != 1 {
		t.Fatal("the resolved hour's push was not recorded")
	}
}

func TestPushWindows_NeverRetries400_409_422AndExits3(t *testing.T) {
	for _, status := range []int{400, 409, 422} {
		m := &recordingManager{statuses: []int{status, 200}}
		if code := newPusher(fixtureStore(), m.client(t)).pushWindows(context.Background(), []time.Time{win}, now); code != exitRejected {
			t.Fatalf("status %d: exit %d, want exitRejected (%d)", status, code, exitRejected)
		}
		if len(m.bodies) != 1 {
			t.Fatalf("status %d: %d attempts, want exactly 1", status, len(m.bodies))
		}
	}
}

func TestPushWindows_Retries5xxUntilTheManagerAccepts(t *testing.T) {
	m := &recordingManager{statuses: []int{503, 502, 200}}
	if code := newPusher(fixtureStore(), m.client(t)).pushWindows(context.Background(), []time.Time{win}, now); code != exitOK {
		t.Fatalf("exit %d, want 0 after retries", code)
	}
	if len(m.bodies) != 3 || !bytes.Equal(m.bodies[0], m.bodies[2]) {
		t.Fatalf("%d attempts, want 3 identical ones", len(m.bodies))
	}
}

func TestPushWindows_Retries429UntilTheManagerAccepts(t *testing.T) {
	m := &recordingManager{statuses: []int{429, 200}}
	if code := newPusher(fixtureStore(), m.client(t)).pushWindows(context.Background(), []time.Time{win}, now); code != exitOK {
		t.Fatalf("exit %d, want 0 after a 429 retry", code)
	}
	if len(m.bodies) != 2 {
		t.Fatalf("%d attempts, want 2", len(m.bodies))
	}
}

// A failed send is fatal and outranks everything; nothing is recorded as
// accepted, but the intent is kept (holding a never-accepted id is harmless).
func TestPushWindows_Persistent5xxIsFatalAndRecordsNoAcceptance(t *testing.T) {
	f := fixtureStore()
	f.rollups[win][0].OrgID = sql.NullString{} // also something held back
	m := &recordingManager{statuses: []int{503}}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now); code != exitFatal {
		t.Fatalf("exit %d, want exitFatal", code)
	}
	if len(m.bodies) != meteredusage.DefaultMaxAttempts || f.accepted[win] != 0 {
		t.Fatalf("%d attempts, %d acceptances; want %d and 0", len(m.bodies), f.accepted[win], meteredusage.DefaultMaxAttempts)
	}
}

func TestPushWindows_RejectedOutranksHeldBack(t *testing.T) {
	f := fixtureStore()
	f.rollups[win][0].OrgID = sql.NullString{}
	m := &recordingManager{statuses: []int{422}}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now); code != exitRejected {
		t.Fatalf("exit %d, want exitRejected", code)
	}
}

// An entirely empty database would send every hour as empty and delete the
// manager's records; refuse instead.
func TestPushWindows_EmptyDatabaseRefusesToSendAnything(t *testing.T) {
	f := newFake()
	f.empty = true
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now); code != exitFatal {
		t.Fatalf("exit %d, want exitFatal", code)
	}
	if len(m.bodies) != 0 {
		t.Fatal("sent a request from an empty database")
	}
}

// D3 and the corrected conventions: a dedicated endpoint with no catalog base
// is sent with base_model "" in the same request as the rest of the hour.
func TestPushWindows_EmptyBaseModelDedicatedRecordIsSentAndDoesNotBlockTheHour(t *testing.T) {
	m := &recordingManager{}
	if code := newPusher(fixtureStore(), m.client(t)).pushWindows(context.Background(), []time.Time{win}, now); code != exitOK {
		t.Fatalf("exit %d, want 0", code)
	}
	sent := sentRequest(t, m.bodies[0])
	if len(sent.Records) != 4 {
		t.Fatalf("sent %d records, want all 4", len(sent.Records))
	}
	custom := sent.Records[3]
	if custom.Attributes.ModelID != "acme-custom-llm" || custom.Attributes.BaseModel != "" || custom.Attributes.ServingMode != "dedicated" {
		t.Fatalf("custom record %+v, want dedicated with base_model \"\"", custom.Attributes)
	}
}
