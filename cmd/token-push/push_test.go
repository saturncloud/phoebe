package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
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
	empty         bool // rated_usage and parked_usage both empty
	noRated       bool // rated_usage empty, parked_usage not
	everPushed    bool
	everPushedErr error
	unsent        []time.Time // candidates after the floor; the fake drops accepted and parked ones
	unsentLimit   int         // the limit the last UnsentHours call was given
	backfill      []time.Time // candidates before the floor; same filtering
	backfillLimit int         // the limit the last BackfillHours call was given (0: not called)
	refused       map[time.Time]*fakeRefused
	deferred      map[time.Time]*fakeDeferred
	checkSeq      int
	rollups       map[time.Time][]rollupRow
	parked        map[time.Time][]identity
	pushed        map[time.Time][]pushedRow
	resolved      []time.Time
	accepted      map[time.Time]int
	sentBefore    map[time.Time]bool // hours with a pushed_hour row from an earlier run
	autoFloor     *time.Time
	// Video usage: hasVideo is VideoUsageState's first answer; everPushedVideo
	// is derived from pushed (a video row) unless forced.
	hasVideo        bool
	everPushedVideo bool
	// staleTombstone: a live rollup's id has a tombstone somewhere.
	staleTombstone bool
	// tombstones are the video usage_ids the rater removed, per hour.
	tombstones   map[time.Time][]string
	videoRollups map[time.Time][]videoRollupRow
	videoParked  map[time.Time][]identity
}

// fakeRefused is one refused_hour row.
type fakeRefused struct {
	status   int
	message  string
	sha      string
	refusals int
	checked  int // order of the last check; lower is checked longer ago
}

// fakeDeferred is one deferred_hour row.
type fakeDeferred struct {
	status   int
	message  string
	refusals int
	last     int // order of the last refusal; lower is refused longer ago
}

func newFake() *fakeStore {
	return &fakeStore{rollups: map[time.Time][]rollupRow{}, parked: map[time.Time][]identity{},
		pushed: map[time.Time][]pushedRow{}, accepted: map[time.Time]int{}, sentBefore: map[time.Time]bool{},
		refused: map[time.Time]*fakeRefused{}, deferred: map[time.Time]*fakeDeferred{},
		videoRollups: map[time.Time][]videoRollupRow{}, videoParked: map[time.Time][]identity{}}
}

func (f *fakeStore) VideoUsageState(context.Context) (bool, bool, bool, bool, error) {
	ever, untombstoned := f.everPushedVideo, f.everPushedVideo
	for h, rows := range f.pushed {
		for _, r := range rows {
			if r.kind() != meteredusage.KindVideo {
				continue
			}
			ever = true
			gone := false
			for _, id := range f.tombstones[h] {
				gone = gone || id == r.UsageID
			}
			untombstoned = untombstoned || !gone
		}
	}
	return f.hasVideo, ever, untombstoned, f.staleTombstone, nil
}

func (f *fakeStore) EverPushedToken(context.Context) (bool, error) {
	for _, rows := range f.pushed {
		for _, r := range rows {
			if r.kind() == meteredusage.KindToken {
				return true, nil
			}
		}
	}
	return false, nil
}

// VideoHour returns the hour's rows stamped as the rater stamps them
// (usage_id from the row's own fields) unless a row carries its own UsageID,
// and the hour's tombstones.
func (f *fakeStore) VideoHour(_ context.Context, h time.Time) ([]videoRollupRow, map[string]bool, []identity, error) {
	out := make([]videoRollupRow, len(f.videoRollups[h]))
	for i, r := range f.videoRollups[h] {
		switch r.UsageID {
		case "":
			r.UsageID = stampedVideoID(h, r)
		case unstampedVideoID:
			r.UsageID = "" // what the query reads for a NULL usage_id
		}
		out[i] = r
	}
	tomb := map[string]bool{}
	for _, id := range f.tombstones[h] {
		tomb[id] = true
	}
	return out, tomb, f.videoParked[h], nil
}

// unstampedVideoID marks a fake rollup the rater never stamped.
const unstampedVideoID = "unstamped"

// stampedVideoID is the usage_id the rater stores for a rollup.
func stampedVideoID(h time.Time, r videoRollupRow) string {
	return meteredusage.VideoUsageID(meteredusage.FormatWindowStart(h), r.OrgID, r.OwnerType, r.OwnerID, r.ResourceID,
		meteredusage.VideoAttributes{AuthID: r.AuthID, ModelID: r.ModelID, ServingMode: r.ServingMode,
			BaseModel: r.BaseModel, Task: r.Task, Resolution: r.Resolution, Region: r.Region})
}

// eligible mirrors the SQL: never accepted, not parked and not deferred.
func (f *fakeStore) eligible(h time.Time) bool {
	return !f.sentBefore[h] && f.accepted[h] == 0 && f.refused[h] == nil && f.deferred[h] == nil
}

func (f *fakeStore) backfillCandidates() []time.Time {
	if f.autoFloor == nil {
		return nil
	}
	var out []time.Time
	for _, h := range f.backfill {
		if h.Before(*f.autoFloor) && f.eligible(h) {
			out = append(out, h)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

func (f *fakeStore) UsagePresent(context.Context) (bool, bool, error) {
	return !f.empty && !f.noRated, !f.empty, nil
}
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
func (f *fakeStore) EverPushed(context.Context) (bool, error) {
	return f.everPushed, f.everPushedErr
}
func (f *fakeStore) HourEverSent(_ context.Context, h time.Time) (bool, error) {
	return f.sentBefore[h] || f.accepted[h] > 0 || len(f.pushed[h]) > 0, nil
}
func (f *fakeStore) PreConnectionHours(context.Context) (int, time.Time, error) {
	c := f.backfillCandidates()
	if len(c) == 0 {
		return 0, time.Time{}, nil
	}
	return len(c), c[0], nil
}
func (f *fakeStore) BackfillHours(_ context.Context, limit int) ([]time.Time, error) {
	f.backfillLimit = limit
	c := f.backfillCandidates()
	if len(c) > limit {
		c = c[:limit]
	}
	return c, nil
}
func (f *fakeStore) UnsentHours(_ context.Context, before time.Time, limit int) ([]time.Time, error) {
	f.unsentLimit = limit
	var out []time.Time
	for _, h := range f.unsent {
		if h.Before(before) && f.eligible(h) && len(out) < limit {
			out = append(out, h)
		}
	}
	return out, nil
}
func (f *fakeStore) RefusedHours(context.Context) ([]refusedHour, error) {
	var out []refusedHour
	for h, r := range f.refused {
		out = append(out, refusedHour{Hour: h, PayloadSHA256: r.sha})
	}
	sort.Slice(out, func(i, j int) bool {
		ci, cj := f.refused[out[i].Hour].checked, f.refused[out[j].Hour].checked
		if ci != cj {
			return ci < cj
		}
		return out[i].Hour.Before(out[j].Hour)
	})
	return out, nil
}
func (f *fakeStore) RecordRefused(_ context.Context, h time.Time, status int, message, sha string) error {
	f.checkSeq++
	r := f.refused[h]
	if r == nil {
		r = &fakeRefused{}
		f.refused[h] = r
	}
	r.status, r.message, r.sha, r.checked = status, message, sha, f.checkSeq
	r.refusals++
	delete(f.deferred, h)
	return nil
}
func (f *fakeStore) DeferredHours(_ context.Context, limit int) ([]time.Time, error) {
	var out []time.Time
	for h := range f.deferred {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool {
		li, lj := f.deferred[out[i]].last, f.deferred[out[j]].last
		if li != lj {
			return li < lj
		}
		return out[i].Before(out[j])
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (f *fakeStore) DeferredSummary(context.Context) (int, time.Time, error) {
	var oldest time.Time
	for h := range f.deferred {
		if oldest.IsZero() || h.Before(oldest) {
			oldest = h
		}
	}
	return len(f.deferred), oldest, nil
}
func (f *fakeStore) RecordDeferred(_ context.Context, h time.Time, status int, message string) error {
	f.checkSeq++
	d := f.deferred[h]
	if d == nil {
		d = &fakeDeferred{}
		f.deferred[h] = d
	}
	d.status, d.message, d.last = status, message, f.checkSeq
	d.refusals++
	delete(f.refused, h)
	return nil
}
func (f *fakeStore) MarkRefusedChecked(_ context.Context, hours []time.Time) error {
	for _, h := range hours {
		f.checkSeq++
		if r := f.refused[h]; r != nil {
			r.checked = f.checkSeq
		}
	}
	return nil
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
func (f *fakeStore) RecordAccepted(_ context.Context, h time.Time, kinds []string, rows []pushedRow, setAutoFloor bool) error {
	listed := map[string]bool{}
	for _, k := range kinds {
		listed[k] = true
	}
	var kept []pushedRow
	for _, r := range f.pushed[h] {
		if !listed[r.kind()] {
			kept = append(kept, r)
		}
	}
	f.pushed[h] = append(kept, rows...)
	f.accepted[h]++
	delete(f.refused, h)
	delete(f.deferred, h)
	if setAutoFloor && f.autoFloor == nil {
		hh := h
		f.autoFloor = &hh
	}
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
	errBody  string // the body of a non-200 answer; default the manager's validation shape
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
			body := m.errBody
			if body == "" {
				body = `{"error":"stub","path":"records[0]"}`
			}
			_, _ = w.Write([]byte(body))
			return
		}
		var req meteredusage.Request
		_ = json.Unmarshal(body, &req)
		_ = json.NewEncoder(w).Encode(meteredusage.Response{Accepted: int64(req.RecordCount()), WindowStart: req.WindowStart, Kinds: req.Kinds})
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
	plan, err := newPusher(fixtureStore(), nil).buildHour(context.Background(), win, false)
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
	plan, err := newPusher(fixtureStore(), nil).buildHour(context.Background(), win, false)
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
	plan, err := newPusher(newFake(), nil).buildHour(context.Background(), win, false)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(plan.req)
	if !strings.Contains(string(b), `"records":[]`) || !strings.Contains(string(b), `"held":[]`) {
		t.Fatalf("empty hour marshals to %s; want records [] (delete-by-absence), never null", b)
	}
}

// An empty hour this install sent before is still SENT, as records: []: that
// is how the manager learns usage it holds for the hour is gone.
func TestPushWindows_EmptyHourPreviouslyPushedStillSentEmpty(t *testing.T) {
	f := newFake()
	f.everPushed = true
	f.sentBefore[win] = true // an earlier run had this hour accepted
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
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
		if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
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
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
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
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("first push: exit %d", code)
	}
	first := sentRequest(t, m.bodies[0])
	sharedID := first.Records[2].UsageID

	// A late event makes idShared ambiguous: the rater parks it and removes its row.
	all := fixtureRollups()
	f.rollups[win] = []rollupRow{all[0], all[1], all[3]}
	f.parked[win] = []identity{idShared}
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("second push: exit %d", code)
	}
	second := sentRequest(t, m.bodies[1])
	if len(second.Records) != 3 || len(second.Held) != 1 || second.Held[0] != sharedID {
		t.Fatalf("second push: %d records, held %v; want 3 records and held [%s]", len(second.Records), second.Held, sharedID)
	}
	// And it stays held on every later push while parked.
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
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
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatal("first push failed")
	}
	firstFT := sentRequest(t, m.bodies[0]).Records[0].UsageID
	f.rollups[win][0].OrgID = sql.NullString{}
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitWithheld {
		t.Fatalf("exit %d, want exitWithheld (%d)", code, exitWithheld)
	}
	req := sentRequest(t, m.bodies[1])
	if len(req.Records) != 3 || len(req.Held) != 1 || req.Held[0] != firstFT {
		t.Fatalf("%d records, held %v; want the other 3 and held [%s]", len(req.Records), req.Held, firstFT)
	}
}

// A parked hour that resolves is re-sent however old, even outside the window,
// after the window.
func TestPushWindows_ResolvedParkedHourIsSentEvenOutsideTheWindow(t *testing.T) {
	old := win.Add(-72 * time.Hour)
	f := fixtureStore()
	f.rollups[old] = fixtureRollups()
	f.resolved = []time.Time{old}
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	if len(m.bodies) != 2 || sentRequest(t, m.bodies[1]).WindowStart != meteredusage.FormatWindowStart(old) {
		t.Fatalf("sent %d hours, want the window first and then the resolved old hour", len(m.bodies))
	}
	if f.accepted[old] != 1 {
		t.Fatal("the resolved hour's push was not recorded")
	}
}

func TestPushWindows_NeverRetries400_409_422AndExits3(t *testing.T) {
	for _, status := range []int{400, 409, 422} {
		m := &recordingManager{statuses: []int{status, 200}}
		if code := newPusher(fixtureStore(), m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitRejected {
			t.Fatalf("status %d: exit %d, want exitRejected (%d)", status, code, exitRejected)
		}
		if len(m.bodies) != 1 {
			t.Fatalf("status %d: %d attempts, want exactly 1", status, len(m.bodies))
		}
	}
}

func TestPushWindows_Retries5xxUntilTheManagerAccepts(t *testing.T) {
	m := &recordingManager{statuses: []int{503, 502, 200}}
	if code := newPusher(fixtureStore(), m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("exit %d, want 0 after retries", code)
	}
	if len(m.bodies) != 3 || !bytes.Equal(m.bodies[0], m.bodies[2]) {
		t.Fatalf("%d attempts, want 3 identical ones", len(m.bodies))
	}
}

func TestPushWindows_Retries429UntilTheManagerAccepts(t *testing.T) {
	m := &recordingManager{statuses: []int{429, 200}}
	if code := newPusher(fixtureStore(), m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
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
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitFatal {
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
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitRejected {
		t.Fatalf("exit %d, want exitRejected", code)
	}
}

// EMPTY-TABLE GUARD, fresh install: an entirely empty database would send every
// hour as empty and delete the manager's records, so nothing is sent. Token-push
// runs on every install (rulings.md 2026-11-17), so an install with no Token
// Factory usage yet is normal and the run succeeds.
func TestPushWindows_EmptyDatabaseOnAFreshInstallSendsNothingAndSucceeds(t *testing.T) {
	f := newFake()
	f.empty = true
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("exit %d, want exitOK", code)
	}
	if len(m.bodies) != 0 {
		t.Fatal("sent a request from an empty database")
	}
}

// EMPTY-TABLE GUARD, wiped database: usage was sent before and rated_usage is
// now empty. Nothing is sent and the run fails loudly.
func TestPushWindows_EmptyDatabaseAfterUsageWasSentRefusesAndIsFatal(t *testing.T) {
	f := newFake()
	f.empty = true
	f.everPushed = true
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitFatal {
		t.Fatalf("exit %d, want exitFatal", code)
	}
	if len(m.bodies) != 0 {
		t.Fatal("sent a request from a wiped database")
	}
}

// An hour the manager never accepted after the automatic re-send floor (for
// instance an outage longer than the trailing window) is re-sent, after the
// window.
func TestPushWindows_AnHourNeverAcceptedIsReSentOutsideTheWindow(t *testing.T) {
	missed := win.Add(-48 * time.Hour)
	f := fixtureStore()
	f.rollups[missed] = fixtureRollups()
	f.unsent = []time.Time{missed}
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	if len(m.bodies) != 2 || sentRequest(t, m.bodies[1]).WindowStart != meteredusage.FormatWindowStart(missed) {
		t.Fatalf("sent %d hours, want the window first and then the missed hour", len(m.bodies))
	}
	if f.accepted[missed] != 1 {
		t.Fatal("the missed hour's push was not recorded as accepted")
	}
}

// An unsent hour that is also in the window is sent once, not twice.
func TestPushWindows_AnUnsentHourInsideTheWindowIsSentOnce(t *testing.T) {
	f := fixtureStore()
	f.unsent = []time.Time{win}
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	if len(m.bodies) != 1 {
		t.Fatalf("sent %d requests, want 1", len(m.bodies))
	}
}

// D3 and the corrected conventions: a dedicated endpoint with no catalog base
// is sent with base_model "" in the same request as the rest of the hour.
func TestPushWindows_EmptyBaseModelDedicatedRecordIsSentAndDoesNotBlockTheHour(t *testing.T) {
	m := &recordingManager{}
	if code := newPusher(fixtureStore(), m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
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

// After a full database wipe (pushed_hour and pushed_usage are empty too, so
// the liveness guard cannot tell), new usage makes the run go ahead. The
// earlier hours of the trailing window have no usage here, but the manager may
// still hold records for them from before the wipe: sending them as
// records: [] would delete those records. Only the hour with usage is sent.
func TestPushWindows_WipedDatabaseNewUsageDoesNotSendEmptyHoursNeverPushed(t *testing.T) {
	f := fixtureStore() // usage only at win; nothing was ever pushed
	m := &recordingManager{}
	windows := []time.Time{win.Add(-2 * time.Hour), win.Add(-time.Hour), win}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), windows, now, false); code != exitOK {
		t.Fatalf("exit %d, want exitOK", code)
	}
	if len(m.bodies) != 1 || sentRequest(t, m.bodies[0]).WindowStart != meteredusage.FormatWindowStart(win) {
		t.Fatalf("sent %d requests, want only the hour with usage (%s)", len(m.bodies), meteredusage.FormatWindowStart(win))
	}
	for _, h := range windows[:2] {
		if f.accepted[h] != 0 || len(f.pushed[h]) != 0 {
			t.Fatalf("hour %s was recorded as sent although it was skipped", h.Format(time.RFC3339))
		}
	}
}

// rated_usage is never pruned, so an empty rated_usage after usage was sent is
// a wiped table, even when parked_usage still has rows. Nothing is sent.
func TestPushWindows_RatedUsageWipedWithParkedRowsIsFatal(t *testing.T) {
	f := newFake()
	f.noRated = true // parked_usage has rows
	f.everPushed = true
	f.sentBefore[win] = true
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitFatal {
		t.Fatalf("exit %d, want exitFatal", code)
	}
	if len(m.bodies) != 0 {
		t.Fatalf("sent %d requests from a wiped rated_usage, want none", len(m.bodies))
	}
}

// If the guard cannot tell whether usage was sent before, it cannot tell a
// fresh install from a wiped one: the run fails and sends nothing.
func TestPushWindows_EverPushedErrorIsFatalAndSendsNothing(t *testing.T) {
	f := fixtureStore()
	f.everPushedErr = errors.New("connection reset")
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitFatal {
		t.Fatalf("exit %d, want exitFatal", code)
	}
	if len(m.bodies) != 0 {
		t.Fatalf("sent %d requests, want none", len(m.bodies))
	}
}

// The run asks the store for at most maxUnsentHoursPerRun never-accepted
// hours, so a long backlog is spread over several runs.
func TestPushWindows_PassesTheUnsentCapToTheStore(t *testing.T) {
	f := fixtureStore()
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	if f.unsentLimit != maxUnsentHoursPerRun || maxUnsentHoursPerRun != 168 {
		t.Fatalf("UnsentHours limit %d (maxUnsentHoursPerRun %d), want 168", f.unsentLimit, maxUnsentHoursPerRun)
	}
}

// The trailing window is sent first, then resolved hours, then unsent hours
// oldest first, each hour once. A long backlog therefore cannot use up the
// run's deadline before the current hours go out.
func TestPushWindows_TrailingWindowSentBeforeBacklog(t *testing.T) {
	f := fixtureStore()
	w0 := win.Add(-time.Hour)
	resolvedOld := win.Add(-30 * time.Hour)
	unsentOlder, unsentOld := win.Add(-50*time.Hour), win.Add(-40*time.Hour)
	for _, h := range []time.Time{w0, resolvedOld, unsentOlder, unsentOld} {
		f.rollups[h] = fixtureRollups()
	}
	f.resolved = []time.Time{resolvedOld}
	f.unsent = []time.Time{unsentOlder, unsentOld, win} // win is also in the window
	m := &recordingManager{}
	if code := newPusher(f, m.client(t)).pushWindows(context.Background(), []time.Time{w0, win}, now, false); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	var got []string
	for _, b := range m.bodies {
		got = append(got, sentRequest(t, b).WindowStart)
	}
	var want []string
	for _, h := range []time.Time{w0, win, resolvedOld, unsentOlder, unsentOld} {
		want = append(want, meteredusage.FormatWindowStart(h))
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("send order %v, want %v", got, want)
	}
}

// A scheduled run sets the automatic re-send floor with its first accepted
// hour; an explicit -since/-until run never does.
func TestPushWindows_OnlyAScheduledRunSetsTheAutoResendFloor(t *testing.T) {
	f := fixtureStore()
	m := &recordingManager{}
	p := newPusher(f, m.client(t))
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, true); code != exitOK {
		t.Fatalf("explicit run: exit %d", code)
	}
	if f.autoFloor != nil {
		t.Fatalf("an explicit run set the floor to %s", f.autoFloor)
	}
	if code := p.pushWindows(context.Background(), []time.Time{win}, now, false); code != exitOK {
		t.Fatalf("scheduled run: exit %d", code)
	}
	if f.autoFloor == nil || !f.autoFloor.Equal(win) {
		t.Fatalf("floor %v after a scheduled run, want %s", f.autoFloor, win)
	}
}
