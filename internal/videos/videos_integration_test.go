//go:build integration

package videos

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/migrations"
)

func newDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("PHOEBE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PHOEBE_TEST_DATABASE_URL not set")
	}
	schema := fmt.Sprintf("phoebe_videos_it_%d_%d", os.Getpid(), time.Now().UnixNano())
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
		_ = admin.Close()
	})
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	db, err := sql.Open("pgx", dsn+sep+"search_path="+schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	entries, _ := migrations.FS.ReadDir(".")
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".up.sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, n := range names {
		b, _ := migrations.FS.ReadFile(n)
		if _, err := db.Exec(string(b)); err != nil {
			t.Fatalf("%s: %v", n, err)
		}
	}
	return db
}

// memBlob stands in for Atlas and the store. Like Atlas it refuses an upload
// for a job that was never registered or is already closed, so these tests
// also check the order the service registers, writes and closes in.
type memBlob struct {
	mu     sync.Mutex
	objs   map[string][]byte
	fail   bool
	grants map[string]*grant
	// refuse makes Register fail, as Atlas does for an org that does not own
	// the endpoint or when the store is not configured.
	refuse bool
}

type grant struct {
	org, resource, inputExt string
	closed                  bool
}

func (b *memBlob) Register(_ context.Context, orgID, jobID, resourceID, inputExt string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.refuse {
		return fmt.Errorf("Atlas answered 404")
	}
	if b.grants == nil {
		b.grants = map[string]*grant{}
	}
	b.grants[jobID] = &grant{org: orgID, resource: resourceID, inputExt: inputExt}
	return nil
}

func (b *memBlob) Close(_ context.Context, orgID, jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	g, ok := b.grants[jobID]
	if !ok || g.org != orgID {
		return fmt.Errorf("no job %s for %s", jobID, orgID)
	}
	g.closed = true
	return nil
}

func (b *memBlob) grant(jobID string) grant {
	b.mu.Lock()
	defer b.mu.Unlock()
	if g, ok := b.grants[jobID]; ok {
		return *g
	}
	return grant{}
}

func (b *memBlob) Put(_ context.Context, key string, r io.Reader, _ int64, _ string) error {
	if b.fail {
		return fmt.Errorf("store down")
	}
	org, jobID, err := (Keys{Prefix: "tf-videos"}).Parse(key)
	if err != nil {
		return err
	}
	b.mu.Lock()
	g, ok := b.grants[jobID]
	open := ok && g.org == org && !g.closed
	b.mu.Unlock()
	if !open {
		return fmt.Errorf("put %s: the job is not in progress", key)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objs[key] = data
	return nil
}

func (b *memBlob) Get(_ context.Context, key string) (io.ReadCloser, int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	d, ok := b.objs[key]
	if !ok {
		return nil, 0, ErrBlobNotFound
	}
	return io.NopCloser(bytes.NewReader(d)), int64(len(d)), nil
}

func (b *memBlob) Delete(_ context.Context, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objs, key)
	return nil
}

func (b *memBlob) keys() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for k := range b.objs {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// fakeEngine returns err for the next calls in errs, then succeeds.
type fakeEngine struct {
	mu    sync.Mutex
	errs  []error
	calls int
	input []byte
}

func (e *fakeEngine) Generate(_ context.Context, job *Job, input []byte, path string, _ int64) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	e.input = input
	if len(e.errs) > 0 {
		err := e.errs[0]
		e.errs = e.errs[1:]
		return 0, err
	}
	data := []byte("\x00\x00\x00\x08ftypMP4!" + job.ID)
	return int64(len(data)), os.WriteFile(path, data, 0o600)
}

type fakeMedia struct{}

func (fakeMedia) ProbeFile(context.Context, string) (Probe, error) {
	return Probe{Frames: 121, FPS: big.NewRat(24, 1)}, nil
}
func (fakeMedia) Thumbnail(_ context.Context, _, out string) error {
	return os.WriteFile(out, []byte("RIFFthumb"), 0o600)
}

type harness struct {
	t      *testing.T
	db     *sql.DB
	store  *Store
	queue  *Queue
	mr     *miniredis.Miniredis
	blob   *memBlob
	engine *fakeEngine
	api    *API
	disp   *Dispatcher
	reaper *Reaper
	now    time.Time
}

func newHarness(t *testing.T) *harness {
	db := newDB(t)
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	h := &harness{t: t, db: db, store: NewStore(db), queue: NewQueue(rdb, "", "c1"), mr: mr,
		blob: &memBlob{objs: map[string][]byte{}}, engine: &fakeEngine{}, now: time.Now().UTC()}
	h.api = NewAPI(APIConfig{Token: "tok", QueueCap: 3, Prefix: "tf-videos"}, h.store, h.queue, h.blob, testLogger())
	h.api.now = func() time.Time { return h.now }
	h.disp = NewDispatcher(DispatchConfig{Prefix: "tf-videos", WorkDir: t.TempDir(), Lease: time.Minute},
		h.store, h.queue, h.blob, h.engine, fakeMedia{}, testLogger())
	h.disp.now = func() time.Time { return h.now }
	h.reaper = NewReaper(ReaperConfig{}, h.store, h.queue, h.blob, testLogger())
	h.reaper.now = func() time.Time { return h.now }
	return h
}

const limitsJSON = `{"tasks":["t2v","i2v"],"sizes":["1280x704"],"seconds":[5],"fps":24,"max_steps":50,"max_concurrency":1,"jobs_in_flight":2,"gpus":1}`

func identity(org, resource string) Handoff {
	return Handoff{OrgID: org, AuthID: "key-" + org, ResourceID: resource, ServedModel: "my-wan",
		BaseModel: "Wan-AI/Wan2.2-TI2V-5B-Diffusers", ServingMode: "dedicated",
		Upstream: "pd-x.ns.svc.cluster.local:8000", Limits: limitsJSON}
}

func (h *harness) do(method, path, body string, id Handoff) *httptest.ResponseRecorder {
	h.t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	enc, _ := EncodeHandoff(id)
	r.Header.Set(HeaderHandoff, enc)
	r.Header.Set(HeaderToken, "tok")
	w := httptest.NewRecorder()
	h.api.ServeHTTP(w, r)
	return w
}

func (h *harness) create(id Handoff) Object {
	h.t.Helper()
	w := h.do(http.MethodPost, "/v1/videos", `{"model":"my-wan","prompt":"a cat on a skateboard"}`, id)
	if w.Code != http.StatusOK {
		h.t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	var o Object
	_ = json.Unmarshal(w.Body.Bytes(), &o)
	return o
}

func (h *harness) object(w *httptest.ResponseRecorder) Object {
	h.t.Helper()
	var o Object
	if err := json.Unmarshal(w.Body.Bytes(), &o); err != nil {
		h.t.Fatalf("decode %s: %v", w.Body, err)
	}
	return o
}

func errorCode(w *httptest.ResponseRecorder) string {
	var body struct {
		Error struct{ Code string } `json:"error"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	return body.Error.Code
}

// run dispatches until nothing is running.
func (h *harness) run() {
	h.disp.tick(context.Background())
	h.disp.wg.Wait()
}

type billingRow struct {
	status, resolution, task, seconds, gpu string
	frames, count                          int
}

func (h *harness) billing(jobID string) (billingRow, int) {
	h.t.Helper()
	var n int
	_ = h.db.QueryRow(`SELECT count(*) FROM video_billing_event WHERE job_id = $1`, jobID).Scan(&n)
	var b billingRow
	if n == 1 {
		if err := h.db.QueryRow(`SELECT status, resolution, task, output_seconds::text, gpu_seconds::text,
			output_frames, request_count FROM video_billing_event WHERE job_id = $1`, jobID).
			Scan(&b.status, &b.resolution, &b.task, &b.seconds, &b.gpu, &b.frames, &b.count); err != nil {
			h.t.Fatal(err)
		}
	}
	return b, n
}

func TestVideoJobRunsEndToEndAndBillsOnce(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	o := h.create(id)
	if o.Status != StatusQueued || o.Progress != 0 || o.Object != "video" || o.Seconds != "5" ||
		o.Size != "1280x704" || o.Model != "my-wan" || o.Usage != nil || o.ExpiresAt != nil {
		t.Fatalf("created object: %+v", o)
	}
	if w := h.do(http.MethodGet, "/v1/videos/"+o.ID+"/content", "", id); w.Code != http.StatusConflict || errorCode(w) != "video_not_ready" {
		t.Fatalf("content while queued: %d %s", w.Code, w.Body)
	}
	if _, n := h.billing(o.ID); n != 0 {
		t.Fatal("a queued job has a billing row")
	}
	h.run()
	got := h.object(h.do(http.MethodGet, "/v1/videos/"+o.ID, "", id))
	if got.Status != StatusCompleted || got.Progress != 100 || got.Usage == nil ||
		got.Usage.OutputSeconds != "5.041666667" || got.Usage.OutputFrames != 121 || got.ExpiresAt == nil ||
		*got.ExpiresAt != h.now.Add(24*time.Hour).Unix() || got.CompletedAt == nil {
		t.Fatalf("completed object: %+v usage %+v", got, got.Usage)
	}
	w := h.do(http.MethodGet, "/v1/videos/"+o.ID+"/content", "", id)
	if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "video/mp4" ||
		!bytes.HasPrefix(w.Body.Bytes(), []byte("\x00\x00\x00\x08ftypMP4!"+o.ID)) ||
		!bytes.Contains(w.Body.Bytes(), []byte(DigitalSourceType)) {
		t.Fatalf("content: %d %q", w.Code, w.Body.String())
	}
	if w := h.do(http.MethodGet, "/v1/videos/"+o.ID+"/content?variant=thumbnail", "", id); w.Code != http.StatusOK || w.Header().Get("Content-Type") != "image/webp" {
		t.Fatalf("thumbnail: %d", w.Code)
	}
	if w := h.do(http.MethodGet, "/v1/videos/"+o.ID+"/content?variant=spritesheet", "", id); w.Code != http.StatusBadRequest {
		t.Fatalf("spritesheet variant: %d", w.Code)
	}
	b, n := h.billing(o.ID)
	if n != 1 || b.status != "completed" || b.resolution != "720p" || b.task != "t2v" || b.seconds != "5.041666667" ||
		b.frames != 121 || b.count != 1 {
		t.Fatalf("billing row (%d): %+v", n, b)
	}
	if g := h.blob.grant(o.ID); g.org != "org-a" || g.resource != "res-1" || g.inputExt != "" || !g.closed {
		t.Fatalf("store grant after completion: %+v (registered for the org and endpoint, then closed)", g)
	}
	keys := h.blob.keys()
	want := []string{"tf-videos/org-a/" + o.ID + "/thumbnail.webp", "tf-videos/org-a/" + o.ID + "/video.mp4"}
	if fmt.Sprint(keys) != fmt.Sprint(want) {
		t.Fatalf("stored keys %v, want %v", keys, want)
	}
}

func TestAnotherOrgsOrEndpointsJobIsNotFound(t *testing.T) {
	h := newHarness(t)
	owner := identity("org-a", "res-1")
	o := h.create(owner)
	h.run()
	for _, other := range []Handoff{identity("org-b", "res-1"), identity("org-a", "res-2")} {
		for _, req := range []struct{ method, path string }{
			{http.MethodGet, "/v1/videos/" + o.ID},
			{http.MethodGet, "/v1/videos/" + o.ID + "/content"},
			{http.MethodDelete, "/v1/videos/" + o.ID},
		} {
			if w := h.do(req.method, req.path, "", other); w.Code != http.StatusNotFound {
				t.Errorf("%s %s as %s/%s: %d, want 404 (never 403)", req.method, req.path, other.OrgID, other.ResourceID, w.Code)
			}
		}
		var list struct{ Data []Object }
		_ = json.Unmarshal(h.do(http.MethodGet, "/v1/videos", "", other).Body.Bytes(), &list)
		if len(list.Data) != 0 {
			t.Errorf("list as %s/%s shows %d jobs", other.OrgID, other.ResourceID, len(list.Data))
		}
	}
	if w := h.do(http.MethodGet, "/v1/videos/video_01ARZ3NDEKTSV4RRFFQ69G5FAV", "", owner); w.Code != http.StatusNotFound {
		t.Errorf("unknown id: %d", w.Code)
	}
	if w := h.do(http.MethodGet, "/v1/videos/../etc", "", owner); w.Code != http.StatusNotFound {
		t.Errorf("malformed id: %d", w.Code)
	}
	if w := h.do(http.MethodGet, "/v1/videos/"+o.ID, "", owner); w.Code != http.StatusOK {
		t.Errorf("owner: %d", w.Code)
	}
}

func TestLimitsAnswer429ForTheOrgAnd503ForAFullQueue(t *testing.T) {
	h := newHarness(t)
	a := identity("org-a", "res-1")
	h.create(a)
	h.create(a)
	w := h.do(http.MethodPost, "/v1/videos", `{"model":"my-wan","prompt":"x"}`, a)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") == "" {
		t.Fatalf("third job of org-a (limit 2): %d %s", w.Code, w.Body)
	}
	// The queue cap (3) counts every org's queued jobs on the endpoint.
	b := identity("org-b", "res-1")
	h.create(b)
	w = h.do(http.MethodPost, "/v1/videos", `{"model":"my-wan","prompt":"x"}`, b)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") == "" {
		t.Fatalf("fourth queued job on the endpoint (cap 3): %d %s", w.Code, w.Body)
	}
	if w := h.do(http.MethodPost, "/v1/videos", `{"model":"other","prompt":"x"}`, identity("org-c", "res-9")); w.Code != http.StatusForbidden || errorCode(w) != "model_not_authorized" {
		t.Fatalf("model mismatch: %d %s", w.Code, w.Body)
	}
	h.mr.Close()
	if w := h.do(http.MethodPost, "/v1/videos", `{"model":"my-wan","prompt":"x"}`, identity("org-c", "res-9")); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("Valkey down: %d, want 503 (a job cannot be queued without the queue)", w.Code)
	}
}

func TestDeleteCancelsQueuedRefusesRunningAndExpiresFinished(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	queued := h.create(id)
	w := h.do(http.MethodDelete, "/v1/videos/"+queued.ID, "", id)
	o := h.object(w)
	if w.Code != http.StatusOK || o.Status != StatusCancelled || o.Error == nil || o.Error.Code != CodeCancelled {
		t.Fatalf("cancel queued: %d %+v", w.Code, o)
	}
	if b, n := h.billing(queued.ID); n != 1 || b.status != "cancelled" || b.count != 0 || b.seconds != "0.000000000" {
		t.Fatalf("cancelled billing row (%d): %+v", n, b)
	}
	h.run() // the cancelled job is skipped
	if h.engine.calls != 0 {
		t.Fatal("a cancelled job reached the engine")
	}
	if !h.blob.grant(queued.ID).closed {
		t.Fatal("a cancelled job's store grant is still open")
	}

	running := h.create(id)
	if _, _, err := h.store.Claim(context.Background(), running.ID, time.Minute, h.now); err != nil {
		t.Fatal(err)
	}
	if w := h.do(http.MethodDelete, "/v1/videos/"+running.ID, "", id); w.Code != http.StatusConflict || errorCode(w) != "video_not_ready" {
		t.Fatalf("delete in_progress: %d %s", w.Code, w.Body)
	}
	_, _ = h.db.Exec(`UPDATE video_job SET status = 'queued', attempts = 0 WHERE id = $1`, running.ID)
	h.run()
	w = h.do(http.MethodDelete, "/v1/videos/"+running.ID, "", id)
	if o := h.object(w); w.Code != http.StatusOK || o.Status != StatusCompleted {
		t.Fatalf("delete finished: %d %+v (status must stay completed)", w.Code, o)
	}
	if len(h.blob.keys()) != 0 {
		t.Fatalf("files remain after delete: %v", h.blob.keys())
	}
	if w := h.do(http.MethodGet, "/v1/videos/"+running.ID+"/content", "", id); w.Code != http.StatusGone || errorCode(w) != "video_expired" {
		t.Fatalf("content after delete: %d %s", w.Code, w.Body)
	}
	var prompt sql.NullString
	_ = h.db.QueryRow(`SELECT prompt FROM video_job WHERE id = $1`, running.ID).Scan(&prompt)
	if prompt.Valid {
		t.Fatal("the prompt outlived the files")
	}
}

func TestEngineFailuresFailRetryOrWait(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")

	// An engine error fails the job, unbilled, without echoing the engine text.
	h.engine.errs = []error{&EngineHTTPError{Status: 500, Body: "CUDA OOM at /secret/path"}}
	o := h.create(id)
	h.run()
	got := h.object(h.do(http.MethodGet, "/v1/videos/"+o.ID, "", id))
	if got.Status != StatusFailed || got.Error == nil || got.Error.Code != CodeEngine || strings.Contains(got.Error.Message, "secret") {
		t.Fatalf("engine error: %+v %+v", got, got.Error)
	}
	if b, n := h.billing(o.ID); n != 1 || b.status != "failed" || b.count != 0 || b.frames != 0 {
		t.Fatalf("failed billing row (%d): %+v", n, b)
	}
	if !h.blob.grant(o.ID).closed {
		t.Fatal("a failed job's store grant is still open")
	}

	// Not ready: requeued, the attempt not counted, then it runs.
	h.engine.errs = []error{fmt.Errorf("%w: refused", ErrEngineNotReady)}
	o = h.create(id)
	h.run()
	var attempts int
	var status string
	_ = h.db.QueryRow(`SELECT status, attempts FROM video_job WHERE id = $1`, o.ID).Scan(&status, &attempts)
	if status != StatusQueued || attempts != 0 {
		t.Fatalf("not ready: status %s attempts %d", status, attempts)
	}
	h.disp.backoff = map[string]time.Time{}
	h.run()
	if got := h.object(h.do(http.MethodGet, "/v1/videos/"+o.ID, "", id)); got.Status != StatusCompleted {
		t.Fatalf("after the engine came up: %+v", got)
	}

	// The pod went away twice: retried once, then failed.
	h.engine.errs = []error{fmt.Errorf("%w: reset", ErrEngineGone), fmt.Errorf("%w: reset", ErrEngineGone)}
	o = h.create(id)
	h.run()
	h.run()
	got = h.object(h.do(http.MethodGet, "/v1/videos/"+o.ID, "", id))
	if got.Status != StatusFailed || got.Error.Code != CodeEngine {
		t.Fatalf("gone twice: %+v", got)
	}

	// Too large and timeout map to their codes.
	for _, tc := range []struct {
		err  error
		code string
	}{{ErrOutputTooLarge, CodeOutputTooLarge}, {context.DeadlineExceeded, CodeTimeout}} {
		h.engine.errs = []error{tc.err}
		o = h.create(id)
		h.run()
		if got := h.object(h.do(http.MethodGet, "/v1/videos/"+o.ID, "", id)); got.Error == nil || got.Error.Code != tc.code {
			t.Fatalf("%v: %+v", tc.err, got)
		}
	}
}

func TestImageToVideoPassesTheStoredImageAndDeletesIt(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	dataURL := "data:image/png;base64," + encodeB64(pngBytes)
	w := h.do(http.MethodPost, "/v1/videos", `{"model":"my-wan","prompt":"move","input_reference":{"image_url":"`+dataURL+`"}}`, id)
	o := h.object(w)
	if w.Code != http.StatusOK {
		t.Fatalf("i2v create: %d %s", w.Code, w.Body)
	}
	if g := h.blob.grant(o.ID); g.inputExt != "png" {
		t.Fatalf("i2v grant: %+v (the input extension is registered before the image is stored)", g)
	}
	h.run()
	if !bytes.Equal(h.engine.input, pngBytes) {
		t.Fatal("the engine did not get the input image")
	}
	if b, _ := h.billing(o.ID); b.task != "i2v" {
		t.Fatalf("task %q", b.task)
	}
	h.now = h.now.Add(25 * time.Hour)
	h.reaper.Once(context.Background())
	if len(h.blob.keys()) != 0 {
		t.Fatalf("files after expiry: %v", h.blob.keys())
	}
	if w := h.do(http.MethodGet, "/v1/videos/"+o.ID+"/content", "", id); w.Code != http.StatusGone {
		t.Fatalf("content after expiry: %d", w.Code)
	}
	if got := h.object(h.do(http.MethodGet, "/v1/videos/"+o.ID, "", id)); got.Prompt != nil {
		t.Fatal("prompt kept after the files expired")
	}
}

func TestReaperTimesOutQueuedJobsAndRecoversLostOnes(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	old := h.create(id)
	h.now = h.now.Add(61 * time.Minute)
	h.reaper.Once(context.Background())
	got := h.object(h.do(http.MethodGet, "/v1/videos/"+old.ID, "", id))
	if got.Status != StatusFailed || got.Error.Code != CodeQueueTimeout {
		t.Fatalf("queue timeout: %+v", got)
	}

	// Valkey lost its data: the reaper rebuilds the stream from Postgres.
	lost := h.create(id)
	h.mr.FlushAll()
	h.now = h.now.Add(3 * time.Minute)
	h.reaper.Once(context.Background())
	h.run()
	if got := h.object(h.do(http.MethodGet, "/v1/videos/"+lost.ID, "", id)); got.Status != StatusCompleted {
		t.Fatalf("a job whose stream entry was lost: %+v", got)
	}

	// A dispatcher died mid-job: the expired lease requeues it once, then fails it.
	dead := h.create(id)
	for i := 0; i < 2; i++ {
		e, _ := h.queue.Next(context.Background(), id.ResourceID, time.Minute)
		if e != nil {
			_ = h.queue.Done(context.Background(), id.ResourceID, e.StreamID)
		}
		if j, _, err := h.store.Claim(context.Background(), dead.ID, time.Minute, h.now); err != nil || j == nil {
			t.Fatalf("claim %d: %v %v", i, j, err)
		}
		h.now = h.now.Add(2 * time.Minute)
		h.reaper.Once(context.Background())
	}
	got = h.object(h.do(http.MethodGet, "/v1/videos/"+dead.ID, "", id))
	if got.Status != StatusFailed || got.Error.Code != CodeEngine {
		t.Fatalf("lease expired twice: %+v", got)
	}
}

func TestListCursorWalksAllJobsInBothOrders(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	var ids []string
	for i := 0; i < 5; i++ {
		j := &Job{OrgID: id.OrgID, AuthID: id.AuthID, ResourceID: id.ResourceID, ServedModel: "my-wan",
			BaseModel: id.BaseModel, ServingMode: "dedicated", Upstream: id.Upstream, Task: TaskT2V,
			Seconds: 5, Size: "1280x704", FPS: 24, GPUs: 1, MaxConcurrency: 1, CreatedAt: h.now.Add(time.Duration(i) * time.Second)}
		j.ID, _ = NewID(j.CreatedAt)
		if err := h.store.Insert(context.Background(), j, 100, 100); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.ID)
	}
	walk := func(order string) []string {
		var got []string
		after := ""
		for {
			q := "/v1/videos?limit=2&order=" + order
			if after != "" {
				q += "&after=" + after
			}
			var page struct {
				Data    []Object
				LastID  string `json:"last_id"`
				HasMore bool   `json:"has_more"`
			}
			_ = json.Unmarshal(h.do(http.MethodGet, q, "", id).Body.Bytes(), &page)
			for _, o := range page.Data {
				got = append(got, o.ID)
			}
			if !page.HasMore {
				return got
			}
			after = page.LastID
		}
	}
	if got := walk("asc"); fmt.Sprint(got) != fmt.Sprint(ids) {
		t.Fatalf("asc %v want %v", got, ids)
	}
	rev := []string{ids[4], ids[3], ids[2], ids[1], ids[0]}
	if got := walk("desc"); fmt.Sprint(got) != fmt.Sprint(rev) {
		t.Fatalf("desc %v want %v", got, rev)
	}
	if w := h.do(http.MethodGet, "/v1/videos?after="+ids[0], "", identity("org-b", "res-1")); w.Code != http.StatusBadRequest {
		t.Fatalf("another org's id as a cursor: %d", w.Code)
	}
	var page struct {
		Object  string
		Data    []Object
		FirstID string `json:"first_id"`
	}
	w := h.do(http.MethodGet, "/v1/videos?limit=1", "", id)
	_ = json.Unmarshal(w.Body.Bytes(), &page)
	if page.Object != "list" || len(page.Data) != 1 || page.FirstID != ids[4] {
		t.Fatalf("default order is newest first: %s", w.Body)
	}
	for _, bad := range []string{"limit=0", "limit=101", "order=sideways", "after=nope", "foo=1"} {
		if w := h.do(http.MethodGet, "/v1/videos?"+bad, "", id); w.Code != http.StatusBadRequest {
			t.Errorf("%s: %d", bad, w.Code)
		}
	}
}

func TestBillingRowsCannotCarryUsageUnlessCompleted(t *testing.T) {
	db := newDB(t)
	_, err := db.Exec(`INSERT INTO video_billing_event (job_id, created_at, org_id, auth_id, resource_id,
		model_id, base_model, serving_mode, task, resolution, status, output_seconds, output_frames, request_count)
		VALUES ('video_x', now(), 'o', 'a', 'r', 'm', 'b', 'dedicated', 't2v', '720p', 'failed', 5, 81, 1)`)
	if err == nil {
		t.Fatal("a failed job's billing row accepted usage")
	}
}

func encodeB64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// A job Atlas will not register is never accepted: nothing is stored, queued
// or billed, and the customer gets the opaque internal error.
func TestCreateIsRefusedWhenAtlasRefusesTheJob(t *testing.T) {
	h := newHarness(t)
	h.blob.refuse = true
	id := identity("org-a", "res-1")
	w := h.do(http.MethodPost, "/v1/videos", `{"model":"my-wan","prompt":"a cat"}`, id)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	var n int
	_ = h.db.QueryRow(`SELECT count(*) FROM video_job`).Scan(&n)
	if n != 0 || len(h.blob.keys()) != 0 {
		t.Fatalf("rows %d, objects %v", n, h.blob.keys())
	}
}

// A job whose create is refused after registration (here the org's
// jobs-in-flight limit) is closed with Atlas and its input image deleted.
func TestARefusedCreateClosesItsGrant(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	h.create(id)
	h.create(id) // jobs_in_flight is 2
	dataURL := "data:image/png;base64," + encodeB64(pngBytes)
	w := h.do(http.MethodPost, "/v1/videos", `{"model":"my-wan","prompt":"move","input_reference":{"image_url":"`+dataURL+`"}}`, id)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("third create: %d %s", w.Code, w.Body)
	}
	h.blob.mu.Lock()
	var refused []string
	for jobID, g := range h.blob.grants {
		if g.inputExt == "png" {
			refused = append(refused, jobID)
			if !g.closed {
				t.Errorf("refused job %s still open", jobID)
			}
		}
	}
	h.blob.mu.Unlock()
	if len(refused) != 1 {
		t.Fatalf("refused grants %v", refused)
	}
	for _, k := range h.blob.keys() {
		if strings.Contains(k, refused[0]) {
			t.Fatalf("the refused job's input image remains: %s", k)
		}
	}
}
