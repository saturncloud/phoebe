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
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	// registers and closes count the calls that reached "Atlas".
	registers, closes int
	// discards lists the keys Discard was asked for.
	discards []string
	// onRegister runs after a successful Register (outside the lock).
	onRegister func()
	// deleteErr, when set, decides Delete's answer for a key.
	deleteErr func(key string) error
	// deleteHangs, when set and true for a key, makes Delete wait until its
	// context ends, like a store that never answers.
	deleteHangs func(key string) bool
	// putErr, when set, decides Put's answer for a key (as Atlas refusing to
	// sign it would).
	putErr func(key string) error
	// signedBeforeClose, when set and true for a key, accepts its upload even
	// after the job closed, as a URL Atlas signed while the job was still in
	// progress would be.
	signedBeforeClose func(key string) bool
}

type grant struct {
	org, resource, inputExt string
	closed                  bool
	outputAttempt           int
}

func (b *memBlob) Register(_ context.Context, orgID, jobID, resourceID, inputExt string) error {
	b.mu.Lock()
	b.registers++
	if b.refuse {
		b.mu.Unlock()
		return fmt.Errorf("Atlas answered 404")
	}
	if b.grants == nil {
		b.grants = map[string]*grant{}
	}
	b.grants[jobID] = &grant{org: orgID, resource: resourceID, inputExt: inputExt}
	hook := b.onRegister
	b.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

func (b *memBlob) Close(_ context.Context, orgID, jobID string, outputAttempt int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closes++
	g, ok := b.grants[jobID]
	if !ok || g.org != orgID {
		return fmt.Errorf("no job %s for %s", jobID, orgID)
	}
	if g.closed && g.outputAttempt != outputAttempt {
		return fmt.Errorf("Atlas answered 409: closed with output attempt %d", g.outputAttempt)
	}
	if !g.closed {
		g.closed, g.outputAttempt = true, outputAttempt
	}
	return nil
}

// Discard is Delete, refused like Atlas for the attempt recorded as the
// job's output.
func (b *memBlob) Discard(ctx context.Context, key string) error {
	_, jobID, err := (Keys{Prefix: "tf-videos"}).Parse(key)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.discards = append(b.discards, key)
	g := b.grants[jobID]
	b.mu.Unlock()
	if g != nil && g.outputAttempt > 0 && strings.Contains(key, fmt.Sprintf("/attempt-%d/", g.outputAttempt)) {
		return fmt.Errorf("Atlas answered 409: %s is the recorded output", key)
	}
	return b.Delete(ctx, key)
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
	if b.putErr != nil {
		if err := b.putErr(key); err != nil {
			return err
		}
	}
	if b.fail {
		return fmt.Errorf("store down")
	}
	org, jobID, err := (Keys{Prefix: "tf-videos"}).Parse(key)
	if err != nil {
		return err
	}
	b.mu.Lock()
	g, ok := b.grants[jobID]
	open := ok && g.org == org && (!g.closed || (b.signedBeforeClose != nil && b.signedBeforeClose(key)))
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

func (b *memBlob) Delete(ctx context.Context, key string) error {
	if b.deleteHangs != nil && b.deleteHangs(key) {
		<-ctx.Done()
		return ctx.Err()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.deleteErr != nil {
		if err := b.deleteErr(key); err != nil {
			return err
		}
	}
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
	h.reaper = NewReaper(ReaperConfig{Prefix: "tf-videos"}, h.store, h.queue, h.blob, testLogger())
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
	want := []string{"tf-videos/org-a/" + o.ID + "/attempt-1/thumbnail.webp", "tf-videos/org-a/" + o.ID + "/attempt-1/video.mp4"}
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
// jobs-in-flight limit, reached by a concurrent create between the pre-check
// and the insert) is closed with Atlas and its input image deleted.
func TestARefusedCreateClosesItsGrant(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	h.create(id) // jobs_in_flight is 2: one slot left, so the pre-check passes
	h.blob.onRegister = func() {
		h.blob.onRegister = nil
		h.insertRow(id) // a concurrent create takes the last slot
	}
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

// orgHost is the handoff an org host sends (issue #71) for list, read,
// download and delete: the host's org and the caller's key, and an endpoint
// only as a restriction (an endpoint-scoped key, or the list's model filter).
// Nothing a stopped or deleted endpoint could no longer provide.
func orgHost(org, scope string) Handoff {
	return Handoff{OrgHost: true, OrgID: org, AuthID: "other-key-" + org, ResourceID: scope}
}

// completed creates a job on the endpoint's own route and runs it.
func (h *harness) completed(id Handoff) Object {
	h.t.Helper()
	o := h.create(id)
	h.run()
	return o
}

// TestOrgHostVideoRetrievalAfterEndpointDeleted: phoebe-videos never sees
// endpoint state, and the org-host handoff carries no served model, base
// model or limits, so a video is served for its org whatever became of the
// endpoint (Atlas has no say on the org host's read path).
func TestOrgHostVideoRetrievalAfterEndpointDeleted(t *testing.T) {
	h := newHarness(t)
	o := h.completed(identity("org-a", "res-1"))
	for _, r := range []Handoff{orgHost("org-a", ""), orgHost("org-a", "res-1")} {
		got := h.object(h.do(http.MethodGet, "/v1/videos/"+o.ID, "", r))
		if got.ID != o.ID || got.Status != StatusCompleted || got.ExpiresAt == nil || got.Usage == nil {
			t.Fatalf("object via the org host: %+v", got)
		}
		w := h.do(http.MethodGet, "/v1/videos/"+o.ID+"/content", "", r)
		if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "video/mp4" ||
			!bytes.HasPrefix(w.Body.Bytes(), []byte("\x00\x00\x00\x08ftypMP4!"+o.ID)) {
			t.Fatalf("content via the org host: %d %q", w.Code, w.Body.String())
		}
		if w := h.do(http.MethodGet, "/v1/videos/"+o.ID+"/content?variant=thumbnail", "", r); w.Code != http.StatusOK || w.Header().Get("Content-Type") != "image/webp" {
			t.Fatalf("thumbnail via the org host: %d", w.Code)
		}
	}
	// Still 24 hours of it: one second before expires_at the file is served.
	got := h.object(h.do(http.MethodGet, "/v1/videos/"+o.ID, "", orgHost("org-a", "")))
	h.now = time.Unix(*got.ExpiresAt, 0).Add(-time.Second)
	if w := h.do(http.MethodGet, "/v1/videos/"+o.ID+"/content", "", orgHost("org-a", "")); w.Code != http.StatusOK {
		t.Fatalf("content just before expiry: %d %s", w.Code, w.Body)
	}
}

// TestOrgHostOtherOrgsVideoIs404 and TestEndpointScopedKeyReachesOnlyItsVideos:
// another org's video, and another endpoint's video for an endpoint-scoped
// key, answer exactly like an unknown id (404, never 403), and a refused
// delete removes nothing.
func TestOrgHostOtherOrgsVideoIs404(t *testing.T) {
	h := newHarness(t)
	o := h.completed(identity("org-a", "res-1"))
	unknown := "video_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	for _, other := range []Handoff{orgHost("org-b", ""), orgHost("org-b", "res-1")} {
		for _, req := range []struct{ method, path string }{
			{http.MethodGet, "/v1/videos/" + o.ID},
			{http.MethodGet, "/v1/videos/" + o.ID + "/content"},
			{http.MethodDelete, "/v1/videos/" + o.ID},
		} {
			w := h.do(req.method, req.path, "", other)
			u := h.do(req.method, strings.Replace(req.path, o.ID, unknown, 1), "", other)
			if w.Code != http.StatusNotFound || w.Body.String() != u.Body.String() {
				t.Errorf("%s %s as %s: %d %s, want the unknown id's 404 %s", req.method, req.path, other.OrgID, w.Code, w.Body, u.Body)
			}
		}
	}
	if len(h.blob.keys()) != 2 {
		t.Fatalf("a refused delete removed files: %v", h.blob.keys())
	}
}

func TestEndpointScopedKeyReachesOnlyItsVideos(t *testing.T) {
	h := newHarness(t)
	mine := h.completed(identity("org-a", "res-1"))
	theirs := h.completed(identity("org-a", "res-2"))
	scoped := orgHost("org-a", "res-1")
	if w := h.do(http.MethodGet, "/v1/videos/"+mine.ID, "", scoped); w.Code != http.StatusOK {
		t.Fatalf("own endpoint's video: %d", w.Code)
	}
	for _, req := range []struct{ method, path string }{
		{http.MethodGet, "/v1/videos/" + theirs.ID},
		{http.MethodGet, "/v1/videos/" + theirs.ID + "/content"},
		{http.MethodDelete, "/v1/videos/" + theirs.ID},
	} {
		if w := h.do(req.method, req.path, "", scoped); w.Code != http.StatusNotFound {
			t.Errorf("%s %s with a key scoped to res-1: %d, want 404", req.method, req.path, w.Code)
		}
	}
	page := h.listPage("/v1/videos", scoped)
	if len(page.Data) != 1 || page.Data[0].ID != mine.ID {
		t.Fatalf("scoped list: %+v", page.Data)
	}
	if w := h.do(http.MethodGet, "/v1/videos?after="+theirs.ID, "", scoped); w.Code != http.StatusBadRequest {
		t.Fatalf("another endpoint's id as the scoped key's cursor: %d", w.Code)
	}
}

func TestOrgHostDeletesAVideoAndThenAnswers410(t *testing.T) {
	h := newHarness(t)
	o := h.completed(identity("org-a", "res-1"))
	r := orgHost("org-a", "")
	w := h.do(http.MethodDelete, "/v1/videos/"+o.ID, "", r)
	if got := h.object(w); w.Code != http.StatusOK || got.Status != StatusCompleted {
		t.Fatalf("delete via the org host: %d %+v", w.Code, got)
	}
	if len(h.blob.keys()) != 0 {
		t.Fatalf("files remain after delete: %v", h.blob.keys())
	}
	if w := h.do(http.MethodGet, "/v1/videos/"+o.ID+"/content", "", r); w.Code != http.StatusGone || errorCode(w) != "video_expired" {
		t.Fatalf("content after delete: %d %s", w.Code, w.Body)
	}
	// A queued job on a stopped endpoint can be cancelled from the org host
	// with no endpoint named in the handoff.
	q := h.create(identity("org-a", "res-1"))
	w = h.do(http.MethodDelete, "/v1/videos/"+q.ID, "", r)
	if got := h.object(w); w.Code != http.StatusOK || got.Status != StatusCancelled {
		t.Fatalf("cancel via the org host: %d %+v", w.Code, got)
	}
	if w := h.do(http.MethodGet, "/v1/videos/"+o.ID, "", r); w.Code != http.StatusOK {
		t.Fatalf("object after expiry: %d", w.Code)
	}
}

func TestOrgHostNeverCreates(t *testing.T) {
	h := newHarness(t)
	if w := h.do(http.MethodPost, "/v1/videos", `{"model":"my-wan","prompt":"x"}`, orgHost("org-a", "res-1")); w.Code != http.StatusNotFound {
		t.Fatalf("create via an org-host handoff: %d", w.Code)
	}
	var n int
	_ = h.db.QueryRow(`SELECT count(*) FROM video_job`).Scan(&n)
	if n != 0 {
		t.Fatalf("%d jobs exist", n)
	}
}

type listPage struct {
	Object  string   `json:"object"`
	Data    []Object `json:"data"`
	FirstID *string  `json:"first_id"`
	LastID  *string  `json:"last_id"`
	HasMore bool     `json:"has_more"`
}

func (h *harness) listPage(path string, id Handoff) listPage {
	h.t.Helper()
	w := h.do(http.MethodGet, path, "", id)
	if w.Code != http.StatusOK {
		h.t.Fatalf("GET %s: %d %s", path, w.Code, w.Body)
	}
	var p listPage
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		h.t.Fatal(err)
	}
	return p
}

// seedJobs writes jobs straight into the store at the given creation times,
// for (org, resource) pairs, and returns their ids in creation order.
func (h *harness) seedJobs(specs []struct {
	org, resource string
	at            time.Time
}) []string {
	h.t.Helper()
	var ids []string
	for _, sp := range specs {
		id := identity(sp.org, sp.resource)
		j := &Job{OrgID: id.OrgID, AuthID: id.AuthID, ResourceID: id.ResourceID, ServedModel: "my-wan",
			BaseModel: id.BaseModel, ServingMode: "dedicated", Upstream: id.Upstream, Task: TaskT2V,
			Seconds: 5, Size: "1280x704", FPS: 24, GPUs: 1, MaxConcurrency: 1, CreatedAt: sp.at}
		j.ID, _ = NewID(sp.at)
		if err := h.store.Insert(context.Background(), j, 1000, 1000); err != nil {
			h.t.Fatal(err)
		}
		ids = append(ids, j.ID)
	}
	return ids
}

// walk pages through a list with limit per page and returns every id.
func (h *harness) walk(id Handoff, base string, limit int, order string) (ids []string, pages int) {
	h.t.Helper()
	after := ""
	for {
		q := fmt.Sprintf("%s%slimit=%d&order=%s", base, sep(base), limit, order)
		if after != "" {
			q += "&after=" + after
		}
		p := h.listPage(q, id)
		pages++
		for _, o := range p.Data {
			ids = append(ids, o.ID)
		}
		if !p.HasMore {
			return ids, pages
		}
		if p.LastID == nil || pages > 1000 {
			h.t.Fatalf("has_more with last_id %v after %d pages", p.LastID, pages)
		}
		after = *p.LastID
	}
}

func sep(base string) string {
	if strings.Contains(base, "?") {
		return "&"
	}
	return "?"
}

func reversed(in []string) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[len(in)-1-i] = v
	}
	return out
}

// TestOrgHostVideoListIsOrgWide: the org host's list covers every endpoint
// of the host's org (a stopped or deleted one's too) and nothing of another
// org.
func TestOrgHostVideoListIsOrgWide(t *testing.T) {
	h := newHarness(t)
	ids := h.seedJobs([]struct {
		org, resource string
		at            time.Time
	}{
		{"org-a", "res-1", h.now}, {"org-b", "res-9", h.now.Add(time.Second)},
		{"org-a", "res-2", h.now.Add(2 * time.Second)}, {"org-a", "res-3", h.now.Add(3 * time.Second)},
	})
	p := h.listPage("/v1/videos", orgHost("org-a", ""))
	var got []string
	for _, o := range p.Data {
		got = append(got, o.ID)
	}
	if want := []string{ids[3], ids[2], ids[0]}; fmt.Sprint(got) != fmt.Sprint(want) || p.Object != "list" || p.HasMore {
		t.Fatalf("org-wide list %v want %v (newest first): %+v", got, want, p)
	}
	if *p.FirstID != ids[3] || *p.LastID != ids[0] {
		t.Fatalf("first_id/last_id: %v %v", *p.FirstID, *p.LastID)
	}
	// The video object carries its endpoint's model, so callers can tell
	// jobs of different endpoints apart.
	if p.Data[0].Model != "my-wan" {
		t.Fatalf("model on the object: %+v", p.Data[0])
	}
}

// TestOrgHostVideoListModelFilter: the interceptor resolves ?model= to one
// endpoint and passes it as the restriction; phoebe-videos accepts the model
// parameter only on the org host.
func TestOrgHostVideoListModelFilter(t *testing.T) {
	h := newHarness(t)
	ids := h.seedJobs([]struct {
		org, resource string
		at            time.Time
	}{
		{"org-a", "res-1", h.now}, {"org-a", "res-2", h.now.Add(time.Second)}, {"org-a", "res-1", h.now.Add(2 * time.Second)},
	})
	p := h.listPage("/v1/videos?model=wan-one", orgHost("org-a", "res-1"))
	if len(p.Data) != 2 || p.Data[0].ID != ids[2] || p.Data[1].ID != ids[0] {
		t.Fatalf("filtered list: %+v", p.Data)
	}
	// On an endpoint route model is not a list parameter (unchanged).
	if w := h.do(http.MethodGet, "/v1/videos?model=wan-one", "", identity("org-a", "res-1")); w.Code != http.StatusBadRequest {
		t.Fatalf("model on an endpoint route's list: %d", w.Code)
	}
}

// TestOrgHostVideoListUnknownModelIsEmptyNot404: a filter that names no
// endpoint the caller may see is an empty page, never a 404, so the filter
// reveals nothing; it stays empty whatever the other parameters are.
func TestOrgHostVideoListUnknownModelIsEmptyNot404(t *testing.T) {
	h := newHarness(t)
	h.seedJobs([]struct {
		org, resource string
		at            time.Time
	}{{"org-a", "res-1", h.now}})
	none := Handoff{OrgHost: true, NoMatch: true, OrgID: "org-a", AuthID: "k"}
	for _, q := range []string{"/v1/videos?model=nope", "/v1/videos?model=nope&limit=5&order=asc"} {
		p := h.listPage(q, none)
		if p.Object != "list" || len(p.Data) != 0 || p.HasMore || p.FirstID != nil || p.LastID != nil {
			t.Fatalf("%s: %+v", q, p)
		}
	}
	// Parameter validation still applies.
	if w := h.do(http.MethodGet, "/v1/videos?model=nope&limit=0", "", none); w.Code != http.StatusBadRequest {
		t.Fatalf("limit=0: %d", w.Code)
	}
}

// TestOrgHostVideoListPaginatesWithAfterLimitOrder walks more than one page
// in both orders, checks the defaults (20, newest first) and the 1..100
// bound, and that any other parameter is a 400.
func TestOrgHostVideoListPaginatesWithAfterLimitOrder(t *testing.T) {
	h := newHarness(t)
	var specs []struct {
		org, resource string
		at            time.Time
	}
	for i := 0; i < 25; i++ {
		specs = append(specs, struct {
			org, resource string
			at            time.Time
		}{"org-a", fmt.Sprintf("res-%d", i%3), h.now.Add(time.Duration(i) * time.Second)})
	}
	ids := h.seedJobs(specs)
	r := orgHost("org-a", "")
	got, pages := h.walk(r, "/v1/videos", 7, "asc")
	if fmt.Sprint(got) != fmt.Sprint(ids) || pages != 4 {
		t.Fatalf("asc: %d pages %v want %v", pages, got, ids)
	}
	got, pages = h.walk(r, "/v1/videos", 10, "desc")
	if fmt.Sprint(got) != fmt.Sprint(reversed(ids)) || pages != 3 {
		t.Fatalf("desc: %d pages %v", pages, got)
	}
	// Defaults: 20 per page, newest first.
	p := h.listPage("/v1/videos", r)
	if len(p.Data) != 20 || !p.HasMore || *p.FirstID != ids[24] || *p.LastID != ids[5] {
		t.Fatalf("defaults: %d items has_more=%v first=%v last=%v", len(p.Data), p.HasMore, *p.FirstID, *p.LastID)
	}
	next := h.listPage("/v1/videos?after="+*p.LastID, r)
	if len(next.Data) != 5 || next.HasMore || next.Data[0].ID != ids[4] {
		t.Fatalf("second default page: %+v", next)
	}
	p = h.listPage("/v1/videos?limit=100", r)
	if len(p.Data) != 25 || p.HasMore {
		t.Fatalf("limit=100: %d %v", len(p.Data), p.HasMore)
	}
	for _, bad := range []string{"limit=0", "limit=101", "limit=x", "order=sideways", "after=nope", "foo=1", "variant=video"} {
		if w := h.do(http.MethodGet, "/v1/videos?"+bad, "", r); w.Code != http.StatusBadRequest {
			t.Errorf("%s: %d", bad, w.Code)
		}
	}
}

// TestOrgHostVideoListCursorStableOnEqualTimestamps: jobs created in the same
// instant are ordered by id, so a cursor never skips or repeats one.
func TestOrgHostVideoListCursorStableOnEqualTimestamps(t *testing.T) {
	h := newHarness(t)
	var specs []struct {
		org, resource string
		at            time.Time
	}
	for i := 0; i < 9; i++ {
		specs = append(specs, struct {
			org, resource string
			at            time.Time
		}{"org-a", fmt.Sprintf("res-%d", i%2), h.now})
	}
	ids := h.seedJobs(specs)
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	r := orgHost("org-a", "")
	for _, limit := range []int{1, 2, 4} {
		if got, _ := h.walk(r, "/v1/videos", limit, "asc"); fmt.Sprint(got) != fmt.Sprint(sorted) {
			t.Fatalf("asc limit %d: %v want %v", limit, got, sorted)
		}
		if got, _ := h.walk(r, "/v1/videos", limit, "desc"); fmt.Sprint(got) != fmt.Sprint(reversed(sorted)) {
			t.Fatalf("desc limit %d: %v", limit, got)
		}
	}
}

// TestOrgHostVideoListAfterFromAnotherOrgIs400: a cursor must be a video of
// the same list; another org's id is refused as a bad parameter, the same
// answer as an unknown id.
func TestOrgHostVideoListAfterFromAnotherOrgIs400(t *testing.T) {
	h := newHarness(t)
	ids := h.seedJobs([]struct {
		org, resource string
		at            time.Time
	}{{"org-b", "res-1", h.now}, {"org-a", "res-1", h.now}})
	w := h.do(http.MethodGet, "/v1/videos?after="+ids[0], "", orgHost("org-a", ""))
	u := h.do(http.MethodGet, "/v1/videos?after=video_01ARZ3NDEKTSV4RRFFQ69G5FAV", "", orgHost("org-a", ""))
	if w.Code != http.StatusBadRequest || w.Body.String() != u.Body.String() {
		t.Fatalf("another org's cursor: %d %s vs unknown %s", w.Code, w.Body, u.Body)
	}
	if !strings.Contains(w.Body.String(), `"param":"after"`) {
		t.Fatalf("param not named: %s", w.Body)
	}
}

// The org-wide list reads video_job_org_list_ix (migration 0021).
func TestOrgWideListIndexExists(t *testing.T) {
	h := newHarness(t)
	var def string
	if err := h.db.QueryRow(`SELECT indexdef FROM pg_indexes WHERE indexname = 'video_job_org_list_ix'`).Scan(&def); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(def, "(org_id, created_at, id)") {
		t.Fatalf("index definition %q", def)
	}
}

// insertRow writes a queued job straight into the store, as a concurrent
// create would, without registering it with Atlas.
func (h *harness) insertRow(id Handoff) *Job {
	h.t.Helper()
	jobID, err := NewID(h.now)
	if err != nil {
		h.t.Fatal(err)
	}
	prompt := "a dog"
	j := &Job{ID: jobID, OrgID: id.OrgID, AuthID: id.AuthID, ResourceID: id.ResourceID,
		ServedModel: id.ServedModel, BaseModel: id.BaseModel, ServingMode: id.ServingMode,
		Upstream: id.Upstream, Task: "t2v", Prompt: &prompt, Seconds: 5, Size: "1280x704",
		FPS: 24, GPUs: 1, MaxConcurrency: 1, Status: StatusQueued, CreatedAt: h.now}
	if err := h.store.Insert(context.Background(), j, 100, 100); err != nil {
		h.t.Fatal(err)
	}
	return j
}

func (h *harness) atlasCalls() (registers, closes int) {
	h.blob.mu.Lock()
	defer h.blob.mu.Unlock()
	return h.blob.registers, h.blob.closes
}

// A create the org's jobs-in-flight limit refuses is refused before Atlas is
// asked to register it: no grant is opened, so none needs closing.
func TestCreateRefusedByOrgLimitDoesNotRegisterWithAtlas(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	h.create(id)
	h.create(id) // jobs_in_flight is 2
	regs, closes := h.atlasCalls()
	w := h.do(http.MethodPost, "/v1/videos", `{"model":"my-wan","prompt":"x"}`, id)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("third create: %d %s", w.Code, w.Body)
	}
	if r, c := h.atlasCalls(); r != regs || c != closes {
		t.Fatalf("Atlas calls for the refused create: %d register(s), %d close(s); want none", r-regs, c-closes)
	}
}

// A create the endpoint's full queue refuses is refused before Atlas is
// asked to register it.
func TestCreateRefusedByFullQueueDoesNotRegisterWithAtlas(t *testing.T) {
	h := newHarness(t)
	a := identity("org-a", "res-1")
	h.create(a)
	h.create(a)
	b := identity("org-b", "res-1")
	h.create(b) // the queue cap is 3
	regs, closes := h.atlasCalls()
	w := h.do(http.MethodPost, "/v1/videos", `{"model":"my-wan","prompt":"x"}`, b)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("fourth queued job: %d %s", w.Code, w.Body)
	}
	if r, c := h.atlasCalls(); r != regs || c != closes {
		t.Fatalf("Atlas calls for the refused create: %d register(s), %d close(s); want none", r-regs, c-closes)
	}
}

// A job the reaper fails for waiting too long is closed with Atlas, so no
// upload can be signed for it again.
func TestReaperClosesGrantOfQueueTimedOutJob(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	o := h.create(id)
	if g := h.blob.grant(o.ID); g.org != "org-a" || g.closed {
		t.Fatalf("grant after create: %+v", g)
	}
	h.now = h.now.Add(61 * time.Minute)
	h.reaper.Once(context.Background())
	if got := h.object(h.do(http.MethodGet, "/v1/videos/"+o.ID, "", id)); got.Error == nil || got.Error.Code != CodeQueueTimeout {
		t.Fatalf("queue timeout: %+v", got)
	}
	if g := h.blob.grant(o.ID); !g.closed {
		t.Fatalf("the timed-out job's grant is still open")
	}
}

// A job the reaper fails because its worker stopped twice is closed with
// Atlas; after the first stop it is requeued and its grant stays open.
func TestReaperClosesGrantOfJobFailedOnLeaseLoss(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	o := h.create(id)
	for i := 0; i < 2; i++ {
		e, _ := h.queue.Next(context.Background(), id.ResourceID, time.Minute)
		if e != nil {
			_ = h.queue.Done(context.Background(), id.ResourceID, e.StreamID)
		}
		if j, _, err := h.store.Claim(context.Background(), o.ID, time.Minute, h.now); err != nil || j == nil {
			t.Fatalf("claim %d: %v %v", i, j, err)
		}
		h.now = h.now.Add(2 * time.Minute)
		h.reaper.Once(context.Background())
		if i == 0 && h.blob.grant(o.ID).closed {
			t.Fatalf("the requeued job's grant was closed; it will run again")
		}
	}
	if got := h.object(h.do(http.MethodGet, "/v1/videos/"+o.ID, "", id)); got.Status != StatusFailed {
		t.Fatalf("lease expired twice: %+v", got)
	}
	if g := h.blob.grant(o.ID); !g.closed {
		t.Fatalf("the failed job's grant is still open")
	}
}

// insertExpired writes a failed job whose input image is due for deletion.
func insertExpired(t *testing.T, db *sql.DB, id, inputKey string, created, now time.Time) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO video_job (id, org_id, auth_id, resource_id, served_model,
			base_model, serving_mode, upstream, task, prompt, seconds, size, fps, gpus, status,
			error_code, created_at, completed_at, input_key)
		VALUES ($1, 'org-a', 'key-a', 'res-1', 'my-wan', 'base', 'dedicated', 'up:8000', 'i2v',
			'a secret prompt', 5, '1280x704', 24, 1, 'failed', 'engine_error', $2, $3, $4)`,
		id, created, now.Add(-time.Hour), inputKey); err != nil {
		t.Fatal(err)
	}
}

type fileState struct {
	deleted, failed, prompt bool
}

func filesOf(t *testing.T, db *sql.DB, id string) fileState {
	t.Helper()
	var s fileState
	if err := db.QueryRow(`SELECT files_deleted_at IS NOT NULL, files_delete_failed_at IS NOT NULL,
			prompt IS NOT NULL FROM video_job WHERE id = $1`, id).Scan(&s.deleted, &s.failed, &s.prompt); err != nil {
		t.Fatal(err)
	}
	return s
}

// Jobs whose files keep failing to delete go behind every job not yet tried,
// so more of them than one reaper batch (500) cannot hold back a newer job's
// files forever.
func TestReaperDoesNotStarveBehindUndeletableFiles(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	base := h.now.Add(-3 * time.Hour)
	if _, err := h.db.Exec(`INSERT INTO video_job (id, org_id, auth_id, resource_id, served_model,
			base_model, serving_mode, upstream, task, prompt, seconds, size, fps, gpus, status,
			error_code, created_at, completed_at, input_key)
		SELECT 'stuck-' || i, 'org-a', 'key-a', 'res-1', 'my-wan', 'base', 'dedicated', 'up:8000', 'i2v',
			'p', 5, '1280x704', 24, 1, 'failed', 'engine_error', $1::timestamptz + i * interval '1 second',
			$2, 'tf-videos/org-a/stuck-' || i || '/input.png'
		FROM generate_series(1, 600) AS i`, base, h.now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	h.blob.deleteErr = func(key string) error {
		if strings.Contains(key, "/stuck-") {
			return fmt.Errorf("store answered 500")
		}
		return nil
	}
	// Every stuck job is tried once (two batches of at most 500).
	h.reaper.Once(ctx)
	h.reaper.Once(ctx)
	var failed, deleted int
	_ = h.db.QueryRow(`SELECT count(*) FILTER (WHERE files_delete_failed_at IS NOT NULL),
		count(*) FILTER (WHERE files_deleted_at IS NOT NULL) FROM video_job`).Scan(&failed, &deleted)
	if failed != 600 || deleted != 0 {
		t.Fatalf("after two runs: %d marked failed, %d marked deleted; want 600 and 0", failed, deleted)
	}

	// A newer job's files expire: one run deletes them, though 600 older
	// undeletable jobs are still due.
	fresh := "tf-videos/org-a/fresh/input.png"
	h.blob.mu.Lock()
	h.blob.objs[fresh] = []byte("png")
	h.blob.mu.Unlock()
	insertExpired(t, h.db, "fresh", fresh, base.Add(time.Hour), h.now)
	h.reaper.Once(ctx)
	if s := filesOf(t, h.db, "fresh"); !s.deleted || s.prompt {
		t.Fatalf("the newer job's files after one run: %+v", s)
	}
	for _, k := range h.blob.keys() {
		if k == fresh {
			t.Fatalf("the newer job's input image is still stored")
		}
	}
}

// When Atlas no longer has a job (404), or a stored key is not one this
// service's layout makes, no retry can delete the file: the reaper logs it and
// marks the files deleted, so the prompt is erased and the row can be pruned.
// A 403 (also Atlas's answer to a wrong internal token) and a 503 are retried.
func TestReaperTreatsMissingAtlasGrantAsFinal(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	keys := Keys{Prefix: "tf-videos"}
	newID := func() string {
		id, err := NewID(h.now)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	gone, denied, down, badKey := newID(), newID(), newID(), newID()
	status := map[string]int{gone: http.StatusNotFound, denied: http.StatusForbidden, down: http.StatusServiceUnavailable}
	var mu sync.Mutex
	asked := map[string]int{}
	atlas := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		jobID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, atlasVideoJobsPath+"/"), "/object-urls")
		mu.Lock()
		asked[jobID]++
		mu.Unlock()
		code, ok := status[jobID]
		if !ok {
			code = http.StatusInternalServerError
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"message":"refused"}`))
	}))
	t.Cleanup(atlas.Close)
	blob, err := NewAtlasBlob(atlas.URL, staticToken("tok"), "tf-videos")
	if err != nil {
		t.Fatal(err)
	}
	h.reaper = NewReaper(ReaperConfig{Prefix: "tf-videos"}, h.store, h.queue, blob, testLogger())
	h.reaper.now = func() time.Time { return h.now }

	created := h.now.Add(-2 * time.Hour)
	for _, id := range []string{gone, denied, down} {
		insertExpired(t, h.db, id, keys.Input("org-a", id, "png"), created, h.now)
	}
	insertExpired(t, h.db, badKey, "tf-videos/org-a/not-a-job/input.png", created, h.now)
	h.reaper.Once(ctx)

	for name, id := range map[string]string{"atlas 404": gone, "unparseable key": badKey} {
		if s := filesOf(t, h.db, id); !s.deleted || s.prompt {
			t.Errorf("%s: %+v; want the files marked deleted and the prompt erased", name, s)
		}
	}
	for name, id := range map[string]string{"atlas 403": denied, "atlas 503": down} {
		if s := filesOf(t, h.db, id); s.deleted || !s.failed || !s.prompt {
			t.Errorf("%s: %+v; want a retry (marked failed, not deleted, prompt kept)", name, s)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if asked["not-a-job"] != 0 || len(asked) != 3 {
		t.Errorf("Atlas was asked %v; want only the three well-formed jobs", asked)
	}
}

// Atlas refusing to sign the video upload (here 409: the job is no longer in
// progress) answers the same on every attempt, so the job fails at once
// instead of generating the whole video again. A 503 is still retried.
func TestDispatcherDoesNotRetryAnAtlasRefusedUpload(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	status := http.StatusConflict
	h.blob.putErr = func(key string) error {
		if strings.HasSuffix(key, "/video.mp4") {
			return &atlasError{status: status, msg: "refused"}
		}
		return nil
	}
	o := h.create(id)
	h.run()
	h.run()
	got := h.object(h.do(http.MethodGet, "/v1/videos/"+o.ID, "", id))
	if got.Status != StatusFailed || got.Error == nil || got.Error.Code != CodeEngine ||
		got.Error.Message != "the video could not be stored" {
		t.Fatalf("refused upload: %+v %+v", got, got.Error)
	}
	if h.engine.calls != 1 {
		t.Fatalf("the engine ran %d times for a refused upload; want 1", h.engine.calls)
	}
	if !h.blob.grant(o.ID).closed {
		t.Fatal("the failed job's grant is still open")
	}

	status = http.StatusServiceUnavailable
	o = h.create(id)
	h.run()
	var st string
	var attempts int
	_ = h.db.QueryRow(`SELECT status, attempts FROM video_job WHERE id = $1`, o.ID).Scan(&st, &attempts)
	if st != StatusQueued || attempts != 1 {
		t.Fatalf("Atlas 503 on upload: status %s attempts %d; want it requeued for a retry", st, attempts)
	}
}

// A video larger than Atlas signs an upload for (413) fails the job with
// output_too_large, not a generic error, and is not generated again.
func TestDispatcherOutputOverStoreCapIsOutputTooLarge(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	h.blob.putErr = func(key string) error {
		if strings.HasSuffix(key, "/video.mp4") {
			return &atlasError{status: http.StatusRequestEntityTooLarge, msg: "too large"}
		}
		return nil
	}
	o := h.create(id)
	h.run()
	h.run()
	got := h.object(h.do(http.MethodGet, "/v1/videos/"+o.ID, "", id))
	if got.Status != StatusFailed || got.Error == nil || got.Error.Code != CodeOutputTooLarge {
		t.Fatalf("upload over the store cap: %+v %+v", got, got.Error)
	}
	if h.engine.calls != 1 {
		t.Fatalf("the engine ran %d times; want 1", h.engine.calls)
	}
}

// A delete that never returns is cut off by the reaper's per-job timeout: the
// job is marked for a later retry and the jobs behind it are still deleted in
// the same run.
func TestReaperContinuesPastAHungDelete(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.reaper = NewReaper(ReaperConfig{DeleteTimeout: 200 * time.Millisecond, Prefix: "tf-videos"}, h.store, h.queue, h.blob, testLogger())
	h.reaper.now = func() time.Time { return h.now }
	keys := Keys{Prefix: "tf-videos"}
	hungID, err := NewID(h.now.Add(-3 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	nextID, err := NewID(h.now.Add(-2 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	hungKey, nextKey := keys.Input("org-a", hungID, "png"), keys.Input("org-a", nextID, "png")
	h.blob.objs[nextKey] = []byte("png")
	insertExpired(t, h.db, hungID, hungKey, h.now.Add(-3*time.Hour), h.now)
	insertExpired(t, h.db, nextID, nextKey, h.now.Add(-2*time.Hour), h.now)
	h.blob.deleteHangs = func(key string) bool { return key == hungKey }

	done := make(chan struct{})
	go func() { h.reaper.Once(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the reaper is stuck on a delete that never returns")
	}
	if s := filesOf(t, h.db, hungID); s.deleted || !s.failed {
		t.Errorf("the hung job: %+v; want it marked for a retry", s)
	}
	if s := filesOf(t, h.db, nextID); !s.deleted || s.prompt {
		t.Errorf("the job behind the hung one: %+v; want its files deleted", s)
	}
}

// P7: a DELETE that cancels a job or deletes its files logs the key that sent
// it, with the org, endpoint, route and request id.
func TestOrgHostDeleteLogsTheCallersKey(t *testing.T) {
	h := newHarness(t)
	var buf bytes.Buffer
	h.api.log.Info = log.New(&buf, "", 0)
	del := func(jobID string) int {
		r := httptest.NewRequest(http.MethodDelete, "/v1/videos/"+jobID, nil)
		enc, _ := EncodeHandoff(orgHost("org-a", "res-1"))
		r.Header.Set(HeaderHandoff, enc)
		r.Header.Set(HeaderToken, "tok")
		r.Header.Set("X-Request-Id", "req-"+jobID)
		w := httptest.NewRecorder()
		h.api.ServeHTTP(w, r)
		return w.Code
	}
	done := h.completed(identity("org-a", "res-1"))
	queued := h.create(identity("org-a", "res-1"))
	for _, tc := range []struct{ jobID, what string }{{done.ID, "deleted"}, {queued.ID, "cancelled"}} {
		if code := del(tc.jobID); code != http.StatusOK {
			t.Fatalf("delete %s: %d", tc.jobID, code)
		}
		want := fmt.Sprintf(`videos: %s %s org_id="org-a" resource_id="res-1" auth_id="other-key-org-a" org_host=true request_id="req-%s"`,
			tc.what, tc.jobID, tc.jobID)
		if !strings.Contains(buf.String(), want) {
			t.Errorf("log %q has no line %q", buf.String(), want)
		}
	}
}

// --- Attempt-scoped outputs (battery P on phoebe#144) ------------------------

// claimDirect takes job id's queue entry and claims it as the next attempt,
// as a dispatcher replica would, without running it.
func (h *harness) claimDirect(id Handoff, jobID string) *Job {
	h.t.Helper()
	if e, _ := h.queue.Next(context.Background(), id.ResourceID, time.Minute); e != nil {
		_ = h.queue.Done(context.Background(), id.ResourceID, e.StreamID)
	}
	j, _, err := h.store.Claim(context.Background(), jobID, time.Minute, h.now)
	if err != nil || j == nil {
		h.t.Fatalf("claim %s: %v %v", jobID, j, err)
	}
	return j
}

// supersede claims job o as attempt 1, lets its lease expire (the reaper
// requeues it), then lets the dispatcher run and complete it as attempt 2.
// It returns attempt 1's claim, still held by its stalled dispatcher.
func (h *harness) supersede(id Handoff, o Object) *Job {
	h.t.Helper()
	first := h.claimDirect(id, o.ID)
	if first.Attempts != 1 {
		h.t.Fatalf("first claim is attempt %d", first.Attempts)
	}
	h.now = h.now.Add(2 * time.Minute)
	h.reaper.Once(context.Background())
	h.run()
	j, err := h.store.GetByID(context.Background(), o.ID)
	if err != nil || j.Status != StatusCompleted || j.Attempts != 2 {
		h.t.Fatalf("attempt 2 did not complete the job: %+v %v", j, err)
	}
	return first
}

// lateComplete runs attempt's completion with its own video, as a dispatcher
// that lost its lease and finished its engine call late would.
func (h *harness) lateComplete(job *Job, attempt int, content string) {
	h.t.Helper()
	path := filepath.Join(h.t.TempDir(), "video.mp4")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		h.t.Fatal(err)
	}
	h.disp.complete(context.Background(), job, attempt, path, int64(len(content)), time.Second)
}

func (h *harness) objectBytes(key string) []byte {
	h.blob.mu.Lock()
	defer h.blob.mu.Unlock()
	return append([]byte(nil), h.blob.objs[key]...)
}

func TestSupersededAttemptsLatePutCannotChangeTheRecordedOutput(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	o := h.create(id)
	first := h.supersede(id, o)
	keys := Keys{Prefix: "tf-videos"}
	output := keys.Video("org-a", o.ID, 2)
	done, _ := h.store.GetByID(context.Background(), o.ID)
	if done.VideoKey != output || done.ThumbnailKey != keys.Thumbnail("org-a", o.ID, 2) {
		t.Fatalf("recorded output %q %q, want attempt 2's keys", done.VideoKey, done.ThumbnailKey)
	}
	if g := h.blob.grant(o.ID); !g.closed || g.outputAttempt != 2 {
		t.Fatalf("grant %+v, want closed with output attempt 2", g)
	}
	before := h.objectBytes(output)

	// Attempt 1 still holds upload URLs signed before the job closed, and
	// its upload lands after attempt 2 completed.
	h.blob.signedBeforeClose = func(key string) bool { return strings.Contains(key, "/attempt-1/") }
	h.lateComplete(first, 1, "\x00\x00\x00\x08ftypLATE attempt 1")

	after, _ := h.store.GetByID(context.Background(), o.ID)
	if after.VideoKey != output || after.Status != StatusCompleted {
		t.Fatalf("the late attempt changed the job: %+v", after)
	}
	if !bytes.Equal(h.objectBytes(output), before) || len(before) == 0 {
		t.Fatal("the late attempt changed the recorded output's bytes")
	}
	w := h.do(http.MethodGet, "/v1/videos/"+o.ID+"/content", "", id)
	if w.Code != http.StatusOK || bytes.Contains(w.Body.Bytes(), []byte("LATE")) {
		t.Fatalf("content after the late attempt: %d %q", w.Code, w.Body.String())
	}
	// The late attempt removed only its own files, and the store keeps
	// exactly attempt 2's output.
	for _, k := range h.blob.discards {
		if !strings.Contains(k, "/attempt-1/") {
			t.Fatalf("the late attempt discarded %s", k)
		}
	}
	want := []string{keys.Thumbnail("org-a", o.ID, 2), output}
	if got := h.blob.keys(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("stored keys %v, want %v", got, want)
	}
	if b, n := h.billing(o.ID); n != 1 || b.count != 1 {
		t.Fatalf("billing (%d): %+v", n, b)
	}
}

func TestCleanupNeverDeletesTheRecordedOutput(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	o := h.create(id)
	h.supersede(id, o)
	done, _ := h.store.GetByID(context.Background(), o.ID)

	// Even a cleanup that names the recorded output (two attempts with one
	// number) leaves it: the dispatcher checks the job row, and Atlas
	// refuses a discard of the output attempt.
	h.disp.discard(context.Background(), o.ID, 2, done.VideoKey, done.ThumbnailKey)
	if len(h.blob.discards) != 0 {
		t.Fatalf("discard asked Atlas for the recorded output: %v", h.blob.discards)
	}
	if err := h.blob.Discard(context.Background(), done.VideoKey); err == nil {
		t.Fatal("the fake Atlas signed a discard of the recorded output")
	}
	for _, k := range []string{done.VideoKey, done.ThumbnailKey} {
		if len(h.objectBytes(k)) == 0 {
			t.Fatalf("%s was deleted", k)
		}
	}
	if w := h.do(http.MethodGet, "/v1/videos/"+o.ID+"/content", "", id); w.Code != http.StatusOK {
		t.Fatalf("content after cleanup: %d", w.Code)
	}
}

func TestRetriedAttemptSucceedsUnderItsOwnKeys(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	// Attempt 1's engine pod goes away; the job is retried once.
	h.engine.errs = []error{fmt.Errorf("%w: reset", ErrEngineGone)}
	o := h.create(id)
	h.run()
	h.run()
	got := h.object(h.do(http.MethodGet, "/v1/videos/"+o.ID, "", id))
	if got.Status != StatusCompleted {
		t.Fatalf("the retried job: %+v", got)
	}
	keys := Keys{Prefix: "tf-videos"}
	j, _ := h.store.GetByID(context.Background(), o.ID)
	if j.Attempts != 2 || j.VideoKey != keys.Video("org-a", o.ID, 2) {
		t.Fatalf("attempts %d, video key %q", j.Attempts, j.VideoKey)
	}
	if g := h.blob.grant(o.ID); g.outputAttempt != 2 {
		t.Fatalf("grant %+v, want output attempt 2", g)
	}
	if w := h.do(http.MethodGet, "/v1/videos/"+o.ID+"/content", "", id); w.Code != http.StatusOK ||
		!bytes.HasPrefix(w.Body.Bytes(), []byte("\x00\x00\x00\x08ftypMP4!"+o.ID)) {
		t.Fatalf("content: %d", w.Code)
	}
}

// Files an attempt left under its own keys (its completion could not be
// recorded, or its discard failed) go when the job's files expire.
func TestReaperDeletesEveryAttemptsFiles(t *testing.T) {
	h := newHarness(t)
	id := identity("org-a", "res-1")
	o := h.create(id)
	h.supersede(id, o)
	keys := Keys{Prefix: "tf-videos"}
	leftover := keys.Video("org-a", o.ID, 1)
	h.blob.mu.Lock()
	h.blob.objs[leftover] = []byte("attempt 1")
	h.blob.mu.Unlock()

	if w := h.do(http.MethodDelete, "/v1/videos/"+o.ID, "", id); w.Code != http.StatusOK {
		t.Fatalf("delete: %d", w.Code)
	}
	h.now = h.now.Add(time.Minute)
	h.reaper.Once(context.Background())
	if got := h.blob.keys(); len(got) != 0 {
		t.Fatalf("left in the store: %v", got)
	}
	if s := filesOf(t, h.db, o.ID); !s.deleted {
		t.Fatalf("files state %+v", s)
	}
}
