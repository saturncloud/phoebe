package videos

import (
	"bytes"
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

// Internal headers between the interceptor and this service. The interceptor
// strips every client X-Saturn-* and X-Phoebe-* header before it sets these,
// and this service is reachable only from the interceptor (NetworkPolicy);
// the shared token is the second lock, so a pod that can reach the service by
// some other path still cannot speak for an org.
const (
	HeaderHandoff = "X-Phoebe-Video-Identity"
	HeaderToken   = "X-Phoebe-Videos-Token"
)

// Handoff is the trusted request identity the interceptor passes on: what
// ForwardAuth and Atlas's route stamped, after the interceptor's own checks.
type Handoff struct {
	// Retrieval marks a request from the always-on retrieval route (issue
	// #13): it may only read, download or delete one video, and carries only
	// the org, the caller and the video's endpoint, never the endpoint's
	// served model, upstream or limits (the endpoint may be stopped or gone).
	Retrieval   bool   `json:"retrieval,omitempty"`
	OrgID       string `json:"org_id"`
	AuthID      string `json:"auth_id"`
	UserID      string `json:"user_id,omitempty"`
	GroupID     string `json:"group_id,omitempty"`
	ResourceID  string `json:"resource_id"`
	ServedModel string `json:"served_model"`
	BaseModel   string `json:"base_model"`
	ServingMode string `json:"serving_mode"`
	Upstream    string `json:"upstream"`
	Region      string `json:"region,omitempty"`
	// Limits is the raw X-Saturn-Video-Limits value.
	Limits string `json:"limits"`
}

// EncodeHandoff renders the header value.
func EncodeHandoff(h Handoff) (string, error) {
	b, err := json.Marshal(h)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func decodeHandoff(v string) (Handoff, error) {
	var h Handoff
	b, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return h, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&h); err != nil {
		return h, err
	}
	if h.Retrieval {
		if h.OrgID == "" || h.AuthID == "" || h.ResourceID == "" {
			return h, fmt.Errorf("incomplete retrieval identity")
		}
		if h.ServedModel != "" || h.BaseModel != "" || h.Upstream != "" || h.ServingMode != "" || h.Limits != "" {
			return h, fmt.Errorf("retrieval identity carries endpoint fields")
		}
		return h, nil
	}
	if h.OrgID == "" || h.AuthID == "" || h.ResourceID == "" || h.ServedModel == "" ||
		h.BaseModel == "" || h.Upstream == "" || h.ServingMode != "dedicated" {
		return h, fmt.Errorf("incomplete identity")
	}
	return h, nil
}

// APIConfig bounds the API.
type APIConfig struct {
	// Token is the shared interceptor token. Empty refuses every request.
	Token string
	// QueueCap is the most queued jobs one endpoint may hold (design 5.4: 200).
	QueueCap int
	// Prefix is the object key prefix (TF_VIDEO_STORE_PREFIX).
	Prefix string
}

// API serves /v1/videos for one video endpoint per request.
type API struct {
	cfg   APIConfig
	store *Store
	queue *Queue
	blob  Blob
	log   *logging.Logger
	now   func() time.Time
}

func NewAPI(cfg APIConfig, store *Store, queue *Queue, blob Blob, log *logging.Logger) *API {
	if cfg.QueueCap <= 0 {
		cfg.QueueCap = 200
	}
	return &API{cfg: cfg, store: store, queue: queue, blob: blob, log: log, now: time.Now}
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Opaque to the caller, specific in the log.
	if a.cfg.Token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get(HeaderToken)), []byte(a.cfg.Token)) != 1 {
		a.log.Warn.Printf("videos: refused a request without the interceptor token (remote %s)", r.RemoteAddr)
		writeError(w, errNotFound)
		return
	}
	id, err := decodeHandoff(r.Header.Get(HeaderHandoff))
	if err != nil {
		a.log.Error.Printf("videos: bad identity handoff: %v", err)
		writeError(w, errNotFound)
		return
	}
	path := r.URL.Path
	if id.Retrieval {
		// The always-on route reads, downloads or deletes one video (issue
		// #13); it never creates or lists, which need the endpoint itself.
		if !strings.HasPrefix(path, "/v1/videos/") {
			writeError(w, errNotFound)
			return
		}
		a.serveOne(w, r, id, strings.TrimPrefix(path, "/v1/videos/"))
		return
	}
	limits, err := ParseLimits(id.Limits)
	if err != nil {
		a.log.Error.Printf("videos: resource %s: %v", id.ResourceID, err)
		writeError(w, errNotFound)
		return
	}
	switch {
	case path == "/v1/videos" && r.Method == http.MethodPost:
		a.create(w, r, id, limits)
	case path == "/v1/videos" && r.Method == http.MethodGet:
		a.list(w, r, id)
	case strings.HasPrefix(path, "/v1/videos/"):
		a.serveOne(w, r, id, strings.TrimPrefix(path, "/v1/videos/"))
	default:
		writeError(w, errNotFound)
	}
}

// serveOne answers the three requests on one video: rest is the path after
// /v1/videos/. The job is looked up by id, org and endpoint on either route.
func (a *API) serveOne(w http.ResponseWriter, r *http.Request, id Handoff, rest string) {
	jobID, sub, hasSub := strings.Cut(rest, "/")
	if !ValidID(jobID) {
		writeError(w, errNotFound)
		return
	}
	switch {
	case !hasSub && r.Method == http.MethodGet:
		a.get(w, r, id, jobID)
	case !hasSub && r.Method == http.MethodDelete:
		a.delete(w, r, id, jobID)
	case sub == "content" && r.Method == http.MethodGet:
		a.content(w, r, id, jobID)
	default:
		writeError(w, errNotFound)
	}
}

var errModel = &APIError{Status: http.StatusForbidden, Code: "model_not_authorized",
	Message: "The requested model is not served by this endpoint. Set model to this endpoint's model name."}

func (a *API) create(w http.ResponseWriter, r *http.Request, id Handoff, limits Limits) {
	req, apiErr := ParseCreate(r, limits)
	if apiErr != nil {
		writeError(w, apiErr)
		return
	}
	if !servedModelAllowed(id.ServedModel, req.Model) {
		writeError(w, errModel)
		return
	}
	ctx := r.Context()
	// Valkey down: a job cannot be queued without the queue, so refuse rather
	// than accept work nothing will run (design 5.4; not a quota fail-open).
	if err := a.queue.Ping(ctx); err != nil {
		a.log.Error.Printf("videos: queue unavailable: %v", err)
		writeError(w, errQueueDown)
		return
	}
	// A create the limits refuse now is refused before Atlas registers it, so
	// a client retrying into a full queue never adds work to Atlas. Insert
	// checks the limits again under its lock and decides.
	if err := a.store.Admit(ctx, id.ResourceID, id.OrgID, limits.JobsInFlight, a.cfg.QueueCap); err != nil {
		a.writeLimitError(w, "count jobs on "+id.ResourceID, err)
		return
	}
	now := a.now().UTC()
	jobID, err := NewID(now)
	if err != nil {
		writeError(w, errInternal)
		return
	}
	keys := Keys{Prefix: a.cfg.Prefix}
	inputExt := ""
	if req.Input != nil {
		inputExt = imageTypes[req.Params.InputContentType]
	}
	// Atlas signs a URL for this job's files only once it knows the job, and
	// only for the input extension registered here (design 6.1).
	if err := a.blob.Register(ctx, id.OrgID, jobID, id.ResourceID, inputExt); err != nil {
		a.log.Error.Printf("videos: register %s with Atlas: %v", jobID, err)
		writeError(w, errInternal)
		return
	}
	job := &Job{
		ID: jobID, OrgID: id.OrgID, AuthID: id.AuthID, UserID: id.UserID, GroupID: id.GroupID,
		ResourceID: id.ResourceID, ServedModel: req.Model, BaseModel: id.BaseModel,
		ServingMode: id.ServingMode, Upstream: id.Upstream, Region: id.Region, Task: req.Task,
		Prompt: &req.Prompt, Seconds: req.Seconds, Size: req.Size, FPS: limits.FPS, GPUs: limits.GPUs, MaxConcurrency: limits.MaxConcurrency,
		Params: req.Params, Status: StatusQueued, CreatedAt: now,
	}
	if req.Input != nil {
		job.InputKey = keys.Input(id.OrgID, jobID, inputExt)
		if err := a.blob.Put(ctx, job.InputKey, bytes.NewReader(req.Input), int64(len(req.Input)), req.Params.InputContentType); err != nil {
			a.log.Error.Printf("videos: store input image for %s: %v", jobID, err)
			a.dropInput(job)
			writeError(w, errInternal)
			return
		}
	}
	if err := a.store.Insert(ctx, job, limits.JobsInFlight, a.cfg.QueueCap); err != nil {
		a.dropInput(job)
		a.writeLimitError(w, "insert "+jobID, err)
		return
	}
	if err := a.queue.Add(ctx, id.ResourceID, jobID); err != nil {
		// The row is queued; the reaper re-adds queued jobs whose stream lost
		// them, so the job still runs. Answer success: it is accepted.
		a.log.Error.Printf("videos: queue %s on %s (the reaper will re-add it): %v", jobID, id.ResourceID, err)
	}
	a.log.Info.Printf("videos: queued %s resource=%s org=%s task=%s size=%s seconds=%d",
		jobID, id.ResourceID, id.OrgID, job.Task, job.Size, job.Seconds)
	writeJSON(w, http.StatusOK, job.Object())
}

// writeLimitError answers a create that Admit or Insert refused.
func (a *API) writeLimitError(w http.ResponseWriter, step string, err error) {
	switch {
	case errors.Is(err, errStoreOrgLimit):
		writeError(w, errOrgLimit)
	case errors.Is(err, errStoreQueueFull):
		writeError(w, errQueueFull)
	default:
		a.log.Error.Printf("videos: %s: %v", step, err)
		writeError(w, errInternal)
	}
}

// dropInput undoes a create that was refused after registration: the input
// image (if any) is deleted and the job closed with Atlas, so nothing more can
// be written under its prefix.
func (a *API) dropInput(job *Job) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if job.InputKey != "" {
		if err := a.blob.Delete(ctx, job.InputKey); err != nil {
			a.log.Warn.Printf("videos: delete input image of refused job %s: %v", job.ID, err)
		}
	}
	CloseGrant(ctx, a.blob, a.log, job, 0)
}

// CloseGrant tells Atlas the job finished, so no upload is signed for it
// again. outputAttempt is the attempt whose files are a completed job's
// output (0 for a job that did not complete). Best effort: a job that is
// never closed stops being writable when its upload window ends (Atlas
// TF_VIDEO_UPLOAD_WINDOW_MINUTES).
func CloseGrant(ctx context.Context, blob Blob, log *logging.Logger, job *Job, outputAttempt int) {
	if err := blob.Close(ctx, job.OrgID, job.ID, outputAttempt); err != nil {
		log.Warn.Printf("videos: close %s with Atlas (its upload window still ends it): %v", job.ID, err)
	}
}

func servedModelAllowed(allowList, model string) bool {
	for _, m := range strings.Split(allowList, ",") {
		if strings.TrimSpace(m) == model && model != "" {
			return true
		}
	}
	return false
}

func (a *API) lookup(w http.ResponseWriter, r *http.Request, id Handoff, jobID string) *Job {
	job, err := a.store.Get(r.Context(), jobID, id.OrgID, id.ResourceID)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, errNotFound)
		return nil
	}
	if err != nil {
		a.log.Error.Printf("videos: get %s: %v", jobID, err)
		writeError(w, errInternal)
		return nil
	}
	return job
}

func (a *API) get(w http.ResponseWriter, r *http.Request, id Handoff, jobID string) {
	if job := a.lookup(w, r, id, jobID); job != nil {
		writeJSON(w, http.StatusOK, job.Object())
	}
}

func (a *API) list(w http.ResponseWriter, r *http.Request, id Handoff) {
	q := r.URL.Query()
	for k := range q {
		if k != "after" && k != "limit" && k != "order" {
			writeError(w, badRequest(k, fmt.Sprintf("unknown query parameter %q", k)))
			return
		}
	}
	limit := 20
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			writeError(w, badRequest("limit", "limit must be from 1 to 100"))
			return
		}
		limit = n
	}
	order := q.Get("order")
	if order == "" {
		order = "desc"
	}
	if order != "asc" && order != "desc" {
		writeError(w, badRequest("order", "order must be asc or desc"))
		return
	}
	after := q.Get("after")
	if after != "" && !ValidID(after) {
		writeError(w, badRequest("after", "after must be a video id"))
		return
	}
	jobs, err := a.store.List(r.Context(), id.OrgID, id.ResourceID, after, limit, order == "asc")
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, badRequest("after", "after names no video of yours on this endpoint"))
		return
	}
	if err != nil {
		a.log.Error.Printf("videos: list %s: %v", id.ResourceID, err)
		writeError(w, errInternal)
		return
	}
	hasMore := len(jobs) > limit
	if hasMore {
		jobs = jobs[:limit]
	}
	data := make([]Object, len(jobs))
	for i, j := range jobs {
		data[i] = j.Object()
	}
	body := map[string]any{"object": "list", "data": data, "has_more": hasMore, "first_id": nil, "last_id": nil}
	if len(jobs) > 0 {
		body["first_id"] = jobs[0].ID
		body["last_id"] = jobs[len(jobs)-1].ID
	}
	writeJSON(w, http.StatusOK, body)
}

func (a *API) delete(w http.ResponseWriter, r *http.Request, id Handoff, jobID string) {
	ctx := r.Context()
	job := a.lookup(w, r, id, jobID)
	if job == nil {
		return
	}
	switch job.Status {
	case StatusQueued:
		j, err := a.store.CancelQueued(ctx, jobID, id.OrgID, id.ResourceID, a.now().UTC())
		if err != nil {
			a.log.Error.Printf("videos: cancel %s: %v", jobID, err)
			writeError(w, errInternal)
			return
		}
		if j.Status == StatusInProgress {
			// It was claimed between the read and the cancel.
			writeError(w, errInProgress)
			return
		}
		CloseGrant(ctx, a.blob, a.log, j, 0)
		a.deleteFiles(ctx, j)
		a.logDeleted(r, id, "cancelled", jobID)
		writeJSON(w, http.StatusOK, j.Object())
	case StatusInProgress:
		writeError(w, errInProgress)
	default:
		// A finished job: delete its files now; the status is unchanged.
		if err := a.store.ExpireNow(ctx, jobID, a.now().UTC()); err != nil {
			a.log.Error.Printf("videos: expire %s: %v", jobID, err)
			writeError(w, errInternal)
			return
		}
		a.deleteFiles(ctx, job)
		a.logDeleted(r, id, "deleted", jobID)
		j, err := a.store.Get(ctx, jobID, id.OrgID, id.ResourceID)
		if err != nil {
			writeError(w, errInternal)
			return
		}
		writeJSON(w, http.StatusOK, j.Object())
	}
}

// logDeleted records which key cancelled a job or deleted its files: a
// DELETE destroys a customer's files, and nothing else records who sent it.
// The request id is the one the interceptor minted.
func (a *API) logDeleted(r *http.Request, id Handoff, what, jobID string) {
	a.log.Info.Printf("videos: %s %s org_id=%q resource_id=%q auth_id=%q retrieval=%v request_id=%q",
		what, jobID, id.OrgID, id.ResourceID, id.AuthID, id.Retrieval, r.Header.Get("X-Request-Id"))
}

// deleteFiles removes a job's objects and records it. A failure is logged and
// left for the reaper, which retries every expired job whose files remain.
func (a *API) deleteFiles(ctx context.Context, j *Job) {
	if err := DeleteJobFiles(ctx, a.blob, a.cfg.Prefix, j); err != nil {
		a.log.Warn.Printf("videos: delete files of %s (the reaper will retry): %v", j.ID, err)
		return
	}
	if err := a.store.MarkFilesDeleted(ctx, j.ID, a.now().UTC()); err != nil {
		a.log.Warn.Printf("videos: mark files deleted for %s: %v", j.ID, err)
	}
}

// DeleteJobFiles deletes every object a job may have stored, every
// attempt's files included (jobFileKeys).
func DeleteJobFiles(ctx context.Context, blob Blob, prefix string, j *Job) error {
	for _, key := range jobFileKeys(prefix, j) {
		if err := blob.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (a *API) content(w http.ResponseWriter, r *http.Request, id Handoff, jobID string) {
	q := r.URL.Query()
	for k := range q {
		if k != "variant" {
			writeError(w, badRequest(k, fmt.Sprintf("unknown query parameter %q", k)))
			return
		}
	}
	variant := q.Get("variant")
	if variant == "" {
		variant = "video"
	}
	if variant != "video" && variant != "thumbnail" {
		writeError(w, badRequest("variant", "variant must be video or thumbnail"))
		return
	}
	job := a.lookup(w, r, id, jobID)
	if job == nil {
		return
	}
	if job.Status != StatusCompleted {
		writeError(w, errNotReady)
		return
	}
	now := a.now()
	if job.FilesDeleted != nil || (job.ExpiresAt != nil && !now.Before(*job.ExpiresAt)) {
		writeError(w, errExpired)
		return
	}
	key, ctype, name := job.VideoKey, "video/mp4", job.ID+".mp4"
	if variant == "thumbnail" {
		if job.ThumbnailKey == "" {
			writeError(w, badRequest("variant", "this video has no thumbnail"))
			return
		}
		key, ctype, name = job.ThumbnailKey, "image/webp", job.ID+".webp"
	}
	body, size, err := a.blob.Get(r.Context(), key)
	if errors.Is(err, ErrBlobNotFound) {
		writeError(w, errExpired)
		return
	}
	if err != nil {
		a.log.Error.Printf("videos: read %s of %s: %v", variant, jobID, err)
		writeError(w, errInternal)
		return
	}
	defer body.Close()
	h := w.Header()
	h.Set("Content-Type", ctype)
	h.Set("Content-Length", strconv.FormatInt(size, 10))
	h.Set("Content-Disposition", `attachment; filename="`+name+`"`)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Cache-Control", "private, no-store")
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, body); err != nil {
		a.log.Warn.Printf("videos: stream %s of %s: %v", variant, jobID, err)
	}
}
