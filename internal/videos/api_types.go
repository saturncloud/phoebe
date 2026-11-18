package videos

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// APIError is an error the API answers with its own status and an
// OpenAI-shaped body. Each error knows its status, so handlers never pick a
// status code by hand.
type APIError struct {
	Status  int
	Code    string
	Message string
	Param   string
	// RetryAfter, when set, is sent as the Retry-After header (seconds).
	RetryAfter int
}

func (e *APIError) Error() string { return e.Message }

func badRequest(param, msg string) *APIError {
	return &APIError{Status: http.StatusBadRequest, Code: "invalid_request", Message: msg, Param: param}
}

var (
	errNotFound = &APIError{Status: http.StatusNotFound, Code: "not_found", Message: "video not found"}
	errNotReady = &APIError{Status: http.StatusConflict, Code: "video_not_ready",
		Message: "the video is not ready: its status is not completed"}
	errInProgress = &APIError{Status: http.StatusConflict, Code: "video_not_ready",
		Message: "the video is being generated and cannot be cancelled now; try again when it has finished"}
	errExpired = &APIError{Status: http.StatusGone, Code: "video_expired",
		Message: "the video's files have expired and were deleted"}
	errOrgLimit = &APIError{Status: http.StatusTooManyRequests, Code: "too_many_video_jobs",
		Message: "your organization has too many video jobs queued or running on this endpoint", RetryAfter: 30}
	errQueueFull = &APIError{Status: http.StatusServiceUnavailable, Code: "video_queue_full",
		Message: "this endpoint's video queue is full", RetryAfter: 30}
	errQueueDown = &APIError{Status: http.StatusServiceUnavailable, Code: "video_queue_unavailable",
		Message: "the video queue is unavailable; try again shortly", RetryAfter: 10}
	errInternal = &APIError{Status: http.StatusInternalServerError, Code: "internal_error",
		Message: "internal error"}
)

func writeError(w http.ResponseWriter, e *APIError) {
	if e.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(e.RetryAfter))
	}
	typ := "invalid_request_error"
	if e.Status >= 500 {
		typ = "server_error"
	}
	body := map[string]any{"error": map[string]any{
		"message": e.Message, "type": typ, "code": e.Code, "param": nilIfEmpty(e.Param),
	}}
	writeJSON(w, e.Status, body)
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Job is one video_job row.
type Job struct {
	ID             string
	OrgID          string
	AuthID         string
	UserID         string
	GroupID        string
	ResourceID     string
	ServedModel    string
	BaseModel      string
	ServingMode    string
	Upstream       string
	Region         string
	Task           string
	Prompt         *string
	Seconds        int
	Size           string
	FPS            int
	GPUs           int
	MaxConcurrency int
	Params         Params
	Status         string
	Progress       int
	ErrorCode      string
	ErrorMessage   string
	Attempts       int
	EnqueuedAt     time.Time
	CreatedAt      time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
	ExpiresAt      *time.Time
	FilesDeleted   *time.Time
	InputKey       string
	VideoKey       string
	ThumbnailKey   string
	VideoBytes     int64
	OutputFrames   int
	OutputSeconds  string // exact decimal, as stored
}

// enqueuedNever reports a job the reaper must put back on its stream: the
// lease expiry requeued it in Postgres only (enqueued_at = epoch).
func (j *Job) enqueuedNever() bool { return j.EnqueuedAt.Year() < 2000 }

// Params are the optional generation knobs the API passes to the engine.
type Params struct {
	NegativePrompt    *string  `json:"negative_prompt,omitempty"`
	Seed              *int64   `json:"seed,omitempty"`
	NumInferenceSteps *int     `json:"num_inference_steps,omitempty"`
	GuidanceScale     *float64 `json:"guidance_scale,omitempty"`
	// InputContentType is the stored input image's media type (i2v).
	InputContentType string `json:"input_content_type,omitempty"`
}

// Object is the video object (design 3.5; a one-way door): OpenAI's fields
// plus Saturn's usage block.
type Object struct {
	ID          string       `json:"id"`
	Object      string       `json:"object"`
	Status      string       `json:"status"`
	Progress    int          `json:"progress"`
	CreatedAt   int64        `json:"created_at"`
	CompletedAt *int64       `json:"completed_at"`
	ExpiresAt   *int64       `json:"expires_at"`
	Model       string       `json:"model"`
	Prompt      *string      `json:"prompt"`
	Seconds     string       `json:"seconds"`
	Size        string       `json:"size"`
	Error       *ObjectError `json:"error"`
	Usage       *ObjectUsage `json:"usage,omitempty"`
}

type ObjectError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ObjectUsage struct {
	OutputSeconds string `json:"output_seconds"`
	OutputFrames  int    `json:"output_frames"`
}

func unixPtr(t *time.Time) *int64 {
	if t == nil {
		return nil
	}
	u := t.Unix()
	return &u
}

// Object renders the job for the API.
func (j *Job) Object() Object {
	o := Object{
		ID:          j.ID,
		Object:      "video",
		Status:      j.Status,
		Progress:    j.Progress,
		CreatedAt:   j.CreatedAt.Unix(),
		CompletedAt: unixPtr(j.CompletedAt),
		ExpiresAt:   unixPtr(j.ExpiresAt),
		Model:       j.ServedModel,
		Prompt:      j.Prompt,
		Seconds:     strconv.Itoa(j.Seconds),
		Size:        j.Size,
	}
	if j.ErrorCode != "" {
		o.Error = &ObjectError{Code: j.ErrorCode, Message: j.ErrorMessage}
	}
	if j.Status == StatusCompleted {
		o.Usage = &ObjectUsage{OutputSeconds: trimDecimal(j.OutputSeconds), OutputFrames: j.OutputFrames}
	}
	return o
}

// trimDecimal drops trailing fractional zeros ("5.062500000" -> "5.0625",
// "5.000000000" -> "5"), the shortest exact form.
func trimDecimal(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}
