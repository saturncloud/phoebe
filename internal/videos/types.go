// Package videos is Token Factory's video generation job system: the
// OpenAI-shaped /v1/videos API, the job store, the per-endpoint queue, the
// dispatcher that calls the engine, and output storage (video models design,
// approved 2026-11-18, sections 3 and 5-7).
//
// Saturn owns the job, not the engine. The engine (vLLM-Omni) keeps jobs in
// process memory and writes files to the pod's disk, so a restart or a second
// replica would lose or not know a job, and phoebe could not meter it. Here a
// job is a Postgres row (the source of truth), its id is queued on a Valkey
// stream per endpoint, a dispatcher calls the engine's synchronous route, and
// the output goes to object storage, read back only through the API with the
// customer's key.
//
// The service (cmd/videos) is reachable only from the phoebe interceptor, which
// has already checked the request's trusted identity; the interceptor hands the
// identity over in one internal header together with a shared token.
package videos

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Job statuses (the video object's status field; one-way door).
const (
	StatusQueued     = "queued"
	StatusInProgress = "in_progress"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
	StatusCancelled  = "cancelled"
)

// Error codes in a failed or cancelled job's error.code (one-way door).
const (
	CodeEngine         = "engine_error"
	CodeTimeout        = "timeout"
	CodeQueueTimeout   = "queue_timeout"
	CodeOutputTooLarge = "output_too_large"
	CodeCancelled      = "cancelled"
)

// Tasks.
const (
	TaskT2V = "t2v"
	TaskI2V = "i2v"
)

// crockford is the ULID alphabet.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// NewID returns "video_" followed by a 26-character ULID: 48 bits of
// millisecond time then 80 random bits, Crockford base32. Ids sort by creation
// time, and the random part makes them unguessable, which matters because a
// guessed id of another org's job must still answer 404 (it does: access is
// checked on the row), but an id should not leak how many jobs exist.
func NewID(now time.Time) (string, error) {
	var b [16]byte
	ms := uint64(now.UnixMilli())
	for i := 5; i >= 0; i-- {
		b[i] = byte(ms)
		ms >>= 8
	}
	if _, err := rand.Read(b[6:]); err != nil {
		return "", fmt.Errorf("video id: %w", err)
	}
	// 128 bits -> 26 base32 characters (the first carries 3 bits).
	var out [26]byte
	var acc uint64
	bits := 0
	// Walk the 128 bits from the most significant, 5 at a time, with the
	// first character taking the top 3 bits (26*5 = 130 = 128 + 2 padding).
	pos := 0
	acc = 0
	bits = 2 // two leading zero padding bits
	for _, by := range b {
		acc = acc<<8 | uint64(by)
		bits += 8
		for bits >= 5 {
			bits -= 5
			out[pos] = crockford[(acc>>uint(bits))&31]
			pos++
		}
	}
	return "video_" + string(out[:pos]), nil
}

// ValidID reports whether s has the shape of an id NewID makes. It is checked
// before any lookup so a malformed id never reaches the database.
func ValidID(s string) bool {
	rest, ok := strings.CutPrefix(s, "video_")
	if !ok || len(rest) != 26 {
		return false
	}
	for i := 0; i < len(rest); i++ {
		if !strings.ContainsRune(crockford, rune(rest[i])) {
			return false
		}
	}
	return rest[0] <= '7'
}

// DefaultJobsInFlight is the per-org limit of queued or running jobs on one
// endpoint when Atlas renders none (design 5.4).
const DefaultJobsInFlight = 20

// Limits is what one video endpoint accepts, rendered by Atlas from the model
// catalog and the org's usage limits into the trusted X-Saturn-Video-Limits
// header on the endpoint's route. Its presence is what marks a route as a
// video endpoint.
type Limits struct {
	// Tasks the model serves: "t2v", "i2v" or both.
	Tasks []string `json:"tasks"`
	// Sizes "WxH"; the first is the default.
	Sizes []string `json:"sizes"`
	// Seconds the clip may last; the first is the default.
	Seconds []int `json:"seconds"`
	// FPS of the model's output; the engine is asked for seconds*fps+1 frames
	// (Wan's frame counts are 4k+1: 5 s at 16 fps is 81 frames).
	FPS int `json:"fps"`
	// MaxSteps caps num_inference_steps.
	MaxSteps int `json:"max_steps"`
	// MaxConcurrency is the jobs the dispatcher sends to the endpoint at once.
	MaxConcurrency int `json:"max_concurrency"`
	// JobsInFlight is the org's limit of queued or running jobs on this
	// endpoint (UsageLimits.token_factory_video_jobs_in_flight); 0 means the
	// default.
	JobsInFlight int `json:"jobs_in_flight"`
	// GPUs the endpoint's engine holds, for gpu_seconds.
	GPUs int `json:"gpus"`
}

// ParseLimits parses and checks the header value. A route whose limits do not
// parse is refused (fail closed): guessing what the endpoint accepts could
// send the engine a request it cannot run, or bill the wrong quantities.
func ParseLimits(raw string) (Limits, error) {
	var l Limits
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&l); err != nil {
		return Limits{}, fmt.Errorf("video limits: %w", err)
	}
	if len(l.Tasks) == 0 || len(l.Sizes) == 0 || len(l.Seconds) == 0 {
		return Limits{}, fmt.Errorf("video limits: tasks, sizes and seconds are required")
	}
	for _, t := range l.Tasks {
		if t != TaskT2V && t != TaskI2V {
			return Limits{}, fmt.Errorf("video limits: unknown task %q", t)
		}
	}
	for _, s := range l.Sizes {
		if _, _, err := ParseSize(s); err != nil {
			return Limits{}, fmt.Errorf("video limits: %w", err)
		}
	}
	for _, s := range l.Seconds {
		if s <= 0 || s > 600 {
			return Limits{}, fmt.Errorf("video limits: seconds %d out of range", s)
		}
	}
	if l.FPS <= 0 || l.FPS > 120 {
		return Limits{}, fmt.Errorf("video limits: fps %d out of range", l.FPS)
	}
	if l.MaxSteps <= 0 || l.MaxConcurrency <= 0 || l.GPUs <= 0 || l.JobsInFlight < 0 {
		return Limits{}, fmt.Errorf("video limits: max_steps, max_concurrency and gpus must be positive")
	}
	if l.JobsInFlight == 0 {
		l.JobsInFlight = DefaultJobsInFlight
	}
	return l, nil
}

// HasTask reports whether the endpoint serves task.
func (l Limits) HasTask(task string) bool {
	for _, t := range l.Tasks {
		if t == task {
			return true
		}
	}
	return false
}

// ParseSize parses "WxH".
func ParseSize(s string) (w, h int, err error) {
	ws, hs, ok := strings.Cut(s, "x")
	if !ok {
		return 0, 0, fmt.Errorf("size %q is not WxH", s)
	}
	w, err1 := strconv.Atoi(ws)
	h, err2 := strconv.Atoi(hs)
	if err1 != nil || err2 != nil || w <= 0 || h <= 0 || w > 8192 || h > 8192 {
		return 0, 0, fmt.Errorf("size %q is not WxH", s)
	}
	return w, h, nil
}

// Resolution buckets a size by its short side (design 7.1; a one-way-door
// attribute): 480p up to 480, 720p up to 720, 1080p up to 1080, 4k above.
func Resolution(size string) (string, error) {
	w, h, err := ParseSize(size)
	if err != nil {
		return "", err
	}
	short := min(w, h)
	switch {
	case short <= 480:
		return "480p", nil
	case short <= 720:
		return "720p", nil
	case short <= 1080:
		return "1080p", nil
	default:
		return "4k", nil
	}
}

// Frames is the frame count the engine is asked for.
func (l Limits) Frames(seconds int) int { return seconds*l.FPS + 1 }
