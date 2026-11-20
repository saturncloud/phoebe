package videos

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/saturncloud/phoebe/internal/atlastoken"
)

// Blob is the object store the outputs live in: the install's Token Factory
// bucket (TF_ADAPTER_STORE_BUCKET) under the video prefix (design 6.1).
// Objects are only ever read back through the API, streamed by this service
// with the customer's key checked; no URL to a customer's file ever leaves it.
//
// Every job is registered before any of its files is written, and closed when
// it finishes, so the store can refuse uploads for a job that is not in
// progress. Closing a completed job names the attempt whose files are its
// output; Discard (a superseded attempt removing its own files) is refused for
// that attempt.
type Blob interface {
	Register(ctx context.Context, orgID, jobID, resourceID, inputExt string) error
	// Close ends the job's grant. outputAttempt is the attempt whose files are
	// a completed job's output, or 0 when the job did not complete.
	Close(ctx context.Context, orgID, jobID string, outputAttempt int) error
	Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
	Get(ctx context.Context, key string) (io.ReadCloser, int64, error)
	Delete(ctx context.Context, key string) error
	// Discard deletes a file of a superseded attempt. Unlike Delete it is
	// refused for the attempt recorded as the job's output.
	Discard(ctx context.Context, key string) error
}

// ErrBlobNotFound is Get on a missing object, or on one whose job's files can
// no longer exist.
var ErrBlobNotFound = errors.New("object not found")

// Keys lays out one job's objects. The input image, written once when the job
// is created, is <prefix>/<org_id>/<video_id>/input.<ext>. Each attempt writes
// its output under its own keys, <prefix>/<org_id>/<video_id>/attempt-<n>/
// video.mp4 and thumbnail.webp, so an attempt whose lease was taken over can
// neither overwrite nor delete the output of the attempt that completed the
// job, even with an upload URL signed before the job closed. The org id in the
// key makes per-org deletion and auditing a prefix operation.
//
// The layout is internal to phoebe-videos and Atlas (saturn
// pdc/models/tf_video_job.py), which signs only keys of this shape. The job
// row records the completed attempt's keys (video_key, thumbnail_key), which
// are the only ones the API reads.
type Keys struct{ Prefix string }

// MaxAttempt is the largest attempt number a key may carry (Atlas refuses a
// larger one). The dispatcher runs a job at most twice.
const MaxAttempt = 999

func (k Keys) Input(orgID, jobID, ext string) string { return k.join(orgID, jobID, "input."+ext) }
func (k Keys) Video(orgID, jobID string, attempt int) string {
	return k.join(orgID, jobID, attemptSegment(attempt)+"/video.mp4")
}
func (k Keys) Thumbnail(orgID, jobID string, attempt int) string {
	return k.join(orgID, jobID, attemptSegment(attempt)+"/thumbnail.webp")
}
func (k Keys) join(orgID, jobID, name string) string {
	return strings.Trim(k.Prefix, "/") + "/" + orgID + "/" + jobID + "/" + name
}

func attemptSegment(attempt int) string { return "attempt-" + strconv.Itoa(attempt) }

// validAttemptSegment is "attempt-<n>" with n from 1 to MaxAttempt and no
// leading zero, matching Atlas.
func validAttemptSegment(s string) bool {
	n, ok := strings.CutPrefix(s, "attempt-")
	if !ok || n == "" || n[0] == '0' || len(n) > 3 {
		return false
	}
	for _, c := range n {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// errNotVideoKey is Parse refusing a key: no retry can make it valid.
var errNotVideoKey = errors.New("not a video job object")

// Parse splits a key this layout made into the job's org and id. Anything
// else is refused before Atlas is asked (Atlas refuses it too).
func (k Keys) Parse(key string) (orgID, jobID string, err error) {
	rest, ok := strings.CutPrefix(key, strings.Trim(k.Prefix, "/")+"/")
	parts := strings.Split(rest, "/")
	valid := ok && (len(parts) == 3 || (len(parts) == 4 && validAttemptSegment(parts[2])))
	if !valid || parts[0] == "" || !ValidID(parts[1]) || parts[len(parts)-1] == "" {
		return "", "", fmt.Errorf("video store: key %q is %w", key, errNotVideoKey)
	}
	return parts[0], parts[1], nil
}

// The internal routes Atlas serves phoebe-videos (saturn
// pdc/routes/token_factory_internal.py). CONTRACT with Atlas.
const atlasVideoJobsPath = "/api/internal/token-factory/video-jobs"

// AtlasBlob is Blob without a store credential: Atlas signs a presigned URL
// for one exact key of one registered job, immediately before each request
// (design 6.1; the way SaturnFS works). Atlas checks the org, the job, the
// key's layout and, for an upload, that the job is in progress and the length
// and content type it signs.
type AtlasBlob struct {
	baseURL string
	// tokens gives phoebe-videos' Atlas service token
	// (tf-internal:video-store) for each call.
	tokens atlastoken.Source
	keys   Keys
	// atlas calls Atlas; store sends the signed requests. Neither uses an
	// ambient HTTP proxy from the pod environment (the token and the URLs are
	// credentials), and neither follows a redirect: a redirect from Atlas
	// would carry the token to another host, and one from the store would
	// send a signed request somewhere it was not signed for.
	atlas *http.Client
	store *http.Client
	now   func() time.Time
	// readTimeout is how long one Read of a Get body may wait for the store
	// to send something (storeResponseHeaderTimeout; tests shorten it).
	readTimeout time.Duration
}

// NewAtlasBlob checks the Atlas URL and the token source. Both are
// required: with either missing the service must not start.
func NewAtlasBlob(baseURL string, tokens atlastoken.Source, prefix string) (*AtlasBlob, error) {
	if tokens == nil {
		return nil, errors.New("video store: no Atlas service token source")
	}
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
		u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, fmt.Errorf("video store: invalid Atlas URL %q", baseURL)
	}
	if err := atlastoken.CheckAtlasURL(baseURL); err != nil {
		return nil, fmt.Errorf("video store: %w", err)
	}
	if strings.Trim(prefix, "/") == "" {
		return nil, errors.New("video store: the key prefix is empty")
	}
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	atlasTransport := http.DefaultTransport.(*http.Transport).Clone()
	atlasTransport.Proxy = nil
	// The store client has no whole-request timeout (a 5 GiB upload takes as
	// long as it takes; the caller's context bounds it), so a store that
	// accepts the request and never answers is cut off by
	// ResponseHeaderTimeout instead. DisableCompression keeps the transport
	// from asking for gzip, so a Get's Content-Length is the object's length.
	storeTransport := http.DefaultTransport.(*http.Transport).Clone()
	storeTransport.Proxy = nil
	storeTransport.ResponseHeaderTimeout = storeResponseHeaderTimeout
	storeTransport.DisableCompression = true
	return &AtlasBlob{
		baseURL:     strings.TrimRight(baseURL, "/"),
		tokens:      tokens,
		keys:        Keys{Prefix: prefix},
		atlas:       &http.Client{Transport: atlasTransport, Timeout: 30 * time.Second, CheckRedirect: noRedirect},
		store:       &http.Client{Transport: storeTransport, CheckRedirect: noRedirect},
		now:         time.Now,
		readTimeout: storeResponseHeaderTimeout,
	}, nil
}

// storeResponseHeaderTimeout is how long the store may take to start
// answering a request it has received (for an upload, after the last byte),
// and how long a download may wait for the next bytes of the body.
const storeResponseHeaderTimeout = 60 * time.Second

// scrubURLError removes the query and fragment from the URL a transport error
// names. A presigned URL's query holds its signature (X-Amz-Signature), which
// must not reach a log; the scheme, host and path are enough to debug.
func scrubURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		if u, perr := url.Parse(ue.URL); perr == nil {
			ue.URL = (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
		} else {
			ue.URL = "(unparseable URL)"
		}
	}
	return err
}

// storageRefusedPermanently reports whether err is Atlas refusing to sign a
// request in a way that sending the same request again cannot change (a bad
// request, a key or job Atlas will not sign for, a job no longer in progress,
// a length over the store's limit), and with what status. Transport errors,
// Atlas 5xx and the store's own answers are not permanent.
func storageRefusedPermanently(err error) (status int, ok bool) {
	var ae *atlasError
	if !errors.As(err, &ae) {
		return 0, false
	}
	switch ae.status {
	case http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound,
		http.StatusConflict, http.StatusRequestEntityTooLarge:
		return ae.status, true
	}
	return 0, false
}

// atlasError is a refusal from Atlas, with its status.
type atlasError struct {
	status int
	msg    string
}

func (e *atlasError) Error() string {
	return fmt.Sprintf("video store: Atlas answered %d: %s", e.status, e.msg)
}

func (b *AtlasBlob) call(ctx context.Context, path string, body any, want int, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	// Never sent without a usable token. The error is not an *atlasError, so
	// callers treat it as temporary (the job is retried, not failed): Atlas's
	// job may write a new token any minute.
	token, err := b.tokens.Token()
	if err != nil {
		return fmt.Errorf("video store: %w", err)
	}
	req.Header.Set("Authorization", "token "+token)
	resp, err := b.atlas.Do(req)
	if err != nil {
		return fmt.Errorf("video store: Atlas: %w", scrubURLError(err))
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode != want {
		var e struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(data, &e)
		return &atlasError{status: resp.StatusCode, msg: e.Message}
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("video store: Atlas answer: %w", err)
		}
	}
	return nil
}

func (b *AtlasBlob) Register(ctx context.Context, orgID, jobID, resourceID, inputExt string) error {
	body := map[string]any{"video_id": jobID, "org_id": orgID, "resource_id": resourceID, "input_ext": nil}
	if inputExt != "" {
		body["input_ext"] = inputExt
	}
	return b.call(ctx, atlasVideoJobsPath, body, http.StatusCreated, nil)
}

func (b *AtlasBlob) Close(ctx context.Context, orgID, jobID string, outputAttempt int) error {
	body := map[string]any{"org_id": orgID}
	if outputAttempt > 0 {
		body["output_attempt"] = outputAttempt
	}
	return b.call(ctx, atlasVideoJobsPath+"/"+jobID+"/close", body, http.StatusOK, nil)
}

// presigned is Atlas's answer: one request to send as is.
type presigned struct {
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	ExpiresAt time.Time         `json:"expires_at"`
	Headers   map[string]string `json:"headers"`
}

// errURLExpired is a URL Atlas signed that has already expired here (a clock
// far behind Atlas's, or a stalled call). It is never sent.
var errURLExpired = errors.New("video store: the presigned URL expired before it was used")

func (b *AtlasBlob) presign(ctx context.Context, key, method string, size int64, discard bool) (*http.Request, error) {
	orgID, jobID, err := b.keys.Parse(key)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"org_id": orgID, "key": key, "method": method}
	if method == http.MethodPut {
		body["content_length"] = size
	}
	if discard {
		body["discard"] = true
	}
	var p presigned
	if err := b.call(ctx, atlasVideoJobsPath+"/"+jobID+"/object-urls", body, http.StatusOK, &p); err != nil {
		return nil, err
	}
	if p.Method != method {
		return nil, fmt.Errorf("video store: Atlas signed %q, not %q", p.Method, method)
	}
	if !b.now().Before(p.ExpiresAt) {
		return nil, errURLExpired
	}
	u, err := url.Parse(p.URL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return nil, errors.New("video store: Atlas signed an invalid URL")
	}
	req, err := http.NewRequestWithContext(ctx, method, p.URL, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range p.Headers {
		if strings.EqualFold(k, "Content-Length") {
			continue // set as req.ContentLength by Put, and checked there
		}
		req.Header.Set(k, v)
	}
	if method == http.MethodPut && p.Headers["Content-Length"] != strconv.FormatInt(size, 10) {
		return nil, fmt.Errorf("video store: Atlas signed length %q, not %d", p.Headers["Content-Length"], size)
	}
	return req, nil
}

// storeError reads a store refusal's code ("AccessDenied: Request has
// expired", "SignatureDoesNotMatch") for the log.
func storeError(op, key string, resp *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	var e struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	detail := strings.TrimSpace(string(data))
	if xml.Unmarshal(data, &e) == nil && e.Code != "" {
		detail = e.Code + ": " + e.Message
	}
	return fmt.Errorf("video store: %s %s: store answered %d: %s", op, key, resp.StatusCode, detail)
}

func (b *AtlasBlob) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	req, err := b.presign(ctx, key, http.MethodPut, size, false)
	if err != nil {
		return err
	}
	if got := req.Header.Get("Content-Type"); got != contentType {
		return fmt.Errorf("video store: Atlas signed %s as %q, not %q", key, got, contentType)
	}
	req.Body = io.NopCloser(r)
	req.ContentLength = size
	resp, err := b.store.Do(req)
	if err != nil {
		return fmt.Errorf("video store: put %s: %w", key, scrubURLError(err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return storeError("put", key, resp)
	}
	return nil
}

// Get opens the object. Its status is known before any byte of the body, so
// a refused read fails before the API has written a 200. A job whose files
// can no longer exist (Atlas 409), a job Atlas no longer has (Atlas 404: its
// row was pruned) or an object that is gone reads as ErrBlobNotFound. A 403
// stays an error: Atlas also answers 403 to a wrong or revoked service token.
//
// The body is cut off when one Read waits longer than readTimeout: a store
// that sends its headers and then stops would otherwise hold the download
// open with nothing written, which the interceptor's stall guard never sees.
func (b *AtlasBlob) Get(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	req, err := b.presign(ctx, key, http.MethodGet, 0, false)
	var ae *atlasError
	if errors.As(err, &ae) && (ae.status == http.StatusConflict || ae.status == http.StatusNotFound) {
		return nil, 0, ErrBlobNotFound
	}
	if err != nil {
		return nil, 0, err
	}
	rctx, cancel := context.WithCancel(ctx)
	resp, err := b.store.Do(req.WithContext(rctx))
	if err != nil {
		cancel()
		return nil, 0, fmt.Errorf("video store: get %s: %w", key, scrubURLError(err))
	}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		resp.Body.Close()
		cancel()
		return nil, 0, ErrBlobNotFound
	case resp.StatusCode != http.StatusOK:
		defer cancel()
		defer resp.Body.Close()
		return nil, 0, storeError("get", key, resp)
	case resp.ContentLength < 0:
		resp.Body.Close()
		cancel()
		return nil, 0, fmt.Errorf("video store: get %s: the store sent no length", key)
	}
	return newStallCutBody(resp.Body, key, b.readTimeout, cancel), resp.ContentLength, nil
}

// stallCutBody is a Get body whose every Read must make progress within
// timeout. On expiry it cancels the store request, which unblocks the Read,
// and returns an error that says the store stalled.
type stallCutBody struct {
	body    io.ReadCloser
	key     string
	timeout time.Duration
	cancel  context.CancelFunc
	timer   *time.Timer
	stalled atomic.Bool
}

func newStallCutBody(body io.ReadCloser, key string, timeout time.Duration, cancel context.CancelFunc) *stallCutBody {
	s := &stallCutBody{body: body, key: key, timeout: timeout, cancel: cancel}
	s.timer = time.AfterFunc(timeout, func() {
		s.stalled.Store(true)
		cancel()
	})
	s.timer.Stop()
	return s
}

func (s *stallCutBody) Read(p []byte) (int, error) {
	if s.stalled.Load() {
		return 0, s.stallError()
	}
	s.timer.Reset(s.timeout)
	n, err := s.body.Read(p)
	s.timer.Stop()
	if s.stalled.Load() {
		return n, s.stallError()
	}
	return n, err
}

func (s *stallCutBody) stallError() error {
	return fmt.Errorf("video store: get %s: the store sent nothing for %s", s.key, s.timeout)
}

func (s *stallCutBody) Close() error {
	s.timer.Stop()
	err := s.body.Close()
	s.cancel()
	return err
}

func (b *AtlasBlob) Delete(ctx context.Context, key string) error {
	return b.delete(ctx, key, false)
}

// Discard deletes a superseded attempt's file. Atlas refuses it (409) for the
// attempt recorded as the job's output.
func (b *AtlasBlob) Discard(ctx context.Context, key string) error {
	return b.delete(ctx, key, true)
}

func (b *AtlasBlob) delete(ctx context.Context, key string, discard bool) error {
	req, err := b.presign(ctx, key, http.MethodDelete, 0, discard)
	if err != nil {
		return err
	}
	resp, err := b.store.Do(req)
	if err != nil {
		return fmt.Errorf("video store: delete %s: %w", key, scrubURLError(err))
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		return nil
	}
	return storeError("delete", key, resp)
}
