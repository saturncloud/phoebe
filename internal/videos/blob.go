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
	"time"
)

// Blob is the object store the outputs live in: the install's Token Factory
// bucket (TF_ADAPTER_STORE_BUCKET) under the video prefix (design 6.1).
// Objects are only ever read back through the API, streamed by this service
// with the customer's key checked; no URL to a customer's file ever leaves it.
//
// Every job is registered before any of its files is written, and closed when
// it finishes, so the store can refuse uploads for a job that is not in
// progress.
type Blob interface {
	Register(ctx context.Context, orgID, jobID, resourceID, inputExt string) error
	Close(ctx context.Context, orgID, jobID string) error
	Put(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
	Get(ctx context.Context, key string) (io.ReadCloser, int64, error)
	Delete(ctx context.Context, key string) error
}

// ErrBlobNotFound is Get on a missing object, or on one whose job's files can
// no longer exist.
var ErrBlobNotFound = errors.New("object not found")

// Keys lays out one job's objects: <prefix>/<org_id>/<video_id>/<name>. The
// org id in the key makes per-org deletion and auditing a prefix operation.
type Keys struct{ Prefix string }

func (k Keys) Input(orgID, jobID, ext string) string { return k.join(orgID, jobID, "input."+ext) }
func (k Keys) Video(orgID, jobID string) string      { return k.join(orgID, jobID, "video.mp4") }
func (k Keys) Thumbnail(orgID, jobID string) string  { return k.join(orgID, jobID, "thumbnail.webp") }
func (k Keys) join(orgID, jobID, name string) string {
	return strings.Trim(k.Prefix, "/") + "/" + orgID + "/" + jobID + "/" + name
}

// Parse splits a key this layout made into the job's org and id. Anything
// else is refused before Atlas is asked (Atlas refuses it too).
func (k Keys) Parse(key string) (orgID, jobID string, err error) {
	rest, ok := strings.CutPrefix(key, strings.Trim(k.Prefix, "/")+"/")
	parts := strings.Split(rest, "/")
	if !ok || len(parts) != 3 || parts[0] == "" || !ValidID(parts[1]) || parts[2] == "" {
		return "", "", fmt.Errorf("video store: key %q is not a video job object", key)
	}
	return parts[0], parts[1], nil
}

// The internal routes Atlas serves phoebe-videos (saturn
// pdc/routes/token_factory_internal.py). CONTRACT with Atlas.
const (
	atlasVideoJobsPath    = "/api/internal/token-factory/video-jobs"
	atlasVideoStoreHeader = "X-Saturn-Internal-Video-Store-Token"
)

// AtlasBlob is Blob without a store credential: Atlas signs a presigned URL
// for one exact key of one registered job, immediately before each request
// (design 6.1; the way SaturnFS works). Atlas checks the org, the job, the
// key's layout and, for an upload, that the job is in progress and the length
// and content type it signs.
type AtlasBlob struct {
	baseURL string
	token   string
	keys    Keys
	// atlas calls Atlas; store sends the signed requests. Neither uses an
	// ambient HTTP proxy from the pod environment (the token and the URLs are
	// credentials), and the store client never follows a redirect.
	atlas *http.Client
	store *http.Client
	now   func() time.Time
}

// NewAtlasBlob checks the Atlas URL and the token. Both are required: with
// either missing the service must not start.
func NewAtlasBlob(baseURL, token, prefix string) (*AtlasBlob, error) {
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("video store: the internal video store token is empty")
	}
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
		u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, fmt.Errorf("video store: invalid Atlas URL %q", baseURL)
	}
	if strings.Trim(prefix, "/") == "" {
		return nil, errors.New("video store: the key prefix is empty")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &AtlasBlob{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		keys:    Keys{Prefix: prefix},
		atlas:   &http.Client{Transport: transport, Timeout: 30 * time.Second},
		store: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}},
		now: time.Now,
	}, nil
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
	req.Header.Set(atlasVideoStoreHeader, b.token)
	resp, err := b.atlas.Do(req)
	if err != nil {
		return fmt.Errorf("video store: Atlas: %w", err)
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

func (b *AtlasBlob) Close(ctx context.Context, orgID, jobID string) error {
	return b.call(ctx, atlasVideoJobsPath+"/"+jobID+"/close", map[string]any{"org_id": orgID}, http.StatusOK, nil)
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

func (b *AtlasBlob) presign(ctx context.Context, key, method string, size int64) (*http.Request, error) {
	orgID, jobID, err := b.keys.Parse(key)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"org_id": orgID, "key": key, "method": method}
	if method == http.MethodPut {
		body["content_length"] = size
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
	req, err := b.presign(ctx, key, http.MethodPut, size)
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
		return fmt.Errorf("video store: put %s: %w", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return storeError("put", key, resp)
	}
	return nil
}

// Get opens the object. Its status is known before any byte of the body, so
// a refused read fails before the API has written a 200. A job whose files
// can no longer exist (Atlas 409) or whose object is gone reads as
// ErrBlobNotFound.
func (b *AtlasBlob) Get(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	req, err := b.presign(ctx, key, http.MethodGet, 0)
	var ae *atlasError
	if errors.As(err, &ae) && ae.status == http.StatusConflict {
		return nil, 0, ErrBlobNotFound
	}
	if err != nil {
		return nil, 0, err
	}
	resp, err := b.store.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("video store: get %s: %w", key, err)
	}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		resp.Body.Close()
		return nil, 0, ErrBlobNotFound
	case resp.StatusCode != http.StatusOK:
		defer resp.Body.Close()
		return nil, 0, storeError("get", key, resp)
	case resp.ContentLength < 0:
		resp.Body.Close()
		return nil, 0, fmt.Errorf("video store: get %s: the store sent no length", key)
	}
	return resp.Body, resp.ContentLength, nil
}

func (b *AtlasBlob) Delete(ctx context.Context, key string) error {
	req, err := b.presign(ctx, key, http.MethodDelete, 0)
	if err != nil {
		return err
	}
	resp, err := b.store.Do(req)
	if err != nil {
		return fmt.Errorf("video store: delete %s: %w", key, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		return nil
	}
	return storeError("delete", key, resp)
}
