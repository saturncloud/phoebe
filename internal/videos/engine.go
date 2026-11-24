package videos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// Engine errors the dispatcher acts on.
var (
	// ErrEngineNotReady: the engine refused the connection (starting, or
	// scaled down). The attempt does not count.
	ErrEngineNotReady = errors.New("engine not ready")
	// ErrEngineGone: the connection broke after the request was sent (the pod
	// went away). Retried once.
	ErrEngineGone = errors.New("engine connection lost")
	// ErrOutputTooLarge: the video is over the output limit.
	ErrOutputTooLarge = errors.New("output too large")
)

// EngineHTTPError is a non-200 answer from the engine.
type EngineHTTPError struct {
	Status int
	Body   string
}

func (e *EngineHTTPError) Error() string {
	return fmt.Sprintf("engine answered %d: %s", e.Status, e.Body)
}

// engineMessage is what the customer sees for an engine failure: the status
// class only. The engine's own text can name internal paths and is logged,
// not returned.
func engineMessage(err error) string {
	var h *EngineHTTPError
	if errors.As(err, &h) {
		if h.Status >= 400 && h.Status < 500 {
			return fmt.Sprintf("the engine refused the request (HTTP %d)", h.Status)
		}
		return fmt.Sprintf("the engine failed to generate the video (HTTP %d)", h.Status)
	}
	return "the engine failed to generate the video"
}

// Engine generates one job's video into path.
type Engine interface {
	Generate(ctx context.Context, job *Job, input []byte, path string, maxBytes int64) (int64, error)
}

// OmniEngine calls vLLM-Omni's synchronous route, POST /v1/videos/sync, which
// answers with the MP4 bytes. The async route is not used: its job lives in
// the engine's memory (design 3.2).
type OmniEngine struct {
	Client *http.Client
}

func NewOmniEngine() *OmniEngine {
	return &OmniEngine{Client: &http.Client{Transport: &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 10 * 1e9}).DialContext,
		MaxIdleConnsPerHost: 4,
	}}}
}

// form renders the engine request. The engine takes multipart form fields;
// num_frames and fps come from the endpoint's limits (seconds*fps+1 frames).
func omniForm(job *Job, input []byte) (*bytes.Buffer, string, error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	w, h, err := ParseSize(job.Size)
	if err != nil {
		return nil, "", err
	}
	fields := [][2]string{
		{"prompt", deref(job.Prompt)},
		{"size", job.Size},
		{"width", strconv.Itoa(w)},
		{"height", strconv.Itoa(h)},
		{"num_frames", strconv.Itoa(job.Seconds*job.FPS + 1)},
		{"fps", strconv.Itoa(job.FPS)},
	}
	p := job.Params
	if p.NegativePrompt != nil {
		fields = append(fields, [2]string{"negative_prompt", *p.NegativePrompt})
	}
	if p.Seed != nil {
		fields = append(fields, [2]string{"seed", strconv.FormatInt(*p.Seed, 10)})
	}
	if p.NumInferenceSteps != nil {
		fields = append(fields, [2]string{"num_inference_steps", strconv.Itoa(*p.NumInferenceSteps)})
	}
	if p.GuidanceScale != nil {
		fields = append(fields, [2]string{"guidance_scale", strconv.FormatFloat(*p.GuidanceScale, 'f', -1, 64)})
	}
	for _, f := range fields {
		if err := mw.WriteField(f[0], f[1]); err != nil {
			return nil, "", err
		}
	}
	if input != nil {
		hdr := make(textproto.MIMEHeader)
		hdr.Set("Content-Disposition", `form-data; name="input_reference"; filename="input.`+imageTypes[p.InputContentType]+`"`)
		hdr.Set("Content-Type", p.InputContentType)
		part, err := mw.CreatePart(hdr)
		if err != nil {
			return nil, "", err
		}
		if _, err := part.Write(input); err != nil {
			return nil, "", err
		}
	}
	if err := mw.Close(); err != nil {
		return nil, "", err
	}
	return &buf, mw.FormDataContentType(), nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (e *OmniEngine) Generate(ctx context.Context, job *Job, input []byte, path string, maxBytes int64) (int64, error) {
	body, ctype, err := omniForm(job, input)
	if err != nil {
		return 0, err
	}
	upstream := job.Upstream
	if !strings.Contains(upstream, "://") {
		upstream = "http://" + upstream
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(upstream, "/")+"/v1/videos/sync", body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", ctype)
	req.Header.Set("Accept", "video/mp4")
	resp, err := e.Client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if isRefused(err) {
			return 0, fmt.Errorf("%w: %v", ErrEngineNotReady, err)
		}
		return 0, fmt.Errorf("%w: %v", ErrEngineGone, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusBadGateway {
		// The engine (or the pod's proxy) is up but not serving yet.
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return 0, fmt.Errorf("%w: HTTP %d: %s", ErrEngineNotReady, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return 0, &EngineHTTPError{Status: resp.StatusCode, Body: strings.TrimSpace(string(b))}
	}
	if resp.ContentLength > maxBytes {
		return 0, ErrOutputTooLarge
	}
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	n, copyErr := io.Copy(f, io.LimitReader(resp.Body, maxBytes+1))
	closeErr := f.Close()
	if copyErr != nil {
		if ctx.Err() != nil {
			return n, ctx.Err()
		}
		return n, fmt.Errorf("%w: %v", ErrEngineGone, copyErr)
	}
	if closeErr != nil {
		return n, closeErr
	}
	if n > maxBytes {
		return n, ErrOutputTooLarge
	}
	if resp.ContentLength >= 0 && n != resp.ContentLength {
		return n, fmt.Errorf("%w: got %d of %d bytes", ErrEngineGone, n, resp.ContentLength)
	}
	return n, nil
}

// isRefused reports a call that never connected to the engine: refused, a
// dial timeout or no route (a Service with no ready engine pod; on uk2 such a
// dial times out instead of being refused, issue #111), or a name that does
// not resolve yet. Nothing was sent, so the engine is not ready rather than
// gone, and the attempt does not count.
func isRefused(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

func readFull(r io.Reader, buf []byte) (int, error) { return io.ReadFull(r, buf) }
