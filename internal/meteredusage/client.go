package meteredusage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Retry policy defaults. A run pushes up to 24 hours one after another, so the
// worst case per hour (about 1+2+4+8 = 15 s of waiting plus request time) stays
// well inside an hourly CronJob even if every hour exhausts its attempts.
const (
	DefaultMaxAttempts = 5
	DefaultBaseBackoff = time.Second
	DefaultMaxBackoff  = 30 * time.Second
)

// maxResponseBody bounds how much of a response is read, so a misbehaving
// server cannot exhaust the pod's memory.
const maxResponseBody = 64 << 10

// RejectedError is a response the manager will give again for the same
// request: 4xx. It is never retried. wire-conventions.md names the ones that
// matter: 400 (the request broke a wire rule; nothing stored), 401 (bad or
// missing token), 409 (the install is recorded on the legacy billing path),
// 422 (window_start is in a billing period already settled to Stripe; the
// correction goes to the human credit path). Each needs a person, not a
// retry.
type RejectedError struct {
	Status int
	Body   string
}

func (e *RejectedError) Error() string {
	return fmt.Sprintf("manager rejected the request with %d (not retried): %s", e.Status, e.Body)
}

// TransientError is a 5xx or network failure that persisted through every
// attempt. The hour is safe to re-send later (the request is a snapshot), and
// the next run's trailing window will do so.
type TransientError struct {
	Attempts int
	Last     error
}

func (e *TransientError) Error() string {
	return fmt.Sprintf("gave up after %d attempts: %v", e.Attempts, e.Last)
}

func (e *TransientError) Unwrap() error { return e.Last }

// Client posts metered-usage requests to the manager.
type Client struct {
	// BaseURL is the manager's base URL; Path is appended.
	BaseURL string
	// Token is the install's existing customer auth token, sent exactly as the
	// install already sends it to /customer/usage: "Authorization: token <t>".
	// No new credential.
	Token string
	HTTP  *http.Client

	// MaxAttempts, BaseBackoff and MaxBackoff default to the Default* values
	// when zero.
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// Sleep waits between attempts; nil means a context-aware time.Sleep.
	// Tests replace it to run instantly.
	Sleep func(ctx context.Context, d time.Duration) error
}

// Post sends one hour. It returns the manager's 200 body, a *RejectedError
// (4xx, not retried), a *TransientError (5xx or network, retried with
// exponential backoff until MaxAttempts), or another error (the request could
// not be built or the 200 body did not match the contract).
//
// Retrying is safe because a request is a snapshot keyed by usage_id: the
// manager applies it in one transaction, so a re-send after an ambiguous
// failure (for instance a timeout after the manager committed) replaces the
// hour with the same content.
func (c *Client) Post(ctx context.Context, req Request) (Response, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return Response{}, fmt.Errorf("marshal request: %w", err)
	}
	url := strings.TrimRight(c.BaseURL, "/") + Path

	attempts := c.MaxAttempts
	if attempts <= 0 {
		attempts = DefaultMaxAttempts
	}
	var last error
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			if err := c.sleep(ctx, c.backoff(attempt-1)); err != nil {
				return Response{}, &TransientError{Attempts: attempt - 1, Last: last}
			}
		}
		resp, err := c.once(ctx, url, body)
		if err == nil {
			if err := checkResponse(req, resp); err != nil {
				return resp, err
			}
			return resp, nil
		}
		var rejected *RejectedError
		if errors.As(err, &rejected) {
			return Response{}, err
		}
		var malformed *malformedResponseError
		if errors.As(err, &malformed) {
			return Response{}, err
		}
		last = err
		if ctx.Err() != nil {
			return Response{}, &TransientError{Attempts: attempt, Last: last}
		}
	}
	return Response{}, &TransientError{Attempts: attempts, Last: last}
}

// malformedResponseError is a 200 whose body is not the contract's. The
// manager committed something, so retrying would not tell us more.
type malformedResponseError struct{ msg string }

func (e *malformedResponseError) Error() string { return e.msg }

// once makes a single attempt. A network error or a 5xx comes back as a plain
// error (retryable); a 4xx as *RejectedError.
func (c *Client) once(ctx context.Context, url string, body []byte) (Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return Response{}, &malformedResponseError{msg: fmt.Sprintf("build request: %v", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "token "+c.Token)

	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return Response{}, fmt.Errorf("POST %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))

	switch {
	case resp.StatusCode == http.StatusOK:
		var out Response
		if err := json.Unmarshal(respBody, &out); err != nil {
			return Response{}, &malformedResponseError{msg: fmt.Sprintf("POST %s: 200 with a body that is not the contract's: %v: %s", url, err, respBody)}
		}
		return out, nil
	case resp.StatusCode >= 500:
		return Response{}, fmt.Errorf("POST %s: status %d: %s", url, resp.StatusCode, respBody)
	default:
		// Every other status, 4xx and unexpected 1xx-3xx alike, is a response
		// the manager would give again. Retrying it cannot help.
		return Response{}, &RejectedError{Status: resp.StatusCode, Body: string(respBody)}
	}
}

// checkResponse holds the manager to the contract: it stored every record we
// sent, for the hour and kinds we sent. A mismatch means the two sides disagree
// about the wire, which no retry fixes.
func checkResponse(req Request, resp Response) error {
	if resp.Accepted != int64(len(req.Records)) {
		return &malformedResponseError{msg: fmt.Sprintf("manager accepted %d records for %s, sent %d", resp.Accepted, req.WindowStart, len(req.Records))}
	}
	if resp.WindowStart != "" {
		got, err := time.Parse(time.RFC3339, resp.WindowStart)
		want, _ := time.Parse(time.RFC3339, req.WindowStart)
		if err != nil || !got.Equal(want) {
			return &malformedResponseError{msg: fmt.Sprintf("manager answered for window %q, sent %q", resp.WindowStart, req.WindowStart)}
		}
	}
	return nil
}

func (c *Client) backoff(retry int) time.Duration {
	base, ceiling := c.BaseBackoff, c.MaxBackoff
	if base <= 0 {
		base = DefaultBaseBackoff
	}
	if ceiling <= 0 {
		ceiling = DefaultMaxBackoff
	}
	d := base
	for i := 1; i < retry && d < ceiling; i++ {
		d *= 2
	}
	if d > ceiling {
		d = ceiling
	}
	return d
}

func (c *Client) sleep(ctx context.Context, d time.Duration) error {
	if c.Sleep != nil {
		return c.Sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
