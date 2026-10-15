// Package waker implements the proxy's wake-from-zero seam
// (internal/proxy/wake.go) by asking Atlas to wake the endpoint. Atlas owns
// the ordering the Planner ruling requires (workers to one, wait until ready,
// then the Planner to one) for shared and dedicated graphs alike, so phoebe
// needs no Kubernetes access to wake a graph.
package waker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/proxy"
)

const internalWakePath = "/api/internal/token-factory/inference-endpoints/wake"
const internalWakeHeader = "X-Saturn-Internal-Wake-Token"

// AtlasWaker asks Atlas to wake a shared or dedicated endpoint, then waits
// until the frontend advertises the requested model.
type AtlasWaker struct {
	baseURL      string
	token        string
	client       *http.Client
	pollInterval time.Duration
	log          *logging.Logger
}

func NewAtlas(baseURL, token string, log *logging.Logger) (*AtlasWaker, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, errors.New("atlas waker: atlas URL is empty")
	}
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("atlas waker: internal wake token is empty")
	}
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
		u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, fmt.Errorf("atlas waker: invalid Atlas URL %q", baseURL)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// The wake token is an internal credential. Never send it through an
	// ambient HTTP proxy inherited from the pod environment.
	transport.Proxy = nil
	return &AtlasWaker{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		client: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		pollInterval: 2 * time.Second,
		log:          log,
	}, nil
}

func (w *AtlasWaker) Wake(ctx context.Context, target proxy.WakeTarget) error {
	if target.ResourceID == "" {
		return errors.New("atlas waker: no resource id on wake target")
	}
	if target.ServedModel == "" {
		return errors.New("atlas waker: no served model on wake target")
	}
	for {
		err := w.requestWake(ctx, target.ResourceID)
		if err == nil {
			break
		}
		if !errors.Is(err, proxy.ErrWakeStatusUnknown) {
			return err
		}
		// Atlas waits for ready workers for less time than a cold start
		// takes and answers 503 while they are still starting; it restores
		// the Planner only on the call that sees them ready. Atlas's wake is
		// idempotent, so ask again until the wake hold expires.
		timer := time.NewTimer(w.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
	}
	w.log.Info.Printf("waker: Atlas woke graph workers and Planner (resource_id=%s)", target.ResourceID)
	return w.waitReady(ctx, target.UpstreamHost, target.ServedModel)
}

// requestWake makes one Atlas wake call. Errors wrapping
// proxy.ErrWakeStatusUnknown mean Atlas may have started the wake.
func (w *AtlasWaker) requestWake(ctx context.Context, resourceID string) error {
	body, err := json.Marshal(map[string]string{"resource_id": resourceID})
	if err != nil {
		return fmt.Errorf("atlas waker: marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.baseURL+internalWakePath, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("atlas waker: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(internalWakeHeader, w.token)
	var gotConn atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) { gotConn.Store(true) },
	}))
	resp, err := w.client.Do(req)
	if err != nil {
		if !gotConn.Load() {
			return fmt.Errorf("atlas waker: request failed before connection: %w", err)
		}
		// Atlas can apply the replica restore before its response is lost.
		// Treat a transport failure after dispatch as indeterminate so callers
		// return the retryable wake-in-progress response, not the cold response.
		return fmt.Errorf("%w: atlas waker: request: %v", proxy.ErrWakeStatusUnknown, err)
	}
	defer func() { _ = resp.Body.Close() }()
	responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode >= http.StatusInternalServerError {
		// Atlas may have restored the workers before a later server-side
		// failure. Treat that outcome as indeterminate, as with a response lost
		// after dispatch, so callers return an honest retryable response.
		return fmt.Errorf("%w: atlas waker: status %d: %s", proxy.ErrWakeStatusUnknown, resp.StatusCode, string(responseBody))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("atlas waker: status %d: %s", resp.StatusCode, string(responseBody))
	}
	return nil
}

func (w *AtlasWaker) waitReady(ctx context.Context, upstreamHost, servedModel string) error {
	ready := func() bool {
		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, "http://"+upstreamHost+"/v1/models", nil)
		if err != nil {
			return false
		}
		resp, err := w.client.Do(req)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var body struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&body); err != nil {
			return false
		}
		for _, model := range body.Data {
			if model.ID == servedModel {
				return true
			}
		}
		return false
	}
	if ready() {
		return nil
	}
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("atlas waker: upstream %s not ready: %w: %w", upstreamHost, proxy.ErrWakeNotReady, ctx.Err())
		case <-ticker.C:
			if ready() {
				return nil
			}
		}
	}
}
