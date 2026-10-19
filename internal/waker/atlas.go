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
	"sync"
	"sync/atomic"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
	"github.com/saturncloud/phoebe/internal/proxy"
)

const internalWakePath = "/api/internal/token-factory/inference-endpoints/wake"
const internalWakeHeader = "X-Saturn-Internal-Wake-Token"

// wakeLoopWindow bounds one wake loop. It is independent of the client's
// hold (wake.timeout), which can be much shorter than a cold start: the loop
// must outlast the cold start so the call that finds the workers ready, and
// restores the Planner, still happens. Ten minutes matches the reaper's
// interval; its repair pass restores a Planner after anything longer.
const wakeLoopWindow = 10 * time.Minute

// AtlasWaker asks Atlas to wake a shared or dedicated endpoint, then waits
// until the frontend advertises the requested model.
//
// Atlas restores the Planner only on the wake call that finds the workers
// ready, so the call loop must not depend on any one client staying
// connected. Each endpoint therefore has at most one loop per phoebe
// replica: it runs detached from the request that started it, and every
// request held for that endpoint waits on it. This also keeps a burst of
// cold requests from becoming a burst of Atlas calls, each of which holds a
// database connection while Atlas waits on the graph's lock.
type AtlasWaker struct {
	baseURL      string
	token        string
	client       *http.Client
	pollInterval time.Duration
	loopWindow   time.Duration
	log          *logging.Logger

	mu      sync.Mutex
	flights map[string]*wakeFlight
}

// wakeFlight is the running wake loop for one endpoint. err is written
// before done is closed.
type wakeFlight struct {
	done chan struct{}
	err  error
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
		loopWindow:   wakeLoopWindow,
		log:          log,
		flights:      map[string]*wakeFlight{},
	}, nil
}

func (w *AtlasWaker) Wake(ctx context.Context, target proxy.WakeTarget) error {
	if target.ResourceID == "" {
		return errors.New("atlas waker: no resource id on wake target")
	}
	if target.ServedModel == "" {
		return errors.New("atlas waker: no served model on wake target")
	}
	f := w.joinFlight(ctx, target.ResourceID)
	select {
	case <-f.done:
		if f.err != nil {
			return f.err
		}
	case <-ctx.Done():
		// The loop keeps running, so the Planner is still restored.
		return fmt.Errorf("%w: atlas waker: hold ended while the wake was running: %v",
			proxy.ErrWakeStatusUnknown, ctx.Err())
	}
	w.log.Info.Printf("waker: Atlas woke graph workers and Planner (resource_id=%s)", target.ResourceID)
	return w.waitReady(ctx, target.UpstreamHost, target.ServedModel)
}

// joinFlight returns the running wake loop for resourceID, or starts one
// that is not cancelled with ctx and runs for at most loopWindow.
func (w *AtlasWaker) joinFlight(ctx context.Context, resourceID string) *wakeFlight {
	w.mu.Lock()
	defer w.mu.Unlock()
	if f, ok := w.flights[resourceID]; ok {
		return f
	}
	loopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.loopWindow)
	f := &wakeFlight{done: make(chan struct{})}
	w.flights[resourceID] = f
	go func() {
		defer cancel()
		f.err = w.callUntilWoken(loopCtx, resourceID)
		w.mu.Lock()
		delete(w.flights, resourceID)
		w.mu.Unlock()
		close(f.done)
	}()
	return f
}

// callUntilWoken calls Atlas until it reports the wake done, refuses it, or
// ctx ends. Atlas waits for ready workers for less time than a cold start
// takes and answers 503 while they are still starting; its wake is
// idempotent, so the loop asks again. Once a call has left the status
// unknown, a later failure (Atlas restarting, a 4xx) cannot prove that
// nothing is starting, so the result stays "status unknown".
func (w *AtlasWaker) callUntilWoken(ctx context.Context, resourceID string) error {
	var unknown error
	for {
		err := w.requestWake(ctx, resourceID)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, proxy.ErrWakeStatusUnknown):
			unknown = err
		case unknown == nil:
			return err
		}
		timer := time.NewTimer(w.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return unknown
		case <-timer.C:
		}
	}
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
		// A shared graph lists every adapter loaded on it; 64 KiB held only a
		// few hundred names, and a truncated list never shows the model (Q69).
		if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&body); err != nil {
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
