// Package waker implements the proxy's wake-from-zero seam
// (internal/proxy/wake.go) by asking Atlas to wake the endpoint. Atlas owns
// the ordering the Planner ruling requires (workers to one, wait until ready,
// then the Planner to one) for shared and dedicated graphs alike, so phoebe
// needs no Kubernetes access to wake a graph.
package waker

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
	// unreachableBudget bounds a flight that has never reached Atlas.
	unreachableBudget time.Duration
	log               *logging.Logger

	mu      sync.Mutex
	flights map[string]*wakeFlight
}

// wakeFlight is the running wake loop for one graph. err is written
// before done is closed.
type wakeFlight struct {
	done chan struct{}
	err  error
	// resourceID is the endpoint whose request started the loop: Atlas
	// validates the wake per endpoint, so a refusal is about this endpoint.
	resourceID string
	// reachedAtlas is set once any call is known to have reached Atlas (a
	// fresh connection, or a response byte on a reused one).
	reachedAtlas atomic.Bool
	// triedUnreached is set when a call failed without reaching Atlas.
	triedUnreached atomic.Bool
	// lastUnreached is the most recent such failure, for the caller's log.
	lastUnreached atomic.Pointer[error]
}

// unavailable reports that Atlas was tried and never reached by this flight:
// nothing was dispatched. It is false before the first call has failed, so a
// hold that ends while the first call is still dialing is "status unknown".
func (f *wakeFlight) unavailable() bool {
	return f.triedUnreached.Load() && !f.reachedAtlas.Load()
}

func (f *wakeFlight) lastUnreachedError() error {
	if err := f.lastUnreached.Load(); err != nil {
		return *err
	}
	return nil
}

// errAtlasRefused marks a definite Atlas refusal (a 4xx): about the endpoint
// in the call, not about the graph.
var errAtlasRefused = errors.New("atlas waker: Atlas refused the wake")

// errBeforeConnection marks a wake call that failed before a connection was
// made, so Atlas never received it. It is safe to retry.
var errBeforeConnection = errors.New("atlas waker: request failed before connection")

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
		// A rolling Atlas restart takes about a minute; twice that is the most
		// a wake waits for an Atlas it has never reached.
		unreachableBudget: 2 * time.Minute,
		log:               log,
		flights:           map[string]*wakeFlight{},
	}, nil
}

func (w *AtlasWaker) Wake(ctx context.Context, target proxy.WakeTarget) error {
	if target.ResourceID == "" {
		return errors.New("atlas waker: no resource id on wake target")
	}
	if target.ServedModel == "" {
		return errors.New("atlas waker: no served model on wake target")
	}
	// One loop per GRAPH (F1): a shared graph carries many endpoints, and
	// each Atlas call holds a database connection while it waits on the
	// graph's lock, so K endpoints waking one graph share one loop instead
	// of running K. A refusal is about the endpoint that made the call, so a
	// joiner for another endpoint never inherits it: it runs its own loop.
	f := w.joinFlight(ctx, "graph/"+target.GraphK8sName, target.ResourceID)
	err := w.waitFlight(ctx, f)
	if errors.Is(err, errAtlasRefused) && f.resourceID != target.ResourceID {
		err = w.waitFlight(ctx, w.joinFlight(ctx, "resource/"+target.ResourceID, target.ResourceID))
	}
	if err != nil {
		return err
	}
	w.log.Info.Printf("waker: Atlas woke graph workers and Planner (resource_id=%s)", target.ResourceID)
	return w.waitReady(ctx, target.UpstreamHost, target.ServedModel)
}

// waitFlight waits for f's loop or for the hold to end.
func (w *AtlasWaker) waitFlight(ctx context.Context, f *wakeFlight) error {
	select {
	case <-f.done:
		if f.err != nil {
			return f.err
		}
	case <-ctx.Done():
		// The loop keeps running, so the Planner is still restored.
		if f.unavailable() {
			return fmt.Errorf("%w: atlas waker: Atlas unreachable for the whole hold: %v (last error: %v)",
				proxy.ErrWakeUnavailable, ctx.Err(), f.lastUnreachedError())
		}
		return fmt.Errorf("%w: atlas waker: hold ended while the wake was running: %v",
			proxy.ErrWakeStatusUnknown, ctx.Err())
	}
	return nil
}

// joinFlight returns the running wake loop under key, or starts one for
// resourceID that is not cancelled with ctx and runs for at most loopWindow
// (a flight that never reaches Atlas stops sooner, after
// unreachableBudget).
//
// The loop closes done BEFORE it leaves the map (F4): a request arriving in
// between joins the finished flight and takes its fresh result, instead of
// starting a second loop.
func (w *AtlasWaker) joinFlight(ctx context.Context, key, resourceID string) *wakeFlight {
	w.mu.Lock()
	defer w.mu.Unlock()
	if f, ok := w.flights[key]; ok {
		return f
	}
	loopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.loopWindow)
	f := &wakeFlight{done: make(chan struct{}), resourceID: resourceID}
	w.flights[key] = f
	go func() {
		defer cancel()
		f.err = w.callUntilWoken(loopCtx, resourceID, f)
		close(f.done)
		w.mu.Lock()
		delete(w.flights, key)
		w.mu.Unlock()
	}()
	return f
}

// callUntilWoken calls Atlas until it reports the wake done, refuses it, or
// ctx ends. Atlas waits for ready workers for less time than a cold start
// takes and answers 503 while they are still starting; its wake is
// idempotent, so the loop asks again. Once a call has left the status
// unknown, a later failure (Atlas restarting, a 4xx) cannot prove that
// nothing is starting, so the result stays "status unknown".
//
// A call that never reached Atlas (it is restarting, as on every deploy) is
// asked again, but only within limits, because nothing has been dispatched:
//   - a failure that looks like misconfiguration (the Atlas host does not
//     resolve, or its certificate is not trusted) ends the flight at once as
//     ErrWakeUnavailable, logged at ERROR with the cause;
//   - a flight that has not reached Atlas within unreachableBudget ends as
//     ErrWakeUnavailable, so held requests stop pinning cold-hold slots and
//     no graph is woken minutes after every caller has left.
func (w *AtlasWaker) callUntilWoken(ctx context.Context, resourceID string, f *wakeFlight) error {
	var unknown error
	var unreachedSince time.Time
	reached := func() { f.reachedAtlas.Store(true) }
	for {
		err := w.requestWake(ctx, resourceID, reached)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, proxy.ErrWakeStatusUnknown):
			unknown = err
		case errors.Is(err, errBeforeConnection):
			f.lastUnreached.Store(&err)
			f.triedUnreached.Store(true)
			if f.reachedAtlas.Load() {
				break // Atlas was reached earlier: keep the loop's earlier verdict
			}
			if looksMisconfigured(err) {
				w.log.Error.Printf("waker: Atlas wake URL looks misconfigured (resource_id=%s): %v", resourceID, err)
				return fmt.Errorf("%w: %v", proxy.ErrWakeUnavailable, err)
			}
			if unreachedSince.IsZero() {
				unreachedSince = time.Now()
			} else if time.Since(unreachedSince) >= w.unreachableBudget {
				w.log.Warn.Printf("waker: Atlas unreachable for %s, giving up this wake (resource_id=%s): %v",
					w.unreachableBudget, resourceID, err)
				return fmt.Errorf("%w: %v", proxy.ErrWakeUnavailable, err)
			}
		case unknown == nil:
			return err
		}
		timer := time.NewTimer(w.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if unknown == nil && f.unavailable() {
				return fmt.Errorf("%w: %v", proxy.ErrWakeUnavailable, f.lastUnreachedError())
			}
			return unknown
		case <-timer.C:
		}
	}
}

// looksMisconfigured reports a failure that a restart does not explain: the
// Atlas host does not resolve, or its TLS certificate is not trusted.
func looksMisconfigured(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return true
	}
	var verifyErr *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameErr x509.HostnameError
	var invalidErr x509.CertificateInvalidError
	return errors.As(err, &verifyErr) || errors.As(err, &unknownAuthority) ||
		errors.As(err, &hostnameErr) || errors.As(err, &invalidErr)
}

// requestWake makes one Atlas wake call. Errors wrapping
// proxy.ErrWakeStatusUnknown mean Atlas may have started the wake. reached
// runs once the call is known to have reached Atlas: a fresh connection, or
// a response byte on a reused keep-alive connection (a pooled connection to
// an Atlas pod that has since gone away proves nothing). The Idempotency-Key
// header lets net/http resend the call on a fresh connection when a pooled
// one turns out to be dead; Atlas's wake is idempotent.
func (w *AtlasWaker) requestWake(ctx context.Context, resourceID string, reached func()) error {
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
	req.Header.Set("Idempotency-Key", resourceID)
	var gotConn atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if !info.Reused {
				gotConn.Store(true)
				reached()
			}
		},
		GotFirstResponseByte: func() {
			gotConn.Store(true)
			reached()
		},
	}))
	resp, err := w.client.Do(req)
	if err != nil {
		if !gotConn.Load() {
			return fmt.Errorf("%w: %w", errBeforeConnection, err)
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
		return fmt.Errorf("%w: status %d: %s", errAtlasRefused, resp.StatusCode, string(responseBody))
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
