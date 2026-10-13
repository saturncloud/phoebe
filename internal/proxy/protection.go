package proxy

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/saturncloud/phoebe/internal/config"
)

// Gateway protection (ruling 2026-10-12): bounds on what any ONE request can
// hold in phoebe, applied to all traffic, shared and dedicated, and set once
// for the whole gateway. Capacity admission (shared tier only) protects the
// engines; these protect phoebe, which every tenant shares.

// The causes a protection bound cancels a request's context with. The capture
// reader then records the request as aborted (the connection was cut, as on a
// client disconnect) and settles any admission lease conservatively; the
// cause is what tells the logs which bound fired.
var (
	errMaxStreamDuration   = errors.New("request exceeded protection.maxStreamDuration")
	errStreamStalled       = errors.New("client stopped reading for longer than protection.streamStallTimeout")
	errResponseBufferLimit = errors.New("response buffering exceeded a protection.maxResponseBufferBytes limit")
)

// protectionCause returns the protection bound that cancelled ctx, or nil when
// none did (a normal finish or a client disconnect).
func protectionCause(ctx context.Context) error {
	cause := context.Cause(ctx)
	for _, e := range []error{errMaxStreamDuration, errStreamStalled, errResponseBufferLimit} {
		if errors.Is(cause, e) {
			return cause
		}
	}
	return nil
}

func (s *Server) protection() config.ProtectionSettings {
	return s.settings.Protection.Effective()
}

// stallGuardWriter bounds every write to the client with a fresh write
// deadline. http.Server.IdleTimeout does not cover a request in flight, and
// phoebe deliberately sets no WriteTimeout (a whole-response deadline would cut
// long streams), so without this a client that stops reading holds its request
// — the engine work, the admission lease, the goroutines and buffers — until
// it disconnects, which it may never do. The deadline is per write: a stream
// may idle between chunks for as long as the engine needs; only a write that
// cannot make progress for the whole timeout fails.
type stallGuardWriter struct {
	http.ResponseWriter
	rc      *http.ResponseController
	timeout time.Duration
	cancel  context.CancelCauseFunc
	// stalled records that a write failed on its deadline. It is kept apart
	// from the context cause because net/http cancels the parent connection
	// context on the same write error before Write returns, so by the time
	// fail runs the request context is usually already cancelled with
	// context.Canceled and cancel(errStreamStalled) cannot set the cause.
	stalled atomic.Bool
}

func newStallGuardWriter(w http.ResponseWriter, timeout time.Duration, cancel context.CancelCauseFunc) *stallGuardWriter {
	return &stallGuardWriter{ResponseWriter: w, rc: http.NewResponseController(w), timeout: timeout, cancel: cancel}
}

// arm sets the deadline for the write about to happen. The proxy handler also
// arms once more when it returns, covering the writes net/http makes after
// the handler (the chunked terminator and final flush in finishRequest).
// net/http then clears the connection's write deadline (conn.serve, right
// after finishRequest), so it never outlives this request; pinned by
// TestStallGuardDeadlineDoesNotAffectPostHandlerWritesOrNextRequest.
// ErrNotSupported (a ResponseWriter without deadline support, such as a
// test recorder) leaves the write unbounded, exactly as before.
func (g *stallGuardWriter) arm() {
	_ = g.rc.SetWriteDeadline(time.Now().Add(g.timeout))
}

// fail records a stall when err is a write that ran past its deadline. A
// network write deadline surfaces as a *net.OpError wrapping
// os.ErrDeadlineExceeded ("i/o timeout"), which does NOT match
// context.DeadlineExceeded; any net.Error reporting Timeout() is treated the
// same way. The cancel still runs: it sets the cause when the parent context
// has not been cancelled yet.
func (g *stallGuardWriter) fail(err error) {
	if err == nil {
		return
	}
	var ne net.Error
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) ||
		(errors.As(err, &ne) && ne.Timeout()) {
		g.stalled.Store(true)
		g.cancel(errStreamStalled)
	}
}

// stallCause returns errStreamStalled when a write to the client failed on
// its deadline, otherwise nil.
func (g *stallGuardWriter) stallCause() error {
	if g.stalled.Load() {
		return errStreamStalled
	}
	return nil
}

func (g *stallGuardWriter) Write(p []byte) (int, error) {
	g.arm()
	n, err := g.ResponseWriter.Write(p)
	g.fail(err)
	return n, err
}

func (g *stallGuardWriter) WriteHeader(status int) {
	g.arm()
	g.ResponseWriter.WriteHeader(status)
}

// FlushError is the method http.ResponseController (and so ReverseProxy's
// per-chunk flush) prefers; the buffered bytes reach the network here, so
// this is where a stalled client actually blocks.
func (g *stallGuardWriter) FlushError() error {
	g.arm()
	err := g.rc.Flush()
	g.fail(err)
	return err
}

func (g *stallGuardWriter) Flush() { _ = g.FlushError() }

func (g *stallGuardWriter) Unwrap() http.ResponseWriter { return g.ResponseWriter }

// responseBufferBudget is the replica-wide total of response bytes held in
// memory for inspection (protection.maxResponseBufferBytesTotal).
type responseBufferBudget struct {
	held atomic.Int64
}

// responseBufferAccount is one request's share of the budget. A request holds
// response bytes in two places — the capture reader's unparsed tail (or a
// whole non-streamed body) and the usage filter's current SSE line — and both
// count toward the request's per-stream limit and the replica total. Each
// place reports its current size; the account charges only the difference.
type responseBufferAccount struct {
	budget    *responseBufferBudget
	perStream int64
	total     int64
	cancel    context.CancelCauseFunc

	mu     sync.Mutex
	slots  [2]int64
	closed bool
}

const (
	bufferSlotCapture = iota
	bufferSlotFilter
)

func (b *responseBufferBudget) newAccount(p config.ProtectionSettings, cancel context.CancelCauseFunc) *responseBufferAccount {
	return &responseBufferAccount{budget: b, perStream: p.MaxResponseBufferBytesPerStream, total: p.MaxResponseBufferBytesTotal, cancel: cancel}
}

// set records that slot now holds n bytes. It fails, charging nothing, when
// the request would exceed its per-stream limit or the replica would exceed
// its total; the caller then ends the response and the request's context is
// cancelled so capture records the cut. A nil account (no protection wired,
// as in some unit tests) accepts everything.
func (a *responseBufferAccount) set(slot int, n int64) error {
	if err := a.reserve(slot, n); err != nil {
		a.cancel(err)
		return err
	}
	return nil
}

// reserve is set without cancelling the request on refusal, for a caller
// that can shrink what it holds instead of ending the response.
func (a *responseBufferAccount) reserve(slot int, n int64) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	delta := n - a.slots[slot]
	if delta > 0 {
		var mine int64
		for i, v := range a.slots {
			if i != slot {
				mine += v
			}
		}
		if mine+n > a.perStream {
			return errResponseBufferLimit
		}
		// Reserve with compare-and-swap so a refused delta never touches the
		// shared counter: an add-then-roll-back would briefly inflate it and
		// could make a concurrent small request see a false over-total.
		for {
			cur := a.budget.held.Load()
			if cur+delta > a.total {
				return errResponseBufferLimit
			}
			if a.budget.held.CompareAndSwap(cur, cur+delta) {
				break
			}
		}
	} else if delta < 0 {
		a.budget.held.Add(delta)
	}
	a.slots[slot] = n
	return nil
}

// force records that slot now holds n bytes with no per-stream or total
// check. It charges bytes the caller has already committed to hold: the head
// and tail an overflowed non-streamed capture keeps, which D22 says is never
// cut. Charging them anyway keeps the replica total honest — it may go above
// its cap, and while it is above, every other request's reserve and set are
// refused, so the pressure those bytes put on the replica is seen. A nil or
// closed account does nothing.
func (a *responseBufferAccount) force(slot int, n int64) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	a.budget.held.Add(n - a.slots[slot])
	a.slots[slot] = n
}

// close returns every byte the request still holds to the budget. Idempotent.
func (a *responseBufferAccount) close() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	a.closed = true
	for i, v := range a.slots {
		a.budget.held.Add(-v)
		a.slots[i] = 0
	}
}

// dedicatedColdHolds counts, per dedicated endpoint, the requests this replica
// is holding while that endpoint starts from zero
// (protection.maxDedicatedColdHoldsPerEndpoint). Per replica is the right
// scope: what the cap protects is this process's goroutines and buffered
// request bodies. Per endpoint keeps one tenant's burst at a cold endpoint
// from using up every other dedicated endpoint's holds.
type dedicatedColdHolds struct {
	mu   sync.Mutex
	held map[string]int64
}

// acquire takes one hold on endpoint if fewer than limit are held, returning
// its release function.
func (d *dedicatedColdHolds) acquire(endpoint string, limit int64) (func(), bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.held == nil {
		d.held = map[string]int64{}
	}
	if d.held[endpoint] >= limit {
		return nil, false
	}
	d.held[endpoint]++
	var once sync.Once
	return func() {
		once.Do(func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			if d.held[endpoint]--; d.held[endpoint] <= 0 {
				delete(d.held, endpoint)
			}
		})
	}, true
}

// writeDedicatedColdHoldsFull answers a request to a cold dedicated endpoint
// that already has the maximum number of requests waiting on its start: 503
// (out of capacity, the code the shared tier's cold-hold limit uses) with
// Retry-After, in phoebe's {"error": "..."} shape.
func writeDedicatedColdHoldsFull(w http.ResponseWriter, requestID string) {
	writeRetryLater(w, requestID, "The endpoint is starting from zero and too many requests are already waiting; retry in about "+
		strconv.Itoa(wakeRetryAfterSeconds)+" seconds.")
}
