package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/capture"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

// chunkedBody serves body in fixed-size reads, like an upstream connection.
type chunkedBody struct {
	r     *bytes.Reader
	chunk int
}

func (c *chunkedBody) Read(p []byte) (int, error) {
	if len(p) > c.chunk {
		p = p[:c.chunk]
	}
	return c.r.Read(p)
}

func (c *chunkedBody) Close() error { return nil }

func noBufferCancel(t *testing.T) func(error) {
	return func(err error) { t.Errorf("an overflowed non-streamed capture cancelled its request: %v", err) }
}

// force charges past both the per-stream limit and the replica total, close
// returns everything it charged, and a force after close charges nothing.
func TestResponseBufferForceChargesPastLimitsAndCloseReturnsIt(t *testing.T) {
	budget := &responseBufferBudget{}
	p := config.ProtectionSettings{MaxResponseBufferBytesPerStream: 10, MaxResponseBufferBytesTotal: 20}
	a := budget.newAccount(p, func(error) {})
	a.force(bufferSlotCapture, 50)
	if held := budget.held.Load(); held != 50 {
		t.Fatalf("held=%d after force(50), want 50 (past perStream 10 and total 20)", held)
	}
	a.force(bufferSlotCapture, 30)
	if held := budget.held.Load(); held != 30 {
		t.Fatalf("held=%d after shrinking to 30, want 30", held)
	}
	other := budget.newAccount(p, func(error) {})
	if err := other.reserve(bufferSlotCapture, 1); !errors.Is(err, errResponseBufferLimit) {
		t.Fatalf("reserve while the total is over its cap = %v, want errResponseBufferLimit", err)
	}
	a.close()
	if held := budget.held.Load(); held != 0 {
		t.Fatalf("held=%d after close, want 0", held)
	}
	a.force(bufferSlotCapture, 99)
	if held := budget.held.Load(); held != 0 {
		t.Fatalf("held=%d after a force on a closed account, want 0", held)
	}
	var nilAccount *responseBufferAccount
	nilAccount.force(bufferSlotCapture, 5) // must not panic
}

// An overflowed non-streamed capture charges exactly the head and tail it
// keeps after every read, even with a per-stream limit below 512 KiB, and
// returns them at close.
func TestOverflowedCaptureChargesItsHeadAndTail(t *testing.T) {
	body := []byte(`{"model":"m","data":"` + strings.Repeat("x", 2<<20) + `","usage":{"prompt_tokens":3,"completion_tokens":0,"total_tokens":3}}`)
	budget := &responseBufferBudget{}
	account := budget.newAccount(config.ProtectionSettings{MaxResponseBufferBytesPerStream: 64 << 10, MaxResponseBufferBytesTotal: 256 << 20}, noBufferCancel(t))
	var result capture.Result
	cr := newCaptureReader(context.Background(), &chunkedBody{r: bytes.NewReader(body), chunk: 32 << 10}, false, func(r capture.Result) { result = r })
	cr.buffers = account

	var got bytes.Buffer
	buf := make([]byte, 32<<10)
	for {
		n, err := cr.Read(buf)
		got.Write(buf[:n])
		if err == nil && cr.overflowed {
			want := int64(len(cr.head) + min(len(cr.tail), overflowKeep))
			account.mu.Lock()
			slot := account.slots[bufferSlotCapture]
			account.mu.Unlock()
			if slot != want || budget.held.Load() != want {
				t.Fatalf("after %d bytes: slot=%d held=%d, want both = head+tail = %d", got.Len(), slot, budget.held.Load(), want)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	if !bytes.Equal(got.Bytes(), body) {
		t.Fatalf("delivered %d bytes, want the whole %d-byte body", got.Len(), len(body))
	}
	if !cr.overflowed || !result.UsageFound || result.Model != "m" {
		t.Fatalf("overflowed=%v result=%+v, want an overflowed capture that still metered", cr.overflowed, result)
	}
	if held := budget.held.Load(); held != 0 {
		t.Fatalf("held=%d after finish, want 0", held)
	}
}

// With the replica total already used up by another request, an overflowed
// capture still delivers the whole body and charges its head and tail past
// the total, so a third request's reserve is refused until it finishes.
func TestOverflowedCaptureChargesPastAnExhaustedReplicaTotal(t *testing.T) {
	const total = 1 << 20
	body := []byte(`{"model":"m","data":"` + strings.Repeat("y", 2<<20) + `"}`)
	budget := &responseBufferBudget{}
	p := config.ProtectionSettings{MaxResponseBufferBytesPerStream: total, MaxResponseBufferBytesTotal: total}
	holder := budget.newAccount(p, func(error) {})
	if err := holder.reserve(bufferSlotCapture, total); err != nil {
		t.Fatalf("holder reserve: %v", err)
	}
	account := budget.newAccount(p, noBufferCancel(t))
	cr := newCaptureReader(context.Background(), &chunkedBody{r: bytes.NewReader(body), chunk: 64 << 10}, false, nil)
	cr.buffers = account

	var got bytes.Buffer
	buf := make([]byte, 64<<10)
	for {
		n, err := cr.Read(buf)
		got.Write(buf[:n])
		if err == nil && got.Len() > 1<<20 {
			if !cr.overflowed {
				t.Fatal("capture did not overflow with the replica total used up")
			}
			want := int64(total + len(cr.head) + min(len(cr.tail), overflowKeep))
			if held := budget.held.Load(); held != want {
				t.Fatalf("held=%d, want the holder's %d plus head+tail = %d", held, total, want)
			}
			third := budget.newAccount(p, func(error) {})
			if err := third.reserve(bufferSlotCapture, 1); !errors.Is(err, errResponseBufferLimit) {
				t.Fatalf("third request's reserve = %v, want errResponseBufferLimit while the total is over its cap", err)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	if !bytes.Equal(got.Bytes(), body) {
		t.Fatalf("delivered %d bytes, want the whole %d-byte body", got.Len(), len(body))
	}
	if held := budget.held.Load(); held != total {
		t.Fatalf("held=%d after the capture finished, want the holder's %d", held, total)
	}
	holder.close()
	if held := budget.held.Load(); held != 0 {
		t.Fatalf("held=%d after every account closed, want 0", held)
	}
}

// While an overflowed non-streamed response is still in flight, the replica
// budget carries the head and tail capture keeps; it returns to zero only
// after the request ends.
func TestNonStreamedOverflowIsChargedWhileInFlight(t *testing.T) {
	release := make(chan struct{})
	first := `{"model":"emb-model","data":"` + strings.Repeat("x", 1<<20)
	rest := `","usage":{"prompt_tokens":11,"completion_tokens":0,"total_tokens":11}}`
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, first)
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, rest)
	}))
	defer backend.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	up, _ := url.Parse(backend.URL)
	s := New(&config.Settings{Protection: config.ProtectionSettings{MaxResponseBufferBytesPerStream: 64 << 10}},
		logging.New(logging.ERROR), &recordingEmitter{})

	req := httptest.NewRequest(http.MethodPost, "/v1/embeddings", strings.NewReader(`{"model":"emb-model","input":"hi"}`))
	setUpstream(req, up)
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "r1")
	rr := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Handler().ServeHTTP(rr, req)
	}()

	// The head is what capture held when it overflowed (more than the 64 KiB
	// limit); the tail is a full overflowKeep once 1 MiB has passed.
	const headAndTail = overflowKeep + 64<<10
	deadline := time.Now().Add(5 * time.Second)
	for s.responseBuffers.held.Load() < headAndTail {
		if time.Now().After(deadline) {
			t.Fatalf("in-flight held=%d, want at least head+tail = %d", s.responseBuffers.held.Load(), headAndTail)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	<-done
	if rr.Code != http.StatusOK || rr.Body.String() != first+rest {
		t.Fatalf("status=%d len=%d, want the whole body", rr.Code, rr.Body.Len())
	}
	if held := s.responseBuffers.held.Load(); held != 0 {
		t.Fatalf("replica budget still holds %d bytes after the request", held)
	}
}

// A client abort runs Close (and so finish) on another goroutine while Read is
// still overflowing a non-streamed body. Every access to scan, head, tail and
// overflowed holds mu, so this is race-free under -race; onDone fires once,
// each read in flight charges exactly the head plus the overflowKeep tail, and
// nothing stays charged at the end.
func TestNonStreamedOverflowCloseConcurrentWithRead(t *testing.T) {
	body := []byte(`{"model":"m","data":"` + strings.Repeat("z", 8<<20) + `","usage":{"prompt_tokens":3,"completion_tokens":0,"total_tokens":3}}`)
	budget := &responseBufferBudget{}
	account := budget.newAccount(config.ProtectionSettings{MaxResponseBufferBytesPerStream: 64 << 10, MaxResponseBufferBytesTotal: 256 << 20}, noBufferCancel(t))
	var fired int
	var firedMu sync.Mutex
	cr := newCaptureReader(context.Background(), &chunkedBody{r: bytes.NewReader(body), chunk: 32 << 10}, false, func(capture.Result) {
		firedMu.Lock()
		fired++
		firedMu.Unlock()
	})
	cr.buffers = account

	overflowing := make(chan struct{})
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 32<<10)
		signalled := false
		for {
			_, err := cr.Read(buf)
			cr.mu.Lock()
			overflowed := cr.overflowed
			want := int64(len(cr.head) + min(len(cr.tail), overflowKeep))
			cr.mu.Unlock()
			if overflowed {
				// The account is charged by this goroutine only, so once
				// Read returns its charge is final until Close closes it.
				account.mu.Lock()
				slot, closed := account.slots[bufferSlotCapture], account.closed
				account.mu.Unlock()
				if !closed && slot != want {
					readDone <- fmt.Errorf("slot=%d, want head+tail=%d", slot, want)
					return
				}
				if want > 2*overflowKeep {
					readDone <- fmt.Errorf("charged %d, past the 2*overflowKeep cap %d", want, 2*overflowKeep)
					return
				}
				if !signalled {
					signalled = true
					close(overflowing)
				}
			}
			if err == io.EOF {
				readDone <- nil
				return
			}
			if err != nil {
				readDone <- err
				return
			}
		}
	}()
	select {
	case <-overflowing:
	case err := <-readDone:
		t.Fatalf("read finished before overflowing: %v", err)
	}
	if err := cr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	firedMu.Lock()
	defer firedMu.Unlock()
	if fired != 1 {
		t.Fatalf("onDone fired %d times, want 1", fired)
	}
	if held := budget.held.Load(); held != 0 {
		t.Fatalf("held=%d after Close, want 0", held)
	}
}

// keepTail keeps exactly the last overflowKeep bytes of a 50 MiB body fed in
// small reads, and in steady state allocates nothing per read (it compacts in
// place only when the tail reaches 2*overflowKeep).
func TestKeepTailHoldsLastBytesWithoutPerReadAllocation(t *testing.T) {
	body := make([]byte, 50<<20)
	for i := range body {
		body[i] = byte(i % 251)
	}
	for _, chunk := range []int{4 << 10, 32 << 10} {
		cr := &captureReader{}
		cr.scan.Write(body[:96<<10])
		cr.overflow()
		for off := 96 << 10; off < len(body); off += chunk {
			cr.keepTail(body[off:min(off+chunk, len(body))])
		}
		if got := cr.tailView(); !bytes.Equal(got, body[len(body)-overflowKeep:]) {
			t.Fatalf("chunk=%d: tail is %d bytes and not the body's last %d", chunk, len(got), overflowKeep)
		}
		if held := cr.overflowHeld(); held != int64(96<<10+overflowKeep) {
			t.Fatalf("chunk=%d: charged %d, want head %d + tail %d", chunk, held, 96<<10, overflowKeep)
		}
		p := body[:chunk]
		if allocs := testing.AllocsPerRun(1000, func() { cr.keepTail(p) }); allocs != 0 {
			t.Fatalf("chunk=%d: keepTail allocated %.2f times per read in steady state, want 0", chunk, allocs)
		}
	}
}

// exhaustedReplicaCapture returns a capture reader over body, served in
// chunk-byte reads, whose replica buffer total is already held by another
// request, so its first reserve is refused and it overflows at once.
func exhaustedReplicaCapture(t *testing.T, body []byte, chunk int, onDone func(capture.Result)) (*captureReader, func()) {
	t.Helper()
	const total = 1 << 10
	budget := &responseBufferBudget{}
	p := config.ProtectionSettings{MaxResponseBufferBytesPerStream: total, MaxResponseBufferBytesTotal: total}
	holder := budget.newAccount(p, func(error) {})
	holder.force(bufferSlotCapture, total)
	cr := newCaptureReader(context.Background(), &chunkedBody{r: bytes.NewReader(body), chunk: chunk}, false, onDone)
	cr.buffers = budget.newAccount(p, noBufferCancel(t))
	return cr, holder.close
}

func drainCapture(t *testing.T, cr *captureReader) []byte {
	t.Helper()
	var got bytes.Buffer
	buf := make([]byte, 64<<10)
	for {
		n, err := cr.Read(buf)
		got.Write(buf[:n])
		if err == io.EOF {
			return got.Bytes()
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
}

// D22: a capture that overflows after a few bytes because the replica total
// is used up still keeps the body's first overflowKeep bytes, so a small
// response read a few bytes at a time keeps its model and usage.
func TestEarlyOverflowedCaptureKeepsHeadAndMeters(t *testing.T) {
	body := []byte(`{"id":"x","object":"chat.completion","model":"m1","choices":[{"index":0,"message":{"role":"assistant","content":"` +
		strings.Repeat("hi ", 1000) + `"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":5,"total_tokens":12}}`)
	var result capture.Result
	cr, release := exhaustedReplicaCapture(t, body, 3, func(r capture.Result) { result = r })
	defer release()
	if got := drainCapture(t, cr); !bytes.Equal(got, body) {
		t.Fatalf("delivered %d bytes, want the whole %d-byte body", len(got), len(body))
	}
	if !cr.overflowed {
		t.Fatal("capture did not overflow with the replica total used up")
	}
	if result.Model != "m1" || !result.UsageFound || result.Usage.PromptTokens != 7 {
		t.Fatalf("result=%+v, want model m1 and usage prompt_tokens=7", result)
	}
}

// D22: the overflowed usage search walks back past a string VALUE "usage"
// (a prompt-logprob token after the usage block, as vLLM lays it out) to the
// real usage key, whether the body fits in the head or not.
func TestOverflowedUsageSearchSkipsUsageStringValues(t *testing.T) {
	usage := `"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}`
	logprobs := `"prompt_logprobs":[null,{"1":{"logprob":-1,"rank":1,"decoded_token":"usage"}}]`
	big := strings.Repeat("x", 1<<20)
	for name, body := range map[string]string{
		"small body, usage string after the key": `{"model":"m",` + usage + `,` + logprobs + `}`,
		"large body, usage string after the key": `{"model":"m","data":"` + big + `",` + usage + `,` + logprobs + `}`,
		"large body, usage string before the key": `{"model":"m","data":"` + big + `","note":"usage",` + usage + `,` + logprobs + `}`,
	} {
		var result capture.Result
		cr, release := exhaustedReplicaCapture(t, []byte(body), 32<<10, func(r capture.Result) { result = r })
		drainCapture(t, cr)
		release()
		if !cr.overflowed || !result.UsageFound || result.Usage.PromptTokens != 3 || result.Model != "m" {
			t.Errorf("%s: overflowed=%v result=%+v, want usage prompt_tokens=3 and model m", name, cr.overflowed, result)
		}
	}
}
