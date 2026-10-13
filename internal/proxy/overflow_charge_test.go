package proxy

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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
	cr := newCaptureReader(nil, &chunkedBody{r: bytes.NewReader(body), chunk: 32 << 10}, false, func(r capture.Result) { result = r })
	cr.buffers = account

	var got bytes.Buffer
	buf := make([]byte, 32<<10)
	for {
		n, err := cr.Read(buf)
		got.Write(buf[:n])
		if err == nil && cr.overflowed {
			want := int64(len(cr.head) + len(cr.tail))
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
	cr := newCaptureReader(nil, &chunkedBody{r: bytes.NewReader(body), chunk: 64 << 10}, false, nil)
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
			want := int64(total + len(cr.head) + len(cr.tail))
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
