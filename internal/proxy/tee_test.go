package proxy

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/capture"
)

// realistic vLLM streaming response: content chunks, then a chunk carrying
// finish_reason (empty delta), then the usage chunk (choices: []), then [DONE].
// This ordering is the LiteLLM trap — we must NOT stop at finish_reason.
const vllmStream = `data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"Hello"}}]}

data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":" world"}}]}

data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

data: {"id":"c1","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":2006,"total_tokens":2306,"completion_tokens":300,"prompt_tokens_details":{"cached_tokens":1920}}}

data: [DONE]

`

// drain reads the whole captureReader (as the client would), returning the
// forwarded bytes and the captured result.
func drain(t *testing.T, body string, streamed bool) ([]byte, capture.Result) {
	t.Helper()
	var got capture.Result
	cr := newCaptureReader(io.NopCloser(strings.NewReader(body)), streamed, func(r capture.Result) {
		got = r
	})
	forwarded, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	_ = cr.Close()
	return forwarded, got
}

func TestTeeStreamingCapturesUsageAfterFinishReason(t *testing.T) {
	forwarded, res := drain(t, vllmStream, true)

	// 1. Bytes forwarded to the client must be byte-identical to upstream.
	if string(forwarded) != vllmStream {
		t.Fatalf("forwarded bytes differ from upstream\n got: %q", string(forwarded))
	}
	// 2. Usage captured from the trailing chunk, not lost at finish_reason.
	if !res.UsageFound {
		t.Fatal("usage not captured")
	}
	if res.Usage.PromptTokens != 2006 || res.Usage.CompletionTokens != 300 {
		t.Fatalf("token counts wrong: %+v", res.Usage)
	}
	if res.Usage.CachedTokens() != 1920 {
		t.Fatalf("cached tokens = %d, want 1920", res.Usage.CachedTokens())
	}
	if res.FinishReason != "stop" {
		t.Fatalf("finish_reason = %q, want stop", res.FinishReason)
	}
	if !res.Streamed {
		t.Fatal("Streamed should be true")
	}
}

func TestTeeStreamingNoCacheDetails(t *testing.T) {
	// Cache miss / flag off: prompt_tokens_details omitted entirely.
	stream := `data: {"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}

data: {"choices":[],"usage":{"prompt_tokens":50,"total_tokens":80,"completion_tokens":30}}

data: [DONE]

`
	_, res := drain(t, stream, true)
	if !res.UsageFound {
		t.Fatal("usage not captured")
	}
	if res.Usage.CachedTokens() != 0 {
		t.Fatalf("cached tokens = %d, want 0 (absent details)", res.Usage.CachedTokens())
	}
	if res.FinishReason != "length" {
		t.Fatalf("finish_reason = %q, want length", res.FinishReason)
	}
}

func TestTeeNonStreaming(t *testing.T) {
	body := `{"id":"c1","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"total_tokens":15,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":8}}}`
	forwarded, res := drain(t, body, false)

	if string(forwarded) != body {
		t.Fatal("non-streaming body altered")
	}
	if !res.UsageFound || res.Usage.PromptTokens != 10 || res.Usage.CompletionTokens != 5 {
		t.Fatalf("usage wrong: %+v", res.Usage)
	}
	if res.Usage.CachedTokens() != 8 {
		t.Fatalf("cached = %d, want 8", res.Usage.CachedTokens())
	}
	if res.FinishReason != "stop" {
		t.Fatalf("finish_reason = %q", res.FinishReason)
	}
	if res.Streamed {
		t.Fatal("Streamed should be false")
	}
}

func TestTeeNoUsage(t *testing.T) {
	// A response with no usage block at all (e.g. an error body, non-OpenAI).
	_, res := drain(t, `{"error":"something"}`, false)
	if res.UsageFound {
		t.Fatal("UsageFound should be false")
	}
}

func TestTeeStreamingSplitAcrossReads(t *testing.T) {
	// Feed the stream one byte at a time to prove line reassembly across Read
	// boundaries — the realistic case where chunks arrive fragmented.
	var got capture.Result
	cr := newCaptureReader(io.NopCloser(iotest1ByteReader(vllmStream)), true, func(r capture.Result) {
		got = r
	})
	forwarded, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	_ = cr.Close()

	if !bytes.Equal(forwarded, []byte(vllmStream)) {
		t.Fatal("forwarded bytes differ under fragmented reads")
	}
	if !got.UsageFound || got.Usage.PromptTokens != 2006 || got.Usage.CachedTokens() != 1920 {
		t.Fatalf("usage lost under fragmented reads: %+v found=%v", got.Usage, got.UsageFound)
	}
}

// TestTeeAbortFinalizes verifies that markAborted + Close produces Aborted=true
// with no usage. The onDone callback is passed at construction so markAborted
// can fire it directly — this matches the real proxy path.
func TestTeeAbortFinalizes(t *testing.T) {
	partial := `data: {"choices":[{"index":0,"delta":{"content":"Hel"}}]}

`
	var got capture.Result
	cr := newCaptureReader(io.NopCloser(strings.NewReader(partial)), true, func(r capture.Result) {
		got = r
	})
	buf := make([]byte, 8)
	_, _ = cr.Read(buf) // partial read
	cr.markAborted()
	// Close is a no-op for done (finish already fired from markAborted), but
	// we call it to mirror the real path.
	_ = cr.Close()

	if !got.Aborted {
		t.Fatal("Aborted not set after markAborted")
	}
	if got.UsageFound {
		t.Fatal("partial stream should not have usage")
	}
}

// TestTeeOnDoneFiresExactlyOnce ensures the callback is invoked exactly once
// even when markAborted, Read-EOF, and Close all converge.
func TestTeeOnDoneFiresExactlyOnce(t *testing.T) {
	var count int32
	cr := newCaptureReader(io.NopCloser(strings.NewReader(vllmStream)), true, func(capture.Result) {
		atomic.AddInt32(&count, 1)
	})

	var wg sync.WaitGroup
	wg.Add(3)

	// Three concurrent paths that all try to fire finish().
	go func() { defer wg.Done(); _, _ = io.ReadAll(cr) }()                        // normal EOF path
	go func() { defer wg.Done(); cr.markAborted() }()                             // abort path
	go func() { defer wg.Done(); time.Sleep(time.Millisecond); _ = cr.Close() }() // close path

	wg.Wait()

	if n := atomic.LoadInt32(&count); n != 1 {
		t.Fatalf("onDone fired %d times, want exactly 1", n)
	}
}

// TestTeeMarkAbortedAfterEOFIsNoop verifies that if the stream completed
// cleanly (EOF) before markAborted is called, the already-fired onDone is NOT
// re-fired and the original Aborted=false result stands.
func TestTeeMarkAbortedAfterEOFIsNoop(t *testing.T) {
	var results []capture.Result
	cr := newCaptureReader(io.NopCloser(strings.NewReader(vllmStream)), true, func(r capture.Result) {
		results = append(results, r)
	})
	// Drain to EOF — onDone fires here with Aborted=false.
	_, _ = io.ReadAll(cr)
	_ = cr.Close()

	// Now simulate a late context-cancel: markAborted should be a no-op.
	cr.markAborted()

	if len(results) != 1 {
		t.Fatalf("onDone fired %d times, want 1", len(results))
	}
	if results[0].Aborted {
		t.Fatal("clean completion should not be marked aborted by a late cancel")
	}
}

// TestTeeAbortWithPartialUsage verifies that if some SSE chunks (including a
// usage block) arrived before the abort, the result carries both Aborted=true
// and the captured usage.
//
// We call markAborted() directly before Close() to simulate the abort path
// where finish() has not yet run. We use a blocking source so that io.ReadAll
// (or the equivalent) never returns EOF on its own, ensuring finish() is only
// triggered by markAborted().
func TestTeeAbortWithPartialUsage(t *testing.T) {
	streamWithUsage := `data: {"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}

data: {"choices":[],"usage":{"prompt_tokens":100,"total_tokens":130,"completion_tokens":30}}

`
	// blockAfterReader is an io.ReadCloser that returns the given bytes then
	// blocks forever (simulating a stream with more data still to come).
	src := &blockAfterReader{data: []byte(streamWithUsage)}

	var got capture.Result
	cr := newCaptureReader(src, true, func(r capture.Result) {
		got = r
	})

	// Drain the pre-loaded bytes in a goroutine; it will block after them.
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		buf := make([]byte, 4096)
		for {
			_, err := cr.Read(buf)
			if err != nil {
				return
			}
		}
	}()

	// Give the goroutine time to consume the pre-loaded bytes.
	time.Sleep(5 * time.Millisecond)

	// Abort: sets Aborted=true and fires finish() while Read is still blocked.
	cr.markAborted()

	// Unblock Read by closing the source.
	src.unblock()
	<-readDone

	if !got.Aborted {
		t.Fatal("expected Aborted=true")
	}
	if !got.UsageFound {
		t.Fatal("expected UsageFound=true — usage block was present before abort")
	}
	if got.Usage.PromptTokens != 100 || got.Usage.CompletionTokens != 30 {
		t.Fatalf("unexpected token counts: %+v", got.Usage)
	}
}

// blockAfterReader yields its pre-loaded data then blocks until unblock() is
// called, at which point further reads return io.ErrClosedPipe.
type blockAfterReader struct {
	data    []byte
	pos     int
	mu      sync.Mutex
	closeCh chan struct{}
	once    sync.Once
}

func (b *blockAfterReader) init() {
	b.once.Do(func() { b.closeCh = make(chan struct{}) })
}

func (b *blockAfterReader) Read(p []byte) (int, error) {
	b.init()
	b.mu.Lock()
	if b.pos < len(b.data) {
		n := copy(p, b.data[b.pos:])
		b.pos += n
		b.mu.Unlock()
		return n, nil
	}
	b.mu.Unlock()
	// No more pre-loaded data: block until unblock() is called.
	<-b.closeCh
	return 0, io.ErrClosedPipe
}

func (b *blockAfterReader) Close() error {
	b.unblock()
	return nil
}

func (b *blockAfterReader) unblock() {
	b.init()
	// Close the channel idempotently.
	select {
	case <-b.closeCh:
	default:
		close(b.closeCh)
	}
}

// iotest1ByteReader returns a reader that yields one byte per Read.
func iotest1ByteReader(s string) io.Reader {
	return &oneByteReader{data: []byte(s)}
}

type oneByteReader struct {
	data []byte
	pos  int
}

func (r *oneByteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	p[0] = r.data[r.pos]
	r.pos++
	return 1, nil
}
