package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"sync"

	"github.com/saturncloud/phoebe/internal/capture"
	"github.com/saturncloud/phoebe/internal/metering"
)

// sseDataPrefix is the framing for every SSE event line vLLM emits.
var sseDataPrefix = []byte("data: ")

// sseDone is the stream terminator. The usage chunk arrives immediately BEFORE
// this line — we must keep scanning past finish_reason to reach it.
var sseDone = []byte("[DONE]")

// captureReader wraps an upstream response body. It is an io.Reader that
// forwards bytes UNCHANGED to the caller (the client) while inspecting them for
// the engine's usage block and finish_reason. It never buffers-then-forwards:
// each Read returns the upstream bytes immediately; capture happens as a
// side effect on the same bytes.
//
// Streaming (SSE) and non-streaming (single JSON body) are both handled:
//   - streaming: scan line-by-line for "data: {...}" chunks, pull usage from
//     the trailing chunk (choices: []) and finish_reason from its own chunk.
//   - non-streaming: accumulate the (small) JSON body and parse usage once.
//
// # Concurrency model
//
// Read is called from ReverseProxy's copy goroutine. markAborted is called from
// the abort-watcher goroutine (launched by handleProxy) when the client context
// is cancelled. mu guards result and done so both paths are race-free.
//
// Ordering guarantee: markAborted sets result.Aborted=true and then calls
// finish() — both paths acquire mu before touching result or done. Therefore:
//   - If the abort-watcher wins: result.Aborted=true is visible to onDone.
//   - If Read/Close wins first: finish() already ran, markAborted sees
//     done=true and skips the second fire. The stream completed cleanly before
//     context cancel — Aborted=false is correct.
//
// scan (the SSE line buffer) is only ever accessed from Read (the single
// ReverseProxy copy goroutine) and from the non-streaming branch of finish(),
// which by then has exclusive access (Read is done). No extra locking needed
// for scan itself.
type captureReader struct {
	src      io.ReadCloser
	streamed bool

	// scan holds bytes not yet split into complete SSE lines (streaming) or
	// the whole accumulating body (non-streaming). Only touched from Read and
	// the non-streaming finish() path.
	scan bytes.Buffer

	// mu guards result and done. All paths that read or write result or done
	// must hold mu for the duration of the access.
	mu     sync.Mutex
	result capture.Result
	onDone func(capture.Result)
	done   bool

	// finishedCh is closed by finish() when onDone fires. The abort-watcher
	// goroutine in handleProxy selects on both r.Context().Done() and
	// finishedCh so it exits on either a client disconnect or a clean
	// completion — preventing goroutine leaks on normal (non-aborted) requests.
	finishedCh chan struct{}
}

func newCaptureReader(src io.ReadCloser, streamed bool, onDone func(capture.Result)) *captureReader {
	return &captureReader{
		src:        src,
		streamed:   streamed,
		onDone:     onDone,
		result:     capture.Result{Streamed: streamed},
		finishedCh: make(chan struct{}),
	}
}

func (c *captureReader) Read(p []byte) (int, error) {
	n, err := c.src.Read(p)
	if n > 0 {
		// Inspect a COPY of exactly the bytes we forward. The bytes in p are
		// returned to the client verbatim; we never mutate them.
		c.scan.Write(p[:n])
		if c.streamed {
			c.mu.Lock()
			c.scanSSELines()
			c.mu.Unlock()
		}
	}
	if err == io.EOF {
		c.finish()
	}
	return n, err
}

func (c *captureReader) Close() error {
	// A Close without EOF (e.g. client abort cancelling the upstream) still
	// finalises capture so the emitter learns what we saw.
	c.finish()
	return c.src.Close()
}

// scanSSELines consumes complete lines from the buffer, parsing any usage /
// finish_reason it finds. Incomplete trailing data stays buffered for the next
// Read. We do NOT stop at finish_reason — the usage chunk comes later.
//
// Must be called with mu held.
func (c *captureReader) scanSSELines() {
	buf := c.scan.Bytes()
	consumed := 0
	for {
		idx := bytes.IndexByte(buf[consumed:], '\n')
		if idx < 0 {
			break // no complete line left
		}
		line := buf[consumed : consumed+idx]
		consumed += idx + 1
		// inspectSSELine only reads from line (and copies what it keeps via
		// json.Unmarshal), so reading before we rewrite the buffer is safe.
		c.inspectSSELine(bytes.TrimRight(line, "\r"))
	}
	if consumed == 0 {
		return
	}
	// Keep only the unconsumed tail. Copy it OUT first to avoid aliasing the
	// buffer's backing array while we reset and rewrite it.
	tail := append([]byte(nil), buf[consumed:]...)
	c.scan.Reset()
	c.scan.Write(tail)
}

func (c *captureReader) inspectSSELine(line []byte) {
	if !bytes.HasPrefix(line, sseDataPrefix) {
		return
	}
	payload := bytes.TrimSpace(line[len(sseDataPrefix):])
	if len(payload) == 0 || bytes.Equal(payload, sseDone) {
		return
	}
	c.inspectChunk(payload)
}

// chunk is the minimal shape we read from an SSE chunk or a non-streaming body.
type chunk struct {
	Choices []struct {
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *metering.Usage `json:"usage"`
}

// inspectChunk parses token counts and finish_reason from a chunk payload.
// Must be called with mu held.
func (c *captureReader) inspectChunk(payload []byte) {
	var ch chunk
	if err := json.Unmarshal(payload, &ch); err != nil {
		return // not a chunk we understand; ignore
	}
	for _, choice := range ch.Choices {
		if choice.FinishReason != nil && *choice.FinishReason != "" {
			c.result.FinishReason = *choice.FinishReason
		}
	}
	if ch.Usage != nil {
		c.result.Usage = *ch.Usage
		c.result.UsageFound = true
	}
}

// finish parses the non-streaming body (if any) and fires onDone exactly once.
// Safe to call from multiple goroutines; the done guard under mu ensures
// onDone fires at most once even if Read (EOF path), Close, and markAborted
// all converge.
func (c *captureReader) finish() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done {
		return
	}
	c.done = true

	if !c.streamed {
		// Single JSON body: parse the whole thing once.
		c.inspectChunk(bytes.TrimSpace(c.scan.Bytes()))
	}

	if c.onDone != nil {
		c.onDone(c.result)
	}
	// Signal the abort-watcher goroutine that we're done so it can exit
	// without leaking on normal (non-aborted) completions.
	close(c.finishedCh)
}

// markAborted records that the client disconnected before the response
// completed and ensures the finalisation callback fires with Aborted=true.
// It is safe to call from any goroutine concurrently with Read/Close.
//
// If finish() has not yet run, markAborted sets the abort flag and triggers
// it — onDone will see Aborted=true. If finish() already ran (a clean EOF
// arrived before the context cancel), the stream completed normally and we do
// NOT re-fire onDone; a clean completion with Aborted=false is correct.
func (c *captureReader) markAborted() {
	c.mu.Lock()
	c.result.Aborted = true
	alreadyDone := c.done
	c.mu.Unlock()

	if !alreadyDone {
		// finish() re-acquires mu and fires onDone with Aborted=true now set.
		c.finish()
	}
}
