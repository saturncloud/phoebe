package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"unicode/utf8"

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
// Read is called from ReverseProxy's copy goroutine; Close from its teardown.
// finish() runs at most once (guarded by `done` under mu) and fires onDone.
//
// Abort determination is race-free because it does NOT depend on goroutine
// ordering: finish() reads the request context's Err() as the single source of
// truth. The client request context is cancelled by net/http the instant the
// client disconnects, and ReverseProxy cancels the upstream from that same
// context — so by the time Close()/finish() runs on an abort, ctx.Err() is
// already non-nil. Whoever calls finish() first therefore observes the correct
// Aborted value. (The earlier design used a separate abort-watcher goroutine
// racing Close(); if Close() won, onDone fired with Aborted=false and the
// partial-bill event was lost. Reading ctx eliminates that race.)
//
// scan (the SSE line buffer) is only ever accessed from Read (the single
// ReverseProxy copy goroutine) and from the non-streaming branch of finish(),
// which by then has exclusive access (Read is done). No extra locking needed
// for scan itself.
type captureReader struct {
	src      io.ReadCloser
	streamed bool

	// ctx is the client request context. A non-nil ctx.Err() at finish time
	// means the client disconnected (or the deadline passed) — i.e. an abort.
	ctx context.Context

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

	// logBuf, when non-nil, accumulates a COPY of the forwarded response bytes
	// for M5 I/O logging — bounded by logCap. It is allocated ONLY when the
	// request passed the iolog policy gate; for the overwhelming common case
	// (logging off) it stays nil and the Read path pays zero extra cost.
	//
	// logBuf is written only from Read (the single ReverseProxy copy goroutine)
	// and read only from finish()/the onDone callback after Read is done, so it
	// needs no separate lock beyond the mu already taken for streaming scans.
	logBuf       *bytes.Buffer
	logCap       int  // max bytes to retain in logBuf
	logTruncated bool // true once we hit the cap and stopped appending
}

func newCaptureReader(ctx context.Context, src io.ReadCloser, streamed bool, onDone func(capture.Result)) *captureReader {
	return &captureReader{
		ctx:      ctx,
		src:      src,
		streamed: streamed,
		onDone:   onDone,
		result:   capture.Result{Streamed: streamed},
	}
}

// enableBodyLog turns on response-body capture for M5 I/O logging, retaining up
// to capBytes of the forwarded response. Call BEFORE the body is read (i.e. in
// ModifyResponse). A non-positive cap disables capture. This is the ONLY way
// logBuf becomes non-nil, so requests that don't opt in never allocate it.
func (c *captureReader) enableBodyLog(capBytes int) {
	if capBytes <= 0 {
		return
	}
	c.logBuf = &bytes.Buffer{}
	c.logCap = capBytes
}

// capturedBody returns the buffered response body and whether it was truncated
// at the cap. Returns ("", false) if body logging was not enabled. Safe to call
// from onDone (Read has finished by then).
func (c *captureReader) capturedBody() (body string, truncated bool) {
	if c.logBuf == nil {
		return "", false
	}
	return c.logBuf.String(), c.logTruncated
}

// appendLog copies up to the remaining cap of p into logBuf. Forward-verbatim
// is unaffected: the client already got these exact bytes; this only buffers a
// bounded COPY for the log. Once the cap is hit we stop appending and flag
// truncation — we never grow logBuf unbounded on a large/streamed response.
func (c *captureReader) appendLog(p []byte) {
	if c.logBuf == nil || c.logTruncated {
		return
	}
	remaining := c.logCap - c.logBuf.Len()
	if remaining <= 0 {
		c.logTruncated = true
		return
	}
	if len(p) > remaining {
		// Truncating at a raw byte offset can split a multibyte rune, leaving
		// the buffered copy invalid UTF-8 — which Postgres TEXT rejects, so
		// the whole io_log record would later fail to insert over a log
		// artifact. Back up to the rune start (a rune spans at most utf8.UTFMax
		// bytes, so at most 3 continuation bytes precede the cut; non-UTF-8
		// payloads bound the backup the same way and are sanitised at the
		// sink instead).
		cut := remaining
		for cut > 0 && remaining-cut < utf8.UTFMax-1 && !utf8.RuneStart(p[cut]) {
			cut--
		}
		c.logBuf.Write(p[:cut])
		c.logTruncated = true
		return
	}
	c.logBuf.Write(p)
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
		// M5: when body logging is enabled for this request, retain a bounded
		// COPY of the forwarded bytes. No-op (nil logBuf) when logging is off,
		// so the common path is unchanged.
		c.appendLog(p[:n])
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
//
// Model is the engine's OWN answer to "what served this request" — every
// OpenAI/vLLM chunk and body carries it at the top level. It is the stable
// price key (rating's model_id), distinct from the request's routing identity
// (X-Saturn-Resource-Id, an ephemeral deployment id). We capture it from the
// authoritative source — the engine response — rather than trusting the caller.
type chunk struct {
	Model   string `json:"model"`
	Choices []struct {
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *metering.Usage `json:"usage"`
}

// inspectChunk parses the engine model name, token counts, and finish_reason
// from a chunk payload. Must be called with mu held.
func (c *captureReader) inspectChunk(payload []byte) {
	var ch chunk
	if err := json.Unmarshal(payload, &ch); err != nil {
		return // not a chunk we understand; ignore
	}
	// The model name is identical across every chunk of a response; the first
	// non-empty one wins and later chunks reaffirm it.
	if ch.Model != "" {
		c.result.Model = ch.Model
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

// finish parses the non-streaming body (if any), determines abort from the
// request context, and fires onDone exactly once. Safe to call from multiple
// goroutines; the done guard under mu ensures onDone fires at most once even if
// Read (EOF path) and Close converge.
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

	// The client request context is the single source of truth for abort: if it
	// is cancelled/expired by the time we finalise, the client disconnected
	// before the response completed. Reading it HERE (rather than relying on a
	// separate goroutine to set the flag before finish runs) makes the abort
	// determination independent of goroutine scheduling — whichever path reaches
	// finish() first observes the same, correct value.
	if c.ctx != nil && c.ctx.Err() != nil {
		c.result.Aborted = true
	}

	if c.onDone != nil {
		c.onDone(c.result)
	}
}
