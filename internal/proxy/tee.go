package proxy

import (
	"bytes"
	"context"
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
// scan, head, tail and overflowed are written only from Read (the single
// ReverseProxy copy goroutine) but read by finish(), which Close can run on
// another goroutine while Read is still in progress (a client abort), so
// every access to them holds mu.
type captureReader struct {
	src      io.ReadCloser
	streamed bool

	// ctx is the client request context. A non-nil ctx.Err() at finish time
	// means the client disconnected (or the deadline passed) — i.e. an abort.
	ctx context.Context

	// scan holds bytes not yet split into complete SSE lines (streaming) or
	// the whole accumulating body (non-streaming). Guarded by mu.
	scan bytes.Buffer

	// mu guards result, done, scan, head, tail and overflowed. All paths that
	// read or write any of them must hold mu for the duration of the access.
	mu     sync.Mutex
	result capture.Result
	onDone func(capture.Result)
	done   bool
	// onFirstRead marks the prefill→decode boundary for admission. sync.Once
	// protects unusual Readers that return both bytes and an error or are closed
	// concurrently with the copy loop.
	firstRead   sync.Once
	onFirstRead func()

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

	// buffers charges scan's size against the gateway protection limits on
	// buffered response bytes (per request and per replica). nil = unbounded
	// (unit tests that build a reader directly).
	buffers *responseBufferAccount
	// bufferErr is the limit error once scan outgrew its allowance; every
	// later Read returns it.
	bufferErr error

	// A non-streamed body that outgrows its buffer allowance is never cut:
	// the client gets every byte. Capture then keeps only the body's head
	// (where the top-level "model" sits) and a rolling tail (where "usage"
	// sits in OpenAI-style bodies) and parses those at finish. Guarded by mu.
	// tail may hold up to 2*overflowKeep bytes between compactions (see
	// keepTail); only its last overflowKeep bytes are the tail (tailView).
	overflowed bool
	head, tail []byte

	// deferBufferClose leaves closing buffers to the reader that wraps this
	// one (the usage filter), which can still hold the stream's last line
	// after upstream EOF. The account must stay open, and keep charging,
	// for as long as any reader holds bytes.
	deferBufferClose bool
}

// overflowKeep is how much of a non-streamed body's head and tail capture
// keeps once the body outgrows its buffer allowance.
const overflowKeep = 256 << 10

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

func (c *captureReader) setOnFirstRead(fn func()) { c.onFirstRead = fn }

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
	// Rune-safe truncation (a raw byte cut could split a multibyte rune, leaving
	// invalid UTF-8 that fails the io_log INSERT) lives in one place —
	// truncateAtRuneBoundary — shared with the request-body cap.
	keep, cut := truncateAtRuneBoundary(p, remaining)
	c.logBuf.Write(keep)
	if cut {
		c.logTruncated = true
	}
}

func (c *captureReader) Read(p []byte) (int, error) {
	if c.bufferErr != nil {
		return 0, c.bufferErr
	}
	n, err := c.src.Read(p)
	if n > 0 {
		c.firstRead.Do(func() {
			if c.onFirstRead != nil {
				c.onFirstRead()
			}
		})
		// Inspect a COPY of exactly the bytes we forward. The bytes in p are
		// returned to the client verbatim; we never mutate them.
		//
		// scan, head, tail and overflowed are read by finish(), which Close
		// can run on another goroutine, so every change to them happens under
		// mu. The buffer account takes its own lock; it is charged after mu
		// is released, and finish() calling buffers.close under mu keeps the
		// one lock order (mu, then the account's).
		c.mu.Lock()
		if c.overflowed {
			// A capture can overflow after only a few bytes, when the replica
			// total (not its own limit) refuses the reserve. Keep filling the
			// head up to overflowKeep so the top-level "model" is still in it
			// (D22 keeps the first overflowKeep bytes). Head and tail may
			// overlap; the charge stays at most 2*overflowKeep (D31).
			if room := overflowKeep - len(c.head); room > 0 {
				c.head = append(c.head, p[:min(n, room)]...)
			}
			c.keepTail(p[:n])
			held := c.overflowHeld()
			c.mu.Unlock()
			c.appendLog(p[:n])
			// D22 keeps the head and tail whatever the limits say, so they are
			// charged even past them to keep the replica total honest.
			c.buffers.force(bufferSlotCapture, held)
			if err == io.EOF {
				c.finish()
			}
			return n, err
		}
		c.scan.Write(p[:n])
		if c.streamed {
			c.scanSSELines()
		}
		scanLen := int64(c.scan.Len())
		// scan now holds the unparsed tail of a stream (normally under one
		// line) or the whole non-streamed body so far. A non-streamed body
		// past its allowance switches to keeping only its head and tail.
		forceHeld := int64(-1)
		if !c.streamed && c.buffers.reserve(bufferSlotCapture, scanLen) != nil {
			forceHeld = c.overflow()
		}
		c.mu.Unlock()
		// M5: when body logging is enabled for this request, retain a bounded
		// COPY of the forwarded bytes. No-op (nil logBuf) when logging is off,
		// so the common path is unchanged.
		c.appendLog(p[:n])
		if forceHeld >= 0 {
			// D22 keeps the head and tail whatever the limits say, so they are
			// charged even past them to keep the replica total honest.
			c.buffers.force(bufferSlotCapture, forceHeld)
		} else if c.streamed {
			// Past the protection limit a stream is cut: these bytes still
			// reach the client, the error ends the copy, and the account has
			// cancelled the request so capture records it as aborted.
			if berr := c.buffers.set(bufferSlotCapture, scanLen); berr != nil {
				c.bufferErr = berr
				c.mu.Lock()
				c.scan.Reset()
				c.mu.Unlock()
				return n, berr
			}
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
		next := *ch.Usage
		current := c.result.Usage
		// Some engines omit prompt_tokens_details after reporting it once.
		// Absence means no new cache information, not that cumulative cached
		// usage fell to zero. Preserve the last known cache count while still
		// accepting newer prompt, completion, and total counts.
		if c.result.UsageFound && next.PromptTokensDetails == nil && current.PromptTokensDetails != nil {
			details := *current.PromptTokensDetails
			next.PromptTokensDetails = &details
		}
		if !c.result.UsageFound ||
			(next.PromptTokens >= current.PromptTokens &&
				next.CompletionTokens >= current.CompletionTokens &&
				next.TotalTokens >= current.TotalTokens &&
				next.CachedTokens() >= current.CachedTokens()) {
			c.result.Usage = next
			c.result.UsageFound = true
		}
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

	if c.streamed {
		// EOF and transport errors may arrive immediately after a complete SSE
		// data line without its optional trailing newline. Parse that residual
		// line once; malformed partial JSON remains safely ignored.
		if c.scan.Len() > 0 {
			c.inspectSSELine(bytes.TrimRight(c.scan.Bytes(), "\r"))
		}
	} else if c.overflowed {
		c.inspectHeadAndTail()
	} else {
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

	if !c.deferBufferClose {
		c.buffers.close()
	}

	if c.onDone != nil {
		c.onDone(c.result)
	}
}

// overflow switches a non-streamed capture from holding the whole body to
// holding its head and tail, and returns the bytes to charge for them (the
// caller charges them with buffers.force after releasing mu).
//
// Must be called with mu held.
func (c *captureReader) overflow() int64 {
	body := c.scan.Bytes()
	c.head = append([]byte(nil), body[:min(len(body), overflowKeep)]...)
	// Copy only the last overflowKeep bytes into a fresh array sized for
	// keepTail's compaction, so the large scan array is not kept alive.
	c.tail = append(make([]byte, 0, 2*overflowKeep), body[max(0, len(body)-overflowKeep):]...)
	c.scan = bytes.Buffer{} // release the large backing array
	c.overflowed = true
	return c.overflowHeld()
}

// overflowHeld is what an overflowed capture is charged: the head plus the
// tail view, at most 2*overflowKeep (D31) whatever spare bytes tail holds
// between compactions.
//
// Must be called with mu held.
func (c *captureReader) overflowHeld() int64 {
	return int64(len(c.head) + len(c.tailView()))
}

// keepTail appends p to the rolling tail. To avoid reallocating and copying
// the tail on every read, it compacts only once tail reaches 2*overflowKeep,
// moving the last overflowKeep bytes to the front of the same array.
//
// Must be called with mu held.
func (c *captureReader) keepTail(p []byte) {
	c.tail = append(c.tail, p...)
	if len(c.tail) > 2*overflowKeep {
		n := copy(c.tail, c.tail[len(c.tail)-overflowKeep:])
		c.tail = c.tail[:n]
	}
}

// tailView is the rolling tail: the last overflowKeep bytes seen.
//
// Must be called with mu held.
func (c *captureReader) tailView() []byte {
	t := c.tail
	if len(t) > overflowKeep {
		t = t[len(t)-overflowKeep:]
	}
	return t
}

// inspectHeadAndTail recovers model and usage from an overflowed body. When
// the head is not full, nothing was dropped and the head is the whole body, so
// it is parsed like any non-streamed body. Otherwise the top-level "model" is
// read by walking the head's keys in order, and "usage" from the last "usage"
// in the tail that is followed by ':' and an object which decodes. Walking
// back past earlier matches skips a string VALUE "usage" (for example a
// logprob token after the usage block). A body laid out otherwise yields no
// usage, recorded as an unmetered attempt.
//
// Must be called with mu held.
func (c *captureReader) inspectHeadAndTail() {
	if len(c.head) < overflowKeep {
		var whole chunk
		if json.Unmarshal(bytes.TrimSpace(c.head), &whole) == nil {
			c.inspectChunk(bytes.TrimSpace(c.head))
			return
		}
	}
	dec := json.NewDecoder(bytes.NewReader(c.head))
	if tok, err := dec.Token(); err == nil && tok == json.Delim('{') {
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				break
			}
			if key == "model" {
				var model string
				if dec.Decode(&model) == nil && model != "" {
					c.result.Model = model
				}
				break
			}
			if skipValue(dec) != nil {
				break
			}
		}
	}
	tail := c.tailView()
	for end := len(tail); end > 0; {
		idx := bytes.LastIndex(tail[:end], []byte(`"usage"`))
		if idx < 0 {
			return
		}
		end = idx
		rest := bytes.TrimLeft(tail[idx+len(`"usage"`):], " \t\r\n")
		if len(rest) == 0 || rest[0] != ':' {
			continue
		}
		if rest = bytes.TrimLeft(rest[1:], " \t\r\n"); len(rest) == 0 || rest[0] != '{' {
			continue
		}
		var usage metering.Usage
		if json.NewDecoder(bytes.NewReader(rest)).Decode(&usage) == nil {
			c.result.Usage = usage
			c.result.UsageFound = true
			return
		}
	}
}
