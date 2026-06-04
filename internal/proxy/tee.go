package proxy

import (
	"bytes"
	"encoding/json"
	"io"

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
type captureReader struct {
	src      io.ReadCloser
	streamed bool

	// scan holds bytes not yet split into complete SSE lines (streaming) or
	// the whole accumulating body (non-streaming).
	scan bytes.Buffer

	result capture.Result
	onDone func(capture.Result)
	done   bool
}

func newCaptureReader(src io.ReadCloser, streamed bool, onDone func(capture.Result)) *captureReader {
	return &captureReader{
		src:      src,
		streamed: streamed,
		onDone:   onDone,
		result:   capture.Result{Streamed: streamed},
	}
}

func (c *captureReader) Read(p []byte) (int, error) {
	n, err := c.src.Read(p)
	if n > 0 {
		// Inspect a COPY of exactly the bytes we forward. The bytes in p are
		// returned to the client verbatim; we never mutate them.
		c.scan.Write(p[:n])
		if c.streamed {
			c.scanSSELines()
		}
	}
	if err == io.EOF {
		c.finish()
	}
	return n, err
}

func (c *captureReader) Close() error {
	// A Close without EOF (e.g. client abort cancelling the upstream) still
	// finalizes capture so the emitter learns what we saw.
	c.finish()
	return c.src.Close()
}

// scanSSELines consumes complete lines from the buffer, parsing any usage /
// finish_reason it finds. Incomplete trailing data stays buffered for the next
// Read. We do NOT stop at finish_reason — the usage chunk comes later.
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

// finish parses the non-streaming body (if any) and fires the callback exactly
// once.
func (c *captureReader) finish() {
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
}

// markAborted records that the client disconnected before completion. Called
// before Close on the abort path.
func (c *captureReader) markAborted() {
	c.result.Aborted = true
}
