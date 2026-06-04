package proxy

import (
	"bytes"
	"io"
	"strings"
	"testing"

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

func TestTeeAbortFinalizes(t *testing.T) {
	// Simulate a client abort: only partial stream arrives, then Close.
	partial := `data: {"choices":[{"index":0,"delta":{"content":"Hel"}}]}

`
	cr := newCaptureReader(io.NopCloser(strings.NewReader(partial)), true, func(capture.Result) {})
	buf := make([]byte, 8)
	_, _ = cr.Read(buf) // partial read
	cr.markAborted()
	var got capture.Result
	cr.onDone = func(r capture.Result) { got = r }
	_ = cr.Close()

	if !got.Aborted {
		t.Fatal("Aborted not set after markAborted + Close")
	}
	if got.UsageFound {
		t.Fatal("partial stream should not have usage")
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
