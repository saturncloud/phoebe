package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/saturncloud/phoebe/internal/config"
)

func TestUsageFilterRestoresClientStreamShape(t *testing.T) {
	upstream := "data: {\"model\":\"m\",\"choices\":[{\"delta\":{\"content\":\"a\"}}],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":1}}\n\n" +
		"data: {\"model\":\"m\",\"choices\":[{\"delta\":{\"content\":\"b\"}}],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2}}\n\n" +
		"data: {\"model\":\"m\",\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2}}\n\n" +
		"data: [DONE]\n\n"

	t.Run("caller omitted usage", func(t *testing.T) {
		filtered := newUsageFilterReader(io.NopCloser(strings.NewReader(upstream)), false, nil)
		got, err := io.ReadAll(filtered)
		if err != nil {
			t.Fatal(err)
		}
		text := string(got)
		if strings.Contains(text, "usage") {
			t.Fatalf("usage leaked to caller: %s", text)
		}
		if !strings.Contains(text, `"content":"a"`) || !strings.Contains(text, "data: [DONE]") {
			t.Fatalf("content stream was not preserved: %s", text)
		}
	})

	t.Run("caller requested final usage", func(t *testing.T) {
		filtered := newUsageFilterReader(io.NopCloser(strings.NewReader(upstream)), true, nil)
		got, err := io.ReadAll(filtered)
		if err != nil {
			t.Fatal(err)
		}
		text := string(got)
		if count := strings.Count(text, `"usage":null`); count != 2 {
			t.Fatalf("ordinary usage:null count = %d, want 2: %s", count, text)
		}
		if count := strings.Count(text, `"prompt_tokens"`); count != 1 {
			t.Fatalf("non-null usage count = %d, want final event only: %s", count, text)
		}
		if !strings.Contains(text, `"choices":[]`) {
			t.Fatalf("final usage event missing: %s", text)
		}
	})
}

type usageOneByteReader struct{ io.Reader }

func (r usageOneByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.Reader.Read(p)
}

var errUsageStream = errors.New("usage stream failed")

type dataAndErrorReader struct {
	data []byte
	done bool
}

func (r *dataAndErrorReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, errUsageStream
	}
	r.done = true
	return copy(p, r.data), errUsageStream
}

func TestUsageFilterFragmentationAndTerminalError(t *testing.T) {
	input := "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}],\"usage\":{\"prompt_tokens\":4}}\r\n\r\n" +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":4}}"

	fragmented := newUsageFilterReader(io.NopCloser(usageOneByteReader{Reader: strings.NewReader(input)}), true, nil)
	var got bytes.Buffer
	buf := make([]byte, 3)
	for {
		n, err := fragmented.Read(buf)
		got.Write(buf[:n])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if text := got.String(); !strings.Contains(text, `"usage":null`) ||
		!strings.Contains(text, `"choices":[]`) || !strings.Contains(text, "\r\n\r\n") {
		t.Fatalf("fragmented stream changed shape: %q", text)
	}

	failing := newUsageFilterReader(io.NopCloser(&dataAndErrorReader{data: []byte("data: {\"choices\":[{}],\"usage\":{}}")}), false, nil)
	got.Reset()
	_, err := io.Copy(&got, failing)
	if !errors.Is(err, errUsageStream) {
		t.Fatalf("terminal error = %v, want %v", err, errUsageStream)
	}
	if got.String() != "data: {\"choices\":[{}]}" {
		t.Fatalf("bytes before terminal error = %q", got.String())
	}
}

func TestUsageFilterRejectsOversizedEventLine(t *testing.T) {
	filtered := newUsageFilterReader(io.NopCloser(strings.NewReader("123456789\n")), false, nil).(*usageFilterReader)
	filtered.maxLineBytes = 8
	if _, err := io.ReadAll(filtered); !errors.Is(err, errSSEEventLineTooLarge) {
		t.Fatalf("oversized event error = %v, want %v", err, errSSEEventLineTooLarge)
	}
}

func TestClientRequestedStreamUsageRestoresBody(t *testing.T) {
	body := `{"stream":true,"stream_options":{"include_usage":true},"messages":[]}`
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://example.test", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	requested, err := clientRequestedStreamUsage(req)
	if err != nil {
		t.Fatal(err)
	}
	if !requested.includeUsage || requested.continuousUsage {
		t.Fatal("include_usage=true was not detected")
	}
	got, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body {
		t.Fatalf("request body changed: %q", got)
	}
}

func TestClientRequestedContinuousUsage(t *testing.T) {
	body := `{"stream":true,"stream_options":{"include_usage":true,"continuous_usage_stats":true}}`
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://example.test", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	requested, err := clientRequestedStreamUsage(req)
	if err != nil {
		t.Fatal(err)
	}
	if !requested.includeUsage || !requested.continuousUsage {
		t.Fatalf("preference = %+v, want include and continuous", requested)
	}
}

// closeRecorder is an upstream body that records Close.
type closeRecorder struct {
	io.Reader
	closed bool
}

func (c *closeRecorder) Close() error { c.closed = true; return nil }

// B4: when the SSE line the filter holds would exceed the per-stream response
// buffer limit, the filter fails closed: it closes the upstream body, surfaces
// errResponseBufferLimit, returns no bytes of the oversize line, never holds
// more than the limit, and the request's account returns every byte to the
// replica budget on close. The control stream (lines under the limit) passes
// through unchanged.
func TestUsageFilterBufferLimitClosesSourceAndSurfacesError(t *testing.T) {
	const perStream = 8 << 10
	p := config.ProtectionSettings{MaxResponseBufferBytesPerStream: perStream, MaxResponseBufferBytesTotal: 1 << 20}

	t.Run("line over the limit", func(t *testing.T) {
		var budget responseBufferBudget
		var cause error
		account := budget.newAccount(p, func(err error) { cause = err })
		short := "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n"
		long := "data: {\"choices\":[{\"delta\":{\"content\":\"" + strings.Repeat("x", 4*perStream) + "\"}}]}\n\n"
		src := &closeRecorder{Reader: strings.NewReader(short + long)}
		filtered := newUsageFilterReader(src, false, account)

		var got bytes.Buffer
		buf := make([]byte, 512)
		var err error
		for err == nil {
			var n int
			n, err = filtered.Read(buf)
			got.Write(buf[:n])
			if held := budget.held.Load(); held > perStream {
				t.Fatalf("filter held %d bytes, over the %d per-stream limit", held, perStream)
			}
		}
		if !errors.Is(err, errResponseBufferLimit) || !errors.Is(cause, errResponseBufferLimit) {
			t.Fatalf("err=%v cause=%v, want %v", err, cause, errResponseBufferLimit)
		}
		if !src.closed {
			t.Fatal("upstream body was not closed on the buffer-limit failure")
		}
		if got.String() != short {
			t.Fatalf("bytes returned = %d, want only the %d bytes before the oversize line", got.Len(), len(short))
		}
		account.close()
		if held := budget.held.Load(); held != 0 {
			t.Fatalf("budget holds %d bytes after the account closed, want 0", held)
		}
	})

	t.Run("lines under the limit pass unchanged", func(t *testing.T) {
		var budget responseBufferBudget
		account := budget.newAccount(p, func(error) { t.Error("an under-limit stream fired the buffer limit") })
		line := "data: {\"choices\":[{\"delta\":{\"content\":\"" + strings.Repeat("y", perStream/2) + "\"}}]}\n\n"
		stream := line + line + "data: [DONE]\n\n"
		src := &closeRecorder{Reader: strings.NewReader(stream)}
		got, err := io.ReadAll(newUsageFilterReader(src, false, account))
		if err != nil || string(got) != stream {
			t.Fatalf("err=%v, under-limit stream changed (len %d, want %d)", err, len(got), len(stream))
		}
		if held := budget.held.Load(); held != 0 {
			t.Fatalf("budget holds %d bytes after the stream finished, want 0", held)
		}
	})
}
