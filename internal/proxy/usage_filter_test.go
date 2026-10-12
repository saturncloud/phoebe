package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
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
