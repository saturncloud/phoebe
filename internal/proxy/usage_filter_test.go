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
		// A completed line and its filtered copy are charged together, so a
		// line under the limit is one whose two copies fit (under half).
		line := "data: {\"choices\":[{\"delta\":{\"content\":\"" + strings.Repeat("y", perStream/4) + "\"}}]}\n\n"
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

// B4 / D2: a completed SSE line moved to the filter's pending output stays
// charged to the request's buffer account until the client has read it. The
// filter slot equals the unread pending bytes after every read and drops to 0
// only once the line has been fully drained, so a slow reader cannot hold
// large lines outside the per-stream and replica-wide budgets.
func TestUsageFilterChargesUnreadPendingLine(t *testing.T) {
	p := config.ProtectionSettings{MaxResponseBufferBytesPerStream: 64 << 20, MaxResponseBufferBytesTotal: 64 << 20}
	var budget responseBufferBudget
	account := budget.newAccount(p, func(err error) { t.Errorf("unexpected buffer cancel: %v", err) })
	line := "data: {\"choices\":[{\"delta\":{\"content\":\"" + strings.Repeat("z", 1<<20) + "\"}}]}\n"
	filtered := newUsageFilterReader(io.NopCloser(strings.NewReader(line)), false, account).(*usageFilterReader)

	buf := make([]byte, 4096)
	var read int
	for {
		n, err := filtered.Read(buf)
		read += n
		remaining := int64(len(line) - read)
		if held := account.slots[bufferSlotFilter]; held != remaining {
			t.Fatalf("after reading %d bytes the filter slot = %d, want the %d unread pending bytes", read, held, remaining)
		}
		if remaining > 0 && budget.held.Load() == 0 {
			t.Fatalf("replica budget released the pending line with %d bytes unread", remaining)
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			break
		}
	}
	if read != len(line) {
		t.Fatalf("read %d bytes, want %d", read, len(line))
	}
	if held := budget.held.Load(); held != 0 {
		t.Fatalf("budget holds %d bytes after the line drained, want 0", held)
	}
}

// B4: the moment a completed line and its filtered copy both exist is
// charged, so a per-stream limit below twice the line rejects it with the
// buffer-limit error instead of letting the copy escape the budget.
func TestUsageFilterChargesLineAndFilteredCopyTogether(t *testing.T) {
	line := "data: {\"choices\":[{\"delta\":{\"content\":\"" + strings.Repeat("w", 1<<20) + "\"}}]}\n"
	p := config.ProtectionSettings{MaxResponseBufferBytesPerStream: int64(len(line))*2 - 1, MaxResponseBufferBytesTotal: 64 << 20}
	var budget responseBufferBudget
	var cause error
	account := budget.newAccount(p, func(err error) { cause = err })
	src := &closeRecorder{Reader: strings.NewReader(line)}
	got, err := io.ReadAll(newUsageFilterReader(src, false, account))
	if !errors.Is(err, errResponseBufferLimit) || !errors.Is(cause, errResponseBufferLimit) {
		t.Fatalf("err=%v cause=%v, want %v", err, cause, errResponseBufferLimit)
	}
	if len(got) != 0 || !src.closed {
		t.Fatalf("returned %d bytes, closed=%v; want 0 bytes and a closed upstream", len(got), src.closed)
	}
	account.close()
	if held := budget.held.Load(); held != 0 {
		t.Fatalf("budget holds %d bytes after close, want 0", held)
	}
}

// D2: an unread pending line counts toward the replica-wide total, so a
// second request that would push the total over the limit is refused while
// the first request's line is still waiting for its client.
func TestUnreadPendingLineCountsTowardReplicaTotal(t *testing.T) {
	line := "data: {\"choices\":[{\"delta\":{\"content\":\"" + strings.Repeat("v", 64<<10) + "\"}}]}\n"
	p := config.ProtectionSettings{MaxResponseBufferBytesPerStream: 1 << 20, MaxResponseBufferBytesTotal: 2*int64(len(line)) + 100}
	var budget responseBufferBudget
	first := budget.newAccount(p, func(error) {})
	filtered := newUsageFilterReader(io.NopCloser(strings.NewReader(line)), false, first)
	if n, err := filtered.Read(make([]byte, 10)); n != 10 || err != nil {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if held := budget.held.Load(); held != int64(len(line)-10) {
		t.Fatalf("budget holds %d, want the %d unread pending bytes", held, len(line)-10)
	}
	second := budget.newAccount(p, func(error) {})
	// Without the pending line charged the budget would be empty and this
	// would fit.
	if err := second.set(bufferSlotCapture, int64(len(line))+200); !errors.Is(err, errResponseBufferLimit) {
		t.Fatalf("second request over the replica total was not refused: %v", err)
	}
	first.close()
	second.close()
}

// filterOverCapture builds the production pairing: a streamed capture reader
// that defers closing the buffer account, wrapped by the usage filter.
func filterOverCapture(stream string, account *responseBufferAccount) io.ReadCloser {
	cr := newCaptureReader(nil, io.NopCloser(strings.NewReader(stream)), true, nil)
	cr.buffers = account
	cr.deferBufferClose = true
	return newUsageFilterReader(cr, false, account)
}

// The account stays open past upstream EOF while the filter still holds the
// stream's last line: those bytes stay charged until the client drains them
// and the filter is closed.
func TestFinalLineAfterUpstreamEOFStaysChargedUntilFilterCloses(t *testing.T) {
	line := "data: {\"choices\":[{\"delta\":{\"content\":\"" + strings.Repeat("f", 300<<10) + "\"}}]}"
	p := config.ProtectionSettings{MaxResponseBufferBytesPerStream: 4 * int64(len(line)), MaxResponseBufferBytesTotal: 4 * int64(len(line))}
	var budget responseBufferBudget
	account := budget.newAccount(p, func(err error) { t.Errorf("unexpected buffer cancel: %v", err) })
	filtered := filterOverCapture(line, account)

	if n, err := filtered.Read(make([]byte, 16)); n != 16 || err != nil {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if held := budget.held.Load(); held < int64(len(line))-16 {
		t.Fatalf("after upstream EOF the budget holds %d, want at least the %d unread pending bytes", held, len(line)-16)
	}
	rest, err := io.ReadAll(filtered)
	if err != nil || len(rest) != len(line)-16 {
		t.Fatalf("drained %d bytes err=%v, want %d", len(rest), err, len(line)-16)
	}
	if err := filtered.Close(); err != nil {
		t.Fatal(err)
	}
	if held := budget.held.Load(); held != 0 {
		t.Fatalf("budget holds %d bytes after the filter closed, want 0", held)
	}
}

// Because the account is still open after upstream EOF, a final line whose
// filtered copy would push the request over its limit is refused there too.
func TestFinalLineAfterUpstreamEOFIsHeldToTheBufferLimit(t *testing.T) {
	line := "data: {\"choices\":[{\"delta\":{\"content\":\"" + strings.Repeat("g", 300<<10) + "\"}}]}"
	limit := 5 * int64(len(line)) / 2 // capture + line fit; the pending copy does not
	p := config.ProtectionSettings{MaxResponseBufferBytesPerStream: limit, MaxResponseBufferBytesTotal: limit}
	var budget responseBufferBudget
	var cause error
	account := budget.newAccount(p, func(err error) { cause = err })
	filtered := filterOverCapture(line, account)

	got, err := io.ReadAll(filtered)
	if !errors.Is(err, errResponseBufferLimit) || !errors.Is(cause, errResponseBufferLimit) {
		t.Fatalf("err=%v cause=%v, want %v", err, cause, errResponseBufferLimit)
	}
	if len(got) != 0 {
		t.Fatalf("returned %d bytes of the refused line", len(got))
	}
	_ = filtered.Close()
	if held := budget.held.Load(); held != 0 {
		t.Fatalf("budget holds %d bytes after close, want 0", held)
	}
}
