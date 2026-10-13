package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

const maxSSEEventLineBytes = 16 << 20

var errSSEEventLineTooLarge = errors.New("SSE event line exceeds 16 MiB")

// usageFilterReader restores the stream shape the caller requested while the
// inner captureReader still observes every cumulative usage value for billing.
// Dynamo emits one SSE event line at a time, so this filter buffers one line
// while it determines whether that event contains usage fields. The generous
// line limit admits large tool-call and reasoning events while preventing an
// unbounded response from exhausting the proxy heap.
type usageFilterReader struct {
	src               io.ReadCloser
	reader            *bufio.Reader
	line              bytes.Buffer
	pending           bytes.Buffer
	includeFinalUsage bool
	maxLineBytes      int
	terminalErr       error
	// buffers charges the held line and the unread filtered lines against the gateway protection limits on
	// buffered response bytes; nil = only maxLineBytes applies.
	buffers *responseBufferAccount
}

func newUsageFilterReader(src io.ReadCloser, includeFinalUsage bool, buffers *responseBufferAccount) io.ReadCloser {
	return &usageFilterReader{
		src:               src,
		reader:            bufio.NewReader(src),
		includeFinalUsage: includeFinalUsage,
		maxLineBytes:      maxSSEEventLineBytes,
		buffers:           buffers,
	}
}

func (r *usageFilterReader) Read(p []byte) (int, error) {
	if r.pending.Len() == 0 && r.terminalErr != nil {
		err := r.terminalErr
		r.terminalErr = nil
		return 0, err
	}
	for r.pending.Len() == 0 {
		fragment, err := r.reader.ReadSlice('\n')
		if r.line.Len()+len(fragment) > r.maxLineBytes {
			_ = r.src.Close()
			return 0, errSSEEventLineTooLarge
		}
		r.line.Write(fragment)
		if berr := r.charge(); berr != nil {
			_ = r.src.Close()
			return 0, berr
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if r.line.Len() > 0 {
			r.pending.Write(filterUsageLine(r.line.Bytes(), r.includeFinalUsage))
			// Charge the moment both the line and its filtered copy exist; the
			// pending copy then stays charged until the client has read it.
			if berr := r.charge(); berr != nil {
				_ = r.src.Close()
				return 0, berr
			}
			r.line.Reset()
			if r.line.Cap() > releaseBufferCap {
				r.line = bytes.Buffer{}
			}
			_ = r.charge()
		}
		if err != nil {
			if r.pending.Len() == 0 {
				return 0, err
			}
			r.terminalErr = err
			break
		}
	}
	n, err := r.pending.Read(p)
	_ = r.charge()
	if r.pending.Len() == 0 && r.pending.Cap() > releaseBufferCap {
		r.pending = bytes.Buffer{}
	}
	return n, err
}

// releaseBufferCap is the backing-array size above which an emptied line or
// pending buffer is replaced, so one large event does not pin its memory for
// the rest of the stream.
const releaseBufferCap = 64 << 10

// charge reports every byte the filter holds — the partial line and the
// filtered lines the client has not read yet — to the buffer account.
func (r *usageFilterReader) charge() error {
	return r.buffers.set(bufferSlotFilter, int64(r.line.Len()+r.pending.Len()))
}

func (r *usageFilterReader) Close() error { return r.src.Close() }

// trailerClearingReadCloser removes body validators both before and after the
// upstream body is consumed. The HTTP transport populates response trailers at
// EOF, after ModifyResponse runs, so clearing them only during setup is too
// early.
type trailerClearingReadCloser struct {
	src     io.ReadCloser
	trailer http.Header
	names   []string
}

func (r *trailerClearingReadCloser) clear() {
	for _, name := range r.names {
		r.trailer.Del(name)
	}
}

func (r *trailerClearingReadCloser) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if err != nil {
		r.clear()
	}
	return n, err
}

func (r *trailerClearingReadCloser) Close() error {
	r.clear()
	return r.src.Close()
}

func filterUsageLine(line []byte, includeFinalUsage bool) []byte {
	ending := []byte{}
	content := line
	if bytes.HasSuffix(content, []byte("\r\n")) {
		ending = []byte("\r\n")
		content = content[:len(content)-2]
	} else if bytes.HasSuffix(content, []byte("\n")) {
		ending = []byte("\n")
		content = content[:len(content)-1]
	}
	if !bytes.HasPrefix(content, sseDataPrefix) {
		return line
	}
	payload := bytes.TrimSpace(content[len(sseDataPrefix):])
	if len(payload) == 0 || bytes.Equal(payload, sseDone) {
		return line
	}
	var event map[string]json.RawMessage
	if json.Unmarshal(payload, &event) != nil {
		return line
	}
	if _, present := event["usage"]; !present {
		return line
	}
	var choices []json.RawMessage
	finalUsage := json.Unmarshal(event["choices"], &choices) == nil && len(choices) == 0
	if finalUsage {
		if includeFinalUsage {
			return line
		}
		// vLLM's final usage event carries no choices. Remove the whole event for
		// callers that did not request usage, matching its normal stream shape.
		return nil
	}
	if includeFinalUsage {
		// OpenAI's final-only stream shape includes usage:null on ordinary
		// chunks and puts the usage object on the final empty-choices event.
		event["usage"] = json.RawMessage("null")
	} else {
		delete(event, "usage")
	}
	rewritten, err := json.Marshal(event)
	if err != nil {
		return line
	}
	out := make([]byte, 0, len(sseDataPrefix)+len(rewritten)+len(ending))
	out = append(out, sseDataPrefix...)
	out = append(out, rewritten...)
	out = append(out, ending...)
	return out
}
