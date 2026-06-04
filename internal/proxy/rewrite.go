package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
)

// forceIncludeUsage rewrites a request body so that streamed responses carry a
// usage block. vLLM only emits streaming usage when
// request.stream_options.include_usage is true; without stream_options it
// emits NONE, which would silently under-bill every streamed request.
//
// We parse → set body["stream_options"]["include_usage"]=true → re-serialize,
// rather than relying on duplicate-key last-writer-wins (vLLM parses via
// Pydantic; duplicate-key resolution is not portable). Overwriting a client's
// explicit include_usage:false is intentional and safe — it mirrors vLLM's own
// --enable-force-include-usage server flag.
//
// Non-streaming requests are left untouched: their responses always carry
// usage. A body that isn't JSON, or has no token-bearing shape, is passed
// through verbatim so we never break a request we don't understand.
func forceIncludeUsage(r *http.Request) error {
	if r.Body == nil {
		return nil
	}

	body, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}

	rewritten, changed := rewriteIncludeUsage(body)
	if !changed {
		// Restore the original body unchanged.
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		r.Header.Set("Content-Length", strconv.Itoa(len(body)))
		return nil
	}

	r.Body = io.NopCloser(bytes.NewReader(rewritten))
	r.ContentLength = int64(len(rewritten))
	r.Header.Set("Content-Length", strconv.Itoa(len(rewritten)))
	return nil
}

// rewriteIncludeUsage returns the body with stream_options.include_usage forced
// to true for streaming requests, and whether it changed anything. It is a pure
// function for testability.
func rewriteIncludeUsage(body []byte) (out []byte, changed bool) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		// Not a JSON object — pass through verbatim.
		return body, false
	}

	// Only streaming requests need forcing; non-streaming always reports usage.
	streamRaw, ok := m["stream"]
	if !ok {
		return body, false
	}
	var stream bool
	if err := json.Unmarshal(streamRaw, &stream); err != nil || !stream {
		return body, false
	}

	// Read existing stream_options (may be absent).
	opts := map[string]json.RawMessage{}
	if raw, ok := m["stream_options"]; ok {
		// If present but not an object, replace it wholesale.
		_ = json.Unmarshal(raw, &opts)
		if opts == nil {
			opts = map[string]json.RawMessage{}
		}
	}

	// Already true? Then nothing to do.
	if raw, ok := opts["include_usage"]; ok {
		var v bool
		if json.Unmarshal(raw, &v) == nil && v {
			return body, false
		}
	}

	opts["include_usage"] = json.RawMessage("true")
	optsBytes, err := json.Marshal(opts)
	if err != nil {
		return body, false
	}
	m["stream_options"] = optsBytes

	out, err = json.Marshal(m)
	if err != nil {
		return body, false
	}
	return out, true
}
