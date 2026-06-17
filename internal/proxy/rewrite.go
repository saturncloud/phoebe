package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
)

// captureRequestBody reads the request body and restores it so a subsequent
// reader (forceIncludeUsage) sees the same bytes — no double-read of the
// underlying stream. It returns the ORIGINAL body (capped for the LOG copy) as a
// string for M5 I/O logging, plus whether that copy was truncated. Called only
// when the iolog policy gate passed, so the read cost is paid exclusively by
// opted-in, sampled requests.
//
// THE LOGGED COPY IS CAPPED at maxBodyBytes — the SAME bound the response copy
// uses (truncateAtRuneBoundary), for the SAME reason: the request body flows into
// to_tsvector at INSERT time, and Postgres rejects a tsvector input past ~1 MiB,
// which would fail the whole io_log INSERT and silently drop the record on any
// long-context prompt. The cap bounds what we LOG; the FORWARDED request to the
// upstream is always the full body (restored below), so this never changes what
// the model sees — only the stored log copy. Truncation is at a rune boundary so
// the stored TEXT is valid UTF-8. maxBodyBytes <= 0 means uncapped.
//
// A nil body yields ("", false, 0) with no error. origLen is the size of the
// FULL body read off the wire (before capping), so a caller can log the
// original-vs-cap sizes when truncated is true.
func captureRequestBody(r *http.Request, maxBodyBytes int) (body string, truncated bool, origLen int, err error) {
	if r.Body == nil {
		return "", false, 0, nil
	}
	raw, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		return "", false, 0, err
	}
	// Restore the FULL body so forceIncludeUsage (and the upstream) read every
	// byte — the cap only bounds the LOG copy, never the forwarded request.
	r.Body = io.NopCloser(bytes.NewReader(raw))
	r.ContentLength = int64(len(raw))
	r.Header.Set("Content-Length", strconv.Itoa(len(raw)))

	logCopy, truncated := truncateAtRuneBoundary(raw, maxBodyBytes)
	return string(logCopy), truncated, len(raw), nil
}

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
