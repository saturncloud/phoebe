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
	raw, err := readBody(r)
	if err != nil {
		return "", false, 0, err
	}
	// Restore the FULL body so forceIncludeUsage (and the upstream) read every
	// byte — the cap only bounds the LOG copy, never the forwarded request.
	replaceRequestBody(r, raw)

	logCopy, truncated := truncateAtRuneBoundary(raw, maxBodyBytes)
	return string(logCopy), truncated, len(raw), nil
}

// forceIncludeUsage rewrites a request body so that streamed responses carry
// cumulative usage on every chunk. vLLM only emits streaming usage when
// request.stream_options.include_usage is true, and it waits until the final
// chunk unless continuous_usage_stats is also true. A client can disconnect
// before that final chunk, so both values are required to retain the latest
// engine-reported counts for an aborted stream.
//
// We parse → set include_usage=true and continuous_usage_stats=true →
// re-serialize,
// rather than relying on duplicate-key last-writer-wins (vLLM parses via
// Pydantic; duplicate-key resolution is not portable). Overwriting a client's
// explicit false values is intentional: billing requires the engine's
// cumulative counts even if the caller does not want usage fields.
//
// Non-streaming requests are left untouched: their responses always carry
// usage. A body that isn't JSON, or has no token-bearing shape, is passed
// through verbatim so we never break a request we don't understand.
// readAndRestoreBody reads the full request body and restores it so a later
// reader (forceIncludeUsage) sees the same bytes — no double-consume. Returns
// the body bytes for inspection (e.g. the model-binding check). A nil body reads
// as empty.
func readAndRestoreBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	body, err := readBody(r)
	if err != nil {
		return nil, err
	}
	replaceRequestBody(r, body)
	return body, nil
}

// bufferedBody is a request body phoebe has already read whole and restored.
// It keeps the buffer so a later full read reuses it (bufferedBytes) instead
// of copying it again: the request-buffer budget charges each received byte
// once, so every extra copy is memory the budget does not see.
type bufferedBody struct {
	*bytes.Reader
	buf []byte
}

func (*bufferedBody) Close() error { return nil }

// bufferedBytes returns the buffer behind a restored, not yet read body
// (looking through the request-buffer budget wrapper). A body something has
// partly read is a miss, so a caller never sees bytes the reader already
// consumed. Callers must not modify the returned slice: it is the same buffer
// the forwarded request and GetBody read.
func bufferedBytes(r *http.Request) ([]byte, bool) {
	body := r.Body
	if bb, ok := body.(*budgetedBody); ok {
		body = bb.ReadCloser
	}
	buffered, ok := body.(*bufferedBody)
	if !ok || buffered.Len() != len(buffered.buf) {
		return nil, false
	}
	return buffered.buf, true
}

// readBody reads the whole request body and closes it, reusing the buffer
// of a body phoebe already restored. Only the first read off the wire
// allocates a copy.
func readBody(r *http.Request) ([]byte, error) {
	if buf, ok := bufferedBytes(r); ok {
		return buf, nil
	}
	body, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	return body, err
}

func replaceRequestBody(r *http.Request, body []byte) {
	r.Body = &bufferedBody{Reader: bytes.NewReader(body), buf: body}
	r.ContentLength = int64(len(body))
	r.Header.Set("Content-Length", strconv.Itoa(len(body)))
	r.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
}

func forceIncludeUsage(r *http.Request) error {
	if r.Body == nil {
		return nil
	}

	body, err := readBody(r)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		// The original body is closed: install the (empty) buffered body so
		// the forward never reads the closed one ("invalid Read on closed
		// Body", a 502 for every empty chunked request).
		replaceRequestBody(r, body)
		return nil
	}

	rewritten, changed := rewriteIncludeUsage(body)
	if !changed {
		replaceRequestBody(r, body)
		return nil
	}

	replaceRequestBody(r, rewritten)
	return nil
}

type streamUsagePreference struct {
	stream          bool
	includeUsage    bool
	continuousUsage bool
}

// clientRequestedStreamUsage reports the caller's original usage preferences
// and restores the request body byte-for-byte. Phoebe may force both options
// upstream for billing, but these values control what it exposes to the caller.
func clientRequestedStreamUsage(r *http.Request) (streamUsagePreference, error) {
	body, err := readAndRestoreBody(r)
	if err != nil {
		return streamUsagePreference{}, err
	}
	var request struct {
		Stream        bool `json:"stream"`
		StreamOptions struct {
			IncludeUsage         bool `json:"include_usage"`
			ContinuousUsageStats bool `json:"continuous_usage_stats"`
		} `json:"stream_options"`
	}
	if json.Unmarshal(body, &request) != nil || !request.Stream {
		return streamUsagePreference{}, nil
	}
	return streamUsagePreference{
		stream:          true,
		includeUsage:    request.StreamOptions.IncludeUsage,
		continuousUsage: request.StreamOptions.IncludeUsage && request.StreamOptions.ContinuousUsageStats,
	}, nil
}

// rewriteIncludeUsage returns the body with stream_options.include_usage and
// stream_options.continuous_usage_stats forced to true for streaming requests,
// and whether it changed anything. It is a pure function for testability.
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

	includeUsage := false
	if raw, ok := opts["include_usage"]; ok {
		_ = json.Unmarshal(raw, &includeUsage)
	}
	continuousUsage := false
	if raw, ok := opts["continuous_usage_stats"]; ok {
		_ = json.Unmarshal(raw, &continuousUsage)
	}
	if includeUsage && continuousUsage {
		return body, false
	}

	opts["include_usage"] = json.RawMessage("true")
	opts["continuous_usage_stats"] = json.RawMessage("true")
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
