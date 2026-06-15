package proxy

import "unicode/utf8"

// truncateAtRuneBoundary returns the longest prefix of b that fits within limit
// bytes WITHOUT splitting a multibyte UTF-8 rune, and whether truncation
// occurred. A split rune would leave invalid UTF-8 — which the io_log Postgres
// TEXT column rejects, failing the whole INSERT over a logging artifact — so we
// back the cut up to a rune boundary (a rune spans at most utf8.UTFMax bytes, so
// at most utf8.UTFMax-1 continuation bytes precede the cut; a non-UTF-8 payload
// is bounded the same way and sanitised at the sink).
//
// This is the SINGLE bound shared by BOTH captured bodies — the streamed response
// copy (captureReader.appendLog) and the request body (captureRequestBody) — so
// there is exactly one definition of "the buffered-body cap" and one rune-safe
// truncation, driven by the same MaxBodyBytes config value. limit <= 0 means "no
// cap": return b unchanged.
func truncateAtRuneBoundary(b []byte, limit int) (out []byte, truncated bool) {
	if limit <= 0 || len(b) <= limit {
		return b, false
	}
	cut := limit
	for cut > 0 && limit-cut < utf8.UTFMax-1 && !utf8.RuneStart(b[cut]) {
		cut--
	}
	return b[:cut], true
}
