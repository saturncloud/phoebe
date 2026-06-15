package proxy

// Fix C: the captured request-body LOG copy is bounded by MaxBodyBytes.
//
// The request body flows into to_tsvector at io_log INSERT time, and Postgres
// rejects a tsvector input past ~1 MiB — so an uncapped long-context prompt would
// fail the whole INSERT and silently DROP the record. We cap the LOGGED copy at
// the same bound the response copy uses (one shared truncateAtRuneBoundary), set
// RequestTruncated, and still forward the FULL body upstream.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/saturncloud/phoebe/internal/identity"
)

func TestTruncateAtRuneBoundary(t *testing.T) {
	// "héllo": 'é' is 2 bytes (0xC3 0xA9), so byte indices are h=0, é=1..2, l=3.
	s := "héllo"
	tests := []struct {
		name      string
		limit     int
		wantLen   int // expected byte length of the kept prefix
		wantTrunc bool
	}{
		{"no cap", 0, len(s), false},
		{"under length", 100, len(s), false},
		{"exact length", len(s), len(s), false},
		{"cap mid-rune backs up", 2, 1, true}, // cap at 2 splits 'é'; backs up to 1 ("h")
		{"cap at rune start", 3, 3, true},     // "hé" is exactly 3 bytes, valid boundary
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, trunc := truncateAtRuneBoundary([]byte(s), tc.limit)
			if trunc != tc.wantTrunc {
				t.Fatalf("truncated = %v, want %v", trunc, tc.wantTrunc)
			}
			if len(out) != tc.wantLen {
				t.Fatalf("len(out) = %d, want %d (out=%q)", len(out), tc.wantLen, out)
			}
			if !utf8.Valid(out) {
				t.Fatalf("truncated output is not valid UTF-8: %q", out)
			}
		})
	}
}

// TestRequestBodyCappedBeforeTsvector (proxy-level): a request body larger than
// the cap is truncated + flagged in the Record, while the upstream still receives
// the FULL body. This is the in-process guard for the >1MiB INSERT failure; the
// integration test (iolog package) proves the real Postgres path.
func TestRequestBodyCappedBeforeTsvector(t *testing.T) {
	const bodyCap = 100
	big := strings.Repeat("A", 5000)

	var upstreamGot int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ := io.ReadAll(r.Body)
		upstreamGot = len(got)
		_, _ = io.WriteString(w, `{"model":"m","usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)

	policy := &spyPolicy{decision: true}
	sink := &recordingSink{}
	srv := newIOLogServer(t, upstream, policy, sink, bodyCap)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(big))
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "model-abc")
	req.Header.Set("X-Request-Id", "req-bigbody")
	srv.Handler().ServeHTTP(rr, req)

	// The upstream must have received the FULL body — the cap bounds only the log.
	if upstreamGot != len(big) {
		t.Fatalf("upstream received %d bytes, want full %d (cap must not truncate the forwarded request)", upstreamGot, len(big))
	}

	recs := sink.all()
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	if len(recs[0].RequestBody) != bodyCap {
		t.Fatalf("logged request body len = %d, want %d (capped)", len(recs[0].RequestBody), bodyCap)
	}
	if !recs[0].RequestTruncated {
		t.Fatalf("RequestTruncated must be true when the body exceeds the cap")
	}
}

// TestRequestBodyUnderCapNotFlagged: a small request body is stored whole and not
// flagged — the cap must not over-truncate or false-flag normal traffic.
func TestRequestBodyUnderCapNotFlagged(t *testing.T) {
	const reqBody = `{"model":"m","messages":[{"role":"user","content":"hi"}]}`
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"model":"m"}`)
	}))
	defer backend.Close()
	upstream, _ := url.Parse(backend.URL)

	policy := &spyPolicy{decision: true}
	sink := &recordingSink{}
	srv := newIOLogServer(t, upstream, policy, sink, 1<<20)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(reqBody))
	req.Header.Set(identity.HeaderAuthID, "auth-1")
	req.Header.Set(identity.HeaderResourceID, "model-abc")
	req.Header.Set("X-Request-Id", "req-small")
	srv.Handler().ServeHTTP(rr, req)

	recs := sink.all()
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	if recs[0].RequestBody != reqBody {
		t.Fatalf("RequestBody = %q, want full %q", recs[0].RequestBody, reqBody)
	}
	if recs[0].RequestTruncated {
		t.Fatalf("small request body must not be flagged truncated")
	}
}
