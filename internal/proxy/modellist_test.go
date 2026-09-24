package proxy

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func modelListResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestFilterModelListResponse(t *testing.T) {
	resp := modelListResponse(`{"object":"list","internal_graph":"secret","data":[{"id":"a","object":"model","owned_by":"one","context_window":131072,"max_output_tokens":8192,"internal":"secret"},{"id":"b","owned_by":"two"}]}`)
	resp.Header.Set("X-Graph-Debug", "b")
	resp.Header.Set("ETag", "graph-wide")
	resp.Header.Set("Access-Control-Allow-Origin", "https://example.test")
	resp.Header.Set("Vary", "Origin")
	resp.Header.Set("Cache-Control", "no-store")
	resp.Trailer = http.Header{"X-Graph-Trailer": []string{"b"}}
	if err := filterModelListResponse(resp, "a"); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	got := string(body)
	if !strings.Contains(got, `"id":"a"`) || strings.Contains(got, `"id":"b"`) {
		t.Fatalf("filtered model list = %s", got)
	}
	if strings.Contains(got, "internal_graph") || strings.Contains(got, `"internal"`) {
		t.Fatalf("filtered model list preserved extension fields: %s", got)
	}
	if !strings.Contains(got, `"context_window":131072`) || !strings.Contains(got, `"max_output_tokens":8192`) {
		t.Fatalf("filtered model list dropped documented limits: %s", got)
	}
	if resp.ContentLength != int64(len(body)) || resp.Header.Get("Content-Length") == "" {
		t.Fatalf("response length metadata not updated: length=%d header=%q", resp.ContentLength, resp.Header.Get("Content-Length"))
	}
	if resp.Header.Get("X-Graph-Debug") != "" || resp.Header.Get("ETag") != "" || resp.Trailer != nil {
		t.Fatalf("filtered model list leaked upstream metadata: headers=%v trailer=%v", resp.Header, resp.Trailer)
	}
	// Stripping graph-wide metadata must not also nuke the client contract:
	// phoebe emits no CORS headers of its own, so wiping the upstream's turns a
	// working browser GET /v1/models into an opaque CORS failure while POST on
	// the same origin keeps working.
	if resp.Header.Get("Access-Control-Allow-Origin") != "https://example.test" {
		t.Fatalf("filtered model list dropped upstream CORS header: %v", resp.Header)
	}
	if resp.Header.Get("Vary") != "Origin" || resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("filtered model list dropped client-contract headers: %v", resp.Header)
	}
}

func TestFilterModelListResponseSanitizesUpstreamErrors(t *testing.T) {
	resp := modelListResponse(`{"error":"adapter-b on base-internal failed"}`)
	resp.StatusCode = http.StatusServiceUnavailable
	resp.Header.Set("X-Graph-Debug", "adapter-b")
	resp.Header.Set("Retry-After", "30")
	resp.Header.Set("Access-Control-Allow-Origin", "https://example.test")
	resp.Trailer = http.Header{"X-Graph-Trailer": []string{"adapter-b"}}
	if err := filterModelListResponse(resp, "adapter-a"); err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if strings.Contains(string(body), "adapter-b") || strings.Contains(string(body), "base-internal") {
		t.Fatalf("sanitized error leaked upstream names: %s", body)
	}
	if resp.Header.Get("X-Graph-Debug") != "" || resp.Trailer != nil {
		t.Fatalf("sanitized error leaked upstream metadata: headers=%v trailer=%v", resp.Header, resp.Trailer)
	}
	// A 503 must keep its backoff guidance and CORS headers — the client needs
	// both, and neither names a sibling model.
	if resp.Header.Get("Retry-After") != "30" {
		t.Fatalf("sanitized error dropped Retry-After backoff guidance: %v", resp.Header)
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "https://example.test" {
		t.Fatalf("sanitized error dropped upstream CORS header: %v", resp.Header)
	}
}

func TestFilterModelListResponseDecodesGzip(t *testing.T) {
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	_, _ = gz.Write([]byte(`{"object":"list","data":[{"id":"a"},{"id":"b"}]}`))
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Encoding": []string{"gzip"}},
		Body:       io.NopCloser(bytes.NewReader(compressed.Bytes())),
	}
	if err := filterModelListResponse(resp, "a"); err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), `"id":"b"`) || resp.Header.Get("Content-Encoding") != "" {
		t.Fatalf("gzip response was not filtered and normalized: headers=%v body=%s", resp.Header, body)
	}
}

func TestFilterModelListResponseFailsClosed(t *testing.T) {
	for _, body := range []string{
		`not json`,
		`{"object":"list"}`,
		`{"data":{}}`,
		// Duplicate top-level "id" keys: the entry is AMBIGUOUS (Go takes the
		// last, another JSON stack may take the first), so phoebe refuses the
		// whole response rather than picking a resolution the upstream may not
		// share. This is the duplicate-key smuggling guard — keep it closed.
		`{"data":[{"id":"sibling","id":"allowed"}]}`,
		`{"data":[{}]}`,
		`{"data":["notanobject"]}`,
	} {
		resp := modelListResponse(body)
		if err := filterModelListResponse(resp, "allowed"); err == nil {
			t.Fatalf("body %q unexpectedly passed", body)
		}
	}
}

// The per-entry guard must never format a nil error with %w: the duplicate-id
// case reports through the COUNT with err == nil, and a combined wrap rendered
// the operator's only diagnostic as "%!w(<nil>)" — for exactly the duplicate-key
// smuggling attempt the guard exists to catch — while making errors.Is/As
// useless. Malformed-JSON entries must still WRAP the real decode error.
func TestFilterModelListResponseMalformedEntryError(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     string
		contains string
	}{
		{"duplicate id keys", `{"data":[{"id":"sibling","id":"allowed"}]}`, "got 2"},
		{"no id key", `{"data":[{}]}`, "got 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := filterModelListResponse(modelListResponse(tc.body), "allowed")
			if err == nil {
				t.Fatalf("body %q must be rejected", tc.body)
			}
			if strings.Contains(err.Error(), "%!w") {
				t.Fatalf("error wrapped a nil error: %q", err.Error())
			}
			if !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("error %q must report the key count (%q)", err.Error(), tc.contains)
			}
			if errors.Unwrap(err) != nil {
				t.Fatalf("count-only rejection must not wrap an error: %v", errors.Unwrap(err))
			}
		})
	}

	// A genuinely malformed entry still wraps the underlying decode error.
	err := filterModelListResponse(modelListResponse(`{"data":["notanobject"]}`), "allowed")
	if err == nil {
		t.Fatal("non-object entry must be rejected")
	}
	if strings.Contains(err.Error(), "%!w") {
		t.Fatalf("error wrapped a nil error: %q", err.Error())
	}
	if !errors.Is(err, errNotObject) {
		t.Fatalf("non-object entry must wrap errNotObject, got %v", err)
	}
}

// Dynamo's readiness endpoints are GRAPH-WIDE: they enumerate the whole graph's
// components/workers/models, i.e. sibling tenants' attached adapters. A
// deployment-scoped subdomain must disclose nothing beyond its status class, or
// /health re-opens the enumeration /v1/models was hardened against.
func TestSanitizeReadinessResponseDropsGraphWideDetail(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		wantBody string
	}{
		{"healthy", http.StatusOK, `{"status":"ok"}`},
		{"unhealthy", http.StatusServiceUnavailable, `{"status":"unavailable"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := modelListResponse(`{"components":[{"model":"org/sibling","workers":2}],"instances":["base-internal"]}`)
			resp.StatusCode = tc.status
			resp.Header.Set("ETag", "graph-wide")
			resp.Header.Set("X-Graph-Debug", "org/sibling")
			resp.Header.Set("Access-Control-Allow-Origin", "https://example.test")
			resp.Trailer = http.Header{"X-Graph-Trailer": []string{"org/sibling"}}

			if err := sanitizeReadinessResponse(resp, false); err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d (liveness must stay truthful)", resp.StatusCode, tc.status)
			}
			body, _ := io.ReadAll(resp.Body)
			if string(body) != tc.wantBody {
				t.Fatalf("readiness body = %s, want %s", body, tc.wantBody)
			}
			if strings.Contains(string(body), "sibling") || strings.Contains(string(body), "base-internal") {
				t.Fatalf("readiness body disclosed graph-wide names: %s", body)
			}
			if resp.Header.Get("ETag") != "" || resp.Header.Get("X-Graph-Debug") != "" || resp.Trailer != nil {
				t.Fatalf("readiness leaked upstream metadata: headers=%v trailer=%v", resp.Header, resp.Trailer)
			}
			if resp.Header.Get("Access-Control-Allow-Origin") != "https://example.test" {
				t.Fatalf("readiness dropped the client-contract CORS header: %v", resp.Header)
			}
			if resp.ContentLength != int64(len(tc.wantBody)) {
				t.Fatalf("readiness length = %d, want %d", resp.ContentLength, len(tc.wantBody))
			}
		})
	}

	// HEAD carries no body but the same header reset.
	resp := modelListResponse(`{"components":[{"model":"org/sibling"}]}`)
	resp.Header.Set("X-Graph-Debug", "org/sibling")
	if err := sanitizeReadinessResponse(resp, true); err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) != 0 {
		t.Fatalf("HEAD readiness must have no body, got %s", body)
	}
	if resp.Header.Get("X-Graph-Debug") != "" {
		t.Fatalf("HEAD readiness leaked upstream metadata: %v", resp.Header)
	}
}
