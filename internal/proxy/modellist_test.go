package proxy

import (
	"bytes"
	"compress/gzip"
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
}

func TestFilterModelListResponseSanitizesUpstreamErrors(t *testing.T) {
	resp := modelListResponse(`{"error":"adapter-b on base-internal failed"}`)
	resp.StatusCode = http.StatusServiceUnavailable
	resp.Header.Set("X-Graph-Debug", "adapter-b")
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
		`{"data":[{"id":"sibling","id":"allowed"}]}`,
	} {
		resp := modelListResponse(body)
		if err := filterModelListResponse(resp, "allowed"); err == nil {
			t.Fatalf("body %q unexpectedly passed", body)
		}
	}
}
