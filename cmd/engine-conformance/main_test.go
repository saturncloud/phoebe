package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testChecker(t *testing.T, events ...string) *checker {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, event := range events {
			_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
		}
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(server.Close)
	return &checker{base: server.URL, model: "m", client: server.Client(), timeout: time.Second}
}

func TestCheckStreamingContinuousUsageShapes(t *testing.T) {
	valid := []string{
		`{"choices":[{"delta":{"content":"a"}}],"usage":{"prompt_tokens":4,"completion_tokens":1,"total_tokens":5,"prompt_tokens_details":{"cached_tokens":2}}}`,
		`{"choices":[{"delta":{"content":"b"}}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}`,
		`{"choices":[],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}`,
	}
	if err := testChecker(t, valid...).checkStreaming(); err != nil {
		t.Fatalf("valid continuous stream: %v", err)
	}
	oneToken := []string{
		`{"choices":[{"delta":{"content":"a"}}],"usage":{"prompt_tokens":4,"completion_tokens":1,"total_tokens":5}}`,
		`{"choices":[],"usage":{"prompt_tokens":4,"completion_tokens":1,"total_tokens":5}}`,
	}
	if err := testChecker(t, oneToken...).checkStreaming(); err != nil {
		t.Fatalf("valid one-token continuous stream: %v", err)
	}

	cases := map[string][]string{
		"sparse usage": {
			valid[0],
			`{"choices":[{"delta":{"content":"b"}}],"usage":null}`,
			valid[2],
		},
		"repeated final usage": {
			valid[0],
			`{"choices":[],"usage":{"prompt_tokens":4,"completion_tokens":1,"total_tokens":5}}`,
			valid[2],
		},
		"malformed event": {
			valid[0],
			`{"choices":`,
			valid[2],
		},
	}
	for name, events := range cases {
		t.Run(name, func(t *testing.T) {
			if err := testChecker(t, events...).checkStreaming(); err == nil {
				t.Fatal("invalid stream passed conformance")
			}
		})
	}
}

func TestCheckStreamingRejectsUsageAfterDone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":1,\"total_tokens\":5}}\n\n")
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		_, _ = fmt.Fprint(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":1,\"total_tokens\":5}}\n\n")
	}))
	defer server.Close()
	c := &checker{base: server.URL, model: "m", client: server.Client(), timeout: time.Second}
	if err := c.checkStreaming(); err == nil {
		t.Fatal("usage after DONE passed conformance")
	}
}

func TestCheckStreamingSendsBothUsageOptions(t *testing.T) {
	var body string
	var acceptEncoding string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		body = string(data)
		acceptEncoding = r.Header.Get("Accept-Encoding")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer server.Close()
	c := &checker{base: server.URL, model: "m", client: server.Client(), timeout: time.Second}
	_ = c.checkStreaming()
	if !strings.Contains(body, `"include_usage":true`) || !strings.Contains(body, `"continuous_usage_stats":true`) {
		t.Fatalf("request omitted forced usage options: %s", body)
	}
	if acceptEncoding != "identity" {
		t.Fatalf("Accept-Encoding = %q, want identity", acceptEncoding)
	}
}

func TestCheckStreamingRejectsIncompatibleResponseHeaders(t *testing.T) {
	for _, tc := range []struct {
		name        string
		contentType string
		encodings   []string
	}{
		{name: "wrong content type", contentType: "application/json"},
		{name: "compressed", contentType: "text/event-stream", encodings: []string{"gzip"}},
		{name: "multiple encodings", contentType: "text/event-stream", encodings: []string{"identity", "gzip"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				for _, encoding := range tc.encodings {
					w.Header().Add("Content-Encoding", encoding)
				}
				_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
			}))
			defer server.Close()
			c := &checker{base: server.URL, model: "m", client: server.Client(), timeout: time.Second}
			if err := c.checkStreaming(); err == nil {
				t.Fatal("incompatible response headers passed conformance")
			}
		})
	}
}
