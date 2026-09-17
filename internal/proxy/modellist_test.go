package proxy

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestFilterModelListResponse(t *testing.T) {
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body: io.NopCloser(strings.NewReader(
			`{"object":"list","data":[{"id":"a","owned_by":"one"},{"id":"b","owned_by":"two"}]}`,
		)),
	}
	if err := filterModelListResponse(resp, "a"); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(body); !strings.Contains(got, `"id":"a"`) || strings.Contains(got, `"id":"b"`) {
		t.Fatalf("filtered model list = %s", got)
	}
	if resp.ContentLength != int64(len(body)) || resp.Header.Get("Content-Length") == "" {
		t.Fatalf("response length metadata not updated: length=%d header=%q", resp.ContentLength, resp.Header.Get("Content-Length"))
	}
}

func TestFilterModelListResponseFailsClosed(t *testing.T) {
	for _, body := range []string{`not json`, `{"object":"list"}`, `{"data":{}}`} {
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
		}
		if err := filterModelListResponse(resp, "a"); err == nil {
			t.Fatalf("body %q unexpectedly passed", body)
		}
	}
}
