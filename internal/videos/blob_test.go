package videos

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// A store whose ETags do not match what minio-go expects refuses any read sent
// with If-Match (SeaweedFS on uk2, 2026-11-18). Get must not send one.
func TestS3GetSendsNoETagPrecondition(t *testing.T) {
	body := []byte("an mp4")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-Match") != "" {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code></Error>`)
			return
		}
		w.Header().Set("ETag", `"not-an-md5-1"`)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("Last-Modified", "Wed, 18 Nov 2026 03:50:00 GMT")
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	s3, err := NewS3(S3Config{Endpoint: srv.URL, Bucket: "bucket", Region: "us-east-1", AccessKey: "a", SecretKey: "s"})
	if err != nil {
		t.Fatal(err)
	}
	r, size, err := s3.Get(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || string(got) != string(body) || size != int64(len(body)) {
		t.Fatalf("got %q size %d err %v", got, size, err)
	}
}
