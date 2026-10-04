package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// trailerOnEOF stands in for the net/http server's body reader: it merges the
// wire trailers into the request's Trailer map when it reaches EOF (or when it
// is closed early), as transfer.go readTrailer does.
type trailerOnEOF struct {
	r       io.Reader
	req     *http.Request
	trailer http.Header
}

func (b *trailerOnEOF) merge() {
	if b.req.Trailer == nil {
		b.req.Trailer = http.Header{}
	}
	for k, v := range b.trailer {
		b.req.Trailer[k] = v
	}
}

func (b *trailerOnEOF) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err == io.EOF {
		b.merge()
	}
	return n, err
}

func (b *trailerOnEOF) Close() error { b.merge(); return nil }

func wireTrailers() http.Header {
	return http.Header{
		"X-Saturn-Service-Tier": {"premium"}, "X-Saturn-Foo": {"bar"},
		"X-Saturn-Upstream": {"decoy.invalid:80"}, "x-saturn-org-ID": {"org-forged"},
		"X-Other-Trailer": {"ok"}, "X-Saturnine": {"ok"},
	}
}

// saturnTrailerLeft returns an X-Saturn-* name still in h, or "".
func saturnTrailerLeft(h http.Header) string {
	for name := range h {
		if isSaturnName(name) {
			return name
		}
	}
	return ""
}

// TestTrailerStripBodyStripsTrailersMergedAtEOF: trailers the server adds to
// r.Trailer at body EOF, after the handler-entry strip, are stripped before
// the reader returns EOF to its caller.
func TestTrailerStripBodyStripsTrailersMergedAtEOF(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Body = &trailerOnEOF{r: strings.NewReader(`{"model":"m"}`), req: req, trailer: wireTrailers()}
	wrapTrailerStrip(req)
	if _, err := io.ReadAll(req.Body); err != nil {
		t.Fatal(err)
	}
	if name := saturnTrailerLeft(req.Trailer); name != "" {
		t.Fatalf("X-Saturn-* trailer %s survived EOF: %v", name, req.Trailer)
	}
	if req.Trailer.Get("X-Other-Trailer") != "ok" || req.Trailer.Get("X-Saturnine") != "ok" {
		t.Fatalf("harmless trailer was dropped: %v", req.Trailer)
	}
}

// TestTrailerStripBodyStripsTrailersMergedOnClose: closing an unread body can
// drain it and merge the trailers, so Close strips as well.
func TestTrailerStripBodyStripsTrailersMergedOnClose(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Body = &trailerOnEOF{r: strings.NewReader(`{"model":"m"}`), req: req, trailer: wireTrailers()}
	wrapTrailerStrip(req)
	_ = req.Body.Close()
	if name := saturnTrailerLeft(req.Trailer); name != "" {
		t.Fatalf("X-Saturn-* trailer %s survived Close: %v", name, req.Trailer)
	}
}

// eofBody returns io.EOF on every Read and does nothing on Close. It touches
// no trailer map itself, so any concurrent map access in the test below comes
// from trailerStripBody.
type eofBody struct{}

func (eofBody) Read([]byte) (int, error) { return 0, io.EOF }
func (eofBody) Close() error             { return nil }

// TestTrailerStripBodyConcurrentReadAndCloseDoNotRace: the http.Transport may
// Close the body on one goroutine while another reads it to EOF, and both
// strip r.Trailer. The strips must be serialized, because concurrent map
// writes are a fatal error that recover() cannot catch and would kill the
// proxy process. Run under -race to detect an unsynchronized strip.
func TestTrailerStripBodyConcurrentReadAndCloseDoNotRace(t *testing.T) {
	for i := 0; i < 200; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		req.Body = eofBody{}
		req.Trailer = http.Header{
			"X-Saturn-Foo": {"bar"}, "X-Saturn-Upstream": {"decoy.invalid:80"},
			"x-saturn-org-ID": {"org-forged"}, "X-Other-Trailer": {"ok"},
		}
		wrapTrailerStrip(req)
		body := req.Body
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				_, _ = body.Read(make([]byte, 8))
			}()
			go func() {
				defer wg.Done()
				_ = body.Close()
			}()
		}
		wg.Wait()
		if name := saturnTrailerLeft(req.Trailer); name != "" {
			t.Fatalf("X-Saturn-* trailer %s survived concurrent Read/Close: %v", name, req.Trailer)
		}
		if req.Trailer.Get("X-Other-Trailer") != "ok" {
			t.Fatalf("harmless trailer was dropped: %v", req.Trailer)
		}
	}
}

// TestUpstreamProxyStripsOutboundTrailers: every upstream forward (the metered
// forward and both wake probes use newUpstreamProxy) strips every
// X-Saturn-* name from the outbound request's own Trailer map, independent of
// the body-EOF strip on the inbound request.
func TestUpstreamProxyStripsOutboundTrailers(t *testing.T) {
	rp := newUpstreamProxy(&url.URL{Scheme: "http", Host: "upstream.test"})
	out := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	out.Trailer = wireTrailers()
	rp.Director(out)
	if name := saturnTrailerLeft(out.Trailer); name != "" {
		t.Fatalf("outbound request kept X-Saturn-* trailer %s: %v", name, out.Trailer)
	}
	if out.Trailer.Get("X-Other-Trailer") != "ok" || out.Trailer.Get("X-Saturnine") != "ok" {
		t.Fatalf("harmless outbound trailer was dropped: %v", out.Trailer)
	}
	if out.URL.Host != "upstream.test" {
		t.Fatalf("wrapped Director did not run the single-host rewrite: %v", out.URL)
	}
}

// TestForceIncludeUsageEmptyBodyIsReplaced: forceIncludeUsage closes the body
// it reads, so an empty body must be replaced too; otherwise the forward reads
// the closed body and fails with a 502.
func TestForceIncludeUsageEmptyBodyIsReplaced(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Body = &closedAfterClose{r: strings.NewReader("")}
	if err := forceIncludeUsage(req); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(req.Body); err != nil {
		t.Fatalf("forward would read the closed original body: %v", err)
	}
	if req.ContentLength != 0 {
		t.Fatalf("ContentLength = %d, want 0", req.ContentLength)
	}
}

// closedAfterClose fails every Read once closed, like the server body.
type closedAfterClose struct {
	r      io.Reader
	closed bool
}

func (c *closedAfterClose) Read(p []byte) (int, error) {
	if c.closed {
		return 0, http.ErrBodyReadAfterClose
	}
	return c.r.Read(p)
}

func (c *closedAfterClose) Close() error { c.closed = true; return nil }
