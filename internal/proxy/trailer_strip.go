package proxy

import (
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	"github.com/saturncloud/phoebe/internal/identity"
)

// trailerStripBody strips every X-Saturn-* name from the request's trailers
// once the body has been read to the end (ruling Q-R8STRIP2: no X-Saturn-*
// header or trailer reaches the upstream).
//
// The strip cannot run only at handler entry: the net/http server fills in
// r.Trailer while the body is being read, when the reader reaches EOF
// (transfer.go readTrailer/mergeSetHeader). It adds every trailer the client
// sent, declared or not, after any entry-time strip has already run. The
// server merges the trailers before it returns io.EOF to this wrapper, so
// stripping here on io.EOF (and on Close, which can drain the body and merge
// the trailers too) cleans the map before anything downstream copies it.
//
// Read and Close can run on different goroutines: the http.Transport reads the
// body on its write loop while the round-trip goroutine may Close it on an
// upstream error or a client abort. Both strip r.Trailer, and Go map writes
// from two goroutines are a fatal error that recover() cannot catch, so mu
// serializes the strips. mu is held only around the strip, never across the
// inner Read: Close must stay able to interrupt a blocked Read. The server's
// own trailer merge into r.Trailer needs no extra lock here: the server body
// merges the trailers once, under its own mutex, at the first EOF (reached
// either by a Read or by Close draining the body). Our Read strips only after
// it has seen that EOF, and once the body is closed a later Read returns an
// error instead of reading on, so no strip overlaps the merge.
type trailerStripBody struct {
	io.ReadCloser
	req *http.Request
	mu  sync.Mutex
}

// strip removes every X-Saturn-* name from the request's trailers, serialized
// against a concurrent Read or Close.
func (b *trailerStripBody) strip() {
	b.mu.Lock()
	defer b.mu.Unlock()
	identity.StripSaturnHeaders(b.req.Trailer)
}

func (b *trailerStripBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.strip()
	}
	return n, err
}

func (b *trailerStripBody) Close() error {
	err := b.ReadCloser.Close()
	b.strip()
	return err
}

// wrapTrailerStrip installs trailerStripBody on r.Body. It must run before any
// code reads the body, so every later reader (the body ceiling, gateway
// resolution, the usage rewrite, the wake probes) reads through it.
func wrapTrailerStrip(r *http.Request) {
	if r.Body == nil || r.Body == http.NoBody {
		return
	}
	r.Body = &trailerStripBody{ReadCloser: r.Body, req: r}
}

// newUpstreamProxy builds the single-host reverse proxy for every forward to
// the upstream (the metered forward and the wake probes). Its Director strips
// every X-Saturn-* name from the outbound request's own Trailer map, the
// one the transport sends. Request.Clone deep-copies Trailer, so this second
// strip covers a clone made from a trailer map that was not yet clean.
func newUpstreamProxy(upstream *url.URL) *httputil.ReverseProxy {
	rp := httputil.NewSingleHostReverseProxy(upstream)
	director := rp.Director
	rp.Director = func(out *http.Request) {
		director(out)
		identity.StripSaturnHeaders(out.Trailer)
	}
	return rp
}
