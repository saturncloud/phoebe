package proxy

import (
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"

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
type trailerStripBody struct {
	io.ReadCloser
	req *http.Request
}

func (b *trailerStripBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		identity.StripSaturnHeaders(b.req.Trailer)
	}
	return n, err
}

func (b *trailerStripBody) Close() error {
	err := b.ReadCloser.Close()
	identity.StripSaturnHeaders(b.req.Trailer)
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
