package proxy

// CROSS-CLUSTER FORWARDING (issue #71, tenant-domains design section 5).
//
// Any cluster that serves Token Factory can be the entry point for an org
// host. Its phoebe resolves (org, model) from the registry; Atlas writes each
// dedicated endpoint's row to its serving cluster with an upstream, and a
// copy to every other cluster with forward_url set to the serving cluster's
// org host. This code never asks whether it runs on the hub: the row alone
// decides, and a spoke holding a copy of a hub endpoint's row forwards to the
// hub exactly as the hub forwards to a spoke.
//
// What crosses the hop: the method, path, query, body, and only the
// Authorization, Content-Type and Accept headers. No X-Saturn-* header is
// sent (the serving cluster's edge strips them anyway, and its own Atlas
// check re-authorizes the customer's key and re-pins the org). The
// destination comes only from the row's forward_url, over TLS with
// certificate verification.
//
// No metering, admission or wake happens on the entry: the forward runs
// before handleProxy's billing gate, so the serving cluster records the usage
// once. The first-byte timeout is the wake hold plus a margin, so a cold
// endpoint is held where it runs, not cut off at the entry. A request is
// never forwarded twice: the forward carries orgHostForwardedHeader, and a
// request that carries it is served only from a local row (508 otherwise).
// Atlas writes forwarding copies only to clusters other than the serving one,
// so on a correct install the serving cluster always holds the local row.

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
)

// orgHostForwardMargin is added to the wake hold to size the forward's
// first-byte timeout (design: "300 s plus a margin").
const orgHostForwardMargin = 60 * time.Second

// orgHostForwardHeaders is the complete list of request headers that cross
// the hop. Everything else is dropped.
var orgHostForwardHeaders = []string{"Authorization", "Content-Type", "Accept"}

// orgHostForwarder holds the transport for forwards to other clusters.
type orgHostForwarder struct {
	transport        http.RoundTripper
	firstByteTimeout time.Duration
}

// newOrgHostForwarder builds the forward transport: TLS 1.2+ with the system
// roots (verification on; there is no option to turn it off), compression
// off so streamed responses pass through chunk by chunk, and a first-byte
// timeout above the serving cluster's wake hold.
func newOrgHostForwarder(hold time.Duration) *orgHostForwarder {
	if hold < defaultWakeTimeout {
		// The serving cluster's hold is its own setting; never cut it off
		// below the default even when this entry runs a shorter one.
		hold = defaultWakeTimeout
	}
	timeout := hold + orgHostForwardMargin
	return &orgHostForwarder{
		firstByteTimeout: timeout,
		transport: &http.Transport{
			Proxy:                 nil,
			DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: timeout,
			DisableCompression:    true,
			ForceAttemptHTTP2:     true,
			MaxIdleConnsPerHost:   32,
			IdleConnTimeout:       90 * time.Second,
		},
	}
}

// forwardOrgHost forwards an org-host request to the serving cluster named by
// ep.ForwardURL and passes its answer through unchanged (status, headers,
// body, streaming).
func (s *Server) forwardOrgHost(w http.ResponseWriter, r *http.Request, id identity.Identity, ep gateway.DedicatedEndpoint, alreadyForwarded bool) {
	if alreadyForwarded {
		s.log.Error.Printf("org-host: refusing to forward a request a second time org_id=%s resource_id=%s (this cluster holds a forwarding copy of a row it should serve; check the cluster Atlas wrote it to)",
			id.OrgID, ep.ResourceID)
		writeClientError(w, "", http.StatusLoopDetected, orgHostForwardLoop)
		return
	}
	target, err := url.Parse(ep.ForwardURL)
	if err != nil || gateway.ValidForwardURL(ep.ForwardURL) != nil {
		s.log.Error.Printf("org-host: invalid forward_url for resource %s", ep.ResourceID)
		writeClientError(w, "", http.StatusBadGateway, orgHostForwardFailed)
		return
	}
	s.log.Info.Printf("org-host: forwarding org_id=%s resource_id=%s to cluster %q", id.OrgID, ep.ResourceID, ep.Cluster)
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			out := pr.Out
			out.URL.Scheme = "https"
			out.URL.Host = target.Host
			out.URL.Path = pr.In.URL.Path
			out.URL.RawPath = pr.In.URL.RawPath
			out.URL.RawQuery = pr.In.URL.RawQuery
			out.Host = target.Host
			h := http.Header{}
			for _, name := range orgHostForwardHeaders {
				if v := pr.In.Header.Values(name); len(v) > 0 {
					h[name] = append([]string(nil), v...)
				}
			}
			h.Set(orgHostForwardedHeader, "1")
			out.Header = h
			out.Trailer = nil
		},
		Transport:     s.orgHosts.forward.transport,
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			if isClientAbort(err) {
				return
			}
			s.log.Error.Printf("org-host: forward to cluster %q failed org_id=%s resource_id=%s: %v", ep.Cluster, id.OrgID, ep.ResourceID, err)
			writeClientError(w, "", http.StatusBadGateway, orgHostForwardFailed)
		},
	}
	rp.ServeHTTP(w, r)
}
