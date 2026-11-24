package proxy

// SPOKE ORG-HOST CHECK RELAY (issue #71, build decision 8).
//
// The org host's ForwardAuth is Atlas's /api/auth/org-host/check, and Atlas
// finds the org from the host the customer called. On the hub, Traefik sends
// that host in X-Forwarded-Host straight to the in-cluster Atlas. On a cluster
// whose Atlas is remote (a spoke), the ForwardAuth request would have to pass
// through the hub's public Traefik, which does not trust forwarded headers
// and replaces X-Forwarded-Host with the hub's own app host, so Atlas could
// never find the org and refused everything.
//
// This relay runs on its own listener (orgHostCheck.listen), reachable only
// from this cluster's Traefik (the chart's NetworkPolicy). The spoke's
// tf-org-host-auth points at it. It reads the X-Forwarded-Host this cluster's
// Traefik set (Traefik replaces a client's copy because it trusts no
// forwarded headers), and asks the hub's Atlas
// GET <orgHostCheck.atlasCheckURL>?host=<host> with only the caller's
// Authorization. Atlas takes the host from that query parameter.
//
// It is an auth relay, so every rule fails closed:
//   - exactly one X-Forwarded-Host value (none, several, or a comma-joined
//     list gets Atlas's own 403 wrong_organization body);
//   - only Authorization is sent: no cookies, no body, no other header;
//   - nothing is cached: every request asks Atlas;
//   - Atlas's answer is relayed as is for 2xx and 4xx (status and body),
//     with only X-Saturn-Org-Id, X-Saturn-Auth-Id, X-Saturn-Owner-Id and
//     X-Saturn-Endpoint-Scope (and the JSON Content-Type of a refusal);
//   - a redirect, a 5xx, a timeout (10 s), a TLS failure or any other error
//     is a 503 JSON refusal;
//   - TLS is verified with the system roots; there is no option to turn it off.

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

// OrgHostCheckPath is the relay's one path on its listener.
const OrgHostCheckPath = "/org-host-check"

// orgHostCheckTimeout bounds one Atlas check.
const orgHostCheckTimeout = 10 * time.Second

// orgHostCheckMaxBody bounds the Atlas answer the relay reads and passes on.
const orgHostCheckMaxBody = 64 << 10

// orgHostCheckResponseHeaders are the only Atlas response headers relayed on
// success: exactly the tf-org-host-auth ForwardAuth's authResponseHeaders.
var orgHostCheckResponseHeaders = []string{
	identity.HeaderOrgID,
	identity.HeaderAuthID,
	identity.HeaderOwnerID,
	identity.HeaderEndpointScope,
}

// orgHostCheckWrongOrganization is byte-for-byte the body Atlas gives for a
// key or host that does not belong (pdc/routes/auth.py,
// ORG_HOST_WRONG_ORGANIZATION_MESSAGE), so a request the relay refuses itself
// looks exactly like one Atlas refused.
var orgHostCheckWrongOrganization = []byte(`{"error": {"message": "This key cannot be used on this host. Use an API key pinned to the organization this host belongs to.", "type": "permission_error", "code": "wrong_organization", "param": null}}`)

// OrgHostCheckRelay is the spoke check relay's handler.
type OrgHostCheckRelay struct {
	atlasCheckURL *url.URL
	client        *http.Client
	log           *logging.Logger
}

// NewOrgHostCheckRelay builds the relay for atlasCheckURL (validated by
// config: https, no query). transport may be nil for the verified default.
func NewOrgHostCheckRelay(atlasCheckURL string, transport http.RoundTripper, log *logging.Logger) (*OrgHostCheckRelay, error) {
	u, err := url.Parse(atlasCheckURL)
	if err != nil || u.Host == "" || u.RawQuery != "" {
		return nil, errors.New("orgHostCheck.atlasCheckURL must be an absolute URL with no query")
	}
	if transport == nil {
		transport = &http.Transport{
			Proxy:               nil,
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12},
			TLSHandshakeTimeout: 5 * time.Second,
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     90 * time.Second,
		}
	}
	return &OrgHostCheckRelay{
		atlasCheckURL: u,
		client: &http.Client{
			Transport: transport,
			Timeout:   orgHostCheckTimeout,
			// A redirect from Atlas (a login page) is never followed; it is
			// answered with 503 below.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		log: log,
	}, nil
}

// Handler serves only GET OrgHostCheckPath; everything else is 404.
func (c *OrgHostCheckRelay) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(OrgHostCheckPath, c.serve)
	return mux
}

func (c *OrgHostCheckRelay) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != OrgHostCheckPath || r.Method != http.MethodGet {
		writeClientError(w, "", http.StatusNotFound, orgHostUnknownPath)
		return
	}
	hosts := r.Header.Values("X-Forwarded-Host")
	if len(hosts) != 1 || hosts[0] == "" || strings.ContainsAny(hosts[0], ", \t") {
		c.log.Warn.Printf("org-host check relay: refusing a request with %d X-Forwarded-Host values (Traefik sets exactly one)", len(hosts))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write(orgHostCheckWrongOrganization)
		return
	}
	target := *c.atlasCheckURL
	target.RawQuery = url.Values{"host": {hosts[0]}}.Encode()
	out, err := http.NewRequestWithContext(r.Context(), http.MethodGet, target.String(), http.NoBody)
	if err != nil {
		c.unavailable(w, "build request", err)
		return
	}
	// Only the caller's credential crosses; never cookies, the body, or any
	// other header.
	out.Header = http.Header{}
	if v := r.Header.Values("Authorization"); len(v) > 0 {
		out.Header["Authorization"] = append([]string(nil), v...)
	}
	resp, err := c.client.Do(out)
	if err != nil {
		c.unavailable(w, "atlas check", err)
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, orgHostCheckMaxBody+1))
	if err != nil || len(body) > orgHostCheckMaxBody {
		c.unavailable(w, "read atlas answer", err)
		return
	}
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		for _, name := range orgHostCheckResponseHeaders {
			if v := resp.Header.Get(name); v != "" {
				w.Header().Set(name, v)
			}
		}
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		if ct := resp.Header.Get("Content-Type"); strings.HasPrefix(ct, "application/json") {
			w.Header().Set("Content-Type", "application/json")
		}
	default:
		c.unavailable(w, "atlas status "+http.StatusText(resp.StatusCode), nil)
		return
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, bytes.NewReader(body))
}

func (c *OrgHostCheckRelay) unavailable(w http.ResponseWriter, what string, err error) {
	c.log.Error.Printf("org-host check relay: %s failed: %v (refusing)", what, err)
	writeClientError(w, "", http.StatusServiceUnavailable, orgHostUnavailable)
}
