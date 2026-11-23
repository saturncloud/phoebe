package meteredusage

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"
)

// Connection timeouts. A manager that cannot be reached (egress blocked by a
// firewall that drops packets, for instance) must fail in seconds, not after
// the whole request timeout, so a run can tell "cannot reach the manager"
// from "the manager is slow" well inside its CronJob deadline. The overall
// request timeout (http.Client.Timeout) still bounds a slow response.
const (
	DialTimeout         = 10 * time.Second
	TLSHandshakeTimeout = 10 * time.Second
)

// NewHTTPClient returns the HTTP client token-push uses: requestTimeout bounds
// each attempt as a whole, and connecting is bounded by DialTimeout and
// TLSHandshakeTimeout. Proxy settings come from the environment, as with
// http.DefaultTransport.
func NewHTTPClient(requestTimeout time.Duration) *http.Client {
	return newHTTPClient(requestTimeout, DialTimeout, TLSHandshakeTimeout)
}

func newHTTPClient(requestTimeout, dialTimeout, handshakeTimeout time.Duration) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext
	t.TLSHandshakeTimeout = handshakeTimeout
	return &http.Client{Timeout: requestTimeout, Transport: t}
}

// Unreachable reports whether err means the manager could not be reached at
// all: a DNS failure, a refused or timed-out connection, or a failed TLS
// handshake (including a certificate that does not verify, as from an
// intercepting proxy). A 5xx, a 429, or a response that arrived and was then
// cut off are not "unreachable": the manager or something in front of it
// answered.
func Unreachable(err error) bool {
	if err == nil {
		return false
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) {
		return true
	}
	var recordErr tls.RecordHeaderError
	if errors.As(err, &recordErr) {
		return true
	}
	var verifyErr *tls.CertificateVerificationError
	if errors.As(err, &verifyErr) {
		return true
	}
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	if errors.As(err, &unknownAuthority) || errors.As(err, &hostname) || errors.As(err, &invalid) {
		return true
	}
	// net/http reports a handshake that exceeded TLSHandshakeTimeout with an
	// unexported error type; its text is stable.
	return strings.Contains(err.Error(), "TLS handshake timeout")
}
