package atlastoken

import (
	"fmt"
	"net/url"
	"strings"
)

// CheckAtlasURL refuses an Atlas URL that would send the Atlas service token
// in cleartext over a network phoebe does not control. The token (and, in the
// transition build, the legacy wake secret) goes in a header on every call,
// and a spoke points this URL at Atlas's external address, so the URL must be
// https unless its host is cluster-local: a name ending in ".svc" or
// ".svc.cluster.local" (dot-anchored, so "evilsvc.com" and "a.svc.evil.com"
// are not local), or exactly localhost, 127.0.0.1 or ::1. A host with a
// trailing root dot ("atlas.saturn.svc.", "localhost.") is not cluster-local:
// the dot skips the pod's DNS search path, so the name resolves outside the
// cluster, and the phoebe chart refuses it too. Any other scheme is refused. It checks only the scheme and host; callers check the rest of the
// URL's shape.
func CheckAtlasURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("the Atlas URL %q does not parse: %w", raw, err)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("the Atlas URL %q has no host", raw)
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return nil
	case "http":
		if isClusterLocalHost(host) {
			return nil
		}
		return fmt.Errorf("the Atlas URL %q uses http to %q; it must be https, because every call carries the Atlas service token (http is allowed only for a host ending in .svc or .svc.cluster.local, or localhost, 127.0.0.1, ::1)", raw, host)
	default:
		return fmt.Errorf("the Atlas URL %q has scheme %q; it must be https (or http to a cluster-local host)", raw, u.Scheme)
	}
}

// isClusterLocalHost reports whether host is a Kubernetes service name
// (ending in .svc or .svc.cluster.local) or a loopback name. A trailing root
// dot makes any host non-local.
func isClusterLocalHost(host string) bool {
	h := strings.ToLower(host)
	if strings.HasSuffix(h, ".") {
		return false
	}
	switch h {
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	return strings.HasSuffix(h, ".svc") || strings.HasSuffix(h, ".svc.cluster.local")
}
