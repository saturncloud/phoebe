package proxy

import (
	"net/url"
	"testing"
)

func TestAuthorizedModelDiscoveryPath(t *testing.T) {
	tests := []struct {
		path       string
		discovery  bool
		authorized bool
	}{
		{"/v1/models", false, false},
		{"/health", false, false},
		{"/v1/models/org/adapter", true, true},
		{"/v1/models/org/adapter/ready", true, false},
		{"/v1/models/org/other", true, false},
		{"/v1/models/org/other/ready", true, false},
	}
	for _, tc := range tests {
		discovery, authorized := authorizedModelDiscoveryPath(tc.path, "org/adapter")
		if discovery != tc.discovery || authorized != tc.authorized {
			t.Fatalf("path %q = (%v,%v), want (%v,%v)", tc.path, discovery, authorized, tc.discovery, tc.authorized)
		}
	}

	// An endpoint whose own exact id ends in /ready remains addressable.
	discovery, authorized := authorizedModelDiscoveryPath(
		"/v1/models/org/adapter/ready", "org/adapter/ready",
	)
	if !discovery || !authorized {
		t.Fatal("exact model id ending in /ready must be authorized")
	}
}

func TestBoundRequestAllowed(t *testing.T) {
	allow := "org/adapter"
	for _, path := range []string{"/health", "/live", "/v1/models", "/v1/models/org/adapter"} {
		if !boundRequestAllowed("GET", path, allow) {
			t.Fatalf("GET %s must be allowed", path)
		}
	}
	for _, path := range []string{
		"/metrics", "/busy_threshold", "/docs", "/openapi.json", "/unknown",
		"/v1/models/org/sibling", "/v1/models/org/adapter/ready",
	} {
		if boundRequestAllowed("GET", path, allow) {
			t.Fatalf("GET %s must be blocked", path)
		}
		if boundRequestAllowed("HEAD", path, allow) {
			t.Fatalf("HEAD %s must be blocked", path)
		}
	}
	// OPTIONS is the method that used to bypass the path switch entirely. It is
	// now scoped to the inference surface a browser actually preflights, and
	// handleProxy answers it locally (204) rather than forwarding it — so
	// Dynamo can never answer a preflight for a graph-wide admin route.
	for _, path := range []string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings"} {
		if !boundRequestAllowed("OPTIONS", path, allow) {
			t.Fatalf("browser preflight on %s must be allowed", path)
		}
	}
	for _, path := range []string{
		"/metrics", "/busy_threshold", "/docs", "/openapi.json", "/unknown",
		"/future-admin", "/v1/models", "/v1/models/org/sibling", "/health", "/live",
	} {
		if boundRequestAllowed("OPTIONS", path, allow) {
			t.Fatalf("OPTIONS %s must be blocked (admin-surface preflight disclosure)", path)
		}
	}
	for _, path := range []string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings"} {
		if !boundRequestAllowed("POST", path, allow) {
			t.Fatalf("POST %s must be allowed", path)
		}
	}
	if boundRequestAllowed("POST", "/v1/responses", allow) {
		t.Fatal("Responses API must remain closed until its usage schema is billable")
	}
	for _, method := range []string{"POST", "PUT", "PATCH", "DELETE"} {
		for _, path := range []string{"/unknown", "/v1/models", "/metrics", "/busy_threshold"} {
			if boundRequestAllowed(method, path, allow) {
				t.Fatalf("%s %s must be blocked", method, path)
			}
		}
	}
}

// A PRESENT but empty-parsing allow-list authorizes no model at all, so
// /v1/models must fail CLOSED at the route gate — before the request reaches
// Dynamo — matching checkModelBinding's decision for the same input. Listing
// used to be allowed unconditionally and only failed later inside
// filterModelListResponse, which meant the request hit the graph and the caller
// got an opaque 502. /health and /live stay routable: they carry no per-model
// data once sanitized.
func TestBoundRequestAllowedModelListRequiresNonEmptyAllowList(t *testing.T) {
	for _, allow := range []string{"  ", " , , ", ","} {
		if boundRequestAllowed("GET", "/v1/models", allow) {
			t.Fatalf("GET /v1/models with empty-parsing allow-list %q must be blocked", allow)
		}
		if boundRequestAllowed("HEAD", "/v1/models", allow) {
			t.Fatalf("HEAD /v1/models with empty-parsing allow-list %q must be blocked", allow)
		}
		if !boundRequestAllowed("GET", "/health", allow) {
			t.Fatalf("GET /health must stay routable with allow-list %q", allow)
		}
		if !boundRequestAllowed("GET", "/live", allow) {
			t.Fatalf("GET /live must stay routable with allow-list %q", allow)
		}
	}
	if !boundRequestAllowed("GET", "/v1/models", "m") {
		t.Fatal("GET /v1/models with a real allow-list must be allowed")
	}
}

// The authorization gates decide on the decoded path while the reverse proxy
// forwards the RAW target. A request whose raw target is not byte-identical to
// its decoded form must be refused, never authorized as one string and
// forwarded as another.
func TestCanonicalRequestPathRefusesEncodedTargets(t *testing.T) {
	for _, target := range []string{
		"/v1%2Fchat/completions", // decodes to an allowlisted POST path
		"/v1/models/%6dine",      // decodes to an allowlisted discovery path
		"/v1/chat%2Fcompletions",
		"/%2e%2e/metrics",
	} {
		u, err := url.ParseRequestURI(target)
		if err != nil {
			t.Fatalf("parse %q: %v", target, err)
		}
		if _, ok := canonicalRequestPath(u); ok {
			t.Fatalf("encoded target %q must not be treated as canonical", target)
		}
	}
	for _, target := range []string{"/v1/chat/completions", "/v1/models", "/v1/models/mine", "/health"} {
		u, err := url.ParseRequestURI(target)
		if err != nil {
			t.Fatalf("parse %q: %v", target, err)
		}
		p, ok := canonicalRequestPath(u)
		if !ok || p != target {
			t.Fatalf("plain target %q = (%q,%v), want (%q,true)", target, p, ok, target)
		}
	}
}

func TestGatewayRequestAllowed(t *testing.T) {
	if !gatewayRequestAllowed("OPTIONS", "/v1/chat/completions") {
		t.Fatal("gateway preflight must be allowed")
	}
	for _, path := range []string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings"} {
		if !gatewayRequestAllowed("POST", path) {
			t.Fatalf("POST %s must be allowed", path)
		}
	}
	for _, path := range []string{"/health", "/live", "/v1/models", "/v1/responses", "/metrics", "/future-admin"} {
		if gatewayRequestAllowed("POST", path) || gatewayRequestAllowed("GET", path) {
			t.Fatalf("gateway route %s must be blocked", path)
		}
		// OPTIONS must not be the one method that opens every path. The handler
		// already 204s gateway preflight before forwarding, so this is defense
		// in depth — but it keeps both route gates saying the same thing.
		if gatewayRequestAllowed("OPTIONS", path) {
			t.Fatalf("gateway OPTIONS %s must be blocked", path)
		}
	}
}

func TestCheckModelBinding(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		allow string
		want  modelBindingResult
	}{
		// No allow-list -> binding not enforced (dedicated single-model route).
		{"no allow-list passes through", `{"model":"anything"}`, "", bindingOK},
		// Authorized model in a single-entry allow-list.
		{"authorized single", `{"model":"acme-bot"}`, "acme-bot", bindingOK},
		// The cross-model attack: authorized for acme-bot's subdomain, body names victim.
		{"cross-model attack refused", `{"model":"victim-bot"}`, "acme-bot", bindingMismatch},
		// Multi-entry allow-list (an endpoint serving base + adapter under two names).
		{"authorized in multi", `{"model":"acme-bot-base"}`, "acme-bot,acme-bot-base", bindingOK},
		{"unauthorized in multi", `{"model":"other"}`, "acme-bot,acme-bot-base", bindingMismatch},
		// Enforced but unverifiable -> fail closed.
		{"no model field, enforced", `{"messages":[]}`, "acme-bot", bindingUnparseable},
		{"empty model, enforced", `{"model":""}`, "acme-bot", bindingUnparseable},
		{"non-json body, enforced", `not json`, "acme-bot", bindingUnparseable},
		{"empty body, enforced", ``, "acme-bot", bindingUnparseable},
		// Whitespace in the header is trimmed.
		{"spaced allow-list", `{"model":"acme-bot"}`, " acme-bot , other ", bindingOK},
		// Case-sensitive (exact model ids).
		{"case sensitive mismatch", `{"model":"Acme-Bot"}`, "acme-bot", bindingMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := checkModelBinding([]byte(tc.body), tc.allow)
			if got != tc.want {
				t.Fatalf("checkModelBinding(%q, %q) = %d, want %d", tc.body, tc.allow, got, tc.want)
			}
		})
	}
}

func TestExtractRequestModel(t *testing.T) {
	if m, ok := extractRequestModel([]byte(`{"model":"x","stream":true}`)); !ok || m != "x" {
		t.Fatalf("extract = %q,%v want x,true", m, ok)
	}
	if _, ok := extractRequestModel([]byte(`{"no":"model"}`)); ok {
		t.Fatalf("missing model should be !ok")
	}
	if _, ok := extractRequestModel([]byte(`garbage`)); ok {
		t.Fatalf("non-json should be !ok")
	}
}

func TestCheckModelBinding_DuplicateAndWhitespace(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		allow string
		want  modelBindingResult
	}{
		// Duplicate top-level model keys -> unparseable (fail closed) regardless of order.
		{"dup model keys refused", `{"model":"victim","model":"acme-bot"}`, "acme-bot", bindingUnparseable},
		{"dup model keys refused rev", `{"model":"acme-bot","model":"victim"}`, "acme-bot", bindingUnparseable},
		// A nested "model" key (inside messages) is NOT a top-level dup -> fine.
		{"nested model ok", `{"model":"acme-bot","messages":[{"model":"x"}]}`, "acme-bot", bindingOK},
		// Whitespace-only allow-list (present but empty set) -> fail closed.
		{"whitespace allow-list fails closed", `{"model":"anything"}`, "   ", bindingMismatch},
		{"all-empty csv fails closed", `{"model":"anything"}`, " , , ", bindingMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := checkModelBinding([]byte(tc.body), tc.allow); got != tc.want {
				t.Fatalf("checkModelBinding(%q,%q)=%d want %d", tc.body, tc.allow, got, tc.want)
			}
		})
	}
}
