package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
)

// authorizedModelDiscoveryPath validates Dynamo's graph-wide per-model GET
// subtree against the endpoint's served-model allow-list. The wildcard model id
// may contain slashes. Bound routes authorize exact model ids only: Dynamo gives
// an exact sibling named <allowed>/ready precedence over the readiness
// subresource, so suffix-based authorization would be ambiguous and fail open.
func authorizedModelDiscoveryPath(path, servedModelAllowList string) (discovery, authorized bool) {
	const prefix = "/v1/models/"
	if !strings.HasPrefix(path, prefix) {
		return false, false
	}
	requested := strings.TrimPrefix(path, prefix)
	allow := parseServedModelAllowList(servedModelAllowList)
	if _, ok := allow[requested]; ok {
		return true, true
	}
	return true, false
}

// boundRequestAllowed is the complete public surface for a deployment-scoped
// endpoint. Dynamo's frontend also exposes graph-wide admin, metrics,
// documentation, batch storage, and future extension routes; those must not
// become customer APIs merely because the reverse proxy can reach them.
//
// OPTIONS is scoped to the inference POST surface — the only surface a browser
// preflights — and is answered LOCALLY by phoebe (handleProxy writes 204), never
// forwarded. An unconditional OPTIONS allowance would have let `OPTIONS /metrics`
// reach the Dynamo frontend, whose framework answers preflight with an Allow
// header enumerating a graph-wide admin route's methods: the exact disclosure
// the GET/HEAD gates below close, re-opened by one method.
func boundRequestAllowed(method, path, servedModelAllowList string) bool {
	if method == "OPTIONS" {
		return inferenceRequestPathAllowed(path)
	}
	if method == "POST" {
		return inferenceRequestPathAllowed(path)
	}
	if method != "GET" && method != "HEAD" {
		return false
	}
	switch path {
	case "/health", "/live":
		// Routable unconditionally — they carry no per-model data ONCE
		// sanitized. proxy.go rewrites their body to a status-only document
		// (sanitizeReadinessResponse), because Dynamo's readiness is
		// graph-wide and would otherwise enumerate sibling adapters.
		return true
	case "/v1/models":
		// Requires a non-empty PARSED allow-list. A present-but-empty header
		// (whitespace, ",,") authorizes no model at all, so listing must fail
		// CLOSED here — at the route gate, before the request reaches Dynamo —
		// matching checkModelBinding's decision for the same input. Allowing it
		// through only to have filterModelListResponse error made the request
		// hit the graph and surfaced as an opaque 502.
		return len(parseServedModelAllowList(servedModelAllowList)) > 0
	default:
		discovery, authorized := authorizedModelDiscoveryPath(path, servedModelAllowList)
		return discovery && authorized
	}
}

// canonicalRequestPath returns the path to authorize on, reporting ok=false
// when the raw request target is not byte-identical to its decoded form.
//
// The authorization gates decide on the percent-DECODED r.URL.Path, but the
// reverse proxy forwards the RAW target: `GET /v1/models/%6dine` decodes to
// "/v1/models/mine" (authorized when "mine" is the served name) while the
// forwarded RequestURI stays "/v1/models/%6dine", and `POST
// /v1%2Fchat/completions` decodes to an allowlisted path while forwarding one
// that is not. Any upstream router that normalizes percent-encoding differently
// from net/url then resolves a path the allow-list never approved. Go populates
// URL.RawPath only when the escaped form differs from the decoded form, so
// RawPath != "" is exactly the "encoded path" signal; phoebe refuses those
// rather than guessing which form the upstream will honour.
func canonicalRequestPath(u *url.URL) (string, bool) {
	if u.RawPath != "" && u.RawPath != u.Path {
		return "", false
	}
	return u.Path, true
}

// inferenceRequestPathAllowed lists the model-bearing APIs whose response
// usage schema Phoebe can meter. Responses API uses different token field names
// and remains closed until its billing parser is implemented.
func inferenceRequestPathAllowed(path string) bool {
	switch path {
	case "/v1/chat/completions", "/v1/completions", "/v1/embeddings":
		return true
	default:
		return false
	}
}

// gatewayRequestAllowed is narrower than the bound-resource surface because a
// shared gateway URL does not identify one model for health or discovery. The
// request body supplies that identity only on model-bearing POST requests.
//
// OPTIONS is scoped to the same inference surface as POST. handleProxy already
// answers gateway preflight locally with a 204 before any forward, so this is
// defense in depth rather than a behaviour change — but it keeps the two route
// gates saying the same thing, so a future refactor that drops the
// short-circuit cannot silently open every path to OPTIONS.
func gatewayRequestAllowed(method, path string) bool {
	return (method == "OPTIONS" || method == "POST") && inferenceRequestPathAllowed(path)
}

var errNotObject = errors.New("request body is not a JSON object")

// modelBindingResult is the outcome of the request-body model= binding check.
type modelBindingResult int

const (
	// bindingOK: the request model matches an authorized served-model (or the
	// binding is not enforced for this route).
	bindingOK modelBindingResult = iota
	// bindingMismatch: the request names a model NOT in the authorized allow-list.
	// Fail closed (403) — the caller is trying to escape the subdomain-authorized
	// resource (the shared-mode cross-model attack).
	bindingMismatch
	// bindingUnparseable: the request body is present but its model= could not be
	// read (not JSON, or no model field). Fail closed when the binding is enforced:
	// a request whose model we cannot verify must not reach a shared upstream where
	// Dynamo would route it by an unverified name.
	bindingUnparseable
)

// checkModelBinding enforces the shared-mode security crux: the request-body
// `model=` must be one the subdomain-authorized resource is allowed to serve.
//
// atlas-auth authorized the caller for a SUBDOMAIN → resource, and injected the
// resource's allowed served-model name(s) as `servedModelAllowList` (a
// comma-separated header). Dynamo, however, routes on the request-body `model=`,
// and a shared graph fronts many tenants' models behind ONE upstream — so
// without this check a caller authorized for model-A could send `model=B` and be
// served B. This binds the two: request model ∈ allow-list, else fail closed.
//
// An ABSENT allow-list means Atlas did not mark this route as bound, and phoebe
// then enforces NOTHING: not the body binding here, not the route gate
// (proxy.go, also conditioned on ServedModel != ""), and not the /v1/models
// filter — so such a route forwards Dynamo's full graph-wide model list. That
// fail-open is safe ONLY because X-Saturn-Served-Model is injected and
// anti-spoof overwritten server-side by the Atlas-rendered Traefik middleware
// (identity.HeaderServedModel): a client cannot cause its absence. It is NOT
// justified by "one subdomain == one model" — a dedicated Dynamo graph may host
// a base model plus several attached adapters, which is exactly why dedicated
// routes now carry a served-name allow-list too. An absent header on such a
// graph would disable all three protections at once.
//
// Atlas decides access; this only guarantees the body can't escape the
// Atlas-authorized resource.
func checkModelBinding(body []byte, servedModelAllowList string) modelBindingResult {
	// An ABSENT header (empty string) = binding not enforced. But a PRESENT
	// header that parses to an EMPTY set
	// (e.g. a whitespace-only served name, or all-empty CSV parts) must fail
	// CLOSED, not open: an empty allow-list on a shared route would let ANY
	// model= through, defeating the binding. The caller only reaches here when
	// the header is non-empty, so "present but empty set" is the attack/bug case.
	if servedModelAllowList == "" {
		return bindingOK // truly absent (empty header) -> not a shared-binding route
	}
	allow := parseServedModelAllowList(servedModelAllowList)
	if len(allow) == 0 {
		// Present (non-empty header) but parsed to nothing — whitespace-only or
		// all-empty CSV parts. Fail CLOSED: an empty allow-list on a route that
		// DID inject the header would let any model= through.
		return bindingMismatch
	}
	model, ok := extractRequestModel(body)
	if !ok {
		// Enforced but the model is unreadable — fail closed.
		return bindingUnparseable
	}
	if _, authorized := allow[model]; authorized {
		return bindingOK
	}
	return bindingMismatch
}

// parseServedModelAllowList splits the comma-separated header into a set,
// trimming spaces and dropping empties. Case-sensitive: model ids are exact
// (the served-name is claimed verbatim and Dynamo routes on it verbatim).
func parseServedModelAllowList(h string) map[string]struct{} {
	if h == "" {
		return nil
	}
	set := map[string]struct{}{}
	for _, part := range strings.Split(h, ",") {
		if p := strings.TrimSpace(part); p != "" {
			set[p] = struct{}{}
		}
	}
	return set
}

// extractRequestModel pulls the top-level "model" string from an OpenAI-shaped
// request body. Returns ("", false) if the body is not a JSON object, has no
// model field, model is not a non-empty string, OR the object has DUPLICATE
// top-level "model" keys.
//
// The duplicate-key check is a security guard: a plain struct Unmarshal silently
// takes the LAST duplicate, but the Dynamo router downstream may resolve
// duplicates differently (e.g. first-wins). If phoebe validated the last and the
// router routed the first, `{"model":"victim","model":"mine"}` would bypass the
// binding. We can't see the router's resolution, so we refuse the ambiguity: a
// body with two top-level "model" keys fails the check (-> bindingUnparseable ->
// 403). A well-formed request has exactly one.
func extractRequestModel(body []byte) (string, bool) {
	if len(body) == 0 {
		return "", false
	}
	// Fast path for the value; then verify uniqueness of the top-level key.
	var m struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		return "", false
	}
	if m.Model == "" {
		return "", false
	}
	if count, err := countTopLevelModelKeys(body); err != nil || count != 1 {
		return "", false
	}
	return m.Model, true
}

// countTopLevelModelKeys counts how many times "model" appears as a key of the
// top-level JSON object, using a streaming token decoder (so nested "model"
// keys, e.g. inside an array element, are not counted). Returns an error if the
// body is not a JSON object.
func countTopLevelModelKeys(body []byte) (int, error) {
	counts, err := countTopLevelKeys(body, map[string]struct{}{"model": {}})
	return counts["model"], err
}

// countTopLevelKeys rejects ambiguity in fields whose interpretation affects
// authorization or capacity. Downstream JSON stacks do not universally agree
// on first-vs-last duplicate-key handling, so Phoebe must never validate or
// reserve against one value while Dynamo/vLLM consumes another.
func countTopLevelKeys(body []byte, wanted map[string]struct{}) (map[string]int, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, errNotObject
	}
	counts := make(map[string]int, len(wanted))
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := tok.(string)
		if !ok {
			return nil, errNotObject
		}
		if _, ok := wanted[key]; ok {
			counts[key]++
		}
		if err := skipValue(dec); err != nil {
			return nil, err
		}
	}
	if _, err := dec.Token(); err != nil { // closing top-level object
		return nil, err
	}
	return counts, nil
}

// skipValue consumes exactly one JSON value from the decoder (scalar, object, or
// array), leaving the decoder positioned after it.
func skipValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	d, ok := tok.(json.Delim)
	if !ok {
		return nil // a scalar value — done
	}
	if d != '{' && d != '[' {
		return nil
	}
	// Recurse through the nested container to its matching close.
	depth := 1
	for depth > 0 {
		t, err := dec.Token()
		if err != nil {
			return err
		}
		if dd, ok := t.(json.Delim); ok {
			switch dd {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
	return nil
}
