package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
)

// requestMethodCarriesModel identifies requests that can ask Dynamo to route
// to a served model. Read-only discovery and health requests have no model body
// and must continue to work on a bound endpoint.
func requestMethodCarriesModel(method string) bool {
	switch method {
	case "GET", "HEAD", "OPTIONS":
		return false
	default:
		return true
	}
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
// The allow-list is empty for routes that don't enforce binding. Atlas decides
// access; this only guarantees the body can't escape the Atlas-authorized
// resource. Dedicated Dynamo routes carry a single served name because one
// graph may host a base model plus several attached adapters.
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
