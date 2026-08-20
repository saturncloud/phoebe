package proxy

import (
	"encoding/json"
	"strings"
)

// modelBindingResult is the outcome of the request-body model= binding check.
type modelBindingResult int

const (
	// bindingOK: the request model matches an authorized served-model (or the
	// binding is not enforced for this route).
	bindingOK modelBindingResult = iota
	// bindingMismatch: the request names a model NOT in the authorized allow-list.
	// Fail closed (403) — the caller is trying to escape the subdomain-authorized
	// resource (the shared-tier cross-model attack).
	bindingMismatch
	// bindingUnparseable: the request body is present but its model= could not be
	// read (not JSON, or no model field). Fail closed when the binding is enforced:
	// a request whose model we cannot verify must not reach a shared upstream where
	// Dynamo would route it by an unverified name.
	bindingUnparseable
)

// checkModelBinding enforces the shared-tier security crux: the request-body
// `model=` must be one the subdomain-authorized resource is allowed to serve.
//
// atlas-auth authorized the caller for a SUBDOMAIN → resource, and injected the
// resource's allowed served-model name(s) as `servedModelAllowList` (a
// comma-separated header). Dynamo, however, routes on the request-body `model=`,
// and a shared graph fronts many tenants' models behind ONE upstream — so
// without this check a caller authorized for model-A could send `model=B` and be
// served B. This binds the two: request model ∈ allow-list, else fail closed.
//
// The allow-list is empty for routes that don't enforce binding (a dedicated
// single-model endpoint: one subdomain == one model, the engine can only serve
// the one thing) → bindingOK, no parse, no cost. atlas DECIDES access; this only
// guarantees the body can't escape the atlas-authorized resource.
func checkModelBinding(body []byte, servedModelAllowList string) modelBindingResult {
	allow := parseServedModelAllowList(servedModelAllowList)
	if len(allow) == 0 {
		// Binding not enforced for this route (no allow-list injected).
		return bindingOK
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
// model field, or model is not a non-empty string. Pure + allocation-light (it
// unmarshals only the model field).
func extractRequestModel(body []byte) (string, bool) {
	if len(body) == 0 {
		return "", false
	}
	var m struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		return "", false
	}
	if m.Model == "" {
		return "", false
	}
	return m.Model, true
}
