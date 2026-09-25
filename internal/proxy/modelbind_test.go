package proxy

import "testing"

func TestCheckModelBinding(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		allow     string
		want      modelBindingResult
		wantModel string
	}{
		// No allow-list -> binding not enforced (dedicated single-model route).
		{"no allow-list passes through", `{"model":"anything"}`, "", bindingOK, ""},
		// Authorized model in a single-entry allow-list.
		{"authorized single", `{"model":"acme-bot"}`, "acme-bot", bindingOK, "acme-bot"},
		// The cross-model attack: authorized for acme-bot's subdomain, body names victim.
		{"cross-model attack refused", `{"model":"victim-bot"}`, "acme-bot", bindingMismatch, ""},
		// Multi-entry allow-list (an endpoint serving base + adapter under two names).
		// The SINGULAR selected model must come back, never the CSV allow-list:
		// wake readiness matches /v1/models exactly.
		{"authorized in multi", `{"model":"acme-bot-base"}`, "acme-bot,acme-bot-base", bindingOK, "acme-bot-base"},
		{"authorized first of multi", `{"model":"acme-bot"}`, "acme-bot,acme-bot-base", bindingOK, "acme-bot"},
		{"unauthorized in multi", `{"model":"other"}`, "acme-bot,acme-bot-base", bindingMismatch, ""},
		// Enforced but unverifiable -> fail closed.
		{"no model field, enforced", `{"messages":[]}`, "acme-bot", bindingUnparseable, ""},
		{"empty model, enforced", `{"model":""}`, "acme-bot", bindingUnparseable, ""},
		{"non-json body, enforced", `not json`, "acme-bot", bindingUnparseable, ""},
		{"empty body, enforced", ``, "acme-bot", bindingUnparseable, ""},
		// Whitespace in the header is trimmed.
		{"spaced allow-list", `{"model":"acme-bot"}`, " acme-bot , other ", bindingOK, "acme-bot"},
		// Case-sensitive (exact model ids).
		{"case sensitive mismatch", `{"model":"Acme-Bot"}`, "acme-bot", bindingMismatch, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, gotModel := checkModelBinding([]byte(tc.body), tc.allow)
			if got != tc.want {
				t.Fatalf("checkModelBinding(%q, %q) = %d, want %d", tc.body, tc.allow, got, tc.want)
			}
			if gotModel != tc.wantModel {
				t.Fatalf("checkModelBinding(%q, %q) model = %q, want %q", tc.body, tc.allow, gotModel, tc.wantModel)
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
			got, gotModel := checkModelBinding([]byte(tc.body), tc.allow)
			if got != tc.want {
				t.Fatalf("checkModelBinding(%q,%q)=%d want %d", tc.body, tc.allow, got, tc.want)
			}
			if got != bindingOK && gotModel != "" {
				t.Fatalf("checkModelBinding(%q,%q) returned model %q on a non-OK result", tc.body, tc.allow, gotModel)
			}
		})
	}
}
