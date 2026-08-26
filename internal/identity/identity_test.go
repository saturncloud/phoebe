package identity

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFromRequestCapturesAllHeaders(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	r.Header.Set(HeaderAuthID, "auth-abc")
	r.Header.Set(HeaderUserID, "user-1")
	r.Header.Set(HeaderGroupID, "group-2")
	r.Header.Set(HeaderResourceID, "model-x")
	r.Header.Set(HeaderResourceType, "deployment")
	r.Header.Set(HeaderOrgID, "org-7")
	r.Header.Set(HeaderBaseModel, "meta-llama/Llama-3.1-8B-Instruct")
	r.Header.Set(HeaderAdapter, "ckpt-artifact-42")
	r.Header.Set(HeaderUpstream, "pd-x.main-namespace.svc.cluster.local:8000")

	id := FromRequest(r)

	if id.AuthID != "auth-abc" {
		t.Errorf("AuthID = %q, want auth-abc", id.AuthID)
	}
	if id.UserID != "user-1" {
		t.Errorf("UserID = %q, want user-1", id.UserID)
	}
	if id.GroupID != "group-2" {
		t.Errorf("GroupID = %q, want group-2", id.GroupID)
	}
	if id.ResourceID != "model-x" {
		t.Errorf("ResourceID = %q, want model-x", id.ResourceID)
	}
	if id.ResourceType != "deployment" {
		t.Errorf("ResourceType = %q, want deployment", id.ResourceType)
	}
	if id.OrgID != "org-7" {
		t.Errorf("OrgID = %q, want org-7", id.OrgID)
	}
	if id.BaseModel != "meta-llama/Llama-3.1-8B-Instruct" {
		t.Errorf("BaseModel = %q, want meta-llama/Llama-3.1-8B-Instruct", id.BaseModel)
	}
	if id.Adapter != "ckpt-artifact-42" {
		t.Errorf("Adapter = %q, want ckpt-artifact-42", id.Adapter)
	}
	if id.Upstream != "pd-x.main-namespace.svc.cluster.local:8000" {
		t.Errorf("Upstream = %q, want pd-x.main-namespace.svc.cluster.local:8000", id.Upstream)
	}
}

func TestFromRequestMissingHeadersAreEmpty(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	id := FromRequest(r)
	if id.AuthID != "" || id.UserID != "" || id.GroupID != "" || id.ResourceID != "" ||
		id.ResourceType != "" || id.OrgID != "" || id.BaseModel != "" || id.Adapter != "" || id.Upstream != "" {
		t.Fatalf("expected all-empty identity, got %+v", id)
	}
}

// TestFromRequestGatewayMarkerIsStrict: only the EXACT value "true" marks a
// gateway request — any other value (or absence) keeps the header-routed path,
// which fails closed on its missing upstream. The middleware injects exactly
// "true"; anything else means the value didn't come from it.
func TestFromRequestGatewayMarkerIsStrict(t *testing.T) {
	for v, want := range map[string]bool{
		"true": true, "": false, "1": false, "TRUE": false, "True": false, " true": false,
	} {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		if v != "" {
			r.Header.Set(HeaderGateway, v)
		}
		if got := FromRequest(r).Gateway; got != want {
			t.Errorf("Gateway for header %q = %v, want %v", v, got, want)
		}
	}
}
