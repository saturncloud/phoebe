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
}

func TestFromRequestMissingHeadersAreEmpty(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	id := FromRequest(r)
	if id.AuthID != "" || id.UserID != "" || id.GroupID != "" || id.ResourceID != "" || id.ResourceType != "" {
		t.Fatalf("expected all-empty identity, got %+v", id)
	}
}
