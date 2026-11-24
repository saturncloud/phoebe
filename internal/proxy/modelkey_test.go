package proxy

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
)

// Battery round 2 (issue #71): encoding/json matches struct keys ignoring
// case and keeps the last match, while the engines match keys exactly. A body
// with "model" and a case variant must never let phoebe resolve, authorize
// and bill one model while the engine serves another.

func TestExtractRequestModelRefusesCaseVariantsAndDuplicates(t *testing.T) {
	for _, body := range []string{
		`{"model":"victim","Model":"mine"}`,
		`{"Model":"mine","model":"victim"}`,
		`{"model":"victim","MODEL":"mine"}`,
		`{"Model":"mine"}`,
		`{"model":"a","model":"b"}`,
	} {
		if m, ok := extractRequestModel([]byte(body)); ok {
			t.Fatalf("%s: got %q, want refusal", body, m)
		}
	}
	if m, ok := extractRequestModel([]byte(`{"model":"a","messages":[{"Model":"x","model":"y"}]}`)); !ok || m != "a" {
		t.Fatalf("nested model keys must be allowed: %q %v", m, ok)
	}
}

func TestCheckModelBindingRefusesACaseVariant(t *testing.T) {
	if got := checkModelBinding([]byte(`{"model":"victim","Model":"acme-bot"}`), "acme-bot"); got != bindingUnparseable {
		t.Fatalf("got %d, want bindingUnparseable", got)
	}
}

func TestOrgHostCaseVariantModelKeyIsRefusedForANarrowedKey(t *testing.T) {
	be := &orgEngine{}
	srv := httptest.NewServer(be)
	defer srv.Close()
	up, _ := url.Parse(srv.URL)
	res := &fakeTierResolver{dedicated: []gateway.DedicatedEndpoint{
		dedicatedRow("org-1", "mine", "dep-1", up.Host),
		dedicatedRow("org-1", "other", "dep-2", up.Host),
	}}
	em := &recordingEmitter{}
	s := newOrgHostTestServer(t, em, res, nil)
	for _, body := range []string{
		`{"model":"other","Model":"mine","messages":[]}`,
		`{"Model":"mine","model":"other","messages":[]}`,
	} {
		req := orgHostRequest(http.MethodPost, "/v1/chat/completions", "org-1", body)
		req.Header.Set(identity.HeaderEndpointScope, "dep-1")
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest || errorCode(t, rr.Body.Bytes()) != "model_required" {
			t.Fatalf("%s: %d %s, want 400 model_required", body, rr.Code, rr.Body.String())
		}
		if strings.Contains(rr.Body.String(), "mine") || strings.Contains(rr.Body.String(), "other") {
			t.Fatalf("refusal echoes a model name: %s", rr.Body.String())
		}
	}
	if be.hits.Load() != 0 {
		t.Fatalf("engine hits %d, want 0", be.hits.Load())
	}
}

func TestSharedHostCaseVariantModelKeyIs400(t *testing.T) {
	resolver := &mapResolver{m: map[[2]string]gateway.Resolution{
		{"org-1", "mine"}: {ResourceID: "tfm-1", BaseModel: "b", GraphK8sName: "g"},
	}}
	srv := newGatewayTestServer(t, &recordingEmitter{}, resolver, nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, gatewayRequest("org-1", `{"model":"victim","Model":"mine"}`))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if atomic.LoadInt32(&resolver.calls) != 0 {
		t.Fatal("an ambiguous body must not reach the resolver")
	}
}

func TestOrgHostVideoCreateRefusesACaseVariantModel(t *testing.T) {
	jsonReq := httptest.NewRequest(http.MethodPost, "/v1/videos", strings.NewReader(`{"model":"a","Model":"b","prompt":"p"}`))
	jsonReq.Header.Set("Content-Type", "application/json")
	if m, err := videoCreateModel(jsonReq); err == nil {
		t.Fatalf("json case variant accepted: %q", m)
	}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("model", "a")
	_ = mw.WriteField("Model", "b")
	_ = mw.Close()
	mpReq := httptest.NewRequest(http.MethodPost, "/v1/videos", &buf)
	mpReq.Header.Set("Content-Type", mw.FormDataContentType())
	if m, err := videoCreateModel(mpReq); err == nil {
		t.Fatalf("multipart case variant accepted: %q", m)
	}
	// The body is still restored for phoebe-videos on success.
	ok := httptest.NewRequest(http.MethodPost, "/v1/videos", strings.NewReader(`{"model":"a","prompt":"p"}`))
	ok.Header.Set("Content-Type", "application/json")
	if m, err := videoCreateModel(ok); err != nil || m != "a" {
		t.Fatalf("plain create: %q %v", m, err)
	}
	if b, _ := io.ReadAll(ok.Body); !strings.Contains(string(b), `"prompt"`) {
		t.Fatal("body not restored")
	}
}
