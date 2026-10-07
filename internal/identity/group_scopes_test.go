package identity

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// TestFromRequestGroupScopesUntrustedHeaderAbsent pins the R3 gate for the new
// envelope: when the header is NOT in the active trusted set, it reads as
// ABSENT — no raw value reaches the proxy parse, no matter what the client
// sends.
func TestFromRequestGroupScopesUntrustedHeaderAbsent(t *testing.T) {
	const gidA = "a1b2c3d4e5f60718293a4b5c6d7e8f90"
	// The active set WITHOUT X-Saturn-Group-Scopes, as a not-yet-upgraded
	// chart would render it.
	withTrustedHeadersEnv(t, strings.Join([]string{
		HeaderGateway, HeaderOrgID, HeaderOwnerID, HeaderServingMode, HeaderServedModel,
	}, ","), true)

	r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	r.Header.Set(HeaderGroupScopes, "v1;"+gidA+":,,,,")
	id := FromRequest(r)
	if id.GroupScopes != "" {
		t.Fatalf("untrusted group scopes read as present: GroupScopes=%q, want absent", id.GroupScopes)
	}
}
