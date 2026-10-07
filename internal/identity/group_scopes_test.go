package identity

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// TestFromRequestMemberGroupIDs pins the metering-evidence side of the group
// quota envelope: the trusted X-Saturn-Group-Scopes header parses to the
// caller's group list (envelope order), and an untrusted or absent header
// leaves the list empty (the drainer stores NULL).
func TestFromRequestMemberGroupIDs(t *testing.T) {
	const gidA = "a1b2c3d4e5f60718293a4b5c6d7e8f90"
	const gidB = "00112233445566778899aabbccddeeff"

	cases := []struct {
		name   string
		header string
		want   []string
	}{
		{"absent", "", nil},
		{"single", "v1;" + gidA + ":30,1000,500,2000,100.000000000", []string{gidA}},
		{"multi in envelope order", "v1;" + gidB + ":,,,,;" + gidA + ":1,,,", []string{gidB, gidA}},
		{"all-empty entry still lists the group", "v1;" + gidA + ":,,,,", []string{gidA}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
			if tc.header != "" {
				r.Header.Set(HeaderGroupScopes, tc.header)
			}
			id := FromRequest(r)
			if len(id.MemberGroupIDs) != len(tc.want) {
				t.Fatalf("MemberGroupIDs = %v, want %v", id.MemberGroupIDs, tc.want)
			}
			for i, want := range tc.want {
				if id.MemberGroupIDs[i] != want {
					t.Fatalf("MemberGroupIDs[%d] = %q, want %q", i, id.MemberGroupIDs[i], want)
				}
			}
			// The raw envelope rides the Identity for the strict proxy parse.
			if tc.header != "" && id.GroupScopes != tc.header {
				t.Fatalf("GroupScopes = %q, want the raw header verbatim", id.GroupScopes)
			}
		})
	}
}

// TestFromRequestMemberGroupIDsLenient pins the deliberate split of
// responsibility: the identity parse extracts group ids LENIENTLY (only the
// v1 prefix and the 32-hex shape), because the strict fail-closed parse runs
// at the proxy and a malformed envelope never reaches metering. A header with
// a bad entry still yields the well-formed groups; a non-v1 header yields
// none.
func TestFromRequestMemberGroupIDsLenient(t *testing.T) {
	const gidA = "a1b2c3d4e5f60718293a4b5c6d7e8f90"

	r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	r.Header.Set(HeaderGroupScopes, "v1;NOT-A-GROUP:,,,,"+";"+gidA+":1,,,")
	if id := FromRequest(r); len(id.MemberGroupIDs) != 1 || id.MemberGroupIDs[0] != gidA {
		t.Fatalf("MemberGroupIDs = %v, want only the well-formed group %s", id.MemberGroupIDs, gidA)
	}

	r = httptest.NewRequest("POST", "/v1/chat/completions", nil)
	r.Header.Set(HeaderGroupScopes, "v9;"+gidA+":,,,,")
	if id := FromRequest(r); id.MemberGroupIDs != nil {
		t.Fatalf("MemberGroupIDs = %v for a non-v1 header, want nil", id.MemberGroupIDs)
	}
}

// TestFromRequestGroupScopesUntrustedHeaderAbsent pins the R3 gate for the new
// envelope: when the header is NOT in the active trusted set, it reads as
// ABSENT — no group list, no raw value, no matter what the client sends.
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
	if id.GroupScopes != "" || id.MemberGroupIDs != nil {
		t.Fatalf("untrusted group scopes read as present: GroupScopes=%q MemberGroupIDs=%v, want both absent", id.GroupScopes, id.MemberGroupIDs)
	}
}
