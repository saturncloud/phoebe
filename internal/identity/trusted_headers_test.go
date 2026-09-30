package identity

import (
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/saturncloud/phoebe/internal/logging"
)

// withTrustedHeadersEnv sets (or unsets) PHOEBE_TRUSTED_HEADERS, reloads the
// registry, and registers a cleanup that restores the pinned fallback — the
// active set is package-global, and no test may leak its configuration into
// another test (the existing FromRequest tests assume the full pinned set).
func withTrustedHeadersEnv(t *testing.T, value string, set bool) {
	t.Helper()
	old, hadOld := os.LookupEnv(TrustedHeadersEnv)
	if set {
		os.Setenv(TrustedHeadersEnv, value)
	} else {
		os.Unsetenv(TrustedHeadersEnv)
	}
	LoadTrustedHeaders(logging.New(logging.INFO))
	t.Cleanup(func() {
		if hadOld {
			os.Setenv(TrustedHeadersEnv, old)
		} else {
			os.Unsetenv(TrustedHeadersEnv)
		}
		LoadTrustedHeaders(logging.New(logging.INFO))
	})
}

func pinnedSet() map[string]struct{} {
	set := make(map[string]struct{}, len(pinnedTrustedHeaders))
	for _, name := range pinnedTrustedHeaders {
		set[name] = struct{}{}
	}
	return set
}

// TestPinnedFallbackIsExactlyTheRuling18 pins the fallback to the exact
// pinned names, in the exact pinned order — the set the chart's ConfigMap
// is supposed to render.
func TestPinnedFallbackIsExactlyTheRuling18(t *testing.T) {
	want := []string{
		"X-Saturn-Gateway",
		"X-Saturn-Org-Id",
		"X-Saturn-Owner-Id",
		"X-Saturn-Serving-Mode",
		"X-Saturn-Served-Model",
		"X-Saturn-Service-Tier",
		"X-Saturn-Rate-Limit-Requests",
		"X-Saturn-Rate-Limit-Total-Prompt-Tokens",
		"X-Saturn-Rate-Limit-Uncached-Prompt-Tokens",
		"X-Saturn-Rate-Limit-Generated-Tokens",
		"X-Saturn-Org-Rate-Limit-Requests",
		"X-Saturn-Org-Rate-Limit-Total-Prompt-Tokens",
		"X-Saturn-Org-Rate-Limit-Uncached-Prompt-Tokens",
		"X-Saturn-Org-Rate-Limit-Generated-Tokens",
		"X-Saturn-Owner-Rate-Limit-Requests",
		"X-Saturn-Owner-Rate-Limit-Total-Prompt-Tokens",
		"X-Saturn-Owner-Rate-Limit-Uncached-Prompt-Tokens",
		"X-Saturn-Owner-Rate-Limit-Generated-Tokens",
	}
	if !reflect.DeepEqual(pinnedTrustedHeaders, want) {
		t.Fatalf("pinnedTrustedHeaders = %v, want the ruling's 18 in order %v", pinnedTrustedHeaders, want)
	}
}

// TestLoadTrustedHeadersFallback: unset, empty, whitespace-only, or
// all-empty-after-split config engages the pinned 18 — never an empty set.
func TestLoadTrustedHeadersFallback(t *testing.T) {
	cases := []struct {
		name  string
		value string
		set   bool
	}{
		{"unset", "", false},
		{"empty", "", true},
		{"whitespace only", "   ", true},
		{"only commas", ",,,", true},
		{"spaces and commas", " , , ", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withTrustedHeadersEnv(t, tc.value, tc.set)
			if got := ActiveTrustedHeaders(); !reflect.DeepEqual(got, pinnedSet()) {
				t.Fatalf("active set = %v, want the pinned 18 %v", got, pinnedSet())
			}
		})
	}
}

// TestLoadTrustedHeadersConfigured: a well-formed env value becomes the
// active set exactly (trimmed, empties dropped, case-insensitive).
func TestLoadTrustedHeadersConfigured(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  map[string]struct{}
	}{
		{
			"subset",
			"X-Saturn-Gateway, X-Saturn-Org-Id",
			map[string]struct{}{"X-Saturn-Gateway": {}, "X-Saturn-Org-Id": {}},
		},
		{
			"single header",
			"X-Saturn-Served-Model",
			map[string]struct{}{"X-Saturn-Served-Model": {}},
		},
		{
			"case insensitive per HTTP convention",
			"x-saturn-gateway, X-SATURN-ORG-ID",
			map[string]struct{}{"X-Saturn-Gateway": {}, "X-Saturn-Org-Id": {}},
		},
		{
			"trims spaces and drops empties",
			"  X-Saturn-Owner-Id ,, X-Saturn-Service-Tier , ",
			map[string]struct{}{"X-Saturn-Owner-Id": {}, "X-Saturn-Service-Tier": {}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withTrustedHeadersEnv(t, tc.value, true)
			if got := ActiveTrustedHeaders(); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("active set = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestFromRequestIgnoresHeadersOutsideActiveSet is the parser negative on
// the real parse path: with the active set configured to a subset, a
// stamped envelope header outside that subset is ABSENT — every gated field
// the trust path consumes comes back empty, so the value can influence no
// trust decision.
func TestFromRequestIgnoresHeadersOutsideActiveSet(t *testing.T) {
	withTrustedHeadersEnv(t, "X-Saturn-Gateway", true)

	r := httptest.NewRequest(http.MethodPost, "/", nil)
	// Stamp every envelope header; only X-Saturn-Gateway is trusted (and its
	// mark value must be exactly "true").
	for _, name := range pinnedTrustedHeaders {
		if name == HeaderGateway {
			r.Header.Set(name, "true")
		} else {
			r.Header.Set(name, "stamped")
		}
	}

	id := FromRequest(r)
	if !id.Gateway {
		t.Errorf("Gateway = false, want true (X-Saturn-Gateway is in the active set)")
	}
	if id.OrgID != "" || id.OwnerID != "" || id.ServingMode != "" || id.ServedModel != "" ||
		id.OrgRateLimitRequests != "" || id.OrgRateLimitTotalPromptTokens != "" ||
		id.OrgRateLimitUncachedPromptTokens != "" || id.OrgRateLimitGeneratedTokens != "" ||
		id.OwnerRateLimitRequests != "" || id.OwnerRateLimitTotalPromptTokens != "" ||
		id.OwnerRateLimitUncachedPromptTokens != "" || id.OwnerRateLimitGeneratedTokens != "" ||
		id.LegacyServiceTier != "" || id.LegacyRateLimitRequests != "" ||
		id.LegacyRateLimitTotalPromptTokens != "" || id.LegacyRateLimitUncachedPromptTokens != "" ||
		id.LegacyRateLimitGeneratedTokens != "" {
		t.Errorf("envelope fields outside the active set must read absent, got %+v", id)
	}
}

// TestFromRequestFallbackTrustsPinnedHeaders is the no-regression pin: with
// the fallback engaged (unset env), all 18 remain trusted on the real parse
// path — request handling is unchanged from before the gate existed.
func TestFromRequestFallbackTrustsPinnedHeaders(t *testing.T) {
	withTrustedHeadersEnv(t, "", false)

	r := httptest.NewRequest(http.MethodPost, "/", nil)
	for _, name := range pinnedTrustedHeaders {
		if name == HeaderGateway {
			r.Header.Set(name, "true")
		} else {
			r.Header.Set(name, "v:"+name)
		}
	}

	id := FromRequest(r)
	if !id.Gateway {
		t.Errorf("Gateway = false, want true")
	}
	checks := map[string]struct {
		got  string
		name string
	}{
		"OrgId":          {id.OrgID, "X-Saturn-Org-Id"},
		"OwnerId":        {id.OwnerID, "X-Saturn-Owner-Id"},
		"ServingMode":    {id.ServingMode, "X-Saturn-Serving-Mode"},
		"ServedModel":    {id.ServedModel, "X-Saturn-Served-Model"},
		"LegacyTier":     {id.LegacyServiceTier, "X-Saturn-Service-Tier"},
		"LegacyReq":      {id.LegacyRateLimitRequests, "X-Saturn-Rate-Limit-Requests"},
		"LegacyTotal":    {id.LegacyRateLimitTotalPromptTokens, "X-Saturn-Rate-Limit-Total-Prompt-Tokens"},
		"LegacyUncached": {id.LegacyRateLimitUncachedPromptTokens, "X-Saturn-Rate-Limit-Uncached-Prompt-Tokens"},
		"LegacyGen":      {id.LegacyRateLimitGeneratedTokens, "X-Saturn-Rate-Limit-Generated-Tokens"},
		"OrgReq":         {id.OrgRateLimitRequests, "X-Saturn-Org-Rate-Limit-Requests"},
		"OrgTotal":       {id.OrgRateLimitTotalPromptTokens, "X-Saturn-Org-Rate-Limit-Total-Prompt-Tokens"},
		"OrgUncached":    {id.OrgRateLimitUncachedPromptTokens, "X-Saturn-Org-Rate-Limit-Uncached-Prompt-Tokens"},
		"OrgGen":         {id.OrgRateLimitGeneratedTokens, "X-Saturn-Org-Rate-Limit-Generated-Tokens"},
		"OwnerReq":       {id.OwnerRateLimitRequests, "X-Saturn-Owner-Rate-Limit-Requests"},
		"OwnerTotal":     {id.OwnerRateLimitTotalPromptTokens, "X-Saturn-Owner-Rate-Limit-Total-Prompt-Tokens"},
		"OwnerUncached":  {id.OwnerRateLimitUncachedPromptTokens, "X-Saturn-Owner-Rate-Limit-Uncached-Prompt-Tokens"},
		"OwnerGen":       {id.OwnerRateLimitGeneratedTokens, "X-Saturn-Owner-Rate-Limit-Generated-Tokens"},
	}
	for field, check := range checks {
		if want := "v:" + check.name; check.got != want {
			t.Errorf("%s = %q, want %q (header must remain trusted under the fallback)", field, check.got, want)
		}
	}
}

// TestFromRequestEnvelopeHeadersAreCaseInsensitiveOnRead: HTTP header reads
// are case-insensitive, and a configured entry in any letter-case trusts
// the header.
func TestFromRequestEnvelopeHeadersAreCaseInsensitiveOnRead(t *testing.T) {
	withTrustedHeadersEnv(t, strings.ToLower(HeaderOrgID), true)

	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set(HeaderOrgID, "org-9")
	if got := FromRequest(r).OrgID; got != "org-9" {
		t.Errorf("OrgID = %q, want org-9 (configured entry %q must match case-insensitively)", got, strings.ToLower(HeaderOrgID))
	}
}
