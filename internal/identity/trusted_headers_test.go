package identity

import (
	"bytes"
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

// TestPinnedFallbackIsExactlyThe13 pins the fallback to the exact
// pinned names, in the exact pinned order — the set the chart's ConfigMap
// is supposed to render. Ruling R8 removed the five legacy single-scope
// quota headers (X-Saturn-Service-Tier, X-Saturn-Rate-Limit-*), taking the
// R3 set from 18 to 13.
func TestPinnedFallbackIsExactlyThe13(t *testing.T) {
	want := []string{
		"X-Saturn-Gateway",
		"X-Saturn-Org-Id",
		"X-Saturn-Owner-Id",
		"X-Saturn-Serving-Mode",
		"X-Saturn-Served-Model",
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
		t.Fatalf("pinnedTrustedHeaders = %v, want the pinned 13 in order %v", pinnedTrustedHeaders, want)
	}
}

// TestLoadTrustedHeadersFallback: unset, empty, whitespace-only, or
// all-empty-after-split config engages the pinned 13 — never an empty set.
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
				t.Fatalf("active set = %v, want the pinned 13 %v", got, pinnedSet())
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
			"  X-Saturn-Owner-Id ,, X-Saturn-Serving-Mode , ",
			map[string]struct{}{"X-Saturn-Owner-Id": {}, "X-Saturn-Serving-Mode": {}},
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
		id.OwnerRateLimitUncachedPromptTokens != "" || id.OwnerRateLimitGeneratedTokens != "" {
		t.Errorf("envelope fields outside the active set must read absent, got %+v", id)
	}
}

// TestFromRequestFallbackTrustsPinnedHeaders is the no-regression pin for the
// gate: with the fallback engaged (unset env), all 13 pinned envelope headers
// are trusted on the real parse path, so a missing or misrendered ConfigMap
// does not change request handling relative to the chart's intended set, and
// the gate's fallback reads exactly what the ungated parser reads. Note:
// ruling R8 removed the five legacy single-scope quota headers
// (X-Saturn-Service-Tier, X-Saturn-Rate-Limit-*) from the parser entirely,
// so pre-R8 phoebe read 18 headers; see TestPinnedFallbackIsExactlyThe13.
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
		"OrgId":         {id.OrgID, "X-Saturn-Org-Id"},
		"OwnerId":       {id.OwnerID, "X-Saturn-Owner-Id"},
		"ServingMode":   {id.ServingMode, "X-Saturn-Serving-Mode"},
		"ServedModel":   {id.ServedModel, "X-Saturn-Served-Model"},
		"OrgReq":        {id.OrgRateLimitRequests, "X-Saturn-Org-Rate-Limit-Requests"},
		"OrgTotal":      {id.OrgRateLimitTotalPromptTokens, "X-Saturn-Org-Rate-Limit-Total-Prompt-Tokens"},
		"OrgUncached":   {id.OrgRateLimitUncachedPromptTokens, "X-Saturn-Org-Rate-Limit-Uncached-Prompt-Tokens"},
		"OrgGen":        {id.OrgRateLimitGeneratedTokens, "X-Saturn-Org-Rate-Limit-Generated-Tokens"},
		"OwnerReq":      {id.OwnerRateLimitRequests, "X-Saturn-Owner-Rate-Limit-Requests"},
		"OwnerTotal":    {id.OwnerRateLimitTotalPromptTokens, "X-Saturn-Owner-Rate-Limit-Total-Prompt-Tokens"},
		"OwnerUncached": {id.OwnerRateLimitUncachedPromptTokens, "X-Saturn-Owner-Rate-Limit-Uncached-Prompt-Tokens"},
		"OwnerGen":      {id.OwnerRateLimitGeneratedTokens, "X-Saturn-Owner-Rate-Limit-Generated-Tokens"},
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

// loadTrustedHeadersCapturingErrors sets PHOEBE_TRUSTED_HEADERS to value,
// loads the registry with a logger whose ERROR output goes to a buffer, and
// returns what was logged at ERROR. withTrustedHeadersEnv restores the
// previous env and reloads the pinned set on cleanup.
func loadTrustedHeadersCapturingErrors(t *testing.T, value string) string {
	t.Helper()
	withTrustedHeadersEnv(t, value, true)
	var buf bytes.Buffer
	log := logging.New(logging.INFO)
	log.Error.SetOutput(&buf)
	LoadTrustedHeaders(log)
	return buf.String()
}

// TestLoadTrustedHeadersErrorsWhenServingModeMissing: a configured list that
// omits X-Saturn-Serving-Mode is installed as written (the header is NOT
// trusted, so a client cannot set its own serving mode) and an ERROR line at
// load time names the header, the env var, and the 404 consequence.
func TestLoadTrustedHeadersErrorsWhenServingModeMissing(t *testing.T) {
	out := loadTrustedHeadersCapturingErrors(t, "X-Saturn-Gateway,X-Saturn-Org-Id")
	if _, ok := ActiveTrustedHeaders()[HeaderServingMode]; ok {
		t.Fatalf("active set trusts %s although the configured list omits it", HeaderServingMode)
	}
	for _, want := range []string{HeaderServingMode, TrustedHeadersEnv, "404"} {
		if !strings.Contains(out, want) {
			t.Fatalf("error log %q does not mention %q", out, want)
		}
	}
	if strings.Contains(out, "omits "+HeaderGateway) {
		t.Fatalf("error log %q flags %s although the list includes it", out, HeaderGateway)
	}
}

// TestLoadTrustedHeadersErrorsWhenGatewayMissing: a configured list that
// omits X-Saturn-Gateway logs an ERROR naming it.
func TestLoadTrustedHeadersErrorsWhenGatewayMissing(t *testing.T) {
	out := loadTrustedHeadersCapturingErrors(t, "X-Saturn-Serving-Mode,X-Saturn-Org-Id")
	if !strings.Contains(out, "omits "+HeaderGateway) || !strings.Contains(out, "gateway requests will not be recognized") {
		t.Fatalf("error log %q does not flag the missing %s", out, HeaderGateway)
	}
	if strings.Contains(out, "omits "+HeaderServingMode) {
		t.Fatalf("error log %q flags %s although the list includes it", out, HeaderServingMode)
	}
}

// TestLoadTrustedHeadersNoErrorWhenRequiredHeadersPresent: a configured list
// containing every required header (in any case) logs nothing at ERROR.
func TestLoadTrustedHeadersNoErrorWhenRequiredHeadersPresent(t *testing.T) {
	out := loadTrustedHeadersCapturingErrors(t, "x-saturn-gateway, X-SATURN-SERVING-MODE, X-Saturn-Org-Id, x-saturn-owner-id")
	if out != "" {
		t.Fatalf("unexpected error log for a list with every required header: %q", out)
	}
}

// TestLoadTrustedHeadersErrorsWhenOwnerOrOrgAnchorMissing: after R8,
// X-Saturn-Owner-Id is the only anchor for the quota policy and admission
// also requires X-Saturn-Org-Id, so a configured list that omits either
// would 503 every admitted shared request. Each omission logs an ERROR line
// naming the header and the 503 consequence, and the list is still installed
// exactly as written (neither header becomes trusted).
func TestLoadTrustedHeadersErrorsWhenOwnerOrOrgAnchorMissing(t *testing.T) {
	out := loadTrustedHeadersCapturingErrors(t, "X-Saturn-Gateway,X-Saturn-Serving-Mode")
	for _, missing := range []string{HeaderOwnerID, HeaderOrgID} {
		if !strings.Contains(out, "omits "+missing) {
			t.Fatalf("error log %q does not flag the missing %s", out, missing)
		}
		if _, ok := ActiveTrustedHeaders()[missing]; ok {
			t.Fatalf("active set trusts %s although the configured list omits it", missing)
		}
	}
	if !strings.Contains(out, "503") || !strings.Contains(out, "owner-id anchor") {
		t.Fatalf("error log %q does not state the 503 owner-anchor consequence", out)
	}
	if got := len(ActiveTrustedHeaders()); got != 2 {
		t.Fatalf("active set has %d headers, want exactly the 2 configured", got)
	}
}

// TestLoadTrustedHeadersNoErrorForPinnedList: the full pinned 13, configured
// explicitly, satisfies every required header and logs nothing at ERROR.
func TestLoadTrustedHeadersNoErrorForPinnedList(t *testing.T) {
	out := loadTrustedHeadersCapturingErrors(t, strings.Join(pinnedTrustedHeaders, ","))
	if out != "" {
		t.Fatalf("unexpected error log for the pinned list: %q", out)
	}
}
