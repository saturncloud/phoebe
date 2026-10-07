package identity

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
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

// TestPinnedFallbackIsExactlyThe14 pins the fallback to the exact
// pinned names, in the exact pinned order — the set the chart's ConfigMap
// is supposed to render. Ruling R8 removed the five legacy single-scope
// quota headers (X-Saturn-Service-Tier, X-Saturn-Rate-Limit-*), taking the
// R3 set from 18 to 13; the membership-aware group scope envelope
// (X-Saturn-Group-Scopes, ruled 2026-10-07) takes it from 13 to 14.
func TestPinnedFallbackIsExactlyThe14(t *testing.T) {
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
		"X-Saturn-Group-Scopes",
	}
	if !reflect.DeepEqual(pinnedTrustedHeaders, want) {
		t.Fatalf("pinnedTrustedHeaders = %v, want the pinned 14 in order %v", pinnedTrustedHeaders, want)
	}
}

// TestLoadTrustedHeadersFallback: unset, empty, whitespace-only, or
// all-empty-after-split config engages the pinned 14 — never an empty set.
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
				t.Fatalf("active set = %v, want the pinned 14 %v", got, pinnedSet())
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

// TestLoadTrustedHeadersNoErrorForPinnedList: the full pinned 14, configured
// explicitly, satisfies every required header and logs nothing at ERROR.
func TestLoadTrustedHeadersNoErrorForPinnedList(t *testing.T) {
	out := loadTrustedHeadersCapturingErrors(t, strings.Join(pinnedTrustedHeaders, ","))
	if out != "" {
		t.Fatalf("unexpected error log for the pinned list: %q", out)
	}
}

// TestStripSaturnHeaders (ruling Q-R8STRIP2): every X-Saturn-* header is
// removed, in any case spelling, with '_' in place of any '-' in the prefix
// (X_Saturn_Owner_Id, which a WSGI/CGI-style upstream folds into
// X-Saturn-Owner-Id), and with all of its values, whether it is in
// the trusted set, an edge-contract identity header, retired, or unknown.
// Non-Saturn headers and look-alike names outside the "X-Saturn-" namespace
// are untouched.
func TestStripSaturnHeaders(t *testing.T) {
	withTrustedHeadersEnv(t, "", false)
	cases := []struct {
		name string
		key  string // written raw into the map, bypassing canonicalization
		keep bool
	}{
		{"retired legacy service tier", "X-Saturn-Service-Tier", false},
		{"retired legacy requests", "X-Saturn-Rate-Limit-Requests", false},
		{"unknown", "X-Saturn-Foo", false},
		{"unknown lowercase", "x-saturn-foo-lower", false},
		{"retired legacy uppercase", "X-SATURN-SERVICE-TIER", false},
		{"mixed case", "x-SaTuRn-Mixed-Case", false},
		{"trusted gateway", HeaderGateway, false},
		{"trusted org", HeaderOrgID, false},
		{"trusted owner limit", HeaderOwnerRateLimitGeneratedTokens, false},
		{"trusted lowercase", "x-saturn-serving-mode", false},
		{"edge contract auth", HeaderAuthID, false},
		{"edge contract upstream", HeaderUpstream, false},
		{"edge contract lowercase", "x-saturn-resource-id", false},
		{"underscore owner", "X_Saturn_Owner_Id", false},
		{"underscore lowercase user", "x_saturn_user_id", false},
		{"underscore resource", "X_Saturn_Resource_Id", false},
		{"underscore upstream lowercase", "x_saturn_upstream", false},
		{"mixed dash and underscore", "X-Saturn_Mixed", false},
		{"mixed underscore and dash", "X_Saturn-Org-Id", false},
		{"non-saturn", "X-Request-Id", true},
		{"non-saturn tenant", "X-Tenant-ID", true},
		{"prefix only, no namespace dash", "X-Saturn", true},
		{"look-alike X-Saturnine", "X-Saturnine", true},
		{"look-alike X-SaturnX", "X-SaturnX", true},
		{"look-alike namespace", "X-Saturnalia-Foo", true},
		{"underscore look-alike X_Saturnine", "X_Saturnine", true},
		{"underscore look-alike X_SaturnX", "X_SaturnX", true},
		{"underscore too short", "X_Sat", true},
		{"underscore prefix only", "X_Saturn", true},
		{"saturn not at the start", "X-Not-Saturn-Foo", true},
	}
	h := http.Header{}
	for _, tc := range cases {
		h[tc.key] = []string{"v1", "v2"}
	}
	wantRemoved := 0
	for _, tc := range cases {
		if !tc.keep {
			wantRemoved++
		}
	}
	if got := StripSaturnHeaders(h); got != wantRemoved {
		t.Fatalf("removed %d header names, want %d", got, wantRemoved)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, present := h[tc.key]
			if present != tc.keep {
				t.Fatalf("%q present=%v after strip, want %v", tc.key, present, tc.keep)
			}
			if tc.keep && len(h[tc.key]) != 2 {
				t.Fatalf("%q kept %d values, want both", tc.key, len(h[tc.key]))
			}
		})
	}
	if StripSaturnHeaders(nil) != 0 {
		t.Fatal("nil header map must strip nothing")
	}
}

// TestStripSaturnHeadersIgnoresTrustedSet: the strip does not consult
// PHOEBE_TRUSTED_HEADERS. A configured trusted name is still removed from the
// forwarded headers; trust governs only what FromRequest reads.
func TestStripSaturnHeadersIgnoresTrustedSet(t *testing.T) {
	withTrustedHeadersEnv(t, HeaderGateway+","+HeaderServingMode, true)
	h := http.Header{}
	h.Set(HeaderGateway, "true")
	h.Set(HeaderServingMode, "shared")
	h.Set(HeaderOrgID, "org-1")
	h.Set(HeaderAuthID, "auth-1")
	if got := StripSaturnHeaders(h); got != 4 {
		t.Fatalf("removed %d, want 4", got)
	}
	if len(h) != 0 {
		t.Fatalf("headers left after strip: %v", h)
	}
}

// candidateSaturnHeaders is every exported X-Saturn-* Header* constant the
// identity package declares. TestCandidateSaturnHeadersCoverPackageConstants
// fails if a constant is added to the package without being listed here.
var candidateSaturnHeaders = []string{
	HeaderAuthID,
	HeaderUserID,
	HeaderGroupID,
	HeaderResourceID,
	HeaderResourceType,
	HeaderOrgID,
	HeaderBaseModel,
	HeaderAdapter,
	HeaderServingMode,
	HeaderServedModel,
	HeaderUpstream,
	HeaderGateway,
	HeaderOwnerID,
	HeaderOrgRateLimitRequests,
	HeaderOrgRateLimitTotalPromptTokens,
	HeaderOrgRateLimitUncachedPromptTokens,
	HeaderOrgRateLimitGeneratedTokens,
	HeaderOwnerRateLimitRequests,
	HeaderOwnerRateLimitTotalPromptTokens,
	HeaderOwnerRateLimitUncachedPromptTokens,
	HeaderOwnerRateLimitGeneratedTokens,
	HeaderGroupScopes,
}

// packageSaturnHeaderConstants parses the package's non-test Go files and
// returns the value of every const whose string literal starts with
// "X-Saturn-" and names a full header (not the bare prefix).
func packageSaturnHeaderConstants(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	var out []string
	for _, pkg := range pkgs {
		for _, f := range pkg.Files {
			for _, decl := range f.Decls {
				gd, ok := decl.(*ast.GenDecl)
				if !ok || gd.Tok != token.CONST {
					continue
				}
				for _, spec := range gd.Specs {
					vs := spec.(*ast.ValueSpec)
					for _, v := range vs.Values {
						lit, ok := v.(*ast.BasicLit)
						if !ok || lit.Kind != token.STRING {
							continue
						}
						s, err := strconv.Unquote(lit.Value)
						if err != nil {
							continue
						}
						if strings.HasPrefix(s, saturnHeaderPrefix) && len(s) > len(saturnHeaderPrefix) {
							out = append(out, s)
						}
					}
				}
			}
		}
	}
	return out
}

func TestCandidateSaturnHeadersCoverPackageConstants(t *testing.T) {
	listed := make(map[string]bool, len(candidateSaturnHeaders))
	for _, name := range candidateSaturnHeaders {
		listed[name] = true
	}
	consts := packageSaturnHeaderConstants(t)
	if len(consts) == 0 {
		t.Fatal("found no X-Saturn-* constants; the parse is broken")
	}
	for _, name := range consts {
		if !listed[name] {
			t.Errorf("X-Saturn-* constant %q is not in candidateSaturnHeaders; add it", name)
		}
	}
}

// TestStripSaturnHeadersRemovesEveryPackageHeader: every X-Saturn-* header the
// identity package declares, including each one FromRequest reads, is removed
// by the strip. Parsing the identity first is what keeps those values usable:
// the Identity parsed before the strip is unchanged by it.
func TestStripSaturnHeadersRemovesEveryPackageHeader(t *testing.T) {
	withTrustedHeadersEnv(t, "", false)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	for i, name := range candidateSaturnHeaders {
		value := "probe-value-" + strconv.Itoa(i)
		if name == HeaderGateway {
			value = "true"
		}
		req.Header.Set(name, value)
	}
	before := FromRequest(req)
	if got := StripSaturnHeaders(req.Header); got != len(candidateSaturnHeaders) {
		t.Fatalf("removed %d header names, want all %d", got, len(candidateSaturnHeaders))
	}
	for _, name := range candidateSaturnHeaders {
		if v := req.Header.Values(name); len(v) > 0 {
			t.Errorf("%s survived the strip: %v", name, v)
		}
	}
	if before.ResourceID == "" || before.Upstream == "" || !before.Gateway || before.OwnerID == "" {
		t.Fatalf("identity parsed before the strip is incomplete: %+v", before)
	}
	if empty := FromRequest(httptest.NewRequest(http.MethodGet, "/", nil)); !reflect.DeepEqual(FromRequest(req), empty) {
		t.Fatalf("a re-parse after the strip still sees X-Saturn-* values: %+v", FromRequest(req))
	}
}

// TestEveryHeaderConstantIsInTheSaturnNamespace: every Header* constant the
// identity package declares (the names FromRequest reads) lives in the
// X-Saturn-* namespace, so StripSaturnHeaders removes it before forwarding. A
// new Header* constant with a name outside the namespace would be read by
// FromRequest yet forwarded to the upstream; this test fails loudly on it.
func TestEveryHeaderConstantIsInTheSaturnNamespace(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	found := 0
	for _, pkg := range pkgs {
		for _, f := range pkg.Files {
			for _, decl := range f.Decls {
				gd, ok := decl.(*ast.GenDecl)
				if !ok || gd.Tok != token.CONST {
					continue
				}
				for _, spec := range gd.Specs {
					vs := spec.(*ast.ValueSpec)
					for i, ident := range vs.Names {
						if !strings.HasPrefix(ident.Name, "Header") || i >= len(vs.Values) {
							continue
						}
						lit, ok := vs.Values[i].(*ast.BasicLit)
						if !ok || lit.Kind != token.STRING {
							t.Errorf("Header constant %s is not a string literal; check it by hand", ident.Name)
							continue
						}
						value, err := strconv.Unquote(lit.Value)
						if err != nil {
							t.Fatalf("unquote %s: %v", ident.Name, err)
						}
						found++
						if !isSaturnHeader(value) || len(value) <= len(saturnHeaderPrefix) {
							t.Errorf("Header constant %s = %q is outside the X-Saturn-* namespace; StripSaturnHeaders would forward it upstream", ident.Name, value)
						}
					}
				}
			}
		}
	}
	if found != len(candidateSaturnHeaders) {
		t.Fatalf("found %d Header* constants, candidateSaturnHeaders lists %d; keep them in step", found, len(candidateSaturnHeaders))
	}
}

// TestCountUnexpectedSaturnHeadersCountsOnlyNamesTheEdgeShouldHaveStripped:
// every X-Saturn-* header the identity package declares is either trusted or
// an edge-contract identity header, so a request carrying all of them counts
// zero. Retired, unknown, and underscore-spelled names each count once, and
// look-alikes outside the namespace do not count. The count never modifies the
// headers.
func TestCountUnexpectedSaturnHeadersCountsOnlyNamesTheEdgeShouldHaveStripped(t *testing.T) {
	withTrustedHeadersEnv(t, "", false)
	h := http.Header{}
	for _, name := range candidateSaturnHeaders {
		h.Set(name, "v")
	}
	h["x-saturn-upstream"] = []string{"v"} // non-canonical spelling of an edge header
	h.Set("X-Saturnine", "v")
	if got := CountUnexpectedSaturnHeaders(h); got != 0 {
		t.Fatalf("count = %d for trusted and edge-contract headers only, want 0", got)
	}
	h.Set("X-Saturn-Service-Tier", "v")
	h.Set("X-Saturn-Foo", "v")
	h["X_Saturn_Owner_Id"] = []string{"v"}
	before := len(h)
	if got := CountUnexpectedSaturnHeaders(h); got != 3 {
		t.Fatalf("count = %d, want 3 (retired, unknown, underscore spelling)", got)
	}
	if len(h) != before {
		t.Fatalf("CountUnexpectedSaturnHeaders modified the headers: %d names, want %d", len(h), before)
	}
}
