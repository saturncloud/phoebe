package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

// These tests cover the membership-aware group quota envelope
// (X-Saturn-Group-Scopes, ruled 2026-10-07) at the proxy layer: the strict
// parse (fail-visible: malformed/oversize → 503) and the end-to-end
// enforcement of both scope classes (per-minute rates and the monthly spend
// cap, both answering 429 + Retry-After on denial).

const (
	testGroupA = "a1b2c3d4e5f60718293a4b5c6d7e8f90"
	testGroupB = "00112233445566778899aabbccddeeff"

	// groupScopesHeader is a fully-unlimited two-group envelope: every rate
	// empty (unlimited), no spend cap. Tests override individual fields.
	groupScopesHeader = "v1;" + testGroupA + ":,,,," + ";" + testGroupB + ":,,,,"
)

// groupScopesValue builds an envelope for one group with the given rate field
// string (verbatim between the colons) and spend field.
func groupScopesValue(gid, rates, spend string) string {
	return "v1;" + gid + ":" + rates + "," + spend
}

// TestParseTrustedGroupScopes exercises the frozen envelope grammar:
// v1;<gid>:<req>,<tot>,<unc>,<gen>,<spend>;<gid>:...
func TestParseTrustedGroupScopes(t *testing.T) {
	valid := []struct {
		name string
		in   string
		want []admission.GroupScope
	}{
		{
			"absent header",
			"",
			nil,
		},
		{
			"v1 alone is zero groups",
			"v1",
			nil,
		},
		{
			"single group all unlimited no spend cap",
			groupScopesValue(testGroupA, ",,,", ""),
			[]admission.GroupScope{{GroupID: testGroupA}},
		},
		{
			"all four rates set",
			groupScopesValue(testGroupA, "30,1000,500,2000", ""),
			[]admission.GroupScope{{
				GroupID: testGroupA,
				Limits: admission.RateLimits{
					Requests: ptr64(30), TotalPromptTokens: ptr64(1000),
					UncachedPromptTokens: ptr64(500), GeneratedTokens: ptr64(2000),
				},
			}},
		},
		{
			"spend cap decimal",
			groupScopesValue(testGroupA, ",,,", "100.000000001"),
			[]admission.GroupScope{{GroupID: testGroupA, SpendCap: "100.000000001"}},
		},
		{
			"spend cap 11-digit integer",
			groupScopesValue(testGroupA, ",,,", "99999999999"),
			[]admission.GroupScope{{GroupID: testGroupA, SpendCap: "99999999999"}},
		},
		{
			"spend cap at the numeric 20 9 bound",
			groupScopesValue(testGroupA, ",,,", "99999999999.999999999"),
			[]admission.GroupScope{{GroupID: testGroupA, SpendCap: "99999999999.999999999"}},
		},
		{
			"spend cap zero is a zero cap not absent",
			groupScopesValue(testGroupA, ",,,", "0"),
			[]admission.GroupScope{{GroupID: testGroupA, SpendCap: "0"}},
		},
		{
			"rate zero is a zero cap",
			groupScopesValue(testGroupA, "0,,,", ""),
			[]admission.GroupScope{{
				GroupID: testGroupA,
				Limits:  admission.RateLimits{Requests: ptr64(0)},
			}},
		},
		{
			"multi group in envelope order",
			"v1;" + testGroupA + ":1,,,," + ";" + testGroupB + ":,2,,,100",
			[]admission.GroupScope{
				{GroupID: testGroupA, Limits: admission.RateLimits{Requests: ptr64(1)}},
				{GroupID: testGroupB, Limits: admission.RateLimits{TotalPromptTokens: ptr64(2)}, SpendCap: "100"},
			},
		},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			scopes, err := parseTrustedGroupScopes(identity.Identity{GroupScopes: tc.in})
			if err != nil {
				t.Fatalf("parse(%q): %v, want success", tc.in, err)
			}
			if len(scopes) != len(tc.want) {
				t.Fatalf("parse(%q) = %d scopes, want %d (%+v)", tc.in, len(scopes), len(tc.want), scopes)
			}
			for i, want := range tc.want {
				got := scopes[i]
				if got.GroupID != want.GroupID || got.SpendCap != want.SpendCap {
					t.Fatalf("scope %d = %+v, want %+v", i, got, want)
				}
				if (got.Limits.Requests == nil) != (want.Limits.Requests == nil) ||
					(got.Limits.Requests != nil && *got.Limits.Requests != *want.Limits.Requests) {
					t.Fatalf("scope %d requests = %v, want %v", i, got.Limits.Requests, want.Limits.Requests)
				}
				if (got.Limits.TotalPromptTokens == nil) != (want.Limits.TotalPromptTokens == nil) ||
					(got.Limits.TotalPromptTokens != nil && *got.Limits.TotalPromptTokens != *want.Limits.TotalPromptTokens) {
					t.Fatalf("scope %d total = %v, want %v", i, got.Limits.TotalPromptTokens, want.Limits.TotalPromptTokens)
				}
				if (got.Limits.UncachedPromptTokens == nil) != (want.Limits.UncachedPromptTokens == nil) ||
					(got.Limits.UncachedPromptTokens != nil && *got.Limits.UncachedPromptTokens != *want.Limits.UncachedPromptTokens) {
					t.Fatalf("scope %d uncached = %v, want %v", i, got.Limits.UncachedPromptTokens, want.Limits.UncachedPromptTokens)
				}
				if (got.Limits.GeneratedTokens == nil) != (want.Limits.GeneratedTokens == nil) ||
					(got.Limits.GeneratedTokens != nil && *got.Limits.GeneratedTokens != *want.Limits.GeneratedTokens) {
					t.Fatalf("scope %d generated = %v, want %v", i, got.Limits.GeneratedTokens, want.Limits.GeneratedTokens)
				}
			}
		})
	}

	malformed := []struct {
		name string
		in   string
	}{
		{"missing v1 prefix", testGroupA + ":,,,,"},
		{"wrong prefix version", "v2;" + testGroupA + ":,,,,"},
		{"empty entry from trailing semicolon", "v1;" + testGroupA + ":,,,,;"},
		{"empty entry in the middle", "v1;;" + testGroupA + ":,,,,"},
		{"gid uppercase hex", "v1;A1B2C3D4E5F60718293A4B5C6D7E8F90:,,,,"},
		{"gid too short", "v1;a1b2:,,,,"},
		{"gid not hex", "v1;g1b2c3d4e5f60718293a4b5c6d7e8f90:,,,,"},
		{"missing colon", "v1;" + testGroupA + ",,,,"},
		{"too few rate fields", groupScopesValue(testGroupA, ",,", "")},
		{"too many rate fields", "v1;" + testGroupA + ":1,2,3,4,5,6"},
		{"negative rate", groupScopesValue(testGroupA, "-1,,,", "")},
		{"non-numeric rate", groupScopesValue(testGroupA, "x,,,", "")},
		{"signed requests rate", groupScopesValue(testGroupA, "+5,,,", "")},
		{"negative zero total rate", groupScopesValue(testGroupA, ",-0,,", "")},
		{"zero-padded uncached rate", groupScopesValue(testGroupA, ",,05,", "")},
		{"signed generated rate", groupScopesValue(testGroupA, ",,,+30", "")},
		{"zero-padded requests rate", groupScopesValue(testGroupA, "0030,,,", "")},
		{"spend cap 12-digit integer", groupScopesValue(testGroupA, ",,,", "100000000000")},
		{"spend cap 12-digit integer with full fraction", groupScopesValue(testGroupA, ",,,", "100000000000.000000000")},
		{"uncached above total", groupScopesValue(testGroupA, ",100,101,", "")},
		{"spend cap exponent", groupScopesValue(testGroupA, ",,,", "1e3")},
		{"spend cap sign", groupScopesValue(testGroupA, ",,,", "+100")},
		{"spend cap bare dot", groupScopesValue(testGroupA, ",,,", ".5")},
		{"spend cap trailing dot", groupScopesValue(testGroupA, ",,,", "5.")},
		{"spend cap too many fraction digits", groupScopesValue(testGroupA, ",,,", "0.0000000001")},
		{"duplicate gid", "v1;" + testGroupA + ":,,,," + ";" + testGroupA + ":1,,,"},
	}
	for _, tc := range malformed {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseTrustedGroupScopes(identity.Identity{GroupScopes: tc.in}); err == nil {
				t.Fatalf("parse(%q) succeeded, want a fail-closed error", tc.in)
			}
		})
	}

	t.Run("over the entry bound", func(t *testing.T) {
		entries := make([]string, 0, maxGroupScopeEntries+1)
		for i := 0; i <= maxGroupScopeEntries; i++ {
			entries = append(entries, groupIDFor(i)+":,,,,")
		}
		if _, err := parseTrustedGroupScopes(identity.Identity{GroupScopes: "v1;" + strings.Join(entries, ";")}); err == nil {
			t.Fatalf("parse of %d entries succeeded, want the %d-entry bound enforced", len(entries), maxGroupScopeEntries)
		}
	})

	t.Run("over the size bound", func(t *testing.T) {
		// 16 maximal entries (32-hex gid + ":,,,,") plus the v1; prefix stays
		// under 4 KiB, so pad one entry's (unused) spend field to cross it.
		entries := make([]string, 0, maxGroupScopeEntries)
		for i := 0; i < maxGroupScopeEntries; i++ {
			entries = append(entries, groupIDFor(i)+":,,,,")
		}
		in := "v1;" + strings.Join(entries, ";")
		in += strings.Repeat("0", maxGroupScopeBytes-len(in))
		if len(in) != maxGroupScopeBytes {
			t.Fatalf("fixture size = %d, want exactly the %d-byte bound", len(in), maxGroupScopeBytes)
		}
		if _, err := parseTrustedGroupScopes(identity.Identity{GroupScopes: in}); err == nil {
			t.Fatalf("parse of a %d-byte header succeeded, want the size bound enforced", len(in))
		}
	})
}

// groupIDFor returns a distinct valid 32-hex group id for i.
func groupIDFor(i int) string {
	s := strconv.FormatInt(int64(i), 16)
	return strings.Repeat("0", 32-len(s)) + s
}

// groupScopesRequest is a shared request carrying the group quota envelope.
func groupScopesRequest(t *testing.T, upstream *url.URL, envelope string) *http.Request {
	t.Helper()
	r := sharedRequest(upstream)
	if envelope != "" {
		r.Header.Set(identity.HeaderGroupScopes, envelope)
	}
	return r
}

// proxyWithGroupScopes builds the admission-enabled proxy over a real
// (frozen-clock) miniredis, with an optional spend store.
func proxyWithGroupScopes(t *testing.T, spend admission.GroupSpendStore) (*Server, *recordingEmitter) {
	t.Helper()
	return proxyWithGroupScopesEnabled(t, true, spend)
}

// proxyWithGroupScopesEnabled builds the proxy over a real (frozen-clock)
// miniredis with the given admission.enabled flag. The admitter runs with the
// effective settings — operator tiers cleared under admission.enabled=false,
// exactly as a default chart install renders them — and an optional spend
// store.
func proxyWithGroupScopesEnabled(t *testing.T, enabled bool, spend admission.GroupSpendStore) (*Server, *recordingEmitter) {
	t.Helper()
	mr := frozenMiniredis(t)
	cfg := proxyAdmissionConfig(10)
	cfg.Enabled = enabled
	cfg.ValkeyAddr = mr.Addr()
	eff, ok := (&config.Settings{Admission: cfg}).EffectiveAdmission()
	if !ok {
		t.Fatal("effective admission settings: no store configured")
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	admitter := admission.New(client, eff)
	if spend != nil {
		admitter.WithGroupSpend(spend, logging.New(logging.ERROR))
	}
	em := &recordingEmitter{}
	s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), em).WithAdmitter(admitter)
	return s, em
}

// TestSharedGroupRateLimitAnswers429: a group requests-per-minute cap on the
// envelope is enforced exactly like the contract scopes — the burst admits,
// the overrun answers 429 + Retry-After (the contractual mapping).
func TestSharedGroupRateLimitAnswers429(t *testing.T) {
	up, hits := sharedGroupBackend(t)
	s, _ := proxyWithGroupScopes(t, nil)

	r1 := groupScopesRequest(t, up, groupScopesValue(testGroupA, "1,,,", ""))
	rr1 := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr1, r1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("first request status=%d, want 200", rr1.Code)
	}

	r2 := groupScopesRequest(t, up, groupScopesValue(testGroupA, "1,,,", ""))
	rr2 := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr2, r2)
	if rr2.Code != http.StatusTooManyRequests {
		t.Fatalf("overrun status=%d, want 429", rr2.Code)
	}
	if rr2.Header().Get("Retry-After") == "" {
		t.Fatal("overrun missing Retry-After")
	}
	if *hits != 1 {
		t.Fatalf("upstream hits=%d, want 1 (the overrun never forwards)", *hits)
	}
}

// TestSharedGroupRateZeroCapAnswers429: an explicit 0 blocks every request
// for the group (R4 zero cap), whatever the other scopes say.
func TestSharedGroupRateZeroCapAnswers429(t *testing.T) {
	up, _ := sharedGroupBackend(t)
	s, _ := proxyWithGroupScopes(t, nil)

	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, groupScopesRequest(t, up, groupScopesValue(testGroupA, "0,,,", "")))
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("zero-cap status=%d, want 429", rr.Code)
	}
}

// staticSpendStore is a GroupSpendStore fake with a fixed verdict.
type staticSpendStore struct{ exhausted bool }

func (s staticSpendStore) GroupSpendExhausted(context.Context, string, string) (bool, error) {
	return s.exhausted, nil
}

// TestSharedGroupSpendCapAnswers429: a group whose month-to-date spend
// reached its cap is refused with 429 + Retry-After — the contractual class,
// never 503 — even though its per-minute rates are unlimited.
func TestSharedGroupSpendCapAnswers429(t *testing.T) {
	up, hits := sharedGroupBackend(t)
	s, _ := proxyWithGroupScopes(t, staticSpendStore{exhausted: true})

	r := groupScopesRequest(t, up, groupScopesValue(testGroupA, ",,,", "100"))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, r)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("capped status=%d, want 429", rr.Code)
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Fatal("capped response missing Retry-After")
	}
	if *hits != 0 {
		t.Fatalf("upstream hits=%d, want 0 (the capped request never forwards)", *hits)
	}
}

// TestSharedGroupSpendStoreErrorFailsOpen: a broken spend store must not deny
// traffic — the request is admitted (fail open) and independently metered.
func TestSharedGroupSpendStoreErrorFailsOpen(t *testing.T) {
	up, hits := sharedGroupBackend(t)
	s, em := proxyWithGroupScopes(t, errSpendStore{})

	r := groupScopesRequest(t, up, groupScopesValue(testGroupA, ",,,", "100"))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, r)
	if rr.Code != http.StatusOK {
		t.Fatalf("store-error status=%d, want 200 (fail open)", rr.Code)
	}
	if *hits != 1 {
		t.Fatalf("upstream hits=%d, want 1", *hits)
	}
	if evs := em.waitForEvents(1, time.Second); len(evs) != 1 {
		t.Fatalf("emitted events=%d, want 1 (metering is independent of admission)", len(evs))
	}
}

type errSpendStore struct{}

func (errSpendStore) GroupSpendExhausted(context.Context, string, string) (bool, error) {
	return false, errors.New("postgres unreachable")
}

// TestSharedGroupScopesMalformedHeaderFailsClosed: a malformed group envelope
// is a broken trusted policy, not a quota — 503 at the proxy, before
// admission, and nothing reaches the upstream or the emitter.
func TestSharedGroupScopesMalformedHeaderFailsClosed(t *testing.T) {
	up, hits := sharedGroupBackend(t)
	s, em := proxyWithGroupScopes(t, nil)

	r := groupScopesRequest(t, up, "v1;NOT-A-GROUP:,,,,")
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, r)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("malformed envelope status=%d, want 503", rr.Code)
	}
	if *hits != 0 {
		t.Fatalf("upstream hits=%d, want 0 (fail closed before forwarding)", *hits)
	}
	if evs := em.waitForEvents(1, 200*time.Millisecond); len(evs) != 0 {
		t.Fatalf("emitted events=%d, want 0 (a refused request meters nothing)", len(evs))
	}
}

// TestSharedGroupScopesUnlimitedHeaderAdmits: the fully-unlimited envelope
// (every field empty) parses to scopes with nothing to enforce — requests
// flow, and the membership list rides the emitted event for the rater.
func TestSharedGroupScopesUnlimitedHeaderAdmits(t *testing.T) {
	up, hits := sharedGroupBackend(t)
	s, em := proxyWithGroupScopes(t, nil)

	r := groupScopesRequest(t, up, groupScopesHeader)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, r)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rr.Code)
	}
	if *hits != 1 {
		t.Fatalf("upstream hits=%d, want 1", *hits)
	}
	evs := em.waitForEvents(1, time.Second)
	if len(evs) != 1 {
		t.Fatalf("emitted events=%d, want 1", len(evs))
	}
	got := evs[0].MemberGroupIDs
	if len(got) != 2 || got[0] != testGroupA || got[1] != testGroupB {
		t.Fatalf("MemberGroupIDs = %v, want [%s %s] (the envelope order)", got, testGroupA, testGroupB)
	}
}

// TestSharedNoGroupHeaderNoMembership: without the envelope the emitted event
// carries no membership (the drainer stores NULL).
func TestSharedNoGroupHeaderNoMembership(t *testing.T) {
	up, _ := sharedGroupBackend(t)
	s, em := proxyWithGroupScopes(t, nil)

	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, groupScopesRequest(t, up, ""))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rr.Code)
	}
	evs := em.waitForEvents(1, time.Second)
	if len(evs) != 1 {
		t.Fatalf("emitted events=%d, want 1", len(evs))
	}
	if len(evs[0].MemberGroupIDs) != 0 {
		t.Fatalf("MemberGroupIDs = %v, want none", evs[0].MemberGroupIDs)
	}
}

// sharedGroupBackend is a shared-path upstream counting its hits.
func sharedGroupBackend(t *testing.T) (*url.URL, *int) {
	t.Helper()
	hits := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		_, _ = w.Write([]byte(`{"model":"model-a","usage":{"prompt_tokens":2,"completion_tokens":3}}`))
	}))
	t.Cleanup(backend.Close)
	up, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	return up, &hits
}

// TestSharedGroupScopesAdmissionTriggerWithAdmissionFlagOff pins the trigger:
// under admission.enabled=false the Admit round-trip is engaged by a scope
// that carries enforcement, not by the mere presence of the group envelope.
// Every org/owner scoped limit header is absent, so only the group scopes can
// engage the trigger. An all-unlimited group envelope performs no admission
// call; an envelope with a rate limit or a spend cap still performs exactly
// one, unchanged from the enabled=true path.
func TestSharedGroupScopesAdmissionTriggerWithAdmissionFlagOff(t *testing.T) {
	up, _ := sharedGroupBackend(t)
	cfg := proxyAdmissionConfig(10)
	cfg.Enabled = false
	send := func(a *countingAdmitter, envelope string) *httptest.ResponseRecorder {
		s := New(&config.Settings{Admission: cfg}, logging.New(logging.ERROR), &recordingEmitter{}).WithAdmitter(a)
		req := groupScopesRequest(t, up, envelope)
		for _, header := range identity.ScopedRateLimitHeaders {
			req.Header.Del(header)
		}
		rr := httptest.NewRecorder()
		s.Handler().ServeHTTP(rr, req)
		return rr
	}

	// The all-unlimited envelope (every rate empty, no spend cap) enforces
	// nothing: no Admit, no Valkey reservation, no completion round-trip.
	a := &countingAdmitter{}
	if rr := send(a, groupScopesHeader); rr.Code != http.StatusOK || a.calls != 0 {
		t.Fatalf("all-unlimited envelope: status=%d Admit calls=%d, want 200 and no admission round-trip", rr.Code, a.calls)
	}

	// A rate-limited group scope engages admission exactly as a contract
	// limit header does.
	a = &countingAdmitter{}
	if rr := send(a, groupScopesValue(testGroupA, "30,,,", "")); rr.Code != http.StatusOK || a.calls != 1 {
		t.Fatalf("rate-limited group scope: status=%d Admit calls=%d, want 200 and exactly one Admit", rr.Code, a.calls)
	}

	// A group carrying only a spend cap engages admission too.
	a = &countingAdmitter{}
	if rr := send(a, groupScopesValue(testGroupA, ",,,", "100")); rr.Code != http.StatusOK || a.calls != 1 {
		t.Fatalf("spend-capped group scope: status=%d Admit calls=%d, want 200 and exactly one Admit", rr.Code, a.calls)
	}
}

// TestSharedGroupScopesEnforcedWithAdmissionFlagOff is the envelope-driven
// enforcement test for the item-13 posture: admission.enabled=false with ONLY
// X-Saturn-Group-Scopes stamped (no org/owner limit headers). A group zero
// cap answers 429 + Retry-After before upstream, and a request within the
// group limits is admitted — the group-scope mirror of the contract-limits
// enabled=false cases in contract_limits_test.go.
func TestSharedGroupScopesEnforcedWithAdmissionFlagOff(t *testing.T) {
	up, hits := sharedGroupBackend(t)
	s, em := proxyWithGroupScopesEnabled(t, false, nil)

	// Only the group envelope: every org/owner scoped limit header absent.
	envelopeOnly := func(envelope string) *http.Request {
		req := groupScopesRequest(t, up, envelope)
		for _, header := range identity.ScopedRateLimitHeaders {
			req.Header.Del(header)
		}
		return req
	}

	// A group zero cap blocks every request with the contractual 429.
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, envelopeOnly(groupScopesValue(testGroupA, "0,,,", "")))
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("zero-cap status=%d, want 429", rr.Code)
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Fatal("zero-cap response missing Retry-After")
	}

	// A request within the group limits is admitted and forwarded.
	rr = httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, envelopeOnly(groupScopesValue(testGroupA, "30,,,", "")))
	if rr.Code != http.StatusOK {
		t.Fatalf("within-limits status=%d, want 200", rr.Code)
	}
	if *hits != 1 {
		t.Fatalf("upstream hits=%d, want 1 (the zero cap never forwards)", *hits)
	}
	evs := em.waitForEvents(1, time.Second)
	if len(evs) != 1 {
		t.Fatalf("emitted events=%d, want 1", len(evs))
	}
	// Membership attribution rides the emitted event whether or not admission
	// ran: skipping the store for an unlimited envelope never drops it.
	if got := evs[0].MemberGroupIDs; len(got) != 1 || got[0] != testGroupA {
		t.Fatalf("MemberGroupIDs = %v, want [%s]", got, testGroupA)
	}
}
