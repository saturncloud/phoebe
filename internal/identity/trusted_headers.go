package identity

// Trusted-header registry (R3 header trust). The shared-inference parser —
// the gateway mark (HeaderGateway) plus the envelope fields (org, owner,
// serving mode, served-model allow-list, and the org/owner rate-limit
// policy) — resolves those headers ONLY through the active set in this
// registry. A header not in the active set is treated as ABSENT: never read
// for a trust decision. The set arrives as runtime config (env
// PHOEBE_TRUSTED_HEADERS, comma-separated, rendered from the saturn-k8s
// phoebe chart's ConfigMap); when the config is empty, unset, or malformed
// (nothing left after trimming and dropping empties), a hard-coded fallback
// equal to the pinned 13 engages so phoebe fails closed, and a loud warning
// flags the misrendered chart for the operator.
//
// The set covers exactly the R3 envelope headers listed in the pinned 13.
// The remaining identity headers (AuthID / UserID / GroupID / ResourceID /
// ResourceType / BaseModel / Adapter / Upstream) are read under the ratified
// edge contract (ForwardAuth authResponseHeaders allowlist + Atlas
// anti-spoof per-deployment injection), which is outside the R3 gate — so
// with the fallback set engaged, request handling is byte-for-byte
// unchanged (a pure hardening).

import (
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"github.com/saturncloud/phoebe/internal/logging"
)

// TrustedHeadersEnv is the runtime-config env var carrying the comma-separated
// list of the ONLY X-Saturn-* envelope headers phoebe's parser trusts. Kept
// exactly this name: the saturn-k8s phoebe chart's ConfigMap renders it.
const TrustedHeadersEnv = "PHOEBE_TRUSTED_HEADERS"

// pinnedTrustedHeaders is the hard-coded fallback set, in the pinned order.
// It is exactly the R3 envelope: the gateway mark plus the shared-inference
// policy headers the proxy's admission/lane inputs and gateway resolution
// consume. An empty or misrendered PHOEBE_TRUSTED_HEADERS engages this set
// (with a loud warning), never an empty trust set.
//
// The five legacy single-scope quota headers (X-Saturn-Service-Tier and
// X-Saturn-Rate-Limit-*) were removed by ruling R8 (hard cut, no fallback):
// phoebe no longer reads them, so they are not trusted either.
var pinnedTrustedHeaders = []string{
	HeaderGateway,
	HeaderOrgID,
	HeaderOwnerID,
	HeaderServingMode,
	HeaderServedModel,
	HeaderOrgRateLimitRequests,
	HeaderOrgRateLimitTotalPromptTokens,
	HeaderOrgRateLimitUncachedPromptTokens,
	HeaderOrgRateLimitGeneratedTokens,
	HeaderOwnerRateLimitRequests,
	HeaderOwnerRateLimitTotalPromptTokens,
	HeaderOwnerRateLimitUncachedPromptTokens,
	HeaderOwnerRateLimitGeneratedTokens,
}

// requiredTrustedHeaders are the envelope headers request handling cannot work
// without, each with the consequence of leaving it out of a configured
// PHOEBE_TRUSTED_HEADERS list. A configured list that omits one is still
// installed as written (the missing header is NOT added: the edge strip
// middleware is rendered from the same list, so a header missing from it is
// not stripped, and trusting it would let a client set its own value), and
// phoebe does not exit (the never-a-startup-failure policy). Instead
// LoadTrustedHeaders logs one ERROR line per missing header at startup, so
// the config gap is visible before the first refused request.
var requiredTrustedHeaders = []struct {
	name        string
	consequence string
}{
	{HeaderGateway, "gateway requests will not be recognized"},
	{HeaderServingMode, "every header-routed (non-gateway) inference request will be refused with 404 because the serving mode reads as absent (ruling #19)"},
	{HeaderOwnerID, "with admission enabled, every shared gateway inference request will be refused with 503 because the quota policy has no owner-id anchor (R7/R8: the legacy service-tier envelope is no longer read)"},
	{HeaderOrgID, "with admission enabled, every shared gateway inference request will be refused with 503 because the trusted organization identity reads as absent"},
}

// trustedHeaderSet is the active trusted-header set. Keys are canonical
// header names (http.CanonicalHeaderKey), so membership is case-insensitive
// per HTTP convention.
type trustedHeaderSet map[string]struct{}

var activeTrustedHeaders atomic.Pointer[trustedHeaderSet]

func init() {
	// Before config load the pinned 13 are active: phoebe never reads an
	// envelope header outside the list, before OR after the runtime config
	// is loaded, and the pre-load default must preserve request handling.
	fallback := newTrustedHeaderSet(pinnedTrustedHeaders)
	activeTrustedHeaders.Store(&fallback)
}

func newTrustedHeaderSet(names []string) trustedHeaderSet {
	set := make(trustedHeaderSet, len(names))
	for _, name := range names {
		set[http.CanonicalHeaderKey(strings.TrimSpace(name))] = struct{}{}
	}
	return set
}

// isTrustedHeader reports whether name is in the active trusted-header set.
func isTrustedHeader(name string) bool {
	_, ok := (*activeTrustedHeaders.Load())[http.CanonicalHeaderKey(name)]
	return ok
}

// trustedHeaderValue resolves name through the active trusted-header set: a
// header not in the set is ABSENT — the value is never read, so a stamped
// header outside the list cannot influence any trust decision.
func trustedHeaderValue(r *http.Request, name string) string {
	if !isTrustedHeader(name) {
		return ""
	}
	return r.Header.Get(name)
}

// LoadTrustedHeaders parses PHOEBE_TRUSTED_HEADERS and installs the active
// trusted-header set for the process. It is called ONCE at startup,
// immediately after config load (cmd/interceptor/main.go). Semantics:
//   - set and non-empty after trimming/dropping empties -> that set is
//     active (entries are canonicalized; case is ignored per HTTP
//     convention);
//   - unset, empty/whitespace-only, or nothing left after splitting and
//     trimming -> the pinned 13 engage AND a loud warning flags the
//     misrendered chart. Fallback, never a startup failure: phoebe must keep
//     serving with the pinned set rather than crash-loop behind a broken
//     ConfigMap render.
//   - a configured list that omits a header in requiredTrustedHeaders is
//     still installed as written, with one ERROR log line per missing
//     header naming it and the consequence.
func LoadTrustedHeaders(log *logging.Logger) {
	raw, set := os.LookupEnv(TrustedHeadersEnv)
	var names []string
	if set {
		for _, part := range strings.Split(raw, ",") {
			if name := strings.TrimSpace(part); name != "" {
				names = append(names, name)
			}
		}
	}
	if len(names) == 0 {
		fallback := newTrustedHeaderSet(pinnedTrustedHeaders)
		activeTrustedHeaders.Store(&fallback)
		log.Warn.Printf("%s is unset, empty, or malformed — falling back to the pinned %d-header default set (%s). If the phoebe chart's ConfigMap is supposed to render this list, the rendering is broken; phoebe keeps serving with the built-in defaults",
			TrustedHeadersEnv, len(pinnedTrustedHeaders), strings.Join(pinnedTrustedHeaders, ", "))
		return
	}
	loaded := newTrustedHeaderSet(names)
	for _, req := range requiredTrustedHeaders {
		if _, ok := loaded[http.CanonicalHeaderKey(req.name)]; !ok {
			log.Error.Printf("%s omits %s; %s. Add %s to the phoebe chart's trustedHeaders list, or unset %s to use the pinned default set",
				TrustedHeadersEnv, req.name, req.consequence, req.name, TrustedHeadersEnv)
		}
	}
	activeTrustedHeaders.Store(&loaded)
	log.Info.Printf("trusted headers: parser trusts %d header(s) from %s: %s",
		len(names), TrustedHeadersEnv, strings.Join(names, ", "))
}

// ActiveTrustedHeaders returns a copy of the active trusted-header set (the
// canonical names), for tests and startup introspection.
func ActiveTrustedHeaders() map[string]struct{} {
	set := *activeTrustedHeaders.Load()
	out := make(map[string]struct{}, len(set))
	for name := range set {
		out[name] = struct{}{}
	}
	return out
}
