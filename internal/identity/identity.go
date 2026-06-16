// Package identity reads the trusted identity headers that atlas-auth
// (Traefik ForwardAuth) injects upstream. The interceptor does NOT
// authenticate or authorize — it trusts the resolved identity in these
// headers, exactly as auth-server emits them.
package identity

import "net/http"

// Header names injected by atlas-auth. Kept identical to auth-server's
// constants so the contract between the two services stays in one shape.
const (
	HeaderUserID       = "X-Saturn-User-Id"
	HeaderGroupID      = "X-Saturn-Group-Id"
	HeaderResourceID   = "X-Saturn-Resource-Id"
	HeaderResourceType = "X-Saturn-Resource-Type"

	// HeaderBaseModel carries the Hugging Face base model id a fine-tune deployment
	// derives from (E3 derived_from). Atlas resolves and validates it at endpoint
	// creation — a fine-tune cannot deploy without a base — and injects it here
	// alongside the other identity headers, so phoebe carries it onto the metering
	// event for the rater to price a fine-tune at base x premium. It is EMPTY for a
	// base-model deployment (whose engine model name is already the price key).
	//
	// PLUMBING SEAM (Atlas-side, separate change): atlas-auth must inject this header
	// (the deploy-time base_model) and add it to Traefik's authResponseHeaders
	// allowlist, exactly as for HeaderAuthID. Phoebe reads it defensively: absent =
	// empty string. An `ft:`-prefixed model with an empty base_model then fails loud
	// at rating (ErrNoPrice), never silently bills $0 — so a missing header surfaces
	// as a screaming anomaly, not lost revenue.
	HeaderBaseModel = "X-Saturn-Base-Model"

	// HeaderAuthID carries the token / API-key identity — the JWT `sub` claim,
	// which in Atlas is the IdentityAuth.id (the same value for both browser-
	// session and API-key tokens; they share one token mechanism). This is the
	// stable key to attribute consumption to a specific API key. Org / user /
	// group are resolved DOWNSTREAM (out of band, at rating time) from this id
	// via the IdentityAuth record, so the hot path never has to resolve the
	// active-org context (which, for a user in multiple orgs, isn't in the
	// token).
	//
	// NOTE: auth-server does not inject this header yet — it currently emits
	// only User/Group/Resource. Wiring `sub` → this header in auth-server, and
	// adding it to Traefik's authResponseHeaders allowlist, is a separate
	// (small) change. Phoebe reads it defensively: absent = empty string.
	HeaderAuthID = "X-Saturn-Auth-Id"
)

// Identity is the trusted, pre-resolved caller identity for a request. Phoebe
// captures everything atlas-auth gives it and attributes downstream; it does
// not decide which field is "the tenant" on the hot path.
type Identity struct {
	// AuthID is the token / API-key identity (JWT sub). Primary attribution key.
	AuthID       string
	UserID       string
	GroupID      string
	ResourceID   string
	ResourceType string
	// BaseModel is the HF base id a fine-tune derives from (E3), present only for a
	// fine-tune deployment. Empty for a base model. Carried to the metering event so
	// the rater can price an ft:<checkpoint> at base x premium.
	BaseModel string
}

// FromRequest extracts the trusted identity headers. It performs no
// validation beyond reading the values; authorization happened at the edge.
func FromRequest(r *http.Request) Identity {
	return Identity{
		AuthID:       r.Header.Get(HeaderAuthID),
		UserID:       r.Header.Get(HeaderUserID),
		GroupID:      r.Header.Get(HeaderGroupID),
		ResourceID:   r.Header.Get(HeaderResourceID),
		ResourceType: r.Header.Get(HeaderResourceType),
		BaseModel:    r.Header.Get(HeaderBaseModel),
	}
}
