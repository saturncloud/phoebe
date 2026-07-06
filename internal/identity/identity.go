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

	// HeaderBaseModel carries the Hugging Face base model id — the CATALOG PRICE KEY
	// (C4). vLLM serves every Token Factory endpoint under its ENDPOINT NAME (a
	// ratified naming contract), so the engine-reported model is NOT a price key the
	// file can name; this header is how the rater finds the catalog rate. Atlas
	// resolves it at endpoint creation and injects it on ALL Token Factory inference
	// deployments: for a base-model endpoint it is the model being served; for a
	// fine-tune checkpoint endpoint it is the base the checkpoint derives from (E3
	// derived_from — a fine-tune cannot deploy without a base).
	//
	// PLUMBING SEAM (Atlas-side, built in parallel): the Atlas-rendered Traefik
	// middleware injects this header AND HeaderAdapter per deployment, as deploy-time
	// resource properties — server-side, anti-spoof overwritten, never trusted from
	// clients. Phoebe reads them defensively: absent = empty string. A fine-tune
	// event (adapter present or ft:-prefixed model) with an empty base_model then
	// fails loud at rating (ErrNoPrice), never silently bills $0 — so a missing
	// header surfaces as a screaming anomaly, not lost revenue.
	HeaderBaseModel = "X-Saturn-Base-Model"

	// HeaderAdapter carries the fine-tune checkpoint artifact id, and is present ONLY
	// on fine-tune checkpoint deployments (absent on base-model endpoints). Like
	// HeaderBaseModel it is a deploy-time resource property, injected server-side by
	// the Atlas-rendered Traefik middleware, anti-spoof overwritten, never trusted
	// from clients. Its PRESENCE is the fine-tune-premium trigger for rating (C4):
	// the endpoint serves under its endpoint name, so the model id alone cannot mark
	// it as a fine-tune. Its VALUE is forensic (which checkpoint artifact served the
	// request), carried onto billing_event.adapter verbatim. Phoebe reads it
	// defensively: absent = empty string (a base-model endpoint).
	HeaderAdapter = "X-Saturn-Adapter"

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
	// BaseModel is the HF base id — the catalog price key (C4), present on all Token
	// Factory inference deployments (base-model AND fine-tune checkpoint endpoints).
	// Carried to the metering event so the rater can price the endpoint-name model.
	BaseModel string
	// Adapter is the fine-tune checkpoint artifact id, present ONLY on fine-tune
	// checkpoint deployments. Its presence triggers the fine-tune premium at rating
	// (C4); its value is forensic. Empty for a base-model endpoint.
	Adapter string
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
		Adapter:      r.Header.Get(HeaderAdapter),
	}
}
