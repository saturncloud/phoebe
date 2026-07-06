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

	// HeaderOrgID carries the org that OWNS the served deployment — the customer to
	// attribute (and ultimately bill) this inference to (E2). This is DELIBERATELY
	// NOT the caller's active-org context (which, per HeaderAuthID above, isn't
	// resolvable from the token for a multi-org user): it is a property of the
	// *resource*, not the *caller*. Atlas knows the deployment's org_id at deploy
	// time (the saturncloud.io/org-id label on the deployment) and injects it here
	// as a per-deployment Traefik Middleware header on the inference route — so it
	// is present whenever the deployment can serve inference. Capturing it HERE,
	// at meter time, removes the push-time resource_id→org_id reconstruction (the
	// torn-down-deployment race): the org rides the metering event like resource_id,
	// instead of being re-joined against the deletable resource_name table at push.
	//
	// PLUMBING SEAM (Atlas-side, separate change): Atlas injects this header per
	// deployment and adds it to Traefik's authResponseHeaders allowlist, exactly as
	// for HeaderBaseModel. Phoebe reads it defensively: absent = empty string. An
	// absent org_id is intentionally NOT a hot-path gate (it must never black-hole
	// inference while the producer header rolls out per-install); the fail-closed
	// for a missing org lives downstream at push (a NULL-org rollup is held +
	// counted + screamed, never billed to a guessed org), exactly where it can't
	// take down the inference path. See internal/proxy missingBillingFields.
	HeaderOrgID = "X-Saturn-Org-Id"

	// HeaderUpstream carries the ROUTING AUTHORITY: the deployment's real backend
	// (host:port) that phoebe must forward to. Atlas injects it per Token Factory
	// inference deployment (the `_phoebe_inference_upstream` Middleware, after
	// atlas-auth), because Atlas authoritatively knows the deployment's own k8s
	// Service (`{k8s_name}.{ns}.svc.cluster.local:{VLLM_SERVE_PORT}`) when it builds
	// the route — a `pd-...` Service name that phoebe canNOT derive by convention
	// (the ConventionResolver's `model-{resource_id}...` template computes a different,
	// non-existent name). So when this header is present, phoebe forwards THERE and
	// does not resolve; the resolver is only the fallback for the non-inference path.
	//
	// TRUST: allowlisted in Traefik's authResponseHeaders, so a client-supplied value
	// is stripped before it reaches phoebe (a client must not be able to point its
	// authorized-for-X request at engine Y — a confused-deputy). Phoebe therefore
	// trusts this header exactly like the identity headers. Absent = empty string
	// (the normal, non-inference path; fall back to the resolver).
	HeaderUpstream = "X-Saturn-Upstream"
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
	// OrgID is the org that owns the served deployment (E2 customer attribution),
	// injected by Atlas as a per-deployment Traefik header. Carried verbatim onto the
	// metering event so push reads org straight off the rollup instead of re-joining
	// the resource_name table at push time. Empty is tolerated on the hot path (a
	// missing org never gates inference; it is held + screamed at push, never billed
	// to a guessed org).
	OrgID string
	// BaseModel is the HF base id — the catalog price key (C4), present on all Token
	// Factory inference deployments (base-model AND fine-tune checkpoint endpoints).
	// Carried to the metering event so the rater can price the endpoint-name model.
	BaseModel string
	// Adapter is the fine-tune checkpoint artifact id, present ONLY on fine-tune
	// checkpoint deployments. Its presence triggers the fine-tune premium at rating
	// (C4); its value is forensic. Empty for a base-model endpoint.
	Adapter string
	// Upstream is the deployment's real backend (host:port) for phoebe to forward to,
	// injected by Atlas per inference deployment (routing authority, trusted). Present
	// only on the Token Factory inference path; empty for everything else (fall back to
	// the resolver). See HeaderUpstream.
	Upstream string
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
		OrgID:        r.Header.Get(HeaderOrgID),
		BaseModel:    r.Header.Get(HeaderBaseModel),
		Adapter:      r.Header.Get(HeaderAdapter),
		Upstream:     r.Header.Get(HeaderUpstream),
	}
}
