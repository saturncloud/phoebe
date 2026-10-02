// Package metering defines the immutable billing event and the Emitter
// contract. Metering captures RAW token counts only — rating (price) is
// applied later, out of band. Emit must never block the client response.
package metering

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

// Usage mirrors the OpenAI-compatible usage block that vLLM/SGLang/TRT-LLM
// emit. It is the billing authority; the interceptor never re-tokenizes.
//
// NOTE: the exact cached-token field name must be verified against the
// deployed engine version (see PromptTokensDetails). vLLM reports cached
// prompt tokens under prompt_tokens_details.cached_tokens.
type Usage struct {
	PromptTokens        int                  `json:"prompt_tokens"`
	CompletionTokens    int                  `json:"completion_tokens"`
	TotalTokens         int                  `json:"total_tokens"`
	PromptTokensDetails *PromptTokensDetails `json:"prompt_tokens_details,omitempty"`
}

type PromptTokensDetails struct {
	CachedTokens int `json:"cached_tokens"`
}

// CachedTokens returns the cached prompt-token count, or 0 if absent.
func (u Usage) CachedTokens() int {
	if u.PromptTokensDetails == nil {
		return 0
	}
	return u.PromptTokensDetails.CachedTokens
}

// Event is one immutable, idempotent metering record per request. It is keyed
// by RequestID for downstream dedup (at-least-once delivery).
//
// Phoebe records the RAW identity it was given (every X-Saturn-* header) plus
// raw token counts. It does NOT resolve org/tenant on the hot path: AuthID (the
// token / API-key id) is the stable attribution key, and rating resolves
// auth_id → IdentityAuth → user/group/org out of band. UserID, GroupID,
// ResourceID, ResourceType are captured verbatim so no information the edge
// gave us is lost.
type Event struct {
	// RequestID is the trusted, server-minted BILLABLE ATTEMPT id.  The JSON and
	// database name are retained for wire compatibility; it is never copied from
	// the client's X-Request-Id.
	RequestID string `json:"request_id"`
	// ClientRequestID is the untrusted correlation/idempotency value supplied by
	// the caller.  It is forensic only: retries that reuse it still receive a new
	// RequestID and therefore remain distinct billable execution attempts.
	ClientRequestID string `json:"client_request_id,omitempty"`

	// Identity, captured verbatim from atlas-auth headers.
	AuthID       string `json:"auth_id,omitempty"`       // token / API-key id (JWT sub) — primary key
	UserID       string `json:"user_id,omitempty"`       // present on user tokens
	GroupID      string `json:"group_id,omitempty"`      // present on group tokens
	ResourceID   string `json:"resource_id,omitempty"`   // model / deployment id
	ResourceType string `json:"resource_type,omitempty"` // e.g. workspace, deployment
	// OrgID is the org that OWNS the served deployment (E2 customer attribution),
	// injected by Atlas as a per-deployment Traefik header (X-Saturn-Org-Id). Captured
	// verbatim at meter time so push reads org off the rollup instead of re-joining
	// resource_name at push. Empty when the producer header is absent (rollout gap) —
	// such a row is held + counted + screamed at push, never billed to a guessed org.
	OrgID string `json:"org_id,omitempty"`

	// Workload.
	Model string `json:"model,omitempty"`
	// Adapter is the fine-tune checkpoint artifact id (X-Saturn-Adapter), injected
	// per deployment by the Atlas-rendered Traefik middleware and present ONLY on
	// fine-tune checkpoint deployments. Its PRESENCE marks the event as fine-tune
	// traffic for the rater (C4 — the endpoint serves under its endpoint name, so
	// Model alone cannot mark it); its VALUE is forensic (which checkpoint served).
	// Empty for a base-model endpoint. Captured verbatim; empty is valid.
	Adapter string `json:"adapter,omitempty"`

	// BaseModel is the Hugging Face base id — the CATALOG PRICE KEY (C4), stamped at
	// deploy time by Atlas on ALL Token Factory inference deployments: the model
	// being served (base-model endpoint) or the base the checkpoint derives from
	// (fine-tune endpoint, E3 derived_from — a fine-tune cannot exist without a
	// base). The rater needs it because Model is the ENDPOINT NAME the engine serves
	// under, which the price file never names: with BaseModel set, a base-model
	// endpoint prices at the plain base rate and a fine-tune (Adapter present, or an
	// ft: Model) at base_price x premium (E3 pointer-not-copy). A fine-tune event
	// with an EMPTY BaseModel is a propagation bug, not a free model — the rater
	// fails it loud (ErrNoPrice), never $0. Captured verbatim; empty is valid.
	BaseModel string `json:"base_model,omitempty"`

	// ServingMode is the serving mode ("shared" | "dedicated"), the SKU pricing
	// axis. The proxy's serving-mode gate guarantees one of the two explicit
	// values; "shared" prices from the distinct shared:<base> rate row. An empty
	// value only appears on events metered before the 2026-09-29 serving-mode
	// cutover, when the key was omitted and its absence meant dedicated. That
	// evidence is billed as dedicated (ratified ledger item 6): UnmarshalEvent
	// (used by the drainer and the spool replay), recovery and migration 0007
	// all map it to "dedicated" at ingest and replay. Decode stored or queued
	// event JSON with UnmarshalEvent, which maps only an ABSENT key; an explicit
	// "" or null from a post-cutover producer is a bug, stays "", and the rater
	// withholds it.
	ServingMode string `json:"serving_mode"`

	// GraphK8sName is the DynamoGraphDeployment (DGD) that served this request —
	// the COST CENTRE. It is carried so a rollup's cost stays attributable to the
	// hardware that produced it, and it is the only way to do so in SHARED mode:
	// there, MANY tf_model rows across MANY orgs ride ONE platform-owned base
	// graph, and that graph has no row in any database (it is keyed by a
	// deterministic name and reference-counted), so resource_id cannot identify it.
	//
	// Sourced from gateway resolution (tf_model.graph_k8s_name) on the gateway
	// path, and derived from the upstream host on the header path. Empty is valid
	// and never withholds money — it only means the cost is not pool-attributable.
	// EVIDENCE ONLY: never part of the billing grain (see migrations/0006).
	GraphK8sName string `json:"graph_k8s_name,omitempty"`

	// Token counts (the engine's own usage block; never re-tokenized).
	PromptTokens     int `json:"prompt_tokens"`
	CachedTokens     int `json:"cached_tokens"`
	CompletionTokens int `json:"completion_tokens"`

	FinishReason string `json:"finish_reason,omitempty"`
	GPUType      string `json:"gpu_type,omitempty"` // for margin; echoed by router/engine
	Aborted      bool   `json:"aborted,omitempty"`
	// UsageFound distinguishes a legitimate zero-token engine usage block from a
	// failed/aborted attempt for which no authoritative counts were available.
	UsageFound bool `json:"usage_found"`
	StatusCode int  `json:"status_code,omitempty"`
	Streamed   bool `json:"streamed,omitempty"`

	// TimestampUnixMs is stamped by the emitter, not in the hot path here.
	TimestampUnixMs int64 `json:"timestamp_unix_ms"`
}

// Emitter ships metering events to a durable queue off the hot path. Emit
// MUST be asynchronous / non-blocking with respect to the client response.
type Emitter interface {
	Emit(ctx context.Context, e Event)
}

// LogEmitter is a placeholder Emitter that writes events to the logger. It
// stands in for the real durable queue (Kafka / Redis Streams) during the
// walking-skeleton phase.
type LogEmitter struct {
	Log *logging.Logger
}

func (l *LogEmitter) Emit(_ context.Context, e Event) {
	l.Log.Info.Printf("metering event: request_id=%s client_request_id=%s auth_id=%s org=%s group=%s user=%s resource=%s/%s model=%s prompt=%d cached=%d completion=%d finish=%s aborted=%t usage_found=%t status=%d streamed=%t",
		e.RequestID, e.ClientRequestID, e.AuthID, e.OrgID, e.GroupID, e.UserID, e.ResourceType, e.ResourceID, e.Model,
		e.PromptTokens, e.CachedTokens, e.CompletionTokens, e.FinishReason, e.Aborted, e.UsageFound, e.StatusCode, e.Streamed)
}

// UnmarshalEvent decodes one event's JSON, the shape written by the emitter to
// the drain queue and the on-disk spool. Evidence written before the
// 2026-09-29 serving-mode cutover carries no serving_mode key (the field was
// omitempty and dedicated was the empty value). All of that traffic was
// dedicated (ratified ledger item 6), so an ABSENT key decodes as "dedicated",
// the same rule internal/recovery applies. A post-cutover producer always
// writes the key, so an explicit "" or null is a producer bug, not pre-cutover
// evidence: it decodes as "" and the rater withholds it as an invalid serving
// mode instead of billing it as dedicated.
//
// Every reader that later re-marshals the event (the spool replay re-sends it
// to the queue, and ServingMode is no longer omitempty) must decode through
// this function, or a pre-cutover event would acquire an explicit "" on the
// way and be withheld instead of billed as dedicated.
func UnmarshalEvent(data []byte) (Event, error) {
	var ev Event
	if err := json.Unmarshal(data, &ev); err != nil {
		return ev, err
	}
	// A map probe, not a pointer field: json.Unmarshal sets a pointer to nil for
	// an explicit null, which would be indistinguishable from an absent key.
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err != nil {
		return ev, err
	}
	// Case-insensitive: json.Unmarshal above matched the key under any casing,
	// so an exact-case lookup would overwrite a decoded "Serving_Mode" value.
	if !HasKeyFold(probe, "serving_mode") {
		ev.ServingMode = identity.ServingModeDedicated
	}
	return ev, nil
}

// HasKeyFold reports whether probe holds name under any casing. encoding/json
// matches object keys to struct fields case-insensitively, so a record with
// "Serving_Mode":"shared" decodes ServingMode as "shared"; an exact-case
// presence check would miss that key, treat the field as absent, and overwrite
// the decoded value with a default (billing a shared event as dedicated).
// Every decoder that applies an absent-key rule to Event JSON must use this.
func HasKeyFold(probe map[string]json.RawMessage, name string) bool {
	for k := range probe {
		if strings.EqualFold(k, name) {
			return true
		}
	}
	return false
}
