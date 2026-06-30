// Package metering defines the immutable billing event and the Emitter
// contract. Metering captures RAW token counts only — rating (price) is
// applied later, out of band. Emit must never block the client response.
package metering

import (
	"context"

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
	RequestID string `json:"request_id"`

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
	Model   string `json:"model,omitempty"`
	Adapter string `json:"adapter,omitempty"`

	// BaseModel is the Hugging Face base id a fine-tune derives from (E3
	// derived_from), stamped at deploy time by Atlas (which enforces base_model is
	// present to deploy a fine-tune — a fine-tune cannot exist without a base). It is
	// EMPTY for a base model (whose Model already IS the price key). The rater needs
	// it because Model for a fine-tune is an `ft:<checkpoint>` id whose base is not
	// otherwise recoverable: with BaseModel set, the rater prices the fine-tune at
	// base_price x premium (E3 pointer-not-copy). An `ft:` Model with an EMPTY
	// BaseModel is a propagation bug, not a free model — the rater fails it loud
	// (ErrNoPrice), never $0. Captured verbatim on the hot path; empty is valid.
	BaseModel string `json:"base_model,omitempty"`

	// Token counts (the engine's own usage block; never re-tokenized).
	PromptTokens     int `json:"prompt_tokens"`
	CachedTokens     int `json:"cached_tokens"`
	CompletionTokens int `json:"completion_tokens"`

	FinishReason string `json:"finish_reason,omitempty"`
	GPUType      string `json:"gpu_type,omitempty"` // for margin; echoed by router/engine
	Aborted      bool   `json:"aborted,omitempty"`

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
	l.Log.Info.Printf("metering event: request_id=%s auth_id=%s org=%s group=%s user=%s resource=%s/%s model=%s prompt=%d cached=%d completion=%d finish=%s aborted=%t",
		e.RequestID, e.AuthID, e.OrgID, e.GroupID, e.UserID, e.ResourceType, e.ResourceID, e.Model,
		e.PromptTokens, e.CachedTokens, e.CompletionTokens, e.FinishReason, e.Aborted)
}
