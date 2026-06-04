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
type Event struct {
	RequestID        string `json:"request_id"`
	GroupID          string `json:"group_id"` // tenant / org
	UserID           string `json:"user_id"`
	Model            string `json:"model"`
	Adapter          string `json:"adapter,omitempty"`
	PromptTokens     int    `json:"prompt_tokens"`
	CachedTokens     int    `json:"cached_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
	FinishReason     string `json:"finish_reason,omitempty"`
	GPUType          string `json:"gpu_type,omitempty"` // for margin; echoed by router/engine
	Aborted          bool   `json:"aborted,omitempty"`
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
	l.Log.Info.Printf("metering event: request_id=%s group=%s user=%s model=%s prompt=%d cached=%d completion=%d finish=%s aborted=%t",
		e.RequestID, e.GroupID, e.UserID, e.Model,
		e.PromptTokens, e.CachedTokens, e.CompletionTokens, e.FinishReason, e.Aborted)
}
