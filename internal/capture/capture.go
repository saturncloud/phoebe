// Package capture defines what the streaming tee extracts from an upstream
// response — the frozen handoff between the proxy's forward-then-inspect tee
// (M1) and the metering emitter (M2). Downstream tracks build against this
// contract, not against the tee's internals.
package capture

import "github.com/saturncloud/phoebe/internal/metering"

// Result is everything the tee captures from a single upstream response,
// whether streaming (SSE) or non-streaming (single JSON body). It carries the
// raw token counts (the engine's own usage block — never re-tokenized) plus
// the facts metering needs to build an Event.
//
// A Result is produced for EVERY proxied request, including ones where usage
// was never seen (UsageFound=false) — the emitter decides policy on those
// (e.g. reconcile later), but the tee always reports what it observed.
type Result struct {
	// Usage is the engine's usage block. Zero-valued if UsageFound is false.
	Usage metering.Usage

	// UsageFound reports whether a usage block was actually captured. False
	// means the upstream never emitted one (client didn't request streaming
	// usage and we somehow failed to force it, a non-OpenAI response, an
	// upstream error, or an abort before the usage chunk). The emitter must
	// not treat a false here as "zero tokens" — it means "unknown", a
	// reconciliation signal, not a free request.
	UsageFound bool

	// FinishReason is the choice's finish_reason ("stop", "length",
	// "tool_calls", ...). In streaming it arrives in a chunk BEFORE the usage
	// chunk, so the tee captures it independently. Empty if never seen.
	FinishReason string

	// Aborted is true if the client disconnected before the response
	// completed (the upstream was cancelled). When true, Usage may be absent
	// or partial; bill-partial policy lives in the emitter, not here.
	Aborted bool

	// Streamed reports whether the response was SSE (true) or a single JSON
	// body (false). Useful for diagnostics and reconciliation.
	Streamed bool
}
