// Package iolog is M5: per-tenant, opt-in, sampled, short-retention capture of
// request and response BODIES, forked from the same upstream bytes the metering
// tee already sees but written to a SEPARATE store with its own retention and
// access controls.
//
// # Why this is a separate subsystem from metering
//
// Metering (internal/emit) is the billing authority: it is DURABLE — a dropped
// metering event is lost revenue, so emit climbs a three-level durability ladder
// (Valkey → WAL → log floor) and NEVER drops on overflow.
//
// I/O logging is the opposite contract: it is BEST-EFFORT debug telemetry. A
// sampled debug body that is dropped under load is fine — losing one is cheaper
// than letting the log path add latency or backpressure to a client response.
// So this package deliberately drops-and-counts on overflow rather than spilling
// to disk. This asymmetry is intentional; do not "fix" it to match metering.
//
// # Privacy posture: fail closed
//
// Request/response bodies are sensitive (prompts, completions, possibly PII).
// I/O logging is therefore OFF BY DEFAULT (see Policy) and only captures bodies
// for requests a tenant has explicitly opted in and that pass the sample gate.
// When logging is off, the hot path pays ZERO extra cost — no buffering, no
// allocation beyond the existing metering path (see internal/proxy).
//
// # Storage
//
// Postgres-first: bodies land in an io_log table with a tsvector + GIN index so
// operators can full-text "grep inside bodies". OpenSearch is designed-for but
// NOT built (see OpenSearchSink); pgvector semantic search is a later milestone
// and is NOT built here.
package iolog

import (
	"context"
	"time"
)

// Record is one captured request/response pair. It mirrors metering.Event's
// identity-capture spirit — every X-Saturn-* field is recorded verbatim so
// attribution is resolved downstream — but carries the BODIES (which metering
// deliberately never holds) plus the status/latency facts useful for tenant
// troubleshooting.
//
// A Record is built only when Policy.ShouldLog returned true for the request,
// so constructing one already implies the opt-in + sample gate passed.
type Record struct {
	// RequestID is the per-request idempotency key (X-Request-Id). Primary join
	// key back to the metering event for the same request.
	RequestID string `json:"request_id"`

	// Identity, captured verbatim from atlas-auth headers (same shape as
	// metering.Event). AuthID is the primary attribution key.
	AuthID       string `json:"auth_id,omitempty"`
	UserID       string `json:"user_id,omitempty"`
	GroupID      string `json:"group_id,omitempty"`
	ResourceID   string `json:"resource_id,omitempty"`
	ResourceType string `json:"resource_type,omitempty"`

	// Model is the workload identifier (in Atlas, the resource id doubles as the
	// model id — see proxy.emit).
	Model string `json:"model,omitempty"`

	// RequestBody is the ORIGINAL client request body (pre-rewrite), capped at the
	// configured MaxBodyBytes. See the proxy wiring comment for why the original
	// is captured rather than the usage-forced rewrite: tenant troubleshooting
	// wants to see what the client actually sent, not phoebe's internal
	// include_usage injection.
	RequestBody string `json:"request_body,omitempty"`

	// RequestTruncated is true if RequestBody was cut at the size cap. Mirrors
	// ResponseTruncated. The cap matters here for correctness, not just memory:
	// the request body flows into to_tsvector at INSERT time, which Postgres
	// rejects past ~1 MiB — an uncapped long-context prompt would fail the whole
	// row. The full body still reached the upstream verbatim; the cap bounds only
	// what we STORE.
	RequestTruncated bool `json:"request_truncated,omitempty"`

	// ResponseBody is the response bytes forwarded to the client, up to the
	// configured cap. For SSE streams this is the concatenated raw stream.
	ResponseBody string `json:"response_body,omitempty"`

	// ResponseTruncated is true if ResponseBody was cut at the size cap. The
	// stored body is the first N bytes; the full body still streamed to the
	// client verbatim (the cap only bounds what we BUFFER for logging).
	ResponseTruncated bool `json:"response_truncated,omitempty"`

	// StatusCode is the upstream HTTP status forwarded to the client.
	StatusCode int `json:"status_code,omitempty"`

	// Streamed reports whether the response was SSE (true) or a single JSON body.
	Streamed bool `json:"streamed,omitempty"`

	// LatencyMs is wall-clock time from request receipt to response completion.
	LatencyMs int64 `json:"latency_ms,omitempty"`

	// Timestamp is when the request completed. Stamped by the proxy at Log time.
	Timestamp time.Time `json:"timestamp"`
}

// Sink is the async, non-blocking handoff for I/O-log records. Log MUST behave
// like metering.Emitter.Emit: it hands the record off and returns immediately,
// and it MUST NEVER block or fail the client response. Unlike the emitter,
// however, a Sink is permitted to DROP records under load (best-effort) — see
// the package doc for why losing a sampled debug body is acceptable.
//
// Abstract the contract, not the implementation: the proxy depends only on this
// interface, so NopSink (logging off), PostgresSink (the built store), and a
// future OpenSearchSink are interchangeable at the wiring site.
type Sink interface {
	// Log records a captured request/response pair. Non-blocking; best-effort.
	Log(ctx context.Context, rec Record)

	// Close drains in-flight records and releases resources on graceful
	// shutdown. The context bounds the drain wait.
	Close(ctx context.Context) error
}

// NopSink is the default Sink when I/O logging is off. It does nothing — no
// allocation, no goroutines, no I/O. Wiring a NopSink (the main.go default)
// guarantees the logging subsystem is inert unless explicitly enabled.
//
// NopSink is the safe/private default: with it in place, no bodies are ever
// captured or stored regardless of what the proxy hands it.
type NopSink struct{}

// Log discards the record.
func (NopSink) Log(context.Context, Record) {}

// Close is a no-op.
func (NopSink) Close(context.Context) error { return nil }

// compile-time interface check.
var _ Sink = NopSink{}
