package proxy

import (
	"context"
	"errors"
	"fmt"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/capture"
)

// settlementKind is the closed set of admission-lease settlements. The kind is
// DECIDED where each outcome becomes known (a completed capture, a RoundTrip
// error) and TRANSLATED into lease calls exactly once, at settleAdmissionLease.
// Keeping the enum closed is the point: a settlement path must name its kind
// explicitly, so a new outcome cannot silently inherit a neighbouring path's
// settlement by falling through a boolean ladder (the pre-fix decision was
// smeared across UsageFound, isClientAbort, isPreWriteDialFailure, and an
// unmarked fall-through for the wake-exhausted cold response).
type settlementKind int

const (
	// settlementUnset is the zero value: it names no settlement path, so an
	// uninitialized kind lands in settleAdmissionLease's default branch and
	// fails closed instead of silently inheriting a real settlement's lease
	// calls. The real kinds start at 1.
	settlementUnset settlementKind = iota
	// settlementActual: the engine's usage block was captured — settle with the
	// engine-authoritative token counts.
	settlementActual
	// settlementUnknown: the request was dispatched but its usage is
	// indeterminate (engine 4xx/5xx with no usage block, client abort, mid-
	// stream fault). Retain the conservative reservation — consumed engine
	// work must not vanish from the contract windows (fail closed).
	settlementUnknown
	// settlementZeroPreWriteDial: a verified pre-write dial failure — the
	// request provably never left the process, so no engine work could have
	// been consumed. Settles zero.
	settlementZeroPreWriteDial
	// settlementZeroNeverServed: determinate never-served — the engine
	// provably did no work for this request (the cold-hold capacity rejection,
	// or the cold response served final after the wake gave up). Settles zero.
	// The requests-window +1 charged at Admit time is KEPT: that window
	// measures demand, not work.
	settlementZeroNeverServed
)

// classifyCaptureSettlement maps a COMPLETED response capture (the ModifyResponse
// path) to its settlement kind. wakeColdFinal reports that the wake round
// tripper gave up and served the engine's cold response as the final response:
// the engine refused every dispatch before any inference, so a usage-less
// capture is provably "never served", not "unknown". A captured usage block
// always wins — even a final cold response that somehow carried one settles
// actual.
func classifyCaptureSettlement(res capture.Result, wakeColdFinal bool) settlementKind {
	if res.UsageFound {
		return settlementActual
	}
	if wakeColdFinal {
		return settlementZeroNeverServed
	}
	return settlementUnknown
}

// classifyRoundTripSettlement maps a RoundTrip error (the ErrorHandler path,
// pre-response) to its settlement kind. The axis is whether the engine could
// have done work, never the HTTP status the rejection later maps to: the
// cold-hold capacity rejection settles zero whether writeAdmissionError renders
// it 503 (non-contractual scope) or 429 (contractual scope).
func classifyRoundTripSettlement(err error) settlementKind {
	var admissionErr *admissionRoundTripError
	if errors.As(err, &admissionErr) {
		// The cold-hold gate refused to extend the lease for a wake: the engine
		// answered cold (never served) and phoebe fails closed without
		// dispatching any inference work. The BeginColdHold ErrUnavailable
		// bypass never reaches this error — it continues the live lease.
		return settlementZeroNeverServed
	}
	if isClientAbort(err) {
		// The client disconnected; the engine may have consumed work before the
		// cancel reached it — indeterminate.
		return settlementUnknown
	}
	if isPreWriteDialFailure(err) {
		// The request never left the process.
		return settlementZeroPreWriteDial
	}
	// Any other transport fault is indeterminate: the upstream may have read
	// the request and consumed engine work before failing.
	return settlementUnknown
}

// settleAdmissionLease is the single boundary where a classified outcome
// becomes lease calls. Every settlement in the proxy flows through here, so
// the kind-to-lease mapping has exactly one definition; callers decide the
// kind, this function performs it. Lease settlement is first-writer-wins
// (Lease.CompleteUsage), so racing callers cannot double-settle.
//
// All zero kinds settle Complete(0): it releases every physical and token
// reservation, charges nothing, and keeps the requests-window +1 — exactly
// the never-served ruling. There is deliberately no kind that refunds the
// requests window.
func settleAdmissionLease(ctx context.Context, lease *admission.Lease, kind settlementKind, usage admission.Usage) error {
	switch kind {
	case settlementActual:
		return lease.CompleteUsage(ctx, usage)
	case settlementUnknown:
		return lease.CompleteUnknownUsage(ctx)
	case settlementZeroPreWriteDial, settlementZeroNeverServed:
		return lease.Complete(ctx, 0)
	default:
		// settlementUnset (the zero value) and anything outside the closed set
		// fail conservative here rather than silently dropping a reservation.
		return fmt.Errorf("unhandled settlement kind %d", int(kind))
	}
}
