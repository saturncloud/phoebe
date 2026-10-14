package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	stdlog "log"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/gateway"
	"github.com/saturncloud/phoebe/internal/identity"
	"github.com/saturncloud/phoebe/internal/logging"
)

// admissionErrorLogEvery throttles the per-request admission error logs: a
// store outage would otherwise emit one ERROR per bypassed request and one
// per in-flight stream whose renewal fails — flooding the log exactly when
// the gate is down. The 1st and then every Nth occurrence is logged, with
// the number of suppressed occurrences appended.
const admissionErrorLogEvery = 100

// admissionErrorLogQuietGap is the silence after which sampledErrorLog treats
// the next occurrence as a NEW incident's onset — logging it — instead of
// continuing the previous incident's 1-in-N cadence. Without the reset, a
// second incident's onset stays suppressed until the counter reaches the next
// 1-mod-100 boundary, and a short second incident can leave zero ERROR lines.
const admissionErrorLogQuietGap = time.Minute

// sampledErrorLog aggregates one recurring per-request error log site so a
// sustained failure is visible at onset without flooding every request.
type sampledErrorLog struct {
	n          atomic.Int64
	suppressed atomic.Int64
	// lastUnixNano is the wall clock of the previous call, used to detect a
	// quiet gap between two incidents.
	lastUnixNano atomic.Int64

	// now and quietGap exist so a test can simulate the passage of time; the
	// zero values select the production defaults (time.Now, 1 minute).
	now      func() time.Time
	quietGap time.Duration

	// afterLoad, when non-nil, runs in logf after the lastUnixNano load and
	// before the quiet-gap check/CAS. It exists so a test can park every
	// concurrent caller at that decision point, forcing all of them to act
	// on the same pre-reset timestamp; nil in production.
	afterLoad func()

	// afterReset, when non-nil, runs in logf only on the quiet-gap CAS
	// winner, after the counters are zeroed and before the winner's first
	// increment. It exists so a test can park the winner in the window a
	// fall-through loser would land in, forcing the duplicate-onset
	// interleaving deterministically; nil in production.
	afterReset func()
}

func (l *sampledErrorLog) quietGapNanos() int64 {
	if l.quietGap > 0 {
		return l.quietGap.Nanoseconds()
	}
	return admissionErrorLogQuietGap.Nanoseconds()
}

func (l *sampledErrorLog) logf(log *logging.Logger, format string, args ...interface{}) {
	l.logTo(log.Error, format, args...)
}

// warnf is logf at WARN, for a recurring site that is routine load shedding
// rather than a fault (a full request-buffer budget).
func (l *sampledErrorLog) warnf(log *logging.Logger, format string, args ...interface{}) {
	l.logTo(log.Warn, format, args...)
}

func (l *sampledErrorLog) logTo(out *stdlog.Logger, format string, args ...interface{}) {
	now := time.Now()
	if l.now != nil {
		now = l.now()
	}
	unixNano := now.UnixNano()
	last := l.lastUnixNano.Load()
	if l.afterLoad != nil {
		l.afterLoad()
	}
	if last != 0 && unixNano-last > l.quietGapNanos() {
		// Only the goroutine that wins the timestamp update resets the
		// counters: a concurrent burst at an incident's onset must not each
		// reset and each log an onset line. Losers return without counting
		// or logging: a loser that fell through could observe n==1 after
		// the winner's reset but before the winner's own increment, logging
		// a duplicate onset. The dropped loser occurrence matches the
		// under-count a wiped loser increment already caused.
		if !l.lastUnixNano.CompareAndSwap(last, unixNano) {
			return
		}
		l.n.Store(0)
		l.suppressed.Store(0)
		if l.afterReset != nil {
			l.afterReset()
		}
	} else {
		l.lastUnixNano.Store(unixNano)
	}
	n := l.n.Add(1)
	if n > 1 && n%admissionErrorLogEvery != 0 {
		l.suppressed.Add(1)
		return
	}
	if dropped := l.suppressed.Swap(0); dropped > 0 {
		out.Printf(format+" (+%d similar suppressed)", append(args, dropped)...)
		return
	}
	out.Printf(format, args...)
}

type admissionEstimate struct {
	Model        string
	InputTokens  int64
	OutputTokens int64
	// OutputDeclared reports that the body set max_tokens or
	// max_completion_tokens; otherwise OutputTokens is the default reservation.
	OutputDeclared bool
	// OutputSequences is how many output sequences the engine generates in
	// parallel for the request: the larger of n and best_of on the routes
	// whose engine reads them (see admissionWork), at least 1. The
	// prompt's KV is shared across them, but each sequence generates up to
	// OutputTokens and needs its own output KV, so admission multiplies the
	// output by sequences in the reserved-output, contract generated-token
	// and reserved-KV charges (the KV charge is input + output x sequences).
	OutputSequences int64
}

// publishedGraphCapacity is the admission capacity phoebe enforces for graph
// (RegistryResolver.GraphCapacity: per-worker entries scaled by ready workers
// plus headroom, graph-level entries as published), or none.
func (s *Server) publishedGraphCapacity(graph string) gateway.GraphCapacity {
	if s.graphCapacity == nil || graph == "" {
		return gateway.GraphCapacity{}
	}
	return s.graphCapacity.GraphCapacity(graph)
}

func admissionLaneForIdentity(settings config.AdmissionSettings, id identity.Identity) config.AdmissionLane {
	laneName := settings.OrganizationLanes[id.OrgID]
	if laneName == "" {
		laneName = "default"
	}
	lane, ok := settings.Lanes[laneName]
	if !ok {
		lane = settings.Lanes["default"]
	}
	return lane
}

// boundRequestBody enforces the gateway-wide request body ceiling. A declared
// Content-Length over the limit is refused before any read; a chunked or
// understated body is cut off by MaxBytesReader at the limit, and the first
// reader that hits it answers 413 through writeRequestBodyError. Responses go
// through w; rawW must be the connection's own ResponseWriter, because
// MaxBytesReader marks the connection close-after-reply only through an
// unexported net/http method that a wrapper cannot forward.
func boundRequestBody(w, rawW http.ResponseWriter, r *http.Request, limit int64) bool {
	if r.ContentLength > limit {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return false
	}
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = http.MaxBytesReader(rawW, r.Body, limit)
	}
	return true
}

// writeRequestBodyError answers a failed request-body read. A read cut off
// because protection.maxStreamDuration expired (the handler sets the
// connection read deadline when it fires, so a trickled body cannot hold the
// handler past the bound) answers 408: the client was too slow to send its
// own request (504 is kept for an upstream that sent no headers in time,
// D3/D40). A full replica request-buffer budget answers phoebe's retryable
// 503 (writeRetryLaterAfter: JSON error body, request id, Retry-After, D43).
// Nothing was forwarded, so no metering row is recorded, as for every other
// pre-forward refusal.
func writeRequestBodyError(w http.ResponseWriter, r *http.Request, requestID string, err error) {
	if errors.Is(context.Cause(r.Context()), errMaxStreamDuration) {
		http.Error(w, "request body not received in time", http.StatusRequestTimeout)
		return
	}
	if errors.Is(err, errRequestBufferFull) {
		writeRetryLaterAfter(w, requestID, "The gateway request buffer is full; retry in about "+
			strconv.Itoa(requestBufferRetryAfterSeconds)+" second.", requestBufferRetryAfterSeconds)
		return
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	http.Error(w, "bad request body", http.StatusBadRequest)
}

// logRequestBodyError logs a failed request-body read before
// writeRequestBodyError answers it. A full replica request-buffer budget is
// routine load shedding that recurs on every request while the replica is
// saturated, so it is logged at WARN through a sampler (onset, then 1 in
// admissionErrorLogEvery) with a fixed countable message; every other body
// error is logged at ERROR as before.
func (s *Server) logRequestBodyError(site, requestID string, err error) {
	if errors.Is(err, errRequestBufferFull) {
		s.requestBufferFullLog.warnf(s.log, "%s: replica request-buffer budget full; refused 503 request_id=%s", site, requestID)
		return
	}
	s.log.Error.Printf("%s: %v (request_id=%s)", site, err, requestID)
}

// admissionInterpretedFields are the top-level request fields admission reads.
var admissionInterpretedFields = []string{"model", "max_tokens", "max_completion_tokens", "messages", "prompt", "input", "n", "best_of"}

// admissionWork returns the exact routed model and cheap token reservations.
// The input estimate is message-text characters / 4 (estimatePromptTokens);
// Dynamo remains authoritative and Phoebe reconciles its actual cache-aware
// token counts after the response. The output sequence count is
// max(n, best_of, 1), times the number of prompts in a batched prompt
// (promptBatchCount) on /v1/completions only, since the completions engine
// generates for each prompt. Other routes (chat completions, embeddings)
// ignore a top-level "prompt", so it never multiplies their sequences (D27).
// Likewise best_of counts only on /v1/completions and n only on completions
// and chat completions; a route that ignores the field is never charged for
// it (D40).
// n, best_of, max_tokens and max_completion_tokens are read like the engine's
// lax integer validation (laxInt, ledger D32): integers, integral floats such
// as 2.0 and integer strings such as "4" count at their value; a fraction, a
// non-numeric string or a bool is refused 400.
func admissionWork(body []byte, defaultOutput int64, routePath string) (admissionEstimate, bool) {
	// Every field the estimate and the routed model are read from must be
	// unambiguous: no duplicates and no case variants (see
	// checkTopLevelKeyAmbiguity), or phoebe could admit against one value
	// while the engine processes another.
	counts, err := checkTopLevelKeyAmbiguity(body, admissionInterpretedFields)
	if err != nil || counts["model"] != 1 {
		return admissionEstimate{}, false
	}
	var v struct {
		Model               string          `json:"model"`
		MaxTokens           json.RawMessage `json:"max_tokens"`
		MaxCompletionTokens json.RawMessage `json:"max_completion_tokens"`
		N                   json.RawMessage `json:"n"`
		BestOf              json.RawMessage `json:"best_of"`
		Prompt              json.RawMessage `json:"prompt"`
	}
	if err := json.Unmarshal(body, &v); err != nil || v.Model == "" {
		return admissionEstimate{}, false
	}
	maxTokens, hasMaxTokens, ok1 := laxInt(v.MaxTokens)
	maxCompletion, hasMaxCompletion, ok2 := laxInt(v.MaxCompletionTokens)
	n, hasN, ok3 := laxInt(v.N)
	bestOf, hasBestOf, ok4 := laxInt(v.BestOf)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return admissionEstimate{}, false
	}
	maximum := defaultOutput
	if hasMaxTokens {
		maximum = maxTokens
	}
	if hasMaxCompletion {
		if hasMaxTokens && maxTokens != maxCompletion {
			return admissionEstimate{}, false
		}
		maximum = maxCompletion
	}
	if maximum <= 0 || maximum > math.MaxUint32 {
		return admissionEstimate{}, false
	}
	// Each count multiplies only on a route whose engine reads it: n on
	// completions and chat completions, best_of on completions alone.
	// Embeddings generate nothing per sequence and chat completions has no
	// best_of, so there the field is still validated above (a malformed
	// value is a 400) but never charged (D40).
	hasN = hasN && (routePath == "/v1/completions" || routePath == "/v1/chat/completions")
	hasBestOf = hasBestOf && routePath == "/v1/completions"
	sequences := int64(1)
	for _, c := range []struct {
		value int64
		set   bool
	}{{n, hasN}, {bestOf, hasBestOf}} {
		if !c.set {
			continue
		}
		// A count the engine would refuse (< 1) is not ours to judge; one
		// past maxOutputSequences cannot be a real request and would
		// overflow the KV charge, so it is refused like a broken body.
		if c.value > maxOutputSequences {
			return admissionEstimate{}, false
		}
		if c.value > sequences {
			sequences = c.value
		}
	}
	// A batched completions prompt is one generation per prompt. Only the
	// completions route reads "prompt"; elsewhere the engine ignores it.
	// Both factors are bounded by maxOutputSequences before multiplying, so
	// the product cannot overflow, and the product is held to the same bound.
	prompts := int64(1)
	if routePath == "/v1/completions" {
		prompts = promptBatchCount(v.Prompt)
	}
	if prompts > maxOutputSequences {
		return admissionEstimate{}, false
	}
	sequences *= prompts
	if sequences > maxOutputSequences {
		return admissionEstimate{}, false
	}
	if maximum*sequences > maxReservedOutput {
		return admissionEstimate{}, false
	}
	return admissionEstimate{Model: v.Model, InputTokens: estimatePromptTokens(body), OutputTokens: maximum,
		OutputDeclared: hasMaxTokens || hasMaxCompletion, OutputSequences: sequences}, true
}

// laxInt reads a count field the way the engine's request validation does
// (pydantic's lax integer mode): a JSON integer, a number with no fractional
// part (2.0, 1e0) or a string holding an integer ("4") is that integer. Absent
// or null reports set=false. Anything else (a fraction, a non-numeric string,
// a bool) reports ok=false: the engine would refuse it too, and phoebe must
// never charge one value while the engine runs another.
func laxInt(raw json.RawMessage) (value int64, set, ok bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, false, true
	}
	var number json.Number
	if json.Unmarshal(raw, &number) != nil {
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return 0, true, false
		}
		number = json.Number(strings.TrimSpace(text))
	}
	if i, err := number.Int64(); err == nil {
		return i, true, true
	}
	f, err := number.Float64()
	if err != nil || f != math.Trunc(f) || math.Abs(f) > 1<<53 {
		return 0, true, false
	}
	return int64(f), true, true
}

// prepareSharedDynamoRequest replaces every client-controlled scheduling and
// cache-isolation value with policy derived from Saturn's trusted identity.
// Priority, strict priority and expected output length are normalized into
// nvext.agent_hints; the caller also overwrites Dynamo's higher-precedence
// priority headers. x-tenant-id has highest precedence for cache isolation;
// nvext.cache_salt is also set so the invariant remains visible in the body.
//
// capOutput sets max_tokens to maxOutput in the forwarded body. The caller
// passes it when the client declared no max_tokens: the reservation phoebe
// admits against (the R10 default, clamped to fit the applicable limits) is
// then also the cap the engine enforces, as ruling R10 defines it, instead of
// the engine generating up to its own default past what was reserved.
func prepareSharedDynamoRequest(body []byte, tenantIdentity string, maxOutput int64, capOutput bool, lane config.AdmissionLane) ([]byte, string, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil || root == nil {
		return nil, "", fmt.Errorf("request body must be a JSON object")
	}
	nvext := map[string]json.RawMessage{}
	if raw, ok := root["nvext"]; ok {
		_ = json.Unmarshal(raw, &nvext)
		if nvext == nil {
			nvext = map[string]json.RawMessage{}
		}
	}
	hints := map[string]json.RawMessage{}
	if raw, ok := nvext["agent_hints"]; ok {
		_ = json.Unmarshal(raw, &hints)
		if hints == nil {
			hints = map[string]json.RawMessage{}
		}
	}
	// Direct worker/rank selection bypasses load- and cache-aware routing.
	// Pre-tokenized input bypasses Dynamo's authoritative rendering/tokenization.
	// Speculative prefill creates unreserved background engine work. None are
	// safe client controls on a shared multi-tenant graph.
	for _, key := range []string{
		"backend_instance_id", "prefill_worker_id", "decode_worker_id",
		"dp_rank", "prefill_dp_rank", "token_data",
	} {
		delete(nvext, key)
	}
	delete(hints, "speculative_prefill")

	tenantHash := sha256.Sum256([]byte("phoebe-dynamo-tenant\x00" + tenantIdentity))
	tenant := fmt.Sprintf("saturn-%x", tenantHash[:])
	setJSON := func(dst map[string]json.RawMessage, key string, value any) error {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		dst[key] = raw
		return nil
	}
	if err := setJSON(nvext, "cache_salt", tenant); err != nil {
		return nil, "", err
	}
	if err := setJSON(root, "cache_salt", tenant); err != nil {
		return nil, "", err
	}
	if err := setJSON(hints, "priority", lane.DynamoPriority); err != nil {
		return nil, "", err
	}
	if err := setJSON(hints, "strict_priority", lane.DynamoStrictPriority); err != nil {
		return nil, "", err
	}
	if capOutput {
		if err := setJSON(root, "max_tokens", maxOutput); err != nil {
			return nil, "", err
		}
	}
	if err := setJSON(hints, "osl", maxOutput); err != nil {
		return nil, "", err
	}
	hintsRaw, err := json.Marshal(hints)
	if err != nil {
		return nil, "", err
	}
	nvext["agent_hints"] = hintsRaw
	nvextRaw, err := json.Marshal(nvext)
	if err != nil {
		return nil, "", err
	}
	root["nvext"] = nvextRaw
	out, err := json.Marshal(root)
	if err != nil {
		return nil, "", err
	}
	if withUsage, changed := rewriteIncludeUsage(out); changed {
		out = withUsage
	}
	return out, tenant, nil
}

// outputShape describes the output a request reserved, so a refusal names
// the term the client controls. hasOutput is false for embeddings, which
// have no client-declared output (phoebe reserves one token on its own);
// declared reports that the client sent max_tokens or max_completion_tokens;
// sequences is the output sequence count (n, best_of, batched prompts).
type outputShape struct {
	hasOutput, declared bool
	sequences           int64
}

// declaredSingleOutput is the shape for a refusal that does not depend on
// the request's output terms.
var declaredSingleOutput = outputShape{hasOutput: true, declared: true, sequences: 1}

// sequenceTerms names the client fields that set the output sequence count.
const sequenceTerms = "(n, best_of, or batched prompts)"

// oversizeMessage is the 413 text for a request larger than an operator or
// published capacity scope. Only the graph's own capacity (the published
// graph_capacity scope and the per-graph operator tier) is described as the
// model's; every other scope (platform, organization, organization_model,
// lane) gets neutral wording so the client is not told the model cannot take
// a request that a configured limit refused. The scope id itself stays in the
// server log, not the client body.
//
// The KV text names the terms of the reservation the client controls: the
// prompt alone for embeddings (which have no client-declared output) and for
// an undeclared max_tokens with one sequence; the prompt plus one output token
// per sequence for an undeclared max_tokens with several sequences; the
// prompt plus max_tokens, times the sequence count when it is above one, for
// a declared max_tokens. A client is never told to lower a field it did not
// send.
func oversizeMessage(rejected *admission.Rejected, out outputShape) string {
	limit := "a configured KV-cache limit"
	prefill := "a configured prefill-token limit"
	output := "a configured reserved-output limit"
	if rejected.Scope == "graph_capacity" || rejected.Scope == "graph" {
		limit = "this model's KV-cache capacity"
		prefill = "this model's prefill capacity"
		output = "this model's reserved-output capacity"
	}
	if rejected.Dimension == "output_exceeds_limit" {
		return outputOversizeMessage(out, output)
	}
	if rejected.Dimension != "kv_exceeds_limit" {
		return "estimated prompt tokens exceed " + prefill
	}
	switch {
	case !out.hasOutput:
		return "estimated prompt tokens exceed " + limit
	case out.declared && out.sequences > 1:
		return "estimated prompt tokens plus max_tokens times the number of output sequences " + sequenceTerms + " exceed " + limit
	case out.declared:
		return "estimated prompt tokens plus max_tokens exceed " + limit
	case out.sequences > 1:
		return "estimated prompt tokens plus one output token per requested sequence " + sequenceTerms + " exceed " + limit
	default:
		return "estimated prompt tokens exceed " + limit
	}
}

// outputOversizeMessage is the 413 text for a request whose own reserved
// output (max_tokens x output sequences, D29) exceeds a positive capacity
// cap. Like the KV text it names only the fields the client sent: an
// undeclared max_tokens is the one-token clamp, so the sequence count is the
// term to lower.
func outputOversizeMessage(out outputShape, limit string) string {
	switch {
	case out.declared && out.sequences > 1:
		return "max_tokens times the number of output sequences " + sequenceTerms + " exceeds " + limit
	case out.declared:
		return "max_tokens exceeds " + limit
	case out.sequences > 1:
		return "the number of output sequences " + sequenceTerms + " exceeds " + limit
	default:
		return "the minimum one-token output reservation exceeds " + limit
	}
}

// generatedLimitMessage is the 400 text for a request whose generated-token
// charge (max_tokens x output sequences, D29) alone exceeds a contract's
// per-window limit. It names the sequence count when it is above one, and
// never blames max_tokens when the client did not send it.
func generatedLimitMessage(out outputShape) string {
	switch {
	case out.declared && out.sequences > 1:
		return "max_tokens times the number of output sequences " + sequenceTerms + " exceeds the per-window generated-token limit"
	case out.declared:
		return "max_tokens exceeds the per-window generated-token limit"
	case out.sequences > 1:
		return "the number of output sequences " + sequenceTerms + " exceeds the per-window generated-token limit"
	default:
		return "the minimum one-token output reservation exceeds the per-window generated-token limit"
	}
}

// writeAdmissionError answers an admission refusal that does not depend on
// the request's output terms (a cold-hold or lease-renewal refusal): the
// wording is that of a declared max_tokens with one sequence.
func (s *Server) writeAdmissionError(w http.ResponseWriter, requestID string, err error) {
	s.writeAdmissionErrorOutput(w, requestID, err, declaredSingleOutput)
}

// writeAdmissionErrorOutput answers an admission refusal; out selects the
// 413 KV and 400 generated-token wording (oversizeMessage,
// generatedLimitMessage).
func (s *Server) writeAdmissionErrorOutput(w http.ResponseWriter, requestID string, err error, out outputShape) {
	// Echo the authoritative attempt id on rejections too — the same handle the
	// normal path stamps in ModifyResponse and the error handler stamps on 502s:
	// a rejected client needs its billing-record correlation id just as much.
	w.Header().Set(requestIDHeader, requestID)
	var rejected *admission.Rejected
	if errors.As(err, &rejected) && rejected.Unsatisfiable && !rejected.Contractual {
		// One request alone is larger than a capacity limit (estimated prompt
		// tokens, prompt plus max_tokens against reserved KV, or max_tokens x
		// output sequences against reserved output). It can never
		// be admitted, so it is refused as too large, the same answer an
		// oversized prompt got from the byte-based limit this replaced.
		http.Error(w, oversizeMessage(rejected, out), http.StatusRequestEntityTooLarge)
		s.log.Warn.Printf("admission: request larger than capacity scope=%s dimension=%s", rejected.Scope, rejected.Dimension)
		return
	}
	if errors.As(err, &rejected) && rejected.Unsatisfiable {
		// One request alone exceeds a contract's whole per-window limit, so
		// waiting can never admit it: no Retry-After.
		message := "shared inference request exceeds the per-window contract limit"
		switch rejected.Dimension {
		case "generated_tokens_exceeds_limit":
			message = generatedLimitMessage(out)
		case "total_prompt_tokens_exceeds_limit", "uncached_prompt_tokens_exceeds_limit":
			message = "estimated prompt tokens exceed the per-window prompt-token limit"
		}
		http.Error(w, message, http.StatusBadRequest)
		s.log.Warn.Printf("admission: unsatisfiable request scope=%s dimension=%s", rejected.Scope, rejected.Dimension)
		return
	}
	if errors.As(err, &rejected) {
		retry := int64(math.Ceil(rejected.RetryAfter.Seconds()))
		if retry < 1 {
			retry = 1
		}
		w.Header().Set("Retry-After", strconv.FormatInt(retry, 10))
		status := http.StatusServiceUnavailable
		message := "shared inference capacity is temporarily unavailable"
		if rejected.Contractual {
			status = http.StatusTooManyRequests
			message = "shared inference rate limit exceeded"
		}
		http.Error(w, message, status)
		s.log.Warn.Printf("admission: rejected scope=%s dimension=%s", rejected.Scope, rejected.Dimension)
		return
	}
	http.Error(w, "shared inference admission state unavailable", http.StatusServiceUnavailable)
	s.log.Error.Printf("admission: unexpected error: %v", err)
}

// parseTrustedRateLimits parses the trusted quota envelope. R4 sentinel
// semantics (the Saturn UsageLimit pattern): an absent limit header parses to
// nil = unlimited for that field; an explicit "0" parses to a zero cap that
// blocks every request (fail-closed) — a forged all-zeros envelope therefore
// mints zero quota, never unlimited. There is no 0-sentinel for unlimited.
//
// R4 x R7 reconciliation: the completeness gate covers the IDENTITY anchor
// only — X-Saturn-Owner-Id. The 8 scoped rate-limit headers are per-field:
// Atlas omits headers for unset UsageLimits, and that absence is the R4
// unlimited encoding, not an R7 violation. What still fails closed (503):
// limit headers present WITHOUT the owner-id anchor, a present-but-malformed
// value (non-numeric/negative), an uncached-above-total relation, and an
// entirely absent policy.
//
// R8: there is no legacy single-scope envelope. The five legacy headers
// (X-Saturn-Service-Tier, X-Saturn-Rate-Limit-*) are not read at all, so a
// request carrying only those and no owner id has no policy and fails closed.
func parseTrustedRateLimits(id identity.Identity) (admission.RateLimits, admission.RateLimits, error) {
	parse := func(name, value string) (*int64, error) {
		if value == "" {
			return nil, nil
		}
		limit, err := strconv.ParseInt(value, 10, 64)
		if err != nil || limit < 0 {
			return nil, fmt.Errorf("invalid trusted %s header", name)
		}
		return &limit, nil
	}
	parseScope := func(names, values [4]string) (admission.RateLimits, error) {
		var out admission.RateLimits
		var err error
		if out.Requests, err = parse(names[0], values[0]); err != nil {
			return out, err
		}
		if out.TotalPromptTokens, err = parse(names[1], values[1]); err != nil {
			return out, err
		}
		if out.UncachedPromptTokens, err = parse(names[2], values[2]); err != nil {
			return out, err
		}
		if out.GeneratedTokens, err = parse(names[3], values[3]); err != nil {
			return out, err
		}
		if out.TotalPromptTokens != nil && out.UncachedPromptTokens != nil &&
			*out.UncachedPromptTokens > *out.TotalPromptTokens {
			return out, fmt.Errorf("trusted uncached prompt limit exceeds total prompt limit")
		}
		return out, nil
	}
	orgValues, ownerValues := id.ScopedRateLimitValues()

	switch {
	case id.OwnerID != "":
		// Scoped envelope: the owner id is the structural anchor (R7). The 8
		// scoped headers are per-field R4 — absent is unlimited.
		organization, err := parseScope(identity.OrgScopedRateLimitHeaders, orgValues)
		if err != nil {
			return organization, admission.RateLimits{}, err
		}
		owner, err := parseScope(identity.OwnerScopedRateLimitHeaders, ownerValues)
		return organization, owner, err
	case anyScopedLimitPresent(id):
		// Scoped limit headers without the owner-id anchor are a structural
		// violation (R7), not unlimited fields.
		return admission.RateLimits{}, admission.RateLimits{}, fmt.Errorf("incomplete trusted shared-inference rate-limit policy: scoped headers without %s", identity.HeaderOwnerID)
	default:
		// No identity anchor and no policy at all: a structural violation
		// (R7) that fails closed.
		return admission.RateLimits{}, admission.RateLimits{}, errNoTrustedRateLimitPolicy
	}
}

// anyScopedLimitPresent reports whether any scoped contract limit header
// reached this request.
func anyScopedLimitPresent(id identity.Identity) bool {
	org, owner := id.ScopedRateLimitValues()
	for _, values := range [][4]string{org, owner} {
		for _, value := range values {
			if value != "" {
				return true
			}
		}
	}
	return false
}

// trustedPolicyEnvelopePresent reports whether any part of the trusted quota
// envelope — the owner-id anchor, any scoped limit header, or the group scope
// envelope — reached this request. Any part of it engages admission's
// fail-closed checks even under admission.enabled=false: a missing
// organization identity, a malformed or partial envelope (a limit header
// without the owner-id anchor fails closed in parseTrustedRateLimits; a
// malformed group scope envelope fails closed in parseTrustedGroupScopes),
// and an underivable graph scope all answer 503.
// Under admission.enabled=false the admission store itself is only consulted
// when a limit is present to enforce — a contract limit header or a group
// scope carrying a rate limit (see the Admit call in proxy.go).
func trustedPolicyEnvelopePresent(id identity.Identity) bool {
	return id.OwnerID != "" || anyScopedLimitPresent(id) || id.GroupScopes != ""
}

// undeclaredOutputReservation is the per-sequence output-token reservation
// for a request that declares neither max_tokens nor max_completion_tokens:
// the configured default, clamped so the reservation times sequences fits
// the smallest positive contract generated-token limit (admission charges
// the generated-token window output x sequences), so that an undeclared
// request always fits an otherwise empty window. An explicit zero cap does
// not clamp; that scope rejects in admission. The result is at least 1; a
// request whose sequence count alone passes a limit is refused for its n.
func undeclaredOutputReservation(defaultOutput, sequences int64, scopes ...admission.RateLimits) int64 {
	if sequences < 1 {
		sequences = 1
	}
	out := defaultOutput
	for _, limits := range scopes {
		if g := limits.GeneratedTokens; g != nil && *g > 0 && *g/sequences < out {
			out = *g / sequences
		}
	}
	if out < 1 {
		out = 1
	}
	return out
}

// maxOutputSequences bounds n and best_of: far above any engine's limit, and
// small enough that output x sequences cannot overflow the KV charge
// (output is at most MaxUint32).
const maxOutputSequences = 1 << 16

// maxReservedOutput bounds output x sequences, the reserved-output part of the
// KV charge. Redis's Lua cjson encodes numbers with only 14 significant
// digits, so a charge at or above 1e14 could not be written to a lease record
// as a JSON number without rounding. 2^46 (about 7.0e13) keeps the whole
// charge, prompt estimate included, below 1e14; no real request comes near it
// (it is max_tokens near 2^32 with n=16384). A larger product is refused like
// a broken body. The lease script also stores kv as a decimal string, so this
// bound is the first of two guards, not the only one.
const maxReservedOutput = 1 << 46

// undeclaredOutputWithinCaps clamps the output reservation of a request
// that declared no max_tokens so it fits every positive per-request-bounding
// cap that applies: the prompt plus the reservation (times sequences) fits
// kvCap, the tightest positive reserved-KV cap, and the reservation times
// sequences fits outputCap, the tightest positive reserved-output cap
// (admission charges the output counter output x sequences). 0 means no cap. Like
// the contract clamp above, a request is never refused for a default the
// client did not choose. The result is at least 1; when the prompt alone
// fills the KV cap the request is still refused for its prompt (413).
func undeclaredOutputWithinCaps(output, input, sequences, kvCap, outputCap int64) int64 {
	if sequences < 1 {
		sequences = 1
	}
	if kvCap > 0 {
		if room := (kvCap - input) / sequences; room < output {
			output = room
		}
	}
	if outputCap > 0 && outputCap/sequences < output {
		output = outputCap / sequences
	}
	if output < 1 {
		output = 1
	}
	return output
}

// admitterSettings is the admission configuration the admitter enforces, as
// reported by the admitter; with no admitter there are no operator tiers.
func (s *Server) admitterSettings() config.AdmissionSettings {
	if s.admitter == nil {
		return config.AdmissionSettings{}
	}
	return s.admitter.Settings()
}

// errNoTrustedRateLimitPolicy is the parser's "no anchor and no scoped policy
// at all" result. The proxy call site matches it to add a log-only diagnostic
// for pre-R8 producers (see legacyQuotaHeadersPresent).
var errNoTrustedRateLimitPolicy = errors.New("incomplete trusted shared-inference rate-limit policy")

// The group scope envelope (X-Saturn-Group-Scopes) bounds, frozen with the
// grammar (see identity.HeaderGroupScopes): at most 16 entries and the whole
// header under 4 KiB. Exceeding either, like any malformed field, is a 503 at
// the proxy with a loud log — the fail-visible default for the multi-group
// case: usage counts against EVERY group the caller belongs to that carries
// limits, and the envelope must never silently drop one.
const (
	maxGroupScopeEntries = 16
	maxGroupScopeBytes   = 4096
)

// groupIDRe pins a group id to 32 lowercase hex chars (uuid4 hex, as Atlas
// mints). The frozen envelope pins this grammar; anything else is malformed.
var groupIDRe = regexp.MustCompile(`^[0-9a-f]{32}$`)

// groupRateRe pins a rate field to the frozen grammar: "0" (zero cap) or a
// positive unpadded decimal. strconv.ParseInt alone also accepts "+30",
// "-0" and "0030", which the grammar forbids — an encoder bug there must
// fail closed (503), not be silently enforced.
var groupRateRe = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

// parseTrustedGroupScopes parses the trusted group quota envelope
// (X-Saturn-Scopes grammar: v1;<gid>:<req>,<tot>,<unc>,<gen>,<spend>;...).
// R4 sentinels exactly as parseTrustedRateLimits: an empty rate field is
// unlimited (nil), "0" is a zero cap, a positive value is the per-minute cap.
// The parse is STRICT — a malformed entry, an out-of-grammar gid, a
// duplicate gid, an uncached-above-total relation, more than
// maxGroupScopeEntries entries, or a header at/over maxGroupScopeBytes is an
// error and the request fails closed (503), like every other envelope
// violation. An absent header parses to nil scopes (no group quotas on this
// request), and "v1" alone parses to zero groups.
//
// THE SPEND FIELD IS THE PARKED-ACTIVATION BOUNDARY: the fifth field is
// reserved for the deferred monthly spend cap and MUST be empty in this
// build. A non-empty spend field is malformed and fails closed (503) with a
// loud log, whatever its shape — activating spend caps later is a parser
// change at this one site, not an envelope renegotiation.
//
// TRUST: the value is read by identity.FromRequest ONLY when the header is in
// the active trusted set, so a client can never stamp its own group quotas.
func parseTrustedGroupScopes(id identity.Identity) ([]admission.GroupScope, error) {
	raw := id.GroupScopes
	if raw == "" {
		return nil, nil
	}
	if len(raw) >= maxGroupScopeBytes {
		return nil, fmt.Errorf("trusted %s header exceeds the %d-byte envelope bound (%d bytes)", identity.HeaderGroupScopes, maxGroupScopeBytes, len(raw))
	}
	parts := strings.Split(raw, ";")
	if parts[0] != "v1" {
		return nil, fmt.Errorf("trusted %s header missing the v1 prefix", identity.HeaderGroupScopes)
	}
	entries := parts[1:]
	if len(entries) > maxGroupScopeEntries {
		return nil, fmt.Errorf("trusted %s header carries %d group entries, over the %d-entry bound", identity.HeaderGroupScopes, len(entries), maxGroupScopeEntries)
	}
	scopes := make([]admission.GroupScope, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		fields := strings.Split(entry, ":")
		if len(fields) != 2 {
			return nil, fmt.Errorf("trusted %s entry %q is not <gid>:<req>,<tot>,<unc>,<gen>,<spend>", identity.HeaderGroupScopes, entry)
		}
		gid := fields[0]
		if !groupIDRe.MatchString(gid) {
			return nil, fmt.Errorf("trusted %s entry has a malformed group id %q (want 32 lowercase hex)", identity.HeaderGroupScopes, gid)
		}
		if _, dup := seen[gid]; dup {
			return nil, fmt.Errorf("trusted %s header lists group %s twice", identity.HeaderGroupScopes, gid)
		}
		seen[gid] = struct{}{}
		rates := strings.Split(fields[1], ",")
		if len(rates) != 5 {
			return nil, fmt.Errorf("trusted %s entry for group %s has %d fields, want <req>,<tot>,<unc>,<gen>,<spend>", identity.HeaderGroupScopes, gid, len(rates))
		}
		var scope admission.GroupScope
		scope.GroupID = gid
		var err error
		if scope.Limits.Requests, err = parseGroupRate(gid, "requests", rates[0]); err != nil {
			return nil, err
		}
		if scope.Limits.TotalPromptTokens, err = parseGroupRate(gid, "total prompt tokens", rates[1]); err != nil {
			return nil, err
		}
		if scope.Limits.UncachedPromptTokens, err = parseGroupRate(gid, "uncached prompt tokens", rates[2]); err != nil {
			return nil, err
		}
		if scope.Limits.GeneratedTokens, err = parseGroupRate(gid, "generated tokens", rates[3]); err != nil {
			return nil, err
		}
		if scope.Limits.TotalPromptTokens != nil && scope.Limits.UncachedPromptTokens != nil &&
			*scope.Limits.UncachedPromptTokens > *scope.Limits.TotalPromptTokens {
			return nil, fmt.Errorf("trusted %s group %s: uncached prompt limit exceeds total prompt limit", identity.HeaderGroupScopes, gid)
		}
		// The fifth field is reserved for the deferred monthly spend cap: it
		// MUST be empty in this build. A non-empty value — whatever its shape —
		// is malformed and fails closed (the parked-activation boundary; see
		// the parseTrustedGroupScopes doc).
		if rates[4] != "" {
			return nil, fmt.Errorf("trusted %s group %s carries a non-empty spend field %q (monthly spend caps are not active in this build)", identity.HeaderGroupScopes, gid, rates[4])
		}
		scopes = append(scopes, scope)
	}
	return scopes, nil
}

// parseGroupRate parses one group envelope rate field: empty is unlimited
// (nil), "0" is a zero cap, a positive integer is the per-minute cap. A
// signed, zero-padded, negative, or non-integer value is malformed and fails
// closed.
func parseGroupRate(gid, dimension, value string) (*int64, error) {
	if value == "" {
		return nil, nil
	}
	if !groupRateRe.MatchString(value) {
		return nil, fmt.Errorf("trusted %s group %s has a malformed %s limit %q", identity.HeaderGroupScopes, gid, dimension, value)
	}
	limit, err := strconv.ParseInt(value, 10, 64)
	if err != nil || limit < 0 {
		return nil, fmt.Errorf("trusted %s group %s has a malformed %s limit %q", identity.HeaderGroupScopes, gid, dimension, value)
	}
	return &limit, nil
}

// anyGroupScopeEnforced reports whether at least one group scope carries a
// non-nil per-minute rate limit. An all-unlimited group envelope (every rate
// empty) enforces nothing, so under admission.enabled=false it must not touch
// the admission store — exactly like a request with no envelope.
func anyGroupScopeEnforced(scopes []admission.GroupScope) bool {
	for _, scope := range scopes {
		if scope.Limits.Any() {
			return true
		}
	}
	return false
}

// legacyQuotaHeaderNames are the five single-scope quota headers that R8
// removed from the trusted envelope. Phoebe never reads them for a trust or
// limit decision; they are listed here ONLY so the proxy can log that a
// not-yet-upgraded producer (Atlas or Traefik) is still stamping them. Remove
// this diagnostic one release after the R8 cutover.
var legacyQuotaHeaderNames = []string{
	"X-Saturn-Service-Tier",
	"X-Saturn-Rate-Limit-Requests",
	"X-Saturn-Rate-Limit-Total-Prompt-Tokens",
	"X-Saturn-Rate-Limit-Uncached-Prompt-Tokens",
	"X-Saturn-Rate-Limit-Generated-Tokens",
}

// legacyQuotaHeadersPresent reports whether the raw request carries any of the
// removed legacy quota headers. It is a logging diagnostic only, and must be
// called before StripSaturnHeaders removes them.
func legacyQuotaHeadersPresent(h http.Header) bool {
	for _, name := range legacyQuotaHeaderNames {
		if h.Get(name) != "" {
			return true
		}
	}
	return false
}
