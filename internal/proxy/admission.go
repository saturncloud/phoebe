package proxy

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/config"
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
		log.Error.Printf(format+" (+%d similar suppressed)", append(args, dropped)...)
		return
	}
	log.Error.Printf(format, args...)
}

type admissionEstimate struct {
	Model        string
	InputTokens  int64
	OutputTokens int64
}

const defaultSharedRequestBodyLimit int64 = 64 << 20

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

// sharedRequestBodyLimit returns the tightest individual body size that could
// possibly fit every applicable aggregate prompt-byte scope. R4 sentinel
// semantics: a nil (unset) dimension is unlimited and does not tighten the
// bound; neither does an explicit zero cap — no body can fit a zero-capped
// scope, and admission rejects the request there. Even when every scope is
// configured as unlimited, retain a process-safety ceiling so an
// authenticated request cannot force an unbounded io.ReadAll allocation.
func sharedRequestBodyLimit(settings config.AdmissionSettings, lane config.AdmissionLane) int64 {
	limit := int64(0)
	add := func(candidate *int64) {
		if candidate == nil || *candidate <= 0 {
			return
		}
		if limit == 0 || *candidate < limit {
			limit = *candidate
		}
	}
	add(settings.Platform.MaxPromptBytes)
	add(settings.Graph.MaxPromptBytes)
	add(settings.Organization.MaxPromptBytes)
	add(settings.OrganizationModel.MaxPromptBytes)
	if laneBytes := lane.Limits.MaxPromptBytes; laneBytes != nil && *laneBytes > 0 {
		weighted := *laneBytes
		weight := lane.Weight
		if weight < 1 {
			weight = 1
		}
		if weighted <= math.MaxInt64/weight {
			weighted *= weight
		}
		add(&weighted)
	}
	if limit == 0 {
		return defaultSharedRequestBodyLimit
	}
	return limit
}

func boundSharedRequestBody(w http.ResponseWriter, r *http.Request, limit int64) bool {
	if limit <= 0 {
		limit = defaultSharedRequestBodyLimit
	}
	if r.ContentLength > limit {
		http.Error(w, "shared inference request body too large", http.StatusRequestEntityTooLarge)
		return false
	}
	if r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, limit)
	}
	return true
}

func writeRequestBodyError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		http.Error(w, "shared inference request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	http.Error(w, "bad request body", http.StatusBadRequest)
}

// admissionWork returns the exact routed model and cheap, conservative token
// reservations. The input estimate follows the common bytes/4 approximation;
// Dynamo remains authoritative and Phoebe reconciles its actual cache-aware
// token counts after the response.
func admissionWork(body []byte, defaultOutput int64) (admissionEstimate, bool) {
	counts, err := countTopLevelKeys(body, map[string]struct{}{
		"model": {}, "max_tokens": {}, "max_completion_tokens": {},
	})
	if err != nil || counts["model"] != 1 || counts["max_tokens"] > 1 || counts["max_completion_tokens"] > 1 {
		return admissionEstimate{}, false
	}
	var v struct {
		Model               string `json:"model"`
		MaxTokens           *int64 `json:"max_tokens"`
		MaxCompletionTokens *int64 `json:"max_completion_tokens"`
	}
	if err := json.Unmarshal(body, &v); err != nil || v.Model == "" {
		return admissionEstimate{}, false
	}
	maximum := defaultOutput
	if v.MaxTokens != nil {
		maximum = *v.MaxTokens
	}
	if v.MaxCompletionTokens != nil {
		if v.MaxTokens != nil && *v.MaxTokens != *v.MaxCompletionTokens {
			return admissionEstimate{}, false
		}
		maximum = *v.MaxCompletionTokens
	}
	if maximum <= 0 || maximum > math.MaxUint32 {
		return admissionEstimate{}, false
	}
	input := int64((len(body) + 3) / 4)
	if input < 1 {
		input = 1
	}
	return admissionEstimate{Model: v.Model, InputTokens: input, OutputTokens: maximum}, true
}

// prepareSharedDynamoRequest replaces every client-controlled scheduling and
// cache-isolation value with policy derived from Saturn's trusted identity.
// Priority, strict priority and expected output length are normalized into
// nvext.agent_hints; the caller also overwrites Dynamo's higher-precedence
// priority headers. x-tenant-id has highest precedence for cache isolation;
// nvext.cache_salt is also set so the invariant remains visible in the body.
func prepareSharedDynamoRequest(body []byte, tenantIdentity string, maxOutput int64, lane config.AdmissionLane) ([]byte, string, error) {
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

func (s *Server) writeAdmissionError(w http.ResponseWriter, requestID string, err error) {
	// Echo the authoritative attempt id on rejections too — the same handle the
	// normal path stamps in ModifyResponse and the error handler stamps on 502s:
	// a rejected client needs its billing-record correlation id just as much.
	w.Header().Set(requestIDHeader, requestID)
	var rejected *admission.Rejected
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
// semantics (the Saturn UsageLimit pattern): an absent header parses to nil =
// unlimited for that dimension; an explicit "0" parses to a zero cap that
// blocks every request (fail-closed) — a forged all-zeros envelope therefore
// mints zero quota, never unlimited. There is no 0-sentinel for unlimited.
// The envelope-level completeness rule is unchanged: a present-but-partial
// envelope still fails closed.
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
	newValues := [9]string{
		id.OwnerID,
		id.OrgRateLimitRequests, id.OrgRateLimitTotalPromptTokens,
		id.OrgRateLimitUncachedPromptTokens, id.OrgRateLimitGeneratedTokens,
		id.OwnerRateLimitRequests, id.OwnerRateLimitTotalPromptTokens,
		id.OwnerRateLimitUncachedPromptTokens, id.OwnerRateLimitGeneratedTokens,
	}
	legacyValues := [5]string{
		id.LegacyServiceTier, id.LegacyRateLimitRequests,
		id.LegacyRateLimitTotalPromptTokens, id.LegacyRateLimitUncachedPromptTokens,
		id.LegacyRateLimitGeneratedTokens,
	}
	completeness := func(values []string) (present, complete bool) {
		complete = true
		for _, value := range values {
			present = present || value != ""
			complete = complete && value != ""
		}
		return present, complete
	}
	newAny, newComplete := completeness(newValues[:])
	legacyAny, legacyComplete := completeness(legacyValues[:])
	if newAny && !newComplete {
		return admission.RateLimits{}, admission.RateLimits{}, fmt.Errorf("incomplete trusted shared-inference rate-limit policy")
	}
	if !newAny {
		if !legacyAny || !legacyComplete {
			return admission.RateLimits{}, admission.RateLimits{}, fmt.Errorf("incomplete trusted shared-inference rate-limit policy")
		}
		legacy, err := parseScope(
			[4]string{identity.HeaderLegacyRateLimitRequests, identity.HeaderLegacyRateLimitTotalPromptTokens, identity.HeaderLegacyRateLimitUncachedPromptTokens, identity.HeaderLegacyRateLimitGeneratedTokens},
			[4]string{id.LegacyRateLimitRequests, id.LegacyRateLimitTotalPromptTokens, id.LegacyRateLimitUncachedPromptTokens, id.LegacyRateLimitGeneratedTokens},
		)
		return legacy, admission.RateLimits{}, err
	}
	organization, err := parseScope(
		[4]string{identity.HeaderOrgRateLimitRequests, identity.HeaderOrgRateLimitTotalPromptTokens, identity.HeaderOrgRateLimitUncachedPromptTokens, identity.HeaderOrgRateLimitGeneratedTokens},
		[4]string{id.OrgRateLimitRequests, id.OrgRateLimitTotalPromptTokens, id.OrgRateLimitUncachedPromptTokens, id.OrgRateLimitGeneratedTokens},
	)
	if err != nil {
		return organization, admission.RateLimits{}, err
	}
	owner, err := parseScope(
		[4]string{identity.HeaderOwnerRateLimitRequests, identity.HeaderOwnerRateLimitTotalPromptTokens, identity.HeaderOwnerRateLimitUncachedPromptTokens, identity.HeaderOwnerRateLimitGeneratedTokens},
		[4]string{id.OwnerRateLimitRequests, id.OwnerRateLimitTotalPromptTokens, id.OwnerRateLimitUncachedPromptTokens, id.OwnerRateLimitGeneratedTokens},
	)
	return organization, owner, err
}
