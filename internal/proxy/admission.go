package proxy

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/saturncloud/phoebe/internal/admission"
	"github.com/saturncloud/phoebe/internal/config"
	"github.com/saturncloud/phoebe/internal/identity"
)

// admissionWork returns the exact routed model and a conservative output-token
// reservation. Phoebe cannot render model-specific chat templates or tokenize
// without duplicating engine state, so input work is intentionally the complete
// JSON byte count; Dynamo remains authoritative for tokenization/KV placement.
func admissionWork(body []byte, defaultOutput int64) (string, int64, bool) {
	counts, err := countTopLevelKeys(body, map[string]struct{}{
		"model": {}, "max_tokens": {}, "max_completion_tokens": {},
	})
	if err != nil || counts["model"] != 1 || counts["max_tokens"] > 1 || counts["max_completion_tokens"] > 1 {
		return "", 0, false
	}
	var v struct {
		Model               string `json:"model"`
		MaxTokens           *int64 `json:"max_tokens"`
		MaxCompletionTokens *int64 `json:"max_completion_tokens"`
	}
	if err := json.Unmarshal(body, &v); err != nil || v.Model == "" {
		return "", 0, false
	}
	maximum := defaultOutput
	if v.MaxTokens != nil {
		maximum = *v.MaxTokens
	}
	if v.MaxCompletionTokens != nil {
		if v.MaxTokens != nil && *v.MaxTokens != *v.MaxCompletionTokens {
			return "", 0, false
		}
		maximum = *v.MaxCompletionTokens
	}
	if maximum <= 0 || maximum > math.MaxUint32 {
		return "", 0, false
	}
	return v.Model, maximum, true
}

// prepareSharedDynamoRequest replaces every client-controlled scheduling and
// cache-isolation value with policy derived from Saturn's trusted identity.
// Priority, strict priority and expected output length are normalized into
// nvext.agent_hints; the caller also overwrites Dynamo's higher-precedence
// priority headers. x-tenant-id has highest precedence for cache isolation;
// nvext.cache_salt is also set so the invariant remains visible in the body.
func prepareSharedDynamoRequest(body []byte, org string, maxOutput int64, tier config.AdmissionTier) ([]byte, string, error) {
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

	tenantHash := sha256.Sum256([]byte("phoebe-dynamo-tenant\x00" + org))
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
	if err := setJSON(hints, "priority", tier.DynamoPriority); err != nil {
		return nil, "", err
	}
	if err := setJSON(hints, "strict_priority", tier.DynamoStrictPriority); err != nil {
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

func (s *Server) writeAdmissionError(w http.ResponseWriter, err error) {
	var rejected *admission.Rejected
	if errors.As(err, &rejected) {
		retry := int64(rejected.RetryAfter.Round(time.Second) / time.Second)
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
	s.log.Error.Printf("admission: fail closed: %v", err)
}

func parseTrustedRateLimits(id identity.Identity) (admission.RateLimits, error) {
	if id.Gateway && (id.ServiceTier == "" || id.RateLimitRequests == "" ||
		id.RateLimitTotalPromptTokens == "" || id.RateLimitUncachedPromptTokens == "" ||
		id.RateLimitGeneratedTokens == "") {
		return admission.RateLimits{}, fmt.Errorf("incomplete trusted gateway rate-limit policy")
	}
	parse := func(name, value string) (int64, error) {
		if value == "" {
			return 0, nil
		}
		limit, err := strconv.ParseInt(value, 10, 64)
		if err != nil || limit < 0 {
			return 0, fmt.Errorf("invalid trusted %s header", name)
		}
		return limit, nil
	}
	var out admission.RateLimits
	var err error
	if out.Requests, err = parse(identity.HeaderRateLimitRequests, id.RateLimitRequests); err != nil {
		return out, err
	}
	if out.TotalPromptTokens, err = parse(identity.HeaderRateLimitTotalPromptTokens, id.RateLimitTotalPromptTokens); err != nil {
		return out, err
	}
	if out.UncachedPromptTokens, err = parse(identity.HeaderRateLimitUncachedPromptTokens, id.RateLimitUncachedPromptTokens); err != nil {
		return out, err
	}
	if out.GeneratedTokens, err = parse(identity.HeaderRateLimitGeneratedTokens, id.RateLimitGeneratedTokens); err != nil {
		return out, err
	}
	if out.TotalPromptTokens > 0 && out.UncachedPromptTokens > out.TotalPromptTokens {
		return out, fmt.Errorf("trusted uncached prompt limit exceeds total prompt limit")
	}
	return out, nil
}
