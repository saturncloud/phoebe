package proxy

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/saturncloud/phoebe/internal/admission"
)

// admissionWork returns the exact routed model and a conservative output-token
// reservation. Phoebe cannot render model-specific chat templates or tokenize
// without duplicating engine state, so input work is intentionally the complete
// JSON byte count; Dynamo remains authoritative for tokenization/KV placement.
func admissionWork(body []byte, defaultOutput int64) (string, int64, bool) {
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
	if maximum <= 0 {
		return "", 0, false
	}
	return v.Model, maximum, true
}

func (s *Server) writeAdmissionError(w http.ResponseWriter, err error) {
	var rejected *admission.Rejected
	if errors.As(err, &rejected) {
		retry := int64(rejected.RetryAfter.Round(time.Second) / time.Second)
		if retry < 1 {
			retry = 1
		}
		w.Header().Set("Retry-After", strconv.FormatInt(retry, 10))
		http.Error(w, "shared inference capacity is temporarily unavailable", http.StatusTooManyRequests)
		s.log.Warn.Printf("admission: rejected scope=%s dimension=%s", rejected.Scope, rejected.Dimension)
		return
	}
	http.Error(w, "shared inference admission state unavailable", http.StatusServiceUnavailable)
	s.log.Error.Printf("admission: fail closed: %v", err)
}
