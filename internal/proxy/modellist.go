package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const maxModelListBytes = 4 << 20

// filterModelListResponse limits OpenAI model discovery to the served names
// authorized for this subdomain. A dedicated Dynamo graph can advertise its
// base, internal discovery alias, and every attached adapter from one frontend;
// forwarding that graph-wide list would disclose endpoints the caller cannot
// access through Atlas.
func filterModelListResponse(resp *http.Response, servedModelAllowList string) error {
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil
	}
	allow := parseServedModelAllowList(servedModelAllowList)
	if len(allow) == 0 {
		return fmt.Errorf("model-list binding has no authorized served model")
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxModelListBytes+1))
	if err != nil {
		return fmt.Errorf("read model list: %w", err)
	}
	if err := resp.Body.Close(); err != nil {
		return fmt.Errorf("close model list: %w", err)
	}
	if len(body) > maxModelListBytes {
		return fmt.Errorf("model list exceeds %d bytes", maxModelListBytes)
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode model list: %w", err)
	}
	rawModels, ok := envelope["data"]
	if !ok {
		return fmt.Errorf("decode model list: missing data")
	}
	var models []json.RawMessage
	if err := json.Unmarshal(rawModels, &models); err != nil {
		return fmt.Errorf("decode model list data: %w", err)
	}

	filtered := make([]json.RawMessage, 0, len(models))
	for _, raw := range models {
		var model struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &model); err != nil {
			return fmt.Errorf("decode model-list entry: %w", err)
		}
		if _, authorized := allow[model.ID]; authorized {
			filtered = append(filtered, raw)
		}
	}
	encodedModels, err := json.Marshal(filtered)
	if err != nil {
		return fmt.Errorf("encode filtered model list: %w", err)
	}
	envelope["data"] = encodedModels
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode model list: %w", err)
	}

	resp.Body = io.NopCloser(bytes.NewReader(encoded))
	resp.ContentLength = int64(len(encoded))
	resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(encoded)))
	resp.Header.Del("Content-Encoding")
	return nil
}
