package proxy

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const maxModelListBytes = 4 << 20

type safeModelListing struct {
	ID              string  `json:"id"`
	Object          string  `json:"object,omitempty"`
	Created         int64   `json:"created,omitempty"`
	OwnedBy         string  `json:"owned_by,omitempty"`
	ContextWindow   *uint64 `json:"context_window,omitempty"`
	MaxOutputTokens *uint64 `json:"max_output_tokens,omitempty"`
}

type safeModelList struct {
	Object string             `json:"object"`
	Data   []safeModelListing `json:"data"`
}

// filterModelListResponse limits OpenAI model discovery to the served names
// authorized for this subdomain. A dedicated Dynamo graph can advertise its
// base, internal discovery alias, and every attached adapter from one frontend;
// forwarding that graph-wide list would disclose endpoints the caller cannot
// access through Atlas. The response is rebuilt from a small OpenAI-compatible
// schema so duplicate keys and extension fields cannot smuggle sibling names.
func filterModelListResponse(resp *http.Response, servedModelAllowList string) error {
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		if err := resp.Body.Close(); err != nil {
			return fmt.Errorf("close model-list error response: %w", err)
		}
		body := []byte(`{"error":"model discovery unavailable"}`)
		resp.Body = io.NopCloser(bytes.NewReader(body))
		resp.ContentLength = int64(len(body))
		resp.Header = make(http.Header)
		resp.Header.Set("Content-Type", "application/json")
		resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(body)))
		return nil
	}
	allow := parseServedModelAllowList(servedModelAllowList)
	if len(allow) == 0 {
		return fmt.Errorf("model-list binding has no authorized served model")
	}

	reader := io.Reader(resp.Body)
	switch resp.Header.Get("Content-Encoding") {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return fmt.Errorf("decode gzip model list: %w", err)
		}
		defer gz.Close()
		reader = gz
	default:
		return fmt.Errorf("unsupported model-list content encoding %q", resp.Header.Get("Content-Encoding"))
	}

	body, err := io.ReadAll(io.LimitReader(reader, maxModelListBytes+1))
	if err != nil {
		return fmt.Errorf("read model list: %w", err)
	}
	if err := resp.Body.Close(); err != nil {
		return fmt.Errorf("close model list: %w", err)
	}
	if len(body) > maxModelListBytes {
		return fmt.Errorf("model list exceeds %d bytes", maxModelListBytes)
	}

	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode model list: %w", err)
	}
	if len(envelope.Data) == 0 {
		return fmt.Errorf("decode model list: missing data")
	}
	var models []json.RawMessage
	if err := json.Unmarshal(envelope.Data, &models); err != nil {
		return fmt.Errorf("decode model list data: %w", err)
	}

	filtered := make([]safeModelListing, 0, len(models))
	for _, raw := range models {
		count, err := countTopLevelJSONKey(raw, "id")
		if err != nil || count != 1 {
			return fmt.Errorf("decode model-list entry: id count=%d: %w", count, err)
		}
		var model safeModelListing
		if err := json.Unmarshal(raw, &model); err != nil {
			return fmt.Errorf("decode model-list entry: %w", err)
		}
		if _, authorized := allow[model.ID]; authorized {
			filtered = append(filtered, model)
		}
	}
	encoded, err := json.Marshal(safeModelList{Object: "list", Data: filtered})
	if err != nil {
		return fmt.Errorf("encode model list: %w", err)
	}

	resp.Body = io.NopCloser(bytes.NewReader(encoded))
	resp.ContentLength = int64(len(encoded))
	resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(encoded)))
	resp.Header.Del("Content-Encoding")
	return nil
}

// sanitizeModelListHeadResponse preserves the upstream status while removing
// graph-wide representation metadata (length, ETag, and extensions). HEAD has
// no body to filter, and the request-id header is added after this step.
func sanitizeModelListHeadResponse(resp *http.Response) {
	resp.Header = make(http.Header)
	resp.ContentLength = -1
}

func countTopLevelJSONKey(body []byte, key string) (int, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return 0, err
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return 0, fmt.Errorf("not a JSON object")
	}
	count := 0
	for dec.More() {
		name, err := dec.Token()
		if err != nil {
			return 0, err
		}
		if name == key {
			count++
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return 0, err
		}
	}
	_, err = dec.Token()
	return count, err
}
