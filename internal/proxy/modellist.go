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
		resetGraphWideHeaders(resp)
		resp.Header.Set("Content-Type", "application/json")
		resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(body)))
		resp.Trailer = nil
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
		// Split the two failure cases: countTopLevelJSONKey reports a
		// duplicate/absent "id" through the COUNT with a nil error, so a
		// combined `%w` on err would format a nil error as "%!w(<nil>)" —
		// garbage in the operator's only diagnostic for exactly the
		// duplicate-key smuggling attempt this guard exists to catch, and an
		// error that errors.Is/As cannot inspect. Both branches still reject
		// the whole response: fail-closed behaviour is unchanged.
		count, err := countTopLevelJSONKey(raw, "id")
		if err != nil {
			return fmt.Errorf("decode model-list entry: %w", err)
		}
		if count != 1 {
			return fmt.Errorf("decode model-list entry: expected exactly one top-level \"id\" key, got %d", count)
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
	resetGraphWideHeaders(resp)
	resp.Header.Set("Content-Type", "application/json")
	resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(encoded)))
	resp.Trailer = nil
	return nil
}

// carryHeaders are the response headers a CLIENT CONTRACT depends on, carried
// across a rebuilt body. Everything else upstream sent is dropped: the body
// phoebe returns is not the upstream representation, so representation
// metadata (ETag, Content-Encoding, Content-Length) would be a lie, and
// graph-wide extension headers (X-*) are exactly the sibling disclosure these
// filters exist to close. CORS is carried because phoebe emits none of its own
// (grep Access-Control across internal/ — nothing), so wiping it turns a
// working browser GET /v1/models into an opaque CORS failure while the POST on
// the same origin keeps working. Retry-After is carried so a 503 keeps its
// backoff guidance.
var carryHeaders = []string{
	"Access-Control-Allow-Origin",
	"Access-Control-Allow-Credentials",
	"Access-Control-Expose-Headers",
	"Access-Control-Max-Age",
	"Cache-Control",
	"Vary",
	"Retry-After",
}

// resetGraphWideHeaders replaces resp.Header with a fresh header set carrying
// only carryHeaders. Callers then Set their own Content-Type/Content-Length,
// which overwrite any carried value.
func resetGraphWideHeaders(resp *http.Response) {
	rebuilt := make(http.Header, len(carryHeaders))
	for _, k := range carryHeaders {
		if vs := resp.Header.Values(k); len(vs) > 0 {
			rebuilt[http.CanonicalHeaderKey(k)] = append([]string(nil), vs...)
		}
	}
	resp.Header = rebuilt
}

// sanitizeModelListHeadResponse preserves the upstream status while removing
// graph-wide representation metadata (length, ETag, and extensions). HEAD has
// no body to filter, and the request-id header is added after this step.
func sanitizeModelListHeadResponse(resp *http.Response) {
	resetGraphWideHeaders(resp)
	resp.ContentLength = -1
	resp.Trailer = nil
}

// sanitizeReadinessResponse replaces a bound endpoint's /health or /live
// response with a status-only document.
//
// Dynamo's frontend readiness is GRAPH-WIDE: it enumerates the registered
// component/worker/model instances of the whole graph, and one graph fronts a
// base model plus every attached adapter — i.e. sibling tenants. A deployment-
// scoped subdomain must not disclose those, which is the same invariant
// filterModelListResponse enforces for /v1/models; leaving /health open would
// re-open through a second route exactly what the model-list filter closes.
// Liveness itself stays truthful: the upstream status code is preserved, only
// the payload is reduced to its status class.
func sanitizeReadinessResponse(resp *http.Response, head bool) error {
	if err := resp.Body.Close(); err != nil {
		return fmt.Errorf("close readiness response: %w", err)
	}
	resetGraphWideHeaders(resp)
	resp.Trailer = nil
	if head {
		resp.Body = io.NopCloser(bytes.NewReader(nil))
		resp.ContentLength = -1
		resp.Header.Set("Content-Type", "application/json")
		return nil
	}
	body := []byte(`{"status":"unavailable"}`)
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		body = []byte(`{"status":"ok"}`)
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Set("Content-Type", "application/json")
	resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(body)))
	return nil
}

// countTopLevelJSONKey counts a single top-level key, reusing the ONE streaming
// duplicate-key parser in the package (modelbind.go's countTopLevelKeys). This
// guard is security-relevant — Phoebe must never validate one duplicate while
// Dynamo consumes another — so a second implementation would mean the tested
// copy and the used copy can drift apart.
func countTopLevelJSONKey(body []byte, key string) (int, error) {
	counts, err := countTopLevelKeys(body, map[string]struct{}{key: {}})
	if err != nil {
		return 0, err
	}
	return counts[key], nil
}
