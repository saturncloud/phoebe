package proxy

import (
	"encoding/json"
	"testing"
)

// errorBody decodes an OpenAI-shaped error body.
func errorBody(t *testing.T, body []byte) clientError {
	t.Helper()
	var env struct {
		Error *clientError `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil || env.Error == nil {
		t.Fatalf("body is not an OpenAI-shaped error: %q (%v)", body, err)
	}
	return *env.Error
}

// errorCode returns the error.code of an OpenAI-shaped error body.
func errorCode(t *testing.T, body []byte) string {
	t.Helper()
	return errorBody(t, body).Code
}
