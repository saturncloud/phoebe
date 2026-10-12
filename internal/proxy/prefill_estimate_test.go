package proxy

import (
	"strings"
	"testing"
)

func TestEstimatePromptTokens(t *testing.T) {
	long := strings.Repeat("a", 400)
	for _, tc := range []struct {
		name string
		body string
		want int64
	}{
		{"no message text is the minimum", `{"model":"m"}`, 1},
		{"string content counts characters / 4", `{"model":"m","messages":[{"role":"user","content":"` + long + `"}]}`, 100},
		{"JSON structure and other fields are not text", `{"model":"m","temperature":0.5,"padding":"` + long + `","messages":[{"role":"user","content":"abcd"}]}`, 1},
		{"characters, not bytes", `{"model":"m","messages":[{"role":"user","content":"` + strings.Repeat("é", 8) + `"}]}`, 2},
		{"rounds up", `{"model":"m","messages":[{"role":"user","content":"abcde"}]}`, 2},
		{"sums every message", `{"model":"m","messages":[{"role":"system","content":"abcd"},{"role":"user","content":"abcd"}]}`, 2},
		{"text parts count, image parts cost a fixed amount",
			`{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"abcdefgh"},{"type":"image_url","image_url":{"url":"data:image/png;base64,` + long + `"}}]}]}`,
			2 + imagePartTokens},
		{"tool-call arguments are message text", `{"model":"m","messages":[{"role":"assistant","content":null,"tool_calls":[{"function":{"name":"f","arguments":"{\"q\":\"abcdef\"}"}}]}]}`, 4},
		{"completions prompt string", `{"model":"m","prompt":"` + long + `"}`, 100},
		{"completions prompt strings", `{"model":"m","prompt":["abcd","abcd"]}`, 2},
		{"pre-tokenized prompt counts exactly", `{"model":"m","prompt":[1,2,3,4,5,6,7]}`, 7},
		{"pre-tokenized batch counts exactly", `{"model":"m","prompt":[[1,2,3],[4,5]]}`, 5},
		{"embeddings input", `{"model":"m","input":"` + long + `"}`, 100},
		{"unexpected shapes fall back to the minimum", `{"model":"m","messages":"not-a-list"}`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := estimatePromptTokens([]byte(tc.body)); got != tc.want {
				t.Fatalf("estimate=%d, want %d", got, tc.want)
			}
		})
	}
}

// The engine frontend reads nested fields by exact key; encoding/json's struct
// decoding matches case-insensitively and keeps the last match. A case-variant
// sibling must not hide the text the engine will prefill.
func TestEstimatePromptTokensReadsNestedFieldsByExactKey(t *testing.T) {
	long := strings.Repeat("a", 400)
	for _, tc := range []struct {
		name string
		body string
		want int64
	}{
		{"message content beside an empty Content",
			`{"model":"m","messages":[{"role":"user","content":"` + long + `","Content":""}]}`, 100},
		{"message Content alone is not content",
			`{"model":"m","messages":[{"role":"user","Content":"` + long + `"}]}`, 1},
		{"text part beside an empty Text",
			`{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"` + long + `","Text":""}]}]}`, 100},
		{"part type beside a different Type",
			`{"model":"m","messages":[{"role":"user","content":[{"type":"text","Type":"other","text":"` + long + `"}]}]}`, 100},
		{"tool-call arguments beside an empty Arguments",
			`{"model":"m","messages":[{"role":"assistant","content":null,"tool_calls":[{"function":{"name":"f","arguments":"` + long + `","Arguments":""}}]}]}`, 100},
		{"tool-call function beside an empty Function",
			`{"model":"m","messages":[{"role":"assistant","content":null,"tool_calls":[{"function":{"name":"f","arguments":"` + long + `"},"Function":{}}]}]}`, 100},
		{"tool_calls beside an empty Tool_calls",
			`{"model":"m","messages":[{"role":"assistant","content":null,"tool_calls":[{"function":{"name":"f","arguments":"` + long + `"}}],"Tool_calls":[]}]}`, 100},
		{"a duplicated exact key keeps its last value, as the engine does",
			`{"model":"m","messages":[{"role":"user","content":"abcd","content":"` + long + `"}]}`, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := estimatePromptTokens([]byte(tc.body)); got != tc.want {
				t.Fatalf("estimate=%d, want %d", got, tc.want)
			}
		})
	}
}
