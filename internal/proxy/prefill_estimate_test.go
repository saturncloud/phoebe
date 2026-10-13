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
		// vLLM's simplified part format: a part with no "type" is classified
		// by its keys, so type-less images cost the same as typed ones.
		{"type-less image_url part is an image",
			`{"model":"m","messages":[{"role":"user","content":[{"image_url":{"url":"data:image/png;base64,` + long + `"}}]}]}`, imagePartTokens},
		{"type-less image_url string is an image",
			`{"model":"m","messages":[{"role":"user","content":[{"image_url":"https://x"}]}]}`, imagePartTokens},
		{"type-less text part counts its text",
			`{"model":"m","messages":[{"role":"user","content":[{"text":"abcdefgh"}]}]}`, 2},
		{"type-less many images count each",
			`{"model":"m","messages":[{"role":"user","content":[` + strings.TrimSuffix(strings.Repeat(`{"image_url":{"url":"data:x"}},`, 200), ",") + `]}]}`, 200 * imagePartTokens},
		{"type-less audio part is not free",
			`{"model":"m","messages":[{"role":"user","content":[{"input_audio":{"data":"x"}}]}]}`, imagePartTokens},
		{"a typed text part with an image_url key is text only",
			`{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"abcdefgh","image_url":{"url":"data:x"}}]}]}`, 2},
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

// D7: a token-array prompt or input counts one token per element whatever
// JSON number form the ids take, so an array a lax engine parser accepts
// (integral floats, exponent form) can never estimate to the one-token
// minimum and slip past the prefill and KV caps. Exact int arrays keep their
// exact count (TestEstimatePromptTokens).
func TestTokenArrayOfAnyNumberFormCountsEveryElement(t *testing.T) {
	big := "[" + strings.Repeat("1.0,", 9999) + "1.0]"
	for _, tc := range []struct {
		name string
		body string
		want int64
	}{
		{"float token ids", `{"model":"m","prompt":[1.0,2.0,3.0]}`, 3},
		{"nested float token ids", `{"model":"m","prompt":[[1.0,2.0],[3]]}`, 3},
		{"exponent-form embeddings ids", `{"model":"m","input":[1e0,2]}`, 2},
		{"numeric strings inside nested arrays", `{"model":"m","prompt":[["1","2"],["3"]]}`, 3},
		{"mixed array: strings are text, other elements tokens", `{"model":"m","prompt":["abcd",5,null]}`, 3},
		{"a large float array is not the minimum", `{"model":"m","prompt":` + big + `}`, 10000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := estimatePromptTokens([]byte(tc.body)); got != tc.want {
				t.Fatalf("estimate=%d, want %d", got, tc.want)
			}
		})
	}
}

// promptBatchCount counts the prompts of a batched completions request and
// classifies elements exactly as promptText does.
func TestPromptBatchCountCountsBatchedPrompts(t *testing.T) {
	for _, tc := range []struct {
		prompt string
		want   int64
	}{
		{``, 1},
		{`"abc"`, 1},
		{`[]`, 1},
		{`[1,2,3]`, 1},
		{`["a","b","c"]`, 3},
		{`[[1,2],[3]]`, 2},
		{`[1,"a",2]`, 3},
		{` [ [1] , 2 ]`, 2},
	} {
		if got := promptBatchCount([]byte(tc.prompt)); got != tc.want {
			t.Errorf("promptBatchCount(%q) = %d, want %d", tc.prompt, got, tc.want)
		}
	}
}

// Tool and schema definitions are counted by raw JSON length / 4: the chat
// template renders tools into the prompt, so a tiny message carrying 4 MB of
// tool schemas (or a 4 MB response_format schema) must estimate at least a
// million tokens, not the one-token minimum. A null field adds nothing, and a
// body without these fields keeps its estimate.
func TestEstimatePromptTokensCountsToolAndSchemaDefinitions(t *testing.T) {
	msg := `"messages":[{"role":"user","content":"hi"}]`
	bigSchema := `{"type":"object","description":"` + strings.Repeat("d", 4<<20) + `"}`
	for _, tc := range []struct {
		name string
		body string
		min  int64
	}{
		{"4 MB tools array", `{"model":"m",` + msg + `,"tools":[{"type":"function","function":{"name":"f","parameters":` + bigSchema + `}}]}`, 1_000_000},
		{"4 MB legacy functions array", `{"model":"m",` + msg + `,"functions":[{"name":"f","parameters":` + bigSchema + `}]}`, 1_000_000},
		{"4 MB response_format json_schema", `{"model":"m",` + msg + `,"response_format":{"type":"json_schema","json_schema":{"name":"s","schema":` + bigSchema + `}}}`, 1_000_000},
		{"4 MB guided_json", `{"model":"m",` + msg + `,"guided_json":` + bigSchema + `}`, 1_000_000},
		{"4 MB structured_outputs", `{"model":"m",` + msg + `,"structured_outputs":{"json":` + bigSchema + `}}`, 1_000_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := estimatePromptTokens([]byte(tc.body)); got < tc.min {
				t.Fatalf("estimate=%d, want at least %d", got, tc.min)
			}
		})
	}
	plain := `{"model":"m","messages":[{"role":"user","content":"abcdefgh"}]}`
	if got := estimatePromptTokens([]byte(plain)); got != 2 {
		t.Fatalf("estimate without schema fields=%d, want 2 (unchanged)", got)
	}
	for _, key := range schemaFields {
		withNull := `{"model":"m","messages":[{"role":"user","content":"abcdefgh"}],"` + key + `":null}`
		if got := estimatePromptTokens([]byte(withNull)); got != 2 {
			t.Errorf("%s: null added to the estimate (%d), want 2", key, got)
		}
	}
	// The raw length counts exactly: 12 bytes of "tools" JSON add 3 tokens.
	small := `{"model":"m","messages":[{"role":"user","content":"abcdefgh"}],"tools":[{"a":"bc"}]}`
	if got := estimatePromptTokens([]byte(small)); got != (8+12+3)/4 {
		t.Fatalf("estimate=%d, want %d", got, (8+12+3)/4)
	}
}

// D36 (extends D33): fields the engine renders into the prompt from outside
// the messages (chat documents, a client chat template and its kwargs,
// completions prompt_embeds) and the template-rendered message fields (name,
// reasoning_content, tool_call_id) count in the estimate, so a request with
// a tiny message and a large document does not estimate as one token.
func TestEstimatePromptTokensCountsTemplateRenderedFields(t *testing.T) {
	msg := `"messages":[{"role":"user","content":"hi"}]`
	text4k := strings.Repeat("d", 4000)
	for _, tc := range []struct {
		name string
		body string
		min  int64
	}{
		{"documents", `{"model":"m",` + msg + `,"documents":[{"text":"` + text4k + `"}]}`, 1000},
		{"chat_template", `{"model":"m",` + msg + `,"chat_template":"` + text4k + `"}`, 1000},
		{"chat_template_kwargs", `{"model":"m",` + msg + `,"chat_template_kwargs":{"x":"` + text4k + `"}}`, 1000},
		{"prompt_embeds", `{"model":"m","prompt_embeds":"` + text4k + `"}`, 1000},
		{"message reasoning_content", `{"model":"m","messages":[{"role":"assistant","content":"hi","reasoning_content":"` + text4k + `"}]}`, 1000},
		{"message name", `{"model":"m","messages":[{"role":"user","content":"hi","name":"` + text4k + `"}]}`, 1000},
	} {
		if got := estimatePromptTokens([]byte(tc.body)); got < tc.min {
			t.Errorf("%s: estimate=%d, want at least %d", tc.name, got, tc.min)
		}
	}
	if got := estimatePromptTokens([]byte(`{"model":"m",` + msg + `,"documents":null}`)); got != 1 {
		t.Errorf("null documents: estimate=%d, want 1 (adds nothing)", got)
	}
}

// D37 (extends D7/D27): a typed video, audio or file part counts as one image
// (imagePartTokens) like an image part, so media never estimates to zero; the
// untyped form of the same part gets the same count.
func TestTypedMediaPartsCountAsOneImage(t *testing.T) {
	for _, tc := range []struct {
		name, typed, untyped string
	}{
		{"video_url", `{"type":"video_url","video_url":{"url":"data:video/mp4;base64,AAAA"}}`, `{"video_url":{"url":"data:video/mp4;base64,AAAA"}}`},
		{"input_audio", `{"type":"input_audio","input_audio":{"data":"AAAA","format":"wav"}}`, `{"input_audio":{"data":"AAAA","format":"wav"}}`},
		{"audio_url", `{"type":"audio_url","audio_url":{"url":"data:audio/wav;base64,AAAA"}}`, `{"audio_url":{"url":"data:audio/wav;base64,AAAA"}}`},
		// An untyped "file" key is not in vLLM's simplified part format (D27),
		// so only the typed form is checked.
		{"file", `{"type":"file","file":{"file_data":"AAAA"}}`, ""},
	} {
		if _, images := contentText([]byte(`[` + tc.typed + `]`)); images != 1 {
			t.Errorf("%s typed: images=%d, want 1", tc.name, images)
		}
		if tc.untyped == "" {
			continue
		}
		if _, images := contentText([]byte(`[` + tc.untyped + `]`)); images != 1 {
			t.Errorf("%s untyped: images=%d, want 1 (same as typed)", tc.name, images)
		}
	}
	if _, images := contentText([]byte(`[{"type":"unknown_kind","unknown_kind":{}}]`)); images != 0 {
		t.Errorf("unknown typed part: images=%d, want 0", images)
	}
}

// Tool-call arguments the chat template renders are never estimated as zero:
// a string counts its characters, and any other present shape counts its raw
// JSON length, so a multi-megabyte arguments object cannot slip past the
// prefill, KV and contract prompt windows. Only absent or null counts nothing.
func TestToolCallArgumentsOfAnyShapeCountTowardThePrompt(t *testing.T) {
	obj := `{"q":"` + strings.Repeat("a", 400) + `"}`
	arr := `[` + strings.TrimSuffix(strings.Repeat(`"abcdefgh",`, 50), ",") + `]`
	call := func(args string) string {
		return `{"model":"m","messages":[{"role":"assistant","content":null,"tool_calls":[{"function":{"name":"f","arguments":` + args + `}}]}]}`
	}
	for _, tc := range []struct {
		name string
		body string
		want int64
	}{
		{"string arguments count characters", call(`"` + strings.Repeat("a", 400) + `"`), 100},
		{"object arguments count raw JSON length", call(obj), (int64(len(obj)) + 3) / 4},
		{"array arguments count raw JSON length", call(arr), (int64(len(arr)) + 3) / 4},
		{"number arguments count raw JSON length", call(`12345678`), 2},
		{"null arguments count nothing", call(`null`), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := estimatePromptTokens([]byte(tc.body)); got != tc.want {
				t.Fatalf("estimate=%d, want %d", got, tc.want)
			}
		})
	}
	// A value that does not parse as JSON counts its raw byte length rather
	// than zero, so an unknown shape errs toward over-estimating.
	for _, raw := range []string{`{"a":`, `"unterminated`, `nul`} {
		if got := toolCallArgumentChars([]byte(raw)); got != int64(len(raw)) {
			t.Errorf("toolCallArgumentChars(%q) = %d, want %d", raw, got, len(raw))
		}
	}
	if got := toolCallArgumentChars(nil); got != 0 {
		t.Errorf("absent arguments = %d, want 0", got)
	}
}
