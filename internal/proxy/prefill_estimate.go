package proxy

import (
	"bytes"
	"encoding/json"
	"unicode/utf8"
)

// imagePartTokens is the fixed prompt-token cost charged per image part. The
// real cost depends on the model's vision encoder and the image size, which
// phoebe does not know; a fixed, generous per-image cost keeps an image-heavy
// request from looking nearly free (ruling 2026-10-12).
const imagePartTokens = 1024

// estimatePromptTokens is the admission estimate of a request's prompt tokens
// (ruling 2026-10-12): the characters of its message text divided by 4, plus
// imagePartTokens per image part. It deliberately does no chat-template
// rendering and no tokenization — it is approximate by design, admission
// releases prefill at the first response byte, and billing always uses the
// engine's exact counts. Pre-tokenized inputs (completions or embeddings token
// arrays) count one token per element, exactly.
//
// Message text is the text the model reads: chat message content (a string,
// or the text of each text part) and assistant tool-call arguments (a
// non-string arguments value counts by its raw JSON length); the
// completions prompt; the embeddings input. JSON structure, keys, sampling
// parameters and the like are not message text. A body with none of these
// estimates 1, the minimum admission accepts.
//
// Tool and schema definitions (schemaFields: tools, functions,
// response_format and the guided-decoding fields) are counted by their raw
// JSON length, because the engine's chat template renders tool definitions
// into the prompt and structured-output schemas can be as large; a request
// with a tiny message and megabytes of tool schemas must not estimate as
// one token (D7 extension, battery fix 2026-10-13). The same raw-length
// count covers the fields the engine renders into the prompt from outside
// the messages: chat documents (RAG) and a client chat_template and
// chat_template_kwargs, which the engine's chat template renders, and
// completions prompt_embeds, whose base64 length over-counts conservatively
// (billing reconciles to the engine's counts). A JSON null counts nothing.
//
// Each message's name, reasoning_content, reasoning and tool_call_id strings
// count as message text too, because chat templates render them. A prior
// assistant turn carries its thinking under either name: "reasoning_content"
// (Dynamo's default response field) or "reasoning" (vLLM 0.31's own server and
// newer OpenAI-compatible clients).
//
// Every field is looked up by its exact key. encoding/json's struct decoding
// matches keys case-insensitively and keeps the last match, while the engine
// frontend reads exact keys, so a struct decode would let
// {"content":"<huge>","Content":""} estimate as empty text that the engine
// then prefills in full. Top-level case variants are refused earlier (D13);
// exact lookup here closes the same gap in nested objects. A duplicated exact
// key keeps its last value, as both encoding/json and the engine do.
func estimatePromptTokens(body []byte) int64 {
	// The caller has already validated the body as one JSON object with a
	// model; a field of an unexpected shape contributes nothing rather than
	// failing the request here.
	top := jsonObject(body)

	var chars, tokens int64
	var messages []json.RawMessage
	_ = json.Unmarshal(top["messages"], &messages)
	for _, rawMsg := range messages {
		m := jsonObject(rawMsg)
		c, images := contentText(m["content"])
		chars += c
		tokens += images * imagePartTokens
		for _, key := range []string{"name", "reasoning_content", "reasoning", "tool_call_id"} {
			chars += jsonStringChars(m[key])
		}
		var calls []json.RawMessage
		_ = json.Unmarshal(m["tool_calls"], &calls)
		for _, rawCall := range calls {
			fn := jsonObject(jsonObject(rawCall)["function"])
			chars += toolCallArgumentChars(fn["arguments"])
		}
	}
	for _, raw := range []json.RawMessage{top["prompt"], top["input"]} {
		c, t := promptText(raw)
		chars += c
		tokens += t
	}
	for _, key := range schemaFields {
		if raw := bytes.TrimSpace(top[key]); len(raw) > 0 && !bytes.Equal(raw, []byte("null")) {
			chars += int64(len(raw))
		}
	}
	estimate := tokens + (chars+3)/4
	if estimate < 1 {
		estimate = 1
	}
	return estimate
}

// schemaFields are the top-level request fields holding tool or
// structured-output definitions, documents, a client chat template or
// prompt embeddings that the engine renders into, or compiles alongside,
// the prompt. estimatePromptTokens counts each by raw JSON length.
var schemaFields = []string{
	"tools", "functions", "response_format",
	"guided_json", "guided_regex", "guided_grammar", "guided_choice",
	"structured_outputs",
	"documents", "chat_template", "chat_template_kwargs", "prompt_embeds",
}

// jsonObject decodes raw as a JSON object keyed by exact field name, or
// returns nil when raw is absent or not an object. Indexing a nil map yields
// a nil RawMessage, which every reader here treats as "field absent".
func jsonObject(raw json.RawMessage) map[string]json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil {
		return nil
	}
	return obj
}

// jsonString decodes raw as a JSON string; ok is false when raw is absent or
// not a string.
func jsonString(raw json.RawMessage) (s string, ok bool) {
	if len(raw) == 0 {
		return "", false
	}
	return s, json.Unmarshal(raw, &s) == nil
}

// toolCallArgumentChars measures a tool call's function.arguments. A JSON
// string counts its characters. Any other present value (an object, an array,
// a number, or bytes that do not parse) counts its raw JSON length, because
// the engine does not strictly validate message tool-call arguments and the
// chat template renders whatever is there into the prompt. Only an absent
// value or a JSON null counts nothing, so an unexpected shape errs toward
// over-estimating, never toward zero.
func toolCallArgumentChars(raw json.RawMessage) int64 {
	if s, ok := jsonString(raw); ok {
		return int64(utf8.RuneCountInString(s))
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0
	}
	return int64(len(trimmed))
}

// jsonStringChars counts the characters of raw when it is a JSON string, else 0.
func jsonStringChars(raw json.RawMessage) int64 {
	s, _ := jsonString(raw)
	return int64(utf8.RuneCountInString(s))
}

// contentText measures a chat message's content: a plain string, or an array
// of parts where text parts contribute their text and image parts count one
// image each. Typed video, audio and file parts also count as one image
// (imagePartTokens, D7) so that media never estimates to zero, consistent
// with the untyped path below. A part of any other type contributes nothing.
//
// A part with no "type" is classified by its keys, as vLLM's chat frontend
// does for its simplified part format ({"image_url": ...}, {"text": ...}): an
// image key makes it one image, else a "text" key makes it text, else an
// audio or video key counts it as one image so media the engine encodes
// never estimates to zero. A typed part keeps its type's handling whatever
// other keys it carries.
func contentText(raw json.RawMessage) (chars, images int64) {
	if len(raw) == 0 {
		return 0, 0
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return int64(utf8.RuneCountInString(text)), 0
	}
	var parts []json.RawMessage
	if json.Unmarshal(raw, &parts) != nil {
		return 0, 0
	}
	for _, rawPart := range parts {
		p := jsonObject(rawPart)
		typ, _ := jsonString(p["type"])
		if typ == "" {
			typ = untypedPartKind(p)
		}
		switch typ {
		case "text", "input_text":
			chars += jsonStringChars(p["text"])
		case "image_url", "input_image", "image", "image_pil", "image_embeds",
			"video_url", "video", "audio_url", "input_audio", "audio", "file", "input_file":
			images++
		}
	}
	return chars, images
}

// untypedPartKind classifies a content part that carries no "type" by its
// keys, in the order vLLM's simplified format is read: an image key first,
// then text, then other media (counted as one image). It returns "" for a
// part with none of them.
func untypedPartKind(p map[string]json.RawMessage) string {
	for _, k := range []string{"image_url", "image", "input_image", "image_pil", "image_embeds"} {
		if _, ok := p[k]; ok {
			return "image"
		}
	}
	if _, ok := p["text"]; ok {
		return "text"
	}
	for _, k := range []string{"video_url", "audio_url", "input_audio"} {
		if _, ok := p[k]; ok {
			return "image"
		}
	}
	return ""
}

// promptBatchCount is how many prompts a completions request's prompt holds:
// the element count of an array whose elements include a string or an array
// (a batch of text prompts or of token-id arrays), else 1 (absent, a single
// string, an empty array, or a flat token-id array). Elements are classified
// exactly as promptText classifies them. The engine runs every prompt of a
// batch as its own generation, so admission multiplies the output sequences
// by this count.
func promptBatchCount(raw json.RawMessage) int64 {
	var elems []json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &elems) != nil || len(elems) == 0 {
		return 1
	}
	for _, e := range elems {
		if e = bytes.TrimSpace(e); len(e) > 0 && (e[0] == '"' || e[0] == '[') {
			return int64(len(elems))
		}
	}
	return 1
}

// promptText measures a completions prompt or an embeddings input: a string,
// an array of strings, an array of token ids, or an array of token-id arrays.
// Strings return characters; token ids return an exact token count. An array
// element that is not a string counts as one token whatever its JSON form
// (1.0, 1e0, a numeric string inside a nested array, null, ...), because a
// lax engine parser may accept it as a token id and prefill it: an array
// never estimates to zero just because its elements are not exact int64s.
func promptText(raw json.RawMessage) (chars, tokens int64) {
	if len(raw) == 0 {
		return 0, 0
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return int64(utf8.RuneCountInString(text)), 0
	}
	var elems []json.RawMessage
	if json.Unmarshal(raw, &elems) != nil {
		return 0, 0
	}
	for _, e := range elems {
		e = bytes.TrimSpace(e)
		switch {
		case len(e) > 0 && e[0] == '"':
			var t string
			if json.Unmarshal(e, &t) == nil {
				chars += int64(utf8.RuneCountInString(t))
			}
		case len(e) > 0 && e[0] == '[':
			var inner []json.RawMessage
			if json.Unmarshal(e, &inner) == nil {
				tokens += int64(len(inner))
			}
		default:
			tokens++
		}
	}
	return chars, tokens
}
