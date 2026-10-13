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
// or the text of each text part) and assistant tool-call arguments; the
// completions prompt; the embeddings input. JSON structure, keys, sampling
// parameters and the like are not message text. A body with none of these
// estimates 1, the minimum admission accepts.
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
		var calls []json.RawMessage
		_ = json.Unmarshal(m["tool_calls"], &calls)
		for _, rawCall := range calls {
			fn := jsonObject(jsonObject(rawCall)["function"])
			chars += jsonStringChars(fn["arguments"])
		}
	}
	for _, raw := range []json.RawMessage{top["prompt"], top["input"]} {
		c, t := promptText(raw)
		chars += c
		tokens += t
	}
	estimate := tokens + (chars+3)/4
	if estimate < 1 {
		estimate = 1
	}
	return estimate
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

// jsonStringChars counts the characters of raw when it is a JSON string, else 0.
func jsonStringChars(raw json.RawMessage) int64 {
	s, _ := jsonString(raw)
	return int64(utf8.RuneCountInString(s))
}

// contentText measures a chat message's content: a plain string, or an array
// of parts where text parts contribute their text and image parts count one
// image each. Other part types (audio, files) contribute nothing; no shared
// model accepts them.
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
		switch typ {
		case "text", "input_text":
			chars += jsonStringChars(p["text"])
		case "image_url", "input_image", "image":
			images++
		}
	}
	return chars, images
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
