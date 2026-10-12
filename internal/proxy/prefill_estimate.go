package proxy

import (
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
func estimatePromptTokens(body []byte) int64 {
	var req struct {
		Messages []struct {
			Content   json.RawMessage `json:"content"`
			ToolCalls []struct {
				Function struct {
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"messages"`
		Prompt json.RawMessage `json:"prompt"`
		Input  json.RawMessage `json:"input"`
	}
	// The caller has already validated the body as one JSON object with a
	// model; a shape this decoder rejects (a field of an unexpected type)
	// still gets the minimum rather than failing the request here.
	_ = json.Unmarshal(body, &req)

	var chars, tokens int64
	for _, m := range req.Messages {
		c, images := contentText(m.Content)
		chars += c
		tokens += images * imagePartTokens
		for _, call := range m.ToolCalls {
			chars += int64(utf8.RuneCountInString(call.Function.Arguments))
		}
	}
	for _, raw := range []json.RawMessage{req.Prompt, req.Input} {
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
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &parts) != nil {
		return 0, 0
	}
	for _, p := range parts {
		switch p.Type {
		case "text", "input_text":
			chars += int64(utf8.RuneCountInString(p.Text))
		case "image_url", "input_image", "image":
			images++
		}
	}
	return chars, images
}

// promptText measures a completions prompt or an embeddings input: a string,
// an array of strings, an array of token ids, or an array of token-id arrays.
// Strings return characters; token ids return an exact token count.
func promptText(raw json.RawMessage) (chars, tokens int64) {
	if len(raw) == 0 {
		return 0, 0
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return int64(utf8.RuneCountInString(text)), 0
	}
	var texts []string
	if json.Unmarshal(raw, &texts) == nil {
		for _, t := range texts {
			chars += int64(utf8.RuneCountInString(t))
		}
		return chars, 0
	}
	var ids []int64
	if json.Unmarshal(raw, &ids) == nil {
		return 0, int64(len(ids))
	}
	var batches [][]int64
	if json.Unmarshal(raw, &batches) == nil {
		for _, b := range batches {
			tokens += int64(len(b))
		}
	}
	return 0, tokens
}
