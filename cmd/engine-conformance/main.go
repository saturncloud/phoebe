// Command engine-conformance verifies that Phoebe's usage-block parsing
// (internal/metering.Usage) matches what a REAL OpenAI-compatible engine
// (vLLM / SGLang / TensorRT-LLM) actually emits — especially the cached-token
// field, which internal/metering flags as needing live verification.
//
// Why this exists: every streaming/usage-capture test in the repo runs against
// SYNTHETIC payloads + source research. That is necessary but not sufficient for
// a billing product — if the real engine names a field differently, every bill
// is wrong. This harness closes that gap by firing real requests at a live
// engine and asserting the bytes parse into the counts we bill on.
//
// It needs NO GPU to develop against: point it at any OpenAI-compatible URL
// (a hosted endpoint, a CPU llama.cpp server, or a real vLLM). The intended
// production use is to run it once against the actual deployed vLLM, which is a
// one-command step gated only on a running engine — see validation/README.md.
//
// Usage:
//
//	engine-conformance -base http://localhost:8000 -model meta-llama/Llama-3.1-8B
//	engine-conformance -base $URL -model $M -api-key $KEY -prompt-cache
//
// Exit codes: 0 = all conformance checks passed; 1 = a check failed (a field
// Phoebe relies on was missing or mis-shaped); 2 = transport/setup error.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/saturncloud/phoebe/internal/metering"
)

func main() {
	base := flag.String("base", "http://localhost:8000", "OpenAI-compatible engine base URL")
	model := flag.String("model", "", "model name to request (required)")
	apiKey := flag.String("api-key", "", "optional bearer token for the engine")
	promptCache := flag.Bool("prompt-cache", false, "also run the prefix-cache check (sends a shared long prefix twice; requires the engine started with --enable-prompt-tokens-details)")
	timeout := flag.Duration("timeout", 60*time.Second, "per-request timeout")
	flag.Parse()

	if *model == "" {
		fmt.Fprintln(os.Stderr, "error: -model is required")
		os.Exit(2)
	}

	c := &checker{
		base:    strings.TrimRight(*base, "/"),
		model:   *model,
		apiKey:  *apiKey,
		client:  &http.Client{Timeout: *timeout},
		timeout: *timeout,
	}

	fmt.Printf("engine-conformance: %s @ %s\n\n", *model, c.base)

	ok := true
	ok = c.run("non-streaming usage block", c.checkNonStreaming) && ok
	ok = c.run("streaming usage block (trailing chunk, choices:[])", c.checkStreaming) && ok
	if *promptCache {
		ok = c.run("prefix-cache: prompt_tokens_details.cached_tokens", c.checkPromptCache) && ok
	} else {
		fmt.Println("SKIP  prefix-cache check (pass -prompt-cache to enable; needs --enable-prompt-tokens-details on the engine)")
	}

	fmt.Println()
	if !ok {
		fmt.Println("RESULT: FAIL — a field Phoebe bills on did not match the engine. Do NOT ship billing against this engine until reconciled.")
		os.Exit(1)
	}
	fmt.Println("RESULT: PASS — Phoebe's usage parsing matches this engine's output.")
}

type checker struct {
	base    string
	model   string
	apiKey  string
	client  *http.Client
	timeout time.Duration
}

func (c *checker) run(name string, fn func() error) bool {
	if err := fn(); err != nil {
		fmt.Printf("FAIL  %s\n        %v\n", name, err)
		return false
	}
	fmt.Printf("PASS  %s\n", name)
	return true
}

func (c *checker) post(ctx context.Context, body map[string]any) (*http.Response, error) {
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/v1/chat/completions", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	return c.client.Do(req)
}

// checkNonStreaming asserts a non-streaming response carries a usage block that
// parses into metering.Usage with non-zero prompt+completion counts.
func (c *checker) checkNonStreaming() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.post(ctx, map[string]any{
		"model":      c.model,
		"messages":   []map[string]string{{"role": "user", "content": "Say hello in one word."}},
		"max_tokens": 16,
	})
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d: %s", resp.StatusCode, truncate(raw, 300))
	}

	// Parse via the SAME struct Phoebe bills on.
	var body struct {
		Usage *metering.Usage `json:"usage"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return fmt.Errorf("unmarshal: %w (body: %s)", err, truncate(raw, 300))
	}
	if body.Usage == nil {
		return fmt.Errorf("no usage block in non-streaming response (body: %s)", truncate(raw, 300))
	}
	if body.Usage.PromptTokens <= 0 || body.Usage.CompletionTokens <= 0 {
		return fmt.Errorf("usage parsed but counts look wrong: prompt=%d completion=%d (a field-name mismatch would zero these)",
			body.Usage.PromptTokens, body.Usage.CompletionTokens)
	}
	fmt.Printf("        prompt=%d completion=%d cached=%d\n",
		body.Usage.PromptTokens, body.Usage.CompletionTokens, body.Usage.CachedTokens())
	return nil
}

// checkStreaming asserts that, with stream_options.include_usage=true, the
// engine emits a trailing chunk with choices:[] carrying the usage block — the
// exact shape Phoebe's tee depends on — and that it parses into metering.Usage.
func (c *checker) checkStreaming() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.post(ctx, map[string]any{
		"model":          c.model,
		"messages":       []map[string]string{{"role": "user", "content": "Count to three."}},
		"max_tokens":     32,
		"stream":         true,
		"stream_options": map[string]any{"include_usage": true},
	})
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, truncate(raw, 300))
	}

	// Scan SSE lines for the trailing usage chunk, mirroring the tee's logic.
	var usage *metering.Usage
	var usageHadEmptyChoices bool
	var sawDone bool
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
		if payload == "[DONE]" {
			sawDone = true
			continue
		}
		var chunk struct {
			Choices []json.RawMessage `json:"choices"`
			Usage   *metering.Usage   `json:"usage"`
		}
		if json.Unmarshal([]byte(payload), &chunk) != nil {
			continue
		}
		if chunk.Usage != nil {
			usage = chunk.Usage
			usageHadEmptyChoices = len(chunk.Choices) == 0
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read stream: %w", err)
	}
	if !sawDone {
		return fmt.Errorf("stream never sent `data: [DONE]` — unexpected SSE framing")
	}
	if usage == nil {
		return fmt.Errorf("no usage chunk in stream — engine may not honor stream_options.include_usage (Phoebe would under-bill every streamed request)")
	}
	if !usageHadEmptyChoices {
		// Not fatal for billing, but it's the documented vLLM shape; flag it.
		fmt.Printf("        WARN: usage chunk had non-empty choices (expected choices:[]) — verify the tee still captures it\n")
	}
	if usage.PromptTokens <= 0 || usage.CompletionTokens <= 0 {
		return fmt.Errorf("streamed usage counts look wrong: prompt=%d completion=%d", usage.PromptTokens, usage.CompletionTokens)
	}
	fmt.Printf("        prompt=%d completion=%d cached=%d (trailing chunk)\n",
		usage.PromptTokens, usage.CompletionTokens, usage.CachedTokens())
	return nil
}

// checkPromptCache sends the same long prefix twice; the second request should
// report a non-zero prompt_tokens_details.cached_tokens IF the engine was
// started with --enable-prompt-tokens-details and prefix caching is on. This is
// the single field internal/metering flags as needing live verification.
func (c *checker) checkPromptCache() error {
	// A long, deterministic prefix maximizes the chance of a prefix-cache hit.
	prefix := strings.Repeat("The quick brown fox jumps over the lazy dog. ", 80)
	msg := []map[string]string{{"role": "user", "content": prefix + "Reply with OK."}}

	send := func() (*metering.Usage, error) {
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		resp, err := c.post(ctx, map[string]any{"model": c.model, "messages": msg, "max_tokens": 4})
		if err != nil {
			return nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("status %d: %s", resp.StatusCode, truncate(raw, 200))
		}
		var body struct {
			Usage *metering.Usage `json:"usage"`
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			return nil, err
		}
		return body.Usage, nil
	}

	if _, err := send(); err != nil { // warm the cache
		return fmt.Errorf("warm-up request: %w", err)
	}
	u, err := send() // measure
	if err != nil {
		return fmt.Errorf("measure request: %w", err)
	}
	if u == nil {
		return fmt.Errorf("no usage block on cache-measure request")
	}
	if u.PromptTokensDetails == nil {
		return fmt.Errorf("prompt_tokens_details ABSENT on a likely cache hit — the engine is probably missing --enable-prompt-tokens-details. " +
			"Phoebe will never see cached tokens (cache discounts impossible) until that flag is set. This is the deployment requirement to confirm")
	}
	if u.CachedTokens() <= 0 {
		return fmt.Errorf("prompt_tokens_details present but cached_tokens=0 on a likely cache hit — verify prefix caching is enabled and the field name is correct")
	}
	fmt.Printf("        cached_tokens=%d of prompt_tokens=%d — field name confirmed, caching works\n",
		u.CachedTokens(), u.PromptTokens)
	return nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}
