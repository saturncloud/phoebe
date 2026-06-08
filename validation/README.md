# Engine conformance validation

Everything in Phoebe's streaming/usage-capture path is unit-tested against
**synthetic** vLLM payloads plus source-code research. That is necessary but not
sufficient for a billing product: if the real engine names a field differently
(most dangerously `prompt_tokens_details.cached_tokens` — flagged in
`internal/metering/metering.go`), every bill is wrong and we'd never know from
the unit tests.

`cmd/engine-conformance` closes that gap. It fires **real** OpenAI-style requests
at a live engine and asserts the bytes parse into the *same* `metering.Usage`
struct Phoebe bills on — proving the field names against the engine that will
actually serve traffic.

## What it checks

1. **Non-streaming usage block** — a real completion returns a `usage` block that
   parses into `metering.Usage` with sane prompt/completion counts.
2. **Streaming usage block** — with `stream_options.include_usage=true`, the
   engine emits a trailing chunk (`choices: []`) carrying usage, terminated by
   `data: [DONE]` — the exact shape Phoebe's tee depends on.
3. **Prefix-cache / cached_tokens** (opt-in `-prompt-cache`) — sends a long shared
   prefix twice and asserts the second response reports
   `prompt_tokens_details.cached_tokens > 0`. This is the single field
   `metering.go` flags as needing live verification, and it doubles as the check
   for the **`--enable-prompt-tokens-details` deployment requirement** (without
   that flag, cached tokens never appear and cache discounts are impossible).

Exit `0` = all checks pass (safe to bill against this engine); `1` = a billed
field didn't match (do **not** ship billing until reconciled); `2` = setup error.

## Run it now — no GPU required

It targets any OpenAI-compatible endpoint, so you can develop/verify the harness
against a CPU server or a hosted endpoint:

```sh
# any OpenAI-compatible server (llama.cpp, a hosted endpoint, etc.)
go run ./cmd/engine-conformance -base http://localhost:8000 -model <model> [-api-key <key>]
```

The harness itself is validated in CI-style against a stub engine (it was run
green against a synthetic vLLM-shaped server during development, including the
cached-tokens path).

## Run it against the real vLLM — the one-command step (needs a GPU)

This is the step gated on Hugo: it needs a running vLLM, which means a GPU. Once
an engine is up (e.g. a Saturn GPU workspace on a vLLM image, **started with
`--enable-prompt-tokens-details`** so the cache check is meaningful):

```sh
go run ./cmd/engine-conformance \
  -base   http://<vllm-host>:8000 \
  -model  <served-model-name> \
  -prompt-cache
```

A green run is the empirical sign-off that Phoebe's metering matches the deployed
engine — the trust property a neocloud is actually buying. **Until this passes
against the real engine, the billing path is verified only against synthetic
data.**

### Spinning up the engine (reference)

A throwaway vLLM for this check can be a Saturn GPU workspace on a vLLM image, or
any `vllm serve <model> --enable-prompt-tokens-details`. The harness does not
start the engine — it only validates one that's running — so no GPU is spent
until someone deliberately stands one up.
