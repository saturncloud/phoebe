# Phoebe

Phoebe is the **token-metering interceptor** for Saturn Cloud's token factory.
It is a thin, tenant-aware reverse proxy that sits behind Traefik and in front
of the (optional) inference router / vLLM engine:

```
Traefik → atlas-auth (ForwardAuth) → Phoebe → [vLLM prod router | llm-d] → vLLM / SGLang / TensorRT-LLM
```

Phoebe is the always-present capture point for **token metering and billing**.
It does **not** authenticate or authorize — that happens at the edge — it
trusts the identity headers atlas-auth injects.

## What it does

- **Identity (read, don't re-auth).** Reads the trusted `X-Saturn-User-Id`,
  `X-Saturn-Group-Id`, `X-Saturn-Resource-Id`, `X-Saturn-Resource-Type`
  headers. See `internal/identity`.
- **Per-model dispatch.** Resolves the target model from `X-Saturn-Resource-Id`
  to an upstream URL, with no redeploy needed for new models and a clean
  404/410 for torn-down ones. See `internal/registry`.
- **Token metering capture.** Captures token counts from the engine's own
  `usage` block (the authority — never re-tokenizes), forces
  `stream_options.include_usage=true`, and emits one immutable, idempotent
  metering event per request keyed by `request_id`. See `internal/metering`.
- **Streaming correctness.** Forward-then-inspect SSE: streams each chunk to
  the client immediately, captures the trailing usage chunk, handles client
  aborts. See `internal/proxy`.

It is **topology-independent**: it behaves identically whether the upstream is
an engine directly (Shape A) or a router (Shape B), and acts as the stable
contract that makes everything below it swappable.

## What it deliberately does NOT do

- Authentication / authorization (edge, via atlas-auth).
- Cache-aware routing across replicas (the inference router's job).
- Format translation (the engines already serve OpenAI shapes).

## Layout

```
cmd/interceptor/    main entrypoint
internal/config/    YAML settings (defaults → unmarshal → parse)
internal/identity/  trusted X-Saturn-* header extraction
internal/logging/   leveled logger
internal/metering/  billing event schema + Emitter contract
internal/proxy/     the reverse-proxy core (streaming tee, dispatch)
internal/registry/  model → upstream resolution
config/             example settings
```

## Local dev

```sh
make test    # go test ./...
make vet     # go vet ./...
make run     # build + run against config/settings.example.yaml
make build   # build to bin/phoebe
```

## Status

Walking skeleton: identity → registry → upstream dispatch and the server
lifecycle are wired and tested. The SSE forward-then-inspect tee, the
`include_usage` forcing, client-abort handling, and the durable metering
emitter (Kafka / Redis Streams) are stubbed with `TODO(milestone-...)` markers
in `internal/proxy` and `internal/metering`.
