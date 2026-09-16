# Shared-tier admission and fairness ownership

## Decision point

Shared inference follows this path:

`Traefik → atlas-auth → Phoebe → Dynamo frontend/router → engine`

Phoebe admits after atlas-auth has supplied trusted identity and after Phoebe
has resolved and bound `(organization, model)`. It admits before the first
engine request. The Valkey Lua transaction is the shared source of truth for
every Phoebe replica; an unavailable store returns HTTP 503. A local fallback
would violate the global bound and is intentionally absent.

## Capability audit

| Capability | Classification | Owner / boundary |
| --- | --- | --- |
| Authentication, route authorization, org/model registry | Already implemented | atlas-auth and Atlas decide access; Phoebe only consumes the trusted result. |
| Platform, graph, org, org×model admission | Saturn-owned, implemented here | Atomic Valkey reservations in Phoebe. |
| Concurrent prefills and input work | Saturn-owned, implemented conservatively | Phoebe reserves concurrent prefills and complete request-body bytes, then releases on the first upstream body byte. Model-specific rendered templates/token counts stay in the engine. |
| Active decodes/streams and reserved output | Saturn-owned, implemented here | `max_tokens`/`max_completion_tokens` is reserved until completion, abort, timeout, rejection, or upstream failure. |
| Request and generated-token windows | Saturn-owned, implemented here | Fixed operator-configured windows. Generated usage is charged from the engine's authoritative completion count; outstanding max-output reservations prevent overbooking. |
| Cold holds and wake churn | Partially implemented before; completed here | Phoebe already detected/actuated wake-from-zero. Distributed hold and wake-window admission now surrounds actuation. |
| Adapter and KV pressure | Partial | Phoebe bounds active adapter-bearing requests, prompt bytes, and reserved output as observable pressure proxies. Dynamo/vLLM owns real adapter residency, prefix cache, KV allocation, eviction, and batching. |
| Service-tier protected shares | Saturn-owned, implemented as coarse admission lanes | Operator-owned org→tier mapping prevents client selection. Hard lanes and weights protect capacity; this is not exact per-request scheduling fairness. |
| Cache-aware/worker routing and prefill/decode scheduling | Externally provided | Dynamo frontend/router and engine scheduler. |
| Gang lifecycle and replica topology | Externally provided | Grove/Dynamo operator. |
| GPU queue quota, DRF, placement, and preemption | Externally provided | KAI Scheduler. It schedules pods, not HTTP requests or tokens. |

## Currencies stay separate

Admission counters protect physical service health. Product/contract limits
remain control-plane policy. Billing continues to use immutable engine-reported
usage events. No admission estimate is written as invoice usage, and no price or
spend balance is used as a GPU-pressure signal.

## Lifecycle and crash recovery

An admitted request owns a lease. Input/prefill counters release at response
body byte; active stream, output, adapter, and generated-window reservation state
release at final body completion. The owning proxy renews the expiry throughout
long streams. Pre-header aborts, upstream errors, ordinary
rejections, wake failures, and handler early returns run the same idempotent
release. A replica crash cannot execute cleanup, so every operation first reaps
expired leases. `leaseTtl` is the maximum crash-leak interval, not a stream
duration limit.

## Rollout and rollback

Roll out with `admission.enabled: false`, deploy all Phoebe replicas, configure
one shared Valkey and conservative measured limits, then enable admission. Watch
429s by scope/dimension and Valkey latency/errors. Roll back by disabling the
feature; existing leases expire without affecting billing or Dynamo. Do not
point replicas at different Valkey instances during a rolling update.

## Explicit non-goals

- Exact weighted fair queuing or token-by-token tenant scheduling. That requires
  a tenant-aware serving scheduler below Phoebe; static lanes only bound
  contention.
- Exact rendered prompt tokens/bytes. Phoebe does not own chat templates or
  tokenizers and uses full JSON bytes as a conservative input-work proxy.
- Direct control of vLLM KV blocks, prefix-cache eviction, or resident LoRA
  adapters.
- Replacing KAI/Grove/Dynamo pod, graph, router, or engine scheduling.
- Contract entitlement or spend enforcement, and invoice computation.
