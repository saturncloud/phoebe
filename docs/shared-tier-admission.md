# Shared-tier admission and fairness ownership

## Decision point

Shared inference follows this path:

`Traefik → atlas-auth → Phoebe → Dynamo frontend/router → engine`

Phoebe admits after atlas-auth has supplied trusted identity and after Phoebe
has resolved and bound `(organization, model)`. It admits before the first
engine request. The Valkey Lua transaction is the shared source of truth for
every Phoebe replica; an unavailable store returns HTTP 503. If that authority
is lost during renewal, Phoebe cancels the affected upstream request rather
than allowing its capacity lease to expire under live work. A local fallback
would violate the global bound and is intentionally absent.

## Capability audit

| Capability | Classification | Owner / boundary |
| --- | --- | --- |
| Authentication, route authorization, org/model registry | Already implemented | atlas-auth and Atlas decide access; Phoebe only consumes the trusted result. |
| Platform, graph, org, org×model admission | Saturn-owned, implemented here | Atomic Valkey reservations in Phoebe. |
| Concurrent prefills and input work | Saturn-owned, implemented conservatively | Phoebe reserves concurrent prefills and complete request-body bytes, then releases on the first upstream body byte. Model-specific rendered templates/token counts stay in the engine. |
| Active decodes/streams and reserved output | Saturn-owned, implemented here | A decode slot and `max_tokens`/`max_completion_tokens` are reserved before forwarding and held until completion, abort, timeout, rejection, or upstream failure. Reserving the slot early is conservative and avoids attempting to queue after response streaming begins. |
| Request and generated-token windows | Saturn-owned, implemented here | Fixed operator-configured windows. Generated usage is charged from the engine's authoritative completion count; outstanding max-output reservations prevent overbooking. |
| Cold holds and wake churn | Partially implemented before; completed here | Phoebe already detected/actuated wake-from-zero. Distributed hold and wake-window admission now surrounds actuation. |
| Adapter and KV pressure | Layered implementation | Phoebe bounds active adapter-bearing requests and reserved work. Saturn enables backend prefix caches and KV event publication; Dynamo routes on actual cache residency and load, while each engine owns allocation/eviction. |
| Service-tier protected shares | Layered implementation | Operator-owned org→tier mapping prevents client selection. Hard Phoebe lanes protect capacity; Phoebe also replaces client hints with the tier's trusted Dynamo soft and strict priorities. |
| Cache-aware/worker routing and prefill/decode scheduling | Saturn-configured Dynamo capability | Shared graphs enable KV routing, output-block tracking, a capacity-triggered WSPT queue, and backend priority scheduling where supported. |
| Gang lifecycle and replica topology | Saturn-configured Grove/Dynamo capability | The graph remains the lifecycle and gang unit; request fairness is enforced above and inside it. |
| GPU queue quota, DRF, placement, and preemption | Saturn-configured KAI capability | KAI protects graph/pod scheduling. Many organizations share one graph, so organization request fairness cannot be delegated to a pod scheduler. |

## Currencies stay separate

Admission counters protect physical service health. Billing continues to use
immutable engine-reported usage events: no admission estimate is written as
invoice usage, and no price or spend balance is used as a GPU-pressure signal.
Contract limits are a separate policy input even when they are enforced at this
same gate, because a physical-capacity rejection and an exhausted entitlement
have different ownership, status, and reset semantics.

## Trusted Dynamo request policy

For admitted shared requests, Phoebe normalizes `nvext.agent_hints.priority`,
`strict_priority`, and `osl` from the operator-owned tier and output-token
reservation. It also replaces `X-Dynamo-Request-Priority` and
`X-Dynamo-Request-Strict-Priority`, because Dynamo gives those headers
precedence over body hints. A client therefore cannot self-promote.

Phoebe derives `X-Tenant-ID` and `nvext.cache_salt` from a one-way hash of the
trusted organization identity. Dynamo gives the header precedence and uses it
for router and backend cache namespacing, preventing identical prompts from
sharing KV entries across organizations without disclosing the Saturn org id.
Unrelated `nvext` fields are preserved.

Dynamo tokenizes the rendered prompt and its WSPT queue charges uncached prompt
tokens. Phoebe intentionally keeps a conservative full-request-byte
reservation as the pre-tokenization distributed safety bound; this avoids
duplicating model chat templates while still letting exact engine-side prompt
work affect scheduling.

## Lifecycle and crash recovery

An admitted request owns a lease. Input/prefill counters release at response
body byte; active request/decode, output, adapter, and generated-window reservation state
release at final body completion. The owning proxy renews the expiry throughout
long streams. Pre-header aborts, upstream errors, ordinary
rejections, wake failures, and handler early returns run the same idempotent
release. A replica crash cannot execute cleanup, so every operation first reaps
expired leases. `leaseTtl` is the maximum crash-leak interval, not a stream
duration limit.

Output-limit fields are accepted at most once in the top-level JSON object.
Ambiguous duplicate keys are rejected before forwarding so Phoebe and the
downstream JSON stack cannot select different values and under-reserve work.

## Rollout and rollback

Roll out with `admission.enabled: false`, deploy all Phoebe replicas, configure
one shared Valkey and conservative measured limits, then enable admission. Watch
429s by scope/dimension and Valkey latency/errors. Roll back by disabling the
feature; existing leases expire without affecting billing or Dynamo. Do not
point replicas at different Valkey instances during a rolling update.

## Enforcement boundaries

Fairness is deliberately layered instead of pretending one counter is an exact
GPU cost model. Phoebe provides global hard admission and trusted tenant/tier
metadata. Dynamo uses actual tokenization, uncached-prefix work, KV residency,
and active output blocks for routing. vLLM and SGLang apply supported engine
priority and cache policy. KAI and Grove govern the graph pods. These layers do
not promise mathematical token-by-token weighted fair queuing, but each
available control surface is configured and no layer accepts tenant-selected
priority.
