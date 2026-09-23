# Shared-inference admission and fairness ownership

## Decision point

Shared inference follows this path:

`Traefik → atlas-auth → Phoebe → Dynamo frontend/router → engine`

Phoebe admits after atlas-auth has supplied trusted identity and after Phoebe
has resolved and bound `(organization, model)`. It admits before the first
engine request. Valkey Lua transactions coordinate the soft fairness and
capacity gate across Phoebe replicas. If that store is unavailable or reset,
otherwise authorized and valid traffic continues with admission limits
temporarily bypassed. Authorization remains fail closed, and independent
engine-usage metering continues through its Valkey/WAL/log path.

## Capability audit

| Capability | Classification | Owner / boundary |
| --- | --- | --- |
| Authentication, route authorization, org/model registry | Already implemented | atlas-auth and Atlas decide access; Phoebe only consumes the trusted result. |
| Platform, graph, org, org×model admission | Saturn-owned, implemented here | Atomic Valkey reservations in Phoebe. |
| Concurrent prefills and input work | Saturn-owned, implemented conservatively | Phoebe reserves concurrent prefills and complete request-body bytes, then releases those physical guards on the first upstream body byte. Model-specific rendered templates/token counts stay in the engine. |
| Reserved decode slots/streams and output | Saturn-owned, implemented here | A future decode slot and `max_tokens`/`max_completion_tokens` are reserved before forwarding and held until completion, abort, timeout, rejection, or upstream failure. The independent prefill cap still bounds that phase. Reserving decode capacity early avoids attempting to queue after response streaming begins. |
| Request and token-throughput windows | Saturn-owned, implemented here | Separate request, total-prompt, uncached-prompt, and generated-token windows. Admission reserves `ceil(original JSON bytes / 4)` as fully uncached input plus the declared maximum output; engine usage then reconciles estimates to exact total, cached, uncached, and generated currencies. Concurrent estimates therefore cannot overbook a window, overestimates are refunded, and underestimates create debt for later requests. |
| Cold holds and wake churn | Partially implemented before; completed here | Phoebe already detected/actuated wake-from-zero. Distributed hold and wake-window admission now surrounds actuation. |
| Adapter and KV pressure | Layered implementation | Phoebe bounds active adapter-bearing requests and reserved work. Saturn enables backend prefix caches and KV event publication; Dynamo routes on actual cache residency and load, while each engine owns allocation/eviction. |
| Operator protected lanes | Layered implementation | An operator-owned organization→lane map partitions capacity. Phoebe replaces client hints with the lane's trusted Dynamo soft and strict priorities. Admission lanes are unrelated to Atlas instance-size tiers and UsageLimits names. |
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

The public token contract never uses an unqualified "tokens" value. Its four
rate dimensions are requests, total prompt tokens (cached plus uncached),
uncached prompt tokens (the subset requiring prefill compute), and generated
tokens. Cached prompt tokens therefore participate in total-prompt throughput
without being double-counted as uncached work. This mirrors the resource-aware
shared-inference convention while keeping invoice prices independent.

Saturn stamps the authenticated stable owner ID and two independent four-currency
contracts through gateway ForwardAuth: one for the organization and one for the
user/group owner. Traefik allowlists that complete envelope, so client-supplied
identity or ceilings are overwritten before Phoebe. Phoebe enforces the
organization contract at an aggregate organization scope and the owner contract
at a stable user/group-owner scope. API-key rotation cannot reset an owner budget,
and another owner cannot bypass the aggregate organization budget.
Zero means no contractual ceiling within that scope, while independent operator
capacity limits still apply. Saturn must stamp every field; a missing field is a
broken trusted contract and fails closed, while the explicit string `0`
represents unlimited.
An exhausted contract returns 429 with Retry-After, and physical pool
saturation returns 503. Admission-store unavailability bypasses only these
soft limits and is logged; it does not bypass trusted-policy validation.

Phoebe can reserve generated capacity strictly before dispatch because the
request declares a maximum output. It cannot derive exact rendered prompt or
cache-hit counts from raw OpenAI JSON without duplicating Dynamo's model chat
templates, tokenizer, and live KV lookup. Instead it reserves the cheap JSON
byte estimate before dispatch, conservatively assigns it to both total and
uncached prompt currencies, and settles the engine's authoritative usage at
completion. Request-body bytes remain a separate instantaneous physical guard
and are never written as billing usage.

## Trusted Dynamo request policy

For admitted shared requests, Phoebe selects the configured admission lane from
the operator-owned `organizationLanes` map (falling back to `default`), then
normalizes `nvext.agent_hints.priority`, `strict_priority`, and `osl` from the
operator-owned lane and output-token
reservation. It also replaces `X-Dynamo-Request-Priority` and
`X-Dynamo-Request-Strict-Priority`, because Dynamo gives those headers
precedence over body hints. A client therefore cannot self-promote.

Phoebe derives `X-Tenant-ID` and `nvext.cache_salt` from a one-way hash of the
trusted organization identity. Dynamo gives the header precedence and uses it
for router and backend cache namespacing, preventing identical prompts from
sharing KV entries across organizations without disclosing the Saturn org id.
Phoebe removes direct worker/rank selection, pre-tokenized input, and
speculative-prefill hints: those controls would bypass routing/tokenization or
create background work outside the request's reservation. Unrelated `nvext`
fields are preserved.

Dynamo tokenizes the rendered prompt and its WSPT queue charges uncached prompt
tokens. Phoebe's pre-tokenization estimate avoids duplicating model chat
templates while exact engine-side prompt work still controls settlement and
scheduling.

## Lifecycle and crash recovery

An admitted request owns a lease. Physical request-byte and concurrent-prefill
counters release at the first response body byte. Active request, reserved
decode slot, output, adapter, estimated-input, and generated-window reservation
state release at final body completion, when authoritative total-prompt,
uncached-prompt, and generated counts are atomically settled. The owning proxy
renews the expiry throughout long streams. Pre-header aborts, upstream errors, ordinary
rejections, wake failures, and handler early returns run the same idempotent
release. A replica crash cannot execute cleanup, so every operation first reaps
expired leases. `leaseTtl` is the maximum crash-leak interval, not a stream
duration limit. Phoebe rejects values below one second so the lease remains
comfortably longer than its 100 ms admission-Valkey operation budget. A Valkey
reset starts fresh best-effort counters; callbacks for
old random lease IDs become no-ops, while independent metering remains the
usage authority.

Output-limit fields are accepted at most once in the top-level JSON object.
Ambiguous duplicate keys are rejected before forwarding so Phoebe and the
downstream JSON stack cannot select different values and under-reserve work.

## Rollout and rollback

Component deployment order is independent while admission remains disabled.
Saturn emits both the new independent
eleven-header envelope and a conservative five-header legacy projection. Traefik
allowlists both during the rolling upgrade. New Phoebe prefers the complete new
envelope, rejects a partial one, and accepts the complete legacy envelope only
when every new field is absent. While admission is disabled, Phoebe does not
require a policy envelope, allowing Saturn's producer headers to roll out after
the proxy. Once admission is enabled, missing or partial policy fails closed.
The legacy service-tier marker is constant and never selects an admission lane. Remove the five compatibility headers after all
Saturn, Traefik, and Phoebe replicas use the new contract.

Keep `admission.enabled: false` until Saturn, Traefik, and all Phoebe replicas
have the new envelope contract; enabling earlier is unsupported because an old
edge does not authenticate the new headers. Then configure one shared Valkey
and conservative measured limits and enable admission. Watch
429 contract rejections, 503 capacity rejections by scope/dimension, and logged
Valkey bypass/latency errors. Roll back by disabling the feature; existing leases expire without affecting billing or Dynamo. Do not
point replicas at different Valkey instances during a rolling update.

The envelope requirement applies to every shared-inference request, not only
gateway-marked requests. The historical per-resource auth path does not stamp
this policy contract, so drain or remove those shared routes before enabling
admission; once enabled, a missing or partial envelope on such a route fails
closed with 503 instead of silently granting unlimited quota.
Per-resource routing metadata must also be internally coherent: shared mode
requires a non-empty served-model allow-list, dedicated or legacy-empty mode
requires that allow-list to be absent, and unknown modes fail closed. This keeps
model authorization and the shared isolation/admission boundary inseparable.
During the admission-disabled migration window, a historical shared route that
lacks organization identity remains isolated by its trusted resource ID rather
than sharing the empty-organization cache namespace. Enabling admission also
requires a non-empty trusted organization ID and rejects a broken identity
contract before the Valkey fail-open boundary. Gateway registry entries must
resolve with `serving_mode: shared`; empty or dedicated entries fail closed.

## Enforcement boundaries

Fairness is deliberately layered instead of pretending one counter is an exact
GPU cost model. While its store is healthy, Phoebe provides globally coordinated
admission plus trusted tenant/lane metadata; store failure deliberately weakens
only that soft fairness layer. Dynamo uses actual tokenization, uncached-prefix work, KV residency,
and active output blocks for routing. vLLM and SGLang apply supported engine
priority and cache policy. KAI and Grove govern the graph pods. These layers do
not promise mathematical token-by-token weighted fair queuing, but each
available control surface is configured and no layer accepts tenant-selected
priority.
