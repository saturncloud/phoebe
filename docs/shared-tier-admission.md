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
An absent limit header means no contractual ceiling for that field (R4), while
independent operator capacity limits still apply. An explicit `0` is a zero
ceiling that blocks the scope. The owner id is the required anchor: limit
headers without it, or a malformed value, fail closed.
An exhausted contract returns 429 with Retry-After, and physical pool
saturation returns 503. Admission-store unavailability bypasses only these
soft limits and is logged; it does not bypass trusted-policy validation.

Contract limits have no enablement switch (ruling R12 clarification,
2026-10-06). They are Atlas database settings that can be changed at any time,
and Phoebe enforces whatever the trusted envelope carries whenever an admission
store is configured. The store is `admission.valkeyAddr` when set and otherwise
the install Valkey that metering uses (`emit.valkeyAddr`); admission keys live
under their own prefix.

The store settings `admission.keyPrefix`, `admission.leaseTtl`, and
`admission.defaultMaxOutputTokens` therefore apply when `admission.enabled` is
false too, whenever a store resolves. They are defaulted (`phoebe:admission`,
`15m`, 4096) and `leaseTtl` is validated (it must parse and be at least 1s,
otherwise startup fails). With no store at all and `admission.enabled=false`,
`leaseTtl` is not used and not validated. Keep these keys the same when you
flip `admission.enabled`: changing `keyPrefix` resets every contract window
counter and strands in-flight leases under the old prefix.

Sharing the metering Valkey couples admission to metering. The shared install
Valkey runs with `noeviction` and a 128mb memory cap. If the metering drainer
is down long enough for the stream backlog to fill that cap, the admission
scripts fail with an `OOM` error from Valkey. Admission treats that as an
unavailable store, so contract limits are bypassed until the backlog drains.
Each bypass is logged, and an OOM is logged with its own message
("admission store out of memory") so it can be told apart from a network
outage. Phoebe logs at startup when the admission store comes from
`emit.valkeyAddr`. Operators who need contract limits isolated from metering
backlogs should set `admission.valkeyAddr` to a dedicated Valkey, which must
also run with `noeviction` (an evicting policy can drop admission hashes and
silently reset contract counters).

`admission.enabled` controls only the operator side:

| `admission.enabled` | Contract scopes (envelope limits) | Operator capacity tiers | Shared request with no envelope |
| --- | --- | --- | --- |
| `false` (chart default) | Enforced: 429 + Retry-After | Off (not configured) | Served without admission (historical per-resource routes) |
| `true` | Enforced: 429 + Retry-After | Enforced: 503 + Retry-After | Fails closed with 503 |

In both modes a request that carries any part of the envelope (the owner-id
anchor or any limit header) goes through admission's identity and policy
checks, so a partial or malformed envelope, a missing organization identity,
or an underivable graph scope fails closed with 503. With
`admission.enabled=false`, a request whose envelope carries no limit header is
unlimited in every scope and does not touch the store: no Admit, no lease, no
completion. Every operator scope, the lane included, is checked before the
contract scopes inside the one atomic transaction, so when both are exhausted
at the same request the answer is 503.

A request that alone reserves more than a contract scope's whole per-window
limit can never be admitted, so it gets 400 without Retry-After instead of a
429 that would repeat forever: a `max_tokens` above the generated-token limit,
or an estimated prompt above a prompt-token limit. A request that declares no
`max_tokens` reserves the configured default (4096) clamped to the smallest
positive contract generated-token limit, and `/v1/embeddings` reserves one
output token, since embeddings generate none.
Only a Phoebe with no Valkey at all (no `admission.valkeyAddr` and no
`emit.valkeyAddr`) cannot enforce contract limits; it logs that at startup.

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

The quota envelope is the scoped contract only: `X-Saturn-Owner-Id` plus
the four `X-Saturn-Org-Rate-Limit-*` and four `X-Saturn-Owner-Rate-Limit-*`
headers, stamped by the gateway ForwardAuth. The owner id is the required
anchor; each absent limit header means unlimited for that field (R4).

The five legacy single-scope quota headers (`X-Saturn-Service-Tier`,
`X-Saturn-Rate-Limit-Requests`, `X-Saturn-Rate-Limit-Total-Prompt-Tokens`,
`X-Saturn-Rate-Limit-Uncached-Prompt-Tokens`,
`X-Saturn-Rate-Limit-Generated-Tokens`) were removed in one cutover release
(ruling R8, a hard cut). In that release Saturn stops stamping them, Traefik
stops allowlisting them, the phoebe chart drops them from `trustedHeaders`,
and Phoebe stops reading them. The strip outlives the trust: the edge keeps
blanking client-supplied copies of the five names until every Phoebe replica
runs the R8 image (see below). There is no dual-contract window after the cutover and no
fallback: a request that carries only the legacy headers and no
`X-Saturn-Owner-Id` has no policy and, with admission enabled, fails closed
with 503.

The four components of that release were safe to roll in any order only
because no replica enforced a policy envelope during the rollout: admission was
disabled (`admission.enabled: false`) and Phoebe at the time did not admit
without it. Phoebe now enforces the scoped envelope whenever it is present,
which relies on the edge strip of every `X-Saturn-*` header that the R8
releases already ship.
No current or pre-cutover Phoebe reads the legacy headers on dedicated
routes. The scoped envelope is stamped only by the gateway ForwardAuth, on
gateway routes; every Phoebe that still read the legacy headers preferred the
scoped envelope whenever the owner id was present. Historical per-resource
shared routes never carried the policy contract, and R8 does not change that:
they must be drained or removed before admission is enabled (see the
requirement later in this section).

The five legacy names leave the trust set but must not leave the edge strip
lists in the same release. A Phoebe replica older than the R8 image still
parses the legacy envelope, and during a mixed rollout it would accept a
client-forged `X-Saturn-Service-Tier` / `X-Saturn-Rate-Limit-*` set on any
admission-enabled request that has no `X-Saturn-Owner-Id`. The edge strip has
always prevented that, and the R8 image cannot protect older replicas by
stripping in its own code. So the names stay stripped at the edge, without
being trusted: the traefik chart's `tf-gateway-headers` middleware blanks
them on the gateway route, and the phoebe chart's
`phoebe-inference-headers` middleware must blank them on the standard route
through a strip-only list that is not rendered into `PHOEBE_TRUSTED_HEADERS`.
On the standard route this breaks the usual equality of the strip set and the
trusted set (ruling R3) on purpose: the strip set is the trusted set plus the
five legacy names. Remove the strip-only names only in a later release, after
every Phoebe replica runs the R8 image. The recommended order is phoebe #55
first (or in the same window), then Atlas #6715, with saturn-k8s #1073
alongside either. Nothing reads these headers after the cutover, so dropping
the strip once no pre-R8 replica remains is harmless.

While `admission.enabled` is false, Phoebe does not require a policy envelope
on every shared request, but it enforces any envelope that is present. Once
`admission.enabled` is true, a missing or structurally broken policy fails
closed.

Set `admission.enabled: true` only after Saturn, Traefik, and all Phoebe
replicas have the scoped envelope contract and historical per-resource shared
routes are drained (see below). Then configure conservative measured operator
limits. Watch 429 contract rejections, 503 capacity rejections by
scope/dimension, and logged Valkey bypass/latency errors. Turning
`admission.enabled` off again removes the operator tiers and the
every-request envelope requirement; it does not stop contract enforcement,
which is changed in Atlas UsageLimits. Existing leases expire without affecting
billing or Dynamo. Do not point replicas at different Valkey instances during a
rolling update.

The envelope requirement applies to every shared-inference request, not only
gateway-marked requests. The historical per-resource auth path does not stamp
this policy contract, so drain or remove those shared routes before enabling
`admission.enabled`; once it is on, a missing or partial envelope on such a
route fails closed with 503 instead of silently granting unlimited quota.
Per-resource routing metadata must also be internally coherent: shared mode
requires a non-empty served-model allow-list, dedicated or legacy-empty mode
requires that allow-list to be absent, and unknown modes fail closed. This keeps
model authorization and the shared isolation/admission boundary inseparable.
While `admission.enabled` is false, a historical shared route that
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
