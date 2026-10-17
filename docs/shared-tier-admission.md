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
| Concurrent prefills and input work | Saturn-owned, implemented conservatively | Phoebe reserves concurrent prefills and an approximate estimated-prompt-token count (message-text characters / 4, a fixed cost per image or media part, and the raw JSON length / 4 of tool, schema, document and chat-template fields), then releases those prefill guards on the first upstream body byte. A separate reserved-KV counter (estimated prompt tokens plus reserved output x output sequences) is held until the request finishes. Model-specific rendered templates/token counts stay in the engine. |
| Reserved decode slots/streams and output | Saturn-owned, implemented here | A future decode slot and `max_tokens`/`max_completion_tokens` are reserved before forwarding and held until completion, abort, timeout, rejection, or upstream failure. The independent prefill cap still bounds that phase. Reserving decode capacity early avoids attempting to queue after response streaming begins. |
| Request and token-throughput windows | Saturn-owned, implemented here | Separate request, total-prompt, uncached-prompt, and generated-token windows. Admission reserves the estimated prompt tokens (the same approximate estimate as the prefill counter) as fully uncached input plus the reserved output (`max_tokens` x output sequences); engine usage then reconciles estimates to exact total, cached, uncached, and generated currencies. Concurrent estimates therefore cannot overbook a window, overestimates are refunded, and underestimates create debt for later requests. |
| Cold holds and wake churn | Partially implemented before; completed here | Phoebe already detected/actuated wake-from-zero. Distributed hold and wake-window admission now surrounds actuation. |
| Adapter and KV pressure | Layered implementation | Phoebe bounds active adapter-bearing requests and reserved work. `maxReservedKvTokens` bounds estimated input plus reserved output (x output sequences); that reservation is held until the request finishes. Saturn enables backend prefix caches and KV event publication; Dynamo routes on actual cache residency and load, while each engine owns allocation/eviction. |
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
templates, tokenizer, and live KV lookup. Instead it reserves the
pre-tokenization token estimate (described above) before dispatch,
conservatively assigns it to both total and uncached prompt currencies, and
settles the engine's authoritative usage at completion. The
`maxPrefillTokens` counter is the instantaneous prefill guard and is never
written as billing usage.

`maxPrefillTokens` replaced the old byte-based `maxPromptBytes` limit and is
counted in estimated tokens, not bytes. A configuration that still sets
`maxPromptBytes` at any scope fails startup; convert the value to tokens and
rename the key to `maxPrefillTokens`.

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

An admitted request owns a lease. Prefill-token and concurrent-prefill
counters release at the first response body byte. Active request, reserved
decode slot, output, adapter, kv, estimated-input, and generated-window reservation
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

## Live operator limits from the admin page (admission-policy ConfigMap)

The shared-tier admin page (Atlas) can retune the operator tiers without a phoebe
restart or a Helm change (shared-tier-admin Part B contracts). Atlas writes one
ConfigMap, `tf-admission-policy`, labelled `saturncloud.io/tf-admission-policy=true`,
in the gateway namespace (where it writes the served-model registry; phoebe's Role
already reads ConfigMaps there). Its data:

```yaml
revision: "<settings revision>"
organization.maxActiveRequests: "6"      # <tier>.<field>: a decimal integer >= 1
organizationModel.maxPrefillTokens: "50000"
graph.maxColdHolds: "4"
```

- **Keys:** `<tier>.<field>`, tier one of `platform`, `graph`, `organization`,
  `organizationModel`, field any AdmissionLimits field by its Helm key. `window`,
  `admission.enabled`, lanes, store mechanics, `protection.*` and `wake.*` stay
  Helm-only. With `admission.enabled=false` phoebe does not watch the ConfigMap. It
  also does not watch it when `gateway.enabled=false`, `gateway.legacyPostgresResolver=true`
  or `gateway.namespace` is empty, or when its kubernetes config is unavailable; in
  those cases the Helm values stay in force and phoebe logs a warning naming the
  reason (`admission policy: not watching ConfigMap tf-admission-policy (<reason>) ...`).
- **Precedence, per field:** a present key replaces the Helm value; an absent key keeps
  it. Each update is laid over the Helm values, never over the previous update. Values
  are integers >= 1, so the page can set a cap but not remove one or zero it.
- **Reload:** each interceptor replica watches the ConfigMap and swaps the admitter's
  settings atomically; the next admitted request uses them. Leases in flight are
  untouched; a cap lowered below current usage refuses new requests with 503 and
  Retry-After until usage drops. A request keeps the limits that applied when it
  was admitted. The cold-hold and wake caps (`graph.maxColdHolds`,
  `organization.maxColdHolds`, `*.wakesPerWindow`) are checked when an admitted
  request begins a cold hold, against those admit-time limits. A lowered cold-hold
  or wake cap therefore reaches requests admitted before the change only after they
  finish, which takes at most one wake timeout.
- **Fail closed:** a malformed entry (unknown key, a value that is not an integer >= 1)
  keeps the last good value for that key, or the Helm value if there never was one; the
  other entries still apply. The last good values are held in each replica's memory
  only and are lost when the pod restarts. A replica that starts while a malformed entry
  is live has no history, so it enforces the Helm value for that key, and it can differ
  from replicas that kept a last good value until Atlas writes a valid revision. Atlas
  must therefore write only valid entries (integers >= 1, known keys): a malformed entry
  plus a pod restart lets replicas enforce different caps on the shared Valkey counters.
  A kept last good value never blocks a valid entry of the same revision: if it breaks a
  per-tier rule against a new value (for example a kept `maxConcurrentPrefills` above a
  lowered `maxActiveRequests`), the kept value is lowered to that cap and the revision
  applies; a kept value is only ever lowered. If the entries are each valid but together
  fail phoebe's cross-field validation (per-tier rules, lane shares), the whole update is
  refused and the last applied settings stay. A deleted ConfigMap keeps the last applied
  settings. A ConfigMap with no `revision` key (or an empty one) is applied and logged as
  a policy with no revision, never as the Helm settings.
  Phoebe has no metrics endpoint: each apply, kept value and refusal is one log line
  with the revision (`admission policy: applied revision ...`, `... applied revision ...
  PARTIALLY from ...` naming the malformed keys when some entries kept their last good
  or Helm value, `... keeping the last good value ...`, `... no last good value in this
  process, the Helm value stays ...`, `... kept value(s) for ... were lowered so revision
  ... could apply`, `... REFUSED, keeping revision ...`, `... has no "revision" key ...`).
  At startup phoebe waits up to 10 s for the first read of the ConfigMap before it
  serves, so a restarted replica enforces the published caps from its first request.
  If the read does not complete in time, phoebe serves on the Helm values and logs an
  error (`admission policy: ConfigMap ... not read within 10s ...`) until it syncs.

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

Phoebe replicas older than the release that enforces contract limits without
`admission.enabled` (phoebe #60) do not admit requests while
`admission.enabled` is false. During a mixed rollout, only requests served by
updated replicas are counted and limited. A contract limit set in Atlas
UsageLimits is therefore under-enforced, roughly in proportion to the share of
old replicas, until every replica runs the new image. The effect is
transient and only weakens the soft fairness limit (consistent with ruling R2).
It never bypasses trusted-policy validation. Operators who need exact limits
from the first minute should set or lower UsageLimits only after
`kubectl rollout status` reports that the rollout is complete.

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

Rolling back across the versioned lease keys needs one extra step. The
release that adds the prefill-token and kv counters writes its leases under
`{<keyPrefix>}:leases:v2` and `{<keyPrefix>}:expiries:v2`, and it also reaps
expired leases from the previous release's `{<keyPrefix>}:leases` and
`{<keyPrefix>}:expiries`. That makes a forward rolling deploy safe: no lease
counter is stranded. The previous release reads only its own keys and never
reaps the versioned ones. After a rollback, any versioned lease that was still
held when its replica stopped (a crash, an OOM kill, or termination before the
in-flight request finished) is never released. Its `active`, `prefills`,
`reserved_decode_slots`, `kv`, and `prefill_tokens` counters stay incremented
in the shared `{<keyPrefix>}:counters` hash, and the graph and lane capacity
scopes under-admit with 503 until something releases them. Replicas that drain
cleanly release their own leases, so the counters are only stranded for leases
that were in flight when a replica died.

After a rollback, check whether `{<keyPrefix>}:leases:v2` is empty (`HLEN`).
If it is not, do one of the following:

- Redeploy the newer release. Its reaper releases every expired versioned
  lease within one `leaseTtl`, and nothing else is needed.
- Stay on the older release and, once it is fully rolled out, delete
  `{<keyPrefix>}:counters`, `{<keyPrefix>}:leases:v2`,
  `{<keyPrefix>}:expiries:v2`, and `{<keyPrefix>}:window-expiries` together.
  Delete the versioned lease keys along with the counters. Otherwise a later
  forward deploy would reap those leases and subtract them from counters that
  no longer include them. Deleting the counters hash also resets every
  contract window and briefly under-counts requests that are in flight at that
  moment. Both effects only loosen the soft fairness limits (ruling R2) and do
  not affect billing.

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
