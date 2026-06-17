# Phoebe — Design & Decisions

Phoebe is the **token-metering interceptor** for Saturn Cloud's token factory: a
thin, tenant-aware reverse proxy behind Traefik and in front of the (optional)
inference router / vLLM engine.

```
client → Traefik → atlas-auth (ForwardAuth) → Phoebe → [vLLM prod router | llm-d] → vLLM / SGLang / TensorRT-LLM
```

This document records the **decisions and their rationale** — the things that
aren't recoverable from reading the code. It is the source of truth for *why*
Phoebe is shaped the way it is. Each decision notes its status and, where
relevant, the condition under which it should be revisited.

---

## 1. Trust model (verified against deployed code)

Phoebe does **not** authenticate or authorize. It trusts the identity headers
`atlas-auth` injects, because Traefik's ForwardAuth guarantees they only arrive
on already-authorized requests.

**The one invariant the whole design rests on:** a client cannot spoof the
`X-Saturn-*` headers. This is enforced by Traefik's `authResponseHeaders`
*allowlist* in the `atlas-auth` middleware
(`saturn-k8s/charts/traefik/templates/middleware/fowardauth.yaml`), which
overwrites/removes any client-supplied copy of each listed header. The in-repo
comment there states this is deliberate ("must be explicitly listed … cannot be
impersonated").

**Consequence:** any header Phoebe trusts for billing MUST be on that allowlist.
Reading a header Phoebe trusts that is *not* allowlisted would be a billing-spoof
vulnerability. (See §3.)

Verified in: `auth-server/auth/server.go` (`setHeaders`, the four `X-Saturn-*`
constants), `saturn-k8s` forwardauth middleware. Authorization is URL-based:
auth-server calls Atlas `/check?resource_url=…` and only emits headers + a 204
(which tells Traefik to forward) after access passes.

**Known limitation — client-supplied `X-Request-Id` reuse:** `X-Request-Id` is
NOT on the allowlist, so it is client-controlled — yet it is the billing
idempotency key (`billing_event`'s primary key). The proxy gates it fail-closed
(absent → server-generated `phoebe-<32 hex>`; >200 chars or non-printable-ASCII
→ 400), so a client cannot dodge billing by omitting the header or wedge the
drainer with an oversize value. What the gate canNOT stop is a client
deliberately **resending a previously billed valid id**: the `ON CONFLICT
(request_id) DO NOTHING` dedup treats it like a stream redelivery, so the
replayed request is served but billed zero. At-least-once delivery makes
request_id-dedup load-bearing, so the drainer cannot distinguish replay from
redelivery; the true fix is an edge-stamped, allowlisted request id (same
pattern as §3). Accepted gap until that lands.

---

## 2. Billing attribution: token-id as the key (decided)

The billing requirement is to **tie token consumption to a specific API key**.

The clean primitive already exists: every token (browser-session OR API-key) is
a signed JWT whose `sub` claim is the Atlas `IdentityAuth.id` — a stable,
DB-backed token identity, identical mechanism for both auth paths (they differ
only by whether `session_id` is set). Verified in
`saturn/pdc/models/authorization.py` (`sign_access_token`) and
`saturn/pdc/basehandler.py` (browser vs API paths share the token).

**Decision:** Phoebe captures the token id as the primary attribution key
(`AuthID`), and **org/user/group are resolved downstream, out of band, at rating
time** from the `IdentityAuth` record. The hot path never resolves a user's
*active org*.

**Why downstream, not in the token:** a user can belong to **multiple orgs**
(`owner` is a user↔org many-to-many; a group is scoped to exactly one org). The
token does not carry which org a user is *currently acting under* — that context
isn't in the JWT or the session at mint time. So org cannot be resolved on the
hot path without a DB lookup. Resolving it downstream from the token id (which
*is* in the token) sidesteps the entire problem. Verified in
`saturn/pdc/models/owner.py`, `org.py`, `group.py`, `pdcuser.py`.

**What Phoebe captures:** everything the edge gives it — `AuthID`, `UserID`,
`GroupID`, `ResourceID`, `ResourceType` — verbatim onto the metering event. No
information is dropped; rating decides what matters.

---

## 3. The `X-Saturn-Auth-Id` header (in flight)

To carry the token id to Phoebe, three coordinated changes (each a PR):

| Layer | Change | PR |
|---|---|---|
| auth-server | emit `X-Saturn-Auth-Id` from `claims.Subject` (the JWT `sub`) | auth-server#85 |
| Traefik (saturn-k8s) | add `X-Saturn-Auth-Id` to the `atlas-auth` allowlist | saturn-k8s#976 |
| Phoebe | capture it into `identity.Identity` / `metering.Event` | phoebe#1 |

**No Atlas token-minting change is needed** — `sub` is already the token id.

**Security (verified safe):** the value is a claim inside the cryptographically
validated JWT, so it's trustworthy transitively; and the allowlist entry
(saturn-k8s#976) makes it un-spoofable. **The allowlist entry is the security
control** — Phoebe must not rely on this header in production until it is live.
Deploy order: allowlist first (safe — it just strips a header nobody sends yet),
then auth-server, then Phoebe.

---

## 4. Fail closed on missing billing identity (decided, in phoebe#1)

A billing product must not serve traffic it can't attribute. Phoebe rejects with
`400` (before any upstream work) if `X-Saturn-Auth-Id` **or**
`X-Saturn-Resource-Id` is absent, naming every missing field. `UserID`/`GroupID`
are NOT required (resolved downstream). A rejected request emits no billing
event.

This is the Ben-Perry "fail closed, assert the negative" instinct: ambiguity in
who-to-bill resolves to "don't serve," and the negative cases are tested by name.

When the header can legitimately be absent: during rollout (before #85/#976 are
live), version skew against an old auth-server, or direct-to-Phoebe traffic
bypassing Traefik. The guard exists for those, not the happy path.

---

## 5. Durability: tiered, Postgres is the system of record (decided)

The durability ladder, in order of authority:

```
Phoebe.Emit (non-blocking, off hot path)
   → Valkey Streams   (hot buffer; at-least-once via consumer groups; request_id dedup)
   → local-disk WAL   (best-effort fallback when Valkey is unreachable)
   → structured log   (last-resort floor)

   ... drainer (separate service) ...
   → Postgres         (SYSTEM OF RECORD — the durable billing record)

   ... reconciliation (future) ...
   → engine request logs ↔ Postgres   (backstop for the crash window)
```

**Decisions:**
- **Postgres is the system of record**, not Valkey and not the WAL. The unbilled
  dollar ultimately lives in Postgres. Valkey is a hot buffer; the WAL is a
  best-effort latency buffer.
- **Valkey, not Kafka.** Kafka is net-new and JVM-based (a stated non-goal).
  Redpanda (JVM-free Kafka) was considered and kept as a future option, but
  Valkey Streams + Postgres covers the requirement with tools already run.
- **Valkey, not Redis.** Valkey is the BSD-licensed Linux-Foundation fork; for a
  product shipped/embeddable for neoclouds, the permissive license avoids
  Redis's SSPL/AGPL. Same Streams API. (Mirrors the org's OpenSearch-over-
  Elasticsearch posture.)
- **WAL loss on pod death is ACCEPTABLE — but in-process loss during a drain is NOT.**
  Two distinct losses, only one is tolerated:
  - *Pod death* with un-shipped WAL events during a Valkey outage is a tolerable,
    bounded, double-failure loss — Postgres is the system of record and
    reconciliation is the backstop. **This is what lets Phoebe be a stateless
    `Deployment`** — no StatefulSet, no per-replica PVC, `emptyDir` WAL is fine.
    Do not "fix" *this* by demanding durable per-replica storage; that re-couples
    Phoebe to stateful infra for no benefit.
  - *A live, healthy pod silently dropping an event during a drain is a bug, not a
    tolerated loss.* The WAL is **`github.com/tidwall/wal`** (chosen over the
    earlier hand-rolled file: small, MIT-licensed, widely tested, go1.13 — and
    the hand-rolled rotate-to-snapshot scheme had real loss modes: a fixed
    snapshot path that a later rotation clobbered during multi-tick outages,
    whole-snapshot deletion after partial reads, and a 64KB line limit).
    Entries are appended at strictly increasing indexes (fsync per write); the
    shipper reads unshipped entries in batches and reclaims space with
    `TruncateFront` **only after each batch is confirmed shipped**, advancing a
    shipped-through watermark. Loss during a drain is structurally impossible:
    an entry is only ever deleted by truncation behind the confirmed-shipped
    index, and an append concurrent with the (network) ship lands at a higher
    index that the truncation never touches. Partial-outage progress is kept
    batch-by-batch rather than all-or-nothing. One wrinkle: tidwall/wal cannot
    truncate to empty, so a fully-shipped log retains its final entry and the
    watermark lives in memory — after a restart that single entry is re-shipped
    once (at-least-once; consumer dedups on `request_id`). A WAL directory that
    fails to open is quarantined aside to `<dir>.corrupt.<ts>` (bounded, loudly
    logged loss — serving is never blocked by a corrupt buffer), and a legacy
    single-file JSONL WAL found at the configured path is auto-imported on
    upgrade. See `internal/emit/wal.go` `append`/`pending`/`markShipped`.

**Status:** Valkey emit + WAL + log floor are BUILT (`internal/emit`). The
Postgres drainer and reconciliation are NOT yet built (see §8).

---

## 6. Replica safety (analyzed)

- **Reverse-proxy hot path:** stateless per-request; scales freely across Phoebe
  replicas. No sticky routing, no coordination.
- **Model replicas are Kubernetes' problem, not Phoebe's.** Phoebe resolves a
  model to a **Service** address; the Service load-balances across engine pods.
  Engine scale/churn is invisible to Phoebe.
  - **Invariant:** resolvers MUST return stable **Service-level** addresses,
    never pod addresses. A lookup that returned a pod IP would re-introduce
    model-replica coupling — that would be a resolver-contract bug.
- **Resolver cache is per-Phoebe-replica** (LRU + TTL). Benign: worst case is
  one TTL of routing-freshness skew across replicas; the short negative-TTL
  bounds it.
- **WAL is node-local** — see §5; loss is accepted, so this is not a problem.

---

## 7. Model dispatch (built: M4)

`X-Saturn-Resource-Id` → upstream URL, with no redeploy for new models and a
clean 404/410 for torn-down ones. Strategies (`internal/registry`):
- `ConventionResolver` — zero-lookup `{id}` template → a Service DNS name (the
  clean, Kubernetes-native path).
- `CachedResolver` — wraps a control-plane `LookupFunc` with LRU + positive/
  negative TTL + single-flight. The `LookupFunc` is the seam for a real Atlas
  resource-resolution call.
- `ChainResolver` — cached → convention fallback (graceful degradation when the
  control plane is unreachable).

**Open verify-gate:** does Atlas mint resource tokens whose `Resource` claim
carries the **model/deployment id** for inference routes, and does `/check`
authorize those model URLs? Until confirmed, the `LookupFunc` degrades to the
convention template. This is an Atlas-side question, not a Phoebe or auth-server
one.

---

## 8. I/O logging store: Postgres-first (decided), OpenSearch designed-for-not-built

M5 is per-tenant, **opt-in, off by default, sampled, short-retention** capture of
request/response bodies for tenant troubleshooting — forked from the *same*
captured bytes as metering, but to a separate store with its own retention/access.

**Decision: Postgres-first.** Storage sits behind an `IOLogSink` interface with a
**Postgres** implementation (full-text search via `tsvector`/GIN for lexical
"grep inside bodies"; `pgvector` available in the same store for future semantic
search). **OpenSearch is designed-for but NOT built** — a second `IOLogSink`
implementation to add only if a real requirement forces it.

**Why Postgres, not OpenSearch (the Ben-Perry framing, anchored in stakes not
taste):**
- The verified requirement is full-text *at M5's scale* — which is small by
  design (sampled, opt-in, short-retention). At that scale Postgres FTS meets the
  need with a small GIN index; the OpenSearch "full-text at scale" justification
  doesn't apply.
- OpenSearch is the heavier, more operationally demanding of two **already-
  deployed** stores (stateful, JVM, shard/heap/ISM tuning). Adding a billing-
  adjacent workload to the *less reliable* operated store is the wrong move;
  consolidate onto the store we run confidently.
- "Don't take a heavyweight dependency you can replace cheaply"; "abstract the
  contract, not the code" → build the interface + the one sink needed today;
  leave OpenSearch as a typed hole.

**The tripwire (documented at the seam, per Ben's "comment the why + the
condition"):** if I/O logging ever changes from *sampled troubleshooting* to
*log-everything / long-retention*, the corpus explodes and Postgres FTS would
strain — that is the condition that justifies building the OpenSearch sink.
Until then, Postgres.

**Status:** NOT built. The `IOLogSink` interface + Postgres sink is the M5
mechanism to build (see §10).

---

## 9. Postgres conventions (to follow — from the Atlas codebase)

The drainer's `billing_event` table and the M5 I/O-log table live in the **shared
Atlas Postgres** and must follow Atlas conventions (verified in `saturn/`):

- **Connection:** `DATABASE_URL` env var, injected from the `atlas-secrets`
  k8s secret. Driver `postgresql://…` (Go `database/sql` + `pgx`/`lib/pq`).
- **IDs:** `Unicode(32)` hex UUID (`uuid.uuid4().hex`, no dashes).
- **Timestamps:** `created_at` as `DateTime(timezone=True)`, UTC default. No
  `updated_at` by convention.
- **Tables:** snake_case, **singular** (`identity_auth`, `org`, `owner`,
  `hourly_usage_record`).
- **Constraint naming:** `pk_%(table)s`, `fk_%(table)s_%(col)s_%(reftable)s`,
  `ix_…`, `uq_%(table)s_%(col)s`.
- **Migrations:** Alembic, in `saturn/alembic/versions/`, run by a pre-deploy
  setup hook (`alembic upgrade head`). **The drainer is Go and cannot run
  Alembic** — see the decision in §10.
- **Token→org join path:** `identity_auth.id` → (`user_id`→`owner.user_id`) or
  (`group_id`→`owner.group_id`) → `owner.org_id` → `org`. This is the downstream
  rating resolution (§2), not Phoebe's job.
- **Existing billing table:** `hourly_usage_record` (rated, units in 0.0001 USD)
  exists. Phoebe's metering is **raw counts** (pre-rating) — a *separate*
  `billing_event` table, no collision. There is no `token_count`/metering table
  yet — greenfield.

---

## 10. Open decisions flagged during autonomous build

These were defaulted (defensibly) while building unsupervised; flagged for review.

- **[DECISION] Schema migration ownership.** The drainer is Go; Atlas migrations
  are Alembic (Python). Default chosen: **define the `billing_event` table as an
  Alembic migration in the Atlas repo** (schema lives with all other schema, run
  by the existing setup hook), and the Go drainer only *uses* the table — it does
  not own migrations. Rationale: one migration tool per shared DB; don't put a
  second migration system on the Atlas Postgres. Revisit if Phoebe should own a
  *separate* database instead of sharing Atlas's.
- **[VERIFY-GATE] Atlas resource tokens for inference routes** (§7) — Atlas-side.
- **[VERIFY-GATE] `--enable-prompt-tokens-details`** must be set on every vLLM
  serving deployment or cached-token counts are always absent (verified against
  vLLM source). A deployment requirement, surfaced for whoever stands up engines.
- **[UN-VERIFIED] No real-engine validation yet.** All streaming/usage-capture
  correctness is tested against synthetic vLLM payloads + source research, never a
  live engine. This is the largest correctness gap before any production use.

---

## Status summary

| Piece | Status |
|---|---|
| Scaffold, M1 tee, M2 Valkey+WAL emit, M3 abort, M4 dispatch, main wiring | Built (on `main`) |
| Auth-id capture + fail-closed billing gate | In review (phoebe#1) |
| `X-Saturn-Auth-Id` edge wiring | In review (auth-server#85, saturn-k8s#976) |
| Postgres drainer + `billing_event` schema | To build |
| Deploy plumbing (Dockerfile in phoebe; manifests/chart in saturn-k8s) | To build |
| M5 I/O logging (`IOLogSink` + Postgres sink) | To build |
| Reconciliation backstop | Deferred (depends on drainer schema + Atlas log format) |
| Real-engine validation | Deferred (needs infra/decisions) |
