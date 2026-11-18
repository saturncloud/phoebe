# Token Factory billing reconciliation and recovery

This runbook describes the deployed `release-2026.08.01` path and the boundary
between guarantees Phoebe owns and guarantees that require the edge, chart, or
central billing service.

## Request-to-invoice data flow

1. Traefik ForwardAuth strips caller-supplied `X-Saturn-*` headers and injects
   authenticated identity, organization, resource, upstream, base-model, adapter,
   and serving-mode headers. Saturn creates those route headers when the endpoint
   is created.
2. Phoebe rejects requests missing the mandatory attribution fields, stores the
   caller's `X-Request-Id` only as `client_request_id` (printable ASCII, fewer
   than 255 bytes; invalid values are rejected before forwarding), and mints a cryptographically
   random `request_id` for the billable execution attempt. Each client retry is a
   new attempt; redelivery of one metering event retains the same attempt id.
3. Phoebe forces streaming usage on, forwards the request, and records the serving
   engine's prompt, cached-prompt, and completion counts. It never re-tokenizes.
   Completed, streamed, aborted, and failed upstream attempts all produce one raw
   event. Missing usage is an explicit `usage_found=false` zero-charge row, not a
   silently dropped event or guessed token count. A client disconnect never
   erases served work: an aborted attempt with authoritative engine usage is
   rated normally; without authoritative usage it remains a zero-charge raw row.
4. The emitter delivers through Valkey Streams, falling back to its fsync'd WAL and
   then a structured `METERING_FLOOR` log. The drainer acknowledges Valkey only after
   the Postgres transaction commits and deduplicates redelivery on the server-minted
   attempt id.
5. The rater (an aggregator since the billing unification) writes hourly
   `rated_usage` rows at the token grain: (auth_id, owner, resource, model,
   serving mode, graph, topology revision, hour), carrying org, region, base model
   and adapter, with prompt, cached and completion token sums and the attempt count.
   **phoebe prices nothing**: the manager owns every price and rates each hour as of
   that hour (billing-design.md D1). The rater fetches no prices and writes the
   money columns as NULL. It withholds, counts and pages (exit 2) on evidence that
   cannot become any record: unattributable, invalid usage, invalid serving mode,
   and an UNEXPLAINED missing-usage attempt. CONFLICTS (owner conflict, ambiguous
   org, ambiguous base model or adapter, missing org) are PARKED per identity
   (see "Parked usage" below): only the conflicted identity is held back, never the
   rest of the hour, and the run exits 4 while any parked usage is unresolved. An absent base model is NOT withheld: a
   dedicated endpoint may serve a model outside the catalog (D3), so the record is
   sent with `base_model ""` and the rater logs a warning; the manager prices
   dedicated + empty base at the plan's default rate and reports shared + empty
   base as unpriced.
6. `token-push` sends each hour to the manager's `POST /customer/metered-usage`
   with `kinds: ["token"]`: the complete set of that hour's token records, each
   keyed by a deterministic `usage_id` (sha256 of the record's grain). The manager
   deletes, by absence, records it holds for the hour that the request omits, so a
   re-sent hour replaces the previous one. An hour with no usage is sent as
   `records: []` only if this install sent that hour before (`pushed_hour` or
   `pushed_usage` has it); an empty hour it never sent is skipped, because the
   manager's records for it did not come from this database (a wiped and
   restarted one, for instance) and an empty request would delete them. A parked identity, or a rollup token-push cannot turn into a valid record,
   is left out of the request while the rest of the hour is sent; usage_ids the
   manager already holds for that identity (recorded in `pushed_usage`) are listed
   in `held`, so delete-by-absence cannot remove usage the manager accepted. The
   manager owns rating, settlement and invoice-line creation.

### Parked usage

A conflict holds back only the conflicted identity, never the whole hour. The
identity is (auth_id, owner, resource_id, model_id, serving_mode) in one hour.

| reason | meaning |
|---|---|
| `owner_conflict` | events carried both `X-Saturn-User-Id` and `X-Saturn-Group-Id` (parked under owner `''`) |
| `ambiguous_org` | the identity's events named more than one org (the payer) |
| `ambiguous_base` | more than one base model or adapter, or a value on some events and none on others |
| `missing_org` | no event of the identity named an org |

- **Recorded, never dropped.** The rater writes one `parked_usage` row per
  identity, hour and reason, with event and token counts. The row is never pruned,
  so the record of unbilled usage outlives `billing_event`'s 30-day horizon.
- **Loud.** Every rater run logs each unresolved row at ERROR (hour, identity,
  reason, token counts) and exits `4` (exit `1` and `2` outrank it), so a Job
  alert keeps firing until it is resolved.
- **Listed.** `phoebe-rater -list-parked` prints the unresolved rows and exits `4`
  when there are any, `0` when there are none. Its `RAW_EVENTS` column is
  `present`, or `raw_events: pruned — settle manually via manager credit` when the
  pruner has removed the hour's raw events (fewer `billing_event` rows remain than
  were parked); the run's ERROR line carries the same marker. The same data in SQL
  (without the raw-event check):

  ```sql
  SELECT window_start, auth_id, owner_type, owner_id, resource_id, model_id,
         serving_mode, reason, event_count, prompt_tokens, cached_tokens,
         completion_tokens, first_parked_at, last_seen_at
    FROM parked_usage WHERE resolved_at IS NULL ORDER BY window_start;
  -- video: the same from parked_video_usage, with output_seconds,
  -- output_frames, request_count and gpu_seconds
  ```
- **Resolution.** Fix the cause (header propagation, or correct the evidence in
  `billing_event`). Every rater run re-checks unresolved parked hours from the last
  ~7 days (`ParkedRescanReach`, inside the pruner's hard floor, so the raw events
  are guaranteed complete); when the conflict is gone it sets `resolved_at`, writes
  the identity's rollup, and token-push re-sends that hour on its next run however
  old it is (any hour with a row resolved after the hour's last accepted push in
  `pushed_hour`). For an older parked hour, confirm `billing_event` still holds its
  events, then run `phoebe-rater -since <hour> -until <hour+1h>`; the same
  resolution and re-send follow.
- **Pruned raw events: settle by hand (Hugo, 2026-10-27).** The `billing_event`
  horizon (30 days by default) has NO exception for parked usage. Once the pruner
  has removed a parked hour's raw events, the hour can never be re-aggregated:
  the rater never re-checks it, and refuses (exit `1`) any `--since/--until`
  window that contains it, so a partial re-run cannot wipe the hour's clean
  rollups or mark the conflict resolved without evidence. The `parked_usage` row
  keeps the hour, identity, reason and token counts. Settle it by hand: decide the
  correct payer and pricing facts from the row and your records, apply the charge
  or credit through the manager's credit path, and keep the `-list-parked` output
  as the evidence. Then mark the row settled so it stops paging:

  ```sql
  UPDATE parked_usage SET resolved_at = now()
   WHERE window_start = :hour AND auth_id = :auth_id AND resource_id = :resource_id
     AND model_id = :model_id AND serving_mode = :serving_mode AND reason = :reason
     AND resolved_at IS NULL;
  ```

  token-push then re-sends that hour from `rated_usage` (the clean identities
  only). The settled identity is no longer held, so any record the manager still
  held for it from before the conflict is deleted by absence: the manual
  settlement must therefore cover the row's FULL parked counts, which include
  those earlier events, and nothing is charged twice. The manager accepts a change
  to an hour whose billing period has closed like any other (settlement decides
  what it bills), so the re-send needs no further action.
- **Held ids.** While an identity is parked, every push of its hour lists the
  usage_ids the manager accepted for it earlier in `held`.

### Parked video usage

Video generation jobs (kind `video`, migration 0017) are parked the same way, in
`parked_video_usage`, with video quantities (`output_seconds`, `output_frames`,
`request_count`, `gpu_seconds`) instead of token counts. The identity is the same
(auth_id, owner, resource_id, model_id, serving_mode) in one hour, and the
reasons are `owner_conflict`, `ambiguous_org` and `missing_org` (org_id `''`).
There is no `ambiguous_base`: base model, task, resolution and region are part
of the video grain, so two values are two records, not a conflict.

- `phoebe-rater -list-parked` lists both kinds; its `KIND` column says which, and
  its `USAGE` column carries the counts in the kind's own units. The raw-event
  check counts `video_billing_event` rows for a video row.
- Resolution, the pruned-raw-events rule and holding are the same as above, with
  `video_billing_event` and `parked_video_usage` in place of `billing_event` and
  `parked_usage`. Holding is per kind: a parked video identity holds only its
  video usage_ids (`pushed_usage.kind`), never a token record.
- Video jobs that cannot become any record (an empty auth, resource or model id,
  a negative or NaN quantity, or a completed job whose `request_count` is not 1)
  are withheld per job, logged at ERROR, and make the rater exit `2`, like
  invalid token events.
- phoebe-videos deletes `video_billing_event` rows after 30 days, always. So the
  rater never re-rates video in an hour older than 29 days, even in an explicit
  `--since/--until` run. That hour's `rated_video_usage` rows are the record and
  are kept as they are, and the run logs a WARN naming how many hours it left
  alone. Tokens in such a window are re-rated as usual. If such a window also
  finds completed, rateable video jobs of an identity with no video rollup and no
  unresolved park, it logs them at ERROR and exits `2`. Those jobs are never
  billed automatically: export them before the 30-day prune, and settle them by
  hand through the manager's credit path.
- Every routine run, once the one-time catch-up is done, also checks the hours
  between 29 days ago and its own trailing window. If an identity has completed,
  rateable video jobs there with no video rollup and no unresolved park (the
  rater was down longer than its trailing window), the run logs them at ERROR
  and exits `2`. It keeps doing so on each run until they are re-rated with
  `--since/--until`, which must happen before they pass 29 days.
- **Tombstones and vanished video.** The rater stores each video rollup's wire
  `usage_id` (`rated_video_usage.usage_id`). Whenever it removes one, it writes a
  tombstone to `video_reconcile_deletion` in the same transaction. That happens
  for a reconcile deletion (`reconcile`), a park (`parked`), or a new `org_id`
  for the same grain (`regrained`). token-push lets the manager delete a sent
  video record by absence only when its id has a tombstone. A parked identity's
  ids stay held while the park is unresolved.
  - A restore of `rated_video_usage` rows must also delete their ids from
    `video_reconcile_deletion` in the same transaction. Otherwise a stale
    tombstone would release the record if it were lost again. token-push
    reports a live rollup that still has a tombstone at ERROR and exits `1`.
    Inside the 29-day reach, the rater's next run of the hour also clears it.
  - Any other sent video id with no rollup was lost. token-push holds it, so the
    manager keeps the record, logs `video record(s) this install sent have no
    rollup and no tombstone` at ERROR, and exits `1` each time the hour is sent.
    - To keep billing it, restore the rollup in `rated_video_usage` with its
      `usage_id`.
    - To let it go, write the tombstone by hand, then re-send the hour with
      `-since/-until`. Re-rating cannot do this: the rater tombstones only rows
      it deletes, and a lost row is not there to delete. Do not delete the id
      from `pushed_usage` instead: if it was the hour's only video record, the
      hour stops listing video and the manager keeps the record silently.

      ```sql
      -- <reason> is 'reconcile' here, 'regrained' for an id replaced by a new one.
      INSERT INTO video_reconcile_deletion (window_start, usage_id, auth_id, owner_type,
          owner_id, resource_id, model_id, serving_mode, reason)
      SELECT window_start, usage_id, auth_id, owner_type, owner_id, resource_id,
             model_id, serving_mode, '<reason>'
        FROM pushed_usage WHERE window_start = '<hour>' AND usage_id = '<id>';
      ```
  - An hour the rater tombstones after its last accepted push (an explicit
    re-rate of an older hour) is re-sent automatically, however old, like an
    hour with a resolved park.
  - If no video usage is recorded at all although video was sent before, the
    video tables look wiped. Every run then logs it at ERROR and exits `1`.
  - A rollup whose stored `usage_id` is not the id of its own fields was edited
    outside the rater, or rated under another `usage_id` rule. It is held back,
    not sent under a new id (exit `2`). Inside the 29-day reach, re-rating the
    hour re-stamps it and tombstones the old id. Older hours are never re-rated:
    fix the row by hand. Set `usage_id` to the id of its fields (the
    `HOLDING BACK` log line names it). If the old stored id is in
    `pushed_usage` (the manager holds it), write a `regrained` tombstone for it
    as above.

## Routine checks and alert conditions

Run these against Phoebe Postgres for every closed hour before treating an invoice
as settled:

```sql
SELECT *
FROM billing_reconciliation_hourly
WHERE window_start >= :start AND window_start < :end
  AND (
      missing_usage_attempts <> 0
      OR invalid_usage_attempts <> 0
      OR invalid_serving_mode_attempts <> 0
      OR missing_org_attempts <> 0
      OR distinct_org_ids > 1
      OR attempt_delta <> missing_usage_attempts
      OR prompt_token_delta <> 0
      OR fresh_input_token_delta <> 0
      OR cached_token_delta <> 0
      OR completion_token_delta <> 0
  )
ORDER BY window_start, org_id, resource_id, model_id;
```

`fresh_input_tokens` is the checked `prompt_tokens - cached_tokens` subset; all
three billable classes remain independently reconcilable. `missing_usage_attempts`
is not automatically revenue loss: it means the engine did
not provide authoritative counts, so Phoebe charged zero and requires engine-log
review.

### A manager outage delays sending; it does not lose it

The rater never calls the manager, so a manager outage cannot stop aggregation.
`token-push` retries 5xx, 429 and network errors with capped exponential backoff
(5 attempts; a 429's `Retry-After` is honoured up to the 30 s cap); if they persist it exits `1` and the hour is re-sent by the next run,
because each run re-sends the trailing `pushTrailingHours` hours (default 24) and a
re-send is idempotent. An outage shorter than that window heals with no operator
action. A longer one is healed too: each run also sends, after the window, up to
168 hours that have usage and were never accepted, oldest first, starting after
the first hour a scheduled run had accepted (`token_push_state.auto_floor`). The
same per-run budget then back-fills the usage recorded before that first
accepted push (an install that ran before it was connected to a manager):
`rated_usage` is never pruned, so all of it is sent, over as many runs as it
takes, and each run logs `back-filling usage from before this install's first
accepted push: N hour(s) not sent yet (oldest ...); this run sends M`. Hours
already accepted (by an earlier run or by an operator) are not sent again.
`token-push -since ... -until ...` still re-sends any range on demand (90 days
per run at most); it never moves `auto_floor`.

**Refused hours are parked.** A `400` whose body is the manager's validation
answer (`{"error", "path"}`) means that hour's payload will never be accepted.
token-push records the hour in `refused_hour` (status, response body, SHA-256 of
the refused request, number of refusals, first and last refusal), logs `PARKED
<hour>` at ERROR, and exits `3` in that run. A parked hour is not re-sent while
its usage is unchanged and does not use up the 168-hour budget, so it cannot hold
back newer hours. Each run rebuilds up to 168 parked hours (least recently
checked first) and sends any whose payload changed (a re-rate, a resolved
conflict); an accepted push removes the row. While any hour is parked, every run
logs at WARN `N hour(s) are parked (K newly this run), oldest ...` and, if
nothing else is wrong, exits `0`. To retry a parked hour after fixing its cause
without changing its usage, send it with `-since/-until`, which sends parked
hours too. The same applies to an hour whose request breaks a request-level wire
rule token-push checks itself (for instance a duplicate usage_id; status `0` in
`refused_hour`, exit `2` in the run that parks it).

Every other 4xx is never retried and exits `3`, and nothing is parked: `401`
means the customer token, `403` an inactive or uninstalled install, and `409`
a manager older than the 2026-11-17 rule that refuses token usage from an
install on the legacy billing path. These three are about the install, not the
hour, so the run stops at the first one and the next run sends again. A `400`
without the manager's validation body (a proxy in front of it, for instance) and
any other 4xx exit `3` every run until fixed.

### Sending is not optional

Token Factory usage is always recorded and always sent (Hugo, 2026-11-17).
There is no opt-out. Whether usage is charged is the manager's decision, by plan
or flag, never the install's. Once an install connects to a manager, all of its
retained usage is sent: the trailing window, then missed hours, then the
back-fill of everything recorded before the first accepted push. Two settings
can delay sending, and neither stops it:

- **A suspended rater or token-push CronJob only delays sending.** While the
  rater is suspended the drainer keeps writing `billing_event`. When the rater
  runs again, its first routine runs rate every `billing_event` hour that was
  never rated (hours with no `rated_usage` and no `parked_usage` row), oldest
  first, up to 168 hours per run after the trailing window, recording progress
  in `rater_state.catchup_cursor`. When nothing is left the rater sets
  `rater_state.catchup_completed_at`, and later runs rate only the trailing
  window. Each run logs `catch-up rated N never-rated hour(s), <first> to
  <last>` and whether more remain. An hour that already has rollups or parked
  rows is never re-aggregated by the catch-up, so a partially pruned hour keeps
  its rollup. The catch-up reaches only hours still in `billing_event`: with the
  pruner enabled, hours older than its horizon are gone and cannot be rated. A
  suspended token-push only delays sending too: `rated_usage` is never pruned,
  and the next runs send every hour never accepted.
- **An empty manager URL is only for an install that has never connected** (an
  air-gapped install, or one not connected yet). Its usage stays in
  `rated_usage` and is all sent once a URL is configured.

**A lost manager URL fails.** An install that has pushed and then loses its URL
is misconfigured, and token-push fails loudly. With no manager URL, token-push
reads `pushed_hour`: an install that has pushed before has lost its URL and the
run exits `1`; one that never pushed is not connected yet and exits `0`. With
no `DATABASE_URL` at all it exits `0`; with one it cannot read, `1`.

**The manager URL must be https.** token-push sends the install's token and its
usage, so `managerURL` (and `-manager-url`) is refused, with exit `1`, unless
its scheme is `https`: `http`, any other scheme, or a URL without one fails at
startup with a message that says https is required. The settings key
`allowInsecureLocalhostManagerURL: true` exists for tests with a stub manager.
It allows `http` to `localhost`, `127.0.0.1` and `::1` only, never to any other
host, and the chart never sets it.

**Blocked egress fails fast.** Connecting to the manager is bounded by 10 s dial
and 10 s TLS-handshake timeouts (`requestTimeout`, default 60 s, still bounds a
slow answer). When 3 hours in a row fail because the manager cannot be reached
at all (DNS failure, connection refused or timed out, TLS handshake failure, or a
certificate that does not verify, as from an intercepting proxy), token-push
logs `cannot reach manager at <url>: <cause>` at ERROR and stops the run with
exit `1`, within minutes instead of at the job deadline. Fix the install's
egress to the manager (firewall, proxy, DNS); nothing is lost meanwhile. A 5xx or
429 is an answer from the manager: it is retried as above and does not count
toward the 3.

Exit `2` means token-push held back a rollup it could
not turn into a valid record (a wire rule, or a row without an org written before
the rater parked missing orgs); the rest of that hour was sent and the rollup's
earlier usage_ids were held. Parked usage is reported by the rater (exit `4`), not
by token-push.

"Failed" means `status_code >= 400` everywhere — the reconciliation view's
`failed_attempts` and the rater's routine-vs-alarming split use the SAME
threshold, so the view you audit and the decision to page can never disagree
about the same rows.

**Missing usage is paged by CAUSE, not by count.** A client disconnect or an
upstream failure legitimately produces a zero-usage attempt, so those are routine
on any install with traffic: they are reported (INFO, and counted in
`missing_usage_attempts`) and reviewed here, never paged. What pages is an attempt
that was NOT aborted and did NOT fail — a response the engine reported as
SUCCESSFUL while supplying no usage block, meaning work may have been served that
cannot be billed. The rater exits non-zero only on that unexplained subset, so
exit 2 stays reserved for rare, wrong conditions (unattributable, invalid usage,
ambiguous base/org, invalid serving mode, owner conflict) rather than firing
every hour.

Page on parked usage (rater exit 4), an unexplained missing-usage attempt, rater anomaly/non-zero exit, reconcile
deletion during a routine run, drainer poison row (an increase in
`phoebe_metering_drainer_poison_dropped_total`, or the log line `drop poison row`), a
drainer pause that lasts (see "Drainer paused" below), the drainer log line
`drop malformed entry` (an entry that could not be decoded, whose logged values are
the only copy left once the trim passes it, so it needs repair by hand), the drainer
log line `valkey is full and consumer group "<group>" does not exist; stored and trimmed`
(see "A full Valkey" below),
`METERING_FLOOR`, WAL corruption,
token-push withheld hour (exit 2), manager rejection or a newly parked hour
(exit 3), push failure, an unreachable manager, or a lost manager URL (exit
1), and on the WARN line
`hour(s) are parked` while it persists. Alert separately when the oldest Valkey
pending entry, oldest WAL entry, or oldest unpushed rated hour exceeds two job
periods. The deployment/chart owns those queue-age metrics.

The view groups raw evidence at the exact rated natural key and exposes
`missing_org_attempts` plus `distinct_org_ids` separately. A rollout-era mix of one
NULL and one real org therefore reconciles to one rated row without false token
deltas, while conflicting non-NULL orgs remain explicit.

**`rated_attempts = 0` with a non-zero `raw_attempts` is NOT by itself lost
rating.** It has three very different causes and the view alone cannot tell them
apart, so never treat the row as a drainer/rating incident before ruling out the
first two: (a) the rater deliberately WITHHELD the rollup at an ambiguity gate
— check `distinct_org_ids > 1` on the row for org-ambiguity, and the same run's
`ambiguous_base_events` count for base-ambiguity (two base models or adapters, or
adapter on some events and not others, for one identity in the hour), and its
which the view does not surface; (b) the rater deliberately
WITHHELD the events because their serving mode is invalid — the run's
`invalid_serving_mode_events` count. These are events whose
`billing_event.serving_mode` is NULL, `''` or anything other than `'shared'` or
`'dedicated'`. Dedicated events stored before migration 0007 look exactly like
this, so inside a re-rate window that covers pre-cutover hours they are withheld
on purpose (see "Serving-mode cutover (migration 0007)" below). The view's
`invalid_serving_mode_attempts` column (added by migration 0007) counts these
attempts for the hour and key, with the same predicate the rater uses; or (c) the
rater has not yet run for that hour. Check the rater's run report for the window
before escalating.

For the invoice boundary, compare the records token-push sent for the interval
(re-derive them with token-push's `-since/-until` against a stub, or from
`rated_usage`) with the manager's received token records: require set equality on
`usage_id` and exact equality of the four quantities per record before comparing
any total. A total can accidentally balance while two customers are mis-attributed.
Prices and money are the manager's to reconcile.

### A full Valkey

Past `maxmemory` Valkey refuses `XADD` and `XGROUP CREATE`, and emitters fall back
to their WALs. Reads, `XACK` and `XTRIM` still work, so the drainer recovers by
itself in the states below. Trimming removes only delivered and acknowledged
entries, and it runs at drainer startup before the Postgres connection and the
schema gate, and every `trimInterval` after that.

- **No consumer group.** Valkey restarted empty while the drainer was down, the
  emitters filled a new stream, and `XGROUP CREATE` is refused. After the schema gate
  the drainer reads the oldest batch with `XRANGE`, stores it, trims it, and tries
  again until the group can be created. The log line is
  `valkey is full and consumer group "<group>" does not exist; stored and trimmed`,
  naming the drainer's own group (`group`, `phoebe-drainer` by default). It should
  stop once that group exists. Other groups, such as a debug consumer, may still be
  listed by `XINFO GROUPS` while it is logged; its `other groups=` count says how many.
  If Postgres is failing, or the gate has not opened, nothing is trimmed, because an
  entry is removed only once it is stored. If another consumer group is on the stream,
  the trim stops at what that group has read and acknowledged, and only entries below
  that point are stored. When that frees nothing,
  the drainer logs `holds the stream` with the group's name: let that group read
  further, or delete it with `XGROUP DESTROY` if nothing uses it.
- **After a Postgres outage.** The drainer stops reading new entries after a failed
  store write, so at most one batch per replica is left pending. It retries by writing
  the entries that replica holds, waiting 500ms after the first failure and twice as
  long after each later one, up to 30s or half of `claimMinIdle`, whichever is shorter
  (15s with the default 30s `claimMinIdle`), so another replica does not reclaim a
  batch that is still being retried. A store that answers `Ping` but rejects every
  write also stays in this state when the error is one of these: a schema gap
  (SQLSTATE `42P01`, `42703` or `42P10`), a read-only replica (`25006`), missing
  privileges (`42501`), a full disk or other insufficient resources (class `53`), an
  admin shutdown or a server that cannot accept connections yet (class `57P`), a
  connection exception (class `08`), or a serialization failure or deadlock (`40001`,
  `40P01`). Any other error on a single row from a store that answers `Ping` is
  treated as a bad row when the store shows it accepts writes: another row of the
  same batch was stored, or, when no row of the batch was, a probe write succeeded
  (see "Drainer paused" below). The drainer then logs `drop poison row` with the
  full event and a `total poisoned=` count, and acknowledges and drops the row.
  `phoebe_metering_drainer_poison_dropped_total` counts these drops across all
  drainers; recover any such event from the logged JSON. If the probe write fails
  too, the batch is a store failure and the drainer pauses with the rows kept.
  Once a retry writes successfully, the
  replica writes the rest of what it holds before it reads new entries. If its entries
  were reclaimed by another replica, it reads one new batch to test the store, and a
  failure there keeps the wait growing rather than starting again at 500ms. Reclaim
  then works through any other pending entries without waiting for `claimInterval`
  while each batch comes back full.

**Manual relief.** If Postgres or the schema stays broken, the drainer cannot free
undelivered entries. Raise `maxmemory` on Valkey (`CONFIG SET maxmemory <bytes>`) so
that emitters can write and the group can be created, and then fix the store. Do not
restart Valkey to clear it: it keeps nothing on disk, so a restart loses every entry
that is not yet stored.

### Drainer paused

The drainer pauses when it cannot write to Postgres. While paused it reads no new
entries from Valkey, and it retries the entries it already holds with backoff
(500ms, doubling up to 30s or half of `claimMinIdle`). A pause starts on:

- a store outage (Postgres does not answer `Ping`);
- a store that answers `Ping` but refuses writes with one of the SQLSTATEs listed
  under "After a Postgres outage" above (a schema gap, a read-only replica, missing
  privileges, a full disk, and so on);
- a batch in which every row failed, for another reason, and the probe write failed
  too.

**The probe write.** When no row of a batch can be stored, the drainer cannot tell
from the row errors alone whether the rows are bad or the store is. So it makes one
probe write: the same `INSERT` real rows use, for one synthetic but valid event
whose `request_id` starts with `phoebe-probe-`, inside a transaction that is always
rolled back. The probe leaves no row behind, and no other session can see it. If the
probe fails, the store is the problem and the drainer pauses with the rows kept. If
it succeeds, the store accepts writes, so the rows themselves are bad and are
dropped as poison (`drop poison row`, with the full event). A batch in which at
least one row was stored makes no probe: its failed rows are dropped as poison. The
drainer logs the probe's outcome at WARN, as `probe write failed too, so this is a
store failure` or `probe write succeeded ... dropping the rows as poison`.

One case still drops rows that a person might want kept: a constraint or trigger
that rejects real rows but accepts the probe's synthetic row (for example a new
CHECK on a column the probe leaves NULL). The probe succeeds, so those rows are
dropped as poison. Each one is in the drainer log in full, with its stream id and
`request_id`, and `phoebe_metering_drainer_poison_dropped_total` rises, so it can
be repaired by hand as described below.

**Nothing is dropped during a pause.** The rows the drainer was writing stay in its
pending list, and new entries stay unread in the stream. The trim removes only
entries that every consumer group has delivered and acknowledged, so it never
removes a pending or unread entry, however long the pause lasts.

**A long pause fills Valkey.** The stream keeps growing while the interceptors keep
serving. Once Valkey reaches `maxmemory` it refuses `XADD`, and each interceptor
writes its events to its WAL on its own PVC instead
(`phoebe_metering_wal_pending_events` rises). If a WAL cannot be written (its disk
is full or it is broken), events go to the `METERING_FLOOR` log line, which is then
the only copy. Do not restart Valkey during a pause: it keeps nothing on disk, so a
restart loses every entry that is not yet in Postgres. See "Manual relief" above
for raising `maxmemory`.

**What shows a pause.**

- The drainer logs one ERROR line when a pause starts:
  `drainer: paused: store write failed: <error>; billing events stay in Valkey and
  none are dropped; retrying with backoff (see docs/billing-reconciliation.md,
  "Drainer paused")`. While it lasts, each failed retry logs `drainer: drain pass:
  ...` at ERROR, and `store still failing` (at most every 30s) while Postgres does
  not answer `Ping`. When it ends it logs `resuming after a pause of <duration>` at
  INFO.
- The drainer publishes its state to the Valkey hash `<stream>:drainer`
  (`phoebe:metering:drainer` by default), and every interceptor exports it from its
  metrics port, with no labels:
  - `phoebe_metering_drainer_paused`: 1 when a drainer that reported within the
    last 120s is paused, 0 when none of them is. Absent when no drainer reported
    within 120s or the hash could not be read.
  - `phoebe_metering_drainer_paused_seconds`: how long the longest current pause
    has lasted (0 when none). Absent under the same conditions.
  - `phoebe_metering_drainer_last_seen_age_seconds`: seconds since a drainer last
    reported. A running drainer reports at least every 15s.
  - `phoebe_metering_drainer_poison_dropped_total` and
    `phoebe_metering_drainer_undecodable_dropped_total`: rows dropped as poison and
    entries dropped as undecodable, across all drainers.

  Every interceptor replica reports the same values, because they read the same
  hash; aggregate with `max`, not `sum`. Once Valkey is full the drainer's own
  reports are refused too, so after 120s the paused series disappears and
  `phoebe_metering_drainer_last_seen_age_seconds` keeps growing. An alert should
  therefore fire on any of: `max(phoebe_metering_drainer_paused) == 1` for some
  minutes, `min(phoebe_metering_drainer_last_seen_age_seconds) > 120`, or the
  paused series being absent while `phoebe_metering_valkey_up` is 1. The drop
  counters are not lost while reports are refused: each drainer keeps the counts it
  has not sent and adds them once a report succeeds. The counters start from 0 when
  the hash is created, so read them with `increase()`.

**Clearing a row that pauses the drainer.** If the pause does not end and the
ERROR lines name the same row each time (`all <n> row(s) failed (first row id=<id>
request_id=<request_id>: <error>) and the probe write failed`), fix the cause in
Postgres first if you can: the drainer resumes by itself once a write succeeds.
If the row must be set aside so the rest of billing can move, do it without
losing it:

1. Find the entry. The ERROR line gives its stream id. The pending list shows
   every entry the drainer holds:

   ```console
   valkey-cli XPENDING phoebe:metering phoebe-drainer - + 20
   ```

2. Save its JSON before you touch it, somewhere outside Valkey:

   ```console
   valkey-cli --raw XRANGE phoebe:metering <id> <id>
   ```

   The value after the `event` field is the event JSON. Save it as one line in a
   file, for example `held-<request_id>.jsonl`, and keep the file with the incident.
3. Acknowledge only that entry, so the drainer stops retrying it. Once acknowledged
   the trim may remove it, so step 2 must be done first:

   ```console
   valkey-cli XACK phoebe:metering phoebe-drainer <id>
   ```

   The drainer retries what it still holds. Once nothing of its own is pending, it
   reads a new batch to test the store. If the next ERROR line names another row,
   repeat these steps for it.
4. Re-insert the row once the cause is fixed. Use `phoebe-recover` with the saved
   file, which validates the event and checks it again on apply (see "Repair and
   replay" step 2):

   ```console
   /app/phoebe-recover -input held-<request_id>.jsonl
   /app/phoebe-recover -input held-<request_id>.jsonl \
     -valkey-addr valkey:6379 -stream phoebe:metering \
     -apply -expected-count 1 -expected-digest <digest from the dry run>
   ```

   The drainer deduplicates on `request_id`, so a repeated replay is harmless. If
   `phoebe-recover` refuses the event, its message names the value the database
   cannot store: correct that value in the saved file (never the `request_id`),
   note the change in the incident, and run the dry run again. If the event is for
   an hour older than the routine rater window, follow "A metering outage longer
   than a day" for that hour. A row already dropped as poison can be re-inserted
   the same way: the `event=` part of its `drop poison row` line is the JSON to save.

### A metering outage longer than a day

The routine rater and token-push runs cover only the trailing 24 hours. Events that
reach Postgres later than that, for example after a long time in a WAL or a full
Valkey, are in `billing_event`, but no routine run aggregates them or sends them. If
this step is missed, the manager never receives those hours and the customer is
undercharged.

1. Wait until the backlog has cleared. Do not run the later steps until all of these
   hold on every replica, and `phoebe_metering_stream_undelivered_entries` is near 0:

   - `phoebe_metering_wal_healthy` is 1 and `phoebe_metering_wal_poisoned` is 0. A
     replica with `phoebe_metering_wal_healthy` at 0 may have unshipped events on disk.
   - `phoebe_metering_wal_pending_events` is 0. A missing series is not zero. It means
     the count is unknown, so check the two metrics above before treating the backlog
     as cleared. The number of replicas reporting this series must also equal the
     number of `phoebe-interceptor` replicas running.
   - `phoebe_metering_wal_quarantined_dirs` is 0. The pending count leaves out these
     `<dir>.corrupt.<ts>` directories, and their events are not billed. Recover them
     manually or accept them as lost before running the rater and token-push. This
     metric counts the directories on disk, so it stays above 0 until they are moved
     away from the WAL path. A missing series means the replica could not read the
     directory, so the count is unknown and must not be treated as 0.

   This query returns a sample only when the conditions above hold across the replicas
   that Prometheus scrapes. An empty result means the backlog has not cleared. The
   query cannot see a replica that is down or not scraped, so compare its replica count
   by hand as well.

   ```promql
   min(phoebe_metering_wal_healthy) == 1
     and count(phoebe_metering_wal_pending_events) == count(phoebe_metering_wal_healthy)
     and sum(phoebe_metering_wal_pending_events) == 0
     and sum(phoebe_metering_wal_poisoned) == 0
     and count(phoebe_metering_wal_quarantined_dirs) == count(phoebe_metering_wal_healthy)
     and sum(phoebe_metering_wal_quarantined_dirs) == 0
   ```

   Also read `phoebe_metering_wal_skipped_entries_total`. It counts records lost from
   the WAL, so the rater and token-push cannot bill them. An increase during the outage
   is a known under-billing to record, and it does not block the later steps. Each
   skipped record was logged at ERROR with its raw bytes, as `undecodable wal entry
   index=<n>`, so it can be repaired by hand from the logs.
2. Find the true start of the outage. Do not use the time someone noticed it: check
   the earliest late event, the `event_ts` of rows inserted long after their
   timestamp. On GB300 in November 2026 events began diverting about 26 hours before
   the drainer's last successful read.
3. Round that start down to the hour, then run these in this order:

   ```
   phoebe-rater -since <hour> -until <start of the current hour>
   phoebe-token-push -since <hour> -until <start of the current hour>
   ```

4. Check the exit codes. token-push exit `3` is explained under "A manager outage
   delays sending": a `404` means the manager is too old for `/customer/metered-usage`,
   so deploy it and run step 3's token-push again. A `400` parks that hour (see
   "Refused hours are parked"). Then compare the hours with the reconciliation view.

An hour already pushed is replaced by its new snapshot, so re-running is safe. A window
that contains a parked hour with pruned raw events makes the rater exit `1`; see
"Pruned raw events" above.

## Repair and replay

1. Quiesce the affected invoice window in the central manager. Do not delete local
   raw events by hand or edit rated money manually. Note raw events are deleted
   AUTOMATICALLY once older than the retention horizon by `cmd/prune` (ruled
   policy: 30 days on `created_at`, hard floor 7; `rated_usage` is never pruned
   and the money rollup lives upstream in the manager) — so if the window you
   need to repair has aged past the horizon, the raw evidence is gone by design
   and the repair path is the manager's rollup plus any retained WAL evidence,
   not the billing_event table.
2. Copy the recovery artifact away from any live writer, then validate it without
   making changes.

   **Recover every interceptor ordinal, not just one.** Each StatefulSet ordinal
   owns its OWN retained WAL PVC (the chart defaults to two interceptor
   replicas), and an ordinal's WAL holds only the attempts that ordinal served.
   Recovering `phoebe-interceptor-0` and stopping leaves every attempt buffered
   on `phoebe-interceptor-1` unbilled, with nothing in the reconciliation view
   to indicate that a whole replica's evidence was skipped. Enumerate the claims
   (they are named `phoebe-wal-phoebe-interceptor-<ordinal>`:
   `kubectl -n saturn get pvc | grep '^phoebe-wal-'`) and run the dry-run/apply
   pair below once per ordinal, reviewing each digest separately. Claims are
   retained, so an ordinal scaled away still has evidence to recover.

   `phoebe-recover` accepts a tidwall WAL directory, legacy/imported
   JSONL, or logs containing `METERING_FLOOR` records. It opens a temporary copy of
   WAL directories so the forensic source is not mutated:

   ```console
   /app/phoebe-recover -input /evidence/phoebe-metering-wal
   ```

   Review the reported record count, duplicate count, and `event_set_sha256`.
   That digest covers the COMPLETE validated event set — every field of every
   event, not just the request ids — so it changes if any token count, org
   attribution, or id differs from what you reviewed.

   Replay only that validated set into the configured Valkey stream by repeating
   BOTH the reported unique count and the reported digest verbatim:

   ```console
   /app/phoebe-recover -input /evidence/phoebe-metering-wal \
     -valkey-addr valkey:6379 -stream phoebe:metering \
     -apply -expected-count 42 \
     -expected-digest 9f2c1d...a71b
   ```

   Both guards are required for `-apply` and are checked before any Valkey
   connection is opened, so the artifact you reviewed is provably the artifact
   that gets replayed: a different set with the same count, or the same ids with
   altered payloads, is refused without writing anything. Do not copy a digest
   from an older run — re-read it from the dry-run you are actually approving.

   The command preserves every original `request_id`, refuses conflicting
   duplicates, schema-poisoning values, and status codes outside the database's
   `100..599` range, and is dry-run-only without both apply guards. A retry
   after a partial write is safe because the drainer deduplicates on
   `request_id`. Never replay a live WAL directory while the interceptor is
   appending or auto-draining it.
3. Run the drainer until the consumer group has no pending/lagging entries. Confirm
   the recovered attempt ids exist once in `billing_event`.
4. Restore attribution headers if they were the problem. There are no prices to
   restore: the manager prices each hour as of that hour.
5. Run the rater explicitly for the complete affected half-open window. Investigate
   every unattributable, ambiguous, missing-usage, or
   reconcile-deletion signal before proceeding.
6. Re-run the hourly reconciliation view. Then replay `token-push` for every affected
   hour, including empty hours within a non-empty authoritative run.
7. Compare the manager's received token records by `usage_id` and quantities, then
   release the invoice window.

Endpoint deletion does not remove `billing_event` or `rated_usage`; attribution and
the pricing facts (base model, adapter, serving mode) are captured before teardown. Never reconstruct historical ownership by
joining the current endpoint table.

Migration 0005 is supported as a verified-empty clean cutover with no legacy
database rows, buffered events, or mixed-version drainers; see
`migrations/README.md`. Its defensive development-database backfill sets
`usage_found=true` for legacy rows except aborted rows whose three counts are
zero, but it is not a rolling-upgrade compatibility guarantee.
The raw ledger deliberately accepts invalid engine counts so evidence is never
discarded merely because it cannot become money. The rater excludes those rows
and reports `invalid_usage_attempts`; repair or explicitly quarantine them before
settling the invoice window.

### Serving-mode cutover (migration 0007)

Migration 0007 makes `'shared'` and `'dedicated'` the only legal serving modes.
Before it, dedicated traffic was stored with a NULL (or `''`) serving mode. All of
that traffic was dedicated (Hugo, 2026-09-30: "assume it's all dedicated (which is
true)"), so 0007:

- rewrites `billing_event.serving_mode` NULL and `''` to `'dedicated'` (a one-time,
  ratified exception to "raw evidence is never edited");
- renames the matching `rated_usage` rows from `''` to `'dedicated'` and recomputes
  their ids with the rater's formula.

After the deploy, the trailing-window rater re-rates the pre-cutover hours and
reproduces exactly those rows: same serving mode, same ids, no reconcile deletions
and no invalid-serving-mode anomaly. token-push sends the new ids once, and
saturn-aws-manager replaces the old `''` records for those windows by absence.

**Re-rate the hours where 0007 deleted a `''` twin.** Atlas #6709 is deployed
before this phoebe release, and from then on it stamps `X-Saturn-Serving-Mode:
dedicated` while the old phoebe is still running. The old rater stores the
pre-stamp (NULL) events of an hour as `''` and the stamped events as
`'dedicated'`, so those hours hold two rollups for one key. 0007 cannot rename
the `''` row onto its twin without breaking the unique key, so it deletes it and
prints a NOTICE with the count, cost, event count and earliest `window_start` of
the deleted rows. The surviving `'dedicated'` row covers only part of its hour. A
`''` row deleted by 0007 is rebuilt only by a re-rate of its hour, because its
events are still in `billing_event` (now backfilled to `'dedicated'`). The
routine rater re-rates only its trailing window (24h by default). If the hour the
Atlas #6709 rollout started, or the earliest `window_start` in the twin NOTICE,
is older than that window, run:

```
rater --since <that hour> --until <start of the current hour>
token-push --since <that hour> --until <start of the current hour>
```

The push makes saturn-aws-manager replace the partial `'dedicated'` rows for
those windows. Skipping this leaves those hours under-counted in `rated_usage`
and pushed under-billed.

**One follow-up step after the rollout.** The migration Job runs before the new
proxy pods replace the old ones. An old pod still serving during the rollout
meters dedicated traffic with no `serving_mode` key in the event JSON. The new
drainer stores an event whose `serving_mode` key is ABSENT as `'dedicated'` (all
pre-cutover traffic was dedicated), and this includes events replayed later from
an on-disk spool or the drain queue: the new spool reader applies the same rule
before it re-sends an event. Only an absent key gets this default. A new pod
always writes the key, so an explicit `"serving_mode":""` or `null` is a producer
bug; the drainer stores it as `''` and the rater withholds it as
`invalid_serving_mode_events` instead of billing it as dedicated. An old drainer still running during the rollout stores it as NULL, which the new
rater withholds as `invalid_serving_mode_events`. Once every interceptor and
drainer pod runs the new image, and no old drainer pod is left draining the
queue, run this statement against phoebe's database. It is idempotent, but it is
NOT the same statement as 0007's one-time backfill, because it leaves `''` rows
alone:

```sql
UPDATE billing_event SET serving_mode = 'dedicated'
WHERE serving_mode IS NULL;
```

Only an old drainer writes NULL during the rollout. The new drainer writes `''`
only for an explicit `"serving_mode":""` from a post-cutover producer, which is a
bug; those rows must stay withheld and be investigated, not backfilled. To keep
the statement to the rollout, you can also bound it by `created_at` (when the
drainer wrote the row) to the rollout window.

The next routine rater run (or an explicit `rater --since <deploy hour> --until
<now>`) then rates those events. If the rater paged with `invalid_serving_mode_events`
for hours inside the rollout, this step is the fix.

Until this backfill runs, an hour that contains NULL events from an old drainer
is rated and pushed WITHOUT those events. The rater withholds the NULL events one
by one rather than withholding the whole rollup, and token-push sends the
smaller rollup. So for that hour saturn-aws-manager briefly holds a dedicated
amount that is too low. This is expected; the backfill and the re-rate correct
it.

Run the backfill within the rater's trailing window (24h by default) of the
deploy. Then the routine rater and token-push runs fix the amounts with no manual
step. If the deploy hour is older than token-push's `pushTrailingHours` (24h by
default) by the time the backfill and re-rate are done, also run:

```
token-push --since <deploy hour> --until <now>
```

The routine token-push only re-sends its trailing window, so without this run
the corrected hours older than that window never reach saturn-aws-manager, and
they stay under-billed.

Outside the cutover, a nonzero
`invalid_serving_mode_events` means the proxy's serving-mode gate or the
served-model registry is broken, and the statement above must NOT be used to hide
it.

### Upgrading an install that already carries billing traffic

The interceptor moved from a Deployment with an `emptyDir` WAL to a StatefulSet
with one retained PVC per ordinal. Helm performs that replacement by DELETING
the old Deployment and its pod, which destroys that pod's `emptyDir` — so any
metering event still buffered in the old WAL is lost, not migrated. Rolling back
to the Deployment has the mirror-image problem: the restored pod does not mount
the retained StatefulSet PVCs, so evidence written after the upgrade is stranded
on those claims (retained, but invisible until an operator mounts them
deliberately).

Neither direction is a problem on an install with no billing traffic to lose,
which is the supported cutover today. Before performing this upgrade on an
install that IS carrying billing traffic, quiesce inference to the interceptor
first, then confirm the old WAL is empty and Valkey has no pending or lagging
entries, and only then replace the workload. Treat a non-empty WAL at that point
as evidence to recover (see Repair and replay) rather than something the upgrade
will carry across for you.

## Failure-injection checklist

The Make suite covers client-id replay, streaming/non-streaming usage, pre/post-header
abort, upstream failure, duplicate delivery, Valkey outage and recovery, WAL restart
and corruption, drainer transaction failure/poison isolation and the probe write that
decides between poison and a pause, re-aggregation, org
and base-model ambiguity, and push retry/no-retry and delete-by-absence behavior. Before a release,
also run the live-Postgres lane and perform chart-level pod-kill tests while Valkey is
unavailable.

## Guarantees outside Phoebe

- Header authenticity depends on auth-server and the saturn-k8s ForwardAuth
  allowlist being deployed before this release.
- WAL survival across pod or node loss depends on deployment storage, not Phoebe
  code alone. The official chart gives every interceptor StatefulSet ordinal a
  retained `ReadWriteOnce` PVC, defaulting to the same `saturn-default-storage`
  class as Atlas Postgres. Custom manifests must provide equivalent durable
  storage; engine-log reconciliation remains the backstop for misconfiguration,
  storage failure, or quarantined corrupt WAL data.
- Exact counts depend on the deployed engine emitting the OpenAI usage block,
  including cached prompt tokens. Phoebe records missing/invalid usage but never
  invents it.
- Pricing, including late events priced at their own hour's rate, is the
  manager's (billing-design.md D1).
- Final invoice equality depends on the manager preserving `usage_id`, the
  quantities, delete-by-absence scoped to (install, hour, kind), and an
  invoice-line export/API. Phoebe cannot prove or repair that external state.
