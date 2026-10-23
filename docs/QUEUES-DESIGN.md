# Native queues in Prisma Streams — design for review

Status: **proposal, revision 2, 2026-10-23, for the owner's review before any
implementation.** Written by the lead from the queue usage review of
7 October 2026 (the control plane's 18 Cloudflare queues and its database
work queues) and a read-only inventory of `slate` at 86d7476d. Revision 2
answers an adversarial review by Fable 5.1 (two blockers, seven high and
nine medium findings; §14 lists each and where it is answered). Nothing here
is built. Every client-visible change becomes an edge record when it lands.

## 1. What we are building and why

The control plane runs 18 Cloudflare queue workloads (about 758,000 writes a
day, 8.8 a second on average, 62,904 in the busiest hour, cron bursts much
faster) and several database work queues (Bloom turns and drafts, Vercel
credential cleanup, the usage outbox). The review concludes that a retained
stream with durable, work-sharing consumer groups can serve them all, if each
group keeps per-delivery state: claims, fenced leases, independent
acknowledgement, scheduled retry, renewal, terminal failure, dead letters,
inspect and redrive.

Prisma Streams already has most of the durable machinery. Consumer groups
lease, acknowledge, retry, extend and dead-letter records, and their state
commits on the same object-store watermark as appends
(`src/shard/commit_plan.rs`), so a pull, a settle and an append share one
failure boundary. What it lacks is the delivery model a queue needs:

| Today | A queue needs |
|---|---|
| Every group is per-key FIFO; records without a routing key share one lane, so an un-keyed group has one batch in flight | Unordered competing consumers by default; per-key order only when asked for |
| A retry with a delay keeps the record leased and its key blocked, and pins the cursor | A retry frees the delivery and schedules it; later work proceeds |
| A pull sees a 4 MiB / ~4,100-record window above the cursor | A claim finds due work wherever it is |
| No per-message delayed availability | `deliverAt` / `delay` on send, surviving restarts, never blocking ready work |
| Dead-lettering only on exhausted attempts; with no DLQ an exhausted record is silently dropped | Explicit fail now, an explicit discard policy, never a silent drop |
| The DLQ copy loses enqueue time and failure reason; no redrive | Envelope with identity, times, attempts, last error; durable redrive with lineage |
| No pause, no stats beyond `backlog`, no cancellation, no listing | Pause/resume, stats (ready, scheduled, leased, oldest ages, DLQ depth), cancel by id |
| Producer dedup by producer id + sequence (client keeps state) | A message-level dedup key with a time window |
| The SDK has a sequential iterator | A concurrent worker runner with outcomes, backoff, auto-extend, graceful stop; Workers support |

This design adds queues as a **mode of the existing engine**, not a second
engine: a queue is a stream whose records are message envelopes, consumed by
groups whose delivery state is a pending set instead of a FIFO cursor.

## 2. Principles

1. **One durability boundary.** A send, a claim, a settle, a dead-letter
   transfer and a redrive return only after their effect is on object storage
   (G1). Message records and group state are in the same shard log and WAL.
2. **At-least-once, stated plainly.** A handler can run more than once.
   Leases are fenced so a stale worker cannot complete someone else's
   delivery, but no queue makes a provider call exactly once. The queue
   supplies the identity (message id, attempt, original time) that domain
   idempotency needs.
3. **No head-of-line blocking unless ordering is requested.**
4. **Nothing disappears silently.** Failed, exhausted, expired and cancelled
   messages are dead-lettered or counted under an explicit policy. A group
   behind retention reports the gap.
5. **The server can manage delivery without the stream key.** Everything the
   dispatcher, expiry and stats need lives in a small metadata row per
   message, written atomically with the message, holding only metadata of the
   kind G8 already admits. Bodies, headers and error texts stay encrypted.
6. **Bounded server memory.** Disk is the truth; memory holds a bounded
   working set per group, charged to the project's memory line.

## 3. Concepts

- **Queue**: a stream created with `kind: "queue"`. Its records are message
  envelopes written by the server. It has a retention policy and a
  dead-letter policy, and keeps the stream's encryption, quotas, stored-bytes
  cap and billing. It is readable as a stream (replay, audit, SSE).
- **Message**: one envelope record plus its metadata row. Its id is
  `msg_<base32(epoch, segment, offset)>`: offsets are segment-local, so the
  segment is part of the identity. The id is stable across retries.
- **Group**: a named consumer of a queue. Members of one group compete;
  separate groups each see every message. Every queue gets a group named
  `default`.
- **Delivery**: one message's state within one group: ready, scheduled,
  leased, or done.
- **Dead-letter queue (DLQ)**: a queue that receives failed deliveries, with
  fixed rules (§5.5).

## 4. Message envelope and metadata row

**Envelope** (the encrypted record value, written by the server):

```json
{
  "id": "msg_01h9…",
  "deliverAt": 1792600120000,
  "key": "workspace_123",
  "dedupKey": "stripe-usage:ws_123:2026-10-22",
  "contentType": "application/json",
  "headers": { "schema": "stripe-usage.v2", "traceparent": "00-…" },
  "lineage": { "redriveOf": "msg_01h7…" },
  "body": { "workspaceId": "ws_123", "date": "2026-10-22" }
}
```

- The envelope has no version field: the stored format is versioned by
  `LAYOUT_VERSION` only, as for every other stored format.
- **Enqueue time** is the record's page commit timestamp, assigned by the
  committer, monotone per segment. It is served as `enqueuedAt` (today
  `PlainRec` drops it; the read path changes to keep it), and retention ages
  by it. Redrive carries the original in `lineage.originalEnqueuedAt`.
- `body` is JSON, or `bodyBase64` for bytes. The whole envelope must fit the
  131,072 B record limit; the server refuses an oversized message with 413 and
  states the envelope's share. `headers`: at most 16 entries, 4 KiB, strings.
- Clients never write envelopes: a raw `records` append to a queue is refused
  (409 `queue_append_via_messages`).

**Metadata row** (plaintext, written in the same WriteBatch as the record):
`{deliverAt?, keyTag?, dedupTag?}` keyed by message id, where `keyTag` and
`dedupTag` are HMACs of the key and dedup key under the stream key (as lease
tokens are MAC'd today, `src/product_cursor.rs`). A delivery time and two
opaque tags are metadata of the kind G8 already allows (routing keys are
visible today); they let the dispatcher, the ordering index, expiry and
dedup work without the stream key.

**Appends.** A queue's records all use the stream's empty routing lane; the
per-message `key` lives only in the envelope and the metadata row. A batch is
therefore one append in one lane, as today, plus N metadata rows in the same
WriteBatch. This changes `src/shard/transaction/append.rs` (a formal-receipt
input) but needs no multi-key page cutting.

## 5. Delivery model

### 5.1 Group state

Per group and per lineage segment (offsets are segment-local; a claim merges
segments oldest-first as `pull` does today), in new shard-log key families:

| State | Meaning |
|---|---|
| dispatch cursor `D` | every offset below `D` has been dispatched into the pending set once |
| pending row per message | `{state, availableAt, attempts, leaseGen, leaseDeadline, firstDeliveredAt, errorCode?}`; `errorCode` is a server enum (`handler_failed`, `lease_expired`, `invalid_payload`, `cancelled`, …), never tenant text |
| due index `(availableAt, msgId)` | ready and scheduled deliveries in claim order |
| key index `(keyTag, offset)` (ordered groups) | the lowest pending offset per key |
| last error (encrypted) | the worker's error text, encrypted under the stream key, at most 1 KiB, written by the settle that failed; read only to build the DLQ envelope |
| low-water mark `L = min(D, lowest pending offset)` | what reclamation must not remove for this group |
| counters | ready, scheduled, leased, done, dead-lettered, discarded, expired, cancelled; delivered, acked, retried, failed, stale |

A dispatched message enters the pending set as `ready` (no `deliverAt`, or
due) or `scheduled`. `D` then moves on: scheduled and retried messages never
hold it back, so the old 4 MiB window disappears. Done deliveries are deleted.

**Working set.** The committer keeps, per group, an in-memory **ready heap**
of at most `maxInFlight + 1,024` due entries, refilled from the due index in
`availableAt` order and from new records past `D`. Claims are served from the
heap in commit order, so concurrent claims never fight over the same
candidates and never scan the index inside the committer. Disk is the truth;
the heap is rebuilt on open. The heap is charged to the project's memory line
(about 100 B per entry).

**Caps.** `maxPending` per group (default 1,000,000): when reached, the
dispatcher stops advancing `D`, so further messages wait as un-dispatched
records (their `deliverAt` honoured when dispatched); sends are never refused
for it, and stats report `dispatchHeld`.

### 5.2 Claim

`claim(group, max, leaseMs, waitMs)` takes up to `max` deliveries from the
heap whose `availableAt ≤ now`, subject to ordering (§5.6) and `maxInFlight`
(§5.7). For each: `attempts += 1`, `leaseGen += 1`, `leaseDeadline = now +
leaseMs`, `firstDeliveredAt` set once; it returns the envelope, `attempt`,
`enqueuedAt` and a fenced lease token. If nothing is due and `waitMs > 0`, the
claim parks until a new record, a settle, a lease expiry or **the next
`availableAt`** (a timed wake; `pull_wait.rs` gains a timer), up to 25 s. A
claim reply is released only after its lease rows are durable.

**Expired leases.** A lease found past its deadline is not re-claimed at once:
it counts as a failed attempt (`errorCode: lease_expired`) and is rescheduled
at `now + backoff(attempts)`, so a crashed worker's leases come back on the
group's backoff, not as an instant flood.

**Read memory.** A claim reserves `min(max, 8) × 128 KiB` (at most 1 MiB)
under the project's memory line before reading envelopes, settled to the bytes
actually read, replacing the pull's fixed 4 MiB coverage.

### 5.3 Settle

One settle request carries any mix of operations, each by lease token:

| Operation | Effect |
|---|---|
| `ack` | done; pending row deleted |
| `retry {delayMs?, error?}` | lease released; scheduled at `now + delay` (given, or the backoff policy); does not block other messages |
| `extend {leaseMs}` | deadline moved to `now + leaseMs`; refused as `expired` if the deadline passed |
| `fail {code?, error?}` | terminal now: dead-lettered, or discarded under a discard policy |
| `release` | lease released, ready again at once; **the attempt is given back** (`attempts -= 1`), for graceful shutdown |

- A token whose `leaseGen` no longer matches is `stale` and changes nothing,
  so settles are safe to replay.
- **Fencing.** An extend after the deadline is refused. An ack after the
  deadline is accepted while the delivery has not been rescheduled or
  re-claimed (the work happened); after that the token is stale. Today an
  extend is honoured after the deadline; queues tighten that.
- Reply: per-token outcomes and the group's counters.

### 5.4 Retry policy

Per group: `maxAttempts` (default 5; the API counts total attempts, not
"retries after the first") and `backoff {initialMs: 1000, maxMs: 43_200_000,
multiplier: 2, jitter: "full"}`. An explicit `retry.delayMs` (the SDK maps a
`retryAfterSeconds` to it) overrides the policy. When `attempts` reaches
`maxAttempts`, the next failure (retry, fail or expired lease) is terminal.

### 5.5 Terminal disposition and dead letters

Each queue has `deadLetter: {queue: "<name>-dlq"}` (default, created with the
queue in the same project with the same encryption key) or `deadLetter:
"discard"` (explicit; every discard counted). There is no silent drop.

**A DLQ's own rules:** `deadLetter: "discard"` is fixed (no DLQ of a DLQ); its
`default` group is the redrive group; retention defaults to 14 days and
expiry there discards with a count.

**Transfer.** A terminal failure appends to the DLQ an envelope with the
original body byte-identical and:

```json
"lineage": {
  "deadLetteredFrom": { "queue": "cp-email", "group": "default",
                         "messageId": "msg_01h7…" },
  "originalEnqueuedAt": 1792600000123,
  "attempts": 11, "firstDeliveredAt": 1792600001000,
  "failedAt": 1792640000000,
  "lastError": { "code": "handler_failed", "message": "Brevo 504" },
  "cause": "exhausted"
}
```

The DLQ append is durable first, under the deterministic producer id
`dlq:{group}:{messageId}` (a repeat is detected), and only then is the source
delivery completed. **DLQ and redrive appends bypass the dedup index**: their
producer id is their idempotency.

**The transfer needs the stream key** (the body is decrypted and re-encrypted
under the DLQ stream's segment keys; ciphertext cannot be moved). So a
terminal delivery is first marked `deadLettering` keylessly and copied by the
next request of that group that carries the key (claim, peek, settle, stats
with key). Stats show `deadLetteringPending`. If no keyed request arrives
before the record is physically reclaimed, the message is counted
`lost_unclaimed` (§7, decision D4).

### 5.6 Ordering

`ordering: "none"` (default for queues) or `ordering: "key"`.

- `none`: deliveries are independent.
- `key`: only the **lowest pending offset of a key is claimable, whatever its
  state**. A scheduled head (a future `deliverAt`, or a retry) blocks its key
  until it is done; that is what per-key order means, and stats count
  `keysBlockedByScheduled`. Messages without a key are unordered, not one
  shared lane. This serves Bloom's one execution per slot and per-tenant
  command order.

### 5.7 Concurrency and pacing

- `maxInFlight` per group (default 1,000, maximum 10,000).
- `rate {perSecond, burst}` per group (optional): a token bucket on claims for
  provider pacing. Shared budgets across groups are a later `limiters`
  resource.
- Every claim and settle is one project request; a worker fleet needs a
  request tier sized to it (§10, decision D11).

### 5.8 Pause, cancel, inspect

- `pause` / `resume` a group: a paused group's claims return empty at once;
  sends continue; granted leases run out. Durable, shown in stats.
- `cancel(messageIds, group?)`: completes the delivery in one group or all.
  A message not yet dispatched gets a `cancelled` row that the dispatcher
  honours and deletes when `D` passes it. A leased holder sees `stale`.
  Recorded in counters and the audit journal.
- `peek(group, state?, limit, after)`: lists pending deliveries with envelope
  and state, without leasing. Scope `queues.inspect`.

### 5.9 Redrive

`redrive(dlq, messageIds | {all, limit}, to?)`:

1. Append to the source queue (or `to`, which must share the DLQ's encryption
   key; scope `queues.manage` on both) a new message with the original body,
   headers, key and dedup key, `lineage.redriveOf`, `originalEnqueuedAt` and
   the attempt history, under the producer id `redrive:{dlq}:{dlqMessageId}`
   and **bypassing the dedup index**.
2. Then complete the DLQ message in the DLQ's `default` group.

Every redrive, cancel, pause, resume and purge is written to `_audit_events`.

### 5.10 Clock

Lease deadlines and due times use the owning server's clock. A shard move to a
host whose clock differs by Δ shifts every deadline and due time by Δ; the
design tolerates Δ up to the 5 s already assumed by lease handoff and states
it in RUNBOOK. Due times are absolute, so restarts do not lose schedules.

## 6. Producer side

- `send(body, {deliverAt | delayMs, key, dedupKey, headers, contentType})` →
  `{id, enqueuedAt, deliverAt, duplicate}`.
- `sendBatch([...])`: up to 1,000 messages or 8 MiB, **atomic** (G4), each
  message with its own `key`, `dedupKey` and `deliverAt` (they live in the
  envelope and metadata rows, §4). Safe to retry as a whole with dedup keys.
- **`deliverAt` horizon:** at most `retention.maxAge` ahead of now (refused
  400 beyond it); a scheduled message ages from `max(enqueuedAt, deliverAt)`,
  so it is never expired before it is due (decision D3).
- **Dedup:** a `dedupKey` seen in the same queue within the dedup window
  (default 24 h, maximum 7 days, never longer than retention) returns the
  original id with `duplicate: true` and stores nothing. The index is
  `dedupTag → (msgId, expiresAt)` plus an expiry-ordered index swept by sends,
  so it stays bounded by the window. Dedup keys ≤ 512 bytes.
- Producer id/epoch/sequence stays available. Internal producer rows
  (`dlq:`, `redrive:`) get a TTL equal to the DLQ's retention instead of
  living forever.
- Every existing send guard applies: quotas, the stored-bytes cap, fair
  shedding, and the 503/429 answers the SDK rides through.

## 7. Retention, expiry and reclamation

- `retention {maxAge, maxBytes}` per queue; default `maxAge` 4 days (the
  Cloudflare baseline), maximum 14 days.
- **Expiry is keyless.** A delivery whose message aged past `maxAge` (from
  `max(enqueuedAt, deliverAt)`) is marked `expired` in that group without the
  key, counted, and listed in the group's `retentionGap`; its DLQ copy is
  made lazily by the group's next keyed request (§5.5).
- A group created after messages aged out, or whose `D` is below the retained
  start, reports `retentionGap {from, to, count}` in stats and in its next
  claim reply. Nothing is skipped silently.
- **Reclamation is required before production queues.** Logically expired
  records still occupy storage, count against the project's stored-bytes cap
  (#158) and are billed; at the control plane's 139 MB a day a project hits
  the 10 GiB beta cap in about 74 days and its sends are then refused. Queue
  segments are therefore reclaimed: a queue's lineage segment is rolled over
  daily, and a sealed segment whose records are past retention and below
  every group's `L` is deleted whole (its objects removed, its gauges
  released). This is a narrower, earlier piece of layout 6 stage 3b's
  reclamation, built for queues in Q3 (decision D5).

## 8. Telemetry

- A group's `GET` returns `ready`, `scheduled`, `leased`, `pending`,
  `oldestReadyAgeMs`, `oldestPendingAgeMs`, `nextScheduledAt`, `paused`,
  `dispatchHeld`, `deadLetteringPending`, `keysBlockedByScheduled`,
  `retentionGap`, and rolling counters.
- A queue's `GET` returns its groups' stats and the DLQ depth.
- `/operator/data.json` gains per-project queue totals and the worst
  oldest-ready age; the cell watch gains rules for DLQ growth and old ready
  work, calibrated after the pilot.
- Usage: queue operations metered as today, plus read and stored bytes.

## 9. HTTP surface

New product routes under `/v1/queues`; the stream read routes also work on a
queue. Header names use the `Streams-` form the Compute edge passes.

| Route | Purpose |
|---|---|
| `PUT /v1/queues/{name}` · `GET` · `DELETE` | create/update (`retention`, `deadLetter`, `dedupWindowMs`, default group config) · config and stats · delete |
| `GET /v1/queues` | list the project's queues |
| `POST /v1/queues/{name}/messages` · `messages:batch` | send |
| `GET /v1/queues/{name}/messages/{id}` | envelope and per-group delivery state |
| `PUT …/groups/{group}` · `GET` · `DELETE` | group config (`ordering`, `maxAttempts`, `backoff`, `leaseMs`, `maxBatch`, `maxInFlight`, `maxPending`, `rate`, `start: earliest \| now`); all but `ordering` updatable in place |
| `POST …/groups/{group}:claim` · `:settle` | claim; ack / retry / extend / fail / release |
| `POST …/groups/{group}:pause` · `:resume` · `:cancel` | operator actions |
| `GET …/groups/{group}/pending` | peek (`queues.inspect`) |
| `POST /v1/queues/{dlq}:redrive` | redrive |

New scopes: `queues.send`, `queues.consume`, `queues.manage`,
`queues.inspect`; the issuer's grants and `cell-admin` learn them.

Today's consumer routes (`/v1/streams/{name}/consumers/{c}:pull` and
`:settle`) become groups with `ordering: "key"` over the same engine
(decision D2).

## 10. SDK

```ts
import { StreamsClient, RetryLater } from "@prisma/streams";

const client = await StreamsClient.connect({ issuerUrl, credential });
const emails = client.queue<EmailJob>("cp-notification-channel-email");

await emails.send({ notificationId }, { dedupKey: notificationId, delay: "2m" });
await emails.sendBatch(jobs.map((j) => ({ body: j, key: j.workspaceId })));

const worker = emails.process(
  async (msg, { signal }) => {
    await deliver(msg.body, { idempotencyKey: msg.id, signal });
    // return → ack
    // throw new RetryLater({ after: "30s" }) → retry with that delay
    // throw any other error → retry with the group's backoff
    // return msg.fail("unsubscribed") → dead-letter now
    // return msg.discard() → done, counted as discarded
  },
  { group: "default", concurrency: 10, lease: "5m", autoExtend: true },
);
await worker.stop(); // stop claiming, finish in-flight, release the rest

await emails.stats();
await emails.deadLetters().redrive({ all: true, limit: 100 });
```

- **Runner.** Keeps up to `concurrency` handlers running; claims `min(free
  slots, maxBatch)` with long-poll; coalesces settles for up to 250 ms into
  one request (a crash inside that window means those messages are
  redelivered: at-least-once); extends leases at half their length while a
  handler runs. If an extend comes back `expired` or `stale`, the runner fires
  the handler's `AbortSignal`, still sends the ack when the handler finishes
  (accepted if the delivery was not yet rescheduled), and reports the outcome.
  `stop()` stops claiming, waits for in-flight handlers up to a deadline, then
  releases the rest without spending their attempts.
- **Requests.** A runner at concurrency 10 makes about 4-6 requests a second
  under load (claims plus coalesced settles). The beta's self-serve 10/s tier
  fits one or two runners; queue-heavy projects need a larger tier (D11).
- **Typed messages.** `client.queue<T>(name, { parse })` validates each body;
  a body that fails is failed with `code: "invalid_payload"`.
- **Runtimes.** Node, Bun, Deno and Compute run the runner. **Cloudflare
  Workers** can send, and can consume with `claimBatch()` / `settle()` from a
  cron trigger (one-minute granularity) or a Durable Object alarm; Workers
  join SDK CI. A Worker cannot host a long-lived runner, so the Console's
  fifteen Worker consumers need push delivery (D9) before they migrate.
- **Local development.** `createLocalQueue()` gives an in-memory
  implementation of the same API with recorded calls, held to the same
  contract tests as the server; the repo's local server runs the real thing.
  A client without configuration throws at construction in production.
- **Telemetry hooks.** `onMessage`, `onSettle` with id, attempt, outcome and
  duration, for OpenTelemetry or Sentry.

## 11. How the control plane's workloads map

| Workload | Queue settings |
|---|---|
| Stripe and spend-limit usage sync | unordered; `maxAttempts 11`, backoff 30 s..12 h; `rate` for Stripe; the 80-minute spread via per-message `deliverAt` |
| Offer and spend-limit enforcement | unordered; dedup key per (workspace, hour); DLQ by default (spend limit has none today) |
| Vercel usage sync → billing data | unordered; `delay 2m`; billing data as a downstream queue fed by the usage handler |
| Notification controller → email | two queues; email `concurrency` matched to Brevo; the domain claim stays |
| Dunning, tenant-manager sync | `ordering: key` by workspace or tenant where the business rule needs order |
| ppg-events-shared | external producer with a `queues.send` credential; tolerant parse; a DLQ group that marks provisioning failed |
| Invoice submission | unordered; self-continuations via `deliverAt`; the reconciler stays |
| Bloom turns | `ordering: key` by slot, `lease 15m` + auto-extend, `cancel` by id; draft admission stays in Bloom |
| Credential cleanup | `deliverAt` per request (within the horizon, D3); the request row keeps per-key results |

Pilot: `cp-plain-support-sync` (low volume, replay-safe), then enforcement,
then billing; Bloom last.

## 12. Correctness and verification

- **TLA+ `QueueDelivery`**: dispatch cursor, pending set, due index, ready
  heap, leases with fencing, scheduled and expired-lease retry, fail and the
  lazy DLQ saga, redrive, keyless expiry, cancel before and after dispatch,
  pause, crash/restart, segment reclamation. Invariants: every appended
  message is, in each group, pending, done, dead-lettered, discarded,
  expired, cancelled or counted `lost_unclaimed`; a stale leaseholder cannot
  change a delivery; DLQ and redrive appends happen at most once per source
  delivery; **a redriven message is stored at least once**; in a `key` group
  only a key's lowest pending offset is ever leased; reclamation never removes
  a record at or above any group's `L`. Liveness: under fair claims a ready
  delivery is eventually leased; a scheduled delivery becomes ready at its
  time.
- **Kani**: envelope and metadata-row codecs, key-family codecs, lease token,
  backoff arithmetic, due-index ordering.
- **DSTs**: mixed-success batches, crash mid-batch and inside the settle
  coalescing window, lease expiry with backoff, scheduled work across
  restart, no head-of-line blocking, key order with scheduled heads, DLQ and
  redrive crash windows, redrive of a message with a live dedup key, keyless
  expiry then lazy DLQ copy, gap reporting, cancel before dispatch, pause,
  `maxInFlight`/`maxPending`/`rate`, segment reclamation against `L`,
  multi-tenant isolation of queues and DLQs.
- **SDK**: runner tests against the real server, Workers in CI, the local
  fake under the same contract tests.
- **Layer B queue arm**: queue workers, a poison-message tenant and a send
  flood in the shared-cell harness, under the same bars as streams.
- **Pilot**: `cp-plain-support-sync` on the scratch cell for a week beside the
  old queue.

## 13. Decisions for the owner (each with a recommendation)

| # | Decision | Recommendation |
|---|---|---|
| D1 | Resource shape | `/v1/queues/{name}` over streams of kind `queue` |
| D2 | One engine | Move today's consumer groups onto the pending model (`ordering: key`) in the same release, with a `LAYOUT_VERSION` bump that deletes the old `c/l/x` rows' decoder (no users; one engine to verify). Reconcile the bump with the paused layout 6 work |
| D3 | `deliverAt` horizon | At most `retention.maxAge` ahead; age scheduled messages from `max(enqueuedAt, deliverAt)` |
| D4 | Expiry when a group is idle | Keyless expiry with a lazy, keyed DLQ copy; a copy that never happens before reclamation is counted `lost_unclaimed` and alerted, not silent |
| D5 | Reclamation | Build queue segment reclamation in Q3, **required before any production queue** (logically expired bytes otherwise fill the stored-bytes cap in ~74 days at control-plane volume) |
| D6 | Default dead-letter policy | Auto-create `<name>-dlq`; a DLQ discards on failure and has no DLQ of its own |
| D7 | Batch semantics | Atomic batches ≤ 1,000 messages / 8 MiB, keys per message via the envelope and metadata row |
| D8 | Dedup | 24 h default window, 7 days maximum, never above retention; DLQ and redrive appends bypass it |
| D9 | Workers consumers | v1: send from Workers, consume via cron or Durable Object alarm; **push delivery** (signed batches POSTed to an HTTPS endpoint, settled by the response) in phase 2, before the Console's Worker consumers migrate |
| D10 | Shared provider budgets | Phase 2 `limiters`; v1 has per-group `rate` |
| D11 | Request tier for worker fleets | A `queues` tier per project (e.g. 100 req/s) set by `cell-admin`, priced like other requests |
| D12 | Caps and memory | `maxInFlight` 1,000 (max 10,000), `maxPending` 1,000,000 per group, ready heap ≤ `maxInFlight + 1,024` entries charged to the project memory line |
| D13 | Billing | Meter queue operations as today; no separate queue price in the beta |
| D14 | Sequencing | Build queues before the beta opens, as asked; the beta's other open items proceed in parallel |

## 14. Review findings and where they are answered

| Finding (Fable 5.1, 2026-10-23) | Answer |
|---|---|
| B1 DLQ/expiry need the stream key; an idle group cannot be dead-lettered | §5.5 lazy keyed copy, §7 keyless expiry, D4 |
| B2 redrive through the dedup index drops the message | §5.5, §5.9, §6: DLQ and redrive bypass dedup; TLA invariant |
| H1 `deliverAt` past retention | §6 horizon, §7 aging from `max(enqueuedAt, deliverAt)`, D3 |
| H2 expired bytes fill the stored-bytes cap | §7 reclamation required, D5 |
| H3 claim contention, memory, caps | §5.1 ready heap and caps, §5.2, D12 |
| H4 per-message keys in an atomic batch | §4 single lane + metadata rows, D7 |
| H5 key order broken by `deliverAt` | §5.6 lowest pending offset only |
| H6 expired leases bypass backoff | §5.2 expired-lease rescheduling |
| H7 tenant text in plaintext rows | §4 MAC'd tags, §5.1 error codes + encrypted error text |
| M1 message id without segment | §3 id, §5.1 per-segment state |
| M2 cancel before dispatch | §5.8 cancelled row |
| M3 DLQ recursion | §5.5 DLQ rules |
| M4 unbounded internal rows | §6 dedup expiry index, internal producer TTL |
| M5 layout bump | §4 no envelope version, D2 bump |
| M6 request and memory quotas for workers | §5.2 claim reservation, §10 request figures, D11 |
| M7 runner races | §5.3 `release` gives the attempt back, §10 runner |
| M8 two enqueue times | §4 page commit timestamp served and used |
| M9 clock | §5.10 |
| Lows (crypto wording, cron granularity, timed wake, redrive target key) | §7, §10, §5.2, §5.9 |
| Estimate 1.6-2× | §15 |

## 15. Plan and size

| Package | Contents | Estimate |
|---|---|---|
| Q1 Delivery engine | pending set, dispatch cursor, due index, ready heap, claim/settle (ack, retry, extend, fail, release), backoff incl. expired leases, both orderings, `maxInFlight`, `maxPending`, `rate`, pause, timed park wake, fencing; consumer routes moved onto it; layout bump | 15-22 days |
| Q2 Queue resource | `kind: queue`, envelope and metadata rows in the append path, `/v1/queues` routes, scopes, dedup index, scheduled messages, lazy DLQ saga, cancel, peek, redrive, audit, `enqueuedAt` served | 10-15 days |
| Q3 Retention and reclamation | keyless expiry, gap reporting, DLQ rules, daily segment rollover and whole-segment reclamation | 8-12 days |
| Q4 SDK | queue API, runner, outcomes, auto-extend with abort, typed parse, local fake, Workers CI, hooks | 6-9 days |
| Q5 Verification | TLA `QueueDelivery`, Kani codecs, DST suite, Layer B queue arm, pilot | 10-15 days |
| Phase 2 | push delivery, `limiters` | 8-14 days |

**Q1-Q5: about 50-73 engineering days** (revision 1 said 28-42; the review's
1.6-2× was right once the heap, the append-path change, the layout bump,
reclamation and a larger formal model are counted). Order: Q1 → Q2 → Q3, with
Q4 against Q2's routes and Q5 alongside. Each package lands red-first with its
edge records and gates.

Costs to plan for: edits to `src/shard/transaction/append.rs` and the shard
transaction core stale formal receipts (about 150 min serial per round);
`src/queue.rs` and `src/product_cursor.rs` are by-path includes, so new key
families and token fields go in new modules; `src/product.rs` has no
headroom, so routes live in `src/product/queues/`; the frozen exception
scopes in `receive`, `settle` and the dispatcher are replaced, not grown;
mutants at about 5 minutes each with the per-push cap mean several pushes per
package.
