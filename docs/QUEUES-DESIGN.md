# Native queues in Prisma Streams — design for review

Status: **proposal, revision 4, 2026-10-23, for the owner's review before any
implementation.** Revision 1 drew on the queue usage review of 7 October 2026
and a read-only inventory of `slate` at 86d7476d. Revision 2 answered an
adversarial review by Fable 5.1. Revision 3 adopted a colleague's review,
which approved the direction and asked for four architectural changes.
Revision 4 folds in Fable 5.1's final review of revision 3. §15 says what
was taken from each review, what was declined, and why. Nothing here is
built. Every client-visible change becomes an edge record when it lands.

## 1. What we are building and why

Prisma Streams can give applications reliable background work beside their
data: send a message, have it run, retry it on failure, inspect and redrive
what failed. Prisma's control plane is the first proving ground. It runs 18
Cloudflare queue workloads (about 758,000 writes a day, 8.8 a second on
average, 62,904 in the busiest hour, cron bursts much faster) and several
database work queues.

**The case is platform capability, not cost.** At Cloudflare's published
price ($0.40 per million operations, about three per message) the control
plane's queue operations cost roughly $27 a month. Native queues are worth
building because they make Prisma a place where an app stores data, runs code
and schedules reliable work under one operating model, not because they
replace that bill.

Prisma Streams already has the hard part. Consumer groups lease, acknowledge,
retry, extend and dead-letter records, and every append, pull and settle is
acknowledged only after it is durable on object storage
(`src/shard/commit_plan.rs`, `DurableEffects`). What the current groups lack
is a queue's delivery model:

| Today | A queue needs |
|---|---|
| Every group is per-key FIFO; records without a routing key share one lane | Unordered competing consumers |
| A retry keeps the record leased, its key blocked and the cursor pinned | A retry frees the delivery and schedules it |
| A pull sees a 4 MiB window above the cursor and stops at a sealed segment with backlog | A claim finds due work wherever it is |
| No per-message delayed availability | `deliverAt` / `delay` on send |
| Dead letters only on exhausted attempts, copied to another stream with the stream key | Failure as a durable, keyless state with recovery |
| No redrive, pause, stats, cancellation or listing | All of these |
| A sequential iterator, and no way to wake a sleeping worker | A worker runner, and a wake-up path for serverless workers |

## 2. Scope and boundaries

**What queues replace:** delivery and retry machinery: claiming work,
leases, retries with backoff, scheduled availability, failure handling and
redrive.

**What queues do not replace:**

- **Atomicity with a business database.** Committing state in Postgres and
  sending a message are two operations, and a send inside a Postgres
  transaction is not part of it. A crash between them leaves one without the
  other, in either order. Applications that need both keep a transactional
  outbox or a reconciler. The control plane's usage outbox, invoice
  reconciler and sweeps stay.
- **Exactly-once effects.** Delivery is at-least-once. Lease fencing protects
  queue state; it cannot stop an old handler from finishing an external call
  after its lease expired. Domain idempotency stays the application's job;
  the queue gives it a stable identity (§4).
- **Workflow state.** Bloom's admission limits, the email domain claim and
  the setup-attempt ownership stay in their databases. Queues carry their
  work items.
- **History.** A queue is not a log. It keeps a message until every group is
  done with it or it expires, and then reclaims it (§8). Workloads that need
  replay use a stream.

## 3. Principles

1. **One durability boundary.** Every queue operation is acknowledged only
   after its effect is durable on object storage, in the same shard log as
   the messages (G1).
2. **At-least-once, stated plainly** (§2).
3. **Ready work is never hidden** by scheduled work, older messages, segment
   boundaries, clocks or caches. Ordering, when requested, is the only thing
   that may hold a message back, and only behind earlier messages of its own
   key.
4. **Nothing disappears silently.** Failed deliveries stay recoverable for a
   stated period; expired and discarded messages are counted and reported.
5. **The server manages delivery without the stream key.** Scheduling, expiry,
   failure and reclamation use plaintext metadata only (times, counts, sizes,
   MAC'd tags, server error codes). The key is needed only to read or write
   message bodies and error text.
6. **Every byte of new state is bounded and accounted.**

## 4. Concepts and identities

- **Queue**: a stream created with `kind: "queue"`. Its records are message
  envelopes the server writes. It has retention, an admission budget and a
  failure-retention period, and keeps the stream's encryption, quotas and
  billing. In the first release a queue is reached only through `/v1/queues`:
  stream reads, raw appends and forks of a queue are refused (409
  `queue_via_queue_routes`), because reclamation leaves gaps a stream reader
  does not expect.
- **Group**: a named consumer of a queue. Members compete; separate groups
  each see every message. Every queue has a group named `default`, and the
  first release exercises that path. Groups stay in the data model.
- **Message id** `msg_<base32(incarnation, segment, offset)>`: stable for the
  message's life, across every retry and every redrive. Every route checks
  that the id's incarnation is the queue's current one (404 otherwise).
- **Delivery**: one message's state in one group. **Delivery id**
  `(group, msgId, cycle)`, where `cycle` starts at 1 and rises by one on each
  redrive. A **lease** within a delivery has a lease generation that fences
  stale workers.
- **Origin id** `originId`: the message id a republish chain started from
  (§7.3). Applications use their own business id (for example
  `notificationId`) or `originId` as the provider idempotency key; neither
  changes across retries, redrives or republishes.

## 5. Storage model

All new state lives in the queue's owning shard log, in new key families
under the stream's storage hash, written by the shard committer and durable
on the same watermark as appends. Index keys are encoded big-endian, so key
order is numeric order.

### 5.1 Message record and metadata row

The **envelope** is the encrypted record value:

```json
{
  "id": "msg_01h9…",
  "deliverAt": 1792600120000,
  "key": "workspace_123",
  "dedupKey": "notification:ntf_77",
  "contentType": "application/json",
  "headers": { "schema": "email.v2", "traceparent": "00-…" },
  "lineage": { "originId": "msg_01h9…" },
  "body": { "notificationId": "ntf_77" }
}
```

- `body` is JSON, or `bodyBase64` for bytes. `headers`: at most 16 entries
  and 4 KiB, strings only. The envelope has no version field: it is versioned
  by `LAYOUT_VERSION`, like every stored format.
- **One message, one page.** A queue send stores each message as its own page
  (`src/crypto_page.rs` otherwise packs up to 4,096 records of one append
  into one page). A page is the unit reclamation can delete, so this makes a
  message deletable on its own. It costs about 70 B of page framing per
  message and gives up compression across messages of one batch. A batch is
  still one atomic WriteBatch.
- `enqueuedAt` is the queue clock at the send (§5.5). It is served with every
  message.
- The envelope must fit the 131,072 B record limit (413 otherwise, stating the
  envelope's share). Failure adds nothing to it (§7.1), so an accepted message
  never outgrows its storage.
- Clients never write envelopes.

The **metadata row**, written in the same WriteBatch as the record, holds
`{availableAt, retentionEnd, bytes, open, cancelled, keyTag?, dedupTag?}`:
`availableAt = max(enqueuedAt, deliverAt)`; `retentionEnd = availableAt +
maxAge`; `bytes` the record size; `open` the number of open deliveries across
all groups (§8.2); `cancelled` a queue-wide cancel (§6.7); the tags HMACs
under the stream key. These are metadata G8 already admits (routing keys are
visible today).

### 5.2 The availability index: one scheduling order for the whole queue

Each send also inserts **one entry per message** into the queue's
**availability index** `A`, keyed `(availableAt, msgId)`, in the same
WriteBatch. `A` is queue-wide: it is keyed by the queue, not by lineage
segment, so segment rollover is a storage matter and never a scheduling
barrier. Message ids order as `(incarnation, segment, offset)`, and a
successor segment always has a higher number, so ids order exactly as sends
commit (a Kani proof in Q5).

**One scheduling owner per queue.** A queue writes one physical lane (the
empty routing key), so one shard owns it and its indexes. The scaler cannot
split a single-key workload (`src/scaler3.rs`, hot-key detection), so a queue
has a per-queue throughput ceiling, to be measured in Q5 and published (the
first target is 1,000 messages a second per queue; the control plane's busiest
hour averages 17.5). A shard move carries the queue with its state.

### 5.3 Group state

Per group:

| State | Meaning |
|---|---|
| group cursor `C` = `(time, msgId)` | every `A` entry at or below `C` has been taken or skipped by this group |
| delivery row per **open** delivery | `{state: leased \| retrying \| failed, cycle, attempts, releases, leaseGen, leaseDeadline, firstDeliveredAt, firstFailedAt?, errorCode}` |
| lease index `L` `(leaseDeadline, msgId)` | this group's leased deliveries |
| retry index `R` `(availableAt, msgId)` | this group's retrying deliveries |
| failure index `F` `(failedAt, msgId)` | this group's failed deliveries |
| cancel rows | group-scoped cancels of messages ahead of `C` |
| failure detail (encrypted) | the worker's error text, ≤ 1 KiB, when a settle carried text and the key (§6.2) |
| counters | delivered, acked, retried, failed, expired, cancelled, discarded, redriven, stale settles |

A delivery that is neither leased, retrying nor failed has no row: it is
ahead of `C` (not yet taken) or done. Group state is proportional to work in
flight, in retry and in failure, not to the backlog. There is no `maxPending`
and no dispatch window.

### 5.4 Bounded memory

The committer keeps, per active group, a **ready cache** of at most
`maxInFlight + 1,024` due entries, refilled from `A` past `C` and from `R`,
both in `availableAt` order. The cache is never the truth: when it runs dry
the next claim refills it from the indexes, and it is rebuilt from them after
a restart, an ownership change or a refused commit. It is charged to the
project's memory line (about 100 B an entry).

### 5.5 The queue clock

All queue times come from one **queue clock** in the owning committer: `t =
max(now_ms(), fields.ts)`, and every queue operation that reads the clock
(send, claim, settle, sweep) writes `fields.ts = t` in its WriteBatch. So:

- `fields.ts` is durable and monotone across restarts, NTP steps and shard
  moves, and `C.time ≤ fields.ts` always: a claim can take an entry only when
  `availableAt ≤ t`, and writes `t` as `fields.ts` in the same batch.
- A send commits at `enqueuedAt = t ≥ fields.ts ≥ C.time`, and its id is above
  every taken id at that time, so **a new message never sorts at or below a
  group's cursor.** This is the invariant the TLA model checks, with clock
  steps both ways and shard moves.
- Queue routes ignore and refuse the `stream-timestamp` hint (400), which
  could otherwise push `fields.ts` into the future.
- A host whose clock is behind sees time stand still until it catches up
  (deadlines and schedules stretch by the gap); one ahead makes scheduled
  messages due and leases expire early by the gap. Lease handoff already
  assumes clocks within 5 s; RUNBOOK states the same bound for queues.

## 6. Delivery

### 6.1 Claim

`claim(group, max, maxBytes, leaseMs, waitMs)` runs in two steps.

**In the committer (keyless):**

1. Take due entries (`availableAt ≤ t`) in `availableAt` order from the
   merge of `A` past `C` and of `R`, within `maxInFlight` and the group's
   `rate`, skipping cancelled messages; stop at `max` or when the next
   message's metadata `bytes` would pass `maxBytes` (default 1 MiB, at most
   8 MiB; one message is always allowed).
2. Reserve read memory for exactly that total under the project's memory
   line; if the reservation is refused, claim nothing (429
   `project_memory_pressure`).
3. Lease exactly that set: delivery row to `leased`, `attempts += 1`,
   `leaseGen += 1`, `leaseDeadline = t + leaseMs`, entry in `L`; `open += 1`
   on each newly taken message; advance `C` past `A` entries taken or
   skipped.

**After the commit is durable (with the key, outside the committer):** read
the leased envelopes and return them. A read that fails releases those
deliveries server-side with the attempt given back (not counted against the
worker's releases, §6.2) and answers 503, retryable.

If nothing is due and `waitMs > 0`, the claim parks until a send, a settle, a
lease deadline or the group's next `availableAt` (a timed wake), up to 25 s.

**Expired leases.** Each claim and settle first moves up to 64 entries from
the head of `L` whose deadline has passed, and a timer at `min(L)` wakes the
sweep when the group is idle. An expired lease counts as a failed attempt
(`errorCode: lease_expired`): `leaseGen += 1`, moved to `R` at `t +
backoff(attempts)`. A crashed worker's leases come back on the backoff, not
as a flood.

### 6.2 Settle

One settle carries any mix of operations, each by lease token. **Every
operation except `extend` ends the lease: it sets `leaseGen += 1`, so a
replayed settle with the same token is `stale` and changes nothing.** The
schedule never moves twice and attempts are never adjusted twice.

| Operation | Effect |
|---|---|
| `ack` | delivery done: row deleted, `open -= 1` |
| `retry {delayMs?, code?, error?}` | to `retrying` in `R` at `t + delay` (given, or the policy, §6.3) |
| `extend {leaseMs}` | deadline to `t + leaseMs`; refused `expired` once the deadline has passed |
| `fail {code?, error?}` | to `failed` now (§7.1) |
| `release {notStarted: true}` | back to `R` due at `t + 1 s`, the attempt given back, `releases += 1`; **only for a message whose handler never started** |

- **Late settles.** `ack`, `retry` and `fail` after the deadline are accepted
  while the sweep has not yet moved the lease (at most about a second), and
  the reply marks them `acceptedLate`. Afterwards the token is stale.
- **Release is bounded.** The third consecutive release of a delivery
  without a started attempt is treated as a `retry`: the attempt counts and
  backoff applies. A worker cannot loop a message for free.
- **Graceful stop.** Work whose handler started and is then aborted is
  settled as `retry` (the attempt counts): a started handler may already have
  had an external effect. Only never-started work uses `release`.
- **Error text.** A keyless settle carries only a `code` (at most 64
  characters of `[a-z0-9_.-]`, stored plaintext). Error text (≤ 1 KiB) needs
  the stream key on the settle, as a claim does; the server encrypts it into
  the failure detail. The SDK sends the key with settles that carry text.
- The reply lists each token's outcome and the group's counters.

### 6.3 Retry policy

Per group: `maxAttempts` (default 5; the API counts total attempts) and
`backoff {initialMs: 1000, maxMs: 43_200_000, multiplier: 2, jitter: "full"}`.
An explicit `retry.delayMs` (the SDK maps `retryAfterSeconds` to it) overrides
the policy. **A retry is never scheduled past the message's `retentionEnd`:**
its time is clamped to it, and a retrying delivery due at or before
`retentionEnd` is always attempted. When `attempts` reaches `maxAttempts`, or
an attempt fails after `retentionEnd`, the delivery moves to `failed`
(`cause: exhausted` or `retention`).

### 6.4 Ordering

**First release: unordered** (`ordering: "none"`, the default). Deliveries
are independent; nothing but `maxInFlight` and `rate` limits concurrency. The
first-release pilots are therefore unordered queues only; the workloads that
need order (Bloom turns, dunning, tenant-manager sync) wait for Q6.

**Ordered groups (`ordering: "key"`) follow as Q6**, once their semantics,
specified here, are implemented and tested:

- Eligibility: a message is eligible in the group only when every earlier
  message of its key (in id order, which is commit order across segments) is
  done in that group. A key's scheduled, retrying or **failed** head blocks
  its key and only its key; a failed head blocks it until it is redriven and
  acknowledged, discarded or its failure retention ends. Stats report
  `keysBlockedByFailed`.
- Index: a queue-wide **key index** `(keyTag, msgId)` written at send, and per
  group a **key-head table** naming each key's current head, one row per key
  with work in the group, deleted when the key drains. The ready cache holds
  **eligible heads only**, so thousands of blocked followers of one key cannot
  crowd out another key.
- Messages without a key are unordered even in a key group.
- Today's stream consumer groups (per-key FIFO, `/v1/streams/{name}/consumers`)
  are untouched by the first release. Once ordered queue groups are proven,
  they move onto this engine and the old `c/l/x` rows are retired with a
  `LAYOUT_VERSION` bump (decision D2).

### 6.5 Concurrency, pacing and overload

- `maxInFlight` per group (default 1,000, maximum 10,000).
- `rate {perSecond, burst}` per group, optional: a token bucket on claims, for
  provider pacing. Shared budgets across queues are a later `limiters`
  resource.
- **Settles and extends have their own bucket.** Today a project has one
  request bucket (`max(rate, 50)` tokens, #162). Queue settles and extends
  draw instead from a **settle bucket** at half the project's request rate
  (burst floor 25), so a send or claim flood cannot starve lease maintenance
  and the free tier's main bucket is not cut. A settle request in which more
  than half the tokens are stale or invalid is charged to the main bucket, so
  junk settles buy nothing. Under the RSS shed (#184) settles follow the
  append rules: a blamed project's settles are refused like its other
  requests; an unblamed project's settles are spared up to the hard line.
  Both are edge changes with their own records.
- **Request rate.** A worker makes about `claims + settles + extends` requests
  a second: claims ≈ throughput / batch size, settles ≈ throughput / messages
  per coalesced settle, extends ≈ in-flight / (lease / 2) for long handlers.
  Projects running worker fleets need a request tier sized by that formula
  (decision D10).

### 6.6 Admission budget

Each queue has an **admission budget**: at most `maxQueuedBytes` (default
1 GiB of record bytes) and `maxQueuedMessages` (default 10 million) of
retained messages not yet done by every group. Messages held only by failed
deliveries do not count: failures have their own bound (§7.1). A send that
would exceed the budget is refused `429 queue_full`, retryable, with the
budget, its use and the group holding the most of it. Sends are never
silently held. A group that stops consuming fills the budget within its
backlog's reach, and its messages expire at `retentionEnd` (§8.1), which frees
it; the stats and the 429 name it so an operator can pause or delete it.

Queue sends are also admitted against the project's stored-bytes cap (#158),
like appends; the cap counts record bytes and the row charges of §8.3.

### 6.7 Pause, cancel, peek

- **Pause / resume** a group: a paused group's claims return empty at once;
  sends continue; granted leases run out. Durable, shown in stats.
- **Cancel** `(msgIds, group?)`: done without processing, counted, audited.
  Without a group, the metadata row's `cancelled` flag is set, every group
  (present or created later) skips the message, and open deliveries end
  (holders see `stale`). With a group, a cancel row in that group makes its
  claim skip the message when `C` reaches it (deleted then, or at
  `retentionEnd`); a leased holder sees `stale`.
- **Peek**: lists open deliveries and the next due entries with envelope and
  state, paginated by an opaque cursor, without leasing. Needs the key; scope
  `queues.inspect`.

## 7. Failure and recovery

### 7.1 Failed deliveries are durable state, not copies

A terminal failure (explicit `fail`, exhausted attempts, or an attempt after
`retentionEnd`) moves the delivery to **`failed`** in its group: a keyless
state change in the committer, durable like any other settle. The original
encrypted record stays where it is; nothing is copied. The failed row records
the cause, code, attempts, cycle, `firstDeliveredAt`, `firstFailedAt`,
`failedAt` and `failureExpiresAt`; the failure detail holds error text if a
keyed settle supplied it.

- Failure needs no stream key, no second append and no transfer saga. A
  delivery that fails while no worker is running and after the owning server
  restarted is still recorded and recoverable.
- Failure adds no bytes to the envelope, so a message accepted at the size
  limit can always fail.
- **Failure retention** (default 14 days, maximum 30) runs from the
  delivery's **first** failure, `firstFailedAt`, and a redrive does not renew
  it. At its end the delivery is discarded and counted, whatever its state.
  So every message has a **hard lifetime** of at most `retentionEnd +
  failureRetention` (44 days at the maxima), after which all of its rows go.

### 7.2 The dead-letter view

`deadLetters(group)` lists a group's failed deliveries: envelope (with the
key), cause, code, error text (with the key), attempts, cycle, first delivery
and failure times, and expiry. The product surface keeps the word "dead
letter"; there is no separate dead-letter stream and no `deadLetter`
configuration to forget. `discard` is an explicit operator action on failed
deliveries, counted and audited.

### 7.3 Redrive and republish

- **Redrive** (default): `redrive(group, msgIds | {all, limit})` starts a new
  delivery cycle **in the failed group only**: `cycle += 1`, `attempts = 0`,
  `releases = 0`, the delivery moves to `R` as due now. With `all`, oldest
  `failedAt` first. The message id is unchanged, so domain idempotency keys
  keyed on it stay valid, and other groups that already succeeded are not run
  again. Redrive is a keyless state change.
- **Republish** (deliberate): `republish(msgIds, {to?})` reads the failed
  message with the key and sends a **new** message, with a new id,
  `lineage.republishedFrom` and the original `originId`, which every group of
  the target queue receives. The new envelope keeps the original `dedupKey`
  for the record, but republish neither checks nor writes the dedup index,
  which keeps pointing at the original id. The first release republishes only
  within the same queue or to a queue of the same project and key.
- Every redrive, republish, cancel, discard, pause, resume and purge is written
  to the project's audit journal with the credential and counts.

## 8. Retention, reclamation and accounting

### 8.1 Retention

- `retention.maxAge` per queue: default 4 days (the Cloudflare baseline),
  maximum 14 days, measured from `availableAt` (so a message scheduled ahead
  is never expired before it is due). `deliverAt` may be at most `maxAge`
  ahead of now (400 beyond it).
- **Expiry is keyless and costs no rows.** When a group's cursor meets an
  entry past `retentionEnd`, the group skips it and adds it to its
  `retentionGap {from, to, count}` (counted, reported, and alerting when
  non-zero). A message that was attempted and is retrying is never skipped
  this way: §6.3 schedules its last attempt inside retention, and its failure
  becomes a failed delivery. A group that does not consume within `maxAge`
  therefore loses the messages it did not take, counted; Cloudflare queues
  behave the same.
- A group created with `start: earliest` begins at the oldest retained
  message; with `start: now` at the queue clock.

### 8.2 Reclamation (required before any production queue)

Without reclamation every message stays in storage, counts against the
project's stored-bytes cap (#158) and is billed. Streams share shard-log and
history storage, so a queue does not own deletable objects: reclamation
deletes **rows**, and SlateDB garbage collection then frees the files.

- **Retired:** a message is retired when every group has taken or skipped it
  (each group's `C` is past it) and no group has an open delivery (`open =
  0`); or when it is cancelled queue-wide; or when its hard lifetime has
  ended. A queue keeps nothing for replay (§2).
- **What is deleted:** the message's page (in the shard log or, once absorbed,
  its history-partition row; one message per page, §5.1), its metadata row,
  its `A` entry, its key-index entry, and its dedup entry once the dedup
  window has passed (§11).
- **Cursor and refcount:** a per-queue reclamation cursor `G` walks `A` in
  order up to the minimum of the groups' cursors, deleting messages with
  `open = 0` and stepping over the rest. A message stepped over is deleted
  later by whichever operation brings its `open` to 0 (ack, discard, failure
  expiry, cancel), in that operation's own WriteBatch, because the operation
  can see the message is behind `G`. There is no list of held messages, and
  the hard lifetime bounds how long any message is held.
- **Progress and budget:** `G` is durable, so a crash resumes where it
  stopped; deletes run in bounded batches under the global trim and GC budget
  (READINESS H5 and R8), never one unbounded batch.
- **Closing TLA-019-F3 for queues.** READINESS lists physical reclamation as
  open: row deletes, history-row deletes, bounded batches, GC convergence on
  quiet partitions (H14, TLA-019-F2), fork references, and billing closes.
  For queues, Q3 builds the shard-log and history-row deletes and the bounded
  batches; forks of queues are refused (§4), so no fork can reference a
  reclaimed page; the stored-bytes decrement and the usage-ledger entry are
  written in the deleting batch, so billing closes with the delete; and Q3
  closes GC convergence or works around it (a compaction nudge after a
  reclamation batch on a quiet partition).

### 8.3 Accounting

The stored-bytes gauge counts, per queue, record bytes plus a fixed per-row
charge for every new row family (metadata, `A`, `L`, `R`, `F`, key index,
dedup, delivery rows, cancel rows, failure detail), added in the batch that
writes a row and removed in the batch that deletes it. Capacity planning uses
about 300 B of rows per message plus about 150 B per open delivery, measured
and published with Q3. Because a queue reclaims what is done, its stored bytes
follow its backlog and failures, not its throughput.

## 9. Execution: workers and wake-up

### 9.1 The worker runner (SDK)

```ts
import { StreamsClient, RetryLater } from "@prisma/streams";

const client = await StreamsClient.connect({ issuerUrl, credential });
const emails = client.queue<EmailJob>("cp-notification-channel-email");

await emails.send({ notificationId }, { dedupKey: notificationId, delay: "2m" });
await emails.sendBatch(jobs.map((j) => ({ body: j })));

const worker = emails.process(
  async (msg, { signal }) => {
    // Domain idempotency: the business id, stable across retries, redrives
    // and republishes.
    await brevo.send(msg.body, { idempotencyKey: msg.body.notificationId, signal });
    // return → ack · throw new RetryLater({ after: "30s" }) → retry
    // throw any other error → retry with backoff
    // return msg.fail("unsubscribed") · return msg.discard()
  },
  { concurrency: 10, lease: "5m", autoExtend: true },
);
await worker.stop();
```

- Claims `min(free slots, maxBatch)` with long-poll and `maxBytes`; settles as
  handlers finish, coalescing within 250 ms (a crash in that window
  redelivers: at-least-once); extends at half the lease while a handler runs.
  An extend answered `expired` or `stale` fires the handler's `AbortSignal`;
  the runner still settles when the handler returns. `stop()` stops claiming,
  waits for started handlers up to a deadline (settling them as `retry` if
  aborted), and `release`s never-started work.
- Typed messages via `client.queue<T>(name, { parse })`. A body that fails
  validation is retried with backoff by default, since a deploy window with a
  schema mismatch would otherwise dead-letter every message; `onInvalid:
  "fail"` makes it a failure with `code: invalid_payload`.
- Telemetry hooks `onMessage` and `onSettle` (id, attempt, outcome,
  duration) for OpenTelemetry or Sentry.
- A client without configuration throws at construction in production.
- Runs on Node, Bun, Deno and Prisma Compute.

### 9.2 Wake-up notifications (first release)

A sleeping process cannot claim, and a Cloudflare Worker cannot host a
long-lived loop. Push delivery of decrypted batches would need the key on the
server. Instead, a group can have a **wake-up subscription**:

1. The scheduler notices that the group has due work and no claim within the
   last `idleMs` (default 1 s).
2. It POSTs a small **signed notification** to the subscription's URL:
   `{queue, group, due, oldestDueAgeMs, nonce, sentAt}`, signed with HMAC.
   No message content, no key.
3. The receiver (a Compute app or a Worker's `fetch` handler) verifies the
   signature and the freshness of `sentAt`, then claims and processes with its
   own credential and key through the ordinary claim/settle protocol, for as
   long as its runtime allows, settling or releasing before it returns.
4. **Coalescing and recovery:** at most one notification is in flight per
   subscription; while due work remains and no claims arrive, the scheduler
   repeats with backoff (1 s → 60 s); failures are counted and visible; a
   notification is a hint, never a delivery, so losing one costs latency
   only. The `due` count lets a receiver fan out.

**Secret custody.** The signing secret is not stored: it is derived as
`HMAC(cell wake key, project ‖ subscriptionId ‖ rotation)`, returned once
when the subscription is created or rotated. The cell wake key is a new cell
secret under the existing secrets custody and rotation process.

**Egress policy.** Notifications go only to `https` URLs on port 443 whose
names resolve to public addresses: the server resolves once, pins the address
for the request, and refuses loopback, private, link-local, CGNAT, metadata
and the cell's own addresses. No redirects, a 2 s timeout, a request body of
at most 1 KiB, the response body ignored, one in flight per subscription, a
per-project notification budget, and every subscription change audited.

The SDK provides `emails.handleWakeup(request, handler, options)` for Compute
and Workers, which verifies, drains within a time budget and returns. This
gives Prisma Compute apps queue-triggered execution and lets the Console's
Worker consumers migrate in the first release. A Worker can also drain from a
cron trigger (one-minute granularity). A **kept-awake Compute worker** (the
plain runner under `KeepAwakeGuard`) is the pilot's simpler option, with its
idle cost stated.

## 10. Telemetry

- A group's `GET`: `due`, `scheduled`, `leased`, `retrying`, `failed`,
  `oldestDueAgeMs`, `oldestFailedAgeMs`, `nextAvailableAt`, `paused`,
  `retentionGap`, admission budget use, `keysBlockedByFailed` (Q6), wake-up
  subscription health, and rolling counters.
- `/operator/data.json`: per-project queue totals, worst oldest-due age,
  failed counts, retention gaps; cell-watch rules for failed growth, old due
  work approaching `maxAge` and non-zero gaps, calibrated after the pilot.
- Usage: queue operations metered as today, plus read and stored bytes.

## 11. HTTP surface

| Route | Purpose |
|---|---|
| `PUT /v1/queues/{name}` · `GET` · `DELETE` | create/update (`retention`, `failureRetention`, admission budget, dedup window) · config and stats · delete (the stream delete saga) |
| `GET /v1/queues` | list |
| `POST /v1/queues/{name}/messages` · `messages:batch` | send (batch: atomic, ≤ 1,000 messages and within the request body limit, per-message `key`, `dedupKey`, `deliverAt`) |
| `GET /v1/queues/{name}/messages/{id}` | envelope and per-group delivery state (needs the key; 404 `reclaimed` once reclaimed) |
| `PUT …/groups/{group}` · `GET` · `DELETE` | group config (`maxAttempts`, `backoff`, `leaseMs`, `maxBatch`, `maxInFlight`, `rate`, `start: earliest \| now`, wake-up subscription), updatable in place |
| `POST …/groups/{group}:claim` · `:settle` | claim; ack / retry / extend / fail / release |
| `POST …/groups/{group}:pause` · `:resume` · `:cancel` · `POST /v1/queues/{name}:cancel` | operator actions; group-scoped and queue-wide cancel |
| `GET …/groups/{group}/failed` · `POST …:redrive` · `:discard` | dead-letter view (paginated) and recovery |
| `POST /v1/queues/{name}:republish` | republish failed messages as new ones |
| `GET …/groups/{group}/pending` | peek (paginated) |

Header names use the `Streams-` form the Compute edge passes. Scopes:
`queues.send` (send), `queues.consume` (claim, settle, wake-up handler),
`queues.manage` (queue and group config, pause, cancel, redrive, republish,
discard, delete), `queues.inspect` (get, peek, dead-letter view; with the
key). Every server-side value is clamped and the clamps published: `leaseMs`
1 s to 12 h, `waitMs` ≤ 25 s, `max` ≤ 1,000, `maxAttempts` 1 to 100,
`backoff.maxMs` ≤ 12 h.

**Dedup:** a `dedupKey` seen within the queue's dedup window (default 24 h,
maximum 7 days, never above retention) returns the original id with
`duplicate: true`. The index (`dedupTag → msgId`, plus an expiry-ordered
index swept by sends) outlives a reclaimed message until its window ends,
then goes with it. Republish bypasses it.

## 12. First release, and what follows

**First release (the beta gate, if queues gate the beta):**

| Requirement | Scope |
|---|---|
| Shared engine | existing persistence, durability, authorization, tenancy |
| Delivery | unordered groups, queue clock, fenced leases, ack, retry with backoff, scheduled availability, claim byte limits |
| Recovery | durable failed deliveries, dead-letter view, group-targeted redrive, republish, discard |
| Storage | retention with gap reporting, row reclamation with GC convergence, accounting of every new row family, admission budget |
| Runtime | SDK runner, wake-up notifications with SDK handler for Compute and Workers, kept-awake pilot option |
| Operations | pause/resume, cancel, stats, audit, cell-watch rules |
| Validation | the evidence list in §13, then the low-risk pilot |

**Later:** ordered groups (Q6) and moving stream consumer groups onto the
engine; reading a queue as a stream; `limiters` shared across queues;
republish across encryption keys; an in-memory local fake
(`createLocalQueue()`); generic HTTPS push of payloads.

**How the control plane's workloads map:**

| Workload | First release? | Settings |
|---|---|---|
| Plain support sync (pilot) | yes | unordered, defaults |
| Offer and spend-limit enforcement | yes | unordered; dedup key per (workspace, hour) |
| Stripe and spend-limit usage sync | yes | `maxAttempts 11`, backoff 30 s..12 h; `rate` for Stripe; spread by `deliverAt` |
| Vercel usage sync → billing data | yes | `delay 2m`; billing data as a downstream queue |
| Notification → email | yes | email `maxInFlight`/`rate` matched to Brevo; the domain claim stays |
| ppg-events-shared | yes | external producer with `queues.send`; tolerant parse |
| Invoice submission | yes | self-continuations via `deliverAt`; the reconciler stays |
| Credential cleanup | yes | `deliverAt` per request within the horizon |
| Dunning, tenant-manager sync | after Q6 | `ordering: key` by workspace or tenant |
| Bloom turns | after Q6 | `ordering: key` by slot, `lease 15m` + auto-extend, `cancel` by id |

## 13. Verification

- **TLA+**, as two models so each stays within CI's formal budget (today's
  bound is TLA-002 at 40-50 min): **`QueueDelivery`** (queue clock with clock
  steps and shard moves, availability index, group cursor, delivery rows,
  lease, retry and failure indexes, fencing and replayed settles, bounded
  release, expiry and gaps, failure, redrive, republish, cancel queue-wide
  and per group, pause, crash/restart) and **`QueueReclamation`** (the
  reclamation cursor, refcounts, hard lifetime, deletes racing settles).
  Invariants: every sent message is, in every group, ahead of `C`, open,
  done, cancelled, in a counted gap or discarded, never lost; no send lands at
  or below a group's cursor; a stale or replayed settle changes nothing;
  reclamation never deletes a message with an open delivery or ahead of any
  group's cursor; a redrive reruns only its group; ready work is claimable
  while `maxInFlight` allows, whatever is scheduled. Ordered groups (Q6) add:
  only eligible key heads are leased.
- **Kani**: envelope, metadata-row and index codecs (big-endian order), the
  message-id order across segments, lease tokens, backoff and retention
  clamps.
- **DSTs**, including the colleague's evidence list as named scenarios:
  failed work stays recoverable after every server-held key is gone; a deep
  delayed backlog cannot hide ready work; segment rollover does not change
  delivery; redrive does not rerun successful groups; replayed settles change
  nothing; a noisy queue does not stop other tenants (shared-cell harness).
  Plus: a clock step back and forward and a shard move to a skewed host lose
  nothing; mixed-success batches; crashes mid-batch and inside the settle
  coalescing window; lease expiry with backoff; claim read failure; release
  loops; cancel queue-wide before a group exists; admission budget with a
  dead group; reclamation racing acks and redrives; wake-up coalescing,
  recovery and the egress refusals.
- **SDK**: runner and wake-up handler against the real server; Workers in CI.
- **Layer B queue arm**: queue workers, a poison-message tenant and a send
  flood under the shared-cell bars, and the per-queue throughput ceiling.
- **Pilot**: `cp-plain-support-sync` on the scratch cell for a week beside the
  old queue. Bar to proceed: every message the old queue processed is
  processed by the new one (compared by business id), none lost, failures
  match, and p99 send-to-start latency within the old queue's. Then
  enforcement, then billing.

## 14. Decisions for the owner (each with a recommendation)

| # | Decision | Recommendation |
|---|---|---|
| D1 | Resource shape | `/v1/queues/{name}` over streams of kind `queue`, reachable only through queue routes in the first release |
| D2 | Stream consumer groups | Unchanged in the first release; moved onto this engine with ordered groups (Q6), retiring the old rows by a `LAYOUT_VERSION` bump |
| D3 | Failure model | Durable failed-delivery state referencing the original record; dead-letter view; no DLQ stream |
| D4 | Failure retention | 14 days default, 30 maximum, from the first failure, not renewed by redrive |
| D5 | Redrive | Group-targeted redrive by default (same message id, new cycle); republish as a separate operation |
| D6 | Execution in the first release | SDK runner plus keyless signed wake-up notifications for Compute and Workers; kept-awake worker for the pilot |
| D7 | Retention | 4 days default, 14 maximum, from `availableAt`; untaken expired messages become counted gaps; `deliverAt` horizon = `maxAge` |
| D8 | Reclamation | Reclaim each message when every group is done with it; one message per page; required before production queues |
| D9 | Admission budget | 1 GiB / 10 million undone messages per queue by default, `429 queue_full` beyond |
| D10 | Request tier for worker fleets | A per-project queue tier set by `cell-admin`, sized by the §6.5 formula |
| D11 | Overload | A separate settle bucket at half the request rate; settles follow #184's append rules |
| D12 | Batch semantics | Atomic ≤ 1,000 messages within the body limit, per-message keys via envelope and metadata row |
| D13 | Dedup | 24 h default, 7 days maximum, never above retention; republish bypasses |
| D14 | Billing | Meter queue operations as today; no separate queue price in the beta |
| D15 | Sequencing | The first release in §12 gates the beta if queues are a beta requirement; ordered groups and the rest follow |
| D16 | Wake-up egress | Tenant-supplied HTTPS URLs under the §9.2 egress policy, with a derived signing secret |

## 15. Review responses

### 15.1 Colleague review (2026-10-23): taken

| Point | Where |
|---|---|
| Failed deliveries durable without a later keyed request | §7.1: keyless `failed` state referencing the original record; D3, D4 |
| Accepted messages stay representable after failure | §5.1, §7.1: failure adds nothing to the envelope |
| `maxPending` hides ready work | removed; group state holds only open deliveries (§5.3); separate memory cache, in-flight and admission limits (§5.4, §6.5, §6.6) |
| Segment walk stops at sealed backlog; daily rollover | queue-wide availability index, one scheduling owner per queue; rollover is storage only (§5.2) |
| Ordered heap crowded by blocked followers | eligible key heads only (§6.4) |
| Redrive reruns successful groups | group-targeted redrive, republish separate (§7.3); D5 |
| Stable execution identity | message id stable across redrive; `originId` across republish; the example uses `notificationId` (§4, §9.1) |
| A usable execution path in the first release | runner plus keyless wake-up notifications (§9.2); D6 |
| Concrete reclamation design | §8.2, §8.3; D8 |
| Replayed settles | every terminal or rescheduling operation ends the lease (§6.2) |
| Graceful shutdown refunds | only never-started work, and bounded (§6.2, §9.1) |
| Claim memory | sized from metadata and reserved before leasing (§6.1) |
| Request sizing | formula instead of a single figure (§6.5) |
| Overload | a separate settle bucket (§6.5); D11 |
| Hot-queue scaling | one owner per queue, stated ceiling to measure (§5.2) |
| Postgres and workflow boundary | §2 |
| First-release scope and evidence list | §12, §13 |
| Business case | §1 |

### 15.2 Colleague review: declined or narrowed, with reasons

- **"Exempt settles from shedding": narrowed to a separate settle bucket.**
  A full exemption would let a project send unlimited cheap settles with
  invalid tokens; a separate bucket, charged back to the main one when tokens
  are junk, keeps lease maintenance moving without opening that door, and a
  project blamed for memory pressure is still refused (§6.5).
- **Generic HTTPS push of decrypted payloads: not in the first release.** The
  review lists it as optional once a complete execution path exists; keyless
  wake-up notifications are that path and keep plaintext off the server.
- **Ordered groups: specified now, built after the first release (Q6).** The
  review suggests adding per-key ordering once its cross-segment semantics are
  specified and tested; §6.4 specifies them, and building them second keeps
  the first release's proof smaller. The pilots that need order wait for Q6
  (§12), and moving today's stream consumers (D2) moves with them.
- **"One logical scheduling owner per queue/group" was offered as an option;
  taken per queue**, because the availability index is queue-wide and every
  group reads it.

### 15.3 Fable 5.1 final review of revision 3 (2026-10-23)

Verdict "revise", with two blockers. Every finding is taken:

| Finding | Revision 4 |
|---|---|
| B1 a cursor can rise above the commit clock; a send lands below it and is lost | queue clock: claims advance `fields.ts`, the invariant `C.time ≤ fields.ts`, big-endian keys, `stream-timestamp` refused, clock section restored (§5.5) |
| B2 the deletable unit is a page, not a message | one message per page; history-row deletes, forks refused, billing closes in the deleting batch (§5.1, §8.2) |
| H1 held list unbounded or blocking | refcount `open` in the metadata row, deletion by the releasing operation, hard lifetime (§8.2, §7.1) |
| H2 wake-up secret custody and SSRF | derived secret, egress policy (§9.2); D16 |
| H3 leasing before reading | sized from metadata, reserved, leased exactly, read after commit, server release on read failure (§6.1) |
| H4 expiry rows per group fill storage and the budget | expiry as counted gaps, failures outside the budget, `start` rules (§8.1, §6.6) |
| H5 reserved quarter undefined against #162 and reverses #184 | separate settle bucket; #184's rules (§6.5); D11 |
| H6 no finder for expired leases | lease index `L`, bounded sweep, timer (§5.3, §6.1) |
| M1 release loops | bounded release (§6.2) |
| M2 late settles | `acceptedLate`, window bounded by the sweep (§6.2) |
| M3 stored-bytes admission | sends admitted against #158 (§6.6) |
| M4 cache rebuild | §5.4 |
| M5 timestamp hint | refused (§5.5) |
| M6 republish dedup, redrive order | §7.3 |
| M7 retries past retention | clamped (§6.3) |
| M8 failed heads block keys | stated, `keysBlockedByFailed` (§6.4) |
| M9 size | two TLA models (§13), estimate raised (§16) |
| M10 revision-2 content dropped | restored: clock, envelope version, body/header limits, `sendBatch`, `discard()`, pagination, hooks, cron, config throws, workload map; the local fake listed as later |
| M11 keyless error text | codes keyless, text needs the key (§6.2) |
| M12 queue-wide cancel | metadata-row flag honoured by every group (§6.7) |
| L1-L9 | figures removed or sourced; incarnation-bound ids (§4); clamps and scopes (§11); invalid payload retried by default (§9.1); Kani order proof (§13); Postgres transactions (§2); pilot bar (§13) |

The review found no colleague request dropped silently, and called the
§15.2 declines sound apart from the overload mechanism, now replaced.

### 15.4 Fable 5.1 review of revision 1 (2026-10-23)

Its findings stay answered: B1 keyless failure (the failed-delivery state);
B2 redrive through dedup (redrive appends nothing; republish bypasses dedup);
H1 horizon; H2 reclamation; H3 claim contention (cache and indexes); H4 one
lane with per-message metadata; H5 key order (§6.4); H6 expired-lease backoff;
H7 no tenant text in plaintext rows (codes only, text encrypted); and the
medium ones (segment-aware ids, cancel before taking, no DLQ recursion since
there is no DLQ stream, bounded internal rows, no envelope version, per-claim
memory and request quotas, settle races, one enqueue time, clock tolerance).

## 16. Plan and size

| Package | Contents | Estimate |
|---|---|---|
| Q1 Engine | queue clock, one-message pages, metadata rows and availability index in the append path, group cursor, delivery rows, lease/retry/failure indexes and sweep, ready cache, two-step claim with byte reservation, settle state machine with lease-ending operations and bounded release, backoff and retention clamps, `maxInFlight`, `rate`, pause, timed park wake, settle bucket | 20-28 days |
| Q2 Queue resource | `kind: queue` and the refusals, envelope, `/v1/queues` routes, scopes and clamps, dedup, scheduled messages, failed view, redrive, republish, discard, cancel (both scopes), peek, audit, admission budget | 10-14 days |
| Q3 Retention and reclamation | gap expiry, failure retention and hard lifetime, reclamation cursor and refcount deletes, shard-log and history-row deletes, bounded batches, GC convergence on quiet partitions, accounting of every row family | 15-22 days |
| Q4 Execution | SDK queue API and runner, typed parse, hooks, wake-up subscriptions (scheduler, derived secrets, egress policy, coalescing), SDK wake-up handler for Compute and Workers, Workers CI | 12-16 days |
| Q5 Verification | TLA `QueueDelivery` and `QueueReclamation`, Kani, DST suite incl. the evidence list, Layer B queue arm and ceiling, pilot | 13-20 days |
| Q6 Ordered groups (after) | key index, key-head table, eligible-head cache; stream consumers moved onto the engine; layout bump | 10-14 days |

**First release (Q1-Q5): about 70-100 engineering days**, before workload
migration. Q6 and the later items follow. Order: Q1 → Q2 → Q3, with Q4
against Q2's routes and Q5 alongside.

Costs to plan for: edits to `src/shard/transaction/append.rs` and the shard
transaction core stale formal receipts (about 150 min serial per round); new
key families go in new modules because `src/queue.rs` and
`src/product_cursor.rs` are by-path includes; routes live in
`src/product/queues/` because `src/product.rs` has no headroom; mutants run at
about 5 minutes each with a per-push cap, so each package lands in several
pushes.
