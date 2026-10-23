# Native queues in Prisma Streams — design for review

Status: **proposal, revision 5, 2026-10-23, for the owner's review before any
implementation.** Revision 1 drew on the queue usage review of 7 October 2026
and a read-only inventory of `slate` at 86d7476d. Revision 2 answered an
adversarial review by Fable 5.1. Revision 3 adopted a colleague's review.
Revision 4 folded in Fable 5.1's final review. Revision 5 adopts the
colleague's final review of revision 4, which kept the failure and scheduling
models and asked for four focused changes: reclamation that cannot race
history absorption, a group lifecycle that cannot break reclamation, expiry
that runs without consumers, and a wake-up contract that fits serverless
runtimes. §15 says what was taken from each review, what was declined, and
why. Nothing here is built. Every client-visible change becomes an edge record
when it lands.

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

| Product | Promise to the customer |
|---|---|
| **Streams** | Retained events, subscriptions and replay |
| **Queues** | Outstanding work, execution attempts, retries and failure recovery |

A queue keeps a message until its work is done or it expires, then reclaims
it (§8). It is not a log: completed message bodies are gone, and the Console
shows outstanding work, failures, counters and attempt telemetry, never the
bodies of completed messages. Workloads that need replay use a stream.

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

The public documentation keeps these three boundaries prominent.

## 3. Principles

1. **One durability boundary.** Every queue operation is acknowledged only
   after its effect is durable on object storage, in the same shard log as
   the messages (G1).
2. **At-least-once, stated plainly** (§2).
3. **Ready work is never hidden** by scheduled work, older messages, segment
   boundaries, clocks or caches.
4. **Nothing disappears silently.** Failed deliveries stay recoverable for a
   stated period; expired and discarded messages are counted and reported.
5. **The server manages delivery without the stream key.** Scheduling, expiry,
   failure and reclamation use plaintext metadata only (times, counts, sizes,
   MAC'd tags, server error codes). The key is needed only to read or write
   message bodies and error text.
6. **Time alone moves state forward.** Expiry, failure retention and
   reclamation happen on deadlines, whether or not anyone sends or claims.
7. **Every byte of new state is bounded and accounted.**

## 4. Concepts and identities

- **Queue**: a stream created with `kind: "queue"`. Its records are message
  envelopes the server writes. It has retention, failure retention and an
  admission budget, and keeps the stream's encryption, quotas and billing.
  **Retention and failure retention are fixed at creation in the first
  release** (§8.1). A queue is reached only through `/v1/queues`: stream
  reads, raw appends and forks of a queue are refused (409
  `queue_via_queue_routes`).
- **Group**: a consumer of a queue. **The first release exposes one group,
  `default`**, created with the queue. The storage model keeps a group id and
  a group generation on every group row and lease token, so named groups can
  follow (§8.4) without a format change; a lease token from an earlier
  generation of a group is stale, as `consumer_gen` makes it today
  (`src/product_cursor.rs`).
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

All queue state lives in the queue's owning **shard log**, in new key families
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
  into one page), so a message is deletable on its own. It costs about 70 B
  of page framing per message and gives up compression across messages; Q0
  measures that cost (§16). A batch is still one atomic WriteBatch.
- `enqueuedAt` is the queue clock at the send (§5.5), served with every
  message.
- The envelope must fit the 131,072 B record limit (413 otherwise, stating the
  envelope's share). Failure adds nothing to it (§7.1), so an accepted message
  never outgrows its storage.

The **metadata row**, written in the same WriteBatch as the record, holds
`{availableAt, retentionEnd, hardExpiresAt, bytes, open, cancelled, keyTag?,
dedupTag?}`: `availableAt = max(enqueuedAt, deliverAt)`; `retentionEnd =
availableAt + maxAge`; `hardExpiresAt = retentionEnd + failureRetention`
(§7.1); `bytes` the record size; `open` the number of open deliveries
(§8.2); `cancelled` a queue-wide cancel (§6.7); the tags HMACs under the
stream key. These are metadata G8 already admits (routing keys are visible
today).

### 5.2 The availability index: one scheduling order for the whole queue

Each send inserts **one entry per message** into the queue's **availability
index** `A`, keyed `(availableAt, msgId)`, in the same WriteBatch. `A` is
queue-wide, not per lineage segment, so segment rollover is a storage matter
and never a scheduling barrier. Message ids order as `(incarnation, segment,
offset)`, and a successor segment always has a higher number, so ids order
exactly as sends commit (a Kani proof in Q5). Because `maxAge` and
`failureRetention` are fixed per queue, `A`'s order is also the order of
`retentionEnd` and of `hardExpiresAt`, which the maintenance sweep relies on
(§6.8).

**One scheduling owner per queue.** A queue writes one physical lane (the
empty routing key), so one shard owns it and its indexes. The scaler cannot
split a single-key workload (`src/scaler3.rs`, hot-key detection), so a queue
has a per-queue throughput ceiling, measured over the whole send → claim →
ack → reclaim cycle in Q0 and Q5 and published (the first target is 1,000
messages a second per queue; the control plane's busiest hour averages 17.5).

### 5.3 Group state

Per group:

| State | Meaning |
|---|---|
| group row | `{generation, config, paused}` |
| group cursor `C` = `(time, msgId)` | every `A` entry at or below `C` has been taken or skipped by this group |
| delivery row per **open** delivery | `{state: leased \| retrying \| failed, cycle, attempts, releases, leaseGen, leaseDeadline, firstDeliveredAt, firstFailedAt?, failureExpiresAt?, errorCode}` |
| lease index `L` `(leaseDeadline, msgId)` | leased deliveries |
| retry index `R` `(availableAt, msgId)` | retrying deliveries |
| failure index `F` `(failedAt, msgId)` | failed deliveries, for listing |
| failure-expiry index `E` `(failureExpiresAt, msgId)` | every delivery that has ever failed, in whatever state redrive left it |
| cancel rows | group-scoped cancels of messages ahead of `C` |
| failure detail (encrypted) | the worker's error text, ≤ 1 KiB, when a settle carried text and the key (§6.2) |
| counters | delivered, acked, retried, failed, expired, hard-expired, cancelled, discarded, redriven, stale settles |

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
(send, claim, settle, sweep) writes `fields.ts = t` in its WriteBatch. Clock
reads, id allocation and cursor advances all happen in the committer's
serialized state. So:

- `fields.ts` is durable and monotone across restarts, NTP steps and shard
  moves, and `C.time ≤ fields.ts` always: a claim takes an entry only when
  `availableAt ≤ t`, and writes `t` as `fields.ts` in the same batch.
- A send commits at `enqueuedAt = t ≥ fields.ts ≥ C.time`, with an id above
  every taken id, so **a new message never sorts at or below a group's
  cursor.** The TLA model checks this with clock steps both ways and shard
  moves.
- Queue routes refuse the `stream-timestamp` hint (400), which could
  otherwise push `fields.ts` into the future.
- A host whose clock is behind sees time stand still until it catches up;
  one ahead makes scheduled messages due and leases expire early by the gap.
  Lease handoff already assumes clocks within 5 s; RUNBOOK states the same
  bound for queues.

### 5.6 Queue pages stay in the shard log

The absorber moves a stream's pages from the shard log into its history
database in three steps across two databases (read, write and flush to
history, then advance the absorbed boundary in the committer;
`src/history/gather.rs`). A message reclaimed between the read and the write
would reappear in history after its rows were gone. **Queue pages are
therefore never absorbed:** the absorber skips streams of kind `queue`, and a
message lives its whole life (at most 58 days, §7.1) in the shard log, where
every queue row and page is written and deleted by the one committer. This
already fits the architecture:

- A stream that is not absorbed keeps its ciphertext tail in the shard log
  today (SPEC §3.6, absorption needs the key).
- Shard moves reopen the shard log under manifest fencing (SPEC D14) and
  splits clone it by key range (D3), so queue pages move and split with
  their shard.
- Shard-log compaction is the keyless compactor service's (D17); it collects
  deleted pages and rows without tenant keys.

Consequences, each owned by Q1 or Q3:

- **Tail repair** (`src/shard/record.rs`, R26-4) expects pages contiguous
  over `[absorbed, next)`. Reclamation leaves gaps, so tail repair skips
  queue streams; their stored tail is derived from the queue rows instead.
- **Maintenance backpressure** (`src/backpressure.rs`) bounds unabsorbed
  bytes per shard and instance. Queue pages are excluded from that gauge;
  queue storage is bounded instead by each queue's admission budget, the
  project's stored-bytes cap, and a per-shard queue-bytes bound that Q0 sets.
- Queue pages give up the history tier's block compression (about 90%);
  Q0 measures what that costs for small messages.

## 6. Delivery

### 6.1 Claim

`claim(group, max, maxBytes, leaseMs, waitMs)` runs in two steps.

**In the committer (keyless):**

1. Take due entries (`availableAt ≤ t`) in `availableAt` order from the
   merge of `A` past `C` and of `R`, within `maxInFlight` and the group's
   `rate`, skipping cancelled and expired messages; stop at `max` or when the
   next message's metadata `bytes` would pass `maxBytes` (default 1 MiB, at
   most 8 MiB; one message is always allowed).
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
worker's releases) and answers 503, retryable.

If nothing is due and `waitMs > 0`, the claim parks until a send, a settle, a
lease deadline or the group's next `availableAt`, up to 25 s. A paused group
answers at once with `paused: true` (§6.7).

### 6.2 Settle

One settle carries any mix of operations, each by lease token. **Every
operation except `extend` ends the lease: it sets `leaseGen += 1`, so a
replayed settle with the same token is `stale` and changes nothing.**

| Operation | Effect |
|---|---|
| `ack` | delivery done: row deleted, `open -= 1` |
| `retry {delayMs?, code?, error?}` | to `retrying` in `R` (§6.3) |
| `extend {leaseMs}` | deadline to `t + leaseMs`; refused `expired` once the deadline has passed; never past `hardExpiresAt` |
| `fail {code?, error?}` | to `failed` now (§7.1) |
| `discard {code?}` | done without success: row deleted, `open -= 1`, counted `discarded`, audited with the credential |
| `release {notStarted: true}` | back to `R` due at `t + 1 s`, the attempt given back, `releases += 1`; **only for a message whose handler never started** |

- **Late settles.** `ack`, `retry`, `fail` and `discard` after the deadline
  are accepted while the sweep has not yet moved the lease (at most about a
  second), marked `acceptedLate`. Afterwards the token is stale.
- **Release is bounded.** The third consecutive release of a delivery
  without a started attempt is treated as a `retry`: the attempt counts and
  backoff applies.
- **Graceful stop.** Work whose handler started and is then aborted is
  settled as `retry` (the attempt counts): a started handler may already have
  had an external effect. Only never-started work uses `release`.
- **Error text.** A keyless settle carries only a `code` (at most 64
  characters of `[a-z0-9_.-]`, stored plaintext). Error text (≤ 1 KiB) needs
  the stream key on the settle, as a claim does; the server encrypts it.
- All settle operations need `queues.consume`. The reply lists each token's
  outcome and the group's counters.

### 6.3 Retry policy

Per group: `maxAttempts` (default 5; total attempts) and `backoff {initialMs:
1000, maxMs: 43_200_000, multiplier: 2, jitter: "full"}`.

- **Policy backoff** (no explicit delay, or an expired lease) is clamped to
  the message's `retentionEnd`, so the last policy retry happens inside
  retention.
- **An explicit delay is never shortened.** `retry.delayMs` (the SDK maps
  `retryAfterSeconds` to it) is honoured exactly; if `t + delayMs` passes
  `retentionEnd`, the delivery fails now with `cause: retention`, because the
  provider's requested next attempt cannot fit.
- When `attempts` reaches `maxAttempts`, or an attempt fails after
  `retentionEnd`, the delivery moves to `failed` (`cause: exhausted` or
  `retention`).
- Nothing outlives `hardExpiresAt` (§6.8): a retry, lease or extend that
  would is ended there.

### 6.4 Ordering

**The first release is unordered.** Deliveries are independent; nothing but
`maxInFlight` and `rate` limits concurrency. The first-release pilots are
unordered queues only; the workloads that need order (Bloom turns, dunning,
tenant-manager sync) wait for Q6.

**Ordered groups (Q6) have an open design problem**, recorded here rather than
treated as solved. Suppose `A1` is scheduled for tomorrow, `A2` has the same
key and is due now, and an unrelated `B1` is due now. `A2` must wait while
`B1` runs. If the group cursor advances past `A2`, the sparse-row model has
no durable record of a blocked, untaken message, and reclamation could
mistake it for done; keeping the cursor before `A2` instead means tracking
later messages already done. Q6 needs durable blocked state or per-key
progress, with a matching reclamation rule, before it is built. Its other
requirements stand: eligibility in id order per key, a failed head blocking
only its key (`keysBlockedByFailed`), eligible heads only in the ready cache,
and keyless messages unordered. Today's stream consumer groups
(`/v1/streams/{name}/consumers`) are untouched until then (D2).

### 6.5 Concurrency, pacing and overload

- `maxInFlight` (default 1,000, maximum 10,000).
- `rate {perSecond, burst}`, optional: a token bucket on claims, for provider
  pacing. Shared budgets across queues are a later `limiters` resource.
- **Settles and extends have their own bucket.** Today a project has one
  request bucket (`max(rate, 50)` tokens, #162). Queue settles and extends
  draw instead from a **settle bucket** at half the project's request rate
  (burst floor 25), so a send or claim flood cannot starve lease maintenance.
  A settle request in which more than half the tokens are stale or invalid is
  charged to the main bucket. Under the RSS shed (#184) settles follow the
  append rules: a blamed project's settles are refused; an unblamed
  project's are spared up to the hard line. Both are edge changes.
- **Request rate.** A worker makes about `claims + settles + extends` requests
  a second: claims ≈ throughput / batch size, settles ≈ throughput / messages
  per coalesced settle, extends ≈ in-flight / (lease / 2) for long handlers.
  Projects running worker fleets need a request tier sized by that formula
  (D10).

### 6.6 Admission budget

Each queue has an **admission budget**: at most `maxQueuedBytes` (default
1 GiB of record bytes) and `maxQueuedMessages` (default 10 million) of
retained messages not yet done. Messages held only by failed deliveries do
not count: failures have their own bound (§7.1). A send that would exceed the
budget is refused `429 queue_full`, retryable, with the budget and its use.
Sends are never silently held. A queue nobody consumes fills its budget, and
the maintenance sweep frees it as messages pass `retentionEnd` (§6.8), with
no claim or send needed. Queue sends are also admitted against the project's
stored-bytes cap (#158), which counts record bytes and the row charges of
§8.3.

### 6.7 Pause, cancel, peek

- **Pause / resume**: a paused group's claims return `paused: true` at once;
  sends continue; granted leases run out. Durable, shown in stats. The SDK
  runner backs off while paused (1 s doubling to 30 s), so a paused worker
  never polls in a tight loop.
- **Cancel** `(msgIds)`: done without processing, counted, audited. The
  metadata row's `cancelled` flag is set, every group skips the message, and
  open deliveries end (holders see `stale`). Group-scoped cancel rows come
  with named groups.
- **Peek**: lists open deliveries and the next due entries with envelope and
  state, paginated, without leasing. Needs the key; scope `queues.inspect`.

### 6.8 The maintenance sweep

Each queue has a keyless **maintenance sweep** in its owning committer. It
runs on a timer set to the earliest pending deadline among its tasks, and as
a bounded step (at most 256 entries) before each claim and settle. Its
progress is durable, so a restart or a shard move resumes it; the new owner
re-arms the timer from the stored indexes. It needs no claims, no sends and
no key. Its tasks, each in bounded batches:

1. **Lease expiry**: heads of `L` past their deadline count as a failed
   attempt (`errorCode: lease_expired`), `leaseGen += 1`, to `R` on the
   clamped policy backoff.
2. **Retention expiry**: `A` entries past `C` with `retentionEnd ≤ t` are
   skipped by `C` and added to the group's `retentionGap {from, to, count}`
   (counted, reported, alerting when non-zero). They stop counting against
   the admission budget and become reclaimable.
3. **Failure expiry**: heads of `E` with `failureExpiresAt ≤ t` are
   discarded and counted, whatever state redrive left them in.
4. **Hard expiry**: a cursor `H` walks `A` in order; any entry still present
   with `hardExpiresAt ≤ t` has every open delivery ended (leases fenced by
   `leaseGen += 1`, retries removed, failures discarded), counted
   `hardExpired`, and is retired.
5. **Dedup expiry**: heads of the dedup expiry index past their window are
   deleted.
6. **Reclamation**: retired messages are deleted (§8.2).
7. **Wake-up re-arm**: a group with due work and no recent claim is notified
   (§9.2).

## 7. Failure and recovery

### 7.1 Failed deliveries are durable state, not copies

A terminal failure moves the delivery to **`failed`**: a keyless state change
in the committer, durable like any other settle. The original encrypted
record stays where it is; nothing is copied. The failed row records the
cause, code, attempts, cycle, `firstDeliveredAt`, `firstFailedAt`,
`failedAt` and `failureExpiresAt`; the failure detail holds error text if a
keyed settle supplied it.

- Failure needs no stream key, no second append and no transfer saga. A
  delivery that fails while no worker is running and after the owning server
  restarted is still recorded and recoverable.
- Failure adds no bytes to the envelope, so a message accepted at the size
  limit can always fail.
- **Lifetime, with one authoritative rule.** `hardExpiresAt = retentionEnd +
  failureRetention` is fixed at send and wins over everything.
  `failureExpiresAt = min(firstFailedAt + failureRetention, hardExpiresAt)`
  is set at the first failure and kept through redrives. Failure retention
  defaults to 14 days (maximum 30). The longest a message can live is
  therefore 58 days from enqueue: a `deliverAt` up to 14 days ahead, 14 days
  of retention, then 30 days of failure retention.

### 7.2 The dead-letter view

`deadLetters()` lists failed deliveries: envelope (with the key), cause,
code, error text (with the key), attempts, cycle, first delivery and failure
times, and expiry. The product surface keeps the word "dead letter"; there is
no separate dead-letter stream and no `deadLetter` configuration to forget.
An operator `discard` of failed deliveries is counted and audited.

### 7.3 Redrive and republish

- **Redrive** (default): `redrive(msgIds | {all, limit})` starts a new
  delivery cycle in the failed group only: `cycle += 1`, `attempts = 0`,
  `releases = 0`, due now, oldest `failedAt` first with `all`. The message id
  is unchanged, so domain idempotency keys keyed on it stay valid, and with
  named groups, groups that already succeeded are not run again. Redrive is a
  keyless state change and does not extend `failureExpiresAt` or
  `hardExpiresAt`.
- **Republish** (deliberate): `republish(msgIds, {to?})` reads the failed
  message with the key and sends a **new** message, with a new id and its own
  lifetime, `lineage.republishedFrom` and the original `originId`. The new
  envelope keeps the original `dedupKey` for the record, but republish neither
  checks nor writes the dedup index. The first release republishes only
  within the same queue or to a queue of the same project and key.
- Every redrive, republish, cancel, discard, pause, resume and purge is written
  to the project's audit journal with the credential and counts.

## 8. Retention, reclamation and accounting

### 8.1 Retention

- `retention.maxAge`: default 4 days (the Cloudflare baseline), maximum 14
  days, measured from `availableAt` (a scheduled message is never expired
  before it is due). `deliverAt` may be at most `maxAge` ahead (400 beyond
  it). `maxAge` and `failureRetention` are fixed when the queue is created, so
  the availability order is the expiry order; changing them later (retention
  epochs or an expiry index) is later work.
- Expiry is keyless and costs no rows per message (§6.8). A message that was
  attempted and is retrying is not expired by retention: §6.3 schedules its
  last policy attempt inside retention, and its failure becomes a failed
  delivery. A queue nobody consumes within `maxAge` loses the messages it did
  not take, counted; Cloudflare queues behave the same.

### 8.2 Reclamation (required before any production queue)

Reclamation deletes rows and pages in the shard log (§5.6); the shard-log
compactor then frees the files. It separates two events:

- **Logical retirement** (accounting): a message is retired when every
  group has taken or skipped it and `open = 0`; or when it is cancelled; or
  when its hard lifetime ends. The retiring batch removes its bytes and row
  charges from the stored-bytes gauge and the admission budget, writes the
  usage-ledger entry, and adds it to a retired index `X`. Billing stops here.
- **Physical deletion** (cleanup): the sweep deletes, for each entry of `X`,
  the page, the metadata row, the `A` entry, the key-index entry and its `X`
  entry, in bounded batches under the global trim budget (READINESS H5 and
  R8). Deletes are idempotent, so a crash in between repeats them safely. The
  dedup entry stays until its window ends (§11).

**Finding retired messages.** A reclamation cursor `G` walks `A` up to the
minimum group cursor, retiring messages with `open = 0` and stepping over the
rest. A message stepped over is retired by whichever operation later brings
its `open` to 0 (ack, discard, failure expiry, cancel, hard expiry), in that
operation's own batch. The hard-expiry cursor `H` (§6.8) guarantees no
message stays behind `G` for longer than its lifetime. This is sound because
no group can start behind `G` (§8.4).

**READINESS's open reclamation items (TLA-019-F3), for queues:** no history
rows exist to delete (§5.6); forks of queues are refused (§4); billing closes
at retirement; deletes are bounded and resumable; and Q0 must show that the
compactor collects deleted queue pages on a quiet shard, the shard-log
counterpart of H14 ("GC convergence without further writes").

### 8.3 Accounting

The stored-bytes gauge counts, per queue, record bytes plus a fixed per-row
charge for every row family (metadata, `A`, `L`, `R`, `F`, `E`, `X`, key
index, dedup, delivery rows, failure detail), added in the batch that writes a
row and removed at retirement or deletion. The planning figures (about 70 B
of page framing and 300 B of rows per message, 150 B per open delivery) are
estimates until Q0 measures them. Because a queue reclaims what is done, its
stored bytes follow its backlog and failures, not its throughput.

### 8.4 Named groups (later)

Named groups follow the first release, with these rules so they cannot break
reclamation:

- A group is created at a cursor no earlier than `G`, atomically with
  reading `G` in the committer: `start: earliest` means `G`, and `start: now`
  means the queue clock, so it includes messages already sent but scheduled
  for later (the cursor follows availability, not enqueue time). A group
  never sees a message reclamation may already have stepped over.
- Retirement counts the groups that exist at the moment of the final
  release, and creating or deleting a group is serialized with it in the
  committer.
- Deleting a group bumps its generation (old tokens stale) and releases its
  open references through the sweep in bounded, resumable steps.
- Group-scoped cancel rows and group-targeted redrive come with them.

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
  An extend answered `expired` or `stale` fires the handler's `AbortSignal`.
  `stop()` stops claiming, waits for started handlers up to a deadline
  (settling them as `retry` if aborted), and `release`s never-started work.
- `msg.discard()` is the `discard` settle operation (§6.2).
- Typed messages via `client.queue<T>(name, { parse })`. A body that fails
  validation is retried with backoff by default, since a deploy window with a
  schema mismatch would otherwise dead-letter every message; `onInvalid:
  "fail"` makes it a failure with `code: invalid_payload`.
- Backs off while the group is paused (§6.7). Telemetry hooks `onMessage` and
  `onSettle`. A client without configuration throws at construction in
  production. Runs on Node, Bun, Deno and Prisma Compute.

### 9.2 Wake-up notifications (first release)

A sleeping process cannot claim, and a Cloudflare Worker cannot host a
long-lived loop. Push delivery of decrypted batches would need the key on the
server. Instead, a group can have a **wake-up subscription**: a small signed
notification that tells a receiver to come and claim.

**The notification.** The sweep sends `{queue, group, due, oldestDueAgeMs,
nonce, sentAt}`, HMAC-signed, no message content, no key. The 2 s limit is for
the receiver's acknowledgement only: connect, verify, answer `202`. It is not
the lifetime of the work.

**The receiver contract (fast acknowledgement, bounded drain):**

1. Verify the signature and the freshness of `sentAt`.
2. Register the drain with the runtime's background-execution mechanism
   (`waitUntil` on Prisma Compute and on Cloudflare Workers).
3. Answer `202` at once.
4. Drain: claim and process with its own credential and key through the
   ordinary claim/settle protocol, within a **drain budget**; stop claiming
   when the time left is less than the lease's settle margin, settle what
   finished and `release` what never started.
5. Return. If work remains, the scheduler wakes it again.

Background execution is best-effort across restarts and deploys; leases and
retries stay responsible for durability. The SDK's
`emails.handleWakeup(request, ctx, handler, { drainBudget })` implements the
contract. Its default budget fits the runtime: on Workers it stays under the
background-execution limit after a response (about 30 s, per Cloudflare's
documentation), on Compute it is configurable.

**Re-arming.** The sweep, not the receiver, owns progress: while a group has
due work and no claim within `idleMs` (default 1 s) of the last
notification's acknowledgement or the last claim, it notifies again, with
backoff (1 s → 60 s) while notifications fail. The state it reads (due work,
time of the last claim) is durable or derived from durable indexes, so a
restart or shard move re-arms it. A lost notification costs latency only.
Coalescing: at most one notification in flight per subscription; the `due`
count lets a receiver fan out.

**Secret custody.** The signing secret is not stored: it is
`HMAC(cell wake key, version k, project ‖ subscriptionId ‖ rotation)`,
returned once at creation or rotation, and the subscription records `k`.
Rotating the cell key creates version `k+1` for new subscriptions and
rotations; older versions stay until every subscription using them has
rotated, and during a subscription's rotation the receiver accepts both
secrets for up to 7 days. The cell wake key is a new cell secret under the
existing custody and rotation process.

**Egress policy.** Notifications go only to `https` URLs on port 443 whose
names resolve to public addresses: the server resolves once, pins the address
for the request, and refuses loopback, private, link-local, CGNAT, metadata
and the cell's own addresses. No redirects, a request body of at most 1 KiB,
the response body ignored, a per-project notification budget, and every
subscription change audited.

**Which consumers this serves.** Prisma Compute apps get queue-triggered
execution. The Console's Cloudflare Worker consumers migrate in the first
release **only where their handlers finish inside a Worker's drain budget**;
longer handlers run in a Compute worker or wait for a longer-lived drain. A
Worker can also drain from a cron trigger (one-minute granularity). A
**kept-awake Compute worker** (the runner under `KeepAwakeGuard`) is the
pilot's simplest option, with its idle cost stated.

## 10. Telemetry

- The group's `GET`: `due`, `scheduled`, `leased`, `retrying`, `failed`,
  `oldestDueAgeMs`, `oldestFailedAgeMs`, `nextAvailableAt`, `paused`,
  `retentionGap`, hard-expired count, admission budget use, wake-up
  subscription health (last notification, failures, last claim), and rolling
  counters.
- `/operator/data.json`: per-project queue totals, worst oldest-due age,
  failed counts, retention gaps, per-shard queue bytes, sweep lag; cell-watch
  rules for failed growth, due work approaching `maxAge`, non-zero gaps and
  sweep lag, calibrated after the pilot.
- Usage: queue operations metered as today, plus read and stored bytes.

## 11. HTTP surface

| Route | Purpose |
|---|---|
| `PUT /v1/queues/{name}` · `GET` · `DELETE` | create (`retention`, `failureRetention` fixed; admission budget and dedup window updatable) · config and stats · delete (the stream delete saga) |
| `GET /v1/queues` | list |
| `POST /v1/queues/{name}/messages` · `messages:batch` | send (batch: atomic, ≤ 1,000 messages within the request body limit, per-message `key`, `dedupKey`, `deliverAt`) |
| `GET /v1/queues/{name}/messages/{id}` | envelope and delivery state (needs the key; 404 `reclaimed` once reclaimed) |
| `PUT …/groups/default` · `GET` | group config (`maxAttempts`, `backoff`, `leaseMs`, `maxBatch`, `maxInFlight`, `rate`, wake-up subscription), updatable in place |
| `POST …/groups/default:claim` · `:settle` | claim; ack / retry / extend / fail / discard / release |
| `POST …/groups/default:pause` · `:resume` · `POST /v1/queues/{name}:cancel` | operator actions |
| `GET …/groups/default/failed` · `POST …:redrive` · `:discard` | dead-letter view (paginated) and recovery |
| `POST /v1/queues/{name}:republish` | republish failed messages as new ones |
| `GET …/groups/default/pending` | peek (paginated) |

Any group name but `default` is 404 `group_not_found` in the first release.
Header names use the `Streams-` form the Compute edge passes. Scopes:
`queues.send` (send), `queues.consume` (claim, settle including `discard`,
wake-up handler), `queues.manage` (queue and group config, pause, cancel,
redrive, republish, operator discard, delete), `queues.inspect` (get, peek,
dead-letter view; with the key). Every server-side value is clamped and the
clamps published: `leaseMs` 1 s to 12 h, `waitMs` ≤ 25 s, `max` ≤ 1,000,
`maxAttempts` 1 to 100, `backoff.maxMs` ≤ 12 h.

**Dedup:** a `dedupKey` seen within the dedup window (default 24 h, maximum
7 days, never above retention) returns the original id with `duplicate:
true`. The index (`dedupTag → msgId`, plus an expiry-ordered index the sweep
drains, §6.8) outlives a reclaimed message until its window ends. Republish
bypasses it.

## 12. First release, and what follows

**First release (the beta gate, if queues gate the beta):**

| Requirement | Scope |
|---|---|
| Feasibility | Q0's storage measurement passes before Q1 starts (§16) |
| Shared engine | existing persistence, durability, authorization, tenancy; queue pages kept in the shard log |
| Delivery | the `default` group, unordered, queue clock, fenced leases, ack, retry with backoff, scheduled availability, claim byte limits |
| Recovery | durable failed deliveries, dead-letter view, redrive, republish, discard |
| Storage | fixed retention, the maintenance sweep, retirement and bounded deletion, accounting of every row family, admission budget |
| Runtime | SDK runner, wake-up notifications with the fast-acknowledgement contract and SDK handler, kept-awake pilot option |
| Operations | pause/resume, cancel, stats, audit, cell-watch rules |
| Validation | the evidence list in §13, then the low-risk pilot |

**Later:** named groups (§8.4); ordered groups, once the §6.4 problem is
designed (Q6), and moving stream consumer groups onto the engine; live
observation or export of a queue's traffic (not replay: reclaimed messages are
gone), or an explicit history-retention feature; updatable retention; shared
`limiters`; republish across encryption keys; an in-memory local fake;
generic HTTPS push of payloads.

**How the control plane's workloads map:**

| Workload | First release? | Settings |
|---|---|---|
| Plain support sync (pilot) | yes | defaults |
| Offer and spend-limit enforcement | yes | dedup key per (workspace, hour) |
| Stripe and spend-limit usage sync | yes | `maxAttempts 11`, backoff 30 s..12 h; `rate` for Stripe; spread by `deliverAt` |
| Vercel usage sync → billing data | yes | `delay 2m`; billing data as a downstream queue |
| Notification → email | yes | `maxInFlight`/`rate` matched to Brevo; the domain claim stays |
| ppg-events-shared | yes | external producer with `queues.send`; tolerant parse |
| Invoice submission | yes | self-continuations via `deliverAt`; the reconciler stays |
| Credential cleanup | yes | `deliverAt` per request within the horizon |
| Console Worker consumers | where handlers fit the Worker drain budget | wake-up handler; others on a Compute worker |
| Dunning, tenant-manager sync, Bloom turns | after Q6 | ordered by workspace, tenant or slot |

## 13. Verification

- **TLA+**, as two models so each stays within CI's formal budget (today's
  bound is TLA-002 at 40-50 min). **`QueueDelivery`**: queue clock with clock
  steps and shard moves, availability index, group cursor, delivery rows,
  lease, retry, failure and expiry indexes, fencing and replayed settles,
  bounded release, the sweep's expiry tasks, failure, redrive, republish,
  cancel, pause, crash/restart. **`QueueReclamation`**: retirement and
  physical deletion as separate steps, the reclamation and hard-expiry
  cursors, refcounts, deletion racing settles and the sweep, restart between
  retirement and deletion, and group creation (for §8.4). Invariants: every
  sent message is ahead of `C`, open, done, cancelled, in a counted gap or
  discarded, never lost; no send lands at or below a group's cursor; a stale
  or replayed settle changes nothing; nothing outlives `hardExpiresAt`;
  reclamation never deletes a message with an open delivery or ahead of any
  group's cursor, and every retired message is eventually deleted; accounting
  is removed exactly once; ready work is claimable while `maxInFlight`
  allows, whatever is scheduled.
- **Kani**: envelope, metadata-row and index codecs (big-endian order), the
  message-id order across segments, lease tokens with group generation,
  backoff, retention and lifetime arithmetic.
- **DSTs**, including the colleague's evidence lists as named scenarios:
  failed work stays recoverable after every server-held key is gone; a deep
  delayed backlog cannot hide ready work; segment rollover does not change
  delivery; replayed settles change nothing; a noisy queue does not stop other
  tenants (shared-cell harness); **a queue nobody consumes frees its budget
  after `maxAge` with no claim or send; hard expiry fences a lease extended
  for a month; an explicit retry past retention fails with `cause:
  retention`; dedup rows expire on a quiet queue; a receiver that drains,
  leaves due work and sees no traffic is woken again, also after a restart
  and a shard move; the absorber never copies a queue page; restart tail
  repair passes over a reclaimed queue.** Plus clock steps and skewed moves,
  mixed-success batches, crashes mid-batch and in the settle coalescing
  window, claim read failure, release loops, admission budget, and the egress
  refusals.
- **SDK**: runner and wake-up handler against the real server; Workers in CI,
  including a drain that outlasts the acknowledgement.
- **Layer B queue arm**: queue workers, a poison-message tenant and a send
  flood under the shared-cell bars, and the per-queue ceiling.
- **Pilot**: `cp-plain-support-sync` on the scratch cell for a week beside the
  old queue. Bar to proceed: every message the old queue processed is
  processed by the new one (compared by business id), none lost, failures
  match, and p99 send-to-start latency within the old queue's. Then
  enforcement, then billing.

## 14. Decisions for the owner (each with a recommendation)

| # | Decision | Recommendation |
|---|---|---|
| D1 | Resource shape | `/v1/queues/{name}` over streams of kind `queue`, reachable only through queue routes |
| D2 | Stream consumer groups | Unchanged until ordered groups (Q6) are designed and built |
| D3 | Failure model | Durable failed-delivery state referencing the original record; dead-letter view; no DLQ stream |
| D4 | Lifetime | `hardExpiresAt = retentionEnd + failureRetention` wins; failure retention 14 days default, 30 maximum, from the first failure; at most 58 days from enqueue |
| D5 | Redrive | Same message id, new cycle, the failed group only; republish as a separate operation |
| D6 | Execution in the first release | SDK runner plus wake-up notifications under the fast-acknowledgement, bounded-drain contract; kept-awake worker for the pilot |
| D7 | Retention | 4 days default, 14 maximum, from `availableAt`, fixed at creation; untaken expired messages become counted gaps |
| D8 | Reclamation | Retirement (accounting) then bounded deletion; one message per page; required before production queues |
| D9 | Admission budget | 1 GiB / 10 million undone messages per queue by default, `429 queue_full` beyond |
| D10 | Request tier for worker fleets | A per-project queue tier set by `cell-admin`, sized by the §6.5 formula |
| D11 | Overload | A separate settle bucket at half the request rate; settles follow #184's append rules |
| D12 | Batch semantics | Atomic ≤ 1,000 messages within the body limit |
| D13 | Dedup | 24 h default, 7 days maximum, never above retention; republish bypasses |
| D14 | Billing | Meter queue operations as today; no separate queue price in the beta |
| D15 | Sequencing | Q0 first; the first release in §12 gates the beta if queues are a beta requirement |
| D16 | Wake-up egress | Tenant-supplied HTTPS URLs under the §9.2 egress policy, with a versioned derived secret |
| D17 | Queue storage tier | Queue pages stay in the shard log and are never absorbed into history |
| D18 | Groups | Only `default` in the first release; named groups later under §8.4 |

## 15. Review responses

### 15.1 Colleague's final review of revision 4 (2026-10-23): all taken

The review approved the product direction and kept the failure, scheduling
and redrive models. Its code claims hold against the tree (`gather.rs`'s
three-step absorption, `record.rs`'s contiguous tail repair, `consumer_gen`).

| Point | Revision 5 |
|---|---|
| Blocker: reclamation races history absorption | queue pages never absorbed (§5.6); retirement and deletion separate (§8.2); tail repair skips queues; D17 |
| Blocker: a group created behind `G` loses messages | `default` only (D18); later groups start at `G` or later, with lifecycle rules (§8.4); group generation in tokens (§4) |
| Expiry must progress without consumers | the maintenance sweep (§6.8) |
| One authoritative lifetime | `hardExpiresAt` and `failureExpiresAt` (§7.1); 58 days from enqueue |
| Updatable retention breaks expiry order | retention fixed at creation (§8.1) |
| Dedup cleanup on quiet queues | the sweep (§6.8) |
| Do not shorten explicit retry delays | §6.3 |
| The 2 s wake request cannot own the drain | fast acknowledgement and bounded drain (§9.2) |
| Explicit re-arming | owned by the sweep, durable state, tested after restart and move (§9.2, §13) |
| Worker migration conditional on handler durations | §9.2, §12 |
| Product boundary; no completed bodies in the Console; stream reading as observation | §2, §12 |
| `start: now` semantics | stated by availability (§8.4) |
| Secret rotation pinned to a root-key version | §9.2 |
| Paused runner backoff | §6.7, §9.1 |
| `discard` wire operation | §6.2 |
| Q6 blocked-but-untaken state | recorded as an open problem (§6.4) |
| Storage feasibility before Q1 | Q0 (§16) |

### 15.2 Colleague's review of revision 2: taken, and declined or narrowed

Taken: durable keyless failure (§7.1); the scheduler with no `maxPending`
(§5.2, §5.3) and separate memory, in-flight and admission limits; eligible
key heads (now part of the open Q6 design); group-targeted redrive and
republish (§7.3); stable identity (§4); a first-release execution path
(§9.2); concrete reclamation (§8); lease-ending settles (§6.2); refunds only
for never-started work (§6.2); claim byte limits (§6.1); a request formula
(§6.5); the per-queue ceiling (§5.2); the Postgres boundary (§2); the
first-release scope and evidence list (§12, §13); the business case (§1).

Declined or narrowed, with reasons:

- **"Exempt settles from shedding": narrowed to a separate settle bucket.**
  A full exemption would let a project send unlimited cheap settles with
  invalid tokens; a separate bucket, charged back when tokens are junk, keeps
  lease maintenance moving, and a project blamed for memory pressure is still
  refused (§6.5).
- **Generic HTTPS push of decrypted payloads: not in the first release.**
  Keyless wake-up notifications are the execution path and keep plaintext off
  the server.
- **Ordered groups: after the first release (Q6).** The final review showed
  they need more design (§6.4), which confirms building them second.
- **One scheduling owner: per queue**, because the availability index is
  queue-wide.

### 15.3 Fable 5.1 reviews

The final review of revision 3 (verdict "revise") is answered in revision 4
and still holds: the queue clock (B1, §5.5); one message per page (B2, §5.1);
refcount reclamation with a hard lifetime (H1); derived secret and egress
policy (H2); claims sized from metadata and leased exactly (H3); expiry as
counted gaps (H4); the settle bucket (H5); the lease index and sweep (H6);
and M1-M12, L1-L9 (bounded release, late settles, #158 admission, cache
rebuild, timestamp hint, republish dedup, retry clamps, failed key heads, two
TLA models, restored revision-2 content, keyed error text, queue-wide
cancel). Revision 5 replaces two of those answers: history-row deletes are
gone because queue pages are never absorbed, and the "held" tracking is now
the hard-expiry cursor. The review of revision 1 stays answered as recorded
in revision 4.

## 16. Plan and size

| Package | Contents | Estimate |
|---|---|---|
| Q0 Storage feasibility | an isolated experiment revision (never production): one-message pages kept in the shard log, metadata and index rows, retirement and bounded deletion, at the target rates beside an ordinary Streams tenant. It measures stored bytes after compaction, write and delete amplification (SSTs and PUTs a second), compactor collection on a quiet shard, memory, p50/p99 latency, cleanup of a failed backlog, and the per-shard queue-bytes bound, and it sets the per-queue ceiling. If one-message pages or shard-log residency cost too much, revise §5 before Q1 (for example multi-message pages with a page-level refcount) | 4-6 days |
| Q1 Engine | queue clock, one-message pages kept from absorption, tail repair and backpressure exclusions, metadata rows and availability index, group cursor, delivery rows, the lease/retry/failure/expiry indexes, ready cache, two-step claim, settle state machine, backoff and retention rules, `maxInFlight`, `rate`, pause, settle bucket | 20-28 days |
| Q2 Queue resource | `kind: queue` and the refusals, envelope, `/v1/queues` routes for the `default` group, scopes and clamps, dedup, scheduled messages, failed view, redrive, republish, discard, cancel, peek, audit, admission budget | 9-13 days |
| Q3 Sweep and reclamation | the maintenance sweep (all seven tasks), gap reporting, lifetimes, retirement and deletion, accounting of every row family, compactor collection | 13-20 days |
| Q4 Execution | SDK queue API and runner, typed parse, hooks, wake-up subscriptions (versioned derived secrets, egress policy, re-arming), the SDK wake-up handler with bounded drain for Compute and Workers, Workers CI | 12-16 days |
| Q5 Verification | TLA `QueueDelivery` and `QueueReclamation`, Kani, DSTs incl. both evidence lists, Layer B queue arm and ceiling, pilot | 13-20 days |
| Q6 Ordered groups (after) | design the blocked-state problem (§6.4), then build; named groups (§8.4); stream consumers onto the engine | to be estimated after design |

**First release (Q0-Q5): about 71-103 engineering days**, before workload
migration. Q0 comes first, and Q1 starts only when its numbers are in. Then
Q1 → Q2 → Q3, with Q4 against Q2's routes and Q5 alongside.

Costs to plan for: edits to `src/shard/transaction/append.rs`, the absorber
and the shard transaction core stale formal receipts (about 150 min serial
per round); new key families go in new modules because `src/queue.rs` and
`src/product_cursor.rs` are by-path includes; routes live in
`src/product/queues/` because `src/product.rs` has no headroom; mutants run at
about 5 minutes each with a per-push cap, so each package lands in several
pushes.
