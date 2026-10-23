# Native queues in Prisma Streams — design for review

Status: **proposal, revision 6, 2026-10-23, for the owner's review before any
implementation.** Revisions 1-5 built the delivery, failure and lifetime
models through two Fable 5.1 reviews and two colleague reviews. Fable's
review of revision 5 found the storage plan unsound: "never absorbed" is a
state six subsystems act on, and the shard-log compactor service it relied
on does not exist. The owner chose to keep queue data in the shard database
with an explicit collection mechanism (option (a)), and set its requirements:
a persisted queue storage kind, durable reclamation debt, compaction
scheduled by amount and age, and verified physical garbage collection on a
quiet shard, all proven by Q0 before Q1 starts. Revision 6 is that design.
§15 maps every review point. Nothing here is built. Every client-visible
change becomes an edge record when it lands.

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
and schedules reliable work under one operating model. Any later pricing
claim needs Q0's cost per million completed messages, including reclamation.

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
  transaction is not part of it. Applications that need both keep a
  transactional outbox or a reconciler. The control plane's usage outbox,
  invoice reconciler and sweeps stay.
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
   after its effect is durable, and a queue transition updates payload,
   delivery state, indexes, retirement and accounting in one WriteBatch of
   the shard database (G1).
2. **At-least-once, stated plainly** (§2).
3. **Ready work is never hidden** by scheduled work, older messages, segment
   boundaries, clocks or caches.
4. **Nothing disappears silently.** Failed deliveries stay recoverable for a
   stated period; expired and discarded messages are counted and reported.
5. **The server manages delivery without the stream key.** Scheduling, expiry,
   failure and reclamation use plaintext metadata only. The key is needed only
   to read or write message bodies and error text.
6. **Time alone moves state forward.** Expiry, failure retention, deletion and
   physical collection complete on deadlines, whether or not anyone sends or
   claims.
7. **Every byte of new state is bounded and accounted**, and live bytes, bytes
   awaiting reclamation and physical storage are kept apart.

## 4. Concepts and identities

- **Queue**: a stream created with `kind: "queue"`, stored in **queue storage
  mode** (§5.6). Its records are message envelopes the server writes. It has
  retention, failure retention and an admission budget, and keeps the
  stream's encryption, quotas and billing. Retention and failure retention are
  fixed at creation in the first release (§8.1). A queue is reached only
  through `/v1/queues`: stream reads, raw appends and forks of a queue are
  refused (409 `queue_via_queue_routes`). (The name does not relate to the
  existing "queue profile" of consumer groups in `src/queue.rs`; Q1 renames
  internal identifiers where the two would meet.)
- **Group**: a consumer of a queue. **The first release exposes one group,
  `default`**, created with the queue. Group rows and lease tokens carry a
  group id and generation, so named groups can follow (§8.5) without a format
  change; a token from an earlier generation is stale, as `consumer_gen` makes
  it today (`src/product_cursor.rs`).
- **Message id** `msg_<base32(incarnation, segment, offset)>`: stable across
  every retry and redrive. Every route checks the id's incarnation (404
  otherwise).
- **Delivery**: one message's state in one group, id `(group, msgId, cycle)`;
  `cycle` rises by one on each redrive. A lease has a lease generation that
  fences stale workers.
- **Origin id** `originId`: the message id a republish chain started from
  (§7.3). Applications use their own business id (for example
  `notificationId`) or `originId` as the provider idempotency key.

## 5. Storage model

All queue state lives in the owning **shard database**, in new key families
under the stream's storage hash, written by the shard committer and durable on
the same watermark as appends. Index keys are big-endian, so key order is
numeric order.

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

- `body` is JSON, or `bodyBase64` for bytes; `headers` at most 16 entries and
  4 KiB, strings only. No version field: `LAYOUT_VERSION` versions it.
- **One message, one page** (`src/crypto_page.rs` otherwise packs up to 4,096
  records of one append), so a message is deletable on its own: about 70-90 B
  of page framing and no cross-message compression, measured in Q0. A batch
  is still one atomic WriteBatch.
- The envelope must fit the 131,072 B record limit (413 otherwise). Failure
  adds nothing to it (§7.1).

The **metadata row**, in the same WriteBatch, holds `{availableAt,
retentionEnd, hardExpiresAt, bytes, open, state: live | retired, cancelled,
keyTag?, dedupTag?}`: `availableAt = max(enqueuedAt, deliverAt)`;
`retentionEnd = availableAt + maxAge`; `hardExpiresAt = retentionEnd +
failureRetention`; `open` the count of open deliveries; `state` the monotone
retirement bit (§8.2); the tags HMACs under the stream key (metadata G8
already admits).

### 5.2 The availability index

Each send inserts one entry per message into the queue-wide **availability
index** `A`, keyed `(availableAt, msgId)`, in the same WriteBatch. Segment
rollover is a storage matter, never a scheduling barrier. Ids order as
`(incarnation, segment, offset)`, so they order as sends commit (a Kani proof
in Q5). With `maxAge` and `failureRetention` fixed per queue, `A`'s order is
also the order of `retentionEnd` and `hardExpiresAt`.

**One scheduling owner per queue.** A queue writes one physical lane, so one
shard owns it. The scaler cannot split a single-key workload
(`src/scaler3.rs`), so a queue has a per-queue ceiling, measured over the
whole send → claim → ack → reclaim cycle in Q0 and published (first target:
1,000 messages a second per queue; the control plane's busiest hour averages
17.5).

### 5.3 Group state

| State | Meaning |
|---|---|
| group row | `{generation, config, paused, lastClaimAt, lastNotifiedAt, notifyBackoffMs}` |
| group cursor `C` = `(time, msgId)` | every `A` entry at or below `C` has been taken or skipped |
| delivery row per **open** delivery | `{state: leased \| retrying \| failed, cycle, attempts, releases, leaseGen, leaseDeadline, firstDeliveredAt, failureExpiresAt?, errorCode}` |
| lease index `L` `(leaseDeadline, msgId)` | leased deliveries |
| retry index `R` `(availableAt, msgId)` | retrying deliveries |
| failure index `F` `(failedAt, msgId)` | failed deliveries, for listing |
| failure-expiry index `E` `(failureExpiresAt, msgId)` | failed deliveries (§7.1) |
| failure detail (encrypted) | the worker's error text, ≤ 1 KiB, when a settle carried text and the key |
| counters | delivered, acked, retried, failed, expired, hard-expired, cancelled, discarded, redriven, stale settles |

A delivery with no row is ahead of `C` or done. Group state is proportional
to work in flight, in retry and in failure, not to the backlog. There is no
`maxPending`.

### 5.4 Bounded memory

The committer keeps, per active group, a **ready cache** of at most
`maxInFlight + 1,024` due entries, refilled from `A` past `C` and from `R`.
It is never the truth: it is refilled from the indexes when dry and rebuilt
after a restart, an ownership change or a refused commit. It is charged to the
project's memory line (about 100 B an entry).

### 5.5 The queue clock

All queue times come from one **queue clock**: `t = max(now_ms(), q.clock)`,
where `q.clock` lives in the queue's **state row** and is written in every
queue batch that reads it (send, claim, settle, sweep). It is not kept in
`fields.ts`: the tail row is persisted only when its listed fields change, and
`ts` is not among them (`src/shard/transaction/finalize.rs:91-98`). Clock
reads, id allocation and cursor advances happen in the committer's serialized
state. So:

- `q.clock` is durable and monotone across restarts, NTP steps and shard
  moves, and `C.time ≤ q.clock` always.
- A send commits at `enqueuedAt = t ≥ C.time`, with an id above every taken
  id, so **a new message never sorts at or below a group's cursor** (checked
  by TLA with clock steps and moves).
- Queue routes refuse the `stream-timestamp` hint (400).
- Clocks are assumed within 5 s, as lease handoff already assumes; RUNBOOK
  states the bound.

### 5.6 Queue storage mode

A queue's tail carries a persisted **queue storage kind**: a tail flag bit.
Today's decoder refuses unknown flag bits (`src/shard.rs:125`), so the first
binary that writes it is a one-way upgrade for any cell that has created a
queue (decision D19). Queue mode keeps queue pages in the shard database for
their whole life (at most 58 days, §7.1), never absorbed into history: the
absorber copies across two databases (read, write and flush to history, then
advance the boundary; `src/history/gather.rs`), which would race reclamation.

"Not absorbed" is a ledger state many subsystems read, so queue mode is
defined by **invariants with guarded consumers**, not by a single field:

1. **Queue pages create no absorption debt.** Queue appends add nothing to
   `unabsorbed_bytes`, and a queue tail keeps `absorbed = next` as a
   projection, so a consumer that reads `absorbed < next` sees nothing.
2. **Queue writes and retirements report no absorber progress.** The
   maintenance ledger moves `last_progress_ms` when frame bytes retire
   (`src/shard/maintenance_row.rs:70`); queue frames never enter that path,
   so queue traffic cannot make a stalled ordinary absorber look healthy.
3. **Queue storage has its own accounting**: live bytes and reclamation debt
   (§8.3), not the absorption gauge.
4. **Ordinary history reads, contiguous-tail repair and stream trimming never
   touch queue pages.** The read path treats `absorbed` as the boundary
   history serves below (`src/application/read.rs`); queue routes read pages
   only by point get (§6.1).
5. **Queue retirement is the only authority that deletes queue pages.**

Each consumer of the absorption state is guarded by the kind and covered by a
named test (§13):

| Consumer | Queue-mode behaviour |
|---|---|
| dirty index (`src/shard/transaction/finalize.rs:100-112`) | a queue tail is never marked dirty for absorption |
| absorber roster and due list (`src/history/gather.rs:252-258`, `src/history.rs:625-645`, `src/history/worker.rs:226-290`) | queues never enter the roster; a tick gathers nothing for them |
| maintenance ledger and stall latch (`src/shard.rs:388-420`, `maintenance_row.rs`, `src/backpressure.rs:150-180,343-350`, cell-watch lag rule) | queue bytes are excluded from unabsorbed bytes and from progress |
| restart tail repair (`src/shard.rs:401-405`, `src/shard/record.rs:258-291`) | a queue tail is never a repair candidate |
| frame reconciliation (`finalize.rs:80-88`) | queue deletes are counted on the queue's own ledger side |
| billing residency (`src/billing.rs:1678-1686`) and idle release (`src/bootstrap/idle.rs:30`) | read queue reclamation debt (§8.2) instead of absorption debt |
| stream read and trim paths | refuse queue streams |

### 5.7 Per-shard queue bytes

Queue pages stay in the shard database, so a shard holds the live and
awaiting-reclamation bytes of its queues. A **per-shard queue-bytes bound**,
set from Q0's compaction working set at the `compute-1g` posture, caps it.
It is **fair per project**: when the shard nears the bound, sends from a
project holding more than its share (1/k of the bound, as #184 shares memory)
are refused `429 queue_full` first, so one tenant inside its own budget cannot
block the others. Splitting a shard by key range (SPEC D3) is not implemented
(`src/scaler3/controller.rs:114-135`); it is the prerequisite for raising the
bound, and default admission budgets (§6.6) are sized so a shard's queues fit
it until then.

## 6. Delivery

### 6.1 Claim

`claim(group, max, maxBytes, leaseMs, waitMs)` runs in two steps.

**In the committer (keyless):** take due entries (`availableAt ≤ t`) in
order from `A` past `C` and from `R`, within `maxInFlight` and `rate`,
skipping cancelled, retired and expired messages, until `max` or `maxBytes`
(by metadata `bytes`; default 1 MiB, at most 8 MiB; one message always
allowed); reserve read memory for exactly that total (429
`project_memory_pressure` if refused); lease exactly that set (row `leased`,
`attempts += 1`, `leaseGen += 1`, deadline, `L` entry, `open += 1` for newly
taken messages, `C` advanced, `lastClaimAt = t`).

**After the commit is durable (with the key):** read each leased envelope by a
**point get** of its page key (`shard_page_key(hash, offset)`), never a window
scan, which assumes dense offsets. A failed read releases those deliveries
server-side with the attempt given back and answers 503, retryable.

With nothing due and `waitMs > 0`, the claim parks until a send, settle, lease
deadline or the next `availableAt`, up to 25 s. A paused group answers at once
with `paused: true`.

### 6.2 Settle

One settle carries any mix of operations, each by lease token. **Every
operation except `extend` ends the lease (`leaseGen += 1`), so a replayed
settle is `stale` and changes nothing.**

| Operation | Effect |
|---|---|
| `ack` | done: row and any `E` entry deleted, `open -= 1` |
| `retry {delayMs?, code?, error?}` | to `retrying` in `R` (§6.3) |
| `extend {leaseMs}` | deadline to `t + leaseMs`; refused `expired` after the deadline; never past `hardExpiresAt`; allowed while paused |
| `fail {code?, error?}` | to `failed` (§7.1) |
| `discard {code?}` | done without success: row and `E` entry deleted, `open -= 1`, counted, audited |
| `release {notStarted: true}` | to `R` due at `t + 1 s`, the attempt given back, `releases += 1`; only for never-started work |

- **Late settles**: `ack`, `retry`, `fail` and `discard` after the deadline
  are accepted until the sweep moves the lease (about a second), marked
  `acceptedLate`; afterwards stale.
- **Release is bounded**: the third consecutive release without a started
  attempt counts as a `retry`.
- **Graceful stop**: started-then-aborted work is settled as `retry`.
- **Error text**: keyless settles carry only a `code` (≤ 64 chars of
  `[a-z0-9_.-]`); text (≤ 1 KiB) needs the key on the settle, and the server
  encrypts it.
- All settle operations need `queues.consume`.

### 6.3 Retry policy

`maxAttempts` (default 5) and `backoff {initialMs: 1000, maxMs: 43_200_000,
multiplier: 2, jitter: "full"}`. Policy backoff is clamped to `retentionEnd`.
**An explicit delay is never shortened**: if `t + delayMs` passes
`retentionEnd`, the delivery fails now with `cause: retention`. Exhausted
attempts, or a failure after `retentionEnd`, move it to `failed`. Nothing
outlives `hardExpiresAt`.

### 6.4 Ordering

**The first release is unordered**; nothing but `maxInFlight` and `rate`
limits concurrency, and the pilots are unordered queues. **Ordered groups
(Q6) have an open design problem**: if `A1` is scheduled for tomorrow, `A2`
(same key) and `B1` are due now, `A2` must wait while `B1` runs. A cursor that
passes `A2` leaves no durable record of a blocked, untaken message, which
reclamation could take for done; a cursor kept before `A2` must track later
messages already done. Q6 needs durable blocked state or per-key progress,
with a matching reclamation rule, before it is built. Today's stream consumer
groups stay untouched until then (D2).

### 6.5 Concurrency, pacing and overload

- `maxInFlight` (default 1,000, maximum 10,000); optional `rate {perSecond,
  burst}` on claims.
- **Settle bucket**: queue settles and extends draw from their own bucket at
  half the project's request rate (burst floor 25), beside the main bucket
  (`max(rate, 50)`, #162). A settle request whose tokens are mostly stale or
  invalid is charged to the main bucket. Under the RSS shed (#184) settles
  follow the append rules. Both are edge changes.
- **Request rate** ≈ throughput / batch size (claims) + throughput /
  messages per coalesced settle (settles) + in-flight / (lease / 2)
  (extends); worker fleets need a tier sized by it (D10).

### 6.6 Admission budget

Each queue may hold at most `maxQueuedBytes` (default 256 MiB, sized with the
per-shard bound, §5.7) and `maxQueuedMessages` (default 1 million) of live,
undone messages; messages held only by failed deliveries do not count. A send
beyond it is refused `429 queue_full`, retryable, with the budget and its use.
A queue nobody consumes is freed by the sweep as messages pass `retentionEnd`.
Sends are also admitted against the project's stored-bytes cap (#158) and the
per-shard bound (§5.7).

### 6.7 Pause, cancel, peek

- **Pause / resume**: a paused group's claims return `paused: true` at once;
  sends continue; leases may still be extended. The SDK backs off while
  paused (1 s doubling to 30 s).
- **Cancel** `(msgIds)`: the metadata row's `cancelled` flag; every group
  skips the message, open deliveries end (holders see `stale`); counted,
  audited.
- **Peek**: open deliveries and the next due entries, paginated, without
  leasing; needs the key; `queues.inspect`.

### 6.8 The maintenance sweep

Each queue has a keyless **maintenance sweep** in its owning committer. Its
tasks, each in bounded batches (at most 256 entries a step):

1. **Lease expiry**: heads of `L` past their deadline count as a failed
   attempt (`lease_expired`), `leaseGen += 1`, to `R` on clamped backoff.
2. **Retention expiry**: `A` entries past `C` with `retentionEnd ≤ t` are
   skipped and added to `retentionGap {from, to, count}` (counted, alerting).
3. **Failure expiry**: heads of `E` with `failureExpiresAt ≤ t` are discarded.
4. **Hard expiry**: a cursor `H` walks `A`; a live entry with `hardExpiresAt
   ≤ t` has every open delivery ended (leases fenced), counted, and retires.
5. **Dedup expiry**: dedup entries past their window are deleted.
6. **Deletion** of retired messages and **reclamation debt** upkeep (§8.2).
7. **Wake-up notification** (§9.2).

**Discovery after restart or move.** Each shard keeps a **queue roster**: one
row per queue with pending work, `DIRTY_SENTINEL ‖ 'Q' ‖ hash → next
deadline`, written in the same batch as the change that sets the deadline and
deleted when nothing is pending. A new owner reads the roster at open in
bounded pages (within the 180 s open deadline), builds a per-shard timer heap,
and each queue's sweep resumes from its durable cursors. A queue nobody
touches still expires, deletes and collects. Roster keys are not hash-first,
which a future key-range split must handle. Roster rows are counted in §8.3.

## 7. Failure and recovery

### 7.1 Failed deliveries are durable state, not copies

A terminal failure moves the delivery to **`failed`**: a keyless state change,
durable like any settle. The original encrypted record stays; nothing is
copied. The failed row records cause, code, attempts, cycle,
`firstDeliveredAt`, `failedAt` and `failureExpiresAt`.

- No stream key, no second append, no transfer saga; a delivery that fails
  with no worker running and after a restart is still recorded.
- Failure adds no bytes to the envelope.
- **Lifetime, one authoritative rule.** `hardExpiresAt = retentionEnd +
  failureRetention`, fixed at send, wins over everything. Each failure sets
  `failureExpiresAt = min(failedAt + failureRetention, hardExpiresAt)`.
  Failure retention defaults to 14 days (maximum 30). A message lives at most
  58 days from enqueue: `deliverAt` up to 14 days ahead, 14 days of retention,
  30 days of failure retention.

### 7.2 The dead-letter view

`deadLetters()` lists failed deliveries: envelope (with the key), cause, code,
error text (with the key), attempts, cycle, times and expiry. There is no
dead-letter stream to configure. An operator `discard` is counted and audited.

### 7.3 Redrive and republish

- **Redrive** (default): `redrive(msgIds | {all, limit})` starts a new cycle in
  the failed group only: `cycle += 1`, `attempts = 0`, `releases = 0`, due now,
  oldest first; its `E` entry is deleted, so the redriven delivery is bounded
  only by `hardExpiresAt`, and a new failure sets `failureExpiresAt` afresh.
  The message id is unchanged. Redrive is keyless.
- **Republish** (deliberate): reads the failed message with the key and sends
  a **new** message, new id and lifetime, `lineage.republishedFrom` and the
  original `originId`; it neither checks nor writes the dedup index. The first
  release republishes within the same queue or to a queue of the same project
  and key.
- Every redrive, republish, cancel, discard, pause, resume, purge and delete is
  audited with the credential and counts.

## 8. Retention, reclamation and accounting

### 8.1 Retention

`retention.maxAge`: default 4 days (the Cloudflare baseline), maximum 14,
from `availableAt`; `deliverAt` at most `maxAge` ahead (400 beyond).
`maxAge` and `failureRetention` are fixed at creation, so availability order
is expiry order; updatable retention (epochs or an expiry index) is later
work. Expiry is keyless, costs no rows, and needs no consumer (§6.8). A queue
nobody consumes within `maxAge` loses what it did not take, counted.

### 8.2 The reclamation lifecycle

Reclamation is four stages, each durable, each with its own completion test.

**1. Retirement (logical, exactly once).** A message retires when every group
has taken or skipped it and `open = 0`, or when it is cancelled, or at
`hardExpiresAt`. Retirement is the monotone transition `live → retired` of
the metadata row's `state`, and every path (the reclamation cursor `G`, the
hard-expiry cursor `H`, a final ack, discard, failure expiry or cancel) tests
and sets it in its own batch, so no message retires twice, including across a
restart or move. The retiring batch also:

- removes the message's live bytes and row charges from the stored-bytes gauge
  and the admission budget (billing stops here), and writes the usage-ledger
  entry;
- adds the message to the retired index `X`;
- updates the queue's **reclamation debt** row: `{pendingBytes,
  oldestRetiredAt, generation}`, where `generation` rises with every
  retirement, and the shard's queue roster.

`G` walks `A` up to the minimum group cursor, retiring `open = 0` messages and
stepping over the rest; a message stepped over retires when its last open
delivery ends, and `H` bounds how long that can take.

**2. Deletion (logical).** The sweep deletes, for each `X` entry, the page,
the metadata row, the `A` entry, the key-index entry and the `X` entry, in
bounded batches under the global trim budget (READINESS H5, R8). Deletes are
idempotent. `GET …/messages/{id}` answers `404 reclaimed` from the retired
state or the missing metadata row, never from page presence (the tail ring
may still hold a deleted page).

**3. Compaction (physical rewrite).** Deleted keys still occupy SSTs until a
compaction drops their tombstones together with the versions they suppress.
The shard database runs SlateDB's embedded compactor and garbage collector on
the owner (`src/config/validation.rs:26`); SPEC D17's separate compactor
service does not exist. The embedded size-tiered scheduler compacts only when
enough L0 files or similar runs accumulate, and drops tombstones only when
writing the last run, so a quiet shard never collects. The design adds a
**queue collection scheduler** per shard:

- **Trigger by amount or age**: collect when a shard's pending queue bytes
  pass a threshold or garbage fraction (efficient under load), or when its
  oldest debt passes a **collection deadline** (progress when quiet). Ratio
  alone misses old garbage in a large database; a blind periodic rewrite
  wastes work.
- **Generations**: a pass records the debt generation it started from and, on
  completion, clears debt only up to that generation, so retirements during a
  pass are never erased.
- **Plan that reaches L0**: `CompactionRequest::Full` and `FullSegment`
  exclude L0 at the pinned SlateDB (`compactor.rs:165`, so writers are not
  stalled), so "flush, then request Full" does not collect tombstones still
  in L0. The plan is a staged explicit compaction (`CompactionRequest::Spec`):
  merge the deletion-bearing L0 SSTs into a sorted run, then rewrite through
  the bottom run. Q0 chooses and validates it under concurrent writes.
- **Process-wide budget**: collection runs under one instance-wide
  maintenance budget for concurrent compaction jobs, memory and object-store
  I/O, never per database alone.
- **Residency**: outstanding queue debt keeps or wakes the shard's owner (idle
  release and billing residency read it, §5.6), since queues no longer
  contribute absorption debt.

**4. Garbage collection (physical deletion of objects).** At the pinned
SlateDB a compacted SST is deletable only after the minimum of the age
threshold, the compaction low watermark and the newest L0 time
(`garbage_collector/compacted_gc.rs:231`), and only when no manifest or
checkpoint references it. On a quiet shard a stalled watermark can block
deletion however often GC runs. **Making that watermark advance on a quiet
shard is in scope**, and may need a targeted engine or dependency change;
checkpoint and recovery protections stay intact. The acceptance test observes
obsolete objects disappearing from the bucket after traffic stops.

**Queue delete.** Deleting a queue is a queue-specific lifecycle, not only a
registry tombstone (`src/application/creation/deletion.rs`): the delete first
cancels and retires every message and deletes the dedup entries in bounded
batches, with the queue's roster row and debt keyed by storage hash, not
registry name, then writes the tombstone. Cleanup and collection continue
after the tombstone, after restarts and moves, until the queue's rows are
gone; the billing close happens at retirement. This answers READINESS
TLA-019-F3 for queues: no history rows exist, forks are refused, billing
closes at retirement, deletes are bounded and resumable, and quiet-shard
collection is stage 4.

**Escalation if shared compaction is too costly.** If Q0 shows queue
collection rewriting too much unrelated live data, or missing its deadline
within latency and memory budgets: first, **queue-specific LSM segments in the
same SlateDB** (segment-aware compaction exists at the pinned version and
shares the WAL and atomic batches, but segmentation is fixed at database
creation, so existing shard databases need a migration plan, and L0 object
counts rise); then a separate queue database per shard holding **all**
authoritative queue state, with the same scheduling and GC requirements.

### 8.3 Accounting

Three quantities are kept apart:

- **Live bytes** (billed): record bytes plus a fixed charge for every row
  family (metadata, `A`, `L`, `R`, `F`, `E`, `X`, key index, dedup, delivery
  rows, failure detail, roster and debt rows), added when written and removed
  at retirement.
- **Bytes awaiting reclamation**: the debt rows' `pendingBytes`, reported per
  queue and shard; the platform's cost, bounded by the collection deadline.
- **Physical storage**: the shard database's object-store size, reported on
  `/operator/data.json`.

Retirement-time decrements are a new ledger path: `owned_frame_bytes_current`
today only rises and is zeroed at close
(`src/shard/transaction/overlay.rs:70-90`), and the stored index applies gauge
steps (`src/rollup/stored_index.rs`). Q3 writes one billing-meta version per
sweep batch and leaves `written` unaffected. The planning figures (70-90 B of
framing and about 300 B of rows per message, 150 B per open delivery) hold
until Q0 measures them.

### 8.4 Dedup

A `dedupKey` seen within the dedup window (default 24 h, maximum 7 days,
never above retention) returns the original id with `duplicate: true`, even
when that message is already done and reclaimed. Entries are swept at the end
of their window (§6.8) and deleted with the queue. Republish bypasses dedup.

### 8.5 Named groups (later)

- A group is created at a cursor no earlier than `G`, read atomically in the
  committer: `start: earliest` means `G`; `start: now` means the queue clock,
  so it includes messages already sent but scheduled later.
- Retirement counts the groups that exist at the final release; creating or
  deleting a group is serialized with it.
- Deleting a group bumps its generation and releases its open references
  through the sweep in bounded, resumable steps.
- Group-scoped cancels and group-targeted redrive come with them.

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
  handlers finish, coalescing within 250 ms (a crash there redelivers);
  extends at half the lease. An extend answered `expired` or `stale` fires the
  handler's `AbortSignal`. `stop()` stops claiming, waits for started
  handlers (settling aborted ones as `retry`) and `release`s never-started
  work.
- `msg.discard()` is the `discard` settle operation.
- `parse` failures are retried with backoff by default (a deploy window with
  a schema mismatch would otherwise dead-letter everything); `onInvalid:
  "fail"` fails them with `code: invalid_payload`.
- Backs off while paused. Hooks `onMessage`, `onSettle`. A client without
  configuration throws in production. Runs on Node, Bun, Deno and Compute.

### 9.2 Wake-up notifications (first release)

A sleeping process cannot claim and a Cloudflare Worker cannot host a loop.
Pushing decrypted batches would need the key on the server. Instead a group
can have a **wake-up subscription**: a signed hint to come and claim.

**The notification**: `{queue, group, due, oldestDueAgeMs, nonce, sentAt}`,
HMAC-signed, no content, no key. Its 2 s limit covers only the receiver's
acknowledgement.

**Receiver contract (fast acknowledgement, bounded drain):**

1. Verify the signature, that `queue` and `group` are the ones it serves, and
   that `sentAt` is within 60 s of its clock.
2. Register the drain with the runtime's background mechanism: `waitUntil` on
   Cloudflare Workers (about 30 s after the response, per Cloudflare's
   documentation). On Prisma Compute the tree today knows only
   `KeepAwakeGuard` (RUNBOOK); Q4 verifies a Compute `waitUntil` before relying
   on it, and otherwise holds a keep-awake guard for the drain.
3. Answer `202` at once.
4. Drain within a **drain budget**: stop claiming when the time left is under
   the settle margin, settle what finished, `release` what never started.
5. Return; the scheduler wakes it again if work remains. Leases and retries,
   not the runtime, carry durability.

**Re-arming and backoff, from durable state.** The group row holds
`lastClaimAt` (set by every claim), `lastNotifiedAt` and `notifyBackoffMs`.
The sweep notifies when the group has due work and no claim since the last
notification plus the backoff. A notification that gets no claim afterwards,
acknowledged or not, doubles the backoff (1 s → 60 s); a claim resets it. At
most one notification is in flight per subscription, at most one per backoff
interval, and at most 10 a second per project. The fan-out a receiver starts
is its own choice, bounded by `maxInFlight`. A restart or move re-arms from
the rows. A lost notification costs latency only.

**Secret custody.** The signing secret is `HMAC(cell wake key version k,
project ‖ subscriptionId ‖ rotation)`, returned once at creation or rotation;
the subscription records `k`. Rotating the cell key adds version `k+1` for
new subscriptions and rotations; old versions stay until no subscription uses
them. During a subscription's rotation the server signs with the new secret
and the receiver accepts both for up to 7 days. The cell wake key is a new
secret under the existing custody process.

**Egress policy.** Only `https` on port 443 to names resolving to public
addresses. The server resolves once, connects to the pinned address and
verifies TLS against the URL's host name. It refuses `0.0.0.0/8`, loopback,
private, link-local, CGNAT, multicast and metadata ranges, the IPv6 `::1`,
`fc00::/7`, `fe80::/10`, IPv4-mapped and multicast ranges, and the cell's own
addresses. No redirects, a body ≤ 1 KiB, the response ignored, every
subscription change audited.

**Which consumers this serves.** Compute apps get queue-triggered execution.
The Console's Worker consumers migrate in the first release only where their
handlers finish within a Worker's drain budget; longer ones run on a Compute
worker. A Worker can also drain from a cron trigger. A **kept-awake Compute
worker** is the pilot's simplest option, with its idle cost stated.

## 10. Telemetry

- The group's `GET`: `due`, `scheduled`, `leased`, `retrying`, `failed`,
  `oldestDueAgeMs`, `oldestFailedAgeMs`, `nextAvailableAt`, `paused`,
  `retentionGap`, hard-expired, admission budget use, wake-up health
  (`lastNotifiedAt`, backoff, failures, `lastClaimAt`), counters.
- `/operator/data.json`: per-project queue totals, oldest-due age, failed
  counts, gaps, per-shard queue live bytes, bytes awaiting reclamation, oldest
  debt age, physical size, sweep lag, collection passes; cell-watch rules for
  failed growth, due work near `maxAge`, gaps, sweep lag and debt past its
  deadline, calibrated after the pilot.
- Usage: queue operations metered as today, plus read and stored bytes.

## 11. HTTP surface

| Route | Purpose |
|---|---|
| `PUT /v1/queues/{name}` · `GET` · `DELETE` | create (`retention`, `failureRetention` fixed; admission budget and dedup window updatable) · config and stats · delete (§8.2) |
| `GET /v1/queues` | list |
| `POST /v1/queues/{name}/messages` · `messages:batch` | send (batch: atomic, ≤ 1,000 messages within the body limit) |
| `GET /v1/queues/{name}/messages/{id}` | envelope and delivery state (needs the key; `404 reclaimed` once retired) |
| `PUT …/groups/default` · `GET` | group config (`maxAttempts`, `backoff`, `leaseMs`, `maxBatch`, `maxInFlight`, `rate`, wake-up subscription) |
| `POST …/groups/default:claim` · `:settle` | claim; ack / retry / extend / fail / discard / release |
| `POST …/groups/default:pause` · `:resume` · `POST /v1/queues/{name}:cancel` | operator actions |
| `GET …/groups/default/failed` · `POST …:redrive` · `:discard` | dead-letter view (paginated) and recovery |
| `POST /v1/queues/{name}:republish` | republish failed messages as new ones |
| `GET …/groups/default/pending` | peek (paginated) |

Any other group name is 404 `group_not_found`. Headers use the `Streams-` form
the Compute edge passes. Scopes: `queues.send`, `queues.consume` (claim,
settle incl. `discard`, wake-up handler), `queues.manage` (config, pause,
cancel, redrive, republish, operator discard, delete), `queues.inspect` (get,
peek, dead-letter view; with the key). Clamps, published: `leaseMs` 1 s to
12 h, `waitMs` ≤ 25 s, `max` ≤ 1,000, `maxAttempts` 1 to 100, `backoff.maxMs`
≤ 12 h.

## 12. First release, and what follows

| Requirement | Scope |
|---|---|
| Feasibility | Q0 passes every gate in §16 before Q1 starts |
| Storage | queue storage mode with guarded consumers, the four-stage reclamation lifecycle with quiet-shard collection, accounting of the three quantities, per-shard fair bound, queue delete lifecycle |
| Delivery | the `default` group, unordered, queue clock, fenced leases, settles, retry with backoff, scheduled availability, claim byte limits |
| Recovery | durable failed deliveries, dead-letter view, redrive, republish, discard |
| Time | fixed retention, the maintenance sweep and queue roster, admission budget |
| Runtime | SDK runner, wake-up notifications under the fast-acknowledgement contract, kept-awake pilot option |
| Operations | pause/resume, cancel, stats, audit, cell-watch rules |
| Validation | §13, then the low-risk pilot |

**Later:** named groups (§8.5); ordered groups once §6.4 is designed (Q6) and
stream consumer groups onto the engine; live observation or export of a
queue's traffic (not replay), or an explicit history-retention feature;
updatable retention; shared `limiters`; republish across keys; an in-memory
local fake; generic HTTPS push.

| Workload | First release? | Settings |
|---|---|---|
| Plain support sync (pilot) | yes | defaults |
| Offer and spend-limit enforcement | yes | dedup key per (workspace, hour) |
| Stripe and spend-limit usage sync | yes | `maxAttempts 11`, backoff 30 s..12 h; `rate`; spread by `deliverAt` |
| Vercel usage sync → billing data | yes | `delay 2m`; billing data as a downstream queue |
| Notification → email | yes | `maxInFlight`/`rate` matched to Brevo |
| ppg-events-shared | yes | external producer with `queues.send` |
| Invoice submission | yes | self-continuations via `deliverAt`; the reconciler stays |
| Credential cleanup | yes | `deliverAt` per request |
| Console Worker consumers | where handlers fit the Worker drain budget | wake-up handler; others on a Compute worker |
| Dunning, tenant-manager sync, Bloom turns | after Q6 | ordered |

## 13. Verification

- **TLA+**, two models within CI's formal budget (bound today: TLA-002 at
  40-50 min). **`QueueDelivery`**: queue clock with clock steps and moves,
  availability index, cursor, delivery rows and indexes, fencing and replayed
  settles, bounded release, the sweep's expiry tasks, failure, redrive,
  republish, cancel, pause, crash/restart. **`QueueReclamation`**: retirement
  by every path with restarts between retirement, deletion and collection;
  debt generations with retirements during a pass; queue delete across a
  restart; group creation (§8.5). Invariants: no message lost; no send at or
  below a cursor; a replayed settle changes nothing; nothing outlives
  `hardExpiresAt`; no message retires twice and accounting moves exactly
  once; nothing open or ahead of a cursor is deleted; every retired message is
  eventually deleted and its debt eventually cleared; a pass never clears a
  newer generation; ready work is claimable while `maxInFlight` allows.
- **Kani**: codecs (big-endian order), the id order, lease tokens with group
  generation, backoff and lifetime arithmetic, the tail flag decoder.
- **DSTs**: the colleague's evidence lists (recoverable failure without keys,
  delayed backlog not hiding ready work, rollover, replayed settles, a noisy
  queue not stopping other tenants, an unconsumed queue freeing its budget,
  hard expiry fencing a long-extended lease, explicit retry past retention,
  dedup expiry on a quiet queue, wake-up re-arm after drain, restart and move)
  and the storage-mode guards: **an absorber tick gathers nothing from a
  queue; a queue tail is never a repair candidate; queue traffic does not mask
  a stalled absorber; a cold queue shard is not absorption debt; a deleted
  queue's rows are gone after restart and move; a queue roster resumes the
  sweep on a new owner; double-retirement attempts move accounting once.**
  Plus clock steps, mixed-success batches, crash windows, claim read
  failures, release loops, the per-shard fair bound and egress refusals.
- **SDK**: runner and wake-up handler against the real server; Workers in CI,
  including a drain that outlasts the acknowledgement.
- **Layer B queue arm**: queue workers, a poison tenant, a send flood and a
  collection pass under the shared-cell bars.
- **Pilot**: `cp-plain-support-sync` beside the old queue for a week; every
  message the old queue processed is processed by the new one (by business
  id), none lost, failures match, p99 send-to-start within the old queue's.

## 14. Decisions for the owner (each with a recommendation)

| # | Decision | Recommendation |
|---|---|---|
| D1 | Resource shape | `/v1/queues/{name}` over streams of kind `queue`, queue routes only |
| D2 | Stream consumer groups | Unchanged until ordered groups (Q6) |
| D3 | Failure model | Durable failed state referencing the original record; no DLQ stream |
| D4 | Lifetime | `hardExpiresAt` wins; failure retention 14 days default, 30 maximum; at most 58 days from enqueue |
| D5 | Redrive | Same id, new cycle, failed group only, failure clock restarted; republish separate |
| D6 | Execution | SDK runner plus wake-up notifications (fast acknowledgement, bounded drain); kept-awake pilot worker |
| D7 | Retention | 4 days default, 14 maximum, fixed at creation; expired untaken messages are counted gaps |
| D8 | Reclamation | Retirement, deletion, scheduled compaction and verified quiet-shard GC (§8.2), required before production queues |
| D9 | Admission budget | 256 MiB / 1 million undone messages per queue by default, within the per-shard fair bound |
| D10 | Request tier | A per-project queue tier sized by the §6.5 formula |
| D11 | Overload | A separate settle bucket; #184's append rules |
| D12 | Batches | Atomic ≤ 1,000 messages within the body limit |
| D13 | Dedup | 24 h default, 7 days maximum, never above retention |
| D14 | Billing | Meter queue operations as today; live bytes billed, reclamation debt not |
| D15 | Sequencing | Q0 gates Q1; the first release gates the beta if queues are a beta requirement |
| D16 | Wake-up egress | §9.2 egress policy with a versioned derived secret |
| D17 | Queue storage | Queue pages in the shard database, never absorbed, under queue storage mode |
| D18 | Groups | Only `default` first; named groups under §8.5 |
| D19 | Tail format | A queue storage flag bit in the tail; the first binary writing it is a one-way upgrade for cells with queues (or a `LAYOUT_VERSION` bump, if the owner prefers) |
| D20 | Collection | The compaction trigger in the shared shard database; segments, then a separate queue database, only if Q0 shows shared compaction too costly |

## 15. Review responses

### 15.1 Fable 5.1 review of revision 5 and the owner's direction (2026-10-23)

Fable's verdict was "revise"; its code claims hold (checked: no compactor
binary in `src/bin/`, the embedded compactor at `src/config/validation.rs:26`,
the repair trigger at `src/shard.rs:404`, `Full` excluding L0, the GC
watermark minimum). The owner chose option (a) and set its requirements.

| Point | Revision 6 |
|---|---|
| B1 "never absorbed" is acted on by six subsystems | queue storage mode: five invariants and a guarded-consumer table (§5.6); `absorbed = next` only as a projection (owner) |
| B2 no compactor service; no quiet-shard collection | the four-stage lifecycle with scheduler, L0-reaching plan and GC progress in scope (§8.2); SPEC D17 noted stale |
| Owner: durable debt, generations, amount-and-age trigger, process-wide budget, residency | §8.2 stage 3 |
| Owner: `Full` excludes L0; GC watermark on quiet shards | §8.2 stages 3 and 4 |
| Owner: absorber-progress masking; read-path boundary; explicit storage kind | §5.6 invariants 2 and 4; D19 |
| Owner: escalation path | segments, then a separate queue database with all state (§8.2); D20 |
| H1 queue delete leaks | queue delete lifecycle (§8.2) |
| H2 double retirement | monotone retired bit tested by every path (§8.2) |
| H3 discovery after restart or move | queue roster and timer heap (§6.8) |
| H4 re-arm backoff and durable inputs | group-row fields, notified-without-claim backoff, budgets (§9.2) |
| H5 per-shard bound blast radius | per-project fair bound; split as prerequisite; smaller defaults (§5.7, §6.6) |
| H6 failure clock through redrive | redrive deletes `E`; a new failure restarts it (§7.1, §7.3) |
| M1 queue clock not persisted | clock in the queue state row (§5.5) |
| M2 wrong D17/D3 citations | corrected (§5.6, §5.7, §8.2) |
| M3 cancel rows | group-scoped cancels moved to §8.5 |
| M4 point gets; 404 from metadata | §6.1, §8.2 |
| M5 new ledger path | §8.3 |
| M6 Compute `waitUntil` unverified | verified in Q4, keep-awake fallback (§9.2) |
| M7 freshness, IPv6, TLS, rotation signing | §9.2 |
| M8 Q0 thresholds | §16, with the owner's adjustments |
| M9 dedup after reclamation and on delete | §8.4 |
| L1-L5 | extend while paused (§6.2); name collision (§4); test names (§13); framing range (§5.1); `E` deleted on ack (§6.2) |

### 15.2 Earlier reviews

The colleague's reviews of revisions 2 and 4 are taken except where narrowed:
a settle bucket instead of a shedding exemption (a full exemption would let
junk settles flood); no payload push in the first release (keyless wake-up
notifications are the execution path); ordered groups after the first release
(§6.4 shows they need more design); one scheduling owner per queue. Fable's
reviews of revisions 1 and 3 stay answered as recorded in revision 4
(`git show a768e650:docs/QUEUES-DESIGN.md`), except that queue pages are no
longer absorbed, so history-row deletes are gone.

## 16. Plan and size

**Q0 storage feasibility (gate for Q1).** An isolated experiment revision,
never production, built at the pinned SlateDB: queue storage mode, one-message
pages, metadata and index rows, the four-stage lifecycle with the collection
scheduler and GC progress, run at `MEMPROFILE_CERT=compute-1g`, **beside
substantial stable Streams data in the same shard**. Cases:

| Case | Establishes |
|---|---|
| a few sends and retirements below the ordinary compaction thresholds, then silence | collection does not depend on later traffic |
| a large queue drains completely; separately, to one live message | empty and nearly empty queues reclaim without losing live work |
| queue delete, then restart or ownership move | cleanup is found without a registry entry |
| retirements during a collection pass | a pass never erases newer debt |
| queue churn beside stable shard data, up to the per-shard bound | rewrite amplification and co-tenant interference |
| everything at `compute-1g` | memory, latency and maintenance progress fit the posture |

Gates (any failure stops Q1 and reopens §5 and §8):

| Measure | Pass |
|---|---|
| Physical convergence after silence | physical size ≤ expected baseline (live data, metadata, recovery overhead) + a residual allowance of 64 MiB per shard, and ≤ 1.25× live for live data over 1 GiB, within the collection deadline (target 60 min, kept only if discovery, scheduling, rewrite, checkpoint protection, GC cadence and deletion all fit), with no new periodic LIST |
| Unit cost | object-store requests and bytes per million completed messages (send → reclaim) at 10, 100 and 1,000 msg/s, plus fixed idle maintenance cost per shard, reported and within the pricing hypothesis |
| Stored overhead | ≤ 600 B per message at 100 B bodies; ≤ 1.5× at 1 KiB; ≤ 1.1× at 64 KiB |
| Steady churn | shard size ≤ 2 × (live backlog + 1 h of churn) at 1,000 msg/s over 2 h, 50 queues |
| Latency during collection | p99 send and claim ≤ 150 ms; the co-tenant's append p99 within 10% of baseline; no backpressure |
| Memory | RSS under the shed line throughout, including the largest compaction |
| Failed backlog | 1 million hard-expiring deliveries cleaned within 2 h under the trim budget while serving 1,000 msg/s |
| Cold open | 10 million queue rows open in ≤ 30 s, no tail repair, roster resumes |
| Per-shard bound | set from the measured working set, with the fair-share rule |

| Package | Contents | Estimate |
|---|---|---|
| Q0 Storage feasibility | the experiment and gates above, including the L0-reaching compaction plan and quiet-shard GC progress | 8-12 days |
| Q1 Engine | queue storage mode and every guarded consumer, the tail flag, queue clock, pages and rows, cursor, delivery rows and indexes, ready cache, claim, settle, backoff and retention rules, `maxInFlight`, `rate`, pause, settle bucket | 22-30 days |
| Q2 Queue resource | `kind: queue` and refusals, routes for `default`, scopes and clamps, dedup, scheduled messages, failed view, redrive, republish, discard, cancel, peek, audit, admission budget, per-shard fair bound | 9-13 days |
| Q3 Sweep and reclamation | the sweep and queue roster, gaps and lifetimes, retirement, deletion, the collection scheduler, GC progress, queue delete lifecycle, accounting of the three quantities | 18-26 days |
| Q4 Execution | SDK API and runner, wake-up subscriptions (secrets, egress, re-arming), the bounded-drain handler for Compute and Workers, Workers CI | 12-16 days |
| Q5 Verification | the two TLA models, Kani, DSTs, Layer B arm, pilot | 13-20 days |
| Q6 Ordered and named groups (after) | design §6.4, then build; §8.5; stream consumers onto the engine | after design |

**First release (Q0-Q5): about 82-117 engineering days**, before workload
migration, plus any SlateDB change Q0 finds necessary for quiet-shard GC.
Q1 starts only when Q0 passes. Then Q1 → Q2 → Q3, with Q4 against Q2's
routes and Q5 alongside.

Costs to plan for: edits to `src/shard/transaction/append.rs`, the absorber,
the tail codec and the shard transaction core stale formal receipts (about
150 min serial per round); new key families go in new modules because
`src/queue.rs` and `src/product_cursor.rs` are by-path includes; routes live in
`src/product/queues/` because `src/product.rs` has no headroom; mutants run at
about 5 minutes each with a per-push cap, so each package lands in several
pushes. SPEC D17 and D23 describe a compactor service and maintenance-free
shard logs that the tree does not have; correcting SPEC is a separate change.
