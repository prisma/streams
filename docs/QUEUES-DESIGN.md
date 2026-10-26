# Native queues in Prisma Streams — design for review

Status: **revision 8, 2026-10-23. The architecture and the Q0-first sequence
are approved; Q0 ([QUEUES-PLAN.md](QUEUES-PLAN.md) §2) is the first
implementation package.** Revision 8 folds in Fable 5.1's review of revision 7
(the SlateDB primitives of a collection pass, a quiet-shard GC completion rule,
timer service through the billing sweep's rotation, a trigger floor, the
pass-duration bound and the queues-per-shard arithmetic) and the pricing goal
([QUEUES-PRICING-BENCHMARK.md](QUEUES-PRICING-BENCHMARK.md)). Revisions 1-5
built the delivery, failure and lifetime models through Fable 5.1 and
colleague reviews. Revision 6 kept queue data in the shard database under an
explicit queue storage mode, with durable reclamation debt, compaction
scheduled by amount and age, and verified physical collection on a quiet
shard, all proven by Q0 before Q1 starts. Revision 7 specifies the
transitions a review of revision 6 found open: retirement into collection
(a coverage rule), auxiliary-row lifetimes, failure into redrive, active
queues into deletion, drains into later wake-ups, timers with no debt, and
capacity reservations. §15 maps every review point. Every client-visible
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
is a queue's delivery model: unordered competing consumers, retries that
free and reschedule the delivery, claims that find due work wherever it is,
scheduled messages, failure as durable keyless state with recovery, redrive,
pause, stats, cancellation, a worker runner, and a wake-up path for
serverless workers. Today's groups are per-key FIFO, pin the cursor on retry,
see a 4 MiB window, and dead-letter only by copying with the stream key.

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

An entry's value carries what a claim of the untaken message needs: its
metadata as sent (an untaken message's metadata row never changes, and a
cancel deletes the entry) and, when the sealed page is at most 2 KiB, the
page itself (`INLINE_PAGE_MAX`). Entries due together are adjacent in `A`,
so a claim of new messages reads them in the blocks its scan of `A` already
reads, with no point get of a metadata row or a page; a scheduled message
claimed an hour after its send would otherwise cost two cold reads. The
inline copy is deleted with the entry when the message retires.

**One scheduling owner per queue.** A queue writes one physical lane, so one
shard owns it. The scaler cannot split a single-key workload
(`src/scaler3.rs`), so a queue has a per-queue ceiling, measured over the
whole send → claim → ack → reclaim cycle in Q0 and published (first target:
1,000 messages a second per queue; the control plane's busiest hour averages
17.5).

### 5.3 Group state

| State | Meaning |
|---|---|
| group row | `{generation, config, paused, lastUsefulClaimAt, lastNotifiedAt, notifyBackoffMs}` |
| group cursor `C` = `(time, msgId)` | every `A` entry at or below `C` has been taken or skipped |
| delivery row per **open** delivery | `{state: leased \| retrying \| failed, cycle, cycleDeadline, attempts, releases, leaseGen, leaseDeadline, firstDeliveredAt, failureExpiresAt?, errorCode}` |
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
queue batch that changes queue state (a send, a claim that leases, a settle, a
sweep step that moves something). **An empty claim or an idle sweep step
writes nothing**: the cursor invariant below needs the clock only where
state changes, and an idle consumer must cost no object-store writes (the
pricing benchmark's idle run). It is not kept in
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
named test ([QUEUES-PLAN.md](QUEUES-PLAN.md) §4):

| Consumer | Queue-mode behaviour |
|---|---|
| dirty index (`src/shard/transaction/finalize.rs:100-112`) | a queue tail is never marked dirty for absorption |
| absorber roster and due list (`src/history/gather.rs:252-258`, `src/history.rs:625-645`, `src/history/worker.rs:226-290`) | queues never enter the roster; a tick gathers nothing for them |
| maintenance ledger and stall latch (`src/shard.rs:388-420`, `maintenance_row.rs`, `src/backpressure.rs:150-180,343-350`, cell-watch lag rule) | queue bytes are excluded from unabsorbed bytes and from progress |
| restart tail repair (`src/shard.rs:401-405`, `src/shard/record.rs:258-291`) | a queue tail is never a repair candidate |
| frame reconciliation (`finalize.rs:80-88`) | queue deletes are counted on the queue's own ledger side |
| billing sweep residency (`probe_debt`, `src/billing.rs:1676-1686`) | counts queue reclamation debt and the roster's earliest deadline (§6.8) instead of absorption debt; idle release only drops caches and is unaffected |
| stream read and trim paths | refuse queue streams |

### 5.7 Per-shard queue bytes

Queue pages stay in the shard database, so a shard holds the live and
awaiting-reclamation bytes of its queues. A **per-shard queue-bytes bound**,
set from Q0's compaction working set at the `compute-1g` posture, caps it.

**Capacity is reserved, not shared after the fact.** Refusing a heavy
project's next send frees none of the data it already holds, so fairness at
send time cannot protect a newcomer. Instead, creating a queue **reserves**
its admission budget (§6.6: `maxQueuedBytes` plus the dedup budget, §8.4)
against its shard's bound, in the committer, and creation is refused (`429
queue_capacity`, not retryable; the caller may choose smaller budgets) when
the reservations would exceed the bound. A queue never holds more than its
reservation, so every admitted queue can always reach its budget whatever its
neighbours do. There is no borrowing in the first release; overcommitting
reservations (with a fairness rule for the borrowed part) is a later owner
decision once Q0 and the pilot show real occupancy. Splitting a shard by key
range (SPEC D3) is not implemented (`src/scaler3/controller.rs:114-135`); it
is the prerequisite for more reserved capacity per shard (D21).

**The arithmetic is a cap on queues per shard.** With the defaults (256 MiB
of messages plus about 100 MiB of dedup entries), queues per shard =
`floor(B ÷ 356 MiB)`: 5 at a 2 GiB bound, 22 at 8 GiB. The bound `B` is
physical (live plus awaiting reclamation, about 2-3× the reservation within
the completion deadline) and is itself limited by how long a collection pass
may run (§8.2), so Q0 sets it. The control plane alone has 18 queues, and a
caller cannot choose its shard (placement is by storage hash over a fixed
prefix code, `src/registry.rs:1332-1340`). The recommendation (D21) is
smaller default budgets with an opt-up at creation, decided with Q0's numbers;
the reservation is a ledger row written in the creation batch the committer
already runs (`src/application/creation/initialization.rs`), carried by
whole-prefix moves, and re-reserved when a budget is updated (§11).

## 6. Delivery

### 6.1 Claim

`claim(group, max, maxBytes, leaseMs, waitMs)` runs in two steps.

**In the committer (keyless):** take due entries (`availableAt ≤ t`) in
order from `A` past `C` and from `R`, within `maxInFlight` and `rate`,
skipping cancelled and retired messages and entries past their **cycle
deadline** (§6.3: an untaken `A` entry's is `retentionEnd`; an `R` entry's is
its cycle's), until `max` or `maxBytes`
(by metadata `bytes`; default 1 MiB, at most 8 MiB; one message always
allowed); reserve read memory for exactly that total (429
`project_memory_pressure` if refused); lease exactly that set (row `leased`,
`attempts += 1`, `leaseGen += 1`, deadline, `L` entry, `open += 1` for newly
taken messages, `C` advanced; `lastUsefulClaimAt = t` when at least one
message was leased). The committer runs a shard's operations one at a
time, so a claim must not wait for storage block by block: its walk of `A`
keeps sixteen block fetches in flight ahead of it, and the claims that
follow find their blocks read or in flight ([pricing benchmark
§11](QUEUES-PRICING-BENCHMARK.md)).

**After the commit is durable (with the key):** open each leased envelope
from the page its `A` entry carried (§5.2), or else read it by a **point
get** of its page key (`shard_page_key(hash, offset)`), never a window scan,
which assumes dense offsets. A failed read releases those deliveries
server-side with the attempt given back and answers 503, retryable.

With nothing due and `waitMs > 0`, the claim parks until a send, settle, lease
deadline or the next `availableAt`, up to 25 s. A paused group answers at once
with `paused: true`. While it waits the request is parked in the live pool
and its project's share, out of the in-flight write gate's active count
(edge change #194). A claim that leased messages reserves exactly their
bytes in the instance's read memory and its project's read bytes before it
reads them, waiting up to 2 s for room; without room it releases its leases
with their attempts given back and answers 503 `read_memory_busy` or 429
`project_memory_pressure`, retryable after 1 s (#195). Its answer holds its
rendered size until the body ends.

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
multiplier: 2, jitter: "full"}`.

Each delivery cycle has a **cycle deadline**, stored on the delivery row: the
first cycle's is `retentionEnd`; a redriven cycle's is `hardExpiresAt` (§7.3).
One rule then governs retries, `R` claims and lease expiry alike:

- Policy backoff is clamped to the cycle deadline.
- **An explicit delay is never shortened**: if `t + delayMs` passes the cycle
  deadline, the delivery fails now with `cause: retention`.
- Exhausted attempts, or a failure after the cycle deadline, move the
  delivery to `failed`.
- A running lease may be extended up to `hardExpiresAt`, and nothing outlives
  `hardExpiresAt`.

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
  follow the append rules. Both are edge changes. The settle bucket is not
  built yet.
- **Overload parity** (layout 6A package 1, edge changes #188-#193, built):
  the pre-auth survival bound covers `/v1/queues`; a verified queue request
  is bound to fair shedding after project admission, so a project over its
  share of the cell's requests is refused on every queue route over the RSS
  shed line and at the in-flight cap; a queue `POST` is refused while its
  project is over its memory line and every queue body is charged to the
  project while it is buffered; sends, batches and settles meet the RSS write
  gate before their bodies and the in-flight write gate once buffered, with
  the append's spared exceptions; and a request's committer operation is
  refused at once, 503 `shard_busy`, when the shard committer's queue is
  full (the sweeps and collector still wait for room).
- **Request rate** has a producer and a consumer term, sized separately
  (D10). Producers: sends ÷ messages per send (singletons make one request a
  message; `sendBatch` divides it). Consumers: throughput ÷ claim batch +
  throughput ÷ messages per coalesced settle + in-flight ÷ (lease ÷ 2) for
  extends. At 1,000 messages a second with 100-message claims and settles,
  consumers make about 20 requests a second before extends, while singleton
  producers make 1,000.

### 6.6 Admission budget

Each queue may hold at most `maxQueuedBytes` (default 256 MiB) and
`maxQueuedMessages` (default 1 million) of live, undone messages; messages
held only by failed deliveries do not count. The budget is reserved against
the shard at creation (§5.7). A send beyond it is refused `429 queue_full`,
retryable, with the budget and its use. A queue nobody consumes is freed by
the sweep as messages pass `retentionEnd`. Sends are also admitted against the
project's stored-bytes cap (#158).

**Redrive re-enters the budget.** A failed message does not count; redriving
makes it undone again, so redrive reserves its count and bytes in the same
batch (the payload is already stored and is not charged twice). A redrive
that does not fit leaves the delivery failed and reports `capacity`;
`redrive({all, limit})` stops at the first message that does not fit and
returns `{redriven, remaining, stoppedAt: "capacity"}`.

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
5. **Dedup expiry**: dedup entries past their window are deleted, and the
   deletion adds collection debt like any other (§8.2).
6. **Deletion** of retired messages and **reclamation debt** upkeep (§8.2).
7. **Wake-up notification** (§9.2).

**Discovery after restart or move.** Each shard keeps a **queue roster**: one
row per queue with pending work, `DIRTY_SENTINEL ‖ 'Q' ‖ hash → next
deadline`, written in the same batch as the change that sets the deadline and
deleted when nothing is pending. A new owner reads the roster at open in
bounded pages (within the 180 s open deadline), builds a per-shard timer heap,
and each queue's sweep resumes from its durable cursors. Roster keys are not
hash-first, which a future key-range split must handle. Roster rows are
counted in §8.3.

**Index floors.** Every lease, retry and failure is put and later deleted,
so the time-ordered indexes (`A`, `L`, `R`, `E`, the dedup expiries) carry
one tombstone per delivery until a compaction reaches the last sorted run,
and a scan from an index's start walks all of them. The state row therefore
keeps, per index, a floor: a lower bound on the time of its first live
entry (for `A`, of its first live entry past the cursor), plus the earliest
cycle deadline among retries. Every put lowers a floor in the batch that
holds the entry; a scan starts at the floor and may raise it to the first
live entry it found, in the batch whose deletes justified it; deletes leave
it low, which is only slower. A claim does not read `A` or `R` while that
index's floor is past the claim's clock, reads it only up to its first live
entry past the clock, and then raises its floor to the first entry it left:
the first due entry it did not take, else that later entry (the index's end
when there is none). The rest of a herd of claims then reads nothing until
a put lowers the floor or the clock reaches it. The roster's deadline is
computed from the floors alone: the earliest lease expiry, retry cycle
deadline, failure expiry, dedup expiry, untaken retention end and wake-up,
or an hour out when none applies. An operation only brings the roster
forward; a sweep sets it. A due retry or a wake-up with nothing claimable
waits for a claim, not for the sweep, so after a sweep those terms look
again a minute on. An active queue is thus swept when something can be due
(about once per lease duration under load), not after every operation.

**Timers are a maintenance obligation of their own.** A queue can have a
scheduled message, a retry, a lease deadline or a dedup expiry with zero
reclamation debt. What actually closes a shard today: fleet eviction or a
move, shutdown, and the billing sweep evicting engines it opened itself
(`src/sharddir/holdoff.rs:36-39`, `src/billing.rs:1543-1578`); a shard a
customer request opened is never closed by the sweep. The sweep runs every
300 s, discovers at most 8 shards a tick, and keeps at most 2 debt-holding
engines for 4 ticks before rotating them out. So:

- `probe_debt` (`src/billing.rs:1676-1686`) adds the roster's earliest due
  deadline to `Debt`, so a closed shard with due queue work is opened by the
  sweep like one with billing debt.
- **Service bound**: on a resident shard a deadline is served within 1 s; on
  a closed one within one sweep rotation, `ceil(owned shards ÷ 8) × 300 s`
  plus the open time. Q0 measures it. (Keeping every shard with a pending
  deadline resident would break the sweep's two-engine budget.)

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
  oldest first. Its `E` entry is deleted and its cycle deadline becomes
  `hardExpiresAt` (§6.3), so the redriven cycle can be claimed and retried
  until then, and a new failure sets `failureExpiresAt` afresh. It reserves
  admission budget first (§6.6). The message id is unchanged. Redrive is
  keyless, and refused while the queue is deleting (§8.2).
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

- removes the message-owned live bytes and row charges from the stored-bytes
  gauge and the admission budget (billing for the message stops here; §8.3
  lists what is not message-owned), and writes the usage-ledger entry;
- adds the message to the retired index `X` with its retirement time, and
  sets the shard's queue roster.

`G` walks `A` up to the minimum group cursor, retiring `open = 0` messages and
stepping over the rest; a message stepped over retires when its last open
delivery ends, and `H` bounds how long that can take.

**2. Deletion (logical).** The sweep deletes, for each `X` entry, the page,
the metadata row, the `A` entry, the key-index entry and the `X` entry, in
bounded batches under the global trim budget (READINESS H5, R8). Deletes are
idempotent. `GET …/messages/{id}` answers `404 reclaimed` from the retired
state or the missing metadata row, never from page presence (the tail ring
may still hold a deleted page). **The deleting batch creates the collection
debt**, not the retirement: it adds the deleted bytes to the queue's
**deletion debt** row `{pendingBytes, oldestDeletedAt, generation}`, where
`generation` rises with every deleting batch. The debt row is a hot row by
design: nearly every claim and settle deletes something and updates it. Every other substantive
deletion (dedup expiry, failure detail, delivery rows, queue delete) adds to
the same debt the same way, so no tombstone is ever written without a
collection obligation.

**3. Compaction (physical rewrite).** Deleted keys still occupy SSTs until a
compaction drops their tombstones together with the versions they suppress.
The shard database runs SlateDB's embedded compactor and garbage collector on
the owner (`src/config/validation.rs:26`); SPEC D17's separate compactor
service does not exist. The embedded size-tiered scheduler compacts only when
enough L0 files or similar runs accumulate, and drops tombstones only when
writing the last run, so a quiet shard never collects. The design adds a
**queue collection scheduler** per shard:

- **Trigger by amount or age, with a floor**: start a pass when a shard's
  pending bytes pass a threshold or garbage fraction (efficient under load),
  or when its oldest outstanding work reaches the **launch age** *and* the
  pending bytes reach a minimum pass size (progress when quiet). Below that
  minimum the age trigger waits up to a maximum interval of hours: every claim
  and settle deletes something, so without the floor a shard with a trickle of
  queue traffic would rewrite its bottom run every launch age (96 times a day
  at one message a minute). Passes a day and bytes a pass are Q0 measurements
  that feed the unit cost.
- **Launch age and completion deadline are different numbers.** The
  completion deadline (target 60 minutes from retirement) bounds when the
  objects must be gone; the launch age must leave room for deletion, flush,
  compaction, checkpoint protection and GC inside it. Q0 measures a pass's
  duration and sets the launch age to `deadline − p99 pass duration − margin`
  (first estimate 15 minutes).
- **Coverage rule.** A pass covers a defined set of work, and the deletions
  that implement it, before it clears anything:
  1. choose a cutoff `T`: the pass covers every `X` entry retired at or
     before `T` and every deletion already in the debt;
  2. wait until the deletion sweep has removed every `X` entry at or before
     `T` (their deleting batches raise the generation);
  3. only then capture the debt generation `g` and its pending bytes, so `g`
     includes every covered deletion;
  4. flush the **memtable**, `flush_with_options(FlushType::MemTable)` as the
     ticker does (`src/shard.rs:1583-1590`; `Db::flush()` flushes only the WAL
     when the WAL is on, `slatedb/src/db.rs:1779-1789`), and read the boundary
     from `Db::manifest()` when it returns;
  5. submit one spec through `Admin::submit_compaction` (`admin.rs:195-209`):
     `CompactionSpec::for_segment(root, every boundary L0 in tree order plus
     every sorted run, destination = the lowest run id)`
     (`compactor_state.rs:140-146`). Tombstones drop only when the destination
     is the tree's last run (`compaction_worker.rs:478-484`,
     `retention_iterator.rs:192-200`) and no version is newer than the
     snapshot floor, which is the last flushed sequence when no snapshot,
     transaction, reader or checkpoint is held, as on the shard log.
     An Admin submission is not checked against live jobs at all (only the
     scheduler's own proposals are, `compactor_state.rs:1029-1041`), and a
     submitted spec that shares its destination with a running job panics
     the compaction worker (`compactor_executor.rs:893`; the 100x capacity
     run of 2026-10-24 closed a shard 66 times that way). So a pass submits
     only into a compactor that holds no live job (read from the stored
     compactor state every 250 ms, up to 60 s, the spec built from that
     stored manifest), submits once, and on a refusal, a failure or a
     timeout returns and starts over at the next tick; a job the scheduler
     starts between that read and the submission can still collide until
     SlateDB rejects such a submission (an upstream fix is drafted);
  6. clear debt up to `g` only when stage 4 completes for that compaction,
     checked by a HEAD per superseded SST, never a LIST.
  Work captured later stays outstanding for the next pass, and a pass is not
  complete until stage 4, so residency (below) holds until then.
- **A pass must finish before writers stall.** `compactor_max_concurrent` is
  1 (`src/config/cli.rs:530`) and a pass occupies that slot; under appends a
  flush makes an L0 file about every 5 s and the flush stalls at
  `l0_max_ssts` = 32 (`cli.rs:528`). A pass therefore has about `(32 − L0s it
  names) × 5 s` before appends block, which bounds the bottom run a pass may
  rewrite (about 4.5 GB at 30 MB/s) and so the per-shard bound (§5.7). Raising
  compaction concurrency is a performance decision for the owner. Q0 measures
  pass duration against L0 count and gates on zero L0 stalls.
- **Plan that reaches L0**: `CompactionRequest::Full` and `FullSegment`
  exclude L0 at the pinned SlateDB (`compactor.rs:165, 226-243`, so writers are not
  stalled), so "flush, then request Full" does not collect tombstones still
  in L0. The plan is a staged explicit compaction (`CompactionRequest::Spec`):
  merge the deletion-bearing L0 SSTs into a sorted run, then rewrite through
  the bottom run. Q0 chooses and validates it under concurrent writes.
- **One pass at a time per instance**: SlateDB's compactor and GC run per
  database, so the application can budget only its own submissions; queue
  passes take an instance-wide permit, and the size-tiered schedulers of the
  other databases run as today.
- **Residency**: outstanding queue debt and roster deadlines are debt to the
  billing sweep (§6.8). A shard the sweep opened is evicted after its quantum
  whatever its debt, and the compactor resumes a submitted pass when the
  shard reopens (`compactor_state_protocols.rs`), so the 60-minute completion
  target holds for resident shards; on a rotated shard collection completes
  within its rotations.

**4. Garbage collection (physical deletion of objects).** At the pinned
SlateDB a compacted SST is deleted only when it is older than the cutoff
`min(min_age, compaction low watermark, newest L0 time)`
(`garbage_collector/compacted_gc.rs:243-249`) and unreferenced. With L0
empty, which a pass leaves it, "newest L0 time" falls back to the newest L0
the pass itself consumed (`:184-200`), and that SST fails the strict `<`
until a later flush, which on a quiet shard never comes (the ticker flushes
only when appends changed, `src/shard.rs:1575-1590`). The low watermark also
waits for any long scheduler job that overlaps the pass. **Completion rule
(D22):** stage 4 is complete when every superseded SST is gone except the one
named by `last_compacted_l0_sst_view_id`, a residual of at most one L0 file
(8 MiB) inside Q0's 64 MiB allowance; the alternative, a one-line SlateDB
patch (`<=` in the empty-L0 branch), would stale every formal receipt and is
the owner's to choose only if Q0 shows the residual matters. GC runs on the
owner with the real intervals (`GC_QUIET_INTERVAL` 600 s,
`src/config/model.rs:127`; compacted `min_age` 300 s), so a pass completes
about 15 minutes after its compaction; checkpoint and recovery protections
are unchanged.

**The last rows.** Deleting a queue's last rows (state, group, roster, debt,
reservation) itself creates debt. A shard-level debt row survives every
queue: the batch that clears a queue's final debt deletes that queue's debt
row and releases its reservation, and the shard row records the residual for
the next pass.

**Queue delete.** Deleting a queue is a queue-specific lifecycle, not only a
registry tombstone (`src/application/creation/deletion.rs`):

1. The first serialized transition persists **`deleting`** on the queue's
   state row for that incarnation, with the delete intent and a cleanup
   cursor. From then on sends and redrives are refused (409
   `queue_deleting`), claims return 409 `queue_deleting`, every open lease is
   fenced (`leaseGen += 1`, so settles answer `stale`), and the reservation
   (§5.7) stays held until the queue's debt clears (step 4).
2. The sweep cancels and retires every message, deletes delivery rows, dedup
   entries and failure detail in bounded, resumable batches; each batch adds
   to the deletion debt. A restart or move resumes from the cursor.
3. When nothing live remains, the registry tombstone is written, conditional
   on the same incarnation (a recreated queue under the same name is a new
   incarnation and is untouched).
4. The roster row and the debt are keyed by storage hash, not name, so
   collection (stages 3 and 4) continues after the tombstone; the reservation
   is released when the debt clears.

This answers READINESS TLA-019-F3 for queues: no history rows exist, forks are
refused, billing closes at retirement, deletes are bounded and resumable, and
quiet-shard collection is stage 4.

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

- **Live bytes**: what exists and is counted against capacity.
- **Bytes awaiting reclamation**: retired-but-undeleted (`X`) plus deletion
  debt, reported per queue and shard; the platform's cost, bounded by the
  completion deadline.
- **Physical storage**: the shard database's object-store size, reported on
  `/operator/data.json`.

Live bytes are accounted **by the lifetime of the state**, not all at message
retirement:

| State | Accounting and cleanup |
|---|---|
| message payload and message-owned rows (metadata, `A`, key index, delivery rows and indexes, failure detail) | counted from write; removed from live at the message's retirement; deletion creates debt |
| dedup entries | counted from write against the queue's dedup budget (§8.4) and the stored-bytes cap until their own deletion at the window's end, whether or not the message is done; deletion creates debt |
| queue-wide rows (state, group, roster, debt rows) | a fixed charge per queue for the queue's life |
| `X` and collection bookkeeping | platform overhead, bounded by the completion deadline, not billed |

Billing for a completed message stops at its retirement; independently
retained dedup state is still accounted while it exists (whether its price is
bundled is a product decision, D14).

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

Dedup entries outlive their messages, so they have their own budget:
`maxDedupEntries` per queue (default 1 million, about 100 MiB), reserved
against the shard with the admission budget (§5.7). A send with a new
`dedupKey` beyond it is refused `429 dedup_full`, retryable; entries are
never evicted early, which would silently weaken dedup. The window and the
send rate set the need: 1,000 unique keys a second with a 24-hour window is
86.4 million entries, so such a queue needs a shorter window or a larger
reserved budget.

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
`lastUsefulClaimAt` (set only by a claim that leased at least one message;
empty polls, paused answers and rate-blocked claims do not count),
`lastNotifiedAt` and `notifyBackoffMs`. The sweep notifies when

```text
claimable work remains (due, not paused, within maxInFlight)
AND now ≥ max(lastUsefulClaimAt, lastNotifiedAt) + notifyBackoffMs
AND the project's notification budget admits it
```

A notification the project budget refuses does not update `lastNotifiedAt`.
So a useful claim suppresses notifications only for one backoff interval: a
receiver that drains for its budget and exits with backlog is woken again
with no external request. A notification followed by no useful claim within
the interval, acknowledged or not, doubles the backoff (1 s → 60 s); a useful
claim resets it to 1 s. At most one notification is in flight per
subscription and at most 10 a second per project. The fan-out a receiver
starts is its own choice, bounded by `maxInFlight`. A restart or move re-arms
from the rows. A lost notification costs latency only.

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
  (`lastNotifiedAt`, backoff, failures, `lastUsefulClaimAt`), dedup budget use,
  counters.
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
| Feasibility | Q0 passes every gate of [QUEUES-PLAN.md](QUEUES-PLAN.md) §2 before Q1 starts |
| Cost | the pricing benchmark reports MET: fully loaded cost at most 20% of the Cloudflare bill for the control-plane month ([QUEUES-PRICING-BENCHMARK.md](QUEUES-PRICING-BENCHMARK.md)) |
| Storage | queue storage mode with guarded consumers, the four-stage reclamation lifecycle with quiet-shard collection, accounting by lifetime, reservations, queue delete lifecycle with the `deleting` fence |
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

**Workloads.** First release: plain support sync (the pilot), offer and
spend-limit enforcement (dedup per workspace and hour), Stripe and spend-limit
usage sync (`maxAttempts 11`, backoff 30 s..12 h, `rate`, spread by
`deliverAt`), Vercel usage sync and billing data, notification email (matched
to Brevo), ppg-events-shared, invoice submission and credential cleanup (both
via `deliverAt`), and the Console's Worker consumers whose handlers fit the
Worker drain budget. After Q6: dunning, tenant-manager sync and Bloom turns.

## 13. Verification

The verification plan (TLA+, Kani, DSTs, SDK, Layer B, pilot) is in
[QUEUES-PLAN.md](QUEUES-PLAN.md) §4.

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
| D9 | Admission budget | 256 MiB / 1 million undone messages and 1 million dedup entries per queue by default, reserved against the shard at creation |
| D10 | Request tier | Producer and consumer request budgets sized separately by the §6.5 formula |
| D11 | Overload | A separate settle bucket; #184's append rules |
| D12 | Batches | Atomic ≤ 1,000 messages within the body limit |
| D13 | Dedup | 24 h default, 7 days maximum, never above retention |
| D14 | Billing | Cloudflare parity: $0.40 per million logical operations per message per 64 KB (send a write, each delivery a read, ack or discard a delete), one million a month included; empty claims free; bandwidth and standard retention included; live bytes count against capacity (a message's until retirement, dedup entries until their window ends) |
| D15 | Sequencing | Q0 gates Q1; the first release gates the beta if queues are a beta requirement |
| D16 | Wake-up egress | §9.2 egress policy with a versioned derived secret |
| D17 | Queue storage | Queue pages in the shard database, never absorbed, under queue storage mode |
| D18 | Groups | Only `default` first; named groups under §8.5 |
| D19 | Tail format | A queue storage flag bit in the tail; the first binary writing it is a one-way upgrade for cells with queues (or a `LAYOUT_VERSION` bump, if the owner prefers) |
| D20 | Collection | The compaction trigger in the shared shard database; segments, then a separate queue database, only if Q0 shows shared compaction too costly |
| D21 | Shard capacity | Reserve each queue's budgets at creation, no overcommit; smaller defaults with an opt-up, sized from Q0's per-shard bound; overcommit with a borrowing rule later |
| D22 | Quiet-shard GC completion | Accept one residual L0 file per shard; a SlateDB patch only if Q0 shows it matters |

## 15. Review responses

Every review point of revisions 1-6, what each revision took, and the few
points declined or narrowed with their reasons, are in
[QUEUES-DESIGN-REVIEWS.md](QUEUES-DESIGN-REVIEWS.md).

## 16. Plan and size

The packages Q0-Q6, their sizes and order, and Q0's experiment and gates
are in [QUEUES-PLAN.md](QUEUES-PLAN.md). The first release (Q0-Q5) is about
85-118 engineering days before workload migration, and Q1 starts only when
Q0 passes.
