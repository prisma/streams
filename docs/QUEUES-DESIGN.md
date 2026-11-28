# Native queues in Prisma Streams — design for review

Status: **revision 9, 2026-11-16. S3b (parts 1 and 2) replaced the storage
model and reclamation: §5.1-§5.3, §5.5-§5.7, §6.1, §6.8, §7.1 and §8.2-§8.3
now describe the code as built (edges #246, #247, #262); the rest stands as
revision 8 approved it.** Revision 8 (2026-10-23) approved the architecture
and the Q0-first sequence (Q0, [QUEUES-PLAN.md](QUEUES-PLAN.md) §2, the first
implementation package) and folds in Fable 5.1's review of revision 7
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
capacity reservations. §15 maps every review point. Revision 9 (2026-11-03,
proposed, not implemented) is [QUEUES-IDLE.md](QUEUES-IDLE.md): sweeps only
at real deadlines, a roster ordered by deadline, catch-up on access, parking
after 24 h without a request, dormant wake-ups and billed wake-up attempts
(D23-D28); it amends §6.8, §8.1, §9.2, §10 and §14. Every client-visible
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

All queue state lives in the owning **shard database**, written by the shard
committer and durable on the same watermark as appends. A queue's messages
are the pages of its own stream (`h16 ‖ 'S' ‖ …`), sealed into segment
objects like any stream's (S3b part 2); its other rows are keyed
`h16 ‖ 'Q' ‖ family ‖ suffix`, its tail among them (layout 8, SPEC.md §3.3).
Index keys are big-endian, so key order is numeric order.

### 5.1 Message record

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

There is **no metadata row** (S3b part 1). A message's facts come from its
page and its offset: `availableAt = max(enqueuedAt, deliverAt)` is its page
header's time, `retentionEnd = availableAt + maxAge` and `hardExpiresAt =
retentionEnd + failureRetention` are derived as the send computes them, and
its bytes are the stored page's length. What changes per message lives in
three places:

- a **range row** `'x'` per 4,096 offsets, present only while it holds
  something: the count of its open deliveries and of its unresolved scheduled
  messages, and three bitmaps: `skip` (scheduled offsets at or past the
  frontier, which the frontier passes without leasing), `cancel` (immediate
  offsets at or past the frontier cancelled untaken) and `sbits` (the
  unresolved scheduled offsets, wherever they are), each stored as runs or
  raw (512 B), whichever is shorter;
- a scheduled message's `availableAt` by offset (`'m'`);
- the delivery row of an open delivery (§5.3), which carries the message's
  facts.

The dedup tag rides in the dedup entry (§8.4), keyed by its HMAC.

### 5.2 The frontier and the scheduled index

**An untaken immediate message costs no row** (S3b part 2): its page is
all there is. The queue's **frontier** is the lowest offset not yet
processed for its first delivery; one frontier serves the default group
(F9). A claim walks the lane from the frontier over the stream's pages:
each page is leased, or passed when the range row marks it `skip` or
`cancel`, or expired into the retention gap when its `retentionEnd` passed.
No page lies at or past the stream's next offset.

A **scheduled** message (a future `deliverAt`) is the exception: its send
writes an entry in the scheduled index `'a'`, keyed `(availableAt, offset)`,
and sets its `sbits` and `skip` bits, so the frontier passes it and a claim
takes it from `'a'` once due. An entry carries the page itself when the
sealed page is at most 2 KiB (`INLINE_PAGE_MAX`) and the queue's inline bytes
stay under its 64 MiB budget (`SCHEDULED_INLINE_BUDGET_BYTES`), so a claim of a due scheduled message reads no page. A cancel deletes the entry.

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
| frontier | every offset below it has been taken, passed or expired (§5.2) |
| scheduled cursor `(cursor_at, cursor_off)` | the scheduled index's entries below it have been taken |
| delivery row per **open** delivery | `{state: leased \| retrying \| failed, enqueuedAt, availableAt, bytes, scheduled, inline, cycle, cycleDeadline, attempts, releases, internalUnleases, leaseGen, leaseDeadline, dueAt, firstDeliveredAt, failedAt, failureExpiresAt, cause?, code?}`: the message's facts ride in it |
| lease index `L` `(leaseDeadline, msgId)` | leased deliveries |
| retry index `R` `(availableAt, msgId)` | retrying deliveries |
| failure index `F` `(failedAt, msgId)` | failed deliveries, for listing |
| failure-expiry index `E` `(failureExpiresAt, msgId)` | failed deliveries (§7.1) |
| failure detail (encrypted) | the worker's error text, ≤ 1 KiB, when a settle carried text and the key |
| counters | delivered, acked, retried, failed, expired, hard-expired, cancelled, discarded, redriven, stale settles |

A message with no delivery row is at or past the frontier, scheduled, or done. Group state is proportional
to work in flight, in retry and in failure, not to the backlog. There is no
`maxPending`.

### 5.4 Bounded memory

Not built as a ready cache (revision 9). The committer keeps no per-group
cache of due entries: a claim reads the indexes through the group's stage,
bounded by the index floors (§6.8) and helped by the commit group's warm-up
of cold queues (§6.1), and the lane's pages are the handle's unsealed ones
or those a handler prepared (§5.6).

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
  moves.
- A send commits at the stream's next offset, above every taken one, so **a
  new message is never at or below the frontier** (TLA-046
  `NoSendAtOrBelowFrontier`).
- Queue routes refuse the `stream-timestamp` hint (400).
- Clocks are assumed within 5 s, as lease handoff already assumes; RUNBOOK
  states the bound.

### 5.6 Queue streams on segments

A queue's pages are its stream's pages (S3b part 2). A send stages them into
the stream's pending row, as an append does; the tail (in the queue segment,
`h16 ‖ 'Q' ‖ 't'`) keeps `absorbed`, and the unsealed index names the queue
while it holds unsealed pages. The sealer takes a queue stream like any
other, with two rules of its own (`shard/seal/queue_cut.rs`): a queue chunk
holds at most 1 MiB of pages and never crosses a range row's 4,096-offset
span, so a chunk's references are one range row's and a claim's handler
reads one ranged GET per chunk; and every page must admit at its own offset,
or the queue is not sealed (F5). A queue has no billing row and no usage
final (F12).

**Where a walk's pages come from.** The committer serves the lane from the
pages it holds: the handle's unsealed pages and the group's own. Once a seal
takes a queue's pages the committer holds none of them, and no store read
for a queue body runs in the committer: a claim's handler, a peek, a read by
id and the sweeper read the sealed pages they may need through the stream
read path (G2) and pass them in their operation
(`shard/native_queue_submit/prepare.rs`). The committer serves a prepared
page only at its own frontier, which only moves on, and drops the rest; a
walk stops at the first offset it holds no page for, so it never steps over
a message. A sweep whose lane is due below the sealed boundary answers
`lane_sealed`; only then does the sweeper prepare the pages and sweep again.

| Consumer | Queue behaviour |
|---|---|
| the sealer (`shard/seal/steps.rs`, `seal/queue_cut.rs`) | seals queue pages into queue chunks (above) |
| the unsealed index, the maintenance ledger and stall latch | count a queue's unsealed pages as any stream's |
| the open's index read and repair (`shard/open_index.rs`) | read a member's tail by stream key, else queue key |
| billing | none: no billing row, no usage final |
| stream read paths | refuse queue streams; queue views read through `prepare.rs` |

### 5.7 Per-shard queue state

A shard's queue state is bounded by `BOUND_BYTES` (2 GiB, provisional) in
five slices, one per category, so no category takes another's room (edge
#246; `shard/layout/queue_gauges.rs`):

| Category | Rows | Slice |
|---|---|---|
| active | open deliveries (leased, retrying) and their lease or retry entries | 30% |
| failed | failed deliveries and their failure entries | 20% |
| delayed | scheduled entries, their by-offset rows and inline pages | 20% |
| dedup | dedup entries and their expiry entries | 20% |
| cleanup | range rows | 10% |

At a category's line the refusal is per container first, then per queue: a
container at or over its share of the slice, and within it a queue at or
over its share of the container's bytes, has its claims lease nothing new
(active, failed: the claim answers `limited: "shard_queue_state"`) or its
scheduled sends (delayed) or sends with fresh dedup keys (dedup) refused
with `429 shard_queue_state_full`, `Retry-After: 5`. Cleanup refuses nothing
(what creates it is never refused); it alarms at 80% of its slice. A queue's
bytes per category follow from its state row's exact counts at one fixed
weight per message, and its range rows' stored bytes; the committer keeps
the shard's and every container's sums as `'N'` rows in the batch that
changes them. Message pages are not queue state: they are the stream's,
sealed and collected like any stream's (§8.2).

Per queue, the caps are set at creation (S3b-DESIGN §8.1; edge #247):
`maxQueuedBytes` and `maxQueuedMessages` (no default), `maxOpenDeliveries`
(leased plus retrying), `maxFailedMessages` and `maxScheduledMessages`
(default 1,000,000 each, clamped to 1,000-10,000,000). Reservations at
creation, `429 queue_capacity` and D21 are not built (SCALING-DECISIONS Q11).

## 6. Delivery

### 6.1 Claim

`claim(group, max, maxBytes, leaseMs, waitMs)` runs in two steps.

**Before the committer (with the key):** when the lane's head is sealed, the
claim's handler reads the sealed pages from the stored frontier on (up to
its `maxBytes`) through the stream read path and passes them in its
operation (§5.6). It holds read memory for exactly the bytes it read while
the committer serves them, released when the committer answers, before the
reply's own reservation and before a park; with no room by its wait deadline
it answers `503 read_memory_busy`, `Retry-After: 1`, before it leases
anything (edge #262). When a seal raced its read it reads again, at most
three times.

**In the committer (keyless):** lease the due work of three sources in
`(due, offset)` order, within `maxInFlight`, `rate` and the byte limit (by
stored page length; default 1 MiB, at most 8 MiB; one message always
allowed): due retries (`R`), due scheduled messages (`'a'`, from the
scheduled cursor) and the lane, the immediate messages from the frontier,
each due at its send. The frontier passes the scheduled and cancelled
offsets it meets, expires an untaken message past its retention end into the
retention gap (§8.1), and never passes a live untaken message without
leasing it; an entry past its **cycle deadline** (§6.3) is not leased. A
lease writes the delivery row (`leased`, `attempts += 1`, `leaseGen += 1`,
its deadline), its `L` entry and the range row's open count;
`lastUsefulClaimAt = t` when at least one message was leased. An index
whose floor is past the claim's clock is not read (§6.8), and an empty claim
writes nothing. The committer runs a shard's operations one at a time, so a
cold queue must not wait for storage on that path: before a commit group
stages, it reads what its operations on cold queues will read (each queue's
state row, a claim's and a sweep's walks of the indexes, a settle's rows),
64 queues at once (`shard/transaction/native_queue/warm.rs`), and the
committer's scans keep sixteen block fetches in flight.

**After the commit is durable (with the key):** open each leased envelope
from the page the committer returned: the lane's page, the scheduled
entry's inline page, or a page read by its offset through the stream read
path. A page whose envelope does not open fails its delivery alone, cause
`unreadable` (§7.1); a failed read releases those deliveries server-side
with the attempt given back and answers 503, retryable.

With nothing due and `waitMs > 0`, the claim parks until a send, settle, lease
deadline or the next `availableAt`, up to 25 s. A paused group answers at once
with `paused: true`. While it waits the request is parked in the live pool
and its project's share, out of the in-flight write gate's active count
(edge change #194). A claim that leased messages reserves exactly their
bytes in the instance's read memory and its project's read bytes before it
reads them, waiting up to 2 s for room; without room it releases its leases
with their attempts given back and answers 503 `read_memory_busy` or 429
`container_memory_pressure`, retryable after 1 s (#195). Its answer holds its
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
  invalid is charged to the main bucket. Both are edge changes. The settle
  bucket is not built yet. Under the RSS shed a settle that only ends leases
  (ack, release, retry, fail, discard) passes up to the hard line, the line
  x 9/8, and one with an `extend` follows the append rule (#191 as amended,
  the lead's ruling of 2026-10-27 under the owner's delegation).
- **Overload parity** (layout 6A package 1, edge changes #188-#193, built):
  the pre-auth survival bound covers `/v1/queues`; a verified queue request
  is bound to fair shedding after project admission, so a project over its
  share of the cell's requests is refused on every queue route over the RSS
  shed line and at the in-flight cap; a queue `POST` is refused while its
  project is over its memory line and every queue body is charged to the
  project while it is buffered; sends and batches meet the RSS write gate
  before their bodies, settles as §6.5's settle bucket bullet says, and all
  three the in-flight write gate once buffered, with the append's spared
  exceptions; and a request's committer operation is
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

A queue may cap its live, undone messages with `maxQueuedBytes` and
`maxQueuedMessages`, set at creation with no default (edge #247); messages
held only by failed deliveries do not count. The shard's queue state is
bounded by category slices instead (§5.7). A send beyond the cap is refused
`429 queue_full`,
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
- **Cancel** `(msgIds)`: a held message ends (its holder sees `stale`); an
  unresolved scheduled one ends; an untaken immediate one is marked in its
  range row's `cancel` bitmap, and the frontier passes it and takes its bytes
  off the queued gauge then (a cancel reads no page). A message is cancelled
  once; counted, audited.
- **Peek**: open deliveries, unresolved scheduled messages and the lane's
  untaken messages from the frontier, by offset, paginated, without leasing;
  needs the key; `queues.inspect`. On a sealed queue a page ends where its
  handler's read of sealed pages reached, with a cursor; a view a seal races
  three times answers `503 read_failed` (edge #262).

### 6.8 The maintenance sweep

Each queue has a keyless **maintenance sweep** in its owning committer. Its
tasks, each in bounded batches (at most 256 entries a step):

1. **Lease expiry**: heads of `L` past their deadline count as a failed
   attempt (`lease_expired`), `leaseGen += 1`, to `R` on clamped backoff.
2. **Retry cycles**: retries past their cycle deadline fail.
3. **Retention expiry**: untaken scheduled messages past their retention end
   leave `'a'`, and the lane's untaken messages past theirs are passed by the
   frontier, both into `retentionGap {from, to, count}` (counted, alerting).
   The lane is walked only once its `lane_expire_at` (a lower bound of the
   frontier message's retention end) is due; below the sealed boundary the
   sweep answers `lane_sealed` and the sweeper passes the prepared pages
   (§5.6).
4. **Failure expiry**: heads of `E` with `failureExpiresAt ≤ t` are discarded.
5. **Dedup expiry**: dedup entries past their window are deleted, and the
   deletion adds collection debt like any other (§8.2).
6. **Wake-up notification** (§9.2).

Each step writes only when something moved; resolved messages' rows go in
the batch that resolves them, and their pages stay in the stream until the
trim gives their chunk back (§8.2).

**Discovery after restart or move.** Each shard keeps a **queue roster**: one
row per queue with pending work, `DIRTY_SENTINEL ‖ 'Q' ‖ hash → next
deadline`, written in the same batch as the change that sets the deadline and
deleted when nothing is pending. A new owner reads the roster at open in
bounded pages (within the 180 s open deadline), builds a per-shard timer heap,
and each queue's sweep resumes from its durable cursors. Roster keys are not
hash-first, which a future key-range split must handle. Roster rows are
counted in §8.3.

**Index floors.** Every lease, retry and failure is put and later deleted,
so the time-ordered indexes (`'a'`, `L`, `R`, `E`, the dedup expiries) carry
one tombstone per delivery until a compaction reaches the last sorted run,
and a scan from an index's start walks all of them. The state row therefore
keeps, per index, a floor: a lower bound on the time of its first live
entry (for the scheduled index `'a'`, of its first live entry past the
scheduled cursor), plus the earliest cycle deadline among retries. Every put lowers a floor in the batch that
holds the entry; a scan starts at the floor and may raise it to the first
live entry it found, in the batch whose deletes justified it; deletes leave
it low, which is only slower. A claim does not read `'a'` or `R` while that
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

**Idle queues (revision 9, proposed).** [QUEUES-IDLE.md](QUEUES-IDLE.md)
removes the fallback of an hour out: a queue is on the roster only for a
real deadline, and deletion debt gets its own index for the collector (D23).
The roster is keyed by deadline, so a tick reads only due queues (D24). An
operation on an overdue queue runs one sweep step first (D25). A queue with
no request for 24 h is parked and swept at most once a day until nothing is
pending (D26).

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
nobody consumes within `maxAge` loses what it did not take, counted. A parked
queue's expired data is retired, and its gap counted, up to 24 h late (D26,
[QUEUES-IDLE.md](QUEUES-IDLE.md) §3.4).

### 8.2 The reclamation lifecycle

Reclamation is four stages for a queue's rows, each durable, each with its
own completion test, and one for its pages (S3b; TLA-047).

**1. Resolution (logical, exactly once).** A message is resolved when it is
acked, discarded, failure-expired, cancelled, hard-expired, or expired
untaken (passed by the frontier into the retention gap, or its scheduled
entry ending); then nothing refers to it: no delivery row, no scheduled
entry, no `sbits` bit, and it is behind the frontier. Each path resolves it
in its own committer batch, which moves the gauges by the message's exact
stored size (I7) and deletes its rows, so no message resolves twice,
including across a restart or move. `GET …/messages/{id}` answers `404
reclaimed` for an offset behind the frontier that no row holds.

**2. Row deletion and debt.** Every batch that deletes queue rows (a
resolution, a dedup expiry, failure detail, the queue delete) adds their
bytes to the queue's collection debt in its state row (`debt_bytes`,
`debt_oldest_at`, `debt_generation`, the generation rising with each), so no
tombstone is written without a collection obligation. The debt is rows only:
a message's page is not a row (§5.6).

**Pages: the trim, the cleaner and the purge.** A sealed queue chunk
`[f, l]` is retired when `l` lies behind the frontier, no delivery row lies
in it and no `sbits` bit: one unresolved message retains exactly its own
chunk. Retirement is permanent. The queue's needed set only shrinks (the
frontier only moves on, a reference is only created at or past it, and a
resolved one never comes back), so the gc may retire anything outside any
durable reading of it, reading the frontier first and the references after,
with no lock against the committer.

- **Hints.** A committer batch that moves the frontier over sealed offsets,
  resolves a reference at a sealed offset, or seals a queue's pages puts a
  hint row `FF×16 ‖ 'Z' ‖ gseq ‖ h16` naming them (coalesced per 4,096-offset
  span, at most 8 ranges a row), under a shard-wide sequence seeded above
  every hint and the trim's cursor.
- **The trim pass** (`shard/seal/gc/trim.rs`, a step of every gc pass) takes
  `refs`, scans the hints after its cursor `FF×16 ‖ 'L' ‖ 'Z'` (at most
  4,096), reads each named queue's frontier, then its candidate chunks'
  references, and writes one batch through `write_index_batch` under the
  maintenance gate: the retired chunks' decrements; the queue's `'H'` row in
  state **Trimming** with its prefix `reclaimed`; its released intervals above
  the prefix (`'k'`, `h16 ‖ 'Q' ‖ 'k' ‖ last → first`); the hints it consumed
  deleted and the chunks it left hinted again; the cursor. The objects then
  follow the segment GC (stage 4).
- **Every reader of `'H'` and `'k'`** applies the prefix and the intervals in
  every `'H'` state (`'k'` outlives Trimming until the purge's last
  decrement): the folds and merges drop released entries, a split's recount
  counts none of them, `purge_due` skips a Trimming row, and a queue read
  offers no chunk to consolidation.
- **The cleaner** (`shard/seal/gc/clean.rs`) rewrites an object a live
  queue's chunks keep stored. A queue is a hash with a queue state row. Live
  (`'H'` absent or Trimming, not deleting), its live set is its live entries
  less what its trim released, its `'H'` row and `'k'` read again under
  `refs` before the copy commits, and its chunks are never joined in the
  copy; Requested, Purged or deleting, it is the purge's. An object with a
  free queue chunk waits for the trim. The saving counts a month when the
  copy holds an ordinary chunk, else until its queue chunks' latest hold
  (an open delivery's hard expiry, a scheduled bit's `now + 2 × retention +
  failure retention`, a chunk at or past the frontier's `now + retention +
  failure retention`), at most a month.
- **A deleted queue's purge** puts its head at the stream's next offset and
  deletes its tail, then reads `'k'` before it deletes a row or decrements
  anything, decrements every chunk in `[reclaimed, head)` outside the
  intervals, and deletes `'k'` in the batch that sets `reclaimed = head`
  (F4): decremented twice, an object shared with another stream would reach
  zero while that stream still needs it.

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
  completion deadline (target 60 minutes from deletion) bounds when the
  objects must be gone; the launch age must leave room for deletion, flush,
  compaction, checkpoint protection and GC inside it. Q0 measures a pass's
  duration and sets the launch age to `deadline − p99 pass duration − margin`
  (first estimate 15 minutes).
- **Layout 7 (2026-10-30, the lead's ruling under the owner's delegation;
  QUEUE-TREE-DECISION appendix A):** queue and stream rows share one tree, so
  a pass rewrites the whole shard log, and the trigger is only the amount
  (256 MiB of debt on the shard) or the maximum interval (24 h): the launch
  age, the 16 MiB floor and the twentieth-of-the-log floor are gone, because
  each pass now also rewrites the stream rows (about 0.13 GB on a 200-S
  cell, 0.6 GB on an M×2 cell at the step-2 seal cadence). The completion
  deadline is therefore 24 h for debt below 256 MiB, and a shard's bytes
  awaiting reclamation stay under 256 MiB plus what accrues during one pass,
  so the physical bound of §5.7 does not grow.
- **Coverage rule.** A pass covers a defined set of work, and the deletions
  that implement it, before it clears anything:
  1. capture the debt generation `g` and its pending bytes: every deletion
     already in the debt is covered (a resolving batch deletes its rows and
     records their debt together, so nothing waits between the two);
  2. flush the **memtable**, `flush_with_options(FlushType::MemTable)` as the
     ticker does (`src/shard.rs:1583-1590`; `Db::flush()` flushes only the WAL
     when the WAL is on, `slatedb/src/db.rs:1779-1789`), and read the boundary
     from `Db::manifest()` when it returns;
  3. submit one spec through `Admin::submit_compaction` (`admin.rs:195-209`):
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
  4. clear debt up to `g` only when stage 4 completes for that compaction,
     checked by a HEAD per superseded SST, never a LIST; the clear keeps
     what was recorded after the capture (`ClearDebt` by generation).
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

**The last rows.** Deleting a queue's last rows (state, group, roster)
itself creates debt, which the shard's collector carries for the next pass.

**Queue delete.** Deleting a queue is a queue-specific lifecycle, not only a
registry tombstone (`src/application/creation/deletion.rs`):

1. `BeginDelete`, the fence, persists **`deleting`** on the queue's state row
   for that incarnation. From then on sends and redrives are refused (409
   `queue_deleting`), claims answer 409 `queue_deleting`, and every open lease
   is fenced (settles answer `stale`).
2. `DeleteStep`s, bounded and resumable, delete every queue row family but
   the released intervals `'k'`; each adds to the debt. The last deletes the
   state row and the roster row, and the queue stream's pending row with its
   unsealed pages.
3. The stream is tombstoned, conditional on the same incarnation (a queue
   recreated under the same name is a new incarnation and is untouched).
4. The purge, keyed by storage hash, then reclaims the queue's sealed chunks
   in F4's order (above), and the collection of its rows (stages 3 and 4)
   continues after the tombstone.

This answers READINESS TLA-019-F3 for queues: sealed chunks are reclaimed by
the purge in F4's order, forks are refused, a queue has no billing row,
deletes are bounded and resumable, and quiet-shard collection is stage 4.

**The queue tree (layout 6).** Queue rows live in their own LSM segment of
the shard log, `'Q' ‖ generation` (`src/keyspace.rs`; generation 0 only), so
a collection pass reads and rewrites only queue data and the stream tree's
compactions never wait on queue churn. The size-tiered scheduler never
compacts the queue tree (`native_queue::compaction_guard`); the collector
owns it twice over:

- **L0 merges** (`shard::queue_maintenance::l0_merge`): once the tree holds
  16 L0s (half the 32-L0 line), one merge takes every L0 and the newest runs
  no larger than twice what it already holds, so runs stay geometric. It
  submits only into a compactor with no live queue-tree job, does not poll
  the job, and looks again after 10 s if the merged L0s are still there (a
  refused or failed merge is retried then); a fresh destination sits 16 run
  ids above the scheduler's next, so a stream-tree job planned at the same
  moment never takes the same id.
- **Debt passes** (stages 3 and 4 above), which also split a pass whose L0s
  and rewrite time at the measured rate would reach the 32-L0 line: the L0s
  into a fresh run first, then every run into the last run.

Measured on a mixed cell (spike A2, Class A/B at half list): the queue tree
costs $0.25 (10x) to $0.72 (1x) per shard-month at a 10 s flush cadence; a
queue-only shard writes no stream-tree L0.

**Escalation if shared compaction is too costly.** If the queue tree's passes
still miss their deadline within latency and memory budgets: a separate queue
database per shard holding **all** authoritative queue state, with the same
scheduling and GC requirements.

### 8.3 Accounting

Three quantities are kept apart:

- **Live bytes**: what exists and is counted against capacity.
- **Bytes awaiting reclamation**: the rows' deletion debt, reported per
  queue and shard, and sealed chunks no trim has retired yet; the platform's
  cost, bounded by the completion deadline and the trim's passes.
- **Physical storage**: the shard database's object-store size, reported on
  `/operator/data.json`.

Live bytes are accounted **by the lifetime of the state**, not all at message
retirement:

| State | Accounting and cleanup |
|---|---|
| message pages (the queue stream's) | counted in the queued gauge at stored size from the send, taken off at resolution; physically collected by the trim and the purge (§8.2) |
| message-owned rows (delivery rows and their indexes, scheduled entries and their by-offset rows, failure detail) | counted from write in their category (§5.7); deleted at resolution; deletion creates debt |
| dedup entries | counted from write against the queue's dedup budget (§8.4) and the stored-bytes cap until their own deletion at the window's end, whether or not the message is done; deletion creates debt |
| queue-wide rows (state, group, roster, debt rows) | a fixed charge per queue for the queue's life |
| range rows, hints, released intervals, the Trimming row and collection bookkeeping | platform overhead, bounded by the completion deadline and the trim, not billed |

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
`maxDedupEntries` per queue (default 1 million, about 100 MiB), counted in
the shard's dedup slice (§5.7). A send with a new
`dedupKey` beyond it is refused `429 dedup_full`, retryable; entries are
never evicted early, which would silently weaken dedup. The window and the
send rate set the need: 1,000 unique keys a second with a 24-hour window is
86.4 million entries, so such a queue needs a shorter window or a larger
budget.

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

**Dormancy and billing (revision 9, proposed).** A subscription with no
useful claim for 24 h goes dormant and is not notified until the next useful
claim or send (D27). Each attempted notification is a billed queue operation,
whatever the receiver answers (D28). See [QUEUES-IDLE.md](QUEUES-IDLE.md)
§3.5-§3.6.

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
- Usage: queue operations metered as today, plus read and stored bytes, and
  wake-up attempts (D28).
- Idle queues: `parkedAt`, `wakeup.dormantSince`, and the operator view's
  parked and overdue counts ([QUEUES-IDLE.md](QUEUES-IDLE.md) §4).

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
| D14 | Billing | Cloudflare parity: $0.40 per million logical operations per message per 64 KB (send a write, each delivery a read, ack or discard a delete, each attempted wake-up notification an operation under D28), one million a month included; empty claims free; bandwidth and standard retention included; live bytes count against capacity (a message's until retirement, dedup entries until their window ends) |
| D15 | Sequencing | Q0 gates Q1; the first release gates the beta if queues are a beta requirement |
| D16 | Wake-up egress | §9.2 egress policy with a versioned derived secret |
| D17 | Queue storage | Superseded (revision 9, S3b part 2): a queue's pages are its stream's, sealed into segment objects and given back by the trim (§5.6, §8.2) |
| D18 | Groups | Only `default` first; named groups under §8.5 |
| D19 | Tail format | Superseded (revision 9): layout 8 keeps a queue's tail in the queue segment (`h16 ‖ 'Q' ‖ 't'`); no flag bit |
| D20 | Collection | The compaction trigger in the shared shard database for queue rows; queue pages are sealed into segments and trimmed (revision 9); a separate queue database only if shared compaction proves too costly |
| D21 | Shard capacity | Superseded (revision 9, edge #246): five category slices of the shard's queue-state bound, fair per container then per queue; no reservations at creation (SCALING-DECISIONS Q11) |
| D22 | Quiet-shard GC completion | Accept one residual L0 file per shard; a SlateDB patch only if Q0 shows it matters |

Revision 9 adds D23-D28, proposed in [QUEUES-IDLE.md](QUEUES-IDLE.md). The owner
approved their direction on 2026-11-03 and decided D28; the details of
D23-D27 are for review.

| # | Decision | Recommendation |
|---|---|---|
| D23 | Sweep schedule | Only real deadlines put a queue on the roster; no fallback of an hour out; deletion debt in its own index |
| D24 | Roster order | Keyed by deadline, so a tick reads only due queues, the earliest first |
| D25 | Catch-up | An operation on an overdue queue runs one sweep step first, never more than one |
| D26 | Parking | After 24 h without a client request, swept at most once a day; expired data retired up to 24 h late |
| D27 | Wake-up dormancy | Dormant after 24 h without a useful claim; active again on the next useful claim or send |
| D28 | Wake-up billing | Each attempted notification is one billed queue operation at D14's rate, whatever the receiver answers; one the project budget refuses is not billed |

## 15. Review responses

Every review point of revisions 1-6, what each revision took, and the few
points declined or narrowed with their reasons, are in
[QUEUES-DESIGN-REVIEWS.md](QUEUES-DESIGN-REVIEWS.md).

## 16. Plan and size

The packages Q0-Q6, their sizes and order, and Q0's experiment and gates
are in [QUEUES-PLAN.md](QUEUES-PLAN.md). The first release (Q0-Q5) is about
85-118 engineering days before workload migration, and Q1 starts only when
Q0 passes.
