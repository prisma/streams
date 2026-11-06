# Idle queues — design for review

Status: **proposed 2026-11-03, not implemented.** This document is revision 9
of [QUEUES-DESIGN.md](QUEUES-DESIGN.md) for queues that nobody uses. It
amends §6.8 (the maintenance sweep), §8.1 (retention), §9.2 (wake-up
notifications), §10 (telemetry) and §14 (decisions D23-D28). The changes to
those sections point here. On 2026-11-03 the owner approved the direction
(catch-up on access instead of hourly sweeps, parking after 24 h, wake-ups
stopped after 24 h) and decided D28, billing each wake-up attempt. The
details of D23-D27 are for review. Every client-visible change
becomes an edge record when it lands.

## 1. The problem

A queue costs nothing while it has no pending state and no deletion debt:
`nq_roster_target` (`src/shard/transaction/native_queue/mod.rs:209`) then
removes its roster row, and nothing schedules work for it. While it has
anything pending — messages, leases, retries, failures, dedup entries or
debt — it stays on its shard's roster. Its next sweep is then at its earliest
deadline, or an hour out when no deadline applies (`mod.rs:252`). Nothing
distinguishes a queue in use from one its owner has forgotten:

| Queue | Sweeps until nothing is pending |
|---|---|
| A backlog nobody claims, default 4-day retention | about 96, one an hour, until the last message expires |
| Drained, with deletion debt below the pass floor | up to 24, one an hour, until the collector's daily pass (`MAX_INTERVAL_MS`, `src/shard/queue_maintenance.rs:29`) |
| A backlog with a wake-up subscription nobody answers | one sweep and one notification a minute: the backoff doubles from 1 s to its 60 s ceiling (`src/shard/transaction/native_queue/sweep.rs:279`) and stays there, about 1,440 a day and 5,760 over the default retention |

Each notification is also a write, because the sweep records
`last_notified_at`, and an outbound HTTPS request with a 2 s timeout.

What a sweep of a cold queue costs has not been measured. A cold touch has:
on 2026-11-02, with every request to the store taking 25 ms, a queue cell
served about 14 cold touches a second, each touch waiting on about three
store reads. A million queues touched once an hour needed 32 cells. A million
forgotten queues with a backlog are swept at the same rate, 278 a second. If
a sweep is as costly as a touch, they need comparable capacity while earning
nothing.

§6.8 also lets the billing sweep open a closed shard for a due queue
deadline. Under that rule, every forgotten queue would reopen its shard once
an hour. `probe_debt` (`src/billing.rs:1667-1673`) does not yet count roster
deadlines, so today a closed shard's timers wait until something else opens
it.

## 2. Findings in the current code

This document fixes none of these; each is for the implementation.

- **The sweeper reads only the first 4,096 roster rows.** `queue_roster`
  (`src/shard/queue_maintenance.rs:293-318`) scans the roster from its start
  every second and stops at `ROSTER_PAGE`. The roster is keyed by queue hash,
  so a shard with more than 4,096 queues on its roster never sweeps the rest.
  Their leases do not expire, their retention gaps are not counted, they are
  not woken, and the collector (which reads the same page) never captures
  their debt. Since their debt is never cleared, they never leave the roster.
- **The collector reads every roster queue's state row every 30 s.**
  `capture_debt` (`queue_maintenance.rs:447`) gets the state row of each queue
  on the page to sum its debt, at every `COLLECT_TICK`. The cost grows with
  the roster, not with the debt.
- **Notifications are sent one at a time on the sweeper's tick.**
  `sweep_due_queues` awaits each `notify` in turn. A receiver that takes its
  full 2 s delays every queue after it on the page.
- **The per-project notification budget is not built.** §9.2 allows at most
  10 notifications a second per project. No code applies it.

## 3. The rules

### 3.1 Sweep only at a real deadline (D23)

A queue is on the roster only for a deadline: a lease expiry, a retry cycle,
a failure expiry, a hard expiry, a dedup expiry, an untaken message's
retention end, or a wake-up. The fallback of an hour out goes. Deletion debt
alone puts no queue on the roster.

Debt gets its own index instead: one row per queue with debt,
`DEBT_PREFIX ‖ hash → (generation, bytes, oldest_at)`, written in the batch
that changes the debt and deleted when it is cleared. The collector sums the
index, not the roster's state rows, so its tick reads the queues that have
debt and nothing else.

### 3.2 The roster is ordered by deadline (D24)

The roster key becomes `ROSTER_PREFIX ‖ deadline ‖ hash`, and the state row
keeps the queue's current roster key, so moving a deadline deletes the old
row in the same batch. A tick scans from the start of the prefix up to now,
at most `ROSTER_PAGE` rows, and so reads only due queues, the earliest first.
This removes the starvation of §2 and makes the tick's cost follow the work
that is due.

### 3.3 Catch-up on access (D25)

Every operation on a queue whose roster deadline has passed — send, claim,
settle, the message and group `GET`s, peek, the failed view, redrive,
republish — first runs one sweep step (`nq_sweep`, at most `SWEEP_STEP` rows
for each family) in its own transaction. The client then sees what a queue
swept on time would show. Claims already skip messages past their retention
(`src/shard/transaction/native_queue/claim.rs:198`). The lease step running
first gives an expired lease's message back to retry before the claim reads
the retry index.

A step that fills its budget leaves the queue due one second on, as a sweep
does today, and the operation proceeds with what the step left. A request
never runs more than one step. A queue with a long overdue backlog catches up
over its next requests and the sweeper's ticks, never in one request.

### 3.4 Parking after 24 h without a request (D26)

A queue with no client request for `PARK_AFTER` (24 h) is **parked** at its
next sweep. That sweep records `parked_at` in the state row, and sets the
roster deadline to the queue's next real deadline rounded up to the next
24 h boundary after `parked_at`. A parked queue is swept at most once a day.
The sweep then retires everything due, in bounded steps a second apart while
the steps fill their budget. When nothing is pending, the queue leaves the
roster.

The first client request after parking clears `parked_at`, runs the catch-up
of §3.3, and puts the queue back on its real deadlines.

The rounding is the trade-off. Expired messages, expired failures and ended
dedup windows of a parked queue are retired, and stop counting as live bytes,
up to 24 h late. At D14's storage terms, that is at most one day of retained
bytes that should already have expired. The retention gap of a parked queue
is counted up to 24 h late as well. Under the default 4-day retention, a
forgotten queue with a backlog is swept about 5 times instead of about 96.
Parking needs no new timer: it is a deadline like any other.

A parked queue on a closed shard opens it at most once a day for the billing
sweep's §6.8 service, not once an hour.

### 3.5 Wake-ups go dormant after 24 h without a useful claim (D27)

A sweep that would notify while `lastUsefulClaimAt` is more than 24 h old
(or, if no claim was ever useful, 24 h after the subscription was set) makes
the subscription **dormant** instead. It records `dormantSince` and sends
nothing. A dormant subscription adds no wake-up deadline to the roster. The
next useful claim, or the next send, makes it active again with its backoff
reset to 1 s.

An unanswered subscription thus costs at most about 1,440 notifications,
over one day, instead of one a minute until its backlog expires. The receiver
contract (§9.2) gains a rule: a receiver that comes back after a day of
silence must claim once, or send, before it is woken again.

### 3.6 Each wake-up attempt is billed (D28)

Each wake-up notification the server attempts is one billed queue operation
at D14's rate, counted against the included million like any other. It is
counted whether the receiver acknowledges it, refuses it, times out or cannot
be reached. A notification the project's notification budget (§9.2, not yet
built) refuses is not attempted and is not billed. A subscription's cost is then visible to its
owner and bounded by §3.5: about 1,440 operations for a forgotten
subscription before it goes dormant.

The meter is the sweeper's `notify` call, the one place a notification is
sent. It adds one to `queue_operations` for the queue's stream identity in
the read accumulator, and one to a separate `queue_wakeup_attempts`
dimension so usage can show what part of the operations were wake-ups.
[OBSERVABILITY-BILLING.md](OBSERVABILITY-BILLING.md) §5 has its row.

## 4. Telemetry

- The group's `GET` adds `parkedAt` and `wakeup.dormantSince`. For a parked
  queue, the counters (`due`, `leased`, `retrying`, `failed`, `retentionGap`)
  are as of its last sweep, and the response says so.
- `/operator/data.json` adds parked queues per shard, dormant subscriptions
  per project, roster rows due and overdue per shard, and the oldest overdue
  deadline.
- Usage adds `queue_wakeup_attempts` (§3.6).

## 5. What has to be proven when it is built

- A DST in which a queue with leases, retries, failures, dedup entries and an
  untaken backlog is parked for 25 h of simulated time and then claimed. The
  claim, its returned messages, the retention gap and the stored rows equal
  those of the same queue swept on time.
- A shard with 10,000 queues on its roster serves every due deadline within
  one rotation of the page, with none starved.
- Catch-up bounds: a claim on a queue with 10,000 expired leases runs one
  step and returns, and the queue drains over the following ticks.
- Dormancy: an unanswered subscription stops after 24 h, and a send or a
  useful claim makes it active again.
- Billing: every attempted notification is metered once, including timeouts
  and refusals by the receiver. A notification refused by the project budget
  is not metered.
- A cost run: 1,000,000 queues created, half holding one unclaimed message,
  then left without a request for 26 h. It reports sweeps an hour, store
  reads, memory and CPU before and after parking.

## 6. Edge changes when it lands

- Wake-up dormancy after 24 h without a useful claim (§3.5).
- Billing for each wake-up attempt (§3.6).
- `parkedAt`, and counters as of the last sweep, on a parked queue's group
  `GET` (§4).
- Retirement of a parked queue's expired data up to 24 h late (§3.4).
