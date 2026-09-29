# Next work after the second external review (handoff)

State: `slate` at `a079315c` (pushed 2026-09-25), plus this handoff commit. Read
`README.md` in this directory first: its decisions table is the owner's
record of the second external review of 9813d1cb, and `edge-changes.md` holds
every client-visible change (#1-#57). This file lists what is **not done**,
in the owner's priority order, with enough context to start without the
previous session.

Contents:

- [0. How work is done in this repository](#0-how-work-is-done-in-this-repository)
- [1. Composed test: a refused chain's overlapping pages read exactly](#1-composed-test-a-refused-chains-overlapping-pages-read-exactly)
- [2. Closure debts: a paging bug and missing tests](#2-closure-debts-a-paging-bug-and-missing-tests)
- [3. Typed registry read error for an append's first read](#3-typed-registry-read-error-for-an-appends-first-read)
- [4. Item 40: heartbeat, controller progress, serving eligibility, drain](#4-item-40-heartbeat-controller-progress-serving-eligibility-drain)
- [5. F1: two different things carry this name](#5-f1-two-different-things-carry-this-name)
- [6. F2: composition test for a retiring engine's written group](#6-f2-composition-test-for-a-retiring-engines-written-group)
- [7. Bug #7: rollup re-keying by physical segment, as an explicit migration](#7-bug-7-rollup-re-keying-by-physical-segment-as-an-explicit-migration)
- [8. Stale-page repair after a refused chain](#8-stale-page-repair-after-a-refused-chain)
- [9. Release-wide mutation campaign (work package 4)](#9-release-wide-mutation-campaign-work-package-4)
- [10. Smaller items](#10-smaller-items)
- [11. Deployment gates (need the owner or the Compute owner)](#11-deployment-gates-need-the-owner-or-the-compute-owner)
- [12. Formal verification: new obligations and what blocks them](#12-formal-verification-new-obligations-and-what-blocks-them)
- [13. Configuration: fewer settings, and defaults that are what production runs](#13-configuration-fewer-settings-and-defaults-that-are-what-production-runs)

---

## 0. How work is done in this repository

The working agreement, environment, verification ladder, ratchet rules,
formal and mutation recipes, records and known traps that used to live here
are in the repository's operating manual, [`AGENTS.md`](../../../AGENTS.md),
which every agent loads. This file is only the queue of open work.

---

## 1. Composed test: a refused chain's overlapping pages read exactly

**Done: b059e4a2** (`history::bounded_discovery_tests::a_refused_chains_overlapping_pages_still_read_exactly`).
The merge of `slate-kani-and-tla` changed the premise below: readers now admit
an overlapping page whose offsets agree with those already admitted (d16559b3),
so the refused-chain state no longer falls back to the envelope scan. The test
pins exact reads through `read_history2` and `read_history2_keyed_cached` and
honest paging under a small budget. The text below is kept as the original
handoff.

**Owner decision.** Part of closing HOLD-SPLIT-500: "Add the composed test that
creates the specific refused/chained-advance state and reads it through the
public path. Preserve exact filtering, honest continuation cursors, and
resource bounds."

**Background.** The absorber fix (5a6f9f56..b2a8001f) leaves a known residual:
when a refused commit group carried a stream's advance and a later advance of
the same stream was chained behind it, the heal regathers from the durable
boundary while the chained advance's postings pages (flushed before submit)
stay. The pages overlap, so a keyed read counts `POSTINGS_CORRUPT` and serves
the bounded canonical envelope scan (`read_history2_keyed_envelope`,
`src/history.rs`). Records and ledger stay exact. No test composes this state
with a read.

**The state.** The review's probe builds it; it is in
`evidence/refused-chain-probe.patch` (test
`probe_a_refused_chain_heals_with_overlapping_pages`, written against
`src/history/bounded_discovery_tests.rs`, whose helpers it uses: `rig`,
`append_stored`, `enqueue_record`, `wait_until`, `stored_len`,
`applied_tail`, `pages_tile`, `engine.test_hold_commit()`,
`engine.fail_next_absorbed_group()`, `engine.group_failures_tripped()`).
Steps: 4 durable records; hold the WAL put and apply 4 more; hold the commit
gate; G1 gathers [0,4) and parks; release the WAL; G2 gathers [4,8) (chained
from the mark); arm the group failure and drop the gate so G1+G2 are refused;
append offset 8; G3 heals [0,9) from the durable boundary; append offset 9.
Result: pages 0:[0,9) and 4:[4,8) overlap, ledger exact, one
`POSTINGS_CORRUPT` per keyed read.

**What to write.** A real test (not a probe) that:
1. Builds that state and asserts it as a precondition: `!pages_tile(..)`
   (comment that the precondition flips when item 8, stale-page repair,
   lands).
2. Reads key `""` over [0,10) through `read_history2` and through
   `read_history2_keyed_cached`, which the public keyed read calls
   (`src/application/read.rs`), with a large budget: exactly offsets 0..9, each once, in order;
   `POSTINGS_CORRUPT` advanced.
3. Pages the same range with a small `max_bytes` (a few frames per call),
   continuing from the returned cursor: the envelope fallback returns
   `(frames, last, completed)`, where `last` is the last offset scanned and
   `completed` is false when the budget stopped it. The union must be exactly
   0..9, with no duplicate and no skip, and every call must stay within its
   budget.
4. A second key with records interleaved, to prove exact filtering (the
   fallback compares routing-key bytes).
5. Ideally the same through the HTTP product read with a key filter
   (`GET /v1/streams/{name}/records?...`), if the rig can produce the refused
   state behind an HTTP rig; otherwise state that the test drives the read
   functions the route calls.

`bounded_discovery_tests.rs` is 483 lines, so there is room. Its owner for
mutations is the history module; check `scripts/quality/mutation_owners.py`.

---

## 2. Closure debts: a paging bug and missing tests

**Owner decision.** Idle-expiry recreation billing is a release blocker:
"a durable, generation-fenced handoff or cleanup obligation with resumable
processing ... Test idle expiry, raw and product recreation, month crossing,
CAS losers, owner movement, and crashes at each handoff boundary."

**What landed (2ba4bc47).** `Registry::recreate` writes a closure debt for the
incarnation it is about to replace (`src/registry/replaced.rs`, objects under
`registry/v4/replaced/<hex project>/<hex name>/<hex epoch>.json`) before its
conditional write. `src/billing/replaced.rs::settle_replaced` runs after
`tombstone_walk` in `sweep_owned_outboxes` (`src/billing.rs`). Rules: if the
debt's incarnation is still stored and live or fork-retained, drop the debt
(the recreation lost to a renewal); if stored and dead, wait (the walk's
terminal path closes it); otherwise close each owned segment at the debt's
`close_ms` while its gauge is open, and mark a segment settled when its owner
finds nothing open; the last settlement deletes the debt. Test:
`dst::dst_tests::billing_controller::a_recreation_over_an_idle_expired_incarnation_still_closes_its_storage`.

**Bug fixed: 40dbf0d3** (the pass now resumes from a cursor on
`BillingService`; test `the_closure_debt_pass_reaches_a_debt_behind_waiting_ones`).
Original note: **Bug to fix first (medium).** `Registry::replaced_page` lists the whole
prefix, sorts, and keeps the first 64 debts. `settle_replaced` therefore
always examines the same first 64. Debts that stay waiting (their
incarnation is still stored and dead, or their segments belong to other
instances) can starve every later debt on this instance forever. Give the
pass a cursor that survives between sweeps (mirror
`state.billing.sweep_walk_cursor()`: `list_with_offset` from the last key,
wrap when exhausted, advance only past completed entries), and bound the
listing itself rather than listing everything each sweep. Red: 65 waiting
debts sort ahead of one actionable debt; the actionable debt must still
settle.

**Tests landed (60d80607, 2026-09-29), and the bugs they found.** The seven
tests below are in `src/dst/tests/billing_closure_debts.rs` and
`billing_closure_owners.rs` (a two-instance rig). They found five settlement
bugs. Three are fixed, each red first:

- B1 (1b65e15d): debts owned by different instances deadlocked.
- B2 (61068487): the tombstone walk stopped at a foreign route, an R29
  regression.
- B4 (8182730e): the drain closed a replaced dirty row at its own clock.

A fourth fix (db126ccd) makes the walk hand back every shard it opened.
Edge change #65 records all four.

The owner decided both policy questions on 2026-09-29. B5: an expired
source its forks still read stops billing at its expiry, which is today's
behaviour, now pinned by
`an_expired_source_its_fork_reads_is_never_replaced_and_stops_billing_at_expiry`.
B3: a correction against a frozen (finalized) month is allowed, so a close
that arrives late must correct its month and stop the carry. **B3 is fixed**
(edge change #66, awaiting the owner's ratification): a settled late figure
(a month-final, or gauge 0) sets the month's floor exactly and records the
signed difference as a correction; a gauge-0 late snapshot advances the
segment state, so no month not yet closed bills the segment; each later
month already carried from the stale gauge is reversed in the same page
(a correction when finalized, in place between its carry and its freeze).
Pinned by `billing_closure_debts::a_month_closed_before_settlement_still_bills_only_up_to_the_expiry_instant`,
`billing_late_close.rs` and `src/rollup/page/late_close_tests.rs`. Left for
the owner: a late snapshot that still owns bytes does not advance the
segment state, a late non-settled snapshot still drops its same-version
month-final, and a gauge-0 month-final inside its own month's
carry-to-freeze window is still dropped. As originally written:

- **B3.** A close snapshot that arrives after its month was finalized never
  corrects the rollup (`src/rollup/page.rs` `apply_snapshot`'s finalized
  branch returns before the `SegmentState` update, and `apply_late_snapshot`
  saturates). The month stays billed to its boundary and every later month
  carries the stale gauge. Decide whether a negative correction against a
  frozen invoice is allowed.
- **B5.** An expired source with live forks counts as retained for
  recreation (`creation::retained_for_forks`, FRK-019 "behaves as
  soft-deleted"), but not for billing: the walk, the drain and
  `judge_unreplaced` use `soft_deleted && !deleted`, so the storage its
  forks still read stops billing at the source's expiry.

The raw and product PUTs share `Registry::recreate`, not `claim::resolve`,
as the text below says. The original list:

**Missing tests** (in a new DST file: `src/dst/tests/billing_controller.rs` is
already 834 lines of its 1,000):
- **Raw path:** recreate through `PUT /v1/stream/{name}` (raw). Both surfaces
  share `application::creation::claim::resolve`, but it is untested.
- **Month crossing:** expiry in month M, recreation and settlement in M+1
  (`BILLING_CLOCK_OVERRIDE` under `billing_clock_lock().write()`): M's row
  and the M+1 carry must bill storage exactly up to the expiry instant.
- **Owner movement:** a segment of the replaced incarnation opened by another
  instance between debt write and settlement (a two-instance rig, as in the
  fleet DST tests): each instance closes only what it owns, and the debt is
  deleted only after every segment is settled.
- **Crash between the debt write and the replacing write:** write the debt
  (call `registry.record_replaced`) and do not replace. The debt must wait
  while the incarnation is stored and dead; the walk closes that incarnation;
  after a later real recreation the debt settles and is deleted.
- **CAS loser with renewal:** the incarnation is renewed (alive) after the
  debt was written: the debt is dropped and the live gauge untouched. A
  version of this is in the landed test (the "renewed" stream); add one where
  the renewal happens through a real append rather than a direct
  `record_replaced` call.
- **Multi-segment incarnation:** a split stream (several segments) replaced:
  every segment is closed, `settled` grows segment by segment, and the debt
  is deleted only after the last one.
- **Fork retention:** a fork-retained incarnation is never replaced
  (`recreatable` excludes it), so no debt; pin that the retained storage
  keeps billing.

**Open, for the owner: a billing close applied to a row that is already
closed (found 2026-09-29).** The committer applies every `BillingClose`:
clock to the instant, gauge 0, `usage_version + 1`, row dirty
(`src/shard/transaction/maintenance.rs` `billing_close`); `billing_retained`
beside it changes nothing when the flag already matches.
`submit_billing_close` only enqueues, four of the five submitters (walk,
debt pass, the drain twice) decide on a plain read of gauge > 0, and hard
delete reads nothing, so a second submitter that reads the row before the
first close applied closes it again. Reachable on one instance: delete with
the drain or the walk at normal latency, drain with drain when the committer
lags past the 2 s drain period, walk with walk only if a close stays
unapplied for a sweep period. Not reachable across instances (ring gate and
writer fencing), after a failed submit, or by replay. Every submitter uses
the persisted instant, so a repeated close changes no billed figure. It
costs a row and a dirty-marker write, re-dirties a clean row (which can keep
a sweep-opened engine resident), adds at most one `_usage` record and one
ack, and makes the rollup rewrite the month row with zero delta, which moves
`updatedAt` on the usage answer. Production reads the version only as an
ordering and dedupe fence; tests read it as a close count, which is why
`an_expired_source_its_fork_reads_is_never_replaced_and_stops_billing_at_expiry`
failed 2 runs in 40 until it waited for the close to land (7cef509c).

- **(a) Leave the committer; say that a version is not a close count.**
  Reword the field's description and the two comments that call the walk
  idempotent (`src/billing.rs`, line-neutral: the file has no headroom) and
  add a sentence to `docs/OBSERVABILITY-BILLING.md`. No stale receipt, no
  mutants, no edge change. The costs above stay, and a test that asserts
  one more version must wait for the close before it sweeps again.
- **(b) A close that would change nothing is a no-op:** skip when the gauge
  is 0 and the instant is not after `storage_accounted_through_ms`;
  otherwise exactly today's behaviour (a later instant on a closed row
  still moves the clock). A skip never touches the overlay's `dirty` (an
  append earlier in the same group may have set it). Shape: a method on
  `BillingOverlay` (`src/shard/transaction/overlay.rs`) that
  `billing_close` calls, so its exception scope shrinks. Cost: five
  receipts stale by digest (KANI-046, TLA-005, TLA-016, TLA-002, TLA-003;
  no model mentions the close), and new shard-level tests, because no
  `shard::` test applies a close today (two closes in one group and in two,
  a later instant, an open gauge, a skip after an append). A client can
  observe it only indirectly: `updatedAt` moves less often, and the version
  digits inside later correction ids may be lower; whether that needs an
  edge record is the owner's ruling.

Recommendation: (b), batched with the next change under `src/shard`. The
committer is the only place that sees both closes in order, so it is the
only place where the guard is exact; it makes the walk's documented
idempotence true and removes every cost above, and the bill cannot change
because the skip applies only where today's close changes nothing but the
version. Unconfirmed and separate: a close enqueued on an engine the walk
cold-opened may be dropped when `walk_settle` retires that engine before
the committer applies it (a lost close retried by the next sweep, not a
double).

Rollback note for the record: an older binary ignores the debt objects (they
live outside the descriptor), so a rollback leaves debts unsettled until
roll-forward.

---

## 3. Typed registry read error for an append's first read

**Done: 7499ec19** (edge change #58): a store failure on an append's first
registry read answers a retryable 503 (raw `internal`, product
`temporarily_unavailable`), and a corrupt descriptor stays 500. The product
handlers' own descriptor reads for metadata, scan, append and usage
followed in edge change #64 (`product::DescriptorReadAnswer`), `product_seal`
and `product_read` included after the owner approved the change to their
exception scopes (2026-09-29; their growth rows went away, the scopes now
below them). The collection listing and the fleet-internal receivers are
unchanged. The
original handoff follows.

**Owner decision.** Ratifying #54: "The first registry read still returning
500 for transient storage failure is a separate inconsistency. Fix it next
with typed error classification and its own regression rather than
broadening every internal error into a retryable response."

**Current behaviour.** `AppendService::prepare` (`src/application/append.rs`)
maps any `registry.get` error to `FailureClass::Internal`/`AppendCode::Internal`:
raw 500 `internal`, product core 500 `append_failed` `retryable:false`, and
the product handler's own descriptor read 500 `retryable:true` (which the
SDK does not retry: `sdk/src/index.ts` retries only 429 and 503).

**Why it is not a one-liner.** The registry cannot yet tell a transient store
failure from descriptor corruption: `invalid_descriptor` (corruption) and the
test failpoint (`src/registry/cache.rs`, "injected registry get failure") both
produce `object_store::Error::Generic { store: "registry", .. }`. Corruption
must stay a fail-closed 500.

**Design.** Give registry reads a typed error, for example
`enum RegistryReadError { Unavailable(object_store::Error), Corrupt(String) }`,
produced where the registry decodes (`decode_desc`, `validate_descriptor`,
`invalid_descriptor`) versus where the store errors. Migrate `get`'s callers
in the append path first (other callers can map it back to today's
behaviour). Then map `Unavailable` to class `Unavailable` (raw 503; product
503 `temporarily_unavailable` `retryable:true`); keep `Corrupt` as 500.
Decide the raw code with the owner: 503 `internal` (no new wire code) or a
new code (the F3 plan, `plans/plans19/registry-read-retry.md` §9 D2, lists
both). Also cover the product handler's own reads (for example
`product_seal`'s descriptor read answers 500 `retryable:true`).

**Tests.** `src/dst/tests/append_application.rs` already has the F3 pattern
(`r02_a_reprepare_the_registry_cannot_read_is_retryable_not_internal`, plan
control (d): the fault armed before the first `prepare`). Red: the first
read under `fail_next_get` answers `(Internal, Internal, None)`. Green:
`(Unavailable, <code>, Some(1))` or as decided. Control: a planted corrupt
descriptor still answers 500. This is an edge change: add a record (#58)
and update WIRE-MATRIX §1.2 and the product sections.

---

## 4. Item 40: heartbeat, controller progress, serving eligibility, drain

**Owner decision (second review), which overrides the plan's D4:**
"Approve a separately supervised Critical heartbeat on the proposed two-second
cadence. Introduce controller-progress and serving-eligibility information
alongside it. A fresh independent heartbeat means the process can still send
heartbeats. It does not mean the fleet tick is progressing or that the
instance should receive new ownership. The acceptance contract should
distinguish process liveness, controller progress, serving eligibility, and
draining state. During planned drain, stop new assignments while retaining
the liveness and fencing needed to finish or hand off existing ownership. Do
not drop the heartbeat first and rely on peers treating the instance as
dead. A drain timeout must not be reported as successful handoff. Use scoped
withdrawal for isolated shard failures when safe; use instance-wide
withdrawal for process-critical or global-authority failure. Ownership
fencing, not heartbeat freshness, must remain what prevents concurrent
writers. Require brownout, stuck-tick-with-live-heartbeat, critical-loop
failure, and multi-instance drain tests. Derive progress deadlines from
bounded store-operation and retry budgets rather than choosing an arbitrary
aggressive timeout."

**Plans.**
- `plans/plans15/heartbeat-cadence.md`: the separate heartbeat publisher on its
  own 2 s cadence (the owner approved this shape).
- `plans/plans18/heartbeat-draining.md`: `draining` = the runtime's readiness
  verdict (task supervisor plus shard directory, §2a); ring eligibility
  honours a peer's published drain and heartbeat age for every candidate,
  this instance included (§2b); live set and load (§2c); return-home judges
  a home only by its published heartbeat (§2d, D1). Its D4 withdrew plans15;
  the owner has since chosen the opposite, so reconcile: keep plans18's
  eligibility and draining rules, run the heartbeat as its own Critical
  supervised task (plans15), and add an explicit controller-progress field
  (the fleet tick's last completed pass) that eligibility also checks, so a
  live heartbeat with a stuck tick is not eligible.
- Its D5 (no graceful-shutdown drain) is superseded by the owner's drain
  contract above: planned drain needs an ordered phase before cancellation
  that publishes "draining" while keeping the heartbeat and fencing alive.
  Mind the Compute detail in D5: a replacement process reuses the ordinal
  name, so readers must be boot-id aware.

**Constraints.** `src/fleet.rs` is at its ceiling (1,142 lines)
and `start` carries six fingerprinted contracts; plans18 §4 lays out a
non-growing edit order. Items 38/39 landed (bounded process exit and the
wrapper), which D3 relied on.

**Tests the owner requires:** brownout (slow store), stuck tick with a live
heartbeat (must lose eligibility), critical-loop failure (instance-wide
withdrawal via item 38), multi-instance drain (no concurrent writers; the
drain timeout is reported as a timeout).

**Progress (2026-09-28).** Step 1 landed: liveness and controller progress.
- The heartbeat is its own Critical task, `fleet-heartbeat`
  (`src/fleet/heartbeat.rs`), on a 2 s interval; the tick no longer
  publishes. Each beat carries `boot_id` and `progress_age_ms`: how long
  ago the tick last published its ownership view, measured on the
  publisher's monotonic clock (a restore or a time step cannot move it).
- The ring (`fleet::planning::active_members`) keeps a candidate only while
  its heartbeat is < 30 s old (this instance is exempt: it is running) and
  its progress age is below `PROGRESS_DEADLINE_MS` = 3 x 45 s pass deadline
  + 2 x 2 s period = 139 s. A pass publishes after its reads and may spend
  the rest of its deadline on moves, so two healthy publications can lie
  two deadlines and a period apart (92 s); one pass abandoned at its
  deadline adds a period and a deadline. The pilot's ring mirror applies
  the same rule.
- Tests: `a_stuck_tick_keeps_its_heartbeat_but_not_its_progress`,
  `a_held_heartbeat_does_not_stop_the_tick`,
  `a_store_brownout_slows_the_tick_without_churning_the_ring` (two
  instances, 600 ms per store operation), planner boundary tests. r09's
  heartbeat case now proves the heartbeat task cancels cooperatively
  (re-pinned).

Step 2 landed: serving eligibility and instance-wide withdrawal.
- Each beat carries `withdrawn`: the runtime's readiness verdict (a task
  supervisor that is stopping or lost a Critical loop, or a shard directory
  that reports a cell failure, never opened a shard, or could not close
  one). Every ring, the instance's own included, drops a withdrawn
  instance; the pilot mirror too.
- A Critical exit under the process root cancels every task at once (the
  review's H1), so the heartbeat's last beat, bounded by 3 s inside the
  ordered stop's 10 s join grace, always withdraws the stopping runtime.
  r09's shutdown budget is 4 s, not 300 ms, to cover that bound (re-pinned).
- Tests: `a_critical_loop_failure_withdraws_the_instance_from_every_ring`
  (two instances; both rings drop it within a pass),
  `a_stopping_runtime_publishes_its_withdrawal`, planner and mirror tests.

**Remaining steps, with the Fable design review's findings (2026-09-28).**
- A failed close still withdraws the whole instance, as readiness does
  today; step 3 narrows it to the prefix (`ShardDirectory::unready_reason`
  must be split into the instance-wide verdict and a withdrawn-prefix
  list, M1). A stuck tick also leaves this instance serving from a stale
  view; an expired view should withdraw it (a request-time change, so an
  edge decision).
- Step 3, scoped withdrawal. Determination (owner-delegated): a failed
  close stays instance-wide. Its prefix can never reopen in this process
  and only a restart heals it; step 2 already takes the instance out of
  every ring within a pass, and the watchdog restarts it. Scoped withdrawal
  fits a prefix that repeatedly fails to OPEN on one instance (the open
  gate's strikes); that is a separate change, and it would move `/readyz`
  semantics, an edge decision. Placement when it comes: not inside
  `OwnershipService::effective_owner` (its own fingerprinted contract) but
  as an `OwnershipView` method or an override; return-home must not hand
  the prefix back; if every member withdraws a prefix, ignore withdrawals
  for it (M1).
- Edge record #63 (the drain's process-surface change) was RATIFIED by the
  owner on 2026-09-28, with the clearer readiness text: `/health` answers
  503 `runtime draining` during a drain (M8), through a new
  `TaskMonitor::readiness_reason` rather than a grown `unready_reason`, so
  no exception row was needed.
- Step 4 landed: planned drain. A requested stop runs the drain the fleet
  registered (`TaskSupervisor::set_stop_preface`, `tasks::drain`) before
  any loop is cancelled. `bootstrap::run` is untouched: its approved growth
  rows record exact values, so extracting the signal task (M5) would have
  made them stale; `ShutdownRequest::request` begins the drain instead, and
  the signal loop's own `Done` during a drain is its consequence while a
  failing Critical exit still stops at once. The process root's bound is
  armed at drain start with the drain's bound added (H3). The drain's
  budget is derived from what it waits for, each at its own bound: its
  draining beat (a PUT in flight, then its own: 2 x 10 s), a peer's view
  that read it (a pass in flight, a period, the next pass: 45 + 2 + 45 s),
  that view's echo (a beat in flight, a period, its own PUT: 10 + 2 + 10 s)
  and shard closes (10 s): 144 s; 1 s to record the outcome; 30 s stop:
  175 s in all; the wrapper's forward grace is 180 s. Completion: this
  runtime holds nothing (no shard, no open still running, reaping ones
  included, no unsettled close, all from the open gate's own observation,
  M3) and every peer whose views keep being published (live and
  progressing, whatever its own standing) has published one that read one
  of its draining beats and left it out (`viewed`: recorded by the tick's
  own heartbeat-set read, promoted when it publishes the view, and
  filtered at each beat by the view's ring, so the ordinal fallback cannot
  count; boot id and beat sequence, no clocks, H4; it holds live draining
  peers only, so it stays small). The drain begins only when the ring would
  keep another member within the desired count and no waited peer is of
  an earlier version (no `seq`); otherwise `NoPeer` at once, never
  announced. Outcomes: `HandedOff`, `NoPeer` (also when every peer went
  away during the drain), `Failed` (a failed close, final), `TimedOut {
  pending }` (also when the fleet cannot be read within a document deadline
  before anything is announced). The outcome is logged from a pure,
  tested report and recorded in the runtime's standing. While a drain runs,
  readiness answers `runtime draining` unless another critical loop failed
  (then that failure) or the stop has begun. A first design that trusted
  each peer's echoed ring alone reported `HandedOff` from a ring a peer
  published before it had seen the drainer; two Fable reviews (2026-09-28)
  found the remaining gaps, all fixed or documented. Tests:
  `dst_tests::fleet_drain` (two instances sharing data and fleet stores:
  the peer serves the acknowledged record while the drain runs and a write
  through the drained instance is refused; a live peer whose tick is parked
  keeps the drain from completing and is named; a synthetic peer that
  never reads; an earlier-version peer; a fleet of one; a sole member
  within the count; the wiring through a signal-shaped stop, which records
  `HandedOff`) and `tasks::drain`.
- Still open, for the owner: name arbitration when a replacement process
  reuses an ordinal name (H2: CAS on the heartbeat PUT, a draining process
  yields as `Superseded`); whether Compute's own stop grace allows
  the 175 s bound (D10); a second termination signal during a drain is not
  observed (the signal loop in `bootstrap::run` ends after the first);
  COMPUTE-SPEC §5.2's one-shard-at-a-time handoff
  (the drain yields every shard at its first excluded pass).

---

## 5. F1: two different things carry this name

The reviewer had not seen the F1 plan, and the owner's decision describes a
different defect from the plan. Treat them as two items and confirm with the
owner.

**F1-a, the owner-approved contract: route the seal fence to its owner.**
`application::lifecycle::fence_segment_for_key` (`src/application/lifecycle.rs`)
resolves the segment's engine locally
(`shards.resolve(&route, Adoption::Internal)`) and maps any failure to
`SealError::Resumable("segment engine unavailable")`. When another instance
owns the segment, the fence can never be placed there. The owner's decision:
"Implement a typed, authenticated fleet-internal seal-fence operation. Carry
and verify the project, stream incarnation, segment identity/route, and seal
generation. The receiving instance must validate current ownership;
redirects/retries must be bounded. Do not open another local writer or
weaken identity checks. The reply is an ordering and durability barrier: a
false result must establish that older fenced final operations cannot
subsequently close the segment. A true result means the earlier close
committed. Test wrong ingress, owner movement, competing old-final/takeover
operations, and crashes around fence durability." Model the route on the
existing fleet-internal segment-close relay (`POST
/v1/internal/segment-close/{*name}`, WIRE-MATRIX §3) and its workload-claim
checks; `src/http.rs` is at its ceiling (plans19 split-producer-lineage §9 D3
suggests extracting the internal route table into
`src/http/internal_routes.rs`).

**F1-b, the plan: producer and Stream-Seq lanes across a split.**
`plans/plans19/split-producer-lineage.md`. After a split, a key's producer
lane and Stream-Seq lane are read only from the serving engine's DB, so a
child on another engine accepts a producer retry the parent already
committed as new (a duplicate) and answers `producer_seq_gap` to the next
sequence. The plan restores ROUTING-V3 §7 with a lazy carry at submission
(commits 1-2, same instance) and a fleet-internal lane read (commit 3, `GET
/v1/internal/segment-lanes/{*name}`). It is an edge change (the plan's D1;
medium) and needs the owner's approval of D1, D2 (transport), D3 (http.rs
ceiling), D5 and D8. The split-boundary harness it builds on is not on
slate: it is on branch `worktree-wf_e38ad9fe-ae6-3` (commits 8a2791b0,
ca80f9d4; 706430f7 as amended); the F1 reds are ignored tests in its
`src/dst/tests/split_boundary_outcomes.rs`. The investigation record is
`evidence/split-investigation.json`.

---

## 6. F2: composition test for a retiring engine's written group

**Owner decision.** "A retiring engine must stop publishing live state, but
retirement does not prove its accepted storage write disappeared ... Ratify
that model. Settle callers within a bounded period. When durable success
cannot be established, return the retryable shard-moving/unknown outcome,
retain owed-final obligations, and let the successor recover canonical state.
Do not publish success without the normal durability barrier, and do not
describe an ambiguous write as definitively rejected. Retries must preserve
producer/idempotency identity. The next useful test is the public append/seal
plus successor-ownership composition."

**What to do.** Write that composition test: over HTTP (or the application
layer), an append and a `:seal` with a final record whose group the old
engine wrote but had not made durable when it retired; the successor engine
opens the same segment; assert the client saw the retryable
`shard_moving`/unknown answer, the successor's canonical state holds the
record exactly once, a producer-keyed retry is answered as a duplicate (not
a second copy), and an owed final is still owed and completes. The existing
R17-B tests stay (they reopen storage and check the tail, queue
configuration, closed state, billing counters and dirty marker).

**Plan for reference, not approved.** `plans/plans19/retiring-engine-written-group.md`
proposes Option A (answer stranded written groups from the retiring close's
final `durable_seq`: success where the close made them durable, else the
same 503). That changes answers and refines R17-B, so it needs an explicit
owner decision; the owner's text above ratifies the unknown-outcome model
and asks for the test first. Also listed there (§9 D6): a fenced write
(`Closed(Fenced)` from `db.write`) currently surfaces as 500; the plan
proposes its own item with a typed mapping to `Moved`.

---

## 7. Bug #7: rollup re-keying by physical segment, as an explicit migration

**Owner decision.** "Approve keying rollup state by physical segment, with
payer as an attribute. Approve developing the tooling now. Gate activation on
rehearsal against a consistent copy of a real rollup database. Do not make
ordinary startup silently rewrite the database format. The migration needs a
format/version fence, a dry-run inventory, a verified backup, resumable
execution, and a second run that is a no-op. Conflicting identities or
inconsistent latest rows must fail clearly rather than being resolved by
arbitrary ordering. Correct already-finalized phantom usage through explicit,
idempotent UsageCorrection records. Preserve legitimate historical payer
attribution; do not silently rewrite finalized artifacts or move historical
charges to the new payer. This is a roll-forward migration; restoration of a
consistent pre-migration state is a separate recovery procedure. This
rekeying does not fix idle-expiry recreation; keep the two issues separate."

**Plan.** `plans/plans20/rollup-rekey-migration.md`: a v3 namespace
(`telemetry/usage-rollup/v3/p0`) beside the untouched v2, a marker-fenced
resumable copy, and `pendingArtifacts` split into publishable,
blocked-corrupt and total counts (commit C1, which the owner already asked
for in the first review and can land first). Reconcile it with the decision:
the plan's C3 runs the copy inside `open_with_cache` when
`ROLLUP_SEGMENT_LAYOUT=physical`, which is a copy at startup; the owner wants
an explicit operation. Prefer an explicit migration command (an operator
subcommand or route) with `--dry-run` (inventory: rows per class, conflicts,
latest-row inconsistencies), a verified backup step, resumable phases, and a
proven no-op second run; activation of the physical layout stays a separate
explicit switch after the rehearsal. Its D5 is now decided: issue
`UsageCorrection` records for finalized phantom rows.

**Acceptance gate (the owner's).** A rehearsal on a consistent copy of a real
rollup database: accounting conservation, replay conservation, interruption
safety, invoice and recovery preservation. That needs the owner (which cell,
which bucket, who runs it; plan D8).

---

## 8. Stale-page repair after a refused chain

**Superseded by the merge (d16559b3):** a reader admits an overlapping page
whose offsets agree with those already admitted and keeps the part past them;
only a disagreeing overlap is still corruption (envelope fallback). Repair is
needed only if a schedule can produce a disagreeing overlap; that is the
question left. The text below is kept as the original handoff.

**Owner decision.** "Schedule stale-page repair, but do not keep the original
hold open indefinitely."

**Design (from the review of the absorber fix).** Settlement proves that every
chunk at or above the durable boundary D is dead. So when `plan_reads`
(`src/history/gather.rs`) rolls a settled stream's mark back from M to D,
carry `[D, M)` on that stream's `ReadPlan`; the heal gather's history
`WriteBatch` then deletes that stream's postings pages whose `page_first` lies
in `[D, M)` (scan `postings_range` over the buckets `[D, M)` touches, across
the stream's routing-key hashes) before writing its own pages, and drops the
postings-cache runs above D for that incarnation. Two variants the rollback
path does not cover:
- a bucket-sharing stream's dropped (Detached) gather;
- an engine retirement that drops two chained advances: the new owner has no
  lane marks, so it cannot know `[D, M)`. Either delete, for each key the
  first gather after open writes, the pages in `postings_range(from, from +
  chunk)` it does not rewrite, or persist chunk boundaries.

**Tests.** Item 1's composed test flips: after the heal, `pages_tile` holds
and `POSTINGS_CORRUPT` does not advance, with the ledger still exact.
**Constraints:** `src/shard.rs` 3,139 and `src/history.rs` 1,655 at their
ceilings; `gather.rs` 718. Mutation owners: `shard`, `commit_plan`,
`transaction_maintenance`, and whichever owns `gather.rs`.

---

## 9. Release-wide mutation campaign (work package 4)

**Owner decision.** "The reported 81-mutant run over the latest pushed range is
not automatically the whole-hardening-range campaign. That work package
explicitly includes billing, rollup, product, and usage owners that the
per-push plan may not select. Verify the base/head range and selected owners
before marking it complete."

**What to do.**
1. Fix the range: from the program's start (the commit before the first
   hardening item on 2026-09-21; the README and `report/ledger.json` name it)
   to the release candidate.
2. Register mutation owners for the changed but unregistered sources in
   `scripts/quality/mutation_owners.py`: at least `src/product.rs` and its
   modules (`usage.rs`, `seal_request.rs`, `operation.rs`, `append_body.rs`),
   `src/billing.rs` and `src/billing/replaced.rs`, `src/rollup*`,
   `src/registry/replaced.rs`, `src/application/append*` (the F3 plan's D5
   declined this for one commit; the campaign should include it). Each owner
   needs a test filter that actually runs the tests pinning it (the `http`
   owner missed a mutant until `security_usage::` was added to its filter,
   b13452dd).
3. Run with `QUALITY_EVENT_NAME`/`QUALITY_BEFORE_SHA` set to that range on one
   pinned artifact, and preserve the selected source paths, test identities,
   mutation outcomes, configuration and artifact identity in `evidence/`.
   Every MISSED mutant gets a test or a written disposition.

---

## 10. Smaller items

- **Item 50 cleanup.** 45f8711c added the holder rule (`Arc::strong_count > 1`)
  beside the counted admission pin. `plans/plans18/tracker-live-eviction.md`
  §2.1 retires the pin (`admitting`, `pin`/`unpin`/`active`,
  `AdmissionPin::drop`) and deletes `has_pressure` and the `live_subs` term,
  re-pointing the Loom model. Optional. If done, delete the redundant terms
  in the same commit, or they leave equivalent mutants. The owner also asked
  to validate sustained churn beyond the ~905 s residence horizon and actual
  memory use in the Compute profile (≈36 first-seen projects/s at 32,768 is
  an estimate).
- **Absorber receipt ordering (test done; explicit drop not possible).**
  `a_receipted_advance_settles_only_after_its_group_is_durable` now asserts
  the durable boundary is published at the first observation of `settled`.
  That assertion is timing-bound: with the receipts dropped at the top of
  the dispatch iteration instead it passed 60 of 60 runs. The explicit
  `drop(group.effects.receipts)` after the tails loop grows
  `ShardEngine::dispatch_durable`'s excepted scope (`unwrap_used`,
  `let_underscore_must_use`, `cast_possible_truncation`,
  `excessive_nesting`: one line, three syntax facts, an ordinary call), so
  it needs an owner-approved growth row or a restructuring of that function
  (for example moving the tail publication into a method that owns the
  drop, which then needs its own lock-poisoning decision). The receipts
  still drop at the end of the iteration, after the tails, as
  `DurableEffects::receipts` documents.
- **`parse_month` (done, edge change #59):** a signed month ("2026-+9")
  answered a zero row; it now requires ASCII digits and answers 400
  `invalid_month`.
- **Flaky test (done).**
  `dst::dst_tests::admission_maintenance::first_request_waits_for_restoration_then_sees_the_restored_ledger`
  failed 1 in 64 under load (a restored 204): the absorber's settlement of
  the first append rewrote the maintenance row after the test's fat row. It
  now waits on observable events (the settlement, engine 1 closed, the
  request's shard open in flight) instead of fixed sleeps.
- **Flaky test (done), and an engine question for the owner.**
  `shard::retirement_tests::tla005_f5_a_failed_write_answers_nothing_from_its_batch`
  failed about 1 run in 10 at its teardown with
  `storage-close: Failed(... Unavailable error: io error (oops))`. Cause:
  since f574d733 the engine begins its close in `write_failed`, before
  SlateDB has recorded the writer's failure, so `Db::close` races the
  writer's exit. SlateDB's close reads the status and then records its own
  result without checking whether it won; the writer records its result and
  then publishes the status (slatedb 0717cc1 `db.rs` 693-707,
  `db_status.rs` 255-262). Close first, or writer first: the close answers
  Ok. Close between the writer's two steps: its final flush is refused with
  the writer's error and the storage close reports Failed. The test now
  keeps the writer parked until the close has recorded Clean, which is the
  ordering f574d733 already names as its cost. **Open, for the owner:**
  production has the same three orderings after any post-apply write
  failure, and a failed storage close is final: the engine stays
  `ShuttingDown`, its prefix answers `shard_closing` on every open and
  readiness fails until restart (`src/tasks/shutdown.rs`, `src/sharddir.rs`).
  That is fail-safe and the window is narrow. Options: settle the close of
  a Db that had already failed; have the finalizer wait, bounded, for the
  writer's status when it retires through `write_failed`; or fix the read
  and the write in SlateDB's close upstream. The first two edit
  `src/shard/lifecycle.rs` or `history_partition.rs` (TLA-006 and TLA-011
  receipts), and the comment in `lifecycle.rs` that no store fault produces
  a failed close does not hold for the third ordering.
- **SIGTERM on a fully wedged executor** is never observed (the signal task
  runs on that executor), so it arms no stop bound; documented in WIRE-MATRIX
  §3 and RUNBOOK. An OS-thread signal path (sigwait or signal-hook) would
  close it; optional.
- **Closure debts across a rollback:** see item 2's rollback note.
- **Nightly mutation rotation (found 2026-09-28, when `slate` became the
  default branch and the schedules started running).** The seven-night
  rotation (`verification_plan.scheduled_owners`, RUST-QUALITY.md) runs
  whole registered owners. Counted with `cargo mutants --list` over the next
  seven buckets: 573, 631, 630, 154, 1,015, 846 and 1,152 mutants (about
  5,000 in all; the largest owners are `shard` 378, `sse_feed` 346, `http`
  270, `fleet` 262). At about 1 min per mutant locally and more on a 4-core
  runner, no bucket but the smallest fits the job's 240 minutes, and whole
  files have only ever been mutation-tested diff by diff, so MISSED mutants
  are expected. The first scheduled run gives the real per-mutant cost with
  incremental rebuilds (CARGO_INCREMENTAL=1 since ca260d30); size the design
  from it: more buckets, `cargo mutants --shard k/n` inside large owners, a
  runner matrix, and a MISSED backlog with dispositions. It is a policy
  change (RUST-QUALITY.md's rotation), so the owner decides the shape.
- **Deferred with reasons (2026-09-28):** the 83 `gap_lock` holders that
  serialize about 126 s of CI's 154 s suite (drop the lock test by test,
  loop each in the parallel suite); Kani recompiling the crate for every
  check (compile once per obligation at the next deliberate `formal.py`
  change, which stales every receipt anyway); `build.rs` embedding the git
  HEAD, which rebuilds the crate after every commit (release provenance
  depends on it); and the seven production files under critical prefixes
  without a mutation owner (`src/application/read_range.rs`,
  `read_retention_probe.rs`, `src/fleet/planning.rs`,
  `src/shard/record/checked.rs`, `src/sse/budget.rs`, `src/sse/mod.rs`,
  `src/tasks/signal.rs`): register each when it next changes;
  `scripts/dev/impact.py` prints REGISTER FIRST. Two more are test code the
  planner cannot see as such: `src/ops/batch_tests.rs` and
  `src/shard/commit_handoff/loom_tests.rs` are declared `#[cfg(test)] mod`
  by their parents but carry no inner `#![cfg(test)]`, and adding one reads
  as a production change of an unregistered file (the planner strips test
  code from base and head and compares), so CI refuses that commit. The
  clean fix is a planner rule that honours the parent's `#[cfg(test)]`
  declaration; it changes what the gate selects, so it is the owner's.

---

## 11. Deployment gates (need the owner or the Compute owner)

These block deployment sign-off, not merging (README, "Deployment gates"):
- Compute validation of the wrapper -> binary -> platform lifecycle: startup,
  readiness, restart, signal delivery (does Compute signal PID 1, the process
  group, or tear the VM down?), memory pressure, rollback.
- The Compute deployment owner enumerates every target project and exports
  its redacted configuration; `scripts/effective-config/` then re-runs the
  comparison and boot validation on the final candidate with the actual
  persisted namespace constraints (the archived comparison's "HEAD" is an
  earlier revision). Remove obsolete `ABSORB_PASS_BYTES` after the export.
- A release-posture Compute family (production fleet authentication, usage
  and audit configuration); the static fleet-auth bridge stays a benchmark
  exception.
- Upstream idle compatibility with the 120 s header timeout, with margin, or
  an explicit validated timeout.
- Bug #7 rehearsal on a consistent copy of a real rollup database.
- The 14 staged wrapper copies under `~/.streams-soak` hold the pre-39
  wrapper until each campaign script next runs `bench/stage-app.sh`.

---

## 12. Formal verification: new obligations and what blocks them

Owner direction (2026-09-25): when the work allows, implement obligations from
the roadmap's unimplemented list
(`docs/PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md`); toolchain setup is in
`verification/README.md`.

- **KANI-005 (done):** `locate_in_spans` positioning, `src/sse/source/proofs.rs`,
  assumption ASM-LINEAGE-CONTRACT (the lineage constructor's unchecked
  `logical += c` is assumed not to overflow).
- **KANI-028 (done):** the coverage rule (`tiles_keyspace`, used by
  `SegmentMap::validate` and `check_partition`), `contains` and `route`,
  `src/segmap/proofs.rs`, one harness per count of one to four segments.
  Whole-map `validate` is out of Kani's reach (over two symbolic segments it
  did not finish in 45 minutes: vectors inside the map's vector lose their
  constant lengths), so the harnesses check the rule it applies. `validate`
  now scans for duplicates instead of hashing (Kani cannot model the random
  seed) and reports a typed `TopologyError` (same messages);
  `Registry::resolve_segment`'s choice moved into `SegmentMap::route`. Its
  open question (sealed covers ordered by `created_ms`) turned out to be a
  production bug at four sites, fixed as edge change #60
  (`SegmentMap::lineage`, allocation order). Harness lesson for KANI-029 to
  KANI-031: keep the number of segments concrete per harness and call pure
  helpers, not `validate`.
- **KANI-017 (done):** stored-record admission (`decode_row`),
  `src/shard/record/proofs.rs`, with the routing-key length fixed per
  harness (0 and 4 bytes); a symbolic length makes the checker validate
  UTF-8 of every length (it did not finish in 40 minutes).
- **KANI-029 (tried, not landed):** a harness calling `SegmentMap::split`
  on two live segments over symbolic tiled ranges did not finish in 12
  minutes. Two costs, both from vectors held inside the map's vector whose
  lengths the checker loses: `split`'s own `debug_assert!(check_partition())`
  (Kani builds with debug assertions) sorts a live-range vector of symbolic
  length, and so does any harness that filters leaves by `successors`. A
  workable harness needs the leaf set computed from known indices and a
  cheaper form of that debug check (for example `tiles_keyspace` over a
  fixed-size array), which is a production change to decide deliberately.
- **KANI-046 (done):** the absorbed boundary and trim frontier
  (`retire_absorbed`, `trim_target` in `src/shard/commit_plan.rs`, which the
  committer's absorb and trim steps now share), `src/shard/commit_plan/proofs.rs`.
- **KANI-047 (done 2026-09-27):** the maintenance summaries,
  `src/shard/maintenance_row/proofs.rs`: the shard row and dirty rows decode
  exactly or are refused, a delta cannot clear live debt, and the stall
  signal survives clock extremes (the overflow found on 2026-09-26 is fixed:
  `no_progress_secs` saturates). The row's impl and codec moved to
  `src/shard/maintenance_row.rs` with a typed `MaintenanceError`, a registered
  mutation owner (`maintenance_row`) whose first run missed four mutants, now
  all caught (the refusal words, an append not moving the progress clock, the
  stall signal's two guards; one equivalent condition removed).
- **KANI-040 (done 2026-09-27):** the seal-claim decision matrix,
  `claim_step` in `src/application/lifecycle/claims.rs`, which
  `decide_claim` applies. One hardening: the plain seal's empty id no longer
  counts as an empty-id claim's own operation (unreachable, pinned by a unit
  test). Still open, KANI-041: every seal-generation allocation
  (`claims.rs` twice, `lifecycle.rs` twice, `topology.rs` twice) is an
  unchecked `seal_gen_counter + 1`, which would wrap after 2^64 claims; a
  checked allocator needs a decline path at each site, inside excepted
  scopes.
- **KANI-006 (done 2026-09-27):** the postings varint codec,
  `src/postings/proofs.rs`. The owner chose option (b): the six
  `crate::postings` rows in `docs/quality/exception-growth.json` (both by-path
  includes, `dead_code`, `unreachable_pub`, `unused_imports`) now pin
  nested_items 131, scope_lines 1466, syntax_facts 2716. Any further harness
  over a module the invariants or fuzz crates include by path (`postings.rs`
  for KANI-007 to KANI-015, `crypto.rs`, `tenant.rs`, `product_cursor.rs`,
  `queue.rs`, `quota/bucket.rs`, `retained_bytes.rs`, `rollup/allocation.rs`,
  `rollup/storage.rs`, `application/read_{batch,budget,retention_probe}.rs`)
  moves such rows again and needs the same owner decision, unless the gate
  stops counting `#[cfg(kani)] mod proofs;` (the rejected option (a)).
- **CI: formal shard 0 died whenever `verification/manifest.json` changed
  (resolved 2026-09-26 by the owner's choice, a smaller KANI-001: its two
  properties are separate harnesses and alphabet membership no longer goes
  through `memchr`; the re-recorded checks peaked at 4.9 GB).** A manifest change selects every obligation
  (`SELECTION_INPUTS` in `scripts/quality/formal.py`), so shard 0 runs
  KANI-001's round trip, whose CBMC process peaks near 15 GB resident
  (measured locally 2026-09-26: 7.8 GB after 6 minutes, 14.95 GB at 8
  minutes, 780 s to verify). GitHub's `ubuntu-latest` runner has 16 GB: the
  formal (0) jobs of 9b7a3874, 4a0a9e52 (twice) and 87de5284 ended with "the
  runner has received a shutdown signal", "the hosted runner lost
  communication" or "the operation was canceled", with no check reported.
  Every other job of those runs passed. Options: a larger runner for the
  formal job (or for KANI-001 alone); selecting only the obligations whose
  manifest entry changed (a driver change, which makes every receipt stale);
  or a KANI-001 harness split that bounds its memory.
- **KANI-004 (not started):** the offset parser's alphabet and aliases depends
  on the pending wire decision on lax token reading (review item 88 step 2,
  pinned by `offsets::tests::non_canonical_tokens_keep_their_lax_reading`).
- **Driver flake on macOS (fixed 2026-09-27):** `stop_group` in
  `scripts/quality/formal.py` now waits while macOS answers EPERM for a
  killed group whose members are not yet reaped (it raised `PermissionError`
  and failed `test_formal.TlcIsolation.test_6_a_*` in about a quarter of
  local `scripts/quality.sh` runs under load). Pinned by
  `test_6_a_group_that_answers_eperm_while_it_drains_is_awaited`. The driver
  is an input of every receipt, so all 24 were re-recorded in the same
  commit, in parallel with `scripts/dev/formal_batch.py`.
- **Stale receipts:** none; all 24 were re-recorded on 2026-09-27 with the
  driver fix above. `python3 scripts/dev/formal_batch.py status` lists them.

---

## 13. Configuration: fewer settings, and defaults that are what production runs

**Owner decision (2026-09-29).** The binary's default L0 cap is 32, the
value the production profile sets (823b3269). The owner asked what else
could be simpler.

**The audit.** `config-simplification.md` in this directory: the binary
reads 152 settings, the production profile sets 24 of them, 47 are set by
nothing and 7 do nothing. Five packages, each for the owner to decide:
settings that do nothing; the binary's defaults as the certified 1 GiB
posture (the compaction and store lines first, because the new L0 default
depends on them); the commit pipeline as one decision; settings nothing sets
as constants; switches with one live path. The verified detail of every
item is in `evidence/config-audit-2026-09-29/detail.md`.

Nothing in the five packages is changed yet.
