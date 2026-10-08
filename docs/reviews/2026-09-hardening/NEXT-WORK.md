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
- [14. K2 parity: storage layout 5, fewer uploads, shared cells](#14-k2-parity-storage-layout-5-fewer-uploads-shared-cells)

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

**Owner decisions (2026-10-01).** "I approve all of these": edge record #87 (the lost close's
walk-side fix, 9f25cf96) is ratified, and its exact fix is approved: a reply
channel on `CommitOp::BillingClose` and `CommitOp::BillingRetained`, answered
with a retryable refusal at the three sites that dropped the operation, with
a warning and a counter, and the walk and the debt pass stopping their page
on a refusal; approved with it are the growth of `CommitOp`'s exception
contract, the module extraction from `src/shard.rs` and the six receipts.

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
(edge change #66, ratified by the owner on 2026-09-29): a settled late figure
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

**Decided and done (edge change #72): a billing close applied to a row
that is already closed (found 2026-09-29; the owner approved option (b) the
same day).** The committer applies every `BillingClose`:
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
version.

**Confirmed and fixed (edge change #87, the commit that records it): a close
the walk or the debt pass enqueued on a shard it cold-opened was lost when
the pass handed the shard back.** An independent investigation (three
analysts, three skeptics, one judge) confirmed it at 88015cc5.
`submit_billing_close` and `submit_billing_retained` only enqueue; the walk
(`src/billing/walk.rs` `walk_segment`) and the debt pass
(`src/billing/replaced.rs` `settle_segment`) then called `walk_settle`, whose
debt probe reads only the durable dirty index, so it retired an engine whose
committer still held the op, and a retiring committer drops a queued
`BillingClose` or `BillingRetained` without a reply (the drain arm of
`committer_loop`, `reject_op`, and `CommitTransaction::run` on a closed
engine). The row stayed open and every month the rollup closed carried the
dead incarnation's gauge, until a later visit landed the close: the debt
pass when its cursor came back, the walk only after a whole catalog pass,
and never while the shard's SST layout let the probe finish without
yielding. Not a double: every retry carries the persisted instant. It is
the CI flake of
`billing_walk_custody::a_shard_the_walk_opened_for_nothing_to_close_is_handed_back_before_the_next_segment`
(gauge 81 still open, accounted-through 55 ms before the expiry). A step that
enqueued an op now keeps the shard scheduler-held, and the next sweep's
phase 1 rotates it as an indebted resident once the op has applied. Three
DSTs in `billing_walk_custody.rs` (the walk's close, the debt pass's close,
the walk's retention flag) make the losing order certain with a committer
that gathers for 2 s (`pace_min_reqs: 1`) and were red 5 of 5 runs each.

**The lost close's exact fix landed (owner decision of 2026-10-01; edge
change #91, the commit that records it, awaits ratification).** #87 left a
window: phase 1 decides from the durable index, so an op still unapplied a
sweep interval later met the same drop, as did the drain's closes and
retention flags on a resident phase 1 retired. Now each
`CommitOp::BillingClose` and `CommitOp::BillingRetained` carries a reply
(`BillingReply`, `src/shard/billing_ops.rs`). Staged, it joins its group's
replies (`src/shard/transaction/billing.rs`): `Ok` once the group is
durable, the group's refusal otherwise. Dropped before it was staged (the
drain arm of `committer_loop`, `reject_op`, `CommitTransaction::run` on a
closed engine, a failed accounting read or stream load, a closed queue), it
answers a retryable refusal (`Moved`) as it drops, with a warning and one
count of `BILLING_OPS_REFUSED`. `submit_billing_close` and
`submit_billing_retained` await the answer: the walk and the debt pass stop
their page on a refusal and replay it next sweep, the drain leaves the row
dirty, and the hard delete logs and leaves it to the tombstone's debt (and
answers once its closes are durable). Pinned by
`shard::billing_read_tests::a_billing_op_is_refused_once_when_a_retiring_committer_drops_it_and_answered_once_applied`
and two `billing_walk_custody` DSTs (the walk's and the debt pass's
refusal), each red 5 of 5 on the unfixed tree. `CommitOp`'s exception
contract grew under its approved row; `src/shard.rs` gave the room by
extracting `src/shard/billing_ops.rs` first. What remains: the six receipts
the `src/shard.rs` and `src/shard/transaction/mod.rs` edits stale
(KANI-047, TLA-002, TLA-005, TLA-006, TLA-011, TLA-016) are to be
re-recorded; the counter has no debug surface, because `src/http.rs` is at
its line ceiling and `ops::collect_snapshot` is a frozen exception scope
(tests read it); and #91 awaits ratification.

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

**Constraints.** `src/fleet.rs` is 999 lines (it was 1,142 at the review; c5791119 and its predecessors shrank it below the 1,000-line rule, so it may grow to 1,000 again)
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

**Owner decisions (2026-10-01).** "I approve all of these": edge records #88 and #89 are
ratified; the negative model control for a relayed fence whose loss answers
false (the F1-a plan's commit 4) is required; the platform contract names
`seal-fence` and the `segment-close` it omitted (the plan's commit 5).

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

Status (2026-09-30, on the owner's instruction "Do them all in that order!
use your own judgement", which took the plan's defaults): three commits.
(1) The operation vocabulary, the raw method-to-operation map and the
`/v1/internal` table moved to `src/http/internal_routes.rs` (no wire change;
`src/http.rs` 3,097 -> 3,031 lines). (2) The receiver, `POST
/v1/internal/seal-fence/{*name}?fence_to=` under the new operation
`seal-fence` (edge #88; `dst_tests::seal_fence_receiver`). (3) The sender:
`fence_segment_for_key` relays to the owner when another instance owns the
segment (`src/application/lifecycle/fence_relay.rs`), and every answer but
the owner's parsed closed-report is Resumable (edge #89;
`dst_tests::seal_fence_relay`, the owner's four classes). The seal model
gained the relay arm and a relayed timeout, so TLA-002 and TLA-003 must be
re-recorded. Left for the owner: ratifying #88 and #89; the plan's commit 4
(a negative control for the relay arm, TLA-002) and commit 5 (the platform
contract must name `seal-fence`, and the `segment-close` it already omits,
before a JWT-only fleet can relay); a mutation owner for `fence_relay.rs`
(`src/application/lifecycle` is no critical prefix, so adding one changes
what the gate selects).

Follow-ups (2026-10-01, on the owner's decision above): (4) the negative
control `TLA-002/nc-relay-loss-answers-false`
(`MC_SealTakeover_NcRelayLossAnswersFalse.tla`, the two-process layout)
answers a relay lost before it lands "not closed" with nothing queued at
the owner, and violates `ClosureAuthorized`. (5) The platform contract
(`contracts/streams-platform/v1/workload-token-claims.schema.json`,
CONTROL-PLANE-INTEGRATION §8.1) names `segment-close` and `seal-fence`,
the emulator's default workload token carries both, and the platform e2e
checks each on its route (edge #90, awaiting ratification). Still the
owner's: a mutation owner for `fence_relay.rs`. Not changed:
MULTITENANCY.md §14.1's r4 status block lists the eight operations of
r4; that document is a frozen contract, changed only by a
contract-revision commit.

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

**Owner decisions (2026-10-01).** "I approve all of these": of the four observations the
composition test recorded, three are the contract (the product `:seal`'s
retryable `temporarily_unavailable` for the unknown outcome; reads reporting
the stream sealed once the durable final closed it; a retry without producer
headers storing a second copy, since the producer headers are the
idempotency contract), and the seal-with-final 200 gains the
`Cache-Control: no-store` WIRE-MATRIX §2.5 lists.

**Owner decision.** "A retiring engine must stop publishing live state, but
retirement does not prove its accepted storage write disappeared ... Ratify
that model. Settle callers within a bounded period. When durable success
cannot be established, return the retryable shard-moving/unknown outcome,
retain owed-final obligations, and let the successor recover canonical state.
Do not publish success without the normal durability barrier, and do not
describe an ambiguous write as definitively rejected. Retries must preserve
producer/idempotency identity. The next useful test is the public append/seal
plus successor-ownership composition."

**Done: 445955e9** (`dst::dst_tests::retiring_written_group`, three DSTs over
the raw append, the raw close and the product `:seal`): the client gets the
retryable unknown answer, the successor runtime holds the record exactly
once, a producer-keyed retry is a duplicate, and an owed final stays owed
until its exact retry completes it. Observations left for the owner: the
product `:seal` renders the unknown outcome as `temporarily_unavailable`
(the same answer as "never written"); the successor's reads report the
stream closed before the retry marks the seal; the seal-with-final 200
lacks the `Cache-Control: no-store` WIRE-MATRIX §2.5 lists; a retry without
producer headers stores a second copy (plan D2/D7). The original task:

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
2. **Done: 88015cc5** (31 owners, each filter proven to select 11-111 tests;
   coverage gaps listed in its message). The original step: register mutation owners for the changed but unregistered sources in
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
  beside the counted admission pin. `has_pressure` and the `live_subs` term
  are deleted (2026-10-08: CI's mutation leg found their equivalent
  mutants). `plans/plans18/tracker-live-eviction.md` §2.1 also retires the
  pin (`admitting`, `pin`/`unpin`/`active`, `AdmissionPin::drop`),
  re-pointing the Loom model. Optional. The owner also asked
  to validate sustained churn beyond the ~905 s residence horizon and actual
  memory use in the Compute profile (≈36 first-seen projects/s at 32,768 is
  an estimate). Both figures are those of `ABSORB_AGE_SECS=300`; at 60, the
  binary's default since 2026-09-29 and what the deployments set, the
  horizon is ~665 s and the estimate ≈49 (edge record #78).
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
- **Flaky test (done), and the engine question (decided 2026-09-29, done).**
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
  ordering f574d733 already names as its cost. **Decided (the owner left the
  call to the implementer, 2026-09-29; edge change #73):** `close_db` settles
  a close error as closed when, after `Db::close` returned, the Db's own
  status carries a close reason other than Clean (`close_verdict`,
  `src/shard/history_partition.rs`): the Db had failed on its own, the close
  never won the result, and every Db task was joined either way. The shard
  then reopens as it does in the other two orderings. A healthy Db whose
  final flush fails keeps the reason Clean and stays Failed, final until
  restart. The race is wider than the writer's two steps: the close's read
  and write are not atomic either. Not done: the fix of the read and the
  write in SlateDB's close upstream, which a later pin may bring; reporting
  it upstream is the owner's to do or to ask for.
- **SIGTERM on a fully wedged executor** is never observed (the signal task
  runs on that executor), so it arms no stop bound; documented in WIRE-MATRIX
  §3 and RUNBOOK. An OS-thread signal path (sigwait or signal-hook) would
  close it; optional.
- **Closure debts across a rollback:** see item 2's rollback note.
- **Scaler loop survivors (owner decision, 2026-10-01).** "I approve all of these": a DST that
  runs the scaler loop may be selected by the `scaler` owner's filter.
- **Nightly rotation, first finding (2026-09-30).** Slot 6 (`scaler`) found 44
  survivors across its four shards; 970c9fd8 and d2fb3558 kill 42. The two
  left are inside `Scaler::start` (542:5 `start -> ()`, 575:53 the pass
  deadline) and need a DST that runs the scaler loop against an app state;
  placing one under the `scaler` owner's filter changes what the gate
  selects, so it is the owner's call.
- **Nightly mutation rotation (found 2026-09-28, when `slate` became the
  default branch; reshaped 2026-10-02 by the owner's decision).** The
  seven-night hash rotation ran buckets of 154-1,152 mutants and stopped at
  the first owner with a survivor, so every night failed early (2026-09-30 at
  `scaler`, its first owner; 2026-10-01 at `bootstrap_rss`, 5th of 24;
  2026-10-02 at `touch`, 3rd of 22) and most owners were never tested as
  whole files. **Done:** 15151a35 runs every owner of the night's group and
  fails once at the end with each owner's missed and timed-out mutants (an
  answer that measured nothing, such as a failing baseline, still stops the
  night); the commit that adds `scripts/quality/mutation-owner-sizes.json`
  packs the owners into groups sized to the job (RUST-QUALITY.md, "A
  scheduled group"). Measured with `cargo mutants --list` at 4472ee95: 6,945
  mutants over 164 owners (25 list none); the largest are `billing` 375,
  `shard` 352, `sse_feed` 346, `product` 340, `rollup` 336 and `http` 266.
  Cost from the four scheduled runs of 2026-09-29 to 10-02: every runner
  pays the owner's unmutated baseline (293-348 s on the service crate,
  430-543 s for the night's first) and then 1.4-3.4 min per mutant (2.3 on
  average; the rebuild is about 200 s), so the slowest runner of an owner
  with n mutants is modeled at 6 + 2.5 x ceil(n/4) minutes (harness owners
  1 + 0.25 x ceil(n/4)). The model matches or overstates the runs (09-29
  runner 0: 68 modeled, 63 ran; 09-30: 81-83.5 modeled, 47-80 ran; 10-02: 46
  modeled, 47 ran). The baseline term is 819 of the ~5,076 modeled minutes,
  so a budget in mutants alone would overrun a group of many small owners.
  A group holds at most 180 modeled minutes, three quarters of the job's 240.
  Result: 29 groups of 171.5-180 modeled minutes (209-386 mutants each);
  `billing`, `shard`, `sse_feed`, `product` and `rollup` exceed one night
  and are dealt over two nights each (111-123.5 minutes per part). A full
  cycle is therefore 29 days. Levers that would shorten it, each the
  owner's call: a wider `mutants` matrix (the baseline term does not shrink:
  8 runners model 17 nights, 16 runners 11), building an owner's baseline
  incrementally instead of in a fresh copy of the tree, or a cheaper
  per-mutant rebuild. **Taken on 2026-10-08** (owner decision, phase A Q3a):
  the `mutants` job is eight runners of 360 min, the cap 270 modeled minutes
  (three quarters of 360), and the owners deal into 12 nights; a push now
  fits about 900 selected mutants. Still open: a MISSED backlog with dispositions as the
  groups report, and re-measuring the sizes as owners grow
  (`python3 scripts/quality/mutation_driver.py --measure-sizes`; a new owner
  row needs its count before the unit tests pass).
- **Deferred with reasons (2026-09-28):** the 83 `gap_lock` holders that
  serialize about 126 s of CI's 154 s suite (drop the lock test by test,
  loop each in the parallel suite); Kani recompiling the crate for every
  check (compile once per obligation at the next deliberate `formal.py`
  change, which stales every receipt anyway); `build.rs` embedding the git
  HEAD, which rebuilds the crate after every commit (release provenance
  depends on it); and (registered by 88015cc5 on 2026-09-30) the seven production files under critical prefixes
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
  earlier revision). The binary no longer reads `ABSORB_PASS_BYTES`,
  `ABSORB_CONCURRENCY` or `ABSORB_SMALL_BYTES` (edge record #74): unset
  them in the projects the export shows still hold them. The export must
  also show no project that passes `--absorb-pass-bytes`,
  `--absorb-concurrency`, `--absorb-small-bytes` or `--gc-max-interval-secs`
  (such a process no longer starts), and no project that holds
  `HISTORY_GC_MAX_INTERVAL_SECS` with a value other than 600 (the sweep is
  600 s, fixed, since edge record #80: `HISTORY_GC_INTERVAL_SECS` is not
  read either). Since edge record #80 the export must also show no project
  that passes one of the eleven arguments that record lists (such a process
  no longer starts) and no project that holds one of its eleven environment
  names at a value other than the constant, because a retired name is
  ignored without a message: `WAL_GC_INTERVAL_SECS` (30),
  `WAL_GC_MIN_AGE_SECS` (60), `COMPACTIONS_GC_INTERVAL_SECS` (30),
  `COMPACTIONS_GC_MIN_AGE_SECS` (120), `GC_QUIET_INTERVAL_SECS` (600),
  `HISTORY_GC_INTERVAL_SECS` (600), `L0_MAX_SSTS_PER_KEY` (0, or the
  project's `L0_MAX_SSTS`), `WAL_GATHER_SKIP_REQS` (32),
  `WAL_GATHER_SKIP_BYTES` (1048576), `TAIL_MAX_BYTES` (1048576),
  `SSE_H1_MAX_BUF` (65536). A project that holds another value is the
  owner's to decide before the binary is deployed. Since edge record #81
  the same holds for `--absorb-pace-ms` and `--absorb-pace-window-ms`
  (refused) and for `ABSORB_PACE_MS` above 0 (ignored: the project stops
  pacing its gathers). Since edge record #82 it holds for
  `STORE_MAX_CONCURRENT` above 0 as well (ignored: the project's store
  calls are no longer capped by count; the v18 experiment of July set 48).
  Since edge record #84 it holds for `HISTORY_COMPACTOR=off` (ignored: the
  project's history databases compact again). Since edge record #85 it
  holds for `BILLING_METER=off` (ignored: the project is billed for the
  ingest and the storage of what it appends from the deploy onward; this is
  the one retired name that can change a bill).
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
- **Stale receipts:** none at 245c6ca8 (2026-09-30); receipts are re-recorded whenever their inputs change (last: TLA-011, TLA-016, TLA-018 and TLA-019 after the residue cleanup, five more after the configuration series and eight after the billing closes). Before that, all 24 were re-recorded on 2026-09-27 with the
  driver fix above. `python3 scripts/dev/formal_batch.py status` lists them.

---

**Owner decisions (2026-10-02).** "So I am accepting/ratifying all of these. Please implement it": edge records #90-#93 are ratified;
with #91 the owner accepted the hard `DELETE`'s added latency, the re-pinned
R13 test and the counter's new meaning; the scaler loop test's 10 s per
mutant is accepted; the nightly rotation runs every owner of its group and
fails once at the end, over enough groups that each night fits its 240
minutes; the settle and watch answers gain the `no-store` WIRE-MATRIX lists;
MULTITENANCY.md gets a revision naming the ten workload operations; the fence
relay gets a mutation owner. Still open, because they need the owner's
content and not a yes: the platform export (section 11), B3's four residues
(section 2), bug #7's migration command and rehearsal (section 7), item 40's
leftovers (section 4) and the release candidate of the mutation campaign
(section 9).

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

**Owner decisions (2026-09-29, second answer).** "I want the 1gig profile to
be the default"; every other question of the page is left to the
implementer's judgement.

**Owner ratification (2026-09-30).** "I accept them all": edge records #67 to
#85 are ratified as written, at the grades and surfaces they carry. The
questions this section attaches to a ratification stay listed as open
items for the owner; the ratification answers none of them by itself.

**Owner ratification of #86 (2026-09-30).** "Ratifying 86": the pace-gauge removal of the residue cleanup (eb39fab6) is ratified as written.

**Field evidence for the pump default (2026-09-30).** The owner asked for
the pump default of #77 to be measured on production hardware and Tigris;
`docs/PUMP-AB-REPORT.md` is the A/B (two cells of eu-central-1, tick
against the default, the same binary): lower acknowledgement latency at
every tier up to 48 producers, 4% more throughput, 6% fewer WAL objects,
garbage collection keeping pace, every acknowledged record durable. The
performance question that #77 left open is answered for that region; a SIN
or NRT run of the same harness (`SOAK_REGIONS` with two cells of one
region) would bound the WAL count where Tigris writes in about 20 ms.

Changed so far (package 2, then package 1, then package 3, then package 4,
then the five rows of package 5):
- The compaction worker and the bulk gate (648d7df4), the absorber's slot,
  packing limit and budget, the SlateDB runtime's threads and the shared
  cache (0b34b86f): edge record #67. The profile keeps its lines: its
  sha256 is pinned by release evidence and `bench/soak/oom-acceptance.sh`
  requires them.
- The shed line, 500 (6fb8d8f0): edge record #68.
- Feed retention, 64 MiB for the cell and half of it for one project
  (18a6eaf6): edge record #69.
- The cap on live subscriptions, 1,200 (f85990ab): edge record #70.
- The record ceiling, 131,072 bytes (fd093a39): edge record #71. With it every line of
  the profile is the binary's default, and `MEMPROFILE_CERT=compute-1g` is
  the only setting the profile adds.
- Package 1, preparation (no setting changes, no edge record): the
  effective-configuration tool's pin of HEAD's leaves was 155 and HEAD
  prints 156 since 0d40dc2a added `FORK_DEBT_SWEEP_SECS`. The pin is 156,
  `rename-map.json` declares `cli.fork_debt_sweep_secs`, and a unit test
  (`PinTest`, run by `scripts/quality.sh`) counts the fields of
  `src/config/cli.rs` and `src/config/model.rs` against the pin, so every
  removal that follows changes the pin with a failing test first. The
  tool's own K9 verdict needs a run of the tool (it builds rc.4 and HEAD):
  once, after the package.
- Package 1, settings that did nothing (edge record #74, one commit): the
  binary no longer declares the three v1 absorber options
  (`--absorb-pass-bytes`, `--absorb-concurrency`, `--absorb-small-bytes`
  and their environment names; the startup warning went with them), the
  three scaler names nothing read (`SCALE_COLD_PCT`, `SCALE_COLD_EVALS`,
  `MAX_SEGMENTS_PER_STREAM`), or the legacy aliases
  `--gc-max-interval-secs` and `HISTORY_GC_MAX_INTERVAL_SECS`. RUNBOOK
  §3.5b and docs/SCALING.md now state the merge rule the scaler runs (cold
  below 5% of the hot threshold, merge after four times `SCALE_HOT_EVALS`
  cold evaluations, no cap on segments); the documented 15% / 180 / 64
  policy was never implemented, and implementing it would be a scaler
  change for the owner to ask for. The tool's pin of HEAD's leaves is 150.
  Left for the next edit of each file, because a comment there is not
  worth its receipts: the doc comment of `absorber_config` in
  `src/bootstrap.rs` still speaks of legacy compatibility options
  (TLA-011), and the comment above `gc_interval` in `src/history.rs` still
  names the alias (TLA-016, TLA-018, TLA-019; the file has no line
  headroom). Not done in that commit: `--compactor-max-concurrent` and the
  second reader of `PATH_PREFIX`.
- Package 1, `PATH_PREFIX` has one reader (edge record #75): the read
  spool opens under the prefix clap resolved, as the stores and the rollup
  do, so `--path-prefix` on argv moves the spool with them. The copy
  `BillingConfig::path_prefix_env` and its key in the startup summary are
  gone; the tool's pin of HEAD's leaves is 149 and `rename-map.json` pairs
  the old leaf with `cli.path_prefix`. A deployment that gives the prefix
  through the environment only (every family in the repository) keeps its
  spool's location. For one that gave it on argv only and meters usage,
  the spool moves from `P/telemetry/read-spool/<instance>` to
  `P/P/telemetry/read-spool/<instance>`; nothing is migrated, rows left in
  the old spool are not billed, and RUNBOOK §11 says how to upgrade
  (`spool.depth` 0, then a graceful stop). No such deployment is known.
  For the owner with the ratification: whether not migrating is right.
  Still not done in package 1: `--compactor-max-concurrent`.
- Package 1, `--compactor-max-concurrent` reaches the compactor (edge
  record #76): the argument was parsed and never read, and only the
  environment overlay set the concurrency. Now `with_knob_defaults` copies
  the value clap resolved (argv, then `COMPACTOR_MAX_CONCURRENT`, then 1)
  and the overlay does not read the name, the rule edge record #48 gave
  the poll interval. An environment-only deployment, which is every script
  and family in the repository, runs as before. With both given, argv now
  wins where the environment did. A process certified with
  `MEMPROFILE_CERT=compute-1g` that passes a value other than 1 on argv
  does not start (two lines name the value); before, the argument was
  ignored and the process started. The literal 1 is written twice, in the
  clap attribute and in `impl Default for EngineConfig`, which every rig
  inherits through `ShardConfig::default()`; a test holds the two equal.
  With this, package 1 is complete. Not changed, and stale since the
  defaults moved: the `COMPACTOR_POLL_MS` row of RUNBOOK §3.2 and the
  `--compactor-poll-ms` help text still say `L0_MAX_SSTS` 64 and that drain
  continuity comes from concurrent compactions; the defaults are 32 and
  one compaction.
- Package 3, the commit pipeline (edge record #77): the binary's defaults
  are `WAL_GROUP_COMMIT=1`, `WAL_FLUSH_GAP_MS=10` and
  `WAL_POST_ACK_GATHER_MS=6`, what eight of the nine server families set.
  A server that sets none of them runs the group-commit pump, and SlateDB's
  own timer is its 1 s failsafe. Timing and the object-store request count
  only: on the local rig the pump gave P1 +31% requests per second at p50
  28.4 -> 15.7 ms for 2.2 times the WAL writes (`config-simplification.md`,
  "Measured"); the field measurement on Tigris, where a WAL write costs
  about 40 ms and a request, has not been made, and performance acceptance
  is the owner's. First the provider contract's SlateDB writer was made to
  flush as the configured pipeline does (under the pump nothing else
  flushes before the failsafe), with two runners that hold the SlateDB and
  HTTP cases under both pipelines. `scripts/bench-fra-ab.sh` sets
  `WAL_GROUP_COMMIT=0` to stay comparable with its baseline. Changed
  without an edit, for the owner to accept or pin: `bench/docker/compose.yml`
  (gap 25 -> 10 ms, gather 0 -> 6 ms),
  `bench/docker/harness/cluster-deploy.sh` (gather 0 -> 6 ms) and the six
  `bench/sse-probes` scripts, which set none of the three names (25 ms tick
  -> pump, gap 10 ms, gather 6 ms; only `sse-matched-loaded.sh` reports
  append and delivery-lag timing). `bench/livefeed-perf/run-one.sh` is not
  changed: it runs the pinned arm binaries of 1834b726 and 3a8016e6, whose
  defaults are the tick. Not done:
  deleting the switch, and `ShardConfig::default` (tests only; it stays
  tick mode until the switch goes). To run before the push, one at a time:
  conformance, the field gate, the platform e2e and its negative twin, the
  LiveFeed certification and the SDK smoke; they start the binary with
  `--flush-interval-ms 1 --wal-flush-gap-ms 2` or with nothing, so they move
  from a tick to the pump. CONFORMANCE.md says that the ~8.6 ms per append
  it records was a 1 ms tick and that the figure under the pump is not
  recorded yet. Green rigs will show that a pump runs (without one every
  append waits for the 1 s failsafe), not that the gap is 10 ms and the
  gather 6 ms: no rig reads the `pump` block of `/v1/debug/timings` or the
  start line `WAL group-commit pump on`, and the rigs that pass
  `--wal-flush-gap-ms 2` never run the default gap. For the owner: an
  assertion on either in CI's SDK smoke step, which starts the binary
  without the three names, would pin the default; it changes what a CI job
  checks, so it was not added.
- Package 3, the absorber's age threshold (edge record #78): the binary's
  default `ABSORB_AGE_SECS` is 60 (300 before), what eight of the nine
  families that set the name run. A server that does not set it absorbs a
  stream's tail once it is 60 s old or holds 4 MiB. It is the value the
  deployments run, not a measured improvement: the ladder's 5.18% against
  1.56% shed is one run that changed two variables, and the cost of up to
  five times as many age-triggered absorptions on sparse streams is not
  measured. Performance acceptance is the owner's. `scripts/bench-fra-ab.sh`
  sets 300 itself and keeps it. Not changed: `AbsorberConfig::default` in
  `src/history.rs` (tests only; it still says 300, the file has no line
  headroom and an edit stales TLA-016, TLA-018 and TLA-019). For the owner
  with the ratification: in a fleet the rebalancer's threshold
  `REBALANCE_LAG_SECS` is also 60 by default and reads the age of the
  oldest unabsorbed bytes, so the two defaults now meet; the fleet
  deployments set both to 60, and no run was made for this change. To run
  before the push, with the rigs listed above: they start the binary
  without the name. With them `scripts/mt-noisy-campaign.mjs` at its
  default `WINDOW_SECS=30`, as `scripts/promote-rc.sh` runs it: the
  victim's tail turns 60 s old near the end of the loaded window, so an
  age-triggered absorption can fall in the loaded window and not in the
  solo window, against locked thresholds. Changed without an edit:
  `bench/docker/compose.yml`, a three-instance fleet that sets neither
  `ABSORB_AGE_SECS` nor `REBALANCE_LAG_SECS` (300/60 -> 60/60), and the
  `bench/sse-probes` scripts. Two effects the record gained after the
  review of 2026-09-29: a stream seeded from the dirty index is published
  at the threshold itself (60 s where it was 300 s), and a stream's handle
  and its project's tracker entry are held about 665 s after a small last
  append where they were held about 905 s.
- Package 3, the two admission caps (edge record #79, medium): the
  binary's defaults are `ADMIT_MAX_INFLIGHT=512` (0, off, before) and
  `ADMIT_MAX_INFLIGHT_PER_STREAM=256` (64 before), what eight of the nine
  server families that set the instance cap set. A server that sets neither
  refuses an authenticated append with 429 `overloaded` and
  `Retry-After: 1` while more than 512 requests are in flight, refuses
  every request to a stream path with a pre-authentication 503 above 2,048,
  and admits 256 concurrent appends to one stream segment. The count covers
  every request on every route, parked long-polls included. They are the
  values the deployments run, not measured optima: no document derives
  either, and performance acceptance is the owner's.
  `scripts/bench-fra-ab.sh` and its family, which set the instance cap to
  256, now set `ADMIT_MAX_INFLIGHT_PER_STREAM=64`, the value they ran, and
  RUNBOOK's Docker example does the same: under an instance cap of 256 the
  new default would let one stream take every slot. RUNBOOK §3.6 gains the
  row of the per-stream cap, which it lacked, and WIRE-MATRIX the code
  `stream_overloaded`. Not pinned by a test: that the values reach the
  controller of a running server (`bootstrap::run` cannot be called by a
  test; the new tests restate its two field copies), and the wire answer of
  `stream_overloaded`. For the owner with the ratification: the A/B rig
  outside the repository (`~/.streams-ab`) runs P1 with 1,024 clients and
  must set `ADMIT_MAX_INFLIGHT=0`, or its figures change;
  `AWS-readyness.md` §3.2 item 7 still states the shed line's default as
  600 (it is 500 since edge record #68) and was not corrected here. To run
  before the push, with the rigs listed above and
  `scripts/mt-noisy-campaign.mjs`: they start the binary without the names,
  and whether one of them holds more than 512 requests in flight is not
  determined (the noisy-neighbour campaign holds at most 1 + `NOISY`, 49 by
  default). Changed without an edit: `bench/docker/compose.yml` with its
  overlays, and the six `bench/sse-probes` scripts.
  `bench/livefeed-perf/run-one.sh` is not changed (pinned arm binaries).
  Pinned after the review of 2026-09-29: the survival refusal of a read, a
  long-poll and the product surface at the default caps
  (`the_survival_refusal_covers_reads_and_the_product_surface_at_the_default_caps`).
  For the owner, from that review: a product seal whose final record the
  cap refuses keeps its Sealing claim, so ordinary appends answer 409
  `sealed` until the seal is retried or taken over after 15 s; whether the
  seal should check the cap before it claims, or release the claim on a
  capacity refusal, is a client-visible change that was not made. A refused
  `_ops_metrics` snapshot is dropped, not retried. Record #68 states the
  same two things too strongly for the memory shed line.
- Package 4, fourteen settings that nothing sets are constants (edge
  record #80, one commit): the five GC cadences and age floors of the shard
  databases (30/60 s, 30/120 s, 600 s; constants of `EngineConfig`) and the
  history sweep (600 s), the per-key L0 cap (always `L0_MAX_SSTS`), the two
  gather skips (32 requests, 1 MiB), the three per-role bucket arguments
  (every role uses `SLATE_S3_BUCKET`), the page of a read woken by a wait
  (1 MiB) and the h1 read buffer (64 KiB). Eleven arguments are refused by
  clap and eleven environment names are ignored without a message; every
  value is the former default. `ABSORB_READ_PAR` stays a setting on
  purpose; the other nine names of the package were not attempted. The
  plan recommended constants for twelve names (the GC intervals, the gather
  skips, the per-key cap and the buckets) and called the others the owner's
  taste: `TAIL_MAX_BYTES` and `SSE_H1_MAX_BUF` are among those others and
  were retired on the delegation, and they are the two a client could
  observe on a deployment that set them, so the record's surface is both
  (corrected after the review of 2026-09-29). Two
  techniques: where `shard_settings` or the overlay was the reader, the
  field or the overlay line is gone; where `bootstrap::run` reads the field
  (the gather skips and the buckets), the field stays in `CliArgs` with
  `#[arg(skip)]`, because `run` is frozen by its exception rows. The tests
  are in a file of their own, `src/config/retired_tests.rs`, which names
  every retired name. The tool's pin of HEAD's leaves is 143, and its
  control K6 now reproduces the refusal of `INITIAL_SHARDS=3`
  (`SSE_H1_MAX_BUF`, which it used, is not read); the tool was not run.
  `scripts/bench-fra-ab.sh` and its family no longer set
  `L0_MAX_SSTS_PER_KEY=0`, and `bench/sse-probes/sse-1per.sh` lost its
  `H1BUF` lever. `docs/STAGING.md` planned three buckets and now plans one:
  a Prisma bucket key is valid for one bucket and the server holds one
  credential pair. Residue, for the change in which the owner next updates
  the rows of `bootstrap::run`, so that TLA-011 and the bootstrap mutation
  owner are paid once: the five skipped fields, the `0 = never skip`
  conversion inside `run`, and the bucket parameter of `raw_store` and
  `store_for`. Removed on 2026-09-30 on the owner's instruction ("Please
  do the cleanup"), with the residue of package 5's third row: the fields,
  the conversion and the bucket parameter are gone, the gather skips are
  the constants `EngineConfig::WAL_GATHER_SKIP_REQS` and
  `WAL_GATHER_SKIP_BYTES`, the six rows of `bootstrap::run` were updated to
  the values the gate measures (`scope_lines` 583 -> 573, `syntax_facts`
  977 -> 968), and the tool's pin of HEAD's leaves is 132. Residue for the
  next edit of each file: the comment above
  `gc_interval` in `src/history.rs` names `HISTORY_GC_INTERVAL_SECS` and
  its alias (TLA-016, TLA-018, TLA-019), and the comment above
  `tail_max_bytes` in `src/http.rs` says "Env TAIL_MAX_BYTES" (a critical
  mutation prefix). Residue in the startup summary: the keys
  `history.gc_interval_ms`, `http.tail_max_bytes` and `http.h1_max_buf`
  still print, for values nothing outside the code can set, until the
  fields leave the model (the summary never held the eleven clap settings,
  so no summary line changed with this package). For the owner with the
  ratification: SPEC.md D6 decided three shared buckets, one per role; no
  configuration reaches that layout now, and SPEC.md (D6, §3.1, §8) says
  one bucket since the correction, to confirm or to revise. The levers given
  up (stopping the quiet or history sweeps, a longer WAL retention, a
  larger h1 buffer) now need a rebuild; OPERATIONS.md specifies a 24 h WAL
  floor for a backup feature that is not built, and when it is, the floor
  is a code change. The platform export (§11) must show no project that
  holds one of the names at another value.
- Package 5, first row: a gather never parks between read waves, and
  `ABSORB_PACE_MS` and `ABSORB_PACE_WINDOW_MS` are gone with the pacing
  code (edge record #81, one commit). The two arguments are refused by
  clap and the two environment names are ignored without a message. The
  park was off by default and in every deployment of the repository, so a
  gather runs as it did; the source had recorded the pacing as measured
  harmful (L1d8) and kept for experiments. Removed: the two `CliArgs`
  fields, the two fields of `AbsorberConfig`, `Pacing` and
  `pace_between_waves` in `src/history/gather.rs`, the DST
  `gather_pacing_preserves_outcomes_and_opens_windows` with its helper
  (the inventory holds 616 tests), the line of `bench/soak/wc-ladder.sh`
  with its levers `WC_PACE_MS` and `WC_PACE_WINDOW`, and the lines of the
  two `wc-ladder` families. The tool's pin of HEAD's leaves is 141; the
  tool was not run. The comment above `gc_interval` in `src/history.rs`,
  a residue of package 4, is corrected in the same commit, because the
  file's three receipts are staled by it anyway. Receipts staled: TLA-011
  (`src/bootstrap.rs`), TLA-016, TLA-018, TLA-019 (`src/history.rs`,
  `src/history/gather.rs`), 115.9 min serial; CI also selects the
  mutation leg of the owner `bootstrap` and Miri. The two environment
  names had no test of their own; since the review of 2026-09-29 the child
  process of `a_retired_name_in_the_environment_changes_nothing` holds
  them, each with a value clap refused while it parsed them. **Not done, for the
  owner:** the counter `GATHER_LAST_PACE_MS` and its three reporters
  (`gather_last_pace_ms` on /v1/debug/load and in the ops gauges,
  `absorber.lastPaceMs` on /v1/debug/absorb) stay and report 0, because
  `collect_snapshot` in `src/ops.rs` has an exact exception row
  (`syntax_facts: 466`) that fails on any change, a deletion included.
  Removing them is commit 2 of the package's plan: the owner rewrites the
  row to the value the gate prints after the four lines of the gauge are
  deleted (the function then has 202 lines, so its architecture budget
  exception of 206 stays needed), and the removal gets an edge record of
  its own (operator-debug) and a WIRE-MATRIX edit. Done on 2026-09-30 on
  the owner's instruction ("Please do the cleanup"): the counter, the
  gauge and the two fields are gone, the row of `collect_snapshot` reads
  `syntax_facts` 457 (was 466), and the removal is edge record #86
  (operator-debug, low; awaits ratification) with the WIRE-MATRIX lines
  of both routes. The platform export
  (§11) must show no project that passes one of the two arguments or holds
  `ABSORB_PACE_MS` above 0.
- Package 5, second row: store operations are not capped by count, and
  `STORE_MAX_CONCURRENT` is not read (edge record #82, one commit). The
  name had no argument; the environment name is ignored without a message,
  and the startup summary loses the key `storage.store_max_concurrent`.
  The cap was off by default and in every deployment of the repository,
  so store calls run as they did; its only measurement (EXPERIMENT-PILOT
  run 12b, `STORE_MAX_CONCURRENT=48`) was a negative result. Removed: the
  field of `StorageConfig` and its overlay line, the semaphore of
  `StoreResources` with `permit` and its `#[expect]`, and the six call
  sites in `src/store_timing.rs`. The byte gate is not changed.
  `docs/runtime-resources.md` still listed a store-I/O concurrency gate as
  owned by the runtime and was corrected after the review of 2026-09-29.
  The tool's pin of HEAD's leaves is 140; the tool was not run. No receipt is staled,
  and the files select no mutation leg and no Miri leg. **For the owner:**
  the R10 mechanism test
  `runtime_store_concurrency_is_shared_locally_and_independent_of_first_access`
  exercised the semaphore. It is rewritten on the byte gate under the same
  name (two runtimes with byte caps of 1 and 2, two stores of one runtime
  sharing one gate, a held byte of one runtime not delaying the other,
  exact capacity afterwards, teardown) and its sha256 is re-pinned in
  `docs/refactor/review-mechanisms.json`; the sibling pinned test is
  untouched. The audit had listed the rewrite as a question for the owner.
  The platform export (§11) must show no project that holds
  `STORE_MAX_CONCURRENT` above 0.
- Package 5, third row: the fleet's desired count has no assumed-capacity
  dimension, and `SCALE_RPS_CAPACITY` is not an option (edge record #83,
  one commit). `--scale-rps-capacity` is refused by clap and the
  environment name is ignored without a message. The dimension was off by
  default and in every deployment of the repository, so the fleet scales
  as it did, on utilisation, edge slots, the hot instance, ack latency and
  edge latency. Removed from `src/fleet.rs` (999 lines, 1,011 before): the
  field `capacity_rps` of `FleetCfg`, `need_rps`, its place in the desired
  count and in the shrink target, and the token `(need_rps)` of the reason
  string in `fleet/desired.json`, in the log line "fleet desired" and in
  `GET /operator/data.json`, which relays the document as `fleet.desired`;
  the string now ends `rps=R live=L`. The measured rate stays: it gates the
  edge-latency dimension. The RUNBOOK §3.5 row and the name in
  COMPUTE-SPEC §4 are gone; the `SCALE_LATENCY_MS` row of the same table
  still explained the ack-latency dimension by an rps signal that scales
  out and was corrected after the review of 2026-09-29. The tool's pin of HEAD's leaves stays 140,
  because the field stays; the tool was not run. Receipt staled: TLA-011
  (`src/fleet.rs`), 12.1 min; CI also selects the mutation leg of the
  owner `fleet` and Miri, and neither was run. **Residue, for the owner's
  next update of the rows of `bootstrap::run`** (with the residue of
  package 4): the field `CliArgs::scale_rps_capacity`, which stays with
  `#[arg(skip)]` and is always 0, and the boot line "fleet coordination on
  (prefix=P, cap=0 rps)", which still prints it. Removed on 2026-09-30 on
  the owner's instruction ("Please do the cleanup"), with the residue of
  package 4: the field is gone, the boot line reads "fleet coordination on
  (prefix=P)", and the rows were updated. The platform export (§11)
  must show no project that passes the argument or holds
  `SCALE_RPS_CAPACITY` above 0.
- Package 5, fourth row: the history compactor is always on, and
  `HISTORY_COMPACTOR` is not read (edge record #84, one commit). The name
  had no argument; the environment name is ignored without a message, and
  the startup summary loses the key `history.compactor_off`. The compactor
  was on by default and in every deployment of the repository, and the
  1 GiB certificate refused a process that turned it off, so the history
  databases open as they did: the embedded compactor on the resolved
  worker options, L0 caps of 64. Removed: the field of `HistoryConfig` and
  its overlay lines, and the branch of `history_settings` that opened
  without a compactor and with L0 caps of 1,000,000 (`src/history.rs`,
  1,621 lines, 1,630 before). The certificate's guard against a disabled
  compactor stays. Given up: the bench hook for discard-mode runs
  (`s3lite --discard-substr`), which no script used; the help of that
  argument still offered the mode for the history tier and was corrected
  after the review of 2026-09-29. The tool's pin of
  HEAD's leaves is 139; the tool was not run. Receipts: TLA-016, TLA-018
  and TLA-019 list `src/history.rs` and were stale before this change, so
  the re-record that is already due does not grow; the files select no
  mutation leg and no Miri leg. The platform export (§11) must show no
  project that holds `HISTORY_COMPACTOR=off`.
- Package 5, fifth and last row: ingest is metered for every customer
  stream, and `BILLING_METER` is not read (edge record #85, one commit;
  medium). The name had no argument; the environment name is ignored
  without a message, and the startup summary loses the key
  `billing.meter_enabled`. Metering was on by default and in every
  deployment of the repository, so appends are counted as they were: every
  append to a stream that is not `_`-reserved carries its billing reference
  and is counted in the write of its records. With the switch off the
  committer skipped the billing row, so neither the ingest nor the storage
  of those appends was billed, and `BILLING_MODE=required` did not refuse
  it. Removed: the field of `BillingConfig` and its overlay line, the
  field `meter_enabled` of `AppendService` and the conjunct of
  `execute_once` that read it (`src/application/append.rs`), and its line
  in `AppState::append_service` (`src/http.rs`, 3,101 lines, 3,102
  before). The unmetered branch stays, for the `_`-reserved streams.
  WIRE-MATRIX's metering line no longer names the switch. The tool's pin
  of HEAD's leaves is 138; the tool was not run. Receipt: TLA-003 lists
  `src/application/append.rs` and was stale before this change, so the
  re-record that is already due does not grow. CI selects the mutation leg
  of the owner `http` and Miri; neither was run. **For the owner:** this
  is the one change of package 5 that can alter a bill. A Compute project
  that still holds `BILLING_METER=off` from the OOM review's experiments
  is billed for what it appends from the deploy onward; the platform
  export (§11) must be searched for the name before the binary is
  deployed. Such a project's appends also gain the committer's read and
  write of the billing row, and answer 500 `internal` where that row
  cannot be read or is invalid; the record said without a condition that
  no answer of an append changes and was corrected after the review of
  2026-09-29.

---

## 14. K2 parity: storage layout 5, fewer uploads, shared cells

The owner's goal (2026-10-02, revised 2026-10-07): launch at prices as close
to Cloudflare K2's as the costs allow, with small tenants, no minimum fee and a
small accepted subsidy of small tenants. The decisions behind this section are
the README rows dated 2026-10-07. Working notes with the arithmetic live
outside the repository, under `~/.streams-k2/analysis/` on the owner's machine
(`compression/`, `upload-once/`, `reprice-100ms/`, `wal-low-throughput/`,
`shared-cells/`, `routing/`, `throughput-history/`).

### 14.1 Prices and billing

- Retention: $0.05 per stored GB-month. Stored means the page bytes we write
  after our own compression.
- Produce: $0.16 per stored GB written (K2 parity for data that compresses 4x),
  with a 1 KiB minimum billable size per append request.
- Consume: $0.04 per GB of the customer's bytes delivered.
- Needs code: a monthly stored-bytes-written meter (none exists;
  `owned_frame_bytes_current` is a gauge) outside `bill_append`'s `#[expect]`
  scope, and the 1 KiB minimum. Both are edge changes.
- Router-leg egress is priced at $0.01/GB until the platform lowers it.

### 14.2 Order of work

1. **Storage layout 5, compressed pages** (README, "Storage layout 5 page
   format"; cryptography accepted). Comes with run leases for consumer groups.
   Three small additions before it lands: a golden vector with non-zero
   timestamp deltas, the delta base fixed, and a page builder that takes a
   timestamp per record (they keep cross-request pages possible without a new
   layout). History pages use the row tag `'g'`, because the history keyspace
   already uses `'p'` for postings (`src/postings.rs:65`).
   The owner's landing calls of 2026-10-08 (README rows L1-L6 and T10): its
   five edge records (#125-#129) are ratified as drafted; `CheckedPage`
   replaces `CheckedFrame` in the compiler fixtures and the source gate (L2);
   `decode_frame` keeps one reasoned `dead_code` allow (L3); and
   `CryptoConfig::frame_compress` is removed in one commit before it lands
   (L4). After landing, the keys CLI decrypts a page on the library crate
   (crypto review F9) and `frame_bytes` keeps its name until the billing
   meter renames it (L6). Run leases' option 3 and the SDK iterator that
   retries a key's later messages after `msg.retry()` come later, together
   (T10).
2. **The 100 ms write tier**, the single tier for everyone, with the WAL
   failsafe at 60 s and the usage drain at 8 s (each an edge record).
   **Implemented:** `WAL_FLUSH_GAP_MS` defaults to 100 (edge record #97),
   SlateDB's own flush timer on a shard log under the pump runs every 60 s
   plus the per-shard offset (#98), `TELEMETRY_DRAIN_SECS` defaults to 8
   (#117; its R09 mechanism test re-pinned on the owner's approval of
   2026-10-08), and the per-stream usage answer states the read-loss window
   of that cadence (#118), each awaiting ratification. The cross-server
   split gate in fleet mode (#119, 14.6; CI's `livefeed-fleet-cert` and the
   other split rigs take their split outside fleet mode first) and the
   fleet shard default (#120, 14.4; the fleet rigs pin their shard counts
   until `home-v1`) land with it. The 100 ms point of 14.8 is not yet
   measured.
   Two drain bounds follow (T9, 2026-10-08): a round takes up to 256 dirty
   rows per shard, not 64 (implemented, edge record #121, awaiting
   ratification), and a graceful stop's terminal round is bounded by
   min(cadence, 5 s), not one cadence, with a low-risk edge record.
3. **"WAL plus one copy" (design B) as layout 6, before launch,** after a
   throwaway seal spike proves at most 2.2 uploads per stored byte and bounded
   memory. Each stored byte is uploaded about 8 times today: WAL 1.16, shard
   L0 1.05, shard compaction 1.56, history L0 1.02, history compaction 2.9 and
   growing. Only the WAL and one final copy are required by a guarantee. Design
   B seals each shard's unabsorbed pages from the ring into one immutable
   object per retention class before each memtable flush, writes index rows
   and exact zero-lag trims, and removes history2, the absorber and history
   compaction.
4. **Shared cells, phase A**, in parallel with 1-3 (14.4).
5. **Cross-request pages**, after layout 5 lands: consecutive appends of one
   lane within one flush window share a page, cut whenever the authenticated
   credential changes (one principal per page).

### 14.3 Retention

- Unlimited by default; any stream may set a policy: a maximum age of at least
  1 h, or an explicit trim. A size cap comes later.
- Expiry runs on server commit time, never on the client's timestamp.
- Billing is exact to the page. Forks inherit their source's policy.
- No archive tier: Prisma Buckets expose Tigris Standard only, and retention
  keeps about 70% margin there at $0.05.
- Client-visible: retention on create and update, 410 for reads below the
  retained start, consumer groups skipping expired records (8-9 edge records,
  with design B's stage 3b).

### 14.4 Shared cells and shards

- **Launch shape: one single-server shared cell** (one 1 GiB single-core
  server, one shard, no routers, fleet off). Phase A of the shared-cells plan
  applies to it, without router hardening.
- **As few shards as possible.** Every shard is its own SlateDB with its own
  WAL writer, and at the 100 ms tier each busy writer costs up to about $97 a
  month whatever it carries. The WAL bill follows the number of writers, not
  tenants or commits. **Decided (2026-10-07), implemented:** the fleet-mode
  default `INITIAL_SHARDS` is the largest power of two at most `FLEET_MAX`
  (`fleet_shard_default`, `src/config/validation.rs`; it was
  `next_pow2(4 x FLEET_MAX)`, 16 shards for a 4-server cell), and the notice
  inverts: `ShardsExceedFleetMax` warns when a fleet sets `INITIAL_SHARDS`
  above `FLEET_MAX` (more WAL writers than servers), where `CoarseInitialShards`
  warned below `4 x FLEET_MAX`. Edge record #120, awaiting ratification. Until
  `home-v1` (14.5) replaces the rendezvous draw (`ring_pick`,
  `src/ownership.rs`), `S = n` leaves servers without a shard: over
  `streams-1..n` the draw is `[0, 2]` for 2 over 2, `[0, 1, 1]` for 2 over 3,
  `[1, 1, 0, 2]` for 4 over 4 and `[1, 1, 0, 1, 1]` for 4 over 5 (the old 16
  over 4 drew `[4, 3, 1, 8]`). While an autoscaled fleet grows from one
  server (`FLEET_MIN` 1), the step to two servers moves both shards of a
  `FLEET_MAX` 2 or 3 fleet to `streams-2` at once, and with 4 shards the
  step from three servers to four leaves `streams-3` without a shard (edge
  record #120). Until `home-v1` the fleet rigs pin their own shard counts
  (16 over four servers, 8 over two or three; the owner's decisions of
  2026-10-08, T3 and T8). Only the rebalancer evens it out, and only
  once the loaded server's absorber lags more than `REBALANCE_LAG_SECS` (60 s)
  for two ticks; the load-aware return-home then keeps the move.
- **Rejected:** a WAL journal shared by a cell's servers (servers must stay
  uncoupled, so more servers do more work); durability classes, lazy leases
  and a pump linger (they save nothing once every writer is saturated).
- **Phase A after it lands** (README rows "Shared cells Q0" to "Q10",
  2026-10-08), in order: the `bootstrap::run` wiring of the cell ceiling
  (Q2(a): the owner updates its six rows; TLA-011; Q10(b)'s queued-bytes
  axis; an edge record); the heap profile and fix of 14.7's RSS retention
  (Q0); the profile's Q1(A) values (`PROJECT_MEMORY_PRESSURE_BYTES=33554432`,
  `MAX_REQUEST_BODY_BYTES=8388608`; edge #110 amended; the C5 and C6 legs);
  the wrapper's feed polling (Q6(a)) and the release posture on a fleet-off
  cell (Q6(b)), each with an edge record; idle journal retirement (Q10(a))
  and the lifecycle walk's close retiring an expired stream's journal; the
  runbook (key custody, the keys-feed cutover, u = 0.5 packing by hand, what
  to alert on); `cell-admin rotate`; then the harness and the certification
  (Q5). Before self-serve: C4 (watch definition bytes), C7 (empty pulls
  charged to the read quota), C8 with Q2(b)'s row, Q1(D), the four mutation
  owners Q3(d) defers, and phase B.

### 14.5 Multi-server cells and routing (later, on the new Compute generation)

Placement (which server runs each shard, and so each WAL writer) and
partitioning (splitting one stream into key-range segments, each key in one
segment) are separate layers, as Pravega's containers and segments are.

- **Platform:** the new Prisma Compute generation provides one hostname per
  cell spreading requests over its servers, `/i/<server>/` routed to a named
  server on that hostname, no 404 while a server wakes, and long-lived
  unbuffered responses. It also removes today's ~1,500-connection edge cap.
- **Placement `home-v1`:** shard *i* lives on server *i*;
  `INITIAL_SHARDS` equals the number of servers. No spare server: Compute
  starts a replacement in under a second. This replaces the rendezvous draw,
  which puts 4 shards on 4 servers as [1, 1, 0, 2] (FNV-1a over `streams-N`
  names, `src/ownership.rs:12-88`).
- **Misroute contract, no router we run:** a server that does not own a stream
  forwards non-waiting requests (appends up to 1 MiB, settles, creates, pulls
  without wait) one hop to the owner, answers a same-origin `307` to
  `/i/<server>/...` for waiting ones (long-poll, SSE, pull with wait), never a
  3xx on a write, and `503` + `Retry-After` on an exhausted hop. The owner is
  named in `Streams-Owner`; the SDK pins it per stream. No cookie stickiness:
  ownership is per stream, a cookie per client.
- **Core package** (placement, the misroute contract, forwarding, SDK pinning):
  about 14-19 days, before the first multi-server shared cell. It retires the
  pilot router from the production path.

### 14.6 One stream across servers

- Segment splits are built (ROUTING-V3) but not correct across servers:
  consumer groups never receive the high child (B1), an ambiguous append
  retry across a split can commit twice (B2, open item F1-b), and watches miss
  the child (B3).
- **Decided (2026-10-07), implemented: cross-server splits are off.** The
  controller declines a split while the server runs in a fleet, that is
  once its fleet loop has published a ring of any size
  (`Controller::declines`, `src/scaler3/controller.rs`, through
  `ShardDirectory::ring_published`), pinned by
  `dst_tests::scaler_split_gate`; a hot stream then gets `429` at its
  per-stream limit instead. A ring of one is gated too: a split there
  became cross-server once the ring grew (review finding F1, option B).
  Fleet off still splits. Edge record #119, awaiting ratification. The
  split package's last commit removes the gate and inverts those DSTs.
- The Compute cluster rung C1 (`bench/docker/harness/cluster-run.sh`) split
  inside a four-server fleet, which the gate refuses. Implemented (T7,
  2026-10-08): it takes its split on streams-1 outside fleet mode
  (`cluster-deploy.sh solo`, `cluster-run.sh solo`), then redeploys all four
  in fleet mode over the same `PATH_PREFIX` (`cluster-deploy.sh up`,
  `cluster-run.sh`), as T2's rigs do; a script change, not run (the field
  run stays the owner's).
- **Before public launch:** the split package (consumer record relay and an
  ancestry-based stop rule, F1-b with its fleet-internal lane read, a touch
  relay for watches, DLQ through forwarding, per-segment rate buckets, a
  cross-server merge probe, and a two-server DST written red first), about
  17-28 days. Shard splits (SPEC D3) come after launch.

### 14.7 Defects found on the way, not yet fixed

- After a load, RSS stays at 499-562 MB and never returns below the 500 MB
  shed line: the idle server refuses its own telemetry appends and a second
  load is shed from its first request. Needs a heap profile.
  On the single-server shared cell (14.4) that is an instance-wide write shed
  no tenant caused and nothing attributes; it is fixed before the first cell
  (README, "Shared cells Q0").
- A history partition's manifest poll (300 s, `src/history.rs`) turns a full
  history L0 into an absorption stall of up to 300 s: Layer A's A6 at 1,000
  projects stalls that long beside the scale module. Watched for in the
  shared-cell certification; shortening the poll stales TLA-016, TLA-018 and
  TLA-019 and the 300 s assumption in `verification/assumptions.md`.
- The pilot router hashes the bare stream name, which no longer matches the
  layout-4 route hash: about 63-65% of first hops in a 4-server cell get `409`
  and are replayed. The SDK never follows `Streams-Replay-To`.
- Two `sharddir` mutants (`492:28`, `653:26`) differ only when the clock reads
  exactly the holdoff deadline; removing them needs one `holdoff_verdict(now)`
  helper and owner-approved exception-growth rows.

### 14.8 Measurements to run when the machine is idle

- The 100 ms model check: one local point (`bench/k2cost/run-local.sh` with
  `WAL_GAP=100`), about 2 h.
- Throughput history: today's binary against July's and August's on the same
  shapes in a 1-CPU, 1 GB container against a local store with 40 ms latency
  (runs R1-R4), about 12 h plus 1 h of builds. The 27 MB/s of July was
  x-padded data compressed 17-30x with absorption deferred; nothing shows a
  regression for the same job.
- Layout 5, after it lands (L5, 2026-10-08): one heap profile per arm, with
  Q0's memory work (incompressible data reaches the RSS shed line sooner; the
  cause is not found), and a controlled single-record rerun on an idle host
  (1.25x layout 4's CPU; the codec explains 0.02-0.05 ms of the 0.16 ms
  gap). No rule that skips zstd for small bodies: stored bytes are billed,
  so it would be an edge change.

### 14.9 Held cost levers

E2 (a live-read block cache for the shard log, `keep/e2-live-read-cache`) is
held because layout 5 rewrites the shard-log scan it changes. E7 (fleet reads
without LIST, `keep/e7-fleet-reads`) waits for the owner's choice on its edit
of the R09 mechanism test.
