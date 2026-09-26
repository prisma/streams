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

---

## 0. How work is done in this repository

**Branch and commits.** Work directly on `slate` (no PRs; the owner's call).
One behaviour per commit. The title is a sentence stating the behaviour
("A usage streamId is served only for an incarnation of the stream the URL
names"); the body says why, what changed, the red/green evidence (exact
panic text), controls, and ledger changes. End every message with:

```text
Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
```

**Red first.** Every behaviour change starts with a test that fails on the
unfixed tree; paste the exact failure into the commit message. Assert exact
state (ledger bytes, record counts, stored rows), not only "no 500". Plant a
wrong fix where cheap and show the test catches it.

**Owner decisions.** Anything client-visible is an edge change: add a record
to `edge-changes.md` (same format as #53-#57: surface, endpoint, condition,
before, after, retry semantics, who is affected, pinning tests, risk reason)
and update `docs/refactor/WIRE-MATRIX.md`. Where the owner has not decided,
write the options down and ask; never self-approve. An agent never adds rows
to `docs/quality/exception-growth.json` and never edits an `#[expect]`
reason to absorb growth (`docs/RUST-QUALITY.md`, `AGENTS.md`).

**Gates before every push** (all on the commit being pushed):

```bash
scripts/quality.sh                      # must end QUALITY_OK
cargo test --release --lib              # full suite (1,335 at a079315c)
bun test ./deploy/supervise.test.ts ./deploy/stage-app.test.ts
node scripts/platform-e2e.mjs && node scripts/platform-e2e-negative.mjs   # when HTTP, boot or lifecycle changed
```

The mutation leg, exactly as CI selects it (zero MISSED and TIMEOUT
required):

```bash
export QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) \
       QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate
scripts/quality/mutations.sh
```

`scripts/quality.sh` needs Python 3.11 or newer as `python3` on `PATH`.
It also runs the formal-verification check (`formal.py check`: an invalid
or mismatched receipt fails, a stale one is reported). A change to code a
Kani proof or TLA+ model maps makes its receipt stale; the release gate
requires them fresh. The toolchain setup and the commands to run or refresh
an obligation are in `verification/README.md`, "Setting up the toolchain".

**Ratchet rules that shape the code** (the gates enforce them; plan for them
before writing):

- An excepted scope (an item carrying `#[expect(...)]`) may not grow: its
  contract measures `scope_lines`, `nested_items` and `syntax_facts`, and an
  `unwrap_used`/`expect_used` contract also fingerprints every ordinary call
  spelling and every path spelling, counted per site. Method calls
  (`x.foo()`) are not fingerprinted; `foo(x)`, `Ok(..)`, a new local's path
  and `self` are. Put new logic in a new function outside the scope and reach
  it by a method call. Removing an exception (by making the code fallible)
  is always allowed.
- A file over 1,000 lines may not exceed its line count at `origin/slate`
  (physical lines, comments included). At a079315c: `src/shard.rs` 3,139,
  `src/http.rs` 3,118, `src/product.rs` 4,024, `src/billing.rs` 2,151,
  `src/registry.rs` 1,452, `src/auth.rs` 1,637, `src/history.rs` 1,655.
  Offset additions by trimming, or add a module.
- DST test files (`src/dst/tests/*.rs`) stay at or under 1,000 lines. A new
  one needs a `#[path]` module line in `src/dst/dst_tests.rs`, a
  `by-path-module` row in `docs/quality/owners.json`, and
  `python3 scripts/test-inventory.py --write`.
- `docs/refactor/review-mechanisms.json` pins test function hashes; after
  editing a pinned test, re-pin it and run
  `python3 scripts/review-evidence.py --check`.
- The architecture gate counts reverse-dependency edges per file: a new file
  naming `crate::http` fails. Import `AppState` through the parent module
  (`use super::AppState`), as `src/product/seal_request.rs` and
  `src/billing/replaced.rs` do.
- `pub(super)` on a field under an exception adds a `super` path fact to its
  contract. Keep such fields private and add accessor methods.

**Traps.**
- `scripts/quality/mutations.sh` lists mutants from the live checkout, per
  owner. Editing or committing while it runs fails it with "Diff content
  doesn't match source file". Run it in a separate `git worktree`.
- `scripts/quality.sh` expects its build under the checkout's own
  `target/`: `CARGO_TARGET_DIR` breaks its Python tests. In a worktree,
  symlink `target` to a warm build instead.
- `verification_plan.py` needs `QUALITY_HEAD_SHA` to be the checked-out HEAD.
- Clippy denies `drop()` of a `Copy` value (use `.ok()`),
  `result_large_err` for `Result<_, Response>` (box it or use a small error
  enum), and deep nesting (factor helpers early).
- macOS's `/bin/bash` 3.2 treats an empty array as unbound under `set -u`
  (use `${a[@]+"${a[@]}"}`).
- `dst::dst_tests::topology_scaling::post_split_throughput_scales` fails its
  1.8x ratio under host load; rerun it alone when the host is idle.

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

Rollback note for the record: an older binary ignores the debt objects (they
live outside the descriptor), so a rollback leaves debts unsettled until
roll-forward.

---

## 3. Typed registry read error for an append's first read

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
- **SIGTERM on a fully wedged executor** is never observed (the signal task
  runs on that executor), so it arms no stop bound; documented in WIRE-MATRIX
  §3 and RUNBOOK. An OS-thread signal path (sigwait or signal-hook) would
  close it; optional.
- **Closure debts across a rollback:** see item 2's rollback note.

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
- **KANI-006 (ready, held for the owner):** the postings varint codec, on branch
  `formal/kani-006` (FORMAL_OK, 4 checks, no finding). It cannot land without
  an owner decision: `src/postings.rs` is compiled by path into
  `tools/quality-invariants`, and three owner-approved exact-state rows in
  `docs/quality/exception-growth.json` (`tools/quality-invariants/src/lib.rs`,
  `crate::postings`, `dead_code`/`unreachable_pub`/`unused_imports`:
  nested_items 126, scope_lines 1397, syntax_facts 2601) measure that whole
  file. Declaring `#[cfg(kani)] mod proofs;` there changes those values, and
  an agent may not update the rows. The branch also lacks the
  `docs/quality/owners.json` macro-dsl rows for its `kani::cover!` sites (see
  KANI-005's row). The same holds for any harness over a module the
  invariants or fuzz crates include by path: `postings.rs` (so KANI-007 to
  KANI-015's postings owners), `crypto.rs`, `tenant.rs`, `product_cursor.rs`,
  `queue.rs`, `quota/bucket.rs`, `retained_bytes.rs`, `rollup/allocation.rs`,
  `rollup/storage.rs`, and `application/read_{batch,budget,retention_probe}.rs`.
  Those crates also needed a `build.rs` declaring `cfg(kani)` and
  `#[rustfmt::skip]` on their by-path `mod postings;` (both on the branch).
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
- **Driver flake on macOS (not fixed):** `scripts/quality/formal.py` `stop_group`
  raises `PermissionError` from `os.killpg(pgid, 0)` when macOS answers EPERM
  for a group whose only members are killed, unreaped children, so
  `test_formal.TlcIsolation.test_6_a_{timeout,cancellation}_kills_the_whole_process_tree`
  fail intermittently in `scripts/quality.sh` on macOS (Linux CI answers
  ESRCH). The fix is one `except PermissionError: pass` in that loop (13 of 13
  runs passed with it), but the driver is an input to every receipt, so
  changing it makes all receipts stale and CI's formal job re-runs every
  obligation. Leave it to the formal program's owner.
- **Stale receipts:** re-recorded on 2026-09-25 for the twelve obligations
  whose inputs slate's changes had moved (see the receipts commit).

