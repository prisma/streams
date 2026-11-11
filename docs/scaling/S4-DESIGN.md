> **Repository copy** (push 4, 2026-11-10) of the design document `S4-DESIGN.md`, rev 2.1 (2026-11-04), package 5: the topology substrate and the manual split, as it stood when the code landed. Where it and the code differ, the code is the authority; later decisions are in the edge records (`docs/reviews/2026-09-hardening/edge-changes.md`), the RUNBOOK and the module docs. Paths under `~/.streams-k2/` and the sibling documents it cites (reviews, decisions, run results) are the design work's own files outside this repository.

# S4: topology substrate and manual split — detailed design (rev 2.1)

**For:** the lead, then Fable's check. **Package:** 5. **Date:** 2026-11-04.
**Tree:** slate `d16f1b05a`; the fork at rev `b10b0e87` (tag `streams-20261102-b10b0e8`).
**Supersedes rev 1** (`S4-DESIGN-rev1.md`). **Addresses** every finding of `S4-REVIEW.md` (Fable 5.1,
proceed after fixes) and the lead's message of 2026-11-04; §0.1 maps each finding to its section.
**Decisions** Fable took under the owner's delegation are applied as decided (§15).
**Rev 2.1** folds in Fable's rev-2 check (`S4-REVIEW-rev2.md`: PROCEED AFTER FIXES, all 19 findings
closed, both fork facts verified):
- N1, the clone's unfenced parent-manifest CAS: §4.3, §6.4, §13, §16, and the never-leaf path contract in
  §15;
- N2, renewal store errors: §4.2;
- N3, opening the kept child early: §6.2 step 2.4, §9.3;
- the two extra fork tests: §2.1.

## 0. Summary

**What changes from rev 1, by finding:**

| Finding | Severity | What rev 2 does |
|---|---|---|
| 1 | CRITICAL | Only an opener of the parent claims a lapsed intent, after its own open fenced the actor. A fleet tick only reports. Pre-`'T'` the actor stops on a failed renewal and on a fenced Db; post-`'T'` it never stops (§4.3, §6.4). |
| 2 | HIGH | The intent is never deleted: it is CASed to a terminal state. `'T'` alone suffices to roll forward (§4.1, §6.4). |
| 3 | HIGH | No compactor inside the pause (§6.2). |
| 4 | HIGH | C4 is a fork change that writes the seed as an SST at clone time: no open in the pause (§6.2, §9, §2.1). The fallback, a maintenance open without a compactor, is specified. |
| 5 | HIGH | `'N'` enters the delta through the committer's touched-queue set, with per-category and per-project sum checks (§7.2). |
| 6 | MEDIUM | Revalidation on a received forward with a newer version. An unknown prefix or home is a retryable `503`, not a holdoff strike. Held operations are re-routed locally when this instance owns the child (§3.4, §6.2 step 4). |
| 7 | MEDIUM | The claimant claims before opening any child. A fenced child writer ends the actor's run for good (§4.3). |
| 8 | MEDIUM | `'T'` and the intent are read together. `'T'` with a live lease answers `503`; `409` comes only once the version has moved (§6.3). |
| 9 | MEDIUM | The intent is a move reservation, the transition capacity is held (`reserved_shards`), and any override on the parent is refused (§4.4, §10). |
| 10 | MEDIUM | A lapse is measured on the claimant's monotone clock (§4.2). |
| 11 | MEDIUM | Readiness reports `\|W\|` and the `open_index` wall time separately. Package 4 bounds the concurrency (§9.3). |
| 12-19 | LOW/LOW-MED | §13, §6.2 step 4.2, §3.3, §15, §11, §1, §8 and §5.2 respectively. |

**Two new facts from the fork check** (§1):
- **Depth 2 works.** A projected clone of a projected clone holds exactly its range, both ancestors carry their
  final checkpoints, and after a full compaction it references no ancestor SST.
- **The pause must flush the WAL before `create_checkpoint(All)`.** Writes still buffered in the WAL when a
  `checkpoint(All)` runs are written past the replay point its memtable flush covers. The fork then refuses
  every projected clone of that log until newer writes are flushed. The sequence "WAL flush, then
  `checkpoint(All)`" is clean, also when repeated, as a retry would run it. The test pins both.

**The pause** (§9):
- with C4: ≈ 27-30 sequential round trips, 0.7-1.2 s at 25-40 ms each on Tigris (estimate; Fable's count,
  checked against §1's measurements);
- with the fallback: ≈ 40-44.

`SHARD_SPLIT_HOLD_MS` stays at 2 s until Tigris measures the arm that ships (Q3's condition).

## 1. ASM-SLATEDB-PROJECTION: the fork check (S4's first commit)

`src/keyspace_projection_tests.rs`, `#[path]` from `src/keyspace.rs`, with an `owners.json` row. It holds
two tests (dev profile, about 2.5 s together). Preflight passes; the gate is running for the commit.

1. **`a_projected_clone_holds_exactly_the_sources_rows_in_its_range`** (rev 1's test):
   - the `Durable`-scope refusal;
   - two parallel projected clones from a `checkpoint(All)` hold exactly their range at the checkpoint
     (merged `'P'` values, deleted tails absent, the sentinel only in the upper child, post-checkpoint
     writes in neither);
   - the child's own operand merges before and after a compaction of every table it holds.

   A recording store with 10 ms injected per request prints the request counts and sequential round trips
   of each phase (§9).
2. **`a_projected_clone_of_a_projected_clone_holds_its_range_of_both`** (new, finding 17):
   - the lower child takes its own operands in the grandchild's quarter;
   - then, **twice, as a retry would run it**: `flush()` (WAL), `create_checkpoint(All)`, a projected clone
     of the quarter. Both clones succeed;
   - the grandchild holds exactly the child's rows in the quarter at its checkpoint (`a1;b1;c1;`);
   - its manifest's `external_dbs` are exactly `[("p/src", final checkpoint), ("p/low", final
     checkpoint)]`, and each ancestor lists that checkpoint;
   - after a compaction of every table it holds, both entries reference **0** SSTs (detach GC drops the
     entries later).

**What the check found on the way** (it shapes §6.2):
- **`checkpoint(All)` with writes buffered in the WAL leaves a data WAL past its replay point.** The probe
  on the child: WAL 5 (331 B) held part of the operands. `checkpoint(All)` without a WAL flush wrote WAL 6
  (498 B) *after* the memtable freeze it flushed, so the checkpoint's replay point stayed at 5. A later
  `flush()`, memtable flush and `Durable` checkpoint did not move it past 6, because the memtable was empty.
  The fork refused every projected clone (`InvalidCloneSourceWithWal`).
- With `flush()` first, the buffered writes land in WAL 6 *before* the freeze. The checkpoint's replay point
  covers 6, and the clone succeeds, also on a second round.
- The rule is therefore the pause's sequence: **barrier, `flush()`, `create_checkpoint(All)`**. A violation
  fails closed (the clone refuses), and §6.4 rolls back.
- An upstream issue goes to slatedb/slatedb with the fork commit: "a checkpoint of everything with WAL
  writes buffered leaves an uncovered data WAL". It is not a blocker.
- **`Manifest::cloned` copies the parent's `writer_epoch`.** A single-source child inherits its parent's
  epoch, so the equal-span tie-break across homes rests on `home`, as designed. ASM-HOME-ORDER records this.

## 2. Scope, build order, dependencies

| # | Commit (one behaviour each) | Depends on |
|---|---|---|
| 1 | The projection test, depth 1 and 2 (§1) | — (lands now) |
| 2 | **Fork:** `CloneBuilder::with_seed` (C4), plus C3's reuse of loaded manifests (§2.1), on prisma/slatedb `streams`; a new pin tag; measured in Streams before the pin moves (the fork rule) | — |
| 3 | Versioned topology: `homes`, `leaves`, `next_home`, ETag revalidation, the swappable snapshot, unknown prefix → retryable `503` (§3) | S3a's `home_of`/`object_root` |
| 4 | `Streams-Topology-Version` on `409`, `Streams-Forwarded: v=`, the second hop, revalidation on a received newer `v` (§3.4; E2) | 3 |
| 5 | The intent: create or CAS over a terminal object, lease, renewal, the monotone lapse, terminal states, the heartbeat report (§4) | 3 |
| 6 | `MaintenanceGate`, every writer behind it, `request_purge` → `Busy`, the lock order (§5.1, §5.3) | — |
| 7 | The barrier as a `SealCommit` kind: the park, the hold, the drain, the intake twin, the local re-route on `Retired` (§5.2, §8; E1) | 6 |
| 8 | The committer's touched-queue mark (§7.2) | S3b part 1 (state rows) |
| 9 | Open-path fence: `'T'` and the intent together, claim only here (§6.3) | 5 |
| 10 | Sentinel helper: snapshot pass, delta, `'N'` re-read, sum checks, reference lists (§7) | S3a's primitives, 8 |
| 11 | The split state machine, its takeover and rollback GC (§6) | 2 (or its fallback), 3-10 |
| 12 | The move reservation: return-home and rebalance skip sources, `reserved_shards` (§4.4, §10) | 5 |
| 13 | Operator routes (§11; E3) | 11 |
| 14 | `fleet_shard_default` → 1 (Q2) | — |
| 15 | TLA-045 with its controls; Kani harnesses; re-records (§12) | 11 |

**Dependencies:** S3a landed (layout 8, `'Y'`, `'W'`/`'M'`, the three primitives); S3b part 1 for `'N'`; P4
(`split_demand`); S1's open-transient and busy MB.

### 2.1 C4 and C3 in the fork (sized on the `streams` branch at `b10b0e87`)

**C4, `CloneBuilder::with_seed(rows: Vec<(Bytes, Bytes)>)` plus `with_sst_format(...)`:**
- In `create_clone` (`clone.rs:46-98`), after the clone manifest exists and before the
  `initialized = true` CAS (`clone.rs:89-93`):
  - build the sorted seed rows into one or more SSTs with `TableStore::table_builder` and `write_sst` under
    the clone path. Their ids are fresh `SsTableId::Compacted`; their sequence numbers are above the
    projected tables' `last_l0_seq` (`manifest/mod.rs:437`);
  - put their views at the front of the dirty manifest's L0 and raise `last_l0_seq`/`last_seq`;
  - then the existing init CAS.
- **Atomic with initialization:** a crash before the CAS leaves an uninitialized clone, which the fork
  refuses to open. A resumed clone (the `Some(uninitialized)` arm) rebuilds the seed. An SST orphaned by the
  first try is unreferenced garbage under the clone path, for the fork's GC.
- **The seed is puts only** (sentinel rows), so no merge operator is needed at clone time.
- **The SST format** must match what the child's opener expects (block size, filter policy, compression).
  The clone takes it from the caller (`keyspace::sst_format()`, shared with `db_builder`) rather than
  guessing.
- **Size:** about 120 lines of code, about 200 lines of fork tests. **M**, as Fable judged. The tests:
  - the seed is visible after open;
  - a crash before init leaves the clone uninitialized, and a resume rebuilds the seed;
  - the seed survives a compaction;
  - seq ordering against projected tables;
  - an empty seed is a no-op;
  - **(Fable's two)** the seed's value is what the child sees for a key on which the parent also holds a
    sentinel row: the parent holds a sentinel row, the projection excludes its range, and the seed writes the
    same key;
  - **(Fable's two)** a child opened with the WAL on replays nothing of the seed: the seed is in L0, not in
    the copied fence WAL.

**C3** (rides the same PR):
- `create_clone_manifest` re-loads each external DB's manifest per entry, and the clone re-reads the GC
  boundary after every manifest operation.
- Reusing the source manifest it already loaded (`build_source`) for the matching `external_dbs` entry
  saves about 3 sequential round trips per clone. Measured, not assumed.

**Process** (the fork rule, memory `slatedb-fork`):
- one commit per patch on `streams`, and an issue on slatedb/slatedb linking it;
- a new tag `streams-YYYYMMDD-<sha>`;
- Streams adopts it only after same-day gate + PACK-S-MUX-0200/PACK-T-1000 + queue 1x/10x/100x show nothing
  worse.

The pin bump is its own Streams commit.

**If C4 slips:**
- the fallback is the **maintenance open**: `compactor_options: None`, no GC, `manifest_poll_interval` 60 s,
  the shared block cache scoped by path;
- it writes the seed batches (≤ 4 MiB each, each flushed; `'Y'` in the last), then `flush()`, then `close()`;
- the serving open after the publish starts the compactor.

## 3. The topology substrate

### 3.1 `topology.json`

Fields as in rev 1 §3.1: `version`, `shards`, `homes`, `leaves`, `next_home`, `last_op`,
`max_request_body_bytes`.
- **Paths:** initial homes are `shards/<prefix>`; every operation-born home is `shards/<prefix>-<home>`
  (agreed).
- **`version` changes only on a published split or merge.** A `next_home` bump changes the ETag but not the
  version.
- **Every write is a CAS on the ETag**: `PutMode::Update` with the `UpdateVersion` read. This covers the
  home allocation and the publish (finding 13, §6.2 step 4).
- `Topology` moves to `src/topology/model.rs`. `load_or_init_topology`'s literal becomes
  `Topology::initial(shards, body_ceiling)`, which shrinks its scope; checked at implementation that no
  growth row pins it.

### 3.2 Snapshot and revalidation

The directory holds `RwLock<Arc<TopologySnapshot>>`. It is revalidated:
- on the fleet tick (GET with `If-None-Match`);
- coalesced, one request in flight:
  - on a received `409` with a newer `Streams-Topology-Version`;
  - **on a received request whose `Streams-Forwarded: v=n` exceeds our version**, before it is routed
    (finding 6a);
- on a fleet-off cell, by the actor itself after its CAS.

**A prefix or home absent from the snapshot** (a routed hash meets a child this instance cannot see yet)
answers a retryable `Opening`-class `503`, `Retry-After: 1`, and triggers the revalidation. **It never
answers `OpenFailed`, and never strikes the holdoff ledger** (finding 6a).

A swap closes the engines of prefixes no longer leaves (`begin_close`, `RetirementReason::Split`).

### 3.3 The prefix code and the entity range

As in rev 1 §3.3, plus the `FF×16` disposition (finding 14):
- the hash `FF×16` is in **no** leaf; the key space `FF×16 ‖ …` is sentinel-only;
- **S3a's creation path refuses a stream or queue whose hash is `FF×16`**. That is package 4's line, asked
  through the lead (§15); it is unreachable for a full-width keyed hash, but enforced;
- the Kani harness states the rule;
- the projection range of prefix `p` is `[lo(p), hi(p))`, with `hi` = `FF×16` for the all-ones prefix.

### 3.4 The version header and the forward (E2, ratified with receiver-side revalidation)

As in rev 1 §3.4:
- `409 not_ring_owner` carries `Streams-Topology-Version`;
- the forward sends `Streams-Forwarded: v=<n>`;
- one more hop only on a strictly newer version, at most two hops;
- a bare legacy value counts as `v=0`.

Plus the receiver side of §3.2: a forwarded request with a newer `v` revalidates first. A child the receiver
still cannot see answers the retryable `503`, never a `409` that routes back to the parent.

**Pins:**
- exactly two hops through a split;
- one `409` at equal versions;
- a client's own header is never forwarded;
- the unknown-prefix `503` with no holdoff strike.

## 4. The intent: one per cell, leased, never deleted

### 4.1 The object and its terminal states (finding 2)

`topology/intent.json`, one object per cell (agreed).

**Fields** (rev 1 §4.1's, plus):
- `state`: `Preparing` | `Reclaimed` | `RolledBack`;
- `outcome`: `null` | `{ result: "published" | "rolled_back", reason }`.

**Writes:**
- **Creation:** `PutMode::Create` when the object is absent. If it exists and is **terminal**, a CAS over it
  (`PutMode::Update` on its ETag) with a new `op_id`. If it exists and is live, the route answers
  `409 topology_busy {reason: "intent"}`.
- **No DELETE, ever.** `Reclaimed` (after publish and checkpoint deletion) and `RolledBack` are CAS
  transitions on the ETag that carry `lease_epoch`. A stale actor's late write fails its CAS. The fence and
  the busy check treat a terminal intent as none.
- **`'T'` suffices to roll forward.** It names `op_id`, the child homes and `version_seen + 1`. The child
  paths follow from the homes. An opener that finds `'T'` with no live intent rolls forward from `'T'` alone
  (§6.4). Re-creating the intent for bookkeeping is optional.

**`GET /operator/topology.json`** shows the live intent's derived state, and a terminal intent's `outcome`
(finding 16).

### 4.2 Lease and lapse (finding 10)

- **Renewal:** the actor renews every `TOPOLOGY_LEASE_MS / 3` (3.3 s) by CAS on the ETag (`lease_until_ms`
  and nothing else). `lease_until_ms` stays as a floor and for display.
- **A renewal that fails with a store error** (timeout, 5xx, a lost reply), not a CAS mismatch, **is
  retried** at the next renewal tick and does not fail `owned()` (N2). The actor's `owned()` fails only on:
  - a CAS mismatch (a real claimant);
  - its parent `Db` reporting fenced;
  - its own monotone clock passing `TOPOLOGY_LEASE_MS + TOPOLOGY_LEASE_SKEW_MS` since its last *successful*
    renewal, the claimant's rule mirrored.

  A lost reply to a renewal that did land shows up as a CAS mismatch on the next try against our own new
  ETag. Re-reading the intent then shows our `lease_epoch` and `lease_owner`, so the actor adopts the new
  ETag and continues.
- **Lapsed** means: the intent's ETag stayed unchanged across the claimant's own observations, spanning at
  least `TOPOLOGY_LEASE_MS + TOPOLOGY_LEASE_SKEW_MS` (10 s + 1 s) on **its monotone clock**.
  - On the fence path (§6.3) that is two GETs, the second after the span.
  - The fleet tick's cached ETag observation may supply the first, if it is fresh and taken on this
    instance's clock.
  - No comparison of `lease_until_ms` with a local wall clock decides a lapse.
- **Constants** (provisional, agreed): lease 10 s, renewal 3.3 s, skew 1 s, gate deadline 5 s, checkpoint
  lifetime 1 h.

### 4.3 Who may claim, and when the actor stops (findings 1, 7)

- **A fleet tick never claims.** It reads the intent (cached by ETag) and publishes a lapsed one in the
  heartbeat (`topology_intent: {op_id, lapsed_for_ms}`) for operators and S5.
- **The only claimant is an opener of a source** (for a split, the parent), in the one open path (§6.3):
  1. open: the SlateDB fence bumps the writer epoch and writes the fence WAL, which fences the actor's parent
     writer, so the actor's `'T'` flush cannot land;
  2. read `'T'` and the intent;
  3. if lapsed, claim by CAS (`lease_owner`, `lease_epoch + 1`);
  4. then roll back (no `'T'`) or forward (`'T'`).

  It claims **before** opening any child (finding 7).
- **Every source is fenced before the decision** (package 6, S6 rev 5). The claim path opens **each source
  the intent names** (N of them: a split has 1, a merge 2), maintenance-opened when this instance does not
  own it, before it reads `'T'` in all of them and CASes the claim. The decision is: `'T'` in any source →
  forward; in none → back.
  - On a rollback, the claimant closes each source it does not own, and its ring owner re-opens it.
  - A source opened only for the claim never serves; the fence keeps it out anyway.
  - This prevents the case where a claimant fences one source, decides "no `'T'`" and rolls back, while the
    slow actor still holds another source and writes `'T'` there.
  - TLA-045 control `nc-claim-fences-one-source` (→ `AckedReadable`/`RetiredNeverServes`, package 6's) runs
    on the shared claim action; its DST is package 6's.
- **The parent's opener** is:
  - its ring owner, after the actor's instance left the ring;
  - or its home owner, after an override lapse (which §4.4 now prevents during a live intent);
  - or **the actor itself**: an actor whose instance stays the ring owner and whose engine is still open
    re-claims its own lapsed intent through that engine (a CAS with `lease_epoch + 1`; its writer is the
    current epoch) and continues.
- **The actor, before `'T'`,** stops at the first step boundary where `owned()` fails (§4.2's three causes:
  a CAS mismatch, its own lease clock, or its parent `Db` reporting fenced (`CloseReason::Fenced`)), **or**
  when any write to the parent fails with a fence error.
  - It rolls back only what the claimant's GC would also delete (idempotent deletes of its own child paths
    and lists).
  - It never writes `'T'` after a failed `owned()`.
  - A fenced write fails anyway, which is what makes this safe; the check is an early exit, not the safety
    argument.
- **Clones are bracketed by `owned()`** (N1). A clone does not go through the parent's writer: it CASes a
  final checkpoint onto the **parent's manifest** through `ManifestStore`, which carries no writer epoch,
  and it writes the child manifest and (C4) seed SSTs.
  - So the actor checks `owned()` immediately before each clone call and immediately after it.
  - On a failed after-check it runs its rollback (the takeover GC on its own child paths: their
    `external_dbs` final checkpoints, then their files, then its lists) before exiting.
  - This narrows the window. It is not the safety argument: a late clone loses nothing (`'T'` cannot land),
    and the never-leaf sweep (§15) removes what a dead actor leaves behind.
- **The actor, after `'T'`,** ignores lease loss and finishes: seed already done, publish idempotent on
  `last_op`, Reclaimed idempotent. **A fenced child writer ends its run for good.** In the fallback arm a
  claimant opened the child; the claimant with the higher `lease_epoch` finishes, and the actor never
  re-opens (no ping-pong). With C4 there is no child writer in the pause, so this case does not arise.

### 4.4 The intent as a move reservation (finding 9)

- While a live intent names a prefix as a source:
  - return-home (`rebalance_return_secs`) and the rebalancer skip that prefix (one cached GET of the intent
    per tick, by ETag);
  - `pick_move_target` and count balance treat the actor as holding `reserved_shards` more (2 for a split),
    published in its heartbeat until the intent is terminal.
- The split route **refuses any override on the parent** (`409 topology_busy {reason: "override"}`), not
  only a young one.

## 5. The mutation gate, the barrier, the lock order

### 5.1 `MaintenanceGate`

Unchanged from rev 1 §5.1: S6 §7.1's writer inventory, which Fable re-checked as complete for slate plus
S3b; `request_purge` → `Busy` by `try_acquire_shared`; one test per caller.

**Preparing's "collection off" flag** covers:
- the orphan sweep's DELETEs;
- consolidation's object PUTs and commits;
- the cleaner;
- purge reclaim;
- the run-index merge.

Seals, folds, trims and `request_purge` continue: their rows are sentinel (the delta covers them) or objects
(the supplementary list covers them). A barriered seal's object PUT is an orphan under the parent's path,
for S6's sweeper (finding 15, §15).

**The "final" flag** (push 4, DESIGN-DECISION-c3-incomplete-rig-2026-11-11 Addendum 5): from the `'T'` a
split or merge wrote, the source's log is final. The operation sets the flag while it holds the gate
exclusive, at the point from which it only rolls forward, and never clears it. A writer queued on the gate
that gets it after the operation drops its hold writes no batch (`write_index_batch_held` refuses, and
`request_purge` answers `Busy`), so the source's rows stay exactly what its successors copied.

### 5.2 The barrier without a `CommitOp` variant (Fable's decision on O1)

**The barrier rides `CommitOp::Seal(Box<SealCommit>)` as a new `SealCommit::Barrier(BarrierReq)` kind,**
dispatched through the existing `Seal` arm, as `Seal(Purge)` already is.
- **The committer loop hands `&mut rx` to `commit_group`** (`self.commit_group(ops, &mut rx).await`: zero
  net lines in `src/shard.rs`). The park, the hold's drain at 2 s and the `Retired` answers then live in
  `src/shard/transaction/barrier.rs`.
- **Before writing it,** the implementer measures the `SealCommit` enum's contract and the dispatch sites'
  contracts. Only if one bites does a row go to Fable, with the gate's `after` values.
- **Semantics** (finding 19): the barrier takes effect **after the group that contains it**.
  - The committer commits that group, makes it durable and answers it, then answers the barrier and parks.
  - Operations dequeued after the barrier's group wait in the queue, never offered to a group.
  - The TLA-005 model's `Barrier` action commits its group first.
- **The intake twin:** after the hold deadline, `try_command` and the direct senders see the engine's
  `splitting_overrun` flag and return `EnqueueError::Splitting`, which renders `503 shard_splitting`,
  `Retry-After: 1`. They never touch the queue. `try_command`'s `#[expect]` scope is measured first. If a
  line there bites, the flag check moves into the callers' shared enqueue helper, which is outside the scope.
- **Release outcomes:**
  - `Resume` (a rollback): the queue proceeds in order.
  - `Retired { version, children }`: §6.2 step 4's local re-route, else `409` with the version.

### 5.3 The lock order

As in rev 1 §5.3:
- order: gate exclusive (the operation only, with a 5 s deadline), then the barrier, then engine internals;
  writers take gate shared, then `refs`, then one batch and its PUT;
- never hold the gate across a committer op, a GET loop, a sleep or another engine's gate;
- the deadline rolls the attempt back, before any `'T'`;
- DSTs: the purge's committer step in flight at the quiesce; a writer stalled past the deadline.

A rollback for the deadline CASes the intent to `RolledBack {reason: "gate_deadline"}` (finding 16).

## 6. The split as a durable state machine

### 6.1 States and durable witnesses

| State | Durable witness | Written | In the pause? |
|---|---|---|---|
| **Preparing** | `topology/intent.json` (`Preparing`, op id, source `(prefix, home)`, child homes, `version_seen`, lease); reference lists `path(A)/lineage/refs/<child home>` | home CAS, intent create/CAS, list PUTs | no |
| **Quiesced** | the checkpoint `topo-<op_id>` on the parent (named, 1 h lifetime) | `flush()`, then `create_checkpoint(All)` | yes |
| **ChildrenReady** | each child's manifest `initialized` with the seed SSTs in its L0 (C4); in the fallback, each child's `'Y'` in its last seed batch | clones with seed (C4); or clone, maintenance open, seed, flush, close | yes |
| **SourcesRetired** | the parent's `'T'` = `(version_seen + 1, [h0, h1], op_id)` | one batch, `flush()` | yes |
| **Published** | `topology.json` at `version_seen + 1`, `last_op.op_id = op_id` | one CAS on the ETag | yes (ends the pause) |
| **Reclaimed** | the intent CASed to `Reclaimed`; `topo-` deleted | DELETE (checkpoint), CAS (intent) | no |
| **RolledBack** | the intent CASed to `RolledBack {reason}` | CAS | no |

### 6.2 The steps

**0. Preparing** (unpaused):
1. **Refuse:**
   - an intent that is live: `409 topology_busy {reason: "intent"}`;
   - any override on the parent: `409 topology_busy {reason: "override"}`;
   - a 24-bit prefix, or a parent carrying `'T'`: `422 not_splittable`;
   - the reservation: `409 instance_full` (§10).
2. The home CAS (`next_home += 2`, on the ETag), then the intent create or terminal-CAS, with the lease.
   From here §4.4's reservation holds.
   - **Order invariant** (package 6's never-leaf sweeper relies on it): the home CAS, then the intent
     create/CAS naming the homes, and only then any list PUT, clone or child-path write. No object under a
     child home exists before an intent names that home.
   - Pinned by a red test: a store wrapper that fails any write under `shards/<p>-<h>` or `refs/<h>` while
     `h` is not in the live intent.
3. **The Preparing fold** (K-SPLIT-DECISION-2026-11-09 §2.1): `fold_once` on the parent until the report's
   `more` is false, at most 8 calls (8 × `max_rows` 4096 rows), outside the pause and under no gate. A fold
   error is logged and the split proceeds: the copy is correct at any `'K'` size, and the fold only shrinks
   the seed. Nothing folds in the pause.
4. **The snapshot pass:** a SlateDB snapshot; collection off (§5.1); the committer's touched-queue mark set
   (§7.2).
5. **The helper's `plan`** (§7): routed and recomputed sentinel rows per child, `'R'`/`'O'`/`'N'` per child,
   and the reference lists. **PUT the lists**, in parallel.

**1. Quiesced** (the pause starts at the barrier):
1. The gate exclusive, with its deadline.
2. The barrier, answered after its group (§5.2).
3. **`flush()`** (the WAL), then **`create_checkpoint(All, name "topo-<op_id>", lifetime 1 h)`** (§1: the WAL
   flush first is required).
4. No fold, no seal. **`'K'` is copied to both children byte for byte, like `'I'`** (`SplitRule::CopyToBoth`;
   K-SPLIT-DECISION-2026-11-09 §2). The key keeps the parent's home, and no row is ever re-encoded: a re-encode
   of a filtered row would shift every later entry's derived `chunk_off`. Every reader of a `'K'` row decodes
   it whole and admits only its range, through package 4's `spill::directory::split::decode_row_in_range`: the
   open's `rebuild_directory`, the fold's `scan_unfolded`, this split's `'R'`/`'O'` recompute, and S6's counts.
   Unsealed pages stay as rows, and `'W'`/`'M'` are routed by hash. Cost against §9.2: +0 RTT (each child's
   seed carries the parent's unfolded `'K'` rows, ≤ 1-2 MiB without the Preparing fold, kilobytes with it).

**2. ChildrenReady:**
1. **The delta** (§7.2): the sentinel diff, plus the re-read of the touched queues' state rows.
2. **The checks, or roll back:**
   - Σ children `'R'` = the parent's `'R'`, and likewise `'O'`, per object;
   - Σ children `'N'[cat]` = the parent's `'N'[cat]`, per category and per project;
   - counters `topology_split_refcount_mismatch`, `topology_split_gauge_mismatch`.
3. A delta that adds a holding the lists lack PUTs `refs/<child>-d` under that path.
4. **C4:** per child, in parallel: `CloneBuilder` from the named checkpoint, the child's entity range,
   `with_seed(child rows)`. The clone is initialized together with its seed. There is no open and no flush.
   **As soon as the kept child's clone is initialized** (the kept child is the one whose hash ring picks this
   instance under the new snapshot), its **serving open** starts in parallel with the other clone, `'T'` and
   the topology CAS (N3).
   - It is a fresh path, so it fences nobody.
   - It is not installed in the serving map until the snapshot is installed (step 4.3).
   - A rollback closes and deletes it with the child path.
5. **Fallback:** per child: the clone, then the **maintenance open** (no compactor, no GC), the seed batches
   (`'Y'` last, each flushed), `flush()`, `close()`. The split DST asserts **no compaction CAS on either
   child path before the topology CAS** (finding 3).

**3. SourcesRetired:**
1. `'T'`, `flush()`.
2. The parent engine stays open, parked: it is closed (`begin_close`, `RetirementReason::Split`) only
   after step 4.4's `Retired` release (implementation departure 4, §14.1). A close here would drain the
   held operations as `shard_moving` before the re-route could take them.
3. From the durable `'T'` the actor only rolls forward and ignores lease loss (§4.3).

**4. Published:**
1. **The CAS on the ETag** (finding 13), setting:
   - `version_seen + 1`;
   - `leaves` (`p` replaced by `p0 → h0`, `p1 → h1`);
   - `homes[h0]` and `homes[h1]`, with `parents [h_p]` and `born_version`;
   - `homes[h_p].retired_version`;
   - `last_op`.
2. **On a failed CAS,** re-read:
   - `version == version_seen` and `p` a leaf (a concurrent `next_home` bump): retry on the new ETag;
   - `version == version_seen + 1` and `last_op.op_id == op_id`: done;
   - anything else: **stop**, with the counter `topology_publish_conflict` and the operator route's intent
     state `stuck`. No server-side paging; an operator resolves it. This is unreachable with one intent per
     cell.
3. Install the snapshot.
4. **Release the barrier with `Retired`,** and for each held operation (finding 6b):
   - **if this instance owns the child** its hash routes to (ring pick under the new snapshot), open that
     child's engine (the serving open, compactor on) and re-submit the operation to it, unchanged. It was
     never offered to a group, so re-submission is unambiguous, and it is acked with the child's offset;
   - **else** answer `409 not_ring_owner` with `Streams-Topology-Version`, which the forward takes one hop to
     the child's owner (E2).
5. Pin: a one-instance cell acks every held append after the CAS with its exact offset.

**5. Reclaimed** (unpaused): delete `topo-<op_id>`; CAS the intent to `Reclaimed`, with outcome
`published`. The clones' final checkpoints stay until each child's detach GC releases them (S6). Between S4
and S6, the retired parent's path and its zeroed (`'G'`) objects leak, counted by
`topology_retired_homes_pending_gc` (finding 15); S6's first commit drains it.

### 6.3 The open-path fence (S6-REVIEW 1; findings 1, 8, 18)

**Every engine open** (owner, takeover, override, the actor's own re-open) runs this, after SlateDB's
open-time fence:

| `'T'` in the log | Intent naming this incarnation as source | Snapshot version vs `'T'`'s | Result |
|---|---|---|---|
| no | none, or terminal | — | serve |
| no | live (not lapsed by §4.2) | — | do not serve: every request, reads included, `503 shard_splitting`, `Retry-After: 1` (E1's fence row) |
| no | lapsed | — | claim (CAS), **roll back** (§6.4), CAS `RolledBack`, then serve |
| yes | live | snapshot < `T.version` | `503 shard_splitting` (the actor is between `'T'` and publish) |
| yes | lapsed, terminal or absent | snapshot < `T.version` | claim (CAS over the intent, or create one) and **roll forward from `'T'`** (§6.4), then `409` with the new version |
| yes | any | snapshot ≥ `T.version` | `Retired { successors }`: `409 not_ring_owner` with the version; the forward takes it to the child |

The claim, and every decision on `'T'`, happens after this open fenced the previous writer, so no `'T'` can
appear after it is read. A child is never a source of its own birth: its open checks its initialization (C4)
or `'Y'` (fallback), plus the intent row for a merge of it (S6).

### 6.4 Recovery by the claimant

| Found at the claim | Durable state | The claimant does |
|---|---|---|
| no `'T'`, the intent `Preparing`, no `topo-` checkpoint | intent, maybe lists | **roll back:** delete the lists (`refs/<h0>`, `refs/<h1>`, `-d`) under every ancestor path of the parent's lineage; CAS `RolledBack`. Homes never reused. |
| no `'T'`, a `topo-` checkpoint and/or child paths | the above, plus maybe child manifests (initialized or not), maybe seed SSTs | **roll back with the takeover GC:** for each child path with a manifest, read its `external_dbs` and delete each `final_checkpoint_id` on its path (parent and inherited ancestors); delete `topo-<op_id>` (by name); delete the child paths' files and the lists; CAS `RolledBack`; serve. |
| `'T'` | children complete (C4: both initialized; fallback: both `'Y'`, since `'T'` follows them) | **roll forward:** publish (idempotent on `last_op`, CAS on the ETag), then Reclaimed. A fallback child lacking `'Y'` after `'T'` is impossible; the claimant asserts it and stops loudly if it ever sees it. |
| the topology at `T.version` naming `op_id` | published | Reclaimed (idempotent) |

**A slow, not dead, actor** against this table:
- **Before `'T'`:** its parent writer is fenced by the claimant's open. Its `'T'` write fails (an attempted
  `'T'`, below), its renewal fails, and its child writes (fallback) or clone calls (C4) touch paths the
  claimant deletes. Its own rollback, on a refusal before the attempt, deletes only the same objects. Its
  terminal CAS fails on `lease_epoch`/ETag.
- **An attempted `'T'` whose put or flush failed:** the actor cannot tell a `'T'` that landed (a put that
  landed before its flush failed, a flush whose response was lost, the close's own flush) from one that did
  not, so it treats the attempt as written. It refuses `Unfinished` with no resume and no rollback, its
  barrier drops unreleased (the parent closes, and the gate's final flag refuses every queued writer), and
  its intent stays live. The next opener of the parent decides by the table above: forward from `'T'`, back
  without it. Only a refusal before the attempt (the actor's ownership check) rolls back in process
  (DESIGN-DECISION-c3-incomplete-rig-2026-11-11, addendum 6).
- **After `'T'`:** both roll forward, the publish is idempotent, and a fenced child writer ends the actor
  (§4.3).

The live-actor DST pins the exact end state: v7 with both children serving, or the parent serving with no
child files, never a mix (finding 1).

### 6.5 Races

- **Split against split or merge:** one intent per cell.
- **Split against a move:** none during a live intent (§4.4). A move the rebalancer had already committed
  before the intent: the route refuses any override.
- **The topology changing under the operation:** a CAS on the ETag; only `next_home` bumps can interleave,
  handled by §6.2 step 4.2.

## 7. The sentinel helper, the delta, the lists

### 7.1 The helper (agreed with package 6)

The interface is as in rev 1 §7 (`SourceView`, `ChildPlan`, `plan`, `reconcile`), with two additions:
- `ChildPlan.gauges: BTreeMap<(Category, Option<ProjectId>), u64>`, for `'N'`;
- `reconcile(.., touched: &TouchedQueues, ..)`, which re-reads the touched state rows.

The per-tag rules come from S3a §2.8 (`keyspace::route` over `SENTINEL_TAGS`), with the enumeration test.
**`plan` enumerates the child's queues from the roster `'Q'` and the debt index `'D'`** (sentinel rows,
routed by trailing hash). No entity-range scan.

### 7.2 The delta, including `'N'` (finding 5)

- **The sentinel diff:** the snapshot's sentinel scan against the quiesced sentinel scan (local reads). Each
  changed key's tag rule is reapplied, and `'R'`/`'O'` are recomputed for every `(home, id)` it touches, from
  the in-range `'K'` entries (`decode_row_in_range`) plus the in-range footer entries of the `'I'` objects.
  Objects named by `'I'` rows the diff adds are read.
  - A `'K'` key the diff finds added (a seal) is copied to both plans; one it finds deleted (a background
    fold between the snapshot and the gate) is deleted from both. No per-entry work (K-SPLIT-DECISION §4).
- **The touched-queue mark:**
  - when the snapshot pass sets it, the committer starts recording the hash of every queue whose state row a
    group writes (send, claim, settle, lane expiry, trim inside the committer);
  - it stops at release;
  - bounded by the distinct queues in the window (a `HashSet<[u8; 16]>`; at most `max_queues_per_shard`
    entries, 16 B each);
  - it lives on the engine beside the barrier state, in `src/shard/transaction/barrier.rs`.
- **Under the barrier,** the delta re-reads exactly those queues' state rows (local point reads) and
  recomputes their `cat_bytes` contribution to each child's `'N'` (S3b's derivation from the state row's
  counts and `range_bytes`), per category and per project.
- **The checks of §6.2 step 2.2** then hold the children's `'N'` to the parent's quiesced `'N'`.
- **Red test:** a send and a claim between the snapshot and the barrier; exact `'N'` rows per child, and
  their sums.
- **Red test for `'K'`** (in the split commit, K-SPLIT-DECISION §5):
  - two streams, one per child half, sealed into one segment object (one `'K'` row), no fold. After the
    split, each child reads its stream back byte-exact (the end-to-end offset check). Each child's `'R'` for
    the parent's object is `live_chunks 1` with that chunk's bytes, and the two sum to the parent's row. Each
    child's own-home `'K'` rows have only in-range entries;
  - a second case runs the Preparing fold and asserts that the children's seeds hold zero parent-home `'K'`
    rows, and the parent's `'I'` rows in both.

### 7.3 Lists

As in rev 1 (S6 §2.3 format): lists under every ancestor home of whose objects a child holds any, PUT in
Preparing; supplementary `-d` lists in the pause only when the delta adds a holding.

## 8. The hold and the answers (E1, amended as Fable ratified)

| Request | Before the barrier | Pause, < 2 s | Past 2 s | After publish |
|---|---|---|---|---|
| appends, queue ops on the parent | served | held in the queue | `503 shard_splitting`, `Retry-After: 1` (queue drained; intake refused) | re-routed to the child's engine when this instance owns it (acked with its offset); else `409` + version → forward |
| reads, SSE, long poll on the parent | served | served (durable state) | served | `409` + version → forward; SSE resumes as on a move |
| any request on an instance that holds no engine of a source named by an unresolved operation (the fence) | — | `503 shard_splitting`, **reads included** | same | resolved |
| the parent with `'T'` before publish (opener) | — | `503 shard_splitting` | same | `409` + version |
| `request_purge` | gate shared | `Busy` | `Busy` | resolves to the child |

The 2 s number is provisional until Tigris measures the arm that ships (Q3's condition: 2× the measured p99
if it is above 1.5 s).

## 9. The pause and readiness

### 9.1 Measured on the fork (§1's recording store, 10 ms per request, first-generation parent)

| Step | Requests | Sequential round trips |
|---|---:|---:|
| `create_checkpoint(All)` | 5 | 6.5 |
| two projected clones in parallel (14 each; one source-manifest CAS retry) | 33 | 20.5 |
| opening one child (production builder, compactor on) | 35 | 24.3 |

### 9.2 The pause, by arm

| Step | C4 (target) | Fallback (maintenance open) |
|---|---:|---:|
| gate and barrier | 0 | 0 |
| `flush()`, then `checkpoint(All)` | ≈ 1 + 6.5 | ≈ 1 + 6.5 |
| delta and checks | 0 store (local) + 1 if a `-d` list | same |
| per child, in parallel: clone (+ seed SST PUTs) | ≈ 14 + 3 (retry) + 2 ≈ 19 | ≈ 17 |
| per child: maintenance open, seed, flush, close | — | ≈ 18-20 (24.3 measured less the compactor's ≈ 5) |
| `'T'` + flush | 1 | 1 |
| topology CAS | 1 | 1 |
| **Total** | **≈ 27-30 (0.7-1.2 s)** | **≈ 40-44 (1.0-1.8 s)** |

C3 saves about 3 more per clone. Deeper lineage adds about 3-4 per `external_dbs` entry per clone (bounded
by `LINEAGE_MAX_DEPTH`). The parent's own compactor keeps CASing its manifest during the pause, so the
clones' final-checkpoint CASes retry more often under load than in the test; the Tigris measurement counts
the retries.

### 9.3 Readiness (external review E; finding 11)

Measured from the barrier until **both children serve normally**: handles warmed, `open_index` done, the
first append and the first read on each child within pre-split p99. Reported per child:
- the pause;
- the serving open's wall time;
- **`|W|` and `open_index`'s wall time** (package 4 issues the member gets with bounded concurrency, 64,
  asked through the lead);
- the first-append and first-read latency.

With C4 there is no open in the pause. The kept child's serving open starts as soon as its clone is
initialized, overlapping the rest of the pause (N3), so the re-route of §6.2 step 4.4 finds it open or
nearly so. The moved child is opened by its owner on first request. **The re-routed appends' ack latency is
reported separately from the pause.**

## 10. Policy: reserve, then split

- The route admits only if resident shards after the split ≤ `SHARDS_PER_INSTANCE_MAX` and RSS headroom ≥
  2 × open transient + 2 × busy shard.
- **The reservation is held, not checked once** (finding 9): `reserved_shards: 2` in the actor's heartbeat
  from the intent's creation until it is terminal. `pick_move_target` and count balance honour it.
- With C4 and no open in the pause, the open transients occur after the publish, on whichever instance owns
  each child. The reservation stays, for the post-publish opens and the re-route.

### 10.1 The fleet count and the topology (FLEET-HYSTERESIS-REVIEW.md §8, Fable, 2026-11-09)

The lead carries these into SCALING-DESIGN §4.3's serialisation row and SCALING-DECISIONS.

1. **The topology waits for the fleet, never the reverse.** A split or a merge starts only when both hold:
   - `fleet/desired.json` is at least `SHARD_TOPOLOGY_COOLDOWN_SECS` (60 s) old, by its `computed_at_ms`;
   - no override anywhere is younger than `REBALANCE_MOVE_COOLDOWN_SECS`.
   The override rule already applied to merges; it now applies to a split's start too. The emergency
   split waits the same 60 s.
2. **No fleet scale-in while a topology intent is live.**
   - The fleet's publish decision (`fleet::hysteresis::Publisher::decide`, the queue session's work)
     refuses a shrink while the intent is non-terminal. It reads the intent once per tick, cached by
     ETag, as §4.4 specifies for the rebalancer.
   - Scale-out is never blocked by the topology.
   - The fleet-hysteresis package builds no hook. The first topology-intent writer, S4 or S6, whichever
     lands first, adds all of it: the refusal, as one more input to `Publisher::decide(plan, cur, now)`
     (`src/fleet/hysteresis.rs`), the intent being non-terminal; its test; and the intent read in
     `fleet::start`, with that function's exception-row re-pin.
3. **A split's perturbation is absorbed, not special-cased.** The pause, the `503 shard_splitting` hold
   and the children's first opens sit well inside the fleet's 30 s grow window. Condition, to measure in
   §9's Tigris leg: if the measured pause plus re-route exceeds 10 s, the topology version's publish
   time joins the fleet's settle anchor, and S4 tells the queue session.
4. **The clamp does the rest.** `need ≤ shards` stands; `split_demand` is now the sustained demand past
   the shards.
   **Amended (Fable, #261 ratification, EDGE-261-RATIFICATION.md §2):** `need ≤ shards + 1` while any
   member's split waits for an instance (never more than +1; the shrink carries it). The instance asked
   for is the home of the shard the split adds, so the clamp counts the post-split topology. At
   `FLEET_MAX` a split proceeds on the actor while `resident + 2 ≤ SHARDS_PER_INSTANCE_MAX`, and
   otherwise holds `topology_at_max`. Package 1 implements both (S5's policy).

**The fleet's timescales, for sizing S4's waits:**
- the count grows on 30 s of sustained load, at least 30 s after the last publication;
- it shrinks one instance per 300 s;
- slot-driven growth is blind for 60 s after any count change.

## 11. Operator routes (E3, ratified with Fable's field names and refusals)

- `GET /operator/topology.json` →
  `{version, shards: [{prefix, home, owner, born_version, state}], intent: null | {kind, op_id, sources, children, state, started_ms, lease_until_ms, outcome}}`.
  `state` is derived from §6.1's witnesses; a terminal intent shows its `outcome`. E3 supersedes Q8's exact
  text.
- `POST /operator/topology/split` `{"prefix"}`:
  - `202 {op_id, version}`;
  - `404 unknown_prefix`;
  - `409 topology_busy {reason: "intent" | "override"}`;
  - `409 instance_full`;
  - `422 not_splittable` (a 24-bit prefix, or a parent carrying `'T'`).
- `POST /operator/topology/merge` → `501 not_implemented` until S6.
- `src/http.rs` has zero headroom: the operator sub-router moves to `src/http/operator_routes.rs` first,
  importing `AppState` via `super`.

## 12. Formal

**TLA-045 `TopologyReconfiguration`, the split arm:**
- variables and actions as in rev 1 §12;
- **the lease does not fence in the model:** `LeaseLapse` and `Claim` stay enabled while `SlowActorStep`
  continues (finding 1);
- **the claim is an action of `Open`**, enabled only after `Open` bumped the writer epoch.

**Properties:**
- `AckedReadable`;
- `NoDualServe`;
- `RetiredNeverServes`;
- `ChildrenExact`;
- `ForwardAfterRetire`;
- `HomesUnique`;
- **`OperationCompletes`** (liveness under fairness, with `SlowActorStep` enabled after `Claim`).

**Negative controls**, each a patch naming the property it breaks:

| Control | Breaks |
|---|---|
| `nc-serve-source-under-intent` | `AckedReadable` |
| `nc-publish-before-retire` | `NoDualServe` |
| `nc-rollback-after-retire` | `AckedReadable` |
| `nc-checkpoint-before-barrier` | `AckedReadable`/`ChildrenExact` |
| `nc-reuse-home` | `HomesUnique`/`ChildrenExact` |
| `nc-decide-before-fence` | `AckedReadable` |
| **`nc-claim-without-fence`** (the claimant rolls back without opening the parent) | `ChildrenExact`/`ForwardAfterRetire` |
| **`nc-delete-successor-intent`** | `ForwardAfterRetire` |
| **`nc-actor-reopens-after-fence`** | `OperationCompletes` |
| **`nc-serve-under-T-before-publish`** | `NoDualServe`/`AckedReadable` |
| `nc-claim-fences-one-source` (package 6; the shared claim action, N sources) | `AckedReadable`/`RetiredNeverServes` |

**Kani** (`src/topology/proofs.rs`):
- `prefix_code_split`, with the `FF×16` disposition: in no leaf, sentinel-only key space;
- `refcount_split`, extended as `gauge_split_sums` for `'N'` per category, or folded into it.

The runtime sum checks are pinned by red tests, not by TLC.

**Re-records:**
- `formal_batch.py cost` decides at each commit;
- avoiding `src/shard/commit_op.rs` (the `SealCommit` route) keeps it out of the re-records;
- TLA-005, TLA-006 and TLA-011 at least, plus TLA-019 (§1 already re-recorded TLA-018/019 for P1's
  `read.rs`).

## 13. Tests

**Red tests, each failing first with its exact text in the commit:**
1. The fork test, depth 1 and 2 (§1), and, in the fork PR, C4's tests (§2.1).
2. Topology:
   - the CAS on the ETag;
   - a `next_home` bump keeps the version;
   - a publish conflict with `next_home` retries;
   - `last_op` makes a repeated publish succeed as done.
3. Revalidation:
   - 304 → no swap;
   - 200 → swap and a closed retired engine;
   - a received forward with a newer `v` revalidates before routing;
   - an unknown child answers `503` with no holdoff strike.
4. The intent:
   - Create when absent;
   - CAS over a terminal object;
   - `409` over a live one;
   - **no code path calls DELETE on it** (a store wrapper in the DSTs refuses DELETE of `topology/intent.json`
     and fails the test);
   - a stale renewal fails;
   - the monotone lapse (paused clock: the ETag unchanged across 11 s → lapsed; changed → not).
5. The forward: exactly two hops through a split; one `409` at equal versions; a client's own header is
   never forwarded.
6. The fence: the six rows of §6.3's table, exact tuples.
7. The helper:
   - exact rows per child for every tag;
   - Σ `'R'`/`'O'` per object;
   - **Σ `'N'` per category and per project after a send and a claim in the window**;
   - lists per ancestor.
8. `request_purge` → `Busy`, per caller.
9. The barrier:
   - both hold arms with exact tuples;
   - **the one-instance cell acks every held append after the CAS with its offset**;
   - the intake twin's `503` past the deadline.

**DSTs** (`topology_split.rs`, `topology_split_crash.rs`, ≤ 1,000 lines each):
- **The happy path, exact state:** halves at the same offsets; Σ `'R'`/`'N'`; **no entity row in either
  child's own SSTs, its own L0 holding exactly the seed's sentinel rows** (C4: exactly the seed SST ids)
  (finding 12); the parent refused by `'T'` with the version moved; the hop pinned.
- **The crash matrix:** a crash at each step boundary of §6.2; exact end states.
- **The live actor whose renewal fails once mid-ChildrenReady** (finding 1). The ring owner stays the actor:
  it re-claims through its engine and finishes (v7). In a variant, a move hands the parent to B, whose open
  fences the actor and rolls back. Exact end state: v7 with both children serving, or the parent serving with
  no child files and the intent `RolledBack`, never a mix.
- **The stale actor's late clone after the claimant's `RolledBack`** (N1). The actor passes its pre-check, the
  claimant fences, rolls back and CASes `RolledBack`, the actor's clone lands, and the actor dies before its
  after-check. Exact state:
  - `topology_never_leaf_paths` = 2;
  - after S6's never-leaf sweep (its DST; a hook in S4's until S6 lands), no checkpoint on the parent beyond
    its own and no file under either dead child path.
- **A renewal store error mid-ChildrenReady** (N2): one failed renewal PUT, retried, and the split completes
  (v7).
- **The stale actor's rollback after the successor's `'T'`** (finding 2): the end state is v7, the
  successor's intent terminal `Reclaimed`, and the stale actor's CAS failed.
- **Override lapse** (S6-REVIEW 1), now prevented by §4.4: the DST asserts the reservation (return-home
  skips the parent). With the reservation disabled by a test hook, it checks the before-`'T'` and
  after-`'T'` fence tuples of §6.3.
- **Stale owner;** **split against move** (refused, or prevented); **split under appends** (the
  ack-with-offset arm and the overrun tuple).
- **Lock order:** the purge's committer step in flight; the fold; the cleaner; consolidation; S3b's trim; a
  stalled writer past the deadline → `RolledBack {reason: "gate_deadline"}`.
- **Fallback arm:** no compaction CAS on either child path before the topology CAS.
- **Queue-heavy split** with S3b; `topology_lifecycle`/`topology_scaling` on the versioned topology; the
  route DST with exact bodies.

**Measurements** (Tigris):
- the pause's round trips and wall time, p50/p99 over 20 splits, for each arm that exists;
- readiness per child, with `|W|` and `open_index`;
- append p99 through a split;
- the 1 → 2 → 4 ladder on one instance, then across instances.

## 14. Ratchets, ledgers, edge records

**Files:**
- new: `src/topology/{mod,model,snapshot,intent,gate,sentinel,split,fence,routes,proofs}.rs` and
  `src/shard/transaction/barrier.rs`, each under 1,000 lines with functions under 100;
- `src/http/operator_routes.rs` (the extraction).

**Shared files:**
- `src/shard.rs`: zero net lines (`commit_and_park`, not `&mut rx`: §14.1);
- `src/registry.rs`: funded by moving `Topology`;
- `src/http.rs`: funded by the extraction.

**Exceptions:**
- no `CommitOp` growth;
- the `SealCommit` enum's and dispatch sites' contracts measured first; only a biting one goes to Fable, with
  the `after` values;
- `load_or_init_topology`'s scopes shrink.

**Ledgers:**
- `owners.json`: the renewal task's spawn, the `#[path]` modules, Kani covers;
- mutation owners `topology_*`, measured with `--measure-sizes`;
- the mt audit (prefix/home maps: infrastructure, with the allow comment);
- the test inventory.

**RULES (2026-11-04):** the local mutation leg for the selected owners runs before every push.

**Edge records** (numbers assigned by the lead, 2026-11-08: **E1 = #249, E2 = #250, E3 = #251**; any other number is asked for first), each "awaiting ratification" with WIRE-MATRIX lines:
- **E1:** the hold, `503 shard_splitting`, the fence row with reads, `'T'` with a live lease, the local
  re-route, the provisional 2 s;
- **E2:** the header, the hop, receiver-side revalidation, the retryable unknown-prefix `503`;
- **E3:** the routes, field names and refusals, superseding Q8's text.

### 14.1 Implementation departures (accepted by the lead under fast mode, 2026-11-09)

1. **No `&mut rx` into the group.** Measured first (§5.2): it grows `committer_loop`'s
   `let_underscore_must_use` and `excessive_nesting` contracts (syntax_facts 92 → 93). The committer loop
   calls `commit_and_park(ops)` where it called `commit_group(ops)`. Past the hold the park sets the gate's
   `splitting` flag and returns, and the committer answers every group it gathers `Splitting` (raw
   `503 shard_splitting`, `Retry-After: 1`) instead of draining the queue itself. `try_command` is untouched:
   intake still enqueues and is answered at once. (S4 7a/7b, 6ecb419a8, f9fc78186.)
2. **The re-route through the handler's own re-resolve.** On `Retired` the committer sends each append to
   the child engine the split's `Reroute` names for its route; every other operation, and an append whose
   child this instance does not own, is answered `Splitting`, and the append path re-resolves its route
   under the installed topology: the non-owner answer is the existing `409 not_ring_owner` with
   `Streams-Replay-To` (and the version, E2). The committer knows child engines, never owners. (7c,
   abf3361a2.)
3. **Test-built until the split.** The barrier's sender (`ShardEngine::barrier`), `Barrier`'s `resume` and
   `retire`, the gate's setters and the touched-queue mark's start and take build for tests until step 11
   calls them; production runs `commit_and_park`'s checks (one atomic load per group) and `commit_group`.
4. **The parent closes after the `Retired` release** (§6.2 step 3.2, corrected above).
5. **The fence holds inside the open** (§6.3; S4 step 9). The open gate (`OpenGate::get_or_open`,
   `ShardDirectory::resolve`) sits under frozen exception contracts, so the fence wraps the opener
   (`topology::fence::fenced`, in `ShardDirectory::new`): a held open re-reads the fence every second,
   collection off, and publishes its engine only once it may serve; past 60 s it fails. A request on a held
   shard, reads included, meets the gate's `503 shard_opening`, `Retry-After: 3`, after `open_wait` (10 s),
   not E1's immediate `503 shard_splitting`, `Retry-After: 1`. The claim and the lapsed rows join in step 11.

## 15. Decisions: taken, and the few still asked

**Taken by Fable (applied):**
- O1 not approved, so the `SealCommit` route;
- one intent per cell;
- `-<home>` paths (`~` until 2026-11-10);
- no fold in the pause;
- `create_checkpoint(All)`, now after a WAL flush, which §1 found necessary;
- the runtime sum checks extended to `'N'`;
- constants provisional, with the skew and the monotone lapse;
- the pause target is C4's number, and the hold stays at 2 s until measured;
- E1, E2 and E3 as ratified.

**For the lead to route (no owner decision needed):**
1. **Package 4 (S3a):**
   - refuse a stream or queue hash of `FF×16` at creation (finding 14);
   - `open_index`'s member gets with bounded concurrency (64) (finding 11).
2. **Package 6 (S6):**
   - the intent is never deleted; the terminal CAS replaces S6 §7.2's deletion and the per-prefix intents;
   - the claim happens only in the open path, after the fence (finding 1 applies to the merge's two sources
     alike);
   - `topology_retired_homes_pending_gc` is S6's first drain (finding 15);
   - with C4, the merge's union clone can take its rebuilt sentinel as a seed too (`with_seed` works for
     `cloned_from_union`), which removes M's open from the merge's pause.
   - **agreed with package 6** (S6 rev 5): the shared claim path opens every source the intent names before
     deciding (§4.3); the merge drops its fold too, so `'K'` is unioned.
   - **The never-leaf path contract** (N1; S6's sweeper, S4's counter; born homes moved from `~` to `-` on 2026-11-10, edaca93eb, because SlateDB's clone re-encodes a percent-encoded source path):
     - A home `h` is **never-leaf** when all three hold: `h < topology.next_home`; `h ∉ topology.homes` (never
       published); `h` is not among a live intent's `children`.
     - Its paths are `shards/<prefix>-<h>`, found by one delimiter LIST of `shards/` (names end `-<decimal
       h>`), plus `refs/<h>` and `refs/<h>-d` under every ancestor path, which S6 already sweeps.
     - The sweep reads the child path's manifest `external_dbs`, deletes each `final_checkpoint_id` on its
       path (the parent and inherited ancestors), then the path's files.
     - S4 publishes `topology_never_leaf_paths` = `next_home − |homes| − |live intent children|`, computed
       from the home map, so the leak is visible before S6 lands.
3. **The fork:** C4 and C3 on prisma/slatedb `streams` (allowed by the owner, 2026-10-23), and the upstream
   issue for the buffered-WAL checkpoint (§1). The pin moves only after the fork rule's measurements.

**Still asked:** none.

## 16. Unverified

- **ASM note (N1):** the fork's clone CASes a final checkpoint onto the source's manifest through
  `ManifestStore`, outside any writer-epoch fence, by design. A stale clone is therefore possible and is
  handled by the `owned()` brackets and the never-leaf sweep. A fork-side refusal keyed on a lease epoch in
  the source checkpoint's name is not worth it now.

- The pause and readiness on Tigris, for both arms. §9 is fork request counts with injected latency, plus
  Fable's C4 count.
- C4's exact size, until the fork patch exists (§2.1's estimate: about 120 + 200 lines).
- The touched-queue set's size under a queue-heavy window (bounded by the queues per shard, unmeasured).
- `open_index`'s wall time at 4,096-32,768 dirty streams with bounded concurrency.
- The clones' final-checkpoint CAS retries under the parent's own compactor on Tigris.
- Whether the buffered-WAL checkpoint behaviour (§1) is intended upstream. The fork's refusal makes it fail
  closed either way.
