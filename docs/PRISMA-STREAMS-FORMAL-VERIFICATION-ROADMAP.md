# Prisma Streams: formal-verification roadmap

**Date:** 22 September 2026  
**Basis:** source inspection of `streams-slate (58).zip`  
**Archive SHA-256:** `f9acc8eb012ece0a2c6972c9ae9c8f4dfb0403af0b9f91d014939b6e294c7db5`  
**Status:** proposed work catalog and implementation policy; **not verification results**. No TLA+ model or Kani harness in this document has been implemented or run as part of preparing it. Priority labels are planning judgments, not findings of existing defects.  
**Implementation record:** §0 lists the items implemented since this catalog was written, their status, and the issues they found. Every other item remains planned.

Place this document at the repository root so the source references remain easy to resolve. Paths refer to the inspected snapshot; agents must resolve the current canonical owner before implementing an item. Do not infer the current Git commit from a historical review document in the archive.

## Contents

0. [Implementation record](#0-implementation-record)
1. [Purpose, value, and verification boundaries](#1-purpose-value-and-verification-boundaries)
2. [Mandatory behavior for coding agents](#2-mandatory-behavior-for-coding-agents)
3. [Portfolio structure and implementation order](#3-portfolio-structure-and-implementation-order)
4. [TLA+ model catalog](#4-tla-model-catalog)
5. [Kani proof catalog](#5-kani-proof-catalog)
6. [Cross-layer assurance cases](#6-cross-layer-assurance-cases)
7. [Repository layout, CI, evidence, and maintenance](#7-repository-layout-ci-evidence-and-maintenance)
8. [Acceptance criteria and initial work packets](#8-acceptance-criteria-and-initial-work-packets)
9. [Source and tool references](#9-source-and-tool-references)

## 0. Implementation record

This section records what has been implemented against the catalog, what it
found and what is still open. It describes the source at `d83af9a4`: the
closure up to `65771905`, merged with `slate` in `3a24eace` (§0.9), then
`6bc53fa3`, `c3d78cd6`, `1991dcb6` and `d83af9a4`. Statuses use the §2.8
vocabulary and nothing else. An item that is absent from §0.3 is **planned**.
Commits are cited by short hash and subject. Traces, bounds, mappings and
regressions are in `verification/` (see `verification/README.md`).

Findings are classified as in §2.7. This record also uses four labels, and
keeps them distinct:

- **Fix-introduced defect:** a production defect created by an earlier fix in
  this campaign. It is not counted as a pre-existing defect.
- **Specification defect:** the requirement text is wrong about the adopted
  design.
- **Documentation defect:** a comment or document is wrong about the code, and
  the code is right.
- **Open obligation:** the requirement is right and the code does less. Only
  the owner can close it, and never by rewording the requirement.

### 0.1 Campaign

| Phase | Commits | Scope |
|---|---|---|
| Packet A, tooling | first spike | Pinned tools (`quality-tools.toml` `[formal]`, checksum-verified installer), `verification/manifest.json`, the assumption ledger, receipts, and the driver `scripts/quality/formal.py`. Harnesses are `cfg(kani)`, declared in `build.rs`. Packet A is tooling, not an obligation, so it has no §2.8 status. Its exit condition, a real-tool self-test that rejects a wrong config, a timeout, a deadlock and a zero-discovery harness, was met. |
| First spike (23 September 2026) | up to `da4e056f` | KANI-001–003, 036–039, 042; TLA-001/002/003 (seal), TLA-005/006/011 (durability), TLA-016/018/019 (history). TLC 2.19 (tla2tools 1.7.4) on Java 17; Kani 0.68.0 with CBMC 6.11.0 on its own `nightly-2026-08-21`. Production gates keep Rust 1.98.1. |
| Closure, after an independent review | `ffa0c7f4` … `65771905` | The review's follow-ups: evidence integrity, the open findings, provider qualification, rollout rules, measured costs and the service backlog. No catalog item was added. |
| Merge with `slate` (25 September 2026) | `3a24eace` … `d83af9a4` | Overlapping fixes reconciled (§0.9); TLA-016 remodelled for slate's absorber and KANI-001–003 retargeted to slate's codec. No catalog item was added. |

### 0.2 Evidence gate

"Receipts are validated, bound to their inputs, and invalidated by assumption
and pin changes" (`ffa0c7f4`) reworked the gate after the review reproduced
these cases:

- Deleting the counterexample receipts went unreported.
- A receipt with an empty or fabricated check list and the old digest passed.
- `select` ignored `assumptions.md`, and gave `Cargo.lock` to Kani only.
- The digest was taken after execution.
- Concurrent TLC runs shared one `java.io.tmpdir`.

Each case now has a negative test in `scripts/quality/test_formal.py`.
"The driver's receipt tests hold whatever the obligations' statuses are"
(`32c0a365`) removed two tests' dependence on the live manifest.

| Level | Runs in | Command | Fails on | Reports only |
|---|---|---|---|---|
| Every commit | `scripts/quality.sh`, the `quality` CI job | `formal.py check` | Manifest, file, harness-discovery, config/property and assumption-ID errors. A `pass-with-recorded-scope` or `counterexample` obligation without a receipt. An invalid receipt: unsupported schema, wrong id, incomplete run, a check set that is not exactly the manifest's, a role or expected string that differs, a verdict the driver would not accept, a known defect on a passing obligation, or a counterexample receipt that reproduces none. | A stale receipt, one whose input digest differs from the current inputs. |
| Every change | the `formal` CI job, 6 shards | `self-test`, then `run --changed-from <base>` for the affected obligations (every obligation on the schedule) | Any verdict that differs from its expected verdict, and any input change during the run. | Nothing. The job runs without `--record`, so it writes no receipt, and it uploads no logs. |
| Every release | `scripts/release-gate.sh` (run by `scripts/rc-certify.sh`) | `formal.py check --fresh` | Everything `check` fails on, and any stale receipt. | — |

- **Input digest.** One sha256 per file:
  - the obligation's source owners, proofs or models, configurations and
    control patches, and the driver;
  - `Cargo.lock` and `Cargo.toml`, for every obligation;
  - `build.rs` and `rust-toolchain.toml`, for Kani.

  It also covers the text of each named assumption entry, the obligation's
  manifest entry, and the `[formal]` and `[slatedb]` pins. `select` applies
  the same rules.
- **Execution.** `run` snapshots the inputs before the first check and after
  each one. Any change fails the run and records nothing. Each TLC run gets
  its own `java.io.tmpdir`. A timeout, interrupt or SIGTERM kills the check's
  process group.
- **Upload hold.** Logs stay local under the raw-evidence upload hold. A
  schema 2 receipt carries each log's sha256.
- **Receipts.** All 17 receipts are being recorded at `d83af9a4`, after the
  merge, with `run --record` on clean trees. The pre-merge receipts
  (`3f386070`, `1d20f76f` and `cb4c6b47`, 317 checks) do not describe it:
  at `d83af9a4`, `formal.py check` fails KANI-001–003 and TLA-016, whose
  check sets changed, and reports the other 13 stale, because the merge
  changed source owners, assumption entries or manifest entries of each.
  On slate, KANI-001–003 and TLA-016 were re-recorded at `f10b997e`
  (`7d52d78d`) and the twelve receipts slate's later changes left stale at
  `7499ec19` (`bd9d9e7e`, run in a separate worktree six at a time, so a
  receipt's dirty flag there reflects only sibling receipts written by the
  same batch); `formal.py check --fresh` then reports none stale.

### 0.3 Obligation status

The status column is the manifest's claim, backed by the obligation's receipt
(§0.2). The receipt column gives the commit the receipt was recorded on and
its matched checks out of the obligation's total. The check counts are the
manifest's at `d83af9a4`. "Conditional on" names the
assumptions that are not established, which make the result a conditional
design check (§7.7).

| Obligation | Status | Checks (baseline / control / witness) | Scope | Receipt | Conditional on |
|---|---|---|---|---|---|
| KANI-001 | pass-with-recorded-scope | 1 / 1 / 0 | Offset round trip over every segment ordinal below 2^30 and every `u64` `next` (`src/offsets/proofs.rs`), retargeted to slate's codec at the merge; the full-width finding is open (§0.4). The `String` wrapper stays with the unit tests (ASM-OFFSET-DOMAIN). | to be recorded at `d83af9a4` | — |
| KANI-002 | pass-with-recorded-scope | 1 / 1 / 0 | `-1` and `next` 0 name start-of-stream; a token resumes at its `next`, `u64::MAX` included. | to be recorded at `d83af9a4` | — |
| KANI-003 | pass-with-recorded-scope | 1 / 1 / 0 | Token injectivity and order below 2^30. | to be recorded at `d83af9a4` | — |
| KANI-005 | pass-with-recorded-scope | 1 / 2 / 0 | `locate_in_spans` (`src/sse/source/proofs.rs`) over one to four spans laid out as `Lineage::build` lays them (first at 0, contiguous sealed caps, only the tail live) with full-width caps and every `u64` position: the answer's span starts at or before the position, its local offset is relative to that start, a sealed span below the tail holds only positions before its end, and no earlier sealed span holds it (so a one-past boundary belongs to the next span at local zero). | recorded with its commit | ASM-LINEAGE-CONTRACT |
| KANI-028 | pass-with-recorded-scope | 4 / 3 / 0 | `tiles_keyspace`, the coverage rule `SegmentMap::validate` applies to its terminal segments and `check_partition` to its live ones, with `SegmentDesc::contains` and `SegmentMap::route` (`src/segmap/proofs.rs`), one harness per count of one to four nonempty ranges with full-width bounds, and every `u64` routing point: exactly one range holds each point (`KEYSPACE_END` in the range ending there), and a map of terminal segments over them, each live or a sealed leaf, routes the point to that segment. `validate` itself (duplicate identities, seal metadata), lineage and pending transitions are outside it. | recorded with its commit | — |
| KANI-036 | pass-with-recorded-scope | 1 / 2 / 0 | `decide_producer`: stale and new epoch admission, over full-width `u64` values and symbolic hashes (`src/shard/commit_plan/proofs.rs`). | to be recorded at `d83af9a4` | — |
| KANI-037 | pass-with-recorded-scope | 1 / 3 / 0 | Duplicates, conflicts and replay results. | to be recorded at `d83af9a4` | — |
| KANI-038 | pass-with-recorded-scope | 2 / 2 / 0 | Sequence gaps at the numeric boundary; a lane at `u64::MAX` only replays. | to be recorded at `d83af9a4` | — |
| KANI-039 | pass-with-recorded-scope | 1 / 2 / 0 | `seal_authorized` over every generation, fence and closing flag. | to be recorded at `d83af9a4` | — |
| KANI-042 | pass-with-recorded-scope | 1 / 2 / 0 | Every `AppendErr` variant against the debt-retention table, on the raw and product surfaces (`src/application/lifecycle/claims/proofs.rs`). | to be recorded at `d83af9a4` | — |
| TLA-001 | pass-with-recorded-scope | 2 / 5 / 9 | `RegistryCas.tla`: the `mutate_incarnation` and `recreate` retry loops against a single-request conditional PUT, at 2 attempts and at the production bound of 5. | to be recorded at `d83af9a4` | ASM-OBJSTORE-CAS (unestablished) |
| TLA-002 | pass-with-recorded-scope | 12 / 11 / 16 | `SealProtocol.tla` (`MC_SealTakeover`): claims, renewal, takeover, the durable fence row, engine replacement, crash failover, and a fence group lost or rejected before it is durable. | to be recorded at `d83af9a4` | ASM-OBJSTORE-CAS (unestablished); ASM-SEAL-OPID (unestablished until KANI-043) |
| TLA-003 | pass-with-recorded-scope | 14 / 16 / 17 | `SealProtocol.tla` (`MC_FinalSeal`): final-record sealing on both surfaces, with validation skew between instances (V4, V5, V5A) and the same-id plain append (OP). No known defect remains. | to be recorded at `d83af9a4` | ASM-OBJSTORE-CAS (unestablished); ASM-SEAL-OPID (unestablished until KANI-043) |
| TLA-005 | pass-with-recorded-scope | 6 / 8 / 15 | `CommitGroups.tla`: commit groups, barriers and replies, with the pinned post-apply read window. | to be recorded at `d83af9a4` | ASM-OBJSTORE-CAS (unestablished) |
| TLA-006 | pass-with-recorded-scope | 3 / 6 / 9 | `HandoffRetirement.tla`: one terminal owner per batch, and late success only for work decided live. | to be recorded at `d83af9a4` | ASM-DURABILITY-10, for liveness only |
| TLA-011 | pass-with-recorded-scope | 3 / 4 / 11 | `ServingOwnership.tla`: two nodes, stale views, an override move, crashes and ambiguous WAL PUTs, in fleet and single-node modes. | to be recorded at `d83af9a4` | ASM-OBJSTORE-CAS (unestablished), through ASM-SLATEDB-FENCE |
| TLA-016 | pass-with-recorded-scope | 10 / 15 / 18 | `HistoryAbsorb.tla`: absorption, trims, the byte ledger and postings pages with overlap admission. Since the merge (`c3d78cd6`) it maps slate's absorber: retirement only from the stream's own boundary, bucketed settlement receipts and the settled plan-time rollback. | to be recorded at `d83af9a4` | — |
| TLA-018 | pass-with-recorded-scope | 8 / 8 / 13 | `ReadCompose.tla`: durable and applied, keyed and unfiltered reads, with the provisional continuation and explicit resync. H11 is claimed only in the scope of TLA-018-F2. | to be recorded at `d83af9a4` | — |
| TLA-019 | pass-with-recorded-scope | 12 / 17 / 21 | `ReachGC.tla` (GC, with the compaction checkpoint) and `ForkPin.tla` (fork references, the debt marker, the reconciler, the backfill and recreation). The catalog's "eventually reclaimable" clause is not met on a partition with no further writes (TLA-019-F2), and it is unchecked for deleted incarnations, because no reclamation policy exists (TLA-019-F3). | to be recorded at `d83af9a4` | ASM-OBJSTORE-CAS (unestablished); ASM-HISTORY-GC-CLOCK (unestablished for multi-host operation) |

The manifest holds 312 checks at `d83af9a4`: 79 baselines, 104 negative
controls, 129 witnesses and no known defect.

### 0.4 Findings from the models and proofs

| Finding | Classification | Disposition | Commit |
|---|---|---|---|
| KANI-001/003: a segment ordinal at or above 2^30 aliased the ordinal mod 2^30 | production defect (the §5 seed, confirmed on real code) | **open**: the merge kept slate's codec, which documents only ordinals below 2^30 as round-tripping; the fix waits on slate's review item 88 wire decision. The proofs cover the domain below 2^30. | `fd8a5fca` (its codec was not kept by the merge) |
| KANI-002: the successor of `Offset(Some(u64::MAX))` overflowed (a panic in debug, a wrap to START in release) | production defect | fixed; slate's codec carries `next` itself, with no successor arithmetic | `fd8a5fca`; slate |
| KANI-002: a 26-byte token with a multi-byte character parsed as START | production defect | **open**: slate pins it, a high leading digit and nonzero pad and `in_block` bits as lax readings (`non_canonical_tokens_keep_their_lax_reading`) until review item 88 decides; KANI-004 | `fd8a5fca` (its codec was not kept by the merge) |
| KANI-002: scan index `u64::MAX` was also the planner's "now" (ASM-READ-NOW-SENTINEL) | domain question (§2.7) | resolved; see §0.6 | `ec55262f` |
| KANI-042: the wildcard arm gave every unlisted refusal the retaining verdict | design hazard | fixed; no existing verdict changed | `241eb282` "The final-append disposition names every refusal instead of defaulting" |
| TLA-002-F1: the seal fence lived only in the engine | production defect, reproduced | fixed | `234f69ab` "A seal takeover's fence outlives the engine that recorded it" |
| TLA-002-F2: a `SealSuperseded` refusal was answered before its fence row was durable | **fix-introduced defect**: a gap in the TLA-002-F1 fix, reproduced end to end | fixed | `e953ef28` "A SealSuperseded refusal waits until the fence behind it is durable" |
| TLA-003-F2: a raw close that took over a claim refused its own final | production defect, reproduced | fixed; its renewal forms have no real-code test | `8e52a8fc` "A raw close that takes over an abandoned final claim writes its own record" |
| TLA-003-F3: a product seal published its intent before the record-ceiling check | production defect | fixed | `ebf4ed44` "A product seal refuses an over-ceiling final before it publishes its intent" |
| TLA-003-F4: a raw exact retry renewed the claim before its ingest-capacity check | model counterexample, **reproduced on real code with two instances**; production defect | fixed; the model control is the pre-fix renewal | `56ffc65f` "A seal retry refused by its own instance's limits neither renews nor releases the claim"; model `3917b397` |
| TLA-003-F5: under record-ceiling skew, a renewed retry released the claim while the original could still commit | model counterexample, **reproduced on real code with two instances**; production defect | fixed. Only the installing attempt releases, and duplicate replay is kept (guard test). | `56ffc65f`; model `3917b397` |
| TLA-005-F5: after a post-apply write error, the next group staged from the failed batch | production defect, reproduced | fixed | `f574d733` "A failed commit write retires its engine before any later group can stage from it" |
| TLA-006-F3: the first `LateSuccessWasClaimedLive` was vacuous | model defect | fixed in the model | spike |
| TLA-011-F3: T11 ("exactly one owner epoch may acknowledge") forbade the §1.5 late durable response | specification defect | reconciled wording in `docs/dst/DST-EXPANSION-SPEC.md` §9.12.1; awaits the spec owner's confirmation | `63202168` "The invariant docs state what the models showed and what stays an owner decision" |
| TLA-016-F1: an advance retired its chunk's bytes, not the range it moved over | production defect | fixed by slate's `9c6675d7` and `b5751e75`, adopted by the merge `3a24eace`; our `6371da0a` (the stored-row recount) is superseded and was dropped | `9c6675d7` "The committer retires an absorbed advance only from its own boundary, so no regather can retire a byte twice"; `b5751e75` "A lane mark is rolled back only when no submitted advance of its stream can still land, so a regather never starts under an advance in flight" |
| TLA-016-F3: a stale re-gather's warm install claimed coverage over a trimmed head | production defect (latent) | fixed; slate's branch still had it, and the merge re-applied the fix | `b47c2d2a` "A re-gather warms the postings cache only over the rows it staged" |
| Cache bridge over a chunk whose runs no slice recorded (found during the F3 fix) | production defect; outside what the model can express | fixed; covered by real-code regressions | `0b37f3b9` "A postings-cache bridge never crosses a chunk whose runs no slice recorded" |
| TLA-016-F2: the `trim_safe_to` comments claimed protection for arbitrarily stale readers | documentation defect | fixed in comments | `63202168` |
| TLA-016-F4: the lane-mark rollback bounds a recount per refusal, not across consecutive late refusals; the comments, and the LAG report's `Cap × MaxChan` bound, claimed more | documentation defect, found while modelling `d9aeaeff` | moot: the merge `3a24eace` dropped the recount, and its witness went with it | `f1de3dcb` "The history model checks refusal receipts and admitted overlapping pages" |
| TLA-018-F1: an applied keyed read skipped a durable record trimmed by a non-durable advance | production defect | fixed | `9cea1b69` "An applied read revalidates its tail scan at the level it scanned, so it never skips a durable record" |
| TLA-018-F3: a stale applied cursor was accepted once the new owner's tail passed it | production defect, reproduced by holding the shard DB's WAL PUT | fixed. A provisional `KIND_KEY_V3` cursor is bound to the history that served it (the writer epoch and a digest), and an incompatible continuation gets 409 `cursor_beyond_tail` with a durable cursor. The regressions are checked in (`dst::dst_tests::reads_applied_history`). Known limits: a V2 session cursor minted before the fix over a lost suffix stays undetectable, and an object-store restore to an older snapshot can repeat a writer epoch. | `55881d7a` "A provisional read cursor proves the history it continues, or answers an explicit resync"; model `d93490f5` |
| TLA-018-F2: the reader does not detect a postings page or canonical row lost after durability | **open obligation** (H11) | owner decision, §0.7 | — |
| TLA-019-F1: GC could delete SSTs that a stale writer view names | abstraction mismatch; not reproduced | withdrawn, **conditionally**. It holds only while ASM-SLATEDB-COMPACTION-CHECKPOINT holds: an upstream 900 s checkpoint the code calls interim, a view refreshed within 300 s, and a read that ends within the remaining 600 s. Beyond that, `baseline-timing-lapse` shows a read fails rather than completing short. Two real-code tests pin the checkpoint; no real-code test covers the timing lapse. | `d377a8e7` "A test pins the checkpoint that keeps a stale history view readable" |
| TLA-019-F4: releasing a fork reference after a raced or interrupted `DELETE` needed the client to repeat a `DELETE` that had succeeded | unjustified assumption (a client retry), then service work | fixed. A write-ahead debt marker, a supervised reconciler, a one-time backfill with a `fork_debt_stale` alert, and indexing of the debt a recreated name overwrites. Excluded by name: debt that the old binary overwrote before the rollout (`LostByOldBinary`), which is unrecoverable. Known limits: a marker whose creator died before installing stays until an operator confirms it inert, and the alert is evaluated in the telemetry cadence. | `0d40dc2a` "A background reconciler releases fork references that deleted children still owe"; `8a03e0d5` "Fork-reference debt from before the index is backfilled, and stale debt raises an alert"; `3f386070` "A recreated name indexes the fork debt of the tombstone it overwrites"; models `d93490f5`, `3f386070` |
| TLA-019-F2: an unreferenced SST newer than a quiet partition's last compaction or newest L0 is never collected | **open obligation**, service (H14) | §0.7 | — |
| TLA-019-F3: no policy reclaims a hard-deleted incarnation's rows | **open obligation**, service | §0.7 | — |
| TLA-005-F3: an empty-entry producer Accept is treated as a no-write group | recorded observation (latent) | optional hardening, owner | — |

### 0.5 Defects found during the closure

These were found outside the models, while implementing the closure.

| Defect | Commit(s) | Evidence |
|---|---|---|
| **Op-id close gating** (TLA-003-F6). A raw append's operation id does not cover `Stream-Closed`, so a plain append with an owed final's bytes passed as its exact retry. It renewed the claim and landed twice, and the close answered 503 with the stream unsealed. | `a92aa9a1` "Only a close can resume an owed final"; model `3917b397` | `dst::dst_tests::seal_cancellation::a_plain_append_with_the_owed_finals_body_is_refused_during_sealing`; control `nc-plain-append-resumes-owed-final` |
| **Conditional-write client retry** (TLA-001-F1). object_store 0.14.1 re-sent a conditional PUT with its original precondition after a 5xx, 429, 408 (an update also after 409) or a transport error. A caller then saw `Precondition` or `AlreadyExists` for its own committed write: `mutate_incarnation` applied a non-idempotent change twice, `recreate` left a stream Initializing, and `create` answered 200 with the quota one low. Conditional requests now use a client with `max_retries: 0`. Cost: a transient 5xx on a conditional write reaches the client as a retryable error. | found by `e15ebef6` "A provider contract suite runs the conditional-write clauses through the production client"; fixed by `6e09a36c` "Registry conditional writes never mistake their own committed write for a refusal"; model `3917b397` | `http_cases.rs` on s3lite through the production client; controls `nc-client-retry*` |
| **JSON drift, DLQ and watch keys.** Every JSON ingest path re-encoded records with a parser that is not correctly rounded, so about 10% of `Math.random()`-style values were stored 1–2 ulp off, and repeated stores walked or oscillated. The ceiling and billing measured the re-encoded bytes (up to 4.6×). The seal id and the DLQ hash covered a re-serialisation. A DLQ copy committed just before a crash blocked its message forever (reproduced). Integral floats at or above 2^63 collapsed to `i64::MAX` in watch keys (reproduced). Contract A now stores the client's validated text, minified. | `1eaf5a5b` "A JSON collection stores each record as the client's validated text" | `dst::dst_tests::json_fidelity`; `consumer_dlq::a_committed_but_unsettled_dead_letter_copy_settles_on_the_next_pass`; corpus, proptest and golden tests; rollout in `SPEC.md` |
| **Seal refusal 500 → 409.** `AlreadySealed` and `OtherOperation` answered 500 `internal` with `retryable: true`, which no retry can satisfy. They are now 409 `sealed`, not retryable. | `fcbd8bd5` "A seal refused by another operation's terminal seal is a definitive 409" | `dst::dst_tests::seal_coordination::another_operation_on_a_sealed_collection_is_refused_definitively` |
| **Process-exit hang.** The intermittent livefeed hang was a test that never shut its rig down, plus a foyer-memory 0.22.3 self-deadlock: a runtime that drops a fetch it spawned under the in-flight mutex. The same deadlock could hold production process exit after a shutdown timeout, an early error or a panic. The runtime now stops with a 5 s bound. | `2c850d11` "The reopened-sealed-span livefeed test shuts its rig down, and every wait names its stall"; `af0e2f04` "The service runtime stops with a bound, so a stuck cache fetch cannot hold process exit" | 0 hangs in 2,500 solo release runs, as the commit reports; `bootstrap::service_runtime::a_scan_on_a_cache_miss_cannot_hold_process_exit`, with a canary for an upstream fix |
| **Scan-option regression and recount lag** (moot since the merge). `da4e056f` "The absorbed-byte recount reads with default scan options" removed the read-ahead because mutants survived. A cold recount then stalled the shard's commit path for 57.5 s (2 chunks) to 491 s (17 chunks). The "at most one gather chunk" comment was false: every consecutive refused group added a chunk. The closure restored the read-ahead and made refused groups roll their lane marks back and replay. The merge `3a24eace` removed the recount: slate's committer drops a mis-started advance instead of reading stored rows. | `21c5e618` "The absorbed-byte recount reads ahead again: the default scan stalls the shard for minutes"; `d9aeaeff` "A refused absorption group rolls its lane marks back, so a recount covers only chunks in flight"; both superseded by `3a24eace` | The regressions (`mis_started_recount_reads_ahead_and_an_aligned_advance_reads_nothing`, `refused_absorbed_groups_do_not_widen_the_next_recount`) were dropped with the recount. |
| **Overlapping postings pages.** Two gathers cut the same rows into different chunks. The cold index load then refused the overlapping pages as corrupt, and the key went to the envelope scan for good. After the merge they still arise, from a refused chain, the ungated sweep prune and a new owner. `keep_past` admits an overlap that lists exactly the offsets already admitted. | `d16559b3` "Overlapping postings pages from a re-gather admit when they agree", kept by the merge; models `f1de3dcb`, `c3d78cd6` | `history::bounded_discovery_tests::a_regather_across_a_detached_chunk_keeps_the_index_readable` and `…::a_new_owner_regather_across_inherited_chunks_keeps_the_index_readable`; `baseline-pages-regather`; `witness-OverlapAdmitted{RefusedChain,Prune,NewOwner}` |
| **DST timing** (reverted by the merge). After `d9aeaeff` a refused group replayed on the next absorber tick, so the atomic-retirement test's 20 ms tick let the retry land before its check. It failed 10 of 12 runs, and the tick became one second. The merge `3a24eace` restored 20 ms: with slate's absorber the retry comes from the rescan. | `29fcacf1` "The atomic-retirement DST test checks the refused group before its replay"; inventory hash `8e16bde3`; reverted in `3a24eace` | 12 of 12 runs at one second, as `29fcacf1` reports |
| **Mutation owners that reached no test.** An owner's filter is its whole test scope. `read_request`'s `application::read_request::` and `http_read`'s `http::read::` named modules that do not exist (the files compile as `application::read::request` and `http::read_adapter`), and the `ops` and `transaction_maintenance` filters left out the DST tests that assert their alerts and the recount's scan settings, so the closure's mutants in them survived. The filters now name the tests that reach the code, and new tests cover what those did not. `read_continuation` and `service_runtime` had no owner. The merge returned the `transaction_maintenance` filter to `shard::`, since the recount tests it was widened for are gone. One survivor is equivalent: `> 1` to `>= 1` in the raw read's segmented-offset rule, since the only one-segment map holds segment 0 and a segment-0 offset encodes identically either way. | `595fe266`, `0c5aef81`, `b054970b`, `15f700ad`, `1948942e`, `cb4c6b47` | every other changed owner's mutants caught or unviable in per-owner runs on the lane's arguments |

**Measured costs, not yet accepted.** Workload acceptance is separate from
correctness (`docs/OPS-RELEASE.md` §6).

- **Recount.** Moot since the merge removed the recount. The pre-merge
  measurements (read-ahead stall 0.13–0.55 s at a 9–74 MiB transient heap)
  describe code that no longer exists.
- **Postings cache** (the bridge fix, kept by the merge). `0b37f3b9`,
  measured with a harness kept outside the tree, at 20 ms per request:
  - below the admission line it costs nothing;
  - above it, in the measured workload, index loads rose from 0.5% to 37% of
    reads, and mean read latency rose by about 65 ms with a warm block cache
    and about 245 ms with a cold one.

  Recording the dropped keys would recover the bridging. That is not
  implemented.

### 0.6 The `u64::MAX` read position

Every `u64` is a valid scan index (since `fd8a5fca`, and in slate's codec,
which the merge kept). `u64::MAX` was still the read planner's in-band "now",
so a crafted token with rawSeq 2^64−1 read the live tail. "A read position of
u64::MAX is a position, and now has its own representation" (`ec55262f`)
carries "now" as `ScanStart::Now` through the planner and the peer relay. A
numeric index follows the ordinary past-the-tail rule: an applied read gets
`CursorBeyondTail`, and a durable replay gets an empty page. The wire is
unchanged, because the relay always sent `now` literally.
ASM-READ-NOW-SENTINEL is retired. During a mixed-version rollout, an owner
that is not yet upgraded still treats a forwarded 2^64−1 as its tail
(`docs/OPS-RELEASE.md` §6). The regressions are in
`dst::dst_tests::read_application`.

### 0.7 Open items

| Item | Kind | Next step |
|---|---|---|
| **Service obligations**: the `docs/READINESS.md` section "Service obligations from formal verification (open)" holds their acceptance criteria. | open obligations | **Physical reclamation (TLA-019-F3):** a written policy (rows, delay, prerequisites); every row of a hard-deleted incarnation deleted within the stated delay; a recreate, a live fork child or an unsettled billing close blocks it, with a DST scenario for each; deletes under the trim or GC budget; a gauge and an alert. **GC without further writes (H14, TLA-019-F2):** an unreferenced SST on a quiet partition is deleted within an owner-stated bound, with no new periodic LIST, checked by a DST scenario and a `ReachGC` baseline (`docs/dst/DST-EXPANSION-SPEC.md` §9.12.3). **H11 (TLA-018-F2):** not met until the owner decides (below). The campaign does not replace the other readiness work: independent restore, real Compute failover, authorization integration, external incident visibility and workload acceptance. |
| **Owner decisions** | decisions | **T11:** confirm the reconciled wording (DST-EXPANSION-SPEC §9.12.1). **H11:** option A keeps H11 and adds a per-chunk coverage record; option B deliberately revises the contract under §2.10 (§9.12.2). This is not a weakening by default. **H14 bound:** state the convergence bound (candidate `min_age + 2 × gc_interval`, §9.12.3). **Reclamation policy:** adopt one (READINESS criterion 1). Smaller, already recorded: D5 met only error-for-error (TLA-005-F4); a readiness gate on the first ring view (TLA-011-F2); the three limit-reduction obligations in `docs/seal-transitions.md` "Limit reductions and accepted finals". |
| Real-provider qualification | qualification, not run | The provider contract suite (`docs/PROVIDER-CONTRACT.md`) has not been run against the production provider. That needs owner credentials: `STREAMS_PROVIDER_CONTRACT=1` with an endpoint, bucket and prefix. Until a logged run, ASM-OBJSTORE-CAS is unestablished, and TLA-001/002/003/005/011/019 are conditional on it. |
| Mixed-version rollout | operational | No rule in `docs/OPS-RELEASE.md` §6 has been qualified with an actual old/new binary pair. Use each rule's coordinated form. |
| SDK rewind on 400 `invalid_cursor` | client follow-up | An older server answers a V3 cursor with 400 `invalid_cursor`, and today's SDK throws. The SDK should rewind to its durable cursor in applied mode. |
| `SealError` variants still mapped to 500 | follow-up | `Missing`, `ChangedIncarnation`, `OwedFinal` and `InvalidClaim` still answer 500 `internal`, retryable (`seal_error_response`, `src/product.rs`). |
| `src/bin/verify.rs` | follow-up | This diagnostic still builds its own retrying S3 client for conditional writes, and it still drops its runtime unbounded. |
| foyer-memory 0.22.3 deadlock | dependency exposure | Process exit is contained by `af0e2f04`. About 324 multi-thread `#[tokio::test]`s that end with live engines, and `verify`, remain exposed. The upstream issue is not yet filed. The exit condition is in `docs/OPS-RELEASE.md` §1. |
| Expired, never-deleted fork child | open obligation (owner decision) | Expiry releases nothing: `DELETE` of an expired fork answers gone and there is no expiry sweep, so a source soft-deleted while an expired fork holds its reference stays retained. A recreation of the fork's name now indexes the reference it overwrites ("A recreation over an expired fork child indexes the reference it held"); whether expiry itself should release fork references needs the owner's decision. Real-code regression only; expiry is not modelled. |
| TLA-019-F1 timing lapse | evidence gap | The model covers the lapse; no real-code test does. |
| Heal latency after a refused or dropped advance | follow-up (merge) | A quiet stream is re-pended only by the dirty-index rescan, up to about 10 minutes at the default tick, instead of replaying at once. Nothing is lost: its records stay in the shard log. `liveness-refusal` proves the heal, not its latency. |
| Settlement-bucket sharing | follow-up (merge) | 1,024 buckets serve all of an absorber's streams, so another stream's in-flight advance holds a refused stream's rollback back (`witness-MateDelaysRollback`). Under a lagging committer the delay can repeat; it is safe with `keep_past`. |
| `keep_past` in `check_postings` | follow-up (merge) | Slate's gather self-check reaches `keep_past` through `append_page_runs`, so it also admits an agreeing duplicate page inside one chunk. Consider a strict variant for the self-check. |
| Code-line comments in the `.tla` files | documentation follow-up | They predate the merge and are stale. The READMEs' mapping tables cite the merged code (`6bc53fa3`, `c3d78cd6`). |

### 0.8 Questions for the owner (future work)

Each answer unblocks the §0.7 row named in brackets. Until then, the item
stays open and is not counted as met.

1. **T11.** Is the reconciled wording in DST-EXPANSION-SPEC §9.12.1 the
   intended requirement? [Owner decisions]
2. **H11.** Keep H11 and add a per-chunk coverage record the reader checks
   (option A), or deliberately revise the contract under §2.10 with
   compensating checks (option B)? [Owner decisions; service obligations]
3. **H14.** What convergence bound should GC meet on a partition that stops
   receiving writes (candidate `min_age + 2 × gc_interval`), and by which
   mechanism? [Owner decisions; service obligations]
4. **Reclamation.** What policy deletes a hard-deleted incarnation's rows:
   which rows, after what delay, and behind which prerequisites?
   [Service obligations]
5. **Expired forks.** Should a fork's expiry release its reference on the
   source, for example through a TTL sweep or the reconciler, given that a
   TTL renewal can revive an expired incarnation? [Expired, never-deleted
   fork child]
6. **Real-provider qualification.** When can the provider contract suite run
   against the production provider with owner credentials, so that
   ASM-OBJSTORE-CAS can be established? [Real-provider qualification]
7. **Offset wire decision (review item 88 step 2).** Admit segment ordinals
   at or above 2^30 (the KANI-001 collision), and refuse multibyte,
   high-leading-digit and nonzero pad or `in_block` tokens? Until then
   KANI-001–003 prove only the domain below 2^30. [KANI-001 finding, §0.4]
8. **Follow-ups to schedule.** The SDK's rewind on 400 `invalid_cursor`, the
   remaining `SealError` variants that answer 500, `src/bin/verify.rs`'s
   retrying client, and filing the foyer-memory issue upstream. [their rows]

### 0.9 Merge with `slate` (25 September 2026)

Both branches fixed some of the same defects. The merge `3a24eace`
reconciled them by meaning, not by side; `6bc53fa3` moved the models' code
mappings to the merged code.

| Area | Resolution |
|---|---|
| Offsets | Slate's codec (`encode`, `parse`, `parse_scalar`) with its pinned lax readings, pending the review item 88 wire decision. Our full-width epoch and multibyte refusal (`fd8a5fca`) are not kept. KANI-001–003 prove slate's codec over epochs below 2^30 (`1991dcb6`); the full-width finding is open (§0.4). |
| Absorber | Slate's retirement only from the stream's own boundary (Exact, Detached or Diverged, `9c6675d7`) and its bucketed settlement receipts (`b5751e75`) replace our recount (`6371da0a`, `21c5e618`) and lane-mark replay (`d9aeaeff`). Kept from ours: `keep_past`, the TLA-016-F3 warm-install rule, the cache bridge and `write_failed` (TLA-005-F5). TLA-016 models the result (`c3d78cd6`). |
| Seal close | Ours (no renewal on a deferred refusal, release only by the installing attempt, resumption only by a close), plus slate's 503 `seal_incomplete` for a transient intent failure. |
| Recreate | Both debts: slate's closure debt (`record_replaced`), then our fork-debt marker, both before the descriptor write. |
| Capacity | The fresh-bucket 413 measures stored bytes on every surface, as the bucket charges. |
| Exception growth | The closure's fixes grew 55 contracts across 27 scopes; the owner approved recording them as rows in `docs/quality/exception-growth.json` (`d83af9a4`). |

## 1. Purpose, value, and verification boundaries

### 1.1 What we are trying to achieve

Prisma Streams coordinates independently durable state, asynchronous work, retries, ownership changes, and customer-visible responses. A correct happy path is not enough. The verification portfolio should make it harder to introduce four especially costly classes of failure: losing acknowledged data, acting under stale authority, losing recovery work, and returning success or completeness without the evidence that response requires.

The objective is **specific, reviewable assurance claims about important properties**, not a badge saying “the service is formally verified.” The work should also improve design: fewer representable invalid states, explicit durable boundaries, and smaller canonical decision functions. Do not introduce an abstraction merely because a verifier prefers it.

The existing repository already has a strong executable foundation. `AGENTS.md` and `docs/RUST-QUALITY.md` require canonical ownership, actual-production-path tests, property/fuzz coverage, instrumented Loom checks, compatible Miri runs, scoped mutation testing, and independent protocol and release gates. `docs/dst/DST-EXPANSION-SPEC.md` §9 contains the D/P/C/L/F/T/H/Q/W/S/R invariant families. This roadmap adds complementary checks rather than replacing that foundation.

### 1.2 What TLA+ contributes

TLA+ describes states and the actions that move between them. TLC explores the reachable behavior of a configured model and can check safety properties and temporal/liveness properties. A model can expose a flaw before the corresponding feature exists in code. It is especially useful here for multi-step protocols whose failures involve the order of reservation, durability, publication, cancellation, and recovery. [T1], [T2]

For example, a seal takeover is not one atomic operation: reserving a generation, making a physical fence durable, observing an old final append, and installing a replacement claim are separate steps. The model should explore their interleavings, not collapse them into a convenient transaction. `src/application/lifecycle.rs` and `docs/seal-transitions.md` identify this boundary.

**A successful TLC run checks the modeled behavior for the recorded configuration and assumptions.** It does not automatically prove the Rust implementation refines the model, or establish correctness for an arbitrary number of tenants, segments, messages, or failures. A finite state-space exploration can include arbitrarily long cycles within that finite model; it should not be confused with checking only a fixed number of execution steps. Bounds, state constraints, fairness, and any reductions must be reported. General proofs or stronger parameterized claims require additional work, not a renamed receipt. [T1], [T2], [T3]

### 1.3 What Kani contributes

Kani checks Rust code through proof harnesses. Symbolic inputs can cover all values of an admitted scalar type, while assertions specify the desired contract. Good initial targets are the actual offset codec, producer/fence decisions, interval calculations, and validated decoding boundaries. Semantic assertions matter: “does not panic” does not establish injective encoding, correct authorization, or conservation of a billing total. [K1]

Use full-width production integers wherever tractable. Bound collection lengths separately and state those bounds. A proof for up to four postings runs is not a proof for an arbitrary postings list. Keep unwinding assertions enabled: insufficient unwinding, unsupported operations, and solver/resource failures are not successful verification. [K2]

Kani is not the tool for proving the Tokio server's concurrent execution or object-store behavior. Its documented limitations include concurrency and panic-stack-unwinding support; I/O-heavy and deep call graphs are poor initial targets. Some dependency-heavy targets below therefore require a compatibility spike or a small, behavior-preserving production extraction before a useful harness is possible. [K3], [K4]

### 1.4 How the tools relate

| Layer | Main question | Evidence to retain |
|---|---|---|
| TLA+/TLC | Is the abstract protocol safe, and does it converge under stated recovery/fairness assumptions? | Specification, configuration, assumption ledger, complete checker result, counterexamples and negative controls |
| Kani | Does this actual Rust decision or transformation satisfy its stated contract for the admitted inputs? | Harness, source mapping, input/loop bounds, dependency assumptions, assertion/coverage results |
| Loom | Does the instrumented small concurrent implementation survive the explored schedules? | Actual implementation mapping and exploration bounds, as required by existing policy |
| Deterministic simulation and fault tests | Does the real application recover correctly through executable failure scenarios? | Reproducible traces, semantic failpoints, external-observation assertions |
| Property tests, fuzzing, Miri, conformance, live tests | Do implementations, parsers, memory operations, dependencies, wire contracts, and deployments behave as required in their respective scopes? | Existing independently owned test and release evidence |

The bridge is: **requirement → model property → implementation transition/guard → proof harness → executable regression → CI receipt**. A connection in this chain is a reviewed mapping, not automatically a machine-checked refinement proof. A model is allowed to be simpler than the implementation only when its abstraction preserves the behavior relevant to its claim.

### 1.5 Important boundaries specific to this repository

**Durability and authority are separate.** An older engine may finish sending a response for work already durably claimed before retirement. Do not assert that every response after retirement is forbidden. Prohibit new unauthorized effects and success based on non-durable state instead. The relevant implementation is in `src/shard/commit_handoff.rs`, `src/shard/transaction/finalize.rs`, and `src/shard.rs`.

**Product ordering is per routing key.** The raw route is the default empty-key sequence. Do not invent global order between unrelated keys. See `docs/dst/DST-EXPANSION-SPEC.md` §8.3 and `docs/ROUTING-V3.md`.

**Idempotence has a precise retained-state contract.** `decide_producer` distinguishes the currently remembered sequence from older duplicate sequences and includes legacy sentinel handling. Do not claim every arbitrarily old retry returns its historical original offset unless production state and the product contract actually provide that information. Separate at-most-once persistence from exact replay-result retention.

**Large-record progress is intentional.** `PageBudget` allows the first record to exceed the requested page size within the permanent record limit. Postings/canonical-span planning also has progress rules. A blanket “returned bytes never exceed requested bytes” property would be wrong. Validate the hard ceiling and the documented first-record exception separately.

**Cryptography remains a dependency, not a solved subproblem.** Verify unambiguous preimages, domain/context binding, envelope lengths, rejection paths, and key separation at the call boundary. Do not assert that distinct inputs must produce distinct finite hashes/MACs, or report an ideal-authenticator model as a proof of cryptographic security. Keep the repository's independent cryptographic acceptance work intact.

**Security high-water memory is not durable anti-rollback storage.** `src/auth/publication.rs` explicitly describes its bounded process-local tables. A restart or eviction can forget entries; durable protection against a misbehaving publisher cannot be inferred from them. Model the Control Plane's feed contract separately.

**Volatile accounting is not durable accounting.** Distinguish read observations accumulated in memory from batches durably spooled and ledger entries durably acknowledged. Formalize the intended pre-spool loss policy rather than silently granting volatile observations crash persistence.

## 2. Mandatory behavior for coding agents

The rules in this section apply when implementing and maintaining this roadmap. They supplement `AGENTS.md` and the adopted `docs/RUST-QUALITY.md`; they do not waive existing source, compatibility, performance, cryptographic, deployment, or evidence-upload requirements.

### 2.1 Before editing

An agent **MUST** read the current repository instructions, the canonical owner's code, the relevant product/transition specification, and the existing invariant/scenario mapping. It must identify whether a statement is an adopted requirement, an observed implementation behavior, or a proposed design. Disagreements become explicit questions or tracked defects; they must not be resolved by silently changing the property to match the code.

For a change involving durable state, publication, authority, ownership, routing, deletion, retention, or a security boundary, the agent must state the affected invariant IDs and catalog entries before changing the implementation. For a codec/arithmetic change, it must state the valid input domain, malformed-input behavior, overflow/exhaustion policy, and compatibility obligations. A proof plan may be brief for a small change, but cannot be omitted merely because tests already pass.

### 2.2 Preserve one canonical implementation

Kani harnesses **MUST** call the production function or compile the unchanged production owner. Prefer colocated `#[cfg(kani)]` harnesses or a thin harness crate including actual modules, subject to the compiler/dependency compatibility check. `tools/quality-invariants/src/lib.rs` is an existing example of unchanged-module inclusion, not a guarantee that its entire dependency graph works with Kani.

Agents must not copy the algorithm into a “verification implementation,” replace real proof-bearing types with convenient stand-ins, widen private fields to let tests construct impossible states, or add runtime feature switches to make a proof easier. A small pure decision extraction is permitted when production uses it and the extraction improves canonical ownership. Both the extraction and its unchanged behavior need ordinary review and executable tests.

Test-only builders for **valid** private state are acceptable. Invalid persisted input must enter through the real decoding/validation boundary. A separate mathematical reference oracle is useful, but agreement with a copied implementation is not independent evidence.

### 2.3 Make assumptions explicit and discharge them

Every assumption must name its source, the boundary that enforces it, and what changes invalidate it. An external assumption must name the dependency and the integration/contract evidence supporting it. A bounded-input harness must distinguish a production restriction from a tractability restriction.

Agents **MUST NOT** assume the property being proved. Examples of prohibited shortcuts include assuming an acknowledgment is durable in a durability model, assuming offsets never reach the dangerous bits without a production bound, assuming all retries are exact, assuming authenticators always succeed, or filtering out the failing schedule.

After a counterexample, narrowing the input domain, changing fairness, weakening an invariant, increasing atomicity, removing an action, or adding a TLC state constraint requires an explicit semantic justification and review. “The checker now passes” is not that justification.

For abstract dependency stubs, document both the over-approximated outcomes and the excluded outcomes. A conditional proof using a storage or authenticator contract must be labeled conditional. Never replace the target guard with its desired result.

### 2.4 Model failure and atomicity honestly

TLA+ agents **MUST** separate independent durable steps and observable responses. Model response loss after commit, client cancellation without server cancellation, delayed old work, stale caches, retry duplication, crash/restart, CAS loss, and incarnation reuse when relevant. Preserve persisted debt across restart; erase genuinely volatile state.

Do not add “crash between everything” mechanically when an operation is actually atomic under an established dependency contract. Conversely, do not treat a multi-store operation as atomic because one Rust function implements it. Each model must have an atomicity/linearization table tied to real code or dependency contracts.

Safety checks must not depend on eventual recovery. Liveness checks must state exactly which faults cease and which enabled actions receive fair scheduling. Do not use fairness to force a desired outcome or assume that a client which may disappear always retries. Where progress requires a public retry rather than background reconciliation, state that limitation.

### 2.5 Keep Kani claims within its execution scope

Use the actual production numeric types and checked/wrapping/saturating operations. Do not replace floating-point quota arithmetic with exact real arithmetic and call it a code proof. Do not claim concurrent behavior from sequentially compiled atomics. Panic cleanup and dependency features outside the supported execution scope remain unverified, even when ordinary return/drop paths are checked. [K3]

Bounds must cover the complete declared input family, including internal loops. Keep unwinding and unsupported-feature failures visible. If a target needs a dependency spike, complete the spike before making its proof job a mandatory gate. Do not weaken the production compiler pin to suit a verifier.

### 2.6 Require falsification and reachability evidence

Each new model/proof family must include at least one relevant negative control, run from a passing unmutated baseline. The deliberately broken guard, ordering, or arithmetic must trigger the expected property failure. A compile error, unrelated panic, timeout, or incomplete search is not a successful negative control.

Harnesses must also demonstrate relevant branches are reachable under their assumptions. Kani's `cover` mechanism can check whether a condition is reachable; use it for meaningful boundary/acceptance/refusal witnesses where supported by the pinned version. TLC models should expose representative progress and failure-recovery witnesses or coverage checks rather than merely showing that `Init` is satisfiable. [K5]

Negative controls belong in isolated test/model variants or temporary patches, never in a runtime production switch. They must not accidentally run the unmodified code or remain enabled in release builds.

### 2.7 Handle failures productively

On a counterexample, an agent must preserve the raw evidence and minimize the case. Classify it as a production defect, specification defect, abstraction mismatch, unjustified assumption, dependency/tool limitation, or incomplete run. For a production defect, add an executable regression through the real code path before or with the fix. For a model defect, explain why the concrete implementation cannot produce the modeled behavior; do not merely delete the action.

Treat overflow at an apparently unreachable generation, offset, clock, or byte total as a domain-design question. Establish a checked refusal, an enforced domain, or a compatible representation change. Do not dismiss it based on expected operating volume alone.

### 2.8 Report exactly what ran

An agent must report the exact source revision/diff, toolchain and tool versions, selected harness/model IDs, commands, configuration hashes, completed verdicts, bounds, and unresolved exclusions. If a tool was not available or the job was not run, say so. Do not turn planned checks, successful compilation, test discovery, cached unrelated results, or zero matched harnesses into proof claims.

CI must reconcile expected discovery with actual execution. Relevant proof files, model configurations, dependency stubs, feature flags, tool pins, and verification-selection changes are verification-impacting changes even when production code is unchanged.

A result may be `planned`, `implemented-unchecked`, `pass-with-recorded-scope`, `counterexample`, `incomplete`, or `unsupported`. Keep these states separate. A permanent exception requires an owner, rationale, scope, expiry/revisit trigger, and compensating checks; it is not a pass.

### 2.9 Preserve the repository's existing quality bar

Use the exact root toolchain for ordinary code and gates. Pin any compatible verifier toolchain separately and record that it is a separate analysis configuration. Run `scripts/quality.sh`, the appropriate protocol/test gates, and `scripts/gate.sh` when the existing commit policy requires it. Use `scripts/quality/verification_plan.py` against the actual PR target merge base. Never regenerate or grow an adoption baseline during ordinary work.

Keep proof-bearing fields private, state machines explicit, task/effect ownership registered, and read optimizations on their single permanent production path. Do not add pass-through owners, swallow errors, silently recover poisoned authority state, or trade away clarity to satisfy a verifier. Structural review and independent performance/cryptographic/deployment evidence remain required.

### 2.10 Keep the catalog honest as the repository evolves

Agents must update source mappings and invalidate affected receipts whenever a guard, durable boundary, serialization format, dependency contract, or supported mode changes. Add new obligations for genuinely new guarantees; merge overlapping harnesses/models rather than proliferating redundant implementations. Retire obsolete entries with a reason and replacement mapping, not by deleting their history.

A coding agent must not approve its own change in assumptions as though that were independent review. Changes that weaken a customer-visible guarantee or a proof's trust boundary require an explicit owner decision.

## 3. Portfolio structure and implementation order

### 3.1 How to read the catalog

There are **44 TLA+ model work items and 96 Kani proof families**. An item is an assurance obligation, not necessarily one file or one harness. Related TLA+ items should share small reviewed modules and be composed where their interaction matters. Related Kani items may have several harnesses targeting one production owner. These counts are planning inventory, not a test-count target or coverage score.

| Priority | Meaning |
|---|---|
| **P0 — foundation** | First implementation wave: acknowledged-data safety, authority/lifecycle correctness, destructive maintenance, and small high-value decision/codec proofs |
| **P1 — expansion** | Follow after the tooling and first models are credible; prioritize further when the corresponding subsystem changes |
| **P2 — targeted** | Useful longer-term checks, often involving more difficult harness dependencies or less direct catastrophic-risk reduction |
| **D — design-triggered** | Specify before the proposed feature, compatibility transition, or external integration is adopted; not a claim that it is fully implemented now |

`P0` does not mean every listed obligation must be completed before any unrelated release. Establish the first mandatory gates incrementally, with explicit owner approval. No existing release hold is lifted by this document.

For Kani, **Direct** means the function is already a plausible bounded/pure target, not that it has been compiled successfully under Kani. **Extract** means a small canonical production decision boundary is needed. **Spike** means dependency, representation, floating-point, or ownership support must be established first. These classifications are estimates from source inspection.

For TLA+, each entry provides a starting finite instance, not a sufficient universal bound. Start with the smallest instance that can express the named failure, then run different shapes and larger configurations. For example, two takeover contenders plus an original claimant are necessary to expose competing reservations; one contender is not enough. Use reduced model domains to exercise exhaustion deliberately, and Kani to check the real full-width arithmetic.

### 3.2 Recommended sequence

Begin with a thin Kani compatibility/discovery path and **KANI-001, 002, 036–039, 042**, plus **TLA-002** with the minimal registry substrate from **TLA-001**. Then add the durable-response models **TLA-005/006**, final-seal debt **TLA-003**, and seal/topology composition **TLA-004**.

Next protect destructive maintenance and references: **TLA-016/018/019**, **TLA-013/014**, and postings/frame/descriptor proof families. Then expand to consumers, tenant/security state, resource ownership, and billing. Models tied to a subsystem should move earlier when that subsystem changes materially. Future-design items are feature prerequisites only after their proposed scope is adopted.

### 3.3 Shared modeling conventions

Use distinct identities for project, stream name, stream incarnation, segment ID, shard owner epoch, producer epoch/sequence, seal generation, consumer generation, lease generation, and operation ID. Do not substitute one for another merely because all are integers in the model.

Track `issued`, `locallyApplied`, `durable`, `responseObserved`, and `definitivelyRejected` separately where needed. A lost response changes client knowledge, not durable truth. Use operation bytes or symbolic content identities so the model can detect acknowledging the wrong record as well as the wrong offset.

For dependency contracts, model conditional writes, durable completion, failures, and ambiguous replies explicitly. Tie every assumption to the pinned SlateDB/object-store integration. Models of GC or owner fencing that use those contracts remain integration-protocol models, not a verification of the upstream storage engine.

## 4. TLA+ model catalog

Except the models [§0](#0-implementation-record) lists, all items below are **planned**. “Validate” states the intended obligation; it is not a claim that the current code has already satisfied it. Source paths identify the initial review owner. Existing invariant IDs identify requirements to reconcile and refine, not proof receipts.

| ID | Model | Priority |
|---|---|---|
| [TLA-001](#tla-001) | Registry CAS, attempt-local outcomes, and incarnation fencing | P0 |
| [TLA-002](#tla-002) | Seal claims, renewal, and competing takeover reservations | P0 |
| [TLA-003](#tla-003) | Final-record sealing, ambiguous append outcomes, and owed debt | P0 |
| [TLA-004](#tla-004) | Seal versus split/merge serialization | P0 |
| [TLA-005](#tla-005) | Commit groups, dependency barriers, and observable replies | P0 |
| [TLA-006](#tla-006) | Commit handoff, retirement, and already-durable completion | P0 |
| [TLA-007](#tla-007) | Producer idempotence, sequence advancement, and ambiguous retry | P0 |
| [TLA-008](#tla-008) | Create, initialization, initial contents, and delete races | P0 |
| [TLA-009](#tla-009) | Split intent, parent closure, and successor publication | P0 |
| [TLA-010](#tla-010) | Merge intent and preservation of both predecessor histories | P1 |
| [TLA-011](#tla-011) | Serving possession, owner fencing, and shard movement | P0 |
| [TLA-012](#tla-012) | Stale routers and per-key traversal through topology lineage | P1 |
| [TLA-013](#tla-013) | Fork initialization and durable source-reference installation | P0 |
| [TLA-014](#tla-014) | Fork deletion, reference release, and recursive cleanup debt | P0 |
| [TLA-015](#tla-015) | Fork read views, cut boundaries, and topology eligibility | P1 |
| [TLA-016](#tla-016) | History absorption, publication, and safe hot-data trimming | P0 |
| [TLA-017](#tla-017) | Dirty-work discovery, bounded deferral, and restart convergence | P1 |
| [TLA-018](#tla-018) | Exact composition of history, hot storage, and read visibility | P0 |
| [TLA-019](#tla-019) | Reachability-based garbage collection and reader/fork protection | P0 |
| [TLA-020](#tla-020) | Reader/postings caches: single-flight, cancellation, and identity | P1 |
| [TLA-021](#tla-021) | Snapshot pagination, cursor validity, and retention races | P1 |
| [TLA-022](#tla-022) | Consumer creation, deletion, and generation fencing | P0 |
| [TLA-023](#tla-023) | Consumer delivery leases, FIFO, acknowledgment, retry, and extension | P0 |
| [TLA-024](#tla-024) | Dead-letter transfer saga and destination incarnation | P0 |
| [TLA-025](#tla-025) | Consumer progress and settlement across split/merge lineage | P1 |
| [TLA-026](#tla-026) | Durable watch publication, journal gaps, and capability routing | P1 |
| [TLA-027](#tla-027) | SSE source switching, cutoffs, resynchronization, and cancellation | P1 |
| [TLA-028](#tla-028) | Runtime task ownership, single-flight opening, and shutdown | P1 |
| [TLA-029](#tla-029) | Reservation ownership and bounded resource admission | P1 |
| [TLA-030](#tla-030) | Maintenance pressure, wedged commits, and admission recovery | P1 |
| [TLA-031](#tla-031) | Authentication feed publication and retained high-water state | P1 |
| [TLA-032](#tla-032) | Request authorization, route capabilities, and tenant confinement | P1 |
| [TLA-033](#tla-033) | Revocation and freshness for long-lived subscriptions | P1 |
| [TLA-034](#tla-034) | Relational tenant noninterference for state and authorization | P2 |
| [TLA-035](#tla-035) | Durable usage outbox, ledger publication, and checkpoint acknowledgment | P1 |
| [TLA-036](#tla-036) | Read-usage accumulation, durable spooling, and drain cancellation | P1 |
| [TLA-037](#tla-037) | Usage rollup, replay idempotence, corrections, and month close | P1 |
| [TLA-038](#tla-038) | Workspace ownership changes and billing attribution boundaries | P1 |
| [TLA-039](#tla-039) | Fleet desired state, observations, and external-action outbox | P1 |
| [TLA-040](#tla-040) | Autoscaler decisions, unsplittable keys, and bounded controller work | P2 |
| [TLA-041](#tla-041) | Catalog pagination across invisible, vanished, and failed entries | P1 |
| [TLA-042](#tla-042) | Peer compatibility and preservation of read/append evidence | P2 |
| [TLA-043](#tla-043) | Control Plane project transfer and deletion sagas | D |
| [TLA-044](#tla-044) | Persisted-format and rolling-upgrade compatibility transitions | D |

<a id="tla-001"></a>
### TLA-001 — Registry CAS, attempt-local outcomes, and incarnation fencing

**Priority:** P0 · **Requirement anchors:** C4, L5, L14, T8  
**Source owners:** [`src/registry.rs`](src/registry.rs); [`src/application/lifecycle.rs`](src/application/lifecycle.rs); [`src/application/creation/claim.rs`](src/application/creation/claim.rs)

**Status:** pass-with-recorded-scope (first spike; see [§0](#0-implementation-record)).

**Validate.** A successful mutation changes only the descriptor version and incarnation it observed. CAS losers cannot leak an allocated generation, an “already completed” result, or a captured success flag into a later attempt. Missing conditional-write metadata must not become an unconditional overwrite. A stale operation on a deleted name cannot mutate a replacement with the same name and key.

**Why valuable.** This is the common authority boundary behind creation, sealing, topology, and deletion. One incorrect retry/outcome rule can invalidate several otherwise correct protocols.

**Starting model and boundary.** Two projects, one reused name, two incarnations, two concurrent mutations, and explicit read/CAS/reply steps. Include a successful write with a lost reply. Abstract object-store CAS atomically only under its documented contract; keep the surrounding retry loop non-atomic.

**Required negative control.** Allow an outcome captured by a losing CAS to survive into a retry, or drop the expected incarnation check; require a false-success or cross-incarnation counterexample.

**Related work:** TLA-002, 008, 009, 014; KANI-032, 040, 041.

<a id="tla-002"></a>
### TLA-002 — Seal claims, renewal, and competing takeover reservations

**Priority:** P0 · **Requirement anchors:** L5–L10  
**Source owners:** [`src/application/lifecycle.rs`](src/application/lifecycle.rs); [`src/application/lifecycle/claims.rs`](src/application/lifecycle/claims.rs); [`docs/seal-transitions.md`](docs/seal-transitions.md)

**Status:** pass-with-recorded-scope (first spike; see [§0](#0-implementation-record)).

**Validate.** Model claim reservation, physical fencing, fence durability, inspection of an old final append, and replacement installation separately. Only the newest eligible reservation may install; exact renewal must obtain usable authority; stale generations cannot perform new protected effects. Time passing alone does not prove the old append failed.

**Why valuable.** This is the best initial protocol model. It directly targets races between independently durable registry and shard state, including the already-guarded competing-reservation failure described in the source.

**Starting model and boundary.** One original final-bearing claimant and two takeover contenders, at least three relevant generations, one segment, delayed append/fence replies, eviction, and a crash. Add a liveness configuration with eventual storage recovery and a specified retrying/reconciling actor; do not assume progress from an absent actor.

**Required negative control.** Remove the newest-reservation condition so a lower generation installs after a higher fence. Separately allow installation before the fence becomes durable.

**Related work:** TLA-001, 003, 004, 006; KANI-039–042.

<a id="tla-003"></a>
### TLA-003 — Final-record sealing, ambiguous append outcomes, and owed debt

**Priority:** P0 · **Requirement anchors:** L1–L4, L11–L12, L15, D6–D7  
**Source owners:** [`src/application/append/close.rs`](src/application/append/close.rs); [`src/application/lifecycle.rs`](src/application/lifecycle.rs); [`src/application/lifecycle/raw_close.rs`](src/application/lifecycle/raw_close.rs); [`src/application/lifecycle/claims.rs`](src/application/lifecycle/claims.rs)

**Status:** pass-with-recorded-scope, since the closure fixed TLA-003-F4 and TLA-003-F5; its receipt was recorded on `3f386070` (see [§0](#0-implementation-record)).

**Validate.** Deterministic validation precedes intent installation. Final intent names the complete operation. Cancellation, timeout, ownership movement, and transient ordering refusals preserve recoverable debt. Only a definitive refusal may release the exact incarnation/operation/generation. A duplicate of an earlier non-closing append cannot satisfy a final-close promise. Terminal success must identify the operation actually completed.

**Why valuable.** Avoids both data loss through premature debt release and permanently sealed-off streams whose impossible promises cannot be cleared. It also prevents raw/product adapters from drifting in failure classification.

**Starting model and boundary.** Two operations with the same bytes but different coordination fields, one renewal, one unrelated plain seal, and an append that commits after its caller cancels. Include exact public retries at each persisted boundary.

**Required negative control.** Classify a producer gap as permanently rejected, release by operation ID without generation, or treat any duplicate success as final-close completion.

**Related work:** TLA-002, 005, 007; KANI-037, 040, 042–044, 096.

<a id="tla-004"></a>
### TLA-004 — Seal versus split/merge serialization

**Priority:** P0 · **Requirement anchors:** L13, T2, T4–T5  
**Source owners:** [`src/application/topology.rs`](src/application/topology.rs); [`src/application/lifecycle.rs`](src/application/lifecycle.rs); [`src/segmap.rs`](src/segmap.rs); [`docs/seal-transitions.md`](docs/seal-transitions.md)

**Validate.** Topology intent and seal intent cannot overlap in an unsupported descriptor state. A pending topology transition must finish or resolve before seal installation. Phase-B topology publication must recheck lifecycle and incarnation. A sealed descriptor implies all required live segments are durably closed and no transition remains pending.

**Why valuable.** Separate proofs of sealing and topology can both pass while their composition deadlocks or publishes an incomplete terminal state. This item checks that interface explicitly.

**Starting model and boundary.** Compose the seal and split/merge state machines with one shared descriptor; do not reimplement both algorithms in a third model. Use one parent, two possible children, one final-bearing seal, and crashes between physical closure and descriptor publication.

**Required negative control.** Permit phase B under Sealing, or permit a seal to install over pending topology. Require either an unsafe publication or a recoverable-work liveness failure.

**Related work:** TLA-002, 003, 009, 010; KANI-030–032, 040.

<a id="tla-005"></a>
### TLA-005 — Commit groups, dependency barriers, and observable replies

**Priority:** P0 · **Requirement anchors:** D1–D8  
**Source owners:** [`src/shard/transaction/finalize.rs`](src/shard/transaction/finalize.rs); [`src/shard/transaction/prepare.rs`](src/shard/transaction/prepare.rs); [`src/shard/transaction/publish.rs`](src/shard/transaction/publish.rs); [`src/shard/commit_plan.rs`](src/shard/commit_plan.rs)

**Status:** pass-with-recorded-scope (first spike; see [§0](#0-implementation-record)).

**Validate.** An append success, duplicate success, idempotent close, and state-dependent refusal cannot become observable before the durable state that justifies that particular result. A no-write transaction may depend on an earlier group. Failed durability cannot authorize dependent replies or publications. Responses bind the exact operation, key, and offset range.

**Why valuable.** This is the direct customer-facing no-acknowledged-data-loss assurance case. It catches mistakes that a simple “all writes wait for WAL” model misses, especially dependent read-only outcomes.

**Starting model and boundary.** Two groups and three requests: an original append, its retry, and a state-dependent refusal. Separate local staging, applied state, durability completion, effect claiming, and client receipt. Include failure of either group and loss of a post-durability response.

**Required negative control.** Skip the prior barrier for a transaction with no new writes, publish before durability, or let a failed group release its dependent responses.

**Related work:** TLA-006, 007, 026, 035; KANI-036–038, 045, 047, 096.

<a id="tla-006"></a>
### TLA-006 — Commit handoff, retirement, and already-durable completion

**Priority:** P0 · **Requirement anchors:** D1–D5, T11–T12, R1–R3  
**Source owners:** [`src/shard/commit_handoff.rs`](src/shard/commit_handoff.rs); [`src/shard/commit_handoff/loom_tests.rs`](src/shard/commit_handoff/loom_tests.rs); [`src/shard/lifecycle.rs`](src/shard/lifecycle.rs); [`src/shard.rs`](src/shard.rs)

**Status:** pass-with-recorded-scope (first spike; see [§0](#0-implementation-record)).

**Validate.** Each effect batch is claimed at most once and is either completed from an authorized durable result or rejected according to the handoff contract. Retirement blocks new unauthorized work and drains/settles existing ownership. Work already claimed as durable may finish responding after retirement; work merely applied may not inherit that permission.

**Why valuable.** Separates a legitimate late response from a stale writer. An overly strict model rejects correct behavior; an overly loose model permits data loss or double publication.

**Starting model and boundary.** An actor, a durability completer, and a retirement actor with two groups. Distinguish claimed-durable, unclaimed-durable, and non-durable batches, plus requester cancellation. Map model transitions to the existing Loom-tested implementation and keep memory-order validation in Loom.

**Required negative control.** Allow retirement to reclaim an already claimed batch, or let a new batch be admitted after the close boundary without valid authority.

**Related work:** TLA-005, 011, 028; KANI-069, 070.

<a id="tla-007"></a>
### TLA-007 — Producer idempotence, sequence advancement, and ambiguous retry

**Priority:** P0 · **Requirement anchors:** P1–P8, D6–D8  
**Source owners:** [`src/shard/commit_plan.rs`](src/shard/commit_plan.rs); [`src/shard/transaction/append.rs`](src/shard/transaction/append.rs); [`src/shard/transaction/overlay.rs`](src/shard/transaction/overlay.rs); [`src/application/append/submit.rs`](src/application/append/submit.rs)

**Validate.** Producer/sequence state has the complete tenant/incarnation/key scope. Exact retained retries create no new offsets, hash conflicts are distinguished, epoch transitions preserve the protocol ordering, and admitted predecessors can resolve a temporarily gapped request. Durable operation state and returned retry metadata agree with the actual retained-state contract, including legacy sentinels and older duplicates.

**Why valuable.** Protects clients from duplicating data after deadlines or owner movement while avoiding an invented guarantee of unlimited historical replay-result storage.

**Starting model and boundary.** Two keys, two producer epochs, three sequences, two concurrent requests, and a lost reply. Model latest remembered producer state explicitly; do not silently add an unbounded deduplication log unavailable to production.

**Required negative control.** Advance producer state before the containing commit is durable, scope it only by producer name, or let a duplicate consume an offset.

**Related work:** TLA-005, 012, 025; KANI-036–038, 043, 045.

<a id="tla-008"></a>
### TLA-008 — Create, initialization, initial contents, and delete races

**Priority:** P0 · **Requirement anchors:** C1–C8  
**Source owners:** [`src/application/creation.rs`](src/application/creation.rs); [`src/application/creation/initialization.rs`](src/application/creation/initialization.rs); [`src/application/creation/claim.rs`](src/application/creation/claim.rs); [`src/application/creation/raw.rs`](src/application/creation/raw.rs); [`src/application/creation/product.rs`](src/application/creation/product.rs)

**Validate.** Initializing descriptors are not Ready or catalog-visible. Same-operation replay joins/resumes initialization; a wrong key or stale creator cannot do so. Initial body, close-on-create, seed data, and required references must complete durably before success. Concurrent deletion cannot yield success for a vanished/replaced target or abandon untracked cleanup.

**Why valuable.** Creation is a multi-step recovery protocol, not just a descriptor PUT. The model catches exposure of incomplete streams and initialization debt that only appears after a crash.

**Starting model and boundary.** Two creators, one deleter, initial content, a close flag, and two incarnations. Include claim expiry without readiness, committed content with lost reply, and recovery by the original public operation. Fork-specific reference detail is composed from TLA-013.

**Required negative control.** Treat an aged initializing claim as Ready, or publish readiness using only the stream name after a delete/recreate.

**Related work:** TLA-001, 003, 013, 041; KANI-032, 043, 044.

<a id="tla-009"></a>
### TLA-009 — Split intent, parent closure, and successor publication

**Priority:** P0 · **Requirement anchors:** T1–T6, T9–T10  
**Source owners:** [`src/application/topology.rs`](src/application/topology.rs); [`src/segmap.rs`](src/segmap.rs); [`src/scaler3/controller.rs`](src/scaler3/controller.rs); [`docs/ROUTING-V3.md`](docs/ROUTING-V3.md)

**Validate.** A split preserves the key-space partition and predecessor lineage. Parent closure is durable before successor publication permits new writes through the new map. The persisted transition is resumable after each phase. An old closed physical segment does not imply the logical collection is terminal.

**Why valuable.** Protects per-key ordering and availability during automatic scaling. It also tests that capacity expansion is a routing change, not simply descriptor bookkeeping.

**Starting model and boundary.** One parent, two children with independently represented physical routes, keys on both sides of the split point, and old/new routers. Include physical closure success followed by publication loss, and delete/recreate before stale phase B.

**Required negative control.** Publish children before parent closure, omit predecessor metadata, or let an old physical close become a terminal logical-read result.

**Related work:** TLA-004, 010, 012, 025; KANI-028–032, 035, 072.

<a id="tla-010"></a>
### TLA-010 — Merge intent and preservation of both predecessor histories

**Priority:** P1 · **Requirement anchors:** T1–T5, T9–T10  
**Source owners:** [`src/application/topology.rs`](src/application/topology.rs); [`src/segmap.rs`](src/segmap.rs); [`src/scaler3/controller.rs`](src/scaler3/controller.rs)

**Validate.** Only eligible adjacent live ranges merge. Both parents close before their successor is published; neither parent's producer, sequence, consumer, or read lineage is lost. Repeated or competing merges are safe, and old phase-B work cannot publish into a different incarnation or lifecycle.

**Why valuable.** Merge is not just split in reverse: two independently progressing parents create additional partial-completion and lineage-loss cases.

**Starting model and boundary.** Two adjacent parents, one non-adjacent candidate, a successor, and a seal contender. Crash after closing only one parent. Include a retry observing a transition it did not originally create.

**Required negative control.** Accept non-adjacent parents, publish after only one close, or copy only one predecessor's lineage.

**Related work:** TLA-004, 009, 012, 025; KANI-028, 030–032.

<a id="tla-011"></a>
### TLA-011 — Serving possession, owner fencing, and shard movement

**Priority:** P0 · **Requirement anchors:** T10–T12, D1, R1  
**Source owners:** [`src/ownership.rs`](src/ownership.rs); [`src/sharddir.rs`](src/sharddir.rs); [`src/shard_directory.rs`](src/shard_directory.rs); [`src/shard/lifecycle.rs`](src/shard/lifecycle.rs); [`src/peer.rs`](src/peer.rs)

**Status:** pass-with-recorded-scope (first spike; see [§0](#0-implementation-record)).

**Validate.** A routing preference alone does not grant durable write authority. Movement establishes the required storage/engine fencing before the new owner performs protected effects. Stale owners cannot acknowledge newly unauthorized writes. Independently already-durable responses retain the exception from TLA-006. Uncertain movement cannot be translated into fabricated success.

**Why valuable.** This is the bridge between fleet/ring state and actual storage safety. It prevents treating a locally fresh routing table as proof that no old writer remains.

**Starting model and boundary.** Two nodes, one shard, an override, stale ring observations, delayed old work, and a move interrupted by crash. Represent single-instance/bootstrapping mode separately from managed fleet mode. Model the pinned storage fencing contract as an explicit dependency.

**Required negative control.** Authorize solely from ring preference, or allow a new owner to begin before the dependency establishes fencing.

**Related work:** TLA-005, 006, 012, 039; KANI-062, 070, 095.

<a id="tla-012"></a>
### TLA-012 — Stale routers and per-key traversal through topology lineage

**Priority:** P1 · **Requirement anchors:** P4–P5, T3, T9–T10, S6  
**Source owners:** [`src/application/append/route.rs`](src/application/append/route.rs); [`src/application/read.rs`](src/application/read.rs); [`src/application/read_keys.rs`](src/application/read_keys.rs); [`src/application/read_remote.rs`](src/application/read_remote.rs); [`src/segmap.rs`](src/segmap.rs)

**Validate.** An authorized request remains bound to its project and incarnation through bounded reroutes. A stale map can cause extra work or a retry but not cross-key ordering violations, extra committed copies under producer idempotence, skipped predecessors, or a false terminal read. Different keys are not forced into an artificial global order.

**Why valuable.** Most clients encounter topology through stale routes rather than through the topology API itself. This model checks the end-to-end behavior exposed by that stale view.

**Starting model and boundary.** A split then a merge, two keys, one old router, one current router, one timeout, and key-scoped producers. Distinguish local/raw default-key reads from product reads.

**Required negative control.** Refresh to a same-name replacement descriptor, skip a closed predecessor, or retry an append under a different routing key.

**Related work:** TLA-007, 009, 010, 018, 027; KANI-028–035, 057–060, 083.

<a id="tla-013"></a>
### TLA-013 — Fork initialization and durable source-reference installation

**Priority:** P0 · **Requirement anchors:** C5–C7, F7, F10  
**Source owners:** [`src/application/creation/fork.rs`](src/application/creation/fork.rs); [`src/application/creation/anchor.rs`](src/application/creation/anchor.rs); [`src/application/creation/initialization.rs`](src/application/creation/initialization.rs)

**Validate.** A Ready child has a valid source incarnation and the required durable reference. Source deletion and child creation serialize safely. Reference installation, child recheck, seed creation, and readiness publication remain distinct recoverable steps. A child deleted mid-initialization cannot leave an untracked permanent source reference.

**Why valuable.** The child descriptor and source reference live in different mutation steps. Their race is a classic source of either dangling forks or immortal retained storage.

**Starting model and boundary.** One source, two prospective children, one source deleter, and a child delete/recreate. Crash before/after each reference and readiness operation; permit CAS failures and response loss.

**Required negative control.** Publish Ready before the source reference is durable, or omit the post-install child identity recheck.

**Related work:** TLA-008, 014, 015, 019; KANI-032–035, 043.

<a id="tla-014"></a>
### TLA-014 — Fork deletion, reference release, and recursive cleanup debt

**Priority:** P0 · **Requirement anchors:** F5–F10, H13–H14  
**Source owners:** [`src/application/creation/deletion.rs`](src/application/creation/deletion.rs); [`src/application/creation/anchor.rs`](src/application/creation/anchor.rs); [`docs/creation-transitions.md`](docs/creation-transitions.md)

**Validate.** Hard deletion requires release of the final required child reference. Releases are idempotent and pinned to the source incarnation. Recursive cleanup debt survives a crash at every ancestor; retrying the documented operation resumes it. Temporary retained references are allowed only with a recoverable path, while live children never lose required ancestry.

**Why valuable.** This protects against both irreversible data loss and leaks that persist because a cleanup cursor or debt marker was forgotten after a partial cascade.

**Starting model and boundary.** A grandparent, parent, two children, deletion of a child, and a source-name replacement attempt. Three ancestry levels are mandatory for the recursive-debt case. State liveness assumptions separately for public retry and background reconciliation.

**Required negative control.** Forget cascade debt before releasing the ancestor, decrement twice on retry, or identify a source only by name.

**Related work:** TLA-001, 013, 019; KANI-032–034, 047.

<a id="tla-015"></a>
### TLA-015 — Fork read views, cut boundaries, and topology eligibility

**Priority:** P1 · **Requirement anchors:** F1–F4, F11–F12  
**Source owners:** [`src/application/creation/fork.rs`](src/application/creation/fork.rs); [`src/application/read.rs`](src/application/read.rs); [`src/application/read_retention_probe.rs`](src/application/read_retention_probe.rs); [`src/registry.rs`](src/registry.rs); [`PER-KEY-ORDERING.md`](PER-KEY-ORDERING.md)

**Validate.** A fork exposes the specified source prefix plus its own suffix, with no duplicate partial-record materialization. Every ancestor hop validates incarnation and decryption context. Chains remain cycle-free/depth-bounded. Raw fork views select the default key, and unsupported topology/fork combinations are rejected before side effects.

**Why valuable.** Reference safety alone does not establish that the child returns the right bytes or uses the right ancestor context. This model covers the logical read view and eligibility contract.

**Starting model and boundary.** Three ancestry levels, a record cut inside a supported binary record, two routing keys, one child suffix append, and an attempted unsupported split. Represent sub-offset behavior only for formats the current product contract admits.

**Required negative control.** Read beyond the source cut, materialize a partial prefix twice, or use the child's incarnation when decrypting ancestor data.

**Related work:** TLA-012–014, 018, 021; KANI-005, 024, 032, 058, 082.

<a id="tla-016"></a>
### TLA-016 — History absorption, publication, and safe hot-data trimming

**Priority:** P0 · **Requirement anchors:** H1, H3–H4, H13  
**Source owners:** [`src/history/gather.rs`](src/history/gather.rs); [`src/history/worker.rs`](src/history/worker.rs); [`src/shard/transaction/maintenance.rs`](src/shard/transaction/maintenance.rs); [`src/shard/commit_plan.rs`](src/shard/commit_plan.rs); [`docs/HISTORY-V2.md`](docs/HISTORY-V2.md)

**Status:** pass-with-recorded-scope (first spike, reworked for the absorber adopted by the merge `3a24eace`; see [§0](#0-implementation-record)).

**Validate.** The absorbed boundary advances only after all required canonical frames and postings are durable. The trim-safe boundary has the required relation to published absorption, and physical trimming never removes the last required recoverable copy. Repeated absorption and lost publication replies cannot create holes or falsely advance frontiers.

**Why valuable.** This is the most direct destructive-maintenance model. An ordering error here can delete data that append durability correctly protected.

**Starting model and boundary.** Three record offsets, canonical data and postings represented separately, one old/new absorbed boundary, a reader, and a crash between every durability/publication/trim step. Include failed history flush and a delayed old publisher.

**Required negative control.** Publish absorbed before history flush, make only canonical data durable without postings, or trim to the newly proposed rather than allowed safe frontier.

**Related work:** TLA-017–019; KANI-007–015, 046–047.

<a id="tla-017"></a>
### TLA-017 — Dirty-work discovery, bounded deferral, and restart convergence

**Priority:** P1 · **Requirement anchors:** H5–H8, H14–H15, R4, R8  
**Source owners:** [`src/history/gather.rs`](src/history/gather.rs); [`src/history/worker.rs`](src/history/worker.rs); [`src/shard.rs`](src/shard.rs); [`src/touch.rs`](src/touch.rs); [`src/scaler3/controller.rs`](src/scaler3/controller.rs)

**Validate.** Budget-deferred history/trim work remains discoverable without a later customer append. Restart discovers untouched durable backlog; failed scans retry without skipping work; ownership loss clears local summaries without deleting durable debt. A hot item cannot indefinitely starve eligible cold backlog under the stated scheduling policy.

**Why valuable.** A system can preserve data yet stop absorbing it forever. This model targets that quiet liveness failure and the related memory/cost amplification from unbounded rescanning.

**Starting model and boundary.** Three dirty streams, a per-pass budget of one, one continually active stream, a transient scan failure, and restart with empty memory. Explore recovery with no new customer signal. Model work-token accounting, not real elapsed latency or provider request pricing.

**Required negative control.** Drop a deferred stream from the pending set, advance a scan cursor after an error, or require a new append to rediscover old debt.

**Related work:** TLA-016, 019, 029, 030; KANI-047, 069, 071.

<a id="tla-018"></a>
### TLA-018 — Exact composition of history, hot storage, and read visibility

**Priority:** P0 · **Requirement anchors:** H1–H2, H9–H11, D8–D9  
**Source owners:** [`src/application/read_batch.rs`](src/application/read_batch.rs); [`src/application/read_scan.rs`](src/application/read_scan.rs); [`src/application/read_keys.rs`](src/application/read_keys.rs); [`src/history/postings_read.rs`](src/history/postings_read.rs); [`src/shard/record.rs`](src/shard/record.rs)

**Status:** pass-with-recorded-scope, since the closure fixed TLA-018-F3; its receipt was recorded on `cb4c6b47`. H11 is claimed only in the scope of the open obligation TLA-018-F2 (see [§0](#0-implementation-record)).

**Validate.** A permitted read view combines the history prefix and hot suffix with exact coverage: no fabricated records, missing eligible offsets, or duplicate delivery caused by a moving boundary. Data corruption or missing required postings cannot become a false complete page. Visibility must be modeled per API: distinguish any permitted applied-state read from a durability promise.

**Why valuable.** Correct absorption ordering does not alone protect a reader that observes mismatched frontiers or resumes from an incorrectly advanced cursor.

**Starting model and boundary.** A reader making at least two storage observations, an absorber, a trimmer, two keys, and a page boundary. Include a large first record, a missing page, and an ownership change. Define the API's allowed snapshot/retention semantics before asserting completeness.

**Required negative control.** Use a new absorbed frontier with an old history view, advance past an unreturned eligible record, or convert missing postings into an empty-success result.

**Related work:** TLA-012, 015–016, 020–021; KANI-009–023, 055–060, 063, 083.

<a id="tla-019"></a>
### TLA-019 — Reachability-based garbage collection and reader/fork protection

**Priority:** P0 · **Requirement anchors:** F6, H13–H14, R4  
**Source owners:** [`src/application/creation/deletion.rs`](src/application/creation/deletion.rs); [`src/history.rs`](src/history.rs); [`src/shard/history_partition.rs`](src/shard/history_partition.rs); [`src/bootstrap.rs`](src/bootstrap.rs); [`docs/dst/DST-EXPANSION-SPEC.md`](docs/dst/DST-EXPANSION-SPEC.md)

**Status:** pass-with-recorded-scope (first spike; see [§0](#0-implementation-record)).

**Validate.** Nothing required by a live descriptor, topology predecessor, fork, valid reader/checkpoint, or uncompleted history transition is deleted. Eligible unreachable objects eventually become reclaimable under the adopted inventory/GC policy. A stale inventory cannot justify deleting a newer reachable object or indefinitely suppress cleanup.

**Why valuable.** This checks the object graph behind no-data-loss claims rather than merely checking that current sample reads return records.

**Starting model and boundary.** A tiny graph with a manifest, old/new data objects, one reader pin, and a fork reference. Split object discovery, reachability observation, delete eligibility, and delete execution when the dependency contract requires it. Treat upstream SlateDB GC/checkpoints as an explicit assumed interface, not source verified by this repository.

**Required negative control.** Delete from an old reachability snapshot without the required generation condition, ignore a reader/fork pin, or permanently trust a stale empty inventory.

**Related work:** TLA-013–018, 020, 044; KANI-033–034, 046–047.

<a id="tla-020"></a>
### TLA-020 — Reader/postings caches: single-flight, cancellation, and identity

**Priority:** P1 · **Requirement anchors:** H12, R2–R4  
**Source owners:** [`src/postings_cache.rs`](src/postings_cache.rs); [`src/shard/history_partition.rs`](src/shard/history_partition.rs); [`src/sharddir.rs`](src/sharddir.rs); [`src/history/postings_read.rs`](src/history/postings_read.rs)

**Validate.** One opening/fill attempt owns publication for its cache key and generation. Cancellation of a waiter does not corrupt the shared fill; retirement prevents late publication into a replacement owner. Cache identity includes the store/tenant/incarnation context it needs. Eviction respects pins and retained-budget ownership, and failed opens remain retryable.

**Why valuable.** A cache can silently turn a sound storage protocol into cross-incarnation reads, leaked resources, or a permanently wedged single-flight entry.

**Starting model and boundary.** Two stores with colliding local IDs, two waiters, one opener, one evictor, and close/reopen. Include leader cancellation, failure followed by retry, and completion after replacement. Bound logical cache charge separately from physical allocator behavior.

**Required negative control.** Key a cache only by local offset/stream ID, let an old fill publish into a replacement generation, or cancel all shared work when one waiter disappears.

**Related work:** TLA-018–019, 027–029; KANI-010–011, 064, 070.

<a id="tla-021"></a>
### TLA-021 — Snapshot pagination, cursor validity, and retention races

**Priority:** P1 · **Requirement anchors:** S7, H2, H9, T3  
**Source owners:** [`src/product_cursor.rs`](src/product_cursor.rs); [`src/product_cursor/decode.rs`](src/product_cursor/decode.rs); [`src/application/read_scan.rs`](src/application/read_scan.rs); [`src/application/read_request.rs`](src/application/read_request.rs); [`src/application/read_retention_probe.rs`](src/application/read_retention_probe.rs)

**Validate.** A cursor resumes the correct project/incarnation/key/operation and any snapshot/map bounds its kind requires. Cursor progress tracks consumed work without silently omitting eligible records. Expiry, deletion, retention loss, corruption, and topology changes produce the specified continuation or explicit error—not false exhaustion.

**Why valuable.** Pagination errors are difficult to observe in single-page tests and can silently truncate analytical or consumer workloads.

**Starting model and boundary.** Two pages, two segments, two cursor kinds, a retention advance between pages, and a delete/recreate. Include empty pages caused by filtering versus genuine completion. Authentication is an idealized dependency only where needed; payload interpretation remains modeled.

**Required negative control.** Accept a cursor for another kind/incarnation, skip a segment after a budget cutoff, or report retained-away history as normal end-of-stream.

**Related work:** TLA-012, 015, 018, 041; KANI-005, 011, 055–060, 063, 083.

<a id="tla-022"></a>
### TLA-022 — Consumer creation, deletion, and generation fencing

**Priority:** P0 · **Requirement anchors:** Q3, Q6, Q8, L14  
**Source owners:** [`src/application/consumer.rs`](src/application/consumer.rs); [`src/application/consumer/deletion.rs`](src/application/consumer/deletion.rs); [`src/shard/commit_plan.rs`](src/shard/commit_plan.rs); [`src/queue.rs`](src/queue.rs)

**Validate.** Reusing a consumer name creates a distinct generation. Old leases, settlements, retries, delayed configuration writes, and deletion work cannot mutate the replacement generation. Legacy empty state binds only according to the documented rule. Fence durability and cleanup publication are separated so crashes do not resurrect old consumer authority.

**Why valuable.** Consumer-name reuse is an ABA problem independent of stream-incarnation reuse. It can wrongly settle or redeliver messages even when the stream itself is healthy.

**Starting model and boundary.** One stream, two consumer generations, one outstanding lease, a delete/recreate, and a delayed old settlement. Include crash after fence persistence but before physical state cleanup.

**Required negative control.** Omit consumer generation from a key/token check, or allow an old deletion pass to remove replacement state.

**Related work:** TLA-023–025; KANI-048–053, 059.

<a id="tla-023"></a>
### TLA-023 — Consumer delivery leases, FIFO, acknowledgment, retry, and extension

**Priority:** P0 · **Requirement anchors:** Q1–Q6  
**Source owners:** [`src/application/consumer/delivery.rs`](src/application/consumer/delivery.rs); [`src/application/consumer_remote.rs`](src/application/consumer_remote.rs); [`src/shard/transaction/prepare.rs`](src/shard/transaction/prepare.rs); [`src/shard/transaction/publish.rs`](src/shard/transaction/publish.rs); [`src/queue.rs`](src/queue.rs)

**Validate.** Delivery respects the adopted FIFO semantics per key while different keys progress independently. Only the matching current lease/consumer generation may acknowledge, retry, or extend. Durable acknowledgment prevents future redelivery, but does not undo a delivery already in flight. Expired unacknowledged work may redeliver with new authority. Settlement results wait for required durability.

**Why valuable.** This defines the queue guarantee precisely: at-least-once delivery, not exactly-once execution of arbitrary external consumer effects.

**Starting model and boundary.** Two keys, two workers, two messages on one key, an expiring lease, and concurrent ack/extend/retry. Use abstract time with skew/reversal assumptions matching the actual clock contract, plus crash and lost replies.

**Required negative control.** Accept a stale lease generation, let a second message bypass the key's unsettled head contrary to the contract, or reply to ack before its state is durable.

**Related work:** TLA-005, 022, 024–025; KANI-049–054, 059.

<a id="tla-024"></a>
### TLA-024 — Dead-letter transfer saga and destination incarnation

**Priority:** P0 · **Requirement anchors:** Q6, Q8  
**Source owners:** [`src/application/consumer/delivery.rs`](src/application/consumer/delivery.rs); [`src/application/consumer.rs`](src/application/consumer.rs); [`src/shard/transaction/prepare.rs`](src/shard/transaction/prepare.rs); [`src/queue.rs`](src/queue.rs); [`src/dst/tests/consumer_dlq.rs`](src/dst/tests/consumer_dlq.rs); [`src/dst/tests/consumer_saga.rs`](src/dst/tests/consumer_saga.rs)

**Validate.** At the retry limit, destination append and source settlement obey the documented saga/idempotency contract. A crash between them cannot lose the only recoverable copy or create multiple logical DLQ transfers under the chosen identity. Destination identity is pinned; deleting/recreating its name does not silently redirect old debt.

**Why valuable.** DLQ transfer crosses state owners and can look successful while silently dropping a message. A model forces the exact recovery and destination-failure policy to be stated.

**Starting model and boundary.** One source message, an exhausted lease, two destination incarnations, a lost destination reply, and retry after restart. Represent in-stream reserved-sequence and separate-target modes separately if both remain supported.

**Required negative control.** Settle the source before durable destination acceptance, derive a fresh transfer identity on retry, or resolve the destination only by its current name.

**Related work:** TLA-007, 022–023, 025; KANI-048, 053–054, 058–059.

<a id="tla-025"></a>
### TLA-025 — Consumer progress and settlement across split/merge lineage

**Priority:** P1 · **Requirement anchors:** Q1–Q3, Q6–Q7, T9  
**Source owners:** [`src/application/consumer/delivery.rs`](src/application/consumer/delivery.rs); [`src/application/consumer_remote.rs`](src/application/consumer_remote.rs); [`src/application/topology.rs`](src/application/topology.rs); [`src/segmap.rs`](src/segmap.rs)

**Validate.** Consumer traversal reaches every eligible predecessor/successor message, preserves per-key ordering, and settles a logical message at most once in its state contract. Moving a key does not reset lease authority or discard unsettled predecessor work. Split/merge completion and consumer deletion cannot publish incompatible generations.

**Why valuable.** Correct queue semantics on a fixed segment do not imply correct semantics after a topology transition. This is a required composition check, not a duplicate lease model.

**Starting model and boundary.** Two parent segments, a split or merge, one leased predecessor message, one successor message, and a stale worker. Include a consumer-generation replacement during the transition.

**Required negative control.** Start successor delivery before required predecessor settlement, or use segment-local consumer state without lineage/generation checks.

**Related work:** TLA-009–010, 022–024; KANI-028–031, 048, 051–054.

<a id="tla-026"></a>
### TLA-026 — Durable watch publication, journal gaps, and capability routing

**Priority:** P1 · **Requirement anchors:** W1–W5, S1  
**Source owners:** [`src/touch.rs`](src/touch.rs); [`src/touch_keys.rs`](src/touch_keys.rs); [`src/application/watch.rs`](src/application/watch.rs); [`src/crypto.rs`](src/crypto.rs); [`src/dst/tests/watch_observation.rs`](src/dst/tests/watch_observation.rs)

**Validate.** Notifications do not promise data before the corresponding durable/readable state. A bounded journal gap produces explicit resynchronization rather than a false no-change result. A capability authorizes only the exact wait operation and bound identity, and valid resumption works under the adopted restart/key contract.

**Why valuable.** Notification correctness matters even when stored records are intact: a false no-change result can strand a consumer indefinitely.

**Starting model and boundary.** One watcher, two matching appends, a small journal that overflows, restart, a split, and replay of the capability on a different route. Treat MAC security conditionally; model authorization routing and journal semantics concretely.

**Required negative control.** Notify from local staging, silently advance a lagging journal cursor, or allow a watch capability on a mutation route.

**Related work:** TLA-005, 027, 032–033; KANI-076–079, 081.

<a id="tla-027"></a>
### TLA-027 — SSE source switching, cutoffs, resynchronization, and cancellation

**Priority:** P1 · **Requirement anchors:** T3, W1–W2, H12, R1–R3  
**Source owners:** [`src/sse/feed.rs`](src/sse/feed.rs); [`src/sse/feed/drive.rs`](src/sse/feed/drive.rs); [`src/sse/source.rs`](src/sse/source.rs); [`src/sse/source/spans.rs`](src/sse/source/spans.rs); [`src/sse/session.rs`](src/sse/session.rs); [`src/sse/service.rs`](src/sse/service.rs)

**Validate.** A session switching among history, live tail, and peer sources preserves its delivery contract. Source-generation cutoffs prevent old work from publishing after replacement; a missed retention window causes explicit resync/error, not silent skipping. Cancellation releases ownership and buffers without aborting another subscriber's shared work.

**Why valuable.** Long-lived sessions combine nearly every transient boundary: stale topology, owner movement, bounded caches, backpressure, and auth refresh.

**Starting model and boundary.** One session, two source generations, two pages, a blocked outbound send, ownership movement, and a retention gap. State whether replay on reconnect is allowed; do not accidentally assert global exactly-once network delivery.

**Required negative control.** Publish an old source completion after the new source takes over, or map a fatal source cutoff to an empty successful page.

**Related work:** TLA-012, 018, 020, 026, 028–029, 033; KANI-005, 063–064, 068, 083.

<a id="tla-028"></a>
### TLA-028 — Runtime task ownership, single-flight opening, and shutdown

**Priority:** P1 · **Requirement anchors:** R1–R4  
**Source owners:** [`src/runtime.rs`](src/runtime.rs); [`src/tasks.rs`](src/tasks.rs); [`src/tasks/shutdown.rs`](src/tasks/shutdown.rs); [`src/bootstrap/runtime_handoff.rs`](src/bootstrap/runtime_handoff.rs); [`src/shard/history_partition.rs`](src/shard/history_partition.rs); [`src/sharddir.rs`](src/sharddir.rs)

**Validate.** Every task/engine/DB handle belongs to one lifecycle owner. Closing rejects new required work and eventually terminates owned work under the stated cancellation assumptions. Open completion after shutdown cannot publish a usable resource. Concurrent callers share one valid open/close result; failures cannot leave immortal opening states. Separate runtimes share no mutable service authority.

**Why valuable.** Resource leaks and zombie tasks can invalidate fencing and cause cascading outages even when each storage operation is individually correct.

**Starting model and boundary.** Two callers, one opener, one closer, one failing required task, and two runtime identities. Include cancellation before and after resource creation and failure during close. Do not assume arbitrary blocked I/O is instantly cancellable.

**Required negative control.** Publish an opened DB after stop, accept a task after shutdown admission closes, or let a failed leader strand waiters forever.

**Related work:** TLA-006, 011, 020, 027, 029; KANI-064, 069–070.

<a id="tla-029"></a>
### TLA-029 — Reservation ownership and bounded resource admission

**Priority:** P1 · **Requirement anchors:** R2–R3, R5–R6, H5, H12  
**Source owners:** [`src/admission.rs`](src/admission.rs); [`src/quota.rs`](src/quota.rs); [`src/retained_bytes.rs`](src/retained_bytes.rs); [`src/application/read_batch.rs`](src/application/read_batch.rs); [`src/postings_cache.rs`](src/postings_cache.rs); [`src/sse/registry.rs`](src/sse/registry.rs)

**Validate.** Logical reservations are conserved as ownership moves from request body to queue, shared frame, cache, or subscriber. Cancellation/refusal releases exactly the resources it owns, not another alias's charge. Capacity admission is checked at the intended scope, including process/project bounds. Safety state needed by queued work is not evicted merely to meet a cache limit.

**Why valuable.** A useful complement to memory tests: it explains who is supposed to own each charge and when the charge can legally disappear.

**Starting model and boundary.** Two projects, two buffer aliases, one queued request, one cache eviction, and a canceled subscriber. Model backing allocations separately from slices; count logical charge and reservation ownership, not all real process RSS.

**Required negative control.** Release on the first alias drop, charge only visible slice length, or transfer a queue item without its accounting owner.

**Related work:** TLA-017, 020, 027–028, 030, 036; KANI-063–069.

<a id="tla-030"></a>
### TLA-030 — Maintenance pressure, wedged commits, and admission recovery

**Priority:** P1 · **Requirement anchors:** R5, H5–H8, H15  
**Source owners:** [`src/backpressure.rs`](src/backpressure.rs); [`src/admission.rs`](src/admission.rs); [`src/sharddir.rs`](src/sharddir.rs); [`src/sharddir/health.rs`](src/sharddir/health.rs); [`src/store_timing.rs`](src/store_timing.rs); [`docs/MAINTENANCE-BACKPRESSURE.md`](docs/MAINTENANCE-BACKPRESSURE.md)

**Validate.** Pressure engages and releases according to the configured policy without hiding a global violation behind shard-local state. Foreground shedding does not also eliminate all work capable of relieving maintenance pressure. Required-task failure changes readiness; recovery can restore admission only after the documented conditions hold.

**Why valuable.** Protects against feedback loops where overload disables its own repair mechanism, or stale health signals leave a healed system permanently closed.

**Starting model and boundary.** Two shards, foreground requests, one maintenance worker, high/low thresholds, a wedged commit, and later healing. Include a zero/disabled threshold configuration. Check safety of admission and liveness of recovery separately; no numerical latency claim follows.

**Required negative control.** Apply foreground shedding to the only cleanup path, let one quiet shard mask global pressure, or release on only one of several active pressure causes.

**Related work:** TLA-017, 028–029, 040; KANI-061–062, 065–067, 071.

<a id="tla-031"></a>
### TLA-031 — Authentication feed publication and retained high-water state

**Priority:** P1 · **Requirement anchors:** S1, S4, security feed requirements  
**Source owners:** [`src/auth/publication.rs`](src/auth/publication.rs); [`src/auth_feed.rs`](src/auth_feed.rs); [`src/project_policy.rs`](src/project_policy.rs); [`docs/CONTROL-PLANE-INTEGRATION.md`](docs/CONTROL-PLANE-INTEGRATION.md)

**Validate.** Accepted snapshots obey generation/content consistency, version monotonicity, omission/reintroduction rules, credential revocation, and workspace/ownership coupling. Rejected input mutates neither authorization nor freshness nor subscriber generation. Bounded process-local high-water eviction/restart is explicit; stronger durable anti-rollback behavior is assumed only from an independently stated publisher contract.

**Why valuable.** Catches accidental resurrection of credentials or previous owners and makes the actual limit of the in-memory defense clear.

**Starting model and boundary.** Two projects, one credential, one signing key, three snapshots, one omission, bounded-history eviction, and restart. Model policy/grant/JWKS feeds as independently arriving unless the production interface provides a stronger transaction.

**Required negative control.** Refresh age on rejected input, reintroduce an omitted entry at an old version, or change workspace without ownership-version advancement.

**Related work:** TLA-032–034, 038, 043; KANI-073–077, 080.

<a id="tla-032"></a>
### TLA-032 — Request authorization, route capabilities, and tenant confinement

**Priority:** P1 · **Requirement anchors:** S1–S6, S9  
**Source owners:** [`src/auth.rs`](src/auth.rs); [`src/product.rs`](src/product.rs); [`src/http.rs`](src/http.rs); [`src/peer.rs`](src/peer.rs); [`src/deployment_bearer.rs`](src/deployment_bearer.rs); [`src/tenant.rs`](src/tenant.rs)

**Validate.** Each route/mode admits only its intended principal/capability class. Authentication and authorization occur before expensive mutation body collection. Internal audience and workload capabilities cannot become customer authorization, or vice versa. Authorized project/incarnation context is preserved through rerouting and effects. Model Off/Shadow/Enforce behavior separately rather than granting Enforce guarantees to all modes.

**Why valuable.** A secure token verifier does not protect an endpoint that selects the wrong verifier, skips the check, or later resolves a different tenant.

**Starting model and boundary.** Two tenants with the same stream name, customer/internal/watch credentials, a mutation route, exact wait route, and near-miss route. Use abstract signature outcomes; keep token-to-route and effect confinement explicit.

**Required negative control.** Authorize a route prefix rather than the exact watch route, collect the large body before auth, or lose project context on a peer retry.

**Related work:** TLA-001, 012, 026, 031, 034; KANI-024–027, 060, 073, 078–084.

<a id="tla-033"></a>
### TLA-033 — Revocation and freshness for long-lived subscriptions

**Priority:** P1 · **Requirement anchors:** S1, security revocation/freshness requirements, R1  
**Source owners:** [`src/auth.rs`](src/auth.rs); [`src/auth/publication.rs`](src/auth/publication.rs); [`src/auth_feed.rs`](src/auth_feed.rs); [`src/sse/auth.rs`](src/sse/auth.rs); [`src/sse/session.rs`](src/sse/session.rs); [`src/sse/registry.rs`](src/sse/registry.rs)

**Validate.** A long-lived authorization lease is rechecked/terminated according to token expiry, feed freshness, revocation, and ownership changes. A blocked or replaced feed cannot extend authority indefinitely. Publication wakes the relevant recheck path, and old generation callbacks cannot reauthorize a terminated session.

**Why valuable.** Request-time authorization is insufficient for a session that may live much longer than the credential or policy that created it.

**Starting model and boundary.** One session, a queued data frame, a revocation update, a stale-feed deadline, and concurrent ownership change. Define whether already-authorized/in-flight bytes may complete; instant network-wide revocation is not assumed.

**Required negative control.** Reset freshness on failed refresh, miss generation wakeup, or continue delivery after the adopted lease cutoff without revalidation.

**Related work:** TLA-026–028, 031–032; KANI-073–080.

<a id="tla-034"></a>
### TLA-034 — Relational tenant noninterference for state and authorization

**Priority:** P2 · **Requirement anchors:** S1, S4–S5, P1, R6  
**Source owners:** [`src/tenant.rs`](src/tenant.rs); [`src/registry.rs`](src/registry.rs); [`src/runtime.rs`](src/runtime.rs); [`src/auth.rs`](src/auth.rs); [`src/queue.rs`](src/queue.rs); [`src/dst/tests/security_noninterference.rs`](src/dst/tests/security_noninterference.rs)

**Validate.** Compare two modeled executions with identical tenant-A actions but different tenant-B data/actions. Tenant B cannot change A's logical records, authority, cursors, consumer state, or billing attribution except through explicitly shared, permitted resource-pressure behavior. Publicly shared capacity effects are not incorrectly treated as forbidden information flow.

**Why valuable.** Identity unit tests catch individual key collisions; a relational model checks whether an entire effect path accidentally depends on another tenant's state.

**Starting model and boundary.** Self-compose a small system with two projects sharing names, offsets, producer IDs, and consumer IDs. Define the observation function narrowly: logical contents and authorization outcomes, not timing, cache-hit rates, or cryptographic side channels.

**Required negative control.** Remove project identity from one state key or use a process-global mutable service map shared across runtimes.

**Related work:** TLA-007, 022, 029, 032, 035–038; KANI-024–027, 033–035, 048, 060, 092.

<a id="tla-035"></a>
### TLA-035 — Durable usage outbox, ledger publication, and checkpoint acknowledgment

**Priority:** P1 · **Requirement anchors:** R6–R7, billing checkpoint requirements  
**Source owners:** [`src/billing.rs`](src/billing.rs); [`src/shard/transaction/publish.rs`](src/shard/transaction/publish.rs); [`src/shard/commit_plan.rs`](src/shard/commit_plan.rs); [`src/shard/bounded_outbox_tests.rs`](src/shard/bounded_outbox_tests.rs); [`src/billing_service.rs`](src/billing_service.rs)

**Validate.** Durable usage changes remain discoverable until the downstream ledger has durably accepted the corresponding event. A usage checkpoint cannot acknowledge newer un-emitted state. Lost ledger replies cause idempotent replay, not duplicate logical charges. Ownership movement and bounded outboxes preserve recoverable accounting debt.

**Why valuable.** Protects the monetary interpretation of durable writes and catches the classic outbox bug: marking an event delivered before delivery is durable.

**Starting model and boundary.** Two source versions, one bounded outbox slot, a ledger append, a lost reply, and a later checkpoint acknowledgment. Include an old ack racing a newer usage update and a source engine moving.

**Required negative control.** Advance the source checkpoint before ledger durability, accept a stale ack for a newer version, or discard overflow instead of retaining discoverable debt.

**Related work:** TLA-005, 017, 036–038; KANI-047, 069, 088–092.

<a id="tla-036"></a>
### TLA-036 — Read-usage accumulation, durable spooling, and drain cancellation

**Priority:** P1 · **Requirement anchors:** R2, R6–R7, read metering requirements  
**Source owners:** [`src/billing/read_accumulator.rs`](src/billing/read_accumulator.rs); [`src/billing/read_spool.rs`](src/billing/read_spool.rs); [`src/billing.rs`](src/billing.rs); [`docs/OBSERVABILITY-BILLING.md`](docs/OBSERVABILITY-BILLING.md)

**Validate.** Accumulator sealing, batch sequence assignment, spool persistence, ledger emission, removal, and requeue have explicit ownership. Cancellation cannot drop a durably spooled batch or duplicate its logical charge. Overload/coalescing preserves attribution under the adopted cardinality policy. Volatile observations may have a distinct crash-loss contract that must be decided and recorded.

**Why valuable.** Avoids silently claiming durable metering for in-memory observations, while rigorously protecting the portion that has crossed the durability boundary.

**Starting model and boundary.** Two billing identities, one active map, two sealed batches, a spool, a canceled drainer, and full-queue pressure. Crash once before spool persistence and once after it. Treat those two loss obligations differently.

**Required negative control.** Remove a spool entry before ledger acceptance, lose a drained batch on cancellation, or merge rows across ownership identities.

**Related work:** TLA-029, 035, 037–038; KANI-064, 069, 088, 090–092.

<a id="tla-037"></a>
### TLA-037 — Usage rollup, replay idempotence, corrections, and month close

**Priority:** P1 · **Requirement anchors:** R7, billing rollup/settlement requirements  
**Source owners:** [`src/rollup.rs`](src/rollup.rs); [`src/rollup/page.rs`](src/rollup/page.rs); [`src/rollup/reconciliation.rs`](src/rollup/reconciliation.rs); [`src/rollup/close.rs`](src/rollup/close.rs); [`src/rollup/totals.rs`](src/rollup/totals.rs)

**Validate.** A ledger page is applied atomically with its cursor under the intended storage contract. Replaying an event does not double-count it. Corrections and month-close work preserve source-version identity and totals; incomplete reconciliation cannot publish a falsely final statement. Late events follow the adopted correction/reopen policy.

**Why valuable.** The ledger can be correct while a downstream cursor or partial month-close result creates incorrect bills. This checks that second durability boundary.

**Starting model and boundary.** Two pages, a duplicate event, one correction, two months, and a crash between aggregate work and cursor/finalization publication. Model “pending/provisional/final” explicitly rather than as a single boolean.

**Required negative control.** Advance the cursor before aggregates are durable, apply the same event twice, or mark a month final while a required scan failed.

**Related work:** TLA-035–036, 038; KANI-085–094.

<a id="tla-038"></a>
### TLA-038 — Workspace ownership changes and billing attribution boundaries

**Priority:** P1 · **Requirement anchors:** security ownership coupling; billing identity requirements  
**Source owners:** [`src/billing.rs`](src/billing.rs); [`src/billing/read_accumulator.rs`](src/billing/read_accumulator.rs); [`src/rollup.rs`](src/rollup.rs); [`src/auth/publication.rs`](src/auth/publication.rs); [`docs/CONTROL-PLANE-INTEGRATION.md`](docs/CONTROL-PLANE-INTEGRATION.md)

**Validate.** An observation or durable usage event retains its authorized attribution identity and ownership version through buffering and replay. A workspace change cannot retroactively relabel already captured usage merely because a name/project lookup now returns a new owner. Define the effective boundary for storage accrual and concurrent in-flight requests before checking conservation.

**Why valuable.** This is a cross-domain correctness case: security can transfer ownership correctly while billing assigns old work to the new workspace.

**Starting model and boundary.** One project, two workspace owners, a policy transition, one in-flight request, a buffered read batch, and storage time crossing the boundary. Check the in-repository identity handling; the full external transfer saga remains TLA-043.

**Required negative control.** Resolve attribution from current policy at drain time instead of the captured identity, or accept an owner change without a version advance.

**Related work:** TLA-031, 035–037, 043; KANI-073–074, 085–086, 090, 092.

<a id="tla-039"></a>
### TLA-039 — Fleet desired state, observations, and external-action outbox

**Priority:** P1 · **Requirement anchors:** T11–T12, fleet controller requirements  
**Source owners:** [`src/fleet.rs`](src/fleet.rs); [`src/fleet/repository.rs`](src/fleet/repository.rs); [`src/fleet/outbox.rs`](src/fleet/outbox.rs); [`src/fleet/planning.rs`](src/fleet/planning.rs); [`src/ownership.rs`](src/ownership.rs)

**Validate.** Controller intent, external action, observed reality, and action acknowledgment remain distinct. Concurrent controllers and lost external replies do not create conflicting authority or lose desired work. Only trusted eligible members/URLs become routing targets. Membership and override observations are published as one coherent local view.

**Why valuable.** External provisioning and placement cannot share the registry's transaction. An explicit outbox protocol prevents confusing “requested” with “exists and may serve.”

**Starting model and boundary.** Two controllers, two members, one desired placement, one external action with a lost reply, and a stale override. Use an explicit external idempotency/inspection contract; do not assume exactly-once cloud APIs.

**Required negative control.** Mark an action complete before its effect is known, route to an untrusted URL, or publish an active-set/override pair from different observations.

**Related work:** TLA-011, 028, 040, 043; KANI-062, 070, 095.

<a id="tla-040"></a>
### TLA-040 — Autoscaler decisions, unsplittable keys, and bounded controller work

**Priority:** P2 · **Requirement anchors:** T6–T8, H5–H6, R2, R8  
**Source owners:** [`src/scaler3.rs`](src/scaler3.rs); [`src/scaler3/controller.rs`](src/scaler3/controller.rs); [`src/sketch.rs`](src/sketch.rs); [`src/segmap.rs`](src/segmap.rs); [`AUTOSCALING-DESIGN.md`](AUTOSCALING-DESIGN.md)

**Validate.** A decision stays bound to the observed incarnation and eligible topology. A dominant unsplittable key does not generate useless children under the adopted policy. Cooldowns, deduplication, and bounded work preserve pending valid decisions without unbounded controller state. Split/merge safety relies on the topology model, not on heat-estimation correctness.

**Why valuable.** Prevents scaling loops, stale mutations, and wasted capacity. It does not prove that thresholds optimize cost or that an approximate sketch predicts future traffic.

**Starting model and boundary.** Two streams, one single hot key, one splittable distribution, one stale sketch, a budget of one action, and noisy heat around a threshold. Abstract measurement values explicitly; include a recreated stream.

**Required negative control.** Drop the epoch from a queued decision, split an unsplittable distribution repeatedly, or discard budget-deferred controller work.

**Related work:** TLA-009–010, 017, 030, 039; KANI-028–031, 061, 071–072, 095.

<a id="tla-041"></a>
### TLA-041 — Catalog pagination across invisible, vanished, and failed entries

**Priority:** P1 · **Requirement anchors:** C8, S7–S8  
**Source owners:** [`src/registry/catalog.rs`](src/registry/catalog.rs); [`src/product_cursor.rs`](src/product_cursor.rs); [`src/product_cursor/decode.rs`](src/product_cursor/decode.rs); [`src/application/creation/ttl.rs`](src/application/creation/ttl.rs)

**Validate.** A page walk does not skip eligible entries because preceding objects are tombstoned, expired, initializing, or vanish during fetch. A transient fetch failure is not treated as a permanently absent entry or false completion. Cursor scope remains project-bound. Specify the consistency contract under concurrent insertion/deletion; do not promise a fixed snapshot unless implemented.

**Why valuable.** Catalog completeness affects discoverability, reconciliation, and cleanup as well as user experience. Filtered pages are a common source of prematurely exhausted scans.

**Starting model and boundary.** Four lexically ordered entries, a page size of one, one initializing object, one vanished object, and one transient error. Run stable-catalog completion separately from concurrent-mutation admissibility.

**Required negative control.** Return end-of-catalog after an all-filtered page, advance past an unresolved fetch error, or accept another project's continuation.

**Related work:** TLA-008, 014, 017, 021; KANI-033–034, 060, 083.

<a id="tla-042"></a>
### TLA-042 — Peer compatibility and preservation of read/append evidence

**Priority:** P2 · **Requirement anchors:** D8, S6, S10, H2, H11  
**Source owners:** [`src/peer.rs`](src/peer.rs); [`src/application/read_wire.rs`](src/application/read_wire.rs); [`src/application/read_remote.rs`](src/application/read_remote.rs); [`src/application/consumer_remote.rs`](src/application/consumer_remote.rs); [`src/protocol_pin.rs`](src/protocol_pin.rs); [`src/dst/tests/read_peer_compatibility.rs`](src/dst/tests/read_peer_compatibility.rs)

**Validate.** Supported peer versions preserve identity, completeness, truncation, generation, and typed outcome meaning. Missing required evidence is an error, not a permissive default. A legacy route is accepted only where the compatibility contract allows it. A wire adapter cannot turn an ambiguous remote result into definitive success/refusal.

**Why valuable.** Correct local decisions are insufficient when a peer decoder drops the field proving which stream, span, or durability result it received.

**Starting model and boundary.** Two supported wire versions, one unsupported variant, truncated/malformed responses, and a retry during ownership movement. Keep protocol compatibility fixtures authoritative; the model abstracts byte parsing, covered separately by Kani/fuzzing.

**Required negative control.** Default a missing identity field, treat a truncated page as complete, or infer an internal outcome by parsing an error-display string.

**Related work:** TLA-005, 012, 018, 021, 023; KANI-055–060, 078, 083, 096.

<a id="tla-043"></a>
### TLA-043 — Control Plane project transfer and deletion sagas

**Priority:** D · **Requirement anchors:** proposed transfer/deletion contract; S1; R7  
**Source owners:** [`docs/CONTROL-PLANE-INTEGRATION.md`](docs/CONTROL-PLANE-INTEGRATION.md); [`src/auth/publication.rs`](src/auth/publication.rs); [`src/fleet.rs`](src/fleet.rs); [`src/billing.rs`](src/billing.rs)

**Validate.** Before adopting the external integration, specify the order of ownership-version change, credential invalidation, placement updates, routing, usage attribution, and deletion cleanup. Partial completion must preserve authority and durable debt. A deleted/recreated project must not inherit old capabilities or outbox work. Define recovery ownership across services.

**Why valuable.** This is a pre-implementation design check in the spirit of catching mistakes before code exists. The source contains integration guidance, not a proof that every external component implements the saga.

**Starting model and boundary.** A Control Plane, two cells, a credential issuer, and usage export with independent failures and delayed snapshots. Require explicit external API/idempotency contracts and an adopted design before making claims about live behavior.

**Required negative control.** Move routing before required authority invalidation, reuse a project identity without fencing old work, or discard transfer debt after one successful substep.

**Related work:** TLA-001, 031–039; KANI-024, 073–080, 092, 095.

<a id="tla-044"></a>
### TLA-044 — Persisted-format and rolling-upgrade compatibility transitions

**Priority:** D · **Requirement anchors:** future migration contract; D1; S7; H13  
**Source owners:** [`src/registry.rs`](src/registry.rs); [`src/shard.rs`](src/shard.rs); [`src/queue.rs`](src/queue.rs); [`src/crypto.rs`](src/crypto.rs); [`src/product_cursor/decode.rs`](src/product_cursor/decode.rs); [`src/protocol_pin.rs`](src/protocol_pin.rs); [`Cargo.toml`](Cargo.toml)

**Validate.** Before any persisted-format or mixed-version rollout, define which versions can read/write each state, when writers are fenced, whether rollback is legal, and how migration progress is durable. Old readers/writers must fail safely on unsupported data rather than fabricate empty state. GC must not remove the only rollback/recovery representation prematurely.

**Why valuable.** A locally verified new implementation can still destroy availability or data when paired with an old writer or a rollback. This model is triggered by a real transition, not a proposal to maintain unnecessary legacy modes.

**Starting model and boundary.** Two binary versions, two persisted formats, one partial migration, an old writer, and a rollback attempt. Include relevant SlateDB/object-store contract changes explicitly rather than assuming old receipts remain valid.

**Required negative control.** Enable new writes before old writers are fenced, silently decode unknown state as default, or delete the old representation before the adopted rollback boundary.

**Related work:** TLA-011, 019, 042; KANI-001–004, 007, 016, 032–033, 048–060, 084.


## 5. Kani proof catalog

Except the proofs [§0](#0-implementation-record) lists, all proof families are **planned**. Each must call the actual production owner after any reviewed extraction; each needs explicit semantic assertions, meaningful reachability checks, and at least one relevant negative control. The build-route labels below are feasibility estimates, not completed compiler tests.

For **full-width scalar** proofs, the stated Rust types remain symbolic across their complete admitted domain. For **bounded collection** proofs, bound length/depth, not the meaningful integer bits. Invalid-input harnesses must complement valid-input round trips. Avoid solving an enormous cryptographic primitive merely to validate a short envelope: verify preimage/context plumbing and structural acceptance separately, with conditional authentication assumptions labeled, while keeping real cryptographic integration tests.

**First regression seed:** in the inspected `src/offsets.rs`, `encode_ep` constructs a `u128` containing a 32-bit epoch in the top bits and then shifts that `u128` left by two. The top epoch bits are discarded. In particular, the source arithmetic makes epoch `0` and epoch `1 << 30` collide for the same ordinary offset. This is a source-inspection seed for KANI-001/003, not a reported Kani result or a demonstrated production incident. The supported epoch domain and any wire-compatible fix must be reviewed; do not hide the case with a harness-only assumption. `Some(u64::MAX)` in the `seq + 1` representation is a separate domain/exhaustion question for KANI-002. **Outcome (first spike):** confirmed on the real code and fixed; `Some(u64::MAX)` is no longer representable. See §0 and `verification/regressions/KANI-001`.

| IDs | Main area |
|---|---|
| [KANI-001–005](#kani-001) | Offsets and span positions |
| [KANI-006–015](#kani-006) | Postings codecs, validation, and planning |
| [KANI-016–023](#kani-016) | Frames, history reads, and structural admission |
| [KANI-024–035](#kani-024) | Identity, routing, topology, and descriptors |
| [KANI-036–047](#kani-036) | Producer/seal/transaction/maintenance decisions |
| [KANI-048–060](#kani-048) | Consumers, leases, and cursors |
| [KANI-061–072](#kani-061) | Admission, resources, lifecycle, and scaling |
| [KANI-073–084](#kani-073) | Authorization, peer contracts, and configuration |
| [KANI-085–096](#kani-085) | Billing, fleet planning, and typed outcomes |

<a id="kani-001"></a>
### KANI-001 — Epoch-aware offset round trip

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/offsets.rs`](src/offsets.rs)

**Status:** pass-with-recorded-scope (first spike; see [§0](#0-implementation-record)), over epochs below 2^30 since the merge took slate's `encode`/`parse`. The full-width finding (ordinals 2^30 apart collided) is open under slate's review item 88 wire decision, not fixed; multibyte and non-canonical readings are pinned as lax.

**Validate.** `parse_ep(encode_ep(epoch, position))` returns the admitted epoch and position without losing high bits; epoch-zero encoding remains compatible with the raw codec. Specify START separately.

**Why valuable.** Catches silent identity collisions that never panic and that small test epochs miss.

**Input scope and implementation boundary.** Full `u32` epochs and admitted `u64` sequence values; fixed 26-character encoding. Resolve the actual epoch range and representation contract, including the source-inspection collision above, before recording a pass.

**Required negative control.** Use the existing high-epoch collision as a failing seed; after fixing it, deliberately truncate the high epoch bits.

**Related work:** TLA-012, 021, 044.

<a id="kani-002"></a>
### KANI-002 — Offset successor, START sentinel, and exhaustion

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/offsets.rs`](src/offsets.rs); [`src/application/read_range.rs`](src/application/read_range.rs)

**Status:** pass-with-recorded-scope (first spike; see [§0](#0-implementation-record)).

**Validate.** START maps to scan index zero; ordinary positions map to the correct strictly-after index; boundary arithmetic cannot wrap into START or a lower valid position. Constructors and callers enforce the supported upper endpoint.

**Why valuable.** Prevents skipped records, replay from the beginning, and debug/release divergence at exhaustion.

**Input scope and implementation boundary.** Full `u64`, including MAX and MAX−1; separate valid position proofs from invalid/exhausted admission. `Some(MAX)` must be rejected, made unrepresentable, or assigned an explicit compatible meaning.

**Required negative control.** Replace checked exhaustion with wrapping `+1`, or confuse START with offset zero.

**Related work:** TLA-007, 018, 021.

<a id="kani-003"></a>
### KANI-003 — Offset encoding injectivity and ordering

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/offsets.rs`](src/offsets.rs)

**Status:** pass-with-recorded-scope (first spike; see [§0](#0-implementation-record)).

**Validate.** Distinct admitted epoch/position tuples have distinct canonical encodings. Lexical order agrees with the intended tuple order for supported positions; START ordering follows the explicit external contract, not an assumed string order for `-1`.

**Why valuable.** Protects seek and ordering decisions from an encoder that round-trips only a restricted sample.

**Input scope and implementation boundary.** Two symbolic tuples, full-width admitted integers, fixed output length. Ordering of numeric segment ordinals does not itself prove complete topology lineage order.

**Required negative control.** Discard an epoch bit or swap high/low sequence bytes; require an injectivity or order failure.

**Related work:** TLA-012, 021, 044.

<a id="kani-004"></a>
### KANI-004 — Offset parser alphabet, aliases, and malformed encodings

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/offsets.rs`](src/offsets.rs)

**Validate.** Accepted ASCII aliases normalize as documented; invalid length/characters and unsupported epochs fail correctly. Specify handling of nonzero padding/in-block bits and noncanonical aliases instead of silently assuming every accepted token is canonical.

**Why valuable.** A forgiving parser can accidentally accept Unicode truncation aliases or erase meaningful unsupported fields.

**Input scope and implementation boundary.** Bound strings around the fixed token length; also verify `decode_char` over the full `char` domain if tractable. Separate byte length from character count and exercise non-ASCII code points.

**Required negative control.** Permit a non-ASCII character whose truncated byte matches the alphabet, or remove a required field/padding check once that policy is adopted.

**Related work:** TLA-021, 042, 044.

<a id="kani-005"></a>
### KANI-005 — Logical-to-segment span positioning

**Status:** pass-with-recorded-scope (implemented 2026-09-25; manifest entry KANI-005, 1 baseline and 2 negative controls: an inclusive sealed end, and an omitted start subtraction). The constructor contract is assumed, not proved (ASM-LINEAGE-CONTRACT): `build` refuses an empty lineage and a live span before the tail, but its `logical += c` is an unchecked add. No finding in `locate_in_spans` itself.

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/sse/source/spans.rs`](src/sse/source/spans.rs); [`src/application/read_range.rs`](src/application/read_range.rs)

**Validate.** `locate_in_spans` maps one-past boundaries to the next segment at local zero and implements the documented last-span fallback. Valid span construction guarantees no underflow/overflow; empty or malformed lineage cannot reach an unchecked assumption.

**Why valuable.** Prevents duplicate or missing records when an SSE/read cursor crosses a sealed predecessor.

**Input scope and implementation boundary.** Two to four valid spans with full-width starts/caps and symbolic logical positions. Prove or separately validate the nonempty, monotonic, non-overflowing constructor contract; do not only assume it in every caller.

**Required negative control.** Change a boundary comparison, omit a start subtraction guard, or admit an overflowing start-plus-cap.

**Related work:** TLA-012, 015, 021, 027.

<a id="kani-006"></a>
### KANI-006 — Varint codec and consumed-input boundaries

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/postings.rs`](src/postings.rs)

**Validate.** `get_varint` and `put_varint` round-trip all `u64` values; malformed/truncated/overlong input follows the adopted rejection policy; consumed input never passes available bytes. No shift or accumulation drops significant bits.

**Why valuable.** A compact decoder is small enough for exhaustive symbolic boundary checking and sits underneath postings integrity.

**Input scope and implementation boundary.** Full `u64` encode inputs and symbolic byte arrays through the maximum representation plus overlong cases. Check exact continuation/termination semantics rather than requiring canonical rejection not promised by the format.

**Required negative control.** Allow an overflowing final byte or remove the termination/length check.

**Related work:** TLA-016, 018, 044.

<a id="kani-007"></a>
### KANI-007 — Postings page count admission before allocation

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/postings.rs`](src/postings.rs)

**Validate.** `admitted_run_count` and `decode_page` cannot trust a claimed count larger than the encoded payload can support. Truncated headers, zero/invalid counts, and unsupported encodings are rejected according to the format before proportional allocation.

**Why valuable.** Protects both correctness and memory admission against hostile or corrupt storage bytes.

**Input scope and implementation boundary.** Full-width count fields with short bounded payloads; a second valid-page harness with a small run count. Record the architecture-specific `usize` conversion scope.

**Required negative control.** Allocate from the declared count before checking encoded capacity, or accept a header count inconsistent with the actual rows.

**Related work:** TLA-016, 018, 029, 044.

<a id="kani-008"></a>
### KANI-008 — Relative-to-absolute postings arithmetic

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/postings.rs`](src/postings.rs)

**Validate.** `decode_page_abs` computes the correct absolute offsets; adding page bases, deltas, and counts cannot wrap. A malformed relative run cannot become a plausible low-offset run.

**Why valuable.** Stops integer overflow from turning corruption into silently misdirected reads.

**Input scope and implementation boundary.** One to four runs with full `u64` bases/deltas and production count types. Include first offsets near MAX and an independent wide-arithmetic oracle.

**Required negative control.** Replace checked addition with wrapping addition or apply the page base twice.

**Related work:** TLA-016, 018; TLA-021.

<a id="kani-009"></a>
### KANI-009 — Validated postings invariants

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/postings/validated.rs`](src/postings/validated.rs)

**Validate.** `ValidatedRuns::new` admits exactly the specified sorted, non-overlapping, nonempty, non-overflowing run representation. Every public operation on a valid owner preserves those invariants.

**Why valuable.** Makes the proof-bearing type's promise explicit so downstream hot paths can rely on it without revalidation.

**Input scope and implementation boundary.** Zero to four symbolic runs with full offsets/counts; separate malformed overlap, adjacency, zero count, and endpoint overflow. Construct invalid inputs through the real constructor, not by exposing private fields.

**Required negative control.** Permit zero-length or overlapping runs, or skip overflow checking of the exclusive endpoint.

**Related work:** TLA-016, 018, 020.

<a id="kani-010"></a>
### KANI-010 — Postings extension and clipping across an absorption cut

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/postings/validated.rs`](src/postings/validated.rs)

**Validate.** `extend_after` combines old and fresh coverage without overlap or lost eligible suffix; `clipped_to` preserves the exact permitted prefix and representation invariants. Cuts inside a run, at its ends, and between runs are handled intentionally.

**Why valuable.** Protects cache refresh when an absorption boundary splits a stored run.

**Input scope and implementation boundary.** Two short validated run lists plus a full-width cut. Compare represented offset sets only within a bounded run count while preserving real integer arithmetic.

**Required negative control.** Keep both copies of a straddling run, or drop the retained part when clipping at an interior cut.

**Related work:** TLA-016, 018, 020.

<a id="kani-011"></a>
### KANI-011 — RunWindow exact interval intersection

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/postings/validated.rs`](src/postings/validated.rs)

**Validate.** `RunWindow::iter` returns exactly the represented intersection with the requested range, with no interval outside the window and no omitted intersection. Empty/reversed windows follow their specified behavior.

**Why valuable.** A small interval bug here changes read completeness across many higher-level APIs.

**Input scope and implementation boundary.** One to four validated runs and full-width `from/upto`; use boundary witnesses for adjacent endpoints and a cut inside a run. This does not prove that the owner is released before async scans; retain the existing ownership/lint checks.

**Required negative control.** Change exclusive to inclusive end handling or fail to clip the first intersecting run.

**Related work:** TLA-018, 020–021.

<a id="kani-012"></a>
### KANI-012 — Stored postings key/header/identity agreement

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/postings.rs`](src/postings.rs)

**Validate.** `decode_stored_page` accepts only the required key shape and agreement between stored-key identity, bucket/first offset, and page contents. Corrupt identity cannot silently become an empty page.

**Why valuable.** Prevents validly encoded data from being interpreted under the wrong storage position or routing context.

**Input scope and implementation boundary.** Bounded symbolic key/header bytes with full numeric fields. Preserve explicitly supported format variants; do not impose clean-cutover rules from unrelated routes.

**Required negative control.** Skip a key/header comparison or accept a truncated numeric suffix.

**Related work:** TLA-016, 018, 042.

<a id="kani-013"></a>
### KANI-013 — Postings builder equivalence to canonical input frames

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/postings.rs`](src/postings.rs)

**Validate.** `note_frame`/`finish` represent exactly the input frame offsets for each routing-key hash, coalesce only eligible consecutive runs, and split buckets/pages correctly. Accumulated byte estimates cannot silently wrap.

**Why valuable.** Decoder proofs cannot catch a writer that emits a well-formed but incomplete index.

**Input scope and implementation boundary.** Up to four keys and eight frames with full-width offsets/lengths, including bucket boundaries. Hash identities are symbolic supplied values; hash collision resistance is outside this proof.

**Required negative control.** Merge across a key/bucket boundary or omit the final open run during finish.

**Related work:** TLA-016, 018.

<a id="kani-014"></a>
### KANI-014 — Canonical span planner coverage and forward progress

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/postings.rs`](src/postings.rs)

**Validate.** `plan_spans`/`plan_spans_iter` cover the selected admissible prefix of postings without skipping a required run; a nonempty admissible workload makes progress, including the explicitly permitted oversized first record/run case. Truncation is reported consistently.

**Why valuable.** Prevents an optimization from wedging a cursor or falsely presenting partial work as complete.

**Input scope and implementation boundary.** One to four runs, full-width offsets/estimates, symbolic small span/work limits. Keep the first-item progress exception in the oracle; do not force every request into its soft byte target.

**Required negative control.** Reject all work when the first item exceeds the soft limit, or advance the continuation past an unplanned run.

**Related work:** TLA-018, 021.

<a id="kani-015"></a>
### KANI-015 — Canonical span planner hard limits and cost arithmetic

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/postings.rs`](src/postings.rs)

**Validate.** Hard span-count/work limits and checked/saturating estimate arithmetic are respected under the adopted planner contract. A soft-budget exception cannot permit unbounded additional work after the first item. Merging spans preserves endpoint and scan-byte calculations.

**Why valuable.** Connects the optimization to a falsifiable work bound without claiming that estimated bytes equal actual provider GETs or latency.

**Input scope and implementation boundary.** Bounded run lists with extreme byte estimates and gaps; treat estimation accuracy separately from arithmetic safety and hard resource admission.

**Required negative control.** Overflow an accumulated estimate so more work appears cheap, or apply the first-record exception to every run.

**Related work:** TLA-017–018, 029.

<a id="kani-016"></a>
### KANI-016 — Frame structural decoder and supported version layouts

**Priority:** P0 · **Build route:** Spike  
**Source owners:** [`src/crypto.rs`](src/crypto.rs)

**Validate.** `decode_frame` accesses only available bytes; accepted version/header/nonce/routing-key/ciphertext fields describe valid in-buffer regions. Invalid UTF-8, truncation, and length arithmetic fail safely. Define trailing-byte policy at the actual admission boundary.

**Why valuable.** A structural parser is a high-leverage boundary even though successful parsing is not authentication.

**Input scope and implementation boundary.** Symbolic fixed headers and bounded payloads across supported legacy/current compressed/uncompressed versions. Compile the actual parser without pretending the crypto dependency graph is already Kani-compatible.

**Required negative control.** Remove a bounds check or use the wrong version's nonce/header length.

**Related work:** TLA-018, 042, 044.

<a id="kani-017"></a>
### KANI-017 — Stored record key and frame offset agreement

**Priority:** P0 · **Build route:** Spike  
**Source owners:** [`src/shard/record.rs`](src/shard/record.rs)

**Validate.** `decode_row`/`decode_at` reject wrong prefix, malformed offset width, or disagreement between the key/expected offset and frame metadata. A valid frame from another row cannot pass solely because its bytes decode.

**Why valuable.** Detects misplaced/corrupt records before they become plausible application results.

**Input scope and implementation boundary.** Bounded key/frame representations using the real frame decoder; full-width embedded offset. Keep the requested segment/incarnation context explicit.

**Required negative control.** Skip the expected-offset comparison or validate only a short key prefix.

**Related work:** TLA-016, 018, 042.

<a id="kani-018"></a>
### KANI-018 — CheckedFrame construction and retained view consistency

**Priority:** P1 · **Build route:** Spike  
**Source owners:** [`src/shard/record/checked.rs`](src/shard/record/checked.rs); [`src/crypto.rs`](src/crypto.rs)

**Validate.** Every accepted `CheckedFrame` view reports the same metadata and ciphertext bounds as the admitted backing bytes. Repeated views and permitted ownership conversions do not bypass structural checks or invent authentication.

**Why valuable.** Protects the optimized checked-once path from diverging from the canonical decoder.

**Input scope and implementation boundary.** Small valid/malformed frames and sequential conversions; establish `Bytes`/dependency support first. Retain real Miri/ownership tests for aliasing behavior outside Kani's supported semantics.

**Required negative control.** Cache metadata from a different buffer or construct a checked frame without the required admission.

**Related work:** TLA-018, 020, 027.

<a id="kani-019"></a>
### KANI-019 — Frame encryption-context and associated-data construction

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/crypto.rs`](src/crypto.rs); [`src/crypto/decrypt.rs`](src/crypto/decrypt.rs)

**Validate.** The invocation's key/segment/epoch/offset/version/header context is assembled according to the adopted frame format. Authentication covers all intended fields. Legacy and misuse-resistant current-frame paths retain their distinct nonce/context policies.

**Why valuable.** Checks implementation plumbing that can invalidate a good cipher; it does not prove AES, AEAD security, or nonce uniqueness for an unsupported rollback model.

**Input scope and implementation boundary.** Extract only canonical preimage/header assembly used by production if needed. Symbolic context fields and fixed small payload; idealize crypto operations only in a separately labeled conditional harness.

**Required negative control.** Omit a bound header field or substitute another segment's key-derivation input.

**Related work:** TLA-015, 018, 032, 044.

<a id="kani-020"></a>
### KANI-020 — Authenticated plaintext limit and bounded append decoding

**Priority:** P1 · **Build route:** Spike  
**Source owners:** [`src/crypto/decrypt.rs`](src/crypto/decrypt.rs); [`src/crypto.rs`](src/crypto.rs); [`src/application/read_decode.rs`](src/application/read_decode.rs)

**Validate.** The real admission path distinguishes authentication failure from an authenticated record exceeding the remaining limit. It never returns a partial unauthenticated record as valid data, and returned plaintext obeys the permanent record ceiling plus the page owner's explicit first-item policy.

**Why valuable.** Protects read safety and budgeting against an otherwise valid oversized payload.

**Input scope and implementation boundary.** Small symbolic authenticated/decrypted outcomes through a reviewed dependency boundary; real crypto/decompression integration tests remain mandatory. Do not report a stubbed decryptor as crypto coverage.

**Required negative control.** Publish plaintext before successful authentication, or treat an over-limit result as an empty complete record.

**Related work:** TLA-018, 029; KANI-063.

<a id="kani-021"></a>
### KANI-021 — Compression/decompression length and limit calculations

**Priority:** P2 · **Build route:** Extract  
**Source owners:** [`src/crypto.rs`](src/crypto.rs); [`src/crypto/decrypt.rs`](src/crypto/decrypt.rs)

**Validate.** Calculating encoded ceilings, header allowances, and the bounded `limit + 1` probe cannot overflow or bypass the intended decompression stop. Unsupported frame versions and length inconsistencies do not become permissive defaults.

**Why valuable.** Covers the Rust arithmetic surrounding a compression bomb defense without pretending to verify the native compression library.

**Input scope and implementation boundary.** Full `usize`/length scalars in the admitted platform/configuration; separate pure limit computation from FFI. Keep actual compressed-corpus/fuzz tests and process memory measurements.

**Required negative control.** Overflow `limit + 1` or subtract header bytes before validating their presence.

**Related work:** TLA-018, 029, 044.

<a id="kani-022"></a>
### KANI-022 — Durable tail-ring coverage predicates

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/shard/record.rs`](src/shard/record.rs); [`src/shard/tail_ring.rs`](src/shard/tail_ring.rs)

**Validate.** A ring fast path is used as durable evidence only when its stored interval, requested range, identity, and captured durable frontier jointly establish coverage. Wrapped/empty intervals cannot masquerade as complete coverage.

**Why valuable.** Prevents a fast in-memory path from returning state under a stronger durability contract than the stored frontier supports.

**Input scope and implementation boundary.** Full-width endpoints and small ring metadata; isolate the actual pure predicate while leaving concurrent frontier publication to Loom/TLA+.

**Required negative control.** Use the applied frontier in place of the durable frontier or accept only a partially covered request.

**Related work:** TLA-005, 018, 027.

<a id="kani-023"></a>
### KANI-023 — Canonical history filtering and truncation bookkeeping

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/history/canonical_span.rs`](src/history/canonical_span.rs)

**Validate.** The scan accumulator advances its consumed-work cursor correctly even over nonmatching keys; it appends only exact-key hits and reports truncation before skipping an unconsumed frame. The first-frame progress exception is finite and intentional.

**Why valuable.** Sparse-key scans can otherwise loop forever or skip a later matching record while appearing complete.

**Input scope and implementation boundary.** A bounded sequence of matching/nonmatching checked frames, symbolic lengths, and a byte limit; call the extracted production accumulator, not a fake async storage scan.

**Required negative control.** Advance `last` past a frame refused for budget, or fail to advance over consumed nonmatching frames.

**Related work:** TLA-018, 021.

<a id="kani-024"></a>
### KANI-024 — Identity-component encoding and domain separation before hashing

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/tenant.rs`](src/tenant.rs)

**Validate.** `append_component`/`encode_hash_input` unambiguously encode their admitted component sequences and domain tags. Boundary ambiguity such as `[ab,c]` versus `[a,bc]` is impossible in the preimage; different domains remain distinct.

**Why valuable.** A small encoding defect can merge unrelated tenant or operation identities before any cryptography is involved.

**Input scope and implementation boundary.** Two to four short symbolic components, including empty values and delimiter-like bytes, with full encoded lengths. Prove injectivity of the encoding, not collision-freedom of a finite hash.

**Required negative control.** Remove a length prefix or the domain tag.

**Related work:** TLA-007, 015, 032, 034.

<a id="kani-025"></a>
### KANI-025 — Project, workspace, cell, and canonical stream-name validation

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/tenant.rs`](src/tenant.rs); [`src/application/names.rs`](src/application/names.rs)

**Validate.** Constructors admit exactly the adopted bounded syntax and reject forbidden separators, traversal-like components, empty/oversized identifiers, and malformed encodings. Display/serialization round trips preserve the validated identity.

**Why valuable.** Prevents path confusion and ensures higher-level proofs may rely on private validated identities.

**Input scope and implementation boundary.** Short symbolic UTF-8/byte cases plus separate length-boundary arithmetic. Review normalization versus rejection; do not normalize two distinct valid names into one without an adopted rule.

**Required negative control.** Allow a forbidden separator/component or truncate an overlong identity into a valid one.

**Related work:** TLA-001, 032, 034, 041.

<a id="kani-026"></a>
### KANI-026 — Stream grant prefixes and normalization without privilege expansion

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/tenant.rs`](src/tenant.rs); [`src/auth.rs`](src/auth.rs)

**Validate.** Prefix normalization is idempotent and preserves the intended permission set. `matches`, prefix-set reduction, and grant intersection cannot grant a name that was forbidden by the original operands. Component-boundary semantics remain exact.

**Why valuable.** Authorization often fails at prefix edges rather than token cryptography.

**Input scope and implementation boundary.** Two to four bounded grants and short names, including exact match, descendant, sibling with a shared textual prefix, and malformed components. Keep `StreamGrant` variants distinct.

**Required negative control.** Use raw string-prefix matching where a component boundary is required, or use union instead of intersection.

**Related work:** TLA-031–034.

<a id="kani-027"></a>
### KANI-027 — Scope bitsets and least-privilege intersection

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/tenant.rs`](src/tenant.rs); [`src/auth.rs`](src/auth.rs)

**Validate.** Parsing recognized scopes sets exactly their bits; unknown scopes never grant authority. Membership, iteration, and intersection agree, with no overlap between distinct scope constants or overflow of their bit positions.

**Why valuable.** A compact, full-domain proof can protect every route's capability check.

**Input scope and implementation boundary.** Full `u64` bitsets plus bounded scope-name inputs; distinguish counting unknown scopes from accepting them. Enumerate the real scope variants rather than a copied test enum.

**Required negative control.** Shift one scope to another's bit, or replace intersection with a widening operation.

**Related work:** TLA-032–034.

<a id="kani-028"></a>
### KANI-028 — Segment partition validation and routing uniqueness

**Status:** pass-with-recorded-scope (implemented 2026-09-26; manifest entry KANI-028, 4 baselines and 3 negative controls: a tiling rule that accepts a one-point gap or overlap, a `contains` without the terminal-range convention, and a `route` without its sealed-cover fallback). The proof targets the coverage rule, not the whole `validate`: Kani could not finish `validate` over even two symbolic segments in 45 minutes, because the checker loses track of the (empty) lineage vectors held inside the map's vector, so the lineage loops and the length of the sort input stay symbolic, and a sort of symbolic length explores every sort strategy. Production changes that made the proof possible, none changing behavior: `validate` scans for duplicate identities instead of using hashed sets (whose seed comes from the OS random source, which Kani cannot model); it reports a typed `TopologyError` whose `Display` keeps every message, instead of formatting a `String` where it finds the fault; the tiling rule is one function, `tiles_keyspace`, shared with `check_partition`; and `Registry::resolve_segment`'s choice of segment moved into `SegmentMap::route`. With a validated map's terminal segments tiling the key space, the `unreachable!` after `resolve_segment`'s choice cannot be reached. Duplicate identities and seal metadata stay with the unit tests, lineage and pending transitions with KANI-031. An open question for KANI-031: when no live segment holds a point, `route` prefers the sealed cover with the latest `created_ms`, a wall-clock value that `validate` does not order along lineage, so after clock skew between scalers it can pick an ancestor over its sealed descendant. Appends answer a retryable 503 for any sealed route; keyed reads have not been checked. No finding in the coverage rule itself.

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/segmap.rs`](src/segmap.rs)

**Validate.** A validated live segment map covers the entire adopted key domain exactly once, including the representation of the maximum endpoint. `contains` and route selection agree with that partition; gaps, overlaps, and duplicate identities are rejected.

**Why valuable.** The partition is a foundational assumption of split, merge, and per-key reads.

**Input scope and implementation boundary.** Two to four segments with full-width endpoints and a symbolic routing point. Include zero, MAX, adjacent ranges, and the format's terminal-range convention.

**Required negative control.** Accept a one-point gap/overlap or mishandle the range containing the maximum key.

**Related work:** TLA-004, 009–010, 012, 025.

<a id="kani-029"></a>
### KANI-029 — Split transformation preservation

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/segmap.rs`](src/segmap.rs)

**Validate.** An eligible split preserves all unrelated segments, exactly partitions the parent range, establishes valid successor/predecessor metadata, and handles ID/map-version exhaustion without collision. Ineligible split points or states leave the map unchanged or return the specified error.

**Why valuable.** Proves the pure transformation independently from the durable multi-step topology protocol.

**Input scope and implementation boundary.** A small valid map, full-width split point and production identifier/version types; validate the transformed map through its real validator.

**Required negative control.** Make both children include the split point, mutate an unrelated segment, or wrap a newly allocated ID.

**Related work:** TLA-009; KANI-028, 031.

<a id="kani-030"></a>
### KANI-030 — Merge transformation preservation

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/segmap.rs`](src/segmap.rs)

**Validate.** Only the specified adjacent eligible parents merge. The successor covers their union, retains both lineage relationships, and preserves unrelated ranges. Invalid parent selection or exhausted identity space cannot produce a partially mutated valid-looking map.

**Why valuable.** Catches purely local topology mistakes before expensive distributed tests.

**Input scope and implementation boundary.** Two eligible parents plus one unrelated segment, full endpoint/ID values, reversed/nonadjacent parent selections, and a pending transition.

**Required negative control.** Ignore adjacency, omit a parent, or alter unrelated segment metadata.

**Related work:** TLA-004, 010, 025.

<a id="kani-031"></a>
### KANI-031 — Topology lineage and pending-transition validation

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/segmap.rs`](src/segmap.rs)

**Validate.** Lineage directions, historical predecessor references, live/closed status, and pending split/merge shape agree. Cycles, impossible parent/child references, wrong boundary relationships, and invalid phase metadata cannot enter the validated map.

**Why valuable.** A valid partition alone can still have broken traversal/recovery lineage.

**Input scope and implementation boundary.** A bounded graph of up to four segments with symbolic IDs and endpoint metadata. Validate exactly the existing historical-lineage rules; do not forbid valid retained predecessors.

**Required negative control.** Remove a direction/reference check or admit a pending merge whose parents do not match the declared transition.

**Related work:** TLA-004, 009–010, 012, 025.

<a id="kani-032"></a>
### KANI-032 — Persisted descriptor admission and lifecycle consistency

**Priority:** P0 · **Build route:** Extract  
**Source owners:** [`src/registry.rs`](src/registry.rs)

**Validate.** `validate_descriptor` and `decode_desc` reject malformed identities/epochs and incompatible lifecycle combinations. A descriptor admitted as a proof-bearing `StreamDesc` satisfies the assumptions used by creation, seal, topology, fork, and routing decisions.

**Why valuable.** Protects all downstream state machines from corrupted or incompatible persisted state.

**Input scope and implementation boundary.** A bounded typed persisted descriptor plus focused byte/serde cases. Keep the real validation owner and enumerate allowed legacy/layout variants. Pure validation may need a narrow extraction from storage/error dependencies.

**Required negative control.** Admit simultaneous unsupported initialization/seal/topology states or substitute a default epoch for a malformed one.

**Related work:** TLA-001–004, 008–010, 013–015, 044.

<a id="kani-033"></a>
### KANI-033 — Storage key codecs and namespace separation

**Priority:** P0 · **Build route:** Extract  
**Source owners:** [`src/shard.rs`](src/shard.rs); [`src/history.rs`](src/history.rs); [`src/registry.rs`](src/registry.rs); [`src/postings.rs`](src/postings.rs)

**Validate.** Each supported tail/record/dirty/history/descriptor key decodes to the intended identity and numeric suffix. Tags and length-delimited components cannot overlap across key families; range-prefix construction contains exactly the intended keyspace.

**Why valuable.** Wrong-key reads/deletes can defeat every higher-level invariant while individual values remain well formed.

**Input scope and implementation boundary.** One harness per actual key family using bounded names and full numeric suffixes. Prove encoding separation before hashing; treat hash collision resistance as a dependency, not a theorem of these encoders.

**Required negative control.** Drop a tag or component length, or compute a prefix range that includes another tenant/segment.

**Related work:** TLA-014, 019, 034, 041, 044.

<a id="kani-034"></a>
### KANI-034 — Catalog key parsing and continuation-range construction

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/registry/catalog.rs`](src/registry/catalog.rs); [`src/registry.rs`](src/registry.rs); [`src/product_cursor/decode.rs`](src/product_cursor/decode.rs)

**Validate.** A stored catalog key maps only to the correct cell/project/canonical name. Resume ranges begin after the intended last item and cannot escape the project prefix. Malformed keys fail rather than repositioning the scan.

**Why valuable.** Supports both listing completeness and safe reconciliation/deletion discovery.

**Input scope and implementation boundary.** Short symbolic key strings with delimiter/escaping cases and an optional continuation; check the actual storage ordering representation rather than an assumed locale order.

**Required negative control.** Use an inclusive continuation that repeats forever, or let a crafted project/name component escape the prefix.

**Related work:** TLA-017, 019, 041.

<a id="kani-035"></a>
### KANI-035 — Stream/segment/child route identity preimages

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/tenant.rs`](src/tenant.rs); [`src/registry.rs`](src/registry.rs); [`src/crypto.rs`](src/crypto.rs)

**Validate.** Production preimages include the correct project, canonical name, stream epoch, segment, and child-routing context for each identity domain. Repeating the same tuple is stable; changes in required context change the preimage.

**Why valuable.** Checks that physical routes, persisted identities, and logical names are not accidentally substituted for one another.

**Input scope and implementation boundary.** Short identity components and full segment IDs. Prove distinct preimages, not unique finite hash outputs or physical-machine placement; routing may intentionally collide at a coarser partition.

**Required negative control.** Omit project/epoch from a storage identity or reuse the stream domain for a child route.

**Related work:** TLA-009, 011–015, 034.

<a id="kani-036"></a>
### KANI-036 — Producer stale-epoch and new-epoch admission

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/shard/commit_plan.rs`](src/shard/commit_plan.rs)

**Status:** pass-with-recorded-scope (first spike; see [§0](#0-implementation-record)).

**Validate.** `decide_producer` rejects lower producer epochs and requires the specified initial sequence for a new epoch. No-current-state behavior follows the real protocol. These checks retain their intended precedence over later collection-state checks.

**Why valuable.** A small decision function controls acceptance of every producer-coordinated append.

**Input scope and implementation boundary.** Full `u64` current/request epochs and sequences, optional current state, and real tail/sealed variants. Bound producer strings only as required to construct the actual request type.

**Required negative control.** Accept a lower epoch or allow a nonzero initial sequence in a new epoch.

**Related work:** TLA-003, 005, 007.

<a id="kani-037"></a>
### KANI-037 — Producer duplicate, conflict, and replay-result semantics

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/shard/commit_plan.rs`](src/shard/commit_plan.rs)

**Status:** pass-with-recorded-scope (first spike; see [§0](#0-implementation-record)).

**Validate.** An exact remembered sequence obeys its hash-conflict rule and returns the stored offset where available. Older duplicates and legacy unknown-hash/offset sentinels follow the documented fallback. Duplicate recognition precedes closure rejection where required and never returns `Accept`.

**Why valuable.** Protects retry correctness without inventing an unlimited original-response archive.

**Input scope and implementation boundary.** Full epoch/sequence/offset values; symbolic hashes including all-zero and absent request hashes; closed and open tails. Separate cases where missing legacy evidence intentionally limits conflict detection.

**Required negative control.** Check collection closure before duplicate replay, ignore a known same-sequence hash conflict, or allocate an offset for a duplicate.

**Related work:** TLA-003, 005, 007.

<a id="kani-038"></a>
### KANI-038 — Producer sequence gaps and numeric exhaustion

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/shard/commit_plan.rs`](src/shard/commit_plan.rs)

**Status:** pass-with-recorded-scope (first spike; see [§0](#0-implementation-record)).

**Validate.** A request beyond the permitted next sequence is classified as a gap with the correct expected value. `checked_add`/saturating diagnostic behavior remains consistent at MAX. A valid adjacent sequence is not rejected by an off-by-one rule.

**Why valuable.** Boundary errors can either admit missing sequence work or permanently strand a valid producer.

**Input scope and implementation boundary.** Full `u64` sequences including MAX−1/MAX and all decision variants; distinguish the immediate decision from whether later durable predecessor progress can change it.

**Required negative control.** Use wrapping successor arithmetic or change the gap comparison from strict to inclusive.

**Related work:** TLA-003, 005, 007.

<a id="kani-039"></a>
### KANI-039 — Seal generation authorization truth table

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/shard/commit_plan.rs`](src/shard/commit_plan.rs)

**Status:** pass-with-recorded-scope (first spike; see [§0](#0-implementation-record)).

**Validate.** `seal_authorized` rejects a supplied generation below the fence; exact/higher generations obey the contract; an untagged closing operation is refused after a nonzero fence, while untagged ordinary writes follow the separate admission contract.

**Why valuable.** This is one of the smallest and most consequential initial proofs.

**Input scope and implementation boundary.** Full `u64` fence/generation, `Option` presence, and closing flag. Cover every branch. Do not infer that this predicate alone authorizes a write through a sealing descriptor.

**Required negative control.** Permit equality incorrectly, invert the generation comparison, or allow an untagged close past an established fence.

**Related work:** TLA-002–004, 006.

<a id="kani-040"></a>
### KANI-040 — Seal claim decision matrix and exact-operation renewal

**Priority:** P0 · **Build route:** Extract  
**Source owners:** [`src/application/lifecycle/claims.rs`](src/application/lifecycle/claims.rs)

**Validate.** `decide_claim` distinguishes terminal ownership, exact renewal, allowed plain joins, conflicting final promises, abandoned claims, and pending topology. Its returned mutation/result pair agrees; a declined attempt carries no write or fabricated installation.

**Why valuable.** Checks the canonical decision matrix behind seal recovery rather than duplicating it in adapters.

**Input scope and implementation boundary.** Bounded valid descriptors and operation IDs; full timestamps/generation counters in dedicated subharnesses. Use the real validated descriptor constructor and separately resolve generation exhaustion.

**Required negative control.** Allow a plain seal to join owed final debt, renew the wrong operation, or return Installed with no corresponding write.

**Related work:** TLA-001–004.

<a id="kani-041"></a>
### KANI-041 — Generation allocation, newest reservation, and exact release guards

**Priority:** P0 · **Build route:** Extract  
**Source owners:** [`src/application/lifecycle.rs`](src/application/lifecycle.rs); [`src/application/lifecycle/claims.rs`](src/application/lifecycle/claims.rs)

**Validate.** Generation allocation cannot wrap or return a stale value. Reservation installation requires the newest eligible generation and the expected old claim. Release/mark predicates require the complete incarnation/operation/generation identity and cannot clear a renewed claim.

**Why valuable.** Turns the critical local guards of the takeover model into checked production decisions.

**Input scope and implementation boundary.** Extract small predicates only where doing so improves the real owner. Full `u64` generations plus a small valid claim state; verify exhaustion/refusal at the production boundary, not only below MAX.

**Required negative control.** Remove newest-reservation equality, omit generation from release, or replace checked allocation with wrapping increment.

**Related work:** TLA-001–003; KANI-040.

<a id="kani-042"></a>
### KANI-042 — Final-append error disposition

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/application/lifecycle/claims.rs`](src/application/lifecycle/claims.rs)

**Status:** pass-with-recorded-scope (first spike; see [§0](#0-implementation-record)).

**Validate.** `final_err_disposition` classifies every real `AppendErr` variant according to the shared debt-retention contract. Producer gap/epoch-start disagreement and ambiguous/transient outcomes retain debt; definitive failures release only via the separate exact-identity guard.

**Why valuable.** Prevents raw and product close behavior from drifting and stops a new enum variant from silently inheriting an unsafe policy.

**Input scope and implementation boundary.** One symbolic enum-discriminant harness with bounded string payloads, plus full numeric payloads where they matter. Review the default branch on enum growth; an exhaustive explicit policy is preferable if it preserves clarity.

**Required negative control.** Classify timeout or producer gap as definitive, or classify an impossible request as endlessly renewable without a contract justification.

**Related work:** TLA-003; KANI-041, 096.

<a id="kani-043"></a>
### KANI-043 — Semantic operation-identity construction

**Priority:** P0 · **Build route:** Extract  
**Source owners:** [`src/application/lifecycle/claims.rs`](src/application/lifecycle/claims.rs); [`src/application/creation.rs`](src/application/creation.rs); [`src/application/append/contract.rs`](src/application/append/contract.rs)

**Validate.** The canonical preimage includes every input affecting acceptance/persisted bytes: routing key, content identity, producer tuple, explicit sequence/timestamp where relevant, and format/domain. Equivalent retries serialize consistently; materially different coordination cannot share an ambiguous preimage.

**Why valuable.** An incomplete operation identity can let one rejected request release or complete another request's promise.

**Input scope and implementation boundary.** Bounded semantic fields and deterministic serialization. Verify prehash encoding independently of SHA-256 collision resistance; review JSON canonicalization/equivalence rather than assuming arbitrary encoders serialize identically.

**Required negative control.** Remove producer sequence, timestamp, or a length separator from the appropriate identity format.

**Related work:** TLA-003, 007–008, 013, 024.

<a id="kani-044"></a>
### KANI-044 — Deterministic append content admission and size arithmetic

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/application/append/content.rs`](src/application/append/content.rs); [`src/application/append/contract.rs`](src/application/append/contract.rs); [`src/application/append/close.rs`](src/application/append/close.rs)

**Validate.** The pure admission boundary checks supported content type, entry framing, routing-key limits, permanent record/ingest ceilings, and arithmetic before returning a prepared command. Preserve intentionally deferred producer-error precedence so exact duplicates are recognized correctly.

**Why valuable.** Rejecting an impossible final record after publishing intent can strand a stream; the temporal ordering is modeled separately, but the deterministic predicate must be correct.

**Input scope and implementation boundary.** Small binary/JSON payload cases plus symbolic declared sizes and full limits. Do not rewrite the parser in the harness or eagerly reject conditions intentionally deferred to duplicate detection.

**Required negative control.** Accept a frame larger than the permanent ceiling, overflow an aggregate length, or change deferred-producer precedence.

**Related work:** TLA-003, 005, 008.

<a id="kani-045"></a>
### KANI-045 — Transaction overlay and staged-effects consistency

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/shard/transaction/overlay.rs`](src/shard/transaction/overlay.rs); [`src/shard/transaction/append.rs`](src/shard/transaction/append.rs); [`src/shard/transaction/prepare.rs`](src/shard/transaction/prepare.rs)

**Validate.** Within a bounded group, staged tail/producer/queue changes and returned effect metadata represent the same accepted operations. A refused or duplicate operation does not consume a new range; dependent decisions observe the intended prior staged state. Failure does not accidentally publish a partially built effect list.

**Why valuable.** Checks the deterministic transaction planner beneath the TLA+ durability protocol.

**Input scope and implementation boundary.** Two to four commands using actual production planning types. Keep storage execution and concurrent effect handoff outside the harness; no fake production transaction implementation.

**Required negative control.** Advance tail on refusal, report a different offset range than staged, or keep effects from an aborted planning branch.

**Related work:** TLA-005, 007, 023.

<a id="kani-046"></a>
### KANI-046 — Absorption and trim frontier arithmetic

**Priority:** P0 · **Build route:** Extract  
**Source owners:** [`src/shard/transaction/maintenance.rs`](src/shard/transaction/maintenance.rs); [`src/history/gather.rs`](src/history/gather.rs)

**Validate.** Frontier updates are monotonic where required, remain within the relevant durable/log bounds, and never permit trimming beyond the safe boundary. Empty state, repeated publication, and MAX-adjacent positions have explicit behavior.

**Why valuable.** A one-line arithmetic mistake at this boundary can authorize destructive data removal.

**Input scope and implementation boundary.** Full `u64` frontiers with valid and malformed combinations; prove local admissibility separately from the TLA+ ordering that makes a proposed absorbed boundary trustworthy.

**Required negative control.** Use a proposed instead of safe frontier, underflow an inclusive/exclusive conversion, or wrap a next-offset calculation.

**Related work:** TLA-016, 018–019.

<a id="kani-047"></a>
### KANI-047 — Dirty/maintenance metadata codecs and debt deltas

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/shard.rs`](src/shard.rs); [`src/shard/transaction/maintenance.rs`](src/shard/transaction/maintenance.rs)

**Validate.** Supported dirty and shard-maintenance rows decode exactly; malformed/unknown layouts fail according to policy. Delta application cannot clear live debt through overflow, underflow, stale versions, or sign confusion. Lag calculations handle signed clock extremes.

**Why valuable.** These small persisted summaries drive discovery and admission; corrupt summaries can hide backlog even while records survive.

**Input scope and implementation boundary.** Bounded row bytes and full counters/timestamps; separate codec round trips, malformed-row tests, and pure delta/age functions. Preserve supported row versions rather than defaulting unknown data.

**Required negative control.** Decode a truncated row as zero debt or subtract more debt than the state owns.

**Related work:** TLA-014, 016–017, 030, 035.

<a id="kani-048"></a>
### KANI-048 — Queue state keys and consumer-generation separation

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/queue.rs`](src/queue.rs)

**Validate.** Cursor, lease, ack, config, and fence keys preserve stream hash, consumer identity, generation, tag, and numeric suffix. `decode_state_key` checks the exact tag-dependent width and rejects ambiguous/truncated identities. Supported legacy forms remain explicitly distinguished.

**Why valuable.** Prevents a recreated consumer from inheriting old leases or a cleanup scan from deleting another generation.

**Input scope and implementation boundary.** Bounded names and full `u64` generation/offset values; multiple key families with prefix-separation and round-trip harnesses.

**Required negative control.** Omit generation, accept an over/under-width suffix, or collide config and settlement tags.

**Related work:** TLA-022–025, 034, 044.

<a id="kani-049"></a>
### KANI-049 — Lease and queue-counter binary codecs

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/queue.rs`](src/queue.rs)

**Validate.** Lease fields round-trip exactly; malformed lengths and unsupported layouts fail without fabricating deadlines, attempts, or generations. Fixed-width counters require their exact representation.

**Why valuable.** A plausible default lease decoded from corrupt bytes can grant settlement authority incorrectly.

**Input scope and implementation boundary.** Full production integer fields and short malformed byte arrays around each supported width. Document any accepted legacy lease shape separately.

**Required negative control.** Accept a truncated generation/deadline or default corrupt counter bytes to zero.

**Related work:** TLA-022–024, 044.

<a id="kani-050"></a>
### KANI-050 — Visibility windows and retry-delay clamping

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/queue.rs`](src/queue.rs)

**Validate.** `visibility_window_ms`, `retry_delay_ms`, and `configured_visibility_ms` preserve the configured/default semantics and clamp full-width inputs to their intended floors and ceilings without truncation.

**Why valuable.** Prevents huge user-supplied durations from wrapping into immediate expiry or effectively permanent leases.

**Input scope and implementation boundary.** Full `u64` requested values, optional requests, and valid/invalid config boundaries. Use the constants from production, including the twelve-hour ceiling present in this snapshot.

**Required negative control.** Cast before clamping or apply the visibility floor to a retry-delay path with different semantics.

**Related work:** TLA-023; KANI-079, 084.

<a id="kani-051"></a>
### KANI-051 — Consumer-generation decision matrix

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/shard/commit_plan.rs`](src/shard/commit_plan.rs)

**Validate.** `decide_consumer_generation` returns Bind for legacy unbound state, Fenced for an older requested generation, Reset for a newer generation, and Continue for equality according to the actual zero-state rule.

**Why valuable.** This small function protects deletion/recreation authority throughout the consumer path.

**Input scope and implementation boundary.** Two unrestricted `u64` values, including zero/MAX, and coverage of every real enum result. Caller validation of whether requested zero is legal is a separate obligation.

**Required negative control.** Swap Reset/Fenced or evaluate equality before the special unbound rule when that changes the contract.

**Related work:** TLA-022–025.

<a id="kani-052"></a>
### KANI-052 — Lease settlement and extension authorization predicates

**Priority:** P0 · **Build route:** Extract  
**Source owners:** [`src/shard/transaction/prepare.rs`](src/shard/transaction/prepare.rs); [`src/application/consumer.rs`](src/application/consumer.rs); [`src/application/consumer/delivery.rs`](src/application/consumer/delivery.rs)

**Validate.** The actual pure settlement predicate requires the matching message, stream incarnation, consumer generation, lease generation, and applicable deadline/state. Invalid or stale authority cannot yield a mutation plan; an exact replay follows the documented idempotent result.

**Why valuable.** Turns the lease model's most important guard into a checked code contract.

**Input scope and implementation boundary.** Full numeric authority fields with a small valid lease/message state. Extract only the canonical decision used by production; durability and concurrent winners remain TLA+/integration obligations.

**Required negative control.** Ignore one generation or let extend/retry reuse an already settled lease.

**Related work:** TLA-022–025.

<a id="kani-053"></a>
### KANI-053 — Attempt counters, deadlines, and retry-limit decisions

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/queue.rs`](src/queue.rs); [`src/shard/transaction/prepare.rs`](src/shard/transaction/prepare.rs); [`src/application/consumer/delivery.rs`](src/application/consumer/delivery.rs)

**Validate.** Attempts advance or saturate/refuse according to policy; deadline arithmetic cannot wrap into an unintended past/future value. Max-attempt comparison triggers the correct retry/DLQ state and preserves exact replay semantics.

**Why valuable.** An overflow or off-by-one can cause infinite retries, premature DLQ movement, or stale lease acceptance.

**Input scope and implementation boundary.** Full attempt/deadline types, signed clock extremes, and boundary values around the configured retry limit. Distinguish clock-policy assumptions from arithmetic facts.

**Required negative control.** Wrap an attempt counter or change the threshold comparison by one.

**Related work:** TLA-023–025.

<a id="kani-054"></a>
### KANI-054 — DLQ transfer identity and destination binding

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/application/consumer/delivery.rs`](src/application/consumer/delivery.rs); [`src/queue.rs`](src/queue.rs); [`src/application/consumer.rs`](src/application/consumer.rs)

**Validate.** The production transfer identity/preimage is stable across retry and includes the source message and required consumer/destination-incarnation context. Reserved in-stream sequence allocation, where supported, cannot collide with ordinary user identity space.

**Why valuable.** A correct saga requires a stable idempotency identity; generating one afresh on retry defeats it.

**Input scope and implementation boundary.** Bounded IDs plus full sequence/generation fields, with separate harnesses for supported DLQ modes. Do not prove finite hash collision-freedom.

**Required negative control.** Omit destination incarnation or generate identity from retry time/attempt randomness.

**Related work:** TLA-024–025.

<a id="kani-055"></a>
### KANI-055 — Cursor fixed-field decoding and bounded payload shape

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/product_cursor/decode.rs`](src/product_cursor/decode.rs)

**Validate.** `field` and `position` consume exactly complete fields, leave the remaining slice correct, and reject truncation without advancing it incorrectly. Kind/shape error precedence and fixed-width endianness follow the wire contract.

**Why valuable.** These helpers underpin several opaque tokens; proving them once reduces repeated parser risk.

**Input scope and implementation boundary.** Symbolic short byte slices through the complete position width plus extra bytes. Token-specific trailing-byte admission is checked by the relevant outer decoder.

**Required negative control.** Advance before checking field availability or read a numeric field using the wrong endianness.

**Related work:** TLA-021–025, 042, 044.

<a id="kani-056"></a>
### KANI-056 — Key cursor structural and contextual binding

**Priority:** P1 · **Build route:** Spike  
**Source owners:** [`src/product_cursor.rs`](src/product_cursor.rs); [`src/product_cursor/decode.rs`](src/product_cursor/decode.rs)

**Validate.** Accepted key-cursor payloads preserve epoch, key hash, segment, and offset; explicit expected-epoch/key checks and kind/length checks cannot be bypassed. The real authentication call receives the expected project/key/epoch context.

**Why valuable.** Stops a validly shaped cursor from being reused for a different logical sequence.

**Input scope and implementation boundary.** Full position scalars and small payload mutations. Separate structural/context proofs from conditional MAC acceptance and real cryptographic test vectors; do not assume different keys can never collide under a finite MAC.

**Required negative control.** Remove expected-key/epoch comparison or pass payload-selected project context instead of the caller's authorized project.

**Related work:** TLA-012, 021, 032.

<a id="kani-057"></a>
### KANI-057 — Scan cursor counts, positions, expiry, and consumer admission

**Priority:** P1 · **Build route:** Spike  
**Source owners:** [`src/product_cursor.rs`](src/product_cursor.rs); [`src/product_cursor/decode.rs`](src/product_cursor/decode.rs); [`src/application/read_scan.rs`](src/application/read_scan.rs); [`src/application/read_request.rs`](src/application/read_request.rs)

**Validate.** Encoded row counts match available bytes before allocation; wire-size caps, epoch, expiry, and kind checks are correct. The consuming boundary safely handles or rejects out-of-range current indices/offsets and malformed segment lists; do not assume the decoder already enforces every semantic constraint.

**Why valuable.** Protects resumable scans against oversized tokens, panics, and false completion.

**Input scope and implementation boundary.** Zero to four segment rows with full fields and timestamps; separate header-count-versus-bytes arithmetic from larger allocation tests. Model authenticated payload admission conditionally where necessary.

**Required negative control.** Allocate from an unchecked count or index the segment list without validating the cursor position.

**Related work:** TLA-018, 021, 042.

<a id="kani-058"></a>
### KANI-058 — Message ID round trip and identity preservation

**Priority:** P1 · **Build route:** Spike  
**Source owners:** [`src/product_cursor.rs`](src/product_cursor.rs); [`src/product_cursor/decode.rs`](src/product_cursor/decode.rs); [`src/application/consumer.rs`](src/application/consumer.rs)

**Validate.** Message ID encoding/decoding preserves the exact stream epoch, key hash, segment, and offset, and the consumer boundary checks the intended context. A token of another kind is not accepted as a message ID.

**Why valuable.** Settlement must identify a logical message, not just an offset that another stream can also contain.

**Input scope and implementation boundary.** Full numeric fields and symbolic fixed-size identity bytes; keep authentication assumptions separate from structural context checks.

**Required negative control.** Drop key hash/segment from serialization or accept another token kind's compatible prefix.

**Related work:** TLA-015, 022–025.

<a id="kani-059"></a>
### KANI-059 — Lease token generation and deadline binding

**Priority:** P0 · **Build route:** Spike  
**Source owners:** [`src/product_cursor.rs`](src/product_cursor.rs); [`src/product_cursor/decode.rs`](src/product_cursor/decode.rs); [`src/application/consumer.rs`](src/application/consumer.rs)

**Validate.** Lease tokens preserve message identity, `u32` lease generation, `u64` consumer generation, and signed deadline. The authenticated payload includes these fields, and settlement receives them without truncation or substituting current state.

**Why valuable.** Omitting the consumer generation reopens the deletion/recreation ABA failure even when the lease generation is correct.

**Input scope and implementation boundary.** Full production types, fixed payload shapes, malformed token kinds/lengths, and a separately reviewed authentication boundary.

**Required negative control.** Remove consumer generation from the payload or truncate it while decoding.

**Related work:** TLA-022–025, 044.

<a id="kani-060"></a>
### KANI-060 — Catalog cursor project binding and configured signature mode

**Priority:** P1 · **Build route:** Spike  
**Source owners:** [`src/product_cursor.rs`](src/product_cursor.rs); [`src/product_cursor/decode.rs`](src/product_cursor/decode.rs)

**Validate.** The project-length/name boundary is exact and the expected project is checked. Signed mode authenticates the intended payload; keyless mode is documented as project binding without cryptographic authenticity. Mode/configuration mismatch behavior follows the adopted contract, not an invented guarantee.

**Why valuable.** Prevents cross-project repositioning and makes single-instance keyless limitations explicit.

**Input scope and implementation boundary.** Bounded project/name bytes, malformed lengths/UTF-8, and signed/keyless configurations. Verify the actual comparison/shape code with cryptographic assumptions isolated.

**Required negative control.** Ignore expected project or parse the last-name field from an unchecked length.

**Related work:** TLA-021, 032, 034, 041.

<a id="kani-061"></a>
### KANI-061 — Maintenance backpressure state transition and hysteresis

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/backpressure.rs`](src/backpressure.rs)

**Validate.** `next_state` engages for each enabled violated bound, preserves the correct cause policy, and releases only under the documented clear conditions. Disabled limits and 100% release configuration behave intentionally; integer arithmetic cannot mask pressure.

**Why valuable.** A small pure decision controls whether the service sheds or recovers under load.

**Input scope and implementation boundary.** Symbolic snapshot/limit fields and engaged state, including extreme counters and zero limits. Cover each cause and multiple simultaneous causes.

**Required negative control.** Release when only one cause clears or allow shard-local state to mask a global violation.

**Related work:** TLA-030, 040.

<a id="kani-062"></a>
### KANI-062 — Watchdog/health age and readiness decision arithmetic

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/sharddir.rs`](src/sharddir.rs); [`src/sharddir/health.rs`](src/sharddir/health.rs); [`src/shard.rs`](src/shard.rs); [`src/store_timing.rs`](src/store_timing.rs)

**Validate.** Staleness/wedge age cannot wrap under clock reversal or extreme timestamps; required engine failure makes readiness fail according to policy. A successful unrelated operation cannot incorrectly erase another required owner's failure.

**Why valuable.** Protects operational fencing/readiness decisions from arithmetic and aggregation mistakes.

**Input scope and implementation boundary.** Full signed clocks and bounded health entries; isolate pure classification from atomics, timers, and actual task execution. Document clock units at every boundary.

**Required negative control.** Use unsigned subtraction for a backward clock or clear all health failures on one owner's success.

**Related work:** TLA-011, 028, 030, 039.

<a id="kani-063"></a>
### KANI-063 — Returned-page budgets, metadata, and large-first-record progress

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/application/read_budget.rs`](src/application/read_budget.rs)

**Validate.** `PageBudget` enforces permanent plaintext, record-count, and metadata bounds; refusal leaves accounting unchanged. The first record may exceed the requested soft size only up to the real record limit, while later records obey remaining budget. JSON/base64 wire-ceiling calculations cannot overflow.

**Why valuable.** One owner governs local, stitched, and peer pages, making this a high-value code proof.

**Input scope and implementation boundary.** Bounded admission sequences with full `usize` requested/plaintext lengths and bounded symbolic keys; separate exact count-limit reachability from smaller sequence proofs.

**Required negative control.** Apply the first-record exception repeatedly or undercharge worst-case escaped metadata.

**Related work:** TLA-018, 021, 027, 029.

<a id="kani-064"></a>
### KANI-064 — Retained bytes, aliases, and reservation lifetime

**Priority:** P2 · **Build route:** Spike  
**Source owners:** [`src/retained_bytes.rs`](src/retained_bytes.rs); [`src/application/read_batch.rs`](src/application/read_batch.rs); [`src/postings_cache.rs`](src/postings_cache.rs)

**Validate.** Across supported sequential alias/slice/drop paths, the actual backing allocation retains its charge until the last relevant owner disappears; subset selection does not replace backing-capacity charge with visible length. Ordinary cancellation/error returns release only their owned reservations.

**Why valuable.** Prevents a small visible slice from hiding a large retained allocation.

**Input scope and implementation boundary.** Two to four aliases over small real `Bytes` values and actual charge owners. Abort this proof route as unsupported if dependencies cannot be analyzed; do not replace them with a toy refcount. Panic unwinding/concurrency stay outside the claim.

**Required negative control.** Release the charge on the first alias drop or detach a returned slice from its charge owner.

**Related work:** TLA-020, 027–029, 036.

<a id="kani-065"></a>
### KANI-065 — Admission arithmetic and scope-local capacity decisions

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/admission.rs`](src/admission.rs); [`src/quota.rs`](src/quota.rs)

**Validate.** Pure admission calculations respect stream, request, subscription, and project/process caps without overflow. A rejected reservation leaves the logical balance unchanged; a transferred reservation is not counted twice. Disabled modes remain explicit.

**Why valuable.** Protects the guard calculations that bound overload before work becomes expensive.

**Input scope and implementation boundary.** Full counters/limits and short reserve/refuse/release sequences using canonical production decisions. Atomic race-freedom and process RSS remain separate checks.

**Required negative control.** Increment before rejecting without rollback, cast a large count into a smaller type, or check only the wrong scope's cap.

**Related work:** TLA-029–030.

<a id="kani-066"></a>
### KANI-066 — Floating-point token bucket, refill, and Retry-After

**Priority:** P2 · **Build route:** Spike  
**Source owners:** [`src/quota/bucket.rs`](src/quota/bucket.rs)

**Validate.** The actual floating-point implementation preserves allowed debt, clamps the burst to the configured rate, performs no debit on refusal, and rounds/clamps retry delay as specified. Clock reversal follows the current last-sample policy, not an assumed monotonic-clock policy.

**Why valuable.** Existing integer-range examples do not establish behavior under extreme clocks, tiny/large rates, or float conversion boundaries.

**Input scope and implementation boundary.** Use real `f64` semantics. Derive finite/nonnegative preconditions from callers; zero-rate cases need their actual semantics. Split numeric subdomains if solving is expensive and report the uncovered domain.

**Required negative control.** Debit on refusal, refill from a negative elapsed interval, or truncate Retry-After before rounding up.

**Related work:** TLA-029–030.

<a id="kani-067"></a>
### KANI-067 — Project memory pressure and release hysteresis

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/quota.rs`](src/quota.rs)

**Validate.** Estimated pressure adds the intended components conservatively without wraparound; one project's state does not alter another's balance. The gate engages/releases at the adopted high/low thresholds and retains meaningful overflow saturation rather than appearing empty.

**Why valuable.** Underestimated pressure defeats admission even if all individual counters are correct.

**Input scope and implementation boundary.** Full pressure counters and percentage limits, with two symbolic project states. Verify only accounting/hysteresis decisions, not accuracy of the model against real allocator RSS.

**Required negative control.** Omit a retained-SSE/body component, overflow the sum, or use another project's release state.

**Related work:** TLA-029–030, 034.

<a id="kani-068"></a>
### KANI-068 — SSE feed and wire-budget calculations

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/sse/budget.rs`](src/sse/budget.rs); [`src/sse/wire.rs`](src/sse/wire.rs); [`src/sse/registry.rs`](src/sse/registry.rs)

**Validate.** Feed ring/total-cap calculations and wire-size accounting respect configured bounds and conversion widths. Charging distinguishes shared retained data from per-session output, with an explicit rule for oversized single records.

**Why valuable.** Many parked sessions magnify a small per-session arithmetic error.

**Input scope and implementation boundary.** Full configuration scalars and bounded wire frames; extract only a pure registry charge decision if needed. Real networking/serialization allocation and shared-owner lifetime need integration/ownership tests.

**Required negative control.** Multiply session/ring limits in a narrow type or forget base64/metadata expansion.

**Related work:** TLA-027, 029.

<a id="kani-069"></a>
### KANI-069 — Bounded outbox/work-queue capacity and reservation transfer

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/shard/commit_handoff.rs`](src/shard/commit_handoff.rs); [`src/shard.rs`](src/shard.rs); [`src/billing/read_accumulator.rs`](src/billing/read_accumulator.rs); [`src/fleet/outbox.rs`](src/fleet/outbox.rs)

**Validate.** The canonical capacity decisions cannot over-admit or lose accounting when a bounded queue is full. Deferred work remains represented by the owner's pending/debt state, and an accepted item receives exactly its required reservation ownership.

**Why valuable.** Complements the liveness models with local arithmetic and transition checks.

**Input scope and implementation boundary.** Small sequential queue states and full counter/size fields. Use separate harnesses per actual owner; do not invent one generic queue abstraction merely to share proofs.

**Required negative control.** Drop overflow work without durable/discoverable debt or transfer an item without its charge.

**Related work:** TLA-006, 017, 028–029, 035–036, 039.

<a id="kani-070"></a>
### KANI-070 — Sequential lifecycle transitions and stale completion predicates

**Priority:** P2 · **Build route:** Extract  
**Source owners:** [`src/tasks.rs`](src/tasks.rs); [`src/tasks/shutdown.rs`](src/tasks/shutdown.rs); [`src/shard/history_partition.rs`](src/shard/history_partition.rs); [`src/shard/commit_handoff.rs`](src/shard/commit_handoff.rs)

**Validate.** Pure open/stop/close/claim decisions never regress a terminal generation or accept a stale completion into a replacement. Result ownership is consumed once in the deterministic state transition.

**Why valuable.** Provides code-level checks for the guards used by lifecycle models without mislabeling sequential analysis as concurrency verification.

**Input scope and implementation boundary.** A bounded sequence of real transition inputs after a justified extraction. Do not assert memory-order or scheduling properties; retain Loom and cancellation integration tests for those obligations.

**Required negative control.** Accept completion from the wrong generation or transition from Stopped back to Serving without a new owner.

**Related work:** TLA-006, 011, 020, 028, 039.

<a id="kani-071"></a>
### KANI-071 — Heat/sketch counters, decay, and bounded work estimates

**Priority:** P2 · **Build route:** Extract  
**Source owners:** [`src/sketch.rs`](src/sketch.rs); [`src/scaler3.rs`](src/scaler3.rs); [`src/store_timing/observations.rs`](src/store_timing/observations.rs)

**Validate.** Counter updates, bucket indices, streaks, age calculations, and estimated work do not overflow into negative/quiet states or invalid indexing. Resetting an incarnation removes the old decision context. Approximate estimates satisfy only their adopted deterministic bounds.

**Why valuable.** Protects controller safety against numeric failures without claiming a sketch is an exact traffic oracle.

**Input scope and implementation boundary.** Short observation sequences with full integer fields; retain IEEE behavior where production uses floats. Probabilistic accuracy and real distribution quality remain statistical/benchmark work.

**Required negative control.** Wrap a hot streak to zero or reuse observations after an incarnation reset.

**Related work:** TLA-017, 030, 040.

<a id="kani-072"></a>
### KANI-072 — Split-point eligibility and unsplittable-distribution decision

**Priority:** P2 · **Build route:** Extract  
**Source owners:** [`src/scaler3.rs`](src/scaler3.rs); [`src/scaler3/controller.rs`](src/scaler3/controller.rs); [`src/segmap.rs`](src/segmap.rs)

**Validate.** A proposed split point is strictly eligible for its parent range, produces nonempty children under the format, and respects the adopted dominant-key refusal rule. Midpoint/quantile calculations cannot overflow or escape the range.

**Why valuable.** Prevents futile topology churn and invalid ranges before entering the durable protocol.

**Input scope and implementation boundary.** Small observed distributions with full key-space endpoints. State explicitly that sketch sampling may affect desirability; this proof concerns valid decisions on the supplied estimate.

**Required negative control.** Choose an endpoint as the split or overflow a midpoint calculation near MAX.

**Related work:** TLA-009, 040.

<a id="kani-073"></a>
### KANI-073 — Customer principal eligibility and policy/grant intersection

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/auth.rs`](src/auth.rs); [`src/project_policy.rs`](src/project_policy.rs); [`src/tenant.rs`](src/tenant.rs)

**Validate.** After signature validation, the canonical decision requires the expected audience/project/workspace ownership, active policy/credential state, version relationships, and the intersection of token and current grants. No later adaptation widens that principal.

**Why valuable.** Proves the authorization policy around the cryptographic dependency rather than only testing a few tokens.

**Input scope and implementation boundary.** Small typed claims/policy/grant states with full version fields and symbolic scopes. Keep signature validity an explicit conditional input and verify its real caller mapping separately.

**Required negative control.** Use token scopes without intersecting current grants, or omit ownership-version/workspace coupling.

**Related work:** TLA-031–034, 038.

<a id="kani-074"></a>
### KANI-074 — Project publication versions, omissions, and owner coupling

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/auth/publication.rs`](src/auth/publication.rs)

**Validate.** Project transition/high-water predicates reject version regression, same-version semantic change as represented by the fingerprint contract, and workspace change without an ownership-version advance. Omitted entries require the specified newer version to return.

**Why valuable.** Targets the omit-and-reintroduce bypass already explicitly discussed in this owner.

**Input scope and implementation boundary.** Full versions plus bounded present/omitted states and symbolic fingerprints. Include separate observe/predicate sequences. The proof covers retained local state, not persistence across eviction/restart.

**Required negative control.** Forget the workspace associated with ownership high-water, or clear an omission tombstone without a newer version.

**Related work:** TLA-031, 033, 038.

<a id="kani-075"></a>
### KANI-075 — Credential revocation, omission, and reactivation versions

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/auth/publication.rs`](src/auth/publication.rs)

**Validate.** Credential high-water and current-state checks cannot reactivate revoked/disabled/omitted authority at a forbidden old version. Same-version content consistency and accepted observation updates follow the contract.

**Why valuable.** Prevents stale snapshots from silently restoring permissions.

**Input scope and implementation boundary.** Full grant versions, all actual credential status variants, symbolic content fingerprints, and a bounded sequence including omission. Explicitly retain the process-local history limitation.

**Required negative control.** Permit Active at or below the recorded dead version or discard the omission version prematurely.

**Related work:** TLA-031–033.

<a id="kani-076"></a>
### KANI-076 — JWKS key identity, algorithm, retirement, and feed generation

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/auth/publication.rs`](src/auth/publication.rs); [`src/auth.rs`](src/auth.rs)

**Validate.** Known key IDs retain their allowed material/algorithm identity; retired IDs and regressing/same-generation inconsistent snapshots are rejected according to the feed contract. The signature-verification call uses the key's pinned permitted algorithm.

**Why valuable.** Catches algorithm confusion and signing-key resurrection in the glue around an audited library.

**Input scope and implementation boundary.** Two key entries, symbolic generations/fingerprints, and real algorithm enums. Verify comparison/publication decisions, not RSA/ECDSA correctness or hash collision resistance.

**Required negative control.** Allow a known key ID to change algorithm/material or reintroduce a retired key without the required policy.

**Related work:** TLA-026, 031–033.

<a id="kani-077"></a>
### KANI-077 — Prevalidate-before-publish atomic decision planning

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/auth/publication.rs`](src/auth/publication.rs); [`src/auth_feed.rs`](src/auth_feed.rs)

**Validate.** Validation of a bounded snapshot completes before any accepted-state/high-water/freshness/generation mutation is planned. A rejected entry leaves the planned publication empty; successful publication refers to one coherent validated snapshot.

**Why valuable.** Checking each row is insufficient if earlier rows were already installed when a later row fails.

**Input scope and implementation boundary.** Two to four policy/grant/key entries and one invalid later entry, using a canonical production plan/validation boundary. Locking and concurrent publication remain TLA+/integration concerns.

**Required negative control.** Mutate the first row before validating the second, or advance freshness on a rejected plan.

**Related work:** TLA-031, 033.

<a id="kani-078"></a>
### KANI-078 — Exact route/capability classification and auth-mode decisions

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/product.rs`](src/product.rs); [`src/http.rs`](src/http.rs); [`src/peer.rs`](src/peer.rs); [`src/deployment_bearer.rs`](src/deployment_bearer.rs)

**Validate.** Only the exact intended watch-wait route may use the capability exception. Customer/internal/deployment authorization modes select the correct path; malformed or near-miss routes cannot gain a broader privilege. Typed route decisions remain independent of error-display strings.

**Why valuable.** Small route-classification mistakes bypass otherwise correct authorization.

**Input scope and implementation boundary.** Bounded route components, real operation/mode enums, absent/present bearer states, and encoded/separator edge cases at the canonical parsing boundary.

**Required negative control.** Match a route prefix instead of the exact operation, or make an absent deployment bearer open Enforce mode.

**Related work:** TLA-026, 032, 042.

<a id="kani-079"></a>
### KANI-079 — Token times, freshness ages, and lease deadline arithmetic

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/auth.rs`](src/auth.rs); [`src/sse/auth.rs`](src/sse/auth.rs); [`src/queue.rs`](src/queue.rs)

**Validate.** Expiry/not-before/issued-at, feed age, and authorization lease deadlines use consistent units and correct inclusive/exclusive boundaries. Signed extreme values and skew allowances cannot overflow into extended authority.

**Why valuable.** Time arithmetic is a security decision, not merely observability math.

**Input scope and implementation boundary.** Full `i64` clocks/token fields and configured skew/freshness limits within the admitted domain; valid and invalid boundary cases. Keep consumer visibility policy in separate subharnesses where its semantics differ.

**Required negative control.** Use milliseconds as seconds, overflow a skew addition, or reset age on an invalid timestamp.

**Related work:** TLA-023, 031–033.

<a id="kani-080"></a>
### KANI-080 — Authorization lease recheck and captured-version identity

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/auth.rs`](src/auth.rs); [`src/sse/auth.rs`](src/sse/auth.rs)

**Validate.** Lease recheck compares the intended project, credential, ownership/grant versions, status, and freshness/expiry context. A removed or changed authority cannot be accepted because another current policy happens to share a name. Deadline computation does not outlive its limiting credential/feed condition.

**Why valuable.** Links request-time authorization to continued long-lived operation.

**Input scope and implementation boundary.** A captured lease and a bounded current snapshot with full versions/times. Preserve explicit allowances for in-flight work from the adopted contract; do not infer instant cancellation.

**Required negative control.** Recheck only expiry while ignoring revocation/ownership change, or use a newer unrelated credential to validate an old lease.

**Related work:** TLA-031–033.

<a id="kani-081"></a>
### KANI-081 — Watch capability envelope and signed-context construction

**Priority:** P1 · **Build route:** Spike  
**Source owners:** [`src/crypto.rs`](src/crypto.rs); [`src/application/watch.rs`](src/application/watch.rs)

**Validate.** Capability parsing and signing preimages preserve project, stream/incarnation, operation context, and required expiry/cursor fields of the adopted format. The extracted project is only a lookup hint until the real verification path authorizes it.

**Why valuable.** Avoids treating an unverified token field as authority or widening a wait capability.

**Input scope and implementation boundary.** Bounded encoded fields and symbolic identity/timestamp values. Keep actual MAC verification or explicitly labeled structural/context harnesses; do not claim unforgeability from a stub.

**Required negative control.** Trust the unverified project extraction without verification or omit an intended identity field from the preimage.

**Related work:** TLA-026, 032–033.

<a id="kani-082"></a>
### KANI-082 — Key-derivation and authorization-domain separation plumbing

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/crypto.rs`](src/crypto.rs); [`src/tenant.rs`](src/tenant.rs); [`src/deployment_bearer.rs`](src/deployment_bearer.rs)

**Validate.** Stream/segment encryption, watch capabilities, cursor signing, routing, and deployment bearer decisions use their intended distinct context/domain inputs. Wrong-domain material cannot be accepted merely because byte lengths coincide.

**Why valuable.** Checks accidental key/context reuse without claiming to prove KDF security or constant-time behavior.

**Input scope and implementation boundary.** Symbolic domain enums/context byte fields before cryptographic operations, plus exact comparison-policy tests. Hash/KDF/AEAD security and side-channel review remain external obligations.

**Required negative control.** Reuse an encryption key directly as an authorization token or remove the domain label from a derivation preimage.

**Related work:** TLA-015, 032, 034.

<a id="kani-083"></a>
### KANI-083 — Peer read-page decoding and completeness evidence

**Priority:** P1 · **Build route:** Spike  
**Source owners:** [`src/application/read_wire.rs`](src/application/read_wire.rs); [`src/application/read_batch.rs`](src/application/read_batch.rs); [`src/application/read_request.rs`](src/application/read_request.rs)

**Validate.** Required identity/continuation/truncation fields cannot default silently when absent. Record counts, payload/metadata limits, offsets, and requested span context are checked through the canonical page contract. Supported legacy adapters preserve required evidence or reject explicitly.

**Why valuable.** A permissive wire decoder can turn a remote failure into a convincing partial-success result.

**Input scope and implementation boundary.** Bounded JSON/typed wire pages with omitted/null/malformed fields and full offsets. Keep serializer dependency support honest and use saved peer fixtures/fuzzing for larger payloads.

**Required negative control.** Default a missing identity or truncated flag, or accept records outside the requested span.

**Related work:** TLA-012, 018, 021, 027, 041–042.

<a id="kani-084"></a>
### KANI-084 — Configuration numeric admission and supported-mode invariants

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/config/validation.rs`](src/config/validation.rs); [`src/config/environment.rs`](src/config/environment.rs); [`src/config/profile.rs`](src/config/profile.rs); [`src/config/model.rs`](src/config/model.rs)

**Validate.** Validated configuration cannot create invalid zero capacities, overflowing durations/byte products, mutually inconsistent limits, or unsupported feature/mode combinations. Each permitted zero/disabled value retains its intentional meaning.

**Why valuable.** Configuration is often the unproved assumption underneath every arithmetic harness.

**Input scope and implementation boundary.** Full numeric input values and real mode enums; bound strings only for parsing. Separate production-valid ranges from proof tractability bounds and prove the boundary enforcing each relied-on restriction.

**Required negative control.** Cast before validating, allow a required zero capacity, or silently enable an incompatible auth/storage mode.

**Related work:** TLA-029–033, 044.

<a id="kani-085"></a>
### KANI-085 — Exact allocation of read meters across time spans

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/rollup/allocation.rs`](src/rollup/allocation.rs)

**Validate.** `allocate` conserves all five meter totals, gives earlier spans their specified floor share, and assigns the full remainder to the final span. Empty/zero-duration input is rejected; multiplication/division and subtraction cannot overflow or underflow in the declared scope.

**Why valuable.** A particularly clean full-width arithmetic proof with direct billing value.

**Input scope and implementation boundary.** Full `[u64; 5]` dimensions and one to four positive full-width durations initially; a malformed-input harness covers zeros/empty. Report the collection bound separately from scalar coverage.

**Required negative control.** Discard the remainder, allocate it twice, or perform multiplication in `u64` instead of the required wide type.

**Related work:** TLA-037–038.

<a id="kani-086"></a>
### KANI-086 — Exact byte-millisecond storage accrual

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/rollup/storage.rs`](src/rollup/storage.rs)

**Validate.** `byte_ms` returns zero for nonpositive intervals and otherwise equals the exact nonnegative signed-clock distance times the gauge. Splitting an interval preserves the total. The maximum signed-clock span times a `u64` gauge fits the chosen representation.

**Why valuable.** Eliminates rounding/overflow uncertainty from a central storage-billing primitive.

**Input scope and implementation boundary.** Full `u64` gauge and full `i64` timestamps; independent `i128` difference/`u128` product oracle. No floating-point substitute.

**Required negative control.** Subtract clocks in `i64`, multiply in `u64`, or charge a reversed interval.

**Related work:** TLA-037–038.

<a id="kani-087"></a>
### KANI-087 — Month parsing, boundary intervals, and storage-clock advancement

**Priority:** P2 · **Build route:** Spike  
**Source owners:** [`src/billing.rs`](src/billing.rs); [`src/rollup.rs`](src/rollup.rs)

**Validate.** Month parsing rejects invalid months/formats; month spans cover the admitted interval without overlap/gaps, including year/leap boundaries. Storage-clock advancement applies each interval once and preserves its attribution/closed-month policy.

**Why valuable.** Protects billing at calendar edges that are hard to sample uniformly.

**Input scope and implementation boundary.** Admitted timestamp/year ranges with explicit extreme-input rejection, bounded two/three-month spans, and actual date-library code where compatible. Pure boundary helpers may need extraction; do not silently replace the calendar implementation.

**Required negative control.** Double-count a boundary millisecond, skip December-to-January, or advance the storage clock without accounting for its interval.

**Related work:** TLA-037–038.

<a id="kani-088"></a>
### KANI-088 — Usage counters, snapshot deltas, and overflow policy

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/billing.rs`](src/billing.rs); [`src/shard/transaction/publish.rs`](src/shard/transaction/publish.rs); [`src/rollup.rs`](src/rollup.rs)

**Validate.** Cumulative counters and emitted deltas agree in the admitted representable domain; stale snapshots cannot subtract newer usage. Overflow has an explicit policy and cannot wrap to a small believable bill. Do not claim exact conservation beyond a saturating representation's capacity.

**Why valuable.** Makes a usually implicit billing assumption visible before it becomes silent undercounting.

**Input scope and implementation boundary.** Full counter/version fields and short update/snapshot sequences. Resolve exact-versus-saturating behavior with the billing owner; enforce any restricted domain in production.

**Required negative control.** Wrap a total, compute a delta against the wrong version, or silently discard the overflow remainder while claiming exact totals.

**Related work:** TLA-035–038.

<a id="kani-089"></a>
### KANI-089 — Billing acknowledgment scope and dirty-state retention

**Priority:** P0 · **Build route:** Direct  
**Source owners:** [`src/shard/commit_plan.rs`](src/shard/commit_plan.rs)

**Validate.** `decide_billing_ack` clears dirty state only for the permitted ThroughVersion relationship, including the documented absent-current case. `FinalRowsOnly` cannot clear unrelated current usage debt.

**Why valuable.** A tiny predicate prevents acknowledgments for old/exported state from erasing newer unexported usage.

**Input scope and implementation boundary.** Full `u64` current/ack versions, optional current state, and every real `UsageAckScope` variant. Cover both clear and retain decisions.

**Required negative control.** Change the version comparison direction or allow FinalRowsOnly to clear dirty state.

**Related work:** TLA-035–037.

<a id="kani-090"></a>
### KANI-090 — Read-usage coalescing, sealing, and sequence accounting

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/billing/read_accumulator.rs`](src/billing/read_accumulator.rs)

**Validate.** Coalescing preserves every meter and attribution identity within the adopted overflow policy; sealing moves rather than duplicates rows, assigns the intended sequence, and leaves active data intact when sealing is deferred. Requeue preserves ordering/identity.

**Why valuable.** Checks the deterministic ownership transformations underneath the spool model.

**Input scope and implementation boundary.** Two billing identities and two/three bounded batches with full counters. Use the canonical owner or extracted transitions; prove sequential state changes, not Mutex scheduling. Exact conservation needs explicit representability conditions.

**Required negative control.** Merge distinct identities, clear the active map when the sealed queue is full, or reuse a batch sequence.

**Related work:** TLA-036, 038.

<a id="kani-091"></a>
### KANI-091 — Read-spool keys, row decoding, and resident accounting

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/billing/read_spool.rs`](src/billing/read_spool.rs)

**Validate.** Spool sequence keys preserve their intended ordering/identity; malformed rows are quarantined/reported according to policy rather than silently deleted as delivered. Resident entry/byte accounting reflects exact insert/replace/remove effects without underflow.

**Why valuable.** A durable spool is useful only if recovery can interpret it and drain it without losing entries.

**Input scope and implementation boundary.** Short rows, full sequence/length values, and bounded insert/remove sequences. Storage persistence ordering stays in TLA+/real storage tests.

**Required negative control.** Treat corrupt data as an empty valid batch, forget replacement-byte adjustment, or remove accounting twice.

**Related work:** TLA-036–037.

<a id="kani-092"></a>
### KANI-092 — Usage event IDs and immutable attribution preimages

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/billing.rs`](src/billing.rs); [`src/rollup.rs`](src/rollup.rs); [`src/tenant.rs`](src/tenant.rs)

**Validate.** Event-ID/preimage construction is deterministic for an exact replay and includes the required source boot/sequence or segment/version identity. Captured workspace/project/stream/ownership attribution cannot be replaced by a current lookup during serialization.

**Why valuable.** Deduplication and correct attribution depend on complete stable event identity.

**Input scope and implementation boundary.** Bounded identity strings and full versions/sequences; verify unambiguous preimage fields and deterministic serialization, not collision-freedom of the final hash/string namespace beyond its admitted encoding.

**Required negative control.** Omit source boot/version or regenerate event identity during a retry.

**Related work:** TLA-034–038.

<a id="kani-093"></a>
### KANI-093 — Signed corrections and effective usage totals

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/rollup.rs`](src/rollup.rs)

**Validate.** `eff_u64`, `eff_u128`, and correction accumulation follow the adopted clamping/overflow/error policy for positive and negative corrections. Malformed decimal input has explicit handling and cannot silently become an authoritative different amount.

**Why valuable.** Correction paths see numeric combinations unlike monotonic counters and can otherwise create massive over/undercharges.

**Input scope and implementation boundary.** Full signed correction/scalar domains and bounded decimal strings, including MIN/MAX and malformed values. Separate intentional saturation from exact arithmetic claims and review parse-failure policy.

**Required negative control.** Cast a negative correction to unsigned, overflow a subtraction, or accept a malformed amount as a valid unrelated total.

**Related work:** TLA-037.

<a id="kani-094"></a>
### KANI-094 — Rollup page, aggregate, and month-close plan consistency

**Priority:** P2 · **Build route:** Extract  
**Source owners:** [`src/rollup/page.rs`](src/rollup/page.rs); [`src/rollup/close.rs`](src/rollup/close.rs); [`src/rollup/reconciliation.rs`](src/rollup/reconciliation.rs); [`src/rollup/totals.rs`](src/rollup/totals.rs)

**Validate.** The deterministic plan for a bounded page preserves source/version idempotence, row totals, and cursor/finalization prerequisites. Missing or failed reconciliation evidence cannot become a final-close plan; bounded scans retain continuation debt.

**Why valuable.** Checks the local transformations that the rollup protocol treats as atomic effects.

**Input scope and implementation boundary.** Two ledger rows, one replay/correction, bounded aggregate maps, and optional scan continuation/error. Extract production planning only; actual DB atomicity remains an external contract.

**Required negative control.** Apply a replay twice, omit one aggregate dimension, or build a final marker from incomplete scan evidence.

**Related work:** TLA-037.

<a id="kani-095"></a>
### KANI-095 — Fleet membership, trusted targets, and deterministic ownership choice

**Priority:** P1 · **Build route:** Direct  
**Source owners:** [`src/fleet/planning.rs`](src/fleet/planning.rs); [`src/ownership.rs`](src/ownership.rs)

**Validate.** Only eligible members and trusted URLs enter the proposed view. Equal canonical ordered membership produces equal `ring_pick` results; selection stays within a nonempty list and inactive overrides are ignored. Empty-list behavior is enforced by callers.

**Why valuable.** A deterministic route must remain deterministic even in hash-tie and ordering edge cases.

**Input scope and implementation boundary.** Two to four bounded member names/URLs and full eligibility fields. Explicitly verify the ordering/tie contract; do not assume finite hash scores never tie or that arbitrary input permutations produce the same tie winner.

**Required negative control.** Honor an inactive override, accept an untrusted URL, or bypass the nonempty-list precondition.

**Related work:** TLA-011, 039–040, 043.

<a id="kani-096"></a>
### KANI-096 — Typed outcome translation without invented certainty

**Priority:** P1 · **Build route:** Extract  
**Source owners:** [`src/application/append/contract.rs`](src/application/append/contract.rs); [`src/application/append/submit.rs`](src/application/append/submit.rs); [`src/http.rs`](src/http.rs); [`src/product.rs`](src/product.rs); [`src/application/consumer_remote.rs`](src/application/consumer_remote.rs)

**Validate.** Adapters preserve success/duplicate/closed/producer metadata and the distinction between definitive refusal and ambiguous/transient failure. Required evidence is not reconstructed from display strings or silently defaulted. Every real outcome variant has an intentional translation.

**Why valuable.** Protects the final boundary where an internally correct decision becomes a misleading customer response.

**Input scope and implementation boundary.** Symbolic typed outcome variants with bounded messages and full offsets/generations; verify canonical pure mappings where feasible and retain actual HTTP/SDK conformance tests.

**Required negative control.** Translate timeout as definitive rejection, drop duplicate/closed evidence, or fabricate success from a status/error string.

**Related work:** TLA-003, 005, 023, 042.


## 6. Cross-layer assurance cases

Individual leaf checks are not a substitute for a composed argument. Maintain these small assurance cases alongside the implemented inventory. Each must name the remaining assumptions and executable evidence; none is currently discharged by this roadmap.

| Assurance case | TLA+ obligations | Kani obligations | Required bridge and remaining boundary |
|---|---|---|---|
| **An acknowledged retained record remains recoverable** | TLA-005/006, 011, 016, 018/019 | KANI-002, 007–023, 045–047 | Map every success path to its actual durable barrier; recover after real held-WAL/crash cases; establish SlateDB/object-store durability/fencing assumptions. Respect permitted retention/deletion. |
| **A seal cannot lose its final promise or corrupt a replacement** | TLA-001–004, 007 | KANI-036–043, 096 | Match claims/reservations/fences/releases to exact production guards. Replay competing takeover, lost-response, cancellation, renewal, and delete/recreate traces. |
| **Split/merge preserves per-key behavior** | TLA-004, 007, 009–012, 025 | KANI-001–005, 028–038, 048–059, 072 | Exercise stale routers and actual peer paths. No global order claim between unrelated keys; define retained producer replay metadata precisely. |
| **Forks retain required ancestry and eventually release it** | TLA-008, 013–015, 019 | KANI-024, 032–035, 043, 046–047, 082 | Use three-level real cleanup regressions and wrong-incarnation/key cases. Upstream checkpoint/object retention remains an explicit contract. |
| **Queue settlement survives retries, movement, and deletion** | TLA-005, 022–025 | KANI-048–059, 096 | Test real durable settlement/DLQ saga failures and consumer recreation. This is not exactly-once execution of arbitrary user side effects. |
| **A tenant cannot obtain another tenant's data or authority** | TLA-031–034; relevant lifecycle models | KANI-024–027, 033–035, 048, 055–060, 073–084 | Preserve actual JWT/MAC/decryption tests, route noninterference tests, and mode-specific contracts. Timing/resource side channels and cryptographic security are not covered by a logical state model. |
| **Work and resources are owned through cancellation and overload** | TLA-017, 020, 027–030 | KANI-047, 061–071 | Tie logical reservations to actual retained backing allocations and task owners. Keep Loom/Miri/DST and real memory/performance measurements; logical charge is not total RSS. |
| **Usage debt survives durable handoff without wrong attribution** | TLA-035–038 | KANI-085–094 and 089 in the source ack path | Test ledger/spool/rollup crashes separately, resolve volatile pre-spool loss policy, and establish overflow/correction semantics. External invoicing is a separate integration. |

### 6.1 Trace-to-regression workflow

Preserve the original TLC counterexample or Kani failing assignment, including the exact model/harness/configuration. Translate it into the smallest real-code test using existing semantic failpoints and `src/dst/tests` owners. Express both the triggering sequence and the customer-visible or durable-state assertion. An internal gauge alone is insufficient to show data recovery or absence of a false response.

For a TLA+ trace, record the mapping from abstract actions to actual operations/failpoints. A model may permit several concrete schedules for one action; select and document a valid concrete witness. If the trace cannot be realized, explain the abstraction mismatch rather than claiming the production test “passes the counterexample.” Keep the corrected model and any useful executable boundary regression.

For Kani, convert a minimized input into an ordinary unit/property/fuzz regression that calls the same function. Concrete-playback tooling may help when supported by the chosen version, but preserving a readable fixed input and assertion is sufficient. Do not require a new general trace framework before landing the first useful regression.

### 6.2 Model-to-code drift control

Every model action must name the relevant production function(s), its observed/durable state, and its atomicity assumption. Every production invariant owner must name the models/harnesses that constrain it. A change to one side triggers review of the other.

The mapping must include exceptional paths, not just successful operations: duplicate replies, no-write transactions, lost replies, CAS retries, definitive/transient failure classification, shutdown, cached state, supported legacy formats, and malformed-input handling. Negative controls should demonstrate that the exceptional path is actually in scope.

An optional later extension is a test-build transition trace checked against the model's allowed relation. Such checking is additional evidence, not automatic full refinement. Avoid a production-wide logging framework or runtime overhead unless justified and measured.

## 7. Repository layout, CI, evidence, and maintenance

### 7.1 Proposed layout

The following is a proposed addition, not a description of files that already exist:

```text
PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md
verification/
  manifest.json                 # implemented obligations, owners, selectors, status
  assumptions.md                # shared contracts and their evidence/revisit triggers
  tla/
    registry/                   # shared CAS/incarnation substrate
    seal/                       # seal model, small/expanded/liveness configs, mapping
    durability/
    topology/
    history/
    forks/
    ...                         # add only when the corresponding work is implemented
  regressions/
    <obligation-id>/             # minimized inputs/traces and replay instructions
  receipts/                     # small checked-in receipt metadata, not giant logs
src/
  <canonical owner>/            # colocated #[cfg(kani)] proof modules where practical
tools/
  <thin verification target>/   # only if unchanged-source inclusion is the right boundary
scripts/quality/
  <formal driver>               # deterministic discovery/execution/receipt validation
```

Reuse the current quality tooling and owner registry rather than creating a parallel policy system. Whether Kani harnesses are colocated or included by a thin crate is a compatibility and ownership decision, not an instruction to create another production crate. Do not build every empty directory in advance.

### 7.2 Toolchain and dependency policy

The inspected root `rust-toolchain.toml` pins Rust `1.98.1`; ordinary production gates keep that exact pin until an explicitly reviewed migration. Select and pin a Kani release plus the compiler/backend configuration it actually supports. If that differs from production, report the difference and check that the selected code compiles unchanged and its semantics/feature configuration remain applicable. A proof in a different compilation configuration does not erase the ordinary production build/test obligation.

Pin the TLA+ tools artifact, Java runtime, and checksums. Pin solver/verifier versions and relevant flags. Review tool/dependency upgrades as changes to the evidence trust base and rerun the required corpus. Do not use floating “latest” tools for required receipts.

Run a small compatibility probe before committing to dependency-heavy targets. If a native library, inline assembly, asynchronous path, allocation strategy, or compiler feature blocks analysis, record the unsupported boundary and use a better-scoped real-code target. Do not substitute a permissive stub and keep the old broad claim.

### 7.3 Manifest contract

Only **implemented** entries can be selected as required jobs. The complete roadmap remains the planned inventory. Suggested manifest fields are:

```json
{
  "id": "KANI-039",
  "status": "implemented-unchecked",
  "owner": "shard commit planning",
  "source_paths": ["src/shard/commit_plan.rs"],
  "harnesses": ["<actual discovered harness name>"],
  "requirements": ["L6", "L9"],
  "models": ["TLA-002"],
  "assumptions": ["<reviewed assumption IDs, or none>"],
  "input_scope": "all production u64 generations/fences and both flags",
  "configurations": ["<pinned configuration ID>"],
  "negative_controls": ["<actual control identifier>"],
  "regressions": ["<actual test path/name>"],
  "required_lane": "relevant-pr"
}
```

This is a schema sketch with placeholders, **not an existing receipt or ready-to-run harness name**. Real entries must refer to discoverable files/commands. Add schema/version validation and reject unknown statuses or missing required fields.

The manifest should distinguish which obligations are covered by a family and which are still pending. A passing KANI-039 does not turn every generation-related row into a pass. When models share modules, their transitive dependencies must appear in change selection.

### 7.4 CI lanes

**Relevant pull requests:** select implemented fast models/proofs through the existing merge-base-aware planner. Changes to canonical owners, callers enforcing assumptions, model modules/configurations, harnesses, stubs, serialization schemas, compiler/tool pins, or selection logic must invalidate the relevant receipts. Proof-only changes must still execute their affected proof/control checks. Unknown selection dependencies fail closed or select the broader safe set.

**Scheduled expanded runs:** explore larger shapes, additional failure combinations, liveness configurations, and rotated negative controls. Keep a recorded full-portfolio schedule so low-churn owners are not left unchecked indefinitely. Size/resource budgets are measured after implementation, not asserted from this document.

**Release/design-change runs:** rerun the adopted assurance cases affected by storage/protocol/dependency/migration changes and the independently required real-code/live/provider checks. A passing formal lane cannot override deployment or cryptographic holds.

For each lane, set an explicit runtime/resource budget only after measurement. A timed-out required job fails as incomplete; it cannot be silently replaced by a smaller configuration. A reviewed smaller mandatory configuration plus a clearly separate expanded lane is acceptable, with both scopes visible.

### 7.5 Zero-work and false-green protection

The driver must enumerate expected implemented IDs and reconcile them with actual tool discovery and completed results. Missing/renamed harnesses, zero discovered targets, skipped model properties, omitted configurations, unsupported warnings that invalidate the claim, and incomplete searches cannot produce a green aggregate result.

Also verify that the negative-control run really used the mutated artifact, reached the intended obligation, and failed for the expected reason. Save hashes of both the baseline and control. A parser error or unrelated assertion is not a killed semantic mutant.

Do not confuse a TLC parser success, a model simulation without violations, a Kani compilation, or a discovered assertion list with completed verification. A random/sampled simulation result must be labeled as such rather than substituted for the promised state-space exploration.

### 7.6 Receipt requirements

A compact receipt must contain:

- Source commit and dirty-diff identity, archive identity when no Git metadata exists, tool/configuration/dependency hashes, platform/target details, exact commands, and elapsed/resource information.
- Expected and executed model/harness/property IDs; per-item verdict; start/end completion state; Kani assumptions/input and unwind bounds; TLC constants, explored-state information, state/action constraints, reductions, and fairness/liveness settings.
- Exact negative-control results, reachability witnesses, counterexample/regression references, excluded/unsupported paths, remaining dependency assumptions, and the reviewer/owner decision where required.

Keep large raw logs as immutable CI artifacts with a checksum and retrieval reference; retain enough metadata in the repository to tell what was checked and reproduce it. A private local path alone is not a durable evidence reference. Credentials, tokens, real user payloads, and encryption keys must never enter trace artifacts.

Use separate verdicts for baseline correctness, negative-control effectiveness, and regression replay. None can stand in for the other two.

### 7.7 Assumption ledger

Each assumption should record its scope, origin, enforcement/evidence, and invalidation trigger. The initial ledger must cover at least these boundaries:

| Assumption area | What must be stated | Typical invalidation trigger |
|---|---|---|
| Object-store conditional writes | Atomicity of create/update, missing ETag behavior, visibility, and ambiguous reply handling | Object-store provider/client or registry CAS change |
| SlateDB durability/fencing | Meaning of applied versus durable, writer fencing, flush/close behavior, and reader/checkpoint guarantees | Pinned SlateDB revision/configuration change |
| Crypto/authentication | Idealized properties used by a model, exact authenticated preimages, supported algorithms/versions | Crypto envelope, key derivation, token or library change |
| Time and failure recovery | Clock units/skew/reversal policy, which faults eventually cease, and which actor performs recovery | Clock provider, timeout policy, controller ownership change |
| Input and numeric domains | Production-enforced bounds versus harness-only collection bounds; generation/offset exhaustion | New size limits, wider types, serialization changes |
| Authorization feeds | Publisher monotonicity/full-snapshot contract versus bounded local high-water defense | Feed publisher, omission semantics, restart/eviction policy |
| Retention and read semantics | Snapshot/retention cutoffs, applied/durable visibility, explicit resync/error behavior | Read optimization, retention policy, cursor kind change |
| Billing | Volatile versus durable metering, saturation/correction policy, attribution capture boundary | Spooling, ownership transfer, meter/version changes |

If an assumption has no production enforcement or external evidence, mark it **unestablished**. The associated result is a conditional design check, not a fully supported production assurance claim.

### 7.8 Managing model size without erasing failures

Start from a small executable specification and decompose by invariant ownership. Add composition models only for real shared boundaries, such as seal/topology, history/read/trim, and consumer/topology. Reuse state definitions where appropriate, but do not construct a universal Streams framework before the first counterexample/regression loop works.

Reduce irrelevant payloads to symbolic identities, not to one identical value when distinguishing operations matters. Use two tenants/incarnations when isolation or ABA matters; use the required number of competing actors. Bound queues and generations in ways that still represent full/empty and exhaustion behavior. Keep at least one deliberately asymmetric configuration when symmetry reductions could hide an identity/role mistake.

Treat state constraints and symmetry as part of the reviewed claim. Do not use symmetry or other reductions for liveness without an applicable tool-supported correctness justification; the safe starting point is unreduced liveness configurations. Do not force eventual success with a fairness clause on “success”; place fairness on the actual actions/actors whose scheduling is justified.

A model that permits only success or omits the destructive operation is not useful evidence. A model that passes while its targeted negative control also passes is not ready to become a gate.

## 8. Acceptance criteria and initial work packets

### 8.1 Definition of done: a TLA+ work item

A TLA+ item is gate-ready only when its requirement and observation boundaries are explicit; its atomicity/assumption mapping is reviewed; its baseline model/configurations complete the intended safety and, where applicable, liveness checks; and a relevant negative control produces the expected counterexample. Required reachable behavior must be witnessed, not removed by constraints.

It must also have a mapped executable regression or a clear reason why its design-only counterexample has no implementation yet, reproducible pinned commands and receipts, CI discovery/selection coverage, and an owner. Bounds and exclusions must be visible. Design-triggered items cannot claim production refinement merely because a proposed model passes.

### 8.2 Definition of done: a Kani proof family

A Kani family is gate-ready only when its harness calls unchanged production code, semantic assertions cover the named obligation, valid and relevant invalid domains are explicit, assumptions have enforcement/evidence, and all required harnesses finish without unresolved unwinding or unsupported-path failures. Required branches must be reachable.

It must include a passing baseline, a relevant assertion-detected negative control, a real-code regression, exact tool/configuration/source evidence, and CI discovery/selection coverage. Dependency stubs and numeric/collection restrictions must be visible in the claim. A no-panic proof alone is insufficient for semantic obligations such as authorization, encoding injectivity, or conservation.

### 8.3 Initial work packets

| Packet | Scope | Exit condition |
|---|---|---|
| **A — inventory and compatibility** | Read current instructions; register a minimal manifest/driver; establish pinned TLC and Kani execution against unchanged production code. | A real small proof/model is discovered and executed; deliberate zero-discovery, wrong-config, and incomplete-run fixtures fail correctly. No broad coverage claim. |
| **B — first scalar/codec proofs** | KANI-001/002/003, 036–039, 042, 089; start with the smallest compilable actual-owner subset. | Concrete domain questions are resolved or explicitly tracked; meaningful symbolic boundaries/negative controls run; minimized regressions are committed. |
| **C — seal takeover model** | TLA-002 with TLA-001 substrate; competing reservations, fence durability, exact renewal/release, and incarnation reuse. | The removed-newest-reservation control fails as expected; an existing real-code guard regression is mapped; recovery assumptions are reviewed. |
| **D — durable response and seal composition** | TLA-003–006; KANI-040/041/045/096 as justified by extraction work. | Dependent no-write replies and legal late durable responses are distinguished; final-debt and seal/topology counterexamples have executable witnesses. |
| **E — destructive maintenance and ancestry** | TLA-013/014, 016/018/019; postings/frame/descriptor/frontier proofs. | “Last required copy” and recursive cleanup debt are explicit; at least three ancestry levels and reader/history races are exercised. |
| **F — expansion by changed owner** | Consumers, subscriptions, resource ownership, auth, billing, fleet, and feature-triggered designs. | Each selected item independently meets the definitions above; no forced completion of unrelated planned entries. |

Do not begin by implementing 140 empty checks. Establish the evidence pipeline with a few strong cases, then grow the catalog according to risk and actual changes. A smaller set of trustworthy, maintained checks is more valuable than a large set of vacuous passes.

### 8.4 Unresolved decisions to settle during implementation

The first review should explicitly settle the supported offset/epoch domain and compatible encoding change; generation/offset exhaustion policies; exact historical duplicate-response retention; API-specific applied-versus-durable read visibility; retention/snapshot invalidation semantics; volatile pre-spool metering loss policy; and billing overflow/correction behavior.

Also establish which dependency-heavy functions can be analyzed unchanged by the chosen Kani toolchain, the actual storage/fencing/reader contracts of the pinned SlateDB integration, and the durable guarantees supplied by external authorization feeds. These are not reasons to postpone the whole program: small conditional models and direct scalar proofs can proceed while the contracts are being resolved, as long as their status is honest.

### 8.5 Measures of progress

Track completed named obligations, counterexamples converted into regressions, critical paths with an explicit model/code bridge, stale/unsupported receipts, and assumptions lacking enforcement. Track runtime and maintenance burden so the portfolio stays usable.

Do not use model count, harness count, state count, lines of TLA+, or “percentage formally verified” as a correctness metric. A high state count can reflect redundant modeling; one well-chosen competing-reservation model can be more useful than many trivial checks. A changed invariant with no active falsifiable check is a more actionable signal than a large total of green jobs.

## 9. Source and tool references

### 9.1 Repository references and provenance

The archive named at the top is the source snapshot reviewed for this roadmap. Source paths in each catalog item identify the inspected implementation/documentation boundary. They are not claims that those paths already contain the proposed checks.

The principal policy/requirement references are `AGENTS.md`, `docs/RUST-QUALITY.md`, `docs/dst/DST-EXPANSION-SPEC.md`, `docs/seal-transitions.md`, `docs/append-transitions.md`, `docs/creation-transitions.md`, `docs/ROUTING-V3.md`, `docs/HISTORY-V2.md`, and `docs/CONTROL-PLANE-INTEGRATION.md`. The latter includes proposed integration work and open decisions; distinguish that from adopted runtime guarantees.

The inspected existing integration points include `rust-toolchain.toml`, `quality-tools.toml`, `.github/workflows/rust-quality.yml`, `scripts/quality/verification_plan.py`, `docs/quality/verification.json`, `tools/quality-invariants/src/lib.rs`, and the source-local Loom/DST/property/regression tests. Existing reports such as `VERIFICATION.md` are historical evidence within their stated scope; this roadmap does not rerun or renew them.

The preceding conversation's screenshot identifies TLA+ and Kani, but does not include the full Habanero article or its eight detailed findings. This roadmap is grounded in the uploaded Streams source and primary tool documentation; it does not claim a one-to-one correspondence with those eight findings.

### 9.2 Primary tool documentation

References below were consulted on 22 September 2026. Confirm the documentation for the exact pinned versions selected during implementation; current documentation is not a substitute for a successful compatibility run.

**[T1]** Leslie Lamport, *TLA+ Tools*: TLC's explicit-state safety/liveness checking and the distinction from other TLA+ tools.  
**[T2]** TLA+ Toolbox, *Checking a Model*: execution results, errors, counterexamples, and coverage information.  
**[T3]** TLA+ Toolbox, *TLC Options Page*: checker configuration and resource/execution options.  
**[K1]** Kani, *First Steps*: symbolic inputs, assertions, assumptions, and proof harnesses.  
**[K2]** Kani, *Loops, Unwinding, and Bounds*: bounded input families and unwinding assertions.  
**[K3]** Kani, *Rust Feature Support*: concurrency, unsupported features, and panic-unwinding limits.  
**[K4]** Kani, *Where to Start on Real Code*: suitable leaf targets and I/O/dependency challenges.  
**[K5]** Kani project, *Checking Code Reachability and Sanity Checking Proof Harnesses with kani::cover*: reachability witnesses and harness sanity checking.

[T1]: https://lamport.azurewebsites.net/tla/tools.html
[T2]: https://tla.msr-inria.inria.fr/tlatoolbox/doc/model/executing-tlc.html
[T3]: https://tla.msr-inria.inria.fr/tlatoolbox/doc/model/tlc-options-page.html
[K1]: https://model-checking.github.io/kani/tutorial-first-steps.html
[K2]: https://model-checking.github.io/kani/tutorial-loop-unwinding.html
[K3]: https://model-checking.github.io/kani/rust-feature-support.html
[K4]: https://model-checking.github.io/kani/tutorial-real-code.html
[K5]: https://model-checking.github.io/kani-verifier-blog/2023/01/30/reachability-and-sanity-checking-with-kani-cover.html

---

**End state:** a maintained collection of bounded, explicit, falsifiable assurance claims linked to real code and executable regressions—not a second implementation, not a weakened test suite, and not an unsupported claim that Prisma Streams as a whole has been proven correct.
