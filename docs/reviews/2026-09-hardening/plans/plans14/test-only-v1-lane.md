# Item 80: the test-only `submit_absorbed` drives the deleted v1 absorb lane

Base: HEAD `5d9d517f` (slate, 15 unpushed commits for items 65/35/32/27). Merge base
for ceilings and ratchets: `origin/slate` = `77c13d74`. None of the 15 unpushed
commits touch `src/shard.rs`, `src/shard/transaction/*` or the four DST files
edited here. `src/dst/tests/history_gather.rs`, `src/history/gather.rs` and
`docs/refactor/test-inventory.json` are touched by them, so rebase any ledger
regeneration on HEAD, not on origin/slate.

Summary:
- The reviewer's problem is real.
- The reviewer's change is only partly buildable. Porting the 12 call sites,
  restaging the seal test, deleting `submit_absorbed` and converting the
  four `cfg_attr` dead_code fns all build and gate cleanly.
- Deleting the `v2` bool is not buildable under the CI mutation leg (§5.2).
  It forces edits inside `CommitTransaction::stage` and
  `CommitTransaction::expand`, whose whole-body mutants the repo records as
  unboundable.
- This plan deletes the only `v2: false` producer and leaves the field as a
  documented, always-true value.

---

## 1. Problem (verified on HEAD)

### 1.1 The producer

`src/shard.rs:1974-1995` has the only `v2: false` construction in the tree:

```rust
    #[expect(
        clippy::let_underscore_must_use,
        reason = "ShardEngine::submit_absorbed; a command the committer queue cannot take is re-driven ..."
    )]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "ShardEngine::submit_absorbed; the single-stream absorbed submit is the DST billing fixtures' way to stage maintenance state and the service submits batches; ..."
        )
    )]
    pub(crate) async fn submit_absorbed(&self, hash: [u8; 16], upto: u64, bytes: u64) {
        let _ = self
            .tx
            .send(CommitOp::Absorbed {
                hash,
                upto,
                bytes,
                v2: false,
            })
            .await;
    }
```

`grep -rn "v2: false" src` returns only `src/shard.rs:1992`. The production submit is
`submit_absorbed_batch_v2` (`src/shard.rs:2005-2013`, `CommitOp::AbsorbedBatch { streams, v2: true }`).
Its only production caller is `src/history/gather.rs:661`.

### 1.2 The state it stages

`src/shard/transaction/maintenance.rs:169-173` and `:203-205`:

```rust
        let lane_ok = if v2 {
            prev_absorbed == 0 || local.fields.history_v2
        } else {
            !local.fields.history_v2
        };
...
            if v2 {
                local.fields.history_v2 = true;
            }
```

A v1 (`v2 == false`) first advance on a fresh stream therefore publishes
`absorbed > 0 && !history_v2`. The reader refuses that state
(`src/application/read.rs`, in the history branch of the read loop):

```rust
            if !hist_v2 {
                // The v1 per-stream layout was deleted in the clean
                // switch: an unabsorbed-below-boundary tail without the
                // v2 flag cannot exist in a fresh namespace.
                return Err("unsupported_storage_layout: v1 history".into());
            }
```

The absorber's classifier never builds a v1 lane either
(`src/history/worker.rs`, "The v1 per-stream layout was DELETED in the pre-launch clean switch").

### 1.3 Every use site of `submit_absorbed`

The grep covered `src` (including `src/dst` and cfg(test) modules), `tools/`, `fuzz/`, `bench/`,
`tests/`, `examples/` and `sdk/`. There are 12 call sites in 7 tests. The reviewer's
"11 call sites" and "12 fixtures" are both off by one.

| File:line | Test | Call |
|---|---|---|
| `src/dst/tests/billing_maintenance.rs:407` | `mixed_append_absorb_group_refreshes_the_progress_clock` | `engine.submit_absorbed(hash, tail0.next, backlog0).await;` |
| `src/dst/tests/billing_maintenance.rs:511` | `balanced_append_absorb_group_still_writes_progress` | same text |
| `src/dst/tests/billing_maintenance.rs:594-596` | `over_retirement_fails_the_group_and_preserves_the_boundary` | `engine\n.submit_absorbed(hash, tail0.next, backlog0 + 999)\n.await;` |
| `src/dst/tests/billing_maintenance.rs:629-631` | same test | `engine\n.submit_absorbed(hash, tail2.next, tail2.unabsorbed_bytes)\n.await;` |
| `src/dst/tests/billing_maintenance.rs:733-735` | `legacy_rows_are_rebuilt_and_legacy_tails_repaired_on_open` | `engine2\n.submit_absorbed(hash, repaired.next, exact_bytes)\n.await;` |
| `src/dst/tests/reads_ring.rs:338, 341, 352` | `a_duplicate_absorbed_op_does_not_advance_the_trim` | `engine.submit_absorbed(hash, 10\|18\|18, 0).await;` |
| `src/dst/tests/runtime_sweep.rs:114-115` | `cold_shard_maintenance_debt_survives_the_sweep_and_drains` | `kept.submit_absorbed(hash, tail.next, tail.unabsorbed_bytes)\n.await;` |
| `src/dst/tests/reads_history.rs:86, 107, 124` | `the_first_advance_seals_the_history_layout` | v1 cross-layout advance on A; v1 first advance on B; v1 continuation on B |

Where else it can or cannot appear:
- **By-path includers.** None include `src/shard.rs`. The `#[path]` users are
  `tools/quality-invariants` (crypto, postings, retained_bytes, tenant,
  quota/bucket, product_cursor, queue, rollup/*, application/read_*),
  `fuzz/fuzz_targets/postings.rs` and `tests/pilot_membership.rs`.
- **Module visibility.** `mod shard` is private in `src/lib.rs:60`, and `mod dst`
  is `#[cfg(test)]` (`src/lib.rs:33-34`). No binary or integration test can reach
  the function.
- **Frozen ledgers.** `docs/quality/legacy-diagnostics*.json` and
  `docs/refactor/clippy-review-baseline.txt` still name the function. They are
  frozen adoption inventories (pinned commits) and are never regenerated.

Every fixture except the seal test uses fresh streams, where `prev_absorbed == 0`.
Both lane predicates accept a first advance there, so the v2 batch form is
behaviour-identical for them, except that it sets `history_v2`. No ported
fixture reads history below the boundary:
- `reads_ring` reads `[10,18)` through `crate::shard::read_frames_range`, a
  shard-log scan that ignores the flag (`src/shard/record.rs:149`).
- The billing and sweep tests read tails, maintenance rows and the ledger.

The sweep rig pauses its absorber (`fixture_http.rs:214-218`), so there is no
concurrent v2 retirement race.

### 1.4 The seal test pins the deleted lane

`the_first_advance_seals_the_history_layout` (`src/dst/tests/reads_history.rs:11-135`) drives the v1 lane three times:
- `:86` sends a v1 advance over sealed v2 and expects a drop.
- `:107` sends a v1 first advance, which "seals v1": the reader-refused state.
- `:124` sends a v1 continuation.

It asserts `absorb_lane_dropped >= 2`. Only the v2-side half of the seal can
still happen in a real namespace: a tail the deleted lane absorbed before the
clean switch, facing a shared-partition advance.

### 1.5 The `cfg_attr(not(test), expect(dead_code))` fns

There are exactly five in `src/shard.rs` (`grep -c "not(test)" src/shard.rs` → 5):

| Line | Item | Use sites (all verified test-only) |
|---|---|---|
| 423 | `decode_shard_maint` | `src/dst/tests/billing_maintenance.rs` (10); `src/shard/maintenance_tests.rs` (5, `#![cfg(test)]`) |
| 1772 | `ShardEngine::await_terminated` | `src/billing_service.rs:573` (inside `#[cfg(test)] mod drain_fairness_tests`, :518); `src/history/controller_tests.rs` (`#[cfg(test)] mod`, history.rs:30); `src/shard/*_tests.rs` (8 files); DST `durability_gather`, `runtime_isolation`, `reads_applied` |
| 1978 | `ShardEngine::submit_absorbed` | deleted by this item |
| 2218 | `ShardEngine::pump_trim_tick` | `src/dst/tests/history_recovery.rs:202` |
| 2646 | `ShardEngine::count_consumer_state_rows` | DST `consumer_delete`, `consumer_atomicity`, `consumer_generations`, `consumer_saga` |

Once `submit_absorbed` is gone, the reviewer's "four" are exactly these four.

The code they call keeps its production users, so the conversion creates no new
dead code:
- `EngineShutdown::wait`: `sharddir.rs:665`, `shard_directory.rs:397`.
- `crate::queue::state_prefix`: `shard/transaction/queue/cleanup.rs:80`.
- `decode_shard_maint_row`: the load path, `shard.rs:455`.
- `CommitOp::TrimTick`: the flush ticker.

Other `cfg_attr(not(test), …dead_code)` sites in the tree are **not** test-only
by design and are out of scope (§8):
- `product.rs:93` `canonical_stream_name`: its reason names a pending production caller.
- `touch_keys.rs`: livebench uses it.
- `shard_directory.rs:64/90`: enum variants, under `allow`.

---

## 2. Contract decision

There is no contract change at the product or raw edge:
- `submit_absorbed` was compiler-proven dead outside `cfg(test)` (its own
  `expect(dead_code)`).
- The four conversions remove only items the non-test build never compiled in.
- `absorb_lane_dropped` (`/v1/debug/load`, `http.rs:1002`) keeps its meaning and its
  producer.
- The committer's lane guard is unchanged, byte for byte.
- There are no wire, status or config changes.

---

## 3. Pinning tests and non-vacuity controls

This item is a test rewrite plus dead-code deletion. There is **no behaviour
change**, so there are no red tests. Every test below must pass on HEAD and after
each commit.

### 3.1 Pins (exact names; libtest path prefix `dst::dst_tests::`)

| Pin | Guards |
|---|---|
| `reads_ring::a_duplicate_absorbed_op_does_not_advance_the_trim` | duplicate op keeps the deferred-trim lag (HIS-013) |
| `billing_maintenance::mixed_append_absorb_group_refreshes_the_progress_clock` | progress clock refresh |
| `billing_maintenance::balanced_append_absorb_group_still_writes_progress` | zero-net group writes |
| `billing_maintenance::over_retirement_fails_the_group_and_preserves_the_boundary` | checked retirement |
| `billing_maintenance::legacy_rows_are_rebuilt_and_legacy_tails_repaired_on_open` | R26-4 repair follow-through |
| `runtime_sweep::cold_shard_maintenance_debt_survives_the_sweep_and_drains` | sweep keeps indebted shard |
| `reads_history::the_first_advance_seals_the_history_layout` (restaged) | first advance claims shared partition; legacy tail keeps its boundary; drop counted |
| `shard::` owner suite (`cargo test --lib shard::`) | cfg(test) conversions compile and the witnesses still work |

Expected output for each is `test dst::dst_tests::<module>::<name> ... ok`.

### 3.2 Non-vacuity controls

Each control is a temporary edit applied after C1b and reverted with `git checkout -- <file>`.

**N1: the port reaches the committer.** In `src/shard.rs::submit_absorbed_batch_v2`,
change `if streams.is_empty() {` to `if !streams.is_empty() {`. Run the 7 DST pins. All 7
must FAIL:

| Test | Expected failure |
|---|---|
| `a_duplicate_absorbed_op_does_not_advance_the_trim` | panics `absorbed never reached 10` |
| `mixed_append_absorb_group_refreshes_the_progress_clock` | `boundary must have advanced` |
| `balanced_append_absorb_group_still_writes_progress` | `assertion left == right failed` with left: 0, right: 1 |
| `over_retirement_fails_the_group_and_preserves_the_boundary` | `a diverged group must commit NOTHING, but the rider acked: Acked { .. }` |
| `legacy_rows_are_rebuilt_and_legacy_tails_repaired_on_open` | `repaired ledger never retired cleanly` |
| `cold_shard_maintenance_debt_survives_the_sweep_and_drains` | `retirement never drained the kept engine` |
| `the_first_advance_seals_the_history_layout` | `first v2 advance seals v2` with left `(0, false)` |

**N2: the legacy-tail leg pins the guard's refusing half.** In
`src/shard/transaction/maintenance.rs:170`, replace `prev_absorbed == 0 || local.fields.history_v2`
with `true`. Then:
- The seal test FAILS: `a v2 advance on a legacy per-stream tail must be dropped whole`,
  left `(5, true)`, right `(3, false)`.
- The other 6 pins stay green, because their streams are fresh.

**N3: the A leg pins the flag set.** In `maintenance.rs:204`, comment out
`local.fields.history_v2 = true;`. The seal test FAILS with `first v2 advance seals v2`,
left `(3, false)`.

**N4: the plant is loaded.** The restaged test asserts
`assert_eq!(sentinel, 5, "the sentinel lands after the planted tail")`. If the
planted tail were not what the engine loaded, the sentinel would land at offset 0
and fail with that message. This assertion is its own control, so no edit is
needed.

**Baseline.** Before C1a, all 7 pins pass on HEAD, including the old seal test. The
restaged seal test also passes on HEAD's engine: the v2 arm of `lane_ok` is
already one-sided.

---

## 4. Edits, file by file, in commit order

Ceilinged files (merge base origin/slate): only `src/shard.rs` (3,186) is touched.
It shrinks to 3,163 after C2 and to 3,139 after C3. Every DST file stays far below
1,000:

| File | Before | After |
|---|---|---|
| `reads_history.rs` | 588 | ~574 |
| `reads_ring.rs` | 595 | 595 |
| `billing_maintenance.rs` | 854 | 850 |
| `runtime_sweep.rs` | 911 | 910 |
| `fixture_storage.rs` | 295 | ~308 |

`src/shard/transaction/{mod,prepare,maintenance}.rs` are **not touched** (§5.2).

### C1a: port the nine mechanical call sites

Commit message: "Maintenance fixtures stage boundary advances on the shared-partition lane the absorber uses"

**`src/dst/tests/fixture_storage.rs`.** Add after `append_sized`:

```rust
/// Stages one stream's absorbed-boundary advance the way one gather
/// confirms it, so a fixture can put a stream into a maintenance state
/// without running the absorber.
pub(super) async fn absorb_through(
    engine: &Arc<crate::shard::ShardEngine>,
    hash: [u8; 16],
    upto: u64,
    retired_bytes: u64,
) {
    engine
        .submit_absorbed_batch_v2(vec![(hash, upto, retired_bytes)])
        .await;
}
```

Why a helper instead of `submit_absorbed_batch_v2(vec![..])` at each site: four of
the seven ported tests sit under reasoned fn-level exceptions, which ratchet
`syntax_facts` and `scope_lines`:
- `mixed_…`, `balanced_…` and `over_retirement_…` carry
  `#[expect(clippy::disallowed_methods)]` (`billing_maintenance.rs:354/464/549`).
- `a_duplicate_absorbed_op_…` carries `#[expect(clippy::excessive_nesting)]`
  (`reads_ring.rs:295`).

The fact counts per site:
- An inline `vec![…]` adds 3 facts per site (`macro`, `macro-tokens`, path `vec`;
  `tools/quality-syntax/src/scan.rs:279-285`). rustfmt also re-blocks the long
  billing chains to 3 lines. Both would fail "accepted exception grew".
- The helper call is count-neutral against the method call it replaces. Before:
  `method-call` + `method-call-site` + paths. After: `call-site` + path
  `absorb_through` + the same argument paths. For example
  `engine.submit_absorbed(hash, 10, 0)` gives 4 facts and
  `absorb_through(&engine, hash, 10, 0)` also gives 4.
- The two 3-line billing sites shrink to 1 line, so `scope_lines` only drops.

The fallback, if a reviewer prefers inline batches, is to re-decide those four
reason texts (new identity). That is not recommended: it churns four fixture
decisions for a `vec!`.

**`src/dst/tests/reads_ring.rs`**
- Import: `use super::fixture_storage::{absorb_through, append_sized, mem, open_engine, open_engine_cfg, skey};` (98 columns, one line).
- `:338` becomes `absorb_through(&engine, hash, 10, 0).await;`, and `:341` and
  `:352` the same with `18`.
- The doc line `A duplicate \`Absorbed{upto}\` op…` stays accurate: the batch expands to that op.

**`src/dst/tests/billing_maintenance.rs`**
- Import: `use super::fixture_storage::{absorb_through, mem, open_engine, skey};`
- `:407` and `:511` become `absorb_through(&engine, hash, tail0.next, backlog0).await;`
- `:594-596` becomes `absorb_through(&engine, hash, tail0.next, backlog0 + 999).await;`
- `:629-631` becomes `absorb_through(&engine, hash, tail2.next, tail2.unabsorbed_bytes).await;`
- `:733-735` becomes `absorb_through(&engine2, hash, repaired.next, exact_bytes).await;`

**`src/dst/tests/runtime_sweep.rs`**
- Import: `use super::fixture_storage::{absorb_through, mem};`
- `:114-115` becomes `absorb_through(&kept, hash, tail.next, tail.unabsorbed_bytes).await;`

**`docs/refactor/test-inventory.json`**: `python3 scripts/test-inventory.py --write`. The diff
must be exactly 6 `function_sha256` lines (the 6 tests above).

Run `cargo fmt --all`. All new lines are under rustfmt's 60-column call width, so each
site stays one line.

### C1b: restage the seal test via a directly written legacy tail

Commit message: "The layout-seal test plants a per-stream-lane tail instead of driving the deleted lane"

**`src/dst/tests/reads_history.rs`**
- Imports: add `use crate::shard::{TailFields, encode_tail_for_tests, tail_key};`
  after `use crate::dst::{FaultPlan, FaultStore};`. All three are `pub(crate)`;
  `encode_tail_for_tests` is the cfg(test) re-export at `shard.rs:3180`.
- Replace lines 11-135 (doc and test) with the draft in
  `plans14/seal_draft.rs`, reproduced below.

```rust
/// The committer owns the history layout. A stream's first advancing
/// boundary claims the shared partition and later advances continue on
/// it. A tail the deleted per-stream lane absorbed (absorbed > 0 without
/// the shared-partition bit) keeps its boundary: covering more of it from
/// the shared partition would publish a range the read path refuses as v1
/// history. The drop is counted so a namespace still carrying such a tail
/// shows in /v1/debug/load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_advance_seals_the_history_layout() {
    let inner = mem();
    let store = FaultStore::uniform(inner.clone(), 103, FaultPlan::new(0, 0, 0));
    let key = skey();

    let db = slatedb::Db::builder("dst-seal", store.clone() as Arc<dyn ObjectStore>)
        .with_settings(slatedb::config::Settings {
            flush_interval: Some(std::time::Duration::from_millis(5)),
            manifest_poll_interval: std::time::Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .expect("open db");
    // Stream B's tail as the deleted per-stream lane left it.
    let b = [0xF2u8; 16];
    let legacy = TailFields {
        next: 5,
        absorbed: 3,
        route: b,
        ..Default::default()
    };
    let mut plant = slatedb::WriteBatch::new();
    plant.put(tail_key(&b), encode_tail_for_tests(&legacy));
    db.write(plant).await.expect("plant the legacy tail");
    let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
    // R25-A: (unchanged three comment lines)
    let __maint = crate::shard::load_or_rebuild_maintenance(&db)
        .await
        .expect("load maintenance");
    let engine = crate::shard::ShardEngine::start(
        "dst-seal".to_string(),
        Arc::new(db),
        store.clone(),
        crate::shard::ShardConfig::default(),
        absorb_tx,
        None,
        __maint,
    );

    // (nested `wait_absorbed` with its #[expect(clippy::excessive_nesting)]: UNCHANGED, byte for byte)

    // Stream A: the first advance claims the shared partition; later
    // advances continue on it.
    let a = [0xF1u8; 16];
    for _ in 0..5 {
        append_sized(&engine, a, &key, "", 512).await;
    }
    engine.submit_absorbed_batch_v2(vec![(a, 3, 0)]).await;
    let (abs, flag) = wait_absorbed(&engine, a, 3).await;
    assert_eq!((abs, flag), (3, true), "first v2 advance seals v2");
    engine.submit_absorbed_batch_v2(vec![(a, 5, 0)]).await;
    let (abs, flag) = wait_absorbed(&engine, a, 5).await;
    assert_eq!((abs, flag), (5, true), "sealed v2 keeps advancing");

    // Stream B: the advance over the legacy tail is dropped whole. The
    // committer queue is FIFO, so the sentinel's ack proves it was staged.
    engine.submit_absorbed_batch_v2(vec![(b, 5, 0)]).await;
    let sentinel = append_sized(&engine, b, &key, "", 64).await;
    assert_eq!(sentinel, 5, "the sentinel lands after the planted tail");
    let h = engine.stream_handle(b).await.unwrap();
    let (abs, flag) = {
        let s = h.state.lock().unwrap();
        (s.durable.absorbed, s.durable.history_v2)
    };
    assert_eq!(
        (abs, flag),
        (3, false),
        "a v2 advance on a legacy per-stream tail must be dropped whole"
    );
    assert_eq!(
        engine.absorb_lane_dropped.load(Ordering::Relaxed),
        1,
        "the dropped cross-layout advance must be counted"
    );
    engine.begin_close();
}
```

Why this staging is sound:
- **The plant loads.** The tail is planted before `load_or_rebuild_maintenance`
  and `ShardEngine::start`. The rebuild scans only the dirty index (`shard.rs:491-508`),
  so a tail without a dirty row is ignored. `stream_handle` reads the planted row
  lazily on first touch (`shard.rs:2369-2372`). `trimmed == trim_safe_to == 0`, so
  there is no trim debt.
- **The counter is exact.** `_absorb_rx` is held and never drained, so no
  background absorber runs. A's two advances are accepted and B's is the only
  drop. The count is exactly 1, stricter than the old `>= 2`.
- **The sentinel is ordered.** It rides the same mpsc behind the batch:
  `try_enqueue` and `submit_absorbed_batch_v2` share `self.tx`.
- **It stays under clippy's length limit.** Clippy counts 88 code lines (the fn
  text counts 90 with awk; clippy strips the brace lines). The old test counts 100
  under the same rule, exactly at the limit, so the new body is safely under
  `too_many_lines` (100).
- **The ratchet does not see it.** Nesting is unchanged, and the outer test carries
  no exception. The nested fn's exception scope is the nested fn only, which is
  unchanged.
- **The name is kept.** No scenario map, relocation or disposition row changes.

**`docs/refactor/test-inventory.json`**: `--write`. Exactly 1 `function_sha256` line changes
(`the_first_advance_seals_the_history_layout`). `configuration` is unchanged
(`FaultStore::uniform(... FaultPlan::new(0, 0, 0))`).

### C2: delete `submit_absorbed`

Commit message: "The engine no longer carries a submit for the deleted per-stream history lane"

**`src/shard.rs`**
- Delete lines 1974-1996: both attribute blocks, the fn and the trailing blank
  line. That is 23 lines, leaving 3,163.
- This deletes two exception identities: `let_underscore_must_use` and the
  cfg_attr-nested `expect(dead_code)`. Deleting an identity is not growth.
  `docs/quality/source-allowances.json` and `owners.json` have **no** rows for either,
  so there is nothing to prune. Run `gate.py`, and use `--prune` only if it reports
  obsolete allowances; none are expected.
- Line-neutral doc fix on `CommitOp::Absorbed` (`shard.rs:947-952`). Six `///`
  lines in, six out. That matters: the enum sits under
  `#[expect(clippy::large_enum_variant)]` (`:877`), and each doc line is one
  attribute fact. Replace from "…history source. The" to the end with:

```rust
    /// `v2` marks the range as living in the SHARED per-shard partition
    /// (docs/HISTORY-V2.md); the first advancing v2 op sets the stream's
    /// history_v2 flag, which gates the read path's history source. Only
    /// the AbsorbedBatch expansion builds this op, and its one producer
    /// always sets `v2`: the per-stream lane is deleted, and the seal keeps
    /// the tails it absorbed out of the shared partition.
```

  The old sentence ("the v2 absorber … submits one of these per covered stream") was
  already stale: the absorber submits `AbsorbedBatch`.
- No other edit. The committer loop's close drain (`shard.rs:2504-2505`) matches
  `CommitOp::Absorbed { .. } | CommitOp::AbsorbedBatch { .. }` and is untouched.

### C3: test-only witnesses compile only under `cfg(test)`

Commit message: "Test-only engine witnesses compile only under cfg(test)"

**`src/shard.rs`.** For each of the following, replace the 7-line
`#[cfg_attr(not(test), expect(dead_code, reason = "…"))]` block with `#[cfg(test)]`
(−6 lines each, −24 total, leaving 3,139):
- `decode_shard_maint` (:423)
- `await_terminated` (:1772)
- `pump_trim_tick` (:2218; keep its `#[expect(clippy::let_underscore_must_use)]`)
- `count_consumer_state_rows` (:2646)

This matches the surrounding idiom, e.g. `#[cfg(test)] pub(crate) async fn await_workers`.

Ratcheted scope touched: `pump_trim_tick`'s `let_underscore_must_use` exception
(fn scope; the item span includes its attributes).
- `scope_lines` −6.
- `syntax_facts` −1. The removed `cfg_attr` counts 3 facts: the attribute, the
  nested `expect(...)` recorded by `scan/attributes.rs:record`, and path `cfg_attr`.
  The added `cfg(test)` counts 2.
- The lint still fires in the test build: clippy does not exempt tests from
  `let_underscore_must_use`. The expectation stays fulfilled and needs no remedy.

The other three fns carry no other exception. The four deleted `expect(dead_code)`
identities have no allowance rows.

Doc links: none of the four is an intra-doc link target
(`grep -rn "\[\`\?(decode_shard_maint|await_terminated|pump_trim_tick|count_consumer_state_rows)" src` is empty).
`shard.rs:1288` names `await_terminated` in plain code, so rustdoc
`-D warnings --document-private-items` is unaffected.

---

## 5. Mutation analysis

### 5.1 What this plan selects

| Commit | Files | Selection | Executable mutants |
|---|---|---|---|
| C1a, C1b | `src/dst/tests/*`, `src/dst/tests/fixture_storage.rs` | not under any `CRITICAL_PREFIXES` entry (`scripts/quality/verification_plan.py:21-31`) and not registered in `mutation_owners.py`: not a mutation source | 0 |
| C2 | `src/shard.rs` (owner `shard`, filter `shard::`) | production changed, so selected | 0: only deletions and line-neutral `///` lines inside the enum; no added line inside any fn body |
| C3 | `src/shard.rs` | selected | 0: the added `#[cfg(test)]` lines sit outside fn bodies, and cargo-mutants skips `cfg(test)` fns anyway |

With zero mutants, `scripts/quality/mutation_driver.py` prints `shard: no executable mutants in the selected scope`
and continues, which is not a failure.

### 5.2 Why the `v2` bool cannot be deleted in this item (the reviewer's Change is unbuildable here)

Removing the field forces added lines inside two committer bodies:
- `CommitTransaction::stage` (`src/shard/transaction/mod.rs:135-184`) destructures
  `CommitOp::Absorbed { upto, bytes, v2, .. } => self.absorbed(&mut local, hash, upto, bytes, v2)`
  at `:176-178`.
- `CommitTransaction::expand` (`src/shard/transaction/prepare.rs:11-48`) destructures
  `CommitOp::AbsorbedBatch { streams, v2 }` and rebuilds `Absorbed { …, v2 }` at `:16-23`.

cargo-mutants' FnValue span covers the first through last body statement, so either edit selects:
- `replace CommitTransaction::stage with ()`
- `replace CommitTransaction::expand -> Vec<CommitOp> with vec![]`

The repo records the first as unboundable. `mod.rs:130-134` carries:

> "the hash arms stay separate so the mutation harness never selects the whole stage as one mutant, whose blank form hangs every waiting reply instead of failing a test"

Commit `4f522b0f` reverted a fold of those arms for exactly this reason, and the
queue chain is HELD on the same blocker. A blank `expand` drops every op before
staging, which is the same observable effect, so treat it as the same unbounded
mutant. The driver runs `--timeout 90`, and TIMEOUT fails CI.

Deleting `v2` from `absorbed()` would also select its body mutants: `==`/`!=` and
`||`/`&&` on the new `lane_ok` line, and FnValue `absorbed → ()`. No test under the
`shard::` filter drives absorption through the committer:
- `grep` finds no `AbsorbedBatch`, `submit_absorbed` or `CommitOp::Absorbed` in `src/shard/*_tests.rs`.
- `task_lifecycle_tests` starts an absorber but asserts only lifecycle.

Those mutants would be MISSED.

This plan therefore touches none of `mod.rs`, `prepare.rs` or `maintenance.rs`.

---

## 6. Ledgers

| Ledger | Change |
|---|---|
| `docs/refactor/test-inventory.json` | C1a: 6 `function_sha256`; C1b: 1 `function_sha256` (`scripts/test-inventory.py --write`, then `--check`) |
| `docs/refactor/review-mechanisms.json` | none. Not a pinned mechanism test, support function, fixture or relocation (checked: only `src/shard/transaction_tests.rs::r03a…` is pinned among shard/DST-maintenance files, and it is untouched) |
| `docs/refactor/test-scenario-map.json`, `scenario-dispositions.json`, `test-relocations.json` | none. No rename or delete. Line fields are unchecked and already stale (FLT-008 says 737, actual 753) |
| `docs/quality/owners.json`, `source-allowances.json` | none. No new effect, glob, by-path module or macro-dsl. The deleted exceptions have no rows |
| `scripts/quality/mutation_owners.py` | none. No new critical file |
| `docs/refactor/architecture-policy.json` | none. `src/shard.rs` shrinks, and shrink is reported, not failed |
| `src/dst/tests/README.md` | none. No module added, renamed or removed |
| Frozen: `docs/quality/legacy-diagnostics*.json`, `docs/refactor/clippy-review-baseline.txt` | never regenerated |

---

## 7. Controls (run in order; never concurrently with a mutation leg)

1. **Baseline pins on HEAD, before C1a.**
   `cargo test --locked --lib -- reads_history::the_first_advance_seals_the_history_layout reads_ring::a_duplicate_absorbed_op_does_not_advance_the_trim billing_maintenance:: runtime_sweep::cold_shard_maintenance_debt_survives_the_sweep_and_drains`
   Expect all `ok`.
2. **After C1a and C1b: the same command.** Expect all `ok`. Then run N1-N3 (§3.2)
   with the listed failure messages, reverting after each
   (`git checkout -- src/shard.rs src/shard/transaction/maintenance.rs`).
3. **Formatting.** `cargo fmt --all -- --check` gives exit 0.
4. **The non-test build proves no production caller.**
   `cargo build --locked --lib --bins` succeeds after C2 and after C3.
5. **Clippy and the ratchet.**
   `cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl`
   then `python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl`.
   - Expect exit 0: no `unfulfilled_lint_expectations`, no `accepted exception grew`,
     no `file growth`.
   - If it prints `obsolete warning allowances; rerun with --prune`, rerun with `--prune`.
     This is not expected.
6. **Rustdoc.** `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items` gives exit 0.
7. **Shard owner suite.** `cargo test --locked --lib shard::` shows all `ok`. The
   converted witnesses are used by `shard::maintenance_tests`, the `*_tests` modules
   and `history::controller_tests`.
8. **Inventory.**
   - `python3 scripts/test-inventory.py --write`. Check that
     `git diff docs/refactor/test-inventory.json` shows only the intended
     `function_sha256` lines.
   - Then `python3 scripts/test-inventory.py --check` → `test-inventory: OK (N tests, M ignored)`,
     with N unchanged from HEAD.
9. **Evidence gates.** For each of `architecture-gate`, `scenario-map-report`,
   `test-inventory` and `review-evidence`, run `python3 scripts/<g>.py --self-test`
   and `--check`. All pass. Alternatively, `scripts/quality.sh` runs all of steps 3-9
   and ends with `QUALITY_OK`.
10. **Greps after C2 and C3.**

    | Command | Expected output |
    |---|---|
    | `grep -rn "submit_absorbed\b" src \| grep -v submit_absorbed_batch_v2` | empty |
    | `grep -rn "v2: false" src` | empty |
    | `grep -c "not(test)" src/shard.rs` | `0` |
    | `wc -l src/shard.rs` | `3139` |
11. **Mutation selection (CI's own plan).**
    `QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan`
    - `mutation_source_files` gains `src/shard.rs` (owner `shard`). No
      `src/shard/transaction/*` path comes from this item.
    - Cheap isolation check: `git diff <pre-C1a-sha> HEAD -- src/shard.rs > target/item80.diff && cargo mutants --list --json --in-diff target/item80.diff --file src/shard.rs --package streams-slate`
      prints `[]`.
    - Run `scripts/quality/mutations.sh` only when no other leg is running. The
      push range also carries items 65/35/32/27, whose owners run there.
12. **Before push:** full `scripts/gate.sh` suite, then verify CI per run with
    `gh run list --branch slate --json headSha,createdAt,status,conclusion`, matching the sha.

---

## 8. Out of scope

- **Deleting the `v2` field** from `CommitOp::{Absorbed, AbsorbedBatch}` and the
  `v2` parameter and v1 arm of `CommitTransaction::absorbed`. After C2 the field is
  always `true` and the `else { !history_v2 }` arm is unreachable. Removing them
  needs two things first:
  1. Make the blank `stage`/`expand` mutants boundable. This is the same blocker
     the queue chain is HELD on.
  2. Add a `shard::`-filter test that drives absorption through the committer, so
     the `absorbed()` mutants are killed inside the owner's filter. Sketch, in
     `src/shard/transaction_tests.rs`:
     - Plant a legacy tail for `FINAL` (`absorbed: 3, next: 5`).
     - `commit_group` one fixture append on `HASH`.
     - Send `submit_absorbed_batch_v2(vec![(HASH, 1, 0), (FINAL, 5, 0)])`, then a
       sentinel append through `engine.tx`.
     - Assert `tail_fields` gives `(1, true)` and `(3, false)`, and that
       `absorb_lane_dropped == 1`.

     That kills FnValue `absorbed`, `==`→`!=`, `||`→`&&` and FnValue
     `submit_absorbed_batch_v2`.
- **`product.rs:93 canonical_stream_name`**: its `cfg_attr(not(test), expect(dead_code))`
  reason names a pending multi-tenant production caller, so it is not test-only by
  design. Also out: `touch_keys.rs` (livebench user) and the `shard_directory.rs`
  enum variants.
- **The empty shared partition behind ported fixtures.** Those fixtures still
  publish a boundary over history that no partition holds (`history_v2 = true`,
  shared partition empty). None of them reads below the boundary through history.
  Running the real absorber would change what they pin (maintenance accounting),
  so this is not changed.
- **`docs/COST-CAMPAIGN-2.md`** keeps its historical v1-lane narrative.

## 9. Decisions for Søren

None. No product or raw edge, wire, status, config or policy change. The v2-field
retention in §8 is an engineering sequencing constraint (mutation-gate
boundability), not an edge decision.

---

## Skeptic corrections (C1..Cn)

Verified against the tree at `5d9d517f`. Checked and confirmed without
correction:
- The 12 call sites in 7 tests. A repo-wide grep outside `.claude/worktrees`
  and the frozen `legacy-diagnostics*.json` finds nothing else.
- `v2: false` appears only at `shard.rs:1992`.
- The five `not(test)` sites in `shard.rs` (424/1773/1979/2219/2647).
- The 23-line deletion (1974-1996) and the 7-line `cfg_attr` blocks.
- `wc -l`: shard.rs 3186, reads_history 588, reads_ring 595,
  billing_maintenance 854, runtime_sweep 911, fixture_storage 295.
- The fact arithmetic under the fn-level `disallowed_methods` and
  `excessive_nesting` exceptions. A method-call gives `method-call` +
  `method-call-site` + receiver/arg paths. The helper call gives `call-site` +
  path + the same arg paths (`tools/quality-syntax/src/scan.rs:222-253`). No
  touched scope is under `unwrap_used`/`expect_used`. There is no impl-level or
  file-level `#[expect]` in `shard.rs` or the touched DST files, so no
  fingerprinted key is added.
- The `CommitOp` doc swap is 6 lines out and 6 in under `large_enum_variant`
  (`shard.rs:877`). The count is neutral.
- `pump_trim_tick` syntax_facts −1, scope_lines −6.
- No allowance, owner, diagnostic-allowance, review-mechanisms (test, support
  function, fixture_changes) or relocation row names any touched item.
  `fixture_storage.rs` appears only in the historical `verification.json`
  receipt, which is not regenerated.
- The seal plant: `stored_tail` accepts it (`shard.rs:161-165`).
  `stream_handle` reads it lazily (`:2370`). The R26-4 repair runs only over
  dirty rows (`:497-508`). No startup scan exists in `start`.
  `absorb_lane_dropped` is per engine, so `== 1` is exact.
- AbsorbedBatch rides the same group as a queued append: the committer drains
  every op kind (`shard.rs:2537-2550`) and `expand` runs at group entry
  (`transaction/mod.rs:51`). So the mixed/balanced/over-retirement "ONE group"
  semantics survive the port.
- N1-N3 are buildable single-line edits, and their failure messages match the
  current assertions.
- The mutation selection is 0 executable mutants (deletions, attribute lines,
  enum doc lines only). `src/dst/**` is under no `CRITICAL_PREFIXES` entry and
  no owner source.

**C1: the base moved.** `origin/slate` is now `5d9d517f` (= HEAD; `git status -sb`
shows no ahead count). The 15 commits for items 65/35/32/27 are pushed.
- The merge base for ceilings and ratchets is `5d9d517f`. The line counts
  above are unchanged, because those files were not touched.
- Drop the header claim that the push range carries other items, and the
  §7.11 sentence "The push range also carries items 65/35/32/27, whose owners
  run there".
- CI's push range for item 80 is only its own commits, so its mutation plan
  selects only owner `shard`, with 0 mutants.

**C2: `absorb_lane_dropped` field doc contradicts the restaged test.**
`src/shard.rs:1274-1277` says a nonzero count "means the absorber raced its own
lane classification — harmless with the seal".
- After this item the only way to get a drop is a tail the deleted per-stream
  lane absorbed. The new seal-test doc says exactly that ("so a namespace still
  carrying such a tail shows in /v1/debug/load").
- Add to C2 a line-neutral rewrite of those 4 `///` lines (4 in, 4 out), for
  example: "Advances rejected by the layout seal: a shared-partition advance
  over a tail the deleted per-stream lane absorbed. Nonzero means such a tail
  still exists in this namespace; the seal keeps its boundary where the read
  path can still refuse it explicitly."
- `ShardEngine` (`:1126`) carries no `#[expect]`, so there is no ratchet, and
  the line count stays 3,163/3,139.

**C3: §1.3 "every fixture except the seal test uses fresh streams where
`prev_absorbed == 0`" is inaccurate.**
- `reads_ring.rs:341` (upto 18 over prev 10) and `:352` (duplicate 18 over
  prev 18) are not first advances. On the v2 lane they pass because the
  `:338` advance already set `history_v2` (`maintenance.rs:203-205`), so the
  flag half of `prev_absorbed == 0 || history_v2` accepts `:341`.
- `:352` is a no-op on both lanes (`upto > prev_absorbed` is false at `:174`
  and `:186`), so `absorb_lane_dropped` stays 0.
- The ported test therefore also pins "a sealed v2 stream keeps advancing".
  State that in the justification. There is no behaviour change and no new
  assertion.
- The same holds for `over_retirement…:629-631`. Its prev is still 0, because
  the refused group committed nothing, so "fresh" is correct there.

**C4: base the v2-field deferral (§5.2) on `stage` alone.**
- The recorded unboundable mutant is `CommitTransaction::stage → ()`
  (`transaction/mod.rs:130-134`, commit `4f522b0f`). That alone blocks any edit
  to the `Absorbed` arm at `mod.rs:176-178`, and the deferral stands on it.
- The claim that a blank `expand` is "the same unbounded mutant" is an
  inference, not a recorded fact. `expand → vec![]` drops every op, which drops
  each `AppendReq.resp` oneshot sender. A direct waiter such as
  `fixture_storage.rs:231` (`rx.await.expect("resp")`) then fails fast instead
  of hanging.
- Reword §5.2 to say "`expand` would also be selected; its boundability is
  unmeasured" rather than asserting that it hangs.
- The reviewer's Change asked for the bool to be deleted, and this plan
  deviates from that. Say so explicitly in the C2 commit body or the item
  report, pointing to §8 for the prerequisite. It is a sequencing deviation,
  not an edge decision.

**C5: the reads_ring import is exactly 100 columns, not 98.**
`use super::fixture_storage::{absorb_through, append_sized, mem, open_engine, open_engine_cfg, skey};`
is 100 characters. rustfmt `max_width` 100 still keeps it on one line, so this
does not break anything, but it has no margin. Run `cargo fmt --all -- --check`
after C1a as planned. If rustfmt ever wraps it, the change is outside any
exception scope, because there is no file-level `#[expect]` in `reads_ring.rs`.

**C6: minor line citations.**
- The `encode_tail_for_tests` re-export is the `pub(crate) use` block at
  `shard.rs:3179-3182` (the name is on `:3181`), not `:3180`.
- `stream_handle`'s tail read is `:2370-2373`.
- The fixture_http absorber pause is `:211-217`.

These are citation fixes only.

**Controls.** All buildable. N1-N3 are single-token edits in `shard.rs`/`maintenance.rs` that compile.
- Under N3, commenting out `maintenance.rs:204` leaves an empty `if v2 {}`.
  That compiles; clippy may flag it, but the control runs tests only.
- N4 is an assertion, not an edit, and it genuinely fails (`sentinel == 0`)
  if the plant is not loaded.

**Ledgers.**
- test-inventory: 6 + 1 `function_sha256` lines. The hash starts at `fn`, so
  attributes are excluded. `configuration` is unchanged for all 7.
- Nothing else. There are no review-mechanisms pins, no scenario map,
  disposition or relocation change (no rename), and no README change: the
  helper is shared by three contracts, which satisfies the README's
  single-contract rule.

**Verdict: ready-with-corrections.** C2 is required, because the doc
contradiction is introduced by this item. C4 must be reworded and the deviation
stated. C1 and C3 are factual fixes. C5 and C6 are informational.
