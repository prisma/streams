# Item 64, step 1: topology phase B stops treating a MapError as "already done"; merge checks its allocator

Status: plan only. Nothing in the repo was edited or run. Every line number below is from HEAD `fba7b844`, re-read by content.
Prepared files (exact post-commit sources and diffs, built in the scratchpad from the HEAD files; not rustfmt-verified):
`plans9/topology-phase-b-artifacts/`: `topology.c1.rs`, `topology.c2.rs`, `topology_scaling.c2.rs`, `segmap.c3.rs`, `c1.topology.diff`, `c2.topology.diff`, `c2.topology_scaling.diff`, `c3.segmap.diff`, plus the generator scripts `c1.py` and `c2_c3.py`. Read the whole of each file before copying it.

Commit order: **C1** is a pure refactor of topology.rs. **C2** is the phase-B fix plus 3 DST red tests plus the inventory. **C3** makes the segmap merge allocator checked and adds 1 unit red test. Push them together.

---

## 1. Problem (verified on the current tree)

### 1a. The two `Err(_)` arms (reviewer's `topology.rs:535-540` and `:616-621`; both still current)

Split phase B, `src/application/topology.rs:523-541`:
```rust
                match map.split(
                    seg_id, p.split_at, frozen, low_route, high_route, crate::shard::now_ms(),
                ) {
                    Ok(_) => {
                        map.pending = None;
                        true
                    }
                    Err(_) => {
                        // Already split (idempotent completion): just clear.
                        map.pending = None;
                        map.version += 1;
                        true
                    }
                }
```
Merge phase B, `:611-622`, is identical except for the comment `// Already merged (idempotent completion): just clear.`

**The comment is false.** Idempotent completion is handled earlier, by the `pending_matches` guard (`:484-490` split, `:600-606` merge):
```rust
                let pending_matches = d
                    .segments
                    .as_ref()
                    .is_some_and(|m| m.pending.as_ref() == Some(&p));
                if !pending_matches {
                    return Mutation::Decline(false); // someone else already completed it
                }
```
A published split or merge clears `pending` in the same CAS write (the `Ok` arm, inside the same `Mutation::Write`). A second resumer therefore always sees a mismatch and declines. `mutate_incarnation` re-reads the stored descriptor on every attempt (`registry.rs:1148-1175`). So once `map.split`/`map.merge` is reached, the map still holds this exact intent and no publication has happened. **Every `Err` is a genuine refusal.**

Which `MapError`s (`segmap.rs:170-177`) can reach these arms:
- **split `NotFound`: unreachable.** `segment_route_by_id(seg_id)` (`registry.rs:725-731`) returns `Some` only if `map.get(seg_id)` exists.
- **split `InvalidSplitPoint`, merge `NotFound`/`NotAdjacent`: unreachable.** `PendingTransition::validate` (`segmap.rs:139-167`) runs on every decode and every `StreamDesc::try_from` (`registry.rs:541-543`), and phase B holds `pending == p`.
- **`AlreadySealed(id)`: no production writer creates this state.** Split and merge seal and clear the intent in one write. But `SegmentMap::validate` admits it, because a sealed segment with no successors is a legal terminal leaf. So it arrives only with a planted or foreign descriptor. The red merge test uses it.
- **split `IdExhausted`: real.** `split` returns it when `next_seg_id >= u32::MAX - 1` (`segmap.rs:303-305`).

**What the arm does today, traced.**
1. `resume_incarnation` has already physically closed the parent through `seal_identity` (`:446`).
2. The arm clears the intent and returns `true`, so the descriptor is written with `pending: None`, `version + 1`, and the parent still **live** in the map.
3. `published == true` then runs `SEGMENT_SPLITS += 1`, `scaler.retire_segments(parent)`, `invalidate` and `SEGMENT_MAP_REFRESHES += 1` (`:548-555`). The merge arm does the same with `SEGMENT_MERGES` (`:629-634`).
4. `execute_split_fenced` returns true, and the scaler controller emits a `split_committed` ops event (`scaler3/controller.rs:124-137`). The merge path emits `merge_committed` (`controller.rs` `merge`, around line 190).

**Effect on writers (the reviewer's "write-dead range", confirmed).**
- A write to the parent's range hits the closed engine.
- `AppendService::closure_is_current` (`application/append.rs:196-238`) refreshes and finds no pending intent. `resolve_segment` returns the parent, live and unsealed, so it returns `Ok(seg.seg_id == attempted && !seg.sealed)`, which is `true`.
- `execute_prepared` then returns the engine's closure to the writer as final (`append.rs:173-175`).
- Nothing is left to resume, because the intent is gone.

### 1b. Merge `next_seg_id += 1` is unchecked (reviewer's `segmap.rs:398`; still current)

```rust
        let c = self.next_seg_id;
        for (id, next) in [(a_id, a_sealed_next), (b_id, b_sealed_next)] {
            let s = self.segments.iter_mut().find(|s| s.seg_id == id).unwrap();
            s.sealed_ms = Some(now_ms); s.sealed_next_offset = Some(next); s.successors = vec![c];
        }
        self.next_seg_id += 1;                                   // segmap.rs:398
```
`split` checks all of its allocation up front (`:303-305`). `merge` mutates both parents **before** the unchecked add. At `next_seg_id == u32::MAX`:
- **dev/quality profile:** panics with `attempt to add with overflow` at `segmap.rs:398:9`, inside the registry's decide closure.
- **release (the gate/CI suite runs `--release`):** wraps to 0 and returns `Ok(u32::MAX)`. Phase B then writes a map that `SegmentMap::validate` rejects ("segment 0 exceeds allocator"). `mutate_incarnation` returns `Err(InvalidData)`, and `.unwrap_or(false)` swallows it. The result is fail-closed, but only by accident and with no typed or logged reason.

### 1c. A second unchecked add the reviewer missed: `topology.rs:502`

```rust
                let child_id = d.segments.as_ref().expect("checked").next_seg_id + 1;
```
At `next_seg_id == u32::MAX`:
- **debug:** panics at `topology.rs:502:32`, so the reviewer's own proposed red input (`u32::MAX`) panics here before it reaches the `Err` arm.
- **release:** wraps `child_id` to 0. `split` then returns `IdExhausted`, and the `Err(_)` arm swallows it.

### 1d. Full use-site list

- **`SegmentMap::split` / `merge` production callers:** only `topology.rs:523` and `:611`. Unit callers: `segmap.rs` tests and `registry/tests.rs:117,120`.
- **`MapError` consumers:** only the two `Err(_)` arms. The segmap tests at `:554` and `:585` also compare it.
- **`next_seg_id`:**
  - Written by `segmap.rs:184` (`initial`), `:321` (split) and `:398` (merge).
  - Read by `:229` (validate), `:303`, `:391`, and `topology.rs:502`.
  - Struct literals in tests: `security_lineage.rs:124,205,256`, `registry/tests.rs:54`, `resolution_tests.rs:66`, `sse/session/tests.rs:15`, and the `golden_tests.rs:589` JSON. None of these change.
- **Consumers of the `published` / `resume*` result:**
  - `execute_split_fenced` / `execute_merge_fenced` tails (`:312-315`, `:392-395`) feed `controller.rs:124` (`split_committed`) and `controller.rs` `merge` (`merge_committed`).
  - `resume_fenced` is used by `request_work.rs:93` (result ignored) and `controller.rs:146`.
  - `resume` is used by `lifecycle.rs:204` (the `claim_seal` loop, 6 tries) and `scaler3.rs:510` (cfg(test)).
  - `TopologyResume::schedule` (`topology.rs:17-35`) is called from `append.rs:231`, `read_request.rs:277,340`, `http.rs:3155`, `http/read.rs:574` and `sse/source.rs:913`.
- **The `seal_segment_identity` forwarder** (`topology.rs:38-50`) only calls `seal_identity`. Its single caller is `lifecycle.rs:732`. `seal_identity` itself is called at `:446`, `:574` and `:577`.
- **Exceptions in the scopes this touches:**
  - `resume_incarnation`: `#[expect(clippy::expect_used)]` (`:413-416`) covering `.expect("checked")` at `:502` and `.expect("checked above")` at `:522`, plus `#[expect(clippy::excessive_nesting)]` (`:417-420`) for the salt loop's `if`: fn 1, closure 2, `let changed = {` 3, `for` 4, `if` 5.
  - `resume_merge`: `#[expect(clippy::expect_used)]` (`:563-566`) covering `:610`.
  - `SegmentMap::merge`: `#[expect(clippy::too_many_arguments)]` (`segmap.rs:353-356`, whose reason text is also wrong: merge takes no "epoch", "owner" or "transition version") and `#[expect(clippy::unwrap_used)]` (`:357-360`).
- **`changed` is already constant.** Both `let changed = { … }` blocks (`:473-542`, `:589-623`) evaluate to `true` on every path that does not `return Mutation::Decline`. That makes the block, and the fifth nesting level, dead structure today.

## 2. Contract decision

**Typed contract after the change.**
- Phase B's decide closure returns `Mutation::Decline(false)` for every `MapError`, and also when no id is left for the high child. It never writes, so the descriptor stays byte-identical and the intent stays pending. `resume*` returns `false`.
- Counters, `retire_segments`, `invalidate` and the ops events are not touched.
- The refusal is logged once per resume at `error`, with the typed `MapError` (`?error`), the seg id(s) and the stream. `Decline` exits `mutate_incarnation` immediately (`registry.rs:1173`), so the log fires once per call even though `decide` is re-run after a precondition conflict.
- `SegmentMap::merge` refuses a spent allocator with `MapError::IdExhausted` **before** either parent seals, matching `split`.
- `resume*` keeps returning `bool`. Carrying a typed verdict out of phase B belongs to step 2, where begin/publish become one typed pair.

**Wire and metrics: no change on any reachable path.**
- No status, body, metric name or `/v1/debug` shape changes.
- `segment_splits`, `segment_merges` and `segment_map_refreshes` keep their keys. They stop counting publications that never happened.
- The only observable change is in the latent state where phase B cannot publish (spent allocator, or a planted or foreign map). It is listed as D1 in section 9.

## 3. Red tests

All reds are traced at HEAD `fba7b844`, with only the test code applied. The tests use only existing APIs.
- The gate and CI suite run `cargo test --release`, which has no overflow checks, so the **release** output is the authoritative red.
- The debug output is also given.
- Line numbers assume the blocks exactly as in the artifacts: `dst_block.rs` appended after `topology_scaling.rs:745`, and `segmap_test_block.rs` inserted after `segmap.rs:602`.

### 3a. `src/dst/tests/topology_scaling.rs`, 3 new tests (block in `artifacts/dst_block.rs`, 96 lines, file 745 to 841)

Helpers:
- **`resume_planted(state, name, planted: &SegmentMap) -> (bool, SegmentMap, SegmentMap)`.** It stores `planted` through `registry.cas_update` and raises `seal_gen_counter` to the intent's `seal_gen`, following the `request_topology_debt.rs:52-66` pattern. It then reads `before` with `fresh_desc`, runs `crate::scaler3::resume` and reads `after`.
- **`a_split_is_refused_with_the_allocator_at(name, next_seg_id)`.** It uses `keyed_collection(name)` and plants `SegmentMap::initial("", 1)` with `next_seg_id` set and a split intent for segment 0 at `0x8000…` (`started_ms: 1`, `seal_gen: 1`). It resumes, then asserts:
```rust
    assert_eq!(
        (resumed, after.pending.as_ref(), after.version),
        (false, before.pending.as_ref(), before.version),
        "a split with the allocator at {next_seg_id} was published or lost its intent"
    );                                                   // topology_scaling.rs:793
    assert_eq!(after, before, "a refused split leaves the map as planted");
```

**1. `a_split_the_allocator_cannot_number_keeps_its_intent_pending`** calls the helper with `("spentsplit", u32::MAX - 1)`.

Trace at HEAD:
1. `seal_identity(seg 0)` closes the parent.
2. Phase B: `child_id = u32::MAX` (no overflow).
3. `split` sets `a = MAX-1`, `b = MAX`, and `b.checked_add(1)` returns `None`, so it returns `IdExhausted`.
4. The `Err(_)` arm sets `pending = None` and `version` to 2, and returns `true`, so `Applied(true)`.
5. `resume` returns `true`.

Expected red, both profiles:
```
thread 'dst::dst_tests::topology_scaling::a_split_the_allocator_cannot_number_keeps_its_intent_pending' panicked at src/dst/tests/topology_scaling.rs:793:5:
assertion `left == right` failed: a split with the allocator at 4294967294 was published or lost its intent
  left: (true, None, 2)
 right: (false, Some(PendingTransition { kind: "split", segs: [0], split_at: 9223372036854775808, started_ms: 1, seal_gen: 1 }), 1)
```

**2. `a_split_with_no_id_for_its_high_child_keeps_its_intent_pending`** calls the helper with `("spentsplitmax", u32::MAX)`.
- **Release red:** `topology.rs:502` wraps to `child_id = 0`. `split` sets `a = MAX` and `a.checked_add(1)` returns `None`, so `IdExhausted`. The same swallow follows. The output is the same as test 1, but with `…allocator at 4294967295…`, `left: (true, None, 2)`, and the same `right`.
- **Debug red:** `panicked at src/application/topology.rs:502:32:` / `attempt to add with overflow`.
- **After C1** the debug location moves to `topology.rs:480:28`, the line `let child_id = map.next_seg_id + 1;`.

**3. `a_merge_the_segment_map_refuses_keeps_its_intent_pending`.** Steps:
1. `keyed_collection("refusedmerge")`, then `split_at_half`. The live ids are `[1, 2]` (asserted), and the map is at version 3: initial 1, then phase A +1, then `split()` +1.
2. Plant segment 2 as sealed (`sealed_ms: Some(1)`, `sealed_next_offset: Some(0)`, no successors) plus a merge intent `[1, 2]` with `seal_gen: 2`.
3. `validate()` accepts this: seal metadata is complete, the leaves 1 and 2 cover the keyspace, and the pending parents exist and are adjacent.

Trace at HEAD:
1. `resume_merge` seals 1 and 2 physically.
2. Phase B: `merge` finds `a` live and `b` not live, so it returns `AlreadySealed(2)`. This is before allocation, so the result does not depend on profile or `next_seg_id`.
3. The `Err(_)` arm sets `pending = None` and `version` to 4, and returns `Applied(true)`.
4. `resume` returns `true`.

Expected red, both profiles:
```
thread 'dst::dst_tests::topology_scaling::a_merge_the_segment_map_refuses_keeps_its_intent_pending' panicked at src/dst/tests/topology_scaling.rs:834:5:
assertion `left == right` failed: a merge the segment map refused was published or lost its intent
  left: (true, None, 4)
 right: (false, Some(PendingTransition { kind: "merge", segs: [1, 2], split_at: 0, started_ms: 1, seal_gen: 2 }), 3)
```

**Why the merge red uses `AlreadySealed` and not `next_seg_id = u32::MAX`.** At HEAD in release, a merge at `u32::MAX` wraps, and validation rejects the written map, so phase B already returns `false` and keeps the intent. A merge-at-MAX DST test would therefore be green in the real suite and red only in debug, so it is not a valid red. `IdExhausted` for merges only exists after C3. That path is covered by C2's arm (proved by test 3) together with C3's unit test.

**Stays red after C1.** C1 keeps both `Err(_)` arms and the unchecked `+ 1`, so all three stay red after C1 with identical release messages.

**No global counters are asserted.** `SEGMENT_*` are process-global statics, so asserting them would repeat the item-66 trap.

### 3b. `src/segmap.rs` unit test `an_exhausted_allocator_refuses_before_any_parent_seals` (block in `artifacts/segmap_test_block.rs`)

```rust
        let mut m = SegmentMap::initial("root", 1);
        let (a, b) = m.split(0, KEYSPACE_END / 2, 0, [1u8; 16], [2u8; 16], 2).unwrap();
        m.next_seg_id = u32::MAX;
        let spent = m.clone();
        assert_eq!(m.merge(a, b, 1, 2, [9u8; 16], 3), Err(MapError::IdExhausted),
                   "a spent allocator refuses the merge");               // segmap.rs:612 at HEAD+test
        assert_eq!(m, spent, "a refused merge seals neither parent");      // pins allocate-before-seal
        assert_eq!(m.split(a, KEYSPACE_END / 4, 0, [3u8; 16], [4u8; 16], 3), Err(MapError::IdExhausted),
                   "the same allocator refuses a split");                // control: passes at HEAD
        assert_eq!(m, spent, "a refused split opens no child");
```
Red at HEAD, release:
```
thread 'segmap::tests::an_exhausted_allocator_refuses_before_any_parent_seals' panicked at src/segmap.rs:612:9:
assertion `left == right` failed: a spent allocator refuses the merge
  left: Ok(4294967295)
 right: Err(IdExhausted)
```
Red at HEAD, debug: `panicked at src/segmap.rs:398:9:` / `attempt to add with overflow`.

**What the second assertion catches.** A C3 that placed the check at the old `+= 1` site, after the seal loop, would still fail on `m == spent`, because both parents would already be sealed.

### 3c. C1 is a pure refactor: pinning tests and compile-level proofs

**Pinning tests** (all exist, and all must stay green):
- `topology_scaling::{split_children_land_on_distinct_engines (salt loop / high route on a distinct prefix), merge_rejoins_cold_children_with_exact_lineage, an_append_routed_by_a_pre_split_descriptor_lands_on_the_child, an_append_routed_by_a_pre_merge_descriptor_lands_on_the_merged_segment}`
- `topology_lifecycle::{merge_phase_b_declines_under_sealing (phase-B sealing decline, then resume completes via the Ok arm), nondeployment_pending_split_resumes, stale_scaler_and_ttl_decisions_decline_after_recreate}`
- `seal_coordination::{topology_transitions_are_fenced_by_sealing, a_parked_split_cannot_publish_under_a_sealed_collection}`
- `seal_recovery::a_seal_never_installs_over_a_pending_transition`
- `request_topology_debt::r09a_cancelled_topology_job_leaves_exact_debt_for_a_fresh_read_owner`
- `scaler_controller::r09_scaler_cancellation_and_deadlines_preserve_intent_and_rotate_work`
- `seal_convergence::*` (collection seal through `seal_segment_identity`, including the cross-owner relay)
- `livefeed_history::livefeed_merge_continuation_in_place`

**Compile-level proofs:**
- `-D warnings` together with the denied `unfulfilled_lint_expectations` proves that no `.expect(` and no nesting beyond 4 remain in `resume_incarnation` / `resume_merge`, because their three expectations are deleted and clippy stays silent.
- The rename leaves `lifecycle.rs:732` compiling unchanged.
- The reorder is safe: `segment_route_by_id` and `d.sref()` are pure, and every guard returns the same `Decline(false)`.

## 4. Edits, file by file, in commit order

**Ceilinged files:** none of the touched files is ceilinged (all are at or under 1,000 lines).
- `src/application/topology.rs`: 636 → 589 (C1) → 610 (C2).
- `src/segmap.rs`: 603 → 628 (C3).
- `src/dst/tests/topology_scaling.rs`: 745 → 841 (C2). This is a DST file with a limit of 1,000, so 159 lines remain.

**Untouched ceilinged files** (current `wc -l`): http.rs 3,366, product.rs 4,205, shard.rs 3,196, billing.rs 2,201, history.rs 1,713, auth.rs 1,676, registry.rs 1,492, sse/feed.rs 1,170, fleet.rs 1,143. `lifecycle.rs` (903) and `scaler3.rs` (852) are also untouched.

### C1: pure refactor, `src/application/topology.rs` only (`artifacts/c1.topology.diff`; `diff -w` is about 60 lines)

1. **Delete the forwarder.**
   - Drop the pass-through `seal_segment_identity` (`:38-50`).
   - Rename `seal_identity` (`:52`) to `pub(crate) async fn seal_segment_identity` and merge the two doc comments into one: "…The one seal a transition's parents and a collection seal's live segments both go through."
   - Update the 3 internal calls (`:446`, `:574`, `:577`). `lifecycle.rs:732` keeps its name.
2. **Flatten `resume_incarnation`'s phase-B closure.**
   - Remove the constant `let changed = { … }` block and end with `Mutation::Write(d, true)`.
   - After the lifecycle guard and `low_route`, take `let stream = d.sref();`. Then fold `pending_matches` plus both `.expect`s into one guard:
     `let Some(map) = d.segments.as_mut().filter(|m| m.pending.as_ref() == Some(&p)) else { return Mutation::Decline(false); // someone else already completed it };`
   - Write `child_id = map.next_seg_id + 1` (still unchecked in C1).
   - Change the salt loop to `RouteHash::for_child(&stream, child_id, &salt.to_be_bytes()).0`, which also stops rebuilding `d.sref()` on each salt.
   - Keep the `match map.split(..) { Ok(_) => map.pending = None, Err(_) => { /* verbatim */ } }`.
   - Nesting is now at most 4 (fn 1, closure 2, `for` 3, `if` 4).
3. **Flatten `resume_merge`'s phase-B closure the same way.** The route comes first, because `segment_route_by_id(&d)` cannot be called while `d.segments` is mutably borrowed.
4. **Delete the three expectations** at `:413-416`, `:417-420` and `:563-566`, because their sites are gone. topology.rs then carries no exception.

**Ratcheted scopes in C1:** only those three. They are removed, so `exception_growth` has no current identity to compare. There is no ratchet on the rename, because no exception survives in the file.

**Contingency, if clippy still reports `excessive_nesting` for `resume_incarnation`.** This is not expected: the model above matches every existing expectation and non-expectation in the file. If it happens, restore the expectation with a re-decided reason that names the flattened structure. Never re-add the `changed` block.

### C2: phase-B refusals and DST reds

- **`src/application/topology.rs`** (`artifacts/c2.topology.diff`):
  - Change `let child_id = map.next_seg_id + 1;` to a `let Some(child_id) = map.next_seg_id.checked_add(1) else { /* reason */ tracing::error!(seg_id, stream = %desc.name, "split stays pending: no segment id is left for its high child"); return Mutation::Decline(false); };`.
  - Change `match map.split(…) {Ok/Err}` to `if let Err(error) = map.split(…) { /* reason */ tracing::error!(seg_id, stream = %desc.name, ?error, "split stays pending: the segment map refused it"); return Mutation::Decline(false); } map.pending = None;`.
  - Make the same change to merge, with `a_id, b_id, stream, ?error, "merge stays pending: the segment map refused it"`.
  - The reason comments state the invariant: it is never an idempotent completion (a published transition cleared this intent in the same write), and the parents are already closed, so clearing the intent strands their range.
  - `resume_incarnation`'s body is 91 counted lines (limit 100). HEAD was 95, and C1 is 81.
  - No exception scope remains in topology.rs. `src/application/` "hard" transport rule: no `crate::http`/`product`/`axum` paths are added.
- **`src/dst/tests/topology_scaling.rs`**: append `artifacts/dst_block.rs`. The file's three expectations (`blast_keys` `:17-20`, `split_children_land…` `:176-179`, `merge_rejoins…` `:447-450`) are fn-scoped and not touched. Tests use no `macro-dsl` (`assert_eq!`, `vec!` and `format!` are expression macros) and no spawn. `src/dst` is excluded from mt_lint. Nesting is at most 2.
- **`docs/refactor/test-inventory.json`**: `python3 scripts/test-inventory.py --write` gives 504 → 507 entries.

### C3: segmap merge allocator (`artifacts/c3.segmap.diff`)

- **`src/segmap.rs` `SegmentMap::merge`.** Right after `let c = self.next_seg_id;`, add:
  ```rust
          // Allocate before either parent seals: a refused merge leaves the
          // map exactly as phase B read it, so its intent can stay pending.
          let next_seg_id = c.checked_add(1).ok_or(MapError::IdExhausted)?;
  ```
  Replace `self.next_seg_id += 1;` with `self.next_seg_id = next_seg_id;`.
- **Ratcheted scopes.** Both expectations on `merge` grow:
  - scope_lines +3 and more syntax_facts.
  - `unwrap_used` also gets new path fingerprints: `MapError::IdExhausted` (0→1) and `next_seg_id`.
- **Remedy: re-decide both reasons.** Each has exactly two `;` and no `"`:
  - `too_many_arguments`: `"SegmentMap::merge; phase B supplies both parents, each parent's frozen next offset, the child's route and the clock as separate facts it proved; a request struct would exist for this single call site"`. The old text named arguments merge does not take.
  - `unwrap_used`: `"SegmentMap::merge; both parents were found live and adjacent and the child id allocated before either seals, so the lookup finds them; a fallible find would add a branch no validated merge reaches"`.
  - Alternative, not chosen (larger diff): drop the `unwrap` by sealing through `self.segments.iter_mut().filter(|s| s.seg_id == id)`, delete the `unwrap_used` exception, and re-decide only `too_many_arguments`.
- **Unit test.** Insert `artifacts/segmap_test_block.rs` after the `serde_round_trip` test. `mod tests` carries no exception.

**Why C3 comes after C2.** With C3 first, a merge at `u32::MAX` in release would move from fail-closed validation to the old swallowing arm for one commit.

## 5. Mutation analysis

- **No mutants are selected.**
  - `src/application/topology.rs`, `src/segmap.rs` and `src/dst/tests/topology_scaling.rs` match no `CRITICAL_PREFIXES` (`verification_plan.py:22-31`): `src/application/read_` does not match `topology.rs`, and `src/shard`/`src/sse` do not match `src/segmap.rs`.
  - None has a row in `scripts/quality/mutation_owners.py`; I grepped all 313 lines for `topology` and `segmap`.
  - The plan therefore lists none of them in `mutation_source_files`.
  - No Loom, Miri or property leg is selected by these files either. The compiler leg only.
- **Changed bodies:** C1 changes `seal_segment_identity` (the body of the former `seal_identity`, unchanged), `resume_incarnation` and `resume_merge`. C2 changes the same two resume functions. C3 changes `SegmentMap::merge` and `segmap::tests`.
- **Coverage map, in case an owner is ever registered:**
  - `resume_incarnation`/`resume_merge` whole-body `true`/`false`: killed by the publication pins (they expect `true`) and the three new tests (they expect `false`).
  - The `child_id` `else` branch: killed only by `a_split_with_no_id_for_its_high_child…`.
  - The split `Err` branch: killed by `a_split_the_allocator_cannot_number…`.
  - The merge `Err` branch: killed by `a_merge_the_segment_map_refuses…`.
  - `SegmentMap::merge` → `Ok(0)`/`Ok(1)`: killed by `merge_adjacent_only` and `an_exhausted_allocator…`.
  - No new comparison or arithmetic operator is introduced; `checked_add` is a method.
- **No owner rows or filters change.**

## 6. Ledgers

- **`docs/refactor/test-inventory.json`:** +3 DST entries in C2, via `--write`. Check that the diff contains only the three new names.
- **`docs/refactor/review-mechanisms.json` / `test-additions.json`:** no change. No topology_scaling test is pinned, and no existing test body changes.
- **`docs/quality/owners.json` / `source-allowances.json`:** no change.
  - The deleted expectations had no allowance rows; only 15 `exception` rows exist, all `result_large_err`/`unreachable_code`.
  - `tracing::` is exempt from `macro-dsl`.
  - There are no new globs, statics or by-path modules.
  - `gate.py` should report no stale rows.
- **No change to:** `docs/quality/legacy-*.json` / `verification.json` (frozen adoption records), `docs/refactor/architecture-policy.json`, `WIRE-MATRIX.md`, scenario map / dispositions (no rename; TOP-002 mapping is optional and not proposed), `src/dst/tests/README.md` (no new module), `scripts/mt-audit-baseline.txt` (no topology or segmap rows; no `stream_hash` or internal-target sites added).
- **Docs** (`MULTITENANCY-MAP.md:69,85`, `ROUTING-V3.md:444`, `seal_convergence.rs:397`) mention `seal_identity` historically. They are records and stay as they are.

## 7. Controls (run only after the running mutation run AND the gate have finished; never concurrently)

1. **Red, at HEAD with the test blocks applied uncommitted** (append `dst_block.rs`; insert `segmap_test_block.rs` after `segmap.rs:602`):
   - `cargo test --locked --release --lib an_exhausted_allocator_refuses_before_any_parent_seals` → `test result: FAILED. 0 passed; 1 failed`, with the section 3b output.
   - `cargo test --locked --release --lib keeps_its_intent_pending` → `0 passed; 3 failed`, with the section 3a outputs.
   - Optional debug check: `cargo test --locked --lib keeps_its_intent_pending` shows `attempt to add with overflow` at `src/application/topology.rs:502:32` for test 2. Tests 1 and 3 show the same assertion as in release.
   - Stash the tests.
2. **C1:**
   - `cargo fmt --all` then `cargo fmt --all -- --check` → clean.
   - `cargo clippy --locked --workspace --all-targets -- -D warnings` → no diagnostics. In particular there is no `excessive_nesting`/`expect_used` on `resume_incarnation`/`resume_merge` and no `unfulfilled_lint_expectations`.
   - `cargo test --locked --release --lib -- dst::dst_tests::topology_scaling:: dst::dst_tests::topology_lifecycle:: dst::dst_tests::seal_coordination:: dst::dst_tests::seal_recovery:: dst::dst_tests::seal_convergence:: dst::dst_tests::request_topology_debt:: dst::dst_tests::scaler_controller:: livefeed_merge_continuation_in_place segmap::tests:: registry:: --skip post_split_throughput_scales` → `0 failed`. Every pinning test in section 3c must have its own `... ok` line.
   - Re-apply the tests: they are still red, with the same release messages.
3. **C2:**
   - The 3 DST tests pass.
   - `python3 scripts/test-inventory.py --write`, then `git diff --stat docs/refactor/test-inventory.json` (only +3 entries), then `python3 scripts/test-inventory.py --check` → `test-inventory: OK (507 tests, 0 ignored)`.
4. **C3:** `cargo test --locked --release --lib segmap::tests:: registry::` → all ok, including the new unit test.
5. **Whole set:**
   - `scripts/quality.sh` → `QUALITY_OK`. This runs clippy JSON, `gate.py` (exception ratchet vs merge base, file growth), architecture/scenario/inventory/review-evidence checks and mt-lint.
   - `bash scripts/multitenancy-audit.sh` → OK.
   - `OUT=/tmp/gate.txt scripts/gate.sh` → last line `GATEDONE`.
6. **CI's selection before push:**
   - Run `QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan`. `plan.json` must list none of the three files among the mutation sources.
   - If mutants are selected at all, it is because of the eight unpushed commits. Then run `scripts/quality/mutations.sh` for those.
   - After the push, verify with `gh run list --branch slate --json headSha,createdAt,status,conclusion`, matching the sha.
7. **Commit messages** follow the repo's style. Suggested titles:
   - C1: "Topology phase B reads its map once through the intent it must still hold: no expect, no seal forwarder"
   - C2: "A split or merge the segment map refuses keeps its intent: phase B no longer reports it published"
   - C3: "A merge allocates its child id before either parent seals: a spent allocator is a typed refusal"

   Each ends with the `Co-Authored-By` attribution line.

## 8. Out of scope

- **Step 2** (the Transition enum parsed once and shared with `PendingTransition::validate`, with one begin/publish): the split and merge phase-A/B duplication stays.
- **"Delete the identical tails"** (`if !ok { return resume_incarnation(..) } resume_incarnation(..)` at `:312-315` and `:392-395`), which the reviewer put in step 1: **deferred to step 2.**
  - Deleting them literally leaves `ok` unused.
  - Dropping the phase-A result needs one of two things. One is `let _ = <Result>`, which trips `let_underscore_must_use`. The other is a named ignored binding or a new log line for the swallowed `MutationError`.
  - Step 2's single typed begin verdict makes that choice naturally. Collapsing now would be churn that step 2 rewrites.
- **Phase-B `.unwrap_or(false)`** silently maps `MutationError` (`ReadUnavailable`/`InvalidData`/`Conflict`/`AmbiguousCompletion`) to `false`. It belongs to the swallowed-errors theme and step 2's typed publish verdict.
- **Phase-A prevention.** Declining a transition the allocator cannot number *before* its parents seal would keep the collection writable instead of leaving an intent pending forever. See D2.
- **Mutation owners** for topology.rs/segmap.rs: registering them would mutation-gate every future topology edit and needs a DST filter set. That is a separate decision.

## 9. Decisions for Søren

- **D1 (edge, latent state only).** When phase B cannot publish (split with `next_seg_id >= u32::MAX-1`, merge at `u32::MAX` after C3, or any `MapError` from a planted or foreign descriptor), the intent now **stays pending** instead of being dropped as if it had been published. What becomes observable in that state:
  - A writer to the closed parent's range gets the bounded wait, then retryable `503 segment_transition` (`append.rs:230-235` / `:183-188`) instead of the engine's closure reported as final.
  - A collection seal answers `Resumable` ("a split or merge is in flight and did not settle") after `claim_seal`'s 6 resumes.
  - The scaler controller carries the intent as debt and logs one error per pass.
  - `split_committed`/`merge_committed` and `segment_splits`/`segment_merges`/`segment_map_refreshes` stop counting the publication that never happened.

  No reachable path changes. Getting there takes about 2^31 splits on one stream, or a hand-edited descriptor. **Recommend: accept.** No backward-compatible alternative is correct: the old answer is the false closure itself. The only other variant, clearing the intent but returning `false`, still strands a closed parent under a live map.
- **D2 (scope).** Should step 1 also refuse an unnumberable transition at phase A? That means `execute_split_fenced` declines when the allocator cannot supply 2 more ids, and `execute_merge_fenced` when it cannot supply 1. The parent then never seals and the range stays writable. **Recommend: do it in step 2,** where the Transition type owns "ids this transition allocates" next to `validate`. The alternative is a small step 1b now.

---

## Skeptic corrections (C1..C6)

Method: I re-read every quoted line at HEAD `fba7b844`. I confirmed that `artifacts/*.head.rs` are byte-identical to the tree (`cmp`) and traced all four reds through `topology.rs`, `segmap.rs`, `registry.rs:1148-1233` (`mutate_incarnation`) and `registry.rs:1259-1305` (`cas_update` → `StreamDesc::try_from` → `SegmentMap::validate`).

I ran standalone `rustfmt --edition 2024` on scratch copies in `plans9/skeptic-fmt/`. No repo file was touched and no cargo was run. HEAD `topology.rs` came back unchanged, which confirms the formatting config.

**Confirmed as written:**
- Both `Err(_)` arms (`topology.rs:535-540`, `:616-621`).
- The `pending_matches` argument (`:484-490`, `:600-606`).
- `registry.rs:1173` Decline exit.
- `split`'s up-front allocation (`segmap.rs:303-305`) and the unchecked merge `+= 1` (`segmap.rs:398`), plus the second unchecked add at `topology.rs:502` (debug panic column 32 is right).
- The 3a/3b red outputs: file:line:col, `left`/`right` Debug text, the version arithmetic 1→2 and 3→4, and `split_at` = 9223372036854775808. The segmap red at `:612:9` and the debug red at `:398:9`.
- `cargo test --release` in gate.sh:17 and ci.yml:98, with `[profile.release]` having no overflow-checks. So the release red is authoritative.
- The reds are bounded: Decline returns at once, seals are single oneshot closes, and no scaler loop runs in `http_rig_build`.
- The planted maps pass `validate()` and `try_from`: the leaves 1 and 2 cover the keyspace, seg 2's seal metadata is complete, and its predecessor 0 is sealed.
- Neither `topology.rs` nor `segmap.rs` matches `CRITICAL_PREFIXES` (`verification_plan.py:22-31`) or has a row in `mutation_owners.py`, and none is forced (no A/D/R). No mutants are selected.
- `exception_growth` (`source_rules.py:204-216`) keys on the full attribute text (`:196`), so deleting the three topology expects is ungated and the C3 re-decided reasons are new identities. Both reasons match `"[^";]+;[^";]+;[^";]+"`.
- No source-allowances, owners, architecture-policy, WIRE-MATRIX or mt-audit row names these scopes.
- test-inventory is 504 entries today, all from `src/dst`, so 507 after C2 is right.
- No doc under `docs/` states the "idempotent completion" contract.

- **C1 (fix the artifacts: they are not rustfmt-stable).** rustfmt's chain_width breaks `d.segments.as_mut().filter(|m| m.pending.as_ref() == Some(&p))`. The chain is 62 characters, over the width of 60; HEAD's own `.is_some_and(...)` chain at `topology.rs:484-487` shows the same break. The formatted let-else is 5 lines at both sites (`topology.c1.rs:469`, `:582`). Effects:
  - **File length:** topology.rs is **597** after C1 and **618** after C2, not 589 and 610.
  - **Counted body lines, `resume_incarnation`:** **85** after C1 and **95** after C2, not 81 and 91. It is still ≤ 100, but the margin is 5 lines, so clippy's `too_many_lines` has to be run.
  - **Counted body lines, `resume_merge`:** 49 after C1 and 53 after C2.
  - **Debug red for test 2 after C1:** the location is `src/application/topology.rs:484:28`, not `:480:28`.

  Copy the rustfmt-formatted files from `plans9/skeptic-fmt/`: `topology.c1.rs`, `topology.c2.rs`. `segmap.c3.rs` and `topology_scaling.c2.rs` are already fmt-stable. Section 4's line table needs the same update.
- **C2 (`redundant_clone` risk in `dst_block.rs:28`, `:30` and `:73`).** `Cargo.toml:153` enables `redundant_clone = "warn"`, and clippy runs with `-D warnings`, `--all-targets`, with no test exemption for this lint.
  - `fresh_desc(..).await.segments.clone().unwrap()` clones a field of a temporary that is dropped straight after. Likewise `desc.segments.clone().unwrap()` at `:73`, where `desc` is never used again.
  - The lint accepts field projections of an ADT without `Drop`, and `StreamDesc` has no `Drop` impl.
  - **Fix:** use `fresh_desc(state, name).await.segments.unwrap()` and `desc.segments.unwrap()`, which move out of the owned value. This keeps every line number, so the 793:5 and 834:5 reds are unchanged.
  - `planted.clone()` inside the `cas_update` closure is fine, because it clones through a `&`.
- **C3 (section 8's reason for deferring the tails is wrong; show the deferral to Søren).** The reviewer put "delete the identical tails" in step 1. Collapsing `topology.rs:312-315` and `:392-395` does **not** need `let _ =` (`let_underscore_must_use`).
  - After `.map(..).unwrap_or(false)` the value is a `bool`, which is not `#[must_use]`, and `unused_results` is not enabled (`Cargo.toml:116-120`).
  - A discarded bool statement is already house style: `request_work.rs:93`, `super::topology::resume_fenced(&service, &stream, &epoch).await;`.
  - A cleaner option is `if let Err(error) = st.registry.mutate_incarnation(..).await { tracing::warn!(?error, ..) }`, which also stops swallowing the phase-A `MutationError`. It is a log-only behaviour change.
  - **Either** do it in C1 as a pure refactor: drop `let ok =` and both `if !ok { return … }` lines, which is −6 lines and no ratchet (topology.rs has no exception after C1). **Or** keep the deferral, fix its reason, and add it as **D3** in section 9, so the dropped reviewer item is a recorded decision and not a silent one.
- **C4 (the use-site list in section 1d is incomplete; no behaviour impact).** Missing entries:
  - The `#[cfg(test)]` forwarders `scaler3.rs:466-475` (`execute_split`), `:477-491` (`execute_split_fenced`), `:495-502` (`execute_merge`) and `:505-511` (`resume`).
  - The DST callers `topology_lifecycle.rs:54` (`execute_split_fenced`), `topology_lifecycle.rs:382` (`scaler3::resume`), `scaler_controller.rs:95` (`resume_fenced`), and 39 `execute_split`/`execute_merge` call sites across `src/dst`.

  I checked that none of them can reach a `MapError`. The only planted `PendingTransition` literals are `request_topology_debt.rs:55` (valid split on an initial map, and `split` succeeds) and `registry/tests.rs:104` (validation only). Every other intent comes from phase A, which already checks liveness, bounds and adjacency. No existing test depended on the `Err` arm returning `true`.
- **C5 (the log sits inside a closure documented as side-effect-free).** `mutate_incarnation`'s contract (`registry.rs:1128-1137`) says `decide` is pure and is re-run after precondition conflicts. The new `tracing::error!` calls are safe only because each one is followed by `return Mutation::Decline(false)`, which exits on the first call (`registry.rs:1173`). Add that reason to the comment above each log ("logged on the Decline path only: decide is re-run after a conflict, a Decline never is"), so a later edit cannot move a log onto a `Write` path and emit it up to 5 times.
- **C6 (control 2 note).** Control 2 runs `cargo fmt --all`, so it would silently reshape C1 (see C1 above). Also check with `git diff --stat` that the fmt pass touched only `topology.rs`.
  - `docs/refactor/test-scenario-map.json:1214-1232` records segmap test line numbers, which are already stale (e.g. `merge_adjacent_only` 486 vs actual 574). C3 shifts them by 3 more. No script validates those lines (`scenario-map-report.py`), so no ledger change is needed; this is listed only so nobody "fixes" it.

**Unbuildable controls:** none. Every command exists: gate.sh, quality.sh, `test-inventory.py --check`/`--write`, and `verification_plan.py --out` with the `QUALITY_*` environment read in `common.py:24-100`. The multi-filter `cargo test` line is valid libtest usage.

**Missed ledgers:** none. `verification.json` and `architecture-review-baseline.json` carry file hashes but are not read by any gate script (`architecture-gate.py` pins only the baseline file's own digest). `review-mechanisms.json` and `test-additions.json` pin no `topology_scaling` test.

**Verdict: ready-with-corrections.** Apply C1 and C2 before copying the artifacts. Resolve C3 by either doing it or adding D3. C4, C5 and C6 are text and comment fixes.
