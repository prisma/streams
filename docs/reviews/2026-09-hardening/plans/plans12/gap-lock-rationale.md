# Item 90, step 1: gap_lock states the reasons it still has

Tree: `slate` @ `aaf2baa5` (4 commits ahead of `origin/slate` = `6ef3bc64`).
Scope: step 1 only. Rewrite the `gap_lock` doc, and delete or correct the comments that
repeat its stale rationale. The lock's scope does not change: all 65 acquisitions stay.
Steps 2 and 3 are out of scope (§8).

---

## 1. Problem (verified on the current tree)

### 1.1 The doc gives three reasons that no longer exist

`src/dst/tests/fixture_failpoints.rs:3-9`, verbatim:

```rust
/// ONE serialization lock for every test that arms a GLOBAL failpoint
/// registry or reads a global parked-counter (fork_failpoints,
/// crate::failpoints). The registries are keyed per stream name, but
/// the parked COUNTERS are process-global: two parallel tests waiting
/// on "count changed" can wake on each other's parks (the reported
/// solo-pass/parallel-flake family). Serializing the armers makes the
/// suite parallel-green without weakening any assertion.
```

Each claim checked against the tree:

- **`fork_failpoints` does not exist.** `grep -rn fork_failpoints src` finds only this doc line.
  The name survives only in historical docs (`docs/DST.md:673`,
  `docs/dst/IMPLEMENTATION-PLAN.md:175`, `docs/dst/DST-EXPANSION-SPEC.md:710`). Commit
  `7da5c6db` ("#108(a): ONE typed failpoint registry") replaced it with `src/failpoints.rs`.
- **The parked counters are per name, not process-global.** In `src/failpoints.rs`, arrivals
  live in the `(Fp, String)`-keyed registry entry:
  - `:221` `arrivals: usize,` sits inside `FpState`.
  - `:244` reads `type Registry = Mutex<HashMap<(Fp, String), FpState>>;`.
  - `:279-285` `parked(fp, name)` returns `.get(&(fp, name.to_string())).map_or(0, |st| st.arrivals)`.
  - The module doc says so itself (`:29-33`): "arrivals per-(Fp, name) — a test observes ITS
    request in the window by ITS stream name. The old per-failpoint global counters were the
    parallel-flake family".
  - `arm`, `release`, `is_armed`, `is_held`, `hit`, `pause` and `pause_oneshot` (`:256-371`)
    are all keyed by `(fp, name.to_string())`.
  - The only per-point shared state is the `ACTIVE` fast-path count (`:236`) and the `gate()`
    `Notify` (`:251`). `ACTIVE` gates only whether the slow path runs. A `notify_waiters` from
    another name causes a spurious wakeup, and the woken task re-checks its own name
    (`:333-337`, `:365-369`).
- **"Serializing the armers" does not describe the suite.**
  - Method: a per-test-fn scan of `failpoints::(arm|park_|arm_|stop_…)(` (excluding `_off`)
    and `rig_in_seal_gap(`, checked against `gap_lock()` in the same body.
  - Result: of the 65 test fns that arm a failpoint, **46 hold the lock and 19 arm without
    it**, in the same parallel suite.
  - Isolation holds anyway because every armed name is distinct. The tree has 64 literal
    `(point, name)` arm sites and none is duplicated; the only non-literal arms are the
    helper parameters `rig_in_seal_gap(stream)`, `park_fork_after_source_ref(name)` and
    `park_create_before_ready(name)`.

  | file | tests that arm without the lock |
  | --- | --- |
  | `consumer_generations.rs` | `failpoint_registry_is_enumerable_and_described` (arms `PullBeforeReceive "fp-enum-a"`; the same point is armed by the holder `consumer_delete.rs::a_parked_pull_…` under `"qc16p"`) |
  | `livefeed_basics.rs` | `livefeed_singleton_large_window_delivers_everything_exactly_once`, `livefeed_handoff_feed_advances_between_subscribe_and_session_start`, `livefeed_ring_wrap_during_initial_handoff_recatchups`, `livefeed_fork_foreign_only_window_is_progress` |
  | `livefeed_engine_retired.rs` | `livefeed_engine_retired_before_a_drive_is_a_typed_read_cutoff` |
  | `livefeed_history.rs` | `livefeed_cursor_now_uses_the_reconciled_source` |
  | `security_noninterference.rs` | `dab_seal_intent_crash_leaves_foreign_stream_intact` (StopAfterSealIntent, also armed by gap_lock holders) |
  | `src/sse/feed/tests.rs` | `driver_permit_releases_exactly_once` |
  | `security_revocation.rs` | `revocation_interrupts_active_direct_delivery`, `queued_frame_is_discarded_and_unbilled_at_revocation`, `livefeed_shared_feed_revocation_gates_one_subscriber_only`, `livefeed_source_swap_revocation_yields_zero_post_cutoff_frames` (SseBeforeSend and SseBodyBeforeYield; SseBeforeSend is the point the gap_lock holder `token_expiry_…` arms) |
  | `livefeed_swap.rs` | `livefeed_split_during_initial_catchup_delivers_everything`, `livefeed_split_seal_before_refresh_drains_then_terminates`, `livefeed_raw_late_attach_after_swap_gets_no_lineage_scalars`, `livefeed_raw_swap_between_peek_and_attach_is_refused` (includes SseBeforeLeaseGate, which is also armed by `security_subscription.rs::lineage_sse_refuses_…`, a holder) |
  | `livefeed_tail.rs` | `livefeed_no_up_to_date_while_backlog_is_undriven`, `livefeed_late_attach_never_claims_stale_up_to_date` |

The same claim also appears without the "serialize" wording. For example, the
`failpoint_registry_is_enumerable_and_described` doc in `consumer_generations.rs:707-722`
says per-name isolation is "the property whose absence was the parallel-flake family".

### 1.2 History of why the lock existed (git)

- **`6e0af451` (2026-07-30) introduced `gap_lock`**, with no doc, next to the
  process-global `crate::scaler3::failpoints` (`arm_before_publish()` /
  `release_before_publish()`). The `7da5c6db` message says that failpoint "armed EVERY
  resume in the process and release_before_publish() woke every parked test". Any test that
  split or merged (any resume) was exposed, which is why most split/merge/lineage tests hold
  the lock.
- **`7da5c6db` (#108(a)) deleted the scaler failpoint** and parked per parent stream name. Its
  message says "gap_lock stays — its other users serialize on OpenGate/global-counter state,
  not failpoints". That attribution was wrong. The OpenGate process counters were serialized
  by a *different* lock, `gate_lock` (`git show 7da5c6db:src/dst/dst_tests.rs`, `:2672`,
  `:2685`, `:2935`, …, each followed by `OpenGate::reset_counters_for_tests()`).
- **`a1cf29f3` (2026-09-23, already on `origin/slate`) deleted the OpenGate statics and
  `gate_lock`.** Its message: "the gate_lock that serialized four tests around the statics are
  gone". The current `src/sharddir.rs` has no `static` counters; stats are per gate
  (`gate.stats_json()`).

### 1.3 What the lock really protects today

1. **The exact delta on `LEASE_TERMINATIONS`.** `src/sse/auth.rs:61-62` defines one process
   counter array (`pub(crate) static LEASE_TERMINATIONS: [AtomicU64; 10]`). It is incremented
   only by `TerminateOnce::record_once` (`:160`). `src/dst/tests/sse_delivery.rs:315-332`
   (`termination_reasons_count_exactly_once_per_subscription`) reads
   `LEASE_TERMINATIONS[TokenExpired]` before and after two subscriptions expire, and asserts
   `after - before == 2`. Any concurrent `TokenExpired` termination breaks that assertion.

   Every short-lived credential in `src/` belongs to a gap_lock holder:
   - `mint_token(…, 5)` at `sse_delivery.rs:306` and `security_revocation.rs:174`
   - `exp: now + 4` at `security_subscription.rs:460`
   - `sr2_workload_jwt_exp(…, now + 4)` at `security_workload.rs:43`

   Other mints are ≥ 120 s. The three movers already say this accurately
   (`security_revocation.rs:160-161`, `security_subscription.rs:353-354`,
   `security_workload.rs:18-19`): "LEASE_TERMINATIONS is process-global; serialize with the
   other expiry tests that move it (gap_lock convention)."

   `security_freshness.rs` (`v01_*`, two holders) records `PolicyStale`/`GrantsStale` through
   `LeaseWatch` on a `ManualClock`. Those are other indices, and no test reads a delta on them,
   so these two are inherited holders.
2. **Machine share for the capacity ratio.** `topology_scaling.rs:297-302`
   (`post_split_throughput_scales`) says: "A capacity RATIO is only valid when the measurement
   owns the machine: run serialized against the other heavy tests". In CI this test is skipped
   from the parallel suite (`.github/workflows/ci.yml:104`,
   `cargo test --release -- --skip post_split_throughput_scales`) and runs alone
   (`ci.yml:120`, `scripts/test-leg.sh … --exact dst::dst_tests::topology_scaling::post_split_throughput_scales`;
   the same in `scripts/gate.sh:17,34` and `scripts/release-gate.sh:80,83`). The lock therefore
   matters only for an unskipped local `cargo test`, where it keeps the other 64 holders off
   the machine.

No gap_lock holder asserts on any other process-global state. A per-body scan found only
three kinds of access, none of them shared:
- `crate::sse::auth::sse_stats::FEED_LAG_DISCONNECTS`, read at
  `persistence_faults.rs:481` for a failure message only.
- `state.livefeed.budget().set_max_for_test`, which is per rig (`src/sse/feed/test_support.rs:19`:
  "per-rig — every AppState builds its own budget").
- `state.runtime.history.paused`, which is per runtime.

The `FaultStore` fault schedule (`src/dst/fault_store.rs`) is per instance and seeded, with no
statics.

### 1.4 Every use site (65 acquisitions in 32 files; 102 `gap_lock` references = 65 + 32 imports + 1 definition + 4 comment lines)

The reviewer's "~100 sites" counts references. There are 65 acquisitions.
`grep -rn "gap_lock().lock()" src | wc -l` gives 65 and `grep -rn gap_lock src | wc -l` gives 102.

Class legend:
- **R**: reads the `LEASE_TERMINATIONS` delta
- **M**: moves `TokenExpired`
- **C**: capacity ratio
- **F**: inherited, and arms per-name failpoints (named isolation already)
- **N**: inherited, and arms no failpoint at all

| site | test | class | comment on the line |
| --- | --- | --- | --- |
| admission_maintenance.rs:405 | split_child_sheds_while_sibling_child_admits | N | |
| admission_memory.rs:467 | frame_debt_survives_restart_via_tail_seed | N | `// shared failpoint schedule` (false: no failpoint, no FaultStore) |
| consumer_delete.rs:239 | a_parked_pull_cannot_lease_after_its_generation_was_deleted | F | |
| consumer_generations.rs:370 | a_parked_saga_never_touches_a_recreated_stream | F | |
| consumer_product.rs:420 | product_consumer_drains_lineage_across_split | N | |
| consumer_saga.rs:48, :159, :225 | a_saga_refresh_failure_is_retryable_not_stable, a_saga_vanished_collection_is_idempotent_success, a_saga_never_finalizes_under_a_pending_transition | F ×3 | |
| durability_failures.rs:328 | a_fence_in_a_failed_group_reports_failure_not_closed | F | |
| durability_fences.rs:212 | a_lower_takeover_reservation_cannot_install | F | |
| fork_cleanup.rs:26 | a_crashed_creators_late_reference_is_repaired_by_delete_retry | F | |
| fork_lifecycle.rs:294, :409, :816 | a_crashed_fork_cascade_can_be_resumed, fork_creation_and_source_deletion_serialize, r05_unknown_child_state_preserves_its_source_reference | F ×3 | |
| fork_lifecycle.rs:660 | fork_lifecycle_is_idempotent_and_epoch_checked | N | |
| lifecycle_creation.rs:218 | create_replay_recovers_from_a_failed_initial_write | F | |
| lifecycle_incarnation.rs:26, :172, :293, :426, :633 | (5 incarnation-race tests) | F ×5 | |
| livefeed_ownership.rs:240 | livefeed_pending_transition_connect_drives_resume | F | |
| livefeed_ownership.rs:573 | livefeed_seal_retry_is_one_task_per_feed_at_fanout | F | `// global failpoint registry` (misleading) |
| livefeed_swap.rs:503 | livefeed_split_held_publication_handoff_is_prompt | F | |
| livefeed_tail.rs:190 | livefeed_seal_publication_race_converges_without_heartbeat | F | `// global failpoint registry` (misleading) |
| persistence_faults.rs:364 | cut_resume_never_skips_a_durable_record | N | `// shared failpoint schedule` (false: its `FaultStore::uniform(mem(), 41, …)` is per instance) |
| producer_protocol.rs:585 | product_producer_hash_survives_split | N | |
| reads_product.rs:212, :675 | product_read_follows_split_lineage, product_scan_traverses_split_lineage | N ×2 | |
| reads_raw.rs:17, :65, :89, :162, :182, :222 | seal_gap_* (6) | F ×6 | |
| reads_raw.rs:551 | raw_route_is_the_default_key_view_across_splits | N | |
| scaler_controller.rs:15 | r09_scaler_cancellation_and_deadlines_preserve_intent_and_rotate_work | F | |
| seal_convergence.rs:112 | a_seal_interrupted_before_publication_resumes_on_retry | F | `// global failpoint registry` (misleading) |
| seal_coordination.rs:236 | topology_transitions_are_fenced_by_sealing | N | |
| seal_coordination.rs:327, :681 | a_parked_split_cannot_publish_under_a_sealed_collection, a_crashed_raw_final_close_is_resumed_by_an_ordinary_retry | F ×2 | |
| seal_fencing.rs:33, :162, :361, :446, :560 | (5 fencing tests) | F ×5 | |
| seal_incarnation.rs:18, :182 | a_product_seal_never_binds_…, a_product_final_never_writes_… | F ×2 | |
| seal_recovery.rs:24, :267, :572 | (3 recovery tests) | F ×3 | |
| security_freshness.rs:102, :137 | v01_owned_time_refreshes_…, v01_owned_time_staleness_… | N ×2 | |
| security_revocation.rs:162 | token_expiry_interrupts_slowly_progressing_delivery | M (+F `SseBeforeSend "ex1"`) | leading comment, accurate |
| security_subscription.rs:355 | subscription_terminates_at_token_expiry | M | leading comment, accurate |
| security_subscription.rs:721 | lineage_sse_refuses_authorization_invalidated_before_body_construction | F | section header `:678` "The failpoint registry is global; gap_lock serializes armers." (misleading) |
| security_workload.rs:20 | raw_sse_terminates_at_workload_token_expiry | M | leading comment, accurate |
| sse_delivery.rs:47 | sse_follows_lineage_across_split | N | |
| sse_delivery.rs:302 | termination_reasons_count_exactly_once_per_subscription | R | `// process-global counters` (true but vague; reads as the parked counters the old doc named) |
| topology_lifecycle.rs:220, :418, :531 | merge_phase_b_declines_under_sealing, concurrent_finals_…, a_child_deleted_before_… | F ×3 | |
| topology_scaling.rs:182, :453 | split_children_land_on_distinct_engines, merge_rejoins_cold_children_with_exact_lineage | N ×2 | |
| topology_scaling.rs:302 | post_split_throughput_scales | C | leading comment `:297-301`, accurate for local runs |

Totals: R 1 + M 3 + C 1 + F 45 + N 15 = 65.

### 1.5 The comment lines to fix

| line | text | verdict |
| --- | --- | --- |
| `livefeed_ownership.rs:573` | `let _serial = gap_lock().lock().await; // global failpoint registry` | delete the comment |
| `seal_convergence.rs:112` | `let _serial = gap_lock().lock().await; // global failpoint registry` | delete the comment |
| `livefeed_tail.rs:190` | `let _serial = gap_lock().lock().await; // global failpoint registry` | delete the comment |
| `persistence_faults.rs:364` | `let _l = gap_lock().lock().await; // shared failpoint schedule` | delete the comment |
| `admission_memory.rs:467` | `let _l = gap_lock().lock().await; // shared failpoint schedule` | delete the comment |
| `security_subscription.rs:677-678` | `//` then `// The failpoint registry is global; gap_lock serializes armers.` | delete both lines |
| `sse_delivery.rs:302` | `let _serial = gap_lock().lock().await; // process-global counters` | rewrite in place to name the counter |

Kept, because they are accurate:
- the three "LEASE_TERMINATIONS is process-global; … (gap_lock convention)" comments
- `topology_scaling.rs:297-301`

### 1.6 Where the reviewer is wrong

- The count is 65 acquisitions, not "~100 sites" (§1.4).
- "Red-first isolation unit test in src/failpoints.rs arming one Fp for two names" **cannot be
  red**. The registry has been per `(Fp, name)` since #108 (quoted in §1.1). The test is a
  *pin* for the claim the new doc rests on, and it is green on this tree. It is included here
  as commit 1, with negative controls in place of a red run (§3.1).
- The capacity-ratio reason only applies to local unskipped runs. CI runs the test alone.
  The new doc says so.

---

## 2. Contract decision

- **No wire change and no metric change.** Every edited file is test-only:
  - `src/failpoints.rs` is `#![cfg(test)]` at `:17`.
  - `src/dst/**` is `#[cfg(test)] mod dst` (`src/lib.rs`) and `#[cfg(test)] mod dst_tests`
    (`src/dst/mod.rs:157`).

  `LEASE_TERMINATIONS`, `/v1/debug` `lease_terminations` (`src/http.rs:867`), `/metrics`,
  status codes and bodies are untouched.
- **No synchronization change.** All 65 `gap_lock().lock().await` sites stay byte-identical
  apart from trailing comments. So Loom and held-commit tests are not applicable.
- **The typed contract is documentation only.** `gap_lock`'s doc now names its two owners:
  - the `LEASE_TERMINATIONS` exact-delta reader and every test whose subscription can die at
    token expiry;
  - the capacity ratio in unskipped local runs.

  It also states that failpoints are isolated by stream name, pinned by
  `failpoints::a_point_armed_for_one_name_never_reaches_another`.

---

## 3. Tests: pins and compile-level proofs (no behaviour change, so no red test)

### 3.1 New pin: `failpoints::a_point_armed_for_one_name_never_reaches_another` (`src/failpoints.rs`, commit 1)

The test appends to the end of `src/failpoints.rs`. The file is 560 lines today and becomes
585; the panic line numbers below assume the block exactly as written here (blank line at `:561`, `#[test]` at `:566`).

```rust

/// The per-name contract that lets a failpoint test run without the
/// DST `gap_lock`: arming, arrivals and release are keyed by (point,
/// stream name), so one test's name can neither park, flag nor release
/// another's.
#[test]
fn a_point_armed_for_one_name_never_reaches_another() {
    use futures_util::FutureExt;
    let fp = Fp::AppendBeforeEnqueue;
    let (a, b) = ("fp-iso-a", "fp-iso-b");
    arm(fp, a);
    assert!(hit(fp, a), "{a} must see its own arm");
    assert!(!hit(fp, b), "arming {a} armed {b}");
    assert!(
        pause(fp, b).now_or_never().is_some(),
        "{b} parked on the arm of {a}"
    );
    assert_eq!(parked(fp, b), 0, "{b} counted an arrival for {a}");
    arm(fp, b);
    release(fp, a);
    assert!(hit(fp, b), "releasing {a} disarmed {b}");
    assert!(pause(fp, b).now_or_never().is_none(), "{b} must park");
    assert_eq!(parked(fp, b), 1, "{b} counts its own arrival once");
    release(fp, b);
}
```

**Trace on the current tree (green):**

- `arm(fp,a)` (`:256-263`) creates the entry `(fp,"fp-iso-a")`, sets `armed=true`, and moves
  `ACTIVE` +1.
- `hit(fp,a)` returns true, via `active` and then `is_armed` → `get(&(fp,"fp-iso-a"))`.
- `hit(fp,b)` returns false: `get(&(fp,"fp-iso-b"))` finds `None`.
- `pause(fp,b)` on its first poll: `active(fp)` is true, but `get_mut(&(fp,"fp-iso-b"))` is
  `None`, so it `return`s (`:322-324`). `now_or_never()` gives `Some(())`, and nothing was
  counted, so `parked(fp,b)` is 0.
- `arm(fp,b)` creates b's entry, and `ACTIVE` goes to 2.
- `release(fp,a)` clears only a's entry (`:266-272`), and `ACTIVE` goes to 1. `hit(fp,b)` is
  still true.
- `pause(fp,b)`: b is armed, so `arrivals` becomes 1, `notified()` is taken, `is_armed` is
  still true, and `n.await` is Pending. `now_or_never()` gives `None` and drops the future.
  `parked(fp,b)` is 1.
- `release(fp,b)` takes `ACTIVE` to 0 and calls `notify_waiters`. Any other test's parked
  tasks get a spurious wakeup and re-park on their own names.

The names `fp-iso-*` are used by no other test (§1.1 pair list), so running in parallel is safe.

**Negative controls.** These are manual, and must be reverted and never committed. They show
the pin fails with a predictable message when the property is broken:

- **A: point-global flag, the shape of the pre-#108 scaler failpoint.** In `hit`
  (`src/failpoints.rs:308`), replace `is_armed(fp, name)` with `true`. The expected failure:
  ```
  thread 'failpoints::a_point_armed_for_one_name_never_reaches_another' panicked at src/failpoints.rs:573:5:
  arming fp-iso-a armed fp-iso-b
  ```
- **B: "release everything" (the failpoints.rs doc at `:35-37` forbids it).** In `release`,
  clear `armed` and `held` on every entry whose point is `fp`, not just `(fp, name)`. At
  `:581`, `hit(fp,b)` is then false (`ACTIVE` is still >0 because the control does not
  decrement it, and `is_armed(b)` is false). The expected failure:
  ```
  thread 'failpoints::a_point_armed_for_one_name_never_reaches_another' panicked at src/failpoints.rs:581:5:
  releasing fp-iso-a disarmed fp-iso-b
  ```

### 3.2 Existing pins that must stay green

- `dst::dst_tests::consumer_generations::failpoint_registry_is_enumerable_and_described`.
  It covers the weaker, arrivals-only form of the same property.
- `dst::dst_tests::sse_delivery::termination_reasons_count_exactly_once_per_subscription`.
  This is the owner the doc names; the only change is its trailing comment.
- The 19 failpoint-arming tests in §1.1 that run without the lock in the parallel CI suite.
  They are the empirical proof that name isolation, not the lock, keeps failpoint tests apart.

### 3.3 Compile-level proofs for commit 2 (comment-only)

- **Every changed Rust line is either a comment or a `//` suffix after unchanged code.**
  The C4 stripped-code diff over the 8 commit-2 source files (7 test files plus
  `fixture_failpoints.rs`) prints `code-identical` for each. The 5 trailing deletions and the
  1 in-place rewrite keep their code prefix byte-identical, and `security_subscription.rs`
  loses only two `//` lines.
- **Hashes are comment-neutral.** `scripts/test-inventory.py::canonical_tokens` skips `//` to
  end of line, and its self-test asserts this. So `function_sha256` is unchanged for all 517
  inventoried tests. `--check` before `--write` reports only `changed configuration` (§7),
  never `changed function_sha256`.
- **The mechanism pin is unchanged.** `review-evidence --check` stays OK. The pinned
  `cut_resume_never_skips_a_durable_record` after-hash `b895148d…` in
  `docs/refactor/review-mechanisms.json:1040` is a `function_sha256`, so the comment deletion
  does not change it.

---

## 4. Edits, file by file, in commit order

Ceilinged files (current `wc -l`, budget = no growth). **None is touched:**

| file | lines |
| --- | --- |
| `http.rs` | 3,155 |
| `product.rs` | 4,205 |
| `shard.rs` | 3,186 |
| `billing.rs` | 2,157 |
| `history.rs` | 1,713 |
| `auth.rs` | 1,676 |
| `registry.rs` | 1,492 |
| `sse/feed.rs` | 1,165 |
| `fleet.rs` | 1,142 |

Every touched file is far below 1,000 lines, so no verbatim-move commit is needed.

### Commit 1: pin the per-name contract

`Arming one stream name can neither park, flag nor release another; the per-name failpoint contract is pinned`

- `src/failpoints.rs` (560 → 585): append the block in §3.1 after the final `}` at `:560`.
  - Commit 1 is otherwise optional, but if it is dropped, delete the test-name clause from
    the `gap_lock` doc in commit 2.
  - rustfmt: the layout above follows default `fn_call_width = 60`. Only the first `pause`
    assert (args width 69) goes vertical; the others are 35-54 wide. Run `cargo fmt --all`
    and accept its layout if it differs. The panic line numbers in §3.1 then shift
    accordingly.
- Ratcheted scopes touched: **none**.
  - The file's only `#[expect]` is `clippy::too_many_lines` on `Fp::site` (`:108-111`). Its
    item scope is unchanged, and the exception identity is `(path, qualified, kind, value)`
    with no line numbers.
  - The new test uses no `unwrap`/`expect` (`clippy.toml` `allow-unwrap-in-tests` would
    allow them anyway), no statics, no spawns and no env reads.
  - It stays under 100 lines with nesting of 1. `futures_util` is a normal dependency
    (`Cargo.toml:34`), and `use futures_util::FutureExt;` inside the fn matches
    `src/runtime.rs:592`.
- mt_lint and multitenancy-audit: no `name: String` parameters, no `.stream_ref(`, no
  `registry.get("…")`, and no `HashMap<String` in scanned files. The audit baseline has no
  line numbers.

### Commit 2: rewrite the doc and the misleading comments

`gap_lock names the two owners it still serves; the failpoint-registry reasons and their comments go`

1. **`src/dst/tests/fixture_failpoints.rs`** (31 → 42): replace `:3-9` with:

   ```rust
   /// Serializes the tests that share process-global state a stream name
   /// cannot key. Two owners remain. `crate::sse::auth::LEASE_TERMINATIONS`
   /// is one counter per process, and
   /// `termination_reasons_count_exactly_once_per_subscription` asserts an
   /// exact `TokenExpired` delta, so every test whose subscription can die
   /// at token expiry holds this lock. `post_split_throughput_scales` holds
   /// it so an unskipped local run keeps the other holders off the machine
   /// while it measures a capacity ratio; CI skips that test in the
   /// parallel suite and runs it alone.
   ///
   /// Failpoints are not a reason: `crate::failpoints` keys arming,
   /// arrivals and release by (point, stream name), pinned by
   /// `failpoints::a_point_armed_for_one_name_never_reaches_another`, so a
   /// failpoint test is isolated by a stream name no other test arms, and
   /// several arm without this lock. The other holders inherited it from
   /// state that is gone (the scaler failpoint that parked every resume in
   /// the process, then process-wide parked counters); each keeps it until
   /// a loop run shows it needs no timing isolation.
   ```

   - The function body, the `static L` and `FailpointGuard`/`sweep_lock` are unchanged.
   - The `source-allowances.json:1795-1801` global row (`crate::gap_lock::L`, syntax
     `std :: sync :: OnceLock < tokio :: sync :: Mutex < () > >`) has no line or doc
     component, so it is unchanged.
   - This code is not built by rustdoc (it is `cfg(test)`); the text still avoids `[..]` and
     `<..>`.
2. **`src/dst/tests/sse_delivery.rs:302`** (769 lines, unchanged): change
   `let _serial = gap_lock().lock().await; // process-global counters` to
   `let _serial = gap_lock().lock().await; // exact LEASE_TERMINATIONS delta below`
   (82 columns). The new text does not match the inventory `configuration` regex
   (`FaultPlan::|failpoint|\.require\(|\bseed\b|start_paused|worker_threads`).
3. **`src/dst/tests/livefeed_ownership.rs:573`** (751 lines, unchanged): change to
   `    let _serial = gap_lock().lock().await;` (delete ` // global failpoint registry`).
4. **`src/dst/tests/seal_convergence.rs:112`** (548 lines, unchanged): same deletion.
5. **`src/dst/tests/livefeed_tail.rs:190`** (561 lines, unchanged): same deletion.
6. **`src/dst/tests/persistence_faults.rs:364`** (519 lines, unchanged): change to
   `    let _l = gap_lock().lock().await;` (delete ` // shared failpoint schedule`).
7. **`src/dst/tests/admission_memory.rs:467`** (548 lines, unchanged): same deletion.
8. **`src/dst/tests/security_subscription.rs`** (790 → 788): delete `:677` (`//`) and `:678`
   (`// The failpoint registry is global; gap_lock serializes armers.`). The block then ends
   `// initial control and NO data — on every SSE surface.` followed by `// ====…`.
9. **`docs/refactor/test-inventory.json`**: regenerate with
   `python3 scripts/test-inventory.py --write` (§6).

Ratcheted scopes touched in commit 2:

- **`cut_resume_never_skips_a_durable_record`** carries `#[expect]` for
  `clippy::disallowed_methods`, `clippy::too_many_lines` and `clippy::excessive_nesting`
  (`persistence_faults.rs:350-361`).
  - The edit removes a trailing comment only. `scope_lines` (`end_line - line + 1`),
    `nested_items` and `syntax_facts` are all unchanged, because the parsed facts exclude
    comments.
  - `too_many_lines` stays fulfilled: clippy counts code lines, and `:364` remains one.
  - Remedy: none.
- **The other five edited test functions** carry no `#[expect]`: only `#[tokio::test(...)]`
  sits directly above each fn, at `livefeed_ownership.rs:571-572`, `seal_convergence.rs:110-111`,
  `livefeed_tail.rs:188-189`, `admission_memory.rs:465-466` and `sse_delivery.rs:300-301`.
- **`security_subscription.rs:677-678`** is module-level text. No file-wide `#![expect]`
  exists in any touched file. Exception identities carry no line numbers, so the −2 shift
  below it does not matter.
- `gap_lock` has no `#[expect]`.

---

## 5. Mutation analysis

- **No mutation source changes.**
  - `src/failpoints.rs` and `src/dst/tests/*.rs` are under no `CRITICAL_PREFIXES`
    (`scripts/quality/verification_plan.py:22-31`) and are sources of no owner in
    `scripts/quality/mutation_owners.py`. `src/dst` appears there only inside test-name
    *filters* such as `'dst_tests::sse_delivery::'` (`:145-147`).
  - `plan()` therefore puts none of these paths in `mutation_source_files` (`:88-91`).
  - `src/failpoints.rs` also has a real `#![cfg(test)]`.
- **No function bodies change**, apart from the new `#[test]` fn, which cargo-mutants never
  mutates, and comments, which generate no mutants.
- **In-diff mutants from these two commits: 0.** No owner rows or filters change.
- The push's plan still carries whatever the four unpushed commits select (`src/billing/…`,
  `src/offsets.rs`, `src/http/…`, …). These commits add nothing to it (§7, C6).

---

## 6. Ledgers (commit 2 only)

- **`docs/refactor/test-inventory.json`**: `--write`. `function_sha256` is comment-neutral,
  but `configuration` lists raw body lines matching `failpoint`. So exactly five entries drop
  a line that was never failpoint configuration, only a misleading comment. The expected
  diff is **1 insertion, 7 deletions**:
  - `a_seal_interrupted_before_publication_resumes_on_retry` (`:973`),
    `livefeed_seal_publication_race_converges_without_heartbeat` (`:3335`) and
    `livefeed_seal_retry_is_one_task_per_feed_at_fanout` (`:3350`) each lose
    `"let _serial = gap_lock().lock().await; // global failpoint registry",`.
  - `cut_resume_never_skips_a_durable_record` (`:1959`) loses
    `"let _l = gap_lock().lock().await; // shared failpoint schedule",`.
  - `frame_debt_survives_restart_via_tail_seed` (`:2426-2434`) changes as follows:
    ```
    -    "configuration": [
    -      "let _l = gap_lock().lock().await; // shared failpoint schedule"
    -    ],
    +    "configuration": [],
    ```
- **`docs/refactor/test-inventory-before.json`: do not touch.** It is the frozen 410-test
  extraction baseline. Its `--compare` is a documented historical command
  (`docs/review-tests-evidence.md:30`), not a CI gate: CI runs only `--self-test` and
  `--check` (`ci.yml:55-56`). It already diverges for unrelated reasons (for example,
  `cut_resume` `ffc97e75…` vs current `b895148d…`).
- **`docs/refactor/review-mechanisms.json`**: no change. Pins are `function_sha256`
  (comment-neutral). The `v01_*` and `cut_resume` pins are untouched.
- **`docs/quality/source-allowances.json`, `legacy-source.json`, `owners.json`,
  `architecture-policy.json`, `WIRE-MATRIX.md`, scenario map and dispositions,
  `src/dst/tests/README.md`**: no change. There are no new or deleted statics, modules,
  renames or wire changes. The `crate::gap_lock::L` rows remain valid.

---

## 7. Controls

Run these only after the in-tree mutation run and gate run have finished. `$S` is the
scratchpad. All commands run from `/Users/sorenschmidt/code/streams`.

- **C0: preflight.**
  - `git status --short` should print nothing.
  - `python3 scripts/test-inventory.py --check` should print
    `test-inventory: OK (517 tests, 0 ignored)`. That count is at `aaf2baa5`; it follows HEAD.
- **C1: commit 1.**
  `cargo test --locked --release --lib -- --exact failpoints::a_point_armed_for_one_name_never_reaches_another dst::dst_tests::consumer_generations::failpoint_registry_is_enumerable_and_described`
  should print two `... ok` lines and `test result: ok. 2 passed; 0 failed`.
  Optionally run controls A and B from §3.1 and check the exact panics. Revert with
  `git checkout -- src/failpoints.rs`, then re-apply the §3.1 block.
- **C2: fmt and clippy after each commit.**
  - `cargo fmt --all -- --check` should be clean.
  - `cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > $S/clippy90.jsonl`
    followed by `python3 scripts/quality/gate.py --clippy $S/clippy90.jsonl` should exit 0,
    with no new diagnostic and no `accepted exception grew`.
- **C3: inventory (commit 2).**
  - Run `python3 scripts/test-inventory.py --check` *before* `--write`. It should print
    exactly these lines, in this order, and exit 1:
    ```
    a_seal_interrupted_before_publication_resumes_on_retry: changed configuration
    cut_resume_never_skips_a_durable_record: changed configuration
    frame_debt_survives_restart_via_tail_seed: changed configuration
    livefeed_seal_publication_race_converges_without_heartbeat: changed configuration
    livefeed_seal_retry_is_one_task_per_feed_at_fanout: changed configuration
    ```
    There should be no `changed function_sha256`; that is the comment-neutrality proof.
  - `python3 scripts/test-inventory.py --write` should print `test-inventory: wrote 517 tests`.
  - `python3 scripts/test-inventory.py --check` should print
    `test-inventory: OK (517 tests, 0 ignored)`.
  - `git diff --stat -- docs/refactor/test-inventory.json` should show
    `1 insertion(+), 7 deletions(-)`.
  - `python3 scripts/review-evidence.py --check` should print
    `review-evidence source inventory: OK; execution and external acceptance require receipts`.
  - `python3 scripts/scenario-map-report.py --check` should pass unchanged.
- **C4: text invariants.**
  - `grep -rn "gap_lock().lock()" src | wc -l` should give **65** (the lock's scope is
    unchanged).
  - `grep -rnE "global failpoint registry|shared failpoint schedule|process-global counters|fork_failpoints|gap_lock serializes armers" src`
    should print nothing. On `aaf2baa5` it prints exactly the 8 target lines:
    `fixture_failpoints.rs:4`, `sse_delivery.rs:302`, `livefeed_ownership.rs:573`,
    `admission_memory.rs:467`, `security_subscription.rs:678`, `persistence_faults.rs:364`,
    `livefeed_tail.rs:190` and `seal_convergence.rs:112`.
  - Code identity, with comments and blank lines stripped:
    ```sh
    for f in $(git diff --name-only HEAD~1 -- src); do
      diff <(git show HEAD~1:$f | sed -E 's#[[:space:]]*//.*$##' | grep -v '^[[:space:]]*$') \
           <(sed -E 's#[[:space:]]*//.*$##' $f | grep -v '^[[:space:]]*$') >/dev/null \
        && echo "$f: code-identical" || echo "$f: CODE CHANGED"
    done
    ```
    This should print `code-identical` for all 8 commit-2 source files.
- **C5: sanity on the edited tests.**
  `cargo test --locked --release --lib -- --exact dst::dst_tests::sse_delivery::termination_reasons_count_exactly_once_per_subscription dst::dst_tests::livefeed_ownership::livefeed_seal_retry_is_one_task_per_feed_at_fanout dst::dst_tests::seal_convergence::a_seal_interrupted_before_publication_resumes_on_retry dst::dst_tests::livefeed_tail::livefeed_seal_publication_race_converges_without_heartbeat dst::dst_tests::persistence_faults::cut_resume_never_skips_a_durable_record dst::dst_tests::admission_memory::frame_debt_survives_restart_via_tail_seed dst::dst_tests::security_subscription::lineage_sse_refuses_authorization_invalidated_before_body_construction`
  should give `test result: ok. 7 passed`. The tests serialize among themselves on
  `gap_lock`, which is expected.
- **C6: CI's plan before push.**
  - First, `cargo build --locked -p streams-quality-syntax`.
  - Then `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) python3 scripts/quality/verification_plan.py --out $S/plan90`.
  - `changed_rust_files` should include the nine item-90 paths: `src/failpoints.rs` plus the
    8 commit-2 files.
  - `mutation_source_files` should contain **none** of `src/failpoints.rs` or
    `src/dst/tests/*`. Its remaining contents are the preceding unpushed commits' own
    selection.
- **CI.** Claim green only from `gh run view` on the pushed SHA.

---

## 8. Out of scope

- **Step 2** (give the four `LEASE_TERMINATIONS` tests their own lock) is **not trivially
  safe**. Those four tests have 4-5 s token windows; `security_revocation.rs:163-167` calls
  the window "timing-sensitive … on a loaded runner". Today they never overlap the other 61
  heavy holders; a dedicated lock would let them. It needs loop runs.
- **Step 3** (drop the lock from inherited holders one at a time, starting with `reads_raw.rs`)
  needs loop testing per module.
- The duplicated module doc in `src/failpoints.rs`: `:1-16` and `:19-40` both open "The
  SEMANTIC failpoint registry (#108…" and repeat "new failpoints add a variant + a helper
  pair".
- The stale "three surface legs" wording at `security_subscription.rs:689-692`: one leg
  remains in that file (`:789`).
- Other stale fixture text:
  - the `// ---- R27-2 …` section header above `sweep_lock` (`fixture_failpoints.rs:24`);
  - the `FailpointGuard` doc's "leak into sibling tests" (releases are per name);
  - the historical `http::fork_failpoints` mentions in `docs/DST.md:673` and `docs/dst/*`.
- The inaccurate "OpenGate/global-counter" attribution in the `7da5c6db` message. That is
  history, not code.

---

## 9. Decisions for Søren

None. There is no edge, wire, metric or policy change: the edits are test-only
documentation, one test-only pin, and a regenerated test inventory.

---

## Skeptic corrections (C1..C7)

Everything below was checked read-only against `aaf2baa5`. These claims hold as written:
- the quoted doc at `fixture_failpoints.rs:3-9`, and `failpoints.rs:17/221/236/244/256/279/304-308`;
- the 65 acquisitions and 102 references across 33 files (32 importers + the definition);
- the exact eight target lines for the C4 grep, and every file length in §4. The ceilinged
  files are untouched, and none is over its `origin/slate` size;
- the 19 lock-free arming tests: an independent per-body scan reproduced the §1.1 table exactly,
  and 46 holders arm a failpoint;
- mints: only `mint_token(…, 5)` at `sse_delivery.rs:306` and `security_revocation.rs:174` are
  short. `new_checked` and `revoked_with_clock` are called only from `sse/session.rs` and
  `security_freshness.rs`;
- the one reader of a `LEASE_TERMINATIONS` delta is `sse_delivery.rs:317/326`. The only
  non-test reader is `/v1/debug` (`http.rs:867` → `sse/auth.rs:75-81`);
- the other `sse_stats` delta reads (`livefeed_ownership.rs:91-172, 334-365`, `livefeed_swap.rs:417-423`)
  are all in non-holder tests;
- the inventory regex and the `1+/7-` diff. None of the four one-line removals is the last
  array element, so no comma churn, and `frame_debt…` becomes `[]`. `--check` ordering is
  sorted by name. The count is 517 with 0 ignored;
- the inventory scans only `src/dst` (`test-inventory.py:138`), so commit 1 needs no inventory
  row;
- review-mechanisms pins and support functions are `function_sha256` (comment-neutral).
  No `fixture_failpoints` helper is pinned;
- no `#![expect]` in any touched file. `Fp::site` is the only ratcheted scope in
  `failpoints.rs`, and the appended fn is outside it (`source_rules.py:133-158` picks the
  smallest enclosing item);
- `cut_resume`'s three expects keep `scope_lines`, `nested_items` and `syntax_facts`, because
  a trailing `//` is not a fact;
- mutation: neither `src/failpoints.rs` nor `src/dst/tests/*` matches `CRITICAL_PREFIXES`
  (`verification_plan.py:22-31`) or any `mutation_owners.py` source. 0 in-diff mutants;
- `docs/quality/verification.json:605,679` hashes both `fixture_failpoints.rs` and
  `failpoints.rs`, but its `scope_note` calls it a "Historical adoption receipt" and no
  script or workflow reads it. **Not a ledger to update.**

The pin trace is correct.
- The second `pause(fp,b)` stays bounded under a parallel suite. A foreign `notify_waiters`
  between `notified()` and the first poll only re-loops once more (`counted` is already true),
  so `now_or_never()` still gives `None`, and `parked(b)` is 1.
- The panic lines 573:5 and 581:5 match the block as written, and rustfmt keeps that layout.
  The first `pause` assert's args are 69 wide, over 60, so it goes vertical; the others are
  60 or less.

**C1: the new doc (and §1.1:51-54) states the isolation condition too weakly.** Failpoints
are keyed by *stream name*, so any request for that name that reaches the site parks or
flags. It does not matter which test armed it. The condition is therefore "a stream name no
other concurrently running test *uses*", not "no other test *arms*".
- The tree already has one overlap. `"fpx"` is armed lock-free with `StopAfterSealIntent`
  (`security_noninterference.rs:496`). `"fpx"` is also the fork *source* stream of the
  lock-free `livefeed_basics.rs:806-827` (`/v1/streams/fpx`, `stream-forked-from: fpx`).
- It is benign today: the only hit site is the close/seal path (`application/append/close.rs:207`),
  and that livefeed test never closes `fpx`. gap_lock would not protect it either, because
  neither test holds the lock.
- Fix: in the doc, replace "isolated by a stream name no other test arms" with
  "isolated only by a stream name that no other test uses".
- In §1.1, replace "every armed name is distinct" with "no armed literal is armed twice.
  One armed name (`fpx`) is also used, un-armed, by a lock-free livefeed test on a path the
  flag never reaches".
- Optionally list renaming one of the two `fpx` streams in §8.

**C2: "`LEASE_TERMINATIONS` is one counter per process" is inaccurate.** It is a
`[AtomicU64; 10]` per-reason array (`sse/auth.rs:62`). Write: "is a process-global per-reason
counter array, and `termination_reasons_…` asserts an exact `TokenExpired` delta on it".

**C3: the capacity-owner sentence misses an unskipped runner.** `scripts/release-provenance.sh:34`
runs `cargo test --release` with no `--skip`, so the lock matters there as well as in a local
`cargo test`.
- In the doc, change "so an unskipped local run keeps…" to "so any unskipped run (a local
  `cargo test`, `scripts/release-provenance.sh`) keeps…".
- Also fix §1.3 item 2 and §1.6 bullet 3 ("CI runs the test alone" is true for `ci.yml`,
  `gate.sh` and `release-gate.sh` only).

**C4: the same stale rationale survives in a quality ledger the plan omits.** The reason on
`docs/quality/owners.json:560` (the `tokio::spawn` effect row for
`crate::livefeed_pending_transition_connect_drives_resume`) reads "shared failpoint serialization
and release guard bound the fault". After commit 2, that directly contradicts the gap_lock doc.
- The reason is free text. `source_gate.py:46-48` only requires it to be non-empty, and the
  row identity is (category, owner, path, syntax), so rewording it changes no gate.
- In commit 2, reword it to: "Test-only orphaned transition; the per-name failpoint and its
  release guard bound the fault; the parked split is explicitly aborted and its cancellation
  joined before recovery." Keep exactly two ';' and no '"'.
- Or move it to §8 explicitly.
- Also add to §8, with no edit, since these are history or parallel wording:
  - `docs/RELEASE-PRODUCT-SURFACE.md:778-780` ("every test arming a global failpoint registry
    … serializes on one lock"), a preview.3 release note;
  - the `SseGateGuard` doc at `security_subscription.rs:681-682` ("never leak into sibling
    tests"), which is the twin of the `FailpointGuard` wording already listed.

**C5: the pin arms the append hot-path point.** `Fp::AppendBeforeEnqueue` is paused on every
append (`failpoints.rs:414-420`). While `fp-iso-a` or `fp-iso-b` is armed lock-free, `ACTIVE`
is greater than 0. Every concurrent append in the process then takes the global-mutex + key-allocation
slow path, and the `ACTIVE` doc (`:229-235`) records that a throughput gate noticed exactly
that cost.
- The window is microseconds, and the capacity test is skipped in the parallel suite, so this
  is low risk.
- Even so, prefer a park point that is off the append and read hot path, such as
  `Fp::InitBeforeSeed` or `Fp::CloseBeforeMark`.
- The trace, panics and line numbers are unchanged, because `hit`, `pause`, `parked` and
  `release` do not depend on which `Fp` is used.

**C6: the control A/B revert step is order-dependent.** "`git checkout -- src/failpoints.rs`,
then re-apply the §3.1 block" is right only while the pin is uncommitted. Run the controls
*after* committing commit 1, so the checkout restores the committed pin. Then check
`git diff --quiet -- src/failpoints.rs`, and drop "re-apply".

**C7: re-anchor the panic lines on the committed file.** C1's expected panic lines (573:5 and
581:5) hold only if the block is appended byte-for-byte after `:560` with one blank line. If C5
changes the `Fp` or rustfmt reflows anything, read the lines from the committed file:
`grep -n 'armed {b}"\|disarmed {b}"' src/failpoints.rs`. Do not use the numbers in §3.1.

No unbuildable controls. C0-C6 use existing CLIs with the stated flags:
- `gate.py --clippy`;
- `verification_plan.py --out` with `QUALITY_EVENT_NAME`/`QUALITY_BEFORE_SHA`;
- `test-inventory.py --check/--write`;
- `review-evidence.py --check`, whose OK text matches `:323`.

The C4 loop needs a shell with process substitution. The zsh and bash used here both have it.

**Verdict: ready-with-corrections.** Required:
- C1 and C2, wording in the doc being written;
- C4, one ledger reason line, or an explicit §8 entry.

Recommended: C3, C5, C6 and C7. None of them changes scope, ceilings, exception ratchets or
mutation selection. No contract decision is needed.
