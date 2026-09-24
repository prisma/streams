# Item 78: DST engine open sequence, restart boundary, sized append builder

Tree: `slate` at `24c4c77a4303e9b4b7a6cb09bfee26453efad657` (= origin/slate, clean). All line numbers below are from this tree and were re-found by content.

Scope: test harness only (`src/dst/tests/*`). No production source, no product/raw edge, no `/v1/debug` or `/metrics` shape changes.

Three commits, each with its own `scripts/test-inventory.py --write`:

1. C1: fixture substitution. `open_engine_with_settings` hands back the absorb-signal receiver. A new `open_engine_with_signals` covers the production-mirrored timing. `open_engine_with_absorber_layout` folds into `open_engine_with_absorber`. 29 of the 37 open sequences go away.
2. C2: the 7 `begin_close(); sleep(..)` pairs become `await_terminated(30 s)`.
3. C3: `append_sized` and `append_n` share one private request builder (`append_entries`).

---

## 1. Problem (verified on the current tree)

### 1a. The open sequence is copy-pasted 37 times in `src/dst` (REAL)

`grep -rn "ShardEngine::start" src tools fuzz bench benches tests` finds 70 lines: 5 are `reason =` strings on `ShardEngine::start` itself (`src/shard.rs:1324-1341`), which leaves 65 call sites. 37 are in `src/dst`, and none are in `tools/`, `fuzz/`, `bench/` or `tests/`; `bench/docker/LADDER-LOG.md` only mentions it in prose. The other 28 are `src/bootstrap.rs:512`, `src/billing_service.rs:544`, `src/history.rs` ×4, `src/history/*_tests.rs` ×4, `src/shard/*_tests.rs` ×16 and `src/sharddir/{holdoff,unwind}.rs` (both `#[cfg(test)]`). Those 28 are out of scope (§8).

Each DST copy is the same 22 to 25 lines. Quote (`src/dst/tests/history_recovery.rs:348-372`):

```rust
        let db = slatedb::Db::builder("dst-bigrec", store.clone() as Arc<dyn ObjectStore>)
            .with_settings(slatedb::config::Settings {
                flush_interval: Some(std::time::Duration::from_millis(5)),
                manifest_poll_interval: std::time::Duration::from_millis(50),
                ..Default::default()
            })
            .build()
            .await
            .expect("open db A");
        let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
        // R25-A: tests use the REAL load path — a fresh DB rebuilds to
        // zero; a reopened DB restores its durable backlog, exactly as
        // the production opener does.
        let __maint = crate::shard::load_or_rebuild_maintenance(&db)
            .await
            .expect("load maintenance");
        let engine_a = crate::shard::ShardEngine::start(
            "dst-bigrec".to_string(),
            Arc::new(db),
            store.clone(),
            crate::shard::ShardConfig::default(),
            absorb_tx,
            None,
            __maint,
        );
```

The `R25-A: tests use the REAL load path` comment appears 32 times in `src/dst/tests`.

**Why the fixture was bypassed.** `fixture_storage.rs:64` drops the receiver (`let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();`). Every scenario that starts its own `Absorber` on the engine's signals therefore had to re-inline the whole sequence.

**The settings have drifted.** The DST tree has five `Settings` shapes:

- `{flush 5 ms, manifest_poll 50 ms, ..Default}`: 53 literal occurrences, all byte-identical.
- implicit `Settings::default()` (no `with_settings`): `billing_maintenance.rs:237,687,765` and `reads_ring.rs:395`.
- `flush 600 s`: `reads_applied.rs:310`.
- the idle-poll production posture: `runtime_open_gate.rs:332`.
- the derived `Some(flush_interval)` inside `open_engine_cfg`.

**Pass-through (REAL).** `fixture_storage.rs:92-97`:

```rust
pub(super) async fn open_engine_with_absorber(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
) -> (Arc<crate::shard::ShardEngine>, tokio::task::JoinHandle<()>) {
    open_engine_with_absorber_layout(store, prefix).await
}
```

`open_engine_with_absorber_layout` (`:99-136`) has exactly one caller, line 96. It is itself a full fifth copy of the sequence with the 5/50 literal.

**The existing fixture callers restate the default too.** `open_engine_with_settings` has 10 external callers: `history_gather.rs:41,63,96,152,293,735` and `producer_protocol.rs:85,135,204,261`. Nine of them pass exactly the 5/50 literal with a default config (`:735` adds `shared_history`), which is what `open_engine`/`open_engine_cfg` already derive. Only `history_gather.rs:152` (`dst-est`, `Settings::default()`) has its own timing.

**Full DST site list and disposition.** "Same" means: db path == engine prefix, same store object for db and engine, `on_close = None`, and nothing else happens between `build()` and `start` (checked by script).

| # | Site (start line) | Test / fn | Settings | ShardConfig | Receiver | C1 disposition |
|---|---|---|---|---|---|---|
| 1 | history_recovery.rs:34 | idle_stream_handles_evict_and_reload | 5/50 | default | unused | `open_engine` |
| 2 | history_recovery.rs:127 | a_second_absorption_wave_trims_under_a_global_budget | 5/50 | `{max_trim_per_op 65_536, trim_global_budget BUDGET}` | own absorber | `open_engine_with_signals(cfg)` |
| 3 | history_recovery.rs:287 | budget_deferred_streams_absorb_on_the_next_tick | 5/50 | default | own absorber | `open_engine_with_signals` |
| 4 | history_recovery.rs:364 | a_large_record_absorbs_after_restart_under_default_policy (A) | 5/50 | default | unused | `open_engine` |
| 5 | history_recovery.rs:396 | same test (B) | 5/50 | default | own absorber | `open_engine_with_signals` |
| 6 | history_recovery.rs:462 | dirty_scan_retries_until_it_succeeds (A) | 5/50 | default | unused | `open_engine` |
| 7 | history_recovery.rs:495 | same test (B) | 5/50 | default | own absorber | `open_engine_with_signals` |
| 8 | history_recovery.rs:573 | sparse_records_rediscovered_after_restart_are_absorbed (A) | 5/50 | default | unused | `open_engine` |
| 9 | history_recovery.rs:603 | same test (B) | 5/50 | default | own absorber | `open_engine_with_signals` |
| 10 | history_recovery.rs:678 | pending_summary_clears_on_shard_close | 5/50 | default | own absorber | `open_engine_with_signals` |
| 11 | history_recovery.rs:759 | handle_capacity_cap_evicts_oldest_first | 5/50 | default | unused | `open_engine` |
| 12 | history_gather.rs:373 | v2_gather_packs_to_the_aggregate_budget | 5/50 | default | unused | `open_engine` |
| 13 | history_gather.rs:451 | an_oversized_chunk_gathers_alone | 5/50 | default | unused | `open_engine` |
| 14 | history_gather.rs:513 | keyed_frames_no_longer_count_twice_against_the_budget | 5/50 | default | unused | `open_engine` |
| 15 | history_gather.rs:603 | untouched_streams_absorb_after_restart (A) | 5/50 | default | unused | `open_engine` |
| 16 | history_gather.rs:640 | same test (B) | 5/50 | default | own absorber | `open_engine_with_signals` |
| 17 | history_absorption.rs:111 | absorber_sweep_recovers_streams_whose_signals_were_lost | 5/50 | default | `drop(engine_rx)` before start | `open_engine` (drops it at return) |
| 18 | history_absorption.rs:194 | absorber_drains_records_larger_than_the_per_stream_gather_cap | 5/50 | default | own absorber | `open_engine_with_signals` |
| 19 | history_absorption.rs:303 | v2_absorbs_without_customer_keys | 5/50 | default | absorber `{1, 1 ms, 20 ms, ..Default}` | `open_engine_with_absorber` (identical absorber config) |
| 20 | history_absorption.rs:442 | tiny_residuals_age_absorb_and_cannot_starve_the_progress_latch | 5/50 | default | own absorber | `open_engine_with_signals` |
| 21 | reads_history.rs:151 | sparse_key_reads_page_with_bounded_spans | 5/50 | default | own absorber | `open_engine_with_signals` |
| 22 | reads_history.rs:265 | corrupt_postings_fall_back_to_the_envelope | 5/50 | default | own absorber | `open_engine_with_signals` |
| 23 | reads_history.rs:389 | repeated_keyed_reads_hit_the_postings_cache | 5/50 | default | own absorber | `open_engine_with_signals` |
| 24 | reads_raw.rs:389 | oversized_keyed_record_pages_through | 5/50 | default | own absorber | `open_engine_with_signals` |
| 25 | reads_raw.rs:468 | long_keyed_run_pages_with_progress | 5/50 | default | own absorber | `open_engine_with_signals` |
| 26 | reads_applied.rs:326 | a_lost_applied_suffix_rewinds_to_the_durable_frontier | flush 600 s | `{wal_group_commit: false}` | unused | `open_engine_with_settings(.., literal)` |
| 27 | reads_ring.rs:403 | keyed_tail_reads_serve_from_ring | **absent** | `{tail_ring_bytes: 1 MiB}` | unused | `open_engine_with_settings(.., Settings::default())` (explicit) |
| 28 | runtime_open_gate.rs:356 | idle_engine_store_traffic_is_bounded_by_the_poll_cadence | idle posture | `{wal_group_commit: true}` | unused | `open_engine_with_settings(.., literal)` |
| 29 | fixture_storage.rs:119 | open_engine_with_absorber_layout | 5/50 | default | fast absorber | folded into `open_engine_with_absorber` |
| 30 | fixture_storage.rs:71 | open_engine_with_settings | param | param | — | **the one sequence (kept)** |
| 31 | billing_maintenance.rs:245 | absorbed_boundary_and_maintenance_retire_atomically | absent | default | own absorber, failpoint armed before start | **leave**: db path `"dst-matomic/shard"` differs from prefix `"dst-matomic"` (reviewer: leave the matomic path) |
| 32 | billing_maintenance.rs:718 | legacy_rows_are_rebuilt_and_legacy_tails_repaired_on_open | absent | default | unused | **leave**: the raw db and `maint` are asserted before start |
| 33 | billing_maintenance.rs:777 | ownership_handoff_moves_backlog_without_aba | absent | default | unused | **leave**: `maint_b` is asserted before start (the "B loads the durable backlog" claim) |
| 34 | fixture_http.rs:297 | rig_opener | 5/50 | param | fast absorber | **leave**: `{prefix}/shard`, park point, `Some(on_close)`, `?` errors; pinned in `review-mechanisms.json` `fixture_changes` |
| 35 | reads_history.rs:52 | the_first_advance_seals_the_history_layout | 5/50 | default | unused | **leave**: plants a legacy tail into the db before the maintenance load |
| 36 | runtime_open_gate.rs:108 | naive_get_or_open | 5/50 | default | unused | **leave**: non-panicking open that counts `Fenced` (the storm under test) |
| 37 | runtime_retirement.rs:29 | open_engine_with_on_close | 5/50 | default | unused | **leave**: `Some(on_close)` |

After C1, `src/dst` has 8 `ShardEngine::start` sites: the fixture core plus 7 shapes that are genuinely different.

### 1b. The restart boundary is `begin_close()` followed by a sleep (REAL, exactly 7)

`grep -rn -A4 "begin_close()" src/dst` finds 53 `begin_close()` calls. Exactly 7 are followed by a sleep:

| Site | Test | Pair |
|---|---|---|
| billing_maintenance.rs:328-329 | absorbed_boundary_and_maintenance_retire_atomically | `engine.begin_close(); sleep(200 ms)` then raw reopen of `"dst-matomic/shard"` |
| billing_maintenance.rs:683-684 | legacy_rows_are_rebuilt_and_legacy_tails_repaired_on_open | `engine.begin_close(); sleep(200 ms)` then raw reopen of the prefix |
| billing_maintenance.rs:788-789 | ownership_handoff_moves_backlog_without_aba | `engine_a.begin_close(); sleep(300 ms)` then assert B untouched. This is a late-shutdown boundary, not a restart. |
| history_recovery.rs:376-377 | a_large_record_absorbs_after_restart_under_default_policy | `engine_a.begin_close(); sleep(200 ms)` |
| history_recovery.rs:472-473 | dirty_scan_retries_until_it_succeeds | same |
| history_recovery.rs:583-584 | sparse_records_rediscovered_after_restart_are_absorbed | same |
| history_gather.rs:618-619 | untouched_streams_absorb_after_restart | same |

`history_recovery.rs:713-717` (`pending_summary_clears_on_shard_close`) is a poll of close's observable effect, not a pair. It stays.

`await_terminated` already exists (`src/shard.rs:1764-1773`, `#[cfg(test)]`):

```rust
    /// Observe the engine's one owned shutdown. Timeout/cancellation only
    /// stops this observer; workers and storage closure retain their owner.
    #[cfg(test)]
    pub(crate) async fn await_terminated(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(), String> {
        self.begin_close();
        self.shutdown_handle().wait(timeout).await
    }
```

`EngineShutdown::wait` (`src/shard/lifecycle.rs:105-121`) returns `Err` on timeout or on any role that did not finish. The storage-close role closes the shard db and history partition (`lifecycle.rs:53-74`). `close_db` maps a `Fenced` close to `Ok` (`history_partition.rs:153`), so a fenced predecessor still terminates `Ok`. `reads_applied.rs:410-413` already relies on that: `"the fenced engine terminates once closed"`.

The DST idiom is already `await_terminated(std::time::Duration::from_secs(30)).await.expect(..)` (`durability_gather.rs:80-83`, `runtime_isolation.rs:146`, `reads_applied.rs:304,409`). The contract is pinned by `src/shard/task_lifecycle_tests.rs` (`r17a_*`, lines 190-295: `!termination_complete()` before, `termination_complete()` after).

### 1c. The sized append request is duplicated (REAL)

`fixture_storage.rs:204-238` (`append_sized`) and `:265-301` (`append_n`) build the same 21-field `AppendReq` literal. The only differences are `entries` and the routing key: `rk` versus `""` in `derive_subkey`, `routing_key` and `key_hash`. The enqueue and ack await are identical.

Also, the `append_sized` doc comment is stranded on `absorb_through` at `:188-189` ("Direct append of a payload of chosen size ...").

Leave as the reviewer said: the other `AppendReq` literals at `durability_fences.rs:408,700`, `reads_history.rs:182,289,412` (JSON-keyed), `history_absorption.rs:228` (per-record fills), `read_page_limits.rs:148`, `read_subset_retention.rs:251`, `producer_protocol.rs:33` and `src/dst/runtime.rs:199,274` are different request shapes.

---

## 2. Contract decision

The product/raw edge, debug and metrics surfaces are untouched. These are fixture-API and test-harness decisions only.

- **D1 (reviewer, kept).** `open_engine_with_settings(store, prefix, cfg, settings)` returns `(Arc<ShardEngine>, tokio::sync::mpsc::Receiver<crate::shard::AbsorbSignal>)`. It stays the only open sequence (db build → `absorber_channel` → `load_or_rebuild_maintenance` → `ShardEngine::start(.., None, maint)`).
- **D2 (refinement of the reviewer's Change).** Add `open_engine_with_signals(store, prefix, cfg)` for the production-mirrored timing plus the receiver. The derivation moves verbatim out of `open_engine_cfg`. `open_engine_cfg` becomes `open_engine_with_signals(..).await.0`; the no-absorber view has 11+33 callers, and its API is unchanged.
  - Why not the reviewer's literal-everywhere form? Every one of the 14 own-absorber sites has settings equal to the mirrored default. Routing them through `open_engine_with_settings` would restate the 5/50 literal 14 more times, which is the drift this item removes, and costs about 5 lines per site.
  - Explicit settings remain only where a scenario owns its timing: sites 26, 27 (explicit `Settings::default()`, where it was absent) and 28, plus `dst-est`.
- **D3.** Sites whose settings equal the mirrored default call `open_engine` / `open_engine_cfg` / `open_engine_with_signals`. The same applies to the 9 existing `open_engine_with_settings` callers that restate the default: the return-type change forces an edit on them anyway, and switching is the substitution this item asks for.
- **D4.** `open_engine_with_absorber_layout` is deleted. Its body becomes `open_engine_with_absorber` built on `open_engine_with_signals(.., ShardConfig::default())` with the unchanged fast-absorber config.
- **D5.** Restart and handoff boundary: `x.await_terminated(std::time::Duration::from_secs(30)).await.expect("..")`. It is bounded and calls `begin_close` itself, so no separate `begin_close()` is needed. The ABA handoff test goes from "300 ms after A's close began" to "after A's owned shutdown completed". That strengthens it without changing what it asserts.
- **D6.** Private `append_entries(engine, hash, key, rk, entries) -> u64` in `fixture_storage.rs`. This is the reviewer's `sized_req`, renamed because it also owns the identical enqueue and ack await; a pure request builder would leave that pair duplicated. It has 5 args, which is at the limit and not over. `append_sized` and `append_n` keep their signatures.

### Why every substituted test keeps its meaning

- **Settings.** `ShardConfig::default().wal_group_commit == false` (`src/shard.rs:1067`). None of the substituted configs set `wal_group_commit`: site 2 sets only trim fields, and `absorber_on_pool` sets only `shared_history`. The mirrored settings are therefore `Settings { flush_interval: Some(5 ms), manifest_poll_interval: 50 ms, ..Default::default() }`, token-identical to all 53 literals, which were checked by script to contain nothing else.
  - The implicit default at site 27 equals an explicit `Settings::default()`. `DbBuilder::new` initialises `settings: Settings::default()`, and `with_settings` only assigns it; its warnings fire only with a `compactor_builder` or `gc_builder`, and neither is set (slatedb `0717cc1` `db/builder.rs:206-255`).
- **Path, prefix, store, `on_close`.** Checked per site by script: identical. An `Arc<FaultStore>` argument coerces to `Arc<dyn ObjectStore>` at the call. The same object reaches db and engine, as before.
- **Receiver lifetime.** Where an unused `_absorb_rx` used to live until the end of the scope, the fixture now drops it at return, which is exactly what `open_engine` already does for its 33 callers. The receiver's only producer is `let _ = self.absorb_tx.try_send(s);` (`src/shard.rs:3033`), which ignores `Full` and `Closed` alike. `AbsorbSignal` is plain data (`src/shard.rs:963-966`, `hash` and `appended_bytes`). Site 17 dropped its receiver before `start`; the fixture drops it before returning. No append happens in between in either case.
- **The only other textual difference is `expect` messages** ("open db A", "maint", "open idle db"), which are visible only on failure.

---

## 3. Red tests, pinning tests and non-vacuity controls

All test paths below are `dst::dst_tests::<module>::<name>`. The lib crate is `streams-slate`, and `cargo test --lib` reaches them.

### C1: pure refactor

**Pins.** The 30 changed tests must pass, and so must every other DST test, because the internals of `open_engine`, `open_engine_cfg` and `open_engine_with_absorber` changed:

- history_recovery: `idle_stream_handles_evict_and_reload`, `a_second_absorption_wave_trims_under_a_global_budget`, `budget_deferred_streams_absorb_on_the_next_tick`, `a_large_record_absorbs_after_restart_under_default_policy`, `dirty_scan_retries_until_it_succeeds`, `sparse_records_rediscovered_after_restart_are_absorbed`, `pending_summary_clears_on_shard_close`, `handle_capacity_cap_evicts_oldest_first`.
- history_gather: `v2_gather_packs_to_the_aggregate_budget`, `an_oversized_chunk_gathers_alone`, `keyed_frames_no_longer_count_twice_against_the_budget`, `untouched_streams_absorb_after_restart`, `adaptive_gather_estimate_seeds_decays_and_jumps`, `sparse_absorption_wave_bounds_append_latency`.
- history_absorption: `absorber_sweep_recovers_streams_whose_signals_were_lost`, `absorber_drains_records_larger_than_the_per_stream_gather_cap`, `v2_absorbs_without_customer_keys`, `tiny_residuals_age_absorb_and_cannot_starve_the_progress_latch`.
- reads_history: `sparse_key_reads_page_with_bounded_spans`, `corrupt_postings_fall_back_to_the_envelope`, `repeated_keyed_reads_hit_the_postings_cache`.
- reads_raw: `oversized_keyed_record_pages_through`, `long_keyed_run_pages_with_progress`.
- reads_applied: `a_lost_applied_suffix_rewinds_to_the_durable_frontier`.
- reads_ring: `keyed_tail_reads_serve_from_ring`.
- runtime_open_gate: `idle_engine_store_traffic_is_bounded_by_the_poll_cadence`.
- producer_protocol: `stream_seq_is_scoped_to_the_routing_key`, `producer_retries_across_a_split_commit_once`, `stream_seq_resolves_through_predecessors`, `producer_lanes_scoped_per_routing_key`.
- Indirect pins (bodies unchanged): `history_gather::gather_parallel_reads_preserve_outcomes_across_reopen`, `gather_pacing_preserves_outcomes_and_opens_windows` and `two_gathers_outgrowing_a_shared_pool_defer_instead_of_deadlocking`, via `gather_after_reopen`, `gather_with_pacing` and `absorber_on_pool`. Also the 6 `open_engine_with_absorber` callers: `history_absorption::{acked_records_survive_absorption_into_history, v2_history_survives_engine_handoff}`, `reads_history::keyed_catch_up_after_a_cold_index_load_sees_later_absorbed_records`, and `runtime_isolation` ×2.

**Non-vacuity controls.** Each is a temporary edit in `fixture_storage.rs` on the C1 tree; run the named test, see it red, then revert.

- **NV1 (the receiver handed back is the one the absorber consumes).** In `open_engine_with_settings`, return `(engine, crate::history::absorber_channel().1)` instead of `(engine, absorb_rx)`. That receiver's sender is dropped, and the absorber loop returns on `None` (`src/history/worker.rs:64`).
  - Expected red: `reads_history::sparse_key_reads_page_with_bounded_spans` panics `stream [b1, b1] never fully absorbed` after about 8 s.
  - Expected red: `history_absorption::v2_absorbs_without_customer_keys` panics `keyless v2 absorption never advanced — the gather lane still depends on the customer key`.
  - Control: `history_absorption::absorber_sweep_recovers_streams_whose_signals_were_lost` stays green. It never used the engine's receiver.
- **NV2 (the caller's settings reach the db).** Replace `.with_settings(settings)` with `.with_settings(slatedb::config::Settings { flush_interval: Some(std::time::Duration::from_millis(1)), ..Default::default() })`.
  - Expected red: `reads_applied::a_lost_applied_suffix_rewinds_to_the_durable_frontier`. The first failing assertion is one of `no flush inside the window`, `durable reads never see the suffix` or `recovery is the durable frontier`, because the 600 s window collapses to 1 ms.
- **NV3 (the caller's `ShardConfig` reaches the engine).** Pass `crate::shard::ShardConfig::default()` instead of `cfg` to `ShardEngine::start`.
  - Expected red: `history_recovery::a_second_absorption_wave_trims_under_a_global_budget` panics `trim work must be deferred as debt, not done inline in the advance batch`. The default `trim_global_budget` is 65,536 and wave 2 has 4,800 offsets.
  - Expected red: `reads_ring::keyed_tail_reads_serve_from_ring`, because the ring is off by default.
- **NV4 (the mirrored derivation reaches the own-absorber sites).** In `open_engine_with_signals`, change the non-group-commit branch from `from_millis(5)` to `from_secs(600)`. Appends and absorbed groups then become durable only on the engine's 5 s flush ticker.
  - Expected red: `history_recovery::budget_deferred_streams_absorb_on_the_next_tick` panics `budget-deferred streams did not absorb within the tick horizon`. The 1.2 s deadline is below the next 5 s tick. The run takes about 30 s because of the 6 paced appends.
  - A 1 s value is **not** a valid control: the acker pipelines the three boundary groups into one flush, so it can converge in about 1.0 s.

### C2: boundary change (red first)

**Red, the reviewer's first step, not committed.** On the C1 tree, in `history_gather::untouched_streams_absorb_after_restart`, insert right after `engine_a.begin_close();`:

```rust
assert!(engine_a.termination_complete(), "engine A still shutting down when its successor opens");
```

Run `cargo test --locked --release --lib -- --exact dst::dst_tests::history_gather::untouched_streams_absorb_after_restart`.

Expected: `FAILED` with panic `engine A still shutting down when its successor opens`. `begin_close` only starts the owned shutdown; storage close is asynchronous. The 200 ms sleep is a timing bet, not a proof.

**Green.** Replace the pair with `await_terminated(30 s).expect(..)` and move the same `assert!` after it. The test passes. Then delete the `assert!`: `await_terminated` returning `Ok` already means `Phase::Stopped` (`lifecycle.rs:102-104`), and `task_lifecycle_tests.rs` pins that.

**Pins.** The 7 changed tests: `billing_maintenance::{absorbed_boundary_and_maintenance_retire_atomically, legacy_rows_are_rebuilt_and_legacy_tails_repaired_on_open, ownership_handoff_moves_backlog_without_aba}`, `history_recovery::{a_large_record_absorbs_after_restart_under_default_policy, dirty_scan_retries_until_it_succeeds, sparse_records_rediscovered_after_restart_are_absorbed}` and `history_gather::untouched_streams_absorb_after_restart`.

**Non-vacuity (NV5).** In `history_recovery::a_large_record_absorbs_after_restart_under_default_policy`, temporarily use `std::time::Duration::ZERO`.

Expected red: panic `engine A terminates before B reopens the shard: "engine shutdown still running; join authority retained"`. Tokio's `Timeout` polls the observer once, and the shutdown cannot finish synchronously. This proves the call actually waits. Revert.

### C3: pure refactor

**Pins.** Every test that reaches `append_sized` or `append_n`. These are the modules `history_gather` (11), `history_recovery` (8), `reads_history` (5), `reads_raw` (10) and `reads_ring` (6), 40 tests in total. They include `sparse_absorption_wave_bounds_append_latency`, `two_gathers_outgrowing_a_shared_pool_defer_instead_of_deadlocking`, `a_refused_oversized_chunk_defers_and_sizes_the_next_reservation`, `one_corrupt_row_fails_only_its_stream`, `the_first_advance_seals_the_history_layout`, `keyed_catch_up_after_a_cold_index_load_sees_later_absorbed_records` and `o3_retained_ring_coverage_skips_only_the_redundant_marker`.

**Non-vacuity.**

- **NV6 (`rk` reaches the request).** In `append_sized`, pass `""` instead of `rk` to `append_entries`.
  - Expected red: `reads_ring::keyed_tail_reads_serve_from_ring` panics `default lane records only`; it gets `[0, 1, 2, 3]` instead of `[0, 2]`.
  - Also red: `reads_raw::oversized_keyed_record_pages_through` (`exact offsets in order`).
- **NV7 (`entries` reach the request).** In `append_n`, use `(0..n.min(1))`.
  - Expected red: `history_recovery::a_second_absorption_wave_trims_under_a_global_budget` panics `wave-2 boundaries never advanced` after about 30 s, because `absorbed` never exceeds `RECS`.

---

## 4. Edits, file by file, in commit order

The 1,000-line ceiling does not bind: no touched file is over 1,000 lines before or after. The largest is `history_gather.rs` at 922, which shrinks in C1. Measure with `wc -l` after each commit.

Two exception ratchets are touched:

- `#[expect]` scope, with `scope_lines`, `nested_items` and `syntax_facts`. These only shrink in C1 and are untouched in C2 and C3. None of the 7 C2 functions carries an `#[expect]`.
- `too_many_lines` fulfilment (`unfulfilled_lint_expectations` is denied).

### Clippy line estimates for the `#[expect(clippy::too_many_lines)]` scopes C1 shrinks

The counter implements clippy's algorithm over the fn body. It is calibrated at clippy count = counter − 2 on this tree:

- `read_page_limits::r06a_compressed_local_and_peer_pages_have_identical_complete_sequences` counts 102 and carries no expectation, so its clippy count must be ≤100.
- `reads_applied::a_stale_applied_cursor_is_refused_after_crash_restart` counts 103 and carries a fulfilled expectation, so its clippy count must be >100.

| Scope | Clippy est. now → after C1 | Action |
|---|---|---|
| `history_recovery::a_second_absorption_wave_trims_under_a_global_budget` | 117 → **101** | keep; still fulfilled, but the margin is 1. If clippy reports it unfulfilled, delete the `#[expect]` block. Do not pad. |
| `reads_applied::a_lost_applied_suffix_rewinds_to_the_durable_frontier` (also `excessive_nesting`, `disallowed_methods`) | 119 → 108 | keep all three expects; the nesting and spawn sites are untouched; the `owners.json` / `source-allowances.json` `tokio::spawn` effect row stays at count 1 |
| `reads_history::repeated_keyed_reads_hit_the_postings_cache` | 108 → **92** | **delete** its `#[expect(clippy::too_many_lines, ..)]` (4 lines); it is now unfulfilled |
| `runtime_open_gate::idle_engine_store_traffic_is_bounded_by_the_poll_cadence` | 104 → **93** | **delete** its `#[expect(clippy::too_many_lines, ..)]` (4 lines) |
| `history_gather::sparse_absorption_wave_bounds_append_latency` (statement `#[expect(clippy::disallowed_methods)]`; its scope is the whole fn) | shrinks by 9 lines | none |

The `test-inventory` `attributes` field records only single-line `#[..]` attributes, so deleting a multi-line `#[expect(` block leaves `attributes` unchanged. Reasoned expects have no `source-allowances.json` rows (the only `exception` rows are `src/http.rs`, `src/product.rs` and `bench/`), so no `--prune` is needed.

### C1: "DST scenarios open engines through the one fixture sequence, which hands back the absorb-signal receiver"

**`src/dst/tests/fixture_storage.rs`** (301 → about 285). Replace lines 15-136 with the following; the rest of the file is unchanged in C1.

```rust
// ---- scenarios over the real engine ---------------------------------

/// Open the engine WITHOUT an absorber: reads come from the shard log
/// only. Used by scenarios that are about the commit path.
pub(super) async fn open_engine(store: Arc<dyn ObjectStore>, prefix: &str) -> Arc<crate::shard::ShardEngine> {
    open_engine_cfg(store, prefix, crate::shard::ShardConfig::default()).await   // unchanged
}

/// No absorber runs on a commit-path engine, so its signals go nowhere.
pub(super) async fn open_engine_cfg(store, prefix, cfg) -> Arc<crate::shard::ShardEngine> {
    open_engine_with_signals(store, prefix, cfg).await.0
}

/// A scenario that starts its own absorber must feed it the signals this
/// engine sends, under the production-mirrored storage timing.
pub(super) async fn open_engine_with_signals(store, prefix, cfg)
    -> (Arc<crate::shard::ShardEngine>, tokio::sync::mpsc::Receiver<crate::shard::AbsorbSignal>)
{
    // Mirror production: ... (the existing 3-line comment, moved verbatim)
    let flush_interval = if cfg.wal_group_commit { 1 s } else { 5 ms };   // verbatim from open_engine_cfg
    open_engine_with_settings(store, prefix, cfg, slatedb::config::Settings {
        flush_interval: Some(flush_interval),
        manifest_poll_interval: std::time::Duration::from_millis(50),
        ..Default::default()
    }).await
}

/// Preserve a scenario's storage timing while sharing the real maintenance/open path.
/// The receiver comes back with the engine for a scenario that runs its own absorber.
pub(super) async fn open_engine_with_settings(store, prefix, cfg, settings)
    -> (Arc<crate::shard::ShardEngine>, tokio::sync::mpsc::Receiver<crate::shard::AbsorbSignal>)
{
    // lines 59-70 unchanged except `_absorb_rx` → `absorb_rx`
    let engine = crate::shard::ShardEngine::start(prefix.to_string(), Arc::new(db), store, cfg, absorb_tx, None, __maint);
    (engine, absorb_rx)
}

// ---- the tiered read path -------------------------------------------

/// (existing doc of open_engine_with_absorber, lines 84-91, kept)
pub(super) async fn open_engine_with_absorber(store, prefix) -> (Arc<crate::shard::ShardEngine>, tokio::task::JoinHandle<()>) {
    let (engine, absorb_rx) =
        open_engine_with_signals(store, prefix, crate::shard::ShardConfig::default()).await;
    let cfg = crate::history::AbsorberConfig {
        threshold_bytes: 1,
        threshold_age: std::time::Duration::from_millis(1),
        tick: std::time::Duration::from_millis(20),
        ..Default::default()
    };
    let handle = crate::history::Absorber::start(engine.clone(), cfg, absorb_rx);
    (engine, handle)
}
// open_engine_with_absorber_layout: deleted
```

Spell the tuple type out; do not add a type alias item. Run `cargo fmt`.

**Site templates.** Keep every test's own comments and absorber blocks.

- **A (open only):** replace the whole `let db = …` through `ShardEngine::start(…);` block with `let <var> = open_engine(store.clone(), "<prefix>").await;`.
- **C (own absorber):** replace it with the following, keeping the site's receiver name (`absorb_rx`, or `engine_rx` at site 18):

  ```rust
  let (<var>, <rx>) = open_engine_with_signals(
      store.clone(),
      "<prefix>",
      crate::shard::ShardConfig::default(),
  )
  .await;
  ```

  Site 2 passes its `ShardConfig { /* 4 comment lines */ max_trim_per_op: 65_536, trim_global_budget: BUDGET, ..Default::default() }` verbatim.
- **D (own timing):** replace it with `let (<var>, _) = open_engine_with_settings(store.clone(), <prefix>, <cfg literal verbatim>, <settings literal verbatim>).await;`. Site 27 passes `slatedb::config::Settings::default()`. In site 28's settings comment, change `(same stretch open_engine_cfg mirrors)` to `(same stretch open_engine_with_signals mirrors)`.
- **Comment wording.** New comments must not contain the words `seed`, `failpoint`, `worker_threads`, `FaultPlan::` or `start_paused`. The inventory `configuration` field records raw lines matching those, comments included.

**`history_recovery.rs`** (800 → about 568):

- Sites 1, 4, 6, 8 and 11 are A. Sites 2, 3, 5, 7, 9 and 10 are C.
- The imports become `use super::fixture_storage::{append_n, append_sized, mem, open_engine, open_engine_with_signals, skey, wait_all_absorbed};`.
- **Delete** `use object_store::ObjectStore;`. Its only uses were the `as Arc<dyn ObjectStore>` casts; `Arc` stays because of `Arc::ptr_eq` at line 796.

**`history_gather.rs`** (922 → about 765):

- Sites 12, 13, 14 and 15 are A; site 16 is C.
- `gather_after_reopen` at `:41` and `:63`, `gather_with_pacing` at `:96`, and `sparse_absorption_wave_bounds_append_latency` at `:293` become `open_engine(store.clone(), <path>).await`.
- `absorber_on_pool` at `:735` becomes `open_engine_cfg(store.clone(), path, crate::shard::ShardConfig { shared_history: Some(pool.clone()), ..Default::default() }).await`.
- `adaptive_gather_estimate_seeds_decays_and_jumps` at `:152` becomes `let (engine, _) = open_engine_with_settings(..)`, with its arguments unchanged.
- The imports gain `open_engine_cfg` and `open_engine_with_signals`.
- **Delete** `use object_store::ObjectStore;`; the only uses were the casts.

**`history_absorption.rs`** (527 → about 430):

- Site 17: replace lines 93-119 with this, and keep the quiet-channel absorber block below it:

  ```rust
  // open_engine drops the engine's signal receiver: its signals go nowhere.
  let engine = open_engine(store.clone(), "dst-sweep").await;
  ```

- Site 19: replace lines 287-321 (open plus absorber) with `let (engine, absorber) = open_engine_with_absorber(store.clone(), "dst-v2nokey").await;`.
- Sites 18 and 20 are C.
- The imports gain `open_engine` and `open_engine_with_signals`. `ObjectStore` stays (the `_ds` bindings).

**`reads_history.rs`** (568 → about 507):

- Sites 21, 22 and 23 are C.
- Delete the `too_many_lines` expect on `repeated_keyed_reads_hit_the_postings_cache` (lines 362-365).
- The imports gain `open_engine_with_signals`.

**`reads_raw.rs`** (776 → about 738): sites 24 and 25 are C; the imports gain `open_engine_with_signals`.

**`reads_applied.rs`** (556 → about 545): site 26 is D; the imports gain `open_engine_with_settings`.

**`reads_ring.rs`** (597 → about 590): site 27 is D; the imports gain `open_engine_with_settings`.

**`runtime_open_gate.rs`** (774 → about 756): site 28 is D; delete the `too_many_lines` expect at lines 316-319; the imports gain `open_engine_with_settings`.

**`producer_protocol.rs`** (799 → about 759): the 4 calls become `open_engine(store.clone(), "<prefix>").await`; the import becomes `{mem, open_engine, skey}`.

**`docs/refactor/test-inventory.json`:** `--write`. Exactly 30 `function_sha256` changes (the §3 C1 list).

### C2: "DST restarts wait for the predecessor engine's owned shutdown instead of sleeping after begin_close"

Replace each of the 7 pairs with 4 lines, which adds 2 lines per site:

```rust
<var>
    .await_terminated(std::time::Duration::from_secs(30))
    .await
    .expect("<boundary message>");
```

Messages:

- matomic: `"the engine terminates before its shard reopens"`.
- legacy: `"the modern engine terminates before the legacy reopen"`.
- ABA: `"the fenced owner's late shutdown completes"`. Change its comment to "A's late shutdown runs to completion after B is serving; it must not move B's state."
- The four restart tests: `"engine A terminates before B reopens the shard"`.

In `untouched_streams_absorb_after_restart`, fix the two inaccurate comments. `begin_close` is a clean close, not a crash.

- "then drop the engine without a clean close" becomes "then close it".
- "Simulate a crash: close the engine (fencing handoff) but note …" becomes: "The absorber never ran, so absorbed == 0 < next == 5 and the dirty marker is durably present; B opens only after A's owned shutdown finished, so no live writer still holds the shard." Do not use the word "seed".

Line and scope budgets:

- `billing_maintenance.rs` 846 → 852.
- `history_recovery.rs` +6, `history_gather.rs` +2.
- `absorbed_boundary_and_maintenance_retire_atomically`: clippy est. 96 → 98, which is ≤100.
- The other six are at most 84 → 86.
- No expect scopes are involved.

`test-inventory.json`: `--write`, 7 `function_sha256` changes.

### C3: "The sized DST append helpers build their request in one place"

`fixture_storage.rs` only (about 285 → about 267):

- Move the stranded two doc lines ("Direct append of a payload of chosen size …") from `absorb_through` to `append_sized`.
- Add the private `async fn append_entries(engine: &Arc<ShardEngine>, hash: [u8; 16], key: &StreamKey, rk: &str, entries: Vec<bytes::Bytes>) -> u64`. It holds the one `AppendReq` literal (the `append_sized` literal with `entries` as the field value) plus the existing `assert!(engine.try_enqueue(req).is_ok(), "enqueue"); rx.await.expect("resp").expect("ack").last_offset`.
- Doc it with the reason: the sized helpers differ only in entries and routing key, and one literal keeps every other field of their appends identical.
- `append_sized` becomes `append_entries(engine, hash, key, rk, vec![bytes::Bytes::from(vec![0x5au8; payload_bytes])]).await`.
- `append_n` becomes `append_entries(engine, hash, key, "", (0..n).map(|_| bytes::Bytes::from(vec![0x5au8; each])).collect()).await`, with `rk = ""`. `"".to_string() == String::new()` and `stream_hash("")` is unchanged, so the request is identical.
- The only order change: `enqueued_at` is now stamped after the entries are allocated rather than before. It feeds only queue-wait telemetry; no test asserts on it.
- `vec!` and `assert!` are expression macros (`source_rules.EXPRESSION_MACROS`), so no `owners.json` rows are needed.

`test-inventory.json`: `--write` is a no-op because no test body changes. `--check` must pass.

---

## 5. Mutation analysis

- **Selected mutants: none.** Every changed path is `src/dst/tests/*.rs` or `docs/refactor/test-inventory.json`. None is under `CRITICAL_PREFIXES` (`scripts/quality/verification_plan.py:22-31`), and `mutation_owners.py` registers no `src/dst` source path; its only `dst` strings are test filters. `plan()` therefore yields `mutants: false`, `miri: false` and `properties_fuzz: false`.
- **Kill power is preserved.** Owners whose filters run touched modules are `tail_ring` and `tail_ring_tests` (`dst_tests::reads_ring::`, line 87-88) and `http_read` (`dst_tests::reads_raw::`, `dst_tests::reads_history::`, line 95). The substituted tests open byte-identical engines (§2), so the scheduled buckets keep their killers.
- **Boundedness.** Every new wait is bounded: `await_terminated(30 s)` has an `expect` that turns a timeout into a panic, not a hang. The C1 and C3 fixture paths add no waits.

## 6. Ledgers

| Ledger | C1 | C2 | C3 |
|---|---|---|---|
| `docs/refactor/test-inventory.json` (`--write`) | 30 hashes | 7 hashes | no change (`--check` OK) |
| `docs/refactor/review-mechanisms.json` | none | none | none |
| `docs/quality/owners.json`, `source-allowances.json` | none | none | none |
| `architecture-policy.json` | none | none | none |
| scenario map, dispositions, `src/dst/tests/README.md` | none | none | none |
| `mutation_owners.py` | none | none | none |

Why each "none" holds:

- **review-mechanisms.json.** No changed test or helper is pinned. The DST pins are in `billing_controller`, `fleet_controller`, `oracle_model`, `runtime_journals`, `runtime_usage`, `scaler_controller`, `security_freshness`, `security_revocation`, `persistence_faults::cut_resume_never_skips_a_durable_record` (which does not use the changed fixtures) and `fixture_http.rs::{http_rig_cold_absorb, default, http_rig_build, rig_opener}`. `rig_opener` is deliberately left untouched. `review-unit-relocations.json` names none of these files.
- **owners.json and source-allowances.json.** No spawn or macro-dsl occurrence moves. The `a_lost_applied_suffix…` `tokio::spawn` effect row stays at count 1, and no `exception` rows exist for DST files.
- **architecture-policy.json.** There are no DST budget entries. Function budgets default to 200 physical lines and nothing grows past that.
- **Scenario map, dispositions, README.** No test is renamed or deleted, so scenario IDs are unchanged (for example HIS-007 and FLT-008). The README lists fixtures by module, not function.
- **mutation_owners.py.** No new file.

## 7. Controls (exact commands, expected output)

**Python.** The gates need Python ≥3.11 and bare `python3` is macOS 3.9, so put a shim first on `PATH` (a directory with `python3 -> /opt/homebrew/bin/python3.12`).

Per commit, before `git commit`. Use `QUALITY_BASE_REF=HEAD`, so the ratchet compares the working tree with the parent.

1. `cargo fmt --all -- --check` → exit 0, no output.
2. `cargo clippy --locked --workspace --all-targets -- -D warnings` → exit 0.
   - C1 first pass: expect exactly two `unfulfilled_lint_expectations` errors, at `reads_history.rs` (`repeated_keyed_reads_hit_the_postings_cache`) and `runtime_open_gate.rs` (`idle_engine_store_traffic_is_bounded_by_the_poll_cadence`). Delete those two attributes and rerun to exit 0.
   - A third error, on `a_second_absorption_wave_trims_under_a_global_budget`, is possible (§4). Delete that one the same way.
3. `mkdir -p target/quality && cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl && QUALITY_BASE_REF=HEAD python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl`
   → `quality ratchets: OK; … 0 emitted warning occurrences; K accepted exception scopes; base <HEAD[:12]>`. K drops by exactly 2 in C1 (3 if §4's third deletion applied) and is unchanged in C2 and C3. No `accepted exception grew` and no `file growth` lines.
4. Inventory:
   - `python3 scripts/test-inventory.py --write` → `test-inventory: wrote 523 tests`.
   - Then run the checker below. It prints exactly the C1 30 or C2 7 names, each with `['function_sha256']`, and nothing for C3.
   - `python3 scripts/test-inventory.py --check` → `test-inventory: OK (523 tests, 0 ignored)`.

   ```
   python3 - <<'EOF'
   import json, subprocess
   old = {t['name']: t for t in json.loads(subprocess.check_output(['git', 'show', 'HEAD:docs/refactor/test-inventory.json']))}
   new = {t['name']: t for t in json.load(open('docs/refactor/test-inventory.json'))}
   assert old.keys() == new.keys(), 'test set changed'
   for name in sorted(new):
       changed = sorted(k for k in new[name] if new[name][k] != old[name].get(k))
       if changed:
           print(name, changed)
   EOF
   ```

5. `python3 scripts/review-evidence.py --check` → `review-evidence source inventory: OK; execution and external acceptance require receipts`.
6. `python3 scripts/scenario-map-report.py --check` and `python3 scripts/architecture-gate.py --check` → OK.
7. Test legs:
   - **C1:** `scripts/test-leg.sh target/legs/item78-c1.log --min 478 <30 × --exact dst::dst_tests::<module>::<name> from §3> -- --locked --release --lib dst::dst_tests:: -- --skip post_split_throughput_scales`
     → `TESTS_RAN_OK: target/legs/item78-c1.log: floor 478, exact 30`. There are 479 `dst::dst_tests` tests, less the capacity test.
   - **C2:** `scripts/test-leg.sh target/legs/item78-c2.log --min 29 <7 × --exact …> -- --locked --release --lib -- dst::dst_tests::billing_maintenance:: dst::dst_tests::history_recovery:: dst::dst_tests::history_gather::`
     → `TESTS_RAN_OK: …: floor 29, exact 7`.
   - **C3:** `scripts/test-leg.sh target/legs/item78-c3.log --min 40 -- --locked --release --lib -- dst::dst_tests::history_gather:: dst::dst_tests::history_recovery:: dst::dst_tests::reads_history:: dst::dst_tests::reads_raw:: dst::dst_tests::reads_ring::`
     → `TESTS_RAN_OK: …: floor 40, exact 0`.
8. Controls from §3 (NV1–NV7 and the C2 red): make each edit, run the named test with `cargo test --locked --release --lib -- --exact <path>`, check for the expected panic text, revert, then run `git diff --stat` to confirm only the intended files are touched.
9. The whole gate once, after C3: `scripts/quality.sh` → `QUALITY_OK`. Then `cargo test --release -- --skip post_split_throughput_scales 2>&1 | tee /tmp/suite.log && python3 scripts/quality/tests_ran.py /tmp/suite.log --inventory docs/refactor/test-inventory.json --skipped 1` → `TESTS_RAN_OK`.
10. Before the push, CI's own selection:

    ```
    QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=24c4c77a4303e9b4b7a6cb09bfee26453efad657 QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan
    ```

    → `"mutants": false`, `"miri": false`, `"properties_fuzz": false` and `"deleted_critical_files": []`. `changed_rust_files` holds exactly the 11 DST files: fixture_storage, history_recovery, history_gather, history_absorption, reads_history, reads_raw, reads_applied, reads_ring, runtime_open_gate, producer_protocol and billing_maintenance.

    After the push, verify each workflow with `gh run list --branch slate --json headSha,createdAt,status,conclusion,name` for the pushed sha. Do not claim green from memory.

## 8. Out of scope

- The 8 remaining `ShardEngine::start` sites in `src/dst` listed in §1a (matomic path, legacy, handoff, `rig_opener`, dst-seal, `naive_get_or_open`, `open_engine_with_on_close`, and the fixture core itself).
  - Folding `open_engine_with_on_close` would need a fifth `on_close` parameter on the core for one caller. That is not worth it.
- The 28 non-DST `ShardEngine::start` call sites (`src/shard/*_tests.rs`, `src/history*`, `src/sharddir`, production openers). Their paths are under critical prefixes, so they are a separate item with its own mutation plan.
- The other 46 `begin_close()` calls in DST are end-of-test teardown, not boundaries. Converting them would add shutdown waits to every test. `pending_summary_clears_on_shard_close`'s post-close poll observes close's effect by design.
- The other `AppendReq` literals: `durability_fences.rs` (reviewer: leave), the reads_history JSON-keyed loops, `history_absorption.rs:228`, `read_page_limits`, `read_subset_retention`, `producer_protocol`'s `LaneSender` and `src/dst/runtime.rs`. `append_entries` stays private.
- The stranded doc comment on `history_recovery.rs:74-81`, where the "Release blocker … second absorption wave" text sits on `all_tails` instead of the test. It is harmless and separate.

## 9. Decisions for Søren

None. This is a test-harness-only change: no status codes, bodies, `/v1/debug` or `/metrics` shapes move, and no product or raw edge behaviour changes.

The one deliberate deviation from the reviewer's wording is an internal design choice, not an edge or policy change, and needs no sign-off: D2 (`open_engine_with_signals` instead of restating the 5/50 literal at 14 sites), plus naming `sized_req` as `append_entries`.

---

## Skeptic corrections (C1..C7)

Re-verified on `24c4c77a` (= origin/slate, clean tree). **Verified as claimed:**
- The 37 `ShardEngine::start` sites in `src/dst` and their dispositions. Normalised by script: every "same" site has db path == prefix, the same store object for db and engine, `on_close = None`, and nothing between `build()` and `start`.
- The 32 `R25-A` comments.
- The 7 `begin_close(); sleep` pairs (53 `begin_close()` calls in all).
- `await_terminated` at `src/shard.rs:1764-1773`, and `EngineShutdown::wait` at `lifecycle.rs:105-121`.
- `close_db` maps `Fenced` to `Ok` (`history_partition.rs:153`).
- `DbBuilder::new` defaults `Settings::default()` (slatedb `0717cc1` `db/builder.rs:210`).
- `absorb_tx` has exactly one producer, `shard.rs:3033`.
- File sizes: history_recovery 800, history_gather 922, billing_maintenance 846, fixture_storage 301. None is near 1,000.
- No DST exception scope carries `unwrap_used`/`expect_used` fingerprints. The statement-level `disallowed_methods` expect in `sparse_absorption_wave_bounds_append_latency` scopes to the whole fn (`source_rules.exception_contracts` picks the smallest *item*), and it only shrinks.
- No review-mechanisms pin, `support_functions` entry, fixture_changes anchor, architecture budget, diagnostic allowance (both files are empty), `mutation_owners` source or `CRITICAL_PREFIXES` path is touched.
- The inventory `configuration` lines of all 37 affected tests sit outside the replaced blocks. Multi-line `#[expect(` blocks are not in `attributes`. So only `function_sha256` changes (30 in C1, 7 in C2).
- My own `too_many_lines` counter reproduces the plan's calibration (r06a 102, a_stale 103). It gives the same after-C1 estimates: a_second_absorption_wave 119→103 (clippy 101), repeated_keyed_reads 110→94 (92), the idle-poll test 106→~95 (93), a_lost_applied_suffix 121→110 (108).
- All NV edits compile. Their only side effect is `unused_variables` warnings (`absorb_rx`, `cfg`, `settings`, `rk`). There is no `.cargo/config.toml` rustflags, and workspace lints do not deny `unused_variables`, so `cargo test` builds them.
- Test-leg floors: 479 `dst::dst_tests` tests (495 in `src/dst/tests` minus `oracle_model` 10 and `fault_substrate` 6, which are registered elsewhere), 29 in C2 and 40 in C3.

**C1 (factual, §1a and §2 "Settings").** The 5/50 literal does not occur 53 times.
- `grep -rn "flush_interval: Some(std::time::Duration::from_millis(5))" src/dst | wc -l` gives **39**. Across all of `src` it gives 42; the other three are `src/shard/{commit_command,durability_frontier,record_validation}_tests.rs`.
- The "five Settings shapes" list is also incomplete. It omits `runtime_open_gate.rs:21-31` (`seed_untrimmed_wal`: flush 1 ms, `l0_sst_size_bytes 1<<30`, `max_unflushed_bytes 2<<30`). That is not a `ShardEngine::start` site, so no disposition changes.
- Fix the numbers in the "no test lost its meaning" argument. After C1, the literal remains at exactly 4 sites: `fixture_http.rs` (rig_opener), `reads_history.rs:25-30` (dst-seal), `runtime_open_gate.rs` (`naive_get_or_open`) and `runtime_retirement.rs` (`open_engine_with_on_close`).

**C2 (§3 C1 indirect pins).** "The 6 `open_engine_with_absorber` callers … `runtime_isolation` ×2" names the wrong units.
- There are 6 call sites in **4 tests**:
  - `history_absorption::acked_records_survive_absorption_into_history` (:50)
  - `history_absorption::v2_history_survives_engine_handoff` (:371, :382)
  - `reads_history::keyed_catch_up_after_a_cold_index_load_sees_later_absorbed_records` (:533)
  - `runtime_isolation::a_fenced_owners_absorber_exits` (:30, :41; one test)
- After C1 add `history_absorption::v2_absorbs_without_customer_keys`, which becomes the fifth.
- List these exact names as pins in the C1 leg (as `--exact`) instead of "×2".

**C3 (D5 and §2 meaning; C2 is not assertion-neutral).** `x.await_terminated(30 s).expect(..)` adds a **new assertion** to all 7 tests.
- Every supervised role must report `TaskOutcome::Finished` (`lifecycle.rs:112-120`), and the storage-close role must return `Done`: the shard db and `history.close()` must close `Ok`, `Clean` or `Fenced` (`lifecycle.rs:61-72`, `history_partition.rs:129-156`).
- For `ownership_handoff_moves_backlog_without_aba`, A is fenced by B while still alive. "Strengthens it without changing what it asserts" is therefore inaccurate. It asserts strictly more.
- Precedent says it should pass:
  - `runtime_isolation.rs:63`: a fenced owner that saw rejected writes terminates `Ok`.
  - `reads_applied.rs:409-411`.
  - `r17a_fenced_final_flush_releases_only_after_the_owned_close_joins` (`src/shard/task_lifecycle_tests.rs:246`).
- The plan must still state it as an added termination assertion, cite that precedent, and treat a red here as a finding. It must not be relaxed or reverted to the sleep.
- For the matomic test, note that the still-running `_absorber` (a bare task, not a supervised role) exits via `shard.closed()` (`history/worker.rs`, biased select). It is not in the outcome report, and the injected absorbed-group failure is a group failure, not a role failure.

**C4 (§3 C2 red step, wording).** The reviewer's red (`assert!(termination_complete())` right after `begin_close()`) proves only that `begin_close` merely starts the owned shutdown. It is red by construction and does not show that the 200 ms sleep ever lost the race. Keep it as the reviewer's step. The C2 commit message must describe the change as removing a timing bet or hardening the boundary, not as a flake fix.

**C5 (§4 C2 budgets, factual).** "The other six are at most 84 → 86" is wrong for `untouched_streams_absorb_after_restart`, which counts 99 on the current tree (clippy 97).
- It is harmless, because C2 lands on C1, where that test drops to about 60.
- Restate the budgets on the C1 tree, measured after C1 (not before) with the same counter.

**C6 (§7 step 9, hygiene).**
- `cargo test --release -- --skip … | tee /tmp/suite.log` writes to `/tmp`. Use `target/legs/item78-suite.log` or the session scratchpad.
- The command omits `--locked`, unlike every other leg. Make it `cargo test --locked --release …`.
- The pipe masks cargo's exit status. `tests_ran.py` still judges the log, so this is not a gate hole, but run it with `set -o pipefail` for parity with `scripts/test-leg.sh`.

**C7 (§3 NV4, expected text).** With the non-group-commit branch at 600 s, each of the 6 appends in `budget_deferred_streams_absorb_on_the_next_tick` acks only on the engine's 5 s flush ticker, which is bounded (about 30 s). The expected red is the tick-horizon panic.
- Record that any red from this control is acceptable. An ack-path failure inside `append_sized` would also prove the derivation reaches the site.
- Keep the stated 1 s non-control warning.

**Checked and not a problem (no change needed):**
- `scripts/read-experiments/followup/prepare.py:37` extracts `append_sized` by name and would not see a private `append_entries`. But `prepare.py:24-25` refuses any modern tree (it has `src/application/read_batch.rs` and no `src/history/span_cache.rs`), so it only runs against historical revisions, which keep their own `append_sized`. C3 breaks no contract.
- `docs/quality/verification.json` `source_inventory_sha256` pins these files. It is a historical receipt that no script or workflow checks, and only 43 of its 299 hashes match today.
- The `test-scenario-map.json` `line` fields are already stale and are not checked; `scenario-map-report.py` only checks that the symbol is defined.
- `redundant_clone` on `store.clone()` where `store` is unused afterwards: the current tree already does this and passes clippy (`gather_with_pacing`, `absorber_on_pool`).
- `type_complexity` for the spelled-out tuple return is about 110, under the 200 threshold.
- `src/dst/tests/README.md` ("helpers used by just one contract remain private") stays true: `open_engine_with_settings` keeps 4 caller modules and `open_engine_with_signals` gets 6.

**Unbuildable controls:** none.
**Missed ledgers or ratchets:** none. test-inventory is `--write` in C1 and C2 and a no-op in C3. review-mechanisms, owners, source-allowances, architecture-policy, scenario map, README and mutation_owners are correctly untouched.

**Verdict: ready-with-corrections.** Apply C1–C3 and C5 to the plan text. C3 must appear in the C2 commit rationale. C4, C6 and C7 are wording and hygiene.
