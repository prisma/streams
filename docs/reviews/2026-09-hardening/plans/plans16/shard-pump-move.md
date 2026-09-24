# Item 84: move the group-commit pump and flush ticker out of `ShardEngine::start`

Base: `slate` = `origin/slate` = `24c4c77a`, clean tree. Everything below was read on that tree. I ran no cargo, rustfmt or scripts. Every "expected" output is a prediction, and the commit that owns it must confirm it.

Commit order:
- **C0.** Pinning tests for the pump and the ticker, on today's code.
- **C1.** Verbatim move into `src/shard/pump.rs`. This commit is deliberately not rustfmt-clean.
- **C2.** `cargo fmt` output only.
- **C3.** Restructure: `Barrier`, `pending_ledger()`, a flat loop, the ticker's equivalent guards removed, and helper unit tests.
- **C4.** Delete `commit_group`. This commit is gated on a boundedness check (§5.4) and has a pre-decided fallback.

**C0 to C3 (or C4) are pushed as one push. C1 or C2 alone must never be pushed.** The reason is in §5.1: the verbatim file selects about 15 mutants that nothing can kill. CI judges only the pushed head against `QUALITY_BEFORE_SHA`.

---

## 1. Problem (verified on 24c4c77a)

The line numbers in the review are stale. The review cites 1479-1683, 1594-1613 and 2632-2634. The same code now sits at 1476-1667, 1575-1598 and 2545-2547. All four claims hold.

**(a) `start` is 391 lines under five expect attributes (six lints).**
- `src/shard.rs:1343` has `pub(crate) fn start(`. Its closing brace is at `:1733`.
- The attributes at 1322-1342 are:
  - `clippy::too_many_lines`
  - `clippy::too_many_arguments`
  - `clippy::let_underscore_must_use`
  - `clippy::unwrap_used`
  - `clippy::cast_possible_truncation, clippy::excessive_nesting` (one attribute)
- The pump body is `:1476` `engine.spawn_required("pump", async move {` through `:1667` `});`. That is 190 body lines (1477-1666): 119 code lines, the rest comments.
- The ticker body is `:1680` `engine.spawn_required("flush-ticker", async move {` through `:1731`, with 45 code lines.
- `docs/refactor/architecture-policy.json:40` carries `"function:src/shard.rs::start": { "limit": 399, ...}`. The baseline length is 383, and today's physical length is 391.

**(b) rustfmt silently skips the pump.**
- `:1638` is at indent 32: `let q = pump.in_flight.lock().unwrap();`.
- `:1639` is at indent 24: `let q = q.pending();`. That sibling is misindented by 8, which rustfmt would never emit.
- `:1548` is 103 columns: `tracing::warn!(shard = %pump.prefix, "group-commit WAL flush failed: {e}");`.
- `:1658` is 106 columns: `pump.in_flight.lock().unwrap().pending().iter().map(|g| g.reqs).sum();`.
- There is no `rustfmt.toml`, so `max_width` is the default 100.
- `scripts/quality.sh` runs `cargo fmt --all -- --check`, and rust-quality was green on 24c4c77a (memory, verified 2026-09-24).
- So rustfmt fails on the 103-column macro line. It cannot parse `%pump.prefix`, so it leaves the line verbatim, then keeps the enclosing `async move` block unformatted.

**(c) `Result<u64, bool>` is decoded only by a comment (`:1575-1598`):**
```
// Ok(seq) = dispatch; Err(true) = watch gone
// (db closed, exit); Err(false) = skip
// (fenced, or failsafe timeout: the acker
// still owns dispatch).
let seen: Result<u64, bool> = {
    ... Ok(Ok(sref)) if sref.close_reason.is_some() => Err(false),
        Ok(Ok(sref)) => Ok(sref.durable_seq),
        Ok(Err(_)) => Err(true),
        Err(_) => Err(false),
let durable_seq = match seen { Ok(seq) => seq, Err(true) => return, Err(false) => continue, };
```
This is the only `Result<u64, bool>` in `src`, `tools`, `bench` and `fuzz`.

**(d) `commit_group` is a one-line pass-through (`:2545-2547`):**
```
async fn commit_group(&self, ops: Vec<CommitOp>, cfg: &ShardConfig) {
    transaction::CommitTransaction::run(self, ops, cfg).await;
}
```

**Use sites (all of `src`, `src/dst`, cfg(test), `tools/`, `fuzz/`, `bench/`, `tests/`, `examples/` grepped):**
- **`ShardEngine::start`:** 65 call sites. The signature is unchanged, so no caller changes.
- **`commit_group`:**
  - Production caller: `shard.rs:2541` (inside `committer_loop`).
  - Test callers (10):
    - `billing_read_tests.rs:80` and `:119`
    - `queue_codec_tests.rs:63`
    - `transaction_tests.rs:289`, `:450` and `:506`
    - `commit_command_tests.rs:186`
    - `bounded_outbox_tests.rs:79` and `:100`
    - `queue_publication_tests.rs:42`
  - Doc mentions: `shard.rs:926` and `:954` (CommitOp variant docs), and `docs/review-followup/commit-transaction.md:5`.
  - Name only: the DST test `seal_converges_through_transient_commit_group_failures` (`src/dst/tests/seal_convergence.rs:264`), which names the concept, not the fn.
  - Historical, not edited: `codereview1.md` and `docs/refactor/BASELINE.md`.
  - None in `tools/`, `fuzz/` or `bench/`.
- **`CommitTransaction::run` / `::reject_op` outside `transaction/`:** `shard.rs:2482`, `:2484` and `:2546` only.
- **`in_flight...pending()`:**
  - Pump: `:1491`, `:1529`, `:1639` and `:1658`.
  - `oldest_inflight_ms`: `:1951`.
  - Tests: `transaction_tests.rs:303` and `:352`; `retirement_tests.rs:252`, `:271`, `:292` and `:467`; `commit_handoff/loom_tests.rs:59` and `:90`.
- **Pump and ticker state:**
  - `pump_wake`: notified at `transaction/publish.rs:102` and `shard.rs:1868`.
  - `pump_*` counters: read by `/v1/debug` at `http.rs:1486-1506` and by `dst/tests/durability_gather.rs`.
  - `ack_armed_at_us`: `shard.rs:1826` (`try_enqueue`).
  - `stats_appended`: written at `publish.rs:82`, read by the ticker and by `retirement_tests.rs:490`.
  - `trim_debt`: ticker, `note_trim_debt`, `publish.rs:60`, `prepare.rs:28`, `stream_handle` and `trim_stats`.
  - `trim_cursor`: `prepare.rs:32`.
  - Role strings `"pump"` and `"flush-ticker"`: `shard/task_lifecycle_tests.rs:130` and `dst/tests/runtime_engine_lifecycle.rs:59`. Both strings are kept.
- **Ratchet rows:**
  - `docs/quality/source-allowances.json`: macro-dsl `tokio::select` for `crate::ShardEngine::start` in `src/shard.rs`, count 1. This is the ticker's `select!`.
  - `architecture-policy.json`: the `start` budget row above.

**The reviewer's Change is buildable but not pushable as written.** The verbatim move alone selects about 27 mutants in the new file. About 15 of them cannot be killed:
- equivalent mutants (`<` vs `<=` on `elapsed()`);
- timing-only mutants (the gap and herd-settle sleeps);
- log-only mutants (`evicted > 0`);
- ticker guards made redundant by `evict_idle_handles`' own guards.

So the flatten must go further than `Barrier` + `pending_ledger()` (§4 C3), and must land in the same push. "Four plain args" becomes five (`pump, gap, gather, skip_reqs, skip_bytes`). Five is within the lint's limit, and a bag type would be the "options bag" the policy forbids.

## 2. Contract decision

**No product or raw edge change.**
- Status codes, bodies and `/metrics` are untouched.
- The `/v1/debug` pump block (`http.rs:1486-1506`) keeps every field and its meaning. The flushed and gathered sums come from the same groups.
- Log messages are identical, including `"WAL group-commit pump on"`, `"group-commit WAL flush failed: {e}"`, `"memtable flush tick failed: {e}"` and `"evicted {evicted} idle stream handles"`.
- The role names `pump` and `flush-ticker` in `/health` and `/readyz` are unchanged.

C3 carries three internal deltas. None is observable, and none needs a decision:
1. **Gap sleep at an exact tie.** The gap sleep becomes `gap.checked_sub(t0.elapsed())`. At an exact nanosecond tie (`since == gap`) it sleeps zero instead of skipping the sleep.
2. **u64 sums.** Pending request sums are u64. Before, a u32 sum of more than 4G requests would have wrapped (release) or panicked (debug).
3. **Unconditional eviction call.** The ticker calls `evict_idle_handles` even when both knobs are 0. The callee returns 0 in that case. Production defaults (600 s, 65,536) already made the guard always true.

## 3. Tests

### 3.1 Existing pins (must stay green at C1, C2, C3 and C4, bodies unchanged)

- `dst::dst_tests::durability_gather`, four tests:
  - `the_gather_window_waits_for_ack_dispatch`
  - `commits_during_a_flush_do_not_extend_its_barrier` (pins "frozen captured before the flush")
  - `a_busy_next_generation_skips_the_gather_window` (pins the busy thresholds)
  - `gather_pump_preserves_invariants_under_faults` (pins that barrier plus gather are exercised)
- `dst::dst_tests::runtime_open_gate::idle_engine_store_traffic_is_bounded_by_the_poll_cadence`. Its assert `dpw == 0`, "an idle pump must not mint WAL objects", pins the emptiness check.
- `dst::dst_tests::runtime_engine_lifecycle::r17a_each_required_engine_exit_changes_real_health_and_readyz`, which pins the role names.
- `dst::dst_tests::history_recovery::a_second_absorption_wave_trims_under_a_global_budget`, whose assert is "flush-ticker trim maintenance never drained the debt".
- `dst::dst_tests::reads_applied::a_lost_applied_suffix_rewinds_to_the_durable_frontier`, which relies on the ticker's 5 s cadence and immediate first tick.
- `shard::task_lifecycle_tests::*` (6 tests), which cover engine roles and the held WAL.

No DST file is edited, so `docs/refactor/test-inventory.json` does not change.

### 3.2 New engine-level pins, landed first (C0), green on today's code

New file `src/shard/pump_tests.rs` (`#![cfg(test)]`, about 110 lines). All three tests are `#[tokio::test(flavor = "current_thread", start_paused = true)]`. SlateDB under paused time is proven by `idle_engine_store_traffic_is_bounded_by_the_poll_cadence`.

The shared fixture is a private `async fn engine(name, cfg) -> (Arc<ShardEngine>, Arc<crate::dst::FaultStore>)`:
- `FaultStore::new(InMemory, 1720, FaultProfile::clean())`
- `Db::builder(name, store.clone())` with `Settings { flush_interval: Some(600 s), ..Default::default() }`, so SlateDB never flushes on its own inside a test.
- `ShardEngine::start(name, db, store, cfg, mpsc::channel(16).0, None, ShardMaintenance::default())`.
- Then `sleep(200 ms)`, so the ticker's immediate first tick is spent on an empty engine. This mirrors `reads_applied`'s settle.

The tests:

**`shard::pump_tests::the_pump_alone_makes_a_committed_close_durable`**
- Config: `ShardConfig { wal_group_commit: true, wal_flush_gap: ZERO, .. }`.
- Calls `try_close(CloseReq { hash: [0x5B; 16], generation: None, resp })`.
- Waits `timeout(10 s virtual, reply)` and asserts `matches!(acked, Ok(Ok(Ok(AppendAck { closed: true, .. }))))`.
- Message: "only the pump flushes the WAL here, so the close must ack through it".
- It then closes the engine: `begin_close()` and `await_terminated(10 s)`.

**`shard::pump_tests::the_ticker_flushes_the_memtable_once_appends_accumulated`**
- Default config (pump off).
- Records `before = store.count(StoreOp::Put, ObjClass::Sst)`.
- Calls `engine.db.put(b"ticker-probe", b"1").await.unwrap()`. In this fork `put` does not await durability.
- Then `engine.stats_appended.fetch_add(1, Relaxed)`. It is put first, then fetch_add, the same order production uses.
- Waits `timeout(12 s virtual, while count == before { sleep(100 ms) })`, then asserts `is_ok()`.
- Message: "no memtable flush reached L0 within two ticker intervals of an append".
- **If this is red on HEAD** (the SlateDB memtable flush needs the WAL timer), use `Settings::default()` for this test only and record why.

**`shard::pump_tests::the_ticker_queues_a_trim_pulse_while_streams_owe_trims`**
- Default config.
- Calls `engine.note_trim_debt([0x5A; 16])`. It only inserts; it does not send a TrimTick.
- Waits `timeout(12 s virtual, while *engine.trim_cursor.lock().unwrap() != [0x5A; 16] { sleep(100 ms) })`, then asserts `is_ok()`.
- Message: "owed trims never reached the committer as a TrimTick within two ticker intervals".
- Only `prepare.rs:32` writes the cursor, and only while expanding a TrimTick with debt present. Nothing else sends TrimTick in this fixture: no absorber, and `pump_trim_tick` is never called.

Expected on HEAD: `test result: ok. 3 passed`.

**C0 non-vacuity controls** (edit `src/shard.rs` at HEAD, run `cargo test --locked --lib shard::pump_tests`, then `git checkout src/shard.rs`):

| Control | Edit | Expected result |
| --- | --- | --- |
| A | Insert `return;` after `:1479` (`let mut last_start ...`) | The pump test FAILS: a required role exit closes the engine, so the reply is `Err(Moved)` or dropped |
| B | `:1697` `appended != last_appended` changed to `==` | The memtable test FAILS with its message |
| C | `:1727` drop the `!` | The trim test FAILS with its message |

### 3.3 New unit pins for the extracted helpers (C3)

These live in an inline `#[cfg(test)] mod tests` in `src/shard/pump.rs`, so the helpers stay private. The module path is `shard::pump::tests`.

**`the_barrier_waits_for_a_covering_watermark`**
```
Barrier::read(6, false, 7).is_none()
read(7, false, 7) is Some(Durable(7))
read(9, false, 7) is Some(Durable(9))
```

**`a_closing_database_hands_dispatch_back_to_the_acker`**
```
read(6, true, 7) is Some(Skip)
read(7, true, 7) is Some(Skip)
```
This is the old guard's precedence: a close wins over a covering watermark.

**`a_generation_at_either_threshold_skips_the_gather_window`**

With `waiting(reqs, bytes)` building a `PendingLedger`:
```
waiting(4, 0).busy(4, 64)
waiting(0, 64).busy(4, 64)
!waiting(3, 63).busy(4, 64)
```

**`the_ledger_sums_every_group_awaiting_durability`**
- `#[tokio::test(flavor = "current_thread", start_paused = true)]`.
- Builds its own 8-line engine: InMemory store, pump off.
- Asserts `pending_ledger().is_none()` on the empty queue.
- Pushes two `InFlightGroup`s through `in_flight.lock().unwrap().publication().unwrap()`:
  - seq `1_000_000`, reqs 2, records 3, bytes 40
  - seq `1_000_001`, reqs 5, records 7, bytes 60
  - Both use `effects: DurableEffects::default()` and seqs far above the fresh durable seq 0, so the acker never takes them.
- Asserts `(last_seq, reqs, records, bytes) == (1_000_001, 7, 10, 100)`, then closes the engine.
- This pins the three sums that C3 folds from three sites into one. They feed `/v1/debug`.

Expected at C3: `cargo test --locked --lib shard::pump` gives `ok. 7 passed`.

**C3 non-vacuity:** the local mutation run in §7, where every viable mutant is CAUGHT. Manual controls, each reverted:

| Edit | Expected |
| --- | --- |
| `>=` changed to `>` in `Barrier::read` | the barrier test FAILS |
| `||` changed to `&&` in `busy` | the threshold test FAILS |
| `bytes` summed from `reqs` in `pending_ledger` | the ledger test FAILS |

## 4. Edits by commit

### C0: pinning tests (behaviour unchanged)

- **`src/shard.rs`, line-neutral (3139 to 3139).**
  - Insert `#[cfg(test)]` and `mod pump_tests;` after `:3117` (`mod task_lifecycle_tests;`).
  - Delete the blank lines at `:3107` and `:3110`, inside the cfg(test) mod list.
  - No exception scope is involved, and no mutant lives on these lines.
- **`src/shard/pump_tests.rs`, new (§3.2).**
  - Explicit imports: `super::{AppendAck, CloseReq, ShardConfig, ShardEngine, ShardMaintenance}` and `crate::dst::{FaultProfile, FaultStore, ObjClass, StoreOp}`.
  - No glob, so no unresolved-glob row.
  - No expects. Nesting is at most 3: fn, async block, while.
- **`scripts/quality/mutation_owners.py`.** Add `owner('pump_tests', 'src/shard/pump_tests.rs', 'shard::pump_tests::'),` after the `task_lifecycle_tests` row. The precedent is `transaction_tests.rs`, which has `#![cfg(test)]` and a row.
- Commit message: "The pump and flush ticker duties get pinning tests before they move".

### C1: verbatim move (not rustfmt-clean by design)

**`src/shard.rs`, 3139 to about 2890 (at most 2893 if rustfmt splits the spawn call).** The ceiling is 3139.
- After `:30` (`mod lifecycle;`): add `mod pump;`.
- After `:39` (`pub(crate) use lifecycle::EngineShutdown;`): add `use pump::{flush_ticker_loop, pump_loop};`.
- `start` attributes `:1322-1342`:
  - keep `too_many_lines` (post-move start is about 121 code lines, still above 100) and `too_many_arguments` (7 args), with unchanged text;
  - **delete** `let_underscore_must_use` and `unwrap_used`. Their only sites leave start (the ticker's `let _ = try_send` and the pump and trim-debt locks), so they would be unfulfilled, which is denied;
  - **replace** the `cast_possible_truncation, excessive_nesting` attribute with a cast-only one. Post-move nesting is impl, fn, `if`, closure: 4, so excessive_nesting would be unfulfilled. The `info!` casts at `:1472-1473` stay in start. New reason: `reason = "ShardEngine::start; the pump's startup log reports its gap and gather windows in whole milliseconds, which fit u64 for any configured window; a checked conversion would only restate the configuration"`. **If clippy reports this unfulfilled (casts inside `tracing::info!` not linted), delete it.**
- `:1476-1667`: replace with `engine.spawn_required("pump", pump_loop(pump, gap, gather, skip_reqs, skip_bytes));`. The lets and `info!` at `:1465-1475` and the rationale comment at `:1455-1463` stay untouched.
- `:1679`: `let mut ticker_closed` becomes `let ticker_closed`. The `mut` moves to the parameter.
- `:1680-1731`: replace with `engine.spawn_required("flush-ticker", flush_ticker_loop(ticker, ticker_closed));`. The F1 comment at `:1675-1677` stays.
- To keep `pump.rs` verbatim while formatting `shard.rs`, run `cp src/shard/pump.rs /tmp/pump.c1 && cargo fmt --all && cp /tmp/pump.c1 src/shard/pump.rs`. rustfmt follows `mod pump;`, so it would otherwise format both files.

**`src/shard/pump.rs`, new (about 270 lines).**
```rust
//! The engine's timer-driven durability workers, moved verbatim out of
//! `ShardEngine::start` so the spawn site reads as the wiring list it is.
use super::{CommitOp, ShardEngine};
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// Body of the group-commit pump `ShardEngine::start` spawns when
/// `wal_group_commit` is on; its rationale stays at the spawn site.
#[expect(clippy::too_many_lines, reason = "pump_loop; the group-commit tick moved verbatim out of ShardEngine::start so its diff reads as a pure move; restructuring it in the same commit would hide behaviour changes inside the move")]
#[expect(clippy::unwrap_used, reason = "pump_loop; a poisoned in-flight queue may hold a half-recorded group; recovering it could acknowledge a group that never committed")]
#[expect(clippy::cast_possible_truncation, clippy::excessive_nesting, reason = "pump_loop; the moved tick nests each flush, barrier and gather verdict inside the wake that produced it and stamps the ack probe in microseconds that fit u64; flattening it belongs to the restructure after this verbatim move")]
pub(super) async fn pump_loop(
    pump: Arc<ShardEngine>,
    gap: std::time::Duration,
    gather: std::time::Duration,
    skip_reqs: u32,
    skip_bytes: u64,
) {
    <shard.rs:1477-1666 at HEAD, each line de-indented by exactly 12 spaces>
}

/// Body of the 5 s memtable/maintenance ticker `ShardEngine::start` spawns;
/// the F1 recovery-bound rationale stays at the spawn site.
#[expect(clippy::unwrap_used, reason = "flush_ticker_loop; a poisoned trim-debt set may hold a half-recorded debt; recovering it could trim a stream that still owes data")]
#[expect(clippy::let_underscore_must_use, reason = "flush_ticker_loop; a trim tick the committer queue cannot take is superseded by the next tick; a handled send would only restate the tick cadence")]
pub(super) async fn flush_ticker_loop(
    ticker: Arc<ShardEngine>,
    mut ticker_closed: tokio::sync::watch::Receiver<bool>,
) {
    <shard.rs:1681-1730 at HEAD, each line de-indented by exactly 8 spaces>
}
```
The parameter names equal the captured variables, so the bodies are byte-identical after de-indent. Child-module privacy gives access to ShardEngine's private fields and methods, including `dispatch_durable` and `evict_idle_handles`.

Clippy per scope at C1:
- `pump_loop`:
  - too_many_lines fires (119 code lines);
  - unwrap_used fires (4 sites);
  - cast fires (`as_micros().max(1) as u64`; the other `as u64` casts are from u32 or usize);
  - nesting fires (fn, loop, `if let`, `if`, `if` reaches 5).
- `flush_ticker_loop`:
  - nesting stays at most 4, because `select!` blocks are expansion-spanned and skipped;
  - no cast; 45 lines;
  - so only unwrap and let_underscore are expected.
- Architecture: `pump_loop` is 7 + 190 + 1 = 198 physical lines, within the 200 limit. `start` is about 150, below 200.

**Other files in C1:**
- `scripts/quality/mutation_owners.py`: add `owner('pump', 'src/shard/pump.rs', 'shard::pump:: shard::pump_tests::'),` after the `shard` row.
- `scripts/quality/test_mutation_owners.py`: in `test_reviewed_moved_owners_are_registered`, add `self.assertEqual(owners['src/shard/pump.rs'].name, 'pump')`.
- `docs/quality/owners.json`: insert near the other `src/shard/` rows (never re-sort):
  `{"category": "macro-dsl", "count": 1, "owner": "crate::flush_ticker_loop", "path": "src/shard/pump.rs", "reason": "The flush ticker races the engine's level-triggered close signal with its 5 s interval; the select macro is expression selection and the compiler checks its expansion.", "syntax": "tokio::select"}`.
  If the source gate's "unregistered source occurrence" message spells the owner differently, use its spelling.
- `docs/quality/source-allowances.json`: remove the `{macro-dsl, crate::ShardEngine::start, src/shard.rs, tokio::select}` row. Use `gate.py --clippy target/quality/clippy.jsonl --prune`, and check the diff is exactly that row; otherwise hand-edit.
- `docs/refactor/architecture-policy.json`: delete `"function:src/shard.rs::start"`. The gate refuses obsolete budget exceptions, and start falls under the 200-line default.

**Ratcheted scopes at C1, and the import-alias trap:**
- `start`'s too_many_lines and too_many_arguments identities keep their text. `scope_lines` (391 to about 150), `nested_items` (the two in-fn `use` items leave) and `syntax_facts` all shrink.
- The function-wide `unwrap_used` scope that fingerprinted every path and call in the pump is **deleted, not carried**. No fingerprint comparison survives for it.
- No existing function is moved or renamed: `pump_loop` and `flush_ticker_loop` are new names, and `CommitTransaction::run`'s path is unchanged. So no other ratcheted caller's resolved import aliases change.
- Remedy if `source_gate` nevertheless reports "accepted exception grew": re-decide that identity's reason text in C1.
- Commit message: "The group-commit pump and flush ticker move verbatim into shard/pump.rs". The message says the commit is intentionally fmt-dirty and pushed only with C2 and C3.

### C2: formatting only

- Run `cargo fmt --all`. The expected diff touches only `src/shard/pump.rs`:
  - `:1639`'s `let q = q.pending();` is re-indented;
  - the `pump.pump_flushed_records` / `.fetch_add` chains are joined (also for bytes, `pump_barrier_acked` and `pump_gathers_skipped_busy`);
  - the imports are reordered.
- Now 91 columns wide, the old `:1548` line no longer blocks rustfmt.
- `pump_loop` shrinks by about 5 lines. Its expects keep their text. Because `pump.rs` is new relative to the push base, there is no growth comparison.
- **If `cargo fmt --check` already passes after C1**, rustfmt still skips something. Record which line (`rustfmt --check --config error_on_line_overflow=true src/shard/pump.rs`), and drop C2 as empty.
- Commit message: "rustfmt formats the moved pump it used to skip".

### C3: restructure (the only commit with logic edits; `src/shard/pump.rs` only)

The final `pump.rs` is about 260 lines including tests. Sketch below; comments carried from the verbatim body are marked `[...]`.
```rust
//! The engine's timer-driven durability workers. `ShardEngine::start` only
//! wires them; their decisions live here so each can be read and tested apart
//! from the spawn order.
use super::{CommitOp, ShardEngine};
use slatedb::DbStatus;
use slatedb::config::{FlushOptions, FlushType};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// What the in-flight queue owes durability, read in one pass under its lock:
/// the barrier target plus the flush ledger `/v1/debug` reports.
pub(super) struct PendingLedger { last_seq: u64, reqs: u64, records: u64, bytes: u64 }

impl PendingLedger {
    /// A gather window in front of an already-big WAL is a pure tax (review #2's
    /// throughput concern at CDG's top tiers), so a waiting generation at either
    /// threshold flushes without one.
    fn busy(&self, skip_reqs: u32, skip_bytes: u64) -> bool {
        self.reqs >= u64::from(skip_reqs) || self.bytes >= skip_bytes
    }
}

/// The post-flush barrier's verdict; it replaces a `Result<u64, bool>` whose
/// meaning lived only in a comment.
enum Barrier {
    /// The watermark covers the frozen generation: its acks leave here, before any window.
    Durable(u64),
    /// Closing, or the failsafe elapsed: the acker still owns dispatch.
    Skip,
    /// The status watch closed with the database: the pump exits.
    Closed,
}

impl Barrier {
    /// A closing database never releases acks from the pump, even when the watermark covers them.
    fn read(durable_seq: u64, closing: bool, target: u64) -> Option<Self> {
        if closing { Some(Self::Skip) } else if durable_seq >= target { Some(Self::Durable(durable_seq)) } else { None }
    }
}

/// [flush() resolves before the watch publishes: measured 1612 of 1626 stale; 250 ms failsafe;
/// the !Send watch Ref never outlives this function]
async fn await_barrier(status: &mut watch::Receiver<DbStatus>, target: u64) -> Barrier {
    let covered = status.wait_for(|s| Barrier::read(s.durable_seq, s.close_reason.is_some(), target).is_some());
    match tokio::time::timeout(Duration::from_millis(250), covered).await {
        Ok(Ok(seen)) => Barrier::read(seen.durable_seq, seen.close_reason.is_some(), target).unwrap_or(Barrier::Skip),
        Ok(Err(_)) => Barrier::Closed,
        Err(_) => Barrier::Skip,
    }
}

impl ShardEngine {
    /// One pass gives the pump its emptiness check, barrier target and ledger, so
    /// the lock and its poisoned-lock decision live in one place.
    #[expect(clippy::unwrap_used, reason = "ShardEngine::pending_ledger; a poisoned in-flight queue may hold a half-recorded group; recovering it could acknowledge a group that never committed")]
    fn pending_ledger(&self) -> Option<PendingLedger> {
        let handoff = self.in_flight.lock().unwrap();
        let pending = handoff.pending();
        Some(PendingLedger {
            last_seq: pending.last()?.seq,
            reqs: pending.iter().map(|g| u64::from(g.reqs)).sum(),
            records: pending.iter().map(|g| u64::from(g.records_n)).sum(),
            bytes: pending.iter().map(|g| g.bytes).sum(),
        })
    }
}

/// [the start-side rationale: the ack path stops paying tick alignment; start-to-start gap; ...]
#[expect(clippy::cast_possible_truncation, reason = "pump_loop; the ack probe stamps whole microseconds since engine start, which fit u64 for 584,000 years; a checked conversion would only restate that horizon")]
pub(super) async fn pump_loop(pump: Arc<ShardEngine>, gap: Duration, gather: Duration, skip_reqs: u32, skip_bytes: u64) {
    // None keeps the plain flush pump: no herd settle, no barrier, no window.
    let window = if gather.is_zero() { None } else { Some(gather) };
    let mut status_rx = pump.db.subscribe();
    let mut last_start: Option<Instant> = None;
    loop {
        pump.pump_wake.notified().await;
        if pump.is_closed() { return; }
        // [only flush when a commit awaits durability: 52 ms vs 29 ms]
        if pump.pending_ledger().is_none() { continue; }
        // The gap runs start-to-start: a PUT slower than the gap adds no wait.
        if let Some(wait) = last_start.and_then(|t0| gap.checked_sub(t0.elapsed())) {
            tokio::time::sleep(wait).await;
            if pump.is_closed() { return; }
        }
        // [herd-settle: 1 ms]
        if window.is_some() {
            tokio::time::sleep(Duration::from_millis(1)).await;
            if pump.is_closed() { return; }
        }
        last_start = Some(Instant::now());
        // [captured BEFORE the flush: after could include, and wait on, the NEXT generation]
        let frozen = pump.pending_ledger();
        if let Err(e) = pump.db.flush_with_options(FlushOptions { flush_type: FlushType::Wal }).await {
            if pump.is_closed() { return; }
            tracing::warn!(shard = %pump.prefix, "group-commit WAL flush failed: {e}");
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        pump.pump_flushes.fetch_add(1, Ordering::Relaxed);
        let Some(frozen) = frozen else { continue };
        pump.pump_flushed_reqs.fetch_add(frozen.reqs, Ordering::Relaxed);
        pump.pump_flushed_records.fetch_add(frozen.records, Ordering::Relaxed);
        pump.pump_flushed_bytes.fetch_add(frozen.bytes, Ordering::Relaxed);
        let Some(window) = window else { continue };
        let durable_seq = match await_barrier(&mut status_rx, frozen.last_seq).await {
            Barrier::Durable(seq) => seq,
            Barrier::Skip => continue,
            Barrier::Closed => return,
        };
        // [drain whatever the acker has not taken; dispatch_gate ordering]
        let acked = pump.dispatch_durable(durable_seq).await;
        pump.pump_barrier_acked.fetch_add(u64::from(acked), Ordering::Relaxed);
        // [arm the ack->next-enqueue probe]
        pump.ack_armed_at_us.store(pump.epoch.elapsed().as_micros().max(1) as u64, Ordering::Relaxed);
        // [gather ONLY when this completion proves concurrency; NOT keyed on acked > 0]
        let Some(next) = pump.pending_ledger() else { continue };
        if next.busy(skip_reqs, skip_bytes) {
            pump.pump_gathers_skipped_busy.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        pump.pump_gathers.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(window).await;
        if pump.is_closed() { return; }
        // What the window caught: requests present now that were not pending when it opened.
        let after = pump.pending_ledger().map_or(0, |ledger| ledger.reqs);
        pump.pump_gathered_reqs.fetch_add(after.saturating_sub(next.reqs), Ordering::Relaxed);
    }
}

/// `max_wal_flushes_before_l0_flush` has a 4096 upstream floor, so this caps the WAL
/// replay window itself (F1 recovery bound) and carries handle eviction, postings
/// sweep and the trim pulse on the same 5 s cadence.
#[expect(clippy::unwrap_used, /* C1 text */)]
#[expect(clippy::let_underscore_must_use, /* C1 text */)]
pub(super) async fn flush_ticker_loop(ticker: Arc<ShardEngine>, mut ticker_closed: watch::Receiver<bool>) {
    <C1 body with exactly two edits:>
    // evict_idle_handles owns both switches: a zero window or a zero cap disables its half.
    let evicted = ticker.evict_idle_handles(ticker.handle_idle_evict, ticker.handle_max_resident);
    if let Some(evicted) = NonZeroUsize::new(evicted) {
        tracing::debug!(shard = %ticker.prefix, "evicted {evicted} idle stream handles");
    }
    (this replaces `let idle = ...; if !idle.is_zero() || ticker.handle_max_resident > 0 { ... if evicted > 0 { debug } }`)
}

#[cfg(test)]
mod tests { /* §3.3 */ }
```

**Equivalence argument, line by line against the C2 body (for review):**
- The emptiness check followed by the post-sleep capture keeps the original "check, sleep, capture before flush" order.
- An empty freeze skips counters that would each have added 0.
- The "gather disabled" `continue` precedes the barrier as before.
- `Barrier::read` reproduces the guard order: a close wins over a covering watermark.
- The drift check is `pending_ledger()` being None, which matches `!q.is_empty()`. The busy test is `busy()`. The window, the closed check after the window, and `after - before` are unchanged.
- Rust facts behind the design:
  - `wait_for` returns the `Ref` of the value that satisfied the predicate, so the second `read` sees the same status.
  - No await follows the `Ref`, so `await_barrier`'s future stays Send.
  - `dispatch_durable` returns u32 (`shard.rs:2977`), so `u64::from` is lossless.

**Clippy at C3:**
- `pump_loop`: about 65 code lines. Nesting stays at most 4: fn, loop, `if`, `if`, and `match` arms are expressions. So too_many_lines, excessive_nesting and unwrap_used are deleted from it, since all three would be unfulfilled. If clippy still fires one of them, reinstate it with a re-decided reason.
- Async fns take `Arc` and `Duration` by value without `needless_pass_by_value`. The precedent is `committer_loop(cfg: ShardConfig)`, which has no such expect.

**Ledgers at C3:** none. The owners.json `select!` row still counts 1 in `crate::flush_ticker_loop`, and `pump.rs` has no new macro-dsl or effect sites. The file is new relative to the push base, so its exceptions are new identities.

Commit message: "The pump's barrier and ledger become types and its equivalent guards go".

### C4: delete `commit_group` (gated, §5.4)

- **`src/shard.rs`:**
  - `:2541` becomes `transaction::CommitTransaction::run(&self, ops, &cfg).await;`. `&Arc<ShardEngine>` deref-coerces to `&ShardEngine`, and the idiom matches `:2482`.
  - Delete `:2544-2547` (blank line plus the fn). That is 4 lines, bringing shard.rs to about 2886.
  - `:926` "at commit_group entry." becomes "at transaction entry." `:954` "(commit_group entry)" becomes "(transaction entry)". Both are line-neutral doc edits.
- **Tests (10 sites):** each `X.commit_group(args).await` becomes `transaction::CommitTransaction::run(&X, args).await`. `transaction` is in scope through each file's `use super::*;` (a child module glob sees private items). `run` is `pub(super)` in `shard::transaction`, so it is visible to every `shard` descendant.
- **Ratcheted scopes in C4:**
  - `committer_loop` (let_underscore_must_use and excessive_nesting): syntax_facts go from 5 to 5 and lines from 1 to 1.
    - Before: `method-call` + `method-call-site` + paths `self`, `ops`, `cfg`.
    - After: `call-site` + path `run` + `self`, `ops`, `cfg`.
  - CommitOp `large_enum_variant`: doc attribute count unchanged.
  - Test fns are also count-neutral, and their layout is equal or shorter:
    - `r13` (nesting, lines, let_)
    - `r08` (let_)
    - `r03a` (lines, nesting)
    - `r03_failed_group...` (let_)
    - `r09` (lines, let_)
    - `r03_queue_refusal...` (lines, let_)
  - Clippy confirms every `too_many_lines` expect is still fulfilled.
- **`docs/refactor/review-mechanisms.json`:**
  - Mechanism `durable-accounting-read-failure`, test `r13_failed_accounting_reads_preserve_group_and_newer_dirty_version`: the `sha256` changes from `df3f9bbc...` to the new hash.
  - `source_adaptations` entry `r03a_mixed_transaction_preserves_every_row_reply_and_publication`: `after_sha256` changes from `bf37a0aa...` to the new hash. Append to its `reason`: "Item 84 hands the group to CommitTransaction::run directly now that the commit_group pass-through is gone; every assertion is unchanged."
- **`docs/refactor/review-unit-relocations.json`:** the `r13...` entry's `function_sha256` changes from `df3f9bbc...` to the new hash. Append to its `reason`: "Item 84 calls CommitTransaction::run instead of the deleted commit_group; inputs and assertions unchanged."
- New hashes come from:
  `python3 -c "import importlib.util,pathlib;s=importlib.util.spec_from_file_location('i','scripts/test-inventory.py');i=importlib.util.module_from_spec(s);s.loader.exec_module(i);p=pathlib.Path('<file>');print([f['function_sha256'] for f in i.functions(p.read_text(),p,include_helpers=True) if f['name']=='<name>'])"`.
- **`docs/review-followup/commit-transaction.md:5`:** "`ShardEngine::commit_group` delegates to `CommitTransaction`" becomes "The committer hands each group to `CommitTransaction::run`". Line-neutral.
- Not touched: `transaction/mod.rs` (its `run`, `stage` and `write` blank mutants are recorded unbounded, commit 4f522b0f).
- Commit message: "The committer calls CommitTransaction::run without a pass-through".

## 5. Mutation analysis (cargo-mutants 27.1.0, source read at `~/.cargo/registry/src/*/cargo-mutants-27.1.0/src/{visit,in_diff}.rs`)

Relevant rules:
- FnValue replaces the body statements' span.
- Binary operators: `< → == > <=`, `> → == < >=`, `>= → <`, `&& → ||`, `|| → &&`, `- → + /`, `!= → ==`.
- Unary `!` is deleted.
- A match guard becomes `true` or `false`.
- Arm deletion happens only when the match has a `_` arm.
- Macros (`select!`, `tracing::*!`) and `#[cfg(test)]` items are not mutated.
- `--in-diff` selects any mutant whose span covers an inserted line or a line adjacent to a deletion.

### 5.1 Why C1 cannot ship alone: every mutant in the verbatim `pump.rs` (all lines are new)

`pump_loop`:

| Mutant | Outcome |
| --- | --- |
| FnValue `()` | killable |
| `since < gap` → `==` | MISSED: gap never enforced, timing only |
| `since < gap` → `<=` | MISSED: equivalent |
| `since < gap` → `>` | killed only by Duration-underflow panics in multi-flush DST |
| `gap - since` → `+` | MISSED |
| `gap - since` → `/` | unviable |
| `!gather.is_zero()` with `!` deleted | MISSED: 1 ms timing |
| closure `>=` → `<` | MISSED: 250 ms stall per flush, the acker still acks |
| closure `||` → `&&` | MISSED: same |
| guard → `false` | MISSED: close path |
| guard → `true` | DST only |
| `!q.is_empty()` with `!` deleted | DST only |
| `drifted && (…)` → `||` | MISSED: equivalent while the thresholds are above 0 |
| `pend_reqs >= skip_reqs` → `<` | DST only |
| inner `||` → `&&` | DST only |
| `pend_bytes >= skip_bytes` → `<` | DST only |

`flush_ticker_loop`:

| Mutant | Outcome |
| --- | --- |
| FnValue | killable |
| `!=` → `==` | C0 test |
| `!idle.is_zero()` with `!` deleted | MISSED: redundant with the callee's guards |
| `||` → `&&` | MISSED: same reason |
| `max > 0` → `==`, `<`, `>=` | MISSED: same reason |
| `evicted > 0` → `==`, `<`, `>=` | MISSED: log only |
| `!` on trim debt deleted | C0 test |

That is about 15 MISSED, so CI fails. C3 removes every one by restructure, not by testing around them (RUST-QUALITY and the memory trap: "equivalent mutants must be restructured away").

### 5.2 Final state: every selected mutant in `src/shard/pump.rs` and its killer

Owner row: `pump`, filters `shard::pump:: shard::pump_tests::`.

| # | Mutant | Killer | Bound |
| --- | --- | --- | --- |
| 1 | `replace pump_loop with ()` | `shard::pump_tests::the_pump_alone_makes_a_committed_close_durable`. The required-role exit closes the engine at birth, so the reply is `Err(Moved)` or dropped | immediate |
| 2 | `replace flush_ticker_loop with ()` | the same test (engine closed), plus both ticker pins | immediate |
| 3 | `!=` → `==` in `flush_ticker_loop` | `the_ticker_flushes_the_memtable_once_appends_accumulated` | 12 s virtual |
| 4 | delete `!` in `!ticker.trim_debt…is_empty()` | `the_ticker_queues_a_trim_pulse_while_streams_owe_trims` | 12 s virtual |
| 5 | `replace ShardEngine::pending_ledger -> Option<PendingLedger> with None` | `the_ledger_sums_every_group_awaiting_durability`. Also the pump test: the pump never flushes, and SlateDB's 600 s timer does not fire | immediate / 10 s virtual |
| 6 | `... with Some(Default::default())` | UNVIABLE (no `Default` derive, on purpose) | n/a |
| 7 | `replace Barrier::read -> Option<Barrier> with None` | `the_barrier_waits_for_a_covering_watermark` | pure |
| 8 | `... with Some(Default::default())` | UNVIABLE | n/a |
| 9 | `>=` → `<` in `Barrier::read` | `the_barrier_waits_for_a_covering_watermark` (`read(7,false,7)`) | pure |
| 10 | `replace await_barrier -> Barrier with Default::default()` | UNVIABLE | n/a |
| 11 | `replace PendingLedger::busy -> bool with true` | `a_generation_at_either_threshold…` (`!waiting(3,63)`) | pure |
| 12 | `... with false` | same test (`waiting(4,0)`) | pure |
| 13 | `>=` → `<` (reqs) in `busy` | same test (`waiting(4,0)`) | pure |
| 14 | `>=` → `<` (bytes) in `busy` | same test (`waiting(0,64)`) | pure |
| 15 | `||` → `&&` in `busy` | same test (`waiting(4,0)`) | pure |

Totals: 12 viable, all CAUGHT; 3 unviable.
- `pump_loop` itself has no operator left. The gap is `checked_sub`, the window is `if/else`, the drift is `let-else`, and the barrier is an exhaustive match without a guard.
- The ticker's eviction guard is removed as redundant, and its log guard is `NonZeroUsize::new`.
- Every killer is paused-time or pure. None waits in real time, so a mutated run costs about the baseline plus seconds, far below `--timeout 90`. No TIMEOUT is possible from these.

### 5.3 Other owners selected by the push

- **`shard` (`src/shard.rs`, filter `shard::`):**
  - C1 selects `replace ShardEngine::start -> Arc<ShardEngine> with Arc::new(Default::default())`. It is UNVIABLE, because `ShardEngine` has no Default.
  - The deletion-adjacent lines in start carry no operators.
  - C0's end-of-file lines and the `mod`/`use` insertions carry no mutants.
- **`pump_tests`:** `#![cfg(test)]` means production-unchanged, so the planner omits it. Confirm it appears under `production_unchanged_files` in `plan.json`.
- **With C4:**
  - `shard` also selects `replace ShardEngine::committer_loop with ()` (see §5.4).
  - `queue_codec_tests` is a registered row but a cfg(test) module, so it has zero mutants and gets an explicit empty selection.
  - The other five test files are `#![cfg(test)]`, so they are omitted.
  - The `commit_group` deletion's adjacent lines (the blank line at 2544 and the cfg(test) `test_hold_commit` doc) select nothing.
- **Side effect:** `scripts/quality/*` changes mark tooling, so rust-quality also runs the saved-corpus and Miri steps. There is precedent from the earlier owner rows.
- **CI time:** about 15 mutants for `pump` plus 1 for `shard` (plus 1 with C4), at about 6 min each on CI plus baselines. That is roughly 2 h against `timeout-minutes: 240`.

### 5.4 C4 gate: `committer_loop → ()`

Commit 32b843f6 and the `commit_plan.rs:65` reason record `committer_loop`'s blank-body mutant as "no bounded test can observe". C4 must edit its body, so C4 selects it.

**Before writing C4's ledger edits,** commit C4's source change locally and run:
```
git diff HEAD~1 -- src/shard.rs > target/c4.diff
cargo mutants --in-diff target/c4.diff --file src/shard.rs --package streams-slate --cargo-arg=--locked --cargo-arg=--lib --cargo-test-arg=shard:: --baseline skip --profile quality --jobs 1 --timeout 90 --build-timeout 600 --gitignore true --output target/mut-84-c4
```
Expected: exactly one mutant, `committer_loop with ()`, **caught**. The committer exits on its first poll, `RequiredExit` fences the engine, and the dropped receiver fails every queued oneshot.

**If it is TIMEOUT:** drop C4 entirely and keep `commit_group`. Record in §8: "blank committer_loop still unbounded (test X hangs)". The payoff, a 3-line pass-through, does not justify bounding an unknown `shard::` wait inside this item. Do not bound the test ad hoc here; that is a separate item.

## 6. Ledgers

| Ledger | C0 | C1 | C2 | C3 | C4 |
| --- | --- | --- | --- | --- | --- |
| `scripts/quality/mutation_owners.py` | + `pump_tests` | + `pump` | – | – | – |
| `scripts/quality/test_mutation_owners.py` | – | + assert `pump` | – | – | – |
| `docs/quality/owners.json` | – | + macro-dsl `crate::flush_ticker_loop` | – | – | – |
| `docs/quality/source-allowances.json` | – | − `crate::ShardEngine::start` select row | – | – | – |
| `docs/refactor/architecture-policy.json` | – | − `function:src/shard.rs::start` | – | – | – |
| `docs/refactor/review-mechanisms.json` | – | – | – | – | r13 `sha256`; r03a `after_sha256` + reason |
| `docs/refactor/review-unit-relocations.json` | – | – | – | – | r13 `function_sha256` + reason |
| `docs/refactor/test-inventory.json`, scenario map/dispositions, `src/dst/tests/README.md` | unchanged (no DST test touched or renamed) | | | | |
| `docs/quality/diagnostic-allowances*.json` (0 warnings), `legacy-*` (immutable) | unchanged | | | | |
| `docs/review-followup/commit-transaction.md` | – | – | – | – | line 5 wording |

## 7. Controls

Use `export PATH=/private/tmp/claude-501/-Users-sorenschmidt-code-streams/4580d18c-ad9e-4e38-8ca1-89558fd0d592/scratchpad/pybin:$PATH` (the gate needs python 3.11 or later). Never run `gate.sh` and a mutation leg concurrently, and never edit the tree while `mutations.sh` runs.

**Step 0 (HEAD evidence for the reviewer's first step):**
- `cargo fmt --all -- --check; echo $?` gives `0`.
- `awk 'NR>=1476 && NR<=1731 && length>100 {print NR": "length}' src/shard.rs` gives `1548: 103` and `1658: 106`.
- `awk 'NR==1638||NR==1639 {match($0,/^ */); print NR": "RLENGTH}' src/shard.rs` gives `1638: 32` and `1639: 24`.

**C0:**
- `cargo test --locked --lib shard::pump_tests` gives `test result: ok. 3 passed`.
- Controls A, B and C (§3.2) each give `FAILED` with the stated message, then `git checkout src/shard.rs`.
- `wc -l < src/shard.rs` gives `3139`.

**C1:**
- `git show HEAD~1:src/shard.rs | sed -n '1477,1666p' | sed -E 's/^ {12}//' | diff - <(sed -n '<pump_loop body range>p' src/shard/pump.rs)` gives no output.
- The same with `'1681,1730p'` and `s/^ {8}//` against the ticker body gives no output.
- `git diff --color-moved=zebra --color-moved-ws=allow-indentation-change HEAD~1` shows both bodies as moved blocks.
- `cargo fmt --all -- --check` gives exit 1 with every `Diff in` line naming `src/shard/pump.rs`. This is the reviewer's "cargo fmt now rewrites those lines".
- `cargo clippy --locked --workspace --all-targets -- -D warnings` is clean, with no `unfulfilled_lint_expectations`. If the start cast expect is reported unfulfilled, delete it.
- `cargo test --locked --lib -- shard::pump_tests shard::task_lifecycle_tests` gives `ok. 9 passed`.
- `cargo test --locked --release --lib -- dst_tests::durability_gather:: dst_tests::runtime_open_gate::idle_engine_store_traffic_is_bounded_by_the_poll_cadence dst_tests::runtime_engine_lifecycle:: dst_tests::history_recovery::a_second_absorption_wave_trims_under_a_global_budget dst_tests::reads_applied::a_lost_applied_suffix_rewinds_to_the_durable_frontier` gives `ok. 8 passed`.
- Non-vacuity: put `return;` as `pump_loop`'s first statement. `the_pump_alone_makes_a_committed_close_durable` then FAILS. Revert.
- `python3 scripts/architecture-gate.py --check` passes, with no "obsolete budget exception" and no function growth.
- `wc -l < src/shard.rs` gives at most 2893.

**C2:**
- `cargo fmt --all -- --check` gives `0`.
- `git diff --stat HEAD~1` shows only `src/shard/pump.rs`.
- Token identity: `cargo build -p streams-quality-syntax`, then:
  ```
  python3 -c "import subprocess,sys;sys.path.insert(0,'scripts/quality');from common import syntax;a=subprocess.check_output(['git','show','HEAD~1:src/shard/pump.rs'],text=True);b=open('src/shard/pump.rs').read();p=syntax({'a.rs':a,'b.rs':b});print(p['a.rs']['tokens']==p['b.rs']['tokens'])"
  ```
  Expected: `True`.
- The 9 and 8 test legs above stay green.

**C3:**
- fmt and clippy are clean.
- `cargo test --locked --lib shard::pump` gives `ok. 7 passed`.
- The DST leg gives `ok. 8 passed`, and `shard::task_lifecycle_tests` gives `ok. 6 passed`.
- The three manual controls in §3.3 each FAIL their test, then revert.
- `scripts/quality.sh` gives `QUALITY_OK`.
- Local CI plan, with the change committed:
  ```
  export QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=24c4c77a4303e9b4b7a6cb09bfee26453efad657 QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BASE_REF=origin/slate
  python3 scripts/quality/verification_plan.py --out target/quality-plan
  ```
  `plan.json` should show:
  - `mutation_source_files: ["src/shard.rs", "src/shard/pump.rs"]`
  - `selected_mutation_owners: ["shard", "pump"]` (table order)
  - `unregistered_mutation_source_files: []`
  - `src/shard/pump_tests.rs` in `production_unchanged_files`
- `QUALITY_MUTANTS_OUT=target/quality-mutations scripts/quality/mutations.sh` should give:
  - `pump`: 15 mutants, 12 caught, 3 unviable, 0 missed, 0 timeout;
  - `shard`: 1 unviable;
  - exit 0.

**C4 (only if the §5.4 gate is caught):**
- `grep -rn "commit_group" --include='*.rs' src` gives only `src/dst/tests/seal_convergence.rs:264` (the test name).
- `cargo test --locked --lib shard::` is green.
- `python3 scripts/review-evidence.py --check` gives `review-evidence source inventory: OK`.
- Re-run the plan and `mutations.sh`: as C3, plus `shard`: `committer_loop` caught, and the `queue_codec_tests` empty selection.

**Pre-push (head):**
- `OUT=/private/tmp/claude-501/-Users-sorenschmidt-code-streams/4580d18c-ad9e-4e38-8ca1-89558fd0d592/scratchpad/gate-84.txt scripts/gate.sh` exits 0, with no `GATEFAIL` in `$OUT`.
- Push C0..C3 (or C4) in one `git push`.
- Verify per run with `gh run list --branch slate --json headSha,createdAt,name,status,conclusion`, matching `headSha`.

## 8. Out of scope

- `committer_loop`'s close-drain arm at `shard.rs:2457` is misindented and 112 columns wide inside `tokio::select!`, which rustfmt never formats. It is a separate cosmetic item.
- `dst_tests::durability_gather::the_gather_window_waits_for_ack_dispatch` polls the gather counter after release (`:71-79`) but asserts nothing afterwards, so its "ack then gather" tail is vacuous. Fixing it is a DST body edit with inventory churn and belongs to a separate item.
- Moving the pump's startup `tracing::info!` into `pump_loop` would drop `start`'s last cast expect. That is optional and not needed.
- Not touched:
  - `evict_idle_handles`, `dispatch_durable` and `acker_loop` bodies;
  - the 250 ms, 50 ms, 1 ms and 5 s literals (no new configuration);
  - the DST test name `seal_converges_through_transient_commit_group_failures`;
  - the historical `codereview1.md` and `BASELINE.md` mentions.
- If the §5.4 gate times out: `commit_group` stays and C4 is dropped (pre-decided).

## 9. Decisions for Søren

None. This changes no edge contract, status, body, `/v1/debug` or `/metrics` shape, or policy. The three internal deltas in §2 are unobservable. C4's fallback is pre-decided: drop the commit if `committer_loop → ()` is not bounded.

## Skeptic corrections (C1..Cn)

Everything below was checked read-only on `24c4c77a` (HEAD = origin/slate, clean). Verified as stated, with no correction needed:
- line numbers (1476/1479/1548/1575/1598/1638-1639/1658/1667/1679-1680/1697/1727/1731, 2541/2545, 3107/3110/3117);
- `wc -l src/shard.rs` = 3139, and the post-C1 arithmetic 3139 − 191 − 51 − 8 − 1 + 2 = 2890;
- the ten `commit_group` test call sites and the other grep lists;
- start's five attributes, and that the remaining casts in start are usize→i64/u64 (not truncation lints). The only unwraps and `let _` in start are in the moved bodies;
- `EnqueueError::Closed` from `try_command` (shard.rs:1817) plus `RequiredExit::drop` → `begin_close` (lifecycle.rs:126-133), so any blank loop is killed as soon as the engine starts;
- the exception ratchet is keyed by (path, qualified, kind, reason text) and skips new identities (source_rules.py:196, 204-209). No unwrap-scoped caller in shard.rs uses the names `pump_loop`, `flush_ticker_loop` or `pump`. The alias resolver reads only `use` items (quality-syntax imports.rs:10-23), so C1 needs no new reason;
- C4 committer_loop syntax_facts 5→5: a method call emits `method-call` plus `method-call-site`, and a call emits `call-site` plus `path` (scan.rs:221-252). Test-fn too_many_lines margins: r09 is 104 code lines, and each site is line-neutral or shorter. queue_codec r08 (97 lines) has no too_many_lines expect;
- the architecture gate measures from the `fn` line (architecture-report.py:159+), so C1's pump_loop is 198 lines;
- cargo-mutants 27.1.0: operator table at visit.rs:585-598; guard and arm rules at visit.rs:645-700; in-diff adjacency at in_diff.rs:203-263;
- the C3 mutant census: 15 mutants, 12 viable. Every killer is pure or paused-time, so all are bounded;
- equivalence of the C3 restructure, line by line: the barrier's precedence, the `frozen`/`window` continue order, and the drift/busy/after arithmetic;
- the ledgers for C1 and C4: source-allowances row at :3093, the architecture-policy start row at :40, review-mechanisms r13 at :110 and r03a at :1018, and review-unit-relocations r13 at :61. docs/dst/STATUS.md, test-inventory.json and test-relocations.json mention only the DST test *name* `seal_converges_through_transient_commit_group_failures`, so they need no edit.

**C1 — The predicted C2 diff is wrong, and C1's own wrapper lines will pollute it.**
- The `.fetch_add` chains will NOT be joined. rustfmt's `chain_width` defaults to 60, and every chain is wider:
  - `pump.pump_flushed_records.fetch_add(fl_records, Ordering::Relaxed)` is 66 columns;
  - the `pump_flushed_bytes` chain is 62;
  - the `pump_barrier_acked` chain is 66;
  - the `pump_gathers_skipped_busy` chain is 62.
- The old `:1658` chain `pump.in_flight.lock().unwrap().pending().iter().map(|g| g.reqs).sum()` is 69 wide. rustfmt WILL split it into about 8 lines.
- So `pump_loop` grows by about 6 lines at C2. It does not shrink by 5 (§4 C2 is wrong).
- That puts it at about 204 physical lines, above the 200 function budget. `architecture-gate.py` fails at C2. The head is unaffected (C3 is about 70 lines), so do not claim C2 is gate-clean. Do not run the architecture gate at C2.
- §4 C1 also writes the three `#[expect]`s as single lines over 100 columns, and its doc/signature is not in rustfmt shape. C2 would then re-wrap all of these too. That dilutes the review the reviewer asked for, which is only the formatting rustfmt used to skip.
- Fix: author every NEW line in C1 in the shape rustfmt would give it: the module doc, attributes, signatures, the `use` block and both spawn calls. Verify by formatting a scratch copy and diffing only the non-body lines. Then C2's diff is exactly the moved bodies.
- Update §4 C2's expected-diff list to: `:1639` re-indent, the `after` chain split, plus whatever else rustfmt reports.
- Also, the `cp ... /tmp/pump.c1` step should use the scratchpad directory, not `/tmp`.

**C2 — §5.4's C4 gate predicts the outcome the repo has already recorded against.**
- Commit 32b843f6's message and the expect reason at `src/shard/commit_plan.rs:63-66` both say the committer loop's blank-body mutants are ones "no bounded test can observe". The current decision to keep the error by value exists precisely so `committer_loop`'s body is never edited.
- C4 edits that body at shard.rs:2541, which selects `replace ShardEngine::committer_loop with ()`.
- The plan says "Expected: caught" and falls back only "If it is TIMEOUT". Corrections:
  - (a) Expect TIMEOUT or MISSED as the likely outcome.
  - (b) Make the fallback trigger on anything other than CAUGHT. MISSED fails the CI leg just as TIMEOUT does.
  - (c) Before spending a local run, look for existing evidence. The nightly rotation already mutates the whole `shard` owner (bucket 1, i.e. sha256("shard")[:4] % 7 == 1; `rust-quality.yml` schedule, `mutation_driver.py`). Its latest slot-1 `outcomes.json` records `committer_loop`'s current outcome, if that run finished.
  - (d) If C4 is dropped, §8/§9 must say plainly that the reviewer's "tests call CommitTransaction::run directly" is not delivered, and why.

**C3 — The inline `#[cfg(test)] mod tests` in `pump.rs` (C3) must not use `use super::*;`.**
- A glob emits an `unresolved-glob` fact (source_rules.py:49-50). `violations` then fails with "unregistered source occurrence" (source_rules.py:260-266) unless an owners.json row exists. The precedent is the registered glob row for `src/shard/transaction_tests.rs` at source-allowances.json:3762.
- Use explicit imports (`use super::{Barrier, PendingLedger};`, plus `crate::shard::...` for `InFlightGroup` and `DurableEffects`), as §4 C0 already does for `pump_tests.rs`.
- Otherwise, add the owners.json `unresolved-glob` row to the §6 ledger table.

**C4 — Test hygiene, which the plan leaves unspecified.**
- `await_terminated` returns `Result<(), String>` (shard.rs:1767-1770), and `unused_must_use` is `deny` (Cargo.toml:117). `let _ =` would trip `let_underscore_must_use`: clippy.toml's test allowances cover unwrap/expect/panic only, and the plan says the file has no expects.
- So every close in `pump_tests.rs` and `pump::tests` must be `.await.unwrap()` or asserted, and `try_close(..)` must be `.unwrap()`ed.
- The kill path for mutants #1/#2 and Control A is `try_close` returning `Err(EnqueueError::Closed)`: the engine is already fenced by RequiredExit before the test enqueues. It is not "reply `Err(Moved)` or dropped" (§3.2 table, §5.2 rows 1-2). The kill only holds if the test unwraps `try_close` or asserts on the reply.

**C5 — C3 doc placement and rustdoc.**
- The sketch puts `/// [the start-side rationale: ...]` on `pump_loop`, while §4 C1 keeps that same rationale at the spawn site (shard.rs:1455-1463). Keep it in one place. The spawn site is the right one, because the reviewer's "wiring list" reads it there.
- The `[...]` placeholders must not reach the file as bracketed text. `cargo doc --document-private-items` with `-D warnings` (RUST-QUALITY.md, Diagnostics) turns `[flush() resolves ...]` into a broken intra-doc link.
- The C1 docs ("Body of the group-commit pump ... spawns") and the `Barrier` doc ("it replaces a `Result<u64, bool>` ...") state what the code is or was, not why it exists. Reword per the style rule.

**C6 — C3 ticker import.**
- C3 adds a top-level `use slatedb::config::{FlushOptions, FlushType};` while the ticker body keeps its own in-body `use` of the same names. That is a redundant shadowing import.
- Drop the in-body `use` in C3; it is a one-line edit already inside the restructure commit. That avoids any `unused_imports` or `redundant_imports` diagnostic under 1.98.1 with `-D warnings`.
- Record it as a third C3 ticker edit. It is not "exactly two edits".

**C7 — C0's blank-line deletions at shard.rs:3107/3110 are unnecessary.**
- The ceiling is judged at the pushed head, which shrinks by about 249 lines. C0 is never pushed alone.
- The deletions are harmless but gratuitous churn inside the cfg(test) mod list. Prefer the plain +2 lines. If they are kept, drop the "wc gives 3139" C0 control, which then tests nothing.

**C8 — The `pump_tests` mutation_owners row (C0) is optional, and the plan should say so.**
- A `#![cfg(test)]` file is always `production_unchanged` (production_changes.py:62-65: `test_only_file` → `''`), so the row never selects anything.
- The six `#![cfg(test)]` siblings have no row: billing_read_tests, bounded_outbox_tests, commit_command_tests, maintenance_tests, queue_publication_tests and storage_decode_tests.
- Either convention passes. The `pump` row's filter `shard::pump:: shard::pump_tests::` is what matters, and it is correct. `shard::pump::` is not a substring of `shard::pump_tests::`, so both filters are needed.

**C9 — §5.3 and §7 C4 local gate.**
- Run the §5.4 command with the same `--baseline run` the driver uses (mutation_driver.py:46).
- `--baseline skip` hides an unrelated baseline hang in `shard::`, and the CI leg would report that hang as the committer_loop TIMEOUT.

Verdict: **ready-with-corrections.**
- The C0→C3 core is sound:
  - the move is verbatim and compiles as child-module access;
  - the ratchet, alias, owners, allowances and architecture ledgers are right;
  - the C3 restructure is equivalent;
  - its 12 viable mutants are all killed by bounded pure or paused-time tests.
- C1 and C3 must be applied before execution.
- C4 should be planned as "expected to be dropped" unless the nightly `shard` evidence or the local gate shows CAUGHT.
