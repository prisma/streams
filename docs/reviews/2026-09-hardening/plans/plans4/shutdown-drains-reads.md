# Item 26: a graceful stop seals and drains the active read window

Repository `/Users/sorenschmidt/code/streams`, branch `slate @ 8dabca7f` (tree clean at read time). Read-only verification; nothing in the repo was touched. Every line number below is from the current tree, not the reviewer's.

**Verdict: the problem is real and current.** Both cancel arms of the `telemetry-drain` loop `return crate::tasks::TaskResult::Done` with no terminal round; the only production seal is the aged one inside `drain_once`; the `seal_if_aged(0)` "shutdown entry" exists in the accumulator but no production caller uses it. Every SIGTERM drops the active read window (up to 10 s of deltas) plus any batch a cut round had taken (those are requeued in memory by `ReadDrain`'s Drop and then die with the process).

**The reviewer's Change is right in substance but unbuildable as written**, for two reasons that the plan corrects:

1. `src/billing.rs` is exactly at its no-growth ceiling (2,300 of 2,300). The terminal round adds lines; they cannot go into `billing.rs`. The drain loop moves verbatim into a new sub-module first (the `system_append.rs` pattern, commits aa445217 + 8dabca7f), then the fix lands there.
2. An *unbounded* terminal round turns an existing regression red and steals the shard close's time under a wedged store. `r09_active_telemetry_cancels_entered_storage_and_preserves_debt` holds the spool's SST PUT with `hold_class(.., u64::MAX)` and asserts `report.aborted.is_empty()` under a 300 ms grace ("active passes must stop cooperatively", R09's recorded contract in `docs/review-storage-evidence.md`). A terminal `drain_once` re-enters `persist_all`, parks on the same hold, and is aborted by the supervisor at the grace deadline. In production the same wedge would consume the whole 10 s task grace before `shards.shutdown` gets its own 10 s. The buildable contract bounds the terminal round by **one drain cadence** (`TELEMETRY_DRAIN_SECS`, already a knob, default 2 s) with `tokio::time::timeout`; the cut round's batches stay Drop-owned exactly as an interrupted ordinary round's do. No new knob, no spool-only step, no `drain_once` signature change, otherwise the reviewer's shape.

---

## 1. Problem (verified)

### 1.1 The cancel arms return without a terminal round — `src/billing.rs:1203-1249`

```
1203  if let Err(rejected) = tasks.spawn(
1204      "telemetry-drain",
1205      crate::tasks::Policy::Critical,
1206      move |cancel| async move {
1207          let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs.max(1)));
1208          let mut last_metrics = None;
1209          loop {
1210              tokio::select! {
1211                  _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
1212                  _ = tick.tick() => {}
1213              }
1214              tokio::select! {
1215                  biased;
1216                  _ = cancel.cancelled() => return crate::tasks::TaskResult::Done,
1217                  _ = async {
1218                      match drain_once(&state).await {
...
1244          }
1245      },
```

Line 1211: cancellation between ticks returns at once. Line 1216: cancellation during a round drops the in-flight `drain_once` future (biased select) and returns at once. Nothing after the loop.

### 1.2 The only production seal is the aged one — `src/billing.rs:784` (the reviewer's `:781`)

```
784      state.billing.seal_aged_reads(READ_FLUSH_INTERVAL_MS);
```

`READ_FLUSH_INTERVAL_MS = 10_000` (`src/billing/read_accumulator.rs:8`). A window younger than 10 s is never sealed by a round, so the last window before a stop is never sealed at all.

### 1.3 The shutdown entry exists and is unused — `src/billing/read_accumulator.rs:144-159`

```
144      /// Timer/shutdown entry: seal if the active interval is at least
145      /// `max_age_ms` old (0 = unconditionally).
...
150      pub(crate) fn seal_if_aged(&self, max_age_ms: i64) {
```

`grep -rn "seal_aged_reads(\|seal_if_aged(" src` shows one production caller (`billing.rs:784`, aged) and only test callers for `(0)` (`billing_usage.rs`, `billing_attribution.rs`, `security_audit.rs`, `security_subscription.rs`, `read_accumulator/tests.rs`). The doc's "shutdown" half was never wired.

### 1.4 The documented contract — `docs/OBSERVABILITY-BILLING.md`

```
101  - graceful shutdown and drain wait for the final usage batch;
...
376  shutdown/drain            (in the §7.2 flush-trigger list)
...
421  - Graceful stops flush all read usage before exit.
```

Line 101 is the reviewer's citation; 376 and 421 make the same promise. None of the three is implemented.

### 1.5 What a stop actually does today — `src/bootstrap.rs:900-919`, `src/tasks/shutdown.rs:181-192`

```
bootstrap.rs
900      let served = crate::http::serve_h1(listener, app, &config.http, tasks.clone()).await;
907      let report = tasks.shutdown(std::time::Duration::from_secs(10)).await;
914      shards
915          .shutdown(std::time::Duration::from_secs(10))
```

The signal task (`bootstrap.rs:735-747`) calls `request.request()` → `TaskSupervisor::cancel()` (`tasks.rs:391-399`): phase `ShuttingDown` and the cancel watch flips for EVERY loop at the instant of SIGTERM. `serve_h1` breaks its accept loop (`http.rs:1309`), aborts and joins its connections (`http.rs:1336-1337`), then `tasks.shutdown(10 s)` joins every loop; only then `shards.shutdown`. So during the task join the engines, registry and ownership are all still live — a terminal ledger append is possible — and the supervisor's own bound is the abort at `deadline = now + grace` (`shutdown.rs:182,186,191`), which drops the task's future (Drop custody runs).

Neither the fleet loop (`fleet.rs:492,503,871` — every arm `return Done`) nor `request-maintenance` fences appends on cancel; the only consumer of the supervisor phase is `/health` (`http.rs:1846`). `system_append` → `append_typed` needs no `request_work` ticket for `_usage` (TTL touch is only for streams with a TTL; topology resume only for pending transitions).

### 1.6 Why the reviewer's literal Change breaks an existing regression — `src/dst/tests/billing_controller.rs:62-86`, `src/dst/fault_store.rs:237-243`

```
billing_controller.rs
 62      let write_entered = spool_store.hold_class(StoreOp::Put, ObjClass::Sst, u64::MAX);
 63      let list_entered = catalog_store.hold_class(StoreOp::List, ObjClass::Other, u64::MAX);
...
 82      let report = tasks.shutdown(Duration::from_millis(300)).await;
 83      assert!(
 84          report.aborted.is_empty(),
 85          "active passes must stop cooperatively: {report:?}"
 86      );
fault_store.rs
237      if let Some((gate, engaged, max_parked)) = held {
240          if engaged.fetch_add(1, Ordering::SeqCst) < max_parked {
242              let _ = gate.acquire().await;
```

`max_parked = u64::MAX` parks every SST PUT until `release_hold()` (line 105, after the report). An unbounded terminal `drain_once` re-enters `spool_sealed_reads` → `persist_all` → parks → the supervisor aborts it at 300 ms → `report.aborted == ["telemetry-drain"]`, outcome `Cancelled`. The R09 contract ("cooperative shutdown without forced abort", `docs/review-storage-evidence.md` §R09) would be reversed, and the test is pinned by hash in `docs/refactor/review-mechanisms.json` (mechanism `active-telemetry-cancellation`, `configuration.shutdown_ms: 300`).

### 1.7 Line budget facts

| File | Now | Ceiling (merge-base ratchet, `source_rules.violations`) |
|---|---|---|
| `src/billing.rs` | 2,300 | 2,300 — may not grow by one line |
| `src/billing/telemetry_loop.rs` (new) | 0 | 1,000 |
| `src/dst/tests/billing_controller.rs` | 505 | 1,000 |
| `src/billing/read_accumulator.rs`, `src/billing_service.rs`, `src/tasks.rs`, `src/bootstrap.rs` | untouched | — |

`spawn_telemetry` (`billing.rs:1157-1249`) is 90 Clippy-counted lines (93 physical, 3 comment lines) against the 100-line limit — 10 lines of headroom, enough for the fix once the terminal round is its own function.

---

## 2. Contract decision

**Typed / behavioural contract (no wire change).**

1. On cancellation the `telemetry-drain` loop leaves its cadence (`break` in both arms) and runs exactly one **terminal round**:
   - `state.billing.seal_aged_reads(0)` — the active read window seals whatever its age (the accumulator's own documented shutdown entry, `read_accumulator.rs:144-145`);
   - one ordinary `drain_once(&state)` — spool first (durable before the ledger is asked), then the `_usage` append, then the spool/outbox acknowledgements; same idempotence, same `ReadDrain` custody on a cut.
2. The terminal round is bounded by **one drain cadence**: `tokio::time::timeout(cadence, drain_once(state))` where `cadence = Duration::from_secs(telemetry_drain_secs.max(1))` — the value the loop already sleeps between rounds. Rationale: the loop would have retried after one cadence; at a stop there is no retry, so the round gets exactly one. A store that cannot take one round inside one cadence is the ledger-outage case the durable spool already owns. This keeps R09 (cooperative stop under a wedged store, no supervisor abort) and keeps the terminal round inside the process's 10 s task grace with the shard close's 10 s untouched.
3. Outcomes are the existing typed ones: the task returns `TaskResult::Done` after the terminal round in every case, so the `ShutdownReport` shows `("telemetry-drain", TaskOutcome::Finished)`. A round that errs logs `usage drain at shutdown: {e}`; a round the cadence cuts logs `usage drain at shutdown did not finish inside {cadence:?}; the spool keeps what the store accepted`. Success records `drain_succeeded(now)` like an ordinary round.
4. What is now guaranteed: with a responsive store, every read metered before the cancel instant reaches the durable spool and the `_usage` ledger before the process exits; the operator gauges (`unflushed_reads()`) read `(0, 0, 0)` after the stop. What is not: deltas metered by connections the accept loop is aborting in the same instant (`http.rs:1336`) can land after the seal — that is scheduler-latency-sized and is the hard-loss case; and when the sealed queue is full (`READ_SEALED_MAX_BATCHES`, a long ledger outage) `seal_locked` defers and the active window remains the hard-loss window, as documented.
5. Wire codes: **none change.** No HTTP status, header, or body changes; `/operator`'s `possibleReadLossWindowSeconds` keeps its hard-loss meaning. `docs/refactor/WIRE-MATRIX.md` needs no row.
6. The outbox-sweep task, `spawn_rollup`, `drain_ops_once`, `drain_audit_once`, `drain_fleet_events` are untouched (see §8).

**Operator invariant to document:** `TELEMETRY_DRAIN_SECS` must stay below the supervisor grace (10 s in `bootstrap.rs:907`); above it, the supervisor's abort is the bound and custody still holds (nothing is lost that is not lost today). Not validated in config — it degrades, it does not corrupt.

---

## 3. Red tests

All in `src/dst/tests/billing_controller.rs` (505 → ≈580 lines; no new DST module, so no README/owners by-path rows). Module path for `--exact`: `dst::dst_tests::billing_controller::<name>`.

### 3.1 New: `graceful_stop_seals_and_drains_the_active_read_window` — RED on the current tree

Rig: `http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default())`, the billing clock read-locked like the other controller tests, a `ReadSpool::open(state.data_store.clone(), "", "stop-drain", &state.config)` installed via `state.billing.install_read_spool` BEFORE spawning (so `open_read_spool` in the sweep task returns at once and the terminal round takes the spool path deterministically), an own `TaskSupervisor` (as r09), `crate::billing::spawn_telemetry(state.clone(), &tasks)`.

This test stops the loop **between ticks** (the first cancel arm): the rig's first tick fires immediately and its round ends with the metrics emission (`emit_metrics_once` → `_ops_metrics`, `ops.rs:600-618`), so wait, bounded to 3 s by `tokio::time::timeout` around a `yield_now` loop, until `crate::billing::system_read(&state, crate::billing::OPS_METRICS_STREAM, &key, None)` returns a body whose `Vec<serde_json::Value>` has one record — from then on the loop sits in `tick.tick()` for the rest of the 2 s cadence. (3.2 covers the mid-round arm deterministically.)

Then meter one read against a fixed identity (`account_id: "acct"`, `project_id: "proj"`, `stream_id: "ab".repeat(8)`, `stream_name: "orders"`; `RowDelta { read_payload_bytes: 4096, read_records: 3, read_operations: 1, ..Default::default() }`), and:

```rust
assert_eq!(state.billing.unflushed_reads(), (1, 150, 0),
    "the window is younger than the flush interval: nothing seals before the stop");
let report = tasks.shutdown(Duration::from_secs(5)).await;
assert!(report.aborted.is_empty(), "the terminal round must finish inside the grace: {report:?}");
assert_eq!(report.outcomes.len(), 2);
assert!(report.outcomes.iter().all(|(_, o)| *o == crate::tasks::TaskOutcome::Finished), "{report:?}");
assert_eq!(state.billing.unflushed_reads(), (0, 0, 0), "a graceful stop left read usage behind");
assert!(spool.pending(10).await.unwrap().is_empty(), "the ledger acknowledged the batch, so the spool released it");
let key = state.billing.usage_key().unwrap();
let (body, _) = crate::billing::system_read(&state, crate::billing::USAGE_STREAM, &key, None)
    .await.unwrap().expect("_usage exists after the terminal round");
let envelopes: Vec<crate::billing::UsageEnvelope> = serde_json::from_slice(&body).unwrap();
let rows: Vec<&crate::billing::ReadRow> = envelopes.iter()
    .filter_map(|e| match &e.payload {
        crate::billing::UsagePayload::ReadBatch(b) => Some(&b.rows),
        crate::billing::UsagePayload::SegmentSnapshot(_)
        | crate::billing::UsagePayload::StreamLifecycle(_)
        | crate::billing::UsagePayload::UsageCorrection(_) => None,
    })
    .flatten().collect();
assert_eq!(rows.len(), 1, "exactly the one metered row reached the ledger");
assert_eq!(rows[0].identity, identity);
assert_eq!((rows[0].read_payload_bytes, rows[0].read_records, rows[0].read_operations), (4096, 3, 1));
spool.close_for_tests().await;
rig.shutdown().await;
```

(150 = 120 + 4 + 4 + 16 + 6, the accumulator's `est_bytes` for one new identity, `read_accumulator.rs:90-94`. `ReadRow` has no `PartialEq`; fields are compared. No `_ =>` arm on `UsagePayload`. `rig.shutdown()` last, as r09.)

**Exact expected red output on the current tree** (the first five assertions pass: both loops return `Done` at once, nothing is aborted):

```
thread 'dst::dst_tests::billing_controller::graceful_stop_seals_and_drains_the_active_read_window' panicked at src/dst/tests/billing_controller.rs:<line of the (0, 0, 0) assert>:5:
assertion `left == right` failed: a graceful stop left read usage behind
  left: (1, 150, 0)
 right: (0, 0, 0)
```

The active row is still in the map (never sealed), nothing was spooled or appended. After the fix the same assertion also distinguishes a terminal round the cadence cut (the batch would be requeued: `left: (0, 0, 1)`) from one that landed (`(0, 0, 0)`), so a slow rig fails with a readable value rather than a timeout.

### 3.2 Modified: `r09_active_telemetry_cancels_entered_storage_and_preserves_debt` — the mid-round stop, the bound from both sides — RED on the current tree

The cancel here arrives while `drain_once` is parked inside `persist_all` (the second, biased arm). Two edits around line 82:

```rust
    // Item 26: a stop runs one terminal round; it parks on the same held
    // PUT and is cut after one drain cadence, so the grace sits above the
    // cadence and the stop still needs no abort (R09).
    let cadence = Duration::from_secs(state.config.billing.telemetry_drain_secs);
    let stop = std::time::Instant::now();
    let report = tasks.shutdown(Duration::from_secs(5)).await;
    assert!(
        report.aborted.is_empty(),
        "active passes must stop cooperatively: {report:?}"
    );
    assert!(
        stop.elapsed() >= cadence,
        "the terminal round must be attempted and given one cadence, not {:?}",
        stop.elapsed()
    );
```

(`telemetry_drain_secs` is 2 in the rig — `CliArgs::deterministic()` + empty environment, `config/model.rs:442`.) Every existing assertion stays: `outcomes.len() == 2`, all `Finished`, the batch recovered from memory with exact bytes (`ReadDrain` Drop requeues it when the timeout drops the cut round), walk cursor `None`, then release-and-retry with the same source/sequence identity — the hold also releases the terminal round's parked flush, which persists the same key, so `spool.pending(10)` still holds exactly the expected bytes.

**Exact expected red output on the current tree** (both loops return `Done` at once; `aborted` is empty; the elapsed check fires):

```
thread 'dst::dst_tests::billing_controller::r09_active_telemetry_cancels_entered_storage_and_preserves_debt' panicked at src/dst/tests/billing_controller.rs:<line of the elapsed assert>:5:
the terminal round must be attempted and given one cadence, not 1.4ms
```

(the duration is whatever the immediate join took, always far below 2 s.)

With the fix and the bound: passes in ≈2.1 s more than today (the terminal round times out at one cadence; the join completes inside the 5 s grace). With the fix but WITHOUT the bound (the reviewer's literal change): `active passes must stop cooperatively: ShutdownReport { outcomes: [("telemetry-outbox-sweep", Finished), ("telemetry-drain", Cancelled)], aborted: ["telemetry-drain"] }`. With the fix and the OLD 300 ms grace: the same abort message — which is why the literal must change. So this one test pins the round from below (it is attempted and waits) and from above (it does not outlive the cadence).

### 3.3 Coverage of the two cancel arms

3.1 stops the loop between ticks (first arm), 3.2 stops it mid-round (second arm); both arms must reach the terminal round, and each test is red on the current tree through its own arm. No separate "bounded" test exists because on the current tree there is no round to bound; 3.2's elapsed window is its regression.

---

## 4. Edits, file by file

Two commits, pushed as a pair, each gateable on its own (no ratcheted caller's fingerprint moves — checked in 4.5).

### Commit 1 — `Move the telemetry loops into billing/telemetry_loop.rs, verbatim`

**`src/billing/telemetry_loop.rs` (new, ≈100 lines):**

```rust
//! The telemetry loops, spawned under the runtime's supervisor: the read
//! spool's opener and ownership sweep, and the ledger drain cadence.

use super::{drain_once, open_read_spool, sweep_owned_outboxes};

<lines 1155-1249 of src/billing.rs, byte-identical>
```

The body already spells every other path absolutely (`crate::tasks::…`, `crate::ops::…`, `crate::audit::…`, `crate::fleet::…`, `crate::http::AppState`, `tokio::…`, `tracing::…`); the three bare callees resolve through the one `use super::…` (a child module sees the parent's items; all three are `pub(crate)` anyway).

**`src/billing.rs`:** delete lines 1155-1250 (doc comment, function, trailing blank: 96 lines); after line 35 add

```rust
mod telemetry_loop;
pub(crate) use telemetry_loop::spawn_telemetry;

```

Result: 2,300 − 96 + 3 = **2,207 lines** (budget 2,300; the ratchet is `min(max(1000, adoption), max(1000, merge-base))` = 2,300, and after this commit the next merge base lowers it to 2,207 — say so in the message). Every caller keeps `crate::billing::spawn_telemetry` (`bootstrap.rs:888`, `billing_controller.rs:72`).

**Ledgers for commit 1** (all mandatory, verified against the gates):

- `docs/refactor/architecture-policy.json`: add `"src/billing/telemetry_loop.rs"` to `transport_and_composition_files` and a `transport_rationales` entry ("Telemetry drain loop: spawns the outbox sweep and the ledger cadence under the runtime supervisor and owns the terminal round a graceful stop performs; it reads the composed AppState and decides nothing about ownership or admission — drain_once and the services own those."). Without the row `architecture-gate.py:117-120` fails `reverse dependency growth: src/billing/telemetry_loop.rs -> crate::http: 1 > 0` (a new file's baseline is `edges: {}`).
- `docs/quality/owners.json`: add `{"category": "macro-dsl", "count": 5, "owner": "crate::spawn_telemetry", "path": "src/billing/telemetry_loop.rs", "reason": "Supervised telemetry loops select cancellation at every iteration boundary and around each active pass (R09); the sweep, the cadence and the cooperative-stop contract are exercised by the billing_controller DST rigs.", "syntax": "tokio::select"}`. `source_rules.classify` makes every `tokio::select!` a `macro-dsl` occurrence keyed by (path, enclosing item, macro); the five sites currently live under `("macro-dsl", "src/billing.rs", "crate::spawn_telemetry", "tokio::select")` in `docs/quality/source-allowances.json` and `legacy-source.json`. Moving them makes the new identity `unregistered source occurrence (5)` until owned.
- `docs/quality/source-allowances.json`: the old `src/billing.rs` row becomes stale (`5 obsolete source allowances; run the quality ratchet with --prune`). Prune it with `gate.py --prune` after a green clippy JSON run (commands in §7); `git diff` must show exactly that one row removed. `legacy-source.json` is the immutable ceiling and is not touched.
- Nothing else: no test bodies change (test-inventory unchanged), no `#[expect]` moves, no mt-lint markers (the moved code has no `name: String` param and no `.stream_ref(`), no mutation-owner row (see 4.4), no WIRE-MATRIX row.

### Commit 2 — `A graceful stop seals the active read window and drains it once, inside one cadence`

**`src/billing/telemetry_loop.rs`** (final shape, ≈135 lines):

```rust
//! The telemetry loops, spawned under the runtime's supervisor: the read
//! spool's opener and ownership sweep, and the ledger drain cadence. The
//! drain loop also owns the one terminal round a graceful stop owes the
//! accuracy contract (OBSERVABILITY-BILLING §2.3, §7.4): the active read
//! window is sealed and drained before the loop reports itself finished,
//! inside one cadence, so a wedged store still stops cooperatively (R09).

use super::{drain_once, open_read_spool, sweep_owned_outboxes};
use std::sync::Arc;
use std::time::Duration;

/// The drainer task: every TELEMETRY_DRAIN_SECS (default 2), one drain
/// round. Errors log and retry — the durable outbox holds the truth.
pub(crate) fn spawn_telemetry(
    state: Arc<crate::http::AppState>,
    tasks: &crate::tasks::TaskSupervisor,
) {
    // …usage_key check and the outbox-sweep spawn, unchanged…
    let cadence = Duration::from_secs(state.config.billing.telemetry_drain_secs.max(1));
    let metrics_secs: u64 = state.config.billing.metrics_interval_secs;
    if let Err(rejected) = tasks.spawn(
        "telemetry-drain",
        crate::tasks::Policy::Critical,
        move |cancel| async move {
            let mut tick = tokio::time::interval(cadence);
            let mut last_metrics = None;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tick.tick() => {}
                }
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    _ = async { /* the round, unchanged */ } => {}
                }
            }
            terminal_round(&state, cadence).await;
            crate::tasks::TaskResult::Done
        },
    ) {
        tracing::warn!("telemetry-drain not spawned: {rejected:?}");
    }
}

/// A graceful stop owes the ledger the window the cadence had not
/// reached yet (§7.4 "graceful stops flush all read usage"): seal it
/// whatever its age, then one ordinary round — spool first, so what the
/// store accepts is durable before the ledger is asked. One cadence bounds
/// the round: the supervisor's grace belongs to the whole process, and a
/// store that cannot take one round inside one cadence is the outage the
/// spool already owns; the `ReadDrain` guard requeues whatever a cut round
/// still held, exactly as it does for an interrupted ordinary round.
async fn terminal_round(state: &Arc<crate::http::AppState>, cadence: Duration) {
    state.billing.seal_aged_reads(0);
    match tokio::time::timeout(cadence, drain_once(state)).await {
        Ok(Ok(_)) => state
            .runtime
            .telemetry
            .drain_succeeded(state.runtime.clock.now()),
        Ok(Err(e)) => tracing::warn!("usage drain at shutdown: {e}"),
        Err(_elapsed) => tracing::warn!(
            "usage drain at shutdown did not finish inside {cadence:?}; the spool keeps what the store accepted"
        ),
    }
}
```

Line-neutral substitutions inside `spawn_telemetry`: `let secs …` → `let cadence …` (1 → 1), `from_secs(secs.max(1))` → `cadence`, `return … Done` → `break` twice; additions: `terminal_round(&state, cadence).await;` and `crate::tasks::TaskResult::Done` (+2) → **92 Clippy lines, limit 100.** Nesting: `break` inside `tokio::select!` arms inside `loop` is the `serve_h1` precedent (`http.rs:1309`); `terminal_round` nests fn → match → arm (depth 3). `tokio::time::timeout` is a function (no macro-dsl row) and is already used in production owners (`auth_feed.rs`, `history.rs`, `shard.rs`, `sharddir.rs`); nothing in `clippy.toml`'s `disallowed_methods` covers it. `Duration` is `Copy`, so moving `cadence` into the closure and passing it by value is lint-clean. Five `tokio::select!` sites remain under `crate::spawn_telemetry` → the owners row from commit 1 stays at `count: 5`, no ledger churn. Doc comments contain no `<…>`/`[…]` (rustdoc `-D warnings`).

Why a separate `terminal_round` rather than inline: it keeps `spawn_telemetry` under 100 lines with margin, gives cargo-mutants a body to replace (§5), and is the one place the doc sentence about the bound lives.

**`src/dst/tests/billing_controller.rs`:** the new test (3.1) after r09; r09's stop block as in 3.2 (grace 300 ms → 5 s, the `cadence`/`stop` bindings and the elapsed assertion). No import changes (full paths inline), so the inventory diff is exactly two function hashes. Both tests stay under 100 Clippy lines and nesting 4 (the `filter_map` match is depth 3).

**`docs/OBSERVABILITY-BILLING.md`** (no ceiling on docs): line 101 → "graceful shutdown seals the active read window and waits one drain cadence (`TELEMETRY_DRAIN_SECS`) for the final batch to reach the spool and the ledger;"; line 421 → "Graceful stops seal the active read window and drain it in one terminal round bounded by one drain cadence; what the spool accepted is durable, and a store that does not answer inside the cadence leaves the batch under the same custody as an interrupted round." Line 376 (`shutdown/drain` as a flush trigger) becomes true and stays.

**No edit** to `src/billing.rs`, `billing_service.rs`, `read_accumulator.rs`, `tasks.rs`, `tasks/shutdown.rs`, `bootstrap.rs`, `http.rs`.

### 4.3 `#[expect]`-ratcheted functions touched: none

- `spawn_telemetry` carries no `#[expect]` (`billing.rs:1155-1157`: doc comment, then the signature).
- `drain_once` (`billing.rs:762-769`, `too_many_lines` + `excessive_nesting`) is not edited; its contract identity is `(path, qualified, kind, value)` and its `scope_lines`/`nested_items`/`syntax_facts` are unchanged by deleting lines below it.
- `bootstrap::run` (`bootstrap.rs:116-140`, `unwrap_used` + `expect_used`, so every call and path site under it is fingerprinted with lexical import aliases resolved) calls `crate::billing::spawn_telemetry(state.clone(), &tasks)` at 888 with an absolute path and no `use` alias in that file; the re-export keeps the spelling and the resolution identical → no `accepted exception grew`. This is the trap that made the `product/scan.rs` move un-gateable alone; it does not apply here, but §7 runs the gate on commit 1 alone to prove it.
- The new file and the DST changes carry no `#[expect]`.

### 4.4 Mutation owner registration: deliberately none

`src/billing.rs` was never a registered owner nor under a critical prefix; the only billing prefixes in `verification_plan.CRITICAL_PREFIXES` are `src/billing/read_accumulator` and `src/billing/read_spool`. `src/billing/telemetry_loop.rs` therefore inherits the same posture: not mutation-selected. Registering it would put the whole loop under the missed-mutant-fails rule, and the metrics-interval predicate `now.since(previous) >= Duration::from_secs(metrics_secs)` (default 15 s, no rig override) has `>=`→`>`/`<`/`==` mutants no bounded test can kill — that is a separate decision (§8). The terminal round's mutants are analysed in §5 so a later registration is safe.

### 4.5 Line budgets after both commits

| File | Before | After c1 | After c2 | Budget |
|---|---|---|---|---|
| `src/billing.rs` | 2,300 | 2,207 | 2,207 | 2,300 (then 2,207) |
| `src/billing/telemetry_loop.rs` | — | ≈100 | ≈135 | 1,000 |
| `src/dst/tests/billing_controller.rs` | 505 | 505 | ≈580 | 1,000 |
| `docs/OBSERVABILITY-BILLING.md` | 1,288 | 1,288 | 1,288 (two lines reworded) | n/a |

---

## 5. Mutation-kill analysis (per predicate, whether or not CI selects the file)

| New code | cargo-mutants variant | Killed by |
|---|---|---|
| `terminal_round` body | → `()` (Default) | 3.1: `unflushed_reads() == (0, 0, 0)` fails with `(1, 150, 0)`; `_usage` absent (`expect("_usage exists …")`). |
| `seal_aged_reads(0)` | (literals are not mutated; the human regression is `READ_FLUSH_INTERVAL_MS`) | 3.1: the window is < 10 s old, so an aged seal leaves `(1, 150, 0)`. |
| `tokio::time::timeout(cadence, drain_once(state))` | drop the timeout (human regression; cargo-mutants does not mutate calls) | 3.2: the parked round is aborted by the supervisor → `aborted == ["telemetry-drain"]`. |
| `match … { Ok(Ok(_)) => drain_succeeded, Ok(Err(e)) => warn, Err(_) => warn }` | arms are not deleted by cargo-mutants; `drain_succeeded` call → no-op is not a generated mutant | Observed indirectly: 3.1 reads `_usage` (side effect of the `Ok(Ok)` path); the `Err` paths log only. |
| `break` ×2 (was `return Done`) | cargo-mutants does not mutate control-flow keywords; the human regression is a `return` left in one arm | First arm (between ticks): 3.1 — a `return` skips the terminal round → `left: (1, 150, 0)`. Second arm (mid-round, biased): 3.2 — a `return` joins in milliseconds → `the terminal round must be attempted and given one cadence, not …ms`. Each test drives exactly one arm (3.1 waits for the first round to end; 3.2 cancels inside a parked persist). |
| `tokio::time::timeout` present but `cadence` wrong (e.g. the grace or `Duration::MAX`) | not a generated mutant | 3.2: `aborted == ["telemetry-drain"]` for anything ≥ 5 s; `stop.elapsed() >= cadence` for anything below 2 s (e.g. `Duration::ZERO`, which would also leave 3.1 at `(0, 0, 1)`). |
| `spawn_telemetry` body | → `()` | r09: `write_entered`/`list_entered` never move → `expect("both real storage operations must be entered …")` fails at 3 s; 3.1: `outcomes.len() == 2` fails with 0. |
| `telemetry_drain_secs.max(1)` | method calls not mutated | — (and `interval(Duration::ZERO)` would panic at spawn on any rig with `telemetry_drain_secs = 0`; no rig sets it). |
| `last_metrics.is_none_or(… >= …)` (pre-existing, moved) | `>=` → `>`, `<`, `==` | NOT killable in bounded time (15 s default interval) — the reason the file stays unregistered (4.4). |

Bounded waits: 3.1 uses `tasks.shutdown(5 s)` (the supervisor's own deadline), `rig.shutdown()` (5 s + 10 s), no unbounded loops; 3.2 keeps its existing 3 s `expect` on the hold counters and a 5 s grace. No `TIMEOUT` (= miss) exposure.

---

## 6. Ledgers

Commit 1:
- `docs/refactor/architecture-policy.json` — `transport_and_composition_files` + `transport_rationales` (4.1).
- `docs/quality/owners.json` — the `macro-dsl` / `tokio::select` row for `src/billing/telemetry_loop.rs::crate::spawn_telemetry`, count 5, with reason (4.1).
- `docs/quality/source-allowances.json` — pruned by the gate (one row removed).

Commit 2:
- `docs/refactor/test-inventory.json` — `python3 scripts/test-inventory.py --write`; the diff must be exactly: one new entry (`graceful_stop_seals_and_drains_the_active_read_window`, attributes `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`) and the changed `function_sha256` of `r09_active_telemetry_cancels_entered_storage_and_preserves_debt`.
- `docs/refactor/review-mechanisms.json` — mechanism `active-telemetry-cancellation` (`mechanisms[11]`): `tests[0].sha256` ← the new `function_sha256` from the inventory (the two hash the same canonical tokens; today both read `b9e0d05d…`), `configuration.shutdown_ms: 300` → `5000` plus `"terminal_round_cadence_secs": 2`, and append to `oracle`: "… the terminal drain round parks on the same held PUT, is given one cadence and cut, so the stop takes at least one cadence and still needs no abort". `scripts/review-evidence.py --check` fails `mechanism test changed or missing` otherwise.
- `docs/refactor/test-additions.json` — entry `[13]` (finding R09) is not checked against the tree by any gate; leave it, it records the original addition.
- `docs/OBSERVABILITY-BILLING.md` — §2.3 line 101 and §7.4 line 421 (4.2).
- `docs/quality/owners.json` — unchanged from commit 1 (still five `select!` sites under `crate::spawn_telemetry`).
- `docs/refactor/WIRE-MATRIX.md` — no row (no wire change). `src/dst/tests/README.md` — no entry (no new DST module). `scripts/quality/mutation_owners.py` — no row (4.4). `docs/quality/owners.json` `effect`/`global` rows — none (no `tokio::spawn`, no statics).

---

## 7. Controls

Python ≥ 3.11 on PATH (the `python3` shim from the review notes) before any gate.

1. **Red first** (commit 2's tests on commit 1's tree, or on the current tree with only the test edits applied):
   `cargo test --locked --lib -- --exact dst::dst_tests::billing_controller::graceful_stop_seals_and_drains_the_active_read_window --exact dst::dst_tests::billing_controller::r09_active_telemetry_cancels_entered_storage_and_preserves_debt`
   → `2 failed`: 3.1 with `left: (1, 150, 0)`, 3.2 with `the terminal round must be attempted and given one cadence, not …`. Save as `scratchpad/item26-red.log`.
2. **Green, exact and floored**:
   `scripts/test-leg.sh target/quality/item26.log --exact dst::dst_tests::billing_controller::graceful_stop_seals_and_drains_the_active_read_window --exact dst::dst_tests::billing_controller::r09_active_telemetry_cancels_entered_storage_and_preserves_debt -- --locked --lib dst::dst_tests::billing_controller::`
   → every result `ok`, both names have their own `… ok` line; r09's wall time ≈2 s longer than before (the cadence cut), 3.1 ≈ sub-second.
3. **Bound control** (manual, once): temporarily replace `tokio::time::timeout(cadence, drain_once(state))` with `drain_once(state).map(Ok)` (or comment the timeout out) and rerun 2 → r09 fails with `aborted: ["telemetry-drain"]`; revert. This is the proof that 3.2 pins the bound.
4. **Whole billing DST surface**: `cargo test --locked --lib dst::dst_tests::billing_` and `cargo test --locked --lib billing::` (unit + spool + accumulator) — no change expected.
5. **Gate on commit 1 alone**: after `git commit` of the move, `scripts/quality.sh` (≈3 min; includes `gate.py`, the four `scripts/<gate>.py --check`, rustdoc `-D warnings`, mt-lint leg). Expected: `quality ratchets: OK`, `file growth` absent, `architecture-gate: OK`. The prune step that this commit needs:
   `cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl && python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl --prune` → `git diff --stat docs/quality/source-allowances.json` shows one removed row (`src/billing.rs`, `crate::spawn_telemetry`, `tokio::select`, 5); commit it with the move.
6. **Gate on commit 2**: `scripts/quality.sh` again; then `python3 scripts/test-inventory.py --check`, `python3 scripts/review-evidence.py --check`, `python3 scripts/architecture-gate.py --check`, `python3 scripts/scenario-map-report.py --check` (all four already run inside quality.sh; listed for the fast pre-flight).
7. **CI's own selection before the push** (both commits committed):
   `QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan`
   Expected `plan.json`: `changed_rust_files` = `src/billing.rs`, `src/billing/telemetry_loop.rs`, `src/dst/tests/billing_controller.rs`; `mutation_source_files: []`, `unregistered_mutation_source_files: []`, `mutants: false`, `loom: false`, `miri: false`, `compiler: true`. If `mutants` is `true` for any reason, run `scripts/quality/mutations.sh` and require zero missed.
8. **After the push**: `gh run list --branch slate --limit 5 --json databaseId,headSha,createdAt,status,conclusion` and match the pushed sha; `gh run view <id>`; never claim green from memory.
9. **Field control** (optional, local binary, the doc's actual promise): `USAGE_STREAM_KEY=… TELEMETRY_DRAIN_SECS=2` server on a scratch store; create a stream, `GET` it once (a metered read), send SIGTERM within 10 s; the log shows `supervised loops stopped finished=[…, "telemetry-drain", …] aborted=[]` with no `usage drain at shutdown` warning, and a restarted server's `_usage` read (or the spool's `pending`) contains a `read_batch` with that stream. Before the fix the same run shows nothing for that read.

---

## 8. Out of scope / follow-ups

1. **Ops, audit and fleet-event outboxes at a stop.** `drain_ops_once`, `drain_audit_once`, `drain_fleet_events` run only inside a cadence round; their in-memory batches are restored to memory on a cut (R09's `cancelled_*_append_restores_batch_order` units) and die with the process. The item is read usage; a terminal round for the journals is a separate decision (audit denials are the security-relevant one).
2. **Deltas metered by connections aborted in the same instant** (`http.rs:1336`, `meter_read_chunk` on a body that is being dropped) can land after the terminal seal. Closing it would order the terminal round after `serve_h1` returns (a composition change in `bootstrap::run`, an 841-line ratcheted function) or give the loop a second signal; both are larger than this item and the window is scheduler-latency sized.
3. **`TELEMETRY_DRAIN_SECS` above the 10 s grace** makes the supervisor's abort the bound (custody intact). If that should be a config error, it belongs in `ServerConfig::validate` alongside the admission validation of item 25.
4. **Registering `src/billing/telemetry_loop.rs` as a mutation owner** needs a metrics-cadence test that can kill the `>=` mutants of the `metrics_interval_secs` predicate (a rig override of `metrics_interval_secs`, or the predicate extracted with an explicit `now`).
5. **`spawn_rollup`'s cancel arms** also return without a terminal step; the rollup's cursor and artifacts are durable, so nothing is lost, only delayed to the next boot.
6. **Sealed-queue-full stop** (`READ_SEALED_MAX_BATCHES` deferral during a ledger outage): the terminal `seal_aged_reads(0)` defers like any seal and the active window is the hard-loss window; the operator gauge already reports it. Unchanged by design.
7. `docs/review-storage-evidence.md` §R09 describes the 300 ms grace; it is historical evidence and is not a gate input — leave it, the mechanism ledger carries the new number.

---

## Skeptic corrections (C1..Cn)

Verified on `slate @ df9ff212` (the plan was written at 8dabca7f; the one commit since touched `src/product*` only — every `src/billing.rs` line number the plan cites is unchanged: `spawn_telemetry` 1157-1249, the cancel arms 1211/1216, the aged seal 784, `drain_once` 762-770, `open_read_spool` 1125, `sweep_owned_outboxes` 1692). Line counts re-measured: `src/billing.rs` 2,300, `src/dst/tests/billing_controller.rs` 505, `src/billing/read_accumulator.rs` 219, `docs/OBSERVABILITY-BILLING.md` 1,288. The tree carries two uncommitted DST edits (`fixture_auth.rs`, `quota_read_volume.rs`) that belong to other work; they are not this item's.

**What holds (checked, no correction needed).** The problem statement (both arms `return Done`, `seal_if_aged(0)` unused in production, doc lines 101/376/421 unimplemented). The buildability argument for the bound: `hold_class(.., u64::MAX)` parks every SST PUT (`fault_store.rs:237-242`), so an unbounded terminal round is aborted at the grace and flips R09. The fingerprint argument in §4.3: `tools/quality-syntax/src/imports.rs:30-60` resolves only *lexical* `use` aliases, so the `crate::billing::spawn_telemetry` spelling at `bootstrap.rs:888` (under `run`'s `unwrap_used`/`expect_used` expects, `bootstrap.rs:116-140`) produces the same `call-site`/`path` facts after the re-export; the trap that hit `product/consumer_pull.rs` (bare same-file call re-spelled through a `use`) does not apply. The ledger set for commit 1 matches the two precedents exactly (`aa445217`: policy row; `df9ff212`: policy row + `owners.json` macro-dsl row + pruned allowance). `source_gate.check` (`scripts/quality/source_gate.py:28`) would reject the new identity in `source-allowances.json` (`legacy source allowance grew`, ceiling 0), so `owners.json` is the right home, and the owner's qualified name is per-file `crate::spawn_telemetry` (confirmed against `source-allowances.json:2628-2634`). `verification_plan.py:22-31` prefixes contain no `src/billing.rs`/`src/billing/telemetry_loop.rs` and `mutation_owners.py:66-68` registers only `read_accumulator`, `read_spool`, `system_append`, so `mutants: false` is the correct expectation. `review-evidence.py:177-181` hashes the test *body* only and `configuration` is free-form, so the mechanism-ledger edit in §6 is right. The rig's config is `CliArgs::deterministic()` + `MapEnvironment::empty()` (`fixture_http.rs:337-348`), so `telemetry_drain_secs == 2` in every rig. `is_reserved_stream` (`read_accumulator.rs:72`) keeps the `_ops_metrics`/`_usage` polling reads out of the accumulator, so `(1, 150, 0)` is exact (120 + 4 + 4 + 16 + 6, `read_accumulator.rs:90-94`). `tokio::select!` drops its future tuple before running the arm handler, so the cut round's `ReadDrain` requeues before `break` reaches the terminal round. No doc outside `OBSERVABILITY-BILLING.md` states a graceful-stop contract for read usage (`grep -rni "sigterm\|graceful" docs/*.md`); `WIRE-MATRIX.md` has no shutdown or loss-window row; `possibleReadLossWindowSeconds` (`src/product/usage.rs:200`) stays `READ_FLUSH_INTERVAL_MS / 1000`.

### C1 — r09 goes over the 100-line Clippy limit with the §3.2 edits (CI red, `-D warnings`)

Evidence: `src/dst/tests/billing_controller.rs:13-116`; body lines 14-115 = 102, of which 4 are comment-only (14, 15, 102, 103) → **98 counted lines today** (`awk 'NR>=14&&NR<=115' … | grep -v '^//' | wc -l` = 98). Clippy's `too_many_lines` counts every body line containing code, skips only blank/comment lines, and is not test-exempt — the same file's neighbours carry `#[expect(clippy::too_many_lines, …)]` on DST tests for exactly this reason (`billing_attribution.rs:15-18`, `billing_usage.rs:75`, `admission_memory.rs:249`; `unfulfilled_lint_expectations` is denied, so those expects are live). §3.2 adds `let cadence`, `let stop` and a five-line `assert!` (+7 code lines; the three comment lines are free) → 105 > 100. The plan's "Both tests stay under 100 Clippy lines" (§4.2) is false for r09.

Correction: put `#[expect(clippy::too_many_lines, reason = "r09 active telemetry cancellation; the entered holds, the stop and the release-and-retry must stay one visible sequence so the stop's side is evident for every batch; a helper phase would hide which side of the stop a batch was on")]` directly above `#[tokio::test(…)]` on r09 (exactly two `;`, no `"` inside — `source_rules.violations` regex at `source_rules.py:257`). A new reasoned exception is a reviewed decision (`source_rules.py:264-266`: `exception` identities with `reason =` are skipped by the occurrence check; `exception_growth` ignores new identities). Consequences the plan must add to §6: the inventory diff for r09 now also changes `attributes` (the `--write` regenerates it; `review-evidence.py` hashes the body only, so the mechanism row edit is unchanged). Do NOT instead trim r09 with `rig_create` — that helper takes a bearer (`fixture_auth.rs:379`), not the `PRISMA_KEY` rig. A two-line helper call (`let stop = …; assert_stop_took_one_cadence(&state, stop);`) lands r09 at exactly 100, which passes (`> 100` lints) but leaves zero rustfmt margin; the expect is the robust choice.

### C2 — §3.1's wait loop as described exceeds nesting 4

Evidence: `excessive-nesting-threshold = 4` (`clippy.toml:3`); the lint counts block depth starting at the fn body (1). r09's existing wait is fn(1) → async block(2) → `while` body(3) (`billing_controller.rs:73-78`). The plan's wait ("a `yield_now` loop until `system_read(OPS_METRICS_STREAM)` returns a body whose `Vec<serde_json::Value>` has one record") needs `loop { if let Some((body, _)) = … { if vec.len() == 1 { break } } yield }` → fn(1) → async(2) → loop(3) → if-let(4) → if(5): lint at depth 5.

Correction: state the shape: a file-local helper `async fn ops_metrics_records(state: &Arc<AppState>) -> usize` (match on `system_read(...).await.unwrap()`: `None => 0`, `Some((body, _))` → `0` if `body.is_empty()` else `serde_json::from_slice::<Vec<serde_json::Value>>(&body).unwrap().len()` — `rollup_step` guards the empty body the same way, `billing.rs:1281-1284`), and in the test `tokio::time::timeout(Duration::from_secs(3), async { while ops_metrics_records(&state).await != 1 { tokio::task::yield_now().await; } }).await.expect("the first round ends with the metrics emission")` — depth 3, as r09. Helpers in a DST test file are not inventoried (`test-inventory.py:107-108` keeps only `#[test]`-attributed fns), so no ledger effect.

### C3 — §3.1's own line budget is unstated and close to the limit; count it after rustfmt

Evidence: the plan's sketch (rig build ≈6, spool open+install ≈7, wait ≈5, identity+delta ≈11, meter+asserts ≈8, shutdown+report asserts ≈10, `_usage` read+decode+filter_map ≈16, row asserts ≈4, close ≈2) is ≈90 code lines before rustfmt spreads the multi-arg `assert_eq!`s; rustfmt typically adds 5-10. Nothing in the plan bounds it.

Correction: move the `_usage` decode into a second file-local helper `async fn usage_read_rows(state: &Arc<AppState>) -> Vec<(BillingIdentity, (u64, u64, u64))>` (the `filter_map` over `UsagePayload` with all four variants named, no `_ =>`), leaving the test at ≈70 lines; or add the same `#[expect(clippy::too_many_lines, …)]` as C1 if the measured count exceeds 100 (it must then actually exceed 100, or the expect is unfulfilled and denied). Record the measured count in §4.5. Note `ReadRow` has no `Clone`/`PartialEq`, so the helper returns the compared fields, not rows.

### C4 — Citation fixes

- §1.6 quotes the hold lines as `billing_controller.rs:62-63`; they are **69-70** (`write_entered`/`list_entered`); the 300 ms grace is at 82 as stated and the requeue assert `drain owns the batch` at 81.
- §1.5 "signal task `bootstrap.rs:735-747`": the block is 733-748; harmless.
- Header: tree is `df9ff212`, not `8dabca7f`; billing.rs lines unchanged (verified above).
- §5 row "`last_metrics.is_none_or(… >= …)` NOT killable": imprecise — `>=`→`<` and `>=`→`==` make the *second* emission fire on the very next 2 s tick (the first is unconditional via `is_none_or(None)`), so a two-round count of `_ops_metrics` records kills them in ≈4 s; only `>=`→`>` needs a rig override of `metrics_interval_secs`. Irrelevant while the file is unregistered (§4.4 stands), but the follow-up §8.4 should say so.

### C5 — Neither red test exercises the production condition: every other supervised loop is cancelled in the same instant

Evidence: both tests spawn the loops under their own `TaskSupervisor` (`billing_controller.rs:71-72`; §3.1 "an own `TaskSupervisor` (as r09)"), so the rig's `request-maintenance` worker (`fixture_http.rs:448`, `request_work.rs:126-153`), livefeed and HTTP accept loop keep running while the terminal round appends. In production `tasks.cancel()` (`tasks.rs:391-399`) flips one watch for every loop at SIGTERM: `RequestWork::run` returns on the first poll (`request_work.rs:224-226`), its `StopGuard` sets `stopped`, and any later `submit` answers `WorkError::Stopped` (`request_work.rs:169-171`). The plan's §1.5 claim that a `_usage` append needs no ticket holds for the steady state (`append.rs:236-241`: a topology ticket only for a pending segment transition; `ttl.rs:83-108`: only for a TTL stream), but nothing in the plan *tests* it, and §7.9 is marked optional.

Correction (pick one; the first is preferred): run §3.1 on the rig's supervisor — `crate::billing::spawn_telemetry(state.clone(), &rig.tasks)` then `let report = rig.tasks.shutdown(Duration::from_secs(5)).await;` (the `billing_terminal_shutdown_waits_for_a_late_open` precedent, `billing_controller.rs:475-476`, already stops `rig.tasks` and then calls `rig.shutdown()`), and assert by name instead of count: `report.aborted.is_empty()`, `report.outcomes.iter().any(|(n, o)| *n == "telemetry-drain" && *o == TaskOutcome::Finished)` and the same for `"telemetry-outbox-sweep"`. The red output is unchanged (`(1, 150, 0)`). Then `_usage` is read in-process after the stop (`system_read` → `read_inner` needs no supervised task), exactly as the late-open test calls `open_or_wait` after its stop. Otherwise, make §7.9 mandatory and save its log next to `item26-red.log`.

### C6 — The `TELEMETRY_DRAIN_SECS` invariant has no landing place

Evidence: §2 says "Operator invariant to document: `TELEMETRY_DRAIN_SECS` must stay below the supervisor grace", but §4.2/§6 only reword `OBSERVABILITY-BILLING.md:101` and `:421`. The knob is documented nowhere in `docs/` (`grep -rn TELEMETRY_DRAIN_SECS docs` → one incidental mention, `RELEASE-PRODUCT-SURFACE.md:1026`); its only doc is `src/config/model.rs:217` (`/// TELEMETRY_DRAIN_SECS, default 2.`; file is 501 lines, no ceiling).

Correction: extend that doc comment by one line ("also bounds the terminal drain round a graceful stop runs; keep it below the 10 s supervisor grace, above it the supervisor's abort is the bound") — a doc-only change to `model.rs` (no `#[expect]` scope, no fact change), or add the sentence to the §7.4 rewording of line 421. Say which in §4.2 and §6. Note that `RELEASE-PRODUCT-SURFACE.md:1026` records field gates running with `TELEMETRY_DRAIN_SECS=1`; the bound is then 1 s — still three sequential store round trips (spool persist, `_usage` append, `remove_spooled`), fine on a healthy store, and the plan's "not validated in config" stance is acceptable.

### C7 — Nightly mutation cost of r09 (note, not a blocker)

Evidence: the `read_accumulator` and `read_spool` owners select tests by the filter `billing` (`mutation_owners.py:66-67`), which matches `dst::dst_tests::billing_controller::r09_…` by module path; on the scheduled rotation r09 now costs ≥ 2 s per mutant (the cadence cut) on top of today's parked-hold wait. cargo-mutants derives its timeout from the baseline run, so this does not create a TIMEOUT miss; record it in the commit message so a later "why is the billing bucket slower" has an answer.

### Verdict

**ready-with-corrections.** The design (bounded terminal round, verbatim move first, cadence as the bound, R09 kept cooperative) is sound and every load-bearing claim about the source, the ratchets and the ledgers checks out. C1 and C2 are hard CI blockers (`-D warnings`) that the plan as written would hit on commit 2; C3 is the same risk unmeasured; C5 is the one place the verification does not reach the production shape; C4/C6/C7 are text.

Unbuildable controls: none. Every control in §7 is runnable as written (`test-leg.sh` syntax matches `scripts/test-leg.sh:1-25`; `--exact` twice is accepted by libtest; the plan's `verification_plan.py` invocation and expected `plan.json` fields match `verification_plan.py:97-105`).

Missed ledgers/ratchets: none missed outright; two entries need amending — (a) `docs/refactor/test-inventory.json`: r09's `attributes` also changes once C1's expect lands; (b) the exception ratchet gains one new reasoned identity (`('exception', 'src/dst/tests/billing_controller.rs', 'crate::r09_active_telemetry_cancels_entered_storage_and_preserves_debt', '<expect text>')`), which is allowed without a ledger row but must keep the `owner; invariant; alternative` shape.
