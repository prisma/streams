# Item 79 — one gap-debt journal owner (`GapJournal`) for `_ops_events` and `_audit_events`

Base: `slate` @ `24c4c77a` (= `origin/slate`, clean tree). Planned read-only; nothing below was run.

Summary: the problem is real (a pure refactor; there is **no live bug**). The reviewer's Change can be built,
with four corrections: (a) `telemetry_batch.rs` has been a registered mutation owner since `77c13d74`, so that part is
already done; (b) `GapJournal` needs a hand-written `Default` that delegates to a `const fn new()`, because
`QuotaRegistry` derives `Default` over `Arc<OpsService>`. The obvious `new() { Self::default() }` would leave an
equivalent mutant; (c) the `ops` mutation owner must gain a DST filter, because `drain_ops_once`'s body changes and no
`ops::` test calls it; (d) the reviewer's "red" first step fails only at compile time. The real first step is an
end-to-end wire pin, which passes before and after the change. Three commits.

---

## 1 Problem (verified on 24c4c77a)

### 1.1 One algorithm written twice

| piece | ops (`src/ops.rs`) | audit (`src/audit.rs`) |
|---|---|---|
| storage | `OpsService { queue: Mutex<OpsQueue{queue, recent}>, dropped, gap, sequence, alerts }` 113-120 | `AuditJournal { sequence, dropped, gap, queue: Mutex<VecDeque<AuditEvent>> }` 101-106 |
| enqueue + cap | `emit` 133-145 (`if g.queue.len() >= OPS_QUEUE_CAP { dropped+1; gap+1; return }`) | `observe_denial` 159-165 (same with `AUDIT_QUEUE_CAP`) |
| batch guard | `struct PendingOps` 178-184 | `struct PendingAudit` 170-176 |
| take | 198-243 | 186-222 |
| Drop (requeue) | 245-263 | 224-242 |
| record_loss | 264-280 | 243-252 |
| persist | `persist_ops_batch` 281-290 | `persist_audit_batch` 253-265 |
| empty-batch early return in drain | 324-326 | 298-300 |

After normalizing names (`Ops`/`Audit`→`X`, caps→`CAP`), `diff` of 176-290 against 168-265 shows only four
differences: ops' `prepare` closure (it stamps the cell at drain time), how `record_loss` recognizes a marker, the
exception reasons, and the queue type (the ops queue shares its lock with the `recent` ring).

### 1.2 The exception reasons have drifted apart (exact text)
- ops.rs 186-189: `#[expect(clippy::too_many_arguments, reason = "PendingOps::take; the batch takes its byte, count and age budgets separately as the drain configured them; a budget struct would exist for this single call site")]`.
  The reason is stale. `take` has one byte budget and no count or age budget. The lint fires because of six parameters (`queue, dropped, gap, max_bytes, make_gap, prepare`).
- ops.rs 190-193 `expect_used`: `"PendingOps::take; the queue was checked non-empty under the same guard, so its first event is present; …"`. This is false: `take` checks nothing for emptiness.
  audit.rs 182-185 states the true invariant: `"PendingAudit::take; an oversized selection is only produced from a first queued event that overflowed the budget, so the queue holds it; …"`.
- The `unwrap_used` reasons use different wording in each ledger: ops says `"a poisoned ops journal may hold a partially appended event or alert; recovering it could emit or report a half-written record"` and audit says `"a poisoned audit queue may hold a half-pushed or half-drained event; recovering it could publish or drop an event twice"`.

### 1.3 The magic 512, written twice
- `src/telemetry_batch.rs:43` has `for event in events.take(512) {`, the most rows `encode_prefix` selects.
- `src/ops.rs:209` has `for event in guard.queue.iter_mut().take(512) {`, which prepares (stamps the cell on) the rows a batch might select.
They must agree, or a selected row reaches `_ops_events` with `"cell":""`. Only the literal ties them together. Today they
agree: with a marker present, ops prepares one queued row more than the batch can take, which does no harm.

### 1.4 Eight mirrored tests, pinned by sha
`docs/refactor/review-mechanisms.json`:
- mechanism `cancelled-telemetry-batch-ownership` (419-466):
  - `src/ops.rs::cancelled_ops_append_restores_batch_order_and_retry_ids`
  - `src/ops.rs::cancelled_ops_batch_overflow_preserves_full_gap_magnitude`
  - `src/audit.rs::cancelled_audit_append_restores_batch_order_and_retry_ids`
  - `src/audit.rs::cancelled_audit_batch_overflow_preserves_full_gap_magnitude`
  - The two fleet tests in this mechanism stay.
- mechanism `byte-bounded-journal-drains` (640-681):
  - `src/ops/batch_tests.rs::{byte_bounded_ops_batches_preserve_unselected_and_cancelled_order, oversized_ops_event_becomes_visible_gap_without_wedging_followers}`
  - `src/audit/batch_tests.rs::{byte_bounded_audit_…, oversized_audit_…}`
  - `telemetry_batch::array_delimiters_and_escaped_strings_count_toward_exact_limit` stays.
- The four `cancelled_*` tests are also pinned a second time, in `source_adaptations` (954-989, before `d1131213`).
  `scripts/review-evidence.py:135-141` (`required_units`, the tuples at 138-139) requires those rows.

### 1.5 No test pins the production gap marker
Tests build their markers by hand:
- `src/ops.rs:896-898` builds `OpsEvent::new("telemetry_gap", "gap-id".into()).fields(json!({"dropped": 17}))`.
- `src/ops/batch_tests.rs:6-8` builds `gap(n)`.
- `src/audit/batch_tests.rs:17-23` builds `gap(n)`.

The production marker is built only inside the drain closures, behind an `AppState`:
- ops.rs 306-317: `"gap/{boot}/{seq}"`, `.warn()`, `fields {"dropped": n}`.
- audit.rs 281-296: `"deny-gap/{boot}/{seq}"`, runtime clock, cell, `code:"audit_gap"`, empty route/method, `status:0`, `dropped:Some(n)`.

`git grep -E 'deny-gap|gap/\{'` finds only those two closures. So nothing checks that `record_loss` recognizes the
marker its own drain builds (for ops: `event_type == "telemetry_gap"` plus `fields.dropped`). Nothing pins the
sequence either: audit shares one counter between `deny/…` ids (spent even for dropped denials, audit.rs:145-149,
before the cap check) and `deny-gap/…` ids. Ops spends its counter only on markers.

### 1.6 Criticality (checked)
- `src/ops.rs` falls under the critical prefix `src/ops` (`scripts/quality/verification_plan.py:29-30` BUFFER_PREFIXES) and is registered as owner `ops`, filter `ops::` (`mutation_owners.py:79`).
  - No `ops::` test calls `drain_ops_once`. Only DST tests call it, and their paths (`dst::dst_tests::…`) do not match `ops::`.
- `src/ops/batch_tests.rs` also falls under `src/ops`, and it is **not registered**.
  - Editing it would fail with "register every changed critical mutation owner".
  - Deleting it is recorded as `prefix-critical-deletion`, and it forces every **added** `.rs` file in the push range into registration (`verification_plan.py:168`). So this series adds no new `.rs` file.
- `src/telemetry_batch.rs` is registered (`mutation_owners.py:76`, filter `telemetry_batch::`). Its whole file was killed at registration (`77c13d74`: 26 mutants, 25 caught, 1 unviable).
  - The reviewer's "register telemetry_batch.rs in the same change" is therefore already done.
- `src/audit.rs` and `src/audit/batch_tests.rs` are **not critical and not registered**; `src/audit` is not a prefix.
  - The move gives audit's batch algorithm mutation coverage for the first time, through `telemetry_batch`.

### 1.7 Every use site (git grep over src, tools, fuzz, bench, tests, scripts, docs)
- `PendingOps`, `persist_ops_batch`, ops `record_loss`: ops.rs 178-290, 301, 327; tests at ops.rs 779-918; `src/ops/batch_tests.rs` 9-19, 55, 79, 113.
- `PendingAudit`, `persist_audit_batch`, audit `record_loss`: audit.rs 170-265, 276, 301; tests at 338-398; `src/audit/batch_tests.rs` 24-25, 58, 77, 110.
- `encode_prefix` / `Selection`: telemetry_batch.rs 33 plus its tests; ops.rs 215-219; audit.rs 197-198.
- `OpsService`: `runtime.rs:152,237` (`OpsService::new()`); `quota.rs:58,501,506`.
  - **`QuotaRegistry` is `#[derive(Default, Clone)]` over `Arc<OpsService>`** (quota.rs:499-503), and `QuotaRegistry::default()` is called in `src/quota/{poison,pressure,pressure_counting}_tests.rs`. So `OpsService: Default` must stay.
  - Other users: `scaler3/controller.rs:39,47,133,195`; `shard.rs:1045,1278,1434,1902`; `bootstrap.rs:446-530`; `http.rs:1105-1109` (`/v1/debug/ops-events`: `recent(128)`, `open_alerts()`, `dropped()`), `http.rs:1762` and `2049`; `application/creation/deletion.rs:317`; DST `fixture_http.rs:438`, `billing_usage.rs:629,636,747,761,767`, `runtime_journals.rs:44,60,75,87-109`.
- `AuditJournal`: `runtime.rs:153,238` (`AuditJournal::new()`); `ops.rs:401` (`audit.dropped()`); audit.rs; `audit/batch_tests.rs`.
- `drain_ops_once` and `drain_audit_once`: `billing/telemetry_loop.rs:81,84`; DST `billing_usage.rs:636`, `runtime_journals.rs:60-61`, `security_audit.rs:22,75,474`.
- `observe_denial`: `http.rs:1624,1777,1848`; DST `runtime_journals.rs:49`.
- `OPS_QUEUE_CAP`, `AUDIT_QUEUE_CAP`, `RECENT_CAP`: only ops.rs and audit.rs (plus tests).
- tools/, fuzz/, bench/, tests/, src/bin: **no use**.
- Scenario map: `RES-003` names `overflow_counts_and_reports` in `src/ops.rs` (docs/refactor/test-scenario-map.json:2729). The name is kept.
- Frozen adoption ledgers name these items: `docs/quality/legacy-*.json` (immutable, sha-pinned in policy.json) and `docs/quality/verification.json` (historical, no script reads it). Neither is edited.

---

## 2 Contract decision

**No journal wire change, and no product, raw, debug or metrics edge change.** These are preserved exactly:
- Ops marker: id `gap/{boot_id}/{seq}`, where the sequence is spent **only** when a marker is built, under the queue lock.
  - Fields: `event_type "telemetry_gap"`, `severity "warn"`, `fields == {"dropped": n}`.
  - `event_time_ms` and `observed_ms` still come from `crate::shard::now_ms()`.
  - The cell is stamped by the drain's `prepare`.
- Audit marker: id `deny-gap/{boot_id}/{seq}`, drawn from the **same** counter as `deny/{boot_id}/{seq}`, and a denial dropped at the cap still spends its number.
  - Fields: `event_time_ms = runtime.clock.now().ms()`, the cell, `code "audit_gap"`, route and method `""`, `status 0`, `dropped: n`, no `project_id`.
- Batch bytes: `encode_prefix` is unchanged apart from naming its constant. Marker first, then queue order. The largest prefix that fits is taken.
- Requeue after cancellation or failure keeps order ahead of newer rows. A marker lost on overflow returns its own magnitude to the gap and counts no new drop.
- Audit without a usage key still counts `dropped` only; no gap debt accrues because no drain will ever run.
- `/v1/debug/ops-events` (`recent`, `alerts`, `dropped`) and the `ops_events_dropped_total` and `audit_events_dropped_total` counters are unchanged.

Internal differences, none of them visible at an edge:
- The ops `recent` ring gets its own mutex, and `emit` releases it before pushing to the journal.
  - Two concurrent emitters may now appear in a different relative order in the ring than in `_ops_events`. That order was already arbitrary and is not a contract.
  - A panic under the journal lock no longer poisons the operator view. Only the impossible `expect` in `take` could cause one.
- `prepare` now runs on `marker + queue` truncated to `MAX_BATCH_ROWS`, instead of `queue` truncated to 512 followed by the marker. Stamping only fills an empty cell, so the bytes are identical.

---

## 3 Pins, tests and non-vacuity controls

Nothing is red on the current tree: the change is a pure refactor. The reviewer's first step ("red test … production
constructor … `represented_gap()==Some(17)`") fails only because the constructor does not exist yet. Commit 2 keeps
that test as a pin. The real pin-first is commit 1.

### 3.1 C1: end-to-end wire pin (DST, green before and after)
`src/dst/tests/runtime_journals.rs` gets a new `#[tokio::test(flavor = "multi_thread", worker_threads = 4)] async fn gap_markers_keep_their_ids_sequences_and_counts`:
- Setup: an enforce-mode rig, as in `r10_runtime_journals_and_alerts_do_not_cross_drain_or_resolve` (`AuthService::new(AuthMode::Enforce, "https://auth.prisma.io".into(), "test-cell")`, `http_rig_build(mem(), RigRuntime::first(), HttpRigOptions { auth_service: Some(auth), ..Default::default() })`).
- `emit(OpsEvent::new("owned", format!("owned/{i}")))` for `i in 0..OPS_QUEUE_CAP + 3`.
- `observe_denial(state, "route", &Method::GET, &tag(401-response, "unauthorized"))` × `AUDIT_QUEUE_CAP + 2`.
- `assert_eq!((ops.dropped(), audit.dropped()), (3, 2))`.
- `assert!(drain_ops_once(state).await.unwrap() > 1)`, and the same for `drain_audit_once`.
- `ops = journal(state, OPS_EVENTS_STREAM)`:
  - `ops[0]` has sorted keys `["cell","event_id","event_time_ms","event_type","fields","observed_ms","severity","v"]`.
  - `event_id == format!("gap/{boot}/0")` (ops ids never spend the sequence), `event_type == "telemetry_gap"`, `severity == "warn"`, `v == 1`, `cell == state.deployment.cell_id().as_str()`.
  - `fields.dropped == 3`, and `fields` has exactly 1 key.
  - Both `event_time_ms` and `observed_ms` are `is_i64()`.
  - `ops[1]["event_id"] == "owned/0"` and `ops[1]["cell"] == cell`.
- `audit = journal(state, AUDIT_EVENTS_STREAM)`:
  - `audit[0] == serde_json::from_str(format!(r#"{{"v":1,"event_id":"deny-gap/{boot}/{seq}","event_time_ms":{now},"cell":"{cell}","code":"audit_gap","route":"","method":"","status":0,"dropped":2}}"#, seq = AUDIT_QUEUE_CAP + 2, now = state.runtime.clock.now().ms()))`, compared as `Value`, so key order does not matter. The rig clock is a `ManualClock`, so `now` is deterministic.
  - `audit[1]["event_id"] == format!("deny/{boot}/0")`.
- `engine_shutdown(state).await`.
- It uses no macro besides `assert*` and `format!`, so it needs no owners row.

Non-vacuity (temporary edit, reverted):
- In `src/audit.rs:286`, replace `journal.sequence.fetch_add(1, Ordering::Relaxed)` with `0`. Expect `assertion `left == right` failed` on `audit[0]`, where the left side contains `deny-gap/<boot>/0` and the right side contains `/4098`.
- In `src/ops.rs:315`, drop `.warn()`. The `severity` assert fails with `"info"` against `"warn"`.

### 3.2 C2: per-ledger constructor pins
- `ops::tests::gap_marker_carries_its_wire_shape_and_debt` (in `src/ops.rs`, `mod tests`):
  - `let m = OpsEvent::gap_marker("boot-a", 3, 17); assert_eq!(m.represented_gap(), Some(17));`
  - `to_value(&m)`: remove `event_time_ms` and `observed_ms` (each must be `is_i64()`), then compare the rest as `Value` with `from_str(r#"{"v":1,"event_id":"gap/boot-a/3","cell":"","event_type":"telemetry_gap","severity":"warn","fields":{"dropped":17}}"#)`.
  - A lookalike, `OpsEvent::new("alert_opened", "x".into()).fields(m.fields.clone())`, gives `represented_gap() == None`.
- `audit::tests::gap_marker_carries_its_wire_shape_and_debt` (new `#[cfg(test)] mod tests` in `src/audit.rs` with explicit imports `use super::AuditEvent; use crate::telemetry_batch::GapEvent;`, and **no glob**, so no ledger row is needed):
  - `AuditEvent::gap_marker("boot-a", 3, 1_234, "cell-a", 17)` gives `represented_gap() == Some(17)`.
  - `serde_json::to_string(&m) == r#"{"v":1,"event_id":"deny-gap/boot-a/3","event_time_ms":1234,"cell":"cell-a","code":"audit_gap","route":"","method":"","status":0,"dropped":17}"#`.
  - `AuditEvent { dropped: None, ..m }.represented_gap() == None`.
- Non-vacuity:
  - Make `represented_gap` for `OpsEvent` compare against `"telemetry-gap"`. Both the ops test (`None` vs `Some(17)`) and `cancelled_ops_batch_overflow_preserves_full_gap_magnitude` (gap `2` vs `18`) turn red.
  - Make `represented_gap` for `AuditEvent` return `None`. The audit test and `cancelled_audit_batch_overflow_…` turn red.

### 3.3 C3: generic `GapJournal` tests (`src/telemetry_batch.rs`, existing `mod tests`)
Test row: `#[derive(Serialize)] struct Row { id: String, #[serde(skip_serializing_if="Option::is_none")] gap: Option<u64>, #[serde(skip_serializing_if="String::is_empty")] pad: String, #[serde(skip_serializing_if="String::is_empty")] cell: String }`, with `impl GapEvent for Row { fn represented_gap(&self) -> Option<u64> { self.gap } }`.

Helpers and conventions:
- `const CAP: usize = 1024; type Journal = GapJournal<Row, CAP>;`
- Helpers `row(id)`, `padded(id, size)`, `marker(n)` (id `"gap"`, `gap: Some(n)`), `ids(&[u8]) -> Vec<String>`, `queued(&Journal) -> Vec<String>`.
- The tests are sync `#[test]`s. A held append is polled once with `Context::from_waker(Waker::noop())`, and a ready one is finished with `futures_util::FutureExt::now_or_never()`. Both idioms already exist in the repo.
- There is **no `tokio::select!` and no `json!`**, so no owners or allowance rows are needed.

| test | port of / purpose | key assertions |
|---|---|---|
| `cancelled_append_restores_batch_order_and_retry_ids` | ports both `cancelled_*_append_…` (mechanism) | the sink sees `["first","second"]` and sets a `Cell<bool>`; after one poll, `Pending` and the queue is empty (sink entered after dequeue); push `newer`; `drop(drain)` gives `["first","second","newer"]`; a failed append (`ready(Err("retry"))`) leaves the same order; a successful one returns `Ok(3)` and empties the queue; `(dropped, gap) == (0, 0)`; an empty `take().persist(\|_\| ready(Err(..)))` returns `Ok(0)` (an empty batch never appends) |
| `cancelled_batch_overflow_preserves_full_gap_magnitude` | ports both `cancelled_*_overflow_…` (mechanism) | take `pending`; `batch.events.push(marker(17))`; push `CAP` rows; drop; then `queue.len() == CAP`, `dropped == 1` ("only the newly lost event is a new drop"), `gap == 18` |
| `byte_bounded_batches_preserve_unselected_and_cancelled_order` | ports both `byte_bounded_*` (mechanism), `LIMIT = 64 KiB`, 900 rows padded to 256 | `0 < selected < MAX_BATCH_ROWS`; `body ≤ LIMIT`; the front of the queue is `event-{selected:04}`; the next row would overflow; a held-then-dropped append; `newer`; a loop of at most 32 pages whose received ids equal 0..900 followed by `newer`; `pages > 1`; queue empty; `(0, 0)` |
| `oversized_event_becomes_visible_gap_without_wedging_followers` | ports both `oversized_*` (mechanism) | the first `take` errs with text containing `"oversized"`; `(dropped, gap) == (1, 1)`; queue len 1; the next `take(LIMIT, marker, …)` persists `Ok(2)` with rows[0] id `gap`, gap 1, and rows[1] id `next`; afterwards `(1, 0)` |
| `prepare_covers_exactly_the_rows_a_batch_can_select` | new; pins the single 512 bound (added to the byte-bounded mechanism) | push `CAP + 2` rows (gap 2); `take(usize::MAX, marker, \|r\| r.cell = "stamped".into())`; `events.len() == MAX_BATCH_ROWS`; every selected row, the marker included, is stamped; every row still queued (513) has an empty cell |
| `pushes_past_the_cap_count_drops_and_owe_gap_debt` | new; push, counter and sequence API | `Journal::default()`; push `CAP + 3` rows; queue len `CAP`, back is `r{CAP-1}`; `(dropped, gap) == (3, 3)`; `count_unreported_loss()` gives `(4, 3)`; three `next_sequence()` calls give `[0, 1, 2]` |
| `a_gap_marker_that_cannot_fit_keeps_its_debt_and_the_queue` | new; the moved marker-oversize branch | push `CAP + 1` rows; `take(2, marker, …)` errs containing `"gap marker"`; gap stays 1; queue len stays `CAP` |

Kept per ledger:
- `ops::tests::overflow_counts_and_reports`, adapted because `OpsQueue` is gone:
  - emit `OPS_QUEUE_CAP + 10` events, then `dropped() == 10`. This is exact only if the journal cap is `OPS_QUEUE_CAP`.
  - `recent(usize::MAX).len() == RECENT_CAP` exactly (it was `<=`).
  - `recent(1)[0].event_id == format!("t/{}", OPS_QUEUE_CAP + 9)`: newest first, and events the journal dropped are included.
- `ops::tests::gap_marker_…` and `audit::tests::gap_marker_…` from C2.
- The C1 DST pin.

Non-vacuity controls for C3 (each a temporary edit, then reverted):
1. `PendingBatch::drop`: iterate without `.rev()`. T1 turns red with `["second","first","newer"]` against `["first","second","newer"]`.
2. `take`: delete `self.gap.fetch_sub(gap, …)`. T4 turns red with `(1, 1)` against `(1, 0)`.
3. `PendingBatch::drop`: replace `self.journal.record_loss(&event)` with a plain `dropped+1; gap+1`. T2 turns red with `gap 2` against `18`.
4. `take`: prepare `queue.iter_mut().take(MAX_BATCH_ROWS)` and then the marker (the old ops shape). T5 turns red, because one queued row is stamped.
5. `persist`: move `self.events.clear()` above `append(..).await?`. T1 turns red, because the failed retry loses the rows.
6. `OpsService::emit`: `>` becomes `>=`. `overflow_counts_and_reports` turns red with `255` against `256`.
7. Re-run the C1 DST pin unchanged. It must stay green; that is the wire proof.

---

## 4 Edits, file by file, in commit order

No touched `.rs` file is over 1,000 lines, so each ceiling is 1,000. Line counts:

| file | before | after C1 | after C2 | after C3 |
|---|---|---|---|---|
| `src/ops.rs` | 922 | 922 | about 960 | about 700 |
| `src/audit.rs` | 425 | 425 | about 460 | about 235 |
| `src/telemetry_batch.rs` | 119 | 119 | about 128 | about 470 |
| `src/dst/tests/runtime_journals.rs` | 215 | about 280 | about 280 | about 280 |
| `src/ops/batch_tests.rs` | 126 | 126 | 126 | deleted |
| `src/audit/batch_tests.rs` | 123 | 123 | 123 | deleted |

### C1 — "The ops and audit gap markers are pinned end to end before their owner moves"
- `src/dst/tests/runtime_journals.rs`: add the §3.1 test. It needs imports `use crate::audit::AUDIT_QUEUE_CAP` (or full paths) and nothing new beyond the fixtures already imported.
- `docs/refactor/test-inventory.json`: run `python3 scripts/test-inventory.py --write`, which brings it from 523 to 524 tests.
- Nothing else. The file is not critical, so no mutants are selected, and no ratcheted scope is touched.

### C2 — "Each journal builds its gap marker with one production constructor and reads its debt back through GapEvent"
- `src/telemetry_batch.rs`: after `encode_prefix`, add the trait below. A trait declaration generates no mutants.
  ```rust
  /// A journal row. A gap marker also stands for the losses it reports, so a
  /// marker lost again on requeue returns its whole magnitude to the debt
  /// instead of counting as one new drop.
  pub(crate) trait GapEvent: Serialize {
      fn represented_gap(&self) -> Option<u64>;
  }
  ```
- `src/ops.rs`:
  - Add `use crate::telemetry_batch::GapEvent;`.
  - Add `const GAP_EVENT_TYPE: &str = "telemetry_gap";`, documented as "what `gap_marker` builds and `represented_gap` recognizes".
  - In `impl OpsEvent`, add `pub(crate) fn gap_marker(boot_id: &str, sequence: u64, dropped: u64) -> Self { OpsEvent::new(GAP_EVENT_TYPE, format!("gap/{boot_id}/{sequence}")).warn().fields(serde_json::json!({ "dropped": dropped })) }`.
  - Add `impl GapEvent for OpsEvent`, with the body of today's `record_loss` recognizer (ops.rs:265-272) and `GAP_EVENT_TYPE` in place of the literal.
  - `record_loss` (264-280): `if let Some(count) = event.represented_gap() { … }`.
  - `drain_ops_once` (306-317): `|gap| OpsEvent::gap_marker(&state.runtime.identity.boot_id, journal.sequence.fetch_add(1, Ordering::Relaxed), gap)`.
  - Add the ops C2 test to `mod tests`.
  - Ratcheted scopes: none change. `PendingOps::take` and `drop` keep their bodies. The new import does not alias any path under an `#[expect]` scope, because `encode_prefix` and `Selection` are spelled `crate::telemetry_batch::…` in full.
- `src/audit.rs`:
  - Add `use crate::telemetry_batch::GapEvent;`.
  - Add `impl AuditEvent { pub(crate) fn gap_marker(boot_id: &str, sequence: u64, event_time_ms: i64, cell: &str, dropped: u64) -> Self { … } }`, which is today's literal from 281-296. It has 5 parameters, which is at the limit and allowed.
  - Add `impl GapEvent for AuditEvent { fn represented_gap(&self) -> Option<u64> { self.dropped } }`.
  - `record_loss`: `if let Some(count) = event.represented_gap()`.
  - `drain_audit_once`: `|gap| AuditEvent::gap_marker(&state.runtime.identity.boot_id, journal.sequence.fetch_add(1, Ordering::Relaxed), state.runtime.clock.now().ms(), state.deployment.cell_id().as_str(), gap)`. The arguments evaluate in the literal's old order.
  - Add the new `mod tests` (§3.2).
  - Ratcheted scopes: none.
- `docs/quality/owners.json`: add `{"category":"macro-dsl","count":1,"owner":"crate::OpsEvent::gap_marker","path":"src/ops.rs","syntax":"serde_json::json","reason":"Gap-marker constructor; the pinned serde_json macro encodes the one dropped-count field that the ledger's represented_gap reads back; unit and DST pins assert the exact field."}`. This macro-dsl row moves with the code, and `source-allowances.json` cannot gain a new identity (its legacy ceiling is 0).
- `docs/quality/source-allowances.json`: prune `(macro-dsl, src/ops.rs, crate::drain_ops_once, serde_json::json)`.
- `scripts/quality/mutation_owners.py:79`: `owner('ops', 'src/ops.rs', 'ops:: dst_tests::runtime_journals::')`, with a comment in the style of the fleet row: "The drain is proven by the runtime journal rigs: `ops::` alone runs no test against `drain_ops_once`."

### C3 — "The ops and audit journals share one GapJournal owner for queue, drop debt and batch requeue"

`src/telemetry_batch.rs`:
- Module doc: "Journal batches: a JSON prefix selection bounded in rows and encoded bytes, and the bounded gap-debt queue that the ops and audit journals drain through it."
- Imports: add `std::collections::VecDeque`, `std::sync::Mutex`, and `std::sync::atomic::{AtomicU64, Ordering}`.
- Add `const MAX_BATCH_ROWS: usize = 512;`, documented as "one append's row bound; a drain prepares exactly the rows a selection can take". Line 43 becomes `events.take(MAX_BATCH_ROWS)`.
- New code, in full:
  ```rust
  /// One runtime's bounded journal queue. Recording never blocks or fails the
  /// transition it describes: past `CAP` a loss is counted and owed as gap
  /// debt, which the next drain publishes as one marker ahead of the queue.
  pub(crate) struct GapJournal<E, const CAP: usize> {
      queue: Mutex<VecDeque<E>>,
      dropped: AtomicU64,
      gap: AtomicU64,
      sequence: AtomicU64,
  }
  impl<E, const CAP: usize> GapJournal<E, CAP> {
      /// The one constructor. `Default` delegates here, so `OpsService` can
      /// keep deriving it for `QuotaRegistry`'s own derived `Default`.
      pub(crate) const fn new() -> Self {
          Self { queue: Mutex::new(VecDeque::new()), dropped: AtomicU64::new(0),
                 gap: AtomicU64::new(0), sequence: AtomicU64::new(0) }
      }
      /// Losses since boot; the drop counter and its alert read this.
      pub(crate) fn dropped(&self) -> u64 { self.dropped.load(Ordering::Relaxed) }
      /// One counter per journal mints its row ids, so no id repeats within a boot.
      pub(crate) fn next_sequence(&self) -> u64 { self.sequence.fetch_add(1, Ordering::Relaxed) }
      /// A loss no drain will ever publish (the runtime has no ledger key):
      /// counted, but owed no marker that could never be written.
      pub(crate) fn count_unreported_loss(&self) { self.dropped.fetch_add(1, Ordering::Relaxed); }
  }
  impl<E, const CAP: usize> Default for GapJournal<E, CAP> {
      fn default() -> Self { Self::new() }
  }
  impl<E: GapEvent, const CAP: usize> GapJournal<E, CAP> {
      /// Never blocks or fails the caller: at the cap the row becomes gap debt.
      #[expect(clippy::unwrap_used, reason = "GapJournal::push; a poisoned journal queue may hold a half-pushed or half-drained event; recovering it could publish or drop an event twice")]
      pub(crate) fn push(&self, event: E) {
          let mut queue = self.queue.lock().unwrap();
          if queue.len() >= CAP {
              self.dropped.fetch_add(1, Ordering::Relaxed);
              self.gap.fetch_add(1, Ordering::Relaxed);
              return;
          }
          queue.push_back(event);
      }
      /// Removes the largest prefix (a gap marker first while debt is owed)
      /// that fits `max_bytes`; `prepare` runs under the lock on exactly the
      /// rows the selection can take.
      #[expect(clippy::unwrap_used, reason = "GapJournal::take; a poisoned journal queue may hold a half-pushed or half-drained event; recovering it could publish or drop an event twice")]
      #[expect(clippy::expect_used, reason = "GapJournal::take; an oversized selection is only produced from a first queued event that overflowed the budget, so the queue holds it; a fallible pop would add a branch no oversized selection reaches")]
      pub(crate) fn take(&self, max_bytes: usize, make_gap: impl FnOnce(u64) -> E,
                         prepare: impl Fn(&mut E)) -> Result<PendingBatch<'_, E, CAP>, String> {
          let mut queue = self.queue.lock().unwrap();
          let gap = self.gap.load(Ordering::Relaxed);
          let mut marker = (gap > 0).then(|| make_gap(gap));
          for event in marker.iter_mut().chain(queue.iter_mut()).take(MAX_BATCH_ROWS) {
              prepare(event);
          }
          let Selection::Encoded { body, count } =
              encode_prefix(marker.iter().chain(queue.iter()), max_bytes)? else {
              if marker.is_some() {
                  return Err("journal byte budget cannot encode its gap marker".into());
              }
              // This event can never fit. Its loss is explicit and the next pass
              // durably publishes gap debt; every later event stays queued.
              let event = queue.pop_front().expect("oversized first event");
              self.record_loss(&event);
              return Err("oversized journal event dropped; gap debt retained".into());
          };
          let mut events = Vec::with_capacity(count);
          if let Some(marker) = marker {
              self.gap.fetch_sub(gap, Ordering::Relaxed);
              events.push(marker);
          }
          let selected = count - events.len();
          events.extend(queue.drain(..selected));
          Ok(PendingBatch { journal: self, events, body })
      }
      fn record_loss(&self, event: &E) {
          if let Some(count) = event.represented_gap() {
              // The represented events were counted at their first loss.
              self.gap.fetch_add(count, Ordering::Relaxed);
          } else {
              self.dropped.fetch_add(1, Ordering::Relaxed);
              self.gap.fetch_add(1, Ordering::Relaxed);
          }
      }
  }
  /// The rows a drain removed, owned across every await of their append.
  /// Dropping it (cancellation, serialization or append failure) requeues the
  /// same rows ahead of newer ones; only a durable append disarms it.
  pub(crate) struct PendingBatch<'a, E: GapEvent, const CAP: usize> {
      journal: &'a GapJournal<E, CAP>, events: Vec<E>, body: Vec<u8>,
  }
  impl<E: GapEvent, const CAP: usize> PendingBatch<'_, E, CAP> {
      /// An empty selection appends nothing, so an idle drain writes no empty array.
      pub(crate) async fn persist<F, Fut>(mut self, append: F) -> Result<usize, String>
      where F: FnOnce(Vec<u8>) -> Fut, Fut: std::future::Future<Output = Result<(), String>> {
          if self.events.is_empty() { return Ok(0); }
          append(std::mem::take(&mut self.body)).await?;
          let count = self.events.len();
          self.events.clear(); // The durable append now owns these events.
          Ok(count)
      }
  }
  impl<E: GapEvent, const CAP: usize> Drop for PendingBatch<'_, E, CAP> {
      #[expect(clippy::unwrap_used, reason = "PendingBatch::drop; a poisoned journal queue may hold a half-pushed or half-drained event; recovering it could publish or drop an event twice")]
      fn drop(&mut self) {
          if self.events.is_empty() { return; }
          let mut queue = self.journal.queue.lock().unwrap();
          for event in self.events.drain(..).rev() {
              if queue.len() < CAP { queue.push_front(event); } else { self.journal.record_loss(&event); }
          }
      }
  }
  ```
  The drop from the cap in `push` stays explicit (one drop and one gap each, as today). It does not go through `record_loss`, because a row a caller pushes is never debt.
- Tests: §3.3 plus the three existing ones, which are unchanged, so the sha of `array_delimiters…` is unchanged.

`src/ops.rs`:
- Imports:
  - add `use crate::telemetry_batch::{GapEvent, GapJournal};`
  - change `use std::sync::atomic::{AtomicU64, Ordering};` to `use std::sync::atomic::Ordering;`, because `RSS_PEAK_MB` spells `AtomicU64` in full, while `collect_snapshot` still reads `Ordering`.
- Delete `OpsQueue` (104-108).
- `OpsService`: keep `#[derive(Default)]` and replace the fields with the block below. `new()` keeps `Self::default()` **unchanged**.
  ```rust
  /// The operator's live view (§12.5); it also shows events the capped journal drops.
  recent: Mutex<VecDeque<OpsEvent>>,
  journal: GapJournal<OpsEvent, OPS_QUEUE_CAP>,
  // mt-lint: allow(name-keyed-map): alert kind, not stream identity
  alerts: …,
  ```
- `dropped()` becomes `self.journal.dropped()`.
- `emit`:
  ```rust
  { let mut recent = self.recent.lock().unwrap(); recent.push_back(ev.clone()); if recent.len() > RECENT_CAP { recent.pop_front(); } }
  self.journal.push(ev);
  ```
  Its reason is **re-decided** to `"OpsService::emit; a poisoned recent ring may hold a half-rotated live view; recovering it could show an evicted event or hide the newest"`.
- `recent()` becomes `self.recent.lock().unwrap().iter().rev().take(limit).cloned().collect()`, with its reason **re-decided** in the same way (owner `OpsService::recent`).
- Delete `PendingOps`, `record_loss` and `persist_ops_batch` (176-290).
- `drain_ops_once`:
  ```rust
  let journal = &state.runtime.ops.journal;
  let batch = journal.take(max, |gap| OpsEvent::gap_marker(&boot_id, journal.next_sequence(), gap), |event| { /* unchanged cell stamp */ })?;
  batch.persist(|body| ops_ledger_append(state, &key, body)).await
  ```
- `mod tests`: adapt `overflow_counts_and_reports` (§3.3) and keep the C2 test.
- Delete `mod cancellation_tests` (779-918) and the `#[path = "ops/batch_tests.rs"] mod batch_tests;` declaration (920-922).
- Delete the file `src/ops/batch_tests.rs`.

`src/audit.rs`:
- Imports: `use crate::telemetry_batch::{GapEvent, GapJournal}; use serde::{Deserialize, Serialize};`. The `VecDeque`, `Mutex` and atomic imports go.
- Module doc lines 7-9 point at the shared `GapJournal` instead of "same discipline as src/ops.rs".
- Replace the struct and impl (99-114) with `/// A denial journal belongs to one server runtime and its durable cell ledger.` and `pub(crate) type AuditJournal = GapJournal<AuditEvent, AUDIT_QUEUE_CAP>;`. `runtime.rs` stays untouched: `AuditJournal::new()` resolves to the `const fn`.
- `observe_denial`:
  - The no-key branch becomes `journal.count_unreported_loss(); return;`.
  - The id uses `journal.next_sequence()`.
  - Lines 159-165 become `journal.push(ev);`.
  - **Remove** its `#[expect(clippy::unwrap_used, …)]` (119-122), because no unwrap remains (`unfulfilled_lint_expectations` is denied).
- Delete `PendingAudit`, `record_loss` and `persist_audit_batch` (168-265).
- `drain_audit_once` becomes `journal.take(max, |gap| AuditEvent::gap_marker(…, journal.next_sequence(), …), |_| {})?.persist(|body| crate::billing::system_append(state, crate::billing::AUDIT_EVENTS_STREAM, &key, body)).await`.
- Delete `mod cancellation_tests` (307-421) and the `batch_tests` declaration (423-425). Keep `mod tests` from C2.
- Delete the file `src/audit/batch_tests.rs`.

**Every ratcheted scope touched in C3, and its remedy**
- `ops.rs OpsService::emit` (`unwrap_used`): the unwrap-site fingerprint changes from `self . queue . lock () . unwrap ()` to `self . recent . lock () . unwrap ()`, which is a new key and would read as "accepted exception grew". The remedy is a re-decided reason, which makes a new identity.
- `ops.rs OpsService::recent` (`unwrap_used`): same situation and same remedy.
- `ops.rs OpsService::open_alerts`, `evaluate_alerts` ×3 and `collect_snapshot`: bodies unchanged. The import change resolves no path in these scopes: none of them names `AtomicU64`, `GapEvent` or `GapJournal`, and `Ordering` still resolves to `std::sync::atomic::Ordering`. No remedy needed.
- `ops.rs PendingOps::take` ×3 and `PendingOps::drop`: deleted. This drops the stale `too_many_arguments` expect, because `take(&self, max_bytes, make_gap, prepare)` has 4 parameters.
- `audit.rs observe_denial`: the expectation is removed. `PendingAudit::take` ×2 and `drop` are deleted.
- `telemetry_batch.rs GapJournal::push`, `take` ×2 and `PendingBatch::drop`: new identities in a new path, so no growth comparison applies. Each reason has exactly two `;` and no `"`.
- Clippy: every function is at most about 40 lines, nesting is at most 3, `take` has 4 parameters, `gap_marker` (audit) has 5, and there are no bool parameters. There are no `_ =>` arms. `new_without_default` is satisfied, `private_interfaces` too (`PendingBatch` is `pub(crate)`), and `needless_pass_by_value` too (`impl Fn` by value, as today).

---

## 5 Mutation analysis (cargo-mutants 27.1.0, `--in-diff`)

**C1**: the only changed file is `src/dst/tests/runtime_journals.rs`, which is neither critical nor registered. No mutant is selected; `mutants` is false.

**C2**:

| file / mutant | killer (within the owner's filters) |
|---|---|
| `ops.rs record_loss → ()` | `ops::cancellation_tests::cancelled_ops_batch_overflow_…` (gap 18) and `ops::batch_tests::oversized_ops_…` (dropped 1) |
| `ops.rs <impl GapEvent for OpsEvent>::represented_gap → None / Some(0) / Some(1)` | `ops::tests::gap_marker_carries_its_wire_shape_and_debt` (`Some(17)`) |
| same, `==` → `!=` | the same test (the marker would give `None`, the lookalike `Some(17)`) |
| `ops.rs OpsEvent::gap_marker → Default::default()` | unviable: `OpsEvent` has no `Default` |
| `ops.rs drain_ops_once → Ok(0) / Ok(1)` | `dst_tests::runtime_journals::gap_markers_keep_…` (`> 1`), via the extended filter; `r10_runtime_journals_…` also catches `Ok(1)` through a 404 journal read that fails at once (`system_read` returns `Ok(None)` on NOT_FOUND, then `.unwrap()` panics) |
| `telemetry_batch.rs` | a trait declaration only: 0 mutants. The planner selects the owner and reports zero explicitly |

**C3** (and the cumulative `24c4c77a..HEAD` range, if the three are pushed together):

| mutant | killer |
|---|---|
| `encode_prefix → Ok(Default::default())` (line 43 touched) | unviable (`Selection` has no `Default`), as at registration |
| `GapJournal::new → Default::default()` | unviable: a non-const trait call inside a `const fn` is E0015. This is why `new` is `const` and holds the fields: with `new() { Self::default() }` the mutant would be **equivalent**. With a plain, non-const `new`, it would recurse without end, and at opt-level 1 that could become a loop, which is a TIMEOUT |
| `<impl Default for GapJournal>::default` | not generated: cargo-mutants skips `impl Default` blocks ("Can't think of how to generate a viable different default"). The same holds for `queue.rs`'s `impl Default for ConsumerConfig` under the registered `queue` owner. Confirm with the `--list` control in §7 |
| `dropped → 0 / 1` | T6 `(3, 3)` and `(4, 3)` |
| `next_sequence → 0 / 1` | T6 `[0, 1, 2]` |
| `count_unreported_loss → ()` | T6 `(4, 3)` |
| `push → ()`; `>=` → `<` | T6 (queue len `CAP`), T1 |
| `take → Ok(Default::default())` | unviable (`PendingBatch` has no `Default`) |
| `gap > 0` → `==` / `<` / `>=` | T1 and T3 (`make_gap = unreachable!` panics under `==` and `>=`); T4 (no marker under `<`) |
| `count - events.len()` → `+` / `/` | T4 (marker present: `drain(..3)` or `drain(..2)` on len 1 panics); T5 (`events.len()` 514 ≠ 512). Without a marker, `+` is equal to `-`, but the mutant is one mutant and dies in T4 and T5 |
| `record_loss → ()` | T2 (gap 18), T4 `(1, 1)` |
| `persist → Ok(0) / Ok(1)` | T1 `Ok(3)`, T4 `Ok(2)`; T3's loop is bounded at 32 pages under `Ok(1)` |
| `<impl Drop for PendingBatch>::drop → ()` | T1 (rows not restored) |
| `queue.len() < CAP` → `==` / `>` | T1 (rows lost when len 1) |
| `queue.len() < CAP` → `<=` | T2 (queue reaches `CAP + 1`) |
| `ops.rs OpsService::emit → ()` | `ops::tests::overflow_counts_and_reports` (dropped 0, recent empty) |
| `ops.rs recent.len() > RECENT_CAP` → `==` / `>=` / `<` | the same test: `recent.len() == RECENT_CAP` exactly (255 under `==` and `>=`, 0 under `<`) |
| `ops.rs OpsService::recent → vec![]` (`vec![Default::default()]` is unviable) | the same test |
| `ops.rs OpsService::dropped → 0 / 1` | the same test (10) |
| `ops.rs drain_ops_once → Ok(0) / Ok(1)` | the DST pin via `dst_tests::runtime_journals::` |
| `ops.rs OpsService::new` | unchanged line, so not selected (derive plus `Self::default()`, as today) |
| `audit.rs` | not an owner, so nothing is selected |
| `src/ops/batch_tests.rs` (deleted) | recorded in `deleted_critical_files`, disposition `prefix-critical-deletion`, and never presented to cargo-mutants. `possible_replacement_files` stays `[]` because the range adds no `.rs` file |

Bounds: every generic test is straight-line or loops at most 32 times. No mutant adds a lock, so none can deadlock. Under
any `ops.rs` mutant, the DST tests fail by assertion or by a 404 `.unwrap()` without waiting, never by blocking. The
90 s per-mutant timeout covers an `ops::` plus `runtime_journals::` run, which is two rig builds plus about 8k in-memory
events. No committer blank-stage mutants (`transaction/mod.rs`) are selected.

---

## 6 Ledgers (each in the same commit as the code that needs it)

- **C1**:
  - `docs/refactor/test-inventory.json`: `--write` goes from 523 to 524 tests. The new test is unmapped; no scenario map entry is needed.
- **C2**:
  - `docs/quality/owners.json`: +1 macro-dsl row (`crate::OpsEvent::gap_marker`, `serde_json::json`).
  - `docs/quality/source-allowances.json`: −1 row (`crate::drain_ops_once` `serde_json::json`), via `gate.py --prune`.
  - `scripts/quality/mutation_owners.py`: the `ops` filter gains `dst_tests::runtime_journals::`, and the edit is marked as tooling in the plan.
- **C3**:
  - `docs/quality/source-allowances.json`: −13 rows, via `--prune`:
    - `src/ops.rs`: `by-path-module crate::batch_tests`; `macro-dsl crate::cancellation_tests::cancelled_ops_append… tokio::select`; `macro-dsl crate::cancellation_tests::cancelled_ops_batch_overflow… serde_json::json`; `unresolved-glob crate::cancellation_tests`.
    - `src/ops/batch_tests.rs`: macro-dsl `byte_bounded_ops… tokio::select`, `crate::event json`, `crate::gap json`; `unresolved-glob crate`.
    - `src/audit.rs`: `by-path-module crate::batch_tests`; `macro-dsl crate::cancellation_tests::cancelled_audit_append… tokio::select`; `unresolved-glob crate::cancellation_tests`.
    - `src/audit/batch_tests.rs`: macro-dsl `byte_bounded_audit… tokio::select`; `unresolved-glob crate`.
    - Kept: `ops.rs` `global RSS_PEAK_MB`, `macro-dsl evaluate_alerts ×3`, `unresolved-glob crate::tests`; `telemetry_batch.rs` `unresolved-glob crate::tests` (count 1, since the new tests use only explicit imports).
  - `docs/quality/owners.json`: no new row. The new tests use no DSL macro and no glob.
  - `docs/refactor/review-mechanisms.json`:
    - `cancelled-telemetry-batch-ownership`:
      - owner: "GapJournal batch guard (ops and audit journals) and fleet durable outbox".
      - `entered_proof`: "The held append's sink runs on its first poll after dequeue (queue observed empty while pending); fleet CAS-clear …" (the fleet part is unchanged).
      - `configuration.queue_capacity`: "GapJournal CAP (1024 in the owner tests; OPS_QUEUE_CAP and AUDIT_QUEUE_CAP in the ledgers)". `prior_gap 17` stays.
      - `limitations`: "GapJournal (both ledgers) uses a controlled entered append sink; …".
      - tests: replace the 4 ops/audit rows with `src/telemetry_batch.rs::cancelled_append_restores_batch_order_and_retry_ids` and `::cancelled_batch_overflow_preserves_full_gap_magnitude` (new sha). Keep the 2 fleet rows.
    - `byte-bounded-journal-drains`:
      - owner: "GapJournal (OpsService and AuditJournal)".
      - configuration: add `"max_batch_rows": 512`.
      - tests: replace the 4 per-ledger rows with `…::byte_bounded_batches_preserve_unselected_and_cancelled_order`, `…::oversized_event_becomes_visible_gap_without_wedging_followers` and `…::prepare_covers_exactly_the_rows_a_batch_can_select`. Keep `array_delimiters…` (same sha).
    - `source_adaptations`: delete the four `src/ops.rs` and `src/audit.rs` `cancelled_*` rows (954-989). They describe tests that no longer exist; the file and name must survive for `fixture_change_failures`.
    - Compute the shas with:
      ```
      python3 -c "import importlib.util,pathlib; s=importlib.util.spec_from_file_location('i','scripts/test-inventory.py'); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); p=pathlib.Path('src/telemetry_batch.rs'); [print(t['name'], t['function_sha256']) for t in m.functions(p.read_text(), p)]"
      ```
  - `scripts/review-evidence.py:135-147`: `required_units` becomes one dict literal holding the six remaining anchors (fleet document, `active_absorber_cancel`, creation, transaction, security_revocation, persistence_faults), in the idiom of `required_fixtures`. The `src/ops.rs` and `src/audit.rs` tuples (138-139) are gone. The code must run on Python 3.11 (a plain dict literal does).
  - Needs no change:
    - `docs/refactor/test-inventory.json`: `src/dst` is untouched in C3.
    - `docs/refactor/test-scenario-map.json` (`RES-003` keeps `overflow_counts_and_reports` in `src/ops.rs`) and `scenario-dispositions.json`: none of the deleted tests is mapped.
    - `src/dst/tests/README.md`: no module added.
    - `docs/refactor/architecture-policy.json`: no function over 100 lines. The `collect_snapshot` exception is untouched and still binding.
    - `mutation_owners.py`: no new file under a critical prefix.
    - `scripts/mt-audit-baseline.txt`: no entries in these files.
    - `docs/quality/legacy-*.json`: immutable, not edited.

---

## 7 Controls (commands and expected output)

Per commit (the `--exact` names are the full test paths):
- **C1**
  - `scripts/test-leg.sh target/quality/i79-c1.log --min 3 --exact dst::dst_tests::runtime_journals::gap_markers_keep_their_ids_sequences_and_counts -- --locked --lib dst_tests::runtime_journals::` is expected to print `test result: ok. 3 passed; 0 failed` and `TESTS_RAN_OK: … floor 3, exact 1`.
  - Then run the §3.1 non-vacuity edits. Each should turn the leg red with the quoted assertion; revert them afterwards.
  - `python3 scripts/test-inventory.py --write` is expected to print `test-inventory: wrote 524 tests`; `--check` is expected to print `test-inventory: OK (524 tests, …)`.
- **C2**
  - `scripts/test-leg.sh target/quality/i79-c2.log --min 17 --exact ops::tests::gap_marker_carries_its_wire_shape_and_debt --exact audit::tests::gap_marker_carries_its_wire_shape_and_debt -- --locked --lib -- ops:: audit::tests:: audit::cancellation_tests:: audit::batch_tests:: telemetry_batch:: dst_tests::runtime_journals::` is expected to report 17 passed (ops 6, audit 5, telemetry_batch 3, runtime_journals 3).
    - Do not use the bare `audit::` filter: it also matches `dst_tests::security_audit::`.
  - Then run the §3.2 non-vacuity edits and revert them.
- **C3**
  - `scripts/test-leg.sh target/quality/i79-c3.log --min 16 --exact telemetry_batch::tests::<each of the 7 new names> --exact ops::tests::overflow_counts_and_reports -- --locked --lib -- ops:: audit::tests:: telemetry_batch:: dst_tests::runtime_journals::` is expected to report 16 passed (ops 2, audit 1, telemetry_batch 10, runtime_journals 3).
  - Then run the §3.3 controls 1-7 and revert them.
- Ledgers:
  - `cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl`
  - `python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl --prune`
  - Expect `git diff docs/quality/source-allowances.json` to show only the removals listed in §6.
- `python3 scripts/review-evidence.py --self-test` is expected to print `review-evidence self-test: OK (33 controls)`. `--check` is expected to print `review-evidence source inventory: OK; execution and external acceptance require receipts`.
  - After committing, `python3 scripts/review-evidence-checkout.py` is expected to print `depth=1: missing-provenance refusal PASS`, `depth=0: anchored comparisons PASS`, `fresh-checkout evidence integration: OK`. The fixture_http rows still anchor `d1131213`.
- `python3 scripts/scenario-map-report.py --check` and `python3 scripts/architecture-gate.py --check` should both pass.
- `bash scripts/quality.sh` is expected to end with `QUALITY_OK`. It covers fmt, clippy `-D warnings`, the ratchet (no "accepted exception grew", no "unregistered source occurrence", no "obsolete source allowances"), rustdoc `-D warnings`, the gates and mt-lint.
- The CI plan, run before pushing the three commits together:
  ```
  QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=24c4c77a python3 scripts/quality/verification_plan.py --out target/plan-i79
  ```
  Expected `plan.json`:
  - `mutation_source_files: ["src/ops.rs","src/telemetry_batch.rs"]`
  - `selected_mutation_owners: ["telemetry_batch","ops"]`
  - `unregistered_mutation_source_files: []`
  - `deleted_critical_files: ["src/ops/batch_tests.rs"]`
  - `possible_replacement_files: []`
  - `mutants/miri/properties_fuzz: true` (buffers, and tooling through `mutation_owners.py`)
- Mutant pre-list:
  ```
  git diff 24c4c77a > target/i79.diff
  cargo mutants --list --json --in-diff target/i79.diff --file src/telemetry_batch.rs --package streams-slate
  ```
  The list should match §5, with no `impl Default for GapJournal` row. Do the same for `--file src/ops.rs`.
- Mutation run: `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=24c4c77a scripts/quality/mutations.sh` should end with both owners at `missed 0, timeout 0`. `telemetry_batch` should show about 20 caught and 3 unviable. `ops` should show about 12 caught and 2 or 3 unviable.
- Full suite: `cargo test --locked --release`, then `python3 scripts/quality/tests_ran.py <log> --inventory docs/refactor/test-inventory.json --skipped 1`, as CI does.

---

## 8 Out of scope
- Stamping the ops cell at `emit` instead of at drain. That would change `/v1/debug/ops-events` rows, which show `"cell":""` today.
- The ops marker's time source (`shard::now_ms()`, wall clock) against audit's runtime clock. Unifying them would change journal rows.
- The fleet CAS outbox (`src/fleet/outbox.rs`). It is a different durability class (the outbox lives inside the CAS object) and is not a `GapJournal`.
- Registering `src/audit.rs` as a mutation owner. After the move it keeps only row construction and HTTP-edge observation, and the algorithm is owned and mutation-tested in `telemetry_batch`.
- Any change to `OPS_QUEUE_CAP`, `AUDIT_QUEUE_CAP`, `RECENT_CAP` or `MAX_BATCH_ROWS`.
- `docs/MULTITENANCY-MAP.md:390`'s stale `src/ops.rs:190` line reference. That map decays by design; `multitenancy-audit.sh` is its enforced form.

## 9 Decisions for Søren
No product, raw, debug or metrics edge change, and no journal wire change. Two policy confirmations, neither blocking:
1. **Review evidence.** The R09 rows for ops and audit move from eight mirrored per-ledger tests to the shared `GapJournal` owner tests: two in the cancellation mechanism, and three in the byte-bounded mechanism counting the new prepare pin. The four `source_adaptations` provenance rows (the `d1131213` originals of the cancellation controls) are retired along with the tests they described, and `review-evidence.py` drops their anchors. The reviewer asked for this; confirm that dropping those provenance rows is acceptable.
2. **CI cost.** Each `ops` mutant now also runs `dst_tests::runtime_journals::` (three rig tests), because `drain_ops_once` has no module-level killer. This follows the precedent of the `fleet` and `system_append` rows. It adds a few minutes to in-diff runs and to the nightly `ops` bucket, well within the job's 240-minute timeout.

---

## Skeptic corrections (C1..C8)

Checked on `24c4c77a` (HEAD = origin/slate, clean), read-only. Verified as stated, with no correction needed:
- Every §1 line citation. The drifted reasons (ops.rs:186-197, audit.rs:178-185). The 512 (telemetry_batch.rs:43, ops.rs:209).
- The wc -l table: 922 / 425 / 119 / 126 / 123 / 215. None of the files is ceilinged.
- Criticality: `src/ops` is in BUFFER_PREFIXES (verification_plan.py:29-30). `telemetry_batch` is registered (mutation_owners.py:76, registered in 77c13d74). `src/audit*` is not critical. No `ops::` test calls `drain_ops_once`.
- `QuotaRegistry` derives `Default` (quota.rs:499-503), and `QuotaRegistry::default()` is used in `quota/*_tests.rs`.
- The re-decision of `OpsService::emit` / `recent` is really needed: `method-call-site` facts carry the call's tokens (tools/quality-syntax/src/scan.rs:233-236), so `self.recent.lock().unwrap()` is a new fingerprint key.
- All 13 C3 `source-allowances.json` prune rows, plus C2's `crate::drain_ops_once` json row, match the file.
- `review-evidence.py` 135-147: after the four anchors go, the depth-1 refusal still has its `fixture_http` anchors.
- The C1 pin is deterministic. The runtime clock is the rig `ManualClock` (fixture_runtime.rs:46-49). No telemetry loop runs in the rig. r10 already proves the rig emits no ops events at build.
- `docs/quality/verification.json`, `docs/refactor/test-additions.json` and `architecture-baseline.json` are read by no gate: `architecture-report.py` runs only `--self-test` in quality.sh:37-39 and ci.yml:52.
- Every non-vacuity control builds.

**C1: the §1.7 use-site list is incomplete.** Missing from it:
- `src/quota.rs:178,203` (`self.ops.emit`) and `:560`
- `src/bootstrap.rs:667` (`QuotaRegistry::new(runtime_caps.ops.clone())`)
- `src/scaler3.rs:550`
- `src/dst/tests/scaler_controller.rs:44,103`
- `src/dst/tests/fixture_http.rs:530`

Each of these uses only `Arc<OpsService>` / `emit`, which the plan keeps, so no code changes. Add them so the list is actually complete.

**C2: T6 and T1 must be written so that they kill the accessor and guard mutants.**
- The only killers of `GapJournal::dropped → 0/1` and `next_sequence → 0/1` sit inside the `telemetry_batch::` filter. The ops and audit accessor tests do not run under it.
- So T6 (and T4) must read the counter through `journal.dropped()`, not `journal.dropped.load(..)`. Only `gap` (which has no accessor) is read as a field.
- T1 must pass `|_| unreachable!()` as `make_gap`, as the originals do (ops.rs:809, audit.rs:339). The `gap > 0 → == / >=` kill in §5 depends on that panic. T3's `marker` also kills `>=`, but only T1 kills `==` when no gap is owed.

**C3: the §5 C3 / push-range table leaves out mutants that C2 introduced and that are still in the final diff.**
- `OpsEvent::gap_marker` is unviable.
- `<impl GapEvent for OpsEvent>::represented_gap` yields `None`, `Some(0)`, `Some(1)`, and `==`→`!=`. All four are killed by `ops::tests::gap_marker_carries_its_wire_shape_and_debt`, including the lookalike.
- Expected `ops` tally over 24c4c77a..HEAD: 13 caught and 2 unviable (`gap_marker`, `recent → vec![Default::default()]`). The 13 caught:
  - represented_gap: 4
  - dropped: 2
  - emit: 4
  - recent `vec![]`: 1
  - drain_ops_once: 2
- §7's "about 12 caught" should say 13.

**C4: three expected failure texts for the non-vacuity controls are wrong.** They become the recorded evidence, so fix them.
- **(a) §3.3 control 3.** The ported T2 asserts `dropped == 1` before `gap == 18` (order at ops.rs:907-916). Under the control it fails first with `2` vs `1` ("only the newly lost event is a new drop"), not gap `2` vs `18`.
- **(b) §3.3 control 5.** `events.clear()` above the await empties the batch before the held append. So T1 fails at the cancellation step: queue `["newer"]` vs `["first","second","newer"]`. It does not fail at the failed retry.
- **(c) §3.2, first control.** `cancelled_ops_batch_overflow_…` fails first at `dropped` `2` vs `1` (ops.rs:907-911), not at gap. The same holds for `cancelled_audit_batch_overflow_…` under the second control (audit.rs:410-414).

**C5: the §4 C3 `drain_ops_once` snippet names an undefined `boot_id`.** It must be `&state.runtime.identity.boot_id` (today's ops.rs:311).

**C6: two doc comments claim more than the code does.** Style rule: a doc comment states a true reason.
- `GapJournal::take`: "prepare runs … on exactly the rows the selection can take" is wrong. `prepare` runs on up to `MAX_BATCH_ROWS` rows (marker first). Under a binding byte budget, prepared rows stay queued; they are stamped, which is harmless for ops. T5 proves the bound only at `usize::MAX`. Say "on every row a selection could take".
- `PendingBatch`: "(cancellation, serialization or append failure)". A serialization error comes from `encode_prefix(..)?` inside `take`, before the guard exists. Drop "serialization".

**C7: §9.2's CI-cost claim ("a few minutes") understates the nightly cost.**
- The scheduled `ops` bucket mutates ops.rs as a whole file: dozens of `collect_snapshot` / `evaluate_alerts` mutants. Each one now also runs three rig tests at opt-level 1 (Cargo.toml:77-80).
- Quantify the cost as mutants × rig-run time; it stays inside rust-quality.yml:51's 240 minutes.
- State the bound: `evaluate_alerts`' page loop (ops.rs:658-690) iterates only over open engines. The runtime_journals rigs open none, so no whole-file mutant there can hang and become a TIMEOUT.

**C8: RUST-QUALITY.md's "Synchronization or retirement changes → Loom" row is not addressed.**
- Add one line saying why no Loom model is needed. Every `gap` read or write in `GapJournal` (push, take, record_loss from take and Drop) happens under the one queue lock. `count_unreported_loss` touches only `dropped`, and the `sequence` counter is a lone `fetch_add`.
- The only new cross-lock ordering is `alerts → recent → queue`, which comes from `evaluate_alerts` emitting under the alerts lock (ops.rs:728-768). Nothing acquires those locks in the reverse order.

**Unbuildable controls:** none. Every §3 and §7 control compiles as a temporary edit. Control 3 leaves `event` unused, which is only a warning under `cargo test`.

**Missed ledgers:** none required. The plan's ledger set is complete: test-inventory (C1), owners and source-allowances, mutation_owners.py, review-mechanisms, and review-evidence.py.

**Verdict: ready-with-corrections.** C2 and C3 are needed for the mutation leg to be predicted exactly. C4 is needed for honest red evidence. The rest are accuracy and wording fixes.
