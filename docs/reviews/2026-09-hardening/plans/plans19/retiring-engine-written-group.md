# F2: a group a retiring engine already WROTE is answered 503 `shard_moving`, yet becomes durable

Repository `/Users/sorenschmidt/code/streams`, branch `slate`. HEAD `eda79ccf` (= `origin/slate`). Every quote below was read on that tree. I edited no repository file and ran no cargo command.

The harness and the SBO tables come from the investigation branch `worktree-wf_e38ad9fe-ae6-3`:
- `8a2791b0`, rebased there as `706430f7`: `src/dst/tests/split_boundary_outcomes.rs`.
- `ca80f9d4`, rebased as `bc2907f7`: the absorber red regression. It is independent of F2.

The branch sits on `d4d631df`, and `slate` has moved six commits since. The only overlap is the two ledgers: `docs/quality/owners.json` and `docs/refactor/test-inventory.json`. The source files are untouched. Line numbers for the harness refer to `bc2907f7:src/dst/tests/split_boundary_outcomes.rs` (841 lines).

---

## 0. Verdict

- **F2 is real and fully explained.** `ShardEngine::begin_close` drains the written-but-not-yet-durable groups out of the terminal handoff. It answers them `AppendErr::Moved` on the spot, which the edge renders as a retryable 503. The same retirement's storage close then flushes the memtable, and the records become durable.
- **This is not the release-hold 500.** It is an at-least-once duplicate for un-keyed appends. The shipped SDK makes it a *silent* duplicate: `Stream.append()` retries every `retryable:true` 503 on its own (§1.7).
- **The engine-level contract already calls this answer "unknown".** SPEC.md G7, docs/DST.md §5, `publish.rs:26-29`, the R17-B record and the `assert_moved` pin all say so. The wire does not say "unknown", though: it uses the same 503 as "never written" and marks it `retryable:true`.
- **Recommended: Option A, the smallest causal fix.** Hand the stranded groups to the engine's own storage close. Once `close_db` returns, answer each group from the store's final `DbStatus::durable_seq`, which is the same proof the live acker's `take_durable` uses:
  - covered by that sequence: the group's staged success;
  - otherwise: the existing `Moved`.
- **Option A adds no new code or status.** It still changes which existing answer this condition receives (503 → success, and 503 → 408 when the close outlasts the 10 s request budget). That makes it an **edge change and an owner decision (D1)**. It also refines the reviewed R17-B contract ("retired replies map to moved/unknown promptly") and changes two pinned R17-B tests.
- **Every option needs the owner.** Options B (a distinct unknown answer) and C (document the at-least-once outcome) are in §2 and §9.

---

## 1. Problem (verified)

### 1.1 The finding as the harness reproduces it

The scenario is `a_written_group_on_a_retiring_parent_engine` (harness lines 746-773). It runs on four prefixes, so the parent keeps prefix `00`, and uses a `Setup::holdable()` rig (FaultStore, cold absorber):
1. The split publishes.
2. The test holds the first WAL PUT (`hold_class(Put, Wal, 1)`).
3. It fires a keyed and a bare product append to the low key `gb`, which the parent engine serves.
4. It waits until `engaged >= 1 && parent.oldest_inflight_ms() >= 50`. That is the entered-proof that a group is *published into `in_flight.pending`*, i.e. written and awaiting durability.
5. `retire_parent` calls `shards.retire(prefix, FleetEviction, |_, _| true)`.
6. After both answers arrive, it calls `store.release_hold()`.

Rows from the investigation run (`scratchpad/sbo-run3.log`):

```
SBO | parent-retiring-written | stranded-written-low Product key="gb" -> seg1 (parent engine 00) | first 503 temporarily_unavailable ra=1 | copies 1 | retry 200 - ra=- dup | copies 1 | AMBIGUOUS: refused but committed
SBO | parent-retiring-written | stranded-written-low-bare Product key="gb" -> seg1 (parent engine 00) | first 503 temporarily_unavailable ra=1 | copies 1 | retry 200 - ra=- | copies 2 | FINDING F2: the retry left 2 copies (retry answered 200 - ra=-)
```

For comparison, the queued twin (`appends_queued_on_a_parent_engine_that_retires`) answers the same 503 with `copies 0`, and its retry commits once. On the wire, *never written* and *written, outcome unknown* are identical.

### 1.2 Causal trace (exact code)

1. **Retirement drains the written groups.** `src/shard_directory.rs:440-462`, `ShardDirectory::retire`, removes the resident under the gate guards and then calls `engine.begin_close();` (line 460).
2. **`begin_close` takes them out of the terminal handoff and fails them.** `src/shard.rs:1855-1898`:
   ```
   1859        let stranded = {
   1860            let mut handoff = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
   1861            let stranded = handoff.retire();
   1862            self.closed.store(true, Ordering::SeqCst);
   1863            stranded
   1864        };
   1865        let first = stranded.is_some();
   1866        if first {
   1867            let _ = self.close_tx.send(true);
   1868            self.pump_wake.notify_one();
   1869            self.tasks
   1870                .begin_close(self.db.clone(), self.history2.clone(), self.prefix.clone());
   1871        }
   ...
   1893        if !first {
   1894            return; // already closing
   1895        }
   1896        for group in stranded.unwrap() {
   1897            group.effects.reject(AppendErr::Moved);
   1898        }
   ```
   `CommitHandoff::retire` (`src/shard/commit_handoff.rs:62-69`) sets `terminal` and returns `std::mem::take(&mut self.pending)`. Those are exactly the groups whose `db.write` returned and which `publish` registered (`transaction/publish.rs:25`, `:86-95`), but which the acker has not yet claimed through `take_durable` (`shard.rs:2982`).
3. **`Moved` becomes a retryable 503.** `src/application/append/contract.rs:299-304`:
   ```
   AppendErr::Moved => Self::new(Unavailable, AppendCode::ShardMoving, "shard fenced by a new owner; retry").retry(1),
   ```
   - Raw (`http.rs:66-73`): 503 `{"error":{"code":"shard_moving","message":"shard fenced by a new owner; retry"}}` with `retry-after: 1`.
   - Product (`product.rs:2353`): `F::Unavailable => ("temporarily_unavailable", "retry shortly", None, true)`, i.e. 503 `retryable:true` plus `retry-after: 1`.
   - `definitively_rejected()` (`contract.rs:307-319`) is false for `Unavailable`, so owed-close debt is kept. Internally the answer is already treated as ambiguous.
4. **The same retirement's close makes the record durable.** `lifecycle.rs:53-75`: `EngineTasks::begin_close` → `begin_shutdown_with(WORKER_GRACE, "storage-close", …)` → `close_db(&db)` (`history_partition.rs:145-156`) → `Db::close`.
   - `tasks/shutdown.rs:57-75`: the driver joins every worker first, including the committer, and only then runs the finalizer. The finalizer is never aborted at grace.
   - Pinned slatedb, `db.rs:692-713`: an open database marks itself closed and then flushes the memtables to L0 (`should_flush` for `close_reason == None`).
   - `memtable_flusher/manifest_writer.rs:722-737`: only after the L0 batch is written to the manifest does it call `oracle.advance_durable_seq(uploaded.last_seq)`.
   - The stranded group's rows sit in that memtable. Once the test releases the held WAL PUT, the close completes, and the replacement engine recovers the record: `copies 1`.
   - R17-B already asserts this recovery: `retirement_tests.rs::r17b_late_successful_write_settles_without_publishing_retired_effects` checks `replacement … next == 1`.
5. **The retry duplicates.** A retry with the producer tuple is recognised as a duplicate (`decide_producer`, `commit_plan.rs:101-121`). A bare retry is a new append on the reopened engine: two copies.

### 1.3 Every producer of `AppendErr::Moved` (complete use-site list)

`grep -rn 'AppendErr::Moved' src`, production sites:

| Site | When | Written? | Outcome |
|---|---|---|---|
| `shard.rs:1897` `begin_close` | groups pending remote durability at retirement | **yes** | **F2**: unknown now, durable after a clean close |
| `transaction/publish.rs:30` | `db.write` returned after retirement (`publication()` is `None`) | **yes** ("Storage may have accepted the batch… Outcome remains unknown", `:26-29`) | residual unknown (§8, D2) |
| `transaction/finalize.rs:58` | a no-write group attaches after retirement (`Attachment::Retired`) | no own write; its truth depends on a retired group | unknown; no duplicate risk (no data) |
| `transaction/mod.rs:52-56` `run` | engine closed before staging | no | nothing committed |
| `shard.rs:2455`, `:2458` committer closed arm | queued ops drained on close | no | nothing committed |
| `shard.rs:2482`, `:2484` set-before-subscribe | op taken after close | no | nothing committed |

String mappings of the same condition:
- `transaction/mod.rs:122` and `commit_plan.rs:69`: queue ops get `"shard fenced/moved; retry"`.
- `contract.rs:299` renders `Moved`.

Test and DST classifiers:
- `dst/runtime.rs:244`, `:337` classify `Moved` as `Outcome::Unknown`.
- `retirement_tests.rs:184-203` (`assert_moved`) pins 503, `retry-after: 1`, not definitively rejected, and no `stream-next-offset`.

### 1.4 Every caller of `ShardEngine::begin_close` (who can strand written groups)

Production callers:
- `shard_directory.rs:460`: explicit retirement (OwnershipMoved, FleetEviction, SweepEviction, Shutdown).
- `sharddir.rs:703`: a died resident removed by its close callback.
- `shard.rs:3058`: the acker sees a `close_reason` (fence or fatal store error). It retires **without** dispatching the `durable_seq` in the same status, so groups that were already durable are also answered `Moved` today.
- `lifecycle.rs:132`: a required worker exited (`RequiredExit`).
- `sharddir.rs:600` (reaper of an over-deadline open) and `sharddir.rs:906` (open refused): these engines were never served, so nothing can be stranded.

Test-only callers:
- the `#[cfg(test)] await_terminated` / `await_workers` helpers (`shard.rs:1771`, `1779`);
- the unit tests.

`EngineTasks::begin_close` has exactly one caller, `shard.rs:1869-1870`. `CommitHandoff::retire` has exactly one production caller, `shard.rs:1861`.

### 1.5 Every answer an append can get while its engine retires (today)

| Where the append is | Answer | Committed? |
|---|---|---|
| resolved, enqueued after `closed` | `try_command` → `EnqueueError::Closed` (`shard.rs:1817-1818`) → `submit.rs:79-85`: 429 `overloaded` "append queue full" | no |
| queued, not taken | committer closed arm: 503 `shard_moving` ra=1 | no |
| taken, before staging | `run` / set-before-subscribe: 503 `shard_moving` | no |
| written, pending durability at retirement | `begin_close`: 503 `shard_moving` (**F2**) | usually yes (clean close), else unknown |
| `db.write` returns after retirement | `publish.rs:30`: 503 `shard_moving` | usually yes (the close runs after the committer is joined) |
| no-write group after retirement | `finalize.rs:58`: 503 `shard_moving` | no own write |
| `db.write` on a store that already detected a fence | `finalize.rs:196` `Err(error) => self.reject(&error.to_string())` → `AppendErr::Internal` → **500** (raw `internal`, product `append_failed` retryable:false) | no (slatedb `check_closed`, `db.rs:278-286`); **out of scope, D6** |
| durable claim won before retirement | the group's success (R17-B) | yes |
| reply sender dropped (worker aborted at grace, finalizer dropped) | `submit.rs:86-92`: 408 `append_timeout` "outcome unknown" (product `append_failed` retryable:false) | unknown |
| resolved after retirement | `from_resolve` (`contract.rs:220-245`): 503 `shard_moving` ra = holdoff remainder, or `shard_closing` / `shard_opening` | no |

### 1.6 What the documents already say about ambiguity

- **SPEC.md:281-293.** G6: "unacked in-flight appends fail into the client retry contract". G7: "On timeout/`408` or a move, an append may or may not have landed; clients must check `Stream-Next-Offset` (or use `Stream-Seq`) before retrying … the honest contract for any at-least-once boundary."
- **docs/DST.md §5 (lines 222-227).** Outcomes are `Acked` / `Rejected` / `Unknown`, with "`Unknown` … an ambiguous fencing error". It also says: "A non-idempotent retry is a second attempt of the same operation — legitimately storable twice." The oracle maps `Moved` to `Unknown` (`dst/runtime.rs:244`).
- **docs/append-transitions.md step 5.** "Timeout remains an explicitly ambiguous failure. An unavailable or foreign owner cannot become a fabricated success."
- **Handover spec.**
  - 04 §8: "an ambiguous timeout is resolved through producer idempotence".
  - 05 §6.3/§8: a split's "ambiguous retry invariant" applies to the *producer tuple*.
  - 05 §8: "The old owner is fenced and cannot acknowledge after the new owner takes possession."
  - 05 §9: "429/503 → honor retry delay, retry exact request".
  - PRISMA_STREAMS_PRODUCT_SURFACE_SPEC §11: "retryable is explicit".
- **WIRE-MATRIX.md:42 (raw append).** Lists "503 … `shard_moving` (`retry-after: 1`)" and "408 `append_timeout`", with no ambiguity note. **:119 (product).** "503 `temporarily_unavailable` (retryable)".
- **docs/review-followup/retirement-handoff.md (R17-B).** "a retired write instead settles every reply as moved/unknown"; "503 `shard_moving`, Retry-After 1, no successful offset and no definite-absence claim". review-mechanisms.json:769, the `oracle` of `terminal-commit-retirement-handoff`, says: "Retired replies map to moved/unknown promptly".

**Reading.** The engine contract already calls the answer ambiguous. The wire gives the same 503 to "nothing committed" rows (§1.5, rows 2-3) and to "committed" rows, and it marks that 503 `retryable:true`.

### 1.7 Why this is a customer-visible defect

The SDK's plain append retries a 503 without asking the application:
- `sdk/src/index.ts:672-678, 709-722`: `Stream.append()` → `appendRaw` → `req()`.
- `:453`, `:467`: `retryable = res.status === 429 || res.status === 503`, overridden by the body's `retryable`.
- `:523-526`, `:573-580`: `retryableRequestError(error) && attempt < 3` → sleep `retry-after` → resend.

The SDK README (lines 64-66) documents this: "Ordinary transient 429/503 responses retry with bounded, abort-aware backoff". Transport errors are *not* retried (`req()` rethrows `StreamsTransportError`), so this 503 is the only automatic path to a duplicate for a plain append. The application sees one success, and two records exist.

For producer appends, the resend carries the same headers and is recognised as a duplicate (row 1 in §1.1).

### 1.8 Line budgets (HEAD `wc -l`)

| File | Now | Limit | After |
|---|---|---|---|
| `src/shard.rs` | 3,139 | no growth (ceilinged) | **3,133** |
| `src/shard/lifecycle.rs` | 135 | 1,000 | ~138 |
| `src/shard/commit_handoff.rs` | 72 | 1,000 | ~92 |
| `src/shard/commit_handoff/loom_tests.rs` | 120 | 1,000 | ~168 |
| `src/shard/retirement_tests.rs` | 562 | 1,000 | ~640 |
| `src/dst/tests/split_boundary_outcomes.rs` (bc2907f7) | 841 | 1,000 (DST) | ~848 |

Untouched, ceilinged: `http.rs` 3,153, `product.rs` 4,205, `history.rs` 1,656.

---

## 2. Contract decision

### 2.1 Options (every one needs the owner)

| | What changes | Wire effect | Code risk | Owner decision because |
|---|---|---|---|---|
| **A (recommended)** | Stranded written groups are answered by the retiring close's final `durable_seq` | 503 → the group's own success once the close completes. The same 503 when the close did not make it durable. 408 `append_timeout` if the close outlasts the 10 s request budget. No new code or status. | small: `begin_close` shrinks; one finalizer line; one pure function; Loom model | the answer to an existing condition changes (edge change) and R17-B's pinned oracle ("promptly … moved/unknown") is refined |
| B | Split `Moved`: written-and-retired → the existing unknown answer | raw 503 `shard_moving` → 408 `append_timeout`; product 503 `temporarily_unavailable` retryable:true → 408 `append_failed` retryable:false | new `AppendErr` variant; new paths in the `unwrap_used` scopes of `publish` and `join_prior_barrier` (fingerprint growth, needs restructuring); `shard.rs` +lines | status and code change. Producer SDK users lose the automatic safe retry: 408 is not retried, so a durable, producer-keyed append surfaces as an error |
| C | Document: the retiring 503 is ambiguous; un-keyed retries are at-least-once | none | none in `src`; harness verdict change | "documented legitimate outcome with an approved protocol/test change". Leaves the SDK's silent duplicate for plain `append()` |

Why A:
- It is the causal fix. The server *knows* the outcome as soon as its own close finishes, and in the common planned-retirement case the answer is "durable".
- It removes the silent SDK duplicate without costing producers anything.
- It removes an `unwrap` and its exception from `begin_close`.
- The only remaining 503s for written data are the genuinely unknown ones (§8), which B or C can address later (D2).

### 2.2 The rule (Option A)

When a retirement drains `pending` (`CommitHandoff::retire`), those `InFlightGroup`s move **by value** into the engine's storage-close finalizer. After `close_db(&db)` returns, the finalizer reads `db.status().durable_seq` once. The status is retained after close; `retirement_tests.rs:281-284` already relies on that. It then settles each group:
- `group.seq <= durable_seq` → `group.effects.reply()`, i.e. the group's **staged** replies: acks and queue acks, including staged duplicates and refusals.
- otherwise → `group.effects.reject(AppendErr::Moved)`.

Why this is sound:
- **Same proof as success.** It is the proof the live acker already uses: `take_durable` claims `group.seq <= durable_seq` (`commit_handoff.rs:53-60`). `DbStatus::durable_seq` is documented as "All writes with a sequence number less than or equal to this value are durably persisted" (slatedb `db_status.rs:33-36`). A memtable flush advances it only after the manifest write (`manifest_writer.rs:722-737`).
- **Fenced owners still acknowledge nothing new (I4).** A fenced or failed writer cannot advance it: close skips the flush when `close_reason` is set (`db.rs:692-704`), and a flush whose manifest write is fenced reports `Fenced`. So a fenced owner still acknowledges nothing written after the fence. What it may now acknowledge is a group that was durable *before* the fence, which the new owner replays. The live acker already does the same today whenever its `dispatch_durable` wins the race.
- **Only replies are released.** The retired incarnation still publishes no tails, rings, touches, usage, pressure, maintenance or signals, exactly as R17-B requires. Watchers are not left stale: the production `on_close` (`bootstrap.rs:503-510`) runs `touch.close_shard(&prefix)`, which wakes every waiter of the shard "with stale" (`touch.rs:407-425`).
- **Ordering holds.** The finalizer runs only after all workers are joined (`tasks/shutdown.rs:67-71`). So:
  - every durable claim the acker won before retirement has already replied, and per-key acknowledgement order (I2) holds;
  - a late `publish` (the committer) happened before the settlement;
  - the replies go out before the engine reports `Stopped`, so before the prefix can reopen.

Unchanged:
- queued appends, whose answers are the rows of §1.5 marked "no";
- writes that finish after retirement (`publish.rs:30`);
- no-write attachments after retirement (`finalize.rs:58`);
- durable claims won before retirement.

Timing: the answer arrives when the close finishes, normally a WAL PUT plus an L0 flush. If the store hangs, the client's own 10 s `APPEND_TIMEOUT` (`submit.rs:86-92`) answers 408 `append_timeout` "outcome unknown", which is the truthful answer. See D4 for a bounded alternative.

### 2.3 Edge change (for D1; record text in §6.5)

| | Before | After |
|---|---|---|
| Condition | engine retires (any reason in §1.4) while a group it wrote awaits remote durability | same |
| Raw | 503 `shard_moving` "shard fenced by a new owner; retry", `retry-after: 1`, at once | the group's own success (204 for non-producer, 200 for a new producer append, 204 for a duplicate) when the close's final durability covers it; otherwise the same 503; 408 `append_timeout` if the close outlasts the request budget |
| Product | 503 `temporarily_unavailable` retryable:true `retry-after: 1` | 200 with the staged offsets; otherwise the same 503; 408 `append_failed` retryable:false on a hung close |
| Queue ops in that group | `"shard fenced/moved; retry"` | their staged result when durable |

---

## 3. Red tests, pins and non-vacuity controls

### 3.1 Red regression (engine level, deterministic)

The test is `src/shard/retirement_tests.rs::f2_a_stranded_written_group_is_answered_by_the_durability_its_close_reached`. It goes at the end of the file:

```rust
/// F2 (split_boundary_outcomes, parent engine retiring): an un-keyed group
/// the engine WROTE is stranded by its retirement while its WAL PUT is held.
/// Nothing answers it before the retirement's own storage close settles its
/// durability; that close makes it durable, so its one answer is its staged
/// success. `Moved` here was retried by clients into a second copy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f2_a_stranded_written_group_is_answered_by_the_durability_its_close_reached() {
    let fixture = Fixture::new("f2-stranded").await;
    let engine = &fixture.engine;
    let status = engine.db.subscribe();
    let durable_before = status.borrow().durable_seq;
    let entered = fixture
        .store
        .hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    let (mut request, mut result) = fixture.append();
    request.producer = None;
    engine.try_enqueue(request).unwrap();
    until(|| {
        entered.load(Ordering::SeqCst) > 0 && !engine.in_flight.lock().unwrap().pending().is_empty()
    })
    .await;
    fixture.remote_missing().await;
    engine.begin_close();
    let early = result.try_recv();
    assert!(
        matches!(early, Err(oneshot::error::TryRecvError::Empty)),
        "a stranded written group was answered before its close: {early:?}"
    );
    assert_eq!(status.borrow().durable_seq, durable_before);
    fixture.store.release_hold();
    let settled = tokio::time::timeout(Duration::from_secs(10), result).await;
    fixture.finish().await;
    let ack = settled
        .expect("the close answers the stranded group")
        .expect("the reply is sent, not dropped")
        .expect("the close made the stranded group durable");
    assert_eq!((ack.last_offset, ack.next_offset, ack.duplicate), (0, 1, false));
    let replacement = Db::builder(engine.prefix.as_str(), fixture.store.clone())
        .build()
        .await
        .unwrap();
    let tail = stored_tail(&replacement.get(tail_key(&HASH)).await.unwrap().unwrap()).unwrap();
    assert_eq!(tail.next, 1, "the close made exactly the one record durable");
    replacement.close().await.unwrap();
}
```

**Expected red output on HEAD** (the test file alone applied; `begin_close` rejects synchronously, so this is deterministic):
```
test shard::retirement_tests::f2_a_stranded_written_group_is_answered_by_the_durability_its_close_reached ... FAILED
---- shard::retirement_tests::f2_a_stranded_written_group_is_answered_by_the_durability_its_close_reached stdout ----
thread '…' panicked at src/shard/retirement_tests.rs:<line>:5:
a stranded written group was answered before its close: Ok(Err(Moved))
```

Why the `Empty` check is deterministic after the fix: the close cannot finish while the WAL PUT is held. `task_lifecycle_tests.rs::r17a_shutdown_timeout_keeps_authority_over_the_held_wal_task` pins that ("held operation must not be reported joined"), as does the R17-B comment at `retirement_tests.rs:281-283`.

### 3.2 Non-vacuity control (committed; passes before and after)

The control is `f2_a_stranded_group_a_new_owner_fenced_stays_moved`, in the same file. It uses the same setup as §3.1, but a new owner opens the prefix while the old engine's WAL PUT is parked. This is the `persistence_faults.rs:187-211` pattern: `max_parked = 1` lets the new owner's fence WAL through. So the retiring close cannot make the group durable:

```rust
/// Control (F2): the same stranded group, but a new owner opens the prefix
/// and fences the held writer first. Its close cannot make the group durable,
/// so the group stays moved (outcome unknown; here nothing was committed):
/// settlement consults durability, it does not acknowledge every stranded group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f2_a_stranded_group_a_new_owner_fenced_stays_moved() {
    let fixture = Fixture::new("f2-fenced").await;
    let engine = &fixture.engine;
    let entered = fixture
        .store
        .hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
    let (mut request, result) = fixture.append();
    request.producer = None;
    engine.try_enqueue(request).unwrap();
    until(|| {
        entered.load(Ordering::SeqCst) > 0 && !engine.in_flight.lock().unwrap().pending().is_empty()
    })
    .await;
    let owner = Db::builder(engine.prefix.as_str(), fixture.store.clone())
        .build()
        .await
        .unwrap();
    engine.begin_close();
    fixture.store.release_hold();
    let settled = tokio::time::timeout(Duration::from_secs(10), result).await;
    fixture.finish().await;
    assert_moved(settled.expect("the close answers the stranded group").expect("the reply is sent"));
    assert!(
        owner.get(tail_key(&HASH)).await.unwrap().is_none(),
        "the fenced write reached no durable copy"
    );
    owner.close().await.unwrap();
}
```

There is one verification point. `close_db` maps `Closed(Fenced)` to `Ok` (`history_partition.rs:153`), so `finish()` should see a clean termination. If the fenced close ever reports another error kind, the reply assertion is still the pin. In that case, replace `fixture.finish()` with `engine.await_workers(Duration::from_secs(10)).await.unwrap()`, the late-write test's pattern. Do not add a `let _ =`, which would need a new exception.

This control is non-vacuous for the fix. A blanket-success settlement (control 7.4c) fails it at `assert_moved`: "retirement cannot acknowledge applied-only truth".

### 3.3 The R17-B pair: changed expectations (`duplicate_order`)

The tests `r17b_retirement_before_duplicate_attachment_cannot_erase_remote_dependency` (retire, staged) and `r17b_retirement_after_duplicate_attachment_owns_every_reply` (retire, attached) keep their names and one-line bodies. Only the helper `duplicate_order` (lines 209-307) changes; its exact new tail is in §4. New expectations:
- in every mode, no reply is sent while the WAL is held. This is the new `first.try_recv()` `Empty` assertion, the R17-B property "no success before durability";
- after release, `first` is `Ok`, not a duplicate;
- attached before retirement: `retry` is `Ok(duplicate)` and `config` is `Ok`. The whole group is settled by the close;
- attached after retirement: `retry` is `Moved`, and `config` is `Err`. `finalize.rs:58` is unchanged.

Red on HEAD with only the test edits (the second red signal):
```
---- shard::retirement_tests::r17b_retirement_after_duplicate_attachment_owns_every_reply stdout ----
assertion failed: matches!(first.try_recv(), Err(oneshot::error::TryRecvError::Empty))
```
The same failure appears for `r17b_retirement_before_duplicate_attachment_cannot_erase_remote_dependency`.

### 3.4 Loom model of the actual transition

The test is `src/shard/commit_handoff/loom_tests.rs::quality_loom_a_stranded_group_is_answered_once_by_the_durability_it_reached`. It uses the existing `explore` bounds: `max_threads` 4, `max_branches` 1,000, preemption bound 2, no duration or permutation truncation. It runs the real `CommitHandoff::retire`, `take_durable` and the new `settle_stranded`:

```rust
type Answer = tokio::sync::oneshot::Receiver<Result<super::super::AppendAck, AppendErr>>;

/// One written group carrying the staged success its client waits for.
fn awaited(seq: u64) -> (InFlightGroup, Answer) {
    let (reply, answer) = tokio::sync::oneshot::channel();
    let mut written = group(seq);
    let staged = super::super::AppendAck {
        last_offset: seq - 1,
        next_offset: seq,
        closed: false,
        producer: None,
        duplicate: false,
    };
    written.effects.acks.push((reply, Ok(staged)));
    (written, answer)
}

/// F2: groups written before a retirement are answered exactly once. The
/// live dispatcher's durable claim (through 1) and the retiring close's
/// settlement (final durability 2) race for them; whoever owns a group, it
/// succeeds only when durability covers it and is moved otherwise.
#[test]
fn quality_loom_a_stranded_group_is_answered_once_by_the_durability_it_reached() {
    explore(|| {
        let mut handoff = CommitHandoff::default();
        let mut answers = Vec::new();
        for seq in 1..=3 {
            let (written, answer) = awaited(seq);
            handoff.publication().unwrap().push(written);
            answers.push(answer);
        }
        let state = Arc::new(Mutex::new(handoff));
        let dispatch = state.clone();
        let dispatch = loom::thread::spawn(move || {
            for claimed in dispatch.lock().unwrap().take_durable(1) {
                claimed.effects.reply();
            }
        });
        let close = state.clone();
        let close = loom::thread::spawn(move || {
            let stranded = close.lock().unwrap().retire().unwrap();
            settle_stranded(stranded, 2);
        });
        dispatch.join().unwrap();
        close.join().unwrap();
        let answered: Vec<_> = answers.into_iter().map(|mut a| a.try_recv().unwrap()).collect();
        assert!(matches!(answered[0], Ok(ref ack) if ack.next_offset == 1));
        assert!(matches!(answered[1], Ok(ref ack) if ack.next_offset == 2));
        assert!(matches!(answered[2], Err(AppendErr::Moved)));
        let mut state = state.lock().unwrap();
        assert!(state.pending().is_empty());
        assert!(state.retire().is_none());
    });
}
```

- `try_recv().unwrap()` fails the model if any reply was dropped. A reply can be sent at most once because each group has one sender.
- Loom explores both orders. When the claim goes first, group 1 is answered by `take_durable`. When retirement goes first, group 1 is answered by `settle_stranded`.
- The three sequence numbers, below, equal to and above the final durability, cover every comparison mutant (§5).

### 3.5 HTTP-level pin (harness)

The harness scenario `a_written_group_on_a_retiring_parent_engine` changes in three ways:
- it releases the WAL once the retirement has begun, while the appends wait;
- it drops `known(F2, …)`;
- it asserts that both appends were acknowledged.

After the fix the two rows read `committed` (§7.2). With the fix reverted it fails twice:
- `finish` fails with the F2 row: `split-boundary violations:\nparent-retiring-written stranded-written-low-bare: the retry left 2 copies (retry answered 200 - ra=-)`;
- the new assertion fails.

---

## 4. Edits, file by file, in commit order

### Commit 0 (prerequisite, not this plan's work)

Land `706430f7` (the harness) on `slate`, and optionally `bc2907f7`. Resolve `docs/quality/owners.json` and `docs/refactor/test-inventory.json` by re-running `python3 scripts/test-inventory.py --write` and re-adding the harness's by-path-module row. This plan's Commit 1 edits that harness file. If the owner does not land the harness, drop §4.1.7 and the harness row from §6, and the engine-level tests still close F2.

### Commit 1: "A group its engine wrote before retiring is answered by the retiring close's durability"

Red first: apply §4.1.5 and §4.1.6 (tests only), run control 7.1, and paste the two red outputs into the message. Then apply the source edits.

#### 4.1.1 `src/shard.rs`: `ShardEngine::begin_close` (lines 1842-1898)

Rewrite the doc comment, keeping 5 lines:
```rust
    /// Proactive close (rebalancer moved this shard away): mark closed and
    /// wake the pump so every queued op fails NOW; the written groups still
    /// awaiting durability go to the storage close, which answers each by
    /// the durability it reaches. Without this, queued requests hang until
    /// the new owner's fence propagates (ladder D3).
```
Delete the `unwrap_used` expectation (lines 1851-1854):
```rust
    #[expect(
        clippy::unwrap_used,
        reason = "ShardEngine::begin_close; the stranded groups were collected under the queue guard that proved them present; a fallible take would add a branch no close reaches"
    )]
```
After this edit `begin_close` has no `unwrap()`. The expectation would be unfulfilled, which is a compile error under `unfulfilled_lint_expectations = "deny"`, so it must go ("Obsolete allowances MUST be removed"). No reason text is edited. The `let_underscore_must_use` expectation (1847-1850) stays unchanged.

Lines 1865-1871 become:
```rust
        let first = stranded.is_some();
        if let Some(stranded) = stranded {
            let _ = self.close_tx.send(true);
            self.pump_wake.notify_one();
            // The written groups this retirement took are answered by the
            // storage close's final durability (commit_handoff::settle_stranded).
            self.tasks.begin_close(self, stranded);
        }
```
Delete lines 1896-1898 (`for group in stranded.unwrap() { group.effects.reject(AppendErr::Moved); }`). Everything else is unchanged: the reader wake loop, `if !first { return; }`, `on_close`, `ops.emit`.

**Ceiling:** `src/shard.rs` goes from 3,139 to 3,133 lines: −4 attribute, −1 call, −3 loop, +2 comment.

**Contracts** (`source_rules.exception_contracts`; the scope is the fn item):
- `(src/shard.rs, …ShardEngine::begin_close, fn, clippy::unwrap_used)` **vanishes**. No new `unwrap_used` contract appears in `shard.rs` or `src/shard/*`, so `exception_predecessors` has nothing to pair it with.
- `(…, clippy::let_underscore_must_use)`:
  - `scope_lines` −4: the call goes from 2 lines to 1 and the loop loses 3;
  - `syntax_facts` −13:
    - `if first` (−1 path) → `if let Some(stranded) = stranded` (+2 paths: `Some`, `stranded`);
    - the old call (12 facts: `begin_close` method-call and site, 3 × `clone` method-call and site, 4 × `self`) → `self.tasks.begin_close(self, stranded)` (5 facts);
    - the loop is removed (−7: `unwrap` ×2, `reject` ×2, paths `stranded`, `group`, `AppendErr::Moved`);
  - `nested_items` 0;
  - every metric shrinks.
- `AppendErr` is still used across `shard.rs`.

#### 4.1.2 `src/shard/lifecycle.rs`: `EngineTasks::begin_close` (lines 53-75); no exception on it

Line 2 becomes `use super::{InFlightGroup, ShardEngine};` (`HistoryPartition` was used only in the old signature). The function:
```rust
    /// Stops the workers, then closes both stores. `stranded` are the written
    /// groups the retirement took from the terminal handoff: they are answered
    /// once the shard store's close has settled their durability.
    pub(super) fn begin_close(&self, engine: &ShardEngine, stranded: Vec<InFlightGroup>) {
        let (db, history) = (engine.db.clone(), engine.history2.clone());
        let prefix = engine.prefix.clone();
        history.stop();
        self.supervisor
            .begin_shutdown_with(WORKER_GRACE, "storage-close", async move {
                // Close independently so one failure cannot orphan the other store.
                let shard = super::history_partition::close_db(&db).await;
                // The close's final durable sequence is the acker's own proof,
                // read once no writer remains: it answers what was stranded.
                super::commit_handoff::settle_stranded(stranded, db.status().durable_seq);
                let history = history.close().await;
                match (shard, history) {
                    (Ok(()), Ok(())) => {
                        tracing::info!(shard = %prefix, "engine workers and stores terminated");
                        TaskResult::Done
                    }
                    (shard, history) => TaskResult::Failed(format!(
                        "{prefix}: shard={shard:?}, history={history:?}"
                    )),
                }
            });
    }
```

Notes:
- The settlement runs whatever `close_db` returned: `durable_seq` is valid proof after a failed close too.
- If the finalizer panics, `catch_unwind` drops the groups, and their clients get 408 "outcome unknown".
- `Db::status` is a pinned slatedb inherent `pub fn` (`db.rs:2049`).
- `InFlightGroup` is private to `shard`, and `begin_close` is `pub(super)`, so the visibility matches. `CommitHandoff::pending` sets the precedent.
- The file's other exceptions (`required` let_underscore; `failure`/`failed` unwrap_used) have unchanged spans.
- Nesting: fn body, then async block, then match. Well under the limit of 4.

#### 4.1.3 `src/shard/commit_handoff.rs` (no exceptions in the file; `#![warn(wildcard_enum_match_arm)]` is not one)

Module doc lines 6-8 become:
```
//! A dispatcher that claimed a remotely durable group before retirement owns
//! its completion. The groups retirement drains belong to the engine's
//! storage close, which answers each by the close's final durable sequence
//! (`settle_stranded`). No other later caller can register or release a
//! successful reply.
```
Line 14 becomes `use super::{AppendErr, DurableEffects, InFlightGroup};`. Before `#[cfg(test)] mod loom_tests;`, add:
```rust
/// Answers the groups a retirement drained, once the engine's storage close
/// has settled their durability. `durable_seq` is that close's final durable
/// sequence: the proof `take_durable` claims by. A covered group releases its
/// staged replies; any other is moved, its outcome unknown. Only replies are
/// released: a retired incarnation publishes no tails, rings, touches, usage,
/// pressure or signals.
pub(super) fn settle_stranded(stranded: Vec<InFlightGroup>, durable_seq: u64) {
    for group in stranded {
        if group.seq <= durable_seq {
            group.effects.reply();
        } else {
            group.effects.reject(AppendErr::Moved);
        }
    }
}
```
This adds no exception: it reuses `DurableEffects::reply` and `reject` (`commit_plan.rs:51-78`), whose own `let_underscore_must_use` and `needless_pass_by_value` expectations are unchanged.

#### 4.1.4 `src/shard/commit_handoff/loom_tests.rs`

Add `type Answer`, `awaited` and the Loom test from §3.4. `super::*` already imports the parent's `AppendErr` import, `settle_stranded`, `InFlightGroup` and `DurableEffects`. There are no exceptions in the file.

#### 4.1.5 `src/shard/retirement_tests.rs` (`#![cfg(test)]`)

- Append the two tests of §3.1 and §3.2 (≈ 75 lines, each well under 100).
- Replace `duplicate_order`'s tail, lines 284-306, as follows. Lines 209-283 are unchanged.
  ```rust
      assert_eq!(status.borrow().durable_seq, durable_before);
      assert!(matches!(first.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
      if !retire {
          fixture.remote_missing().await;
      }
      fixture.store.release_hold();
      let first = tokio::time::timeout(Duration::from_secs(10), first).await;
      let retry = tokio::time::timeout(Duration::from_secs(10), retry).await;
      let config = tokio::time::timeout(Duration::from_secs(10), config).await;
      let empty = engine.in_flight.lock().unwrap().pending().is_empty();
      fixture.finish().await;
      let first = first.unwrap().unwrap();
      let retry = retry.unwrap().unwrap();
      let config = config.unwrap().unwrap();
      assert!(empty, "no sender survives its terminal handoff");
      assert!(!first.unwrap().duplicate);
      if retire && !attach_first {
          assert_moved(retry);
          assert!(config.is_err());
      } else {
          assert!(retry.unwrap().duplicate);
          assert!(config.is_ok());
      }
  ```
  Under retirement, `remote_missing()` would read a closing database (the comment at 281-283 explains why), so it stays under `if !retire`.

**Contract:** `(retirement_tests.rs, duplicate_order, fn, clippy::fn_params_excessive_bools)` must not grow, and it does not:
- `scope_lines`: 23 → 23 for lines 284-306.
- `syntax_facts`: 22 → 22 (worked out below).
- `nested_items`: 0.
- The literal `1` → `10` in `Duration::from_secs` changes a call-site *value*, not a count. This lint keeps no fingerprints.

The `syntax_facts` arithmetic. Before (22):
- `if !retire` (1 path);
- `fixture.remote_missing()` (3: method-call, site, `fixture` path);
- `fixture.store.release_hold()` (3);
- `if retire` (1);
- `assert_moved(first)` and `assert_moved(retry)` (3 each: call-site plus two paths);
- four `assert!` macros (2 each: macro and macro-tokens).

After (22):
- `assert!(matches!(…))` (2);
- `if !retire { fixture.remote_missing() }` (4);
- `release_hold` (3);
- `assert!(!first…)` (2);
- `if retire && !attach_first` (2);
- `assert_moved(retry)` (3);
- three `assert!` macros (6).

`duplicate_order` stays under clippy's 100 lines. It currently has no `too_many_lines` exception, and this edit is line-neutral.

#### 4.1.6 `scripts/quality/mutation_owners.py`

After `owner('commit_handoff', …)`, add:
```python
    owner('commit_handoff_loom_tests', 'src/shard/commit_handoff/loom_tests.rs', 'shard::'),
```
The file is under the `src/shard` critical prefix. It has no `#![cfg(test)]` (it is included by `#[cfg(test)] mod loom_tests;`), so the planner counts it as changed production and fails closed on an unregistered source (`mutation_owners.py:260-297`; `test_unregistered_live_critical_source_remains_selected_to_fail_closed`). `transaction_tests`, `retirement_tests` and others are registered the same way. cargo-mutants skips the `#[cfg(test)]` module, so discovery reports zero mutants there and claims no experiment.

#### 4.1.7 `src/dst/tests/split_boundary_outcomes.rs` (as at `bc2907f7`; not ceilinged, ≤ 1,000; no exceptions)

- Module doc, lines 33-41, becomes:
  ```
  //! No phase answers 500. One finding is named where it reproduces (a fix
  //! turns its rows into passes; any other failure fails the scenario):
  //! - F1: a retry of an append the PARENT committed reaches the high child,
  //!   whose engine reads producer and Stream-Seq lineage only from its own
  //!   database, so the parent's rows are invisible and the retry commits a
  //!   second copy (red regressions below).
  //!
  //! F2 is fixed: a group the parent WROTE before its engine retired is
  //! answered by the retiring close's durability, so it is acknowledged
  //! instead of refused `shard_moving` while its record becomes durable
  //! (`a_written_group_on_a_retiring_parent_engine`).
  ```
- Delete lines 61-62 (`/// F2's failure …` and `const F2`). Otherwise the const is dead code, a `-D warnings` failure.
- The scenario, lines 746-773, becomes:
  ```rust
  /// The parent engine retiring while a group it WROTE awaits durability: the
  /// WAL write is held, the directory retires the prefix, and the store is
  /// released while the appends wait. The retiring close makes the group
  /// durable and answers it, so both appends are acknowledged once (F2).
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_written_group_on_a_retiring_parent_engine() {
      let (store, setup) = Setup::holdable();
      let sc = Scenario::start("sbo-stranded", setup).await;
      assert!(sc.split().await, "the split publishes");
      let parent = sc.parent_engine().await;
      let shots = [
          Shot::product("stranded", "written-low", LOW),
          Shot::product("stranded", "written-low-bare", LOW).bare(),
      ];
      let engaged = store.hold_class(crate::dst::StoreOp::Put, crate::dst::ObjClass::Wal, 1);
      let retire = async {
          let written = || {
              engaged.load(std::sync::atomic::Ordering::SeqCst) >= 1
                  && parent.oldest_inflight_ms() >= 50
          };
          wait_for("a written group to wait on its held WAL write", written).await;
          retire_parent(&sc);
          store.release_hold();
      };
      let (fired, ()) = futures_util::future::join(sc.fire(&shots), retire).await;
      let refused: Vec<String> = fired
          .iter()
          .filter(|(_, answer)| !answer.ok())
          .map(|(shot, answer)| format!("{} {}", shot.marker, answer.show()))
          .collect();
      await_reopen(&sc).await;
      sc.finish("parent-retiring-written", fired, &[]).await;
      assert!(
          refused.is_empty(),
          "the retiring close made the written group durable, yet: {refused:?}"
      );
  }
  ```
  Releasing inside `retire` is required: after the fix the answers wait for the close, and the close waits for the held PUT.

#### 4.1.8 Documents (no ceilings)

- **`docs/review-followup/retirement-handoff.md`.** Add a paragraph before "Status:":
  > F2 follow-up (release hold, split_boundary_outcomes): retirement no longer answers the written groups it drains as moved on the spot. `ShardEngine::begin_close` hands them to the engine's storage close (`EngineTasks::begin_close`), which, once `close_db` has returned, answers each by the store's final `DbStatus::durable_seq` — the proof `take_durable` claims by: its staged replies when covered, moved/unknown otherwise (`commit_handoff::settle_stranded`). Only replies are released; the retired incarnation still publishes no applied/durable mirrors, rings, touches, usage, pressure or signals. Writes that finish after retirement and no-write attachments after it still settle as moved at once. Regressions: `f2_a_stranded_written_group_is_answered_by_the_durability_its_close_reached`, its fenced control `f2_a_stranded_group_a_new_owner_fenced_stays_moved`, the updated duplicate-order pair, and `quality_loom_a_stranded_group_is_answered_once_by_the_durability_it_reached`.
- **`docs/append-transitions.md`, step 5.** Append: "A group the owner wrote before it retired is answered by the retiring close's final durability: its success when covered, otherwise 503 `shard_moving`, whose outcome is unknown (SPEC.md G7)."
- **`docs/refactor/WIRE-MATRIX.md:42`.** Replace `` `shard_moving` (`retry-after: 1`) `` with:
  > `shard_moving` (`retry-after: 1`; nothing committed for an append the retiring engine never wrote; outcome unknown, SPEC.md G7, for a write that completed after retirement or one the retiring close did not make durable; a group written before retirement is answered by that close: its success when durable)
  
  §1.2 (raw) and the product §119 status list are otherwise unchanged.

### Commit 2 (after D1/D5 approval; docs only): "The retiring-close settlement is recorded as an owner-approved edge change"

In `docs/reviews/2026-09-hardening/edge-changes.md`, add record **#53** (text in §6.5) under "Medium risk", plus the index row. Update the count table:
- Medium: both 6 → 7, total 11 → 12;
- Total: both 14 → 15, total 52 → 53;
- "52 records in total" → 53.

---

## 5. Mutation analysis

These are the critical prefixes and registered owners of `mutation_owners.py`:
- `src/shard.rs` (owner `shard`);
- `src/shard/lifecycle.rs` (`shard_lifecycle`);
- `src/shard/commit_handoff.rs` (`commit_handoff`);
- `src/shard/commit_handoff/loom_tests.rs` (new row);
- `src/shard/retirement_tests.rs` (`retirement_tests`; `#![cfg(test)]`, so it is `production_unchanged` and no mutants are run).

All use the test filter `shard::`. `src/dst/**` is not a critical prefix.

Mutants whose spans intersect the diff (`cargo mutants --in-diff`):

| Mutant | Caught by | How |
|---|---|---|
| `ShardEngine::begin_close` → `()` (existing mutant, re-selected) | §3.1 red; every R17-A/B test | Without the close, `fixture.finish()` → `await_terminated(10 s)` → `Err("engine shutdown still running…")` → `unwrap` panics. It fails within 10 s, the same bound as today. |
| `EngineTasks::begin_close` → `()` (re-selected) | §3.1 | `stranded` is dropped at return, so `early` is `Err(Closed)` and the `Empty` assertion fails at once |
| `settle_stranded` → `()` | §3.4, §3.1 | dropped replies: `try_recv().unwrap()` panics; the red test gets `RecvError` |
| `<=` → `>` in `settle_stranded` | §3.4 (group 2 is answered `Moved`); §3.1 (`Moved`) | assertion |
| `<=` → `<` / `==` (if the pinned cargo-mutants emits them) | §3.4: seq 2 == durable 2 kills `<`; the close-first interleaving answers seq 1 < 2 as `Moved`, which kills `==` | Loom explores that interleaving |

No new mutants live in test-only files; cargo-mutants skips `#[cfg(test)]` modules. There is no TIMEOUT risk beyond today's: neither re-selected function-level mutant introduces an unbounded wait in a `shard::` test, because every awaited termination is bounded. The expected result is 0 MISSED and 0 TIMEOUT.

---

## 6. Ledgers (same commit as the code)

1. **`docs/refactor/review-mechanisms.json`**, mechanism `terminal-commit-retirement-handoff` (lines 759-822):
   - `oracle` (769) becomes: "Retired replies map to moved/unknown promptly with no stranded senders or new live mirrors, except written groups drained at retirement, which the storage close answers by its final durable sequence (staged success when covered, moved otherwise); actual durable claims retain exact completion; replacement recovers all canonical data/config/close/billing rows once"
   - `entered_proof`: append "; a held WAL PUT released only after retirement, and a new owner fencing the held writer (F2 control)".
   - `tests`: add `f2_a_stranded_written_group_is_answered_by_the_durability_its_close_reached`, `f2_a_stranded_group_a_new_owner_fenced_stays_moved` (file `src/shard/retirement_tests.rs`) and `quality_loom_a_stranded_group_is_answered_once_by_the_durability_it_reached` (file `src/shard/commit_handoff/loom_tests.rs`). Each takes the `function_sha256` from `inventory.functions` (§7.6). None is `#[ignore]`d, as `review-evidence.py:182-183` requires.
   - `support_functions`: `duplicate_order` gets a new sha (810). `assert_moved`, `completion_checkpoint` and `remote_missing` are unchanged.
   - The two R17-B wrapper tests keep their shas, because their bodies are unchanged.
2. **`docs/refactor/test-inventory.json`**: `python3 scripts/test-inventory.py --write`. Exactly one entry changes: `a_written_group_on_a_retiring_parent_engine` (`function_sha256`). Comments and the removed const do not affect other functions' token hashes.
3. **`scripts/quality/mutation_owners.py`**: +1 row (§4.1.6).
4. **`docs/quality/owners.json`**: no change. No new glob, spawn, by-path module or exception occurrence: `loom::thread::spawn` and the standard macros are not classified.
5. **`docs/refactor/WIRE-MATRIX.md`**: §4.1.8.
6. **Edge record** (Commit 2, after approval). Proposed text:
   > ### #53 <Commit 1 sha> — A group its engine wrote before retiring is answered by the retiring close's durability
   > - **Program item:** release hold (split-boundary outcomes), finding F2
   > - **Surface:** both
   > - **Endpoint:** Raw POST /v1/stream/{name}; product POST /v1/streams/{name}/records and records:batch, the final-record append inside :seal; internal AppendService appends; consumer queue ops committed in the same group.
   > - **Condition:** the shard engine retires (fleet eviction, ownership moved, sweep eviction, shutdown, a failed required worker, a detected fence) while a commit group it has written awaits remote durability.
   > - **Before:** answered at once: raw 503 `shard_moving` "shard fenced by a new owner; retry", Retry-After: 1; product 503 `temporarily_unavailable` retryable:true, Retry-After: 1; queue ops "shard fenced/moved; retry". The retiring close then usually made the records durable, so an un-keyed retry (including the SDK's automatic 503 retry of `append()`) stored a second copy.
   > - **After:** answered when the engine's storage close completes: the group's staged result when the close's final durable sequence covers it (raw 204/200, product 200 with offsets); otherwise the same 503. A close that outlasts the append's 10 s budget yields the existing 408 `append_timeout` (product 408 `append_failed`, retryable:false). Queued appends, writes finishing after retirement and no-write attachments after it are unchanged.
   > - **Retry semantics:** a retryable 503 whose record usually committed becomes a success; no client-side duplicate. A hung close turns an immediate retryable 503 into a 408 at the request deadline.
   > - **Who is affected:** every writer to a shard whose engine retires with writes in flight.
   > - **Pinning tests:** src/shard/retirement_tests.rs::f2_a_stranded_written_group_is_answered_by_the_durability_its_close_reached; ::f2_a_stranded_group_a_new_owner_fenced_stays_moved; ::r17b_retirement_before_duplicate_attachment_cannot_erase_remote_dependency; ::r17b_retirement_after_duplicate_attachment_owns_every_reply; src/shard/commit_handoff/loom_tests.rs::quality_loom_a_stranded_group_is_answered_once_by_the_durability_it_reached; src/dst/tests/split_boundary_outcomes.rs::a_written_group_on_a_retiring_parent_engine.
   > - **Risk reason:** medium: success replaces a retryable error for the same condition, the answer is delayed by the close, and a hung close changes 503 to 408.

---

## 7. Controls (exact commands, expected outputs)

Run everything in a worktree at `origin/slate` with Commit 0 applied. The main checkout may carry other sessions' work.

### 7.1 Red first (tests only: §4.1.5; `src` unchanged)
```
cargo test --locked --lib shard::retirement_tests:: 2>&1 | tee /tmp/f2-red.log
```
Expected:
- `f2_a_stranded_written_group_is_answered_by_the_durability_its_close_reached ... FAILED` with "a stranded written group was answered before its close: Ok(Err(Moved))";
- both retire-mode `r17b_retirement_*_duplicate_attachment_*` tests FAILED with "assertion failed: matches!(first.try_recv(), Err(oneshot::error::TryRecvError::Empty))";
- `f2_a_stranded_group_a_new_owner_fenced_stays_moved ... ok`, `r17b_live_duplicate_waits_for_actual_remote_durability ... ok`, `r17b_durable_dispatch_claim_before_retirement_keeps_its_completion ... ok`, `r17b_late_successful_write_settles_without_publishing_retired_effects ... ok`.

### 7.2 Green (all of Commit 1)
```
cargo test --locked --lib shard::retirement_tests::            # test result: ok. 7 passed; 0 failed
cargo test --locked --lib quality_loom                          # includes the new model ... ok
cargo test --locked --lib shard::                               # all ok
cargo test --locked --lib dst::dst_tests::split_boundary_outcomes -- --nocapture 2>&1 | grep -E 'SBO \| parent-retiring|test result'
```
Expected SBO rows (the engine prefix number varies):
```
SBO | parent-retiring-written | stranded-written-low Product key="gb" -> seg1 (parent engine 00) | first 200 - ra=- | copies 1 | retry 200 - ra=- dup | copies 1 | committed
SBO | parent-retiring-written | stranded-written-low-bare Product key="gb" -> seg1 (parent engine 00) | first 200 - ra=- | copies 1 | retry - | copies 1 | committed
```
The `parent-retiring-queued` rows are unchanged (503, copies 0, retry committed once). The result line is `test result: ok. 9 passed; 0 failed; 2 ignored`; the 2 ignored are the F1 reds.

### 7.3 Invariants the change must not disturb (I1/I4, handoffs, held-WAL closes)
```
cargo test --locked --lib dst::dst_tests::persistence_faults dst::dst_tests::runtime_isolation dst::dst_tests::runtime_retirement dst::dst_tests::producer_handoff dst::dst_tests::request_topology_debt shard::task_lifecycle_tests shard::durability_frontier_tests shard::commit_command_tests
```
All must pass, in particular:
- `a_fenced_owner_acknowledges_nothing` (ghost acks 0);
- `a_handoff_with_an_append_in_flight_resolves_safely`;
- `a_fenced_owners_absorber_exits`;
- `r17a_*`;
- `r24_prior_group_close_retry_and_fence_wait_on_actual_remote_frontier`.

### 7.4 Non-vacuity controls (local edits, reverted, never committed)

- **(a)** Make `settle_stranded` reject every group.
  - §3.1 fails: "the close made the stranded group durable: Moved".
  - §3.4 fails at `answered[0]` or `answered[1]`.
  - The harness fails with the F2 row violation and `refused`.
- **(b)** Change `<=` to `<`. §3.4 fails at `answered[1]`.
- **(c)** Use `settle_stranded(stranded, u64::MAX)` (blanket success). §3.2 fails in `assert_moved`: "retirement cannot acknowledge applied-only truth: AppendAck { … }".
- **(d)** Restore the old `begin_close` loop (reject at once). You get §7.1's three failures, plus the harness failure.

### 7.5 Quality gate (the ratchet measures the merge base, `QUALITY_BEFORE_SHA` on a push)
```
scripts/quality.sh
```
Expected:
- `quality ratchets: OK`;
- no `accepted exception grew` line;
- no `file growth:` line: `src/shard.rs` is 3133, below its 3139 ceiling;
- `cargo clippy --locked --workspace --all-targets -- -D warnings` and `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items` are clean.

The measured contract deltas to confirm against §4.1.1:
- `begin_close` `let_underscore_must_use`: `scope_lines` −4, `syntax_facts` −13;
- `begin_close` `unwrap_used`: gone;
- `duplicate_order`: all metrics equal.

### 7.6 Ledgers
```
python3 scripts/test-inventory.py --write && python3 scripts/test-inventory.py --check
python3 - <<'EOF'
import importlib.util, pathlib
s = importlib.util.spec_from_file_location('inv', 'scripts/test-inventory.py'); inv = importlib.util.module_from_spec(s); s.loader.exec_module(inv)
for f in ['src/shard/retirement_tests.rs', 'src/shard/commit_handoff/loom_tests.rs']:
    p = pathlib.Path(f)
    for fn in inv.functions(p.read_text(), p, include_helpers=True):
        if fn['name'].startswith(('f2_', 'duplicate_order', 'quality_loom_a_stranded')):
            print(f, fn['name'], fn['function_sha256'])
EOF
python3 scripts/review-evidence.py --check
```
Expected: `--check` passes both times, after pasting the printed shas into review-mechanisms.json.

### 7.7 Mutation leg (as CI plans it)
```
cargo build --locked -p streams-quality-syntax
QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=<slate before this series> python3 scripts/quality/verification_plan.py --out target/quality-plan
QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=<same> QUALITY_MUTANTS_OUT=target/quality-mutations scripts/quality/mutations.sh
```
Expected `plan.json`:
- `mutants: true`;
- `mutation_source_files`: `src/shard.rs`, `src/shard/commit_handoff.rs`, `src/shard/commit_handoff/loom_tests.rs`, `src/shard/lifecycle.rs`;
- `unregistered_mutation_source_files: []`;
- `retirement_tests.rs` in `production_unchanged_files`.

Expected mutation outcome: every mutant in §5 caught, and 0 MISSED and 0 TIMEOUT.

### 7.8 Whole gate before pushing
```
OUT=/tmp/gate-f2.txt scripts/gate.sh; tail -5 /tmp/gate-f2.txt
```
Expected: no `GATEFAIL-*`, and the suite floor from `tests_ran.py` is met. `post_split_throughput_scales` runs alone and is unaffected: splits do not retire the parent engine.

---

## 8. Out of scope (named, with evidence)

- **Residual "unknown" 503s** (D2). All three still answer 503 `shard_moving` at once:
  - `publish.rs:24-31`: a `db.write` that returns after retirement. It is reachable when the write is blocked in slatedb backpressure at the retirement instant. The data is in the memtable the close flushes, so an un-keyed SDK retry can still duplicate.
  - `finalize.rs:58`: a no-write attachment after retirement. It has no data, so it carries no duplicate risk.
  - Groups the retiring close could not make durable (a fence or failed close). These are genuinely unknown or lost.

  Extending the settlement to late writes needs a settlement slot shared between `publish` and the finalizer. That touches `CommitTransaction::publish`'s `unwrap_used` scope, which needs restructuring, plus its own Loom model.
- **A 500 at a real ownership move** (D6). `db.write_with_options` on a store that has detected a fence returns `Closed(Fenced)` (slatedb `check_closed`, `db.rs:278-286`). `finalize.rs:196` turns that into `AppendErr::Internal` → raw 500 `internal`, product 500 `append_failed` retryable:false. The window is between the WAL-flush fence detection and the acker's `begin_close` (`shard.rs:3056-3059`). This belongs to the release-hold family ("no 500 at a move"). It needs its own harness row (a new owner fencing mid-traffic) and a typed mapping.
- **SDK auto-retry semantics** (D3). `Stream.append()` retries any `retryable:true` 503 (§1.7). After A, only the residual unknowns can duplicate.
- **Wording.**
  - The raw message "shard fenced by a new owner; retry" is used for every retirement, not only fences.
  - A closed engine's enqueue refusal renders as 429 `overloaded` "append queue full" (`submit.rs:79-85`; nothing committed).

  Changing either is an edge change.
- **F1** (lineage across the split) and the **absorber double retirement 500** (`ca80f9d4`) have their own plans.
- **No settlement deadline shorter than the request budget.** See D4.

---

## 9. Decisions for the owner

1. **D1 — Adopt Option A (edge change).** Engine retirement answers the written groups it strands from the retiring close's final durability. Where that durability covers a group, the client gets its success. Otherwise it gets today's 503. On a close longer than 10 s it gets the existing 408 `append_timeout` (§2.3, record §6.5, medium risk).
   - This refines R17-B's reviewed oracle ("moved/unknown promptly").
   - It changes the pinned expectations of `r17b_retirement_{before,after}_duplicate_attachment_*`:
     - with attachment before retirement, all three replies now succeed after the close;
     - with attachment after retirement, `first` succeeds and `retry` stays moved.
   - Without this approval, nothing in §4 may land.
2. **D2 — Residual unknown 503s** (§8, first bullet). Pick one:
   - **(a) accept and document now** (recommended for release): the WIRE-MATRIX note in §4.1.8 plus an SDK README caveat that a plain `append()` retried after 503 `shard_moving` can store twice, so use a `Producer` for exactly-once;
   - **(b) extend the settlement to late writes** (shared slot, publish restructure, Loom);
   - **(c) Option B for the residuals** (408 `append_timeout` / product `append_failed` retryable:false), which costs producers their automatic safe retry.
3. **D3 — SDK.** Keep the automatic 503 retry for non-producer appends (documented at-least-once for the residuals), or stop retrying non-producer appends on 503 (an SDK edge change). The product error cannot currently tell "never written" from "unknown": both are `temporarily_unavailable`.
4. **D4 — Settlement timing.** Recommended: no extra bound; the request's own 10 s budget answers 408 "outcome unknown" on a hung close. The alternative is a bounded settle that races the close against N seconds and settles by the durability reached so far. That keeps the answer prompt, but it can return a retryable 503 for a group the close later makes durable.
5. **D5 — Where the edge record goes.** Record #53 in `docs/reviews/2026-09-hardening/edge-changes.md`, with the index and counts updated, or a separate release-safety record.
6. **D6 — Fenced-write 500** (§8). Open it as its own release-hold item: a harness row where a new owner fences mid-traffic, and a typed mapping of `Closed(Fenced)` from `db.write` to `Moved`. It is out of this plan's scope.
7. **D7 — If A is declined:** Option C.
   - Approve a harness verdict change: a bare append answered 503 `shard_moving`/`temporarily_unavailable` whose record proves durable and whose retry stores twice is classified "documented at-least-once (SPEC G7)" instead of a finding.
   - Add the WIRE-MATRIX and SDK README notes.
   - Leave `src` unchanged.
   - This keeps the SDK's silent duplicate for plain `append()`.
8. **D8 — Prerequisite.** Land the investigation harness (`706430f7`, and `bc2907f7` if wanted) before Commit 1, or accept Commit 1 without the harness edit (§4.1.7). The engine-level regressions close F2 on their own.

---

## Skeptic corrections (C1..C12)

I checked this plan against the tree at `46f668b3`. `slate` moved one commit past `eda79ccf` while the plan was being written: `46f668b3` touches `src/auth.rs`, `src/auth/lease.rs` and `docs/refactor/WIRE-MATRIX.md`, but not line 42. I also read `8a2791b0` and `ca80f9d4` (rebased as `706430f7` and `bc2907f7`). The pinned slatedb is `~/.cargo/git/checkouts/slatedb-a6e73982df30678a/0717cc1`. I measured the contract claims with the gate's own code: `source_rules.exception_contracts` and `exception_growth`, over `common.tracked_sources` and `common.syntax`, using the prebuilt `target/debug/streams-quality-syntax`. `tools/quality-syntax` has not changed since that binary was built. The inputs were patched copies of the files; the scripts are in `scratchpad/f2skeptic/measure.py` and `measure2.py`. No repository file was edited and cargo was not run.

The core of Option A holds up:
- `CommitHandoff::retire` moves the stranded groups out by value, so each sender has exactly one owner.
- `take_durable` returns nothing once the handoff is terminal (`commit_handoff.rs:54-60`).
- The finalizer runs only after the workers are joined (`tasks/shutdown.rs:67-71`), and before `Phase::Stopped` is set.
- `DbStatus::durable_seq` is valid proof after a close. `report_durable_seq` keeps updating after close marks the status closed (`db_status.rs:137-146`, `db.rs:697-699`).
- A fenced writer cannot advance it: WAL PUTs are put-if-absent, and the FaultStore parks the PUT before it dispatches it (`fault_store.rs:237-243`).
- I found no interleaving that answers a stranded group twice or reports success for a record that is not durable.

The corrections below cover the gate, how completely F2 is closed, boundedness, and the ledgers.

### C1 (BLOCKING, gate): the new `duplicate_order` tail grows its exception contract

§4.1.5 claims `syntax_facts` 22 → 22. The ratchet reports otherwise:

```
accepted exception grew without an approved growth row:
('src/shard/retirement_tests.rs', 'crate::duplicate_order', 'function', 'clippy::fn_params_excessive_bools'): syntax_facts 209 -> 210
```

The arithmetic undercounts macros. `visit_macro` emits `macro` and `macro-tokens` (`tools/quality-syntax/src/scan.rs:278-283`), and syn's `visit::visit_macro` then visits the macro path, which emits one more `path` fact. So every `assert!` is 3 facts, not 2. The added `assert!(matches!(first.try_recv(), …))` is therefore net +1.

**Fix (measured: `GROWTH: []`, `scope_lines` 96 → 95, `syntax_facts` 209 → 207):**
- Do not add the new `assert!(matches!(…Empty))` line.
- Instead, move the existing precondition `assert!(first.try_recv().is_err() && retry.try_recv().is_err() && config.try_recv().is_err());` (`retirement_tests.rs:256`) to just after `if retire { engine.begin_close(); }` (line 257-259).

It then asserts the new property in every mode:
- after the fix, nothing is answered while the WAL is held;
- in the retire + staged mode, `retry` and `config` are still paused at the NoWrite checkpoint;
- in the attached mode, they travel with `first`'s stranded group.

On HEAD, `begin_close` answers `first` with `Moved` synchronously. The red output in §3.3 and §7.1 therefore becomes:

```
assertion failed: first.try_recv().is_err() && retry.try_recv().is_err() && config.try_recv().is_err()
```

Everything else in the §4.1.5 tail stays as written. The `ShardEngine::begin_close` claims in §4.1.1 and §7.5 are confirmed:
- `let_underscore_must_use`: `scope_lines` 44 → 40, `syntax_facts` 80 → 67;
- the `unwrap_used` contract vanishes, and no other `unwrap_used` contract appears in `src/shard*`;
- `src/shard.rs` goes from 3,139 to 3,133 lines.

One detail is wrong: `nested_items` is 1 → 1, not 0.

### C2 (MAJOR, the closure claim): Option A closes only the harness's sub-window of F2; the same mechanism stays reachable in routine traffic

F2 is "a group the engine already WROTE is answered 503 `shard_moving`, yet becomes durable". `publish.rs:24-31` is that same mechanism for a group whose `db.write` returns after retirement. The plan leaves that path untouched and calls it a residual (§8, D2) that is "reachable when the write is blocked in slatedb backpressure".

That understates it:
- `CommitTransaction::run` checks `is_closed()` only once, before staging (`transaction/mod.rs:52-56`).
- After that check come `billing_rows(...).await` (a store read, `mod.rs:58`), prepare/encode, and `db.write_with_options(...).await` (`finalize.rs:178-183`).
- The retiring close joins the committer before it flushes (`WORKER_GRACE` 5 s), so the late group's rows reach the memtable the close then flushes.
- On a busy engine, the committer is inside that window for much of the time. A retirement under load (the capacity rig, a fleet eviction during traffic) will often strand a late write. That write is answered `Moved`, becomes durable, and the SDK's automatic 503 retry duplicates a plain `append()` (§1.7).

The tree already pins this answer as correct. `r17b_late_successful_write_settles_without_publishing_retired_effects` (`retirement_tests.rs:357-…`) asserts `assert_moved(result…)` and then that the replacement recovers `next == 1`. That is F2's shape, kept green.

Required:
- **(a)** State in §0 and §9 that Option A does not close F2's mechanism. It closes the "written before retirement" sub-window only.
- **(b)** Make D2 part of the release-hold closure decision, not a later item. The owner position requires a causal fix plus a red, or an approved documented outcome, for the mechanism.
- **(c)** Add the late-write row to the harness as the red or known row for D2. `CompletionPhase::Written` gives an existing, deterministic hold.
- **(d)** If D2(b) is chosen, one shape keeps R17-B's "no retired mirrors" rule:
  - When `publication()` is `None`, publish hands the finished `InFlightGroup` (replies only) to a retained slot on `CommitHandoff`. For example, `retire_late(group)` pushes onto a `stranded` vector that `retire()` created.
  - The finalizer takes that slot under `in_flight` after the join and settles it with the same `durable_seq`.
  - This adds a call and paths inside `CommitTransaction::publish`'s `unwrap_used` scope (`publish.rs:4-7`). That is fingerprint growth, so it needs restructuring, e.g. a `let Some(pending) = … else { return self.strand(handoff, sequence) }` moved into a new method outside the scope. The alternative is an owner row.
  - The Loom model would then add a publish thread. It must replace one of the three spawned threads, because `max_threads = 4` includes the main thread.

### C3 (MAJOR, boundedness): replies that were prompt now wait for the storage close, and several awaiters have no deadline

§2.2 ("Timing") and D4 argue the wait is bounded by the client's 10 s `APPEND_TIMEOUT`. That holds for `submit.rs:86-92` and `creation/initialization.rs:152`. It does not hold for these engine-reply awaiters, which call `rx.await` with no timeout:
- `application/topology.rs:112`, `close_segment_on_engine`: the split and seal coordinator, and the fleet-internal `/v1/internal/segment-close` handler `http.rs:3039`;
- `application/lifecycle.rs:888`, the seal fence;
- `shard.rs:2308`, `submit_queue`: every consumer queue op, from `consumer.rs:401`, `:681`, `consumer/deletion.rs:359`, and `consumer/delivery.rs:120`, `:487`, `:719`.

Today a retirement answers a stranded close, fence or queue op at once. After the change they wait for `close_db`, which never returns while the store hangs; slatedb retries store faults indefinitely.

A live engine with a stuck WAL already makes these callers wait. So this is a new occasion, not a new class of wait, but the plan must say so. Required:
- **(a)** List these awaiters in §1.5, §2.2 and the §6.5 edge record.
- **(b)** Decide D4 with them in view. The bounded settle is:
  - race `close_db` against a deadline;
  - at the deadline, settle by `db.status().durable_seq` as it stands then (still valid proof), answering the rest `Moved`;
  - keep closing.

  It gives every caller a bounded answer at the price of D4's stated cost.

### C4 (edge record accuracy): consumer queue ops are a 500, not a "retry" string

`AppendErr::Moved` for a queue op becomes the string `"shard fenced/moved; retry"` (`transaction/mod.rs:122`, `commit_plan.rs:69`). `consumer.rs:401-403` maps every `submit_queue` error to `FailureClass::Internal`, "internal", retryable:true. `product.rs:2956` renders that as **500**.
- So §2.3's row "Queue ops in that group" is before **500 `internal` (retryable:true)**, after 200 with the staged result.
- Queued queue ops at any retirement (`shard.rs:2460`, `transaction/mod.rs:52-56`) still answer 500.

That is a "500 at retirement" on the consumer surface, in the same family as D6. Name it for the owner as its own release-safety item. It is outside this plan's scope, but it should not go unmentioned.

### C5 (causal trace, §1.2 step 4 and §2.2): durability in the close comes first from the WAL flush

`Db::close_with_options` marks the status closed (Clean) and then calls `flush_memtables` (`db.rs:697-699, 706-714`). `flush_memtables` first calls `request_batch_writer_flush(true)`, which flushes every WAL (`db.rs:413-416`). The durable advance for the held group therefore normally comes from the WAL flush callback (`db.rs:2174` → `oracle.advance_durable_seq`). It only later comes from `finish_ready_batch` (`memtable_flusher/manifest_writer.rs:~722-737`), where §1.2 and §2.2 place it.

The conclusion is unchanged. Fix the trace and the "same proof" argument: both the WAL path and the manifest path are fenced.

### C6 (harness pin robustness, §4.1.7)

The entered-proof `engaged >= 1 && oldest_inflight_ms() >= 50` proves that one group is written, the oldest one. With the new `refused.is_empty()` assertion:
- if the second shot's group publishes after `retire_parent`, it takes the C2 path (`publish.rs:30`): 503, copies 1, and the bare retry leaves 2 copies, which is a violation;
- if it resolves after the retirement, it gets 503 with copies 0, and `refused` is non-empty.

Both are flaky failures unrelated to the fix. Add `parent.appends_enqueued() >= base + 2`, with `base` read before firing as in `appends_while_the_parent_seal_awaits_durability` (`sbo.rs:604-612`), ahead of the 50 ms check. Also say in the doc comment that the residual C2 window is why both shots must be written first.

### C7 (red-first mechanics, §4 Commit 1 and §7.1)

"Apply §4.1.5 and §4.1.6 (tests only)" names the wrong section: §4.1.6 is `mutation_owners.py`. The Loom test (§4.1.4) cannot be red-first either, because it does not compile without `settle_stranded`. Instead:
- red-first applies §4.1.5 (with C1) and the §4.1.7 harness edit;
- the Loom test lands with the source;
- the commit message says so.

In §3.1, `ack.last_offset == 0` is unverified; only `next_offset == 1` and `duplicate == false` are pinned anywhere today (`retirement_tests.rs:329`). Assert `(ack.next_offset, ack.duplicate)`, or confirm `last_offset` for a single-entry append before committing.

### C8 (retention and accounting of settled groups)

The stranded `InFlightGroup`s now live until the close finishes. That includes:
- `effects.ring_pub` (record bytes), `tails` (`Arc<StreamHandle>`), `touches` and `usage`, not only the replies;
- groups settled as success never publish `usage.plaintext_bytes`/`frame_bytes` (`shard.rs:3020-3024`; read by `usage.rs:613`).

Either:
- strip the group to `(seq, acks, queue_acks)` in `begin_close` before handing it over. That shrinks what a hung close pins, and `settle_stranded` then takes a smaller type; or
- state the bound (pending groups are capped by slatedb's unflushed-WAL backpressure) and record the unpublished usage explicitly in the retirement-handoff paragraph and edge record #53.

The canonical billing rows are in the batch and are recovered by the replacement, as the late-write test shows (`retirement_tests.rs:497-520`).

### C9 (Loom adequacy, §3.4)

The model uses the real `retire`, `take_durable` and `settle_stranded`, and kills the `<=` operator mutants. Its assertions are identical in both interleavings, though, so it would pass with the dispatch thread deleted. The real concurrency (dispatcher vs retirement) is already covered by `quality_loom_publication_and_durable_claim_have_one_terminal_owner`.

Two acceptable fixes:
- Say that the new transition is a by-value hand-off whose ordering relies on the supervisor join (`tasks/shutdown.rs:67-71`), which Loom cannot model; the model pins the partition by durability.
- Replace the dispatcher with an `attach` thread whose no-write reply must follow the last pending group. That checks that a group attached after the dispatcher claimed group 1 is answered `Moved` with group 3.

### C10 (ledger details)

- **review-mechanisms.json.** The key in `tests`/`support_functions` is `sha256`, not `function_sha256` (`review-evidence.py:182, 191`). The value is still `inventory.functions(...)['function_sha256']`. The `duplicate_order` sha changes; the three R17-B wrapper shas do not.
- **edge-changes.md.**
  - The count line (`edge-changes.md:22`) also changes: "53 records in total; 52 matched their commit and 1 is flagged".
  - Record number #53 collides with the sibling plan `plans19/split-producer-lineage.md` (its D1 and §569 also claim #53). Number the record at landing time.
- **§1.8.** `src/product.rs` is 4,138 lines, not 4,205. It is untouched either way.
- **§6.2.** "Exactly one entry changes" is correct: `test-inventory.json` covers only `src/dst` tests.
- **mutation_owners.py.** The new `commit_handoff_loom_tests` row is needed. `loom_tests.rs` has no `#![cfg(test)]`, and `production_changes.py:66` only honours an inner `cfg(test)`. The existing `test_mutation_owners.py:103` only checks name uniqueness.

### C11 (no change needed; confirmations)

- **Use sites.** The `AppendErr::Moved` production sites in §1.3 are complete. The `begin_close` callers in §1.4 are complete for production; `sharddir/unwind.rs:203,232` are inside its test module.
- **Quotes.** `contract.rs:299-304`, `product.rs:2353`, `publish.rs:24-31`, `finalize.rs:58, 196` and `transaction/mod.rs:187` all match.
- **SDK.** The §1.7 claim holds: `req()` rethrows transport errors at `sdk/src/index.ts:555-558`, before `retryableRequestError` is consulted.
- **§3.2 control.** It is sound: the FaultStore parks before dispatch, so the new owner's fence WAL takes the id first.

### C12 (unchanged tests the plan should list in §7.3)

`r17a_fenced_final_flush_releases_only_after_the_owned_close_joins` and the `held_wal()` tests drop or pre-await their receivers, so they are unaffected. `a_handoff_with_an_append_in_flight_resolves_safely` is also unaffected: its stranded group is fenced, so it stays `Moved`. Name them as the I4 regression set.

### Verdict

**Ready with corrections.**
- C1 must be applied or the gate fails.
- C2 and C3 must be put to the owner before anyone claims that this closes the F2 release hold. They change how completely the mechanism is closed and whether answers are bounded, not the soundness of Option A.
- C4 to C10 are accuracy and ledger fixes.
