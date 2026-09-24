# Item 83, step 1: one supervised touch flusher per runtime

Plan only. Read-only analysis of `slate` @ `c397e9e5`. At plan time the local
`origin/slate` tracking ref also reads `c397e9e5`; the task statement says
`5d9d517f` (8 commits behind). Of those 8, only `ed9238da` touches files this plan
edits: `src/bootstrap.rs`, `src/dst/tests/fixture_http.rs` and
`docs/refactor/review-mechanisms.json`. Build on HEAD. Land this after the
mutation leg that is running now has finished. Do not run cargo while it runs.

Scope: step 1 only. The per-journal task goes. One supervised flusher drains a
queue of dirty journals. `TouchJournal::start` becomes a plain constructor. The
Loom model for register/close stays. The reviewer also asked for a
per-journal budget doc fix. It is a one-line, doc-only change, planned as C1.
Step 2 (retiring idle journals) is out of scope.

---

## 1. Problem (verified on c397e9e5)

### 1.1 The per-journal raw spawn is real

`src/touch.rs:77-106`:

```rust
    #[expect(
        clippy::disallowed_methods,
        reason = "TouchJournal::start; the per-stream flusher is started under the journal map lock, where no supervisor is reachable, and it exits on its own once the journal closes; ..."
    )]
    #[expect(
        clippy::excessive_nesting,
        reason = "TouchJournal::start; the flusher nests the close verdict inside the tick loop of the spawned flusher; ..."
    )]
    pub(crate) fn start(entropy: &dyn crate::runtime::Entropy) -> Arc<TouchJournal> {
        ...
        let flusher = journal.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(BUCKET_MS));   // BUCKET_MS = 25 -> 40 Hz
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut ticks = 0u64;
            loop {
                tick.tick().await;
                ticks += 1;
                if flusher.flush_bucket(ticks.is_multiple_of(40)) {
                    return;
                }
            }
        });
```

- **Exits only on close.** `flush_bucket` (`touch.rs:142-213`) returns `true` only
  from `if inner.closed { return true; }`. In production `closed` is set only by
  `TouchJournal::close`, and the only production caller of that is
  `TouchRegistry::close_shard` (`touch.rs:414-426`). `close_shard` is reached
  only through the engine's `on_close` callback at `src/bootstrap.rs:503-510`:
  `touch.close_shard(&prefix); notifier.closed(&prefix, incarnation);`.
- **The task holds the journal.** The task keeps an `Arc<TouchJournal>`
  (`flusher`), so the journal cannot be freed while its task runs.
- **Idle, deleted and recreated streams keep everything.** The map key is the
  storage hash, which carries the epoch (`watch.rs:144`, `watch.rs:333`:
  `self.touch.journal(descriptor.storage_hash(), ...)`). A recreated stream
  therefore gets a new key, and the old one stays. `map.remove` appears only in
  `close_shard`. An idle, deleted or recreated stream keeps its task, its map
  entry and its history until its shard closes.
- **The supervisor cannot see these tasks.** They come from a raw
  `tokio::spawn`, allowed by the `disallowed_methods` expectation above. They
  also have an effect row in `docs/quality/owners.json:1907-1914` and one in
  `docs/quality/source-allowances.json:1508-1514`. Supervisor shutdown neither
  cancels nor joins them. In DST, a rig restart leaves the old rig's flushers
  running until the test runtime drops.

### 1.2 The reviewer's Change drops the waiter sweep

The reviewer's Change says nothing about the sweep that the per-journal loop
runs, and that sweep matters. `flush_bucket(reap = true)` runs on every 40th
tick (once a second) whether the journal is dirty or not. It removes waiters
whose receiver is gone (`touch.rs:147-157`, `w.tx.is_closed()`). `wait` never
removes its own waiter when it times out or is cancelled. The timeout arm,
`touch.rs:263-269`, only reads `generation`/`end_offset`:

```rust
            _ => {
                let inner = self.inner.lock().unwrap();
                WaitOutcome::Timeout { cursor: ..., end_offset: inner.end_offset }
            }
```

A flusher that visits only dirty journals would therefore leak one `Waiter`
plus its `key_index` entries per timed-out or cancelled long-poll on an idle
journal. Clients re-poll at most every 25 s (`watch.rs:340`), so the leak would
grow without bound. The plan keeps the sweep: once a second, over every
registered journal (§2).

### 1.3 "Bounded" must not mean a capacity limit

A capacity bound would be wrong here. If a fixed-capacity queue refused a push
on the false->true edge, that journal would stay dirty forever: later ingests
see `dirty == true` and never push again, so its waiters would never wake. The
bound comes from the edge rule instead (§2.4).

### 1.4 The per-journal budget doc is wrong (C1)

`touch.rs:16`: `/// Global cap on retained history keys (~8 MB as sorted u32 vecs).`
Enforcement is per journal: `inner.history_keys` is a field of `Inner`, and
`touch.rs:204` checks `inner.history_keys > HISTORY_KEY_BUDGET` for that one
journal. Nothing bounds the total across journals (see D2).

### 1.5 Complete use sites

`git grep` over `src`, `tools`, `fuzz`, `bench`, `benches`, `scripts`,
`.github` and `tests`. There are no uses in `tools/`, `fuzz/`, `bench/`,
`tests/` or `.github/`.

**Production**
- `src/touch.rs`:
  - `TouchJournal::start` 77-106
  - `ingest` 113-136
  - `flush_bucket` 138-213
  - `close` 216-222
  - `wait` 226-271
  - `Inner::register`/`close`/`catch_up` 279-346
  - `TouchRegistry` 376-427: `journal` 391-405 calls `start` at 402; `close_shard` 410-426
- `src/application/watch.rs`: field 87/103; `append_touch` 144 creates a journal for every append to a stream with watch definitions; `observe` 333 plus `WaitOutcome` matching 354-367.
- `src/http.rs:220` `AppState.touch`; `:295-300` `WatchService::new(.., self.touch.clone(), ..)`. Unchanged.
- `src/bootstrap.rs`:
  - 370-372 builds the registry
  - 503-510 `on_close` calls `close_shard`
  - 660 `touch,` into `AppState`
  - 706-710 the request-maintenance start (placement anchor for this plan)
- `src/shard.rs:725` `TouchFeed.journal`; `:3036-3039` acker `t.journal.ingest(&t.key_ids, t.next_offset)`; `:1291` doc. Unchanged: `ingest(self: &Arc<Self>)` autorefs from `t.journal: Arc<TouchJournal>`.
- `src/shard/transaction/append.rs:242` fills `next_offset`. `src/application/append.rs:378` passes `touch`. Both unchanged.

**Tests and fixtures**
- `src/touch.rs` tests: `TouchJournal::start` at 436, 451, 469, 485, 503, 531; struct literal at 549-552; `flush_bucket(false)` at 441, 455, 505, 507, 510, 512, 535, 561.
- `src/touch/loom_tests.rs`: `Inner::register`/`Inner::close` model. Stays.
- `src/shard/transaction_tests.rs`: 16, 43 (`TouchJournal::start`), 80, 156, 400-405 (waits for `Touched{end_offset:1}`, so it needs a running flusher), 426/457/534 `close`.
- `src/shard/retirement_tests.rs`: 66, 105 (`TouchJournal::start`), 136, 364-375 (waits for `Touched`, needs a flusher), 494-504.
- `src/dst/tests/fixture_http.rs`: 399 registry, 450-451 supervisor plus `request_work.start`, 518 into `AppState`. This is the only other `TouchRegistry` constructor besides bootstrap.
- `src/dst/tests/runtime_isolation.rs:206` reads the journal epoch. Entropy draw order is unchanged.
- `src/dst/tests/watch_observation.rs`: 110/184 `close_shard`. All 8 of its tests, plus `watch_admission`, rely on flushes.

**Ledgers and tooling**
- `docs/quality/owners.json:1907-1914` and `docs/quality/source-allowances.json:1508-1514`: effect rows for `crate::TouchJournal::start`/`tokio::spawn`.
- `docs/quality/legacy-source.json:2062-2068` and `legacy-diagnostics*.json`: immutable adoption inventories. Do not touch.
- `scripts/quality/mutation_owners.py:66` `owner('touch', 'src/touch.rs', 'touch::')`; `scripts/quality/verification_plan.py:27` exact prefix `'src/touch.rs'`.

---

## 2. Contract decision

### 2.1 Owner: one flusher per runtime, owned by the `TouchRegistry`

Not per engine and not per process:
- Journals are owned by the registry and keyed by storage hash. The engine
  that feeds a stream changes across incarnations, and the engine never learns
  which journals it feeds.
- The sweep needs the registry map.
- The registry is the one owner that bootstrap (`state.touch`) and each DST rig
  can reach.
- A process-global flusher would break per-runtime isolation. Two rigs in one
  test process must not share delivery.

`TouchRegistry::start(self: &Arc<Self>, &TaskSupervisor)` has the same shape as
`RequestWork::start`. It spawns `"touch-flusher"` with `Policy::Critical`
(D1). Every other service loop is Critical (request-maintenance, scaler,
auth-refresher, telemetry, fleet, rss-sampler), and `Policy::Noncritical` is
documented as "observability or hygiene only". Losing the flusher means losing
watch delivery.

Cancellation is read at each tick boundary: `while !cancel.is_cancelled()`.
That is at most 25 ms of latency, well inside any grace. This avoids a
`tokio::select!`, which would be a new `macro-dsl` occurrence needing an
owners row.

### 2.2 Latency and ordering

These are pinned by T2, T3, T4 and T6, and each pinning test is green on
c397e9e5 as well.
- **Publication.** A touch publishes on the first flusher tick after its
  ingest: at most `BUCKET_MS` = 25 ms, plus that tick's serial flush work.
  Before, each journal ticked on its own phase. Now all dirty journals share
  one phase. No contract depended on the phase.
- **Buckets per journal are unchanged.** Keys ingested between two ticks share
  one generation. Generations are per journal. An idle journal never publishes
  an empty bucket (`seal` returns `None` when clean).
- **Sweep.** Waiters that callers abandoned are reaped once a second (every
  `SWEEP_TICKS = 40` ticks) over every journal in the map. This is the same
  cadence as today's per-journal reap and the same total work: O(journals) per
  second. Between sweeps the flusher visits only queued (dirty) journals. That
  removes the cost item 83 is about: an idle journal no longer causes 40
  wakeups a second or owns a task.
- **Close is unchanged.** `close_shard` and bootstrap's `on_close` stay as they
  are. A journal that was queued and then closed publishes nothing, because
  `flush_bucket` still returns early on `closed` (T6). The flusher no longer
  exits on a close. Only cancellation stops it.
- **Shutdown.** Supervisor shutdown cancels and joins the flusher. Ingests
  after that still queue, but nothing publishes them. The accept loop has
  stopped by then. Before this change, per-journal tasks ran past shutdown
  until the runtime dropped.

### 2.3 Lock order: no nesting

- `ingest` releases the journal lock before it takes the queue lock.
- The drain takes the queue lock (one `mem::take`), releases it, then locks
  journals one at a time.
- The sweep snapshots `Arc`s under the map lock, releases it, then locks each
  journal.
- The existing map-then-journal order in `close_shard` is untouched.

### 2.4 Queue invariant

`DirtyJournals(Mutex<Vec<Arc<TouchJournal>>>)`:
- **Enter.** A journal enters only on its clean-to-dirty edge. `Inner::ingest`
  returns `true` for exactly that ingest.
- **Leave.** It leaves when the drain takes it, and only `seal` clears `dirty`.
  The sweep reaps but never seals, so the sweep cannot re-arm the edge while
  the journal is still queued.
- **Result.** A journal waits in the queue at most once. Queue length is at
  most the number of live journals. There is no capacity refusal (§1.3).
- **Pinned by** T7 (deterministic, through the real `TouchJournal::ingest`
  glue) and by the new Loom model.

### 2.5 Ownership edges

- The queue holds a strong `Arc<TouchJournal>` until it publishes. A queued
  journal must stay flushable.
- The journal holds a `Weak<DirtyJournals>` back to the queue. There is no
  journal -> queue -> journal cycle.
- A journal built without a registry (`Weak::new()`, as in the touch unit
  fixtures) never queues. Its buckets publish only when a test calls
  `flush_bucket`.

### 2.6 Boot

```rust
state.touch.start(&tasks).expect("fresh runtime accepts the touch flusher");
```

This goes directly after the maintenance-worker start (`bootstrap.rs:706-710`).
It uses the same form and the same function-wide `expect_used` decision,
re-decided to cover two sites. The expect can never fire: the supervisor is
fresh and cannot be stopping, because the signal task that could stop it is
spawned later. So this is not a new boot refusal. Using `expect` rather than
`?` also avoids adding a `?` after a loop has started (PR 6.1-A).

### 2.7 Visible surfaces

- `/v1/debug` `tasks.loops` gains `{"name":"touch-flusher","policy":"Critical","state":"Running"}`. This is one more element in an existing array; the shape is unchanged.
- Readiness would report `critical task terminated: touch-flusher` only if the flusher ever exits, which takes a panic from a poisoned lock.
- These are the only edge-visible effects. They are flagged as D1.

---

## 3. Red tests, pinning tests, non-vacuity controls

New file `src/touch/flusher_tests.rs` (`#![cfg(test)]`), declared with
`#[cfg(test)] mod flusher_tests;` beside `mod loom_tests;` in `src/touch.rs`.
Every name is under `touch::`, so the mutation owner's filter selects them.
There is no standalone `tokio::join!`/`tokio::pin!`, which would need a
macro-dsl row. `futures_util::poll!` is used only inside `assert!`, as the
existing touch tests do.

Helper:

```rust
fn started_registry(tasks: &TaskSupervisor) -> Arc<TouchRegistry> {
    let registry = Arc::new(TouchRegistry::with_entropy(Arc::new(OsEntropy)));
    registry.start(tasks).unwrap();   // absent in the c397e9e5 staging (journals self-flush there)
    registry
}
```

### 3.1 Red procedure (uncommitted staging on c397e9e5)

1. Add `flusher_tests.rs` with T1, T2, T3, T4 and T6. Use the helper without
   its `start` line. Add the `mod` line to `touch.rs`.
2. Run `cargo test --locked --lib touch::flusher_tests`.
3. Expected: `4 passed; 1 failed`.

**T1 `journals_spawn_no_task_of_their_own`** is RED. This is the reviewer's first step.

```rust
#[tokio::test(start_paused = true)]
async fn journals_spawn_no_task_of_their_own() {
    let registry = TouchRegistry::with_entropy(Arc::new(OsEntropy));
    for n in 0..3u8 { registry.journal([n; 16], RouteHash([n; 16])); }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(tokio::runtime::Handle::current().metrics().num_alive_tasks(), 0,
        "a journal must not own a task");
}
```

Trace on c397e9e5:
1. `journal` (`touch.rs:402`) runs `or_insert_with` -> `TouchJournal::start` -> `tokio::spawn` (`:93`) once for each of the 3 distinct hashes.
2. Each loop leaves only when `flush_bucket` returns `true`, which needs `closed`. Nothing closes these journals, and the map keeps them alive.
3. The paused second advances 40 ticks per task, and all three stay alive. `num_alive_tasks` is a stable API in tokio 1.52.3.

Expected red output:

```
---- touch::flusher_tests::journals_spawn_no_task_of_their_own stdout ----
thread 'touch::flusher_tests::journals_spawn_no_task_of_their_own' panicked at src/touch/flusher_tests.rs:<line>:5:
assertion `left == right` failed: a journal must not own a task
  left: 3
 right: 0
```

After C2 it is green: no task is spawned, and the registry is never started in
this test.

**Pinning tests: green on c397e9e5 staging and after C2.**
- **T2 `one_flusher_wakes_every_journals_waiters_within_a_tick`**
  1. Start the flusher. Create journals `a` and `b`.
  2. `sleep(40ms)`, which lands off the 25 ms grid (ticks at 0 and 25).
  3. Register `a.wait("now",[7],1s)` and `b.wait("now",[11],1s)`, each polled once inside `assert!`.
  4. `ingested = Instant::now()`, then `a.ingest(&[7],3); b.ingest(&[11],5);` and await both.
  5. Assert both outcomes are `Touched{proven:true, end_offset:3|5}`, with message `"each journal's touch must wake its own waiter"`.
  6. Assert `ingested.elapsed() <= 25ms`, with message `"published {waited:?} after ingest"`. The tick at 50 gives 10 ms.
  - Old tree: the per-journal intervals start at the first poll (t0), which gives the same ticks.
- **T3 `each_journal_publishes_one_bucket_per_tick_that_touched_it`**
  1. `a.ingest(&[11],1); a.ingest(&[7],2); b.ingest(&[7],1);`
  2. `sleep(10ms)`. The tick at t0 publishes.
  3. `a.ingest(&[13],3);`
  4. `sleep(30ms)`. The tick at t0+25 publishes.
  5. `buckets(&a) == [(1, vec![7, 11]), (2, vec![13])]` (message `"journal a"`) and `buckets(&b) == [(1, vec![7])]` (message `"journal b: no empty bucket while idle"`). `buckets` maps `inner.history` to `(generation, keys)`.
  - The sleeps avoid tick ties on purpose.
- **T4 `idle_journals_are_visited_only_by_the_once_a_second_sweep`**
  1. Start the flusher and record `started`. Create a journal.
  2. Pin a `wait("now",[7],60s)`, poll it once, then drop it. The waiter is now dead but still registered.
  3. `sleep_until(started+960ms)` and assert `waiters.len() == 1`, message `"an idle journal is not visited between sweeps"`. Tick 39 runs at 950 ms, tick 40 at 975 ms.
  4. `sleep_until(started+1000ms)` and assert `== 0`, message `"the sweep drops a waiter whose caller went away"`.
  - Old tree: the per-journal reap runs at its own tick 40 (975 ms). Same result.
- **T6 `a_closed_journal_publishes_nothing_it_had_queued`**
  1. Ingest, then `close()`.
  2. `sleep(30ms)`.
  3. Assert `(generation, history.len()) == (0, 0)`, message `"a fence outranks a pending bucket"`.
  - Old tree: the flusher sees `closed`, returns `true` and exits.

### 3.2 New-structure tests (added in C2)

These do not compile on c397e9e5 because they use new API.

- **T5 `the_flusher_is_one_supervised_critical_loop_that_stops_on_shutdown`**
  - Start the registry with 3 journals.
  - `tasks.monitor().snapshot() == [TaskStatus{name:"touch-flusher", policy:Policy::Critical, state:TaskState::Running}]`, message `"the registry's one flusher is a supervised critical loop"`.
  - `tasks.shutdown(1s)` yields `report.outcomes == [("touch-flusher", TaskOutcome::Finished)]` and `report.aborted.is_empty()`.
  - Red on c397e9e5: `error[E0599]: no method named `start` found for struct `Arc<TouchRegistry>``.
  - Behavioural red on a staged skeleton (the C2 tree with the body of `TouchRegistry::start` replaced by `Ok(())`): `left: []` / `right: [TaskStatus { name: "touch-flusher", policy: Critical, state: Running }]`.
- **T7 `a_dirty_journal_waits_in_the_queue_once_however_often_it_is_touched`**
  - Plain `#[test]`, registry not started.
  - `a.ingest` three times, then `b.ingest` once.
  - The queue (`registry.dirty.0`) holds exactly `[a, b]`, checked with `Arc::ptr_eq`. Message: `"each dirty journal waits once, in the order it was dirtied"`.
  - `registry.flush_tick(false)` leaves the queue empty.
  - `a.ingest` again gives `len() == 1`, message `"a published journal is queued again by its next touch"`.
  - Red on c397e9e5: `error[E0609]: no field `dirty` on type `TouchRegistry``.
- **Loom `quality_loom_an_ingest_racing_a_flush_is_published_or_left_queued_once`**
  - In `src/touch/loom_tests.rs`, using the same bounds as the existing model: `max_threads 3`, `max_branches 1000`, `preemption_bound Some(2)`, no duration or permutation cutoff.
  - The model drives the real `Inner::ingest` and `Inner::seal` under `loom::sync::Mutex`. A `loom::sync::Mutex<usize>` stands in for the one journal's queue entries. The two-line glue mirrors `TouchJournal::ingest` (lock, ingest, release, then push only if `dirtied`) and the drain (take the queue, then seal once per entry).
  - Acker thread: ingests `(7,1)` then `(11,2)`.
  - Flusher thread: one drain.
  - After join, one more drain stands in for the next tick. Then assert:
    - `taken <= 1`: `"a journal waits in the flush queue at most once"`
    - `!dirty`: `"a dirty bucket always has a queued flush"`
    - sealed keys `== [7, 11]`: `"every ingested key publishes exactly once"`
    - sealed generations `== 1..=generation`
  - Red on c397e9e5: E0599, no method `ingest`/`seal` on `MutexGuard<'_, touch::Inner>`.
  - The existing `quality_loom_retirement_cannot_leave_a_late_registered_waiter` stays unchanged.

### 3.3 Non-vacuity controls

Apply each after C2, observe the red, then revert.

| # | Edit | Expected red |
|---|---|---|
| NV1 | `Inner::ingest`: `let dirtied = !self.dirty;` -> `false` | T2: `each journal's touch must wake its own waiter` (after a 1 s virtual timeout). Loom: `a dirty bucket always has a queued flush`. |
| NV2 | `flush_until_cancelled`: `ticks.is_multiple_of(SWEEP_TICKS)` -> `true` | T4: `an idle journal is not visited between sweeps`, `left: 0`, `right: 1` |
| NV3 | `flush_tick`: delete `journal.reap();` | T4: `the sweep drops a waiter whose caller went away`, `left: 1`, `right: 0` |
| NV4 | `Inner::seal`: delete `self.dirty = false;` | T3: `journal a`, left `[(1, [7, 11])]` vs right `[(1, [7, 11]), (2, [13])]` |
| NV5 | `Inner::ingest`: `let dirtied = true;` | T7: `each dirty journal waits once, ...` (3 entries vs 2). Loom: `a journal waits in the flush queue at most once`. |
| NV6 | `fixture_http::http_rig_build`: delete `touch.start(&tasks).unwrap();` | `dst_tests::watch_observation::touch_close_shard_matches_route_hash_not_storage_hash` panics on `assert_eq!(v["invalidated"], true, "{v}")` after the 8 s long-poll (invalidated:false) |
| NV7 | `transaction_tests::Fixture::new`: delete `touch.start(&touch_tasks).unwrap();` | `r03a_mixed_transaction_preserves_every_row_reply_and_publication` fails `assert!(matches!(.., WaitOutcome::Touched { end_offset: 1, .. }))` after 1 s |
| NV8 | `TouchRegistry::start` body -> `Ok(())` | T5 `left: []` (T2/T3/T4 also fail) |

---

## 4. Edits, file by file, in commit order

None of the edited files is over 1,000 lines, and none crosses 1,000.
- `touch.rs`: 583 -> about 700
- `bootstrap.rs`: 915 -> 920. The architecture function budget for `run` is 836 (baseline); it is 773 -> 778 lines.
- `fixture_http.rs`: 793 -> 794
- `transaction_tests.rs`: 541 -> about 548
- `retirement_tests.rs`: 562 -> about 569

The ceilinged files (`http.rs`, `shard.rs`, `product.rs` and the rest) are not
touched.

### C1: "The history key budget is per journal, and the doc says so" (doc only; can be folded into C2)

- `src/touch.rs:16`: replace the doc with:
  `/// Caps one journal's retained history keys (~8 MB as sorted u32 vecs); every journal keeps its own, so the process total grows with the journal count.`
- This is a const doc only: no mutants, and the planner reports "no executable mutants" for `touch`.

### C2: "One supervised flusher publishes every touch journal; a journal is state, not a task"

**`src/touch.rs`**

1. **Module doc.** Add one sentence: one supervised flusher per runtime publishes every journal's buckets. A journal owns no task, so an idle one costs nothing between the once-a-second sweeps.
2. **Imports.** `use std::sync::{Arc, Mutex, Weak};`
3. **Constant.** `const SWEEP_TICKS: u64 = 40;` with the doc "reaping walks every journal's waiters; once a second keeps idle journals free between sweeps".
4. **`struct SealedBucket { generation: u64, keys: HashSet<u32>, overflow: bool, end_offset: u64 }`.** Doc: taken whole under the journal lock, so no ingest splits a bucket.
5. **`#[derive(Default)] struct DirtyJournals(Mutex<Vec<Arc<TouchJournal>>>)`.** Put the §2.4 invariant and the §1.3 capacity hazard in its doc. It has two methods, each with a new `#[expect(clippy::unwrap_used)]`:
   - `fn push(&self, journal: Arc<TouchJournal>)`, reason: `"DirtyJournals::push; the queue lock guards a single Vec push or take, so poison means that step panicked; recovering it could lose a queued journal and strand its touches unpublished"`
   - `fn take(&self) -> Vec<Arc<TouchJournal>>` (`std::mem::take(&mut *self.0.lock().unwrap())`), with the same reason under the `DirtyJournals::take` owner.
6. **`TouchJournal` fields.** Add `queue: Weak<DirtyJournals>`. Doc: the registry owns the queue, so a journal only points back to it, and one built without a registry never queues.
7. **`TouchJournal::start` -> private `fn new(entropy: &dyn Entropy, queue: Weak<DirtyJournals>) -> Arc<TouchJournal>`.** Same epoch draw (8 bytes, same order), no spawn. Delete both expectations (`disallowed_methods`, `excessive_nesting`) and the whole spawned loop.
8. **`TouchJournal::ingest(self: &Arc<Self>, key_ids, next_offset)`:**
   ```rust
   let dirtied = self.inner.lock().unwrap().ingest(key_ids, next_offset);
   // Queued after the journal lock is released: no path holds both locks.
   if dirtied && let Some(queue) = self.queue.upgrade() {
       queue.push(self.clone());
   }
   ```
   Re-decide the `unwrap_used` reason to `"TouchJournal::ingest; a poisoned journal may hold a half-ingested bucket; recovering it could queue or publish touches that were never recorded"`. New path facts `dirtied`, `queue` and `Some` grow its fingerprints.
9. **New `Inner::ingest(&mut self, key_ids, next_offset) -> bool`.** This is the body moved from `TouchJournal::ingest` with `inner.` -> `self.`. The `closed` check returns `false`. Add `let dirtied = !self.dirty;` before `self.dirty = true;`, and make the overflow early return and the tail return `dirtied`. Doc: only the ingest that dirties a clean bucket queues the journal, so it waits at most once.
10. **New `Inner::seal(&mut self) -> Option<SealedBucket>`.**
    - Moves `touch.rs:158-167`: `if !self.dirty { return None; }` `self.generation += 1;` `self.dirty = false;`.
    - Then returns `Some(SealedBucket{ generation: self.generation, keys: mem::take(&mut self.current), overflow: mem::take(&mut self.current_overflow), end_offset: self.current_end_offset })`.
    - Doc: runs under the lock `ingest` dirties under, so a racing ingest lands either in this bucket or in the next, which it queues.
11. **`flush_bucket(&self)`.**
    - Drop the `reap` parameter and its block (`:147-157`).
    - `if inner.closed { return; }`
    - Replace `:158-167` with `let Some(SealedBucket { generation, keys, overflow, end_offset }) = inner.seal() else { return; };` (rustfmt picks the layout).
    - Delete the trailing `false`.
    - The wake loop and the history eviction lines (`:169-211`) stay byte-identical, so they select no operator mutants.
    - Re-decide its `unwrap_used` reason to `"TouchJournal::flush_bucket; a poisoned journal may hold a half-sealed bucket; recovering it could publish or wake on touches that were never recorded"`. `Some` goes 2 -> 3 and `SealedBucket` is a new path key.
    - The statement-level `let_underscore_must_use` inside keeps its text. Its scope shrinks by about 13 lines and about 30 syntax facts. If the gate still reports growth, re-decide it too.
12. **New `fn reap(&self)`.** This is the moved reap block, with a new `#[expect(clippy::unwrap_used, reason = "TouchJournal::reap; a poisoned journal may hold a partially registered waiter; recovering it could keep a dead waiter indexed or drop a live one")]`. Doc: `wait` leaves timed-out and cancelled waiters registered, so idle journals must be swept.
13. **`TouchRegistry` fields.** Add `dirty: Arc<DirtyJournals>`. `with_entropy` initialises it with `Arc::default()`.
14. **`journal()`.** `.or_insert_with(|| (route, TouchJournal::new(&*self.entropy, Arc::downgrade(&self.dirty))))`. Re-decide the reason to `"TouchRegistry::journal; a poisoned journal map may hold a half-inserted or half-closed journal, and each journal it inserts points at this registry's flush queue; recovering it could revive a journal a fence already closed"`. `TouchJournal::new` and `Arc::downgrade` are new call and path keys.
15. **New `pub(crate) fn start(self: &Arc<Self>, tasks: &crate::tasks::TaskSupervisor) -> Result<(), crate::tasks::SpawnRejected>`.**
    ```rust
    let registry = self.clone();
    tasks.spawn("touch-flusher", crate::tasks::Policy::Critical, |cancel| async move {
        registry.flush_until_cancelled(cancel).await;
        crate::tasks::TaskResult::Done
    }).map(|_| ())
    ```
    Doc: supervised, so shutdown joins it and a panic surfaces as a critical exit rather than silently ending delivery.
16. **New `async fn flush_until_cancelled(&self, cancel: crate::tasks::Cancellation)`.** It uses the same `interval(BUCKET_MS)` with `MissedTickBehavior::Delay`, a `ticks: u64` counter, and `while !cancel.is_cancelled() { tick.tick().await; ticks += 1; self.flush_tick(ticks.is_multiple_of(SWEEP_TICKS)); }`. Doc: cancellation is read at tick boundaries, and a tick is 25 ms.
17. **New `fn flush_tick(&self, sweep: bool)`.**
    - `for journal in self.dirty.take() { journal.flush_bucket(); }`
    - `if sweep`: snapshot `self.map.lock().unwrap().values().map(|(_, j)| j.clone()).collect::<Vec<_>>()`, release, then `journal.reap()` for each.
    - New `unwrap_used` expectation: `"TouchRegistry::flush_tick; a poisoned journal map may hold a half-inserted or half-closed journal; recovering it could sweep a journal a fence already closed or skip one it kept"`.
    - Nesting 3, one bool parameter.
18. **Tests module.**
    - 6x `TouchJournal::start(&crate::runtime::OsEntropy)` -> `TouchJournal::new(&crate::runtime::OsEntropy, std::sync::Weak::new())`
    - 8x `flush_bucket(false)` -> `flush_bucket()`
    - The struct literal at `:549` gains `queue: std::sync::Weak::new(),`
    - Existing `journal.close()` calls stay; they are harmless.
    - Add `#[cfg(test)] mod flusher_tests;`.

**`src/touch/flusher_tests.rs`** (new, about 200 lines): the helper, T1 to T7 (§3). Imports: `super::{BUCKET_MS, TouchJournal, TouchRegistry, WaitOutcome}`, `crate::crypto::RouteHash`, `crate::runtime::OsEntropy`, `crate::tasks::{Policy, TaskOutcome, TaskState, TaskStatus, TaskSupervisor}`.

**`src/touch/loom_tests.rs`** (40 -> about 88): header doc becomes "registration, retirement and the dirty-edge handoff"; add the Loom model from §3.2. `use super::{Inner, ...}` is unchanged; the fields of `SealedBucket` are reached through values.

**`src/bootstrap.rs`**
- After `:706-710` add:
  ```rust
      // Watch delivery: the runtime's one touch flusher publishes every journal's buckets.
      state
          .touch
          .start(&tasks)
          .expect("fresh runtime accepts the touch flusher");
  ```
- `run` grows, so `scope_lines` and `syntax_facts` grow for all six of its function-wide expectations, and each is re-decided. Exactly two `;` each, no `"`:
  - `too_many_lines`: `"run; boot wires the stores, keys, runtime, caches and tasks (the touch flusher among them) in one visible dependency order; splitting it would hide which resource each later step relies on"`
  - `cast_possible_truncation`: `"run; the flush interval fits u64 milliseconds and the shared cache size fits usize on the 64-bit targets the service builds for, and the touch-flusher start converts nothing; checked conversions would only restate the target width"`
  - `let_underscore_must_use`: `"run; the probe delete is best effort, the supervisor rejects a spawn only while stopping, and the capability publication is advisory, while the touch-flusher start is expected rather than discarded; handled results would only restate what boot already logs"`
  - `expect_used`: `"run; covers exactly two sites, the maintenance-worker and touch-flusher starts, and none in the shard opener: the runtime's task supervisor is fresh at boot, so it accepts both workers; a fallible start would leave the process serving without maintenance or watch delivery"`
  - `unwrap_used`: `"run; covers exactly four sites, the shared-cache lock and the three auth file paths, and none in the shard opener or the touch-flusher start: a poisoned cache lock at boot would mean a half-built shared cache, and those paths were validated by the CLI parser before boot began; recovering the former or re-checking the latter would boot on state the parser already rejected"`
  - `excessive_nesting`: `"run; boot nests each shard opener's flush stagger, database open and close callback inside the opener closure it hands the directory, and the touch-flusher start adds no nesting; flattening them would separate the opener from the shard it builds"`

**`src/dst/tests/fixture_http.rs`**
- After `:451` (`rig_runtime.request_work.start(&tasks).unwrap();`) add `touch.start(&tasks).unwrap();`. It must come before `touch` moves into `AppState` at `:518`.
- Re-decide both expectations on `http_rig_build`:
  - `too_many_lines`: `"HTTP rig builder; every runtime owner, the touch flusher included, and the scenario's command-line edit are wired in one place so the fixture's dependency order stays visible to scenario authors; pass-through steps would hide which owner a scenario option changed"`
  - `let_underscore_must_use`: `"http_rig_build; the supervisor rejects a spawn only while it is stopping, when the rig is being torn down, whatever command line the scenario configured, and the touch flusher's start is unwrapped at build instead; a rejected rig task has nothing left to serve"`

**`src/shard/transaction_tests.rs`**
- `Fixture` gains `_touch: crate::tasks::TaskSupervisor`. The field is kept so the flusher's supervisor lives as long as the fixture; the leading `_` silences `dead_code`.
- `Fixture::new` replaces `:43` with:
  ```rust
  let touch_tasks = crate::tasks::TaskSupervisor::new();
  let touch = Arc::new(crate::touch::TouchRegistry::with_entropy(Arc::new(crate::runtime::OsEntropy)));
  touch.start(&touch_tasks).unwrap();
  let journal = touch.journal(HASH, crate::crypto::RouteHash([9; 16]));
  ```
- No test body changes, so the pinned `r03a_…` source-adaptation hash is untouched.

**`src/shard/retirement_tests.rs`**
- Same fixture change: struct `:62-68`, `new` `:69-107`, with `journal: touch.journal(HASH, RouteHash([9;16]))` and `_touch`.
- The pinned mechanism tests and support functions (`duplicate_order`, `assert_moved`, `completion_checkpoint`, `remote_missing`) are untouched.

**`docs/quality/owners.json`**: delete the `effect` row at `crate::TouchJournal::start`/`tokio::spawn` (`:1907-1914`).

**`docs/quality/source-allowances.json`**: delete the matching row (`:1508-1514`). Otherwise the gate reports `1 obsolete source allowances; run the quality ratchet with --prune`. Hand-edit the row or run `gate.py --prune`.

**`docs/refactor/review-mechanisms.json`**
- In `fixture_changes` for `http_rig_build` (`:927-932`), recompute `after_sha256`:
  ```
  python3 -c "import importlib.util,pathlib;s=importlib.util.spec_from_file_location('i','scripts/test-inventory.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m);print([f['function_sha256'] for f in m.functions(pathlib.Path('src/dst/tests/fixture_http.rs').read_text(),include_helpers=True) if f['name']=='http_rig_build'])"
  ```
- Append to `reason`: `" Item 83 starts the runtime's one touch flusher on the same supervisor, beside the request-maintenance worker, because journals no longer spawn their own; no value or assertion changed."`

### Ratcheted scopes touched, with remedy

| Scope | Lint(s) | Remedy |
|---|---|---|
| `TouchJournal::start` | disallowed_methods, excessive_nesting | deleted with the spawn (fn becomes `new`) |
| `TouchJournal::ingest` | unwrap_used | reason re-decided |
| `TouchJournal::flush_bucket` | unwrap_used; inner let_underscore_must_use | unwrap reason re-decided; let_underscore shrinks (re-decide only if the gate objects) |
| `TouchRegistry::journal` | unwrap_used | reason re-decided |
| `DirtyJournals::push`/`take`, `TouchJournal::reap`, `TouchRegistry::flush_tick` | unwrap_used | new narrow expectations in "owner; invariant; alternative" form |
| `bootstrap::run` | all 6 | reasons re-decided |
| `fixture_http::http_rig_build` | too_many_lines, let_underscore_must_use | reasons re-decided |
| `close`, `wait`, `close_shard`, `Inner::close` | — | untouched |

---

## 5. Mutation analysis (cargo-mutants 27.1.0, `--in-diff`)

**Owners selected** (check with the CI plan, §7):
- `touch` (`src/touch.rs`, filter `touch::`), which covers `touch::tests`, `touch::flusher_tests` and `touch::loom_tests`
- `bootstrap` (`bootstrap::`)
- `transaction_tests` and `retirement_tests` (`shard::`). These edits are only inside `#[cfg(test)]` modules and contain no operators, so expect "no executable mutants".

**Not selected:**
- `src/touch/flusher_tests.rs` and `loom_tests.rs`: the prefix is the exact `'src/touch.rs'`, and neither file is registered. Test-only, so no row is needed.
- `src/dst/**`: not critical.

**`src/touch.rs`, every mutant on inserted or changed lines, and its killer:**

| Site | Mutant | Killed by |
|---|---|---|
| `DirtyJournals::push` | body -> `()` | T2, T7 |
| `DirtyJournals::take` | -> `vec![]` | T2, T7 (queue not emptied) |
| `TouchJournal::new` | -> `Arc::new(Default::default())` | unviable (no `Default`) |
| `TouchJournal::ingest` | -> `()` | T2, `live_matching_key_is_proven` |
| `ingest` let-chain `&&` -> `\|\|` | — | unviable (let chains refuse `\|\|`) |
| `Inner::ingest` | -> `true` / `false` (body gone, nothing recorded) | T2, `live_matching_key_is_proven` |
| `let dirtied = !self.dirty` | delete `!` | T2, T7, Loom |
| `self.current.len() >= BUCKET_KEY_CAP` | `<` | `live_matching_key_is_proven` (proven:false) |
| `Inner::seal` | -> `None` | all delivery tests |
| `Inner::seal` | `Some(Default)` | unviable |
| `if !self.dirty` | delete `!` | T2, `live_matching_key_is_proven` |
| `generation += 1` | `-=` | overflow-check panic (profile.quality inherits dev) |
| `generation += 1` | `*=` | `catch_up_uses_the_first_relevant_bucket` (generation stays 0, so `None` ≠ `Some(true)`) |
| `flush_bucket` | -> `()` | existing live tests, T2 |
| `reap` | -> `()` | T4 |
| `TouchRegistry::with_entropy` / `journal` | FnValue | unviable (no `Default` for `TouchRegistry`/`TouchJournal`) |
| `start` | -> `Ok(())` | T2, T5 |
| `flush_until_cancelled` | -> `()` | T2 |
| `while !cancel.is_cancelled()` | delete `!` | T2 (loop never runs) |
| `ticks += 1` | `-=` | overflow panic on the first tick (T2) |
| `ticks += 1` | `*=` | T4 (sweeps every tick, so reaped before 960 ms) |
| `flush_tick` | -> `()` | T2, T4, T7 |

- No operator mutants are selected in the unchanged eviction arithmetic (`history_keys > HISTORY_KEY_BUDGET` and the rest). Those lines stay byte-identical. This is why `flush_bucket`'s body is not moved into `Inner`, and it avoids needing a 2,000,000-key budget test.
- No equivalent mutants are left.
- Every test is bounded:
  - paused time
  - waits ≤ 1 s virtual
  - T5 grace 1 s
  - Loom bounded by branches and preemption

  A mutant can make a test fail but cannot make it hang. The per-mutant `--timeout` is 90 s.

**`src/bootstrap.rs`:** the diff touches `run`'s body, which selects its FnValue `Ok(())`. `bootstrap::tests::process_bootstrap_cannot_be_an_empty_success` kills it (it `unwrap_err`s). The new lines contain no operators.

---

## 6. Ledgers (all in C2)

- `docs/quality/owners.json`: effect row removed.
- `docs/quality/source-allowances.json`: effect row removed.
- `docs/refactor/review-mechanisms.json`: `http_rig_build` `after_sha256` plus the reason extension.
- `docs/refactor/test-inventory.json`: no change. No `src/dst` test body changes, and the inventory covers only `src/dst` tests. Confirm with `--check`.
- Scenario map, dispositions and relocations: none (no DST test renamed or deleted).
- `src/dst/tests/README.md`: none.
- `scripts/quality/mutation_owners.py`: no new row. `src/touch.rs` is already `owner('touch', ...)`. The new files are test-only and outside the critical prefixes.
- `docs/refactor/architecture-policy.json`: none. `run` 778 ≤ 836; `touch.rs` has no function over 200 lines and no `crate::http`/`product` edges.
- Immutable inventories (`legacy-source.json`, `legacy-diagnostics*.json`): not touched.

---

## 7. Controls (after the running mutation leg finishes)

| Command | Expected |
|---|---|
| Staging on c397e9e5 (§3.1): `cargo test --locked --lib touch::flusher_tests` | `test result: FAILED. 4 passed; 1 failed` (T1 as quoted) |
| `cargo test --locked --lib touch::` | `test result: ok. 16 passed` (7 existing + 2 Loom + 7 flusher) |
| `cargo test --locked --lib -- shard::transaction_tests shard::retirement_tests` | ok, same count as before |
| `cargo test --locked --lib -- dst_tests::watch_observation dst_tests::watch_admission dst_tests::runtime_isolation` | ok |
| `cargo test --locked --lib bootstrap::` | ok, 2 passed |
| `scripts/test-leg.sh target/legs/quality.log --min 15 -- --locked --release --lib quality_` | ok; both `quality_loom_…` touch models listed `... ok` |
| `cargo test --locked --release` | full suite green (CI floor unchanged) |
| `cargo fmt --all -- --check`; `cargo clippy --locked --workspace --all-targets -- -D warnings`; `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items` | clean. In docs, name private items in backticks and never as intra-doc links. |
| `scripts/quality.sh` (or `python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl`) | `quality ratchets: OK`; no `accepted exception grew`, no `obsolete source allowances` |
| `python3 scripts/review-evidence.py --check` | `review-evidence source inventory: OK` |
| `python3 scripts/test-inventory.py --check`; `python3 scripts/architecture-gate.py --check` | OK |
| CI plan before push: `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) python3 scripts/quality/verification_plan.py --out target/quality-mutations` | `selected_mutation_owners` ⊇ {bootstrap, retirement_tests, touch, transaction_tests}; `unregistered_mutation_source_files` empty |
| `scripts/quality/mutations.sh` | `touch`: every mutant caught or unviable, 0 missed, 0 timeout; `bootstrap`: 1 caught; the shard test owners: `no executable mutants in the selected scope` |
| NV1–NV8 (§3.3) | each red as tabled, then reverted green |

---

## 8. Out of scope

- **Step 2.** Retiring idle journals, map growth, and deleted or recreated streams keeping their map entry and history.
- **Deregistering waiters inside `wait`.** A waiter could deregister itself on timeout or cancellation (a drop guard). That would make the once-a-second sweep unnecessary, but it changes the hot wait path and adds lock-in-`Drop` poison policy. It is a separate item.
- **A process-wide history memory bound (D2).** C1 only corrects the doc.
- **Parallel or yielding flush within a tick.** Flush work is now serial on one task. A tick runs synchronously with no await, so thousands of dirty journals in one tick hold one worker for that tick. Measure before changing anything.
- **`close_shard` keying.** Already route-keyed (`27adb40d`).

## 9. Decisions for Søren

- **D1. Policy of `touch-flusher`: Critical (applied in C2), please confirm.**
  - The only edge-visible effects are an extra element in `/v1/debug` `tasks.loops`, and the unready reason `critical task terminated: touch-flusher` if the flusher ever exits. An exit takes a panic, which needs a poisoned journal, map or queue lock.
  - Precedent treats joining the critical set as an ordinary change (request-maintenance, scaler, auth-refresher). No status code, body shape or boot refusal is added.
  - The alternative, Noncritical, is one token in `start` plus T5's expected policy. With it, a flusher panic would leave the instance serving with every watch dead, which is worse than today's single-stream loss.
- **D2. `HISTORY_KEY_BUDGET` is per journal.**
  - Each state-protocol journal may retain 2,000,000 keys (about 8 MB) and 4,096 buckets. Nothing bounds the sum across journals, so about 125 hot, high-cardinality journals could hold about 1 GiB.
  - Whether to add a process-wide history budget, and which journal to evict under it, is a memory-policy call. This plan only corrects the doc (C1).

---

## Skeptic corrections (C1..Cn)

Checked against `slate` @ `c397e9e5`, read-only. `git rev-parse origin/slate` also gives `c397e9e5` now, so the merge base for the ceilings is HEAD. No ceilinged file is edited: `touch.rs` 583, `bootstrap.rs` 915, `fixture_http.rs` 793, `transaction_tests.rs` 541, `retirement_tests.rs` 562 and `loom_tests.rs` 40 lines. The unedited `watch_observation.rs` has 924. `shard.rs` (3,139) and `http.rs` (3,153) are over the ceiling but untouched. `shard.rs:3038` `t.journal.ingest(..)` still resolves after the change to `self: &Arc<Self>`, and its text and fingerprint are unchanged.

I confirmed these claims:
- The quoted lines: `touch.rs:77-106`, `:142-213`, `:263-269` and `:391-426`; `bootstrap.rs:370-372`, `:502-510`, `:660` and `:706-710`; `watch.rs:87/103/144/333/340`; `shard.rs:725/1291/3036-3039`.
- The use-site list. `git grep` finds `TouchRegistry` only in `bootstrap.rs` and `fixture_http.rs`. `AppState` has no other constructor. `TouchJournal::start` appears only in `touch.rs`, `transaction_tests.rs:43` and `retirement_tests.rs:105`.
- The effect rows at `owners.json:1907-1914` and `source-allowances.json:1508-1514`. No other `src/touch*` row exists besides `touch_keys`.
- The reason format for every re-decided or new reason: exactly two `;` and no `"`.
- The ratchet mechanics. `exception_growth` skips an identity whose reason text changed (`source_rules.py:204-209`), so re-deciding a reason is a valid remedy.
- That `review-mechanisms.json` pins only `http_rig_build` among the edited fixtures. `Fixture::new` in the transaction and retirement tests is not pinned. `test-inventory.py` hashes only `#[test]` functions under `src/dst` (`test-inventory.py:108,138`), so the inventory does not change.
- The mutation profile: `profile.quality` inherits dev, so `-=` on a zero counter panics on overflow. The CI run uses `--timeout 90` (`mutation_driver.py:52`).
- The tokio APIs: tokio is 1.52.3, and `num_alive_tasks` is stable. `start_paused` is already used in the crate.
- The toolchain allows let chains: edition 2024, Rust 1.98.1.
- The red traces for T1, NV6 and NV7. NV6 fails at `watch_observation.rs:127` after the 8 s timeout. NV7 fails at `transaction_tests.rs:396-406` after 1 s.
- Test-only placement. `flusher_tests.rs` and `loom_tests.rs` are outside `CRITICAL_PREFIXES`, because `'src/touch.rs'` is an exact prefix (`verification_plan.py:26`). The `cfg(test)` shard test owners select no mutants.

**C1 (blocking for C2 landing): D1 is applied in the commit, but the constraints require edge-visible changes to be held.**
- C2 makes two changes that are visible at the edge:
  - A new `/v1/debug` `tasks.loops` element (`http.rs:996-1000`).
  - A new `/readyz` and `/health` 503 cause, `critical task terminated: touch-flusher` (`tasks.rs:269-281`).
- §9 marks this "applied in C2, please confirm". The rule says such changes are "listed as decisions for Søren and kept out of the commits". So C2 is blocked until Søren answers D1.
- The `loops` element comes with any supervised flusher, whatever policy is chosen, so it belongs in D1 as well.
- §2.1 is wrong that "every other service loop is Critical". `runtime-watchdog` is `Policy::Noncritical` (`http.rs:2857-2861`). So Noncritical is an existing precedent, not a hypothetical. Put that precedent into D1's alternative.

**C2: T2 and T4 must use `tokio::time::Instant`.**
- §3.1 T2 writes `ingested = Instant::now()` and asserts `ingested.elapsed() <= 25ms`.
- Under `start_paused` a `std::time::Instant` barely advances, so the latency pin would pass no matter which tick publishes. It would be vacuous.
- Write `tokio::time::Instant` explicitly in T2, and for `started` in T4. `sleep_until` requires it anyway.

**C3: NV5's expected red has the wrong count.**
- With `let dirtied = true`, T7's three `a.ingest` calls plus one `b.ingest` queue `[a, a, a, b]`.
- The red is 4 entries vs 2, not "3 entries vs 2".

**C4: the retirement fixture does not compile as written.**
- §4 has `journal: touch.journal(HASH, RouteHash([9;16]))`. But `retirement_tests.rs` imports only `super::*` (the shard module), and `shard.rs` does not import `RouteHash`.
- Write `crate::crypto::RouteHash([9; 16])`, as the transaction fixture already does.

**C5: §2.2 "same total work" hides a new lock profile. Record it; it does not block.**
- Today's per-journal reap never takes the registry map lock. The planned sweep clones every journal `Arc` under `self.map` once a second.
- `TouchRegistry::journal()` takes that lock on every state-protocol append (`watch.rs:144`) and every wait (`watch.rs:333`).
- Step 2 is deferred, so the map holds every journal created since the shard opened, deleted and recreated incarnations included.
- Also, every dirty journal's flush and wake now runs serially on one worker. The pinned "≤ 25 ms" is measured in paused time, where flush work costs nothing.
- Add both points to §8 as measured-later risks. Give T2's doc the qualifier "plus serial flush work of every dirty journal".

**C6: D2 must cite the documented contract, not only the code doc.**
- `SPEC.md:567-568` names PROFILES.md §6 as the contract of record. `docs/history/PROFILES.md:175-177` promises a **global** 2M-key budget (~8 MB).
- C1 (the plan's doc fix) corrects only `touch.rs:16`. D2 is therefore a choice: amend the historical contract of record, or implement the global bound. It is not only a memory-policy question.
- No other doc (RUNBOOK, SCALING, READINESS, DST, OPS) describes per-journal tasks or lists supervised loops. I found no other doc breakage.

**C7: add a timing control for the mutation leg.**
- Every `touch` mutant now runs the `touch::` filter. That filter includes both Loom models and the paused-time tests, run under `--profile quality` (opt-level 1) with a 90 s per-mutant timeout.
- A TIMEOUT fails the leg. Add to §7: time `cargo test --locked --profile quality --lib touch::` once and confirm it is far below 90 s. In particular, the new Loom model's execution count at `preemption_bound Some(2)` should be small.
- Two surviving existing tests take 1 s of real time only when a mutant makes them fail, so they are fine.

**C8: minor accuracy points.**
- §3.1: the staged helper leaves `tasks` unused. Name it `_tasks` in the staging, or the red run prints an unused-variable warning.
- §4: the `bootstrap.rs` insert is 6 lines (a comment plus a rustfmt-broken 5-line chain, since chain_width is 60), so the file goes 915 -> 921, not 920. It stays under 1,000, and `run` stays at or below its 836-line budget.
- §2.2 "Shutdown" is correct and could cite why. `serve_h1` aborts and joins every connection on cancel (`http/serve.rs:84-119`), so no parked long-poll outlives the flusher.

I found no missed mutants. The FnValue, `!`-deletion and `+=` mutants on changed lines are all covered. `>=` → `<` on the moved `BUCKET_KEY_CAP` line is killed by `live_matching_key_is_proven`. `-=` and `*=` on `generation`/`ticks` are covered. The let-chain `&&` → `||` does not compile, so it is unviable. No equivalent mutant remains.

The ledger list is complete: `owners.json`, `source-allowances.json` and `review-mechanisms.json`, with no changes needed to `test-inventory`, `architecture-policy`, `mutation_owners` or the scenario map.

**Verdict: ready-with-corrections.** C1 blocks landing C2 until Søren answers D1. Fold C2 through C8 into the plan before implementation.
