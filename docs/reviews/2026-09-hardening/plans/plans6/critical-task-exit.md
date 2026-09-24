# Item 38: a critical loop's exit stops the process root; a failed engine close ends the directory stop at once

Repository `/Users/sorenschmidt/code/streams`, branch `slate`. The task named HEAD `0afa2597`; while this plan was written `origin/slate` moved to `fba5af56` (four commits touching only `scripts/`, `docs/refactor/architecture-policy.json` and `docs/review-verification-evidence.md`; `git diff --stat 0afa2597..HEAD -- src` is empty). Every line number below is from that tree. I only read the repository; nothing in it was edited or run.

**Verdict: all three problems are real on the current tree.**

1. A Critical loop that returns or panics while the runtime runs flips `TaskMonitor::unready_reason` to `Some(..)`, and it stays there for good. `/health` answers 503 for the life of the process. Every other route keeps serving, because only `health_axum` consults the supervisor.
2. The only thing that ever requests the ordered shutdown is the `signal` task. The exit watchdog (`spawn_unready_watchdog`) looks at `ShardDirectory::unready_reason` only. It never sees the supervisor's predicate, so such an instance lives as a 503 zombie until someone kills it.
3. `ShardDirectory::shutdown` treats an engine whose storage close **failed** as still pending. It waits out the whole grace in 10 ms steps, then returns `"shutdown ongoing or failed: 0 opens, 1 engines; owners retained"`. That message drops the joined report that names the failure.

**The reviewer's Change is right in substance. As written it cannot be built without five corrections, which this plan makes:**

- (a) A supervisor-wide "self-cancel on critical exit" breaks the R17 DST pin. `readiness_endpoint_refuses_each_permanent_critical_exit` (`src/dst/review_readiness.rs:14-45`, sha-pinned in `docs/refactor/review-mechanisms.json`) serves through `serve_h1` with a **rig** supervisor. It needs `/health` to answer 503 after the exit, and a self-cancelling supervisor would have closed the listener first. Engine supervisors (`EngineTasks: Default`) have their own required-exit fence. So escalation must be chosen at construction: `TaskSupervisor::process_root()` for the binary only, and `new()`/`Default` stay as they are.
- (b) The supervisor has no exit observer. It learns of exits only passively, through `JoinHandle::is_finished()` at `/health` time. The hook has to go into `TaskSupervisor::spawn`, which carries a function-wide `#[expect(clippy::unwrap_used)]` and a `#[expect(clippy::disallowed_methods)]`, both ratcheted. Both reasons are re-decided.
- (c) "run returns Err after the ordered sequence" cannot be written inside `bootstrap::run`, for two reasons. Its six expectations are ratcheted (two of them fingerprint every call and path). And no test can drive `run` (`RUN_WAS_INVOKED`, real stores), so any predicate there becomes a missed mutant. The post-serve sequence is therefore first extracted verbatim into `bootstrap::ordered_stop` (commit C2), where `bootstrap::tests` can drive it.
- (d) No store fault makes an engine's storage close fail: `close_db` maps Clean and Fenced to `Ok`, and SlateDB retries `FaultStore` errors. The directory fix therefore needs a `#[cfg(test)]` hook that installs a failing close finalizer. It sits in `src/shard/lifecycle.rs`; production tokens are unchanged.
- (e) The `shard_directory` mutation owner filters only `shard_directory::`. The only tests that drive `ShardDirectory::shutdown` against real engines are in `shard::task_lifecycle_tests`, so the owner row gains that filter.

---

## 1. Problem (verified)

### 1.1 A critical exit flips readiness for good — `src/tasks.rs:253-274`

```
253  impl TaskMonitor {
254      /// Serving requires a live supervisor in its running phase and all
255      /// required loops still running. Recoverable errors inside a loop do
256      /// not change this verdict; permanent task exit does.
...
261      pub(crate) fn unready_reason(&self) -> Option<String> {
262          let Some(inner) = self.inner.upgrade() else {
263              return Some("runtime supervisor unavailable".into());
264          };
265          let state = inner.state.lock().unwrap();
266          match state.phase {
267              Phase::ShuttingDown => Some("runtime shutting down".into()),
268              Phase::Stopped => Some("runtime stopped".into()),
269              Phase::Running => state.tasks.values().find_map(|task| {
270                  (task.policy == Policy::Critical && task.handle.is_finished())
271                      .then(|| format!("critical task terminated: {}", task.name))
272              }),
273          }
274      }
```

`is_finished()` never goes back to false, and the phase stays `Running` (only `cancel` and `launch_shutdown` move it). The verdict is therefore permanent. `readiness_changes_after_each_kind_of_critical_exit` (`src/tasks/tests.rs:7-49`) pins exactly this for Done, Failed and a panic.

### 1.2 Nothing but a signal requests the ordered shutdown

`grep -rn -e "\.request()" -e "shutdown_request()" -e "\.cancel()" src` finds exactly one production requester, `src/bootstrap.rs:733-747`:

```
734          let request = tasks.shutdown_request();
735          let _ = tasks.spawn(
736              "signal",
737              crate::tasks::Policy::Critical,
738              move |cancel| async move {
739                  tokio::select! {
740                      _ = cancel.cancelled() => {}
741                      _ = terminate.recv() => {
742                          tracing::info!("termination signal: shutting down");
743                          request.request();
```

`ShutdownRequest::request` → `TaskSupervisor::cancel` (`src/tasks.rs:384-399`) is the only `Running → ShuttingDown` transition outside `launch_shutdown`:

```
391      pub(crate) fn cancel(&self) {
392          {
393              let mut st = self.inner.state.lock().unwrap();
394              if st.phase == Phase::Running {
395                  st.phase = Phase::ShuttingDown;
396              }
397          }
398          self.inner.cancel_tx.send_replace(true);
399      }
```

The accept loop leaves only on that cancellation (`src/http.rs:1305,1309`: `let cancel = tasks.cancellation(); … _ = cancel.cancelled() => break,`), and `run`'s ordered stop (`src/bootstrap.rs:902-919`) runs only after `serve_h1` returns. A critical exit therefore never reaches the stop.

### 1.3 The exit watchdog looks at a narrower predicate than `/health`

`src/sharddir.rs:155-183` (the watchdog) samples the directory only:

```
173                  let reason = directory.unready_reason();
174                  match window.observe(reason.is_some(), clock.monotonic(), limit) {
175                      WatchdogDecision::Expired { elapsed } => {
...
182                          std::process::exit(1);
```

`src/http.rs:1845-1848` (`health_axum`) checks the supervisor **first**:

```
1845  async fn health_axum(State(state): State<Arc<AppState>>) -> Response {
1846      if let Some(reason) = state.tasks.unready_reason() {
1847          return (StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
1848      }
```

The only other readers of the supervisor verdict are `debug_load` (`src/http.rs:1017`, `"critical_failure": state.tasks.critical_failure()`) and tests. No request handler checks it, so the zombie keeps answering appends and reads while `/health` says 503.

### 1.4 The critical loops on the process root, and how each can end

These were checked so the new policy cannot kill a healthy instance.

| Loop (spawn site) | Returns on its own while Running? |
|---|---|
| `request-maintenance` (`application/request_work.rs:142-150`) | only after `run(cancel)` returns, which happens on cancel (`:228`) |
| `unready-watchdog` (`sharddir.rs:163-181`) | never returns; `process::exit(1)` on expiry |
| `signal` (`bootstrap.rs:735-746`) | returns after `request.request()`; by then the phase is already `ShuttingDown` |
| `auth-refresher` (`auth_feed.rs:363-387`) | cancel arms only |
| `scaler` (`scaler3.rs:527-570`) | `Done` if `Weak<AppState>` cannot upgrade. The router holds the state until `serve_h1` returns, which is after the cancel. |
| `rss-sampler` (`bootstrap/rss.rs:10-53`) | `Failed("allocator purge worker: …")` if the purge `spawn_blocking` join fails, which is a real failure |
| `fleet` (`fleet.rs:458-…`) | cancel arms only (`:492,503,871`); panics possible |
| `telemetry-outbox-sweep`, `telemetry-drain` (`billing/telemetry_loop.rs`) | cancel arms only |
| `usage-rollup` (`billing.rs:1390-…`) | `Failed("usage rollup open failed: …")` when `ROLLUP=1` and `BILLING_MODE≠required`, which is a real failure (required mode opens it at boot) |

Every early end is either a real failure or a panic. None of them is a normal "work complete".

### 1.5 `ShardDirectory::shutdown` waits out its grace after a failed close and drops the cause — `src/shard_directory.rs:375-413`

```
387          let deadline = tokio::time::Instant::now() + grace;
388          loop {
389              let (engines, opens) = self.inner.gate.shutdown_pending();
390              let reports = futures_util::future::join_all(engines.iter().map(|engine| {
391                  engine.wait(deadline.saturating_duration_since(tokio::time::Instant::now()))
392              }))
393              .await;
394              let pending = engines.iter().filter(|engine| !engine.terminated()).count();
395              if opens == 0 && pending == 0 {
396                  let failures: Vec<_> = reports.into_iter().filter_map(Result::err).collect();
...
403              if tokio::time::Instant::now() >= deadline {
404                  return Err(format!(
405                      "shutdown ongoing or failed: {opens} opens, {pending} engines; owners retained"
406                  ));
407              }
408              tokio::time::sleep_until(
409                  deadline.min(tokio::time::Instant::now() + Duration::from_millis(10)),
```

A failed close is final. See `src/tasks/shutdown.rs:78-89`:

```
78                  resources_closed = matches!(outcome, TaskOutcome::Finished);
...
84                  // A failed/panicked close cannot prove resource termination.
85                  // Publish the failure, but keep the owner's replacement fence.
86                  if resources_closed {
87                      st.phase = Phase::Stopped;
88                  }
89                  st.report = Some(report.clone());
```

and `src/shard/lifecycle.rs:98-104`:

```
 99      pub(crate) fn failure(&self) -> Option<String> {
100          self.0.incomplete_shutdown_failure()
101      }
102      pub(crate) fn terminated(&self) -> bool {
103          self.0.monitor().phase() == Some(crate::tasks::Phase::Stopped)
104      }
```

`terminated()` stays false for good. `wait()` returns the published report at once, with an `Err` naming `storage-close: Failed(..)`. Line 394 still counts the engine as pending, so the loop runs every 10 ms until the deadline (10 s in production, `bootstrap.rs:915`). Lines 404-406 then drop `reports`. `EngineShutdown::failure()` is already consulted by `OpenGate::unready_reason` (`sharddir.rs:417-431`) and by nothing in the stop.

### 1.6 The stale comment — `src/tasks.rs:29-31`

```
29      /// The runtime is not healthy without it (fleet loop, telemetry
30      /// drain, the watchdogs): an unexpected exit is a critical failure
31      /// (surfaced to readiness by WP-15's remaining slice).
```

Readiness is already surfaced by `TaskMonitor::unready_reason` (§1.1). Also, "the watchdogs" is wrong for `runtime-watchdog`, which is `Noncritical` (`http.rs:3082`).

### 1.7 Line budgets

| File | Now | Ceiling | After C1 / C2 / C3 |
|---|---|---|---|
| `src/http.rs` | 3,371 | 3,371 | untouched |
| `src/product.rs` | 4,205 | 4,205 | untouched |
| `src/shard.rs` | 3,232 | 3,232 | untouched (the hook lives in `shard/lifecycle.rs`) |
| `src/billing.rs` | 2,201 | 2,201 | untouched |
| `src/history.rs` | 1,713 | 1,713 | untouched |
| `src/auth.rs` | 1,676 | 1,676 | untouched |
| `src/registry.rs` | 1,509 | 1,509 | untouched |
| `src/sse/feed.rs` | 1,200 | 1,200 | untouched |
| `src/fleet.rs` | 1,143 | 1,143 | untouched |
| `src/bootstrap.rs` | 923 | 1,000 (a file may not cross from ≤1,000 to above it) | 923 / ≈934 / ≈947 |
| `src/tasks.rs` | 403 | 1,000 | – / – / ≈520 |
| `src/tasks/shutdown.rs` | 278 | 1,000 | – / – / ≈287 |
| `src/tasks/tests.rs` | 472 | 1,000 | – / – / ≈625 |
| `src/bootstrap/tests.rs` | 60 | 1,000 | – / ≈105 / ≈135 |
| `src/shard_directory.rs` | 638 | 1,000 | ≈646 |
| `src/shard/lifecycle.rs` | 135 | 1,000 | ≈147 |
| `src/shard/task_lifecycle_tests.rs` | 300 | 1,000 | ≈375 |

None of the ceilinged files is touched, so no verbatim move into a sub-module is needed. C2 is an extraction inside `bootstrap.rs`, which is not ceilinged. It is kept verbatim and behaviour-neutral so C3's diff stays small.

---

## 2. Contract decision

### 2.1 Typed contract (tasks owner)

- `TaskSupervisor::process_root() -> TaskSupervisor`: the binary's own supervisor. `new()` and `Default` keep today's semantics, where the owner answers for a critical exit. Engines (`EngineTasks: Default`), every test rig (`fixture_http.rs:447`, `http/serve.rs:67`, the DST controllers) and every unit test keep `new()`.
- A private `enum ExitAuthority { Owner, Supervisor }` on `Inner` holds that choice. It has no wildcard arms.
- `pub(crate) struct CriticalExit { pub name: &'static str, pub outcome: TaskOutcome }`, with `outcome ∈ {Finished, Failed(msg), Panicked(msg)}` using the join report's own classification and panic message.
- `TaskSupervisor::stop_cause() -> Option<CriticalExit>`.
- **Invariant:** under `process_root`, the first Critical loop whose future ends (by return or panic) while the phase is `Running` does two things in **one critical section of the existing state mutex**: it moves the phase to `ShuttingDown` and records itself as the stop cause. The cancellation is then published, exactly as `cancel()` publishes it (`Inner::request_stop` is now the one transition that both use).
  - A loop that ends after any stop was requested (signal, an earlier exit, `shutdown`) is a consequence. It appears only in the join report.
  - Noncritical ends never stop anything.
  - An aborted task never reaches the observer. Only the shutdown driver aborts.
- The join handle carries the same `TaskResult` or the same panic payload as before (`catch_unwind` then `resume_unwind`). `ShutdownReport` classification is unchanged.
- `TaskMonitor::unready_reason` is unchanged. After an escalation it answers `runtime shutting down`, not `critical task terminated: <name>`. That is moot, because the listener is gone within the same scheduling round.

### 2.2 Process contract (bootstrap)

- `run` builds its supervisor with `process_root()`. `bootstrap::ordered_stop` (extracted in C2) runs the same sequence as today: loops joined within 10 s, then shards closed within 10 s. After that, if `tasks.stop_cause()` is `Some`, it returns `Err`: `critical task <name> exited while the runtime was running: <outcome:?>`. A shard-close failure in the same stop is logged at error level (`shard close after the critical exit: …`) instead of returned, because the cause outranks what the stop itself then failed at.
- `main` returns that error, so the process prints `Error: critical task fleet exited while the runtime was running: Failed("…")` and exits with code 1.
- A requested stop (SIGTERM or SIGINT) still returns `Ok(())` with exit code 0.

### 2.3 Directory contract

- An engine whose storage close failed (`EngineShutdown::failure().is_some()`) is **settled**, not pending. When no open is in flight and every engine is terminated or has failed, `ShardDirectory::shutdown` returns at once: `Err(joined reports)`, which names `storage-close: Failed(..)`.
- At the deadline the error keeps its prefix and appends every joined report: `shutdown ongoing or failed: {opens} opens, {pending} engines; owners retained; <report>; …`. So a failed close is no longer dropped when another engine or an open is still pending.
- `pending` now counts only engines that are still closing.

### 2.4 What changes at the edge

| | Before | After |
|---|---|---|
| Critical loop ends/panics while serving (binary) | Process stays up. `/health` and `/readyz` 503 `critical task terminated: <name>` forever. Every other route still served. Exit watchdog blind. | Listener closes at once (new connections refused, open keep-alives and SSE aborted, exactly as on SIGTERM). Loops joined, shards closed, exit code 1 with the cause on stderr. |
| Same, under a test rig or an engine supervisor | readiness 503 | unchanged |
| SIGTERM | exit 0 after the ordered stop | unchanged |
| Engine close failed during the stop | 10 s wait, cause dropped from the process error | immediate; cause in the error |

No HTTP status code or body changes on any route. The change is a process lifecycle change, so it needs Søren's approval (decisions D1, D2).

### 2.5 Backward-compatible alternatives

- **Status quo plus a delay.** Widen the existing unready watchdog to also sample `TaskMonitor::unready_reason`. The zombie then keeps serving for `UNREADY_EXIT_AFTER_SECS` (default 300 s, 0 disables) and dies by `process::exit(1)`: no ordered stop, no join, no shard close. Behaviour is unchanged for five minutes and still configurable. I do not recommend it: an unclean exit after five minutes of billing and fleet blindness.
- **Opt-in.** `process_root()` could be gated by an env knob, default off. This keeps today's zombie by default.

---

## 3. Red tests

Test paths for `--exact`: `tasks::tests::<name>`, `bootstrap::tests::<name>`, `shard::task_lifecycle_tests::<name>`.

### 3.1 C1: directory (red on the current tree plus the test-only hook of §4.C1.2)

**T8 `a_failed_storage_close_ends_the_directory_stop_at_once`** in `src/shard/task_lifecycle_tests.rs`:

```rust
#[expect(
    clippy::let_underscore_must_use,
    reason = "a_failed_storage_close_ends_the_directory_stop_at_once; the scripted close never closed the database, so the fixture closes it on the way out; a failed close leaves nothing the assertions depend on"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_storage_close_ends_the_directory_stop_at_once() {
    let (engine, _store) = fixture().await;
    let directory = serving(&engine);
    assert!(matches!(
        directory.open_or_wait(&engine.prefix, Duration::from_secs(1)).await,
        crate::sharddir::OpenOutcome::Ready(_)
    ));
    engine.tasks.begin_failed_close_for_test("scripted close failure");
    let stopped = tokio::time::timeout(Duration::from_secs(5), directory.shutdown(Duration::from_secs(30)))
        .await
        .expect("a failed close is final: the directory stop must not wait out its grace");
    let error = stopped.unwrap_err();
    assert!(error.contains("storage-close: Failed(\"scripted close failure\")"), "{error}");
    assert!(
        directory.unready_reason().unwrap().contains("scripted close failure"),
        "the owner keeps the readiness failure"
    );
    assert!(!engine.termination_complete(), "a failed close proves no termination");
    let _ = engine.db.close().await;
}
```

Expected red. Today the loop waits 30 s; the outer bound fires at 5 s:

```
thread 'shard::task_lifecycle_tests::a_failed_storage_close_ends_the_directory_stop_at_once' panicked at src/shard/task_lifecycle_tests.rs:<L>:<C>:
a failed close is final: the directory stop must not wait out its grace: Elapsed(())
```

**T9 `a_directory_stop_past_its_grace_carries_the_joined_reports`** (held WAL):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_directory_stop_past_its_grace_carries_the_joined_reports() {
    let (engine, store) = held_wal().await;
    let directory = serving(&engine);
    assert!(matches!(
        directory.open_or_wait(&engine.prefix, Duration::from_secs(1)).await,
        crate::sharddir::OpenOutcome::Ready(_)
    ));
    assert_eq!(
        directory.shutdown(Duration::from_millis(5)).await.unwrap_err(),
        "shutdown ongoing or failed: 0 opens, 1 engines; owners retained; engine shutdown still running; join authority retained"
    );
    store.release_hold();
    directory.shutdown(Duration::from_secs(10)).await.unwrap();
    assert!(engine.termination_complete());
}
```

Expected red:

```
thread 'shard::task_lifecycle_tests::a_directory_stop_past_its_grace_carries_the_joined_reports' panicked at src/shard/task_lifecycle_tests.rs:<L>:<C>:
assertion `left == right` failed
  left: "shutdown ongoing or failed: 0 opens, 1 engines; owners retained"
 right: "shutdown ongoing or failed: 0 opens, 1 engines; owners retained; engine shutdown still running; join authority retained"
```

Why this is deterministic: with 5 ms of grace, the joined `EngineShutdown::wait` times out, which gives exactly one report, `engine shutdown still running; join authority retained` (`lifecycle.rs:108`). The engine is neither terminated nor failed, so `pending = 1` and `opens = 0` (the open completed). If the first pass ends before the deadline, the second pass's `wait(0)` yields the same report.

Shared helper, with no block nesting (the inline opener in `r17a_runtime_shutdown_…` needs an `excessive_nesting` expect; this one does not):

```rust
/// A directory whose one prefix is served by `engine`.
fn serving(engine: &Arc<ShardEngine>) -> crate::shard_directory::ShardDirectory {
    let opened = engine.clone();
    crate::shard_directory::ShardDirectory::new(
        vec![engine.prefix.clone()],
        crate::ownership::OwnershipService::new(""),
        crate::shard_directory::OpenTiming {
            open_deadline: Duration::from_secs(10),
            open_wait: Duration::from_secs(1),
        },
        move |_| Box::new(move |_, _| Box::pin(std::future::ready(Ok(opened.clone())))),
    )
}
```

The closure signature is deduced from `OpenFn`, as in the existing r17a opener. The `Ok` type is fixed by that signature.

### 3.2 C2: extraction (behaviour-neutral; pins the extracted owner and kills its body mutants)

These tests are green before and after C2; there is no red. In `src/bootstrap/tests.rs`:

```rust
/// A directory that holds no shard: its opener is never called.
fn idle_directory() -> crate::shard_directory::ShardDirectory {
    crate::shard_directory::ShardDirectory::new(
        Vec::new(),
        crate::ownership::OwnershipService::new(""),
        crate::shard_directory::OpenTiming {
            open_deadline: Duration::from_secs(1),
            open_wait: Duration::from_secs(1),
        },
        |_| Box::new(|_, _| Box::pin(std::future::pending())),
    )
}

/// A loop that stops only when the stop is requested.
fn polite(tasks: &TaskSupervisor) {
    tasks
        .spawn("fleet", Policy::Critical, |cancel| async move {
            cancel.cancelled().await;
            TaskResult::Done
        })
        .unwrap();
}

/// T6: the ordered stop joins every loop before it reports the listener's
/// own failure, so the process never exits with a loop still running.
#[tokio::test]
async fn the_ordered_stop_joins_the_loops_before_reporting_a_failed_listener() {
    let tasks = TaskSupervisor::new();
    polite(&tasks);
    tasks.cancel();
    let error = ordered_stop(&tasks, &idle_directory(), Err(std::io::Error::other("accept loop failed")))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "accept loop failed");
    assert_eq!(tasks.monitor().phase(), Some(Phase::Stopped));
}

/// T7: a requested stop (a termination signal) ends in success.
#[tokio::test]
async fn a_requested_stop_ends_in_success() {
    let tasks = TaskSupervisor::new(); // C3 changes this to process_root()
    polite(&tasks);
    tasks.shutdown_request().request();
    ordered_stop(&tasks, &idle_directory(), Ok(())).await.unwrap();
    assert_eq!(tasks.monitor().phase(), Some(Phase::Stopped));
}
```

The `Box::pin(std::future::pending())` opener is the pattern that `shard_directory::directory_tests::slow_open_is_reported_as_retryable` already compiles. Imports: `use super::{RUN_WAS_INVOKED, absorber_config, ordered_stop, run};` and `use crate::tasks::{Phase, Policy, TaskResult, TaskSupervisor};`.

### 3.3 C3: the process root

On the literal current tree these tests do not compile:
- `error[E0432]: unresolved import super::CriticalExit`
- `error[E0599]: no function or associated item named process_root found for struct TaskSupervisor`
- `error[E0599]: no method named stop_cause …`

The **red step** therefore stages the new shape with the old semantics. This is local only; it runs `cargo test`, never `quality.sh`, and it is never committed:
- `CriticalExit` declared as in §4.C3;
- `pub(crate) fn process_root() -> Self { Self::new() }`;
- `pub(crate) fn stop_cause(&self) -> Option<CriticalExit> { None }`.

On that tree T1, T3 and T5 fail as below. T2, T4 and the C2 tests pass (they are guards).

**T1 `a_critical_exit_stops_the_process_root_and_names_its_cause`** (`src/tasks/tests.rs`):

```rust
/// Item 38: the process root answers a critical loop's exit itself. It
/// records the first one as the cause and requests the ordered stop, so the
/// process fails instead of serving behind a permanent 503.
#[tokio::test]
async fn a_critical_exit_stops_the_process_root_and_names_its_cause() {
    let cases: [(fn() -> TaskResult, TaskOutcome); 3] = [
        (|| TaskResult::Done, TaskOutcome::Finished),
        (|| TaskResult::Failed("boom".into()), TaskOutcome::Failed("boom".into())),
        (|| panic!("critical panic"), TaskOutcome::Panicked("critical panic".into())),
    ];
    for (outcome, expected) in cases {
        root_exit_case(outcome, expected).await;
    }
}

async fn root_exit_case(outcome: fn() -> TaskResult, expected: TaskOutcome) {
    let root = TaskSupervisor::process_root();
    let (release, held) = tokio::sync::oneshot::channel();
    root.spawn("required", Policy::Critical, |_| async move {
        held.await.unwrap();
        outcome()
    })
    .unwrap();
    assert_eq!(root.stop_cause(), None);
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), root.cancellation().cancelled())
        .await
        .expect("a critical exit must request the process root's ordered stop");
    assert_eq!(root.phase(), Phase::ShuttingDown);
    let cause = CriticalExit { name: "required", outcome: expected.clone() };
    assert_eq!(root.stop_cause(), Some(cause.clone()));
    let report = root.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.outcomes, vec![("required", expected)], "the join report carries the same end");
    assert_eq!(root.stop_cause(), Some(cause), "the cause outlives the stop");
}
```

Expected red (first case, Done):

```
thread 'tasks::tests::a_critical_exit_stops_the_process_root_and_names_its_cause' panicked at src/tasks/tests.rs:<L>:<C>:
a critical exit must request the process root's ordered stop: Elapsed(())
```

**T2 `only_an_unrequested_critical_exit_stops_the_process_root`** (guard; green on the staged tree and after the fix; kills the `!=`/`||` mutants):

```rust
/// Only an UNREQUESTED critical exit is a cause. A noncritical loop's end, a
/// critical end under a supervisor whose owner answers for it (an engine, a
/// rig) and a loop answering a requested stop all stop nothing.
#[tokio::test]
async fn only_an_unrequested_critical_exit_stops_the_process_root() {
    let root = TaskSupervisor::process_root();
    root.spawn("hygiene", Policy::Noncritical, |_| async {
        TaskResult::Failed("optional".into())
    })
    .unwrap();
    let owned = TaskSupervisor::new();
    owned.spawn("committer", Policy::Critical, |_| async { TaskResult::Done }).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while owned.monitor().critical_failure().is_none()
            || root.monitor().snapshot()[0].state == TaskState::Running
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!((root.phase(), root.stop_cause()), (Phase::Running, None));
    assert!(!root.cancellation().is_cancelled(), "a noncritical end stops nothing");
    assert_eq!((owned.phase(), owned.stop_cause()), (Phase::Running, None));
    assert_eq!(
        owned.monitor().unready_reason().as_deref(),
        Some("critical task terminated: committer"),
        "the owner answers: readiness fails, nothing stops"
    );
    let requested = TaskSupervisor::process_root();
    polite_loop(&requested); // "fleet": awaits cancel, returns Done
    requested.cancel();
    assert_eq!(requested.shutdown(Duration::from_secs(1)).await.finished(), vec!["fleet"]);
    assert_eq!(requested.stop_cause(), None, "a requested stop has no critical cause");
    root.shutdown(Duration::from_secs(1)).await;
    owned.shutdown(Duration::from_secs(1)).await;
}
```

The loop condition is sound because the observer runs **before** the task's future completes. `is_finished()` being true therefore implies the exit was already judged.

**T3 `the_first_critical_exit_is_the_cause_and_later_exits_its_consequences`** (held interleaving):

```rust
/// Held interleaving: the first critical exit is the cause. A critical loop
/// that ends because of the stop it requested is a consequence, visible only
/// in the join report, and a later request cannot replace the cause.
#[tokio::test]
async fn the_first_critical_exit_is_the_cause_and_later_exits_its_consequences() {
    let root = TaskSupervisor::process_root();
    let (release, held) = tokio::sync::oneshot::channel();
    root.spawn("fleet", Policy::Critical, |_| async move {
        held.await.unwrap();
        TaskResult::Failed("repository gone".into())
    })
    .unwrap();
    root.spawn("telemetry-drain", Policy::Critical, |cancel| async move {
        cancel.cancelled().await;
        TaskResult::Failed("stopped by the first".into())
    })
    .unwrap();
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), root.cancellation().cancelled())
        .await
        .expect("the first exit requests the stop");
    let report = root.shutdown(Duration::from_secs(1)).await;
    assert_eq!(
        report.outcomes,
        vec![
            ("fleet", TaskOutcome::Failed("repository gone".into())),
            ("telemetry-drain", TaskOutcome::Failed("stopped by the first".into())),
        ]
    );
    root.cancel();
    assert_eq!(
        root.stop_cause(),
        Some(CriticalExit { name: "fleet", outcome: TaskOutcome::Failed("repository gone".into()) })
    );
}
```

Expected red:

```
thread 'tasks::tests::the_first_critical_exit_is_the_cause_and_later_exits_its_consequences' panicked at src/tasks/tests.rs:<L>:<C>:
the first exit requests the stop: Elapsed(())
```

**T4 `racing_exits_and_a_request_settle_on_one_stop`** (multi-thread race; guard):

```rust
/// Racing exits and a racing request settle in one critical section: every
/// round publishes the stop and joins every loop, and a recorded cause is
/// one of the loops that raced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_exits_and_a_request_settle_on_one_stop() {
    const RACERS: [&str; 4] = ["r0", "r1", "r2", "r3"];
    for _round in 0..20 {
        let root = TaskSupervisor::process_root();
        let (go, gate) = tokio::sync::watch::channel(false);
        for name in RACERS {
            spawn_racer(&root, name, gate.clone());
        }
        go.send_replace(true);
        root.cancel();
        let report = root.shutdown(Duration::from_secs(1)).await;
        assert!(report.aborted.is_empty(), "{report:?}");
        assert_eq!(report.finished(), RACERS.to_vec());
        assert!(root.stop_cause().is_none_or(|cause| RACERS.contains(&cause.name)));
    }
}

fn spawn_racer(root: &TaskSupervisor, name: &'static str, mut gate: tokio::sync::watch::Receiver<bool>) {
    root.spawn(name, Policy::Critical, move |_| async move {
        gate.wait_for(|go| *go).await.unwrap();
        TaskResult::Done
    })
    .unwrap();
}
```

No `tokio::spawn` and no `tokio::select!` appear in the new tests, so no `owners.json` effect or macro-dsl rows are needed. Imports become `use super::{CriticalExit, Phase, Policy, SpawnRejected, TaskOutcome, TaskResult, TaskState, TaskSupervisor};`. `polite_loop` is a small helper like `polite` in §3.2.

**T5 `a_critical_exit_ends_the_accept_loop_and_fails_the_ordered_stop`** (`src/bootstrap/tests.rs`, end-to-end through the real accept loop):

```rust
/// Item 38: under the process root a critical loop's exit ends the accept
/// loop. The ordered stop then runs to its end and fails the process, naming
/// the loop.
#[tokio::test]
async fn a_critical_exit_ends_the_accept_loop_and_fails_the_ordered_stop() {
    let tasks = TaskSupervisor::process_root();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    tasks
        .spawn("fleet", Policy::Critical, |_| async { TaskResult::Failed("repository gone".into()) })
        .unwrap();
    let served = tokio::time::timeout(
        Duration::from_secs(5),
        crate::http::serve_h1(listener, axum::Router::new(), &crate::config::HttpConfig::default(), tasks.clone()),
    )
    .await
    .expect("a critical exit must end the accept loop");
    let error = ordered_stop(&tasks, &idle_directory(), served).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "critical task fleet exited while the runtime was running: Failed(\"repository gone\")"
    );
    assert_eq!(tasks.monitor().phase(), Some(Phase::Stopped), "the loops were joined before the process failed");
}
```

Expected red:

```
thread 'bootstrap::tests::a_critical_exit_ends_the_accept_loop_and_fails_the_ordered_stop' panicked at src/bootstrap/tests.rs:<L>:<C>:
a critical exit must end the accept loop: Elapsed(())
```

T7 switches to `TaskSupervisor::process_root()` in C3. It then pins that a signal-requested stop on the root still succeeds, even though a Critical loop ended after the request.

**Existing controls that must stay green unchanged:**
- `tasks::tests::readiness_changes_after_each_kind_of_critical_exit`
- `tasks::tests::critical_exits_failures_and_panics_are_reported` (sha-pinned; body untouched)
- `tasks::tests::cancel_closes_registration_before_the_join`
- `tasks::tests::cancellation_refuses_poisoned_registration_state`
- `tasks::tests::a_panicking_builder_poison_prevents_health_claims_and_later_spawns`
- `dst::dst_tests::review_readiness::readiness_endpoint_refuses_each_permanent_critical_exit` (sha-pinned; the rig keeps `new()`)
- `dst::dst_tests::runtime_engine_lifecycle::*` (engine role exits: the owner answers, the rig keeps serving 503)
- `shard::task_lifecycle_tests::r17a_*`
- `http::serve::tests::*`
- `bootstrap::tests::process_bootstrap_cannot_be_an_empty_success`

---

## 4. Edits file by file, in commit order

There are three commits, each independently green under `scripts/quality.sh`. They can be pushed together, but each must pass alone, because the CI plan compares against the `before` revision.

### C1: "A failed engine close ends the directory stop at once, with its cause"

**C1.1 `src/shard_directory.rs`, `ShardDirectory::shutdown` (375-413).** It carries `#[expect(clippy::excessive_nesting, …)]`, which is ratcheted on `scope_lines`/`syntax_facts`. The scope grows, so the reason is re-decided (below). The nesting that fulfils the expectation stays exactly as it is: the `return if failures.is_empty() {…} else {…}` stays inside `if opens == 0 && pending == 0 {` inside `loop`. The expectation therefore stays fulfilled.

```rust
    /// Stops admission and observes the same retirement owners on every call.
    /// A deadline cancels observers only; late opens and database closes remain
    /// fenced in the gate until their owners establish termination. A close
    /// that failed is settled, not pending: its owner keeps the fence and the
    /// readiness failure, and no wait turns it into a close, so the stop
    /// answers at once with the joined reports that name it (item 38).
    #[expect(
        clippy::excessive_nesting,
        reason = "ShardDirectory::shutdown; the drain nests the joined-report verdict inside the settled branch of the wait loop, where a failed close counts as settled; flattening it would separate the verdict from the drain it concludes"
    )]
    pub(crate) async fn shutdown(&self, grace: Duration) -> Result<(), String> {
        … unchanged through `.await;` (join_all) …
            let failures: Vec<_> = reports.into_iter().filter_map(Result::err).collect();
            let pending = engines
                .iter()
                .filter(|engine| !engine.terminated() && engine.failure().is_none())
                .count();
            if opens == 0 && pending == 0 {
                return if failures.is_empty() {
                    Ok(())
                } else {
                    Err(failures.join("; "))
                };
            }
            if tokio::time::Instant::now() >= deadline {
                let joined: String = failures.iter().map(|failure| format!("; {failure}")).collect();
                return Err(format!(
                    "shutdown ongoing or failed: {opens} opens, {pending} engines; owners retained{joined}"
                ));
            }
        … sleep_until unchanged …
```

**C1.2 `src/shard/lifecycle.rs`, in `impl EngineTasks` next to the other `#[cfg(test)]` methods.** No expectation-carrying function is touched. Because the new item carries its own `#[cfg(test)]`, the planner classifies the file as production-unchanged.

```rust
    /// Tests only: the engine's storage close FAILS. The fault store cannot
    /// produce this outcome (SlateDB retries its faults, and `close_db`
    /// settles Clean and Fenced), so the proof of what a failed close means
    /// to the directory installs one. The roles are aborted and joined as a
    /// real close would, and the database stays open for the test to close.
    #[cfg(test)]
    pub(super) fn begin_failed_close_for_test(&self, error: &'static str) {
        self.supervisor
            .begin_shutdown_with(Duration::ZERO, "storage-close", async move {
                TaskResult::Failed(error.into())
            });
    }
```

This is sound because `begin_shutdown_with` is single-flight. The roles ignore cancellation, so at `Duration::ZERO` they are aborted, and their `RequiredExit` guards call `engine.begin_close()`. That call's own `begin_shutdown_with` finds the completion already installed, so the real close future is dropped unrun. The report is `[<roles>: Cancelled…, ("storage-close", Failed("scripted close failure"))]`, the phase stays `ShuttingDown`, and `failure()` is `Some`. The test reaches `engine.tasks` because `shard::task_lifecycle_tests` is a descendant of `shard`.

**C1.3 `src/shard/task_lifecycle_tests.rs`:** add `serving`, T8 and T9 (§3.1). Existing tests are not edited, so their expectations' scopes are unchanged.

**C1.4 `scripts/quality/mutation_owners.py`:** change `owner('shard_directory', 'src/shard_directory.rs', 'shard_directory::')` to `owner('shard_directory', 'src/shard_directory.rs', 'shard_directory:: shard::task_lifecycle_tests::')`.

### C2: "The ordered stop is its own function, verbatim"

**C2.1 `src/bootstrap.rs`, `run`.** Replace lines 903-919:

```
    // PR 6-F / 6.1-A: the accept loop returned because a stop was requested.
    ordered_stop(&tasks, &shards, served).await
```

Add this function after `run` (the moved lines are byte-identical apart from the indentation that the function body gives them):

```rust
/// The ordered stop, once the accept loop has returned because a stop was
/// requested (PR 6-F / 6.1-A): its connections are already gone. Every
/// supervised loop is cancelled, joined and reported, then the shards close
/// (WP-15 §9 sequences admission, engines and stores ahead of this in its
/// remaining slice). The listener's own failure is reported last.
async fn ordered_stop(
    tasks: &crate::tasks::TaskSupervisor,
    shards: &crate::shard_directory::ShardDirectory,
    served: std::io::Result<()>,
) -> anyhow::Result<()> {
    let report = tasks.shutdown(std::time::Duration::from_secs(10)).await;
    tracing::info!(
        finished = ?report.finished(),
        aborted = ?report.aborted,
        panicked = ?report.panicked(),
        "supervised loops stopped"
    );
    shards
        .shutdown(std::time::Duration::from_secs(10))
        .await
        .map_err(anyhow::Error::msg)?;
    served?;
    Ok(())
}
```

**Ratcheted expectations on `run` (six, all function-wide).**
- `run` loses 15 lines and about 20 syntax facts, so `too_many_lines`, `cast_possible_truncation`, `let_underscore_must_use` and `excessive_nesting` only shrink and stay fulfilled. The moved lines contain no cast, `let _`, unwrap, expect, `json!` or `select!`. The `macro-dsl` rows `crate::run` `serde_json::json`/`tokio::select` stay where they are.
- `unwrap_used` and `expect_used` gain two new fingerprints: `…:ordinary-call` and `…:path` for `ordered_stop`. Both reasons are re-decided:
  - `expect_used`: `"run; covers exactly one site, the maintenance-worker spawn, and none in the ordered stop it hands off: the runtime's task supervisor is fresh at boot, so it accepts that worker; a fallible spawn would leave the process serving without maintenance"`
  - `unwrap_used`: `"run; covers exactly four sites, the shared-cache lock and the three auth file paths, and none in the ordered stop it hands off: a poisoned cache lock at boot would mean a half-built shared cache, and those paths were validated by the CLI parser before boot began; recovering the former or re-checking the latter would boot on state the parser already rejected"`

  Each has exactly two `;` and no `"`.

**C2.2 `src/bootstrap/tests.rs`:** `idle_directory`, `polite`, T6 and T7 (§3.2).

### C3: "A critical loop's exit stops the process root and fails the process"

**C3.1 `src/tasks.rs`.**

1. Module doc (after line 10): add *"The process's own supervisor (`TaskSupervisor::process_root`) also answers for a critical loop that exits while it runs. Nothing else would stop a runtime that can no longer serve, so it requests its own ordered stop and names the loop as the cause (item 38)."*
2. Lines 29-31 become:
   ```
       /// The runtime cannot serve without it (fleet loop, telemetry drain,
       /// the unready watchdog): an unexpected exit fails readiness, and under
       /// the process root it stops the runtime and fails the process.
   ```
3. After `TaskOutcome`:
   ```rust
   /// The first critical loop that exited while its process-root runtime was
   /// running, and how it ended. It is the cause of the stop the supervisor
   /// then requested, and so of the process's failure exit.
   #[derive(Clone, Debug, PartialEq, Eq)]
   pub(crate) struct CriticalExit {
       pub name: &'static str,
       pub outcome: TaskOutcome,
   }
   ```
   The `pub` fields follow `TaskStatus`.
4. After `struct Supervised`:
   ```rust
   /// Who answers for a critical loop that exits while its runtime runs.
   #[derive(Clone, Copy, Debug, PartialEq, Eq)]
   enum ExitAuthority {
       /// The supervisor's owner: an engine closes itself through its
       /// required-exit fence; a test rig reports the exit as readiness.
       Owner,
       /// The supervisor itself, at the process root (see `Inner::exited`).
       Supervisor,
   }
   ```
5. `SupervisorState` gains `/// Set once, by the critical exit that moved the phase out of Running; a requested stop leaves it empty.` `stop_cause: Option<CriticalExit>,`. `Inner` gains `exits: ExitAuthority,`.
6. `impl Inner` gains two methods:
   ```rust
       /// The one Running-to-ShuttingDown transition, then the cancellation
       /// every loop observes. A termination signal requests it without a
       /// cause. The process root requests it with the critical exit that
       /// forced it, and only the request that makes the transition records
       /// its cause. Returns whether this request made the transition.
       #[expect(
           clippy::unwrap_used,
           reason = "Inner::request_stop; a poisoned phase may reflect incomplete registration or a half-recorded stop cause; silent recovery could abandon a task outside the shutdown drain or name a cause for a stop it did not start"
       )]
       fn request_stop(&self, cause: Option<CriticalExit>) -> bool {
           let started = {
               let mut st = self.state.lock().unwrap();
               let started = st.phase == Phase::Running;
               if started {
                   st.phase = Phase::ShuttingDown;
                   st.stop_cause = cause;
               }
               started
           };
           self.cancel_tx.send_replace(true);
           started
       }

       /// The process root answers a critical loop's exit itself: nothing else
       /// would ever stop a runtime that can no longer serve. Only the exit
       /// that moves the phase out of Running is a cause. A loop that ends
       /// because a stop was already requested is that stop's consequence.
       /// Any other supervisor's owner answers for its own loops.
       fn exited(&self, name: &'static str, policy: Policy, ended: &std::thread::Result<TaskResult>) {
           if policy != Policy::Critical || self.exits != ExitAuthority::Supervisor {
               return;
           }
           let outcome = outcome_of(ended);
           if self.request_stop(Some(CriticalExit { name, outcome: outcome.clone() })) {
               tracing::error!(
                   task = name,
                   outcome = ?outcome,
                   "critical task exited while the runtime was running; requesting the ordered shutdown"
               );
           }
       }
   ```
7. Constructors. `new()` becomes `Self::with_exits(ExitAuthority::Owner)`. Add:
   ```rust
       /// The process's own supervisor: a critical loop's unexpected exit
       /// requests its ordered stop and fails the process (item 38). Engines
       /// and rigs build theirs with `new`, whose owner answers instead.
       pub(crate) fn process_root() -> Self {
           Self::with_exits(ExitAuthority::Supervisor)
       }
   ```
   `fn with_exits(exits: ExitAuthority) -> Self` holds the old `new` body, plus `stop_cause: None` and `exits`.
8. Add:
   ```rust
       /// Why the process root stopped its runtime on its own, if it did: the
       /// first critical loop that exited while the runtime was running. A
       /// requested stop has no cause.
       #[expect(
           clippy::unwrap_used,
           reason = "TaskSupervisor::stop_cause; a poisoned supervisor state may hold a half-recorded phase or cause; recovering it could report a stop without its cause or a cause without its stop"
       )]
       pub(crate) fn stop_cause(&self) -> Option<CriticalExit> {
           self.inner.state.lock().unwrap().stop_cause.clone()
       }
   ```
9. `spawn` (346-382). The spawn statement becomes:
   ```rust
           let handle = tokio::spawn(observed(
               Arc::downgrade(&self.inner),
               label,
               policy,
               build(self.inner.cancel.clone()),
           ));
   ```
   **Two ratcheted expectations cover `spawn`.** The statement-level `disallowed_methods` expectation's smallest enclosing item is `spawn`. Scope lines and facts grow, and there are new call and path fingerprints (`observed`, `Arc::downgrade`), so both reasons are re-decided:
   - `unwrap_used`: `"Supervisor registration; a poisoned phase may contain an incomplete task insertion or a half-recorded stop cause; recovering and spawning again could leave a task outside the eventual drain or its end unanswered"`
   - `disallowed_methods`: `"TaskSupervisor worker owner; the registration lock retains each handle before shutdown can take the map, and every worker reports its own end back to this owner; spawning through another supervisor would recursively delegate this canonical owner"`

   The `effect` row `crate::TaskSupervisor::spawn tokio::spawn 1` is unchanged: still one `tokio::spawn` path. The primitive-spawn check passes, because the qualified owner is unchanged.
10. `cancel` (384-399) becomes `self.inner.request_stop(None);`. Its `#[expect(clippy::unwrap_used, reason = "TaskSupervisor cancellation; …")]` would be **unfulfilled**, which is a denied lint, so it is **deleted**. The poison contract is kept: `request_stop` locks with `unwrap` before it publishes, so `cancellation_refuses_poisoned_registration_state` still sees a panic and no published cancellation.
11. Free functions, before `#[cfg(test)] mod tests;`:
    ```rust
    /// How a loop ended, as its supervisor reports it: the join report's own
    /// classification, read before the join.
    fn outcome_of(ended: &std::thread::Result<TaskResult>) -> TaskOutcome {
        match ended {
            Ok(TaskResult::Done) => TaskOutcome::Finished,
            Ok(TaskResult::Failed(error)) => TaskOutcome::Failed(error.clone()),
            Err(panic) => TaskOutcome::Panicked(shutdown::panic_message(&**panic)),
        }
    }

    /// Every supervised loop ends through here. Its end is the one moment its
    /// supervisor can still answer for it while the runtime runs (see
    /// `Inner::exited`). The join handle then carries the same result, or the
    /// same panic, into the shutdown report.
    async fn observed(
        supervisor: Weak<Inner>,
        name: &'static str,
        policy: Policy,
        task: impl std::future::Future<Output = TaskResult>,
    ) -> TaskResult {
        use futures_util::FutureExt;
        let ended = std::panic::AssertUnwindSafe(task).catch_unwind().await;
        if let Some(inner) = supervisor.upgrade() {
            inner.exited(name, policy, &ended);
        }
        match ended {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
    ```

    - **`Weak`, not `Arc`:** a strong edge from each task back to the supervisor would make a failed-to-start runtime immortal, which the module doc forbids.
    - **`catch_unwind` then `resume_unwind`, not a `Drop` guard.** A guard's `Drop` would lock the state during unwinding, and on a poisoned lock that is a double panic (abort) that skips the ordered stop. `resume_unwind` does not run the panic hook again (one stderr line, as today), and tokio still records `JoinError::Panic` with the same payload.
    - **Owner supervisors** return before touching the lock, so engines get no new lock traffic.
    - **Clippy:** `needless_pass_by_value` does not fire on async-fn parameters (compare `bootstrap/rss.rs::run`); four arguments; nesting 2.

**C3.2 `src/tasks/shutdown.rs`.** Factor the panic message out of `classify`. `classify` has no expectation; `drive_shutdown` (which has `let_underscore_must_use`) is not touched.

```rust
        Err(e) if e.is_panic() => TaskOutcome::Panicked(panic_message(&*e.into_panic())),
```

```rust
/// A panic as both of the supervisor's reports name it: the text of a
/// literal or formatted `panic!`, else only that it panicked. The join
/// report and the process root's stop cause read the same payload, so they
/// cannot disagree.
pub(super) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panic".to_string())
}
```

The parameter is typed `&(dyn Any + Send)` so a `&Box<dyn Any>` can never be downcast by mistake. The callers pass `&*payload` or `&**panic`.

**C3.3 `src/tasks/tests.rs`:** T1-T4 plus the helpers `root_exit_case`, `polite_loop` and `spawn_racer` (§3.3). The file is `#![cfg(test)]`, so it is production-unchanged for the planner.

**C3.4 `src/bootstrap.rs`.**
- Line 295 becomes `let tasks = crate::tasks::TaskSupervisor::process_root();`. The line count and fact count of `run` are unchanged, so the four size-only expectations are unaffected. The call and path fingerprint changes (`…::new` → `…::process_root`), so `unwrap_used` and `expect_used` are re-decided again:
  - `expect_used`: `"run; covers exactly one site, the maintenance-worker spawn, and none in the ordered stop it hands off: the runtime's process-root supervisor is fresh at boot, so it accepts that worker; a fallible spawn would leave the process serving without maintenance"`
  - `unwrap_used`: `"run; covers exactly four sites, the shared-cache lock and the three auth file paths, none under the process-root supervisor or in the ordered stop: a poisoned cache lock at boot would mean a half-built shared cache, and those paths were validated by the CLI parser before boot began; recovering the former or re-checking the latter would boot on state the parser already rejected"`

  No comment lines are added inside `run`, because comments count toward `scope_lines`.
- `ordered_stop`: the doc gains the item-38 sentence, and the tail becomes:
  ```rust
      let closed = shards.shutdown(std::time::Duration::from_secs(10)).await;
      // Item 38: a stop the process root requested for a critical loop's exit
      // fails the process, after the same ordered sequence. The cause outranks
      // what that stop then failed to close, which is still logged.
      if let Some(cause) = tasks.stop_cause() {
          if let Err(error) = &closed {
              tracing::error!("shard close after the critical exit: {error}");
          }
          anyhow::bail!(
              "critical task {} exited while the runtime was running: {:?}",
              cause.name,
              cause.outcome
          );
      }
      closed.map_err(anyhow::Error::msg)?;
      served?;
      Ok(())
  ```
  `tracing::`/`anyhow::` macros are exempt from macro-dsl.

**C3.5 `src/bootstrap/tests.rs`:** add T5. In T7, `TaskSupervisor::new()` becomes `TaskSupervisor::process_root()`.

**C3.6 Documentation ledgers:** see §6.

### Every ratcheted function touched, and the remedy

| Function | Expectation(s) | Commit | Remedy |
|---|---|---|---|
| `ShardDirectory::shutdown` | `excessive_nesting` (size) | C1 | reason re-decided; nesting structure kept (still fulfilled) |
| `bootstrap::run` | `unwrap_used`, `expect_used` (fingerprints) + 4 size-only | C2 | two reasons re-decided; the other four only shrink |
| `bootstrap::run` | same | C3 | two reasons re-decided again; size unchanged |
| `TaskSupervisor::spawn` | `unwrap_used` (fingerprints), `disallowed_methods` (size) | C3 | both re-decided |
| `TaskSupervisor::cancel` | `unwrap_used` | C3 | **deleted** (unfulfilled) |
| `Inner::request_stop`, `TaskSupervisor::stop_cause` | new `unwrap_used` | C3 | new, reasoned `owner; invariant; alternative` |
| `a_failed_storage_close_ends_the_directory_stop_at_once` | new `let_underscore_must_use` | C1 | new, reasoned |
| untouched and verified unaffected | `TaskMonitor::unready_reason`, `Inner::snapshot`, `Inner::phase`, `launch_shutdown`, `observe_shutdown`, `incomplete_shutdown_failure`, `drive_shutdown`, `EngineTasks::{required, failure, failed}`, `OpenGate::*`, the r17a tests | – | – |

Import aliases: none of the moved or new code goes through a `use` alias, and no call moves between modules, so there are no re-fingerprinted alias callers. The multitenancy audit is unaffected: no `stream_hash(`, registry bare-name, tenant-fallback or internal-target text is added, moved or edited.

---

## 5. Mutation-kill analysis

Owners selected by the diff:
- **C1:** `shard_directory`; `task_lifecycle_tests` (0 mutants, because its `mod` is `#[cfg(test)]`, which is reported explicitly); `shard_lifecycle` (production-unchanged, or 0 mutants because the new item is `#[cfg(test)]`).
- **C2:** `bootstrap`; `bootstrap_tests` is `#![cfg(test)]`, so it is omitted.
- **C3:** `tasks`, `tasks_shutdown`, `bootstrap`. `src/tasks/tests.rs` is `#![cfg(test)]`, so it is omitted and needs no owner row.

No new file is added under a critical prefix, so no new owner row is needed. The one row change is the `shard_directory` filter (C1.4).

| Mutant (cargo-mutants 27.1.0) | Killing test (filter) |
|---|---|
| `shutdown` pending filter `&&`→`\|\|` | T8 (failed engine counted pending, so the 5 s bound fires); `r17a_runtime_shutdown_…` final `.unwrap()` (a terminated engine is counted pending, so it waits the 10 s grace and returns Err) — `shard::task_lifecycle_tests::` |
| delete `!` before `engine.terminated()` | T9 (a held engine is not pending, so it returns `"engine shutdown still running; …"` without the prefix, and `assert_eq` fails) |
| `ShardDirectory::shutdown` → `Ok(())` | T8, T9 (`unwrap_err`) |
| → `Err(String::new())` / `Err("xyzzy".into())` | T9 and r17a final `.unwrap()`; T8 `contains` |
| `ordered_stop` → `Ok(())` | T6 (C2), T5 (C3) — `bootstrap::` |
| `ordered_stop` → `Err(anyhow!("mutated!"))` | T7 |
| `run` → `Ok(())` / `Err(anyhow!("mutated!"))` (the body is in-diff) | `process_bootstrap_cannot_be_an_empty_success` (expects Err containing `starts process infrastructure once`) |
| `TaskSupervisor::process_root` → `Default::default()` (an `Owner` supervisor) | T1 (Elapsed) — `tasks::` |
| `TaskSupervisor::new` / `with_exits` → `Default::default()` | infinite recursion (`Default` → `new` → …), so every test aborts on stack overflow: caught, not a timeout |
| `TaskSupervisor::stop_cause` → `None` | T1 |
| `TaskSupervisor::cancel` → `()` | `cancel_closes_registration_before_the_join` (phase stays Running) |
| `Inner::request_stop` → `true` / `false` | `cancel_closes_registration_before_the_join`; T1 |
| `request_stop` `==`→`!=` | `cancel_closes_registration_before_the_join`; T1 |
| `Inner::exited` → `()` | T1, T3 |
| `exited` `policy != Critical` → `==` | T1 (the critical case returns early); T2 (noncritical escalates) |
| `exited` `exits != Supervisor` → `==` | T1; T2 (the `new()` supervisor escalates) |
| `exited` `\|\|`→`&&` | T2 (root + noncritical, and owner + critical, both escalate) |
| `shutdown::panic_message` → `String::new()` / `"xyzzy".into()` | T1 panic case (`Panicked("critical panic")`) |
| `classify`: delete arm `Err(e) if e.is_panic()` (a wildcard `Err(_)` exists; the arm's lines are in-diff) | `critical_exits_failures_and_panics_are_reported` (`panicked() == ["boom"]`); T1 report assertion |
| `outcome_of` → `Default::default()`, `observed` → `Default::default()` | unviable (`TaskOutcome` and `TaskResult` have no `Default`): reported as unviable, not missed |

There is no equivalent mutant. `ExitAuthority` has two variants, and both comparison directions are killed by distinct tests (T1 and T2).

Every wait in the new tests is bounded (1 s, 5 s, 10 s), so no mutant can turn into a TIMEOUT. The slowest surviving-until-assert mutant (`&&`→`||`) makes the r17a final stop and T9's final stop wait their 10 s grace, well inside the 90 s per-mutant budget.

**Synchronization proof (item-48 rule, "Loom or held-commit test").** The transition is one critical section under the existing `SupervisorState` mutex, shared with `spawn`, `cancel` and `launch_shutdown`.
- T3 is the held interleaving: an exit first, then its consequence and a late request.
- T2's requested-stop leg is the other order: a request first, then an exit.
- T4 explores exits racing a request on a 4-worker runtime.

There is no Loom model. `tasks.rs` uses `std::sync::Mutex` and `tokio::sync::watch` without a Loom shim (Loom exists only for `touch.rs` and `commit_handoff.rs`). See decision D3.

---

## 6. Ledgers

- **`scripts/quality/mutation_owners.py` (C1):** `shard_directory` filters `'shard_directory:: shard::task_lifecycle_tests::'`.
- **`docs/refactor/test-inventory.json`:** unchanged. The inventory scans only `src/dst`, and no DST test is added or edited. `python3 scripts/test-inventory.py --check` stays OK.
- **`docs/refactor/review-mechanisms.json` (C3):** no re-pin is required, because both pinned tests in `critical-runtime-exit` (`readiness_endpoint_refuses_each_permanent_critical_exit`, `critical_exits_failures_and_panics_are_reported`) are byte-unchanged. Recommended update to the same mechanism:
  - `limitations`: from "Process drain/restart policy in live fleet remains pending." to "Process policy landed (item 38): the process root requests the ordered stop on the first critical exit and the binary exits 1 naming it; the platform restart behind deploy/app-server/supervise.ts remains pending (item 39)."
  - Append `{"file": "src/tasks/tests.rs", "name": "a_critical_exit_stops_the_process_root_and_names_its_cause", "sha256": <function_sha256>}`, computed with:
    ```
    python3 - <<'EOF'
    import importlib.util, pathlib
    s = importlib.util.spec_from_file_location('inv', 'scripts/test-inventory.py'); m = importlib.util.module_from_spec(s); s.loader.exec_module(m)
    p = pathlib.Path('src/tasks/tests.rs')
    print([f['function_sha256'] for f in m.functions(p.read_text(), p) if f['name'] == 'a_critical_exit_stops_the_process_root_and_names_its_cause'])
    EOF
    ```
    Then run `python3 scripts/review-evidence.py --check`.
- **`docs/refactor/WIRE-MATRIX.md` §3 "Health" (line 194), C3:** fix the stale cite `src/http.rs:2066-2124` to `src/http.rs:1845-1906`. Add the missing supervisor 503s (`runtime shutting down` / `runtime stopped` / `runtime supervisor unavailable`, and `critical task terminated: <name>` under a non-root supervisor such as a test rig). Add: "Under the binary's process-root supervisor a critical loop's exit instead requests the ordered stop: the listener closes, loops and shards close, and the process exits 1 naming the loop (item 38)."
- **`docs/review-runtime-evidence.md` §R17 (optional, one sentence):** the process-root policy and its tests.
- **`docs/quality/owners.json`:** unchanged. No new `tokio::spawn`, `tokio::select!`, `json!`, static, by-path module or `use super::*` in a new file.
- **`docs/quality/source-allowances.json`:** unchanged; no prune (nothing becomes obsolete).
- **`docs/refactor/architecture-policy.json`:** unchanged. There is no new file; `bootstrap/tests.rs` is test-only and `bootstrap.rs` is the composition root.
- **`src/dst/tests/README.md`:** unchanged; no DST module.
- **`scripts/mt-audit-baseline.txt`:** unchanged; see §4.

---

## 7. Controls

Put Python ≥ 3.11 on PATH first (the `python3` shim) and build `streams-quality-syntax` (`cargo build --locked -p streams-quality-syntax`).

1. **C1 red.** Apply C1.2 and C1.3 only, then:
   `cargo test --locked --lib -- --exact shard::task_lifecycle_tests::a_failed_storage_close_ends_the_directory_stop_at_once shard::task_lifecycle_tests::a_directory_stop_past_its_grace_carries_the_joined_reports`
   Expected: `2 failed`, with the §3.1 messages (T8 after about 5 s). Save as `scratchpad/item38-c1-red.log`.
2. **C1 green, floored:**
   `scripts/test-leg.sh target/quality/item38-c1.log --exact shard::task_lifecycle_tests::a_failed_storage_close_ends_the_directory_stop_at_once --exact shard::task_lifecycle_tests::a_directory_stop_past_its_grace_carries_the_joined_reports -- --locked --lib shard::task_lifecycle_tests::`
   Expected: all `ok`, including the six r17a tests. Then `cargo test --locked --lib shard_directory:: dst::dst_tests::runtime_engine_lifecycle::` shows no change.
3. **C2:** `cargo test --locked --lib bootstrap::` gives 4 passed (the two old tests plus T6 and T7).
4. **C3 red** (the staged shape of §3.3 on top of C2):
   `cargo test --locked --lib -- --exact tasks::tests::a_critical_exit_stops_the_process_root_and_names_its_cause tasks::tests::the_first_critical_exit_is_the_cause_and_later_exits_its_consequences bootstrap::tests::a_critical_exit_ends_the_accept_loop_and_fails_the_ordered_stop`
   Expected: `3 failed`, with the three `Elapsed(())` messages. Save as `scratchpad/item38-c3-red.log`. Then discard the staging.
5. **C3 green, floored:**
   `scripts/test-leg.sh target/quality/item38-tasks.log --exact tasks::tests::a_critical_exit_stops_the_process_root_and_names_its_cause --exact tasks::tests::only_an_unrequested_critical_exit_stops_the_process_root --exact tasks::tests::the_first_critical_exit_is_the_cause_and_later_exits_its_consequences --exact tasks::tests::racing_exits_and_a_request_settle_on_one_stop -- --locked --lib tasks::`
   Then `scripts/test-leg.sh target/quality/item38-boot.log --exact bootstrap::tests::a_critical_exit_ends_the_accept_loop_and_fails_the_ordered_stop --exact bootstrap::tests::a_requested_stop_ends_in_success -- --locked --lib bootstrap::`
   Expected: every result `ok`, each named test on its own `… ok` line. `tasks::` now includes the signal child-process test.
6. **Neighbours that must not move:** `cargo test --locked --lib dst::dst_tests::review_readiness:: dst::dst_tests::runtime_engine_lifecycle:: http::serve:: auth_feed:: dst::dst_tests::billing_controller:: dst::dst_tests::scaler_controller:: dst::dst_tests::fleet_controller::` gives all `ok`. These are the rigs and controllers that build their own `TaskSupervisor::new()`.
7. **Gate on each commit:** `scripts/quality.sh`. Expected: `quality ratchets: OK`; no `accepted exception grew without a new decision` (the re-decided identities are new, so they are skipped); no `exception needs owner; invariant; alternative`; no `file growth`; `architecture-gate: OK`; rustdoc `-D warnings` clean (docs use backticks only, no bracket links and no `<`); mt-audit OK.
8. **CI's own selection before the push** (all three commits committed):
   `QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan`
   Expected `plan.json`:
   - `mutation_source_files` ⊇ `src/shard_directory.rs`, `src/bootstrap.rs`, `src/tasks.rs`, `src/tasks/shutdown.rs`, `src/shard/task_lifecycle_tests.rs`
   - `production_unchanged_files` ⊇ `src/tasks/tests.rs`, `src/bootstrap/tests.rs` (and `src/shard/lifecycle.rs` if the planner erases the `#[cfg(test)]` impl item)
   - `unregistered_mutation_source_files: []`, `mutants: true`
9. **Mutation run:** `QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate scripts/quality/mutations.sh`
   Expected: zero `MISSED` and zero `TIMEOUT`. The only unviable mutants are `outcome_of` and `observed`. The `new`/`with_exits` recursion mutants show as caught. Save the log to `scratchpad/mutants-item38.log`.
10. **Field control** (optional; a local binary on a scratch store): build the server with a scratch `RUST_LOG=info`, and inject a critical exit by temporarily arming a panicking loop (a local-only patch, never committed; `ROLLUP=1` against a store that refuses the rollup DB open is the no-patch variant).
    - Expected: one `critical task exited while the runtime was running; requesting the ordered shutdown` error line, then `supervised loops stopped`, then the process exits with status 1 and `Error: critical task usage-rollup exited while the runtime was running: Failed("usage rollup open failed: …")`.
    - Before the fix, the same run stays up with `/health` 503 `critical task terminated: usage-rollup` and `/v1/streams/…` still 2xx.
11. **After the push:** `gh run list --branch slate --limit 5 --json databaseId,headSha,status,conclusion`, match the sha, then `gh run view <id>`. Never claim green from memory.

---

## 8. Out of scope / follow-ups

- **Item 39 (deploy wrapper).** Behind `deploy/app-server/supervise.ts:39-72`, a process exit becomes a held unauthenticated 500 diagnostic, not a restart. Until item 39 lands, item 38 turns a 503 zombie into a 500 zombie that serves its stderr tail. See decision D2.
- **Remaining `/health` predicates the exit watchdog still does not watch:** auth feeds unpublished (`http.rs:1849-1871`), and billing prerequisites in required mode (opened synchronously at boot, so a 503 there is a startup-order bug). A dead `auth-refresher` is now covered by this item; a live refresher that never publishes is not.
- **`spawn_unready_watchdog` still calls `std::process::exit(1)`.** Its expiry could now request the ordered stop instead. That would change the exit path (engines closed, loops joined) and needs its own review.
- **A critical exit that races boot registration.** Between `request_work.start` and the `rss-sampler` registration (`bootstrap.rs:798-805`, `.map_err(…)?`), `run` would return `registering RSS sampler: ShuttingDown` without the ordered stop. The window is microseconds and the process still exits 1, but the error names the registration, not the cause. The follow-up is to make that registration non-fatal, or to check `stop_cause` on that path.
- **In-flight graceful drain.** Connections are still aborted at once on any stop (WP-15 §9, item 37's note).
- **Item 37** (handler panics and poisoned process-wide locks invisible to readiness) is separate.
- **`TaskMonitor::critical_failure` / `debug_load` show `null` after an escalation.** Moot, because the listener is closed.
- **The deadline message's joined reports include `engine shutdown still running; join authority retained`** for engines still closing. This is accurate but redundant with `owners retained`. It could be filtered to finalized failures (`EngineShutdown::failure`) if Søren prefers a terser message.

---

## Skeptic corrections (C1..C12)

I checked every claim against HEAD `fba5af56`; `git diff 0afa2597..HEAD -- src` is empty. I only read the repository. The problem statement holds: `tasks.rs:253-274`, `sharddir.rs:155-183`, `http.rs:1845-1848`, `shard_directory.rs:375-413`, `tasks/shutdown.rs:78-89` and `shard/lifecycle.rs:98-104` are quoted correctly. The design choice is sound: only the binary gets `process_root`, and rigs and engines keep `new()`. `bootstrap.rs:733-747` is the only production requester. The traces below agree with the plan: T1, T3, T5, T8 and T9 are red as stated. T2, T4, T6 and T7 are guards. The sha-pinned R17 tests keep their bytes.

**C1 (gate failure, missed ledger): `std::thread::Result` is an *effect* to the source gate.** `scripts/quality/source_rules.py:44-46` classifies every `path` fact that starts with `std::thread::` as `effect`. `tools/quality-syntax/src/scan.rs:222-225` emits a `path` fact for every `syn::Path`, and that includes type paths in signatures. `docs/quality/owners.json:451-457` shows `std::thread::sleep` registered as an effect. So `Inner::exited(…, ended: &std::thread::Result<TaskResult>)` (plan line 820) and `outcome_of(ended: &std::thread::Result<TaskResult>)` (plan line 876) each add one identity: `('effect','src/tasks.rs','crate::Inner::exited','std::thread::Result')` and `('effect','src/tasks.rs','crate::outcome_of','std::thread::Result')`. Neither is registered, so `source_rules.py:319-323` fails the gate with `unregistered source occurrence (1): …` twice. §6 "owners.json: unchanged" is wrong as written. Fix: write the type as `Result<TaskResult, Box<dyn std::any::Any + Send>>` in both signatures. Callers still pass `&**panic` or `&*payload` to `panic_message`. With that change, owners.json really stays unchanged. The other fix is two owners.json effect rows with reasons, and I do not recommend it.

**C2 (missing contract decisions): D1, D2 and D3 are cited but never written.** §2.4 cites "decisions D1, D2", and §5 and §8 cite D3. The plan has no Decisions section. Add one for Søren to approve, with each decision's backward-compatible alternative:
- **D1:** under the binary's supervisor, a critical exit stops the process, which exits 1 naming the loop. The alternatives are §2.5 "status quo plus a delay" and "opt-in env knob".
- **D2:** order relative to item 39. `deploy/app-server/supervise.ts:39-72` (verified; three copies under `deploy/*/supervise.ts`) turns the exit into a held 500 `binary_exited`. Its `hint` says "check required env vars and that the binary is x86_64", which is wrong for this cause. See also `docs/READINESS.md:51-53`.
- **D3:** no Loom model. T3 is the held interleaving and T4 is the race.
- **D4 (new, at the edge):** with `ROLLUP=1` and `BILLING_MODE≠required`, a rollup DB open failure (`billing.rs:1397-1399`) used to leave the instance serving appends and reads behind a 503 `/health`. Now the process exits 1. This is the plan's own field-control scenario (§7.10), so it must be approved explicitly and not hidden inside "a real failure" (§1.4).

**C3 (boot window: the plan understates a new stranding path).** §8 calls the rss-sampler race "microseconds", but the more important effect is structural. Under `process_root`, any critical loop registered before `bootstrap.rs:798-805` (the signal, unready-watchdog, auth-refresher and scaler loops, plus request-maintenance) can end on another worker of the multi-thread runtime. When it does, the `?` on the rss-sampler registration returns from `run`, and every loop already started is left running. That breaks the PR 6.1-A invariant stated at `bootstrap.rs:290-293` and `bootstrap.rs:725-730` ("an early `?` never strands a running loop"). No code change inside `run` can be tested (ratchets, `RUN_WAS_INVOKED`), so list it as **D5**. It is either accepted as a follow-up with that wording, or fixed by making `ordered_stop` also run on that error path. Also note that `request_work.start(...).expect(...)` (`bootstrap.rs:714-718`) stays sound only because it is the **first** registration. The re-decided `expect_used` reason should say "first registration on a fresh process root", not only "fresh at boot".

**C4 (mutation table misses in-diff guard mutants).** C3.2 rewrites the whole `classify` arm `Err(e) if e.is_panic() => …` (`tasks/shutdown.rs:215`). cargo-mutants 27.1 therefore generates **guard→true** and **guard→false** mutants on that line. Add both rows:
- guard→true: a cancelled `JoinError` reaches `into_panic()`, and the driver task panics. `observe_shutdown` then returns the synthetic `shutdown-driver` report. `tasks::tests::aborted_tasks_are_destroyed_before_shutdown_returns` kills it, because it asserts `[("holder", Cancelled)]`.
- guard→false: panics are classified `Cancelled`. `critical_exits_failures_and_panics_are_reported` (`panicked()==["boom"]`) and T1's panic case kill it.

The "delete arm" row is most likely **not** generated, because `Err(_)` is not a bare `_` wildcard. Keep it only as "if generated". Also, §7.9 "The only unviable mutants are `outcome_of` and `observed`" is incomplete: `stop_cause → Some(Default::default())` is unviable too, because `CriticalExit` has no `Default`.

**C5 (unverified "caught, not a timeout" claim).** The `new`/`with_exits → Default::default()` mutants loop through `Default::default → new → with_exits → Default::default`. Nothing has exercised such a mutant before: `new()` last changed in `24cfdac0` (2026-09-06), before the mutation leg's first `mutation_owners.py` commit `99d5c098` (2026-09-13). Under `profile.quality` (opt-level 1) this should overflow the stack (SIGABRT, caught). Control §7.9 must confirm that explicitly by reading the `new`/`with_exits` lines of the mutants log. If either line shows TIMEOUT, CI fails. Record that as a stop condition. Do not "fix" it by pointing `Default` straight at `with_exits`: that makes `new → Default::default()` an equivalent mutant.

**C6 (wrong control count).** §7.3 expects `cargo test --locked --lib bootstrap::` to give "4 passed". The filter also selects `bootstrap::process_executor` (1 test) and `bootstrap::runtime_handoff` (2 tests), so there are 5 today and **7** after C2. Use `scripts/test-leg.sh target/quality/item38-c2.log --exact bootstrap::tests::the_ordered_stop_joins_the_loops_before_reporting_a_failed_listener --exact bootstrap::tests::a_requested_stop_ends_in_success -- --locked --lib bootstrap::`.

**C7 (a stale comment the plan leaves behind).** `src/sharddir.rs:144-147` says the watchdog "until WP-15 task supervision gives critical tasks a result policy — keeps the survival `process::exit`". After C3 that condition is met, but the watchdog covers shard unreadiness, not task exit. Reword it in C3 with the **same line count**: `spawn_unready_watchdog` carries a ratcheted `let_underscore_must_use` expect, and syn item spans include doc attributes, so extra doc lines grow `scope_lines`.

**C8 (missed doc ledger).** `RUNBOOK.md:489-495` is compiled into the operator page (`src/operator.rs:26`, `include_str!`). It tells operators that `binary_exited` "usually" means a missing env var or the wrong arch, and that a "single instance dies" case heals itself. Add a row: `stderrTail` shows `Error: critical task <name> exited while the runtime was running: …`, the meaning is item 38, and the action is to read the loop's error line and, until item 39, redeploy. Also reword `docs/review-runtime-evidence.md:23` so the "TaskMonitor reports unready after any critical task returns" sentence is scoped to owner (non-root) supervisors.

**C9 (bound every wait).** T9's two `directory.shutdown(..)` calls, and T5/T6/T7's `ordered_stop(..)` calls, are bounded only by the code under test. Wrap each in `tokio::time::timeout(Duration::from_secs(15), …)` with a named `expect`, so a mutant that breaks the deadline path fails and cannot TIMEOUT. With the plan's hunks the deadline `>=` line (`shard_directory.rs:403`) stays a context line, but git's diff heuristics decide that, not the plan.

**C10 (wrong ledger citation).** The `macro-dsl` rows for `crate::run` in `src/bootstrap.rs` (`serde_json::json`, `tokio::select`) live in `docs/quality/source-allowances.json:2664-2676`, not in owners.json. The rows that owners.json does have for `crate::run` are for `src/bootstrap/rss.rs`. Nothing needs to change there, because nothing moves; only the citation in §4.C2.1 is wrong.

**C11 (factual).** `src/registry.rs` at HEAD is **1,501** lines, not 1,509 (the task text is stale). It is untouched either way. The working tree also has **uncommitted** `M src/registry.rs` and `?? src/registry/failpoints.rs` from other work. Stage explicit paths only (`git add <files>`, never `-a`/`.`). Run §7.7/§7.8 where those changes cannot leak into the planner's diff or the gate's tracked sources, for example a scratch worktree at the commit.

**C12 (minor quote drift).** In the §1.3 excerpt, `sharddir.rs:182` is the closing `);` of `tracing::error!`. `std::process::exit(1);` is line 183.

**Checked and correct (no change):**
- The line budgets in §1.7, apart from registry: http 3,371, product 4,205, shard 3,232, billing 2,201, history 1,713, auth 1,676, sse/feed 1,200, fleet 1,143 are all untouched. bootstrap.rs 923 has a limit of max(1000, legacy 929) = 1,000.
- The ratcheted functions and re-decided reasons all have exactly two `;` and no `"`.
- Deleting `cancel`'s unfulfilled expect is right.
- `readiness_endpoint_refuses_each_permanent_critical_exit` is at `src/dst/review_readiness.rs` (module `dst::dst_tests::review_readiness`) and uses a `new()` rig.
- `test-inventory.json` scans `src/dst` only (`scripts/test-inventory.py:138`), so it is unchanged.
- The `shard_directory` owner really has no `ShardDirectory::shutdown` caller under `shard_directory::` (C1.4 is needed), and multi-filter rows have precedent (`mutation_owners.py:68-88`).
- `bootstrap/tests.rs` is `#![cfg(test)]`, so the architecture gate skips it even with the new `crate::http::serve_h1` reference.
- The primitive-spawn check still finds the `crate::TaskSupervisor::spawn` effect row (`owners.json:627-634`).
- `main` returns `run`'s `anyhow` error (`src/main.rs:15,58`).
- T8's hook is single-flight as described (`ShardEngine::begin_close` at `shard.rs:1876` → `EngineTasks::begin_close` → `launch_shutdown` finds `completion` already set). Its `unready_reason` assertion goes through `OpenGate::unready_reason`'s closing-failure branch (`sharddir.rs:417-431`), because `ShardHealth.engine_failure` has no writer.

**Verdict: ready-with-corrections.** C1 must be fixed, or `scripts/quality.sh` fails on the C3 commit. C2 must be fixed before Søren can approve anything, and D4 and D5 are new edge and invariant decisions. C3-C9 are small edits to the plan's text, tests and controls. None of them changes the architecture.
