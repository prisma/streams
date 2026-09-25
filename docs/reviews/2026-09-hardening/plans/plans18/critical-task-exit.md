# Items 38/39: a critical loop's exit ends the process within a hard bound, and the deploy wrapper replaces it; a failed engine close ends the directory stop at once

Repository `/Users/sorenschmidt/code/streams`, branch `slate`. Everything below was verified at HEAD `d4d631df`. While this plan was being written, `origin/slate` moved to `d255ad6d` (1d079e01 and d255ad6d touch only `.github/workflows/ci.yml`, `scripts/platform-e2e*.mjs` and `docs/reviews/`, and `git diff d4d631df d255ad6d -- src` is empty). Line numbers are the same at both commits. I edited no repository file and ran no cargo command.

**The main checkout has uncommitted changes from another session** (`src/http/serve.rs`, `docs/refactor/WIRE-MATRIX.md`, `docs/refactor/test-inventory.json`, `src/dst/tests/security_operations.rs`: the 401 `WWW-Authenticate` work). Implement this plan in its own worktree at `origin/slate` (`git worktree add ../streams-item38 origin/slate`) so none of that leaks into these commits. `git add -p` is not available.

**Drafts, measured.** The exact text of every Rust edit is in `scratchpad/p18cte/` (paths below). It was formatted with `rustfmt --edition 2024` in a mirror tree. It was measured with the repo's own scanner (`target/debug/streams-quality-syntax`, built from the current `tools/quality-syntax`) and `scripts/quality/source_rules.py`:
- `exception_growth` against HEAD reports **0 failures**. No `exception-growth.json` row and no reason edit is needed.
- The wrapper change and its Bun test were run with bun 1.3.13: red against the HEAD wrapper, green four times against the draft, and a non-vacuity control fails as expected.

`scratchpad/p18cte/contracts.py` and `scratchpad/p18cte/growth.py` reproduce those measurements.

---

## 0. Verdict and what changed since the stale plan (`plans6/critical-task-exit.md`)

All four defects are real on the current tree (§1). The stale plan cannot be used. It re-decides five exception reasons (forbidden since bcddd615/6a36e406) and adds a free-function call inside `run` (a new fingerprint, which is growth). Its mutex-based escalation also strands loops in a boot window (its D5). This plan's design:

1. **The process root is chosen by a method call.** `run` changes by exactly two method-call rewrites, and method calls are not fingerprinted:
   - `crate::tasks::TaskSupervisor::new()` becomes `crate::tasks::TaskSupervisor::new().process_root()`.
   - `tasks.shutdown(..)` plus the report log plus `shards.shutdown(..)` become `tasks.ordered_stop(<grace>, shards.shutdown(<grace>))`.

   Measured result: all six of `run`'s contracts shrink (`scope_lines` 582→578, `syntax_facts` 967→966), and no fingerprint key is added.
2. **Every supervised loop ends through a new `ExitWatch`**, which runs outside every excepted scope. The excepted registration body of `TaskSupervisor::spawn` keeps its exact text and attribute, and its reason is untouched, under the private name `register`. A new, unexcepted `spawn` wraps the builder and calls it. The gate pairs `register` with the vanished `spawn` contract as a rename. Every metric is less than or equal to before: `syntax_facts` 60→58, because `pub(crate)` is dropped. See decision D6.
3. **Escalation takes no lock.** It uses the cancellation watch and two `OnceLock`s. It does not move the phase, so registration stays open and the later `?`-registrations in `run` cannot strand a loop (the old D5 disappears). No new `unwrap` is written.
4. **The bound is a dedicated OS thread (`stop-deadline`),** armed at the moment the root requests its stop and never disarmed. It calls `process::exit(1)` after 30 s. It also bounds the runtime drop in `main`, which otherwise waits forever for blocking work.
5. **The directory fix fits inside `ShardDirectory::shutdown`'s `excessive_nesting` scope without growth.** A new `EngineShutdown::settled()` method replaces `terminated()` in the pending filter. The joined reports go into the existing `format!` string as `{failures:?}`.
6. **The deploy wrapper gets a death policy.** A child that dies after it accepted on `$PORT` ends the wrapper with its code, so Compute replaces the instance. A child that never accepted is still held and served as before.
7. **The lock-free stop transition gets a Loom model.** `stop_on_exit` is the actual production function, generic over its two cells, in the same style as `quota/pin.rs`.

---

## 1. Problem (verified)

### 1.1 A critical exit flips readiness for good: `src/tasks.rs:261-282`

```
265     #[expect(
266         clippy::unwrap_used,
...
269     pub(crate) fn unready_reason(&self) -> Option<String> {
270         let Some(inner) = self.inner.upgrade() else {
271             return Some("runtime supervisor unavailable".into());
272         };
273         let state = inner.state.lock().unwrap();
274         match state.phase {
275             Phase::ShuttingDown => Some("runtime shutting down".into()),
276             Phase::Stopped => Some("runtime stopped".into()),
277             Phase::Running => state.tasks.values().find_map(|task| {
278                 (task.policy == Policy::Critical && task.handle.is_finished())
279                     .then(|| format!("critical task terminated: {}", task.name))
280             }),
```

- `is_finished()` never goes back to false.
- The phase leaves `Running` only in `cancel` (`tasks.rs:417-425`) and `launch_shutdown` (`tasks/shutdown.rs:61`).
- So the verdict is permanent. `tasks::tests::readiness_changes_after_each_kind_of_critical_exit` pins it for `Done`, `Failed` and a panic.
- An unexpected `Ok` return counts: `Done` is a critical exit here.

### 1.2 Nothing but a signal requests the ordered stop

Complete list of stop requesters, from `grep -rn 'shutdown_request()\|\.request();\|\.cancel();' src` excluding tests:
- `src/bootstrap.rs:726`: `let request = tasks.shutdown_request();`
- `src/bootstrap.rs:735`: `request.request();`. It sits inside the `signal` task, in the `terminate.recv()` arm of its `tokio::select!` (`bootstrap.rs:724-741`).
- `src/tasks.rs:249`: `TaskSupervisor { inner }.cancel();`. This is `ShutdownRequest::request` itself.

`cancel` (`tasks.rs:417-425`) is the only `Running → ShuttingDown` move before the tail:

```
417     pub(crate) fn cancel(&self) {
418         {
419             let mut st = self.inner.state.lock().unwrap();
420             if st.phase == Phase::Running {
421                 st.phase = Phase::ShuttingDown;
422             }
423         }
424         self.inner.cancel_tx.send_replace(true);
```

The accept loop leaves only on that cancellation (`src/http/serve.rs:88` `let cancel = tasks.cancellation();`, `:92` `_ = cancel.cancelled() => break,`). `run`'s ordered stop (`bootstrap.rs:894-913`) runs only after `serve_h1` returns. So a critical exit never reaches the stop.

Every production reader of the supervisor's verdict:
- `http.rs:1634` in `health_axum`: `if let Some(reason) = state.tasks.unready_reason() {`
- `http.rs:994` in `debug_load`: `"critical_failure": state.tasks.critical_failure(),`

No request handler reads it. The zombie keeps serving appends and reads while `/health` answers 503.

### 1.3 The exit watchdog samples a narrower predicate: `src/sharddir.rs:151-189`

```
179                 let reason = directory.unready_reason();
180                 match window.observe(reason.is_some(), clock.monotonic(), limit) {
181                     WatchdogDecision::Expired { elapsed } => {
...
189                         std::process::exit(1);
```

It never consults `tasks.unready_reason()`, which `health_axum` checks **first** (`http.rs:1633-1636`). Its doc comment (`sharddir.rs:151-154`) is stale: it says the watchdog keeps the survival `process::exit` "until WP-15 task supervision gives critical tasks a result policy". This item gives them one.

The watchdog is itself a task on the executor (its exit is at `sharddir.rs:189`), so its exit cannot fire when the executor is wedged. That stays out of scope (§8).

### 1.4 The ordered stop has no bound

- `drive_shutdown` joins an aborted task without a deadline (`src/tasks/shutdown.rs:197-203`: `let joined = (&mut t.handle).await;`).
- `main` builds the runtime as a temporary (`src/main.rs:38-42`: `...build()?.block_on(streams_slate::run(config))`). Tokio documents that dropping a runtime makes "the thread initiating the shutdown block until all spawned work has been stopped", and that `Drop` "waits forever for this" (`tokio-1.52.3/src/runtime/runtime.rs:40-44`).
- The process spawns blocking work: the RSS sampler's `spawn_blocking(mi_collect)` at `src/bootstrap/rss.rs:29-33`.

Today no watchdog bounds any of this. The only process exit, the unready watchdog's, runs **on** the executor.

### 1.5 The critical loops registered on the process root, and how each can end while `Running`

Every production spawn site, from `grep -rn '\.spawn(' src`:

| Loop | Spawn site | Policy | Ends on its own while Running? |
|---|---|---|---|
| `request-maintenance` | `application/request_work.rs:143-152` | Critical | only after `run(cancel)` returns, which happens on cancel (`:228`); or by panic |
| `unready-watchdog` | `sharddir.rs:169-189` | Critical | never; `process::exit(1)` on expiry |
| `signal` | `bootstrap.rs:727-740` | Critical | only after a stop request (either arm); a consequence |
| `auth-refresher` | `auth_feed.rs:392-414` | Critical | cancel arms only (`:403`, `:408`); or by panic |
| `scaler` | `scaler3.rs:540-590` | Critical | `Done` when `Weak<AppState>` fails to upgrade (`:545`, `:560`). The router holds the state until `serve_h1` returns (`bootstrap.rs:889`, `:894`), so this happens only after a stop |
| `rss-sampler` | `bootstrap.rs:790-796` → `bootstrap/rss.rs` | Critical | `Failed("allocator purge worker: …")` if the purge join fails (`rss.rs:34-36`), a real failure |
| `fleet` | `fleet.rs:459-…` | Critical | cancel arms only (`:493`, `:504`, `:871`); or by panic |
| `telemetry-outbox-sweep`, `telemetry-drain` | `billing/telemetry_loop.rs:27`, `:60` | Critical | cancel arms only (drain: `break`, then `terminal_round`); or by panic |
| `usage-rollup` | `billing.rs:1386-1399` | Critical | `Failed("usage rollup open failed: …")` when `ROLLUP=1` and `BILLING_MODE≠required`. A real failure, and deterministic at boot (see D4) |
| `runtime-watchdog` | `http.rs:2858` (started by `serve_h1`) | Noncritical | not a cause |

Other supervisors, which are **not** process roots and keep their owner-answers semantics:
- Engines: `EngineTasks` is `#[derive(Default)]` (`shard/lifecycle.rs:11-15`) and is built by `shard.rs:1450` `lifecycle::EngineTasks::default()`. `TaskSupervisor::default()` calls `new()`. Each engine answers through its `RequiredExit` fence (`lifecycle.rs:122-135`).
- Rigs and tests: all 28 other `TaskSupervisor::new()` call sites (`fixture_http.rs:450`, `http/serve.rs:161`, `auth_feed.rs:469`, `application/creation.rs:299`, `tasks/shutdown.rs:234,260`, the DST controllers, the `tasks::tests`).
- The only production supervisor is `bootstrap.rs:295`.

### 1.6 `ShardDirectory::shutdown` waits out its grace after a failed close, then drops the cause: `src/shard_directory.rs:381-419`

```
400             let pending = engines.iter().filter(|engine| !engine.terminated()).count();
401             if opens == 0 && pending == 0 {
402                 let failures: Vec<_> = reports.into_iter().filter_map(Result::err).collect();
...
409             if tokio::time::Instant::now() >= deadline {
410                 return Err(format!(
411                     "shutdown ongoing or failed: {opens} opens, {pending} engines; owners retained"
412                 ));
```

A failed close is final. `tasks/shutdown.rs:78-90` shows why:

```
 78                 resources_closed = matches!(outcome, TaskOutcome::Finished);
...
 84                 // A failed/panicked close cannot prove resource termination.
 85                 // Publish the failure, but keep the owner's replacement fence.
 86                 if resources_closed {
 87                     st.phase = Phase::Stopped;
 88                 }
 89                 st.report = Some(report.clone());
```

`shard/lifecycle.rs:98-104` then reads that state:
- `failure()` is `incomplete_shutdown_failure()`. It is `Some` exactly when the phase is `ShuttingDown` and the report is published, which is terminal.
- `terminated()` is `phase == Stopped`, which stays false for good after a failed close.

The consequence:
- `EngineShutdown::wait` returns the published report at once.
- Line 400 still counts the engine as pending, so the loop spins in 10 ms steps until the deadline (10 s in production).
- Lines 410-412 then drop `reports`, which named `storage-close: Failed(..)`.

`OpenGate::unready_reason` (`sharddir.rs:399-413`) already treats `failure()` as final. Nothing in the stop does.

### 1.7 The deploy wrapper holds every death: `deploy/app-server/supervise.ts:39-72`

`deploy/app-lb` and `deploy/app-gen` hold byte-identical copies (all three have md5 `5d198371…`):

```
39   const code = await proc.exited;
...
61   console.error(`binary exited with code ${code}; serving diagnostic on :${port}`);
62   Bun.serve({
...
71   // Never resolve: keep the diagnostic reachable for the operator.
72   return new Promise<never>(() => {});
```

The Compute runbook says the platform replaces a dead instance: `RUNBOOK.md:492` "single instance dies (OOM/exit/wedge), even under traffic | plaform reprovisions transparently in seconds". The wrapper never dies, though, so any process death becomes a permanent, unauthenticated 500. After item 38 alone, a critical exit would turn today's 503 zombie (which still serves appends) into a 500 zombie that serves nothing. So item 39's policy has to land with item 38 or before it.

The hint at `supervise.ts:51-54` ("check required env vars and that the binary is x86_64") is right only for boot failures.

`bench/fleet/handoff-gate.sh:71-91` relies on corpses being held ("its supervisor serves the crash diagnostic"), but only to fail its precheck with a remedy.

### 1.8 Stale comment: `src/tasks.rs:32-34`

"The runtime is not healthy without it (fleet loop, telemetry drain, the watchdogs): an unexpected exit is a critical failure (surfaced to readiness by WP-15's remaining slice)." Readiness is already surfaced (§1.1). Also, `runtime-watchdog` is Noncritical (`http.rs:2860`), so "the watchdogs" is wrong.

### 1.9 Line budgets (HEAD `wc -l`)

| File | Now | Limit | After |
|---|---|---|---|
| Ceilinged, all untouched: `http.rs` 3,153; `product.rs` 4,205; `shard.rs` 3,139; `billing.rs` 2,151; `history.rs` 1,656; `auth.rs` 1,637; `registry.rs` 1,452; `sse/feed.rs` 1,165; `fleet.rs` 1,142 | – | no growth | untouched |
| `src/bootstrap.rs` | 915 | 1,000 | 912 |
| `src/tasks.rs` | 429 | 1,000 | 464 |
| `src/tasks/exits.rs` (new) | – | 1,000 | 229 |
| `src/tasks/exits/tests.rs` (new, `#![cfg(test)]`) | – | 1,000 | 352 |
| `src/tasks/exits/loom_tests.rs` (new, `#![cfg(test)]`) | – | 1,000 | 93 |
| `src/tasks/shutdown.rs` | 278 | 1,000 | 284 |
| `src/shard_directory.rs` | 650 | 1,000 | 653 |
| `src/shard/lifecycle.rs` | 135 | 1,000 | 153 |
| `src/shard/task_lifecycle_tests.rs` | 298 | 1,000 | 386 |
| `src/sharddir.rs` | 920 | 1,000 | 920 (doc only) |

No DST file is touched.

---

## 2. Contract decision (the owner's adopted position, made concrete)

### 2.1 Tasks owner (`src/tasks.rs`, new `src/tasks/exits.rs`)

- **`TaskSupervisor::process_root(self) -> Self`** (a builder method): this is the process's own supervisor. `new()` and `Default` keep today's semantics, where the owner answers for a critical exit. Engines, rigs and every test keep `new()`.
- **`Inner` gains two fields:**
  - `root: OnceLock<Duration>`: the stop bound, present only on a process root.
  - `stop_cause: OnceLock<CriticalExit>`.
- **`CriticalExit { name, outcome: TaskOutcome }`**. It uses the join report's own classification and panic text (the shared `shutdown::panic_message`). It displays as `critical task <name> exited while the runtime was running: <outcome:?>`.
- **Every supervised loop runs inside `ExitWatch::run`:** `catch_unwind`, answer, then hand back the same result or `resume_unwind` the same panic. The panic hook does not run twice, and the join report is unchanged.
  - An aborted loop never reaches the answer (only the shutdown driver aborts).
  - The watch holds the supervisor through a `Weak`, so a loop never keeps its runtime alive.
- **The one transition, `stop_on_exit`,** applies on a process root, to a Critical loop, whatever its end (`Done`, `Failed`, panic):
  - If the cancellation is already published, the exit is a consequence. It is visible only in the join report.
  - Otherwise the first such exit records itself in `stop_cause`, and only then publishes the cancellation. Whoever sees the stop therefore sees its cause (Loom-modelled). Racing exits settle on one cause through `OnceLock::set`.
  - Noncritical ends never stop anything. On a non-root supervisor nothing changes.
- **The phase is not moved by the escalation.** Readiness keeps answering `critical task terminated: <name>` until the tail's `shutdown` moves the phase (milliseconds, during which the listener closes). `debug_load`'s `critical_failure` names the loop in that window. Registration stays open, and a loop registered after the escalation starts already cancelled and is joined by the tail.
- **Bound:** the escalating exit arms `arm_stop_deadline(deadline)`. This is a named OS thread that sleeps the bound, writes `critical exit: the ordered stop outlived its <d> deadline; exiting` to stderr, and calls `std::process::exit(1)`.
  - It is never disarmed. Its only job is to bound a process that is ending anyway, including the runtime drop after `run` returns.
  - If the thread cannot be started, the process exits 1 at once (D3).
  - The binary's bound is `PROCESS_STOP_DEADLINE = 30 s`: the loops' 10 s join, the shards' 10 s close, the teardown, and margin.
- **`TaskSupervisor::ordered_stop(grace, resources) -> Result<(), String>`:**
  - It joins every loop and logs `supervised loops stopped` (the log moves here from `run`, with the same fields).
  - It then awaits `resources`.
  - If the root stopped for a critical exit, it returns `Err(cause.to_string())` and logs a resource failure at error level. Otherwise it returns the resources' own result.

### 2.2 Process (bootstrap, main)

- `run` builds `TaskSupervisor::new().process_root()`.
- `run`'s tail becomes `tasks.ordered_stop(10 s, shards.shutdown(10 s)).await.map_err(anyhow::Error::msg)?; served?; Ok(())`.
- A critical exit therefore ends the process with the ordered sequence and exit code 1, and stderr carries `Error: critical task <name> exited while the runtime was running: <outcome>` (`main` returns `run`'s `anyhow` error).
- A requested stop (SIGTERM/SIGINT) is unchanged and exits 0.
- `main.rs` is untouched.

### 2.3 Directory

- An engine whose close failed is **settled**: `EngineShutdown::settled() = terminated() || failure().is_some()`. `pending` counts only engines still closing.
- With no open in flight and every engine settled, `shutdown` answers at once with `Err(joined reports)`, which names `storage-close: Failed(..)`.
- At the deadline the error keeps its prefix and adds the joined reports: `shutdown ongoing or failed: {opens} opens, {pending} engines; owners retained; joined reports: {failures:?}`. That format is a Rust debug list of the report strings (D9).

### 2.4 Deploy wrapper (item 39's slice)

- `superviseBinary(bin, argv, env, policy = { onDeathAfterReady: "hold" })`.
- While the child runs, the wrapper probes `127.0.0.1:$PORT` every 250 ms with a TCP connect.
- With `"exit"`, a child that ever accepted and then died ends the wrapper with `process.exit(code || 1)`, and Compute replaces the instance.
- A child that never accepted is held and served exactly as today, including under `"exit"`. A child that binds and dies within one probe interval also counts as never having accepted.
- `app-server` and `app-lb` pass `"exit"` (D5). `app-gen` keeps the default `"hold"`, because its workload runs to completion.
- The three `supervise.ts` copies stay byte-identical, and a test pins that.

### 2.5 What changes at the edge

| Situation | Before | After |
|---|---|---|
| Binary: a Critical loop returns `Done` or `Failed`, or panics, while serving | Process stays up. `/health` and `/readyz` answer 503 `critical task terminated: <name>` forever. Every other route still serves. Nothing exits it. | The listener closes at once, as on SIGTERM (new connections refused, keep-alives and SSE aborted). Loops are joined (≤10 s) and shards closed (≤10 s). Exit code 1, with `Error: critical task <name> exited while the runtime was running: <outcome>` on stderr. A hard bound of 30 s from the exit is enforced off the executor (exit 1 on expiry). |
| Same, under a rig or engine supervisor | readiness 503; engine fence | unchanged |
| `ROLLUP=1`, `BILLING_MODE≠required`, rollup DB open fails | serves appends behind a 503 `/health` | the process exits 1 (D4) |
| SIGTERM/SIGINT | exit 0 after the ordered stop | unchanged. When an engine close fails in that stop, `run` returns the joined reports at once instead of after 10 s; the exit code is 1 either way. |
| Directory stop past its deadline | `…; owners retained` | `…; owners retained; joined reports: ["…"]` (a process error or log string, not a wire body) |
| Compute: server or LB binary dies after it accepted on `$PORT` | wrapper serves a permanent 500 `binary_exited`; never replaced | wrapper exits with the child's code (1 for a clean or signal-less exit); Compute reprovisions |
| Compute: binary dies before it ever accepted | 500 `binary_exited` held | unchanged |

No HTTP route changes its status or body. There is no new configuration and no new environment variable.

---

## 3. Red tests, pins and non-vacuity controls

Filter paths: `tasks::exits::tests::<name>`, `tasks::exits::loom_tests::<name>`, `shard::task_lifecycle_tests::<name>`.

### 3.1 Wrapper (commit C1): `deploy/supervise.test.ts` (Bun)

The exact text is in `scratchpad/p18cte/deploy-new/supervise.test.ts`. The wrapper ends its own process, so each case runs it in a child Bun process over a scripted "binary". There are four tests:
1. `the three wrapper copies are byte-identical`
2. `a child that died after accepting ends the wrapper with its code`: the child serves `$PORT`, then `exit(1)` after 1.5 s; the wrapper must exit 1 within 10 s.
3. `a child that never accepted is held and served as a diagnostic`: the child runs `exit(2)` at once; the wrapper must serve a 500 `binary_exited` with `exitCode` 2 and stay up.
4. `a holding caller serves the diagnostic after a ready child's death`.

**Red (actually run)** against the HEAD wrapper plus the new test, from `bun test ./supervise.test.ts`:

```
error: expect(received).toBe(expected)

Expected: 1
Received: "running"

(fail) a child that died after accepting ends the wrapper with its code [10012.21ms]

 3 pass
 1 fail
```

**Green (actually run, four times):** `4 pass, 0 fail`.

**Control (actually run):** with `accepts()` forced to `return false` in all three copies, test 2 fails again with `Received: "running"`. The readiness probe is therefore what the exit depends on.

### 3.2 Directory (commit C2): `src/shard/task_lifecycle_tests.rs`

The exact text is in `scratchpad/p18cte/task_lifecycle_tests.rs`, appended after the existing tests. It adds a helper `serving(&engine)` and two tests.

**Helper:** a one-prefix `ShardDirectory` whose opener returns the fixture engine. It has no block nesting, so it needs no exception.

**T8 `a_failed_storage_close_ends_the_directory_stop_at_once`:**
- The fixture engine is served and opened.
- `engine.tasks.begin_failed_close_for_test("scripted close failure")` runs (a `#[cfg(test)]` hook, §4.C2).
- `tokio::time::timeout(5 s, directory.shutdown(30 s))` must return.
- The error contains `storage-close: Failed("scripted close failure")`.
- `directory.unready_reason()` still names the failure, and `!engine.termination_complete()` holds.
- Cleanup is `drop(engine.db.close().await)`. It uses `drop`, not `let _`, so the test needs no new exception.

**T9 `a_directory_stop_past_its_grace_carries_the_joined_reports`:**
- On a held WAL, `timeout(5 s, directory.shutdown(5 ms))` must equal `"shutdown ongoing or failed: 0 opens, 1 engines; owners retained; joined reports: [\"engine shutdown still running; join authority retained\"]"`.
- Then `store.release_hold()`.
- `timeout(15 s, directory.shutdown(10 s))` must be `Ok`, and `engine.termination_complete()` holds.

**Red:** on HEAD plus only the test-only hook and these tests (production unchanged):

```
thread 'shard::task_lifecycle_tests::a_failed_storage_close_ends_the_directory_stop_at_once' panicked at src/shard/task_lifecycle_tests.rs:<L>:<C>:
a failed close is final: the directory stop must not wait out its grace: Elapsed(())
```

```
thread 'shard::task_lifecycle_tests::a_directory_stop_past_its_grace_carries_the_joined_reports' panicked at src/shard/task_lifecycle_tests.rs:<L>:<C>:
assertion `left == right` failed
  left: "shutdown ongoing or failed: 0 opens, 1 engines; owners retained"
 right: "shutdown ongoing or failed: 0 opens, 1 engines; owners retained; joined reports: [\"engine shutdown still running; join authority retained\"]"
```

Traces:
- **T8:** `pending` counts the failed engine (`terminated()` is false), so the loop runs to the 30 s grace and the outer 5 s bound fires.
- **T9:** with 5 ms of grace, the joined `EngineShutdown::wait` times out and yields exactly one report, `engine shutdown still running; join authority retained` (`lifecycle.rs:108`). The engine is neither terminated nor failed, so `pending = 1`, and the completed open gives `opens = 0`.

**Why the hook is sound:**
- `begin_shutdown_with` is single-flight. At `Duration::ZERO` the roles are aborted.
- Their `RequiredExit` guards call `engine.begin_close()`. That call's `EngineTasks::begin_close` finds the completion already installed (`shutdown.rs:58-60`), so the real close future is dropped unrun.
- The published report ends with `("storage-close", Failed("scripted close failure"))`, and the phase stays `ShuttingDown`.
- No store fault can produce this, because `close_db` maps both `Closed(Clean)` and `Closed(Fenced)` to `Ok` (`shard/history_partition.rs:145-155`).

### 3.3 Process root (commit C3): `src/tasks/exits/tests.rs` and `src/tasks/exits/loom_tests.rs`

The exact text is in `scratchpad/p18cte/exits_tests.rs` and `exits_loom_tests.rs`.

In-process tests use `test_root()`, which is `TaskSupervisor::new().bounded_root(Duration::from_secs(86_400))`. Their deadline thread cannot fire inside the suite; the test binary's exit ends it.

| # | Test | Asserts |
|---|---|---|
| T1 | `a_critical_exit_stops_the_process_root_and_names_its_cause` | For `Done`, `Failed("repository gone")` and `panic!("critical panic")`: before the release there is no cause. After it, the cancellation is published within 5 s and `stop_cause() == Some(CriticalExit { "fleet", <outcome> })`. The join report carries the same outcome. The cause outlives the stop. |
| T2 | `only_an_unrequested_critical_exit_on_the_process_root_is_a_cause` (guard) | A root's Noncritical `Failed` stops nothing. An owner supervisor's (`new()`) Critical `Done` keeps readiness `critical task terminated: committer`, publishes no cancellation and records no cause. A root whose loop ends in answer to `cancel()` has no cause. |
| T3 | `the_first_critical_exit_is_the_cause_and_later_exits_its_consequences` | Held interleaving: `fleet` fails and so requests the stop. `telemetry-drain` then ends because of it. A later `cancel()` changes nothing. The report is in registration order with both outcomes, and the cause is `fleet`. |
| T4 | `the_ordered_stop_fails_with_the_critical_cause_after_the_whole_sequence` | A requested stop gives `Ok` and phase `Stopped`. A requested stop with a failed resource gives `Err("close failed")`. A critical exit plus a failed resource gives `Err("critical task fleet exited while the runtime was running: Failed(\"repository gone\")")`, and the phase is `Stopped` (loops joined first). |
| T5 | `a_critical_exit_ends_the_accept_loop_and_fails_the_ordered_stop` | End to end through the real `crate::http::serve_h1`: a critical `Done` ends the accept loop (5 s bound), and `ordered_stop` returns `Err("critical task fleet exited while the runtime was running: Finished")`. |
| T6 | `the_process_root_bounds_its_stop_off_the_executor` + helper `stop_deadline_helper` | A **child process** (pattern of `tasks::signal::tests`: `current_exe`, `--exact --nocapture --test-threads=1`, `env_clear`, a marker variable `STREAMS_STOP_DEADLINE_HELPER`, a `Helper` that kills and reaps on drop). Phase 1: the binary's `process_root()` answers a critical exit, and the child prints `process root stopped`. Phase 2: a `bounded_root(200 ms)` root escalates, the child prints `executor wedged`, then blocks its **only executor thread** for good (`mpsc::Receiver::recv` with the sender held). The parent polls `try_wait` for up to 20 s using tokio sleeps (no `std::thread` path) and asserts exit code `Some(1)`, both markers, and the stderr line `the ordered stop outlived its 200ms deadline; exiting`. |
| L1 | `quality_loom_one_cause_at_most_and_the_stop_always_published` | Loom: two exits race a signal's `request` (three threads). At most one exit claims the cause, the recorded cause is the claimant, and the flag is published. |
| L2 | `quality_loom_a_published_exit_stop_carries_its_cause` | Loom: an observer that sees the flag sees the cause. |

The Loom settings copy `quota/pin/loom_tests.rs`: `max_threads = 3`, `max_branches = 1000`, `preemption_bound = Some(2)`, no duration or permutation cutoff. `StopFlag` is implemented for `loom::sync::atomic::AtomicBool` and `CauseCell` for `loom::sync::Mutex<Option<T>>` inside the test module. The model runs the production `stop_on_exit` itself.

**Red step (local, never committed).** The new tests do not compile on HEAD (`E0432 unresolved import super::CriticalExit`, `E0599 no method named process_root / bounded_root / stop_cause / ordered_stop`). So first stage the **new shape with the old semantics**:
- Add `mod exits;` to `tasks.rs`.
- `exits.rs` contains `CriticalExit` and its `Display`, `StopFlag`, `CauseCell` and `stop_on_exit` exactly as in the draft, plus these stubs:
  - `process_root(self) -> Self { self }`
  - `bounded_root(self, _after: Duration) -> Self { self }`
  - `stop_cause(&self) -> Option<CriticalExit> { None }`
  - `ordered_stop` = `self.shutdown(grace).await; resources.await` (today's tail, with no verdict)
- Add no `ExitWatch`, no `Inner` fields, and keep `spawn` unchanged.

On that tree:

```
tasks::exits::tests::a_critical_exit_stops_the_process_root_and_names_its_cause ... FAILED
  a critical exit must request the process root's stop: Elapsed(())
tasks::exits::tests::the_first_critical_exit_is_the_cause_and_later_exits_its_consequences ... FAILED
  the first exit requests the stop: Elapsed(())
tasks::exits::tests::the_ordered_stop_fails_with_the_critical_cause_after_the_whole_sequence ... FAILED
  the critical exit requests the stop: Elapsed(())        (its first two legs pass)
tasks::exits::tests::a_critical_exit_ends_the_accept_loop_and_fails_the_ordered_stop ... FAILED
  a critical exit must end the accept loop: Elapsed(())
tasks::exits::tests::the_process_root_bounds_its_stop_off_the_executor ... FAILED
  assertion `left == right` failed: <transcript containing "the binary's own root must answer a critical exit: Elapsed(())">
    left: Some(101)
   right: Some(1)
```

- T2, L1 and L2 pass on the staged tree. T2 is a guard; the Loom tests model a function that exists unchanged.
- Each `Elapsed` comes from `stop_requested`'s or T5's 5 s bound. The stubs never publish the cancellation, so `serve_h1` never breaks.
- 101 is libtest's exit code for the child's failed phase 1.

### 3.4 Existing tests that must stay green unchanged (pins)

- `tasks::tests::critical_exits_failures_and_panics_are_reported`, sha-pinned in `review-mechanisms.json` `critical-runtime-exit`. Its body is untouched. Its owner supervisor's panics now pass `ExitWatch`, and the report still reads `Panicked("scripted")`.
- `dst::dst_tests::review_readiness::readiness_endpoint_refuses_each_permanent_critical_exit`, sha-pinned. The rig is built with `new()`, so the listener stays up and `/health` answers 503.
- The rest of `tasks::tests::*`, in particular:
  - `readiness_changes_after_each_kind_of_critical_exit`
  - `cancel_closes_registration_before_the_join`
  - `cancellation_refuses_poisoned_registration_state`
  - `a_panicking_builder_poison_prevents_health_claims_and_later_spawns`: the builder is still called inside `register`'s lock, so it still poisons it.
  - the F-G refusal tests `a_spawn_refused_by_a_closing_runtime_cannot_deadlock_its_supervisor`, `a_multi_thread_teardown_cannot_deadlock_any_supervisor`, `a_refused_future_that_spawns_again_is_set_aside_again` and `a_spawn_that_unwinds_closes_its_slot_and_drops_nothing_under_the_lock`: the refused future is now the `ExitWatch::run` future that contains the loop, and it is still set aside and dropped after the lock.
- `tasks::shutdown::tests::r17a_*` and `tasks::signal::tests::*`.
- `shard::task_lifecycle_tests::r17a_*`. In particular, `r17a_runtime_shutdown_retains_held_engine_and_refuses_replacement` still gets `is_err()` at 5 ms, then `Ok` twice.
- `dst::dst_tests::runtime_engine_lifecycle::*`, `dst::dst_tests::request_topology_debt::*`, `http::serve::tests::*`, `bootstrap::tests::process_bootstrap_cannot_be_an_empty_success`.

### 3.5 Non-vacuity controls (local edits, never committed)

- **Loom ordering:** in `stop_on_exit`, move `flag.request()` above `cause.record(exit)`. L2 must fail with `a stop without its cause`. This follows the d4d631df precedent: a control is a local mutation of the actual function.
- **Directory:** revert `!engine.settled()` to `!engine.terminated()`. T8 must go red again with `Elapsed(())`.
- **Deadline:** replace `arm_stop_deadline`'s body with `{}`. T6 must fail with `the deadline never ended the helper`, after 20 s.
- **Wrapper:** as in §3.1 (run).

---

## 4. Edits, file by file, in commit order

Each commit must pass `scripts/quality.sh` on its own. CI compares each push against its `before` revision.

### C1: "A served binary that dies ends its deploy wrapper, so Compute replaces it" (item 39's slice)

1. **`deploy/app-server/supervise.ts`, `deploy/app-lb/supervise.ts`, `deploy/app-gen/supervise.ts`:** the three copies stay byte-identical. The exact text is in `scratchpad/p18cte/deploy-new/app-server/supervise.ts`:
   - The header comment gets an item 39 paragraph.
   - Add `READY_PROBE_MS = 250` and `export type DeathPolicy = { onDeathAfterReady: "exit" | "hold" }`.
   - Add a fourth parameter `policy: DeathPolicy = { onDeathAfterReady: "hold" }`.
   - A `probe` loop sets `ready` through `accepts(port)`, a single `Bun.connect` to `127.0.0.1:$PORT`, until the child exits.
   - After `await proc.exited` and the stderr pump: if `ready && policy.onDeathAfterReady === "exit"`, log one line and `process.exit(code || 1)`.
   - Otherwise the existing held-diagnostic code runs, byte-for-byte.
2. **`deploy/app-server/index.ts:90-94`:**
   - The comment becomes: `superviseBinary: a binary that dies before it accepted on $PORT is held and serves its exit code + stderr tail … one that dies after it was serving (an OOM kill, item 38's critical exit) ends this wrapper with its code, so Compute replaces the instance (item 39, deploy/README.md).`
   - The call becomes `await superviseBinary(bin, ["--listen", \`0.0.0.0:${port}\`], process.env, { onDeathAfterReady: "exit" });`
3. **`deploy/app-lb/index.ts:36-39`:** the same comment in one line, and `await superviseBinary(bin, [], { ...process.env, MODE: mode }, { onDeathAfterReady: "exit" });` (D5). `deploy/app-gen/index.ts` is unchanged, since its default holds.
4. **`deploy/supervise.test.ts` (new):** the text of `scratchpad/p18cte/deploy-new/supervise.test.ts`. It sits outside the app directories, so it is never deployed.
5. **`.github/workflows/ci.yml`, job `sdk-package`,** directly after `- name: Credential-wait cancellation (Bun)`:
   ```yaml
         - name: Deploy wrapper replaces a binary that died after serving (Bun)
           run: bun test ./deploy/supervise.test.ts
   ```
   The job already has `oven-sh/setup-bun@v2` and `timeout-minutes: 30`, and `supervise.ts` imports nothing.
6. **`deploy/README.md`,** in the `supervise.ts` paragraph (lines 21-34): state that the diagnostic is served for a binary that exits **before it ever accepted on `$PORT`**. Add: "A binary that dies after it was accepting (an OOM kill; streams-slate's exit 1 after a critical loop's exit, item 38) is a runtime death: `app-server` and `app-lb` pass `{ onDeathAfterReady: "exit" }`, so the wrapper exits with the child's code and Compute replaces the instance. `app-gen` holds every death, because its workload runs to completion. `bun test ./deploy/supervise.test.ts` pins both paths and that the three copies stay byte-identical."
7. **`RUNBOOK.md` §7.5:**
   - Row `:495` becomes: `| domain returns a JSON binary_exited body | the binary died before it ever accepted on $PORT (a boot failure) and the wrapper holds the port to explain it | read exitCode + stderrTail in the body — usually a missing required env var or wrong arch |`
   - After the §7.2 code block, add one sentence: "The deployed wrapper is `deploy/app-server/{index,supervise}.ts` (deploy/README.md); a binary that dies after it accepted on `$PORT` ends it with its exit code, and Compute replaces the instance."
   - `RUNBOOK.md` is `include_str!`d into the operator page (`src/operator.rs:26`). This is a content change only.

C1 changes no Rust source, so the mutation leg selects nothing. `workflow-lint` checks the new step.

### C2: "A failed engine close ends the directory stop at once, with its cause"

1. **`src/shard/lifecycle.rs`:**
   - In `impl EngineShutdown`, after `terminated`, add `pub(crate) fn settled(&self) -> bool { self.terminated() || self.failure().is_some() }` with its doc comment. `EngineShutdown` carries no exception.
   - In `impl EngineTasks`, after `abort`, add the `#[cfg(test)] pub(super) fn begin_failed_close_for_test(&self, error: &'static str)`. It calls `self.supervisor.begin_shutdown_with(Duration::ZERO, "storage-close", async move { TaskResult::Failed(error.into()) })`.
   - Both edits are in `scratchpad/p18cte/lifecycle.rs`.
   - Ratchet: the three `EngineTasks` contracts (`required`/`let_underscore_must_use`, `failure`/`unwrap_used`, `failed`/`unwrap_used`) measure identically, since neither edit is in their scope.
2. **`src/shard_directory.rs`, `ShardDirectory::shutdown`** (the text is in `scratchpad/p18cte/shard_directory.rs`):
   - The doc gets two sentences about item 38.
   - `:400` `!engine.terminated()` becomes `!engine.settled()`.
   - The `let failures …` line moves one line up, out of the `if opens == 0 && pending == 0 {` block and unchanged.
   - The deadline `format!` string becomes `"shutdown ongoing or failed: {opens} opens, {pending} engines; owners retained; joined reports: {failures:?}"`, which is an inline argument inside the existing macro tokens.
   - **Ratchet (`excessive_nesting`, `scope_lines`/`syntax_facts`/`nested_items`): measured identical.** Only a method name changes, one line moves, and the macro-token string changes. The nesting that fulfils the expectation (`return if … {Ok} else {Err}` inside the `if` inside `loop`) is untouched, so the expectation stays fulfilled. A `failures.join("; ")` argument would have added a line, which is growth, and was rejected.
3. **`src/shard/task_lifecycle_tests.rs`:** `serving`, T8 and T9 (§3.2) are appended. Existing tests are not edited.
4. **`scripts/quality/mutation_owners.py`:** `owner('shard_directory', 'src/shard_directory.rs', 'shard_directory::')` becomes `owner('shard_directory', 'src/shard_directory.rs', 'shard_directory:: shard::task_lifecycle_tests::')`. The directory's own tests never call `shutdown` (`grep -n shutdown src/shard_directory.rs` shows only the item itself), and multi-filter rows have precedent at `mutation_owners.py:70`.

### C3: "A critical loop's exit stops the process root, bounded off the executor, and fails the process"

1. **`src/tasks.rs`** (the full text is in `scratchpad/p18cte/tasks.rs`; the diff is in `scratchpad/p18cte/rust.diff`):
   - Add a module-doc paragraph about the process root.
   - Add `mod exits;`.
   - Import `OnceLock` and `std::time::Duration`.
   - Fix the `Policy::Critical` doc (§1.8).
   - `Inner` gains `root: OnceLock<Duration>` and `stop_cause: OnceLock<exits::CriticalExit>`, and `new()` initializes both with `OnceLock::new()`. `new()` and the `Inner` struct carry no exception.
   - **`spawn` is split:**
     - The old function, with its `#[expect(clippy::unwrap_used, reason = "Supervisor registration; …")]` attribute and its body **byte-identical**, becomes private `fn register<F, Fut>(…)`, with a one-line doc.
     - A new `pub(crate) fn spawn<F, Fut>(…)` has the same signature, carries the old doc (plus one sentence about `ExitWatch`) and has no exception:
       ```rust
       let watch = exits::ExitWatch::new(&self.inner, label, policy);
       self.register(label, policy, move |cancel| watch.run(build(cancel)))
       ```
     - The builder still runs inside `register`, under the lock and after the phase check.
     - **Measured:** the `unwrap_used` contract `crate::TaskSupervisor::spawn` vanishes, and `crate::TaskSupervisor::register` (same file, same kind, same lint) is paired with it as a rename. `nested_items` 1→1, `scope_lines` 33→33, `syntax_facts` 60→58, and `unwrap_site:path` for `crate` 1→0. Every other fingerprint key is equal and no other `function`/`unwrap_used` contract in `src/tasks*` vanishes in this commit, so the pairing has exactly one candidate. See D6.
2. **`src/tasks/exits.rs` (new, 229 lines;** the exact text is in `scratchpad/p18cte/exits.rs`). It contains:
   - `PROCESS_STOP_DEADLINE`, `CriticalExit` and its `Display`.
   - `StopFlag`, implemented for `tokio::sync::watch::Sender<bool>` (`*self.borrow()` / `send_replace(true)`).
   - `CauseCell<T>`, implemented for `OnceLock<T>` (`set(..).is_ok()`).
   - `stop_on_exit`, `ExitWatch { supervisor: Weak<Inner>, name, policy }` with `new`, `run` and `answer`, and `outcome_of`. The type is `Result<TaskResult, Box<dyn Any + Send>>`, never `std::thread::Result`: a `std::thread::` path is an `effect` to the source gate (`source_rules.py:44-46`).
   - `arm_stop_deadline`.
   - `impl TaskSupervisor { process_root, bounded_root (pub(super)), stop_cause (pub(super)), ordered_stop }`.
   - `#[cfg(test)] mod loom_tests; #[cfg(test)] mod tests;`.
   - **New exception** on `arm_stop_deadline` (new code, so the gate treats it as "reviewed in source"; D7): `#[expect(clippy::disallowed_methods, reason = "arm_stop_deadline; the process root's bound on its ordered stop must run where a wedged executor cannot hold it, and it ends the process rather than being joined; a supervised task would share the executor it has to outlive")]`. It has exactly two `;` and no `"`. The primitive-spawn check passes through the owners rows in §6.
   - No `unwrap`, `expect`, `let _` or `tokio::select!` appears. Nesting stays at 3 or less and every function is under 40 lines.
3. **`src/tasks/shutdown.rs`:**
   - `classify`'s panic arm body becomes `let payload = e.into_panic(); TaskOutcome::Panicked(panic_message(&*payload))`. There are two statements, so rustfmt keeps the block and the guard line `Err(e) if e.is_panic() => {` stays out of the diff. The single-expression form would be collapsed onto the guard line and would pull guard mutants into the diff.
   - `pub(super) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String` is added after `classify`. Its downcast chain is the old one, verbatim.
   - `classify` has no exception, and the file's seven contracts measure identically.
4. **`src/tasks/exits/tests.rs`** and **`src/tasks/exits/loom_tests.rs`** (new, `#![cfg(test)]`): §3.3.
5. **`src/bootstrap.rs`** (the text is in `scratchpad/p18cte/bootstrap.rs`):
   - `:295` becomes `let tasks = crate::tasks::TaskSupervisor::new().process_root();`. The call-site and path facts `crate::tasks::TaskSupervisor::new` are unchanged, and `.process_root()` adds only `method-call` facts.
   - `:895-911` is replaced by a five-line comment and:
     ```rust
     tasks
         .ordered_stop(
             std::time::Duration::from_secs(10),
             shards.shutdown(std::time::Duration::from_secs(10)),
         )
         .await
         .map_err(anyhow::Error::msg)?;
     served?;
     Ok(())
     ```
   - Every fingerprint key keeps or lowers its count:
     - the `tasks` receiver moves from `.shutdown` to `.ordered_stop`;
     - both `std::time::Duration::from_secs(10)` call and path facts stay at 2;
     - `shards` and `anyhow::Error::msg` stay at 1;
     - the `tracing::info` path and its macro facts leave.
   - **Measured for all six contracts** (`too_many_lines`, `cast_possible_truncation`, `let_underscore_must_use`, `expect_used`, `unwrap_used`, `excessive_nesting`): `scope_lines` 582→578, `syntax_facts` 967→966, and 0 growth failures. Every one of the six stays fulfilled, because their sites are elsewhere in `run`.
   - The `source-allowances.json` rows `crate::run` `tokio::select` (1) and `serde_json::json` (1) are unchanged.
6. **`src/sharddir.rs:151-154`:** the stale doc is replaced with the text in `scratchpad/p18cte/sharddir.rs`. Its line count is unchanged, and the `spawn_unready_watchdog` contract measures identically.
7. **Ledgers:** see §6.
8. **`RUNBOOK.md` §7.5:** add a row after the `binary_exited` row: `| instance replaced; its log ends in Error: critical task <name> exited while the runtime was running: … (exit 1) | a critical loop ended while serving: the process root stopped the runtime and exited 1 (item 38), and the wrapper exited too, so Compute reprovisioned (item 39) | read the loop's own error line before the stop; a loop that fails the same way at every boot (e.g. usage-rollup with ROLLUP=1 and an unopenable rollup DB) will crash-loop, so fix its cause |`

**Ratcheted scopes touched in C3, in sum:**
- `run`'s six contracts shrink.
- `spawn`'s contract is renamed to `register` and is not larger.
- One new exception is added on new code.
- `ShardDirectory::shutdown` (C2) and `spawn_unready_watchdog` are identical.
- No reason is edited, re-wrapped or split, and `docs/quality/exception-growth.json` gets no row.
- Import aliases: none of the moved or new code goes through a `use` alias.

---

## 5. Mutation analysis (cargo-mutants 27.1.0, `--in-diff`, per-mutant timeout 90 s)

Selected owners and files:
- **C2:** `shard_directory` (filter widened by §4.C2 item 4) and `shard_lifecycle` (`shard::`). `task_lifecycle_tests.rs` is `#[cfg(test)]` inside `shard`, so it is omitted.
- **C3:** `tasks` (`src/tasks.rs`), the new row `tasks_exits` (`src/tasks/exits.rs`), `tasks_shutdown`, `bootstrap` and `sharddir`. The `sharddir` change is doc-only: no mutant is selected, and that is reported explicitly. The `#![cfg(test)]` files `exits/tests.rs` and `exits/loom_tests.rs` are production-unchanged.

| Mutant | Killed by |
|---|---|
| `EngineShutdown::settled → true` | T9 (the held engine counts as settled, so the error lacks the prefix) |
| `settled → false`; `\|\|→&&` | T8 (runs to the 30 s grace; the 5 s bound fires) and the `r17a_runtime_shutdown_…` final `unwrap` |
| delete `!` in `!engine.settled()` | T8 (the failed engine counts as pending); T9 (the held engine is not pending, so no prefix) |
| `ShardDirectory::shutdown → Ok(())` / `Err(String::new())` / `Err("xyzzy".into())` | T8 `unwrap_err` and `contains`; T9 `assert_eq`; `r17a_*` `unwrap` |
| `<CriticalExit as Display>::fmt → Ok(Default::default())` | T4 and T5 exact error strings |
| `<Sender<bool> as StopFlag>::requested → true` | T1 (nothing escalates) |
| `… requested → false` | T2 (the requested leg records a cause) |
| `… request → ()` | T1 (`Elapsed` after 5 s) |
| `<OnceLock<T> as CauseCell<T>>::record → true` | T1 (`stop_cause() == None`) |
| `… record → false` | T1 |
| `stop_on_exit → true` / `false` | T1 |
| delete `!` in `!cause.record(exit)` | T1 (the cause is recorded but the stop is never published) |
| `ExitWatch::answer → ()` | T1 and T5 |
| `answer`: `!=`→`==` | T1 (a critical exit is skipped) and T2 |
| `arm_stop_deadline → ()` | T6 (the child never exits; the parent's 20 s bound fails) |
| `process_root → Default::default()` | T6 phase 1 (child exit 101) |
| `bounded_root → Default::default()` | T1 |
| `stop_cause → None` | T1 |
| `ordered_stop → Ok(())` | T4 (refused leg) |
| `ordered_stop → Err(String::new())` / `Err("xyzzy".into())` | T4 (requested leg, `unwrap`) |
| `shutdown::panic_message → String::new()` / `"xyzzy".into()` | T1 panic case (`Panicked("critical panic")` in both the cause and the report) |
| `run → Ok(())` / `Err(anyhow!("mutated!"))` | `bootstrap::tests::process_bootstrap_cannot_be_an_empty_success` |
| `TaskSupervisor::new → Default::default()` | See below: recursion into a stack overflow, reported as caught |
| **Unviable:** `ExitWatch::new`, `ExitWatch::run`, `outcome_of`, `stop_cause → Some(Default::default())`, `spawn`/`register → Ok(Default::default())` | The types `ExitWatch`, `TaskResult`, `TaskOutcome`, `CriticalExit` and `TaskId` have no `Default` |

**`TaskSupervisor::new` is in the diff** because of the two new field initializers. `impl Default for TaskSupervisor` calls `Self::new()` (`tasks.rs:308-312`), so the mutant recurses: `new → Default::default → new …`.
- The mutation profile is `profile.quality` = opt-level 1 (`Cargo.toml:77-81`). LLVM's O1 function pipeline (`buildO1FunctionSimplificationPipeline`) has no `TailCallElimPass`: it only carries the TODO to investigate it. rustc marks no call `tail`, so the backend makes no sibling call either.
- The recursion is therefore real. It overflows libtest's 2 MiB test-thread stack within microseconds, and Rust aborts with `fatal runtime error: stack overflow`. cargo-mutants counts the crashed test binary as CAUGHT, not TIMEOUT.
- `new` cannot be kept out of the diff: every `Inner` field lives in its literal. It also cannot be `const` (`Arc::new`).
- **Stop condition:** control §7.9 must show `CAUGHT` for this line. If it shows `TIMEOUT`, stop and take it to Søren. Do **not** point `Default` at a separate constructor, because that turns the mutant into an equivalent MISSED.

There are no equivalent mutants. Every wait in the new tests is bounded (5 s, 10 s, 15 s or 20 s). The slowest failing mutant is `arm_stop_deadline → ()` at about 20 s, well inside 90 s.

**Synchronization proof.** `stop_on_exit` is the only new state transition. L1 and L2 run it under Loom (in the `--lib quality_` leg), with a local ordering control (§3.5). T3 is the held interleaving.

The directory predicate `settled()` adds no transition: it reads two terminal, monotonic states (phase `Stopped`, or `ShuttingDown` with a published report), each under the supervisor mutex. T8 is the failed-close integration test and T9 the held-WAL test. See D8.

---

## 6. Ledgers (in the commit that needs them)

- **C2 `scripts/quality/mutation_owners.py`:** the `shard_directory` filter (§4.C2).
- **C3 `scripts/quality/mutation_owners.py`:** add `owner('tasks_exits', 'src/tasks/exits.rs', 'tasks::'),` next to `tasks_shutdown`. `validate_sources` requires the literal row.
- **C3 `docs/quality/owners.json`:** append two rows. The file is not sorted, and the d4d631df rows were appended.
  ```json
  {"category": "effect", "count": 1, "owner": "crate::arm_stop_deadline", "path": "src/tasks/exits.rs",
   "reason": "Item 38 stop deadline: the process root's bound on the ordered stop its critical exit requested runs on its own named OS thread, so a wedged executor cannot hold it; the thread only sleeps and then ends the process, and is never joined because the process is ending either way.",
   "syntax": "std::thread::Builder::new"},
  {"category": "effect", "count": 1, "owner": "crate::arm_stop_deadline", "path": "src/tasks/exits.rs",
   "reason": "Item 38 stop deadline: the deadline thread sleeps out the bound off the executor, then ends the process.",
   "syntax": "std::thread::sleep"}
  ```
  An inventory diff of all drafts against HEAD shows exactly these two new non-exception identities. `source-allowances.json` is unchanged, and no prune is needed.
- **`docs/quality/exception-growth.json`:** no row.
- **C3 `docs/refactor/review-mechanisms.json`, mechanism `critical-runtime-exit`:**
  - New `limitations`: "The binary's process root answers a critical exit with its own ordered stop and exits 1 naming the loop, bounded at 30 s off the executor (item 38); the deploy wrapper exits after a served binary's death so Compute replaces it (item 39); Compute's replacement itself has no receipt until a cloud run."
  - Append pins for `src/tasks/exits/tests.rs` `a_critical_exit_stops_the_process_root_and_names_its_cause` and `the_process_root_bounds_its_stop_off_the_executor`. Compute the `sha256` values in the committed tree with:
    ```
    python3 - <<'EOF'
    import importlib.util, pathlib
    s = importlib.util.spec_from_file_location('inv', 'scripts/test-inventory.py'); m = importlib.util.module_from_spec(s); s.loader.exec_module(m)
    p = pathlib.Path('src/tasks/exits/tests.rs')
    print({f['name']: f['function_sha256'] for f in m.functions(p.read_text(), p)})
    EOF
    ```
    Then run `python3 scripts/review-evidence.py --check`. The two existing pins are byte-unchanged.
- **C3 `docs/refactor/WIRE-MATRIX.md` §3 Health (`:194`):**
  - Fix the stale cite `src/http.rs:2066-2124` to `src/http.rs:1633-1693`.
  - Add the supervisor 503s (`runtime supervisor unavailable`, `runtime shutting down`, `runtime stopped`, `critical task terminated: <name>`).
  - Add: "Under the binary's process-root supervisor a critical loop's exit (a `Done` return included) also requests the ordered stop (item 38): the listener closes at once, loops and shards close, and the process exits 1 naming the loop, bounded at 30 s off the executor; `critical task terminated: <name>` is transient there and persistent only under a rig's supervisor."
  - The main checkout's uncommitted edit of this file (the 401 line) belongs to the other session. Apply only this hunk, in the clean worktree.
- **C3 `docs/review-runtime-evidence.md` §R17 (`:23`):** add one sentence: "Item 38: under the binary's process root a critical exit, `Done` included, also requests the ordered stop and fails the process (exit 1), bounded off the executor; `TaskMonitor` readiness is unchanged for every other supervisor."
- **Unchanged:**
  - `docs/refactor/test-inventory.json`: it scans only `src/dst`, and no DST test changes.
  - `docs/refactor/architecture-policy.json`: no transport edge, the new file is 229 lines, and every function is under 200 lines.
  - `src/dst/tests/README.md`.
  - `scripts/mt-audit-baseline.txt`: no `stream_hash(`, registry or tenant text changes.

---

## 7. Controls (commands and expected outputs)

Work in the clean worktree. Put Python 3.11 or newer on PATH and build the scanner (`cargo build --locked -p streams-quality-syntax`).

1. **C1 red, then green:**
   - With only `deploy/supervise.test.ts` added, `bun test ./deploy/supervise.test.ts` gives `3 pass, 1 fail`, with the §3.1 output.
   - After C1 it gives `4 pass, 0 fail`.
   - Control (§3.1): `3 pass, 1 fail` again.
   - Save the three logs as `scratchpad/item39-wrapper-{red,green,control}.log`.
2. **C2 red:** apply only the `lifecycle.rs` test hook and T8/T9, then run:
   `cargo test --locked --lib -- --exact shard::task_lifecycle_tests::a_failed_storage_close_ends_the_directory_stop_at_once shard::task_lifecycle_tests::a_directory_stop_past_its_grace_carries_the_joined_reports`
   Expected: `2 failed`, with the §3.2 messages (T8 after about 5 s).
3. **C2 green, floored:**
   `scripts/test-leg.sh target/quality/item38-dir.log --exact shard::task_lifecycle_tests::a_failed_storage_close_ends_the_directory_stop_at_once --exact shard::task_lifecycle_tests::a_directory_stop_past_its_grace_carries_the_joined_reports -- --locked --lib shard::task_lifecycle_tests::`
   Expected: every result `ok`, including the six `r17a_*`. Then `cargo test --locked --lib shard_directory:: dst::dst_tests::runtime_engine_lifecycle:: dst::dst_tests::request_topology_debt::` must show every result `ok`.
4. **C3 red** (the staged shape of §3.3, on C2):
   `cargo test --locked --lib -- --exact tasks::exits::tests::a_critical_exit_stops_the_process_root_and_names_its_cause tasks::exits::tests::the_first_critical_exit_is_the_cause_and_later_exits_its_consequences tasks::exits::tests::the_ordered_stop_fails_with_the_critical_cause_after_the_whole_sequence tasks::exits::tests::a_critical_exit_ends_the_accept_loop_and_fails_the_ordered_stop tasks::exits::tests::the_process_root_bounds_its_stop_off_the_executor`
   Expected: `5 failed`, with the §3.3 messages. Save as `scratchpad/item38-c3-red.log`, then discard the staging.
5. **C3 green, floored:**
   `scripts/test-leg.sh target/quality/item38-tasks.log --exact tasks::exits::tests::a_critical_exit_stops_the_process_root_and_names_its_cause --exact tasks::exits::tests::only_an_unrequested_critical_exit_on_the_process_root_is_a_cause --exact tasks::exits::tests::the_first_critical_exit_is_the_cause_and_later_exits_its_consequences --exact tasks::exits::tests::the_ordered_stop_fails_with_the_critical_cause_after_the_whole_sequence --exact tasks::exits::tests::a_critical_exit_ends_the_accept_loop_and_fails_the_ordered_stop --exact tasks::exits::tests::the_process_root_bounds_its_stop_off_the_executor -- --locked --lib tasks::`
   Expected: every result `ok`. This covers all of `tasks::` (the pins in §3.4, the signal child test, `stop_deadline_helper` running inert).
   Then: `scripts/test-leg.sh target/legs/quality.log --min 15 -- --locked --release --lib quality_`. Expected: every result `ok`, including `tasks::exits::loom_tests::quality_loom_one_cause_at_most_and_the_stop_always_published` and `…quality_loom_a_published_exit_stop_carries_its_cause`.
6. **Neighbours:**
   `cargo test --locked --lib dst::dst_tests::review_readiness:: dst::dst_tests::runtime_engine_lifecycle:: http::serve:: auth_feed:: bootstrap:: dst::dst_tests::billing_controller:: dst::dst_tests::scaler_controller:: dst::dst_tests::fleet_controller::`
   Expected: every result `ok`. These are the rigs and controllers that build `TaskSupervisor::new()`.
7. **Controls from §3.5** (local only): Loom ordering gives `a stop without its cause`; directory revert gives T8 `Elapsed(())`; deadline body `{}` gives T6 `the deadline never ended the helper`. Record each as a log.
8. **Gate on each commit:** `scripts/quality.sh`. Expected:
   - `quality ratchets: OK`;
   - no `accepted exception grew without an approved growth row` (drafts measured at 0);
   - no `exception needs owner; invariant; alternative`;
   - no `primitive-spawn exception needs a registered function owner`;
   - no `unregistered source occurrence`;
   - no `file growth`;
   - `architecture-gate: OK`;
   - clippy and rustdoc `-D warnings` clean;
   - mt-audit OK.
9. **CI's own selection and the mutation run, before the push** (all commits made):
   `QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan`
   Expected `plan.json`:
   - `mutation_source_files` ⊇ {`src/bootstrap.rs`, `src/shard/lifecycle.rs`, `src/shard_directory.rs`, `src/sharddir.rs`, `src/tasks.rs`, `src/tasks/exits.rs`, `src/tasks/shutdown.rs`};
   - `production_unchanged_files` ⊇ {`src/shard/task_lifecycle_tests.rs`, `src/tasks/exits/tests.rs`, `src/tasks/exits/loom_tests.rs`};
   - `unregistered_mutation_source_files: []`;
   - `mutants: true`;
   - `properties_fuzz` and `miri` true (the `scripts/quality/` edit counts as tooling).

   Then run `… scripts/quality/mutations.sh` with the same environment. Expected: zero `MISSED`, zero `TIMEOUT`, and the §5 unviable list. **Read the `TaskSupervisor::new` line: it must be `CAUGHT`** (the stop condition in §5). Save the log as `scratchpad/mutants-item38.log`.
10. **After the push:** `gh run list --branch slate --limit 5 --json databaseId,headSha,status,conclusion`, match the SHA, then `gh run view <id>`. Never claim CI is green from memory.
11. **Field acceptance (needs Søren, D10):** deploy the C3 binary behind the C1 wrapper to a scratch Compute service with `ROLLUP=1` and a rollup prefix the instance cannot open. This is the one no-patch trigger of a critical exit after bind.
    - Expected: one `critical task exited while the runtime was running; requesting the ordered stop, bounded at 30s` line, then `supervised loops stopped`.
    - Then `Error: critical task usage-rollup exited while the runtime was running: Failed("usage rollup open failed: …")`, then the wrapper's `exiting 1 so the platform replaces this instance`, then a reprovisioned instance.
    - This is a deterministic boot failure, so expect a crash loop. That is the D4 trade-off made visible, and it confirms the replacement path.
    - Before the fix, the same deploy stays up with `/health` answering 503 `critical task terminated: usage-rollup` and appends still answering 2xx.

---

## 8. Out of scope and follow-ups

- **Arming the bound on a requested (SIGTERM) stop** as well. `ShutdownRequest::request` (`tasks.rs:246-252`) carries no exception, so this is a small follow-up (D1).
- **The unready watchdog** still calls `process::exit(1)` on the executor (`sharddir.rs:189`), so a wedged executor defeats it. It could request the root's bounded stop instead.
- **Predicates `/health` checks that no exit watches:** auth feeds unpublished (`http.rs:1641-1659`) and billing prerequisites in required mode (`:1670-1680`). A hung (not exited) critical loop is also unwatched. That belongs to item 40 (progress checks).
- **The rest of item 39:**
  - bearer-gating `stderrTail` (the held 500 is still unauthenticated);
  - an explicit hold-corpse environment variable for `bench/fleet/handoff-gate.sh`;
  - one wrapper file instead of three byte-identical copies;
  - the full rewrite of the drifted RUNBOOK §7.2 sketch;
  - cloud validation (D10, D11).
- **In-flight graceful drain:** connections are still aborted at once on any stop (WP-15 §9; item 37's note).
- **An aborted critical loop** is not an exit the root answers. Only the shutdown driver aborts.
- **Item 37** (handler panics, poisoned process-wide locks) is separate.

---

## 9. Decisions for the owner

- **D1 (scope).** The hard bound is armed only when the root stops for a critical exit, which is the approved scope. **Recommendation:** also arm it on SIGTERM and SIGINT. It is a small edit in `ShutdownRequest::request`, which carries no exception. It would change SIGTERM behaviour: a hung stop would exit 1 after 30 s instead of waiting for the platform's kill.
- **D2 (value).** The bound is `PROCESS_STOP_DEADLINE = 30 s`, a constant: 10 s for loops, 10 s for shards, then the teardown and margin. Making it configurable would mean a new setting in `run`, which is fingerprint growth. **Recommendation:** keep the constant.
- **D3 (fail-closed).** If the `stop-deadline` thread cannot be started (EAGAIN), the process exits 1 at once and skips the ordered stop. The alternative is to proceed without a bound. **Recommendation:** exit at once. This branch cannot be tested.
- **D4 (behaviour at the edge, and the wrapper criterion).**
  - With `ROLLUP=1` and `BILLING_MODE≠required`, a rollup DB open failure (`billing.rs:1392-1395`) used to leave the instance serving behind a 503. It now exits 1.
  - Behind the item 39 wrapper this is treated as a death after `$PORT` accepted, because the bind precedes the rollup open. So a deterministic misconfiguration crash-loops until the platform gives up.
  - Options:
    - (a) accept, as planned, following the review's "TCP-ready" criterion;
    - (b) the wrapper counts a child as ready only after `GET /health` answered 200 once. Caveat: `/health` stays 503 until the first shard opens, so an idle instance would never become "ready";
    - (c) ready = accepted **and** up for at least N s (say 60 s). A death sooner than that is held and served as a boot failure.
  - **Recommendation:** (c) if you want boot-adjacent failures to stay diagnosable over HTTP; otherwise (a).
- **D5 (scope).** `app-lb` adopts the exit-after-accept policy, which is the review's item 39 change. `app-gen` keeps holding. **Recommendation:** confirm.
- **D6 (governance).** The excepted `TaskSupervisor::spawn` keeps its body and attribute byte-for-byte, reason untouched, under the private name `register`. The new unexcepted `spawn` wraps it. RUST-QUALITY forbids renaming an exception's owner *to absorb growth*. Here nothing under the exception grows: the gate pairs it as a rename, and every metric is less than or equal to before. The new code lives outside any exception, which is the prescribed remedy.
  - Alternatives:
    - (a) a growth row for `spawn` holding the wrapper inline. That is growth: a new `refusal::spawn_set_aside(…)` call-site key, and the `self`, `label` and `policy` path counts each +1.
    - (b) keep `spawn` as it is, and switch every production caller to a new wrapper name. That is 12 method-name edits in 10 files, including `fleet.rs`, `billing.rs` and `http.rs`. It leaves a footgun: a loop registered through `spawn` would not be answered.
  - **Recommendation:** the rename.
- **D7 (new exception and new effect).** `arm_stop_deadline` adds `#[expect(clippy::disallowed_methods)]` for `std::thread::Builder::spawn`, and two owners.json effect rows (`std::thread::Builder::new`, `std::thread::sleep`). This is new code, not growth, so the gate reviews it in source. The OS thread is exactly the adopted requirement: a watchdog not on the executor. Confirm the owner row wording.
- **D8 (verification).** The directory's `settled()` predicate gets integration tests (T8 failed close, T9 held WAL) and no Loom model: it only reads two terminal, monotonic supervisor states. The escalation transition does get a Loom model. **Recommendation:** accept.
- **D9 (message).** The directory-stop deadline error appends `joined reports: [..]` in Rust debug-list form. This is the only zero-growth way to carry the reports inside the ratcheted scope; a `join("; ")` argument would add a line. It is a process error and log string, not a wire body. **Recommendation:** accept.
- **D10 (acceptance).** The review requires a cloud validation of item 39. Control §7.11 needs Compute access and a scratch service. **Recommendation:** run it before the next deploy that carries C3.
- **D11 (order).** Push C1 (the wrapper) in the same push as C3, or before it. Never deploy a C3 binary behind the old wrapper: a critical exit would leave a 500 that is never replaced, where today's zombie at least serves appends. The recommended push order is C1, C2, C3.

---

## Skeptic corrections (C1..C14)

I checked the plan against `d4d631df`, reading with `git show`. `git diff d4d631df d255ad6d -- src` is empty. I ran no cargo command and edited no repository file.

What I re-ran:
- The plan's own ratchet measurement, rebased on `d4d631df` (`scratchpad/skeptic_growth.py`, the scanner in `target/debug`). It reproduces the plan's numbers:
  - 0 growth failures;
  - `register` is paired with the vanished `spawn` contract;
  - `run`'s six contracts go 582→578 / 967→966;
  - the only new exception is `arm_stop_deadline`.
- An inventory diff of every draft. It shows exactly the two new `effect` identities the plan names.

These checks hold: every quoted line, the line budgets, the use sites (all 29 `TaskSupervisor::new()` sites; the only production root is `bootstrap.rs:295`), the no-growth claims for C2/C3, the red traces for T1–T5/T8/T9, the `bootstrap::tests::process_bootstrap_cannot_be_an_empty_success` killer (`bootstrap/tests.rs:54-58`), and the owners-row mechanics (`source_rules.py:462-471`).

The corrections follow.

**C1 (blocking: T6 hangs on exactly the path it exists to catch, so the mutation leg would report TIMEOUT).** `exits_tests.rs:307-318` reads the child's pipes before it kills the child. The sequence:
1. `drained(&mut helper)` calls `read_to_string` on the child's stdout, then its stderr.
2. When the 20 s poll times out, the child is still alive, blocked for good in `blocked.recv()` (`:349-350`), and still holds the pipes' write ends.
3. The read never returns, so the `unwrap_or_else(panic!)` is never reached and `Helper::drop` never runs.

Two consequences:
- The mutant `arm_stop_deadline → ()` becomes a cargo-mutants **TIMEOUT** (90 s, `mutation_driver.py:52-53`), not CAUGHT. That fails the mutation leg for the `src/tasks` prefix.
- The §3.5/§7.7 deadline control hangs instead of printing `the deadline never ended the helper`.

Fix: on `Err(elapsed)`, first call `drop(helper.0.kill()); drop(helper.0.wait());`, then drain. Both are std `Child` calls, and neither is an effect in `source_rules.py:44-49`. Re-run the control afterwards, and record in its log that it fails in about 20 s.

**C2 (wrong scope: `deploy/app-lb` also deploys the load generator).** `bench/fleet/deploy-fleet.sh:196-200` deploys `fleet-gen` from `$S/fleet-app-lb` with `PILOT_MODE=gen` ("same pilot wrapper, MODE=gen").
- The generator binds `$PORT` for its stats server (`src/bin/pilot/generator.rs:544`), so the probe marks it ready.
- It dies only on an error (`generator worker exited before drain` and similar, `generator.rs:504-506`).
- Under `{ onDeathAfterReady: "exit" }`, that death restarts the generator on the platform. The generator then re-ramps load in the middle of a campaign and loses the stderr tail that explains the failure.

Required changes:
- §4.C1 item 3 must pass `{ onDeathAfterReady: mode === "lb" ? "exit" : "hold" }` (`deploy/app-lb/index.ts:35-39`).
- D5 and `deploy/README.md` must say that MODE=gen holds.

**C3 (missed mutants: cargo-mutants 27.1.0 also marks the line before every deletion).** `in_diff.rs:affected_lines` pushes `lineno - 1` for each `Line::Delete`. So two premises of the plan are wrong:
- `tasks/shutdown.rs`: the guard line `Err(e) if e.is_panic() => {` precedes the deleted `let p = e.into_panic();`, so it **is** in the diff. The rationale in §4.C3.3 fails, and the `MatchArmGuard` mutants are selected:
  - **guard → `false`**: a panic is classified `Cancelled`. Killed by `tasks::tests::critical_exits_failures_and_panics_are_reported` and T1's panic case.
  - **guard → `true`**: `into_panic()` on a cancelled `JoinError` panics inside the driver, and observers get the `shutdown-driver` failure. Killed by `shutdown_is_ordered_bounded_and_joins_everything` (`tasks/tests.rs:118-123`) and `aborted_tasks_are_destroyed_before_shutdown_returns` (`:191`).
- `shard_directory.rs`: the deleted `let failures …` inside the `if` marks `if opens == 0 && pending == 0 {` (`:401`). This selects:
  - `&&→||`: T9 fails, because the stop returns early with the bare `"engine shutdown still running; join authority retained"`.
  - `opens == 0 → !=`: T8 fails with `Elapsed`.
  - `pending == 0 → !=`: T8 fails with `Elapsed`.

Add these five rows to §5 and read them in the §7.9 log. The two-statement restructure of `classify` no longer buys anything. It is harmless, but the stated reason for it should go.

**C4 (§5/§7.9 expectation wrong: `task_lifecycle_tests.rs` is not "production-unchanged").**
- The file has no `#![cfg(test)]` (`src/shard/task_lifecycle_tests.rs:1`), so the scanner's `test_only_file` is false.
- Its new `#[tokio::test]` attributes are non-builtin, so `production_changes.normalized_source` returns `None`.
- It is therefore a changed mutation source, selected through its own registered row `owner('task_lifecycle_tests', 'src/shard/task_lifecycle_tests.rs', 'shard::')` (`mutation_owners.py`, the rows after `shard_lifecycle`).
- cargo-mutants then lists 0 mutants, because it skips `#[cfg(test)] mod task_lifecycle_tests;` (`shard.rs:3116-3117`), and the driver prints `task_lifecycle_tests: no executable mutants in the selected scope`.

Move the file from `production_unchanged_files` to `mutation_source_files` in §7.9. Also fix the "omitted" sentence in §5.

**C5 (the readiness name in the escalation window is not the cause).** `TaskMonitor::unready_reason` and `Inner::critical_failure` report the first finished Critical loop in `TaskId` order (`tasks.rs:222-229`, `:277-280`). The loops that end *because of* the stop were registered earlier than most causes:
- `request-maintenance`: `bootstrap.rs:709`;
- `unready-watchdog`: `:714`;
- `signal`: `:727`.

So in the window, `/health` can say `critical task terminated: signal`, and `debug_load`'s `critical_failure` can name a consequence. Required changes:
- Reword §2.1 ("names the loop in that window").
- Reword the proposed WIRE-MATRIX §3 sentence: the name there is transient and may be any exited critical loop; only the stderr/`Error:` line and the `critical task exited…` log carry the cause.
- Do not "fix" this in `unready_reason`, because it is an `unwrap_used`-excepted scope.

**C6 (the wrapper's `code || 1` is unpinned and changes a graceful stop's code).** `deploy-new/app-server/supervise.ts` maps a clean exit 0 after ready to exit 1 and logs "so the platform replaces this instance".
- streams-slate exits 0 after ready only on a requested SIGTERM/SIGINT stop.
- Test 2 scripts `exit(1)`, so a mutation that drops `|| 1` survives.

Either preserve the child's code (`process.exit(code)`), or keep the mapping and add a test that pins it (the child serves, then exits 0). In the second case, list the mapping as part of D4/D5 for the owner.

**C7 (missing controls: the real binary under the process root).** §7.6 exercises only rigs built with `new()`. Several CI jobs run the release binary's root:
- `scripts/platform-e2e.mjs` sets `ROLLUP: "1"` (`:109`), injects feed faults, and asserts `cellX.exitCode === null` (`:138-140`);
- `scripts/platform-e2e-negative.mjs` (added in 1d079e01) expects an exact failure set;
- the LiveFeed fleet cert;
- the sdk-package smoke.

Any critical exit there now ends a cell instead of leaving a 503. Add these to §7 as neighbours, after `cargo build --release`, and expect them unchanged:
- `node scripts/platform-e2e.mjs` → `PLATFORM_E2E_OK`;
- `node scripts/platform-e2e-negative.mjs` → `PLATFORM_E2E_NEGATIVE_OK`.

This is also the memory rule "run CI's plan before push".

**C8 (evidence ledger: T6's proof lives partly in its helper).** `review-evidence.py:check` sha-pins only the named functions (`:177-186`). T6 is meaningless without `stop_deadline_helper` (`exits_tests.rs:330-352`), and T1 relies on `test_root` and `stop_requested`. Add to `critical-runtime-exit`:
- `stop_deadline_helper` as a second `tests` entry (it is a `#[tokio::test]`);
- `test_root` and `stop_requested` as `support_functions` (the `include_helpers=True` path, `:187-192`).

**C9 (doc cites and stale docs).**
- The reprovision row is `RUNBOOK.md:491`, not `:492`.
- The new D4 RUNBOOK row should point at the crash-loop-zombie row (`RUNBOOK.md:490`: "platform gave up silently"). That row is where a deterministic post-bind failure ends up under option (a).
- `bench/fleet/handoff-gate.sh:71-73` says an aborted owner "is a corpse (its supervisor serves the crash diagnostic)". After C1 a server corpse is replaced, so correct that comment in C1 rather than leaving it to a follow-up.
- `docs/CHAOS-CAMPAIGN.md:271` and `docs/CAPACITY-R27.md:210` describe held-corpse diagnosis. They are historical records and need no edit, but §2.5 should state that an OOM kill after bind is no longer observable as `exitCode:137` over HTTP; it is only in the wrapper's log line.

**C10 (cross-plan interaction with item 40, `plans18/heartbeat-draining.md`).** That plan treats `critical task terminated: <name>` as a persistent readiness verdict that makes the fleet tick publish `draining:true` (its R1 and §2a).
- Under C3, a production root's critical exit cancels the fleet loop at once, so no drain is published. Ring exit then comes only from heartbeat staleness.
- Its rig tests use `new()`, so they stay valid.

Record the interaction in both plans (§8 here), and agree the order in which they land. Both edit the §3 wording of `WIRE-MATRIX` and the readiness narrative.

**C11 (base drift to watch).** This checkout's branch holds four unpushed commits (`da635ec8..eda79ccf`). They touch:
- `src/http/serve.rs`: `serve_h1` now wraps `serve_connections`, with the same signature;
- `docs/refactor/WIRE-MATRIX.md`;
- `docs/refactor/review-mechanisms.json`;
- `docs/refactor/test-inventory.json`.

If they reach `slate` first, rebase the WIRE-MATRIX §3 hunk and the `critical-runtime-exit` edit onto them. T5 is unaffected.

**C12 (Loom honesty; not blocking).** L1's "at most one cause" is a property of the `Mutex<Option>` stand-in, which rewrites `OnceLock::set`. L2's flag⇒cause property is not what production relies on: `ordered_stop` reads `stop_cause` only after `shutdown` joined the claimant, so the happens-before comes from the join.

The model also leaves one thing out: that a loop ending through a `Weak` upgrade failure (`scaler3.rs:545,560`) is a consequence only transitively, because the router drop follows `serve_h1`'s cancelled return.

Say all of this in the `loom_tests.rs` header. The trigger (`RUST-QUALITY.md:195`) is still met, because the actual `stop_on_exit` runs on instrumented primitives with bounds recorded.

**C13 (drafts never compiled).** §0 is explicit that no cargo command ran, so clippy/rustdoc cleanliness is asserted, not observed. Two points to confirm first:
- `serving`'s nested closures (`task_lifecycle_tests.rs` draft `:300-311`) under `excessive_nesting` (threshold 4; the neighbouring r17a test needed an exception for a deeper shape).
- T8's `unready_reason().contains("scripted close failure")`. It holds only because `ShardHealth::engine_failure` stays `None` (`sharddir/health.rs:57-59`), and that holds because the fixture passes a `None` close notifier (`task_lifecycle_tests.rs:30`).

Run `cargo clippy --all-targets -- -D warnings` on each commit before claiming it green.

**C14 (owner visibility of C2's edge change).** On a SIGTERM stop in which an engine close fails, `run` now returns at once instead of after 10 s, with a longer error string. The exit code is unchanged. D9 covers only the string. Add the timing change to D9, or state that the adopted 38/39 position covers it; the external review names this directory defect inside item 38.

**Verdict: ready-with-corrections.** The design holds against the tree:
- process-root builder;
- lock-free escalation that leaves registration open;
- an off-executor deadline thread;
- `settled()`;
- a wrapper death policy.

The ratchet measurement reproduces at 0 growth, and D6/D7 are correctly routed to the owner. C1 must be fixed before implementation, because as written the mutation leg fails with a TIMEOUT. C2–C4 change the edits and the expected CI plan. The rest are text, ledger and control additions.
