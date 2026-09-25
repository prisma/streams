# Item 37, step A: make handler panics visible (log line + per-runtime counter)

Tree: `slate` @ 33fbd10e (origin/slate = 2f2c3015). This plan was written from reads only; nothing was built or run.
Scope: step A only. Step B (a poisoned process-wide lock makes /health answer 503) is a readiness policy change. It appears only as decision D2 in §9.

Two commits:

- **C1** moves `serve_h1` from `src/http.rs` into `src/http/serve.rs`. The move is verbatim apart from one required token change.
- **C2** does three things: it logs and counts every connection task the accept loop reaps as panicked; it keeps the count on the runtime's own task record (`TaskSupervisor`, read through `TaskMonitor`); and it exposes the count as `tasks.connection_panics` on `/v1/debug/load`.

---

## 1. Problem (verified on the current tree)

Reviewer (review.md item 37): *serve_h1 discards JoinErrors (http.rs:1334,1339) … shutdown abort_all()s every in-flight request (:1338)*. The line numbers are stale. The code now sits at `src/http.rs:1162-1227`.

The two places that discard a connection task's `JoinError`:

```rust
// src/http.rs:1218-1220 (inside tokio::select! in the accept loop)
            // Reap finished connections so the set never grows with
            // completed entries.
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
...
// src/http.rs:1223-1225 (shutdown drain)
    drop(listener);
    conns.abort_all();
    while conns.join_next().await.is_some() {}
```

How a panic travels, checked against the locked dependency sources:

- Each connection task runs `h1.serve_connection(io, svc)` (http.rs:1202-1209). `svc` is `TowerToHyperService::new(app)` (1176).
- hyper 1.10.1 polls the axum handler future, and any streaming body, inside that task.
- Nothing in the chain catches an unwind. `catch_unwind` appears in hyper 1.10.1 only under `src/ffi`. It does not appear in hyper-util 0.1.20 or axum 0.8.9. The router has no `CatchPanicLayer`; `tower-http` is not a dependency.
- So a panicking handler unwinds the whole connection task. Tokio catches the unwind in the task harness. The socket is dropped (the client sees EOF or a reset, and no response). `join_next()` then yields `Some(Err(JoinError::Panic))`. The `Some(_)` pattern above throws it away.
- A task that panics just before cancellation is reaped by the drain loop (`is_some()`), which throws it away too.

What operators see today:

- `main.rs:16-21` installs a `tracing_subscriber::fmt()` subscriber.
- No panic hook is installed anywhere in `src` (grep for `set_hook`: none).
- The release profile keeps unwinding (`Cargo.toml:72-73`; the only `panic` key is the clippy lint level on line 144).
- So the only trace is the standard hook's unstructured stderr line, `thread 'tokio-runtime-worker' panicked at …`. It sits outside the tracing pipeline and has no level or connection context.
- No counter exists anywhere. `/v1/debug/load` (`debug_load`, http.rs:811-1009) has no panic field. Its `tasks` block (992-1002) covers only the supervised loops.

This is the only production `JoinSet` in the service library: `git grep JoinSet|join_next|abort_all -- src` finds only http.rs:1168/1194/1220/1224/1225. The other JoinSets are in `src/bin/bench*` and `src/bin/pilot`, and they already handle their joins.

Callers of `serve_h1`, all of which pass the same supervisor that feeds `AppState.tasks`:

- `src/bootstrap.rs:902`: `serve_h1(listener, app, &config.http, tasks.clone())`. `AppState.tasks = tasks.monitor()` at bootstrap.rs:660.
- `src/dst/tests/fixture_http.rs:543`: `serve_tasks = tasks.clone()` (534). `AppState.tasks = tasks.monitor()` (473), both inside `http_rig_build` (365).
- `src/dst/tests/read_peer_compatibility.rs:203`: `owner.tasks.clone()`.
- `src/http/serve.rs:78` (rig test): `super::super::serve_h1(...)`.

Doc references that remain true after the move, because the name resolves through a re-export: `src/tasks.rs:10` "see `http::serve_h1`", `src/http.rs:2920` "Spawned once from serve_h1", `src/config/model.rs:460`, and `docs/refactor/WIRE-MATRIX.md:9`.

Reviewer claims I checked but that are out of this step:

- *shutdown abort_all()s every in-flight request*: true (http.rs:1224). It belongs to the WP-15 §9 graceful drain. See §8.
- */health consults only supervised tasks*: partly true. `health_axum` (http.rs:1701-1761) consults `state.tasks.unready_reason()`, publication of the auth feeds, `state.shards.unready_reason()` and billing readiness. None of those reflects a poisoned lock or a panicked connection. See D2.
- bootstrap.rs:903-908 (now 902-913): `serve_h1` returns only on cancellation, and then `tasks.shutdown` reports the loops. Verified. Nothing changes there.

## 2. Contract decision

**Typed internal contract**

- `http::serve::reap(tasks: &TaskSupervisor, joined: Result<(), JoinError>)` classifies a reaped connection with `JoinError::is_panic()`, a typed verdict with no string matching:
  - A panic: one `tracing::error!` line (tokio's `JoinError` Display includes the panic payload) plus one count.
  - A cancellation (the shutdown's own `abort_all`) or a clean end: nothing.
  - Both reap sites call it: the select arm and the shutdown drain.
- The count lives on the runtime's task record: `Inner.connection_panics: AtomicU64`, written by `TaskSupervisor::record_connection_panic(&self)` and read by `TaskMonitor::connection_panics(&self) -> u64`. The monitor reads 0 once the supervisor is gone, matching the existing "a runtime whose supervisor is gone reports nothing" rule.
- `Relaxed` ordering is enough. It is a monotonic display counter with no protocol attached. The tests read it after `tasks.shutdown()` has joined the serve loop.

**Why the supervisor and not a process static**

- a1cf29f3 ("Shard-open counters belong to the gate that counts them") and 0aaa1ef1 removed exactly the pattern of a process static read by `/v1/debug/*`. With a static, every runtime in a process reports every other runtime's events, and tests can only assert "moved by at least one".
- Both ends are already wired to the supervisor. `serve_h1` is handed one, and `AppState.tasks` is its monitor in bootstrap and in every DST rig. So no call site changes, and the count is exact per runtime.
- The accept loop still owns and joins its connections. The supervisor only keeps the count, which the health and debug surfaces read. It supervises nothing new.
- No new global static means no `owners.json` global row.

**Wire and edge**

- Client-facing: no change. A panicking handler's client still sees a closed socket; no 500 is synthesized. `/health`, `/readyz` and `/livez` are unchanged. There is no `/metrics` endpoint.
- Operator debug surface, one additive key: `GET /v1/debug/load` gains `tasks.connection_panics` (u64), next to `tasks.critical_failure`.
  - The surface sits behind the one debug bearer gate.
  - Every consumer in `bench/` picks keys by name with jq or Python dicts, and no test asserts the key set of the `tasks` block. So the change is backward compatible for every reader found.
  - Listed as D1 because it changes a `/v1/debug` JSON shape.

## 3. Red tests (exact names, assertions, expected red output)

Recommended red sequence inside C2. The current tree cannot compile a test that names the new accessor, so the counting red is taken once the `tasks.rs` half of C2 exists and before `serve_h1` reaps.

**R1: wire, runtime red on the current tree**

- File: `src/dst/tests/admission_maintenance.rs`, test `debug_load_reports_typed_limiter_and_frame_totals`.
- Insert after line 721 (`let before = load(&body);`):
  ```rust
  assert_eq!(
      before["tasks"]["connection_panics"], 0,
      "item 37: a rig that never panicked a handler reports zero on its own task record: {before}"
  );
  ```
- Trace on the current tree: `before["tasks"]` is the object built at http.rs:994-1002 (`phase`, `critical_failure`, `loops`). Indexing a missing key yields `Value::Null`. `Value: PartialEq<i32>` holds, so it compiles.
- Expected red:
  ```
  assertion `left == right` failed: item 37: a rig that never panicked a handler reports zero on its own task record: {…load JSON…}
    left: Null
   right: 0
  ```
- Green: `0`. The rig's own supervisor counts nothing, so the value is exact because it is per runtime. The same test already warns that `appends_shed` equality cannot be asserted because that counter is process-global.

**R2: tasks-owner API, compile red on the current tree**

- File: `src/tasks/tests.rs`, new test `panicked_connections_are_counted_on_the_monitor`:
  ```rust
  /// Item 37: the accept loop's panicked connections are counted on the
  /// runtime's task record, which a monitor reads while the supervisor
  /// lives and reports as none once it is gone.
  #[test]
  fn panicked_connections_are_counted_on_the_monitor() {
      let supervisor = TaskSupervisor::new();
      let monitor = supervisor.monitor();
      assert_eq!(monitor.connection_panics(), 0);
      supervisor.record_connection_panic();
      supervisor.record_connection_panic();
      assert_eq!(monitor.connection_panics(), 2);
      drop(supervisor);
      assert_eq!(monitor.connection_panics(), 0);
  }
  ```
- Expected red (current tree): `cargo test --locked --lib tasks::` fails to build with
  - `error[E0599]: no method named `connection_panics` found for struct `TaskMonitor` in the current scope`
  - `error[E0599]: no method named `record_connection_panic` found for struct `TaskSupervisor` in the current scope`

**R3: the serve loop, runtime red once the tasks.rs half exists and before `reap` is wired**

- File: `src/http/serve.rs` (tests), new test `a_panicking_handler_is_counted_once_and_an_aborted_connection_is_not`, plus two rig routes and one helper:
  ```rust
  /// A handler that panics mid-request: neither axum nor hyper catches the
  /// unwind, so it ends the connection task.
  async fn panicking_handler() -> &'static str {
      panic!("scripted handler panic")
  }
  // in serve(): the helper doc "over a two-route app" becomes "over the rig's routes"
  .route("/hang", axum::routing::get(|| std::future::pending::<()>()))
  .route("/panic", axum::routing::get(panicking_handler))

  /// Item 37 (A): a panicking handler ends its connection with no response,
  /// and the accept loop that reaps the task counts it on the runtime's task
  /// record exactly once. A connection the shutdown aborts mid-request is
  /// cancelled, not panicked, and is not counted.
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn a_panicking_handler_is_counted_once_and_an_aborted_connection_is_not() {
      let (addr, tasks) = serve(DEADLINE).await;
      // Served once (so it is certainly accepted), then parked in a handler
      // that never answers: only the shutdown's abort ends it.
      let mut hung = TcpStream::connect(addr).await.unwrap();
      hung.write_all(b"GET /fast HTTP/1.1\r\nhost: rig\r\n\r\n").await.unwrap();
      let head = response_head(&mut hung).await;
      assert!(head.starts_with("HTTP/1.1 200"), "{head}");
      hung.write_all(b"GET /hang HTTP/1.1\r\nhost: rig\r\n\r\n").await.unwrap();
      let mut panicked = TcpStream::connect(addr).await.unwrap();
      panicked.write_all(b"GET /panic HTTP/1.1\r\nhost: rig\r\n\r\n").await.unwrap();
      assert_eq!(closed_within(&mut panicked, BOUND).await, Ok(()), "a panicked request is answered by a closed socket");
      tasks.shutdown(Duration::from_secs(5)).await;
      assert_eq!(closed_within(&mut hung, BOUND).await, Ok(()), "the shutdown aborts and joins the parked connection");
      assert_eq!(tasks.monitor().connection_panics(), 1, "one panicked connection is counted; the aborted one is not");
  }
  ```
  - The handler is a named `async fn` with an explicit return type. In edition 2024 an `async { panic!() }` block's output falls back to `!`, which is not `IntoResponse`.
- Trace on C1's loop (tasks.rs API present, no `reap`):
  - The panic closes `panicked`: EOF or a reset, and `closed_within` takes both as closed.
  - The shutdown aborts `hung`: `abort_all()` and the join close it.
  - The select arm `Some(_) = …` and the drain `is_some()` discard `Err(Panic)` and `Err(Cancelled)` alike. Nothing calls `record_connection_panic`.
- Expected red:
  ```
  assertion `left == right` failed: one panicked connection is counted; the aborted one is not
    left: 0
   right: 1
  ```
- On the current tree (no API at all) it is the E0599 build error for `connection_panics` from R2.

**Pins** (green before and after): the three existing rig tests in `http::serve::tests` (`headless_connection_is_closed_at_the_deadline`, `in_flight_response_outlives_the_deadline`, `idle_keep_alive_is_served_inside_and_closed_past_the_deadline`), plus `the_validated_buffer_floor_is_the_one_hyper_asserts`. These run the moved loop unchanged. The first two assertions of R3 also pin today's behaviour: no response, and the parked connection is joined at shutdown.

**C1 is a pure refactor.** Compile-level proofs:

- The four external callers keep `crate::http::serve_h1` through `pub(crate) use serve::serve_h1;`. The rig's `super::super::serve_h1` resolves the same way, so no caller changes.
- The moved body differs by one token.
- The four `http::serve::` tests, `security_workload::` and every DST rig (all serve through this function) stay green unchanged.

The log line is not pinned by a test:

- cargo-mutants has no operator that deletes a statement, so no mutant needs a killer.
- The crate has no tracing capture fixture.
- The standard hook still carries payload and location on stderr.
- If Søren wants it pinned: wrap the rig's `serve_h1` future in `tracing::instrument::WithSubscriber` with a `fmt()` writer over `Arc<Mutex<Vec<u8>>>`, and assert that "connection task panicked" appears. That costs about 20 test lines. Not in this plan.

## 4. Edits, file by file, in commit order

Ceilinged file touched: `src/http.rs` only.

- Now: 3,225 (HEAD). At origin/slate (2f2c3015): 3,314.
- The ceiling is the merge base of the pushed range: 3,314 if C1/C2 go up together with 33fbd10e, and 3,225 if 33fbd10e is pushed first.
- After C1: **3,159** (−67 moved, +1 re-export). After C2: **3,159** (debug_load stays line-neutral). Under both ceilings.

Files without a ceiling:

| File | Lines |
|---|---|
| `src/http/serve.rs` | 211 → 279 (C1) → ~335 (C2) |
| `src/tasks.rs` | 403 → ~425 |
| `src/tasks/tests.rs` | 472 → ~486 |
| `src/dst/tests/admission_maintenance.rs` | 918 → 922 (DST ceiling 1,000) |

### C1 — "serve_h1 moves into http/serve.rs, beside the posture it serves with, verbatim"

**`src/http.rs`**
- Delete lines 1162-1228: the doc (3), the `#[expect]` (5), the fn (58), and the trailing blank line.
- Add `pub(crate) use serve::serve_h1;` in the module tail, between `pub(crate) use read_adapter::{…};` (3215) and `use telemetry_append::…` (3216).
- `raise_nofile` (2878), `NOFILE_SOFT`/`NOFILE_HARD` (2906-2907) and `spawn_runtime_watchdog` (2930) stay. The child module imports them, so none becomes dead.

**`src/http/serve.rs`**
- Line 1 of the module doc becomes `//! The h1 serve loop and its connection posture: what every accepted socket is served with.`
- After line 7, add `use super::{NOFILE_HARD, NOFILE_SOFT, raise_nofile, spawn_runtime_watchdog};`.
- After `h1_builder` (line 38), paste the moved block byte-for-byte. The one change is at old line 1177: `let h1 = serve::h1_builder(http);` becomes `let h1 = h1_builder(http);`, because there is no `serve` path inside `serve`.
- The doc line that mentions `` `serve::h1_builder` `` is plain code, not a link, and is kept as is.
- The architecture gate's reverse edges are unaffected: `super::{…}` names neither `http` nor `product`, and `super::super::serve_h1` in the tests does not match either. This file is not a transport-rationale file.

**Ratcheted scope touched:** `serve_h1`'s `#[expect(clippy::disallowed_methods, clippy::let_underscore_must_use, reason = "serve_h1; …")]`.
- Its identity is now `(src/http/serve.rs, crate::serve_h1, …)`, which the merge base does not have. So `exception_growth` treats it as new, the "explicit review decision" branch.
- `violations` then requires two things:
  1. The reason must have the owner; invariant; alternative shape. It does: exactly two `;` and no `"`.
  2. A registered **effect** row at the new path, or the gate fails with `primitive-spawn exception needs a registered function owner: src/http/serve.rs: crate::serve_h1`.

**Ledgers (same commit)**
- `docs/quality/owners.json`: the effect row `{owner crate::serve_h1, syntax tokio::task::JoinSet::spawn}` changes `path` from `src/http.rs` to `src/http/serve.rs`. The reason is unchanged.
- `docs/quality/owners.json`: new macro-dsl row `{category macro-dsl, count 1, owner crate::serve_h1, path src/http/serve.rs, syntax tokio::select, reason "The accept loop selects cancellation, accept and connection reaping; the pinned Tokio macro is the await-any form and all three arms are owned by the loop; the http::serve rig tests drive every arm."}`.
- `docs/quality/source-allowances.json`: prune the row `{macro-dsl, src/http.rs, crate::serve_h1, tokio::select, 1}`, which would otherwise be "1 obsolete source allowances". Do not add it back there: the legacy baseline cannot grow, per the precedent in 33fbd10e.
- `scripts/quality/mutation_owners.py`: no change. `http_serve` (`src/http/serve.rs`, filter `http::serve::`) already exists and its tests drive `serve_h1`.
- `docs/refactor/WIRE-MATRIX.md:9` names `serve_h1` without a path and stays true (optional: add `src/http/serve.rs`).
- Frozen `docs/quality/legacy-*.json` files keep their stale `src/http.rs` `serve_h1` rows. They are immutable (sha-pinned in `docs/quality/policy.json`), and those diagnostics were already `#[expect]`ed before the move.

### C2 — "A panicking request handler is logged and counted on its runtime's task record"

**`src/tasks.rs`**
- Add `use std::sync::atomic::{AtomicU64, Ordering};` after line 22. The `tasks` submodules use explicit imports, so nothing clashes.
- Add a new field to `struct Inner` (174-179):
  ```rust
  /// Connection tasks their accept loop reaped as panicked (item 37). The
  /// loop owns and joins them, never this supervisor; only the count lives
  /// here, on the record the health and debug surfaces read, because a
  /// panicked handler's client sees nothing but a closed socket.
  connection_panics: AtomicU64,
  ```
- In `TaskSupervisor::new` (302-313), add `connection_panics: AtomicU64::new(0),`.
- In `impl TaskSupervisor`, after `shutdown_request`:
  ```rust
  /// The accept loop's report of a connection task that ended in a panic
  /// (see `http::serve_h1`).
  pub(crate) fn record_connection_panic(&self) {
      self.inner.connection_panics.fetch_add(1, Ordering::Relaxed);
  }
  ```
- In `impl TaskMonitor`, after `phase`:
  ```rust
  /// Connection tasks reaped as panicked since the runtime started; a
  /// runtime whose supervisor is gone reports none.
  pub(crate) fn connection_panics(&self) -> u64 {
      self.inner
          .upgrade()
          .map_or(0, |inner| inner.connection_panics.load(Ordering::Relaxed))
  }
  ```
- Module doc lines 8-10 become: "…(the HTTP accept loop owns its connections itself and reports only a panicked one here, see `http::serve_h1`)."
- Ratcheted scopes: none. The expects in tasks.rs are per-fn (`Inner::snapshot`, `Inner::phase`, `TaskMonitor::unready_reason`, `TaskSupervisor::spawn`, `TaskSupervisor::cancel`), and `new` has none. The new code sits outside all of them.
- This is not a synchronization change: a Relaxed monotonic counter with no ordering protocol, so no Loom or held-commit obligation.

**`src/tasks/tests.rs`**: R2's test.

**`src/http/serve.rs`**
- New free fn above `serve_h1`:
  ```rust
  /// A reaped connection's verdict (item 37). hyper does not catch a panic in
  /// the service or its response body, so a panicking handler unwinds the
  /// connection task and its client sees only a closed socket: this
  /// JoinError is the one place the server learns of it. A cancelled task
  /// is the shutdown's own abort, not a fault.
  fn reap(tasks: &crate::tasks::TaskSupervisor, joined: Result<(), tokio::task::JoinError>) {
      match joined {
          Err(error) if error.is_panic() => {
              tracing::error!("connection task panicked; its request got no response: {error}");
              tasks.record_connection_panic();
          }
          Ok(()) | Err(_) => {}
      }
  }
  ```
  - `JoinError` is not a domain enum, and there is no `_ =>` arm.
  - `clippy::single_match` does not fire, because the first arm has a guard.
  - `needless_pass_by_value` does not fire, because the by-value binding consumes `joined`.
- In `serve_h1`, the select arm (inside `tokio::select!`; macro tokens are opaque to the fact scanner) becomes:
  `Some(joined) = conns.join_next(), if !conns.is_empty() => reap(&tasks, joined),`
- The drain `while conns.join_next().await.is_some() {}` becomes (+2 lines):
  ```rust
  while let Some(joined) = conns.join_next().await {
      reap(&tasks, joined);
  }
  ```
  - I rejected a separate `drain(conns)` helper. Its FnValue mutant (`()`) would drop the JoinSet, which aborts without joining. That near-equivalent mutant has no deterministic killer.
- **Ratcheted scope:** `serve_h1`'s expect grows by +2 `scope_lines` and a few new facts (`reap` call-site and path facts). Relative to origin/slate the identity is already new (C1), so nothing fails if C1 and C2 are pushed together. The gate compares against the push's `before`, so if C1 is pushed alone first, C2 would fail with `accepted exception grew … scope_lines 58 -> 60`.
  - Remedy, applied in C2 regardless: re-decide the reason. The loop now does reap its tasks, so the text changes on its merits. New text, with exactly two `;` and no `"`:
    `reason = "serve_h1; each accepted connection is served by a task the listener's own JoinSet owns, reaps (counting a panicked one) and joins at shutdown, and nodelay and connection errors are routine client behaviour; a supervised task per connection and handled connection results would restate what the JoinSet already owns"`
  - Both expectations stay fulfilled: `conns.spawn` and the two `let _ =` are unchanged.
- Tests: the R3 routes, the helper and the test.

**`src/http.rs`** (debug_load, line-neutral)
- Lines 992-993 (2 lines) become 1 line:
  `        // PR 6-F / item 37: supervised loops, the first critical exit and panicked connections.` (96 columns)
  - The dropped parenthetical, "(readiness adopts it in WP-15's remaining slice)", is stale: `health_axum` already calls `state.tasks.unready_reason()` (http.rs:1702).
- After line 996, add `            "connection_panics": state.tasks.connection_panics(),`.
- **Ratcheted scope:** `debug_load`'s `#[expect(clippy::too_many_lines, clippy::cast_possible_truncation, reason = "debug_load; …")]`.
  - `scope_lines` does not change (−1 +1).
  - `syntax_facts` does not change: the edits sit inside `serde_json::json!`, which the scanner records as one `macro` fact and one `macro-tokens` fact however long it is (tools/quality-syntax/src/scan.rs:278-285). This is the same way d93b421b added `cutoff_engine_retired`.
  - The fn stays over 100 lines, so its `too_many_lines` expectation remains fulfilled.
  - Macro-dsl row `crate::debug_load serde_json::json` count 1: unchanged.

**`src/dst/tests/admission_maintenance.rs`**: R1's assertion (+4 lines).

**Ledgers (same commit)**
- `docs/refactor/test-inventory.json`: `python3 scripts/test-inventory.py --write`. The only change is the `function_sha256` of `debug_load_reports_typed_limiter_and_frame_totals`. No name, attribute or scenario changes, and no review-mechanism pin covers it.
  - `review-mechanisms.json` pins `tasks/tests.rs::critical_exits_failures_and_panics_are_reported` by function sha. That function is untouched; the pin is per function, not per file.
- `docs/refactor/WIRE-MATRIX.md:207`: append to the `/v1/debug/load` row: "; `tasks.connection_panics` is this runtime's count of connection tasks the accept loop reaped as panicked (item 37: each one a request answered with a closed socket)".
- Optional: `RUNBOOK.md:310` debug/load row, add `tasks.connection_panics` ("nonzero = a handler panicked; see the ERROR log").
- No changes to `owners.json`, `source-allowances.json`, `mutation_owners.py`, architecture-policy, the scenario map or `src/dst/tests/README.md`. There is no new static, macro, spawn, file or DST module.

## 5. Mutation analysis (cargo-mutants 27.1.0, `--in-diff`)

Facts checked in the 27.1.0 source:

- FnValue spans run from the first to the last body statement.
- `impl` fns named `new` are skipped (`visit_impl_item_fn`), so `TaskSupervisor::new` yields nothing.
- There are no `if`-condition mutants and nothing inside macro tokens (`select!`, `json!`).
- Match guards get true and false mutants.
- A deletion marks the new-file lines before and after it.

**C1**

| Mutant | Killer | Expected failure |
|---|---|---|
| `src/http/serve.rs`: `replace serve_h1 -> std::io::Result<()> with Ok(())` (whole body inserted) | `http::serve::tests::*` | The listener drops at return: `connect` is refused (`unwrap` on ECONNREFUSED), or a backlogged socket is reset at once and fails "closed on sight, not at the deadline" or `closed before the response head completed`. Every one is bounded by BOUND = 4 s, so none times out. |

`src/http.rs` in C1: the deletion marks 1160/1161 (the `}`/blank after `debug_usage_reconcile`, outside its last-statement span) and the router's doc line (not in `router`'s span). The re-export sits outside every fn. So the `http` owner is selected with **no executable mutants**, which the driver reports explicitly.

**C2** (the C1 row repeats when both commits go up together)

| Owner (filter) | Mutant | Killer | Failure |
|---|---|---|---|
| http_serve (`http::serve::`) | `serve_h1` → `Ok(())` | existing rig tests + R3 | as above |
| http_serve | `reap` → `()` | R3 | `left: 0, right: 1` |
| http_serve | guard `error.is_panic()` → `false` | R3 | `left: 0, right: 1` |
| http_serve | guard `error.is_panic()` → `true` | R3 (the parked `/hang` connection is aborted, so it is reaped as `Cancelled`) | `left: 2, right: 1` |
| tasks (`tasks::`) | `record_connection_panic` → `()` | R2 | `left: 0, right: 2` |
| tasks | `TaskMonitor::connection_panics` → `0` | R2 | `left: 0, right: 2` |
| tasks | `TaskMonitor::connection_panics` → `1` | R2 (first assert) | `left: 1, right: 0` |
| http (`… debug_surface_ …`) | `debug_load` → `Default::default()` | `debug_surface_serves_every_handler_with_the_token` (also `livefeed_engine_retired`) | `GET /v1/debug/load -> 200: not JSON (EOF while parsing a value …)` |

That is 8 mutants, all caught. None can hang:

- R3 is bounded by BOUND and the 5 s shutdown grace.
- R2 is synchronous.
- The `hung` connection is proven accepted (it answers `/fast`) before it parks, so the guard→true kill does not depend on accept order.

Equivalent mutants: none. The `drain()` helper that would have created one was rejected in §4.

Owner rows and filters: none change. New tests fall inside the existing filters: `http::serve::tests::…` matches `http::serve::`, `tasks::tests::…` matches `tasks::`, and R1's test is not a mutation killer.

## 6. Ledgers (summary)

| Ledger | C1 | C2 |
|---|---|---|
| docs/quality/owners.json | effect row path → `src/http/serve.rs`; new macro-dsl `tokio::select` row for `crate::serve_h1` @ serve.rs | — |
| docs/quality/source-allowances.json | prune `macro-dsl src/http.rs crate::serve_h1 tokio::select` | — |
| scripts/quality/mutation_owners.py | — (http_serve exists) | — |
| docs/refactor/test-inventory.json | — | `--write`: one function_sha256 (admission_maintenance) |
| docs/refactor/review-mechanisms.json | — | — (pinned tasks test untouched) |
| docs/refactor/WIRE-MATRIX.md | optional path note on line 9 | `/v1/debug/load` row: `tasks.connection_panics` |
| RUNBOOK.md | — | optional key mention |
| architecture-policy / scenario map / dst README / owners globals | — | — |

## 7. Controls (exact commands, expected output)

**Before C1**, as a baseline:

- `wc -l src/http.rs src/http/serve.rs src/tasks.rs src/dst/tests/admission_maintenance.rs` → 3225 / 211 / 403 / 918.

**After C1:**

1. Verbatim proof:
   ```
   diff <(git show HEAD~1:src/http.rs | sed -n 1162,1227p) <(sed -n '/^\/\/\/ #269: the one h1 serve loop/,/^}/p' src/http/serve.rs)
   ```
   Expect exactly one hunk: `<     let h1 = serve::h1_builder(http);` / `>     let h1 = h1_builder(http);`.
2. `wc -l src/http.rs src/http/serve.rs` → `3159`, `279`.
3. `cargo fmt --all -- --check` → no output, exit 0.
4. `cargo test --locked --lib http::serve::` → `test result: ok. 4 passed`.
5. `cargo test --locked --lib -- security_workload:: livefeed_engine_retired debug_surface_` → all ok. These serve through the moved loop.
6. `scripts/quality.sh` → `QUALITY_OK`. In particular none of:
   - `primitive-spawn exception needs a registered function owner: src/http/serve.rs: crate::serve_h1`
   - `unregistered source occurrence (1): ('macro-dsl', 'src/http/serve.rs', 'crate::serve_h1', 'tokio::select')`
   - `obsolete source allowances`
   - `file growth: src/http.rs`
7. Mutation plan, as CI runs it:
   ```
   QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate \
     python3 scripts/quality/verification_plan.py --out target/quality-plan && scripts/quality/mutations.sh
   ```
   Expect selected owners `http`, `http_serve`; `serve_h1` FnValue caught; `http: no executable mutants in the selected scope`; `MUT_EXIT=0`.

**C2 red sequence** (record in the commit message):

1. R1 only: `cargo test --locked --lib debug_load_reports_typed_limiter_and_frame_totals` → the §3 R1 message (`left: Null`, `right: 0`).
2. R2 added: `cargo test --locked --lib tasks::` → build fails with the two E0599 errors.
3. tasks.rs half added: R2 is green. Add R3: `cargo test --locked --lib http::serve::tests::a_panicking_handler` → `left: 0`, `right: 1`.

**After C2:**

1. `cargo test --locked --lib http::serve::` → `5 passed`.
2. `cargo test --locked --lib tasks::` → prior count + 1, all ok.
3. `cargo test --locked --lib -- debug_load_reports_typed_limiter_and_frame_totals debug_surface_` → `3 passed`.
4. `python3 scripts/test-inventory.py --write && python3 scripts/test-inventory.py --check` → clean.
5. `wc -l src/http.rs` → `3159`. `git diff HEAD~1 -- src/http.rs | grep -c '^[+-] '` → 4 (two comment lines out; one comment line and one key in).
6. `cargo clippy --locked --workspace --all-targets -- -D warnings` → clean. `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items` → clean.
7. `scripts/quality.sh` → `QUALITY_OK` (no `accepted exception grew` for `crate::debug_load` or `crate::serve_h1`).
8. The mutation plan as in C1 step 7 → owners `http`, `http_serve`, `tasks`; 8 mutants, 8 caught; `MUT_EXIT=0`.
9. Full gate run (the orchestrator's) → suite green. Inventory floor +1 is not DST: R2 and R3 are unit tests.

## 8. Out of scope

- **Step B**, readiness on poisoned locks: decision D2.
- **Graceful in-flight drain at shutdown.** `conns.abort_all()` still cancels every in-flight request. That belongs to the WP-15 §9 slice with its SSE-cancellation and supervisor-ordering preconditions, as the reviewer says.
- **Panics in detached request-scoped spawns** that the accept loop never joins: `sse/session.rs::serve`, `<GatedSseBody as Stream>::poll_next` (sse/auth.rs), `LiveFeed::schedule_transition_retry` (sse/feed/drive.rs), `PostingsCache::spawn_load`, `TouchJournal::start`. A panic there ends that task silently, and the connection sees an EOF, not a panic. The shard open path is already unwind-proof (sharddir/unwind.rs).
- **Answering a panic with a 500** through a catch-unwind layer: rejected. It is a wire change, and it would keep serving from possibly poisoned state, which RUST-QUALITY forbids as silent recovery.
- **A test that captures the log line** (see §3).
- **Moving the `NOFILE_*` statics, `raise_nofile` or `spawn_runtime_watchdog` into serve.rs.** It is not needed. It would move global-static allowance rows for no behaviour gain.

## 9. Decisions for Søren

**D1: the new `/v1/debug/load` key.** Chosen: `tasks.connection_panics` (u64, per runtime, monotonic since boot), inside the existing `tasks` block. It is additive; the key-picking scripts in `bench/` ignore unknown keys; no test asserts the block's key set.
- Backward-compatible alternatives:
  - (a) a top-level `connection_panics` key;
  - (b) no key, log line only. The panic then stays invisible to samplers such as `bench/soak/poll.py`.
- Also chosen, and reversible: the counter lives on the runtime's task record, not in a process static, per a1cf29f3.

**D2: step B, readiness on a poisoned process-wide lock** (policy; not implemented).

Today:
- Several process-wide owners deliberately panic on poison, each pinned by a poison test:
  - `PeerClient.peer_urls` (peer.rs:61,69,78; test at 255)
  - `OwnershipService.view` (ownership.rs:177)
  - `AdmissionController.inner.streams` (admission.rs:634)
  - `UsageService.map` (usage.rs:800)
  - `BillingService` cursors (billing_service.rs:408,424)
- A lock is poisoned only by a panic while its guard is held. After that, every request that touches it panics, and its connection resets.
- Meanwhile `/health` and `/readyz` (both `health_axum`) keep answering 200, because none of their inputs touches those locks.
- The one exception is the supervisor's own state: `TaskMonitor::unready_reason` unwraps, so /health itself resets.
- The router's comment (http.rs:1234-1250 before C1; 1167-1183 after it) states that mid-life failures deliberately do **not** unready an instance ("a store blip must not cascade every instance out of rotation"). The precedent in the other direction is `critical-runtime-exit`: a dead critical loop already makes /health and /readyz answer 503.

With step A:
- Every one of those resets increments `tasks.connection_panics`. The poisoned state therefore becomes observable on `/v1/debug/load` and in the ERROR log.

Options:
1. **The reviewer's B.** Each owner exposes `is_poisoned()`, and `health_axum` answers 503 naming the owner. There is no recovery and no `panic=abort`.
   - Risk: a deterministic poison pill sent to every instance takes the whole fleet out of rotation, instead of leaving a partial outage.
   - The red test needs a `cfg(test)` poison hook on `PeerClient`, because `inner` is private to peer.rs.
2. **Exit instead.** A poisoned owner requests the supervisor's shutdown, and the platform restarts the instance. Same fleet risk, but self-healing.
3. **Backward-compatible.** No readiness change. Add `tasks.poisoned: [owner…]` to `/v1/debug/load` and alert on `tasks.connection_panics` growth. Operators restart.

Recommendation: 3 now (it needs only step A plus one debug field), with 1 or 2 decided together with WP-15's readiness slice.

---

## Skeptic corrections (C1..C6)

I checked these against the tree at 33fbd10e using reads only: `git show`, `sed`, `grep` and `wc`, plus the locked sources of cargo-mutants 27.1.0, hyper 1.10.1, axum 0.8.9 and tokio 1.52.3 in `~/.cargo/registry`.

**Claims I confirmed**

- Serve loop location and span:
  - The loop is at `src/http.rs:1162-1227`, with the blank line at 1228. That is doc 3 + `#[expect]` 5 + fn 58.
  - The reap arm is at :1220 and the drain at :1224-1225.
  - `mod serve;` is at :3213 and the `read_adapter` re-export at :3215.
- The only non-bin `JoinSet` in `src` is this one.
- `catch_unwind` appears only in `hyper-1.10.1/src/ffi`. It does not appear in hyper-util 0.1.20 or axum 0.8.9. `JoinError` Display carries the payload (`tokio-1.52.3/src/runtime/task/error.rs:135-152`).
- Callers:
  - bootstrap.rs:902 and :660, and fixture_http.rs:473/534/543, are as the plan quotes them.
  - read_peer_compatibility.rs:203 passes `tasks`, which is bound at :197 as `owner.tasks.clone()`.
  - The serve.rs:78 rig and the doc mentions (tasks.rs:10, http.rs:2920, config/model.rs:460, WIRE-MATRIX.md:9, security_workload.rs:767) all stay true through the re-export.
- Ledger mechanics:
  - `source_rules.exception_growth` skips a new identity (path or reason changed), at source_rules.py:204-216.
  - The primitive-spawn check needs an `effect` row with the same path and qualified name (source_rules.py:250-257). So the owners.json effect row at :2076-2082 must move to `src/http/serve.rs`.
  - The `macro-dsl src/http.rs crate::serve_h1 tokio::select` row (source-allowances.json:2880-2885) becomes stale and must be pruned. The new row must go to owners.json, because `legacy source allowance grew` forbids re-adding it to source-allowances.
- The plan's own ledger claims hold:
  - `debug_load`'s expect is line-neutral and fact-neutral, because json! is one `macro` and one `macro-tokens` fact (tools/quality-syntax/src/scan.rs:278-285).
  - None of tasks.rs's per-fn expects covers the new code.
  - test-inventory.json covers only DST (517 entries). `debug_load_reports_typed_limiter_and_frame_totals` has no scenarios and no review-mechanism pin.
  - review-mechanisms pins `critical_exits_failures_and_panics_are_reported` per function (review-evidence.py:178-181).
- `src/tasks/tests.rs` starts with `#![cfg(test)]`. So production_changes treats it as production-unchanged and it needs no mutation-owner row.
- Architecture: `reverse_edges` on serve.rs finds no http or product edge. Neither `use super::{NOFILE_*, raise_nofile, spawn_runtime_watchdog}` nor `super::super::serve_h1` matches.
- Mutation analysis:
  - `visit_impl_item_fn` skips `new` (cargo-mutants visit.rs:454-468).
  - `visit_expr_match` deletes arms only when there is a `Pat::Wild`, and gives each guard true and false mutants (visit.rs:645-700). `Ok(()) | Err(_)` is not `Pat::Wild`, so no arm-deletion mutant exists.
  - The `debug_load -> Default::default()` mutant is killed by `debug_json`'s "not JSON" panic (security_routes.rs:713-717) inside the `http` owner's `debug_surface_` filter.
- `Value: PartialEq<i32>` holds, so R1 compiles and the red reads `left: Null / right: 0`.
- Neither `bench/` nor `src` reads `["tasks"]` on /v1/debug/load, so D1 is additive for every reader found.
- D2 line citations check out (peer.rs:61/69/78/255, ownership.rs:177, admission.rs:634, usage.rs:800, billing_service.rs:408/424). The router comment starts at http.rs:1235.

**C1: the merge base moved. origin/slate is now 33fbd10e (§4, §7).**
- `git rev-parse origin/slate` = 33fbd10e14cd…, and `git merge-base HEAD origin/slate` = HEAD.
- So the http.rs ceiling is **3,225** for any push of C1 and C2. The "3,314 if pushed together" branch in §4 no longer applies; delete it.
- 3,225 → 3,159 after C1 and 3,159 after C2 is still correct: 1162-1228 is 67 lines, plus 1 re-export line.
- The push `before` for a combined push is 33fbd10e. The serve_h1 expect identity is new under both push orders, because of the path change, the new reason, or both.

**C2: R3's `/hang` route fails `clippy -D warnings` (§3 R3, §4 serve.rs tests).**
- `axum::routing::get(|| std::future::pending::<()>())` is a zero-argument closure that only calls a function path. `clippy::redundant_closure` is in the `style` group, which the workspace enables through `all = warn` (Cargo.toml:121) and CI denies. `--all-targets` lints the `#[cfg(test)]` rig.
- Write it as `.route("/hang", axum::routing::get(std::future::pending::<()>))`.
- That still compiles: a fn item `pending::<()>` is `FnOnce() -> Pending<()> + Clone + Send + Sync + 'static`, and `Pending<()>: Future<Output = ()> + Send`.
- The named `async fn panicking_handler() -> &'static str` is correct as planned: in edition 2024 an `async { panic!() }` block's output falls back to `!`.

**C3: R1 as written cannot fail a broken wiring. Strengthen it (§3 R1).**
- R1 asserts only `0`. A `debug_load` that emits `"connection_panics": 0` as a literal, or reads another supervisor, passes it green.
- No other test carries a nonzero count through AppState's monitor to the JSON. R3 reads the supervisor directly, and json! tokens are not mutated, so no mutant covers the key.
- `HttpRig.tasks` is `pub(super)` (fixture_http.rs:84), and it is the same supervisor the rig's `serve_h1` and `AppState.tasks` use (:473/:534).
- Restructure the test's opening:
  `let rig = http_rig_build(...).await; rig.tasks.record_connection_panic(); let (_state, addr) = rig.parts();`
  The serve task's `serve_tasks` clone keeps `Inner` alive after `parts()` drops `rig.tasks`. Then assert `before["tasks"]["connection_panics"] == 1` with the same message.
- Red on the current tree becomes the E0599 build error for `record_connection_panic` (same text as R2).
- Red for a runtime-only run: stage the tasks.rs half first, so the result reads `left: Null / right: 1`.
- It stays green, and exact, because the count is per runtime.
- Line cost: about +3 on top of the +5 below. admission_maintenance.rs goes 918 → about 926, under the 1,000 DST ceiling.

**C4: line counts after rustfmt (§3 R1, §4 table).**
- rustfmt lays out an `assert_eq!` whose arguments do not fit on one line one argument per line. So R1's assertion is 5 lines, not 4 (918 → 923 before C3).
- R3's `assert_eq!(closed_within(&mut panicked, BOUND).await, Ok(()), "…")` lines exceed 100 columns and will split the same way.
- Treat the serve.rs figures (~335) and admission_maintenance figures as post-`cargo fmt`. Neither file is near a ceiling.

**C5: control C1-7 and D-list wording (§7, §9).**
- `QUALITY_BEFORE_SHA=$(git rev-parse origin/slate)` now resolves to 33fbd10e, which is correct.
- Update the prose "origin/slate = 2f2c3015" in the header and §4.
- The router comment D2 cites is at http.rs:1235-1251 before C1 (1169-1185 after), not 1234-1250. This is cosmetic.

**C6: the reap arm match (confirmation, no change).**
- `match joined { Err(error) if error.is_panic() => …, Ok(()) | Err(_) => {} }` is lint-clean:
  - `single_match` requires both arms to be unguarded.
  - `match_same_arms` does not apply, because the arm bodies differ.
  - `needless_pass_by_value` sees the by-value binding `Err(error)` as a consume of `joined`.
- I rejected the alternative `if let Err(error) = joined && error.is_panic()` because it adds no value.
- The only mutants are FnValue `()` and guard true/false, and R3 kills all three as the plan says. That is 8 mutants in total, or 9 if C1 and C2 are counted separately.

**Missed ledgers**
- None beyond the plan's list.
  - C1: owners.json effect path, owners.json new macro-dsl row, source-allowances prune.
  - C2: test-inventory `--write` for the admission_maintenance sha; the WIRE-MATRIX:207 row.
- Optional: the WIRE-MATRIX:9 path note and RUNBOOK:310.
- Not needed:
  - mutation_owners: `http_serve` already owns serve.rs, and tasks/tests.rs is `#![cfg(test)]`.
  - architecture-policy, scenario map, dst README, owners globals: the NOFILE statics stay in http.rs; only readers move.

**Unbuildable controls:** none after C2. As written, C2-6 (`cargo clippy … -D warnings`) fails on `redundant_closure` in the R3 rig route.

**Verdict: ready-with-corrections** (apply C1-C4; C5 is cosmetic).
