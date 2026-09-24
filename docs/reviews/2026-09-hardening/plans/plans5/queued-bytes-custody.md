# Item 31: the queued-byte charge belongs to the committer request, not the handler future

Tree: `slate` at `0afa2597` (= origin/slate). All line numbers were re-read on this tree. The review's
citations are stale: product.rs 2060-2075 is now 2046-2058, append.rs 330-333 is now 362-400, and
contract.rs 340 is now 366.

---

## 1 Problem (verified on the current tree)

**The charge is a local of the handler.** `src/product.rs:2046-2058` (`product_append_inner`):

```rust
    // SR2-4 queued_append_bytes: the body is charged to the project
    // BEFORE the committer sees it and released when this handler's
    // awaited append DECIDES (the guard drops on return, every path).
    let _queued_charge = if let Some(p) = principal {
        match state
            .quotas
            .charge_queued(&p.project_id, &p.quotas, body.len() as u64)
        {
            Ok(g) => g,
```

Nothing hands `_queued_charge` on. The append command that the handler builds carries only the
raw buffering charge, and on this surface that charge is always None
(`src/product.rs:2313`, `body_charge: None,`).

**The handler stops waiting while the request is still queued.** `src/application/append/submit.rs:79-92`:

```rust
    if engine.try_enqueue(req).is_err() {
    ...
    match tokio::time::timeout(APPEND_TIMEOUT, rx).await {
        Ok(Ok(o)) => Ok(o),
        _ => fail(
            FailureClass::Timeout,
            AppendCode::AppendTimeout,
            "append timed out; outcome unknown",
```

`APPEND_TIMEOUT` is 10 s (`src/application/append.rs:19`). When it expires, `execute_prepared` returns
and `product_append_inner` returns, and `_queued_charge` is dropped. The `AppendReq`, with its `entries`
(the payload), is still sitting in the engine's mpsc queue or in the committer's `first` slot
(`src/shard.rs:2582-2584`: `let _hold = self.commit_gate.lock().await;` … `let mut ops = vec![first];`).
A client disconnect has the same effect sooner. Hyper's h1 dispatcher (`serve::h1_builder`, half-close
off) drops the service future on EOF, which drops the handler future and the guard with it.

**The committer is what actually holds the bytes.** It drops an `AppendReq` in these places:
- `ShardEngine::try_enqueue`: on refusal, or when a close-only request is converted (`src/shard.rs:1804-1822`).
- The closed-engine drain: `CommitOp::Append(r) => { let _ = r.resp.send(Err(AppendErr::Moved)); }` (`src/shard.rs:2541`).
- `CommitTransaction::reject_op`.
- The end of `CommitTransaction::append`/`accept_append` (`src/shard/transaction/append.rs:19-253`).
  In `accept_append`, `req.resp` moves into `self.effects.acks`. The rest of `req` drops at function
  exit, after the frames are staged into the `WriteBatch` (`:222`) and before the local write. The
  frames enter project pressure only at publish (`src/shard/transaction/publish.rs:46`
  `b.frames_added(local.frames.added_bytes)`).

**The documentation promises the wrong owner.**
- `src/quota.rs:78` says `/// SR2-4 queued_append_bytes: bytes admitted but not yet decided.`
- `src/quota.rs:439-440` says `/// Releases the queued-byte charge when the append is DECIDED (the` / `/// handler's await returns, success or failure).`
- `src/quota.rs:863-865` says `/// BEFORE the append is enqueued; the guard releases when the` / `/// append DECIDES.`
- `docs/refactor/WIRE-MATRIX.md:118` says "Project `queued_append_bytes` charged for the handler's lifetime".
- `docs/LIVE-FEED.md:291` says `queued_bytes (exact append-queue occupancy)`. That is false today.

**Consequence.** During a committer stall (a blocked `db.write` or a stalled WAL flush), every caller
that times out after 10 s or disconnects removes its bytes from `queued_bytes`. Those bytes stay in the
committer queue, and the queue is the memory the round-13.3 field finding (A1) was about. As a result:
- `estimated_pressure_bytes()` (`src/quota.rs:138-146`) and the project memory latch under-report exactly while the stall lasts.
- The `queued_append_bytes` ceiling (429 `queued_bytes`) re-admits a retry storm on top of the abandoned queue.
- `/v1/debug/load` shows `queued_append_bytes` too low.

**The raw surface never charges.** The review asked us to check this. `charge_queued` has exactly one
caller, `src/product.rs:2051`. Raw `append_typed` (`src/http.rs:2740-2860`) puts only the buffering
guard into `body_charge`, and `execute_once` drops that guard before enqueue
(`src/application/append.rs:359` `drop(command.body_charge.take());`). Raw is also the deployment
tenant (`raw_adapter_sref`), which has no project quota. Raw is out of scope (§8).

### The reviewer's Change, and what is buildable

The direction is right: the committer's `AppendReq` must co-own the charge. Taken literally
("move `queued_charge` into a new AppendReq field"), the change is **wrong in two ways**, and it
understates the cost of building it.

1. **Retries lose the charge.** `execute_prepared` runs `execute_once(self, prepared, &mut command)` up
   to 4 times (`src/application/append.rs:166-189`). A `take()` into the first attempt's `AppendReq`
   releases the charge as soon as the committer decides that attempt (a closed-segment verdict), so
   attempts 2-4 are queued uncharged.
2. **The happy path moves the gap instead of closing it.** A single owner releases at the end of
   `accept_append`, before the local write and the durable answer. The caller is still waiting and
   still holds `body`, and the staged frames are not yet counted. That is the same under-report,
   shifted into the `db.write`-stall window that `submit.rs:62-78` names as the wedge mode.
   **Buildable fix:** shared custody. `AppendCommand.queued_charge: Option<Arc<QueuedBytesGuard>>`
   lives as long as the caller's `execute_prepared`, and every attempt's `AppendReq` gets a `clone()`.
   The charge ends at max(the caller is answered or abandoned, the committer drops the request).
3. **The AppendReq field is never read**, so `-D warnings` fails on `dead_code` for "field is never
   read". It must be named `_queued_charge`, following the RAII-holder precedent of `_gather` and
   `_bytes` (`src/history.rs:303-304`) and `_slot` (`src/sse/auth.rs:330`).
4. **Cost.** There are 20 `AppendReq` literals, not just the one in `append.rs`, and 4 `AppendCommand`
   literals. 13 of those functions carry `#[expect]`s (21 attributes). history.rs and shard.rs are at
   their ceilings, and product.rs and http.rs grow. The red test cannot go into
   `quota_enforcement.rs`, because 946 + ~130 lines crosses 1,000.

A `Bytes::from_owner` charge that follows the payload would need no field, and
`crate::retained_bytes::with_charge` is precedent for it. It does **not** work here:
`crate::application::creation::json_entries` re-serializes every JSON record into a new allocation
(`src/application/creation.rs:241-246`), so the committer's entries never alias the charged body.

---

## 2 Contract decision

**Typed contract (owner: quota / append application):**
- `AppendCommand.queued_charge: Option<Arc<crate::quota::QueuedBytesGuard>>` is Some only on the product surface for a verified principal.
- `AppendReq._queued_charge: Option<Arc<crate::quota::QueuedBytesGuard>>` is the committer's share. It is held and never read.
- Invariant: a project's queued charge for one payload ends only when the append command (the caller's
  `execute_prepared`) and every committer request built from it have been dropped.
- `QueuedBytesGuard` keeps private fields, and `charge_queued` is still its only constructor. Its code is unchanged.

**Wire codes: unchanged.** 429 `queued_bytes` (audit `project_queued_bytes`, retryable, no retry-after)
and 429 `project_memory_pressure` (`retry-after: 1`), `src/product.rs:855-921`.

**Visible at the product edge (decision for Søren).** A product append that times out (the 10 s timeout)
or disconnects while the committer still holds it now keeps counting toward:
- the project's `queued_append_bytes` ceiling, which can answer 429 `queued_bytes`;
- the memory-pressure estimate, which can answer 429 `project_memory_pressure`.

It counts until the committer drops the request (staged, rejected, or failed on close). So during a
stall, a project that retries abandoned appends is refused sooner instead of stacking a second copy into
the queue. On the operator surface, `/v1/debug/load` rows (`queued_append_bytes`,
`estimated_pressure_bytes`) now include bytes the committer holds for abandoned callers. The raw surface
(`/v1/stream/*`) is unchanged, because it never charges.

**Backward-compatible alternative.** Keep refusals keyed to handler-held bytes, and report the bytes the
committer holds for abandoned callers in a separate, non-refusing counter (for example
`abandoned_queued_bytes` on `/v1/debug/load`). No refusal would change timing, but the latch and ceiling
would stay blind to the memory A1 showed matters, and it needs a second counter plus its own guard.
**Not recommended.**

---

## 3 Red tests

There is one new DST module, `src/dst/tests/quota_queued_custody.rs` (new file, about 130 lines, under
the 1,000-line cap). It contains no `tokio::spawn` (so no effect row), no `json!`/`select!` (so no
macro-dsl row) and no `use super::*` (so no glob row).

```rust
//! Queued-append custody (§17.3 `queued_append_bytes`): the charge a verified
//! append takes rides the append command into every committer request built
//! from it, so a caller that times out or disconnects cannot release bytes the
//! committer still holds.

use super::fixture_auth::{auth_rig, mint_token, rig_create};
use super::fixture_http::engine_shutdown;
use super::fixture_requests::PRISMA_KEY;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Every wait outlasts the 10 s append timeout: a handler that misses the
/// disconnect still ends by timing out, and no wait can hang the suite.
const BOUND: Duration = Duration::from_secs(15);
const RECORD_BYTES: u64 = 1024;

fn record() -> String {
    let body = format!("{{\"pad\":\"{}\"}}", "x".repeat(1014));
    assert_eq!(body.len() as u64, RECORD_BYTES);
    body
}

/// The project's queued-append bytes as the operator load surface reports them.
fn queued_bytes(state: &crate::http::AppState, project: &str) -> u64 {
    let load = state.quotas.memory_pressure_json(0, 64);
    load["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["project"] == project)
        .map_or(0, |row| row["queued_append_bytes"].as_u64().unwrap())
}

async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + BOUND;
    while !done() {
        assert!(Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// One product append over a raw connection whose answer the test may never read.
async fn send_append(
    addr: std::net::SocketAddr,
    bearer: &str,
    stream: &str,
    connection: &str,
) -> tokio::net::TcpStream {
    let body = record();
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "POST /v1/streams/{stream}/records HTTP/1.1\r\nhost: {addr}\r\nconnection: {connection}\r\n\
         content-length: {RECORD_BYTES}\r\ncontent-type: application/json\r\n\
         authorization: {bearer}\r\nprisma-encryption-key: {PRISMA_KEY}\r\n\r\n"
    );
    socket.write_all(head.as_bytes()).await.unwrap();
    socket.write_all(body.as_bytes()).await.unwrap();
    socket
}

async fn segment_engine(
    state: &crate::http::AppState,
    project: &str,
    stream: &str,
) -> (Arc<crate::shard::ShardEngine>, [u8; 16]) {
    let sref = crate::tenant::ProjectId::new(project).unwrap().stream_ref(stream);
    let desc = state.registry.get(&sref).await.unwrap().unwrap();
    let seg = desc.resolve_segment("");
    let route = desc.segment_route_by_id(seg.seg_id).unwrap();
    (state.engine_for(&route).await.unwrap(), seg.identity)
}
```

### RED-1 `an_abandoned_append_keeps_its_queued_charge_until_the_committer_drops_it`

This test uses the review's own first step. It runs against the enforce rig with default quotas, which
means no ceiling; the charge is still taken, because it is unconditional.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_abandoned_append_keeps_its_queued_charge_until_the_committer_drops_it() {
    let (_svc, state, addr) = auth_rig("proj-qc", "ws-qc", &["c-qc"], None).await;
    let bearer = mint_token("c-qc", "proj-qc", "ws-qc", 1, 1, "t", 3600);
    rig_create(addr, "qc", &bearer).await;
    let (engine, _) = segment_engine(&state, "proj-qc", "qc").await;
    let hold = engine.test_hold_commit().await;
    let base = engine.appends_enqueued();
    // Keep-alive, not close: with `connection: close` hyper stops reading after
    // the body and never sees the client leave.
    let client = send_append(addr, &bearer, "qc", "keep-alive").await;
    wait_for("the append never reached the committer queue", || engine.appends_enqueued() > base).await;
    assert_eq!(queued_bytes(&state, "proj-qc"), RECORD_BYTES, "a waiting append holds its queued charge");
    drop(client);
    // The product entry's QuotaGuard lives exactly as long as the handler future.
    wait_for("the abandoned append's handler never ended", || state.quotas.stats().1 == 0).await;
    assert_eq!(
        queued_bytes(&state, "proj-qc"),
        RECORD_BYTES,
        "an append the committer still holds keeps its queued charge after its handler is gone"
    );
    drop(hold);
    wait_for("the committer never released the abandoned append's charge", || {
        queued_bytes(&state, "proj-qc") == 0
    })
    .await;
    engine_shutdown(&state).await;
}
```

The handler ends either on the disconnect (hyper h1 with half-close off, `src/http/serve.rs:32-37`) or
on the 10 s `APPEND_TIMEOUT`, and `BOUND` covers both. Both release paths the review names are therefore
exercised against a committer that is still holding the request.

**Exact red on the current tree** (and on C1's tree):
```
---- dst::dst_tests::quota_queued_custody::an_abandoned_append_keeps_its_queued_charge_until_the_committer_drops_it stdout ----
thread 'dst::dst_tests::quota_queued_custody::an_abandoned_append_keeps_its_queued_charge_until_the_committer_drops_it' panicked at src/dst/tests/quota_queued_custody.rs:<line of the second assert_eq!>:5:
assertion `left == right` failed: an append the committer still holds keeps its queued charge after its handler is gone
  left: 0
 right: 1024
```

The first `assert_eq!` (the charge while the handler is waiting) passes on both trees. So the red is
specifically "the handler's exit released the charge". After the fix, all three steps pass: 1024 while
waiting, 1024 after abandonment, and 0 after `drop(hold)`, when `accept_append` drops the
`AppendReq`'s share.

### GUARD-2 `a_staged_append_keeps_its_queued_charge_until_its_caller_is_answered`

This test is **green on the current tree**. Its job is to fail the reviewer's move-only variant: it
pins the shared-custody decision and the unchanged happy-path release point.

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staged_append_keeps_its_queued_charge_until_its_caller_is_answered() {
    let (_svc, state, addr) = auth_rig("proj-qs", "ws-qs", &["c-qs"], None).await;
    let bearer = mint_token("c-qs", "proj-qs", "ws-qs", 1, 1, "t", 3600);
    rig_create(addr, "qs", &bearer).await;
    let (engine, identity) = segment_engine(&state, "proj-qs", "qs").await;
    let handle = engine.stream_handle(identity).await.unwrap();
    let dispatch = engine.test_hold_dispatch().await;
    let mut client = send_append(addr, &bearer, "qs", "close").await;
    // Applied means staged, written and published: the committer already
    // dropped its request; only the waiting caller's command still holds a share.
    wait_for("the append was never staged and applied", || {
        handle.state.lock().unwrap().applied.next >= 1
    })
    .await;
    assert_eq!(
        queued_bytes(&state, "proj-qs"),
        RECORD_BYTES,
        "a staged append whose caller still waits for its answer keeps its queued charge"
    );
    drop(dispatch);
    let mut answer = Vec::new();
    tokio::time::timeout(BOUND, client.read_to_end(&mut answer)).await.unwrap().unwrap();
    assert!(answer.starts_with(b"HTTP/1.1 200"), "{}", String::from_utf8_lossy(&answer));
    wait_for("the answered append never released its charge", || {
        queued_bytes(&state, "proj-qs") == 0
    })
    .await;
    engine_shutdown(&state).await;
}
```

Under the move-only variant (`command.queued_charge.take()` into a single owner), this test fails as:
`assertion `left == right` failed: a staged append whose caller still waits for its answer keeps its queued charge` with `left: 0`, `right: 1024`.

The two functions are 30-40 lines each, with nesting ≤ 3 and no `#[expect]`.

---

## 4 Edits, file by file, in commit order

### Ceilings (the source gate compares against push-`before` = origin/slate)

| file | now | C1 | C2 | final | ceiling |
|---|---|---|---|---|---|
| src/product.rs | 4,205 | −59 | +4 | 4,150 | 4,205 |
| src/shard.rs | 3,232 | −7 | +4 | 3,229 | 3,232 |
| src/http.rs | 3,371 | −2 | +1 | 3,370 | 3,371 |
| src/history.rs | 1,713 | −5 | +2 | 1,710 | 1,713 |
| src/billing.rs 2,201 / auth.rs 1,676 / registry.rs 1,509 / sse/feed.rs 1,200 / fleet.rs 1,143 | — | 0 | 0 | — | untouched |
| src/quota.rs (must stay ≤1,000) | 975 | 0 | +2 | 977 | 1,000 |

Every other touched file is under 1,000 lines, with the largest delta +4. The largest is
`producer_protocol.rs` at 799 → 800. `quota_enforcement.rs` (946) is not touched.

### C1: "Test-only seal, abort and budget-floor helpers move beside their owners' test support, verbatim"

Every edit in C1 removes or adds an item carrying its own `#[cfg(test)]`, or edits an
`#![cfg(test)]` file. The production tokens are therefore unchanged, and the planner classifies all
four ceilinged files as `production_unchanged_files`. No ledger changes.

1. **src/shard.rs**: delete `3212-3218` (`#[cfg(test)]`, `impl ShardEngine { pub(crate) fn test_abort_task(&self, role: &str) -> tokio::task::AbortHandle { self.tasks.abort(role) } }`, and the blank line). Append the `impl ShardEngine { … }` block verbatim, without the redundant `#[cfg(test)]`, to **src/shard/test_support.rs** (`#![cfg(test)]`, `use super::*;`). A child module may read the private `tasks`. Callers are the method calls `runtime_engine_lifecycle.rs:74` and `task_lifecycle_tests.rs:134`, which involve no alias, so no fingerprint moves.
2. **src/history.rs**: delete `279-285` (the two-line doc comment, `#[cfg(test)]`, `pub(crate) fn floored_budget_capacity(configured: usize) -> usize { configured.max(absorb_worst_frame_transient()) }`, and the blank line). After line 31 (`mod test_support;`), add `#[cfg(test)]` and `pub(crate) use test_support::floored_budget_capacity;`. Move the doc comment and the fn verbatim to **src/history/test_support.rs**, which is `#![cfg(test)]` and already has `use super::*`. There are two callers. The first is `history.rs` `tests::worst_frame_floor_serializes_oversized_gathers_without_starvation`, whose `#[expect]`s are `disallowed_methods` and `excessive_nesting`, so its call text and fact counts are unchanged. The second is `config/validation_tests.rs:425`, which has no expects.
3. **src/product.rs**: delete `4039-4101`, the three `#[cfg(test)]` wrappers `enter_sealing_cas`, `claim_seal` and `install_reserved_claim` (the last carries its reasoned `#[expect(clippy::too_many_arguments)]`). Keep line 4102 blank. Insert in their place:
   ```rust
   #[cfg(test)]
   mod seal_test_support;
   #[cfg(test)]
   pub(crate) use seal_test_support::{claim_seal, enter_sealing_cas, install_reserved_claim};
   ```
   **New file src/product/seal_test_support.rs** (about 70 lines):
   - `//! Test-only seal lifecycle entry points for DST scenarios that drive a claim directly, not through the product handler.`
   - `#![cfg(test)]`
   - Explicit imports, with no glob: `use std::sync::Arc; use crate::http::AppState; use crate::application::lifecycle::EnterSeal;`
   - The three fns verbatim, without their `#[cfg(test)]` lines.

   Ledger consequences:
   - The architecture gate skips it as test-only (`#![cfg(test)]` within the first 12 lines).
   - src/product is not a critical prefix, so no owner row.
   - The moved `#[expect]` gets a new path identity and is treated as a new, reviewed exception. Its reason already has exactly two `;`.
   - The callers are `crate::product::claim_seal` and the other two wrappers in the DST files `durability_fences.rs`, `seal_fencing.rs` and `lifecycle_incarnation.rs`, none of which carry an `unwrap_used`/`expect_used` expectation. Nothing is re-fingerprinted. The `#[cfg(test)] pub(crate) use crate::application::lifecycle::EnterSeal;` at 4036-4037 stays, because the DST uses `crate::product::EnterSeal`.
4. **src/http.rs**: replace `2864-2867` with one line, `#[cfg(test)]` / `pub(crate) use crate::application::read::{TEST_ASSERT_KEYED_DENSE, read_merged};`. The paths are identical, and `sse/session.rs`'s adapter export of `TEST_ASSERT_KEYED_DENSE` is unchanged. **Do not** pay for it with a comment edit inside `product_entry_axum_inner`: a changed line inside a fn body selects that fn's `Default::default()` FnValue mutant, and `Response: Default` would make it viable.

### C2: "An abandoned append keeps its queued charge until the committer drops it"

In C2, the red test was first run on C1's tree plus the new test file, and its output is recorded in
the commit message (the precedent is 527d3d3a).

**Production**
1. **src/application/append/contract.rs:366**: after `body_charge`, add:
   ```rust
       /// The project's queued-byte charge for this payload (product surface,
       /// verified principal; None elsewhere). Each committer request built from
       /// the command holds a share, so a caller that times out or disconnects
       /// cannot end the charge while the committer still holds the payload.
       pub(crate) queued_charge: Option<std::sync::Arc<crate::quota::QueuedBytesGuard>>,
   ```
2. **src/shard.rs** `AppendReq` (after `pub billing`, line ~803): add
   ```rust
       /// The project's queued-byte charge, shared with the append command that
       /// built this request: held, never read, so the charge lasts until the
       /// committer drops the request. None for writers no project quota admitted.
       pub _queued_charge: Option<Arc<crate::quota::QueuedBytesGuard>>,
   ```
   The line cost is +4. `CommitOp`'s `large_enum_variant` expectation stays fulfilled (+8 B). There is no derive on `AppendReq`.
3. **src/application/append.rs** `execute_once` (`AppendReq` literal, 362-400): insert `_queued_charge: command.queued_charge.clone(),` before `resp: tx,`. This is +1 line. The clone is required because `command` is `&mut` and reused by the retry loop (not a `redundant_clone`).
4. **src/product.rs**:
   - **2046-2048** comment, 3 lines to 3 lines:
     `// SR2-4 queued_append_bytes: the body is charged to the project` /
     `// BEFORE the committer sees it; the append command and each committer` /
     `// request share the charge, so an abandoned caller never releases it.`
   - **2049** becomes `let queued_charge = if let Some(p) = principal {`, and **2054** becomes `Ok(g) => g.map(Arc::new),`. `std::sync::Arc` is already imported at :13.
   - **2222-2233** (the `submit_product_append(` call): add `queued_charge,` after `seal_auth,`. This is +1 line.
   - **2242-2254** (`submit_product_append`): add the parameter `queued_charge: Option<Arc<crate::quota::QueuedBytesGuard>>,` after `seal_auth`, which is +1. In the `AppendCommand` literal, add `queued_charge,` after `body_charge: None,` (2313), which is +1.
   - **1956-1967** (`product_append_sealing`'s call): add `None,` after `Some(auth),`. This is +1. The seal's final record stays uncharged, as today.
   - Net product.rs: +4.
5. **src/http.rs** `append_typed` literal (2842-2858): add `queued_charge: None,` after `body_charge,`. This is +1.
6. **src/application/consumer/delivery.rs:706** (`dlq_and_settle`): add `queued_charge: None,` after `body_charge: None,`. This is +1.
7. **src/quota.rs**, doc-only:
   - **:78** becomes `/// SR2-4 queued_append_bytes: admitted payload bytes a caller or the committer still holds.`
   - **:439-440** becomes a 4-line doc:
     `/// One append payload's queued-byte charge. The append command and every` /
     `/// committer request built from it share it (`Arc`), so it ends only when the` /
     `/// caller is answered or gone AND the committer has dropped the payload: a` /
     `/// timed-out or disconnected caller never releases bytes still queued.`
   - **:465** becomes `/// ceiling; retry after the committer drains them.`
   - **:864-865** becomes `/// BEFORE the append is enqueued; the charge ends when its last holder` / `/// drops (see `QueuedBytesGuard`). 0 = not configured.`
   - The code is unchanged: `charge_queued` still returns `Option<QueuedBytesGuard>`.

**Uncharged `AppendReq` literals.** Insert `_queued_charge: None,` on the line after `billing: …,`. That is +1 line each, 19 sites:
- src/history.rs:1577 and :1659
- src/dst/runtime.rs:199 and :274
- src/dst/tests/durability_fences.rs:408 and :700
- src/dst/tests/fixture_storage.rs:207 and :268
- src/dst/tests/reads_history.rs:198, :301 and :426
- src/dst/tests/producer_protocol.rs:33
- src/dst/tests/history_absorption.rs:236
- src/dst/tests/read_subset_retention.rs:251
- src/dst/tests/read_page_limits.rs:148
- src/history/controller_tests.rs:75
- src/application/creation/initialization.rs:96
- src/shard/transaction_tests.rs:55
- src/shard/retirement_tests.rs:111

**Uncharged `AppendCommand` literal:** src/dst/tests/append_application.rs:39 gets `queued_charge: None,`.

The closest clippy `too_many_lines` margin among the touched functions with no expectation is
`reads_history.rs::corrupt_postings_fall_back_to_the_envelope`, which goes from 98 to 99 counted lines
(the limit is 100). Every other one is ≤ 92.

**Tests and registration:**
- New file src/dst/tests/quota_queued_custody.rs (§3).
- **src/dst/dst_tests.rs**: after `mod quota_enforcement;` (line 130), add `#[path = "tests/quota_queued_custody.rs"]` / `mod quota_queued_custody;` and a blank line. This is +3.

### `#[expect]`-ratcheted functions touched, with the remedy

In every case below the function grows by +1 scope line and the new path fact `None` (or a
clone/`Arc::new` call), which trips the ratchet. Each reason is re-decided. Every new text has exactly
two `;` and no `"`.

**src/product.rs**
1. **`product_append_inner` `[too_many_arguments]`**
   - Old: "product_append_inner; the parameters are the request's typed context parts, not tunables; a bundle struct for this single call site would only rename the same positional list"
   - New: "product_append_inner; the parameters are the request's typed context parts, not tunables, and the queued charge taken from them travels on inside the append command; a bundle struct for this single call site would only rename the same positional list"
2. **`product_append_inner` `[fn_params_excessive_bools, too_many_lines]`** (both stay fulfilled; the architecture budget is 213 → 214 of 251)
   - New: "product_append_inner; the append carries its sealing and close flags as the two independent facts the wire names and charges, validates, admits and hands to the command in one sequence; an enum or a split would restate the flags and separate the steps from the request they share"
3. **`submit_product_append` `[too_many_arguments]`**
   - New: "submit_product_append; parsed protocol fields and the caller's queued charge converge here into one typed application command; bundling them earlier would parse the wire shape twice"
4. **`product_append_sealing` `[too_many_arguments]`**
   - New: "product_append_sealing; the seal's final record takes every extractor and authorization part the entry resolved and carries no project queued charge; a request struct would exist only for this signature"

**src/http.rs**
5. **`append_typed` `[too_many_arguments, too_many_lines, excessive_nesting]`**
   - New: "append_typed; the typed append takes the request parts as the handler parsed them and validates, drains, admits and executes them, with no project queued charge, in one sequence whose drain cap nests inside the body walk; a request struct, a split or a flattened drain would separate the parts from the sequence that orders them"

**src/application/append.rs**
6. **`execute_once` `[too_many_lines]`**
   - New: "execute_once; one append validates, admits, shares its queued charge with the committer request, commits and settles in the order the retry contract fixes; splitting it would separate the steps from the retry that orders them"

**src/application/creation/initialization.rs `seed`**
7. `[too_many_arguments]`: "seed; seeding takes the plan, descriptor, key and fork parts separately as creation resolved them and commits them with no project queued charge; a seed struct would exist only for this signature"
8. `[too_many_lines]`: "seed; the uncharged tail seed, the fork reference and the readiness publish are one initialization whose compensation depends on which step failed; splitting them would separate the steps from the compensation they order"
9. **`[unwrap_used]`**: this is the fingerprinted one. Its path fingerprint for `None` grows.
   - New: "seed; a poisoned stream state read before the uncharged seed append may hold a half-advanced durable frontier; recovering it could seed a child from a length never made durable"
   - Narrowing it to the single `let mut next = { … }` statement (:36) would not shrink the measured scope, because the gate measures the smallest containing *item*, which is still `seed`. So re-deciding the reason is the effective remedy.

**src/application/consumer/delivery.rs `dlq_and_settle`**
10. `[too_many_arguments]`: "dlq_and_settle; the dead-letter path takes the stream, consumer, key, epoch, identity, engine and segment separately as settlement resolved them and appends with no project queued charge; a context struct would exist only for this signature"
11. `[too_many_lines]`: "dlq_and_settle; the uncharged dead-letter appends and the settlement of the poisoned leases are one bounded sequence over the same leases; splitting it would separate the appends from the leases they release"

**src/history.rs `tests::commit_blocked_detects_real_flush_stall`**
12. `[disallowed_methods]`: "commit_blocked_detects_real_flush_stall; the fixture spawns the uncharged feeding task it then aborts or joins before asserting; a supervised spawn would tie the fixture's teardown to a supervisor it never builds". The spawn stays registered through the effect row in `source-allowances.json` (`crate::tests::commit_blocked_detects_real_flush_stall`, `src/history.rs`), which is part of the gate's `registered` set.
13. `[let_underscore_must_use]`: "commit_blocked_detects_real_flush_stall; the fixture ignores a join or uncharged enqueue result whose only failure is the shutdown it stages itself; treating it as fallible would add branches the pinned sequence never takes"
14. `[excessive_nesting]`: "commit_blocked_detects_real_flush_stall; the fixture nests the stalled flush inside the blocked commit inside the uncharged feeding task it stages; flattening it would separate the stall from the commit it must block"

**src/history.rs `tests::wedge_detects_stale_durability`**
15. `[let_underscore_must_use]`: "wedge_detects_stale_durability; the fixture ignores a join or uncharged enqueue result whose only failure is the shutdown it stages itself; treating it as fallible would add branches the pinned sequence never takes"
16. `[excessive_nesting]`: "wedge_detects_stale_durability; the fixture nests the stale durability report inside the wedged engine it feeds uncharged appends; flattening it would separate the report from the wedge it must detect"

**src/history/controller_tests.rs `active_absorber_cancel`**
17. `[too_many_lines]`: "active_absorber_cancel; the fixture stages the engine, its uncharged append, the held store, the absorber and its cancellation in the order the scenario requires; splitting it would separate the stages from the cancellation they set up"
18. `[let_underscore_must_use]`: "active_absorber_cancel; the fixture closes the database and partition on the way out after its uncharged append; a failed close leaves nothing the assertions depend on"

**src/dst/runtime.rs**
19. `Workload::attempt [too_many_arguments]`: "Workload::attempt; the DST workload takes the engine, stream, key, payload and fault knobs separately as each scenario states them and submits one uncharged committer request; a request struct would hide which knob a scenario varies"
20. `Workload::attempt_with_deadline [too_many_arguments]`: the same text, with the `Workload::attempt_with_deadline;` prefix.

**src/dst/tests/reads_history.rs**
21. **`repeated_keyed_reads_hit_the_postings_cache` `[too_many_lines]`**
    - New: "postings cache scenario; the uncharged seed appends, the first read, the cached second read and the physical load counts form one causal sequence; helper phases would hide which read loaded the index again"

These need no expect or reason edits: `product_append`, the DST helpers `append_sized`, `append_n`,
`seed` and `seed_records`, `LaneSender::send`, the two `Fixture::append` functions, and the
durability_fences, history_absorption and reads_history tests other than the one in item 21. Their
enclosing functions and impls carry no `#[expect]`, and there are no inner `#![expect]`s in any
touched file.

Nothing becomes unfulfilled:
- `too_many_arguments` still fires at 11 params (`submit_product_append`) and 9 params (`product_append_inner`).
- `fn_params_excessive_bools` still fires on 2 bools, above the limit of 1.
- `too_many_lines` still fires on every function that already had it.

---

## 5 Mutation-kill analysis (cargo-mutants 27.1.0, in-diff)

**Critical files in the push diff** (compared against origin/slate):

- **src/shard.rs**, owner `shard`, filter `shard::`. The diff is the struct field, plus the removed
  `#[cfg(test)]` impl.
  - Selected mutants: none. Neither change is a function body.
- **src/http.rs**, owner `http`, filters `http:: livefeed_engine_retired security_workload::`. The diff
  is `queued_charge: None,` inside `append_typed`, plus the test-only `use`.
  - Selected mutants: the `append_typed` value replacement `Ok(Default::default())`. It is
    **unviable**, because `AppendOutcome` has no `Default` and `AppendResult` is a plain `Result`.
    Unviable is reported as not missed.
  - This is the reason C1 must not touch any other http.rs fn body.
- **src/quota.rs**, owner `quota_registry`, filter `quota::`. The diff is doc comments only; no code
  changes.
  - Selected mutants: none. No function body lines change.

These files are production-unchanged: `src/shard/test_support.rs`, `transaction_tests.rs` and
`retirement_tests.rs` are `#![cfg(test)]`, so they go into `production_unchanged_files` and never reach
discovery. No new file is created under a critical prefix. The new files are in `src/product/` and
`src/dst/`, and neither is critical.

**Result:** the mutation leg selects owners shard, quota_registry and http, runs their unmutated
baselines, and discovers no viable mutant. It reports that explicitly; it is not a claimed experiment.
**No owner rows or filters change** in `scripts/quality/mutation_owners.py`.

**No new guard, predicate or boundary exists in critical code.** The only new logic is `g.map(Arc::new)`
(product.rs) and `command.queued_charge.clone()` (append.rs), and neither file is critical. The DST
tests kill them behaviourally:

| Regression of the new logic | Killed by |
|---|---|
| product passes `None` instead of its charge (or drops the `.map(Arc::new)` share) | RED-1 fails with `left: 0, right: 1024` after the handler is gone |
| `execute_once` puts `None` in the `AppendReq` | RED-1 fails with `left: 0` |
| `execute_once` uses `.take()` (move) instead of `.clone()` (share) | GUARD-2 fails with `left: 0, right: 1024` while the caller waits |
| the `AppendReq` share is leaked (never dropped) | RED-1 fails at the last wait ("the committer never released…"), bounded at 15 s |
| the command's share is leaked | GUARD-2 fails at the last wait ("the answered append never released…") |

Every wait is bounded (15 s, `tokio::time::timeout` on the read), so a leak surfaces as an assertion,
never as a TIMEOUT.

**Loom/held-commit (item 48).** No synchronization primitive or state transition changes. The only
shared state is an `Arc` refcount released on `Drop`, and the committer's ordering is untouched. The
held-commit coverage is RED-1 (the commit gate) and GUARD-2 (the dispatch gate). The loom leg is still
selected by the `src/shard` prefix and runs the existing models. There is no new loom model, because
nothing new would be modelled.

**Other legs.**
- Properties/fuzz is selected by `src/quota`, but admission code is unchanged, so no new property is needed.
- Miri is selected by `src/http`, which runs the existing tests.

---

## 6 Ledgers (all in C2)

- **docs/quality/owners.json**: one `by-path-module` row.
  ```json
  {"category": "by-path-module", "count": 1, "owner": "crate::quota_queued_custody", "path": "src/dst/dst_tests.rs",
   "reason": "Queued-append custody scenarios; a real enforce-mode HTTP append held in the committer, abandoned or answered, against one project queued-byte counter; compiled and executed with DST.",
   "syntax": "path = \"tests/quota_queued_custody.rs\""}
  ```
  No effect, macro-dsl, glob or global rows are needed.
- **docs/refactor/test-inventory.json**: `python3.11 scripts/test-inventory.py --write`. It moves from 497 to 499 tests. The function hashes change for:
  - the 2 new tests;
  - `a_fence_survives_handle_eviction` and `a_fence_outlives_the_maintenance_sweep`;
  - `sparse_key_reads_page_with_bounded_spans`, `corrupt_postings_fall_back_to_the_envelope` and `repeated_keyed_reads_hit_the_postings_cache`;
  - `absorber_drains_records_larger_than_the_per_stream_gather_cap`.
  - Helpers are not inventoried.
- **docs/refactor/review-mechanisms.json**: `active_absorber_cancel`'s body changes, so update:
  - (a) mechanism `owned-active-absorber` → `support_functions[active_absorber_cancel].sha256` (currently `55db5d661e4c…`);
  - (b) `source_adaptations[src/history/controller_tests.rs::active_absorber_cancel].after_sha256` (currently `55db5d661e4c…`). Append one sentence to its `reason`: "Review item 31: its direct append names an absent queued charge."
  - Set both to the new function hash.
  - Compute the new sha with `scripts/test-inventory.py`'s `functions(…, include_helpers=True)`. `review-evidence.py --check` prints both mismatches until they are updated.
  - No other pinned test or support function is touched. The `r03a` and `r17b` bodies are unchanged; only their `Fixture::append` helpers change, and those are not pinned.
- **src/dst/tests/README.md:16**: the row becomes `| Multitenancy and authorization | `security_*`, `quota_enforcement`, `quota_queued_custody`, `quota_read_volume` |`.
- **docs/refactor/WIRE-MATRIX.md:118** (§2.7 Metering): replace "Project `queued_append_bytes` charged for the handler's lifetime" with "Project `queued_append_bytes` charged from admission until the caller is answered (or gone) and the committer has dropped the payload; a timed-out or disconnected append keeps its charge while it is still queued". §2.8 inherits it ("Same success/error contract as 2.7").
- **docs/MULTITENANCY.md:1252-1253**: "`queued_append_bytes` — the request body is charged to the project BEFORE the committer sees it and released only when the caller is answered (or gone) AND the committer has dropped the request; a timed-out or disconnected append keeps its charge while it is still queued."
- These need no change:
  - `docs/refactor/architecture-policy.json`: the only new non-DST file is `#![cfg(test)]`.
  - `scripts/quality/mutation_owners.py`.
  - `docs/quality/source-allowances.json`: no row goes stale, because the moved items had none.
  - `scripts/mt-audit-baseline.txt`: no new `stream_hash(`/registry-literal site.
  - `docs/LIVE-FEED.md:291`: its "exact append-queue occupancy" becomes true.

---

## 7 Controls

Run the Python gates with python3.11 (the MEMORY trap). Every command runs from the repo root.

1. **Red observation** (C1 plus the new test module, before the production edits):
   - Command: `cargo test --locked --lib dst::dst_tests::quota_queued_custody -- --test-threads=1`
   - Expected result: `a_staged_append_keeps_its_queued_charge_until_its_caller_is_answered ... ok`
   - Expected result: `an_abandoned_append_keeps_its_queued_charge_until_the_committer_drops_it ... FAILED`, with the §3 panic (`left: 0`, `right: 1024`).
   - Expected summary: `test result: FAILED. 1 passed; 1 failed`.
2. **Move-only variant check** (local only, not committed):
   - Temporarily replace `command.queued_charge.clone()` with `command.queued_charge.take()`.
   - Expected result: GUARD-2 fails with `a staged append whose caller still waits for its answer keeps its queued charge` / `left: 0` / `right: 1024`.
   - Revert the change afterwards.
3. **Green**: the same command as step 1 prints `test result: ok. 2 passed; 0 failed`.
4. **Regression**: `cargo test --locked --lib quota` (unit `quota::` plus DST `quota_*`) and `cargo test --locked --lib shard::`. Both must pass, as must the CI DST suite (`cargo test --locked --lib dst::`).
5. **Formatting and lint**:
   - `cargo fmt --all -- --check` prints nothing.
   - `cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl` must exit 0.
   - `python3.11 scripts/quality/gate.py --clippy target/quality/clippy.jsonl` must exit 0. In particular, there must be no `accepted exception grew`, no `file growth`, no `exception needs owner; invariant; alternative` and no `primitive-spawn exception needs a registered function owner`.
6. **Docs**: `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items` must succeed.
7. **Repo gates** (each must exit 0):
   - `python3.11 scripts/test-inventory.py --check` prints `test-inventory: OK (499 tests, …)`.
   - `python3.11 scripts/review-evidence.py --check`.
   - `python3.11 scripts/architecture-gate.py --check`.
   - `bash scripts/multitenancy-audit.sh` prints `MT_AUDIT_OK`.
8. **Line counts**: `wc -l src/product.rs src/shard.rs src/history.rs src/http.rs src/quota.rs` prints 4150, 3229, 1710, 3370 and 977.
9. **Verification plan**: `python3.11 scripts/quality/verification_plan.py --out target/quality-plan`. The local run uses the merge-base with origin/slate. Expected `plan.json`:
   - `mutants: true`
   - `mutation_source_files == ["src/http.rs", "src/quota.rs", "src/shard.rs"]`
   - `selected_mutation_owners == ["shard", "quota_registry", "http"]`
   - `unregistered_mutation_source_files == []`
   - `production_unchanged_files` includes `src/shard/test_support.rs`, `src/shard/transaction_tests.rs` and `src/shard/retirement_tests.rs`
   - `loom`, `miri` and `properties_fuzz` are all `true`

   If `src/shard/test_support.rs` shows up as unregistered instead, add `owner('shard_test_support', 'src/shard/test_support.rs', 'shard::')`.
10. **Mutations**: `scripts/quality/mutations.sh` passes its baselines and reports no viable mutant selected (the append_typed FnValue mutant is unviable). The expected missed count is 0.
11. **Whole gate**: `scripts/quality.sh` prints `QUALITY_OK`. Then push. Per the MEMORY rule, claim CI green only after `gh run view` confirms it.

---

## 8 Out of scope (recorded, not changed)

- **The raw surface** (`/v1/stream/*`) never charges queued bytes. It runs as the deployment tenant and is bounded by the global inflight and RSS gates. The buffering guard it carries ends at enqueue (`append.rs:359`), as today.
- **Other uncharged writers, as today:** the seal's final record (`product_append_sealing`), create-with-initial-records (`initialization::seed`), consumer DLQ appends (`dlq_and_settle`), and internal and history writers. Whether create-with-content should charge is a separate item.
- **The staging window for an abandoned request.** Once the committer stages an abandoned request, its `AppendReq` share drops, and the frames in a `WriteBatch` whose `db.write` is blocked count neither as queued nor as unabsorbed until publish. That window is bounded per shard by `max_batch_bytes` (one group), not by queue depth. Closing it means carrying the share through `DurableEffects` in `src/shard/transaction/*`. That is a critical committer change with its own mutation and loom exposure.
- **Retry-attempt custody.** Attempts 2-4 of `execute_prepared` share the charge via `clone()`. There is no deterministic DST hook that forces an engine closed-segment verdict without a split, so it is covered by construction and by the `.take()` kill in GUARD-2, not by a dedicated test.
- **Timeout and refusal answers.** `APPEND_TIMEOUT` (10 s) and its wire answer are unchanged. So is the missing `retry-after` on 429 `queued_bytes`.

---

## Skeptic corrections (C1..Cn)

I re-checked the plan against the tree at **fba5af56**. HEAD has moved four commits past the plan's 0afa2597. All four touch only scripts and docs: `multitenancy-audit.sh` now scans every `src/**/*.rs`, the audit baseline was re-cut, the architecture gate now refuses budget exceptions that are no longer needed, and there is a clippy diagnostics change. No `src/` file changed, so every ceiling still holds: product 4,205, shard 3,232, http 3,371, history 1,713, quota 975, registry 1,509 at origin.

What I verified and found correct:
- Every quoted line.
- The 20 `AppendReq` literals, the 4 `AppendCommand` literals and the `append_application.rs` helper.
- The 21 exception attributes across 13 functions. No `#![expect]`/`#![allow]` exists in any touched file, and no enclosing impl or mod carries one.
- The unwrap-fingerprint and dead_code reasoning (`_queued_charge`).
- The architecture budget (`product_append_inner` 213→214 of 251).
- The `corrupt_postings_fall_back_to_the_envelope` margin. It is a `#[tokio::test]`, so the body is a sync block and clippy strips the braces: 98→99. That is correct. Async fns keep the braces. I calibrated this against `the_first_advance_seals_the_history_layout` (source count 102, no expect, CI green).
- The red path. Hyper h1 with half-close off hits `mid_message_detect_eof` on a keep-alive connection, and the 10 s `APPEND_TIMEOUT` is the fallback. Both paths release the handler-held charge today, and after the fix both leave the committer's share alive.
- GUARD-2's semantics. `applied` is published at `transaction/publish.rs:44`, after `accept_append` has dropped the request and before `dispatch_durable`, which takes the dispatch gate (`shard.rs:3071`).
- The by-path owner row format, which matches the `quota_read_volume` row in owners.json.
- The review-mechanisms pins. Only `active_absorber_cancel` is pinned. `retirement_tests` pins `r17b_*` plus `duplicate_order`, `assert_moved`, `completion_checkpoint` and `remote_missing`, not `Fixture::append`.

**C1 — The tree drifted, and the working tree is dirty.**
- The plan's header says HEAD = 0afa2597. It is fba5af56 (0a8ed40a, 47799eb3, 09fd911d, fba5af56). Re-cite it.
- The working tree also holds another session's uncommitted WIP: `M src/registry.rs` (1,501 lines) and `?? src/registry/failpoints.rs`.
- `verification_plan.py` includes working changes (`git diff <base>` of the worktree, `:329-332`).
- `multitenancy-audit.sh` scans `src/registry/*.rs` for `registry-bare-name` declarations.
- The source gate reads every tracked source.
- So controls 5, 7 and 9 must run in a clean worktree at the C1 or C2 commit, or after that WIP lands. Otherwise control 9's `changed_rust_files` and the MT audit pick up unrelated files.

**C2 — C1's claim "all four ceilinged files are `production_unchanged_files`" is false for product.rs, history.rs and http.rs.**
- `production_changes.normalized_source` erases only explicit-`cfg(test)` *items* of kind module, function, impl, struct, enum, trait, const, static or type (`scripts/quality/production_changes.py:83-88`).
- A `use` declaration is not an item in the syntax facts, so its `#[cfg(test)]` does not erase it.
- Three edits change the normalized token stream:
  - the new `#[cfg(test)] pub(crate) use seal_test_support::{…}` in product.rs;
  - `#[cfg(test)] pub(crate) use test_support::floored_budget_capacity;` in history.rs;
  - the merged `#[cfg(test)] pub(crate) use crate::application::read::{TEST_ASSERT_KEYED_DENSE, read_merged};`, which replaces two `use`s at `http.rs:2864-2867`.
- Only shard.rs qualifies. Its removed `#[cfg(test)] impl ShardEngine` at 3212-3217 is an erased item, and the `use` lines around it are unchanged.
- No push-level consequence: http.rs is mutation-selected by C2 anyway, and product.rs and history.rs are not critical. But correct the C1 text. The rest of C1 is sound:
  - `test_abort_task` can read the private `tasks` field from the child module.
  - `lifecycle_service()` is `pub(crate)` (`http.rs:309`), and `EnterSeal` and the lifecycle fns are `pub(crate)`.
  - The moved `install_reserved_claim` reason has exactly two `;`.
  - No moved item or its callers carries an unwrap/expect fingerprint or a registered allowance row.
  - There is no MT-audit pattern in any moved text.

**C3 — The §5 and control 10 mutation details are inaccurate. The outcome is unchanged: 0 missed.**
- `append_typed` returns the alias `crate::application::append::AppendResult` (`http.rs:2748`). cargo-mutants cannot see `Result` through a type alias, so the FnValue mutant is `replace append_typed -> AppendResult with Default::default()`, not `Ok(Default::default())`. It is still unviable, because `Result` has no `Default`.
- `mutation_driver.py:147-149` skips an owner whose in-diff `--list` is empty. So `shard` and `quota_registry` print `no executable mutants in the selected scope`, and their baselines do **not** run. Only `http` runs its baseline (filters `http:: livefeed_engine_retired security_workload::`) and builds 1 unviable mutant.
- The expected driver line is `Mutation verification executed 1 selected mutant(s) across 3 registered owner(s).`, with cargo-mutants reporting 1 unviable and 0 missed/timeout. It is not "no viable mutant selected / runs their unmutated baselines".

**C4 — Rewrite the docs claim for `docs/LIVE-FEED.md:291` rather than assert it "becomes true".** After the fix, `queued_bytes` is still not "exact append-queue occupancy":
- (a) It charges the product body length (`product.rs:2052`), not the committer's `entries` bytes. `json_entries` re-serialises them (`creation.rs:241-244`), and single appends are re-wrapped as `[value]`.
- (b) The waiting caller's share keeps it charged after staging. From publish until the durable ack it is double-counted with `unabsorbed_frame_bytes` (`publish.rs:46`).
- (c) It is absent for an abandoned request between staging and publish (plan §8).

Edit LIVE-FEED.md:291 in C2 to "(payload bytes a caller or the committer still holds)" so the documented pressure model matches.

Also add to the Søren decision whether `PROJECT_PRESSURE_MODEL_VERSION` stays 1. It is at `quota.rs:120` and served per row at `quota.rs:926` and in `/v1/debug/load`. The term's meaning changes for abandoned appends. Recommend keeping 1, because the documented definition is unchanged and the implementation now approaches it, but make the choice explicit.

**C5 — The §2 decision omits the cross-stream blast radius.**
- `queued_bytes` is one per-project counter across all of the project's shards (`quota.rs:78`, `charge_queued` `:867-888`).
- Under committer custody, a single wedged shard pins every byte still in its mpsc queue or `first` slot until its committer drains or the engine retires. The wedge is the `db.write`-blocked or stale-durability mode that `submit.rs:62-78` sheds.
- Today those charges expire with the 10 s `APPEND_TIMEOUT`.
- With a configured `queued_append_bytes`, or with the memory watermark, one wedged shard can now refuse the project's appends to *healthy* shards and streams:
  - 429 `queued_bytes`, which still carries no `retry-after`, so clients may hot-loop;
  - 429 `project_memory_pressure`.
  - `CONTROL-PLANE-INTEGRATION.md:430` shows a 64 MiB ceiling example.
- The exposure is bounded by queue capacity × request size, because the wedge shed stops new enqueues after 5 s and a Full queue refuses at `try_enqueue`.
- State this bound in the decision, and name it in the backward-compatible alternative as the risk that option avoids.
- Also ask whether 429 `queued_bytes` should now carry a `retry-after`, since the condition can last as long as a stall. The plan lists this as unchanged in §8.

**C6 — The `Bytes::from_owner` rejection gives the wrong reason. The decision stands.**
- Aliasing is not required. `execute_once` could make the guard the owner of a re-wrapped entry (for example `retained_bytes::with_charge` over `entries[0]`). Entries drop at `accept_append` exit exactly like the proposed field: `append_touch` only reads them, and the ring holds encrypted frames.
- That design would avoid all 19 literal edits and about 18 reason re-decisions.
- The real reasons to reject it:
  - implicit coupling of an accounting guard to a payload buffer;
  - no carrier when `entries` is empty (a `deferred_error` or producer-only close) while `body` is not;
  - an extra copy.
- Record those reasons instead of the aliasing argument.

**C7 — Harden RED-1's two observation points. These are small, non-blocking flake risks.**
- (a) `appends_enqueued()` counts every client op `try_command` or `submit_queue` enqueues on the engine (`shard.rs:1846`, `:2393`), not only this append. A stray op would let the first `assert_eq!` run before the handler charged. Worse, after the fix it would let `drop(client)` abandon the append *before* enqueue, and the post-abandon assertion would then read 0.
  - Poll `wait_for(|| engine.appends_enqueued() == base + 1 && queued_bytes(..) == RECORD_BYTES)`.
  - Assert `appends_enqueued() == base + 1` once more after `drop(hold)` is not needed. The `consumer_delete.rs:514-520` precedent shows the rig is quiet, so this is belt and braces.
- (b) The observer infers "handler gone" from `inflight == 0`. Both counters are `Relaxed` (`quota.rs:449-451`, `:482`). The dropping thread decrements queued first, because `_quota_guard` (`http.rs:2087`) outlives the inner awaitee. On aarch64, another core may still see the stores in the opposite order, so a spurious 1024 on the current tree is possible but rare.
  - To make the red deterministic, poll for up to 100 ms for the post-abandon value to settle before asserting, or keep the assertion and accept the rarity. Document either choice.

**C8 — GUARD-2 holds the dispatch gate. Any no-write commit group also takes that gate, in `join_prior_barrier` (`shard/transaction/finalize.rs:45-48`), and parks the committer ahead of the append. The test would then fail at the 15 s bound with "the append was never staged and applied".**
- The rig's trim ticker sends `TrimTick` only with trim debt (`shard.rs:1742-1744`).
- `durability_fences.rs:76` and `durability_gather.rs:47` use the same hold.
- So this is accepted precedent, but name it in the test's doc comment as the known failure mode.

**C9 — Optional.** The doc comment on `quota_enforcement.rs:574-577` (`queued_append_bytes_charge_and_release`) says the charge is "released when the append is DECIDED". That becomes stale, and its body still passes, because sequential appends are staged before they are acked. Editing it changes that test's inventory hash. Either leave it, noting the inventory churn saved, or fix it in C2 and include it in `--write`.

**Verdict: ready-with-corrections.**
- Every red and guard claim traces correctly: RED-1 fails with `left: 0, right: 1024` today and passes after the fix, and GUARD-2 kills the move-only variant.
- The ceilings, the exception inventory and the re-decided reason texts are correct and buildable. Each reason has exactly two `;` and no `"`. The `disallowed_methods` one keeps its registered effect owner `crate::tests::commit_blocked_detects_real_flush_stall`.
- The ledgers are complete apart from the LIVE-FEED.md wording (C4).
- No control is unbuildable. Controls 5, 7 and 9 need a clean worktree (C1), and the control 10 and §5 output text must be corrected (C3).
- The Søren decision must add the cross-shard blast radius, the `retry-after` question and the pressure-model-version question (C4, C5).
