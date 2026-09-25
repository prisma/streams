# Item 28 step B: operation counts come from the typed outcome

Tree: `slate` at `24c4c77a4303e9b4b7a6cb09bfee26453efad657` (= origin/slate). Read-only planning pass. Nothing was built or run. Line numbers are HEAD's.
Adopted position: external review §8 (claim `meter-op`, confirmed). One point was learned beyond the review, in §1(e): the raw copy cannot be fixed "inside `append`" as the review proposes without growing a ratcheted scope. §4 gives the restructure that avoids that growth.

---

## 1 Problem (verified on HEAD)

**(a) The helper**, `src/product.rs:1023-1047`:

```rust
/// Operation-count metering at the dispatch choke point (§4.5's
/// non-priced dimensions). ... The
/// registry read is a warm cache hit for a request that just succeeded.
enum OpKind { Append, Queue }
async fn meter_op_if_ok(state: &Arc<AppState>, sref: &crate::tenant::TenantStreamRef, ok: bool, kind: OpKind) {
    if !ok { return; }
    if let Ok(Some(desc)) = state.registry.get(sref).await {
        match kind {
            OpKind::Append => crate::billing::meter_append_request(state, &desc),
            OpKind::Queue => crate::billing::meter_queue_op(state, &desc),
        }
    }
}
```

The helper has four problems:
- It takes a bool derived from the rendered HTTP status.
- It reads the descriptor again **by name**.
- It skips silently on `Err` or `Ok(None)`.
- It has no `desc_alive` check and no incarnation pin.

The identity key is `identity_with_capabilities` (`src/billing.rs:540-579`), whose `stream_id: desc.stream_epoch.clone()` (`:576`) is the incarnation. The workspace is resolved at the re-read.

**(b) Every use site** (grep `meter_op_if_ok|OpKind|meter_append_request|meter_queue_op` over `src/`):

| site | code |
|---|---|
| `src/product.rs:1118-1131` POST records | `let r = product_append(state.clone(), &tenant, name.clone(), headers, body, false, principal.as_ref()).await; let ok = r.status().is_success(); meter_op_if_ok(&state, &tenant.stream_ref(&name), ok, OpKind::Append).await; r` |
| `src/product.rs:1133-1146` POST `:batch` | the same with `true` |
| `src/product.rs:1240-1255` POST consumers `:settle` | `let r = product_consumer_settle(state.clone(), &tenant, name.clone(), cname, headers, body, access).await; let ok = ...; meter_op_if_ok(..., OpKind::Queue).await; r` |
| `src/http.rs:2059-2081` raw POST (inline copy) | `let r = append(state.clone(), state.deployment.raw_adapter_sref(&name), headers, body, None, None, None).await; if r.status().is_success() && let Ok(Some(desc)) = state.registry.get(&state.deployment.raw_adapter_sref(&name)).await { crate::billing::meter_append_request(&state, &desc); } r` |
| `src/billing.rs:652-661`, `:664-675` | definitions of `meter_queue_op` and `meter_append_request`. They have no other callers. |

**(c) The failure modes are real on HEAD:**
- **Lost count.** `Registry::get` (`src/registry/cache.rs:294-311`) answers `Err` on an injected fault (`take_fail_next_get`) or on any store error during `fill`. It answers `Ok(None)` on absence. Either way the count disappears with no log and no counter.
- **Wrong incarnation.** The helper looks up by name. A delete plus recreate between the answer and the re-read puts the count on the successor's `stream_id`. A tombstoned descriptor is counted, because there is no `desc_alive` check.
- **A second await after the operation.** `fill` returns `Lookup::Fresh` without yielding (`cache.rs:224-227`). On a miss or an expired TTL it does a store GET (`:229-233`). If the request future is dropped there (a client disconnect), the committed operation is never counted. `product_entry` is awaited directly at `src/http.rs:1940-1942`.

**(d) Each handler already holds the pinned identity:**
- **Product append.** `product_append_inner` resolves `desc` (`src/product.rs:2077-2107`). `submit_product_append` pins `expected_epoch: Some(desc.epoch())` (`:2300`). `execute_once` refuses any other incarnation (`src/application/append.rs:252-262`). `resolve_segment` only swaps in a descriptor with the same `stream_epoch` (`src/application/append/route.rs:13-17`).
- **Raw append.** `append_typed` pins `expected_epoch: Some(prepared.descriptor().epoch())` (`src/http.rs:2624`). The descriptor is consumed by `execute_prepared`, and `AppendOutcome` (`src/application/append/contract.rs:306-316`) does not carry it. That is why the raw surface today has no descriptor to meter from.
- **Settle.** `settle` destructures `AuthorizedStreamContext { desc, .. }` (`src/application/consumer/delivery.rs:414-424`). Its fields are private to `application::consumer` (`src/application/consumer.rs:144-155`).
- **Precedent.** Pull meters inline from its typed outcome (`src/product/consumer_pull.rs:80-88`, `out.descriptor`). The product read does too: `render_product_read` meters with `meter_read_outcome(state, out)` and builds its cursor from `out.descriptor` (`src/product.rs:2842-2884`).

**(e) Something the review did not see.** Its proposed raw fix is "meter inside `append` with its pinned desc". Three ratchets block it:
- `append` (`src/http.rs:2479-2513`) carries a `too_many_arguments` exception. That exception ratchets scope lines and syntax facts, and its body has no room.
- Three of its seven parameters (`product_hash`, `product_key`, `seal_auth`) are always `None` at every caller (`stream_entry_inner`, and one test at `billing_usage.rs:878`). Removing them would change the `append(...)` call under `stream_entry_inner`'s function-wide `unwrap_used` exception, which fingerprints every call site.
- `append_typed` carries a combined ratchet (arguments, lines, nesting) and is shared with system appends.

§4 routes around all three without editing any reason text.

**What is not affected.** Billed ingest bytes are the committer's (`src/shard/transaction/append.rs:268-297`, from `req.billing` built with the pinned descriptor at `src/application/append.rs:381-393`). Both counted dimensions are non-priced (`docs/OBSERVABILITY-BILLING.md` §4.5).

## 2 Contract decision

**Current semantics** (derived from the code above; every row is kept unless marked):

| outcome | `append_requests` / `queue_operations` today | after this plan |
|---|---|---|
| product records or `:batch`, typed `Ok` (200), including a producer duplicate (`duplicate:true`) | +1 per request | +1, **kept** |
| raw POST typed `Ok` (204 or 200), including a duplicate and a close (with or without content) | +1 | +1, **kept** |
| any refusal (typed `Err`: 4xx, 5xx, 408 ambiguous timeout) and any pre-submit refusal | 0 | 0, **kept** |
| seal's final record, create's initial content, DLQ, system and telemetry appends | 0 (no op count; the committer bills the bytes) | 0, **kept** |
| settle typed `Ok` (200), including an all-stale or empty (no-op) settle | +1 | +1, **kept** |
| pull | +1 plus bytes, inline | unchanged |
| identity counted | whatever the name resolves to at a later re-read | **the incarnation the operation committed to** (the fix) |
| re-read `Err` or `None` after success | count lost | cannot happen (no re-read) |
| handler future dropped *before* it receives the typed outcome (committed but disconnected mid-commit) | not counted | not counted, **kept** |
| handler dropped *at* the post-answer re-read (store GET) | count lost | cannot happen (no await between outcome and count) |

**Definitions adopted.**
- An operation counts exactly once, when its handler holds a typed `Ok` outcome. The count is taken synchronously at that point, from the descriptor the outcome committed under (`AppendOutcome.descriptor`, or the settle context's descriptor). Nothing awaits between the outcome and the count.
- A request whose handler never receives its outcome is not counted. Its bytes are still billed by the committer. The counter counts requests the service answered as accepted, which is unchanged.
- Metering has no error path. The accumulator is in memory, and a poisoned lock panics exactly as before. So "committed but metering failed, so answer retryable" does not exist and is not introduced.
- There is no wire change. §4 shows that the product append answer stays byte-identical.

## 3 Red tests, pins, non-vacuity

All tests go in the new DST module `src/dst/tests/billing_operation_counts.rs`. The module path is `dst::dst_tests::billing_operation_counts`. Raw-surface tests are named `raw_*` so the http mutation owner runs them.

Stream names use the `opc` prefix, which no existing test uses. Every test is `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]` on `http_rig(mem())` and ends with `engine_shutdown(&state)`.

**Helpers** (all in the module):
- `counts(state, stream_id) -> RowDelta` reads `state.billing.reads().snapshot_active()`, matching on `id.stream_id`, and returns the default when there is no row. `reads()` is `#[cfg(test)]` (`src/billing_service.rs:200-206`). The rig runs no telemetry loop, so the active window is not rotated under the test; `billing_usage::read_meter_covers_the_matrix_exactly` relies on the same fact.
- `product_stream(addr, name)` does `PUT {"format":{"kind":"json"}}` and asserts 201.
- `park_append(name, request) -> JoinHandle<T>`:
  - It records `crate::failpoints::parked(Fp::AppendBeforeEnqueue, name)`, calls `park_append_before_enqueue(name)` and then `tokio::spawn(request)`.
  - It polls `parked` for at most 300 × 10 ms, then does `assert!(arrived, "{name}: the append never reached the committer enqueue")`.
  - It carries `#[expect(clippy::disallowed_methods, reason = "operation-count fixture; the request is parked at the committer enqueue and joined by the caller after release; run inline it would block on its own failpoint")]`.
  - The park point is `src/application/append.rs:347-352`, which comes after every descriptor read the append makes. Nothing between enqueue and the handler's answer reads the registry: `submit.rs` resolves the shard directory only, and `close::complete` is a no-op for a non-close append (`close.rs:231`).
- No `json!` in tests (it would need a macro-dsl row). Bodies are byte literals, and settle bodies are built with `format!`.

### Red (fail on `24c4c77a` plus only this test module; pass after)

**R1 `an_append_straddling_a_recreate_counts_on_the_incarnation_it_committed_to`**
- Steps:
  1. Create `opcrecreate` and read `committed = registry.get(sref)`.
  2. Build the successor: take `committed.to_persisted()`, set `stream_epoch = format!("{:032x}", u128::from_str_radix(&committed.stream_epoch, 16).unwrap() ^ 1)`, and apply `StreamDesc::try_from`. Validation only requires 16 hex bytes (`src/registry.rs:536-540`). This follows the precedent at `lifecycle_incarnation.rs:733-736`.
  3. `park_append` a product POST `/records` with `{"n":1}`.
  4. `state.registry.test_poison_cache(&sref, successor.clone())`, which is exactly what the name resolves to once a recreate lands. A real delete during the park would fence the parked write itself; the defect depends only on the registry's answer.
  5. Release, then assert 200.
- Assertions:
  1. `assert_eq!(counts(&state, &successor.stream_epoch).append_requests, 0, "an append committed to the old incarnation was counted on its successor")`
  2. `assert_eq!(counts(&state, &committed.stream_epoch).append_requests, 1, "the committed incarnation lost its append count")`
- Non-vacuity: `registry.get(&sref)` now returns the successor's epoch, which proves the old re-read would have seen it. Then `invalidate`.
- Red trace (old tree): commit → `render_product_append` → 200 → `product_entry:1128` `ok = true` → `meter_op_if_ok` → `registry.get` → cache `Fresh(successor)` → counted on the successor. Expected output:
  ```
  thread 'dst::dst_tests::billing_operation_counts::an_append_straddling_a_recreate_counts_on_the_incarnation_it_committed_to' panicked at src/dst/tests/billing_operation_counts.rs:<L>:5:
  assertion `left == right` failed: an append committed to the old incarnation was counted on its successor
    left: 1
   right: 0
  ```

**R2 `an_append_counts_when_its_descriptor_cannot_be_read_again`**
- Steps:
  1. Create `opcreread` and record `epoch`.
  2. `park_append` a product POST `/records`.
  3. `state.registry.fail_next_get("opcreread")`.
  4. Release, then assert 200.
- Assertion: `assert_eq!(counts(&state, &epoch).append_requests, 1, "a failed descriptor re-read dropped a committed append's count")`
- Non-vacuity: `assert!(state.registry.get(&sref).await.is_err(), "the metering path read the descriptor again")`. The armed one-shot fault is still unconsumed, which proves no descriptor read happened after the outcome. It also covers the "no await between outcome and count" half of (c).
- Red trace (old tree): 200 → `meter_op_if_ok` → `registry.get` → `cache.rs:299` injected `Err` → `if let Ok(Some(_))` fails → nothing counted.
  ```
  assertion `left == right` failed: a failed descriptor re-read dropped a committed append's count
    left: 0
   right: 1
  ```

**R3 `raw_append_counts_when_its_descriptor_cannot_be_read_again`**
- Steps:
  1. Raw `PUT /v1/stream/opcrawreread` with `content-type: application/json` and an empty body (201).
  2. `park_append` a raw POST of `[{"n":1}]` with `content-type: application/json`.
  3. `fail_next_get("opcrawreread")`.
  4. Release, then `assert_eq!(st, 204)`.
- Assertions: the same count assertion (`... dropped a committed append's count`) and the same `is_err()` non-vacuity.
- Red trace (old tree): `http.rs:2072-2076` → `Err` → `left: 0, right: 1`.

### Pins (pass on the old tree and the new)

| name | pins | non-vacuity inside the test |
|---|---|---|
| **P0 `a_product_append_answers_json_that_is_never_cached`** | Product POST `/records` answers 200 with `content-type: application/json` and `cache-control: no-store`, and a body with exactly the keys `count`, `cursor`, `duplicate`, `sealed`. This is the wire pin for replacing `render_product_append`'s builder with the `IntoResponse` tuple. The existing pins `producer_protocol::product_append_and_append_many` (cursor decode, count) and `product_append_producer_duplicate` also apply. | Control run: temporarily change `"no-store"` to `"no-cache"` in the tuple, see P0 fail with `left: "no-cache"`, then revert. |
| **P1 `product_append_counts_follow_the_typed_outcome`** | On one incarnation row: applied → 1; `:batch` of 2 → 2 (one per request, not per record); producer `p1/0/0` → 3; exact retry with `duplicate:true` → 4; producer gap (seq 5, 409) → 4; invalid JSON `{` (400) → 4. | The two refusals leave the count unchanged, and the duplicate is asserted from the body. |
| **P2 `raw_append_counts_follow_the_typed_outcome`** | Raw: plain append (204) → 1; producer seq 0 (200) → 2; retry (204 duplicate) → 3; `producer-epoch: x` (400) → 3; close-only (`stream-closed: true`, empty body, 204) → 4. | The refusal leaves the count unchanged. |
| **P3 `settle_counts_follow_the_typed_outcome`** | Append 2 records, then consumer `c1` (`{"visibilityTimeoutMs":30000,"maxAttempts":3}`). The pull brings `queue_operations` to 1. Settle an ack of `messages[0].leaseToken` → 2. Settle `{"acks":[{"leaseToken":"not-a-token"}]}` → 200 with `stale:1` → 3 (the no-op settle counts). `{"bogus":[]}` (400) → 3. All on the pulled incarnation's row. | The refusal leaves the count unchanged. The stale settle is proven a no-op by `stale == 1`. |
| **P4 `an_append_its_handler_never_answered_is_not_counted`** | Committed but disconnected. See the steps below this table. | `is_cancelled` and "visible" together prove the append committed and the handler never answered. |
| **P5 `a_recreated_stream_counts_each_incarnation_on_its_own_identity`** | Append on A; product `DELETE` (200 or 204) and `PUT` (201, precedent `lifecycle_incarnation.rs:78-94`); invalidate; B's epoch differs from A's; append on B. Then A = 1 and B = 1. | `assert_ne!(A, B)`. |

**P4 steps:**
1. Resolve the engine: `desc.segment_route_by_id(desc.resolve_segment("").seg_id)` and `state.engine_for(&route)`.
2. `guard = engine.test_hold_dispatch().await` and `entered = engine.appends_enqueued()`.
3. `tokio::spawn(crate::product::product_entry(state.clone(), "opcgone/records".into(), Method::POST, {prisma-encryption-key}, String::new(), Bytes::from_static(br#"{"n":1}"#), ProductAuthorization::Deployment))`. This is a direct call, so dropping the handler is deterministic.
4. Poll `appends_enqueued() > entered` for at most 5 s, then `abort()`.
5. `assert!(handle.await.unwrap_err().is_cancelled())`, then `drop(guard)`.
6. Poll a product GET until `{"n":1}` is visible (at most 3 s).
7. `assert_eq!(counts(..).append_requests, 0, "an append its handler never answered was counted")`.

Durability acks are sent only at dispatch (`src/shard.rs:2952`, `effects.queue_acks` and producer acks), so the handler is still waiting when it is aborted. P4 carries its own `#[expect(clippy::disallowed_methods, reason = "abandoned-append fixture; the handler is aborted while durability dispatch is held and joined before release; run inline the test could not drop it between the commit and its answer")]`.

**Refactor pins for code moved without a behaviour change:**
- `execute_once` alias: any append test, e.g. `producer_protocol::*` and `durability_fences::*`. The compiler proves each `seg.identity` read is the same expression.
- `append()` deleted: `billing_usage::telemetry_crash_points_and_cost_gates` now drives `append_typed` directly.
- Settle serialisation moved: P3 plus the existing `consumer_product::*` settle tests.

**Settle has no red.** There is no seam between a settle's outcome and the old re-read. The only by-name read before it is `authorize`, and `fail_next_get` is one-shot. Holding dispatch also holds `active()`'s `ConfigGet`, because queue acks are dispatch-gated (`src/shard/transaction/queue/mod.rs`).

The defect is the shared helper, and R1 and R2 show it. The settle path loses the helper entirely and is pinned by P3. A DLQ-handoff seam exists (a poisoned settle appends to the DLQ through `AppendService::execute` and would park at `AppendBeforeEnqueue`), but building it is out of scope (§8).

## 4 Edits, file by file (one commit; the ratchet status of every touched scope)

**It is one commit.** The pieces cannot land separately:
- Moving `render_product_append` alone to the response tuple grows `product.rs` by about 8 lines, above its 4,205 ceiling.
- Metering only one of append and settle leaves an `OpKind` variant unconstructed, which is a `dead_code` error.

Commit message: "Operation counts are taken from the typed outcome, against the incarnation the operation committed to". The body records the three red outputs from §3 and the §2 table.

### 1. `src/application/append/contract.rs` (487 → 491; no exception on `AppendOutcome`)

Append a last field to `AppendOutcome`:

```rust
    /// The descriptor the append committed under (the incarnation
    /// `expected_epoch` fenced): a surface counts the request (§4.5)
    /// against it instead of reading the registry again.
    pub(crate) descriptor: StreamDesc,
```

`StreamDesc` derives `Debug` and `Clone` and is already imported for `AuthorizedAppend`. The struct is built in exactly one place (`append.rs:407`). No code destructures it and no test constructs it.

### 2. `src/application/append.rs` `execute_once` (ratchet: `too_many_lines`; must not grow)

- Delete `let hash = seg.identity;` (`:303`). Its three uses become `seg.identity`:
  - `:338` → `crate::crypto::SegmentHash(seg.identity)`
  - `:342` → `state.keys.put(seg.identity, key, epoch)`
  - `:358` → `hash: seg.identity,`
- Add `descriptor: desc,` as the last field of the `Ok(ack)` literal (`:415`). Field initialisers evaluate in order, so `materialized: desc.segments.is_some()` borrows first. The `Err` arm is a separate arm.
- Scope result: lines −1 +1 = 0. Syntax facts: −1 (the `seg` path in the deleted `let`) +1 (the `desc` path) = 0. Each use swaps a `hash` path for a `seg` path. Nested items: 0.
- `hash` was a second name for `seg.identity`; the slot admission on the next line already reads `seg.identity` directly.

### 3. `src/application/consumer.rs` (764 → about 771; no exception)

After `struct AuthorizedConsumerContext`:

```rust
impl AuthorizedConsumerContext {
    /// The incarnation this context was authorized against: the one a
    /// queue operation under it is counted to (§4.5).
    pub(crate) fn descriptor(&self) -> &StreamDesc {
        &self.stream.desc
    }
}
```

This passes the owner leak checks: no `AppState`, `axum`, `HeaderMap` or `Response` (`scripts/architecture-gate.py:104-113`).

### 4. `src/product.rs` (ceiling 4,205; ends about 4,195)

**a. Import.** `use axum::response::Response;` becomes `use axum::response::{IntoResponse, Response};`. This is module level, so no scope is involved. It adds no path named `IntoResponse` anywhere, so no fingerprint changes.

**b. Delete lines 1023-1048** (26 lines): the choke-point doc, `enum OpKind` and `meter_op_if_ok`.
- Lines 1020-1022 ("Everything under `/v1/streams/{*path}` ...") were never `product_entry`'s doc; they were attached to `OpKind`. Turn them from `///` into `//`.
- Left as `///`, they would become three doc attributes inside `product_entry`'s span. That is +3 scope lines, +6 syntax facts and +3 `path:doc` fingerprints under its `unwrap_used` exception: growth. As `//` they are not tokens and sit outside the span.

**c. `product_entry`.** It has two exceptions: a function-wide `unwrap_used` (fingerprints) and `too_many_arguments`/`too_many_lines`/`excessive_nesting` (counts). In each of the three arms (records `:1117`, `:batch` `:1132`, settle `:1240`):
- `let r = X(...).await;` becomes the arm expression `X(...).await`.
- Delete `let ok = r.status().is_success();`, the `meter_op_if_ok(...)` line and the trailing `r`.
- The `ExprCall` tokens `product_append(state.clone(), &tenant, name.clone(), headers, body, false|true, principal.as_ref())` and `product_consumer_settle(state.clone(), &tenant, name.clone(), cname, headers, body, access)` are **unchanged**, so their fingerprints are unchanged.

Effect on the ratchets:
- Scope lines −9. Syntax facts −15 per arm (−45).
- The `meter_op_if_ok` call-site and path keys disappear.
- The `state`, `name`, `tenant`, `r`, `ok` and `OpKind::*` path counts only fall. Unwrap sites stay at 1.
- The `excessive_nesting` expectation stays fulfilled by the untouched SSE quota-refusal arm.

Redundant-clone check:
- `state` and `name` become the last uses after the clones. `clippy::redundant_clone` does not fire on coroutine-saved locals: it gives up on a place it cannot move out of.
- Evidence on HEAD: `product_consumer_settle`'s `cname.clone()` (`:3718`) is the last use of `cname` and passes clippy `-D warnings` today. `put` and `get` use `cname` again later, so they are not evidence. `state` and `name` are held across the arm's await and dropped at the end of the scope, so they are saved in the coroutine the same way. The clippy control in §7 is the proof.
- If the lint ever fired, **stop**. Changing those call tokens is a fingerprint growth, which is an owner decision.

**d. `product_append_inner`.** It has `too_many_arguments` and `fn_params_excessive_bools`+`too_many_lines` (counts only). Line 2235 `render_product_append(&desc, &key, routing_key, count, result)` becomes `render_product_append(&state, &key, routing_key, count, result)`. One path is swapped for another: 0 lines and 0 facts. `state` is still owned there, because `:2223` passes `state.clone()`.

**e. `render_product_append` (`:2319-2351`).** Its `unwrap_used` exception (`:2322-2325`) is **deleted together with its only unwrap**: the builder becomes the infallible `IntoResponse` tuple, as `consumer_pull.rs` already does. The new shape:

```rust
/// Map the shared path's typed outcome into the product contract:
/// {cursor, count, duplicate, sealed} on success, the stable product
/// error schema otherwise. An accepted request (applied, or a producer
/// duplicate answered from the dedup window) is one `append_requests`
/// (§4.5), counted against the incarnation the outcome committed to
/// before the answer exists; a refusal counts nothing.
fn render_product_append(
    state: &AppState,
    key: &crate::crypto::StreamKey,
    routing_key: &str,
    count: usize,
    result: crate::application::append::AppendResult,
) -> Response {
    let out = match result {
        Ok(out) => out,
        Err(error) => return render_product_append_error(error),
    };
    crate::billing::meter_append_request(state, &out.descriptor);
    let next = /* unchanged */;
    let cursor = crate::product_cursor::KeyCursor {
        epoch: out.descriptor.epoch(),
        key_hash: crate::crypto::stream_hash(routing_key),   // keep this exact line: MT audit fingerprint
        seg_id: out.seg_id,
        offset: next,
    }
    .encode(&out.descriptor.project_id, key);
    let body = json!({"cursor":cursor,"count":if out.duplicate {0}else{count},"duplicate":out.duplicate,"sealed":out.closed}).to_string();
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
        .into_response()
}
```

- It still has 5 parameters.
- The wire bytes are identical: axum's `String` body sets `text/plain`, then the header array's `insert` replaces `content-type` in place and appends `cache-control`, and the tuple sets status 200. The header order is the builder's.
- `out.descriptor` has the handler's incarnation. `expected_epoch` was pinned to `desc.epoch()`, and `project_id` is part of the same `sref`, so the cursor is unchanged. `product_append_and_append_many` decodes it.
- The single `json!` stays under the same owner, so the macro-dsl allowance row `crate::render_product_append / serde_json::json` (count 1) is unchanged.
- Run `cargo fmt` and keep whatever it produces for the `json!` line. The budget is ample.

**f. `product_consumer_settle` (`:3693-3748`).** It has `expect_used` (fingerprints) and `too_many_arguments` (counts).
- The `expect_used` exception (`:3693-3696`) is **narrowed**: it moves with its one `.expect` to a 6-line serialiser.
- In the body:
  - `:3718` `cname.clone(),` becomes `cname,`. `cname` has no later use in this handler, unlike in `put` and `get`, so the clone is redundant.
  - Before the `match`, add `let desc = context.descriptor().clone();`.
  - `Ok(out) => json_ok(&serde_json::to_value(out).expect("settle outcome serializable")),` becomes `Ok(out) => render_product_settle(&state, &desc, out),`.
- Arithmetic for the remaining `too_many_arguments` scope:
  - Scope lines: −4 (attribute) +1 = **−3**.
  - Syntax facts: −2 (attribute fact plus the `expect` path), −2 (`clone` method call and its call site), +5 (`context` path, `descriptor()` ×2, `clone()` ×2), −2 (the `Ok` arm goes from 7 facts to 5) = **−1**.
  - Nested items: 0.

**g. New `render_product_settle`**, directly after `product_consumer_settle`:

```rust
/// An accepted settle, including one whose tokens were all stale, is one
/// queue operation (§4.5), counted against the incarnation the settle was
/// authorized for before the outcome is answered.
#[expect(
    clippy::expect_used,
    reason = "render_product_settle; the outcome derives Serialize with plain fields, so converting it to a JSON value cannot fail; a fallible conversion would turn a completed operation into a spurious wire error"
)]
fn render_product_settle(
    state: &AppState,
    desc: &StreamDesc,
    out: crate::application::consumer::SettleOutcome,
) -> Response {
    crate::billing::meter_queue_op(state, desc);
    json_ok(&serde_json::to_value(out).expect("settle outcome serializable"))
}
```

- **Governance note.** This is the sanctioned narrowing ("narrow the scope, move code out of it"). The same decision moves word for word, with only the owner prefix changed, from a 50-line handler to the one expression it covers. No scope absorbs growth and no reason is re-decided. The tool sees a deleted identity plus a new, smaller one.
- The response is unchanged: the same `json_ok(&to_value(out))`.

### 5. `src/http.rs` (ceiling 3,153; ends about 3,118)

**a. Add `AppState::raw_append`** in the existing `impl AppState` block at `:236` (next to `append_service`; the block has no exception):

```rust
    /// The raw surface's append (`POST /v1/stream/{name}`): the typed
    /// append, then its accepted outcome (applied, a producer duplicate or
    /// a close) counted once (§4.5 `append_requests`) against the
    /// incarnation it committed to, with no second descriptor read and no
    /// await between the outcome and the count; then the protocol answer.
    pub(crate) async fn raw_append(
        self: &Arc<Self>,
        sref: crate::tenant::TenantStreamRef,
        headers: HeaderMap,
        body: Body,
    ) -> Response {
        let result = append_typed(self.clone(), sref, headers, body, None, None, None).await;
        if let Ok(out) = &result {
            crate::billing::meter_append_request(self, &out.descriptor);
        }
        render_append(result)
    }
```

**b. The `stream_entry_inner` POST arm** (`:2059-2081`, 23 lines) becomes:

```rust
        Method::POST => {
            state
                .raw_append(state.deployment.raw_adapter_sref(&name), headers, body)
                .await
        }
```

`stream_entry_inner` has one combined exception (`too_many_arguments`, `too_many_lines`, `unwrap_used`):
- Scope lines −18. Syntax facts go from about 33 to 9.
- The `append(...)` and `crate::billing::meter_append_request(&state, &desc)` call-site fingerprints are removed.
- Path counts: `state` 5 → 2, `name` 2 → 1, `headers` and `body` 1 → 1. `None` ×3, `r`, `Ok`, `Some`, `desc` and `append` go to 0.
- No new call-site fingerprint is added. A method call is not a fingerprinted site: the unwrap ratchet only fingerprints `.unwrap()` and `.unwrap_err()` methods, free calls and paths (`scripts/quality/source_rules.py:171-196`).
- **Why a method and not a free function:** a new `raw_append(...)` free call under this function-wide `unwrap_used` exception would be a new fingerprint, which is growth. The method moves the raw append out of the exceptional scope, and a method named `raw_append` cannot be a candidate unwrap site.
- `raw_adapter_sref` stays in `stream_entry_inner`, its sanctioned home (`src/mt_lint.rs:108`).
- `too_many_lines` stays fulfilled (about 127 lines remain).

**c. Delete `append()` and its doc** (`:2479-2513`, 35 lines). This deletes its `too_many_arguments` exception outright.
- Its doc describes the retry semantics that `AppendService::closure_is_current` (`src/application/append.rs:192-199`) already documents.
- `append()` had one production caller, now gone. `SealAuthz` and `render_append` are still used by `append_typed`, `raw_append` and `telemetry_append`.

### 6. `src/dst/tests/billing_usage.rs` (961 → 961)

- `:878` `crate::http::append(` becomes `crate::http::append_typed(`. The arguments are identical.
- `:887` `assert!(r.status().is_success());` becomes `assert!(r.is_ok());`.
- This is the system `_usage` path, which never counted before (the count lived in `stream_entry_inner`).

### 7. `src/dst/dst_tests.rs` (273 → 276)

Insert after the `billing_readiness` pair:

```rust
#[path = "tests/billing_operation_counts.rs"]
mod billing_operation_counts;
```

### 8. `src/dst/tests/billing_operation_counts.rs` (new, about 450 lines, under 1,000)

- The §3 tests and helpers.
- Nesting stays at 4 or less (loop, then `if`).
- Every test is under 100 lines, with no `json!` and no globs.
- `tokio::spawn` appears only in `park_append` and in P4.

**Ratcheted scopes that are not touched:** `append_typed`, `submit_product_append`, `product_append`, `product_append_sealing`, `settle` in `delivery.rs`, `render_append`, `stream_entry`, `billing.rs` (unchanged: 2,151 lines).

**The `product_entry` architecture budget** (`function:src/product.rs::product_entry`, limit 318) is still needed and not exceeded: about 305 lines, which is over 200.

## 5 Mutation analysis

**Selection.**
- The only changed file under the critical prefixes or owner rows is `src/http.rs`, with owner `http` (`scripts/quality/mutation_owners.py:158`).
- `src/product.rs` is not selected. It does not match `src/product_cursor` and has no row.
- `src/application/append*`, `src/application/consumer.rs` and `src/dst/**` match no prefix and have no row (`scripts/quality/verification_plan.py:21-31`).

**The filter change.** Add `dst_tests::billing_operation_counts::raw_` to the http owner's filters. This adds R3 and P2 only; the product tests stay out of every future http run.

**The changed lines in `src/http.rs`** are the `raw_append` body and the three new POST-arm lines. Deletions produce no mutants. cargo-mutants 27.1.0 viable mutants whose spans overlap them:

| mutant | killed by | how |
|---|---|---|
| `replace AppState::raw_append -> Response with Default::default()` (`http::Response<Body>: Default`, 200 with an empty body) | P2, R3 | P2: the first POST expects 204 and gets 200. R3: never parks, and the 3 s assertion fires. |
| `replace stream_entry_inner -> Response with Default::default()` (the body span overlaps the changed arm) | P2, R3 | P2: the raw `PUT` expects 201 and gets 200. R3: create fails. |
| `delete match arm Method::POST in stream_entry_inner` (the match has `_ =>`) | P2, R3 | The POST falls to 405. P2 expects 204. R3 never parks. |

- The changed lines contain no binary or unary operators, guards or struct bases. `if let Ok(out) = &result` has no mutation operator.
- There are no equivalent mutants: each replacement gives a distinguishable status or skips the append entirely.
- **Boundedness.** Every killer fails within 3 s (the park wait) or on its first request. `hreq` and `preq` are capped at 60 s. The baseline run of the two raw tests takes about 1 to 2 s each.

## 6 Ledgers (same commit)

| ledger | change |
|---|---|
| `docs/refactor/test-inventory.json` | `python3 scripts/test-inventory.py --write`. The diff must be exactly 9 new entries (file `src/dst/tests/billing_operation_counts.rs`, `scenarios: []`) plus one `function_sha256` change for `telemetry_crash_points_and_cost_gates`. |
| `docs/quality/owners.json` (insert in place, beside the `crate::billing_readiness` row, without re-sorting; `ensure_ascii=False`) | **1.** `by-path-module`, owner `crate::billing_operation_counts`, path `src/dst/dst_tests.rs`, syntax `path = "tests/billing_operation_counts.rs"`, count 1. Reason: "Operation-count scenarios for §4.5 append_requests/queue_operations: real HTTP appends and settles, appends parked at the committer enqueue under registry faults, and a handler dropped mid-commit; compiled and executed with DST." **2.** `effect`, owner `crate::park_append`, path `src/dst/tests/billing_operation_counts.rs`, syntax `tokio::spawn`, count 1. Reason: "Operation-count fixture owns one request handle per scenario; the request is parked at the committer enqueue by stream name and joined after release. Concurrent execution is required to change the registry while the append is in flight." **3.** `effect`, owner `crate::an_append_its_handler_never_answered_is_not_counted`, same path and syntax, count 1. Reason: "Abandoned-append fixture owns the product handler task; it is aborted while durability dispatch is held and joined before release. Concurrent execution is required to drop the handler between the commit and its answer." |
| `scripts/quality/mutation_owners.py` | Add ` dst_tests::billing_operation_counts::raw_` to the `http` row's filter string. No test pins that string. |
| `docs/refactor/WIRE-MATRIX.md` | Metering identity wording only; no wire change. **Line 44** (raw POST): "an accepted typed outcome (applied, producer duplicate, close) → `meter_append_request` against the incarnation the outcome committed to (`AppState::raw_append`; no second descriptor read); refusals and requests dropped before their outcome count nothing". **Line 118** (product records): "... → `meter_append_request` in `render_product_append` from `AppendOutcome.descriptor`". **Line 162** (settle): "an Ok settle outcome, including an all-stale settle → `meter_queue_op` in `render_product_settle`, against the settle context's incarnation". **Lines 257-258** (summary): "at the typed outcome", naming the three sites. |
| `docs/refactor/review-mechanisms.json`, `test-additions.json` | None. No pinned mechanism or test is touched (checked by grep). |
| `docs/quality/source-allowances.json`, `diagnostic-allowances*.json`, `architecture-policy.json`, `scripts/mt-audit-baseline.txt`, `src/dst/tests/README.md` | None. There are no legacy rows for the touched items, the `render_product_append` macro row count is unchanged, the `key_hash: crate::crypto::stream_hash(routing_key),` MT-audit line is unchanged, and `billing_*` already covers the new module. |

## 7 Controls (exact commands and expected outputs)

Put Python 3.11 or newer first on `PATH` (the shim).

1. **Red on the previous tree.**
   ```
   git worktree add /tmp/opc-red 24c4c77a4303e9b4b7a6cb09bfee26453efad657
   ```
   Copy in the new test file and the `dst_tests.rs` registration only, then run:
   ```
   CARGO_TARGET_DIR=<shared> cargo test --locked --lib dst_tests::billing_operation_counts:: -- --test-threads 4
   ```
   Expect `test result: FAILED. 6 passed; 3 failed`, with R1, R2 and R3 failing with the exact `left`/`right` output in §3. P0 to P5 pass, which proves they are pins. Paste the three failures into the commit message.
2. **Green.** Run `cargo test --locked --lib dst_tests::billing_operation_counts::`. Expect `9 passed; 0 failed`.
3. **Flake loop.**
   ```
   for i in $(seq 1 20); do cargo test --locked --lib dst_tests::billing_operation_counts:: -q || break; done
   ```
   Expect 20 green runs. A background registry read on an `opc*` name would show up here as R2 or R3 non-vacuity failures.
4. **Existing pins.**
   ```
   cargo test --locked --lib dst_tests::producer_protocol:: -- dst_tests::billing_usage:: dst_tests::consumer_product:: dst_tests::durability_fences::
   ```
   Expect all ok.
5. **P0 non-vacuity control.** Change `"no-store"` to `"no-cache"` in `render_product_append` and run P0. It fails with `left: "no-cache"`. Revert.
6. **Quality gate.** Run `scripts/quality.sh`. Expect `QUALITY_OK`. This includes:
   - `cargo fmt --check`;
   - `cargo clippy ... -- -D warnings`, with no `redundant_clone` or `unfulfilled_lint_expectations` in `product.rs` or `http.rs`;
   - `gate.py --clippy`, with no "accepted exception grew", no "file growth", and no "unregistered source occurrence";
   - rustdoc;
   - `architecture-gate --check`;
   - `test-inventory --check`;
   - `MT_AUDIT_OK`;
   - the mt-lint leg.
7. **File sizes.** Run `wc -l src/product.rs src/http.rs src/billing.rs`. Expect at most 4,205, at most 3,153 and exactly 2,151. Run `git diff --stat 24c4c77a -- src/dst/tests/billing_usage.rs` and expect a line count of 0.
8. **Full suite** as CI runs it: `cargo test --locked --release`. Expect all ok.
9. **Mutation**, after committing:
   ```
   QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=24c4c77a4303e9b4b7a6cb09bfee26453efad657 QUALITY_BASE_REF=origin/slate scripts/quality/mutations.sh
   ```
   Expect `plan.json` `selected_mutation_owners: ["http"]` and `mutation_source_files ⊇ ["src/http.rs"]`. cargo-mutants should report 3 mutants: 3 caught, 0 missed, 0 timeout. Do not run this concurrently with step 6.
10. **After the push.** Run `gh run list --branch slate --json headSha,name,status,conclusion,createdAt`. Every workflow for the pushed sha must be green before claiming CI green.

## 8 Out of scope (recorded; each is its own item)

- **Dead parameters in `product_append_inner`.** `seal_after` and `seal_auth` are always `false`/`None` at its only caller, `product_append` (`:2009`); the seal path calls `submit_product_append` directly. Removing them re-decides the `fn_params_excessive_bools` reason, whose text names the flags.
- **Dead parameters in `append_typed`.** `product_hash`, `product_key` and `seal_auth` are always `None`. Removing them changes `raw_append` and `system_append` (a mutation owner).
- **`AppendOutcome.materialized`** is now derivable from `descriptor.segments`. Dropping it changes a fingerprinted call in `render_append`.
- **The same status-derived, second-lookup pattern for ops events.** The raw PUT's `stream_created` event (`src/http.rs:2043-2056`) and `stream_entry`'s `note_fleet_op` on `is_success()` (`:1971-1973`) are not billing.
- **A deterministic settle red** through the DLQ-handoff `AppendBeforeEnqueue` seam.
- **Counting at the committer.** Requests whose handler never answered could be counted there. That would need a new committer-side counter (see D2).

## 9 Decisions for the owner

**None are required for this commit.** It changes no edge, no reason text and no policy.

- The only exception whose text moves is a narrowing: `product_consumer_settle`'s `expect_used` becomes `render_product_settle`'s, with the same decision and the owner prefix changed.
- The only structural novelty is the `raw_append` method (§4 5b).

Surfaced, not acted on:
- **D1.** `docs/OBSERVABILITY-BILLING.md` §5's ops-count column says "duplicate append → duplicate", "create with initial content → create + append" and "seal with final content → seal + append". The code has no duplicate dimension and has never counted create or seal content as `append_requests`. This plan keeps the code's semantics. Either amend the matrix, or add a `producer_duplicates` dimension and create/seal op counts. The second option is a telemetry-visible change.
- **D2.** A committed append whose handler was dropped before the answer stays uncounted (current semantics; pinned by P4). Counting it would move `append_requests` to the committer beside the billed bytes.

---

## Skeptic corrections (C1..C9)

Read-only check against the tree. Nothing was built or run. I confirmed these parts of the plan on HEAD:
- Every quoted line and use site. `meter_op_if_ok`, `OpKind`, `meter_append_request` and `meter_queue_op` have only the call sites the plan lists. `http::append` has two callers: `http.rs:2060` and `billing_usage.rs:878`.
- File ceilings: product.rs 4,205, http.rs 3,153, billing.rs 2,151. The projected ends are about 4,192 and about 3,118.
- The `product_entry` architecture budget. The gate's own `find_functions` puts it at 315 lines today, 306 after the change. That is still above the baseline default of 292 (`architecture-review-baseline.json`), so the 318 exception stays needed.
- The syntax-fact arithmetic: −15 facts per `product_entry` arm, −1 facts / −3 lines in `product_consumer_settle`, 0/0 in `execute_once`.
- Fingerprints: method calls are not fingerprinted (`source_rules.py:171-196`), and `raw_adapter_sref` stays in its sanctioned home (`mt_lint.rs:108`).
- The R1, R2 and R3 red traces. `Registry::get` checks `take_fail_next_get` before the cache (`cache.rs:298-304`). `test_poison_cache` inserts a Fresh entry (`cache.rs:279-292`). `submit.rs` reads the shard directory only, and `shard.rs` has no registry reads. `close::complete` does nothing for a non-close append (`close.rs:231-252`).
- Borrows in `execute_once`: `ClosePlan` and `SegRoute` own their data, so moving `desc` in the `Ok` arm compiles.
- The mutation selection: `src/product.rs`, `src/application/*` and `src/dst/*` match no critical prefix or row (`verification_plan.py:21-31`).
- The owners and effect row shapes (precedents: `durability_gather`, `billing_readiness`). `test-inventory --check` compares against `test-inventory.json` only, so `--write` is enough.
- No reason text is edited. Two exceptions are deleted outright. One (`expect_used`) moves to a strictly smaller scope with the same decision.

The corrections:

**C1. The base is not 24c4c77a.**
- Local `slate` HEAD is `95bfabc6`, one unpushed commit ahead of `origin/slate`. It touches `docs/quality/owners.json` (10 lines changed), `src/tasks/refusal.rs` and `src/tasks/tests.rs`.
- Build on the real parent. Merge the three owners.json rows with 95bfabc6's rows, inserted in place.
- Use the actual parent sha in control 1 (red worktree), control 7 (diff base) and control 9 (`QUALITY_BEFORE_SHA`). If 95bfabc6 is pushed in the same push, control 9 also selects `tasks_refusal` (`mutation_owners.py:65`) and the `src/tasks` prefix, so "selected_mutation_owners: [\"http\"]" is wrong for that push.
- Put the red worktree under the scratchpad, not `/tmp/opc-red`.

**C2. An MT-audit trap in the §4 4e snippet.**
- The snippet shows `key_hash: crate::crypto::stream_hash(routing_key),   // keep this exact line: MT audit fingerprint`.
- `scripts/multitenancy-audit.sh:60-64` fingerprints the whole normalized line text, trailing comment included. The baseline line is `scripts/mt-audit-baseline.txt:44`. Copying the comment would produce a NEW and a GONE fingerprint, and `MT_AUDIT_OK` would fail.
- Fix: the line must be exactly `key_hash: crate::crypto::stream_hash(routing_key),`, with no comment on it. The same applies to the unchanged `let kh = crate::crypto::stream_hash(&rk);` (`:45`).

**C3. Settle does have a buildable red. Adopt the reviewer's settle variant ("arm the failure after authorize and active resolve").**
- The plan says "there is no seam" (§3) and defers the DLQ seam to §8. The seam is buildable with existing hooks and the `park_append` helper, with no new effect row.
- Add **R4 `a_settle_counts_when_its_descriptor_cannot_be_read_again`**:
  1. Create `opcdlqsrc` and the DLQ target `opcdlqtgt`. Append 1 record.
  2. Create consumer `c1` with `{"maxAttempts":1,"deadLetterStream":"opcdlqtgt"}` (precedent: `consumer_dlq.rs:13-62`, r07). Pull, which gives `queue_operations` = 1.
  3. `park_append("opcdlqtgt", <settle {"retries":[{"leaseToken":T,"delayMs":0}]}>)`. The body is built with `format!`, not `json!`. The retry exceeds `maxAttempts`, so the lease is poisoned. `dlq_and_settle` then reads the target descriptor (`delivery.rs:630`, before the park) and appends through `state.append.execute` (`delivery.rs:712`). That call parks at `append.rs:349`, keyed on the DLQ name.
  4. `fail_next_get("opcdlqsrc")`, then release. Assert 200 and a body `dlq` of 1.
  5. `assert_eq!(counts(&state, &src_epoch).queue_operations, 2, "a failed descriptor re-read dropped a committed settle's count")`.
- Red trace (old tree): after the DLQ append, the settle does only `submit_queue` (`delivery.rs` about :730) and no registry read → `json_ok` → 200 → `product_entry:1252` → `meter_op_if_ok` → `take_fail_next_get("opcdlqsrc")` → `Err` → `left: 1, right: 2`.
- Non-vacuity: `registry.get(src_sref).await.is_err()`. The DLQ append is `AppendService::execute`, which is not metered as `append_requests`.
- Knock-on changes: control 1 becomes `6 passed; 4 failed`, control 2 becomes `10 passed`, and test-inventory gets 10 new entries.
- The §3 sentence "Settle has no red" and the §8 bullet are withdrawn.

**C4. Mutation boundedness has to cover the existing filter set.**
- Under the `replace stream_entry_inner -> Response with Default::default()` mutant, every raw-surface request answers an empty 200. That affects the whole http owner filter set: `http::` also matches `dst_tests::fixture_http::`, plus `livefeed_engine_retired`, `security_workload::`, `debug_*` and `dst_tests::billing_readiness::`. The run uses `--timeout 90` (`mutation_driver.py:52`), and a TIMEOUT fails the leg.
- This whole-body mutant has never run under the current filters. The http row landed in `99d5c098` (2026-09-13), and `stream_entry_inner` last changed in `96fed7cc` (2026-09-06).
- §5 "Boundedness" only argues for the new tests. Control 9 must therefore run before the push, and any TIMEOUT is a blocker. If one appears, the fix is to bound the offending test, not to narrow the filter.
- Also correct control 9's expected receipt:
  - Editing `scripts/quality/mutation_owners.py` makes `tooling` true (`verification_plan.py:77-99`), so `properties_fuzz` is true.
  - `src/http.rs` is a BUFFER_PREFIX, so `miri` is true.
  - Expect `mutants: true`, `properties_fuzz: true`, `miri: true`. The run is longer than the plan implies.

**C5. Controls for the meter call sites that nothing else guards.**
- cargo-mutants does not delete statements, and `src/product.rs` is not mutated at all. So nothing in §5 proves that P1, P2 and P3 guard the three new `meter_*` calls.
- Add three temporary-deletion controls, each followed by a revert:
  - Remove `crate::billing::meter_append_request(state, &out.descriptor);` from `render_product_append`. P1 fails on its first count assertion, `left: 0, right: 1`.
  - Remove `meter_append_request` from `AppState::raw_append`. P2 fails, `left: 0, right: 1`.
  - Remove `meter_queue_op` from `render_product_settle`. P3 fails at the ack settle, `left: 1, right: 2`.

**C6. Name the documented contract the edits must honour.**
- `docs/OBSERVABILITY-BILLING.md:275` says: "Every public operation gets a single metering choke point and a focused test. No billing logic is duplicated between raw, product, ...".
- After this change each operation still has exactly one meter site: raw POST → `AppState::raw_append`, records/`:batch` → `render_product_append`, settle → `render_product_settle`. The identity logic stays in `billing::meter_*`. State this in the commit body and in the WIRE-MATRIX 257-258 edit. It is not a contract change.
- WIRE-MATRIX:106 ("seal final: op count does NOT fire") stays true only because the seal path calls `submit_product_append` directly (`product.rs:1956`) and `render_product_append` has exactly one caller (`product_append_inner`, `:2235`). Cite that in the §2 row, "seal's final record ... 0, kept".

**C7. A touched ratcheted scope the plan does not list.**
- `telemetry_crash_points_and_cost_gates` carries `#[expect(clippy::too_many_lines)]` (`billing_usage.rs:776-779`). Its status after the edit: scope lines 0, syntax facts −2 (`r.status().is_success()` becomes `r.is_ok()`), no growth.
- Control 7's `git diff --stat ... billing_usage.rs` prints 2+/2−, not 0. Assert `wc -l src/dst/tests/billing_usage.rs` = 961 instead.

**C8. §2 table precision.** Pull counts +1 (plus bytes) only when `messages` is non-empty (`consumer_pull.rs:80-88`). "+1" per pull is inaccurate, although the row is unchanged by this plan.

**C9. The `expect_used` narrowing: record its metrics.**
- The moved exception is a new identity, so `exception_growth` does not compare it (`source_rules.py:207-208`). Under the governance rule, the proof that it is a narrowing is ours to write down.
- Record it in the commit body:
  - old scope: `product_consumer_settle`, 3693-3748, 56 lines, `expect_sites` 1;
  - new scope: `render_product_settle`, about 12 lines, `expect_sites` 1;
  - identical decision text after the owner prefix.
- I accept this as a sanctioned narrowing, not a re-decided reason.

**Verdict: ready-with-corrections.** The design is sound, and it is stronger than the review's own fix: it counts against `AppendOutcome.descriptor`, the incarnation the append committed to, rather than the handler's pre-submit desc. Every correction is in the tests, controls or ledgers. None changes the design or needs an owner decision. The one gap in adopting the reviewer's position was the settle red, which C3 supplies.
