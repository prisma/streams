# Item 52: the telemetry-append receiver reads its body before authentication

Repo `/Users/sorenschmidt/code/streams`, branch `slate`. HEAD is `2fb92fb9`. The merge base is `origin/slate` = `729c52ac`. HEAD is **2** commits ahead of the merge base (557e8ba6, 2fb92fb9), not five as the task text says. Neither of those commits touches a file this plan edits. Every line count below was taken with `wc -l` and matches `git show origin/slate:<file> | wc -l`.

Plan in one line: move the handler verbatim into `src/http/telemetry_append.rs`. Then change its `body: Bytes` extractor to `body: Body` and buffer it with `axum::body::to_bytes(body, state.config.cli.max_request_body_bytes)` after every header check. An over-limit body gets the append contract's typed `too_large`. `billing::append_local` is unchanged.

---

## 1. Problem (verified on the current tree)

### 1.1 The body is buffered before authentication, under axum's implicit 2 MiB cap

The handler is at `src/http.rs:1220-1268`:

```rust
async fn internal_telemetry_append(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,                                   // http.rs:1227
) -> Response {
    if !fleet_operation_authorized(&state, &headers, InternalOperation::TelemetryAppend) {   // :1229
        return internal_unauthorized();
    }
```

axum runs every extractor before the handler body executes. The auth check at :1229 therefore runs after `Bytes::from_request` has already buffered the upload. The extractor source (axum-core 0.5.6, `src/extract/request_parts.rs:106-112`):

```rust
let bytes = req.into_limited_body().collect().await.map_err(FailedToBufferBody::from_err)?.to_bytes();
```

`into_limited_body` applies axum's default limit (`src/ext_traits/request.rs:319,326`):

```rust
const DEFAULT_LIMIT: usize = 2_097_152; // 2 mb
None => self.map(|b| Body::new(http_body_util::Limited::new(b, DEFAULT_LIMIT))),
```

Nothing in `src/` sets a `DefaultBodyLimit` or `RequestBodyLimit` layer. `grep -rn DefaultBodyLimit src` returns nothing, and `router()` (http.rs:1343-1643) adds only `track_inflight` and the origin-marker `map_response`. When the limit trips, axum answers 413 `text/plain` with the body `Failed to buffer the request body: length limit exceeded` (`rejection.rs:40-48`, plus `LengthLimitError`'s Display `length limit exceeded`).

**The contract this breaks** is R25-E, stated at `src/http.rs:710-716`:

> "...the contract requires 401: authenticate before buffering or materially consuming the request body."

Consequences on the current tree:

- An unauthenticated POST of up to 2 MiB is fully buffered and then answered 401.
- An unauthenticated POST over 2 MiB has 2 MiB plus one chunk read and is answered **413**, never 401.
- A fleet-authenticated caller is capped at 2 MiB. The local append path allows `max_request_body_bytes`, which is 32 MiB by default (`src/config/cli.rs:128-129`, `MAX_REQUEST_BODY_BYTES`, `default_value_t = 32 * 1024 * 1024`).
- The senders size their batches against that same configured limit:
  - `ops::drain_ops_once` calls `PendingOps::take(..., state.config.cli.max_request_body_bytes, ...)`.
  - `audit::drain_audit_once` calls `PendingAudit::take(..., state.config.cli.max_request_body_bytes, ...)`.

  So the sender and the receiver disagree about the limit.

### 1.2 "Copied twice": real, and structural

For a multi-frame hyper body there are two copies:

1. `Collected::to_bytes()` in the extractor copies into one buffer.
2. `append_local` → `attempt` (`src/billing/system_append.rs:61-77`) wraps the `Bytes` in `Body::from(body)` and calls `append_typed`. That re-buffers through `buffer_body_charged` (`src/http.rs:651-670`, `buf.extend_from_slice(&c)`).

On first use, `append_local`'s NotFound → create → retry clones the `Bytes` (a refcount bump). The first attempt fails in `service.prepare` before its body is read, so there is no third copy.

The second copy exists because `append_local` must **replay** the body after creating on NotFound. That needs owned `Bytes`, and `append_typed` buffers whatever `Body` it is handed. This plan does not remove the second copy (see §8).

### 1.3 "The wedge is not reachable": true for the journals, not demonstrated for `_usage`

- The ops and audit journals are capped at 512 events (`telemetry_batch::encode_prefix`, `events.take(512)`). Audit routes are cut to 256 bytes (`audit.rs` `ROUTE_MAX`). A 2 MiB batch from them is implausible.
- `_usage` has no such cap. `billing::drain_once` step 1 (`src/billing.rs:787-822`) takes up to 64 spooled read batches with **no byte budget**. `DRAIN_MAX_BYTES` (1,000,000) applies only from step 2 on (`:908`). Each read batch seals at an estimate of about 1 MiB (`read_accumulator.rs:10`, `READ_FLUSH_MAX_EST_BYTES = 1 << 20`).
- A `_usage` body over 2 MiB is therefore plausible under a spooled backlog. It is not measured. If it happens while the `_usage` owner is remote, the relay gets a 413 and the sender requeues the same batch every round: a wedge.
- This fix raises the relay's threshold to the configured limit, the same as the local path. The step-1 byte budget itself is a separate item (§8).

### 1.4 Rank 21 (8dabca7f) did not fix this

8dabca7f moved the handler onto `crate::billing::append_local` (http.rs:1243). The extractor and the ordering were left unchanged, so the problem is still present on HEAD.

### 1.5 Every use site

| Site | Location | Role |
|---|---|---|
| Handler | `src/http.rs:1220-1268` | the defect |
| Route | `src/http.rs:1390-1393` (`post(internal_telemetry_append)`) | registration |
| Sender (only client) | `src/billing/system_append.rs:141-170` `relay()` | reqwest POST with bearer, key and JSON body; any non-2xx becomes `Err("telemetry relay {stream}: {status}")` |
| Relay entry | `src/billing/system_append.rs:120-139` `system_append` | local first, then relays once on an owner-bearing refusal |
| `system_append` callers | `billing.rs:747` (`_usage`), `ops.rs:335` (`_ops_events`), `ops.rs:617` (`_ops_metrics`), `audit.rs:302` (`_audit_events`), `fleet.rs:1137` (fleet outbox → `_ops_events`) | batch producers |
| `append_local` callers | `system_append.rs:130`, `http.rs:1243` | typed local path |
| Same defect class, left alone | `src/product.rs:3332-3344` `internal_sweep_segment` (`body: Bytes` before `fleet_operation_authorized`) | §8 |
| Tests on the route | `security_workload.rs:139-149, 219-226, 386-458, 550-576, 586-656, 746-763`; `admission_maintenance.rs:630-677` | pinning tests (§3) |
| Docs | `docs/refactor/WIRE-MATRIX.md:225` | contract row |

Only two handlers in the tree use a buffering body extractor: `internal_telemetry_append` and `internal_sweep_segment`. `grep -rn "body: Bytes," src`; every other hit is an internal function that is handed an already-buffered body.

### 1.6 The reviewer's Change cannot be built on the current tree

The reviewer's Change is "Take `body: Body` and pass it to `append()`". That does not work here:

- **`append()` is the raw surface's rendering wrapper.** Routing the receiver through it would drop `append_local`'s typed create-on-NotFound. An unprimed owner would answer a relayed first batch with `404 not_found`. That is exactly the red that 8dabca7f fixed; `unprimed_system_append_relays_its_first_batch_to_the_owner` and `telemetry_append_receiver_reports_ownership_not_absence` would go red.
- **`append_local` must replay the body after a create**, and a `Body` can be used only once.

The alternative that builds: keep `append_local` exactly as it is, and move only the buffering. It happens after authentication and every header-level refusal, and it uses the configured limit.

---

## 2. Contract decision

**Typed contract.** An over-limit body is refused with the append contract's own verdict:

```rust
render_append(fail(FailureClass::Invalid, AppendCode::TooLarge, "body too large"))
```

`append_failure_status` maps that to 413 and the envelope is `{"error":{"code":"too_large","message":"body too large"}}`. Both `append_typed` (for a streamed body over the limit) and the raw PUT create path (`to_bytes` at http.rs:2236-2241) already answer with exactly this code and message. No new string code is introduced: the code comes from `AppendCode::TooLarge.as_str()`.

**Order after the fix:**

1. Authenticate (401).
2. Reserved-name check (403 `not_system_stream`).
3. Key (400 `missing_key`).
4. Canonical name (400 `invalid_name`).
5. Read the body under `MAX_REQUEST_BODY_BYTES` (413 `too_large`).
6. `append_local`, unchanged.

**Wire changes.** These affect only the fleet-internal route `/v1/internal/telemetry-append/{*name}`:

| Request | Before | After |
|---|---|---|
| No or invalid credential, body > 2 MiB | 413 `text/plain` after about 2 MiB read | 401 `unauthorized` JSON, body never read |
| No or invalid credential, body ≤ 2 MiB | 401 after a full buffer | 401, body never read |
| Credential OK, 2 MiB < body ≤ configured limit | 413 `text/plain` | the append contract (204, or 409 `not_ring_owner`, ...) |
| Credential OK, body > configured limit | 413 `text/plain` | 413 `too_large` JSON |
| Credential OK, header refusal (403, or 400 `missing_key` / `invalid_name`), body > 2 MiB | 413 | the header verdict |

**No product/raw-edge, metric, `/metrics` or `/v1/debug` change.**

**Mixed-version fleets are safe in both directions.** The only client is `system_append::relay`. It is unchanged, and it reports every non-2xx or transport error the same way: an old sender against a new receiver works, and a new sender against an old receiver keeps today's 2 MiB relay ceiling.

**Considered and rejected:**
- Creating the stream first, then appending the `Body` in one streamed pass. This adds a create round on every relay. It also turns a rotated key's typed `wrong_key` into a creation refusal, which breaks the invariant pinned by `a_rotated_system_key_is_the_appends_refusal_never_a_recreate`.
- `DefaultBodyLimit::max(..)` on the route. It still buffers before authentication.

---

## 3. Red tests (commit 2; written first, on top of commit 1)

**File:** `src/dst/tests/security_workload.rs`, currently 763 lines, ceiling 1,000, so there is room for 237 lines. The additions are about 95 lines, for about 858. They go in a new section after the rank-21 section, at the end of the file:
- `// ---- review item 52: the receiver reads a body only after authentication ----`.
- Two helpers plus two tests.
- Existing imports already cover `PRISMA_KEY`, `http_rig_build`, `HttpRigOptions`, `RigRuntime`, `mem` and `engine_shutdown`.

**Why in process rather than `hreq`.** An early refusal over TCP races the unread upload. The client's `write_all` gets EPIPE/ECONNRESET, or the RST drops the response, so the red message would not be predictable. The in-process call goes through `crate::http::router(state)` wrapped in `hyper_util::service::TowerToHyperService`, which is exactly what `serve_h1` does per connection (http.rs:1284). It also lets the test **count the bytes the receiver polled**, which is the R25-E claim itself.

```rust
/// The router in process, as `serve_h1` hands it each request: the test
/// owns the body stream and can count what the receiver polled, where
/// over TCP a refusal racing an unread upload surfaces as a reset.
async fn route_in_process(
    state: &std::sync::Arc<crate::http::AppState>,
    request: axum::http::Request<axum::body::Body>,
) -> (u16, String) {
    use hyper::service::Service as _;
    let router = crate::http::router(state.clone());
    let Ok(response) = hyper_util::service::TowerToHyperService::new(router).call(request).await;
    let status = response.status().as_u16();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// A relay-shaped POST of `_audit_events` carrying the system key.
fn telemetry_append_request(
    bearer: Option<&str>,
    body: axum::body::Body,
) -> axum::http::Request<axum::body::Body> {
    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/internal/telemetry-append/_audit_events")
        .header("content-type", "application/json")
        .header("stream-encryption-key", PRISMA_KEY);
    if let Some(bearer) = bearer {
        request = request.header("authorization", bearer);
    }
    request.body(body).unwrap()
}
```

The irrefutable `let Ok(..)` compiles on the pinned 1.98.1 toolchain because the error type is `Infallible`. `hyper::service::Service` is public in hyper 1.10.1 (`src/lib.rs:125`, `service/mod.rs:29`), and hyper-util's `service` feature is enabled in `Cargo.toml`.

### Test 1: `telemetry_append_refuses_an_unauthenticated_body_unread`

Full name: `dst::dst_tests::security_workload::telemetry_append_refuses_an_unauthenticated_body_unread`.

```rust
/// RED (review item 52): R25-E — authenticate before buffering or
/// materially consuming a request body. The receiver extracted
/// `body: Bytes`, so axum buffered the upload under its implicit 2 MiB
/// default BEFORE the fleet-credential check: an unauthenticated 3 MiB
/// POST was read past 2 MiB and answered 413, never 401.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn telemetry_append_refuses_an_unauthenticated_body_unread() {
    use futures_util::StreamExt as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    const CHUNK: usize = 64 * 1024;
    let (state, _addr) = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default())
        .await
        .parts();
    let polled = std::sync::Arc::new(AtomicUsize::new(0));
    let counter = polled.clone();
    let upload = futures_util::stream::iter(0..48).map(move |_| {
        counter.fetch_add(CHUNK, Ordering::Relaxed);
        Ok::<_, std::io::Error>(bytes::Bytes::from(vec![b' '; CHUNK]))
    });
    let request = telemetry_append_request(None, axum::body::Body::from_stream(upload));
    let (status, text) = route_in_process(&state, request).await;
    assert_eq!(
        (status, polled.load(Ordering::Relaxed)),
        (401, 0),
        "an unauthenticated telemetry append is refused before its body is read: {text}"
    );
    engine_shutdown(&state).await;
}
```

**Red on the current tree, traced:**
- The `Bytes` extractor wraps the body in `Limited(2_097_152)` and collects it.
- The mapped stream is polled once per frame. Chunks 1-32 sum to exactly 2,097,152, which leaves `remaining = 0`. Chunk 33 has 65,536 > 0, so it raises `LengthLimitError`.
- Result: 33 × 65,536 = 2,162,688 bytes polled, then a 413 rejection. The handler never runs.

Exact output:

```
assertion `left == right` failed: an unauthenticated telemetry append is refused before its body is read: Failed to buffer the request body: length limit exceeded
  left: (413, 2162688)
 right: (401, 0)
```

`StatusCode` is compared as a `u16`, so Debug prints plain numbers.

**Green:** the `Body` extractor is `req.into_body()` (axum-core `request_parts.rs:164-172`), which applies no limit. The auth check fails first, returns `internal_unauthorized()`, and the stream is dropped without being polled: `(401, 0)`.

### Test 2: `telemetry_append_reads_to_the_configured_limit_after_authentication`

```rust
/// RED (review item 52): the receiver's body limit is the CONFIGURED
/// request limit — the one the local append path enforces and the
/// senders' journal batches are budgeted against — not axum's implicit
/// 2 MiB: a relayed batch between the two was refused on the owner and
/// requeued by its sender every round. Over the configured limit the
/// refusal is the append contract's typed `too_large`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn telemetry_append_reads_to_the_configured_limit_after_authentication() {
    const AXUM_DEFAULT: usize = 2 * 1024 * 1024;
    const CONFIGURED: usize = 4 * 1024 * 1024;
    let (state, _addr) = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            max_request_body_bytes: Some(CONFIGURED),
            ..Default::default()
        },
    )
    .await
    .parts();
    let pad = "x".repeat(1024);
    let rows: Vec<String> = (0..2560)
        .map(|i| format!(r#"{{"eventId":"big-{i}","pad":"{pad}"}}"#))
        .collect();
    let batch = format!("[{}]", rows.join(","));
    assert!((AXUM_DEFAULT..CONFIGURED).contains(&batch.len()), "{}", batch.len());
    let fleet = Some("Bearer dst-internal-token");
    let request = telemetry_append_request(fleet, axum::body::Body::from(batch));
    let (status, text) = route_in_process(&state, request).await;
    assert_eq!(status, 204, "an authorized batch within the configured limit lands: {text}");
    let (page, _) = crate::billing::system_read(&state, "_audit_events", PRISMA_KEY, None)
        .await
        .expect("system read")
        .expect("the batch created the stream");
    assert!(String::from_utf8_lossy(&page).contains("\"big-0\""), "the batch landed");
    let over = axum::body::Body::from(vec![b' '; CONFIGURED + 1]);
    let (status, text) = route_in_process(&state, telemetry_append_request(fleet, over)).await;
    assert!(
        status == 413 && text.contains("\"too_large\""),
        "over the configured limit the append contract refuses it: {status} {text}"
    );
    engine_shutdown(&state).await;
}
```

**Sizing checks:**
- The batch is exactly 2,702,251 bytes. That is 2,560 rows of `1051 + digits(i)` bytes (sum 2,699,690), plus 2,559 commas, plus 2 brackets. It sits inside [2 MiB, 4 MiB), and the first assert pins that.
- 2,560 records is under the fresh record bucket of 5,000/s × 2 s = 10,000.
- 2.7 MB is under the byte bucket of 5 MB/s × 2 s = 10 MB (`config/model.rs:508-511`).
- The deterministic CLI leaves `MAX_RECORD_PAYLOAD_BYTES` unset, so there is no per-record ceiling.
- The rig's default static fleet token is `dst-internal-token` (`fixture_http.rs:441`).
- `render_append` answers 204 because no producer is set (http.rs:44).

**Red on the current tree, traced:** `Body::from(String)` is a single frame of 2,702,251 bytes, which is more than 2,097,152, so `Limited` raises the error and the answer is 413. Exact output:

```
assertion `left == right` failed: an authorized batch within the configured limit lands: Failed to buffer the request body: length limit exceeded
  left: 413
 right: 204
```

**Green, traced:**
- The request passes auth, the reserved name, the key and the canonical name.
- `to_bytes(4 MiB)` accepts the body.
- `append_local`: the first attempt hits NotFound in `prepare`, the stream is created, the second attempt lands, and the answer is 204.
- `system_read` returns the first page, which contains `"big-0"`.
- The 4 MiB + 1 byte body makes `Limited` fail, and the answer is `render_append(fail(Invalid, TooLarge))`: 413 `{"error":{"code":"too_large","message":"body too large"}}`.

**Test-only lint budgets:**
- Each test is under 100 lines.
- Nesting depth is at most 2.
- The helpers take at most 2 arguments.
- `unwrap`/`expect` are covered by `allow-unwrap-in-tests` (clippy.toml:8-10).

### Pinning tests for commit 1 (verbatim move; no behaviour change)

`security_workload::` covers all 11 tests; the ones that reach the route are:
- `telemetry_append_receiver_reports_ownership_not_absence` (409, `__ds` 400, `invalid_key` 400)
- `workload_jwt_operations_scope_the_internal_surface` (401 for a `segment-read` token)
- `static_token_is_dead_in_workload_mode` (401 static, ≠401 JWT)
- `jwt_only_fleet_relay_succeeds`
- `unprimed_system_append_relays_its_first_batch_to_the_owner`
- `a_relay_the_skewed_owner_refuses_is_reported_not_counted_as_landed`

Also `admission_maintenance::reserved_streams_append_through_a_latched_engine`.

**Compile-level proof of the move:** `git diff -M --color-moved=zebra HEAD~1 -- src/http.rs src/http/telemetry_append.rs` shows the function body as moved lines. The only changed token is `async fn` → `pub(super) async fn`. A successful build then resolves every `super::` import, including the private `raw_key`, which child modules may see.

---

## 4. Edits, file by file, in commit order

### Ceilinged files (wc -l now = at merge base → after)

| File | Now | After | Budget |
|---|---|---|---|
| `src/http.rs` | 3,362 | **3,314** | may not exceed 3,362 |
| `src/product.rs` | 4,205 | 4,205 | untouched |
| `src/shard.rs` | 3,196 | 3,196 | untouched |
| `src/billing.rs` | 2,201 | 2,201 | untouched |
| `src/history.rs` | 1,713 | 1,713 | untouched |
| `src/auth.rs` | 1,676 | 1,676 | untouched |
| `src/registry.rs` | 1,492 | 1,492 | untouched |
| `src/sse/feed.rs` | 1,170 | 1,170 | untouched |
| `src/fleet.rs` | 1,143 | 1,143 | untouched |
| `src/dst/tests/security_workload.rs` | 763 | ~858 | ≤ 1,000 |
| `src/http/telemetry_append.rs` (new) | — | ~64 (commit 1), ~71 (commit 2) | ≤ 1,000 |

### Commit 1: "The telemetry-append receiver moves into http/telemetry_append.rs, verbatim"

**1. New `src/http/telemetry_append.rs`.** Explicit imports, not `use super::*`, so no `unresolved-glob` owners row is needed:

```rust
//! The owner-side receiver of the fleet's system-stream relay (round-21
//! blocker 5). The sender is `billing::system_append`; both halves run
//! `billing::append_local`, so a refusal here is the owner's own typed
//! decision.

use super::{
    AppState, InternalOperation, creation_error_response, err_resp, fleet_operation_authorized,
    internal_unauthorized, raw_key, render_append,
};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use bytes::Bytes;
use std::sync::Arc;

<http.rs:1220-1268 verbatim, with `async fn` → `pub(super) async fn`>
```

Run `cargo fmt` afterwards; it may reorder the `use` block.

**2. `src/http.rs`:**
- Delete lines 1220-1268 (the 3-line doc comment and the function) plus one adjacent blank line: −50 lines.
- In the module tail (3350-3353), add two lines. `mod telemetry_append;` goes after `mod serve;`. `use telemetry_append::internal_telemetry_append;` goes after the `pub(crate) use read_adapter::{..}` line: +2 lines.
- The route at 1390-1393 stays **byte-identical**, so the scope of `router`'s `#[expect(too_many_lines, disallowed_methods)]` does not change and no FnValue mutant is selected for `router`.
- Net 3,314.

**3. `scripts/quality/mutation_owners.py`:** add after line 88 (`http_read`):

```python
    owner('http_telemetry_append', 'src/http/telemetry_append.rs', 'security_workload:: reserved_streams_append'),
```

**4. `docs/refactor/WIRE-MATRIX.md:225`:** change `` (`src/http.rs:1227`, `` to `` (`src/http/telemetry_append.rs`, ``.

**Ratcheted scopes touched in commit 1:**
- The moved function carried no `#[expect]` or `#[allow]`, no macros and no statics. No `source-allowances.json` row names it: the http.rs rows are only `engine_for*`, statics, `router`/`serve_h1` effects and `json!` macro-dsl rows for other functions. Nothing is vacated.
- `router`'s exception: the route text is unchanged, so its scope_lines, nested_items and syntax_facts are unchanged. The `use` alias sits outside any exception scope.
- `serve_h1`'s exception only shifts lines; its size is identical.
- The new file has no exceptions.
- `architecture-gate`: `reverse_edges` counts only `http`/`product` segments. `use super::{..}` names none, and the file uses `crate::billing`/`crate::tenant` only, so there are no edges and no `architecture-policy.json` entry is needed. The file's `before` defaults to 1,000 lines and its functions to 200.
- `mt_lint`: `Path(name): Path<String>` is a TupleStruct pattern, not `Pat::Ident`, so it is not flagged. There is no `.stream_ref(` and no `.tenant` field read.
- `multitenancy-audit.sh`: the moved code contains no `stream_hash(`, `registry.get("`, `acct_local` or `streams-internal-*` text, so the baseline is unchanged.

### Commit 2: "The telemetry-append receiver reads its body only after authentication, under the configured limit"

**1. `src/http/telemetry_append.rs`:**

```diff
 };
+use crate::application::append::{AppendCode, FailureClass, fail};
+use axum::body::Body;
 use axum::extract::{Path, State};
 use axum::http::{HeaderMap, StatusCode};
 use axum::response::Response;
-use bytes::Bytes;
 use std::sync::Arc;
@@
-    body: Bytes,
+    body: Body,
 ) -> Response {
@@
     let sref = crate::tenant::TenantStreamRef::new(crate::tenant::system_project(), canonical);
+    // R25-E: authenticate before reading a byte. The body is read only
+    // after every header-level refusal, under the configured limit the
+    // local append path enforces and the senders budget their batches
+    // against — never axum's implicit 2 MiB for a `Bytes` extractor.
+    let limit = state.config.cli.max_request_body_bytes;
+    let Ok(body) = axum::body::to_bytes(body, limit).await else {
+        return render_append(fail(FailureClass::Invalid, AppendCode::TooLarge, "body too large"));
+    };
     // The same typed local path the sender took: a refusal here is the
```

- The function grows to about 55 lines, still under 100.
- It takes 4 arguments.
- Nesting depth is 1.
- There are no bool parameters.
- `to_bytes` is not in `disallowed-methods`.
- The `Err` arm answers exactly like the raw PUT intake (http.rs:2236-2241) and `buffer_body_charged` → `TooLarge` (http.rs:2785-2794), where a transport error is also answered 413. The error is not swallowed more than on those paths: the answer goes to a client whose upload failed.

**2. `src/dst/tests/security_workload.rs`:** the section, the two helpers and the two tests from §3.

**3. `docs/refactor/test-inventory.json`:** `python3 scripts/test-inventory.py --write` goes from 510 to 512 entries. The new tests are not pinned in `review-mechanisms.json` (0 `security_workload` rows there).

**4. `docs/refactor/WIRE-MATRIX.md:225`:** replace the row's opening with:

> `POST /v1/internal/telemetry-append/{*name}` (`src/http/telemetry_append.rs`, `src/billing/system_append.rs::append_local`) — fleet credential or workload JWT with op `telemetry-append`, decided before the body is read (R25-E; 401 otherwise); reserved `_`-streams only (403 `not_system_stream`); 400 `missing_key` (...), `invalid_name` (...); the body is then read under `MAX_REQUEST_BODY_BYTES` — the local append path's limit, which the senders' batches are budgeted against — and over it is 413 `too_large` (the append contract's buffering refusal, no drain); then the same TYPED local path ...

Keep the rest of the row as it is.

**Ratcheted scopes touched in commit 2:** none. The new file has no exceptions. `render_append`'s `#[expect(unwrap_used)]` fingerprints only facts inside its own body, and a new caller elsewhere does not enter that scope. `append_typed`'s `#[expect(too_many_arguments, too_many_lines, excessive_nesting)]` is untouched. The test file has no exceptions.

---

## 5. Mutation analysis (cargo-mutants 27.1.0, `--in-diff` against 729c52ac)

**Selected owners:**
- `http` (`src/http.rs`, changed). Filters are unchanged: `http:: livefeed_engine_retired security_workload:: debug_store_reports_this_runtimes_shard_opens`.
- The new `http_telemetry_append` (`src/http/telemetry_append.rs`). Filters: `security_workload:: reserved_streams_append`.

`src/http` is a `BUFFER_PREFIXES` path, so the plan also selects miri, as any http edit does. Loom and properties are not selected.

**`src/http.rs`: expect 0 mutants.**
- The inserted lines are two module-level items (`mod` and `use`); no function span covers them.
- At the deletion, the adjacent context lines are: line 1218 `}`, which closes `debug_usage_reconcile` (its FnValue span ends at the last body statement, line 1217); a blank line; and the `/// #269` doc line of `serve_h1`. None of these lies inside a mutant span.
- `router` is not touched.
- This matters because `debug_usage_reconcile` has **no** test anywhere, so if its FnValue mutant were selected it would be MISSED. The control in §7 checks this listing explicitly. If it ever shows an `http.rs` mutant, keep one extra blank line so the hunk boundary moves; do not add a test for an unrelated route.

**`src/http/telemetry_append.rs`:** every line is inserted, so all of the function's mutants are selected. There are three. The fix's `let … else` block and the `to_bytes`/`fail` calls add no operator mutants: cargo-mutants does not mutate `let` patterns, call arguments or match arms when there is no wildcard arm.

| Mutant | Killed by (all inside `security_workload::`) |
|---|---|
| `replace internal_telemetry_append -> Response with Default::default()` (200, empty) | new test 1 (`(200,0)` ≠ `(401,0)`); new test 2 (200 ≠ 204); `telemetry_append_receiver_reports_ownership_not_absence` (200 ≠ 409) |
| `delete ! in internal_telemetry_append` (auth guard) | new test 1: the unauthenticated upload is now read (3 MiB polled; `invalid_json` 400), so the assertion fails. Test 2 and the receiver test: the authorized request gets 401 |
| `delete ! in internal_telemetry_append` (`is_reserved_stream` guard) | new test 2 (403 ≠ 204); receiver test (403 ≠ 409) |

- If commit 1 is pushed alone, the same three mutants are killed by the existing receiver test.
- There is no timeout risk: every mutated request resolves in milliseconds, with a status that makes the assertion fail.
- `src/dst/**` is `#[cfg(test)]` and matches neither a critical prefix nor a registered owner, so it is not selected.
- **Owner-table change:** one new row (§4, commit 1). `test_mutation_owners.py` checks that names and sources are unique and that the literal table parses the same as its runtime mapping; both hold.

---

## 6. Ledgers

| Ledger | Commit | Change |
|---|---|---|
| `scripts/quality/mutation_owners.py` | 1 | `+owner('http_telemetry_append', 'src/http/telemetry_append.rs', 'security_workload:: reserved_streams_append')` |
| `docs/refactor/WIRE-MATRIX.md` | 1, 2 | path reference in 1; body order and the 413 `too_large` contract in 2 |
| `docs/refactor/test-inventory.json` | 2 | `--write`, +2 entries (510 → 512) |
| `docs/quality/owners.json` | — | none: explicit imports, so no glob; no statics or macro-dsl in the moved code; no `#[path]` |
| `docs/quality/source-allowances.json` | — | none vacated (the moved function had no rows) |
| `docs/refactor/architecture-policy.json` | — | none (no reverse edges, within budgets) |
| `docs/refactor/review-mechanisms.json` | — | none (no pinned `security_workload` tests) |
| `src/dst/tests/README.md`, scenario map/dispositions | — | none (existing module, no renames) |
| `scripts/mt-audit-baseline.txt` | — | none (no fingerprinted text moved) |

---

## 7. Controls

After commit 1:

```
wc -l src/http.rs src/http/telemetry_append.rs        # 3314 src/http.rs ; ~64 src/http/telemetry_append.rs
diff <(git show HEAD~1:src/http.rs | sed -n '1220,1268p') \
     <(sed -n '/^\/\/\/ Fleet-internal telemetry append/,/^}$/p' src/http/telemetry_append.rs)
                                                      # exactly one hunk: `async fn` -> `pub(super) async fn`
cargo test --locked --lib -- security_workload:: reserved_streams_append
                                                      # test result: ok. 12 passed; 0 failed
python3 -m unittest scripts/quality/test_mutation_owners.py   # OK
python3 scripts/architecture-gate.py --check          # architecture-gate: OK (385 Rust files; fixed baseline a7e2070f3b43)
bash scripts/multitenancy-audit.sh                    # MT_AUDIT_OK
```

**Red.** Add the §3 tests on top of commit 1, before the fix:

```
cargo test --locked --lib -- security_workload::telemetry_append_
# telemetry_append_receiver_reports_ownership_not_absence ... ok
# telemetry_append_refuses_an_unauthenticated_body_unread ... FAILED
#   assertion `left == right` failed: an unauthenticated telemetry append is refused before its body is read: Failed to buffer the request body: length limit exceeded
#     left: (413, 2162688)
#    right: (401, 0)
# telemetry_append_reads_to_the_configured_limit_after_authentication ... FAILED
#   assertion `left == right` failed: an authorized batch within the configured limit lands: Failed to buffer the request body: length limit exceeded
#     left: 413
#    right: 204
# test result: FAILED. 1 passed; 2 failed
```

**Green, after commit 2:**

```
cargo test --locked --lib -- security_workload::telemetry_append_     # test result: ok. 3 passed; 0 failed
cargo test --locked --lib -- security_workload:: reserved_streams_append   # test result: ok. 14 passed; 0 failed
python3 scripts/test-inventory.py --write && python3 scripts/test-inventory.py --check
                                                      # test-inventory: wrote 512 tests / test-inventory: OK (512 tests, 0 ignored)
cargo fmt --all -- --check                            # clean
cargo clippy --locked --workspace --all-targets -- -D warnings           # clean
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items   # clean
cargo test --locked --release --lib multitenancy_identity_lint           # ok. 1 passed
QUALITY_OUT=target/q52 python3 scripts/quality/verification_plan.py --out target/q52-plan
    # mutation_source_files ["src/http.rs","src/http/telemetry_append.rs"];
    # selected owners include http + http_telemetry_append; unregistered []; miri true, loom false
cargo mutants --list --in-diff target/q52-plan/pr.diff -f src/http.rs -f src/http/telemetry_append.rs
    # exactly 3 lines, all src/http/telemetry_append.rs: 1 FnValue + 2 "delete !"; none in src/http.rs
QUALITY_MUTANTS_OUT=target/q52-mut scripts/quality/mutations.sh
    # http_telemetry_append: 3 caught, 0 missed, 0 timeout; http: no mutants selected (reported explicitly)
scripts/quality.sh                                    # QUALITY_OK
```

Run `mutations.sh` and the gate only when the tree is otherwise idle. CPU contention causes mutant timeouts.

---

## 8. Out of scope (recommended follow-ups)

1. **`internal_sweep_segment`** (`src/product.rs:3332-3344`) has the same `body: Bytes` before `fleet_operation_authorized`. That lets unauthenticated callers make the server buffer up to 2 MiB. The reviewer excluded it. `product.rs` is at its 4,205 ceiling, and the fix is the same pattern plus a verbatim move. Recommended as its own item.
2. **Double copy.** `append_typed` re-buffers the `Body::from(Bytes)` it is handed (`buffer_body_charged` copies). A single-frame fast path in `buffer_body_charged` would remove that copy for every in-process caller: the receiver, `append_local` and the `system_append` local path. It is a hot-path edit to ceilinged `http.rs` (BUFFER prefix, so miri and mutation apply). The impact is low: relay bodies are usually ≤ 1 MB.
3. **`_usage` drain step 1 has no byte budget** (`billing.rs:787-822`). It takes up to 64 spooled read batches of about 1 MiB (estimated) each, which can exceed even `MAX_REQUEST_BODY_BYTES` on the local path and wedge `_usage` with 413 `too_large`. This needs measurement and its own item.
4. **Memory shed before buffering.** The receiver buffers before `append_typed`'s `check_memory`; today axum buffers even earlier. Concurrency is bounded by the number of peers times four ledgers.
5. **Drain before the over-limit 413.** `append_typed` drains up to 8 MiB for a declared-oversize body; the receiver does not. Its only client reports a 413 and a reset the same way.
6. The stale `src/http.rs:NNN` line references in `docs/MULTITENANCY-MAP.md` are a historical map; leave them.

---

## 9. Decisions for Søren

1. **How much the fleet-internal receiver accepts.** Recommended: the configured `MAX_REQUEST_BODY_BYTES` (32 MiB by default), the same as the local append path and the senders' batch budgets. It is the only limit that closes the relay-only wedge.
   - This grants no new capability: the static fleet credential can already POST 32 MiB to `/v1/stream/*`. A workload JWT scoped only to `telemetry-append` can now send 32 MiB (instead of 2 MiB) to reserved streams.
   - Backward-compatible alternative: `to_bytes(body, 2 MiB)` after authentication. That still fixes the R25-E ordering (401 before any read) but keeps the 2 MiB sender/receiver mismatch.
   - No product/raw-edge, metric or debug-JSON contract changes either way. The internal wire changes are listed in §2 and recorded in WIRE-MATRIX.

---

## Skeptic corrections (C1..C7)

I checked every claim against the tree at 2fb92fb9. These hold:
- **The defect is still there.** It is at `src/http.rs:1220-1268`: `body: Bytes` at :1227, auth at :1229. Rank 21 (8dabca7f) changed only the dispatch into `billing::append_local` (:1260).
- **axum-core 0.5.6.** `Bytes::from_request` is `into_limited_body()`, capped at `DEFAULT_LIMIT = 2_097_152` (`ext_traits/request.rs:319-326`). `Body::from_request` is `req.into_body()`, which applies no limit.
- **The rejection text.** It is `Failed to buffer the request body: length limit exceeded` (`rejection.rs` + `macros.rs:134-139` + http-body-util 0.1.3 `LengthLimitError`).
- **http-body-util 0.1.3 `Limited::poll_frame`.** It raises the error when `data.remaining() > remaining`. That makes the red count exactly 33 × 65,536 = 2,162,688.
- **The test 2 batch.** It is exactly 2,702,251 bytes; I recomputed it.
- **No body-limit layer.** `DefaultBodyLimit`/`RequestBodyLimit` are absent from `src/`, and `router` adds only `track_inflight` plus the origin `map_response` (`src/http.rs:1629-1643`).
- **The in-process harness.** `hyper::service` is public (hyper 1.10.1 `lib.rs:125`). `TowerToHyperService: hyper::service::Service<R>` with `Error = S::Error = Infallible` (hyper-util 0.1.20 `service/glue.rs:33-45`). `router` has no construction side effects. Both tests are bounded and have no wait loops.
- **Line budget.** `wc -l` matches the table. `src/http.rs` 3,362 → 3,314: delete 1220-1269 (50 lines) and add 2. The new file stays ≤ 1,000 lines, and `security_workload.rs` goes from 763 to ~860.
- **In-diff scope.**
  - The deletion's affected lines are 1218/1219 and 1219/1220 (cargo-mutants 27.1.0 `in_diff.rs:213-256`, before and after the deletion).
  - `debug_usage_reconcile`'s FnValue span ends at its tail `match` (:1217; `visit.rs:757-762` spans first..last stmt), so it is not selected.
  - The tail `mod`/`use` inserts sit outside every fn span.
  - That leaves exactly 3 mutants in the new file (FnValue plus two `delete !`), each killed as §5 traces.
- **Ratchets.** The moved fn has no `#[expect]`, macros, statics or effect paths. `router`'s `too_many_lines`/`disallowed_methods` scope is byte-identical. No module-wide `#![expect]` exists in `http.rs`. `render_append`'s `unwrap_used` scope does not contain the new caller. No `source-allowances.json` or `owners.json` row names the function.
- **The other gates are clean.**
  - `mt_lint`: `Path(name)` is `Pat::TupleStruct` (`src/mt_lint.rs:163`). There is no `.stream_ref(`, `.tenant` or `deployment_tenant` use.
  - MT-audit patterns: none match.
  - Architecture gate: the explicit `use super::{..}` names no `http`/`product` segment, so there are no reverse edges.
- **Docs.** No doc outside WIRE-MATRIX states the receiver's limit or ordering.

**C1 — The merge base moved, so correct §0 and §5.** `origin/slate` is now `2fb92fb9` = HEAD, 0 commits ahead; items 64/86 were pushed. The ceilings are unchanged (557e8ba6/2fb92fb9 touched only `sse/*`, `sse_delivery.rs`, `test-inventory.json` and `mutation_owners.py`). But every "against 729c52ac" and "2 ahead" statement is stale. A push to `slate` compares against the event's `before` revision (docs/RUST-QUALITY.md §Verification). If commit 1 is pushed alone, its range selects the whole new file (the 3 mutants, killed by existing tests, as §5 already says). Push commits 1+2 together so the red tests are in the mutation filter's view.

**C2 — The control's expected plan receipt is wrong: loom, properties_fuzz and miri are all `true`.** Commit 1 edits `scripts/quality/mutation_owners.py`. That sets `tooling` (`scripts/quality/verification_plan.py:77-79`), which forces `compiler`, `properties_fuzz`, `loom` and `miri` all `true` (`:98-99`). §7's "miri true, loom false" is wrong. Budget the extra Loom and property legs on CI. Nothing fails because of this; it only costs wall time (CI runs about 4× slower than local).

**C3 — Unbuildable control: `python3 -m unittest scripts/quality/test_mutation_owners.py`.** `scripts/quality` has no `__init__.py`. Given a file path, unittest imports it as `scripts.quality.test_mutation_owners`, and its `from mutation_driver import mutation_command` then raises `ModuleNotFoundError`. Use the gate's own form, `python3 -m unittest discover -s scripts/quality -p test_mutation_owners.py` (`scripts/quality.sh:16`), run from the repo root. `test_prior_table_parser_recovers_every_current_owner_without_execution` reads a cwd-relative path.

**C4 — Fix the expected `mutations.sh` output text.**
- The driver prints `http: no executable mutants in the selected scope` (`scripts/quality/mutation_driver.py:143`). It then prints `Mutation verification executed 3 selected mutant(s) across 2 registered owner(s).` (:152-154).
- The caught/missed/timeout split is in cargo-mutants' output under `target/q52-mut/http_telemetry_append/` (`mutants.out/outcomes.json`), not on the driver's stdout.
- `QUALITY_OUT=target/q52` on the `verification_plan.py` line has no effect; only `--out` matters.

**C5 — The use-site table (§1.5) is incomplete.** It misses a test caller of `system_append`: `src/dst/tests/billing_controller.rs:471`. It takes the local path with a small body and is not affected. Add it for completeness. It does not change the pinning set.

**C6 — The §9 decision should cite the fleet-wide invariant and state the memory cost of the chosen limit.**
- **(a) Cite the invariant.** `MAX_REQUEST_BODY_BYTES` is namespace-pinned. Bootstrap refuses to start when it differs from the topology's stored ceiling (`src/bootstrap.rs:335-361`; `registry.rs:1385`). So "the receiver reads under its own configured limit" is provably the same limit every sender budgets against (`audit.rs:280`, `ops.rs:305`). That is the strongest argument for the recommended option; say so.
- **(b) State the memory cost.** The new `to_bytes` buffer is neither charged to project buffered-body pressure nor preceded by the RSS write-shed. `append_typed` runs `check_memory` (`src/http.rs:2784`) only after the receiver has already buffered, and `buffer_body_charged` then copies it again. Under the recommended limit an authenticated relay can therefore hold up to 2 × 32 MiB on an owner that is already shedding, before it gets its 503. Today the figure is 2 × 2 MiB.
  - List this in §9 as part of the decision.
  - Option: call `state.append_service().check_memory().await` before `to_bytes` and render its `Err` with `render_append(Err(e))`. This is a behaviour change, so it needs its own red test under a forced memory shed.
  - Otherwise accept it explicitly: callers are fleet-credentialed, at most peers × 4 ledgers.
  - Do not leave it only in §8.4.

**C7 — Minor.**
- **Owner-row placement.** `mutation_owners.py` line 88 is still `http_read`, so the "after line 88" placement holds.
- **Imports in the new file.** Commit 2 may import `AppendCode, FailureClass, fail` through `super::` (http.rs:4 already imports them) or through `crate::application::append` as written. Either passes the architecture and source gates, because `src/http/**` is not a hard owner.
- **Counts.** The inventory count claim (510 → 512, 0 ignored) is correct.
- **Filter results.** Filter `security_workload:: reserved_streams_append` selects 12 tests today and 14 after.

**Verdict: ready-with-corrections.** The defect is real on HEAD, and both red tests fail today with exactly the output traced. The move is verbatim and ratchet-neutral, and the mutation scope is 3 killable mutants with no timeout risk. The fixes: the control texts (C2-C4) and the stale merge base (C1), plus a stated memory-posture decision (C6b) before landing.
