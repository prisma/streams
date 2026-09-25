# Chain 45 -> 29: one ResolveError transport mapping, then a typed fork read

Tree: `slate` @ `0afa2597` (= origin/slate). Work lands directly on slate, four commits:

| # | Commit | Kind |
|---|---|---|
| C1 | A read's shard refusal renders through the one resolve mapping (item 45) | wire (code rename on one path) |
| C2 | The read-request mutation owner runs the tests that exercise it | tooling (latent gate defect) |
| C3 | A fork read keeps its shard refusals typed: opening is retryable, an ancestor is never a redirect, a catalog fault is not "gone" (item 29) | wire |
| C4 | Fork reads and fork creation adopt as customer traffic (item 29, own commit) | custody, not wire-visible |

No verbatim-move commit is needed. No ceilinged file is touched (§4.0), and `src/application/read.rs` stays under 1,000 lines (938 -> 983 -> 986). §4.5 gives a contingency move if rustfmt pushes it over.

---

## 1. Problem (verified on 0afa2597)

### 1.1 Item 45: two ResolveError transport mappings (confirmed)

`src/http.rs:563-601` is documented as the one mapping:

```rust
/// The ONE transport mapping of a shard-resolution refusal (PR 6-A):
/// not-owner → 409 + Streams-Replay-To (so a stale router corrects
/// itself), opening → retryable 503 + Retry-After, open failure → 500.
pub(crate) fn resolve_error_response(e: crate::shard_directory::ResolveError) -> Response {
    ...
                "shard not currently serving here; retry",                       // :589
            if let Ok(v) = axum::http::HeaderValue::from_str(&retry_after_secs.to_string()) { // :591
        ResolveError::OpenFailed { prefix, error } => err_resp(
            StatusCode::INTERNAL_SERVER_ERROR,
            "shard_open",                                                         // :598
            &format!("open shard {prefix}: {error}"),
```

The raw read adapter keeps a private copy of it (`src/http/read.rs:11`, `:58-100`):

```rust
        E::Resolve(error) => resolve_response(error),                              // :11
#[expect(
    clippy::unwrap_used,
    reason = "resolve_response; a retry-after delay renders as decimal digits, ..." // :58-61
)]
fn resolve_response(error: crate::shard_directory::ResolveError) -> Response {    // :62
                "shard is opening; retry",                                         // :86
                axum::http::HeaderValue::from_str(&retry_after_secs.to_string()).unwrap(), // :90
            "shard_open_failed",                                                   // :96
            &error,                                                                // :97
```

Two things have drifted. The OpenFailed wire code is `shard_open_failed` here and `shard_open` on every other surface (http.rs:598, creation.rs:135, consumer.rs:97, append/contract.rs:101). The OpenFailed and Opening messages also differ. The unwrap is harmless (decimal digits are a valid header value), but it is an unwrap under a function-wide expect. Nothing in the repo reads `shard_open_failed`: `git grep` finds only read.rs:96. WIRE-MATRIX:170 already calls `shard_open` "the shared `resolve_error_response` envelope".

### 1.2 Item 29: the fork read core stringifies resolution (confirmed, with one correction)

`src/application/read.rs:381-397` (`ReadService::handle_of`):

```rust
            .resolve(&ro.shard_route, crate::shard_directory::Adoption::Internal)  // :388
            .await
            .map_err(|e| format!("engine unavailable: {e:?}"))?;                   // :390
```

Callers relabel that string as Storage, which is 500:

- `src/application/read_request.rs:391`: `let (_, handle) = self.handle_of(desc).await.map_err(ReadFailure::Storage)?;` (the fork's own shard)
- `src/application/read_request.rs:441-443`: `.read_stitched(..).await.map_err(ReadFailure::Storage)?;` (the ancestors, via `read_stitched` -> `state.handle_of(d).await?` at read.rs:518)
- `src/http/read.rs:50`: `E::Storage(message) => err_resp(StatusCode::INTERNAL_SERVER_ERROR, "internal", &message),`
- `src/product.rs:2972`: `E::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal", "read failed", true, None)`
- Fork creation, `src/application/creation/fork.rs:207-210`: `Err(m) => return Err(CreationError::new(CreationFailure::Storage, "internal", &m)),` The same happens at `:272-275` for the sub-offset record read. That path goes through `http.rs:2664` `F::Storage => StatusCode::INTERNAL_SERVER_ERROR`.

The raw GET path of a fork is `read_inner` -> `respond_read` -> `execute_read` -> `execute_fork_read` -> `handle_of`, and nothing earlier resolves an engine (renew_ttl is a no-op without a TTL). So:

- A fork read whose own shard, or any ancestor's shard, is opening, moving or closing answers **500 `internal`**, with body message `engine unavailable: Opening { prefix: "00", code: "shard_moving", retry_after_secs: 2 }`. It should be 503 with a Retry-After.
- If the fork's own shard belongs to another instance (a stale router), the read answers 500 with no `Streams-Replay-To`, so the router never corrects itself. A non-fork read in the same position answers 409 plus the header (read_request.rs:237-260).
- An ancestor another instance owns also answers 500 with no header. That is correct as far as it goes: the request belongs to the fork's owner, so redirecting to the ancestor's owner would ping-pong. The naive typed fix of routing everything through `ReadFailure::Resolve` would add that wrong header. The typed version must keep "no redirect".

`src/application/read.rs:436-439` (`fork_chain_of`):

```rust
            let d = match state_reg.get(&src_ref).await {
                Ok(Some(d)) if !d.deleted => d,
                _ => return Err(format!("fork source '{src}' is gone")),
            };
```

The `_` arm folds a registry `Err` into "gone". **Correction to the review:** the status is already 500 `internal` either way. Only the message lies ("gone" for a catalog I/O fault). Nothing decides on it, so the fix is an honest split, not a new wire status.

`handle_of` resolves with `Adoption::Internal`. Every production caller is customer traffic: `execute_fork_read`, `read_stitched` (raw and product reads, SSE `SingleSource::read_batch` at sse/source.rs:59, fork creation) and `validate_boundary`. `Adoption::External` is documented as "stamps the adoption sequence, revoking any sweep custody so the scheduler cannot close the engine under it" (shard_directory.rs:18-21). Today a fork read never stamps, so the R29 sweep may close an engine a fork reader holds.

The review's list of mapper drifts is correct, but they are out of scope here (§8). The product renderer maps `E::Resolve(_) | E::Remote(_)` to 503 `temporarily_unavailable` with no Retry-After (product.rs:2965). `ConsumerFailure::ownership` drops `retry_after_secs` (consumer.rs:88).

### 1.3 Additional findings while verifying

1. **The read_request mutation owner runs zero tests.** The row is `owner('read_request', 'src/application/read_request.rs', 'application::read_request::')` (scripts/quality/mutation_owners.py:86). But the file is compiled as `#[path = "read_request.rs"] mod request;` inside `application::read` (read.rs:724-725), and it contains no tests. No test path contains `application::read_request::`, so every viable mutant in that file is reported MISSED. It has never fired because read_request.rs has not changed since the row landed (99d5c098). C2 fixes it before C3 touches the file.
2. **The src/http/read.rs module path.** The file is `#[path = "http/read.rs"] mod read_adapter;` (http.rs:3359-3360), so its tests would be `http::read_adapter::`. The `http_read` filter's `http::read::` term matches nothing either. Its DST terms carry it (see §5), so this plan leaves the row alone.
3. **Cross-owner fork chains are unsupported.** A fork routes by its own name (registry.rs:745, `RouteHash::for_stream(&self.sref())`), so in a multi-instance ring the source is usually owned elsewhere. Reading or creating such a fork is 500 today and stays 500 after this chain. The chain only makes the refusal honest and non-redirecting (§8).
4. The "different incarnation" message at read.rs:442 contains a 30-space run (a line continuation missing its `\`). It is cosmetic and left alone.

---

## 2. Contract decision

### 2.1 Typed contract

A new `ForkReadError`, owned by `src/application/read.rs`:

```rust
pub(crate) enum ForkReadError {
    Own(ResolveError),      // the read stream's own shard: the request's routing target
    Ancestor(ResolveError), // an ancestor's shard: never a redirect target
    Storage(String),        // catalog, chain integrity, hop storage (all 500 on every surface)
}
```

This deliberately departs from the review. There are no `Registry` or `AncestorGone` variants. No consumer decides differently on them: they are 500 `internal` on the raw, product and create surfaces alike. A variant rendered identically would be a gratuitous abstraction. The registry fix is an honest split of the match arm (§4.3). If Søren picks 503 for catalog faults (decision D6's alternative), a `Registry` variant is added then.

There is one mapping per consumer, and each is exhaustive with no `_` arm and no string matching:

| ForkReadError | ReadFailure (raw/product read) | CreationError (fork create; the child is the routing target) |
|---|---|---|
| `Own(NotOwner)` | `Resolve(NotOwner)`: 409 + Streams-Replay-To | Storage `internal` 500, no owner |
| `Own(Opening/OpenFailed)` | `Resolve(..)`: 503 + Retry-After / 500 `shard_open` | `CreationError::refused(..)`: 503 + Retry-After / 500 `shard_open` |
| `Ancestor(NotOwner)` | `Storage("fork ancestor shard P is served by O; this instance cannot stitch it")`: 500 `internal`, **no** Streams-Replay-To | Storage `internal` 500, no owner |
| `Ancestor(Opening/OpenFailed)` | `Resolve(..)`: 503 + Retry-After / 500 `shard_open` | as `Own` |
| `Storage(m)` | `Storage(m)`: 500 `internal` | Storage `internal` 500 |

`handle_of` tags its refusal `Own`, meaning the named stream's own shard. `read_stitched` re-tags the ancestor hop with `ForkReadError::at_ancestor`. For fork creation, every refusal on the source chain is non-routing, so its mapping ignores the tag.

### 2.2 What changes at the edge (all need Søren's approval; see D1-D7 in the summary)

| Surface / condition | Today | After | Backward-compatible alternative |
|---|---|---|---|
| Raw GET/HEAD, internal segment-read, raw SSE connect refusal: shard open failed | 500 `shard_open_failed`, msg `<error>` | 500 `shard_open`, msg `open shard <prefix>: <error>` | Keep the private copy (the status quo drift), or rename every other surface to `shard_open_failed` (4 surfaces). Recommend accepting: no reader of the code exists. |
| Same paths: shard opening | 503 `<code>` + Retry-After, msg `shard is opening; retry` | same status, code and header; msg `shard not currently serving here; retry` | none needed |
| Raw fork GET/HEAD: own or ancestor shard opening/moving/closing | 500 `internal` | 503 `shard_opening`/`shard_moving`/`shard_closing` + Retry-After | keep 500 (clients retry 5xx anyway, but lose Retry-After and see a fault that is transient) |
| Product fork read, same condition | 500 `internal` (retryable) | 503 `temporarily_unavailable` (retryable; still no Retry-After, see §8) | keep 500 |
| Raw/product fork read: fork's OWN shard owned elsewhere (stale router) | 500 `internal` | 409 `not_ring_owner` + `Streams-Replay-To: <owner>` | keep 500 (the router never converges on forks) |
| Raw/product fork read and raw fork PUT: ANCESTOR/source shard owned elsewhere | 500 `internal`, msg `engine unavailable: NotOwner {..}` | 500 `internal`, **no** Streams-Replay-To, msg names the shard and owner | 503 retryable (rejected: retrying cannot help until the ring moves) |
| Raw fork GET: ancestor or own shard failed to open | 500 `internal` | 500 `shard_open` | keep `internal` |
| Raw fork PUT: source shard opening | 500 `internal` | 503 `<code>` + Retry-After | keep 500 |
| Raw fork PUT: source shard failed to open | 500 `internal` | 500 `shard_open` | keep `internal` |
| Fork read: catalog read of an ancestor fails | 500 `internal`, msg `fork source 'x' is gone` | 500 `internal`, msg `fork source 'x' could not be read: <error>` | none; the 500/503 split for catalog faults stays a separate decision (item 45's note) |
| Fork read/create: custody (C4, not wire-visible) | resolves Internal: no adoption stamp | resolves External: stamps, revokes sweep custody | keep Internal (the sweep may close an engine under a fork reader) |

---

## 3. Red tests

All DST tests go in a **new** file, `src/dst/tests/fork_read_refusals.rs` ("beside fork_lifecycle.rs"). `fork_lifecycle.rs` is 976 lines, so it has only 24 lines of headroom under the 1,000-line rule.

**Shard-not-serving mechanism.** The review suggested the slow-open directory pattern. This plan uses the retirement holdoff instead. The rig's `open_wait` is the config default of 10 s (`config/model.rs:378`, used by `fixture_http.rs:487`), and `open_park` parks every open in the rig. A parked open would therefore make each assertion wait 10 s, and it could not target one prefix. `shards.retire(prefix, RetirementReason::Shutdown, ..)` arms `HOLDOFF_BASE` (3 s, sharddir.rs:49). During that window, `get_or_open` -> `wait_retired` answers `OpenOutcome::Wait { code: "shard_moving", retry_after_secs: (until-now).as_secs().max(1) }` at once (sharddir.rs:685-691), and only for that prefix. `clear_holdoff` ends it. Both waits are bounded.

**Placement.** The default rig's prefixes are `["00"]`. `prefix_for` gives `"00"` for routes whose bits start with 00 and `""` for everything else (registry.rs:1489-1497), so a source and a fork can be put in different shards by probing names. This is the same probing the existing tests use (runtime_sweep.rs:59-66, watch_observation.rs:62).

Each prediction below is the first failing assertion when the test runs against the tree just before the commit that fixes it. C1 does not change `Storage` rendering, so the outputs are the same on 0afa2597. `retry_after_secs: 2` is the 3 s holdoff less the milliseconds since the retire. It would be 1 only if the rig stalled for more than 1 s.

### R1 (C1): `http::tests::a_read_refusal_renders_through_the_one_resolve_mapping`, in `src/http/tests.rs`

```rust
/// Review item 45: a read renders a shard-resolution refusal through the ONE
/// transport mapping; the read adapter kept a private copy that drifted
/// (`shard_open_failed`, another Opening message, an unwrap on Retry-After).
#[tokio::test]
async fn a_read_refusal_renders_through_the_one_resolve_mapping() {
    use crate::shard_directory::ResolveError;
    for refusal in [
        ResolveError::OpenFailed { prefix: "10".into(), error: "scripted open failure".into() },
        ResolveError::Opening { prefix: "10".into(), code: "shard_opening", retry_after_secs: 3 },
        ResolveError::NotOwner { prefix: "10".into(), owner: "streams-2".into() },
    ] {
        let read = super::read_adapter::read_failure_response(
            crate::application::read::ReadFailure::Resolve(refusal.clone()),
        );
        let shared = resolve_error_response(refusal.clone());
        assert_eq!(read.status(), shared.status(), "{refusal:?}");
        assert_eq!(read.headers(), shared.headers(), "{refusal:?}");
        let read = axum::body::to_bytes(read.into_body(), usize::MAX).await.unwrap();
        let shared = axum::body::to_bytes(shared.into_body(), usize::MAX).await.unwrap();
        assert_eq!(String::from_utf8_lossy(&read), String::from_utf8_lossy(&shared), "{refusal:?}");
    }
}
```

The test goes above the trailing `use super::*;`. Exact red output on 0afa2597 (status 500 = 500 and headers equal, so the body assert fires first):

```
---- http::tests::a_read_refusal_renders_through_the_one_resolve_mapping stdout ----
thread 'http::tests::a_read_refusal_renders_through_the_one_resolve_mapping' panicked at src/http/tests.rs:<line>:9:
assertion `left == right` failed: OpenFailed { prefix: "10", error: "scripted open failure" }
  left: "{\"error\":{\"code\":\"shard_open_failed\",\"message\":\"scripted open failure\"}}"
 right: "{\"error\":{\"code\":\"shard_open\",\"message\":\"open shard 10: scripted open failure\"}}"
test result: FAILED. 0 passed; 1 failed
```

### DST helpers (top of `src/dst/tests/fork_read_refusals.rs`)

```rust
//! Fork read refusals (review item 29): a stitched read or a fork creation
//! meets two kinds of shard, the named stream's own, which is where the
//! request belongs, and an ancestor's, which never is.

use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig, http_rig_build};
use super::fixture_requests::{PRISMA_KEY, hreq, preq};
use super::fixture_runtime::RigRuntime;
use super::fixture_storage::mem;

const JSON: [(&str, &str); 1] = [("content-type", "application/json")];

/// Layout-4 routes are project-qualified, so placement is probed, never
/// hardcoded: the first name of `family` in shard `prefix` of the default
/// rig ("00", or "" for every route outside it).
fn name_in(state: &crate::http::AppState, family: &str, prefix: &str) -> String {
    (0..256)
        .map(|i| format!("{family}-{i}"))
        .find(|name| {
            let route =
                crate::crypto::RouteHash::for_stream(&state.deployment.raw_adapter_sref(name));
            state.shards.prefix_for(&route.0) == prefix
        })
        .expect("a name in the wanted shard")
}

/// A fork at the tail of a three-record source: its first read must hop
/// into the source's shard.
async fn source_and_fork(addr: std::net::SocketAddr, source: &str, fork: &str) {
    let path = format!("/v1/stream/{source}");
    let (st, _, b) = hreq(addr, "PUT", &path, &JSON, br#"[{"n":0},{"n":1},{"n":2}]"#).await;
    assert!(st == 200 || st == 201, "{}", String::from_utf8_lossy(&b));
    let headers = [("content-type", "application/json"), ("stream-forked-from", source)];
    let (st, _, b) = hreq(addr, "PUT", &format!("/v1/stream/{fork}"), &headers, b"").await;
    assert_eq!(st, 201, "{}", String::from_utf8_lossy(&b));
}

/// A shutdown retirement arms the base holdoff (3 s): every resolution of
/// `prefix` is then the retryable `shard_moving` refusal, and only of it.
fn hold_off(state: &crate::http::AppState, prefix: &str) {
    let outcome =
        state.shards.retire(prefix, crate::shard_directory::RetirementReason::Shutdown, |_, _| true);
    assert!(
        matches!(outcome, crate::shard_directory::RetireOutcome::Retired(_)),
        "shard {prefix:?} was resident"
    );
}

/// What an opening shard owes a client: 503, its code, a Retry-After, and
/// no redirect.
fn assert_opening(st: u16, headers: &std::collections::HashMap<String, String>, body: &[u8]) {
    assert_eq!(st, 503, "{}", String::from_utf8_lossy(body));
    let envelope: serde_json::Value = serde_json::from_slice(body).unwrap();
    assert_eq!(envelope["error"]["code"], "shard_moving", "{envelope}");
    let retry: u64 = headers
        .get("retry-after")
        .expect("an opening shard owes a Retry-After")
        .parse()
        .unwrap();
    assert!((1..=3).contains(&retry), "retry-after {retry}");
    assert!(!headers.contains_key("streams-replay-to"), "{headers:?}");
}
```

The file needs no `json!`, no spawn and no `#[expect]`. Every function stays under 100 lines with nesting of at most 2.

### R2 (C3): `dst::dst_tests::fork_read_refusals::a_fork_read_while_its_own_shard_opens_is_retryable`

Setup: the source goes in `""` (`name_in(&state, "own-src", "")`) and the fork in `"00"` (`name_in(&state, "own-fork", "00")`). Call `source_and_fork` then `hold_off(&state, "00")`. The steps:

1. GET the fork on the raw route and apply `assert_opening`.
2. GET `/v1/streams/{fork}/records` with `prisma-encryption-key: PRISMA_KEY`, and assert 503, `error.code == "temporarily_unavailable"` and `error.retryable == true`. This is the product edge, decision D2.
3. `clear_holdoff("00")`, GET again and assert 200 with 3 records. This is the control.
4. `engine_shutdown`.

Red:

```
thread 'dst::dst_tests::fork_read_refusals::a_fork_read_while_its_own_shard_opens_is_retryable' panicked at src/dst/tests/fork_read_refusals.rs:<assert_opening line>:5:
assertion `left == right` failed: {"error":{"code":"internal","message":"engine unavailable: Opening { prefix: \"00\", code: \"shard_moving\", retry_after_secs: 2 }"}}
  left: 500
 right: 503
```

### R3 (C3): `...::a_fork_read_while_an_ancestor_shard_opens_is_retryable`

Setup: the source goes in `"00"` and the fork in `""`, so `hold_off("00")` refuses only the ancestor. The fork's own engine stays resident, and the stitched read hops to the source for offsets [0,3). Steps: `assert_opening` on the raw GET, then `clear_holdoff("00")` and GET 200 with 3 records as the control. Red: the same text as R2, because the prefix is again `"00"`. The stringified refusal is indistinguishable, which is the defect.

### R4 (C3): `...::an_ancestor_served_elsewhere_is_never_a_redirect`

Rig: `http_rig_build(mem(), RigRuntime::first(), HttpRigOptions { instance: Some("a".into()), ..Default::default() }).await.parts()`. The source goes in `"00"` and the fork in `""`, then `source_and_fork`. Then set the ring view:

```rust
let view = |fork_owner: &str| {
    let overrides = std::collections::HashMap::from([
        ("00".to_string(), "b".to_string()),
        (String::new(), fork_owner.to_string()),
    ]);
    (vec!["a".to_string(), "b".to_string()], overrides)
};
let (active, overrides) = view("a");
state.ownership.set_view(active, overrides);
```

1. GET the fork. Assert `!h.contains_key("streams-replay-to")`, `st == 500` and `error.code == "internal"`, then `assert!(message.contains("fork ancestor shard 00 is served by b"), "{message}")`.
2. PUT `far-child` (`name_in(.., "far-child", "")`) with `stream-forked-from: <source>`. Assert 500, no `streams-replay-to`, and `assert!(message.contains("fork source shard 00 is served by b"), "{message}")`.
3. Control: `view("b")` gives the fork's own shard to `b`. GET the fork and assert 409 with `streams-replay-to == Some("b")`.

On 0afa2597 the first three assertions already pass: 500, no header, `internal`. This test is a guard against the naive `ReadFailure::Resolve(ancestor)` fix. It is red only on the typed message:

```
thread 'dst::dst_tests::fork_read_refusals::an_ancestor_served_elsewhere_is_never_a_redirect' panicked at src/dst/tests/fork_read_refusals.rs:<line>:5:
engine unavailable: NotOwner { prefix: "00", owner: "b" }
```

If Søren picks D4's 503 alternative, change step 1 to `st == 503` and it goes red on the status instead.

### R5 (C3): `...::a_fork_source_the_catalog_cannot_read_is_not_reported_gone`

Call `source_and_fork(addr, "reg-src", "reg-fork")`, then `state.registry.fail_next_get("reg-src")`. That is a one-shot injected `object_store::Error::Generic { store: "registry", source: "injected registry get failure" }` (registry/cache.rs:298-304). The fork GET's first `get` of `reg-src` is the one in `fork_chain_of`. Assert 500, `!message.contains("is gone")` with the message `"a catalog failure is not a deleted source: {message}"`, and `message.contains("injected registry get failure")`. Control: the next GET is 200. Red:

```
thread 'dst::dst_tests::fork_read_refusals::a_fork_source_the_catalog_cannot_read_is_not_reported_gone' panicked at src/dst/tests/fork_read_refusals.rs:<line>:5:
a catalog failure is not a deleted source: fork source 'reg-src' is gone
```

### R6 (C3): `...::a_fork_created_while_its_source_shard_opens_is_retryable`

The source goes in `"00"`. PUT it with `[{"n":0}]`, then `hold_off("00")`. PUT `cre-fork` with `stream-forked-from: <source>` and apply `assert_opening`. Raw create runs `fork::prepare` (and so `validate_boundary` -> `handle_of(src)`) before it resolves the child (creation/raw.rs:33 vs :93), so the child's own placement does not matter. Control: `clear_holdoff("00")`, and the same PUT gives 201. Red: the same text as R2, since creation's Storage message is the same stringified refusal:

```
assertion `left == right` failed: {"error":{"code":"internal","message":"engine unavailable: Opening { prefix: \"00\", code: \"shard_moving\", retry_after_secs: 2 }"}}
  left: 500
 right: 503
```

### R7 (C4): `...::a_fork_read_is_customer_adoption`

```rust
/// A fork read is customer traffic (R29 custody): every shard it reads
/// carries external adoption, so the sweep can never close an engine under a
/// fork reader; the lookup used to resolve as maintenance does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_read_is_customer_adoption() {
    let (state, addr) = http_rig(mem()).await;
    let source = name_in(&state, "adopt-src", "00");
    let fork = name_in(&state, "adopt-fork", "");
    source_and_fork(addr, &source, &fork).await;
    let mut engines = Vec::new();
    for (role, name) in [("source", &source), ("fork", &fork)] {
        let route = crate::crypto::RouteHash::for_stream(&state.deployment.raw_adapter_sref(name)).0;
        let prefix = state.shards.prefix_for(&route);
        hold_off(&state, &prefix);
        state.shards.clear_holdoff(&prefix);
        // Reopened as maintenance reopens: an engine no customer has touched.
        let engine = state.engine_for_scaler(&route).await.expect("an internal reopen");
        let seq = engine.last_external_seq.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(seq, 0, "{role}: the reopen is internal");
        engines.push((role, engine));
    }
    let (st, _, b) = hreq(addr, "GET", &format!("/v1/stream/{fork}"), &[], b"").await;
    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&b));
    for (role, engine) in &engines {
        let seq = engine.last_external_seq.load(std::sync::atomic::Ordering::Relaxed);
        assert!(seq > 0, "the fork read met the {role}'s shard as customer traffic");
    }
    engine_shutdown(&state).await;
}
```

Red on the C3 tree, because `handle_of` is still `Adoption::Internal` and no other External resolve is on the fork GET path:

```
thread 'dst::dst_tests::fork_read_refusals::a_fork_read_is_customer_adoption' panicked at src/dst/tests/fork_read_refusals.rs:<line>:9:
the fork read met the source's shard as customer traffic
```

Two cases get no separate red test. Ancestor `OpenFailed` shares the `Opening | OpenFailed` or-pattern arm in both mappings, which R3 and R6 exercise. An opener fault for a single prefix is not exposed by the rig.

---

## 4. Edits, file by file, in commit order

### 4.0 Line budgets

Ceilinged files (no growth versus the merge base). All are **untouched** by this chain, so every budget is 0 and every end count equals the start count:

| file | wc -l now | budget | after chain |
|---|---|---|---|
| src/http.rs | 3,371 | 0 | 3,371 (untouched; `resolve_error_response` is reused via `use super::*` in http/read.rs) |
| src/product.rs | 4,205 | 0 | 4,205 (untouched; the renderer already maps Resolve/Storage) |
| src/shard.rs | 3,232 | 0 | untouched |
| src/billing.rs | 2,201 | 0 | untouched |
| src/history.rs | 1,713 | 0 | untouched |
| src/auth.rs | 1,676 | 0 | untouched |
| src/registry.rs | 1,509 | 0 | untouched |
| src/sse/feed.rs | 1,200 | 0 | untouched |
| src/fleet.rs | 1,143 | 0 | untouched |

Touched files that are at or under 1,000 lines (hard wall 1,000; counts simulated in scratch against 0afa2597):

| file | now | C1 | C3 | C4 |
|---|---|---|---|---|
| src/http/read.rs | 637 | 594 | 594 | 594 |
| src/http/tests.rs | 64 | ~88 | | |
| src/application/read.rs | 938 | | 983 | ~986 (≤1,000; 14 spare) |
| src/application/read_request.rs | 618 | | 636 | |
| src/application/creation.rs | 316 | | 346 | |
| src/application/creation/fork.rs | 326 | | 317 | |
| src/dst/dst_tests.rs | 267 | | 270 | |
| src/dst/tests/fork_read_refusals.rs | new | | ~265 | ~295 |
| src/dst/tests/fork_lifecycle.rs | 976 | untouched | | |

Functions measured with clippy's `too_many_lines` algorithm (body without braces, comment-only and blank lines skipped; threshold 100):

- `read_stitched`: 96 -> 96. Only the signature and the ancestor-hop line change.
- `validate_boundary`: **108 -> 99**, so its `too_many_lines` expect becomes unfulfilled and must be deleted (§4.3).
- `execute_fork_read`: 76 -> 75.

### C1: src/http/read.rs, src/http/tests.rs, WIRE-MATRIX

1. `src/http/read.rs:11`: `E::Resolve(error) => resolve_response(error),` becomes `E::Resolve(error) => resolve_error_response(error),`.
2. Delete `src/http/read.rs:58-100`: the `#[expect(clippy::unwrap_used, reason = "resolve_response; ...")]` and `fn resolve_response` with it.
   - Ratchet: deleting the function deletes its exception identity. Reasoned exceptions are not in `docs/quality/source-allowances.json` (only `unresolved-glob crate` for this file is), so there is no stale allowance and no `--prune`.
   - `read_failure_response` carries no expect.
3. `src/http/tests.rs`: add R1 above `use super::*;`. The file is `#![cfg(test)]`, so it is production-unchanged and has no mutation selection.
4. `docs/refactor/WIRE-MATRIX.md` §1.3 **Errors**: after `409 not_ring_owner+Streams-Replay-To`, add `503 shard_opening / shard_moving / shard_closing + Retry-After; 500 shard_open (the shared resolve_error_response envelope — review item 45; raw reads answered shard_open_failed before)`. §1.4 inherits it ("Errors as GET").

### C2: scripts/quality/mutation_owners.py

Line 86 changes from:

```python
    owner('read_request', 'src/application/read_request.rs', 'application::read_request::'),
```

to:

```python
    owner('read_request', 'src/application/read_request.rs',
          'application::read dst_tests::read_application:: dst_tests::fork_lifecycle::'),
```

`application::read` matches `application::read::read_contract_tests::*`, `application::read::o2_tests::*`, `application::read_remote_tests::*` and similar. The DST terms are the real execute_read and fork-read killers. No source path changes, so `declared_source_map` and `validate_sources` are unaffected. This is a tooling change, so the push selects every verification leg (props/fuzz, loom, miri); that is expected.

### C3: typed fork read

**`src/application/read.rs`**

1. `ReadService::read_stitched` (wrapper, no expect): `-> Result<ReadPage, String>` becomes `-> Result<ReadPage, ForkReadError>`.
2. `ReadService::handle_of` (no expect). Doc: `/// (engine, handle) for a stream's sole segment identity. A refusal is` / `/// the named stream's OWN: only a chain walk knows it met an ancestor.` Return type becomes `Result<(Arc<ShardEngine>, Arc<crate::shard::StreamHandle>), ForkReadError>`. `.map_err(|e| format!("engine unavailable: {e:?}"))?` becomes `.map_err(ForkReadError::Own)?`. `.map_err(|e| e.to_string())?` stays byte-identical; `?` converts through `From<String>`.
3. Insert before the `ForkChain` doc:

```rust
/// Why a stitched fork read failed. A shard's refusal keeps its type and the
/// hop that met it: only the read stream's OWN shard is where the request
/// belongs, so only that owner may become a redirect (review item 29).
#[derive(Debug)]
pub(crate) enum ForkReadError {
    /// The read stream's own shard refused resolution.
    Own(crate::shard_directory::ResolveError),
    /// An ancestor's shard refused resolution.
    Ancestor(crate::shard_directory::ResolveError),
    /// The catalog, the chain's integrity or a hop's storage failed.
    Storage(String),
}
impl ForkReadError {
    /// A refusal met while hopping into an ancestor is never the request's
    /// routing target, whichever lookup reported it.
    fn at_ancestor(self) -> Self {
        match self {
            Self::Own(refusal) | Self::Ancestor(refusal) => Self::Ancestor(refusal),
            Self::Storage(message) => Self::Storage(message),
        }
    }
}
/// The page executor and the chain walk report storage and integrity
/// failures as text; text is the one untyped class of a fork read failure.
impl From<String> for ForkReadError {
    fn from(message: String) -> Self {
        Self::Storage(message)
    }
}
impl From<&str> for ForkReadError {
    fn from(message: &str) -> Self {
        Self::Storage(message.to_string())
    }
}
/// The live feed carries a fork source's failure as an opaque error.
impl std::fmt::Display for ForkReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
```

The `From<String>`/`From<&str>` impls keep every existing `"…".into()` literal and `ReadPlan::execute(..).await?` in `read_stitched` byte-identical. Without them the body grows by 3 code lines, to 99, next to the `too_many_lines` wall. `Display` exists only so `sse/source.rs:59-67` (`.map_err(|e| anyhow::anyhow!(e))`) compiles unchanged. That file is critical, and it sits under an impl-wide `unwrap_used` expect.

4. Append to the `ForkChain` doc: `…a hard-deleted ancestor is an integrity error, and a catalog / that cannot be read is a storage failure, never a gone source.`
5. `ForkChainFuture`: `Output = Result<ForkChain, String>` becomes `Output = Result<ForkChain, ForkReadError>` (97 columns).
6. `fork_chain_of` (no expect): split the registry arm.

```rust
            let d = match state_reg.get(&src_ref).await {
                Ok(Some(d)) if !d.deleted => d,
                Ok(_) => return Err(format!("fork source '{src}' is gone").into()),
                Err(error) => {
                    return Err(format!("fork source '{src}' could not be read: {error}").into());
                }
            };
```

The incarnation check becomes `return Err(ForkReadError::Storage(format!(` … `)));`. That explicit variant pins the async block's error type for the `.into()` sites; today the `format!` sites pin `String`.

7. `read_stitched`: the signature becomes `-> Result<ReadPage, ForkReadError>`. The ancestor hop at :518, `state.handle_of(d).await?`, becomes `state.handle_of(d).await.map_err(ForkReadError::at_ancestor)?`. Nothing else in the body changes.

**`src/application/read_request.rs`**

8. After `impl std::error::Error for ReadFailure {}` (:84), add:

```rust
/// A fork read's refusal as the read verdict (review item 29). Only the
/// fork's OWN shard is where this request belongs, so only its owner becomes
/// a redirect; an ancestor another instance owns is a chain this instance
/// cannot stitch, and an ancestor still opening is as retryable as the fork.
impl From<super::ForkReadError> for ReadFailure {
    fn from(error: super::ForkReadError) -> Self {
        use super::ForkReadError as F;
        use crate::shard_directory::ResolveError as R;
        match error {
            F::Own(refusal)
            | F::Ancestor(refusal @ (R::Opening { .. } | R::OpenFailed { .. })) => {
                Self::Resolve(refusal)
            }
            F::Ancestor(R::NotOwner { prefix, owner }) => Self::Storage(format!(
                "fork ancestor shard {prefix} is served by {owner}; this instance cannot stitch it"
            )),
            F::Storage(message) => Self::Storage(message),
        }
    }
}
```

9. `execute_fork_read` (:389, under `#[expect(clippy::unwrap_used, reason = "ReadService::execute_fork_read; …")]`):
   - `:391` `self.handle_of(desc).await.map_err(ReadFailure::Storage)?` becomes `self.handle_of(desc).await?`.
   - `:441-443` `.await\n.map_err(ReadFailure::Storage)?;` becomes `.await?;`.
   - Nothing is added, only removed.

**`src/application/creation.rs`**

10. `CreationService::resolve` (no expect) becomes `.map_err(CreationError::refused)`, and its closure body moves verbatim into:

```rust
impl CreationError {
    /// The ONE creation rendering of a refusal by the shard this request is
    /// routed to: a non-owner names its owner as the redirect.
    fn refused(refusal: crate::shard_directory::ResolveError) -> Self { /* the closure's three arms, verbatim */ }
}
/// A fork is created on the CHILD's shard, so a refusal met anywhere on the
/// source chain is never where this request belongs: it carries no owner,
/// while a source shard still opening is as retryable as the child's own.
impl From<crate::application::read::ForkReadError> for CreationError {
    fn from(error: crate::application::read::ForkReadError) -> Self {
        use crate::application::read::ForkReadError as F;
        use crate::shard_directory::ResolveError as R;
        match error {
            F::Own(R::NotOwner { prefix, owner }) | F::Ancestor(R::NotOwner { prefix, owner }) => {
                Self::new(
                    CreationFailure::Storage,
                    "internal",
                    &format!("fork source shard {prefix} is served by {owner}"),
                )
            }
            F::Own(refusal @ (R::Opening { .. } | R::OpenFailed { .. }))
            | F::Ancestor(refusal @ (R::Opening { .. } | R::OpenFailed { .. })) => {
                Self::refused(refusal)
            }
            F::Storage(message) => Self::new(CreationFailure::Storage, "internal", &message),
        }
    }
}
```

Control: `needless_pass_by_value` should stay silent on `refused`, because the match destructures the refusal by move. If it fires anyway, keep the closure in `resolve` and inline the two `refused` arms in the `From` impl.

**`src/application/creation/fork.rs`** (`validate_boundary` has four function-wide expects: too_many_lines, unwrap_used, cast_possible_truncation, excessive_nesting)

11. `:207-210` becomes `let (_, src_handle) = state.reads.handle_of(src).await?;`.
12. `:262-276` becomes `let rec = state.reads.read_stitched(src, &src_key, crate::application::read::ReadRange::bounded(base, base.saturating_add(1)), 64 << 20).await?;` (rustfmt chain, 9 lines).
13. **Delete** the `#[expect(clippy::too_many_lines, reason = "validate_boundary; …")]` at `:183-186`. Clippy's count falls from 108 to 99, and an unfulfilled expectation is denied. The other three expects stay fulfilled:
   - `unwrap_used`: `src_handle.state.lock().unwrap()` at :211 remains.
   - `cast_possible_truncation`: `sub as usize` remains.
   - `excessive_nesting`: the ceiling verdict's `if let Some(over)` block is still nesting level 5 under the threshold of 4; the removed `Err(m) => {..}` block was level 4, not a lint site.

**DST, ledgers, docs**

14. `src/dst/dst_tests.rs`: after `mod fork_lifecycle;`, add a blank line, `#[path = "tests/fork_read_refusals.rs"]` and `mod fork_read_refusals;`.
15. `src/dst/tests/fork_read_refusals.rs` (new): the helpers plus R2-R6.
16. `scripts/quality/mutation_owners.py`: append ` dst_tests::fork_read_refusals::` to the read_request filters.
17. Ledgers: see §6.

**Every `#[expect]`-ratcheted function C3 touches, and its remedy**

| function | exception | what changes | ratchet effect | remedy |
|---|---|---|---|---|
| `read_stitched` (read.rs:460-470) | `unwrap_used` (function-wide) | signature path `String` -> `ForkReadError`; new path `ForkReadError::at_ancestor` | `unwrap_site:path` fingerprints grow 0->1 | **Re-decide the reason** (explicit review decision; see below) |
| `ReadService::execute_fork_read` (read_request.rs:385-389) | `unwrap_used` | two `.map_err(ReadFailure::Storage)` removed | scope_lines -1, syntax_facts and path fingerprints decrease | none |
| `validate_boundary` (fork.rs:183-199) | too_many_lines, unwrap_used, cast_possible_truncation, excessive_nesting | two match blocks become `?` | scope_lines -9, facts decrease on all four | delete too_many_lines (unfulfilled) |
| `impl FeedSourceRead for SingleSource` (sse/source.rs:42-46) | impl-wide `unwrap_used` | not edited (compiles through `Display`) | none | none |
| `resolve_response` (http/read.rs:58) | `unwrap_used` | deleted in C1 | identity removed | none |

Narrowing `read_stitched`'s exception is not available. A statement-level `#[expect]` is still scoped to the smallest enclosing item, which is `read_stitched` itself (source_rules.py:133-140). A helper fn holding only the lock unwrap would be a wrapper introduced solely for a lint, which RUST-QUALITY.md forbids. The new reason has exactly two `;` and no `"`:

```
reason = "read_stitched; a poisoned stream state may hold a half-advanced durable frontier and the fork failure this read returns is typed for resolution, catalog and storage faults only; recovering the lock could stitch a page past a length never made durable"
```

No function under an unwrap expect gains a call through a lexical import alias. The new `use … as F/R` aliases live inside the `From::from` bodies, which carry no expect.

### C4: customer adoption

18. `src/application/read.rs` `handle_of`: `crate::shard_directory::Adoption::Internal` becomes `crate::shard_directory::Adoption::External`. The doc becomes: `/// (engine, handle) for a stream's sole segment identity. Fork reads and` / `/// fork creation are customer traffic: the resolution stamps external` / `/// adoption so the sweep never closes an engine under them (R29). A refusal` / `/// is the named stream's OWN: only a chain walk knows it met an ancestor.`
    - `handle_of` has no expect.
    - No synchronization primitive or custody protocol changes: `stamp_external` and `install_custody` are untouched, and only who calls External changes. So no loom or held-commit leg is owed. `runtime_sweep` and `livefeed_swap` are the custody neighbours to rerun.
    - DST `append_application.rs:90,159` also call `handle_of`. Their engines now get stamped, which is harmless because neither test observes custody.
19. `fork_read_refusals.rs`: add R7.
20. `docs/refactor/test-inventory.json`: regenerate.

### 4.5 Contingency (only if `wc -l src/application/read.rs` > 1,000 after `cargo fmt`)

Prepend a verbatim-move commit, C3a. Move `#[cfg(test)] mod o2_tests { … }` (read.rs:862-938, 77 lines) to `src/application/read/o2_tests.rs`, declared as `#[cfg(test)] mod o2_tests;`.

- The test path `application::read::o2_tests::…` is unchanged.
- The file is not under a critical prefix.
- `docs/quality/source-allowances.json` loses the `unresolved-glob / src/application/read.rs / crate::o2_tests` allowance: run the ratchet with `--prune`.
- `docs/quality/owners.json` gains an `unresolved-glob` row for the new file's `use super::*;`.

---

## 5. Mutation-kill analysis (cargo-mutants 27.1.0, `--in-diff`, TIMEOUT = miss)

Critical files in the chain's diff: `src/http/read.rs` (owner `http_read`) and `src/application/read_request.rs` (owner `read_request`).

- `src/http/tests.rs` is `#![cfg(test)]`, so it is production-unchanged and has no selection.
- `src/application/read.rs`, `creation.rs` and `creation/fork.rs` do not match `src/application/read_`, and they are not registered. `src/dst/**` is not critical.
- No new file falls under a critical prefix, so no new owner row is needed.

Expected plan: `mutation_source_files = ['src/application/read_request.rs', 'src/http/read.rs']`, `selected_mutation_owners = ['read_request', 'http_read']`, `unregistered_mutation_source_files = []`.

| file / function | in-diff mutant | viable? | killed by (owner filter term) |
|---|---|---|---|
| http/read.rs `read_failure_response` (line 11 changed) | replace body with `Default::default()` (`Response<Body>: Default`) | yes | `dst_tests::read_application::the_page_route_types_its_refusal_and_the_public_route_keeps_its_envelope`, which asserts `status == 404` and `error.code == "not_found"` on the raw route (read_application.rs:471-482). The mutant answers 200 with an empty body. `http_read` filter term: `dst_tests::read_application::`. |
| http/read.rs match on `ReadFailure` | delete-arm mutants | not generated: there is no wildcard arm | none |
| read_request.rs `<impl From<ForkReadError> for ReadFailure>::from` | replace body with `Default::default()` | **unviable** (`ReadFailure: !Default`) | none |
| read_request.rs `from` match | guard/arm mutants | none: no guards, no wildcard, or-patterns are not mutated | none |
| read_request.rs `execute_fork_read` (:391, :441-443 changed) | replace body with `Ok(Default::default())` | **unviable** (`ReadOutcome: !Default`) | none |
| read_request.rs `execute_fork_read` operators on the changed lines | none: the changed lines hold only `?` and method calls | none | none |

New guards and predicates: none in critical files. The only new branching is the or-pattern match in `From::from`, which cargo-mutants does not mutate. `at_ancestor` and the registry arm split are in non-critical read.rs. There are no equivalent mutants to restructure away.

Behavioural kill coverage outside the mutation leg:

| code | killed by |
|---|---|
| ancestor re-tag (`at_ancestor`) | R4 (a mutant keeping `Own` would add the header) |
| `Own -> Resolve` | R2 and R4's control |
| `Ancestor(Opening) -> Resolve` | R3 |
| creation mapping | R4 step 2 and R6 |
| registry split | R5 |
| adoption | R7 |

Owner rows and filters that change:

- **`read_request`**. C2: `application::read_request::`, which selects 0 tests (by construction; control §7.2), becomes `application::read dst_tests::read_application:: dst_tests::fork_lifecycle::`. C3 adds `dst_tests::fork_read_refusals::`.
- **`http_read`**: unchanged. Its `http::read::` term matches nothing, since the module is `http::read_adapter` (§1.3), but the DST terms kill its only viable mutant.

The DST tests in these filters bound every wait: holdoff 3 s, `open_wait` 10 s, `hreq` per-request. The mutation baseline runs the widened filter, roughly 30 DST tests, well inside `--timeout 90`.

Nightly consequence of C2: the scheduled rotation slot for `read_request` will now actually test the whole of `read_request.rs` (execute_read etc.). Any pre-existing surviving mutants there surface in that nightly bucket, not in this push's in-diff run.

---

## 6. Ledgers (same commit as the change)

| ledger | C1 | C2 | C3 | C4 |
|---|---|---|---|---|
| `docs/refactor/test-inventory.json` (`python3 scripts/test-inventory.py --write`) | no (unit test, not DST) | no | +5 DST (497 -> 502) | +1 (-> 503) |
| `docs/quality/owners.json` | no | no | + by-path row (below) | no |
| `docs/refactor/architecture-policy.json` | no | no | no (`src/dst/**` is test-only for the architecture gate; the application files add no `crate::http`/`product` edge and no `Response`/`HeaderMap`/`AppState` token) | no |
| `docs/refactor/WIRE-MATRIX.md` | §1.3 errors (see C1) | no | yes (below) | no |
| `docs/refactor/review-mechanisms.json` | no pinned test body changes | | no | no |
| `src/dst/tests/README.md` | | | no edit: the `fork_*` glob in the "Lifecycle and topology" row already owns `fork_read_refusals` | |
| `scripts/quality/mutation_owners.py` | | read_request filter | + `dst_tests::fork_read_refusals::` | |
| `docs/quality/source-allowances.json` | no stale identity (reasoned exceptions are not inventoried) | | no | no |
| `scripts/mt-audit-baseline.txt` | no new `registry.get("…")`, `HashMap<String` or `stream_hash(` sites (`fail_next_get(` does not match the `registry.(get|…)("` pattern) | | no | no |

The C3 `docs/quality/owners.json` row:

```json
{
  "category": "by-path-module",
  "count": 1,
  "owner": "crate::fork_read_refusals",
  "path": "src/dst/dst_tests.rs",
  "reason": "Fork read refusal scenarios; real HTTP fork reads and fork creation against a held-off, foreign-owned or catalog-failing source shard; compiled and executed with DST.",
  "syntax": "path = \"tests/fork_read_refusals.rs\""
}
```

C3 WIRE-MATRIX edits:

- **§1.1 Errors**, append: `Fork source shard refusals (fork::validate_boundary, typed by review item 29): 503 shard_opening / shard_moving / shard_closing + Retry-After while the SOURCE's shard is not serving here; 500 shard_open when it failed to open; 500 internal when another instance owns it — never Streams-Replay-To, which names only the child's own shard.`
- **§1.3**, add a bullet: `**Forks** (review item 29): the fork's OWN shard refuses like any read (409 not_ring_owner + Streams-Replay-To / 503 + Retry-After / 500 shard_open); an ANCESTOR's shard answers 503 + Retry-After while opening and 500 shard_open on open failure, but 500 internal and never Streams-Replay-To when another instance owns it (cross-owner chains are not stitched); a catalog read failure mid-chain is 500 internal, not gone.`
- **§2.9 Errors**, append: `fork reads whose own or ancestor shard is opening: 503 temporarily_unavailable (retryable, no Retry-After yet); own shard foreign: 409 + Streams-Replay-To.`

---

## 7. Controls

All commands run from `/Users/sorenschmidt/code/streams`.

### 7.1 C1

- Red: `cargo test --locked --lib http::tests::a_read_refusal_renders_through_the_one_resolve_mapping`, with only the R1 hunk applied, prints the §3 R1 output and `test result: FAILED. 0 passed; 1 failed`.
- Green: the same command with the fix prints `test http::tests::a_read_refusal_renders_through_the_one_resolve_mapping ... ok` and `test result: ok. 1 passed; 0 failed`.
- Neighbours: `cargo test --locked --lib -- dst_tests::read_application:: dst_tests::reads_raw:: product::tests:: http::tests::` passes with 0 failed.
- `wc -l src/http/read.rs` prints `594`, and `git grep -n shard_open_failed` prints nothing.

### 7.2 C2

- `cargo test --locked --lib application::read_request:: -- --list | tail -1` on the old filter prints `0 tests, 0 benchmarks`. That is the defect.
- `cargo test --locked --lib application::read -- dst_tests::read_application:: dst_tests::fork_lifecycle:: --list | tail -1` prints `N tests, 0 benchmarks` with N > 0.
- `python3 -m unittest discover -s scripts/quality -p 'test_*.py'` passes.

### 7.3 C3

- Red: `cargo test --locked --lib dst_tests::fork_read_refusals::`, with only the new test file and its `dst_tests.rs` registration applied, gives 5 FAILED with the §3 R2-R6 messages.
- Green: the same command after the fix gives `test result: ok. 5 passed; 0 failed`.
- Neighbours: `cargo test --locked --lib -- dst_tests::fork_lifecycle:: dst_tests::fork_cleanup:: dst_tests::read_application:: dst_tests::reads_raw:: dst_tests::livefeed_basics:: dst_tests::livefeed_history:: dst_tests::lifecycle_creation:: dst_tests::append_application:: shard_directory:: product::tests:: http::tests::` passes with 0 failed. This includes the stale-epoch 500 control at fork_lifecycle.rs:748-754.
- `cargo fmt --all -- --check` prints nothing.
- `cargo clippy --locked --workspace --all-targets -- -D warnings` is clean. It proves three things: `validate_boundary` has no `unfulfilled_lint_expectations`, `read_stitched` has no `too_many_lines`, and `refused` has no `needless_pass_by_value`.
- `wc -l src/application/read.rs` is at most 1,000 (planned 983).
- `python3 scripts/test-inventory.py --write` prints `test-inventory: wrote 502 tests`, then `--check` prints `test-inventory: OK (502 tests, 0 ignored)`.
- `python3 scripts/architecture-gate.py --check` passes.
- `bash scripts/multitenancy-audit.sh` prints `MT_AUDIT_OK`.
- `scripts/quality.sh` prints `QUALITY_OK`.

### 7.4 C4

- R7 is red, then green, via `cargo test --locked --lib dst_tests::fork_read_refusals::a_fork_read_is_customer_adoption`.
- Custody neighbours: `cargo test --locked --lib -- dst_tests::runtime_sweep:: dst_tests::livefeed_swap:: dst_tests::fork_` passes.
- Inventory: `test-inventory: OK (503 tests, 0 ignored)`.

### 7.5 Before push (CI's plan, per the slate push event)

- `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=0afa2597 python3 scripts/quality/verification_plan.py --out target/quality-plan`, then check `target/quality-plan/plan.json`:
  - `mutation_source_files == ["src/application/read_request.rs","src/http/read.rs"]`
  - `unregistered_mutation_source_files == []`
  - compiler, properties_fuzz, loom and miri are all true (tooling changed)
- `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=0afa2597 python3 scripts/quality/mutation_driver.py --out target/quality-mutations` exits 0.
  - `http_read`: 1 mutant, caught.
  - `read_request`: listed mutants are all unviable, with no MISSED and no TIMEOUT.
- Full suite as CI runs it: `cargo test --release -- --skip post_split_throughput_scales`, all 0 failed, then `python3 scripts/quality/tests_ran.py /tmp/suite.log --inventory docs/refactor/test-inventory.json --skipped 1` passes.
- After the push, confirm CI with `gh run view` (never claim green without it).

---

## 8. Out of scope

- **Product read Retry-After drift.** `render_product_read_failure` maps `Resolve(Opening)` to 503 `temporarily_unavailable` with no Retry-After (product.rs:2965). The raw path sends it. This is item 29's "product read code/Retry-After" drift. It touches product.rs (4,205, at its ceiling) and needs its own move-then-fix.
- **ConsumerFailure** drops `retry_after_secs` on Opening (consumer.rs:88). This is item 29's other mapper drift.
- **Cross-owner fork stitching and creation** (a peer relay of ancestor spans). Today, and after this chain, a fork whose source lives on another instance cannot be read or created: it answers 500, now honestly and without a redirect. This is a real product gap in multi-instance rings (§1.3.3).
- **The 500 vs 503 split for catalog (registry) faults**, on reads and elsewhere (item 45's note).
- **SSE fork sources.** `SingleSource::read_batch` still type-erases the stitched failure into `anyhow`, so a live fork subscription whose ancestor is opening is handled as any transient source error.
- **The `http_read` owner's dead `http::read::` term** (§1.3.2), and a quality-script guard that fails an owner whose filters select zero tests.
- **The 30-space run** in the "different incarnation" message (read.rs:442).
- **The double resolution of the fork's own shard.** `execute_fork_read` resolves it, then `read_stitched` resolves it again (a perf nit).
- Items 30 and 46.

---

## Skeptic corrections (C1..C11)

Verified against the tree at `fba5af56` (the current HEAD). Read-only: no cargo was run, and no repo file was edited.

**Verified true (no action needed):**

- **Ceilings.** wc -l matches the §4.0 table: http.rs 3,371, product.rs 4,205, shard.rs 3,232, billing.rs 2,201, history.rs 1,713, auth.rs 1,676, sse/feed.rs 1,200, fleet.rs 1,143. registry.rs is 1,509 at HEAD. None of these is touched.
- **C1 line counts.** http/read.rs goes 637 -> 594, because the deleted :58-100 is 43 lines.
- **`validate_boundary` size.** It is 108 clippy lines today (braces stripped, blanks and comments skipped). The two `?` rewrites save 3 + 6 lines, giving 99, so the `too_many_lines` expect becomes unfulfilled and must be deleted, as the plan says. `excessive_nesting` stays fulfilled: its lint site is the `if let Some(over)` block at nesting level 5 (fn body 1, `if sub > 0` 2, is_json-else 3, `sub == len` else 4). The removed `Err(m) => {}` arm block sits at level 4.
- **Other function sizes.** `read_stitched` is 96 lines, which is correct.
- **Mutation receipt.**
  - `src/http/tests.rs` is `#![cfg(test)]`, so `test_only_file` holds (tools/quality-syntax/src/scan.rs:28) and the file lands in production_unchanged.
  - read.rs, creation.rs and creation/fork.rs match no critical prefix.
  - The receipt is therefore exactly `[read_request.rs, http/read.rs]`.
  - The `read_failure_response` `Default::default()` mutant is killed by the first assertion (status 409) at read_application.rs:426.
  - `ReadOutcome` and `ReadFailure` do not derive `Default`, so the `read_request` in-diff mutants are unviable.
- **R1-R7 red traces hold on the tree.**
  - No pre-check resolves the engine before `execute_fork_read`: http.rs:2905-2935 goes to read_inner, then renew_ttl (a no-op without a TTL), then `execute_read` :206.
  - `fail_next_get` fires before the descriptor cache (registry/cache.rs:297-303).
  - The rig spawns only the http task (fixture_http.rs:533), so nothing stamps External in the background and R7's red is deterministic.
  - Shutdown retirement arms HOLDOFF_BASE, 3 s (sharddir/holdoff.rs:52-53).
  - `set_view` honours a `""` override (ownership.rs:76-89).
- **Gates.**
  - mt-audit patterns are not triggered: `HashMap<String` is scanned only in scaler3.rs and registry.rs, and `fail_next_get(` does not match `registry.get("`.
  - mt_lint excludes src/dst.
  - The architecture gate is clean: no `Response`, `HeaderMap`, `AppState` or `axum::` token in the application files.
  - The owners.json row shape matches the existing dst rows, e.g. `crate::quota_read_volume`.
  - The README `fork_*` glob covers the new file.
  - review-mechanisms pins only a creation.rs test body, which is untouched.
  - The re-decided reason passes source_rules.py:258's regex `"[^";]+;[^";]+;[^";]+"`.

**C1 (base SHA is stale).**
- HEAD and origin/slate are `fba5af56`, not `0afa2597`. The three commits since then touch only scripts and docs (architecture-policy.json, mt-audit, quality diagnostics). They leave every planned file unchanged, so the edits and line numbers stand.
- §7.5 must use the actual origin/slate at push time as `QUALITY_BEFORE_SHA`, not `0afa2597`.
- Another session has uncommitted edits in the working tree: `M src/registry.rs` (1,501 lines) and an untracked `src/registry/failpoints.rs`, which moves `fail_next_list` there.
  - Stage explicit paths only; never `git add -A`.
  - If that work lands first, `fail_next_get` may move into failpoints.rs. Its API is unchanged, so R5 is unaffected.

**C2 (§1.2 misstates the non-fork contract, and D5 needs restating).**
- A non-fork read whose shard is foreign does **not** answer 409 by default. read_request.rs:237-247 relays through `remote_read_page` whenever `command.allow_remote && !LongPoll`. That is the raw default (`allow_remote: !params.no_fanout`, http/read.rs:207) and product always (`allow_remote: true`, product.rs:2780). Only long-poll, `no_fanout`, and the `index > entry_index` handoff answer 409 / Handoff.
- So "fork own shard foreign -> 409 + Streams-Replay-To" is a *different* edge contract from non-fork reads (relay).
- Restate this row for Søren with that contrast. The alternative is to keep 500. A peer relay is not available for stitched reads (§8, cross-owner stitching).

**C3 (decision labels are dangling).**
- `D1-D7`, `D2`, `D4` and `D6` are referenced (lines 115, 129, 268, 312) but never defined, and "§4.3" (line 115) points to no section; the registry split is §4 C3 item 6.
- Number the §2.2 table rows D1..Dn (there are 11 rows) and fix the cross-references, so Søren can approve by label.

**C4 (`read_stitched` ratchet understated).**
- `unwrap_site:path` does not only grow 0->1. `syntax_facts` also grows (the `.map_err` method-call site plus the `ForkReadError::at_ancestor` path), and the return-type path changes from `String` to `ForkReadError`.
- The re-decided reason (a new exception identity) covers all of these, so the remedy is correct. The table should list the `syntax_facts` growth too.
- `execute_fork_read` and `validate_boundary` only lose facts; `?` adds no fact.

**C5 (code blocks are not rustfmt-clean; counts are pre-fmt).**
- rustfmt's `struct_lit_width` (18) forces the R1 `ResolveError::{OpenFailed, Opening, NotOwner} { .. }` literals and R4's override pairs onto multiple lines.
- src/http/tests.rs will be about 100 lines, not ~88.
- Re-measure read.rs **after** `cargo fmt` before deciding on the §4.5 contingency. The `ForkReadError::Storage(format!(` wrap can add a line.

**C6 (the C2 filter is too narrow for "the tests that exercise it", and the nightly timeout risk is unmeasured).**
- `execute_read`'s non-fork branches are killed by `dst_tests::reads_raw::`, `reads_product::`, `reads_ring::`, `reads_applied::`, `reads_history::`, `read_page_limits::` and `read_peer_compatibility::`, not by read_application/fork_lifecycle alone. Those branches are the remote relay, the Handoff empty page, the refresh rewind and the applied long-poll.
- With the planned filter, the nightly rotation will report those branches MISSED.
- Use the `http_read` term set plus the fork terms. Add a control timing the filtered suite (`time cargo test --locked --lib application::read -- <rest>`), well under `--timeout 90`, because TIMEOUT counts as a miss.

**C7 (the DST attributes and the holdoff window are unspecified).**
- R2-R6 give no attribute. Specify `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]` as the neighbours do. The inventory records attributes.
- Every step before `clear_holdoff` must finish inside the 3 s holdoff. If more than 3 s passes, the open succeeds and the "503" assertion sees 200.
- `assert_opening`'s `1..=3` already tolerates a stall under 2 s. State the window in the helper doc.

**C8 (item 29 scope cut is only partly justified).**
- The reviewer's change list includes "fix the three concrete mapper drifts (ConsumerFailure retry_after, product read code/Retry-After)".
- The product.rs ceiling justifies deferring the product drift. The ConsumerFailure drift is in src/application/consumer.rs, which has 764 lines and no ceiling. :88 `Opening { code, .. }` drops `retry_after_secs`, and that needs no move.
- Either add a small in-chain commit, or record the deferral explicitly as a Søren decision or follow-up. §8 must not imply that the ceiling blocks it.

**C9 (optional strengthening of C4 custody).**
- R7 proves the stamp, not the consequence. To prove the sweep cannot close the engine under a fork reader, add a control:
  1. After the fork GET, run `crate::billing::sweep_owned_outboxes(&state)`.
  2. Assert that both prefixes are still `state.shards.is_open(..)`.
- It must serialize on runtime_sweep's `sweep_lock()`, as runtime_sweep.rs:52 does.
- Not required, since no sync primitive changes, but it is the behavioural evidence item 48 asks for.

**C10 (doc style).** The `ForkReadError` variant docs ("The read stream's own shard refused resolution.") say what the code does. The repo rule asks for why: owner or invariant. For example, `Own`: "the request's routing target: its owner may become a redirect". `Ancestor`: "never a routing target: its owner must never be named to the client".

**C11 (WIRE-MATRIX, optional).**
- §1.3 line 47 still names `read_fork_inner` and stale http.rs line ranges.
- Since C3 edits §1.3 for forks anyway, point the fork row at `execute_read -> execute_fork_read`.
- §2.9's `translate_read_error` / `not_stream_owner` is also stale: product reads render through `render_product_read_failure`, which answers `not_ring_owner`. The C3 §2.9 text should not repeat the stale name.

**Unbuildable controls.**
- None outright.
- §7.5's `QUALITY_BEFORE_SHA=0afa2597` runs, but against the wrong base (C1).
- §7.3's "5 FAILED" red and the C1/§7.1 line counts must be taken after `cargo fmt` (C5).

**Missed ledgers or ratchets.**
- None mandatory.
- The C4 note (read_stitched `syntax_facts` also grows) belongs in the ratchet table.
- test-scenario-map.json is optional. The FRK-0xx IDs could map the new tests, but no gate requires it.

**Verdict: ready-with-corrections.**
