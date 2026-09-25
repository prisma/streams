# system_append: typed create-vs-relay, no status sniffing (review rank 21)

Repository `/Users/sorenschmidt/code/streams`, branch `slate @ 82095942`. Read-only verification; nothing in the repo was touched.

**Verdict: the claim is CORRECT and current.** The non-owner first append of any reserved stream fails with `system append <stream>: 404 Not Found` until the owner creates the stream on its own; the "create refused by ownership relays below" branch in `billing.rs` is unreachable; failures are flattened to a status string. Evidence below.

---

## 1. Mechanism (file:line evidence)

### 1.1 The sender, `src/billing.rs:2197-2280` (`system_append`)

```
2212  let mut r = crate::http::append(...).await;                 // local attempt #1
2223  if r.status() == NOT_FOUND {
2224      let c = crate::http::create_stream(...).await;          // local create
2232      let cst = c.status().as_u16();
2233      if !(cst == 200 || cst == 201 || cst == 409) {          // <-- 409 accepted as "exists"
2235          if replay_peer_url(state, &c).is_none() { return Err(..) }
2238          r = c;                                               // <-- intended ownership relay: DEAD
2239      } else {
2240          r = crate::http::append(...).await;                 // local attempt #2
2251      }
2252  }
2253  if r.status().is_success() { return Ok(()); }
2257  if let Some((_, base)) = replay_peer_url(state, &r) { ...relay... }   // needs streams-replay-to on r
2279  Err(format!("system append {stream}: {}", r.status()))
```

### 1.2 Why a not-owner create is a 409 that the sender misreads

- `src/application/creation/raw.rs:20-31` — `CreationService::create` checks `self.ownership.foreign_owner(&prefix)` FIRST and returns `CreationError { kind: CreationFailure::Conflict, code: "not_ring_owner", owner: Some(owner), .. }`.
- `src/http.rs:2655-2679` — `creation_error_response` maps `F::Conflict => StatusCode::CONFLICT` (409) unconditionally, then adds `streams-replay-to` only when `error.owner` is `Some`. So the ownership refusal IS a 409 — exactly the status `billing.rs:2233` treats as "exists". The `!(200|201|409)` branch can only be entered for 4xx/5xx that never carry an owner (the only `owner: Some` sites are `raw.rs:22-31`, `creation/product.rs:37`, `creation.rs:113`, all `Conflict`), so `replay_peer_url(state, &c)` there is always `None` and the "relay the create" branch is dead code.

### 1.3 Why the second local append is a 404 with no replay header

- `src/application/append.rs:47-94` — `AppendService::prepare` does `registry.get(sref)` BEFORE any ownership check; a stream nobody created is `AppendFailure { class: Missing, code: NotFound, owner: None }`.
- Ownership on the append path is only decided later, in `src/application/append/submit.rs:18-25` (`shards.resolve` -> `AppendFailure::from_resolve` -> `AppendCode::NotOwner` + `owner`), which a missing stream never reaches.
- `src/http.rs:41-108` `render_append`: `NotFound` -> 404 `not_found`; `streams-replay-to` is set only from `error.owner`.
- `src/http.rs:3177-3186` `replay_peer_url` requires the `streams-replay-to` header; a 404 has none -> `None` -> `billing.rs:2279` returns `Err("system append _audit_events: 404 Not Found")`.

### 1.4 The receiver, `src/http.rs:1227-1273` (`internal_telemetry_append`)

Same sniff: `create_stream` then `if !(cst == 200 || cst == 201 || cst == 409) { return c; }` then `append`. A receiver whose own ring says another instance owns the shard (fleet skew) masks the 409 `not_ring_owner` + `streams-replay-to` as "exists" and answers the append's 404 `not_found` instead — the sender cannot tell "owner has not converged" from "stream missing".

### 1.5 Consequences for the callers (all requeue on `Err`, none can self-heal)

- `src/audit.rs:298-305` — `drain_audit_once` returns early on an empty batch and otherwise calls `system_append`; on `Err` the batch requeues (`persist_audit_batch`) and is retried next tick. A non-owner instance's denials therefore sit in memory (bounded queue: `dropped`/`gap` accounting) until the OWNER happens to create `_audit_events` from its own denials.
- `src/ops.rs:335,617`, `src/fleet.rs:1137`, `src/billing.rs:742` — same shape for `_ops_events`, `_ops_metrics`, `_usage`.

### 1.6 The existing tests knew

`src/dst/tests/security_workload.rs:249-259` and `351-361`: "Prime: the OWNER creates the system stream ... B's local attempt then refuses on OWNERSHIP with streams-replay-to, not existence." The prime exists because the unprimed path does not work.

### 1.7 Trace of the unprimed case on current code (rig from 1.6 without the prime)

rig-b (`instance: "rig-b"`, ring `["rig-a","rig-b"]`, override `"00" -> "rig-a"`; the rig has one prefix `"00"`, `fixture_http.rs:44`):

1. `append` -> `prepare` -> `registry.get` (shared `mem()` store) = `None` -> 404 `not_found`, no replay header.
2. `create_stream` -> `create` -> `foreign_owner("00") = Some("rig-a")` -> 409 `not_ring_owner` + `streams-replay-to: rig-a`.
3. `cst == 409` -> `append` again -> 404 again.
4. `is_success()` false; `replay_peer_url` on the 404 -> `None`.
5. `Err("system append _ops_events: 404 Not Found")` (`http::StatusCode` Display is `"404 Not Found"`).

---

## 2. Design

### 2.1 Design A (recommended): follow the typed verdicts, local first

One local function `append_local(state, sref, key, body) -> Result<AppendOutcome, LocalFailure>` used by BOTH the sender (`system_append`) and the receiver (`internal_telemetry_append`):

1. `append_typed` locally. `Ok` -> done. `Err(code == NotFound)` -> step 2. Any other `Err` -> `LocalFailure::Append(e)` (which may carry `owner`).
2. `creation_service().create(CreateCommand{ empty body, application/json, no ttl })`. `Ok(_)` -> step 3. `Err(e)` -> `LocalFailure::Create(e)` (which may carry `owner`).
3. `append_typed` again -> `Ok` or `LocalFailure::Append(e)`.

Sender: `Ok` -> `Ok(())`; `Err(f)` with `f.owner()` resolving through `state.peer.url_for` -> ONE relay hop (the existing `mk`/`peer.send` block, extracted verbatim into `relay`); otherwise `Err(format!("system append {stream}: {f}"))` where `Display for LocalFailure` renders `append <code>: <message>` / `create <code>: <message>`.

Receiver: `Ok(out)` -> `render_append(Ok(out))`; `Append(e)` -> `render_append(Err(e))` (keeps 409 `not_ring_owner` + `streams-replay-to` from the append path); `Create(e)` -> `creation_error_response(e)` (409 `not_ring_owner` + `streams-replay-to` on a non-owner, never masked); `Key(m)` -> 400 `invalid_key`.

Why no "Conflict without owner = exists" arm (the item's proposal): `CreationService::create` already resolves the only benign race internally — `claim.rs:108-121` turns a lost `registry.create` race into `Ok((false, winner))` after `validate_live`. The remaining owner-less `Conflict` codes are `config_mismatch`, `gone`, `creating`-with-a-different-request: genuine faults that must surface with their message, and every caller requeues and retries next tick anyway. Keeping that arm would add a branch no test can reach deterministically.

Why local-first, not create-first, on the receiver: creating on every relayed batch runs `claim::resolve` + `anchor::install` + `initialization::seed` + `publish` for an already-existing stream; the append-first shape creates once per stream lifetime and is the same shape as the sender, so there is exactly one copy of the logic.

Properties: no HTTP status is read anywhere (RUST-QUALITY "Domain decisions MUST NOT be reconstructed from HTTP responses"); the dead branch is gone; the ownership decision is the owners' (`CreationService`/`ShardDirectory::resolve`), followed, not re-derived.

### 2.2 Design B (alternative): owner-first pre-check

Before any local attempt: `let prefix = state.shards.prefix_for(&RouteHash::for_stream(&sref).0); if let Some(owner) = state.ownership.foreign_owner(&prefix) { relay }` else local create-if-missing + append.

- Pro: a non-owner never touches the local registry/admission path; one predicate, the same one `raw.rs:22` and `shard_directory.rs:243` use.
- Con: it re-derives routing (`prefix_for` + `foreign_owner`) in billing — a second site that must agree with `ShardDirectory::resolve`, which additionally knows about fenced/moved residents; a ring change between the pre-check and the local append still yields an owner-bearing typed failure that B would have to handle anyway (so B needs A's matching too, or turns a followable bounce into a retry-next-tick). It also changes what the two existing relay tests exercise (B never attempts locally).
- Verdict: B is a possible later optimisation layered on A if non-owner registry reads ever matter (telemetry ticks are seconds apart; they do not). Ship A.

---

## 3. Red tests (fail on current code)

All three go in `src/dst/tests/security_workload.rs` (459 lines -> ~610; ceiling 1,000; no new by-path module, so no `docs/quality/owners.json` row). Existing tests stay byte-identical (inventory diff = additions only). Helpers reused: `http_rig_build`, `HttpRigOptions`, `engine_shutdown`, `RigRuntime`, `mem`, `hreq`, `PRISMA_KEY`, `sr2_workload_jwt`. Add `use super::fixture_auth::sr2_workload_jwt;` is already imported; add nothing to fixture files (they carry sha256 pins in `docs/refactor/review-mechanisms.json`).

### 3.1 Shared rig helper (new, private to the file)

```rust
/// The two-instance JWT-only fleet the relay tests share: A owns the
/// single shard `00`; B's ring says so, B holds NO static fleet token
/// and presents `src`'s workload JWT outbound.
async fn jwt_only_fleet(
    src: crate::peer::FleetTokenSource,
) -> (
    std::sync::Arc<crate::http::AppState>,
    std::sync::Arc<crate::http::AppState>,
) {
    const PUB: &str = include_str!("../fixtures/mt-test-rsa.pub.pem");
    let now = crate::shard::now_ms() / 1000;
    let svc = std::sync::Arc::new(
        crate::auth::AuthService::new(
            crate::auth::AuthMode::Enforce,
            "https://auth.prisma.io".into(),
            "test-cell",
        )
        .unwrap(),
    );
    let mut keys = std::collections::HashMap::new();
    keys.insert(
        "wl-1".to_string(),
        crate::auth::JwksKey {
            alg: jsonwebtoken::Algorithm::RS256,
            key: jsonwebtoken::DecodingKey::from_rsa_pem(PUB.as_bytes()).unwrap(),
            fp: crate::auth::key_fp(PUB.as_bytes()),
        },
    );
    svc.publish_jwks(crate::auth::JwksSnapshot {
        keys,
        fetched_at_unix: now,
        feed_version: 1,
    })
    .unwrap();
    let store = mem();
    let (state_a, addr_a) = http_rig_build(
        store.clone(),
        RigRuntime::first(),
        HttpRigOptions {
            instance: Some("rig-a".to_string()),
            auth_service: Some(svc.clone()),
            ..Default::default()
        },
    )
    .await
    .parts();
    let (state_b, _addr_b) = http_rig_build(
        store,
        RigRuntime::incarnation(1),
        HttpRigOptions {
            instance: Some("rig-b".to_string()),
            auth_service: Some(svc),
            fleet_auth: Some((None, Some(src))),
            ..Default::default()
        },
    )
    .await
    .parts();
    state_b
        .ownership
        .set_ring_active(vec!["rig-a".to_string(), "rig-b".to_string()]);
    state_b.ownership.set_override("00", "rig-a");
    state_b.peer.set_peer("rig-a", &format!("http://{addr_a}"));
    (state_a, state_b)
}

fn telemetry_append_source() -> crate::peer::FleetTokenSource {
    std::sync::Arc::new(move |_force: bool| {
        Some(sr2_workload_jwt(
            "wl-1",
            &["telemetry-append"],
            crate::shard::now_ms() / 1000,
        ))
    })
}
```

### 3.2 RED 1 — the unprimed first append relays and lands on the owner

```rust
/// RED (review rank 21): a fleet member that does NOT own a reserved
/// stream nobody has created yet must still land its first batch on
/// the owner. Today the sender sniffs statuses: its local create is
/// refused on OWNERSHIP as 409 `not_ring_owner`, which it reads as
/// "exists", re-appends locally, gets 404 `not_found` (no
/// `streams-replay-to` on a 404) and reports
/// `system append _ops_events: 404 Not Found` — the relay branch is
/// unreachable until the owner happens to create the stream itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unprimed_system_append_relays_its_first_batch_to_the_owner() {
    let (state_a, state_b) = jwt_only_fleet(telemetry_append_source()).await;
    // NO owner-side prime: `_ops_events` exists nowhere yet.
    crate::billing::system_append(
        &state_b,
        "_ops_events",
        PRISMA_KEY,
        br#"[{"v":1,"eventId":"unprimed-relay-1","eventTimeMs":1,"eventType":"t"}]"#.to_vec(),
    )
    .await
    .expect("a non-owner's FIRST system append must relay to the owner");
    // The record is readable on the OWNER through the system path.
    let (page, _) = crate::billing::system_read(&state_a, "_ops_events", PRISMA_KEY, None)
        .await
        .expect("owner-side system read")
        .expect("the owner now holds _ops_events");
    let events: Vec<serde_json::Value> =
        serde_json::from_slice(&page).expect("system page is a JSON array");
    assert_eq!(
        events
            .iter()
            .filter(|e| e["eventId"] == "unprimed-relay-1")
            .count(),
        1,
        "the relayed batch must be durable on the owner exactly once: {events:?}"
    );
    engine_shutdown(&state_a).await;
    engine_shutdown(&state_b).await;
}
```

Exact red on current code (trace 1.7): panic at the first `.expect`:

```
a non-owner's FIRST system append must relay to the owner: "system append _ops_events: 404 Not Found"
```

### 3.3 RED 2 — a receiver that is not the owner answers the typed ownership refusal, not 404

```rust
/// RED (review rank 21, receiver half): a telemetry-append RECEIVER whose
/// own ring assigns the shard elsewhere (fleet skew) must answer the
/// creation's ownership refusal — 409 `not_ring_owner` +
/// `Streams-Replay-To` — not mask it as "exists" and report the
/// append's 404 `not_found`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn telemetry_append_receiver_reports_ownership_not_absence() {
    let (state, addr) = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            instance: Some("rig-a".to_string()),
            ..Default::default()
        },
    )
    .await
    .parts();
    state
        .ownership
        .set_ring_active(vec!["rig-a".to_string(), "rig-c".to_string()]);
    state.ownership.set_override("00", "rig-c");
    let (st, headers, body) = hreq(
        addr,
        "POST",
        "/v1/internal/telemetry-append/_ops_events",
        &[
            ("content-type", "application/json"),
            ("authorization", "Bearer dst-internal-token"),
        ],
        br#"[{"v":1,"eventId":"skew-1","eventTimeMs":1,"eventType":"t"}]"#,
    )
    .await;
    let text = String::from_utf8_lossy(&body);
    assert_eq!(st, 409, "ownership refusal must not be masked: {text}");
    assert!(text.contains("not_ring_owner"), "typed code on the wire: {text}");
    assert_eq!(
        headers.get("streams-replay-to").map(String::as_str),
        Some("rig-c"),
        "the relay target must be on the refusal: {headers:?}"
    );
    engine_shutdown(&state).await;
}
```

Exact red on current code: `create_stream` -> 409 masked -> `append` -> `prepare` -> `NotFound`:

```
assertion `left == right` failed: ownership refusal must not be masked: {"error":{"code":"not_found","message":"stream not found"}}
  left: 404
 right: 409
```

### 3.4 RED 3 — a sender that cannot relay reports code + message, not a status

```rust
/// RED (review rank 21, diagnostics): a sender with no route to the
/// owner reports the owner's typed refusal (code and message), never a
/// flattened `404 Not Found` that hides which step refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn system_append_without_a_peer_reports_the_typed_refusal() {
    let (state, _addr) = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            instance: Some("rig-b".to_string()),
            ..Default::default()
        },
    )
    .await
    .parts();
    state
        .ownership
        .set_ring_active(vec!["rig-a".to_string(), "rig-b".to_string()]);
    state.ownership.set_override("00", "rig-a");
    // No `peer.set_peer`: the owner has no published URL.
    let err = crate::billing::system_append(
        &state,
        "_ops_events",
        PRISMA_KEY,
        br#"[{"v":1,"eventId":"no-peer-1","eventTimeMs":1,"eventType":"t"}]"#.to_vec(),
    )
    .await
    .expect_err("no peer URL: the append cannot land");
    assert_eq!(
        err,
        "system append _ops_events: create not_ring_owner: shard 00 belongs to rig-a"
    );
    engine_shutdown(&state).await;
}
```

Exact red on current code:

```
assertion `left == right` failed
  left: "system append _ops_events: 404 Not Found"
 right: "system append _ops_events: create not_ring_owner: shard 00 belongs to rig-a"
```

Notes: no unbounded waits (the relay has a 20 s reqwest timeout; each test is a few seconds, well under the mutation leg's `--timeout 90`); no `json!` (macro-DSL row) — `serde_json::from_slice` + `assert!`; no `tokio::spawn`; every `unwrap`/`expect` is inside the `#[cfg(test)] mod dst_tests` tree (`src/dst/mod.rs:157-158`, `allow-unwrap-in-tests = true`).

---

## 4. Code change

Two commits. Commit 1 is a pure verbatim move (so the fix diff reads as a fix); commit 2 is the fix.

### 4.1 Commit 1 — verbatim move `system_append` out of the ceilinged `billing.rs`

**`src/billing.rs`** (2,389 lines, ceilinged; budget: must not grow).
- Delete lines 2190-2281 (the doc comment, the `// mt-lint: allow(name-param-shared-core)` marker, `pub(crate) async fn system_append { ... }`, and the blank line after it — 92 lines). Keep the section header 2186-2189 (`Fleet-safe system-stream client`) because `system_read` stays under it. `urlencode` stays (only `system_read` uses it).
- Insert after line 32 (`pub(crate) use read_spool::ReadSpool;`):

```rust

mod system_append;
pub(crate) use system_append::system_append;
```

- Net: 2,389 - 92 + 3 = **2,300** lines. No `#[expect]` in the moved function; no `#[expect]` scope in `billing.rs` covers it (all are item-scoped on other functions), so no ratchet moves. All callers (`audit.rs:302`, `ops.rs:335,617`, `fleet.rs:1137`, `billing.rs:742`, both DSTs) keep `crate::billing::system_append(...)` unchanged. A module and a function may share the name `system_append` (type vs value namespace); if the reviewer dislikes the pun, name the file `system_ledger.rs` — nothing else changes.

**`src/billing/system_append.rs`** (new, ~100 lines): module doc + `use super::is_reserved_stream;` + the function body byte-identical to `billing.rs:2190-2280` (its two `// mt-lint: allow(...)` markers included; `debug_assert!(is_reserved_stream(stream))` resolves through the import).

**`docs/refactor/architecture-policy.json`** — REQUIRED in this commit or `scripts/architecture-gate.py --check` fails with `reverse dependency growth: src/billing/system_append.rs -> crate::http: N > 0` (a new file's baseline edge count is 0, `architecture-gate.py:100,117-120`; the moved code references `crate::http::` ~8 times). Add `"src/billing/system_append.rs"` to `transport_and_composition_files` and to `transport_rationales`:

```json
"src/billing/system_append.rs": "Fleet-internal system-stream client: typed append/creation results decide create-vs-relay; one relay hop through the peer client. Owners (AppendService, CreationService) decide ownership; this file only follows them."
```

`billing.rs` itself only LOSES `crate::http` edges (baseline 43), which the fixed-commit gate allows.

The multitenancy audit (`scripts/multitenancy-audit.sh`) scans `src/*.rs`, `src/config`, `src/dst/**`, `src/bin` only and none of its patterns occur in the moved code, so `scripts/mt-audit-baseline.txt` is unchanged. `src/mt_lint.rs` DOES scan the new file (basename not in `STREAM_REF_FILES`/`SURFACE_FILES`): the two moved markers satisfy `name-param-shared-core` (`stream: &str`) and `stream-ref-construction`.

### 4.2 Commit 2 — the fix

#### `src/billing/system_append.rs` (rewritten; ~150 lines, no `#[expect]`, no ratchet)

```rust
//! Fleet-safe system-stream append (round-21 blocker 5): local first,
//! one relay hop to the ring owner.
//!
//! The decision between "create it here", "another instance owns it"
//! and "it failed" is read from the TYPED append and creation results,
//! never from a status code: a creation the ring refuses carries its
//! owner, and only an owner-bearing refusal relays. Ambiguity is safe
//! end to end because every record downstream deduplicates by
//! deterministic id / source sequence.

use super::is_reserved_stream;
use crate::application::append::{AppendCode, AppendFailure, AppendOutcome, AppendResult};
use crate::application::creation::{CreateCommand, CreationError};
use crate::http::AppState;
use axum::http::{HeaderMap, HeaderValue};
use bytes::Bytes;
use std::sync::Arc;

/// Why a local system append did not land. Typed so a caller can
/// follow an ownership bounce and otherwise report code and message.
#[derive(Debug)]
pub(crate) enum LocalFailure {
    /// The system key is not a valid stream key or header value.
    Key(String),
    Append(AppendFailure),
    Create(CreationError),
}

impl LocalFailure {
    /// The instance the ring assigned the stream to, when the refusal
    /// was ownership: the relay target.
    pub(crate) fn owner(&self) -> Option<&str> {
        match self {
            Self::Append(AppendFailure { owner, .. })
            | Self::Create(CreationError { owner, .. }) => owner.as_deref(),
            Self::Key(_) => None,
        }
    }
}

impl std::fmt::Display for LocalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Key(message) => write!(f, "invalid_key: {message}"),
            Self::Append(error) => write!(f, "append {error}"),
            Self::Create(error) => write!(f, "create {}: {}", error.code, error.message),
        }
    }
}

fn system_headers(key: &str) -> Result<HeaderMap, LocalFailure> {
    let mut hdrs = HeaderMap::new();
    hdrs.insert(
        "stream-encryption-key",
        HeaderValue::from_str(key).map_err(|e| LocalFailure::Key(e.to_string()))?,
    );
    hdrs.insert("content-type", HeaderValue::from_static("application/json"));
    Ok(hdrs)
}

async fn attempt(
    state: &Arc<AppState>,
    sref: crate::tenant::TenantStreamRef,
    hdrs: HeaderMap,
    body: Bytes,
) -> AppendResult {
    crate::http::append_typed(
        state.clone(),
        sref,
        hdrs,
        axum::body::Body::from(body),
        None,
        None,
        None,
    )
    .await
}

/// Append `body` to the reserved stream `sref` on THIS instance,
/// creating the stream on first use: the raw-surface semantics
/// (`append_typed`, `CreationService::create`) with their typed
/// results. A not-found append creates and appends again; every other
/// refusal is returned as it was decided, owner included.
pub(crate) async fn append_local(
    state: &Arc<AppState>,
    sref: crate::tenant::TenantStreamRef,
    key: &str,
    body: Bytes,
) -> Result<AppendOutcome, LocalFailure> {
    let stream_key = crate::crypto::StreamKey::from_b64(key).map_err(LocalFailure::Key)?;
    let hdrs = system_headers(key)?;
    match attempt(state, sref.clone(), hdrs.clone(), body.clone()).await {
        Err(error) if error.code == AppendCode::NotFound => {}
        result => return result.map_err(LocalFailure::Append),
    }
    state
        .creation_service()
        .create(CreateCommand {
            sref: sref.clone(),
            key: stream_key,
            content_type: Some("application/json".to_string()),
            ttl_secs: None,
            expires_at_ms: None,
            close: false,
            body: Bytes::new(),
            fork: None,
        })
        .await
        .map_err(LocalFailure::Create)?;
    attempt(state, sref, hdrs, body)
        .await
        .map_err(LocalFailure::Append)
}

/// Append to a reserved system stream from ANY fleet member: local
/// first; on an ownership refusal the body relays ONCE to the owner's
/// fleet-internal telemetry endpoint, authenticated with the fleet
/// credential and carrying the system key.
// mt-lint: allow(name-param-shared-core): system ledger under the system project; names are crate constants (_usage, _ops_*), never customer input
pub(crate) async fn system_append(
    state: &Arc<AppState>,
    stream: &str,
    key: &str,
    body: Vec<u8>,
) -> Result<(), String> {
    debug_assert!(is_reserved_stream(stream));
    let body = Bytes::from(body);
    // mt-lint: allow(stream-ref-construction): system ledger under the system project; names are crate constants
    let sref = crate::tenant::system_project().stream_ref(stream);
    let failure = match append_local(state, sref, key, body.clone()).await {
        Ok(_) => return Ok(()),
        Err(failure) => failure,
    };
    // Ownership bounce: relay once to the owner.
    match failure.owner().and_then(|owner| state.peer.url_for(owner)) {
        Some(base) => relay(state, &base, stream, key, body).await,
        None => Err(format!("system append {stream}: {failure}")),
    }
}

/// One relay hop: the owner's fleet-internal telemetry endpoint.
// mt-lint: allow(name-param-shared-core): system ledger under the system project; the name is a crate constant encoded into the relay path
async fn relay(
    state: &Arc<AppState>,
    base: &str,
    stream: &str,
    key: &str,
    body: Bytes,
) -> Result<(), String> {
    let mk = |bearer: Option<&str>| {
        let mut req = crate::http::peer_client()
            .post(format!(
                "{base}/v1/internal/telemetry-append/{}",
                crate::http::encode_stream_name_path(stream)
            ))
            .timeout(std::time::Duration::from_secs(20))
            .header("stream-encryption-key", key)
            .header("content-type", "application/json")
            .body(body.clone());
        if let Some(t) = bearer {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        req
    };
    match state.peer.send(mk).await {
        Ok(resp) if resp.status().is_success() => Ok(()),
        Ok(resp) => Err(format!("telemetry relay {stream}: {}", resp.status())),
        Err(e) => Err(format!("telemetry relay {stream}: {e}")),
    }
}
```

Lint notes: `owner()` uses ONE or-pattern arm (both fields are `Option<String>`; `AppendFailure { owner, .. }` is legal from outside its module because `..` skips the private `conflict` field) so `clippy::match_same_arms` cannot fire; `sref`/`hdrs`/`body` are consumed by the final `attempt` (no `needless_pass_by_value`, no `redundant_clone`); `relay` has exactly 5 params; functions are 10-45 lines; nesting <= 2; no `unwrap`/`expect`/`panic`; no spawn/env/statics; no glob imports; no macro DSL (`format!`/`write!`/`debug_assert!` are expression macros). Doc comments use backticks, never `[links]` (rustdoc `-D warnings`). `LocalFailure` is not proof-bearing; it exposes nothing that was private.

#### `src/billing.rs` (line-neutral edit)

Line 35: `pub(crate) use system_append::system_append;` -> `pub(crate) use system_append::{LocalFailure, append_local, system_append};`. Stays at **2,300** lines.

#### `src/http.rs` (3,372 lines, ceilinged; budget: must not grow — this edit SHRINKS it by 7)

Replace lines 1243-1272 of `internal_telemetry_append` (the Stage 7 comment, the `hdrs` build, `create_stream`, the `cst` sniff, `append`) with:

```rust
    // Stage 7 review fix: the relay RECEIVER must address the reserved
    // stream under the SYSTEM project — the same identity the sender
    // (billing::system_append) appended toward and every reader
    // (system_read, rollup_step) reads. Writing under the deployment
    // tenant here put relayed usage/ops/audit batches in a stream
    // nobody reads (route hashes include the project), silently losing
    // every batch relayed across instances.
    let Some(key) = raw_key(&headers, &state) else {
        return err_resp(
            StatusCode::BAD_REQUEST,
            "missing_key",
            "Stream-Encryption-Key required",
        );
    };
    let sref = crate::tenant::system_project().stream_ref(&name);
    match crate::billing::append_local(&state, sref, key, body).await {
        Ok(out) => render_append(Ok(out)),
        Err(crate::billing::LocalFailure::Key(m)) => {
            err_resp(StatusCode::BAD_REQUEST, "invalid_key", &m)
        }
        Err(crate::billing::LocalFailure::Append(e)) => render_append(Err(e)),
        Err(crate::billing::LocalFailure::Create(e)) => creation_error_response(e),
    }
```

23 lines replace 30 -> **3,365** lines. `raw_key` (private, `http.rs:2366`) keeps today's deployment-default-key fallback (`state.bearer.default_key()`), which `create_stream` used before, so a relay whose header is absent behaves exactly as today; `admission_maintenance::reserved_streams_append_through_a_latched_engine` (telemetry-append on a latched engine, expects 200/204) and `static_token_is_dead_in_workload_mode` (expects `!= 401`) keep passing because the reserved-stream admission skip lives in `submit.rs:48`, which `append_typed` still reaches. `internal_telemetry_append` carries no `#[expect]`; `render_append` (`#[expect(unwrap_used)]`) and `creation_error_response` are only CALLED, their scopes untouched. `.stream_ref(` in `http.rs` needs no mt-lint marker (ingress file). The fn doc at 1224-1226 stays true.

Ratcheted functions touched: **none**. (`create_stream`, `append`, `append_typed`, `render_append`, `serve_h1` are neighbours, not edited; the diff must not reflow them.)

#### `scripts/quality/mutation_owners.py` (line-neutral)

`src/http` is a critical prefix (`verification_plan.py:29-31`), so the changed `internal_telemetry_append` is mutation-tested under the `http` row, whose filters `http:: livefeed_engine_retired` run NO test that can kill the `-> Response` body mutant (`Default::default()` = 200 empty; `reserved_streams_append_through_a_latched_engine` and `static_token_is_dead_in_workload_mode` accept a bare 200). Widen the row exactly as the `fleet` row did:

```python
    owner('http', 'src/http.rs', 'http:: livefeed_engine_retired dst_tests::security_workload::'),
```

Killers: the `Default::default()` mutant returns 2xx without writing -> RED 1's read-back finds no record; RED 2's 409/`streams-replay-to` assertions also fail on a bare 200. Every other changed line in `http.rs` is a `match` arm / `let else` (cargo-mutants does not mutate those), so the diff yields exactly that one mutant class. `declared_source_map` only pins source paths, not filters, so `test_mutation_owners.py` is unaffected.

`src/billing/system_append.rs` is NOT under a critical prefix and `billing.rs` is not registered, so it is not mutation-tested; do not register it voluntarily (the mutation leg fails on ANY survivor). For the record, if the owner later registers `owner('system_append', 'src/billing/system_append.rs', 'dst_tests::security_workload::')`, the three REDs kill every viable mutant: `owner()` -> `None`/`Some("")` (RED 1 fails: no relay), `system_headers` -> `Ok(Default)` (both existing primed tests fail: `missing_key`), `==` -> `!=` on `NotFound` (RED 1 fails), `relay`/`system_append` -> `Ok(())`/`Err("")` (RED 1 read-back / expect), `Display::fmt` -> `Ok(())` (RED 3's exact string).

---

## 5. Ledgers and docs

| Row | Change | When |
| --- | --- | --- |
| `docs/refactor/architecture-policy.json` `transport_and_composition_files` + `transport_rationales` | add `src/billing/system_append.rs` (otherwise `reverse dependency growth ... -> crate::http` fails) | commit 1 |
| `scripts/quality/mutation_owners.py` `http` row | append `dst_tests::security_workload::` to the filters | commit 2 |
| `docs/refactor/test-inventory.json` | `python3 scripts/test-inventory.py --write` (3 new entries; existing hashes unchanged); `--check` in CI | commit 2 |
| `docs/refactor/WIRE-MATRIX.md:225` | `POST /v1/internal/telemetry-append/{*name}` — "reserved `_`-streams only (403 `not_system_stream`); appends under the SYSTEM project with the carried key, creating the stream on `not_found`; response is the raw append contract (200/204 + `Stream-Next-Offset`), a refused creation the raw create contract: 409 `not_ring_owner` + `Streams-Replay-To` on a non-owner receiver (never masked as existing), 400 `missing_key` / `invalid_key`." | commit 2 |
| `docs/refactor/review-mechanisms.json` | nothing — no pinned fixture (`fixture_http.rs`, `fixture_livefeed.rs`) changes | — |
| `docs/quality/owners.json` | nothing — no new by-path module, spawn, env read, static or macro DSL | — |
| `scripts/mt-audit-baseline.txt` | nothing (see 4.1) | — |
| `docs/LIVE-FEED.md` | nothing — no SSE contract change | — |
| `docs/OBSERVABILITY-BILLING-STATUS.md:21`, `docs/MULTITENANCY.md §10.4.1` | still accurate ("one Streams-Replay-To hop"); optional one-liner that the hop no longer needs the owner to have created the stream | optional |

Verification to run before push (per AGENTS.md / the traps): `python3 scripts/quality/verification_plan.py` against the actual merge base (expect `mutants: true` with `selected_mutation_owners: ["http"]`, `mutation_source_files: ["src/http.rs"]`; `src/billing/*` and `src/dst/tests/*` are not critical), then `scripts/quality.sh` (fmt, clippy `-D warnings`, rustdoc, machete, deny, architecture gate, test-inventory `--check`, review-evidence, mt-audit, `mt_lint::multitenancy_identity_lint`), then `cargo test dst_tests::security_workload::` (the three REDs plus the two primed relay tests), `dst_tests::admission_maintenance::reserved_streams_append_through_a_latched_engine`, `dst_tests::runtime_journals::`, `dst_tests::security_audit::`, `dst_tests::billing_controller::` (all consume `system_append`), and the local mutation leg for the `http` owner with the saved diff (`scripts/quality/mutation_driver.py`). CI green is only what `gh run view` says.

---

## 6. What could go wrong

**Wire compatibility (mixed fleet during rollout).**
- New sender -> old receiver (owner): old receiver creates + appends -> 2xx. Fine. New sender -> old receiver that is itself not the owner (skew): old receiver answers 404 `not_found` (masked) -> new sender `Err("telemetry relay _x: 404 Not Found")` -> requeue; identical to today.
- Old sender -> new receiver: old sender only relays when its LOCAL append bounced with `streams-replay-to` (stream already exists); the new receiver answers 2xx as before. A skewed new receiver now answers 409 + `Streams-Replay-To` instead of 404: the old sender's `Ok(resp)` non-success arm turns either into `Err` -> requeue. No second hop is ever taken by any version (relay ONCE by design), so no loops.
- Public surfaces untouched; `/v1/internal/telemetry-append` remains fleet-credential-gated (`fleet_operation_authorized` is not edited); a relay without a key now gets 400 `missing_key` from `raw_key` exactly as `create_stream` gave before; a malformed key 400 `invalid_key` as before.

**Fleet / ring skew.** The sender relays to the owner ITS ring names; if that receiver's ring disagrees, the receiver's typed refusal (now honest: 409 + the receiver's owner) makes the sender fail the tick and retry after the rings converge. Under a rebalance, `ShardDirectory::resolve` may say `Opening`/`ShardMoving` locally (typed `Unavailable`, no owner) -> `Err` string -> requeue, same as today. Standalone (no ring, `foreign_owner` = `None`): purely local, unchanged.

**Behavioural deltas to be aware of.** (1) The sender's first non-owner append now creates the stream on the owner via the relay instead of waiting for the owner's own telemetry to create it — this is the fix. (2) Owner-less `Conflict` from `create` (`config_mismatch`, `gone`, foreign `creating`) is now reported with its code instead of being retried as an append that then failed anyway; every caller requeues, so nothing is lost and the log line says why. (3) Error strings change shape (`system append _x: append not_found: stream not found`, `... create not_ring_owner: shard 00 belongs to rig-a`); grep any log-based alerting for `system append .*: 404` (I found none in `src/`).

**Flakiness.** RED 1 depends on the loopback relay and on `system_read` seeing the record immediately after the relay's 2xx; the relay response is produced only after the owner's `append_typed` acked (`submit.rs:86`), and `billing_controller::r09_active_rollup_cancels_entered_read_and_replays_ledger_once` already relies on append-then-`system_read` on one instance. Background telemetry on either rig may append its own `_ops_events`; RED 1 filters by `eventId` and asserts count == 1, so a concurrent prime by rig-a's own ops drain cannot break it (it would only make the relay land on an existing stream). All waits are bounded (20 s reqwest timeout on the relay; no polling loops), so a regression fails by assertion, never by hang.

**Gate traps to double-check at implementation time.** `cargo fmt` will reflow the new `http.rs` block — count the lines after fmt (must stay <= 3,372; expected 3,365). Keep the deleted `billing.rs` range at 2190-2281 so no double blank line remains before `system_read` (fmt fails on two consecutive blank lines). Make sure the `#[expect]` blocks around `create_stream`/`append_typed`/`render_append` are not touched by the diff (their `scope_lines`/`syntax_facts` are ratcheted). If clippy nonetheless reports `match_same_arms` on `Display::fmt` (it should not: the three `write!` calls differ), reorder nothing — split the format strings further rather than adding an `#[expect]`.

---

## Skeptic corrections

Checked first-hand against `slate @ 82095942` (read-only). The mechanism claim (§1), Design A (§2.1), the commit-1 move (§4.1), the `http.rs` edit shape (§4.2) and the ledger table (§5) hold. Every named helper exists with the stated signature: `append_typed` (`src/http.rs:2741`, 7 args, `pub(crate)`), `render_append` (`:41`), `creation_error_response` (`:2655`, takes `CreationError` by value), `raw_key` (`:2366`, private, `&HeaderMap, &AppState -> Option<&str>`), `AppState::creation_service` (`:272`, `self: &Arc<Self>`), `CreationService::create` (`src/application/creation/raw.rs:5`, `self: &Arc<Self>`), `CreateCommand` (8 fields exactly as listed), `CreationError { pub kind, pub code: &'static str, pub message, pub owner: Option<String>, pub retry_after }`, `AppendFailure { pub(crate) class, code, message, retry_after, owner: Option<String>, private conflict }` with `Display = "{code}: {message}"`, `AppendCode: PartialEq`, `PeerClient::url_for(&str) -> Option<String>`, `peer.send(impl Fn(Option<&str>) -> RequestBuilder)`, `peer_client`/`encode_stream_name_path` re-exported at `http.rs:3170`. Line references 2190-2281 / 32 / 1243-1272 are exact; `billing.rs` is 2,389, `http.rs` 3,372, `security_workload.rs` 459 (legacy ceiling 459, so the merge-base limit is 1,000). No `#[expect]` scope covers `system_append` (nearest is `tombstone_walk` at 2079, item-scoped) or `internal_telemetry_append` (no `#![...]` in http.rs). The `mt-audit` patterns (`stream_hash(`, registry literal calls, `HashMap<String` in scaler3/registry, tenant-fallback, `streams-internal-*`) do not occur in the moved code. `transport_rationales` is documentation only (no script reads it); `transport_and_composition_files` IS read (`architecture-gate.py:113`) and is required in commit 1 as the plan says. `test-inventory.py` inventories only `#[test]`/`#[tokio::test]` fns, so the helpers are not pinned. `review-mechanisms.json` pins nothing in billing.rs, http.rs or security_workload.rs.

Three things are wrong and must change; two more need a line each.

### C1 (BLOCKING) — the rig's `"00"` override does not govern `_ops_events`; RED 2 is not red and fails after the fix, RED 3's exact string is wrong

`fixture_http.rs:44` gives the rig ONE prefix, `"00"`, and `registry::shard_for_hash` (`src/registry.rs:1489`) is a longest-prefix match over the route hash's leading bits with `unwrap_or_default()` — a hash whose first two bits are not `00` lands in the EMPTY prefix `""`. Recomputed from `tenant::route_hash_input` (`route-v1` + `system` + name, length-prefixed, SHA-256[..16]; the encoding is pinned by `tenant/tests.rs:44`):

| stream | leading bits | `prefix_for` in the rig |
| --- | --- | --- |
| `_ops_events` | `1011` | `""` |
| `_usage` | `1011` | `""` |
| `_ops_metrics` | `0100` | `""` |
| `_audit_events` | `0000` | `"00"` |

(Cross-check: `reserved_streams_append_through_a_latched_engine` asserts `cust-r` and `_usage` share ONE engine; `proj-test/cust-r` is `0111` -> `""`, consistent.)

So for `_ops_events`, `OwnershipService::effective_owner("")` (`src/ownership.rs:76`) finds no override for `""` and falls to `ring_pick("", active)` (FNV-1a over `" rig-a"` / `" rig-b"`), which happens to return `rig-a` for both `[rig-a, rig-b]` and `[rig-a, rig-c]`. The two existing primed tests (and the plan's RED 1 and trace §1.7) therefore pass by rendezvous luck, not by the override; the refusal message is `shard  belongs to rig-a` (empty prefix, two spaces), not `shard 00 belongs to rig-a`.

Consequences, as written:
- **RED 2** (`telemetry_append_receiver_reports_ownership_not_absence`, override `"00" -> rig-c`, stream `_ops_events`): rig-a's `foreign_owner("")` is `None` (rendezvous says rig-a), so the receiver creates and appends LOCALLY: current code answers 201/204, not 404, and the FIXED code also answers 2xx. `assert_eq!(st, 409)` fails on both trees. Not a red test.
- **RED 3** (`system_append_without_a_peer_reports_the_typed_refusal`): red on current code as stated (`404 Not Found`), but after the fix the string is `system append _ops_events: create not_ring_owner: shard  belongs to rig-a` — the exact `assert_eq!` against `shard 00 belongs to rig-a` fails after the change.
- **RED 1** works, but only through the same rendezvous coincidence.

Fix: use `_audit_events` in all three REDs (it is also the stream the review item names). Then the override is what decides, deterministically, and the prefix in the message is `00`:

```rust
// RED 1: "_ops_events" -> "_audit_events" in the sender call and the owner-side read,
//        eventId stays unique; red on current code:
//   a non-owner's FIRST system append must relay to the owner: "system append _audit_events: 404 Not Found"
// RED 2: "/v1/internal/telemetry-append/_ops_events" -> "/v1/internal/telemetry-append/_audit_events";
//        red on current code is then exactly the plan's
//   assertion `left == right` failed: ownership refusal must not be masked: {"error":{"code":"not_found","message":"stream not found"}}
//     left: 404
//    right: 409
//        (create_stream -> 409 not_ring_owner + streams-replay-to: rig-c, masked by the cst sniff, append -> 404)
// RED 3: "_ops_events" -> "_audit_events"; expected after the fix:
assert_eq!(
    err,
    "system append _audit_events: create not_ring_owner: shard 00 belongs to rig-a"
);
//        red on current code: left: "system append _audit_events: 404 Not Found"
```

Also correct §1.7 step 2 and §3.2's doc comment: on the current tree the refusal comes from `foreign_owner("00")` only for `_audit_events`; for `_ops_events` it is `foreign_owner("")` via rendezvous. Optionally note in the existing two primed tests that their `set_override("00", "rig-a")` is inert (do NOT edit them — their inventory hashes must stay unchanged).

The rig has no background telemetry/audit/ops drain (`http_rig_build` spawns only the h1 server), so no concurrent prime can flake RED 1 in either direction; §6 "Flakiness" can drop that concern.

### C2 (BLOCKING) — the new `http.rs` block turns `POST /v1/internal/telemetry-append/__ds` from 400 `reserved` into a handler panic

`is_reserved_stream` accepts anything starting with `_`, including `__ds` and `__ds/...`. Today `create_stream` (`http.rs:2513-2521`) refuses those with 400 `reserved` BEFORE any `stream_ref`. The plan's replacement calls `crate::tenant::system_project().stream_ref(&name)` first; `ProjectId::stream_ref` (`src/tenant.rs:166`) is `CanonicalStreamName::new(..).expect(..)` and `CanonicalStreamName::new` returns `Err(ReservedRoot)` for `__ds` (`tenant.rs:319`) — a panic in a fleet-credentialed handler, i.e. a wire regression (400 -> connection abort) and a `panic` outside tests. (The same constructor also rejects `.`/`..`/empty components; today those already reach `create_stream`'s own `stream_ref` at 2623, so that part is latent, not introduced.)

Fix (line-neutral within the budget: +3 lines, `http.rs` 3,365 -> 3,368 <= 3,372), replace the single `let sref = ...stream_ref(&name);` line with a fallible construction:

```rust
    let Ok(canonical) = crate::tenant::CanonicalStreamName::new(&name) else {
        return err_resp(StatusCode::BAD_REQUEST, "invalid_name", "not a canonical stream name");
    };
    let sref = crate::tenant::TenantStreamRef::new(crate::tenant::system_project(), canonical);
```

Both constructors are `pub(crate)` (`tenant.rs:293`, `:351`); `TenantStreamRef::new(` is not an mt-lint pattern (only `.stream_ref(` is, and http.rs is an ingress file anyway). A `let-else` yields no cargo-mutants mutant (see C3), so no killer is REQUIRED by the leg, but add one assertion to RED 2's test (same rig, before the shutdown) as the regression guard and wire proof:

```rust
    let (st, _, body) = hreq(
        addr,
        "POST",
        "/v1/internal/telemetry-append/__ds",
        &[("content-type", "application/json"), ("authorization", "Bearer dst-internal-token")],
        br#"[]"#,
    )
    .await;
    assert_eq!(st, 400, "the __ds root stays refused, never constructed: {}", String::from_utf8_lossy(&body));
```

(On the current tree this assertion passes too — it is a guard, not a red.) Add `400 invalid_name` (non-canonical name, including the `__ds` root) to the WIRE-MATRIX row in §5; the old row's `400 reserved` for `__ds` becomes `400 invalid_name`.

### C3 — the mutation rationale in §4.2 is wrong for the pinned cargo-mutants; the conclusion survives only because the new `match` has no `_` arm and no guard

`quality-tools.toml` pins `cargo-mutants = "27.1.0"`, whose `Genre` enum (registry copy, `src/mutant.rs:23`) is `FnValue, BinaryOperator, UnaryOperator, MatchArm, MatchArmGuard, StructField`: it DOES delete match arms (every non-wildcard, unguarded arm of a `match` that HAS a `_` catch-all), replaces every match guard with `true` and with `false`, deletes `!`/unary `-`, and deletes fields of struct literals that carry `..base`. `--in-diff` selects a mutant if any line of its span is an inserted line, the line before a deletion or the line after one (`in_diff.rs:213`).

For the `http.rs` block as planned (with or without C2's `let-else`): no `_` arm, no guard, no operator, no `..base` literal, so the only in-diff mutant is `internal_telemetry_append -> Default::default()` (200 empty). It is killed by `static_token_is_dead_in_workload_mode` (expects 401), `rotated_workload_jwt_refreshes_and_retries` (expects exactly one forced refresh; a 200-always receiver never 401s) and RED 2 (409) — all under the widened `http` row, which is therefore still REQUIRED. Rule for the implementer: do not add a `_ =>` arm, a guard, or `..Default::default()` to that block, or each arm/field becomes its own mutant needing its own killer.

For `src/billing/system_append.rs` (correctly not registered), the "if the owner later registers" paragraph misses the two `MatchArmGuard` mutants on `Err(error) if error.code == AppendCode::NotFound`: guard `-> false` is killed by RED 1 (NotFound no longer creates), but guard `-> true` SURVIVES all planned tests (an owner-bearing `NotOwner` from the append path would go through `create`, which refuses with the same owner, and still relays). If that file is ever registered, add a killer: an existing system stream appended with a WRONG key from the same instance must report `system append _x: append wrong_key: key mismatch` (guard-true reports `create wrong_key: ...`).

### C4 — `#[expect]`-ratchet and ceiling facts, confirmed

- `wc -l`: `src/billing.rs` 2,389 (merge-base limit 2,389; legacy 3,039), `src/http.rs` 3,372 (legacy 3,382 + 2 bounded = 3,384, so the merge-base 3,372 is the binding limit), `src/dst/tests/security_workload.rs` 459 (limit 1,000). New file limit 1,000. The plan's arithmetic (2,300 / 3,365 -> 3,368 with C2 / ~640 with C1+C2) is within every limit.
- Functions with `#[expect]` that the diff must not touch or reflow: `render_append` (:37), `create_stream` (:2498), `append` (:2709), `append_typed` (:2735), `serve_h1` (:1278, directly BELOW the edited handler — keep the blank line at 1274 so the hunk ends at the handler's closing brace). None is edited; no reason text needs re-deciding.
- `PeerClient::url_for` carries `#[expect(clippy::unwrap_used)]`; it is only CALLED, its scope is untouched.

### C5 — small exactness fixes to the plan text

- §4.1: the moved function carries THREE `mt-lint` markers (one `name-param-shared-core` at 2196, two `stream-ref-construction` at 2214 and 2242), not two; a verbatim move carries all of them, and commit 2's rewrite needs exactly the three it shows (two `name-param-shared-core`, one `stream-ref-construction`). `mt_lint` keys files by path relative to `src/` (`billing/system_append.rs`), so the SURFACE/STREAM_REF exemptions of `http.rs` do not apply to it — the markers are needed, as planned.
- §2.1 / §4.2: `LocalFailure::owner()`'s or-pattern binds `owner: &Option<String>` in both alternatives (`AppendFailure.owner` is `pub(crate) Option<String>`, `CreationError.owner` is `pub Option<String>`), so it compiles; the `..` legitimately skips the private `conflict`. Keep the or-pattern: two textually identical `error.owner.as_deref()` arms would risk `match_same_arms`.
- §4.2 `internal_telemetry_append`: `raw_key(&headers, &state)` with `state: Arc<AppState>` deref-coerces exactly as `create_stream:2522` already does; the returned `&str` borrows `headers`/`state` immutably while `append_local(&state, ..)` borrows `state` immutably — no conflict.
- §6 wire skew: also state explicitly that the receiver's `Streams-Replay-To` on a skewed non-owner is the RECEIVER's owner name; the sender never follows a second hop (unchanged), so a ring disagreement still resolves by requeue after convergence.
- §5 verification: the `http` mutation leg's per-mutant test run now includes all of `dst_tests::security_workload::` (8-9 tests, ~15 s dominated by `raw_sse_terminates_at_workload_token_expiry`), well under the driver's `--timeout 90`; the `--baseline run` must pass on the unmutated tree first, so run the three REDs green locally before invoking `mutation_driver.py`.
