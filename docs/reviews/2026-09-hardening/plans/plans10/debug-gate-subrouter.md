# Item 44: one gate for /v1/debug, a nested sub-router instead of twelve copies

**Tree.** `slate` @ `2fb92fb9`. The task text says HEAD is five commits ahead of `origin/slate` = `729c52ac`, but that is stale. `refs/remotes/origin/slate` was updated by a push to `2fb92fb9` during this session (`git reflog show origin/slate`: `2fb92fb9 … update by push`). The merge base is therefore HEAD. No file this plan touches changed in the five commits, and `src/http.rs` is 3,362 lines both at `729c52ac` and at HEAD. I read everything and ran nothing, because a mutation run owns the tree.

**Plan in brief.** Two commits.

- **C1 (item 44).** A new `src/http/debug.rs` owns the gate: `gated(state, table)` gives the debug table its own fallback and layers `require_deployment_bearer` over it. `router()` mounts that result with `.nest("/v1/debug", debug::gated(&state, debug_routes()))`. The debug table becomes its own function, `debug_routes()`, which stays in `http.rs` at the same lines and indentation. The twelve 7-line copies, the twelve `headers` parameters and `let _ = &q;` are deleted, and the false comment is rewritten.
- **C2 (the reviewer's "separate commit").** `fleet_operation_authorized` becomes `fleet_operation_authorization(..).is_some()`, and `workload_jwt_operation` is deleted. The Off-mode refusal moves into the typed function. That move is what makes it a pure refactor. The reviewer's one-liner as written is not one (§1.5).

**Where the reviewer's Change is wrong or unbuildable, and what replaces it:**

1. **"Move the handlers into src/http/debug.rs".** It compiles, but the CI mutation leg cannot pass it. A moved body is all inserted lines, so `--in-diff` selects every operator in it. `debug_load` alone carries about 20 (`a.0 + v.0`, `a.1.max(v.1)`, `now - RUNTIME_LAST_TICK_MS`, `as f64 / 1048576.0`, …). The absorb closure has `v / 1048576` three times over `/sys/fs/cgroup` reads, which are `None` in a rig, so those mutants cannot be killed. The leg fails on any MISSED mutant.
   - **Replacement.** The gate moves to `debug.rs`. The table and the handler bodies stay where they are. The table is split out of `router()` at its existing position and indentation, so its lines do not move (§4, C1).
2. **"A DEBUG_ROUTES table".** An axum `Router` cannot be enumerated. A `const [(path, fn() -> MethodRouter)]` would force the four closures out into named functions, which is another move and selects their mutants, and it adds indirection the gate does not need.
   - **Replacement.** The nested router is the table. The test proves the property that matters, that the gate covers every path under the prefix, by probing a path nothing routes (§3). A literal list of the 12 routes in the test pins that each existing route is inside the nest.
3. **The reviewer asks that an unknown `/v1/debug` path be 401, but `.layer` on a nest does not deliver that.** In axum 0.8.9, `Router::nest` drops a nested router's *default* fallback (`routing/mod.rs:227`: `if !default_fallback { fallback_router.nest(..) }`), so unrouted paths reach the outer, unlayered 404. axum's own test `tests/fallback.rs::with_middleware_on_inner_fallback` shows an inner layer not running for `/foo/bar`.
   - **Replacement.** `gated` sets an explicit `.fallback(..)` *before* `.layer(..)`.
4. **`fleet_operation_authorized = fleet_operation_authorization(..).is_some()` as written is a behaviour change.** Only the boolean path refuses workload JWTs in Off mode (§1.5). The Off guard has to move into the typed function first.

---

## 1. Problem (verified on the current tree)

### 1.1 The copied gate: 12 handlers, one 7-line block each

Every `/v1/debug` handler starts with this block. It is byte-identical apart from indentation: 4 spaces in the named handlers, 20 in the router closures.

```rust
    if !authorized(&state, &headers) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
```

| # | Route (registration) | Handler | The copy |
|---|---|---|---|
| 1 | `GET /v1/debug/timings` (`http.rs:1394`) | `debug_timings` | `:1664-1670` |
| 2 | `GET /v1/debug/load` (`:1395`) | `debug_load` | `:826-832` |
| 3 | `GET /v1/debug/store` (`:1396`) | `debug_store` | `:1041-1047` |
| 4 | `GET /v1/debug/usage` (`:1397`) | `debug_usage` | `:1083-1089` |
| 5 | `GET /v1/debug/auth` (`:1398`) | `debug_auth` | `:1063-1069` |
| 6 | `GET /v1/debug/ops-events` (`:1399`) | `debug_ops_events` | `:1149-1155` |
| 7 | `GET /v1/debug/usage-reconcile` (`:1400`) | `debug_usage_reconcile` | `:1174-1180` |
| 8 | `POST /v1/debug/absorb-pause` (`:1406-1425`) | closure in `router` | `:1412-1418` |
| 9 | `POST /v1/debug/abort` (`:1432-1459`) | closure in `router` | `:1436-1442` |
| 10 | `GET /v1/debug/sleep` (`:1460`) | `debug_sleep` | `:729-735` |
| 11 | `POST /v1/debug/history-stall` (`:1465-1484`) | closure in `router` | `:1471-1477` |
| 12 | `GET /v1/debug/absorb` (`:1490-1597`) | closure in `router` | `:1494-1500` |

**What the grep shows:**

- `grep -n 'if !authorized(&state, &headers)' src/http.rs` finds 13 sites: these 12 plus `:1913` (`billing_readiness_axum`, `/operator/billing.json`, message "operator bearer required", not a debug route).
- `grep -rn '"/v1/debug' src` outside `http.rs` finds only doc comments, the pilot (`bin/pilot/benchmark/config.rs:112`, `proxy.rs:204`) and DST callers. No other router registers a debug path.
- Each `headers: HeaderMap` parameter exists only for this block: `:726`, `:822`, `:1038`, `:1062`, `:1082`, `:1148`, `:1172`, `:1663`, and the closures at `:1410`, `:1435`, `:1469`, `:1493`.

**There is no layer.** `router()` (`http.rs:1336-1643`) has exactly two `.layer` calls:

- `track_inflight` at `:1616`, which does no authentication. Its own comments say auth moved *out* of pre-auth middleware.
- the origin-marker `map_response` at `:1629`.

### 1.2 The false comments

- **`http.rs:1080-1081`**, on `debug_usage`: `/// Per-stream usage counters + the active limits. Auth: same bearer as` / `/// the other debug endpoints (enforced by the middleware layer).` This is false: no middleware layer authenticates anything.
- **`http.rs:1401-1405`**: `// Every /v1/debug/* route is account-gated (round-19 … ) SR-5: /operator is bearer-gated like the debug surface.` This one does not claim a middleware. It claims a property that holds only because the block was copied into every handler.
- The reviewer's "`http.rs:1083-1084 and 1407-1411`" are these two comments on an older numbering. `grep -rn -i middleware src` finds no other claim of this kind.

### 1.3 Nothing tests an unauthenticated debug request

- **No DST test sends a debug request without a token to a rig that has one.** `git grep -n '/v1/debug' src/dst` finds these callers:
  - `security_modes.rs:202`, which sends the bearer and expects 200
  - `runtime_open_gate.rs:731,744,757`
  - `livefeed_engine_retired.rs:86,126`
  - `billing_usage.rs:674`
  - `admission_maintenance.rs:719,755`
- Every caller except `security_modes.rs:202` sends no token to a rig that has none configured. `DeploymentBearer::authorizes` (`deployment_bearer.rs:26-33`) is `None => mode == AuthMode::Off`, so all of those are authorized. That is the "Off mode authorizes everything" posture from SR-5, and it is why deleting any one copy would pass the whole suite.
- `src/http/tests.rs` has no debug test.
- `bench/fleet/ci-fanout.sh:43` checks `POST /v1/debug/absorb-pause` → 401, but no workflow runs it. `ci.yml` runs only `bench/fleet/livefeed-cert.sh`.

### 1.4 "Regressed once already", and the debris

- **The regression.** `8fd308eb` (MF1) says: "Also closes the documented-vs-actual auth gap: every /v1/debug/* route is now bearer-gated (absorb-pause and sleep MUTATE state…)". Before that commit the docs claimed a gate that did not exist. The same commit wrote the "enforced by the middleware layer" comment, which was already false then.
- **The debris.**
  - `http.rs:825` `let _ = &q;` is still there. It exists only to silence the unused `Query` extractor at `:823`: `debug_load` never reads a query.
  - The reviewer's other debris, `1054-1058` (an empty `if let Some(_obj) = snap.as_object_mut() { /* comment */ }` in `debug_store`), was already deleted by `a1cf29f3`.

### 1.5 The fleet_operation_authorized pair (C2)

At `http.rs:445-519` there are two verifications of the same workload JWT:

- `fleet_operation_authorization` (`:445-470`): static bridge, else `verify_internal` + `operations` filter → `Option<RawSurfaceAuth>`. It has **no** Off-mode check, and its comment points at the other function: `// SR3-1: exclusive modes at runtime (see fleet_operation_authorized).`
- `workload_jwt_operation` (`:489-499`): `if state.auth.mode == crate::auth::AuthMode::Off { return false; }`, then the same verify-and-filter, as a `bool`.
- `fleet_operation_authorized` (`:507-519`) returns `static_ok || workload_jwt_operation(..)`.

**Use sites:**

- `fleet_operation_authorization` is called only from `raw_surface_authorization`, and only in its `Enforce` arm (`:440`).
- `fleet_operation_authorized` is called at `http.rs:1229` (telemetry-append), `:3202` (segment-close) and `:3279` (segment-read), and at `product.rs:3338`, `:3433` and `:3515`.
- `workload_jwt_operation` is called only from `:518`.

**Why the literal one-liner changes behaviour.** In Off mode the boolean gate refuses every workload JWT. The typed gate would verify it. `verify_internal` (`auth.rs:780-807`) does not check the mode. Production Off mode never publishes JWKS (`bootstrap.rs:752` starts the feed only when `auth_mode != Off`), so there the two agree by accident. A rig can publish JWKS on an Off-mode service, though: `publish_jwks` has no mode check (`auth/publication.rs:318`). The explicit guard at `:490` is the stated posture. Moving it into the typed function keeps the fold exact:

- `raw_surface_authorization` calls the typed function only under `Enforce`, where the guard is false.
- The six boolean callers keep the Off refusal.

---

## 2. Contract decision

### C1: typed contract

- **One authorization function** decides every request under `/v1/debug`: `debug::require_deployment_bearer`, which calls `http::authorized`, which calls `DeploymentBearer::authorizes`. It covers each routed handler under any method, every path nothing routes, and the bare `/v1/debug`.
- **It runs as a layer on the nested router, before anything else**: before method routing, extractors and the handler. No handler under `/v1/debug` reads the bearer.
- **The gate is structural.** It is a `nest`, never a `path.starts_with` check, and its verdict is the existing `authorized()` boolean. No string matching is introduced.
- **The Off-mode posture is unchanged:** with no bearer configured, the debug surface is open (SR-5 local development).

### C1: wire

| Request | Today | After |
|---|---|---|
| A routed debug path, correct method, no token or wrong token | 401 `{"error":{"code":"unauthorized","message":"bearer token required"}}` | **identical bytes** |
| Same, with the token | the handler's answer | **identical** |
| An unrouted path (`/v1/debug/nope`) or bare `/v1/debug`, no token | 404, empty body | **401** in the envelope above |
| A routed path under the wrong method (`GET /v1/debug/abort`), no token | 405 + `Allow` | **401** in the envelope above |
| Unrouted or wrong method, with the token | 404 empty, or 405 + `Allow` | **identical** (the fallback returns a bare `StatusCode::NOT_FOUND`, the same bytes as axum's `NotFound`; the method router's own 405 still answers behind the gate) |
| `/v1/debug/` (trailing slash, empty tail) | outer 404 | outer 404 (axum registers the nested fallback at `/v1/debug` and `/v1/debug/{*tail}`; a catch-all never matches an empty tail) |

- **No metric, JSON shape or body text changes.**
- **Extractor ordering changes.** The gate now runs before the handler's `Query<HashMap<String, String>>` and `RawQuery` extractors, which ran first before. Neither can reject: `serde_urlencoded` into `HashMap<String, String>` decodes lossily and never errors, and `RawQuery` is infallible. So no observable ordering changes.
- The only wire change is the two **401** rows. They are **Decision D1** (§9), which also gives the byte-compatible alternative.

### C2: no wire change

The boolean internal gate stays equal to `static_ok || (mode != Off && JWT names op)` in every mode, and the typed gate is unchanged under `Enforce`, its only caller.

---

## 3. Red tests and pinning tests

All three new tests are DST scenarios, and every one of them is inside a mutation owner's filter (§5).

### 3.1 RED (C1): `dst::dst_tests::security_routes::debug_surface_refuses_every_path_without_the_token`

**Home.** `src/dst/tests/security_routes.rs`, inserted after `product_requires_the_account_token` (which ends at `:553`). It is the product-surface twin, and the file already has `http_rig_auth(store, token)` at `:12-26` and imports `preq`, `engine_shutdown` and `mem`. The reviewer's "beside security_modes.rs" is the same directory; `security_modes.rs` (827 lines) has no bearer-configured rig helper.

```rust
/// Item 44: /v1/debug is ONE gated sub-router, not a check each handler
/// must remember (MF1 once found the documented gate missing). With an
/// account token configured, every debug path (a routed handler under
/// its own and the other method, a path nothing routes, the bare
/// prefix) refuses a caller without the token before a handler, a 404
/// or a 405 can say what exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn debug_surface_refuses_every_path_without_the_token() {
    let (state, addr) = http_rig_auth(mem(), "s3cret").await;
    let routed = [
        ("GET", "/v1/debug/timings"),
        ("GET", "/v1/debug/load"),
        ("GET", "/v1/debug/store"),
        ("GET", "/v1/debug/usage"),
        ("GET", "/v1/debug/auth"),
        ("GET", "/v1/debug/ops-events"),
        ("GET", "/v1/debug/usage-reconcile"),
        ("POST", "/v1/debug/absorb-pause?on=1"),
        ("POST", "/v1/debug/abort"),
        ("GET", "/v1/debug/sleep"),
        ("POST", "/v1/debug/history-stall"),
        ("GET", "/v1/debug/absorb"),
    ];
    let unrouted = [
        ("GET", "/v1/debug/nope"),
        ("POST", "/v1/debug/nope"),
        ("GET", "/v1/debug"),
    ];
    let other_method = routed.map(|(m, p)| (if m == "GET" { "POST" } else { "GET" }, p));
    for (method, path) in routed.into_iter().chain(unrouted).chain(other_method) {
        for bearer in [None, Some("Bearer wrong")] {
            let headers: Vec<(&str, &str)> =
                bearer.map(|b| ("authorization", b)).into_iter().collect();
            let (st, _, body) = preq(addr, method, path, &headers, b"").await;
            let text = String::from_utf8_lossy(&body);
            assert_eq!(st, 401, "{method} {path} with {bearer:?} must be refused: {text}");
            assert!(text.contains(r#""code":"unauthorized""#), "{method} {path}: {text}");
        }
    }
    assert!(
        !state.runtime.history.paused.load(std::sync::atomic::Ordering::Relaxed),
        "an unauthenticated absorb-pause landed"
    );
    engine_shutdown(&state).await;
}
```

The `history-stall` probe carries no `?ms=`, so even a mutant that let it through would store 0, never a stall.

**Expected RED on the current tree** (the test added alone on `2fb92fb9`), traced:

1. **The 12 routed probes × 2 bearers pass.** Each handler's first statement is the copy, and `authorized` is false for `None` and for `"Bearer wrong"` (`secret_eq` mismatch). The body is `{"error":{"code":"unauthorized","message":"bearer token required"}}`. The `Query` extractor on absorb-pause runs first and accepts `on=1`.
2. **The first unrouted probe fails.** `GET /v1/debug/nope` with `None` matches no route, and no outer route shares the prefix. The outer default fallback, axum's `NotFound`, answers 404 with an empty body, and `track_inflight` passes it through. The failure is:

```
thread 'dst::dst_tests::security_routes::debug_surface_refuses_every_path_without_the_token' panicked at src/dst/tests/security_routes.rs:<line>:<col>:
assertion `left == right` failed: GET /v1/debug/nope with None must be refused: 
  left: 404
 right: 401
```

The message ends at `refused: ` because the 404 body is empty. If the unrouted probes were skipped, the first `other_method` probe (`POST /v1/debug/timings`) would give `left: 405`.

**Green after C1:**

- `/v1/debug/nope` and bare `/v1/debug` hit the nested fallback, which is layered, so the gate answers 401.
- A wrong method reaches the method router's fallback. `MethodRouter::layer` wraps `fallback` too (`method_routing.rs:990`), so again 401.

### 3.2 PIN (C1): `dst::dst_tests::security_routes::debug_surface_serves_every_handler_with_the_token`

Same file, directly after 3.1. It passes on the current tree and after C1, which proves authorized callers see no wire change. It also kills every handler-body mutant C1 selects (§5).

```rust
/// Item 44 pin: the gate lets the account token through to every
/// handler unchanged, judged by each handler's own answer (an empty 200
/// is a failure), and an authorized caller keeps the bare 404 for an
/// unrouted path and the 405 for a wrong method.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn debug_surface_serves_every_handler_with_the_token() {
    let (state, addr) = http_rig_auth(mem(), "s3cret").await;
    // A published policy gives /v1/debug/auth a feed age to report.
    rig_publish_policy(&state.auth, rig_policy("proj_dbg", "ws_dbg", 1, 1), 1).unwrap();
    for (path, field) in [
        ("/v1/debug/timings", None),
        ("/v1/debug/load", Some("inflight_now")),
        ("/v1/debug/store", None),
        ("/v1/debug/usage", Some("limits")),
        ("/v1/debug/ops-events", Some("events")),
        ("/v1/debug/absorb", Some("budget")),
    ] {
        let (st, v) = debug_json(addr, "GET", path).await;
        assert!(st == 200 && v.is_object(), "{path} -> {st}: {v}");
        if let Some(f) = field {
            assert!(!v[f].is_null(), "{path} lacks {f}: {v}");
        }
    }
    let (st, v) = debug_json(addr, "GET", "/v1/debug/auth").await;
    assert_eq!((st, &v["shadow"]["mode"]), (200, &serde_json::Value::from("off")), "{v}");
    let age = v["feeds"]["policies"]["ageSecs"].as_i64();
    assert!(age.is_some_and(|a| (0..=60).contains(&a)), "policy age in whole seconds: {v}");
    assert_eq!(v["feeds"]["policies"]["stale"], false, "{v}");
    let (st, v) = debug_json(addr, "GET", "/v1/debug/usage-reconcile").await;
    assert_eq!((st, &v["error"]["code"]), (503, &serde_json::Value::from("rollup_unavailable")));
    for on in [true, false] {
        let path = format!("/v1/debug/absorb-pause?on={}", u8::from(on));
        let (st, v) = debug_json(addr, "POST", &path).await;
        assert_eq!((st, &v["absorb_paused"]), (200, &serde_json::Value::from(on)));
        assert_eq!(state.runtime.history.paused.load(std::sync::atomic::Ordering::Relaxed), on);
    }
    // The rig never sets STREAMS_DEBUG_EXIT, so abort must refuse.
    let (st, v) = debug_json(addr, "POST", "/v1/debug/abort").await;
    assert_eq!((st, &v["error"]["code"]), (403, &serde_json::Value::from("disabled")));
    // history-stall is not called: it sets a PROCESS-wide flush stall
    // (history::HISTORY_FLUSH_STALL_MS) that other rigs in the run share.
    let ok = [("authorization", "Bearer s3cret")];
    let (st, _, body) = preq(addr, "GET", "/v1/debug/sleep?ms=1", &ok, b"").await;
    assert_eq!((st, body.as_slice()), (200, &b"ok"[..]));
    let (st, _, body) = preq(addr, "GET", "/v1/debug/nope", &ok, b"").await;
    assert_eq!((st, body.len()), (404, 0), "an unrouted debug path stays the bare 404");
    let (st, _, _) = preq(addr, "GET", "/v1/debug/abort", &ok, b"").await;
    assert_eq!(st, 405, "a wrong method stays 405 for an authorized caller");
    engine_shutdown(&state).await;
}

/// One debug request with the rig's token; the answer must be JSON.
async fn debug_json(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
) -> (u16, serde_json::Value) {
    let (st, _, body) = preq(addr, method, path, &[("authorization", "Bearer s3cret")], b"").await;
    let v = serde_json::from_slice(&body).unwrap_or_else(|e| {
        panic!("{method} {path} -> {st}: not JSON ({e}): {}", String::from_utf8_lossy(&body))
    });
    (st, v)
}
```

**What the rig provides for each assertion:**

- **Auth.** `http_rig_auth` gives an Off-mode `AuthService` and `DeploymentBearer::new(Some("s3cret"))`. `rig_publish_policy` (`fixture_auth.rs:205`) stamps `fetched_at_unix = now_s`, and `publish_policies` checks no mode.
  - `feed_json` (`auth.rs:876-903`) reports `ageSecs = now − fetched`, which is 0 or 1, and `stale = false` (the limit is 300).
- **abort.** `debug_exit` defaults to false (`config/model.rs:431`; `fixture_config` applies no env overlay), so abort answers 403 `disabled`.
- **usage-reconcile.** The rig's `RollupSlot::default()` has no rollup, so usage-reconcile answers 503 `rollup_unavailable`.
- **absorb-pause.** `paused` is per rig: `RuntimeCaps::with_config` builds a fresh `HistoryResources` (`runtime.rs:185`).
- **The expected fields exist** at `http.rs:873` (`inflight_now`), `:1114` (`limits`), `:1158` (`events`) and `:1541` (`budget`).

**Imports.** Add `use super::fixture_auth::{rig_policy, rig_publish_policy};` to `security_routes.rs`. The tests avoid `json!`, so no macro-dsl ledger row is needed.

### 3.3 PIN (C2): `dst::dst_tests::security_workload::off_mode_internal_routes_refuse_workload_jwts`

Appended to `src/dst/tests/security_workload.rs` after `:763`. Add `http_rig_with_auth_service` to the `fixture_http` import.

```rust
/// Off mode is the deployment-bearer posture: even a workload JWT this
/// cell could verify is refused on the fleet-internal surface, which
/// takes only the static bridge token there. Pins the refusal through
/// the fold of the boolean internal gate into the typed one (the typed
/// gate had run only under enforce).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn off_mode_internal_routes_refuse_workload_jwts() {
    const PUB: &str = include_str!("../fixtures/mt-test-rsa.pub.pem");
    let now = crate::shard::now_ms() / 1000;
    let svc = std::sync::Arc::new(
        crate::auth::AuthService::new(
            crate::auth::AuthMode::Off,
            "https://auth.prisma.io".into(),
            "test-cell",
        )
        .unwrap(),
    );
    let mut keys = std::collections::HashMap::new();
    keys.insert(
        "off-1".to_string(),
        crate::auth::JwksKey {
            alg: jsonwebtoken::Algorithm::RS256,
            key: jsonwebtoken::DecodingKey::from_rsa_pem(PUB.as_bytes()).unwrap(),
            fp: crate::auth::key_fp(PUB.as_bytes()),
        },
    );
    svc.publish_jwks(crate::auth::JwksSnapshot { keys, fetched_at_unix: now, feed_version: 1 })
        .unwrap();
    let (state, addr) = http_rig_with_auth_service(mem(), svc).await;
    // `live` is refused right after authorization, so 400 = authorized.
    let path = "/v1/internal/segment-read/offjwt?live=sse";
    let jwt = format!("Bearer {}", sr2_workload_jwt("off-1", &["segment-read"], now));
    let (st, _, _) = hreq(addr, "GET", path, &[("authorization", jwt.as_str())], b"").await;
    assert_eq!(st, 401, "Off mode verified a workload JWT on the internal surface");
    let fleet = ("authorization", "Bearer dst-internal-token");
    let (st, _, body) = hreq(addr, "GET", path, &[fleet], b"").await;
    assert_eq!(st, 400, "the static bridge reaches the route: {}", String::from_utf8_lossy(&body));
    engine_shutdown(&state).await;
}
```

**Current tree and after C2, it passes:**

- **JWT request.** `internal_segment_read` (`http.rs:3272-3294`) → `fleet_operation_authorized`. `static_ok` is false; the Off guard refuses → `internal_unauthorized()` → 401.
- **Static bridge request.** `dst-internal-token` is the rig default (`fixture_http.rs:441`), so the request is authorized and reaches `params.live.is_some()` → 400 `live_unsupported`.

**Under the reviewer's literal one-liner it FAILS:**

- `verify_internal` accepts the JWT: issuer, `aud prisma-streams-internal` and `cell test-cell` all match `sr2_workload_jwt_exp` in `fixture_auth.rs:148-176`, and the ops include `segment-read`.
- The failure is `assertion `left == right` failed: Off mode verified a workload JWT on the internal surface` / `left: 400` / `right: 401`.

**Compile-level proof for C2.** `workload_jwt_operation` is deleted, so any remaining caller fails to build. `fleet_operation_authorized`'s signature is unchanged, so its six callers compile untouched.

### 3.4 Existing tests that pin C1 and C2

- `dst::dst_tests::security_modes::shadow_mode_observes_without_enforcing`: the bearer on `/v1/debug/auth` → 200.
- `dst::dst_tests::runtime_open_gate::debug_store_reports_this_runtimes_shard_opens`
- `dst::dst_tests::livefeed_engine_retired::*`: `/v1/debug/load` in an Off rig with no token → 200, which pins the Off posture through the gate.
- `dst::dst_tests::billing_usage` (`:674`, ops-events)
- `dst::dst_tests::admission_maintenance` (`:719`, `:755`, load)
- `dst::dst_tests::security_workload::*`: all eleven. `workload_jwt_operations_scope_the_internal_surface`, `jwt_only_fleet_relay_succeeds` and `static_token_is_dead_in_workload_mode` exercise both gates under enforce.

---

## 4. Edits, file by file, in commit order

### Budgets

**Current `wc -l` of the ceilinged files.** `http.rs` 3,362; `product.rs` 4,205; `shard.rs` 3,196; `billing.rs` 2,201; `history.rs` 1,713; `auth.rs` 1,676; `registry.rs` 1,492; `sse/feed.rs` 1,170; `fleet.rs` 1,143. Only `http.rs` is touched.

**`http.rs` in C1.**

| Change | Lines |
|---|---|
| The `router` `#[expect]`, 5 lines, replaced by a 2-line doc | −3 |
| The 4 closure copies | −28 |
| 2 closure `headers` lines | −2 |
| The 5-line comment at `:1401-1405` | −5 |
| The handler copies and headers: sleep −8, load −12 (the 5-line signature collapses to 1, plus `let _`), store −8, auth −7, usage −7, ops −7, reconcile −8, timings −7 | −64 |
| New: router `}`, blank line, 4-line doc, 5-line `#[expect]`, `fn debug_routes`, `Router::new()`, and the `.nest` line | +14 |
| `mod debug;` | +1 |
| **Net** | **−87, to about 3,275** |

**`http.rs` in C2.**

| Change | Lines |
|---|---|
| Doc for `fleet_operation_authorization` | +4 |
| SR3-1 comment moves in, replacing 1 line | +4 |
| Off guard, including a 2-line comment | +5 |
| `workload_jwt_operation`: doc, fn and blank line | −15 |
| `fleet_operation_authorized` body, 7 lines to 1 | −6 |
| **Net** | **−8, to about 3,267** |

The ceiling is 3,362 in both commits. Sibling plan item 52 (`plans10/telemetry-append-preauth-body.md`) also shrinks `http.rs`. It also adds a `mod telemetry_append;` line beside `mod serve;`, which is a trivial textual merge with C1's `mod debug;`.

**Other files.**

- DST files (ceiling 1,000): `security_routes.rs` 634 → about 734; `security_workload.rs` 763 → about 800.
- New file: `src/http/debug.rs`, about 50 lines.
- Architecture gate (`FUNCTION_BUDGET = 200`, `scripts/architecture-report.py:40`): `router`'s baseline is 302 (`architecture-review-baseline.json`). The new `router` is about 99 raw lines and `debug_routes` about 172. There is no budget exception for either.

### Ratcheted scopes touched, with the remedy

| Scope | Effect | Remedy |
|---|---|---|
| `crate::router`'s `#[expect(clippy::too_many_lines, clippy::disallowed_methods, reason = "router; …")]` (`:1336-1341`) | `router` falls to about 66 code lines (head 30 + tail 33 + `.nest` + signature and brace; measured with `grep -v` of blank and comment lines over `:1343-1393` and `:1598-1642`). The only `tokio::spawn` leaves with the table. Both expectations would be unfulfilled, and `unfulfilled_lint_expectations` is denied. | **Delete the attribute.** |
| new `crate::debug_routes` `#[expect(clippy::too_many_lines, clippy::disallowed_methods, reason = …)]` | About 150 code lines, and it holds the abort's `tokio::spawn`, so both lints fire. A new identity, so `exception_growth` skips it as the explicit decision. The reason has exactly two `;` and no `"`. The primitive-spawn rule (`source_rules.py`: `disallowed_methods` needs a registered `effect` owner for the same qualified name) is met by moving the `owners.json` effect row. | New reason (text below). `owners.json` effect row owner `crate::router` → `crate::debug_routes`. |
| `crate::debug_load` `#[expect(too_many_lines, cast_possible_truncation)]` (`:815-819`) | Scope shrinks by 13 lines. Facts are only removed (the Query extractor, `let _`, the copy). Still more than 100 code lines, and the `as u64` at `now_unix_ms` stays. | None. |
| `crate::debug_timings` `#[expect(clippy::unwrap_used)]` (`:1659-1662`) | Function-wide unwrap contract: only keys vanish. These are the `authorized` and `err_resp` call sites and the `StatusCode::UNAUTHORIZED` and `HeaderMap` paths. The `.lock().unwrap()` site is untouched. No key goes from 0 to 1 and no count grows. | None. |
| C2 | `fleet_operation_authorization`, `fleet_operation_authorized` and `workload_jwt_operation` carry no exception. | None. |
| `debug.rs` | No exception. It uses no macros, statics or globs. | None. |

**Reason text for `debug_routes`:**

`"debug_routes; the operator debug table is one declaration behind one gate, and the debug abort spawns a bare task that ends the process itself; splitting the table would scatter what the gate covers and supervising the abort would separate it from the death it causes"`

### Why the table stays put, and the rustfmt facts that keep it put

- **The lines stay where they are.** The debug block `:1394-1597` becomes the body of `debug_routes()` where it stands: `Router::new()` at 4 spaces, `.route(` at 8, closure bodies at 20, exactly as inside `router()`.
- **The router tail moves instead.** The tail, `:1598-1642` (the operator and product routes plus both layers), moves up to follow the new `.nest(..)` line. That makes 45 moved lines against about 170 kept, so git's minimal diff moves the tail and keeps the table.
  - The tail has no binary or unary operators, no `match` and no struct base, so the move selects nothing new beyond `router`'s whole-body mutant, which is selected anyway.
- **rustfmt leaves `debug_routes` alone.** The only lines in `router()` wider than 100 columns are `:1585`, `:1586`, `:1588` and `:1591`, all inside the absorb closure's `serde_json::json!`, which rustfmt cannot reflow. That failure is why rustfmt leaves the whole route chain verbatim; `state.runtime.history.paused` / `.store(..)` at `:1420-1421` is visibly hand-wrapped. Those lines stay in `debug_routes`, so that chain stays verbatim too.
  - **Hand-format the edited closure parameters.** rustfmt will not touch them.
  - **`router()` becomes formattable.** Its head and tail are already rustfmt-shaped, but `cargo fmt` may still restyle a line there. None of those lines carries an operator (§7 checks this).

### Commit C1: "Every /v1/debug path answers through one bearer gate, not twelve copies"

**1. New file `src/http/debug.rs`:**

```rust
//! The operator debug surface's one authorization gate.
//!
//! The debug routes MUTATE production state or expose per-stream data
//! (round-19). MF1 found the documented gate missing once; after it the
//! gate lived as a copy in each handler, which a new route could forget.
//! Here the gate is the mount: `gated` is the only way the debug table
//! reaches the router, so no route under /v1/debug is served past it, and
//! the §14.2 operator audience replaces the credential in one place.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::{Next, from_fn_with_state};
use axum::response::Response;

use super::{AppState, authorized, err_resp};

/// The debug table behind the deployment bearer. The table gets its own
/// fallback because axum drops a nested router's default one: the
/// layer would then never see an unrouted path, and an anonymous probe
/// could tell routed from unrouted by 401 versus 404. With the token, an
/// unrouted path keeps the outer router's bare 404.
pub(super) fn gated(
    state: &Arc<AppState>,
    table: Router<Arc<AppState>>,
) -> Router<Arc<AppState>> {
    table
        .fallback(|| async { StatusCode::NOT_FOUND })
        .layer(from_fn_with_state(state.clone(), require_deployment_bearer))
}

/// The deployment bearer, checked before method routing, extractors or
/// any handler run: a missing or wrong token is 401 on every debug path.
/// Off mode with no bearer configured stays open (SR-5 local development).
async fn require_deployment_bearer(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    if !authorized(&state, request.headers()) {
        return err_resp(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        );
    }
    next.run(request).await
}
```

**Notes on `debug.rs`:**

- `super::` imports keep `reverse_edges` at `{}` (`scripts/architecture-gate.py:33-50` counts only a `super::http` segment), so `architecture-policy.json` needs no edit. The child module sees the parent's `pub(crate)` items.
- `request.headers()` replaces a `HeaderMap` extractor, so the headers are not cloned.

**2. `src/http.rs`, top to bottom:**

- **`:720-743` `debug_sleep`.** Delete `headers: HeaderMap,` (`:726`) and the copy (`:729-735`).
- **`:815-832` `debug_load`.** Collapse the signature to `async fn debug_load(State(state): State<Arc<AppState>>) -> Response {`. That removes `headers` and the unused `Query` extractor at `:822-823`. Delete `let _ = &q;` (`:825`) and the copy (`:826-832`). The `#[expect]` is untouched.
- **`:1036-1047` `debug_store`.** Delete `headers: HeaderMap,` (`:1038`) and the copy.
- **`:1062-1069` `debug_auth`.** Change the signature to `async fn debug_auth(State(state): State<Arc<AppState>>) -> Response {` and delete the copy.
- **`:1080-1089` `debug_usage`.** Rewrite the doc to 2 lines: `/// Per-stream usage counters + the active limits: per-stream data, which` / `/// is why the whole /v1/debug table mounts through \`debug::gated\`.` Drop `headers` from the signature and delete the copy.
- **`:1148-1155` `debug_ops_events`.** Drop `headers` and delete the copy.
- **`:1169-1180` `debug_usage_reconcile`.** Delete `headers: HeaderMap,` (`:1172`) and the copy.
- **`:1336-1342`.** Replace the `#[expect(..)]` with `/// Every route the service answers. The operator debug table mounts under` / `/// \`/v1/debug\` only through its one gate, \`debug::gated\`.`
- **`router()`.** After `:1393` (the telemetry-append route's `)`), insert `        .nest("/v1/debug", debug::gated(&state, debug_routes()))`. Then move `:1598-1642`, from `// Operator dashboard: …` through `.with_state(state)`, verbatim to follow it, and close with `}`.
- **Then open the table:**

```rust
/// The operator debug table. Its routes MUTATE production state (pausing
/// absorption, stalling flushes, aborting the process, resetting peak
/// gauges) or expose per-stream usage, so it mounts only through
/// `debug::gated`: no handler here checks the bearer itself.
#[expect(
    clippy::too_many_lines,
    clippy::disallowed_methods,
    reason = "debug_routes; …as in the ratchet table above…"
)]
fn debug_routes() -> Router<Arc<AppState>> {
    Router::new()
```

**Edits inside the table.** It keeps lines `:1394-1597` and the original closing `}` at `:1643`.

- **`:1394-1400`, `:1460` and the four `.route(` string lines** drop the `/v1/debug` prefix: `"/timings"`, `"/load"`, `"/store"`, `"/usage"`, `"/auth"`, `"/ops-events"`, `"/usage-reconcile"`, `"/absorb-pause"`, `"/abort"`, `"/sleep"`, `"/history-stall"` and `"/absorb"`.
- **`:1401-1405`.** Delete the comment. Its content is now the doc above.
- **absorb-pause.** Delete ` headers: HeaderMap,` (`:1410`) and the copy (`:1412-1418`). The parameters become `|State(state): State<Arc<AppState>>,` / ` Query(q): Query<std::collections::HashMap<String, String>>| async move {`.
- **abort.** Change `:1435` to `|State(state): State<Arc<AppState>>| async move {` and delete the copy (`:1436-1442`).
- **history-stall.** Delete `:1469` and the copy (`:1471-1477`).
- **absorb.** Change `:1493` to `|State(state): State<Arc<AppState>>| async move {` and delete the copy (`:1494-1500`).
- **Comments that stay:** the abort comment "auth-gated like every debug route" (`:1430-1431`) and the absorb comment "Authorized like every other /v1/debug route" (`:1488-1489`). Both are now true through the mount.

**Module declaration.** At `:3352`, after `mod serve;`, add `mod debug;`. The file resolves to `src/http/debug.rs`, like `serve`. It needs no `#[path]`, so no by-path owners row.

**3. `src/dst/tests/security_routes.rs`.** Add the §3.1 and §3.2 tests, the `debug_json` helper, and the `fixture_auth` import.

**4. `scripts/quality/mutation_owners.py`:**

- The `http` row (`:150`) becomes `'http:: livefeed_engine_retired security_workload:: debug_store_reports_this_runtimes_shard_opens debug_surface_'`.
- Insert directly after it: `owner('http_debug', 'src/http/debug.rs', 'debug_surface_'),`. `src/http` is a `BUFFER_PREFIXES` critical prefix, so a new file there must have a row.

**5. Ledgers:** see §6 (`owners.json`, `source-allowances.json`, `test-inventory.json`, `WIRE-MATRIX.md`).

### Commit C2: "The internal surface's boolean gate is the typed gate's is_some()"

**`src/http.rs` `:445-519`:**

```rust
/// Fleet identity for ONE exact operation (§14.1): the static bridge
/// token, or a verified workload JWT whose operations claim names `op`.
/// The one verification behind the internal routes and the enforce-mode
/// raw surface; `fleet_operation_authorized` is its boolean view.
fn fleet_operation_authorization(
    state: &AppState,
    headers: &HeaderMap,
    op: InternalOperation,
) -> Option<RawSurfaceAuth> {
    // SR3-1: the modes are EXCLUSIVE at runtime, not just at boot —
    // with a workload source configured, the static credential is
    // dead even if a legacy FLEET_INTERNAL_TOKEN leaked into the
    // environment. Startup also refuses that coexistence under the
    // release posture; this is the defense-in-depth layer beneath it.
    let static_ok = state.peer.inbound_static_ok(bearer(headers));
    if static_ok {
        return Some(RawSurfaceAuth::StaticBridge);
    }
    // Off mode is the deployment-bearer posture: it verifies no workload
    // identity, whatever keys were published.
    if state.auth.mode == crate::auth::AuthMode::Off {
        return None;
    }
    bearer(headers).and_then(|t| { /* :455-469 unchanged */ })
}
```

**Also in C2:**

- Delete `:486-500`: `workload_jwt_operation`, its doc and the blank line.
- Keep `fleet_operation_authorized`'s doc (`:501-506`) and signature. Its body becomes `    fleet_operation_authorization(state, headers, op).is_some()`.
- `src/dst/tests/security_workload.rs`: add the §3.3 test and the import.
- Ledger: `test-inventory.json` (§6).

---

## 5. Mutation analysis (cargo-mutants 27.1.0, `--in-diff`)

**The rules.** Checked in `~/.cargo/registry/src/*/cargo-mutants-27.1.0/src`:

- `in_diff.rs:213-260` marks every inserted line, the new-file line *before* each deletion, and the first surviving line *after* a deletion run.
- A `FnValue` mutant spans the body's first to last statement (`visit.rs:function_body_span`).
- Operator mutants span only the operator token.
- Closures get no `FnValue`.

In the C1 table below, every marked line of `http.rs` outside the listed ones carries no operator. I checked:

- every line before and after the 12 copies, the 6 `headers` lines, `:825` and the signature collapses: `:728` `) -> Response {`, `:736` `let ms: u64 = q`, `:833` `let (now, peak) = …`, `:1048` `let window: u64 = q`, `:1090` `let l = …`, `:1156` `let recent = …`, `:1181` `let mut month …`, `:1671` `let mut shards = …`, `:1478` `let ms: u64 = q.get("ms")…unwrap_or(0);` and `:1501` `let ord = …`
- the 12 path-literal lines, the deleted comment's neighbours, the moved tail, the inserted header lines and `mod debug;`

### C1

| # | Mutant | Why it is selected | Test that kills it |
|---|---|---|---|
| 1 | `http.rs: replace router -> Router with Default::default()` | body edited (`.nest`, moved tail) | every rig test in the `http` filter. An empty router answers 404 to everything; for example `livefeed_engine_retired` and both `debug_surface_` tests. |
| 2 | `http.rs: replace debug_routes -> Router<Arc<AppState>> with Default::default()` | new body | 3.2: `GET /v1/debug/timings` with the token → the nested fallback's empty 404 → `debug_json` panics "not JSON". Also `livefeed_engine_retired`. |
| 3 | `replace debug_sleep -> Response with Default::default()` | copy deleted | 3.2: `(200, b"")` ≠ `(200, b"ok")` |
| 4 | `replace debug_load -> Response with Default::default()` | copy and signature | 3.2: empty body is not JSON. Also `livefeed_engine_retired`. |
| 5 | `replace debug_store -> Response with Default::default()` | copy | 3.2. Also `debug_store_reports_this_runtimes_shard_opens`. |
| 6 | `replace debug_auth -> Response with Default::default()` | copy | 3.2: not JSON |
| 7 | `replace / with % in debug_auth` (`let now = crate::shard::now_ms() / 1000;`, the line after the deleted copy) | line after a deletion | 3.2: `now ∈ [0, 999]`, so `ageSecs ≈ −1.76e9`, outside `0..=60` |
| 8 | `replace / with * in debug_auth` | same | 3.2: `ageSecs ≈ 1.76e15`, and `stale` is `true` |
| 9 | `replace debug_usage -> Response with Default::default()` | copy | 3.2: not JSON / `limits` |
| 10 | `replace debug_ops_events -> Response with Default::default()` | copy | 3.2: not JSON / `events` |
| 11 | `replace debug_usage_reconcile -> Response with Default::default()` | copy | 3.2: 200 empty ≠ 503 `rollup_unavailable` |
| 12 | `replace debug_timings -> Response with Default::default()` | copy | 3.2: not JSON |
| 13 | `replace == with != in debug_routes` (absorb-pause `v == "1"`, the line after the deleted copy) | line after a deletion | 3.2: `?on=1` answers `absorb_paused: false` |
| 14 | `delete ! in debug_routes` (abort `if !state.config.http.debug_exit`, the line after the deleted copy) | line after a deletion | 3.2 asserts 403 `disabled` and gets 200 `{"aborting":true}`, so it panics. The spawned abort, if it ever fires, also ends the test binary nonzero. Either way the mutant is CAUGHT. The unmutated run never spawns. |
| 15 | `debug.rs: replace gated -> Router<Arc<AppState>> with Default::default()` | new file | 3.1: an empty nested router falls to the outer 404, and the test asserts 401 |
| 16 | `debug.rs: replace require_deployment_bearer -> Response with Default::default()` | new file | 3.1: 200 with an empty body ≠ 401 |
| 17 | `debug.rs: delete ! in require_deployment_bearer` | new file | 3.1: an unauthenticated `GET /v1/debug/timings` reaches the handler → 200. Also 3.2: the token gets 401. |

The fallback closure `|| async { StatusCode::NOT_FOUND }` is a closure, so it gets no `FnValue`. If it were deleted, 3.1's `/v1/debug/nope` probe would fail.

### C2

| # | Mutant | Test that kills it |
|---|---|---|
| 18 | `replace fleet_operation_authorization -> Option<RawSurfaceAuth> with None` | `security_workload::workload_jwt_operations_scope_the_internal_surface`: the enforce-mode stage `PUT /v1/stream/opx` with the static token goes through `raw_surface_authorization` (`http.rs:2214`) → 401, and `assert!(st == 200 \|\| st == 201)` fails. Also 3.3: the static request gets 401, not 400. |
| 19 | `… with Some(Default::default())` | UNVIABLE: `RawSurfaceAuth` has no `Default` |
| 20 | `replace == with != in fleet_operation_authorization` (the Off guard) | `workload_jwt_operations_scope_the_internal_surface`: under enforce the guard refuses, so the segment-read JWT on its own route gets 401 and `assert_ne!(st, 401)` fails. Also 3.3: in Off mode the JWT is verified and answers 400. |
| 21 | `replace fleet_operation_authorized -> bool with true` | 3.3: the JWT gets 400, not 401. Also `workload_jwt_operations_…`: the segment-read token reaches telemetry-append. |
| 22 | `replace fleet_operation_authorized -> bool with false` | 3.3: the static token gets 401, not 400. Also `workload_jwt_operations_…`: `assert_ne!(st, 401)`. |

`:458`'s `now_ms() / 1000` is not marked. Only inserted lines lie near it: C2 inserts between `:454` and `:455` and replaces `:450`, and the line after an insertion is not marked. `workload_jwt_operation`'s `/ 1000` is deleted code, which carries no mutant.

**Totals.** 21 viable mutants and 1 unviable one; none is equivalent.

**Owner rows and filters (C1):**

- `http` gains the substring `debug_surface_`. That is the only way the §3.1 and §3.2 DST tests run for `http.rs` mutants. §3.3 is already inside `security_workload::`.
- New row: `http_debug` → `src/http/debug.rs`, filter `debug_surface_`. It matches two tests, never zero.

---

## 6. Ledgers

### C1

- **`docs/quality/owners.json`.** Edit in place; the file is not sorted.
  - The effect row at `:2083-2090` changes `"owner": "crate::router"` to `"crate::debug_routes"`. The reason "The debug abort route spawns one bare task that sleeps briefly and aborts the process; nothing outlives it." still holds.
  - Insert directly after it:

    ```json
    {
      "category": "macro-dsl",
      "count": 7,
      "owner": "crate::debug_routes",
      "path": "src/http.rs",
      "reason": "Operator debug documents built inline by the debug table's routes (absorb-pause, abort, history-stall, absorb); serde macro output is compiled, and the debug_surface_ tests read the absorb-pause, abort and absorb answers.",
      "syntax": "serde_json::json"
    },
    ```

    The count checks out: `awk 'NR>=1394 && NR<=1597' src/http.rs | grep -c 'json!'` = 7, and the remainder of `router` has 0.
- **`docs/quality/source-allowances.json`.** Delete the two rows `router` vacates. Both are stale once the table moves, and the gate fails with "obsolete source allowances" until they go. Use `python3 scripts/quality/gate.py --prune` after a clean clippy build, or delete them by hand; these are the only two stale rows.
  - `{"category":"effect","count":1,"owner":"crate::router","path":"src/http.rs","syntax":"tokio::spawn"}`
  - `{"category":"macro-dsl","count":7,"owner":"crate::router","path":"src/http.rs","syntax":"serde_json::json"}`
- **`docs/refactor/test-inventory.json`.** Run `python3 scripts/test-inventory.py --write`: +2 tests. Check that the diff only adds those two rows.
- **`docs/refactor/WIRE-MATRIX.md:204`.** Replace the Debug heading with:

  > `### Debug (one gate, src/http/debug.rs::gated: every /v1/debug path — routed or not, any method — answers a caller without the deployment bearer 401 unauthorized in the err_resp envelope; with it, an unrouted path is the bare 404 and a wrong method 405)`

  The per-route bullets are unchanged. If Søren picks D1's alternative, the text becomes "routed paths answer 401 …; unrouted paths and wrong methods keep 404/405 for every caller".
- **Not needed:**
  - `review-mechanisms.json`: no pinned test changes.
  - `architecture-policy.json`: no reverse edges, and no function over 200.
  - `src/dst/tests/README.md`: no new DST module.
  - Scenario map and dispositions: no renames.
  - `source-allowances` for `debug.rs`: no macros, statics, globs or `#[path]`.
  - `legacy-*`: frozen.

### C2

- `docs/refactor/test-inventory.json`: `--write`, +1 test. Nothing else.

---

## 7. Controls

Run these after the concurrent mutation and gate runs have finished; they contend for CPU. Never run a mutation leg and the gate together.

1. **Red.** On `2fb92fb9` with only the §3.1 and §3.2 tests and their import added:
   - `cargo test --locked --lib debug_surface_refuses_every_path_without_the_token`: expect `FAILED` with the §3.1 panic (`GET /v1/debug/nope with None must be refused: ` / `left: 404` / `right: 401`).
   - `cargo test --locked --lib debug_surface_serves_every_handler_with_the_token`: expect `ok`, which shows the pin holds before the change.
   - `cargo test --locked --lib off_mode_internal_routes_refuse_workload_jwts`, with §3.3 added: expect `ok`.
2. **Optional reviewer-literal check for C2.** Locally only, never committed: replace `fleet_operation_authorized`'s body with `fleet_operation_authorization(..).is_some()` *without* the Off guard. The §3.3 test then fails with `Off mode verified a workload JWT on the internal surface` / `left: 400` / `right: 401`. Revert, then apply C2 as planned; the test is `ok`.
3. **Format.** `cargo fmt --all -- --check` exits 0 with no output.
   - Then `git diff HEAD~1 -- src/http.rs | grep '^[-+]' | grep -E '[^=!<>]=[^=>]|[+*/%<>!&|-]'` and read every hit. The only operator lines may be the four in §5 (#7/8, #13, #14) plus the unchanged `-` side of deletions.
   - If rustfmt restyled any `router()` head or tail line, confirm that line has no operator.
4. **Green legs for the named tests:**

   ```
   scripts/test-leg.sh target/legs/item44.log \
     --exact dst::dst_tests::security_routes::debug_surface_refuses_every_path_without_the_token \
     --exact dst::dst_tests::security_routes::debug_surface_serves_every_handler_with_the_token \
     --exact dst::dst_tests::security_workload::off_mode_internal_routes_refuse_workload_jwts \
     -- --locked --lib dst_tests::security_
   ```

   Expect every named test `... ok` and `tests_ran.py` exit 0. The suite should include every `security_routes`, `security_modes` and `security_workload` test.
5. **Surrounding callers.** `cargo test --locked --lib livefeed_engine_retired`, `cargo test --locked --lib debug_store_reports_this_runtimes_shard_opens`, `cargo test --locked --lib dst_tests::billing_usage::` and `cargo test --locked --lib dst_tests::admission_maintenance::` should all pass.
6. **Ledgers.** `python3 scripts/test-inventory.py --check` should report `test-inventory: OK (N tests, …)`, with N the HEAD count +2 after C1 and +3 after C2. `python3 scripts/architecture-gate.py --check` should exit 0.
7. **Quality.** `scripts/quality.sh` should print `QUALITY_OK`, with:
   - clippy `-D warnings` clean: no `unfulfilled_lint_expectations` on `router`, and both `debug_routes` lints fulfilled
   - no `accepted exception grew` line
   - no `file growth` line (`http.rs` ≤ 3,362)
   - no `primitive-spawn exception needs a registered function owner`
   - no `obsolete source allowances`
   - the rustdoc leg green
8. **Mutation, on CI's push plan, after both commits are made.** Run:

   ```
   QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) \
   QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate \
     python3 scripts/quality/verification_plan.py --out target/quality-plan
   ```

   Expect `selected_mutation_owners` = `["http", "http_debug"]`. Then `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) scripts/quality/mutations.sh`. Expect:
   - `target/quality-mutations/http/selected.json` lists exactly §5 #1-14 and #18-22.
   - `http_debug/selected.json` lists exactly #15-17.
   - Every viable mutant is CAUGHT, #19 is UNVIABLE, and there are 0 MISSED and 0 TIMEOUT.
9. **Full gate.** `scripts/gate.sh`, alone, is green.
10. **After the push.** Run `gh run list --branch slate --json headSha,createdAt,status,conclusion` and match the SHA. Do not claim CI is green without that.

---

## 8. Out of scope (recorded, not done)

- **Moving the eight handler bodies and four closures into `debug.rs`.** Each moved operator would need a test that pins exact gauge arithmetic (`debug_load`'s fold sums and maxima, `/ 1048576`, `now - RUNTIME_LAST_TICK_MS`), and some sit over `/sys/fs/cgroup` reads a rig cannot supply. Do it when those gauges get typed owners.
- **The operator surface has the same smell.**
  - Four hand-copied gates: `operator.rs:36` through `operator_gate` on `page`, `runbook` and `data`, plus `http.rs:1913` `billing_readiness_axum`.
  - Its 401 body differs from the debug one: `text/plain` "operator bearer required" versus JSON. A gated `/operator` sub-router would therefore change a wire body, so it needs its own decision.
  - Two stale comments contradict SR-5 (`operator.rs:27-31`): `http.rs:1598-1601` "Operator dashboard: UNSECURED by explicit product decision" and `operator.rs:1-2`.
- **`raw_surface_authorization`'s `_ =>` arm on `AuthMode`** (`http.rs:441`).
- **A route registered on the *outer* router under `/v1/debug`** would bypass the gate, and axum cannot forbid it. The doc on `router` and `debug_routes`, and the 12 per-route probes in §3.1, are the guard.
- **Off mode with no bearer opens the whole debug surface**, including abort when `STREAMS_DEBUG_EXIT=1`. This is the SR-5 posture and is unchanged.
- **`/v1/debug/`** (trailing slash, empty tail) keeps the outer 404.
- **`config/model.rs:209` mentions a nonexistent `/v1/debug/billing`.**

---

## 9. Decisions for Søren

**D1: an unauthenticated probe of an unrouted debug path, or of a debug route under the wrong method, answers 401 instead of 404/405.**

- **Recommendation:** the prefix-wide gate as planned. `SECURITY.md:66` already states `/v1/debug/*` is bearer-gated, and an anonymous caller should not be able to map which debug endpoints exist.
- **What changes.** With the gate as planned (`gated` = explicit fallback + `.layer`):
  - `GET /v1/debug/nope`, bare `/v1/debug`, and for example `GET /v1/debug/abort`, sent without the token, answer 401 `{"error":{"code":"unauthorized","message":"bearer token required"}}` instead of 404 (empty) or 405 (+`Allow`).
  - Authorized callers see no change, and neither does any routed path under its correct method.
- **Byte-compatible alternative:** `table.route_layer(from_fn_with_state(state.clone(), require_deployment_bearer))` with no `.fallback`. Unrouted paths and wrong methods then keep 404/405 for every caller, and the wire is byte-identical.
  - §3.1 becomes a pin, not a red: its `unrouted` and `other_method` probes flip to asserting 404/405, and only the per-route 401s remain. There is then no red test.
  - Mutants #15-17 are still killed: #15 and #17 by 3.2 and by the per-route probes, #16 by the per-route probes.
  - The WIRE-MATRIX line changes as noted in §6.

C2 needs no decision. It is a pure refactor, and it keeps the Off-mode refusal that the reviewer's literal one-liner would have dropped.

---

## Skeptic corrections (C1..C6)

I checked the plan against `slate` @ `2fb92fb9`. `origin/slate` is also `2fb92fb9`, so the merge base is HEAD, as §0 says. I read and grepped only; I built nothing and ran nothing.

**What holds up:**

- **Use sites.** There are 13 `if !authorized(&state, &headers)` sites: the 12 debug copies plus `:1913`. `fleet_operation_authorized` has 6 callers (`http.rs:1229,3202,3279`, `product.rs:3338,3433,3515`), and `workload_jwt_operation` is called only from `:518`. Every `/v1/debug` caller in `src/dst` sends no token to an Off rig, except `security_modes.rs:202`. No other router registers a debug path (`bootstrap.rs:897` and `fixture_http.rs:531` both build `http::router`).
- **Line counts.** `http.rs` 3,362, `security_routes.rs` 634, `security_workload.rs` 763. All the other ceilinged files match the task text.
- **Line budgets.** C1 nets −87 and C2 nets −8. I recounted both.
- **axum 0.8.9 routing.**
  - `nest` drops a *default* inner fallback (`routing/mod.rs:225-228`).
  - An explicit inner fallback is nested at `/v1/debug` and `/v1/debug/{*__private__axum_fallback}` (`path_router.rs:33-36`, `:535-546`).
  - `Router::layer` wraps the fallback router (`mod.rs:311-316`), and `MethodRouter::layer` wraps the 405 fallback (`method_routing.rs:990`).
  - The red trace and the green claims are correct.
- **Ratchets.**
  - `router`'s expect becomes unfulfilled on both lints: 30 + 32 code lines, and `tokio::spawn` leaves with the table. The table keeps 179 − 30 = 149 code lines.
  - `debug_load` stays above 100 code lines (173 − 13).
  - `debug_timings`'s function-wide `unwrap_used` contract only loses keys.
  - The primitive-spawn rule (`source_rules.py:249-256`) is satisfied by the renamed `owners.json` effect row. The macro-dsl row belongs in `owners.json`: a new owner row in `source-allowances.json` would fail "legacy source allowance grew" (`source_gate.py:26`). The two stale `crate::router` allowance rows (`source-allowances.json:1431-1437`, `:2887-2893`) are the only ones.
- **Architecture gate.** `debug.rs` adds no edges (`architecture-gate.py:33-50`), `debug_routes` is about 172 raw lines against the budget of 200, and the MT audit is unaffected (its patterns do not touch these lines).
- **in-diff rules.** They match `cargo-mutants-27.1.0/src/in_diff.rs:213-260`. No struct-base or match mutants are in scope (`visit.rs:645`, `:703`), and closures get no FnValue.
- **Red and pin tests.**
  - §3.1 fails today with exactly `GET /v1/debug/nope with None must be refused: ` / `left: 404` / `right: 401`.
  - §3.2 passes today: `debug_exit` false (`config/model.rs:431`, empty env at `fixture_http.rs:348`), `RollupSlot::default()` (`:474`), per-rig `HistoryResources` (`runtime.rs:185`), `absorb_pause_initial` false (`config/model.rs:389`).
  - §3.3 passes today and would fail under the reviewer's literal one-liner, because `verify_internal` (`auth.rs:780-807`) checks no mode.
  - Every request is bounded (`fixture_requests.rs:11-30`, 60 s).
  - The mutation table (#1-#22) misses no changed body or operator line.

**C1. §1.4's history is wrong.** The "enforced by the middleware layer" comment was not written by `8fd308eb` (MF1). In `git show 8fd308eb` it is a context line. `git log -S'enforced by the middleware layer'` shows it arrived in `477fd445` ("service limits + usage telemetry + billing stream"). The edits do not change, but the commit message must not say MF1 wrote it.

**C2. rustfmt: the hand-formatted new code will fail `cargo fmt --all -- --check`, which `scripts/quality.sh:13` runs.**

- **`gated`'s signature.** It is 99 columns on one line (`pub(super) fn gated(state: &Arc<AppState>, table: Router<Arc<AppState>>) -> Router<Arc<AppState>> {`). That fits `max_width` 100, so rustfmt collapses the plan's 4-line form.
- **`debug_json`'s `preq(addr, method, path, &[("authorization", "Bearer s3cret")], b"")`.** Its arguments are 62 characters, over `fn_call_width` 60, so rustfmt goes vertical.
- **Several `assert_eq!` lines in §3.1-§3.3.** They will be reflowed the same way.
- **Remedy.** Write the code, then run `cargo fmt --all` rather than hand-formatting. It is safe:
  - `debug_routes`' chain stays verbatim for the same reason `router`'s is verbatim today (`:1585-1591` exceed 100 columns inside `json!`; the hand-wrapped `:1420-1421` proves rustfmt is not touching it).
  - Only `router()`'s head and tail and the new or test code can move.
  - Then re-run the mutant listing in C3.

**C3. §7 control 3's grep cannot show the operators it expects.**

- The four selected operator sites (`debug_auth`'s `/ 1000`, absorb-pause's `v == "1"`, abort's `!state.config.http.debug_exit`, and in C2 the Off guard's `==`) are mostly *context* lines next to deletions. They never appear in `git diff | grep '^[-+]'`. Meanwhile `->`, `|…|` and generics on `+` lines flood the regex.
- **Replacement.** Before any mutation run, and again after `cargo fmt`, list what `--in-diff` really selects, with CI's own inputs:
  - `QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=2fb92fb9 python3 scripts/quality/verification_plan.py --out target/quality-plan`
  - `cargo mutants --list --json --in-diff target/quality-plan/pr.diff --file src/http.rs --package streams-slate`, and the same with `--file src/http/debug.rs`.
  - Expect exactly §5 #1-14 and #18-22 for `http.rs`, and #15-17 for `debug.rs`.
- **The failure this protects against.** The whole mutation plan rests on git's Myers diff keeping the 170-line table as context and moving the 45-line tail. `pr.diff` is a plain `git diff --no-ext-diff --binary` (`verification_plan.py:376-379`, no algorithm flag). If the listing shows any mutant inside the absorb closure (the `v / 1048576` sites at `:1586`/`:1588`), the table was rendered as moved.
  - Those mutants cannot be killed and would be MISSED.
  - **Do not push.** Re-cut the commit so the tail stays textually in place. For example, put `debug_routes` *above* `router` and replace the table's lines in `router` with the `.nest` line. Then list again.

**C4. Pin the layer order that makes the gate's answers "ours".**

- The plan is right to put `.nest(..)` above the moved `track_inflight` and origin-marker layers. Placing it after `.layer(..)` would silently strip `prisma-streams-origin` (`http.rs:1620-1640`, "round-19 must-fix 4") from every debug response, including the new 401s and the nested 404. No planned assertion would notice.
- **Fix.** Add one assertion to §3.1 for the unauthenticated `/v1/debug/nope` probe, and one to §3.2 for the authorized unrouted 404: `assert_eq!(h.get("prisma-streams-origin").map(String::as_str), Some("dst-instance"))`. The rig's marker is `"dst-instance"` (`fixture_http.rs:517`). Bind `h` instead of `_` from `preq`.
- This adds no mutant. It is a structural pin for the one ordering the diff introduces.

**C5. §7 control 8's plan receipt is incomplete.** C1 edits `scripts/quality/mutation_owners.py`, which sets `tooling` (`verification_plan.py:75-77`). That forces `compiler`, `properties_fuzz`, `loom` and `miri` all `true` (`:95-97`); `src/http/` is a buffer prefix, so `miri` is `true` anyway. Expect that receipt and budget the extra CI legs. Nothing fails because of it; it only costs wall time. The owner-table unit tests run inside control 7 (`quality.sh:16`, `python3 -m unittest discover -s scripts/quality -v`).

**C6. The sibling-plan merge note is incomplete.** Item 52 (`telemetry-append-preauth-body.md`) also edits `mutation_owners.py`: it adds `http_telemetry_append` after `:88`, while this plan adds `http_debug` after `:150`. It also adds a `mod` line beside `mod serve;` (`http.rs:3352`). Neither edit conflicts semantically. Whichever lands second must re-run `test-inventory.py --write`, since both append DST tests. It must also re-list mutants against its own `before` SHA, per C3.

**Verdict: ready-with-corrections.**

- No correction changes the design, a test's red or green, or the mutation kill table.
- C2 and C3 must be applied before the gate or mutation run, or control 3 and the fmt step fail.
- C4 is a cheap structural pin.
