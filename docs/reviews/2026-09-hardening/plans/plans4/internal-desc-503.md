# Review item 30 — internal receivers answer 404 for a registry storage error

Tree: `slate` @ `8dabca7f` (2026-09-23). Read-only analysis; nothing below was
run or edited. The reviewer's line numbers are stale; every citation here was
re-found by content on the current tree.

## 1. Problem (verified)

Three fleet-internal receivers in `src/product.rs` collapse a registry
`Result<Option<StreamDesc>, object_store::Error>` into one 404 with a `_ =>`
catch-all. Identical text at all three sites:

`src/product.rs:3372-3375` (`internal_sweep_segment`):
```rust
    let desc = match state.registry.get(&sref).await {
        Ok(Some(d)) => d,
        _ => return perr(StatusCode::NOT_FOUND, "not_found", "stream", None, false),
    };
```
`src/product.rs:3465-3468` (`internal_queue_cursor`) and
`src/product.rs:3552-3555` (`internal_segment_scan`): byte-identical.

The `_` arm binds both `Ok(None)` (no descriptor for the name) and `Err(_)`
(the store could not be read). A store error is transient; "no descriptor" is
a lifecycle fact. The wire cannot tell them apart.

The sealed-span SSE sender turns that 404 into a typed, terminal cutoff.
`src/application/read_remote.rs:190-192` (`scan_page_once`):
```rust
    if status == 404 || status == 410 {
        return Err(RemoteSpanError::TargetGone);
    }
```
`src/sse/source.rs:528-530` (`LineageSource::sealed_span_page`):
```rust
            Err(crate::application::read_remote::RemoteSpanError::TargetGone) => Err(
                anyhow::Error::new(FatalSpanCutoff(SourceCutoff::IncarnationChanged)),
            ),
```
`src/sse/feed.rs:930-940` (`read_and_publish`): a `FatalSpanCutoff` sets
`st.lifecycle = Lifecycle::Gone(reason)` and returns
`DriveOutcome::IncarnationClosed(reason)`, whose doc (`feed.rs:1177-1180`) says
"sessions disconnect WITHOUT a terminal control". A plain `anyhow` error
instead returns `DriveOutcome::SourceFailed` — "The session parks and retries
on the next wake" (`feed.rs:1181-1182`).

So one injected/real registry GET failure on the owner of a sealed span makes
every subscriber of that feed disconnect as if the stream had been recreated.
Confirmed real on the current tree.

What is NOT broken (and therefore not touched):

- `src/http.rs:3332-3346` (`internal_segment_read`) and `http.rs:3218-3228`
  (`internal_segment_close`) already split the cases:
  ```rust
          Ok(None) => return err_resp(StatusCode::NOT_FOUND, "not_found", "stream not found"),
          Err(e) => {
              return err_resp(
                  StatusCode::SERVICE_UNAVAILABLE,
                  "temporarily_unavailable",
                  &e.to_string(),
              );
          }
  ```
  The reviewer's "also used by http.rs internal_segment_read" is a dedup, not a
  fix; http.rs is at 3,371/3,372 lines and is a registered mutation owner. Left
  as a follow-up (§8).
- The sender mapping is correct once the receiver is honest: 503 goes through
  `scan_page_once:193-199` → `RemoteSpanError::Retryable { status, code }` →
  `source.rs:516-518` `anyhow::bail!(...)` → `SourceFailed` → park and retry.
  A 404 then means exactly "no descriptor" = the incarnation is gone, which IS
  `IncarnationChanged`. No sender change.
- The two consumer relays (`src/application/consumer_remote.rs:41-44` and
  `:129-132`) treat any non-2xx as `None` → their own retryable refusal, so the
  404→503 change on queue-cursor/sweep-segment is status-agnostic for them.
  They are fixed anyway because the receiver contract must be one contract.

## 2. Contract decision

One typed registry prelude for the product.rs internal receivers, owned by a
new sub-module `src/product/internal.rs`:

```rust
pub(super) async fn internal_desc(
    registry: &crate::registry::Registry,
    sref: &crate::tenant::TenantStreamRef,
) -> Result<StreamDesc, Response>
```

| registry answer | wire | body (`perr` envelope) |
|---|---|---|
| `Ok(Some(desc))` | continue to `verify_internal_target` | — |
| `Ok(None)` | **404** `not_found` | `{"error":{"code":"not_found","message":"stream","retryable":false}}` (byte-identical to today) |
| `Err(store)` | **503** `temporarily_unavailable` | `{"error":{"code":"temporarily_unavailable","message":"<store error Display>","retryable":true}}` |

Applies to `POST /v1/internal/sweep-segment`, `GET /v1/internal/queue-cursor`,
`GET /v1/internal/segment-scan`. No `Retry-After` header: `perr` has none, the
http.rs twins send none, `scan_page_once` never reads one, and the feed's retry
cadence is its own. No liveness gating (`desc_alive`) in the prelude: the
incarnation check that follows (`verify_internal_target`) is what binds the
request; a dead descriptor whose epoch matches is still the one the sender
addressed — today's behaviour, kept.

Sender contract (unchanged, now made true): `scan_page_once` 404/410 →
`TargetGone` → `FatalSpanCutoff(IncarnationChanged)`; 429/503 → `Retryable` →
bounded retry.

Why the reviewer's Change is adjusted:

- The helper takes `&Registry`, not `&AppState`: the new file then has zero
  `crate::http` reverse edges and needs no `architecture-policy.json` row
  (`scripts/architecture-gate.py:117-120` fails any edge on a file not listed
  under `transport_and_composition_files`).
- "Pay for it in product.rs by replacing the five public prelude copies with a
  typed live_descriptor" is unnecessary and riskier: it touches five public
  handlers whose `#[expect]` scopes are fingerprinted (five reason
  re-decisions) and edits customer-facing preludes for no behaviour change. A
  stale 4-line comment (§4) plus a 1-line `mod` pays for this item outright.
- A verbatim-move commit of the three handlers was evaluated and rejected as
  the larger change: it re-keys `internal_segment_scan`'s two `json!` sites
  (legacy `macro-dsl` allowance at `src/product.rs` goes stale → needs
  `gate.py --prune` + a new `owners.json` row), needs an mt-lint marker on
  `internal_sref`, and its `allow(clippy::result_large_err, reason = "...")`
  one-`;` reasons are only tolerated as legacy debt at their current path.
  Line-neutral edits in place avoid all of that.

## 3. Red tests

One new DST test in the existing module `src/dst/tests/read_peer_compatibility.rs`
(281 lines today; the module already drives `internal_segment_scan` over real
HTTP and has the `seed`/`request` helpers). It exercises all three receivers so
a regression on one route cannot hide behind the others, then the two
controls the reviewer named (one-shot failpoint → the same scan serves 200;
absent name → still 404).

Test name (full path for `--exact`):
`dst::dst_tests::read_peer_compatibility::o2c_registry_store_error_on_a_receiver_is_retryable_not_gone`

```rust
/// One request per receiver, all three sharing the registry prelude, so a
/// status that regresses on one route cannot hide behind the others.
fn internal(
    rig: &HttpRig,
    target: &InternalTarget,
    method: reqwest::Method,
    path: &str,
) -> reqwest::RequestBuilder {
    let mut request = crate::peer::client()
        .request(method, format!("http://{}/v1/internal/{path}", rig.addr))
        .bearer_auth("dst-internal-token");
    for (key, value) in target.headers() {
        request = request.header(key, value);
    }
    request
}

async fn answer(request: reqwest::RequestBuilder) -> (u16, String, Option<bool>) {
    let response = tokio::time::timeout(Duration::from_secs(10), request.send())
        .await
        .unwrap()
        .unwrap();
    let status = response.status().as_u16();
    let body: serde_json::Value = response.json().await.unwrap();
    (
        status,
        body["error"]["code"].as_str().unwrap_or("").to_string(),
        body["error"]["retryable"].as_bool(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o2c_registry_store_error_on_a_receiver_is_retryable_not_gone() {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            absorber: Some(cold_absorber()),
            ..Default::default()
        },
    )
    .await;
    let desc = seed(&rig).await;
    let target = InternalTarget::of(&desc, 0).unwrap();
    let mut answers = Vec::new();
    rig.state.registry.fail_next_get("compat");
    answers.push(("segment-scan", answer(request(&rig, &target)).await));
    rig.state.registry.fail_next_get("compat");
    answers.push((
        "queue-cursor",
        answer(
            internal(&rig, &target, reqwest::Method::GET, "queue-cursor/compat")
                .header("streams-internal-consumer", "c")
                .header("streams-internal-gen", "1"),
        )
        .await,
    ));
    rig.state.registry.fail_next_get("compat");
    answers.push((
        "sweep-segment",
        answer(
            internal(&rig, &target, reqwest::Method::POST, "sweep-segment/compat")
                .body(r#"{"consumer":"c","segId":0,"fenceBelow":0,"maxSteps":1}"#),
        )
        .await,
    ));
    let unavailable = (503, "temporarily_unavailable".to_string(), Some(true));
    assert_eq!(
        answers,
        [
            ("segment-scan", unavailable.clone()),
            ("queue-cursor", unavailable.clone()),
            ("sweep-segment", unavailable),
        ],
        "a registry the receiver cannot read is not a stream that is gone"
    );
    // The failpoint is one-shot: the same scan then serves its page ...
    let served = tokio::time::timeout(Duration::from_secs(10), request(&rig, &target).send())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(served.status(), 200, "the store error was transient, not a state change");
    // ... and a name with no descriptor is still not_found.
    let absent = internal(&rig, &target, reqwest::Method::GET, "segment-scan/absent")
        .header("streams-internal-from", "1")
        .header("streams-internal-max-bytes", "1024")
        .header("stream-encryption-key", PRISMA_KEY);
    assert_eq!(
        answer(absent).await,
        (404, "not_found".to_string(), Some(false)),
        "no descriptor is the one honest 404"
    );
    engine_shutdown(&rig.state).await;
    rig.tasks.shutdown(Duration::from_secs(5)).await;
}
```

Notes that keep this test gate-clean:
- `fail_next_get` is the existing `#[cfg(test)]` one-shot on `Registry::get`
  (`src/registry.rs:1318-1326`, consumed in `src/registry/cache.rs:298-304`
  BEFORE the cache, so a cached "compat" still fails). Same arming pattern as
  `consumer_saga.rs:92`, `append_application.rs:189`.
- Parameter is named `path`, not `name`: mt_lint flags a `name: &str` param in
  any non-surface file (`src/mt_lint.rs:95,165-166,179-187`), and DST files are
  walked.
- No `serde_json::json!`: it is a `macro-dsl` fact keyed by owning fn
  (`source_rules.py:51-55`); the module's existing row in `owners.json:227-234`
  is for another fn. Indexing a parsed `Value` is enough.
- No `crate::http::`/`crate::product::` mention in the new code (reverse-edge
  count of the file stays what it is).
- Every wait is bounded (10 s send timeout; the rig shutdown is 5 s).
- The static fleet token authorises every `InternalOperation`
  (`http.rs:507-518`, `inbound_static_ok`; rig wires `dst-internal-token` at
  `fixture_http.rs:441`).
- Handler order guarantees the registry GET is the first fallible step after
  auth/header parsing on all three routes (sweep parses its JSON body first;
  the body above is valid `Doc`).

Expected red output on the current tree (only the test added), from
`cargo test --release --lib -- --exact dst::dst_tests::read_peer_compatibility::o2c_registry_store_error_on_a_receiver_is_retryable_not_gone`:

```
---- dst::dst_tests::read_peer_compatibility::o2c_registry_store_error_on_a_receiver_is_retryable_not_gone stdout ----
thread 'dst::dst_tests::read_peer_compatibility::o2c_registry_store_error_on_a_receiver_is_retryable_not_gone' panicked at src/dst/tests/read_peer_compatibility.rs:<L>:<C>:
assertion `left == right` failed: a registry the receiver cannot read is not a stream that is gone
  left: [("segment-scan", (404, "not_found", Some(false))), ("queue-cursor", (404, "not_found", Some(false))), ("sweep-segment", (404, "not_found", Some(false)))]
 right: [("segment-scan", (503, "temporarily_unavailable", Some(true))), ("queue-cursor", (503, "temporarily_unavailable", Some(true))), ("sweep-segment", (503, "temporarily_unavailable", Some(true)))]
```
(`404` because every current arm is
`perr(StatusCode::NOT_FOUND, "not_found", "stream", None, false)`.)

Existing tests that must stay green and pin the neighbours:
`o2c_old_scan_headers_preserve_records_progress_and_physical_frontier` (200 page
shape, 400 `invalid_body`, 403 `wrong_key`, mandatory identity headers),
`product::tests::internal_target_*` (six `verify_internal_target` cases — the
prelude change does not move that function).

## 4. Edits, file by file

### 4.1 `src/product/internal.rs` — NEW (~36 lines; ceiling 1,000)

```rust
//! The fleet-internal receivers' registry prelude. A peer relays on behalf
//! of an incarnation it has already bound, so what it hears back must say
//! which of two different things happened: the name has no descriptor (the
//! incarnation is gone; the sender may cut over) or this instance could not
//! read its registry (nothing is known; the sender must try again). One 404
//! for both let a registry blip read as "gone" — the sealed-span SSE sender
//! made it `FatalSpanCutoff(IncarnationChanged)` and disconnected every
//! subscriber of a feed whose stream still existed (review item 30).
use axum::http::StatusCode;
use axum::response::Response;

use super::perr;
use crate::registry::{Registry, StreamDesc};
use crate::tenant::TenantStreamRef;

/// `Ok(None)` is 404 `not_found`; a store error is 503
/// `temporarily_unavailable`, retryable. No liveness gating here: the
/// incarnation check that follows binds the request, and a dead descriptor
/// whose epoch matches is still the one the sender addressed.
pub(super) async fn internal_desc(
    registry: &Registry,
    sref: &TenantStreamRef,
) -> Result<StreamDesc, Response> {
    match registry.get(sref).await {
        Ok(Some(desc)) => Ok(desc),
        Ok(None) => Err(perr(StatusCode::NOT_FOUND, "not_found", "stream", None, false)),
        Err(error) => Err(perr(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            &error.to_string(),
            None,
            true,
        )),
    }
}
```

`clippy::result_large_err`: every `Result<_, Response>` fn in product.rs and
http.rs carries a legacy `#[allow(clippy::result_large_err, reason = "...")]`
(12 rows in `docs/quality/source-allowances.json` for product.rs alone), so
whether the lint actually fires on `Response` (~120-128 B, borderline against
the 128 B default) cannot be read off the tree. Decide empirically: write the
helper with NO attribute, run clippy (§7 step 3). If `result_large_err` is
reported, add
`#[expect(clippy::result_large_err, reason = "internal_desc; the three receivers answer the wire response this prelude decided, unchanged; a compact error would be rendered into that same response at each call site")]`
(two `;`, no `"`). Do not copy the legacy one-`;` `allow` text — a new identity
must match `"[^";]+;[^";]+;[^";]+"` (`source_rules.py:258`).

Gate facts for the new file: no statics, no spawn, no macros, no `#[path]`,
no `use ... *` → nothing for `owners.json`. No `crate::http`/`crate::product`
reference and `super::perr` is not an `http|product` edge
(`architecture-gate.py:33-40`) → no `architecture-policy.json` row. mt_lint:
params `registry`/`sref` are not in `NAME_PARAMS`; no `.stream_ref(`.
`src/product/` is outside every `CRITICAL_PREFIXES` entry
(`verification_plan.py:22-31`) and unregistered → no mutation-owner row.

### 4.2 `src/product.rs` — 4,277 → 4,274 (ceiling = merge-base 4,277; may not grow)

| lines (current) | edit | Δ |
|---|---|---|
| 3324-3327 | delete the stale header `// Relay one segment's ConfigDeleteStep loop to its owner. Chunks the / caller's remaining step budget ... (durable progress either way).` + its trailing blank. That relay left product.rs for `src/application/consumer_remote.rs:105` (`/v1/internal/sweep-segment/`); the comment describes nothing that follows it. | −4 |
| 3372-3375 | prelude swap (below) | 0 |
| 3465-3468 | prelude swap | 0 |
| 3552-3555 | prelude swap | 0 |
| 3331-3334 | re-decide `internal_sweep_segment`'s `expect_used` reason (single-line string edit) | 0 |
| 3427-3430 | re-decide `internal_queue_cursor`'s `expect_used` reason | 0 |
| 4102 (before `mod scan;`) | `mod internal;` | +1 |

Prelude swap, identical at the three sites (4 lines → 4 lines):
```rust
    let desc = match internal::internal_desc(&state.registry, &sref).await {
        Ok(d) => d,
        Err(r) => return r,
    };
```
(`state.registry: Arc<Registry>` at `http.rs:172`; `&Arc<Registry>` derefs.)
The call is spelled through the module path so no `use` line is spent.

`#[expect]`-ratcheted functions touched (`source_rules.py:116-216`: identity =
`(path, qualified, kind, attribute text)`; metrics may not grow; a changed
reason is a new identity and is skipped):

1. `internal_sweep_segment` — `#[expect(clippy::expect_used, reason = "…")]`,
   fn-wide. The swap adds a `call-site` for `internal::internal_desc` and
   `path` facts for `internal_desc`/`Err`/`r`; the `expect_site:ordinary-call:*`
   and `expect_site:path:*` fingerprint keys are new (count 1 > 0) → the
   ratchet would fail under the unchanged reason. Remedy (RUST-QUALITY:41-53):
   re-decide the reason. New text:
   `"internal_sweep_segment; the outcome is rendered only after the typed registry prelude and the incarnation check admitted the target, and it derives Serialize with plain fields, so converting it to a JSON value cannot fail; a fallible conversion would turn a completed operation into a spurious wire error"`
   (two `;`, no `"`).
2. `internal_queue_cursor` — same lint, same mechanism. New text:
   `"internal_queue_cursor; the position is rendered only after the typed registry prelude and the incarnation check admitted the target, and it derives Serialize with plain fields, so converting it to a JSON value cannot fail; a fallible conversion would turn a completed operation into a spurious wire error"`.
3. `internal_segment_scan` — `#[expect(clippy::too_many_lines, reason = "…")]`,
   fn-wide. Not an unwrap/expect lint, so only `scope_lines`, `nested_items`,
   `syntax_facts` are ratcheted. `scope_lines`: 4 → 4 (unchanged).
   `nested_items`: unchanged. `syntax_facts` (scanner kinds, `tools/quality-syntax/src/scan.rs:222-255`):
   before ≈ 11 (`method-call` + `method-call-site` for `.get`, paths `state`,
   `sref`, `Ok`, `Some`, `d`, `perr`, `StatusCode::NOT_FOUND`, `None`,
   `call-site perr`); after ≈ 8 (`call-site` + path `internal::internal_desc`,
   paths `state`, `sref`, `Ok`, `d`, `Err`, `r`). Non-growing → reason kept.
   The gate names any miscount exactly (`accepted exception grew without a new
   decision: … syntax_facts N -> M`); if it does, re-decide this reason too.
4. `perr`, `json_ok` (`unwrap_used`) — not touched.

None of the three callers of `perr` change how `perr` is spelled, and
`internal_sref`/`verify_internal_target` stay where they are, so
`docs/quality/source-allowances.json` rows for product.rs remain live (no
prune), the `crate::internal_segment_scan` `macro-dsl` allowance keeps its
path, and the MT audit's `internal-target` fingerprints (`product.rs:3220,
3261-3262, 3291, 3307`) are untouched.

clippy sanity on the edited handlers: `match_same_arms` — the two arms differ;
`needless_pass_by_value` — refs only; nesting unchanged; fn lengths unchanged.

### 4.3 `src/dst/tests/read_peer_compatibility.rs` — 281 → ~350 (ceiling 1,000)

- Module doc line 1 → `//! The scan RPC accepts old omissions without accepting malformed present bounds, and every internal receiver tells an unreadable registry apart from an absent stream.`
- Add `internal`, `answer`, and the test from §3 after `o2c_old_scan_headers_…`.

### 4.4 `docs/refactor/WIRE-MATRIX.md` (§3 fleet-internal, lines 222-224)

- 222 sweep-segment: `404 not_found` → `404 not_found (no descriptor for the name); 503 temporarily_unavailable (retryable — the registry could not be read; never a "gone" signal)`.
- 223 queue-cursor: `404` → same pair.
- 224 segment-scan: `404` → same pair.
- 218 intro, append one sentence: `The three product.rs receivers share one registry prelude (`internal_desc`, `src/product/internal.rs`); the sealed-span sender (`scan_page_once`, `src/application/read_remote.rs`) reads 404/410 as incarnation gone (typed cutoff) and 429/503 as a bounded retry.`

### Untouched on purpose
`src/http.rs` (3,371/3,372), `src/sse/source.rs`, `src/sse/feed.rs`,
`src/application/read_remote.rs`, `src/product/tests.rs`,
`scripts/quality/mutation_owners.py`, `docs/quality/owners.json`,
`docs/refactor/architecture-policy.json`, `scripts/mt-audit-baseline.txt`,
`src/dst/tests/README.md`.

One commit. No verbatim-move commit is needed (nothing moves).

## 5. Mutation-kill analysis

Planner outcome for this diff (`scripts/quality/verification_plan.py:73-101`):
changed Rust = `src/product.rs`, `src/product/internal.rs`,
`src/dst/tests/read_peer_compatibility.rs`; none is under
`CRITICAL_PREFIXES`, registered, or forced → `mutation_source_files: []`,
`mutants: false`, only the `compiler` leg. No mutation experiment runs and no
`mutation_owners.py` row is required (RUST-QUALITY:84-91 — registration is
mandatory only for critical paths). Recorded so nobody adds a row to be safe:
registering `src/product/internal.rs` would also pull nothing else in (one
file per row), but is not required.

Kill map anyway, so the helper is safe the day `src/product/` is registered
(cargo-mutants 27.1.0 in-diff mutants for `internal_desc`):

| mutant | killed by |
|---|---|
| `replace internal_desc -> Result<StreamDesc, Response> with Err(Default::default())` (`Response: Default` = empty 200) | every receiver early-returns an empty 200: `o2c_old_scan_headers_…` does `response.json()` on it → `unwrap` panics; the new test's `answer()` likewise (the failpoint is never consumed, status 200, empty body → `json()` fails). |
| `… with Ok(Default::default())` | unviable — `StreamDesc` has no `Default` (`registry.rs:398-400`); cargo-mutants skips it. |
| match-arm content swaps | cargo-mutants does not mutate arm selection; the `Ok(None)` vs `Err` distinction is a behaviour, pinned by the new test (503/`temporarily_unavailable`/`true` vs 404/`not_found`/`false`) and by the absent-name control. |

The three prelude swaps in product.rs contain no operators, guards or literals
cargo-mutants rewrites. The DST file is a test file.

## 6. Ledgers

| ledger | action |
|---|---|
| `docs/refactor/test-inventory.json` | `python3 scripts/test-inventory.py --write` then `--check` (adds the new entry: name, attributes, `function_sha256`, `configuration: ["let desc = seed(&rig).await;"]`, `scenarios: []`). The full-suite floor in `tests_ran.py` rises by one automatically. |
| `docs/refactor/WIRE-MATRIX.md` | rows 218, 222-224 per §4.4 (wire change: new 503 class on three internal routes). |
| `docs/quality/owners.json` | no change — new file has no static/spawn/macro/`#[path]`; the new test uses no `json!` (the module's existing `serde_json::json` row is keyed to another fn). |
| `docs/quality/source-allowances.json` | no change and no `--prune` — no allowance goes stale (nothing moved; the two re-decided `expect_used` reasons are not ledgered there: the file holds zero `expect_used`/`unwrap_used` rows). |
| `docs/refactor/architecture-policy.json` | no change — `src/product/internal.rs` has zero `crate::http`/`crate::product` edges. |
| `scripts/quality/mutation_owners.py` | no change (§5). |
| `scripts/mt-audit-baseline.txt` | no change — no `streams-internal-(epoch\|seg\|identity\|project)` site moves. (The audit already reports 2 GONE product.rs rows from an earlier move to `read_remote.rs`; GONE is a notice, not a FAIL.) |
| `src/dst/tests/README.md` | no change — no new DST module. |

## 7. Controls

Run from `/Users/sorenschmidt/code/streams` on `slate`, after the current gate
run finishes.

1. Red first — add only §3 to the DST module, then:
   `cargo test --release --lib -- --exact dst::dst_tests::read_peer_compatibility::o2c_registry_store_error_on_a_receiver_is_retryable_not_gone`
   Expect the panic in §3 verbatim (`left: […(404, "not_found", Some(false))…]`).
2. Apply §4.1-4.4, re-run step 1 → `test result: ok. 1 passed`. Then the
   module and the target-verification unit tests:
   `cargo test --release --lib -- dst::dst_tests::read_peer_compatibility product::tests::internal_target`
   → 4 + 6 passed.
3. `cargo clippy --locked --workspace --all-targets -- -D warnings` — decides
   `result_large_err` on `internal_desc` (§4.1). Also proves no
   `unfulfilled_lint_expectations` on the two re-decided `expect_used` scopes
   (their `.expect(...)` calls are unchanged).
4. `cargo fmt --all -- --check`.
5. `python3 scripts/test-inventory.py --write && python3 scripts/test-inventory.py --check`.
6. `scripts/quality.sh` — runs `gate.py` (exception ratchet: expect no
   `accepted exception grew` line; if one names `internal_segment_scan`
   `syntax_facts`, re-decide that reason and re-run), `architecture-gate.py --check`
   (file/function budgets, reverse edges), `scenario-map-report.py --check`,
   `test-inventory.py --check`, the `mt_lint::multitenancy_identity_lint` leg.
7. `scripts/multitenancy-audit.sh` → `MT_AUDIT_OK`, zero `+` rows.
8. Budgets: `wc -l src/product.rs` = 4274; `src/http.rs` = 3371 (untouched);
   `src/product/internal.rs` ≤ 40; `src/dst/tests/read_peer_compatibility.rs` < 1000.
9. Run CI's plan before push (memory rule) and confirm it shows
   `mutants: false`, `mutation_source_files: []`, compiler leg only.
10. Full suite as CI runs it: `cargo test --release -- --skip post_split_throughput_scales`.
11. After push: `gh run view` on the new run — never claim green without it.

## 8. Out of scope / follow-ups

1. `src/http.rs` `internal_segment_read` / `internal_segment_close`: same
   decision, different envelope (`err_resp`, no `retryable`). Unify on
   `internal_desc` once http.rs has line budget (it is at 3,371/3,372 and is
   the registered `http` mutation owner; a touch selects a mutation run under
   the `http:: livefeed_engine_retired security_workload::` filters). Wire
   note: that would add `retryable` to two internal error bodies.
2. `internal_segment_close` gates on `desc_alive` (404 for a dead descriptor
   even when the epoch matches) while the other four receivers do not — decide
   once, in its own item.
3. The reviewer's `live_descriptor` for the public preludes (four in
   `product.rs`: 1544-1569 metadata with its two `desc_alive` arms, 1597-1622,
   2076-2100, 2634-2658; plus the copy in `src/product/scan.rs`): they answer
   `500 internal` on a registry error; whether customers should see 503 there
   is a public wire decision, separate from this item.
4. MT audit coverage: `scan internal-target … src/*.rs src/config/*.rs` is a
   shallow glob; `src/application/read_remote.rs` (`InternalTarget::headers`)
   already escaped it (2 stale baseline rows). Widen to `src/**/*.rs` and
   `--regen` in a commit of its own.
5. `check_key`'s `_ =>` catch-all on `KeyCheck` at `product.rs:3564` — style
   debt (RUST-QUALITY "no `_ =>` on domain enums"), untouched here.
6. `RemoteSpanError::TargetGone` doc (`read_remote.rs:62`) could say
   "404 = no descriptor at the owner" now that the receiver guarantees it.
7. A `Retry-After` on the new 503 — not sent by any internal receiver today;
   add only if a sender starts honouring it.

## Skeptic corrections (C1..C9)

Verified on `slate` @ `df9ff212` (HEAD; one local commit past the plan's
`8dabca7f`, which is still `origin/slate`). Every §1-§7 citation was re-read
by content; the three `registry.get(&sref)` sites are still at
`src/product.rs:3372`, `:3465`, `:3552` and are byte-identical `_ =>` 404s.
The mechanism, the red test's failure path, the gate analysis and the ledger
table all hold. What follows are the concrete misstatements.

**Verdict: ready-with-corrections.** No unbuildable control, no missed
ledger or ratchet.

### C1 — line counts and line numbers are stale (HEAD moved)

- `wc -l src/product.rs` = **4,207** on `df9ff212` (the plan's 4,277 is the
  `8dabca7f` figure; `df9ff212` moved `product_consumer_pull` out). The edit
  is still net −3 → **4,204**, not 4,274. The ceiling is
  `min(max(1000, legacy 4,920), max(1000, merge-base lines))`
  (`scripts/quality/source_rules.py:226-228`, `source_gate.py:27,45-50`): the
  merge-base is `origin/slate` = `8dabca7f` = 4,277 until `df9ff212` is
  pushed, then 4,207. Either way −3 fits. Fix §4.2 heading and §7 step 8
  (`= 4204`).
- The `mod` block is at `src/product.rs:4030-4036` (`mod consumer_pull;` /
  `use consumer_pull::…;` / `mod scan;` …), not 4102. `mod internal;` between
  `use consumer_pull::product_consumer_pull;` and `mod scan;` keeps the
  consecutive-`mod` group alphabetical for rustfmt.
- `src/dst/tests/read_peer_compatibility.rs` is **280** lines by `wc -l`
  (281 by `splitlines` only if a trailing newline were missing; it is not).
- MT-audit fingerprint sites cited as `product.rs:3220, 3261-3262, 3291,
  3307` are actually `:3228`, `:3269-3270`, `:3299`, `:3315`. No impact:
  `scripts/mt-audit-baseline.txt` is content-keyed (`scripts/multitenancy-audit.sh:67`),
  so the −4 line deletion above them changes nothing.

### C2 — the mt_lint rationale for naming the param `path` is wrong (choice is fine)

`src/mt_lint.rs:49-52` and `:362-370`: `src/dst/` is **excluded by
directory** ("src/dst/ (harness + tests) … stay out by directory"). The lint
never sees the new test helper, so `name: &str` would not be flagged there.
Keep `path` anyway (it reads correctly); drop the "DST files are walked"
sentence from §3.

### C3 — "429/503 as a bounded retry" is not what the sender does

`remote_span_page` returns `Retryable` unretried
(`src/application/read_remote.rs:137` `Err(e) => return Err(e)`; only
`WrongOwner` gets the second hop). What happens next is decided per caller:
- SSE sealed span: `src/sse/source.rs:516-518` `anyhow::bail!` →
  `src/sse/feed.rs:941-942` `DriveOutcome::SourceFailed` → the session parks
  and retries on the next wake (the feed's cadence, not a bounded loop).
- Product scan relay: `src/application/read_scan.rs:213-226` →
  `ReadFailure::Remote` → `src/product.rs:2965-2971` **503
  `temporarily_unavailable`, retryable true** to the customer.
- Raw read relay: `src/http/read.rs:51-55` **503 `temporarily_unavailable`**.

Reword the §4.4 intro sentence to: "the sealed-span sender (`scan_page_once`,
`src/application/read_remote.rs`) reads 404/410 as incarnation gone (typed
cutoff) and 429/503 as `Retryable`, which the SSE feed parks on and the
scan/read relays render as their own 503."

### C4 — §1's "not broken" inventory is missing two senders (both status-agnostic)

Add to §1: the product scan relay (`read_scan.rs:213-226` →
`product.rs:2965` folds every `Remote(_)` into 503) and the raw read relay
(`http/read.rs:51`, same) already answer 503 for ANY `RemoteSpanError`, so
the 404→503 change on `segment-scan` is invisible to them. Together with the
two consumer relays (`consumer_remote.rs:41-44`, `:129-132`, any non-2xx →
`None`) that is the complete sender set for the three routes
(`grep -rn "segment-scan\|queue-cursor\|sweep-segment" src` outside
`src/dst/tests`: `http.rs:392-393,1383-1392`, `read_remote.rs:163`,
`consumer_remote.rs:25,105`). The SSE sealed-span path is the only
variant-sensitive consumer — as the plan says, but it should say it was
checked.

### C5 — the test-inventory `configuration` list will not be what §6 says

`scripts/test-inventory.py:126-127` keeps every body line matching
`FaultPlan::|failpoint|\.require\(|\bseed\b|start_paused|worker_threads`
— comments included. The plan's test body contains the comment
`// The failpoint is one-shot: …`, so `--write` records TWO configuration
lines (`let desc = seed(&rig).await;` and that comment). Harmless (it is
generated), but either accept it or write the comment as "one-shot store
error" so the ledger carries only the real configuration line.

### C6 — `internal_desc` doc comment is "what it does"; repo style wants owner/invariant

The `///` in §4.1 opens with "`Ok(None)` is 404 not_found; a store error is
503 …" — a wire table, not the reason the fn exists. The module `//!` already
carries the reason; make the fn doc the invariant, e.g. "The receivers may
answer 404 only for a name the registry has positively answered `None` for;
an unreadable registry answers nothing about the incarnation, so it must not
answer `not_found`. No liveness gating: the incarnation check that follows
binds the request, and a dead descriptor whose epoch matches is still the one
the sender addressed." Keep the table in WIRE-MATRIX (§4.4).

### C7 — failpoint hazard to state (not a blocker)

`fail_next_get` is keyed by bare stream name and consumed by ANY
`Registry::get` for that name, before the cache and regardless of project
(`src/registry/cache.rs:298-304`, `:313-318`). The plan arms immediately
before each send, which is the right mitigation; note in §3 that the idle
rig has no background reader of `compat` (the scaler controller reads the
registry only on a split decision, `src/scaler3/controller.rs:117`, which
three 1-byte records never trigger; the absorber is cold), so the one-shot
cannot be stolen between arm and send. Same accepted pattern as
`consumer_saga.rs:92`.

### C8 — `result_large_err`: state the failure mode of guessing wrong

The empirical decision in §4.1 is correct; add why it must precede the
commit: `clippy.toml` sets no `large-error-threshold` (default 128 B), the
tree carries the legacy `allow` on an `async fn` with the same Err type
(`src/http.rs:535-544` `engine_for_quiet`), so the lint plausibly fires on
`internal_desc`; but `unfulfilled_lint_expectations = "deny"`
(`Cargo.toml:118`) means an `#[expect]` that does NOT fire is itself a build
failure. §7 step 3 is therefore a required decision point, not a check.
If it fires, the proposed two-`;` reason passes `source_rules.py:258`; the
new identity is skipped by `exception_growth` (`:207-208`) and by the
inventory (`:263-264`).

### C9 — ratchet arithmetic confirmed; record the identity rule precisely

Recounted from `tools/quality-syntax/src/scan.rs:222-255` (path facts are
emitted for pattern paths `Ok`/`Some`/`Err` too): current prelude = 11 facts
(method-call, method-call-site, `state`, `sref`, `Ok`, `Some`, `d`,
call-site `perr`, `perr`, `StatusCode::NOT_FOUND`, `None`); new prelude = 8
(call-site, `internal::internal_desc`, `state`, `sref`, `Ok`, `d`, `Err`,
`r`). `internal_segment_scan`'s `too_many_lines` contract
(`scope_lines`/`nested_items`/`syntax_facts`) does not grow — keep its reason.
For the two `expect_used` scopes the new `expect_site:ordinary-call:*` and
`expect_site:path:*` keys (`source_rules.py:176-188`) go 0→1 under the old
reason, so the re-decision is mandatory, as the plan says. Both proposed
reasons have exactly two `;` and no `"`. `docs/quality/source-allowances.json`
holds zero `expect_used`/`unwrap_used` rows (verified), so nothing goes stale
and no `--prune` is needed.

### Checked and confirmed (no correction)

- Red path traced on all three routes: auth (`fleet_operation_authorized`,
  static `dst-internal-token`, `fixture_http.rs:439-441`) → header/body
  parse → `internal_sref` → `registry.get` is the first fallible step;
  today's `_ =>` yields exactly `(404, "not_found", Some(false))` ×3, so the
  `assert_eq!` panics with the message in §3. After §4 the same three yield
  `(503, "temporarily_unavailable", Some(true))`; the second scan serves 200
  (failpoint consumed before the cache, cache untouched); `absent` → `Ok(None)`
  → 404 on both trees (a true control).
- No existing test or doc pins a 404 for a registry error on these routes
  (`grep` over `docs/` for the three route names: only WIRE-MATRIX rows
  222-224; `fail_next_get` has three unrelated users).
- `src/product/internal.rs`: zero `crate::http|product` edges (`super::perr`
  is not an edge, `architecture-gate.py:33-40`); not in `CRITICAL_PREFIXES`
  (`src/product_cursor` does not prefix `src/product/`); not registered in
  `mutation_owners.py`; no static/spawn/macro/`#[path]`/glob → no
  `owners.json`, `architecture-policy.json`, `mutation_owners.py` or
  `README.md` row. `verification_plan.py:73-101` → `mutants: false`.
- `src/dst/tests/read_peer_compatibility.rs` is `test_only`
  (`architecture-report.py:237-243`) so its `crate::http`/`crate::product`
  mentions are not budgeted; the new code adds none anyway.
- `StreamDesc` has no `Default` (`registry.rs:398-400`); `Registry` is
  `pub(crate) struct` (`registry.rs:839`); `state.registry: Arc<Registry>`
  (`http.rs:172`); `Registry::get` lives in `impl Registry`
  (`registry/cache.rs:264,294`).
- `product::tests::internal_target_*` = 6 tests; `verify_internal_target`
  untouched.
- `Vec<(&str, (u16, String, Option<bool>))> == [_; 3]` compiles
  (`PartialEq<[U; N]> for Vec<T>`); every wait bounded; helper has 4 params;
  test fn < 100 lines; DST file lands ≈ 375 / 1,000.
