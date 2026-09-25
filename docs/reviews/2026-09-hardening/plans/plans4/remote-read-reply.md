# Plan — review item 22: `remote_read_page` rebuilds `ReadFailure` from the peer status

Tree: `/Users/sorenschmidt/code/streams`, branch `slate`, HEAD `8dabca7f` (all line numbers below are from this revision; the reviewer's numbers were stale and every site was re-found by content).

Verdict on the reviewer's Change: the diagnosis is correct and the direction (a typed refusal body owned by `read_remote.rs`, decoded by the coordinator, bare-409 arm deleted, no `error.code` sniffing) is buildable. Two of its details are NOT buildable/advisable as written and are replaced below:

1. `WireReadReply{Page, Refused}` as an externally-tagged wire enum would change the SUCCESS body shape (`{"page":{...}}`). An old coordinator relaying to a new owner would then fail every page decode during a rolling deploy. The page body stays exactly `WireReadPage`; only the refusal gets its own body shape, and it keeps the public status so old coordinators are unaffected in both directions.
2. "Serialise Refused ... before the match" cannot be done inside `read_inner` in place: `read_inner` carries three function-wide `#[expect]`s whose scope is ratcheted on `scope_lines`/`syntax_facts`; any added call grows them. The tail of `read_inner` is extracted into a new fn instead (metrics shrink), and the same ratchet forces a decision on `remote_read_page`'s `#[expect(clippy::expect_used)]` (§4).

---

## 1. Problem (verified on 8dabca7f)

### 1.1 The owner renders a relayed read verdict through the PUBLIC renderer

`src/http.rs:3281-3357` `internal_segment_read` (the `/v1/internal/segment-read/{*name}` handler registered at `src/http.rs:1374-1377`) ends by delegating to the public handler:

```rust
// src/http.rs:3347-3356
    read_inner(
        state,
        sref,
        params,
        headers,
        head_only,
        true,
        SseSurface::Raw,
    )
    .await
```

`src/http/read.rs:237-252` (`read_inner` tail) — the failure arm is the public renderer regardless of the page header; only the SUCCESS path is typed:

```rust
// src/http/read.rs:237-252
    let out = match state.read_service().execute_read(command).await {
        Ok(out) => out,
        Err(error) => return read_failure_response(error),
    };
    if params.internal
        && headers
            .get("streams-internal-read-page")
            .and_then(|v| v.to_str().ok())
            == Some("1")
    {
        return axum::Json(crate::application::read_remote::WireReadPage::from_outcome(
            &out,
        ))
        .into_response();
    }
    render_raw_read(&state, &params, &headers, key.as_ref(), out)
```

`src/http/read.rs:30-39` — the public renderer makes the two verdicts indistinguishable by status (both bare 409, no `streams-replay-to`):

```rust
// src/http/read.rs:30-39
        E::ChangedIncarnation => err_resp(
            StatusCode::CONFLICT,
            "target_mismatch",
            "stream incarnation changed",
        ),
        E::CursorBeyondTail => err_resp(
            StatusCode::CONFLICT,
            "cursor_beyond_tail",
            "cursor is ahead of the stream tail; resume from the durable cursor",
        ),
```

### 1.2 The coordinator reconstructs the verdict from the status

`src/application/read_remote.rs:380-411` (`remote_read_page`):

```rust
// src/application/read_remote.rs:380-411
        let status = response.status();
        if status.as_u16() == 409
            && let Some(next) = response
                .headers()
                .get("streams-replay-to")
                ...
        {
            ... // one verified redirect / RedirectLoop
        }
        if !status.is_success() {
            return Err(match status.as_u16() {
                404 => ReadFailure::Missing,
                410 => ReadFailure::Gone,
                401 => ReadFailure::Remote(RemoteSpanError::Unauthorized),
                409 => ReadFailure::ChangedIncarnation,
                429 | 503 => ReadFailure::Remote(RemoteSpanError::Retryable {
                    status: status.as_u16(),
                    code: None,
                }),
                _ => ReadFailure::Remote(RemoteSpanError::InvalidResponse(format!(
                    "read peer status {status}"
                ))),
            });
        }
```

Line 402 is the bug: every 409 without a routing header — the owner's `cursor_beyond_tail`, the owner's `target_mismatch`, and `verify_internal_target`'s `stale_target` (`src/product.rs:3251-3259`) — becomes `ChangedIncarnation`.

### 1.3 The verdict that gets lost is produced on the owner only in applied mode

`src/application/read_request.rs:291-293` (`execute_read`):

```rust
            if command.visibility == Deliver::Applied && start > end {
                return Err(ReadFailure::CursorBeyondTail);
            }
```

and the relay is taken exactly when the coordinator is not the owner (`src/application/read_request.rs:236-247`):

```rust
                    if let crate::shard_directory::ResolveError::NotOwner { owner, .. } = &error {
                        if command.allow_remote && !matches!(command.mode, ReadMode::LongPoll(_)) {
                            return crate::application::read_remote::remote_read_page(
                                &self.peer,
                                owner,
                                &command,
                                span.seg_id,
                                start,
                            )
                            .await;
```

`remote_read_page` forwards applied mode (`read_remote.rs:367-369`, header `streams-internal-deliver: applied`), the owner installs it (`src/http.rs:3312-3318`), so a stale applied cursor on a foreign segment is refused by the owner as `CursorBeyondTail`, arrives at the coordinator as `ChangedIncarnation`, and the product edge renders it as (`src/product.rs:2929-2934`):

```rust
        E::ChangedIncarnation => (
            StatusCode::CONFLICT,
            "target_mismatch",
            "stream incarnation changed",
```

instead of `cursor_beyond_tail` (`src/product.rs:2936-2941`). The SDK's rewind contract (`src/dst/tests/reads_applied.rs:429-433`: "Presenting it ... in applied mode is a 409 `cursor_beyond_tail`; the durable cursor from the same response resumes cleanly") is broken exactly on the cross-owner path, which is the path R06 introduced. `src/product/tests.rs:257-261` documents the mirror-image bug already fixed once ("an ownership 409 translated to cursor_beyond_tail told SDKs to rewind healthy cursors"); this is the remaining half.

Also verified: `RUST-QUALITY.md:64` ("Domain decisions MUST NOT be reconstructed from HTTP responses, JSON blobs or error-display strings") is violated by line 402, and no test pins the relayed applied verdict (grep of `streams-internal-deliver`/`Deliver::Applied` in `src/dst/tests` finds only single-instance uses).

---

## 2. Contract decision

### 2.1 Typed refusal on the page route (owner side)

On `/v1/internal/segment-read/{*name}` **with** `streams-internal-read-page: 1`, a read verdict the owner DECIDED is answered with its own body shape:

| `ReadFailure` at the owner | HTTP status (unchanged) | body |
|---|---|---|
| `CursorBeyondTail` | 409 | `{"refused":"cursor_beyond_tail"}` |
| `ChangedIncarnation` | 409 | `{"refused":"changed_incarnation"}` |
| `Missing` | 404 | `{"refused":"missing"}` |
| `Gone` | 410 | `{"refused":"gone"}` |

Every other failure keeps the public error envelope: `Resolve(NotOwner)` (409 `not_ring_owner` + `streams-replay-to`, the one routing hint the coordinator follows), `Resolve(Opening)`/`Creating`/`Remote` (503-class), `Storage`/`Resolve(OpenFailed)` (500), and the coordinator's own mistakes (`MissingKey`/`WrongKey`/`InvalidCursor`/`KeylessLive`/`AppliedFork`, 400/403). Answers produced BEFORE the read service runs (fleet gate 401, registry miss 404, `invalid_target` 400, `stale_target` 409, admission 429/503) are untouched.

The status is deliberately kept: an OLD coordinator ignores the body and behaves exactly as today; a NEW coordinator against an OLD owner sees the envelope, not a typed body, and falls to the status class (§2.2). The codes are the `snake_case` of the `ReadFailure` variant names (serde-derived), a 1:1 with the application verdict — NOT the public `error.code` vocabulary (`target_mismatch`), which stays the renderer's business.

### 2.2 Coordinator decode (`remote_read_page`)

Per hop, in this order:

1. transport error → `Remote(Transport)` (unchanged);
2. 409 + `streams-replay-to` → the one verified redirect / `RedirectLoop` (unchanged, header-driven);
3. the body is read (bounded by `read_wire::body`, as pages already are);
4. 2xx → `WireReadPage` decode (unchanged);
5. non-2xx → `peer_refusal(status, &body)`: a body that decodes as `{"refused": ...}` yields the SAME `ReadFailure` the owner decided, whatever status carried it; otherwise ONLY the transport class of an untyped answer is used: 401 → `Remote(Unauthorized)`, 404 → `Missing`, 410 → `Gone`, 429|503 → `Remote(Retryable{status, code: None})`, anything else **including a bare 409** → `Remote(InvalidResponse("read peer status N without a typed refusal"))`.

The bare-409 arm is deleted. Consequence (documented, intended): `stale_target` from `verify_internal_target`, and a genuine verdict from an old owner during a rolling deploy, now surface at the product edge as 503 `temporarily_unavailable` (retryable, `src/product.rs:2970-2977` `E::Remote(_)` arm) instead of a non-retryable 409 `target_mismatch`. A retry after caches/rollout converge yields the typed verdict; the old mapping told the SDK to give up on a segment-map lag that was never an incarnation change.

No `error.code` string is matched anywhere on this path (`Retryable.code` stays `None`).

### 2.3 Types (owned by `src/application/read_remote.rs`)

```rust
/// The read verdicts an owner decided and a coordinator relays as its own.
/// On the wire they are their own body, never the public error envelope, so
/// the relaying instance decodes a decision instead of guessing one from a
/// status (a relayed cursor_beyond_tail surfaced as target_mismatch and
/// broke the SDK's rewind on cross-owner applied replays).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WireReadRefusal { CursorBeyondTail, ChangedIncarnation, Missing, Gone }

/// The page route's refusal body: `{"refused": <verdict>}`.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct WireRefusedPage { pub(crate) refused: WireReadRefusal }

impl WireReadRefusal {
    /// Only verdicts about the stream are relayed; a failure the coordinator
    /// caused (its key, its cursor) or must retry is not a verdict.
    pub(crate) fn of(failure: &ReadFailure) -> Option<Self> { /* exhaustive match, no `_` arm */ }
}
impl From<WireReadRefusal> for ReadFailure { /* four arms */ }

/// The owner's decision for a refused page, or the transport class of an
/// answer no read verdict produced. A 409 with neither routing header nor
/// verdict is nobody's decision, so it is invalid rather than an incarnation
/// change the owner never declared.
fn peer_refusal(status: u16, body: &[u8]) -> ReadFailure { /* §2.2 step 5 */ }
```

`of` lists every `ReadFailure` variant explicitly (`Creating | MissingKey | WrongKey | InvalidCursor | KeylessLive | AppliedFork | Resolve(_) | Storage(_) | Remote(_) => None`) so a new variant is a compile error, per the no-catch-all rule.

---

## 3. Red tests

All three are written FIRST and must be red on 8dabca7f before any production edit.

### 3.1 DST, end to end through the real relay — `src/dst/tests/read_application.rs` (310 lines now)

Name: `relayed_applied_read_beyond_the_tail_keeps_the_owner_verdict`
Attribute: `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`
Rig: the single-rig loopback already proven by `src/dst/tests/read_page_limits.rs:27-135` (`http_rig_build(mem(), RigRuntime::first(), HttpRigOptions::default())`, then `state.peer.set_peer("relay-owner", &format!("http://{}", rig.addr))`). Create `relay-tail` via `preq PUT /v1/streams/relay-tail {"format":{"kind":"json"}}` (201), POST two records to `/v1/streams/relay-tail/records`, load `desc` from the registry.

Command: the file's `command(&desc)` helper with `start = ReadStart::Position(ReadPosition { segment: 0, after: 100 })`, `visibility = crate::shard::Deliver::Applied`, `refresh = false`.

Assertions:

```rust
let local = state.read_service().execute_read(command.clone()).await.err()
    .expect("an applied read beyond the tail is refused locally");
assert!(matches!(local, ReadFailure::CursorBeyondTail), "local verdict: {local:?}");   // control, green today
let relayed = remote_read_page(&state.peer, "relay-owner", &command, 0, 100).await.err()
    .expect("an applied read beyond the tail is refused through the relay");
assert!(
    matches!(relayed, ReadFailure::CursorBeyondTail),
    "the relayed verdict must be the owner's cursor_beyond_tail: {relayed:?}"
);
engine_shutdown(&state).await; rig.tasks.shutdown(Duration::from_secs(5)).await;
```

Why it is red today (traced): the owner takes `internal_segment_read` → `read_inner` → `execute_read` with `start=100 > end=2` under `Applied` → `Err(CursorBeyondTail)` → `read_failure_response` → 409 `cursor_beyond_tail`; the coordinator hits `read_remote.rs:402`.

Exact expected red output:

```
thread 'dst::dst_tests::read_application::relayed_applied_read_beyond_the_tail_keeps_the_owner_verdict' panicked at src/dst/tests/read_application.rs:<L>:5:
the relayed verdict must be the owner's cursor_beyond_tail: ChangedIncarnation
```

### 3.2 DST, the owner's wire body and the public route's envelope — same file

Name: `the_page_route_types_its_refusal_and_the_public_route_keeps_its_envelope`
Attribute: `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`
Rig: same single rig, same `relay-tail` stream (2 records). Headers built as in `src/dst/tests/billing_usage.rs:207-217` (`authorization: Bearer dst-internal-token`, `stream-encryption-key: PRISMA_KEY`, the four `InternalTarget::of(&desc, 0).headers()`), plus `streams-internal-read-page: 1`, `streams-internal-deliver: applied`, `streams-internal-max-bytes: 4096`.

```rust
let offset = crate::offsets::encode_ep(0, crate::offsets::Offset(Some(99)));   // scan_from() == 100
let (status, _, body) = preq(addr, "GET",
    &format!("/v1/internal/segment-read/relay-tail?offset={offset}"), &hdr_refs, b"").await;
assert_eq!(status, 409, "{}", String::from_utf8_lossy(&body));
let reply: serde_json::Value = serde_json::from_slice(&body).unwrap();
assert_eq!(reply["refused"], "cursor_beyond_tail",
    "the page route answers the owner's typed verdict, got {reply}");
// The public route never speaks the fleet vocabulary, header or not.
let (status, _, body) = preq(addr, "GET", "/v1/stream/relay-tail",
    &[("stream-encryption-key", PRISMA_KEY), ("streams-internal-read-page", "1")], b"").await;
assert_eq!(status, 200);
let public: serde_json::Value = serde_json::from_slice(&body).unwrap();
assert!(public.is_array(), "the public route renders records, never the page DTO: {public}");
// ...and a public refusal keeps the public envelope.
let (status, _, body) = preq(addr, "GET", "/v1/stream/relay-absent",
    &[("stream-encryption-key", PRISMA_KEY)], b"").await;
assert_eq!(status, 404);
let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
assert_eq!(envelope["error"]["code"], "not_found", "{envelope}");
```

Exact expected red output today (first assertion that fails; the body is the public envelope from `read_failure_response`):

```
thread 'dst::dst_tests::read_application::the_page_route_types_its_refusal_and_the_public_route_keeps_its_envelope' panicked at src/dst/tests/read_application.rs:<L>:5:
assertion `left == right` failed: the page route answers the owner's typed verdict, got {"error":{"code":"cursor_beyond_tail","message":"cursor is ahead of the stream tail; resume from the durable cursor"}}
  left: Null
 right: "cursor_beyond_tail"
```

(The last two blocks are green today; they exist to kill the guard mutants in §5 and to pin that the header is inert on the public route.)

### 3.3 Unit tests of the pure decoder — NEW `src/application/read_remote_tests.rs`

Mounted from `read_remote.rs` exactly like `read_wire.rs:125-127`: `#[cfg(test)] #[path = "read_remote_tests.rs"] mod tests;`. First line `use super::*;` (precedent). Plain `#[test]` (no tokio, no spawn, no `json!`, byte-string bodies only). Must not contain the identifiers `Response`, `HeaderMap`, `AppState`, `axum::`, `crate::http`, `crate::product` (architecture gate treats `src/application/*_tests.rs` as a hard owner file, `scripts/architecture-gate.py:98-106`).

`the_page_route_refusal_names_the_owner_verdict`:
- for `(CursorBeyondTail,"cursor_beyond_tail") (ChangedIncarnation,"changed_incarnation") (Missing,"missing") (Gone,"gone")`: `WireReadRefusal::of(&failure)` is `Some`, and `serde_json::to_string(&WireRefusedPage{refused}).unwrap() == format!("{{\"refused\":\"{code}\"}}")` — pins the wire codes;
- for `WrongKey, InvalidCursor, MissingKey, KeylessLive, AppliedFork, Creating, Storage("x".into()), Resolve(ResolveError::NotOwner{prefix:"0".into(), owner:"b".into()}), Remote(RemoteSpanError::Unauthorized)`: `of(..).is_none()`.

`a_typed_refusal_is_relayed_and_an_untyped_answer_keeps_its_transport_class`:
- `peer_refusal(409, br#"{"refused":"cursor_beyond_tail"}"#)` → `CursorBeyondTail`; `409 changed_incarnation` → `ChangedIncarnation`; `404 missing` → `Missing`; `410 gone` → `Gone`;
- the verdict wins over the status it travelled with: `peer_refusal(500, br#"{"refused":"cursor_beyond_tail"}"#)` → `CursorBeyondTail`;
- untyped: `(401, b"")` → `Remote(Unauthorized)`; `(404, br#"{"error":{"code":"not_found"}}"#)` → `Missing`; `(410, b"")` → `Gone`; `(429, b"")` → `Remote(Retryable{status:429, code:None})`; `(503, br#"{"error":{"code":"creating"}}"#)` → `Remote(Retryable{status:503, code:None})`;
- no code sniffing: `(409, br#"{"error":{"code":"cursor_beyond_tail","message":"x"}}"#)` and `(409, br#"{"error":{"code":"target_mismatch"}}"#)` and `(409, b"")` and `(500, b"")` → `Remote(InvalidResponse(_))`.

Expected red on 8dabca7f: does not compile —

```
error[E0433]: failed to resolve: use of undeclared type `WireReadRefusal`
 --> src/application/read_remote_tests.rs:...
error[E0425]: cannot find function `peer_refusal` in this scope
```

(the `mod tests;` line is added together with the file, so the crate fails to build until §4 lands — that is the red).

---

## 4. Edits, file by file

No ceilinged file is touched: `src/http.rs` (3,371 / 3,372) and `src/product.rs` (4,277) stay byte-identical, so no verbatim-move commit is needed. One commit: tests + fix + ledgers.

### 4.1 `src/application/read_remote.rs` — 420 lines now, budget 1,000; after ≈ 490

Hard-owner constraints apply (no `crate::http`/`crate::product`/`axum::`/`AppState`/`HeaderMap`/`Response` tokens; `InvalidResponse` is fine, no word boundary).

(a) After `impl WireReadPage` (line 315) add `WireReadRefusal`, `WireRefusedPage`, `impl WireReadRefusal { fn of }`, `impl From<WireReadRefusal> for ReadFailure` (§2.3). ~45 lines. `of` matches every variant by name.

(b) `remote_read_page` (lines 320-420). **Ratcheted**: it carries

```rust
#[expect(
    clippy::expect_used,
    reason = "remote_read_page; a fleet request carries no streaming body, so it is clonable; a fallible clone would add a branch no fleet request reaches"
)]
```

`scripts/quality/source_rules.py:exception_contracts` retains, under this identity, `scope_lines`, `syntax_facts`, `expect_sites` and per-callee fingerprints (`expect_site:ordinary-call:*`, `expect_site:path:*`); `exception_growth` fails on ANY new fingerprint. Replacing the status match with a call to `peer_refusal` is a new fingerprint, so the function cannot be edited under the unchanged exception. Chosen remedy — **remove the exception by removing its only site**: the closure handed to `peer.send` builds the request instead of cloning it (`peer.send` already re-invokes the closure with a fresh bearer after a 401, so "build per invocation" is the honest expression; `try_clone().expect(..)` was only there to satisfy `Fn`). Concretely, move lines 354-369 (client().get(..).query(..).timeout(..).header(..)×3, the `target.headers()` loop, the applied header) inside the closure at 371-377, drop line 372's `try_clone().expect("read request is clonable")`, and DELETE the `#[expect(clippy::expect_used, ...)]` at 320-323 (it becomes unfulfilled; `unfulfilled_lint_expectations` is denied, so leaving it is a compile error). Captures are shared borrows (`base`, `query`, `key`, `target`, `command`), so the closure stays `Fn`. Nesting depth stays 4 (the existing `if hop == 0 ...` inside the let-chain is already depth 4).

Then the tail (lines 380-417) becomes:

```rust
        let status = response.status();
        if status.as_u16() == 409 && let Some(next) = ... { /* lines 381-396 UNCHANGED, byte for byte */ }
        let bytes = read_wire::body(response.content_length(), response.bytes_stream())
            .await
            .map_err(ReadFailure::Remote)?;
        if !status.is_success() {
            return Err(peer_refusal(status.as_u16(), &bytes));
        }
        let page: WireReadPage = serde_json::from_slice(&bytes)
            .map_err(|e| ReadFailure::Remote(RemoteSpanError::InvalidResponse(e.to_string())))?;
        return page.into_outcome(command).map_err(ReadFailure::Remote);
```

Keep the redirect block textually identical so its lines are outside the diff hunks (its `== 409`/`hop == 0`/`!=` mutants have no killing test under the `read_remote` filter). Function stays under 100 lines (97 today; −15 match, −1 clone, +2 closure braces, +3 refusal call ≈ 86). `remote_read_page`'s doc comment gains one sentence: the owner's verdict is decoded from its typed body, the status only classifies an untyped answer.

(c) After `remote_read_page`, add `fn peer_refusal(status: u16, body: &[u8]) -> ReadFailure` (§2.2 step 5, ~20 lines; `_ =>` on a `u16` is not a domain enum).

(d) Last lines: `#[cfg(test)] #[path = "read_remote_tests.rs"] mod tests;`.

Fallback if (b)'s closure change is rejected in review: keep the clone and re-decide the reason text (new identity is exempt from the growth comparison) — still exactly two `;`, no `"` — e.g. `"remote_read_page; the page request is rebuilt per hop and cloned only for the bearer retry, so the clone of a bodyless fleet request cannot fail; a fallible clone would add a branch no fleet request reaches"`. This is the sanctioned remedy but the closure version deletes a panic site and an exception outright, which is the repo's stated preference.

Untouched neighbours: `scan_page_once` (same clone+expect pattern, same string matching on `error.code` at 212-220) — follow-up §8.

### 4.2 `src/http/read.rs` — 614 lines now, budget 1,000; after ≈ 640

(a) `read_inner` (143-253). **Ratcheted** by three function-wide exceptions (`too_many_lines`, `too_many_arguments`, `fn_params_excessive_bools`, lines 131-142), all on `scope_lines`/`syntax_facts`. Do NOT add anything inside it. Replace lines 237-252 with one call:

```rust
    respond_read(&state, command, &params, &headers, key.as_ref()).await
```

`read_inner` shrinks from 111 code lines to ≈ 96 → `clippy::too_many_lines` no longer fires → its `#[expect(clippy::too_many_lines, reason = "read_inner; ...")]` (131-134) becomes UNFULFILLED and must be deleted (denied lint). Verify with clippy (§7 step 4): if clippy instead still reports `too_many_lines` for `read_inner`, keep the attribute — either way the other two identities shrink, never grow. The dropped facts (`state.read_service()`, `execute_read`, `read_failure_response`, `params.internal`, `headers.get`, `axum::Json`, `WireReadPage::from_outcome`, `into_response`, `render_raw_read`) outnumber the one added call.

(b) New, after `read_inner`:

```rust
/// The page route answers in the coordinator's vocabulary — a typed page,
/// and a typed refusal where the read service decided a verdict — so the
/// relaying instance carries the owner's decision instead of rebuilding one
/// from a status. Everything else, and every public read, keeps the raw
/// rendering and the public error envelope.
async fn respond_read(
    state: &AppState,
    command: ReadCommand,
    params: &ReadParams,
    headers: &HeaderMap,
    key: Option<&StreamKey>,
) -> Response {
    let page = params.internal
        && headers.get("streams-internal-read-page").and_then(|v| v.to_str().ok()) == Some("1");
    match state.read_service().execute_read(command).await {
        Ok(out) if page => axum::Json(crate::application::read_remote::WireReadPage::from_outcome(&out)).into_response(),
        Ok(out) => render_raw_read(state, params, headers, key, out),
        Err(error) if page => typed_refusal(error),
        Err(error) => read_failure_response(error),
    }
}

/// A decided verdict swaps the envelope for its typed body; the status the
/// public renderer chose is kept so an older coordinator is unaffected.
fn typed_refusal(error: ReadFailure) -> Response {
    use crate::application::read_remote::{WireReadRefusal, WireRefusedPage};
    let refused = WireReadRefusal::of(&error);
    let mut response = read_failure_response(error);
    if let Some(refused) = refused {
        *response.body_mut() = axum::Json(WireRefusedPage { refused }).into_response().into_body();
    }
    response
}
```

Five params, no bool params, references only (avoids `needless_pass_by_value`), no `#[expect]`. `err_resp` (`src/http.rs:1652-1659`) sets only `content-type`; hyper derives `content-length` from the swapped `Bytes` body, so no header surgery. `http/read.rs` is a `transport_and_composition_files` entry, so the application import is allowed; `read_failure_response` is not ratcheted.

### 4.3 `src/dst/tests/read_application.rs` — 310 lines now; after ≈ 430 (DST files are under the 1,000 ceiling)

Append the two tests of §3.1/§3.2. New imports: `remote_read_page` from `crate::application::read_remote`, `InternalTarget`, `http_rig_build`/`HttpRigOptions` from `fixture_http`, `RigRuntime` from `fixture_runtime`, `std::time::Duration`. The file's `command()` helper is reused (mutate `start`, `visibility`, `refresh` on the clone).

### 4.4 `src/application/read_remote_tests.rs` — NEW, ≈ 70 lines (§3.3)

### 4.5 Ledgers — see §6.

---

## 5. Mutation-kill analysis (cargo-mutants 27.1.0, `--in-diff`, `--timeout 90`)

Owners selected by the diff (`scripts/quality/verification_plan.py`, CRITICAL prefix `src/application/read_` and `src/http`): `read_remote`, `read_remote_tests` (new row, §6), `http_read`. `src/dst/tests/*` is not a mutation source. Filters are OR'ed libtest substrings passed as `cargo test <f0> -- <f1> <f2>...`; a DST test is selected only if its path contains a filter, so the two owner rows are extended with `dst_tests::read_application::` (§6) — `application::read` does NOT match `dst_tests::read_application::`, and `http::read::` matches nothing at all (the module is `http::read_adapter`), so without the extension the relay tests would not run in the mutation leg.

| Mutant (file:item) | Viable? | Killed by |
|---|---|---|
| `read_remote.rs::WireReadRefusal::of` → `None` | yes | §3.3 `the_page_route_refusal_names_the_owner_verdict` (`expect("… is typed")`), §3.2 (`reply["refused"]` Null) |
| `of` → `Some(Default::default())` | no (no `Default`) | — |
| `From<WireReadRefusal> for ReadFailure` → `Default::default()` | no | — |
| `peer_refusal` → `Default::default()` | no | — |
| `remote_read_page` → `Ok/Err(Default)` | no | — |
| `remote_read_page`: `!status.is_success()` → `status.is_success()` | yes | §3.1 (page decode of the refusal body fails → not `CursorBeyondTail`); `dst_tests::read_application::r06_cross_owner_replay_and_scan_share_typed_pages_and_bill_once` (success body treated as refusal → `InvalidResponse`, 200 assertion fails) |
| `remote_read_page` closure: `command.visibility == Deliver::Applied` → `!=` | yes | §3.1: applied header not sent → owner reads durable → beyond-tail on the last span is an empty page, `Ok(..)`, `.err().expect(..)` panics |
| `read_remote.rs` redirect block (`== 409`, `hop == 0`, `next != owner`) | not in diff (bytes unchanged) | — (keep it unchanged; see §4.1(b)) |
| `http/read.rs::respond_read` → `Default::default()` (empty 200) | yes | `dst_tests::reads_raw::raw_route_is_the_default_key_view_across_splits` (parses records), §3.2 (404 expected) |
| `respond_read` guard `Ok(out) if page` → `true` | yes | `reads_raw::raw_route_is_the_default_key_view_across_splits` (body becomes the DTO object, `Vec<Value>` parse fails), §3.2 public `is_array()` |
| `Ok(out) if page` → `false` | yes | §3.1 and `r06_cross_owner_…` (internal page rendered raw → `WireReadPage` decode fails → `InvalidResponse`) |
| guard `Err(error) if page` → `true` | yes | §3.2 public 404: body becomes `{"refused":"missing"}`, `error.code` is Null |
| `Err(error) if page` → `false` | yes | §3.2 first block (envelope instead of typed body), §3.1 (`InvalidResponse` ≠ `CursorBeyondTail`) |
| `page = params.internal && …` → `\|\|` | yes | §3.2 public route with `streams-internal-read-page: 1` → DTO object instead of array |
| `… == Some("1")` → `!=` | yes | §3.1 / `r06_cross_owner_…` (internal + header → raw body → decode fails) |
| `typed_refusal` → `Default::default()` (empty 200) | yes | §3.2 (`status == 409`), §3.1 |
| `read_inner` → `Default::default()` | yes | every raw read test |

Every DST wait is bounded (`preq`/`hreq` bounded responses, `remote_read_page` 20 s reqwest timeout, `rig.tasks.shutdown(5s)`), so no mutant can time out into a false pass. Test files themselves generate zero mutants (`#[test]` items are skipped; the `read_remote_tests` row exists so the unregistered-owner check passes, like `read_wire_tests`).

---

## 6. Ledgers (same commit)

1. `scripts/quality/mutation_owners.py`
   - `owner('read_remote', 'src/application/read_remote.rs', 'application::read dst_tests::read_application::')`
   - `owner('read_remote_tests', 'src/application/read_remote_tests.rs', 'application::read')` (new row, next to `read_wire_tests`)
   - `owner('http_read', 'src/http/read.rs', 'http::read:: dst_tests::reads_raw:: dst_tests::reads_history:: dst_tests::read_application::')`
   - `python3 -m unittest scripts/quality/test_mutation_owners.py` still passes (rows are literal; `declared_source_map` must equal the runtime map).
2. `docs/quality/owners.json` — one `by-path-module` row: `{"category":"by-path-module","count":1,"owner":"crate::tests","path":"src/application/read_remote.rs","reason":"Peer-verdict decoder unit tests live beside the adapter they pin; compiled and run under cfg(test) with the application::read filter.","syntax":"path = \"read_remote_tests.rs\""}`. No `effect`/`global`/`macro-dsl` rows: no spawn, no static, no `json!`.
3. `docs/refactor/test-inventory.json` — `python3 scripts/test-inventory.py --write` (two new DST tests; `--check` must then pass).
4. `docs/refactor/WIRE-MATRIX.md`
   - Fleet-internal row `GET /v1/internal/segment-read/{*name}` (line 220): add the page-header vocabulary — 200 `WireReadPage`; a decided verdict keeps its public status with body `{"refused":"cursor_beyond_tail"|"changed_incarnation"|"missing"|"gone"}`; untyped answers keep the envelope; coordinator fallback per §2.2 incl. bare 409 → 503 `temporarily_unavailable`.
   - §1.3 Errors (line 53): the `409 cursor_beyond_tail … reachable via /v1/internal/segment-read` parenthetical gains "(typed body on the page route)".
5. `docs/refactor/architecture-policy.json` — no change (no new file references `crate::http`; `http/read.rs` already listed).
6. `src/dst/tests/README.md` — no change (no new DST module; tests land in the existing `read_application`).
7. `docs/quality/source-allowances.json` — must NOT be edited (legacy ceiling; `remote_read_page`'s removed exception simply disappears from the current inventory, which is allowed).

---

## 7. Controls

Run from `/Users/sorenschmidt/code/streams`, in this order.

1. Red first (before touching production code; the crate must still build, so write §3.1/§3.2 first and §3.3 + the `mod tests;` line together with §4.1):
   `cargo test --release --lib dst::dst_tests::read_application::relayed_applied_read_beyond_the_tail_keeps_the_owner_verdict -- --exact` → the panic text of §3.1.
   `cargo test --release --lib dst::dst_tests::read_application::the_page_route_types_its_refusal_and_the_public_route_keeps_its_envelope -- --exact` → the `left: Null` output of §3.2.
2. After §4: both above green; `cargo test --lib application::read_remote::tests` → 2 passed; `cargo test --release --lib dst::dst_tests::read_application::` and `dst::dst_tests::read_page_limits::` and `dst::dst_tests::reads_raw::` green (relay + raw regressions).
3. `wc -l src/http.rs src/product.rs src/application/read_remote.rs src/http/read.rs src/dst/tests/read_application.rs src/application/read_remote_tests.rs` → `http.rs` still 3371, `product.rs` still 4277, the rest ≤ 1000.
4. `cargo fmt --all -- --check`; `cargo clippy --locked --workspace --all-targets -- -D warnings` (expect zero; if `unfulfilled_lint_expectations` names `read_inner`'s `too_many_lines`, delete that attribute; it must NOT name `remote_read_page` — its `expect_used` attribute is already gone with the `.expect`).
5. `cargo doc --locked --workspace --no-deps --document-private-items` with `RUSTDOCFLAGS='-D warnings'` (new doc comments).
6. `python3 scripts/architecture-gate.py --self-test && python3 scripts/architecture-gate.py --check` (hard-owner token rules for `read_remote.rs` and the new tests file; budgets).
7. `python3 scripts/test-inventory.py --write && python3 scripts/test-inventory.py --check`.
8. `scripts/quality.sh` (source ratchet: `exception_growth` must report nothing for `read_inner`'s remaining identities; `unregistered source occurrence` must be empty — proves the `by-path-module` row; mt-lint; scenario/inventory/evidence gates).
9. `cargo build --locked -p streams-quality-syntax && python3 scripts/quality/verification_plan.py --out target/quality-plan` → `plan.json` shows `mutants: true` and `selected_mutation_owners` ⊇ `[http_read, read_remote, read_remote_tests]`, `unregistered_mutation_source_files: []`.
10. `scripts/quality/mutations.sh` (pinned cargo-mutants 27.1.0) → outcome summary with `missed: 0`, `timeout: 0`; every viable mutant of §5 caught.
11. Full suite as CI runs it: `cargo test --release -- --skip post_split_throughput_scales 2>&1 | tee /tmp/suite.log && python3 scripts/quality/tests_ran.py /tmp/suite.log --inventory docs/refactor/test-inventory.json --skipped 1`.
12. After push: `gh run list --branch slate --limit 3` then `gh run view <id>` for `ci` and `rust-quality` — never claim green without it.

Commit message subject (one behaviour, one sentence): "A relayed read carries the owner's verdict on the wire; the coordinator no longer rebuilds one from a 409".

---

## 8. Out of scope / follow-ups

- `scan_page_once` (`read_remote.rs:201-221`) still sniffs `error.code` (`not_ring_owner`/`target_mismatch`/`invalid_target`) and maps a `stale_target` 409 to `InvalidResponse` — give `/v1/internal/segment-scan` the same typed-refusal body and the same `peer_refusal`-style decode; it also has the identical `try_clone().expect` pattern that §4.1(b) removes here.
- `verify_internal_target` (`src/product.rs:3241-3314`) returns an opaque `Response`; a typed `InternalTargetError` rendered by each internal route would let the page route type `stale_target` too, removing the last bare 409 on this path. Needs a `product.rs` line-neutral edit or a move of the five internal handlers out of `product.rs`/`http.rs` first.
- `internal_segment_read` (`src/http.rs:3281-3357`, 77 lines) is a candidate verbatim move into `http/read.rs`: it buys `http.rs` headroom (currently 1 line) and is the natural place for the pre-read 404/410 to become typed as well.
- Product-surface end-to-end proof (a signed applied cursor beyond the tail on a two-owner rig renders `cursor_beyond_tail` at the edge) — needs the crash-restart cursor fixture of `reads_applied.rs:429-556` adapted to `http_rig_owner`/`http_rig_owner_at`; the application verdict is the contract this change pins.
- Rolling-deploy note for the release ledger: during a mixed fleet, new-coordinator/old-owner relayed verdicts appear as 503 `temporarily_unavailable` (retryable) until the owner is upgraded; old-coordinator/new-owner is unchanged.
- The `http_read` filter `http::read::` selects nothing (module is `http::read_adapter`); it is left as-is here but should be corrected or dropped in a tooling change.

---

## Skeptic corrections (C1..C14)

Verified against the working tree at HEAD `df9ff212` (one commit past the plan's `8dabca7f`: "Move the product consumer pull handler into product/consumer_pull.rs, verbatim"). Every quoted site in §1 was re-read; all `read_remote.rs`, `http/read.rs`, `http.rs` and `read_request.rs` line numbers in the plan are still exact. Verdict at the end.

### C1 (compile error) — `respond_read(state: &AppState)` cannot call `state.read_service()`

`src/http.rs:328`: `pub(crate) fn read_service(self: &Arc<Self>) -> Arc<ReadService>` takes `&Arc<AppState>`, not `&AppState`. The §4.2(b) signature `state: &AppState` fails to compile at `state.read_service()`. Fix: `state: &Arc<AppState>` (call site `respond_read(&state, …)` is unchanged); `render_raw_read(state, …)` still type-checks because `&Arc<AppState>` deref-coerces to `&AppState` (`src/http/read.rs:355-356`). Still five params, no bool.

### C2 (missed mutant → CI mutation leg FAILS) — the guard `Err(error) if page → true` is NOT killed by §3.2's public 404

The §5 row claims "§3.2 public 404: body becomes `{"refused":"missing"}`". False: `GET /v1/stream/relay-absent` is answered by `read_inner` at `src/http/read.rs:152-155` (`registry.get` → `Ok(None)` → `read_failure_response(Missing)`) BEFORE `respond_read` ever runs, so the mutant leaves that body untouched. No selected test reaches a `respond_read` `Err` on a non-page request with a verdict in `WireReadRefusal::of`'s domain: on the raw route `deliver` is serde-skipped (`src/http.rs:1759-1763`) so `CursorBeyondTail` is unreachable; `Missing`/`Gone` are decided by `authorize_read` at `read.rs:215` first; `ChangedIncarnation` needs a stale descriptor that HTTP never has (`read.rs:152` fetches fresh). Neither `reads_raw` nor `reads_history` asserts a 409/404/410 `error.code` after `execute_read` (grep `target_mismatch|cursor_beyond_tail` in `src/dst/tests` hits only `reads_applied.rs`, `livefeed_ownership.rs`, both outside the `http_read` filter).

Remedy (green today, required for the kill): add a fourth block to §3.2 — the SAME internal request as block 1 but WITHOUT `streams-internal-read-page` (keep `authorization`, `stream-encryption-key`, the four target headers, `streams-internal-deliver: applied`, `streams-internal-max-bytes`):

```rust
let (status, _, body) = preq(addr, "GET",
    &format!("/v1/internal/segment-read/relay-tail?offset={offset}"), &plain_refs, b"").await;
assert_eq!(status, 409);
let envelope: serde_json::Value = serde_json::from_slice(&body).unwrap();
assert_eq!(envelope["error"]["code"], "cursor_beyond_tail",
    "without the page header the relay route keeps the raw vocabulary: {envelope}");
```

This kills `Err(error) if page → true` (body becomes `{"refused":…}`, `error.code` is Null), and independently kills `params.internal && … → ||` and `== Some("1") → !=` (both make `page` true when the header is absent on an internal request). It also pins the contract that `billing.rs:2258-2279` (`system_read` relay, no page header) keeps receiving the raw vocabulary. Rename `hdr_refs` → build both vectors; the plan's §5 table rows for `Err … → true`, `&& → ||`, `== → !=` must cite this block.

### C3 (table accuracy, no behaviour change) — `Ok(out) if page → false` is killed only by `r06_cross_owner_…`

§3.1 never reaches the `Ok` arm on the page route (the relayed call is the refusal). The single killer is `dst_tests::read_application::r06_cross_owner_replay_and_scan_share_typed_pages_and_bill_once` (page rendered raw → `WireReadPage` decode fails → `Remote(InvalidResponse)` → product 503 at `src/product.rs:2965-2971` → `assert_eq!(status, 200)` at `read_application.rs:213` fails). That is a single, two-rig, timing-heavy killer. Add `dst_tests::read_page_limits::` to BOTH the `http_read` and `read_remote` filters: `r06a_compressed_local_and_peer_pages_have_identical_complete_sequences` (`read_page_limits.rs:85-87`, single loopback rig) unwraps `remote_read_page` on the success path and kills the same mutant deterministically. Cost: one more DST module in the mutation run (173-line file, already bounded).

### C4 (facts) — line counts and anchors

- `read_inner`: clippy's `too_many_lines` counts non-blank, non-comment BODY lines (`read.rs:152-252` = 101 today, which is why the expect is currently fulfilled). After replacing 237-252 with one call the body is 86 lines, so deleting the `#[expect(clippy::too_many_lines, …)]` at 131-134 is CERTAIN, not "verify with clippy". §7 step 4's "if" can be dropped.
- `product.rs` is **4,207** lines at `df9ff212` (plan: 4,277); anchors moved: `ChangedIncarnation` arm 2930-2936, `CursorBeyondTail` 2937-2943, `Resolve(_) | Remote(_)` 2965-2971, `verify_internal_target` 3242-3315, `stale_target` 3252-3260. Untouched by the plan, so cosmetic.
- `http.rs` ceiling is **3,371, not 3,372**: `source_rules.violations` (`scripts/quality/source_rules.py:226-230`) takes `limit = min(max(1000, legacy 3382 + adoption 2), max(1000, merge-base 3371)) = 3371` and fails on `now > limit`. "3,371 of 3,372 max" in the brief is off by one; the plan does not touch `http.rs`, so nothing changes, but §7 step 3 should say "still 3371 (= its ceiling)".
- `legacy-source.json:51` records `read_remote.rs` at 404 and `read_application.rs` at 310; both budgets are 1,000 as stated.

### C5 (in-diff semantics) — the redirect block claim holds, with the exact rule stated

cargo-mutants 27.1.0 `in_diff.rs:212-259` (`affected_lines`) selects a mutant iff any line of its span is an INSERTED line or the first line after a deleted run; context lines are ignored. So §4.1(b)'s ordering — insert the three `let bytes = …` lines AFTER the redirect block's closing `}` (396) and BEFORE `if !status.is_success() {` (397) — keeps 381-396 unselected. Two consequences the plan does not state: (i) `!status.is_success()` at 397 is a context line, so the §5 row for it is moot (not selected, nothing to kill); (ii) the whole-function `remote_read_page` mutants (`Ok(Default)`/`Err(Default)`) are selected because the span covers changed lines, and they are non-viable only because neither `ReadOutcome` nor `ReadFailure` is `Default` — true today (`read_request.rs:47`, `:64`); if anyone ever derives `Default` on either, this becomes a live mutant.

### C6 (contract decision for Søren) — a GENUINE incarnation change now renders 503 instead of 409 on the relayed path

`verify_internal_target` answers `stale("epoch")` (`product.rs:3284-3286`) precisely when the stream was deleted and recreated — the one case where `target_mismatch` (non-retryable) is the RIGHT product verdict. Under §2.2 that bare 409 becomes `Remote(InvalidResponse)` → product 503 `temporarily_unavailable` with `retry: true` (`product.rs:2965-2971`), and the coordinator's `execute_read` returns the `Remote` error without a refresh (`read_request.rs:239-246`), so an SDK will retry a dead cursor until the coordinator's registry cache turns over. The plan calls this "intended" and lists the typed-`stale_target` follow-up in §8; the reviewer's Change ("keeping status handling only for bodyless answers") does accept it. It is still a product-edge behaviour change on a path the reviewer did not ask to change, and it must be (a) called out in the commit body, (b) written into WIRE-MATRIX §Fleet-internal line 218 (receiver still answers 409 `stale_target`; the RELAYING coordinator now renders it 503) and (c) explicitly approved. The line-neutral alternative (type `stale_target` in the same commit) is blocked by the `product.rs` ceiling; the plan's choice is defensible but not free.

### C7 (§3.3 pins a case no owner produces) — "verdict wins over status" (`peer_refusal(500, typed)` → `CursorBeyondTail`)

`typed_refusal` always keeps the status `read_failure_response` chose, so a 500 with a typed body never comes from our owner. Pinning it is harmless (the decoder is simpler if the body is decoded first) but it is a contract statement about answers nobody sends; either keep it and say so in the test name, or drop the case. Not blocking.

### C8 (ledgers — confirmed complete, two precisions)

- `owners.json` `by-path-module` row: format matches the precedent, which lives in `docs/quality/source-allowances.json:26-30` (category `by-path-module`, owner `crate::tests`, syntax `path = "read_wire_tests.rs"`), and `source_gate.py:46-49` requires `reason` on owners.json rows — plan §6.2 has it. Correct that the row goes in `owners.json`, never in `source-allowances.json` (adding there trips `legacy source allowance grew`).
- Neither `remote_read_page`'s nor `read_inner`'s `#[expect]` identity is in `source-allowances.json`/`owners.json`/`legacy-source.json` (grep empty), so deleting them triggers no `obsolete source allowances; --prune` message (`source_gate.py:59-62`) — §6.7 holds.
- `mutation_owners.py` rows: `read_remote_tests` is REQUIRED (path starts with the critical prefix `src/application/read_`, `verification_plan.py:22-31, 87-90`; `validate_sources` refuses unregistered files, `mutation_owners.py:279-282`). `cargo mutants --list --in-diff` on a test-only file returns `NoMutants` with exit `Success` (`in_diff.rs:51`), and `mutation_driver.py:145-147` prints "no executable mutants" and continues — the `read_wire_tests` precedent works for the same reason.
- `test-inventory.py` scans only `src/dst/**/*.rs` (`scripts/test-inventory.py:79`), so the two DST tests are the only inventory rows; the unit tests are not inventoried. `scenario-map-report --check` does not require new tests to be mapped (`scripts/scenario-map-report.py:150-215` fails only on catalogue/ID/file-existence drift). `architecture-policy.json` has no peer-adapter list (keys: `sse_core_files`, `transport_and_composition_files`, `budget_exceptions`, `transport_rationales`, `adapter_http_exports`), and `src/http/read.rs` is in `transport_and_composition_files` (line 14) — no change needed, as stated.
- `read_remote_tests.rs` starting with `use super::*;` is NOT `is_test_only` for `architecture-gate` (`scripts/architecture-report.py:237-256`: only `src/dst/` or a leading `#![cfg(test)]`), so it gets a 1,000-line file budget, 200-line function budgets and the hard-owner token checks (`architecture-gate.py:100-108`: `\bAppState\b|\baxum\s*::` and `\b(?:HeaderMap|Response)\b`, plus `reverse_edges` for `crate::http|product`). The plan's identifier ban list is exactly right; `InvalidResponse` passes the `\b` boundary.

### C9 (closure rewrite — buildable, one precedent worth citing)

`PeerClient::send(&self, mk: impl Fn(Option<&str>) -> reqwest::RequestBuilder)` (`src/peer.rs:124-127`) re-invokes `mk` on a 401 with a workload source. Building the request inside the closure with shared borrows of `base`, `query`, `key`, `target`, `command` is `Fn` and is EXACTLY what `billing.rs:2258-2274` already does for the same route (no `try_clone`, no `expect`). Cite it in the commit body as the precedent; it also answers the fallback question — the closure form is the repo's own pattern, not a novelty.

### C10 (§3.1 trace — confirmed red, with the two facts the plan leaves implicit)

- `position_in` (`read_request.rs:606-617`) validates only the SEGMENT, never `after`, so `after: 100` passes and reaches line 291 (`Applied && start > end`) — the local control is `CursorBeyondTail`, not `InvalidCursor`.
- On the owner, `raw_start` takes the NON-segmented arm (`read.rs:121-127`, `Offset::parse`) for `encode_ep(0, Offset(Some(99)))`; epoch-0 `encode_ep` equals plain `Offset::encode` (`src/offsets.rs:25-31` vs `83-95`), so `scan_from() == 100` — this is the same path `read_page_limits.rs:85` already exercises.
- Under the `visibility == Applied → !=` mutant the owner reads durable: line 294 (`start >= end && !last`) is skipped on the single live span, `ResolvedRead::execute` reads a bounded range from 100 → empty `Ok` page → `.err().expect(…)` panics. Kill confirmed.
- Expected red text and `:5` column are correct (`assert!` at four-space indent).

### C11 (§3.2 trace — confirmed red; red message format is correct)

`err_resp` (`http.rs:1652-1659`) serialises `json!` through `serde_json::Value` (BTreeMap → keys sorted: `code` before `message`), so the quoted body is byte-exact. `serde_json::Value: PartialEq<&str>` makes `assert_eq!(reply["refused"], "cursor_beyond_tail", …)` compile; `left: Null` / `right: "cursor_beyond_tail"` is the Rust ≥1.73 format. `preq` (`fixture_requests.rs:212-270`) injects nothing (unlike `hreq`, which injects the rig key), so the header list is exactly what the owner sees.

### C12 (body swap in `typed_refusal` — confirmed safe)

`err_resp` returns `(status, [(CONTENT_TYPE, "application/json")], String).into_response()`: axum sets only `content-type`; `content-length` comes from the body's exact size hint at serve time, so swapping in `Json(..).into_response().into_body()` needs no header surgery (plan §4.2 is right). `Resolve(Opening)`'s `retry-after` and `NotOwner`'s `streams-replay-to` (`read.rs:73-75, 88-91`) are preserved because `of` returns `None` for both and the envelope is kept.

### C13 (working tree) — do not sweep the in-flight file

`git status` shows `M src/dst/tests/quota_read_volume.rs` (the running gate's work, not this plan's). §7's commit must `git add` by explicit path: the two source files, the new tests file, `read_application.rs`, `mutation_owners.py`, `owners.json`, `test-inventory.json`, `WIRE-MATRIX.md`.

### C14 (§3.3 red is a build break, not a failing test)

A missing type is `error[E0433]`/`E0425` for the WHOLE `--lib` test target, so §3.1/§3.2 cannot be re-run red once the `mod tests;` line exists. The plan's ordering (run §3.1/§3.2 red first, then add §3.3 together with §4.1) is the only workable sequence; state in the commit body that the unit tests' red evidence is the compile error, so nobody looks for a panic transcript.

### Verdict

**ready-with-corrections.** C1 (compile error) and C2 (a selected mutant with no killer → mutation leg fails) must be applied before the commit; C3 and C4 are cheap accuracy fixes; C6 is a product-edge behaviour change that needs Søren's explicit yes and a WIRE-MATRIX line. No control in the plan is unbuildable: the loopback rig, `preq` raw headers, the static `dst-internal-token` bearer (`fixture_http.rs:441`), the offset encoding and the closure rewrite are all proven by existing tests or existing code. No ledger is missed; C8 only sharpens where each row goes and why the test-only owner row survives an empty mutant list.
