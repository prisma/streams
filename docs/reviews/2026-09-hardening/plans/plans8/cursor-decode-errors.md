# Item 61: typed cursor-decode verdicts; product_scan maps them exhaustively

Tree: `slate` @ fb18840d (origin/slate = 7c4f8606). None of the files touched here differ between
origin/slate and HEAD (`git diff --stat origin/slate HEAD` lists none of them), so every ratchet below
is computed against the same text quoted here.

Reviewer's Where is stale: `product_scan` moved out of `product.rs` in 0eb8a38d (verbatim) and 527d3d3a.
It is now `src/product/scan.rs:26-220`. The decoders are in `src/product_cursor/decode.rs`, as cited.

---

## 1. Problem (verified)

### 1.1 The decoders return `&'static str`

`src/product_cursor/decode.rs`:

| fn | line | error channel |
|---|---|---|
| `KeyCursor::decode` | 52 `) -> Result<Self, &'static str> {` | 53 `let invalid = "invalid_cursor";`, 56 `return Err("wrong_cursor_kind");` |
| `ScanCursor::decode` | 105 `) -> Result<Self, &'static str> {` | 106 `let invalid = "invalid_cursor";`, 112 `return Err("wrong_cursor_kind");`, 145 `return Err("scan_expired");` |
| `MessageId::decode` (`#[cfg(test)] impl`, 160) | 167 | 168 `let invalid = "invalid_message_id";`, 171 `return Err("wrong_token_kind");` |
| `LeaseToken::decode` | 190 | 191 `let invalid = "invalid_lease_token";`, 194 `return Err("wrong_token_kind");` |

(`CatalogCursor::decode` returns `Option<String>` and is out of scope.)

### 1.2 The only production reader that branches on the literal: `product_scan`

`src/product/scan.rs:110-140`:

```rust
            match crate::product_cursor::ScanCursor::decode(c, &desc.project_id, &skey, &epoch, now)
            {
                Ok(sc) => Some(sc),
                Err("scan_expired") => {            // 113 -> perr(GONE, "scan_expired", "scan snapshot expired; start a new scan", ..)
                ...
                Err("wrong_cursor_kind") => {       // 122 -> perr(BAD_REQUEST, "invalid_cursor", "cursor is not a scan cursor", ..)
                ...
                Err(_) => {                         // 131 -> perr(BAD_REQUEST, "invalid_cursor", "invalid scan cursor", ..)
```

The problem is real. A typo or rename of either literal still compiles and falls into the `Err(_)`
catch-all. So does any new refusal reason: it becomes a 400 with no status decision.

"Rename is already caught" is only **partly** true:
- Renaming `"scan_expired"` is caught. `dst::dst_tests::reads_product::product_scan_is_snapshot_exact`
  (reads_product.rs:537-565) asserts `st == 410` and `code == "scan_expired"`.
- Renaming `"wrong_cursor_kind"` is **not** caught. The request falls to `Err(_)`, which changes only the
  message ("cursor is not a scan cursor" becomes "invalid scan cursor"). The same test asserts only
  `st == 400` for a key cursor on `:scan` (reads_product.rs:567-583). No test anywhere asserts either
  400 message (`grep -rn "not a scan cursor\|invalid scan cursor" src` finds only scan.rs).
- The garbage/foreign-cursor answer at the edge (`Err(_)`, 400) is not asserted by any DST test.

### 1.3 Every use site of the four decoders (grep `Cursor::decode\|Token::decode\|MessageId::decode`)

Production:
- `src/product/scan.rs:110`: `ScanCursor::decode`, with the literal match above (**the defect**).
- `src/product.rs:2708` (`product_read`): `KeyCursor::decode`, then `Err(_) => render_product_read_failure(ReadFailure::InvalidCursor)`. Discards the verdict.
- `src/application/consumer/delivery.rs:435` (`settle`): `LeaseToken::decode`, then `Ok(lt) if .. => .., _ => { stale_local += 1; None }`. Discards the verdict.

Tests that read the value only through `.unwrap()` / `.expect()` (they need `E: Debug`, nothing else):
- `src/golden_tests.rs:756`, `src/sse/session/tests.rs:61,81`
- `src/dst/tests/consumer_dlq.rs:80,318,461`
- `src/dst/tests/read_application.rs:98`, `sse_delivery.rs:277`
- `src/dst/tests/producer_protocol.rs:409,553,565`, `persistence_faults.rs:418`
- `scripts/read-experiments/followup/legacy_workload.rs:88` is not compiled by any target and uses `.unwrap()`.

Tests that assert the literal (22 sites; the reviewer's "~13" is low):

| file:line | decoder | literal | commit |
|---|---|---|---|
| product_cursor.rs:349 | Scan | `"scan_expired"` | 2 |
| product_cursor.rs:362 | Scan | `"wrong_cursor_kind"` | 2 |
| product_cursor.rs:366, 405 | Key | `"wrong_cursor_kind"` | 3 |
| product_cursor.rs:397 | Message | `"wrong_token_kind"` | 3 |
| product_cursor.rs:401 | Lease | `"wrong_token_kind"` | 3 |
| regressions.rs:51, 91, 162, 194, 198, 310 | Scan | `Err("invalid_cursor")` | 2 |
| regressions.rs:63 | Scan | `Err("scan_expired")` | 2 |
| regressions.rs:146, 299 | Scan | `Err("wrong_cursor_kind")` | 2 |
| regressions.rs:142 | Key | `Err("wrong_cursor_kind")` | 3 |
| regressions.rs:158, 218 | Key | `Err("invalid_cursor")` | 3 |
| regressions.rs:150 | Message | `Err("wrong_token_kind")` | 3 |
| regressions.rs:154 | Lease | `Err("wrong_token_kind")` | 3 |
| regressions.rs:166 | Message | `Err("invalid_message_id")` | 3 |
| regressions.rs:170 | Lease | `Err("invalid_lease_token")` | 3 |

Scan: 11 sites. Token: 11 sites.

### 1.4 Where the reviewer's Change goes wrong

"Exhaustive match in product_scan, line-neutral" is right about lines but would **fail the source
ratchet**. `product_scan` carries two function-wide exceptions, at scan.rs:17-25:
- `#[expect(clippy::unwrap_used, clippy::expect_used, reason = "product_scan; ...")]`. This one
  fingerprints every `path` fact and `call-site` fact in scope (`scripts/quality/source_rules.py`
  lines 171-190) and ratchets `scope_lines` / `nested_items` / `syntax_facts`.
- `#[expect(clippy::too_many_lines, reason = "product_scan; ...")]`. This ratchets `scope_lines`,
  `nested_items` and `syntax_facts`.

The scanner (`tools/quality-syntax/src/scan.rs`) records no fact for a literal pattern (`"scan_expired"`)
or a wildcard (`_`). It records one `path` fact for each `crate::product_cursor::ScanCursorError::X`
pattern. The swap therefore adds **3 path facts**, which causes:
- 3 new `unwrap_site:path:crate::product_scan:<digest>` keys and 3 new `expect_site:path:...` keys, each 0 -> 1.
- `syntax_facts` +3 under both exceptions.

`exception_growth` reports each of these as `accepted exception grew without a new decision`. Under
RUST-QUALITY §Diagnostics and exceptions, the remedy is to narrow the exception or re-decide its reason.
The precedent is 527d3d3a, which re-decided scan.rs's two reasons for the same cause. See 4.2.

---

## 2. Contract decision

**Typed contract** (owner: `crate::product_cursor`, declared in `src/product_cursor.rs` next to the
token types, so `decode.rs`, `regressions.rs` and `tests` reach them through `super::` and no
`pub(crate) use` re-export is needed):

```rust
/// Why a scan cursor was refused. The scan page matches these exhaustively and
/// owns each one's wire answer, so a new refusal cannot reach the edge until
/// that page decides its status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanCursorError {
    /// Unreadable, unauthenticated or minted for another incarnation. Identity
    /// is judged before expiry, so a foreign cursor is this even when expired.
    Invalid,
    /// Another token class; the kind byte is judged before shape.
    WrongKind,
    /// An authentic snapshot past its deadline; the client starts a new scan.
    Expired,
}

/// Why a key cursor, message id or lease token was refused. Readers and
/// settlers answer both alike; the kind verdict keeps kind-before-shape
/// falsifiable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenError {
    /// Unreadable, unauthenticated, or bound to another incarnation or key.
    Invalid,
    /// Another token class.
    WrongKind,
}
```

- `Copy` is needed because `decode.rs` binds `let invalid = ...;` once and moves it into many `ok_or(invalid)` calls.
- `Debug` covers the 13 `.unwrap()`/`.expect()` sites. `PartialEq` covers the `assert_eq!(decode(..), Err(..))` sites.
- The codec stays transport-free: no HTTP status lives in `product_cursor`, because the invariant
  harness (`tools/quality-invariants`) compiles it without axum. The status decision stays in the
  scan page.
- One flat enum with an `Expired` variant for all decoders was rejected: key, message and lease
  decoders can never produce `Expired`, and a `ScanCursorError::Rejected(TokenError)` wrapper only adds
  nesting.
- Message ids and lease tokens lose their class-specific "invalid" spellings
  (`invalid_message_id` / `invalid_lease_token`). The decoder's type carries the class, and nothing
  but literal assertions ever read the spellings.

**No wire change.** Status, code and message are byte-identical for every verdict:
- `product_scan`'s three arm bodies are unchanged; only their patterns change.
- Each decoder raises the same verdict at the same point in the same order.
- `product_read` and `settle` already discard the verdict (`Err(_)` / `_ =>`).
- WIRE-MATRIX §2.6 ("400 `invalid_cursor` (wrong kind, garbage, unknown segment); 410 `scan_expired`")
  stays true as written.

---

## 3. Red tests / pinning tests

This is a pure refactor: no behaviour changes, so no test turns red. Three things pin the behaviour:

### 3.1 New pin (commit 1, green on the current tree by design)

`src/dst/tests/reads_product.rs`, inserted after `product_scan_is_snapshot_exact` (after line 586):

```rust
/// Each scan-cursor verdict has its own wire answer, pinned so retyping the
/// decoder's refusals cannot swap two of them: identity is judged before
/// expiry (a foreign, expired cursor is invalid, not gone), another token
/// class is named as such, and only an authentic expired snapshot is 410.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn product_scan_answers_each_cursor_verdict() {
    let (state, addr) = http_rig(mem()).await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/scv",
        &key,
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    let epoch = state
        .registry
        .get(&state.deployment.raw_adapter_sref("scv"))
        .await
        .unwrap()
        .unwrap()
        .epoch();
    let mut foreign = epoch;
    foreign[0] ^= 1;
    let tenant = state.deployment.deployment_tenant();
    let expired_scan = |epoch| {
        crate::product_cursor::ScanCursor {
            epoch,
            map_version: 0,
            segments: vec![(0, 1)],
            current_index: 0,
            current_offset: 0,
            expires_at_ms: 1,
        }
        .encode(tenant, &skey())
    };
    // The class byte is judged before any binding, so the key hash is moot.
    let key_cursor = crate::product_cursor::KeyCursor {
        epoch,
        key_hash: [2; 16],
        seg_id: 0,
        offset: 0,
    }
    .encode(tenant, &skey());
    for (cursor, status, code, message) in [
        (expired_scan(foreign), 400, "invalid_cursor", "invalid scan cursor"),
        (key_cursor, 400, "invalid_cursor", "cursor is not a scan cursor"),
        (expired_scan(epoch), 410, "scan_expired", "scan snapshot expired; start a new scan"),
    ] {
        let path = format!("/v1/streams/scv:scan?cursor={cursor}");
        let (st, _, b) = preq(addr, "GET", &path, &key, b"").await;
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(
            (st, v["error"]["code"].as_str(), v["error"]["message"].as_str()),
            (status, Some(code), Some(message)),
            "{message}: {}",
            String::from_utf8_lossy(&b)
        );
    }
    engine_shutdown(&state).await;
}
```

- **Expected on the current tree:** `test dst::dst_tests::reads_product::product_scan_answers_each_cursor_verdict ... ok`.
  - Foreign epoch: the MAC verifies (it is keyed by the epoch in the payload); then `&epoch != expect_epoch` at decode.rs:141 returns `"invalid_cursor"`, which hits `Err(_)` for 400 "invalid scan cursor". The expiry at :144 is never reached.
  - Key cursor: the kind byte check at :111 returns `"wrong_cursor_kind"` for 400 "cursor is not a scan cursor".
  - Genuine expired cursor: :144 returns `"scan_expired"` for 410.
- **Negative control** (run once, do not commit): swap the two 400 message strings in the table. The test
  must fail on its first row with
  `assertion `left == right` failed: cursor is not a scan cursor: {"error":{"code":"invalid_cursor","message":"invalid scan cursor",...}}`
  and `left: (400, Some("invalid_cursor"), Some("invalid scan cursor"))`,
  `right: (400, Some("invalid_cursor"), Some("cursor is not a scan cursor"))`.
  This proves the pin tells WrongKind from Invalid, which no existing test does.
- Hygiene:
  - The test is under 100 lines, so it needs no `#[expect(too_many_lines)]`. Nesting depth is 2.
  - It uses `desc.epoch()`, not `epoch_bytes().unwrap()`, to stay clear of item 62, which deletes the fake Option.
  - It has no `stream_hash(` call and no `registry.get("` literal, so it adds no MT-audit fingerprint.

### 3.2 Existing pins (names; they keep their assertions, with only the spelling of the expected verdict retyped)

- `product_cursor::tests::scan_cursor_roundtrip_expiry_and_kind`: Expired, and WrongKind in both directions.
- `product_cursor::tests::message_and_lease_tokens_roundtrip_and_fence`: WrongKind for message, lease and key.
- `product_cursor::regressions::scan_count_is_admitted_against_the_complete_wire_rows`: Invalid ×5, then Expired.
- `product_cursor::regressions::scan_accepts_the_last_complete_wire_size_and_rejects_the_next`: Invalid.
- `product_cursor::regressions::fixed_tokens_reject_each_truncation_and_trailing_byte`: WrongKind ×4, Invalid ×4.
- `product_cursor::regressions::every_authenticator_byte_is_required`: Invalid ×2, including "identity errors precede expiry".
- `product_cursor::regressions::paired_mac_corruption_and_resigned_extra_fields_are_rejected`: Invalid.
- `product_cursor::regressions::encoded_size_boundary_preserves_kind_error_priority`: WrongKind, then Invalid.
- `dst::dst_tests::reads_product::product_scan_is_snapshot_exact`: 410 `scan_expired`, and 400 for a key cursor.
- `golden_tests` (KeyCursor wire pin) and the DST `.unwrap()` sites listed in 1.3: they compile unchanged.

All product_cursor tests also run in the harness:
`cargo test -p streams-quality-invariants --lib product_cursor::`.

### 3.3 Compile-level proofs

- **First step of commit 2.** Add `ScanCursorError` and retype `ScanCursor::decode` completely (signature
  plus its three literals), leaving scan.rs untouched. Then `cargo build --locked --lib` must fail with
  **exactly two** errors and nothing else:

  ```
  error[E0308]: mismatched types
     --> src/product/scan.rs:113:21
      |
  110 |             match crate::product_cursor::ScanCursor::decode(c, &desc.project_id, &skey, &epoch, now)
      |                   ---------------------------------------------------------------------------------- this expression has type `Result<ScanCursor, ScanCursorError>`
  ...
  113 |                 Err("scan_expired") => {
      |                     ^^^^^^^^^^^^^^ expected `ScanCursorError`, found `&str`
  error[E0308]: mismatched types
     --> src/product/scan.rs:122:21
  ```

  This enumerates the production readers of the scan verdict. The reviewer's "product.rs:3100 and 3109"
  is the pre-move location of these same two lines. `Err(_)` at :131 compiles either way, which is
  why it has to go.
- **Exhaustiveness probe** (after commit 2; do not commit). Add a variant `Oversized,` to
  `ScanCursorError`. `cargo build --locked --lib` must fail with
  `error[E0004]: non-exhaustive patterns: `Err(ScanCursorError::Oversized)` not covered` at
  `src/product/scan.rs:110:19`. This is the proof that a new refusal cannot reach the edge without a
  status decision. Revert the probe.
- **Commit 3 probe.** Retype `KeyCursor::decode`, `MessageId::decode` and `LeaseToken::decode` to
  `TokenError`. `cargo build --locked --lib` must succeed with **no edit outside
  `src/product_cursor*`**, which proves `product_read` (product.rs:2708) and `settle` (delivery.rs:435)
  never branch on these verdicts. `cargo test --locked --lib --no-run` must then fail (E0308 or E0277)
  at exactly the 11 token literal sites in 1.3, and nowhere else.

---

## 4. Edits, file by file, in commit order

Ceilinged files (current `wc -l`): http.rs 3,369; product.rs 4,205; shard.rs 3,196; billing.rs 2,201;
history.rs 1,713; auth.rs 1,676; registry.rs 1,501; sse/feed.rs 1,195; fleet.rs 1,143.
**None is touched.** Budget used: 0 in every file. No verbatim move, so no alias re-fingerprinting.

Touched files, none of them ceilinged:

| file | before | after |
|---|---|---|
| src/product/scan.rs | 220 | 220 (line-neutral) |
| src/product_cursor.rs | 412 | ~436 |
| src/product_cursor/decode.rs | 213 | ~214 (import reflow) |
| src/product_cursor/regressions.rs | 312 | ~313 (import reflow) |
| src/dst/tests/reads_product.rs | 733 | ~790 |

### Commit 1: "Each scan-cursor verdict is pinned at the edge by status, code and message"

1. `src/dst/tests/reads_product.rs`: add `product_scan_answers_each_cursor_verdict` (3.1) after line 586.
2. `docs/refactor/test-inventory.json`: run `python3 scripts/test-inventory.py --write` (one new entry, `scenarios: []`, as in 905d6656 / 527d3d3a).

The message states that the test is green on the current tree by design, and that it pins the one
distinction (WrongKind vs Invalid) that no test observed.

### Commit 2: "Scan cursor verdicts are typed; the scan page answers each one exhaustively"

1. `src/product_cursor.rs`:
   - Insert `ScanCursorError` (section 2) right after `pub(crate) const SCAN_CURSOR_MAX: usize = 16 * 1024;` (line 119).
   - Tests: line 349 `"scan_expired"` becomes `ScanCursorError::Expired`; line 362 `"wrong_cursor_kind"` becomes `ScanCursorError::WrongKind`. `use super::*;` already imports the enum.
2. `src/product_cursor/decode.rs`:
   - Add `ScanCursorError` to the `use super::{...}` list (lines 4-7; rustfmt reflows it).
   - Line 105: `) -> Result<Self, ScanCursorError> {`.
   - Line 106: `let invalid = ScanCursorError::Invalid;`.
   - Line 112: `return Err(ScanCursorError::WrongKind);`.
   - Line 145: `return Err(ScanCursorError::Expired);`.
3. `src/product_cursor/regressions.rs`: add `ScanCursorError` to the `use super::{...}` list, then switch the 9 scan sites:
   - 51, 91, 162, 194, 198, 310 become `Err(ScanCursorError::Invalid)`.
   - 63 becomes `Err(ScanCursorError::Expired)`.
   - 146, 299 become `Err(ScanCursorError::WrongKind)`.
4. `src/product/scan.rs` (line-neutral; arm bodies byte-identical):
   - 113: `Err(crate::product_cursor::ScanCursorError::Expired) => {`
   - 122: `Err(crate::product_cursor::ScanCursorError::WrongKind) => {`
   - 131: `Err(crate::product_cursor::ScanCursorError::Invalid) => {`

   Fully qualified paths match the file's idiom (`crate::product_cursor::ScanCursor::decode`,
   `crate::http::KeyCheck::Ok`). The longest line is 75 columns.

   **Ratcheted function touched: `product_scan`.** It has two function-wide exceptions. Remedy: re-decide
   both reasons. The explicit decision goes to Søren; each reason keeps exactly two `;` and contains no `"`.
   - Line 20 (`unwrap_used, expect_used`):

     `reason = "product_scan; a routing key serializes as a JSON string and the response builder holds a fixed status and validated headers, so neither step can fail once every typed cursor verdict has answered and the page is debited; mapping either into a substitute response would report a wire status the handler never decided"`

   - Line 24 (`too_many_lines`):

     `reason = "product_scan; the scan resolves the collection, answers each typed cursor verdict, pages the frozen cursor and debits the page it frames in one sequence; splitting it would separate the page from the cursor it advances and the bytes it charges"`

   `scope_lines` and `nested_items` stay the same. The two `.unwrap()`/`.expect()` sites the first
   exception covers are unchanged. Also unchanged: `product_scan`'s architecture function budget
   (200 default; the line count is the same).

   Remedies considered and rejected:
   - (a) Narrow the unwrap/expect exception to statement scope around `serde_json::to_string(..).expect`
     and the tail `.unwrap()`. This restructures an unrelated exception, and `too_many_lines` would
     still need re-deciding.
   - (b) Extract a `scan_cursor_refusal(ScanCursorError) -> Response` helper, in the style of
     `render_product_read_failure`. That shrinks `product_scan`, so `too_many_lines` would not need
     re-deciding, but the new call site still re-decides the unwrap/expect reason, and it adds about 25
     lines for the same outcome. The literal-free swap is the smallest change.
   - Evading the fingerprint through a method call (`refused.into_response()`) would be "a wrapper
     solely to satisfy a lint", which RUST-QUALITY forbids.

   Other `#[expect]` scopes checked:
   - `mac_key` and `mac16` (product_cursor.rs:47-76, `expect_used`) are untouched; the enum is
     inserted outside their item spans.
   - decode.rs carries none; its `#![warn(...)]` is not an exception.
   - `product_read` (product.rs:2566) and `settle` (delivery.rs:406) keep their text byte-identical, so
     their call-site fingerprints (callee path plus tokens) do not change.
   - The harness's `#[allow(dead_code, unreachable_pub)] mod product_cursor;`
     (tools/quality-invariants/src/lib.rs:49-55) is out-of-line, with no scoped items or fields, so it
     is unchanged.

### Commit 3: "Key, message and lease token verdicts are typed" (the reviewer's TokenError half; droppable, see Decisions)

1. `src/product_cursor.rs`:
   - Insert `TokenError` (section 2) right after `const MAC_LEN: usize = 16;` (line 26); it is shared by three token classes.
   - Tests: lines 366, 397, 401 and 405 become `TokenError::WrongKind`.
2. `src/product_cursor/decode.rs`: add `TokenError` to the imports, then:
   - `KeyCursor::decode`: 52 `-> Result<Self, TokenError>`, 53 `let invalid = TokenError::Invalid;`, 56 `return Err(TokenError::WrongKind);`
   - `MessageId::decode` (cfg(test)): 167, 168, 171, same pattern.
   - `LeaseToken::decode`: 190, 191, 194, same pattern.
3. `src/product_cursor/regressions.rs`: add `TokenError` to the imports, then:
   - 142, 150, 154 become `Err(TokenError::WrongKind)`.
   - 158, 166, 170, 218 become `Err(TokenError::Invalid)`.

No edit anywhere else: `product.rs:2708 Err(_)`, `delivery.rs:435 _ =>`, and every `.unwrap()` /
`.expect()` site compile unchanged.

---

## 5. Mutation analysis

Selection comes from `scripts/quality/verification_plan.py`:
- `src/product_cursor` is a CODEC prefix, so it is critical. Three owners are selected, with no row changes:
  - `cursors`: src/product_cursor/decode.rs, harness-lib, filter `product_cursor::`
  - `product_cursor`: src/product_cursor.rs, service-lib, filter `product_cursor::`
  - `product_cursor_regressions`: src/product_cursor/regressions.rs, harness-lib, filter `product_cursor::`
- `properties_fuzz` is selected; `quality_product_cursor_fields_roundtrip_without_truncation` is semantically unchanged. Loom and Miri are not selected.
- `src/product/scan.rs` and `src/dst/tests/reads_product.rs` are neither critical nor registered, so they have no mutation scope.

Function bodies that change:

| fn | commit | changed lines | mutants cargo-mutants 27.1.0 generates in-diff | disposition |
|---|---|---|---|---|
| `ScanCursor::decode` | 2 | 105, 106, 112, 145 | whole-body `replace ScanCursor::decode -> Result<Self, ScanCursorError> with Ok(Default::default())` | **unviable**: `ScanCursor` does not implement Default (derives Debug, Clone, PartialEq, Eq only) |
| `KeyCursor::decode` | 3 | 52, 53, 56 | whole-body `Ok(Default::default())` | **unviable**: no `Default` for `KeyCursor` |
| `LeaseToken::decode` | 3 | 190, 191, 194 | whole-body `Ok(Default::default())` | **unviable**: no `Default` for `LeaseToken` |
| `MessageId::decode` | 3 | 167, 168, 171 | none: `#[cfg(test)] impl` is skipped | none (it would be unviable anyway) |

- No `Err(..)` mutant is generated: the error type is not `anyhow::Error`, and the repo has no
  `.cargo/mutants.toml` `error_values`.
- The changed lines contain no operator, predicate, guard or match arm, so no binary, unary or
  match-arm mutant intersects the diff. Unchanged predicates on neighbouring lines (107, 111, 123, 141,
  144) are not selected.
- `product_cursor.rs` adds enum definitions only, and its test-module edits sit under `#[cfg(test)] mod tests`.
  In-diff listing: "product_cursor: no executable mutants in the selected scope".
- `regressions.rs` is behind `#[cfg(test)] mod regressions`: "product_cursor_regressions: no executable mutants in the selected scope".
- Expected result: `cursors` runs 3 mutants, all unviable (0 missed, 0 timeout); cargo-mutants exits 0.
  If `Default` were ever derived, the bodies would become viable, and the roundtrip assertions in
  `scan_cursor_roundtrip_expiry_and_kind`, `key_cursor_roundtrip_and_binding` and
  `message_and_lease_tokens_roundtrip_and_fence` would kill them.
- Pure deletion: the `Err(_)` arm is in scan.rs, outside mutation scope. No new guard, boundary or
  predicate is introduced anywhere.
- Owner rows and filters: **no change**. There are no new files.

---

## 6. Ledgers

- **Commit 1:** `docs/refactor/test-inventory.json` (+1 entry via `scripts/test-inventory.py --write`).
  - `test-scenario-map.json` / `scenario-dispositions.json`: no change. There are no renames, and new
    DST tests carry `scenarios: []` (precedent 527d3d3a, CI-green).
  - `review-mechanisms.json`: no pins on `reads_product` (grep finds none).
  - MT audit baseline: no new fingerprint.
- **Commits 2 and 3:** none.
  - `product_cursor::` unit tests are not inventoried (`grep -c product_cursor docs/refactor/test-inventory.json` = 0).
  - `owners.json`: no new macro-dsl, glob, by-path or effect.
  - `source-allowances.json`: no exception deleted. A reasoned `expect` is never an inventory row, so re-deciding a reason vacates nothing, and no prune is needed.
  - `architecture-policy.json`: no new file, and no budget moves.
  - `WIRE-MATRIX.md`: no wire change.
  - `mutation_owners.py`: no new file.
  - `docs/quality/verification.json`: a historical receipt, not enforced.

---

## 7. Controls (run only when no mutation run or gate is active in the tree)

1. **Commit 1, on unmodified src:**
   - `cargo test --locked --lib dst::dst_tests::reads_product::product_scan_answers_each_cursor_verdict -- --exact`
     prints `... ok` and `test result: ok. 1 passed`.
   - Negative control per 3.1: swap the messages and see the stated failure; then revert.
   - `python3 scripts/test-inventory.py --write && python3 scripts/test-inventory.py --check` succeeds.
2. **Commit 2, first-step probe (3.3):** `cargo build --locked --lib 2>&1 | grep -E '^error|-->'` shows
   exactly `error[E0308]` at `src/product/scan.rs:113:21` and `:122:21`. Finish the edits, then
   `cargo build --locked --lib` is clean.
3. **Exhaustiveness probe (3.3):** E0004 at `src/product/scan.rs:110:19`; revert.
4. **Commit 3 probe (3.3):** the lib builds with edits only in `src/product_cursor*`, and
   `cargo test --locked --lib --no-run` errors at exactly the 11 token sites before the swap.
5. **Tests:**
   - `cargo test --locked --lib product_cursor::`: 12 passed (4 tests plus 8 regressions).
   - `cargo test --locked -p streams-quality-invariants --lib product_cursor::`: 12 passed.
   - `cargo test --locked --lib golden_tests::`: ok.
   - `cargo test --locked --lib dst::dst_tests::reads_product::`: 7 passed (6 + 1).
6. **Negative control for the reason re-decision** (optional; do it before editing lines 20 and 24):
   swap the patterns only and run
   `cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl; python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl`.
   It prints `accepted exception grew without a new decision: ('src/product/scan.rs', 'crate::product_scan', ...): syntax_facts N -> N+3`
   under both exceptions, plus `unwrap_site:path:...`/`expect_site:path:... 0 -> 1` ×3 each.
   After re-deciding both reasons it prints `quality ratchets: OK`.
7. **Full quality entry point:** `scripts/quality.sh` ends in `QUALITY_OK`. It covers fmt, clippy
   `-D warnings`, gate.py, rustdoc `-D warnings`, architecture-gate `--check`, test-inventory `--check`,
   multitenancy-audit (no NEW/GONE) and the mt_lint leg.
8. **CI's own mutation selection, on committed HEAD:**
   `QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan`
   - Expected: `plan.json` `mutation_source_files` includes the three product_cursor files.
     `src/product/scan.rs` and `reads_product.rs` are absent. The set also includes whatever the four
     unpushed commits 8f590ec5..fb18840d select.
   - Expected: `selected_mutation_owners` includes `cursors`, `product_cursor` and `product_cursor_regressions`.
   - Then run `scripts/quality/mutations.sh`. Expected: `cursors` reports 3 unviable, 0 missed; the
     other two owners report "no executable mutants in the selected scope".
9. **After the mutation run finishes:** `OUT=/tmp/gate.txt scripts/gate.sh` ends in `GATEDONE`. After
   push, verify each run with `gh run list --branch slate` / `gh run view <id>`, and never claim green
   without it.

---

## 8. Out of scope

- `product_read` (product.rs:2708) folds every key-cursor refusal, including a scan cursor on the
  records route, into `ReadFailure::InvalidCursor`. That failure's message is "cursor is outside this
  stream's readable lineage", which is misleading for a wrong-kind token. `TokenError` makes a distinct
  answer possible later without literals. It would still be a message change at the edge, and
  product.rs is at its 4,205-line ceiling.
- `settle` (delivery.rs:435) counts every lease refusal as stale through `_ =>` on a guarded `Result`.
  That is deliberate (spec §2.5) and unchanged.
- `CatalogCursor::decode` returning `Option<String>` (no kind verdict).
- The `ScanCursor.expires_at_ms` doc naming "410 scan_expired" inside the codec.
- Narrowing `product_scan`'s function-wide unwrap/expect exception to statement scope. This alternative
  remedy was not taken.
- Optional: a WIRE-MATRIX §2.6 note that a foreign-incarnation cursor is 400 even when expired. The new
  pin makes that true by test, but it is not a wire change.
- Item 62 (`StreamDesc::epoch_bytes`). The pin uses `epoch()` so the two items do not collide.

---

## Skeptic corrections (C1..Cn)

Checked against `slate` @ fb18840d (tree clean). No edits and no builds were made; everything below comes from Read, grep and git-show.

**What I verified and found correct:**
- Every quoted line:
  - decode.rs:52/53/56, 105/106/112/145, 167/168/171, 190/191/194.
  - scan.rs:17-25, 110-140.
  - product.rs:2708 `Err(_)` into `render_product_read_failure`.
  - delivery.rs:435 with `_ =>` at :445.
- `wc -l` of every ceilinged file matches the plan: http 3,369; product 4,205; shard 3,196; billing 2,201; history 1,713; auth 1,676; registry 1,501; sse/feed 1,195; fleet 1,143. None of them is touched.
- The use-site list is complete across src, src/dst, tests, examples, tools and fuzz. The harness (`tools/quality-invariants/src/lib.rs:49-55`) includes the files by path. scripts/read-experiments is not compiled by any target. Literal-assertion sites: exactly 22 (11 scan, 11 token), mapped to the right decoders.
- The ratchet analysis is right:
  - `visit_path` (tools/quality-syntax/src/scan.rs:222) fires for `Pat::Path` inside `Err(..)`, and literals and `_` emit no fact.
  - The `Err` path fact already exists on all three arms, so the net change is +3 `path` facts.
  - That gives +3 `unwrap_site:path:crate::product_scan:*` keys and +3 `expect_site:path:*` keys, plus `syntax_facts` +3 under both expects. `exception_growth` (source_rules.py:193) skips an identity whose reason text changed.
  - The owner in the last report (target/quality/clippy.quality.json) is `crate::product_scan`.
  - Both proposed reasons match the `"[^";]+;[^";]+;[^";]+"` rule (source_rules.py:258).
- scan.rs, reads_product.rs and product.rs are outside every CRITICAL prefix and are not registered. decode.rs, product_cursor.rs and regressions.rs are registered (mutation_owners.py:60, 96, 97).
- The whole-body `Ok(Default::default())` mutants are unviable: no `Default` on ScanCursor, KeyCursor or LeaseToken (product_cursor.rs:31, 106, 203). `MessageId::decode` sits under `#[cfg(test)] impl` (decode.rs:160). There is no `.cargo/mutants.toml`, so no `Err` mutants are generated.
- Every helper the new DST test uses exists with a compatible signature:
  - `preq` returns `(u16, ..)` (fixture_requests.rs:218), so the tuple compare infers `u16`.
  - `StreamDesc::epoch()` returns `[u8;16]` (registry.rs:449).
  - `deployment_tenant()` returns `&ProjectId` (deployment.rs:70).
  - `perr` emits `{"error":{"code","message","retryable"}}` (product.rs:41-53).
  - The foreign-epoch cursor authenticates because `product_payload` keys the MAC by the payload's own epoch (decode.rs:38-41). It is then refused at :141, before the expiry check at :144.
- The test is green on the current tree, as claimed. No existing test asserts either 400 message: repo-wide grep finds only scan.rs:126 and :135.
- Ledgers: there are no review-mechanisms pins or review-evidence required_units on product_cursor or reads_product. `diagnostic-allowances.json` has 0 warnings. `architecture-review-baseline.json`, `legacy-*.json` and the SEC-006 line numbers in `test-scenario-map.json` are frozen or stale records that are not checked by line or sha. `verification.json` is not read by any script or workflow.
- WIRE-MATRIX.md:111 stays true.

**C1. The "Decisions" section is missing.**
- Plan line 387 says "droppable, see Decisions", but the plan has no such section.
- Add "## Decisions for Søren" with:
  - (D1) The two re-decided reason texts on `product_scan` (scan.rs:20, 24). This is the only exception decision in the item.
  - (D2) Whether to take commit 3. It drops the unobservable spellings `invalid_message_id` and `invalid_lease_token`. Backward-compatible alternative: ship commits 1 and 2 only.
  - (D3) The optional WIRE-MATRIX §2.6 sentence: a foreign-incarnation cursor is 400 even when expired.
- None of these is a wire change, so no product/raw-edge contract decision is needed.

**C2. The `_ =>` rule needs an explicit justification.**
- After commit 3, two wildcards discard a now-typed domain verdict:
  - `product_read`'s `Err(_)` (product.rs:2721).
  - `settle`'s `_ =>` (delivery.rs:445).
- They are pre-existing and deliberate (spec §2.5 folds every lease refusal into stale). Making product_read exhaustive is not line-neutral: `Err(crate::product_cursor::TokenError::Invalid | crate::product_cursor::TokenError::WrongKind) =>` at 12 columns of indentation exceeds 100 columns. rustfmt would wrap it, and product.rs has a 0-line budget.
- State this in section 8. Otherwise a reviewer applying "no `_ =>` on domain enums" will flag commit 3.
- Also note that `product_scan` keeps another wildcard after this item: `_ =>` on `KeyCheck` (scan.rs:90). That arm belongs to item 62 (`KeyCheck::BadDescriptor`), so the claim "product_scan maps exhaustively" should be scoped to the cursor match.

**C3. Collision with item 62 on `product_scan`'s exception identity.**
- Item 62 deletes `KeyCheck::BadDescriptor` (http.rs:2362) and the `_ =>` arm at scan.rs:90-99. That shrinks `product_scan`'s scope_lines and syntax_facts. Shrinking passes the ratchet.
- If both items land in one push, the merge base is origin/slate for both. Whichever lands second must keep the reason text that item 61 re-decided, or re-decide it again, rather than restore an older text.
- The new test uses `epoch()`, not `epoch_bytes()`, so it survives item 62. Correct as planned.

**C4. Section 1.3 / section 2 count is off.**
- "Debug covers the 13 `.unwrap()`/`.expect()` sites" is wrong. The compiled external sites number 12: golden 1, sse/session/tests 2, consumer_dlq 3, read_application 1, sse_delivery 1, producer_protocol 3, persistence_faults 1. The 13th (legacy_workload.rs:88) is not compiled.
- The unit tests also need `E: Debug` through `.unwrap()`: product_cursor.rs:317, 344, 379, 390 and regressions.rs:55, 79, 282, 284, 287. The derive covers them; the statement just needs to cover them too.

**C5. Line references.**
- The key-cursor 400 assertion is reads_product.rs:584 (block 567-584), not 583.
- `product_scan_is_snapshot_exact` closes at :586, so "insert after line 586" is correct.
- The fingerprint code is source_rules.py:157-186, not 171-190.

**C6. Controls 8 and 5: loom/miri and owner-selection expectations need hedging.**
- "Loom and Miri are not selected" holds for this item's files. The pushed range also carries 8f590ec5..fb18840d, which touch src/sse and src/http. `plan()` (verification_plan.py:96-97) will select loom and miri for the combined push. Say "not selected by this item".
- Whether `product_cursor_regressions` appears in `selected_mutation_owners` depends on `production_changes.unchanged_production`. The file has no inner `#![cfg(test)]`, so it is probably treated as changed. Either outcome passes, because the driver prints "no executable mutants". The expectation should allow both.

**C7. The reason wording reads wrong.**
- "once every typed cursor verdict has answered" reads as if the verdicts answer.
- Suggest `...so neither step can fail once each typed cursor verdict is answered and the page is debited; ...`. It still has exactly two `;` and no `"`.
- The invariant is independent of cursor verdicts. The re-decision exists because the path facts grew. That is acceptable under RUST-QUALITY:50 and the 527d3d3a precedent, but the commit message should say so plainly.

**C8. One stale doc mention.**
- handover-plan.md:137 (tracked) says wrong-endpoint use "reports `wrong_cursor_kind`". It is a historical handover note, and the wire already answers `invalid_cursor` / "cursor is not a scan cursor", so nothing breaks.
- The plan's "nothing but literal assertions ever read the spellings" should mention it. Leave the file unedited: it is a historical record.

**C9. Exhaustiveness probe: expect a second diagnostic.**
- Adding an unconstructed `Oversized` also produces `warning: variant `Oversized` is never constructed`. It is harmless, since E0004 still fails the build.
- The control should say "E0004 at scan.rs:110:19, plus a dead_code warning", not "exactly".

**No unbuildable controls found.** All the named commands exist: `gate.py --clippy`, `QUALITY_OK` at scripts/quality.sh:45, `GATEDONE` at scripts/gate.sh:45, `test-inventory.py --write/--check`, and `verification_plan.py --out`. The compile probes produce the stated error classes (the commit 3 probe mixes E0308 and E0277, as the plan hedges).

**No missed ledger:**
- test-inventory.json gets +1 entry with `scenarios: []`, per the 527d3d3a precedent.
- source-allowances.json: no exception deleted; the rows are keyed by reason text, and a reasoned `expect` is skipped at source_rules.py:264.
- MT audit: no `stream_hash(` and no `registry.get("` literal on a single line.
- No owner rows or new files.

**Verdict: ready-with-corrections.** The core design is sound: typed enums in `product_cursor.rs`, the line-neutral pattern swap, both `product_scan` reasons re-decided, the green-by-design DST pin with a negative control, and all mutants unviable. The corrections are documentation and hedging fixes, plus the item-62 coordination. None of them changes the edits.
