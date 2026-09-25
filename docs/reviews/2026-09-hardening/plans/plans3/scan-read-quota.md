# Plan: `GET {collection}:scan` draws on the project read-byte quota (review rank 28, step A)

Repo: `/Users/sorenschmidt/code/streams`, branch `slate` @ `82095942`. Read-only verification; nothing edited.

## 0. Claim verification — CONFIRMED, not stale

| Claim | Evidence |
|---|---|
| `check_read_quota` / `debit_read_response` wrap only GET records and long-poll | `src/product.rs:1151-1184`: both `(Method::GET, None)` and `(Method::GET, Some("long-poll"))` arms call `check_read_quota(&state, principal.as_ref())` before `product_read` and `debit_read_response(&state, principal.as_ref(), &resp)` after. The SSE arm (1185-1216) uses `admit_subscription` instead (by design). |
| `product_scan` neither checks nor debits | `src/product.rs:1371`: `(Method::GET, Some("scan")) => product_scan(state, &tenant, name, headers, &query).await,` — no principal reaches it; `product_scan` (3006-3198) has no quota call. It calls `crate::billing::meter_read` (3187-3196, billing, not quota). |
| Up to 8 MiB per page | `src/product.rs:2452-2453`: `SCAN_DEFAULT_BYTES = 4 << 20`, `READ_MAX_BYTES_CAP = 8 << 20`; clamp at 3125-3130. |
| A project in read debt keeps exporting | `check_read` (`src/quota.rs:714-735`) refuses only when the bucket `level < 0.0`; nothing on the scan path calls it, and nothing debits, so the bucket never goes negative from scans. |
| `debit_read_response` re-derives facts from the HTTP response | `src/product.rs:822-841`: `resp.status().is_success()` and `axum::body::HttpBody::size_hint(resp.body()).exact()` decide whether and how much to debit — a domain decision reconstructed from the wire response (docs/RUST-QUALITY.md "Canonical boundaries"). |

Quota owner facts (`src/quota.rs`): `check_read` (714-735) refills and refuses with `QuotaRefusal::Rate { retry_after_secs }` when `level < 0`; `debit_read` (744-760) refills then `level -= bytes` unconditionally, early-returns when `read_bytes_per_sec == 0 || bytes == 0`; the bucket is created full at `admit` (`Bucket::full(quotas.read_bytes_per_sec, now)`, 568) — `project_admission` runs before every product request (`src/http.rs:2088`), so `tracked(project)` exists by the time any page is served. `QuotaRefusal::Rate` renders as 429 `project_rate_limit` + `retry-after`, audit-tagged (`src/product.rs:876-888, 916-926`).

Scope: `:scan` requires `streams.records.read` (`src/product.rs:601-609`), i.e. it is already classified as a bulk record read — only the volume quota was missed.

## 1. Mechanism (today)

```
product_entry_axum_inner (src/http.rs:2067)
  └ project_admission (rate/concurrency; tracks the project; creates the read bucket full)
  └ product::product_entry (src/product.rs:1063)
      ├ Records GET / long-poll:  check_read_quota → product_read → debit_read_response(size_hint of Response)
      ├ Records SSE:              admit_subscription → product_read → guard rides the body
      └ Collection :scan:         product_scan(state,&tenant,name,headers,&query)   ← no check, no debit
```

`product_scan` frames the page into `body: Vec<u8>` (3147-3174) — the exact framed byte count is `body.len()` at the point the 200 is decided (3175-3197). That is the typed render site.

## 2. Designs

### Hard constraints discovered (these shape every design)

1. **`product_entry` is at its architecture function budget exactly.** `docs/refactor/architecture-policy.json` `budget_exceptions["function:src/product.rs::product_entry"].limit = 318`; `scripts/architecture-gate.py` measures it at 318 today (`find_functions`, `count > limit` fails). A re-decided `#[expect]` reason does NOT lift this. Any growth in `product_entry` must be paid inside `product_entry`.
2. **`product_scan` cannot gain one call where it stands.** Its `#[expect(clippy::unwrap_used, clippy::expect_used, …)]` (2997-3001) fingerprints every call/path expression (`scripts/quality/source_rules.py:159-188`); a new `debit_read_bytes(..)` call = "accepted exception grew". Its second attribute (`too_many_lines`, 3002-3005) ratchets `scope_lines`/`syntax_facts`. Route: verbatim move to a new file first (identity = `(path, qualified, kind, value)`, `source_rules.py:196`; a new path is skipped by `exception_growth`, line 207-208). Precedent: `d902a85b` (feed → `sse/feed/drive.rs`).
3. **`product_entry`'s `unwrap_used` attribute (1053-1056) fingerprints the whole `product_scan(...)` / `product_read(...)` call tokens** (`tools/quality-syntax/src/scan.rs:242-255`). Passing the principal changes those calls ⇒ that reason must be re-decided (sanctioned route). Its second attribute (1057-1062) only ratchets `scope_lines`/`nested_items`/`syntax_facts` — it stays untouched if `product_entry` does not grow.
4. **`mt_lint` does not exempt nested product files.** `src/mt_lint.rs:101,106,441-449`: `SURFACE_FILES`/`STREAM_REF_FILES` match `"product.rs"` only; `"product/scan.rs"` would flag `name: String` (`name-param-shared-core`, 179-188) and `tenant.stream_ref(&name)` (`stream-ref-construction`, 288-300). Same reason `product_usage` takes `sref: TenantStreamRef` (`src/product/usage.rs:32-36`). So the moved `product_scan` must take the resolved `TenantStreamRef` (the fix commit) — which also keeps it at 5 params (no `too_many_arguments` expectation needed).
5. **Architecture reverse-edge rule.** A file outside `transport_and_composition_files` may not reference `crate::http::`/`crate::product::` more than its baseline (0 for a new file) (`architecture-gate.py:117-120`). `product_scan` has 5 `crate::http::` references + `AppState`. So `src/product/scan.rs` joins `transport_and_composition_files` + `transport_rationales` in `docs/refactor/architecture-policy.json` (precedent: `d902a85b` added `drive.rs` to `sse_core_files`).
6. **New function budget**: a new file's functions are capped at 200 lines by the architecture gate (`max(200, before)`); `product_scan` is 193 today — keep it ≤ 200 in `scan.rs` (the check therefore lives in `product_entry`, not inside `product_scan`).
7. **Mutation leg**: `src/product.rs`, `src/product/*`, `src/dst/*` are not under `CRITICAL_PREFIXES` (`scripts/quality/verification_plan.py:22-31`) and have no `mutation_owners.py` row ⇒ `mutants: false` for this diff. No owner row needed. (Do NOT touch `src/quota.rs` — that would select `quota_registry`.)
8. **DST file ceiling**: `src/dst/tests/quota_enforcement.rs` is 946 lines; the red test formats to 79 lines ⇒ does not fit. New by-path module (+ `owners.json` by-path row + `test-inventory.json --write`). Its `quota_rig` helper is private and its `#[expect(too_many_lines)]` would ratchet on a visibility change (+1 `syntax_facts`), so the new module uses `fixture_auth::{auth_rig, rig_policy, rig_publish_policy, mint_token}` instead (`src/dst/tests/fixture_auth.rs:188-326`) — zero edits to `quota_enforcement.rs`.

### Design S — smallest correct (scan only)

Move `product_scan` verbatim to `src/product/scan.rs`; give it `sref` + `principal`; check in the `product_entry` scan arm; debit `body.len()` at the render site through a new typed `debit_read_bytes`; keep `debit_read_response` for the records arms (make it delegate to `debit_read_bytes`). Pays `product_entry` growth (+12 lines for the scan arm) with the GET/long-poll arm fold (below, −16), because constraint 1 forbids +12 outright. Leaves the HTTP-derived debit for records reads in place ⇒ two debit mechanisms, the boundary smell the reviewer named survives, `debit_read_response` is not deleted.

### Design O — owner-first (RECOMMENDED; it is what the reviewer's step A describes)

Same move; the records path switches to the typed debit too: `product_read` takes `principal: Option<&RequestPrincipal>` instead of `lease` (derives the SSE lease itself), `render_product_read` debits `payload.len()`, `debit_read_response` is deleted, `check_read_quota` stays the single admission helper called from `product_entry` for all three page routes (records GET, long-poll, `:scan`). The two records arms fold into one (`live @ (None | Some("long-poll"))`) — a genuine special-case removal that also pays the scan arm's lines under constraint 1. Every debit now happens where the framed bytes are known; a refusal never reaches a render site.

Cost: three re-decided reasons in `product.rs` (`product_entry` unwrap; `product_read`; `render_product_read`), one re-decided reason in `scan.rs` for push-order independence, one verbatim-move commit. `product.rs` ends ~207 lines smaller.

Alternative O-full (not for step A): also move `product_read` + `render_product_read` + `render_product_read_failure` + the two quota helpers into `src/product/read.rs` (~650 lines, one "product page" owner, zero re-decisions in `product.rs` besides `product_entry`). Bigger move, same behaviour; offer it only if the reviewer prefers zero re-decisions to a smaller diff.

### Debit before or after rendering — decision

**After framing, at the 200 decision, never before.** The only byte count that means "served" is the framed body (`body.len()`, JSON framing + base64 expansion for `bytes` streams), which exists only after framing; every refusal (`missing_key`, `invalid_cursor`, `scan_expired`, `wrong_key`, `not_found`, `creating`, `render_product_read_failure` from `execute_scan`) returns before the render site, so a refused page is never debited; a 204 long-poll timeout debits 0 (`debit_read` early-returns on 0). This is byte-identical to today's `size_hint().exact()` on a sized 200 body, so no quota arithmetic changes for records reads. Debiting "before" would require guessing a size and would charge pages that are then refused; debiting on a streaming body is impossible and stays the subscription slot's job.

## 3. Red test (near-complete Rust)

New file `src/dst/tests/quota_read_volume.rs` (formatted with the repo's rustfmt; 91 lines, test fn 79 lines < 100, nesting ≤ 2, no expectations needed):

```rust
//! Read-byte quota (§17.2) on every page-serving product route.

use super::fixture_auth::{auth_rig, mint_token, rig_policy, rig_publish_policy};
use super::fixture_http::engine_shutdown;
use super::fixture_requests::{PRISMA_KEY, preq};

/// RED (review rank 28): `GET {collection}:scan` pages back decrypted
/// record bodies, so a page draws on the SAME project read-byte
/// bucket as `GET records`: the project is admitted at entry only
/// while the bucket is not in debt, a served page is debited by its
/// framed body bytes, and a refused page (wrong key) is never debited.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_pages_draw_on_the_read_byte_quota() {
    let (svc, state, addr) = auth_rig("proj-rv", "ws-rv", &["c-rv"], None).await;
    // Policy v2: an 8 B/s read budget, published before the first request
    // so the project's bucket is created from it.
    let mut policy = rig_policy("proj-rv", "ws-rv", 1, 2);
    policy.quotas.read_bytes_per_sec = 8;
    rig_publish_policy(&svc, policy, 2).unwrap();
    let bearer = mint_token("c-rv", "proj-rv", "ws-rv", 1, 1, "t", 3600);
    let a = ("authorization", bearer.as_str());
    let ekey = ("prisma-encryption-key", PRISMA_KEY);
    let wrong = (
        "prisma-encryption-key",
        "CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg=",
    );
    let text = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
    let (st, _, _) = preq(
        addr,
        "PUT",
        "/v1/streams/sq",
        &[ekey, a],
        br#"{"format":{"kind":"json"}}"#,
    )
    .await;
    assert_eq!(st, 201);
    // A ~2 KiB record: the page's debt takes minutes to refill at 8 B/s,
    // so no scheduling gap between two requests can clear it.
    let record = format!("{{\"pad\":\"{}\"}}", "x".repeat(2048));
    let (st, _, _) = preq(
        addr,
        "POST",
        "/v1/streams/sq/records",
        &[ekey, a],
        record.as_bytes(),
    )
    .await;
    assert_eq!(st, 200);
    // A refused page is not read volume: 403 leaves the bucket full.
    let (st, _, b) = preq(addr, "GET", "/v1/streams/sq:scan", &[wrong, a], b"").await;
    assert_eq!(st, 403, "{}", text(&b));
    // No debt yet: the first page serves, and it exceeds the budget.
    let (st, _, b) = preq(addr, "GET", "/v1/streams/sq:scan", &[ekey, a], b"").await;
    assert_eq!(
        st,
        200,
        "a refused scan must not have been debited: {}",
        text(&b)
    );
    assert!(
        b.len() > 2048,
        "the page must exceed the budget: {}",
        b.len()
    );
    // That page put the project in debt: the next scan is refused with
    // the read-quota class, and so is a records read — one bucket.
    let (st, h, b) = preq(addr, "GET", "/v1/streams/sq:scan", &[ekey, a], b"").await;
    assert_eq!(
        st,
        429,
        "scan in read debt must refuse typed: {st} {}",
        text(&b)
    );
    assert!(
        text(&b).contains("project_rate_limit"),
        "typed code: {}",
        text(&b)
    );
    assert!(
        h.get("retry-after").and_then(|s| s.parse::<u64>().ok()) >= Some(1),
        "retry-after: {h:?}"
    );
    let (st, _, b) = preq(addr, "GET", "/v1/streams/sq/records", &[ekey, a], b"").await;
    assert_eq!(
        st,
        429,
        "a scan page is debited to the shared bucket: {st} {}",
        text(&b)
    );
    engine_shutdown(&state).await;
}
```

Helpers reused (all `pub(super)` in `src/dst/tests/`): `fixture_auth::auth_rig` (enforce rig, RSA kid `rig-1`, policy v1 with default quotas, 249-294), `rig_policy` (188-203; `quotas` is a public field), `rig_publish_policy` (205-217; feed v2 > v1, `project_policy_version` 2 keeps the token's `ownership_version` 1 valid — same pattern as `max_streams_transition_seeds_from_reality`, `quota_enforcement.rs:905-929`), `mint_token` (300-326; scope = `RIG_SCOPES` which includes `streams.records.read`, the scope `:scan` requires), `fixture_requests::preq` (196; reads to EOF with `connection: close`, headers lower-cased — every wait is bounded by the server closing), `fixture_http::engine_shutdown`. The wrong key is the 32×0x08 key `security_audit.rs:58-61` uses for its 403.

**Exact failure on current code (`82095942`)** — the fourth request:

```
thread 'dst::dst_tests::quota_read_volume::scan_pages_draw_on_the_read_byte_quota' panicked at src/dst/tests/quota_read_volume.rs:68:5:
assertion `left == right` failed: scan in read debt must refuse typed: 200 [{"routingKey":"","value":{"pad":"xxxx…xxxx"}}]
  left: 200
 right: 429
```

Today `product_scan` is never checked and never debits, so the second page is served (200, a ~2.1 KB page) instead of 429. If only the check were added without the debit, the same assertion fails the same way (the bucket never goes negative). If the debit were placed before the key check (or on refusals), the assertion at line 54 fails instead (`a refused scan must not have been debited: … left: 429 right: 200`). The last assertion (records read 429) proves the scan's debit landed in the shared bucket; on current code it is unreached (the run stops at line 68), after the fix it distinguishes "debited a private counter" from "debited the project bucket".

Why it cannot flake: the page's debt (~2.1 KB at 8 B/s) takes ~260 s to refill (`Bucket::refill`, `src/quota/bucket.rs:20-24`; capacity = one second of rate), the requests are sequential local HTTP, there are no sleeps and no spawned tasks, and a 429/403/200 all close the connection, so `preq` cannot hang. It runs under the full-suite floor (`tests_ran.py --inventory … --skipped 1`, `ci.yml:106`) once inventoried.

Unit-level red (optional, cheap): none needed — `check_read`/`debit_read` are already pinned in `src/quota/tests.rs:199-246`; the defect is purely a missing call in the transport adapter, which the DST test pins end-to-end.

## 4. Code change (near-complete Rust, per file, with budgets)

Commit order (push together; ratchets compare the final tree against the push's `before`):

1. **Move** — `product_scan` verbatim into `src/product/scan.rs` (+ policy row).
2. **Red** — the DST module above + `dst_tests.rs` mod + `owners.json` row + `test-inventory.json --write` (capture the failure text for the fix commit's message by running the test at this commit).
3. **Fix** — the quota plumbing + docs.

(2 and 3 may be one commit; the inventory must be regenerated in whichever commit adds the test.)

### 4.1 `src/product/scan.rs` (NEW; ~220 lines after the fix; ceiling 1,000; `product_scan` 195 lines < 200 architecture budget — comment lines count, `product_entry` measures 318 physical)

Commit 1 content = the module header + `product_scan` copied verbatim from `src/product.rs:2997-3198` with `async fn` → `pub(super) async fn`, plus — so the move commit is `mt_lint`-green on its own — two marker comments the fix commit removes (`// mt-lint: allow(name-param-shared-core): moved verbatim; the fix takes the resolved reference` above the attributes and `// mt-lint: allow(stream-ref-construction): same` directly above the `registry.get` line). If the pair is only ever gated together, the markers can be skipped (precedent `d902a85b`: "the full gate runs on the pair").

After commit 3:

```rust
//! `GET {collection}:scan` — the snapshot export page: resolves the
//! collection, decodes the frozen scan cursor and frames one page of
//! decrypted records (docs/refactor/WIRE-MATRIX.md §2.6). The entry
//! admits the read-byte quota; this render site debits the framed page.
use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;

use super::{
    READ_MAX_BYTES_CAP, SCAN_DEFAULT_BYTES, SCAN_TTL_MS, debit_read_bytes, perr, product_key,
    q_num, render_product_read_failure, strict_query,
};
use crate::http::AppState;

#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "product_scan; a routing key serializes as a JSON string and the response builder holds a fixed status and validated headers, so neither step can fail once the page is debited; mapping either into a substitute response would report a wire status the handler never decided"
)]
#[expect(
    clippy::too_many_lines,
    reason = "product_scan; the scan resolves, decodes and pages the frozen cursor and debits the page it frames in one sequence; splitting it would separate the page from the cursor it advances and the bytes it charges"
)]
pub(super) async fn product_scan(
    state: Arc<AppState>,
    sref: crate::tenant::TenantStreamRef,
    headers: HeaderMap,
    query: &str,
    principal: Option<&crate::auth::RequestPrincipal>,
) -> Response {
    /* … verbatim 3013-3025 (missing_key, strict_query) … */
    let desc = match state.registry.get(&sref).await {          // ← was &tenant.stream_ref(&name)
        /* … verbatim 3027-3057 … */
    };
    /* … verbatim 3058-3174: key check, cursor decode, maxBytes, execute_scan, framing into `body` … */
    body.push(b']');
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store");
    /* … verbatim 3179-3186: Prisma-Next-Scan-Cursor / Prisma-Scan-Complete … */
    crate::billing::meter_read(
        &state,
        &desc,
        outcome.records.iter().map(|record| record.payload.len() as u64).sum(),
        outcome.records.len() as u64,
    );
    // The page is decided: charge the framed bytes the transport will send.
    debit_read_bytes(&state, principal, body.len());
    response.body(Body::from(body)).unwrap()
}
```

Notes: `tenant`/`name` parameters are replaced by `sref` (5 params; no `too_many_arguments`; no `mt_lint` markers because the nested file never constructs identity or receives a bare name). `sref` by value on an `async fn` that only borrows it is the exact shape of `product_usage` (`src/product/usage.rs:32-48`), which is clean under `needless_pass_by_value = "warn"` + `-D warnings` (clippy exempts async fns; `product_read`'s by-value `name` is the same case). Every other line is verbatim. Both `#[expect]` reasons are new decisions (three `;`-separated parts, no `;`/`"` inside a part) so the commit pair is push-order independent: pushed together, `scan.rs` is entirely new to the ratchet; pushed separately, the changed reasons are the explicit new decision. `unfulfilled_lint_expectations` is satisfied (`.expect("string serialization")`, `.unwrap()`, 195 lines: 193 today − `tenant`/`name` params + `sref` + `principal` + one comment + one debit line). Explicit `use super::{…}` (no glob) ⇒ no `unresolved-glob` owners row; `use crate::http::AppState` + the five `crate::http::` calls are covered by the policy row in §5. rustdoc: backticked `{collection}` is fine under `-D warnings`.

### 4.2 `src/product.rs` (CEILINGED: 4,484 → 4,283 after commit 1 → ~4,277 after commit 3; must never exceed 4,484)

**Commit 1 (−203 +2 = −201):** delete 2997-3199 (`#[expect]`s, `product_scan`, trailing blank); at 4311 (`mod usage;`) add:

```rust
mod scan;
use scan::product_scan;
mod usage;
```

**Commit 3, `product_entry` (1053-1380; architecture budget 318 = current; ends at 314):**

Re-decide the `unwrap_used` reason (the `product_read`/`product_scan` call fingerprints change):

```rust
#[expect(
    clippy::unwrap_used,
    reason = "product_entry; the preflight response builder holds a fixed status and literal ASCII header values, so building it cannot fail; mapping a builder error into a substitute response would report a wire status the handler never decided"
)]
```

Leave the second attribute (`too_many_arguments, too_many_lines, excessive_nesting`) untouched: `scope_lines` 318 → 314, `syntax_facts` shrinks (two check/debit blocks and the `map(|pr| pr.lease())` closures go), `nested_items` unchanged, the SSE nested `match` keeps `excessive_nesting` fulfilled.

Replace 1151-1184 (two 17-line arms) with one 18-line arm, and change the SSE arm's lease argument:

```rust
                (Method::GET, live @ (None | Some("long-poll"))) => {
                    // §17.2: a page route is admitted only while the
                    // project's read-byte bucket is out of debt; the page
                    // render debits the framed bytes it serves.
                    if let Some(r) = check_read_quota(&state, principal.as_ref()) {
                        return r;
                    }
                    product_read(
                        state,
                        &tenant,
                        name,
                        headers,
                        &query,
                        live,
                        principal.as_ref(),
                    )
                    .await
                }
                (Method::GET, Some("sse")) => {
                    /* 1186-1201 unchanged */
                    let resp = product_read(
                        state,
                        &tenant,
                        name,
                        headers,
                        &query,
                        Some("sse"),
                        principal.as_ref(),      // was principal.as_ref().map(|pr| pr.lease())
                    )
                    .await;
                    /* 1212-1215 unchanged */
                }
```

Replace 1371 (1 line) with 13 lines (rustfmt-verified shape):

```rust
        (Method::GET, Some("scan")) => {
            if let Some(r) = check_read_quota(&state, principal.as_ref()) {
                return r;
            }
            product_scan(
                state,
                tenant.stream_ref(&name),
                headers,
                &query,
                principal.as_ref(),
            )
            .await
        }
```

Line arithmetic for `product_entry`: −34 +18 (fold) −1 +13 (scan) = −4 ⇒ 314 ≤ 318. Without the fold the scan arm alone is +12 ⇒ 330 > 318 and the architecture gate fails regardless of any `#[expect]` reason — the fold (or an equivalent −12 inside `product_entry`) is mandatory in every design. `tenant.stream_ref(&name)` is constructed in `product.rs`, a `STREAM_REF_FILES` ingress file (`mt_lint.rs:101`).

**Commit 3, quota helpers (806-841):** keep `check_read_quota` (808-818) verbatim; replace `debit_read_response` (820-841, 22 lines) with the typed debit (18 lines, rustfmt-verified):

```rust
/// Debit the SERVED page bytes — the framed body a render site hands to
/// the transport — against the project's read-byte bucket (§17.2
/// post-hoc volume metering). Only a render site knows that count, so
/// only a render site debits; a refused request never reaches one, and
/// a streaming body is governed by its live-subscription slot instead.
fn debit_read_bytes(
    state: &AppState,
    principal: Option<&crate::auth::RequestPrincipal>,
    bytes: usize,
) {
    let Some(p) = principal else { return };
    state.quotas.debit_read(
        &p.project_id,
        &p.quotas,
        bytes as u64,
        crate::shard::now_ms(),
    );
}
```

`usize as u64` already compiles clean under this profile (`record.payload.len() as u64`, today's 3193, sits under an expectation that does not list `cast_possible_truncation`). No `Result<_, Response>` ⇒ no new `result_large_err` allowance.

**Commit 3, `product_read` (2573-2843; ratcheted `too_many_arguments, too_many_lines`; 266 lines, architecture budget 324):** the principal replaces the lease, the mode string loses its `'static` (the folded arm binds it from `verb.as_deref()`), and the render call carries the principal. `syntax_facts` grows by ~4 (`principal`, `map`, `RequestPrincipal::lease`) ⇒ re-decide the reason:

```rust
#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "product_read; the read takes every extractor and the verified principal the entry resolved, dispatches raw, keyed and long-poll reads from one place and hands the principal to the page render that debits it; a request struct or a split would separate the dispatch from the parts it needs"
)]
async fn product_read(
    state: Arc<AppState>,
    tenant: &crate::tenant::ProjectId,
    name: String,
    headers: HeaderMap,
    query: &str,
    live: Option<&str>,                                   // was Option<&'static str>
    principal: Option<&crate::auth::RequestPrincipal>,    // was lease: Option<crate::auth::AuthLease>
) -> Response {
    /* 2587-2814 unchanged */
    if live == Some("sse") {
        let params = crate::http::ReadParams {
            /* 2817-2827 unchanged */
            lease: principal.map(crate::auth::RequestPrincipal::lease),   // was `lease,`
            internal_lease: None,
        };
        /* 2831-2837 unchanged */
    }
    match state.read_service().execute_read(command).await {
        Ok(outcome) => render_product_read(&state, principal, &skey, &rk, &outcome),
        Err(error) => render_product_read_failure(error),
    }
}
```

Net 0 lines. `live` is only compared against `Some("long-poll")`/`Some("sse")` (2782, 2815); `ReadParams.live` is built from a literal (2819), so dropping `'static` is type-safe. `RequestPrincipal::lease(&self) -> AuthLease` (`src/auth.rs:253-261`).

**Commit 3, `render_product_read` (2845-2893; ratcheted `unwrap_used`, fingerprints):** +1 param (5 ≤ threshold), +1 debit ⇒ +2 lines; the new call fingerprint requires the re-decided reason:

```rust
#[expect(
    clippy::unwrap_used,
    reason = "render_product_read; the status is fixed and every header value was validated when the descriptor and cursor were produced, so building the response cannot fail once the served bytes are debited; mapping a builder error into a substitute response would report a wire status the handler never decided"
)]
fn render_product_read(
    state: &AppState,
    principal: Option<&crate::auth::RequestPrincipal>,
    key: &crate::crypto::StreamKey,
    routing_key: &str,
    out: &crate::application::read::ReadOutcome,
) -> Response {
    /* 2855-2890 unchanged: headers, `payload` (empty on Timeout) */
    crate::http::meter_read_outcome(state, out);
    debit_read_bytes(state, principal, payload.len());
    response.body(Body::from(payload)).unwrap()
}
```

A 204 timeout debits 0 (early return in `debit_read`), an error path never reaches this function — identical arithmetic to today's `size_hint().exact()` on a sized 200/204.

`product.rs` totals for commit 3: `product_entry` −4, helpers −4, `product_read` 0, `render_product_read` +2 ⇒ −6. Nothing else in `product.rs` moves; no `use` is orphaned (`axum::body::HttpBody` was referenced by full path only).

### 4.3 `src/dst/dst_tests.rs` (265 → 268)

After line 130:

```rust
#[path = "tests/quota_read_volume.rs"]
mod quota_read_volume;
```

### 4.4 Ratchet summary

| Function | File | `#[expect]` list | Edit | Ratchet outcome |
|---|---|---|---|---|
| `product_entry` | product.rs | `unwrap_used` | call tokens of `product_read`×2, `product_scan` change | re-decide reason (text above) |
| `product_entry` | product.rs | `too_many_arguments, too_many_lines, excessive_nesting` | −4 lines, fewer facts | inside `scope_lines`/`syntax_facts`; untouched |
| `product_read` | product.rs | `too_many_arguments, too_many_lines` | +4 facts, 0 lines | re-decide reason |
| `render_product_read` | product.rs | `unwrap_used` | new call `debit_read_bytes`, `payload.len()` | re-decide reason |
| `product_scan` | scan.rs (new path) | `unwrap_used, expect_used` / `too_many_lines` | new file; +1 call (`debit_read_bytes`) | new identities; reasons re-decided anyway |
| `check_read_quota`, `debit_read_bytes` | product.rs | none | — | — |
| `quota_rig` etc. | quota_enforcement.rs | — | untouched | — |

Architecture budgets: `product_entry` 314/318, `product_read` 266/324, `product_scan` 195/200 (a new file's functions cap at 200), `product.rs` 4,277 (source-ratchet ceiling 4,484; architecture file limit 8,216), `scan.rs` ~220/1,000.

## 5. Ledger and doc rows

| File | Change |
|---|---|
| `docs/refactor/architecture-policy.json` (commit 1) | add `"src/product/scan.rs"` to `transport_and_composition_files`; add `transport_rationales["src/product/scan.rs"] = "Product scan page adapter: parses the scan query, pages the frozen cursor and frames the page; ReadService owns the scan."` |
| `docs/quality/owners.json` (commit 2) | new row next to the other `by-path-module` rows (~line 220): `{"category": "by-path-module", "count": 1, "owner": "crate::quota_read_volume", "path": "src/dst/dst_tests.rs", "reason": "Read-byte quota scenarios across the page-serving product routes; real HTTP scan and records pages against one enforce-mode project bucket; compiled and executed with DST.", "syntax": "path = \"tests/quota_read_volume.rs\""}` |
| `docs/refactor/test-inventory.json` (commit 2) | `python3 scripts/test-inventory.py --write` then `--check` (adds `scan_pages_draw_on_the_read_byte_quota`; raises the full-suite floor by one) |
| `src/dst/tests/README.md` | "Multitenancy and authorization" row: `security_*`, `quota_enforcement`, `quota_read_volume` |
| `docs/refactor/WIRE-MATRIX.md` §2.6 (108-112) | handler path → `src/product/scan.rs`; Errors: add `429 project_rate_limit (+retry-after) while the project's read-byte bucket is in debt — checked at entry, before the key`; Metering: add `project read-byte debit of the framed page bytes at the render site (debit_read_bytes)` |
| `docs/refactor/WIRE-MATRIX.md` §2.9 (127) | `post-hoc project read debit (check_read_quota at entry, debit_read_bytes in render_product_read by framed body bytes)` |
| `docs/refactor/WIRE-MATRIX.md` 255, 259 | replace `debit_read_response` with `debit_read_bytes at render_product_read`; "read-byte admission at entry on records GET/long-poll and :scan, framed-page debit at the render sites" |
| `docs/MULTITENANCY.md` ~1418 | "read bytes debited POST-HOC at the page render sites (records GET/long-poll and :scan) by framed body bytes; refusal at entry while in debt; streaming bodies hold a subscription slot instead" |
| `docs/LIVE-FEED.md` | no change (SSE contract untouched) |
| `scripts/quality/mutation_owners.py` | no row: `src/product/*` is not a critical prefix; the planner records `mutants: false` for this diff |
| `docs/quality/source-allowances.json` | nothing stale (no `product_scan` rows exist; `--prune` not needed) |
| `docs/refactor/review-mechanisms.json`, `verification.json`, `scenario-map` | untouched (no pinned fixture, frozen receipt, no scenario ID) |
| `scripts/mt-audit-baseline.txt` | unchanged (`product_scan` carries no fingerprinted site; the audit scans `src/*.rs` top level + `src/dst/**`) |

Local gate before push: `scripts/quality.sh` (clippy `-D warnings`, rustdoc, architecture-gate, test-inventory --check, mt-lint leg), `python3 scripts/quality/verification_plan.py` against the push `before`, then the DST leg (`… --lib quota_read_volume` and `dst_tests::reads_product::` for the scan regressions), `product::`, `quota::`.

## 6. What could go wrong

- **Wire**: `:scan` gains a refusal class it never had (429 `project_rate_limit` + `retry-after`). The SDK's `scan()` iterator (`sdk/src/index.ts:864-882`) surfaces it as a retryable error (`retryable = res.status === 429`, 453); scan cursors are stateless snapshots so re-issuing the same cursor after `retry-after` is safe. No header, body or cursor format changes. Ordering on the scan route: `canonical_name` errors still precede the 429 (the check sits inside the arm, after 1360), matching records where `classify_route` precedes the arm; the 429 precedes `missing_key` on both routes, as today.
- **Fleet skew**: a mixed fleet only differs in whether a cell refuses an in-debt project; pages, cursors and peer relays are unchanged. Peer-relayed pages are debited once, by the coordinator that holds the principal; `/v1/internal/segment-scan` carries no principal and stays undebited (matches "the public coordinator meters once").
- **Semantics**: debit is by framed wire bytes (base64-expanded for `bytes` streams), the same unit `debit_read_response` used; billing (`meter_read`) keeps payload bytes. A client that disconnects before reading is still debited (unchanged posture). The check uses the principal's snapshot quotas (review item 5) — a `read_bytes_per_sec` of 0 still means unlimited.
- **Gate traps**: (a) `product_entry` is at its 318-line architecture budget — the fold is not optional; (b) the moved `product_scan` must not exceed 200 lines in `scan.rs` (195 planned; comments count) — do not put the check inside it and keep new comments to one line; (c) a verbatim move that keeps `tenant`+`name` trips `mt_lint` in the nested file unless the two markers are present until the fix; (d) every re-decided reason must have exactly two `;` and no `"` inside a part; (e) if commit 1 is pushed alone, `architecture-policy.json` must ship with it or the reverse-edge rule fails on `scan.rs`; (f) `unfulfilled_lint_expectations` is deny — do not add a `too_many_arguments` expectation to the 5-param `product_scan`.
- **Flakiness**: none expected — no sleeps, sequential requests, ~260 s of debt vs. millisecond gaps; every `preq` completes because the server closes 200/403/429. The only timing dependence is `retry-after ≥ 1`, which holds for any negative level.
- **Out of scope, observed**: `product_consumer_pull` (1257-1259) also serves record bytes with neither check nor debit — likely the reviewer's step B; not touched here. `product_read`'s `timeout` (2760-2774) and `LongPoll` (2784-2787) parse `waitMs` twice — a later shrink candidate if `product_read` ever needs line room.

## Skeptic corrections

Verdict: **sound with corrections** (none structural). Checked against `slate @ 82095942` read-only. Rather than argue the ratchet claims, I applied the plan's §4 edits to a scratch copy of `src/product.rs` (`scratchpad/skeptic/product_after.rs`, built by `scratchpad/skeptic/build_after.py` from the plan's literal snippets) plus the plan's `scan.rs`, and ran the repo's own `scripts/quality/source_rules.exception_contracts` / `exception_growth` and `scripts/architecture-report.find_functions` / `architecture-gate.reverse_edges` on before vs after (`scratchpad/skeptic/ratchet.py`; uses the prebuilt `target/debug/streams-quality-syntax`, no cargo). Numbers below are measured, not estimated.

### Measured (confirms the plan)

| Claim | Measured |
|---|---|
| `product_entry` architecture budget 318 today, 314 after | `find_functions`: 318 → **314** (limit 318 in `architecture-policy.json`). Without the GET/long-poll fold the scan arm alone gives 330 > 318: the fold is mandatory, exactly as §4.2 says. |
| `product_entry` second `#[expect]` (too_many_arguments/lines/nesting) can stay untouched | contract: `scope_lines` 328 → 324, `nested_items` 1 → 1, `syntax_facts` 494 → 451. No growth. ✓ |
| `product_entry` `unwrap_used` reason must be re-decided | 142 → 138 fingerprints but with **new keys** (the `product_read`/`product_scan` call tokens). Kept unchanged, the ratchet emits 5× `accepted exception grew without a new decision: ('src/product.rs', 'crate::product_entry', 'function', 'expect (clippy :: unwrap_used , …')`. Re-decided reason ⇒ new identity ⇒ skipped. ✓ |
| `product_read` reason must be re-decided | `syntax_facts` 313 → **317** (+4, plan said ~4). Kept unchanged ⇒ 1 growth failure. ✓ |
| `render_product_read` reason must be re-decided | `scope_lines` 49 → 51, `syntax_facts` 97 → 106, fingerprints 42 → 47 with new keys. Kept unchanged ⇒ 9 growth failures. ✓ |
| `product_scan` in `scan.rs` ≤ 200 | `find_functions`: 193 → **195** (new path; `max(200, 0)` budget). Contract scope 204 lines / 281 facts — new path, never compared. ✓ |
| `exception_growth(after, before)` with all five reasons re-decided | **empty**. With the three `product.rs` reasons NOT re-decided: **15 failures**. |
| `product.rs` lines | 4,484 → 4,277 with the plan's literal snippets (see C1: rustfmt makes it ~4,275). Ceiling `min(max(1000, legacy 4920), max(1000, prior 4484))` = 4,484 (`source_rules.violations`). ✓ |
| `scan.rs` reverse edges | `reverse_edges` = `{'http': 6}` (`use crate::http::AppState` + `desc_alive`, `initializing`, `check_key`, `KeyCheck::Ok`, `KeyCheck::Wrong`). A new file's baseline edge count is 0 ⇒ the `transport_and_composition_files` row is **required in the same push as commit 1** (architecture-gate.py:117-120). ✓ |
| No stale allowance rows | `rules.inventory` delta on `product.rs` is only the five reasoned `exception` identities (skipped by `violations`, absent from `source-allowances.json`); `scan.rs` inventories only its two reasoned exceptions. No `--prune` needed. ✓ |
| Callers | `grep` over `src/`: `product_read(` ×3, `product_scan(` ×1, `render_product_read(` ×1, `debit_read_response` ×2, `check_read_quota` ×2 — all inside `product_entry`/`product_read`. Nothing else consumes the changed signatures. `scripts/read-experiments/followup/prepare.py:78` text-patches `fn product_read(` (still matches; not a gate). |
| Mutation leg | `verification_plan.py` `CRITICAL_PREFIXES` do not cover `src/product` / `src/dst`; `mutation_owners.py` has no `src/product.rs` / `src/product/*` / `src/dst/*` row; `tooling` is only `scripts/quality/`, `tools/quality-invariants/`, `fuzz/`, `Cargo.*`, toolchain files. ⇒ `mutants: false`, `properties_fuzz: false`. Do NOT touch `src/quota.rs` or `scripts/quality/*`. ✓ |
| `mt_lint` | `SURFACE_FILES`/`STREAM_REF_FILES` = `["http.rs","product.rs",…]` by `source_relative_path` (`src/mt_lint.rs:101,106,441`), so `product/scan.rs` is NOT exempt; the fix's `sref: TenantStreamRef` param has no `NAME_PARAMS` name and no `.stream_ref(` call ⇒ clean; `tenant.stream_ref(&name)` moves into `product_entry` (ingress, already does it at 1286). ✓ |
| `needless_pass_by_value` on by-value `sref` in an `async fn` that only borrows it | precedent compiles under this exact profile: `src/product/usage.rs:33-48` (`sref` used once as `&sref`). ✓ |
| `usize as u64` under `cast_possible_truncation` | clippy lints usize→fixed only when `to_nbits <= 32`; `record.payload.len() as u64` already sits under an expectation that does not list it (product.rs:3193). ✓ |
| DST file ceiling | `quota_enforcement.rs` = 946; +~85 lines ⇒ 1,031 > 1,000. New module is required. `dst_tests.rs` = **264** (plan says 265; → 267 after the two lines + blank). |
| Test fixture shapes | `auth_rig -> (Arc<AuthService>, Arc<AppState>, SocketAddr)`; `rig_publish_policy(&AuthService, ProjectPolicy, u64)` (`&svc` deref-coerces); `ProjectPolicy.quotas` is `pub`; `mint_token` returns `"Bearer …"` with `RIG_SCOPES` (has `streams.create`, `streams.records.append`, `streams.records.read`); `preq` returns `(u16, HashMap<String,String>, Vec<u8>)` with lower-cased header keys; `engine_shutdown(&Arc<AppState>)`; the wrong key literal exists at `security_audit.rs:60` and `consumer_saga.rs:304`. Only `ownership_version` is enforced against the token (`auth.rs:509,725`), so policy v2 with `ownership_version: 1` keeps the token valid. The test compiles as written. |
| Red on current code | Traced: create 201 → append 200 → wrong-key scan 403 (no check today either) → scan 200 (~2,084 B page; `SCAN_DEFAULT_BYTES` 4 MiB so one page) → second scan **200** on today's tree ⇒ `assert_eq!(st, 429, …)` fails exactly as §3 states. After the fix: bucket `Bucket::full(8)` (created at the create request's `project_admission`, `http.rs:2088`; refill caps at rate so publish order is not even load-bearing) → 403 not debited → 200 debits ~2,084 → level ≈ −2,076 → next scan 429 `project_rate_limit`, `retry-after` = ceil(2076/8) = 260 → records GET 429. Every branch of a wrong implementation (check-only, debit-only, debit-before-key-check, private counter) fails at a distinct assertion, as §3 claims. |

### C1 — rustfmt shapes in §4.2 are not what rustfmt will produce (cosmetic; affects the line arithmetic only)

`chain_width` is 60 (default): `state.quotas.debit_read(&p.project_id, &p.quotas, bytes as u64, crate::shard::now_ms())` is 91 chars, so rustfmt breaks the chain, not the argument list — exactly the shape the current `debit_read_response` (835-840) already has:

```rust
fn debit_read_bytes(
    state: &AppState,
    principal: Option<&crate::auth::RequestPrincipal>,
    bytes: usize,
) {
    let Some(p) = principal else { return };
    state
        .quotas
        .debit_read(&p.project_id, &p.quotas, bytes as u64, crate::shard::now_ms());
}
```

That is 16 lines (plan: 18), so `product.rs` lands at ~4,275 (fewer is fine; the ceiling is what matters). Likewise the `use super::{…}` list in `scan.rs` may pack `q_num,` onto the first line (width lands exactly on 100). Do not hand-count any of the plan's line totals: run `cargo fmt --all` and then `wc -l src/product.rs src/product/scan.rs` (must be ≤ 4,484 and ≤ 1,000) and `python3 scripts/architecture-gate.py --check` (product_entry ≤ 318, product_scan ≤ 200). The scan arm and the folded records arm ARE rustfmt-stable (their argument lists exceed `fn_call_width` 60, so vertical is right).

### C2 — the failure text's line numbers are listing-derived

`quota_read_volume.rs:68:5` / `:54:5` hold only if the file is byte-for-byte the §3 listing after `cargo fmt`. The `preq(addr, "GET", "/v1/streams/sq:scan", &[wrong, a], b"")` one-liners fit `fn_call_width` (51/53 chars of args), so the listing is probably fmt-stable, but capture the panic text from the actual red run at the commit that adds the test and put THAT in the fix commit message; do not copy the plan's line numbers.

### C3 — commit/push shape (gate ordering, not code)

The ratchets compare the pushed HEAD against the push's `before` (`common.py: merge_base`/`verification_comparison`), so a three-commit push is judged once at HEAD. But the repo works directly on `slate` (memory: no PRs), so any commit that is pushed alone must be green alone:
- Commit 1 pushed alone: needs the `architecture-policy.json` row (measured `http: 6` reverse edges) **and** the two `mt-lint: allow(...)` markers (`name-param-shared-core` above the attributes, `stream-ref-construction` directly above the `registry.get` line; `marker()` walks up through the contiguous comment/attribute block, `mt_lint.rs:113-130`). The plan already says both; treat them as mandatory, not optional.
- Commit 2 (red test only) must never be pushed alone: it is red by design. Fold the test into the fix commit (the plan permits this) — the commit message carries the captured failure text as the red-first proof.
- Run before push, in this order: `cargo fmt --all -- --check`, `scripts/quality.sh`, `QUALITY_BEFORE_SHA=82095942 QUALITY_EVENT_NAME=push python3 scripts/quality/verification_plan.py` (expect `mutants: false`), then the DST legs (`quota_read_volume`, `dst_tests::reads_product::` for the scan regressions, `dst_tests::quota_enforcement::volume_quotas_meter_appends_and_reads` — this existing test is what pins that the records path still debits after the render-site move), `product::`, `quota::`, and the `mt_lint` test.

### C4 — the records-path refactor is covered, but say so explicitly in the commit

Switching records GET/long-poll from `debit_read_response(size_hint)` to `debit_read_bytes(payload.len())` is a behaviour-neutral move (`Body::from(Bytes)` reports `exact() == len`; 204 timeout ⇒ 0 ⇒ `debit_read` early-returns; every failure returns before `render_product_read`; the SSE arm never debited). The existing `quota_enforcement.rs:343-351` (`volume_quotas_meter_appends_and_reads`, second records GET → 429) is the regression that would catch a lost debit on that path. No new test is needed for it, but name it in the fix commit as the pin for the moved records debit.

### C5 — wire consequence for the SDK worth recording (not a gate item, not step A)

`sdk/src/index.ts:864-882`: `scan()` calls `req(...)`, which throws (`retryable: true` for 429, line 453) BEFORE the page is yielded, and the iterator never exposes `cursor` to the caller. So after a mid-export 429 the caller can only restart `scan()` from the beginning (or from a cursor it never saw), not resume after `retry-after`. Snapshot cursors are stateless so correctness holds, but "re-issuing the same cursor after retry-after is safe" (§6) is only true for a client that holds the cursor — the shipped SDK does not. Add one line to §6 and a follow-up note (expose the page cursor or retry inside `scan()` on 429/`retry-after`); no server change.

### C6 — small factual fixes to the plan text

- §2 constraint 8 / §4.3: `src/dst/dst_tests.rs` is 264 lines, not 265.
- §4.2 "helpers −4": with the rustfmt shape in C1 it is −6; `product.rs` ≈ 4,275 after commit 3.
- §4.1 says "195 lines" for `product_scan` — measured 195 by `find_functions` (which strips comments but keeps line positions), so the one-line debit comment does count; there is a 5-line margin to the 200 budget. Do not add the quota check inside `product_scan`.
- §5 `owners.json` row shape matches the existing `by-path-module` rows for `src/dst/dst_tests.rs` exactly (`owner: "crate::<mod>"`, `syntax: "path = \"tests/<mod>.rs\""`); the row goes in `owners.json`, not `source-allowances.json` (the latter is a shrinking legacy ledger; `source_gate.check` unions both).
- §5 `test-inventory.py` discovers files by `rglob("src/dst/**/*.rs")` (scripts/test-inventory.py:138), so the new module is picked up by `--write` without any other registration; CI's `tests_ran.py --inventory … --skipped 1` floor rises by one automatically.
- `docs/quality/verification.json` pins `src/product.rs`/`src/dst/dst_tests.rs` sha256s but is a historical adoption receipt ("not a current-HEAD attestation"); leave it alone.
- `scripts/architecture-report.py --baseline-diff` will print `src/product/scan.rs` as a NEW `axum_outside_transport` entry; that report always exits 0 (diagnostic only, `baseline_diff` returns 0). Not a failure; do not add it to any baseline.

### C7 — optional hygiene (not required by any gate)

`budget_exceptions["function:src/product.rs::product_entry"].limit` could be lowered 318 → 314 in the same commit to ratchet the win; the policy file is owner-approved per row, so only do this if the reviewer wants the number pinned. Leaving it at 318 is gate-neutral.

### What I looked for and did NOT find

- No second producer/consumer of `ReadParams.lease`, `AuthLease`, or `RequestPrincipal::lease` is affected (`lease(&self) -> AuthLease`, `auth.rs:253-261`; `ReadParams.lease: Option<AuthLease>`, `http.rs:1777`).
- No existing DST/conformance scan test runs under an enforce rig with a non-zero `read_bytes_per_sec` (`grep read_bytes_per_sec src/dst/tests` hits only `quota_enforcement.rs:205`); `ProjectQuotas` derives `Default` (all 0 = unlimited), so `billing_usage.rs:148`, `read_application.rs:227`, `reads_product.rs`, `security_*` scans are unaffected by the new entry check.
- No `#[expect(clippy::too_many_arguments)]` is needed or allowed on the 5-param `product_scan` / `render_product_read` (threshold 5 ⇒ lint at 6+; `unfulfilled_lint_expectations` is deny).
- `multitenancy-audit.sh` scans `src/*.rs` + `src/config/*.rs` + `src/dst/**` + `src/bin/*.rs`; `product_scan` carries none of its patterns (`stream_hash(`, `registry.get("`, `streams-internal-*`), so `scripts/mt-audit-baseline.txt` is unchanged whether the code lives in `product.rs` or `product/scan.rs`.
- Nothing in the change touches `docs/LIVE-FEED.md` territory (SSE arm only changes how the lease is derived, not the wire).
