# Item 51: raw close-with-final publishes its Sealing intent before the rate limiter admits it

Repo `/Users/sorenschmidt/code/streams`, branch `slate`, HEAD `7c4f8606` (two commits ahead of
`origin/slate` = `9b2f53bc`). Plan written read-only: nothing was built or run. Every quote below
was re-read on this tree. The reviewer's line numbers are stale by about 23 lines in `append.rs`,
so code is cited at its current lines.

**Verdict:** the problem is real, and slightly worse than the review says. The reviewer's change
("hoist only `admit_usage` above `install_intent`") is correct and buildable as one line-neutral
move of a statement. It changes the edge in three ways Søren must approve (§2, D1-D3), plus a
document decision and a scope decision (D4-D5). No verbatim move,
no ceilinged file, no mutation owner and no ratchet re-decision is involved. One commit.

---

## 1. Problem (verified)

### 1.1 The order in `execute_once`

`src/application/append.rs` (423 lines; not ceilinged):

```rust
274:    let mut close_plan = close::prepare_close(state, &desc, command, producer).await?;
275:    let content = content::parse_content(
...
282:    state.creation.renew_ttl(&desc).await.map_err(|error| {
...
289:    })?;
290:    close::install_intent(state, &desc, command, &content, &mut close_plan).await?;
291:    let content::ContentPlan { entries, deferred } = content;
292:    let close_carries_content = !entries.is_empty();
293:    let usage_c = admission::admit_usage(
294:        &state.usage,
295:        &desc,
296:        close_only,
297:        deferred.is_none(),
298:        body.len(),
299:        entries.len(),
300:    )?;
```

`install_intent` (`src/application/append/close.rs:165-217`) runs for any raw close that is not
already sealed, is not resuming an owed final, has no deferred error and has no internal
`seal_auth`. A close that carries a record publishes a durable **Final** intent through a registry
CAS:

```rust
165: /// Publish intent only after deterministic validation; malformed closes leave no debt.
181:    if close && !desc.sealed && !is_owed_final && deferred.is_none() && seal_auth.is_none() {
182:        let intent = if entries.is_empty() {
183:            crate::registry::SealIntent::Empty
184:        } else {
185:            crate::registry::SealIntent::Final {
...
188:                final_committed: false,
...
191:        match crate::application::lifecycle::begin_sealing_for_close(
```

The admission step that follows answers the per-stream token bucket's **transient** 429
(`src/application/append/admission.rs:19-31`):

```rust
19:    let counters = if !close_only && valid_content {
20:        match usage.admit_append(&name_hash, body_bytes as u64, record_count as u64) {
21:            Err(hit) => {
22:                // Every refusal here is transient: parse_content has already
23:                // answered 413 for anything larger than a fresh bucket, so
24:                // the wait this names is one the bucket will honour.
25:                crate::usage::note_limit_refusal(&hit);
26:                return fail(
27:                    FailureClass::Capacity,
28:                    AppendCode::RateLimited(hit.code()),
```

Its `?` at `append.rs:300` returns before `submit::submit` (`append.rs:395`) and before
`close::complete` (`append.rs:397`). `complete_raw_close` (`src/application/lifecycle/raw_close.rs:15-47`)
is the only raw-path owner that releases an uncommitted intent (`abandon_seal_intent`), and it runs
only on a committer verdict. **A close the limiter refuses therefore leaves its Final intent
installed with nothing in flight.** Item 25 (`a8c99c23`) already moved the *permanent* refusal in
front of the intent (`content.rs:101-106`: "it is decided BEFORE the lifecycle intent, or a close
would leave the collection sealing forever"). The transient refusal was left behind it.

### 1.2 What the stranded intent does at the edge

- **Ordinary raw appends get a closed-stream answer.** `prepare_close`
  (`close.rs:114-126`):

  ```rust
  114:    let sealed_reject_new =
  115:        if (desc.sealed || desc.sealing.is_some()) && !close_only && !is_owed_final {
  ...
  124:    if sealed_reject_new.is_some() && producer.is_none() {
  125:        return Err(closed_tail_failure(state, desc).await);
  ```

  `closed_tail_failure` builds `AppendFailure::declared_closed` (`contract.rs:169-175`, which maps
  to `AppendErr::Closed`, then `Conflict` / `stream_closed`). `render_append` adds
  `stream-closed: true` and `stream-next-offset` (`src/http.rs:94-103`). A Durable Streams client
  is therefore told that the stream **is closed**, when in fact its close was refused with 429.
  Producer appends reach the committer with `SealedReject::Sealing` and are also refused.
- **Product appends to the same stream** get 409 `sealed` / "collection is being sealed"
  (`src/product.rs:1984-1991`, `refuse_if_sealed`).
- **Nothing expires the claim.** `SEAL_CLAIM_MS` is read in one place,
  `src/application/lifecycle/claims.rs:95`, inside `decide_claim`. It only lets **another
  seal** take over an abandoned final-bearing claim (`EnterSeal::AbandonedClaim`). No sweeper
  releases it. Ordinary appends stay refused until one of these arrives: the exact retry of the
  throttled close (it resumes the owed final), or any other close or `:seal` after 15 s (it takes
  the claim over and seals without the record). Before 15 s, a different close or seal is refused
  409 `sealed`, with the message "a seal with a final record is in flight; retry that request to
  finish it" (`claims.rs:102-104`). If the throttled client gives up, the collection stays
  open-but-unwritable indefinitely.

### 1.3 The document claim that is false

`docs/RELEASE-PRODUCT-SURFACE.md:651-658` (round-7 record, "The observable contract this settles"):

> It is bounded — after 15 s the claim is abandoned and any subsequent
> seal takes it over — so no single bad request can hold a collection
> open-but-unwritable indefinitely.

The 15 s lapse only makes a takeover *possible*. It does not reopen anything by itself (§1.2).
`:224-225` ("**No indefinite Sealing.** A seal claim is a 15 s fence lease; any retry takes over
through the committer fence.") overclaims in the same way. `docs/append-transitions.md:8-9` still
states the old order ("`close::install_intent` runs only after deterministic validation … 3.
`admission::admit_usage` …"), and so does `docs/refactor/WIRE-MATRIX.md:43` ("… → CT/body
validation → seal intent → enqueue").

### 1.4 Scope of the defect (and what this item does not fix)

- **Affected:** a raw `POST /v1/stream/{name}` with `Stream-Closed: true` and a non-empty valid
  body: not already sealed, not an owed-final retry, and no deferred producer error. On a JSON
  stream an empty array is a 400, so every such close carries a Final intent.
- **Not affected:** a close-only request (`admit_usage` skips admission when `close_only`); a
  deferred-error close (`install_intent` is skipped); product seals (`seal_after = seal_auth.is_some()`
  at `product.rs:2254`, and `install_intent` is skipped under `seal_auth`).
- **Other post-intent, pre-enqueue refusals stay as they are (§8):** `route::resolve_segment`
  503 `segment_transition`, `stream_slot` 429 `stream_overloaded` (`append.rs:304-314`), and
  inside `submit.rs`: resolve 409 `not_ring_owner` / 503 shard-opening, 503
  `maintenance_backpressure`, 429 `engine_backpressure`, 429 `overloaded`, and 408 timeout. All of
  them keep the intent, as the `FinalDisposition` policy says ("timeouts, throttles, write failures
  and ownership moves are the moment", `claims.rs:205-211`). The exact retry resumes the intent. The
  limiter's refusal is different because it needs no route and no engine, so it can simply be
  decided first.

---

## 2. Contract decision

**Typed contract.** No new types. `AppendCode::RateLimited(&'static str)` and `FailureClass::Capacity`
are unchanged, and so are the wire codes: 429 `limit_bytes_per_sec` / `limit_requests_per_sec` /
`limit_records_per_sec` with a computed `retry-after`. The status the throttled request itself gets
does not change. The invariant becomes: *the per-stream limiter decides before a close's seal intent.
A close the limiter refuses publishes nothing.*

Edge-visible changes (raw surface, plus the product view of the same stream). **Each is a decision
for Søren:**

| # | Change at the edge | Backward-compatible alternative |
|---|---|---|
| D1 | After a raw close-with-body is refused 429 `limit_*_per_sec`, the stream stays **open**. Raw appends keep succeeding, where they used to get 409 `stream_closed` + `Stream-Closed: true`. Product appends keep succeeding, where they used to get 409 `sealed`. Another close or `:seal` is no longer refused with "a seal with a final record is in flight". The throttled client's retry becomes a fresh close instead of resuming an owed final. The end result is the same: 204 + `Stream-Closed: true`. Other writers may append between the 429 and the retry, which is correct for a refused (429) request. | Keep the order and fix only the documents: say that a throttled close strands the stream until its exact retry or another seal. **Not recommended**, because the old behaviour tells DS clients a stream is closed after its close was refused. |
| D2 | **Refusal precedence.** A close-with-body can hit both the limiter and a refusing intent CAS: another operation's live final-bearing claim, a pending topology the CAS gave up on, or a registry error (`install_intent` maps every `SealError` to 409 `sealed`, `close.rs:204`). It now answers 429 `limit_*` + `retry-after` instead of 409 `sealed`. After the wait it gets the 409. R2 pins this. | A read-only `desc.sealing` pre-check before admission, so that 409 wins. **Not recommended.** It restates `decide_claim` at a second decision point, and it is still racy against the CAS. |
| D3 | **Accounting.** A close-with-body that the intent CAS then refuses (409 `sealed`) now spends its limiter tokens. It also increments the stream's `requests` / `records` / `bytes_in` counters, which only `/v1/debug/usage` reads (`UsageService::snapshot` has one caller, `http.rs:1095`; billing meters at the committer through `BillingRef`, and `src/billing*` never reads these counters). This matches every other post-admission refusal (`stream_overloaded`, backpressure, committer verdicts). | Refund the tokens on an intent refusal. This needs a new `UsageService` refund API, which is an admission-owner change with its own ≥1,024-case property. **Not recommended.** |
| D4 | **Documents.** Correct the round-7 paragraph (`RELEASE-PRODUCT-SURFACE.md:651-658`) in place, with a dated "corrected by review item 51 (2026-09-23)" note, in the same style item 25 used for its superseded row. Also fix the two consistency lines `:224-225` and `:703`, and `WIRE-MATRIX.md` §1.2 (`:41`, `:43`). | Add a separate erratum section and leave the historical paragraph untouched. |
| D5 | **Scope.** This item leaves the other post-intent pre-enqueue refusals (§1.4) and the product seal-with-final twin (§8) unchanged. | Open a follow-up item: "a close refused before enqueue releases its own generation's intent". That is a `FinalDisposition` policy change and needs its own decision. |

---

## 3. Red tests

Both are DST scenarios through the real HTTP rig. Each rig gets a shared `UsageService` on a
**manual** clock, so the bucket drains and refills only when the test says so (the same pattern
as `admission_maintenance::debug_load_reports_typed_limiter_and_frame_totals` and
`runtime_usage.rs`). The bucket is `limit_bytes_per_sec: 50.0` × the default burst of 2, which is
**100 bytes**. The other limits stay at their defaults: 2,000 requests and 10,000 records, which
these tests never approach. No failpoints, no spawn, no `json!`.

### R1 `dst::dst_tests::seal_recovery::a_throttled_raw_close_leaves_the_collection_open`

File `src/dst/tests/seal_recovery.rs`. Insert it directly after
`a_refused_raw_close_does_not_strand_the_collection`, which is its sibling: "a refused raw close
leaves nothing behind". Import changes: line 4 becomes
`use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig, http_rig_build};`, and a new
line `use super::fixture_runtime::RigRuntime;` goes between `fixture_requests` and `fixture_storage`.

```rust
/// A close the per-stream limiter refuses never reached the committer, so it
/// owes the collection nothing: admission precedes the seal intent and the
/// collection stays open for ordinary appends (review item 51). The usage
/// clock is manual, so the drained bucket refills only when the test says.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_throttled_raw_close_leaves_the_collection_open() {
    let clock = crate::runtime::ManualClock::at(0);
    let usage = std::sync::Arc::new(crate::usage::UsageService::new(
        &crate::config::AdmissionConfig {
            limit_bytes_per_sec: 50.0, // x LIMIT_BURST_SECS 2 = 100 bytes
            ..Default::default()
        },
        std::sync::Arc::new(clock.clone()),
    ));
    let (state, addr) = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            shard: crate::shard::ShardConfig {
                shared_usage: Some(usage),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .parts();
    let ct = [("content-type", "application/json")];
    let close = [("content-type", "application/json"), ("stream-closed", "true")];
    // A JSON body of exactly `n` bytes: `[{"p":"` + (n - 10) filler + `"}]`.
    let body = |n: usize| format!("[{{\"p\":\"{}\"}}]", "x".repeat(n - 10)).into_bytes();
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/limitclose", &ct, b"").await;
    assert!(st == 200 || st == 201);
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/limitclose", &ct, &body(60)).await;
    assert!(st == 200 || st == 204, "60 bytes fit the fresh bucket: {st}");

    // 60 bytes fit a fresh 100-byte bucket (no permanent 413) but not the
    // 40 left: the limiter's transient refusal.
    let (st, headers, b) = hreq(addr, "POST", "/v1/stream/limitclose", &close, &body(60)).await;
    let b = String::from_utf8_lossy(&b).to_string();
    assert_eq!(st, 429, "{b}");
    assert!(b.contains("limit_bytes_per_sec"), "{b}");
    assert_eq!(headers.get("retry-after").map(String::as_str), Some("1"));
    let sref = state.deployment.raw_adapter_sref("limitclose");
    state.registry.invalidate(&sref);
    let d = state.registry.get(&sref).await.unwrap().unwrap();
    assert_eq!(
        (d.sealing.as_ref().map(crate::registry::SealState::owes_final), d.sealed),
        (None, false),
        "a close the limiter refused published its seal intent"
    );

    clock.advance(std::time::Duration::from_secs(2));
    let (st, _, b) = hreq(addr, "POST", "/v1/stream/limitclose", &ct, &body(20)).await;
    assert!(
        st == 200 || st == 204,
        "a throttled close bricked ordinary appends: {st} {}",
        String::from_utf8_lossy(&b)
    );
    // The exact retry, once the bucket holds it, closes the collection.
    let (st, headers, _) = hreq(addr, "POST", "/v1/stream/limitclose", &close, &body(60)).await;
    assert_eq!(st, 204);
    assert_eq!(headers.get("stream-closed").map(String::as_str), Some("true"));
    state.registry.invalidate(&sref);
    let d = state.registry.get(&sref).await.unwrap().unwrap();
    assert!(d.sealed && d.sealing.is_none(), "{:?}", d.sealing);
    engine_shutdown(&state).await;
}
```

The arithmetic, checked against `admit_on` (`usage.rs:197-236`) and `admit_usage`:
- The fresh bucket holds 100.0 bytes. The first append takes 60 and leaves 40.0. The manual clock
  has not moved, so `dt = 0` and nothing refills.
- The close needs 60 against 40. The deficit is 20, so `need_ms = ceil(20/50·1000) = 400`,
  `retry_ms = max(400, 50)`, and `retry-after = 400.div_ceil(1000).max(1) = "1"`.
  `permanently_unadmittable(60, 1)` is `None` because 60 ≤ 100.
- The test then advances the clock 2 s, which refills to `min(40+100, 100) = 100`. The plain
  append takes 20 and leaves 80, and the retried close takes 60.
- The retried close answers **204**: its synthetic producer is filtered from the outcome
  (`append.rs:414`), and `render_append` gives 204 when `producer.is_none()` (`http.rs:44`). It also
  carries `stream-closed: true`. `complete_raw_close` runs `run_seal` before the response, so the
  descriptor is `sealed` with `sealing: None`.

**Expected red on the current tree** (test hunk only). The status, code and `retry-after`
assertions pass on both trees, because the old tree answers the same 429 after it has installed
the intent. The descriptor assertion fails:

```
running 1 test
test dst::dst_tests::seal_recovery::a_throttled_raw_close_leaves_the_collection_open ... FAILED

failures:

---- dst::dst_tests::seal_recovery::a_throttled_raw_close_leaves_the_collection_open stdout ----

thread 'dst::dst_tests::seal_recovery::a_throttled_raw_close_leaves_the_collection_open' panicked at src/dst/tests/seal_recovery.rs:598:5:
assertion `left == right` failed: a close the limiter refused published its seal intent
  left: (Some(true), false)
 right: (None, false)
```

The line `598` assumes the insertion point and formatting above: the old `}` of the sibling moves
to 550, a blank line is 551, and the test starts at 552. The implementer records the actual line
from the red run. If the descriptor assertion were skipped, the next red on the old tree would be
the ordinary append:
`a throttled close bricked ordinary appends: 409 {"error":{"code":"stream_closed","message":"stream is closed"}}`.

### R2 `dst::dst_tests::admission_maintenance::the_limiter_answers_a_close_before_a_standing_seal_claim`

File `src/dst/tests/admission_maintenance.rs`, appended at the end (after
`a_deferred_producer_verdict_outranks_the_capacity_refusal`, currently line 918). It pins D2. Every
import it needs is already there (`Arc`, `http_rig_build`, `HttpRigOptions`, `RigRuntime`, `mem`,
`hreq`). It reuses the file's `records_body` helper: `records_body(8)` is 8×7 + 7 commas + 2
brackets = **65 bytes** with 8 records.

```rust
/// The per-stream limiter decides before a close's seal intent (review
/// item 51), so a close it refuses answers 429 even while another
/// operation's final-bearing claim stands; once the bucket holds it, the
/// same close meets that claim's 409 `sealed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_limiter_answers_a_close_before_a_standing_seal_claim() {
    let clock = crate::runtime::ManualClock::at(0);
    let usage = Arc::new(crate::usage::UsageService::new(
        &crate::config::AdmissionConfig {
            limit_bytes_per_sec: 50.0, // x LIMIT_BURST_SECS 2 = 100 bytes
            ..Default::default()
        },
        Arc::new(clock.clone()),
    ));
    let (_state, addr) = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            shard: crate::shard::ShardConfig {
                shared_usage: Some(usage),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .parts();
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/cap-claim", &ct, b"").await;
    assert!(st == 200 || st == 201);
    // A producer gap is an ordering verdict: its final-bearing claim stays
    // for the exact retry, and its 65 bytes leave 35 in the bucket.
    let gap = [
        ("content-type", "application/json"),
        ("stream-closed", "true"),
        ("producer-id", "p"),
        ("producer-epoch", "1"),
        ("producer-seq", "5"),
    ];
    let (st, _, _) = hreq(addr, "POST", "/v1/stream/cap-claim", &gap, &records_body(8)).await;
    assert_eq!(st, 409, "a producer gap is refused");
    let close = [("content-type", "application/json"), ("stream-closed", "true")];
    let (st, _, b) = hreq(addr, "POST", "/v1/stream/cap-claim", &close, &records_body(8)).await;
    let b = String::from_utf8_lossy(&b).to_string();
    assert_eq!(st, 429, "the limiter decides before the seal-intent CAS: {b}");
    clock.advance(std::time::Duration::from_secs(2));
    let (st, _, b) = hreq(addr, "POST", "/v1/stream/cap-claim", &close, &records_body(8)).await;
    let b = String::from_utf8_lossy(&b).to_string();
    assert_eq!(st, 409, "the standing claim still refuses the close: {b}");
    assert!(b.contains("\"sealed\""), "{b}");
}
```

Why step 1 retains a claim on both trees: this is the same mechanism as the gap leg of
`seal_recovery::a_refused_raw_close_does_not_strand_the_collection` (`:520-547`). The gap
(`ProducerGap`) is `FinalDisposition::AmbiguousOrTransient`, so `complete_raw_close` does not
release, and the answer is 409 `producer_seq_gap`. The second close has no producer headers, so its
`close_identity` differs. `decide_claim` finds a claim that is fresh, not ours, not joinable and owes
a final, and returns `Conflicting("a seal with a final record is in flight; retry that request to
finish it")`. `install_intent` maps that to 409 `sealed` (`close.rs:204`). The claim's `claimed_ms`
comes from the lifecycle clock, not from the test's usage clock, so the claim is never abandoned
inside this test.

**Expected red on the current tree** (test hunk only): the old tree runs the CAS before admission.

```
thread 'dst::dst_tests::admission_maintenance::the_limiter_answers_a_close_before_a_standing_seal_claim' panicked at src/dst/tests/admission_maintenance.rs:964:5:
assertion `left == right` failed: the limiter decides before the seal-intent CAS: {"error":{"code":"sealed","message":"a seal with a final record is in flight; retry that request to finish it"}}
  left: 409
 right: 429
```

The line `964` assumes a blank line after line 918 and the formatting above. The implementer
records the actual line.

**If Søren picks D2's alternative**, R2's second assertion becomes `409` + `"sealed"` (a green guard
of the pre-check). Its doc comment then states the opposite precedence.

---

## 4. Edits (one commit, in this order inside the working tree)

No verbatim-move commit is needed, and no ceilinged file is touched. Current sizes of the
ceilinged files, none of which is edited: `http.rs` 3,371/3,371, `product.rs` 4,205/4,205,
`shard.rs` 3,197 (ceiling 3,232), `billing.rs` 2,201/2,201, `history.rs` 1,713/1,713, `auth.rs`
1,676/1,676, `registry.rs` 1,501 (ceiling 1,509), `sse/feed.rs` 1,200/1,200, `fleet.rs` 1,143/1,143.

Step 0 (red, never committed): apply only the R1 hunk (with its two import lines) and the R2 hunk.
Run §7 C1 and record both outputs.

### 4.1 `src/application/append.rs`: 423 → 423 (line-neutral)

```diff
     state.creation.renew_ttl(&desc).await.map_err(|error| {
         AppendFailure::new(
             FailureClass::Unavailable,
             AppendCode::TtlRenewal,
             error.to_string(),
         )
         .retry(1)
     })?;
-    close::install_intent(state, &desc, command, &content, &mut close_plan).await?;
-    let content::ContentPlan { entries, deferred } = content;
-    let close_carries_content = !entries.is_empty();
     let usage_c = admission::admit_usage(
         &state.usage,
         &desc,
         close_only,
-        deferred.is_none(),
+        content.deferred.is_none(),
         body.len(),
-        entries.len(),
+        content.entries.len(),
     )?;
+    close::install_intent(state, &desc, command, &content, &mut close_plan).await?;
+    let content::ContentPlan { entries, deferred } = content;
+    let close_carries_content = !entries.is_empty();
     let routing_key = command.routing_key.clone();
```

This is the reviewer's change exactly: `admit_usage` moves; everything else keeps its order,
including `renew_ttl` before admission, so TTL behaviour for throttled appends is unchanged. It
builds because `admit_usage` reads only a `bool` and a `usize` from `content`, and the destructure
still follows `install_intent`, which borrows `&content`.

**Ratcheted function touched: `execute_once`** (`#[expect(clippy::too_many_lines, reason = "execute_once; …")]`,
`append.rs:242-245`). This is not an unwrap/expect exception, so only `scope_lines`,
`nested_items` and `syntax_facts` are ratcheted (`scripts/quality/source_rules.py:150-158`):
- `scope_lines` is unchanged, because statements move inside the function and no line is added.
- `nested_items` is unchanged.
- `syntax_facts` is unchanged. The `quality-syntax` scanner records `path`, `method-call` and
  `call-site` facts but no field accesses (`tools/quality-syntax/src/scan.rs:222-255`). So the
  path `deferred` is replaced one-for-one by the path `content` in `content.deferred`, and the same
  holds for `entries`. The `is_none` / `len` method calls are the same.
- The expectation stays fulfilled, since the function is still over 100 lines. Its reason ("validates,
  admits, commits and settles in the order the retry contract fixes") is still true. No comment
  line is added inside `execute_once`, because any added line would grow `scope_lines`. The
  invariant is documented on `install_intent` instead (4.2).

`admit_usage` (`#[expect(fn_params_excessive_bools, too_many_arguments)]`) is not edited, only its
call site moves.

### 4.2 `src/application/append/close.rs`: 253 → 254

The doc comment on `install_intent` (line 165) currently states only the deterministic half. It
becomes:

```rust
/// Publish intent only after deterministic validation and the per-stream limiter: a close
/// refused by either never reached the committer, so it leaves no debt.
```

`install_intent` has no `#[expect]`. `closed_tail_failure` (line 137, function-wide `unwrap_used`)
sits above the edit and its content is untouched. Its fingerprints are keyed by qualified name and
value, not line, so nothing changes. `complete` (`too_many_arguments`) shifts down one line and
its scope is unchanged.

### 4.3 `src/dst/tests/seal_recovery.rs`: 896 → 966

R1 plus two import edits (+1 line). It stays under the 1,000-line crossing ban. The file's
`#[expect]`s are all `too_many_lines` / `disallowed_methods` on other tests, so the new imports
move no unwrap/expect fingerprint.

### 4.4 `src/dst/tests/admission_maintenance.rs`: 918 → 970

R2 appended. No import change. The file's only `#[expect]` is at `:604` (`disallowed_methods`,
in another test).

### 4.5 Documents

- `docs/RELEASE-PRODUCT-SURFACE.md:651-658`: replace the paragraph with:

  > A close that carries content and is refused for **ordering** leaves
  > the collection `Sealing`, and ordinary appends are refused with 409
  > `stream_closed` while that claim stands. This is deliberate: the
  > collection is sealing, and the exact retry must still be able to
  > deliver the promised record. The claim does not lapse by itself:
  > after 15 s it is abandoned, which lets any subsequent seal take it
  > over, so the collection stays open-but-unwritable until the exact
  > retry or another seal arrives. A close refused for a reason that
  > retrying cannot change releases the intent immediately. *Corrected by
  > review item 51 (2026-09-23):* the per-stream rate limiter decides
  > before the intent, so a close it refuses (429 `limit_*_per_sec`)
  > leaves the collection open. A refusal after the intent (an engine or
  > maintenance shed, a moving shard, a timeout) keeps it, like an
  > ordering verdict.

  Consistency edits (D4): at `:224-225`, change "**No indefinite Sealing.** A seal claim is a 15 s
  fence lease; any retry takes over through the committer fence." to "**No unrecoverable
  Sealing.** A seal claim is a 15 s fence lease; the exact retry resumes it and any later seal
  takes it over through the committer fence." At `:703`, change "recovers by itself:" to "recovers
  without an operator:". Both are line-neutral where possible; Markdown has no ceiling.
- `docs/append-transitions.md:8-9`, steps 2 and 3:

  > 2. `content::parse_content` validates media type, entries and permanent record/ingest ceilings. Producer errors remain deferred until duplicate detection.
  > 3. `admission::admit_usage` charges the per-stream limiter and resolves one shared counters object. `close::install_intent` runs only after deterministic validation and this admission, so neither an impossible final record nor a throttled close can strand a collection in Sealing; a refusal after the intent keeps it for the exact retry. `route::resolve_segment` refreshes topology only under the already-authorized project and epoch. The per-segment admission slot stays alive through submission and lifecycle completion.

- `docs/refactor/WIRE-MATRIX.md` §1.2:
  - `:41`: the limiter parenthetical gains "…; `src/usage.rs` `admit_on`; decided before a close's
    seal intent, so a close it refuses leaves the stream open".
  - `:43`: change "… → CT/body validation → seal intent → enqueue." to "… → CT/body validation →
    per-stream limiter (429 `limit_*_per_sec`) → seal intent → enqueue."

### 4.6 `docs/refactor/test-inventory.json`

Run `python3 scripts/test-inventory.py --write`. The diff must be exactly two added entries (R1 in
`seal_recovery.rs`, R2 in `admission_maintenance.rs`) and nothing else.

### 4.7 Commit message (draft)

```
A close the rate limiter refuses leaves the collection open: admission precedes the seal intent

A raw close carrying a record published its durable Final intent
(install_intent) BEFORE admit_usage charged the per-stream bucket, and the
limiter's 429 returned before the committer, so complete_raw_close never
ran and nothing released the intent. The collection stayed Sealing: raw
appends got 409 stream_closed with Stream-Closed: true for a stream whose
close had been REFUSED, product appends 409 sealed, other seals 409 until
the 15 s lapse let one take the claim over. Nothing lapses by itself, so a
client that gave up left the collection open-but-unwritable indefinitely
(review item 51; item 25 had already moved the permanent 413 in front).

admit_usage now runs immediately before install_intent; nothing else
moves (TTL renewal still precedes both). A close the limiter refuses
publishes nothing. Consequences, approved by Søren: [D1..D3 outcome]. A
refusal AFTER the intent (stream slot, engine/maintenance shed, a moving
shard, a timeout) keeps it for the exact retry, unchanged.

Red first (both on the previous tree):
  a close the limiter refused published its seal intent
  left: (Some(true), false)   right: (None, false)        (dst seal_recovery)
  the limiter decides before the seal-intent CAS: {"error":{"code":"sealed",...}}
  left: 409   right: 429                                   (dst admission_maintenance)
Green: both, plus the throttled close's retry sealing with 204 +
Stream-Closed after a refill, and the 1,024-case admission property
unchanged.

execute_once is line- and fact-neutral under its too_many_lines
expectation. The round-7 contract paragraph of RELEASE-PRODUCT-SURFACE,
append-transitions and WIRE-MATRIX §1.2 are corrected. seal_recovery.rs
896 -> 966; admission_maintenance.rs 918 -> 970.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
```

---

## 5. Mutation-kill analysis

- **No new guard, boundary or predicate.** The only executable change is the order of two
  statements in `execute_once`. The argument expressions `content.deferred.is_none()` and
  `content.entries.len()` compute the same values as before. cargo-mutants 27.1.0 generates no
  operator or `!` mutants for them, since there is no binary or unary operator in the diff. The
  only variant that would overlap the diff is a whole-body replacement of `execute_once`. R1 and R2
  both kill it, and the whole DST suite kills it too.
- **Selection:** `src/application/append.rs`, `src/application/append/close.rs` and the two
  `src/dst/tests/*.rs` files are neither under `CRITICAL_PREFIXES`
  (`scripts/quality/verification_plan.py:21-31`; only `src/application/read_` is critical in
  `src/application`) nor registered in `scripts/quality/mutation_owners.py`. The planner therefore
  selects **no** mutation owners for this commit (`mutants: false`,
  `mutation_source_files: []`, `unregistered_mutation_source_files: []`). **No owner rows or filters
  change.** Neither DST module is named in any owner filter, so no filter needs widening.
- **The regression R1/R2 exist to kill** is "`install_intent` moved back in front of
  `admit_usage`". R1 fails on it with the §3 R1 message, and R2 with the §3 R2 message.
- **Loom / held-commit (item 48):** not a synchronization change. The two steps are sequential
  awaits in one request task. The limiter's lock (`admit_append_in`) and the registry CAS are
  untouched and are never held together. No Loom model is selected (`loom: false`, since no
  `LIFECYCLE_PREFIXES` file is touched).
- **Property gate ("codec, index or admission changes", RUST-QUALITY.md):** the admission predicate
  is untouched. `usage.rs`, `admission.rs` and `content.rs` are not edited. The affected property,
  `usage::runtime_tests::permanent_refusal_is_exactly_fresh_bucket_refusal` (1,024 cases, from item
  25), is kept unchanged and run explicitly (C4), because the planner does not select
  `properties_fuzz` for this diff (no `src/quota` or codec prefix).

---

## 6. Ledgers (all in the same commit)

| Ledger | Change |
|---|---|
| `docs/refactor/test-inventory.json` | two added entries (R1, R2) via `--write`; no other diff |
| `docs/quality/owners.json` | **none**: no `tokio::spawn`, `select!`, `json!`, `proptest`, glob import, `#[path]` module or static |
| `docs/refactor/architecture-policy.json` | **none**: no new file |
| `docs/refactor/WIRE-MATRIX.md` | §1.2 `:41`, `:43` (4.5) |
| `docs/refactor/review-mechanisms.json` / `test-additions.json` | **none**: neither DST file has a pinned test (grep: 0 hits) |
| `src/dst/tests/README.md` | **none**: no new module |
| `scripts/quality/mutation_owners.py` | **none** (§5) |
| `docs/quality/source-allowances.json` / legacy baselines | **none**: no exception added, removed or grown |
| `docs/RELEASE-PRODUCT-SURFACE.md`, `docs/append-transitions.md` | 4.5 |

mt_lint: R1 and R2 take no `name: String` parameter and call no `.stream_ref(`. They use
`raw_adapter_sref`, as the neighbouring tests do. There are no `_ =>` arms and no string-prefix
verdicts.

---

## 7. Controls

Preflight: `export PATH=/private/tmp/claude-501/-Users-sorenschmidt-code-streams/4580d18c-ad9e-4e38-8ca1-89558fd0d592/scratchpad/pybin:$PATH`
(the gate needs Python 3.11 or later).

- **C1: red** (test hunks only, production untouched):
  `cargo test --locked --lib -- --exact dst::dst_tests::seal_recovery::a_throttled_raw_close_leaves_the_collection_open dst::dst_tests::admission_maintenance::the_limiter_answers_a_close_before_a_standing_seal_claim`.
  Expected: both `FAILED` with the §3 messages, then
  `test result: FAILED. 0 passed; 2 failed; 0 ignored; 0 measured; <N> filtered out`.
- **C2: green** (full edit), through the leg that proves it ran:
  `scripts/test-leg.sh target/quality/item51.log --exact dst::dst_tests::seal_recovery::a_throttled_raw_close_leaves_the_collection_open --exact dst::dst_tests::admission_maintenance::the_limiter_answers_a_close_before_a_standing_seal_claim -- --locked --lib -- --exact dst::dst_tests::seal_recovery::a_throttled_raw_close_leaves_the_collection_open dst::dst_tests::admission_maintenance::the_limiter_answers_a_close_before_a_standing_seal_claim`.
  Expected: `test result: ok. 2 passed; 0 failed`, and `tests_ran.py` exits 0.
- **C3: neighbours** that must not move:
  `cargo test --locked --lib -- dst::dst_tests::seal_ dst::dst_tests::admission_maintenance:: dst::dst_tests::runtime_usage:: dst::dst_tests::append_application:: dst::dst_tests::durability_fences:: dst::dst_tests::durability_failures:: dst::dst_tests::security_noninterference:: dst::dst_tests::producer_protocol:: dst::dst_tests::topology_lifecycle:: dst::dst_tests::lifecycle_incarnation:: dst::dst_tests::reads_raw:: dst::dst_tests::product_lifecycle:: usage::`.
  Expected: `0 failed`. This covers the four users of the `stop_after_seal_intent` failpoint
  (`durability_fences`, `security_noninterference`, `seal_fencing`, `seal_coordination`), which now
  also admit before stopping, on default limits.
- **C4: the 1,024-case property, unchanged:**
  `cargo test --locked --lib -- --exact usage::runtime_tests::permanent_refusal_is_exactly_fresh_bucket_refusal`.
  Expected: `test result: ok. 1 passed`.
- **C5: static gates:** `cargo fmt --all -- --check` (clean);
  `cargo clippy --locked --workspace --all-targets -- -D warnings` (0 warnings; the `execute_once`
  expectation is still fulfilled);
  `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items`
  (clean); `python3 scripts/test-inventory.py --check` (ok after `--write`); then
  `scripts/quality.sh` (all legs green, including the source gate: no "accepted exception grew",
  and no file-growth failure).
- **C6: CI's own selection, after committing:**
  `cargo build --locked -p streams-quality-syntax && QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse HEAD~1) QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan && python3 -c "import json;p=json.load(open('target/quality-plan/plan.json'));print(p['mutants'],p['loom'],p['miri'],p['properties_fuzz'],p['mutation_source_files'],p['unregistered_mutation_source_files'])"`.
  Expected: `False False False False [] []`. The real push compares against `origin/slate`, and
  also carries `517f4fa1` and `7c4f8606` if they are still unpushed. Rerun C6 with
  `QUALITY_BEFORE_SHA=$(git rev-parse origin/slate)`. If that selects mutants (from those commits'
  critical files), run `scripts/quality/mutations.sh` before pushing.
- **C7: full suite as CI runs it:**
  `cargo test --release -- --skip post_split_throughput_scales 2>&1 | tee target/quality/suite-item51.log; python3 scripts/quality/tests_ran.py target/quality/suite-item51.log --inventory docs/refactor/test-inventory.json --skipped 1`.
  Expected: 0 failed, and the floor is met (inventory + 2).
- **C8: protocol conformance** (raw close is protocol-visible), per `CONFORMANCE.md:20-31`.
  Expected: `332 passed · 0 failed · 6 skipped (338)`, and `check.mjs` ok.
- **After push:** `gh run list --branch slate --json headSha,createdAt,status,conclusion,name` for
  the pushed sha. Never claim green from memory.

---

## 8. Out of scope

- **Other post-intent, pre-enqueue refusals** (§1.4): 429 `stream_overloaded` / `engine_backpressure`
  / `overloaded`, 503 `maintenance_backpressure` / `segment_transition` / shard-opening,
  409 `not_ring_owner`, and 408 timeout. They still keep the intent, and the exact retry resumes it.
  Moving them in front of the intent needs the routed segment and engine before the claim.
  Releasing the intent on them is a `FinalDisposition` policy change (D5 follow-up option).
- **The product seal-with-final twin:** `seal_final` claims first (`enter_sealing`) and then appends
  the final through `execute_once` under `seal_auth`. A 429 from that append is
  `AmbiguousOrTransient` (`product.rs:1797-1801`), so the claim is kept. This is the documented
  "throttles" retention, and fixing it needs admission outside the append service (for example a
  reserve API).
- **`renew_ttl` before admission:** a throttled append still renews TTL. This is pre-existing and
  deliberately untouched ("keep everything else").
- **F-C** (scratchpad `followups.md`): retried append attempts re-debit the bucket. This is
  pre-existing and unchanged by the reorder, since there is still one admission per attempt.
- **RUNBOOK.md §3.2** says a "billing emitter appends … DELTA requests/records/bytes_in" from these
  counters. The only reader of `UsageService::snapshot` is `/v1/debug/usage`, so the sentence looks
  stale. It is not edited here (noted for a docs pass).

---

## Skeptic corrections (C1..C6)

Re-verified read-only on HEAD `7c4f8606`. **Confirmed as written:** `append.rs` 423 lines, with
`install_intent` at :290, the destructure at :291-292 and `admit_usage` at :293-300. `close.rs` is 253
lines (`install_intent` :165-217, CAS→409 `sealed` at :204). `admission.rs` :19-31. `content.rs`
:101-106. Every ceilinged `wc -l` matches §4 (shard.rs 3,197 and registry.rs 1,501 are under their
ceilings; the rest are at them). `seal_recovery.rs` is 896 lines, with the sibling's closing `}` at :549,
so R1's `assert_eq!` does land on :598. `admission_maintenance.rs` is 918, so R2's assert lands on :964. Both
red traces hold. Old tree R1: the Final intent is installed, then `admit_on` refuses 60 vs 40 bytes
(retry_ms 400 → `retry-after: 1`), and nothing releases the intent (`complete_raw_close` is never
reached) → `(Some(true), false)`. Old tree R2: `decide_claim` reaches `Conflicting` (claims.rs:102-104)
before admission → 409 with exactly that message. The fixed tree gives 429, then 409 `sealed` after
the refill. The commit_plan.rs:131-135 new producer seq 5 is `ProducerGap` → 409. All test APIs exist:
`ManualClock` is `Clone` (runtime.rs:282), `AdmissionConfig: Default` (config/model.rs:491),
`SealState::owes_final` is `pub(crate)` (registry.rs:250), and the import order matches
runtime_journals.rs:2. The syntax-fact claim holds: `syntax_facts` is `len(scoped_facts)`
(source_rules.py:150-155), and `content.deferred` yields one `path` fact (`content`), the same as
`deferred`, because scan.rs:222 emits no field-access fact. `execute_once` has only a
`too_many_lines` expectation, so no unwrap/expect fingerprint applies. `src/application/append*` is
not critical and has no owner row (verification_plan.py:21-31, mutation_owners.py), so no mutants are
selected. `usage.admit_append` has exactly one caller (admission.rs:20). `UsageService::snapshot` has
one reader (http.rs:1095). There is no sweeper: `SEAL_CLAIM_MS` is read only at claims.rs:95.

- **C1 — The invariant overclaims ("A close the limiter refuses publishes nothing", §2, the commit
  message, §4.5 append-transitions text).** An *owed-final exact retry* still writes durably before
  admission. `prepare_close` (close.rs:74-99) calls `renew_owed_claim` (lifecycle.rs:449-464), which
  is a registry CAS that bumps `claim_generation` and refreshes `claimed_ms`. It runs at append.rs:274,
  ahead of both `parse_content` and `admit_usage`, on both trees. A throttled exact retry therefore
  re-arms the 15 s lease on every attempt. While the owner keeps getting 429, the other seals stay
  fenced out. Admission cannot simply be hoisted above it, because `admit_usage` needs `entries.len()`
  and `deferred`, and `parse_content` needs `close_plan.producer`. Fix: state the invariant as "a
  close the limiter refuses publishes no *new* seal intent; an owed-final retry's lease renewal
  (close.rs:74-99) still precedes admission". Add that sentence to the §4.5 RELEASE paragraph and to
  append-transitions step 3. List it under §8 / D5 as a known remaining pre-admission write. It
  is not a regression.
- **C2 — A scenario ledger is missed: SEL-001 already claims this coverage.** `docs/dst/SCENARIO-CATALOG.md:190-194`
  (SEL-001, "Deterministic validation precedes intent") names "byte/record/**request capacity**; no
  Sealing state may be published". `docs/refactor/test-scenario-map.json` (entry SEL-001) marks it
  `"coverage": "full"` with three tests, and none of them covers the transient limiter. That
  full-coverage claim is exactly what this defect falsifies. Add
  `{"name": "a_throttled_raw_close_leaves_the_collection_open", "file": "src/dst/tests/seal_recovery.rs", "line": 552}`
  to SEL-001's `tests`. `scenario-map-report.py --check` validates only the symbol's existence
  (:209-221), and the counts in `SCENARIO-MAP.md` do not change. Consequence for §4.6: after `--write`,
  R1's inventory entry carries `"scenarios": ["SEL-001"]` (test-inventory.py:133-143), not `[]`. Add
  this to §6's ledger table. Optionally amend `docs/dst/DST-EXPANSION-SPEC.md:581` (L1) to read "…and
  the per-stream limiter completes before…", since the limiter is not deterministic validation.
- **C3 — D3 is a behaviour change with no red/pinning test.** The red-first rule applies. In the new
  order, a close that the intent CAS then refuses has already spent its tokens, and nothing pins this.
  Append a step 4 to R2, after the 409 `sealed` assertion. Step 3 left 35 bytes (100 − 65), so a
  second close with `records_body(5)` (41 bytes) must answer 429 `limit_bytes_per_sec`. Under D3's refund
  alternative it would be admitted and meet the claim's 409 instead. A plain append cannot probe this,
  because `prepare_close` refuses it 409 `stream_closed` before admission while the step-1 claim
  stands. About +5 lines, so `admission_maintenance.rs` 918 → about 975 (still < 1,000). R2's red
  line :964 is unchanged. If Søren picks D3's alternative, this step flips to `409` + `"sealed"`.
- **C4 — The `stop_after_seal_intent` user count is wrong (§7 C3).** It has five users, not four.
  `src/dst/tests/lifecycle_incarnation.rs` also arms it (grep). C3's filter already includes
  `dst::dst_tests::lifecycle_incarnation::`, so the control is still complete. Fix only the prose.
- **C5 — D1's "product appends to the same stream" holds only when the product project is the
  deployment tenant.** Raw requests address `deployment.raw_adapter_sref(name)` (deployment.rs:58-64).
  Product requests address `TenantStreamRef::new(project, cn)` (product.rs:3228). Qualify the D1 row
  and the commit message: "(when the product surface resolves to the same tenant stream)". R1
  exercises only the raw surface, and that is sufficient for the fix.
- **C6 — R1 has no counterpart if Søren picks D1's alternative.** §3 gives R2's flipped form but
  not R1's. Under the docs-only alternative there is no code change. R1 must then be dropped, or
  inverted into a green guard that asserts `Some(true)`, the stranded state, together with the 409
  `stream_closed` on the plain append. State this so that the red-first ledger stays consistent with
  whichever option is chosen.

Nothing else was found missing. owners.json needs no row (`format!` only, no `json!`, `select!`,
spawn or glob). architecture-policy, the README module table, review-mechanisms (0 hits for either
file) and the source-allowances need no change. No Loom model is needed. The controls C1-C8 are buildable as
written: `test-leg.sh` takes `<log> [--exact …] -- <cargo args>`; `verification_plan.py --out` reads
`QUALITY_*` env (common.py:61-83), and the plan keys it prints exist (mutation_owners.py:37-43);
`tests_ran.py` takes `--inventory` and `--skipped`; the CI suite command matches ci.yml:98/106.

**Verdict: ready-with-corrections.** The one-statement reorder is correct, line- and fact-neutral,
and both red tests fail today with the stated messages. Before implementing, apply C1 (scope the
invariant and the doc text), C2 (SEL-001 mapping plus the changed inventory expectation) and C3 (pin
D3 in R2). C4-C6 are fixes to the prose and the decision table.
