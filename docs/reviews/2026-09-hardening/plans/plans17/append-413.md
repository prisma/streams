# External review §5: a permanent 413 on the append surfaces (stable code, its limit, no side effects before it)

Base: `slate` @ `24c4c77a`, which equals `origin/slate` and has a clean tree. I planned this read-only and ran nothing.

## Summary

The problem is real and reachable at the defaults. The fix is three commits, and the first two carry the adopted item:

- **C1: one typed refusal with a stable code.** The per-stream capacity refusal becomes one typed value that carries its limit (`CapacityRefusal`).
  - The product renders it as `payload_too_large` with `details {dimension, capacity, requested}`. Today product append and batch say `body_too_large`, which is the transport ceiling's code.
  - The raw message names the numbers.
- **C2: no quota debit before the 413.** The product handler's body contract moves out of `product_append_inner` into `src/product/append_body.rs`. There the capacity 413 is decided before the §17.2 volume debit.
- **C3: no lifecycle write before the 413.** A raw close renews an owed final's claim only after the content owner has validated the record.

Nothing edits an exception's reason text. Every ratcheted scope I touch either shrinks or loses its exception entirely:
- `render_product_append_error` loses its `unwrap_used` exception because its only unwrap becomes the infallible `HeaderValue::from(u64)`.
- `prepare_close` loses its `too_many_lines` exception: it falls to 84 code lines.

One new narrow exception is added: the f64→u64 capacity cast in `CapacityRefusal::new`.

Mutation testing:
- No changed production file is under a critical prefix or a registered owner, so CI's mutation leg selects nothing. §5 still lists every mutant and what kills it.
- `src/http.rs` is not touched. That is deliberate: raw machine-readable `details` would need `render_append`'s function-wide unwrap exception restructured. That choice is decision D1.

---

## 1 Problem (verified on 24c4c77a)

### 1.1 The product handler charges the project's volume quota before the per-stream 413

The order in `product_append_inner` (`src/product.rs:2024-2236`) is:

1. the shape is parsed into `(wire_body, count)` (2126-2189);
2. §17.2 volume admission (2190-2204):

   ```rust
   if let Some(p) = principal
       && let Err(refusal) = state.quotas.admit_append(
           &p.project_id, &p.quotas, body.len() as u64, count as u64, crate::shard::now_ms(),
       )
   { return crate::audit::tag_project(quota_refusal_response(&refusal), &p.project_id); }
   ```
3. then the memory gate (2208), the key parse (2213), `submit_product_append` (2222), `prepare`, the producer parse, `check_memory` and `execute_prepared`;
4. only then `execute_once` (`src/application/append.rs:274-294`) runs `prepare_close` → `parse_content` → `renew_ttl` → `install_intent` → `admit_usage`.

The 413 is decided at the end of `parse_content` (`src/application/append/content.rs:105-116`):

```rust
if deferred.is_none()
    && let Some(kind) = usage.permanently_unadmittable(body.len() as u64, entries.len() as u64)
{
    return fail(FailureClass::Invalid, AppendCode::PayloadTooLarge,
        &format!("request exceeds the per-stream ingest {kind} capacity"));
}
```

`QuotaRegistry::admit_append` (`src/quota.rs:634-703`) has two relevant branches:
- `if full && cost > rate { *slot = Some((rate, cost)); return Ok(()); }` (672-677). An oversized request against a full bucket is admitted, and then `bytes_b.level -= cost;` (699) drives the bucket into debt.
- `if b.level < cost { return Err(QuotaRefusal::Rate { retry_after_secs: ... }) }` (678-682). The same request against a bucket that is not full gets 429.

`quota_refusal_response` (`src/product.rs:856-887`) renders `Rate` as 429 `project_rate_limit` with `retryable: true` and `retry-after`. The reviewer wrote `rate_limited`; that is the translation of the *per-stream* limiter, not of the project quota.

This gives two failure modes:
- **Full bucket.** The request is admitted into debt and then refused 413 by the core. The next valid append is 429 `project_rate_limit` until the debt clears.
- **Bucket not full.** A permanently unadmittable request gets 429 plus `Retry-After`, a permanent refusal answered as transient.

It is reachable at the defaults:
- The per-stream capacity is `limit_bytes_per_sec: 5_000_000.0` × `limit_burst_secs: 2.0` = 10,000,000 bytes (`src/config/model.rs:512-515`).
- That is below the 32 MiB body ceiling, `max_request_body_bytes: 32 * 1024 * 1024` (`src/config/cli.rs:572`).
- Any project with `append_bytes_per_sec > 0` is exposed.

The DST rigs use these defaults: `fixture_config` → `ServerConfig::load(CliArgs::deterministic())`, and `fixture_http.rs:424` builds `UsageService` from `rig_config.admission`.

### 1.2 The code differs by surface, and no refusal carries its limit

| site | status / code | message | details |
|---|---|---|---|
| raw append (`content.rs:110-114` → `contract.rs:83` → `http.rs:6-28`, `render_append` → `err_resp` `http.rs:1447`) | 413 `payload_too_large` | `request exceeds the per-stream ingest {kind} capacity` | none (the raw body is `{"error":{"code","message"}}`) |
| product append and batch (`render_product_append_error`, `product.rs:2413-2418`: `F::Invalid if status == StatusCode::PAYLOAD_TOO_LARGE => ("body_too_large", "request body exceeds the limit", None, false)`) | 413 `body_too_large` | generic | none |
| product seal final pre-check (`product.rs:1743-1755`) | 413 `payload_too_large` | `the final record exceeds the per-stream ingest {kind} capacity` | none |
| product transport body ceiling (`http.rs:1916-1926`) | 413 `body_too_large` | `request body exceeds the limit` | none |

So `body_too_large` currently means two different refusals on the product surface. `permanently_unadmittable` (`src/usage.rs:321-340`) returns only `Option<&'static str>`. The capacity is never computed for a caller.

### 1.3 Raw close: a lifecycle write before the 413 (narrow)

`prepare_close` (`src/application/append/close.rs:19-139`), lines 73-99:

```rust
let mut raw_seal_gen: Option<u64> = seal_auth.as_ref().map(|a| a.generation);
if is_owed_final && seal_auth.is_none() {
    match crate::application::lifecycle::renew_owed_claim(&state.lifecycle, &desc.sref(), &this_close_op, &desc.stream_epoch).await {
        Ok(Some(g)) => raw_seal_gen = Some(g), ...
```

`renew_owed_claim` (`lifecycle.rs:449-481`) is a registry write:

```rust
next.seal_gen_counter += 1;
claim.claim_generation = generation;
claim.claimed_ms = ...
```

It runs before `parse_content` (`append.rs:274-281`).

A raw close with a body always carries a producer. It is either the caller's or the synthetic one (`close.rs:101`: `close && !body.is_empty() && producer.is_none()`). As a result, every content refusal *other than* capacity is deferred to the shard, and the renewal is legitimately needed for duplicate recognition.

The capacity 413 is immediate. So the edge is: an owed final published under a larger `LIMIT_*` is retried after a restart with a smaller one. The retry renews the claim, then gets a 413.

Separately, and not caused by this ordering, such a seal stays in Sealing until the limit is restored. See §8.

### 1.4 Things that do not violate "no side effects before the refusal" (checked)

- **`charge_queued`** (`product.rs:2050-2062`). This is an RAII guard released on every return. The pressure charge in body buffering (`http.rs:1913`) is also released.
- **§17.3 `admit`.** It takes one request-rate token for every authenticated request, by design, whatever the outcome.
- **`meter_op_if_ok`.** It meters only successes.
- **The product seal pre-check** (`product.rs:1743`). It runs before `seal_final`, so no intent is written.

### 1.5 Every use site (git grep over the whole repo)

- **`permanently_unadmittable`:**
  - definition `usage.rs:327`;
  - `content.rs:108`, `product.rs:1746`;
  - tests `usage/runtime_tests.rs:26,27,109,113,114,116,170`;
  - docs `RELEASE-PRODUCT-SURFACE.md:520` (historical);
  - the frozen `legacy-diagnostics*.json:24610` (immutable, not edited).
- **`AppendCode::PayloadTooLarge`:** `contract.rs:40` (variant), `:83` (as_str), `http.rs:13` (status), `content.rs:112` (the only constructor).
- **`"payload_too_large"`:** `contract.rs:83`, `product.rs:1750`, `admission_maintenance.rs:800`, `RUNBOOK.md:138`, `WIRE-MATRIX.md:41,105`.
- **`"body_too_large"`:**
  - `contract.rs:71` (raw declared-length), `http.rs:1922` (product transport), `product.rs:2414`;
  - `admission_maintenance.rs:816,860`, `livefeed_ownership.rs:698` (a comment; that test asserts status only);
  - `RUNBOOK.md:139`, `WIRE-MATRIX.md:41,79,117,121`;
  - `docs/CHAOS-CAMPAIGN.md:382` (a historical campaign record, not edited);
  - a binary `bench/costab/bin/awsbench-ab`.
- **`QuotaRegistry::admit_append`:** the only production caller is `product.rs:2195`; `quota/tests.rs` and `quota/poison_tests.rs`.
- **`product_append_inner`:** the only caller is `product_append` (`product.rs:2009`), with `seal_after = false` and `seal_auth = None`. `product_append` is called from `product_entry` 1118 and 1133.
- **`render_product_append_error`:** `product.rs:1811` (seal), 2215 (key parse), 2335.
- **`MAX_BATCH_RECORDS`:** `product.rs:1902,2149`; a comment in `admission_maintenance.rs:815`.
- **`prepare_close` / `install_intent`:** `append.rs:274,290`. `renew_owed_claim` is called only from `close.rs:75`.
- **Owed-final failpoint users (pins for C3):** `durability_fences.rs:219`, `lifecycle_incarnation.rs:302`, `seal_coordination.rs:695`, `seal_fencing.rs:567`, `security_noninterference.rs:496`.
- **No use** in tools/, fuzz/, tests/, the SDK or conformance sources.

---

## 2 Contract decision (within the adopted §5)

1. **One stable code for the permanent per-stream capacity refusal: `payload_too_large`, on both surfaces.**
   - It applies to raw append, product append, product `records:batch` and the product seal final.
   - `body_too_large` stays the product transport body ceiling (http.rs:1922) and the raw declared-length refusal.
   - Rationale: raw and the product seal already say `payload_too_large`. Only product append and batch deviate, and they share their code with a different refusal.
   - Alternative, if SDK compatibility matters more: keep `body_too_large` on the product. Only the literal in `capacity_refused` and three test assertions change. I do not recommend it.
2. **Limit information.**
   - Product: every product capacity 413 carries `details: {"dimension": "bytes"|"records", "capacity": <u64>, "requested": <u64>}`.
     - `retryable: false`, and there is no `Retry-After`.
     - `capacity` is `LIMIT_*_PER_SEC × LIMIT_BURST_SECS` in whole units, the largest request that dimension admits.
     - `requested` is measured exactly as the core measures it: the committer wire body. A single JSON value travels as `[value]`, 2 bytes more than the request body. A batch counts its elements.
   - Raw: the message names the numbers: `request of {requested} {dimension} exceeds the per-stream ingest capacity of {capacity} {dimension}`.
   - Raw machine-readable `details` is decision D1. Raw has no `details` field anywhere; its structured extras are headers.
3. **No side effects before the refusal.**
   - Product: the 413 is decided with the body contract, before `admit_append`.
   - Raw close: the owed claim is renewed only after `parse_content`.
   - Necessary consequence: on the product surface the capacity 413 now outranks every verdict that used to come between the debit and the core. That is the full list in item 4 below. The only way to avoid reordering some of them would be to move the key and producer checks ahead of the quota, which is a different edge change.
4. **Client-visible changes (documented, all consequences of 1-3):**
   - (a) Product append and batch over capacity: `body_too_large` becomes `payload_too_large` plus `details`, and the message text changes. The product seal final gets `details`, and its message changes to the shared wording.
   - (b) Raw 413 message text: the code is unchanged and nothing is added to the body.
   - (c) A product append over capacity no longer charges the project's append-volume quota. The next valid append is not refused 429.
   - (d) While the project bucket is not full, it gets 413 instead of 429 `project_rate_limit` with `retry-after`.
   - (e) Product precedence for a body that is also over capacity: 413 now comes before these verdicts:
     - 429 `project_rate_limit` / `project_memory_pressure`;
     - 403 `stale_or_wrong_credentials` (key parse or fingerprint);
     - 400 `invalid_producer`, 429 `overloaded`, and 503 `creating` / 409 races inside `prepare`;
     - 413 `record_too_large` for a body that is also over the per-record ceiling (the product renders that as `body_too_large`);
     - 400 for JSON numbers that `RawValue` accepts but `Value` refuses (e.g. `1e400`);
     - a producer-deferred verdict. It can only be a duplicate if the original committed under a larger limit.

     The raw ordering, "a deferred producer verdict outranks the 413", is unchanged.
   - (f) Raw close resuming an owed final: the claim renewal now follows content validation and `renew_ttl`. A retry refused 413 leaves the claim untouched. In a double fault, 503 `ttl_renewal_unavailable` or a content verdict now precedes the superseded-claim 409 / renewal 503.

---

## 3 Red tests, pins, non-vacuity controls

The test paths are `dst::dst_tests::<module>::<name>`, `product::tests::<name>` and `usage::runtime_tests::<name>`.

### C1: typed refusal, stable code, limit information

**The red step.** Apply only the three DST edits below on the parent tree. They use HTTP only and compile against it. Run:

```
cargo test --locked --lib -- --exact \
  dst::dst_tests::admission_maintenance::over_capacity_record_count_is_a_permanent_413_not_a_429 \
  dst::dst_tests::admission_maintenance::product_batch_over_record_capacity_is_413_without_retry_after \
  dst::dst_tests::seal_recovery::an_impossible_final_never_publishes_an_intent
```

Expect `3 failed`.

- **R1:** `product_batch_over_record_capacity_is_413_without_retry_after` (`admission_maintenance.rs:820`). The doc comment's "(the product spelling is `body_too_large`)" changes to "(one spelling on both surfaces, `payload_too_large`, with its limit in `details`)". The body assertions are replaced with:

  ```rust
  let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
  assert_eq!(st, 413, "101 records never fit a 100-record bucket: {body}");
  let (code, d) = (body["error"]["code"].as_str(), &body["error"]["details"]);
  assert_eq!(
      (code, d["dimension"].as_str(), d["capacity"].as_u64(), d["requested"].as_u64()),
      (Some("payload_too_large"), Some("records"), Some(100), Some(101)),
      "{body}"
  );
  assert_eq!(headers.get("retry-after"), None);
  ```

  Red, traced: the core's `PayloadTooLarge` reaches the arm at product.rs:2413.

  ```
  assertion `left == right` failed: {"error":{"code":"body_too_large","message":"request body exceeds the limit","retryable":false}}
    left: (Some("body_too_large"), None, None, None)
   right: (Some("payload_too_large"), Some("records"), Some(100), Some(101))
  ```
- **R2:** `over_capacity_record_count_is_a_permanent_413_not_a_429` (`admission_maintenance.rs:781`). Add after the `payload_too_large` assertion:

  ```rust
  assert!(
      body.contains("of 10001 records exceeds the per-stream ingest capacity of 10000 records"),
      "the refusal names its limit: {body}"
  );
  ```

  Red:

  ```
  the refusal names its limit: {"error":{"code":"payload_too_large","message":"request exceeds the per-stream ingest records capacity"}}
  ```
- **R3:** `an_impossible_final_never_publishes_an_intent` (`seal_recovery.rs:173`). Add a leg before "// And a valid one still seals.". It is 11 lines, and the function stays at 87 code lines, under 100.

  ```rust
  // A final no FRESH per-stream bucket admits (external review §5):
  // the 413 names its limit, measured on the `[value]` wire body.
  let body = format!(r#"{{"final":"{}"}}"#, "x".repeat(10_000_000));
  let (st, _, b) = preq(addr, "POST", "/v1/streams/impossible:seal", &key, body.as_bytes()).await;
  let b: serde_json::Value = serde_json::from_slice(&b).unwrap();
  let d = &b["error"]["details"];
  assert_eq!(
      (st, d["dimension"].as_str(), d["capacity"].as_u64(), d["requested"].as_u64()),
      (413, Some("bytes"), Some(10_000_000), Some(10_000_004)),
      "{b}"
  );
  untouched("an over-capacity final").await;
  ```

  `requested` is 10,000,004: `fin.to_string()` is the 10,000,002-byte JSON string plus the pre-check's 2. Red:

  ```
  assertion `left == right` failed: {"error":{"code":"payload_too_large","message":"the final record exceeds the per-stream ingest bytes capacity","retryable":false}}
    left: (413, None, None, None)
   right: (413, Some("bytes"), Some(10000000), Some(10000004))
  ```

**Pins that compile only with the new API.** Their red is a compile error, so they count as pins, not reds:
- **P1:** a new `product::tests::a_core_capacity_refusal_renders_the_stable_413`, a `#[tokio::test]` in `src/product/tests.rs`.
  - It builds `render_product_append_error(AppendFailure::from_capacity(CapacityRefusal::new("records", 100.0, 101)))`.
  - It asserts status 413, no `retry-after`, `code == "payload_too_large"`, `retryable == false`, `details.capacity == 100` and `details.requested == 101`.
  - It collects the body with `axum::body::to_bytes`, as `every_auth_refusal_keeps_its_response` does.
  - It keeps the backstop render path covered after C2, when the handler's pre-check refuses first.
- **P2:** `usage::runtime_tests::the_runtime_owner_decides_only_the_requests_own_size`. The two refused assertions become full struct literals:
  - `Some(CapacityRefusal { dimension: "bytes", capacity: 10_000_000, requested: 10_000_001 })`;
  - `Some(CapacityRefusal { dimension: "records", capacity: 10_000, requested: 10_001 })`.

  Literals, not `new`, so the whole-unit capacity is pinned independently.
- **P3:** the proptest `permanent_refusal_is_exactly_fresh_bucket_refusal` (1,024 cases) keeps "kind for kind" and gains the exact limit. Written by hand, because rustfmt does not format inside `proptest!`:

  ```rust
  let refusal = usage.permanently_unadmittable(bytes, records);
  proptest::prop_assert_eq!(refusal.map(|r| r.dimension), fresh);
  if let Some(r) = refusal {
      let (rate, size) = if r.dimension == "bytes" { (bytes_rate, bytes) } else { (recs_rate, records) };
      proptest::prop_assert_eq!((r.capacity, r.requested), (u64::from(rate) * u64::from(burst), size));
  }
  ```

  The doc comment gains "…and names the exact capacity and the size requested".
- **P4:** the r10 pin `r10_runtime_usage_limits_counters_and_backlog_are_isolated`. One line becomes `assert_eq!(a.usage.permanently_unadmittable(11, 1).map(|r| r.dimension), Some("bytes"));`. The `None` line compiles unchanged. Its sha is re-pinned (§6).

**Unchanged pins that must stay green:**
- `a_deferred_producer_verdict_outranks_the_capacity_refusal` (raw deferred order).
- `debug_load_reports_typed_limiter_and_frame_totals` (the transient 429 limiter).
- `livefeed_ownership::record_ceiling_refuses_one_oversized_record_not_the_batch` (the product record ceiling is still 413).
- `r10_usage_refill_and_eviction_follow_only_owned_monotonic_time`.

**Non-vacuity controls** (revert after recording):
- (a) Delete the new early return in `render_product_append_error`. R1 fails with the R1 red above, and P1 fails with `left: (Some("body_too_large"), Some(false), None, None)`.
- (b) Make `Display for CapacityRefusal` write the old sentence. R2 fails with its red.

### C2: the product refuses before the project's volume debit

**Red.** Two new tests in `src/dst/tests/quota_enforcement.rs`. The drafts were measured at 49 lines, so the file goes from 946 to 995, under the 1,000 limit. Both run against the C1 tree, and both use HTTP only.

```rust
type CapacityRig = (std::sync::Arc<crate::http::AppState>, std::net::SocketAddr, String);
/// The §5 rig: one project whose append volume is 1 byte/s (any debit
/// outlives the test) and a JSON stream `c` at the default per-stream
/// capacity of 10,000,000 wire bytes.
async fn volume_capacity_rig(tag: &str) -> CapacityRig {
    let quotas = crate::project_policy::ProjectQuotas {
        append_bytes_per_sec: 1,
        ..Default::default()
    };
    let (state, addr, auth) = quota_rig(tag, "streams.create streams.records.append", quotas).await;
    let h = [("prisma-encryption-key", PRISMA_KEY), ("authorization", auth.as_str())];
    let json = br#"{"format":{"kind":"json"}}"#;
    assert_eq!(preq(addr, "PUT", "/v1/streams/c", &h, json).await.0, 201);
    (state, addr, auth)
}
```

- **R4:** `an_unadmittable_append_leaves_the_project_volume_quota_untouched` (rig tag `cu`).
  - It POSTs `format!("\"{}\"", "x".repeat(9_999_997))` to `/v1/streams/c/records` and expects 413.
  - That body is 9,999,999 bytes; its `[value]` is 10,000,001 bytes.
  - It then POSTs `{"n":1}` and expects `assert_eq!(st, 200, "the 413 left the project in debt: {}", ...)`, then calls `engine_shutdown`.
  - Red, traced: the bucket is full (level 1) and `cost 9,999,999 > rate 1`, so the request is admitted with level −9,999,998. The core answers 413. Then `{"n":1}`: level < 7, so it gets `Rate`.

    ```
    assertion `left == right` failed: the 413 left the project in debt: {"error":{"code":"project_rate_limit","message":"the project's rate quota is exhausted; retry","retryable":true}}
      left: 429
     right: 200
    ```
- **R5:** `an_unadmittable_append_is_413_not_429_while_the_quota_refills` (rig tag `cr`).
  - It first POSTs a 1,002-byte JSON string: 200. From a full bucket that is admitted into debt at −1,001, which takes about 1,000 s to refill at 1 B/s, so the timing margin is huge.
  - Then the same 9,999,999-byte body. It asserts `assert_eq!(st, 413, "a permanent refusal answered as transient: {b}")`, and that `b.contains("payload_too_large") && !headers.contains_key("retry-after")`.
  - Red: the bucket is not full and level < cost, so it gets `Rate`.

    ```
    assertion `left == right` failed: a permanent refusal answered as transient: {"error":{"code":"project_rate_limit","message":"the project's rate quota is exhausted; retry","retryable":true}}
      left: 429
     right: 413
    ```

**Pins:**
- R1: the batch 413 now comes from the handler. Exactly 100 records still returns 200, which pins `>` against `>=` and pins `count == elems.len()`.
- `producer_protocol::product_append_and_append_many`, which covers the moved shape code: 405 for a bytes batch, `empty_batch`, invalid JSON, and a single append on a bytes stream.
- `quota_enforcement::volume_quotas_meter_appends_and_reads`: the debit still uses the exact parsed count after the pre-check.
- `queued_append_bytes_charge_and_release`.

**Non-vacuity controls:**
- (a) In `parse_append_body`, measure `body.len()` instead of `wire_body.len()`. R4 fails at the second append with the R4 red, and R5 fails with the R5 red. The 9,999,999-byte body is within capacity by itself and over it only as `[value]`, so the mismeasured check passes it on to the debit.
- (b) Delete the capacity check in `parse_append_body`. Both reds return.

### C3: raw close renews an owed claim only after validation

**Red:** `a_refused_owed_final_retry_leaves_its_claim_unrenewed` in `src/dst/tests/seal_coordination.rs`. The draft is 43 lines, and the file goes from 785 to about 830. It runs against the C2 tree.

It needs one import change: `use super::fixture_http::{HttpRigOptions, engine_shutdown, http_rig, http_rig_build};` plus `use super::fixture_runtime::RigRuntime;`.

The test:
1. Uses a nested `async fn claim(state) -> Option<u64>`, which does `invalidate` + `registry.get(raw_adapter_sref("owedcap"))` and returns `.sealing.map(|s| s.claim_generation)`.
2. Rig 1 is `http_rig(store.clone())` at the default capacity. It does a raw PUT of `/v1/stream/owedcap`.
3. With `stop_after_seal_intent("owedcap")`, it sends a raw POST of a 212-byte `[{"pad":"x"×200}]` with `stream-closed: true`, and expects 503.
4. It clears the failpoint, then `engine_shutdown` and `drop(state)`.
5. Rig 2 is `http_rig_build(store, RigRuntime::incarnation(1), HttpRigOptions { admission: Some(AdmissionConfig { limit_bytes_per_sec: 50.0, ..Default::default() }), ..Default::default() })`. That is a 100-byte capacity. This is the restart pattern of `admission_memory.rs:469-508`.
6. `before = claim(&state)`, asserted `is_some()`.
7. It sends the identical retry and expects 413.
8. It asserts `assert_eq!(claim(&state).await, before, "a refused retry renewed the owed claim")`.

Red, traced: `is_owed_final` holds, because the op id is recomputed identically from the body and headers (`http.rs:2599-2621`). So `renew_owed_claim` increments the counter. Then `parse_content` finds `deferred` is `None` (valid CT and JSON, synthetic producer, no ceiling), 212 > 100, and answers 413.

```
assertion `left == right` failed: a refused retry renewed the owed claim
  left: Some(g+1)
 right: Some(g)
```

Here g is the crashed close's claim generation; on a fresh store it should be `Some(2)` against `Some(1)`. Record the actual values.

**Pins** (they all resume an owed final through the moved renewal):
- `seal_coordination::a_crashed_raw_final_close_is_resumed_by_an_ordinary_retry`
- `durability_fences` (race9)
- `lifecycle_incarnation` (abaseal)
- `seal_fencing` (rescue)
- `security_noninterference` (fpx)
- the C1 and C2 reds, which stay green.

**Non-vacuity controls:**
- (a) Restore the renewal in `prepare_close`. The red returns.
- (b) Delete the new owed branch in `install_intent`. `a_crashed_raw_final_close_is_resumed_by_an_ordinary_retry` fails with "the exact retry could not resume: 503 …".
  - Traced: `plan.generation` is `None`, so `complete_raw_close` hits `close.generation.ok_or(SealError::InvalidClaim)?`, which becomes `SealIncomplete` / 503.
  - Record the exact line. This proves the pin exercises the moved renewal.

---

## 4 Edits, file by file, in commit order

Ceilings, checked with `wc -l` at every commit:

| file | limit | before → after |
|---|---|---|
| `src/product.rs` | 4,205 | about 4,197 (C1) → about 4,125 (C2) |
| `src/usage.rs` | 1,000 | 993 → 995 |
| `src/dst/tests/quota_enforcement.rs` | 1,000 | 946 → ≤ 995 |
| `src/dst/tests/admission_maintenance.rs` | 1,000 | 924 → about 933 |
| `src/dst/tests/seal_recovery.rs` | 1,000 | 896 → about 909 |
| `src/dst/tests/seal_coordination.rs` | 1,000 | 785 → about 830 |

`src/http.rs` (3,153) is untouched. New files: `src/usage/capacity.rs` (about 45 lines) and `src/product/append_body.rs` (about 25 lines in C1, about 115 after C2).

### C1: "The per-stream capacity 413 is one typed refusal that names its limit, spelled payload_too_large on both surfaces"

1. **`src/usage/capacity.rs` (new; declared as `mod capacity;` in usage.rs, so no `#[path]` is needed).**
   - A module doc (§5).
   - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub(crate) struct CapacityRefusal { pub(crate) dimension: &'static str, pub(crate) capacity: u64, pub(crate) requested: u64 }`.
   - `impl CapacityRefusal { pub(crate) fn new(dimension: &'static str, tokens: f64, requested: u64) -> Self { Self { dimension, capacity: tokens as u64, requested } } }`.
   - The new exception on `new`:

     ```
     #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss,
       reason = "CapacityRefusal::new; boot validation (config::admission_limits) proved an enabled bucket finite and at least one token, so truncation is its whole-unit floor and a saturating as-cast loses nothing a request can reach; a checked conversion would only restate the boot proof")]
     ```

     The reason has three parts, as the regex requires. It is modelled on `quota/bucket.rs::retry_after`, and it is a new narrow decision, not a re-decision.
   - `impl std::fmt::Display` with `write!(f, "request of {requested} {dimension} exceeds the per-stream ingest capacity of {capacity} {dimension}")`, using a destructured `Self`. `write!` is an expression macro, so there is no owners.json row.
   - Truncation equals floor, and `n > c ⇔ n > ⌊c⌋` for an integer `n`, so the reported capacity agrees exactly with the f64 predicate.
2. **`src/usage.rs` (+2).**
   - After the `use` block: `mod capacity;` and `pub(crate) use capacity::CapacityRefusal;`.
   - `permanently_unadmittable` returns `Option<CapacityRefusal>`. The two returns become `return Some(CapacityRefusal::new("bytes", l.bytes_per_sec * l.burst_secs, bytes));` (92 cols) and `…("records", l.recs_per_sec * l.burst_secs, records)` (96 cols).
   - The predicate lines are unchanged. The doc comment is reworded in place with no new lines: "every append surface refuses it 413 before any side effect".
   - No exception scope is involved: there is none on `impl UsageService` or on this method.
3. **`src/application/append/contract.rs`.**
   - `AppendConflict` gets `Capacity(crate::usage::CapacityRefusal)`, with a doc line: "not a conflict: the structured refusal detail the renderers report".
   - `pub(crate) fn from_capacity(refusal: CapacityRefusal) -> Self` = `Self::new(Invalid, PayloadTooLarge, refusal.to_string())` plus `conflict = Some(Box::new(AppendConflict::Capacity(refusal)))`.
   - `pub(crate) fn capacity_refusal(&self) -> Option<&CapacityRefusal>`, written as `if let Some(AppendConflict::Capacity(r)) = self.conflict.as_deref() { Some(r) } else { None }`, so there is no new `_ =>` arm.
   - `AppendFailure`'s size is unchanged because the detail is boxed, so `result_large_err` is not affected.
   - This is a hard owner, and it takes no transport dependency.
4. **`src/application/append/content.rs`.**
   - `Some(kind)` becomes `Some(refusal)`, and the 5-line `fail(...)` becomes `return Err(AppendFailure::from_capacity(refusal));`.
   - Ratchet (`parse_content` has an `excessive_nesting` expect): scope_lines −3, syntax_facts −1 (6 facts become 5), nested_items unchanged. The comment is untouched.
5. **`src/product/append_body.rs` (new; `mod append_body;` plus `use append_body::capacity_refused;` next to `mod consumer_pull;`, +2).**
   - `use super::perr; use crate::usage::CapacityRefusal; use axum::{http::StatusCode, response::Response};`.
   - `pub(super) fn capacity_refused(refusal: &CapacityRefusal) -> Response` = `perr(PAYLOAD_TOO_LARGE, "payload_too_large", &refusal.to_string(), Some(serde_json::json!({"dimension": refusal.dimension, "capacity": refusal.capacity, "requested": refusal.requested})), false)`.
   - Its doc: "the ONE product spelling … `body_too_large` stays the transport ceiling's code".
   - Architecture: `super::perr` is not a reverse edge. No `crate::http`/`crate::product` path, and it is not a hard owner.
6. **`src/product.rs`.**
   - **`render_product_append_error`:**
     - Delete its `#[expect(clippy::unwrap_used, …)]` (−4). This is not a reason edit: its only unwrap goes away.
     - `axum::http::HeaderValue::from_str(&retry.to_string()).unwrap()` becomes `axum::http::HeaderValue::from(retry)` (`From<u64>`, the same decimal bytes), reflowed to one line (−3).
     - The first statement after the `use` becomes `if let Some(refusal) = error.capacity_refusal() { return capacity_refused(refusal); }` (+3).
     - The generic `F::Invalid if 413 => body_too_large` arm stays for `RecordTooLarge`/`TooLarge`/`BodyTooLarge`.
   - **`product_seal`** (exception `too_many_lines` + `excessive_nesting`): `Some(kind)` becomes `Some(refusal)`, and the 8-line `perr(...)` becomes `return capacity_refused(&refusal);`.
     - Ratchet: scope_lines −6.
     - syntax_facts −3: 6 removed (the perr call and path, the `StatusCode::PAYLOAD_TOO_LARGE` path, `format!` plus its tokens, the `None` path), 3 added.
     - Nesting depth is unchanged, so `excessive_nesting` still fires elsewhere in the fn, and `too_many_lines` still fires (201).
7. **Tests:** R1, R2, R3, P1, P2, P3, P4 (§3).
8. **Docs:**
   - `RUNBOOK.md:138-139`: "(product: `body_too_large`)" becomes "on both surfaces … the product adds `details {dimension, capacity, requested}`, the raw message names both numbers".
   - WIRE-MATRIX §6.

**Ratcheted scopes in C1:** `parse_content` and `product_seal` shrink. `render_product_append_error`'s exception is removed. New: `CapacityRefusal::new`'s cast expect. `product_request_hash` in the same file is untouched.

### C2: "A product append no fresh per-stream bucket admits is refused before the project's volume quota is charged"

1. **`src/product/append_body.rs`: add `parse_append_body`, a verbatim move with two changes.**
   - It adds `const MAX_BATCH_RECORDS: usize = 10_000;` (moved from product.rs:1902, its only user) and `use crate::registry::StreamDesc; use crate::usage::UsageService; use bytes::Bytes;`.
   - The signature: `pub(super) fn parse_append_body(usage: &UsageService, desc: &StreamDesc, body: &Bytes, batch: bool) -> Result<(Bytes, usize), Box<Response>>`. It has 4 args and 1 bool. The error is boxed to satisfy `result_large_err` with no new exception, following the precedent of `http.rs:2767` `sse_acquire`.
   - The body is product.rs:2112-2189 moved verbatim, with these changes:
     - `let is_json = desc.is_json();` (`registry.rs:669`, the same `media_type(..) == "application/json"`);
     - `return perr(..)` becomes `return Err(Box::new(perr(..)))`, and rustfmt keeps the line counts;
     - `&body` becomes `body` in the three `from_slice` / `extend_from_slice` sites, to avoid `clippy::needless_borrow`.
   - Then:

     ```rust
     if let Some(refusal) = usage.permanently_unadmittable(wire_body.len() as u64, count as u64) {
         return Err(Box::new(capacity_refused(&refusal)));
     }
     Ok((wire_body, count))
     ```

     `wire_body` and `count` are exactly the `command.body` and `entries.len()` the core measures. That is argued in §2 and pinned by R1, R4 and R5.
   - About 78 code lines, nesting ≤ 4 as in its old home. Not in any exception scope.
   - The module doc is extended to "the product append body contract … every permanent refusal of the body is decided here, before the project's append-volume debit and any lifecycle write".
2. **`src/product.rs`.**
   - Delete 2112-2189: `let is_json`, the 405 check, the Stage 4 §5 comment and the shape block (78 lines).
   - Delete `const MAX_BATCH_RECORDS` (1 line).
   - Insert:

     ```rust
     // The body contract, including the permanent 413 for a body no fresh
     // per-stream bucket admits, is decided BEFORE the §17.2 debit below
     // (external review §5): a refusal leaves no quota debt behind.
     let (wire_body, count) = match parse_append_body(&state.runtime.usage, &desc, &body, batch) {
         Ok(shape) => shape,
         Err(refused) => return *refused,
     };
     ```

     The `match` line is 93 columns. The `use` line becomes `use append_body::{capacity_refused, parse_append_body};`.
   - `state.runtime.usage` is the same `Arc` the core uses (`http.rs:239`, `append_service` → `usage: self.runtime.usage.clone()`). Rigs share it with the shard via `fixture_http.rs:424,437`.
   - **Ratchet** (`product_append_inner` has two attributes, each scoped to the fn):
     - scope_lines about −72;
     - syntax_facts drop by over 100 (the whole shape block), about +10 added;
     - nested_items 0 → 0.
   - **The expectations still hold:** 182 − 71 + 4 = 115 code lines, still over 100, so `too_many_lines` fires. `batch` and `seal_after` still trip `fn_params_excessive_bools`, and there are still 9 args. No attribute text changes.
3. **Tests:** R4 and R5 (§3).
4. **Docs:** WIRE-MATRIX §2.7 and §2.8 (§6).

**Ratcheted scopes in C2:** only `product_append_inner`, and it shrinks. `quota_rig` is called, not modified. Its `too_many_lines` scope is unchanged; it stays private, because making it `pub(super)` would add a visibility fact.

### C3: "A raw close renews its owed final's claim only after the content owner has validated the record"

1. **`src/application/append/close.rs`.**
   - `prepare_close`: delete lines 74-99, the renewal `if`/`match`. `let mut raw_seal_gen` becomes `let raw_seal_gen`.
   - The doc becomes "Authenticate a final's execution token and plan the close; an owed claim is renewed by `install_intent`, after validation".
   - The function falls to 84 code lines, so `#[expect(clippy::too_many_lines, …)]` becomes unfulfilled, which is a denied error, and **the exception is deleted**. That is not a reason edit.
   - `install_intent`: at the top, after the existing `let`s, add the moved block, unchanged except `raw_seal_gen = Some(g)` becomes `plan.generation = Some(g)`:

     ```rust
     if is_owed_final && seal_auth.is_none() {
         // Resuming an owed final renews only its own claim, and only now,
         // after deterministic validation: a refused retry writes nothing.
         match crate::application::lifecycle::renew_owed_claim(&state.lifecycle, &desc.sref(), this_close_op, &desc.stream_epoch).await { … }
     }
     ```

     - The condition is unchanged: there was no `close` or `deferred` gate before and there is none now. The deferred-producer path still renews, so a committed final's duplicate completes under the renewed generation.
     - `this_close_op` borrows `plan.operation` while `plan.generation` is assigned. Those are disjoint fields, and the existing branch already does this.
     - The doc becomes "Publish the close's lifecycle write, a fresh intent or the renewal of the owed final it resumes, only after deterministic validation".
     - The function goes from 45 to about 67 code lines, nesting ≤ 3. It has no exception.
2. **`src/application/append.rs`:** unchanged. `execute_once` and its `too_many_lines` scope are not touched.
3. **Tests:** R6. **Docs:** `docs/append-transitions.md` steps 1-2 (§6) and WIRE-MATRIX §2.1.

**Ratcheted scopes in C3:** the `prepare_close` exception is removed. Nothing else is in scope.

---

## 5 Mutation analysis

**CI selection.** None of these paths is under `CRITICAL_PREFIXES` (`verification_plan.py:21-32`) or in `mutation_owners.py`:
- `src/usage.rs`, `src/usage/capacity.rs`;
- `src/application/append/{contract,content,close}.rs`;
- `src/product.rs`, `src/product/append_body.rs`, `src/product/tests.rs`;
- `src/usage/runtime_tests.rs`;
- `src/dst/tests/*`.

Details:
- `src/product` is not `src/product_cursor`, and `src/application/append` is not `src/application/read_`.
- Nothing is deleted, so no added file is forced into registration.

`plan.json` should therefore show `"mutants": false` and `mutation_source_files: []`, with `properties_fuzz` and `miri` false (§7). No `mutation_owners.py` row is needed.

**Adequacy anyway.** These are the mutants cargo-mutants 27.1.0 would generate on the changed lines, and what kills each:

| where | mutant | killer |
|---|---|---|
| `permanently_unadmittable` | body → `None` | P2, P3, R1-R5 |
| `permanently_unadmittable` | → `Some(Default::default())` | unviable (no `Default`) |
| `permanently_unadmittable` bytes `return` line | `*` → `+` / `/` (capacity 5,000,002 / 2,500,000) | P2, P3 |
| `permanently_unadmittable` records `return` line | `*` → `+` / `/` (capacity 52 / 25) | R1, P3 |
| `CapacityRefusal::new` | body → default | unviable |
| `Display::fmt` | → `Ok(Default::default())` (empty message) | R2 |
| `from_capacity` | → default | unviable (no `Default`) |
| `capacity_refusal` | → `None` | P1 (and R1 at C1) |
| content.rs let-chain | `&&` → `||` | unviable (`let` in `||`) |
| `capacity_refused` | → `Default::default()` (200, empty) | R1, R3, R5, P1 |
| `render_product_append_error` | → default | P1, R1, and the existing product error tests |
| `parse_append_body` | → `Ok((Default::default(), 0 \| 1))` | `product_append_and_append_many`, R1 |
| `parse_append_body` | → `Err(Default::default())` | same |
| `batch && !is_json` | → `\|\|` | `product_append_and_append_many` (single JSON gets 405) |
| `batch && !is_json` | delete `!` | the bytes-batch 405 assertion in `product_append_and_append_many` |
| `elems.len() > MAX_BATCH_RECORDS` | `<` | R1 (a 100-record batch) |
| `elems.len() > MAX_BATCH_RECORDS` | `==`, `>=` | **no killer**: no test posts exactly 10,000 or 10,001 records |
| `body.len() + 2` (`Vec::with_capacity`) | `-`, `*` | **equivalent** (capacity hint) |
| `install_intent` | `is_owed_final && seal_auth.is_none()` → `\|\|` | every raw append: renewal declines and gives 409 (`over_capacity_record_count…`'s follow-up append, the rawcrash test) |
| `install_intent` | body → `Ok(())` | the rawcrash test and R6 (no intent) |

The two batch-bound mutants and the equivalent capacity-hint mutant sit in moved, non-critical code. They are pre-existing gaps, and CI does not select them; §8 lists them as a follow-up.

**Boundedness.** Every new or changed test issues a fixed number of HTTP requests, at most 4, against in-memory rigs, with no sleeps or polls.
- The 10 MB bodies are fixed and finite.
- The parent-tree red paths end in 413 or 429 and never park.
- R5's timing margin is about 1,000 s against a sub-second window.

---

## 6 Ledgers (same commit as the change)

**C1:**
- **`docs/quality/owners.json`:** add

  ```
  {"category":"macro-dsl","count":1,"owner":"crate::capacity_refused","path":"src/product/append_body.rs","syntax":"serde_json::json",
   "reason":"The permanent capacity refusal's details object {dimension, capacity, requested} is rendered with serde_json's literal macro; its wire shape is the product error contract pinned by admission_maintenance, seal_recovery, quota_enforcement and product::tests, and WIRE-MATRIX §2.5/§2.7/§2.8."}
  ```

  `source-allowances.json` needs nothing: the `product_seal` json count of 1 and the `render_product_append_error` count of 2 are unchanged, so there is nothing stale to prune.
- **`docs/refactor/test-inventory.json`:** `python3 scripts/test-inventory.py --write`. This updates the shas of `over_capacity_record_count_is_a_permanent_413_not_a_429`, `product_batch_over_record_capacity_is_413_without_retry_after` and `an_impossible_final_never_publishes_an_intent`. Scenario-map names are unchanged.
- **`docs/refactor/review-mechanisms.json`:** mechanism `runtime-usage-isolation` gets the new sha of `src/usage/runtime_tests.rs::r10_runtime_usage_limits_counters_and_backlog_are_isolated`. Compute it with `inv.functions(Path('src/usage/runtime_tests.rs').read_text(), Path(...))`, loading `scripts/test-inventory.py` via importlib. It is not in `required_units`, so `source_adaptations` needs no entry.
- **`docs/refactor/WIRE-MATRIX.md`:**
  - §2.1 errors (line 41): the `payload_too_large` message names requested and capacity.
  - §2.5 (105): "413 `payload_too_large` (+details `{dimension, capacity, requested}`, requested = the `[value]` wire bytes of the final)".
  - §2.7 translation (117): "`payload_too_large` (+details …, no `retry-after`) for the per-stream fresh-bucket refusal; other 413s (per-record ceiling) → `body_too_large`".
  - §2.8 (121): `body_too_large` becomes `payload_too_large` plus details.
- **`RUNBOOK.md`** 138-139.

**C2:**
- `test-inventory.json`: `--write`, adding 2 entries.
- WIRE-MATRIX §2.7:
  - The handler line (115): "the body contract and the permanent 413 are decided in `src/product/append_body.rs` before project admission's §17.2 debit; `requested` counts the `[value]` wire body".
  - The Metering line: "`admit_append` … after the capacity 413".
- No owners, allowances, architecture-policy or mutation rows: `parse_append_body` uses only `format!`, there is no glob, and there is no reverse edge.

**C3:**
- `test-inventory.json`: `--write`, adding 1 entry.
- `docs/append-transitions.md` step 1: "validates any trusted final-record claim; an exact owed-final operation is renewed only in `install_intent`, after step 2's deterministic validation". Step 2 names the renewal alongside the intent.
- WIRE-MATRIX §2.1: "before any lifecycle intent or owed-claim renewal".

None of the commits touches `architecture-policy.json`, `mutation_owners.py`, `policy.json` or the frozen `legacy-*.json`.

---

## 7 Controls

`$S` is the scratchpad and `P` is the commit's parent. Run each block per commit.

```
# ceilings: product.rs <= 4205, usage.rs <= 1000, every touched DST file <= 1000
wc -l src/product.rs src/usage.rs src/http.rs src/dst/tests/{quota_enforcement,admission_maintenance,seal_recovery,seal_coordination}.rs

cargo fmt --all -- --check                                  # expect: no output
cargo build --locked -p streams-quality-syntax
cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > $S/p17-clippy.jsonl
QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$P python3 scripts/quality/gate.py --clippy $S/p17-clippy.jsonl
    # expect exit 0 with none of: "file growth", "accepted exception grew",
    # "unregistered source occurrence", "exception needs owner"
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items
python3 scripts/architecture-gate.py --check                # expect: no failures
python3 scripts/test-inventory.py --check && python3 scripts/review-evidence.py --check && python3 scripts/scenario-map-report.py --check
QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$P python3 scripts/quality/verification_plan.py --out $S/p17-plan
    # expect: "mutants": false, "mutation_source_files": [], "properties_fuzz": false, "miri": false
```

**Red, then green, per commit:** the §3 red commands on the parent state, which must show exactly the listed panics, then:

```
scripts/test-leg.sh $S/p17-c1.log --exact dst::dst_tests::admission_maintenance::over_capacity_record_count_is_a_permanent_413_not_a_429 \
  --exact dst::dst_tests::admission_maintenance::product_batch_over_record_capacity_is_413_without_retry_after \
  --exact dst::dst_tests::seal_recovery::an_impossible_final_never_publishes_an_intent \
  --exact product::tests::a_core_capacity_refusal_renders_the_stable_413 \
  --exact usage::runtime_tests::the_runtime_owner_decides_only_the_requests_own_size \
  --exact usage::runtime_tests::permanent_refusal_is_exactly_fresh_bucket_refusal \
  --exact usage::runtime_tests::r10_runtime_usage_limits_counters_and_backlog_are_isolated \
  -- --locked --lib -- --exact <the same seven names>
scripts/test-leg.sh $S/p17-c2.log --exact dst::dst_tests::quota_enforcement::an_unadmittable_append_leaves_the_project_volume_quota_untouched \
  --exact dst::dst_tests::quota_enforcement::an_unadmittable_append_is_413_not_429_while_the_quota_refills -- --locked --lib -- --exact <same two>
scripts/test-leg.sh $S/p17-c3.log --exact dst::dst_tests::seal_coordination::a_refused_owed_final_retry_leaves_its_claim_unrenewed -- --locked --lib -- --exact <same>
```

**Neighbourhood, per commit:**

```
cargo test --locked --lib dst::dst_tests::admission_maintenance:: dst::dst_tests::quota_enforcement:: dst::dst_tests::seal_ dst::dst_tests::producer_protocol:: dst::dst_tests::livefeed_ownership:: dst::dst_tests::durability_fences:: dst::dst_tests::lifecycle_incarnation:: dst::dst_tests::security_noninterference:: dst::dst_tests::runtime_usage:: usage:: product:: application::append
```

Expect 0 failed.

**Non-vacuity controls:** the §3 controls, each with its expected output recorded, then reverted.

**Before push:** CI's suite `cargo test --release -- --skip post_split_throughput_scales`, then `python3 scripts/quality/tests_ran.py <log> --inventory docs/refactor/test-inventory.json --skipped 1`. Then the full `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=24c4c77a bash scripts/quality.sh`, which must print `QUALITY_OK`. Verify with `gh run view` after the push, and do not claim CI is green without it.

---

## 8 Out of scope (follow-ups)

- **Stuck Sealing after a lowered limit.** An owed final published under a larger `LIMIT_*` can never be written after a restart with a smaller one, so the collection stays Sealing until the limit is restored. C3 only stops the pointless renewal. The fix belongs with an operator-visible abandon policy.
- **`product_append_sealing` duplicates the `[value]` wrapping.** It could share `parse_append_body`'s wire shape if it took an `AppendFailure`-typed variant.
- **Dead parameters on `product_append_inner`.** `seal_after` and `seal_auth` are always `false`/`None`, because the only caller is `product_append`. Removing them narrows `fn_params_excessive_bools` by deleting a lint from an attribute. That is a separate, attribute-narrowing change.
- **No batch-bound tests.** Nothing posts a batch of exactly 10,000 or 10,001 records, so the moved `MAX_BATCH_RECORDS` comparison has two unkilled mutants (§5). It is not critical code.
- **Two raw transport-ceiling codes.** Raw has `body_too_large` for the declared length and `too_large` for buffering. That is pre-existing and not the per-stream refusal.
- **`charge_queued` precedes the 413.** Under queue pressure, an over-capacity body can first get a transient 429 `queued_bytes`, with no lasting side effect. See D3 for the permanent case.

## 9 Decisions for the owner

- **D1: raw machine-readable `details`.** The reviewer's optional ask was to add `details` to the raw body too.
  - Raw has no error `details` field anywhere; its structured extras are headers.
  - `render_append` (`http.rs:41`, critical, owner `http`) carries a function-wide `unwrap_used` exception. Any new call or path in it grows the fingerprint ratchet. So doing this means splitting it into OK and error renderers with narrowed exceptions. That puts moved critical code in the in-diff mutation leg under the `http` owner's filters, and it creates a new raw body field (or new headers).
  - Recommendation: keep the numbers in the raw message now, as the raw 429 limiter does, and decide raw parity separately.
- **D2: product per-record ceiling code.** The product renders `RecordTooLarge` as `body_too_large` ("request body exceeds the limit"), while raw says `record_too_large`. It is the same kind of stable-code gap. Recommendation: map it to `record_too_large` on the product. That is a client-visible edge change.
- **D3: project quotas a single request can never fit.** A body larger than the project's `queued_append_bytes` ceiling is refused 429 `queued_bytes` forever (`quota.rs:879-883`). The question is whether a project-quota ceiling that one request can never fit should be a 413, the same principle as §5, applied to `src/quota` (critical).
- **D4: the seal final pre-check over-refuses non-JSON finals by 2 bytes.** At `product.rs:1746` it always adds `+ 2`, but the wrapper is JSON-only (`product_append_sealing`, 1947-1955). A non-JSON final within 2 bytes of capacity is refused 413 although the core would admit it. The fix is `+ if validated.is_json() { 2 } else { 0 }`, which is neutral on `product_seal`'s facts. It is an edge change (413 → 200 at the boundary), so it is held.

---

## Skeptic corrections (C1..C12)

Checked against `slate` @ `24c4c77a` (read-only; nothing run). Every quote and line cite in §1 checks out except where noted below. Also verified:
- the ceilings (product.rs 4205, usage.rs 993, and the DST files 946/924/896/785);
- the ratchet mechanics (`source_rules.exception_contracts`/`exception_growth`, reason regex `"[^";]+;[^";]+;[^";]+"`);
- that `prepare_close` falls from 110 to 84 counted lines, so its `too_many_lines` expect really does become unfulfilled;
- that `render_product_append_error`'s only unwrap is the retry header;
- that `http::HeaderValue: From<u64>`;
- that `state.runtime.usage` is the shard's usage Arc in rigs (`fixture_http.rs:424-437`);
- the R1-R6 red traces;
- that no changed path is under `CRITICAL_PREFIXES` or in `mutation_owners.py`;
- that `super::perr` is not a reverse edge (`architecture-gate.py:33-50`);
- that no SDK/conformance/contracts file names `body_too_large`/`payload_too_large`.

No exception reason text is edited anywhere. The only new exception is the narrow cast expect on `CapacityRefusal::new`, which is a new identity and not growth absorbed into an old one.

**C1 (substantive; C2 design): the handler pre-check breaks the product spec's duplicate-first MUST for producer requests.**
- `handover/prisma_streams_surface_spec_prelaunch_hard_cutover/04-APPEND-AND-APPEND-MANY.md:154-173` orders the checks as "6. producer tuple is valid and duplicate detection runs" before "10. service limits admit the request". It then says: "Duplicate producer requests MUST be recognized before rejecting a retried body for a later validation condition…".
- The handler states the same rule in its own comment at `src/product.rs:2121-2123`: "the shared path handles producer duplicate recognition ahead of later-validation rejections".
- §2.4(e)'s last bullet turns this into a documented regression: a producer retry of a record committed under a larger `LIMIT_*` gets 413 instead of `200 duplicate:true`. That is an edge change outside the adopted item, and the reviewer's placement ("immediately before `admit_append`") inherits it.
- Fix, keeping "no side effects before the 413":
  - In `product_append_inner`, compute `over = permanently_unadmittable(wire_body.len(), count)`. Refuse with `capacity_refused` only when none of `producer-id`/`producer-epoch`/`producer-seq` is present.
  - When a producer header IS present and `over.is_some()`, skip `state.quotas.admit_append` entirely. Such a request can only end as a duplicate (commits nothing) or as the core's 413/deferred refusal (`content.rs:103-115`), so skipping the debit is not a quota bypass.
  - The core then renders the 413 through C1's backstop arm, with `details`.
  - Keep `parse_append_body` shape-only (4 args, 1 bool), or pass `&HeaderMap` as a 5th argument. Do not add a second bool parameter (`fn_params_excessive_bools`).
- This also removes "400 `invalid_producer` → 413" from (e): a partial trio still reaches `parse_producer` first.
- Add a leg with producer headers: oversized body → 413 `payload_too_large` with details (via the core), then `{"n":1}` → 200 (no debt).
  - Red on the C1 tree: the second append is 429 `project_rate_limit`.
  - Control: drop the `over.is_none()` guard on the debit, and the red returns.
- Line budget: `quota_enforcement.rs` is 995 after R4+R5, and crossing 1,000 is blocked (RUST-QUALITY file-growth row). Fold the leg into R4 (post the oversized body once without and once with the producer trio before the small append) and re-measure with `wc -l`. If it does not fit, stop and ask the owner. Do not make `quota_rig` `pub(super)`: that is a ratcheted scope.

**C2 (owner visibility): the remaining precedence flips in §2.4(e) are edge changes, not "necessary consequences".**
- Flipped: 403 `stale_or_wrong_credentials` (key parse at `product.rs:2213` and fingerprint in `prepare`), 503 `creating`/409 prepare races, the `record_too_large`→`body_too_large` 413, the `1e400` 400, and 429 `project_memory_pressure`. All of them now lose to 413.
- They follow from the reviewer-adopted placement. However, the spec orders "authenticate" (1) before "service limits" (10), and the task says any other edge change is a decision for the owner.
- List them as **D5**, with the facts that make the plan's placement the least disruptive choice:
  - the debit already precedes the key check today, so an over-capacity request against a non-full bucket already gets 429 before 403;
  - the alternative (move the pre-check and the §17.2 debit after `prepare`/`parse_producer` in `submit_product_append`) reorders 429 vs 403/400 for every request.
- Recommend the plan's placement (the reviewer's position) with C1 applied.

**C3: WIRE-MATRIX section names are wrong.** Line 41 is **§1.2** (raw `POST /v1/stream/{name}`), not §2.1 (§2.1 is `OPTIONS /v1/streams/{*name}`, WIRE-MATRIX.md:87). Fix both the C1 ledger bullet "§2.1 errors (line 41)" and the C3 bullet "WIRE-MATRIX §2.1: before any lifecycle intent or owed-claim renewal"; both belong in §1.2. §2.5 (105), §2.7 (115-118) and §2.8 (121) are correct.

**C4 (unbuildable control): the §7 neighbourhood command.** `cargo test --locked --lib dst::dst_tests::admission_maintenance:: dst::dst_tests::quota_enforcement:: …` passes several positional TESTNAMEs, and cargo accepts one. Put every filter after `--`: `cargo test --locked --lib -- dst::dst_tests::admission_maintenance:: dst::dst_tests::quota_enforcement:: … application::append`. The red commands (`-- --exact a b c`) are fine.

**C5: the refactor of `render_product_append_error` has no pin.** `HeaderValue::from_str(&retry.to_string()).unwrap()` → `HeaderValue::from(retry)` (`product.rs:2425-2430`) is a behaviour-preserving refactor. The rules require it to name a pin plus a non-vacuity control. No existing test asserts a `retry-after` rendered by `render_product_append_error`:
- `admission_memory.rs:435` is `project_memory_gate`;
- `admission_maintenance.rs:282` is raw.

Extend P1 (or add P1b in `src/product/tests.rs`): render `AppendFailure::new(FailureClass::Capacity, AppendCode::RateLimited("limit_bytes_per_sec"), "x").retry(7)` and assert 429, `retry-after == "7"`, `retryable == true`. Control: delete the `if let Some(retry)` insert, and P1b fails with `left: None right: Some("7")`.

**C6: the tree is NOT clean.** `git status` at 24c4c77a shows uncommitted `docs/quality/owners.json`, `src/tasks/refusal.rs` and `src/tasks/tests.rs` from other in-flight work.
- `src/tasks` is a critical prefix and has registered owners (`tasks`, `tasks_refusal`).
- C1 edits `owners.json`, so stage only this plan's hunk (`git add -p`), or land after that work.
- Never `git commit -a`. Doing so would pull tasks code into the in-diff mutation leg and into these commits.
- Re-run `verification_plan.py` on a clean checkout of each commit: it diffs the working tree ("Includes working changes during local development"), so a local plan.json would otherwise show tasks files.

**C7: cite fixes.**
- `AppendCode::PayloadTooLarge => "payload_too_large"` is `contract.rs:84`, not :83.
- `BodyTooLarge` is `contract.rs:72`, not :71.
- `prepare_close` spans `close.rs:19-135`; its attribute is at 15.

**C8: §5 mutation-table errors** (CI does not select these mutants, but the table should be right).
- `batch && !is_json` → `||`: for a single JSON append this is `false || false`, so no 405 happens. The killer is "a single append on a bytes stream gets 405" in `product_append_and_append_many`.
- `body.len() + 2` → `-` in `Vec::with_capacity` is not equivalent: it underflows and panics in debug for a 1-byte JSON body (`1`). Only `*` is equivalent.

**C9 (policy: an admission change needs a property).** RUST-QUALITY's trigger table requires, for admission changes, "at least 1,024 cases for each affected property". C2 adds a second site that decides the same admission. Its correctness rests on `(wire_body.len(), count)` equalling the core's `(command.body.len(), json_entries(body,false).len())` (`content.rs:108`, `creation.rs:251-266`), and R1/R4/R5 pin that only by example. Add a 1,024-case proptest over generated JSON values/arrays/bytes bodies (a `proptest!` needs a macro-dsl owners.json row, like the one for `usage/runtime_tests.rs`), or record why P3 suffices as an explicit decision.

**C10: missed docs.**
- C2: `docs/append-transitions.md` step 2 should say that the product adapter decides the same capacity refusal (`src/product/append_body.rs`) before project admission.
- C2: `docs/review-usage-ownership-evidence.md:12-14` lists the users of the runtime usage handle ("the product final-record capacity check"). Add "and the product append body contract".
- C3 §2.4(f): also say that a superseded owed-final retry now performs `renew_ttl` (`append.rs:282-289`) before its 409 `Sealed`. Previously the renewal refused it first.

**C11: R6 details.**
- The raw PUT must carry `content-type: application/json`, because the body is a JSON array.
- Arm the failpoint through `fixture_failpoints::FailpointGuard` so that a panic cannot leave "owedcap" armed.
- Rig 2 is an `HttpRig`: use `rig.state`/`rig.addr`, and end with `engine_shutdown(&rig.state)` so the test stays bounded.
- Control (b)'s expected 503 is plausible (`raw_close.rs:68`, where `close.generation.ok_or(InvalidClaim)?` → `SealIncomplete`). But the shard receives `seal_gen: None` first (`append.rs:379`), so record the actual line rather than asserting it.

**C12: minor.**
- `AppendConflict::Capacity` puts a non-conflict detail in a field named `conflict`. That is acceptable, since the doc line says so, but a reviewer under the pinned skill may ask for a neutral name.
- The product seal pre-check (`product.rs:1743`) also refuses before duplicate recognition for producer finals. That is pre-existing; add it to §8 next to D4.

**Verdict: ready-with-corrections.**
- C1 and C2 change C2's design and decision list: a producer-gated refusal with a skipped debit, and D5.
- C4 and C5 fix an unbuildable control and a missing refactor pin.
- The rest are cite, doc and hygiene fixes.

The C1 (typed refusal, stable code, details) and C3 (renewal after validation) designs, their reds, the ratchet accounting and the ceilings hold as written.
