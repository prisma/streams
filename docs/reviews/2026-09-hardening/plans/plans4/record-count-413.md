# Review item 25 — Record count over bucket capacity returns 429 + Retry-After forever; limit config unvalidated

Repo: `/Users/sorenschmidt/code/streams`, branch `slate` (HEAD `8dabca7f`), work lands directly on slate.
All line numbers below are from the current tree (read 2026-09-23), not the reviewer's.

---

## 1. Problem (verified)

### 1a. Over-capacity RECORD counts on an ordinary content append are a permanent 429

`src/application/append/admission.rs:19-41` — the 413 arm inside `admit_usage` exists for `Bytes` only:

```rust
            Err(hit) => {
                crate::usage::note_limit_refusal(&hit);
                let l = usage.limits();
                if matches!(hit, crate::usage::LimitHit::Bytes { .. })
                    && body_bytes as f64 > l.bytes_per_sec * l.burst_secs
                {
                    // Larger than the bucket's CAPACITY: no retry can
                    // ever admit it — that is 413, not 429.
                    return fail(
                        FailureClass::Invalid,
                        AppendCode::PayloadTooLarge,
                        "request exceeds the per-stream ingest capacity",
                    );
                }
                return fail(
                    FailureClass::Capacity,
                    AppendCode::RateLimited(hit.code()),
                    &hit.message(l),
                )
                .map_err(|e| e.retry(hit.retry_ms().div_ceil(1000).max(1)));
```

`src/application/append/content.rs:101-119` — the static owner check (`permanently_unadmittable`, bytes AND records) runs only under `close`:

```rust
    let close_carries_content = !entries.is_empty();
    // A body larger than the ingest bucket's CAPACITY can never be
    // admitted — that is a permanent 413, and it must be decided BEFORE
    // the lifecycle intent, or the collection is left sealing forever
    // owing a record the limiter will always refuse.
    if close && close_carries_content && deferred.is_none() {
        // Bytes AND records: a batched close with more records than the
        // record bucket can ever hold is just as permanently refused as
        // an oversized body, and publishing an intent for it stranded
        // the collection at 429 forever.
        if let Some(kind) = usage.permanently_unadmittable(body.len() as u64, entries.len() as u64)
        {
            return fail(
                FailureClass::Invalid,
                AppendCode::PayloadTooLarge,
                &format!("request exceeds the per-stream ingest {kind} capacity"),
            );
        }
    }
```

`src/usage.rs:318-339` — the owner check itself (note the third, `requests`, arm that decides a CONFIGURATION fact per append):

```rust
    /// Permanent capacity check used before publishing lifecycle intent.
    pub(crate) fn permanently_unadmittable(&self, bytes: u64, records: u64) -> Option<&'static str> {
        let l = self.limits();
        if l.bytes_per_sec > 0.0 && bytes as f64 > l.bytes_per_sec * l.burst_secs {
            return Some("bytes");
        }
        if l.recs_per_sec > 0.0 && records as f64 > l.recs_per_sec * l.burst_secs {
            return Some("records");
        }
        // The REQUEST bucket too: rates are floats, so a configuration like
        // 0.1 req/s over a 2 s burst holds 0.2 tokens and can never admit
        // the one token every request costs. Publishing an intent against
        // that leaves the collection sealing behind a permanent 429.
        if l.reqs_per_sec > 0.0 && l.reqs_per_sec * l.burst_secs < 1.0 {
            return Some("requests");
        }
        None
    }
```

Consequence at defaults (`LIMIT_RECS_PER_SEC=5000`, `LIMIT_BURST_SECS=2`, capacity 10,000): a raw `POST /v1/stream/x` with 10,001 tiny JSON records goes `parse_content` (no `close` → check skipped) → `admit_usage` → `admit_on` (`src/usage.rs:224-228`) → `LimitHit::Records { retry_ms: ceil(1/5000·1000)=1 }` → `retry_ms().max(50)=50` → `.retry(1)` → **429 `limit_records_per_sec`, `retry-after: 1`**. One second later the bucket is back at exactly 10,000 and the same request is refused again — forever. The product surface hides this at defaults only because its batch cap `MAX_BATCH_RECORDS = 10_000` (`src/product.rs:1901`) coincides with the record capacity; with any lower `LIMIT_RECS_PER_SEC` a product batch is 429-forever too.

The tree's own DST test encodes the bug as expected behaviour — `src/dst/tests/admission_maintenance.rs:708-720`:

```rust
    // One request over the record-bucket CAPACITY (5,000/s x 2 s burst)
    // trips the ordinary limiter — the refusal must carry its own code
    // and count under its own counter, never the maintenance one.
    let over: Vec<serde_json::Value> = (0..10_001).map(|n| serde_json::json!({ "n": n })).collect();
    ...
    assert_eq!(st, 429, "over-capacity record burst must 429");
```

History: the `close &&` guard dates from `7604a5cb` (R4, "Validate the whole promise before making it"), when the non-close path's only permanent check was the Bytes arm in the (then http.rs) admission code; the records arm was added under the same `close` guard in R5 (`3a09e802`) and never extended to ordinary appends.

### 1b. Limit configuration is unvalidated

`src/config/validation.rs:652-707` (`ServerConfig::validate`) calls `validate_engine_and_profile`, `validate_identity`, `validate_topology_and_ceilings`, `validate_billing_prerequisites`, `validate_auth_and_keys`, `validate_posture`, `validate_instruments` — none reads `self.admission.limit_*`. `src/config/load.rs:281-292` overlays the four knobs through `env_parse::<f64>` (`load.rs:11-13`: `env.get(k).and_then(|v| v.parse().ok())`), and `src/config/numeric_tests.rs:9-15` already proves the f64 parser accepts `"NaN"`, `"inf"`, `"-inf"`.

Effects on the current tree (traced through `src/usage.rs:194-233` `admit_on`):
- `LIMIT_BURST_SECS=0`: every bucket capacity is 0 → `bucket.bytes < bytes` for any non-empty body → every content append is refused; a close is 413, an ordinary append is 413 (Bytes arm) — the write path is bricked while `/health` is fine.
- `LIMIT_BURST_SECS=NaN`: `(x + dt·rate).min(NaN)` returns `x + dt·rate` (f64::min drops the NaN), so buckets grow without bound and `bytes as f64 > NaN` is false: the limiter is silently OFF.
- `LIMIT_*_PER_SEC=NaN` or negative: `rate > 0.0` is false → that bucket silently OFF.
- `LIMIT_REQS_PER_SEC × LIMIT_BURST_SECS < 1` (e.g. `0.1`×`2`): fresh bucket holds 0.2 request tokens → every content append 429 forever (a close is 413 via the `requests` arm).

Both halves of the item are real.

---

## 2. Contract decision

**Ownership.** Two owners, one invariant each:

1. `ServerConfig::validate` (config owner) proves the *posture*: every limit knob is a finite number, rates are ≥ 0 (0 = bucket disabled), `LIMIT_BURST_SECS` > 0, and every ENABLED bucket holds at least one token (`rate × burst ≥ 1.0`). A posture that cannot admit a 1-byte / 1-record / 1-request append never boots. This is where the current `requests` arm belongs, generalised to all three buckets by one predicate.
2. `UsageService::permanently_unadmittable` (runtime owner) decides only from the *request's own size*: `Some("bytes")` / `Some("records")` iff the body or the record count is larger than a FRESH bucket of an enabled limit. The `requests` arm is removed — it re-decided a configuration fact per append and could only fire under a posture validation now refuses.

**Wire (raw `POST /v1/stream/{name}`, and product `POST /v1/streams/{name}/records[:batch]` through the same `AppendService`):**

| Condition | Before | After |
|---|---|---|
| body bytes > `LIMIT_BYTES_PER_SEC×LIMIT_BURST_SECS`, any content append | 413 `payload_too_large` (raw) / 413 `body_too_large` (product), no retry-after — decided in `admit_usage` AFTER TTL renewal | same status/code, decided in `parse_content` BEFORE TTL renewal and before any lifecycle intent |
| record count > `LIMIT_RECS_PER_SEC×LIMIT_BURST_SECS`, ordinary append | **429 `limit_records_per_sec` + `retry-after`, forever** | **413 `payload_too_large` (raw) / 413 `body_too_large` (product), no `retry-after`**, nothing consumed, no intent |
| record count over capacity, close-with-content | 413 | 413 (unchanged) |
| request fits a fresh bucket but not the current one | 429 `limit_*` + computed `retry-after` | unchanged; now the ONLY way to get a `limit_*` 429, and the wait it names is always honourable |
| `LIMIT_BURST_SECS` ≤ 0 / non-finite; `LIMIT_*_PER_SEC` < 0 / non-finite; enabled `rate×burst < 1` | boots, then bricks or silently disables | refused by `ServerConfig::validate` with an error naming the variable |

Explicit non-changes: no `LimitHit::OverCapacity`, no new `AppendCode`, `permanently_unadmittable` keeps its `Option<&'static str>` (display-only, never matched on), product keeps translating `PayloadTooLarge` to `body_too_large` (pre-existing, `src/product.rs:2412-2417`). `/v1/debug/load` `rate_limit_refusals.limit_*` count TRANSIENT 429s only — the old Bytes-413 also incremented `limit_bytes_per_sec`; after this change a permanent 413 is not a limiter refusal and is not counted (nothing asserts the old behaviour).

**Invariant tying the two owners (the property test):** for every posture `validate_admission_limits` accepts and every `(bytes, records)`, `permanently_unadmittable(bytes, records)` is `Some(kind)` exactly when `admit_append` on a never-seen stream returns `Err(hit)`, and `kind` names `hit`. Because the property is run over VALIDATED postures only, it is also the proof that validation is sufficient: were the one-token rule missing, a fresh bucket would refuse with `Requests` while the owner answers `None`.

---

## 3. Red tests

All red tests use only APIs that exist on the current tree, so they compile today and fail with the messages quoted. Test-body helper (add once, `src/dst/tests/admission_maintenance.rs`, free fn, no `json!` so no macro-dsl ledger row):

```rust
/// `n` one-field JSON records: the smallest batch the raw surface parses.
fn records_body(n: usize) -> Vec<u8> {
    let items: Vec<String> = (0..n).map(|i| format!("{{\"n\":{i}}}")).collect();
    format!("[{}]", items.join(",")).into_bytes()
}
```

### R1 — `src/dst/tests/admission_maintenance.rs`

```rust
/// A content append with more records than the record bucket can EVER hold
/// is a permanent 413 with no Retry-After: a 429 would name a wait no wait
/// can honour (review item 25). The refusal consumes nothing and leaves
/// nothing behind: the stream stays writable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_capacity_record_count_is_a_permanent_413_not_a_429() {
    let (_state, addr) = http_rig(mem()).await;
    let ct = [("content-type", "application/json")];
    let (st, _, _) = hreq(addr, "PUT", "/v1/stream/cap-recs", &ct, b"").await;
    assert!(st == 200 || st == 201);
    // 10,001 records: one more than LIMIT_RECS_PER_SEC x LIMIT_BURST_SECS.
    let (st, headers, body) =
        hreq(addr, "POST", "/v1/stream/cap-recs", &ct, &records_body(10_001)).await;
    let body = String::from_utf8_lossy(&body).to_string();
    assert_eq!(st, 413, "10,001 records never fit a 10,000-record bucket: {body}");
    assert!(body.contains("payload_too_large"), "{body}");
    assert_eq!(headers.get("retry-after"), None, "a permanent refusal names no wait");
    let (st, _, body) = hreq(addr, "POST", "/v1/stream/cap-recs", &ct, &records_body(1)).await;
    assert!(st == 200 || st == 204, "{st}: {}", String::from_utf8_lossy(&body));
}
```

Expected red output on the current tree (first assertion):

```
thread 'dst::dst_tests::admission_maintenance::over_capacity_record_count_is_a_permanent_413_not_a_429' panicked at src/dst/tests/admission_maintenance.rs:<L>:5:
assertion `left == right` failed: 10,001 records never fit a 10,000-record bucket: {"error":{"code":"limit_records_per_sec","message":"stream record limit exceeded: 5000 records/s per stream shard"}}
  left: 429
 right: 413
```
(The current response also carries `retry-after: 1`: `retry_ms()` floors 1 ms at 50, `div_ceil(1000).max(1)` = 1.)

### R2 — `src/dst/tests/admission_maintenance.rs` (product surface, non-default posture)

```rust
/// The product batch surface reaches the same owner: with a record bucket
/// smaller than MAX_BATCH_RECORDS, a batch larger than a fresh bucket is a
/// permanent 413 (the product spelling is `body_too_large`), and exactly the
/// capacity is still admitted afterwards because the refusal consumed nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn product_batch_over_record_capacity_is_413_without_retry_after() {
    let rig = http_rig_build(
        mem(),
        RigRuntime::first(),
        HttpRigOptions {
            admission: Some(crate::config::AdmissionConfig {
                limit_recs_per_sec: 50.0, // x LIMIT_BURST_SECS 2 = 100 records
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await;
    let key = [("prisma-encryption-key", PRISMA_KEY)];
    let (st, _, _) = preq(rig.addr, "PUT", "/v1/streams/cap-batch", &key,
        br#"{"format":{"kind":"json"}}"#).await;
    assert_eq!(st, 201);
    let (st, headers, body) = preq(rig.addr, "POST", "/v1/streams/cap-batch/records:batch",
        &key, &records_body(101)).await;
    let body = String::from_utf8_lossy(&body).to_string();
    assert_eq!(st, 413, "101 records never fit a 100-record bucket: {body}");
    assert!(body.contains("body_too_large"), "{body}");
    assert_eq!(headers.get("retry-after"), None);
    let (st, _, body) = preq(rig.addr, "POST", "/v1/streams/cap-batch/records:batch",
        &key, &records_body(100)).await;
    assert_eq!(st, 200, "exactly the capacity fits a fresh bucket: {}", String::from_utf8_lossy(&body));
}
```

Expected red output:

```
assertion `left == right` failed: 101 records never fit a 100-record bucket: {"error":{"code":"rate_limited","message":"admission or rate limit","retryable":true}}
  left: 429
 right: 413
```

### R3 — `src/config/validation_tests.rs` (aggregator wiring; uses the existing `rejects` helper at `:489`)

```rust
    #[test]
    fn validation_rejects_a_limit_posture_that_can_never_admit() {
        rejects(|_| {}, &[("LIMIT_BURST_SECS", "0")], "LIMIT_BURST_SECS");
        rejects(|_| {}, &[("LIMIT_BURST_SECS", "NaN")], "LIMIT_BURST_SECS");
        rejects(|_| {}, &[("LIMIT_RECS_PER_SEC", "NaN")], "LIMIT_RECS_PER_SEC");
        rejects(|_| {}, &[("LIMIT_BYTES_PER_SEC", "-1")], "LIMIT_BYTES_PER_SEC");
        rejects(|_| {}, &[("LIMIT_REQS_PER_SEC", "0.1")], "LIMIT_REQS_PER_SEC");
    }
```

Expected red output (first line of the test):

```
thread '...validation_rejects_a_limit_posture_that_can_never_admit' panicked at src/config/validation_tests.rs:<L>:
validate() must reject (marker "LIMIT_BURST_SECS")
```

### R4 — `src/usage/runtime_tests.rs` (pins the ownership split; only existing APIs)

```rust
/// The request-token floor is a boot decision (`config::admission_limits`),
/// never a per-append verdict: the runtime owner answers only for the
/// request's own size, so a sub-token request bucket — which validation
/// refuses before any service exists — is not its concern.
#[test]
fn the_runtime_owner_decides_only_the_requests_own_size() {
    let sub_token = crate::config::AdmissionConfig {
        limit_reqs_per_sec: 0.1,
        limit_burst_secs: 2.0,
        ..Default::default()
    };
    let usage = UsageService::new(&sub_token, Arc::new(ManualClock::at(0)));
    assert_eq!(
        usage.permanently_unadmittable(1, 1),
        None,
        "the request-token floor is a boot decision, not a per-append verdict"
    );
    assert_eq!(usage.permanently_unadmittable(10_000_001, 1), Some("bytes"));
    assert_eq!(usage.permanently_unadmittable(1, 10_001), Some("records"));
    assert_eq!(usage.permanently_unadmittable(10_000_000, 10_000), None, "exactly the capacity fits");
}
```

Expected red output:

```
thread 'usage::runtime_tests::the_runtime_owner_decides_only_the_requests_own_size' panicked at src/usage/runtime_tests.rs:<L>:5:
assertion `left == right` failed: the request-token floor is a boot decision, not a per-append verdict
  left: Some("requests")
 right: None
```

### Guards that are green on both trees (not red, required by policy)

- **P1 property** (RUST-QUALITY.md:153, admission change ⇒ ≥1,024 cases), `src/usage/runtime_tests.rs`, written with the fix because it names `validate_admission_limits`:

```rust
proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig { cases: 1024, ..Default::default() })]
    /// Permanent refusal is exactly fresh-bucket refusal, kind for kind, for
    /// every posture validation accepts — including disabled buckets, the
    /// exact capacity, one over it, zero and u64::MAX.
    #[test]
    fn permanent_refusal_is_exactly_fresh_bucket_refusal(
        bytes_rate in 0u32..=20_000, recs_rate in 0u32..=20_000, reqs_rate in 1u32..=2_000,
        burst in 1u32..=3, bytes_pick in 0u8..6, recs_pick in 0u8..6, jitter in 0u64..40_000,
    ) {
        let cfg = crate::config::AdmissionConfig {
            limit_bytes_per_sec: f64::from(bytes_rate), limit_reqs_per_sec: f64::from(reqs_rate),
            limit_recs_per_sec: f64::from(recs_rate), limit_burst_secs: f64::from(burst),
            ..Default::default()
        };
        proptest::prop_assert!(crate::config::admission_limits::validate_admission_limits(&cfg).is_empty());
        // Integer rate x integer burst: the capacity is exact, so cap-1 / cap / cap+1 are exact.
        let pick = |rate: u32, which: u8| -> u64 {
            let cap = u64::from(rate) * u64::from(burst);
            match which { 0 => 0, 1 => cap.saturating_sub(1), 2 => cap, 3 => cap + 1, 4 => u64::MAX, _ => jitter }
        };
        let (bytes, records) = (pick(bytes_rate, bytes_pick), pick(recs_rate, recs_pick));
        let usage = UsageService::new(&cfg, Arc::new(ManualClock::at(0)));
        let fresh = match usage.admit_append(&[7u8; 16], bytes, records) {
            Ok(_) => None,
            Err(LimitHit::Bytes { .. }) => Some("bytes"),
            Err(LimitHit::Records { .. }) => Some("records"),
            Err(LimitHit::Requests { .. }) => Some("requests"),
        };
        proptest::prop_assert_eq!(usage.permanently_unadmittable(bytes, records), fresh);
    }
}
```
  (No casts, so no new `#[expect]`; `reqs_rate ≥ 1` keeps the generated posture valid by construction while the `prop_assert!` on validation kills over-rejecting mutants. Match all three `LimitHit` arms — no `_ =>`. The `_ =>` in the local `pick` closure is over a `u8`, not a domain enum.)

- **U1 unit tests** for the predicate, `src/config/admission_limits.rs` `mod tests` (explicit imports, no glob): `defaults_validate_and_zero_disables_a_bucket`, `non_finite_or_negative_limits_are_refused_by_name`, `an_enabled_bucket_must_hold_one_token` (0.4×2 refused, 0.5×2 accepted, for each of the three names).

- **Rewritten** `debug_load_reports_typed_limiter_and_frame_totals` (green before and after): see §4.

---

## 4. Edits file by file

Budgets: source_gate limit = `min(max(1000, legacy), max(1000, merge-base))`; architecture-gate limit = `max(1000, a7e2070f baseline)`. Nothing here touches a ceilinged file (http.rs 3371/3371, product.rs 4207/4207 stay untouched by design).

| File | now | limit | after (est.) | `#[expect]`-ratcheted fns touched |
|---|---|---|---|---|
| `src/application/append/content.rs` | 121 | 1000 | ~117 | `parse_content` (`excessive_nesting`) — scope SHRINKS |
| `src/application/append/admission.rs` | 59 | 1000 | ~52 | `admit_usage` (`fn_params_excessive_bools`, `too_many_arguments`) — scope SHRINKS |
| `src/usage.rs` | 992 | 1000 | ≤ 993 | none (`permanently_unadmittable` carries no expect; `admit_on`'s expects untouched) |
| `src/usage/runtime_tests.rs` | 94 | 1000 | ~200 | none |
| `src/config/admission_limits.rs` | new | 1000 | ~110 | none |
| `src/config/mod.rs` | 47 | 1000 | 49 | none |
| `src/config/validation.rs` | 961 | 1000 | 963-964 | none |
| `src/config/model.rs` | 501 | 1000 | 503 (optional doc) | none |
| `src/config/validation_tests.rs` | 691 | 1000 | ~702 | none |
| `src/dst/tests/admission_maintenance.rs` | 743 | 1000 | ~830 | none (keep each test < 100 lines; `debug_load…` lands ≈ 76) |
| `docs/refactor/WIRE-MATRIX.md` | — | — | — | — |

Ratchet note (`scripts/quality/source_rules.py:116-201`): every `#[expect(..., reason=...)]` is keyed by `(path, item, kind, reason text)` and its `scope_lines` / `nested_items` / `syntax_facts` may not grow. Both touched expectations lose lines and path/call facts, so no reason text changes. Both stay fulfilled: `parse_content` still nests 5 deep in the content-type block (`content.rs:30-60`); `admit_usage` still has 6 params and 2 bools.

### 4.1 `src/application/append/content.rs` (lines 101-119 → one let-chain)

```rust
    // Anything larger than a FRESH bucket can never be admitted, so it is a
    // permanent 413 for every content append — a 429 would name a wait no
    // wait can honour — and it is decided BEFORE the lifecycle intent, or a
    // close would leave the collection sealing forever, owing a record the
    // limiter will always refuse. A deferred verdict outranks it: the shard
    // still answers a duplicate producer request 204.
    if deferred.is_none()
        && let Some(kind) =
            usage.permanently_unadmittable(body.len() as u64, entries.len() as u64)
    {
        return fail(
            FailureClass::Invalid,
            AppendCode::PayloadTooLarge,
            &format!("request exceeds the per-stream ingest {kind} capacity"),
        );
    }
    Ok(ContentPlan { entries, deferred })
```

Why `deferred.is_none()` alone: `json_entries(body, false)` refuses an empty array, so with `deferred == None`, `entries.is_empty()` ⇔ `close_only` ⇔ `body.is_empty()`, and `permanently_unadmittable(0, 0)` is `None`. The guard therefore equals `admit_usage`'s `!close_only && valid_content`: the check runs for exactly the requests that will be admitted, one predicate instead of three. `close` stays (still feeds `close_only`); `close_carries_content` is deleted.

### 4.2 `src/application/append/admission.rs` (lines 21-41)

Delete the Bytes 413 arm (dead after 4.1: same `usage`, same `body.len()`, same limits, and 4.1 runs first on every path that reaches `admit_append`) and inline `usage.limits()`:

```rust
            Err(hit) => {
                // Every refusal here is transient: parse_content has already
                // answered 413 for anything larger than a fresh bucket, so
                // the wait this names is one the bucket will honour.
                crate::usage::note_limit_refusal(&hit);
                return fail(
                    FailureClass::Capacity,
                    AppendCode::RateLimited(hit.code()),
                    &hit.message(usage.limits()),
                )
                .map_err(|e| e.retry(hit.retry_ms().div_ceil(1000).max(1)));
            }
```

No `crate::http` / `AppState` / `Response` mentions (hard-owner rules, `scripts/architecture-gate.py:100-106`) — unchanged.

### 4.3 `src/usage.rs` (992 → ≤ 993)

- Lines 331-337: delete the `requests` arm and its comment (−7).
- Line 318 doc (1 line) → owner/invariant doc (+5 net):
  ```rust
    /// A request larger than a FRESH bucket can never be admitted, so the
    /// content owner refuses it 413 before any lifecycle intent instead of
    /// a 429 whose Retry-After no wait can honour. Only the request's own
    /// size is decided here: `ServerConfig::validate` has proven every
    /// enabled bucket holds at least one token (`config::admission_limits`),
    /// so the request bucket needs no arm.
  ```
- Module doc line 6 "Rejections are 429s whose error code names the limit that fired." → "Transient rejections are 429s whose error code names the limit that fired; a request larger than a fresh bucket is a permanent 413, and `ServerConfig::validate` refuses a posture whose buckets cannot hold one token." (+2).
- No new statics, no `#[path]` module, no new `mod` line (the property and R4 go into the existing `runtime_tests.rs`, already mounted at `:990-992`).

### 4.4 `src/usage/runtime_tests.rs` (94 → ~200)

Append R4 and P1 from §3. Imports already present: `use super::*;` (legacy-allowed glob, count 1), `ManualClock`, `Arc`. Spell the macro `proptest::proptest! { … }` and `proptest::prop_assert!` / `proptest::prop_assert_eq!` fully qualified (no new glob → no unresolved-glob row). The one new ledger fact is the macro-dsl row (§6).

### 4.5 `src/config/admission_limits.rs` (new, ~110 lines with tests)

```rust
//! Boot-time proof of the per-stream token-bucket posture.
//!
//! `UsageService` refills `LIMIT_*_PER_SEC × LIMIT_BURST_SECS` tokens into a
//! fresh bucket and charges one request, `n` bytes and `m` records per
//! append. A posture whose enabled bucket cannot hold ONE unit refuses every
//! append forever, and a non-finite knob either bricks the write path
//! (`burst = 0`) or silently turns the limiter off (`NaN`): both are
//! configuration facts, so they are refused here, before any service exists,
//! rather than re-decided per append by the runtime owner.

use crate::config::AdmissionConfig;

/// Every problem with the limit posture, each naming its variable. Empty
/// means `UsageService` can admit at least one unit of every enabled resource.
pub(crate) fn validate_admission_limits(a: &AdmissionConfig) -> Vec<String> {
    let mut errors = Vec::new();
    let burst = a.limit_burst_secs;
    let burst_ok = burst.is_finite() && burst > 0.0;
    if !burst_ok {
        errors.push(format!(
            "LIMIT_BURST_SECS={burst} must be a finite number > 0: every per-stream bucket \
             holds rate x burst tokens, so 0 refuses every append and NaN disables every limit"
        ));
    }
    for (name, rate, unit) in [
        ("LIMIT_BYTES_PER_SEC", a.limit_bytes_per_sec, "byte"),
        ("LIMIT_REQS_PER_SEC", a.limit_reqs_per_sec, "request"),
        ("LIMIT_RECS_PER_SEC", a.limit_recs_per_sec, "record"),
    ] {
        if !rate.is_finite() || rate < 0.0 {
            errors.push(format!(
                "{name}={rate} must be a finite number >= 0 (0 disables the bucket)"
            ));
        } else if burst_ok && rate > 0.0 && rate * burst < 1.0 {
            errors.push(format!(
                "{name}={rate} x LIMIT_BURST_SECS={burst} holds {} tokens: a bucket that cannot \
                 hold one {unit} refuses every append forever (raise it, or set 0 to disable)",
                rate * burst
            ));
        }
    }
    errors
}

#[cfg(test)]
mod tests {
    use super::validate_admission_limits;
    use crate::config::AdmissionConfig;
    // U1 tests (§3), each asserting `errors.iter().any(|e| e.contains("<NAME>"))`
    // or `errors.is_empty()`; cover 0 / NaN / -1 / inf per knob, 0.4x2 vs 0.5x2 per rate.
}
```

Lint check: `clippy::all` only; no `float_cmp`/`cast_precision_loss` (pedantic, off); no casts; the `!rate.is_finite() || rate < 0.0` form avoids `neg_cmp_op_on_partial_ord`. rustdoc: backticks only, no intra-doc links to private items.

### 4.6 `src/config/mod.rs` (+2)

`pub(crate) mod admission_limits;` (needed `pub(crate)` because P1 in `usage::runtime_tests` calls it) plus one bullet in the Layout doc list.

### 4.7 `src/config/validation.rs` (961 → 963-964)

In `validate_topology_and_ceilings` (`:779`), right after `validate_body_ceiling` (`:780-782`) and BEFORE the fleet early-return (`:783-788`):

```rust
        f.errors
            .extend(super::admission_limits::validate_admission_limits(&self.admission));
```
Doc of that method (`:775-778`): "…the effective body ceiling …, the per-stream bucket posture, and the effective initial shard count…" (+0/1). The aggregator's "every problem collected" contract is preserved (no early return before it).

### 4.8 `src/config/model.rs` (optional, +2)

Field docs at `:291-298`: `LIMIT_BURST_SECS, default 2 (finite, > 0)`; on each rate: `(finite, >= 0; 0 disables; an enabled bucket must hold >= 1 token — validate())`.

### 4.9 `src/dst/tests/admission_maintenance.rs` (743 → ~830)

1. Add `records_body` (§3).
2. Rewrite `debug_load_reports_typed_limiter_and_frame_totals` (`:684-743`): build the rig with a MANUAL usage clock so refill is zero between bursts, then trip the limiter with a DRAINED bucket, not an over-capacity request:
   ```rust
   let usage = Arc::new(crate::usage::UsageService::new(
       &crate::config::AdmissionConfig::default(),
       Arc::new(crate::runtime::ManualClock::at(0)),
   ));
   let (_state, addr) = http_rig_build(mem(), RigRuntime::first(), HttpRigOptions {
       shard: crate::shard::ShardConfig { shared_usage: Some(usage), ..Default::default() },
       ..Default::default()
   }).await.parts();
   ...
   // Two bursts of 6,000 records against a 10,000-record bucket: the first is
   // admitted (9,999 -> 3,999 left after the 1-record probe), the second is
   // refused TRANSIENTLY — it would fit a fresh bucket — by the ordinary
   // limiter, under its own code and counter, never the maintenance gate.
   let (st, _, _) = hreq(addr, "POST", "/v1/stream/load-t", &ct, &records_body(6_000)).await;
   assert!(st == 200 || st == 204);
   let (st, headers, body) = hreq(addr, "POST", "/v1/stream/load-t", &ct, &records_body(6_000)).await;
   assert_eq!(st, 429, "a drained record bucket must 429");
   assert!(headers.contains_key("retry-after"), "a transient refusal names its wait");
   ```
   Rest of the test (counter increments, no `maintenance_backpressure`) unchanged. This version is green on both trees, so it can land as its own preparatory commit. Dropping the `serde_json::json!` there makes its legacy `macro-dsl` allowance obsolete → prune (§6). (Keeping exactly one `json!` instead avoids the prune; either is fine, prune is the ratchet's intended path.)
3. Add R1 and R2 (§3). `Arc`, `HttpRigOptions`, `http_rig`, `http_rig_build`, `RigRuntime`, `PRISMA_KEY`, `hreq`, `preq`, `mem` are already imported at `:3-10`.

### 4.10 `docs/refactor/WIRE-MATRIX.md`

- `:41` (§1.2 errors): `413 … / payload_too_large (per-stream bucket capacity)` → `payload_too_large (body bytes or record count larger than a FRESH per-stream bucket, LIMIT_{BYTES,RECS}_PER_SEC × LIMIT_BURST_SECS, on every content append; permanent, no retry-after; src/application/append/content.rs)`; and `429 … limit_bytes_per_sec / limit_requests_per_sec / limit_records_per_sec (+computed retry-after, src/usage.rs:236-274)` → `(transient only — the request fits a fresh bucket; +computed retry-after; src/usage.rs admit_on)`.
- `:117` (§2.7 by-status): `413 body_too_large` → `413 body_too_large (declared-length refusal AND the per-stream bucket-capacity 413, no retry-after)`.
- `:121` (§2.8): after `≤ 10,000 records (400 batch_too_large)` add `; a batch larger than the per-stream record bucket (LIMIT_RECS_PER_SEC × LIMIT_BURST_SECS) is 413 body_too_large with no retry-after`.
- §0 or a config note: `LIMIT_BURST_SECS` must be finite and > 0, rates finite and ≥ 0, every enabled bucket ≥ 1 token — refused at boot by `ServerConfig::validate`.

### Commit shape

- **Commit A (prep, green before and after):** 4.9 items 1-2 (`records_body`, `debug_load…` rewrite) + `scripts/test-inventory.py --write` + prune. Message idea: "A limiter refusal is shown by a drained bucket, not by an over-capacity burst".
- **Commit B (the fix):** 4.1-4.8, R1-R4, P1, U1, 4.9 item 3, 4.10, owners row, inventory `--write`. Message idea: "A request larger than a fresh bucket is refused 413 on every append, and a bucket that cannot hold one token is refused at boot".
- No verbatim-move commit is needed: nothing moves; the only new production file is new code.

---

## 5. Mutation-kill analysis

Selection facts (`scripts/quality/verification_plan.py:22-31`, `mutation_owners.py:53-169`): none of `src/usage.rs`, `src/application/append/*`, `src/config/*`, `src/dst/tests/*` is under a `CRITICAL_PREFIXES` entry or a registered owner, so the in-diff mutation leg selects nothing from this change and reports "no experiment". The kill table below therefore (a) is the falsifiable-regression proof RUST-QUALITY.md:158 asks for regardless, and (b) makes it safe to OPT IN by registering two rows (recommended, not required):

```python
    owner('usage', 'src/usage.rs', 'usage::runtime_tests:: usage::tests::'),
    owner('admission_limits', 'src/config/admission_limits.rs', 'config::admission_limits:: usage::runtime_tests::'),
```
(filters are whitespace-split test-name substrings; every selected test is a bounded unit/property test — no HTTP waits, so no TIMEOUT-as-miss risk.)

### 5.1 `permanently_unadmittable` (post-change body, `src/usage.rs`)

| Mutant | Killed by | Case |
|---|---|---|
| fn body → `None` | P1 | any `pick == 3` (cap+1) with rate > 0: `admit_append` → `Err`, owner says `None` |
| fn body → `Some("")` / `Some("xyzzy")` | P1 | any admitted case (`pick ∈ {0,1,2}`): `Ok` vs `Some` |
| `bytes > cap` → `>=` | P1, R4 | `pick == 2` (bytes == cap): bucket admits (`cap < cap` false), mutant says `Some` |
| `>` → `<` / `==` | P1 | `pick == 1` (cap−1): mutant `Some`, bucket `Ok` |
| `bytes_per_sec > 0.0` → `>= 0.0` | P1 | `bytes_rate == 0` (disabled) with bytes > 0: mutant `Some("bytes")`, bucket `Ok` |
| `&&` → `\|\|` (either arm) | P1 | disabled rate (`0 > 0` false, bytes > 0 = cap): mutant `Some`, bucket `Ok` |
| `*` → `+` / `-` / `/` in a cap | P1 | `pick == 2` with rate ≥ 4, burst 2: `rate+burst < cap` so mutant `Some` on an admitted request (`/`,`-` likewise shrink the cap) |
| `Some("bytes")` ↔ `Some("records")` swap | P1 | kind equality against `LimitHit::Bytes` / `Records` |

### 5.2 `validate_admission_limits` (`src/config/admission_limits.rs`)

| Mutant | Killed by |
|---|---|
| body → `Vec::new()` | U1 rejections (burst 0), R3 |
| body → `vec![String::new()]` | U1 `defaults_validate…`, `deterministic_default_configuration_is_valid`, P1 `prop_assert!(…is_empty())` |
| `burst.is_finite()` negated | defaults (rejected) / NaN (accepted) |
| `burst > 0.0` → `>=` | burst 0 accepted → U1/R3 `LIMIT_BURST_SECS=0` |
| `burst > 0.0` → `<` | defaults rejected |
| `&&` in `burst_ok` → `\|\|` | burst NaN: `!finite \|\| NaN>0` … `burst_ok` true → NaN accepted → U1 NaN |
| `!burst_ok` → `burst_ok` | defaults rejected / burst 0 accepted |
| `!rate.is_finite() \|\| rate < 0.0` → `&&` | NaN rate accepted → U1/R3 `LIMIT_RECS_PER_SEC=NaN` |
| `rate < 0.0` → `<=` | rate 0 rejected → U1 zero-disables |
| `rate > 0.0` → `>=` | rate 0: `0×2 < 1` → error → U1 zero-disables |
| `rate * burst < 1.0` → `<=` | 0.5×2 = 1.0 rejected → U1 boundary-accept |
| `< 1.0` → `>` / `>=` | defaults rejected |
| `< 1.0` → `==` | 0.4×2 accepted → U1 sub-token |
| `*` → `+` | 0.4+2 ≥ 1 accepted → U1 sub-token |
| `*` → `/` or `-` | 0.5/2, 0.5−2 < 1 rejected → U1 boundary-accept |
| `errors.push(...)` → `()` (either) | the matching rejection test |
| loop body → `()` | every rate rejection test |

### 5.3 `parse_content` guard (`src/application/append/content.rs`, unregistered; listed for completeness)

- `deferred.is_none()` → `false`: 413 never fires → R1/R2 (429 instead of 413).
- → `true`: a producer request with a deferred verdict AND an over-capacity body would get 413 instead of the deferred answer. Not distinguished by a test in this change (needs a rig with `MAX_RECORD_PAYLOAD_BYTES` set so a producer append can be deferred with non-empty `entries`; `HttpRigOptions` has no such knob). Recorded in §8; only matters if content.rs is ever registered.
- `&& let` chain → dropping the `permanently_unadmittable` call (fn-body `Ok(Default)` is impossible: `ContentPlan` has no `Default`).

### 5.4 `admit_usage` (`src/application/append/admission.rs`, unregistered)

- `!close_only && valid_content` → `false`: no 429 ever → `dst::dst_tests::runtime_usage::r10_append_admission_and_committed_counters_share_only_their_runtime` (expects 429 on the 1-byte follow-up) and the rewritten `debug_load…`.
- → `true`/`\|\|`: a close-only or deferred request would be charged/counted — not distinguished today (pre-existing gap, §8).

### 5.5 `validate_topology_and_ceilings` (`src/config/validation.rs`, unregistered)

- Body → `None`: `validate()` then refuses with "withheld a required value" → `deterministic_default_configuration_is_valid`.
- The `extend` line has no operator to mutate; its omission is R3.

### 5.6 Rewritten `debug_load…` distinguishes transient from permanent

With `ManualClock` there is no refill, so 6,000 + 6,000 is refused deterministically by a drained bucket while both bursts individually fit a fresh one — the test passes before and after, proving the counter/typing contract independent of this fix.

---

## 6. Ledgers (same commit as the change they describe)

| Ledger | Action |
|---|---|
| `docs/refactor/test-inventory.json` | `python3 scripts/test-inventory.py --write` (commit A: changed `debug_load…` hash; commit B: R1, R2 rows). Then `--check`. |
| `docs/quality/owners.json` | add one `macro-dsl` row for the property macro in `src/usage/runtime_tests.rs`. Expected identity (copy the exact `owner`/`syntax` from the `architecture-gate --check` failure, which prints it): `{"category":"macro-dsl","count":1,"owner":"crate::macro(proptest::proptest)","path":"src/usage/runtime_tests.rs","reason":"Per-stream capacity property; the pinned macro generates 1,024 validated bucket postures and request sizes and runs each through the production admission entry against the permanent-refusal owner; it produces test cases, not service state.","syntax":"proptest::proptest"}`. No `global` rows (no new statics), no `effect` rows (no spawns), no `by-path-module` rows (no `#[path]`, no new DST module), no `unresolved-glob` rows (explicit imports only in new code — do NOT write `use super::*;` or `use proptest::prelude::*;` in `admission_limits.rs`). |
| `docs/quality/source-allowances.json` | commit A: the legacy `{"category":"macro-dsl","owner":"crate::debug_load_reports_typed_limiter_and_frame_totals","path":"src/dst/tests/admission_maintenance.rs","syntax":"serde_json::json"}` row becomes obsolete once the `json!` is gone → `cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl && python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl --prune` (prune only removes; "removed source debt cannot return" thereafter). |
| `docs/refactor/architecture-policy.json` | no change: no new file references `crate::http`; new files are outside `src/application/` and `sse_core_files`. |
| `docs/refactor/WIRE-MATRIX.md` | §4.10 rows. |
| `src/dst/tests/README.md` | no change (tests join `admission_maintenance`, already under "Accounting and admission"). |
| `scripts/quality/mutation_owners.py` | optional two rows (§5); if added, `python3 -m unittest discover -s scripts/quality -v` must stay green (`test_mutation_owners.py` checks name/path uniqueness). |
| `docs/refactor/test-scenario-map.json`, `review-mechanisms.json` | no change (no scenario IDs, no `mech::` claims; `r10_append_admission…` untouched). |

---

## 7. Controls

Run in this order; every command's expected outcome is stated.

1. **Red first (current tree, before any production edit).** Add R1-R4 only, then:
   ```
   cargo test --locked --lib -- --exact dst::dst_tests::admission_maintenance::over_capacity_record_count_is_a_permanent_413_not_a_429
   cargo test --locked --lib -- --exact dst::dst_tests::admission_maintenance::product_batch_over_record_capacity_is_413_without_retry_after
   cargo test --locked --lib -- validation_rejects_a_limit_posture_that_can_never_admit
   cargo test --locked --lib -- --exact usage::runtime_tests::the_runtime_owner_decides_only_the_requests_own_size
   ```
   Expected: four failures with exactly the `left: 429 / right: 413`, `validate() must reject (marker "LIMIT_BURST_SECS")`, `left: Some("requests") / right: None` messages from §3. Record the output.
2. **Budgets before/after:** `wc -l src/usage.rs src/config/validation.rs src/application/append/content.rs src/application/append/admission.rs src/dst/tests/admission_maintenance.rs src/usage/runtime_tests.rs` — usage.rs ≤ 1000 (target ≤ 993), validation.rs ≤ 1000 (target 964), all others ≤ 1000; `git diff --stat` must not list `src/http.rs` or `src/product.rs`.
3. **Ratchet:** `python3 scripts/architecture-gate.py --check` → `architecture-gate: OK`; if it reports `macro-dsl … proptest`, paste the identity into owners.json (§6) and rerun. It must NOT report `exception growth` for `parse_content` / `admit_usage` (both scopes shrink) nor `file growth`.
4. **Compiler/lints:** `cargo fmt --all -- --check`; `cargo clippy --locked --workspace --all-targets -- -D warnings` (watch for `unfulfilled_lint_expectations` on `parse_content`/`admit_usage` — must not appear); `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items`.
5. **Green:** the four tests above pass; `cargo test --locked --lib -- usage::` shows `permanent_refusal_is_exactly_fresh_bucket_refusal … ok` (1,024 cases) and `debug_load_reports_typed_limiter_and_frame_totals … ok`; `cargo test --locked --lib -- config::` all ok; `cargo test --locked --lib -- dst::dst_tests::runtime_usage:: dst::dst_tests::seal_recovery::an_impossible_final_never_publishes_an_intent` unchanged ok.
6. **Ledgers:** `python3 scripts/test-inventory.py --write && python3 scripts/test-inventory.py --check`; `python3 scripts/scenario-map-report.py --check`; `python3 scripts/review-evidence.py --check`.
7. **Whole gate:** `scripts/quality.sh` → `QUALITY_OK`. Then `python3 scripts/quality/verification_plan.py --out target/quality-plan` and inspect `plan.json`: with no owner rows, `mutants: false` and `unregistered_mutation_source_files: []` (our files are not critical); with the optional rows, `mutants: true`, `selected_mutation_owners: ["usage","admission_limits"]` → `scripts/quality/mutations.sh` must report zero surviving/timeout mutants.
8. **Full suite as CI runs it:** `cargo test --release -- --skip post_split_throughput_scales 2>&1 | tee /tmp/suite.log && python3 scripts/quality/tests_ran.py /tmp/suite.log --inventory docs/refactor/test-inventory.json --skipped 1`.
9. **Do not claim CI green without** `gh run view` on the pushed commit (memory rule).

---

## 8. Out of scope / follow-ups

- **Typed capacity kind.** `permanently_unadmittable` returns `Option<&'static str>` (display-only; never matched). A `CapacityKind { Bytes, Records }` with `Display` would be cleaner but costs ~12 lines in usage.rs (budget 8) — do it only with a move of `Limits`/`LimitHit`/`permanently_unadmittable` into `src/usage/limits.rs` (verbatim-move commit first).
- **Proof-bearing limits.** `UsageService::new(&AdmissionConfig, …)` still accepts an unvalidated posture (test rigs build configs via `ServerConfig::load` without `validate()`; `shard.rs:1440` and `runtime.rs:230` use `AdmissionConfig::default()`, which is valid). Production is covered because `bootstrap::run` only takes `ValidatedServerConfig`. Making `Limits` constructible only from a validated posture is the RUST-QUALITY "proof-bearing types" shape — a separate item.
- **Deferred-vs-capacity ordering test** (§5.3): a producer append whose verdict is deferred AND whose body is over capacity keeps the deferred answer; adding a `max_record_payload_bytes` knob to `HttpRigOptions` would let a DST test pin it.
- **`admit_usage` guard mutants** (§5.4): a test that a close-only request neither charges nor counts (`requests` counter stays) — pre-existing gap.
- **Ops docs:** `docs/STAGING.md:170-173` and the RUNBOOK limiter section could state the boot rule (`burst > 0`, `rate × burst ≥ 1`), and that `/v1/debug/load rate_limit_refusals` counts transient 429s only.
- **Reviewer's "1,024-case property" is satisfied by P1**; a fuzz corpus is not warranted (no decoder involved).

---

## Skeptic corrections (C1..C13)

Verified 2026-09-23 against the working tree at `df9ff212` (the plan says `8dabca7f`; one verbatim-move commit landed since — every quoted line and every `wc -l` in §4 re-checked and still exact: content.rs 121, admission.rs 59, usage.rs 992, runtime_tests.rs 94, mod.rs 47, validation.rs 961, model.rs 501, validation_tests.rs 691, admission_maintenance.rs 743, http.rs 3371, product.rs 4207). The code-path traces for R1–R4 hold: `execute_once` (`src/application/append.rs:281-306`) runs `parse_content` → `renew_ttl` → `install_intent` → `admit_usage`, with the same `(body.len(), entries.len())` at both sites; `json_entries(body, false)` refuses an empty array (`src/application/creation.rs:238-240`) so the `deferred.is_none()`-only guard is equivalent to `!close_only && valid_content`; retry-after is emitted only when `retry_after` is `Some` (`src/http.rs:72-91`, `src/product.rs:2425-2430`); `hreq`/`preq` lower-case header keys into a `HashMap<String,String>` (`fixture_requests.rs:74-78`, `246-250`) so `headers.get("retry-after") == None` type-checks and means what R1/R2 need; the product batch body is a bare JSON array (`src/product.rs:2126-2158`) and `preq` adds no content-type (none needed); `HttpRigOptions.admission` reaches the rig's `UsageService` (`fixture_http.rs:415-426`); `rejects(mutate, env, marker)` is at `validation_tests.rs:489` and no validator reads `self.admission.limit_*` today, so all five R3 lines are red. `#[expect]` scopes: only `parse_content` (`excessive_nesting`, still fulfilled by the content-type block at depth 5 and the JSON-error block) and `admit_usage` (`fn_params_excessive_bools`, `too_many_arguments`, signature unchanged) — neither carries `unwrap_used`/`expect_used`, so no call-path fingerprints (`source_rules.py:157-190` fingerprints only those two lints); both scopes shrink. Mutation selection: none of the touched paths is under `CRITICAL_PREFIXES` (`verification_plan.py:22-31`) or registered (`mutation_owners.py:53-169`), so the in-diff leg selects nothing — as the plan says.

**Verdict: ready-with-corrections.** No control is unbuildable. Corrections:

### C1 — Do NOT register `src/usage.rs` as a mutation owner (§5, §6 "optional two rows")
`plan_schedule` (`scripts/quality/verification_plan.py:108-125`, "no diff-oriented filter participates") runs a seven-slot nightly rotation (`.github/workflows/rust-quality.yml:70-72`, `mutation_owners.py:287-298`) that mutates every registered owner **whole-file**. Registering `src/usage.rs` (992 lines: billing emission, overflow eviction, lag maps, `note_limit_refusal`, counters) with filters `usage::runtime_tests:: usage::tests::` puts all of it under a zero-survivor requirement on the night its bucket comes up; nothing in this change audits those functions' kill coverage. If opting in, register only `src/config/admission_limits.rs` (≈40 production lines, every branch in the §5.2 table killed by U1/R3/P1). Drop the `usage` row.

### C2 — Third caller of `permanently_unadmittable` is unlisted: `src/product.rs:1743-1755` (product seal, final record)
The plan names content.rs and R4/P1 only. The seal path calls `usage.permanently_unadmittable(fin.len()+2, 1)` and answers 413 `payload_too_large` — removing the `requests` arm changes its verdict for a sub-token request posture (now impossible past boot). No code change needed there and WIRE-MATRIX `:105` ("final record over ingest bucket capacity") stays true, but list it in §2 so the ownership statement is complete. Also `docs/RELEASE-PRODUCT-SURFACE.md:520` records the round-6 fix "`permanently_unadmittable` ignored the request bucket … request capacity is checked too" (`ee36553a`); this change reverses that documented fix by moving the decision to boot — add a one-line supersession note next to that row (or in commit B's message) rather than silently reversing an audit record.

### C3 — RUNBOOK §3.2b is a documented contract this change breaks; move it from §8 into commit B
`RUNBOOK.md:112-136` documents the four knobs with no boot constraint and states "Rejections are 429s with error codes `limit_*` … and a `Retry-After` header". After commit B an over-capacity request is 413 without Retry-After and a sub-token/NaN/zero posture is refused at boot. Three lines: the knob table gains "finite; rate ≥ 0 (0 disables); burst > 0; enabled rate × burst ≥ 1 — refused by `validate()`", and the paragraph gains "a request larger than a fresh bucket is a permanent 413 `payload_too_large` (product: `body_too_large`), no Retry-After". `docs/STAGING.md:170-173` is only a posture listing — leave it.

### C4 — §1b misdescribes the `LIMIT_BURST_SECS=NaN` mechanism
The fresh bucket is built as `rate * burst` = NaN (`src/usage.rs:273-275`, `389-392`); `(NaN + dt·rate).min(NaN)` stays NaN (`f64::min` of two NaNs is NaN), and `NaN < bytes as f64` is false → admitted. The bucket does not "grow without bound"; it is NaN forever. Conclusion (limiter silently OFF, and `bytes as f64 > NaN` false in the owner) is unchanged; fix the sentence so the doc comment in `admission_limits.rs` (which repeats it) is accurate.

### C5 — Predicate gap: finite × finite can overflow to +inf and the limiter is silently OFF again
`LIMIT_BYTES_PER_SEC=1e308` with burst 2 passes `is_finite()` on both knobs, but `rate * burst = inf`: the bucket holds `inf`, `inf < bytes` is never true, the owner's `bytes > inf` is never true — exactly the "silently disables" class the item names. Add to the per-rate check: `else if burst_ok && rate > 0.0 && !(rate * burst).is_finite()` → error naming the variable ("capacity overflows"), and one U1 case (`1e308`). P1 is unaffected (rates ≤ 20,000). Kill: mutant dropping the arm → the new U1 case.

### C6 — §2 overclaims what P1 proves
"Because the property is run over VALIDATED postures only, it is also the proof that validation is sufficient: were the one-token rule missing, a fresh bucket would refuse with `Requests` while the owner answers `None`" — P1 generates `reqs_rate in 1..=2_000` and `burst in 1..=3`, so it never produces a sub-token request posture; with the one-token rule deleted from `validate_admission_limits`, P1 still passes (its `prop_assert!(is_empty())` is trivially true). Sufficiency of the one-token rule is pinned only by U1 `an_enabled_bucket_must_hold_one_token` and R3's `LIMIT_REQS_PER_SEC=0.1` line. Either reword §2, or widen P1 to `reqs_rate in 0u32..=2_000` (0 = disabled, valid) and add `reqs_pick` so the request bucket is also exercised — still never sub-token, so the reword is the honest fix.

### C7 — §5.3 "not distinguished by a test in this change" is wrong: the `deferred.is_none() → true` mutant IS killable with the existing rig
A producer request (producer-id/epoch/seq headers) with NO content-type takes `DeferredErr::BadBody("missing Content-Type")` (`content.rs:33-36`) with `entries` empty, and both trees answer the deferred 400 `invalid_body` (`contract.rs:275`) after the duplicate check. Build the rig with `admission: Some(AdmissionConfig { limit_bytes_per_sec: 50.0, ..Default::default() })` (100-byte cap) and send a 101-byte body: current tree and fixed tree → 400; the mutant → 413. (`state.admission.set_record_ceiling(n)` at `src/admission.rs:441-444` is `cfg(test)` and also available if the record-ceiling route is preferred.) Add it as a green guard in `admission_maintenance.rs` (≈25 lines, file stays < 900); it is the only mutant of the fix's central guard the plan leaves alive. Move the §8 bullet accordingly.

### C8 — §2's "a permanent 413 is not counted under `rate_limit_refusals`" is a stated contract with no test, and cannot be pinned by equality
`note_limit_refusal` is process-global (the current test's own comment at `admission_maintenance.rs:739-741`), and the rewritten `debug_load…` increments `limit_records_per_sec` in the same parallel suite, so an `==` assertion in R1 would flake. State in §2 that the non-counting is structural (the 413 path in `parse_content` never reaches `note_limit_refusal`) and deliberately unasserted, rather than presenting it as a verified wire fact.

### C9 — Placement of the validation call (§4.7)
`validate_topology_and_ceilings` (`validation.rs:775-779`) is documented as the body ceiling + initial shard count; burying the bucket posture there (before the fleet early-return at `:783-788`) works but hides a validator the aggregator's doc (`:648-651`, "every problem is collected") enumerates by method. One line in `validate()` itself between `:664` and `:665` — `f.errors.extend(super::admission_limits::validate_admission_limits(&self.admission));` — costs the same +1/+2 (961 → 963) and keeps the validator list legible. Either is acceptable; if kept in topology, the doc rewrite in §4.7 is mandatory, not "+0/1".

### C10 — `records_body(1)` in R1 proves "consumed nothing" only weakly
A 1-record follow-up always fits. Since the rig's usage clock is `SystemClock` the bucket refills anyway, so the honest evidence that the refusal did not charge the bucket is R2's exactly-the-capacity follow-up (a manual-clock rig). Keep R1's follow-up as "stream stays writable" (the doc comment already says so) and let R2 carry the "nothing consumed" claim; do not add a debug/load counter assertion (C8).

### C11 — Minor citation fixes
`MAX_BATCH_RECORDS` is at `src/product.rs:1902` (not `:1901`). The admission.rs quote spans `:21-40` (the plan says `:19-41`; harmless). `docs/quality/owners.json` fully-qualified proptest rows spell `"owner":"crate::macro(proptest::proptest)"`, `"syntax":"proptest::proptest"` (`owners.json:1593-1612`) — the plan's expected identity matches; the `proptest::prop_assert*!` calls inside the block are not counted separately (`src/rollup/allocation.rs` precedent, one row).

### C12 — Ledger check (nothing missed beyond C3)
test-inventory `--write` (commit A hash change + R1/R2/guard rows; unit tests in the new file and `runtime_tests.rs` are picked up by the same scan); owners.json one `macro-dsl` row (`runtime_tests.rs`); source-allowances `--prune` in commit A (`source_gate.py:64-68` reports "obsolete source allowances" otherwise, and `architecture-gate.py:190-191` runs `source_gate.check()`); WIRE-MATRIX rows as §4.10; README.md unchanged (`admission_*` glob at `README.md:17`); architecture-policy unchanged (no `crate::http`/`product` edge from any touched file; hard-owner regexes at `architecture-gate.py:100-106` are not tripped by the new comment text); scenario map / review-mechanisms unchanged (catalogue-driven, line numbers not checked); mt-lint: `validate_admission_limits(a: &AdmissionConfig)` has no `name`-typed param and no `.stream_ref(` (rules at `src/mt_lint.rs:95-101`). `docs/quality/policy.json` immutables are untouched. Add: RUNBOOK §3.2b (C3), RELEASE-PRODUCT-SURFACE note (C2).

### C13 — Commit-B red-run order caveat
Control 1 says "Add R1-R4 only" on the current tree. P1 and U1 must not be present in that run (they reference `validate_admission_limits`, which does not compile yet) — the plan says so for P1 in §3 but Control 1's command list should say it explicitly, and the rewritten `debug_load…` (commit A) must already be in place or the current `10_001 → 429` assertion at `:720` sits beside R1 asserting `413` for the same request shape.
