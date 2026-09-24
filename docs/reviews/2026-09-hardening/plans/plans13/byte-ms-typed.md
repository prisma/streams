# Item 27: typed byte-time (u128/i128 carried as String, a name-sniffing validator, bypassed decoders)

Tree: `slate` @ 6669d3b5 (ten unpushed commits over `origin/slate` = aaf2baa5, plus the uncommitted `mutation_owners.py` owner rows). None of the files this item touches differs between aaf2baa5 and 6669d3b5. `docs/refactor/test-inventory.json` differs, and C3 rewrites it with `--write` on top of HEAD. This item does not touch `scripts/quality/mutation_owners.py`, so it cannot conflict with the uncommitted edit there.

Source: reviewer item 27 (robustness-maintainability-review.md:932-942). I re-read every cited location and its neighbours on the current tree. Line numbers below are current. The reviewer's numbers are stale: rollup.rs:414-454 is now 423-463, shard.rs:2888-2892 is now 2845-2849, and rollup.rs:591-603 is now 590-615.

**Verdict:** the maintainability problem is real, as are three silent or bypassing decode paths. One claim is **not** real on the current tree: "a malformed decimal becomes 0". No production path can reach it today (§1b). The reviewer's Change is wrong on one point. A plain `ByteMs(u128)` with `""`=0 would re-encode every unwritten field as `"0"`. That changes the customer usage API, every published invoice artifact and a golden test (§2). The buildable alternative keeps the unwritten state inside the type.

| # | Commit | Kind | Can land now? |
|---|---|---|---|
| C0 | Pin the byte-time wire (tests only) | pinning | yes |
| C1 | A late byte-time no correction can carry fails its page | bug fix, red-first | yes |
| C2 | An undecodable pending monthly artifact is logged and stays in the outbox | bug fix, red-first | yes |
| C3 | Byte-time is one typed decimal. The Value walk, the envelope re-encode, the shard ensure and all 16 lax parses go | behaviour + refactor, red-first | yes (no wire change; D1 stays out) |

Why this order:
- C0 lands first so the wire pin is demonstrably green on the String tree before the type changes.
- C1 comes before C3 because the typed field cannot hold a `u128` difference above `i128::MAX`. C3 would otherwise have to invent a conversion rule silently.
- C2 comes before C3 because C3 makes a malformed decimal in a pending row undecodable. The row must already be logged, not silently skipped.

---

## 1. Problem (verified)

### 1a. Seven `String` fields carry a u128/i128, and one validator finds them by key name

The seven carriers (current lines):

| # | Field | Width | Where the bytes go |
|---|---|---|---|
| 1 | `billing.rs:235` `SegmentBillingMetaV1.month_storage_byte_ms: String` | u128 | shard DB `'B'` row |
| 2 | `billing.rs:364` `SegmentSnapshot.storage_byte_ms_month: String` | u128 | `_usage` ledger, shard `'V'` month-final rows |
| 3 | `billing.rs:460` `UsageCorrection.storage_byte_ms_delta: String` | i128 | ledger, month rows' `corrections`, `corr-pending/` outbox, correction artifacts, customer `correctionList[].storageByteMsDelta` |
| 4 | `rollup.rs:98` `SegMonth.storage_byte_ms: String` | u128 | month rows, monthly artifacts |
| 5 | `rollup.rs:167` `CorrTotals.storage_byte_ms_delta: String` | i128 | month rows, aggregates, customer `correctionTotals` (usage.rs:176, :293) |
| 6 | `rollup.rs:213` `FrozenTotals.storage_byte_ms: String` | u128 | month rows, monthly artifacts |
| 7 | `rollup.rs:278` `AggRow.storage_byte_ms: String` | u128 (doc at :275 wrongly says "i128-safe") | name/project aggregates |

The only type-independent rule is `decode_json` (rollup.rs:423-463). It parses every row into a `serde_json::Value` and walks it:
```rust
    fn validate_decimal(key: &str, value: &serde_json::Value) -> anyhow::Result<()> {
        if !key.contains("storage_byte_ms") {
            return Ok(());
        }
        ...
        if key.ends_with("delta") {
            number.parse::<i128>()?;
        } else {
            number.parse::<u128>()?;
        }
```
followed by `validate(&serde_json::from_slice::<serde_json::Value>(raw)?)?; Ok(serde_json::from_slice(raw)?)`. Every validated read therefore parses twice. The width is chosen by the key's spelling. The walk covers every key in every row read through it, including the metric-name keys of `OpsM1.counters`/`gauges_max` (rollup.rs:761, `get_json` in `apply_ops_page`). A future metric whose name contained `storage_byte_ms` would wedge the ops rollup. No such metric exists today (ops.rs:395-460), so that is latent.

Callers of `decode_json`: `read_json` (:419; `month_row`, `name_row`, `project_row`, `get_json`), `stream_segment_states` (:583), `MonthClose::read_page` (close.rs:135), and the envelope re-encode at page.rs:55-56:
```rust
        // Validate the same financial decimal encodings at input and storage boundaries.
        decode_json::<UsageEnvelope>(&serde_json::to_vec(env)?)?;
```
The ledger itself is decoded plainly (billing.rs:1186 `serde_json::from_slice(&body)`). This per-envelope re-serialize, re-parse and walk is the only thing that validates `_usage` input.

The third validator is shard.rs:2845-2849, inside `load_billing_meta`:
```rust
                anyhow::ensure!(
                    meta.month_storage_byte_ms.is_empty()
                        || meta.month_storage_byte_ms.parse::<u128>().is_ok(),
                    "invalid billing byte-time"
                );
```

### 1b. 16 lax parse sites (the reviewer said ~14)

`grep -rn "unwrap_or(0)" src | grep -i "byte_ms\|delta_str"`:

- billing.rs:243 `month_byte_ms`
- rollup.rs:181 and :182 (`CorrTotals::absorb`), :198 (`eff_u128`), :236 (`MonthRow::storage_byte_ms`), :253 (`storage_byte_ms_provisional`), :300 (`AggRow::add_storage`)
- page.rs:366, :367 (`apply_snapshot`) and :436, :437 (`apply_late_snapshot`)
- close.rs:227 (`finalize_row`) and :305 (`CarryPage::segment`)
- totals.rs:69 (`effective`)
- product/usage.rs:195 (`product_usage` name aggregate) and :280 (`project_usage`)

The two `?` parses inside `decode_json` (:437, :439) and the shard `ensure` parse make 19 parse sites in all.

**Can a malformed decimal become 0 today? No.** Every input to those 16 sites passes one of the three validators first:
- Meta rows go through `load_billing_meta`. It is the only production decode of `SegmentBillingMetaV1`; the committer preloads through it at transaction/prepare.rs:70, `drain_once` at billing.rs:872 and the sweep at billing.rs:1989.
- Envelopes go through page.rs:56.
- Rollup rows go through `decode_json`.
- Every write site uses `to_string()` of an integer.

So the sites are latent, not live. Their correctness depends on every future read path remembering to validate.

### 1c. Three decoders bypass the rule, and one of them is silent (all real; each is reachable only through a corrupted row)

- **`pending_artifacts`** (rollup.rs:590-615):
  ```rust
              if parts.len() == 5
                  && let Ok(row) = serde_json::from_slice::<MonthRow>(&kv.value)
  ```
  A pending row whose key does not split into five parts, or whose JSON does not decode, is skipped with no log. It is also excluded from `/operator/billing.json` `pendingArtifacts`, because http.rs:1721 counts `.len()` of this list. Operators never learn about it. A row whose decimal is malformed does decode, and `publish_artifacts` (billing.rs:1319 `serde_json::to_vec(&row)`) publishes the bad text into the immutable invoice artifact. Publication runs hourly (billing.rs:1433-1466), and readiness runs on every GET.
- **`usage_month_finals_page`** (shard.rs:2816 `rows.push((kv.key.to_vec(), serde_json::from_slice(&kv.value)?))`). This finding is not in the reviewer's list. A `'V'` row with a malformed `storage_byte_ms_month` decodes and is appended to the immutable `_usage` ledger by `drain_once`. There, page.rs:56 refuses it on every rollup step, so the rollup cursor is wedged on an immutable page for ever.
- **Reconciliation** (reconciliation.rs:26 `MonthRow`, :55 `AggRow`, both plain `from_slice`). The reviewer says a malformed decimal "becomes 0" here. It does not: `invoice_meters` excludes storage, so the value is ignored. The actual defect is a false `ok: true` on `GET /v1/debug/usage-reconcile` for a row that `month_row`/`project_row` refuse, which means the customer usage API answers 503 for it.

`pending_correction_artifacts` (rollup.rs:654-656) also decodes plainly. It falls back to the key's id when the body does not decode and publishes the stored bytes verbatim, so its output is identical either way (§8).

### 1d. The wire really emits `""`

The task allows keeping `""`=0 only if the current wire emits `""`. It does, in four places:
- **Golden.** golden_tests.rs:466 pins `"month_storage_byte_ms":""` for the default meta row.
- **New segment, first observation.** A new segment's first `advance_storage_clock` (billing.rs:255-261) returns before it touches the field. The first snapshot therefore carries `"storage_byte_ms_month":""` into `_usage`, into `'V'` rows, into `SegMonth` (page.rs:372 clones it) and so into published monthly artifacts.
- **Row with no correction.** Every month row or aggregate with no correction has `CorrTotals::default()`, whose `storage_byte_ms_delta` is `""`. That reaches the customer response as `"correctionTotals": {…,"storage_byte_ms_delta":"",…}` (usage.rs:176, :293) and every monthly artifact.
- **Aggregate touched only by reads.** An aggregate that only reads touched keeps `AggRow.storage_byte_ms == ""`.

`"0"` is emitted as well:
- page.rs:277: the late-read correction.
- billing.rs:284: the month reset.
- Every arithmetic write: `absorb`, `add_storage`, the close carry, `apply_late_snapshot`.

Both spellings of zero are therefore on the wire, and a type that preserves the wire must tell them apart.

---

## 2. Contract decision

**Typed contract: no wire change.** New module `src/billing/byte_ms.rs`. It sits under billing, the owner of the `_usage` schema, and is a new file so that the ceilinged billing.rs does not grow:

```rust
pub(crate) type ByteMs = ByteTime<u128>;      // carriers 1, 2, 4, 6, 7
pub(crate) type ByteMsDelta = ByteTime<i128>; // carriers 3, 5
pub(crate) struct ByteTime<T>(Option<T>);     // Clone, Copy, Debug, Default, PartialEq, Eq
```
The rules:
- **Reading.**
  - `""` reads as `None` and is the unwritten field.
  - A string that parses as `T` (Rust `FromStr`, the grammar `decode_json` used) reads as `Some(v)`.
  - Anything else refuses the whole value (a non-string, `null`, `"bad"`, `"-1"` for u128, or out of range).
- **Writing.**
  - `None` writes `""`.
  - `Some(v)` writes the decimal.
  - Every computed value is `From<T>`, and therefore `Some`.
- **Reading a value.** `get()` returns `unwrap_or_default()`: an unwritten field is zero.

Every current write site produces `Some` exactly where it produces a decimal today, and every copy carries `None` where it carries `""` today. The bytes of rows, ledger records, artifacts and customer responses therefore do not change. C0 pins this (P1, P2), as do the two billing-meta goldens.

**Why the reviewer's `ByteMs(u128)` is wrong.** It re-encodes unwritten zeros as `"0"`. That changes:
- the customer `correctionTotals.storage_byte_ms_delta` on nearly every usage response;
- every monthly artifact;
- `golden_layout4_billing_meta_value_default_json_and_decode`.

It also leaves the reviewer's other instruction ("delete the parse sites") intact. The Option-backed type is equally small (about 50 lines) and keeps the wire.

**Behaviour that changes. It applies only to rows no writer produces:**
- **Plain decoders now apply the rule.**
  - `usage_month_finals_page`: a malformed `'V'` row becomes a read error. The existing drain branch (billing.rs:865-871) logs `month-final read failed (dirty row deferred)` and defers that one dirty row instead of poisoning the ledger.
  - `pending_artifacts`: the row is skipped and logged (C2) instead of published with bad text.
  - Reconciliation: the row is reported as `undecodable month row: <key>` / `undecodable project aggregate: <key>`, the vocabulary it already uses for JSON-invalid rows.
- **Same verdicts, new error texts.**
  - `rollup_step`: the page is refused at `ledger decode: …` instead of at apply.
  - `load_billing_meta`: `invalid billing metadata: invalid digit found in string at line 1 column N` instead of `invalid billing byte-time`.
  - A customer 503 for a corrupt row gains serde's ` at line 1 column N` suffix.
- **Non-canonical spellings.** `"+5"` and `"007"` still read, as today, and are written back as `"5"`/`"7"`. No writer produces them.
- **C1.** The page fails before commit instead of persisting a correction that makes the month row unreadable one page later.

None of this changes a wire shape, a status code or a metric. The `/v1/debug/usage-reconcile` point is listed as FYI in §9.

**Kept out (D1):** exposing a count of undecodable pending artifacts on `/operator/billing.json`.

---

## 3. Red tests and pins

Every test below compiles on the tree it is claimed red or green on: it names only types and functions that exist on both sides, and it builds rows from JSON text. P1, P2, R2 and R3 need no edit in C3, except the planned one-row extension of R2.

### C0 (pins; green before and after C3)

**P1 `rollup::accounting_failure_tests::an_unwritten_byte_time_keeps_its_empty_wire`**, `#[test]`:
- Decode a `MonthRow` from `{"segments":{"0":{"storage_byte_ms":""},"1":{"storage_byte_ms":"0"},"2":{"storage_byte_ms":"340282366920938463463374607431768211455"}},"frozen":{"storage_byte_ms":""}}`. Assert `row.storage_byte_ms() == u128::MAX`.
- Re-encode it with `serde_json::to_value`. Assert `segments.0 == ""`, `segments.1 == "0"`, `segments.2 == "3402…455"`, `frozen.storage_byte_ms == ""` and `corr.storage_byte_ms_delta == ""`.
- Assert `to_value(AggRow::default())["storage_byte_ms"] == ""`.
- Assert `to_value(SegmentBillingMetaV1::default().to_snapshot(false))["storage_byte_ms_month"] == ""`.
- Decode a `UsageCorrection` without the field (`{"account_id":"a","project_id":"p","stream_id":"s","stream_name":"orders","month":"2026-07","reason":"r"}`). Assert that it re-encodes `storage_byte_ms_delta == ""`.

Result: ok on 6669d3b5 (String keeps text) and ok after C3 (None ↔ `""`, Some(0) ↔ `"0"`).

**P2 `rollup::accounting_failure_tests::a_late_read_correction_carries_a_written_zero_byte_time`**, multi_thread:
- `db.put(k_month("2026-07","a","p","s"), br#"{"account_id":"a","finalized_at_ms":1}"#)`.
- `r.apply_page(&[batch(0)], "c0")`, using the file's existing `batch` helper.
- Read the raw month row and assert that `corrections[0].storage_byte_ms_delta == "0"` (page.rs:277) and `corr.storage_byte_ms_delta == "0"` (absorb).
- Read the raw `k_project("2026-07","a","p")` and assert `storage_byte_ms == ""` (a read-only aggregate) and `corr.storage_byte_ms_delta == "0"`.

Trace on 6669d3b5: `apply_read_row` sees `finalized_at_ms` and builds the correction with `"0".into()`. `push_correction` calls `absorb`, which computes `("".parse→0)+("0"→0)` to `"0"`. Both aggregates start at the default, absorb, and keep `storage_byte_ms` `""`. **ok.** After C3 it is ok too, which pins that C3 used `ByteMsDelta::from(0)`, not `default()`, at page.rs:277.

### C1

**R3 `rollup::accounting_failure_tests::a_late_byte_time_no_correction_can_carry_fails_its_page`**, multi_thread:
```text
db.put(k_month("2026-07","a","p","s"),
       {"account_id":"a","finalized_at_ms":1,"segments":{"0":{"usage_version":1,"storage_byte_ms":"5"}}})
late = serde_json::from_str::<UsageEnvelope>(JSON of a segment_snapshot for a/p/s/orders, segment 0,
       usage_version 2, month "2026-07", month_final false, zeros, storage_byte_ms_month = u128::MAX)
before = snapshot(&db)
assert!(r.apply_page(&[late], "c1").await.is_err(),
        "a byte-time difference no correction can carry must fail its page");
assert_eq!(snapshot(&db).await, before);
r.month_row("2026-07","a","p","s").await.expect("the finalized row stays readable");
```

Traced red on C0:
1. page.rs:56 accepts (u128::MAX parses as u128).
2. `apply_snapshot` then sees a finalized row, version 2 > 1 and `month_final` false, and calls `apply_late_snapshot`.
3. `d_ms = u128::MAX-5` is written as correction text `"340282366920938463463374607431768211450"`. `absorb` reads it as i128, fails, and uses `unwrap_or(0)`.
4. The page commits and `apply_page` returns `Ok`.

**Expected red:**
```text
thread 'rollup::accounting_failure_tests::a_late_byte_time_no_correction_can_carry_fails_its_page' panicked at src/rollup/accounting_failure_tests.rs:<assert line>:5:
a byte-time difference no correction can carry must fail its page
test result: FAILED. 0 passed; 1 failed
```
If the assertion were removed, the next `month_row` would fail with `number too large to fit in target type`. That is the wedge. Green after C1: `i128::try_from` refuses, nothing is staged, and the row reads.

### C2

**R2 `rollup::accounting_failure_tests::an_undecodable_pending_artifact_is_logged_and_stays_pending`**, multi_thread. It puts three rows (all `&str`):
- `artifact-pending/2026-07/a/p/good` → `{}`
- `…/a/p/not-json` → `not json`
- `artifact-pending/2026-07/short` → `{}`

It then:
- captures the log with `let log = crate::sse::test_log::ErrorLog::capture();` (the existing thread-local `error`-field recorder);
- calls `let pending = r.pending_artifacts(64).await.unwrap();`;
- asserts that the stream ids equal `["good"]`;
- asserts `assert_eq!(log.causes().len(), 2, "every skipped pending artifact is logged: {:?}", log.causes());`;
- asserts that the `not-json` row is still in the DB.

The log is captured only around the scan, in a multi_thread runtime, so SlateDB's worker-thread events are not recorded. Traced red on C1: `parts.len()==5` is false for `short` and `from_slice` fails for `not-json`, and both are silently skipped. **Expected red:**
```text
assertion `left == right` failed: every skipped pending artifact is logged: []
  left: 0
 right: 2
```

### C3

**R1 `rollup::accounting_failure_tests::every_byte_time_carrier_refuses_a_malformed_decimal`**, `#[test]`. It uses a table `[(&str, fn(&str) -> bool); 7]`, one plain `serde_json::from_str::<T>(..).is_ok()` per carrier, and substitutes the literal into the minimal JSON for that carrier:
- `SegmentBillingMetaV1 {"month_storage_byte_ms":V}`
- `SegmentSnapshot {…all 11 fields…,"storage_byte_ms_month":V}`
- `UsageCorrection {identity,"month","reason","storage_byte_ms_delta":V}`
- `SegMonth {"storage_byte_ms":V}`
- `FrozenTotals {"storage_byte_ms":V}`
- `CorrTotals {"storage_byte_ms_delta":V}`
- `AggRow {"storage_byte_ms":V}`

For each carrier it pushes `"{carrier} refused a decimal"` if `"5"` does not decode (a control, so the JSON is not refused for the wrong reason) and `"{carrier} read a malformed decimal"` if `"bad"` decodes. It then asserts `assert_eq!(wrong, Vec::<String>::new(), "byte-time carriers that disagree with the rollup's rule")`. **Expected red on C2** (each is a `String` field):
```text
assertion `left == right` failed: byte-time carriers that disagree with the rollup's rule
  left: ["SegmentBillingMetaV1 read a malformed decimal", "SegmentSnapshot read a malformed decimal", "UsageCorrection read a malformed decimal", "SegMonth read a malformed decimal", "FrozenTotals read a malformed decimal", "CorrTotals read a malformed decimal", "AggRow read a malformed decimal"]
 right: []
```

**R2 extended in C3.** Add `…/a/p/bad-decimal` → `{"segments":{"0":{"storage_byte_ms":"bad"}}}` and make the count 3. **Expected red on C2**: keys sort `bad-decimal` < `good` < `not-json` < `short`, and the row decodes because its field is a String, so it appears in the list:
```text
assertion `left == right` failed
  left: ["bad-decimal", "good"]
 right: ["good"]
```

**R4 `rollup::reconciliation::tests::a_row_whose_byte_time_does_not_decode_is_not_reconciled`**:
- `check.stream(b"month/2026-07/a/p/s", br#"{"account_id":"a","segments":{"0":{"storage_byte_ms":"bad"}}}"#)`
- `check.project(b"project/2026-07/a/p", br#"{"storage_byte_ms":"bad"}"#)`
- `assert_eq!(check.finish().mismatches, ["undecodable month row: month/2026-07/a/p/s", "undecodable project aggregate: project/2026-07/a/p"])`

Traced red on C2: both rows decode, the stream contributes zero meters, the aggregate serves zero meters, they are equal, and `finish` sees an empty `computed`. **Expected red:**
```text
assertion `left == right` failed
  left: []
 right: ["undecodable month row: month/2026-07/a/p/s", "undecodable project aggregate: project/2026-07/a/p"]
```

**R5 `shard::billing_read_tests::a_malformed_byte_time_in_a_shard_billing_row_is_a_read_failure`**, multi_thread, using the same `ShardEngine::start` rig as r13 (prefix `"r27"`) and `hash = [27; 16]`:
1. Let `meta = SegmentBillingMetaV1 { v: 1, stream_id: "s".into(), month_year: 2026, month_month: 7, ..Default::default() }`, which passes the identity/version and month `ensure`s, so only the byte-time rule can refuse it. Write the `'B'` row as `to_value(&meta)` with `month_storage_byte_ms` set to `"bad"`. Assert `engine.load_billing_meta(hash).await.is_err()` with the message "the billing row still refuses a malformed byte-time". This passes on both trees and pins the deleted `ensure`.
2. Put a valid `'V'` row for 2026-06 from `meta.to_snapshot(true)`. Assert `usage_month_finals_page(hash, 32)` returns 1 row.
3. Put a `'V'` row for 2026-07 with `storage_byte_ms_month = "bad"`. Assert `usage_month_finals_page(hash, 32).await.is_err()` with the message "a month-final with a malformed byte-time must not reach the ledger".
4. Finish with `engine.begin_close();` (no `let _ =` on `db.close()`, so no new `let_underscore_must_use` expectation).

**Expected red on C2:**
```text
thread 'shard::billing_read_tests::a_malformed_byte_time_in_a_shard_billing_row_is_a_read_failure' panicked at src/shard/billing_read_tests.rs:<assert line>:5:
a month-final with a malformed byte-time must not reach the ledger
```

**New owner unit tests** in `src/billing/byte_ms.rs` (`billing::byte_ms::tests::`; the module does not exist before C3). Their imports are explicit, with no glob, so no owners.json row is needed:
- `an_unwritten_byte_time_is_the_empty_zero_and_a_written_zero_is_0`:
  - `to_string(&ByteMs::default()) == r#""""#`
  - `to_string(&ByteMs::from(0)) == r#""0""#`
  - `from_str::<ByteMs>(r#""""#) == ByteMs::default()`
  - `ByteMs::default().get() == 0`
  - `from_str::<ByteMsDelta>(r#""0""#) == ByteMsDelta::from(0)`
- `each_width_reads_exactly_its_own_decimals`:
  - u128 reads `"340282366920938463463374607431768211455"` as `u128::MAX` and `"+5"` as 5.
  - u128 refuses `"340282366920938463463374607431768211456"`, `"-1"`, `"bad"`, `" 1"`, `1` and `null`.
  - i128 reads `"-170141183460469231731687303715884105728"` as `i128::MIN` and `"-5"` as -5.
  - i128 refuses `"170141183460469231731687303715884105728"`, `"bad"` and `-1`.
- `a_written_byte_time_round_trips_as_its_decimal`: for `[0, 1, 12_345_678_901_234_567_890, u128::MAX]`, the wire is `"\"{v}\""` and the value round-trips.

These replace `decimal_validation_preserves_nested_and_legacy_zero_rules` (accounting_failure_tests.rs:148-168), which C3 deletes because it calls the deleted `decode_json`. Where each of its cases goes:
- `""` read as zero: P1 and the owner tests.
- The nested `rows` array: R1, since every carrier is nested somewhere.
- `"-1"` for a delta: the owner tests.
- `"-1"` unsigned, the number `1`, `"bad"` and i128 overflow: the owner tests, plus R1 for `"bad"`.

**Compile-level proofs in C3.**
- `rg -n 'decode_json|validate_decimal|month_byte_ms' src` finds nothing.
- `rg -n 'unwrap_or\(0\)' src | rg 'byte_ms|delta_str'` finds nothing.
- The seven fields have type `ByteMs`/`ByteMsDelta`, so `.parse()` on them no longer compiles, and no lax site can come back.

**Existing pins that must stay green:**
- `golden_tests::billing::golden_layout4_billing_meta_value_json`. Its literal becomes `12_345_678_901_234_567_890_u128.into()`; the assertion is unchanged.
- `golden_layout4_billing_meta_value_default_json_and_decode`, untouched.
- `rollup::tests::rollup_applies_deltas_and_closes_months`. At :648 the assertion becomes `== crate::billing::ByteMsDelta::from(0)`, which is stricter than `.get()` because it tells a written `0` from an unwritten one.
- `r14_*` (pinned; untouched), `r13_*` (pinned; untouched), `billing_meta_is_exact_durable_and_ackable`, `storage_clock_splits_at_month_boundaries`, `signed_corrections_saturate_at_the_invoice_bounds`, `frozen_base_and_corrections_match_served_meters` (its expected `"storageByteSeconds":"0"` is unchanged), and `r09_dirty_and_final_pages_are_bounded_and_partial_ack_preserves_debt` (`to_snapshot(true)` of a default meta carries `""`, which is valid).

---

## 4. Edits, file by file, in commit order

**Line budgets** (the merge base `origin/slate` equals HEAD for all of them):

| File | Now | After C3 |
|---|---|---|
| http.rs | 3,155 | untouched |
| product.rs | 4,205 | untouched |
| shard.rs | 3,186 | **3,181** |
| billing.rs | 2,157 | **2,150** |
| history.rs | 1,713 | untouched |
| auth.rs | 1,676 | untouched |
| registry.rs | 1,492 | untouched |
| sse/feed.rs | 1,165 | untouched |
| fleet.rs | 1,142 | untouched |
| DST `billing_usage.rs` | 961 | 961 (line-neutral) |

No ceilinged file grows, so no verbatim-move commit is needed. Non-ceilinged files:
- rollup.rs: 826 → about 836 (C2) → about 790 (C3).
- page.rs: 475 → 478 → 476.
- accounting_failure_tests.rs: 238 → about 400.
- reconciliation/tests.rs: 165 → 180.
- billing_read_tests.rs: 134 → about 170.
- byte_ms.rs: new, about 130.

**Ratcheted scopes.** I listed every `#[expect]` in each touched file:
- billing.rs :115, :766, :770, :1383, :1936. None is touched.
- rollup.rs, page.rs, close.rs, totals.rs, reconciliation.rs, golden_tests.rs, accounting_failure_tests.rs: none.
- product/usage.rs :22 and :26; rollup/tests.rs :65 and :477; billing/tests.rs :38; DST billing_usage.rs :271. These are handled per commit below.

### C0: `The byte-time wire is pinned: an unwritten field stays the empty zero beside a computed 0`
- `src/rollup/accounting_failure_tests.rs`: add P1 and P2. Paths to `super::MonthRow`, `super::AggRow` and `crate::billing::{SegmentBillingMetaV1, UsageCorrection}` are written inline, so the `use` lines do not change. Long JSON is split with `concat!` so every line stays under 100 columns. No expect scope is involved.

### C1: `A late byte-time no correction can carry fails its page instead of persisting a row the rollup cannot read`
- `src/rollup/page.rs`:
  - :355 becomes `if let Some(correction) = apply_late_snapshot(sm, snap)? {`.
  - :421-426: the return type becomes `anyhow::Result<Option<UsageCorrection>>`. Add a reason sentence to the doc: "A byte-time difference wider than a correction's signed 128 bits cannot come from one month of a 64-bit gauge (31 d × u64::MAX ≈ 4.9e28 < 1.7e38); it refuses the page before any floor moves, so no unreadable correction is staged."
  - :438 becomes `let d_ms = i128::try_from(new_ms.saturating_sub(old_ms)).map_err(|_| anyhow::anyhow!("late byte-time difference exceeds a correction"))?;`, which rustfmt splits over 2-3 lines. It sits before the floor updates at :442-446.
  - :468/:470 become `Ok(Some(c))`/`Ok(None)`.
  - `d_ms > 0` and `d_ms.to_string()` are unchanged: the text is identical for every value up to `i128::MAX`.
- `src/rollup/accounting_failure_tests.rs`: add R3.
- Scopes: none; `apply_snapshot` and `apply_late_snapshot` carry no expect.

### C2: `A pending monthly artifact that does not decode is logged on every scan and stays in the outbox`
- `src/rollup.rs:588-615` `pending_artifacts`:
  - The doc gains "A row whose key or body does not decode is never published and never counted as pending, so every scan logs it; it stays in the outbox for an operator."
  - The loop body becomes:
    ```rust
    let k = std::str::from_utf8(&kv.key).unwrap_or("").to_string(); // unchanged
    let parts: Vec<&str> = k.splitn(5, '/').collect();                // unchanged
    let [_, month, account, project, stream_id] = parts.as_slice() else {
        tracing::error!(key = %String::from_utf8_lossy(&kv.key),
            error = "the key does not name a month, account, project and stream",
            "pending monthly artifact stays unpublished");
        continue;
    };
    match serde_json::from_slice::<MonthRow>(&kv.value) {
        Ok(row) => out.push((kv.key.to_vec(), (*month).to_owned(),
                             format!("{account}/{project}"), (*stream_id).to_owned(), row)),
        Err(error) => tracing::error!(key = %k, %error, "pending monthly artifact stays unpublished"),
    }
    if out.len() >= max { break; } // unchanged
    ```
  - `error!` matches the drain's `month-final read failed` level. The non-UTF-8 key behaviour is kept (`""` → one part → logged, never published under a lossy path). Nesting depth is 3. The return shape is unchanged, so http.rs:1721, billing.rs:1307, rollup/tests.rs and DST billing_controller.rs are untouched.
- `src/rollup/accounting_failure_tests.rs`: add R2.
- Scopes: none.

### C3: `Byte-time is a typed decimal on its string wire; every decoder refuses what the rollup refused, and the name walk, the envelope re-encode and the shard ensure go`

1. **`src/billing/byte_ms.rs` (new).**
   - Module doc (the reason it exists): "One stream-month of byte-milliseconds passes 2^53, so shard billing rows, `_usage` records, rollup rows and published invoice artifacts carry it as a JSON string. A field no write ever set has always carried `""`: it reads as zero and writes back as `""`, so every stored row, record and artifact keeps its bytes. Any other text that is not a decimal of the field's width refuses the whole value it sits in; no reader turns it into zero."
   - The two aliases, and `ByteTime<T>(Option<T>)` with a private field and the doc "`None` is the unwritten field; every computed value, zero included, is `Some` and keeps its decimal".
   - `impl<T: Copy + Default> ByteTime<T> { pub(crate) fn get(self) -> T { self.0.unwrap_or_default() } }`
   - `impl<T> From<T> for ByteTime<T>`
   - `Serialize` for `T: Display`: `None => serialize_str("")`, `Some(v) => collect_str(v)`.
   - `Deserialize` for `T: FromStr, T::Err: Display`: `let text = String::deserialize(d)?;` Empty text gives `Ok(Self(None))`; otherwise `text.parse().map(Self::from).map_err(serde::de::Error::custom)`. `String`, not `&str`, so escaped strings still read, as they did through `String` fields.
   - The `#[cfg(test)] mod tests` from §3.
   - No macro-dsl (no `json!`/`proptest!`), no glob, no static.
2. **`src/billing.rs`** (−7 lines):
   - +2: `mod byte_ms;` and `pub(crate) use byte_ms::{ByteMs, ByteMsDelta};` before `mod read_accumulator;` (:26).
   - :232-235: delete the 2-line doc (its reason now lives on the type), and the field becomes `pub month_storage_byte_ms: ByteMs,` (−2).
   - :242-245: delete `month_byte_ms` and the blank line after it (−4).
   - :272-273 become `self.month_storage_byte_ms = ByteMs::from(self.month_storage_byte_ms.get() + add);` (94 columns, −1).
   - :284 becomes `self.month_storage_byte_ms = ByteMs::from(0);`. A written zero, as today. The next loop iteration always overwrites it, so no test can observe it; I kept the "0" meaning anyway.
   - :316 becomes `storage_byte_ms_month: self.month_storage_byte_ms,` (a Copy; `.clone()` would trip `clone_on_copy`).
   - :363-364: delete the doc; the field becomes `ByteMs` (−1).
   - :458-460: delete the doc; the field becomes `ByteMsDelta` (−1).
3. **`src/shard.rs`**: delete :2845-2849 (the byte-time `ensure`) (−5). `load_billing_meta` carries no expect. The typed `serde_json::from_slice` at :2839 now refuses with `invalid billing metadata: …`. `usage_month_finals_page` (:2816) is unchanged and refuses through the type.
4. **`src/rollup.rs`**:
   - :16 import adds `ByteMs, ByteMsDelta` (rustfmt splits it across lines).
   - :96 and :165: delete the docs. :98 becomes `ByteMs` and :167 becomes `ByteMsDelta`. :213 becomes `ByteMs`. :275-276: delete the wrong doc, and :278 becomes `ByteMs`.
   - :181-183 become `self.storage_byte_ms_delta = ByteMsDelta::from(self.storage_byte_ms_delta.get() + c.storage_byte_ms_delta.get());` (2 lines after rustfmt).
   - :197-198: `eff_u128(base: u128, delta: ByteMsDelta)` with `let d = delta.get();`, plus the doc line "base + a correction's signed byte-time, floored at zero".
   - :236 becomes `.map(|s| s.storage_byte_ms.get())`. :253 becomes `let base = s.storage_byte_ms.get();`.
   - :300-301 become `self.storage_byte_ms = ByteMs::from(self.storage_byte_ms.get() + delta);`.
   - :416-421 `read_json` becomes `Ok(read_bytes(db, key).await?.map(|raw| serde_json::from_slice(&raw)).transpose()?)`.
   - Delete `decode_json` (:423-463).
   - :583 becomes `out.push(serde_json::from_slice(&kv.value)?);`.
   - `pending_artifacts` and `pending_correction_artifacts` need no change: the type does it.
5. **`src/rollup/page.rs`**:
   - Drop `decode_json` from `use super::{…}`. Add `ByteMs, ByteMsDelta` to `use crate::billing::{…}`.
   - Delete :55-56.
   - :277 becomes `storage_byte_ms_delta: ByteMsDelta::from(0),`. This must not be `default()`, and P2 pins it.
   - :366-367 become `let new_ms = snap.storage_byte_ms_month.get(); let old_ms = sm.storage_byte_ms.get();`.
   - :372 becomes `sm.storage_byte_ms = snap.storage_byte_ms_month;`.
   - :436-437 get the same `.get()` pair.
   - :445 becomes `sm.storage_byte_ms = ByteMs::from(new_ms.max(old_ms));`.
   - :466 becomes `storage_byte_ms_delta: ByteMsDelta::from(d_ms),` (d_ms is already i128 after C1).
6. **`src/rollup/close.rs`**:
   - :6-9: drop `decode_json`. :10 becomes `use crate::billing::{BillingIdentity, ByteMs};`.
   - :135 becomes `rows.push((row.key.to_vec(), serde_json::from_slice(&row.value)?));`.
   - :227 becomes `let current = segment.storage_byte_ms.get();`. :230 becomes `segment.storage_byte_ms = ByteMs::from(current + add);`.
   - :238 becomes `storage_byte_ms: ByteMs::from(row.storage_byte_ms()),`.
   - :305-306 get the same `.get()` / `ByteMs::from` pair.
7. **`src/rollup/totals.rs`**:
   - :69 becomes `Some(f) => f.storage_byte_ms.get(),`.
   - :80 becomes `eff_u128(storage, self.corr.storage_byte_ms_delta)`. It is inside the `json!` of `MonthRow::effective`. Its owners.json macro-dsl row (count 1) is unchanged.
8. **`src/product/usage.rs`**:
   - :195 becomes `(a.storage_byte_ms.get() / 1000)` and :280 becomes `let byte_ms = agg.storage_byte_ms.get();`.
   - :302 becomes `eff_u128(byte_ms, agg.corr.storage_byte_ms_delta)`.
   - :176, :190 and :293 are untouched, because `Serialize` gives the same strings.
   - **Ratchet.** `product_usage` carries a fn-wide `#[expect(clippy::unwrap_used)]` (:22-25).
     - Under it, every `call-site` fact is fingerprinted with its whole token stream. `json_ok(&json!({…}))` at :153 encloses :195, so its fingerprint changes. The new key `unwrap_site:ordinary-call:crate::product_usage:<digest>` goes 0 → 1, which fails "accepted exception grew".
     - Nothing else changes: :195 is inside the macro, so there are no path, call or syntax-fact changes; scope_lines are equal; and the second expect (:26, too_many_lines/cast) sees equal counts.
     - **Remedy: re-decide the reason.** It becomes `"product_usage; the month was parsed once at admission, so its two re-parses cannot fail; a fallible re-parse would turn an already accepted request into a spurious error"`. That keeps exactly two `;` and no `"`.
     - The narrowing alternative is §8: remove both `parse_month(..).unwrap()` and delete the expect.
     - `project_usage` has no expect.
9. **`src/rollup/reconciliation.rs`**: no code change; the types do it. `src/rollup/reconciliation/tests.rs`:
   - :160-164 become `eff_u128(1, i128::MIN.into())`, `eff_u128(u128::MAX, 1.into())`, `eff_u128(15, (-5).into())`, `eff_u128(15, 5.into())` and `eff_u128(15, Default::default())`.
   - Add R4.
10. **Tests (no scope growth):**
    - `src/rollup/accounting_failure_tests.rs`: delete :148-168; :177 becomes `7.into()`; add R1; extend R2.
    - `src/rollup/tests.rs`:
      - :86 and :193 become `byte_ms.into()`. :86 is inside the `snap` fixture's `too_many_arguments` scope (:65). There, `byte_ms.to_string()` has 3 facts (path, method-call, method-call-site) and `byte_ms.into()` has the same 3, so the count is equal.
      - :121 and :364 become `0.into()`.
      - :160 and :166 become `assert_eq!(x.storage_byte_ms.get(), 31 * day * total_gauge)`, and :470 does the same with `31 * day * 100`.
      - :510 becomes `assert_eq!(proj.storage_byte_ms.get(), 9000)` (inside a macro).
      - :606 becomes `storage_byte_ms_delta: 0.into(),` (2 facts to 2).
      - :648 becomes `assert_eq!(c.storage_byte_ms_delta, crate::billing::ByteMsDelta::from(0))` (inside a macro).
      - :510, :606 and :648 sit inside `rollup_applies_deltas_and_closes_months`'s `too_many_lines` scope (:477). All three are line-neutral and fact-neutral.
    - `src/billing/tests.rs`:
      - :62 and :65 become `jul_final.month_storage_byte_ms.get()` / `m.month_storage_byte_ms.get()`. They are inside `assert_eq!`, within the `cast_sign_loss` scope at :38, so macro tokens only change.
      - :102 becomes `0.into()`.
    - `src/golden_tests.rs:449` becomes `month_storage_byte_ms: 12_345_678_901_234_567_890_u128.into(), // u128 as string`.
    - `src/dst/tests/billing_usage.rs`:
      - :390 becomes `assert_eq!(fsnap.storage_byte_ms_month.get(), expect);` (a macro).
      - :426 becomes `let oct_ms: u128 = closed.month_storage_byte_ms.get();`. Its facts go path `closed` + method-call + site (3) to path `closed` + method-call + site (3).
      - Both are in `billing_meta_is_exact_durable_and_ackable`'s `too_many_lines` scope (:271). Both are line-neutral, so the file stays at 961 lines.
    - `src/shard/billing_read_tests.rs`: add R5. The file is `#![cfg(test)]`, so the planner classes it as production-unchanged. It needs no owner row even though it sits under `src/shard`.
11. **clippy.**
    - `clone_on_copy` is avoided at billing.rs:316 and page.rs:372.
    - No fn crosses 100 lines in either direction.
    - No new `bool` parameter, and no nesting beyond 4.
    - No `unwrap` in production.
    - The `private_interfaces` lint is not triggered: `ByteTime` is nominally `pub(crate)`, the same as its aliases.

---

## 5. Mutation analysis

Critical or registered files by commit:
- **C0, C1 and C2** touch only non-critical files: rollup.rs, rollup/page.rs, rollup/accounting_failure_tests.rs. The prefixes are only `src/rollup/allocation` and `src/rollup/storage`, and none of these files is registered. So `plan.json` has `mutants: false`, and no experiment is claimed.
- **C3** touches exactly one executable critical file: `src/shard.rs` (owner `shard`, filter `shard::`).
  - `src/shard/billing_read_tests.rs` is `#![cfg(test)]`, so it lands in `production_unchanged_files` and is excluded.
  - Every other touched file is outside the prefixes and unregistered. That includes `src/billing/byte_ms.rs`: `src/billing/read_accumulator|read_spool` do not match it.
  - `properties_fuzz: false` and `miri: false` (no codec or buffer prefix).

**shard.rs in-diff.** The only hunk is the 5-line deletion inside `load_billing_meta`. The `ensure!` operators sit inside macro tokens, which cargo-mutants never mutates, and the `.map(|v| {…})` closure gets no FnValue mutant. That leaves the function's own FnValue mutants, whose span (first to last body statement, starting about :2826) covers the lines before and after the deletion:

| Mutant | Killed by |
|---|---|
| `src/shard.rs:~2826:9: replace ShardEngine::load_billing_meta -> anyhow::Result<Option<crate::billing::SegmentBillingMetaV1>> with Ok(None)` | **R5** (`load_billing_meta(..).is_err()` becomes false). Also `r13_failed_accounting_reads_preserve_group_and_newer_dirty_version`: the injected fault check lives in the replaced body, so the group succeeds and "no group success on required read failure" panics on its first iteration. |
| `… with Ok(Some(Default::default()))` | the same two tests, for the same reasons |

Both tests are under `shard::`. Neither mutant can hang: the group commits or fails immediately. If cargo-mutants 27.1.0 selects nothing for a deletion-only hunk, the driver reports zero selected mutants for the registered owner `shard`. The policy accepts that outcome, and no experiment is claimed.

**No owner-row change.** `byte_ms.rs` needs no row. Registering it would add almost nothing: the only viable mutant would be `get → Default::default()`, because the other FnValue replacements need `T: Default`, which the bounds on `From` and `Deserialize` do not provide, so they are unviable. The unit tests and goldens pin both branches. The uncommitted `offsets`/`segmap`/`telemetry_batch` rows are unaffected.

---

## 6. Ledgers

- **C3.** `docs/refactor/test-inventory.json`: run `python3 scripts/test-inventory.py --write`. Only the `function_sha256` of `billing_meta_is_exact_durable_and_ackable` changes. No other DST test is touched; billing_controller.rs's `pending_artifacts` callers keep their shape.
- `docs/refactor/review-mechanisms.json`: no change. The pinned `r14_required_read_failures_…`, `r14_corrupt_watermarks_…` and `r13_failed_accounting_reads_…` bodies are untouched, and the pins are per-function hashes, so the added tests do not disturb them.
- `docs/quality/owners.json`: no change.
  - No new static, glob, by-path module or macro-dsl use.
  - The `macro-dsl` rows (`MonthRow::effective` count 1, `product_usage` count 3, `project_usage` count 1, `mark_*_published`) keep their counts and owners.
  - `tracing::`/`anyhow::`/`concat!`/`format!`/`assert*!` are exempt.
- `docs/quality/source-allowances.json`: nothing is vacated, so no prune.
- `docs/refactor/architecture-policy.json`: no change. byte_ms.rs imports neither `http` nor `product`, and every file stays under the 1,000-line and 200-line function budgets.
- `docs/refactor/WIRE-MATRIX.md`: no change (no wire change).
- Scenario map, dispositions and `src/dst/tests/README.md`: no renames and no new DST modules. The deleted `decimal_validation_…` is a unit test, not in any ledger (checked with `git grep`).
- `scripts/quality/mutation_owners.py`: no change.
- Docs: `docs/review-storage-evidence.md:23` ("Decimal financial fields validate at ingestion and persisted-read boundaries (historical empty decimal zero remains supported)") stays true, now at every decode. No edit needed.

---

## 7. Controls

Run after the mutation and gate runs finish, so there is no CPU contention. `$S` is the scratchpad.

**C0**
1. `cargo test --locked --lib rollup::accounting_failure_tests::` → `test result: ok. 7 passed` (5 existing + P1 + P2).

**C1**
1. Before the fix, with only the test added: `cargo test --locked --lib rollup::accounting_failure_tests::a_late_byte_time_no_correction_can_carry_fails_its_page -- --exact`. Expect the §3 R3 panic and `test result: FAILED. 0 passed; 1 failed`.
2. After the fix: `cargo test --locked --lib rollup::` gives ok. `rollup::accounting_failure_tests::` gives 8 passed.

**C2**
1. Before, test only: `…::an_undecodable_pending_artifact_is_logged_and_stays_pending -- --exact`. Expect `left: 0` / `right: 2`.
2. After: `cargo test --locked --lib rollup::` gives ok, 9 passed in the module. Also `rollup::tests::rollup_applies_deltas_and_closes_months` stays ok: its `x` pending row still publishes and is counted.

**C3**
1. Red run. Add R1, R4, R5 and the R2 extension on the C2 tree before any type edit, then run:
   - `cargo test --locked --lib every_byte_time_carrier_refuses_a_malformed_decimal`
   - `cargo test --locked --lib a_row_whose_byte_time_does_not_decode_is_not_reconciled`
   - `cargo test --locked --lib a_malformed_byte_time_in_a_shard_billing_row_is_a_read_failure`
   - `cargo test --locked --lib an_undecodable_pending_artifact_is_logged_and_stays_pending`

   Expect the four §3 red outputs, each `FAILED. 0 passed; 1 failed`.
2. Green run:
   - `cargo test --locked --lib rollup:: billing:: golden_tests::billing:: shard::billing_read_tests:: shard::bounded_outbox_tests::` gives all ok.
   - `billing::byte_ms::tests::` gives 3 passed.
   - `rollup::reconciliation::tests::` gives 5 passed.
   - `shard::billing_read_tests::` gives 2 passed.
   - P1 and P2 pass unchanged, which is the wire proof.
   - Then `cargo test --locked --release` (the full suite, including DST `billing_usage::billing_meta_is_exact_durable_and_ackable` and `billing_controller::r09_*`) gives ok.
3. `rg -n 'decode_json|validate_decimal|month_byte_ms|parse::<u128>|parse::<i128>' src/rollup.rs src/rollup src/billing.rs src/product/usage.rs src/shard.rs src/dst/tests/billing_usage.rs` finds nothing, except `parse::<…>` inside `byte_ms.rs` (it uses `text.parse()`, so there is none there either). `rg -n 'unwrap_or\(0\)' src | rg 'byte_ms|delta_str'` finds nothing.
4. `wc -l src/billing.rs src/shard.rs src/dst/tests/billing_usage.rs` gives 2150, 3181 and 961.
5. Mutation.
   - Run `git diff HEAD~1 -- src/shard.rs > $S/c3-shard.diff && cargo mutants --list --in-diff $S/c3-shard.diff --file src/shard.rs --package streams-slate`. Expect the two `load_billing_meta` FnValue lines in §5, or none (see §5).
   - Run `python3 scripts/quality/verification_plan.py --out $S/plan-c3`. In the plan, `mutation_source_files` includes `src/shard.rs`, `selected_mutation_owners` includes `shard`, and `production_unchanged_files` includes `src/shard/billing_read_tests.rs`. Other entries come from the unpushed in-flight commits.
   - Run `scripts/quality/mutations.sh`. The owner `shard` reports its load_billing_meta mutants as caught, with no MISSED and no TIMEOUT.
6. `cargo fmt --all -- --check` produces no output. `cargo clippy --locked --workspace --all-targets -- -D warnings` is clean. `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items` is clean.
7. `python3 scripts/test-inventory.py --write`, then `--check` gives OK; `git diff --stat docs/refactor/test-inventory.json` shows 1 changed line. `python3 scripts/review-evidence.py --check` gives OK.
8. `scripts/quality.sh` gives `QUALITY_OK` (python3.11). Before the product_usage reason is re-decided, the ratchet prints exactly one `accepted exception grew without a new decision: ('src/product/usage.rs', 'crate::product_usage', 'function', …clippy::unwrap_used…): unwrap_site:ordinary-call:crate::product_usage:<digest> 0 -> 1`. After the re-decision it prints none.
9. Run CI's plan for the actual push range before pushing, as the memory trap says.

---

## 8. Out of scope (follow-ups)

- **D1: exposing the undecodable-pending count.**
- **`pending_correction_artifacts`** (rollup.rs:654-656): an undecodable correction body silently falls back to the key's id and is published verbatim. The same log-and-hold rule as C2 applies. It is a separate owner and a separate artifact family.
- **`product_usage`'s two `parse_month(&month).unwrap()` re-parses** (usage.rs:109 and :131). Binding `(y, m)` once at the :73 check would delete the fn-wide `unwrap_used` expectation entirely, which is the better narrowing. It is a customer handler refactor, not this item.
- **Unchecked u128/i128 additions:**
  - billing.rs:273, `absorb`, `add_storage`, close.rs:230/:306;
  - `storage_byte_ms_provisional` :257;
  - the u128 `sum()`s.

  They are unreachable magnitudes and unchanged by C3, which keeps `+` verbatim.
- **A 1,024-case round-trip property for `ByteTime`.** It needs a `proptest::proptest` macro-dsl owner row; the unit tests pin both widths' boundaries.
- **Rate-limiting the C2 log.** Publication is hourly; only `/operator/billing.json` polling repeats it.
- **The OpsM1 key-name hazard** disappears with the walk, so nothing is left to do.

---

## 9. Decisions for Søren

- **D1: count undecodable pending monthly artifacts on `/operator/billing.json`?**
  - C2 lands **log-only**. Each skipped row emits one `error!` with `key` and `error` on every scan (hourly publication, plus each readiness GET). Today such a row is invisible: it is neither published nor counted in `pendingArtifacts`.
  - Options:
    - (a) **Backward-compatible (recommended):** add `rollup.undecodablePendingArtifacts: n`. `pending_artifacts` would return the count alongside the rows.
    - (b) Fold the rows into `pendingArtifacts`. That changes the meaning of an existing field.
    - (c) Stay log-only.
  - Cost of (a) or (b):
    - http.rs is at its ceiling (3,155 lines), and the new key adds a line to the `rollup_info` `json!`, so it needs an offsetting deletion or a verbatim move of `billing_readiness_axum`.
    - http.rs is a registered owner (`http`), so the changed readiness lines need killing tests under its filters.
    - The signature change touches rollup/tests.rs and DST billing_controller.rs (a test-inventory update).

**FYI (not decisions; each comes with C3 and cannot be split from the type):**
- `GET /v1/debug/usage-reconcile` now reports a row whose byte-time is malformed as `undecodable month row: <key>` / `undecodable project aggregate: <key>` (`ok: false`) instead of `ok: true`. The shape and vocabulary are unchanged. Only rows the customer read path already refuses with 503 change answer, and no writer produces them. The only backward-compatible alternative would be a second, lax copy of `MonthRow`/`AggRow`.
- Error texts for corrupt rows gain serde's position suffix:
  - the customer 503 `usage_unavailable` message;
  - `invalid billing metadata: …` (was `invalid billing byte-time`);
  - `ledger decode: …` (was the apply-time `invalid digit found in string`).
- Non-canonical decimal spellings (`"+5"`, `"007"`), which no writer produces, still read and are written back canonically.
- C1: a late snapshot whose byte-time difference exceeds `i128::MAX` now stops the rollup at that ledger page, before commit. Today it commits a correction that makes the month row unreadable, and the rollup stops one page later.

---

## Skeptic corrections (C1..Cn)

The correction labels below are separate from the plan's commit names. Commits are called "commit C0..C3" here.

**What I re-verified on 6669d3b5 and found correct.** I checked these against the source and found nothing to correct:
- Every quoted line and line number in §1: billing.rs:235/364/460, rollup.rs:98/167/213/278, decode_json at rollup.rs:423-463, page.rs:55-56, shard.rs:2845-2849, rollup.rs:590-615 and :654-656, reconciliation.rs:26/:55, http.rs:1721.
- The 16 lax sites.
- The four places the wire emits `""` (golden_tests.rs:466; billing.rs:255-261; `CorrTotals::default()`; aggregates touched only by reads).
- Use-site completeness. `git grep` over the whole repo outside docs/ (src/dst, cfg(test), tests/, tools/, fuzz/, bench/, sdk/, contracts/, conformance/) finds no carrier use the plan misses. The quality-invariants harness includes only rollup/storage.rs and rollup/allocation.rs, and neither is touched.
- wc -l of every ceilinged file, and the budgets: billing.rs 2157→2150 (+2 −2 −4 −1 −1 −1) and shard.rs 3186→3181. DST billing_usage.rs stays at 961.
- The `#[expect]` scopes.
- The `product_usage` unwrap_used ratchet. The `json_ok(&json!{…})` call-site fact carries the macro tokens (tools/quality-syntax/src/scan.rs:241-253; source_rules.py:176-180), so exactly one new `unwrap_site:ordinary-call` key appears. A changed reason is a new identity, which `exception_growth` skips (source_rules.py:207-208). The new text satisfies the three-segment regex at source_rules.py:258.
- The P1, P2, R1, R3 and R4 traces.
- R5 step 1 is green on both trees.
- The shard.rs in-diff mutants: the `load_billing_meta` FnValue mutants are killed by R5 under `shard::`.
- `billing_read_tests.rs` is `#![cfg(test)]`, so it lands in `production_unchanged_files`.
- The test-inventory impact is exactly one `function_sha256`. It needs `--write` only, because CI runs `--check` without adaptations.
- The review-mechanisms pins are per-function (review-evidence.py:178-181), and their bodies are untouched.
- `pending_correction_artifacts` output is path-identical whether or not the body decodes (billing.rs:1336 applies the same `/`→`~` as page.rs:157).

**C1 (unbuildable control).** §7 commit C3, step 2 runs `cargo test --locked --lib rollup:: billing:: golden_tests::billing:: shard::billing_read_tests:: shard::bounded_outbox_tests::`. `cargo test` accepts one positional TESTNAME, so the extra filters are rejected as unexpected arguments. The fix is `cargo test --locked --lib -- rollup:: billing:: golden_tests::billing:: shard::billing_read_tests:: shard::bounded_outbox_tests::`. Libtest takes several filters after `--`. Keep the per-module counts as the proof that the selection ran (RUST-QUALITY.md, "must prove that it ran them").

**C2 (unbuildable control).** §7 commit C3, step 3 greps `parse::<u128>` across `src/dst/tests/billing_usage.rs` and expects no hits. src/dst/tests/billing_usage.rs:530-536 parses the customer `storageByteSeconds` response string with `.parse::<u128>()`. That parse is legitimate and stays, so the control cannot report "nothing". The fix: drop `billing_usage.rs` from the `parse::<…>` alternation, keeping it only for `month_byte_ms`, or accept exactly that one hit.

**C3 (edge-visible changes filed as FYI; the task requires decisions).** §9 lists two effects of commit C3 as FYI. Both are visible at the product or debug edge, so per the task they are decisions for Søren, kept out of what lands now.
- **Debug reconcile.** `GET /v1/debug/usage-reconcile` (reconciliation.rs:26/:55) moves from `ok:true` to `ok:false` plus a mismatch line for a malformed-decimal row.
- **Customer 503 text.** The body `message` of the customer 503 changes (product/usage.rs:9-17, `usage_unavailable(&error)` → `error.to_string()`). The §2/§9 claim that only serde's position suffix is added is also inaccurate.
  - For a non-string value (`"storage_byte_ms":1`, `null`), the old text is `invalid decimal accounting field` (rollup.rs:431-433). The new text is serde's `invalid type: integer `1`, expected a string at line 1 column N`.
  - For a malformed string, `invalid digit found in string` gains ` at line 1 column N`.

Make these **D2**, recommended accept, and name the backward-compatible alternatives:
- (a) Reconciliation keeps a lax decode: a private `#[serde(default)]` struct with only the invoice meters, or skipping storage fields via `serde(skip_deserializing)` in a local view.
- (b) `read_json` maps a byte-time decode failure back to the old message text.

Commits C0–C2 land now. Commit C3 waits for D2, because the plan itself says these effects cannot be split from the type.

**C4 (R1 fixture precision).** `SegmentSnapshot` has 14 required keys, not "all 11 fields". The flattened `BillingIdentity` contributes 4 required keys (billing.rs:69-76: account_id, project_id, stream_id, stream_name, none with `serde(default)`). The struct's own fields add 10 (billing.rs:353-367, none defaulted). Write the exact literal into the plan. Otherwise the `"5"` control fails red for the wrong reason (`"SegmentSnapshot refused a decimal"`), and the §3 expected red output would be wrong.

**C5 (R2 assertion robustness).** `ErrorLog` (src/sse/test_log.rs:9-47) has no level filter. It records the `error` field of every event emitted on the test thread, and `iter.next().await` polls SlateDB on that thread inside the capture window. The count-only `assert_eq!(log.causes().len(), 2, …)` can therefore be satisfied or broken by an unrelated SlateDB event that has an `error` field.
- **Fix.** Assert the exact captured values, which are Debug-formatted strings. For example, assert that `log.causes()` contains `"\"the key does not name a month, account, project and stream\""` plus one serde error, or filter `causes()` by those texts before counting.
- **Red output.** Update the expected red to match. On the commit C1 tree it is `left: []` against the expected vector.
- **Commit C3 extension.** In the commit C3 R2 extension, compare the 3 filtered causes the same way.

**C6 (policy row for the new decoder; possible missed ledger).** RUST-QUALITY.md's "Codec, index or admission changes" row requires a ≥1,024-case property that exercises the production decoder with valid and malformed data. `ByteTime`'s `Deserialize` becomes the only decoder for every persisted financial decimal. §8 defers the property because it "needs a proptest macro-dsl owner row". There is precedent for exactly that row: owners.json `crate::tests::macro(proptest::proptest)` for src/rollup/storage.rs and src/rollup/allocation.rs.
- **Either add it in commit C3.** The property: `Some(v)` round-trips for any u128 and i128; an arbitrary string decodes iff it is `""` or `T::from_str` accepts it, and re-encodes to `""` or the canonical decimal. Run it with `ProptestConfig::with_cases(1024)`, and add the matching **docs/quality/owners.json macro-dsl row** for `src/billing/byte_ms.rs` in the same commit.
- **Or justify the exemption.** State in §8 why a JSON field type under `src/billing` is not a "codec" change under the policy. The planner will not select `properties_fuzz`, because billing is not a codec prefix, so no gate forces this either way.

**C7 (mutation-scope precision).**
- **The "mutants: false" claim.** §5's "`plan.json` has `mutants: false`" for commits C0–C2 holds only for a per-commit plan. CI compares against the push `before` (aaf2baa5). That range includes the registered `offsets`/`segmap`/`telemetry_batch` owners (the uncommitted mutation_owners.py rows) once they are committed, so the pushed plan has `mutants: true`. Commits C0–C2 still add no mutation source. Say so, so the receipt is not read as contradicting the plan.
- **The "cannot hang" claim.** "Neither mutant can hang" covers only R5 and r13. cargo-mutants runs every `shard::` test against `load_billing_meta → Ok(None)` and `→ Ok(Some(Default::default()))`, so check that no shard test polls for billing state. I checked src/shard/bounded_outbox_tests.rs (no loops). retirement_tests.rs and transaction_tests.rs still need the same check, and control 5 should record the per-mutant durations against the baseline so a TIMEOUT cannot pass unnoticed.

**C8 (read-path pin after the walk is deleted; optional but cheap).** Commit C3 rewires `read_json` (rollup.rs:416-421), `stream_segment_states` (:583) and `MonthClose::read_page` (close.rs:135) from `decode_json` to plain serde. R1 pins the types, but nothing pins that the customer point reads still refuse a malformed decimal once the only test that did (`decimal_validation_preserves_nested_and_legacy_zero_rules`) is deleted. Add one pin to commit C0 or R4 that is green on both trees. `r.month_row(..)` and `r.project_row(..)` over rows holding `"storage_byte_ms":"bad"` both return `Err`, which is the 503 path.

**C9 (ledger note, no edit).** docs/quality/legacy-diagnostics.json:16658 and :2470 (and the -linux twins) still name `crate::SegmentBillingMetaV1::month_byte_ms` and `crate::eff_u128`. They are immutable adoption inventories: policy.json `immutable_sha256`, and gate.py:26-29/:41-44 refuses any edit. `diagnostic-allowances*.json` is empty, so there is nothing to prune. Add a line to §6 so nobody "cleans up" those rows. This is not a missed ledger.

**Verdict: ready-with-corrections.**
- **Unbuildable controls:** C1 and C2, the commit C3 cargo multi-filter and the parse grep.
- **Decisions:** C3 moves the reconcile verdict and the 503 text change into D2. Commits C0–C2 land now; commit C3 waits for D2.
- **Before implementation:** C4 and C5 fix test fixtures and assertions. C6 needs a decision, either the property plus its owners.json row or a stated exemption.
- **Plan text only:** C7 and C9.
- **Optional:** C8.
