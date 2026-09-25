# Item 62: `StreamDesc::epoch_bytes` is a fake Option

Repo `/Users/sorenschmidt/code/streams`, branch `slate`. HEAD = origin/slate = `fb18840d` at planning
time: items 53 and 59 have been pushed since the task text was written, so the merge base for this
item's push is `fb18840d`. Everything below was read on this tree, and every line number is current.

**Short version.** The problem is real, and the core of the reviewer's Change builds. Three parts of
the review are wrong or incomplete:

1. **The reviewer's First step does not enumerate anything.** `StreamDesc: Deref<Target =
   PersistedDescriptor>`, and the DTO has an `epoch_bytes(&self) -> Option<[u8; 16]>` of its own.
   So deleting `StreamDesc::epoch_bytes` makes all 24 `StreamDesc` call sites (5 in production,
   19 in DST) **silently compile** against the DTO's fallible hex re-parse. Deleting the variant then
   produces exactly one error (http.rs:2379:26). The compiler lists the sites only when **both**
   methods are gone (control R0' below: 25 E0599 errors).
2. **"build_fresh uses the known epoch" has a false premise.** In `create_product`'s `build_fresh`,
   `d` is the `PersistedDescriptor` that `fresh_desc` returns. No epoch value is in scope there:
   `fresh_desc` mints the epoch (`service.runtime.epoch()`) and writes only its hex. The version of
   this that builds is C2: `fresh_desc` returns the epoch it minted. That grows `fresh_desc`'s
   ratcheted `too_many_arguments` scope, so its reason must be re-decided. C2 is therefore separable
   and needs Søren's approval.
3. **Two things the review does not mention would break the build or the ledgers:**
   - `delete`'s `#[expect(clippy::too_many_lines)]` becomes unfulfilled. The function drops from
     102 to 94 counted lines, and `unfulfilled_lint_expectations` is denied, so the expectation must
     be deleted in the same commit.
   - Two pinned mechanism hashes in `review-mechanisms.json` and 19 `test-inventory.json` hashes
     change.

This is a pure refactor: there is no wire change and no behaviour change.

---

## 1 Problem (verified, with quotes)

### The fake Option and the method it shadows

`src/registry.rs:442-448` (the review says 441-450):
```rust
    #[expect(
        clippy::unnecessary_wraps,
        reason = "StreamDesc::epoch_bytes; the option is the shape the raw and product readers already match against, and epoch() gives the bare value; unwrapping it here would move every caller's match"
    )]
    pub(crate) fn epoch_bytes(&self) -> Option<[u8; 16]> {
        Some(self.epoch)
    }
    pub(crate) fn epoch(&self) -> [u8; 16] {      // 449, the real accessor
        self.epoch
    }
```
`src/registry.rs:629-632` holds the DTO method of the same name:
```rust
impl PersistedDescriptor {
    pub(crate) fn epoch_bytes(&self) -> Option<[u8; 16]> {
        crate::crypto::unhex(&self.stream_epoch)?.try_into().ok()
    }
```
`StreamDesc` derefs to `PersistedDescriptor` (registry.rs:407-412), and the inherent method wins.

### Why the Option can never be `None`

`StreamDesc`'s only constructor is `TryFrom<PersistedDescriptor>` (registry.rs:420-425), which runs
`validate_descriptor`. That function refuses any descriptor whose epoch is not exactly 16 bytes
(registry.rs:541-545):
```rust
    let epoch: [u8; 16] = crate::crypto::unhex(&d.stream_epoch)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| {
            invalid_descriptor(&d.name, "stream_epoch must be exactly 16 hex-encoded bytes")
        })?;
```
The rest of the type keeps it that way:
- Both fields (`persisted`, `epoch`) are private.
- There is no `DerefMut`.
- `Deserialize` goes through `try_from`.
- `Ok(Self { persisted, epoch })` (registry.rs:424) is the only literal construction anywhere.

The rest of the code already treats `epoch()` as total: `consumer.rs:249` (the application's own
`check_key`), `read_remote.rs:27` (`InternalTarget::of`), `watch.rs:223`, `creation/product.rs:78`,
`creation/raw.rs:87` and `creation/initialization.rs:34`.

### What the Option forces (every use site; `git grep -n "epoch_bytes()" -- src` finds 25)

**Production `StreamDesc` sites (5):**

| Site | Function | Quote | Consequence |
|---|---|---|---|
| src/http.rs:2379 | `check_key` | `let Some(epoch) = desc.epoch_bytes() else { return KeyCheck::BadDescriptor; };` (2379-2381) | forces the variant `BadDescriptor` (http.rs:2362) |
| src/product.rs:3284 | `verify_internal_target` | `if desc.epoch_bytes() != Some(want_epoch) {` | `!= Some(..)` |
| src/application/consumer/deletion.rs:57 | `delete` | `let Some(epoch) = desc.epoch_bytes() else { return Err(failure(FailureClass::Internal, "internal", "bad descriptor", None, true)); };` (57-65) | dead branch 1 (500 internal) |
| deletion.rs:216 | `resume_deletion` | `epoch: cur_desc.epoch_bytes(),` | forces `SweepTarget.epoch: Option<[u8; 16]>` (deletion.rs:316) |
| deletion.rs:273 | `resume_deletion` | `if fresh.epoch_bytes() != Some(epoch) {` | `!= Some(..)` |

Dead branch 2, which the Option in `SweepTarget` forces, is at `deletion.rs:342-350` in
`sweep_segment` (the relayed cross-owner sweep):
```rust
                let Some(stream_epoch) = round_epoch else {
                    return Err((
                        "segment_unavailable",
                        format!(
                            "segment {seg_id}: no incarnation to bind the \
                         relayed sweep to; retry"
                        ),
                    ));
                };
```
Note that `DeletionDebt.epoch` (consumer.rs:331) is already `[u8; 16]`. Only the per-round
`SweepTarget` carried the Option.

**What `KeyCheck::BadDescriptor` flows into.** No code matches the variant by name
(`git grep BadDescriptor` finds only http.rs:2362 and 2380). The wildcard arms absorb it:
- product.rs:2693 `_ =>` in `product_read` answers 400 `missing_key`.
- product/scan.rs:92 `_ =>` in `product_scan` answers 400 `missing_key`.
- product.rs:3561 `_ =>` in `internal_segment_scan` answers 403 `wrong_key`.
- product.rs:1677 `matches!(.., Ok(..))` in `product_seal` answers 403 `wrong_key`.
- dst consumer_generations.rs:332 `_ => panic!`.

None of them has ever received it.

**Production DTO site (1).** src/application/creation/product.rs:98, `create_product`'s `build_fresh`,
where `d: PersistedDescriptor` is fresh from `fresh_desc`:
```rust
            if let Some(epoch) = d.epoch_bytes() {
                use base64::Engine;
                let token = crate::crypto::touch_token(&key, &epoch);
                d.watch_sig_key = Some(
                    base64::engine::general_purpose::STANDARD
                        .encode(crate::crypto::wait_sig_key(&token, &epoch)),
                );
            }
```
This is dead branch 3. `fresh_desc` has just written `stream_epoch: hex(&epoch)`, so the parse
cannot fail. If it ever did fail, a product stream would be created silently **without** its watch
verifier.

**DST `StreamDesc` sites (19), all `desc.epoch_bytes().unwrap()`.** Each is inside the test named:

| Site | Test |
|---|---|
| src/dst/review_security.rs:120 | `r15_bodyless_and_unauthorized_requests_never_poll_the_body` (**sha-pinned**) |
| livefeed_history.rs:388 | `livefeed_connect_already_split_cursor_in_sealed_predecessor` |
| livefeed_history.rs:425 | `livefeed_connect_already_split_cursor_positions` |
| livefeed_history.rs:678 | `livefeed_merge_continuation_in_place` |
| livefeed_swap.rs:353 | `livefeed_split_cursor_decodes_to_segment_local_and_resumes` |
| livefeed_swap.rs:473 | `livefeed_split_shared_subscribers_swap_once_deliver_twice` |
| livefeed_swap.rs:638 | `livefeed_split_seal_before_refresh_drains_then_terminates` |
| livefeed_swap.rs:916 | `livefeed_refresh_installs_after_external_completion` (`old_desc`) |
| persistence_faults.rs:422 | `cut_resume_never_skips_a_durable_record` (`&desc.epoch_bytes().unwrap(),`; **sha-pinned**) |
| producer_protocol.rs:407 | `product_append_and_append_many` |
| producer_protocol.rs:551 | `product_producer_hash_discipline` |
| quota_enforcement.rs:686 | `watch_waits_occupy_the_subscription_pool` |
| reads_product.rs:137 | `product_read_pages_and_binds_cursors` |
| reads_product.rs:544 | `product_scan_is_snapshot_exact` |
| sse_delivery.rs:275 | `product_sse_controls_carry_signed_cursors` |
| watch_observation.rs:324 | `product_watch_wakes_on_matching_append` |
| watch_observation.rs:515 | `watch_capability_respects_project_suspension` |
| watch_observation.rs:626 | `watch_capability_waits_occupy_project_admission` |
| watch_observation.rs:693 | `stale_policy_fails_watch_capabilities_closed` |

Each receiver is a `StreamDesc`: it comes from `registry.get(..).unwrap().unwrap()`, or from
`lf7_split_stream -> StreamDesc` for livefeed_history.rs:388 and 425.

**Outside `src/`.** `scripts/read-experiments/followup/{prepare.py,legacy_workload.rs,transport.rs}`
call `.epoch_bytes().unwrap()`. This is the historical O5 harness. It extracts a *recorded* revision
with `git archive`, and `prepare.py` refuses a modern tree ("Historical O5 harness only").
`docs/quality/verification.json` pins those files. They are unaffected and must not be edited.

The legacy rows (`legacy-diagnostics{,-linux}.json:12080,20823,21061`) are immutable adoption
inventories, sha-pinned in `policy.json`, and stay as they are.

## 2 Contract decision

**Typed contract after the change:**
- A serving descriptor has exactly one epoch accessor, `StreamDesc::epoch() -> [u8; 16]`, and it is
  total by construction.
- `KeyCheck` is `{Ok(StreamKey, [u8; 16]), Missing, Wrong}`.
- `SweepTarget.epoch` is `[u8; 16]`, like the `DeletionDebt.epoch` it is derived from.
- After C2, the name `epoch_bytes` no longer exists. A reintroduced `desc.epoch_bytes()` fails with
  E0599 instead of binding to a fallible re-parse through `Deref`.

**Wire: no wire change, and `docs/refactor/WIRE-MATRIX.md` is untouched.** Each removed branch was
unreachable because of the constructor invariant in §1:
- The consumer DELETE `500 internal "bad descriptor"` (deletion.rs:57-65) was never emitted.
- The relayed-sweep `segment_unavailable "no incarnation to bind"` (deletion.rs:342-350) was never
  emitted.
- `BadDescriptor`, and with it the 400 `missing_key` / 403 `wrong_key` answers the `_` arms gave
  it, was never produced.
- `verify_internal_target` compares the same 16 bytes as before (`Some(e) != Some(w)` ⇔ `e != w`).
- In C2, the watch verifier is still installed from the same minted epoch. The persisted descriptor
  JSON is byte-for-byte the same shape.

**Alternative that keeps the variant (not recommended).** Keep `BadDescriptor` as a defensive arm.
Rejected: it cannot be produced, and if it could, the `_` arms would report a corrupt descriptor to
the client as 400 `missing_key`, which is wrong.

**Synchronization.** Nothing concurrent changes. There is no Loom or held-commit obligation.

## 3 Red tests and pinning tests

**This is a pure refactor.** There is no behavioural red test, because no behaviour changes.
Instead there are compile-level proofs and the existing tests that pin every touched path.

### R0: the reviewer's First step, as written (scratch only; do not commit)

Delete registry.rs:442-448 (the `StreamDesc::epoch_bytes` method and its expectation) and
http.rs:2362 (`BadDescriptor,`). Then run:
```
cargo check --locked -p streams-slate --lib --tests --keep-going --message-format=short 2>&1 \
  | grep -oE '^src/[^:]+:[0-9]+:[0-9]+: error\[E[0-9]+\]' | LC_ALL=C sort -u -t: -k1,1 -k2,2n
```
Expected output (exactly one line: `return KeyCheck::BadDescriptor;` moved from 2380 up to 2379):
```
src/http.rs:2379:26: error[E0599]
```
The full message is ``no variant or associated item named `BadDescriptor` found for enum `KeyCheck` in the current scope``.
None of the 24 `StreamDesc` sites errors: they resolve through `Deref` to
`PersistedDescriptor::epoch_bytes`. This is the proof that the review's First step is wrong.

### R0': the First step that works (scratch only; do not commit)

On a clean tree, delete **both** methods: registry.rs:442-448 and 630-632. Keep the variant. Same
command. Expected output, 25 lines:
```
src/application/consumer/deletion.rs:57:28: error[E0599]
src/application/consumer/deletion.rs:216:29: error[E0599]
src/application/consumer/deletion.rs:273:18: error[E0599]
src/application/creation/product.rs:98:36: error[E0599]
src/dst/review_security.rs:120:22: error[E0599]
src/dst/tests/livefeed_history.rs:388:22: error[E0599]
src/dst/tests/livefeed_history.rs:425:22: error[E0599]
src/dst/tests/livefeed_history.rs:678:22: error[E0599]
src/dst/tests/livefeed_swap.rs:353:22: error[E0599]
src/dst/tests/livefeed_swap.rs:473:22: error[E0599]
src/dst/tests/livefeed_swap.rs:638:22: error[E0599]
src/dst/tests/livefeed_swap.rs:916:26: error[E0599]
src/dst/tests/persistence_faults.rs:422:19: error[E0599]
src/dst/tests/producer_protocol.rs:407:22: error[E0599]
src/dst/tests/producer_protocol.rs:551:22: error[E0599]
src/dst/tests/quota_enforcement.rs:686:22: error[E0599]
src/dst/tests/reads_product.rs:137:22: error[E0599]
src/dst/tests/reads_product.rs:544:22: error[E0599]
src/dst/tests/sse_delivery.rs:275:22: error[E0599]
src/dst/tests/watch_observation.rs:324:22: error[E0599]
src/dst/tests/watch_observation.rs:515:22: error[E0599]
src/dst/tests/watch_observation.rs:626:22: error[E0599]
src/dst/tests/watch_observation.rs:693:22: error[E0599]
src/http.rs:2379:28: error[E0599]
src/product.rs:3284:13: error[E0599]
```
The raw log ends with ``could not compile `streams-slate` (lib) due to 6 previous errors`` and
``(lib test) due to 25 previous errors``. The messages read ``no method named `epoch_bytes` found``
for:
- ``struct `StreamDesc` `` at owned receivers;
- ``reference `&StreamDesc` `` at http.rs and product.rs;
- ``struct `PersistedDescriptor` `` at creation/product.rs.

This list is the exact migration set: C1 fixes the 24 `StreamDesc` sites and C2 fixes
creation/product.rs:98.

### Compile-level proof that stays in the tree

- After C1, `KeyCheck::BadDescriptor` does not exist, so any construction of it fails with E0599.
- `SweepTarget.epoch: [u8; 16]` feeds `InternalTarget.stream_epoch: [u8; 16]` directly. A
  `let Some(..) = target.epoch else` would fail with E0308.
- After C2, `git grep -n epoch_bytes -- src` is empty, and any new `.epoch_bytes()` call is E0599.

### Existing tests that pin every touched path (all stay green, none is edited for semantics)

| Path | Tests |
|---|---|
| Constructor invariant the refactor relies on | `registry::tests::r04_invalid_descriptors_cannot_reach_storage` (`stream_epoch = "00"` and `"z"×32` are refused), `registry::tests::corrupt_descriptor_fails_closed` |
| `check_key` Ok / Wrong via `product_read` | `dst::dst_tests::reads_product::product_read_pages_and_binds_cursors` (200 with the right key; `assert_eq!(st, 403)` with the wrong key), `dst::dst_tests::security_routes::product_requires_the_account_token` ("token ok, key wrong -> 403") |
| `check_key` Ok via product SSE (inside the `http` mutation filter) | `dst::dst_tests::livefeed_engine_retired::livefeed_engine_retired_under_the_same_owner_cuts_a_parked_session`, `..._before_a_drive_is_a_typed_read_cutoff` |
| `check_key` called directly | `dst::dst_tests::consumer_generations::a_stale_delete_retry_cannot_delete_the_replacement_consumer` (`_ => panic!("key check failed")`) |
| `verify_internal_target` epoch comparison | `product::tests::internal_target_accepts_its_own_incarnation`, `product::tests::internal_target_refuses_a_recreated_stream` (409) |
| `delete`: `expect_epoch != epoch` gives TargetGone | `consumer_generations::a_stale_delete_retry_cannot_delete_the_replacement_consumer`, `consumer_saga::a_stale_delete_after_cross_key_recreation_is_untouched_204` |
| `resume_deletion` epoch pin at refresh | `consumer_generations::a_parked_saga_never_touches_a_recreated_stream` |
| `SweepTarget` multi-segment sweep | `consumer_delete::a_split_consumers_deletion_fails_one_segment_then_retries_clean`, `consumer_saga::a_saga_never_finalizes_under_a_pending_transition` |
| Relayed-sweep branch of `sweep_segment` | **No local DST test.** It was validated on cloud (#113 round 2). The change there is type-only (`Some(e)` becomes `e`), and the proof is the compile-level one above. |
| C2: product watch verifier installed at create | `watch_observation::product_watch_wakes_on_matching_append`, `quota_enforcement::watch_waits_occupy_the_subscription_pool`, `review_security::r15_...` (each signs with `wait_sig_key(touch_token(key, epoch), epoch)`; without `watch_sig_key` the capability is refused at watch.rs:231) |
| C2: raw create through `fresh_desc` | `runtime_request_work::r09a_old_ttl_cas_cannot_mutate_or_suppress_a_recreated_incarnation`, `r09a_ttl_overflow_returns_retryable_http_refusals_before_append_effects`, and the lifecycle_creation module |

## 4 Edits, file by file, in commit order

No verbatim-move commit is needed. Every ceilinged file shrinks or is edited line-neutrally.

### Line budgets (`wc -l` now; ceiling = merge base `fb18840d`)

| File | Now = ceiling | After C1 | After C2 |
|---|---|---|---|
| src/http.rs | 3,369 | **3,366** (−3) | 3,366 |
| src/product.rs | 4,205 | 4,205 (line-neutral) | 4,205 |
| src/registry.rs | 1,501 | **1,496** (−7, +2 doc) | **1,492** (−4) |
| src/shard.rs | 3,196 | untouched | untouched |
| src/billing.rs | 2,201 | untouched | untouched |
| src/history.rs | 1,713 | untouched | untouched |
| src/auth.rs | 1,676 | untouched | untouched |
| src/sse/feed.rs | 1,195 | untouched | untouched |
| src/fleet.rs | 1,143 | untouched | untouched |
| src/application/consumer/deletion.rs | 417 | 396 (−21) | 396 |
| src/application/creation.rs | 390 | 390 | 393 (+3) |
| src/application/creation/product.rs | 178 | 178 | 176 (−2) |
| 10 DST files | ≤ 946 | line-neutral | line-neutral |

### C1: "A serving descriptor has one epoch and it is never absent: the fake Option and its branches are gone"

1. **src/registry.rs**
   - Delete 442-448: the `#[expect(clippy::unnecessary_wraps, ...)]` and `StreamDesc::epoch_bytes`.
   - Add a doc to `epoch()` (it has none today). It sits outside the body, and the file is neither
     critical nor registered:
     ```rust
         /// The incarnation epoch `validate_descriptor` decoded when this serving
         /// descriptor was built: it cannot be absent, so no caller branches on it.
         pub(crate) fn epoch(&self) -> [u8; 16] {
     ```
   - Leave `PersistedDescriptor::epoch_bytes` alone; C2 removes it.
2. **src/http.rs**
   - Delete 2362 `    BadDescriptor,`.
   - In `check_key`, replace 2379-2381 with `    let epoch = desc.epoch();`.
   - Nothing else in the file names the variant.
3. **src/product.rs:3284** (`verify_internal_target`, line-neutral):
   `if desc.epoch_bytes() != Some(want_epoch) {` becomes `if desc.epoch() != want_epoch {`.
4. **src/application/consumer/deletion.rs**
   - Delete 16-19, the `#[expect(clippy::too_many_lines, reason = "delete; the fence, the tombstone ...")]`.
     **This is mandatory.** `delete` counts 102 non-blank, non-comment lines in 27..169, braces
     included as clippy counts an `async fn` body. The lint fires today, since the tree is green
     with `-D warnings`. After the let-else replacement in the next bullet it counts 102 − 9 + 1 = **94**, so the expectation would
     be unfulfilled, which is denied. The `too_many_arguments` expectation (12-15, 6 params) stays.
   - Replace 57-65 (the 9-line let-else) with `    let epoch = desc.epoch();`.
   - 216: `epoch: cur_desc.epoch_bytes(),` becomes `epoch: cur_desc.epoch(),`. This is the literal
     translation. It equals the saga's `epoch`, because `cur_desc` is either the targeted
     descriptor or a `fresh` that passed the pin at 273.
   - 273: `if fresh.epoch_bytes() != Some(epoch) {` becomes `if fresh.epoch() != epoch {`.
   - 316: `epoch: Option<[u8; 16]>,` becomes `epoch: [u8; 16],`. The struct doc ("The exact stream
     incarnation ... authorized for one topology round") is now literally true.
   - Delete 342-350 (the relay let-else), and change 353 `stream_epoch,` to
     `stream_epoch: round_epoch,`. `let round_epoch = target.epoch;` at 328 stays.
5. **19 DST sites** (§1 table), each line-neutral:
   - `desc.epoch_bytes().unwrap()` becomes `desc.epoch()`.
   - livefeed_swap.rs:916 becomes `old_desc.epoch()`.
   - persistence_faults.rs:422 becomes `&desc.epoch(),`.
   - rustfmt keeps that `KeyCursor::decode(..)` call vertical, because it is over 100 columns
     joined.
6. **Ledgers in the same commit:** see §6. That means `test-inventory.json` (19 hashes) and
   `review-mechanisms.json` (2 pins).

### C2 (needs Søren's approval, separable): "A fresh incarnation hands back the epoch it minted; the descriptor DTO no longer re-parses its own hex"

1. **src/application/creation.rs**, `fresh_desc` (203-242):
   - Return the pair:
     `) -> (crate::registry::PersistedDescriptor, [u8; 16]) {`,
     then `let desc = crate::registry::PersistedDescriptor { ... };`, then `(desc, epoch)`.
   - Add a doc above the attribute:
     ```rust
     /// A fresh incarnation hands back the epoch it minted, so key material derived
     /// from it (the product watch verifier) never re-parses the hex written here.
     ```
   - **Re-decide the exception reason.** This is the ratchet's required "new decision": the scope
     grows by 3 lines and by 5 syntax facts (2 doc attributes, `u8`, and the `desc` and `epoch`
     paths). The current reason also names "epoch ... and fork parts", which `fresh_desc` does not
     take. New text, with exactly two `;` and no `"`:
     `reason = "fresh_desc; a fresh descriptor is built from the resolved name, key, content type and expiry policy as creation decided them, and returns the epoch it minted; a builder would restate the descriptor's own fields"`
2. **src/application/creation/claim.rs:52 and :87** (`resolve`, line- and fact-neutral, because a
   tuple pattern records no path fact): `let mut fresh = fresh_desc(` becomes
   `let (mut fresh, _) = fresh_desc(`.
3. **src/application/creation/product.rs:88-105** (`build_fresh`, inside `create_product`):
   - `let mut d = fresh_desc(` becomes `let (mut d, epoch) = fresh_desc(`.
   - Delete the `if let Some(epoch) = d.epoch_bytes() {` line and its closing `}`, and dedent the
     5 lines between them. `use base64::Engine;` stays first in that run.
   - The comment "Only this key-bearing creation attempt can install the verifier." stays.
   - Net −2 lines.
4. **src/registry.rs**: delete `PersistedDescriptor::epoch_bytes` and the blank line after it
   (630-633 today; 625-628 after C1). `crate::crypto::unhex` is still used by `validate_descriptor` (registry.rs:517 and 541 today).
5. **src/dst/tests/runtime_request_work.rs:27, :221, :268**:
   `let mut desc = crate::application::creation::fresh_desc(` becomes
   `let (mut desc, _) = crate::application::creation::fresh_desc(`, and the same for
   `mut replacement` at 221. Each line is under 100 columns.
6. **Ledger:** `test-inventory.json` gets 2 hashes (`r09a_old_ttl_cas_cannot_mutate_or_suppress_a_recreated_incarnation`,
   `r09a_ttl_overflow_returns_retryable_http_refusals_before_append_effects`). `setup` is a helper
   and is not inventoried.

**If C2 is declined,** land C1 alone and add one doc line to `PersistedDescriptor::epoch_bytes`, for
example `/// Parses a DTO under construction; serving code holds StreamDesc::epoch().`. The `Deref`
reachability stays; that is the cost of declining.

### Every `#[expect]`/`#[allow]`-ratcheted scope the diff touches

A ratchet fails only on growth (`source_rules.exception_growth`: `count > before`). No scope here
carries a function-wide `unwrap_used`/`expect_used` exception, so no call-path fingerprint changes.
The checks covered all of these: `product_scan`'s unwrap/expect scope (scan.rs:17-25) is not
touched, there is no module-wide or impl-wide exception on any touched file, and no `mod`
declaration carries one.

| Scope | Exceptions | Effect |
|---|---|---|
| `StreamDesc::epoch_bytes` (registry.rs) | `unnecessary_wraps` | **Deleted with the function.** It has no `source-allowances.json` row (it had a triple reason), so there is nothing to prune. |
| `delete` (deletion.rs) | `too_many_arguments`, `too_many_lines` | `too_many_lines` is **deleted** (it would be unfulfilled at 94 lines). The `too_many_arguments` scope shrinks by 12 lines and loses facts. |
| `resume_deletion` | `too_many_lines` | Line-neutral. Facts drop by 2 (`Some` call-site and path), and it stays fulfilled at 108 counted lines. |
| `verify_internal_target` (product.rs) | `allow(result_large_err, reason = "transport boundary …")`, a legacy allowance row | Attribute text unchanged, so the row's identity is unchanged. Line-neutral; facts drop by 2. |
| 9 of the 19 DST tests (`too_many_lines`, `excessive_nesting`, `disallowed_methods`) | not unwrap/expect | Line-neutral; each site drops 2 facts (`.unwrap()`). All stay fulfilled. |
| C2 `fresh_desc` (creation.rs) | `too_many_arguments` | **Grows**, so the reason is re-decided (new identity). |
| C2 `create_product` | `too_many_lines` | Shrinks: 115 to 113 counted lines, still fulfilled; facts drop by 3. |
| C2 `resolve` (claim.rs) | `too_many_lines` | Unchanged (line- and fact-neutral). |
| `check_key`, `sweep_segment`, C2 `setup` / r09a tests | none | nothing |

## 5 Mutation analysis

**Selection.** Critical or registered paths in the diff:
- C1: **only `src/http.rs`**, owner `http`, with filters `http:: livefeed_engine_retired security_workload::`.
- Not selected, because none of these is under `CRITICAL_PREFIXES` and none has an owner row:
  `src/registry.rs` (only `src/registry/cache.rs` is registered), `src/product.rs`,
  `src/application/consumer/deletion.rs`, `src/application/creation*` and `src/dst/**`.
- C2 selects **nothing**: `mutation_source_files: []`, `mutants: false`.
- `src/http.rs` is also under `BUFFER_PREFIXES`, so the plan selects the Miri leg. That leg runs its
  fixed unit set, and this change adds no obligation to it.

**Function bodies that change:**
- http.rs: `check_key` (critical). The `KeyCheck` enum is not a function.
- deletion.rs: `delete`, `resume_deletion`, `sweep_segment`.
- product.rs: `verify_internal_target`.
- registry.rs: `StreamDesc::epoch_bytes` is deleted.
- The 19 DST tests.
- C2: `fresh_desc`, `resolve`, `create_product` (closure), `PersistedDescriptor::epoch_bytes`
  (deleted), and `setup` plus the two r09a tests.

**In-diff mutants.** In cargo-mutants 27.1.0, `in_diff.rs::affected_lines` (lines 213-257) marks
added lines, the line before each deleted run, and the first surviving line after it.

The http.rs hunks:
- `-2362` marks new 2361 and 2362, which are the enum's `Wrong,` and `}`. They hold no mutant.
- `-2379..2381 +2378` marks new 2377 and 2378. Git may instead align the old `};` differently and
  mark 2376 through 2378. Either way, only these lines inside `check_key` are marked.

| # | Mutant (new-file position) | Selected? | Outcome and killing test |
|---|---|---|---|
| M1 | `src/http.rs:2372:5: replace check_key -> KeyCheck with Default::default()` (the body span 2372..2382 covers 2378) | yes | **unviable**: `KeyCheck` has no `Default` (fnvalue.rs:164-169 falls back to `Default::default()` for unrecognised path types). Not a miss. |
| M2 | `src/http.rs:2379:32: replace != with == in check_key` | **no**: 2379 is a context line that follows an *insert*, not a delete | If a different diff alignment ever selected it, both `livefeed_engine_retired` tests kill it. With the right key, `check_key` answers `Wrong`, so product SSE answers 403 and the test panics with "parked at the live tail:\n…". |

- **Missed mutants:** none. There is no new guard, boundary or predicate, so no equivalent mutant
  needs restructuring.
- **Timeouts:** none. An unviable mutant does not run tests.
- **Owner rows and filters:** unchanged. No new files.

Expected driver line for C1: `Mutation verification executed 1 selected mutant(s) across 1 registered owner(s).`
Expected for C2 alone: `No executable mutations in the registered owners of this selection; no mutation experiment is claimed.`

## 6 Ledgers

| Ledger | C1 | C2 |
|---|---|---|
| `docs/refactor/test-inventory.json` | `python3 scripts/test-inventory.py --write`. Exactly the 19 tests in §1 change `function_sha256`. `git diff --numstat` gives `19 19 docs/refactor/test-inventory.json`; no `configuration` line changes. | `--write` again: 2 hashes (the r09a pair). |
| `docs/refactor/review-mechanisms.json` | **Two pins.** (a) line 162, mechanism `body-poll-admission`, test `r15_bodyless_and_unauthorized_requests_never_poll_the_body`: `sha256` takes the new inventory hash. (b) line 1040, `source_adaptations`, `cut_resume_never_skips_a_durable_record`: `after_sha256` takes the new inventory hash, and one sentence is appended to its `reason`, as 7e4a00a8 did: "Item 62: the initial-cursor decode reads the epoch through StreamDesc::epoch() instead of the deleted Option-returning epoch_bytes(); the decoded position and every assertion are unchanged." Both hashes come from `test-inventory.json` after `--write` (same `function_hash`; neither test uses `include_str!`). | none |
| `docs/refactor/test-adaptations.json` | Not gate-checked, and 3 of its 14 rows are already stale. ee99975d left it alone. Optional: update `after_sha256` for r15 (line 12), `livefeed_refresh_installs_after_external_completion` (line 5) and cut_resume (line 99). | none |
| `docs/quality/source-allowances.json` | No row names a deleted exception (its 15 `exception` rows are all `result_large_err`). The `verify_internal_target` row's attribute text is unchanged. No `--prune`. | none |
| `docs/quality/owners.json`, `architecture-policy.json`, `WIRE-MATRIX.md`, `mutation_owners.py`, `scripts/mt-audit-baseline.txt` | No change. No macro, effect, glob, owner or wire change. No edited line matches an MT-audit pattern (`stream_hash(`, registry literal, `streams-internal-…`). | none |
| `test-scenario-map.json`, `scenario-dispositions.json` | No renames, and the DST edits are line-neutral, so recorded lines hold. | none |
| `docs/MULTITENANCY-MAP.md:237` | Optional. The decaying historical map says `round_epoch = cur_desc.epoch_bytes()`; leave it (see §8). | none |

## 7 Controls (exact commands, expected output)

Use the Python ≥ 3.11 shim: `export PATH=/private/tmp/claude-501/-Users-sorenschmidt-code-streams/4580d18c-ad9e-4e38-8ca1-89558fd0d592/scratchpad/pybin:$PATH`.
Never run the gate and a mutation leg concurrently.

1. **Scratch controls R0 and R0'** (§3) on a throwaway copy of the tree, then `git checkout -- .`.
   Expect 1 line and 25 lines respectively.
2. **Grep controls** after C1:
   - `git grep -n "BadDescriptor\|bad descriptor\|no incarnation to bind" -- src` prints nothing
     (exit 1).
   - `git grep -n "epoch_bytes" -- src` prints exactly two lines: the DTO method in registry.rs
     and `src/application/creation/product.rs:98`.
   - After C2 it prints nothing (exit 1).
3. `cargo fmt --all -- --check` prints nothing (exit 0).
4. Clippy and the ratchet:
   ```
   mkdir -p target/quality
   cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl; echo "clippy $?"
   python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl
   ```
   Expected: `clippy 0`, then `quality ratchets: OK; …`.

   **Negative control N1:** keep deletion.rs:16-19. Clippy fails with
   `error: this lint expectation is unfulfilled` at `src/application/consumer/deletion.rs:17:5`
   (`clippy::too_many_lines,`).

   **Negative control N2 (C2):** keep `fresh_desc`'s old reason. gate.py prints
   `accepted exception grew without a new decision: ('src/application/creation.rs', 'crate::fresh_desc', 'function', …): scope_lines 40 -> 43`
   and a `syntax_facts` line.
5. `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items`
   finishes with no warnings. The new docs use plain backticks and no links.
6. Ledger checks, each printing the line shown:
   - `python3 scripts/test-inventory.py --check` prints `test-inventory: OK (503 tests, 0 ignored)`.
   - `python3 scripts/review-evidence.py --check` prints
     `review-evidence source inventory: OK; execution and external acceptance require receipts`.
   - `python3 scripts/architecture-gate.py --check` prints `architecture-gate: OK (…)`.
   - `python3 scripts/scenario-map-report.py --check` exits 0.
   - `bash scripts/multitenancy-audit.sh` prints `MT_AUDIT_OK`.

   **Negative control N3:** skip the pins. review-evidence prints
   `mechanism test changed or missing: src/dst/review_security.rs::r15_bodyless_and_unauthorized_requests_never_poll_the_body`
   and `cut_resume_never_skips_a_durable_record: after fixture body changed or missing`.

   **Negative control N4:** skip `--write`. test-inventory prints 19 lines of the form
   `<test>: changed function_sha256` and exits 1.
7. Pinning tests (a quick leg before the full gate):
   ```
   cargo test --locked --release --lib -- registry::tests::r04_invalid_descriptors_cannot_reach_storage \
     registry::tests::corrupt_descriptor_fails_closed product::tests::internal_target_ \
     dst_tests::consumer_generations:: dst_tests::consumer_saga:: dst_tests::consumer_delete:: \
     dst_tests::reads_product:: dst_tests::livefeed_engine_retired:: dst_tests::review_security:: \
     dst_tests::security_routes::product_requires_the_account_token dst_tests::livefeed_history:: \
     dst_tests::livefeed_swap:: dst_tests::persistence_faults::cut_resume_never_skips_a_durable_record \
     dst_tests::producer_protocol:: dst_tests::quota_enforcement::watch_waits_occupy_the_subscription_pool \
     dst_tests::sse_delivery::product_sse_controls_carry_signed_cursors dst_tests::watch_observation:: \
     dst_tests::runtime_request_work::
   ```
   Expected: `test result: ok. N passed; 0 failed`.
8. Full gate:
   `OUT=/private/tmp/claude-501/-Users-sorenschmidt-code-streams/4580d18c-ad9e-4e38-8ca1-89558fd0d592/scratchpad/gate-r62.txt scripts/gate.sh`.
   The last line is `GATEDONE`, and the suite floor comes from the inventory (503 − 1 capacity).
9. CI's mutation selection, after committing and before pushing:
   ```
   QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) \
     QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan
   python3 -c "import json;p=json.load(open('target/quality-plan/plan.json'));print(p['mutation_source_files'],p['selected_mutation_owners'])"
   cargo mutants --list --in-diff target/quality-plan/pr.diff --file src/http.rs --package streams-slate
   QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) \
     QUALITY_BASE_REF=origin/slate scripts/quality/mutations.sh
   ```
   Expected output, in order:
   - `['src/http.rs'] ['http']`
   - The listing is exactly
     `src/http.rs:2372:5: replace check_key -> KeyCheck with Default::default()`.
   - The `http` run reports 1 mutant, 1 unviable, 0 missed.
   - The driver prints `Mutation verification executed 1 selected mutant(s) across 1 registered owner(s).`
10. After the push, check the run with
    `gh run list --branch slate --json headSha,status,conclusion,createdAt,name`, matching `headSha`.
    Never claim green from memory.

## 8 Out of scope (recorded follow-ups)

- **`KeyCheck::Missing` is the same fake-Option shape.** All five `check_key` callers pass
  `Some(..)`: product.rs:1677, 2679 and 3559, scan.rs:78, and consumer_generations.rs:330. So
  `Missing` is never produced, and the `_ =>` arms at product.rs:2693 and scan.rs:92 (400
  `missing_key`) are dead as well. The follow-up: take `&str`, delete `Missing`, and let
  `unreachable_patterns` enumerate the two arms (product.rs −9 lines). It touches `product_scan`'s
  function-wide unwrap/expect scope and coordinates with item 61, so it is not folded in here.
- **Two `check_key`s.** `application/consumer.rs:241-255` duplicates `KeyCheck` and `check_key`.
  The application layer may not import `crate::http` (the `owner transport dependency` rule), so
  deduplicating means moving the canonical owner into `application`. That is a separate
  canonical-owner change.
- The review's optional "method migration to StreamDesc limited to resolve_segment/sref".
- `product/tests.rs:181` `expect("descriptor has an epoch")` has stale wording: `InternalTarget::of`'s
  Option is about whether the segment exists.
- `docs/MULTITENANCY-MAP.md:237` and the `scripts/read-experiments` harness (§1): historical and
  pinned, so they are left alone.
- Local DST coverage of the saga's relayed-sweep branch, which is a pre-existing gap.
- **Coordination with item 61** (`plans8/cursor-decode-errors.md`). It also edits
  `src/dst/tests/reads_product.rs`, adding a test after line 586. There is no textual overlap with
  lines 137 and 544 here. Run `test-inventory.py --write` after whichever lands second.

---

## Skeptic corrections (C1..C6)

Checked on the current tree (HEAD = origin/slate = `fb18840d`). No repo file was edited and no cargo command was run.

**Confirmed as written:**
- **Line counts:** http.rs 3,369, product.rs 4,205, registry.rs 1,501, deletion.rs 417, creation.rs 390, creation/product.rs 178. The deltas are http −3, registry −5 then −4, deletion −21, creation +3, creation/product −2, and the DST files are line-neutral.
- **Use sites:** `git grep epoch_bytes` over the whole repo finds these 25 `src` sites and nothing else, apart from the historical ones outside `src`. There are 5 StreamDesc sites in production, 1 DTO site and 19 DST sites. Every DST receiver is a `StreamDesc`: `Registry::get` returns `Option<StreamDesc>` (registry/cache.rs:294-297), and `lf7_split_stream` returns one too (livefeed_history.rs:304). None of the by-path includers pulls in registry, http, deletion or creation: tools/quality-invariants, src/bin/*, fuzz and bench all include other files. There are no cargo features.
- **R0 / R0':** the Deref shadowing is real (registry.rs:407-412, 443-448 and 629-632). The columns check out: http 2379:26 in R0, and 2379:28, product 3284:13, deletion 57:28, 216:29 and 273:18, creation/product 98:36, and the DST sites at :22, :26 and :19 in R0'.
- **`delete`:** it counts 102 lines with braces, as `async fn` bodies are counted, and 94 after the edit, so the `too_many_lines` expectation at deletion.rs:16-19 becomes unfulfilled. N1 is correct. `resume_deletion` counts 108 and `create_product` counts 115 → 113, so both stay fulfilled. `FailureClass::Internal` is still used elsewhere, for example consumer.rs:96 and delivery.rs:159, so deleting the let-else creates no dead variant.
- **Ratchets:** no function-wide `unwrap_used`/`expect_used` scope covers any edited line. The nearest ones are:
  - http.rs `stream_entry_inner`, which ends before 2357;
  - product.rs `json_ok` (3193-3202) and `internal_sweep_segment` (3328-);
  - creation.rs `json_entries` (244-).

  The deleted `unnecessary_wraps` and `too_many_lines` reasons have exactly two `;` each, so they have no `source-allowances.json` row. The only exception rows are the 15 `result_large_err` rows. `verify_internal_target`'s row (source-allowances.json:1634-1639) keeps its identity. The `exception_growth` identity is `(path, qualified, kind, reason text)` (source_rules.py:196, 204-216), so C2's re-decided `fresh_desc` reason is a new identity and is not compared. The item location includes outer attributes, because `syn` `node.span()` does (quality-syntax scan.rs:129). That means the new doc lines count toward scope_lines 40 → 43, and N2 is correct.
- **Mutation:** only `src/http.rs` is selected (owner `http`). Neither product.rs, registry.rs, application/consumer/*, application/creation* nor src/dst/** is under CRITICAL_PREFIXES or in mutation_owners.py. In the new file, `affected_lines` (cargo-mutants 27.1.0 in_diff.rs:213-257) marks 2361-2362 and 2377-2378, or 2376-2378. The only mutant is the whole-body `Default::default()` at `2372:5`: `function_body_span` is first stmt → last stmt (visit.rs:757-761), and fnvalue.rs:164-169 is the fallback. That mutant is unviable, because `KeyCheck` derives nothing. `!=` at new 2379:32 is not selected.
- **Ledgers:** there are exactly two review-mechanisms pins, `body-poll-admission` r15 (line 161-162) and the `source_adaptations` cut_resume entry (1037-1040). No `support_functions`, `fixture_changes` or `review-unit-relocations.json` entry names an edited file. The `r05_cancelled_ttl_attempt_releases_only_its_owned_slot` source adaptation lives in creation.rs, but its body is untouched by C2, so its hash holds. `function_hash` covers only the fn text plus `include_str!` targets (test-inventory.py:74-90), so the `setup` helper change does not rehash the r09a tests beyond their own edited lines. There is no architecture-policy budget exception on any touched file (the only ones are billing/fleet/product_entry/shard/ops), so shrinking cannot produce an "obsolete budget exception". The MT-audit baseline is content-keyed, and no edited line matches.

**C1: R0/R0' cannot run in the shared tree (§3, §7.1).** "On a throwaway copy of the tree, then `git checkout -- .`" contradicts itself. The task forbids touching the tree while the mutation run or gate run is active, and a scratch deletion there would corrupt the in-flight cargo-mutants copy and baseline. Replace it with this: `git worktree add <scratchpad>/wt-r62 fb18840d`, run R0 and R0' there with `CARGO_TARGET_DIR=<scratchpad>/wt-r62-target`, then `git worktree remove --force <scratchpad>/wt-r62`. Run it only after the mutation and gate runs have finished; it must never run concurrently with them.

**C2: Cross-plan contradiction with item 61.** `plans8/cursor-decode-errors.md` C2/C3 (lines ~564-567) says item 62 "deletes ... the `_ =>` arm at scan.rs:90-99" and so shrinks `product_scan`'s scope. It does not. `KeyCheck::Missing` survives C1, and scan.rs:92-100 still needs that arm. Item 62 does not touch `src/product/scan.rs` at all, so both `product_scan` exception scopes (scan.rs:17-25) are unchanged by this item. Add one line to §8 ("Coordination with item 61") saying so. Item 61's plan must not count on that shrink when it re-decides `product_scan`'s reason.

**C3: State the `_ =>` disposition after C1 (§1 "What KeyCheck::BadDescriptor flows into", §2).** After C1, these wildcards absorb:
- product.rs:2693 (`product_read`) and scan.rs:92 (`product_scan`): exactly `KeyCheck::Missing`;
- product.rs:3561 (`internal_segment_scan`) and consumer_generations.rs:332: `Missing | Wrong`.

That is a single-variant wildcard on a domain enum, so the plan should say it leaves them deliberately.

The line-neutral rewrite `crate::http::KeyCheck::Missing =>` is not free:
- It adds path facts under `product_read`'s `too_many_arguments`/`too_many_lines` scope (product.rs:2565-2569).
- It adds path facts under `product_scan`'s function-wide `unwrap_used`/`expect_used` scope, which fingerprints every path. Both ratchets would grow and need re-decided reasons.

So the rewrite belongs to the §8 `Missing` follow-up. Say this, so that a reviewer applying the "no `_ =>` on domain enums" rule does not flag C1.

**C4: The verification.json premise is wrong (§1 "Outside src/").** `docs/quality/verification.json` `source_inventory_sha256` (lines 495-522) is the historical adoption receipt at `recorded_revision` 74e1faae. Its own `scope_note` says "not a current-HEAD attestation", and no script reads it (`grep -rn source_inventory_sha256 scripts/` finds nothing). It also pins deletion.rs, creation.rs, creation/claim.rs and creation/product.rs, which this plan edits. So "pinned, must not be edited" is not the reason to leave the read-experiments harness alone. The actual reason is that `prepare.py` refuses a modern tree ("Historical O5 harness only"). Also state that verification.json is not a ledger for this item and is not updated.

**C5: Historical-doc list is incomplete (§8).** `codereview1.md:3170` also cites "the current `epoch_bytes()` contract". It is historical, so leave it, but list it next to `docs/MULTITENANCY-MAP.md:237`, so that nobody reads the post-C2 `git grep epoch_bytes` hits outside `src` as missed sites.

**C6: The mutation expectation holds only for a push containing this item alone (§5, §7.9).** CI compares against the push event's `before` SHA (common.py:24-29, verification_plan). If C1/C2 are pushed together with item 61 (product_cursor/decode.rs, owner `cursors`, harness-lib) or with crypto-dead-code-allows (src/crypto.rs, owners `crypto`/harness), the selection is the union. In that case the "1 selected mutant across 1 registered owner" line changes. Either push item 62 on its own, or recompute the expected driver line for the combined push. Also, the single unviable mutant still triggers the `http` owner's `--baseline run` (a full build plus the `http:: livefeed_engine_retired security_workload::` tests, unmutated), so the leg is not free.

**Controls that cannot be built as written:** R0/R0' in the shared tree (C1). Every other control is buildable.
**Missed ledgers or ratchets:** none are required. Optional: `test-adaptations.json` (not gate-checked). verification.json is explicitly not a ledger (C4).

**Verdict: ready-with-corrections.** The core analysis holds: the Deref shadowing, the unfulfilled `delete` expectation, the C2 ratchet re-decision, the two mechanism pins with 19+2 inventory hashes, and the single unviable http mutant. C1 is the only change needed to execute the plan safely. C2 through C6 are documentation and coordination fixes.
