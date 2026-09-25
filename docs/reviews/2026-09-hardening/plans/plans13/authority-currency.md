# Item 65: the "is this authority still current?" check was copied by hand and the copies drifted

This plan was checked against the working tree at `6669d3b5`, with merge base `origin/slate = aaf2baa5`. `src/auth.rs`, `src/sse/auth.rs` and `src/product.rs` are identical to the merge base. The uncommitted `mutation_owners.py` and `test_verification_plan.py` edits belong to another item. Stage only the files named below.

Source: review item 65, `robustness-maintainability-review.md` lines 1388–1398.

---

## 1. Problem (verified on the current tree)

All four problems the reviewer lists are real. Checking the code turned up four more copies of the same drift:
- `status_and_quotas` has the same cell gap as the lease path.
- `feed_json` uses a different staleness window.
- The product mapper has a second wildcard.
- `shadow_observe` has an `Err(_)` catch-all.

### 1a. The lease path has no cell check (real, and a live bug)

The request path, `verify_customer`, refuses a policy that is placed on another cell. It does this at `src/auth.rs:714-720`:
```rust
let policy = policies
    .projects
    .get(&project_id)
    .ok_or(AuthError::WrongCell)?;
if policy.cell_id.as_ref() != self.cell_id.as_ref() || c.cell_id != *self.cell_id {
    return Err(AuthError::WrongCell);
}
```
The lease path, `lease_check`, has no such check. At `src/auth.rs:503-505` it only asks whether the project is present:
```rust
let Some(p) = pols.projects.get(&l.project_id) else {
    return Err(R::ProjectMissing);
};
```
Publishing a policy with a new `cell_id` is accepted if it carries a new `project_policy_version` and a new `feed_version`. The feed checks in `src/auth/publication.rs` (`ProjHw::check`, `check_projects_transition`) compare versions, workspace and fingerprint. None of them compares `cell_id`. `auth_feed.rs:168-173` only validates the id's syntax and never filters by cell.

So when a policy is republished for another cell, three things happen today:
- A new request answers 421 `wrong_cell`.
- A live subscription's lease still re-proves `Ok(())`.
- A subscription established after the move is admitted.

**A third copy of the same gap (found during verification): `status_and_quotas` at `src/auth.rs:867-870`.**
```rust
Ok(policies
    .projects
    .get(project)
    .map(|p| (p.status, p.quotas.clone())))
```
Its doc comment says "`Ok(None)` = the project is not in a FRESH snapshot (not served here)". A capability (watch) principal whose project now names another cell still gets `Some((Active, quotas))`. The only caller is `src/application/watch.rs:286-288`, which maps `None` to `WatchFailure::Unauthorized` (403 `watch_unauthorized`).

**Use sites:**
- `lease_check`: `src/sse/auth.rs:207` (`LeaseWatch::new_checked`), `src/sse/auth.rs:276` (`LeaseWatch::revoked_at`), `src/dst/tests/security_freshness.rs:155`.
- `lease_deadline`: `src/sse/auth.rs:208`, `src/sse/auth.rs:278`.
- `status_and_quotas`: `src/application/watch.rs:286`.
- `verify_customer`: `src/product.rs:748` (`enforce_customer`), `src/auth.rs:818` (`shadow_observe`), plus unit tests.
- `AuthLease`: `src/http.rs:1564` (`ReadParams.lease`), `src/sse/auth.rs:115`, `src/dst/tests/security_freshness.rs:4`.
- The lease is built at `src/product.rs:2821` (`principal.map(crate::auth::RequestPrincipal::lease)`).

**DST fixtures:** every `AuthService::new` in `src/dst/tests` uses `"test-cell"`, and every policy those tests publish uses `"test-cell"`. The one exception is `security_modes.rs:267` (`proj_far` on `"other-cell"`), which is only exercised through `verify_customer` (421). So no fixture relies on the gap.

### 1b. `index()` falls back to 0 (real, currently latent)

`src/auth.rs:245-247`:
```rust
pub(crate) fn index(self) -> usize {
    Self::ALL.iter().position(|r| *r == self).unwrap_or(0)
}
```
All 10 variants are in `ALL` today, in declaration order (`src/auth.rs:205-230`), so nothing is miscounted now. But a reason missing from `ALL` would be counted silently as `TokenExpired`, in the counter `/v1/debug/load` exports. The counter array repeats the length as a separate literal, `src/sse/auth.rs:62`:
```rust
pub(crate) static LEASE_TERMINATIONS: [std::sync::atomic::AtomicU64; 10] = [ ...10 lines... ];
```

**Use sites:**
- `index()`: `src/sse/auth.rs:81` (`lease_terminations_json`), `src/sse/auth.rs:160` (`TerminateOnce::record_once`), `src/dst/tests/sse_delivery.rs:315`.
- `ALL`: `src/sse/auth.rs:77`.
- `LEASE_TERMINATIONS`: `src/sse/auth.rs:62/81/160`, `src/dst/tests/sse_delivery.rs:317,326`.
- Export: `src/http.rs:867` (`"lease_terminations"`).

### 1c. Both status mappers end in a wildcard (real). `CredentialInactive` really does answer 401 on the lease path and 403 on the request path.

`lease_refusal_response`, `src/sse/auth.rs:95-99`:
```rust
let status = match r {
    R::PolicyStale | R::GrantsStale => StatusCode::SERVICE_UNAVAILABLE,
    R::ProjectMissing | R::ProjectNotActive => StatusCode::FORBIDDEN,
    _ => StatusCode::UNAUTHORIZED,
};
```
`auth_failure_response`, `src/product.rs:666-694`, has two wildcards:
- `_ => (StatusCode::UNAUTHORIZED, "the bearer token failed verification", false)` at line 689.
- The journal match at lines 708-711: `E::WrongCell | E::PolicyStale | E::GrantsStale | E::KeysStale => r, _ => crate::audit::tag(r, e.kind())`.

A third `AuthError` catch-all is `shadow_observe`, `src/auth.rs:818-828` (`Err(AuthError::WrongCell) => …, Err(_) => …`).

The drift the wildcards hide: `E::CredentialNotActive(_) => (StatusCode::FORBIDDEN, …)` (`product.rs:678`), whereas `R::CredentialInactive` falls through `_` to 401. The comment at `sse/auth.rs:88-92` says these statuses "mirror `auth_failure_response`", which records that this mirroring is done by hand.

**Use sites:**
- `lease_refusal_response`: `src/sse/session.rs:200`.
- `auth_failure_response`: `src/product.rs:749, 972, 978, 1436, 3017, 3904`, `src/http.rs:1806`, `src/dst/tests/security_audit.rs:138,145`, `src/dst/tests/security_lineage.rs:333`.
- `shadow_observe`: `src/product.rs:579`.

### 1d. Staleness is computed three ways, and one of them uses the wrong window (real)

| Site | Code |
|---|---|
| `lease_check` policy, `auth.rs:500` | `if now_unix - pols.fetched_at_unix > w {` |
| `lease_check` grants, `auth.rs:513` | `if now_unix - creds.fetched_at_unix > w {` |
| `lease_deadline`, `auth.rs:540,542` | `d = d.min(self.projects.load().fetched_at_unix + w);` / `d = d.min(creds.fetched_at_unix + w);` |
| `verify_signature`, `auth.rs:603` | `if now.saturating_sub(jwks.fetched_at_unix) > JWKS_STALENESS_MAX_SECS {` |
| `verify_customer`, `auth.rs:704,736` | `if now - policies.fetched_at_unix > self.staleness_max_secs() {` (and the grants twin) |
| `status_and_quotas`, `auth.rs:864` | `if now - policies.fetched_at_unix > self.staleness_max_secs() {` |
| `feed_json`, `auth.rs:894,900` | `"stale": now - policies.fetched_at_unix > POLICY_STALENESS_MAX_SECS,` (and grants) |

The spellings are unchecked subtraction, saturating subtraction and addition. `feed_json` also judges staleness against the **constant**, while enforcement and the neighbouring `"stalenessMaxSecs": self.staleness_max_secs()` field use the **instance** window. In production the two windows are equal: the only writer of the atomic is `#[cfg(test)] set_staleness_max_secs` (`auth.rs:476`). In any rig that shortens the window, `/v1/debug/auth` reports `stale: false` for a feed that verification is refusing.

**Callers of `feed_json`:** `src/http.rs:1037` (`/v1/debug/auth`), `src/http.rs:1642` (health, which reads only `ageSecs`), and `auth_feed.rs` tests. DST tests read `feeds.policies.stale` only with the default window (`security_routes.rs:651`, `security_modes.rs:207`).

### 1e. Further drift, out of scope and turned into decisions (§9)

- **Check order.** `lease_check` checks status before ownership (`auth.rs:506-511`). `verify_customer` checks ownership first on purpose, so that a stale post-transfer token does not learn the new owner's project status (`auth.rs:721-730`).
- **Missing project.** On the lease path, `ProjectMissing` answers 403. On the request path, a project absent from the cell answers 421 `wrong_cell` (`auth.rs:707-717`).

---

## 2. Contract decision

**The typed contract has five parts, all owned by `auth`:**

1. **One placement predicate.** `AuthService::served_policy(&self, &PolicySnapshot, &ProjectId) -> Option<&ProjectPolicy>` returns the policy of a project that this cell serves: present in the snapshot **and** placed here. Three callers use it:
   - `verify_customer`, where it becomes absent/foreign → `WrongCell` (no behaviour change);
   - `lease_check`, where absent/foreign → `ProjectMissing` (**fix**);
   - `status_and_quotas`, where absent/foreign → `Ok(None)` (**fix**).
2. **One freshness boundary.** `feed_fresh_until(fetched, window) -> i64` uses `saturating_add`, and `feed_stale(fetched, window, now) -> bool` is `now > feed_fresh_until(..)`. They are private free functions in `auth.rs`. All 10 sites in §1d use them. `lease_deadline` uses the boundary, and everything else uses the predicate. `feed_json` switches to the instance window.
3. **Refusal classes.** New file `src/auth/refusal.rs`:
   - `Refusal { WrongCell, FeedStale, Denied(Denial), Unverified }`.
   - `Denial { Project, Credential, Scope, Prefix }`.
   - `AuthError::refusal(&self)` and `LeaseInvalidReason::refusal(self)`: two exhaustive tables, side by side, with no wildcard.
   - The transports keep ownership of status codes. They match exhaustively over the class: `auth_failure_response` has 7 arms, the same count as today, plus the journal decision; `lease_refusal_response` has 4 arms; `shadow_observe` matches on the class too.
4. **Counter slot.** `LeaseInvalidReason::index()` becomes `self as usize`, the discriminant.
5. **Counter array size.** `LEASE_TERMINATIONS: [AtomicU64; LeaseInvalidReason::ALL.len()]`, built with `[const { AtomicU64::new(0) }; …]`. This follows the idiom already used for `failpoints.rs:236` `ACTIVE`.

**No wire change in any commit that lands now.** The reasons:
- Every `AuthError` keeps its status, code, message, `retryable` flag, journal tag and `prisma-error-code` header.
- Every `LeaseInvalidReason` keeps its status and code.
- Both are pinned by exhaustive table tests (§3).
- The `/v1/debug/load` key `lease_terminations` keeps the same keys: same `ALL`, same `as_str`, no new reason.
- `/v1/debug/auth` keeps the same shape. The `stale` value now uses the instance window, which production never changes, so production output is byte-identical.

**Two newly refused cases.** Both reuse existing reasons and codes, and both restore contracts the code already documents:
- A lease whose project is now placed on another cell: 403 `project_missing` at establishment, or a counted `project_missing` termination when live. §8.1 says "this cell's policy snapshot lists EXACTLY the projects placed here".
- A watch capability for such a project: 403 `watch_unauthorized`. The `status_and_quotas` doc says `Ok(None)` means "not served here".

These are bug fixes in the same class as the earlier SR2 finding 3, which made the capability path fail closed on stale policy. They are not contract changes.

**Kept out of the landed commits:** the reviewer's `CredentialInactive` 401 → 403 (decision 1), and the two other lease/request divergences found in §1e (decisions 2 and 3). `LeaseInvalidReason::refusal` encodes today's classes, with a comment naming each divergence.

### Where the reviewer's Change is adopted, changed or rejected

| Reviewer | Verdict |
|---|---|
| `index() = self as usize` with an array sized from `ALL`, plus a test | Adopted. |
| Refuse `p.cell_id != self.cell_id` in `lease_check` as `ProjectMissing` | Adopted, through the shared `served_policy`. The capability path gets the same predicate. |
| Make both mappers exhaustive | **Cannot be built literally.** `product.rs` is at 4,205 of 4,205 lines. Two explicit 19-variant or-patterns add about 40 lines. Moving `auth_failure_response` to a sub-module to make room adds `use sub::auth_failure_response;` to `product.rs`. That import re-resolves the call at `product.rs:3904` inside `product_list`'s `#[expect(clippy::unwrap_used)]` scope, which creates a new `unwrap_site:ordinary-call` fingerprint, and the gate fails with "accepted exception grew". **What can be built:** the `Refusal` class above. It puts the exhaustive enumeration in `auth`, where `product.rs` changes by 0 net lines. |
| Move `CredentialInactive` to 403 and update WIRE-MATRIX:133 | Wire change → decision 1. Not landed. |
| "One `is_stale` on the snapshots" | A method on `PolicySnapshot`/`GrantSnapshot` would sit inside `project_policy.rs`'s file-wide `#![allow(dead_code, reason = …)]` scope. That grows its `scope_lines`, `nested_items` and `syntax_facts` → "accepted exception grew". It would also not cover `JwksSnapshot` (which lives in `auth.rs`) or `lease_deadline`, which needs the boundary rather than the predicate. **Instead:** two free functions in `auth.rs`. |
| `sse/auth.rs` needs mutation dispositions | Not needed. The only mutant selected in the diff is killed (§5). Removing the wildcard also removes its match-arm mutants. |

---

## 3. Red tests and pinning tests

### Red tests: each one fails on the tree just before its fix

**R1. `auth::tests::every_freshness_reader_shares_one_boundary`** in `src/auth.rs` `mod tests` (commit C5).
```rust
let svc = service();
svc.set_staleness_max_secs(3);
let lease = svc.verify_customer(&sign(&claims()), NOW).unwrap().lease();
let edge = NOW + 3;                                   // age == window: fresh everywhere
assert!(svc.verify_customer(&sign(&claims()), edge).is_ok());
assert_eq!(svc.lease_check(&lease, edge), Ok(()));
assert_eq!(svc.lease_deadline(&lease), edge);
assert!(svc.status_and_quotas(&lease.project_id, edge).is_ok());
assert_eq!(svc.feed_json(edge)["policies"]["stale"], false);
let past = edge + 1;                                  // one second later: stale everywhere
assert_eq!(svc.verify_customer(&sign(&claims()), past).unwrap_err(), AuthError::PolicyStale);
assert_eq!(svc.lease_check(&lease, past), Err(LeaseInvalidReason::PolicyStale));
assert_eq!(svc.feed_json(past)["policies"]["stale"], true,
           "the operator surface must call a refusing feed stale");
assert_eq!(svc.feed_json(past)["grants"]["stale"], true);
```
**Trace on the pre-C5 tree.** `service()` publishes policies and grants at `fetched_at_unix = NOW`, and the window is 3.
- At `edge`: the policy age is 3, and `3 > 3` is false, so verification is `Ok`, the lease is `Ok`, the deadline is `min(NOW+600, NOW+3, NOW+3) = edge`, `status_and_quotas` is `Ok`, and `feed_json` evaluates `3 > 300`, i.e. false. All pass.
- At `past`: verification returns `4 > 3` → `PolicyStale` (pass), and the lease returns `PolicyStale` (pass).
- `feed_json(past)` evaluates `4 > POLICY_STALENESS_MAX_SECS (300)`, i.e. `false`, so the assertion fails.

**Expected red output:**
```
thread 'auth::tests::every_freshness_reader_shares_one_boundary' panicked at src/auth.rs:<line>:9:
assertion `left == right` failed: the operator surface must call a refusing feed stale
  left: Bool(false)
 right: true
```

**R2. `auth::tests::a_lease_ends_when_its_project_is_placed_on_another_cell`** in `src/auth.rs` `mod tests` (commit C6). It adds a test helper, `place_on(svc, cell)`. The helper clones `svc.projects`, sets `proj_456.cell_id = Arc::from(cell)`, sets `project_policy_version += 1` and `feed_version += 1`, then calls `publish_policies(snap).unwrap()`.
```rust
let svc = service();
let lease = svc.verify_customer(&sign(&claims()), NOW).unwrap().lease();
assert_eq!(svc.lease_check(&lease, NOW), Ok(()));
place_on(&svc, "sin-cell-01");
assert_eq!(svc.verify_customer(&sign(&claims()), NOW).unwrap_err(), AuthError::WrongCell);
assert_eq!(svc.lease_check(&lease, NOW), Err(LeaseInvalidReason::ProjectMissing),
           "a lease must not outlive its project's placement on this cell");
```
**Trace on the pre-C6 tree:**
- The publish succeeds. The ownership version stays at 12, the policy version is 41 ≥ 40, the workspace is unchanged, the fingerprint pair is new, and the feed generation is new.
- Verification hits `policy.cell_id != self.cell_id`, so `WrongCell` (pass).
- `lease_check` finds the project (`get` → `Some`). Its status is Active, ownership 12 == 12, the grants are fresh, the credential is Active, grant 7 == 7 with the same project, and there is no expiry. So it returns `Ok(())`.

**Expected red output:**
```
assertion `left == right` failed: a lease must not outlive its project's placement on this cell
  left: Ok(())
 right: Err(ProjectMissing)
```

**R3. `auth::tests::a_capability_is_not_served_for_a_project_placed_on_another_cell`** in `src/auth.rs` `mod tests` (commit C7).
```rust
let svc = service();
let pid = ProjectId::new("proj_456").unwrap();
assert_eq!(svc.status_and_quotas(&pid, NOW).map(|s| s.is_some()), Ok(true));
place_on(&svc, "sin-cell-01");
assert_eq!(svc.status_and_quotas(&pid, NOW).map(|s| s.is_some()), Ok(false),
           "a capability must not be served for a project placed on another cell");
```
**Trace on the pre-C7 tree.** The policy is fresh (age 0), and `get` returns `Some` regardless of `cell_id`, so the result is `Ok(true)`.

**Expected red output:**
```
assertion `left == right` failed: a capability must not be served for a project placed on another cell
  left: Ok(true)
 right: Ok(false)
```

### Pinning tests for the pure refactors

These land in C2, ahead of the refactors. They pass on the C1 tree and must stay green through C3–C7.

**P1. `sse::auth::tests::lease_terminations_have_one_slot_per_reason`** in `src/sse/auth.rs`, a new inline `#[cfg(test)] mod tests`.
- It imports explicitly (`use super::{LEASE_TERMINATIONS, lease_refusal_response, lease_terminations_json}; use crate::auth::LeaseInvalidReason as R; use axum::http::StatusCode;`). There is no glob, so no `unresolved-glob` fact is created.
- For every `(slot, reason)` in `R::ALL.into_iter().enumerate()`, it asserts `reason.index() == slot`.
- It asserts that `lease_terminations_json().as_object().map(Map::len) == Some(LEASE_TERMINATIONS.len())`.
- Across C3 it pins that `ALL` is listed in declaration order, which is what makes discriminant == position true.

**P2. `sse::auth::tests::lease_refusals_answer_their_class_status`**, a `#[tokio::test]` in the same module.
- For every `reason` in `R::ALL`, it builds an independent oracle with an exhaustive `match reason`: `PolicyStale|GrantsStale` → 503, `ProjectMissing|ProjectNotActive` → 403, the other six → 401.
- It asserts `lease_refusal_response(reason).status() == expected`.
- It asserts that the body's `["error"]["code"] == reason.as_str()` (body read with `axum::body::to_bytes`, the `sse/session/tests.rs:124` idiom).
- It pins C4 and kills the `lease_refusal_response` mutant (§5).

**P3. `product::tests::every_auth_refusal_keeps_its_response`**, a `#[tokio::test]` in `src/product/tests.rs`.
- It has 27 rows, one per `AuthError` variant, each `(error, status, message)`.
- For each row it asserts:
  - the status;
  - `DenialTag` present ⇔ status is 401 or 403;
  - `prisma-error-code` header present ⇔ 421;
  - body `code == error.kind()`;
  - body `message` equals the exact text;
  - body `retryable` ⇔ 503.
- The 421 message is copied byte for byte from `product.rs:669`. That literal contains a run of 14 spaces; it is wire text and stays.

**Existing pins, named per refactor:**

- **C1 (moving the lease code).**
  - Compile-level: `cargo check --locked --all-targets` passes with no call site edited, because every consumer names `crate::auth::{AuthLease, LeaseInvalidReason}`, which the re-export serves.
  - Byte identity is checked by the diff in §7.
  - `auth::tests::*`.
  - `dst_tests::security_freshness::v01_owned_time_refreshes_survive_every_preceding_deadline` and `…::v01_owned_time_staleness_refuses_each_unrefreshed_feed_at_the_boundary`.
  - `dst_tests::security_subscription::{transfer_terminates_established_subscriptions, subscription_terminates_at_token_expiry, suspension_terminates_established_subscriptions, lineage_sse_refuses_authorization_invalidated_before_body_construction}`.
  - `dst_tests::security_revocation::{subscription_terminates_when_only_the_grant_feed_goes_stale, token_expiry_interrupts_slowly_progressing_delivery}`.
  - `dst_tests::sse_delivery::termination_reasons_count_exactly_once_per_subscription`.
  - `dst_tests::security_workload::raw_sse_terminates_at_workload_token_expiry`.
- **C3 (`index` and array size).** P1, plus the `sse_delivery`/`security_*` termination-count tests above.
  - Compile-level: the counter array's length is `ALL.len()` by type.
- **C4 (`Refusal`).** P2, P3, `auth::tests::shadow_observe_counts_without_rejecting`, `dst_tests::security_audit::audit_tags_distinguish_caller_failures_from_cell_failures`, `dst_tests::security_lineage::stale_jwks_maps_to_retryable_503`, and `dst_tests::security_modes` (shadow counters `ok=1, failed=2, missing=1`, `proj_far` → 421).
  - Compile-level: `AuthError::refusal` and `LeaseInvalidReason::refusal` have no `_` arm, so a new reason does not compile until it is classified. The `rg` proof is in §7.
- **C5 (staleness).** The refactor part (verification, lease, JWKS, status) is pinned by R1's `edge` and `past` legs, `v01_*`, `auth::tests::negative_matrix_policy_and_credential` (`PolicyStale`), `auth::tests::stale_key_set_fails_closed` (JWKS +1), and `auth_feed::tests::*` (`feed_json` with the default window).
  - The JWKS rewrite is algebraically equal to the old form for every i64 input. `now.saturating_sub(f) > W` ⇔ `now > f.saturating_add(W)` holds in all four saturation cases, because both saturate towards the same verdict.
- **C6 (the `verify_customer` half).** Behaviour is unchanged: absent or foreign → `WrongCell`, and a wrong claim → `WrongCell`. Pinned by `auth::tests::negative_matrix_tokens` (claim `sin-cell-01`), `negative_matrix_policy_and_credential` (`proj_nope`), R2's verification assertion, and `security_modes` `proj_far`.

There is no synchronization change. `LeaseWatch::new_checked`'s generation-stable loop is untouched, and `served_policy`/`feed_stale` are pure reads of the snapshots already loaded. So no Loom or held-commit test applies.

---

## 4. Edits, file by file, in commit order

**Line budgets.** Only `auth.rs` and `product.rs` are ceilinged files that get touched.

| File | Now | Budget |
|---|---|---|
| `src/auth.rs` | 1,676 | ≤ 1,676 (merge base 1,676) |
| `src/product.rs` | 4,205 | ≤ 4,205 |
| `src/sse/auth.rs` | 518 | ≤ 1,000 |
| `src/product/tests.rs` | 335 | ≤ 1,000 |
| New `src/auth/lease.rs`, `src/auth/refusal.rs` | — | ≤ 1,000 each |

These ceilinged files are not touched: `http.rs` 3,155, `shard.rs` 3,186, `billing.rs` 2,157, `history.rs` 1,713, `registry.rs` 1,492, `sse/feed.rs` 1,165, `fleet.rs` 1,142. No DST file is touched.

**Scopes with ratcheted exceptions (checked for every commit).**
- `src/auth.rs`: no `#[expect]`/`#[allow]` in production. Its source allowances are two `macro-dsl` rows (`crate::AuthService::feed_json` and `shadow_json`, `serde_json::json`) and `unresolved-glob crate::tests`. The code that moves contains no macro, static, glob or exception.
- `src/sse/auth.rs` has four scopes: `LeaseWatch::nap` (`cast_sign_loss`), `GatedSseBody::new` (`too_many_arguments`), and `poll_next` (`excessive_nesting`, `let_underscore_must_use`, `unwrap_used`, `cfg_attr(test, expect(disallowed_methods))`). **None of them is edited.** The new top-level imports (`LeaseInvalidReason`, `Refusal`, `AtomicU64`) re-resolve no bare path inside them, because `poll_next` names neither `AtomicU64` nor `LeaseInvalidReason`/`Refusal` bare. So no `unwrap_site:*` fingerprint changes.
- `src/product.rs`: `auth_failure_response` has no attribute, and `product.rs` has no crate-level exception. `product_list`'s `unwrap_used` scope calls `auth_failure_response(&e)`, and the spelling and resolution of that call are unchanged because the function stays in `product.rs`.
- `src/project_policy.rs` has a file-wide `#![allow(dead_code)]`. It is **not touched**, and that is deliberate (§2).

### C1: "The lease re-proof moves under auth as its own module; nothing else moves"

This is a pure verbatim move.

- **`src/auth.rs`:**
  - Delete lines 187-201 (`AuthLease` plus the trailing blank line), 202-249 (`LeaseInvalidReason` enum, its `impl`, and the blank line), and 485-552 (`lease_check` and `lease_deadline` with their docs, plus the blank line). That is 131 lines.
  - Add `mod lease;` beside `mod publication;` and `pub(crate) use lease::{AuthLease, LeaseInvalidReason};` beside `use publication::HighWater;` (+2 lines).
  - Result: **1,547**.
- **`src/auth/lease.rs` (new, about 142 lines):**
  - A module doc giving the reason for the module: it owns the live-subscription lease — the facts a long-lived subscription keeps re-proving, why it stops, and the instant it must re-prove — and it is a child of `auth`, so it reads the snapshots verification reads.
  - Imports `use std::sync::Arc; use super::AuthService; use crate::tenant::ProjectId;`.
  - Then the two blocks byte for byte, then `impl AuthService { <old 485-551 byte for byte> }`.
  - A child module can see `AuthService`'s private fields (`projects`, `credentials`); `publication.rs` already relies on this.
- **Ledgers:** none. Architecture: the new file has no `crate::http`/`crate::product` edges and no function over 200 lines.

### C2: pinning tests for the refactors that follow

Tests only.

- `src/sse/auth.rs`: append `#[cfg(test)] mod tests { … }` with P1 and P2 (about +50 lines, to about 568).
- `src/product/tests.rs`: add P3 (about +55 lines, to about 390).
- **Ledgers:** none. The new tests are not DST tests, so there is no `test-inventory` change. The inline module uses explicit imports and no glob. The existing `use super::*` in `product/tests.rs` is already registered, and its count stays 1.

### C3: "A lease reason's counter slot is its discriminant, and the counters are sized by the reasons"

- **`src/auth/lease.rs`:** `index()` becomes `self as usize`, with a doc giving the reason: the slot in `sse::auth::LEASE_TERMINATIONS`; a fieldless enum's discriminant is its declaration position; `ALL` lists every reason in that order (pinned by P1); so there is no search whose miss could count one reason as another.
  - Clippy: `cast_possible_truncation` returns early for an enum without `repr` cast to `usize`, and neither `as_conversions` nor `cast_lossless` is enabled.
- **`src/sse/auth.rs`:**
  - Add `use crate::auth::LeaseInvalidReason;` and `use std::sync::atomic::AtomicU64;` (run `cargo fmt` for the order).
  - Replace lines 61-73 with:
    ```rust
    /// Review round 3 F1: lease terminations by reason (canary counter),
    /// one slot per reason at its `LeaseInvalidReason::index`.
    pub(crate) static LEASE_TERMINATIONS: [AtomicU64; LeaseInvalidReason::ALL.len()] =
        [const { AtomicU64::new(0) }; LeaseInvalidReason::ALL.len()];
    ```
  - Net −7 lines, to about 561.
- **Ledgers (same commit):**
  - `docs/quality/source-allowances.json`: prune `{"category":"global","owner":"crate::LEASE_TERMINATIONS","path":"src/sse/auth.rs","syntax":"[std :: sync :: atomic :: AtomicU64 ; 10]"}` with `gate.py --prune` (§7).
  - `docs/quality/owners.json`: add a row after `crate::sse_stats::FEED_CUTOFF_ENGINE_RETIRED`:
    ```json
    {"category": "global", "count": 1, "owner": "crate::LEASE_TERMINATIONS", "path": "src/sse/auth.rs",
     "reason": "Process instrumentation counters: one AtomicU64 per lease-termination reason, sized by LeaseInvalidReason::ALL so a reason's slot is its discriminant; /v1/debug/load exports them as lease_terminations, and a per-runtime copy would hide terminations from the canary.",
     "syntax": "[AtomicU64 ; LeaseInvalidReason :: ALL . len ()]"}
    ```
    Copy the `syntax` value exactly as the gate prints it in the `unregistered source occurrence` line; the precedent is `failpoints.rs` `[std :: sync :: atomic :: AtomicUsize ; Fp :: ALL . len ()]`.
  - The new identity cannot go in `source-allowances.json`, because its legacy ceiling is 0 ("legacy source allowance grew").

### C4: "Request and lease refusals classify in one exhaustive table each; the transports have no catch-all arm"

This is a pure refactor with no status change.

- **`src/auth/refusal.rs` (new, about 90 lines):**
  - Module doc giving the reason: the request path and the lease path classify here, exhaustively and side by side, so a new reason cannot fall into a transport's catch-all and one reason cannot answer differently on the two paths without these tables showing it. Transports own status codes; `auth` owns the class.
  - `#[derive(Clone, Copy, Debug, Eq, PartialEq)] pub(crate) enum Refusal { WrongCell, FeedStale, Denied(Denial), Unverified }`, one doc line per variant.
  - `pub(crate) enum Denial { Project, Credential, Scope, Prefix }`.
  - `impl AuthError { pub(crate) fn refusal(&self) -> Refusal }`:
    - `WrongCell` → `WrongCell`;
    - `PolicyStale|GrantsStale|KeysStale` → `FeedStale`;
    - `ProjectNotActive(_)` → `Denied(Project)`;
    - `CredentialNotActive(_)` → `Denied(Credential)`;
    - `MissingScope(_)` → `Denied(Scope)`;
    - `PrefixDenied` → `Denied(Prefix)`;
    - the 19 others, listed by name → `Unverified`.
  - `impl LeaseInvalidReason { pub(crate) fn refusal(self) -> Refusal }`:
    - `PolicyStale|GrantsStale` → `FeedStale`;
    - `ProjectMissing|ProjectNotActive` → `Denied(Project)`;
    - `TokenExpired|OwnershipChanged|CredentialMissing|CredentialInactive|GrantChanged|CredentialExpired` → `Unverified`.
    - Its doc names the two deliberate divergences from the request path: an inactive credential is `Unverified` here but `Denied` there, and a project not served here is `Denied` here but `WrongCell` there. Both are wire decisions and are not taken in this item.
- **`src/auth.rs`:**
  - `mod refusal;` and `pub(crate) use refusal::{Denial, Refusal};` (+2, to 1,549).
  - `shadow_observe`: `match self.verify_customer(token, now).map_err(|e| e.refusal()) { Ok(_) => ok, Err(Refusal::WrongCell) => wrong_cell, Err(Refusal::FeedStale | Refusal::Denied(_) | Refusal::Unverified) => failed }` (±0).
- **`src/product.rs` `auth_failure_response`:**
  - Replace `use crate::auth::AuthError as E;` with `use crate::auth::{Denial as D, Refusal as R};`.
  - Add `let refusal = e.refusal();`.
  - `match refusal` gets 7 arms with the same tuples and literals: `R::WrongCell`, `R::FeedStale`, `R::Denied(D::Project)`, `R::Denied(D::Credential)`, `R::Denied(D::Scope)`, `R::Denied(D::Prefix)`, `R::Unverified`. The two single-line arms stay within 100 columns (99 and 93).
  - `if refusal == R::WrongCell {` replaces `matches!(e, E::WrongCell)`.
  - The journal match becomes `R::WrongCell | R::FeedStale => r, R::Denied(_) | R::Unverified => crate::audit::tag(r, e.kind())`.
  - Shorten the 6-line doc to 5 lines ("One response per refusal class (§7.1/§8.1, `AuthError::refusal`): 421 …, 503 …, 403 for verified-but-denied, 401 …") to pay for the `let`.
  - **Net 0 → 4,205.**
- **`src/sse/auth.rs` `lease_refusal_response`:**
  - `let status = match r.refusal() { Refusal::WrongCell => MISDIRECTED_REQUEST, Refusal::FeedStale => SERVICE_UNAVAILABLE, Refusal::Denied(_) => FORBIDDEN, Refusal::Unverified => UNAUTHORIZED };`. No lease reason is classified `WrongCell` today; the arm answers what the request path answers for placement.
  - Rewrite the doc: the status follows the reason's `Refusal` class, the same classes `auth_failure_response` answers the request path from. Add `Refusal` to the top import.
- **Ledgers:** `docs/refactor/WIRE-MATRIX.md`.
  - Line 15: cite `auth_failure_response` at its new lines, and add "classes from `AuthError::refusal` (`src/auth/refusal.rs`)". The codes are unchanged.
  - Line 133: `src/sse/auth.rs:90-102` becomes the new `lease_refusal_response` range, with "classes from `LeaseInvalidReason::refusal`".

### C5: "Every freshness reader derives from one boundary; the operator surface calls a feed stale when enforcement does"

This commit contains R1 and its fix together. Show R1 red first by applying only the test hunk.

- **`src/auth.rs`:**
  - After `JWKS_STALENESS_MAX_SECS` (line 49), add (+12 lines):
    ```rust
    /// §7.1: the last second a snapshot fetched at `fetched_at_unix` still
    /// authorizes. Every staleness refusal, the operator surface and every
    /// lease deadline derive from this one boundary, so "stale" and
    /// "re-check now" cannot disagree about when a window closes.
    fn feed_fresh_until(fetched_at_unix: i64, window_secs: i64) -> i64 {
        fetched_at_unix.saturating_add(window_secs)
    }

    /// §7.1: the one fail-closed staleness predicate.
    fn feed_stale(fetched_at_unix: i64, window_secs: i64, now: i64) -> bool {
        now > feed_fresh_until(fetched_at_unix, window_secs)
    }
    ```
  - Rewrite `verify_signature`, `verify_customer` ×2 and `status_and_quotas` in place as `if feed_stale(<snapshot>.fetched_at_unix, <window>, now) {`.
  - `feed_json`: `"stale": feed_stale(policies.fetched_at_unix, self.staleness_max_secs(), now),` and the grants twin, inside `json!`. The `macro-dsl` row's identity is unchanged.
  - Add R1 (about +30 lines). Result about 1,591.
- **`src/auth/lease.rs`:** `use super::{feed_fresh_until, feed_stale};`. `lease_check` uses `feed_stale(pols.fetched_at_unix, w, now_unix)` and the creds twin. `lease_deadline` uses `d.min(feed_fresh_until(…, w))` ×2.
- **Ledgers:** none. `POLICY_STALENESS_MAX_SECS` is still used by the constructor, `config/validation.rs:893-900` and the tests.

### C6: "A lease re-proves its project's placement on this cell, like request verification"

This commit contains R2 and its fix together.

- **`src/auth.rs`:**
  - Add `served_policy` beside `status_and_quotas`, with a doc giving the reason (§8.1 — the policy of a project this cell serves: present and placed here; verification, leases and capability status ask this one question, so a policy republished for another cell cannot keep authorizing one path after another refuses it). About +16 lines:
    ```rust
    fn served_policy<'a>(
        &self,
        policies: &'a PolicySnapshot,
        project: &ProjectId,
    ) -> Option<&'a crate::project_policy::ProjectPolicy> {
        policies.projects.get(project).filter(|p| p.cell_id == self.cell_id)
    }
    ```
  - `verify_customer`: `let policy = self.served_policy(&policies, &project_id).ok_or(AuthError::WrongCell)?; if c.cell_id != *self.cell_id { return Err(AuthError::WrongCell); }` (−1 line). The §8.1 comment above it stays accurate.
  - Add `place_on` and R2 (about +30 lines). Result about 1,636.
- **`src/auth/lease.rs`:** `let Some(p) = self.served_policy(&pols, &l.project_id) else { return Err(R::ProjectMissing); };`, with one comment line: "§8.1: absent from this cell's snapshot, or placed on another cell".
- **Ledgers:** WIRE-MATRIX line 133: "403 `project_missing` (absent from, or placed on another cell than, this cell's policy snapshot)/`project_not_active`".

### C7: "A capability is served only for a project placed on this cell"

This commit contains R3 and its fix together. It is separable: drop C7 alone if Søren wants the item limited to the reviewer's list.

- **`src/auth.rs`:**
  - `status_and_quotas` becomes `Ok(self.served_policy(&policies, project).map(|p| (p.status, p.quotas.clone())))` (−1 line).
  - Doc: "`Ok(None)` = this cell does not serve the project in a FRESH snapshot (absent, or placed on another cell)".
  - Add R3 (about +14 lines). **Final about 1,650 ≤ 1,676.**
  - Fallback if the measured count exceeds the budget: move R2 and R3 with `place_on` into `src/auth/lease.rs`'s own `#[cfg(test)] mod tests`, reaching `service`/`sign`/`claims` by marking them `pub(super)` in `auth::tests` (the parent module is visible to descendants). Do not trim unrelated doc comments.
- **Ledgers:** WIRE-MATRIX line 168: "Enforce mode: capability must still pass fresh policy for a project this cell serves (503 `policy_stale`, 403 `project_not_active`, 403 `watch_unauthorized` when absent or placed on another cell …)".

---

## 5. Mutation analysis

**Files in the mutation scope.** Only `src/sse/auth.rs`: it is under the critical `src/sse` prefix, with owner `sse_auth` and filter `sse::`.
- `src/auth.rs`, `src/auth/lease.rs`, `src/auth/refusal.rs`, `src/product.rs` and `src/product/tests.rs` are neither critical nor registered.
- The new files are `A` records under a non-critical prefix, and no critical file is deleted, so none is forced into registration.
- No owner row or filter changes.
- Run the push-equivalent plan to confirm (§7). The expected `mutation_source_files` include `src/sse/auth.rs`, alongside whatever items 90/93/41/89 already select.

**What changes in `src/sse/auth.rs` (final diff against `origin/slate`):**

| Change | Mutants selected in the diff |
|---|---|
| Top-level `use` lines | None (no function). |
| `LEASE_TERMINATIONS` static, lines 61-73 → about 4 lines | None. It is a static item with no binary or unary operator. The line after the deletion is the blank line before `lease_terminations_json`, whose function-value span starts at its first body statement and is not affected. |
| `lease_refusal_response` body (C4) | **1** (below). |
| `#[cfg(test)] mod tests` | None (cargo-mutants skips attributes containing `cfg(test)`). |

**Mutants cargo-mutants 27.1.0 generates in the diff for `src/sse/auth.rs`:**

1. `replace lease_refusal_response -> Response with Default::default()` (function value; `Response` is not a recognized type, so cargo-mutants falls back to `Default`, which gives 200 and an empty body).
   - **Killed by** `sse::auth::tests::lease_refusals_answer_their_class_status`: the first `ALL` element, `TokenExpired`, expects 401 and gets 200. The test name contains `sse::`, so it is inside the owner's filter.

No match-arm mutant is generated, because no match in the new body has a `_` arm. The two mutants the wildcard used to produce (`delete match arm R::PolicyStale | R::GrantsStale` and `delete match arm R::ProjectMissing | R::ProjectNotActive`) no longer exist. There are no guards and no binary or unary operators. **No survivors, so no dispositions are needed and no equivalent mutants had to be restructured.**

**Outside CI's scope, killed anyway:**

| Predicate | Mutant | Killed by |
|---|---|---|
| `feed_stale` | `>` → `==`, `<`, `>=` | R1's `edge`/`past` legs |
| `feed_stale` | function value `true`/`false` | R1 |
| `feed_fresh_until` | function value `0`/`1`/`-1` | `lease_deadline == edge` in R1 |
| `served_policy` | `==` → `!=` | `happy_path_yields_a_full_principal` and R2 |
| `index` | function value `0`/`1` | P1 |
| `refusal` | arms mutated or swapped | P2 and P3 tables |

`saturating_add` is a method call, so it produces no binary-operator mutant.

---

## 6. Ledgers

| Ledger | Commit | Change |
|---|---|---|
| `docs/quality/owners.json` | C3 | Add the `global` `crate::LEASE_TERMINATIONS` row (§4 C3). |
| `docs/quality/source-allowances.json` | C3 | Prune the vacated `[std :: sync :: atomic :: AtomicU64 ; 10]` row with `gate.py --prune`. |
| `docs/refactor/WIRE-MATRIX.md` | C4, C6, C7 | Lines 15/133 (C4 citations and class source), 133 (C6 `project_missing` wording), 168 (C7 capability placement). No status or code text changes. |
| `docs/refactor/test-inventory.json` | — | Unchanged: no `src/dst` edits. `test-inventory.py --check` must pass as is. |
| `docs/refactor/review-mechanisms.json` | — | Unchanged: no pinned DST test touched. |
| `docs/refactor/test-scenario-map.json` / `scenario-dispositions.json` | — | Unchanged. The auth tests stay in `src/auth.rs` under the same names; SEC-001 references `negative_matrix_*` there. |
| `docs/refactor/architecture-policy.json` | — | Unchanged. The `sse/auth.rs` HTTP imports are unchanged and the new tests reference no `crate::http`. The new `auth/*` files have no http/product edges. |
| `src/dst/tests/README.md` | — | Unchanged: no new DST module. |
| `scripts/quality/mutation_owners.py` | — | Unchanged: no new critical file. |
| `docs/quality/legacy-source.json`, `policy.json` | — | Never edited (immutable). |

---

## 7. Controls

Run these after each commit unless noted. Use Python ≥ 3.11 for the quality scripts, because they need `tomllib`. Run them only once the tree is idle, since CPU contention times out mutants.

```bash
cargo fmt --all -- --check                                     # no output, exit 0
cargo clippy --locked --workspace --all-targets -- -D warnings # "Finished", no warnings
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items
wc -l src/auth.rs src/product.rs src/sse/auth.rs src/auth/lease.rs src/auth/refusal.rs
#   auth.rs: C1 1547, C4 1549, C5 ~1591, C6 ~1636, C7 ~1650 (hard limit 1676)
#   product.rs: 4205 after every commit
```

**C1 verbatim proof.** Adjust the lease.rs spans if `cargo fmt` reorders its imports:
```bash
diff <(git show HEAD~1:src/auth.rs | sed -n '187,200p;202,248p;485,551p') \
     <(sed -n '11,24p;26,72p;75,141p' src/auth/lease.rs) && echo VERBATIM
git diff --stat HEAD~1 -- src/auth.rs      # 2 insertions, 131 deletions
rg -n 'auth::lease::' src                  # no hits outside src/auth.rs
```

**Red demonstrations (C5, C6, C7).** Stage only the test hunk, run it, see the §3 message, then apply the fix:
```bash
cargo test --locked --lib -- --exact auth::tests::every_freshness_reader_shares_one_boundary
cargo test --locked --lib -- --exact auth::tests::a_lease_ends_when_its_project_is_placed_on_another_cell
cargo test --locked --lib -- --exact auth::tests::a_capability_is_not_served_for_a_project_placed_on_another_cell
# before fix: "test result: FAILED. 0 passed; 1 failed" with the §3 panic text; after: "1 passed"
```

**Pins and neighbours:**
```bash
cargo test --locked --lib -- auth::tests:: product::tests:: auth_feed:: \
  dst_tests::security_freshness:: dst_tests::security_subscription:: dst_tests::sse_delivery:: \
  dst_tests::security_revocation:: dst_tests::security_workload:: dst_tests::security_audit:: \
  dst_tests::security_lineage:: dst_tests::security_modes:: dst_tests::watch_observation:: \
  dst_tests::security_policy::
# all "ok"; auth::tests:: also selects sse::auth::tests::{lease_terminations_have_one_slot_per_reason, lease_refusals_answer_their_class_status}
```

**No catch-all arms left (after C4):**
```bash
rg -n '_ =>' src/auth/refusal.rs src/sse/auth.rs                   # no hits
sed -n '/fn auth_failure_response/,/^}/p' src/product.rs | rg '_ =>' # no hits
sed -n '/fn shadow_observe/,/^    }/p' src/auth.rs | rg 'Err\(_\)'   # no hits
```

**Quality gates.** For C3, first expect these two lines:
- `unregistered source occurrence (1): ('global', 'src/sse/auth.rs', 'crate::LEASE_TERMINATIONS', '[AtomicU64 ; LeaseInvalidReason :: ALL . len ()]')`
- `1 obsolete source allowances; run the quality ratchet with --prune`

Add the owners row, then:
```bash
mkdir -p target/quality
cargo build --locked -p streams-quality-syntax
cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl
python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl --prune   # C3 only; rewrites source-allowances.json
python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl          # no failures printed, exit 0
python3 scripts/architecture-gate.py --check                                 # exit 0
python3 scripts/test-inventory.py --check                                    # exit 0, manifest untouched
python3 scripts/scenario-map-report.py --check                               # exit 0
bash scripts/multitenancy-audit.sh                                           # exit 0, no NEW/GONE fingerprints
bash scripts/quality.sh                                                      # last line: QUALITY_OK
```

**CI's mutation plan and run (after C7), mirroring the push:**
```bash
QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) \
  python3 scripts/quality/verification_plan.py --out target/quality-plan
jq '.mutation_source_files, .selected_mutation_owners, .unregistered_mutation_source_files' target/quality-plan/plan.json
#   includes "src/sse/auth.rs" and "sse_auth"; unregistered == []
QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) scripts/quality/mutations.sh
#   src/sse/auth.rs: exactly 1 mutant (lease_refusal_response -> Default::default()), caught; 0 missed, 0 timeout
```

---

## 8. Out of scope

- **`CredentialInactive` 401 → 403, the lease check order, and the lease `ProjectMissing` 403 vs 421.** Wire decisions (§9).
- **Token-expiry skew.** `verify_customer` tolerates `CLOCK_SKEW_SECS` (30 s) on `exp`, while `lease_check` ends at the exact `exp` (`now_unix >= l.expires_at`). This is deliberate: the lease "terminates no later than token expiry". Credential expiry is the same predicate on both paths, written `cexp <= now` vs `now >= e`, which is cosmetic.
- **`AuthService::workspace_for`** (`auth.rs:833`, used by billing at `billing.rs:557,1047`) reads the policy with no freshness or placement check. That is attribution ("who owns P"), not authorization currency. Worth its own item.
- **A misplaced doc block.** `auth.rs:456-467` is the "Monotonic publication" doc for `publish_*`, but it is attached to `staleness_max_secs`. It is a doc fix for `auth/publication.rs`.
- **Wire text.** The 421 message's embedded 14-space run (`product.rs:669`), and SSE lease refusals using the raw `err_resp` envelope on product routes (WIRE-MATRIX:247). Both are wire changes.
- **Merging verification and lease into one currency proof.** The reviewer says to defer this. After this item they share the placement predicate, the freshness boundary and the refusal classes.
- **Registering `src/auth/*` as mutation owners.** It is security-relevant but not on the critical list today.

---

## 9. Decisions for Søren

None of these land in C1–C7. Each would be its own later commit, with the named red test.

1. **An inactive credential on lease establishment: 401 → 403 `credential_inactive`**, to match the request path's 403 `credential_not_active`. This is the reviewer's change.
   - **Wire effect:** only subscriptions refused at construction change. A live termination has no status, and the counter key is unchanged. WIRE-MATRIX:133 moves `credential_inactive` from 401 to 403.
   - **If yes:**
     - In `LeaseInvalidReason::refusal`: `Self::CredentialInactive => Refusal::Denied(Denial::Credential)`.
     - In `lease_check`: move the credential project-mismatch test (`GrantChanged`) ahead of the status test, mirroring verification's `CredentialProjectMismatch`-before-`CredentialNotActive` order.
     - Flip P2's oracle row. Red, since `StatusCode`'s Debug prints the number: `assertion `left == right` failed: credential_inactive` / `left: 401` / `right: 403`.
   - **Backward-compatible alternative (status quo):** keep 401. The client refreshes its token, and its next request meets verification's 403 `credential_not_active`: one extra round trip, no contract change.
2. **Lease check order: ownership before status**, mirroring `verify_customer`'s guard at `auth.rs:721-730`, which stops an old owner learning the new owner's project status.
   - **Wire effect:** for a lease whose project both changed owner and went inactive, establishment answers 401 `ownership_changed` instead of 403 `project_not_active`, and `/v1/debug/load` `lease_terminations` counts it under `ownership_changed`. Both are existing keys.
   - **Backward-compatible alternative:** the status quo order. The leak is limited to a narrow race at establishment and to a counter operators see.
3. **A lease project not served here (absent or placed elsewhere): 403 `project_missing` vs 421.**
   - **The divergence:** the request path answers 421 `wrong_cell`, which tells the client to re-resolve. The lease path answers 403, which a client reads as "do not retry".
   - **Option:** classify `ProjectMissing` as `Refusal::WrongCell`, so the code stays `project_missing` with no new metric key, and the status becomes 421.
   - **Backward-compatible alternative:** keep 403. A client's reconnect then meets verification's 421. C6 deliberately reuses today's `ProjectMissing`/403, so it adds no new wire surface.

C7 (capability placement) is included as a bug fix, restoring `status_and_quotas`'s documented `Ok(None)` = "not served here". It is a separate commit and can be dropped independently; it is not a decision.

---

## Skeptic corrections (C1..C10)

I checked this plan against the tree at `6669d3b5`. `src/auth.rs`, `src/sse/auth.rs` and `src/product.rs` are unchanged from `origin/slate`. Line counts: auth.rs 1,676; product.rs 4,205; sse/auth.rs 518; product/tests.rs 335; http.rs 3,155; shard.rs 3,186; billing.rs 2,157; history.rs 1,713; registry.rs 1,492; sse/feed.rs 1,165; fleet.rs 1,142.

**Claims that check out:**
- **Quoted lines.** auth.rs 187-201/202-249/485-552 = 131 lines, so C1 → 1,547. `index()` is at 245-247. The cell check is at 714-720, `lease_check` at 503-505, `status_and_quotas` at 867-870. `feed_json` uses the constant at 894/900. The mappers are at sse/auth.rs:95-99 and product.rs:666-711.
- **AuthError.** It has 27 variants: 8 are mapped explicitly and 19 fall to `_`.
- **Use-site lists.** They are complete. I grepped src (dst included), tools, fuzz and bench.
- **Fixtures.** Every AuthService and policy fixture shares one cell. There are two exceptions. `auth_feed.rs` uses the service cell "cell" with policies on "fra-cell-07", but it only calls `feed_json`. `security_modes` `proj_far` only goes through `verify_customer`.
- **The `place_on` publish passes every high-water check.** It keeps ownership 12, goes to policy 41 ≥ 40 with the same workspace, gets a new version pair and moves feed 40→41 (`publication.rs:85-116,382-419`).
- **The red traces for R1, R2 and R3 and their expected output are correct.**
  - `Value: PartialEq<bool>` makes R1's left side print as `Bool(false)`.
  - The test helpers publish at `NOW`, the token has `exp NOW+600` and there is no credential expiry. So `lease_deadline == edge` holds.
- **Exception ratchets.**
  - auth.rs has no `#[expect]`/`#[allow]`.
  - The aliases the quality-syntax tool resolves are lexically scoped (`tools/quality-syntax/src/imports.rs` `with_block`). So the fn-local `use … as R/D` in `auth_failure_response` and the new top-level `use` lines in sse/auth.rs change no fingerprint in the `nap`/`GatedSseBody::new`/`poll_next` scopes. `poll_next` names neither `AtomicU64` nor `LeaseInvalidReason` bare.
  - The §2 argument against moving `auth_failure_response` is correct. The call-site fact is `target\ttokens` (`scan.rs:246-253`), and `product.rs:3904` sits in `product_list`'s `unwrap_used` scope (3883-3891).
- **The C3 global-row mechanics are correct.**
  - The `static` fact is the type's tokens (`scan.rs:199-205`).
  - `owners.json` already has the precedent `crate::sse_stats::FEED_CUTOFF_ENGINE_RETIRED`.
  - `legacy-source.json` cannot grow.
- **Mutation.** The only critical file touched is `src/sse/auth.rs`, with owner `sse_auth` and filter `sse::`. The only selected mutant is `lease_refusal_response → Default::default()`, which returns 200 with an empty body. P2 kills it. No arm-deletion mutants remain once the wildcard is gone.
- **Ledgers.** `test-inventory` only scans `src/dst`. `review-mechanisms` does not pin these files. The scenario map does not check line numbers. The active diagnostic ledgers are empty. `mt-audit-baseline` has no auth rows. The new `auth/*` files have no `crate::http`/`crate::product` edges.

**C1. C4 does not compile clean as written. Delete the now-unused local alias at `src/sse/auth.rs:94`.** `use crate::auth::LeaseInvalidReason as R;` is only used by today's match arms. Once the match is `match r.refusal() { Refusal::… }`, `R` is unused, `unused_imports` fires, and `clippy -D warnings` fails. Delete line 94, and optionally spell the parameter as `LeaseInvalidReason` using C3's top-level import. The deletion is inside the body that is already selected, so it adds no mutant. The plan's arms also need the `StatusCode::` prefix: they currently read `MISDIRECTED_REQUEST` etc. bare.

**C2. C4 `refusal.rs`: `Denial` needs `#[derive(Clone, Copy, Debug, Eq, PartialEq)]`.** `Refusal` derives those traits over `Denied(Denial)`, and product.rs compares `refusal == R::WrongCell`. The plan gives the derive only on `Refusal`, so as written the build fails with E0277.

**C3. C3 changes a silent miscount into an out-of-bounds panic on the SSE hot path, and P1 cannot see it.**
- With `index() = self as usize`, a new reason that is declared but missing from `ALL` gets slot 10 in a 10-slot array. `LEASE_TERMINATIONS[r.index()]` then panics in `TerminateOnce::record_once` (`sse/auth.rs:160`). That is reached from `LeaseWatch::revoked_at` (`sse/auth.rs:269,281`), which runs inside `GatedSseBody::poll_next`.
- P1 iterates `ALL`, so it can never observe a variant that is missing from `ALL`. On the C1/C2 tree P1 is also tautological, because `position` equals the enumerate index for any `ALL` without duplicates. It only pins order after C3.
- **Fix:** in `src/auth/lease.rs`, beside `ALL`, spell the length from the discriminant and add a compile-time order check:
  - `pub(crate) const ALL: [Self; Self::CredentialExpired as usize + 1]`
  - `const _: () = { let mut i = 0; while i < LeaseInvalidReason::ALL.len() { assert!(LeaseInvalidReason::ALL[i] as usize == i); i += 1; } };`
  - This turns an omission, reordering or mid-enum insertion into a compile error.
- In `index()`'s doc, state the one remaining hole: a reason appended after `CredentialExpired` must move the length expression.
- Say in §2 that the failure mode is now a panic, not a miscount.
- None of this adds a mutant in CI's scope (`lease.rs` is not critical) or a source-rule identity (it is a const item, not a static).

**C4. Take C7 out of the landed commits and make it decision 4 in §9.**
- The task authorizes three non-wire fixes: the lease-path cell refusal, exhaustive `index`/mappers, and one staleness predicate. C7 is a new refusal that the product edge can see.
- §1a is also too narrow. `status_and_quotas` is called for **every** enforce-mode watch waiter, not only for capabilities (`src/application/watch.rs:283-288`). That includes `WatchAccess::AdmittedAccount` principals that were verified by JWT.
- After C7, a watch wait whose policy names another cell answers 403 `watch_unauthorized`, and that response is **audit-tagged and journaled** (`product.rs:4123-4135`). That contradicts the §10.4 journal scope that `auth_failure_response` states (`product.rs:703-707`): placement is never journaled as a denial, and the request path answers 421 `wrong_cell` without journaling it.
- **The options for Søren:**
  - (a) 403 `watch_unauthorized`, journaled, which is the plan's C7;
  - (b) 421 `wrong_cell`, not journaled, which matches the request path;
  - **backward-compatible alternative:** the status quo. The per-cell feed (`docs/CONTROL-PLANE-INTEGRATION.md:378`; the emulator's `own` filter at `platform-demo/src/emulator.mjs:122`) already *omits* a transferred project, which yields `Ok(None)` today. Only a Control Plane that publishes a foreign `cell_id` reaches the gap.
- **Consequences:**
  - R3 moves into the decision.
  - `served_policy` gets two callers in C6.
  - The auth.rs total drops by about 13 lines.
  - Leave WIRE-MATRIX:168 unchanged.

**C5. §1a overstates the third bullet, "A subscription established after the move is admitted".**
- A product SSE subscription is established only after `enforce_customer` → `verify_customer` (`product.rs:748`), which answers 421 once the policy names another cell.
- Only the window between that verification and `LeaseWatch::new_checked` (`sse/auth.rs:201-214`) admits such a subscription.
- The live-lease survival (bullet 2) is the real bug. Fix the wording; no code change.

**C6. The C7 fallback as written trips a lint.** Declaring `pub(super) fn claims() -> C` while `struct C` stays private in `auth::tests` fires `private_interfaces`, a warn-level lint, so `-D warnings` fails. If the fallback is used, mark `C` (and `NOW`, `place_on`) `pub(super)` as well. Otherwise use `super::super::tests::…` paths with those items made visible.

**C7. The new tests must not use non-expression macros.**
- Any macro outside `EXPRESSION_MACROS` (`scripts/quality/source_rules.py:15-19,52-54`) is a new `macro-dsl` identity and fails the gate with "unregistered source occurrence" until an `owners.json` row exists. The obvious one is `serde_json::json!`.
- This applies to P1/P2 in `src/sse/auth.rs`, P3 in `src/product/tests.rs`, and R1-R3/`place_on` in `src/auth.rs`.
- Build the expected bodies with `serde_json::from_slice`/`Value` indexing only.
- P1's `Map::len` needs `use serde_json::Map;`, or spell it `serde_json::Map::len`.

**C8. The auth.rs budget is tighter than the estimate.**
- rustfmt splits every `assert_eq!` that carries a message, and the R1 `past` assertion lands exactly at 100 columns at an 8-space indent.
- Expect R1 at about 40 lines, `place_on`+R2 at about 35, and R3 at about 14.
- The C7 total is then about 1,665-1,670 against the limit of 1,676. With C4 (C7 moved to a decision), the final is about 1,655.
- Run `wc -l` after every commit, as §7 already says. The fallback in C6 above stays available.

**C9. C3 prune: check the prune diff is exactly one row.** `gate.py --prune` rewrites `docs/quality/source-allowances.json` wholesale as `active & current` (`source_gate.py:63-65`). If another in-flight item on this tree has left an allowance stale, that item's row would be dropped in this commit too. Before committing, check that `git diff docs/quality/source-allowances.json` removes only `crate::LEASE_TERMINATIONS` / `[std :: sync :: atomic :: AtomicU64 ; 10]`.

**C10. WIRE-MATRIX citations: optional, but fix them correctly if you touch them.**
- The citations are already stale on the current tree:
  - line 15 cites `product.rs:660-708`, while `auth_failure_response` is at 657-712;
  - line 168 cites `product.rs:7157-7479` in a 4,205-line file.
- No script gates this file: `rg WIRE-MATRIX scripts tools .github` finds nothing.
- The C4/C6 edits are documentation-only. If you edit them, update the ranges from the committed tree, and do not claim a gate verifies them.

**Unbuildable controls:** none are blocking.
- The C1 verbatim `diff` spans depend on the header length of `lease.rs`. The plan already says to adjust them.
- The §7 `rg '_ =>'` check is sound. It deliberately does not match the class sub-pattern `Denied(_) =>`, which is intended because the class semantics cover it.

**Missed ledgers:** none required for C1-C6.
- The `owners.json` global row and the `source-allowances.json` prune in C3 are correct. Scope the prune as C9 says.
- If Søren later approves C7 (decision 4), WIRE-MATRIX:168 belongs in that commit.

**Verdict: ready-with-corrections.**
- Apply C1 and C2 to C4, otherwise the build fails.
- Harden C3 as in C3 above.
- Move C7 to decision 4 as in C4.
- The rest is wording or checks.
