# Item 9: the raw PUT recreate CAS ignores `fork_children`

Base: `slate` at `0a8ed40a`. The only commit after `0afa2597` changes quality
scripts, so every file this plan touches is the same as at `0afa2597`. The
working tree has someone else's uncommitted edits in
`docs/refactor/architecture-policy.json`, `docs/review-verification-evidence.md`
and `scripts/architecture-gate.py`. This plan touches none of them.

Scratch drafts, all rustfmt-clean (`rustfmt --edition 2024 --check`), are in
`plans6/probe/`: `claim.new.rs`, `product.new.rs`, `creation.new.rs`,
`append.new.rs`, `incarnation_tests.rs`, `product_test.rs`, `creation_unit.rs`
and `claim.diff`.

---

## 1. Problem (checked on the current tree)

### 1.1 The raw recreate CAS does not check for forks

`src/application/creation/claim.rs`, inside `resolve`. The only check for
forks runs against the **snapshot**:

```rust
36        Some(d) if desc_alive(&d) => match validate_live(plan, d) {
  ...
40        Some(d)
41            if d.soft_deleted
42                || (!d.fork_children.is_empty()
43                    && !d.deleted
44                    && d.expires_at_ms.map(|e| now_ms() >= e).unwrap_or(false)) =>
  ...
48            return Err(CreationError::new(
49                CreationFailure::Conflict,
50                "gone",
51                "name is soft-deleted; live forks retain its data",
```

The CAS that actually replaces the incarnation, run against the **stored**
descriptor, has no fork check:

```rust
73            match state
74                .registry
75                .recreate(&plan.sref, fresh, |d| !desc_alive(d) && !d.soft_deleted)
  ...
78                Ok((true, d)) => (true, d),
79                Ok((false, winner)) => match validate_live(plan, winner) {
```

The snapshot comes from `state.registry.get(&plan.sref)` (claim.rs:21). That
call is served from the descriptor cache (`src/registry/cache.rs:294-310`,
`self.cache.fill(...)`), whose TTL is 5 s:
`cache: cache::DescriptorCache::new(Duration::from_secs(5)),` (registry.rs:940).
`Registry::recreate` evaluates the closure on the descriptor it just fetched
and returns it when the closure declines (registry.rs:1032-1034:
`if !still_dead(&current) { self.invalidate(sref); return Ok((false, current)); }`).

A fork's reference is written by the fork child's creation path, not by the
source's owner. `anchor.rs:76-106` runs `mutate_incarnation` on the source
from the child's instance, and that instance invalidates only its own cache
(`state.registry.invalidate(&fc.source_desc.sref());`, anchor.rs:132). The
install predicate refuses `deleted`, `soft_deleted`, `init`, a different epoch,
`sealing` and splits. It does **not** refuse an expired source. So a fork that
was validated while its source was live (`fork.rs:85`,
`Ok(Some(d)) if desc_alive(&d) => d,`) can anchor after the source expires.

Two sequences reach the bug.

- **(a) Across instances.** Instance A owns source `S` and caches it: live,
  short expiry, no children. Instance B, which owns the child `F`, installs
  `F` into `S.fork_children`. `S` expires. Within 5 s a raw `PUT S` reaches A.
  The snapshot is expired, has no children and is not soft-deleted, so the
  guard at 40-44 does not fire. The CAS closure is true on the stored
  descriptor (expired, children `[F]`), so `S` gets a fresh epoch with
  `fork_children: Vec::new()` (`fresh_desc`, creation.rs:214). The PUT answers
  201.
- **(b) One instance, check-then-act.** The PUT reads `S` (expired, no
  children), then `F`'s anchor lands (the fork was prepared before expiry),
  then the CAS overwrites `S`.

The damage is in `src/application/read.rs:436-445`:

```rust
437                Ok(Some(d)) if !d.deleted => d,
...
440            if !want_epoch.is_empty() && d.stream_epoch != want_epoch {
441                return Err(format!(
442                    "fork source '{src}' is a different incarnation ...
```

Every read of `F` now fails. `fork_lifecycle_is_idempotent_and_epoch_checked`
pins that error as HTTP **500**. No descriptor names the old epoch any more,
so `F`'s inherited records can never be read again.

### 1.2 The declined arm answers "exists" for a deleted name

The same closure also declines a **soft-deleted** winner (`!d.soft_deleted`).
Line 79 then runs `validate_live` on that winner, which checks only config and
key and returns `Ok((false, d))`. `raw.rs` then answers **200** for a name
whose appends answer 410. This is reachable when the snapshot is dead but not
retained (for example, a cached tombstone of the name's previous
incarnation) while the store holds a soft-deleted incarnation that its forks
retain. The test in §3.2 checks this.

### 1.3 Product path: the CAS checks forks, but a declined winner is misclassified

`src/application/creation/product.rs:113-115` already checks for forks in the
CAS:
`!desc_alive(d) && !d.soft_deleted && d.fork_children.is_empty()`. So sequences
(a) and (b) do **not** affect product creates. The declined arm is another
hand copy that ignores liveness:

```rust
122                } else if winner.soft_deleted || !winner.fork_children.is_empty() {
123                    return Err(CreationError::new(
124                        CreationFailure::Conflict,
125                        "gone",
126                        "name is retained for live forks",
```

When a **live** winner has forks (another instance recreated and forked the
name while this instance cached the old tombstone), this answers 409 `gone`.
The same stream without the race would get the idempotent comparison.

### 1.4 The hand copies of "retained for forks"

| site | text |
|---|---|
| `creation.rs:42-47` (`CreationError::gone`) | `d.soft_deleted \|\| (!d.deleted && !d.fork_children.is_empty() && d.expires_at_ms.is_some_and(\|expires\| now_ms() >= expires))` |
| `claim.rs:40-44` | the same, spelled `.map(..).unwrap_or(false)` |
| `append.rs:63-67` | the same |
| `creation/product.rs:114` and `:122` | CAS `... && d.fork_children.is_empty()`, and the liveness-blind classification above |

The first three are term-for-term the same predicate. The product CAS differs
only for `deleted && !fork_children.is_empty()`. That state cannot occur: a
tombstone is written only when the child set is empty
(`delete_transition`, deletion.rs:470-478; the release cascade, deletion.rs:164-166),
and `anchor::install` declines a `deleted` source (anchor.rs:92).

**The reviewer's diagnosis is confirmed.** The corrections to the reviewer's
**Change** are in §2.3.

---

## 2. Contract decision

### 2.1 Typed contract, with one owner

The owner is `src/application/creation.rs`, next to `desc_alive`, which is
already the crate's time-dependent liveness owner (re-exported at
`http.rs:2396`):

- `fn alive_at(desc, at_ms) -> bool` is private. `desc_alive(desc)` becomes
  `alive_at(desc, now_ms())`, with identical behaviour.
- `pub(crate) fn retained_for_forks(desc, at_ms) -> bool` means soft-deleted,
  or expired with children and not deleted. It is exactly the existing hand
  copies.
- `pub(crate) fn recreatable(desc, at_ms) -> bool` is
  `!alive_at && !retained_for_forks`. It is the recreate CAS predicate for both
  surfaces.

The caller supplies the instant (`let now = now_ms();`), once for the CAS and
the classification of the winner it declines. With one instant, a declined
winner is either retained or live, and nothing else. The classification is
therefore two arms: retained gives 409 `gone`, and anything else goes to
`validate_live`.

### 2.2 What changes at the edge (no new code, status or message)

| surface / window | today | after |
|---|---|---|
| raw PUT, stale snapshot or check-then-act, **expired source with live forks** (1.1) | **201**, fresh epoch; every fork read then 500 "different incarnation"; the old incarnation's records are unreachable | **409 `gone`** "name is soft-deleted; live forks retain its data". This is what the race-free path answers today, pinned by `fork_lifecycle_and_stitched_reads`, and it is listed in WIRE-MATRIX §1.1 |
| raw PUT, CAS declined on a **soft-deleted** winner (1.2) | **200** "exists" (appends then 410) | **409 `gone`** |
| raw PUT, CAS declined on a **live** winner | 200 / 409 `config_mismatch` / 403 `wrong_key` | unchanged |
| product PUT, CAS declined on a **live winner with forks** (1.3, commit 3) | **409 `gone`** "name is retained for live forks" | the idempotent comparison: **200** / 409 `config_mismatch` / 403 `wrong_key`. This is what the race-free path answers today |
| append / delete of a retained name | 410 `gone` | unchanged; the same predicate, now called from one place |

Backward-compatible alternatives:

- **D1 (raw, commits 1-2).** No alternative preserves data. Keeping the 201
  is the bug. A narrower option would fix only the CAS closure and keep
  `validate_live` for every declined winner, which keeps the 200 for
  soft-deleted names. It is not recommended: it tells a client "exists" for a
  name whose appends answer 410.
- **D2 (product, commit 3).** Swap only the product CAS closure to
  `recreatable(d, now)`. That part is behaviour-neutral and can move into
  commit 2. Keep the old classification line
  `winner.soft_deleted || !winner.fork_children.is_empty()`, and with it the
  409 for a live winner that has forks.

No WIRE-MATRIX edit is needed. §1.1 already lists `409 gone (name retained for
forks)`, the product row lists `409 gone (name retained for live forks)`, and
the codes do not change.

### 2.3 Where this differs from the reviewer's Change, and why

1. **Placement.** The review proposed a new `src/registry/lifecycle.rs`. This
   plan uses `src/application/creation.rs`. `mod lifecycle;` would add a line
   to `src/registry.rs` (1,509 lines, ceilinged), which forces a verbatim-move
   commit out of registry.rs first. The predicates must share `alive_at` with
   `desc_alive`, which lives in `application::creation`. Registry's
   `Lifecycle` enum does not depend on the clock by design (`RetainedForks`
   there means soft-deleted only). Moving `desc_alive` too would re-fingerprint
   every ratcheted caller that resolves it through the `crate::http::desc_alive`
   alias (billing.rs:967, product.rs:1548…). This is **D3**.
2. **No third "not alive → retryable" arm.** With one instant for the CAS and
   the classification, "declined, not retained, not live" cannot happen. The
   arm would be unreachable code with a mutant nothing can kill. This is
   **D4**.
3. **Red DST location.** The review proposed `fork_lifecycle.rs`, which is
   976 lines. The tests are about 150 lines, which would push it over 1,000,
   and that is blocked. They go in `lifecycle_incarnation.rs` (711 to 872
   lines), whose subject is that an incarnation is never replaced or touched by
   a later one. The product test goes in `product_lifecycle.rs` (819 to 871).
4. **No failpoint.** `registry.test_poison_cache` (registry/cache.rs:279, "the
   cross-instance stale-descriptor shape") reproduces sequence (a)
   deterministically. There is no `pause_fork_before_source_ref`, no
   `tokio::spawn` and no `gap_lock`, so owners.json needs no effect rows.

---

## 3. Red tests

All three are appended at the end of their file, after a blank line. Test
files are ceiling-checked: `lifecycle_incarnation.rs` goes from 711 to 872
lines and `product_lifecycle.rs` from 819 to 871. Each function has 45-81 lines
and nesting depth 2, and uses only `assert*`/`format!` macros (no macro-dsl
rows). No test carries an `#[expect]`. The full texts are in
`plans6/probe/incarnation_tests.rs` and `plans6/probe/product_test.rs`.

### 3.1 `dst::dst_tests::lifecycle_incarnation::a_raw_recreate_never_replaces_an_expired_source_its_forks_read` (commit 1)

Steps:

1. Raw PUT `keptsrc` with `[{"n":0}]`, expect 201.
2. Take `unforked = registry.get(keptsrc)`.
3. Raw PUT `keptchild` with `stream-forked-from: keptsrc`, expect 201.
4. `cas_update_retry(keptsrc, expires_at_ms = now-1)`.
5. Take `retained = get(keptsrc)` and assert `fork_children.len() == 1`.
6. `test_poison_cache(keptsrc, unforked with expires_at_ms = now-1)`. This is
   the snapshot of an instance that never saw the fork.
7. Raw PUT `keptsrc` with `content-type: application/json` and an empty body.

Assertions after step 7:

- Status **409** and `error.code == "gone"`.
- After `invalidate`, the stored `stream_epoch` and `fork_children` equal
  `retained`'s.
- GET `keptchild` returns 200 with exactly 1 inherited record.
- POST `keptsrc` returns **410**. This pins commit 2's `append.rs` change; it
  already passes today.
- DELETE `keptsrc` returns **410**. This pins commit 2's `CreationError::gone`
  change; it already passes today.
- Control: `keptfree` is created, expired, and re-PUT. Expect **201** and a
  different epoch, so the check does not over-refuse. This also passes today.

**Exact red on the current tree** (first failing assertion; the 201 body is
empty):

```
thread 'dst::dst_tests::lifecycle_incarnation::a_raw_recreate_never_replaces_an_expired_source_its_forks_read' panicked at src/dst/tests/lifecycle_incarnation.rs:753:5:
assertion `left == right` failed: a source retained for its fork was recreated: 
  left: 201
 right: 409
```

### 3.2 `dst::dst_tests::lifecycle_incarnation::a_raw_put_behind_a_stale_tombstone_never_revives_a_deleted_source` (commit 1)

Steps:

1. Raw PUT `heldsrc`, expect 201.
2. Fork `heldchild` from it, expect 201.
3. DELETE `heldsrc`, expect 200 or 204. The source is now soft-deleted and
   retained.
4. Poison the cache with the previous incarnation's tombstone: the stored
   descriptor with `soft_deleted=false`, `deleted=true`, children cleared and
   epoch `0…01`.
5. Raw PUT `heldsrc` with `content-type: application/json` and an empty body.

Assertions after step 5:

- Status **409** and `error.code == "gone"`.
- GET `heldsrc` returns 410.
- GET `heldchild` returns 200.
- Control: `heldlive` is live and sits behind the same kind of stale
  tombstone. Its re-PUT answers **200**. This pins the validate-as-live arm
  and already passes today.

**Exact red on the current tree** (the 200 body is empty):

```
thread 'dst::dst_tests::lifecycle_incarnation::a_raw_put_behind_a_stale_tombstone_never_revives_a_deleted_source' panicked at src/dst/tests/lifecycle_incarnation.rs:841:5:
assertion `left == right` failed: a PUT was answered for a deleted name: 
  left: 200
 right: 409
```

### 3.3 `dst::dst_tests::product_lifecycle::a_product_create_that_loses_to_a_live_forked_incarnation_is_idempotent` (commit 3, D2)

Steps:

1. Product PUT `livefork` with `{"format":{"kind":"json"}}`, expect 201.
2. `cas_update_retry` pushes child `"livefork-child"`. This stands for
   another instance forking the collection.
3. Take `live = get(livefork)`.
4. Poison the cache with a tombstone of a previous incarnation (`deleted=true`,
   children cleared, epoch `0…01`).
5. Product PUT the same body again.

Assertions after step 5:

- Status **200**.
- The stored epoch and children are unchanged.

**Exact red on the current tree.** It is still red after commits 1 and 2,
which do not touch product.rs. The serde_json map has no `preserve_order`, so
keys are sorted:

```
thread 'dst::dst_tests::product_lifecycle::a_product_create_that_loses_to_a_live_forked_incarnation_is_idempotent' panicked at src/dst/tests/product_lifecycle.rs:854:5:
assertion `left == right` failed: a live forked collection was refused as retained: {"error":{"code":"gone","message":"name is retained for live forks","retryable":false}}
  left: 409
 right: 200
```

### 3.4 Unit test for the partition: `application::creation::tests::a_name_is_live_retained_or_recreatable_at_one_instant` (commit 1)

This is not a behaviour red. The functions are new, so it does not compile on
the current tree. It fixes the invariant that the claim and product arms rely
on: at one instant every descriptor is exactly one of live, retained for forks
or recreatable, **including at the expiry instant itself**. It uses a table of
7 cases over `(expiry, deleted, soft_deleted, children)`. It goes inside the
existing `#[cfg(test)] mod tests` of `creation.rs`, after
`r05_cancelled_ttl_attempt_releases_only_its_owned_slot`. The text is
`plans6/probe/creation_unit.rs` minus the `mod tests {` wrapper, about 57
lines.

---

## 4. Edits file by file, in commit order

No verbatim-move commit is needed, because **no ceilinged file is touched**.
Budgets are unchanged:

| file | lines now | lines after |
|---|---|---|
| http.rs | 3,371 | 3,371 |
| product.rs | 4,205 | 4,205 |
| shard.rs | 3,232 | 3,232 |
| billing.rs | 2,201 | 2,201 |
| history.rs | 1,713 | 1,713 |
| auth.rs | 1,676 | 1,676 |
| registry.rs | 1,509 | 1,509 |
| sse/feed.rs | 1,200 | 1,200 |
| fleet.rs | 1,143 | 1,143 |

The files that are touched are not ceilinged:

| file | lines now | lines after |
|---|---|---|
| `creation.rs` | 316 | ≈393 (≈388 after commit 2) |
| `creation/claim.rs` | 273 | 277 |
| `creation/product.rs` | 178 | 178 |
| `append.rs` | 429 | 426 |
| `dst/tests/lifecycle_incarnation.rs` | 711 | 872 |
| `dst/tests/product_lifecycle.rs` | 819 | 871 |

Functions touched that carry an `#[expect]`:

| function | exception | ratchet | effect |
|---|---|---|---|
| `claim.rs::resolve` | `too_many_lines` (reason unchanged) | scope_lines, syntax_facts, nested_items | scope_lines **158 → 152** (5..156). syntax_facts **−5**: removes the 19-fact snapshot guard, the 4-fact closure and the 11-fact declined `match`; adds `let now` (2), the snapshot guard (9), the closure (4), the retained declined arm (9) and the live declined `?` arm (5). nested_items unchanged (closures are not items). The expectation stays fulfilled (>100 lines). |
| `creation/product.rs::create_product` | `too_many_lines` (reason unchanged) | same | scope_lines **129 → 129** (lines 20..148). A 3-line closure becomes 1 line, and 1 comment line plus `let now` are added. syntax_facts **−1** (11 removed, 10 added). |
| `Registry::recreate` (registry.rs) | `expect_used`, function-wide, fingerprinted | — | **not edited**. The changed closures are written at the callers, outside its scope. |
| `creation.rs` tests `r05_cancelled_ttl_attempt_releases_only_its_owned_slot` | `excessive_nesting` | — | untouched. The new test is a sibling, not inside its scope. Its body hash pin in `review-mechanisms.json` (`source_adaptations[6]`) stays valid. |
| `append.rs::AppendService::prepare`, `creation.rs::CreationError::gone`, `desc_alive` | none | — | — |

No `#[expect]` is added, removed or made unfulfilled. No `_ =>` arms, no
`name: String` parameters, no `.stream_ref(` calls and no new files are
added.

### Commit 1: "A raw PUT never replaces a name its forks read through, and a declined recreate answers the winner's verdict"

**Red first.** Add §3.1 and §3.2 alone and run control 1. Expect exactly the
two panics above.

**`src/application/creation.rs`.** Replace `desc_alive` (lines 141-145) with
the four functions below (probe `creation.new.rs`). Leave `CreationError::gone`
alone until commit 2. Add the §3.4 unit test to `mod tests`.

```rust
pub(crate) fn desc_alive(desc: &crate::registry::PersistedDescriptor) -> bool {
    alive_at(desc, now_ms())
}
/// Liveness at the caller's instant: the recreate CAS and the classification
/// of the winner it declines on must judge one instant, or a winner expiring
/// between the two would be neither live nor retained.
fn alive_at(desc: &crate::registry::PersistedDescriptor, at_ms: i64) -> bool {
    !desc.deleted && !desc.soft_deleted && desc.expires_at_ms.is_none_or(|expires| at_ms < expires)
}
/// A dead incarnation whose epoch and records its live forks still read
/// through (pinned fork lifecycle): soft-deleted, or expired with children.
/// Creation, append and delete answer it as gone and nothing may replace it
/// until the last fork releases it, so this is the one place that decides it.
pub(crate) fn retained_for_forks(desc: &crate::registry::PersistedDescriptor, at_ms: i64) -> bool {
    desc.soft_deleted
        || (!desc.deleted
            && !desc.fork_children.is_empty()
            && desc.expires_at_ms.is_some_and(|expires| at_ms >= expires))
}
/// The recreate CAS predicate. It judges the STORED descriptor, because a
/// cached snapshot can predate a fork another instance anchored, and replacing
/// a retained source strands every fork reading through it.
pub(crate) fn recreatable(desc: &crate::registry::PersistedDescriptor, at_ms: i64) -> bool {
    !alive_at(desc, at_ms) && !retained_for_forks(desc, at_ms)
}
```

**`src/application/creation/claim.rs`.** The exact diff is `plans6/probe/claim.diff`.

- After `resume_initialization`, add
  `// One instant judges the stored descriptor inside the CAS and the winner`,
  `// a declined CAS returns: a declined winner not retained for forks is live.`
  and `let now = now_ms();`.
- Leave the `desc_alive` Live arm untouched. Keeping it off the diff means no
  in-diff mutant is generated on it (it is a fast path equivalent to the CAS).
- Replace the 14-line guard arm with:
  `// The name still backs live forks: blocked, not recreated` /
  `// (pinned fork lifecycle).` /
  `Some(d) if retained_for_forks(&d, now) => return Err(retained_name()),`
- Replace the recreate-arm comment with: "The CAS judges the STORED
  descriptor, not the snapshot above: a snapshot can predate a fork anchored
  by another instance, and replacing a retained source strands every fork
  reading through it. One winner; a live winner validates like an idempotent
  PUT."
- CAS closure: `.recreate(&plan.sref, fresh, |d| recreatable(d, now))`.
- Split the declined arm:
  `Ok((false, winner)) if retained_for_forks(&winner, now) => { return Err(retained_name()); }`
  then `Ok((false, winner)) => validate_live(plan, winner)?,`.
- Add, after `resolve`, a helper that keeps the raw message byte-identical:
  ```rust
  /// The pinned fork lifecycle blocks re-creation while forks read through the
  /// name: its epoch and data are theirs until the last reference is released.
  fn retained_name() -> CreationError {
      CreationError::new(
          CreationFailure::Conflict,
          "gone",
          "name is soft-deleted; live forks retain its data",
      )
  }
  ```

**`src/dst/tests/lifecycle_incarnation.rs`.** Append §3.1 and §3.2. The file
needs no new imports: `hreq`, `http_rig`, `engine_shutdown` and `mem` are
already imported.

**Ledgers.** Update `docs/refactor/test-scenario-map.json` for FRK-019 and
regenerate `docs/refactor/SCENARIO-MAP.md` and `docs/refactor/test-inventory.json`
(details in §6).

### Commit 2: "Append and delete ask the retained-name owner instead of restating it" (behaviour-neutral)

- `creation.rs` `CreationError::gone`: lines 43-48 become
  `if desc.is_some_and(|d| retained_for_forks(d, now_ms())) {`. This is
  term-for-term the old predicate.
- `append.rs::prepare`: lines 63-67 become
  `crate::application::creation::retained_for_forks(d, now_ms())` inside the
  existing `is_some_and(|d| { … })` block. The dependency edge from `append`
  to `creation` already exists (`AppendService.creation`).
- There is no red, because behaviour does not change. §3.1's POST 410 and
  DELETE 410 assertions pin both sites, and existing tests do too
  (`fork_lifecycle_and_stitched_reads` 410 legs). There is no inventory
  change.

### Commit 3: "A product create that loses to a live forked incarnation is idempotent" (only if D2 is approved)

**Red first.** Add §3.3 and run control 4.

**`src/application/creation/product.rs`** (probe `product.new.rs`):

- In `Some(_) => {`, add `// One instant judges the CAS and the winner it declines on.`
  and `let now = now_ms();`.
- Closure: `.recreate(&sref, build_fresh(), |d| recreatable(d, now))`.
- Classification: `} else if retained_for_forks(&winner, now) {`. The message
  "name is retained for live forks" is unchanged.

Run `test-inventory --write`, which adds 1 entry.

If D2 is rejected, move only the closure line and `let now` into commit 2 and
drop §3.3.

---

## 5. Mutation-kill analysis

**What CI selects.** None of the changed paths is under `CRITICAL_PREFIXES`
(`scripts/quality/verification_plan.py:21-31`: `src/application/read_`, but not
`src/application/creation*` or `src/application/append.rs`). None is
registered in `scripts/quality/mutation_owners.py` (only
`src/registry/cache.rs`, which is untouched). The planner will therefore
report `mutants: false`, `mutation_source_files: []`, and also `loom`, `miri`
and `properties_fuzz` all false. **No owner rows or filters change.**

**Loom.** Loom is not applicable. This change adds no in-process
synchronization primitive. The ordering is the object store's ETag CAS inside
`Registry::recreate` (a conflicting write between its GET and PUT is a
`Precondition` retry that re-reads the children, already covered by
`registry::tests::recreate_race_has_one_winner`). The poisoned cache
reproduces the stale-snapshot interleaving deterministically.

Even though CI does not gate these files, every mutant cargo-mutants 27.1
would generate on the diff has a killing test. That matters if `creation` is
ever registered.

| mutant | killing test | why |
|---|---|---|
| `desc_alive` → `true` | §3.1 | the snapshot is "live", so validate_live answers 200, not 409 |
| `desc_alive` → `false` | §3.1 (fork PUT, line 3) | `fork.rs:85` needs a live source, so the fork PUT gets 404, not 201 |
| `alive_at` → `true` / `false`; deleting either `!`; either `&&`→`\|\|` | §3.4 (cases `None/…/1`, `None,true,…`) + §3.1 | the table asserts `live` per case |
| `alive_at` `<` → `<=` | §3.4 case `Some(at)` | at the expiry instant, liveness must be false |
| `alive_at` `<` → `==` / `>` | §3.4 case `Some(at + 1)` | must be live |
| `retained_for_forks` → `false` | §3.1 | the CAS recreates, giving 201 |
| `retained_for_forks` → `true` | §3.1 control `keptfree`, §3.4 | an expired childless name gets 409, not 201 |
| `retained`: delete `!deleted` / `!is_empty()`; `\|\|`→`&&` | §3.4 cases `Some(at)/1`, `None,false,true` + §3.1 | retained is asserted |
| `retained` first `&&`→`\|\|` (`(!del \|\| has_children) && expired`) | §3.4 case `Some(at)/0` + §3.1 `keptfree` | an expired childless name must not be retained |
| `retained` second `&&`→`\|\|` (`!del && (has_children \|\| expired)`) | §3.4 case `None/1` + §3.3 | a live name with forks is not retained. Without the fix, product would answer 409 |
| `retained` `>=` → `>` | §3.4 case `Some(at)/1` | expiry-instant boundary |
| `retained` `>=` → `==` / `<` | §3.4 case `Some(at - 1)/1` | expired earlier with a fork |
| `recreatable` → `true` | §3.1, §3.4 | the retained source is recreated |
| `recreatable` → `false` | §3.1 `keptfree`, §3.4 | the declined path runs validate_live, giving 200, not 201 |
| `recreatable`: delete either `!`; `&&`→`\|\|` | §3.4 (the tombstone case, the live case) | |
| claim guard `retained_for_forks(&d, now)` → `true` | §3.1 `keptfree` | a dead childless snapshot gets 409 |
| claim declined guard → `true` | §3.2 control `heldlive` | a live winner gets 409, not 200 |
| claim declined guard → `false` | §3.1, §3.2 | a retained winner runs validate_live, giving 200 |
| product declined `retained_for_forks(&winner, now)` → `true` | §3.3 | 409, not 200 |
| product declined → `false` | `fork_lifecycle::fork_lifecycle_is_idempotent_and_epoch_checked` | its product PUT on a soft-deleted source must be 409 |

**Equivalent mutant (claim.rs, not gated).** One mutant behaves the same as
the original: the claim snapshot guard `retained_for_forks(&d, now)` → `false`.
It is a fast path. When the snapshot is fresh the CAS declines the same
retained winner and answers the same 409, only after one more GET. The only
way to kill it would be a test that pins answering 409 from a *stale* retained
snapshot whose store copy has since been freed, which is the less correct
answer. If `creation` is ever registered, the fix is to delete that guard and
let the CAS decide. That changes the answer in that window from 409 to 201
and would need its own red and decision; see §8.

**Optional local run.** This mirrors `scripts/quality/mutation_driver.py`; it
is not required, because CI does not select these files:

```
git diff origin/slate -- src/application > $SCRATCH/item9.diff
cargo mutants --cargo-arg=--locked --cargo-arg=--lib --cargo-arg=--target-dir=target/mut-item9 \
  --baseline run --in-diff $SCRATCH/item9.diff \
  --file src/application/creation.rs --file src/application/creation/claim.rs \
  --file src/application/creation/product.rs --package streams-slate \
  --cargo-test-arg=dst::dst_tests::lifecycle_incarnation:: --cargo-test-arg=-- \
  --cargo-test-arg=dst::dst_tests::product_lifecycle:: \
  --cargo-test-arg=dst::dst_tests::fork_lifecycle:: --cargo-test-arg=application::creation:: \
  --profile quality --jobs 1 --timeout 90 --build-timeout 600 --gitignore true \
  --output $SCRATCH/mut-item9
```

Expect 0 missed, apart from the claim snapshot-guard `→ false` mutant
described above.

---

## 6. Ledgers

- **`docs/refactor/test-inventory.json`.** Commit 1 adds 2 entries (497 →
  499). Commit 3 adds 1 (→ 500). Run `python3 scripts/test-inventory.py --write`
  and check that `git diff` shows only the added entries, plus the
  `"scenarios": ["FRK-019"]` of §3.1.
- **`docs/refactor/test-scenario-map.json`** (commit 1). FRK-019 ("Expiry with
  fork references", catalog: *source expires with children; behaves as
  soft-deleted until final reference release*) is currently an unmapped GAP.
  Set it to:
  ```
  tests: [{"name": "a_raw_recreate_never_replaces_an_expired_source_its_forks_read",
           "file": "src/dst/tests/lifecycle_incarnation.rs", "line": 720}]
  mapped: true
  coverage: "partial"
  note: "Covers re-creation refused on the stored descriptor behind a stale
         snapshot, append/delete 410, child still reads. Not covered: the
         final-release cascade, and the source's own GET/HEAD, which answer
         404 (authorize_read maps expired-with-forks to Missing) where the
         catalog expects soft-deleted behaviour (410)."
  ```
  Then run `python3 scripts/scenario-map-report.py`, which rewrites
  `docs/refactor/SCENARIO-MAP.md`. Status stays `L1-now` to match the catalog.
  Regenerate the inventory **after** the map, because the inventory reads it.
- **`docs/quality/owners.json`.** No rows: no new files, globs, statics, spawns
  or macro-dsl macros (`assert*`/`format!` are `EXPRESSION_MACROS`).
- **`docs/refactor/architecture-policy.json`.** No change: no new files.
- **`docs/refactor/WIRE-MATRIX.md`.** No change: the codes and messages
  already listed stay (§2.2).
- **`docs/refactor/review-mechanisms.json`.** No change: no pinned test body
  is modified.
- **`src/dst/tests/README.md`.** No change: no new DST module.
- **`scripts/quality/mutation_owners.py`.** No change (§5).

---

## 7. Controls

Run from `/Users/sorenschmidt/code/streams` with the Python shim first on the
path: `PATH=/private/tmp/claude-501/-Users-sorenschmidt-code-streams/4580d18c-ad9e-4e38-8ca1-89558fd0d592/scratchpad/pybin:$PATH`.
The gate needs Python ≥ 3.11.

1. **Red (commit 1 tests only):**
   `cargo test --release --lib -- --exact dst::dst_tests::lifecycle_incarnation::a_raw_recreate_never_replaces_an_expired_source_its_forks_read dst::dst_tests::lifecycle_incarnation::a_raw_put_behind_a_stale_tombstone_never_revives_a_deleted_source`
   Expect `test result: FAILED. 0 passed; 2 failed` with the two panics in §3.1
   and §3.2, verbatim (lines 753:5 and 841:5).
2. **After commit 1 code:** the same command gives `2 passed`. Then run the
   neighbours:
   `cargo test --release --lib -- dst::dst_tests::lifecycle_incarnation:: dst::dst_tests::fork_lifecycle:: dst::dst_tests::fork_cleanup:: dst::dst_tests::lifecycle_creation:: application::creation::`
   Expect **33 passed** (8 + 10 + 2 + 8 + 5, where `application::creation::`
   covers the 2 creation.rs tests and 3 in ttl.rs).
3. **After commit 2:** step 2's command gives 33 passed, and
   `cargo test --release --lib -- dst::dst_tests::append_application::` gives
   5 passed.
4. **Commit 3:** first the red,
   `cargo test --release --lib -- --exact dst::dst_tests::product_lifecycle::a_product_create_that_loses_to_a_live_forked_incarnation_is_idempotent`,
   which should show the §3.3 panic (854:5). After the code, it passes, and
   `cargo test --release --lib -- dst::dst_tests::product_lifecycle:: dst::dst_tests::fork_lifecycle::`
   gives **19 passed** (9 + 10). The fork_lifecycle product-overwrite leg still
   answers 409.
5. `cargo fmt --all -- --check`, which prints nothing.
6. `cargo clippy --locked --workspace --all-targets -- -D warnings` is clean.
   This includes no `unfulfilled_lint_expectations`: `resolve` is 152 lines
   and `create_product` is 129, both still over 100.
7. `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items`
   is clean. The new docs use no `[..]` or `<..>`.
8. Ledgers:
   - `python3 scripts/scenario-map-report.py && python3 scripts/scenario-map-report.py --check`
     exits 0.
   - `python3 scripts/test-inventory.py --write && python3 scripts/test-inventory.py --check`
     prints `test-inventory: OK (500 tests, 0 ignored)`, or 499 without
     commit 3.
9. `scripts/quality.sh` exits 0 with **no** `accepted exception grew` line.
   The two ratcheted scopes shrink (§4). The architecture gate, the mt-lint
   leg and scenario/inventory checks pass.
10. `scripts/multitenancy-audit.sh` prints `MT_AUDIT_OK` with zero `+` rows.
11. Budgets: running `wc -l` on the nine ceilinged files gives exactly
    3371 / 4205 / 3232 / 2201 / 1713 / 1676 / 1509 / 1200 / 1143.
    `src/dst/tests/lifecycle_incarnation.rs` is 872 and
    `src/dst/tests/product_lifecycle.rs` is 871, both under 1,000.
12. **CI's own selection, before push** (commits must be committed):
    `cargo build --locked -p streams-quality-syntax && QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan && python3 -c "import json;p=json.load(open('target/quality-plan/plan.json'));print(p['mutants'],p['loom'],p['miri'],p['properties_fuzz'],p['mutation_source_files'],p['unregistered_mutation_source_files'])"`
    should print `False False False False [] []`.
13. Full suite as CI runs it:
    `cargo test --release -- --skip post_split_throughput_scales`, all ok.
14. After push:
    `gh run list --branch slate --json databaseId,headSha,createdAt,status,conclusion --limit 5`.
    Match the head SHA, then run `gh run view <id>`. Never claim green without
    this.

---

## 8. Out of scope and follow-ups

- **Raw GET/HEAD of an expired source with live forks answers 404.**
  `read_request.rs:134-139` does `if desc.soft_deleted { Gone } else { Missing }`,
  while append, delete and PUT answer 410/409 `gone`, and the catalog (FRK-019)
  says the source "behaves as soft-deleted". Fixing it is a one-line switch to
  `retained_for_forks`. It is a wire change (404 → 410) in a mutation-gated
  file (`read_request` owner), so it needs a separate decision.
- **Dropping the claim snapshot guard** (§5). Answering 409 from a snapshot up
  to 5 s old can refuse a name whose last fork has just been released. Letting
  the CAS decide alone is simpler and fresher. The cost is one more GET for
  PUTs against retained names, and the answer in that window changes from 409
  to 201.
- **`anchor::install` does not refuse an expired source** (anchor.rs:92-99).
  A fork validated before expiry anchors after it, which keeps the source
  retained. That is consistent with the retention semantics, and this plan
  makes the CAS respect it. Whether a lapsed source should accept new children
  at all is a separate question.
- **Liveness copies with an injected clock:** `AppendService::alive`
  (append.rs:39), `LifecycleService::alive` (lifecycle.rs:15),
  `WatchService::alive` (watch.rs:155), and catalog `active`
  (registry/catalog.rs:159). They duplicate `alive_at`; deduplicating them is
  a separate change.
- **Product `count_project_streams`** counts `desc_alive || soft_deleted`, not
  expired-with-forks (`src/application/creation/product.rs:170`). A
  retained-by-expiry name occupies no quota slot, yet it cannot be recreated.
- **The two surfaces' `gone` messages differ** ("name is soft-deleted; live
  forks retain its data" vs "name is retained for live forks"). Unifying them
  changes wire text.
- **Stale-live snapshots** (validate_live answering "exists" for a stream
  deleted elsewhere within the cache TTL) are general cache staleness, already
  in `followups.md` (create's post-PUT insert).

---

## Skeptic corrections (C1..C7)

Verified on the current tree, HEAD = origin/slate = `fba5af56`. The probe
`*.orig.rs` files are byte-identical to `src/application/creation.rs`,
`creation/claim.rs`, `creation/product.rs` and `append.rs` at HEAD.

**What checks out (no correction needed).**
- The diagnosis. `claim.rs:40-53` has a snapshot-only fork guard, and the
  closure at `claim.rs:75` has no fork check. `Registry::recreate` judges the
  stored descriptor and returns it on decline (`registry.rs:1036-1039`).
  `anchor.rs:92-99` has no expiry refusal. `anchor.rs:132` invalidates only
  the local cache. `read.rs:436-445` gives "different incarnation", which is
  pinned as 500 at `fork_lifecycle.rs:748-754`.
- Both reds trace as claimed. In §3.1 the poisoned snapshot is expired, has
  no children and is not soft-deleted, so the guard is skipped. The stored
  descriptor is expired and has `[keptchild]`, so the old closure is true and
  the CAS writes a fresh epoch. `needs_init` is false, so the PUT answers 201
  with `Body::empty()` (`http.rs:2632-2646`). In §3.2 the stored descriptor is
  `soft_deleted`, so the closure declines. `validate_live` then matches ct,
  ttl, fork and key, and `seed`/`publish` are no-ops for `created=false`, so
  the PUT answers 200. In §3.3 the stored descriptor is live with a child, so
  the closure declines, `winner.fork_children` is non-empty, and the response
  is 409. The body key order is right: `serde_json` in `Cargo.lock` has no
  `indexmap` dependency, so maps are sorted. The panic lines 753, 841 and
  854 follow from 711 and 819 plus one blank line.
- Every control passes after the fix. `cas_update`/`cas_update_retry`
  invalidate the cache (`registry.rs:1249,1314`), so the `keptfree` re-PUT
  sees the expired stored copy and answers 201. On decline, `recreate`
  invalidates, so GET `heldsrc` answers 410 through `authorize_read`'s
  `soft_deleted` arm. DELETE `keptsrc` answers 410 through
  `CreationError::gone` (`deletion.rs:15-21`), and POST answers 410 through
  `append.rs:62-67`.
- Everything the tests use exists: `test_poison_cache` (`registry/cache.rs:279`),
  `raw_adapter_sref`, `to_persisted`, `TryFrom<PersistedDescriptor>`, and
  `Deref<Target=PersistedDescriptor>` for `StreamDesc` (`registry.rs:405`),
  which gives the `&StreamDesc → &PersistedDescriptor` coercions at every new
  call. The unit test's struct literal lists all 24 fields, and
  `PersistedDescriptor` derives `Clone`.
- The ratchets. I counted scan.rs facts (path, call-site, method-call×2) by
  hand. `resolve`'s scope goes from 158 to 152 lines and syntax_facts from
  −34 to +29, net −5. `create_product` stays at 129 lines, with −11/+10 facts.
  Neither function has unwrap/expect lints, so there are no call-site
  fingerprints. Code lines stay over 100 (resolve 136→126, create_product
  122→~121), so neither `too_many_lines` expect becomes unfulfilled. The
  `#[expect]`s in `creation.rs` at 151/188/229/263 shift lines but not
  content, and the identity is (path, qualified, kind, value). The r05 hash
  pin is unaffected. No ceilinged file is touched, and `wc -l` matches the
  nine budgets exactly.
- The test counts. lifecycle_incarnation has 6 tests (8 after), fork_lifecycle
  10, fork_cleanup 2, lifecycle_creation 8, `application::creation::` 1+3 (5
  after), product_lifecycle 8 (9 after), append_application 5. The inventory
  holds 497 entries and covers `src/dst` only, so the unit test is not
  counted.
- Mutation and loom selection. No touched path is in `CRITICAL_PREFIXES` or
  `mutation_owners.py`, so `mutants`, `loom`, `miri` and `properties_fuzz` are
  all false. The `plan.json` keys that control 12 reads all exist
  (`verification_plan.py:95-99`).

**C1. The base and working-tree statements are stale (plan lines 3-7).**
HEAD is `fba5af56`, not `0a8ed40a`. The "someone else's uncommitted edits"
landed as `47799eb3`, `09fd911d` and `fba5af56`. Two of them change gates
this plan runs:
- `scripts/multitenancy-audit.sh` now scans **every** `src/**/*.rs`,
  including `src/dst/tests`, and fails on GONE fingerprints.
- `architecture-gate.py` now fails an obsolete budget exception.

I re-checked both against the plan. None of the new lines match a
`stream_hash(`, `registry.<m>("literal"`, tenant-fallback or
`streams-internal-` pattern. Every registry call in the tests takes `&src`
or `&sref` across lines, so there are no NEW or GONE rows. No
`budget_exceptions` key names a touched function (`resolve` is 152 lines,
under the 200 default). Controls 9 and 10 stand. Rebase the text: "Base:
slate at fba5af56; tree clean."

**C2. Missed ledger: `docs/refactor/scenario-dispositions.json` FRK-019
(lines 268-279).** The plan flips FRK-019 in `test-scenario-map.json` to
`mapped: true, coverage: partial`. The disposition row would still say
`"state": "pending"`, `"disposition": "GAP: no test…"`,
`"source_tests": []`.

`review-evidence.py:65` only requires that a partial scenario *has* a
disposition, so the gate passes. The ledger would then contradict the map.
Every other partial row (DUR-007, CRT-007, CRT-010) carries
`"state": "partial"`, a disposition sentence and
`closure_evidence.source_tests`.

Commit 1 must set:
- `state: "partial"`
- `source_tests: ["src/dst/tests/lifecycle_incarnation.rs::a_raw_recreate_never_replaces_an_expired_source_its_forks_read"]`
- a disposition naming what is pinned (re-create refused on the stored
  descriptor behind a stale snapshot; append and delete 410; the fork still
  reads)
- `remaining`: the source's own GET/HEAD answers 404 rather than 410
  (`read_request.rs:134-139`), and the final-release cascade from an
  expired-with-forks source.

**C3. The documented transition contract is missing.
`docs/creation-transitions.md:10-23`** is the owner table for creation
decisions ("Observed state | Command | Durable decision"). It has no row for
Create over a dead name or a retained name, which is exactly the decision
this change defines. Add these in commit 1:
- "Soft-deleted, or expired with children | Create | 409 `gone`; the recreate
  CAS re-judges the STORED descriptor, so a snapshot older than a fork
  anchored elsewhere cannot replace the source."
- "Tombstone, or expired without children | Create | predicated recreate CAS
  at one instant. A declined winner is either retained (409 `gone`) or live
  (idempotent compare)."

Commit 3 needs the matching product row if D2 is approved.

Optional: the `Registry::recreate` doc (`registry.rs:1002-1004`) still says
"a loser observes the winner's live descriptor". §1.2 shows a declined
winner can be a retained, non-live incarnation. That wording is where the
misclassification came from. `registry.rs` is at its 1,509-line ceiling, so
fix it only as a same-line-count rewording (for example "a loser gets the
current descriptor `still_dead` refused"). Doc attributes are not
call-site/path facts, so the `expect_used` fingerprints on that scope do not
change. Or leave the file untouched.

**C4. Line citations.**

| plan says | actual |
|---|---|
| `registry.rs:940` (5 s TTL) | `registry.rs:943` |
| `registry.rs:1032-1034` | `registry.rs:1036-1039` |

Everything else cited (claim 21/36/40-53/73-82, creation 141-145/214,
fork.rs:85, anchor 76-106/92/132, read.rs 436-445, deletion 164/470,
read_request 134-139, http.rs:2396, product.rs 113-127/170) is exact.

**C5. §8 misdescribes `AppendService::alive` (`append.rs:39-43`).** It calls
`now_ms()` directly and has no injected clock. Only `LifecycleService::alive`
(`lifecycle.rs:15`, `self.clock`) and `WatchService::alive` (`watch.rs:155`)
take a clock. So `AppendService::alive` is a plain copy of `desc_alive`, and
it could be deduplicated in commit 2 at no cost. The follow-up text should
say so.

**C6. §5 lists two mutants cargo-mutants 27.1 never generates.** The product
classification is an `else if retained_for_forks(&winner, now)`, not a match
guard. cargo-mutants mutates match guards (true/false), binary and unary
operators and function return values, but not `if` conditions. So "product
declined → true/false" are not mutants. The matching operator and function
mutants in `retained_for_forks` are killed by §3.4.

This does not change the outcome, because no file is gated. It matters only
for the claim that "if `creation` is ever registered, every mutant is
killed". The claim-arm guards (snapshot and declined) *are* match guards,
and the one equivalent mutant is correctly identified (snapshot guard →
`false`). There is one extra non-equivalence the plan omits: when a stale
snapshot is retained but the stored object is gone, the mutant answers 500
(`recreate` NotFound → `Storage`) instead of 409. It is not worth pinning.

**C7. Controls: no unbuildable control, one wording fix.** Control 12 must
run after commit (the plan says so). Its expected output is correct.
Controls 2-4 should go through `scripts/test-leg.sh` only if they become CI
legs. As local checks they are fine. Controls 8 and 9 now also run
`review-evidence.py`, which validates the C2 row.

**Documented contracts elsewhere.** `WIRE-MATRIX.md:32` (raw: 409
`gone (name retained for forks)`) and `:93` (product: 409
`gone (name retained for live forks)`) remain true after D1 and D2. No
listed code, status or message changes. `SCENARIO-CATALOG.md:452-454`
(FRK-019, "behaves as soft-deleted") is met for PUT, POST and DELETE but not
GET/HEAD, which answer 404. The plan records that correctly as partial, out
of scope and needing its own decision. No doc pins the raw 200 on a declined
soft-deleted winner.

**Verdict: ready-with-corrections.** The diagnosis, the three reds, the
controls, the ratchet arithmetic and the budgets are correct against the
source. Before commit 1: fix C2 (the disposition ledger) and C3 (the
transition table), and update the stale base and line/prose references (C1,
C4, C5, C6). D1 (raw: 201/200 → 409 `gone`) and D2 (product: 409 → idempotent
200 for a live forked winner) remain Søren's decisions as the plan states.
