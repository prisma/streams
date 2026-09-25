# Item 77, stage 1: the incarnation ABA tests go through the production fence

Tree: `slate` HEAD `5d9d517f` (origin/slate `77c13d74`, 15 unpushed commits).
`src/registry.rs`, `src/registry/*` and `src/dst/tests/lifecycle_incarnation.rs`
are byte-identical to origin/slate (`git diff --quiet origin/slate -- …` holds),
so the merge-base ceilings below are also the current sizes.
`docs/refactor/test-inventory.json` is NOT identical: the unpushed commits add
44 lines, so it must be regenerated on HEAD, not on origin/slate.

The reviewer's line numbers are stale. The current locations are
`registry.rs:1100-1126` (adapter), `:902-912` (`IncarnationCas`),
`:1169-1171` (production fence) and `:1177` (identity backstop).
`lifecycle_incarnation.rs:482-493` is still accurate.

## 1. Problem (verified)

### 1a. The two ABA tests prove the test adapter, not production — REAL

`src/registry.rs:1100-1126` (cfg(test)):

```rust
    /// Test compatibility adapter for old incarnation-outcome fixtures.
    #[cfg(test)]
    pub(crate) async fn cas_update_incarnation_outcome(
        ...
        let mut moved = false;
        let applied = self
            .cas_update_retry(sref, |d| {
                if d.stream_epoch != expected_epoch {
                    moved = true;
                    return false;
                }
```

This adapter goes through `cas_update_retry` → `cas_update` (`:1231-1312`, both
`#[cfg(test)]`). It never calls `mutate_incarnation`, so the epoch fence it
checks is its own closure at `:1111`. The fence that production runs is
`mutate_incarnation` at `:1169-1171`:

```rust
            if desc.stream_epoch != expected_epoch {
                return Ok(MutationResult::IncarnationChanged);
            }
```

It has a second, independent guard at `:1177-1182`:

```rust
            if next.sref() != *sref || next.stream_epoch != expected_epoch {
                return Err(MutationError::InvalidData(invalid_descriptor(
                    &next.name,
                    "mutation changed incarnation identity",
                )));
```

The production callers are all `mutate_incarnation`:
`application/creation/deletion.rs:45,158,295`, `anchor.rs:78,232`,
`initialization.rs:199`, `ttl.rs:57`, `application/lifecycle.rs:177,274,361,457,495,535,786`
and `application/topology.rs:247,335,450,570`.

**Complete use sites of the adapter** (`git grep`, whole repo incl. tools/, fuzz/, bench/, tests/):

| symbol | use sites |
|---|---|
| `cas_update_incarnation_outcome` | def `registry.rs:1102`; callers `src/dst/tests/lifecycle_incarnation.rs:484`, `:584` only |
| `IncarnationCas` | def `registry.rs:905`; `registry.rs:1107,1120,1122,1124`; `lifecycle_incarnation.rs:491`, `:601` only |
| docs (non-gated) | `docs/RELEASE-PRODUCT-SURFACE.md:646` (dated release note, already names the long-gone `cas_update_incarnation`); `docs/MULTITENANCY-MAP.md:74,132` (dated map that says it decays); `docs/quality/legacy-diagnostics{,-linux}.json:20760,20907` (immutable adoption baseline per `policy.json` `immutable_sha256`, must not be edited; active `diagnostic-allowances*.json` hold 0 warnings, so nothing to prune) |

Nothing in tools/, fuzz/, bench/, tests/, examples/, sdk/ or conformance/ uses them. The only by-path includer is
`src/dst/dst_tests.rs:100` (`#[path = "tests/lifecycle_incarnation.rs"]`).

The two tests are:

* `a_parked_delete_never_removes_a_later_incarnation` (`lifecycle_incarnation.rs:417-516`).
  The only ABA proof is the direct replay at `:482-493`. The parked HTTP delete is
  released and finishes before the replacement exists (`:455-462`), so the HTTP
  half never races the replacement.
* `a_fork_stamp_never_lands_on_a_later_incarnation` (`:518-618`). The replayed
  stamp at `:588-594` writes `source_epoch: String::new()`. That is itself a
  malformed descriptor: `validate_descriptor` (`registry.rs:512-521`) refuses it
  with `fork source_epoch "" does not decode to 16 bytes — corruption`, and
  `cas_update` runs that check at `StreamDesc::try_from(desc)?`. So even the
  adapter's own fence was not the only thing standing between the stamp and the
  replacement.

Consequence: delete `registry.rs:1169-1171` and both tests stay green, because
nothing on their path reaches it. Control 0 in §7 demonstrates this on the
current tree before any edit.

Also, the adapter reintroduces the exact shape that `mutate_incarnation`'s doc
(`:1128-1138`) says is unrepresentable: an `FnMut` bool over `&mut` with a
captured out-parameter (`moved`).

### 1b. The reviewer's stage-1 deletion list: two of four items do NOT become unused

The task asked to delete these only if they become unused.

* **`Registry::update`** (`registry.rs:1055-1098`, cfg(test)) still has three callers in
  `src/registry/tests.rs`:
  * `:659` `reg.update(&ts("s"), |d| d.deleted = true)`
  * `:744` (tombstone stamp)
  * `:789` `assert!(reg.update(&ts("s1"), |_| {}).await.is_err());`

  Deleting it in stage 1 does not compile. It is **not** deleted here.
* **`r08_only_precondition_conflicts_are_retried`** (`src/registry/tests.rs:22-34`) pins
  `retryable_cas_error` (`registry.rs:48-54`). That function still gates
  `cas_update_retry` (`:1246`), which keeps seven DST callers after stage 1:
  * `fork_lifecycle.rs:738,759`
  * `lifecycle_incarnation.rs:739,789`
  * `product_lifecycle.rs:838`
  * `seal_recovery.rs:727,805`

  `docs/review-application-evidence.md:23` names r08 as the regression that
  "changing human text to contain 'precondition conflict' does not make an
  unclassified error retryable". Deleting it now would leave a live classifier
  unpinned. It is **not** deleted here. It goes with `retryable_cas_error` in stage 2.
* The reviewer's "diverging semantics" note is true but belongs to stage 2:
  * `retryable_cas_error` also retries `AlreadyExists`; `mutate_incarnation` retries only `Precondition`.
  * `update` retries without backoff and writes tombstones.
  * `cas_update` refuses tombstones (`:1273-1275`).

**Stage 1 therefore deletes exactly:** `cas_update_incarnation_outcome` and `IncarnationCas`.

## 2. Contract decision

None. No production code path changes. `mutate_incarnation` and every production
caller are untouched. The only production-token change is that the orphaned doc
line `/// One page of the stream catalog.` (`registry.rs:901`) goes back onto
`CatalogPage`; today it is attached to the cfg(test) `IncarnationCas`. Nothing
changes at the product or raw edge.

## 3. Pinning tests and non-vacuity controls (test rewrite, no red-first test)

This is a test rewrite plus deletion of dead test-only APIs. There is no
behaviour change, so there is no red-first test.

**Pinning tests.** These stay green. The first three bullets live in `src/dst/tests/lifecycle_incarnation.rs`
under `dst::dst_tests::lifecycle_incarnation::`; the last in `src/registry/tests.rs`:

* `a_parked_delete_never_removes_a_later_incarnation` (rewritten)
* `a_fork_stamp_never_lands_on_a_later_incarnation` (rewritten)
* the other six tests of the module (untouched; they are the neighbours sharing `gap_lock`)
* `registry::tests::*` (15 tests, including `r08_only_precondition_conflicts_are_retried`,
  which is kept). It shows the adapter deletion broke nothing in the registry unit fixtures.

**Non-vacuity controls** (exact commands in §7):

* Control 0 (the problem, current tree): delete the production fence and the two
  old tests still pass.
* Control A (after): delete the production fence (`:1169-1171`, which becomes
  `:1129-1131` after the deletions). Both rewritten tests fail at their outcome
  assertion with `Err(InvalidData(... "mutation changed incarnation identity" ...))`.
  The identity backstop catches the stale write, but the typed verdict the fence
  owes its callers is gone.
* Control B (after): delete the fence **and** the backstop's epoch clause. Both
  fail with `Ok(Applied(()))`. The stale decision would really have landed on the
  replacement, which shows the replays are real writes and not no-ops that
  validation refuses anyway (the old stamp's weakness, §1a).

## 4. Edits, one commit

The edits are one commit because the rewrite alone would leave
`cas_update_incarnation_outcome`/`IncarnationCas` as unused `pub(crate)` items
under cfg(test). That raises `dead_code`, which `clippy -D warnings` rejects, so
no green intermediate commit exists.

Suggested subject, matching the repo's sentence style:

`The incarnation ABA tests replay their stale decisions through mutate_incarnation, the fence production runs`

The body names Controls 0/A/B and ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

### 4.1 `src/dst/tests/lifecycle_incarnation.rs` (872 lines; not ceilinged, <1,000)

**Test 1 `a_parked_delete_never_removes_a_later_incarnation`.** This function carries a
fn-scoped `#[expect(clippy::disallowed_methods, reason = "parked deletion fixture; …")]`
(`:420-423`). Its whole body is therefore a ratcheted scope: `scope_lines`,
`nested_items` and `syntax_facts` may not grow versus origin/slate. The rewrite on
its own adds 4 syntax facts:

* +`current` path
* +`to_persisted` method-call and method-call-site
* +`Mutation::Write` call-site, path and `next` arg
* +`next` path
* −`x` path
* −`.unwrap()` method-call and method-call-site

The paths inside `assert!(matches!(…))` are macro tokens, not facts.

Remedy: narrow the syntax, not the decision. Hoist the stream ref that the body
derives 7 times (`:434,437,465,468,484,496,499`) into one `let sref`. Each use
drops from 3 facts (`state` path + `raw_adapter_sref` call pair) to 1, a saving of
14. The `let` costs 3. Net for the function: **syntax_facts −7, scope_lines −15**
(each of the 3 invalidates goes 3→1 lines, `second`/`d` gets go 6→1, +1 `let`),
nested_items unchanged. The reason text is not touched; the exception decision is
about `tokio::spawn`, which is unchanged. The alternative remedy, re-deciding the
reason text, is rejected: the reason is still exactly true, and editing it only
to reset a counter is gaming the ratchet.

Edits:

* After `let (state, addr) = http_rig(store).await;` (`:428`) add
  `let sref = state.deployment.raw_adapter_sref("abadel");`
* `:432-434`, `:463-465`, `:494-496` → `state.registry.invalidate(&sref);`
* `:437` `.get(&state.deployment.raw_adapter_sref("abadel"))` → `.get(&sref)` (the chain stays vertical: 70 chars > chain_width 60)
* `:466-471` → `let second = state.registry.get(&sref).await.unwrap().unwrap();`
* `:497-502` → `let d = state.registry.get(&sref).await.unwrap().unwrap();`
* `:482-493` becomes the following. The comment at `:478-481` stays: "the same CAS the parked deleter would have run" is now literally true.

```rust
    let outcome = state
        .registry
        .mutate_incarnation(&sref, &first, |current| {
            let mut next = current.to_persisted();
            next.deleted = true;
            crate::registry::Mutation::Write(next, ())
        })
        .await;
    assert!(
        matches!(outcome, Ok(crate::registry::MutationResult::IncarnationChanged)),
        "a stale delete decision was not fenced: {outcome:?}"
    );
```

The decision mirrors production's `delete_transition`
(`application/creation/deletion.rs:465-480`): a tombstone derived from the current
descriptor. `delete_transition` is private and stays private; there is no
visibility change to production. The outcome is matched unwrapped, so a regression
surfaces as a named assertion that carries the `Err`/`Applied` value, not as an
anonymous `unwrap` panic.

**Test 2 `a_fork_stamp_never_lands_on_a_later_incarnation`.** It has no `#[expect]`,
so there is no ratchet and the stream-ref calls are left alone (smallest diff).
`:582-603` becomes:

```rust
    let outcome = state
        .registry
        .mutate_incarnation(
            &state.deployment.raw_adapter_sref("abachild"),
            &stale_epoch,
            |current| {
                let mut next = current.to_persisted();
                next.forked_from = forked.forked_from.clone();
                crate::registry::Mutation::Write(next, ())
            },
        )
        .await;
    assert!(
        matches!(outcome, Ok(crate::registry::MutationResult::IncarnationChanged)),
        "a stale fork stamp was not fenced: {outcome:?}"
    );
```

The replay installs the deleted child's own, stamped, **valid** parentage
(`forked`, `:547-554`, still in scope). The old `source_epoch: String::new()` stamp
failed `validate_descriptor` by itself, so it could not show that the fence
matters (Control B). The closure is `Fn`, so the `.clone()` is required (not
`redundant_clone`). The one-line call form would be 101 columns, so rustfmt makes
it vertical. Run `cargo fmt` rather than hand-wrapping.

The file shrinks by about 21 lines, to roughly 851. `configuration` strings in the
inventory (failpoint lines `:445-458`) are untouched. No `tokio::spawn` count
changes, so the owners.json and source-allowances.json effect rows are unchanged.

### 4.2 `src/registry.rs` (ceiling 1,492 → 1,452, −40)

* Delete `:902-913`: the `/// Why a generation-fenced mutation did not apply.` line,
  `#[derive(Debug, Clone, Copy, PartialEq)]`, `#[cfg(test)]`, `pub(crate) enum IncarnationCas { … }`
  and the following blank line. That is 12 lines. Keep `:901` `/// One page of the stream catalog.`
  so it lands directly on `pub(crate) struct CatalogPage`.
* Delete `:1100-1127`: the doc line, `#[cfg(test)]`, `cas_update_incarnation_outcome`
  and the trailing blank line. That is 28 lines.
* Ratcheted scopes touched: **none**. There is no impl-wide or module-wide
  `#[expect]` in the file. The item-scoped expects at `:57,301,628,648,676,733,945,996,1140,1144,1232,1390-1398`
  keep identical bodies; exception identity is `(path, qualified, kind, reason)`
  with no line numbers, so the upward shift is neutral. No `#[expect(dead_code)]`
  loses its last item. `cas_update_retry`, `cas_update`, `retryable_cas_error`,
  `update`, the `Mutex` import and `take_fail_next_put` all keep callers.
* The multitenancy audit is unaffected. None of the deleted or edited lines match
  its fingerprints, and `scripts/mt-audit-baseline.txt` has only three
  `stream-hash` rows in registry.rs (`:46-48`), all untouched.

### 4.3 `docs/refactor/test-inventory.json`

Regenerate on HEAD with `python3 scripts/test-inventory.py --write`. The expected
diff is exactly the two `function_sha256` values of the rewritten tests (2+/2−).
Attributes, mechanisms, configuration and scenarios do not change.

## 5. Mutation analysis

* `src/registry.rs` is under no `CRITICAL_PREFIXES` entry (`verification_plan.py:21-31`)
  and has no row in `mutation_owners.py`. `registry_cache` owns only `src/registry/cache.rs`,
  which is untouched.
* `src/dst/tests/lifecycle_incarnation.rs` is under `src/dst`, which is neither critical nor registered.
* So this commit contributes **no mutants and no unregistered critical source**. In
  isolation the plan selects `"mutants": false` with an empty `mutation_source_files`.
  The push range as a whole still carries the 15 earlier commits' own selection.
* Informational: registry.rs is outside the mutation scope, so this is not a CI
  obligation. The rewritten tests are the first that would kill a `!=`→`==` or
  "drop the early return" mutant at the production fence `registry.rs:1169` (Control A).
  The old tests could not. Mutants on the backstop's epoch clause (`:1177`) stay
  unreachable from any decision derived from the current descriptor while the fence
  stands. That is pre-existing and stage-2 territory if registry.rs is ever registered.

## 6. Ledgers (same commit)

| ledger | change |
|---|---|
| `docs/refactor/test-inventory.json` | two `function_sha256` values (regenerate on HEAD) |
| `docs/refactor/review-mechanisms.json` | none: neither test, nor registry.rs, nor lifecycle_incarnation.rs is pinned (checked all 34 pinned paths) |
| `docs/refactor/review-unit-relocations.json` | none (no registry/incarnation entries) |
| `docs/refactor/test-scenario-map.json` / `scenario-dispositions.json` / `test-relocations.json` | none: no test renamed or deleted; both tests have `"scenarios": []` |
| `docs/quality/owners.json`, `source-allowances.json` | none: the effect rows (`tokio::spawn` ×1 for test 1) are unchanged, and no registry.rs rows name the deleted items |
| `docs/quality/legacy-*.json`, `syntax-fragments.json` | none, and must not be edited (immutable adoption inventories) |
| `docs/refactor/architecture-policy.json` | none |
| `src/dst/tests/README.md` | none (the `lifecycle_*` row is unchanged) |
| `scripts/mt-audit-baseline.txt` | none |

## 7. Controls

Wait until the running mutation leg has finished. These builds compete for CPU.
`T` below is:

```
dst::dst_tests::lifecycle_incarnation::a_parked_delete_never_removes_a_later_incarnation dst::dst_tests::lifecycle_incarnation::a_fork_stamp_never_lands_on_a_later_incarnation
```

**Control 0: the problem, on the current tree, before any edit.**

```
perl -0pi -e 's/\n            if desc\.stream_epoch != expected_epoch \{\n                return Ok\(MutationResult::IncarnationChanged\);\n            \}//' src/registry.rs
git diff --stat src/registry.rs        # 1 file changed, 3 deletions(-)
cargo test --locked --lib -- --exact $T
git checkout -- src/registry.rs && git status --short   # clean
```

Expected: `running 2 tests` and `test result: ok. 2 passed; 0 failed`. The fence
is gone and the old tests do not notice.

**After making §4 and committing:**

```
cargo fmt --all -- --check                                   # silent
cargo test --locked --lib -- --exact $T                      # ok. 2 passed
cargo test --locked --lib dst::dst_tests::lifecycle_incarnation::   # ok. 8 passed; 0 failed
cargo test --locked --lib registry::                         # every result line ok, 0 failed (includes registry::tests::r08_only_precondition_conflicts_are_retried ... ok)
wc -l src/registry.rs                                        # 1452
git grep -n -e cas_update_incarnation_outcome -e IncarnationCas -- src   # no output
python3 scripts/test-inventory.py --check                    # passes
git show --stat HEAD -- docs/refactor/test-inventory.json    # 2 insertions(+), 2 deletions(-)
```

**Control A: rewritten tests are not vacuous (production fence removed).**

```
perl -0pi -e 's/\n            if desc\.stream_epoch != expected_epoch \{\n                return Ok\(MutationResult::IncarnationChanged\);\n            \}//' src/registry.rs
git diff --stat src/registry.rs        # 3 deletions (the block now at :1129-1131)
cargo test --locked --lib -- --exact $T
git checkout -- src/registry.rs
```

Expected: `test result: FAILED. 0 passed; 2 failed`, with these panics:

* `a stale delete decision was not fenced: Err(InvalidData(Generic { store: "registry", source: "descriptor 'abadel' corruption: mutation changed incarnation identity" }))`
* `a stale fork stamp was not fenced: Err(InvalidData(Generic { store: "registry", source: "descriptor 'abachild' corruption: mutation changed incarnation identity" }))`

**Control B: the replays are real writes (fence and backstop epoch clause removed).**

```
perl -0pi -e 's/\n            if desc\.stream_epoch != expected_epoch \{\n                return Ok\(MutationResult::IncarnationChanged\);\n            \}//; s/if next\.sref\(\) != \*sref \|\| next\.stream_epoch != expected_epoch \{/if next.sref() != *sref {/' src/registry.rs
cargo test --locked --lib -- --exact $T    # an `unused variable: expected_epoch` warning is expected
git checkout -- src/registry.rs && git status --short   # clean
```

Expected: `0 passed; 2 failed`, with these panics:

* `a stale delete decision was not fenced: Ok(Applied(()))`
* `a stale fork stamp was not fenced: Ok(Applied(()))`

**Quality gate.** Must not report `accepted exception grew`.

```
mkdir -p target/quality
cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl
python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl
```

Expected: `quality ratchets: OK; …`, and no `file growth` or
`accepted exception grew without a new decision` line.

Optional negative control for the hoist decision: revert only the `sref` hoist in
test 1 and rerun the gate. The predicted failure is
`accepted exception grew without a new decision: ('src/dst/tests/lifecycle_incarnation.rs', 'crate::a_parked_delete_never_removes_a_later_incarnation', 'fn', 'expect(clippy::disallowed_methods, …)'): syntax_facts N -> N+4`.

Then run the rest of the gate set:

```
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items   # CatalogPage doc reattached, no warning
for g in scenario-map-report test-inventory review-evidence architecture-gate; do python3 scripts/$g.py --check; done
bash scripts/multitenancy-audit.sh      # MT_AUDIT_OK
```

**Mutation-plan isolation.** Run with `<pre>` set to the HEAD before this commit:

```
QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=<pre> python3 scripts/quality/verification_plan.py --out target/quality-plan
```

Expected: `"mutants": false`, `"mutation_source_files": []`, `"unregistered_mutation_source_files": []`.
Then run CI's own plan for the full push, `QUALITY_BEFORE_SHA=77c13d74`, before
pushing. Its selection comes only from the earlier 15 commits.

## 8. Out of scope

* **Stage 2.**
  * Port the 37 `cas_update`/`cas_update_retry` arrange sites to one fixture helper.
  * Delete `cas_update`, `cas_update_retry` and `retryable_cas_error` along with
    `r08_only_precondition_conflicts_are_retried`.
  * Settle `Registry::update` and its 3 `src/registry/tests.rs` callers.
  * Resolve the diverging retry semantics (`AlreadyExists`, backoff, tombstone visibility).
* **The HTTP half of `a_parked_delete_never_removes_a_later_incarnation`.** The parked
  delete is released and finishes before the replacement exists (`:455-462`), so the
  parked request never races a replacement. The fn doc ("the parked delete resumes. It
  must not delete the replacement") overclaims. Making it a true race is a separate test change.
* **`a_fork_stamp_never_lands_on_a_later_incarnation`'s doc says "an ABA of the SOURCE";**
  the test ABAs the child. This is a one-word doc fix. It is left out to keep this
  diff to the fence.
* **Dated docs naming removed symbols:**
  * `docs/RELEASE-PRODUCT-SURFACE.md:646`, a release record that already names the deleted `cas_update_incarnation`.
  * `docs/MULTITENANCY-MAP.md:74,132`, a generated 2026-08-15 map that states it decays.
* **Registering `src/registry.rs` as a mutation owner.**

## 9. Decisions for Søren

None. There is no product or raw edge change and no policy change. The reviewer's
stage-1 list included `update` and `r08_only_precondition_conflicts_are_retried`.
Neither becomes unused (§1b), so both move to stage 2. This is a correction to the
review, not a decision.

## Skeptic corrections (C1..C5)

Verified on HEAD `5d9d517f` (read-only). The following claims are **correct as written**:

* Every quoted line and location: adapter `registry.rs:1100-1126`, `IncarnationCas` `:902-912`, fence `:1169-1171`, backstop `:1177`, `retryable_cas_error` `:48-54`/`:1246`, tombstone refusal `:1273-1275`, `validate_descriptor` fork-epoch check `:512-521`.
* `wc -l`: registry.rs 1,492 and lifecycle_incarnation.rs 872. Both are byte-identical to origin/slate.
* Use sites: `git grep` over the whole repo finds only the plan's rows.
* `update` keeps 3 callers (`registry/tests.rs:659,744,789`). `cas_update_retry` keeps 7 DST callers, so r08 stays pinned.
* No impl-wide or module-wide `#[expect]` covers the deleted lines. The closest are item expects at `:945/:996/:1140/:1144/:1232`, and their bodies are unchanged.
* The test-1 ratchet arithmetic, recounted against `tools/quality-syntax/src/scan.rs`:
  * The rewrite alone adds syntax_facts +4 (old replay 10 facts, new 14 before the hoist).
  * The hoist removes 11 net (7×3 → 3 + 7×1).
  * Net: syntax_facts −7, scope_lines −15 (rustfmt chain_width 60 keeps `first` vertical at 70 chars; `second`/`d` collapse at 49), nested_items unchanged.
  * The exception is `disallowed_methods`, so there is no per-site fingerprinting.
* Deletions: 12 + 28 = −40, giving 1,452.
* No hard-coded lint deny blocks Control B. Only `unused_must_use` and `unfulfilled_lint_expectations` are denied, so `unused variable: expected_epoch` is only a warning.
* No gate cares about line or position changes:
  * `validate_descriptor` refuses neither `deleted = true` nor a valid copied `forked_from`, so Control B's `Ok(Applied(()))` holds.
  * The scenario map's `line` fields are already stale (`:159`, `:603`) and `scripts/scenario-map-report.py` never reads them.
  * review-mechanisms pins none of the 34 pinned paths touched here. The mt-audit baseline has no lifecycle_incarnation rows.
  * The diagnostic allowances are empty.
  * `source-allowances` has no rows that go stale.
  * No mutation owner filter names `dst_tests::lifecycle_incarnation`.
  * registry.rs is neither critical nor registered, so `mutants: false`.

**C1 (§5 "Informational" is wrong).** The claim is that the rewritten tests "are the first that would kill a `!=`→`==` or 'drop the early return' mutant at the production fence". They are not the first.
* `!=`→`==` at `registry.rs:1169` turns every same-epoch call into `Ok(IncarnationChanged)`. `registry/tests.rs:381-385` expects `Err(MissingConditionalToken)` on a matching epoch, so that test fails at once. So do the production paths of every DST lifecycle test.
* Deleting the early return is already killed by `a_release_parked_across_recreation_cannot_touch_the_replacement` (`lifecycle_incarnation.rs:25`). Its stale release carries epoch A into `mutate_incarnation` (`application/creation/deletion.rs:156-158`) and decides `removed = true` on B, which holds the same fork id. The backstop then returns `InvalidData`, and `.map_err(|e| e.to_string())?` (`:185`) surfaces it as `Err`. The test panics at `.expect("release must not error")` (`:123`). This is PLAUSIBLE, reasoned from source, not executed.

Restate §5 (and §1a's "Consequence" framing): the two ABA tests were the only ones in this file that never reached the production fence. After the rewrite they pin its **typed verdict** (`IncarnationChanged`) for a delete decision and a parentage write directly. They are not the fence's first killers. This changes no edit and no control.

**C2 (§8 dated docs).** `docs/RELEASE-PRODUCT-SURFACE.md:646` names both `cas_update_incarnation` **and** `IncarnationCas`. After this commit both symbols are gone, not just "the long-gone `cas_update_incarnation`". Leaving the doc alone is still correct: it is a dated release record, and no script reads it (`git grep` in scripts/.github). The plan should record this accurately.

**C3 (§7 Control A/B pass criteria).** The exact panic strings are predictions of `object_store::Error`'s Debug rendering (`Generic { store, source }` with a boxed `String`). They should not be the pass criterion. The criterion is:
* `0 passed; 2 failed`;
* each panic starts with `a stale delete decision was not fenced:` / `a stale fork stamp was not fenced:`;
* Control A: the panic contains `InvalidData` and `mutation changed incarnation identity`;
* Control B: the panic contains `Ok(Applied(()))`.

Also, all three controls run `git checkout -- src/registry.rs` in the shared tree. Run them only after the running mutation leg has exited, as §7 says, and confirm `git status --short` is clean before and after each one.

**C4 (§4.1 test 2, in-test comment).** Production's stamp is `stamp_fork_reference` (`application/creation/anchor.rs:232-234`). It would `Decline` on the parentless replacement, so replaying the real stamp would make Control B vacuous. The plan's choice to write the deleted fork's own valid parentage is therefore right. But the comment kept at `lifecycle_incarnation.rs:580-581` ("The in-flight stamp from the deleted fork, replayed") then describes a fork-id stamp that the new body no longer performs. Suggestion: reword it to say the deleted fork's parentage is replayed under the epoch it was issued against. `function_hash` masks comments (see the `test-inventory.py` self-test), so the inventory diff stays at the two shas already expected. Test 2 has no `#[expect]`, so there is no ratchet effect.

**C5 (§3 wording).** "the other six tests … are the neighbours sharing `gap_lock`" is inaccurate. Only four of them take `gap_lock` (`:26,:172,:293,:633`). `a_raw_recreate_never_replaces_an_expired_source_its_forks_read` (`:720`), `a_raw_put_behind_a_stale_tombstone_never_revives_a_deleted_source` (`:807`) and test 2 itself do not. This is cosmetic; the pinning set is unchanged.

**Unbuildable controls:** none. The Control 0/A/B perl patterns match exactly one site each: `desc.stream_epoch != expected_epoch` occurs only at `:1169`, and the backstop pattern only at `:1177`. Controls 0 and A leave `expected_epoch` used. Control B only warns.

**Missed ledgers:** none. The only ledger that changes is test-inventory (2 shas, regenerated on HEAD). The following were checked and need no change: review-mechanisms, review-unit-relocations, test-scenario-map/dispositions/relocations, owners.json/source-allowances (test 1's `tokio::spawn` effect row is unchanged), architecture-policy/baselines (the historical a7e2070f snapshot is not a per-commit pin), mt-audit baseline, and src/dst/tests/README.md.

**Verdict: ready-with-corrections.** C1 and C2 fix false or incomplete statements in the plan text. C3 tightens the control pass criteria. C4 is a comment accuracy fix inside the rewritten block. C5 is cosmetic. None of them changes the edit set, the ceilings, the ratchet outcome or the ledger set.
