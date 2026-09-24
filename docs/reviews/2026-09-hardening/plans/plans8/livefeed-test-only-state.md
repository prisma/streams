# Item 60: LiveFeed carries production state that only tests read

Plan for `slate` at `fb18840d` (origin/slate = `7c4f8606`). This is a read-only design: I edited nothing and ran nothing.

Reviewer entry (review lines 1329-1339). Where: `feed.rs:521-522`, `feed.rs:1241-1252`, `feed.rs:625-631`. Change: delete `retained_charge` so that `retained()` reads `st.charge`; drop `clear_ring`'s gauge parameter and the `AtomicUsize` import; `cfg(test)`-gate `source_reads` the way `retry_spawns` is gated; remove `_key`. First step: point `retained()` at `st.charge` while the gauge still exists, then delete.

**Verdict.** The three problems are real. Two parts of the reviewer's Change, done exactly as written, fail the build or the quality gate:

- **Deleting the store in `take_visible`.** It leaves `if popped { if st.batches.is_empty() {..} }`. Clippy's `collapsible_if` then fails `-D warnings`.
- **Pointing `retained()` at `st.charge` inside `impl LiveFeed`.** It adds an `.unwrap()` under the impl-wide `unwrap_used` expectation. The exception ratchet then fails.
- **Changing the `clear_ring(..)` arguments.** It re-fingerprints two `ordinary-call` ratchets.
- **Gating `source_reads` with `cfg(test)`.** It grows two impl-wide `unwrap_used` contracts, so it cannot land without one of Søren's decisions (D1).

Below is the buildable version: commits C1-C3 need no decision. C4 is conditional on D1.

`src/sse/feed.rs` is 1,195 lines. The cited `1241-1252` no longer exists; `clear_ring` is now at 1078-1091.

---

## 1. Problem (verified on the current tree)

### 1a. `retained_charge` is a shadow of `FeedState::charge`, and only a `cfg(test)` accessor reads it

Declaration, `src/sse/feed.rs:521`:
```rust
    retained_charge: AtomicUsize,
```
Every use site (`grep -rn retained_charge src tests benches`):

| Site | Code | Kind |
|---|---|---|
| feed.rs:28 | `use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};` | `AtomicUsize` exists only for this field and the gauge parameter (other uses: :521, :653, :1084) |
| feed.rs:653 | `retained_charge: AtomicUsize::new(0),` | init |
| feed.rs:764-767 | `#[cfg(test)] pub(crate) fn retained(&self) -> usize { self.retained_charge.load(Ordering::Relaxed) }` | **the only reader, and it exists only in test builds** |
| feed.rs:848 | `self.retained_charge.store(st.charge, Ordering::Relaxed);` (take_visible, after the solo drain-release) | store 1 |
| feed.rs:1004-1009, 1050-1055 | `clear_ring(&self.budget, &self.project_reserved, &mut st, &self.retained_charge,);` (read_and_publish: oversize and uncached paths) | thread-through |
| feed.rs:1073 | `self.retained_charge.store(st.charge, Ordering::Relaxed);` (read_and_publish, after a retained publish) | store 2 |
| feed.rs:1080-1091 | `fn clear_ring(budget, proj, st: &mut FeedState, gauge: &AtomicUsize) { ...; st.charge = 0; gauge.store(0, Ordering::Relaxed); }` | store 3 |

The gauge equals `st.charge` whenever the lock is released. The `st.charge` mutations are feed.rs:838 (`-=`, take_visible), :1064 (`-=`) and :1067 (`+=`, read_and_publish), and :1089 (`= 0`, clear_ring). Each is followed by a gauge store under the same `st` lock before release. Both start at 0 (:646 and :653). Rustc raises no `dead_code` warning because `.store(..)` on the field counts as a use.

`retained()` callers, all in tests:
- `src/sse/feed/tests.rs:247, 288, 309, 338, 358, 401, 804, 836, 842, 883`
- `src/sse/feed/tests/retry.rs:89`
- `src/dst/tests/livefeed_swap.rs:444`: diagnostic text inside a `panic!`, in `livefeed_split_shared_subscribers_swap_once_deliver_twice`

### 1b. `source_reads` is incremented unconditionally for a test-only accessor

- feed.rs:522 `source_reads: AtomicU64,`
- feed.rs:654 `source_reads: AtomicU64::new(0),`
- drive.rs:120, unconditional on every permit-held read: `self.source_reads.fetch_add(1, Ordering::Relaxed);`
- feed.rs:750-753, the only reader: `#[cfg(test)] pub(crate) fn source_read_count(&self) -> u64`

Its sibling test counter is already gated at feed.rs:543-544, 666-667 and drive.rs:61-62 (`#[cfg(test)] retry_spawns`).

`source_read_count()` callers:
- `feed/tests.rs:507, 521` (`retained_batches_drain_without_extra_reads`)
- `feed/tests/retry.rs:62, 124`
- `dst/tests/livefeed_basics.rs:138/149` (`livefeed_two_subscribers_share_one_feed_and_one_source_read`)
- `dst/tests/livefeed_basics.rs:457/459` (`livefeed_singleton_large_window_delivers_everything_exactly_once`)
- `dst/tests/livefeed_basics.rs:903/905` (`livefeed_fork_foreign_only_window_is_progress`)

These are real contracts: "one append must cost exactly one source read" and "the retry task issues no source read". The counter must stay available to tests.

### 1c. `new_with_budget` takes a dead `_key`

feed.rs:625-631:
```rust
    pub(crate) fn new_with_budget(
        _key: FeedKey,
        src: Arc<dyn FeedSourceRead>,
        ring_budget: usize,
        budget: Arc<FeedMemoryBudget>,
        project: crate::tenant::ProjectId,
    ) -> Arc<Self> {
```
The body never names it; the feed's identity lives only as the registry map key. Nine callers:

| Caller | Build |
|---|---|
| `src/sse/service.rs:68` `LiveFeed::new_with_budget(k2, s2, ring, budget, project)` | production. `let k2 = key.clone();` at :63 exists only for this argument. |
| `src/sse/registry.rs:227` (`make_feed(key, ..)`), `:340`, `:349` | `#[cfg(test)] mod tests` |
| `src/sse/feed/tests.rs:202` (`feed_with`), `:637`, `:648`, `:767`, `:776` | `#![cfg(test)]` file |

Removing it also leaves `make_feed`'s `key` parameter (registry.rs:225) unused. Its ten call sites (:242, 264, 267, 270, 295, 298, 319, 321, 370, 375) lose that argument too; otherwise `unused_variables` fails `-D warnings`.

### 1d. What the reviewer's Change breaks, verified against the gate code

1. **collapsible_if.** Deleting only feed.rs:848 leaves the `if popped {` block (842-849) holding nothing but `if st.batches.is_empty() {..}` with no else. The outer block does not start with a comment. `clippy::collapsible_if` (style, inside `all`) then fails `-D warnings`. Writing `if popped && st.batches.is_empty()` instead adds a `&&`→`||` mutant that no existing test kills.
2. **The `retained()` rewrite.** `retained()` sits inside `#[expect(clippy::unwrap_used, reason = "LiveFeed; a poisoned feed state ...")] impl LiveFeed` (feed.rs:604-608). `scripts/quality/source_rules.py::exception_contracts` fingerprints every `unwrap` method-call site under that scope, including `#[cfg(test)]` fns, because facts are not filtered by `test_only`. `self.st.lock().unwrap().charge` there raises `unwrap_sites` N→N+1 and creates a new key `unwrap_site:crate::LiveFeed::retained:<digest>` (0→1). Gate output: `accepted exception grew without a new decision`.
3. **Changing the `clear_ring(..)` arguments.** Each call is a `call-site` fact whose value is the full token stream, fingerprinted under both of these:
   - `unwrap_site:ordinary-call:crate::LiveFeed::read_and_publish:<digest>` (impl-wide expect)
   - `expect_site:ordinary-call:...` (read_and_publish's own `#[expect(.., clippy::expect_used, ..)]`, feed.rs:904-912)

   New call tokens mean a new digest (0→2) in both contracts, so the gate fails. A method call (`st.clear_ring(..)`) creates no `call-site` fact. Its paths (`st`, `self`) are existing keys whose counts only fall.
4. **Gating `source_reads` with `cfg(test)`.** Each added `#[cfg(test)]` creates an `attribute` fact and a `path` fact (`cfg`):
   - **feed.rs.** `new_with_budget` already carries two such paths (:666, :668), so `unwrap_site:path:crate::LiveFeed::new_with_budget:<digest(cfg)>` goes 2→3.
   - **drive.rs.** The impl-wide expect (drive.rs:37-40) grows `scope_lines` +1 and `syntax_facts` +2, and `unwrap_site:path:crate::LiveFeed::drive_under_permit:<digest(cfg)>` goes 0→1.

   Every no-growth spelling I checked either adds a new path or call key, or is a production no-op wrapper, which RUST-QUALITY forbids. Hence D1.

---

## 2. Contract decision

Internal typed contract after C1-C3:
- `LiveFeed::new_with_budget(src, ring_budget, budget, project) -> Arc<LiveFeed>`. A feed's identity is its registry key and nothing else.
- `FeedState` is the only holder of the retained charge. `FeedState::clear_ring(&mut self, &FeedMemoryBudget, &ProjectRetention)` is the only reset for the uncached posture. The free fn `clear_ring` becomes this method: same body, the gauge parameter gone, and `st` is now `self`.
- The test accessor `LiveFeed::retained()` moves to `src/sse/feed/test_support.rs` (`#![cfg(test)]`). It reads `FeedState::charge` under the state lock, which is the value `Drop` releases.
- `source_reads` is unchanged unless D1 = (b).

**No wire change.** `retained_charge` and `source_reads` were never exported: they appear in no `sse_stats` counter, no `LiveFeedSnapshot` or debug field, no log, and nothing under `docs/`, `scripts/` or `.github/` (grep is empty). SSE frames, controls, status codes, headers and metric names are untouched. `docs/refactor/WIRE-MATRIX.md` is untouched. Nothing is visible at the product or raw edge.

---

## 3. Red tests / pinning tests

This is a pure refactor with no behaviour change, so there is no red test. Behaviour is pinned by existing tests and by compile-level proofs.

### C1: `retained()` reads `st.charge` (the gauge still exists)
It stays green only if the gauge and the ring charge agree wherever tests look.

**Pinning tests** (unchanged; all under the owner filter `sse::`):
- `src/sse/feed/tests.rs`:
  - `shared_retention_reserves_actual_bytes_only`: `budget.reserved() == retained as u64`, "the reservation IS the retained charge, exactly"
  - `budget_exhaustion_publishes_without_retention`: `retained() == 0`
  - `oversized_batch_is_never_self_evicted`: `retained() == 0`
  - `eviction_releases_exactly_the_evicted_charge`: `retained() == budget.reserved()`
  - `survivor_drains_retained_batches_after_drop_to_one`: `retained() > 0`, then `== 0` "the passed batch was released", and `floor() == 5`
  - `external_exhaustion_clears_unreachable_ring`: `feed_a.retained() == 0`, "A's stale ring was cleared"
  - `oversized_batch_clears_the_old_ring`: `retained() == 340`, then `== 0`, "the old ring was cleared too"
  - `concurrent_retention_never_exceeds_cap`: `budget.reserved() == Σ retained()`, "no phantom, no underflow"
- `src/sse/feed/tests/retry.rs::retry_never_skips_the_survivor_past_a_retained_ring`: `retained() > 0`
- `src/dst/tests/livefeed_swap.rs::livefeed_split_shared_subscribers_swap_once_deliver_twice`: diagnostic only

After C1 these tests compare the budget against the state that `Drop` actually releases, which is strictly stronger than before.

**Compile-level proof.** Two `retained` definitions would fail with E0592. `cargo check --tests` passing proves the old accessor is gone.

### C2: the gauge is deleted
**Census step** (local, not committed): delete only the field line (`retained_charge: AtomicUsize,`), then run `cargo check --locked --workspace --all-targets`. Expect exactly five distinct sites, each reported by the lib and lib-test units:
- `error[E0560]: struct \`LiveFeed\` has no field named \`retained_charge\``, in `new_with_budget`
- `error[E0609]: no field \`retained_charge\` on type \`&LiveFeed\``, four times: take_visible's store, the two `clear_ring` arguments in read_and_publish, and read_and_publish's final store

No other file appears. After the full edit, the proof is that no `retained_charge` or `AtomicUsize` token remains and the crate compiles under `-D warnings` (an unused import would fail).

**Folding `popped` into the drain loop.** This is behaviour-identical: `head` and `cursor` are loop-invariant, and the pop that empties the ring is the last loop iteration. It is pinned by `survivor_drains_retained_batches_after_drop_to_one`:
- `floor() == 0` while a retained batch remains ("draining floor stays put — the survivor is not lagged")
- `floor() == 5`, `retained() == 0` and `budget.reserved() == 0` after the pop that empties the ring

It is also pinned by `retry_never_skips_the_survivor_past_a_retained_ring` and `retained_batches_drain_without_extra_reads`.

**`FeedState::clear_ring`** is pinned by `oversized_batch_clears_the_old_ring` and `external_exhaustion_clears_unreachable_ring` (which also kill its body mutant, see §5).

### C3: `_key` is removed
**Census step** (local, not committed): delete only the `_key: FeedKey,` line, then run `cargo check --locked --workspace --all-targets`. Expect exactly nine `error[E0061]: this function takes 4 arguments but 5 arguments were supplied`: service.rs:68; registry.rs:227, 340, 349; feed/tests.rs:202, 637, 648, 767, 776. Then change `make_feed` to take only `budget`. Leaving its old `key` parameter produces `unused variable: \`key\``, and each of its ten old call sites fails with E0061 until updated.

Behaviour is pinned by all `sse::registry::tests::*`:
- `last_subscriber_evicts_and_reconnect_does_not_grow`
- `shared_subscribers_cost_nothing_until_retention`
- `shared_refusal_leaves_no_partial_state`
- `zero_budget_is_singleton_only`
- `zero_ring_is_singleton_only`
- `many_shared_feeds_share_one_budget`

The DST livefeed suite exercises `LiveFeedService::subscribe` end to end.

### C4 (only if D1 = b)
**Compile proof.** Gating the field alone makes `cargo check --lib` fail with E0560 at the `new_with_budget` init and E0609 at drive.rs:120. Gating all three restores the build. With the field left ungated and only the increment gated, the lib build fails on `field \`source_reads\` is never read`.

Pins (unchanged):
- `retained_batches_drain_without_extra_reads`
- `retry.rs`: `retry_install_leaves_the_successor_read_to_the_singleton`, `retry_tick_on_a_readable_tail_wakes_the_session_and_reads_nothing`
- DST: `livefeed_two_subscribers_share_one_feed_and_one_source_read`, `livefeed_singleton_large_window_delivers_everything_exactly_once`, `livefeed_fork_foreign_only_window_is_progress`

---

## 4. Edits, file by file, in commit order

Ceilinged files touched: **only `src/sse/feed.rs`, 1,195 lines now, ceiling 1,195.** After C1 it is 1,190; after C2, 1,168; after C3, 1,167 (1,169 if C4 lands). None of the other ceilinged files (http.rs 3,369, product.rs 4,205, shard.rs 3,196, billing.rs 2,201, history.rs 1,713, auth.rs 1,676, src/registry.rs 1,501, fleet.rs 1,143) is touched. Other touched files, none ceilinged:

| File | Now | After |
|---|---|---|
| src/sse/feed/test_support.rs | 50 | ≈58 |
| src/sse/feed/tests.rs | 970 | ≈950; must stay ≤1,000, and it shrinks |
| src/sse/registry.rs | 385 | ≤385 |
| src/sse/service.rs | 142 | 141 |
| src/sse/feed/drive.rs | 214 | 215, only with C4 |

Exception-scoped items touched, and their remedies:

| Scope (identity) | Touched by | Contract effect | Remedy |
|---|---|---|---|
| feed.rs:604-608 impl-wide `unwrap_used` on `impl LiveFeed` | C1 (retained removed), C2 (new_with_budget, take_visible, read_and_publish), C3 (new_with_budget signature) | scope_lines, nested_items and syntax_facts all fall. Per-key changes: `path` keys `popped` −2, `clear_ring` −2, `self` −4, `st` −2, `Ordering::Relaxed` −2, `AtomicUsize::new` −1, `FeedKey` −1. `ordinary-call` keys `clear_ring(..)` ×2 and `AtomicUsize::new(0)` removed. No key is added or grows. `unwrap_sites` unchanged. | none needed |
| feed.rs:817-821 `take_visible` (`excessive_nesting`, `expect_used`) | C2 | Shrinks. Both lints stay fulfilled: `.expect("front checked")` remains, and while→if keeps the same depth as the old if-popped→if-empty. | none needed |
| feed.rs:904-912 `read_and_publish` (6 lints incl. `expect_used`) | C2 | Shrinks. All six stay fulfilled; the fn goes from 163 to 152 lines, still over 100. | none needed |
| drive.rs:37-40 impl-wide `unwrap_used` | C4 only | grows (scope_lines, syntax_facts, cfg path key) | D1 |

`impl Drop for LiveFeed`'s expect and every other expect are untouched. No `#[expect]` becomes unfulfilled or deleted, so no `source-allowances.json` row is vacated. The only `sse/feed.rs` rows there are `macro-dsl` `CancelFlag::cancelled` and `read_and_publish`, both unchanged.

### Commit C1: "Retention tests read the ring's own charge, not a shadow gauge"

**`src/sse/feed.rs`.** Delete lines 764-768, the `#[cfg(test)] pub(crate) fn retained ...` item and its trailing blank line.

**`src/sse/feed/test_support.rs`.** Module doc, lines 1-2:
```rust
//! Test-only knobs of the feed memory budget (sized, exhausted and released
//! per rig) and the ring charge its reservations are checked against.
```
Append:
```rust

impl LiveFeed {
    /// Retention tests compare the budget's reservation with the charge the
    /// ring itself holds, the number `Drop` releases, never with a copy of it.
    pub(crate) fn retained(&self) -> usize {
        self.st.lock().unwrap().charge
    }
}
```
Clippy accepts this `unwrap` because the file is `#![cfg(test)]` (`allow-unwrap-in-tests = true`). The precedent is `project_entries_for_test`, test_support.rs:26-28, which calls `.lock().unwrap()` with no expect. No exception scope covers it, so no ratchet contract applies. `use super::*` gives the child module access to the private `st` and `charge`.

**`docs/quality/owners.json`.** In the `unresolved-glob` row for `src/sse/feed/test_support.rs`, change the reason to: "Feed budget test knobs import their enclosing feed owner; the compiler resolves the parent exports and the knobs only size, exhaust and release budgets or read a feed's retained charge." Only the reason text changes; category, path, owner, syntax and count stay as they are.

### Commit C2: "The feed's retained charge lives only in its ring state: the shadow gauge is gone"

All edits are in `src/sse/feed.rs`. Line numbers are pre-C1.

1. **:28** becomes `use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};`
2. **:521** delete `    retained_charge: AtomicUsize,`
3. **:653** delete `            retained_charge: AtomicUsize::new(0),`
4. **take_visible, :831-850.** Delete :832 `let mut popped = false;`, :840 `popped = true;`, :841 `}`, :842 `if popped {` and :848 (the store). The unchanged inner `if st.batches.is_empty() { .. }` block, with its comment, now sits in the loop at the same indentation. The git diff is deletions only. Result:
   ```rust
           if self.subscribers.load(Ordering::Relaxed) == 1 {
               while let Some(b) = st.batches.front() {
                   if b.scan_to > cursor {
                       break;
                   }
                   let b = st.batches.pop_front().expect("front checked");
                   st.charge -= b.charge;
                   self.budget.release(&self.project_reserved, b.charge);
                   if st.batches.is_empty() {
                       // Nothing below the survivor's own cursor is owed
                       // to anyone: the floor may follow it.
                       st.floor = st.floor.max(cursor.min(st.head));
                   }
               }
           }
   ```
   This removes a boolean flag (a simpler state model), and it is the only clippy-clean form that adds no new operator mutant.
5. **read_and_publish.** Replace :1004-1009 and :1050-1055, each with `st.clear_ring(&self.budget, &self.project_reserved);`. Delete :1073, the store.
6. **:1078-1091.** The free fn becomes a method, with the doc comment moved verbatim:
   ```rust
   impl FeedState {
       /// Release and clear EVERY retained batch (uncached posture): nothing
       /// unreachable may keep a global reservation.
       fn clear_ring(&mut self, budget: &FeedMemoryBudget, proj: &ProjectRetention) {
           for b in self.batches.drain(..) {
               budget.release(proj, b.charge);
           }
           self.charge = 0;
       }
   }
   ```
   It stays in place, between `impl LiveFeed` and `impl Drop for LiveFeed`, outside every exception scope. `&self.budget` is `&Arc<FeedMemoryBudget>` and deref-coerces. Why a method (§1d.3): the ring state owns its own reset, and the call sites stop being fingerprinted `call-site` facts.

No other file changes.

### Commit C3: "A feed never used its key: new_with_budget no longer takes one"

1. **`src/sse/feed.rs:626`.** Delete `        _key: FeedKey,`. The signature stays vertical because it is over 100 columns.
2. **`src/sse/service.rs`.** Delete :63 `let k2 = key.clone();`. At :68, change the call to `super::feed::LiveFeed::new_with_budget(s2, ring, budget, project)`.
3. **`src/sse/registry.rs`, test module.**
   - :225-228 becomes:
     ```rust
     fn make_feed(budget: &Arc<FeedMemoryBudget>) -> Arc<LiveFeed> {
         let src = Arc::new(FakeSource::new(0, 8));
         LiveFeed::new_with_budget(src, RING, budget.clone(), tpid())
     }
     ```
   - The ten `make_feed(key(n), &budget)` calls become `make_feed(&budget)`.
   - At :340 and :349, `LiveFeed::new_with_budget(src, 0, budget.clone(), tpid())`.
   - `fn key` stays; `subscribe(key(1), ..)` still uses it.
4. **`src/sse/feed/tests.rs`.** Drop the `FeedKey::default_lane(..)` first argument at :202, :637, :648, :767 and :776. rustfmt collapses :202, :767 and :776 to one line each. `FeedKey` is no longer named in this file; `use super::*` is a glob, so no warning.
5. Run `cargo fmt --all` and let rustfmt decide the layout.

### Commit C4, only if D1 = (b): "A feed's source-read tally exists only in test builds"

- **`src/sse/feed.rs`.** Add `#[cfg(test)]` above :522 `source_reads: AtomicU64,` and above :654 `source_reads: AtomicU64::new(0),`.
- **`src/sse/feed/drive.rs:120`.** Add `#[cfg(test)]` above `self.source_reads.fetch_add(1, Ordering::Relaxed);`.
- **Re-decided reasons in the same commit.** Each needs exactly two `;` and no `"`. Proposed texts, which are Søren's to accept or reword:
  - feed.rs:604-607: `"LiveFeed; a poisoned feed state or source cell may hold a partially published frontier, subscriber accounting or swapped source, while test builds add only counters; recovering it could deliver records twice, skip them or serve a retired source"`
  - drive.rs:37-40: `"LiveFeed drive; a poisoned feed state may hold a partially published head, version or lifecycle, while test builds add only a read tally; recovering it could re-read delivered records or settle a transition the feed already retired"`

---

## 5. Mutation analysis (cargo-mutants 27.1.0, `--in-diff`)

In-diff semantics (`in_diff.rs::affected_lines`): inserted lines, plus the new-file lines immediately before and after every deletion, select any mutant whose span covers them. The FnValue span runs from the first to the last body statement, so signature-only edits select nothing.

**File classification** (`production_changes.py`):
- `test_support.rs` and `feed/tests.rs` are `#![cfg(test)]` files: production-unchanged, omitted.
- `registry.rs` changes only inside `#[cfg(test)] mod tests`: production-unchanged, omitted.
- In C1 alone, `feed.rs` loses only an explicit `#[cfg(test)]` item: production-unchanged.
- Mutation sources for the pushed range: `src/sse/feed.rs` (owner `sse_feed`, filter `sse::`) and `src/sse/service.rs` (owner `sse_service`, filter `sse::`). With C4, also `src/sse/feed/drive.rs` (owner `sse_feed_drive`, filter `sse::`).

No new files, so no new owner rows, and `scripts/quality/mutation_owners.py` is unchanged.

| # | Commit | Mutant (by content) | Why selected | Outcome, and killing test |
|---|---|---|---|---|
| 1 | C2 | `replace LiveFeed::new_with_budget -> Arc<Self> with Arc::new(Default::default())` | line after the deleted `retained_charge` init | **unviable** (`LiveFeed: !Default`) |
| 2 | C2 | `replace LiveFeed::take_visible -> Take with Default::default()` | fold and store deletions | **unviable** (`Take: !Default`) |
| 3 | C2 | `replace == with != in LiveFeed::take_visible`, on the `if self.subscribers.load(Ordering::Relaxed) == 1` line | line before the deleted `let mut popped = false;` | **caught** by `sse::feed::tests::survivor_drains_retained_batches_after_drop_to_one`: without the drain, `take_visible(5)` leaves the batch, so `assert_eq!(feed.retained(), 0, "the passed batch was released")` fails. Also `floor() == 5` and `budget.reserved() == 0`. |
| 4 | C2 | `replace LiveFeed::read_and_publish -> DriveOutcome with Default::default()` | `clear_ring` call and store edits | **unviable** (`DriveOutcome: !Default`; the 13:23 local run already recorded this one as unviable) |
| 5 | C2 | `replace FeedState::clear_ring with ()` | new fn | **caught** by `sse::feed::tests::oversized_batch_clears_the_old_ring` ("the old ring was cleared too": `retained()` = 340 ≠ 0, reserved 340 ≠ 0) and `sse::feed::tests::external_exhaustion_clears_unreachable_ring` ("A's stale ring was cleared"; reserved 680 ≠ 340) |
| 6 | C3 | `replace LiveFeedService::subscribe -> Result<FeedSubscription, CapacityRejected> with Ok(Default::default())` | `k2` deletion and call edit | **unviable** (`FeedSubscription: !Default`; no `--error` values configured) |
| (C3) | C3 | `feed.rs` `_key` deletion | signature lines only | none |
| 7 | C4 | FnValue on `drive_under_permit` and on `new_with_budget` | inserted `#[cfg(test)]` lines | both **unviable** |

Nothing else is affected:
- **Other operators in these fns** sit on lines no edit touches: `>` at the `b.scan_to > cursor` line, `-=` at `st.charge -= b.charge`, and in read_and_publish `>` at `batch_charge > self.ring_budget`, `+=`, `-=` and `<=`.
- **The `ReserveOutcome::ProjectOver` MatchArm mutant** spans only its own arm. The adjacent line is the inner match's closing `}`, which no mutant span covers.
- **Macros** (`tracing::warn!`, `matches!`) are not mutated.

**Totals.** C1-C3 give sse_feed 5 mutants (2 caught, 3 unviable) and sse_service 1 (unviable): six selected across two owners, zero missed. The change adds no new predicate, guard or boundary. The fold keeps `if st.batches.is_empty()` unchanged, and cargo-mutants does not mutate method-call conditions. No equivalent mutant is introduced.

**Timeout risk.** Mutant 3 already exists in the owner's full scope, but it is newly in-diff. A `sse::` test that hangs under it would count as a miss. Control 7.4 below runs it before push. If it times out, the fallback for step C2.4 keeps `popped` and hoists the comment above the inner `if`. Clippy's collapsible_if does not fire on a block that starts with a comment, and this leaves the `== 1` line unaffected. Verify that fallback with clippy before relying on it.

---

## 6. Ledgers

- **`docs/quality/owners.json`.** The reason text for the test_support.rs `unresolved-glob` row changes in C1; the count stays 1. That is the only ledger change.
- **`docs/quality/source-allowances.json`.** No row vacated, no `--prune`. Confirm by expecting no `obsolete source allowances` line from gate.py.
- **`docs/refactor/test-inventory.json`.** Unchanged: no `src/dst` file changes. The `retained` and `source_read_count` names are kept so `livefeed_swap.rs` and `livefeed_basics.rs` stay byte-identical. `scripts/test-inventory.py --check` must stay OK.
- **`docs/refactor/review-mechanisms.json`.** No pins on sse/feed or sse/registry tests (grep).
- **`docs/refactor/test-scenario-map.json`, `scenario-dispositions.json`, `SCENARIO-MAP.md`.** No tests are renamed. Their `src/sse/registry.rs:221/244` line refs are already stale, and the scripts do not check line numbers.
- **`docs/refactor/WIRE-MATRIX.md`.** No wire change.
- **`docs/refactor/architecture-policy.json`.** `sse_core_files` is unchanged; no transport paths are added.
- **Historical anchors (`architecture-review-baseline.json`, `architecture-baseline.json`, `docs/quality/verification.json`).** Recorded at old revisions and not updated per change; the files only shrink.
- **`scripts/quality/mutation_owners.py`.** Unchanged.
- **C4 only.** Re-decided reason strings in source. There are no ledger rows for reasoned expects.

---

## 7. Controls (run after each commit unless noted)

1. **Census steps**, before the full C2 and C3 edits, per §3: expect five distinct E0560/E0609 sites, then nine E0061 sites.
2. **Format:** `cargo fmt --all -- --check`. Expect no output, exit 0.
3. **Clippy and ratchets:**
   ```bash
   cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings > target/quality/clippy.jsonl
   python3 scripts/quality/gate.py --clippy target/quality/clippy.jsonl
   ```
   Expect clippy exit 0. The last line reads `quality ratchets: OK; <n> Rust files; 0 emitted warning occurrences; <S> accepted exception scopes; base <sha12>`, with S equal to its pre-change value. There must be no `accepted exception grew` line, no `obsolete ... --prune` line, and no `file growth` line. For C4 without D1 approval, expect these three failures, which prove the D1 analysis:
   - `accepted exception grew without a new decision: ('src/sse/feed.rs', 'crate::LiveFeed', 'impl', ...): unwrap_site:path:crate::LiveFeed::new_with_budget:<d> 2 -> 3`
   - `('src/sse/feed/drive.rs', 'crate::LiveFeed', 'impl', ...): scope_lines ... -> +1`
   - `... syntax_facts ... -> +2`
4. **Mutation**, after C3 and before push, isolated to this item:
   ```bash
   QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=fb18840d QUALITY_MUTANTS_OUT=target/qm-item60 scripts/quality/mutations.sh
   ```
   Adjust `QUALITY_BEFORE_SHA` if other commits land first. Expect selected owners `sse_feed` and `sse_service`, and `Mutation verification executed 6 selected mutant(s) across 2 registered owner(s).` Check `target/qm-item60/sse_feed/mutants.out/missed.txt` and `timeout.txt`: both empty, `caught.txt` holds mutants 3 and 5, and `unviable.txt` holds mutants 1, 2 and 4. For sse_service, 1 unviable. To preview the scope first: `cargo mutants --list --in-diff target/qm-item60/pr.diff --file src/sse/feed.rs --package streams-slate` lists exactly mutants 1-5. Then run it once more with the CI push range (`QUALITY_BEFORE_SHA=$(git rev-parse origin/slate)`), per the "run CI's plan before push" rule.
5. **Targeted tests that prove they ran:**
   ```bash
   scripts/test-leg.sh target/quality/item60-sse.log \
     --exact sse::feed::tests::survivor_drains_retained_batches_after_drop_to_one \
     --exact sse::feed::tests::oversized_batch_clears_the_old_ring \
     --exact sse::feed::tests::external_exhaustion_clears_unreachable_ring \
     --exact sse::feed::tests::concurrent_retention_never_exceeds_cap \
     --exact sse::feed::tests::retained_batches_drain_without_extra_reads \
     --exact sse::registry::tests::zero_ring_is_singleton_only \
     -- --locked --lib sse::
   scripts/test-leg.sh target/quality/item60-dst.log -- --locked --lib dst_tests::livefeed_
   ```
   Expect every result line to read ok and every `--exact` name to have its own `... ok` line.
6. **Other gates:**
   - `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items`: exit 0
   - `python3 scripts/architecture-gate.py --check`: OK
   - `python3 scripts/test-inventory.py --check`: OK, unchanged
7. **Line counts:** `wc -l src/sse/feed.rs` gives 1190 after C1, 1168 after C2 and 1167 after C3; `src/sse/feed/tests.rs` stays ≤ 1000.
8. **Before push:** full `scripts/quality.sh` ending in `QUALITY_OK`. After push, `gh run view` on the push run; never claim green without it.

---

## 8. Out of scope

- **`source_reads` gating.** It lands only if Søren picks D1 = (b) or (c); otherwise it stays production state, costing 8 bytes per feed and one relaxed add per source read.
- **Narrowing the impl-wide `unwrap_used` expectations** in feed.rs and drive.rs to per-fn scope. That would admit the `source_reads` gate without a reason re-decision, but it is its own item.
- **The other `#[cfg(test)]` accessors inside the unwrap-scoped impl:** `lifecycle_for_test`, `version`, `floor`, `source_read_count`. `retry_spawns` and `fp_name` are also left alone.
- **FakeSource read counting, and any DST test change.**
- **Stale line refs** in the scenario maps and the reviewer's stale `feed.rs:1241-1252`.
- **Items 59 and 61.**

---

## Decisions for Søren

- **D1: `source_reads`.** The reviewer's "cfg(test)-gate like retry_spawns" cannot pass the exception ratchet as-is: it grows the feed.rs `impl LiveFeed` contract (cfg path key 2→3) and the drive.rs `impl LiveFeed` contract (scope_lines +1, syntax_facts +2, cfg path key 0→1). Options:
  - **(a) Recommended: leave it** as a production field. It costs 8 B per feed and one relaxed add per read, and C1-C3 land without it.
  - **(b) Apply C4** with re-decided reason text on both impl-wide expectations (texts in §4).
  - **(c) Narrow drive.rs's impl-wide expect** to `tail`, `retire` and `bump_version`, and re-decide feed.rs's reason.
- **D2 (low stakes, to confirm).** `clear_ring` becomes the method `FeedState::clear_ring` instead of staying a free fn with three parameters. Keeping the free fn would mean re-deciding two reasons: the impl-wide `unwrap_used` and `read_and_publish`'s `expect_used`, because the changed call tokens are new `ordinary-call` fingerprints.

---

## Skeptic corrections (C1..C7)

Checked against the tree at `fb18840d`, read-only. These claims hold up:
- **Use sites.** All `retained_charge`, `source_reads`, `retained()`, `source_read_count()` and `new_with_budget` sites match my grep over src, tests, benches, examples, fuzz, tools, sdk and bench. Nothing in docs/, scripts/ or .github/ names them; only the historical legacy-diagnostics JSON names `take_visible`.
- **Line counts.** feed.rs 1,195, tests.rs 970, test_support.rs 50, registry.rs 385, service.rs 142, drive.rs 214.
- **Collapsible-if analysis.** Correct.
- **Nesting and length after the fold.** The fold keeps `excessive_nesting` fulfilled: impl, fn, if, while, if gives depth 5, over the threshold of 4. read_and_publish drops from 129 to 118 non-comment code lines, so `too_many_lines` stays fulfilled.
- **Unviable mutants.** `Take`, `DriveOutcome`, `LiveFeed` and `FeedSubscription` all lack Default. `FeedRegistry` derives it, but no mutant returns a `FeedRegistry`.
- **In-diff semantics.** They match cargo-mutants-27.1.0 `in_diff.rs::affected_lines` and `visit.rs::function_body_span`, which runs from the first statement to the last, with no braces.
- **`==` mutant.** `==`→`!=` at feed.rs:831 is killed by `survivor_drains_retained_batches_after_drop_to_one` (tests.rs:401).

**C1 (blocking): C2 as written fails the exception ratchet in `new_with_budget`.** `Arc::new(Self { .. })` (feed.rs:639-671) is an `ExprCall`. `tools/quality-syntax/src/scan.rs:242-255` records it as a `call-site` fact whose value is `std::sync::Arc::new\t<tokens of the whole call>`, including the struct literal. Under the impl-wide `unwrap_used` expect (feed.rs:604-608), `source_rules.py:176-180` fingerprints every `call-site` as `unwrap_site:ordinary-call:crate::LiveFeed::new_with_budget:<sha(qualified\0value)>`.

Deleting `retained_charge: AtomicUsize::new(0),` (feed.rs:653) changes that value. The old key goes 1→0, but the new key goes 0→1, which `exception_growth` (source_rules.py:204-215) reports as `accepted exception grew without a new decision`.

This makes two statements in the plan wrong:
- §4's table row and §1d.3 say "No key is added or grows". They do not account for this call-site.
- C4's feed.rs half has the same flaw. Adding `#[cfg(test)]` inside the literal (:654) re-fingerprints the same call. The C4 failure list in control 7.3 omits this key.

Recommended fix, which needs no decision: add a verbatim-move commit **C1b** after C1. Move `new_with_budget` (feed.rs:625-677) unchanged into its own `impl LiveFeed { .. }` block that carries no `#[expect]`. Its body has no `unwrap`/`expect`, so this narrows the exception as RUST-QUALITY.md:49-51 allows. An optional one-line `//` owner note can say that construction takes no lock.

Put the block right after `pub(crate) struct LiveFeed { .. }` (ends feed.rs:548), or after the scoped impl closes (feed.rs:1076). Do **not** put it directly before `#[expect] impl LiveFeed` (feed.rs:604). There, git's minimal diff would move the attribute plus `bind_pressure` (about 24 lines) instead of `new_with_budget` (52 lines). That selects `replace LiveFeed::bind_pressure with ()`, and no `sse::` test pins it: its only caller is session.rs:233. That mutant would be missed.

After C1b, the C2 and C3 edits to `new_with_budget` sit outside every exception scope.

Alternative, which needs a decision (**D0**, Søren's): keep the impl whole and re-decide the feed.rs:606 reason text in C2. That creates a new identity, so there is no comparison.

**C2 (mutation cost of C1b): a new test is needed.** The moved body shows up as inserted lines. That selects:
- the FnValue `replace LiveFeed::new_with_budget -> Arc<Self> with Arc::new(Default::default())`, which is unviable
- **two viable operator mutants** on feed.rs:659: `replace / with %` and `replace / with *` in `(ring_budget.saturating_mul(2) / 3).clamp(1024, MAX_DRIVER_BATCH_BYTES)`

No `sse::` test pins `read_cap`. Its only reader is feed.rs:915, and the FakeSource rigs read payload-8 records a few at a time, so page caps of 1024, 2730 or 24576 behave the same. Also, `*` is equivalent whenever ring ≥ 393,216, and `%` whenever ring ≤ 1,536. Both would be MISSED.

Add a pin test to `src/sse/feed/tests.rs`. It can read the private field from the child module. For example, `read_cap_is_two_thirds_of_the_ring_within_its_bounds`:
- `feed_with(0, 8, 4096, &budget).0.read_cap == 2730` kills `%` (gives 1024) and `*` (gives 24576)
- `1 << 20` gives `262_144`
- `1_000` gives `1024`

It passes today. It pins the refactor; it is not a red test. tests.rs stays under 1,000 lines: about 950 after C3's collapses, plus about 14.

Revised mutation totals: sse_feed has 7 mutants: new_with_budget FnValue, #2 and #4 unviable; the two `/` mutants, #3 and #5 caught. sse_service has 1 (unviable). Control 7.4's expected count becomes 8 across 2 owners. Before running mutations, run `cargo mutants --list --in-diff` and confirm that no `bind_pressure`, `CancelFlag::*` or `DriverPermit::drop` mutant appears.

**C3: line budget.** C1b adds 3 or 4 lines to feed.rs: the `impl LiveFeed {` line, the closing `}`, a blank line and an optional `//` line. It must land after C1, because 1,195 + 3 would exceed the 1,195 ceiling. Revised counts for control 7.7:

| After | feed.rs lines |
|---|---|
| C1 | 1,190 |
| C1b | 1,193 or 1,194 |
| C2 | 1,171 or 1,172 |
| C3 | 1,170 or 1,171 |

**C4: D1 gets smaller.** With C1b in place, C4's feed.rs edits (`#[cfg(test)]` on the field at :522 and on the init at :654) grow no contract. Only the drive.rs:37-40 impl-wide expect grows: `scope_lines` +1, `syntax_facts` +2 (an attribute fact plus a `cfg` path, per scan/attributes.rs and `visit_attribute`), and `unwrap_site:path:crate::LiveFeed::drive_under_permit:<cfg>` 0→1. D1(b) then needs only the drive.rs reason re-decided, and D1(c) needs only the drive.rs narrowing. If Søren picks D0 instead of C1b, the feed.rs reason re-decided in C2 also covers C4's feed.rs half.

**C5: stale header.** `git rev-parse origin/slate` = `fb18840d`, and `git status` shows slate level with origin. The four commits are pushed. The gate's merge base (common.py:18-27) is `fb18840d`, so the two runs in control 7.4 are the same run. Nothing else changes: the feed.rs diff in 7c4f8606..fb18840d only deleted `floor_for_test`, and no mutant span covers the lines next to it.

**C6: timeout coverage.** A TIMEOUT counts as a miss. The timeout risk applies to mutant #5 (`FeedState::clear_ring` → `()`, where the ring and budget are never released under `concurrent_retention_never_exceeds_cap`'s 32 drivers) and to the new `/` mutants, as well as to #3. The C2 check of `timeout.txt` must cover all of these. The existing control already reads `timeout.txt`, so only the expected lists need updating.

**C7: ledgers.** After C1b and the pin test, the ledger conclusions still hold:
- No new file, so no `mutation_owners.py` row.
- No `src/dst` change, so test-inventory.json is unchanged (it hashes only `src/dst`, per scripts/test-inventory.py:138).
- No review-mechanisms.json or architecture-policy.json change. feed.rs has no `function:` budget exceptions.
- source-allowances.json is unchanged. Its only feed.rs rows are the `macro-dsl` rows for `CancelFlag::cancelled` and `read_and_publish`.
- The owners.json reason edit for `src/sse/feed/test_support.rs` (owners.json:2124-2130) is allowed. owners.json is not in the policy.json `immutable_sha256` list.

No wire or product-edge contract changes. Nothing under docs/ documents the removed state.

**Verdict: ready-with-corrections.** C1 blocks C2: land C1b plus the `read_cap` pin test, or take decision D0. With that, C1 through C3 are buildable and decision-free, and C4 stays behind a D1 that now covers drive.rs only.
