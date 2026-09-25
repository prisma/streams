# Item 76, steps A + D: orphaned history.rs docs and the never-read Absorber fields

Tree: `slate` HEAD 5d9d517f (15 unpushed commits over origin/slate 77c13d74). All line numbers
below are from HEAD. The implementer re-finds each site by the quoted content, not by the number.
Do not start until the running mutation leg has finished: the controls below build and test.

## 1. Problem (checked on the current tree)

### 1.0 The reviewer's premise is stale. The growth gate passes today.

The reviewer wrote "working tree is 1,750 > 1,744 so the file-growth gate fails on this branch
now", with the first step "Run the source gate (must fail 1750 > 1744)". That is **not true on
the current tree**:

- `wc -l src/history.rs` gives **1,693**. `git show origin/slate:src/history.rs | wc -l` gives
  **1,713**. The legacy ceiling (`docs/quality/legacy-source.json`) is 2,418.
- The limit is `min(max(1000, legacy), max(1000, merge-base))` = 1,713, so the gate passes today
  with 20 lines to spare.

Steps A and D are therefore hygiene, not headroom work. Both only shrink the file. Steps B and C
(moving the budget types and wedge tests out) are not needed and are not planned.

### 1.1 Step A: orphaned doc fragments and headers (all real)

| Reviewer ref | Now | Exact text | What it documents today |
|---|---|---|---|
| (empty header) | :35 | `// ---- block transformer: AES-256-GCM with a random nonce per block ----` | Nothing. No block transformer exists in `src/history.rs`: grep finds no `BlockTransformer` or `block_transformer` in `src/`. The next items are the scan-option fns. |
| :41 | :37 | `/// Operator pause for the whole absorber (fleet runbook).` | Rustdoc merges it into the doc of `hist_scan_opts` (:38-41), so it mis-documents that fn. It is left over from `absorb_pause_flag()`, which was deleted in 7975ca77 when the flag moved to `HistoryResources::paused` (:220). |
| (misplaced header) | :83 | `// ---- settings (D23 maintenance profile + F2 pattern) ----` | It heads `history_l0_stats` and the budget section. The settings fns it names are `history2_settings`/`history_settings` at :450-537. D23 and F2 are SPEC.md IDs that those fns still implement (`wal_enabled: false`, GC profile). |
| :89-91 | :85-87 | `/// Shared block cache for ALL history DBs (absorber writes + reads):` / `/// SlateDB's per-DB default is 512 MB, and the absorber opens a DB per` / `/// absorbed stream — unbounded aggregate cache on a 1 GB box.` | It is prepended to the doc of `history_l0_stats` (:88-93). It describes the deleted v1 per-stream DBs ("opens a DB per absorbed stream"). |
| :212-214 | :211-213 | `/// The gather packing limit AS RESOLVED at startup — after the clamp to` / ``/// `capacity / ABSORB_BUILD_MULTIPLIER`. Published so the concurrency`` / `/// arithmetic below matches what the absorber actually does.` | It is prepended to the doc of `struct HistoryResources` (:214). Re-homing it is not safe: `packing_bytes` is read only by `per_gather_reservation_bytes` (:262). The absorber packs by `cfg.gather_max_bytes`, which bootstrap clamps on its own (bootstrap.rs ~:399-415). So the "matches what the absorber actually does" claim would move with it. |
| (inside :753-770) | :752-759 | `/// History DB handles kept open across passes. The original F2 design` … `/// absorber exit.` (8 lines) | Rustdoc attaches it to the `submitted` field, ahead of that field's real doc at :760. The LRU of history DB handles (604d20ba) no longer exists: grep finds no LRU, `open_dbs` or DB-handle map in `src/history*` apart from a stale comment in `worker.rs:237` (out of scope). |
| hoist | :862 | `use std::sync::atomic::AtomicU64;` | A `use` in the middle of the file, after `impl Absorber`, although `AtomicU64` is used from :160 onwards. |

### 1.2 Step D: `Absorber.data_store` and `Absorber.keys` are never read (real)

The struct is at src/history.rs:734-778:

```rust
#[expect(
    dead_code,
    reason = "Absorber; the store and key cache it was started with are kept for the gather planner, which reaches them through the engine today; dropping them would touch boot's mutation-gated wiring for no behaviour change"
)]
pub(crate) struct Absorber {
    ...
    data_store: Arc<dyn ObjectStore>,   // :748
    shard: Arc<ShardEngine>,
    keys: Arc<KeyCache>,                // :750
    ...
```

**Reads.** The fields are private, so only `crate::history` and its descendants can read them.
There are no `#[path]` includers of history files outside `src/dst/dst_tests.rs`, and those are
DST test files in another module. `tools/`, `fuzz/`, `bench/` and `tests/` do not reference them.

- `grep -rn "self\.data_store\|self\.keys\|absorber\.keys\|absorber\.data_store\|\.data_store\b" src/history.rs src/history/` finds **nothing**.
- The only `.keys` hit is `pending.keys()` in `worker.rs:196`, which is a HashMap.
- The only writes are the constructor (`Absorber { data_store, shard, keys, cfg, ... }`, :793-805).
- No read exists under `cfg(test)` either: the child test modules (`controller_tests`,
  `bounded_discovery_tests`, `test_support`, `worker/lane_isolation_tests`, `history::tests`) only
  pass values into the constructors.
- Control C1 below (§7) turns this grep result into a compiler proof.

The other five fields are all read in production:

- `recent_transient` at :820/834/836
- `shard` in `worker.rs:30,54,93,162` and :817
- `cfg` in `worker.rs:55,59,125,226,411`
- `submitted` in `gather.rs:340,681` and `worker.rs:136`
- `discovery_after` in `gather.rs:268` and `worker.rs:85,96`

So the struct-wide `dead_code` expect covers exactly these two fields. Deleting them fulfils
nothing it covers, so the expect has to go in the same edit, because
`unfulfilled_lint_expectations = "deny"`.

**Constructors that take the dead values:**

- `Absorber::new(data_store, shard, keys, cfg)` at :783-806
- `Absorber::start_owned(data_store, shard, keys, cfg, rx)` at :850-859
- the test-only `Absorber::start(data_store, shard, keys, cfg, rx)` in `src/history/test_support.rs:11-19`

**All 35 constructor call sites** (`grep -rnE "Absorber::(new|start_owned|start)\(" src`), each
with the `store`/`keys` arguments it passes:

| File:line | Fn | store arg | keys arg |
|---|---|---|---|
| src/bootstrap.rs:544 | `run` (per-open async block) | `data_store` (move) | `keys` |
| src/history.rs:1195 | test `absorber_exits_when_shard_engine_is_fenced` | `store.clone()` | `Arc::new(KeyCache::default())` |
| src/history/bounded_discovery_tests.rs:86 | test `r09_discovery_pages_progress_without_exceeding_pending_capacity` | `store` (move) | fresh |
| src/history/controller_tests.rs:109, :170 | `active_absorber_cancel` (**pinned**) | `data_store.clone()` / `data_store` (move) | fresh |
| src/history/worker/lane_isolation_tests.rs:82 | test `corrupt_row_backs_off_only_its_stream` | `store` (move) | fresh (`KeyCache` imported at :4 only for this) |
| src/shard/task_lifecycle_tests.rs:37 | `fixture` | `store.clone()` | fresh |
| src/dst/tests/fixture_http.rs:312 | `rig_opener` (**pinned**) | `store` (move) | `keys` (a param, only for this) |
| src/dst/tests/fixture_storage.rs:142 | `open_engine_with_absorber_layout` | `store` (move) | `keys` (seeded with `keys.put(hash, key.clone(), hash)`, only for this) |
| src/dst/tests/billing_maintenance.rs:270 | test `absorbed_boundary_and_maintenance_retire_atomically` | `store.clone()` | fresh |
| src/dst/tests/reads_history.rs:174, 290, 416 | three tests | `store.clone()` | fresh |
| src/dst/tests/reads_raw.rs:398, 479 | two tests | `store.clone()` | fresh |
| src/dst/tests/history_recovery.rs:144, 301, 411, 510, 622, 697 | six tests | `store.clone()` | fresh |
| src/dst/tests/history_gather.rs:74, 115, 776 | helpers `gather_after_reopen`, `gather_with_pacing`, `absorber_on_pool` | `store.clone()`, `store.clone()`, `store` (move) | fresh |
| src/dst/tests/history_gather.rs:169, 325, 408, 485, 551, 674, 925 | seven tests | `store` (move at :169), else `store.clone()` | fresh |
| src/dst/tests/history_absorption.rs:125, 211, 322, 466 | four tests | `store.clone()` | a `keys` local: seeded with `keys.put` at :121, :208 and :464-465; unseeded at :321 |

Plus the two `Self::new(...)` calls inside `start_owned` (:857) and `test_support::start` (:18).

**Knock-on dead code that the removal exposes** (all real):

- `fixture_storage.rs:132-135` seeds a KeyCache "so the cache must agree or nothing decodes". That
  is a v1 claim: `KEY_CACHE_MAX`'s doc (:553-555) and `gather.rs:6` both say v2 absorption never
  consults it. Once `keys` goes, `hash` and `key` are unused parameters of
  `open_engine_with_absorber(_layout)`. The 6 callers pass `hash, &key`:
  - history_absorption.rs:50, 383, 394
  - reads_history.rs:553
  - runtime_isolation.rs:30, 41

  Every caller uses `hash` and `key` elsewhere (checked), so dropping the two arguments is a pure
  deletion at each call site.
- `history_absorption.rs` seeds `keys.put(...)` at :121, :208 and :464-465 for a cache no absorber
  reads. The `key`, `hash`, `tiny` and `fat` locals keep other uses (checked). At :320-321,
  `// NO keys.put: ...` plus `let keys` becomes an unused variable.
- `fixture_http.rs` `rig_opener` has a `keys` parameter used only for `start_owned`. Its only
  caller, `http_rig_build`, clones `let opener_keys = keys.clone();` (:409) only to pass it
  (:504). The rig's own `keys` stays used by `AppState` (:527).
- `bootstrap.rs` has `let keys = keys.clone();` at :382 (opener factory) and :459 (per-open
  closure), both only for `start_owned`. The outer `keys` (:369) stays used by `AppState` (:667).
- `history.rs:22` `use object_store::ObjectStore;` is used in production only by the two fields
  and the constructor parameters (:748, :784, :851). Afterwards only `cfg(test)` code needs it:
  - the `mod tests` block (SlowPuts, etc.)
  - `controller_tests.rs:16`
  - `bounded_discovery_tests.rs:47`

  All three reach it through `use super::*`. Without a move, the lib build would warn
  `unused import`.

## 2. Contract decision

- **No edge contract changes.** Nothing changes on the product/raw HTTP surface, the wire, config,
  environment, metrics or persisted formats.
- **Crate-internal API only.** `Absorber::new`, `Absorber::start_owned` and the test-only
  `Absorber::start` lose their `data_store` and `keys` parameters:
  - `new(shard, cfg)`
  - `start_owned(shard, cfg, rx)`
  - `start(shard, cfg, rx)`
- **Behaviour is identical.** The values were stored and never read (C1 gives the compiler proof).
- **HIS-002 still holds, and more strongly.** "V2 absorption needs no customer key"
  (`v2_absorbs_without_customer_keys`) now holds by construction: the absorber cannot be handed a
  key cache. The catalogue text ("key cache empty/restarted") stays true at the node level, since
  `AppState.keys` still exists, so neither the catalogue nor the scenario map changes.

## 3. Pinning tests and non-vacuity controls (this is a refactor: no red test)

There is no behaviour change, so no test can go red for the right reason. What the plan does
instead:

**Deadness proof (C1, before step D).** Delete *only* the `#[expect(dead_code, …)]` on
`Absorber` and build. Expected: `cargo clippy --locked --workspace --all-targets` reports, for
both the lib and the lib-test targets:
`src/history.rs:748:5: warning: fields `data_store` and `keys` are never read`
(`= note: -D warnings`). That the test target reports it too proves no `cfg(test)` code reads them.
Revert.

**Pinning tests.** They must be green on HEAD and after each commit, with every assertion
unchanged.

Lib unit:
- `history::tests::absorber_exits_when_shard_engine_is_fenced`
- `history::controller_tests::r09_owned_absorber_cancels_entered_budget_and_replays_dirty_debt`
- `history::controller_tests::r09_owned_absorber_cancels_entered_storage_and_replays_dirty_debt`
- `history::bounded_discovery_tests::r09_discovery_pages_progress_without_exceeding_pending_capacity`
- `history::worker::lane_isolation_tests::corrupt_row_backs_off_only_its_stream`
- `shard::task_lifecycle_tests::` (all six `r17a_*`, via `fixture()`)
- `bootstrap::tests::process_bootstrap_cannot_be_an_empty_success`
- `bootstrap::tests::ignored_absorber_options_cannot_change_active_runtime_configuration`

DST:
- `dst_tests::history_absorption::` (6 tests)
- `dst_tests::history_gather::` (all, including the helper-driven ones)
- `dst_tests::history_recovery::` (6)
- `dst_tests::reads_history::` (4)
- `dst_tests::reads_raw::oversized_keyed_record_pages_through`
- `dst_tests::reads_raw::long_keyed_run_pages_with_progress`
- `dst_tests::runtime_isolation::a_fenced_owners_absorber_exits`
- `dst_tests::billing_maintenance::absorbed_boundary_and_maintenance_retire_atomically`
- `dst_tests::admission_maintenance::raw_hierarchical_append_sheds_typed_503_under_backlog`
  (real absorption through `rig_opener`)
- then the full `cargo test --locked --release` (ci.yml).

**Non-vacuity controls.** Apply each temporarily, confirm the red, then revert:

- **NV1: the rewritten keyless test still needs real absorption.** In
  `v2_absorbs_without_customer_keys`, change the AbsorberConfig to
  `threshold_bytes: u64::MAX, threshold_age: Duration::from_secs(3600)`.
  - `cargo test --locked --lib dst_tests::history_absorption::v2_absorbs_without_customer_keys`
    fails with the panic `keyless v2 absorption never advanced — the gather lane still depends on
    the customer key`.
  - After revert, the test is green.
- **NV2: the rewritten rig opener still starts a draining absorber.** In `rig_opener`, replace the
  `Absorber::start_owned(...)` statement with `drop(absorb_rx);`. The engine's `try_send` result
  is discarded (shard.rs:3080), so nothing else changes.
  - `cargo test --locked --lib dst_tests::admission_maintenance::raw_hierarchical_append_sheds_typed_503_under_backlog`
    fails in `drain_backlog` with `real absorption did not release backlog: ...`.
  - After revert, the test is green.
- **NV3: the bootstrap ratchet is exercised, not bypassed.** Run the source gate after D's code
  edits but *before* re-deciding the two `run` reasons (command in §7).
  - Expected: `accepted exception grew without a new decision: ('src/bootstrap.rs', 'crate::run', 'function', <expect_used attribute>): expect_site:ordinary-call:crate::run:<hex16> 0 -> 1`,
    and the same for `unwrap_site:ordinary-call`.
  - One line per changed call expression inside `run`: the opener's `Box::new(move |prefix, …| …)`,
    `Box::pin(async move …)`, `ShardEngine::start(…)` and `Absorber::start_owned(…)`.
  - With the re-decided reasons, those lines disappear.

## 4. Edits, file by file, in commit order

### Commit A: src/history.rs only (comment and import hygiene)

Suggested subject: "history.rs stops documenting items it no longer has: the block-transformer
header and three orphaned doc fragments go, the settings header moves to the settings".

1. Delete :35-37 (the block-transformer header, the blank line after it, and
   `/// Operator pause for the whole absorber (fleet runbook).`). Do not re-home the pause
   sentence: `worker.rs:172` calls the same flag a "Test hook (SCALING.md D3)", so which
   description is right is a separate question (§8). −3 lines.
2. Delete :83-87: the misplaced `// ---- settings (D23 maintenance profile + F2 pattern) ----`, its
   blank line, and the 3-line "Shared block cache for ALL history DBs…" fragment. Re-insert the
   header and one blank line directly above `/// Settings for the SHARED history v2 partition`
   (:450). Net −3.
3. Delete :211-213 (the "gather packing limit AS RESOLVED" fragment). −3.
4. Delete :752-759 inside `struct Absorber` (the "History DB handles kept open across passes…
   absorber exit." fragment), so `submitted`'s own doc (:760-775) stands alone. −8.
5. Hoist the import:
   - :19 `use std::sync::{Arc, Mutex};` becomes `use std::sync::{Arc, Mutex, atomic::AtomicU64};`.
     That matches the repo's rustfmt order, as in `use object_store::{PutOptions, …, path::Path as OPath};`.
   - Delete :862 `use std::sync::atomic::AtomicU64;` and the blank line after it. −2.

Result: **1,693 → 1,674** (the ceiling is 1,713).

Ratcheted scopes touched in A:

- `struct Absorber`'s `#[expect(dead_code)]` scope shrinks by 8 lines. scope_lines and
  syntax_facts shrink, and its `field_site` keys are unchanged, so nothing grows.
- No `unwrap`/`expect` exception scope contains the edits.
- The `global` allowances for the history.rs statics (syntax `AtomicU64`, the type as written)
  are unchanged, because the statics are untouched.

Ledgers: none. No test body, pinned function, owner row or allowance changes.

### Commit D: the absorber holds only what it reads

Suggested subject: "The absorber holds only what it reads: the store and key cache it never
touched leave its fields, its constructors and every caller".

**src/history.rs** (1,674 → ~1,656)

- Delete :22 `use object_store::ObjectStore;`. −1
- Delete the `#[expect(dead_code, reason = "Absorber; …")]` block (:734-737). −4
- Delete the fields `data_store: Arc<dyn ObjectStore>,` and `keys: Arc<KeyCache>,`. −2
- `new`: `pub(crate) fn new(shard: Arc<ShardEngine>, cfg: AbsorberConfig) -> Self {` fits on one
  line (−5). Drop `data_store,` and `keys,` from the struct literal (−2). The seed computation is
  unchanged.
- `start_owned(shard: Arc<ShardEngine>, cfg: AbsorberConfig, rx: mpsc::Receiver<AbsorbSignal>)`
  stays split across lines by rustfmt (−2). The body becomes
  `let absorber = Self::new(shard.clone(), cfg);`.
  - Its `#[expect(clippy::needless_pass_by_value, reason = "Absorber::start_owned; …")]` stays: the
    lint still fires, because `shard` is still cloned and then borrowed.
  - Its scope shrinks, so there is no growth, and the reason text is still true.
- `mod tests`: change `use object_store::{PutOptions, PutPayload, PutResult, path::Path as OPath};`
  to `use object_store::{ObjectStore, PutOptions, PutPayload, PutResult, path::Path as OPath};`.
  In `absorber_exits_when_shard_engine_is_fenced`, drop `store.clone(),` and
  `Arc::new(KeyCache::default()),` (−2).

**src/history/test_support.rs** (20 → 18)

- `start(shard: Arc<ShardEngine>, cfg: AbsorberConfig, rx: mpsc::Receiver<AbsorbSignal>)`, with
  the body `tokio::spawn(Self::new(shard, cfg).run(rx))`.
- Its `disallowed_methods` expect shrinks.
- The owners.json effect row (`crate::Absorber::start`, `tokio::spawn`, count 1) is unchanged.

**src/history/controller_tests.rs**

- Add `use object_store::ObjectStore;` (+1). It was reached through the glob.
- Line :109 becomes `Absorber::start_owned(engine.clone(), absorber_cfg.clone(), absorb_rx);`.
- Line :170 becomes `Absorber::start_owned(retry.clone(), absorber_cfg, absorb_rx);`.
- In the second `ShardEngine::start`, `data_store.clone()` becomes `data_store`, since it is now
  the last use.
- The fn-scoped `too_many_lines` expect (179 code lines to ~171, still over 100) and the
  `let_underscore_must_use` expect both shrink.
- **Pinned** (see §6).

**src/history/bounded_discovery_tests.rs**

- Add `use object_store::ObjectStore;` (+1).
- `let absorber = Absorber::new(engine.clone(), AbsorberConfig::default());`
- The preceding `ShardEngine::start(…, store.clone(), …)` becomes `store`, the last use.
- The test's `cast_possible_truncation` and `let_underscore_must_use` expects shrink.

**src/history/worker/lane_isolation_tests.rs**

- `use crate::history::{AbsorberConfig, absorber_channel};` (drop `KeyCache`, which is otherwise
  an unused import).
- `let absorber = Absorber::new(engine.clone(), cfg);`
- `ShardEngine::start(…, store.clone(), …)` becomes `store`.

**src/shard/task_lifecycle_tests.rs** (300 → ~298)

- `crate::history::Absorber::start_owned(engine.clone(), crate::history::AbsorberConfig::default(), rx);`
  (rustfmt keeps it split: over 100 columns).
- `store.clone()` stays: `store` is still returned.

**src/bootstrap.rs** (923 → 915; critical, and mutation owner `bootstrap`)

- Delete `let keys = keys.clone();` at :382 (opener factory) and at :459 (per-open closure).
- In the per-open `ShardEngine::start(…)`, change `data_store.clone(),` (:517) to `data_store,`.
  This is its last use inside the async block. Keeping `.clone()` would also compile, since clippy
  does not flag coroutine-saved locals, as `prefix.clone()` two lines up shows. The move is
  simply what is true now.
- :544-550 becomes `Absorber::start_owned(engine.clone(), absorber_config, absorb_rx);` (one line).
- **Re-decide both `run` reasons** (the NV3 remedy; the precedent is 71345c03). Each keeps exactly
  two `;` and no `"`:
  - `clippy::expect_used`: `reason = "run; covers exactly one site, the maintenance-worker spawn, and none in the shard opener: the runtime's task supervisor is fresh at boot, so it accepts that worker; a fallible spawn would leave the process serving without maintenance"`
  - `clippy::unwrap_used`: `reason = "run; covers exactly four sites, the shared-cache lock and the three auth file paths, and none in the shard opener: a poisoned cache lock at boot would mean a half-built shared cache, and those paths were validated by the CLI parser before boot began; recovering the former or re-checking the latter would boot on state the parser already rejected"`
  - These are true on the tree: the one `.expect(` (:718) and the four `.unwrap()` (:610, and
    :756, :759, :762) are all outside the opener closure (:379-~555). :610 is the fleet-token
    cache closure.
- The other four `run` expects (`too_many_lines`, `cast_possible_truncation`,
  `let_underscore_must_use`, `excessive_nesting`) only shrink, and all still fire.
- The architecture budget has no `bootstrap::run` exception, so nothing becomes obsolete.

**src/dst/tests/fixture_http.rs** (803 → ~795)

- `rig_opener`:
  - Delete the `keys: Arc<crate::history::KeyCache>` parameter and `let keys = keys.clone();`.
  - `ShardEngine::start(prefix, Arc::new(db), store.clone(), …)` becomes `store`, the last use.
  - Drop the `store,` and `keys,` arguments of `start_owned`.
  - It now has **5 parameters**, so `clippy::too_many_arguments` (threshold 5) no longer fires.
    **Delete** its `#[expect(clippy::too_many_arguments, reason = "rig_opener; the rig opener takes the store, keys, …")]`,
    which would otherwise be unfulfilled and denied. It is a reasoned exception with no allowance
    row, so no `--prune` is needed.
  - `excessive_nesting` still fires (inner `Arc::new(move || …)` at depth 5), and its scope only
    shrinks.
- `http_rig_build`: delete `let opener_keys = keys.clone();` and the `opener_keys,` argument
  (rustfmt may collapse the call).
  - Its `too_many_lines` expect goes from 167 to ~165 code lines (still over 100).
  - Its `let_underscore_must_use` expect shrinks.
- Both fns are **pinned** (§6).

**src/dst/tests/fixture_storage.rs**

- `open_engine_with_absorber(store, prefix)` and `open_engine_with_absorber_layout(store, prefix)`:
  drop the `hash`/`key` parameters.
- Delete `let keys = …;`, its 2-line comment and `keys.put(hash, key.clone(), hash);` (:132-135).
- `ShardEngine::start(…, store.clone(), …)` (:126) becomes `store`.
- `let handle = crate::history::Absorber::start(engine.clone(), cfg, absorb_rx);`

**src/dst/tests/history_absorption.rs**

- :50, :383, :394: drop `, hash, &key` from `open_engine_with_absorber`.
- :120-121, :207-208, :463-465: delete `let keys = …` and the `keys.put(…)` lines.
- :320-321: delete the `// NO keys.put…` comment and `let keys`.
- Drop the `store.clone(),` and `keys,` arguments at :125, :211, :322, :466.
- Doc of `v2_absorbs_without_customer_keys` (:283): change "so an absorber whose KeyCache is EMPTY
  must still absorb" to "so the absorber, which holds no key cache, must still absorb". Doc
  comments are outside the inventory hash.

**src/dst/tests/history_gather.rs, history_recovery.rs, reads_history.rs, reads_raw.rs, billing_maintenance.rs**

- Drop the `store(.clone())` and `Arc::new(crate::history::KeyCache::default())` arguments at
  every site in the §1.2 table.
- Where the dropped argument was a **move** (history_gather :169 `store`, :776 `store`), turn the
  preceding `open_engine_with_settings(store.clone(), …)` into `store`, so the diff does not leave
  a trailing clone.
- reads_history.rs:553: drop `, hash, &key`.

**src/dst/tests/runtime_isolation.rs**

- :30, :41: drop `, hash, &key`.

`Arc` stays used in every touched file (checked per file). No touched DST test crosses its
`too_many_lines` line: `repeated_keyed_reads_hit_the_postings_cache` goes 112 to ~110, and
`a_second_absorption_wave_trims_under_a_global_budget` goes 121 to ~119. All touched files except
history.rs are under 1,000 lines, and all shrink.

**Ledgers in commit D:** see §6.

## 5. Mutation analysis

The plan uses the `--in-diff` range CI will use. For a local push simulation of just these
commits, see §7.

- **src/history.rs, src/history/\*\*, src/dst/\*\***: none of these is under a critical prefix or
  registered in `mutation_owners.py`, so there is no mutation selection.
- **src/shard/task_lifecycle_tests.rs** (critical prefix `src/shard`, owner
  `task_lifecycle_tests`, filter `shard::`):
  - The file has no `#![cfg(test)]`, so the planner counts it as a production change and selects
    the owner.
  - `shard.rs:3163` declares it `#[cfg(test)] mod task_lifecycle_tests;`, which cargo-mutants
    skips, so discovery finds 0 mutants.
  - Expected driver line: `task_lifecycle_tests: no executable mutants in the selected scope`.
- **src/bootstrap.rs** (owner `bootstrap`, filter `bootstrap::`):
  - Added lines are the `data_store,` argument, the one-line `start_owned(...)` and the two reason
    attribute lines. The attributes sit outside every FnValue span.
  - No operator, comparison or match arm is added, so the only selected mutant is the `run`
    FnValue `replace run -> anyhow::Result<()> with Ok(())`. Its span covers the added body lines.
  - **Killed** by `bootstrap::tests::process_bootstrap_cannot_be_an_empty_success`. With
    `RUN_WAS_INVOKED` preset, the real `run` returns `Err("…starts process infrastructure once…")`
    and the test calls `unwrap_err()`. The mutant's `Ok(())` panics that `unwrap_err`.
  - No timeout risk: both paths return immediately.
  - Precedent: 71345c03 added a line inside `run` under the same owner.
- There are no equivalent mutants to restructure away.

## 6. Ledgers (commit D only; commit A has none)

- **docs/refactor/test-inventory.json**: run `python3 scripts/test-inventory.py --write`. Before
  `--write`, `--check` must list exactly these **27** entries as `…: changed function_sha256`:
  - billing_maintenance: `absorbed_boundary_and_maintenance_retire_atomically`
  - reads_history:
    - `sparse_key_reads_page_with_bounded_spans`
    - `corrupt_postings_fall_back_to_the_envelope`
    - `repeated_keyed_reads_hit_the_postings_cache`
    - `keyed_catch_up_after_a_cold_index_load_sees_later_absorbed_records`
  - history_gather:
    - `adaptive_gather_estimate_seeds_decays_and_jumps`
    - `sparse_absorption_wave_bounds_append_latency`
    - `v2_gather_packs_to_the_aggregate_budget`
    - `an_oversized_chunk_gathers_alone`
    - `keyed_frames_no_longer_count_twice_against_the_budget`
    - `untouched_streams_absorb_after_restart`
    - `one_corrupt_row_fails_only_its_stream`
  - history_recovery:
    - `a_second_absorption_wave_trims_under_a_global_budget`
    - `budget_deferred_streams_absorb_on_the_next_tick`
    - `a_large_record_absorbs_after_restart_under_default_policy`
    - `dirty_scan_retries_until_it_succeeds`
    - `sparse_records_rediscovered_after_restart_are_absorbed`
    - `pending_summary_clears_on_shard_close`
  - history_absorption:
    - `acked_records_survive_absorption_into_history`
    - `absorber_sweep_recovers_streams_whose_signals_were_lost`
    - `absorber_drains_records_larger_than_the_per_stream_gather_cap`
    - `v2_absorbs_without_customer_keys`
    - `v2_history_survives_engine_handoff`
    - `tiny_residuals_age_absorb_and_cannot_starve_the_progress_latch`
  - reads_raw:
    - `oversized_keyed_record_pages_through`
    - `long_keyed_run_pages_with_progress`
  - runtime_isolation: `a_fenced_owners_absorber_exits`

  No other changes (no names, attributes or scenarios). If a different set appears, stop.
- **docs/refactor/review-mechanisms.json**: 4 pins. Current values are checked equal to the tree
  today. Recompute with
  `inventory.functions(path.read_text(), path, include_helpers=True)` from
  `scripts/test-inventory.py`. The snippet in §7 does this.
  - `mechanisms[id=owned-active-absorber].support_functions[active_absorber_cancel].sha256`:
    currently 55db5d66…
  - `source_adaptations[active_absorber_cancel].after_sha256`: currently 55db5d66…. Extend its
    reason with: "Item 76 starts both absorbers without the store and key cache they never read;
    no assertion changed."
  - `fixture_changes[rig_opener].after_sha256`: currently 1d2bc61c…. Extend its reason with:
    "Item 76 drops the key cache the absorber never read (and the store start_owned no longer
    takes) from the opener's inputs; absorption settings and scheduling unchanged."
  - `fixture_changes[http_rig_build].after_sha256`: currently 27bf37b6…. Extend its reason with:
    "Item 76 stops handing the opener a key cache the absorber never read; the rig's own key cache
    and every value and assertion are unchanged."
  - `before_*` values and `before_commit` stay unchanged.
- **docs/quality/owners.json, docs/quality/source-allowances.json**: no change.
  - No new effect, global or macro sites.
  - The two deleted exceptions (`Absorber` `dead_code`, `rig_opener` `too_many_arguments`) are
    reasoned in-source exceptions without allowance rows, so nothing becomes stale.
  - The gate must print no `obsolete source allowances`. If it does, stop and inspect; do not
    blindly `--prune`.
- **docs/refactor/architecture-policy.json, test-scenario-map.json, scenario-dispositions.json,
  src/dst/tests/README.md, mutation_owners.py**: no change.
  - No test is renamed or deleted, and no file is added or moved.
  - The scenario map checks symbol existence only.
  - Found and left alone: the stale `owners.json` row `crate::read_history2_keyed_cached allow (dead_code)`
    names a `cfg_attr` that no longer exists in history.rs (§8).

## 7. Controls (exact commands, expected outputs)

Run from the repo root with the CI Python (3.11).

**C0 (before anything): the stale premise.**
`wc -l < src/history.rs; git show origin/slate:src/history.rs | wc -l` prints `1693` and `1713`.

**C1 (before D): deadness proof.** Remove only the `dead_code` expect on `Absorber`, then run
`cargo clippy --locked --workspace --all-targets 2>&1 | grep -n "never read"`.
- Expected: `fields `data_store` and `keys` are never read` at `src/history.rs:748` (after A the
  line is ~:739), once for the lib and once for the lib-test target.
- Revert.

**After A:**
- `cargo fmt --all -- --check` is clean.
- `cargo clippy --locked --workspace --all-targets -- -D warnings` is clean.
- `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items`
  is clean.
- `wc -l < src/history.rs` prints `1674` (±1 if rustfmt differs).
- `git show --stat HEAD` shows only src/history.rs.

**After D's code edits, before the reason re-decision and the ledgers:**

1. Run `cargo build --locked -p streams-quality-syntax && (cd scripts/quality && python3 -c 'import source_gate; p=source_gate.check(); print("\n".join(p) or "source gate OK")')`.
   Expected: only the NV3 lines, `accepted exception grew without a new decision:` for
   `crate::run`'s `expect_used` and `unwrap_used` contracts (`…:ordinary-call:crate::run:… 0 -> 1`).
   Apply the re-decided reasons and rerun. Expected: `source gate OK`.
2. `python3 scripts/test-inventory.py --check` lists exactly the 27 names in §6. Run `--write`,
   then `--check`. Expected: `test-inventory: OK (<N> tests, <k> ignored)`, where `N` equals the
   count before (no additions or removals).
3. `python3 scripts/review-evidence.py --check` prints exactly these 4 lines:
   - `mechanism support function changed or missing: src/history/controller_tests.rs::active_absorber_cancel`
   - `active_absorber_cancel: after fixture body changed or missing`
   - `rig_opener: after fixture body changed or missing`
   - `http_rig_build: after fixture body changed or missing`

   Recompute the pins with the snippet below. After the update, the check prints no failures.

   ```
   python3 - <<'EOF'
   import importlib.util; from pathlib import Path
   s=importlib.util.spec_from_file_location('i','scripts/test-inventory.py'); i=importlib.util.module_from_spec(s); s.loader.exec_module(i)
   for f,n in [('src/history/controller_tests.rs','active_absorber_cancel'),('src/dst/tests/fixture_http.rs','rig_opener'),('src/dst/tests/fixture_http.rs','http_rig_build')]:
       p=Path(f); print(f,n,[x['function_sha256'] for x in i.functions(p.read_text(),p,include_helpers=True) if x['name']==n])
   EOF
   ```
4. The whole local gate: `scripts/quality.sh` ends with no `QUALITY_FAIL`. Clippy with
   `-D warnings` is clean: no `unused import` for `ObjectStore`/`KeyCache`, no
   `unused variable: keys`, and no `unfulfilled_lint_expectations` (the `rig_opener`
   `too_many_arguments` expect is gone). The ratchet prints `quality ratchets: OK`.
   `architecture-gate --check`, `scenario-map-report --check`, `test-inventory --check` and
   `review-evidence --check` are all OK.
5. Run the pinning tests in §3:
   `cargo test --locked --lib -- history:: shard::task_lifecycle_tests:: bootstrap::tests:: dst_tests::history_absorption:: dst_tests::history_gather:: dst_tests::history_recovery:: dst_tests::reads_history:: dst_tests::reads_raw:: dst_tests::runtime_isolation:: dst_tests::billing_maintenance:: dst_tests::admission_maintenance::`
   Expected: all ok, 0 failed. Then run the full `cargo test --locked --release`.
6. Run NV1 and NV2 (§3): each is red with the quoted message, then green after revert.
7. Mutation leg for these commits alone. Use `<pre-A HEAD>`, which is 5d9d517f unless more
   commits land first:
   `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=<pre-A HEAD> scripts/quality/mutations.sh`.
   Expected:
   - `task_lifecycle_tests: no executable mutants in the selected scope`.
   - bootstrap: 1 mutant (`replace run -> anyhow::Result<()> with Ok(())`), caught, with 0 missed
     and 0 timeout.
   - Final line `Mutation verification executed 1 selected mutant(s) across 2 registered owner(s).`
   - To list without running:
     `cargo mutants --list --json --in-diff target/quality-mutations/pr.diff --file src/bootstrap.rs --package streams-slate`
     prints a single FnValue row for `run`.

## 8. Out of scope

Noted, not planned:

- Step B (budget types into `src/history/budget.rs`) and step C (wedge tests into
  `durability_frontier_tests.rs`) are not needed: history.rs ends at ~1,656 against a 1,713
  ceiling.
- `start_owned` could take `&Arc<ShardEngine>` and delete its `needless_pass_by_value` expect.
  Every call site moves in D anyway, so a follow-up is cheap. Its reason ("borrowing would ripple
  through boot's … wiring") is still literally true, so it is left alone.
- `open_engine_with_absorber` and `open_engine_with_absorber_layout` (fixture_storage.rs) are
  identical wrappers; `_layout` has one caller.
- More stale v1 documentation in the same neighbourhood:
  - the module doc of history.rs (:1-10), which still describes the v1 per-stream keyspace and
    block transformer
  - "the decoded slice cache (next PR)" in `postings_scan_opts_pub`'s doc (:50-53)
  - the v1 lanes and `open_dbs`/LRU comment in `worker.rs:227-242`
- `HistoryResources::paused` is undocumented. The deleted orphan called it an operator pause;
  `worker.rs:172` calls it a test hook.
- The stale `owners.json` row `src/history.rs crate::read_history2_keyed_cached allow (dead_code)`
  names a `cfg_attr` that no longer exists.
- Last-use `store.clone()` arguments that already exist in DST tests where the dropped absorber
  argument was itself a clone. They are not flagged, because clippy's `redundant_clone` does not
  see coroutine-saved locals, and they are not touched.

## 9. Decisions for Søren

- None at the product/raw edge.
- One reviewed ratchet decision, recorded in the commit message as with 71345c03: the two
  `bootstrap::run` exception reasons (`expect_used`, `unwrap_used`) are re-decided to add "and
  none in the shard opener". That is because the opener's call expressions, which those
  exceptions fingerprint, changed. The site counts (1 and 4) are unchanged.

## Skeptic corrections (C1..C7)

I checked every claim against the tree at 5d9d517f. These hold: the orphan and header sites (:35/:37/:83-87/:211-213/:752-759/:862), deadness (no reader of `data_store`/`keys` anywhere under `crate::history`, including cfg(test) children; the only `.keys` hit is `pending.keys()` in worker.rs:196), all 35 constructor sites, the 6 `open_engine_with_absorber` callers, the single `rig_opener` caller (fixture_http.rs:502, `opener_keys` at :409), the `ObjectStore` glob users (only `mod tests`, controller_tests.rs:16 and bounded_discovery_tests.rs:47; canonical_span/postings_validation_tests/test_support do not name it), the rig_opener `too_many_arguments` removal (clippy.toml threshold 5, 6 -> 5 args), the 27 test-inventory entries (inventory hashes only `#[test]` bodies under src/dst, with comments/docs stripped; no `configuration`/`mechanisms` field changes), the 4 review-evidence pins (the r09 mechanism test bodies do not change; no `include_str!` in the pinned files, so the path/no-path hash is the same), the NV3 fingerprint mechanics (scan.rs:242-252 hashes the whole normalized `ExprCall`; enclosing `Box::new`/`Box::pin`/`ShardEngine::start`/`Absorber::start_owned` in `crate::run` get new digests, `path` keys only shrink), the bootstrap mutant and its killer (bootstrap/tests.rs:53-54), the too_many_lines counts (121, 112, 171, ~176 code lines, all stay >100), and KeyCache staying live (product.rs:3588, application/*).

- **C1 (stale merge base: the ceiling has no slack now).** `git reflog origin/slate` shows `5d9d517f ... update by push`, so origin/slate == HEAD == merge base. The history.rs limit is now min(max(1000,2418), max(1000,1693)) = **1,693**, not 1,713. Fix these: §1.0 ("20 lines to spare" -> 0), C0's expected output (`1693` / `1693`), §8 ("~1,656 against a 1,713 ceiling" -> 1,693), and the header ("15 unpushed commits over 77c13d74"). Both commits still only shrink history.rs (A −19, D −18), so the plan stays valid. But the steps inside A must not be committed separately so that the header re-insert (+2 at :450) lands before the deletions. `QUALITY_BEFORE_SHA` stays 5d9d517f.
- **C2 (wrong provenance).** `absorb_pause_flag()` was deleted in **b612fbff** ("refactor(R10): own shared budgets caches…"), not 7975ca77 (`git log -S absorb_pause_flag -- src/history.rs`). This is narrative only; correct it if the commit message cites it.
- **C3 (NV1 is not deterministic).** `seed_from_dirty_index` (src/history/gather.rs:284-292) sets `since = now − threshold_age` for any stream not yet in `pending`. With `threshold_age = 3600s`, a stream the first discovery scan finds is due at once. worker.rs:82-84 runs that scan on the first tick (phase < 20 ms after start). If it lands between the first commit and the loop receiving that commit's signal, the stream absorbs and NV1 stays green. Use the pause hook instead, which is deterministic: after `ShardEngine::start` in the test, `engine.history_resources.paused.store(true, std::sync::atomic::Ordering::Relaxed);`. worker.rs:178 then skips classify and gather even for backdated entries. The expected red is still `keyless v2 absorption never advanced — the gather lane still depends on the customer key`, after about 400×25 ms. Revert afterwards.
- **C4 (NV2 does not build under -D warnings).** Replacing `Absorber::start_owned(...)` with `drop(absorb_rx);` leaves the inner `let absorber_cfg = absorber_cfg.clone();` (fixture_http.rs:270) unused, so rustc warns `unused variable`. That breaks the build if RUSTFLAGS carries `-D warnings`. Use `drop((absorb_rx, absorber_cfg));`. Expected red (admission_maintenance.rs:222-225) is unchanged.
- **C5 (C1 control wording).** Without `-D warnings` the note is `` `#[warn(dead_code)]` on by default ``, not `-D warnings`. Judge C1 by the grep, as the plan does, not by the fingerprint gate. The same message is already allowlisted in scripts/clippy-baseline-fingerprints.txt:27 (legacy line, stale today because the expect hides it; no ledger action).
- **C6 (the start_owned reason is not literally true).** §4 says the `needless_pass_by_value` reason ("…boot's mutation-gated wiring and four fixtures…", history.rs:848) "is still true". After D, `start_owned` is called by boot plus **three** fixtures: `active_absorber_cancel` (two calls), task_lifecycle_tests `fixture`, and `rig_opener`. This inaccuracy predates the plan and the plan does not touch the attribute, so leave it (changing it is a re-decision). Just do not claim it is true. The lint itself still fires, because `shard` is only borrowed.
- **C7 (CI cost, not a failure).** Touching src/bootstrap.rs matches BUFFER_PREFIXES (verification_plan.py:29, :97), so the push also schedules the **miri** leg besides mutation. Budget for it. Also: NV3 lists 8 lines (4 call sites × 2 lints). If the implementer keeps `data_store.clone()` in the per-open `ShardEngine::start`, it lists 6. Either is fine; the re-decided reasons clear both.

Missed ledgers: none. test-inventory (27), review-mechanisms (4 pins), owners/source-allowances (no rows for the two deleted reasoned exceptions; statics' `AtomicU64` global rows keyed on the type as written, unchanged), architecture-policy (no bootstrap/history budget exception), and the scenario map/dispositions/README/test-relocations (no rename or deletion; relocations carry no hashes) are all handled or correctly left alone.

**Verdict: ready-with-corrections.** Apply C1 (numbers), C3 (NV1 via the pause hook) and C4 (NV2 buildable) before running. The rest is wording.
