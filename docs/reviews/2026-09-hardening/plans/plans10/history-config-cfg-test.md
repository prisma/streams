# Item 42: `HistoryConfig::default()` forks on `cfg!(test)`, and no test pins the shipped absorber defaults

Tree: `slate` @ `2fb92fb9`, 5 commits ahead of `origin/slate` = `729c52ac`, which is also the merge base. None of the files this plan touches has changed since the merge base: `git diff --stat 729c52ac HEAD -- src/config src/runtime.rs src/history.rs` is empty. I read everything on that tree and ran nothing, because a mutation run owns the tree.

**Summary.**
- The whole change is one commit touching three files:
  - `src/config/model.rs` loses the fork and its stale docs.
  - `src/config/tests.rs` pins the shipped values, plus one new test for the floored capacity the runtime actually builds.
  - `src/history.rs` gets a doc-only 2-for-2 line fix, because its doc still says the budget is a process-level static.
- What it does not change: no ceilinged file grows, no ratcheted exception scope is touched, no wire, metric or JSON value changes in any shipped build, no mutants are selected, and no ledger changes.
- **The reviewer's Change is correct and buildable, with two refinements:**
  - (a) The lever a test uses to ask for more headroom is `ShardConfig.history`, `ShardConfig.shared_history`, or for rigs `HttpRigOptions.shard.shared_history`. It is not `RuntimeCaps::with_config`: rigs build their runtime from `fixture_config` and have no `HistoryConfig` option.
  - (b) I traced every consumer and found no test that needs more than the shipped posture, so no test gets an opt-in. The full gate, including the capacity leg, has the final say.

---

## 1. Problem (verified)

Where the reviewer's line numbers are now:

| Reviewer cite | Current location |
| --- | --- |
| `model.rs:376-393` | `model.rs:386-404` (Default impl) and `:132-139` (field docs) |
| `model.rs:383-388` | `:393-398` |
| `tests.rs:126-127` | exact |

### 1.1 The fork (`src/config/model.rs:386-404`)

```rust
386 impl Default for HistoryConfig {
387     fn default() -> Self {
388         Self {
389             absorb_pause_initial: false,
390             // The test/profile split is preserved from the old
391             // history.rs OnceLock: tests get headroom, production
392             // gets the field-validated 64 MiB / 2 gathers posture.
393             absorb_global_budget_bytes: if cfg!(test) {
394                 4 * 1024 * 1024 * 1024
395             } else {
396                 64 * 1024 * 1024
397             },
398             absorb_global_gathers: if cfg!(test) { 64 } else { 2 },
399             cache_bytes: 32 * 1024 * 1024,
400             compactor_off: false,
401             gc_interval: Some(Duration::from_secs(600)),
402         }
403     }
404 }
```

These are the only two `cfg!(test)` expressions in `src`: `git grep -n 'cfg!(test)' -- src` returns just `model.rs:393` and `:398`.

### 1.2 The field docs repeat the fork and name a test-only helper (`model.rs:132-139`)

```rust
132     /// ABSORB_GLOBAL_BUDGET_BYTES. Defaults: 4 GiB under cfg(test),
133     /// 64 MiB otherwise (preserved exactly, including the test split);
134     /// `floored_budget_capacity()` still raises it to the worst-frame
135     /// floor at the use site.
136     pub absorb_global_budget_bytes: usize,
137     /// ABSORB_GLOBAL_GATHERS, max(1). Defaults: 64 under cfg(test),
138     /// 2 otherwise.
139     pub absorb_global_gathers: usize,
```

- `floored_budget_capacity` exists only under `#[cfg(test)]` (`history.rs:281-284`).
- The real floor is `HistoryResources::with_body_limit`, `history.rs:240-244`:

```rust
240         let worst_frame_transient = worst_frame_transient_for(body_limit);
241         let capacity = cfg
242             .absorb_global_budget_bytes
243             .max(worst_frame_transient)
244             .min(u32::MAX as usize);
```

### 1.3 The only pin pins the *test* value (`src/config/tests.rs:126-127`, in `default_values_are_pinned`)

```rust
126     assert_eq!(c.history.absorb_global_budget_bytes, 4 * 1024 * 1024 * 1024); // cfg(test)
127     assert_eq!(c.history.absorb_global_gathers, 64); // cfg(test)
```

- `cfg(test)` is always true in the library's test binary. So the shipped branch (64 MiB / 2) is observed by no test at all.
- The one integration test (`tests/pilot_membership.rs`) cannot reach the `pub(crate)` config (`lib.rs:29` `mod config;`).
- Other tests that set these fields always set them explicitly, so none of them pins a default:
  - `runtime.rs:421-422,464`
  - `history/controller_tests.rs:24-25`
  - `dst/tests/history_gather.rs:745-746`
- `worst_frame_floor_serializes_oversized_gathers_without_starvation` (`history.rs:1437-1440`) uses a literal 64 MiB in `floored_budget_capacity(64 * 1024 * 1024)`, but that literal is not tied to `HistoryConfig::default()`.

### 1.4 The headroom was for a process-global budget that no longer exists

The fork comes from the old `absorb_budget()` OnceLock (`git show 6bcaff69^:src/history.rs`, lines 267-284):

```rust
pub fn absorb_budget() -> &'static AbsorbBudget {
    static B: std::sync::OnceLock<AbsorbBudget> = std::sync::OnceLock::new();
    B.get_or_init(|| {
        // The test binary runs MANY independent DST engines in one
        // process; a production-sized global budget would serialize
        // their gathers ACROSS TESTS and turn timing-sensitive
        // scenarios flaky. ...
        #[cfg(test)]
        let default_bytes: usize = 4 * 1024 * 1024 * 1024;
        #[cfg(not(test))]
        let default_bytes: usize = 64 * 1024 * 1024;
```

`b612fbff` ("own shared budgets caches scaler time and readiness per runtime") replaced that static with `HistoryResources`, which is built per owner:

- **Production runtime:** `runtime.rs:185-189` builds it via `RuntimeCaps::with_config`. The call sites are `bootstrap.rs:172-173` and, for every HTTP rig, `fixture_http.rs:415-416`. `bootstrap.rs:448,530` hands that one pool to every engine the runtime opens.
- **Default runtime:** `runtime.rs:239-242`, in `RuntimeCaps::with_sources`.
- **Standalone engine:** `shard.rs:1451-1456`, when `ShardConfig.shared_history` is `None`, which is the default at `shard.rs:1081`. Its config comes from `shard.rs:1088`, `history: HistoryConfig::default()`.

So no budget is shared across tests any more, and the cross-test serialization that motivated the fork cannot happen.

One doc still states the old premise, `history.rs:151-152`:

```rust
151 /// Budgets are process-wide BY CONSTRUCTION: the semaphores live in one
152 /// process-level static, not per absorber.
```

### 1.5 What the fork costs: the values each build actually runs

This table uses the runtime composition that bootstrap and every rig use, with packing = `absorb_gather_max_bytes` = 32 MiB and body pin = 32 MiB. Worst frame = (33,554,432 + 65,536) x 3 = **100,859,904**.

| | test binary today | shipped binary (and test binary after the fix) |
| --- | --- | --- |
| `absorb_global_budget_bytes` | 4,294,967,296 | 67,108,864 |
| `budget.capacity()` | 4,294,967,295 (clamped to `u32::MAX`) | 100,859,904 (floored) |
| `gather_slots()` | 64 | 2 |
| `per_gather_reservation_bytes()` | 100,859,904 | 100,859,904 |
| `effective_gather_concurrency()` | min(64, 42) = 42 | min(2, 1) = 1 |
| `packing_bytes` | 33,554,432 | 33,554,432 |

- `deploy/profiles/compute-1g.env:27-28` sets `ABSORB_GLOBAL_BUDGET_BYTES=100859904` and `ABSORB_GLOBAL_GATHERS=1`. `RUNBOOK.md:725-731` says "the binary FLOORS this".
- `RUNBOOK.md:129-130` documents the defaults as 67108864 and 2.
- No test proves the floor value or the effective concurrency that the shipped defaults produce.

### 1.6 Complete use-site list

**Readers and writers of the two fields** (`git grep`):

| Site | Role |
| --- | --- |
| `config/model.rs:136,139` | the fields |
| `config/model.rs:393-398` | the default (the fork) |
| `config/load.rs:98-103` | env overlay (`ABSORB_GLOBAL_GATHERS` is `.max(1)`) |
| `config/summary.rs:38-39` | redacted summary, logged once at `bootstrap.rs:210` |
| `history.rs:241-246` | floor + clamp, `AbsorbBudget::new(capacity, gathers)` |
| `runtime.rs:421-422,464` | tests, explicit values |
| `history/controller_tests.rs:24-25` | test, explicit |
| `dst/tests/history_gather.rs:745-746` | test (`shared_pool`), explicit |
| `config/tests.rs:126-127` | the pin of the test value |

**Consumers of `HistoryConfig::default()`:**

- `model.rs:337`, in `with_knob_defaults`, which feeds `ServerConfig::load`. That has 32 call sites: bootstrap, `fixture_config` for every HTTP rig (`fixture_http.rs:337`), and the config, runtime, usage, telemetry and store tests.
- `runtime.rs:239-242`, in `with_sources`.
- `shard.rs:1088`, in `ShardConfig::default`. There are 47 `ShardConfig` default or struct-update uses in `src/dst`, and each builds a per-engine pool at `shard.rs:1451-1456`.
- `history.rs:1392`, `reported_concurrency_matches_what_the_budget_admits`.
- `controller_tests.rs:23-27` and `history_gather.rs:744-748` reach it via `..Default::default()`, but both override the two fields.

**Downstream readers of the pool:**

- Reserve and seed: `history/worker.rs:334-335`, `history/gather.rs:285-297`, the seed at `history.rs:809-812`, and the estimate at `history.rs:832-844`.
- Admission: `application/append.rs:124`.
- Bootstrap: packing clamp at `bootstrap.rs:400-416`; startup line and `resolvedMemoryConfig` at `bootstrap.rs:830-875`.
- Debug JSON: `/v1/debug` budget at `http.rs:1539-1557`, and `ops.rs:431,435`.
- No test reads `capacityBytes`, `gatherSlots`, `effectiveGatherConcurrency`, `perGatherReservationBytes` or `absorbBudgetBytes`. A `git grep` over `src` finds only the emitters.

---

## 2. Contract decision

**Typed contract.** `HistoryConfig::default()` is the one shipped posture, 64 MiB and 2 gathers, in every build. If a test needs headroom, it states it as configuration. A build flag never forks the default. The levers already exist:

- **Engine-level test:** `ShardConfig { history: HistoryConfig { absorb_global_budget_bytes, absorb_global_gathers, ..Default::default() }, .. }`. This gives a per-engine pool, `shard.rs:1451-1456`.
- **HTTP rig:** `HttpRigOptions { shard: ShardConfig { shared_history: Some(pool), .. }, .. }`. `fixture_http.rs:428-430` swaps the rig runtime's pool for `pool`.
- **Unit test:** `HistoryResources::with_body_limit(&HistoryConfig { .. }, ..)`, as `history_gather.rs:742-752` and `controller_tests.rs:22-27` already do.

Any such opt-in carries a comment saying why the test is not about the absorber posture. This plan adds none (see below).

Two things this plan deliberately does not add: a `HistoryConfig` parameter on `RuntimeCaps::with_sources` (the reviewer agrees), and a new `HttpRigOptions` field.

**Wire, metric and JSON changes: none.**

- `cfg!(test)` expands to `false` in every non-test build. `if false { A } else { B }` is `B`, and the literal that replaces it is the same `B`.
- So in the shipped binary, `HistoryConfig::default()` and everything derived from it keeps exactly the same values:
  - the redacted startup summary (`summary.rs:38-39`)
  - the startup memory line and `resolvedMemoryConfig` (`bootstrap.rs:830-875`)
  - `/v1/debug` `budget.*` (`http.rs:1539-1557`)
  - `/v1/debug/load` (`ops.rs:431,435`)
- Only test-binary values move, and no test reads those keys (§1.6).

**Alternatives rejected:**

- *Keep the fork and add a `cfg(not(test))` pin.* It can never run: the library test binary always has `cfg(test)`.
- *Named `DEFAULT_ABSORB_*` constants.* There is no second reader, and the neighbouring defaults are literals (`cache_bytes: 32 * 1024 * 1024`).

### What changes inside the test binary

This change of test posture is the point of the item.

**Pools now run the shipped values.** Per-engine pools (`ShardConfig::default`) and per-rig pools now hold 100,859,904 bytes with 2 slots.

**Engine-level tests and single-shard rigs: nothing observable.**
- Each pool has at most one absorber pump, and the pump gathers serially (`history/worker.rs:310-336`).
- A lone reservation is never refused. `try_grow` clamps `additional` to `capacity - held` and returns `true` when that is 0 (`history.rs:399-403`).
- The per-gather reservation is identical in both postures. `est` is at most `max(gather_max_bytes x 3, worst frame)`, which is at most 100,859,904 for every `AbsorberConfig` in the tree (`history.rs:832-844`).

**Multi-shard rigs whose absorbers run now serialize gathers across their engines.** Affected:
- `http_rig_owner` and `http_rig_owner_at`: 4 prefixes, used by `livefeed_ownership`, `livefeed_history`, `livefeed_engine_retired` and `read_application`.
- `http_rig_opts` and `http_rig_full` in `topology_scaling` and `watch_observation`.

How the serialization plays out:
- Each absorber's first gathers reserve the whole floored pool: seed = worst frame, `history.rs:809-812`.
- The estimate decays by 1/8 per gather (`history.rs:851-857`). Two gathers fit only after about six gathers per absorber.
- This is the production behaviour.
- Idle absorbers reserve nothing (`worker.rs:316`, `if !v2_lane.is_empty()`).

**Paused rigs are unchanged:** `http_rig_cold_absorb` and `backlog_rig` (`admission_maintenance.rs:190-194`).

**Admission is unchanged in rigs.** Rigs set `rss_shed_mb: 0` (`fixture_http.rs:503`), so reserved bytes never shed there. In any case, reserved bytes are at most `capacity`, which can only fall.

**Hazards I checked. None applies:**

1. **Failpoints inside a gather.** `src/history*` has no failpoints and no `Fp::` references.
2. **A held store parking a gather that holds the pool while another engine of the same pool must absorb.** None of the `FaultStore::hold_class` users has that shape:
   - `persistence_faults.rs:187` uses `open_engine`, so each engine has its own pool.
   - `request_topology_debt.rs:74` and `runtime_request_work.rs:70,130,210,278` use single-prefix rigs.
   - `billing_controller.rs:73-74,492,554` hold separate billing stores.
3. **Tests that assert concurrent gathers.** They all build explicit pools: `history_gather.rs:795-852` and `:868-906`, `controller_tests.rs:22-27`, and the `AbsorbBudget::new` unit tests at `history.rs:1257-1380`.
4. **Signals dropped while an absorber waits in `reserve()`.** The channel holds 65,536 (`history.rs:1105-1107`).
5. **Tests reading the budget JSON.** There are none (§1.6).

**Conclusion:** no test is known to need more than the shipped posture, so the plan adds no opt-in. The fallback in §7 C9 covers the case where the gate finds one.

---

## 3. Red tests

Both live in `src/config/tests.rs`. They run under the lib test binary as `config::tests::*`. The file already has `use super::*;` (`tests.rs:12`), so no new glob is added.

### R1: `config::tests::default_values_are_pinned` (edited)

Replace lines 126-127 with:

```rust
    assert_eq!(c.history.absorb_global_budget_bytes, 64 * 1024 * 1024);
    assert_eq!(c.history.absorb_global_gathers, 2);
```

Traced on the current tree:
- `load_with(&[])` → `ServerConfig::load(CliArgs::deterministic(), empty)` → `with_knob_defaults` → `HistoryConfig::default()`.
- Under `cfg(test)` that gives `absorb_global_budget_bytes = 4 * 1024^3 = 4294967296`.
- No env overlay applies, because `load.rs:98` finds no key.
- The `assert_eq!` starts at column 5 of line 126.

Expected red:

```
thread 'config::tests::default_values_are_pinned' (<tid>) panicked at src/config/tests.rs:126:5:
assertion `left == right` failed
  left: 4294967296
 right: 67108864
```

### R2: `config::tests::shipped_absorber_budget_floors_to_one_worst_frame_gather` (new)

Insert it after `default_values_are_pinned`: after line 188 `}` and blank line 189. It becomes lines 190-210, followed by a blank line. The existing `#[test] fn env_overlay_applies_with_legacy_parse_semantics` moves from 190 to 212.

```rust
/// The shipped absorber posture, built the way bootstrap builds it
/// (`RuntimeCaps::production(..).with_config`). The 64 MiB default is
/// below one worst-frame build, so the budget floors to exactly that
/// build, (32 MiB + 64 KiB) x3, the value deploy/profiles/compute-1g.env
/// pins, and admits one worst-case gather at a time.
#[test]
fn shipped_absorber_budget_floors_to_one_worst_frame_gather() {
    let c = load_with(&[]);
    let caps = crate::runtime::RuntimeCaps::production("absorber-defaults").with_config(&c);
    let history = &caps.history;
    assert_eq!(
        history.budget.capacity(),
        100_859_904,
        "the 64 MiB default must floor to one worst-frame build"
    );
    assert_eq!(history.worst_frame_transient, 100_859_904);
    assert_eq!(history.budget.gather_slots(), 2);
    assert_eq!(history.packing_bytes, 32 * 1024 * 1024);
    assert_eq!(history.per_gather_reservation_bytes(), 100_859_904);
    assert_eq!(history.effective_gather_concurrency(), 1);
}
```

**Line numbers.** The multi-line `assert_eq!` begins at line 200. Line 198 is 92 columns, under rustfmt's 100, so it stays on one line. The three-argument `assert_eq!` exceeds 100 columns and rustfmt breaks it as shown.

**Accessibility.**
- `RuntimeCaps::production` and `with_config` are `pub(crate)` (`runtime.rs:174,199`).
- `RuntimeCaps.history`, `HistoryResources.budget`, `.worst_frame_transient` and `.packing_bytes` are `pub` fields.
- `capacity`, `gather_slots`, `per_gather_reservation_bytes` and `effective_gather_concurrency` are `pub(crate)`.

**No Tokio runtime is needed.** `runtime::tests::different_runtime_body_limits_size_independent_history_floors` (`runtime.rs:455-478`) already calls `RuntimeCaps::production(..).with_config(..)` from a plain `#[test]`.

**Traced on the current tree:**
- `with_config` → `with_body_limit(&c.history, 32 MiB, 32 MiB)`.
- worst = (33,554,432 + 65,536) x 3 = 100,859,904.
- capacity = max(4,294,967,296, 100,859,904) = 4,294,967,296, then `.min(u32::MAX as usize)` = 4,294,967,295. `AbsorbBudget::new` clamps the same value to [1, u32::MAX], so it is unchanged.

Expected red:

```
thread 'config::tests::shipped_absorber_budget_floors_to_one_worst_frame_gather' (<tid>) panicked at src/config/tests.rs:200:5:
assertion `left == right` failed: the 64 MiB default must floor to one worst-frame build
  left: 4294967295
 right: 100859904
```

If the first assertion were skipped, the later ones would also fail on today's tree: slots 64 vs 2, effective 42 vs 1.

**Red leg summary.** Apply only the `tests.rs` hunk and run `cargo test --locked --release --lib config::tests::`. Expected `failures:` list, in either order:

```
    config::tests::default_values_are_pinned
    config::tests::shipped_absorber_budget_floors_to_one_worst_frame_gather
```

and `test result: FAILED. 15 passed; 2 failed`. The filter `config::tests::` matches exactly the 16 existing tests of `src/config/tests.rs` plus R2. `config::validation::validation_tests::config_validation_tests::`, `config::numeric_tests::` and `config::certification_tests::` do not contain the substring.

**Green after the fix.**
- R1: the default is 67,108,864 and 2.
- R2: capacity = max(67,108,864, 100,859,904) = 100,859,904; slots = 2; packing = min(33,554,432, 33,619,968) = 33,554,432; per-gather = max(100,663,296, 100,859,904) = 100,859,904; effective = min(2, max(1, 1)) = 1.

### Hand-mutation controls

cargo-mutants selects nothing here (§5), so these show that R1 and R2 are not vacuous:
- **M1: re-add the fork.** R1 and R2 go red exactly as above. That is the red itself.
- **M2: delete `.max(worst_frame_transient)` at `history.rs:243`.** R2 goes red at 200:5 with `left: 67108864`, `right: 100859904`. This proves R2 pins the floor, not just the configured value.
- **M3: set `absorb_global_gathers: 3` in the default.** R1 goes red at 127:5 with `left: 3`, `right: 2`. R2 goes red at 206:5 with `left: 3`, `right: 2`.

### Pinning tests that must stay green, traced under the new default

- **`history::tests::reported_concurrency_matches_what_the_budget_admits`** (`history.rs:1391`):
  - `HistoryResources::new(&default, usize::MAX)` gives capacity 100,859,904.
  - packing = min(usize::MAX, 33,619,968) = 33,619,968.
  - per = 100,859,904 and reported = min(2, 1) = 1.
  - Every assertion holds: `per <= cap`, `1 >= 1`, `1 <= 2`, `per*1 <= cap`. The rest are constants.
  - Today it also reports 1, with cap 4,294,967,295 and per 4,294,967,295.
- **`history::tests::worst_frame_floor_serializes_oversized_gathers_without_starvation`**: independent of the default.
- **`config::validation::validation_tests::config_validation_tests::body_ceiling_sizes_the_absorber_reservation_and_only_lowers`** (`validation_tests.rs:400-435`): uses `floored_budget_capacity(0)`, which is independent of the default.
- **`runtime::tests::configured_resources_are_shared_only_within_their_runtime_and_release`** and **`different_runtime_body_limits_size_independent_history_floors`**: both set explicit values.
- **`dst::dst_tests::runtime_journals::r10_runtime_body_limits_apply_to_each_collector_and_preflight`**: asserts only `worst_frame_transient`.
- **`dst::dst_tests::history_gather::*`**: the adaptive estimate depends on `worst_frame_transient` and `gather_max_bytes`, not on capacity. Each test gathers alone on its own pool, or on an explicit pool.
- **The multi-shard rig modules in §7 C4**: production posture, per the §2 hazard analysis.

---

## 4. Edits, file by file, in commit order

**One commit.** Suggested message:

```
The absorber budget default is the shipped 64 MiB and two gathers in every build

HistoryConfig::default() forked on cfg!(test): 4 GiB / 64 gathers in the
test binary, 64 MiB / 2 in the shipped one. The headroom was for the old
process-global OnceLock budget; budgets are per runtime now, so tests run
the shipped posture and the floored capacity it produces is pinned.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
```

**Line budgets.** Current `wc -l` of the ceilinged files: `http.rs` 3,362; `product.rs` 4,205; `shard.rs` 3,196; `billing.rs` 2,201; `history.rs` 1,713; `auth.rs` 1,676; `registry.rs` 1,492; `sse/feed.rs` 1,170; `fleet.rs` 1,143. Only `history.rs` is touched, at net 0.

| File | Before | After | Ceilinged? |
| --- | --- | --- | --- |
| `src/config/model.rs` | 514 | 508 | no |
| `src/config/tests.rs` | 601 | 622 | no |
| `src/history.rs` | 1,713 | 1,713 | yes (budget: +0; hunk is 2-for-2) |

### 4.1 `src/config/model.rs`

Field docs, lines 132-139 (8 lines → 6):

```rust
    /// ABSORB_GLOBAL_BUDGET_BYTES, default 64 MiB. The runtime floors it
    /// at one worst-frame build (`HistoryResources::with_body_limit`), so
    /// the default runs at 100,859,904 bytes under the 32 MiB body pin.
    pub absorb_global_budget_bytes: usize,
    /// ABSORB_GLOBAL_GATHERS, max(1), default 2.
    pub absorb_global_gathers: usize,
```

Default body, lines 390-398 (9 lines → 5):

```rust
            // The field-validated posture in every build. Budgets are per
            // runtime, so a test that needs more headroom states it in its
            // own HistoryConfig; the default never forks on the build.
            absorb_global_budget_bytes: 64 * 1024 * 1024,
            absorb_global_gathers: 2,
```

Rustdoc safety: `HistoryResources::with_body_limit` is plain code in backticks, not an intra-doc link, because it names a `pub(crate)` item from a `pub` struct. There are no `[..]` or `<..>`.

### 4.2 `src/config/tests.rs`

- Lines 126-127 as in R1: same line count, and the `// cfg(test)` remarks go.
- Insert R2 after line 189.
- `default_values_are_pinned` stays 83 lines (106-188). R2 is 15 lines with nesting 1. The asserts are under an exact `#[cfg(test)] mod tests` (`config/mod.rs:32-33`), so clippy is fine.

### 4.3 `src/history.rs` (doc only)

Lines 151-152 (2 → 2):

```rust
/// Budgets are per runtime: the semaphores live in the `HistoryResources`
/// that `RuntimeCaps` owns and bootstrap hands to every engine it opens.
```

This is a doc comment on `pub(crate) struct AbsorbBudget` (`history.rs:153`), outside any function body.

### Ratcheted exception scopes touched: none

- `model.rs` and `config/tests.rs` contain no `#[expect]` or `#[allow]`.
- `history.rs` has no inner attribute, and `AbsorbBudget` carries no `#[expect]`. The nearest one sits on `AbsorbBudget::reserve` (`history.rs:338-341`) and is untouched.
- `cfg!` is in `EXPRESSION_MACROS` (`scripts/quality/source_rules.py:15-19`), so `classify()` returns `None` for it. Removing it vacates no `macro-dsl` or `source-allowances.json` row.
- R2's paths (`crate::runtime::RuntimeCaps::production`) are not effect paths: `classify()` flags only `std::env::var`, `std::thread::`, `tokio::spawn` and `std::mem::forget`.
- The architecture gate's reverse edges count only `http` and `product`. A config test that names `crate::runtime` adds none.
- No `mt_lint` markers apply: no `name: String` parameter and no `.stream_ref(`.

---

## 5. Mutation analysis

**Changed Rust files:** `src/config/model.rs`, `src/config/tests.rs`, `src/history.rs`.

- **Not critical.** None is under `CRITICAL_PREFIXES` (`scripts/quality/verification_plan.py:22-31`). `src/config/` is not a critical prefix, and `src/history.rs` matches none of the prefixes.
- **Not registered.** None is in `mutation_owners.OWNERS`. The only `src/config` row is `admission_limits` (`mutation_owners.py:69`), and no row names `src/history.rs`.
- **Planner verdict.** `mutation_source_files` is `[]` and `"mutants": false`. `loom`, `miri` and `properties_fuzz` are `false`, and `compiler` is `true`.
- **No mutants are generated.** cargo-mutants never sees these files, so there is no FnValue mutant for `HistoryConfig::default` and none from the comment inside its body. This matters because a comment inside a function body would otherwise select that function's whole-body mutant. The `history.rs` hunk is a doc comment outside any body.
- **No owner rows or filters change.**
- The hand controls M1-M3 (§3) stand in for the experiment the planner will not run.
- An owner row for `src/config/model.rs` is not warranted. It is not critical, and its obligation is value pins, which R1 and R2 now carry.

---

## 6. Ledgers

| Ledger | Change | Why |
| --- | --- | --- |
| `docs/refactor/test-inventory.json` | none | covers `src/dst` only (`scripts/test-inventory.py:138`); no DST test changes |
| `docs/refactor/review-mechanisms.json` | none | pins DST tests only; none touched |
| `docs/quality/owners.json` | none | no global static, macro-dsl, glob, effect or by-path fact added or moved |
| `docs/quality/source-allowances.json` | none | `cfg!` is an expression macro, not an inventoried fact; the `unresolved-glob` row for `src/config/tests.rs` (count 1, `use super::*`) is unchanged |
| `docs/refactor/architecture-policy.json` | none | no reverse edge or budget exception; `history.rs` stays 1,713 |
| `docs/refactor/WIRE-MATRIX.md` | none | no wire change (§2) |
| scenario map / dispositions | none | no rename; `default_values_are_pinned` is in no ledger (`grep -rn default_values_are_pinned docs scripts` finds nothing) |
| `src/dst/tests/README.md` | none | no new DST module |
| `scripts/quality/mutation_owners.py` | none | §5 |
| `RUNBOOK.md` | none | it already documents the shipped defaults (67108864, 2) at `:129-130` |

---

## 7. Controls

Run these after the in-tree mutation run finishes, and never concurrently with the gate. Put a Python ≥ 3.11 first on `PATH` (the scratchpad `pybin/python3` points to 3.12).

- **C1 (red, before the fix).** Apply only the `src/config/tests.rs` hunk (§4.2), then run `cargo test --locked --release --lib config::tests::`. Expected: the two panics quoted in §3 R1 and R2, then `test result: FAILED. 15 passed; 2 failed`.

- **C2 (green).** With the full commit, run:

  ```
  scripts/test-leg.sh target/legs/item42-config.log --min 17 --exact config::tests::default_values_are_pinned --exact config::tests::shipped_absorber_budget_floors_to_one_worst_frame_gather -- --locked --release --lib config::tests::
  ```

  Expected: `test result: ok. 17 passed; 0 failed`, and `TESTS_RAN_OK: target/legs/item42-config.log: floor 17, exact 2`.

- **C3 (neighbours pinned by the old posture).** Run:

  ```
  scripts/test-leg.sh target/legs/item42-neighbours.log --min 5 --exact history::tests::reported_concurrency_matches_what_the_budget_admits --exact history::tests::worst_frame_floor_serializes_oversized_gathers_without_starvation --exact runtime::tests::configured_resources_are_shared_only_within_their_runtime_and_release --exact runtime::tests::different_runtime_body_limits_size_independent_history_floors --exact dst::dst_tests::runtime_journals::r10_runtime_body_limits_apply_to_each_collector_and_preflight -- --locked --release --lib -- reported_concurrency_matches_what_the_budget_admits worst_frame_floor_serializes_oversized_gathers_without_starvation configured_resources_are_shared_only_within_their_runtime_and_release different_runtime_body_limits_size_independent_history_floors r10_runtime_body_limits_apply_to_each_collector_and_preflight
  ```

  Expected: `test result: ok. 5 passed`, and `TESTS_RAN_OK ... exact 5`.

- **C4 (multi-shard rigs at the shipped posture, three repetitions to surface timing drift).** Run:

  ```
  for i in 1 2 3; do scripts/test-leg.sh target/legs/item42-multishard-$i.log --min 44 -- --locked --release --lib -- dst_tests::livefeed_ownership:: dst_tests::livefeed_history:: dst_tests::livefeed_engine_retired:: dst_tests::read_application:: dst_tests::topology_scaling:: dst_tests::watch_observation:: --skip post_split_throughput_scales; done
  ```

  Expected each time: `test result: ok. 44 passed; 0 failed; ... 1 filtered out` or more filtered. The 44 is 8 + 11 + 3 + 6 + (9 − 1) + 8 test attributes in those files.

- **C5 (absorber modules).** Run:

  ```
  scripts/test-leg.sh target/legs/item42-absorber.log --min 12 -- --locked --release --lib -- dst_tests::history_gather:: history::controller_tests::
  ```

  Expected: `test result: ok. 12 passed` (10 + 2).

- **C6 (hand mutations, §3 M2 and M3).** After committing:
  - M2: delete `history.rs:243` `.max(worst_frame_transient)` and run `cargo test --locked --release --lib shipped_absorber_budget_floors_to_one_worst_frame_gather`. Expected: red at `src/config/tests.rs:200:5` with `left: 67108864`, `right: 100859904`. Then `git checkout -- src/history.rs`.
  - M3: set `absorb_global_gathers: 3` in `model.rs`, then run R1 and R2. Expected red at `tests.rs:127:5` and `tests.rs:206:5`, both `left: 3`, `right: 2`. Then `git checkout -- src/config/model.rs`.
  - Afterwards, `git status --short` is empty.

- **C7 (greps).** Each of these prints nothing:
  - `git grep -n 'cfg!(test)' -- src`
  - `git grep -n 'floored_budget_capacity()' -- src/config`
  - `git grep -n 'process-level static' -- src`

  `wc -l src/config/model.rs src/config/tests.rs src/history.rs` prints `508`, `622` and `1713`.

- **C8 (static gates).** `scripts/quality.sh` ends with `QUALITY_OK`. It covers `cargo fmt --all -- --check`, `cargo clippy --locked --workspace --all-targets ... -D warnings`, `RUSTDOCFLAGS='-D warnings' cargo doc --document-private-items`, the architecture, source, multitenancy and inventory gates, and the mt-lint leg. Also:
  - `python3 scripts/architecture-gate.py` passes, with `history.rs` at 1,713.
  - `python3 scripts/test-inventory.py --check` reports OK, unchanged.

- **C9 (full gate).** Run `OUT=<scratchpad>/gate-item42.txt scripts/gate.sh`. Expected:
  - The output ends in `GATEDONE`.
  - The suite line reads `test result: ok. N+1 passed; 0 failed; ... 1 filtered out`, where N is the count at the parent commit. At `2fb92fb9` the lib holds 1,203 tests: the mt-lint leg shows 1 passed plus 1,202 filtered. So N = 1,202 and the expected line is 1,203 passed.
  - `TESTS_RAN_OK` against the inventory floor.
  - The capacity leg `dst::dst_tests::topology_scaling::post_split_throughput_scales` shows `test result: ok. 1 passed`.
  - The capacity leg is the one multi-shard rig whose absorbers run under store latency. Before the split, only one engine has debt. After it, the two children's gathers serialize on the rig pool. Admission does not depend on absorption lag at these volumes (256 MiB per shard, 900 s), so the ≥ 1.8x append-throughput ratio should not move.
  - **Fallback if a multi-shard test or the capacity leg fails at the shipped posture:** treat it first as a finding, since production runs this posture. Only if the test's subject is not the absorber, give that test explicit headroom through a §2 lever with a reason comment. In the same commit, run `python3 scripts/test-inventory.py --write`, check that the diff is only that test's hash, update any `review-mechanisms.json` pin, and re-check the DST file's 1,000-line ceiling.

- **C10 (CI's own plan before push).** Run:

  ```
  cargo build --locked -p streams-quality-syntax && QUALITY_EVENT_NAME=push QUALITY_BASE_REF=origin/slate QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse HEAD~1) python3 scripts/quality/verification_plan.py --out target/quality-plan
  ```

  Expected:
  - `"changed_rust_files": ["src/config/model.rs", "src/config/tests.rs", "src/history.rs"]`
  - `"mutation_source_files": []`
  - `"mutants": false`, `"loom": false`, `"miri": false`, `"properties_fuzz": false`, `"compiler": true`

  Then repeat with `QUALITY_BEFORE_SHA=729c52ac` for the real push range. That range also carries the 5 unpushed commits' selections.

---

## 8. Out of scope

- **`RUNBOOK.md:129-130` "PROCESS-WIDE" and the `history.rs:134-136` section header "Process-wide absorber memory budget".** Both are true in production, where one runtime runs per process. Only the "process-level static" claim at `:151-152` was false, and §4.3 fixes it.
- **`src/application/read_batch.rs:97-100` `#[cfg(test)]` / `#[cfg(not(test))] let charge`.** This is retention-probe instrumentation that swaps a charge carrier. It is not a value fork of a shipped default.
- **`scripts/read-experiments/followup/prepare.py:42`.** It writes a `HistoryConfig { canonical_span_cache: .. }` field that no longer exists. It is a stale experiment script and unrelated.
- **A source rule forbidding `cfg!(test)` in non-test code.** After this change there are zero occurrences, so a ratchet would be cheap. It is a quality-policy addition and belongs in its own item.
- **Item 41.** `ABSORB_GLOBAL_GATHERS=1x` silently falls back to the default (`load.rs:101`, `env_parse`). That is a separate item and not touched here.
- **The floor itself is silent.** `with_body_limit` raises 64 MiB to 96.2 MiB without a log line. The old OnceLock warned `ABSORB_GLOBAL_BUDGET_BYTES raised ... -> ...`, although the startup `memory budget:` line shows the floored figure. This is observability, not this item.
- **CHAOS-5 (the concurrent-gather transient model).** Unrelated.

## 9. Decisions for Søren

None are required. This plan changes nothing at the product, raw, wire, metric or JSON edge of any shipped build, as §2 shows. The only behaviour that moves is the test binary's absorber posture, and it moves to the shipped one, which is what the reviewer asked for. If C9 finds a test that fails only at the shipped posture, the fallback there applies: a finding first, then an explicit opt-in with a reason.

---

## Skeptic corrections (C1..C8)

I re-checked every claim against the tree at `2fb92fb9` using only reads and greps. **Confirmed:**
- The fork, the field docs and the pin: `model.rs:132-139`, `:386-404`, `tests.rs:126-127`. The only two `cfg!(test)` in the repo are `model.rs:393,398`.
- The floor arithmetic: `history.rs:240-246`, `:308-330`, `:369-377`. Line 243 is `.max(worst_frame_transient)`.
- The use-site list, including `src/dst`. The only explicit `HistoryConfig {..}` literals are `history_gather.rs:744` and `controller_tests.rs:23`, and the only `HistoryResources::new` / `with_body_limit` calls are the ones listed.
- The rig pool override: `fixture_http.rs:415-436`. Rigs have no `HistoryConfig` option (`HttpRigOptions`, `fixture_http.rs:14-37`).
- `rss_shed_mb: 0` in rigs (`fixture_http.rs:503`). No test reads the budget JSON or `/v1/debug/load` absorb keys.
- No `#[expect]` or `#[allow]` in `model.rs` or `config/tests.rs`. The `history.rs:151-152` hunk is outside every expect scope (nearest `:338`).
- `cfg` is in `EXPRESSION_MACROS` (`source_rules.py:15-19`).
- The two files are neither critical nor owned (`verification_plan.py:22-31`, `mutation_owners.py`), so no mutants are selected.
- R1 and R2 fail today with exactly the quoted messages (Rust 1.98.1 panic format), pass after the fix, and are synchronous and bounded.
- R2 fits on one line under rustfmt defaults: there is no rustfmt.toml, and the tree already has 2-element chains of 77 columns on one line, e.g. `read_subset_retention.rs:114`.
- `CliArgs::deterministic` gives 32 MiB body and 32 MiB packing (`cli.rs:566,586`).
- Counts: config::tests has 16 tests; the C4 files hold 8, 11, 3, 6, 9 and 8 tests; the C5 files hold 10 and 2.
- The historical hash ledgers (`docs/quality/legacy-*.json`, `verification.json`, `architecture-review-baseline.json`) pin genesis-commit hashes of `model.rs` and `tests.rs`. Those hashes already differ from HEAD, so they are not live and need no update.
- No doc under `docs/`, the RUNBOOK or `deploy/` documents the test split. RUNBOOK:129-130 already states 67108864 and 2.

**C1: the tree state is stale.**
- `origin/slate` is now `2fb92fb9`. `git rev-parse HEAD origin/slate` and `git merge-base HEAD origin/slate` all print `2fb92fb9`.
- So the merge base for the line ceilings is `2fb92fb9`. The ceilinged figures are unchanged, since none of the 9 files differs, and `history.rs` is 1,713 with a budget of +0.
- Fix the header (line 3).
- In C10 drop the second run with `QUALITY_BEFORE_SHA=729c52ac`. That range now contains 5 already-pushed commits, including the critical `src/sse/session/*`, and would report `"mutants": true` for work that is not this item's. The only real push range is `HEAD~1` = `2fb92fb9`.

**C2: the `src/config/tests.rs` line count is off by one.**
- R2 as written is 21 lines (190-210). Adding the separating blank line makes 22 inserted lines. The plan itself says `env_overlay_applies_with_legacy_parse_semantics` moves from 190 to 212.
- So the file goes 601 → **623**, not 622. Fix the §4 table and C7 (`wc -l` prints `508`, `623`, `1713`).
- §4.2 "R2 is 15 lines" should read "R2 is 21 lines (15-line body)".
- The red line numbers 200:5, 206:5 and 127:5 are correct.

**C3: the `filtered out` figures in C4 are wrong.**
- libtest reports filtered = total − matched. After the commit the lib holds 1,204 tests, so C4 prints `44 passed; ... 1160 filtered out`, not "1 filtered out".
- For the same reason, C2 prints 1,187 filtered, C3 prints 1,199 and C5 prints 1,192. `tests_ran.py` checks only the floor and the `--exact` names, so no leg fails, but the stated expectation is wrong.
- C9's `1203 passed; ... 1 filtered out` is right, matching `gate.sh:17` with `--skip post_split_throughput_scales` against today's 1,202 + 1.

**C4: the rationale in §1.3 and §2 is factually wrong. The fix itself is unchanged.**
- The plan says the integration test "cannot reach the `pub(crate)` config" and that a `cfg(not(test))` pin "can never run". Both are false:
  - `streams_slate::ServerConfig` is `pub use` (`lib.rs:77`) with `pub history: HistoryConfig` (`model.rs:35`).
  - `ServerConfig::load` is `pub` (`load.rs:18`), and `Environment` and `ProcessEnvironment` are `pub`.
  - A `tests/*.rs` integration test compiles the library without `cfg(test)`, so it could pin 64 MiB and 2 even with the fork.
- Restate the rejected alternative as: "an integration-test pin would run, but it would leave the whole lib suite exercising a posture no build ships, which is the defect".

**C5: §2's claim that the per-gather reservation is identical in both postures is false for rigs with a lowered body limit.**
- Take `runtime_journals.rs:117-131` (`r10_…`, body limits 64 KiB and 128 KiB):
  - worst = 393,216 and 589,824, so capacity = max(64 MiB, worst) = 67,108,864.
  - The rig absorber keeps `gather_max_bytes` = 32 MiB (`fixture_http.rs:314-320`, `history.rs:667`), so the estimate cap = max(100,663,296, worst) = 100,663,296.
  - `reserve` clamps the grant to 67,108,864, where today it is 100,663,296.
- This is not observable. It is a single-prefix rig with one absorber, the lone reservation then holds the whole pool, and `try_grow` returns `true` at `add == 0` (`history.rs:400-402`).
- Replace the sentence with: "identical whenever capacity ≥ the estimate cap. Otherwise the lone grant clamps to the whole pool and `try_grow` treats it as covered."

**C6: the new `history.rs:151-152` doc omits the standalone pool, and it must stay exactly 2 lines.**
- `shard.rs:1451-1456` builds a private pool for any engine opened with `shared_history: None`.
- Suggested 2-for-2 text:
  ```
  /// Budgets are per runtime: `RuntimeCaps` owns the `HistoryResources` bootstrap
  /// hands every engine; an engine opened without one (tests) builds its own.
  ```
- A third line would grow the ceilinged `history.rs` (1,713) and fail the architecture and ceiling gates.
- `history.rs:147,332`, `history/gather.rs:304,339` and `history/worker.rs:317` still say "process-wide". As §8 argues, that is acceptable, because production runs one runtime per process. C7's grep for `process-level static` is correctly narrow.

**C7: cross-plan line shift.**
- `plans10/telemetry-append-preauth-body.md:292` cites `config/model.rs:508-511` for the admission byte bucket.
- This item deletes 6 lines above them (2 in the field docs and 4 in `Default`), so they move to 502-505.
- Whichever plan lands second must re-cite. The two edits do not overlap textually.

**C8: the hazard list in §2 is complete for what can observe the change. One addition.**
- Every multi-prefix rig not in C4 either pauses its pool (`backlog_rig`, `admission_maintenance.rs:190-194`, used by `split_admission` with 4 prefixes at `:320-323`; and `http_rig_cold_absorb`) or opens its prefixes directly through the open gate (`runtime_open_gate.rs:480`).
- There are no failpoints in `src/history*`.
- The `hold_class` users are exactly the ones listed.
- `HISTORY_FLUSH_STALL_MS` is set by no test.
- Add `admission_maintenance::split_admission` to the paused list explicitly, so the C4 selection reads as exhaustive.

**Mutation, ratchets and ledgers.**
- I agree there are no mutants, since no changed file is critical or owned.
- No `#[expect]` scope is touched, and there are no new source-inventory facts.
- `src/config/tests.rs` keeps its single `unresolved-glob` row.
- There are no DST, inventory, review-mechanism, WIRE-MATRIX or architecture-policy changes.
- No ledger is missed.

**Verdict: ready-with-corrections.** C1-C3 are wrong expectations in the controls, C4-C5 are inaccurate rationale, C6 is a doc wording fix within the 2-line budget, and C7-C8 are coordination and completeness notes. None changes the edit set, the red tests, or the mutation or ledger analysis.
