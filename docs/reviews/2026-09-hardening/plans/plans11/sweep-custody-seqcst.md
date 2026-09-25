# Item 63, step A: the sweep-custody Dekker handshake (SweepCustody)

Tree read: `slate` @ 33fbd10e (origin/slate = 2f2c3015). Read-only planning; nothing was built or run.
Scope: step A only (the handshake type, its orderings and a Loom model). Step B (SweepScheduler
consolidation of `SweepSched`, the 11 `BillingService` accessors and the derived resident gauge) is out of scope.

Short version:
- The bug is real. Both sides of the handshake use Relaxed, and nothing orders them against each other: no common lock, and the gate's Ready path stamps outside the map guard. The bad interleaving is allowed under C11, and it can also happen on x86-TSO hardware, not just in theory (§1.3).
- Two parts of the reviewer's Change do not work as written. **`cfg(loom)` atomics** are not buildable or checked in this repo: no leg builds `--cfg loom`, and `unexpected_cfgs` fails under `-D warnings`. **"SeqCst" with a plain store** as the install's publish is correct under C11, but Loom 0.7.2 cannot confirm it: it treats SeqCst accesses as AcqRel, so the Loom test would fail on correct code.
- What to build instead:
  - The type is generic over its atomic word, with std as the default. Loom instantiates exactly the same code.
  - Every write to `custody` is a read-modify-write (the install publishes with `swap`).
  - The handshake accesses are SeqCst.
  - The correctness argument Loom checks is acquire/release through custody's modification order (§2.3).

---

## 1. Problem (verified on the current tree)

### 1.1 The handshake as it stands

`src/billing.rs:1772-1780`, the external stamp. It is called for every customer resolution:
```rust
pub(crate) fn stamp_external(engine: &std::sync::Arc<crate::shard::ShardEngine>) {
    let seq = ADOPTION_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    engine
        .last_external_seq
        .store(seq, std::sync::atomic::Ordering::Relaxed);      // W(L)  :1776
    engine
        .sweep_custody
        .swap(0, std::sync::atomic::Ordering::Relaxed);          // RMW(C) :1779
}
```
`src/billing.rs:1786-1807`, the sweep's install:
```rust
fn install_custody(engine: &std::sync::Arc<crate::shard::ShardEngine>) -> Option<u64> {
    use std::sync::atomic::Ordering;
    if engine.last_external_seq.load(Ordering::Relaxed) != 0 {   // :1788
        return None;
    }
    let seq = ADOPTION_SEQ.fetch_add(1, Ordering::Relaxed) + 1;
    engine.sweep_custody.store(seq, Ordering::Relaxed);          // W(C)  :1792  (plain store)
    // Re-check: ...
    if engine.last_external_seq.load(Ordering::Relaxed) != 0 {   // R(L)  :1796
        if engine
            .sweep_custody
            .compare_exchange(seq, 0, Ordering::Relaxed, Ordering::Relaxed)   // :1799
            .is_ok()
        { /* we still held it; release cleanly (no SCHED_HELD yet) */ }
        return None;
    }
    Some(seq)
}
```
The stamp does W(L) then reads C (the swap). The install does W(C) then R(L). This is the store-buffering (Dekker) shape.

With every access Relaxed, the C11 model allows this execution:
- The stamp's swap is earlier than the install's store in C's modification order, so the swap reads the old 0 and writes 0.
- The install's R(L) reads the initial 0.

So `install_custody` returns `Some(seq)`, and the install's store is the last write to C, which leaves `sweep_custody == seq` on an engine with `last_external_seq != 0`. That is exactly what the reviewer's first step forbids.

### 1.2 Why it matters: a debt-free sweep retires an adopted engine

`close_scheduler_engine` (`src/billing.rs:1872-1913`) decides with `compare_exchange(seq, 0, Relaxed, Relaxed)` at :1896-1901. Its doc and the R29 comment at :1765 say "custody still present implies no external stamp since install". After 1.1 that premise is false.

The CAS succeeds and the engine is retired while the customer that stamped it still holds its `Arc<ShardEngine>`. This happens:
- in the SAME sweep's phase-2 discovery (`:1691` mark, then `:1709` close when `probe_debt` reads clean);
- in any later phase-1 audit (`:1622` `custody_intact` sees `== seq`, then `:1643`);
- in the walk (`:1935` mark, then `walk_settle` `:1950`/`:1955`).

The window stays open until that customer's NEXT resolution, which stamps again and swaps custody away. A holder that resolves once and then keeps the engine gets no second stamp:
- the LiveFeed source (`src/sse/source.rs:342`, `:433` `Adoption::External`);
- the test `dst_tests::livefeed_swap::livefeed_swap_externally_adopts_child_engines`, which pins exactly that single stamp.

The impact is low: the retired-engine cutoff and the replay contract recover it. The invariant that is broken is the one the whole R29 model is built on.

### 1.3 The race is reachable

Nothing orders the two sides.

**Ready path.** `ShardDirectory::resolve`, `src/shard_directory.rs:278-285`:
```rust
OpenOutcome::Ready(engine) => {
    // R29: a customer who coalesced into (or raced) an open
    // the sweep started still counts as external adoption.
    if external {
        crate::billing::stamp_external(&engine);   // :284 -- outside any guard
    }
```
The sweep's phase 2 does `open_or_wait` → `Ready` → `state.shards.open` → `mark` → `install_custody` (`src/billing.rs:1675-1691`). The customer and the sweep wake on the SAME single-flight open and run with no shared lock.

**Fast path.** `:246-256` stamps inside the map READ guard. `install_custody` holds no guard at all, so the fast path does not exclude the install either.

**On hardware.** On x86-TSO (Prisma Compute is x86_64):
- the install's plain `MOV [C]` can sit in the store buffer while its `MOV r,[L]` reads 0;
- the stamp's `XCHG [C]` drains only the stamp's own buffer and reads the old C.

The bad outcome is therefore possible on the production ISA, not only under the abstract model.

### 1.4 Every use site (grep of `last_external_seq|sweep_custody|stamp_external|install_custody|ADOPTION_SEQ`)

Production:

| Site | What |
|---|---|
| `src/shard.rs:1129-1141` | doc + `pub last_external_seq: AtomicU64`, `pub sweep_custody: AtomicU64` on `ShardEngine` |
| `src/shard.rs:1382-1383` | both fields initialised in the `Arc::new(ShardEngine { .. })` literal inside `ShardEngine::start` |
| `src/billing.rs:1576` | `scheduler_held`: `e.sweep_custody.load(Relaxed) != 0` (budget and peak gauge) |
| `src/billing.rs:1752-1767` | R29 invariant doc + `static ADOPTION_SEQ` |
| `src/billing.rs:1772-1780` | `stamp_external` |
| `src/billing.rs:1786-1807` | `install_custody` (called only from `mark`, `:1820`) |
| `src/billing.rs:1839-1853` | `custody_intact`: `engine.sweep_custody.load(Relaxed) == seq` (callers `:1622`, `:1950`) |
| `src/billing.rs:1873`, `:1896-1901` | `close_scheduler_engine`: `use Ordering` + the retire closure's CAS (callers `:1643`, `:1709`, `:1955`) |
| `src/shard_directory.rs:255`, `:284` | the only callers of `crate::billing::stamp_external` |

`mark` has two callers, `:1691` (phase 2) and `:1935` (`walk_engine_budgeted`). `scheduler_held` has two callers, `:1826` (`mark`) and `:1930` (walk budget).

Tests (read-only uses of the raw words):

| Site | Test |
|---|---|
| `src/dst/tests/runtime_sweep.rs:496-502` | `custody_declines_on_prior_external_use` |
| `src/dst/tests/runtime_sweep.rs:563-566`, `:571-577` | `internal_touch_does_not_leak_an_engine_from_the_rotation` |
| `src/dst/tests/runtime_sweep.rs:893-898`, `:915-919` | `revoked_close_keeps_the_identical_engine_with_no_new_open` |
| `src/dst/tests/livefeed_swap.rs:800-806`, `:814-820` | `livefeed_swap_externally_adopts_child_engines` |

No other reader exists: nothing in `/v1/debug`, `/metrics`, docs, fuzz or `tools/quality-invariants`. No writer exists outside `src/billing.rs`.

---

## 2. Contract decision

### 2.1 The typed owner

New module `src/billing/sweep_custody.rs`, re-exported as `crate::billing::SweepCustody`:

```rust
pub(crate) trait CustodyWord { load, store, swap, compare_exchange }   // std signatures, u64
impl CustodyWord for std::sync::atomic::AtomicU64 { /* delegation */ }

#[derive(Default)]
pub(crate) struct SweepCustody<W = std::sync::atomic::AtomicU64> {
    last_external_seq: W,   // private
    custody: W,             // private
}
impl<W: CustodyWord> SweepCustody<W> {
    pub(crate) fn stamp_external(&self, seq: u64);
    pub(crate) fn install(&self, seq: u64) -> bool;      // true = custody held under seq
    pub(crate) fn revoke_if(&self, seq: u64) -> bool;    // the close / decline CAS
    pub(crate) fn holds(&self, seq: u64) -> bool;        // audit hint (custody_intact)
    pub(crate) fn held(&self) -> bool;                   // budget hint (scheduler_held)
    #[cfg(test)] pub(crate) fn value(&self) -> u64;
    #[cfg(test)] pub(crate) fn externally_resolved(&self) -> bool;
}
```

`ShardEngine` replaces its two `pub` atomics with `pub(crate) sweep_custody: crate::billing::SweepCustody`. The field name stays, so the conceptual churn is limited to the type.

The draw from `ADOPTION_SEQ` stays in `billing.rs`. The static does not move, so there is no churn in `source-allowances.json`/`owners.json`. The methods take the drawn value, which keeps the Loom model free of process globals.

`crate::billing::stamp_external(engine)` keeps its path and signature. That leaves `src/shard_directory.rs` untouched, and with it the fingerprinted unwrap scope of `ShardDirectory::resolve` (§4.3).

### 2.2 Why not `cfg(loom)` and why not "SeqCst with the plain store" (the reviewer's Change as written)

**`cfg(loom)` does not fit this repo.**
- Every Loom model here runs inside the ordinary `cargo test` build: `src/touch/loom_tests.rs`, `src/shard/commit_handoff/loom_tests.rs`, `src/billing/read_{accumulator,spool}/tests.rs`, all named `quality_loom_*`.
- No workflow builds with `RUSTFLAGS=--cfg loom`. `rust-quality.yml:79-82` exports only `CHECK_MIRI/PROPERTIES_FUZZ/MUTANTS`, so plan.json's `loom` flag is consumed by nothing.
- A `#[cfg(loom)]` switch would trip `unexpected_cfgs`, which fails under `-D warnings` without a Cargo check-cfg change, and the test would never run in CI.
- Switching the word type under `cfg(test)` instead would break every DST rig: Loom atomics panic outside `loom::model`.
- The fix is a type parameter that defaults to std. Loom instantiates the SAME methods, which is what RUST-QUALITY asks for: "Loom exercises the actual small state-transition implementation through instrumented primitives".

**SeqCst with a plain store is right in C11 but fails in Loom 0.7.2.** Loom 0.7.2 does not implement SeqCst accesses. In `loom-0.7.2/README.md` "Unsupported features", SeqCst accesses "are regarded as `AcqRel`". In `src/rt/thread.rs`, `seq_cst()` is an intentional no-op. The initial store of every Loom atomic is a non-SeqCst `Release` store (`rt/atomic.rs` `State::new`).

With `install = store(C, SeqCst); load(L, SeqCst)`, Loom still lets `load(L)` return the initial 0 after the stamp's swap has run, because no acquire edge exists. The model therefore reports the store-buffering outcome, a false alarm against code that is correct. Control C4 in §7 demonstrates it.

**The buildable fix: make the install's publish an RMW (`swap`).** Then every write to `custody` is an RMW: the stamp's `swap`, the install's `swap`, and the `revoke_if` CAS in both the close and the decline path. Correctness follows from acquire/release alone (§2.3), which Loom models exactly. SeqCst is still applied as the reviewer asked, and adds the textbook SC-total-order argument on top.

### 2.3 The ordering argument (goes into the type's doc)

Take the two RMWs on `custody`. Whichever comes second in custody's modification order settles the outcome:

- **The stamp's swap is second.** It reads the install's value (or a later one) and writes 0. Custody is revoked, whatever the install returned.
- **The install's swap is second.** It reads from the stamp's swap, or from a later RMW in that swap's release sequence: every later write to C is an RMW, so the sequence is never broken. The stamp's swap is SeqCst ⊇ Release and the install's is SeqCst ⊇ Acquire, so the stamp's `store(L)`, sequenced before its swap, happens-before the install's re-check `load(L)`. By coherence that load reads the stamp or later, so it reads nonzero, and the install declines and `revoke_if`s.

In both cases, once a stamp has completed, custody is 0. That is strictly stronger than the reviewer's "never `install()==Some(seq) && custody==seq && last_external_seq!=0`", and it is what the Loom test asserts.

The following stay Relaxed, each with a stated reason:
- `held`/`holds`: hints; every decision they feed is re-made by the `revoke_if` RMW, so a stale read costs one probe, never a wrong close.
- `revoke_if`'s failure ordering.
- The `ADOPTION_SEQ` draw: uniqueness only.

**Hot-path cost.** `stamp_external` runs on every external resolution. On x86_64 the stamp becomes XCHG+XCHG (it was MOV+XCHG), on a cache line the existing swap already owns. The install moves from MOV to XCHG, on a cold path.

A Loom-verified alternative with zero x86 codegen change: `store(L, Relaxed)` in the stamp, keeping the SeqCst swap. The §2.3 argument needs only the swap's release. It is offered in §9 and not recommended by default.

### 2.4 Wire and metric surface

No wire change. No status, body, header, `/v1/debug` or `/metrics` shape changes:
- `sweep_resident_engines` (ops gauge) reads the `SweepSched.opened` map, which is untouched.
- `sweep_open_peak` reads `SweepSched.peak`, which is untouched.
- `scheduler_held` keeps its meaning (count of engines with custody ≠ 0).

The only behaviour change: an install that races an external stamp now always ends with custody revoked or declined. Before, it could end with custody installed over external history.

`ADOPTION_SEQ` now also advances on declined installs, because the draw moves ahead of the early check. The values are compared only for equality or nonzero and are exposed nowhere, so this is unobservable.

---

## 3. Tests

### 3.1 Red test (behaviour change): the Loom model

- **File:** `src/billing/sweep_custody/tests.rs` (new; declared by `#[cfg(test)] mod tests;` in `sweep_custody.rs`).
- **Name:** `billing::sweep_custody::tests::quality_loom_external_stamp_never_coexists_with_custody`
- **Bounds (stated in its doc):** one spawned stamper plus the installing model thread; `max_threads = 2`, `max_branches = 1000`, `preemption_bound = Some(2)`, `max_permutations = None`, `max_duration = None`.
- **Scope note (in its doc):** the close CAS runs under the directory write guard and stays covered by the DST revoked-close rig. It is not modelled here.

```rust
impl CustodyWord for loom::sync::atomic::AtomicU64 { /* 4 delegations to loom's inherent methods */ }

#[test]
fn quality_loom_external_stamp_never_coexists_with_custody() {
    use loom::sync::Arc;
    use loom::sync::atomic::AtomicU64;
    let mut model = loom::model::Builder::new();
    model.max_threads = 2;
    model.max_branches = 1000;
    model.preemption_bound = Some(2);
    model.max_permutations = None;
    model.max_duration = None;
    model.check(|| {
        let custody = Arc::new(SweepCustody::<AtomicU64>::default());
        let customer = custody.clone();
        let stamp = loom::thread::spawn(move || customer.stamp_external(1));
        let installed = custody.install(2);
        stamp.join().unwrap();
        if installed {
            assert!(!custody.holds(2), "install 2 kept custody over an external stamp");
        }
        assert!(!custody.held(), "an external stamp left custody installed");
    });
}
```
The `if installed { assert!(..) }` form avoids any `nonminimal_bool` question. Nesting is fn → closure → if = 3, within the limit of 4.

**Where the red is taken.** HEAD has no seam that can run the handshake over Loom atomics. Commit 1 (§4) creates the seam with the orderings and the plain `store` publish copied verbatim, so commit 1 behaves exactly like HEAD. The red is taken on commit 1 plus this test file.

Trace under Loom 0.7.2 on commit 1's code:
1. Model thread: `install(2)` runs `load(L, Relaxed)` and reads the initial 0 (the only store).
2. One preemption (bound 2): the stamper runs `store(L,1,Relaxed)`, then `swap(C,0,Relaxed)`. As an RMW the swap reads the newest store of C, the initial 0, and writes 0. The stamper finishes.
3. The model thread resumes with `store(C,2,Relaxed)`, then the re-check `load(L, Relaxed)`. Candidates are the initial 0 and the stamper's 1. The stamper's store is not in the model thread's causality: it never acquired anything from the stamper, and `last_yield` is None. So `match_load_to_stores` offers both, and Loom explores returning 0.
4. `install` returns true. C's last store is the model thread's 2.
5. After join, `holds(2)` is true, so the first assertion fires.

The message is deterministic. On commit 1 every failing execution has `installed == true && C == 2`. A declined install on commit 1 always ends at C = 0: either its CAS revokes 2, or the stamp's swap came later. So the second assertion can never be the first to fire.

EXACT expected red output (`cargo test --locked --lib billing::sweep_custody::tests::quality_loom_external_stamp_never_coexists_with_custody`, on commit 1 with the test file applied):
```
running 1 test
test billing::sweep_custody::tests::quality_loom_external_stamp_never_coexists_with_custody ... FAILED

failures:

---- billing::sweep_custody::tests::quality_loom_external_stamp_never_coexists_with_custody stdout ----

thread 'billing::sweep_custody::tests::quality_loom_external_stamp_never_coexists_with_custody' panicked at src/billing/sweep_custody/tests.rs:<line>:<col>:
install 2 kept custody over an external stamp
...
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; <n> filtered out
```
`<line>:<col>` is the `assert!(!custody.holds(2), ..)` line. Loom 0.7 runs model threads as coroutines on the test's own OS thread, so the thread name is the test path.

Green on commit 2: `test result: ok. 1 passed`.

### 3.2 Deterministic unit tests (std instantiation; pin the refactor and kill every mutant)

Same file. They pass on commit 1 and on commit 2.

| Test | Body (all on `let custody: SweepCustody = SweepCustody::default();`) |
|---|---|
| `install_holds_exactly_its_own_value` | `assert!(!custody.held()); assert!(custody.install(5)); assert!(custody.held()); assert!(custody.holds(5)); assert!(!custody.holds(6));` |
| `an_earlier_external_stamp_declines_the_install` | `custody.stamp_external(1); assert!(custody.externally_resolved()); assert!(!custody.install(2)); assert!(!custody.held());` |
| `an_external_stamp_revokes_installed_custody` | `assert!(custody.install(5)); custody.stamp_external(6); assert!(!custody.held()); assert!(!custody.revoke_if(5));` |
| `revoke_releases_only_the_installers_value` | `assert!(custody.install(5)); assert!(!custody.revoke_if(4)); assert!(custody.holds(5)); assert!(custody.revoke_if(5)); assert!(!custody.held());` |

### 3.3 Pinning tests for the refactor (commit 1: behaviour-identical)

- `dst::dst_tests::runtime_sweep::*` (9 tests), especially:
  - `custody_declines_on_prior_external_use`
  - `internal_touch_does_not_leak_an_engine_from_the_rotation`
  - `customer_race_into_a_sweep_opened_engine_prevents_its_close`
  - `revoked_close_keeps_the_identical_engine_with_no_new_open`
  - `sweep_peak_open_never_exceeds_the_budget` (drives `scheduler_held`)
  - `tombstone_walk_peak_residency_stays_under_the_budget` (walk mark/settle)
- `dst::dst_tests::livefeed_swap::livefeed_swap_externally_adopts_child_engines`
- `shard_directory::` unit tests (resolve External/Internal; the file is untouched).

Only the four DST tests in §1.4 change, and only their read spelling:
- `.load(Relaxed) == 0` becomes `!held()`.
- `!= 0` becomes `held()`.
- The equality check in `internal_touch` becomes `value()`.
- `last_external_seq.load() > 0` becomes `externally_resolved()`.

Their assertions and messages are unchanged.

### 3.4 Compile-level proofs

- The fields are private. Any `engine.sweep_custody.custody.load(..)` or `.last_external_seq` outside `billing::sweep_custody` is E0616. After commit 1, `grep -rn "last_external_seq" src --include=*.rs` hits only `src/billing/sweep_custody.rs`.
- `grep -rnE "sweep_custody\s*\.\s*(load|store|swap|compare_exchange)" src --include=*.rs` returns nothing.
- `ShardEngine` still constructs through `crate::billing::SweepCustody::default()`. The type parameter defaults, so no production call site names `W`.

---

## 4. Edits, file by file, in commit order

Ceilinged files versus origin/slate. Budget = the current `wc -l`, which may not be exceeded by even one line.

| File | wc -l now | After C1 | After C2 | Budget |
|---|---|---|---|---|
| `src/billing.rs` | 2,201 | ~2,159 (−42) | 2,159 | 2,201 |
| `src/shard.rs` | 3,196 | ~3,186 (−10) | 3,186 | 3,196 |
| `src/dst/tests/runtime_sweep.rs` (DST, 1,000 ceiling) | 922 | ~908 (−14) | 908 | 1,000 |
| `src/dst/tests/livefeed_swap.rs` (DST, 1,000 ceiling) | 936 | ~928 (−8) | 928 | 1,000 |
| `src/billing/sweep_custody.rs` (new) | — | ~120 | ~140 | 1,000 |
| `src/billing/sweep_custody/tests.rs` (new) | — | ~45 | ~95 | 1,000 |

Untouched on purpose:
- `src/shard_directory.rs` (650; registered owner `shard_directory`; `resolve` carries a fn-wide `unwrap_used` expect that fingerprints both `crate::billing::stamp_external(..)` call-sites).
- `src/billing_service.rs` (step B).

### Commit 1: "The sweep custody words move behind a private-field SweepCustody, orderings verbatim"

**C1.1 `src/billing/sweep_custody.rs` (new).**
- Module doc: the reason the type exists. It is whether the billing sweep may close an engine it opened, and both words are private so no caller can reorder the handshake between them.
- `use std::sync::atomic::Ordering;`
- Trait `CustodyWord` with the std signatures `load`, `store`, `swap`, `compare_exchange -> Result<u64, u64>`. Doc: production runs `SweepCustody` over std atomics, and the Loom model runs the same code over Loom's atomics, which exist only inside a model.
- The std impl, as pure delegations spelled `std::sync::atomic::AtomicU64::load(self, order)` etc. (inherent resolution, no recursion).
- `#[derive(Default)] pub(crate) struct SweepCustody<W = std::sync::atomic::AtomicU64> { last_external_seq: W, custody: W }`. Its doc is the R29 invariant list, moved verbatim from `billing.rs:1752-1766`, keeping the `///   * ` list format that passes the pinned clippy doc lints today. Each field gets a one-line doc (nonzero once stamped; 0 = not held, otherwise the installer's value).
- The methods, with orderings **verbatim** (Relaxed everywhere, plain-store publish):
```rust
pub(crate) fn stamp_external(&self, seq: u64) {
    self.last_external_seq.store(seq, Ordering::Relaxed);
    self.custody.swap(0, Ordering::Relaxed);
}
pub(crate) fn install(&self, seq: u64) -> bool {
    if self.last_external_seq.load(Ordering::Relaxed) != 0 {
        return false;
    }
    self.custody.store(seq, Ordering::Relaxed);
    // Re-check: a stamp that landed between the first read and the
    // store has either already revoked (swap saw our value) or carries
    // a newer last_external_seq; both mean decline.
    if self.last_external_seq.load(Ordering::Relaxed) != 0 {
        self.revoke_if(seq);
        return false;
    }
    true
}
pub(crate) fn revoke_if(&self, seq: u64) -> bool {
    self.custody.compare_exchange(seq, 0, Ordering::Relaxed, Ordering::Relaxed).is_ok()
}
pub(crate) fn holds(&self, seq: u64) -> bool { self.custody.load(Ordering::Relaxed) == seq }
pub(crate) fn held(&self) -> bool { self.custody.load(Ordering::Relaxed) != 0 }
#[cfg(test)] pub(crate) fn value(&self) -> u64 { self.custody.load(Ordering::Relaxed) }
#[cfg(test)] pub(crate) fn externally_resolved(&self) -> bool { self.last_external_seq.load(Ordering::Relaxed) != 0 }
```
- `#[cfg(test)] mod tests;`
- Rustdoc: code in backticks only, with no `[..]` links or bare `<W>` in prose. The trait is `pub(crate)`, the same nominal visibility as the impl methods, so `private_bounds` does not fire. All items are `pub(crate)`, so `unreachable_pub` does not fire.

**C1.2 `src/billing/sweep_custody/tests.rs` (new).** The four §3.2 unit tests. Use explicit imports `use super::SweepCustody;` (no `use super::*`, which would need an unresolved-glob `owners.json` row). There is no Loom test yet in C1.

**C1.3 `src/billing.rs`** (not critical, not a registered mutation owner; none of the touched fns is under an `#[expect]`):
- After `:37` (`pub(crate) use telemetry_loop::spawn_telemetry;`): `mod sweep_custody;` and `pub(crate) use sweep_custody::SweepCustody;` (+2).
- `:1576`: `.filter(|e| e.sweep_custody.held())` (±0).
- `:1752-1807` (56 lines) are replaced by 23 (−33):
  - A 4-line doc on `ADOPTION_SEQ` giving the reason it is ONE process-wide sequence: custody values stay unique across engines and across incarnations of one prefix, so a record left by a retired engine never matches its successor's custody. The handshake itself is `SweepCustody`. Then the static itself, **token-identical**, which keeps its `source-allowances.json` row valid.
  - `stamp_external`, with its doc keeping the "internal paths (tombstone walk, scaler) never stamp" reason:
    `let seq = ADOPTION_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1; engine.sweep_custody.stamp_external(seq);`
  - `install_custody`, keeping its 4-line doc and signature:
    `let seq = ADOPTION_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1; engine.sweep_custody.install(seq).then_some(seq)`
  - `mark` is untouched.
- `custody_intact` `:1839-1853` (17 → 12, −5): body `state.billing.sweep_custody_seq(prefix).is_some_and(|seq| engine.sweep_custody.holds(seq))`.
- `close_scheduler_engine`: delete `:1874` `use std::sync::atomic::Ordering;` (−1). The closure `:1896-1901` becomes `|engine, _incarnation| engine.sweep_custody.revoke_if(seq),` (−5).
- The `reverse_edges` count in the architecture gate (http|product) does not grow: no new `crate::http` references.

**C1.4 `src/shard.rs`** (registered owner `shard`, critical prefix `src/shard`):
- `:1129-1141` (11 doc lines + 2 fields) become 4 lines (−9):
```rust
    /// R29: whether the billing sweep may close this engine — the
    /// scheduler's custody and the external stamp that revokes it, owned
    /// by `crate::billing::SweepCustody` so no caller can reorder them.
    pub(crate) sweep_custody: crate::billing::SweepCustody,
```
- `:1382-1383` become `sweep_custody: crate::billing::SweepCustody::default(),` (−1).
- **Ratcheted scope: `ShardEngine::start` (`:1337-1358`, five `#[expect]`s, fn ≈390 lines).**
  - The fn-wide `clippy::unwrap_used` expect fingerprints every call-site and path fact in its scope. That includes the whole `Arc::new(ShardEngine { .. })` call-site, so ANY literal edit mints a new key. The gate would print `accepted exception grew without a new decision: ('src/shard.rs', 'crate::ShardEngine::start', 'function', ...): unwrap_site:ordinary-call:crate::ShardEngine::start:<digest> 0 -> 1`, plus a `unwrap_site:path:...` key for `crate::billing::SweepCustody::default`.
  - Narrowing is not viable. The 5 unwraps (`:1507`, `:1544`, `:1654`, `:1674` `pump.in_flight.lock()`; `:1743` `ticker.trim_debt.lock()`) sit in spawned closures, and statement-level expects would add about 20 lines to a ceilinged file. Moving the literal out would need a >5-argument builder, and an options bag is forbidden.
  - **Remedy: re-decide the reason on `:1351`, in place and line-neutral.** Exactly two `;`, no `"`:
    `reason = "ShardEngine::start; the pump and trim tickers unwrap only the in-flight queue and trim-debt locks, and a poisoned one may hold a half-recorded group or debt; recovering either could acknowledge a group that never committed or trim a stream that still owes data"`
    This names the five actual sites, so it clarifies rather than launders.
  - The other four expects (`too_many_lines`, `too_many_arguments`, `let_underscore_must_use`, `cast_possible_truncation`+`excessive_nesting`) ratchet only `scope_lines` (−1), `syntax_facts` (−2: two path+call-site pairs out, one pair in) and `nested_items` (±0). They pass unchanged. `too_many_lines` stays fulfilled.

**C1.5 DST (read-spelling only):**
- `runtime_sweep.rs:496-502` → `assert!(!engine.sweep_custody.held(), "custody must not be installed over external history");` (4 lines).
- `:563-565` → `let custody0 = engine.sweep_custody.value();`
- `:571-577` → `assert_eq!(engine.sweep_custody.value(), custody0, "internal resolution must not revoke scheduler custody");` (rustfmt: 5 lines).
- `:893-898` → `assert!(engine.sweep_custody.held());`
- `:915-919` → `assert!(!now.sweep_custody.held(), "scheduler must have dropped its claim");`
- `livefeed_swap.rs:800-806` → `assert!(engine.sweep_custody.externally_resolved(), "<same message>");`
- `:814-820` → `assert!(!engine.sweep_custody.held(), "<same message>");`
- No edited test sits under an `#[expect]`. The DST exceptions at `runtime_sweep.rs:11/693/697/826` and `livefeed_swap.rs:387/497` cover other fns.

**C1.6 Ledgers in the same commit** (§6): `test-inventory.json` (4 hashes) and the mutation owner row.

### Commit 2: "An external stamp and a custody install can no longer both miss each other"

**C2.1 `src/billing/sweep_custody.rs`** (only file with production changes):
- `stamp_external`: `store(seq, SeqCst)`; `swap(0, SeqCst)`.
- `install`: the early `load(SeqCst)`; **`self.custody.swap(seq, Ordering::SeqCst);` replaces the `store`**; re-check `load(SeqCst)`; comment "a stamp landed during the install: release it unless the stamp's swap already did".
- `revoke_if`: `compare_exchange(seq, 0, SeqCst, Relaxed)`.
- `holds`/`held`/`value`/`externally_resolved`: stay Relaxed. The doc sentences give the reasons from §2.3.
- The struct doc gains the §2.3 paragraph, and the invariant list's "(called inside the request path's map guard)" is corrected: the gate Ready path stamps outside it (`shard_directory.rs:284`), which is exactly why the handshake, not the guard, orders it.

**C2.2 `src/billing/sweep_custody/tests.rs`:** add `impl CustodyWord for loom::sync::atomic::AtomicU64` (4 delegations) and the §3.1 Loom test (`use super::{CustodyWord, SweepCustody};`). `loom` is already a dev-dependency (`Cargo.toml:52`, `=0.7.2`). `loom::thread::spawn` is not on the disallowed list and is not an `effect` fact (`classify` only matches `std::thread::`/`tokio::spawn`).

Push C1 and C2 together. Each is gateable alone: C1's mutants are all killed by the §3.2 tests, because none of them depends on orderings.

---

## 5. Mutation analysis (cargo-mutants 27.1.0, `--in-diff`)

Criticality of each changed file:

| File | Critical? |
|---|---|
| `src/billing.rs` | no (not under `CRITICAL_PREFIXES`, not registered) |
| `src/billing/sweep_custody.rs` | new; `src/billing/` is critical only for `read_accumulator`/`read_spool`, so it is **not** critical by prefix. Registered by a new row (below) because it owns a synchronization invariant |
| `src/billing/sweep_custody/tests.rs` | not critical; not passed via `--file` |
| DST files | `src/dst` is not critical |
| `src/shard.rs` | critical (`src/shard` prefix) and registered (`shard`, filter `shard::`) |

**New owner row.** In `scripts/quality/mutation_owners.py`, after `system_append`:
`owner('sweep_custody', 'src/billing/sweep_custody.rs', 'billing::sweep_custody::'),`
The filter matches the 5 tests (`billing::sweep_custody::tests::…`), never zero.

**`src/billing/sweep_custody.rs`.** The whole file is inserted, so every mutant is in-diff: 20 viable mutants, each killed deterministically by the §3.2 tests (the Loom test also runs under the filter).

| Mutant | Killed by |
|---|---|
| `stamp_external` → `()` | `an_earlier_external_stamp_declines_the_install` (install returns true), `an_external_stamp_revokes_installed_custody` |
| `install` → `true` | `an_earlier_external_stamp_declines_the_install` |
| `install` → `false` | `install_holds_exactly_its_own_value` |
| `install` early `!= 0` → `== 0` | `install_holds_exactly_its_own_value` (fresh install returns false) |
| `install` re-check `!= 0` → `== 0` | `install_holds_exactly_its_own_value` (fresh: revoke, false) |
| `revoke_if` → `true` | `revoke_releases_only_the_installers_value` (`!revoke_if(4)`), `an_external_stamp_revokes_installed_custody` |
| `revoke_if` → `false` | `revoke_releases_only_the_installers_value` (`revoke_if(5)`) |
| `holds` → `true` | `install_holds_exactly_its_own_value` (`!holds(6)`) |
| `holds` → `false` | same (`holds(5)`) |
| `holds` `==` → `!=` | same (`holds(5)`) |
| `held` → `true` | same (fresh `!held()`) |
| `held` → `false` | same (`held()` after install) |
| `held` `!=` → `==` | same (fresh `!held()`) |
| std `load` → `0` | `an_earlier_external_stamp_declines_the_install` (install sees L=0, returns true) |
| std `load` → `1` | `install_holds_exactly_its_own_value` (early check declines) |
| std `store` → `()` | `an_earlier_external_stamp_declines_the_install` (L never set) |
| std `swap` → `0` / `1` | `install_holds_exactly_its_own_value` (C2: the publish writes nothing, so `held()` is false); `an_external_stamp_revokes_installed_custody` (the stamp does not revoke) |
| std `compare_exchange` → `Ok(0)` / `Ok(1)` | `revoke_releases_only_the_installers_value` (`!revoke_if(4)` fails; custody not cleared) |

Not mutated:
- `#[cfg(test)]` accessors (skipped by cargo-mutants).
- The trait declaration (no bodies).
- `#[derive(Default)]`.
- The test file (not a `--file` source).

No equivalent mutants. Every predicate and body has an observable effect on a single-threaded std instance.

cargo-mutants generates `Err(..)` replacements for `Result` only when `--error` is configured, and the driver passes none. If it ever did generate `Err(0)`/`Err(1)` for `compare_exchange`, `revoke_releases_only_the_installers_value` would kill them too, because `revoke_if(5)` must be true.

Orderings are not mutation targets: cargo-mutants 27 does not mutate `Ordering` path arguments. Controls C3-C4 (§7) are the executable proof that each changed ordering is load-bearing.

There are no timeout risks: no loops, and the mutated Loom runs end in a panic or explore a two-thread model.

**`src/shard.rs`.** The literal edit sits inside `ShardEngine::start`'s body, so its FnValue mutant is selected: `replace ShardEngine::start -> Arc<ShardEngine> with Arc::new(Default::default())`. It is **unviable**, because `ShardEngine: Default` does not hold. It is not MISSED, and cargo-mutants exits 0.
- No operator sits on the inserted line or on either neighbour of the deletion (`db,` / `maintenance: …RwLock::new(initial_maintenance),`).
- The attribute-reason line and the struct-field lines lie outside every fn body span.
- The cost is the owner's baseline run (`cargo test shard::`) plus one failing build.

`src/shard_directory.rs` is not in the diff and is not selected.

Expected receipt:
- `mutation_source_files` ⊇ `['src/billing/sweep_custody.rs', 'src/shard.rs']`
- `selected_mutation_owners` ⊇ `['sweep_custody', 'shard']` (table order)
- `unregistered_mutation_source_files == []`

If 33fbd10e is still unpushed when this lands, the push diff also carries its http owners (`http`, `http_debug`, `http_telemetry_append`). Run the plan with the real before-SHA.

CI cost estimate: about 21 mutant builds. Item 55's 20 shard mutants took 2h04m on CI, and the job timeout is 240 min.

---

## 6. Ledgers (same commit as the change)

| Ledger | Change |
|---|---|
| `docs/refactor/test-inventory.json` (C1) | `python3 scripts/test-inventory.py --write`. The diff must be exactly 4 `function_sha256` updates: `custody_declines_on_prior_external_use`, `internal_touch_does_not_leak_an_engine_from_the_rotation`, `revoked_close_keeps_the_identical_engine_with_no_new_open`, `livefeed_swap_externally_adopts_child_engines`. The new unit/Loom tests are outside `src/dst` and are not inventoried |
| `scripts/quality/mutation_owners.py` (C1) | + `owner('sweep_custody', 'src/billing/sweep_custody.rs', 'billing::sweep_custody::')` |
| `docs/refactor/review-mechanisms.json` | none. No edited test is pinned (grep: 0 hits for runtime_sweep/livefeed_swap) |
| `docs/quality/owners.json` | none. No moved or new static, no macro-dsl, no glob import, no `#[path]` |
| `docs/quality/source-allowances.json` | none. `ADOPTION_SEQ` stays token-identical in `src/billing.rs` (row `global, src/billing.rs, crate::ADOPTION_SEQ, std :: sync :: atomic :: AtomicU64`). Nothing is vacated, so `--prune` has nothing to prune |
| `docs/refactor/architecture-policy.json` | none. No budget exception touched; `billing.rs` and `shard.rs` shrink |
| `docs/refactor/WIRE-MATRIX.md` | none (§2.4) |
| Scenario map / dispositions | none (no renames) |
| `src/dst/tests/README.md` | none (no new DST module) |
| `docs/quality/verification.json` | none. It is a historical receipt at a1d9dabf that no script reads |

---

## 7. Controls (run sequentially, never alongside a mutation run)

Environment: put a python ≥3.11 shim first on PATH. The gate uses `tomllib`, and stock macOS python3 is 3.9.

| # | Command | Expected |
|---|---|---|
| C0 | `wc -l src/billing.rs src/shard.rs src/dst/tests/runtime_sweep.rs src/dst/tests/livefeed_swap.rs src/billing/sweep_custody.rs src/billing/sweep_custody/tests.rs` | ≤ 2201 / 3196 / 1000 / 1000 / 1000 / 1000 (≈2159 / 3186 / 908 / 928 / ~140 / ~95) |
| C1 (red) | on commit 1 with C2.2's test file applied: `cargo test --locked --lib billing::sweep_custody::tests::quality_loom_external_stamp_never_coexists_with_custody` | FAILED, panic message `install 2 kept custody over an external stamp` (§3.1) |
| C2 (green) | commit 2: `cargo test --locked --lib billing::sweep_custody::` | `test result: ok. 5 passed; 0 failed` |
| C3 (ordering is load-bearing, not committed) | temporarily set the stamp's `swap(0, Relaxed)`, then separately the install's `swap(seq, Relaxed)`; rerun C2's Loom test | each FAILS with `install 2 kept custody over an external stamp`. With no release/acquire pair, the install's swap can read the stamp's 0 without acquiring the stamp's `store(L)` |
| C4 (why the publish is a swap, not committed) | temporarily set the install's publish to `store(seq, SeqCst)` (the reviewer's literal Change); rerun | FAILS with the same message. This is Loom 0.7.2's SeqCst-as-AcqRel false alarm. It documents why the RMW publish is required for the model to certify the code |
| C5 (pins) | `cargo test --locked --lib -- dst_tests::runtime_sweep:: dst_tests::livefeed_swap::livefeed_swap_externally_adopts_child_engines shard_directory::` | all ok: 9 runtime_sweep + 1 livefeed_swap + the shard_directory unit tests; 0 failed |
| C6 | `grep -rn "last_external_seq" src --include=*.rs` ; `grep -rnE "sweep_custody\s*\.\s*(load\|store\|swap\|compare_exchange)" src --include=*.rs` | first: only `src/billing/sweep_custody.rs`; second: no output |
| C7 | `python3 scripts/test-inventory.py --write && git diff --stat docs/refactor/test-inventory.json && python3 scripts/test-inventory.py --check` | exactly 4 hash lines change; `--check` exits 0 |
| C8 | `scripts/quality.sh` | exit 0: fmt, clippy `-D warnings` (no unfulfilled expectation; `too_many_lines` on `start` still fulfilled), rustdoc `-D warnings --document-private-items`, source gate (no `accepted exception grew`; the new reason satisfies the `owner; invariant; alternative` form), architecture gate, mt-lint |
| C9 | `QUALITY_EVENT_NAME=push QUALITY_HEAD_SHA=$(git rev-parse HEAD) QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) QUALITY_BASE_REF=origin/slate python3 scripts/quality/verification_plan.py --out target/quality-plan` | `mutants: true`; §5 receipt; `unregistered_mutation_source_files: []` |
| C10 | (after C1 and C2 are committed) `scripts/quality/mutations.sh` | `sweep_custody`: 20 caught, 0 missed, 0 timeout. `shard`: 1 unviable. Final line `Mutation verification executed 21 selected mutant(s) across 2 registered owner(s).` (more if 33fbd10e's http owners ride along) |
| C11 | full suite as CI runs it: `cargo test --release -- --skip post_split_throughput_scales` (then `scripts/quality/tests_ran.py` against the inventory) | 0 failed; floor met |
| C12 | after push: `gh run list --branch slate --json headSha,createdAt,status,conclusion,name` for the pushed SHA | ci + rust-quality green. Never claim green from memory |

---

## 8. Out of scope

- **Step B:** `SweepScheduler` in `src/billing/sweep.rs`, one per-prefix record, deleting the 11 `BillingService` sweep accessors (`src/billing_service.rs:272-391`), deriving the resident gauge from custody flags.
- **The documented Ready-path residual.** A customer that got the engine from `get_or_open` but stamps a whole sweep cycle later, after a close. That window is separate from the Dekker race; `close_scheduler_engine`'s doc accepts it under the replay contract.
- **The stale R28 wording** in `close_scheduler_engine`'s doc ("touch counter is re-checked"). This is a doc-only follow-up; leave it to step B to avoid churn in a ceilinged file.
- **Making `ADOPTION_SEQ` per-engine** (rejected: uniqueness across incarnations of one prefix is what makes a stale `SweepSched` record unable to match a successor engine), or dropping the global.
- **Adding `SweepCustody` to `source_rules.PROOFS`** (compile-fail fixtures). It is not a listed proof-bearing type, and touching `scripts/quality/` would flip `tooling` and select every invariant leg.
- **Adding `src/billing/sweep_custody` to `LIFECYCLE_PREFIXES`.** The `loom` plan flag is not consumed by CI, and the Loom test runs in the ordinary suite regardless.

## 9. Decisions for Søren

No product/raw edge, status, body, `/v1/debug` or `/metrics` change: nothing needs an edge decision. Three reviewed-decision notes:

1. **Hot-path ordering.** `stamp_external` runs on every external engine resolution.
   - Recommended (reviewer's contract): SeqCst on all handshake accesses. The x86_64 cost is one extra locked XCHG per resolution, on a cache line the existing swap already owns.
   - Loom-verified alternative with zero x86 codegen change: the stamp's `store(L)` Relaxed, keeping the swaps SeqCst (the §2.3 proof uses only the swap's release).
2. **The `ShardEngine::start` `unwrap_used` reason is re-decided** (text in C1.4). The exception ratchet requires this as the reviewed decision, because any edit to the engine literal changes its fingerprint. Behaviour is unchanged.
3. **New mutation owner `sweep_custody`.** It is voluntary: the file is outside `CRITICAL_PREFIXES`. It adds about 20 mutants to this push's rust-quality run and one owner to the nightly rotation. The alternative is to leave it unregistered and rely on the Loom and unit tests alone.

---

## Skeptic corrections (C1..C9)

Checked against the tree at 33fbd10e. I did not run any cargo command or repo script. I also read the Loom 0.7.2 source in `~/.cargo/registry`.

**Confirmed as written:**
- Every quoted line in §1.1 and §1.3 (billing.rs:1752-1807, 1571-1577, 1839-1853, 1873-1910; shard_directory.rs:246-256 and 278-285).
- The use-site list in §1.4, grepped across all of `src/`, `tools/`, `fuzz/`, `scripts/` and `docs/`. The only other hits are:
  - `legacy-diagnostics*.json` (a genesis capture; `stamp_external` keeps its path);
  - the `ADOPTION_SEQ` rows in `source-allowances.json`/`legacy-source.json` (the static stays token-identical);
  - `test-inventory*.json`/`test-relocations.json`, which carry names only; only `test-inventory.json` carries hashes.
- wc -l: billing.rs 2,201, shard.rs 3,196, runtime_sweep.rs 922, livefeed_swap.rs 936, shard_directory.rs 650.
- No `#[expect]` covers any edited billing.rs fn. The expects at 113/764/768/1381/1980 cover other items.
- The five unwraps in `ShardEngine::start` are at 1507/1544/1654/1674/1743. The fingerprint mechanics are source_rules.py:161-186 plus quality-syntax scan.rs:242-253 (a call-site value is the whole token stream). A changed reason is a new identity (source_rules.py:204-208). The proposed reason passes the `"[^";]+;[^";]+;[^";]+"` check at :258.
- Loom 0.7.2 behaviour:
  - `seq_cst()` is a no-op (rt/thread.rs:336-341).
  - An RMW reads only the newest store (atomic.rs:781-787) and carries the read store's sync, which gives release sequences (atomic.rs:530-536).
  - The SeqCst-load filter (atomic.rs:766) excludes an older store only when BOTH stores are SeqCst, and the initial store is `seq_cst: false` (atomic.rs:861).
  - So the C1 red, the C2 green, and the C3/C4 failures all hold as traced.
  - `max_threads = 2` admits main plus one spawn (rt/thread.rs:215, `len() < max`).
  - Loom's `AtomicU64: Default` exists (sync/atomic/int.rs:169).
- The mutant count holds: 7 trait delegations + 1 + 4 + 2 + 3 + 3 = 20, plus 1 unviable `start` FnValue mutant.

**C1 — The comparison base is stale.** `origin/slate` is now **33fbd10e**, not 2f2c3015 (`git rev-parse origin/slate`). The push diff will NOT carry the http/http_debug/http_telemetry_append owners. 33fbd10e did not touch billing.rs, shard.rs or either DST file (`git show --stat 33fbd10e`), so every ceiling in §4 stands. Changes:
- Delete the "if 33fbd10e is still unpushed" paragraph in §5.
- C10 now expects exactly `Mutation verification executed 21 selected mutant(s) across 2 registered owner(s).`
- C9 uses `QUALITY_BEFORE_SHA=33fbd10e`.

**C2 — Adding the owner row flips `tooling`, which §8 itself cites as a cost and §5/§6/§9 do not account for.** `scripts/quality/verification_plan.py:75-77` sets `tooling` for any path under `scripts/quality/`. That makes `properties_fuzz`, `loom` and `miri` true (:98-99). rust-quality.yml:79-94 then also runs the "Saved production-decoder corpus" leg and the "Compatible retained-buffer tests under Miri" leg on this push.
- §8 rejects the PROOFS entry for exactly this reason, so §9 decision 3 must state it.
- C9's expected receipt must add `miri: true, properties_fuzz: true, loom: true`. `loom` is true in any case, because `src/shard.rs` matches LIFECYCLE prefix `src/shard`.
- The alternative in decision 3 is to drop the voluntary row. Then no `scripts/quality/` churn happens, but `sweep_custody.rs` never gets a mutation run.

**C3 — Control C6 cannot pass as written, and its second half proves nothing.**
- The first grep will also print `src/dst/tests/livefeed_swap.rs:790` (the comment "LiveFeed build (last_external_seq > 0)"). Add a line-neutral rewording of lines 789-791 to C1.5, e.g. "(`externally_resolved()`)", or change the expected output.
- The second grep is line-based. Every DST read is split over lines (runtime_sweep.rs:497-499, 564-565, 572-574, 894-896; livefeed_swap.rs:801-803, 815-817), so it can never match them. The proof is the compiler: `engine.sweep_custody.load(..)` becomes E0599 (no method `load` on `SweepCustody`) and `.custody`/`.last_external_seq` become E0616. Say so, and keep the grep only as a smoke check.

**C4 — The §2.3 conclusion is misstated.** "In both cases, once a stamp has completed, custody is 0" is false at intermediate instants. In the "install's swap is second" branch, custody == seq between the install's `swap` and its `revoke_if`, even though the stamp has already returned. The true statement, which is also exactly what the Loom test asserts (after `join` and after `install` returns), is: **once both `stamp_external` and `install` have returned, custody is 0.** The transient is harmless, because:
- the only readers that decide are the same sweep task's `custody_intact`/`close_scheduler_engine`, which run sequentially after `mark` returns (billing.rs:1691-1709, 1935-1955);
- `scheduler_held` is a hint that can over-count by one inside the window.

The `SweepCustody` doc must use the corrected wording.

**C5 — §3.3 says the DST "assertions and messages are unchanged", but the proposed rewrites change assertion forms.**
- runtime_sweep.rs:893-898 is `assert_ne!(…, 0)`.
- runtime_sweep.rs:496-502 and :915-919 are `assert_eq!(…, 0, msg)`.
- Rewriting them to `assert!(held())`/`assert!(!held(), msg)` drops the left/right diagnostics.

Use the `#[cfg(test)] value()` spelling instead, so that only the read changes:
- `assert_eq!(engine.sweep_custody.value(), 0, "custody must not be installed over external history")`
- `assert_ne!(engine.sweep_custody.value(), 0)`
- `assert_eq!(now.sweep_custody.value(), 0, "scheduler must have dropped its claim")`
- livefeed_swap.rs:814-820 the same way.

The files still shrink. The livefeed :800-806 `externally_resolved()` rewrite is fine. §3.2 kills are unaffected, because `held()` keeps its production callers.

**C6 — Line-reference slip in §1.4.** `let custody0 = …` is runtime_sweep.rs:563-565. Line 566 is the `assert_ne!(custody0, 0, …)` that stays. C1.5 already says 563-565, so fix the table.

**C7 — The §2.3/§9 "cache line the existing swap already owns" claim is unfounded.**
- Before the change, the two `ShardEngine` fields were separately reordered by rustc.
- After it, `SweepCustody` is 16 bytes with 8-byte alignment inside a `ShardEngine` whose layout is unspecified, so it can straddle a 64-byte line.

State the cost as "one extra locked XCHG per external resolution, on a word adjacent to the one the swap already writes". Do not claim a shared line.

**C8 — The red output's `<line>:<col>` must be filled in** from the written tests.rs before the plan counts as "exact". You can also strengthen the trace with a second failing schedule that needs no mid-install preemption:
1. The stamper runs to completion right after the spawn.
2. The model thread's early `load(L)` can still read the initial 0, because the stamper's store is not in its causality (`match_load_to_stores`, atomic.rs:716-770).
3. The same store(C=2) + stale re-check then follows.

The first message is the same either way.

**C9 — Missing check to record: the architecture-gate budget exception for `start`.** `function:src/shard.rs::start` (architecture-policy.json:40-46, limit 399) must stay *needed*, or the gate reports `obsolete budget exception`. The fixed baseline measures `start` at 383 (docs/refactor/architecture-review-baseline.json). The edit takes it from ≈391 to ≈390, which stays above 383 and at or below 399, so there is no failure. Add this to §6/C8 as verified, not assumed.

**Also note, not blocking:** `CustodyWord` is a new production trait that exists only so Loom can instantiate the same code. There is no precedent for it: every other Loom model wraps a plain state type in `loom::sync::Mutex` (touch/loom_tests.rs:18, shard/commit_handoff/loom_tests.rs:6, billing/read_accumulator/tests.rs:158). RUST-QUALITY's "no gratuitous abstractions" bar makes it worth listing as §9 decision 4, with the reason in the trait doc (done). The alternative is `cfg(loom)`, rejected in §2.2.

**Verdict: ready-with-corrections.** The bug, the fix (every custody write is an RMW, with SeqCst handshake accesses), the Loom red/green, the mutation analysis (20 killed + 1 unviable), the exception-ratchet remedy and the ceilings all hold. Before execution:
- fix control C6 (C3) and the §2.3 wording (C4);
- account for the tooling-flip CI legs (C2);
- update the base SHA (C1).
