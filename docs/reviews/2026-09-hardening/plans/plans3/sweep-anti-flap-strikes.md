# Sweep evictions earn anti-flap strikes (review rank 12) — implementation plan

Repo: /Users/sorenschmidt/code/streams, branch slate @ 82095942. Verified first-hand on
2026-09-23; every line number below is from that tree.

**Verdict on the claim: CORRECT, not stale.** The line numbers in the review drifted by a
few lines (fd810688 touched sharddir.rs) but the mechanism is exactly as described.

## 1. Mechanism, with evidence

### 1.1 The gate's strike ledger judges every retirement as if the engine had died

`src/sharddir.rs:269-278`:

```rust
fn arm_holdoff_locked(st: &mut HashMap<String, PrefixGate>, prefix: &str) {
    let g = st.entry(prefix.to_string()).or_default();
    let lifetime = g.opened_at.map(|t| t.elapsed());
    g.opened_at = None;
    match lifetime {
        Some(l) if l >= SHORT_LIVED => g.strikes = 0,          // SHORT_LIVED = 30 s (:51)
        _ => g.strikes = g.strikes.saturating_add(1),
    }
    g.holdoff_until = Some(Instant::now() + holdoff_for(g.strikes));
}
fn holdoff_for(strikes: u32) -> Duration {                      // :280-283
    let mult = 1u32 << strikes.min(5); // 3s,6s,12s,24s,48s,96s→cap
    (HOLDOFF_BASE * mult).min(HOLDOFF_CAP)                       // 3 s base, 60 s cap (:47-48)
}
```

Two callers, both judged identically:

- `OpenGate::notify_closed` (`:718-740`, the engine's OWN close: fenced or fatal store fault)
  → `arm_holdoff_locked(&mut st, prefix)` at `:734`.
- `OpenGate::retire_resident(prefix, decide)` (`:759-778`, every explicit retirement)
  → `arm_holdoff_locked(&mut st, prefix)` at `:775`. **It has no `reason` parameter.**

`ShardDirectory::retire(prefix, reason: RetirementReason, decide)` (`src/shard_directory.rs:433-455`)
receives the reason, logs it at `:452` (`tracing::info!(shard, reason = reason.as_str(), "shard retired")`)
and calls `self.inner.gate.retire_resident(prefix, decide)` at `:445` — the reason never
reaches the gate. `RetirementReason` (`:51-70`) has exactly four variants:
`OwnershipMoved`, `FleetEviction`, `SweepEviction`, `Shutdown`.

### 1.2 The sweep is an open-probe-close loop measured in seconds

`src/billing.rs:1689-1815 sweep_owned_outboxes`, phase 2 (`:1759-1810`): for each cold owned
prefix (up to `sweep_discovery_max`, default 8, `src/config/model.rs:446`),
`open_or_wait(prefix, 20 s)` → `mark` → `probe_debt` → if no debt, `close_scheduler_engine`
(`:1808`). The tombstone walk does the same (`walk_engine_budgeted :2020-2040` →
`walk_settle :2044-2057` → `close_scheduler_engine :2054`). `close_scheduler_engine`
(`:1972-2010`) is:

```rust
match state.shards.retire(prefix, crate::shard_directory::RetirementReason::SweepEviction,
                          |engine, _incarnation| engine.sweep_custody.compare_exchange(seq, 0, ..).is_ok()) {
```

A debt-free probe engine lives for one `probe_debt` — well under `SHORT_LIVED` — so every
sweep close is a strike.

### 1.3 Strikes never decay on this path, and the sweep cadence outlasts the cap

- The only reset is `lifetime >= SHORT_LIVED` inside `arm_holdoff_locked`; `publish_open`
  (`src/sharddir.rs:913-952`) sets `opened_at` and clears `holdoff_until` but **does not**
  touch `strikes` (`:937-938`).
- The sweep runs at start and every `outbox_sweep_secs` = 300 s (`src/billing.rs:1181-1186`,
  `src/config/model.rs:441`). 300 s > 60 s cap, so the holdoff has always expired before the
  next sweep re-discovers the shard: nothing ever blocks the next strike.
- Escalation per sweep cycle of a cold, debt-free shard: 6 s, 12 s, 24 s, 48 s, 60 s, 60 s …
  (`holdoff_for(1..)`; note the FIRST young close already gives 6 s, the 3 s base is
  `holdoff_for(0)`).

### 1.4 Customers see it

`ShardDirectory::resolve` (`src/shard_directory.rs:237-297`) → `get_or_open` →
`wait_retired` (`src/sharddir.rs:680-706`): `holdoff_until > now` ⇒
`OpenOutcome::Wait { code: "shard_moving", retry_after_secs: (until - now).as_secs().max(1) }`
⇒ `ResolveError::Opening` ⇒ 503 + `Retry-After` in the HTTP adapter (`src/http.rs:566-600`).
At steady state a cold shard is unreachable for up to 60 s of every 300 s (20 % of wall
time) for the first customer request after each sweep cycle. A customer-opened engine is
never sweep-closed (custody install declines when `last_external_seq != 0`,
`src/billing.rs:1885-1906`), so the damage is confined to genuinely cold shards — which is
exactly where a first request lands.

### 1.5 What the holdoff on a sweep close actually protects

The close→reopen race is already serialised by `PrefixGate::closing` (`:248`): `get_or_open`
refuses with `shard_closing` while the retired engine has not terminated (`:503-510`), and
`wait_retired` waits for it (`:697-704`). The holdoff after a sweep close is therefore pure
anti-flap policy — and a release the runtime chose is not a flap. PR 6.1.1-B's invariant
("retirement arms A holdoff", pinned by
`retirement_arms_the_holdoff_and_a_stale_close_cannot_evict_a_replacement`,
`src/dst/tests/runtime_retirement.rs:86-164`, which asserts `shard_moving` after a
`FleetEviction` AND after a `Shutdown` retirement) must stay true; only the ESCALATION is
wrong.

## 2. Design

Policy (as proposed in the item, adopted unchanged):

| Departure | Strike ledger | Holdoff armed |
| --- | --- | --- |
| engine died on its own (`notify_closed`) | young ⇒ +1; lived ≥ 30 s ⇒ reset to 0 | `holdoff_for(strikes)` (unchanged) |
| `Retired(OwnershipMoved)`, `Retired(FleetEviction)` | same as above (unchanged) | `holdoff_for(strikes)` (unchanged) |
| `Retired(SweepEviction)`, `Retired(Shutdown)` | **untouched** (never grown, never reset) | **`HOLDOFF_BASE` (3 s)** |
| failed / deadlined open (`get_or_open :583-584, :607-608`) | +1 (unchanged, not touched by this change) | unchanged |

`Shutdown` is neutral because `ShardDirectory::shutdown` (`:381-412`) calls `gate.stop()`
first (no open can start anyway) and the rigs `clear_holdoff` after `retire(Shutdown)`
(`fixture_http.rs:738-753`, `runtime_sweep.rs`, …): the only observable effect is that a rig's
restart boundary no longer accrues strikes. "Do not reset strikes on sweep close" is kept
as instructed: a sweep close is evidence of nothing, in either direction. (Consequence to
be aware of: a prefix carrying strikes from an earlier flap keeps them across sweep cycles
until a JUDGED long-lived close resets them; its next young judged close escalates one
step further than today. That is the correct reading of "no evidence".)

The reason must be judged **under the same guards that removed the resident**
(`retire_resident` holds gate state + serving map), so it is a parameter of
`retire_resident`, not something the directory computes afterwards.

### 2.1 Smallest correct design (in place)

Everything in `src/sharddir.rs`: `use crate::shard_directory::RetirementReason;`, a private
`enum Departure { Died, Retired(RetirementReason) }` with `fn is_evidence(self) -> bool`
(the ONE exhaustive match), a pure `fn ledger_after(departure, lived: Option<Duration>,
strikes) -> (u32, Duration)`, `arm_holdoff_locked(st, prefix, departure)`,
`retire_resident(prefix, reason, decide)`; tests in a new `#![cfg(test)]`
`src/sharddir/strike_policy_tests.rs`. Line budget: 959 → **998** (block 262-283 grows 22 → 56,
+1 use, +2 mod decl, +1 param, +1 module-doc line). It fits, with 2 lines of slack, and the
file is the gate/reaper hot spot that fd810688 touched this very session. The next edit
there pays for a move anyway.

### 2.2 Owner-first design (recommended)

Same production semantics, but the verdict policy and its pure tests get their own child
module `src/sharddir/holdoff.rs` (≈ 45 production lines + tests), registered as a mutation
owner (`owner('sharddir_holdoff', 'src/sharddir/holdoff.rs', 'sharddir::')`). The constants,
`holdoff_for` and `arm_holdoff_locked` STAY in `sharddir.rs` so `get_or_open` (five
`#[expect]`s, the reaper spawn fingerprinted whole) is not touched at all: only the callee
of `arm_holdoff_locked` changes. `sharddir.rs` lands at **958** lines (959 − 22 + 15 + 3 + 1 + 1 + 1).

Both designs re-decide the same two `#[expect]` reasons (`notify_closed`, `retire_resident`,
§4.1) and neither touches `get_or_open`, `billing.rs` (ceilinged, 0 lines) or `fleet.rs`
(ceilinged, 0 lines). Recommendation: **2.2**. It is not a pass-through: the gate calls the
policy under its lock and the module owns the one decision plus the boundary predicate the
mutation leg needs explicit inputs for. If Søren prefers a single-file diff, 2.1 is
line-for-line the same code inside `sharddir.rs` at 998/1000.

Rejected: (a) a fifth `RetirementReason::EngineClosed` variant (an engine's own close is not
an explicit retirement; `retire`'s doc contract is "every explicit removal"); (b) moving
`RetirementReason` into `sharddir.rs` (its `Shutdown` variant carries a legacy
`allow(dead_code)` inventoried at `docs/quality/owners.json:135-149` and in
`legacy-diagnostics-linux.json`; moving it is ledger churn for nothing — reference it
instead, `shard_directory` already imports from `sharddir` and the architecture gate only
tracks edges to `http`/`product`, `scripts/architecture-gate.py:33-50`); (c) `arm_holdoff_locked(.., evidence: bool)`
(within the 1-bool threshold but `arm_holdoff_locked(&mut st, prefix, true)` says nothing);
(d) no holdoff at all on a sweep close (breaks the pinned 6.1.1-B test at
`runtime_retirement.rs:157-163` which asserts `shard_moving` after `retire(Shutdown)`; the
`closing` handle would make it safe, so it is a legitimate follow-up, not this fix).

## 3. Red tests (near-complete Rust)

Sequencing: commit R adds `src/sharddir/holdoff.rs` containing ONLY `#[cfg(test)] mod tests`
with tests A and C (they use the unchanged `ShardDirectory::retire(prefix, reason, decide)`
API and read gate state directly, so they compile on 82095942), the `mod holdoff;` line in
`sharddir.rs`, and DST test B. Run → A and B fail as stated below, C passes (the contrast).
Commit F adds the production items to `holdoff.rs`, test D, the owner row and the docs.
(One push; CI gates the push diff against its `before` revision, so R need not be green
on its own — but do not push R alone.)

Helpers shared by A, C, D live at the top of `holdoff.rs`'s test module. The real-engine
opener is the same shape as `sharddir/unwind.rs:71-91` (the DST fixture
`dst/tests/fixture_storage.rs::open_engine` is `pub(super)`, unreachable from here).

### 3.1 `src/sharddir/holdoff.rs` — test module (final form)

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::{Departure, ledger_after};                                   // added in commit F
    use crate::shard_directory::{OpenTiming, RetireOutcome, RetirementReason, ShardDirectory};
    use crate::sharddir::{EngineIncarnation, HOLDOFF_BASE, OpenFn, OpenOutcome, SHORT_LIVED, holdoff_for};

    const PREFIX: &str = "0";
    /// Bounded: a reopen that never becomes Ready fails by assertion, never hangs.
    const OPEN_WAIT: Duration = Duration::from_secs(10);

    /// A real engine over an in-memory store (`unwind::tests` keeps the same shape).
    async fn open_engine(prefix: &str) -> Arc<crate::shard::ShardEngine> {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let db = slatedb::Db::builder(prefix, store.clone())
            .build()
            .await
            .expect("open db");
        let (absorb_tx, _absorb_rx) = crate::history::absorber_channel();
        let maintenance = crate::shard::load_or_rebuild_maintenance(&db)
            .await
            .expect("load maintenance");
        crate::shard::ShardEngine::start(
            prefix.to_string(),
            Arc::new(db),
            store,
            crate::shard::ShardConfig::default(),
            absorb_tx,
            None,
            maintenance,
        )
    }

    /// A directory whose opener produces real engines: retirement runs the real
    /// close path and a reopen waits for the real termination.
    fn directory() -> ShardDirectory {
        let opener: OpenFn = Box::new(|prefix: String, _inc: EngineIncarnation| {
            Box::pin(async move { Ok(open_engine(&prefix).await) })
        });
        ShardDirectory::new(
            vec![PREFIX.to_string()],
            crate::ownership::OwnershipService::new(""),
            OpenTiming {
                open_deadline: Duration::from_secs(60),
                open_wait: Duration::from_millis(50),
            },
            |_notifier| opener,
        )
    }

    async fn open(dir: &ShardDirectory) -> EngineIncarnation {
        match dir.open_or_wait(PREFIX, OPEN_WAIT).await {
            OpenOutcome::Ready(_) => dir.resident_incarnation(PREFIX).expect("resident"),
            OpenOutcome::Wait { code, .. } => panic!("open must be ready, got Wait({code})"),
            OpenOutcome::Failed(e) => panic!("open failed: {e}"),
        }
    }

    /// The prefix's ledger as the gate holds it: strikes and the holdoff left.
    fn ledger(dir: &ShardDirectory) -> (u32, Option<Duration>) {
        let gate = dir.gate_for_tests();
        let st = gate.inner.st.lock().unwrap();
        let g = st.get(PREFIX).expect("gate state for the prefix");
        let left = g
            .holdoff_until
            .map(|until| until.saturating_duration_since(Instant::now()));
        (g.strikes, left)
    }

    /// Forget the holdoff and ONLY the holdoff (`clear_holdoff` also zeroes the
    /// strikes), so a reopen is allowed at once while the ledger is exactly what
    /// the previous departure left.
    fn forget_holdoff(dir: &ShardDirectory) {
        let gate = dir.gate_for_tests();
        if let Some(g) = gate.inner.st.lock().unwrap().get_mut(PREFIX) {
            g.holdoff_until = None;
        }
    }

    fn retired(dir: &ShardDirectory, reason: RetirementReason) {
        assert!(
            matches!(dir.retire(PREFIX, reason, |_, _| true), RetireOutcome::Retired(_)),
            "{reason:?}: the resident was there to retire"
        );
    }

    // ---- test A (RED on 82095942) -------------------------------------------

    /// Review rank 12: the billing sweep opens a cold shard, probes it and closes
    /// it within seconds, every cycle. Judged like an engine that died young,
    /// every cycle earned a strike, strikes never decayed, and cold shards
    /// drifted to the 60 s ceiling. A release the runtime chose is not evidence:
    /// the base holdoff, and the ledger exactly as it was.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_sweep_eviction_or_shutdown_leaves_the_strike_ledger_alone() {
        let dir = directory();
        let cycles = [
            RetirementReason::SweepEviction,
            RetirementReason::Shutdown,
            RetirementReason::SweepEviction,
        ];
        for (cycle, reason) in cycles.into_iter().enumerate() {
            open(&dir).await;
            retired(&dir, reason);
            let (strikes, left) = ledger(&dir);
            assert_eq!(strikes, 0, "cycle {cycle}: {reason:?} is not a strike");
            let left = left.expect("a retirement still arms the holdoff (PR 6.1.1-B)");
            assert!(
                left <= HOLDOFF_BASE,
                "cycle {cycle}: {reason:?} arms the base holdoff, not an escalated one ({left:?})"
            );
            assert!(matches!(
                dir.open_or_wait(PREFIX, Duration::from_millis(20)).await,
                OpenOutcome::Wait { code: "shard_moving", .. }
            ));
            forget_holdoff(&dir);
        }
    }

    // ---- test C (passes today; the contrast, and the killer of `is_evidence → false`,
    //      the guard mutants and `notify_closed → true/false`) -------------------

    /// The storm signature still escalates: an engine that dies, or that the ring
    /// moves or the fleet evicts while young, is a strike every time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_young_engine_that_dies_or_is_moved_away_still_strikes() {
        let dir = directory();
        let inc = open(&dir).await;
        let gate = dir.gate_for_tests();
        assert!(gate.notify_closed(PREFIX, inc), "the live incarnation evicts itself");
        assert!(!dir.is_open(PREFIX), "an engine that died is gone from the map");
        assert!(!gate.notify_closed(PREFIX, inc), "a stale close is a no-op");
        assert_eq!(ledger(&dir).0, 1, "an engine that died young is a strike");
        forget_holdoff(&dir);
        for (expected, reason) in [
            (2, RetirementReason::OwnershipMoved),
            (3, RetirementReason::FleetEviction),
        ] {
            open(&dir).await;
            retired(&dir, reason);
            let (strikes, left) = ledger(&dir);
            assert_eq!(strikes, expected, "{reason:?} of a young engine is a strike");
            // 12 s / 24 s minus microseconds: a > 9 s stall between two adjacent
            // statements would be needed to read this below the 3 s base.
            assert!(
                left.expect("armed") > HOLDOFF_BASE,
                "{reason:?}: an escalated holdoff, got {left:?}"
            );
            forget_holdoff(&dir);
        }
    }

    // ---- test D (arrives with commit F; the boundary over explicit inputs) -----

    /// The ledger over explicit inputs: the `SHORT_LIVED` line is a test input,
    /// never a wall-clock race. Every `cargo mutants` mutant of `holdoff.rs`
    /// dies here (§4.4).
    #[test]
    fn the_ledger_judges_evidence_by_lifetime_and_ignores_releases() {
        let died = Departure::Died;
        let moved = Departure::Retired(RetirementReason::OwnershipMoved);
        let evicted = Departure::Retired(RetirementReason::FleetEviction);
        let swept = Departure::Retired(RetirementReason::SweepEviction);
        let shutdown = Departure::Retired(RetirementReason::Shutdown);
        let one_ns = Duration::from_nanos(1);
        // Evidence: a young engine is a strike, escalating; a long-lived one resets.
        assert_eq!(ledger_after(died, None, 0), (1, holdoff_for(1)));
        assert_eq!(ledger_after(moved, Some(Duration::ZERO), 1), (2, holdoff_for(2)));
        assert_eq!(ledger_after(evicted, Some(SHORT_LIVED - one_ns), 4), (5, holdoff_for(5)));
        assert_eq!(ledger_after(died, Some(SHORT_LIVED), 4), (0, HOLDOFF_BASE));
        assert_eq!(ledger_after(moved, Some(SHORT_LIVED * 2), 1), (0, HOLDOFF_BASE));
        // Releases: the base holdoff, the ledger untouched — never reset, never grown.
        assert_eq!(ledger_after(swept, Some(Duration::ZERO), 0), (0, HOLDOFF_BASE));
        assert_eq!(ledger_after(swept, Some(Duration::ZERO), 3), (3, HOLDOFF_BASE));
        assert_eq!(ledger_after(shutdown, Some(SHORT_LIVED * 2), 3), (3, HOLDOFF_BASE));
        assert_eq!(ledger_after(swept, None, u32::MAX), (u32::MAX, HOLDOFF_BASE));
        assert_eq!(holdoff_for(5), Duration::from_secs(60), "the cap");
    }
}
```

Expected failure of **test A on current code** (cycle 0: `arm_holdoff_locked` sees
`opened_at = Some(t_open)`, lifetime ≈ ms < 30 s, strikes 0 → 1):

```
thread 'sharddir::holdoff::tests::a_sweep_eviction_or_shutdown_leaves_the_strike_ledger_alone' panicked:
assertion `left == right` failed: cycle 0: SweepEviction is not a strike
  left: 1
 right: 0
```

(Were the strike assertion removed, the next one fails too: `holdoff_for(1)` = 6 s ⇒
"cycle 0: SweepEviction arms the base holdoff, not an escalated one (5.99…s)".)

Test C on current code: passes (Died → 1, OwnershipMoved → 2, FleetEviction → 3). After the
fix: identical, by design. Test D does not exist on current code (`Departure`/`ledger_after`
are new); it is the boundary pin, not the red test.

### 3.2 `src/dst/tests/runtime_retirement.rs` — test B (RED; end-to-end through the directory and the real close path)

Append after `a_declined_retirement_keeps_the_engine_and_arms_nothing` (`:222`). Reuses the
file's `mem()` and `notifying_opener` (`:41-56`), so the retired engine's late close
callback exercises the idempotent `notify_closed` path exactly as production wires it.

```rust
/// Review rank 12: a sweep eviction is not a flap. The billing sweep opens a
/// cold shard, probes it and closes it again within seconds, every cycle.
/// 6.1.1-B judged that close exactly like an engine that died young: each
/// cycle earned a strike, strikes never decayed, and a cold shard drifted to
/// the 60 s ceiling, so a customer's first request after a sweep close met
/// `shard_moving` with a Retry-After of up to a minute. The reason now
/// reaches the gate: a sweep close arms the base holdoff and leaves the
/// strike ledger alone (the ledger itself is pinned in `sharddir::holdoff`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sweep_eviction_arms_only_the_base_holdoff() {
    let store = mem();
    let st = store.clone();
    let dir = crate::shard_directory::ShardDirectory::new(
        vec!["0".into(), "1".into()],
        crate::ownership::OwnershipService::new(""),
        crate::shard_directory::OpenTiming {
            open_deadline: std::time::Duration::from_secs(60),
            open_wait: std::time::Duration::from_millis(50),
        },
        |notifier| notifying_opener(st, notifier),
    );
    let prefix = "0";
    let crate::sharddir::OpenOutcome::Ready(_) = dir
        .open_or_wait(prefix, std::time::Duration::from_secs(30))
        .await
    else {
        panic!("the probe open must be ready");
    };
    // The sweep returns its probe engine within seconds of opening it.
    assert!(matches!(
        dir.retire(
            prefix,
            crate::shard_directory::RetirementReason::SweepEviction,
            |_, _| true
        ),
        crate::shard_directory::RetireOutcome::Retired(_)
    ));
    match dir
        .open_or_wait(prefix, std::time::Duration::from_millis(20))
        .await
    {
        crate::sharddir::OpenOutcome::Wait {
            code,
            retry_after_secs,
        } => {
            assert_eq!(code, "shard_moving", "a sweep close still arms the base holdoff");
            // Stall-safe: a slow test thread can only make this smaller.
            assert!(
                retry_after_secs <= 3,
                "a sweep eviction is not a strike: the holdoff must be the 3 s base, \
                 not an escalated one (Retry-After {retry_after_secs}s)"
            );
        }
        _ => panic!("a just-retired shard must not reopen immediately"),
    }
}
```

Expected failure on current code (holdoff = `holdoff_for(1)` = 6 s;
`retry_after_secs = (until - now).as_secs().max(1)` = 5, or 6 on a coarse clock):

```
a sweep eviction is not a strike: the holdoff must be the 3 s base, not an escalated one (Retry-After 5s)
```

After the fix: 3 s ⇒ `retry_after_secs` = 2 (or 3) ⇒ passes. Adding B requires
`python3 scripts/test-inventory.py --write` (§5). The two pinned tests in this file
(`retirement_arms_the_holdoff_… :86-164`, asserting `shard_moving` after `FleetEviction`
and after `Shutdown`; `a_declined_retirement_… :171-222`) stay green: FleetEviction is still
judged, Shutdown still arms the 3 s base, a declined decision still arms nothing.

## 4. Code change (near-complete Rust, per file)

### 4.1 `src/sharddir.rs` (959 → 958 lines; not ceilinged, but under 1,000 must hold)

Line budget: −22 (old block `:262-283`) + 15 (new `arm_holdoff_locked` + `holdoff_for`)
+ 3 (`mod holdoff;` with doc + `use holdoff::…`) + 1 (`use … RetirementReason`) + 1 (param in
`retire_resident`) + 1 (module-doc sentence) = **−1 ⇒ 958**. Verify with `wc -l`.

**(a) Imports** — after `:43 use crate::shard::ShardEngine;`:

```rust
use crate::shard_directory::RetirementReason;
```

**(b) Module doc** — `:34-36`, append one line to the "Escalating holdoff" bullet:

```rust
//!   A sick store gets a trickle of opens, not a storm. A release the runtime
//!   chose (the billing sweep, a shutdown) is not evidence: see `holdoff`.
```

**(c) Child module** — after `:77 mod unwind;`:

```rust
/// The anti-flap ledger's verdict on a departure: evidence, or a release.
mod holdoff;
use holdoff::{Departure, ledger_after};
```

**(d) Replace `:262-283`** (the doc, `arm_holdoff_locked` and `holdoff_for`) with:

```rust
/// Arm the anti-flap holdoff for `prefix` under an ALREADY-HELD gate
/// state. PR 6.1.2-A: it takes the guard, never `st`, so a caller holding
/// the gate state cannot reach for it in the forbidden order (`ServingMap`).
fn arm_holdoff_locked(st: &mut HashMap<String, PrefixGate>, prefix: &str, departure: Departure) {
    let g = st.entry(prefix.to_string()).or_default();
    let lived = g.opened_at.take().map(|t| t.elapsed());
    let (strikes, holdoff) = ledger_after(departure, lived, g.strikes);
    g.strikes = strikes;
    g.holdoff_until = Some(Instant::now() + holdoff);
}

fn holdoff_for(strikes: u32) -> Duration {
    let mult = 1u32 << strikes.min(5); // 3s,6s,12s,24s,48s,96s→cap
    (HOLDOFF_BASE * mult).min(HOLDOFF_CAP)
}
```

(`fn arm_holdoff_locked(…)` is 97 columns — under rustfmt's 100; `holdoff_for` is byte-identical
to today so `get_or_open`'s call/path fingerprints of it do not move.)

**(e) `notify_closed` `:734`** — one token change:

```rust
        arm_holdoff_locked(&mut st, prefix, Departure::Died);
```

Ratchet: `notify_closed` carries ONE `#[expect(clippy::unwrap_used, reason = …)]` (`:714-717`).
`scope_lines` is unchanged (same line count) but the ordinary-call fingerprint of the
`arm_holdoff_locked(…)` call and the new `Departure::Died` path fact are new
fingerprints under that scope ⇒ "accepted exception grew" unless the reason is
re-decided. **Re-decide** (`:716`) to:

```
OpenGate::notify_closed; a poisoned gate state or serving map may hold a half-recorded retirement, the resident matched under the same write guard is still present and the close it reports is judged as evidence under those same guards; recovering the former could retire the wrong incarnation and a fallible remove would deny a resident the guard just proved
```

(three `;`-separated parts, no `;` or `"` inside a part — `source_rules.py:258` regex.)

**(f) `retire_resident` `:742-778`** — doc, reason, signature, arming call:

```rust
    /// Retire the resident of `prefix`: remove it, arm the anti-flap
    /// holdoff as `reason` warrants (see `holdoff`), and hand the engine
    /// back for closing — as ONE decision that no request observer can see
    /// half-applied. `decide` sees the engine AND its incarnation and may
    /// decline, which reinstates the very same resident under the same guards.
    ///
    /// PR 6.1.2-A: retirement lives HERE, in the component that owns
    /// both pieces of state, so it can take them in the one permitted
    /// order — gate state, then serving map (see `ServingMap`). The
    /// directory used to hold the serving map and then reach for the
    /// gate state, which deadlocked against `get_or_open` across
    /// unrelated prefixes. Closing the engine is deliberately NOT done
    /// here: the caller does it after both guards are released.
    #[expect(
        clippy::unwrap_used,
        reason = "OpenGate::retire_resident; a poisoned gate state or serving map may hold a half-recorded open, retirement or holdoff and the stated reason is judged under the same guards that removed the resident; recovering either could serve, reopen or reap the wrong incarnation"
    )]
    pub(crate) fn retire_resident(
        &self,
        prefix: &str,
        reason: RetirementReason,
        decide: impl FnOnce(&Arc<ShardEngine>, EngineIncarnation) -> bool,
    ) -> Retirement {
        let mut st = self.inner.st.lock().unwrap();
        let mut map = self.inner.shards.write().unwrap();
        let Some(resident) = map.remove(prefix) else {
            return Retirement::Absent;
        };
        if !decide(&resident.engine, resident.incarnation) {
            map.insert(prefix.to_string(), resident);
            return Retirement::Kept;
        }
        // Armed while the slot is still held: the removal and its verdict
        // are one decision, never half-applied.
        arm_holdoff_locked(&mut st, prefix, Departure::Retired(reason));
        st.entry(prefix.to_string()).or_default().closing = Some(resident.engine.shutdown_handle());
        Retirement::Retired(resident.engine)
    }
```

Ratchet: ONE `#[expect(clippy::unwrap_used)]`; `scope_lines` grows by the parameter line and
the call/path fingerprints change ⇒ **re-decide** the reason to the text above (new identity
⇒ no comparison, `source_rules.py:204-208`). Both `.unwrap()` sites remain, so the
expectation stays fulfilled (`unfulfilled_lint_expectations` is denied). Four args incl.
`self` (threshold 5); no bool params.

**Untouched, deliberately:** `get_or_open` (`:461-670`, five `#[expect]`s incl. the whole
`tokio::spawn` reaper fingerprint), `wait_retired`, `publish_open`, `clear_holdoff` (test-only,
still zeroes strikes — the tests above bypass it on purpose), the inline failed/deadlined
strike code at `:583-584`/`:607-608`.

### 4.2 `src/sharddir/holdoff.rs` (NEW, ≈ 230 lines with tests; mutation owner)

```rust
//! The anti-flap ledger's verdict: which departures are EVIDENCE of a sick
//! prefix (a strike, escalating the holdoff) and which are releases the
//! runtime chose (the base holdoff, the ledger untouched).
//!
//! Review rank 12: the billing sweep opens a cold shard, probes it and
//! closes it within seconds, every cycle (`billing::sweep_owned_outboxes`,
//! `close_scheduler_engine`). PR 6.1.1-B routed that close through the same
//! arming body as an engine that died young, so each cycle earned a strike.
//! Strikes decay only when an engine outlives `SHORT_LIVED`, which a probe
//! engine never does, and the sweep's 300 s cadence always outlasts the 60 s
//! cap: every cold shard drifted to the ceiling, and a customer's first
//! request after a sweep close met 503 `shard_moving` with a Retry-After of
//! up to a minute.

use std::time::Duration;

use super::{HOLDOFF_BASE, SHORT_LIVED, holdoff_for};
use crate::shard_directory::RetirementReason;

/// How a resident left the serving map.
#[derive(Clone, Copy, Debug)]
pub(super) enum Departure {
    /// The engine reported its own close: fenced by a new owner, or a fatal
    /// store fault (`OpenGate::notify_closed`).
    Died,
    /// A caller retired it for the reason it stated (`OpenGate::retire_resident`).
    Retired(RetirementReason),
}

impl Departure {
    /// The ONE strike decision. Only evidence of a sick prefix escalates: an
    /// engine that died, or that the ring moved or the fleet evicted while
    /// young — rapid open→die cycles ARE the storm the gate exists to prevent.
    /// A release the runtime chose says nothing about the store.
    fn is_evidence(self) -> bool {
        use RetirementReason::{FleetEviction, OwnershipMoved, Shutdown, SweepEviction};
        match self {
            Departure::Died | Departure::Retired(OwnershipMoved | FleetEviction) => true,
            Departure::Retired(SweepEviction | Shutdown) => false,
        }
    }
}

/// The ledger after a departure: the strikes the prefix carries and the
/// holdoff before its next open. Pure over explicit inputs, so the
/// `SHORT_LIVED` line is a test input and never a wall-clock race.
pub(super) fn ledger_after(
    departure: Departure,
    lived: Option<Duration>,
    strikes: u32,
) -> (u32, Duration) {
    if !departure.is_evidence() {
        return (strikes, HOLDOFF_BASE);
    }
    match lived {
        Some(l) if l >= SHORT_LIVED => (0, HOLDOFF_BASE),
        _ => {
            let strikes = strikes.saturating_add(1);
            (strikes, holdoff_for(strikes))
        }
    }
}

#[cfg(test)]
mod tests { /* §3.1 */ }
```

Notes: `pub(super)` (no `unreachable_pub` exposure); explicit imports only (no glob ⇒ no
`unresolved-glob` row); no macros beyond `assert*`/`matches!`/`panic!` (all in
`EXPRESSION_MACROS`, `source_rules.py:15-19`), no statics, no spawns ⇒ **no `owners.json`
row**. The in-function `use RetirementReason::{…}` is a uniform-path import (edition 2024).
No `#[expect]` anywhere in the file. `match lived { Some(l) if …, _ => … }` — the `_` is on
`Option`, and `wildcard_enum_match_arm` is not in sharddir's scope overlay
(`legacy-diagnostics-linux.json` `scope_overlay` lists only commit_handoff.rs and sse/feed.rs);
the enum match in `is_evidence` is exhaustive with no wildcard regardless.

### 4.3 `src/shard_directory.rs` (637 → ≈ 641 lines)

- `:445`: `let engine = match self.inner.gate.retire_resident(prefix, reason, decide) {`
  (`reason` is `Copy`; `reason.as_str()` at `:452` still works).
- `:48-50` — the stray first doc line (`/// The fate of a `remove_if` decision …`) belongs to
  `RetireOutcome`; replace the enum doc with:

```rust
/// Why a shard is being retired. Recorded on the retirement so the reason a
/// prefix went cold is visible where the decision was made, and judged by
/// the gate's anti-flap ledger: an engine moved or evicted while young is a
/// strike; a sweep release or a shutdown is not (`sharddir::holdoff`).
```

- `:414-418` (`retire` doc): "…remove exactly the resident the decision was made about, arm
  the anti-flap holdoff **as the reason warrants**, and close the engine."

Ratchet: `retire` has no `#[expect]`. `resolve` (`unwrap_used`) and `shutdown`
(`excessive_nesting`) are untouched — they call `self.retire(..)`, whose signature does not
change. No new test is needed here for mutation (the in-diff mutant of `retire` is
`Default::default()` for `RetireOutcome`, which is unviable — `fnvalue.rs:257`; unviable is
not a miss, `exit_code.rs`). `shard_directory::directory_tests` keep passing unchanged.

### 4.4 Mutation-kill map (owner `sharddir` + new owner `sharddir_holdoff`, filter `sharddir::`)

cargo-mutants 27.1.0 in-diff selects every mutant whose span intersects a changed line
(`in_diff.rs`), i.e. the function-body mutant of each touched function plus operator /
guard / arm mutants inside them (`visit.rs:572-700`):

| Mutant | Killed by |
| --- | --- |
| `Departure::is_evidence` → `true` (sweep is evidence) | A (strikes 1 ≠ 0), D (`swept,…,3` → `(4, 48 s)`) |
| `is_evidence` → `false` (nothing is evidence) | C (Died → 0 ≠ 1), D (`died, None, 0` → `(0, 3 s)`) |
| `ledger_after` body → `(0, Duration::default())` / `(1, Duration::default())` | D (every holdoff is ≥ 3 s) |
| delete `!` in `!departure.is_evidence()` | D (swept judged, died neutral) |
| guard `l >= SHORT_LIVED` → `true` / `false` | D (`died, None, 0` / `died, Some(SHORT_LIVED), 4`) |
| `>=` → `<` (the only replacement cargo-mutants generates for `Ge`) | D (`Some(SHORT_LIVED)` and `Some(SHORT_LIVED − 1 ns)`) |
| `arm_holdoff_locked` body → `()` | A (`holdoff_until` None ⇒ `expect("…arms the holdoff")`), C |
| `Instant::now() + holdoff` → `-` | A/B (holdoff in the past ⇒ Ready, not `Wait{shard_moving}`) |
| `notify_closed` → `true` / `false` (in-diff via `:734`) | C (`!dir.is_open` / first assert) |
| `retire_resident` / `ShardDirectory::retire` body → `Default::default()` | unviable (enum without Default), not a miss |

Method calls (`saturating_add`, `take`, `map`) are never mutated (`visit.rs:412-423`).
Timeout budget: `--timeout 90` per mutant covers the whole `sharddir::` selection; A and C
add six real-engine open/close cycles (≈ 1–3 s total); every wait is bounded (10 s) so a
broken mutant fails by assertion, never by hang.

### 4.5 Ledger rows

- `scripts/quality/mutation_owners.py` — after `:75`:
  `owner('sharddir_holdoff', 'src/sharddir/holdoff.rs', 'sharddir::'),`
  (required: `src/sharddir/` is under the `src/shard` lifecycle prefix,
  `verification_plan.py:26`; `validate_plan` fails before discovery on an unregistered
  critical file, `mutation_owners.py:277-281`; `test_mutation_owners.py:103` only checks name
  uniqueness; the seven-night bucket hash does not reshuffle existing rows).

## 5. Every ledger / doc row to update

| File | Action |
| --- | --- |
| `scripts/quality/mutation_owners.py` | add the `sharddir_holdoff` row (§4.5) |
| `docs/refactor/test-inventory.json` | `python3 scripts/test-inventory.py --write` after adding test B (CI runs `--check`, `.github/workflows/ci.yml:56`; the full-suite floor in `tests_ran.py` derives from it) |
| `docs/quality/owners.json` | **no row**: no glob import, no by-path module, no spawn/static/macro DSL in the new code |
| `docs/quality/source-allowances.json`, `legacy-*.json`, `docs/quality/policy.json` | untouched (no new warning; no bounded line exception needed at 958) |
| `docs/refactor/review-mechanisms.json` | untouched: the pinned fixtures are `fixture_http.rs`/`fixture_livefeed.rs` helpers and unrelated unit tests; nothing in `runtime_retirement.rs` is pinned |
| `docs/refactor/WIRE-MATRIX.md` | **no change**: the wire shape (503 `shard_moving` + `Retry-After`, `:41`, `:170`) is unchanged; only the Retry-After VALUE after a sweep/shutdown close drops from 6–60 s to 2–3 s. If a note is wanted, add "(escalates only for engines that die, are moved or evicted while young — `sharddir::holdoff`)" to the `shard_moving` entry at `:41` |
| `docs/LIVE-FEED.md:117` | unchanged ("reopens after the anti-flap holdoff" remains true) |
| `docs/SOAK-REGIONS.md:253,524,542` | optional one-line note that releases the runtime chose no longer escalate |
| `scripts/quality/verification_plan.py` | nothing; the loom leg is selected by the `src/shard` prefix (existing loom tests only; no loom model touches the holdoff), miri/property legs are not |

Commit checklist (the implementer runs these; this plan was produced read-only):
`wc -l src/sharddir.rs src/shard_directory.rs src/sharddir/holdoff.rs src/dst/tests/runtime_retirement.rs`;
`cargo fmt --all -- --check`; `cargo test --lib sharddir::holdoff` (red at R, green at F);
`cargo test --lib shard_directory::`; `cargo test --lib runtime_retirement`;
`python3 scripts/test-inventory.py --write`; `scripts/quality.sh`;
`python3 scripts/quality/verification_plan.py --out /tmp/vp` and confirm `selected_mutation_owners`
= `[shard_directory, sharddir, sharddir_holdoff]`; `python3 scripts/quality/mutation_driver.py`
(the CI plan, before push — memory trap). Never claim CI green without `gh run view`.

## 6. What could go wrong

1. **Wire compatibility** — none. Same codes, same header. Clients honouring `Retry-After`
   simply retry sooner after a sweep close. `shard_opening`/`shard_closing` paths unchanged.
2. **Fleet skew** — none. The ledger is per-process memory; mixed versions differ only in how
   long a cold shard is held off after a sweep close. No protocol, no store format.
3. **Sick-store protection is intact** — failed and deadlined opens still strike in
   `get_or_open` (untouched), an engine that dies young still strikes (`Departure::Died`), an
   ownership flap still escalates (`OwnershipMoved`/`FleetEviction`). A sweep whose opens
   FAIL therefore still backs off exponentially; only a sweep whose opens SUCCEED stops
   being punished for succeeding.
4. **Strikes are no longer reset by a long-lived sweep close** (neutral in both directions).
   A prefix with historic strikes keeps them until a judged long-lived close; its next young
   judged close escalates one step further than today. Per the item's instruction; state it
   in the commit message.
5. **Flakiness** — the holdoff is `std::time::Instant` (not tokio-paused). Assertions are
   chosen accordingly: strike counts are deterministic; "≤ base" can only be helped by a
   stall; "> base" after a 12 s/24 s arming needs a > 9 s stall between adjacent statements.
   Every reopen waits ≤ 10 s and fails by assertion. Test C's `notify_closed` before the
   engine is retired relies on `begin_close` inside `notify_closed` — the same pattern as
   `open_gate_escalates_holdoff_for_engines_that_die_young` (`runtime_open_gate.rs:566-568`).
6. **Ratchet surprises** — the two re-decided reasons must keep the exact 3-part format; a
   fourth `;` or a `"` fails `source_rules.py:258`. Do not touch any line inside
   `get_or_open` (its `tokio::spawn` fingerprint includes the whole reaper body). Do not
   touch `sharddir/unwind.rs` (its test asserts `g.strikes == 1` after a panicked open —
   still true, the failed-open path is untouched).
7. **Line ceiling** — `sharddir.rs` must end ≤ 1000: budget says 958 (design 2.2) / 998 (2.1);
   `wc -l` before every push. `billing.rs` and `fleet.rs` (both ceilinged) are not touched.
8. **Registration** — forgetting the `sharddir_holdoff` row fails the mutation leg before
   discovery; forgetting `test-inventory.py --write` fails `--check`.
9. **Rustfmt** — two signatures sit at 97 columns; if rustfmt wraps them the line budget
   moves by +3/+4, still under 1000 in design 2.2.
10. **Follow-up, not this fix** — dropping the holdoff entirely for `SweepEviction`
    (the `closing` handle already serialises close→reopen). It needs the pinned 6.1.1-B test
    re-decided (`runtime_retirement.rs:157-163` asserts `shard_moving` after `Shutdown`), so
    it is a separate, reviewed decision.

## Skeptic corrections

Checked read-only against slate @ 82095942 on 2026-09-23. Verdict: **sound with
corrections**. The mechanism, every cited line, every signature and the ratchet analysis
hold; the corrections below are about the red commit's hygiene, the mutation leg's real
kill set, one fragile design option and a few claims that need a sharper statement.

### S1. Verified first-hand (no change needed)

- `src/sharddir.rs:269-278` `arm_holdoff_locked(st, prefix)` and `:280-283` `holdoff_for`;
  `:734` (notify_closed) and `:775` (retire_resident) are its only two callers.
  `retire_resident` (`:759-778`) has exactly ONE caller in the tree:
  `src/shard_directory.rs:445`. Production reasons: `billing.rs:1994` SweepEviction,
  `fleet.rs:840` OwnershipMoved, `fleet.rs:1048` FleetEviction, `shard_directory.rs:263`
  OwnershipMoved, `:384` Shutdown. No other producer/consumer of the changed signature.
- `strikes` is read nowhere outside `sharddir.rs` (+ `sharddir/unwind.rs:168` test, failed-open
  path, untouched). `stats_json` does not expose it. No DST test asserts escalation after a
  SweepEviction/Shutdown retirement (grep `retry_after_secs [><=]|strikes|escalat` over
  `src/dst`): only `runtime_open_gate.rs:520-585` (dies-young via `notify_closed`, still
  judged) and `shard_directory.rs:589` (`>= 1`). Nothing pinned breaks.
- Ratchet (`scripts/quality/source_rules.py`): identity = `(path, qualified, kind, full
  attribute text)` (`:196`); `exception_growth` skips identities absent from the merge base
  (`:207-208`), so a re-decided reason IS a fresh contract. The format check (`:258`) is
  `re.search(r'"[^";]+;[^";]+;[^";]+"', value)` — exactly two `;` between the quotes; both
  proposed reasons have exactly two. Reasoned exceptions are skipped by the allowance
  inventory (`:263`), so no `source-allowances.json` row. Confirmed.
- File ceilings: `source_rules.violations:226-228` gives `sharddir.rs` a limit of
  `min(max(1000, legacy 979), max(1000, merge-base 959))` = **1000**; the architecture gate
  (`scripts/architecture-gate.py:120-123`) uses `max(1000, baseline lines)` too. 958 and 998
  are both legal — but see S3.
- Privacy of the test helpers: `sharddir::holdoff::tests` is a descendant of `sharddir`, so
  `gate.inner.st`, `PrefixGate.strikes/holdoff_until`, `HOLDOFF_BASE`, `SHORT_LIVED` and
  `holdoff_for` are reachable without any visibility change — same mechanism
  `src/sharddir/unwind.rs:157-173` already relies on (`gate.inner.st.lock()`, `g.strikes`).
- `clippy::match_like_matches_macro` (style, warn ⇒ `-D warnings` error) does NOT fire on
  `is_evidence`: the lint requires the LAST arm to be a wildcard, and the plan's last arm is
  `Departure::Retired(SweepEviction | Shutdown)`. Keep it that way — do not "simplify" the
  match into `matches!` and do not turn the last arm into `_`.
- `clippy::items_after_test_module` only fires for a module literally named `tests`: in
  `holdoff.rs` the `#[cfg(test)] mod tests` MUST stay the last item (the plan has it last).
  `sharddir.rs`'s test modules have other names, which is why `publish_open` may follow them.
- `wildcard_enum_match_arm` overlay (`legacy-diagnostics-linux.json` `scope_overlay`) covers
  only `read_batch.rs`, `read_budget.rs`, `postings.rs`, `commit_handoff.rs`, `record.rs`,
  `sse/feed.rs`. The `_` on `Option` in `ledger_after` is fine.
- Legacy diagnostics for `crate::RetirementReason::Shutdown` (`legacy-diagnostics-linux.json:
  801-806`, `allow_attributes_without_reason`) are fingerprinted from the PRIMARY SPAN's
  excerpt + owner + message (`scripts/quality/diagnostics.py:78-96`), not line numbers. The
  plan's doc edit at `shard_directory.rs:48-50` does not touch `:62-68`, so the fingerprint
  survives the +1 line shift. Do NOT touch `:62-68` in this fix.
- `docs/refactor/test-inventory.json` covers `src/dst/**` only (`scripts/test-inventory.py:
  131`), so the `holdoff.rs` unit tests are not inventoried; only test B needs `--write`.
  `review-mechanisms.json` pins nothing in `runtime_retirement.rs` or `sharddir*`. Existing
  `runtime_retirement.rs` entries carry `scenarios: []`, so B needs no scenario mapping.
- `mutation_owners.py`: `src/sharddir/holdoff.rs` starts with `src/shard`
  (`verification_plan.py:26` LIFECYCLE_PREFIXES ⇒ CRITICAL), so `validate_plan` fails
  before discovery without the row (`mutation_owners.py:277-281`). The filter `sharddir::`
  matches `sharddir::holdoff::tests::*` (cargo substring filter). Confirmed.
- `ShardDirectory::shutdown` is production code (`src/bootstrap.rs:913-916`), so
  `RetirementReason::Shutdown` IS constructed in non-test builds and its
  `cfg_attr(not(test), allow(dead_code))` is vestigial. Irrelevant to this fix — but do not
  remove it here: dropping it orphans the legacy diagnostics row above and needs
  `gate.py --prune` in a separate commit.
- The mt-lint `name-keyed-map` rule (`src/mt_lint.rs:84-90,145`) scans struct fields and
  statics; `arm_holdoff_locked`'s `&mut HashMap<String, PrefixGate>` parameter is unchanged
  from today and is not a field. No marker needed.

### S2. Commit R must compile warning-free on its own (import hygiene)

`§3.1` imports `SHORT_LIVED` and `holdoff_for` in the test module, but both are used only by
test D (commit F) and `Departure`/`ledger_after` only exist in F. In commit R those two are
`unused_imports` warnings — harmless for `cargo test`, but `scripts/quality.sh` runs clippy
with `-D warnings`, so R would not be a clean commit. Corrected R-form of the imports in
`src/sharddir/holdoff.rs`:

```rust
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::shard_directory::{OpenTiming, RetireOutcome, RetirementReason, ShardDirectory};
    use crate::sharddir::{EngineIncarnation, HOLDOFF_BASE, OpenFn, OpenOutcome};
```

Commit F then adds `use super::{Departure, ledger_after};` and extends the `sharddir` line
with `SHORT_LIVED, holdoff_for` (let `cargo fmt` fix the brace ordering; do not hand-sort).
Also put the `owner('sharddir_holdoff', 'src/sharddir/holdoff.rs', 'sharddir::')` row in R,
not F: `validate_sources` (`mutation_owners.py:296-303`) requires a registered path to exist,
and the file exists from R on — registering in the commit that creates the file keeps R
self-consistent if anyone ever gates it alone.

### S3. Drop design 2.1 entirely; only 2.2 is safe under the ceiling

`§6.9` says "if rustfmt wraps them the line budget moves by +3/+4, still under 1000 in
design 2.2" — correct for 2.2 (958 → 962), but 2.1 sits at 998 and the same wrap lands at
1001-1002 > 1000 ⇒ `file growth: src/sharddir.rs` (`source_rules.py:229-230`) AND
`file budget growth` in the architecture gate. `arm_holdoff_locked`'s new signature is
97 columns (counted: `fn arm_holdoff_locked(` 22 + `st: &mut HashMap<String, PrefixGate>, `
38 + `prefix: &str, ` 14 + `departure: Departure) {` 23), so it fits today, but 2.1 has zero
slack for any later doc line. Recommend striking 2.1 from the plan rather than offering it.

### S4. The mutation leg's kill set: B is out of scope, C is load-bearing

- `§4.4` credits "A/B" for `Instant::now() + holdoff → -`. Test B lives in
  `dst_tests::runtime_retirement::…`, which matches NEITHER selected owner's filter
  (`sharddir::` for `sharddir`/`sharddir_holdoff`, `shard_directory::` for `shard_directory`),
  so B never runs under `cargo mutants`. A alone must kill it, and it does: with the holdoff
  in the past, `wait_retired` (`sharddir.rs:687-694`) skips the `shard_moving` branch and
  either waits ≤ 20 ms on the closing engine (⇒ `shard_closing`) or falls through to a fresh
  open (⇒ `shard_opening`/`Ready`); none matches `Wait { code: "shard_moving", .. }`. State
  "A" alone in the table so nobody later removes A believing B covers the leg.
- `notify_closed → true` / `→ false` become in-diff mutants via `:734` and TODAY have no
  killer under the `sharddir::` filter: the only callers are the notifier
  (`shard_directory.rs:153`) and `runtime_open_gate.rs:568` (a `dst_tests::` test). Test C is
  therefore not "the contrast" — it is the ONLY thing standing between this change and a
  missed mutant (`→ false` dies on C's first `assert!`, `→ true` on `!dir.is_open`). Say so
  in §4.4 and keep both asserts exactly as written.
- `holdoff_for` is meant to stay out of the diff (byte-identical, shown as context lines).
  If `git diff` pairs the replaced 22-line block so that `holdoff_for`'s lines print as
  removed+added, cargo-mutants (in-diff keys on ADDED new-file lines) will select
  `1u32 << … → >>` (viable, ⇒ 0 s) and `HOLDOFF_BASE * mult → /` (viable, ⇒ ~93 ms);
  `* → +` is unviable (`Duration + u32`). D's `holdoff_for(5) == 60 s` and C's
  `left > HOLDOFF_BASE` kill both, so nothing is lost — but confirm with
  `cargo mutants --list --json --in-diff <pr.diff> --file src/sharddir.rs` before pushing
  and record the count in the commit message.
- `retire_resident` / `ShardDirectory::retire` body mutants (`Default::default()` for
  `Retirement`/`RetireOutcome`, neither implements `Default`) are unviable; cargo-mutants
  27.1.0 exits 0 on unviable (only missed ⇒ 2, timeout ⇒ 3) and the driver uses
  `check=True` on that exit code (`mutation_driver.py:152`). Confirmed, not a miss.

### S5. Reopen patience in the unit tests: use 30 s, not 10 s

`OPEN_WAIT = 10 s` bounds the wait for the PREVIOUS incarnation's termination
(`wait_retired` → `EngineShutdown::wait`, `src/shard/lifecycle.rs:105-108`), which includes a
5 s worker grace (`lifecycle.rs:10 WORKER_GRACE`) plus two db closes. The DST tests that do
the same reopen use 30 s (`runtime_retirement.rs:102,132`), and the mutation leg runs the
`sharddir::` set under `--profile quality` (opt-level 1, `Cargo.toml:77-81`) with `--jobs 1`
on a box also running the other legs. 30 s is still a bound (a hung reopen fails by
`panic!("open must be ready, got Wait(shard_closing)")`, never hangs), and it stays far
inside the 90 s per-mutant timeout because it is only ever consumed on a failure path:

```rust
    /// Bounded: a reopen that never becomes Ready fails by assertion, never hangs.
    const OPEN_WAIT: Duration = Duration::from_secs(30);
```

### S6. Expected red output — confirmed, with the exact shapes

- Test A, current code, cycle 0: `retire_resident` → `arm_holdoff_locked` sees
  `opened_at = Some`, lifetime ≈ ms < 30 s ⇒ `strikes = 1`, holdoff 6 s. Panics at the
  `assert_eq!(strikes, 0, …)`:
  ```
  assertion `left == right` failed: cycle 0: SweepEviction is not a strike
    left: 1
   right: 0
  ```
  (`assert_eq!` with a format message prints exactly this on the pinned toolchain.)
- Test B, current code: `retry_after_secs = (6 s − ε).as_secs().max(1) = 5` ⇒
  `a sweep eviction is not a strike: the holdoff must be the 3 s base, not an escalated one (Retry-After 5s)`.
  After the fix: `(3 s − ε).as_secs()` = 2 ⇒ passes. A stall can only lower both values.
- Test C passes on the current tree (Died → 1, OwnershipMoved → 2, FleetEviction → 3) and
  after the fix; D does not compile before F (no `Departure`) and is the boundary pin.
- Type inference nits that DO compile as written: `(2, RetirementReason::OwnershipMoved)`
  infers `u32` from `assert_eq!(strikes, expected)`; `(1, holdoff_for(1))` infers
  `(u32, Duration)`; `Wait { code: "shard_moving", .. }` is a valid `&'static str` pattern.

### S7. Two doc lines that go stale under the new policy (line-neutral, optional)

`src/sharddir.rs:45-46` and `:49-51` still describe the base holdoff as "after a fence-close
or failed open" and SHORT_LIVED as unconditional. If touched, keep the line counts:

```rust
/// Base holdoff after a fence-close, a failed open or a release the runtime
/// chose (sweep, shutdown); doubling per JUDGED strike up to [`HOLDOFF_CAP`].
const HOLDOFF_BASE: Duration = Duration::from_secs(3);
const HOLDOFF_CAP: Duration = Duration::from_secs(60);
/// A JUDGED departure younger than this counts as a strike; surviving
/// longer resets the escalation. Releases are never judged (`holdoff`).
const SHORT_LIVED: Duration = Duration::from_secs(30);
```

Neither line sits under any `#[expect]` scope, so this is ratchet-free. The `notify_closed`
doc (`:708-713`) stays accurate unchanged. `[`HOLDOFF_CAP`]` is an existing intra-doc link
to a private const that already passes `cargo doc --document-private-items -D warnings`.

### S8. Smaller precision fixes to the text

- `§1.1` says `retire_resident` "has no `reason` parameter" — correct; add that the plan's
  new parameter order `(prefix, reason, decide)` mirrors `ShardDirectory::retire`
  (`shard_directory.rs:433-438`), so the one call site is a positional insert.
- `§2` table, row "failed / deadlined open": these two arming sites (`sharddir.rs:583-584`,
  `:607-608`) never go through `arm_holdoff_locked`/`ledger_after`; the doc phrase "the one
  arming body" at `:266` is already inaccurate today and the plan's replacement doc drops
  it — good, keep it dropped.
- `§4.3`: the enum-doc replacement fixes the stray first line at `:48` (which belongs to
  `RetireOutcome`); note that `RetireOutcome`'s own doc at `:83` is then still missing that
  sentence. Line-neutral option: move `/// The fate of a `remove_if` decision, settled under
  ONE write guard.` to sit above `pub(crate) enum RetireOutcome` at `:83` (+1 there, −1 at
  `:48`), or leave it out — either is fine, but do not leave the stray line on
  `RetirementReason`.
- `§5` "loom leg": `lifecycle = True` because both `src/sharddir.rs` and
  `src/shard_directory.rs` start with `src/shard`; the leg runs existing loom tests only
  (`grep cfg(loom)` over `sharddir*`/`shard_directory.rs` is empty). No loom model touches
  the ledger; the policy's "retirement changes ⇒ Loom" row is satisfied by the existing
  suite because no lock order or state transition changes — say that explicitly in the
  commit message so the reviewer does not ask.
- Optional simplification the reviewer may prefer: `is_evidence` can spell the variants
  in full (`Departure::Retired(RetirementReason::OwnershipMoved | RetirementReason::FleetEviction)`)
  and drop the in-function `use`; +1 line, zero semantic difference. Either form is
  lint-clean (no `enum_glob_use`, pedantic is off).

### S9. Not needed, confirmed

`docs/refactor/WIRE-MATRIX.md:41,170` — no change (same codes, header; the 503 `shard_moving`
`retry-after` value was already computed, the matrix's "(`retry-after: 1`)" is a pre-existing
approximation). `docs/LIVE-FEED.md:117` — unchanged. `docs/quality/owners.json` — no row
(no glob, no `#[path]`, no spawn/static/macro DSL; the sibling `watchdog_policy_tests.rs`
needed one only for its `use super::*`). `docs/quality/policy.json` immutables untouched.
No fleet-skew or wire break: per-process memory only.
