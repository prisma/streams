# Item 40: the heartbeat gets its own supervised period, separate from the fleet tick

Tree: `slate`. The task text said HEAD c397e9e5 had 8 unpushed commits. They have since been pushed:
`git rev-parse origin/slate HEAD` now prints c397e9e5 twice, so the merge base for every
ratchet below is c397e9e5. `git diff origin/slate HEAD` is empty. `src/fleet.rs` is 1,142 lines
there, which is its ceiling.

**Wait for the running mutation leg to finish before starting.** The controls rebuild the lib and
run DST rigs, and that CPU load is enough to time out mutants.

## 1. Problem (checked against the current tree)

### 1a. The heartbeat PUT is the first step of one serial tick (REAL)

All of it lives in `start` in `src/fleet.rs` (the reviewer's line numbers are stale by about 7).

- The period is the tick's own sleep plus the whole tick: `:491-495`
  `'ticks: loop { tokio::select! { … _ = tokio::time::sleep(Duration::from_secs(2)) => {} }`,
  then `:499` `let pass_deadline = tokio::time::Instant::now() + Duration::from_secs(45);`.
  Every `fleet_io!` step (`:500-509`) runs under that deadline, and a missed deadline hits
  `continue 'ticks`.
- Step 1 is `:529` `// 1. Heartbeat (single writer per object: plain PUT).`. The sampler at
  `:510-527` and `:530-586` builds the document. `:587-590` publishes it:
  ```rust
  if let Err(e) = fleet_io!(repository.publish_heartbeat(&cfg.instance, &hb)) {
      tracing::warn!("heartbeat put failed: {e}");
      continue;
  }
  ```
  A failed or slow heartbeat therefore skips the whole tick. That is the reviewer's "`continue`
  at :580-583".
- These store operations follow the heartbeat in the same pass. Each is bounded by
  `DOCUMENT_DEADLINE` = 10 s (`repository.rs:35`), and the populations by their own 10 s
  (`repository.rs:152`):
  1. `:608` `read_heartbeat_set` (LIST `fleet/`, then up to 4,099 GETs, 8 at a time)
  2. `:650` `read_router_reports` (LIST + GETs)
  3. `:735` `read_desired_state` (GET)
  4. `:763` `read_published_urls` (GET)
  5. `:783` `read_overrides` (GET)
  6. `:862-881` up to 16 eager shard opens, 4 at a time, 1 s wait each
  7. `:954` return-home CAS on `overrides.json`
  8. `:1028` move CAS on `overrides.json`
  9. `:1108` desired CAS

  That is the "~9 store ops". Three required reads also `continue` on error (`:609-611`,
  `:737-739`, `:785-787`), and each time the next heartbeat waits for the next tick.
- Consequence: the heartbeat interval is 2 s plus the pass. One document that stalls until its
  10 s deadline (overrides, desired or the population) makes the interval at least 12 s. A pass
  that runs to its deadline makes it up to 47 s.

### 1b. Heartbeats are judged by bare 10 s and 30 s literals (REAL)

- Live set, load, peer URL table and rebalance targets, `fleet.rs:615`:
  `if now_ms() - other.ts_ms < 10_000 && !other.draining {`.
- Ring membership, `planning.rs:17`:
  `name.as_str() == instance || ages.get(*name).is_some_and(|age| *age < 30_000)`.
- Neither is expressed in terms of the 2 s period. The documentation states the relationship
  (`COMPUTE-SPEC.md:118`: "< 10 s old (5 missed beats)"; `RUNBOOK.md:346` and
  `fleet.rs:3`: "every 2 s"), but the code only honours it while a tick finishes almost
  immediately.

### 1c. A store brownout drops peers and makes instances fence each other (REAL; follows from 1a and 1b, not run)

When the pass passes 8 s, every instance publishes less often than every 10 s. Every peer then
loses it from `live`, `peer_load` and heartbeat-sourced `peer_urls` (`:615-630`) for part of each
cycle. When the pass passes 28 s, ages cross 30 s. Each instance's `active_members` (`:754`) then
keeps only itself by name (`planning.rs:17`), the ring gives it every shard, and routed requests
open those shards and fence their real owners. The reverse happens when beats resume. The pilot
router mirrors the same windows (`src/bin/pilot/lb.rs:312`, `:331`), so it re-routes on the same
stale data. Ages are also computed after the population read completes (`:614`), which adds up
to 10 s more (see §8).

### 1d. Use sites (grepped: src incl. src/dst and cfg(test), tools/, fuzz/, bench/, scripts/, .github/)

| what | sites |
|---|---|
| `Heartbeat` struct | defined `fleet.rs:25-90`; built `fleet.rs:568`; `repository.rs:19,57,73`; `dst/tests/fleet_controller.rs:419`; the operator view `repository.rs:277-297` → `operator.rs:94,119` (`/operator/data.json`) |
| `publish_heartbeat` | `repository.rs:54`; `fleet.rs:587`; `repository/document_tests.rs:371` |
| sampler inputs | `admission.fleet_ops()` `fleet.rs:510` (also `ops.rs:394`); `admission.swap_peak()` `fleet.rs:565` (also `http.rs:811` debug_load, which also resets the peak); `store_timing::heartbeat_summary()` `fleet.rs:567` only; `cpu_time_secs` `fleet.rs:318,476,520` only; `rss_bytes` `fleet.rs:249,576`, `bootstrap/rss.rs:20,38`, `http.rs:853`, `operator.rs:109`; engine `timings` `fleet.rs:545` (writer `shard.rs:3001`) |
| tick-local lag and wedge | `hb.absorb_lag_max_secs`/`hb.wedge_max_ms` `fleet.rs:970-972`; `wedge_prefix`/`wedge_max_ms` `fleet.rs:1003-1004` |
| 10 s heartbeat window | `fleet.rs:615`; mirrors: `src/bin/pilot/lb.rs:312`, `bench/fleet/observe-fleet.py:67`. Not heartbeat windows: `fleet.rs:218` (router reports), `lb.rs:111` (router upstream stats) |
| 30 s ring window | `planning.rs:17`; mirror `lb.rs:331` |
| "heartbeat is published before the tick's reads" assumption | `fleet.rs:751` comment "Self is always fresh (just wrote)"; `fleet_controller.rs:414-422` (`heartbeat_stamp` doc: "a newer stamp proves the previous tick ran to its end") and its only use, the witness at `:464-474` |
| fleet loop starters | `bootstrap.rs:802`; `fleet_controller.rs:149,190,310,445,527,578`; `runtime_isolation.rs:322` (then `sleep(2600ms)` "One tick is 2s; give both loops a heartbeat") |
| held heartbeat test | r09 case `("fleet/streams-1.json", true)` `fleet_controller.rs:104`, checked as task `"fleet"` at `:176`; pinned in `review-mechanisms.json:703-706` (sha 3d48ff56…), `test-inventory.json`, `test-additions.json:261` (historical; no script reads it), prose `docs/review-controller-evidence.md:18` |
| task names in edge surfaces | `/v1/debug/load` `tasks.loops` (`http.rs:991-1000`); `/health` and `/readyz` 503 `critical task terminated: <name>` (`tasks.rs:269-281`, `http.rs:1634`) |
| real-fleet harnesses | `bench/fleet/livefeed-cert.mjs` (CI job `livefeed-fleet-cert`, `ci.yml:183`). Its SIGSTOP leg stops the whole process, which the change does not affect. No tools/, fuzz/, tests/ or conformance/ hits. |

The reviewer's "lag_hot_ticks as a duration" item: `fleet.rs:472,973-981` counts consecutive
ticks with lag. That count only needs to change if the tick stops reading its own pressure. This
plan keeps that reading, so the count is not a problem here (§8).

## 2. Contract decision

- **Heartbeat document**: same struct, same serde and the same `fleet/<instance>.json` path, so
  there is no shape change.
  - `rps`, `cpu_pct` and `inflight_peak` now cover exactly one beat (2 s), which is what the field
    docs at `fleet.rs:35-49` already say. Before, they covered 2 s plus the tick.
  - `owned_shards` order becomes `engines_by_prefix()` order. It was HashMap order before.
  - The first beat still goes out 2 s after the loop starts, as before.
- **Windows**: the values do not change. 10 s = 5 beats and 30 s = 15 beats, now named
  `LIVE_WINDOW_MS` and `RING_WINDOW_MS` as multiples of `PERIOD_MS`. The pilot router and
  `observe-fleet.py` keep their literal copies, which still agree.
- **Tick**: it no longer publishes, waits on or skips for the heartbeat. Its rebalancer reads this
  instance's pressure (absorb lag, worst wedge) itself at the tick, exactly as before. It does not
  read the published document or a shared channel.

  Consequence: this instance's own load now enters `live` and totals only through its stored
  heartbeat, the same way every peer's does. If its heartbeat PUT keeps failing, its tick
  excludes it from the load. Peers already did, and before this change its tick abstained
  entirely.
- **Edge**: one new supervised Critical loop, `fleet-heartbeat`. It shows in `/v1/debug/load`
  `tasks.loops` with the same row shape, and if it ever exits, `/health` and `/readyz` answer
  `503 critical task terminated: fleet-heartbeat`. This is decision 1 in §9, and the push waits
  for it.
- **Changes from the reviewer's proposal, and why:**
  - **No watch channel.** The tick reads its own `Pressure` at the tick. A watch would add a
    cross-task synchronization primitive, which triggers the Loom obligation. It would also let
    two ticks read one sample, and that is the very problem the reviewer's `lag_hot_ticks` change
    was meant to fix.
  - **`lag_hot_ticks` stays a count.** Two ticks still mean two independent readings.
  - **`Heartbeat`, `rss_bytes` and `cpu_time_secs` stay in fleet.rs.** Moving them verbatim would
    put the FFI probes' arithmetic into the in-diff mutation scope, and nothing can kill those
    mutants. It would also change `start`'s alias fingerprints. Removing the heartbeat from `start`
    already shrinks fleet.rs by 80 lines, so no separate move commit is needed to make room.

## 3. Red tests (and pinning tests)

Every test below compiles on the current tree. For the red capture, apply only the
fleet_controller.rs edits of commit A (§4) and run:

```
cargo test --locked --lib -- dst_tests::fleet_controller:: 2>&1 | tee "$SCRATCH/red-40a.log"
```

Expected: `test result: FAILED. 4 passed; 3 failed; …`. Green on the current tree:
`a_lagging_owner_moves_its_shard_only_to_an_active_member`,
`an_override_the_ring_ignores_is_never_opened_by_its_target` (with the new witness),
`an_unreadable_router_report_defers_only_the_desired_publication`,
`the_rings_owner_opens_every_overridden_shard_it_is_assigned_at_the_tick`.

### R1 `dst::dst_tests::fleet_controller::a_held_fleet_document_does_not_stop_the_heartbeat` (new, fleet_controller.rs)

Setup: `held(&inner, "fleet/overrides.json", false)` with the gate open (parks). Rig
`streams-1`. `start_configured`. `settled(5 s)` until `entered > 0`, then
`assert_eq!(entered, 1, "the tick must reach the held overrides read")`.

Window: `at_entry = published_stamp(&inner, "streams-1")`. For 9 s, every 100 ms, collect stamps
`> at_entry` into a `BTreeSet`. Then:
`assert_eq!(entered, 1, "the tick must stay parked in the held read for the whole window")` and
`assert!(stamps.len() >= 3, "the heartbeat must keep its period while the tick is held: {} new stamps in 9 s", stamps.len())`.

Trace on the current tree:
1. The tick wakes at about 2 s and PUTs the heartbeat, which gives `at_entry` = that stamp.
2. It reads the heartbeat set, routers, desired and urls.
3. It parks in the overrides GET (entered = 1). `read_bytes`'s 10 s timeout fires at entry + 10 s,
   `:787` `continue`s, and the next heartbeat PUT comes at about entry + 12 s. The window ends at
   entry + 9 s.
4. So `entered` is still 1, the first assert passes, and no new stamp exists.

Expected red:
```
thread 'dst::dst_tests::fleet_controller::a_held_fleet_document_does_not_stop_the_heartbeat' panicked at src/dst/tests/fleet_controller.rs:<line of the assert!>:5:
the heartbeat must keep its period while the tick is held: 0 new stamps in 9 s
```

Green: beats land at about 2, 4, 6, 8, 10 s after start, so there are 4 new stamps in the window
(1 beat of slack), while the tick stays parked until entry + 10 s.

Teardown: `shutdown(3 s)` with `aborted.is_empty()`. The tick cancels through `fleet_io!`.

### R2 `dst::dst_tests::fleet_controller::a_held_heartbeat_does_not_stop_the_tick` (new)

Setup: `held(&inner, "fleet/streams-1.json", true)`. Rig `streams-1`, then
`set_view(vec!["prior-owner".into()], HashMap::new())`, then `start_configured`.
`settled(5 s)` until `entered > 0`, then `settled(6 s)` until `ring_active() == ["streams-1"]`.

Asserts: `assert_eq!(entered, 1, "the heartbeat PUT must still be held")`, then
`assert_eq!(rig.state.ownership.ring_active(), ring, "the tick must publish the ring while its heartbeat PUT is held")`.
Teardown: `shutdown(3 s)` with `aborted.is_empty()`, message
`"a held heartbeat PUT must cancel cooperatively: {report:?}"`.

Trace on the current tree: the tick's `:587` PUT parks from about 2 s. The 10 s deadline, then
`continue`, then a 2 s sleep put the next PUT at about 14 s. So entered stays 1 through the check
at about 8 s, and `set_view` (`:798`) is never reached.

Expected red:
```
assertion `left == right` failed: the tick must publish the ring while its heartbeat PUT is held
  left: ["prior-owner"]
 right: ["streams-1"]
```

### R3 `r09_fleet_cancels_entered_documents_without_partial_authority_or_lost_retry` (modified; red because it now names the new task)

- Case rows become `(path, write, holder)` with holders `"fleet-heartbeat"`, `"fleet"` and
  `"fleet"`.
- The prior-view assertion runs for `path == "fleet/overrides.json"` only. It said "unread
  overrides must not publish a new ring", and for the heartbeat case R2 now asserts the opposite.
- The outcome check uses `*name == holder`.

On the current tree, case 1's PUT is entered inside task `fleet`, so the check fails. Expected red:
`assertion failed: report.outcomes.iter().any(|(name, outcome)| *name == holder && *outcome == crate::tasks::TaskOutcome::Finished)`
(the stringified expression; the prefix up to `*name == holder` is stable).

### Pinning tests and non-vacuity controls

- **P1** `an_override_the_ring_ignores_is_never_opened_by_its_target` (modified witness). "A
  second tick must run" is now proven by `held(&inner, "fleet/overrides.json", false)` with its
  gate closed, which counts without parking: `settled(10 s)` until `entered >= 2`, then
  `assert!(… >= 2, "a second tick must run")`. The tick reads overrides once per pass, before
  its eager open, so a second read means the first pass ran to its end. After this change a
  heartbeat stamp no longer proves that.
  - Green before and after.
  - Non-vacuity: temporarily make the eager filter at `fleet.rs:852` admit
    `ov.entries[p.as_str()].to == cfg.instance` (the pre-6515a15d predicate). Expected failure,
    as recorded in 6515a15d:
    `an override the ring ignores must never open the shard on its target` with
    `left: Number(1)` and `right: Number(0)`. Revert.
- **Commit B** is a pure refactor with these pins:
  - `fleet::tests::the_active_ring_is_self_and_every_ordinal_peer_under_fifteen_missed_beats`
    and `fleet::tests::published_urls_keep_only_bare_origins`, added first and green on the
    post-A tree before B's production edit.
  - `fleet::heartbeat::tests::a_peer_is_live_under_five_missed_beats_and_in_the_ring_under_fifteen`
    (boundaries 9,999/10,000 and 29,999/30,000, a future stamp, and a draining peer).
  - Non-vacuity, run each and then revert:
    1. `LIVE_WINDOW_MS = 6 * PERIOD_MS` fails with `assertion failed: !live(&beat(10_000, false), now)`.
    2. `RING_WINDOW_MS = 14 * PERIOD_MS` fails with `assertion failed: in_ring(29_999)` and, for
       active_members, `left: ["streams-1"]`, `right: ["streams-1", "streams-2"]`.
    3. Partitioning on `!valid_peer_url` fails the URL test with
       `left: {"streams-2": "https://b.example/path"}`.
- **Synchronization**: no new primitive is added. The two tasks share only `AppState` and the
  store. R1, R2 and r09 are the held-operation tests (real held GET and PUT), so no Loom model is
  needed.

## 4. Edits, file by file, in commit order

Two commits. **Push them together** so both are compared against c397e9e5 (§5 explains).

### Commit A: "The heartbeat has its own task and period: a held fleet document no longer stops it, and a held heartbeat no longer stops the tick"

**`src/fleet/heartbeat.rs`** (new, about 235 lines; the limit is 1,000):

```rust
//! This instance's heartbeat, `fleet/<instance>.json`, published by its own
//! supervised task once per period.
//!
//! Peers judge an instance by the age of this one document: under 10 s it
//! counts toward their live set and fleet load, under 30 s it keeps its place
//! in their ring. The heartbeat used to be the first step of the fleet tick,
//! so its period was the whole tick: up to nine more store operations under a
//! 45 s pass deadline, and a failed read skipped to the next tick. One slow
//! coordination document aged every instance out of every peer's live set,
//! and a brownout that stretched the tick past 30 s dropped instances from
//! each other's rings, so each served, and fenced, shards another still held.
//! On its own task a beat costs one PUT.
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{Heartbeat, cpu_time_secs, rss_bytes};
use crate::http::AppState;
use crate::shard::now_ms;
use crate::tasks::{Cancellation, Policy, TaskResult, TaskSupervisor};

/// How often this instance publishes its heartbeat.
const PERIOD: Duration = Duration::from_secs(2);
/// Commits older than this no longer describe the durable-write cost; the
/// store's WAL-PUT summary uses the same window.
const ACK_WINDOW_MS: i64 = 15_000;

/// Start this runtime's heartbeat beside its fleet tick, on the runtime's
/// own repository.
#[expect(
    clippy::let_underscore_must_use,
    reason = "heartbeat::start; the supervisor refuses a spawn only while the runtime is stopping, when no beat is owed; a refused publisher has nothing left to publish"
)]
pub(super) fn start(state: Arc<AppState>, instance: String, tasks: &TaskSupervisor) {
    let _ = tasks.spawn("fleet-heartbeat", Policy::Critical, move |cancel| {
        run(state, instance, cancel)
    });
}

/// Publish until cancelled. A slow or failed PUT delays only the next beat.
async fn run(state: Arc<AppState>, instance: String, cancel: Cancellation) -> TaskResult {
    let repository = state.fleet.clone();
    let mut sampler = Sampler::new(&state);
    let mut beats = tokio::time::interval(PERIOD);
    beats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick is immediate; the first beat waits one period so its
    // rates cover one.
    beats.tick().await;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return TaskResult::Done,
            _ = beats.tick() => {}
        }
        let heartbeat = sampler.beat(&state, &instance);
        let published = tokio::select! {
            _ = cancel.cancelled() => return TaskResult::Done,
            published = repository.publish_heartbeat(&instance, &heartbeat) => published,
        };
        if let Err(e) = published {
            tracing::warn!("heartbeat put failed: {e}");
        }
    }
}

/// The previous beat's readings, so each beat's rates span exactly one beat.
struct Sampler { at: Instant, ops: u64, cpu_secs: f64, rps: f64, cpu_pct: f64 }

impl Sampler {
    fn new(state: &AppState) -> Self {
        Self { at: Instant::now(), ops: state.admission.fleet_ops(), cpu_secs: cpu_time_secs(), rps: 0.0, cpu_pct: 0.0 }
    }

    fn beat(&mut self, state: &AppState, instance: &str) -> Heartbeat {
        let (at, ops, cpu_secs) = (Instant::now(), state.admission.fleet_ops(), cpu_time_secs());
        let dt_secs = at.duration_since(self.at).as_secs_f64().max(0.001);
        self.rps = ewma(self.rps, per_second(ops, self.ops, dt_secs));
        self.cpu_pct = ewma(self.cpu_pct, cpu_percent(cpu_secs, self.cpu_secs, dt_secs));
        (self.at, self.ops, self.cpu_secs) = (at, ops, cpu_secs);
        let engines = state.shards.engines_by_prefix();
        #[expect(
            clippy::unwrap_used,
            reason = "Sampler::beat; a poisoned timing ring may hold a half-recorded wait; recovering it would publish a median over a torn window"
        )]
        let waits: Vec<(i64, u32)> = engines
            .iter()
            .flat_map(|(_, engine)| {
                let timings = engine.timings.lock().unwrap();
                timings.iter().map(|g| (g.ts_ms, g.durable_wait_us)).collect::<Vec<_>>()
            })
            .collect();
        let pressure = Pressure::read(state);
        let (inflight, inflight_peak) = state.admission.swap_peak();
        let (wal_put_p50_ms, wal_put_p99_ms, out_inflight, out_inflight_peak) =
            crate::store_timing::heartbeat_summary();
        let now = now_ms();
        Heartbeat {
            instance: instance.to_owned(),
            ts_ms: now,
            rps: tenths(self.rps),
            ack_p50_ms: tenths(median_wait_ms(&waits, now)),
            cpu_pct: tenths(self.cpu_pct),
            inflight,
            inflight_peak,
            rss_mb: tenths(megabytes(rss_bytes())),
            wal_put_p50_ms,
            wal_put_p99_ms,
            out_inflight,
            out_inflight_peak,
            owned_shards: engines.into_iter().map(|(prefix, _)| prefix).collect(),
            draining: false,
            absorb_lag_max_secs: pressure.absorb_lag_max_secs,
            wedge_max_ms: pressure.wedge_max_ms,
            url: state.config.fleet.self_url.clone(),
        }
    }
}

/// This instance's backlog, as its heartbeat publishes it and its fleet
/// tick's rebalancer judges it.
pub(super) struct Pressure {
    /// The oldest unabsorbed bytes across held shards (s).
    pub(super) absorb_lag_max_secs: u64,
    /// The worst wedge across held shards (ms).
    pub(super) wedge_max_ms: i64,
    /// The shard behind `wedge_max_ms`: the rebalancer moves it when nothing
    /// committed lags, because a wedged shard sheds before it commits.
    pub(super) wedge_prefix: String,
}

impl Pressure {
    pub(super) fn read(state: &AppState) -> Self {
        let wedges = state.shards.engines_by_prefix().into_iter()
            .map(|(prefix, engine)| (prefix, engine.wedge_ms()));
        let (wedge_prefix, wedge_max_ms) = worst_wedge(wedges);
        Self { absorb_lag_max_secs: state.runtime.usage.absorb_lag_max(), wedge_max_ms, wedge_prefix }
    }
}
```

Private helpers follow. Each exists so its arithmetic is a test input, not a wall-clock read:

- `worst_wedge(wedges: impl IntoIterator<Item = (String, i64)>) -> (String, i64)` =
  `max_by_key(|(_, wedge)| *wedge).unwrap_or_default()`
- `per_second(now: u64, before: u64, dt_secs: f64) -> f64` = `(now - before) as f64 / dt_secs`
- `cpu_percent(now_secs, before_secs, dt_secs) -> f64` = `((now_secs - before_secs) / dt_secs * 100.0).max(0.0)`
- `ewma(average, sample) -> f64` = `if average == 0.0 { sample } else { average * 0.6 + sample * 0.4 }`
- `tenths(value) -> f64` = `(value * 10.0).round() / 10.0`
- `megabytes(bytes: u64) -> f64` = `bytes as f64 / 1_048_576.0`
- `median_wait_ms(samples: &[(i64, u32)], now: i64) -> f64`:
  `let cutoff = now - ACK_WINDOW_MS;`, keep `*ts_ms >= cutoff`, `sort_unstable`, then
  `waits.get(waits.len() / 2).map_or(0.0, |us| f64::from(*us) / 1000.0)`

Each helper gets a one-line doc giving its reason, for example "the load vector's moving
average: seeded by its first sample, then 60/40 toward history so one busy beat does not scale
the fleet". These are the same formulas as `fleet.rs:511-563` and `:571-576`, with one exact
change of evaluation order: one `now` for both the stamp and the cutoff.

`#[cfg(test)] mod tests` uses explicit `use super::{cpu_percent, ewma, median_wait_ms, megabytes, per_second, tenths, worst_wedge};`, with no glob:

- `rates_span_the_beat_and_cpu_is_never_negative`:
  - `per_second(30, 10, 2.0) == 10.0`
  - `cpu_percent(3.0, 2.0, 2.0) == 50.0`
  - `cpu_percent(2.0, 3.0, 2.0) == 0.0`
  - `megabytes(3 * 1_048_576) == 3.0`
- `the_average_starts_at_its_first_sample_and_moves_four_tenths_toward_each_next`:
  - `ewma(0.0, 5.0) == 5.0`
  - `ewma(10.0, 20.0) == 14.0`. This is exact in f64: 10·0.6 rounds to 6.0 and 20·0.4 to 8.0.
  - `tenths(1.26) == 1.3`
  - `tenths(1.24) == 1.2`
- `ack_latency_is_the_median_durable_wait_of_the_last_fifteen_seconds`:
  - samples `[(84_999, 500), (85_000, 1_000), (90_000, 4_000), (95_000, 2_000), (99_999, 3_000)]` at now 100_000 give `3.0`
  - `&[]` gives `0.0`
- `the_worst_wedge_names_its_shard_and_no_shard_is_no_wedge`:
  - `[("00",5),("01",90),("10",7)]` gives `("01", 90)`
  - `Vec::new()` gives `(String::new(), 0)`

**`src/fleet.rs`** (1,142 to 1,062; ceiling 1,142):

1. `:19`: insert `mod heartbeat;` before `mod outbox;`. rustfmt keeps it sorted. +1.
2. `:423` `start_configured`: insert `heartbeat::start(state.clone(), cfg.instance.clone(), tasks);`
   before `start(state, cfg, tasks);`. +1. This function carries no `#[expect]`.
3. `:436` too_many_lines reason, re-decided (its old text claims the tick "derives the
   heartbeat"): `"start; the fleet tick reads the authority once and derives the desired count, the rebalance and the publications from that one read; splitting it would separate the decisions from the read that dates them"`.
4. `:444` unwrap_used reason, re-decided. The timing-ring unwrap moves to heartbeat.rs, and
   `heartbeat::Pressure::read` adds path and call facts under this fingerprinted scope:
   `"start; the fleet documents serialise infallibly as plain data, an unreadable router snapshot is a typed deferral of the desired CAS, and the tick's own pressure reading, the move target and the eager move-in are infallible reads and ring decisions over the view this tick published rather than panic sites; handling the serialisation or aborting the tick on the snapshot would add branches no tick reaches"`
   (exactly two `;`, no `"`).
5. Delete `:463` `ewma_rps`, `:464` `last_ops`, `:466` `last_tick`, `:476` `last_cpu` and
   `:477` `ewma_cpu`. −5.
6. Delete `:510-528` (the sampler and its trailing blank line). −19.
7. Replace `:529-590` (62 lines: the heartbeat block, its PUT and its `continue`) with these 4
   lines. −58.
   ```rust
               // 1. This instance's own pressure, read at the tick. The heartbeat
               // task publishes its sample on its own period (`heartbeat`).
               let hb = heartbeat::Pressure::read(&state);
               let (wedge_prefix, wedge_max_ms) = (hb.wedge_prefix, hb.wedge_max_ms);
   ```
   Lines `:970-972` (`hb.absorb_lag_max_secs … (hb.wedge_max_ms / 1000)…`) and `:1003-1004` stay
   byte-identical. Using the `Copy` fields after moving `wedge_prefix` out is allowed. Keeping
   them identical keeps their `/ 1000` out of the in-diff scope: it has no killer, because every
   DST has wedge 0.
8. `:751`: "Self is always fresh (just wrote)." becomes "Self is always a member." ±0.

`start` shrinks by about 80 lines and several hundred facts. That satisfies the ceilings of
excessive_nesting, cast_sign_loss, cast_possible_truncation and let_underscore_must_use as they
stand. None of the six expects becomes unfulfilled: a 590-line body, nesting in return-home, three
`serde_json::to_vec(..).unwrap()`, the f64→u64 casts at `:662` and elsewhere, the wedge casts
`:618`/`:972`, and `let _ = tasks.spawn`.

**`src/dst/tests/fleet_controller.rs`** (598 to about 690; limit 1,000):

- `:1` module doc: add ", and a heartbeat and tick that never wait on each other".
- After `impl ObjectStore for HeldDocument`, add `fn held(inner: &Arc<dyn ObjectStore>, path: &'static str, write: bool) -> Arc<HeldDocument>`
  (fresh `AtomicU64::new(0)`, `Semaphore::new(0)`), with the doc "a closed gate counts without
  parking".
- r09 edits, all keeping line, item and fact counts unchanged:
  - `:103` becomes `for (path, write, holder) in [`
  - `:104-106` rows gain `"fleet-heartbeat"`, `"fleet"`, `"fleet"`
  - `:160` becomes `if path == "fleet/overrides.json" {`
  - `:176` `*name == "fleet"` becomes `*name == holder`, inside `assert!` tokens

  Add no comment line inside r09: its `too_many_lines` and `excessive_nesting` expects ratchet
  `scope_lines`.
- `:414-422` `heartbeat_stamp` becomes `published_stamp(store, instance) -> Option<i64>`
  (`store.get(&path).await.ok()?`), with the doc "`instance`'s published stamp; `None` before
  its first beat".
- `:429-474` witness (P1): the store becomes `let reads = held(&inner, "fleet/overrides.json", false); reads.gate.close();`,
  with `fleet_store: Some(reads.clone())`. Replace the stamp loop (`:464-474`) with
  `settled(Duration::from_secs(10), || reads.entered.load(Ordering::SeqCst) >= 2).await;` and
  `assert!(reads.entered.load(Ordering::SeqCst) >= 2, "a second tick must run");`. Its comment:
  "each pass reads the overrides once, before its eager open".
- Append R1 and R2 (§3). Each gets a doc naming item 40 and the reason. Each uses
  `settled`/`held`/`published_stamp`, stays at nesting ≤ 3 (`stamps.extend(published_stamp(..).await.filter(|ts| Some(*ts) > at_entry))`)
  and under 60 lines.

**Ledgers**: see §6.

### Commit B: "The heartbeat's freshness windows are named in missed beats"

**`src/fleet/heartbeat.rs`** (+about 45):

- Replace `const PERIOD` with:
  - `const PERIOD_MS: i64 = 2_000;` (doc: "peers judge freshness in whole periods, so every window below is a number of missed beats")
  - `const PERIOD: Duration = Duration::from_millis(PERIOD_MS.unsigned_abs());`
  - `const LIVE_WINDOW_MS: i64 = 5 * PERIOD_MS;` (doc: "five missed beats: the peer stops counting toward the live set, the fleet load and the rebalancer's targets")
  - `const RING_WINDOW_MS: i64 = 15 * PERIOD_MS;` (doc: "fifteen missed beats: the peer leaves the ring; it is wedged, since a request would have woken a merely sleeping one")
- Add:
  ```rust
  pub(super) fn live(heartbeat: &Heartbeat, now: i64) -> bool {
      now - heartbeat.ts_ms < LIVE_WINDOW_MS && !heartbeat.draining
  }
  pub(super) fn in_ring(age_ms: i64) -> bool { age_ms < RING_WINDOW_MS }
  ```
- Test `a_peer_is_live_under_five_missed_beats_and_in_the_ring_under_fifteen`. It builds
  `Heartbeat`s with `serde_json::from_str(&format!(r#"{{"instance":"streams-2","ts_ms":{ts},"rps":0.0,"owned_shards":[],"draining":{draining}}}"#))`,
  with now = 1_000_000:
  - live at ages 9,999, 10,000, −60,000 → `true`, `false`, `true`
  - draining at 9,999 → `false`
  - `in_ring(29_999)` → `true`, `in_ring(30_000)` → `false`, `in_ring(-60_000)` → `true`

**`src/fleet.rs`** (±0; 1,062):

- `:615` becomes `if heartbeat::live(&other, now_ms()) {`. This line has no operators left.
- `:749-750` comment: "heartbeat-dark >30 s" becomes "whose heartbeat missed fifteen beats
  (`heartbeat::in_ring`)". Same line count.
- `:444` unwrap_used reason re-decided once more, for accuracy and in case A lands alone. Replace
  "the tick's own pressure reading, the move target" with "the tick's own pressure reading, each
  peer's liveness verdict, the move target".

**`src/fleet/planning.rs`** (38 to about 44):

- `use super::heartbeat::in_ring;`.
- `:17` becomes `name.as_str() == instance || ages.get(*name).is_some_and(|age| in_ring(*age))`.
  This is 92 columns, and rustfmt keeps the existing block shape.
- `trusted_urls` body becomes:
  ```rust
  let (trusted, rejected): (HashMap<_, _>, HashMap<_, _>) =
      map.into_iter().partition(|(_, url)| super::valid_peer_url(url, policy));
  for instance in rejected.keys() {
      tracing::warn!(%instance, "rejecting malformed peer URL from urls.json");
  }
  trusted
  ```
  The result is identical. This is needed because registering planning.rs (§6) puts
  `trusted_urls` in the nightly rotation, and its log-only `if !valid` would be an equivalent
  mutant: deleting `!` changes only which entries are logged.

**`src/fleet/tests.rs`** (195 to about 225; `#![cfg(test)]`, so it is `production_unchanged`):

- `the_active_ring_is_self_and_every_ordinal_peer_under_fifteen_missed_beats`: ages
  `{streams-2: 29_999, streams-3: 30_000}`, count 3, self `streams-1`, gives
  `ring(&["streams-1","streams-2"])`. Count 2, self `streams-9`, empty ages gives
  `ring(&["streams-1","streams-2"])` (the fallback).
- `published_urls_keep_only_bare_origins`: `{streams-1: https://a.example, streams-2: https://b.example/path}`
  under `FleetConfig::default()` gives `{streams-1: https://a.example}`.

## 5. Mutation analysis (cargo-mutants 27.1.0, `--in-diff`, `--timeout 90`, `--jobs 1`)

Const initializers are mutated too: `visit_expr_binary` has no function guard (visit.rs:572;
`fn_stack` is optional). Methods (`.max`, `.round`, `unsigned_abs`) and literals are not mutated.
Test-only modules and the name `new` are skipped.

**Commit A, owner `fleet_heartbeat` (all lines inserted), about 58 mutants:**

| function | mutants | killer |
|---|---|---|
| `start → ()` | 1 | R1 (0 stamps), R2 (entered stays 0), r09 case 1, `runtime_isolation` (no heartbeat doc) |
| `run`, `Sampler::beat`, `Pressure::read` (return `TaskResult`, `Heartbeat`, `Pressure`, none `Default`) | 3 unviable | none needed |
| `worst_wedge` | 6 FnValue (`String::new()`/`"xyzzy"` × 0/1/−1) | `("01", 90)` |
| `per_second` | 3 FnValue; `-`→`+` gives 20, `-`→`/` gives 1.5, `/`→`%` gives 0, `/`→`*` gives 40 | `== 10.0` |
| `cpu_percent` | 3 FnValue; `-`→`+` gives 250, `-`→`/` gives 75, `/`→`%` gives 100, `/`→`*` gives 200, `*`→`+` gives 100.5, `*`→`/` gives 0.005 | `== 50.0` |
| `ewma` | 3 FnValue; `==`→`!=` gives 2.0 (seed case); `*`,`+`,`*` variants give 18.6, 24.7, −2, 48, 26.4, 56 | `5.0`, `14.0` |
| `tenths` | 3 FnValue; `*`→`+` gives 1.1, `*`→`/` gives 0, `/`→`%` gives 3, `/`→`*` gives 130 | `1.3` |
| `megabytes` | 3 FnValue; `%` gives 0, `*` gives about 3e12 | `3.0` |
| `median_wait_ms` | 3 FnValue; `-`→`+` gives 0.0 (nothing in window), `-`→`/` gives 2.0 (cutoff 6 admits the stale 500 µs); `>=`→`<` gives 0.5; `len/2`→`%` gives 1.0, `*` gives 0.0; `/1000`→`%` gives 0.0, `*` gives 3e6 | `3.0` |

The stale sample is deliberately small (500 µs): with 9,000 µs the `-`→`/` mutant was
equivalent (median unchanged).

**Commit A, owner `fleet` (fleet.rs):** FnValue `start_configured → true/false` and
`start → ()` are killed by every fleet_controller rig (the ring is never published). The new
fleet.rs lines contain no operators.

**Commit B, about 29 mutants:**

- `live`: 2 FnValue + `-` (2) + `<` (3) + `&&`→`||` + `!` deletion. Killed as follows: `-`→`+`
  (case 1 false), `-`→`/` (case 2 gives 1 < 10,000), `<=` (case 2), `==`/`>` (case 1), `||`
  (case 2), `!` (case 1).
- `in_ring`: 2 FnValue + 3 operators, killed at 29,999 and 30,000.
- Consts: `5 * PERIOD_MS` → `+` gives 2,005, `/` gives 0; `15 *` → 2,015 or 0. All killed by the
  literal boundaries. The tests use literals, never the constants, so a mutated constant cannot
  move both sides.
- planning `:17`: `==`→`!=` gives `[2,3]`, `||`→`&&` gives the ordinal fallback `[1,2,3]`, plus
  3 FnValue of `active_members` (`vec![]`, `[""]`, `["xyzzy"]`).
- `trusted_urls`: 5 FnValue (`HashMap::new()` and `from_iter` with ""/"xyzzy" pairs), killed by
  the URL test.
- fleet.rs: `start → ()`, killed as before.

**Timeouts:** every wait in the new tests is bounded: 5 s entry, 9 s window, 6 s settle, 10 s
witness. Slowest test runs: R1 about 12 s; a mutant that makes `an_unreadable_router_report`
use its full budget takes 32 s. All are under 90 s. `per_second` `-`→`/` divides by
`self.ops == 0` in a rig, which panics the heartbeat task. That is fast, and the unit test
catches it first.

**Ratchet interplay (why the two commits push together):** `source_gate` compares against the
merge base (`common.merge_base`, origin/slate).
- Commit B's `:615` adds one fact to `start` (5 facts against 4). Against c397e9e5 that is
  absorbed by A's shrink.
- If A were pushed alone and became the base, B would also have to re-decide the four unchanged
  reasons on `start`. So push them together.
- r09's metrics do not move (see the edit list).

## 6. Ledgers (same commit as the code)

**Commit A:**
- `scripts/quality/mutation_owners.py`: add
  `owner('fleet_heartbeat', 'src/fleet/heartbeat.rs', 'fleet:: dst_tests::fleet_controller::'),`
  after `fleet_document_tests`. `src/fleet` is a critical prefix, and an unregistered file fails
  closed.
- `docs/quality/owners.json`: add `{category: macro-dsl, count: 2, owner: "crate::run", path: "src/fleet/heartbeat.rs", syntax: "tokio::select", reason: "The heartbeat publisher selects cancellation at its period boundary and around its one PUT (R09); the fleet_controller DST rigs cancel it while that PUT is held."}`,
  next to the telemetry_loop row.
- `docs/quality/source-allowances.json`: `(macro-dsl, src/fleet.rs, crate::start, fleet_io)`
  `count` goes from 9 to 8. The heartbeat's `fleet_io!` goes away, and a stale count fails as
  "obsolete source allowances". Either edit it by hand or `--prune`.
- `docs/refactor/architecture-policy.json`: add `"src/fleet/heartbeat.rs"` to
  `transport_and_composition_files`, with the `transport_rationales` entry "Fleet heartbeat
  publisher: samples this runtime's load vector from the composed AppState and publishes it on
  its own supervised period; it decides nothing — the fleet tick and the repository own
  ownership and the store." This follows the `telemetry_loop.rs` precedent (fc18a704). Without it:
  `reverse dependency growth: src/fleet/heartbeat.rs -> crate::http: 1 > 0`.
- `docs/refactor/test-inventory.json`: `python3 scripts/test-inventory.py --write`. This changes
  two rows (r09 and `an_override_…`) and adds two (R1, R2).
- `docs/refactor/review-mechanisms.json` (`active-fleet-authority`, `:705`): set r09's `sha256`
  to its new `function_sha256` from the inventory. Mechanism, configuration and oracle text is
  unchanged.
- `docs/review-controller-evidence.md:18`: "three real production-loop cases hold heartbeat PUT,
  required overrides GET, or desired CAS PUT … Failed required reads retain …" becomes "three
  real cases hold the heartbeat task's PUT, the tick's required overrides GET, or its desired CAS
  PUT. Shutdown must join the task that entered each hold without forced abort … The failed
  required read retains …".
- No changes needed:
  - `src/dst/tests/README.md`: no new DST module.
  - scenario map and dispositions: no rename or deletion.
  - `test-additions.json`: historical, and no script reads it.
  - `legacy-diagnostics*.json`: adoption snapshot.
  - `docs/refactor/architecture-review-baseline.json`: fixed commit.
  - owners `unresolved-glob`: the new tests import explicitly.

**Commit B:**
- `scripts/quality/mutation_owners.py`: add
  `owner('fleet_planning', 'src/fleet/planning.rs', 'fleet:: dst_tests::fleet_controller::'),`.
- Nothing else: no macros, no DST test changes, no http edge.

## 7. Controls (run after the current mutation leg finishes; `$SCRATCH` is the session scratchpad)

**Commit A:**
1. Red (§3), with the fleet_controller.rs edits only:
   `cargo test --locked --lib -- dst_tests::fleet_controller:: 2>&1 | tee "$SCRATCH/red-40a.log"`.
   Expected: `test result: FAILED. 4 passed; 3 failed`. The failures are R1
   (`0 new stamps in 9 s`), R2 (`left: ["prior-owner"]`) and r09 (`assertion failed: report.outcomes.iter().any(`).
2. Implement A. Then
   `cargo test --locked --lib -- fleet:: dst_tests::fleet_controller:: dst_tests::runtime_isolation::`
   gives all ok, including 4 `fleet::heartbeat::tests::`, 7 `fleet_controller`, and the unchanged
   repository, outbox and isolation tests.
3. Pinning control P1 (§3): red with `left: Number(1)`, then revert.
4. `cargo fmt --all -- --check`, `cargo clippy --locked --workspace --all-targets -- -D warnings`
   and `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items`
   are clean. There should be no `unfulfilled_lint_expectations`: `start`'s six expects are still
   met, and the new two are met.
5. `wc -l src/fleet.rs` prints `1062` (±2); `wc -l src/dst/tests/fleet_controller.rs` is ≤ 700.
6. `cargo build --locked -p streams-quality-syntax && (cd scripts/quality && python3 -c 'import source_gate; p=source_gate.check(); print("\n".join(p) or "source gate OK")')`.
   - Before the ledgers, expect exactly:
     `unregistered source occurrence (2): ('macro-dsl', 'src/fleet/heartbeat.rs', 'crate::run', 'tokio::select')`
     and `1 obsolete source allowances; run the quality ratchet with --prune`.
   - Without the two reason re-decisions it would also print
     `accepted exception grew without a new decision: ('src/fleet.rs', 'crate::start', 'function', 'expect(clippy::unwrap_used, …)'): unwrap_site:path:crate::start:<digest> 0 -> 1`.
   - After the ledgers: `source gate OK`.
7. `python3 scripts/architecture-gate.py --check`: before the policy row it prints
   `reverse dependency growth: src/fleet/heartbeat.rs -> crate::http: 1 > 0`; after, OK.
8. `python3 scripts/test-inventory.py --check` lists r09 and `an_override_…` as changed and R1
   and R2 as new. Run `--write`, then `--check` gives OK.
9. `python3 scripts/review-evidence.py --check` prints
   `mechanism test changed or missing: src/dst/tests/fleet_controller.rs::r09_fleet_cancels_entered_documents_without_partial_authority_or_lost_retry`.
   Copy the new hash, then the check is clean. To print it:
   ```
   python3 - <<'EOF'
   import importlib.util; from pathlib import Path
   s=importlib.util.spec_from_file_location('i','scripts/test-inventory.py'); i=importlib.util.module_from_spec(s); s.loader.exec_module(i)
   p=Path('src/dst/tests/fleet_controller.rs'); print([x['function_sha256'] for x in i.functions(p.read_text(),p) if x['name'].startswith('r09_')])
   EOF
   ```
10. `scripts/quality.sh` shows no `QUALITY_FAIL`.

**Commit B:**
11. On the A tree, add the two `fleet::tests` pins first:
    `cargo test --locked --lib -- fleet::tests::the_active_ring_is_self fleet::tests::published_urls_keep_only_bare_origins`
    gives `2 passed`, which pins today's literal and `trusted_urls`. Then apply B.
    `cargo test --locked --lib -- fleet:: dst_tests::fleet_controller::` is all ok.
12. Non-vacuity controls 1-3 (§3): each is red with the quoted message, then revert.
13. Repeat steps 4, 6, 7 and 10. Expected: `wc -l src/fleet.rs` still 1062, and the gates OK with
    no new ledger lines except the planning owner.

**Both commits:**
14. Mutation leg for A+B:
    `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=c397e9e5 scripts/quality/mutations.sh`.
    - Expected owners: `fleet`, `fleet_heartbeat`, `fleet_planning`.
    - Expected final line: `Mutation verification executed <≈90> selected mutant(s) across 3 registered owner(s).`
    - Every owner reports 0 missed and 0 timeout (only caught or unviable).
    - To see the exact rows first, list them with
      `cargo mutants --list --json --in-diff target/quality-mutations/pr.diff --file src/fleet/heartbeat.rs --package streams-slate`.
    - Budget: about 60-80 min at the historic ~50 s per mutant.
15. The full suite, `cargo test --locked --release` (the ci.yml leg).

## 8. Out of scope (noted, not planned)

- **Reviewer's watch channel and `lag_hot_ticks` duration:** not done. §2 explains why.
  - A duration would change the rebalancer's damping, which is a behavior change with its own red
    test.
  - It would also put `Instant` arithmetic into the mutation scope with no deterministic killer.
- **Reader-side age inflation:** `fleet.rs:614-615` computes `now_ms() - ts_ms` after the whole
  population read, which can take up to 10 s.
  - With a 2 s beat, a slow read can still push a peer's age past `LIVE_WINDOW_MS`. It cannot push
    it past the ring window, so there is no fencing.
  - Taking `now` before the read is a separate behavior change.
- **Router-report freshness (`fresh_edge_p50`, `fleet.rs:218`):** its 10 s belongs to the
  router's cadence (`lb.rs:86`), not the heartbeat's.
- **Mirrors of the windows:** `src/bin/pilot/lb.rs:312,331` and `bench/fleet/observe-fleet.py:67`
  are separate programs. Their values still agree.
- **The tick itself:** its 2 s sleep and 45 s pass deadline stay as they are. Nobody's liveness
  depends on them any more.
- **Moving `Heartbeat`, `rss_bytes` and `cpu_time_secs` into heartbeat.rs:** not needed for the
  line budget (§2).
- **Pre-existing nightly survivors in `src/fleet.rs`:** not addressed here, beyond the heartbeat
  arithmetic that leaves `start`.
- **Documentation:** RUNBOOK §6 and COMPUTE-SPEC §2 already say "every 2 s" and "5 missed
  beats". After this change they are true, so they need no edit.

## 9. Decisions for Søren

1. **A new supervised loop is visible at the edge.** `/v1/debug/load` `tasks.loops` gains
   `{"name":"fleet-heartbeat","policy":"Critical","state":"Running"}`, with the same row shape.
   If that loop ever exits while the runtime runs, `/health` and `/readyz` answer
   `503 critical task terminated: fleet-heartbeat`, as they already do for `fleet`.
   - Recommendation: accept, Critical. Without heartbeats, peers drop the instance from their
     ring within 30 s, so it is not healthy.
   - The item cannot land without a separate task: running both loops inside the `fleet` task
     would re-indent all of `start`.
   - The commits are prepared locally. Hold the push until he answers.

## Skeptic corrections (C1..Cn)

Checked against c397e9e5 (`git rev-parse origin/slate HEAD` both print c397e9e5, so the plan's
note that the task text's 5d9d517f is stale is right). `wc -l`: fleet.rs 1,142; planning.rs 38;
fleet/tests.rs 195; fleet/repository.rs 443; dst/tests/fleet_controller.rs 598;
dst/tests/runtime_isolation.rs 674. The line arithmetic works out: 1,142 − 5 − 19 − 58 + 2 = 1,062.

These claims hold against the source:
- Every cited fleet.rs line: 491-499, 510-528, 529, 565-590, 608, 614-615, 650, 735, 763, 783,
  852, 954, 970-972, 1003-1004, 1028 and 1108.
- repository.rs: `DOCUMENT_DEADLINE`=10 s at :35, `publish_heartbeat` has its own 10 s timeout at
  :64, and the population read has its own 10 s at :152.
- `read_population` skips desired/overrides/urls (:108-114). So in R1 the only overrides GET
  is the tick's :783, and no other task in the rig reads it: `http_rig_build` spawns only `http`
  (fixture_http.rs:537), with no telemetry drainer calling `read_doc`.
- source_gate: a new reason is a new identity (source_rules.py:207-208). The reason regex is at
  :258, and all five new or re-decided reasons match it.
- Macro facts inside `assert!` are opaque tokens (quality-syntax scan.rs:273-281). The r09 edits
  at :103-106, :160 and :176 therefore leave its scope_lines, nested_items and syntax_facts
  unchanged.
- The source-allowances row `(macro-dsl, src/fleet.rs, crate::start, fleet_io)` is 9, so going to
  8 is right. The owner-name convention `crate::run` matches the `src/bootstrap/rss.rs` row.
- The architecture gate would fail on `reverse dependency growth: src/fleet/heartbeat.rs ->
  crate::http: 1 > 0` (architecture-gate.py:121-124), and the telemetry_loop precedent is in the
  policy.
- cargo-mutants 27.1.0 has `Ge => [<]` only, `Lt => [==, >, <=]` (visit.rs:589-592), impl fns
  named `new` are skipped (:466), and const initializers are visited (collect_mutant has no fn
  guard). Commit A's count of about 58 and commit B's of 29 are correct. The median
  sample set has no equivalent mutant: `>=`→`>` is never generated, and every generated
  operator changes 3.0.
- Float claims: 10·0.6+20·0.4 = 14.0 exactly, tenths(1.26)=1.3, tenths(1.24)=1.2.
- `trusted_urls` really does have an equivalent `!` mutant once planning.rs enters the rotation.
  The rewrite is needed.
- Traces: R1 is red on the current tree ("0 new stamps"; the next PUT is at entry+12 s), R2 is
  red (`left: ["prior-owner"]`), r09 is red on the holder, and P1 passes on both trees. All waits
  are bounded. The worst DST under a mutant is about 32-35 s, well under `--timeout 90`
  (mutation_driver.py:52).
- test-inventory scans only src/dst (test-inventory.py:138), so the heartbeat.rs and
  fleet/tests.rs unit tests need no inventory rows. review-controller-evidence.md is not read by
  any script. test-additions.json is not read by any script.

Corrections:

- **C1 (test hygiene): R1 and R2 must end with `engine_shutdown(&rig.state).await`.** Every test
  in the module does this after `shutdown` (fleet_controller.rs:378, :486, :546, :597). As
  written, §3 R1/R2 teardown is only `shutdown(3 s)`. Add the line in both tests. They stay far
  under 100 lines.

- **C2 (§2 omits three behaviour consequences of taking self's load from the store; list them as
  accepted, or fix and red-test them):**
  1. **Startup race.** The tick's first pass and the first beat both fire 2 s after spawn:
     fleet.rs:491-495 against the new `interval` after its immediate first tick. Spawning the
     heartbeat first does not order them, so tick 1 can read the set without self. `live`,
     `total_*`, `peer_load[self]` and the `live={live}` reason text then lack this instance. The
     bootstrap desired publication (fleet.rs:1067-1068, `cur.is_none()`) is then computed
     without its own CPU and in-flight counts.
  2. **Return-home health.** For `home == cfg.instance`, fleet.rs:906-909 now reaches the
     wedge-blind fallback `absorb_lag_max() == 0` whenever self's stored beat is ≥ 10 s old or
     missing. Before, self was always in `peer_load`, fresh, with `eff_lag` including
     `wedge_max_ms / 1000` (:617-619). So a wedged self whose heartbeat PUTs are failing now gets
     overrides returned to it.
  3. **Write brownout.** In a write brownout the tick no longer abstains. It goes on to the
     overrides/desired CAS writes, each bounded by 10 s. §2 mentions only the load exclusion.

  The cheapest structural fix for item 2 is to judge `home == cfg.instance` by the tick's own
  `Pressure` (`hb`). That touches :909, so it needs a red DST and a killing test. Otherwise
  record all three in §2 as accepted.

- **C3 (semantic change, add to §9 as decision 2): the heartbeat no longer proves that the
  controller is making progress.**
  - Today a fresh `fleet/<i>.json` implies this instance's fleet tick completed its step 1 within
    the last pass. After this change, a tick parked or looping on `continue` (the brownout case,
    which is the point of the fix) keeps ring membership indefinitely while its ownership mirror,
    yield-at-tick and eager opens are stale.
  - That is the intended trade: stop mutual fencing. But it removes the only fleet-level signal
    that a tick is stuck. `/readyz` only sees a *finished* Critical task (tasks.rs:269-281), not
    a stalled one.
  - State this, and keep the "wedged" wording honest. The fleet.rs:750 comment and B's
    `RING_WINDOW_MS` doc should say the *process* stopped publishing, not that the controller
    wedged.

- **C4 (use-site table §1d):** `Heartbeat` is also constructed through serde at
  `src/fleet/repository/document_tests.rs:369`, next to the :371 `publish_heartbeat` call the
  table does list. It is not affected, but the "grepped" list is incomplete.

- **C5 (stale anchors after commit A):** Commit B and controls 3 and 12 cite pre-A lines. After
  A removes 79 lines above them (net, above :529):
  - fleet.rs:615 becomes about :536.
  - The "heartbeat-dark >30 s" comment is at **:750**, not ":749-750". It becomes about :671.
  - The P1 control's eager filter at :852 becomes about :773.

  Anchor these edits by quoted text, not number.

- **C6 (style: avoid a new exception): `heartbeat::start` should follow `telemetry_loop`.** That
  is `if let Err(rejected) = tasks.spawn(..) { tracing::warn!("fleet-heartbeat not spawned:
  {rejected:?}") }` (src/billing/telemetry_loop.rs:27, :59). Use it instead of `let _ =` plus a
  new `#[expect(clippy::let_underscore_must_use)]`: RUST-QUALITY says never to swallow an error,
  and fleet.rs:459's `let _` is legacy, not a precedent to copy. This drops one new exception,
  and the owners/allowances ledgers do not change.

- **C7 (docs that go stale, no ledger impact):**
  - `start_configured`'s doc (fleet.rs:390-392, "start the loop on its own repository") should
    name both tasks. This is +1-2 lines, still ≤ 1,142.
  - runtime_isolation.rs:326 "One tick is 2s; give both loops a heartbeat" and :329 "Each loop
    published ITS heartbeat" should say the heartbeat task. These are comment-only edits:
    `canonical_tokens` strips comments, so neither the inventory nor the mechanism hash changes.
  - Optionally extend review-mechanisms.json `active-fleet-authority.owner` ("FleetRepository
    and fleet controller") to name the heartbeat publisher. r09 case 1 now proves that task.
    Optionally also add R1 and R2 to its `tests` with their `function_sha256`.

- **C8 (control 6 wording):** if C6 is taken, the pre-ledger source-gate output is unchanged
  (the `let _` exception is gone either way). Keep the expected
  `unregistered source occurrence (2): ('macro-dsl', 'src/fleet/heartbeat.rs', 'crate::run',
  'tokio::select')` only if `run` holds exactly the two `select!`s, and do not add a third (for
  example around the first `tick()`).

**Verdict: ready-with-corrections.** The design, ratchet handling, mutation analysis, red traces
and ledgers are sound. C1 must be applied before coding. C2 and C3 need recording, C3 as a
decision for Søren alongside decision 1. C4-C8 are small. No control is unbuildable. The P1
control is buildable but its line anchor moves (C5).
