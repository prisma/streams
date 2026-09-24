# Item 34: the rebalancer target and the eager open ignore the active ring

Plan only. Repo `/Users/sorenschmidt/code/streams`, branch `slate`, HEAD `2fb92fb9`.
At planning time origin/slate is also `2fb92fb9` (`git rev-list --count origin/slate..HEAD`
is 0; the task text said `729c52ac`, but that was pushed since). `src/fleet.rs`, `src/fleet/*`,
`src/ownership.rs` and `src/dst/tests/fleet_controller.rs` did not change in those commits.
One commit lands on `slate`.

The reviewer's line numbers are stale by about five lines. `856-857` is now
`fleet.rs:851-853` and `994` is now `fleet.rs:989`. `113-123` is still correct.

---

## 1. Problem (verified on the current tree)

### 1a. The move target is any fresh heartbeat. Ring membership is not checked.

`src/fleet.rs:113-123`:
```rust
pub(crate) fn pick_move_target(
    peers: &std::collections::HashMap<String, (f64, u64)>,
    me: &str,
    lag_threshold_secs: u64,
) -> Option<String> {
    peers
        .iter()
        .filter(|(n, (_, lag))| n.as_str() != me && *lag < lag_threshold_secs / 2)
        .min_by(|a, b| a.1.0.total_cmp(&b.1.0))
        .map(|(n, _)| n.clone())
}
```
The loop fills `peers` from every heartbeat that is fresh and not draining, under any
instance name (`fleet.rs:612-618`):
```rust
if now_ms() - other.ts_ms < 10_000 && !other.draining {
    ...
    peer_load.insert(other.instance.clone(), (other.cpu_pct, eff_lag));
```
The ring is narrower. It holds only the ordinal names `streams-1..streams-<desired>` whose
heartbeats are under 30 s old (`fleet/planning.rs:6-22`, called at `fleet.rs:753`
`let active = planning::active_members(cur_count, &cfg.instance, &hb_age_ms);`).
The call at `fleet.rs:989` passes no ring:
```rust
let target = pick_move_target(&peer_load, &cfg.instance, rebalance_lag_secs);
```
Some instances heartbeat but are outside the ring:
- an ordinal above the desired count that is still awake after a scale-in;
- a non-ordinal name, such as the default `INSTANCE_NAME=streams` or `streams-9` when `FLEET_MAX=4`.

Such an instance owns nothing, so it is usually the coolest heartbeat and gets picked.
`fleet.rs:1022-1048` then writes and CAS-commits `OverrideEntry { to }`. It calls
`state.ownership.set_override(&prefix, &to)` and retires the shard locally
(`RetirementReason::FleetEviction`).

### 1b. Every ring reader ignores that override

`src/ownership.rs:69-88` (`effective_owner`):
```rust
if let Some(t) = view.overrides.get(prefix)
    && active.iter().any(|a| a == t)
{
    return Some(t.clone());
}
Some(active[ring_pick(prefix, active)].clone())
```
The router mirror in `src/bin/pilot/proxy.rs:56-63` does the same:
`(Some(t), _) if active.iter().any(|a| a == t) => t`. So routing and `resolve`
(`shard_directory.rs:244`, `foreign_owner`) still send the shard to its rendezvous home,
which is the laggard. The move does nothing, except that the laggard evicts the shard and
takes a holdoff strike (`sharddir/holdoff.rs:34-40`: `FleetEviction` of a young
engine counts as evidence).

### 1c. The target still opens the shard, and fences the real owner in a loop

The eager handoff at `fleet.rs:850-853` reads the raw `to`:
```rust
let mut mine: Vec<String> = ov.entries.iter()
    .filter(|(prefix, e)| e.to == cfg.instance && !state.shards.is_open(prefix))
    .map(|(p, _)| p.clone()).collect();
```
On the target (a non-member), `e.to == cfg.instance` holds, so it calls
`open_or_wait(prefix, 1 s)`. That open fences the owner's slatedb writer; the comment at
`:845-849` says "the open fences the loser's db immediately". On its next tick the
target's yield (`fleet.rs:826-843`) asks `effective_owner(&prefix)`, gets the home, and
retires the shard (`OwnershipMoved`). Once its holdoff expires (`sharddir.rs:47-53`,
`272-275`: 3 s doubling to a 60 s cap), the eager open fires again.

Strikes decay only when an engine lives at least `SHORT_LIVED` (30 s). Neither engine does,
so the owner is fenced at least once a minute for as long as the override survives.

Return-home (`fleet.rs:895-913`) drops the entry only when the home has zero lag and fits
its fair share. The home is the laggard, so the entry survives and the loop persists. The
laggard's own rebalancer also re-targets the same idle non-member after every cooldown.

Void overrides also arise without the rebalancer: a target that was a member leaves the
ring through scale-in or by going dark for more than 30 s. The eager predicate fences in
that case too. So both halves must change.

### Full use-site list

| Symbol | Sites |
|---|---|
| `pick_move_target` | `fleet.rs:113` (def), `fleet.rs:989` (only production call); `fleet/tests.rs:17,23,30,116` |
| raw `e.to == cfg.instance` (eager) | `fleet.rs:852` only |
| other readers of `OverrideEntry.to` | `fleet.rs:793` (mirror into `set_view`), `fleet.rs:903` (`home == e.to` return-home no-op drop). Both are correct and unchanged |
| `effective_owner` | `fleet.rs:829` (yield), `fleet.rs:926` (return-home fair share), `sse/source.rs:478`, `ownership.rs:93` (`foreign_owner`). All honour the active-only rule and are unchanged |
| `is_mine` / `foreign_owner` | `ownership.rs:92-99`; `shard_directory.rs:244` (the `resolve` admission); `sse/source.rs:210`; `application/creation/{product,raw}.rs`; `billing.rs:1614,1667`. All unchanged |
| `ring_active()` | `fleet.rs:891` (return-home re-read, which this plan deletes), `http.rs:1006` (debug `ring.active`), `operator.rs:101`, DST tests. The reads outside `fleet.rs` are unchanged |
| `set_view` | `fleet.rs:797`, `dst/tests/fleet_controller.rs:144` (pinned r09 test, untouched) |

---

## 2. Contract decision

The fix is typed at both ends, and the `active` set is the only authority.

**I1, writer (`pick_move_target`).** The signature becomes
`pick_move_target(peers, active: &[String], me, lag_threshold_secs)`. It returns
`Some(t)` only if all of these hold:
- `t ∈ active`
- `t ≠ me`
- `lag(t) < threshold/2`
- `t` has the lowest CPU among the candidates that pass the first three checks.

Because the ring is a required parameter, every caller must say which ring the target has
to belong to. The call passes the ring the tick itself just published, not a re-read of it.
It uses `&active`, the local computed at `:753`, which `set_view(active.clone(), map)`
publishes at `:797`. This ring is value-identical to `state.ownership.ring_active()`: only
this task writes the view, and nothing between `:797` and `:989` calls `set_view` or
`set_override`. So return-home's re-read at `:891` is deleted and both decisions use one
ring. The borrow checker proves the reuse: without the `.clone()` the later uses would be
use-after-move.

**I2, reader (eager handoff).** The candidate set is
`{p ∈ overrides.keys() | state.ownership.is_mine(p) ∧ ¬state.shards.is_open(p)}`.
- `is_mine` is the same typed verdict `ShardDirectory::resolve` uses to admit an open
  (`foreign_owner(..).is_none()`). The eager open therefore opens exactly what the first
  routed request would open, earlier.
- In fleet mode it is the exact complement of this tick's yield
  (`effective_owner = Some(other)`), because `active_members` never returns an empty set
  (`planning.rs:21`) and the instance is named. An instance can therefore never open at one
  tick what the next tick yields.
- It is not the reviewer's `effective_owner(prefix) == me`. The two agree whenever a ring
  exists. `is_mine` costs four fewer syntax facts in `start` (−1 net instead of +3 for the `effective_owner(p).is_some_and(|o| o == cfg.instance)` spelling, §4) and is literally the `resolve`
  admission. The only divergence is the unsupported empty-`INSTANCE_NAME` fleet, where
  ownership is disabled and `resolve` already opens anything.

**One deliberate consequence** (see §9): the rendezvous home now also opens, at its next
tick, a shard whose override names a non-member. Previously it waited for the first routed
request. This is how a home fences a wedged target's zombie db (dark for more than 30 s, so
it cannot yield itself) instead of leaving it running while the shard is cold.

**No wire change.**
- `fleet/overrides.json`, heartbeats, `desired.json`, the `rebalance_move` /
  `rebalance_return` ops events, `/v1/debug` `ring`, the operator `ring_active`, and the
  `shard_opens` keys all keep their schema. Only values change: `rebalance_move.to` is
  always a ring member, and a non-member target's `shard_opens.started` stops climbing.
- New binaries write a subset of what old binaries could write, and old readers already
  ignore void overrides in `effective_owner` and the router.
- During a rolling deploy, an old-binary non-member target can still eager-open until it is
  replaced. That window already exists today.
- No product or raw edge status, body or metric changes.

This is not a synchronization change. The view is replaced under the existing lock, and
the yield, the eager open, return-home and the move stay sequential in the one fleet task
over one published view. No Loom or held-commit test is required.

---

## 3. Red tests

The red order matters. The unit tests change `pick_move_target`'s arity, so the lib test
target stops compiling. Capture the DST reds first, before the unit tests are applied (§7).

### 3a. DST, `src/dst/tests/fleet_controller.rs`

Helper changes in the same file:
- `peer_heartbeat(store, instance, inflight, cpu_pct: f64)`: add `"cpu_pct":{cpu_pct}` to
  the JSON and to the doc. The one existing call (`an_unreadable_router_report…`) becomes
  `peer_heartbeat(&inner, "streams-2", 300, 0.0)`, which has the same semantics because
  `cpu_pct` is `#[serde(default)]`.
- New `seed_ring_of_two(store, entries: &[(&str, &str)])`. It PUTs
  `fleet/desired.json` = `{"count":2,"epoch":1,"reason":"seed","computed_at_ms":0}` and
  `fleet/overrides.json` = `serde_json::to_vec(&crate::fleet::Overrides { entries:
  <prefix → OverrideEntry { to, ms: now_ms() }>, ..Default::default() })`. It is typed, so
  there is no escaped JSON.
- New `heartbeat_stamp(store, instance) -> i64`. It GETs `fleet/<instance>.json`, parses
  it as `crate::fleet::Heartbeat` and returns `.ts_ms`. Its doc: a tick publishes its
  heartbeat before its first read, so a newer stamp proves the previous tick ran to its end.

Scenario constants used below:
- `ring_pick("00", [streams-1, streams-2]) = 0`, because FNV-1a of "00 streams-1" is
  2684974198 and of "00 streams-2" is 2668196579.
- `ring_pick("10", [streams-1, streams-2]) = 1`.
- The rig's `fleet_max` is 4 (`config/cli.rs:630`), so `streams-9` is never ordinal and
  never a member, whatever `desired` the loop publishes from this process's CPU. This is why
  the non-member is `streams-9` and not `streams-3`: a CPU-noise scale-out to 3 would make
  `streams-3` a member and flake the green run.

Only expression macros are used (`assert!`, `assert_eq!`, `matches!`, `format!`), with no
`vec!` passed as a slice and no `serde_json::json!` / `tokio::spawn` / `tokio::select!`. So
no `docs/quality/owners.json` rows are needed. Arrays are used where a ring is only borrowed
as `&[String]`, which avoids `clippy::useless_vec`.

#### T1 `an_override_the_ring_ignores_is_never_opened_by_its_target` (behavioural red for I2)

Setup:
- `seed_ring_of_two(&inner, &[("00", "streams-9")])`
- `peer_heartbeat` for streams-1 and streams-2, each `(0, 0.0)`
- rig `instance: Some("streams-9")`
- `let opened = rig.state.shards.open_stats()["started"].clone();`
- `start_configured`

Steps:
1. `settled(20 s, ring_active() == ["streams-1","streams-2"])`, then:
   - `assert_eq!(ring_active(), ring, "the tick must publish the ring")`
   - `assert_eq!(effective_owner("00").as_deref(), Some("streams-1"), "the ring ignores an override to a non-member")`
2. `let published = heartbeat_stamp(&inner, "streams-9")`. Poll, bounded at 10 s, until the
   stamp is greater than `published`. Then
   `assert!(heartbeat_stamp(..) > published, "a second tick must run")`.
3. `assert_eq!(rig.state.shards.open_stats()["started"], opened, "an override the ring ignores must never open the shard on its target")`
4. `assert!(held_prefixes().is_empty(), "the target must hold nothing the ring assigns elsewhere")`
5. Cooperative shutdown and `engine_shutdown`, as in the existing test.

Current-tree trace:
- Tick 1 (≈2 s): heartbeat T1; active_members(2, "streams-9", …) = [s1, s2];
  set_view([s1, s2], {00: s9}).
- The yield holds nothing.
- Eager: `e.to == "streams-9" && !is_open("00")` → `open_or_wait("00", 1 s)` →
  `OpenGate::get_or_open` increments `opens.started` to 1 (`sharddir.rs:517`) before it
  spawns the open.
- Return-home skips (entry younger than 300 s). The rebalancer has lag 0, so it does nothing.
- The test sees the ring and reads T1, then sees T2 only after tick 1 has ended. At tick 2 the
  yield retires 00 (strike, 6 s holdoff) and the eager open returns `Wait`, so the count stays 1.

Expected red:
```
assertion `left == right` failed: an override the ring ignores must never open the shard on its target
  left: Number(1)
 right: Number(0)
```

#### T2 `a_lagging_owner_moves_its_shard_only_to_an_active_member` (behavioural red for I1)

Setup:
- `seed_ring_of_two(&inner, &[])`
- `peer_heartbeat(streams-2, 0, 50.0)`, a busy member
- `peer_heartbeat(streams-9, 0, 0.0)`, idle and outside the ring
- `let ring = ["streams-1".to_string(), "streams-2".to_string()];`
- `assert_eq!(ring_pick("00", &ring), 0, "the ring gives 00 to streams-1")`
- rig `instance: Some("streams-1")`
- `assert!(matches!(open_or_wait("00", 5 s), Ready(_)))`
- `rig.state.runtime.usage.set_absorb_lag(crate::crypto::SegmentHash([0x34; 16]), 120)`. No
  absorber owns this segment, so the lag stays. 120 is above the default 60 s threshold.
- `rig.state.runtime.usage.set_shard_lag("00", 120)`. This makes 00 the victim. The 00
  absorber may overwrite the lag with 0, but the entry remains and `pick_victim_shard`
  still returns it.
- `start_configured`

Steps:
1. `settled(20 s, overrides().contains_key("00"))`
2. `assert_eq!(rig.state.ownership.overrides().get("00").map(String::as_str), Some("streams-2"), "a move target must be a member of the active ring")`
3. Shutdown.

Current-tree trace:
- Tick 1: `lag_hot_ticks` = 1.
- Tick 2: it is 2; `cooled` holds (`last_move` is None).
- `pick_move_target(peer_load, "streams-1", 60)` filters to {s2 (50.0, 0), s9 (0.0, 0)}
  and `min_by` cpu gives s9.
- The victim is "00". The override is CAS-committed; `set_override("00", "streams-9")`.

Expected red:
```
assertion `left == right` failed: a move target must be a member of the active ring
  left: Some("streams-9")
 right: Some("streams-2")
```

#### T3 `the_rings_owner_opens_every_overridden_shard_it_is_assigned_at_the_tick` (red for the §2 consequence, and the pin for the classic move-in)

Setup:
- `seed_ring_of_two(&inner, &[("00", "streams-9"), ("10", "streams-1")])`
- `peer_heartbeat(streams-2, 0, 0.0)`
- ring array; `assert_eq!((ring_pick("00",&ring), ring_pick("10",&ring)), (0, 1), "the ring gives 00 to streams-1 and 10 to streams-2")`
- rig `prefixes: vec!["00".into(), "10".into()]`, `instance: Some("streams-1")`
- `start_configured`

Steps:
1. `settled(10 s, is_open("00") && is_open("10"))`
2. `assert!(is_open("10"), "a move-in the ring honours opens at the tick, not at the first routed request")`.
   This passes on both trees and pins the original eager-handoff contract, which no test
   covers today (`git grep` finds no eager-open test).
3. `assert!(is_open("00"), "the ring's owner opens a shard whose override it ignores at the tick")`
4. Shutdown.

Current-tree trace: eager `e.to == "streams-1"` selects only "10". "00" is overridden to a
non-member, so no tick opens it and there are no requests.

Expected red (`assert!` with a message panics with the message alone):
```
the ring's owner opens a shard whose override it ignores at the tick
```

### 3b. Unit, `src/fleet/tests.rs` (the reviewer's first step, plus the mutant-killing table)

- Add `fn ring(v: &[&str]) -> Vec<String>`.
- Add `&ring(..)` to the four existing calls:
  - `target_is_the_coolest_healthy_peer`: `["a","b","c"]`
  - `target_excludes_self`: `["a","b"]`
  - `no_target_when_every_peer_is_also_lagging`: `["a","b","c"]`
  - `target_must_be_well_under_the_threshold_not_merely_under_it`: `["a","b"]`
- In `target_must_be_well_under_the_threshold_not_merely_under_it`, add the exact boundary:
  peers `[("a",5.0,0),("b",5.0,30)]`, threshold 60, gives `None`.
- New test:
  ```rust
  // Item 34: the ring ignores an override to a non-member, so the idlest
  // heartbeat outside `active` must never be the target.
  #[test]
  fn target_must_be_a_member_of_the_active_ring() {
      let p = peers(&[("a", 90.0, 120), ("b", 40.0, 0), ("streams-9", 1.0, 0)]);
      assert_eq!(pick_move_target(&p, &ring(&["a", "b"]), "a", 60).as_deref(), Some("b"));
      assert_eq!(pick_move_target(&p, &ring(&["a"]), "a", 60), None);
  }
  ```

Expected red on the current tree (a compile-level red; T2 is the behavioural one), once for
each changed call:
```
error[E0061]: this function takes 3 arguments but 4 arguments were supplied
```

### 3c. Pinning for the refactor part (return-home reuses the tick's `active`)

- `an_unreadable_router_report_defers_only_the_desired_publication`: its return-home CAS of
  an aged override under the ring [s1, s2] must still land.
- T2 covers the move path.
- Compile-level proof: `set_view(active.clone(), map)` is required. Without the clone, the
  new uses at return-home and `:989` are E0382 (use of moved value). So the tick provably
  decides from the ring it published.

---

## 4. Edits (one commit, in this order)

Commit title (repo style): *A rebalance move names only a ring member, and an override the
ring ignores never opens on its target*. The body records the three DST reds and the
E0061 verbatim, as 4bb51c0d did.

### 4.1 `src/dst/tests/fleet_controller.rs` (378 → about 560 lines; ceiling 1,000)

The helpers and T1-T3 as in §3a. Optionally extend the `//!` line with "and the tick's ring
decisions (move target, eager handoff)". Each new test is under 100 lines with nesting of
2 or less, and has no `#[expect]`. The file's existing expects are scoped to `r09_…` and
`HeldDocument::enter`, and neither scope is touched.

### 4.2 `src/fleet/tests.rs` (170 → about 196; `#![cfg(test)]`, so it is `production_unchanged` for the planner)

As in §3b. The `owners.json` unresolved-glob row for this file keeps its reason ("pin peer
choice, the return-home budget and victim selection"), which stays accurate.

### 4.3 `src/fleet.rs` (ceiling 1,143 = merge base; budget 0; result 1,143)

**(a) `pick_move_target`, lines 108-123** (outside every expect scope; rustfmt applies):

The doc goes from 5 lines to 4 (−1):
```
/// Rebalance target: the coolest peer that is HEALTHY and a member of `active`. A lagging
/// peer only hands the backlog around (ladder pass 3: 7 moves in 10 minutes of ping-pong), and
/// every ring reader ignores an override to a non-member (`effective_owner`, the router mirror),
/// so such a move is void. `peers` holds every fresh heartbeat, self and non-members included.
```
The signature gains `active: &[String],` after `peers` (+1). The body gains
`.filter(|(n, _)| active.contains(n))` right after the existing health filter (+1). The
existing filter line 120 is left untouched on purpose, so its operators are not re-selected
by `--in-diff`.

Net for (a): +1.

**(b) `start`**. Everything below is inside the `let _ = tasks.spawn(…)` statement,
which rustfmt leaves verbatim: lines 610, 650, 763, 852, 878 and 1103 are 101-298 chars
today, and the precedent's 121-char line survived CI's `fmt --check` (4bb51c0d is an
ancestor of the green ee99975d). Lines are written exactly as below:

| Line | Before | After | Lines | Facts |
|---|---|---|---|---|
| 797 | `state.ownership.set_view(active, map);` | `state.ownership.set_view(active.clone(), map);` | 0 | +2 (`clone` method-call + site) |
| 845-849 | eager comment (5 lines) | 5 lines: "Eager handoff: open any overridden shard the ring assigns / to ME (what `resolve` would admit) now, not at the first / routed request — the open fences the loser's db (ladder p3: / lazy opening left a moved shard unowned for 92 minutes). A / target the ring ignores never opens: it would fence the owner." | 0 | 0 |
| 851 | `let mut mine: Vec<String> = ov.entries.iter()` | `let mut mine: Vec<String> = ov.entries.keys()` | 0 | 0 |
| 852 | `.filter(\|(prefix, e)\| e.to == cfg.instance && !state.shards.is_open(prefix))` | `.filter(\|p\| state.ownership.is_mine(p) && !state.shards.is_open(p))` | 0 | +2 |
| 853 | `.map(\|(p, _)\| p.clone()).collect();` | `.cloned().collect();` | 0 | −3 |
| 891 | `let active = state.ownership.ring_active();` | *(deleted; return-home uses the tick's `active`)* | −1 | −3 |
| 983 | `// Move my laggiest shard to the coolest HEALTHY peer.` | `// Move my laggiest shard to the coolest HEALTHY member of this tick's ring.` | 0 | 0 |
| 989 | `let target = pick_move_target(&peer_load, &cfg.instance, rebalance_lag_secs);` | `let target = pick_move_target(&peer_load, &active, &cfg.instance, rebalance_lag_secs);` | 0 | +1 (path `active`) |

The facts in lines 851-853 total 18 before and 17 after (the `Vec<String>` type facts are
unchanged): `ov`, `keys`×2 and `filter`×2; paths `state, p, state, p`; `is_mine`×2 and
`is_open`×2; `cloned`×2 and `collect`×2.

`start` totals: scope_lines 695 → 694, syntax_facts N → N−1, nested_items unchanged. No
item is added, since closures are not items.

**(c) The `#[expect(clippy::unwrap_used)]` reason on `start` is re-decided.** Three new
fingerprints make the contract grow:
- `unwrap_site:ordinary-call` for the `pick_move_target(&peer_load, &active, …)` call-site
  value, 0 → 1;
- path `active`, +1;
- path `p`, +1.

(Path `state` nets 0, since the `:891` deletion is −1 and `:852` is +1.) This growth is
unavoidable while the call stays in `start`, as in 4bb51c0d. New text, with exactly two
`;` and no `"`:
```
start; a poisoned timing ring may hold a half-recorded wait, the fleet documents serialise infallibly as plain data, an unreadable router snapshot is a typed deferral of the desired CAS, and the move target and the eager move-in are ring decisions over the view this tick published rather than panic sites; recovering the ring, handling the serialisation or aborting the tick on the snapshot would add branches no tick reaches
```

The other five expects on `start` keep their reason text:
- too_many_lines
- excessive_nesting
- cast_sign_loss
- cast_possible_truncation
- let_underscore_must_use

All five stay fulfilled (the fn is about 690 lines; the nesting, the `as u64` sites, the unwrap
sites and `let _ = tasks.spawn` are untouched), and none of their metrics grows:
scope_lines −1, syntax_facts −1, nested_items +0.

No `exception` rows exist for `crate::start` in `source-allowances.json` or
`legacy-source.json`, so re-deciding the reason vacates nothing. Macro-dsl counts for
`crate::start` stay the same: `fleet_io` 9, `serde_json::json` 2, `tokio::select` 2,
`macro_rules` 1.

**fleet.rs line total:** (a) +1, (b) −1, giving **1,143** (budget 0, met exactly).

The lint surface is clean:
- fn ≤ 100: `pick_move_target` is 13 lines; `start` keeps its expect.
- Four args, no bool param, no new nesting, no `_ =>`, no `unwrap` outside tests.
- `redundant_clone` is not triggered, because `active` is used after the clone.
- mt_lint: `active` is not in `NAME_PARAMS`, there is no `.stream_ref(`, and no new
  String-keyed map type.
- Rustdoc names private items only in backticks.

Fallback if `cargo fmt --check` ever wants to reformat an edited statement in `start`
(it does not today): take rustfmt's output, then pay any extra line and facts by inlining
the yield's `let held: Vec<String> = state.shards.held_prefixes(); for prefix in held {`
into `for prefix in state.shards.held_prefixes() {`. That is −1 line and −3 facts
(`Vec<String>`×2 and `held`), and it adds no operators.

### 4.4 `docs/SCALING.md` §Rebalancer (lines 216-220, prose)

- "coolest fresh peer" becomes "coolest fresh, healthy member of the active ring".
- "`effective_owner()` = override (if target live) else rendezvous" becomes "override (if
  its target is in the active ring) else rendezvous — an override to a non-member is void:
  it is never written and its target never opens the shard".
- Add: the instance the ring assigns opens it at its next tick (eager handoff).

### 4.5 `docs/refactor/test-inventory.json`

Regenerate with `python3 scripts/test-inventory.py --write` (§6).

**Other ceilinged files are not touched** (wc -l = merge base): http.rs 3,362; product.rs
4,205; shard.rs 3,196; billing.rs 2,201; history.rs 1,713; auth.rs 1,676; registry.rs
1,492; sse/feed.rs 1,170. `ownership.rs` (264) and `fleet/planning.rs` (38) are
unchanged. `planning.rs` has no mutation-owner row, and editing it would need one.

---

## 5. Mutation analysis (cargo-mutants 27.1.0, `--in-diff`, `--timeout 90`)

Changed critical sources: `src/fleet.rs` only. It belongs to owner `fleet`, with filters
`fleet:: dst_tests::fleet_controller:: dst_tests::runtime_isolation::`.
- `src/fleet/tests.rs` is `#![cfg(test)]`, so the planner marks it `production_unchanged`.
- `src/dst/**` is not under a critical prefix.
- `docs/` is not Rust.

No owner row or filter changes. Every killer below matches `fleet::tests::` or
`dst_tests::fleet_controller::`.

The fn bodies that change are `pick_move_target` and `start`. `start_configured`, `rss_bytes`,
`return_home_allowed`, `pick_victim_shard` and `fresh_edge_p50` are untouched.

| # | Mutant (in-diff) | Why selected | Killed by |
|---|---|---|---|
| 1 | `replace pick_move_target -> Option<String> with None` | FnValue span covers inserted filter line | `fleet::tests::target_is_the_coolest_healthy_peer` (expects `Some("b")`), `target_excludes_self`, `target_must_be_a_member_of_the_active_ring`, T2 |
| 2 | `… with Some(String::new())` | same | `no_target_when_every_peer_is_also_lagging` (`None`), `target_must_be_well_under…`, `target_must_be_a_member…` 2nd assert, coolest (`Some("b")`) |
| 3 | `… with Some("xyzzy".into())` | same | same as #2 |
| 4 | `replace start with ()` | FnValue span covers every edited body line | T1/T2/T3 and `an_unreadable_router_report…` (ring never published; they fail after their 10-20 s settle), `r09_…` (entered-wait `expect`), `runtime_isolation::two_runtimes_never_share_fleet_state` (no heartbeat docs). This is the same mutant 4bb51c0d had killed |
| 5 | `replace && with \|\| in start` (line 852) | operator on inserted line | **T1**: `is_mine=false \|\| !open=true` opens 00, so `started` becomes 1 ≠ 0 at the tick-2 check (about 4-6 s) |
| 6 | `delete ! in start` (line 852) | operator on inserted line | **T3**: `is_mine && is_open` is never true for an unopened shard, so neither 00 nor 10 opens and the settle fails at 10 s |

There are no other mutants:
- Lines 797, 851, 853, 989, the deleted-line neighbours 890/891' and the comment lines have
  no operators.
- The new `.filter(|(n, _)| active.contains(n))` has no operator.
- cargo-mutants does not mutate closures' return values, method calls or clones.

There are no equivalent mutants and no timeouts. Each mutant fails within about 20 s, far
under the 90 s per-mutant timeout, and the new tests run in parallel with the existing ones,
so the baseline stays of the same order.

Insurance: if the driver ever treats unchanged line 121 (old 120) as selected, its operators
are already killed.
- `!=→==`: `target_excludes_self`
- `&&→||`: `no_target…`
- `<→==` and `<→>`: coolest
- `<→<=`: the new exact-boundary assertion (lag 30, threshold 60)
- `/→%`: coolest (`60%2=0` means always `None`)
- `/→*`: `no_target…` (lags 70-90 < 120)

---

## 6. Ledgers (same commit)

- `docs/refactor/test-inventory.json`: `python3 scripts/test-inventory.py --write`.
  - It adds 3 entries (T1-T3, file `src/dst/tests/fleet_controller.rs`, attribute
    `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`, configuration captured
    automatically).
  - It re-hashes `an_unreadable_router_report_defers_only_the_desired_publication`, because
    its `peer_heartbeat` call gains `0.0`.
  - Nothing else moves.
- `docs/refactor/review-mechanisms.json`: **unchanged**. The only pinned fleet DST test,
  `r09_fleet_cancels_entered_documents_without_partial_authority_or_lost_retry`
  (sha `3d48ff56…`), is not edited. It never calls `peer_heartbeat`, and the sha covers the
  function only.
- `docs/quality/owners.json` and `docs/quality/source-allowances.json`: **unchanged**. No new
  macro-dsl, effect, global or glob; `crate::start` macro counts are unchanged; no row is
  vacated; there are no exception rows for `start`.
- `docs/refactor/architecture-policy.json` and `docs/refactor/WIRE-MATRIX.md`: unchanged
  (no wire change).
- Scenario map and dispositions: unchanged (no renames).
- `src/dst/tests/README.md`: unchanged (no new module).
- `scripts/quality/mutation_owners.py`: unchanged.
- `docs/SCALING.md`: the prose update in §4.4.

---

## 7. Controls (run by the implementer, never concurrently with another mutation run)

1. **Red, with the DST tests and helpers only (§4.1) applied:**
   `cargo test --locked --lib dst_tests::fleet_controller:: 2>&1 | tee target/item34-red-dst.log`.
   Expected: `test result: FAILED. 2 passed; 3 failed`. The three panics are exactly the
   §3a messages: `left: Number(1)` / `right: Number(0)`,
   `left: Some("streams-9")` / `right: Some("streams-2")`, and
   `the ring's owner opens a shard whose override it ignores at the tick`.
2. **Red, after adding §4.2:** `cargo test --locked --lib fleet::tests::` gives
   `error[E0061]: this function takes 3 arguments but 4 arguments were supplied`, pointing
   at `src/fleet/tests.rs`.
3. **Green, after §4.3:**
   - `cargo test --locked --lib fleet::tests::`: `test result: ok. 16 passed; 0 failed`
   - `cargo test --locked --lib dst_tests::fleet_controller::`: `ok. 5 passed; 0 failed`
   - `cargo test --locked --lib dst_tests::runtime_isolation::`: ok
   - `cargo test --locked --lib ownership::`: ok (unchanged)
4. `cargo fmt --all -- --check`: no output, exit 0. If it reports a diff inside `start`,
   apply the §4.3 fallback and recount.
5. `wc -l src/fleet.rs`: `1143 src/fleet.rs`.
6. `cargo clippy --locked --workspace --all-targets -- -D warnings`: exit 0, no warnings,
   and no `unfulfilled_lint_expectations`.
7. `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items`: exit 0.
8. `python3 scripts/test-inventory.py --write && python3 scripts/test-inventory.py --check`: exit 0.
9. After committing: `QUALITY_BASE_REF=HEAD~1 bash scripts/quality.sh` prints `QUALITY_OK`.
   `QUALITY_BASE_REF=HEAD~1` scopes the local merge base to this commit. That equals
   origin/slate today, and it stays correct if other unpushed work is stacked below this
   commit. In particular it must show:
   - no `file growth: src/fleet.rs`;
   - no `accepted exception grew … start` for the five unchanged reasons;
   - no `exception needs owner; invariant; alternative`.
10. `QUALITY_BASE_REF=HEAD~1 QUALITY_MUTANTS_OUT=target/quality-mutations-item34 bash scripts/quality/mutations.sh`.
    Expected:
    - `selected-owners.json` owners `["fleet"]`, changed sources `["src/fleet.rs"]`;
    - the baseline passes;
    - `6 mutants tested: 6 caught` (0 missed, 0 timeout, 0 unviable), matching §5 rows 1-6.
11. Push, then `gh run view` on the push run. CI is never claimed green without it.

---

## 8. Out of scope

- **The reviewer's "remove the always-false draining filter" (`fleet.rs:614 && !other.draining`).**
  It is not needed, because the budget is met by deleting the `:891` re-read.
  - It is not always false on the wire. `draining` is a required field of bucket-writable
    heartbeats, and COMPUTE-SPEC R5 reserves it as the drain signal. This binary always
    writes `false` (`:581`), but the filter is the reader half of a specified protocol.
    Removing it changes policy and should be a separate item.
- **Dropping void overrides at once.** An override whose target left the ring is now inert
  everywhere, but it still sits in `overrides.json` until return-home judges its home
  healthy. If the target later rejoins (scale-out), the override comes back into force and
  moves the shard with no rebalancer decision. This is pre-existing behaviour and belongs in
  its own item.
- `pick_victim_shard` accepts a zero-lag served shard while `my_lag` comes from segment
  absorb lag. This is pre-existing and unrelated to target choice.
- Flagging misnamed (non-ordinal) instances that heartbeat into a fleet bucket. After this
  change they are never targets, but nothing reports them.
- The pilot router mirror (`src/bin/pilot/proxy.rs`) already honours active-only overrides.

---

## 9. Decisions for Søren

Nothing changes at the product or raw edge (no wire, status, body or metric changes), so
there is no edge decision. One internal policy point follows from I2, with a recommended
default so the work is not blocked:

1. **The eager-open admission is `is_mine` (the `resolve` admission), not "`to` names me and
   is a member".**
   - Consequence: when an override's target leaves the ring (scale-in, or dark for more
     than 30 s and so wedged), the rendezvous home opens that shard at its next tick instead
     of at the first routed request. That fences a wedged target's zombie compactor or GC
     at once, which is the ladder-p3 reason the eager handoff exists. The cost is one cold
     open per such shard at scale-in. T3's `00` assertion pins this.
   - **Recommended: accept.**
   - Alternative, the honoured-move-ins-only form: `e.to == cfg.instance &&
     active.contains(&e.to) && !is_open`. It opens nothing new, but:
     - it copies the honour rule outside `OwnershipService`;
     - it costs +4 facts in `start`, so it needs the held-inline offset and one more;
     - it adds `==`, `&&`×2 and `!` mutants;
     - T3's `00` assertion is dropped.

---

## Skeptic corrections (C1..C4)

These were checked read-only against HEAD `2fb92fb9`, where `origin/slate` is also `2fb92fb9`
(`git merge-base` = HEAD). I did not run cargo or any repo scripts.

### Verified as written (no change needed)
- **Line counts.** `wc -l`: fleet.rs 1,143, which is also the ceiling
  (`min(max(1000, legacy 1,248), max(1000, base 1,143))`, source_rules.py:226-228).
  The other files are http.rs 3,362, product.rs 4,205, shard.rs 3,196, billing.rs 2,201,
  history.rs 1,713, auth.rs 1,676, registry.rs 1,492, sse/feed.rs 1,170,
  fleet/tests.rs 170, dst/tests/fleet_controller.rs 378, ownership.rs 264 and planning.rs 38.
- **Quoted source lines.** fleet.rs:113-123, 614, 753, 797, 827, 845-853, 891, 903, 926, 983
  and 989 all match the plan. So do ownership.rs:69-99, planning.rs:6-22 and
  proxy.rs:61-62. `open_or_wait` performs no ownership check (shard_directory.rs:303-305),
  and `started` is incremented before the spawn (sharddir.rs:517). So T1's
  `left: Number(1)` red holds: at tick 2 the gate returns `Wait` or closing (strike 1 gives a
  6 s holdoff, holdoff.rs:34-40 and sharddir.rs:272-275) before it reaches the counter.
- **FNV-1a.** `"00 streams-1"` = 2684974198 and `"00 streams-2"` = 2668196579, so
  `ring_pick("00")` = 0. `"10 streams-1"` = 1714687627 and `"10 streams-2"` = 1731465246, so
  `ring_pick("10")` = 1.
- **Use-site list.** It is complete. I grepped all of src, including src/dst and cfg(test)
  code, for `pick_move_target`, `effective_owner`, `is_mine`, `foreign_owner`,
  `ring_active()`, `set_view` and `e.to`.
- **Fact arithmetic.** Lines 851-853 go from 18 facts to 17. `start` goes from 695 scope lines
  to 694 (433..1127) and from N syntax facts to N−1. `nested_items` does not change.
- **unwrap_used fingerprints on `start`.** Three keys are new or grow:
  - `unwrap_site:ordinary-call` for the new `pick_move_target(..)` token stream, 0 → 1;
  - `unwrap_site:path` for `active`, +1;
  - `unwrap_site:path` for `p`, +1.

  `state` nets out to 0. No enclosing path-call site covers the edited lines: the task body
  is the argument of the method call `tasks.spawn`, and method-call sites are fingerprinted
  only for unwrap and expect. The new reason has exactly two `;` and no `"`. A changed
  reason is a new identity, so `exception_growth` skips it (source_rules.py:207-208). The
  other five expects only shrink.
- **Other ledgers.** None needs a change:
  - No ledger pins the four edited `fleet::tests` functions (grep of docs/ and scripts/ is
    empty).
  - test-additions.json needs no row for new DST tests (the precedent 4bb51c0d added none).
  - review-mechanisms pins only `r09_…` by its own function hash, and has no
    `support_functions` for this file.
  - mt_lint excludes src/dst (mt_lint.rs:49).
  - architecture-policy `function:src/fleet.rs::start` `limit: 687` is a ceiling, and the
    function shrinks by one line.
  - The legacy-diagnostics `unreachable_pub` row for `crate::pick_move_target` is
    fingerprinted by the primary-span line excerpt (diagnostics.py:92-96). Line 113 stays
    `pub(crate) fn pick_move_target(`.
- **Controls.** Every control is buildable. `QUALITY_BASE_REF` (common.py:24,100) and
  `QUALITY_MUTANTS_OUT` (mutations.sh:7) exist, and the fleet owner filters
  (mutation_owners.py:148-149) match T1-T3.

### C1 — Mutation analysis misses two selected operator mutants (fix before implementing)

**What cargo-mutants 27.1.0 selects.** In `--in-diff`, a *deleted* line marks the new-file
line before it (`~/.cargo/registry/src/*/cargo-mutants-27.1.0/src/in_diff.rs:247-254`). The
test `affected_lines_from_replacement` (:497-517) shows that a replaced line `i` affects
`i-1` and `i`. So every modified line also selects its predecessor. §5 audited only the
inserted lines.

**Where it bites.** The §4.3(b) row for **983**, a comment-only edit, therefore selects
fleet.rs:**982**, `if lag_hot_ticks >= 2 && cooled {`. That line carries two operators
(visit.rs:587 and 592):
- **M7** `replace >= with < in start`
- **M8** `replace && with || in start`

Neither has a deterministic killer inside the owner filters:
- T2 asserts only the target. Under M7/M8 the move fires at tick 1 instead of tick 2, still
  to `streams-2`, and T2 passes.
- T1's instance holds nothing, so there is no victim.
- `an_unreadable_router_report…` has already yielded 00 when the rebalancer runs, so `served`
  is empty.
- `r09_…` runs a single instance with no peers, so there is no target.
- `runtime_isolation` also has no peers.
- T3 kills M7/M8 only if its 20 ms `settled` poll misses the sub-millisecond window between
  the second eager open and a tick-1 move of an absorber-published victim. That is a race,
  and a lost race is MISSED, which fails the leg.

**Remedy.** Drop the 983 row. Keep `// Move my laggiest shard to the coolest HEALTHY peer.`
verbatim; the ring rule already lives in the new `pick_move_target` doc. If the wording is
wanted, edit only lines 984-988, whose predecessors are comment lines. This changes neither
the line budget nor the fact counts.

**Add to §5 the predecessor audit for every hunk:**

| Predecessor line | Content | Operators |
|---|---|---|
| 107 | `}` | none |
| 796 | comment | none |
| 844 | blank | none |
| 850 | `{` | none |
| 890 and new 891 (around the deletion) | casts and a `Vec::new()` | none |
| 988 | comment | none |

With C1 applied, control 10's `6 mutants tested: 6 caught` holds. Before the full run, list
the selection read-only and require exactly §5 rows 1-6:
`cargo mutants --list --in-diff <pr.diff> --file src/fleet.rs`, or read
`target/quality-mutations-item34/fleet/selected.json`.

### C2 — §9 must name the unnamed-instance consequence of the `is_mine` admission

`OwnershipService::effective_owner` returns `None` when the instance name is empty
(ownership.rs:78-80), so `is_mine` is `true` for every prefix (ownership.rs:97-99). Nothing
prevents this configuration:
- fleet mode needs only a fleet store (fleet.rs:402-404);
- `INSTANCE_NAME` is free-form and nothing validates it as non-empty (cli.rs:412-414;
  there are no `instance_name` checks in src/config/validation.rs).

With the new predicate, such an instance eager-opens **every** override key that is not
open, 16 per tick in rotation. Each open fences whoever holds that shard. The old predicate
opened only keys whose `to` equals `""`.

The §2 sentence "resolve already opens anything" is true, but that happens per routed
request, while this happens proactively every tick. Add it as a §9 sub-point.
- **Recommended:** accept it as an unsupported configuration.
- **Alternative:** spell the admission `state.ownership.effective_owner(p).is_some_and(|o| o == cfg.instance)`.
  - It returns false when the name is empty.
  - It costs +4 facts versus `is_mine`, per §2's arithmetic.
  - It adds one `==→!=` mutant. T1 kills it: `Some("streams-1") != "streams-9"` opens 00.

Also add to §8: override keys come from the bucket-writable `fleet/overrides.json`, and the
eager block opens them without checking `state.shards.prefixes()`. Before, only keys naming
this instance were opened. Now void keys whose rendezvous home is this instance are opened
too. This is pre-existing and broadened, so it is out of scope.

### C3 — T2 is not a pin for the two-tick gate (documentation only)

§3c lists T2 as covering "the move path". It does not pin `lag_hot_ticks >= 2`, `cooled` or
the cooldown: it passes whether the move happens at tick 1 or at tick 2 (see C1). State this
so no one relies on T2 for that gate in later edits of line 982.

### C4 — Control 1 / commit body wording

The T3 red is the bare panic message (`assert!` with a format string). T3's step-2
`is_open("10")` assertion passes on the current tree, as §3a says. Record that explicitly in
the commit body next to the two `left/right` reds, so the reviewer does not expect three
`assertion \`left == right\`` blocks.

### Verdict

**ready-with-corrections.** The design (typed `active` parameter, `is_mine` eager admission,
reuse of the tick's `active`) is sound and fits the 1,143-line budget exactly. The red tests
T1-T3 fail on today's tree with the stated messages and pass after the change, all bounded
(20 s / 10 s settles, cooperative shutdown). C1 is mandatory: as written, the 983 comment
edit would put two operator mutants with no deterministic killer into the in-diff set. C2 is
a decision record. C3-C4 are wording.
