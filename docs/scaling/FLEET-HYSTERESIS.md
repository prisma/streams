> **Repository copy** (push 4, 2026-11-10) of the design document `FLEET-HYSTERESIS.md`, rev 1 (2026-11-09): the fleet count hysteresis, as it stood when the code landed. Where it and the code differ, the code is the authority; later decisions are in the edge records (`docs/reviews/2026-09-hardening/edge-changes.md`), the RUNBOOK and the module docs. Paths under `~/.streams-k2/` and the sibling documents it cites (reviews, decisions, run results) are the design work's own files outside this repository.

# Fleet count hysteresis (design note, rev 1, 2026-11-09)

**For:** the lead, then Fable 5.1. **Tree:** slate 48597a882, read only (worktree `~/.streams-k2/wt/fleet-hyst`). **Status:** design only; implementation after Fable's check.

**Inputs:**
- package 2's interim D ramp on 48597a882 (8 shards, `FLEET_MIN..FLEET_MAX` 1..8, default scaling knobs), runs `~/.streams-k2/wt/p5-ramp/bench/ramp/results/dramp-each{,-r2}`;
- Compute's answer of 2026-11-09 (external mode: Streams sets its own count and the platform applies it within a second, with no delay of its own);
- SCALING-DESIGN rev 4 §4.3.

`p5-0-numbers.md`, named in the brief, is not on disk. The timeline below is read from the runs' `scaler.jsonl`.

## 1. What flaps, measured

Run `dramp-each-r2`, 10,000 streams, one tenant each. `desired` is the published count, `live` the members it reads; the readings are the published `reason`.

| t (s) | desired | live | cores | in-flight | the binding dimension |
|---|---|---|---|---|---|
| 630 | 1 → 3 | 1 | 0.26 | 241 | slots → 3 |
| 693 | 3 → 1 | 3 | 0.43 | 36 | slots → 1 (after 60 s below) |
| 698 | 1 → 4 | 3 | 0.49 | 413 | slots → 4 |
| 767 | 4 → 2 | 4 | 0.55 | 47 | slots → 1, shrink → 2 |
| 772 | 2 → 5 | 4 | 0.56 | 449 | slots → 5 |
| 779 | 5 → 8 | 5 | 0.74 | 749 | slots → 8 |
| 838 | 8 → 2 | 8 | 0.69 | 44 | slots → 1, shrink → 2 |
| 843 | 2 → 6 | 8 | 0.56 | 628 | slots → 6 |

`dramp-each` (the first run) is the same shape: 3, 2, 3, 4, 5, 8, 3, 6, at 0.26-1.52 cores.

**The mechanism:**
- **One dimension drives every change:** in-flight against the edge slots, `slots = ceil(Σ in-flight / (0.75 × 140))`. Utilization never asked for more than 1-3, and no member was hot, latent or refused.
- **Nothing filters a short spike:** a single tick's in-flight sets the target. Scale-out is published at once (`fleet.rs:854-870`); only the latency and hot dimensions carry a sustain (`SCALE_LAT_SUSTAIN_SECS`, `SCALE_CPU_SUSTAIN_SECS`, 20 s each).
- **Scale-in jumps:** after `SCALE_IN_SECS` (default 60 s; its doc says "pilot-scaled from the spec's 10 min") the fleet drops straight to the conservative target, 8 → 2 in one step.
- **It feeds itself:** every count change moves shards (the ring, then `count_balance`, one move per `REBALANCE_MOVE_COOLDOWN_SECS` = 60 s). Moved shards refuse and retry writes, so in-flight rises, which drives the next change. In-flight read between two moves is low (36, 44, 47), which drives a shrink.

So the fix is hysteresis on the count: no new signal, no new knob beyond what §3 lists.

## 2. Where the decision lives, and the constraint on changing it

- **`planning::Demand::plan`** (`src/fleet/planning.rs:279-357`): a pure function. It takes the tick's fold of fresh, non-draining members' beats (`Loads`) and returns `need`, the grow target, and `shrink`, the conservative target. It already keeps state across passes (`latency_since`, `hot_since`).
- **The publish decision** sits inside `fleet::start`, at `fleet.rs:854-870`: grow at once when `need > cur`, shrink to `shrink` once it has been below `cur` for `cfg.scale_in`.
- **The constraint.** `fleet::start` has an owner-approved row in `docs/quality/exception-growth.json` (`crate::start`, `clippy::unwrap_used`, scope_lines 371). The row freezes the scope exactly, so any edit to `start`, a shrink included, fails the gate as "stale exception growth row". Changing the decision inside `start` therefore needs the owner to re-pin that row.

**Proposal (option A): leave `start` byte-identical.**
- Put all hysteresis inside `Demand::plan`, which `start` already calls, so `need` and `shrink` arrive pre-filtered.
- Change `SCALE_IN_SECS`'s default.

Everything §3 asks for can be expressed there:
- `start` publishes `need` as soon as it exceeds the count, so a sustained `need` gives sustained scale-out.
- `start` publishes `shrink` after `SCALE_IN_SECS` below the count, so a `shrink` bounded to one below the members gives one step per window.

**Option B, if Fable prefers it:** extract the publish decision into a new pure `fleet::hysteresis` that `start` calls. That is clearer, but it shrinks `start`, so the owner must re-pin the frozen row first. I recommend A.

## 3. The rules

The sizes follow Compute's advice, which is the asymmetry of Habanero's `auto` mode (ADR 0071), at our own costs. The fleet tick runs every 2 s (`TICK_PERIOD`).

| # | Rule | Value | Why this size |
|---|---|---|---|
| H1 | **Scale-out on load that lasts.** The grow target is the count every tick of the trailing window wanted (the minimum of the per-tick wanted counts). | `SCALE_OUT_SUSTAIN` 30 s (15 ticks); a constant, not a knob | Compute's 30 s. Longer than any spike in the timeline (the troughs and peaks between publications last 5-7 s), shorter than one shard move's cooldown (60 s), so real load still grows the fleet within a minute |
| H2 | **The sustained dimensions keep their own sustain.** Latency, hot CPU, refusal streaks and RSS at 80% of the shed line already require 20 s (`SCALE_LAT_SUSTAIN_SECS`, `SCALE_CPU_SUSTAIN_SECS`) and stay the emergency path. | unchanged | These are the "the instance is struggling" signals; they should not wait 30 s more |
| H3 | **Scale-in only after the conservative target has held below the count for the whole window, then one instance.** `shrink` is at least `live − 1`. | `SCALE_IN_SECS` default 60 → **300** | Compute's "5 min below half the target": the conservative target already uses `SCALE_IN_CPU_PCT` 50 against the scale-out 75. The doc's own "spec's 10 min" was cut to 60 s for a pilot |
| H4 | **One step at a time, after the hand-off.** A draining member leaves `live` at once (drains are not folded), so the next step's 300 s window starts after the count change that removed it. The drain bound (`drain.rs`) and the count balancer's move cooldown (60 s) end well inside it. | follows from H3 | The platform SIGTERMs, the instance drains and hands off within the stop deadline; one step per 5 min means at most one drain in flight |
| H5 | **No reversal inside the window.** After a scale-out, a scale-in needs H3's full window measured from the change; after a scale-in, a scale-out needs H1's window measured from the change. | from H1 and H3: ≥ 300 s and ≥ 30 s | Compute's "a cooldown after any change before reversing it". Growth reverses faster on purpose: a wrong shrink costs refusals, a wrong grow costs an instance-minute |
| H6 | **In-flight read during a hand-off is not load.** The slots dimension's window restarts when `live` changes, and counts again only one move cooldown later. | 60 s settle (`REBALANCE_MOVE_COOLDOWN_SECS`) + H1 | The measured spikes come from moved shards refusing writes. Utilization, latency and hot stay counted throughout, so a real overload during a hand-off still grows the fleet |

**What the measured run would have done** (a replay of the published readings, each held until the next publication):
- 241 in-flight held 58 s → **3** at 660 s;
- 36 for 5 s → nothing;
- 413 held 64 s → **4** at 728 s (with H6: 790 s);
- 47, then 449, each under 30 s → nothing;
- 749 held 54 s → **8** at 809 s;
- 44, 628 → it stays at 8, since scale-in needs 300 s.

That is three monotonic changes against eight with four reversals. Whether the 10,000-stream step then passes is unmeasured: package 2's ramp re-run is the acceptance.

## 4. With shard splits and merges (SCALING-DESIGN rev 4 §4.3)

- **Three controls, three timescales:**
  - the fleet count responds first (out 30 s, in 5 min per step);
  - a normal split takes 10 min at 70% of safe capacity (emergency 45 s at 90%);
  - a merge takes 6 h below 25%, never within 12 h of the split it reverses.
- **The ordering holds:** the count settles long before the topology moves. A burst of a few minutes grows the fleet and never reshapes the topology. A quiet hour shrinks the fleet by up to 12 steps (8 → 1 takes 35 min) long before any merge is eligible.
- **The count stays clamped to the shards** (`need ≤ shards`, as today). Demand past them is published as `split_demand` for S4's split to read. A split adds shards, so the clamp rises and the sustained `need` above it is published at the next tick. No hysteresis rule waits on the topology.
- **One operation at a time per cell.** Today no topology operation exists on a native cell: S4's splits and merges are not built, and `scaler3` only splits stream key segments. When S4 lands, a count change and a topology operation should not overlap. Neither starts while the other's hand-off is settling, using H6's settle window and §4.3's 60 s topology cooldown. Recorded here as an S4 obligation, not built now.
- **`FLEET_MIN` stays the floor.** Compute's earlier note ("Streams' desired count shrank the ring below the serving instances until `FLEET_MIN` was pinned") is what H3 and H4 address without a pin: the count falls one instance per window, behind the drains.

## 5. Red first

**The test.** A deterministic replay in `src/fleet/planning.rs`'s tests, or a new `src/fleet/hysteresis_tests.rs`, of the r2 timeline.
- It feeds per-tick `Loads` on an injected clock (`Demand::plan` already takes `now: Instant`).
- It applies `start`'s publish rule (`need > cur` publishes; `shrink < cur` for `SCALE_IN_SECS` publishes), reproduced as the test's model.
- It asserts the published counts exactly: `[3, 4, 8]`, no reversal, and no scale-in within 300 s of a scale-out.
- On today's tree it fails with the measured eight-change sequence.

**Whether to call it a DST.** A test over the real tick (like `dst::dst_tests::fleet_desired`) would need minutes of real 2 s ticks per case. If Fable wants the tick itself pinned, add one short DST: two peers' beats alternate a 5 s in-flight spike with a trough under a 60 s scale-in. The first `desired.json` stays at its seed for one H1 window and moves once after it.

**Commits, one behaviour each:**
1. The sustained grow target, H1 and H6, with the replay red.
2. The one-step shrink and the 300 s window, H3 and H4, red on the 8 → 2 jump.

`SCALE_IN_SECS`'s new default is an operator-visible config default. It goes in commit 2 with:
- the defaults table in `RUNBOOK.md:241` (compiled into the binary);
- `docs/STAGING.md:190`, which sets 60;
- the K2 field rig's `bench/k2cost/field/deploy.py:119`, which pins 60. Whether that pin follows is a measurement question for its owner.

The bench rigs that pin it high on purpose, to stop shrinks (`bench/docker/compose.yml`, `bench/fleet/*.sh`), stay as they are.

**Ladder:** focused tests, clippy, gate, the local mutation leg for `fleet_planning` (and `fleet` only under option B), the Linux suite, all through `locked.sh`. Acceptance: package 2's interim D ramp re-run at 10,000 streams.

## 6. Client-visible?

No wire change: no route, code, header or field changes. A client sees fewer refusals during ramps, and no scaling happens on a burst shorter than 30 s.

Operator-visible:
- the `SCALE_IN_SECS` default, and a slower scale-in (one instance per 5 min);
- `fleet/desired.json` changes less often, with the same reason string.

My reading: no edge record, since the scaling posture is server-internal; no edge record names `SCALE_IN_SECS` today. The lead decides.

## 7. Open for Fable

1. **Should in-flight alone drive the count at all when CPU is low** (0.26-0.74 cores here)?
   - The slots dimension models the Compute front door's ~145-150 admitted concurrent requests per instance (`SCALE_EDGE_SLOTS` 140).
   - In this rig, in-flight was store-latency-bound: cold opens of 10,000 tenants' streams. Little's law, not admission.
   - A stricter rule: count in-flight only while the edge reports queueing (`edge_p50` above its line) or CPU is at least the scale-in line. I left it as is, with H1 and H6, because the edge cap is real on Compute.
2. **Option A vs B** (§2), and whether the frozen `start` row should be re-pinned anyway, to move the decision out of the tick.
3. **H6's settle signal** is `live` changing. A rebalancing move between members without a count change does not restart the slots window. Is the 30 s window enough there, or should `Loads` gain the overrides' youngest age? That needs a field folded in `start` (option B).
