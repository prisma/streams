> **Repository copy** (push 4, 2026-11-10) of the design document `S5-PLAN.md`, the P2c session's plan of 2026-11-09: split and merge triggers and their coupling to the fleet, as it stood when the code landed. Where it and the code differ, the code is the authority; later decisions are in the edge records (`docs/reviews/2026-09-hardening/edge-changes.md`), the RUNBOOK and the module docs. Paths under `~/.streams-k2/` and the sibling documents it cites (reviews, decisions, run results) are the design work's own files outside this repository.

# S5 plan: split and merge triggers, and their coupling to the fleet (P2c session, 2026-11-09)

Sources: SCALING-DECISIONS §2 S5, Q4, Q5 and §4; SCALING-DESIGN rev 4 §4.3 (which replaces Q9's
constants); S4-DESIGN §4.4, §10 and §10.1; FLEET-HYSTERESIS-REVIEW §8; RULES.md, "Fast mode for #149".
S4 status, from package 5 (2026-11-09): no S4 code yet. The branch is `s4-split`
(~/.streams-k2/wt/s4-split, on s3a-4a b6dc580c7), commits 3 (topology) and 5 (intent) first; S5 stacks
after 5. Names as designed:
- `Topology` in `src/topology/model.rs`; the directory's `RwLock<Arc<TopologySnapshot>>`; leaves are
  bit-prefix strings, as `registry::shard_for_hash` uses.
- The intent `topology/intent.json`: non-terminal means `state == Preparing`; read by ETag once a tick.
- The split's internal entry is §6's state machine; its name comes with S4 commit 11. S5 calls that
  entry, not the route.
- S4's heartbeat fields are `reserved_shards` and `topology_intent`, so S5's are named apart.
- There is no merge until S6. S4's first intent writer adds the no-shrink rule to `Publisher::decide`.
Until commit 5, S5 builds what does not depend on S4: the pure policy and its tests, the `ShardLoad`
field and its counters, the configuration, and Q5's fence test. It does this on s3a-4a, per fast mode.

## 1. Code

**`src/topology/policy.rs`, new and pure, with unit tests beside it.** It runs over one tick's view, on
the tick's clock, like `planning::Demand::plan`.
- Input `TopologyView`:
  - the leaves: prefix, depth, born_ms, owner, and each leaf's `ShardLoad`;
  - siblings by prefix;
  - the live intent (yes or no), and the last operation's end;
  - desired.json's `computed_at_ms`, and the youngest override's age;
  - per instance: resident shards, `SHARDS_PER_INSTANCE_MAX`, RSS headroom, fresh or stale;
  - the live instance count and `FLEET_MAX`;
  - congestion in the window (`503 shard_busy`, a queued-bytes refusal, or a shed).
- Output: one `Decision` per tick for the leaves this instance owns:
  - `Split { prefix, kind: Normal | Emergency | Doubling }`, `Merge { pair }`, `AskInstance { reason }`,
    or `Hold { reason }`;
  - each with its reason segments, for the heartbeat and the desired reason (E6).
- State across ticks: per-leaf sustain starts for the normal and emergency lines, per-pair merge sustain
  starts, and the last operation's time.
- Rules, rev 4 §4.3 exactly:
  - **Normal split:** a leaf at 70% or more of its safe capacity on any axis for 600 s of fresh beats.
  - **Emergency split:** at 90% or more for 45 s.
  - **Broad growth doubles:** with half or more of the leaves over the line, every leaf splits, one leaf
    per operation, deepest first. Otherwise one hot leaf splits.
  - **Merge:** two sibling leaves, combined below 25% on every axis for 21,600 s, neither younger than
    43,200 s, and no override on either younger than `REBALANCE_MOVE_COOLDOWN_SECS`. The owner of `…0`
    acts. Deepest pairs first.
  - **Serialisation:** one operation per cell, 60 s between operations.
  - **Contraction guard:** a stale or missing beat (over 30 s) from either owner, or congestion anywhere
    on the cell in the window, blocks every merge. It never blocks a split.
  - **Depth:** a 24-bit prefix never splits.
  - **At the top:** beyond 64 leaves, or at `FLEET_MAX`, the answer is `Hold { topology_at_max }`.
  - **Hot entity:** one stream or queue carrying 50% or more of a leaf's load on the binding axis gives
    `Hold { hot_entity }`. A split can't help it.
  - **Backlog is never an input:** stored payload bytes are not load.
- **Reserve, then split, then place:**
  - transition capacity on the actor: parent plus two children within `SHARDS_PER_INSTANCE_MAX`, and RSS
    headroom of at least 2 × open transient plus 2 × busy shard (S4 §10's check);
  - destination capacity on another live instance for one child.
  - If either is missing: `AskInstance { instance_full }`, with heartbeat `split_refused: "instance_full"`
    and the desired reason's ` shards_per_instance=<n>/<max> ` segment (Q4). The split waits for the
    instance to join.
- **Fleet coupling, rule 1 of S4 §10.1:** an operation starts only when desired.json is at least 60 s old
  and no override anywhere is younger than `REBALANCE_MOVE_COOLDOWN_SECS`. This applies to the
  emergency split too.
  - Rule 2, no scale-in while an intent is live, is S4's or S6's, as assigned.
  - Rule 4: the clamp stands; `split_demand` is the sustained demand past the shards.

**The policy loop, `src/topology/policy_loop.rs` (name to fit S4's module).**
- A supervised task started from `fleet::start_configured`, beside `heartbeat::start` and
  `count_balance::start`. That is one line in a function with no growth row; `crate::start`, which has a
  frozen row, is not touched.
- Cadence 10 s. Each tick it reads:
  - the heartbeat set (as count_balance does);
  - desired.json and the intent, both cached by ETag;
  - this instance's leaves from the directory.
- It builds `TopologyView` and runs the policy.
- On `Split`, it calls S4's split entry, the same function `POST /operator/topology/split` calls. It
  handles `409 topology_busy`, `409 instance_full` and `422 not_splittable` as `Hold`, with the reason.
- On `Merge`, it records the decision (reason and counter) and acts once S6's merge entry lands. Until
  then the rule is unit-tested and the call is a stub that holds, `merge_pending_s6`.

**Signals: the `ShardLoad` beat field.** A list, one entry per owned shard, every field
`#[serde(default)]`, in `src/fleet/heartbeat.rs`:
- appends/s and append bytes/s;
- resident streams;
- committer occupancy and waiting (P4's);
- refused demand;
- unsealed bytes;
- queue control-state bytes and their growth (§6.4's counters, when S3b lands);
- maintenance lag (fold, collector, sweep);
- the predicted per-child split: appends by the leaf's next hash bit, two counters at append admission;
- the top entity's share for `hot_entity`, from the scaler's per-stream sketches.

Plus the heartbeat-level fields `split_refused` and the topology decision's reason segments. S4 owns
`reserved_shards` and `topology_intent`.

**Configuration, in `src/config/cli.rs`, with the effective-config pin moving:**
- the rev 4 constants: `SHARD_SPLIT_LINE_PCT` 70, `SHARD_SPLIT_SUSTAIN_SECS` 600,
  `SHARD_EMERGENCY_LINE_PCT` 90, `SHARD_EMERGENCY_SUSTAIN_SECS` 45, `SHARD_MERGE_LINE_PCT` 25,
  `SHARD_MERGE_SUSTAIN_SECS` 21,600, `SHARD_SPLIT_REVERSAL_MIN_SECS` 43,200,
  `SHARD_TOPOLOGY_COOLDOWN_SECS` 60, `SHARD_BEAT_STALE_SECS` 30;
- the capacities: `SHARD_APPENDS_CEILING` 5,000 and `SHARD_STREAMS_CEILING` 65,536;
- `SHARDS_PER_INSTANCE_MAX`, 7 in compute-1g.env, provisional (Q4);
- `SHARD_MANIFEST_POLL_MS` 30,000, applied in `keyspace::db_builder` for shard logs only (Q5).

## 2. Red tests

- **Policy unit tests, one per rule, exact decisions per load table:**
  - the 70% and 600 s edges, and the 90% and 45 s edges;
  - the doubling's half-of-leaves edge, and deepest first;
  - the 25% and 21,600 s merge edges, the 43,200 s reversal edge, and the override cooldown;
  - one operation per cell, and the 60 s cooldown;
  - the stale-beat and congestion guards (blocking merges only);
  - the 24-bit prefix, `topology_at_max` at 64 leaves and at `FLEET_MAX`;
  - `hot_entity` at 50%;
  - the reservation's two capacities and `instance_full`;
  - rule 1's desired-age and override-age gates;
  - backlog bytes given as load produce no split.
- **The Q5 fence test, before the poll moves:** a fenced owner's next write fails at once with a 30 s
  shard-log poll (shard_directory, seal open, the compactor's refresh). If anything depends on the poll,
  the poll stays at 2 s (Q5's condition).
- **DSTs:**
  - new `topology_policy.rs`, on the paused clock:
    - a 1-shard cell under demand reaches 4 shards in at most N ticks, doubling;
    - cold siblings merge after the sustain, never inside the anti-flap (with S6; before S6, the
      decision is pinned);
    - a hot shard beside a full instance asks for an instance first and splits after it joins;
  - `fleet_desired` extended with the reason segments;
  - `scaler_split_gate` and `scaler_loop` checked. They gate per-stream segment splits, not shards, so I
    expect no change; I'll say if one is needed.

## 3. Ratchets, formal, edge

- **Mutation owners:** `fleet_planning` and a new `topology_policy`.
- **Formal:** TLA-011 re-recorded only if `src/fleet.rs` changes (one line in `start_configured`). No new
  obligation: the policy is pure.
- **Edge E6**, ratified on condition: every new field `#[serde(default)]`, and the record names the
  reason segments its DSTs pin. The number is the lead's.
- **Measurements, in `meas-` legs:** #149's growth half (`INITIAL_SHARDS=1`, 10 to 1,000,000 locally), and
  the manifest-poll A/B of idle Class B/s per shard at 2 s and 30 s.

## 4. Questions before code

1. **`split_demand` under rev 4.** Q9's trigger (a), `split_demand > 0`, is superseded by rev 4's leaf
   lines, but S5's "done when" still says "`split_demand` 3 reaches 4 shards". I propose `split_demand > 0`
   for the planner's own sustain (30 s, P4) as a doubling trigger, alongside the leaf lines, since it is
   the fleet's demand past the shards. Or should it only be published? Is that a departure for Fable?
2. **The streams axis.** `SHARD_STREAMS_CEILING` 65,536 is "resident streams". P2c put
   `HANDLE_MAX_RESIDENT` at 8,192 on the 1 GiB profiles, so resident handles never reach 70% of 65,536. I
   propose the streams axis read tracked streams per shard (the per-stream maps' bound, 65,536), not
   resident handles. Or should the ceiling become 8,192?
3. **Heartbeat reads.** The policy loop reads the heartbeat set itself every 10 s, as count_balance does,
   rather than through `crate::start`'s fold, which has a frozen growth row. That is one more LIST plus
   GETs per tick per instance. Is that acceptable, or should the fleet tick share its fold through
   `Standing`, which needs `start`'s row re-pinned by the owner?

## 5. Status (2026-11-09, after S5-PLAN-CHECK)

Fable's ruling (S5-PLAN-CHECK.md) supersedes §1 and §4 where they differ:
- `split_demand` is the fleet axis under the cell's 600 s sustain. It marks the d busiest leaves; it has
  no emergency form; its reason segment is ` fleet_axis=<d> `.
- Streams axis: resident handles against the shard's `handle_max_resident` (0 disables it).
  `SHARD_STREAMS_CEILING` is dropped.
- The beats come from Standing's cache of the tick's read, with no own LIST or GETs. A snapshot older than
  30 s gives `beats_stale`: merges are blocked, splits are not.
- `topology_at_max` applies only to a split past 64 leaves; the FLEET_MAX branch is gone.
- Predicted-exhaustion emergency: deferred to stage 2.
- Mutation leg: topology_policy, fleet_planning, fleet_heartbeat, fleet_repository, fleet_standing.
- TLA-011 re-recorded after the `start_configured` line. E6 is #261.

Branch `s5-triggers` (~/.streams-k2/wt/s5, on s3a-4a b6dc580c7), all local and unpushable alone (dead code
until the policy loop):
- bb973f469: the policy.
  - 514 lines; tests.rs 858, merge_tests.rs 227.
  - 45 tests. Red was 31 of 36 failing on the stub; then 9 of 45 with the fleet axis and beats_stale
    disabled. Green.
- 3700a47a0: Standing's beat cache, filled by read_heartbeat_set.
  - 2 tests, red on a stub; fleet:: 101 green.

Next: the `ShardLoad` beat field and counters, the config constants with the effective-config pin, Q5's
fence test, then the policy loop once S4 commit 5 is in.

## 6. Milestone: the loop is wired (2026-11-09)

Branch `s5-triggers` (~/.streams-k2/wt/s5), on package 5's s4-split tip 2dee359dd. Three local commits,
none pushed:
- 46abf305c: the eleven knobs, the CLI surface pin (the topology rows in `config/tests/topology_surface.rs`,
  so `config/tests.rs` stays at 1,000 lines), and the effective-config pin 130 → 141.
- 303887b10: `ShardLoad` in the heartbeat: appends/s, bytes/s and committer busy % from the 128-group
  timing ring (15 s window), plus resident handles against `HANDLE_MAX_RESIDENT`. `src/shard.rs` is
  untouched.
- d7e6237f3: the policy, Standing's caches (beats, desired's and the overrides' times), the loop, and
  `fleet::start_configured`'s one call. The split entry is a seam: production holds `split_unavailable`.
  - The DST `topology_policy` runs the doubling shape (1 → 2 → 4, splits asked exactly `["", "0", "1"]`,
    then stable for 20 s). It is red with the stub (`[""]` after 90 s) and green 4 of 4 (40-44 s), once
    under package 4's mutants.
Not done, in order:
1. `intent_live`: read `topology/intent.json` (ETag) once a pass. `topology::intent` is `#[cfg(test)]`
   until S4 step 9, its first caller; the loop holds `intent_live: false` until then. S4's entry also
   answers `409 topology_busy`, which the loop maps to `Hold(IntentLive)`.
2. AskInstance → the fleet:
   - heartbeat `split_refused: "instance_full"` and the desired reason's
     ` shards_per_instance=<n>/<max> `;
   - the planner taking it as instance demand;
   - DST shape 3 (a hot shard beside a full instance asks for an instance, then splits).
3. The decision's reason segments on the heartbeat (E6): ` fleet_axis=<d> `, the hold names,
   `merge_pending_s6`. Edge record #261.
4. The top entity's share (`hot_entity`) from the scaler's sketches; maintenance lag and the queue-state
   axes.
5. Q5's fence test, then `SHARD_MANIFEST_POLL_MS`.
6. DST shape 2 (merge) after S6; until then the merge rule is unit-pinned.
7. At the push: gate, Linux suite, in-diff mutants, TLA-011 re-recorded. TLA-016's receipt failure is
   package 5's base (`receipt omits: TLA-016/nc-remainder-keeps-index` on s4-split 2dee359dd).
Loop facts the DST settled:
- The rig's clock is manual, so the loop times the last operation's end and a leaf's birth from the wall
  clock when it first sees them, not from `Snapshot::installed_at`.
- An f64 RSS can't become integer bytes without a denied cast, so the transition memory is S4's entry's
  check, and a destination is accepting only below its shed line.

### 6.1 Update: the ask for an instance reaches the fleet (e6d700441)
Item 2 is done. The beat carries `split_refused: "instance_full"` and `shards_resident`. The planner wants
live + 1, keeps it in the shrink target, and appends ` shards_per_instance=<n>/<max> (<need>)` to the
reason. DST shape 3 is green twice, and red with the old clamp (the desired document stays at the seed).
Two questions for Fable, through the lead:
- (a) The rule-4 clamp gives way by exactly one shard for a waiting split. Without that, a one-shard cell
  on one instance deadlocks: the split waits for an instance the clamp will never add. Is this an
  acceptable elaboration of rule 4?
- (b) At `FLEET_MAX`, or with a single-instance fleet (FLEET_MAX = 1), a split with room for both
  children on the actor but no destination waits forever. My proposal: when live ≥ FLEET_MAX, the split
  goes ahead without a destination (both children stay on the actor), since no instance can join. Today
  it asks, and nothing answers.

### 6.2 Package 5's answers (2026-11-09; shared note pkg5-s4-answers-2026-11-09.md)
- s5-triggers is rebased onto s4-split f0e9f7ad3. Clippy is clean, 254 focused tests pass, and both
  policy DSTs are green (42.5 s).
- The intent is on the registry's store. Live means `!is_terminal()`; an `IntentError` is live (fail
  closed). The loop's Cell takes the registry's store for one `intent::read` per pass.
- The intent's `#[cfg(test)]` comes off in package 6's sweeper work. Their f2327853c (lineage sweeper)
  does not drop it yet. S5 takes that commit from them rather than editing the line, then wires
  `intent_live`.
- Split entry: package 5 adopts S5's `SplitFn` seam (the prefix in; `Refused` carrying topology_busy,
  instance_full, not_splittable, unknown_prefix). Commit 11 supplies the function.
- TLA-016: package 4's known control, not S4's.

### 6.3 The decision on the beat (fb3e5f823)
Item 3's code is done: `topology_decision` carries `Decision::reason` segments, and merges are held as
`merge_pending_s6`. Stack on s4-split f0e9f7ad3: 31d00c596, 489cd66cb, 4a5df0b75, a55a4f84c,
fb3e5f823 (all local). Next: write edge record #261 (with the WIRE-MATRIX rows), the hot-entity share,
Q5's fence test, the intent once package 6's sweeper sha arrives, then the push legs.

### 6.4 The live intent (866ed3ef2)
Item 1 is done. Package 6's gate split (1578acbf6) is folded in. The loop reads `topology/intent.json`
each pass through `state.data_store`; a non-terminal intent, or a read error (fail closed), gives
`hold=intent_live`. A new DST pins it, red with the read ignored: the cell split through to four leaves
under a Preparing intent. Stack on s4-split f0e9f7ad3: 31d00c596, 489cd66cb, 4a5df0b75, a55a4f84c,
fb3e5f823, 866ed3ef2 (all local). Package 6's s6-step3 rebases onto 866ed3ef2.

### 6.5 Through S4's split (09d17987b, on s4-split 6c2d4609d)
- `split_entry` wraps `topology::split::split`, and a fourth DST splits a loaded one-shard cell through
  it, keeping all twelve pre-split streams whole (7.3 s; red with the stub).
- `topology::split` and its tree are still `#[cfg(test)]` (package 5's un-gating, S4 step 13), so
  `split_entry` is too, and production keeps `split_unavailable`. The switch is one line in
  `fleet::start_configured`.
- Base reds at 6c2d4609d, both reported to package 5:
  - TLA-016's control;
  - 24 exception-growth sites in `bootstrap::run` from c6cba877a.
- Stack: 20c2a289e, e72d42149, d95e1aa4d, 80b05c2d3, eac85d5b1, 79f20a769, 09d17987b (all local).

### 6.6 New base s4-split c235d4fec (S4 rebased onto s3a-4a 518e1c6f2)
- s5-triggers is at 9bcd28c5f (six commits).
  - The effective-config pin is now 129 → 140: the base dropped ACCOUNT_ID.
  - `fleet::hysteresis`'s test `Thresholds` gains `shards_per_instance_max`.
  - A duplicate `fleet_standing` size from the keep-both merge is removed.
- Checks: clippy clean, 274 focused tests, three DSTs green. In preflight, formal now passes; the only
  failures are architecture and clippy-ratchet on bootstrap::run's six rows (base, with Fable).
- The split_entry commit is parked on branch `s5-split-entry`. Since c235d4fec, S4's `split()` future is
  !Send (a `Cell` held across `publish`'s await, split.rs:219). Reported to package 5; it restacks after
  their fix.

### 6.7 Verified on s4-split cb040ed21; edge record #261 committed (30e5632da)
- s5-triggers is eight local commits: bbfae8d00, 7f71179d0, 2d693a964, fd48ccea4, 96a82e7ec, 95126c504,
  15fb6d72c (split_entry, restacked after package 5's Send fix), 30e5632da (the record and WIRE-MATRIX).
- Checks: clippy clean; 275 focused tests; the four DSTs (40.4 s). Preflight passes every leg.
- Left: the hot-entity share; Q5's fence test and `SHARD_MANIFEST_POLL_MS`; the production switch to
  `split_entry` after package 5's un-gating sha; the merge DST after S6; Fable's two rulings; the push
  legs (gate, Linux suite, in-diff mutants, TLA-011).

### 6.8 On S4's route (s4-split 779bde03b), production splits (2026-11-10)
- SHARDS_PER_INSTANCE_MAX went to package 5 standalone (bda480f76, now 8adbd4d17 in s4-split). S5's copy
  dropped out on rebase.
- s5-triggers is eight local commits; the tip 79c9fe64c is the record:
  2b1e2a721, db0bd5019, 67d7559fa, d105473d4, 6e587b800, 3e2b6cfba,
  7401ad982 (split_entry through `topology_ops().split`, production), 79c9fe64c (#261).
- Every commit passes clippy -D warnings on its own. At the tip: 283 focused tests, the four DSTs in
  42.6 s, and preflight passes every leg.
- Left: the hot-entity share; Q5's fence test and SHARD_MANIFEST_POLL_MS; the merge DST after S6; Fable's
  two rulings; the push legs (gate, Linux suite, in-diff mutants, TLA-011).

### 6.9 Ratification follow-ups, hot entity, Q5 (2026-11-10, on s4-split 84efb4309)
- Twelve local commits. New since §6.8:
  - 3792792ba: the FLEET_MAX rule and #261's A1-A4;
  - 95d2e7825: the hot-entity share, `top_stream_appends_per_s`;
  - 76a557c21: Q5's fence test;
  - ae18db13f: S4's Busy reasons → fleet_settling / override_recent / intent_live.
- Q5 is held. The 2 s shard-log poll is also flush liveness: RUNBOOK:101 and TIGRIS-404-COST.md say 60 s
  polls gave 14 s flush stalls, and 5 s was refused. The lead has three options: no change, measure, or a
  fork patch. MANIFEST_POLL_MS's default is unchanged.

### 6.10 The convergence DST (2026-11-10)
- `dst::dst_tests::topology_convergence::a_one_shard_cell_grows_to_four_shards_on_two_instances_and_converges_back_to_one`
  is in src/dst/tests/topology_convergence.rs, on branch s5-convergence (wt/s5conv) over package 6's
  635b266de. It is uncommitted. Its leg `s5-conv-dst` (red with merges held, then green twice; script
  analysis/s5/conv/leg.sh) is queued under locked.sh behind layout 8 and rename part 3.
- The lead chose (a): package 6 wires the pre-merge override (`…1` to `…0`'s owner). Then:
  - restack s5-convergence onto that commit;
  - add the FLEET_MIN 2 case `a_cold_pair_split_across_two_instances_merges_with_no_scale_in`, unless
    package 6's red test is exactly that;
  - send both names to package 2.
- Package 2's CI step (p5-0/s7/ci-topology-convergence-leg.yml, timeout 45 → 60) goes in the same commit
  if their job 397f71e4b is below it. Otherwise package 2 sends a follow-up.
- My final commit touches only the new file, dst_tests.rs, owners.json, the README and the inventory.
- Inventory fixes 1b500939d and e3d06a81e are with package 6, between S5 and S6.
- Package 2's floors: if the second case lands in topology_convergence.rs, the leg uses `--min 2` with a
  second `--exact dst::dst_tests::topology_convergence::a_cold_pair_split_across_two_instances_merges_with_no_scale_in`;
  if package 6's test covers it instead, it stays `--min 1`. Send package 2 the final names and the
  measured time.
- 2026-11-10 07:10Z: package 6's override (a) is 74f226387 (on s6-step3 570cce46b). Reviewed: approve,
  one liveness gap sent to package 6 and the lead (colocate only pulls to the …0 owner; an actor at
  max − 1 never colocates even when the partner has room; fix: colocate onto the partner). Canonical
  two-instance case is package 6's `dst::dst_tests::topology_policy::a_cold_pair_split_across_two_instances_is_colocated_and_merged`;
  topology_convergence.rs keeps one test, leg `--min 1`. Restack s5-convergence on package 6's sizes tip.
