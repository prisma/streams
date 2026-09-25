# Item 40: a heartbeat's `draining` is its runtime's readiness verdict, and every ring honours it

**Tree.** Planned against `slate` at d4d631df. While planning, `origin/slate` moved to d255ad6d. The two
new commits add only the acceptance record (`docs/reviews/2026-09-hardening/`) and the platform-e2e
scripts. No file this plan reads or edits changed between them (`git diff --stat d4d631df d255ad6d --
src scripts/quality docs/refactor RUNBOOK.md` is empty), so every line number below holds at d255ad6d.

**The checkout is shared.** At planning time another session had uncommitted edits in `src/http/serve.rs`,
`src/dst/tests/security_operations.rs`, `docs/refactor/WIRE-MATRIX.md` and
`docs/refactor/test-inventory.json`. Two of those ledgers are also edited here. Start only on a clean tree
(`git status --short` empty), or after that work lands, and regenerate the inventory then.

**How this was measured.** Nothing in the repo was edited and cargo was not run. Every ratchet,
architecture, formatting and inventory number below comes from scratch copies in
`plans18/work/`: `src/fleet.rs`, `src/fleet/{planning,standing,tests}.rs`, `fleet_controller.final.rs`, and
the diffs `*.diff`. Those were measured with the repo's own gate code:
- `scripts/quality/source_rules.py` and `source_gate.check(sources=…)`, with the prebuilt
  `target/debug/streams-quality-syntax` (built Sep 22, after the last scanner change dafce85f on Sep 14);
- `scripts/architecture-gate.py` `violations()`;
- `scripts/test-inventory.py` `functions()`;
- standalone `rustfmt --edition 2024 --check`.

The harness is `plans18/work/measure.py`.

**Supersedes** `plans15/heartbeat-cadence.md` for item 40 (see §9 D4).

## 1. Problem (verified on the current tree)

### 1a. `draining` is published as a constant

`src/fleet.rs:70`, in `Heartbeat`, is the field. It has no doc comment and no serde default, so it is
required on the wire:
```rust
    pub draining: bool,
```
`src/fleet.rs:582` is the only writer, inside the fleet tick's heartbeat literal in `start`:
```rust
                draining: false,
```
Nothing in the process ever sets it. The draining-related words in `src/bin/pilot/generator*` and
`bench/fleet/drain-account.py` belong to the load generator, not to this field.

### 1b. The ring planner never reads it, and always keeps this instance

`src/fleet/planning.rs:14-18`, inside `active_members`:
```rust
    let active: Vec<String> = ordinal
        .iter()
        .filter(|name| {
            name.as_str() == instance || ages.get(*name).is_some_and(|age| *age < 30_000)
        })
```
Its only input is `ages: &HashMap<String, i64>`, which `start` fills at `src/fleet.rs:614` for every
heartbeat, draining or not:
```rust
                hb_age_ms.insert(other.instance.clone(), now_ms() - other.ts_ms);
```
It is called only at `src/fleet.rs:754`:
`let active = planning::active_members(cur_count, &cfg.instance, &hb_age_ms);`

This instance is a member by name whatever its health.

### 1c. The one reader of `draining` is dead code

`src/fleet.rs:615`:
```rust
                if now_ms() - other.ts_ms < 10_000 && !other.draining {
```
This gates `live`, the load totals, `peer_load` (the rebalance and return-home health input) and the
heartbeat-sourced `peer_urls`. Since the flag is always false, `!other.draining` never excludes anything.

### 1d. What that costs

A runtime whose own `/readyz` answers 503 for a task or shard reason still holds its full ring share.
- **Task reasons.** `TaskMonitor::unready_reason`, `src/tasks.rs:269-283`: "runtime shutting down",
  "runtime stopped", or "critical task terminated: <name>".
- **Shard reasons.** `ShardDirectory::unready_reason` (`src/shard_directory.rs:161`, then
  `src/sharddir.rs:399-413`, then `src/sharddir/health.rs:53-66`): "required engine task terminated:
  <prefix>/<role>", "no shard has ever opened (3 distinct shards failed)…", or a closing engine's failure.

Every instance still routes that runtime's shards to it, and its own ring keeps them. With shard storage
unready, those shards answer open failures for as long as the process lives. With a critical loop ended,
the runtime keeps serving until someone restarts it.

The design of record says otherwise:
- `COMPUTE-SPEC.md:123`: the ring is "weighted rendezvous hashing over (live, non-draining instance set +
  overrides)".
- `COMPUTE-SPEC.md:174`: "`/readyz` = participating in ring".

The operator surface `/operator/data.json` (`src/fleet/repository.rs:283-287`, then
`src/operator.rs:94,119`) shows `draining: false` for every instance.

### 1e. The code states idleness as the liveness rationale

`src/fleet.rs:749-751`:
"dropping any that have been heartbeat-dark >30 s (wedged — requests would have woken a merely-sleeping
one). Self is always fresh (just wrote)."

The pilot router's mirror repeats it:
- `src/bin/pilot/lb.rs:327`: "(same rule as the servers' R2 check)"
- `src/bin/pilot.rs:110`: "minus any heartbeat-dark >30 s (mirrors the servers' R2 view)"

What the 30 s window actually measures is whether the peer's **fleet tick** still runs. The stamp is
written as step 1 of every pass (`src/fleet.rs:570` `ts_ms: now_ms()`, published at `:587` before
the pass's reads). That is a progress check, not a traffic or sleep signal.

### 1f. Return-home judges this instance by a live counter, not its published heartbeat

`src/fleet.rs:906-909`:
```rust
                        let healthy = peer_load
                            .get(&home)
                            .map(|(_, lag)| *lag == 0)
                            .unwrap_or(home == cfg.instance && state.runtime.usage.absorb_lag_max() == 0);
```
A peer home is healthy only by its published heartbeat: fresh, no drain, `eff_lag` including wedge
(`:616-618`). This instance, when it is missing from `peer_load`, is judged by its live absorb counter
instead. That counter knows neither its drain nor its wedge.

The block's own comment (`:883-884`) says a home must be "fresh and healthy again". Once 1a is fixed,
the fallback would return shards to a draining instance whenever the ring falls back to every ordinal.

### 1g. The fencing authority today (verified; this plan keeps it)

- **Who fences.** The shard log's SlateDB writer epoch, taken by whichever instance next opens the shard.
  - The production opener at `src/bootstrap.rs:479-488` logs
    `"opening shard log {path} (lazy; fences prior owner)"`, then `Db::builder(p2.as_str(),
    shard_store)…build()`.
  - `src/sharddir.rs:18-19`: "Each detached open that *completes* bumps the writer epoch, fencing the
    previous zombie".
  - `RUNBOOK.md:355-357`: "**Fencing**: opening a shard fences the previous owner via CAS on the shard
    manifest; a fenced owner's next write fails cleanly."
  - The loser learns it on its next write: `src/history.rs:707` `absorb_error_is_fence`, and the engine
    closes.
- **Who may open.** Only an instance whose own ownership view names it the owner.
  - `src/shard_directory.rs:262-268`: possession yields to the ring; then "R2/R3: only the ring owner may
    claim a shard", `return Err(ResolveError::NotOwner { prefix, owner })`, which becomes 409
    `not_ring_owner` with `Streams-Replay-To` (`src/http.rs:569-578`).
  - The view is replaced only from a complete authority read (`src/fleet.rs:798`
    `state.ownership.set_view(active.clone(), map)`).
  - The tick also retires any held shard its view assigns elsewhere (`src/fleet.rs:826-843`, "possession
    yields at tick").
- **Where the heartbeat sits.** It is one input to that view: ring membership. It opens nothing, fences
  nothing and gates no write.

### 1h. Use sites (grepped in src, incl. src/dst and cfg(test), scripts, bench, deploy, tools, docs)

| what | sites |
|---|---|
| `Heartbeat.draining` | defined `fleet.rs:70`; written `fleet.rs:582`; read `fleet.rs:615`; serialized to the operator view (`repository.rs:283-287` → `operator.rs:94,119`); fixtures write `false`: `dst/tests/fleet_controller.rs:243` (`peer_heartbeat`), `fleet/repository/document_tests.rs:369` |
| `active_members` | `planning.rs:5-22`; sole caller `fleet.rs:754` |
| `hb_age_ms` | declared `fleet.rs:599-600` (`HashMap<String, i64>`); filled `:614`; passed `:754` |
| the ring (`active`) downstream | `set_view` `fleet.rs:798`; return-home home pick `:897-901`, fair share `:929-934`; `pick_move_target(&peer_load, &active, …)` `:988`; `OwnershipService::effective_owner` (every routed request, `ownership.rs:72-86`); `ring_active` in `/operator/data.json` (`operator.rs:101`, `operator.html:58`) |
| return-home health | `fleet.rs:906-909` |
| readiness verdicts | `TaskMonitor::unready_reason` `tasks.rs:269`; `ShardDirectory::unready_reason` `shard_directory.rs:161`; `/readyz` `health_axum` `http.rs:1633-1680` (also auth feeds and billing) |
| mirrors that ignore `draining` | pilot LB `active_ring` `src/bin/pilot/lb.rs:327-338` (and its doc `src/bin/pilot.rs:110`); `bench/fleet/observe-fleet.py:67` (live from `ts_ms` only) |
| fleet-loop starters (all tests that run the tick) | `bootstrap.rs:802`; `dst/tests/fleet_controller.rs:149,190,310,445,527,578`; `dst/tests/runtime_isolation.rs:322` |

## 2. Contract decision

### 2a. `draining` = this runtime's own readiness verdict at each beat

```text
draining = state.tasks.unready_reason().is_some() || state.shards.unready_reason().is_some()
```
These are exactly the two mid-life halves of `/readyz`: the task supervisor (stopping, stopped, a
critical loop ended) and the shard directory (a required engine role ended, a failed close, or no shard
ever opened after three distinct failures).

The fleet tick stamps the flag into every heartbeat it publishes, from `AppState::fleet_draining` in a
new `src/fleet/standing.rs`. It is never inferred from idleness: no traffic, no owned shards and sleeping
are not drains.

`/readyz`'s two boot-only gates, auth feeds not yet published and billing prerequisites, are left out
(§9 D2).

### 2b. Ring eligibility: one rule for every candidate, this instance included

`active_members` keeps the first `count` ordinals that meet two conditions:
1. Their published heartbeat does not drain.
2. For a peer, the heartbeat was stamped within 15 beats (30 s, the unchanged literal).

This instance is judged by its own published drain alone. Its tick stamped that heartbeat this pass, so
its age is ignored. If the listing missed it, it is kept, as today.

An empty result still falls back to every ordinal. So a lone or all-unready fleet keeps serving: the
bootstrap rule is unchanged.

Every instance judges every candidate, itself included, from the same published documents, so their rings
agree. That agreement is what keeps ring changes from becoming fencing ping-pong.

### 2c. Live set and load

The code at `fleet.rs:615` is unchanged, and it now takes effect. A draining peer leaves:
- `live`;
- the CPU, rps and in-flight totals;
- `max_loaded_*`;
- `peer_load`, and with it rebalance targets and return-home health;
- heartbeat-sourced `peer_urls`.

### 2d. Return-home judges a home only by its published heartbeat, this instance included

`healthy = peer_load.get(&home).is_some_and(|(_, lag)| *lag == 0)`. The self fallback in 1f goes.

A home gets shards back only while its own heartbeat is live (under 10 s old), not draining, and free of
lag and wedge. That is what the block's comment already promised.

This deletion is also what keeps `start`'s fingerprinted scope from growing (§4, §9 D1).

### 2e. Progress, not idleness

The heartbeat stays the fleet tick's own step 1. A fresh stamp therefore keeps meaning "this peer's
controller started a pass within 15 beats". Plans15's separate publisher is **not** adopted: it would have
made freshness mean "the process is alive".

Pin P1 (§3c) makes this contract a test. The idle-based comments are rewritten (`fleet.rs:749-751`; the
planner's docs).

### 2f. Not the fencing authority

What stays the same:
- Fencing stays SlateDB's writer epoch on open (1g).
- `draining` changes ring membership and nothing else.
- Peers open a drained instance's shards the way they open any shard the ring moves to them: lazily on a
  routed request, or eagerly only for override entries (the existing handoff, `fleet.rs:845-881`). The
  SlateDB open then fences the old writer.
- The draining instance stops serving through its own existing possession-yield at its next tick (a
  voluntary retire). A write that loses that race is fenced, as in every ring change.

What is explicitly not done:
- no open triggered by seeing a drain;
- no self-fencing when a heartbeat PUT fails;
- no heartbeat freshness as a write lease;
- no new retirement path.

### 2g. What changes at the edge (owner position 40: health-aware eligibility)

1. **A runtime that answers `/readyz` 503 for a task or shard reason now publishes `draining: true`.**
   Within one pass, every current-version instance leaves it out of its ring, and so does its own tick.
   - Its held shards are retired at that tick. Its parked SSE sessions get the typed `WrongOwner` cutoff
     through the existing ownership-view wake (`fleet.rs:809-816`).
   - Requests for its former shards get 409 `not_ring_owner` with `Streams-Replay-To: <new owner>` from
     it. The new rendezvous owner serves them after its open.
   - No new status code, no new header, no new task.
2. **`/operator/data.json`.** `fleet.heartbeats[].draining` reports that instance's verdict (it was
   always false). `local.ring_active` omits draining instances, this one included.
3. **Heartbeat document.** Unchanged in shape: the field already existed and was required. Old-version
   readers already drop a draining peer from live, load and peer URLs.
4. **Return-home.** Changed as 2d. This is internal fleet behaviour, but it is a change: §9 D1.

### 2h. Consequences and residual risks (recorded, accepted unless §9 says otherwise)

- **No flapping within a process.** Every cause is sticky while the instance is excluded:
  - a critical task that ended stays ended;
  - an engine failure is "retained through eviction" (`sharddir/health.rs:15`);
  - a closing failure clears only on the next open of that prefix (`sharddir.rs:478-484`), which an
    excluded instance never gets;
  - "no shard has ever opened" clears only on a successful open.

  Recovery is a restart, through item 38/39's bounded exit or the unready watchdog (default 300 s,
  `config/model.rs:391`). §9 D3.
- **A lone instance, or a fleet where nobody qualifies,** keeps the ordinal fallback. It serves as today
  (pinned by R1b).
- **Desired count.** `desired.count` counts ordinal slots, and a draining instance keeps its slot. The
  load dimensions are computed from the survivors (2c), so the scale-out dimensions react to their load.
  Until the drained instance exits, the fleet can serve with one fewer instance than its count. §8.
- **Mixed-version rollout.** Old instances keep a draining peer in their rings. No shard can be opened by
  two instances because of that: the drained instance excludes itself and never opens. An old instance
  whose view names the drained one only forwards with a 409 replay. Shards that an old instance would
  inherit can bounce between 409 replays until it upgrades. This is availability only, and only while an
  instance drains during a rollout.
- **Shutdown.** "Runtime shutting down" is part of the verdict. The fleet loop is cancelled in the same
  step (`tasks.rs:417-425`), so a shutdown drain is almost never published. §9 D5.

### 2i. Alternatives considered and rejected

- **Stamping `draining` inside the `fleet_io!` publish arguments.** Macro tokens are opaque to the
  ratchet, so it would measure no growth. It would also hide a new callee from start's `unwrap_used`
  fingerprints, the thing those fingerprints exist to see.
- **A free function `standing::draining(&state)` called from `start`.** It is a new call-site and path
  fingerprint under `unwrap_used`, which is growth. The method form plus the return-home deletion keeps
  the multiset of `state` paths at 30 (§4 table).
- **A flattened heartbeat sub-struct carrying `url` and `draining`.** That would be a wrapper introduced to
  satisfy the ratchet, and it changes the fingerprinted `valid_peer_url(&other.url, …)` call argument.

## 3. Red tests, pins and non-vacuity controls

Every DST below compiles on the current tree: it uses only existing APIs and the existing `draining`
field. Test path prefix: `dst::dst_tests::fleet_controller::`. File: `src/dst/tests/fleet_controller.rs`.
Assert anchors are from the formatted draft (`plans18/work/fleet_controller.final.rs`). Anchor by
message if lines shift.

### 3a. Reds on the current tree

Command (DST block of §4 commit B only, no production edit):
```
cargo test --locked --lib -- dst_tests::fleet_controller:: 2>&1 | tee "$SCRATCH/red-40-drain.log"
```
Expected: `test result: FAILED. 6 passed; 4 failed`. The 6 are the 5 existing tests plus P1.

| test | setup | current-tree trace | expected red |
|---|---|---|---|
| **R1** `a_runtime_that_is_not_ready_publishes_its_drain_and_leaves_its_own_ring` | desired 2, overrides `{}`; streams-2 fresh (60 s ahead); rig streams-1 opens `00` (rendezvous home streams-1); a Critical probe task returns, so `unready_reason` is `critical task terminated: fleet-probe`; start | the tick publishes `draining:false` (`:582`); `active_members` keeps self by name and streams-2 (age −60 s) → ring `[s1,s2]`, `00` stays held; the 10 s settle expires | `panicked at src/dst/tests/fleet_controller.rs:696:5:` `a runtime whose critical loop ended must publish its drain` |
| **R1b** `a_runtime_whose_shard_storage_is_not_ready_publishes_its_drain` | desired 1; rig streams-1 opens `00`; `engine.test_abort_task("flush-ticker")` → `shards.unready_reason()` contains `flush-ticker` (as r17a); start | ring settles `[s1]` (self by name); published `draining:false` | `panicked at …:756:5:` `a runtime whose shard storage is not ready must publish its drain` |
| **R2** `a_peer_that_publishes_its_drain_leaves_the_ring` | desired 2; streams-2 publishes a fresh `draining:true` heartbeat; healthy rig streams-1 | planner ignores the flag → ring `[s1,s2]`; the 10 s settle expires | `panicked at …:795:5:` ``assertion `left == right` failed: a peer that publishes its drain must leave the ring`` `left: ["streams-1", "streams-2"]` `right: ["streams-1"]` |
| **R-P** `a_draining_home_never_gets_its_shard_returned` | desired 2; overrides `{"00": {to: streams-2, ms: now−301 s}}` (return window default 300 s); streams-2 never heartbeats; rig streams-1 with the ended probe; start | self kept by name, streams-2 absent → ring `[s1]`; the 10 s settle for `[s1,s2]` expires | `panicked at …:845:5:` ``assertion `left == right` failed: with every ordinal out, the ring falls back to all of them`` `left: ["streams-1"]` `right: ["streams-1", "streams-2"]` |

### 3b. The return-home red (on the intermediate tree)

R-P's second assertion is the 2d red. It cannot be red on the current tree, because there this instance
is never out of its own ring. So it is shown on the tree with every commit-B production edit except the
return-home line (fallback kept).

**Trace.**
1. Tick 1 publishes `draining:true`. The planner excludes self (present, draining) and streams-2 (absent).
2. The ring falls back to `[s1,s2]`, and `set_view` makes `00` → streams-2 effective.
3. `peer_load` lacks s1 (`:615` excludes a drain) and s2.
4. Return-home: the entry is aged; home = `ring_pick("00", [s1,s2])` = streams-1 (index 0, asserted at
   `fleet_controller.rs:498-504`), which differs from `to`.
5. `healthy` falls through to `unwrap_or(s1 == s1 && absorb_lag_max() == 0)`, which is `true`.
6. Fair share: `owned` = 0 (`00` is effectively s2's; the rig's only prefix is `00`), gain 1, so
   `0+1 <= ceil(1/2)` is allowed.
7. The CAS drops `00`.
8. The witness waits for the next stamp: tick 2's PUT, which follows the end of tick 1.

**Expected red:**
```
cargo test --locked --lib -- dst_tests::fleet_controller::a_draining_home_never_gets_its_shard_returned
```
→ `panicked at …:859:5:` `a draining home must never get its shard returned`.

Apply the return-home line and it goes green: `peer_load.get(s1)` is `None`, so `healthy` is `false` and
the entry is kept.

### 3c. Pin P1 (progress, not idleness): `a_stalled_fleet_tick_stops_stamping_its_heartbeat`

**Setup.** `HeldDocument` holds the GET of `fleet/overrides.json` (gate at 0 permits, so it parks).
The overrides read is the tick's only GET of that path: `read_population` skips it at
`repository.rs:110-116`, and the rig runs no other reader. Rig streams-1, start. `entered == 1` is
asserted. Record the stamp, sleep 6 s, then assert the stamp is unchanged, with message
`a stalled tick must not stamp its heartbeat`.

**Trace (same on both trees).**
1. Tick 1 PUTs its heartbeat, then reads the set, routers, desired and urls.
2. It parks in the overrides GET.
3. `read_typed`'s 10 s `DOCUMENT_DEADLINE` fires at entry + 10 s, the `:785-787` `continue` runs, and
   the next PUT comes at about entry + 12 s.
4. The window ends at entry + 6 s.

Teardown cancels through `fleet_io!` (as r09 case 2 does).

Green before and after. It documents that a peer's ring freshness is its controller's progress. A future
separate-cadence publisher must break this pin on purpose (§9 D4).

### 3d. Unit tests (they need the new API, so they land with it; killers and pins)

File `src/fleet/tests.rs` (`#![cfg(test)]`), prefix `fleet::tests::`. Helpers:
- `beat(instance, ts_ms, draining) -> Heartbeat` builds a heartbeat through serde.
- `candidacies(now_ms, &[Heartbeat]) -> HashMap<String, planning::Candidacy>`.

| test | assertion |
|---|---|
| **U1** `a_peer_keeps_its_place_while_its_tick_stamps_within_fifteen_beats` | now 1_000_000; s2 age 29_999, s3 age 30_000, s4 age −60_000 (future stamp); `active_members(4, "streams-1", …)` → `["streams-1","streams-2","streams-4"]` |
| **U2** `a_draining_instance_leaves_every_ring_its_own_included` | `{s1 fresh, s2 draining}` → `["streams-1"]` ("a peer's drain"); `{s1 draining, s2 fresh}` → `["streams-2"]` ("its own drain") |
| **U3** `this_instance_is_judged_by_its_drain_alone` | `{s1 age 60_000}` → `["streams-1"]`; empty map → `["streams-1"]` |
| **U4** `with_no_ordinal_qualifying_the_ring_is_every_ordinal` | `{s1 draining, s2 age 30_000}` → `["streams-1","streams-2"]` (fallback pin) |
| **U5** `published_urls_keep_only_bare_origins` (commit A) | `{s1: https://a.example, s2: https://b.example/path}` under `FleetConfig::default()` → `{s1: https://a.example}` |

### 3e. Existing tests that start the fleet loop, traced against the new rule

- **`r09_…`** (hash `3d48ff56…`, pinned in `review-mechanisms.json:705`; the draft leaves it
  byte-identical, which the inventory extractor confirms).
  - Phase 1: the rig supervisor is running, so `draining` is false.
  - Phase 2 runs the loop under a fresh `retry` supervisor, while `state.tasks` still monitors the
    stopped one. `unready_reason` is "runtime stopped", so self publishes `draining:true` and leaves its
    own ring.
  - `count` 1 (cases 1-2 seed it, and case 3's `cur` is `None`, so `cur_count` is 1). The ring falls back
    to `["streams-1"]`, which is the expected ring, and `desired.is_some()` is unaffected. Green; it now
    exercises the fallback.
- **`an_unreadable_router_report_defers_only_the_desired_publication`.** Home of `00` is self, which is
  in `peer_load` (fresh, not draining, lag 0), so healthy via 2d. Return-home still commits. Green.
- **`an_override_the_ring_ignores_is_never_opened_by_its_target`, `a_lagging_owner_…`,
  `the_rings_owner_opens_…`.** Healthy rigs, so `draining` is false. Rings unchanged.
- **`runtime_isolation::two_runtimes_never_share_fleet_state`.** Non-ordinal names, so the ordinal
  fallback applies. It asserts only the heartbeat documents and the operator instances. Green.

### 3f. Non-vacuity controls (run each, see the red, revert)

| id | temporary edit | expected failure |
|---|---|---|
| C-A | commit A: partition on `!super::valid_peer_url(url, policy)` | U5 ``assertion `left == right` failed`` `left: {"streams-2": "https://b.example/path"}` `right: {"streams-1": "https://a.example"}` |
| C1 | P1: add `store.gate.close();` right after building `store` (it counts without parking) | P1 `a stalled tick must not stamp its heartbeat`, left (newer) ≠ right (older) |
| C2 | delete `&& !other.draining` from `fleet.rs:615` | R2 `nor may a draining peer's URL reach the peer table` (its ring assertion still passes: the planner excludes the drain) |
| C3 | planning `member`: `Some(candidacy) => is_self \|\| (!candidacy.draining && candidacy.age_ms < 30_000)` (self ignores its drain) | R1 `a draining runtime must leave its own ring` `left: ["streams-1", "streams-2"]` `right: ["streams-2"]`; U2 `its own drain` |
| C4 | `standing.rs`: keep only `self.tasks.unready_reason().is_some()` | R1b `a runtime whose shard storage is not ready must publish its drain` (R1 still green). This is the one mutation cargo-mutants cannot generate. |
| C5 | restore the old return-home fallback | R-P `a draining home must never get its shard returned` (§3b) |

### 3g. Loom

Not triggered. No synchronization primitive, shared state or retirement code changes:
- `fleet_draining` reads two existing lock-protected verdicts, each lock taken and released inside its
  own call (`tasks.rs:273`, `sharddir/health.rs:54`, then `sharddir.rs:401-405`). Neither is held across
  an await or across the other. `/readyz` already does the same concurrently.
- A draining instance's engines are retired by the unchanged possession-yield call (`fleet.rs:836-840`,
  `RetirementReason::OwnershipMoved`).

The held-operation and cancellation DSTs (r09, P1) are kept.

## 4. Edits, file by file, in commit order

Push **both commits together**, so the ratchet and mutation leg compare them against the same
before-SHA.

### Commit A: "The fleet planner's URL filter partitions instead of logging inside its predicate, and planning.rs has a mutation owner"

Why here: commit B must change `src/fleet/planning.rs`, which is under the critical prefix `src/fleet`
and has no owner row. An unregistered file fails closed (`verification_plan.py:88`,
`mutation_owners.py:237-247`). Registering it puts `trusted_urls` into the nightly rotation. Today:
- no test reaches it, so its FnValue mutants survive;
- its `if !valid { warn }` makes deleting `!` an equivalent mutant (it only changes which entries are
  logged).

1. `src/fleet/tests.rs`: append U5. Run it green on the current tree first (it is a pin).
2. `src/fleet/planning.rs` `trusted_urls` body becomes the following. Same result; the log moves out of
   the predicate:
   ```rust
       let (trusted, rejected): (HashMap<_, _>, HashMap<_, _>) = map
           .into_iter()
           .partition(|(_, url)| super::valid_peer_url(url, policy));
       for instance in rejected.keys() {
           tracing::warn!(%instance, "rejecting malformed peer URL from urls.json");
       }
       trusted
   ```
3. `scripts/quality/mutation_owners.py`: after `fleet_document_tests` (`:144`), add
   `owner('fleet_planning', 'src/fleet/planning.rs', 'fleet:: dst_tests::fleet_controller::'),`

### Commit B: "A heartbeat's draining is its runtime's readiness verdict, and every ring leaves a draining instance out, its own included"

**`src/fleet/standing.rs`** (new, 18 lines):
```rust
//! This runtime's standing as its fleet peers see it: whether its heartbeat
//! asks them, and its own tick, to leave it out of every ring.
//!
//! Draining is the runtime's own readiness verdict, as `/readyz` answers it
//! from the task supervisor and the shard directory: the runtime is stopping,
//! a critical loop has ended, or its shard storage is unready. It is never
//! inferred from idleness. The boot-only gates `/readyz` also checks (auth
//! feeds, billing) are left out: a restarted instance would otherwise leave
//! and rejoin every ring on its first beats. Draining moves ring membership
//! only. The fencing authority stays the shard log's writer epoch, which the
//! next instance to open a shard takes; no heartbeat opens or fences one.

impl crate::http::AppState {
    /// Whether this runtime's heartbeat says it drains.
    pub(super) fn fleet_draining(&self) -> bool {
        self.tasks.unready_reason().is_some() || self.shards.unready_reason().is_some()
    }
}
```
It is a method, not a free function, because the call sits inside `start`'s `unwrap_used` scope, where a
new callee path is fingerprinted and a method call is not (§2i).

**`src/fleet/planning.rs`** (38 → 75 lines, commit A included). Replace `active_members` and add the
planner's input type:
```rust
/// What the ring planner reads from one published heartbeat: how long ago,
/// on this reader's clock, its publisher's fleet tick stamped it, and
/// whether its publisher drains.
///
/// The stamp is written at the start of every pass of the publisher's own
/// fleet tick, so its age is a progress check on that controller, not a
/// measure of traffic: an idle instance keeps its place, a stalled one
/// loses it. `draining` is the publisher's own readiness verdict
/// (`AppState::fleet_draining`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Candidacy {
    age_ms: i64,
    draining: bool,
}

impl super::Heartbeat {
    /// This heartbeat as the ring planner judges it at `now_ms`.
    pub(super) fn candidacy(&self, now_ms: i64) -> Candidacy {
        Candidacy {
            age_ms: now_ms - self.ts_ms,
            draining: self.draining,
        }
    }
}

/// The ring's active members: the first `count` ordinal instances whose
/// heartbeat publishes no drain and, for a peer, was stamped within fifteen
/// beats (30 s). This instance's own tick stamped its heartbeat this pass,
/// so it is judged by its drain alone, and kept when the listing missed it.
/// Every instance judges every candidate, itself included, from the same
/// published documents, so their rings agree. An empty result falls back to
/// the unfiltered ordinal set (bootstrap: everyone asleep, the first request
/// must land).
// mt-lint: allow(name-keyed-map): fleet instance -> its published candidacy
pub(super) fn active_members(
    count: u64,
    instance: &str,
    candidacies: &HashMap<String, Candidacy>,
) -> Vec<String> {
    let ordinal: Vec<String> = (1..=count.max(1))
        .map(|index| format!("streams-{index}"))
        .collect();
    let active: Vec<String> = ordinal
        .iter()
        .filter(|name| member(name.as_str() == instance, candidacies.get(*name)))
        .cloned()
        .collect();
    if active.is_empty() { ordinal } else { active }
}

/// One ordinal candidate's ring membership (see `active_members`).
fn member(is_self: bool, candidacy: Option<&Candidacy>) -> bool {
    match candidacy {
        Some(candidacy) => !candidacy.draining && (is_self || candidacy.age_ms < 30_000),
        None => is_self,
    }
}
```
It stays within the clippy limits: one bool parameter, nesting under 4, no `_ =>` arm. The `mt-lint`
marker stays directly above the signature; the map rule checks fields and statics, not parameters
(`mt_lint.rs:229-253`), so it is informational.

**`src/fleet.rs`** (1,142 → 1,141 lines; ceiling 1,142; the diff is `plans18/work/fleet.rs.diff`):
1. After `pub(crate) mod repository;` (`:23`), add `mod standing;`. rustfmt orders it there, not after
   `mod planning;`. +1 line.
2. `:70`: a doc on the field. +2 lines. Outside every exception scope.
   ```rust
       /// This runtime's own readiness verdict at publication (`standing`): its
       /// peers leave a draining instance out of their rings and load, and so does it.
       pub draining: bool,
   ```
3. `:582` in `start`: `draining: false,` becomes `draining: state.fleet_draining(),`. ±0 lines.
4. `:599-600` in `start`:
   `let mut hb_age_ms: std::collections::HashMap<String, i64> =` / `std::collections::HashMap::new();`
   becomes `let mut hb_age_ms = std::collections::HashMap::new();`. The value type is inferred from the
   insert and from `active_members`' parameter. The name is kept because it is a path in the
   fingerprinted, unchanged `:754` call. −1 line.
5. `:614` in `start`: becomes
   `hb_age_ms.insert(other.instance.clone(), other.candidacy(now_ms()));`. ±0 lines.
6. `:748-753` in `start`, comment only (six lines stay six):
   ```rust
               // Publish the ring's ACTIVE set for the R2 ownership check: the
               // first `desired` ordinal instances whose heartbeat publishes no
               // drain and, for a peer, whose fleet tick stamped it within 15
               // beats (progress, not traffic). Self is judged by its own
               // published drain. Empty falls back to the unfiltered ordinal set
               // (bootstrap: everyone asleep, first request must land).
   ```
7. `:883-887` in `start`, comment only (five stay five):
   ```rust
                   // Return-home: drop an override once the rendezvous owner is
                   // live and healthy by its own published heartbeat (fresh, no
                   // drain, no lag or wedge; this instance too) and the entry has
                   // aged past the hysteresis window — otherwise moves are sticky
                   // (ladder p3: streams-2 owned nothing by D3).
   ```
8. `:906-909` in `start` becomes one line. −3 lines. rustfmt keeps it at 95 columns; the chain is 54
   characters, under `chain_width` 60.
   ```rust
                           let healthy = peer_load.get(&home).is_some_and(|(_, lag)| *lag == 0);
   ```
   `:615`, `:754`, `:798`, `:826-843` and every exception attribute and reason stay byte-identical.

**`src/fleet/tests.rs`** (195 → 309 lines): U1-U4 and the two helpers (§3d). `beat`'s closure parameter
in `candidacies` is named `heartbeat` so it does not shadow the helper.

**`src/dst/tests/fleet_controller.rs`** (598 → 910 lines; limit 1,000):
- `:1` doc gains ", and ring membership from each heartbeat's published drain and tick progress" (two
  lines).
- `:2` import gains `HttpRig`.
- Append the block `plans18/work/fleet_controller.final.rs:600-910`. It contains four helpers:
  - `draining_peer_heartbeat`;
  - `publishes_drain(store, instance, budget) -> bool`, a bounded poll;
  - `end_a_critical_loop(&HttpRig)`, which spawns the Critical `fleet-probe` returning `Done`, settles
    and asserts `"critical task terminated: fleet-probe"`;
  - `fleet_rig`.

  It also contains R1, R1b, R2, R-P and P1 exactly as drafted: each under 70 lines, nesting ≤ 3, every
  wait bounded, teardown `rig.tasks.shutdown(3 s)` with `aborted.is_empty()`.
  - R1b tears down as r17a does: `shards.shutdown(10 s)` is asserted `is_err()`, then `drop(engine)`.
  - The others end with `engine_shutdown`.
- No existing function changes. The inventory extractor reports `changed: []`, five added, and r09 still
  `3d48ff56…`.

**Ledgers:** §6.

### Budgets and the exception ratchet (measured on the drafts)

| gate | before | after | limit |
|---|---|---|---|
| `src/fleet.rs` lines | 1,142 | 1,141 | 1,142 (base) |
| `start` physical lines (architecture) | 669 | 665 | 670 (`architecture-review-baseline.json`) |
| `src/fleet/planning.rs` / `standing.rs` / `fleet/tests.rs` / `fleet_controller.rs` | 38 / – / 195 / 598 | 75 / 18 / 309 / 910 | 1,000 |
| each of `start`'s six contracts (`too_many_lines`, `excessive_nesting`, `unwrap_used`, `cast_sign_loss`, `cast_possible_truncation`, `let_underscore_must_use`) | `scope_lines` 532, `syntax_facts` 1,017, `nested_items` 2 | 528, 1,012, 2 | non-growing |
| `unwrap_used` on `start`: `unwrap_sites` | 4 | 4 | – |
| `unwrap_site:path` fingerprints that change | `state` 30, `cfg` 26, `home` 6, `String` 17, `i64` 3, `std::collections::HashMap` 8 | 30, 25, 5, 16, 2, 7 | none may grow; **no new fingerprint**; every `call-site` fingerprint unchanged |

**How `start` stays non-growing without touching any reason.**
- **Path multiset.** `other.candidacy(now_ms())` keeps the multiset: `other` ×2, `now_ms` ×1, and the
  call text `now_ms ()` is unchanged. The dropped annotation removes three paths.
- **The `state` receiver.** The new `state.fleet_draining()` adds one `state` path. The return-home line
  removes one (and one `cfg` and one `home`).
- **Method calls.** They carry no fingerprint (`source_rules.py:232-280`).
- **Nothing added.** No reason edit, no new exception, no `exception-growth.json` row.
- **Source gate.** On the full tracked tree with the five drafts overlaid, with the other session's two
  dirty sources pinned to HEAD, `source_gate.check()` returns `source gate OK`.
- **Architecture gate.** It reports exactly `reverse dependency growth: src/fleet/standing.rs ->
  crate::http: 1 > 0` until the §6 policy row, then nothing.

Without the return-home deletion (§9 D1) the same edits measure:
- `unwrap_site:path::ed8ca5365d0895fe` (`state`) 30 → 31;
- `syntax_facts` 1,017 → 1,019 on all six contracts;
- `fleet.rs` 1,144 lines.

## 5. Mutation analysis (cargo-mutants 27.1.0, `--in-diff`, `--timeout 90`, `--jobs 1`)

Only mutants whose span meets a changed line run. The unchanged operators of `fleet.rs:615` are not in
scope. `src/fleet/tests.rs` and `src/dst` are test-only (not selected). Selected owners: `fleet`,
`fleet_planning`, `fleet_standing`.

| owner / function | mutant | killer (message or observable) |
|---|---|---|
| `fleet_planning` `trusted_urls` (commit A) | FnValue `HashMap::new()` / `from_iter` with `""`/`"xyzzy"` pairs | U5 |
| `Heartbeat::candidacy` | FnValue `Default::default()` | unviable (`Candidacy` has no `Default`) |
| | `-`→`+` (ages 1_970_001, 1_970_000, 2_060_000: all out) | U1 `left: ["streams-1"]` |
| | `-`→`/` (ages 1, 1, 0: all in) | U1 `left: [s1,s2,s3,s4]` |
| `active_members` | FnValue `vec![]`, `vec![String::new()]`, `vec!["xyzzy".into()]` | U1-U4 |
| | `==`→`!=` (self and peers swap roles) | U1 `left: ["streams-2","streams-3","streams-4"]`; U3 `left: ["streams-2"]` |
| `member` | FnValue `true` | U1 (s3 in) |
| | FnValue `false` | U1 (fallback to all four) |
| | delete `!` | U1 (s2 out → `[s1,s4]`); U2 |
| | `&&`→`\|\|` | U1 (s3 in) |
| | `\|\|`→`&&` | U1 (s2 out); U3 (stale self out → fallback) |
| | `<`→`==` / `>` / `<=` | U1 (s2 out & s3 in / only s1 / s3 in) |
| `fleet_standing` `AppState::fleet_draining` | FnValue `false` | R1, R1b, R-P |
| | FnValue `true` | `the_rings_owner_opens_…` ("a move-in the ring honours opens at the tick"), `an_unreadable_router_report_…` (ring `[s2]`), R2 (fallback `[s1,s2]`) |
| | `\|\|`→`&&` | R1 (task half only), R1b (shard half only) |
| `fleet` `start` | FnValue `()` | every fleet_controller rig, e.g. r09 "target storage operation must be entered" at 5 s; runtime_isolation |
| | `*lag == 0` → `!=` (the new return-home line) | `an_unreadable_router_report_…` "return-home must still run and commit while a router report is unreadable" (home is self, in `peer_load` with lag 0) |

- **Counts.** About 25 viable mutants: `fleet` 2, `fleet_planning` 17 (14 in B plus about 3 in A),
  `fleet_standing` 3.
- **Equivalents.** None. The only candidate, `trusted_urls`' log-only `!`, is removed by commit A.
- **Timeouts.** All waits are bounded:
  - the new tests: 2 s probe, 5-10 s settles, 1 s drain poll, 6 s P1 window, 10 s witness;
  - the slowest existing test under a mutant is `an_unreadable_router_report_…`, about 22 s (20 s settle
    plus its assert);
  - r09 cancels in 300 ms.

  No mutant leaves a loop unbounded: the planner changes only membership; `start → ()` leaves nothing
  running. All are well under 90 s.
- **Budget.** About 25 × (incremental build + about 25-35 s of tests) ≈ 30-40 min.

## 6. Ledgers (same commit as the code they describe)

**Commit A**
- `scripts/quality/mutation_owners.py`: the `fleet_planning` row (§4).

**Commit B**
- `scripts/quality/mutation_owners.py`: after `fleet_planning`, add
  `owner('fleet_standing', 'src/fleet/standing.rs', 'dst_tests::fleet_controller::'),`. `fleet::` would
  run no test that reaches it.
- `docs/refactor/architecture-policy.json`:
  - Add `"src/fleet/standing.rs"` to `transport_and_composition_files`.
  - Add `transport_rationales["src/fleet/standing.rs"]`: "Fleet standing: reads this runtime's readiness
    verdicts (task supervisor, shard directory) from the composed AppState for its heartbeat's drain
    flag; it decides nothing — the fleet tick's planner owns ring membership and the shard log's writer
    epoch owns fencing."
  - Precedent: `src/billing/telemetry_loop.rs`.
- `docs/refactor/test-inventory.json`: `python3 scripts/test-inventory.py --write` adds 5 rows (R1, R1b,
  R2, R-P, P1) and changes none.
- `docs/refactor/review-mechanisms.json`: no change (r09's `sha256` is unchanged).
- `docs/refactor/WIRE-MATRIX.md`, the `GET /operator/data.json` bullet: append "Each
  `fleet.heartbeats[]` entry is that instance's published heartbeat; its `draining` is the instance's own
  readiness verdict (a critical loop ended, the runtime is stopping, or its shard storage is unready),
  and `local.ring_active` leaves draining instances out, this one included (item 40)." Apply this on top
  of the other session's pending edit to the same file.
- `RUNBOOK.md` §6, after the **Placement** bullet (`:352-354`): "An instance whose heartbeat says
  `draining` (its own `/readyz` verdict from the task supervisor or the shard directory failed) is out of
  every server's ring, its own included, and yields its shards at its next tick; a peer is also out once
  its fleet tick has not stamped its heartbeat for 30 s. The pilot LB's mirror does not read `draining`
  yet: it routes such a shard to the draining instance and follows its 409 `Streams-Replay-To`."
- Acceptance record (`docs/reviews/2026-09-hardening/`, its README convention: one `plans/<round>/`
  directory per planning round):
  - copy this plan to `plans/plans18/heartbeat-draining.md`;
  - in the README's work package 3, move item 40 from "Plans in progress" to done with the two hashes;
  - mark `plans15/heartbeat-cadence.md` superseded where `report/remaining.json` (item 40) and
    `report/decisions.json` (`ITEM-40`) cite it.

  Coordinate with whoever owns that record's next update.
- **No change:**
  - `docs/quality/exception-growth.json` (no growth);
  - `owners.json` and `source-allowances.json` (no new macro, effect or glob; the source gate is OK);
  - `src/dst/tests/README.md` (no new module);
  - `COMPUTE-SPEC.md` (its §2 ring rule becomes true);
  - `test-scenario-map.json` (optional).

## 7. Controls (exact commands, expected output)

Run on a clean tree, with no other cargo or mutation job on the machine (DST timing). `$SCRATCH` is the
session scratchpad.

**Commit A**
1. Append U5 only:
   `cargo test --locked --lib -- fleet::tests::published_urls_keep_only_bare_origins`
   → `test result: ok. 1 passed`.
2. Apply the `trusted_urls` rewrite and repeat → `ok. 1 passed`.
3. C-A (§3f) → the quoted red. Revert.

**Commit B**

4. **Red.** Apply the fleet_controller.rs edits only:
   `cargo test --locked --lib -- dst_tests::fleet_controller:: 2>&1 | tee "$SCRATCH/red-40-drain.log"`
   → `test result: FAILED. 6 passed; 4 failed` with the four messages of §3a.
5. **Intermediate red.** Apply every production edit of commit B except `fleet.rs:906-909`. Then:
   `cargo test --locked --lib -- dst_tests::fleet_controller::a_draining_home_never_gets_its_shard_returned`
   → `a draining home must never get its shard returned`. Apply the line and repeat → `ok. 1 passed`.
6. `cargo test --locked --lib -- fleet:: dst_tests::fleet_controller:: dst_tests::runtime_isolation::`
   → all ok: the five new `fleet::tests`, 10 `fleet_controller` tests, and `runtime_isolation` unchanged.
7. Controls C1-C4 (§3f), each red with the quoted message, then reverted. C5 is step 5.
8. Formatting, lints and docs:
   - `cargo fmt --all -- --check`: clean.
   - `cargo clippy --locked --workspace --all-targets -- -D warnings`: clean. There must be no
     `unfulfilled_lint_expectations`: `start` still has 4 unwraps, its casts, `let _ = tasks.spawn`, and
     more than 100 lines.
   - `RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --document-private-items`:
     clean. New docs name items in plain code only.
9. `cargo test --locked --lib -- --exact mt_lint::multitenancy_identity_lint` → ok.
10. `wc -l src/fleet.rs src/fleet/planning.rs src/fleet/standing.rs src/fleet/tests.rs src/dst/tests/fleet_controller.rs`
    → `1141 75 18 309 910`.
11. Source ratchet:
    `cargo build --locked -p streams-quality-syntax && (cd scripts/quality && QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) python3 -c 'import source_gate; p=source_gate.check(); print("\n".join(p) or "source gate OK")')`
    → `source gate OK`.
12. Architecture gate: `python3 scripts/architecture-gate.py --check`.
    - Before the policy row: `reverse dependency growth: src/fleet/standing.rs -> crate::http: 1 > 0`,
      then `architecture-gate: FAIL (…)`.
    - After: `architecture-gate: OK (…)`.
13. Test inventory: `python3 scripts/test-inventory.py --check`.
    - First: five lines `new test requires inventory: <R1|R1b|R2|R-P|P1 name>`.
    - Then `--write` → `test-inventory: wrote 528 tests` (523 at d255ad6d plus 5; add any tests other
      work landed first).
    - Then `--check` → `test-inventory: OK (528 tests, …)`.
14. `python3 scripts/review-evidence.py --check` →
    `review-evidence source inventory: OK; execution and external acceptance require receipts`.
15. `scripts/quality.sh` → no `QUALITY_FAIL`.
16. Mutation leg:
    `QUALITY_EVENT_NAME=push QUALITY_BEFORE_SHA=$(git rev-parse origin/slate) scripts/quality/mutations.sh`
    - Owners `fleet`, `fleet_planning`, `fleet_standing`.
    - Every owner: 0 missed, 0 timeout.
    - Final line: `Mutation verification executed <≈25> selected mutant(s) across 3 registered
      owner(s).`
    - The `mutation_owners.py` change also selects the tooling legs (`properties_fuzz`, `miri`); expect
      them to run.
17. `cargo test --locked --release` (the ci.yml suite).

## 8. Out of scope (noted, not planned)

- **The pilot LB mirror** (`src/bin/pilot/lb.rs:327-338` `active_ring`, `src/bin/pilot.rs:110` doc) and
  `bench/fleet/observe-fleet.py:67`. They still ignore `draining`.
  - The LB routes a drained instance's shard to it and follows the 409 replay: one extra hop.
  - Follow-up: read `draining` in `heartbeat_entry`, and fix the "same rule" comments.
- **The store-brownout ring churn** that plans15 addressed: a stalled tick stops stamping, so peers drop
  it after 30 s. Under the owner's position this is kept on purpose (2e, P1). §9 D4.
- **A graceful shutdown drain** (COMPUTE-SPEC §5.2: SIGTERM, then a draining beat, then handoff, then
  exit). §9 D5.
- **Desired sizing that reserves a slot for draining instances** (2h). The scale-out dimensions already
  follow the survivors' load; a sizing change is its own behaviour change.
- **Cross-host clock skew** in `now_ms() - ts_ms` (a future stamp counts as fresh, pinned by U1), and
  naming the 10 s and 30 s windows. Both were plans15 commit B, and both are separate changes.
- **Moving `/readyz`'s full verdict out of `http.rs`** so the fleet could share it. §9 D2.

## 9. Decisions for the owner

1. **D1: return-home judges this instance only by its published heartbeat (2d).**
   - The plan deletes the self fallback at `fleet.rs:906-909`.
   - Behaviour changes only where this instance is missing from its own live set: it drains (the
     ordinal-fallback case R-P proves), or its own beat read back ≥ 10 s old in a slow PUT-plus-list
     pass. There it now waits for a live beat instead of trusting a counter that ignores drain and wedge.
   - Trade-off: in a degenerate fallback where a peer is dead and this instance drains, an aged override
     stays on the dead peer instead of returning to the draining instance.
   - Recommendation: accept. It matches the block's own contract ("fresh and healthy"), and it is what
     keeps `start` non-growing.
   - The alternative keeps the fallback and needs owner-approved `exception-growth.json` rows for all six
     `start` contracts (`syntax_facts: 1019`) plus `unwrap_used`
     `unwrap_site:path::ed8ca5365d0895fe: 31`. It also needs two lines found elsewhere, since
     `fleet.rs` would be 1,144 against its 1,142 ceiling.
2. **D2: what drains.**
   - Plan: the task-supervisor and shard-directory verdicts only.
   - Alternative: `/readyz`'s whole verdict, adding auth feeds not yet published and billing
     prerequisites. That needs the verdict moved out of `http.rs`, and it makes every restarted ordinal
     leave and rejoin every ring (two handoffs) while its auth feeds publish.
   - Recommendation: the plan.
3. **D3: an instance that never opened a shard stays out until it restarts.**
   - Excluded, it gets no routed open, so it cannot recover in place.
   - The unready watchdog restarts it after `UNREADY_EXIT_AFTER_SECS` (default 300). With 0 it stays out
     of every ring, and `/readyz` 503, until restarted.
   - Recommendation: accept, together with 38/39's bounded exit.
4. **D4: plans15 (the heartbeat on its own cadence) is withdrawn.**
   - Your position asks for progress checks, and a separate publisher would make freshness mean "process
     alive".
   - The heartbeat stays the tick's progress witness, pinned by P1.
   - A later decoupling needs its own tick-progress field that eligibility checks. That would remove the
     brownout churn P1 now pins.
   - Recommendation: withdraw now; revisit after 38/39.
5. **D5: no graceful shutdown drain in this item.**
   - SIGTERM cancels the fleet loop together with every other loop, so no draining beat is published.
     Publishing one needs an ordered drain phase before cancellation.
   - On Compute a replacement process reuses the ordinal name, so an old process's last beat can
     overwrite the new process's live one. It would also need a boot-id-aware reader.
   - Recommendation: a separate item, after 38/39.

## Skeptic corrections (C1..C11)

**What I checked.** I checked everything against the tree at `eda79ccf`, which is now local HEAD (see C1). I read the
quotes in §1 at `src/fleet.rs:70,582,599-600,614-615,748-754,884-909`,
`src/fleet/planning.rs:5-38`, `src/tasks.rs:262-283,373-425`, `src/sharddir.rs:90-195,395-413,470-484`,
`src/sharddir/health.rs:1-66`, `src/shard_directory.rs:150-165,258-268`, `src/http.rs:566-580,1633-1680` and
`src/ownership.rs:72-86`. They match. I re-ran the measurements independently:
- **`start`'s six contracts.** `scope_lines` goes 532 → 528 and `syntax_facts` 1,017 → 1,012. Five path
  fingerprints shrink: `cfg`, `home`, `String`, `i64` and `HashMap`. `state` stays at 30. No fingerprint is new
  and `unwrap_sites` stays at 4.
- **Without the return-home deletion** (`fleet.nopay.rs`) there are seven GROWTH lines, exactly as §4/§9 D1 say.
- **Source gate.** `source_gate.check()` on the full tracked tree at `eda79ccf`, with the five drafts overlaid and
  `QUALITY_BEFORE_SHA=HEAD`, returns `source gate OK`.
- **Architecture gate.** `violations()` reports only
  `reverse dependency growth: src/fleet/standing.rs -> crate::http: 1 > 0`, and nothing once the policy row is
  added. `start` measures 665 lines and `fleet.rs` 1,141.
- **rustfmt.** `rustfmt --edition 2024 --check` is clean on all five drafts.
- **Inventory extractor.** It reports five tests added, none changed and none removed. r09 keeps
  `3d48ff56002f…`, which is `review-mechanisms.json:705`.
- **New test names** are unique in `src`.

**Traces.** The red traces for R1, R1b, R2 and R-P hold, and so does the §3b intermediate trace:
- `ring_pick("00",[s1,s2]) = 0` (`fleet_controller.rs:498-504`);
- `return_home_allowed(0,1,1,2)` (`fleet.rs:188-198`);
- the rig's only prefix is `00` (`fixture_http.rs:47`).

P1's stall holds as well: `read_population` skips `overrides.json` at `repository.rs:110-116`, and
`DOCUMENT_DEADLINE` is 10 s at `repository.rs:35,270`.

**Visibility and lints.** Visibility is sound:
- `HttpRig` is `pub(super)`;
- `planning::Candidacy` and `Heartbeat::candidacy` are `pub(in crate::fleet)`, which `fleet::tests` reaches
  through `use super::*`;
- `TaskId` is not `#[must_use]`.

Nothing trips the configured clippy set (`Cargo.toml:121-158`, `clippy.toml`).

### C1. The tree moved again, and three numbers in the plan are now stale

HEAD is `eda79ccf`. Another session made four local commits, not yet pushed (`da635ec8`, `9b3a3829`, `eac89829`,
`eda79ccf`); `origin/slate` is still `d255ad6d`. None of them touches a source file this plan reads or edits:
`src/fleet*`, `tasks*`, `sharddir*`, `shard_directory.rs`, `http.rs`, `ownership.rs` and `scripts/quality` are all
unchanged. They do change three things the plan states:
- **The test inventory now has 527 rows** (`docs/refactor/test-inventory.json`). §7 step 13 must expect
  `test-inventory: wrote 532 tests` and then `OK (532 tests, …)`, not 528.
- **`RUNBOOK.md` §6 moved down two lines.** **Placement** is now `:354-356` and **Fencing** is `:357-359`, not
  `:352-354` and `:355-357`. This affects §1g and §6.
- **The other session's edits are committed.** Its `WIRE-MATRIX.md`, `test-inventory.json`, `owners.json` and
  `serve.rs` changes are in (`git status` is clean). Drop the §0 and §6 caveats about applying on top of a pending
  edit. Still regenerate the inventory at implementation time.

### C2. The mutation and ratchet base must be the parent of commit A, not `origin/slate`

§7 steps 11 and 16 set `QUALITY_BEFORE_SHA=$(git rev-parse origin/slate)`. That is `d255ad6d` while the four
commits in C1 are unpushed. `verification_plan.plan_changes` would then also select their changed critical files:
`src/http/serve.rs`, `src/product.rs`, `src/usage*.rs` and `src/application/append/*`. That pulls their owners into
the local run: http, product, usage and the append owners. The run gets far longer, and a MISSED there cannot be
attributed to this item.

Use `QUALITY_BEFORE_SHA=$(git rev-parse <commit-A>^)` locally, and push only on top of an `origin/slate` that already
contains those commits. If they are pushed in the same push, expect CI's leg to cover their owners too.

### C3. Two recovery statements in §2h and §9 D3 are wrong, and one verdict is missing from §2a

- **The unready watchdog covers only the shard half.** `spawn_unready_watchdog` (`src/sharddir.rs:160-195`) samples
  only `directory.unready_reason()`. Nothing in production exits on `TaskMonitor::unready_reason`:
  `critical_failure` is only displayed, at `src/http.rs:994`. So a runtime whose critical loop ended stays out of
  every ring, and at 503, until item 38/39 lands or an operator restarts it. Say this in §2h and in D3. That makes
  item 40's task half depend on 38/39 for recovery, not on the watchdog.
- **A failed close never clears in the process.** §2h says "a closing failure clears only on the next open of that
  prefix". It does not clear at all:
  - `incomplete_shutdown_failure` holds only while the engine supervisor stays `ShuttingDown`
    (`src/tasks/shutdown.rs:20-28`);
  - `EngineShutdown::terminated()` needs `Stopped` (`src/shard/lifecycle.rs:102-104`);
  - so `open_or_wait` keeps returning `closing_outcome()` at `src/sharddir.rs:476-482` and never reaches
    `g.closing = None`.

  The prefix can never be reopened, and the drain lasts until the watchdog exits the process (300 s, or never with
  `UNREADY_EXIT_AFTER_SECS=0`).
- **§2a's verdict list is incomplete.** Add `"runtime supervisor unavailable"` (`src/tasks.rs:270-272`).

### C4. A residual risk to record: correlated drains concentrate load (proposed D6)

The shard half includes any one prefix's failed close (`sharddir.rs:399-413`), and closes are routine:
- `SweepEviction` from the billing sweep;
- `OwnershipMoved` and `FleetEviction` (`shard_directory.rs:53-59`).

A store brownout that fails closes on several instances drains them all within a pass. `active_members` falls back
only when no ordinal qualifies. So with k−1 of k instances draining, every shard lands on the survivor, and the
handoff opens run during the same brownout. Before this change those instances stayed in the ring until the 300 s
watchdog exit.

This fits owner position 40, but it is a blast-radius change. Add it to §2h and ask the owner (D6):
- accept it; or
- keep the prefix-local closing failure out of `fleet_draining`, and drain only on the task half, `engine_failure`
  and "no shard has ever opened"; or
- cap exclusion at a minority of ordinals.

My recommendation is to accept, because `/readyz` already treats all three as instance failures, but it must be an
explicit decision.

### C5. A residual risk to record: scale-in can remove the healthy ordinal

The §2h "Desired count" bullet is incomplete in two ways:
- `live` no longer counts the drainer, so `need_latency`, `need_edge` and `need_hot` (`live + 1`, `fleet.rs:683-697`)
  equal the current count instead of adding the lost slot.
- Scale-in (`fleet.rs:1064-1076`) always drops the highest ordinal. With count 2 and streams-1 draining, a
  sustained scale-in to 1 removes the healthy streams-2. The ring then becomes the fallback `[streams-1]`: the
  draining instance serves everything.

This is not a regression against today, where streams-1 is always in the ring, but it undoes the eligibility
change. Record it in §2h and add it to §8's desired-sizing follow-up.

### C6. The documented contracts are not all covered: §6's "COMPUTE-SPEC no change" is wrong

- **COMPUTE-SPEC defines `draining` as the voluntary drain.** §5.2 (`COMPUTE-SPEC.md:251-262`) describes the
  SIGTERM or scale-in drain, with the precondition "never drain if it would push any surviving dim > 90%".
  R5 (`:169-171`) requires the router to stop routing on a draining heartbeat.
  - This plan overloads the flag with the involuntary readiness verdict, which cannot honour that precondition.
  - The pilot router still routes to a drainer (§8).

  Add a two-line note at §5.2/R5: an involuntary drain is the readiness verdict; the precondition applies to the
  voluntary drain only; the router half of R5 is pending. Or list this as an owner decision.
- **WIRE-MATRIX `/readyz` (`docs/refactor/WIRE-MATRIX.md:196`).** The edge-visible part of §2g.1 belongs here. The
  proposed edit covers only `/operator/data.json`. Append that a task or shard 503 now also removes the instance
  from every server's ring: its former shards answer 409 `not_ring_owner` with `Streams-Replay-To`. Note also that
  the bullet already omits the task-supervisor 503 reasons.
- **Acceptance record (optional).** Add record #53 to `docs/reviews/2026-09-hardening/edge-changes.md` (surface
  fleet-internal, risk low or medium). The external reviewer asked for one record per edge change. The four
  commits in C1 did not add theirs, so coordinate with the record's owner.

### C7. "The heartbeat is not the fencing authority" has no pin

§2f promises "no open triggered by seeing a drain", but no test holds it. Add one to R2, or add a sixth DST (about
15 lines; the file stays under 1,000):
- **Setup.** Use prefixes `["00","10"]`. `ring_pick("10",[s1,s2]) = 1` is already asserted at
  `fleet_controller.rs:562-576`. Streams-2 publishes its drain.
- **Assertion.** After the ring settles to `[s1]` and a newer `heartbeat_stamp` proves a second pass,
  `open_stats()["started"]` is unchanged and `!is_open("10")`. Message: "seeing a peer's drain must open nothing;
  the next routed request's open fences".
- **Non-vacuity control.** Seed `overrides {"10": "streams-1"}`. The existing eager handoff
  (`fleet.rs:850-881`) opens `10` at the tick, and the pin fails.

It changes no production code and adds no mutant.

### C8. One trace in the mutation table is wrong

Deleting `!` in `member` makes the arm `candidacy.draining && …`, which is false for all three U1 peers. U1 yields
`["streams-1"]`, not `[s1,s4]`. The mutant is still killed (by U1 and U2).

### C9. An owner decision is missing: what "progress checks" means (proposed D7)

§2e reads "progress" as controller-tick progress: the stamp age, pinned by P1. The data-plane progress signals are
already in every heartbeat:
- `wedge_max_ms` and `absorb_lag_max_secs` (`fleet.rs:75-86`) stay rebalance and return-home inputs;
- they do not become eligibility inputs.

That is a reasonable reading. Excluding wedged instances under load would cascade. But it interprets owner
position 40, so state it as D7 (recommendation: accept) instead of implying it.

### C10. Commit B is gated on D1; make that explicit

Without D1, commit B cannot land: seven growth findings plus `fleet.rs` at 1,144 against its 1,142 ceiling. So §4
must say that commit B waits for the owner's D1 answer. If D1 is rejected, the item stops for owner-approved
`exception-growth.json` rows and two freed lines. An agent may not add those rows.

R-P's second assertion and control C5 exist only because of D1. R-P's own scenario is D1's trade-off case: the aged
override stays on a heartbeat-dark streams-2 rather than going to the draining instance. Its doc comment should say
that plainly, rather than presenting the result as correct in every case.

### C11. Loom: agree it is not triggered, but state why the unlocked read is safe

The two verdicts are read under separate locks, so they are not one snapshot. That is harmless:
- every cause is sticky (C3), except "no shard has ever opened", which only goes from Some to None, on a
  successful open;
- a torn read can therefore publish the verdict at most one beat late, and can never publish a false drain.

No shared state or retirement implementation changes, so the trigger-table row does not apply.

**Verdict: ready with corrections.** C1, C2, C3, C6 and C10 must change before implementation. C7 adds a
recommended pin. C4, C5 and C9 add owner decisions D6 and D7 and residual risks. C8 and C11 are text fixes. No
exception reason is edited and no `exception-growth.json` row is needed while D1 is accepted.
