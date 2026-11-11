# Scale validation for #149: a cell that grows and shrinks itself

Status: guide, 2026-11-09 (scaling package S7), a companion to
`docs/SCALE-VALIDATION.md`, whose rules hold here except its fixed
topology (§1.3) and the two topology rows of §1.5.


prisma/streams#149's "done when": a cell starts with `FLEET_MIN=1`,
`FLEET_MAX=64` and one shard. The same ramp, from 10 to 10,000,000 tiny
streams with one tenant and with a tenant each, holds at least 98% of
touches with a p99 of at most 2 s at every step, while the cell grows its
shards and its instances by itself. At the low steps it runs on one
instance with few shards. When the load goes, it merges shards and scales
back in. This section is how to run that on Compute. It replaces nothing
above: §1.2's binary rules, §1.4's environment (except the topology rows)
and §1.6's forwarding rules all apply.

## 1. Before the run

- **The commit.** Splits (S4), triggers (S5) and merges (S6) must be on the
  slate commit you build. Each lands with its own DSTs, and CI's `topology`
  job runs them (S7). Check that the job is green for the commit
  (`scripts/dev/ci-status.sh <sha>` prints `CI_GREEN`). A commit without
  split and merge cannot pass this run: an instance beyond the shard count
  owns nothing (#149 §2).
- **The local gate.** This Compute run, Habanero's, starts only once #149
  is closed, after the local runs of §6 have held, the confirmation run
  last. It does not start early, and nothing is posted on #149 before then
  (owner, 2026-11-10).
- **Names.** Once edge change #253's API part is on the commit, a stored
  tenant is a **container**: the usage route is
  `/v1/containers/{container}/usage`, the wait route
  `/v1/containers/{container}/live:wait`, the internal header
  `streams-internal-container`, error codes `container_*` instead of
  `project_*`, and environment names `CONTAINER_*`, with `ACCOUNT_ID`
  removed. The old routes answer 404. Until #253 lands, everything above
  keeps its project names. Check which one you have before writing any
  client or env file:

```bash
git grep -q '/v1/containers/' <commit> -- src && echo containers || echo projects
```

## 2. Topology and size

| Setting | Value | Why |
|---|---|---|
| Instance size | 1 vCPU, 1 GiB (`deploy/profiles/compute-1g.env`) | the size the profiles certify. The size comparison (P5.0, 2026-11-08, program notes) found no larger size cheaper per unit of work for tiny tenants. For small ones, 2 vCPU, 4 GiB was 15% cheaper. Medium tenants were measurable only on 1 GiB. The owner decides any change. `PROJECT_SHARE_K` (8) and the wait pool stay at today's fixed values for #149, whatever the size (owner, 2026-11-10, RULES.md: "Let's keep todays fixed values for now") |
| `INITIAL_SHARDS` | 1 | the cell starts on one shard; splits make the rest |
| `FLEET_MIN`, `FLEET_MAX` | 1, 64 | the cell grows from one instance, up to #149's 64 |
| `PATH_PREFIX`, `FLEET_PREFIX` | fresh for every run, on a fresh bucket | as §1.3: a reused prefix keeps the old topology |
| Split and merge triggers | the binary defaults: set none | SCALING-DESIGN rev 4 §4.3. Any `SHARD_SPLIT_*`, `SHARD_MERGE_*`, `SHARD_EMERGENCY_*` or `SHARD_TOPOLOGY_*` value other than the default voids the run |
| `SCALE_*` | the binary defaults: set none | the fleet's own hysteresis (scale out on load that lasts, scale in only after a window, one instance at a time) is what the run measures |

The shards an instance may hold are capped by the profile's ceiling
(SCALING-DECISIONS Q4). An instance at the ceiling refuses a split with
`split_refused: "instance_full"` in its heartbeat, and the fleet adds an
instance instead. More than 64 leaves, or more than `FLEET_MAX` instances,
is refused with `topology_at_max`. Either one before the last step is a
finding to report, not a reason to stop.

## 3. Compute's scaling mode

Run the service in Compute's **external** scaling mode. In that mode Streams
sets its own instance count, the fleet's desired count
(`fleet.desired.count` on `/operator/data.json`), and Compute applies it
within a second, with no delay of its own (Compute, 2026-11-09). All the
hysteresis is therefore Streams', and the run measures exactly that. Do not
run it in Habanero's `auto` mode: auto applies its own scale-out and
scale-in windows on top of Streams', and the result is no longer the
cell's.

Record every change of the desired count with its published `reason`, and
every change of the live count, with timestamps (§7).

### 3.1 One service per ordinal, each with its own environment

The fleet names its members by ordinal. For a published count N, the ring's
active set is exactly the instances named `streams-1` to `streams-N`, less
any dark, stalled or draining one (`planning::active_members`,
`src/fleet/planning.rs`). An instance named anything else never joins the
ring. The desired set is therefore always the first N ordinals, and
`streams-1` is always in it, since the count is floored at 1 and at
`FLEET_MIN`. Deploy the cell as up to `FLEET_MAX` services, one per ordinal
(as `bench/k2cost/field/deploy.py` and `bench/fleet/deploy-fleet.sh` do).
Give each its own environment, on top of SCALE-VALIDATION.md §1.4:

| Variable | `streams-1` | `streams-i`, i ≥ 2 |
|---|---|---|
| `INSTANCE_NAME` | `streams-1` | `streams-i` |
| `SELF_URL`, or the resolved `fleet/urls.json` | its own address | its own address |
| `ROLLUP` | `1` | `0` |

Under external mode, Compute must start and stop **ordinals**: on a count of
N it runs `streams-1` to `streams-N`, and on a scale-in it stops the highest
ordinal first. It must never stop `streams-1` while the cell runs. The
ring drops an instance above N on its own, but `streams-1` alone runs the
usage rollup (RUNBOOK, "ROLLUP=0 on ingestion instances"). If Compute
cannot start ordinals but only identical replicas of one service, this
guide cannot be run: stop and report it. The fleet then has neither names
nor a rollup owner.

**Do not paste `deploy/profiles/shared-cell.env`'s `ROLLUP=1` onto every
ordinal.** The rollup's database is one path per cell (`<PATH_PREFIX>/rollup`).
Each instance that opens it fences the one that opened it before, and
nothing reopens a fenced rollup. What follows:

- The instance that booted last owns the rollup. Every other one logs
  `rollup step: Closed error: detected newer DB client` every 2 s for the
  rest of its life (P5.0's fleet ramp, 2026-11-08: seven of eight).
- Instances that boot together can fence each other while they open. Under
  `BILLING_MODE=required` the fenced one exits before serving, with
  `Error: BILLING_MODE=required: rollup DB must open before serving: Closed
  error: detected newer DB client`. Six of eight did so on the local rig on
  2026-11-10, booting within 0.53 s. Each restart fences the owner again.
- Once the owner is scaled in, no instance runs the rollup. Usage stops
  being applied, the stored cap stops moving, months stop closing, and
  `rollup.lastApplyAgeSecs` grows until the alert at 600 s (RUNBOOK, "What
  to alert on"). Meters read in that window are not the cell's.

Before the first step, check the env of every ordinal: `ROLLUP=1` on exactly
one, `streams-1`.

## 4. Held waits: what binds first

A held wait's memory, measured idle over HTTP/2 at 16,000 waits
(2026-11-08, program notes, the held-waits leg): 20.9 KB an SSE wait,
44.7 KB a long-poll and 54 KB a `live:wait` on a 1 vCPU, 1 GiB instance;
22.6, 49.0 and 47.3 KB on 2 vCPU, 4 GiB. Memory is therefore not the bound
at these densities: under the 500 MB shed line a 1 GiB instance holds about
8,700 long-polls or 17,000 SSE waits. The bound is the pool,
`SSE_MAX_CONNECTIONS` = 1,200 on the 1 GiB profile. The packing legs hit it
exactly: PACK-T at 3,000 tenants on 1 vCPU, 1 GiB parked 1,200 of 1,200 and
refused the rest (2026-11-08, program notes).

A forwarded wait is held on two instances (`docs/SCALE-VALIDATION.md` §1.6), so plan for twice the waits.

## 5. The workload and the bars

- **Ramp.** #149's: 10, 100, 1,000, 10,000, 100,000, 1,000,000 and
  10,000,000 tiny streams, each touched once an hour by one 400-byte append
  and one read of it from its start, with auth enforced. Run it twice: once
  with one tenant owning every stream, once with a tenant each. Each step
  adds its streams to the last step's. Hold each step for the hour a touch
  takes, after the cell has stopped resizing for 10 minutes.
- **A step holds** when every stream was created, at least 98% of the
  touches due were served at a p99 of at most 2 s, and no instance shed or
  was OOM-killed (#149's bars). The bar holds over the whole step, the time
  the cell spends reshaping for it included, not only once it has settled
  (owner, 2026-11-10, RULES.md). The settled and the reshaping figures are
  still reported apart, so a failure shows where it happened. A touch the cell refused is a failed
  touch: it counts against the 98% and is reported, but the bar does not
  also ask for zero. That includes a `409 not_stream_owner` while a shard
  moves, and a `503 shard_splitting`. The latter is an append held past a
  split's pause, answered with `Retry-After: 1`, known unwritten and never
  committed (`src/application/append/contract.rs`, `AppendErr::Splitting`).
  Neither is an admission shed (the shed check reads admission's
  counters). A client that honours `Retry-After` retries the append once
  the split has published. The bench does not retry, so the step's served
  share is a floor.
- **Low steps.** At 10, 100 and 1,000 streams the cell must run on one
  instance with at most 2 shards.
- **Drain.** After the last step, stop the load and keep the service up.
  The run holds when the cell is back on one instance with at most 2
  shards. Record when each merge and each scale-in happened. Size the wait
  from the binary defaults:
  - The first merge waits for the longer of `SHARD_SPLIT_REVERSAL_MIN_SECS`
    (43,200 s since the last split) and `HANDLE_IDLE_EVICT_SECS` plus
    `SHARD_MERGE_SUSTAIN_SECS` (600 + 21,600 s). A leaf goes quiet only once
    its handles idle out. The two clocks run at once.
  - Each further merge waits one `SHARD_TOPOLOGY_COOLDOWN_SECS` (60 s) plus
    the merge itself (about 30 s).
  - A merge whose halves sit on two instances first co-locates them: one
    `REBALANCE_MOVE_COOLDOWN_SECS` (60 s) plus one policy pass (10 s,
    `policy_loop::PERIOD`). On a count-balanced fleet that is most merges,
    so charge it to every one.
  - Each scale-in waits one `SCALE_IN_SECS` window (300 s) plus a cooldown,
    one instance at a time. There is no scale-in while a merge's intent is
    live.
  - At the top of the ramp the cell holds 64 leaves (below), so from 64
    leaves and N instances that is 43,200 + 62 × (90 + 70) + (N − 1) × 360 s:
    about 15.9 h at 12 instances and 21.1 h at 64. The co-locations are
    4,340 s of that, about 8%. Allow a quarter over that before calling the
    drain failed.
- **What drives the splits.** A leaf splits at 70% of `HANDLE_MAX_RESIDENT`,
  8,192 resident handles on the 1 GiB profile, so 5,734 streams. A handle
  stays resident `HANDLE_IDLE_EVICT_SECS` (600 s) after its touch.
  `SHARD_APPENDS_CEILING`, 5,000 a second, never binds here: 10,000,000
  hourly touches are 2,778 a second. With an hourly touch, about one stream
  in six is resident at a time:

  | Streams | Leaves on Compute | Leaves locally (60 s touch, 10 s idle-out) |
  |---:|---|---|
  | 10,000 | 1 | 1 |
  | 100,000 | 4 | 4 |
  | 1,000,000 | 32 | not run (§6.1) |
  | 10,000,000 | 64, every leaf full: expect `topology_at_max` | not run |

  At 10,000,000 streams `topology_at_max` is the expected outcome, not a
  finding. Whether the step still holds is what it shows. On Compute
  every touch reloads its handle, so the p99 is the cold path's.
- **Waits.** A tiny tenant parks one wait. Plan the pool from §4's
  figures: the 1 GiB profile parks 1,200 waits an instance, so the tenant
  each arm needs more instances for its waits alone (about 1 per 1,200
  tenants, twice that for forwarded waits) before anything else binds.

## 6. The local compressed ramp (the gate)

`bench/ramp/ramp.py` runs the same ramp on the Mac, in Linux containers with
Compute's 1 GiB class limits, compressed 60x: a touch every 60 s instead of
an hour, so a step carries 60 times the cloud's touch rate at the same
stream count. A Scaler stands in for Compute's external mode: it starts and
stops servers as `fleet.desired.count` says, with no delay of its own.

Scaled with `--set`, by 60x where the constant is a time window:
- `SHARD_SPLIT_SUSTAIN_SECS` 600 → 10;
- `SHARD_MERGE_SUSTAIN_SECS` 21,600 → 360;
- `SHARD_SPLIT_REVERSAL_MIN_SECS` 43,200 → 720;
- `HANDLE_IDLE_EVICT_SECS` 600 → 10, so a 60 s touch period leaves the same
  share of streams resident as an hourly touch on Compute. No lower bound
  but 0, which `STREAMS_AUTH_MODE=enforce` refuses; idle handles are
  evicted every 5 s.
- `SHARD_TOPOLOGY_COOLDOWN_SECS` 60 → 10, and `REBALANCE_MOVE_COOLDOWN_SECS`
  60 → 10, the override cooldown that also gates an operation. Neither has
  a lower bound in the code (0 is accepted). The policy's 10 s tick and the
  fleet's 2 s tick are their floors.

The three values above (idle-out 10 s, both cooldowns 10 s) are the
owner's for the official local run (2026-11-10, RULES.md), on top of the
three sustain and reversal compressions.

These stay at the binary defaults:
- `SHARD_EMERGENCY_SUSTAIN_SECS` (45 s, now slower than the normal line,
  so the emergency path is not shown locally);
- `SCALE_IN_SECS` (300 s);
- the fleet's 30 s scale-out sustain;
- the policy's 10 s tick.

Each step first settles. Its streams are touched in 60 s chunks until the
shards, the topology version and the live count have held still for
`--settle-secs`, then the step is measured. The bound, `--settle-max-secs
auto`, is 300 s plus one cooldown and 30 s per leaf the step adds, by the
table above: 420 s at 100,000 streams with a 10 s cooldown. Every
chunk's served share, p99, errors by code and sheds are kept beside the
settled figures. The bar applies to the chunks too: the step's served share
counts every chunk's touches and the measured window's, against the touches
due over both. Its p99 is the worst window's, and its sheds are both
windows'. The step reports `settled_window` and `reshaping` apart.

Two rig limits matter at the top step:
- **The driver.** It paces touches over workers sized from the rate,
  about 8 s per worker (an earlier 2,000-worker cap left each worker 120 ms
  at 1,000,000 streams, below the append p99, and skipped healthy touches).
  Compressed 60x, a step's touch rate is its count over 60 s: 1,667 a
  second at 100,000 streams, 16,667 at 1,000,000.
- **The store.** s3lite runs on the host, so a top step's GBs stay out of
  the container VM. Its live size is recorded per step.

The local ramp's top step is **100,000 streams** (owner's proviso,
2026-11-10: 1,000,000 only if `meas-149-rig` held it, and it did not).
`meas-149-rig` (a fixed 8 instances, 8 shards, one tenant, slate dbfed12ca,
2026-11-10) gave:
- **100,000: held.** 1.000 served, p99 273 ms, 0 sheds. The driver used 29%
  CPU, 100 MB and 559 descriptors, with no late ticks.
- **1,000,000: failed on the driver and the cell's envelope.**
  - Every stream was created (5,228 a second, 0 errors). Then 3 of 151,431
    touches reached the cell, and the rest were `error sending request`
    to the router.
  - The driver was at 599% CPU, 2.7 GB and 29,784 descriptors, the
    container's ephemeral port range, with every tick late.
  - 16,667 touches a second is also past the fixed cell's envelope (8 ×
    `CELL_ENVELOPE_REQUESTS_PER_SEC` 1,411 = 11,288). It is past the Mac's
    cores too: about 32 one-vCPU servers at that step, beside the driver.

1,000,000 and 10,000,000 streams, and 32 to 64 instances, are Compute's,
through Habanero (§1 to §5).

### 6.1 What the local run cannot show

At 100,000 streams with a 10 s idle-out about 16,700 streams are resident,
so the cell reaches about **4 leaves** (§5's table). The local run shows the
whole loop at that depth: one shard and one instance at the low steps,
splits and scale-outs by the cell's own triggers, the merge back and the
scale-in after the load. It cannot show:
- depth: 32 to 64 leaves, `topology_at_max` and 32 to 64 instances;
- the 1,000,000 and 10,000,000 steps.

Those come from Habanero's Compute run. Multi-split growth in a local run
comes from the topology DSTs in CI's `topology` job (the split, merge and
convergence legs), not from this ramp.

**Order** (owner, 2026-11-10):
1. `meas-149-rig` on slate: done 2026-11-10 (above), so the top local step
   is 100,000.
2. The official streams ramp, `meas-149`: both tenancies, on push 4's
   pushed sha once it is `CI_GREEN`, before queue parts 1 and 2.
3. After S3b lands, a short confirmation run closes #149: one tenancy to
   100,000 streams, with the drain and the queue arm gating.
4. Only then, Habanero's Compute run (§1 to §5). Nothing is posted on #149
   before the close.

```bash
bench/ramp/build.sh
# meas-149: --tenancy one, then --tenancy each, both to 100,000 (the top local step, §6.1), each in its own lock leg
bench/ramp/ramp.py --shards 1 --fleet-min 1 --fleet-max 12 --tenancy one \
  --counts 10 100 1000 10000 100000 --scale-in-secs 0 \
  --low-until 1000 --low-max-shards 2 --settle-secs 120 --drain-secs auto \
  --set SHARD_SPLIT_SUSTAIN_SECS=10 SHARD_MERGE_SUSTAIN_SECS=360 SHARD_SPLIT_REVERSAL_MIN_SECS=720 \
        HANDLE_IDLE_EVICT_SECS=10 SHARD_TOPOLOGY_COOLDOWN_SECS=10 REBALANCE_MOVE_COOLDOWN_SECS=10
# The confirmation after S3b: one tenancy to 100,000, the drain, and 1,000 queues gating each step
bench/ramp/ramp.py --shards 1 --fleet-min 1 --fleet-max 12 --tenancy one \
  --counts 10 100 1000 10000 100000 --scale-in-secs 0 \
  --low-until 1000 --low-max-shards 2 --settle-secs 120 --drain-secs auto --queues 1000 --queue-gate \
  --set SHARD_SPLIT_SUSTAIN_SECS=10 SHARD_MERGE_SUSTAIN_SECS=360 SHARD_SPLIT_REVERSAL_MIN_SECS=720 \
        HANDLE_IDLE_EVICT_SECS=10 SHARD_TOPOLOGY_COOLDOWN_SECS=10 REBALANCE_MOVE_COOLDOWN_SECS=10
```

It records, per step, the shards the live instances own (from their
heartbeats) and the instances live, and fails a low step that ran on more
than one instance or more than 2 shards. `--drain-secs auto` sizes the
drain by §5's arithmetic from the constants the servers run and prints it.
At the compressed constants with 10 s cooldowns, from 4 leaves (2 merges
down to 2 shards) and at most 12 instances, that is
720 + 2 × (40 + 20) + 11 × 310 = 4,250 s, so 5,312 s with the margin. Fewer
instances shorten it by 310 s each.

The drain runs after a failed step too. Before it and after it, the ramp
reads back `--readback` streams (200), spread over all it created. With no
load during the drain the record counts must be equal, which shows the
merged shards still serve the data. A difference fails the drain. Every
server's log is kept in the results directory. `--queues N` adds a small
queue arm per step: N queues, each touched by one send, one claim and its
ack. It is reported in the step (`queue`), and with `--queue-gate` its
verdict is the step's too, as in the confirmation run after S3b part 2. Run both tenancies under the machine lock, one at a
time.

## 7. What to report

Add to `docs/SCALE-VALIDATION.md` §7's report:

```markdown
### #149 run (SCALE-VALIDATION-149)
Commit: <sha>   Compute scaling mode: external   container names (#253): yes|no
| Tenancy | Streams | held % | p99 ms | instances at start/end/max | shards at start/end/max | splits | merges | sheds | OOM | VALID / VOID |
| Drain | tenancy | time to 1 instance | time to <= 2 shards | merges | scale-ins |
Desired-count timeline: <t, desired, reason, live> for every change
```
