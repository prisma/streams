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
- **The local gate.** The compressed ramp (§6) must have held on the same
  commit before the Compute run starts.
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
| Instance size | 1 vCPU, 1 GiB (`deploy/profiles/compute-1g.env`) | the size the profiles certify. The size comparison (P5.0, 2026-11-08, program notes) found no larger size cheaper per unit of work for tiny tenants. For small ones, 2 vCPU, 4 GiB was 15% cheaper. Medium tenants were measurable only on 1 GiB. The owner decides any change. **TODO (owner):** whether `PROJECT_SHARE_K` and the wait pool should scale with a larger size |
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
  touches due were served with no touch error, their p99 is at most 2 s,
  and no instance shed or was OOM-killed (#149's bars, unchanged).
- **Low steps.** At 10, 100 and 1,000 streams the cell must run on one
  instance with at most 2 shards.
- **Drain.** After the last step, stop the load and keep the service up.
  The run holds when the cell is back on one instance with at most 2
  shards. Merges wait 6 h of low load (`SHARD_MERGE_SUSTAIN_SECS`,
  SCALING-DESIGN §4.3), so allow at least 8 h, and record when each merge
  and each scale-in happened.
- **Waits.** A tiny tenant parks one wait. Plan the pool from §4's
  figures: the 1 GiB profile parks 1,200 waits an instance, so the tenant
  each arm needs more instances for its waits alone (about 1 per 1,200
  tenants, twice that for forwarded waits) before anything else binds.

## 6. The local compressed ramp (the gate)

`bench/ramp/ramp.py` runs the same ramp on the Mac, in Linux containers with
Compute's 1 GiB class limits, compressed 60x: a touch every 60 s instead of
an hour, so a step carries 60 times the cloud's touch rate at the same
stream count. A Scaler stands in for Compute's external mode: it starts and
stops servers as `fleet.desired.count` says, with no delay of its own. The
trigger constants are scaled by the same factor with `--set`, and nothing
else is changed. It runs up to 1,000,000 streams and the fleet the machine
holds. 10,000,000 streams and 64 instances are for Compute only.

```bash
bench/ramp/build.sh
bench/ramp/ramp.py --shards 1 --fleet-min 1 --fleet-max 12 --tenancy one \
  --counts 10 100 1000 10000 100000 1000000 --scale-in-secs 0 \
  --low-until 1000 --low-max-shards 2 --drain-secs 1800 \
  --set SHARD_SPLIT_SUSTAIN_SECS=10 SHARD_MERGE_SUSTAIN_SECS=360 SHARD_SPLIT_REVERSAL_MIN_SECS=720
```

It records, per step, the shards the live instances own (from their
heartbeats) and the instances live, and fails a low step that ran on more
than one instance or more than 2 shards. `--drain-secs` waits with no load
for the merge back and the scale-in. Every server's log is kept in the
results directory. Run both tenancies under the machine lock, one at a time.

## 7. What to report

Add to `docs/SCALE-VALIDATION.md` §7's report:

```markdown
### #149 run (SCALE-VALIDATION-149)
Commit: <sha>   Compute scaling mode: external   container names (#253): yes|no
| Tenancy | Streams | held % | p99 ms | instances at start/end/max | shards at start/end/max | splits | merges | sheds | OOM | VALID / VOID |
| Drain | tenancy | time to 1 instance | time to <= 2 shards | merges | scale-ins |
Desired-count timeline: <t, desired, reason, live> for every change
```
