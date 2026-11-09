# Ramp rig: a shard's footprint, one queue's ingest, and #149's ramp on the Mac

Measurements for how a cell grows (prisma/streams#149; the scaling design's
stage 1, package 0). All run Linux servers in containers with Prisma
Compute's limits, by default the 1 GiB class (`mem_limit: 1g`, 1 vCPU, the
`deploy/profiles/compute-1g.env` memory profile, `ADMIT_RSS_SHED_MB=500`),
beside s3lite at 20 ms a request. None is a gate: they are measurements,
run under the machine lock (`~/.streams-k2/bin/locked.sh`), one heavy leg at
a time. Every driver takes `--dry-run`, which prints `DRY` lines and starts
nothing.

```bash
bench/ramp/build.sh                                  # Linux binaries + image (niced, 4 jobs)
bench/ramp/footprint.py --postures off on --shards 1 8 32
bench/ramp/ingest.py --sizes 1024 16384 65536
bench/ramp/ramp.py --instances 1 --tenancy one       # steps of 3 min, stops at the first miss
bench/ramp/envelope.py --size 4/8                    # one server's append envelope at a size
bench/ramp/packing_sizes.py --first-only             # the packing points per size
```

`build.sh` builds `streams-slate` and `s3lite` from this checkout (so the
server reports its commit) and `bench` with habanero's population and queue
modes (`population-mode.patch`, from prisma/habanero#771's
`examples/edge-bench/services/bench-modes.patch`, applied to a scratch copy
under `target/`), all for aarch64 Linux with `cargo zigbuild`, then the image
`streams-ramp`. Changes to habanero's patch, kept here: `BENCH_QUEUE_MAX_BYTES`
(the queue's `maxQueuedBytes`), `BENCH_QUEUE_TOKEN` (a bearer for the queue
routes), and random message bodies, a different window of a random
alphanumeric pool per message (a repeated filler compresses to nothing in
zstd frames; alphanumerics still compress about 25%).

## Postures (`rig.py`)

- `off`: auth, billing and the rollup off.
- `on`: `compute-1g.env` plus a cell profile, as a cell runs:
  `dedicated-cell.env` for one tenant, `shared-cell.env` for a tenant each;
  auth enforced from cell-admin feed files, billing required, the rollup
  on, the debug routes behind a bearer. `tenants.sh COUNT DIR` makes the
  feeds and tokens with `bench/k2cost/tenants-admit.ts` (cached per count
  under `~/.streams-k2/ramp-tenants/`; a rerun keeps the admitted cell and
  re-mints the 12 h tokens and workload JWT). The one tenant's quotas are lifted
  in its policy feed, so a ramp measures the cell, not one project's
  bounds (#149's single tenant was not throttled). A fleet of more than one
  server uses the workload JWT `tenants.sh` mints (the release posture
  refuses the static token); with auth off it uses a static token.

Secrets (the usage key, the debug bearer, tokens, the cell's signing key)
are made per run or per tenant set outside the repository, mode 0600.

## Instance sizes (`--size VCPU/GB`)

Compute scales to 8 vCPU / 16 GB. `footprint.py`, `ingest.py` and
`envelope.py` take `--size`, which sets the container's limits and scales the
profile's per-instance memory budgets with GB (`rig.SCALED`, recorded in the
run's `size-*.json`): the shed line, the two caches, the bulk budget, the
unabsorbed and project-pressure lines, the SSE connection cap. The feed budgets
stay, since the release posture refuses a feed above the 64 MiB a campaign
certified, on every profile.
The per-shard budget lines (`rig.PER_SHARD`: clean handles, tail ring) scale
with GB per shard where the shard count is known; the footprint leg keeps
the profile's, since it measures what a shard costs. Per-database and CPU
knobs stay.

Off 1 GiB the server runs without `MEMPROFILE_CERT=compute-1g`, which refuses to
start on any other bulk or compactor setting. `ingest.py` and `envelope.py` take
`--set KEY=VALUE` to vary a knob on top of the size (recorded), for instance the
compactor's, which stay at the profile's one fetch task and 1 MiB read-ahead
unless set.

## footprint.py: memory per resident shard

One server with `INITIAL_SHARDS=N` for each posture and N, through fixed
phases: boot, open (creates and touches 64 streams a shard, so every
shard's engine opens at once), idle, light (5 touches a second a shard),
rest, busy (2,000 touches a second over 64 streams a shard, past what one
vCPU serves), an optional busy hold (`--hold-mins`, the largest N), cool.
Every 0.5 s it reads:

- `/proc/1/statm` resident: what the admission shed line reads on Linux
  (`fleet::rss_bytes`) and what Compute's memory line charges;
- the container's cgroup `memory.current`: what the OOM killer charges,
  page cache included;
- `/v1/debug/load`: the server's own sample, its shed counters and its open
  shard engines.

`summary.json` gives each phase's settled median and peak, the growth in MB
a minute, and per posture and N the MB per resident shard over the boot
baseline.

## ingest.py: one queue's send rate on one shard

For each message size, a fresh cell of one server and one shard (posture
`on` by default: the dedicated cell) takes one queue created with
`maxQueuedBytes` 8 GiB (`--max-queued`) and an open-loop ladder (`--mb-steps`, MB a second, a new
queue a rung, `--secs` each after a 10 s warm-up) from `--producers` 8
senders with batches of up to 1 MiB (at most 1,000 messages), with no
consumer. A rung holds with no send error and at least 98% of its target
sent; the ladder stops at the first that does not, and the size's sustained
rate is the highest rung held. Every 5 s it samples resident memory, the
server's sheds and s3lite's `/_s3lite/stats2` (WAL PUTs and the live bytes
of each object class). s3lite runs on the host for this leg and the envelope
(`rig.HostStore`): it holds every object in memory, and a ladder's store
outgrows the container VM, whose OOM killer took it at about 13 GB. A ladder
also ends at `--store-max-gb` of live bytes (`store_full`), the rig's limit and
not a refusal. Per size and rung (`sizes.jsonl`): messages and MB a
second sent, send latency, the first refusal, RSS, the WAL PUT rate and the
store's growth.

## ramp.py: #149's tiny-stream ramp, compressed 60x

Each step is a fresh cell of `--instances` servers (1 to 8,
`FLEET_MIN=FLEET_MAX`), `--shards` shards (default: one an instance) behind
an nginx router that pins a request carrying `compute-instance: k` to
`streams-(k+1)`, as Compute's edge does. A multi-instance step first waits
for habanero's gate (every ring holds the instances, ownership is the fair
share, stable 30 s). The driver is the population mode: it creates COUNT
streams, then touches them open-loop, one 400-byte append and one read from
the start per stream per `--period` (60 s, against #149's hour), for
`--step-secs` (180 s) after a 30 s warm-up, as tenancy `one` or `each`
(`--auth off` runs without tokens).

A step holds, as #149's did, when every stream was created, at least 98% of
the touches due were served with no touch error, their p99 is at most 2 s,
and no server shed or was OOM-killed. The ramp stops at the first step that
does not hold (`steps.jsonl`, `watch-COUNT.jsonl` per server).

Because the touch period is 60× shorter, each shard sees 60× the cloud's
touch rate at the same stream count: limits on rates arrive at a sixtieth of
the stream count, limits on resident state at the same count. That is the
point of a fail-early rig; it is not #149's numbers.

Tenant-each runs to 100,000 tenants locally (the sets are cached). One million tenants and more is
unmeasured locally: a habanero-scale run, after the streaming auth feed.

With `--fleet-min` and `--fleet-max` (the design's interim posture:
`--shards 8 --fleet-min 1 --fleet-max 8`) the ramp keeps one cell for every
step and adds each step's streams to the last's (`BENCH_POP_CREATE_FROM`); a
scaler shim plays Compute's scaler, starting servers up to the fleet's desired
count (`fleet.desired.count` on `/operator/data.json`) and stopping the
newest after the desired count stays lower for 30 s, then reloading the
router (`scaler.jsonl`).

## envelope.py: one server's append envelope per size

A fresh cell of one server (posture `on`, `--size`, `--shards` default one a
vCPU) takes an open-loop ladder of appends (`--rates`, a second in total, of
`--payload-bytes` each, over 64 streams a shard) for `--secs` a rung. A rung
holds with at least 98% served, no error, a p99 of at most 2 s and no shed;
the ladder stops at the first that does not. `envelope.json` gives the highest
rung held in requests and MB a second, beside the cgroup's CPU and the RSS.

## packing_sizes.py: the standard packing points per size

For each `--sizes` and `--points` (`t` PACK-T, `s` PACK-S-MUX, `m` the
m-shared mix), `bench/k2cost/run-local.sh` runs the point with its server in a
sized container (`k2-server.sh`, `K2_SERVER_DOCKER=1`): tenants x `--density`
x the vCPUs, the memory budgets scaled as above, compute priced at measured
cgroup vCPU and `K2_INSTANCE_GB`. `points.jsonl` gives $/tenant-month at ab50
and owner prices, validity, the steady append p99, the RSS against its shed
line, the measured vCPU and the shards. `--first-only` runs one point per size
first. Run it from the worktree the image was built from: run-local checks the
server's commit.

## #149's run (`ramp.py`, one shard to start)

`--shards 1 --fleet-min 1 --fleet-max N` starts the cell on one instance and
one shard; servers past compose.yml's eight come from a compose file the
run writes (`fleet_compose`), so N may go to 64 (the Mac holds about 12 at
1 GiB). `--scale-in-secs 0` makes the Scaler follow `fleet.desired.count`
at once, as Compute's external mode does (the interim D ramp ran 30 s).
Every sample reads the shards the live heartbeats own, so each step records
its `shard_range` (first, last, max). `--low-until 1000 --low-max-shards 2`
fails a low step that ran on more than `--fleet-min` instances or 2 shards,
and `--drain-secs` waits with no load after the last step for the merge back
and the scale-in (`count: "drain"` in `steps.jsonl`). `--set KEY=VALUE` puts
server settings on top, for the trigger constants scaled to the 60x
compressed time. Every server's log is kept in the results directory.
`docs/SCALE-VALIDATION-149.md` §6 has the command.
