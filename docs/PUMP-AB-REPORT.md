# Pump versus tick on Prisma Compute and Tigris — eu-central-1, 2026-09-30

**Question.** Edge record #77 made the group-commit pump (`WAL_GROUP_COMMIT=1`,
`WAL_FLUSH_GAP_MS=10`, `WAL_POST_ACK_GATHER_MS=6`) the binary's default on
local evidence only: on the owner's rig against s3lite at 2 ms per
operation the pump halved append latency for closed-loop clients and wrote
about 2.2 times as many WAL objects. The open acceptance question was
whether that WAL cost carries to Tigris, where a WAL write takes tens of
milliseconds and WAL garbage collection has been outrun before (a 5 ms tick
in the pilot), and whether the latency gain survives the region.

**Method.** Two cells in eu-central-1, each its own Compute project and
Prisma bucket (`bench/soak` invariant 6), the same x86_64 binary built from
`eeff9b8a` (sha256 `7e07c4cc…`, verified on both servers by
`verify-running.py`), the same generator (`awsbench`, 32 streams, batches of
10 records of 1 KiB, tiers 1, 2, 4, 8, 12, 16, 24, 32, 48 and 64 concurrent
closed-loop producers, 180 s each, consumer on), released within 1.6 s of
each other and sampled every 45 s for 35 minutes. The only difference is
the server's WAL posture, set through the cell's environment and confirmed
in each server's startup log:

| cell | `WAL_GROUP_COMMIT` | `WAL_FLUSH_GAP_MS` | `FLUSH_INTERVAL_MS` | `WAL_POST_ACK_GATHER_MS` | startup log |
|---|---|---|---|---|---|
| tick | 0 | 0 | 25 | 0 | no pump line |
| pump (the default) | 1 | 10 | 25 | 6 | `WAL group-commit pump on shard=.. gap_ms=10 gather_ms=6` (four shards) |

Everything else is the checked-in campaign posture of `deploy-region.sh`
(the `compute-1g` profile, four shards, admission caps 512/256, absorb at
4 MiB or 60 s, `FRAME_COMPRESS=1`, per-stream record limiter raised to
100,000/s so it does not bind). Run id `pumpab-20260930T034929Z`; the raw
results stay outside the repository (`SOAK_HOME`), as every campaign's do.

## Client-observed: acknowledgement latency and rate, per tier

Medians of each tier's 20 s windows, first window of each tier dropped
(`harvest.py`); p99 is the worst window of the tier.

| tier | conc | append p50 tick | pump | pump/tick | append p99 tick | pump | roundtrip p50 tick | pump | req/s tick | pump |
|---|---|---|---|---|---|---|---|---|---|---|
| t01 | 1 | 82.5 | **64.0** | 0.78 | 575.5 | 417.5 | 127.5 | 105.5 | 10 | 12 |
| t02 | 2 | 83.1 | **70.7** | 0.85 | 425.7 | 394.8 | 112.5 | 141.6 | 20 | 22 |
| t03 | 4 | 81.9 | **71.2** | 0.87 | 350.2 | 368.9 | 108.5 | 115.0 | 40 | 44 |
| t04 | 8 | 85.5 | **74.7** | 0.87 | 516.9 | 347.9 | 112.0 | 114.5 | 80 | 89 |
| t05 | 12 | 75.4 | **68.1** | 0.90 | 558.1 | 250.1 | 96.0 | 93.0 | 142 | 154 |
| t06 | 16 | 74.2 | **69.3** | 0.93 | 288.0 | 286.5 | 96.5 | 101.5 | 194 | 206 |
| t07 | 24 | 75.8 | **70.7** | 0.93 | 465.7 | 438.3 | 103.0 | 100.5 | 284 | 299 |
| t08 | 32 | 76.7 | **72.8** | 0.95 | 710.7 | 436.0 | 107.0 | 103.0 | 376 | 395 |
| t09 | 48 | 83.6 | **80.0** | 0.96 | 272.4 | 288.8 | 133.1 | 111.5 | 516 | 544 |
| t10 | 64 | 82.1 | 83.8 | 1.02 | 330.5 | 310.3 | 115.5 | 120.0 | 698 | 684 |

Totals over the ramp: tick 421,222 requests (4,212,220 records), pump
438,025 requests (4,380,250 records); 0 errors and 0 throttled on both.

- The pump's gain is largest where the review predicted it, at low
  concurrency: 22% lower append p50 with one producer, 13 to 15% at two to
  eight, and it narrows to nothing at 64 producers, where both arms are
  bound by the same 40 ms WAL write and the pump's gather has nothing to
  add. Throughput follows the latency: +4% requests over the whole ramp.
- The c1→c2 boundary that soak5 measured for the gather is flat on both
  arms here (tick 1.01, pump 1.10): the tick arm has no two-generation WAL
  crossing at 25 ms against a 40 ms write, so this campaign does not
  reproduce that soak5 mechanism; the pump's advantage at c1 is the gather
  and the self-clocked flush, not that fix.
- Tail latency is not worse under the pump: p99 is lower in seven tiers of
  ten and within noise in the other three.

## The WAL cost on Tigris: fewer objects, not more

Every `/v1/debug/store` snapshot is a trailing 60 s window; 43 were taken
per arm during the ramp, 45 s apart, so the sums below overlap by about a
quarter and overstate both arms alike. The per-window medians do not.

| arm | `put:wal` sum | median per window | p90 window | `put:wal` p50 / p99 ms | `delete:wal` sum | median per window | `get:wal` | `head:wal` sum |
|---|---|---|---|---|---|---|---|---|
| tick | 126,425 | 4,131 | 4,895 | 40 / 151 | 127,116 | 4,073 | 0 | 72 |
| pump | **118,690** | **3,968** | **4,354** | 40 / 154 | 117,241 | 3,911 | 0 | 108 |

- The local finding (2.2 times the WAL writes) does not carry to Tigris.
  At a 40 ms WAL write the 10 ms floor never binds: the pump self-clocks
  to the write's round trip, and the 6 ms post-acknowledgement gather
  packs the producers' next records into the next WAL instead of paying
  a tick's worth of alignment. The pump wrote 6% fewer WAL objects while
  acknowledging 4% more requests: 0.271 WAL writes per acknowledged
  request against 0.300.
- WAL garbage collection kept pace on both arms: `delete:wal` matches
  `put:wal` window by window (deletes are 100.5% of puts on the tick arm
  and 98.8% on the pump arm over the sampled windows), no `get:wal` read
  storm on either, and `head:wal` stays at the poller's 36 per window.
  The concern from the pilot's 5 ms tick, WAL objects minted faster than
  they are reaped, does not appear at the pump's 10 ms floor in this
  region.
- Everything else the store does is the same on both arms within a few
  percent: manifest gets and puts, SST puts and gets, compaction deletes
  (`get:sst` 58k on both, `put:sst` 2.6k on both).

## Integrity and recovery

- Recovery (`recovery.py`): every ramp finished; the backlog and latches
  cleared 0.8 s (tick) and 1.6 s (pump) after the recovery window opened.
- Integrity (`reconcile.py`'s count bound, computed from the durable tail
  of each of the 32 streams per arm): tick 4,212,220 records acknowledged
  and 4,212,220 durable; pump 4,380,250 acknowledged and 4,380,250 durable;
  0 ambiguous requests on either arm. The exact-once walk of every record
  (the op-identity ledger against the stored records, `mode: exact-ledger`)
  walked 4,212,220 and 4,380,250 records and found every acknowledged
  operation exactly once, no ambiguous operation landed and no problem
  on either arm: verdict OK for both. The walk ran from the operator's
  machine at 0.5 to 4 MB/s and took about 1 h 50 min; the campaign
  then tore down its four services, two buckets and two projects.

## What this does and does not establish

- It establishes, on production hardware and the production object
  store, that the pump default of #77 costs no additional WAL traffic in
  eu-central-1 and lowers acknowledgement latency at every concurrency up
  to 48 producers, with garbage collection keeping pace. That is the
  acceptance evidence #77 lacked.
- It does not measure a region whose WAL write is faster than the 10 ms
  floor. Tigris's own write time for 1 KiB is 7 ms in SIN and NRT and
  10 to 15 ms elsewhere (`docs/TIGRIS-REGION-CENSUS.md`), but the write as
  the server sees it, with the connection and the WAL object's size, was
  40 ms p50 here in FRA and about 20 ms in SIN and NRT on the observatory's
  client-side probes. In SIN or NRT the floor could bind part of the time,
  so the WAL count there would sit between this run's 0.9x and the local
  2.2x; a SIN run of the same harness (`SOAK_REGIONS` with two SIN cells)
  would tighten that bound. The local 2.2x is the figure for a store that
  answers in 2 ms, which no Prisma region is.
- The RUNBOOK's 25 ms argument was about a 5 ms tick that outran WAL
  garbage collection in the pilot; the pump does not flush an idle shard,
  and at the loads run here its 10 ms floor produced fewer WAL objects than
  the 25 ms tick and left none behind.
- One region, one run, 35 minutes: the ratios at a single tier carry the
  noise of two 20 s windows; the direction is consistent across all ten
  tiers and both cost columns, which is what the decision needs.

## Harness changes this campaign needed

- `bench/soak` runs two arms in one region: a cell is a region name with
  an arm suffix (`eu-central-1-tick`); `provision.py`, `deploy-region.sh`
  and `harvest.py` map it to the region, key every file, service and
  project name by the cell, and `deploy-region.sh` sources
  `$SOAK_HOME/cell-<cell>.env` for the arm's `SOAK_WAL_*` posture.
- `bench/awsbench/Cargo.toml` declares its own `[workspace]`: since the
  server manifest became a workspace (a1d9dabf, 2026-09-08) cargo refused
  to build the generator from inside the tree.
