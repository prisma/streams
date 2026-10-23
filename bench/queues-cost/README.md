# queues-cost: the native-queue cost benchmark rig

This rig runs the benchmark defined in
[docs/QUEUES-PRICING-BENCHMARK.md](../../docs/QUEUES-PRICING-BENCHMARK.md).
It replays Prisma's control-plane queue traffic against a server's
`/v1/queues` routes on the local costab rig. It measures what the server
spends at the object store and in CPU and memory, prices those costs for a
30-day month, and prints the verdict against the Cloudflare Queues ceiling.
The rig follows [docs/COST-METHODOLOGY.md](../../docs/COST-METHODOLOGY.md):
s3lite at 25 ms per operation, whose `/_s3lite/stats2` ledger is ground
truth, and the soak7 server environment of `bench/costab/run-soak.sh`. The
default ports are 9500 and 8090.

| File | Role |
|---|---|
| `scenario.json`, `cf_bill.py` | the scenario and the Cloudflare bill and ceiling; `--measured costs.json` prints the verdict |
| `schedule.mjs` | the replay's schedule, rate and fate math (pure functions) |
| `replay.mjs` | the generator: creates the queues, produces, consumes, verifies, writes the summary |
| `prices.mjs` | the pricing math (pure functions): windows, months, terms, the floor share, `costs.json` |
| `collect.mjs` | the instruments (`mark`, `sample`) and the pricing (`price`, `idle`) |
| `run-ab.sh` | the orchestration: `ab`, `idle` and `standalone` modes |
| `fake-server.mjs` | an in-memory `/v1/queues` used by the tests and the rig self-test; never measured |
| `*.test.mjs`, `tests.mjs`, `package.json` | `node --test bench/queues-cost/` (the package's `main` runs every test) |

All of it is Node 20+ ESM with no dependencies, plus bash 3.2 and Python 3
for `cf_bill.py`.

## Prerequisites

- A server binary that serves `/v1/queues` (the API contract of 2026-10-23).
- The census-capable s3lite. `run-ab.sh` uses `target/release/s3lite` when
  it exists; otherwise it falls back to `bench/costab/bin/s3lite-ab`, an
  older build without the `live_bytes` census. With the old build, storage
  is unmeasured and `collect.mjs` prints a warning.
- The stream key file outside the tree: `~/.streams-soak/costab-streamkey.txt`,
  as costab uses it, or `COSTAB_KEY_FILE`. The bearer token is generated per
  run into a private temporary directory, which is removed when the run
  exits. No secret is ever written to a command line or to the results.
- `bench/costab/bin/awsbench-ab` for the baseline co-tenant (`BENCH_SHAPE=wide`
  with as many active streams as streams and no scanner).
- An idle machine. Run one run at a time and no builds during a run
  (methodology invariant 4; AGENTS.md: one heavy leg per machine). Start a
  multi-hour run in the background.

## The runs

Each mode runs two arms, one after the other. Each arm gets a fresh s3lite
and a fresh `PATH_PREFIX`. An arm goes through these steps: boot, mark `t0`,
set up the baseline, `WARMUP_S`, mark `start`, the measured `DURATION_S`
(split at `burst_end` when `BURST_WINDOW_S` is set), mark `end`, the
replay's drain, mark `final`, teardown. Only `start`..`end` is priced.

| Run (benchmark §6) | Command |
|---|---|
| **1x A/B (gated)**: 48 h each arm; the first 24 h holds the month-start invoice burst and is counted once | `DURATION_S=172800 BURST_WINDOW_S=86400 bench/queues-cost/run-ab.sh ab q1x <server> <out>` |
| **10x A/B**: 2 h each arm at the busiest-hour shape | `SCALE=10 PROFILE=peak DURATION_S=7200 WARMUP_S=4800 bench/queues-cost/run-ab.sh ab q10x <server> <out>` |
| **100x A/B** | the same with `SCALE=100` |
| **Idle**: 6 h, consumers connected, no messages | `DURATION_S=21600 bench/queues-cost/run-ab.sh idle qidle <server> <out>` |
| **Standalone 1x**: 24 h, a dedicated cell (reported) | `DURATION_S=86400 bench/queues-cost/run-ab.sh standalone qsa <server> <out-sa>` |

- **Warm-up at peak.** The peak runs use `WARMUP_S=4800` because the Stripe
  queue spreads `deliverAt` over 80 minutes. Until that spread has filled,
  the scheduled backlog, and with it the stored bytes, is still growing.
  Each arm then lasts about 3 h 20 min plus the drain.
- **Standalone in the verdict.** To fold a standalone run into an A/B
  `costs.json`, set `STANDALONE_DIR=<out-sa>` on the `ab` run. You can also
  re-price afterwards:
  `node bench/queues-cost/collect.mjs price --a <out>/A --b <out>/B --sa <out-sa>/A --sb <out-sa>/B --scale 1 --out <out>/costs.json`.
- **Idle baseline.** The idle run keeps the baseline (`BASELINE=1`), as §6
  says ("no object requests beyond the baseline"). `BASELINE=0` gives the
  exact signal of an otherwise idle cell. `IDLE_TOLERANCE` (default 0) is
  the allowed excess over arm A, as a fraction of arm A's rate.
- **Compression.** `COMPRESS=K` runs the day shape K times faster at the
  same rates per second. Cron firings come K times as often with 1/K of the
  messages each, and the deliverAt spread of a pass shrinks by K. Fixed
  per-message delays and retry delays do not change. The month figure is
  still the measured rate times 30 days, so compression changes cron
  spacing, not rates. Use it to explore; the gated 1x run is uncompressed.
  `run-ab.sh` refuses a burst window with `COMPRESS` other than 1.
- **The invoice burst.** The month-start burst is replayed only when
  `BURST_WINDOW_S` is set. Without a burst window, a rate-scaled window would
  count the burst in every hour of the month.
- **Other settings.** `ARMS="B"` reruns one arm. `REPLAY_ARGS` passes extra
  flags to `replay.mjs`. `SERVER_ENV_EXTRA="K=V ..."` adds server
  environment, for example a queue feature flag or a commit window.
  `CELL_VCPUS`, `CELL_MEM_GIB` and `UTILIZATION` set the reference cell for
  the floor share.

**Rig self-test.** This needs no real server. It checks the plumbing, marks,
sampler, pricing and verdict end to end in about a minute. The object-store
terms are zero because the fake keeps queues in memory. The verdict is
meaningless: the fake's own Node CPU is billed, so it usually prints
`NOT MET` and exits 1. Check that `replay.log` ends `VALID` and that
`costs.json` is written.

```bash
printf '#!/bin/sh\nexec node %s/bench/queues-cost/fake-server.mjs "$@"\n' "$PWD" > /tmp/fake-slate
chmod +x /tmp/fake-slate
S3_PORT=9591 SRV_PORT=9592 BASELINE=0 WARMUP_S=5 DURATION_S=20 DRAIN_S=30 \
  COMPRESS=1440 START_HOUR=9.9 \
  REPLAY_ARGS="--delay-scale 0.001 --handler-ms 1,5 --wait-ms 500" \
  bench/queues-cost/run-ab.sh ab selftest /tmp/fake-slate /tmp/qcost-selftest
```

## Outputs

`<out>/A/` and `<out>/B/` each hold:

| File | Content |
|---|---|
| `marks.jsonl`, `snaps/<tag>-{stats2,stats,store}.json` | at each mark: the s3lite ledger (Class A/B per tier and kind, `put_bytes`, `live_bytes`), the server's `/v1/debug/store`, the server process's CPU-seconds and RSS |
| `samples.jsonl` | every 10 s: CPU-seconds and RSS; every 60 s: the bucket's live bytes |
| `replay-summary.json` | arm B: the replay's counts, latencies, per-queue server stats and its validity |
| `replay-timeline.jsonl` | arm B: cumulative counters every 10 s (sent, delivered, delivered bytes, ...) |
| `server.log`, `s3lite.log`, `baseline.log`, `replay.log` | the processes' output |

At the top of `<out>/`:

- **`costs.json`** (`ab` mode) has these fields:
  - `month_usd {fully_loaded, marginal, standalone}` at the owner's prices.
    This is what `cf_bill.py --measured` reads. `standalone` is null unless
    `STANDALONE_DIR` was given.
  - `month_usd_list`: the same three costs at Tigris list prices (the
    sensitivity).
  - `per_million_messages_usd`, over the scenario's messages a month at this
    scale (`messages_month`, the same count `cf_bill.py` bills).
  - `terms.marginal`: each term's monthly quantity and price. The terms are
    `class_a`, `class_b`, `transfer_in` (bytes PUT), `storage` (average
    bytes held), `cpu` (vCPU-seconds), `memory` (average RSS) and `egress`
    (bytes delivered to consumers).
  - `terms.floor_share`: the CPU share, the memory share, the larger of the
    two, and its dollars.
  - `months` and `windows`: the raw quantities per arm.
  - `void` and `void_reasons`, and `warnings`.
- **`costs.txt`**: the same costs as a table.
- **`verdict.txt`**: `cf_bill.py`'s bill, its ceiling and `goal MET`,
  `NOT MET` or `VOID (...)`. The script exits 0 only on MET.
- **`standalone.json`** (`standalone` mode): read `month_usd.standalone`.
  It is the scenario's measured cost on an otherwise empty cell plus the
  whole cell floor. The `marginal` and `fully_loaded` values in this file
  belong to that dedicated cell, not to the gated figure.
- **`idle.json`** (`idle` mode): object requests and bytes per hour, arm B
  minus arm A, per tier, and the empty-claim rate. The verdict is
  `IDLE_CLEAN` or `IDLE_NOT_CLEAN`.

**How costs are computed** (`prices.mjs`):

- **A month from the windows.** With one window, additive quantities are
  scaled to 30 days by rate, and averages (RSS, live bytes) are
  time-weighted. With a burst window, the burst window counts once and the
  steady window's rate fills the rest of the month.
- **CPU** is capped at `CELL_VCPUS` times the window, as Compute bills.
- **Marginal cost** is arm B minus arm A, term by term. Noise can make a
  term negative; it is reported as measured.
- **Floor share** is the floor ($95.3 a month) times the larger of two
  shares, each taken at the cell's planned utilization: the CPU share is
  marginal CPU-seconds over the cell's vCPU-seconds, and the memory share is
  marginal average RSS over the cell's memory. Fully loaded cost is the
  marginal cost plus the floor share.
- **Standalone cost** is the standalone pair's marginal cost plus the whole
  floor.

**Prices** (benchmark §6):

| Item | Owner's price | List price (sensitivity) |
|---|---|---|
| Class A requests | $3.75 per million | $5 per million |
| Class B requests | $0.375 per million | $0.50 per million |
| Storage | $0.015 per GiB-month | $0.02 per GiB-month |
| Transfer into Tigris | $0.01 per GB | |
| Memory | $0.006 per GB-hour (720 h) | |
| CPU | $0.064 per vCPU-hour | |
| Egress | $0.005 per GB | |

## What the replay does

- **Queues.** It creates one queue per scenario queue, named `qb-<name>`.
  - `maxAttempts` comes from the scenario.
  - The backoff is a constant retry delay: `initialMs = maxMs = retry_delay_s`,
    at least 1 s, with `multiplier 1`. Every retry op also carries the delay
    explicitly.
  - `leaseMs` is 30000.
  - The budgets are twice the backlog the shape can hold, and never below
    the defaults. At 100x the Stripe queue's 80-minute spread holds about
    4.5 M scheduled messages.
- **Producers.** They follow `schedule.mjs`:
  - **Stripe**: 14 sweeps a day in batches of 100, with `deliverAt` spread
    uniformly over 80 minutes.
  - **Spend-limit usage and enforce**: hourly sweeps.
  - **Offer limits**: hourly sweeps plus domain events, with a 10 s delay.
  - **Vercel usage**: 12 runs a day with a 2-minute delay.
  - **Notifications**: bursts at 10:00, 10:30 and 12:00.
  - **Invoices**: the month-start burst at the peak-hour rate, with 5 s and
    60 s continuations.
  - **Other queues**: single sends spread evenly.

  Bodies are JSON of the queue's average size and carry `{i, s, f}`: the
  queue index, the sequence number and the fate.
- **Consumers.** Per queue, the number of loops is at least the scenario's
  concurrency (or 2), enough for twice the peak rate, and at most 64. Each
  loop claims up to 10 messages (`leaseMs` 30000, `waitMs` 20000), sleeps a
  handler latency drawn uniformly from 50-500 ms per batch, and settles the
  batch in one request. Each message is settled by its fate:
  - **ack**;
  - **retry once, then ack**: at the rate that reproduces 1.0032 deliveries
    per message;
  - **retry to exhaustion**: at the dead-letter rate. The invoice queue
    fails 36.5% of its messages after all 6 attempts. With its 2.826
    deliveries per message, the scenario implies that every other invoice
    succeeds at once. The last attempt is a `retry` (the server exhausts it
    into failed), or an explicit `fail` with `--final-op fail`.

  Fates are a low-discrepancy sequence per queue, so the rates are exact
  rather than sampled.
- **Validity.** `ok: true` requires all of the following. Anything else
  voids the run, and `cf_bill.py` then prints `VOID`.
  - Zero unexpected errors. These include network errors, non-2xx answers
    other than retryable 429/503, stale or expired settles, deliveries after
    a terminal settle, and foreign or unknown bodies.
  - For every queue, the server's `due + scheduled + leased + retrying`
    equals the replay's sent messages minus those acked and failed (none
    lost).
  - The server's `failed` and `counters.{sent, acked}` equal the replay's
    counts.
  - The drain ends quiet: nothing due, leased or retrying, within
    `--drain-s`. Messages still scheduled are legitimately pending.
  - Once a queue has 1,000 terminal messages, its dead-letter rate is within
    0.02 of the scenario's.
- **Summary** (`replay-summary.json`) holds:
  - totals (`sent`, `delivered`, `acked`, `retried`, `failed`, `claims`,
    `empty_claims`, `throttled`, `pending`, `lost`, ...);
  - latency p50/p99 for `send`, `settle`, `lag` (claim time minus
    `availableAt`) and `send_lag` (schedule to send);
  - per queue: its counts, the server's stats, the checks, and observed
    against expected rates.

## Assumptions to confirm (marked ASSUMPTION in `schedule.mjs`)

- The offer-limit queue is half hourly sweep and half domain events, and the
  10 s delay applies to both.
- Hourly sweeps are staggered: spend usage at :05, offer at :10, spend
  enforce at :15 and Vercel usage at :20.
- Notification bursts are weighted 0.45 at 10:00, 0.45 at 10:30, 0.05 at
  12:00, with 0.05 steady. This puts 90% of the day in the 10:00 hour,
  against the measured 89.6%.
- Invoice continuations alternate between 5 s and 60 s delays. The burst
  day is 82% of the week's writes and the other six days share 18%.
- The peak profile runs every queue at its own peak hour, scaled so the
  peaks add up to the measured busiest hour (62,904). It is flat within the
  hour.
- The reference cell for the floor share is one 1 vCPU / 1 GiB instance at
  60% planned utilization (the local rig is one server instance).
- The server posture is costab's soak7 environment, not the `shared-cell`
  profile on `compute-1g`. The shared-cell profile needs the platform
  emulator's authentication, as `bench/k2cost` does.
