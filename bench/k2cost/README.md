# K2 cost harness (local phase)

Measures our internal cost per GiB produced, consumed and retained, using
Tigris Standard request and storage prices plus parametric compute, to compare with
Cloudflare K2's prices ($0.04/GB produced, $0.04/GB consumed,
$0.02/GB-month retained). The experiment design is
`target/k2-design.md` (rev 2). This directory holds design item C3: the
generator, the rig and the analysis. None of it is Rust, so the only
ratchet here is 1,000 lines per file; the s3lite census it reads (C2) is
`src/bin/s3lite/census.rs`.

| File | Role |
|---|---|
| `k2gen.ts` (+ `common.ts`, `produce.ts`, `consume.ts`, `churn.ts`, `corpus.ts`) | Bun load generator: `setup`, `produce`, `walk`, `tail`, `group`, `churn`, `subs`, `corpus-stats` (`bun bench/k2cost/k2gen.ts help`) |
| `run-local.sh <point-file>` | one point in the release posture, from boot to `summary.json` |
| `scrape.py` | the instruments: `loop`, `snapshot`, `usage`, `posture`, `envfile` |
| `price.py [run-dir] [--baseline run-dir]` | Tigris pricing, the §4.3 allocation, K2 revenue, per-GiB costs and margins, W(r), calibration, validity; with `k2cells.py` (cell pricing and allocation), `k2store.py` (stored bytes, k, the a/b/c fit, U) and `k2valid.py` (§8 checks) |
| `points/*.env` | one tiny smoke point and one full-scale point per design run (L0, L1, L2, L3, L5, L6, L7) |

## Running a point

```bash
nice -n 19 cargo build --locked --release --bin streams-slate --bin s3lite --bin pilot
bench/k2cost/run-local.sh bench/k2cost/points/L1-tiny.env      # about 2-7 min
WAL_GAP=500 LAT=40 bench/k2cost/run-local.sh bench/k2cost/points/L1-full.env
```

Run full points in the background, one at a time, never during a gate,
the suite or a mutation leg (AGENTS.md §1). The rig uses ports 9580-9584:
emulator 9580, s3lite 9581, streams-1 9582, streams-2 9583, pilot LB 9584.
It refuses to start if one is taken. Results go to
`$K2_HOME/results/<point>-<UTC stamp>/`, with `K2_HOME` defaulting to `~/.streams-k2`. The rig
never writes them inside the repository.

### Point files

A point file is a shell file that the rig sources. The rig's own defaults
take the caller's environment first, and point files use `${VAR:-default}`
for anything meant to vary, so one file covers a grid
(`SHARDS=16 WAL_GAP=1000 ... L2-full.env`, `SERVERS=2 ... L6-tiny.env`,
`QUIESCE=1 QUIESCE_MIN_SECS=900 QUIESCE_WINDOW_SECS=120 ... L3-tiny.env`). A value the point
sets plainly wins over the environment.

| Variable | Meaning (default) |
|---|---|
| `S3LITE_LATENCY_MS` | injected s3lite latency per request, a fixed RTT stand-in (20) |
| `SERVERS` | 1 = T-single (`FLEET_MAX=1`, no LB); 2 = T-launch locally (fleet of 2, `FLEET_MIN=2 FLEET_MAX=2`, pilot LB, `desired.json` seeded to 2) |
| `SERVER_ENV_EXTRA` | space-separated `KEY=VALUE` overrides of the pinned posture; `-KEY` removes a key |
| `PHASES` | one k2gen invocation per line, without `--base/--headers-file/--out/--ledger` (the rig appends them). Three lines are run by the rig itself: `idle SECS` (scrape only, a baseline), `restart` (graceful stop and start of every server on the same bucket, for cold reads) and `quiesce` (wait for quiescence now, so earlier phases' deferred work is priced as produce there and not in the phases that follow). A line ending in `&` runs in the background until the next foreground line ends. |
| `QUIESCE` | 1 = wait for quiescence (below) after the last phase and snapshot `quiesced`; the deferred window ends there (0) |
| `IDLE_SECS` | idle tail after the phases or quiescence (0). After a `quiesced` snapshot it is priced as floor and is the point's own per-cell baseline |
| `K2_BASELINE` | a priced run directory (an L6 idle point in the same posture) whose per-cell floor rates price.py uses instead of the point's own (`--baseline`) |
| `K2_ALLOW_COMMIT_MISMATCH` | 1 = run although the servers' `git_commit` is not the worktree HEAD (the rig refuses otherwise, §6; price.py voids the point either way) |
| `QUIESCE_MIN_SECS` / `QUIESCE_MAX_SECS` / `QUIESCE_WINDOW_SECS` | the GC-cycle floor, the give-up time and the stationarity window (1800 / 10800 / 300) |
| `USAGE_WAIT_SECS` | the longest wait for the meters to settle (330: the 300 s outbox sweep plus margin) |
| `POINT_NAME`, `SCRAPE_SECS` | the results directory name (the file's basename), and the scrape interval (10) |

Tiny points stay under 50 req/s, 3 minutes of load and 50 MB. Full
points follow design §9.1. L6 (c) with billing off is untested in the
release posture.

`$RUN` in a phase line (written `\$RUN` inside the double-quoted
`PHASES`) is the point's results directory: the rig expands it when it
runs the line, so `walk ... --expect \$RUN/phase-3.ledger.json` checks a
walk record for record against an earlier phase's acked ledger.

Two product rules shape the read and queue points:

- **A consumer pull leases at most one message per routing key**
  (per-key FIFO, `src/queue.rs`). Unkeyed records share one key, so on
  unkeyed data every pull delivers one message whatever `--pull` says.
  Group phases therefore read streams seeded with `--routing-keys K`
  (K at least the pull size), and a group's in-process producer takes
  the same flag.
- **A read without `routingKey` returns only records appended without a
  key.** Walks and tails of keyed streams take `--routing-keys K` and
  read each stream once per key (L5 (e)).

### What one point does

1. It starts the platform emulator (`platform-demo/src/emulator.mjs`) with
   one cell (`cell-k2`) and one customer project (`proj-k2` in `ws-k2`). It mints a
   credential with every scope (`streams.*`, `streams.usage.read` included)
   **before** boot, so the cells' first feed snapshot carries the grant.
2. It starts a fresh `target/release/s3lite` with the point's latency, and uses a fresh bucket
   `k2-<stamp>`, `PATH_PREFIX=k2data` and `FLEET_PREFIX=k2fleet`.
3. It starts `SERVERS` cells of `target/release/streams-slate` under `env -i`, so the
   server sees exactly the posture file and nothing from the caller's shell:
   - `deploy/profiles/compute-1g.env` verbatim;
   - the §6 engine posture `WAL_GROUP_COMMIT=1 FLUSH_INTERVAL_MS=25
     WAL_POST_ACK_GATHER_MS=6 FRAME_COMPRESS=1 ABSORB_BYTES=4194304
     ABSORB_AGE_SECS=60 TRIM_PER_OP=65536 TRIM_GLOBAL_BUDGET=65536
     ADMIT_MAX_INFLIGHT=512 ADMIT_MAX_INFLIGHT_PER_STREAM=256
     LIMIT_BYTES_PER_SEC=5000000 LIMIT_REQS_PER_SEC=1000
     LIMIT_RECS_PER_SEC=5000`, and `INITIAL_SHARDS=4` (D2, STAGING);
   - binary defaults for `WAL_FLUSH_GAP_MS` (10, P-exp), `TAIL_RING_BYTES` (0),
     the manifest and compactor polls, `OUTBOX_SWEEP_SECS` (300) and
     `TELEMETRY_DRAIN_SECS` (2);
   - the release posture: `STREAMS_AUTH_MODE=enforce` with the
     emulator's feed files, `FLEET_AUTH_MODE=workload` with its rotating
     `WORKLOAD_TOKEN_FILE`, `STREAMS_RELEASE_POSTURE=1`,
     `BILLING_MODE=required`, `USAGE_STREAM_KEY`,
     `ACCOUNT_ID=acct-k2cost`, `PROJECT_ID=proj-k2cost-deploy`,
     `CELL_ID=cell-k2`, `ROLLUP=1` on streams-1 only, and `AUTH_TOKEN`
     (see below);
   - finally the point's `SERVER_ENV_EXTRA`.
4. It writes `posture.json` (and refuses the point when a server's
   `git_commit` is not the worktree HEAD), starts `scrape.py loop`,
   snapshots the exact ledgers (`s3lite-start.json`, `store-start.json`)
   and runs the phases. It then snapshots `loadend`, waits for quiescence
   (`QUIESCE=1`) and snapshots `quiesced`, then idles `IDLE_SECS`. After
   that it snapshots `end`, reads the meters (`usage.json`) and runs
   `price.py` (with `--baseline $K2_BASELINE` when set).
5. It kills everything it started, including on Ctrl-C, and deletes the
   secrets directory.

### Auth and debug access

- **Data plane.** The emulator's credential secret is exchanged at
  `POST /v1/token/streams` for an RS256 JWT, which lives 600 s. The rig writes
  `authorization: Bearer <jwt>` and `prisma-encryption-key: <per-run
  key>` to a headers file under `$K2_HOME/secrets/<run>/` (mode 600).
  It rewrites the file atomically every 240 s, and k2gen re-reads it when it
  changes and after a 401.
- **Debug routes.** Every `/v1/debug/*` route mounts behind
  `debug::gated` (`src/http/debug.rs`), which checks the **deployment
  bearer**, `AUTH_TOKEN`, in every auth mode. In enforce mode an unset
  `AUTH_TOKEN` closes the routes; only off mode leaves them open. The
  release posture does not refuse `AUTH_TOKEN`; it refuses only
  `FLEET_INTERNAL_TOKEN` and static fleet auth. So the rig sets a random
  `AUTH_TOKEN` per run and gives scrape.py a file holding its header
  line. Customer JWTs never reach `/v1/debug/*`.
- **Meters.** `GET /v1/projects/proj-k2/usage` needs a customer
  principal with `streams.usage.read` and an unrestricted stream grant
  (`require_project_usage`). It must be read on the `ROLLUP=1`
  instance, because others answer 503. The rig polls it until
  `ingestRecords` reaches the generator's acked records and two polls
  agree, for at most `USAGE_WAIT_SECS`.
- Nothing secret reaches the results directory: `posture.json` redacts
  `AUTH_TOKEN`, `USAGE_STREAM_KEY` and every `*TOKEN*`, `*SECRET*` and
  `*ACCESS_KEY*` value.

### Knob check (against `src/config/cli.rs` and `src/config/load.rs`)

The binary reads every §6 knob: the pinned engine and limit knobs, every
line of `compute-1g.env`, `WAL_FLUSH_GAP_MS`, `INITIAL_SHARDS`,
`FLEET_MIN`/`FLEET_MAX`, `OUTBOX_SWEEP_SECS`, `TAIL_RING_BYTES`,
`MANIFEST_POLL_MS` and `COMPACTOR_POLL_MS`. `FRAME_COMPRESS`, the
`LIMIT_*` knobs, `FLEET_MIN`, `OUTBOX_SWEEP_SECS`, `MEMPROFILE_CERT`, the
`ABSORB_GLOBAL_*`, cache, `SSE_FEED_*`, `STORE_BULK_*` and `COMPACT_*`
knobs are read in `load.rs`; the rest are clap arguments in `cli.rs`.
`KEEP_AWAKE` is **not** a binary knob. It belongs to the Compute Bun
wrapper (RUNBOOK §11) and has no meaning locally. With `FLEET_MAX=1`,
fleet mode is off (`fleet_mode()` needs `FLEET_MAX > 1`), and the
non-fleet shard default is 1, hence the explicit `INITIAL_SHARDS=4`.

## Outputs

| File | Content |
|---|---|
| `posture.json` | binary sha256s, `k2gen.ts` sha256, worktree HEAD and dirty paths, each server's redacted env, `git_commit`, `boot_id`, `compactor_profile`, the startup "effective configuration" summary, and the prices used |
| `marks.jsonl` | `load_start`, `phase_start`/`phase_end` (index, mode, args, rc), `quiesce_request` (a `quiesce` phase), `load_end` (with `quiesce`: the point's QUIESCE), `quiesced` or `quiesce_timeout` (each with the request's `id`: `load_end` or `phase-<i>`), `end` |
| `scrape.jsonl` | every 10 s: `store` (debug/store minus the slow list: `totals`, the `?window=10` ring, gauges), `load` (debug/load), `usage` (debug/usage aggregates; the per-stream list summed, plus `frame_bytes_by_stream` up to 256 streams: the gauge series), `s3lite` (stats2 + stats); plus `event` lines (`boot_change`, `counter_reset`, `s3lite_reset`) and `quiescence` |
| `s3lite-{start,loadend,quiesced,end}.json` | the exact physical ledger (stats2 cells by tier/kind/op/status, `live_objects`, `live_bytes` when present, stats v1) |
| `store-{start,loadend,quiesced,end}.json` | each server's `/v1/debug/store` at the same moments, and `procs`: CPU seconds and RSS of every server and the router (`ps`) |
| `boundaries.jsonl` | the same pair at every phase start and end (and just before a `restart` stops the servers), one line each |
| `phase-<i>.jsonl`, `phase-<i>.ledger.json`, `phase-<i>.log` | k2gen windows, exact totals and console |
| `usage.json` | the project usage (raw and `effective`), settle status, poll count, each live customer stream's `usage/current` (`streams_current`), and every server's full `/v1/debug/usage` |
| `summary.json` | price.py's result (below) |

### Quiescence (design §8, as implemented)

§8's literal rule cannot hold in the billing-on release posture. It
asks for an empty absorb backlog and no SST, compaction or history PUTs
for 120 s apart from timer L0s. The telemetry system streams
(`_usage`, `_ops_*`) keep appending, absorbing and compacting on a
rhythm of about 60 s. An idle T-single cell with 4 shards showed:

- one stream always cycling through the 60 s absorb age;
- a history SST PUT about every 65 s;
- shard compaction-state PUTs every 1-2 min.

So a deferred tail ends when the cell is back on that floor. The rig asks
for quiescence at `load_end` and at every `quiesce` phase; `scrape.py`
answers the request with a `quiesced` mark (same `id`) once all of these
hold, counted from the request:

- at least `QUIESCE_MIN_SECS` have passed (1800 = two GC cycles of 600 s
  sweep + 300 s minimum age);
- at least `QUIESCE_MIN_SECS` have passed since the last sliding
  `QUIESCE_WINDOW_SECS` window of PUT bytes *above the floor* (more than
  2 × the quietest such window since the request + 256 KiB). The floor's
  telemetry writes are tens of KB per window, customer absorption and
  compaction MBs, so an absorption or history compaction that ends after
  load end restarts the clock and the GC of its inputs (≤ 600 s sweep +
  300 s minimum age later) falls inside the tail. PUT counts cannot do
  this: the floor's own count varies by ±40% between windows;
- at least `QUIESCE_WINDOW_SECS` have passed since the last window of
  deletes above their floor (2 × the quietest + 20): no GC burst is cut
  in half;
- absorption is not stuck: every server's `absorb_backlog.oldest_eligible_secs`
  is at most 2 × `ABSORB_AGE_SECS`;
- the heavy-PUT count is stationary: three consecutive windows of
  `QUIESCE_WINDOW_SECS` (300) agree within max(5, 30% of their mean).
  Heavy PUTs are multipart requests, compaction-state objects, and
  history-tier SST and meta PUTs.

The deferred window ends at the `quiesced` snapshot (§4.4). `IDLE_SECS`
after it is an idle tail, priced as floor and used as the point's
baseline. A `QUIESCE=1` point that times out, or a `quiesce` phase that
does, is void.

## Pricing (`price.py`)

- **Requests** are priced from s3lite's physical cells, never from
  s3lite's own `billing()` rollup, which bills 404s as free.
  - Class A ($5e-6): PUT, COPY, LIST, multipart create.
  - Class B ($5e-7): GET, HEAD.
  - Free: DELETE and abort, and the statuses 304, 412, 301, 307, 400,
    403, 405, 409, 411, 416, 500 and 501. s3lite's `4xx` bucket only
    holds 400 and 405, and its `5xx` bucket only 500.
  - Billed at the op's class: 2xx, 404, 429, 502, 503 and 504.
  - The headline column prices UploadPart, CompleteMultipartUpload and
    `POST ?delete` as Class A. The `alt` column prices them free.
    s3lite files a multipart create, its parts and its complete in one
    `multipart` cell, so in every window and phase group the alt column
    re-bills one create per logical upload the servers counted in that
    window (their `totals` at the window's bounds); windows then add up.
    s3lite also files DeleteObjects under `other/meta/delete`.
- **Windows.** `load` runs from start to loadend, `deferred` from loadend
  to the `quiesced` snapshot (to `end` without one), `tail` from
  `quiesced` to end, and `total` covers all. Each phase group is priced
  exactly from the boundary snapshots. Without them, price.py falls back
  to the 10 s series and records the error in `boundary_error_ms`.
- **Allocation (§4.3), cell by cell** (`k2cells.py`). Every request of
  every window lands in one bucket:
  - *floor by kind*, in every window: the fleet, registry and telemetry
    tiers (heartbeats, router reports, fleet LISTs, the rollup DB, read
    spool and monthly artifacts), and every GET/HEAD answered 404 on a
    manifest or compaction-state key (each open DB's pollers);
  - *produce*, in every window: GC deletes and history-tier work other
    than GETs (absorption, history compaction, history WAL probes), which
    land wherever their timers fire; and everything above the floor in
    produce groups, `quiesce` phases and the deferred window;
  - *floor at the baseline rate*: for every other cell, its idle rate
    (per cell and status) × the window's seconds;
  - above that rate: *lifecycle* in churn and setup groups; in groups
    with readers, GETs and HEADs go to *consume*, and shard write cells
    (WAL, L0, manifest, compaction PUTs, LISTs) split by commits: produce
    share = acked append requests / (acked appends + non-empty pulls +
    settles). Without any commits a consume group's writes are read-driven
    (billing appends for reads) and go to consume;
  - restart groups are *excluded* (shutdown flush and cold open), and the
    gaps between phase boundaries are reported, not attributed.
  Lifecycle is reported separately and folded into produce for the K2
  comparison. The read spool shares the telemetry tier with the rollup
  DB, so its writes are floor here, not consume.
- **Baseline** (per-cell idle rates, CPU and PUT bytes per second), in
  this order: `--baseline <run-dir>` (that run's `summary.json`
  `floor_baseline`), the point's post-quiescence idle tail (≥ 30 s), its
  idle phases taken after the first appends (DBs open), its idle phases
  before any data (flagged: the floor is understated). Baseline rates
  never include floor-by-kind cells, GC deletes or history writes. Every
  summary exports its own `floor_baseline` for other points to borrow.
  Without any baseline only the floor by kind is separated, and the
  margins say so (`margins_vs_k2.floor_basis`).
- **Per-GiB figures** are floor-excluded: `produce_requests` = (produce +
  lifecycle) $ ÷ meter `ingestPayloadBytes`, `consume_requests` = consume
  $ ÷ `readPayloadBytes`; `*_egress` and `*_compute` likewise;
  `*_total` is their sum; `*_floor_share` is the whole floor (requests,
  floor-tier storage, memory, idle CPU, floor PUT egress) spread by
  revenue (§4.3). A point whose reads all happen inside churn lifecycles
  gets no consume figure.
- **Margins vs K2** (`margins_vs_k2`): `requests`, `total` (requests +
  egress + compute above the floor) and `total_with_floor` per dimension.
  `blended` is the §1 pass-rule figure, 1 − cost/revenue over everything
  the run cost (all requests, storage, D1 compute, egress) against all
  meter revenue, with the decimal-GB, 25%-price-cut, egress $0 and $0.02
  variants and the floor-excluded one.
- **W(r)** (`wal_function`, L1 and L2): per produce group, billed
  `shard/wal/put` above its baseline rate per second per active shard
  (min(INITIAL_SHARDS, streams written)), against acked requests per
  second per active shard; `W_usd_per_s` prices that cell alone, and the
  group's floor Class A is reported beside it.
- **Storage** (`k2store.py`) is $0.02 per GiB-month on the average of
  UTC-daily peaks of s3lite `live_bytes` (a sub-day run: its peak). The
  customer tiers are shard and hist; the others are the floor's storage.
  The gauge is the meter's owned frame bytes: at the end, Σ
  `ownedStoredBytesNow` over the live customer streams; over time,
  debug/usage `frame_bytes_by_stream` for those stream ids (stitched
  across restarts; exact while nothing is trimmed or deleted).
  - `k_end` = customer bytes ÷ gauge at the end; `k_peak` = the customer
    daily peak ÷ the gauge at that moment. `retained_usd_per_gib_month` =
    $0.02 × `k_peak` (both sides on one basis, so the ingest ramp does not
    inflate it); the old peak ÷ run-average figure is kept as
    `retained_usd_per_gib_month_ramp_inclusive`.
  - `fit`: least squares of customer bytes = c + a·gauge + b·ingest frame
    bytes/s over the 10 s series (§4.1's S_phys): a is the steady
    amplification ($0.02·a per GiB-month), b the transient bytes per unit
    ingest rate, c the fixed per-DB bytes. Tiny points fit poorly; read r².
  - A point whose customer streams were all deleted or expired (L7) gets
    no retained figure. It reports U instead: customer bytes still live
    at the end minus at the start, per GiB deleted, and their monthly cost
    per GiB ingested.
- **Revenue** is K2's prices applied to the meters' `effective` values:
  per 2^30 bytes, with the decimal-GB total beside it (×1.0737).
  Retention uses a 30.4-day month.
- **The retained meter.** The project row's `storageByteSeconds` is the
  recorded integral: it advances only when a segment's gauge is
  accounted (appends, sweeps, closes), so it stops at the last append
  and a short point's idle tail adds nothing. `scrape.py usage`
  therefore also reads `GET /v1/streams/{name}/usage/current` for every
  live customer stream (provisional: the gauge extrapolated to the
  read), and price.py backs each off to the `end` snapshot by
  `ownedStoredBytesNow` x (read - end). The larger of the two sums is
  used (`meters.retained.basis`). It is exact when every stream is
  live, or every stream was deleted; a point that does both gets a
  lower bound.
- **Compute and egress** (the owner's D1, design §2): Prisma Compute's
  published price, 1 GiB provisioned per instance (compute-1g) × run
  hours × $0.006 (floor), plus active CPU-seconds ÷ 3600 × $0.064, capped
  at 1 vCPU per instance. CPU-seconds come from `ps` at every snapshot
  and phase boundary (`procs`), stitched per process across a restart;
  the baseline's idle CPU rate is floor and the rest is split by
  ingested versus delivered payload bytes. Local CPU is an M-series
  core, so it is indicative. Egress at $0.005 per 1e9 B: every byte PUT
  to the store (produce, less the baseline's PUT-byte rate, which is
  floor), client answers (consume when the phase delivered data, else
  produce; twice behind the router), and behind the router the
  router→server leg of request bodies. $0 and $0.02 are the sensitivity.
- **Calibration** compares the servers' logical `totals` (one count per
  object_store call: `store_timing` totals, stitched across boots) with
  s3lite's physical requests, mapped onto the same `op:class` grid.
  Expect ratios above 1 for `mpu`, since one upload is a create, its
  parts and a complete, and for `list` once it pages. In the
  single-server smokes without a restart, every cell's ratio was exactly
  1.0, and bytes put and got matched to the byte. The pilot
  router's requests, in the `fleet` class, are physical only.
- **Validity** (§8, `k2valid.py`). Counter checks run over the whole run
  (load window and deferred tail), widened to the scrape samples just
  before and after it; counter deltas are summed per boot, so a restart
  is never read as a decrease:
  - every server has `/v1/debug/load` samples around the run;
  - every rate-limit refusal, shed and wedge counter delta is 0, and so
    are `maintenance_backpressure.appends_shed` and
    `project_memory.project_memory_shed_total`;
  - maintenance backpressure never engaged: the `engage_count` delta is
    0 (an engage that cleared between scrapes still counts) and no
    sample shows `engaged`;
  - `absorb_lag_max_secs` ≤ 90; no scaler splits;
  - `boot_id` is unchanged unless the point has a `restart` phase;
  - `rss_mb` ≤ 450 (indicative on macOS), and its slope over the second
    half < 2 MB/min (judged when that half is at least 300 s);
  - the absorb backlog is not trending up over the load window (judged
    on load windows of at least 300 s);
  - the project meter was read and settled; acked records (every phase's
    appends, consumers' producers included) equal `ingestRecords`; and
    delivered payload bytes and records equal `readPayloadBytes` and
    `readRecords` (records not compared when a bytes stream estimated
    them);
  - every sent append was acked and every churn lifecycle completed; the
    open-loop pacing held (no send more than 100 ms late, none unsent);
  - no `quiesce_timeout` in a point that waited for quiescence;
  - the servers' `git_commit` is the worktree HEAD (`posture.json`);
  - every phase exited 0.

  A point that fails any check is void.

## Floor findings from the smokes (local, tiny scale; for the owner)

- **Every delete is a `POST ?delete`.** object_store's bulk delete
  appears in s3lite as `other/meta/delete`, and there are no keyed
  DELETE cells. A 16 min tail made 1,050 of them, so whether Tigris
  bills DeleteObjects (§1 prices it both ways) is material. The headline
  prices them Class A; `usd_alt` prices them free.
- **404 probes dominate Class B at idle.** These are the manifest and
  `compactions` GETs. A T-single cell with 4 shards made about 27
  GET/s during that tail, over 90% of them 404.
- **Fleet LISTs run at about 1/s even with `FLEET_MAX=1`.** At
  continuous Class A that is about $13 per instance-month.
- **The idle floor** was $0.05/h with 1 shard (L1-tiny) and $0.12/h
  with 4 shards and 8 streams (L6-tiny), before compute.
- **The meters settle late for reads.** `scrape.py usage` waits until
  `ingestRecords` and `readPayloadBytes` both reach the generators'
  acked records and delivered bytes (reads flush every 10 s) and two
  polls agree, for at most `USAGE_WAIT_SECS`.

## Known limits

- The servers' `totals` count one object_store call, not one HTTP
  request: a retried request counts once, and a multipart upload once
  (`mpu`) though its create, parts and complete are each billed.
  Locally, price.py prices s3lite's physical buckets, and the
  calibration table gives the physical-per-logical ratio the field
  pricing applies. A 404 is its own billed `not_found` since fcd16d84
  (edge #94, amended); an older binary reports it inside `unbilled`.
- The first append to a shard that is not open answers 503
  `temporarily_unavailable` (retryable). At boot the billing sweep
  evicts the idle shards it just opened, so a point's first appends
  meet them. k2gen's producers retry a retryable 429/503 the SDK's way
  (up to 3 retries after `retry-after`); every attempt stays in
  `status_counts` and `error_codes`, and `retries` counts them. price.py
  reports them in a note and does not void the point.
- Accounting windows do not start at boot + k x 600 s (§8). Long points
  should run whole GC cycles, or subtract an idle baseline measured over
  whole cycles (L6, `K2_BASELINE`).
- The commit split of shard write cells in mixed groups is a
  proportional model (WAL PUTs are group-committed). L5-full runs
  producer-only arms beside (d), (f) and (g) so the on-minus-off
  difference can check it.
- s3lite keeps everything in RAM, so cap a point at about 12 GiB of
  physical data (§8).
- The local RTT is a fixed per-request delay. Its size dependence is
  validated in the field runs (F3/F4).
- The rig does not subtract its own scrape traffic, because scrapes hit
  debug routes and s3lite's stats routes, which make no store
  requests. The usage poll runs after the `end` snapshot, so it is
  outside every priced window.
