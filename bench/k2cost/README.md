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
| `k2gen.ts` (+ `common.ts`, `produce.ts`, `consume.ts`, `mux.ts`, `churn.ts`, `corpus.ts`) | Bun load generator: `setup`, `produce`, `walk`, `tail`, `group`, `churn`, `subs`, `mux` (a tenant's tails and groups through one multiplexed wait, `POST /v1/projects/{p}/live:wait`), `corpus-stats` (`bun bench/k2cost/k2gen.ts help`) |
| `run-local.sh <point-file>` | one point in the release posture, from boot to `summary.json` |
| `scrape.py` | the instruments: `loop` (with `--operator`, also `/operator/data.json`), `snapshot`, `usage`, `posture`, `envfile` |
| `price.py [run-dir] [--baseline run-dir]` | Tigris pricing, the §4.3 allocation, K2 revenue, per-GiB costs and margins, W(r), calibration, validity; with `k2cells.py` (cell pricing and allocation), `k2store.py` (stored bytes, k, the a/b/c fit, U) and `k2valid.py` (§8 checks) |
| `points/*.env` | one tiny smoke point and one full-scale point per design run (L0, L1, L2, L3, L5, L6, L7); the screening points `SCR-S`, `SCR-M` and `SCR-IDLE` on the launch posture (below) |
| `counts.py RUN` / `counts.py --compare REF CAND` | a run's screening counts per window (requests by cell and class, bytes PUT and GET, pump WAL flushes, ring hits, CPU seconds) to `counts.json`, and two runs side by side (below) |
| `oracle.ts` | the shared-cell leak oracle: a `--payload oracle` record names its project and carries a MAC under that project's harness key, which the server never sees. Every read path (pages, long-polls, SSE, pulls, churn's read-back, the audit) counts each record it is delivered as `oracle_ok`, `oracle_foreign` (another project's record: a leak), `oracle_bad_mac` (corruption) or `oracle_unmarked`. Only a generator that names an owner writes it (`tenants.ts`) |
| `tenants.ts` (+ `tenant-plan.ts`, `tenants/*.json`) | the shared-cell harness's multi-project driver: a tenants manifest of classes, counts, quotas and phases, every project on k2gen's own modes under its own token (below) |
| `resolve.ts` | the read-back `tenants.ts audit` runs for appends answered with an unknown outcome (`common.ts` `appendOutcomeUnknown`; `produce` and `churn` count them per stream as `unknown_records`): each stream they went to is counted from the beginning (or by the audit's own read), and the records past the acked ones, between 0 and the unknown ones, are written; a stream that is gone, a bytes stream or a count out of bounds is reported unresolved (`audit.json` `resolution`) |
| `audit.ts` (reads) | `tenants.ts audit` asks again for a read the cell refuses or fails (429, 503, 5xx, transport) for ten minutes since the last good page, as the read-back does (`common.ts` `READ_BACK`): a hostile project's streams are read at its read quota. A stream not read in time fails the audit with the class of its last answer; a 4xx the cell answers the same again fails it at once. Every failed audit records `causes` (`audit.json`; `failedBecause` or `read_failed <class>`), and the `leaks: every producing project's audit passes` bar prints `by_cause` and `detail` for the failing projects |
| `hostile.ts` | the hostile legs `tenants.ts` runs: phase modes `h9`, `h9b`, `cost` and run-level legs `h11t`, `h11t-fresh`, `probes` (below) |
| `shared_verdict.py` (+ `test_shared_verdict.py`) | the Q5 arms' bars and the arm manifest the single-cell RC gate reads (below) |
| `layer_b_summary.py` (+ `test_layer_b_summary.py`) | a Layer B run's public summary (each bar, the RSS line, the driver pools, the verdict) for the `layer-b` workflow, whose raw evidence stays on its runner (below) |
| `run-shared.sh` (+ `../docker/Dockerfile.rig`, `test_run_shared.py`) | the Layer B rig: one shared cell from a release directory in a linux/amd64 container at 1 vCPU / 1 GiB, s3lite at 20 ms, a tenants manifest, the verdict (below) |
| `tenants-admit.ts` | the manifest's projects onto a cell-admin cell (its own CLI's `init` and `admit`, each with its class's quota set), the token map (12 h tokens, re-minted every 8 h, optionally uploaded), the bundle's publication, and its three feed files for a natively run cell (`feeds`) |
| `packing.py price RUN` / `packing.py curve RUN...` (+ `points/PACK-{T,S}.env`, `tenants/pack-*.json`) | the packing measurement: N tenants of one profile on one launch cell, the instance's month and each tenant's share at the owner's prices against the K2 bill, the cell's limits, and the steps side by side (below) |
| `fake-cell.ts`, `*.test.ts` | the harness's tests, against a product-surface fake: `bun test ./bench/k2cost` |

## Running a point

```bash
nice -n 19 cargo build --locked --release --bin streams-slate --bin s3lite --bin pilot
bench/k2cost/run-local.sh bench/k2cost/points/L1-tiny.env      # about 2-7 min
WAL_GAP=500 LAT=40 bench/k2cost/run-local.sh bench/k2cost/points/L1-full.env
```

Run full points in the background, never during a gate, the suite or a
mutation leg (AGENTS.md §1). The rig uses five ports from `K2_PORT_BASE`
(9580): emulator BASE, s3lite BASE+1, streams-1 BASE+2, streams-2 BASE+3,
pilot LB BASE+4. It refuses to start if one is taken; points on distinct
bases run at once (a screening pair). Results go to
`$K2_HOME/results/<point>[-<K2_RUN_TAG>]-<UTC stamp>/`, with `K2_HOME`
defaulting to `~/.streams-k2`. The rig never writes them inside the
repository. `run-local.sh <point> --dry-run` prints the ports, the
binaries, the commit the servers are held to and each server's
environment (secrets redacted) and starts nothing.

The caller's environment picks what runs (never the point file):

| Variable | Meaning (default) |
|---|---|
| `K2_PORT_BASE` | the first of the five ports (9580) |
| `K2_SERVER_BIN`, `K2_S3LITE_BIN`, `K2_PILOT_BIN` | the binaries (`target/release/{streams-slate,s3lite,pilot}`; pilot only with `SERVERS=2`), recorded with their sha256 in `posture.json` `binaries` |
| `K2_SERVER_COMMIT` | the commit the servers must report on `/v1/debug/load` (`git_commit`, which `build.rs` embeds: the build tree's HEAD), 7-40 hex digits, for a binary built from another commit than the worktree HEAD (the default). `posture.json` `server_commit` records the expected commit, where it came from and what each server reported; the rig refuses a mismatch and price.py voids it |
| `K2_RUN_TAG` | a word in the results directory's name (`ref`, `cand`): two runs of one point started in the same second |

A screening pair runs one point on two binaries (each arm's
`K2_SERVER_BIN` and `K2_SERVER_COMMIT`), or on one binary with two
`SERVER_ENV_EXTRA`s, at the same time on two port bases with two
`K2_RUN_TAG`s, from one checkout (so both arms run the same harness and
s3lite), and is read with `counts.py --compare REF CAND`, which also
prints the server environment keys that differ between the arms.

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
| `POSTURE` | `k2` (the default): §6's pinned posture (below). `launch`: the launch cell, `deploy/profiles/compute-1g.env` then `shared-cell.env` as `deploy/composer/streams/env.ts` layers them (one shard, fleet off, an 8 MiB tail ring since 4f048b12, 32/16 MiB caches; `KEEP_AWAKE`, the wrapper's, dropped), with the rig's store, feed files, `REGION=local`, billing identity and `AUTH_TOKEN`; no engine pins, no fleet names, `INSTANCE_NAME` at its default; `SERVERS=1` only |
| `SERVER_ENV_EXTRA` | space-separated `KEY=VALUE` overrides of the posture; `-KEY` removes a key |
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

The rig sets its own defaults (`QUIESCE_*`, `IDLE_SECS`, `USAGE_WAIT_SECS`,
`SCRAPE_SECS`, `S3LITE_LATENCY_MS`, `SERVERS`, `POSTURE`, `QUIESCE`)
before it sources the point, so `${IDLE_SECS:-900}` in a point yields the
caller's value or the rig's default (0), never 900: a point that needs its
own value for one of them sets it plainly, as the screening points do.

The screening points run the launch posture for about 20-25 minutes each
(`SCR_SMOKE=1` shortens them to 4-6 minutes); each file's header names
the windows and cells that carry its verdict:

| Point | Load | Read with |
|---|---|---|
| `SCR-S` | Fable's profile S (2026-10-24 report §4.1): 20 streams of 1 KiB JSON singles at 2 req/s, a long-poll tail per stream, one group (pull 10, settle each; pulls wait 20 s, as the SDK's) on the 5 streams that take a quarter of the appends; 840 s, a short quiescence, 60 s idle | `to_quiesced`: `shard/sst/get` (ring, E2), `shard/wal/put` and `pump_flushes`, `put_bytes` |
| `SCR-M` | a quarter slice of profile M: 10 streams, 100 x 1 KiB JSON per request under 64 routing keys at `M_RATE` (3) req/s, two groups (pull 100, settle each), a long-poll tail per stream on key k0; 780 s, a short quiescence, 60 s idle; `PROJECT_SHARE_K=1 PROJECT_MEMORY_PRESSURE_BYTES=268435456`, since at k = 8 one project may append only 257,500 B/s and its 20 pulls hold the 32 MiB memory line (429 `project_memory_pressure`) | `to_quiesced`: `shard/sst/get` (E2), `hist/sst/put` and the deferred `put_bytes` (history fan-in), `shard/wal/put` |
| `SCR-IDLE` | a 30 s burst over 4 streams, 120 s to settle, then 1200 s of idle cell | `phase 4` per hour: `shard/wal/put`, `pump_flushes`, `shard/sst/put`, `shard/manifest/put`, telemetry cells (the `_ops_metrics` cadence) |

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
2. It starts a fresh s3lite (`K2_S3LITE_BIN`) with the point's latency, and uses a fresh bucket
   `k2-<stamp>`, `PATH_PREFIX=k2data` and (`POSTURE=k2`) `FLEET_PREFIX=k2fleet`.
3. It starts `SERVERS` cells of streams-slate (`K2_SERVER_BIN`) under `env -i`, so the
   server sees exactly the posture file and nothing from the caller's shell.
   `POSTURE=launch` is described above; `POSTURE=k2`:
   - `deploy/profiles/compute-1g.env` verbatim;
   - the §6 engine posture `WAL_GROUP_COMMIT=1 FLUSH_INTERVAL_MS=25
     WAL_POST_ACK_GATHER_MS=6 FRAME_COMPRESS=1 ABSORB_BYTES=4194304
     ABSORB_AGE_SECS=60 TRIM_PER_OP=65536 TRIM_GLOBAL_BUDGET=65536
     ADMIT_MAX_INFLIGHT=512 ADMIT_MAX_INFLIGHT_PER_STREAM=256
     LIMIT_BYTES_PER_SEC=5000000 LIMIT_REQS_PER_SEC=1000
     LIMIT_RECS_PER_SEC=5000`, and `INITIAL_SHARDS=4` (D2, STAGING);
   - binary defaults for `WAL_FLUSH_GAP_MS` (100, the one write tier; the
     L1, L2 and L3 full points pass it as `WAL_GAP`, default 100),
     `TAIL_RING_BYTES` (0),
     the manifest and compactor polls, `OUTBOX_SWEEP_SECS` (300) and
     `TELEMETRY_DRAIN_SECS` (8, 2 before edge change #117);
   - the release posture: `STREAMS_AUTH_MODE=enforce` with the
     emulator's feed files, `FLEET_AUTH_MODE=workload` with its rotating
     `WORKLOAD_TOKEN_FILE`, `STREAMS_RELEASE_POSTURE=1`,
     `BILLING_MODE=required`, `USAGE_STREAM_KEY`,
     `ACCOUNT_ID=acct-k2cost`, `PROJECT_ID=proj-k2cost-deploy`,
     `CELL_ID=cell-k2`, `ROLLUP=1` on streams-1 only, and `AUTH_TOKEN`
     (see below);
   - finally the point's `SERVER_ENV_EXTRA`.
4. It writes `posture.json` (and refuses the point when a server's
   `git_commit` is not `K2_SERVER_COMMIT`, or the worktree HEAD without it), starts `scrape.py loop`,
   snapshots the exact ledgers (`s3lite-start.json`, `store-start.json`)
   and runs the phases. It then snapshots `loadend`, waits for quiescence
   (`QUIESCE=1`) and snapshots `quiesced`, then idles `IDLE_SECS`. After
   that it snapshots `end`, reads the meters (`usage.json`) and runs
   `price.py` (with `--baseline $K2_BASELINE` when set) and `counts.py`.
5. It kills everything it started, including on Ctrl-C, and deletes the
   secrets directory.

### Auth and debug access

- **Data plane.** The emulator's credential secret is exchanged at
  `POST /v1/token/streams` for an RS256 JWT, which lives 600 s. The rig writes
  `authorization: Bearer <jwt>` and `streams-encryption-key: <per-run
  key>` to a headers file under `$K2_HOME/secrets/<run>/` (mode 600).
  The harness sends a `prisma-encryption-key`, `prisma-routing-key` or
  `prisma-consumer-version` line under its `streams-` name, because the
  Prisma Compute edge strips `Prisma-*` request headers (edge change #173).
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
| `posture.json` | `posture` and `port_base`, the binaries run (`binaries`: path and sha256; `binaries_sha256`), `k2gen.ts` sha256, worktree HEAD and dirty paths, `server_commit` (the expected commit, its source, what each server reported, `matches_expected`, `matches_head`) and `commit_mismatch_allowed`, each server's redacted env, `git_commit`, `boot_id`, `compactor_profile`, the startup "effective configuration" summary, and the prices used |
| `marks.jsonl` | `load_start`, `phase_start`/`phase_end` (index, mode, args, rc), `quiesce_request` (a `quiesce` phase), `load_end` (with `quiesce`: the point's QUIESCE), `quiesced` or `quiesce_timeout` (each with the request's `id`: `load_end` or `phase-<i>`), `end` |
| `scrape.jsonl` | every 10 s: `store` (debug/store minus the slow list: `totals`, the `?window=10` ring, gauges), `load` (debug/load), `usage` (debug/usage aggregates; the per-stream list summed, plus `frame_bytes_by_stream` up to 256 streams: the gauge series), `s3lite` (stats2 + stats); plus `event` lines (`boot_change`, `counter_reset`, `s3lite_reset`) and `quiescence` |
| `s3lite-{start,loadend,quiesced,end}.json` | the exact physical ledger (stats2 cells by tier/kind/op/status, `live_objects`, `live_bytes` when present, stats v1) |
| `store-{start,loadend,quiesced,end}.json` | each server's `/v1/debug/store` and `/v1/debug/timings` per shard (`pump`: WAL `flushes`, `barrier_acked`, flushed requests, records and bytes; `tail_ring`: hits, misses, evictions) at the same moments, and `procs`: CPU seconds and RSS of every server and the router (`ps`) |
| `boundaries.jsonl` | the same pair at every phase start and end (and just before a `restart` stops the servers), one line each |
| `counts.json` | counts.py's windows (`load`, `deferred`, `to_quiesced`, `tail`, `total`, one per phase group): `class_a`, `class_b`, `free`, `put_bytes`, `get_bytes`, `pump_flushes`, `pump_flushed_reqs`, `ring_hits`, `ring_misses`, `cpu_s` and every cell's count by status; the server's commit and sha256, the validity verdict and streams-1's redacted env |
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

## Packing (N tenants of one profile on one launch cell)

The question (owner, 2026-10-25): what does a tenant cost when an
instance carries as many tenants of its profile as it can hold? One
instance's fixed costs (the 1 GiB, the store's idle polls and system
writes, and the WAL: one PUT per ~101 ms pump window that holds any write,
about 9.5 a second per shard when written often) are shared by its
tenants, so the cost per tenant falls with N until a limit of the cell
binds. A packing step runs N projects of one of Fable's profiles (the
streams-cost report of 2026-10-24, section 4.1) on one launch cell, each a
separate project with its own token, admitted as a shared cell admits
them, and prices the instance's whole month from a 20-minute steady
window.

The rig is `run-local.sh`'s tenants mode (a point that sets
`TENANTS_MANIFEST`): the launch posture exactly as SCR-* runs it
(`compute-1g.env` + `shared-cell.env`), but no platform emulator: the
manifest's projects go through cell-admin (`tenants-admit.ts admit` on the
two profiles, so each project's quotas are the cell's ceilings at k = 8),
12 h tokens (`tokens`) and the bundle's feed files (`feeds`), and its
traffic is `tenants.ts run` (one driver for every project, each with its
own token and record key, its records the oracle's). The phase line
`tenants` snapshots the store, the pump and the server's CPU and RSS at
each manifest period's edges, so `counts.py` has a `period steady`
window; the scrape also reads `/operator/data.json` (the parked waits).
After the run the rig reads every project's meters (`tenants.ts usage`),
audits every produced stream (`tenants.ts audit`) and prices the step
(`packing.py price`).

| Point | Steps (`PACK_N`) | Per project | Holds per project |
|---|---|---|---|
| `points/PACK-T.env` (`tenants/pack-t-NNNN.json`) | 1, 100, 400, 1,000 (cell-admin's default `--max-projects`, the scale Layer B certifies) | T: 3 streams, 200 appends a day of 400 B JSON singles, one long-poll tail always connected | 1 parked wait |
| `points/PACK-S.env` (`tenants/pack-s-NNNN.json`) | 1, 5, 10, 20, 30, 46 (46 x 25 = 1,150 of the live pool's 1,200) | S: 20 streams, 2 appends a second of 1 KiB JSON singles, a long-poll tail per stream, one consumer group (pull 10, settle each) on 5 of them | 25 parked waits (20 tails, a pull loop per group stream) |
| `points/PACK-S.env` with `PACK_MUX=1` (`tenants/pack-s-mux-NNNN.json`) | 48, 100, 150 | S as above, its 20 tails and 5 group streams in one `mux` phase (layout 6A package 2) | 1 parked wait (one multiplexed wait carrying 25 subscriptions) |

Each step: admission, the manifest's 1,640 s (setup 0-120 s, the load from
180 s, `steady` 360-1,560 s), a short quiescence, 60 s of idle tail, the
usage read and the audit: about 40 minutes. `TOKIO_WORKERS=2` is what the
binary runs on a 1-vCPU instance. A manifest class's `pool` sizes its
driver pool so every wait is offered to the cell at once (the default 128
would hold the rest in the driver). `PACK_SMOKE=1` runs a step at time
scale 0.15 (about 4 minutes of run) to check a setup.

```bash
cd ~/code/streams                       # a checkout with this rig
export K2_HOME=$HOME/.streams-k2/streams-bench
S3=~/.streams-k2/queues-bench/bin/s3lite-5d84f296
BIN=~/.streams-k2/queues-bench/bin/streams-slate-<sha8>; SHA=<sha8>
PACK_N=100 K2_PORT_BASE=9770 K2_SERVER_BIN=$BIN K2_SERVER_COMMIT=$SHA K2_S3LITE_BIN=$S3 \
  bench/k2cost/run-local.sh bench/k2cost/points/PACK-T.env > $K2_HOME/console/PACK-T-0100.log 2>&1
python3 bench/k2cost/packing.py curve $K2_HOME/results/PACK-T-*-<stamp>... --out pack-t.json
```

Steps on distinct port bases run at once (their counts do not move with
host load; their CPU and RSS are indicative, as every Mac number is).

`packing.py price RUN` (`RUN/packing.json`): the instance's month at the
owner's prices from `period steady` (Class A and B as `k2cells` classes
them, PUT bytes as transfer in, the live bytes' growth held half a month,
the server's CPU seconds, the provisioned 1 GiB, the bytes delivered as
egress; no floor share: the instance carries only these tenants), the same
with Class A and B at half, each per tenant; a tenant's K2 bill from the
customer's bytes in the window (T keeps everything, S 7 days) and its 20%
ceiling; the WAL PUTs a second and the pump's fill of its ~9.9 a second
cap; and each limit's use: projects of 1,000, parked waits of the live
pool (`SSE_MAX_CONNECTIONS`, 1,200), vCPU of 1 (`--cpu-factor 2` for
F5's field factor), RSS of the shed line (500 MB), requests and append
bytes a second of the cell's envelope. A step is VOID when a compliant
tenant was refused, the cell shed in the window, an audit failed, a
project's meters did not settle, the window is missing or the server is
not the declared commit. `packing.py curve RUN...`: the steps side by
side, a line through the instance's month (its fixed part and the cost of
each added tenant), the N at which each limit's use reaches 1 along a
line through the steps, and the cost per tenant at the first of them.

## Shared-cell tenants (the Q5 harness)

`tenants.ts` drives one shared cell with many projects (deploy-q5 design
§3.2; shared-cells PLAN §4.1). A manifest (`tenant-plan.ts` documents the
shape) names tenant classes with a count, a role (`compliant`, `victim`,
`hostile`, `idle`), an optional quota set and phases. Each phase is a k2gen
mode (`setup`, `produce`, `walk`, `tail`, `group`, `churn`, `subs`) or
`idle`, started at `at` seconds (project j of n at `at + spread * j / n`)
and bounded by `secs`. Every project of the class runs every phase, in the
same process, under its own token and its own record key, and writes
oracle records (`oracle.ts`), so every read is checked for leaks. Three
more phase modes and three run-level legs are the hostile legs (`hostile.ts`):

| Mode or leg | What it does |
|---|---|
| `h9` | 64 concurrent 8 MiB reads from the beginning of the project's streams, then a cheap read every `--probe-ms` until the read debt clears (no 429), then again until the phase ends; per burst the answers, bytes, latency and the debt's clearing time. A burst always finishes, so every page the server served is counted |
| `h9b` | `--readers` raw GETs of an 8 MiB page that stall after the response headers for `--hold-secs` (the server's response buffers stay full), then resume and read to the end: statuses, bytes before the stall, resets during the hold, early graceful closes, bodies cut short. A page cut short (the drain floor, edge #107) was billed when the server rendered it, so it is read again from the same cursor: matched to the cut page (its content-length and the bytes before the cut) it counts as billed (`cut_short_records`/`cut_short_payload_bytes`, added to the billing leg's read expectation), the read again as delivered; unmatched it is `cut_short_unresolved` and the project differs |
| `cost` | the cost-hostile mix at `--rate`: catalog reads, creates of new names, reads of names that never existed, a usage read of a project not its own; answers per kind |
| leg `h11t` | `--pairs` timed pairs on the anonymous capability path (`GET …/watches/w/keys/<16 hex>?cap=<B>.x`): a quiet name against a name that never existed in the same project, in random order, at most `--rate` pairs a second: a pair starts at least 1000/rate ms after the one before it, and never sooner to catch up after a stall (`gap_min_ms` is the shortest gap, `starts_ms` every start). Up to `PAIRS_IN_FLIGHT` (8) pairs are in flight at once, each pair's two requests one after the other, so a rig answering in ~110 ms still meets the rate (one pair at a time needed 90 ms answers); a pair once started runs to its end, the window stopping only new ones. A quiet name is one the `--target` class's setup created and nothing touched since, so its descriptor slot on the cell is past its 5 s TTL; the names are those the target's setup ledgers record as created (`setup.names`), never names derived from the manifest, and a target whose setup recorded none fails the leg (a quiet name nobody created would match a never-held one by construction). The manifest check refuses an `h11t` target that runs anything besides setup. Answers must be identical; the timings go to `shared_verdict.py`'s KS test |
| leg `h11t-fresh` | the same pairs, paced the same, on a `--target` class that uses its names every few seconds (from its setup record too): a name used in the last 5 s against a never-held one. The server answers the first from its fresh descriptor slot without the store, so the two stay apart: the residual the owner accepted (P4), which `shared_verdict.py` reports as a fact and never judges |
| leg `probes` | up to `--pairs` cross-project probes from a class with no streams (`--from`) to the classes `--to`, each a first contact: every (prober A, target name) once, in seeded random order. A reads a stream only B has against a name nobody has (status, code, header names and the masked body must match), B's usage against a never-placed project's, and then creates B's name in its own namespace (201) and deletes it; A never reads that name again. Pair i starts no sooner than i/rate s into the leg, up to `PAIRS_IN_FLIGHT` (8) pairs at once, each pair's six requests one after the other (one pair at a time needed 45 ms answers). A pair with a load refusal on any of its four reads (`PROBE_LOAD_REFUSALS`: 429 `project_rate_limit` or `project_memory_pressure`, 503 `read_memory_busy` or `overloaded`, each answered before the server looks at the name or project) is unjudged, never retried, and replaced by the next unused (prober, name); every other answer, a 429 or 503 with any other code or none included, is judged. `first_contacts` (the distinct pairs judged) is the bar's value, `available` the probers times the names (layer-b: 139 x 21 = 2,919, so up to 919 pairs may go unjudged before the leg falls short); `difference_classes` counts each difference as `<name|usage> <held status:code> vs <nobody's status:code>`, and `unjudged` the replaced pairs by class and by the side refused (`held`, `never`) | A reader
phase embeds no producer and a produce phase never repeats, so the audit
can replay every acked record.

```bash
bun bench/k2cost/tenants.ts plan  --manifest bench/k2cost/tenants/layer-b.json
bun bench/k2cost/tenants.ts run   --manifest M --base URL --token-map F --out DIR [--time-scale F]
bun bench/k2cost/tenants.ts usage --manifest M --base URL --token-map F --out DIR    # after the drain
bun bench/k2cost/tenants.ts audit --manifest M --base URL --token-map F --out DIR    # after usage
```

| Manifest | What |
|---|---|
| `tenants/layer-b.json` | Layer B on the single-server shape, 1,000 projects: S0 ×8, S1 ×8, S2 ×12, S3s ×4 (100 SSE subscribers each), N0 ×8, the victim, H1-H8 at Q-hostile and again at the cell's k ceilings, the cost-hostile tenant, H9 and H9b, the h11t-fresh target (10 appends a second over 16 streams), 200 warm idle (about 1 MiB each) and cold idle (562 with 3 empty streams, 139 with none). Both quota postures (lead decision deploy-q5 #6 (1)): those classes run at the cell's ceilings as the stress, and free-tier twins of S0, S1, S2, S3s, N0 and the victim (as many projects each, except S3s's twin: one, so the noisy period's 830 SSE connections and 184 parked waits fit the instance's live pool of 1,200, where four held 1,130 and left about 114 waits active; lead decision of 2026-10-14 night) run at the issuer's free-tier table (quota set `free-tier`, `requests_per_sec` 10), shaped, on purpose, under the stricter one-second bucket of 10 tokens, not the request bucket's 50-token floor (#162; lead decision at landing-5, 2026-10-18: the conservative shape is kept, so the twins never exercise the 50-request burst): each twin's mean (appends, pulls, settles, reads, SSE connects) at most 5 requests a second, as k2gen's appends are Poisson; a twin with a consumer group appends at most 0.1 times a second (S1's twin as a :batch of 410 every 10 s), as each append brings a pull's return and a settle within milliseconds, and starts its consumers a second apart (`group --stagger 1`); S3s's twin connects 4 subscribers a second, the victim's appends 2 a second; every twin creates its streams a second apart (`setup --stagger 1`) and never deletes. `tenants.test.ts` pins the set to the issuer's `FREE_TIER_REQUESTS_PER_SEC` and these rules, and the live pool's hold. Setup 0-600 s, the compliant classes 600-4,200 s, the hostile ones from 1,800 s, H11t (2,000 pairs at 8 a second on the cold class's quiet names, its own 360 s window) 2,040-2,400 s, H9 2,400-3,000 s, H9b 3,000-3,300 s, h11t-fresh (400 pairs, a fact) 3,300-3,420 s, the probes (2,000 pairs) from 3,600 s; the victim's periods `solo` 900-1,800 s and `noisy` 2,100-4,200 s. Writers stop 60 s before the readers of their window |
| `tenants/layer-b-tiny.json` | Layer B's classes and legs, one project per class (three warm, three cold, two with no stream) and the free-tier twins shaped as Layer B's, about 4 min: for building the harness. `tenants.e2e.test.ts` runs it on the real binary (`TENANTS_E2E_BIN_DIR=target/release`), in CI's sdk-package job too |

Free-tier twins' setup (found red on 2026-10-14, settled by the owner's
call 2 of 2026-10-15). A create weighs 11 of its project's store budget
(a 404 and a PUT; edge record #143). A project's first create since its
cell booted also walks the project's streams to seed its `max_streams`
count (`count_project_streams`, `src/application/creation/product.rs`;
every admitted project has the quota), and that walk's LIST made the
create weigh 21 (measured on a DST cell with `max_streams` 10: 21, 11,
11): at `requests_per_sec` 10, `setup --stagger 1` sent each twin's
second create inside the first's 1.1 s of debt, refused `429
project_rate_limit` once (the lane's two runs of `tenants.e2e.test.ts` on
2026-10-14; s0-free: 201 x3 and 429 x1). The owner's call 2 charges a
seed walk that completes to no project (#143 as amended; the server
commit "Every served create weighs 11, a project's first since its cell
booted included"), so every served create weighs 11. The budget holds 10
and refills 10 a second, so a create a second after the one before finds
one less than that one found: a twin's k-th create finds 11 - k, and
with at most 10 streams a twin (`tenants.test.ts` pins it; n0-free has
5, the most) none meets the budget in debt. The stagger stays 1 s. This
holds on a binary that includes that commit; one without it still
refuses each twin's second create once. A seed walk that fails (503
`catalog_unavailable`) stays charged. By the lead's decision deploy-q5
#6 (1) an honest refusal that survives the shaping is a PB-E3 finding,
never an allowance.

Admission and tokens (operator side, where the cell's signing key is):

```bash
bun bench/k2cost/tenants-admit.ts admit  --manifest M --state DIR --key-file K \
    --cell-id C --deployment-project P --account A       # init once, then admit what is missing
bun bench/k2cost/tenants-admit.ts tokens --manifest M --state DIR --key-file K --out MAP --every 28800 \
    [--upload s3://BUCKET/KEY --s3-endpoint URL --s3-creds FILE]
bun bench/k2cost/tenants-admit.ts publish --state DIR --upload s3://BUCKET/KEY --s3-endpoint URL --s3-creds FILE
```

`admit` runs cell-admin's own CLI (`init`, then `admit` per project, with
the class's quota set as `--quota` flags): 1,000 projects took 6.7 s here.
`tokens` mints with `deploy/cell-admin/tokens.ts`, writes the map with mode
0600, never prints a token, and with `--every` re-mints until stopped. S3
credentials come only from a 0600 JSON file `{"accessKeyId", "secretAccessKey"}`.

The token map is a JSON list `[{"project", "token"}]`, re-read every 10 min
and soon after a 401.

Connections: every short request goes through a bounded keep-alive pool
(`conn-pool.ts`): one per class, shared by its projects, and one per
run-level leg, each of `CLASS_POOL_SOCKETS` (128, twice the cell's
per-project in-flight ceiling of 512 / k 8) sockets kept open between
requests. Bun's fetch keeps only 64 idle sockets per process and closes the
rest as their answers end, so a driver with more in flight opened a
connection for most requests: in the Layer B rehearsal of 2026-10-19 h1 and
h1k (400 appends a second each, thousands in flight against a slow cell) ran
the macOS host out of its 16,383 ephemeral ports (30 s TIME_WAIT: about 546
new connections a second), and every class and the scrape met
`EADDRNOTAVAIL` for 35 minutes. A request that finds its pool's every
socket busy waits in the pool, in arrival order, before it is sent or
timed: the driver's own backlog, counted as `pool_waits` (and
`pool_unsent` when its caller gives up while it waits: status 0, error
`unsent: ...`, never a request), never a status or a transport error. An
open-loop producer's append latency is still measured from its scheduled
arrival, so that wait is in it. SSE subscriptions stay on fetch: each holds
its connection while it lives. Outputs under `--out`:

| File | Content |
|---|---|
| `run.json` | the manifest's name and sha256, the run's window, the classes, the token map's reads, failed phases, and `pools`: each keep-alive pool's sockets, peak in use, peak waiting, waits, wait times and unsent requests (below) |
| `windows.jsonl` | one line per class per 10 s window: the class's summed counters, latency percentiles, error codes (`status:code`, the typed refusals), connections and requests in flight |
| `loop.jsonl` | one line per 10 s window: the driver's own event-loop lag (a timer due every 50 ms, how late it fired: p50, p99, max, ticks); `run.json`'s `loop_lag` has it over the run and each manifest period. One process drives every project, so this lag is in every latency the run measures |
| `classes.json` | per class: totals, error codes, refusals (`typed` by code, `untyped` by status, `transport`), latency over the whole run (`run`) and over each manifest period |
| `projects/<id>/phase-<i>.ledger.json` | k2gen's ledger of each phase (the audit's `--expect`) |
| `projects/<id>/ledger.json` | the project's totals, oracle counts, error codes and refusals |
| `legs/<leg>-<i>.json` | a run-level leg's result (`h11t`: the quiet names' count, answers, differences, the pacing, the quiet and never timing samples and their KS, or the error that stopped it; `h11t-fresh`: the same with fresh samples; `probes`: pairs, differences and their classes, the pairs load left unjudged, examples, answers) |
| `usage.json` | `usage`: every project's meters (`GET /v1/projects/{p}/usage`) polled until they reach its ledger and two polls agree |
| `audit.json` | `audit`: every project's produced streams read back record for record against its produce ledgers, with the oracle's counts. It reads the names the project's setup recorded as created (`setup.names`), and any name a ledger acked records to; a name setup could not create (a stream limit: H8 at Q-hostile holds 50 of its 400) is listed in `not_created` and not read, every block sent to it counted missing, so every request to it must have failed. A created stream that answers 404 still fails its audit |

### The verdict and the arm manifest

```bash
python3 bench/k2cost/shared_verdict.py layer-b RUN_DIR        # writes RUN_DIR/layer-b-manifest.json
python3 -m unittest discover -s bench/k2cost -p 'test_*.py'
```

CI's `sdk-package` job runs these Python tests and `bun test ./bench/k2cost`
(where `tenants.e2e.test.ts` skips), by the owner's round-3 call 13, and
`scripts/quality/test_layer_b_ci.py` holds the job to both.

`shared_verdict.py` judges a `run-shared.sh` run (its `rig.json`,
`marks.jsonl`, `scrape.jsonl` and `tenants/`) against named bars and writes
the manifest `scripts/rc-certify.sh --shape single-cell` reads (arm,
`server_sha256`, verdict, legs, run window, projects; docs/OPS-RELEASE.md
§7) plus the fence fields: commit, the profiles' sha256s and pins,
`cell_env_sha256`, the harness trees, the region, every bar's value, and
`facts`, what the run measured and nothing judges (`H11t-fresh`: its pairs,
answers and the fresh-versus-never KS, or null without the leg;
`driver-pools`: the driver's keep-alive pools that met a backlog, any wait
for a socket or unsent request, from `run.json`'s `pools`, so a FAIL in a
class listed there may be the driver's; null for a driver without pools). The
layer-b legs: `H9`, `H9b` (each ran, 0 `admit_shed_rss` in its window),
`H11t` (2,000 quiet-versus-never pairs, identical answers, KS p > 0.01:
the owner's round-3 call 9, (a), 2026-10-13; see below), `leaks` (0 foreign,
bad-MAC or unmarked records on any read, every producing project's audit
passes), `probes` (2,000 first contacts, every pair one; 0 differences,
every create 201: the bar counts distinct (prober, name) first contacts,
not pairs run, by the owner's decision of 2026-10-13, round-3 call 13, a
change to what the gate counts, AGENTS.md section 1 (3)),
`compliant-refusals` (0, every typed code counted), `hostile-typed`
(untyped answers under 0.5%), `victim-p99` (noisy <= max(2 x solo, 25 ms),
appends and reads, for each victim class, its bars named by the class;
flagged `harness_bound` when the driver's own event-loop
lag p99 in the noisy period is at least a quarter of the victim's p99, as
the victim is timed in the process that drives every project), `billing` (every project's meters settled at its
ledger; an append answered with an unknown outcome (a 408, a transport failure, a 5xx: the SDK's `AppendOutcomeUnknownError`) may have been written and billed, so the ingest expectation is the acked records plus the ones the audit's read-back found written (`resolve.ts`); a project whose resolution is missing, short of its ledger's `outcome_unknown_records` or names an unresolved stream still differs), `memory` (peak < 500 MB, slope < 2 MB/min over the load's last
30 min), `Q0` (back within the idle baseline + 20 MB 10 min after the
load), `load` (the run's manifest is the server commit's
`tenants/layer-b.json` byte for byte, at time scale 1: an edited manifest
or a `--time-scale` rehearsal cannot certify; the manifest records both,
and the quota posture: which classes ran at the cell's ceilings, which
under a quota set), `harness` (the checkout the harness ran from,
`checkout.json` in `rig.json`, holds the server commit's `bench/k2cost`,
`deploy/cell-admin` and `bench/docker` trees, nothing uncommitted under
them: a harness fixed after the RC commit cannot judge it, so Layer B runs
first, rehearsed before the cut, docs/OPS-RELEASE.md §7, a rule no run can
follow until the rig takes a cut release, below),
`identity` (the wrapper's `binary sha256` line, `/v1/debug/load`
and the shipped file agree), `release-binary` (not a `--binary`
rehearsal). The owner confirmed `load`, `harness` and the first-contact
bar as built on 2026-10-13 (round-3 call 13). Known-red legs (Q0) fail like any
other; `known_red` only says why. The bars of the owner's decision D4
(a) of 2026-10-12 (SC0-1s projection, day-90 open, idle RSS, restart
window) are constants here for the field arms, which take their evidence
from the field adapters.

H11t's bar, and what it does not certify. On the anonymous capability
path the server answers a capability against a fresh descriptor slot (one
read through the cell's cache in the last 5 s; a cache hit does not renew
it) without the store, and against an expired slot or none after one
budgeted store GET that fills nothing. A quiet name and a never-held name
both take that GET, so their answers must be identical and their times
must not tell them apart; a regression that answers an expired slot
without the store fails the KS bar. A name used in the last 5 s is answered
from its fresh slot, faster than a never-held one: the residual the owner
accepted (P4, 2026-10-12, edge record #141's "Given up"), measured by the
`h11t-fresh` leg and reported in the manifest's `facts`, never judged.
The rig's s3lite sleeps the same 20 ms before every operation whatever it
answers (`--latency-ms`), so the bar certifies the server's path only: a
real store's latency difference between a 404 and a 200 is invisible to
it. If the first Layer B run fails the quiet KS bar, the measured gap goes
back to the owner with two options (call 9's fallback): judge the answers
only and document the gap in #141, or close it in the server; it is never
absorbed into the bar.

### The Layer B rig (`run-shared.sh`)

```bash
bench/k2cost/run-shared.sh --release ~/.streams-releases/<tag> [--manifest M] [--time-scale F] \
    [--bun-baseline-sha256 HEX]          # an arm64 host: Rosetta has no AVX for bun's x64 build
bench/k2cost/run-shared.sh --release DIR --binary PATH ...   # a rehearsal (layout 5's, the pre-cut one): any x86_64 binary
bench/k2cost/run-shared.sh --release DIR --dry-run ...       # stage and print, start nothing
```

The release directory is one `scripts/cut-release.sh` cut, in
`deploy/composer/release.ts`'s layout. The rig stages its `deploy/` tree
alone, the build dir Compute ships (`release.json`, `cells/`, `profiles/`,
`composer/app.ts`, the bundled `composer/streams/entry.js` and the
release's own x86_64-musl `streams-slate`; no `node_modules`), with its own
`deploy/cells/scratch.env`, builds `bench/docker/Dockerfile.rig` over it
and runs `bun deploy/composer/streams/entry.js` in a `--platform
linux/amd64 --cpus 1 --memory 1g` container. The entry's stage and deploy
nonce come from the service's input (`COMPOSER_INPUT`), as Composer's
runtime reads it: the document the release's `app.ts` binding makes, with
`CELL_STAGE` scratch (triage B2's stages are scratch, cert and production,
each its cells file's name), `CELL_DEPLOY_NONCE` the release's tag and a
`$secret` pointer to each of the cell's seven platform secret names, whose
values come from an env file. A release whose `app.ts` binds any other
secret or setting is refused before anything is staged. The staged
`release.json` is the release's, byte for byte (a rehearsal's names its
`--binary`); the entry reads its `server_sha256` and no cells file's sha
(the lead's decision deploy-q5 #6 (3): `release.ts`'s `checkRelease`,
which `deploy-release.sh` runs, holds a cells file to `release.json`).
`deploy/composer/streams/entry.e2e.test.ts` runs the bundled entry from
the rig's staging of a release that `release.ts` staged, certifying and
rehearsal, through every check before the feeds download. s3lite runs on
the host at 20 ms (the cell reaches it as `host.docker.internal`),
`tenants-admit.ts` admits
the manifest and publishes the bundle to s3lite's feeds bucket, and
`scrape.py loop --operator` samples the cell every 10 s. Then: 15 min idle
(the Q0 baseline), the tenants run, usage, the audit, 15 min idle (the Q0
bar), and `shared_verdict.py layer-b`, whose exit code is the rig's.
Certification takes only the release's binary (its sha256 is
`release.json`'s); a `--binary` rehearsal is recorded as one and can never
certify. The harness that runs is the checkout's: `checkout.json` (in
`rig.json`) records its trees and uncommitted paths, and the verdict's
`harness` leg requires the server commit's trees, clean. Results under
`$K2_HOME/results/layer-b-<stamp>/`; the secrets
directory is removed at exit. On an x86_64 Linux host s3lite is the
release's `bin/s3lite`; elsewhere this checkout's `target/release/s3lite`
(or `--s3lite PATH`). `--canary` needs the solo LiveFeed canary (triage
B9) and refuses until it exists.


### Layer B on a native x86_64 runner (`.github/workflows/layer-b.yml`)

```bash
gh workflow run layer-b.yml -f ref=<sha>                               # Layer B, about 3 h
gh workflow run layer-b.yml -f ref=<sha> -f manifest=layer-b-tiny.json # a ~20 min smoke of the rig, never evidence
```

On the owner's arm64 Mac the rig's container runs under Rosetta, which
inflates the cell's CPU about 2.5x (mem2, 2026-10-20). This manually
dispatched job (the lead's decision of 2026-10-20 13:00) runs the same rig
on one of GitHub's ubuntu-24.04 x86_64 runners (4 vCPU): it builds the
commit as a cut does (`scripts/cut-release.sh --local`: the same `cargo
zigbuild` x86_64-musl binaries, frozen install and bundled entry, but no
CI verdict, no upload and no `binaries.json`, with a `NOT-A-RELEASE` file
that `deploy-release.sh` refuses), widens the host's ephemeral ports and
raises `ulimit -n`, runs `run-shared.sh` with the committed `layer-b.json`
at time scale 1 (s3lite on the docker bridge's gateway), and prints only
`layer_b_summary.py`'s lines: each bar with its numbers, the RSS line
(peak, the plateau over the load's last 30 min, RSS and all admission
sheds, the container's cgroup peak, boots), the driver pools and the
verdict. Nothing is uploaded: the manifest, the scrape and the logs stay on
the runner (the owner's raw-evidence hold). It proves Layer B's legs on
native CPU for that commit; it is not a certification arm (the release is
a local staging, which `rc-certify.sh` never reads) and not a gate (it runs
only when dispatched; `ci-status.sh` and `required-checks.sh` read push
runs of other workflows). The driver shares the runner's 4 vCPU with the
cell's 1, so read the driver-pools fact and `harness_bound` before reading
a victim or probes FAIL as the cell's.
