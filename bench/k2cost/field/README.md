# K2 cost field runs (Prisma Compute + Tigris, fra)

The field half of the K2 cost experiment (`target/k2-design.md` §9.2):
cells on Prisma Compute in `eu-central-1` against Prisma Buckets (Tigris
Standard), in the release posture of design §6, observed from outside so
that compute (memory GiB-hours, CPU-seconds), requests and stored bytes can
be priced. The local harness (`k2gen.ts`, `scrape.py`, `price.py`) lives in
`bench/k2cost/`; this directory deploys k2gen in-region and adds the
field-only instruments.

## Rules every tool here enforces

- **Secrets** come only from files under `~/.streams-k2/field`
  (`$K2_FIELD_HOME`): `platform-token.txt`, `binid.txt`, `binsec.txt`,
  `artifact-endpoint.txt`, `artifact-bucket.txt`, and per cell
  `runs/<run>/<cell>/secrets/` (0700; bucket key, deployment bearer, fleet
  token, usage and cursor keys, feed signing key, data key, generator
  tokens). Nothing secret is printed, put on a command line or written to
  the repo: platform API calls run in-process (`User-Agent: curl/8.7.1`,
  or Cloudflare answers 1010), the compute CLI gets the token through its
  environment, and deploy env values travel in a 0600 env file deleted
  after the call. Stored environments are redacted (`fieldlib.redact`).
- **One compute-cli call at a time** (parallel `bunx` races its package
  cache): every `bunx` subprocess of every field tool, and every `bun
  install` of a staged app, holds `$K2_FIELD_HOME/.cli.lock`, so a second
  tool (a backgrounded gen.sh, teardown.sh, observe.py's log reads) blocks
  instead of racing.
- **Names and the ledger.** Everything a run creates is named
  `k2c-<run>-<cell>[-s<i>|-r<j>|-g<k>]` and written to
  `runs/<run>/resources.json` BEFORE its platform call (projects, buckets
  and services as `pending`; a POST is never retried, and a pending entry
  is resolved by exact name). `teardown.sh` acts only on that ledger and
  only after the live object agrees: the live name must equal the ledger's
  and start `k2c-<run>-`, a bucket's live project must be the ledger's; it
  refuses the artifact project and bucket (by id, from
  `artifact-platform-receipt.json`), any id another run's ledger lists, any
  service in a run project that the ledger does not name, and a project
  whose services it cannot list.
- **KEEP_AWAKE bills continuously, so it is bounded twice.** A cell or
  generator that sets it gets an expiry in `resources.json`
  (`KEEP_AWAKE_TTL_MIN` in the cell file, `GEN_TTL_MIN` for a generator)
  that also travels to the instance as `KEEP_AWAKE_UNTIL_MS`: the wrapper's
  guard is `new KeepAwakeGuard({ signal: AbortSignal.timeout(...) })`, so
  it releases itself at the expiry even if the operator and the laptop are
  gone, and an idle instance then sleeps. A finished generator releases its
  guard 2 min after its plan ends. And observe.py destroys every service
  past its expiry (`teardown.sh <run> --expired --yes`, every minute).
  Smoke cells come down within 30 minutes.
- **Region and workspace.** fra (`eu-central-1`) only, in the K2 workspace
  (the token's workspace holds only the artifact project and bucket, which
  are never modified or deleted; the run's feeds and generator header files
  under `k2c/<run>/` in the artifact bucket are deleted at teardown, the
  binaries under `bin/` stay). Never go near the Tigris observatory (another
  workspace; RUNBOOK §14).
- **Results** go to `~/.streams-k2/field/results/<run>/`, never into the
  repo, and are never uploaded anywhere.

## Files

| File | Role |
|---|---|
| `fieldlib.py` | shared: paths, secrets, platform API, compute CLI, `resources.json` (locked, atomic), S3 clients, verified PUT, log reader |
| `bins.py` | uploads `streams-slate` and `pilot` from `~/.streams-k2/bin/<TAG>/` (`--tag` or `K2_BIN_TAG`) (sha256 checked against `SHA256SUMS`, ELF byte 18 = `0x3e`) and compiles + uploads k2gen; ranged-GET verified; manifest `bins.json` |
| `provision.py <run> <cell>...` | per cell: project `k2c-<run>-<cell>` in eu-central-1 (`createDatabase: false`: the API otherwise adds a Prisma Postgres to every project), bucket of the same name, read_write key |
| `deploy-cell.sh <run> <cell>` (`deploy.py`) | servers, ring gate, routers; env check; instance shapes |
| `gen.sh <run> <cell> <plan>` (`gen.py`) | the in-region k2gen generator with a phase plan |
| `observe.py <run> [cells]` | bucket census, per-beat heartbeat integration (awake s, CPU s), debug scrapes with the window ring (SCRAPE=1 or `--scrape-cell`; live servers only), generator windows, wrapper CPU logs, KEEP_AWAKE expiry teardown |
| `price_field.py <run> <cell>` | prices a cell from observe.jsonl: stitched request totals (the local harness's `stitched_delta`), 404 split, loss bound, egress range, compute, per-invocation tiers and §8 gates |
| `teardown.sh <run> [--yes] [cell...]` (`teardown.py`) | guarded, step-isolated teardown and verification; `--expired`, `--service <name>` |
| `selftest.py` | offline checks (stubs, no network): teardown guards and isolation, POST no-retry, CLI lock, CPU inversion, router spreading |
| `feeds.mjs` | the cell's auth feeds bundle and customer JWTs (as `bench/soak/mtgen.mjs`) |
| `cells/*.env` | example cell files for the design's cells (copy to `~/.streams-k2/field/cells/`) |
| `plans/*.plan` | generator phase plans |

### Changes outside this directory

- `deploy/app-gen/plan.ts` (new) and a hook in `deploy/app-gen/index.ts`:
  app-gen hands its binary no arguments, and k2gen takes its configuration
  only as arguments, so `GEN_PLAN_JSON` makes the wrapper run a phase plan
  (stages in order, a stage's invocations concurrently; `@HEADERS` is the
  file `TOKENS_S3_KEY` was downloaded to; `["idle","SECS"]` waits), give
  each invocation its own `--out`/`--ledger`, serve `GET /` (plan state),
  `GET /windows?since=N` (window lines, tagged `inv`) and `GET /ledgers` on
  `$PORT`, and hold when the plan ends, as every generator does. Without
  `GEN_PLAN_JSON` app-gen is unchanged.
- One `instance shape:` boot line in `deploy/app-{server,lb,gen}/index.ts`
  (kernel memory, cgroup limits, CPU count, libc). No platform API reports
  an instance's memory size (the management API's service and deployment
  objects carry none), and compute is priced by it.
- In the same three wrappers: `KEEP_AWAKE_UNTIL_MS` bounds the keep-awake
  guard (unset: the guard holds for the process's life, as every other
  campaign expects); `CPU_LOG_SECS` logs `cpu sample:` lines (cumulative
  user+sys of every process in the instance's pid namespace, reaped
  children included, and the kernel's busy time; unset: nothing). app-lb
  now logs `binary sha256` like app-server and app-gen. plan.ts releases
  the guard 2 min after a plan ends and never starts a plan after
  `GEN_START_BY_MS`.
- `bun test ./deploy/supervise.test.ts ./deploy/stage-app.test.ts` passes
  (22 tests).

## Cells

A cell file (`~/.streams-k2/field/cells/<cell>.env`, no secrets):

| Key | Meaning |
|---|---|
| `SERVERS` | ordinal servers `streams-1..N`, every one deployed and in `UPSTREAMS`, always fleet mode with `FLEET_MAX=N`: the scaler can grow the ring to every deployed ordinal (the pilot routes to the first `desired` upstreams and wakes a desired one that sleeps) and no further. Fleet mode needs `FLEET_MAX > 1` (`src/config/validation.rs` `fleet_mode`), so a T-single cell (N=1) gets `FLEET_MAX=2` with no second ordinal and is labelled `capped` in deploy.json; its heartbeats carry `cpu_pct` |
| `FLEET_MIN` | the ring's floor, default `min(2,N)`; ordinals above it are spares that sleep until wanted |
| `ROUTERS` | pilot load balancers `router-1..M` (`PILOT_MODE=lb`, `UPSTREAMS` = the servers in ordinal order) |
| `KEEP_AWAKE` / `KEEP_AWAKE_TTL_MIN` | `1` on servers `1..FLEET_MIN` and every router (never a spare), with its expiry, enforced in-instance |
| `SCRAPE` | `1`: observe.py scrapes `/v1/debug/*` on the live servers; `0`: nothing is ever sent to the cell's servers or routers (unless `observe.py --scrape-cell`, which F2 uses on f1b) |
| `WAL_POSTURE` | `P-exp` (binary default gap 10, gather 6), `P-500`, `P-1000` (`WAL_FLUSH_GAP_MS`) |
| `INITIAL_SHARDS` | default 4 (D2) |
| `SERVER_ENV_EXTRA` | space-separated `KEY=VALUE` overrides, `-KEY` removes |

| Cell | Topology (ordinals, MIN-MAX + routers) | KEEP_AWAKE | SCRAPE | Generator plan |
|---|---|---|---|---|
| `f1a` | T-launch 4, 2-4 + 2 | 1 (25 h; ordinals 1-2, routers) | 1 | none |
| `f1b` | T-launch 4, 2-4 + 2 | – | 0 (F2: `--scrape-cell f1b`) | none (F2 later: `f2-half-day`) |
| `f1c` | T-staging 6, 2-6 + 2 (STAGING §2) | – | 0 | none |
| `f3-exp`, `f3-500`, `f3-1000` | T-single 1, capped | 1 (150 min) | 1 | `f3-b64rand` |
| `f3-corpus` | T-single 1, capped | 1 (150 min) | 1 | `f3-corpus` |
| `f5` | T-launch 4, 2-4 + 2 | 1 (270 min; ordinals 1-2, routers) | 1 | `f5` (both routers) |
| `smoke-single`, `smoke-launch`, `smoke-wake` | T-single; T-launch; T-single | 1 (30 min); –; – (generator `GEN_TTL_MIN`) | 1; 0; 1 | `smoke-produce`; none; `smoke-short` |

## The server environment (design §6)

Every server gets, in this order (later wins): `deploy/profiles/compute-1g.env`
verbatim; the §6 engine knobs (`WAL_GROUP_COMMIT=1 FLUSH_INTERVAL_MS=25
WAL_POST_ACK_GATHER_MS=6 FRAME_COMPRESS=1 ABSORB_BYTES=4194304
ABSORB_AGE_SECS=60 TRIM_PER_OP=65536 TRIM_GLOBAL_BUDGET=65536
ADMIT_MAX_INFLIGHT=512 ADMIT_MAX_INFLIGHT_PER_STREAM=256 TAIL_RING_BYTES=0`);
production limits (`LIMIT_BYTES_PER_SEC=5000000 LIMIT_REQS_PER_SEC=1000
LIMIT_RECS_PER_SEC=5000 LIMIT_BURST_SECS=2`); STAGING §4's production
scale values, stated explicitly; the WAL posture; binary poll defaults (no
`MANIFEST_POLL_MS`/`COMPACTOR_POLL_MS`); topology (`PATH_PREFIX=k2d`,
`FLEET_PREFIX=k2f`, `INSTANCE_NAME=streams-<i>`, `FLEET_MIN`, `FLEET_MAX`,
`INITIAL_SHARDS`, `FLEET_PEER_DOMAINS=prisma.build`); the release posture:
`STREAMS_AUTH_MODE=enforce` with the cell's feeds (`FEEDS_S3_KEY`,
materialized by app-server into `/tmp/feeds/{keys,policies,grants}.json`,
refresh 60 s), `BILLING_MODE=required`, `USAGE_STREAM_KEY`, real-shaped
`ACCOUNT_ID`/`PROJECT_ID`/`CELL_ID` (`acct_`/`proj_`/`cell_` + 24 chars,
minted once per cell), `REGION=eu-central-1`, `ROLLUP=1` on `streams-1`,
`STREAMS_CURSOR_KEY`, a deployment bearer `AUTH_TOKEN` (it gates every
`/v1/debug` route, `src/http/debug.rs`) and `FLEET_AUTH_MODE=static` with
`FLEET_INTERNAL_TOKEN`; the wrapper's `SERVER_BINARY_S3_KEY`, `BIN_S3_*`,
`RESOLV_OVERRIDE`, `KEEP_AWAKE`; the data bucket `SLATE_S3_*`. Routers get
`LB_BINARY_S3_KEY`, `BIN_S3_*`, `S3_*` (the data bucket, pilot's names),
`FLEET_PREFIX`, `DATA_PREFIX`, `PILOT_MODE=lb`, `ROUTER_NAME`, `UPSTREAMS`,
`RESOLV_OVERRIDE`, `KEEP_AWAKE`.

`deploy.py` refuses an env name that no process of the role reads: the
binary's names come from `src/config/*.rs`, the pilot's from
`src/bin/pilot{.rs,/}`, the wrappers' from `deploy/app-*/*.ts`. Compute
env is project-scoped and merged (RUNBOOK §7.3), so every deploy restates
the service's whole env and unsets every other project variable.

Deviations from the letter of §6, all deliberate:

- **Static fleet auth, no `STREAMS_RELEASE_POSTURE=1`.** The release
  posture demands `FLEET_AUTH_MODE=workload`, whose workload JWT lives at
  most 24 h and must be rotated by the platform; nothing on Compute rotates
  it for us and F1/F2 run 24-48 h. §6 accepts static fleet auth ("it costs
  the same"). Customer traffic is fully in enforce mode.
- **Deployment and customer projects differ** (`PROJECT_ID` vs the feeds'
  customer project), as in `run-local.sh` and `mt-tenants.sh`.
- **T-single has `FLEET_MAX=2`**, not 1: with 1 fleet mode is off and the
  heartbeats are not published. There is no second ordinal, so the cell is
  `capped`: an envelope cell never scales out.

## Run book

One-time per machine and per k2gen revision:

```bash
F=bench/k2cost/field
cp $F/cells/*.env ~/.streams-k2/field/cells/
python3 $F/bins.py --tag <short commit>  # binaries from ~/.streams-k2/bin/<tag>/; glibc k2gen: Compute's image is glibc, the musl build cannot exec there
python3 $F/selftest.py                 # offline: 26 checks, no network
```

Per run (`RUN` = `[a-z0-9]{1,12}`; one cell's commands strictly in order;
the CLI lock serialises deploys anyway). The observer always starts right
after deploy-cell.sh and BEFORE gen.sh: a generator starts its plan at
boot, and every plan opens with `idle 300` so the observer has a pre-load
baseline and every tier boundary falls between 20 s scrapes.

```bash
# F1 (24 h floor, three cells in parallel once deployed)
python3 $F/provision.py $RUN f1a f1b f1c
$F/deploy-cell.sh $RUN f1a; $F/deploy-cell.sh $RUN f1b; $F/deploy-cell.sh $RUN f1c
python3 $F/observe.py $RUN f1a f1b f1c          # background; 24 h; restartable
$F/teardown.sh $RUN f1a f1c                      # dry run, then --yes; f1b continues as F2

# F2 (24 h loaded day on f1b, two 12 h generator halves)
#   stop the F1 observer (SIGTERM), then restart it for f1b WITH scraping
#   (it scrapes only live servers, so it never wakes a sleeping one) and
#   wait for its first `scrape` line before the generator boots:
python3 $F/observe.py $RUN f1b --scrape-cell f1b --census-secs 120   # background
GEN_TTL_MIN=750 $F/gen.sh $RUN f1b f2-half-day   # +0 h; alternates over both routers' service URLs
#   +6 h rolling deploy, server by server (routers adopt urls.json; the
#        generator targets service URLs, so it keeps its base):
for i in 1 2 3 4; do $F/deploy-cell.sh $RUN f1b --only $i; done
#        then check the generator windows stayed 2xx across the roll
#        (price_field.py: non_2xx per invocation).
#   +12 h instance kill, revived at once (its cost goes to the floor):
$F/deploy-cell.sh $RUN f1b --kill 2 && $F/deploy-cell.sh $RUN f1b --only 2   # records gap and revived boot
GEN_TTL_MIN=750 $F/gen.sh $RUN f1b f2-half-day --replace   # destroys g1 (teardown.py --service), deploys g2
python3 $F/price_field.py $RUN f1b
$F/teardown.sh $RUN --yes

# F3 (four T-single envelope cells, ~1.5 h each, in parallel once deployed)
python3 $F/provision.py $RUN f3-exp f3-500 f3-1000 f3-corpus
for c in f3-exp f3-500 f3-1000 f3-corpus; do $F/deploy-cell.sh $RUN $c; done
python3 $F/observe.py $RUN --census-secs 120     # background, BEFORE the generators
for c in f3-exp f3-500 f3-1000; do $F/gen.sh $RUN $c f3-b64rand; done
$F/gen.sh $RUN f3-corpus f3-corpus
for c in f3-exp f3-500 f3-1000 f3-corpus; do python3 $F/price_field.py $RUN $c; done
$F/teardown.sh $RUN --yes

# F5 (T-launch consumption cell, ~3.5 h)
python3 $F/provision.py $RUN f5 && $F/deploy-cell.sh $RUN f5
python3 $F/observe.py $RUN f5 --census-secs 120  # background, BEFORE the generator
$F/gen.sh $RUN f5 f5                             # invocations alternate over both routers; subs split
python3 $F/price_field.py $RUN f5
$F/teardown.sh $RUN --yes
```

`observe.py` restarts cleanly (its integrals are saved in
`observe-state.json` every 5 s and restored on start), so a laptop sleep
loses only resolution (missed beats are interpolated). Its defaults are the
design's field cadences: heartbeats every 1.5 s (every 2 s beat, on their own
thread), scrapes every
20 s, census every 300 s (pass `--census-secs 120` under load). A
generator's KEEP_AWAKE expiry is the plan's length plus 15 min unless
`GEN_TTL_MIN` says otherwise; the plan never starts later than that expiry
less its length.

## Outputs (`~/.streams-k2/field/results/<run>/`)

| File | Content |
|---|---|
| `<cell>/deploy.json` | per instance: redacted env, the platform's env-name list for its version, deploy time, health/stats gate, `instance shape`, `memory_gib` (kernel) and `memory_gib_class` (priced), binary sha256 from the wrapper log against bins.json (`binary_ok`; a mismatch fails the deploy); topology (`capped`); the ring gate; the cell file and identities |
| `<cell>/deploy-only-s<N>-<utc>.json`, `kill-s<N>-<utc>.json` | a single-ordinal redeploy (revive or roll step): revived boot, gap since the kill; a kill's version and time |
| `<cell>/gen-g<k>.json` | the generator's plan, each invocation's target router, redacted env, shape, binary identity, early failures |
| `<cell>/observe.jsonl` | stamped lines: `census` (bytes/objects per tier and kind, LIST pages, daily peak), `heartbeat` / `router` (every 60 s and on events: seq, ts, boot, cpu_pct, rss, owned shards, awake and CPU totals, bucket-clock age), `wake` (the first beat after a sleep, its reading both ways), `desired`, `scrape` (each GET stamped: store `totals` plus the `?window` ring's n/err cells, `load`, `usage` aggregates), `identity` (a git_commit or binary mismatch: the cell is void), `gen` (plan state, invocation times, lines fetched, keep-awake state), `wrapper_cpu`, `expiry_teardown`, `error`, `start`/`stop` |
| `<cell>/observe-summary.json` | running totals: per instance awake s, CPU s (exact and interpolated beats, first-read credit, wake extra), wrapper CPU per boot, sleeps, boots, awake intervals, memory class and the D1-priced memory/CPU dollars; census peaks; the harness's own requests and their Tigris price; `void` |
| `<cell>/price-field.json` | price_field.py: requests (stitched, resets, loss bound, Class A/B headline/low/high, billed-404 estimate), egress range, compute per instance, per-invocation tiers with §8 gate deltas |
| `<cell>/observe-state.json` | the integrals, for a restart |
| `<cell>/gen-g<k>-windows.jsonl` | every k2gen window line (10 s), tagged with its invocation |
| `<cell>/gen-g<k>-ledgers.json` | every finished invocation's exact ledger |
| `teardown.json` | what was deleted, every failed step, refusals and the 404 verification (written even on a crash) |
| `teardown-partial.jsonl` | one line per `--expired` / `--service` action |

`runs/<run>/resources.json` (not a result) is the ledger of platform
resources, KEEP_AWAKE expiries and their teardown status.

## Deploy sequence and gates

`deploy-cell.sh` checks every env name, stages (under the CLI lock) `deploy/app-server` and
`deploy/app-lb` (`bench/stage-app.sh`), mints the cell's identities, keys
and feeds, then deploys each server and gates it on `GET /health` before
the next, publishes `<FLEET_PREFIX>/fleet/urls.json`, and waits for the
ring in the bucket: every server has beaten since its deploy, and the
desired count and the servers' boot ids hold for 60 s; with KEEP_AWAKE the
ring itself (the first `desired` beating ordinals) must hold too. Without
KEEP_AWAKE an idle server sleeps within seconds, so liveness cannot hold
still; a server never seen beating gets the routers' wake ping (`/health`).
Routers are then deployed and gated on `GET /stats` (the pilot answers it
itself; its `/health` would be proxied to a server). Each instance's
memory comes from its `instance shape:` boot line, read with `compute logs`
(killed after at most 45 s). One service deploy takes about 105-150 s
(the CLI writes each env variable to the project one API call at a time),
so a T-launch cell (4 ordinals + 2 routers) takes about 13-15 minutes and a
T-staging cell (6 + 2) about 18-20. Every instance's `binary sha256` line
must match bins.json, or the deploy fails after writing deploy.json.

## Smoke results (2026-10-02, fra)

- **T-single + generator** (run `sm1002b`, created 09:03:15Z, torn down
  09:16:16Z): the generator ran k2gen in enforce mode with the cell's
  customer JWT (8 streams created, 201; 1,774 appends acked in 180 s,
  9.86 req/s, all 200, p50 49.7 ms, p99 229 ms). Census 45,172 B / 115
  objects before load, 5,438,381 B / 1,304 objects (889 WAL) during it,
  9,360,392 B / 956 objects after. `totals` `put:wal` ok 32 before the
  load, 1,975 after it (1,943 WAL PUTs for 1,774 appends at 10 req/s on
  4 shards); 22 to 32 over the idle first two minutes. Heartbeats: one
  beat every 2.0 s, 424 awake s integrated over 7 reads with no gap, 6.91
  CPU-s (cpu_pct 0.8-0.9 % idle, 2.4 % under load). Memory: the kernel
  sees 1,025,904,640 B (0.955 GiB, no cgroup limit), priced as the 1 GiB
  class; 1 CPU; glibc.
- **T-launch, no KEEP_AWAKE, bucket-only** (run `sm1002d`, created
  09:28:51Z, torn down 09:53:32Z): both routers came up (`/stats` lists 2
  upstreams) and published reports; the servers beat and formed the ring
  (desired 2). Every instance went to sleep within seconds of its last
  inbound request: router-1 ~3 s after its gate, router-2 ~3 s, the servers
  ~3 s after router-2's last wake ping (last beats 09:37:46-09:37:49). For
  the whole 15 min window (09:38-09:53) nothing beat or reported: 0 awake
  s, 0 CPU-s on all four instances, and the bucket did not change (42,643 B,
  131 objects at every census). An idle T-launch cell without KEEP_AWAKE
  costs no instance time at all; its floor is storage plus whatever wakes it.

- **T-single re-smoke after the review fixes** (runs `sm1002e`, created
  10:43:13Z, torn down 10:55:29Z, 12 min 16 s; `sm1002f`, 10:59:12Z to
  11:06:05Z, 6 min 53 s; cell `smoke-wake`: one server without
  KEEP_AWAKE, scraped while live). Deploy: server 97 s and 124 s, ring
  gate 77 s; `binary_ok` (sha256 08ef7ede... = bins.json) for the server
  and the generator (49799ffb...); topology `capped` (FLEET_MAX 2). The
  observer started before gen.sh; the generator (`smoke-short`, GEN_TTL_MIN
  8) targeted streams-1's SERVICE URL, ran idle 20 s, setup, 30 s at
  10 req/s (321 acked, 0 non-2xx), ended 10:49:47Z, logged `plan:
  keep-awake guard released (plan ended)` at 10:51:47Z, after which its
  CPU lines (then every 60 s) stop (it slept); at its expiry (10:54:34Z) observe.py ran
  `teardown.py --expired` and destroyed it at 10:54:49Z. The server slept
  4 s after its health gate and woke on the generator's first request
  (warm: same boot_id, seq 2 -> 3, 252 s frozen, cpu_pct 4.0 -> 4.1); a
  single `GET /health` after 66 s asleep woke it warm again (seq 23 -> 24,
  1.3 -> 1.8), and `totals.since_ms` stayed 1790937897547 through both.
  It was scraped only while live (3 scrapes); the window ring split the
  get/head `unbilled` 608 into an estimated 555 billed 404s and 53 free,
  so Class B is 701 against 146 counting ok alone. Heartbeat CPU 1.26 s,
  wrapper CPU 1.73 s (all processes; kernel busy 1.87 s). sm1002f checked
  the two observer changes the first smoke led to: one baseline scrape of
  the sleeping server at start (`was_live: false`, a warm wake, then no
  scrape until it was live again) and the per-cell heartbeat thread (6
  exact beats, 0 interpolated, against 9 of 27 interpolated in sm1002e
  when slow scrape GETs blocked the loop). Both teardowns VERIFIED (404s,
  no k2c- project, bucket or database left, the run's k2c/ artifact
  objects absent).

## Known limits

- **A scrape can extend a wake**: a server is scraped only while its
  heartbeat is live, but that request keeps it awake for the platform's
  idle timeout (~3 s) once per wake at most.

- **CPU of a server** comes from its heartbeats read every 1.5 s (each beat):
  the smoothing is inverted exactly when consecutive beats are read and
  interpolated over missed ones (`beats_interp`). A first read and a new
  boot are credited from `seq` with the beat's smoothed value
  (`cpu_approx_s`). The first beat after a sleep does NOT span the freeze:
  in smoke sm1002e two wakes (252 s and 66 s asleep, the second a single
  deliberate `GET /health`) raised the smoothed cpu_pct (4.0 -> 4.1, 1.3 ->
  1.8), so the reading covered about one 2 s period (heartbeat.rs divides
  by `Instant` time, and the monotonic clock stops while frozen; the wall
  clock jumps). `cpu_wake_extra_s` (the reading times the frozen span) is
  therefore an upper bound that is not real; the headline uses one period.
- **CPU of a router** exists only as the wrapper's `cpu sample:` log lines,
  read with `compute logs --tail 300` (every 30 min for awake instances,
  and for all at stop and before an expiry teardown), one line every 15
  awake seconds: a boot loses the CPU after its last line, and a boot
  whose lines left the platform's log buffer before a read is lost
  (`wrapper_boots` vs the heartbeat `boots`). Router awake time is the span of its reports
  (every ~2 s), so a laptop stall longer than 6 s looks like a sleep.
- **Customer tokens live at most 24 h** (`MAX_TOKEN_LIFETIME_SECS`): a
  longer plan needs a new generator deployment (`gen.sh ... --replace`).
- **The pilot routes `/health` to a server**, so routers are gated on
  `/stats` and never probed by observe.py.
- **A wake was warm in both smoke sm1002e wakes** (same `boot_id`, `seq`
  continuing, totals not reset), so a sleep/wake costs no shard reopen
  there; observe.py still counts `boots` per instance and the pricer
  stitches any reset.
- **No platform usage export** was found in the management API, so awake
  time is the heartbeat integral (or, for KEEP_AWAKE, deploy to teardown),
  not the platform's billing record.
- **Revenue meters** (`GET /v1/projects/{project}/usage` on the `ROLLUP=1`
  instance after the drain) are not read by these tools yet: use
  `bench/k2cost/scrape.py usage` with the cell's generator header file
  (`runs/<run>/<cell>/secrets/gen-<k>-headers.txt`) before teardown.
- **Billed 404s are estimated**, not counted: `totals` files 404 under
  `unbilled` with the free 304s; the `?window=20` ring's `err` (NotFound
  plus Failed) gives the split per op and class (price_field.py's
  `billed_404_est`). C1's per-status attempts (design §7) would count them.
  A logical multipart upload is priced as `--mpu-requests` (3) Class A
  requests until L0 calibrates its parts.
- **Egress to Tigris is a range**: `bytes_put` counts PUT payloads only;
  the high end adds `--req-overhead-bytes` (700 B, uncalibrated) per
  request for the request line and SigV4 headers. A SCRAPE=0 cell has no
  request or egress measure at all (only the cadence model).
