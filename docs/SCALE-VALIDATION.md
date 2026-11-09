# Scale validation on Prisma Compute: streams and queues, cost and revenue

Status: guide, written 2026-11-01 against slate `3a86e7e52`; its open items
were decided on 2026-11-03 (§8). It tells an agent that already runs Streams
on Prisma Compute, as a multi-instance fleet against an S3-compatible store
(s3lite or Tigris), how to check at scale what the local rigs measured: what
streams and queues cost us per tenant and per message, and what they earn at
Cloudflare's public prices. Follow it literally. It runs nothing by itself.

Every figure has a source. Repository paths are cited as they are. A figure
that exists only in the program's working notes outside the tree is marked
"(measured 2026-10-xx, program notes)". A question the sources do not answer
is marked **TODO (owner)**; none is open now. Acceptance of performance,
cost and deployment stays with the owner (AGENTS.md §1, standing holds):
this guide sets the bars (§6); the owner accepts the result.

Terms used below:

- **shard**: one SlateDB log with its own WAL writer (RUNBOOK §3.2).
- **cell**: the launch shape, one 1 GiB server, one shard, fleet off
  (`deploy/profiles/shared-cell.env` on `deploy/profiles/compute-1g.env`).
- **fleet**: N servers with `FLEET_PREFIX` set, shards placed by the
  rendezvous ring (RUNBOOK §6).
- **ab50**: the owner's prices with Tigris Class A and B halved (below, §4.1).
- **window month**: a measured window scaled to a 30-day month
  (`bench/queues-cost/prices.mjs` `MONTH_S`; `bench/k2cost/packing.py`).

## 1. Deployment posture for the test

### 1.1 The owner's posture, and why

| Rule | Value | Why |
|---|---|---|
| Shards per instance | 1 or 2 | every busy shard pays its own WAL PUTs, about 10 a second, whatever it carries (§4.3) |
| WAL flush gap | 100 ms, the binary default (`WAL_FLUSH_GAP_MS`, `src/config/cli.rs`) | it caps a busy shard at 1000 / 101 ≈ 9.9 WAL objects a second (RUNBOOK §3.2) |
| WAL PUTs in flight per shard | 1 | the group-commit pump flushes one WAL at a time; there are no pipelined WAL writes (`src/shard.rs`, the pump) |

The first two rules exist to bound Tigris operation cost. A previous run of
this bench set `FLEET_MAX=64` (so the fleet default gave 64 shards on one
instance) and `WAL_FLUSH_GAP_MS=25`. §4.3 shows what that costs. Results
from that configuration are void for cost.

### 1.2 Binary

- x86_64 musl only. Compute runs x86_64 microVMs; an aarch64 binary deploys,
  then crash-loops with `ENOEXEC` as a silent zombie (RUNBOOK §2).

```bash
cargo zigbuild --release --target x86_64-unknown-linux-musl --bin streams-slate --bin pilot
xxd -s 18 -l 2 target/x86_64-unknown-linux-musl/release/streams-slate   # must print 3e00
sha256sum target/x86_64-unknown-linux-musl/release/streams-slate
```

- Build from an exact, pushed slate commit. Record the sha. The server
  reports it as `git_commit` on `/v1/debug/load` (`build.rs` embeds HEAD).
- Fleet stream tests need request forwarding (edge change #235, §1.6).
  Check that the commit has it before a fleet run:

```bash
git cat-file -e <commit>:src/http/forward.rs && echo forwarding-present
```

### 1.3 Topology: instances, shards, prefixes

| Instances N | Fleet | `INITIAL_SHARDS` | `FLEET_MAX` | `FLEET_MIN` |
|---:|---|---|---:|---:|
| 1 | off: do not set `FLEET_PREFIX` | 1 or 2 | not set | not set |
| 8 | on | 8 or 16 | 8 | 8 |
| 16 | on | 16 or 32 | 16 | 16 |

- `INITIAL_SHARDS` must be a power of two (`src/config/validation.rs`). It is
  read only when the keyspace is created; the topology is stored with it
  (RUNBOOK §3.2). **Use a fresh `PATH_PREFIX` and a fresh `FLEET_PREFIX`
  for every configuration; on Tigris, a fresh bucket used by nothing else
  (§4.2).** Reusing a prefix keeps the old shard count whatever the
  variable says.
- `INITIAL_SHARDS = 2N` logs `INITIAL_SHARDS=2N > FLEET_MAX=N: ...` at boot
  (`src/config/notice.rs`). That warning is expected here.
- `FLEET_MIN = FLEET_MAX` holds the ring at N: the desired count is floored
  at `FLEET_MIN` and clamped to `FLEET_MAX` (`src/fleet.rs`, RUNBOOK §3.5),
  so no autoscaling dimension can grow or shrink it during a run.
- Fleet mode needs `FLEET_MAX > 1` (`src/config/validation.rs`
  `fleet_mode`). For N = 1 run fleet off, the launch shape.
- Rendezvous placement is uneven: 4 shards over 4 servers draws
  `[1, 1, 0, 2]` (RUNBOOK §3.2). The count balancer (edge change #236) moves
  shards until every member holds at most `ceil(shards / members)`. Record
  each instance's shard count anyway (§1.7): the per-shard cost model holds
  either way, but per-instance CPU and memory divide evenly only once it has
  converged.

### 1.4 Environment of every server

Restate the complete environment on every deploy; Compute merges env vars
project-wide (RUNBOOK §7.3). Never put a secret on a command line: read
each from a file (AGENTS.md §1; `bench/k2cost/field/README.md`, "Rules").

| Group | Variables | Source |
|---|---|---|
| Wrapper | `SERVER_BINARY_S3_KEY`, `BIN_S3_ENDPOINT`, `BIN_S3_BUCKET`, `BIN_S3_REGION=auto`, `BIN_S3_ACCESS_KEY_ID`, `BIN_S3_SECRET_ACCESS_KEY`, `RESOLV_OVERRIDE` | `bench/fleet/deploy-fleet.sh` |
| Keep-awake | `KEEP_AWAKE=1` with `KEEP_AWAKE_UNTIL_MS=<run end + 15 min>` | bills every hour; the expiry releases it (`bench/k2cost/field/README.md`) |
| CPU samples | `CPU_LOG_SECS=15`, on queue instances too | the wrapper logs `cpu sample:` lines (`bench/k2cost/field/README.md`, "Changes outside this directory"); the queue rig's field mode reads them (§3.2) |
| Store | `SLATE_S3_ENDPOINT`, `SLATE_S3_BUCKET`, `SLATE_S3_REGION` (`auto` for Tigris), `SLATE_S3_ACCESS_KEY_ID`, `SLATE_S3_SECRET_ACCESS_KEY` | RUNBOOK §3.1 |
| Memory profile | every line of `deploy/profiles/compute-1g.env`, verbatim | the 1 GiB survival posture; deploy scripts pass each line as `--env` |
| Topology | `PATH_PREFIX`, `INITIAL_SHARDS`, `INSTANCE_NAME=streams-<i>`; fleet only: `FLEET_PREFIX`, `FLEET_MAX`, `FLEET_MIN`, `FLEET_PEER_DOMAINS=prisma.build` | §1.3 |
| Debug access | `AUTH_TOKEN` (the deployment bearer; it gates every `/v1/debug` route) | `bench/k2cost/README.md`, "Auth and debug access" |
| Billing | `BILLING_MODE=required`, `USAGE_STREAM_KEY`, `PROJECT_ID`, `CELL_ID`; `ROLLUP=1` on `streams-1` only | `bench/k2cost/field/README.md`, "The server environment" |
| Auth | whatever the fleet already runs (`STREAMS_AUTH_MODE=enforce` with feeds; fleet auth static or workload) | unchanged by this guide |

For the cell shape (§2.2, test ST-1) add every line of
`deploy/profiles/shared-cell.env` after `compute-1g.env` (later wins). It
sets one shard, fleet off, the 8 MiB tail ring and the cell's ceilings, and
it expects a cell-admin cell (RUNBOOK "Shared cells"). That profile keeps
the cell awake for the process's life and cell-admin refuses
`KEEP_AWAKE_UNTIL_MS` on it, so bound its cost by destroying the service at
the end of the run instead.

After every server redeploy in a fleet, republish
`<FLEET_PREFIX>/fleet/urls.json` (the `urls` step of
`bench/fleet/deploy-fleet.sh`): each Compute version mints a new URL, and
forwarding needs the owner's URL (§1.6).

Do not deploy `bench/fleet/deploy-fleet.sh` as it is: it pins
`INITIAL_SHARDS=16` on four servers, sets `WAL_FLUSH_GAP_MS` and
`FLUSH_INTERVAL_MS`, scaler test values (`SCALE_HOT_PCT=1`) and absorber
variables that layout 6 removed (RUNBOOK §3.2). Use it only as a reference
for the wrapper and store variables.

### 1.5 Settings that must not be present

A server whose environment holds any variable below with a value other than
the binary default voids every cost result of that run. Present with exactly
the default value: the run stays valid, report it as a deviation, and remove
it before the next run.

| Variable | Binary default | Why it voids |
|---|---|---|
| `WAL_FLUSH_GAP_MS` | 100 | sets the WAL PUT cap per shard (§4.3) |
| `FLUSH_INTERVAL_MS` | 25 | the gap when `WAL_FLUSH_GAP_MS=0`; the tick when the pump is off |
| `WAL_GROUP_COMMIT` | 1 | 0 replaces the pump with SlateDB's fixed tick |
| `WAL_POST_ACK_GATHER_MS` | 6 | changes how many requests share a WAL object |
| `MANIFEST_POLL_MS`, `COMPACTOR_POLL_MS` | 2000, 2500 | the idle-cost posture; "deploy scripts must not re-tighten" them (RUNBOOK §3.2) |
| `TOKIO_WORKERS` | one per core, at least 2 (`src/config/model.rs`) | CPU figures stop being Compute's |
| `FLEET_MAX` above N, `FLEET_MIN` below N | N | the ring can move; more shards by default |
| `INITIAL_SHARDS` not N or 2N | §1.3 | breaks the 1-2 shards per instance rule |
| `UNSAFE_LEGACY_MEMORY_PROFILE=1`, or any `compute-1g.env` line missing or changed | the profile | not the certified 1 GiB posture |
| `SCALE_*` test values, `ABSORB_*`, `TRIM_*` | unset | scaler pins or removed knobs; remove them |
| `STREAMS_DEBUG_TIMING`, `STREAMS_CERTIFICATION_MODE` | unset | diagnostic modes |

### 1.6 Forwarding in a fleet (edge change #235, with #236 and the inherited seal)

A request that reaches an instance which does not own its stream's shard is
forwarded once to the owner (`src/http/forward.rs`; the wire contract is in
`docs/refactor/WIRE-MATRIX.md` and edge change #235):

- Scope: stream routes only (`/v1/stream...`, `/v1/streams/...`), and only
  on an instance whose trusted peer table is not empty. A fleet-off cell
  never forwards and pays nothing for the layer.
- The non-owner sends the request once to the owner, marked
  `Streams-Forwarded`, when the trusted peer table holds the owner's URL
  (`fleet/urls.json` or heartbeat `SELF_URL`, https, under
  `FLEET_PEER_DOMAINS`). The client gets the owner's status, headers and
  body, plus `Streams-Forwarded-To: <owner>`.
- One hop only. A marked request is never forwarded again. A client that
  sends `Streams-Forwarded` itself opts out (the LiveFeed fleet cert uses
  this to find the owner).
- Bounds: the owner has 1 s to accept the connection (else the 409); its
  answer must begin within the shard-open wait (10 s) plus the request's own
  wait (at most 25 s) plus 5 s (else `503 forward_failed`). At most a
  quarter of `ADMIT_MAX_INFLIGHT` forwards (1,024 at the default 4,096) wait for
  owners at once; past that the client gets the 409.
- The 409 `not_ring_owner` with `Streams-Replay-To` stays when the owner's
  URL is unknown, the owner does not accept within 1 s, the forward cap is
  reached, or the handler read more than 64 KiB of the body (a product
  append over 64 KiB always).
- A failure after the request was sent answers `503 forward_failed` with
  `Retry-After: 1` and no `retryable` flag. The owner may have applied it:
  a client must not replay it as new (the SDK treats a plain append as
  outcome-unknown).
- A forwarded request is an active request at the non-owner until the
  owner's answer head arrives; a forwarded live feed holds a connection on
  both instances for its life (`docs/SCALING.md`).
- A create refused as not the shard's owner drops the non-owner's cached
  descriptor, so an append right after a forwarded create succeeds.

Limits that shape the tests:

- **Queues are not forwarded.** `/v1/queues` is not a data-plane route.
- **`live:wait` is not forwarded in practice.** It answers each
  subscription on a stream this instance does not serve with `421
  not_owner`, "wait on it per stream" (`src/product/live_wait.rs`). A
  multiplexed tenant on a fleet therefore loses its subscriptions on other
  instances' shards.
- The shard count balancer (edge change #236, §1.3) moves at most one
  shard per `REBALANCE_MOVE_COOLDOWN_SECS` (60 s) across the fleet, and
  none in the first cooldown after a boot. At this guide's posture (1-2
  shards per instance) the excess over the share is a few shards, so a ring
  evens out in minutes; the 25 minutes of edge change #236 is the retired
  64-shards-on-8 shape. Wait for it before measuring (§1.7).
- A new owner seals a moved shard's inherited backlog at once; before, a
  moved shard read as lag for 45 s, and in a 16-instance fleet 6 of 64
  shards kept moving with no load.

Consequences: run multiplexed tenants (`mux`, PACK-S-MUX) and queues only
on single-instance cells or with each client pinned to the owner. In a
fleet, use per-stream tails, groups and appends.

### 1.7 Verify the posture on a running instance

Run these per instance before load and again after it. `AUTH_HDR` is a
0600 file holding one line, `authorization: Bearer <AUTH_TOKEN>`.

```bash
U=https://<instance-url>; AUTH_HDR=$HOME/<secrets>/auth.hdr
# build identity: must equal the recorded commit and sha256
curl -sf -H @"$AUTH_HDR" "$U/v1/debug/load" | jq '{git_commit, binary_sha256, boot_id, rss_mb}'
# shards this instance has open: the keys of /v1/debug/timings (one per shard prefix)
curl -sf -H @"$AUTH_HDR" "$U/v1/debug/timings" | jq 'keys'
# ring and local state
curl -sf -H @"$AUTH_HDR" "$U/operator/data.json" | jq '.local | {instance, open_shards, ring_active, admit_shed, l0_write_sheds}'
# pump flushes per shard per second over 60 s: must be <= 9.9 on every shard
snap() { curl -sf -H @"$AUTH_HDR" "$U/v1/debug/timings" | jq -c 'with_entries(.value = .value.pump.flushes)'; }
a=$(snap); sleep 60; b=$(snap)
jq -n --argjson a "$a" --argjson b "$b" '$b | with_entries(.value = ((.value - ($a[.key] // 0)) / 60))'
# beside it, all WAL PUTs per shard per second over the same 60 s: the instance's
# put:wal delta divided by its shard count S (the keys of /v1/debug/timings)
walp() { curl -sf -H @"$AUTH_HDR" "$U/v1/debug/store" | jq '.totals.ops["put:wal"] // {} | (.ok // 0) + (.not_found // 0) + (.err // 0)'; }
w1=$(walp); sleep 60; w2=$(walp); echo "($w2 - $w1) / 60 / $S" | bc -l
```

`put:wal` counts every WAL object the instance writes, other databases'
beside the shards' included, so the quotient is an upper bound on a shard's
own. The instance's `put:wal` delta over the sum of its shards' pump-flush
deltas in the same window is §6's single-WAL-writer ratio.

From the boot log (`compute logs <version>` streams forever: run it in the
background and stop it after about 45 s, `bench/k2cost/field/README.md`):

- `effective configuration (redacted)`: the redacted summary
  (`src/config/summary.rs`; includes `fleet.fleet_min`, the compactor and
  memory knobs, `runtime.memprofile_cert`).
- `WAL group-commit pump on` once per shard, with `gap_ms=100` and
  `gather_ms=6` (`src/shard.rs`). Any other `gap_ms` voids the run.
- With 2N shards, the `INITIAL_SHARDS=... > FLEET_MAX=...` notice.

From the platform: `compute versions show <version>` lists the env names the
version holds. None of §1.5's names may appear.

Ring gate before any load (RUNBOOK §7.4b): all N instances live and
`ring_active` unchanged for 60 s. Load sent to a converging ring once lost
371,900 acknowledged records (`docs/SCALING.md` §10). Also wait until every
instance holds at most `ceil(shards / N)` shards and `fleet/overrides.json`
has not changed for 60 s. Expect one move per 60 s, so a fresh 8- or
16-member ring is gated within about 5 minutes; a longer wait is a finding
to report (edge change #236, ratified 2026-11-03).

## 2. Streams scaling tests

### 2.1 Workload profiles

| Profile | One tenant | Source |
|---|---|---|
| T, tiny | 3 streams, 200 appends a day of 400 B JSON singles, one long-poll tail always connected | `bench/k2cost/tenants/pack-t-1000.json` |
| S, small | 20 streams, 2 appends a second of 1 KiB JSON singles (open-loop Poisson), a tail on every stream, one consumer group (pull 10, settle each) on 5 streams | `bench/k2cost/tenants/pack-s-0030.json`; multiplexed: `pack-s-mux-0200.json` |
| M, medium CDC | 100 GiB a day batched: 10 streams, 100 × 1 KiB per request under 64 routing keys at 12.1 requests a second, two consumer groups, a tail per stream | `bench/k2cost/points/ML.env` (`m-shared`, `m-alone`); `bench/k2cost/tenants/ml-m-shared.json` |
| L, large sink | 150 GiB a day: 50 streams, 1,000 × 1 KiB per request at 1.78 requests a second (1.82 MB/s), one daily walk | `bench/k2cost/points/ML.env` (`l-alone`) |

M is admitted on a k = 8 cell at weights (request 2, bytes 6), one M per
cell and no L beside it: at (2, 5) its ceiling refused 22% of its appends.
L is not admitted on a shared cell; it gets a dedicated cell (k = 1): its
1.82 MB/s exceeds (k − 1)/k of the 2.06 MB/s envelope for any k. Both were
measured on the final 6A tree (`results-ml`, tree `6ed91580b`, program
notes; §5.2).

cell-admin places them by these rules (decision C, 2026-11-03; it lands in
`deploy/cell-admin`, whose README is its record): a k = 1 profile is a
dedicated cell (`deploy/profiles/dedicated-cell.env`, `init --max-projects
1`, one project, no `--weight`), not a refusal; `admit --class m` presets
weights (2, 6); `admit --class l` is admitted only on a dedicated cell.
Neither caps the stored bytes: storage is unlimited (the owner's decision
of 2026-11-03, edge change #244; docs/LIMITS.md), so M's 100 GiB and L's
150 GiB a day are never refused for what they store, and the classes'
earlier 320 GiB and 1.25 TiB caps are gone. Until that change is in the
commit under test, admit M with `--weight request=2 --weight bytes=6` and
run L's cell as `ML.env`'s `l-alone` does.

### 2.2 The tests

| Test | Shape | Load | What it confirms |
|---|---|---|---|
| ST-1 cell anchor | N = 1, cell shape (`shared-cell.env`), fleet off, 1 shard | PACK-T-1000, then PACK-S-MUX-0200 | the two measured per-tenant costs on Compute and the real store (§5.2) |
| ST-2 fleet WAL and linearity | N = 1, 8, 16; fleet shape; 1 shard per instance, then 2 | per instance: 20 S tenants with per-stream tails (`pack-s-0020.json`'s class), then 400 T tenants (`pack-t-0400.json`'s class) | WAL PUTs per shard stay at most about 10 a second; per-instance cost is flat as N grows |
| ST-3 heavy profiles | N = 1, fleet off, as they are placed: (a) one shared cell (`shared-cell.env`, k = 8); (b) one dedicated L cell (k = 1) | (a) 48 S + one M at weights (2, 6) (`ML_CELL=m-shared`); (b) one L (`ML_CELL=l-alone`'s phases) | M and L confirmed on Compute + Tigris against the Mac measurement: M shared $68.10, L dedicated $87.88 (§5.2) |
| ST-4 idle floor | each shape, no load, 60 min | none | the fixed per-instance and per-shard cost (§4.4) |

Per-instance load must stay under the cell envelope: 1,411 requests, 2.06 MB
appended and 30 MB read a second (`deploy/profiles/shared-cell.env`
`CELL_ENVELOPE_*`, marked PENDING re-measurement there). A multiplexed S
tenant made about 4.1 requests a second (measured 2026-10-28, program
notes).

The ST-2 densities leave room in the live pool. A cell parks at most 1,200
waits (`SSE_MAX_CONNECTIONS`, `deploy/profiles/compute-1g.env`). A
per-stream S tenant holds 25 (`bench/k2cost/points/PACK-S.env`), a T tenant
1. A forwarded wait is held on two instances (§1.6), and with clients
spread evenly over N instances about (N − 1) / N of the waits are
forwarded, so plan for twice the waits: 20 S (500, up to 1,000) and 400 T
(400, up to 800).

### 2.3 Running a step

Generators run in the fleet's region, never from a laptop across regions.
One tenants driver carried 200 S or 1,000 T tenants on the Mac rig
(`bench/k2cost/README.md`, "Packing"); its socket pool saturated at peak
from S × 20 on with the per-stream driver (`docs/STREAMS-COST-LEVERS-2026-10.md`),
so for ST-2 give each instance its own driver.

Tenants need one project and one token each. On a cell-admin cell:

```bash
bun bench/k2cost/tenants-admit.ts admit  --manifest M.json --state $STATE --key-file $KEY
bun bench/k2cost/tenants-admit.ts tokens --manifest M.json --state $STATE --key-file $KEY \
    --out $SECRETS/token-map.json --every 28800
```

On another auth setup, write the token map `[{"project", "token"}]` (mode
0600) from the issuer the fleet trusts. For ST-2, copy a manifest and change
the class `count` and `pool` to the instance count times the per-instance
tenants.

Every manifest's verdict window is `steady`, 360-1,560 s of a 1,640 s run
(`bench/k2cost/tenants/*.json` `periods`). Snapshot every instance at its
edges:

```bash
snapshot_all() {  # $1 = label; one file per instance and label
  for i in $(seq 1 $N); do
    U=$(cat $HOME_RUN/url-streams-$i.txt)
    curl -sf -H @"$AUTH_HDR" "$U/v1/debug/store?window=60" > $OUT/store-$1-s$i.json
    curl -sf -H @"$AUTH_HDR" "$U/v1/debug/timings"         > $OUT/timings-$1-s$i.json
    curl -sf -H @"$AUTH_HDR" "$U/v1/debug/load"            > $OUT/load-$1-s$i.json
    curl -sf -H @"$AUTH_HDR" "$U/operator/data.json"       > $OUT/operator-$1-s$i.json
  done
}
```

One step:

```bash
OUT=$RESULTS/ST-2-n08-s1-$(date -u +%Y%m%dT%H%M%SZ); mkdir -p $OUT
TM=$SECRETS/token-map.json
bun bench/k2cost/tenants.ts run --manifest M.json --base $BASE --token-map $TM --out $OUT > $OUT/run.log 2>&1 &
RUN_PID=$!
sleep 360;  snapshot_all steady-start
sleep 1200; snapshot_all steady-end
wait $RUN_PID
bun bench/k2cost/tenants.ts usage --manifest M.json --base $ROLLUP_URL --token-map $TM --out $OUT
bun bench/k2cost/tenants.ts audit --manifest M.json --base $BASE --token-map $TM --out $OUT
```

Run `usage` before `audit`: the audit's reads are metered too
(`bench/k2cost/tenants.ts` header).

With an s3lite store, also take the store's own ledger:

```bash
python3 bench/k2cost/scrape.py snapshot --run $OUT --label steady-start \
    --servers streams-1=$U1,streams-2=$U2 --s3lite $S3LITE_URL --auth-file $AUTH_HDR
```

ST-3 runs `bench/k2cost/points/ML.env`'s cells (`bench/k2cost/README.md`,
its `points/ML.env` row), each on a fresh single instance:

- (a) `m-shared` is a tenants step as above, with
  `bench/k2cost/tenants/ml-m-shared.json` (48 S and one M; the manifest
  carries M's weights, request 2 and bytes 6). M's cost is its marginal
  over `s-base`, `pack-s-mux-0048.json` alone, run the same day.
- (b) `l-alone` is k2gen's phases on a dedicated cell
  (`PROJECT_SHARE_K=1 PROJECT_MEMORY_PRESSURE_BYTES=268435456`, one
  project): the load, then one walk of everything it wrote (the day's walk).

```bash
H=$SECRETS/gen-headers.txt     # "authorization: Bearer ..." and the encryption key line
bun bench/k2cost/k2gen.ts setup   --base $BASE --headers-file $H --streams 50 --prefix st3/l
bun bench/k2cost/k2gen.ts produce --base $BASE --headers-file $H --streams 50 --prefix st3/l --rate 1.78 \
    --records 1000 --record-bytes 1024 --payload corpus --duration 1380 --seed 62 \
    --out $OUT/prod.jsonl --ledger $OUT/prod.ledger.json
bun bench/k2cost/k2gen.ts walk    --base $BASE --headers-file $H --streams 50 --prefix st3/l --parallel 8 \
    --out $OUT/walk.jsonl
```

The measurement priced a dedicated L as continuous ingest plus one read of
every byte: the produce window scaled to a month, plus its walk once per
produce span (`results-ml`, program notes); price ST-3's L the same way.

### 2.4 What to measure

| Quantity | Instrument | Per |
|---|---|---|
| WAL pump flushes | `/v1/debug/timings` `<shard>.pump.flushes` delta | shard, second |
| Store operations by class | s3lite `/_s3lite/stats2` (ground truth); on Tigris the server's `/v1/debug/store` `totals` (§4.2) | instance, window |
| Bytes into the store | s3lite `put_bytes`; else `totals.bytes_put` | instance, window |
| Stored bytes | s3lite `live_bytes`; on Tigris a bucket census | bucket, hourly |
| CPU seconds | wrapper `cpu sample:` lines, or heartbeat `cpu_pct` (fleet) | instance |
| RSS | `/v1/debug/load` `rss_mb` | instance |
| Meters (revenue) | `GET /v1/containers/{p}/usage` on the `ROLLUP=1` instance after the drain (`tenants.ts usage`) | tenant |
| Correctness | `tenants.ts audit` (every produced stream read back against its ledger) | tenant |
| Ownership | `/operator/data.json` `local.ring_active`; `fleet/overrides.json` | fleet |
| Forwarding | `Streams-Forwarded-To` answers; `503 forward_failed` count | fleet |

### 2.5 Void rules

A step is void when any of these holds. The list is the rig's
(`bench/k2cost/README.md`, "Validity"; `bench/k2cost/packing.py`) plus the
posture checks of §1:

1. A rate-limit refusal, shed, wedge, maintenance backpressure engage or
   project-memory shed for a compliant tenant: any delta above 0 in
   `admit_shed`, `stream_shed`, `wedge_shed` or `l0_write_sheds`
   (`/operator/data.json` `local`).
2. An append sent and not acked; an open-loop send more than 100 ms late or
   never sent (pacing).
3. The meters did not settle, or acked records differ from `ingestRecords`,
   or delivered bytes differ from `readPayloadBytes`.
4. An audit failed (a lost, duplicated or foreign record).
5. A `boot_id` changed during the window.
6. `rss_mb` above 450, or rising more than 2 MB a minute over the second half
   of a window of at least 300 s.
7. `git_commit` is not the declared commit.
8. `ring_active` or `fleet/overrides.json` changed during the window, or the
   window started before the ring gate (§1.7).
9. A pump flush rate above 9.9 a second on any shard, a `gap_ms` other than
   100, or a §1.5 setting present with a non-default value.
10. More shards on an instance than the configuration planned, with no
    count balancer (§1.3): not void, but report it and price per shard.

### 2.6 Fail-early stop rules

The owner's rule is to abort a run at its first red (cheapest checks first,
watch long runs live). For each step:

- Before load: §1.7 fails on any instance → stop; fix the deploy.
- First 2 minutes of load: any void condition 1, 2, 5 or 9 → abort the
  step; it would end void.
- Every minute: pump flushes per shard above 9.9 a second → abort.
- `rss_mb` above 450 at any time → abort.
- A ring or override change → abort the window; restart it after the gate.
- A density step that fails rule 1 is the cell's limit, not an error: record
  N and stop the ladder there.
- ST-3 sends requests of about 100 KiB (M) and 1 MB (L). Its cells run
  fleet off, so nothing is forwarded. The Mac `m-shared` run passed every
  check but rule 1: M met its memory line and rate limit on 376 of about
  100,600 requests (0.37%), all retried, every append acked, and the S
  tenants saw none. Report a matching run as the Mac run was reported
  (valid but for M's retried refusals, priced); a refusal of an S tenant,
  or any on the dedicated L cell (the Mac run had none), stays void.

## 3. Queues scaling tests

### 3.1 The scenario and the runs

The scenario is Prisma's control-plane queue month: 18 queues, 22,747,641
messages, 3.018 operations a message (`docs/QUEUES-PRICING-BENCHMARK.md` §3;
`bench/queues-cost/scenario.json`). The replay is
`bench/queues-cost/replay.mjs`.

| Run | Shape | Length | Source |
|---|---|---|---|
| 1x beside a co-tenant | day profile | 30 min warm-up + 60 min (measured legs); the gated plan is 48 h a side | `docs/QUEUES-PRICING-BENCHMARK.md` §6, §9 |
| 10x beside a co-tenant | busiest-hour profile (1.99× the average rate) | as above | §10 |
| 100x alone, dedicated instance | busiest-hour profile, 1,747 messages a second sent | 10-30 + 30-60 min | §11 |
| Hot queue | the Stripe queue alone at 100x, `--compress K` to build a due backlog in minutes | 10 min | §11 |

### 3.2 Where to run them

- **Locally** (the measured numbers): `bench/queues-cost/run-ab.sh` on the
  costab rig, s3lite at 25 ms, 4 shards on one server
  (`bench/queues-cost/README.md`, "The runs").
- **On Compute**: one instance per queue run, never a fleet target (§1.6,
  queues are not forwarded), with `CPU_LOG_SECS=15` (§1.4). Run the replay
  from an in-region host. The rig's field mode (decision B6) replaces
  `--pid`: `collect.mjs mark` and `sample` take `--cpu-log FILE`, the
  wrapper's `cpu sample:` lines (`proc_cpu_s`, cumulative per boot) from
  `compute logs` tailed to a file, and `--server URL --auth-file F` for RSS
  from `/v1/debug/load` `rss_mb`; `price` is unchanged. The field mode
  lands in a separate bench commit (`bench/queues-cost/collect.mjs`, its
  README's "Field mode"); check that the commit under test has it. Without
  it, sample by hand and mark the report "CPU by hand".

```bash
compute logs <version> > $OUT/B/cpu.log 2>&1 &     # streams forever: stop it after the last mark
LOGS_PID=$!
F="--cpu-log $OUT/B/cpu.log --server $U --auth-file $SECRETS/auth"
node bench/queues-cost/collect.mjs mark   --dir $OUT/B --tag t0 $F
node bench/queues-cost/collect.mjs sample --dir $OUT/B $F > $OUT/B/sampler.log 2>&1 &
SAMPLER_PID=$!
node bench/queues-cost/replay.mjs --base $U --auth-file $SECRETS/auth --key-file $SECRETS/key \
    --scale 100 --profile peak --no-invoice-burst --duration-s 3600 --drain-s 900 \
    --out $OUT/replay-summary.json --timeline $OUT/replay-timeline.jsonl > $OUT/B/replay.log 2>&1 &
REPLAY_PID=$!
sleep $WARMUP_S;   node bench/queues-cost/collect.mjs mark --dir $OUT/B --tag start $F   # §3.1's lengths
sleep $DURATION_S; node bench/queues-cost/collect.mjs mark --dir $OUT/B --tag end $F
wait $REPLAY_PID;  node bench/queues-cost/collect.mjs mark --dir $OUT/B --tag final $F
kill $SAMPLER_PID $LOGS_PID
```

  These are `run-ab.sh`'s marks (`t0`, `start`, `end`, `final`). Every
  other arm takes the same marks in its own directory, and `collect.mjs
  price` prices them as `run-ab.sh` does for that run
  (`bench/queues-cost/README.md`, "The runs").

- **Shard count: the queue product is priced on 2 shards per instance**
  (decision B5, §8 item 5; `docs/QUEUES-PRICING-BENCHMARK.md` §12). Every
  measured queue figure so far ran `INITIAL_SHARDS=4` on one instance;
  before the scan-fetch fix one shard saturated at 589 messages a second
  with 1.8 s sends (§11 there). Three arms:
  - **2 shards, 100x: the gated arm.** If 2 shards do not hold 100x (hot
    drain under 95% of the due rate for 5 minutes, or send p99 over 1 s),
    the rate they hold is the product's per-tenant ceiling and the price
    rests on it.
  - **1 shard: a ceiling probe.** Report what one shard holds after the
    scan-fetch fix.
  - **4 shards: the Mac cross-check only**, reported apart; it breaks the
    posture.

  The decision asks for a Mac rerun of 100x at 2 shards before the field
  run, as its expected figure; while the 24 h soak holds the Mac it cannot
  run, so the field run's 2-shard arm provides the figure. `run-ab.sh`
  hard-codes `INITIAL_SHARDS=4`; its `QUEUE_SHARDS` variable (default 2,
  recorded in each arm's `costs.json`) lands in a separate bench commit.
- **1x and 10x are A/B pairs.** Arm A runs the co-tenant alone, arm B the
  co-tenant plus the replay; each arm on a fresh prefix, with the co-tenant's
  streams named the same in both arms (`run-ab.sh` `BASELINE_PREFIX`: a
  stream's name decides its shard, and differently placed baselines moved
  Class A by $25-30 a month at 10x, `docs/QUEUES-PRICING-BENCHMARK.md` §6,
  §10). The marginal cost is B − A, term by term. On Compute, drive the
  co-tenant with the same k2gen produce command in both arms.

### 3.3 Hot queue and consume ceilings

- Before the fix, one queue drained at a flat 204-215 messages a second
  whatever the workers or consumers: the shard committer fetched the
  availability index one 4 KiB block per GET (`docs/QUEUES-PRICING-BENCHMARK.md` §11).
- After it (`SCAN_FETCHES` = 16 fetches in flight), the hot drain kept up
  with the due rate (374 against 377 a second) at 13.1% of a core; at 100x
  the Stripe queue delivered 2,184,911 messages in the hour, up from
  1,158,197 (§11).
- Measure: drain rate against due rate (replay timeline), the hot shard's
  committer from `/v1/debug/timings` groups, CPU and RSS. A drain under 95%
  of the due rate over a 5-minute window is the ceiling (§6): record it and
  stop.

```bash
node bench/queues-cost/replay.mjs --base $U --auth-file $SECRETS/auth --key-file $SECRETS/key \
    --scale 100 --profile peak --no-invoice-burst --only cp-stripe-usage-records-sync \
    --compress 10 --duration-s 600 --drain-s 5 --out $OUT/hotq.json --timeline $OUT/hotq.jsonl
```

### 3.4 Void and stop rules

A queue run is void on any unexpected error, a lost message (server
`due + scheduled + leased + retrying` against sent minus acked and failed),
server counters that differ from the replay's, or a dead-letter rate off the
scenario's by more than 0.02 (`bench/queues-cost/README.md`, "Validity").
Every 10x and 100x run so far was void only by the drain rule (leases
still out at the deadline, nothing lost); report that case as "void by
drain only" and price it, as the benchmark does (§10, §11). Flag a run
whose rate is outside 0.9-1.1× its target (§11). Abort at the first lost
message or unexpected error.

## 4. Cost model

### 4.1 Prices

| Item | Owner's price | ab50 (the basis for every target) | Tigris list (sensitivity) |
|---|---:|---:|---:|
| Class A, per million | $3.75 | **$1.875** | $5.00 |
| Class B, per million | $0.375 | **$0.1875** | $0.50 |
| Storage, per GiB-month | $0.015 | $0.015 | $0.02 |
| Transfer into Tigris, per GB | $0.01 | $0.01 | |
| Compute memory, per GB-hour | $0.006 | $0.006 | |
| Compute CPU, per active vCPU-hour | $0.064 | $0.064 | |
| Delivery egress, per GB | $0.005 | $0.005 | |

Sources: owner's prices and list, `docs/QUEUES-PRICING-BENCHMARK.md` §6 and
`bench/queues-cost/prices.mjs`; ab50 is "Class A and B at half"
(`bench/k2cost/packing.py`), confirmed by the owner on 2026-10-26 as A
$1.875 and B $0.1875 (program notes). A 1 GiB instance's memory is
1 × 720 × $0.006 = $4.32 a month.

### 4.2 Counting operations

Prefer the store's own counters over client estimates:

1. **s3lite**: `/_s3lite/stats2` is ground truth, per (tier, kind, op) and
   status (`docs/COST-METHODOLOGY.md`). Fresh s3lite and fresh prefix per
   run; counters are cumulative.
2. **Tigris**: no per-request export was found in the platform API
   (`bench/k2cost/field/README.md`, "Known limits"). Use the server's
   `/v1/debug/store` `totals`: one count per object-store call, per
   `op:class` (`put`, `mpu`, `get`, `head`, `delete`, `list`, `copy` ×
   `wal`, `manifest`, `sst`, `fleet`, `other`), split `ok`, `not_found`,
   `unbilled` (304 and refusals) and `err` (`src/store_timing/observations.rs`).
   Reconcile them against Tigris's own counts (decision B2, §8 item 2):
   - (a) **Run every configuration in a fresh bucket used by nothing
     else.** The platform has no per-request export, but the console's
     per-bucket usage then is a ledger of that configuration alone.
   - (b) **The 304 probe, before any load.** Against an otherwise idle
     bucket, send 100,000 conditional GETs (`If-None-Match` on one object)
     that answer 304. The next day, read the bucket's request counts in the
     Tigris console or usage export: if Class B moved by about 100,000,
     304s are billed. It costs about $0.02. Until it answers, report every
     margin on the conservative basis (304 billed as Class B) with the
     optimistic figure beside it. Asking Tigris support is optional and the
     owner's to send; the probe decides on its own.
   - (c) **Reconcile after each run.** Sum the server's `/v1/debug/store`
     Class A and Class B deltas over the instances on the bucket and
     compare each with the bucket's Tigris-side count: tolerance ±5% per
     class. A retry counts once on the server, so `err` is the
     explanation for a gap.

```bash
# the 304 probe; $SECRETS/tigris.curlrc (0600) holds two lines:
#   user = "<access key id>:<secret access key>"   and   aws-sigv4 = "aws:amz:auto:s3"
O="$SLATE_S3_ENDPOINT/$PROBE_BUCKET/probe-304"
echo probe | curl -sf -K "$SECRETS/tigris.curlrc" -T - "$O"
ETAG=$(curl -sfI -K "$SECRETS/tigris.curlrc" "$O" | awk 'tolower($1)=="etag:" {print $2}' | tr -d '\r')
seq 100000 | xargs -P 16 -I{} curl -s -o /dev/null -w '%{http_code}\n' -K "$SECRETS/tigris.curlrc" \
    -H "If-None-Match: $ETAG" "$O" | sort | uniq -c       # must print: 100000 304
```

Billing rules (`docs/COST-METHODOLOGY.md`; `bench/k2cost/README.md`, "Pricing"):

- Class A: PUT, COPY, LIST, multipart create, parts and complete, on 2xx and
  404. Class B: GET and HEAD on 2xx and 404. Free: DELETE, 304, 412 and other
  4xx refusals. 429, 502, 503 and 504 are billed at the op's class.
- The server counts a multipart upload once (`mpu`); bill it as 3 Class A
  (`--mpu-requests 3`, `bench/k2cost/field/README.md`). A retried request
  counts once; `err` is an upper bound.
- Whether Tigris bills 304s is unknown. Registry revalidations answer 304
  about once per long-poll re-arm; billed as Class B they would add 33-62% to
  T and S cells (`docs/STREAMS-COST-LEVERS-2026-10.md`). Report `unbilled`
  separately so both can be priced; until the 304 probe (above) answers,
  the conservative basis is the headline.

```bash
# Class A, Class B, unbilled and err from one /v1/debug/store snapshot
jq '.totals.ops | to_entries | reduce .[] as $e ({a:0, b:0, unbilled:0, err:0};
      ($e.key | split(":")[0]) as $op
      | if   ($op | IN("put","mpu","list","copy")) then .a += $e.value.ok + $e.value.not_found
        elif ($op | IN("get","head"))              then .b += $e.value.ok + $e.value.not_found
        else . end
      | .unbilled += $e.value.unbilled | .err += $e.value.err)' store-end-s1.json
```

Subtract the start snapshot from the end snapshot per instance; if
`totals.since_ms` changed, the process restarted and the step is void (§2.5
rule 5).

### 4.3 The formulas

A window of `w` seconds scales to the month by `2,592,000 / w` (30 days;
`bench/queues-cost/prices.mjs`). The instance's month
(`bench/k2cost/packing.py price`; `bench/queues-cost/prices.mjs`):

```
month = A × pA + B × pB                      (window counts × 2,592,000 / w)
      + put_bytes / 1e9 × $0.01              (transfer in)
      + stored GiB × $0.015                  (live-byte growth held half a month in packing.py)
      + cpu_s / 3600 × $0.064                (capped at 1 vCPU per instance)
      + 1 GiB × 720 h × $0.006               ($4.32)
      + delivered bytes / 1e9 × $0.005       (egress)
ab50 month  = owner month − (Class A $ + Class B $) / 2
per tenant  = instance month / N             (no floor share: the instance carries only these tenants)
per million = month / (messages in the window × 2,592,000 / w) × 1e6
```

**WAL PUTs per shard.** The pump starts a flush at most once per 101 ms
(100 ms gap plus the 1 ms settle), only when a commit waits, with one PUT in
flight. A busy shard therefore writes at most 1000 / 101 = 9.90 WAL objects
a second, and fewer when the PUT takes longer than the gap (RUNBOOK §3.2).

| WAL PUTs per shard per second | PUTs a month | ab50 ($1.875/M) | owner ($3.75/M) | Source of the rate |
|---:|---:|---:|---:|---|
| 10.0 (planning figure) | 25,920,000 | **$48.60** | $97.20 | 10 × 86,400 × 30 |
| 9.90 (the cap) | 25,663,366 | $48.12 | $96.24 | RUNBOOK §3.2 |
| 9.67 (pump flushes, one shard, 200 S tenants; pre-`e1e7a45f4`/`da3008f56`; now expected equal) | 25,064,640 | $47.00 | $93.99 | 11,604 in 1,200 s (measured 2026-10-31, program notes) |
| 10.71 (all shard WAL PUTs, same run; pre-`e1e7a45f4`/`da3008f56`; now expected equal) | 27,762,480 | $52.05 | $104.11 | 12,853 in 1,200 s (measured 2026-10-31, program notes) |
| 9.71 (4 shards, queue 100x) | 25,174,260 | $47.20 | $94.40 | 139,857 an hour over 4 shards (`docs/QUEUES-PRICING-BENCHMARK.md` §11) |

The 9.67 and 10.71 rows were measured before `e1e7a45f4` (a seal and a
forced flush take no WAL flush of their own) and `da3008f56` (the fork's
explicit flush freezes exactly once; its test asserts WAL objects equal
pump flushes, 20/20, on a loaded Linux container). One WAL object per pump
flush is now the contract (§6). The planning figure stays 10.0 a second.

A shard is "busy" when most 101 ms windows hold a write. That happens long
before it is loaded: the WAL tier ran at its ceiling from 70 S tenants on one
shard (9.6-9.9 flushes a second; measured 2026-10-28, program notes), and at
10x the queue groups filled every window of all four shards
(`docs/QUEUES-PRICING-BENCHMARK.md` §10). WAL cost is per busy shard, not
per byte. That is why the shard count per instance matters:

| Configuration | WAL PUTs a month, all shards busy | ab50 |
|---|---:|---:|
| 1 instance, 1 shard, gap 100 | 25.9 M | $48.60 |
| 1 instance, 2 shards, gap 100 | 51.8 M | $97.20 |
| 16 instances, 32 shards, gap 100 | 829.4 M | $1,555.20 |
| The earlier bench: 64 shards on 1 instance, gap 100 | 1,658.9 M | $3,110.40 |
| The earlier bench: 64 shards, gap 25, store answering in under 26 ms | at most 6,380.3 M (38.5 a second a shard) | at most $11,963.08 |

The gap-25 row is a ceiling: with one PUT in flight, a shard writes at most
1 / RTT a second, so a slower store writes fewer. For scale, the 1 GiB
instance's memory is $4.32 a month: one busy shard's WAL costs about 11
instances' memory.

**Class A and B per shard, measured.** One shard carrying 200 S tenants
(PACK-S-MUX-0200, layout 7, binary 61c23542; measured 2026-10-31, program
notes):

| Per second, whole instance | Class A | Class B |
|---|---:|---:|
| Shard WAL PUTs | 10.71 | |
| Shard SST, manifest, compaction PUTs | 0.74 | |
| Shard SST GETs (reads) | | 10.53 |
| Shard manifest and compaction GETs (pollers) | | 5.72 |
| Everything (with the telemetry, history and registry tiers) | 13.06 | 49.38 |
| Month at ab50 | $63.49 | $24.00 |

The instance's whole month was $159.40 at ab50: Class A $63.49, Class B
$24.00, transfer $28.98, CPU $18.16 (Mac), storage $11.82, egress $8.63,
memory $4.32 (the owner-price terms in results-sgap-step2, program notes,
with Class A and B halved). Its 10.71 WAL PUTs a second predate
`da3008f56`; on today's tree expect them equal to the pump flushes.

### 4.4 Fixed floors (derived, to compare with ST-4)

- Pollers: each open shard database polls its manifest every 2,000 ms and
  its compaction state every 2,500 ms, each a GET that usually answers 404
  (RUNBOOK §3.2). That is about 0.9 Class B a second, 2.3 M a month, $0.44 at
  ab50, per open database. The measured loaded figure above (5.7 a second)
  is higher: writers read manifests too.
- Heartbeats: one PUT every 2 s per fleet instance (RUNBOOK §6): 1.3 M a
  month, $2.43 at ab50.
- The idle floor measured on older binaries: $0.05 an hour with 1 shard and
  $0.12 with 4 shards, before compute (`bench/k2cost/README.md`, "Floor
  findings"); a T-only cell of 1,000 tenants: $29.0 a month at ab50 (§5.2).

## 5. Revenue at Cloudflare's prices

### 5.1 Prices

| Product | Price | Source |
|---|---|---|
| Streams, at Cloudflare's durable-stream product ("K2") | produced $0.04/GB, consumed $0.04/GB, retained $0.02/GB-month, on uncompressed bytes; fetched 2026-10-24, "billing not yet enabled" | `docs/STREAMS-COST-LEVERS-2026-10.md`; `bench/k2cost/README.md` |
| Queues, at Cloudflare Queues | $0.40 per million operations, one per 64 KB written, read or deleted; one million a month included; no egress charge | `docs/QUEUES-PRICING-BENCHMARK.md` §1 (pricing page updated 2026-04-21) |

Re-read the K2 and Cloudflare Queues price pages **twice** (decision B7,
§8 item 7): before the first run and on the day the report is written.
Record each fetch date, the quoted prices and whether K2 billing is enabled
in the report's "Against the measured figures" section (§7); K2's billing
was not enabled when it was last read. A price change never stops a run
(the cost bars are cost-side): report the margins at both the old and the
new price. Any re-pricing is the owner's.

A tenant's K2 bill is the three prices on its meters (`bench/k2cost/packing.py`:
T keeps everything, S keeps 7 days). The queue scenario's Cloudflare bill is
$27.06 at 1x, $274.17 at 10x and $2,745.26 at 100x, about $1.21 per million
messages (`docs/QUEUES-PRICING-BENCHMARK.md` §4;
`python3 bench/queues-cost/cf_bill.py`).

### 5.2 Measured cost against revenue

Margin = revenue − cost. Costs at ab50. "Bar" is the acceptance figure the
lead set for layout 6A on 2026-10-30: the pre-6A reference cost, "no worse
than today" (`docs/reviews/2026-09-hardening/README.md`, Owner decisions,
"S-gap step 2 and layout 7"). It is not a revenue ceiling.

| Workload | Our cost (ab50) | Bar | Revenue | Margin | Margin % | Cost source |
|---|---:|---:|---:|---:|---:|---|
| Queue 100x, per million messages sent | $0.0888 (owner $0.1416) | $0.0960 | $1.207 | $1.118 | 92.6% | measured 2026-10-31, program notes |
| Queue 10x, marginal month beside a co-tenant | $66.90 | $71.61 | $274.17 | $207.27 | 75.6% | measured 2026-10-31, program notes |
| Queue 1x, marginal month beside a co-tenant | $20.24 (bar missed by 6.2%) | $19.06 | $27.06 | $6.82 | 25.2% | measured 2026-10-31, program notes |
| Stream tenant S, at 200 per cell (PACK-S-MUX-0200) | $0.797 | $0.926 | $0.501 | −$0.296 | −59% | measured 2026-10-31, program notes |
| Stream tenant T, at 1,000 per cell (PACK-T-1000) | $0.0290 | none | $0.00015 | −$0.02885 | cost 193× revenue | measured 2026-10-31, program notes |
| Stream tenant M, shared cell at weights (2, 6), marginal over 48 S | $68.10 (dedicated: $126.21) | | $404.02 measured, $530 nominal | $335.92-461.90 | 83.1-87.2% | measured 2026-11-01 on `6ed91580b` (`results-ml`, program notes; `bench/k2cost/points/ML.env`) |
| Stream tenant L, dedicated cell (k = 1) | $87.88 (shared marginal: $67.91) | | $483 | $395.12 | 81.8% | same |
| Fleet of 1,000 tenants (700 T, 250 S, 40 M, 10 L), month 1, **derived** | $3.8k-6.1k | | $26.2k | $20.1k-22.4k | 77-85% | derived from the measured points above: 700 T $20 + 250 S $199 + 40 M $2,724-5,048 + 10 L $879 |

M's measured K2 bill ($404) is below the nominal one ($530) because a
tail follows one routing key of its stream; L's measured bill is its
nominal one. The fleet row's M range runs from the shared marginal to the
dedicated cell: one M per cell with few S neighbours sits nearer the
dedicated figure than the shared marginal. The earlier model of M and L
($248-260 and $232-236 at owner prices, and a $14.4k fleet) is superseded
(`docs/STREAMS-COST-LEVERS-2026-10.md`).

How to read the queue rows:

- 100x ran alone on a dedicated 4-shard instance and includes the 1 GiB
  instance; it is per message sent in the busiest hour. The repository's
  last recorded 100x figure, before layout 7, is $0.104 per million at ab50,
  and an average 100x month of about $345 at ab50 against $2,745.26 (87%
  margin) (`docs/QUEUES-PRICING-BENCHMARK.md` §11).
- 10x and 1x are marginal: the queue's addition to a cell that already runs
  a co-tenant, without a share of the cell's floor. 10x ran at the busiest
  hour (1.99× the average), so its month overstates the volume terms. 1x ran
  in a quiet hour (0.30×); the reference's average 1x month was $24.0-24.3
  against its $19.06 run-rate month (measured 2026-10-30, program notes), so
  the 1x margin at the average month is lower than 25.2%.
- Against the original goal (cost at most 20% of Cloudflare's bill, §5 of
  the benchmark): 100x meets it ($0.0888 against $0.241); 10x ($66.90
  against $54.83) and 1x ($20.24 against $5.41) do not.

Earlier repository figures for T and S, before layout 6A (binary 409acbc8,
per-stream tails): T at 1,000 per cell $0.043 at ab50, S at 30 per cell
$3.74, S at 20 per cell $4.97 (`docs/STREAMS-COST-LEVERS-2026-10.md`).
ST-2 runs the per-stream S shape at 20 per instance, so compare it with the
S × 20 row there, not with PACK-S-MUX-0200.

The CPU term is a Mac figure. The program's notes put the Compute factor
at 1.3-2.5× (2026-10-28, program notes); `packing.py price --cpu-factor 2` applies the
field factor F5 measured. At 2× the S tenant's CPU share ($18.16 / 200 =
$0.091) doubles, and S becomes $0.888. ST-1 measures the real factor.

### 5.3 Tiny and small tenants: access fee or marketing expense

The owner, 2026-11-01: "The cost reduction on small and tiny tenants is good
enough now. These will either be considered a marketing expense by us, or be
operated by a customer paying 10$/ month in access fee."
(`docs/reviews/2026-09-hardening/README.md`, Owner decisions.)

The model, decided on 2026-11-03 (§8 item 3): usage is priced at K2
parity; the paid plan carries a **$10 per month minimum spend per
workspace**, and the fee counts as the first $10 of usage, so a tenant pays
max($10, usage). The free tier is the T profile and is the marketing
expense. The published price list (the $10 figure, the free tier's limits,
K2 parity itself) is held by the owner; this fixes the model only. Storage
is unlimited on every cell (edge change #244) and billing still meters
retained bytes, so a tenant's limits are throughput (docs/LIMITS.md); how
retained bytes are priced, the free tier's included, is part of that price
list.

| Tenant | Marketing expense: loss a month per 1,000 tenants | Minimum spend $10: revenue | Minimum spend $10: margin |
|---|---:|---:|---:|
| T at 1,000 per cell (the free tier; on the paid plan) | $28.85 | $10 | $9.97 (99.7%) |
| S at 200 per cell (cost $0.797) | $296 | $10.00 | $9.20 (92%) |
| M, L | | their usage ($404-530, $483), no fee on top | §5.2 |

"On top" against "minimum" differs by $0.50 for an S tenant and by nothing
material for M and L; these margins hold within 5% either way.

## 6. Acceptance bars

The bars and their tolerances were decided on 2026-11-03 (§8 items 1, 2,
5 and 8); the owner still accepts the result (AGENTS.md §1, standing
holds). Counts do not move with host load
(`docs/STREAMS-COST-LEVERS-2026-10.md`), so the bars compare counts first
and dollars second.

| Claim | Confirms | Refutes |
|---|---|---|
| The posture holds (hard: a miss voids the run) | every shard ≤ 9.9 pump flushes a second over a 10-minute window; every instance at the planned shard count (or the count reported, §1.3); no §1.5 setting | any shard above 9.9 pump flushes a second: the gap is overridden and the posture is wrong |
| WAL cost is per busy shard (single WAL writer) | per shard over a 10-minute window: all WAL PUTs ≤ 10.1 a second, and WAL PUTs ÷ pump flushes ≤ 1.02 (the 2% covers a fresh shard's open-time WALs) | a ratio above 1.02: the run is not void and its counts price as measured, but this claim fails; report the finding by name, a WAL writer besides the pump |
| Fleet scales linearly | ST-2 Class A and B per instance, and cost per tenant, at N = 8 and 16 within ±10% of N = 1 at the same per-instance load, after subtracting the fleet floor ST-4 measured at that N (heartbeat PUTs, tick reads) | more than ±10% after the floor: name the term (forwarding, moves) |
| The cell anchors hold on Compute | ST-1 Class A and B per tenant within ±10% of the Mac run; ab50 per tenant at most the Mac figure plus its CPU term × (factor − 1), +10% (S: $0.797 + $0.091 × (factor − 1), +10%) | counts off by more than 10% |
| The queue costs hold | at 2 shards, the priced shape (§3.2), with the bars unchanged as ceilings: 100x at most $0.0960 per million sent at ab50; 10x at most $71.61; 1x at most $19.06 (missed by 6.2% on the Mac until slatedb#2208 is in the pin). The 2-shard result becomes the reference | 100x above $0.241 per million (the 80% margin line) |
| The hot queue keeps up | drain ≥ 95% of the due rate over every 5-minute window | a 5-minute window under 95%: report the ceiling |
| Correctness | zero lost, duplicated or foreign records; zero lost messages | any loss voids every cost claim of the run |
| Counters reconcile with Tigris | ±5% per class per bucket (§4.2 (c)) | a gap over 5% not explained by `err` |

The two WAL bars were decided on 2026-11-03 (§8 item 1). The planning
figure stays 10.0 a second ($48.60 per busy shard-month at ab50); the
earlier 9.3-11 band and the 10.71 figure are retired (§4.3). The ratio is
reported, not assumed: the double-freeze defect `da3008f56` fixed showed
only under scheduling delay, which a 1-vCPU microVM has in abundance.

The tolerances (decision B8, §8 item 8):

| Claim | Tolerance |
|---|---|
| Posture | pump flushes ≤ 9.9/s per shard, exact (B1) |
| Single WAL writer | ratio ≤ 1.02, total ≤ 10.1/s (B1) |
| Linearity (N = 8, 16 vs 1) | Class A and B per instance ±10% **after subtracting the fleet floor measured by ST-4 at that N** (heartbeat PUTs, tick reads) |
| Cell anchors (Compute vs Mac) | Class A and B per tenant ±10%; dollars per tenant ≤ Mac figure + CPU term × (factor − 1), +10% |
| Tigris reconciliation | ±5% per class (B2) |
| Queue dollars | the §6 ceilings, exact, at 2 shards (B5) |
| Hot queue | drain ≥ 95% of the due rate over every 5-minute window |
| Correctness, boot_id, RSS 450 | exact |

Clock-paced terms (the WAL) are held tight. Load-paced terms (reads,
pollers) and a store with a latency tail (one PUT in flight writes fewer
WALs when the PUT outlasts the gap) get 10%. The fleet floor is subtracted
so that heartbeats (about 4% of Class A per instance) do not consume the
linearity budget.

## 7. Reporting template

Report back exactly this, as text or Markdown tables. Never upload raw
logs, ledgers or evidence anywhere (AGENTS.md §1); keep them under the
run's results directory outside the repository. Tear down every service and
bucket the run created. Never stop or redeploy the Tigris observatory
probes (RUNBOOK §14).

```markdown
## Scale validation report, <UTC date>

Commit: <40-hex sha>   binary sha256: <...>   ELF machine: 3e00
Store: s3lite (<latency ms>) | Tigris (<region>)   Region: <compute region>
Forwarding (edge #235): yes|no   Count balancer (edge #236): yes|no   Inherited seal at open: yes|no

### Environment (one instance; secrets redacted; names of every variable set)
<NAME=value lines; secrets as NAME=<redacted>>
Boot log: gap_ms=<..> gather_ms=<..> per shard; effective configuration excerpt

### Topology per configuration
| Config | N | INITIAL_SHARDS | PATH_PREFIX | shards per instance (min/max) | ring gate passed at |

### Runs
| Run | Test | N | shards | tenants or scale | window (s) | VALID / VOID (reason) |

### Per-shard WAL (every run)
| Run | shard | pump flushes/s | all WAL PUTs/s | WAL PUTs ÷ pump flushes | PUT p50/p99 ms (put:wal) |

### Store operations and cost (every valid run)
| Run | Class A | Class B | unbilled | err | put GB | stored GiB | CPU s | RSS max MB |
| Run | month owner $ | month ab50 $ | per tenant ab50 $ | per M messages ab50 $ |

### Tigris reconciliation (every Tigris run; §4.2)
| Run | bucket | server Class A | Tigris Class A | delta % | server Class B | Tigris Class B | delta % | err |
304 probe: billed | not billed | pending (probe date <..>, Class B moved by <n>); margins on the conservative basis until it answers

### Against the measured figures and revenue
Price pages (§5.1): K2 fetched <date> before the first run and <date> at writing, prices <..>, billing enabled yes|no; Cloudflare Queues fetched <date> and <date>, prices <..>
| Workload | ours ab50 | Mac figure | delta % | revenue | margin $ | margin % | margin % at the new price (if a page changed) |

### Queues
| Run | shards | sent/s target | sent/s held | delivered | lost | p99 send ms | drain vs due |

### Compute CPU factor
| Run | CPU s per tenant-hour (Compute) | Mac figure | factor |

### Deviations, forwarded share, shard moves, open questions
```

## 8. Open items, and their decisions

Collected from the sections above. Fable 5.1 decided them on 2026-11-03
under the owner's delegation of that day ("Please have Fable 5.1 make
decisions on those"); the decision record is kept outside the tree
(`~/.streams-k2/analysis/decisions-fable-2026-11-03.md`, items B1-B8), and
the hardening README's "Owner decisions" table lists them.

1. **Closed** (decision B1). The WAL bar: two bars per shard over a
   10-minute window, pump flushes ≤ 9.9 a second (hard) and all WAL PUTs
   ≤ 10.1 a second with WAL PUTs ÷ pump flushes ≤ 1.02 (reported, not
   void); §6, §1.7, §4.3.
2. **Closed** (decision B2). Whether Tigris bills 304s, and a Tigris-side
   request count to reconcile against: a fresh bucket per configuration,
   the 304 probe before any load (conservative basis until it answers),
   and reconciliation within ±5% per class after each run (§4.2, §6, §7).
3. **Closed** (decision B3). The $10 access fee is a minimum spend: the
   paid plan's $10 a month per workspace counts as the first $10 of usage
   (a tenant pays max($10, usage)); the free tier (T) is the marketing
   expense (§5.3). The model only: publication is held below.
4. **Closed** (decision B4). M and L are measured (`results-ml`, tree
   `6ed91580b`): M shared $68.10 (K2 $404 measured, $530 nominal), L
   dedicated $87.88 (K2 $483); ST-3 now confirms them on Compute + Tigris,
   one shared M cell and one dedicated L cell, and L's phase plan is
   `ML.env`'s `l-alone` (§2.1-§2.3, §5.2).
5. **Closed** (decision B5). The queue product is priced on 2 shards per
   instance: the gated arm is 100x at 2 shards, 1 shard a ceiling probe, 4
   shards the Mac cross-check only (§3.2, §6;
   `docs/QUEUES-PRICING-BENCHMARK.md` §12).
6. **Closed** (decision B6). The queue rig gets a minimal field mode (bench
   code only, bounded to one day): `collect.mjs mark|sample --cpu-log FILE
   --server URL --auth-file F` in place of `--pid`; by-hand sampling is the
   fallback, marked "CPU by hand" (§1.4, §3.2).
7. **Closed** (decision B7). The validating agent re-reads both price pages
   twice, before the first run and on the report's day, records fetch
   dates, prices and K2's billing state, and reports margins at both prices
   if one changed (§5.1, §7). Re-pricing is held below.
8. **Closed** (decision B8). The tolerances: exact for the posture,
   correctness, `boot_id` and RSS 450; tight for the WAL (B1); ±10% for
   linearity after the ST-4 fleet floor and for the cell anchors; ±5% for
   the Tigris reconciliation; the queue ceilings exact at 2 shards; the hot
   queue at 95% of the due rate per 5-minute window (§6 table).

Held for the owner (not decided under the delegation):

- **Price publication** (from item 3): the price page and terms (the $10
  figure, the free tier's limits, K2 parity) are customer-facing and
  financially binding; the decision fixes only the model.
- **Re-pricing** (from item 7): if a re-read price page differs, re-pricing
  is the owner's; the agent only reports both margins.
