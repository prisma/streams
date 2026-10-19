# Prisma Streams — Operator Runbook

How to build, configure, run, deploy, scale, monitor, and debug the service.
Everything here was exercised in the production pilot (Singapore,
1-CPU/1-GB Prisma Compute instances, Tigris object storage); numbers carry
their provenance. Architecture rationale lives in [SPEC.md](./SPEC.md) and
[COMPUTE-SPEC.md](./COMPUTE-SPEC.md); durability/security posture in
[OPERATIONS.md](./OPERATIONS.md); the full measurement history in
[EXPERIMENT-PILOT.md](./EXPERIMENT-PILOT.md).

---

## 1. Components

One crate, several binaries (`cargo build --release` builds them all):

| binary | role |
|---|---|
| `streams-slate` | the server: HTTP surface, shard engine, history tier, fleet member |
| `pilot` | swiss-army harness: `MODE=lb` routing load balancer, `MODE=gen` load generator, `MODE=bench` calibrated probes |
| `s3lite` | local S3 emulator with configurable latency (`--latency-ms`) and conditional-PUT support — the dev/CI store |
| `streams-keys` | generates stream encryption keys (32-byte base64) |
| `bench` | single-node benchmark matrix (see [bench/run_matrix.sh](./bench/run_matrix.sh)) |
| `livebench` | live end-to-end load harness (used for the PG-WAL invalidation stress) |
| `verify` | conformance/verification runner |
| `edgesim`, `cryptobench` | edge-behavior simulator; crypto micro-benchmarks |

There is no coordination service, no database, no local durable state. The
object store is the only thing that must survive.

## 2. Building

**Local development (any platform):**

```bash
cargo build --release          # native target, all binaries
cargo check                    # fast validation
```

**For Prisma Compute — x86_64 is mandatory.** Compute instances are x86_64
microVMs (Unikraft on Firecracker). An aarch64 binary deploys "successfully"
and then crash-loops at boot with `ENOEXEC`, which the platform surfaces as a
permanent zombie: the domain serves the platform 404, `versions list` says
`running`, and no logs are retrievable (we lost a fleet to this; see §10).

```bash
# one-time
rustup target add x86_64-unknown-linux-musl
cargo install cargo-zigbuild        # zig-based cross linker

# every release build for Compute
cargo zigbuild --release --target x86_64-unknown-linux-musl --bin streams-slate --bin pilot

# ALWAYS verify the architecture before uploading (e_machine 0x3e = x86_64)
xxd -s 18 -l 2 target/x86_64-unknown-linux-musl/release/streams-slate  # expect: 3e00
```

The aarch64-musl target is still useful — it is what the local Docker fleet
tests run on an ARM Mac (`--platform linux/arm64`). The two targets coexist;
never reuse one's upload for the other. The deploy wrappers (§8.2) verify ELF
magic and machine type after download as a backstop.

Toolchain note: musl builds use mimalloc as the global allocator — musl's own
allocator fragments badly under this workload (~4× RSS at identical load,
measured 2026-07-16).

## 3. Configuration reference

Every option is a CLI flag with an env-var alias (clap). Env names below are
what deployments set. **On Prisma Compute, always restate the complete
environment on every deploy** — see the §8.3 trap.

### 3.1 Object store

| env | default | notes |
|---|---|---|
| `SLATE_S3_ENDPOINT` | — (required) | e.g. `https://t3.storage.dev` (Tigris) or `http://127.0.0.1:9500` (s3lite) |
| `SLATE_S3_BUCKET` | `streams` | the bucket of every role (ops, shard logs, data). The arguments `--ops-bucket`, `--shard-bucket` and `--data-bucket` are not accepted (since 2026-09-29) |
| `SLATE_S3_REGION` | `us-east-1` | `auto` for Tigris |
| `SLATE_S3_ACCESS_KEY_ID` / `SLATE_S3_SECRET_ACCESS_KEY` | `test` | |
| `PATH_PREFIX` | — | key prefix inside the bucket; independent deployments can share a bucket. **Changing it = a fresh, empty keyspace**. One value, given as the variable or as `--path-prefix` (the argument wins), places the stores, the usage rollup and the usage read spool (§11) |

Provider requirements (strong read-after-write, conditional PUT/If-Match,
durability): [OPERATIONS.md §1](./OPERATIONS.md). Tigris satisfies all of
them and negotiates HTTP/1.1 (relevant to §10's latency story). The client
keeps its connection-pool idle timeout at 4 s deliberately: the platform
silently kills flows idle ≳5 s, and a restored scale-to-zero image must wake
with an empty pool rather than dead sockets.

### 3.2 Engine (shard log)

| env | default | guidance |
|---|---|---|
| `INITIAL_SHARDS` | 1 standalone; in fleet mode the largest power of two at most `FLEET_MAX` (4 for the default `FLEET_MAX=4`) | power of two. Every shard is its own SlateDB and WAL writer, and a busy writer pays its own WAL PUTs whatever it carries, so run as few as the load needs: a fleet that sets more than `FLEET_MAX` is warned at boot (`INITIAL_SHARDS=N > FLEET_MAX=M: ...`). Until 2026-10-07 the fleet default was `next_pow2(4 × FLEET_MAX)` (16 for four servers; the pilot fleet ran 16). Until `home-v1` placement exists the rendezvous draw spreads the new default unevenly (4 over 4 servers draws `[1, 1, 0, 2]`), so the fleet rigs and the §7 docker recipe pin their own counts (16 over four servers, 8 over two or three) and are warned. Set at keyspace creation; topology is stored |
| `FLUSH_INTERVAL_MS` | 25 | WAL flush cadence = the ack floor (flush + one PUT ≈ 40 ms on Tigris at 25 ms). 50 ms halves WAL-object churn for ~10 ms of ack; 5 ms mints WAL SSTs faster than GC reaps them and degrades the watermark to ~0.3–1 s — do not go below 25. It is the cadence only with `WAL_GROUP_COMMIT=0`. With the pump (the default) it is read only as the gap when `WAL_FLUSH_GAP_MS=0`; SlateDB's own timer is then the fixed 60 s failsafe (below), whatever this says (until 2026-10-07 it was this value stretched to ≥1 s) |
| `WAL_GROUP_COMMIT` | 1 | per-shard pump flushes the WAL the moment the previous flush completes when commits are waiting, instead of on the fixed tick; 0 = SlateDB's fixed tick at `FLUSH_INTERVAL_MS` (the default until 2026-09-29). Sparse/moderate appends stop paying tick alignment (local A/B vs 25 ms s3lite: sequential append p50 55→28 ms, durable_wait 47→27 ms); at saturation both modes self-clock to the PUT RTT (equal). Skips entirely when nothing awaits durability, so idle churn is zero |
| `WAL_FLUSH_GAP_MS` | 100 (0 = `FLUSH_INTERVAL_MS`) | pump-mode floor on start-to-start flush spacing: the one write tier (owner decision of 2026-10-07; 10 until then). With the 1 ms herd-settle, a shard's flushes start at least 101 ms apart, so a busy shard writes at most ~9.9 WAL objects/s. An append waits for the rest of the gap since its shard's last flush started, then the settle and one WAL PUT: on a shard whose last flush started at least 100 ms ago, settle + PUT only; a producer with one append in flight gets one ack per ~101 ms (the PUT overlaps the gap while it is shorter). Only binds when the PUT RTT is faster than the gap. Lowering it buys acknowledgement latency with more WAL PUTs on busy shards |
| `L0_SST_SIZE_BYTES` | 32 MiB | pilot used 8 MiB on 1-GB instances |
| `MAX_UNFLUSHED_BYTES` | 16 MiB | per-shard byte backpressure. SlateDB's default is 512 MB — a byte flood OOMs a 1-GB box before backpressure fires; keep this small |
| `L0_MAX_SSTS` | 32 | L0 count that triggers write backpressure. An L0 costs a stored object, not memory; 8 (the default until 2026-09-29) stalled batch ingest. It is also SlateDB's per-key L0 cap: a totally-ordered stream rewrites one meta row per memtable, so every L0 overlaps on that key and a lower per-key cap would be the real dispatch gate (the upstream default of 8 stalled the flusher). `L0_MAX_SSTS_PER_KEY` is not read (since 2026-09-29) |
| `MANIFEST_POLL_MS` | 2000 | also how the flusher learns compaction freed L0 slots; loaded shards want 1000–2000. 60 s polls produced 14 s flush stalls. Each poll is a live Tigris 404 probe (~200-240 ms Tigris-internal) — the default IS the idle-cost posture, deploy scripts must not re-tighten it (docs/TIGRIS-404-COST.md; DST-pinned) |
| `COMPACTOR_MAX_CONCURRENT`, `COMPACT_MAX_SUBCOMPACTIONS`, `COMPACT_MAX_FETCH_TASKS`, `COMPACT_BYTES_TO_FETCH`, `COMPACT_MAX_SST_SIZE_BYTES`, `STORE_BULK_INFLIGHT_MAX_BYTES` | 1, 1, 1, 1 MiB, 32 MiB, 32 MiB | the certified 1 GiB posture (`deploy/profiles/compute-1g.env`), the default since 2026-09-29. SlateDB's own worker values (4, 4, 4, 2 MiB, 256 MiB) with the bulk gate off stage about 1 GB for a 32-input L0 merge. A larger instance class raises them; batch ingest gains about a third from the larger values. `COMPACTOR_MAX_CONCURRENT` is also the argument `--compactor-max-concurrent`: the argument wins over the variable, and either one reaches the compactor of every database (shard, history, telemetry, rollup, read spool). The other five are read from the environment only. With `MEMPROFILE_CERT=compute-1g` a concurrency other than 1, from either source, refuses the start |
| `COMPACTOR_POLL_MS` | 2500 | compactions-log probe cadence = the largest idle-404 class (the old 500 ms pin was 8 probes/s/instance forever, pre-limiter era). At 5 MB/s/shard a 2.5 s scheduling gap bounds L0 accumulation to ~3 SSTs vs `L0_MAX_SSTS` 64; drain continuity comes from `COMPACTOR_MAX_CONCURRENT`, not scheduling latency (docs/TIGRIS-404-COST.md; DST-pinned) |
| `TRIM_PER_OP` | 8192 | hot-log records retired per absorb commit; must outpace ingest (at 50k rec/s and one pass per 5 s a pass must retire ~250k) |
| `ABSORB_BYTES` / `ABSORB_AGE_SECS` | 4 MiB / 60 | absorber thresholds into the history tier: a tail is absorbed at 4 MiB, or once it is 60 s old (the age was 300 until 2026-09-29) |
| `ABSORB_GATHER_MAX_BYTES` / `ABSORB_READ_PAR` | 8 MiB / 8 | active v2 gather packing limit and concurrent frame reads within one gather (32 MiB until 2026-09-29). A gather stops staging at the limit; one oversized chunk still proceeds alone. The process budget may clamp the packing limit |

**Fixed values (not settings, since 2026-09-29).** Garbage collection of the
shard databases runs at constants of the binary; no argument and no variable
sets them, and a process that holds one of the old names
(`WAL_GC_INTERVAL_SECS`, `WAL_GC_MIN_AGE_SECS`,
`COMPACTIONS_GC_INTERVAL_SECS`, `COMPACTIONS_GC_MIN_AGE_SECS`,
`GC_QUIET_INTERVAL_SECS`, `HISTORY_GC_INTERVAL_SECS`) runs the same values.
Since 2026-10-07 so does SlateDB's WAL failsafe under the pump (last row),
which `FLUSH_INTERVAL_MS` set before:

| what | value | why |
|---|---|---|
| WAL sweep interval / minimum age | 30 s / 60 s | tighter than upstream (60/300): a loaded shard mints ~20 WAL SSTs/s and the WAL prefix must stay small — GC lists share the path with ack-critical PUTs. The minimum age must cover shard-move replay (<1 s; 60 s is generous) |
| compactions-log sweep interval / minimum age | 30 s / 120 s | tighter than upstream (60/300): every compactor state change mints a `.compactions` version and shard OPEN pages through the survivors — at cross-region latency that class fed the eu-central-1 slow-open hang (docs/SOAK-REGIONS.md; upstream slatedb#1970). Only superseded versions below the GC boundary are reaped |
| manifest, compacted and WAL-fence sweep interval | 600 s | the quiet directories: reclamation latency is traded for LIST steady-state (docs/TIGRIS-404-COST.md) |
| history GC sweep interval | 600 s | every history database, for the same reason |
| SlateDB's WAL flush timer on a shard log under the pump (the failsafe) | 60 s, plus a fixed per-shard offset below 30 s | since 2026-10-07 (owner decision, the write tier; 1-1.5 s before). Every commit wakes the pump, so no acknowledgement waits for this timer; a tick that finds commits waiting out the pump's gap in the WAL buffer writes them early as one more WAL object (about 0.8/s on a busy shard at 1 s). The offset (`bootstrap::run`, O14a) staggers shards: the single shard (prefix `""`) ticks every 76.3 s, shards `00`/`01`/`10`/`11` every 79.4/71.8/66.6/74.2 s. Under `WAL_GROUP_COMMIT=0` the timer is the tick, `FLUSH_INTERVAL_MS` |

### 3.2b Service limits, usage telemetry, billing

| env | default | notes |
|---|---|---|
| `LIMIT_BYTES_PER_SEC` | 5000000 (5 MB/s) | per-stream-shard ingest byte limit (token bucket; finite, >= 0; 0 disables) |
| `LIMIT_REQS_PER_SEC` | 1000 | per-shard append-request limit (same rule) |
| `LIMIT_RECS_PER_SEC` | 5000 | per-shard record limit (same rule) |
| `LIMIT_BURST_SECS` | 2 | bucket capacity = rate x this (finite, > 0). Every ENABLED bucket must hold >= 1 token (rate x burst >= 1) and the product must be finite: a posture that cannot admit one unit, or that would silently disable a limit, is refused at boot by `validate()` |
| `USAGE_STREAM_KEY` | — | base64url 32-byte key for the `_usage`/`_ops_*` system ledgers; unset = telemetry pipeline off |
| `BILLING_MODE` | `off` | `required` = refuse to serve without ledger key, real identities, an open read spool (and rollup DB on the rollup owner) |
| `ACCOUNT_ID` / `PROJECT_ID` / `CELL_ID` | `acct_local`/`proj_local`/`local` | the cell's tenant identity (one project per cell); placeholders are refused in required mode |
| `ROLLUP` | — | `1` = this instance runs the usage rollup consumer + month closer |
| `TELEMETRY_DRAIN_SECS` | 8 | drain cadence: sealed reads + dirty snapshots -> `_usage`, and the ops, audit and fleet-event journals; usage reaches the usage answers up to one cadence (plus the rollup's 2 s poll) after it is metered, reads only once their 10 s window has sealed. A graceful stop's terminal round gets one cadence and at most 5 s (edge change #122), so any value stays inside the 10 s supervisor grace. 2 before the write tier (edge change #117) |
| `OUTBOX_SWEEP_SECS` | 300 | owned-shard outbox sweep + billing tombstone walk cadence |
| `FORK_DEBT_SWEEP_SECS` | 300 | pause between fork-debt reconciler circles (releases source references deleted forks still owe; a backlog, and the one-time backfill, drain back to back); alert `fork_debt_stale` fires after 3 periods (§8) |
| `MONTH_CLOSE_GRACE_MS` | 86400000 | wait after a month boundary before closing it |
| `METRICS_INTERVAL_SECS` | 15 | `_ops_metrics` snapshot cadence |
| `ALERT_USAGE_OUTBOX_DIRTY` | 1000 | unacked usage snapshots that open the outbox-lag alert |
| `ABSORB_GLOBAL_BUDGET_BYTES` | 100859904 | active PROCESS-WIDE absorber gather budget; every gather reserves (estimate x build multiplier) BEFORE reading pages. The default is one worst-page build at the 32 MiB body ceiling, (32 MiB + 64 KiB) x 3; a smaller value is raised to that build for the server's own ceiling |
| `ABSORB_GLOBAL_GATHERS` | 1 | active concurrent-gather ceiling, process-wide and further bounded by the byte budget. 2 until 2026-09-29; a gather reserves its adaptive estimate (about 12 MiB at the floor), so at 2 slots two gathers did run at once. Do not raise it on a 1 GiB instance (4 slots were OOM-killed, docs/CHAOS-CAMPAIGN.md CHAOS-5) |
| `TELEMETRY_CACHE_BYTES` | 16777216 | ONE bounded cache shared by the read-spool and rollup SlateDB DBs (they must never inherit SlateDB's per-DB defaults) |
| `SLATEDB_RT_THREADS` | 4 | worker threads of the dedicated SlateDB runtime (two-runtime split). 4 is what every deployed family runs; 2 was the default until 2026-09-29. Tests that open storage directly run on 2 |

Transient rejections are 429s with error codes `limit_bytes_per_sec` /
`limit_requests_per_sec` / `limit_records_per_sec`, a human message naming
the limit, and a `Retry-After` header the bucket will honour. A request
larger than a FRESH bucket (body bytes or record count above rate x burst)
can never be admitted and is a permanent 413 `payload_too_large` with no
`Retry-After`, on every content append and on both surfaces: the raw message
names the requested size and the capacity, and the product error adds them
as `details {dimension, capacity, requested}`. `/v1/debug/usage` (bearer) exposes
per-stream cumulative requests, records, bytes_in, bytes_out,
plaintext_bytes, frame_bytes (since layout 5 the stored page bytes, after
compression), and the derived compression ratio. The
billing emitter appends a JSON array per interval to the billing stream —
one record per active stream with DELTA requests/records/bytes_in/bytes_out
plus cumulative plaintext/frame byte totals (stored-volume-pre-compression
and achieved compression rate are derivable; join hash->name via the
registry's by-name objects). The billing stream's own usage is excluded.

### 3.3 Memory & runtime (1-GB instance discipline)

| env | default | guidance |
|---|---|---|
| `SHARED_CACHE_BYTES` | 128 MiB | ONE block cache shared by all shard DBs (192 MiB until 2026-09-29). SlateDB's per-DB default is 512 MB — 16 shards × 512 MB on a 1-GB box dies by cache fill in tens of minutes (this *was* our "platform kills instances" mystery). The shared cell sets 32 MiB (`deploy/profiles/shared-cell.env`): cache-on-write keeps it full under any write load, and at 128 MiB its blocks and the allocator pages they pin held a one-shard cell over the shed line under Layer B's compliant load |
| `HISTORY_CACHE_BYTES` | 32 MiB | shared cache for history-tier/absorber DBs; the shared cell sets 16 MiB, for the same reason |
| `TOKIO_WORKERS` | max(2, cores) | **do not run one worker.** On 1-vCPU instances the old `#[tokio::main]` default was a single worker; inline blocking quanta (SST build/compress) froze every future including commit acks — the O14a saga. The floor of 2 is enforced in code; the pilot runs 3. Measured effect at identical load: ack-excursion windows 30 % → 10 %, median-window WAL-PUT p99 617 → 141 ms |

Memory budget on 1-GB/~750-MB-kill-line instances (revised 2026-07-21 after
the saturation gate OOM'd the old envelope): the old numbers — shared cache
192 + history cache 32 + unflushed 16×16 + absorber pass 32 + HTTP ≈ 700 MB
with shed at 800 — left NO headroom below the platform's ~750 MB kernel
kill line and put the shed above it. When a fast substrate let the closed
loop sustain 1.3–1.6k req/s and a flush stall piled memtables up, the
catch-up burst crossed the line (4 OOM kills in one 18-min run; the same
morning's slower substrate had paced the identical binary to a survivable
266 req/s). The envelope that gates green:

- `SHARED_CACHE_BYTES` 128 MiB, `MAX_UNFLUSHED_BYTES` 16 MiB ×16 shards
  (do NOT halve it: 8 MiB doubled the L0 mint rate and turned OOM risk
  into an 8-minute L0-full flush wedge), `L0_MAX_SSTS` 32 for
  compaction-lag headroom (L0 count costs S3 objects, not RAM), absorber
  pass 32 MiB;
- `ADMIT_RSS_SHED_MB` 500, the default (this envelope first ran 550; the
  OOM review of 2026-08-07 lowered it) — below the kill line with margin,
  so the shed fires while the process can still serve;
- note: before 2026-07-21 the RSS sampler only ran in fleet mode, so the
  shed was silently DEAD in standalone deployments (compared against a
  frozen 0). Fixed — the sampler is unconditional now.

### 3.4 Auth, crypto, metrics

| env | default | notes |
|---|---|---|
| `AUTH_TOKEN` | — | when set, all `/v1/*` requires `Authorization: Bearer <token>`. `/health` is always open |
| `USAGE_STREAM_KEY` | — | (see billing table above) system-ledger key; the old `__metrics__`/`METRICS_KEY` plane is deleted |
| `INSTANCE_NAME` | `streams` | instance tag in metrics + fleet heartbeats (`streams-1`…) |

Stream keys: `streams-keys generate` → 32-byte base64. Clients pass it as
`Stream-Encryption-Key` on create and on every data-path request. The
service holds it in memory for the request only. Losing the key = losing the
data (by design); rotation and compromise runbooks are
[OPERATIONS.md §3.3](./OPERATIONS.md).

### 3.5 Fleet & autoscaling

Set `FLEET_PREFIX` to enable fleet mode; without it the server runs
standalone.

| env | default | meaning |
|---|---|---|
| `FLEET_PREFIX` | — | shared coordination prefix: heartbeats (`fleet/<instance>.json`, every 2 s), `fleet/desired.json`, router reports (`routers/<name>.json`) |
| `FLEET_MAX` | 4 | hard fleet-size cap (cost ceiling) |
| `SCALE_OUT_CPU_PCT` | 75 | scale-out target: fleet grows when measured utilization approaches this |
| `SCALE_IN_CPU_PCT` | 50 | shrink only if post-shrink utilization would stay below this (the 75/50 gap prevents flapping) |
| `SCALE_CPU_SUSTAIN_SECS` | 20 | hot-instance breach must persist this long (shard handoffs spike CPU briefly) |
| `SCALE_LATENCY_MS` / `SCALE_LAT_SUSTAIN_SECS` | 250 / 20 | ack-latency dimension: while the highest ack p50 among the instances that serve at least 5 requests per second stays above the threshold for the sustain time, this dimension asks for one instance more than are live. It catches congestion that shows as neither CPU nor in-flight requests (object-store slowness, for example) |
| `SCALE_EDGE_SLOTS` | 140 | per-instance ingress-concurrency capacity through the platform front door (two-layer model, [PLATFORM-EDGE-REPORT.md](./PLATFORM-EDGE-REPORT.md)). Post-Conduit-fix guidance: recalibrate toward ~250 |
| `SCALE_EDGE_LATENCY_MS` | 1000 | router-observed *client* latency breach: adds an instance AND blocks scale-in (server-side metrics cannot see client pain) |
| `SCALE_IN_SECS` | 60 | hysteresis before any shrink |

Desired count = **max over all dimensions** (utilization, in-flight/slots,
hot instance, ack latency, router-observed edge latency), clamped to
`FLEET_MAX`. Every dimension exists because a specific incident demanded it;
the reason string is logged on every change. The model is generalized in
[AUTOSCALING-DESIGN.md](./AUTOSCALING-DESIGN.md).

The fleet tick reads its members by name, with conditional GETs (an
unchanged document answers 304, which is not billed): `fleet/desired.json`,
then the heartbeats of the ordinals its count names, of every member it has
read before, and its own. It lists `fleet/` and `routers/` only on its first
pass and every 30th after it (about once a minute), to find the members no
count names and the router reports, so a member above the count that boots
between listings counts toward `live` from the next listing, and its planned
drain waits up to that long for its peers to read it. An absent ordinal
within the count costs a billed 404 per pass; when more than nine are
absent, the pass lists instead. `FLEET_MAX` does not change what the tick
reads.

### 3.5b Stream auto-scaling (segments) & shard rebalancing

Per-stream Pravega-style segment splits/merges and absorb-lag-driven
shard moves — design and validation ladder in
[docs/SCALING.md](./docs/SCALING.md). Opt a stream in with
`Stream-Scaling: auto` at create; appends then require a `Stream-Key`
routing key and are routed server-side to internal child streams
(`<name>#<seg>`; invisible to readers of the segment map API).

| env | default | meaning |
|---|---|---|
| `SCALE_EVAL_SECS` | 10 | scaler evaluation cadence |
| `SCALE_RATE_WINDOW_SECS` | 120 | EWMA window for per-segment rates (Pravega's two-minute rate) |
| `SCALE_HOT_PCT` | 75 | split above this % of the per-segment service limits; a segment is cold below 5% of it (3.75% of the limits at the default) |
| `SCALE_HOT_EVALS` | 2 | consecutive hot evaluations before a split; a stream merges after every segment stayed cold for four times as many (8) |
| `SCALE_COOLDOWN_SECS` | 600 | min segment age before it may re-scale |
| `REBALANCE_LAG_SECS` | 60 | absorb-lag (s) beyond which the laggard moves a shard to a peer |
| `REBALANCE_MOVE_COOLDOWN_SECS` | 60 | min gap between moves per host |

Shard moves are published to `fleet/overrides.json` (CAS) and win over
the rendezvous ring while the target is live. Splits survive scaler
crashes: the append path re-seals and publishes the missing transition
itself (`resume_split`). `SCALE_FAULT_POINT=after_seal` and
`ABSORB_PAUSE=1` are test-only fault hooks (docker ladder D3/D4).

While the server runs in a fleet (`FLEET_PREFIX` set, once its fleet loop
has published a ring, of any size), the scaler splits nothing (edge change
#119): a split's child lands on another shard, which is another server's
now or once the ring grows, and that server does not yet serve it
correctly. A hot stream stays one segment on its owner, and its appends
past the per-stream limits get `429 limit_*_per_sec`; each declined split
logs `split declined: the server runs in a fleet`. Merges still run, and a
server with fleet off splits as before.

### 3.6 Admission control (run with these ON in production)

| env | default | pilot value | behavior |
|---|---|---|---|
| `ADMIT_MAX_INFLIGHT` | 512 (0 = off, the default until 2026-09-29) | 256 | above this many in-flight requests, an authenticated append gets `429 + Retry-After: 1` after a 25 ms tarpit. The count covers every request on every route, parked long-polls included; above four times the cap (2,048 at the default) every `/v1/stream` and `/v1/streams` request, reads included, gets `503 + Retry-After: 1` before authentication, with no tarpit. Direct-path instance capacity measured at ~510 concurrent; 256 is the guarded setting for router-fronted 1-CPU boxes |
| `ADMIT_MAX_INFLIGHT_PER_STREAM` | 256 (0 = off; 64 until 2026-09-29) | — | above this many concurrent appends on one stream segment, a further append to it gets `429 stream_overloaded` + `Retry-After: 1` (product: `429 rate_limited`), with no tarpit, so that one hot stream cannot take every slot of the instance cap. Where `ADMIT_MAX_INFLIGHT` is lowered to 256 or less, set this below it: at the default it would equal the instance cap |
| `ADMIT_RSS_SHED_MB` | 500 | 500 | appends (clients' and the server's own to its usage, audit and ops streams; not creates or deletes) get `429 + Retry-After: 2` after a 25 ms tarpit while sampled RSS plus the absorber's reserved bytes exceeds this (counted in MiB). Without it a 1-GB box OOM-dies at full throughput instead of shedding. The first pilot's 800 sat above the ~750 MB kill line and protected nothing; 0 turns it off |
| `PROJECT_SHARE_K` | 1 (a dedicated cell) | — | k, the number of projects a shared cell is sized for (shared cells: 8). Each project's quota on every shared bound (the three envelopes below, `ADMIT_MAX_INFLIGHT`, the effective `SSE_MAX_CONNECTIONS`, the 65,536-entry per-stream maps, `MAX_UNABSORBED_BYTES_PER_INSTANCE`) is at most ⌊bound ÷ k⌋, and a 0 or missing quota takes exactly that; above 1 the deployment's `ACCOUNT_ID` is reserved too (its `PROJECT_ID` always is): a feed policy naming either is dropped and its tokens get `421 wrong_cell`. Must be at least 1 |
| `CELL_ENVELOPE_REQUESTS_PER_SEC` / `CELL_ENVELOPE_APPEND_BYTES_PER_SEC` / `CELL_ENVELOPE_READ_BYTES_PER_SEC` | 0 (no shared bound) | — | the requests, appended bytes and read bytes per second the cell was measured to carry, divided by `PROJECT_SHARE_K` into each project's rate ceilings. Set only measured values: a bound set too low caps honest tenants |
| `PROJECT_STORED_BYTES_DEFAULT` | 0 (no stored-bytes limit) | — | the `max_stored_bytes` a project gets when its policy sets 0 or none, at any k: a default, not a bound ÷ k, and a policy's own value stands (shared cells: 10737418240, 10 GiB, the owner's answer S1 of 2026-10-12; `cell-admin` and the issuer publish it explicitly at admit, and `cell-admin quota --project P --quota max_stored_bytes=N` gives one project more). A project whose stored bytes (stored page bytes after compression, page headers included, each live segment once) reach its quota is refused new appends `429 stored_bytes_limit`, not retryable, no `Retry-After`, until a delete or a larger quota brings it under; reads, creates, deletes, seals and consumers are never refused on it (edge change #158). Until physical reclamation exists a delete frees quota but not the bucket's bytes; `rollup.stored.writtenBytes` watches those a drain round saw ("What to alert on", Shared cells). The figure is the one the usage rollup last published (`/operator/billing.json` `rollup.stored`), so only the `ROLLUP=1` instance enforces it (a multi-server cell must distribute it first), and it trails the store by a drain round and a rollup step: about 10 s while a shard dirties at most 256 segments a round, minutes at tens of thousands of written streams, and the whole of a rollup stall ("Billing pipeline runbooks"). Under `BILLING_MODE=required` (the shared-cell profile) the rollup publishes before the server binds; in any other mode it opens inside its task after serving begins, and the cap refuses nothing until it has. Ops events (`_ops_events`, `/v1/debug/ops-events`): `project_stored_cap_reached` (warn) when a project is first refused, `project_stored_cap_cleared` (info) when it is next admitted, never one per refusal; their count a day is a product signal, not a page |

The A/B is stark (run 11): identical overload, guards off = all four
instances dead in ~2 minutes; guards on = zero deaths, zero stalls, client
p90 5.4× better at 2× the offered load.

**Client contract**: clients MUST honor `Retry-After` with jitter. A client
that retries 429s instantly creates a reject storm that starves the
instance's own health checks (measured: 2.7 M rejects, goodput ~1/s). The
`pilot` generator implements the required behavior (`Retry-After` + per-task
jitter).

## 4. Running locally

### 4.1 Single node against s3lite

See the README quick start. s3lite flags: `--listen`, `--latency-ms`
(simulated per-op latency; 5 ms for fast dev, 25 ms ≈ Tigris-realistic),
`--discard-substr` (fault injection: silently discard matching PUTs).

### 4.2 A full fleet in Docker (resource-realistic)

Replicates the cloud shape on one machine — 1-CPU/1-GB cgroups, real
concurrency, s3lite behind it:

```bash
cargo zigbuild --release --target aarch64-unknown-linux-musl --bin streams-slate  # ARM Mac; use x86_64 on x86
mkdir -p /tmp/docker-fleet && cp target/aarch64-unknown-linux-musl/release/streams-slate /tmp/docker-fleet/
cat > /tmp/docker-fleet/Dockerfile <<'EOF'
FROM alpine AS certs
RUN apk add -U ca-certificates
FROM scratch
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY streams-slate /streams-slate
ENTRYPOINT ["/streams-slate"]
EOF
docker build -t streams-slate:local /tmp/docker-fleet
./target/release/s3lite --listen 0.0.0.0:9500 --latency-ms 25 &
for i in 1 2 3 4; do
  docker run -d --restart on-failure --name streams-$i --cpus 1 -m 1g -p 910$i:8080 \
    -e SLATE_S3_ENDPOINT=http://host.docker.internal:9500 -e SLATE_S3_BUCKET=streams \
    -e SLATE_S3_REGION=auto -e SLATE_S3_ACCESS_KEY_ID=t -e SLATE_S3_SECRET_ACCESS_KEY=t \
    -e PATH_PREFIX=dockerfleet -e FLEET_PREFIX=dockerfleet -e INSTANCE_NAME=streams-$i \
    -e INITIAL_SHARDS=16 -e AUTH_TOKEN=devtoken -e TOKIO_WORKERS=3 \
    -e ADMIT_MAX_INFLIGHT=256 -e ADMIT_MAX_INFLIGHT_PER_STREAM=64 \
    streams-slate:local --listen 0.0.0.0:8080
done
```

Run `pilot` with `MODE=lb` in front (`UPSTREAMS=http://localhost:9101,…`)
and `MODE=gen` against it for load. `--restart on-failure` matters: an
OOM-killed container under load otherwise stays down.

## 5. The HTTP surface (operator view)

| endpoint | auth | purpose |
|---|---|---|
| `GET /health` | none | liveness (`ok`). Health checks and wake pings use this |
| `GET /v1/streams` | bearer | list streams |
| `PUT /v1/stream/{name}` | bearer + `Stream-Encryption-Key` | create (400 `missing_key` without the key header) |
| `POST /v1/stream/{name}` | bearer + key | append (`{"events":[…]}`); 204 on durable commit |
| `GET /v1/stream/{name}?…` | bearer + key | read/tail (offsets, long-poll, SSE; profile-specific routes per [PROFILES.md](./PROFILES.md)) |
| `GET /v1/debug/timings` | bearer | per-shard commit-pipeline rings: `queue_wait_us`, `encode_us`, `write_us`, `durable_wait_us` per group — splits our pipeline from store waits |
| `GET /v1/debug/load` | bearer | `inflight_now`, `inflight_peak` (swap-on-read), `rss_mb`, `admit_shed` |
| `GET /v1/debug/store?window=60&swap=1` | bearer | per-(op,class) object-store latency cells (`put:wal`, `get:manifest`, …: n/err/p50/p90/p99/max), slow-op ring (≥300 ms with paths), outbound in-flight gauge, **timer sentinels** (`timer_thread`, `timer_tokio` drift) and `steal_pct`. `swap=1` resets the gauge peak — samplers only |
| `GET /v1/debug/sleep?ms=N` | bearer | calibrated-latency probe (≤5000 ms): separates concurrency caps from rate caps at the edge |

Every `/v1/debug` path, an unrouted one included, answers 401 without the
deployment bearer (`src/http/debug.rs`), as `/operator` does (§8); only
auth mode off with no `AUTH_TOKEN` set, for local development, leaves it open.

**429 semantics**: body `{"error":{"code":"overloaded"…}}` with
`Retry-After: 1` (in-flight shed) or `2` (RSS shed), after a 25 ms tarpit.
The per-stream cap answers `stream_overloaded` with `Retry-After: 1` and no
tarpit. Above four times `ADMIT_MAX_INFLIGHT` every `/v1/stream` and
`/v1/streams` request is refused before authentication with `503
overloaded`, `"retryable": true`, `Retry-After: 1` and no tarpit (edge
change #155).
Sustained 429s are the *designed* behavior under overload — the alternative
was death (§3.6).

### Reading `/v1/debug/store` (the 5-minute diagnosis)

- `put:wal` p99 spikes alone, timers tight → provider tail latency.
- ALL classes elevated + `timer_tokio` drift ≫ `timer_thread` drift → the
  event loop is blocked: check `TOKIO_WORKERS`, then look for new inline
  blocking work (this exact signature root-caused O14a).
- `timer_thread` drift also high or `steal_pct` > a few % → host/vCPU
  contention: platform conversation.
- Timers tight, classes flat, but acks slow in `/v1/debug/timings`
  (`durable_wait_us` ≫ `write_us`) → our pipeline; file an engine bug.
- `wal_read_storm.stalled` true — thousands of `get:wal` with **zero**
  `put:sst` and **zero** `delete:wal` → compaction has stopped, the WAL is
  never trimmed, and readers are scanning it directly. Self-reinforcing:
  the reads eat the outbound budget and starve the appends that would
  advance the WAL. Took eu-central-1 out of the 2026-07-26 soak
  ([docs/SOAK-REGIONS.md](./docs/SOAK-REGIONS.md)). Page on it.
- `history_readers` hits ≪ misses, or `stale_reopens` tracking request
  rate instead of absorb cadence → the history reader cache is not
  absorbing per-request metadata traffic; every miss is a fresh DbReader
  open (manifest reads + a checkpoint write) on the user-visible read
  path.
- `served_from` shows a large share from a **remote** PoP (>10 % non-local)
  → the provider is routing this bucket's traffic out of region. Every op
  carries the extra RTT; correlate before blaming our pipeline.

## 6. Fleet mode: how it actually works

- **Heartbeats**: every 2 s each instance's `fleet-heartbeat` task PUTs
  `<FLEET_PREFIX>/fleet/<instance>.json`: rps, ack_p50_ms, cpu_pct
  (getrusage), inflight/inflight_peak, rss_mb, wal_put_p50/p99_ms,
  out_inflight/peak, owned_shards, boot_id, `progress_age_ms` (how long
  ago its fleet tick last published an ownership view) and, when it takes
  no new ownership, `withdrawn` (why: a lost Critical loop, a cell failure,
  or the runtime stopping; a stopping runtime's last beat always carries
  it). Staleness > 10 s = not live. The ring drops an instance whose
  heartbeat is > 30 s old, whose `progress_age_ms` reached 139 s (a live
  process whose controller is stuck), that published `withdrawn`, or that
  is `draining`. Each beat also carries its per-boot `seq` and, in
  `viewed`, which draining beat of each live draining peer its current view
  read and left out.
- **Planned drain** (item 40): SIGTERM/SIGINT to a fleet instance first
  drains it, before any loop is cancelled. It publishes `draining` at once,
  keeps beating and serving (the listener stays open; `/health` answers
  503 `runtime draining`, or a critical loop's failure if one fails
  meanwhile, and the draining beat's `withdrawn` says the same or names a
  cell failure), yields its shards as every ring drops it, and waits until
  it holds nothing (no shard, no open still running, no close unsettled)
  and every peer whose views keep being published has published one that
  read its drain and left it out (`viewed`). The log line names the
  outcome: `planned drain complete`; `no peer can take ownership through a
  drain` (no other ring member within the desired count, or a peer of an
  earlier version, which would keep routing here: then nothing is
  announced; or every peer went away during the drain); `planned drain
  failed` (a shard close failed, which waiting cannot change); or `planned
  drain timed out` with what was pending. The drain is bounded at 144 s,
  derived from what it waits for, and the process stop at 175 s in all
  (armed when the drain begins, even for a drain that ends at once); the
  wrapper kills at 180 s. A second signal during the drain is not observed
  (the signal loop ends after the first); SIGKILL stops at once, with no
  ordered stop, as any kill does.
- **Desired count**: any instance may write `fleet/desired.json`; the
  computation is deterministic from heartbeats so writers agree.
- **Placement**: rendezvous hash (FNV-1a over `"<shard> <instance>"`) across
  the first `desired` instances, computed identically by servers and LBs —
  the live set IS the assignment. No shard directory, no lease service.
- **Fencing**: opening a shard fences the previous owner via CAS on the
  shard manifest; a fenced owner's next write fails cleanly. Routing errors
  cost a retry, never corruption. A just-fenced shard is held off 3 s
  (anti-flap) → brief 503s during handoff are normal.
- **Waking**: the LB sends out-of-band `/health` pings to desired-but-stale
  ordinals every poll. **Routing must never be the wake mechanism** — a
  filtered ring never routes to dark instances, so they'd never wake
  (run-5 deadlock: desired=4, live=1, forever).
- **Router reports**: each LB publishes `routers/<ROUTER_NAME>.json` with
  worst-upstream client-latency EWMA; breach adds an instance and blocks
  scale-in (run 7: the fleet once scaled IN during client congestion,
  because delivered rps falls when clients queue).

The `pilot` LB (`MODE=lb`) is itself stateless: `UPSTREAMS` (full candidate
list), `FLEET_PREFIX`/`DATA_PREFIX`, `ROUTER_NAME`, and the same S3 creds
(`S3_ENDPOINT`, `S3_BUCKET`, `S3_REGION`, `S3_ACCESS_KEY_ID`,
`S3_SECRET_ACCESS_KEY` — note: *not* the `SLATE_`-prefixed names). If the
LB's fleet view uses a plain-http store (local testing), it needs
`allow_http` — a silent misconfiguration here once routed a whole fleet to
instance 1.

## 7. Deploying on Prisma Compute

### 7.1 Shape

N server services (`streams-1…N`), M pilot-LB services (`lb-1…M`), all in
one project/region. Servers carry `FLEET_PREFIX`; LBs front them; clients
hit the LBs. Compute wakes instances on request and sleeps them when idle
(`KEEP_AWAKE=1` via the wrapper's guard opts out during soak tests — never
leave it on after a test; it bills continuously). The one exception is a
shared cell, kept awake for good ("Shared cells" below).

### 7.2 The wrapper app

Compute runs a Bun app; ours downloads the Rust binary and execs it. The
canonical wrapper (server variant; generator variant is identical with
`GEN_BINARY_URL`/no `--listen`):

```ts
import { chmod } from "node:fs/promises";
import { KeepAwakeGuard } from "@prisma/compute";
if (process.env.KEEP_AWAKE === "1") new KeepAwakeGuard();
const bin = "/tmp/streams-slate";
const f = Bun.file(bin);
if (!(await f.exists()) || f.size < 1_000_000) {
  console.log("downloading binary...");
  const r = await fetch(process.env.SERVER_BINARY_URL!);
  if (!r.ok) throw new Error(`binary download: ${r.status}`);
  await Bun.write(bin, r);
  const head = new Uint8Array(await Bun.file(bin).slice(0, 20).arrayBuffer());
  const machine = head[18] | (head[19] << 8);
  console.log(`downloaded ${Bun.file(bin).size} bytes, e_machine=${machine}`);
  if (!(head[0] === 0x7f && head[1] === 0x45 && head[2] === 0x4c && head[3] === 0x46))
    throw new Error("downloaded file is not an ELF binary");
  if (machine !== 0x3e) throw new Error(`ELF machine ${machine} is not x86_64 (62)`);
}
await chmod(bin, 0o755); // no `chmod` executable in the Compute image
const port = process.env.PORT ?? "8080";
const proc = Bun.spawn([bin, "--listen", `0.0.0.0:${port}`], {
  env: process.env, stdout: "inherit", stderr: "inherit",
});
process.exit(await proc.exited);
```

The deployed wrapper is `deploy/app-server/{index,supervise}.ts`
(deploy/README.md); a binary that dies after it was ready (it accepted on
`$PORT` and had been up for 60 s) ends it with its exit code, and Compute
replaces the instance (item 39). Never deploy a streams-slate that has
item 38 behind an older wrapper (plan decision D11): the old one holds
every death, so a critical exit would leave a 500 that is never replaced.
The campaign scripts deploy from copies of `deploy/<app>` under
`$SOAK_HOME`; `bench/stage-app.sh` re-stages each copy from the repo before
every deploy (`bench/soak/deploy-region.sh`, `mt-tenants.sh`,
`wc-ladder.sh`, `bench/fleet/setup-fleet.sh`, `deploy-fleet.sh`) and
refuses one that holds any entry the repo's app does not (hidden files
such as `.env`, which Bun loads on its own, other directories, symlinks)
before installing anything; it then builds a fresh directory with exactly
the repo's files, installs unless an install succeeded for exactly this
manifest, and swaps it in only once it verifies. Any other deploy from a
copied app directory must run it first.

Once a stop is requested — a critical loop's exit, or a SIGTERM/SIGINT the
binary has observed — streams-slate bounds it at 30 s on a thread of its
own and exits 1 if it has not finished (item 38). The signal itself is
observed by a task on the async executor, so a SIGTERM that reaches a
process whose executor workers are all already blocked is never observed:
it arms no bound, and that process runs until the platform's SIGKILL,
exactly as before item 38.

Inside that bound the HTTP connections drain first (edge change #180): at
the stop the listener closes, and each connection finishes the request it
is serving and takes no further one (an idle keep-alive closes at once).
Connections still serving after 10 s (an SSE session, a long-poll, a client
reading slowly) are reset; only then do the supervised loops join and the
shards close, so an append in flight at a SIGTERM is answered, not reset.

The ELF check is not optional politeness — it converts the silent
crash-loop-zombie failure mode (§10) into a readable boot log. Binaries are
uploaded to the object store and passed as 24 h-presigned GET URLs
(re-presign on every deploy day). Use **distinct env names per role**
(`SERVER_BINARY_URL` vs `GEN_BINARY_URL`) — see the next trap.

### 7.3 The project-scope env-merge trap (will bite you)

Compute env vars are **project-scoped and merged**: every deploy snapshots
the union of everything ever set in the project. Consequences we hit:

- a probe service inherited the fleet's `BINARY_URL` and ran the wrong
  binary → distinct names per role;
- LBs inherited `MODE=gen` from a generator deploy and started generating
  load at themselves;
- an `--unset-env` cleanup for one service gutted another's env.

**Rule: every deploy restates the service's COMPLETE environment** from a
canonical per-role script; removals are explicit `--unset-env`. Never do an
incremental deploy. Also: `--env KEY=` (empty value) is rejected — use
`--unset-env`; values containing commas must be quoted or the CLI splits
them into bogus keys (`RUST_LOG="info,slatedb=info"`).

### 7.4 Deploy procedure (per release)

1. Build + verify arch (§2). Upload binaries; capture presigned URLs.
2. Roll servers one at a time from the canonical script (full env restated,
   including the binary URL).
3. **Health-gate each instance before the next**: poll `/health` up to
   ~2 min (wake + boot + shard reopen). If it 404s past that, check the
   version's *preview domain* (`cv-….prisma.build/health`) — service-domain
   404 with healthy preview = route propagation; both 404 = boot failure.
4. Roll LBs the same way.
5. Redeploying under live load can zombie an instance (observed ~once per
   ~20 deploys). The heal is simply another deploy. Watch the first minute
   of `/v1/debug/load` after each roll.
   On a fleet-off cell (edge change #179) each boot writes its id to
   `_cell/boot` at start, and the new boot's first open fences the old
   process. The old process then yields: it opens no shard again, answers
   requests that need one `503` (`shard_closing` raw, `temporarily_unavailable`
   product) with `Retry-After: 1` and nothing written, and `/health` and
   `/readyz` answer `503 shard storage unavailable: superseded by a newer
   boot` (cell-watch reason `superseded`) until the platform stops it. If the
   new boot crashes after that, the old process does not take the shard
   back: the platform's restart of the new one (a fresh boot) serves, and an
   old process the platform keeps instead exits through the unready watchdog
   (`UNREADY_EXIT_AFTER_SECS`, 300 s) and its replacement serves.
6. After load tests: destroy generator versions, redeploy servers/LBs
   *without* `KEEP_AWAKE` so the fleet scales to zero.

### 7.4b Multi-instance deploys: wait for the ring before routing

**Do not send traffic until the fleet is stable.** Compute cold-starts
instances one at a time, and shard ownership is derived from live
heartbeats — so while the fleet is still coming up, the ownership map
changes under load and shards move repeatedly mid-write. Measured on the
4-instance cluster (2026-07-25): load applied ~4 min after deploy, with
`desired.json` still reporting `live=1`, lost **371,900 acknowledged
records**; the identical workload on a stable ring was clean.

| | converging ring | stable ring |
|---|---|---|
| accepted | 1,531,200 | 1,548,800 |
| abandoned | 17,600 | 0 |
| ring redirects | 3,107 | 47 |
| cold-start 404s | 148 | 0 |
| order check | FAIL | PASS |

Gate: **all N instances live AND `ring_active` unchanged for 60 s**
(poll `/operator/data.json`). `bench/docker/harness/cluster-run.sh` has
a reference implementation.

**Instances scale to zero and the platform answers 404 while one wakes.**
Routing must never be the wake mechanism — the LB sends out-of-band
`/health` pings (§6). Any client that talks to instances directly must
treat 404 as retryable and health-ping to wake the target, or it will
drop writes that would have succeeded. A kept-awake shared cell does not
sleep, so its clients meet this 404 only while it boots after a deploy or
a restart.

### 7.5 Platform failure modes you will meet

| symptom | meaning | action |
|---|---|---|
| domain + preview 404, version `running`, `logs` hangs empty | **crash-loop zombie**: app exits at/near boot repeatedly, platform gave up silently ([repro-no-restart/](./repro-no-restart/)) | fix the boot cause; redeploy (a deploy always heals). With the §7.2 wrapper the cause is in the boot log |
| domain + preview 404, version `running`, but the boot log shows the app **listening** and no request ever reaches it | **the edge never published this service** — seen 2026-07-31 for every new service, across projects and PoPs, including a hello-world app ([repro-edge-404/](./repro-edge-404/)) | not fixable from here. Confirm with `repro-edge-404/repro.sh` (hello-world control) before burning deploys; old services keep working, so do NOT redeploy anything you still depend on |
| single instance dies (OOM/exit/wedge), even under traffic | plaform reprovisions transparently in seconds | nothing — this genuinely works (verified legs 1–5 of the repro) |
| deploy CLI throws `styleText` import error | Node < 20 resolving the CLI | run `bunx --bun @prisma/compute-cli …` |
| first requests after idle are slow | scale-to-zero wake + connection-pool warmup; on an awake instance, the idle release: 120 s after the last `/v1/stream*` request, once its absorb backlog has drained (under 1 MiB owed), the instance flushes its shard memtables and drops its SlateDB block caches and postings slices (log line `idle: released the block caches and the postings slices`), so the first read of each table pays its object-store GETs once | expected; the 4 s pool idle timeout (§3.1) exists for exactly this, and the release is what returns an idle instance's RSS after a load (Q0) |
| a URL that worked before now 503s, service looks healthy | **preview domains are per-version**: a redeploy mints a new one and retires the old | re-resolve after every deploy: `compute versions list --project P --service S \| awk '$2=="running"{print $3}'` |
| domain returns a generic JSON `binary_exited` 500 | the binary died at boot — before it ever accepted on `$PORT`, or within its first 60 s — and the wrapper holds the port unhealthy | read the platform log: the binary's stderr, then the wrapper's `binary exited with code N; holding :PORT unhealthy; details: {...}` (exit code, arguments, hint) — usually a missing required env var, wrong arch, or a store it cannot open. The body carries none of it: the port is customer-reachable |
| instance replaced (exit 1); its log has `critical task <name> exited while the runtime was running: <outcome>; requesting the ordered stop, bounded at 30s` | a critical loop ended after the binary had been serving for 60 s: the process root stopped the runtime and exited 1 (item 38), and the wrapper exited with it, so Compute reprovisioned (item 39) | that line names the loop and how it ended; read the loop's own error line before it. The log usually ends in `Error: critical task <name> …`, but a stop that outlives its 30 s bound ends in `stop deadline: a stop was requested 30s ago and has not finished (cause: critical task <name> …); exiting` on stderr with no `Error:` line; that line and the escalation line both name the cause. The same exit within the first 60 s (e.g. `usage-rollup` with `ROLLUP=1` and a rollup DB the instance cannot open) is held as a `binary_exited` body instead; a loop that fails the same way after every boot can still end as the crash-loop zombie (first row), so fix its cause |
| parallel deploys fail with `EEXIST` | concurrent `bunx` invocations race on the shared package cache | fan out regions **sequentially**, or pre-warm with one call |
| `--service` calls all fail after a scripted deploy | the script captured the **version** id (`cpv_…`) that `deploy` prints, not the **service** id (`cps_…`) | take service ids from `services list` only |

**Who answers.** A PAGE (a shared cell's "What to alert on" rows, and the
paging tool's uptime check and heartbeat monitor) reaches `<on-call>` and,
unacknowledged after 15 minutes, `<secondary>`; a NOTIFY goes to
`<notify channel>`, read the next business day. The tool is Prisma's
existing on-call tool, else a standalone one with an uptime check, a
heartbeat monitor and phone escalation (the owner's answer A1 of
2026-10-12; a chat channel alone is not a page); the hours are `<hours>`,
and waking hours under the published "beta, no SLA" notice are acceptable
for the beta. What this section calls not fixable from here (the edge
never publishing a service) and a platform fault (§8: steal above 3 %
sustained) go to Compute's escalation contact, `<compute contact>`. The
names are placeholders until the paging service is set up and Platform
names the contact.

**Setting up the paging service** (once Platform/SRE names the tool and
the people; none of it exists yet). (1) A service for the shared cells
with an Events API v2 integration: the watch's `ALERT_SINK_KIND` is
`events-v2`, `ALERT_SINK_URL` the tool's events endpoint and
`ALERT_SINK_KEY` the integration's routing key. (2) Urgency by severity:
`critical` (a PAGE) is high urgency, the phone; `warning` (a NOTIFY) is
low urgency and goes to `<notify channel>` alone. (3) Its escalation
policy: `<on-call>`, then `<secondary>` after 15 minutes unacknowledged,
on a schedule for `<hours>`. (4) An uptime check of the cell's
`GET /health` from 2 locations every 60 s with a 10 s timeout, alerting
after 3 failures in a row (row 1). (5) A heartbeat monitor that alerts
when no GET came for 3 minutes (row 2; the watch beats every 15 s), its
URL the watch's `HEARTBEAT_URL`. Both checks alert into the service at
high urgency. (6) The three secrets go into the watch's own project with
set-secrets, never a shell or a log; redeploy the watch and replace this
section's placeholders with the names. (7) Drill it (Shared cells,
"Drills"): each drill's page rings `<on-call>`, and one left
unacknowledged 15 minutes rings `<secondary>`.

Deploy-time footguns (region-code mismatches, version-less service shells,
`bun install` on fresh app dirs) are catalogued in
[deploy/README.md](./deploy/README.md#deploy-footguns).

## 8. Monitoring

**Primary feeds**: fleet heartbeats (object store), LB `/stats` (per-upstream
rps/ackMs/live/cpu + desired), `/v1/debug/*` per instance. OTel export is
spec'd in [OPERATIONS.md §5](./OPERATIONS.md).

**Operator dashboard**: every server serves `/operator` behind the
deployment bearer (SR-5: no unauthenticated metadata on the customer
listener; the bearer is the interim operator credential), and its payload
is restricted to operational metadata: never stream names, tenant
identifiers, tokens, keys, or signed URLs. Panels: this-instance
load/RSS, admission (instance cap, per-stream cap, shed counters, RSS shed
threshold), fleet heartbeat table (freshness judged against the serving
instance's clock, not the browser's), and the per-op object-store latency
table with the O14a sentinels. `/operator/data.json` is the same payload as
JSON, plus `ops` (the last telemetry tick's `_ops_metrics` snapshot, cached:
reading it drains no peak) and `feeds` (the auth feeds' ages, as on
`/health`); `/operator/runbook` serves this document (compiled into the
binary).

**Memory envelope** (AWS-readyness.md §2): the platform kernel kills the
process at ~750 MB RSS on the pilot instance class, and the crash loop that
follows is unrecoverable under load (each replacement replays WAL under full
pressure and dies again — observed on slate-codex 2026-07-21). Therefore:
`ADMIT_RSS_SHED_MB` defaults to 500 and must always sit well below the kill
line; steady-state feature budgets must sum ≤ 450 MB, leaving headroom for
shard-open replay and compaction bursts.

**Healthy baselines** (pilot, 4×1-CPU fleet, 16 shards, conc 128×4 offered):

| signal | healthy | investigate |
|---|---|---|
| ack_p50_ms | 50–65 under load | > 250 sustained 15 min (also the scale-out trigger) |
| rss_mb | 190–300 | > 450 (appends are shed at 500) |
| cpu_pct | tracks load; 75 % = scale-out | pinned > 90 with low rps |
| wal_put_p99 (heartbeat) | < 150 ms | > 300 ms sustained → run the §5 diagnosis |
| out_inflight_peak | < 100 | — (no hard egress cap observed up to 258) |
| timer_tokio p99 (debug/store) | < 20 ms | > 100 ms = blocking-work regression (the O14a alarm) |
| timer_thread max | < 10 ms | higher = host contention |
| steal_pct | ~0 | > 3 % sustained = platform conversation |
| admit_shed rate | 0 outside overload | growing while clients report errors = check client Retry-After handling |
| fence events | ≈ shard-move rate | excess = routing flap |

SLO targets (append availability 99.95 %, durable-ack p99 < 250 ms, tail
freshness p99 < 500 ms, …): [OPERATIONS.md §5](./OPERATIONS.md).

**Fork cleanup debt** (TLA-019-F4). A deleted fork whose release of its
source reference was inconclusive, or whose DELETE died after the tombstone,
leaves a marker under `registry/v4/fork-debt/`; the `fork-debt-reconcile`
task pays it with no client retry. On first start after the upgrade it also
walks the cell's descriptors once to index tombstones written before the
index existed; progress and completion live in
`registry/v4/fork-debt-backfill.json` (delete that object to walk again, for
example after an instance on an older binary deleted forks during a rolling
upgrade). `_ops_metrics` gauges, published after each completed circle:

| gauge | meaning |
|---|---|
| `fork_debt_pending` | markers whose debt the last circle could not pay |
| `fork_debt_deferred` | markers whose stream owes nothing yet (live, or soft-deleted and still retained for its own forks) |
| `fork_debt_oldest_pending_age_ms` | age of the oldest pending marker (0 when none) |
| `fork_debt_circle_age_ms` | time since the last completed circle (since start before the first) |
| `fork_debt_stale_after_ms` | the alert threshold: 3 × `FORK_DEBT_SWEEP_SECS` |
| `fork_debt_backfill_complete` | 1 once the one-time backfill has walked the whole cell |

Alert `fork_debt_stale` opens when the oldest pending marker or the last
circle is older than `fork_debt_stale_after_ms` (15 min at the default). A
payable debt is paid on the first circle after its creator's install, so a
marker that survives three circles means either the release keeps failing
(logs: `fork-debt reconcile:` warnings; check store health and the source
descriptor) or the child's creator died before installing its reference, in
which case the source's `fork_children` does not list the child's epoch and
the marker is inert. Remove an inert marker only after confirming that and
that no create request for that child incarnation can still be running. A
stale circle means the reconciler is wedged or the index cannot be listed.

## 9. Capacity planning

Measured envelopes (1-CPU/1-GB, 16 shards, Tigris, 2026-07):

| dimension | number | provenance |
|---|---|---|
| per-instance sustained, guarded, router-fronted | ~1,200 req/s | run 10 post-edge-fix |
| per-instance max observed | ~1,180–1,240 req/s | runs 10/12 |
| fleet of 4, sustained avg / peak window | ~1,250 / 2,760 req/s | runs 11–12 |
| ingress concurrency per instance (via platform router) | ~145–150 front-door, ~124 zero-queueing single-source; post-Conduit-fix ≈ half of direct per source | [PLATFORM-EDGE-REPORT.md](./PLATFORM-EDGE-REPORT.md) |
| ingress concurrency, direct path | 509–511 simultaneous ≈ 4,900 req/s | platform team measurement |
| outbound object-store concurrency | ≥ 258 observed, no hard cap encountered | run 13 gauge |
| ack floor | flush interval + one Tigris PUT ≈ 40–65 ms | all runs |

Sizing: `instances = clamp(max(need_util, need_slots, need_latency, …),
1, FLEET_MAX)` with `need_util = ceil(Σ cores_used / 0.75)` and
`need_slots = ceil(Σ inflight / (0.75 × SCALE_EDGE_SLOTS))` — the fleet
computes this itself; your job is choosing `FLEET_MAX` (cost ceiling) and
keeping `SCALE_EDGE_SLOTS` calibrated when the platform edge changes.

## 10. Troubleshooting matrix

| symptom | root cause | fix |
|---|---|---|
| every instance 404s right after deploy; versions say `running`; no logs | wrong-arch binary (aarch64 on x86_64 platform) crash-looping into zombies | §2 build + verify `3e00`; wrapper ELF check turns this into a boot-log error; redeploy heals |
| instances die after 20–40 min under load, look like platform kills | per-DB block cache: 16 shards × SlateDB's 512 MB default | `SHARED_CACHE_BYTES` (one shared cache) — already the default here; don't raise per-DB caches |
| RSS ~2× expected on musl builds | musl allocator fragmentation | mimalloc is compiled in; if you fork, keep it |
| acks 600 ms+; ALL debug/store classes slow; CPU low; tokio drift ≫ thread drift | event-loop starvation (blocking quanta, 1 worker) | `TOKIO_WORKERS≥2` (default now enforced); hunt new inline blocking work |
| goodput collapses to ~zero under overload, millions of 429s | client retries without honoring Retry-After (reject storm) | fix the client; the 25 ms tarpit + jittered backoff is the contract |
| fleet stuck below desired (desired=N, live=1) | ring never routes to dark instances → they never wake | LB wake pings (implemented); never rely on routing to wake |
| fleet scales IN while clients are drowning | delivered rps falls when clients queue; servers can't see it | router latency reports block scale-in (implemented); keep `SCALE_EDGE_LATENCY_MS` on |
| LB routes everything to instance 1 (local/docker) | fleet store missing `allow_http` on plain-http endpoints | set it (implemented); verify LB `/stats` shows all upstreams |
| flusher stalls though L0 count is low | per-key L0 overlap gate (meta row) | the per-key cap cannot differ from `L0_MAX_SSTS` (32 by default): raise `L0_MAX_SSTS` |
| WAL prefix grows unboundedly; watermark lags | flush cadence outrunning WAL GC | under the pump (the default) the floor on flush spacing is `WAL_FLUSH_GAP_MS` (default 100): raise it; `FLUSH_INTERVAL_MS ≥ 25` applies only with `WAL_GROUP_COMMIT=0`. WAL GC runs at 30/60 s, fixed (§3.2) |
| deploy applies but service behaves like a different role | project-scope env merge | §7.3 — restate complete env, always |
### Latency knobs (2026-07-27, colleague-review implementation)

| env | default | what it does |
|---|---|---|
| `WAL_POST_ACK_GATHER_MS` | 6 (0 = off) | Pump releases each flush's acks itself (explicit barrier on the durable watch), then waits this long before the next freeze — closed-loop herds join one WAL instead of straddling two. Local A/B (25 ms store): c2 append p50 1.97x -> 1.01x of c1; c32 throughput +70 %, WAL PUT/s down. Soaked at 6. Gathers only when the completed flush left work in flight (drift), so a solo producer pays ~1 ms (herd-settle), not the window. Adaptive gather: the window is skipped when the NEXT WAL already holds 32 requests or 1 MiB — the window exists for a small next generation; at drift+saturation it is a tax (review #2's CDG throughput question). The two thresholds are fixed: `WAL_GATHER_SKIP_REQS` and `WAL_GATHER_SKIP_BYTES` are not read (since 2026-09-29). `/v1/debug/timings` pump block: gathers_applied vs gathers_skipped_busy, gathered_reqs, flushed_{reqs,records,bytes} (requests-per-WAL), ack_to_enqueue_{sum_us,count}. |
| `TAIL_RING_BYTES` | 0 (off) | Per-engine durable-tail ring: dispatch publishes freshly-durable pages to memory BEFORE acks; woken live reads serve from it instead of scanning SlateDB. Canonical scan remains the fallback (restart/eviction/lag/filters). 32 MiB suggested. `/v1/debug/timings` -> `tail_ring{published,hits,misses,evicted}`. A read WOKEN by a long-poll wait (a fresh commit group, not a backlog) has a page budget of 1 MiB, fixed: `TAIL_MAX_BYTES` is not read (since 2026-09-29). Bulk reads keep 8 MiB. |
| `HIST_READER_CAP` | 8 | Cached history DbReaders per process. **Size it ≥ the number of streams concurrently reading history**: LRU with a rotating working set of cap+1 reopens on ~every read (the DST thrash test pins this behavior). Each cached reader costs one manifest poll (5 s) + one checkpoint. |
| `HIST_READER_IDLE_SECS` | 120 | Idle eviction for cached history readers. |
| `STREAMS_DEBUG_TIMING` | off | Benchmark-only: woken long-poll responses carry `Streams-Debug-Wait: waited arm_us read_us`, splitting the roundtrip-minus-append interval into wait-wake vs read-build stages. Do not enable outside benches. |

**Campaign hygiene (destroyed-specimen lesson):** every soak deploy now
requires `SOAK_RUN_ID` and stamps `proj-<r>.txt.campaign`; `teardown.sh`
refuses projects whose stamp mismatches its own `SOAK_RUN_ID` and
anything listed in `$SOAK_HOME/preserve.txt`.


| brief 503s on a shard after a fleet change | 3 s anti-flap hold-off after fencing | normal; clients retry |
| client reports a timeout but the record is in the stream | storage slowness outlived the client's deadline — the append committed late (no fencing needed) | expected under a degraded store; clients MUST use producer idempotence (`Producer-Id`/`Epoch`/`Seq`) so the retry is deduped at the original offset |
| `--env KEY=` rejected / RUST_LOG splits into bogus keys | CLI env parsing | `--unset-env` for removals; quote comma values |
| throughput → 0, clients see connection timeouts, but stored record count keeps rising | **WAL read storm**: detached shard-open replays piling up after an engine death (each client timeout used to spawn another full-WAL replay; the zombies fence each other and the serving map never fills) | fixed by single-flight opens (sharddir.rs, 2026-07-27); `/v1/debug/store` → `wal_read_storm.stalled` detects the shape, `shard_opens` shows the loop (started ≫ completed). If it ever recurs: restart the instance, check `served_from` for out-of-region routing. Clients must use producer idempotence or late-landing writes become duplicates on retry |

## 11. Data operations

- **Storage layout** (all under `PATH_PREFIX/`): `topology.json`,
  `shards/<id>/…` (SlateDB per-shard: `wal/`, `manifest/`, `compacted/`),
  `history/…` (absorbed per-stream SSTs), `registry/…` (by-name),
  `fleet/`+`routers/` under `FLEET_PREFIX`. Everything except
  topology/fleet metadata is tenant-key ciphertext.
- **Usage databases**: the read spool and the rollup are SlateDB databases
  inside the prefixed data store, and their own paths repeat the prefix:
  `PATH_PREFIX/PATH_PREFIX/telemetry/read-spool/<instance>/…` and
  `PATH_PREFIX/PATH_PREFIX/telemetry/usage-rollup/v2/p0/…` (with no prefix:
  `telemetry/read-spool/<instance>/…`, `telemetry/usage-rollup/v2/p0/…`).
  Both follow the one resolved prefix, whether it came from the variable or
  from `--path-prefix`. A binary before this rule read the spool's prefix
  from the variable only: a deployment that gave the prefix on argv alone
  kept its spool at `PATH_PREFIX/telemetry/read-spool/<instance>/…`, which
  this binary does not read. Before upgrading such a deployment that meters
  usage, wait for `spool.depth` 0 in `/operator/billing.json` and stop it
  gracefully (the stop seals the read window and runs one drain round); rows
  left in the old spool are not billed.
- **GC**: WAL objects reaped per §3.2 after the 60 s minimum age; history SSTs retired
  by compaction; deletion protection, soft-delete windows and GDPR erasure:
  [OPERATIONS.md §2.4](./OPERATIONS.md).
- **Backups / PITR / restore drills**: checkpoint-pin + async copy design in
  [OPERATIONS.md §2](./OPERATIONS.md) (RPO ≤ 5 min target). Not yet wired in
  the pilot — treat the pilot keyspace as re-creatable.
- **Fresh environment**: pick a new `PATH_PREFIX` (and `FLEET_PREFIX`).
  Cheap, instant, and how every pilot run isolated itself.
- **Layout 5** (storage layout version 5): each append request is stored as
  pages of up to 64 KiB of records, compressed with zstd-1 when that pays
  and encrypted once. There is no compression setting; `FRAME_COMPRESS` is
  no longer read and can be dropped from deployments. `format=frames` reads
  answer uncompressed version 4 frames on every deployment. A binary refuses any
  namespace written under another layout (`unsupported_storage_layout`),
  so a layout 4 deployment moves to a fresh `PATH_PREFIX` or bucket.
- **Decommission**: stop generators, redeploy without `KEEP_AWAKE`, let the
  platform sleep the fleet; delete the prefix when the data is disposable.

## 12. Security notes for operators

Bearer token gates the API; the stream key gates the data (two independent
factors — a leaked token cannot decrypt). Keys never persist server-side;
backups are ciphertext. System telemetry ledgers (`_usage`, `_ops_events`,
`_ops_metrics`) are encrypted under `USAGE_STREAM_KEY`.
Full identity/custody/audit design: [OPERATIONS.md §3](./OPERATIONS.md).
Never commit tokens, keys, or presigned URLs; the deploy scripts keep them
in a local scratch directory outside the repo.

## 12b. Staging deployment

A long-lived, production-shaped staging cell (Compute + Prisma Buckets)
for wider testing: topology, the canonical environment, blockers, phased
rollout and exit criteria live in [docs/STAGING.md](./docs/STAGING.md).

## 13. The release gate

Every substantive change runs, in order:

1. `scripts/release-gate.sh` — everything the commit gate
   `scripts/quality.sh` checks (fmt, clippy with `-D warnings`, rustdoc,
   `cargo deny check` for advisories, licenses, bans and sources with
   exceptions in `deny.toml` and `SECURITY.md`, the source and evidence
   ratchets), every formal receipt current, the full test suite and the
   capacity gate.
2. The **single-instance saturation benchmark** on Prisma Compute
   (`scripts/bench-fra-ab.sh`; procedure and pass thresholds in
   [AWS-readyness.md §5](./AWS-readyness.md)). One server, the pilot
   generator aimed straight at it, fresh management-API bucket, conc 128 /
   32 streams / batch 16, ≥16 min. No client errors, no restarts, RSS
   ≤ 620 MB, throughput/latency within the recorded baseline band
   (`bench/fra-ab-baseline.md`). This exact harness is what exposed the
   slate-codex OOM crash loop — treat a red run as a hard stop.

## 14. Tigris latency observatory

Six always-on probes (one per Prisma region; `bench/probe/`), each with its
own project, Prisma Postgres, and management-API bucket. Every 10 s: solo
PUT + hot/cold GET at 1 KB and 256 KB; every 60 s a fresh-connection GET;
at the top of each hour a 60 s 16-concurrent 256 KB PUT burst (separates
time-of-day tails from load-correlated contention). Daily pages with
UTC-day pagination, hourly p50/p90/p99/max charts and tables:

| region | page |
|---|---|
| eu-central-1 | https://yezqz7cxdangclekp4yc6x6n.fra.prisma.build |
| ap-southeast-1 | https://jsvsjfo6r2z6jmuijunbaty0.sin.prisma.build |
| ap-northeast-1 | https://k8jze0k59mes4teujlu7yx4h.nrt.prisma.build |
| us-east-1 | https://tzngb1lztglq1sbdl8e24s40.ewr.prisma.build |
| us-west-1 | https://rtni7hd1ug3ecv8q6u5f4oc1.sjc.prisma.build |
| eu-west-3 | https://kjzr1wu7dkfc0oks2f3620h3.cdg.prisma.build |

First-hour reading (2026-07-22, p50/p99 ms): PUT latency varies 5–10× by
region (SIN/NRT ~21–22 ms 1 KB PUTs; FRA 143, CDG 184, EWR 237) while 1 KB
GETs sit at 3–20 ms everywhere — the published "14–18 ms median" read story
holds, and the write story our WAL lives in is region-dependent with p99s
already reaching 1.6–2.5 s in EU. Ops note: the compute CLI prints
"Service URL" on deploy (no domain guessing), and `--env` values
containing commas must be wrapped in inner quotes or the CLI splits them
into separate variables.


## Billing pipeline runbooks (round-22 item D4)

Readiness surface: `GET /operator/billing.json` — one JSON answer with
`ready`, spool state, last-drain age, rollup cursor age, the
oldest-unclosed month, pending artifact counts, mismatch and
tombstone-walk counters, and open alerts. In `BILLING_MODE=required`,
`/health` answers 503 until the spool (and rollup DB, on the rollup
owner) is open.

| symptom | meaning | action |
|---|---|---|
| `spool.open=false` | read spool failed to open (required mode refuses to boot in this state) | check store credentials/path; restart; reads meter in memory only in non-required mode |
| `spool.depth` climbing | `_usage` ledger unreachable — sealed read batches accumulating durably | check the rollup/owner instance and store health; depth drains automatically on recovery |
| `spool.quarantined > 0` (alert `read_spool_corruption`) | corrupt spool rows moved to `quarantine/` — those reads are NOT billed | inspect the quarantine rows of the spool database (`telemetry/read-spool/<instance>` under the path prefix, §11); recover or write off explicitly; the counter persists across restarts until the quarantine is cleared |
| alert `usage_outbox_lag` | dirty segment snapshots not acknowledged (threshold `ALERT_USAGE_OUTBOX_DIRTY`) | ledger append path down or committer wedged; see `drain.lastOkAgeSecs` |
| `drain.lastOkAgeSecs` large (healthy: below `TELEMETRY_DRAIN_SECS`, 8) | no successful drain round — ledger unreachable or scans failing CLOSED | financial scans defer on error by design; fix the store fault, drains self-heal |
| `rollup.lastApplyAgeSecs` large with traffic | rollup consumer stalled (cursor not progressing); the stored cap is stale: admission judges the figure published before the stall, so an under-cap project can store past its `max_stored_bytes` by what it writes meanwhile, and an at-cap project that deleted stays refused until the rollup resumes (alert above 600 s: "What to alert on", Shared cells) | check the ROLLUP=1 instance; the ledger retains everything, catch-up is automatic |
| `rollup.oldestUnclosedMonth` far behind | month closes are overdue | closes catch up IN ORDER automatically each tick; investigate close errors in logs if the marker stays put |
| `rollup.pendingArtifacts`/`pendingCorrectionArtifacts` stuck > 0 | create-only PUTs failing, or a content mismatch | see next row; transient store faults retry every tick. `pendingArtifacts` counts the monthly rows the publisher can publish, `pendingArtifactsBlockedCorrupt` the rows it cannot (two rows down) and `pendingArtifactsTotal` both; the three stop at the same row, the 1,000th publishable one |
| `artifactContentMismatches > 0` | an immutable artifact path holds bytes we did not stage — POTENTIAL TAMPERING | the row stays pending on purpose; diff the object against the rollup row, resolve manually, never overwrite without recording why |
| `rollup.pendingArtifactsBlockedCorrupt > 0` | pending monthly artifact rows whose key or body does not decode: the publisher skips them on every pass, so they never publish on their own; every scan logs `pending monthly artifact stays unpublished` with the key and the error | operator repair: restore each logged row from the frozen `month/` row it was staged from, or record why it is written off; never delete one silently. The month's invoice inputs are incomplete until the count is 0 |
| `tombstoneWalkCloseSubmits` climbing steadily | closures being recovered by the walk rather than the delete path | acceptable under crash churn; investigate if it grows without deletes |
| invoice readiness check | before export: `ready=true`, no open alerts, `oldestUnclosedMonth` = previous month, pending artifact queues empty (`pendingArtifactsTotal` and `pendingCorrectionArtifacts` 0), mismatches 0 | then the frozen rows + correction artifacts under `telemetry/usage-monthly/` are the invoice inputs |


## Memory survival posture (OOM review, 2026-08-07)

The ab21 cloud death was a PRE-EXISTING cumulative-work
history/absorber memory failure caused by the absence of a
process-wide gather bound (the freeze4 control also dies on the same
harness). Sixteen shards amplified the exposure but were neither
necessary nor sufficient (the 4-shard arm also died). Telemetry was
disabled in the failing cloud arm and was therefore NOT the immediate
cause; the telemetry DBs independently inherited unsafe cache defaults
— a latent additional hazard fixed in the same round. Preview.7 died
~40% earlier than freeze4, so a partial regression exists but is not
yet isolated. Deploy posture until the acceptance campaign clears a
looser one (canonical file: deploy/profiles/compute-1g.env, sourced by
every active deploy script; opt-out only via
UNSAFE_LEGACY_MEMORY_PROFILE=1):

```text
INITIAL_SHARDS=4                    # topology, per-script; fresh namespace required to change
ABSORB_GATHER_MAX_BYTES=8388608
ABSORB_GLOBAL_BUDGET_BYTES=100859904 # hard process-wide bound; the binary FLOORS this
                                     # at the worst-frame transient ((32 MiB + 64 KiB
                                     # encoding allowance) x3 ≈ 96 MiB) so one oversized
                                     # frame's modeled build cost is always covered
                                     # (~2x budget + gathers=2 restores two-way)
ABSORB_GLOBAL_GATHERS=1             # EVERY gather reserves >= the worst-frame floor,
                                    # so effective concurrency at this budget is ONE
SLATEDB_RT_THREADS=4
TELEMETRY_CACHE_BYTES=16777216
ADMIT_RSS_SHED_MB=500               # shed = RSS + reserved absorber bytes
SHARED_CACHE_BYTES / HISTORY_CACHE_BYTES / POSTINGS_CACHE_BYTES explicit
ROLLUP=0 on ingestion instances (designated instance carries ROLLUP=1)
```

Watch: `history_flush_wait_ms_max` (leading indicator: history L0
approaching 64 SSTs makes flushes block), `history_l0_ssts_max`,
`absorb_reserved_bytes` + `absorb_gathers_inflight` (bounded by
construction), `absorb_bytes_total` vs `ingest_bytes_total` (absorption
keeping up), `rss_peak_since_scrape_mb` (inter-sample spikes),
`read_spool_pending_bytes`, `sweep_resident_engines`. Detail:
`GET /v1/debug/absorb` (authorized).


## Shared-cell release posture (SR3)

The release configuration carries NO permanent shared credential and
is refused at boot otherwise:

```text
STREAMS_AUTH_MODE=enforce
FLEET_AUTH_MODE=workload
WORKLOAD_TOKEN_FILE=<platform-managed file>
STREAMS_RELEASE_POSTURE=1
FLEET_INTERNAL_TOKEN absent
```

`FLEET_AUTH_MODE=static` is a NAMED migration/test posture only; the
binary refuses it under `STREAMS_RELEASE_POSTURE=1` in fleet mode or
beside a `FLEET_INTERNAL_TOKEN`, and in workload mode the static token
does not exist at runtime even if the environment still carries it. A
server without the fleet (no `FLEET_PREFIX`) and without
`FLEET_INTERNAL_TOKEN`, the single-server shared cell, holds no shared
credential to bridge: it passes the posture with `FLEET_AUTH_MODE` at
its default and no `WORKLOAD_TOKEN_FILE`, which it would never read. Feed files (`STREAMS_AUTH_*_FILE`) and
the workload token file MUST be replaced atomically: write a
temporary file, flush, `rename(2)` over the live path — never
truncate-and-rewrite in place (docs/CONTROL-PLANE-INTEGRATION.md
§7.2/§8.2).

## Shared cells (single-server, design partners)

One 1 GiB server, one shard, the fleet off, `deploy/profiles/shared-cell.env`
layered on `compute-1g.env`, projects placed by `deploy/cell-admin` (its
README holds the commands). Owner decisions: README "Shared cells Q0"-"Q10"
(`docs/reviews/2026-09-hardening/`).

**Kept awake.** The profile sets `KEEP_AWAKE=1` (the owner's public-beta
answer (4) of 2026-10-11, "Keep awake"): the wrapper holds Compute's
keep-awake guard for the process's life, so the cell does not sleep when
idle and bills every hour it is up. Never set `KEEP_AWAKE_UNTIL_MS` on it
(that ends the guard); `cell-admin` refuses a profile without the one or
with the other.

**Key custody.** For a cell whose interim issuer signs, the owner's
answer I2 (2026-10-12) supersedes this paragraph: the Ed25519 key lives in
the issuer's encrypted project environment (`STREAMS_ISSUER_SIGNING_KEY`,
set from its file through Composer's `envSecret`), the control bucket's
writer keys are root-of-trust secrets, there is no backup, and the
rotation and compromise drills are in `deploy/cell-admin/README.md`. For
a cell without the issuer: one signing key per cell (RSA of 2,048 bits or
more, or Ed25519), made offline, kept in one mode-0600 file on the
operator's machine and in its backup, never on the cell, in the bucket or
in a deploy environment. Only `cell-admin` reads it (`--key-file`), only the
public half is published, pinned to `prisma-streams-data`; tokens are
minted per credential for at most 24 h and handed over as mode-0600 files.
The audience pin makes separate custody enforceable (a customer key cannot
sign for `prisma-streams-internal`); it does not create it: whoever holds
the file can mint any partner's token. Rehearse a rotation on a scratch
cell before the first rotation of a live one: `cell-admin keys add
--public-key` (the overlap generation), `canary --key-file` with the new
key, then `keys retire --kid` the old one after the longest token lifetime
(`--retire-others` on `keys add` is the compromise path). A lost key means
re-minting every credential under a new key.

**Secret custody.** Every other secret of the cell, the issuer and the
watch (`USAGE_STREAM_KEY`, `STREAMS_CURSOR_KEY`, `AUTH_TOKEN`, the bucket
keys, the issuer's pepper and session key, the paging keys,
`CANARY_CREDENTIAL`) has a row in `docs/SECRETS-CUSTODY.md`: who holds it,
where it is stored, how it rotates, and what a leak or a loss means. Each
value reaches the platform only through `deploy/composer/set-secrets` and
an app only by a redeploy, so a rotation of a cell secret is a restart of
the cell. `USAGE_STREAM_KEY` has no rotation path: never change it on a
cell that holds data.

**Keys feed and binary change together.** Every key in the keys feed names
its audience (`aud`, contract r7). A binary from before r7 refuses a feed
that carries `aud`, a binary from r7 on refuses one without it, and either
mismatch answers every token `401 kid_unknown`. `cell-admin` always writes
`aud`, so a cell's binary and its feed bundle come from one tree: deploy
the r7 binary with the r7 bundle in one step, and keep campaign tools
(`bench/k2cost/field`) on the same tree as the binary they deploy. Never
redeploy the six-region Tigris observatory probes for it (§14).

**Publishing.** A publish is an upload of `feeds-bundle.json` to the
cell's `FEEDS_S3_KEY`. The wrapper (`deploy/app-server/feeds.ts`) polls
it every 15 s with `If-None-Match` and rewrites the three feed files
atomically; the server reads them at its next refresh (30 s), so an
admission or revocation lands within about 45 s, with no restart. After
120 s of awake time without a successful poll the wrapper deletes the
files (`feeds deleted` in the log), and the cell refuses every request
once its policies are 300 s stale, until a poll succeeds: an unreachable
bundle bucket takes the cell down within about 7 minutes. A fetched bundle
that is not JSON or lacks a feed is a failed poll (`feeds bundle refused`
in the log) and replaces nothing, so a malformed bundle left published
takes the cell down the same way: republish a good bundle at once.

**Reprofiling a shared cell.** `cell-admin init` records the digest of the
layered profile (`compute-1g.env` under `shared-cell.env`), and `admit`,
the issuer and `deploy-release.sh` (through `check-sizing`) refuse any
other, so k and every ceiling stay fixed for the cell's life. A release
whose profile changes only the block-cache sizes (`SHARED_CACHE_BYTES`,
`HISTORY_CACHE_BYTES`, `POSTINGS_CACHE_BYTES`, `TELEMETRY_CACHE_BYTES`:
the server's memory, not a ceiling; no sizing rule reads them) is taken
with `cell-admin reprofile`. `deploy-release.sh` names the exact command
when it refuses such a release. The procedure:
1. Run `deploy-release.sh` for the new release. On "the layered profile
   differs", read the refusal: it ends with `cell-admin reprofile --state
   S --recorded-profile <the running release's deploy/profiles/*.env>
   --profile <the new release's>`.
2. Run that command. It refuses unless the `--recorded-profile` files give
   the digest the cell records, and refuses any changed, added or removed
   key outside the four, naming each one; such a change needs a new cell.
   Accepted, it records the new digest by compare-and-swap, journals
   `reprofile` with each key `was -> now`, and prints them. No feed changes.
3. Run `deploy-release.sh` again at once, with `--issuer-release-dir`. Until
   the issuer runs the new release its `profile_ok` is false and it
   refuses every create `409 beta_at_capacity`; exchanges and existing
   projects are unaffected.
A rollback to the release before is the same command reversed: the new
release's files as `--recorded-profile`, the old release's as `--profile`,
then `deploy-release.sh` with the old release.

**Packing by hand (u = 0.5).** Quotas oversubscribe by design: k = 8
projects at their ceilings fill each shared bound exactly. Place projects
by their expected load instead: on each envelope axis (requests/s,
appended bytes/s, read bytes/s; `CELL_ENVELOPE_*` in the profile) the sum
of the admitted projects' expected rates must stay at or below half the
envelope (705 req/s, 1,030,000 B/s appended, 15,000,000 B/s read on the
current profile). Ask each partner for its expected peak rates before `admit`, write the
sum down beside the cell's state directory, and refuse the next partner
that would pass a half. `cell-admin` checks this itself once phase B
declares load.

**What to alert on.** The cell's watch, `deploy/cell-watch` (a Composer
app of its own, `deploy/README.md`), reads the cell every 15 s: `/health`
on a fresh connection and `/operator/data.json` with the deployment bearer
(a `where` below is a key of it unless it names another source), and
`/operator/billing.json` every 60 s; with `ISSUER_ORIGIN` set, also the
issuer's `/healthz` and, every 60 s, its `/v1/admin/metrics` and the
canary (rows 19 and 22, below the table). It never reads `/v1/debug/*`. Each row is one rule: it opens as the row says,
resolves after four clean readings in a row, and is not raised again while
open, except once when its NOTIFY rises to PAGE. An alert carries the cell
id, the rule, the class, named numbers, fixed enums and a link into this
runbook; never a body, a log line, a project or a token. **PAGE** is the
phone: every tenant is hurt now or within minutes, or an isolation or
security invariant broke. **NOTIFY** is chat or e-mail, read the next
business day; billing rows notify, because the beta is metered, not
charged. §7.5's "Who answers" names `<on-call>` and `<notify channel>`.
Rows 1 and 2 are the paging tool's own checks, so they page while the
watch is down. The level rules (marked) wait for calibration on the
certification cell (no PAGE at idle or under the certified load); until
then the watch runs with `NOTIFY_ONLY=1`, which sends their PAGE as a
NOTIFY, while every other row pages from the first deploy;
`NOTIFY_ONLY=0` turns PAGE on for them once that bar is met.

| # | rule | class | where | alert when | paged by |
|---|---|---|---|---|---|
| 1 | uptime check | PAGE | the paging tool's `GET /health` from 2 locations every 60 s, 10 s timeout | 3 failures in a row: no 2xx from outside (§7.5: a zombie, the edge, `binary_exited`) | the paging tool to `<on-call>` |
| 2 | heartbeat | PAGE | the watch's heartbeat GET after each cycle | none for 3 min: the watch is down (a watch whose `STREAMS_OPERATOR_TOKEN` the cell refuses (401 or 403) does not start: its log says `refused: STREAMS_OPERATOR_TOKEN is refused by the cell`), or it withholds the beat after three cycles in a row that ended with a trigger undelivered (its alerts are not getting out) | the paging tool to `<on-call>` |
| 3 | `r03_no_2xx_inside` | PAGE | `/health` | not 200, or no answer in 5 s, for 2 min. The alert names who answered: `server` (its origin header) with the 503's reason (`tasks`, `feeds_unpublished`, `superseded`, `shard_storage`, `billing_not_ready`, `other`; `superseded` is an older boot a newer boot of a fleet-off cell fenced, a deploy or a restart that left the old process answering, §7.4), `wrapper` (500 `binary_exited`, §7.5) or `platform` (anything else: the edge, a zombie, no instance) | cell-watch to `<on-call>` |
| 4 | `r04_restarts` | NOTIFY / PAGE | `/health` `x-streams-boot-id` (sent on a 200 only); `/operator/data.json` `boot` | a new boot id: NOTIFY; 3 boot ids or more in 30 min, a crash loop: PAGE (a loop that never reaches 200 is row 3). A boot id its successor reports as replaced is left out of the count, so a deploy and a restart within 30 min notify twice and do not page: the serving boot's `boot.predecessor` is the boot id it replaced (the boot record `_cell/boot` named it when this boot registered) and `boot.predecessorSuperseded` is `true` once that boot wrote its mark `_cell/superseded`, which it does when it learns a newer boot is registered (at its yield, or at its stop). `null` is undecided or no predecessor; `false` is no mark within 60 s of the registration: the predecessor went before its successor registered (a crash, an OOM kill, a self-exit), and it counts. A fleet-on cell, or a server older than this field, reports no `boot`, and every boot id counts as before. A replaced boot whose mark failed to write counts too | cell-watch to `<notify channel>` / `<on-call>` |
| 5 | `r05_wrong_build` | NOTIFY | `/health` `x-streams-git` | does not start with `EXPECTED_GIT`, the release's commit, for 10 min: the cell is not running the release deployed. Off when `EXPECTED_GIT` is `none` | cell-watch to `<notify channel>` |
| 6 | `r06_feed_delivery` | PAGE | `feeds.jwks.ageSecs`, `feeds.policies.ageSecs`, `feeds.grants.ageSecs`; `feeds.policies.stale`, `feeds.grants.stale` | an age above 75 s (about 30 is normal), some 3 min before the cell refuses every request at 300 s; or `stale: true`: the outage has begun (`503 policy_stale`). The wrapper's log says `feeds poll failed`, `feeds bundle refused` or `feeds deleted` (Publishing, above) | cell-watch to `<on-call>` |
| 7 | `r07_reserved_policy_dropped` | PAGE | `feeds.policies.reservedDropped` | above 0: a feed names the system project, `PROJECT_ID` or `ACCOUNT_ID` | cell-watch to `<on-call>` |
| 8 | `r08_rss_at_shed_line` (level) | PAGE / NOTIFY | `local.rss_mb` + `local.absorb_reserved_bytes_now` / 2^20 against `local.rss_shed_mb` (the shed's own sum) | at or above the line for 10 min: PAGE, every tenant's writes are shed (NEXT-WORK 14.7, "Shared cells Q0"); at 90 % of it for 30 min: NOTIFY. After a load, an idle instance releases its caches 120 s after the last `/v1/stream*` request (or when its absorb backlog drains, if later) and its RSS falls back towards the boot reading within minutes; no `idle: released` line 10 minutes after the last request means a client is still sending (any `/v1/stream*` request, refused ones included, restarts the 120 s) or the loop has stopped | cell-watch to `<on-call>` / `<notify channel>` |
| 9 | `r09_rss_write_shed` | PAGE / NOTIFY | `local.admit_shed_rss` | growth on 2 readings in a row: PAGE; on one: NOTIFY. Look at the largest writers | cell-watch to `<on-call>` / `<notify channel>` |
| 10 | `r10_overload_sheds` (level) | PAGE / NOTIFY | `local.admit_shed_inflight` + `local.admit_shed_survival` | growth on 3 readings in a row (45 s: the u = 0.5 packing failed): PAGE; on one: NOTIFY | cell-watch to `<on-call>` / `<notify channel>` |
| 11 | `r11_durability_wedge` | PAGE | `local.wedge_shed` | any growth | cell-watch to `<on-call>` |
| 12 | `r12_backlog_latch` (level) | PAGE / NOTIFY | `local.maintenance_backpressure` | `engaged`, or `appends_shed` growth: PAGE; `absorb_lag_secs` above 450 (half the 900 s latch) or `unabsorbed_bytes` above 256 MiB (half the instance's 512 MiB): NOTIFY. `cause` and `instance_cause` say why | cell-watch to `<on-call>` / `<notify channel>` |
| 13 | `r13_history_flush_wait` (level) | PAGE | `ops.gauges.history_flush_wait_ms_max` (the last snapshot's peak since the one before; a gather's flush that waits raises it every 5 s) | above 30,000: a gather has waited 30 s at the history L0 cap because compaction frees no slot (one long sorted-run merge on the only compaction slot, or failing compactions), and with one gather permit (`ABSORB_GLOBAL_GATHERS=1`) no shard absorbs meanwhile. The alert carries `history_l0_ssts_max` (64 is the cap) and `history_compacted_runs_max`; detail `GET /v1/debug/absorb` | cell-watch to `<on-call>` |
| 14 | `r14_read_memory` (level) | PAGE | `local.read_memory_bytes` / `local.read_memory_capacity_bytes` | above 80 % for 5 min: neighbours' reads are waiting, then `503 read_memory_busy` | cell-watch to `<on-call>` |
| 15 | `r15_live_pool` (level) | NOTIFY / PAGE | (`local.sse_connections` + `local.parked`) / `local.sse_max_connections` (one live pool; `SSE_MAX_CONNECTIONS`, 1,200) | 90 % for 1 min: NOTIFY; full for 1 min: PAGE, waits stop parking | cell-watch to `<notify channel>` / `<on-call>` |
| 16 | `r16_object_store` (level) | PAGE / NOTIFY | `local.store.totals.ops["put:wal"]` and `["put:manifest"]` over the trailing minute (the growth of each cell's counts; a restart starts a new baseline), and `local.store.ops["put:wal"]` `p99_ms` (the last 60 s) | failed puts (`err`: a failure left after the client's retries, a transport error or a cancelled call) at 5 % or more of all puts on 2 readings in a row: PAGE. A refused put (412, a manifest CAS conflict, counted `unbilled`) is no failure, though `local.store.ops`' own `err` counts it and every 404. `put:wal` `p99_ms` above 2,000 for 5 min: NOTIFY. Diagnose with §5's `/v1/debug/store` | cell-watch to `<on-call>` / `<notify channel>` |
| 17 | `r17_unowned_usage` | NOTIFY | `ops.counters.unowned_meter_events_total` | grew in the last 15 min: usage billed to the cell's `ACCOUNT_ID` sink. An idle cell does not grow it: the rollup's reads of its own ledgers meter nothing (NEXT-WORK 14.7, fixed in round 4, edge #160), so any growth is a read or write the policy snapshot cannot attribute | cell-watch to `<notify channel>` |
| 18 | `r18_billing_pipeline` | NOTIFY | `/operator/billing.json` `openAlerts`, `ready`, `drain.lastOkAgeSecs` | any open alert, `ready: false`, or no drain for 300 s (Billing pipeline runbooks, below) | cell-watch to `<notify channel>` |
| 19 | `r19_canary` | PAGE | the canary: one small record appended to the canary project's stream every 60 s and read back durable, under its customer credential | 3 failures in a row; the alert names the step, who answered (as row 3) and the `error.code`. Only with `ISSUER_ORIGIN` | cell-watch to `<on-call>` |
| 20 | `r20_snapshot_stale` | NOTIFY | `/operator/data.json` itself, beside `/health`; `ops`; `ops.ts_ms` against `ts_ms` | `/health` 200 while `/operator/data.json` has not been a 200 JSON document for 60 s: every row that reads it is blind. The alert carries `data_status`, the status it last answered: `data_status` 401 or 403 is a wrong or rotated `STREAMS_OPERATOR_TOKEN` (set the watch's secret to the cell's `AUTH_TOKEN` with set-secrets and redeploy the watch), 0 no answer in 5 s. Or `ops` still `null` 60 s after the boot id was first seen (no usage key, so the telemetry tick collects nothing), or `ops.ts_ms` over 60 s behind `ts_ms` (the telemetry loop stopped): the rows that read `ops` are blind | cell-watch to `<notify channel>` |
| 21 | `r21_issuer_down` | PAGE | the issuer's `/healthz` | not 200, or no answer, for 2 min: tokens live 60 min by default, so every customer loses access within the hour. Only with `ISSUER_ORIGIN` | cell-watch to `<on-call>` |
| 22 | `r22_canary_exchange` | PAGE | the canary's exchange at the issuer (`POST /v1/token/streams`) | 3 failures in a row, or a 200 whose `endpoint` is not `STREAMS_CELL_ORIGIN` (every refreshing client would go there): the issuer's fault, against row 19's cell. Only with `ISSUER_ORIGIN` | cell-watch to `<on-call>` |
| 23 | `r23_stored_growth` | NOTIFY | `/operator/billing.json` `rollup.stored.writtenBytes` (each increase of the cell's projects' stored bytes that a drain round saw; a delete never lowers it) | grew by more than 50 GiB over the trailing 24 h (G, about 0.6 MB/s sustained; the owner's answer S3 of 2026-10-12, confirmed again when the production workspace exists). Skipped while the billing report has no `rollup.stored`. Until physical reclamation exists a delete frees quota but not the bucket's bytes, so this is the bucket's growth as far as the drain saw it: a drain round emits each segment as it then stands, so bytes stored and deleted between two rounds of a shard (8 s apart by default) never reach it, and a tenant that deletes what it writes within a round grows the bucket without growing this figure. `/operator/data.json` `local.store.totals.bytes_put` (every byte the process put since `totals.since_ms`, WAL and compaction included) bounds the bucket's growth from above, and row 29 alerts on its growth. `rollup.stored.largest` lists the ten projects that wrote the most | cell-watch to `<notify channel>` |
| 24 | `r24_daily_spend` | NOTIFY | `/operator/billing.json` `rollup.stored.writtenBytes`; `local.store.totals` | the day's estimated spend above $10, the cell's budget (S3, as G): billed store requests (`ok` plus `not_found`) of Class A (`put`, `mpu`, `copy`, `list`) and of Class B (`get`, `head`), each at the store's price (scaled to a day once an hour is seen), plus `writtenBytes` × 1.2 at $0.015 per GiB-month (`totalBytes` once physical reclamation exists), plus the kept-awake server's $0.144. The prices are inputs until the platform gives Prisma Buckets' own; the totals restart with the process (`totals.since_ms`), so a restart starts a new baseline. The authoritative budget alarm is the platform's, on the workspace's bill. Skipped as row 23 | cell-watch to `<notify channel>` |
| 25 | `r25_stale_rollup` | NOTIFY | `/operator/billing.json` `rollup.lastApplyAgeSecs`; `ops.counters.ingest_bytes_total` | above 600 while ingest grew in the last 10 min (the cell takes appends): the rollup consumer stalled, and the stored cap judges a stale figure (Billing pipeline runbooks). Skipped as row 23 | cell-watch to `<notify channel>` |
| 26 | `r26_usage_overflow` | NOTIFY | `local.usage_overflow_admits` | above 0: overflow streams share one limiter and one counter set, so their usage is mis-attributed under metered billing | cell-watch to `<notify channel>` |
| 27 | `r27_issuer_cache_age` | NOTIFY | the issuer's `/v1/admin/metrics` `cache_age_secs` (bearer `STREAMS_ISSUER_METRICS_TOKEN`) | above 150: the exchange refuses with `503 issuer_unavailable` at 300. Only with `ISSUER_ORIGIN` | cell-watch to `<notify channel>` |
| 28 | `r28_admission_gate_closed` | NOTIFY | the issuer's `/v1/admin/metrics` `gate_open` | `false`: sign-ups are refused, as expected after `cell-admin gate --close`. Only with `ISSUER_ORIGIN` | cell-watch to `<notify channel>` |
| 29 | `r29_bytes_put_growth` | NOTIFY | `/operator/data.json` `local.store.totals.bytes_put` (every byte the process put since `totals.since_ms`: the WAL, L0 flushes, compaction and the history absorber included; a restart starts it again) | grew by more than 8 × G (400 GiB at G = 50 GiB) over the trailing 24 h: the bound from above on the bucket's growth, which sees what row 23 cannot (bytes stored and deleted between two drain rounds of a shard; the owner's call stored-cap #1 (c) of 2026-10-14). Cell-wide and unattributed, and raised by the store's own rewrites as much as by writes: the factor 8 is the lead's, uncalibrated until the certification cell measures `bytes_put` per stored byte. With row 23 quiet, look for a tenant that deletes what it writes: `rollup.stored.largest` and the projects' append and delete rates | cell-watch to `<notify channel>` |

**The canary (rows 19 and 22).** The watch probes the cell as a
customer does: it exchanges `CANARY_CREDENTIAL` at the issuer's
`POST /v1/token/streams`, keeps the token until two minutes before it
expires, and every 60 s appends one record to the stream
`cell-watch-canary` of the canary project `cell-admin init` placed
(`sp_canary_<cell id>`) and reads it back. With `ISSUER_ORIGIN` set the
watch does not start without the credential; before the issuer exists
both are `none` and the two rows do not run (a hand-minted token lives at
most 24 h). Make the credential once per cell, on the operator's machine:
`cell-admin credential --project sp_canary_<cell id> --hidden --name
watch --scopes "streams.create streams.records.append
streams.records.read" --pepper-file <the issuer's pepper file> --out
<file>`, then set the file's one line, without its newline, as the
watch project's secret `CANARY_CREDENTIAL` (set-secrets, never a shell
or a log) and redeploy the watch. To rotate it (a leak, or a watch
project's secrets exposed), make a new one as above, set it as
`CANARY_CREDENTIAL` and redeploy the watch, then revoke the old one,
which the issuer cannot (it skips hidden credentials): `cell-admin
revoke --project sp_canary_<cell id> --credential <old id>`, and
publish the bundle. A row 19 page whose `error_code` is
`kid_unknown` or another auth code while `/health` is green is what the
canary exists for: start at "Keys feed and binary change together".

**Drills (rows 1, 2, 3, 6, 7, 12, 13, 19 and 21).** Each drill makes
its rule happen on a local cell and checks that it pages:
`deploy/cell-watch/drills.e2e.test.ts` runs the watch in `prisma dev`
with `NOTIFY_ONLY=0` against the real binary over s3lite, sending to
`pager.ts`, a local stand-in for the paging tool with rows 1 and 2's
checks. Build both binaries (`cargo build --locked --bin streams-slate
--bin s3lite`) and install `deploy/cell-watch` (`bun install
--frozen-lockfile`), then run `CELL_WATCH_E2E_BIN_DIR=target/debug
CELL_WATCH_DRILLS=1 bun test ./deploy/cell-watch/drills.e2e.test.ts`
(about 25 minutes: each rule waits as long as it does on a cell). They
hold row 17 among the rules that never open on the drills' cell (the
cell's own metering no longer grows its counter: NEXT-WORK 14.7). In
order, each paged and, where the
cell can be put right, resolved: a
policy naming `PROJECT_ID` in the bundle (row 7); the three feed files
deleted, as the wrapper's `feeds deleted` does (row 6, once a feed is
75 s old); `POST /v1/debug/history-stall?ms=35000` (row 13); `POST
/v1/debug/absorb-pause?on=1` until the absorb-lag latch engages (row 12;
the drill's cell sets `MAX_ABSORB_LAG_SECS=150`, so it engages in
minutes, not 15); the cell stopped (row 3 after 2 min and the uptime
check after 3 failures; its return notifies row 4); the watch killed
(the heartbeat, 3 min after its last beat); then, on a cell with the
issuer and the canary, the issuer stopped (row 21) and a keys feed
carrying the issuer's key under another kid for
`prisma-streams-internal` (row 19: the cell retires the kid the issuer
signs with, so every customer token answers `401 kid_unknown` while
`/health` is green). The other PAGE rules (row 4's crash loop, 9, 11,
14, 15, 16 and 22) are held by `rules.ts`'s tests alone. On Compute the
same drills, on the certification cell under the Q5 field approval and
with the watch sending to the paging service, must each ring
`<on-call>`'s phone (§7.5).

**A scratch cell's paging stand-in.** A scratch or cert cell deployed
before the paging service exists still needs https endpoints for the
watch's sink and heartbeat (it refuses anything else off this machine).
The watch's release carries `pager.ts` as a Composer app of its own
(`pager-app.ts`, project `streams-pager-standin`, apart from the watch so
that its heartbeat monitor pages while the watch is down): set its two
secrets, `PAGER_ROUTING_KEY` (16 characters or more) and
`PAGER_HEARTBEAT_TOKEN` (16 to 128 of `[A-Za-z0-9_-]`), with
`set-secrets <stage> --app pager-standin`, deploy it with
`deploy-release.sh <scratch|cert> <tag>-watch --app pager-standin`, and
give the watch `ALERT_SINK_KIND` `events-v2`, `ALERT_SINK_URL`
`https://<domain>/v2/enqueue`, `ALERT_SINK_KEY` the routing key and
`HEARTBEAT_URL` `https://<domain>/heartbeat/<token>` (set-secrets, then
redeploy the watch). It runs rows 1 and 2's checks against the cell's
origin and keeps what it took in memory: `GET /changes` with the routing
key as bearer lists it. Nobody is called: read the listing after each
drill. Production pages through the real paging service, PagerDuty Events
v2, as §7.5 sets it up; `deploy-release.sh` refuses the stand-in there.
The whole scratch sequence, from the projects to the teardown, is
`docs/BETA-SCRATCH-DEPLOY.md`.

A single-server shared cell publishes no `prisma-streams-internal` key and
sets no `FLEET_INTERNAL_TOKEN`: its internal surface opens to nothing. A
key for that audience in its keys feed is an incident.

## The interim token issuer (public beta)

The issuer (`deploy/issuer`, a Bun service in its own Compute project,
`deploy/composer-issuer`) is the shared cell's authorization root: it
exchanges a customer credential's secret for a signed access token
(`POST /v1/token/streams`), signs users in with Prisma and serves the
page and management API that create and delete projects and credentials,
and drains deleted projects. It holds the cell's signing key, the
credential pepper, the session key and the control and feeds buckets'
writer keys: whoever holds its environment can mint any project's token
and rewrite any project's state. Treat its environment, and the Prisma
workspace that hosts it (the owner plus one backup member, both with
2FA), as the root of trust. The customer side is
`docs/AUTHENTICATING.md`; the code's own map is `deploy/issuer/server.ts`.

**Deploy.** The release run deploys it beside the cell
(`deploy-release.sh ... --issuer-release-dir ... --issuer-project ...
--issuer-branch ...`); the first deploy and every step from the projects
to the teardown are `docs/BETA-SCRATCH-DEPLOY.md`. Its secrets
(`STREAMS_ISSUER_SIGNING_KEY`, `_PEPPER`, `_SESSION_KEY`,
`_OAUTH_CLIENT_SECRET`, `_METRICS_TOKEN`, the two buckets' key pairs)
are set from mode-0600 files, one per secret, by the set-secrets step
of `docs/BETA-SCRATCH-DEPLOY.md` §4 (`--app issuer --from <directory>`),
never on a command line, in a chat or a ticket; its other inputs are the
release's settings file (`deploy/composer-issuer/app.ts` lists them).
`STREAMS_ISSUER_OAUTH_AUTHORITY` stays unset on Compute (the issuer
refuses any origin but this machine's for it: it names the mock identity
provider). It refuses to boot (exit 1, a message that names inputs and
ids only) when the state is not `STREAMS_CELL_ID`'s, is behind the
published bundle, names another issuer than `STREAMS_ISSUER_ID` (the
`iss` every token carries and the cell compares exactly), or the cell's
keys feed lacks its key. It holds Compute's keep-awake guard.

**Health.** `GET /healthz` is 200 `{"status":"ok"}` while it can mint,
else 503 with `reason` `state_stale` (its copy of the control bucket's
state was last revalidated over 300 s ago: the exchange answers 503
`issuer_unavailable`) or `key_not_in_state` (its key is not in the cell's
keys feed: a rotation step out of order, or the compromise drill). `GET
/v1/admin/metrics` with `Authorization: Bearer
<STREAMS_ISSUER_METRICS_TOKEN>` gives the counters: exchanges by outcome,
rate-limit refusals by bucket, `cache_age_secs`, `state_feed_version`
against `published_feed_version`, `gate_open`, `profile_ok` (false: every
create is refused 409 `beta_at_capacity` while `/healthz` stays 200; the
release's profiles moved, see `check-sizing`, or the cell was reprofiled
and the issuer's new release is not deployed yet), the offboarding backlog,
`rehash_pending` and `live_credentials_by_pepper`.

**What pages** ("What to alert on", Shared cells): row 21
`r21_issuer_down` (PAGE: `/healthz` not 200 for 2 min; customers lose
access as their tokens expire, within 60 min at the default lifetime),
row 22 `r22_canary_exchange` (PAGE: the canary's exchange fails 3 times,
or names another `endpoint`), row 19 (PAGE: the canary's cell round trip;
`kid_unknown` there with `/health` green is a keys-feed incident), rows
27 (`cache_age_secs` above 150) and 28 (`gate_open` false) NOTIFY. For a
row 21 page read the issuer's platform log first: `state revalidation
failed` (the control bucket: its keys, its reachability, or a state
restored behind the bundle, which the issuer refuses to follow until you
recover the newest state) or a boot refusal after a redeploy.

**Keys and rotation.** The issuer signs with one Ed25519 key whose public
half is in the cell's keys feed. Rotate it yearly, before the allow-list
first opens and on any doubt, by the rotation drill in
`deploy/cell-admin/README.md` ("Rotating the issuer's key, and a
compromised key": `keys add` the new public half, `canary` with the new
key, redeploy the issuer with the new key, `keys retire` the old kid 70
minutes later); `deploy/issuer/drills.test.ts` runs both drills against
a fake cell. The pepper rotates by prepending a new version
(`v2:<base64>,v1:<base64>`) and redeploying: credentials re-hash under
the newest as they are next exchanged (`rehash_pending`,
`live_credentials_by_pepper`), and the older version is dropped once none
is left under it and the state written after the re-hash is backed up.
Rotating `STREAMS_ISSUER_SESSION_KEY` signs every browser out (sessions
are cookies only; nothing else ends one before its 8 hours).

**Suspend, resume, offboard.** `cell-admin suspend --project P`: within
5 s the issuer mints P no token (403 `project_not_active`) and its users
cannot delete it; once the cell holds the bundle (about 45 s) every
request of P is refused 403 `project_not_active`, tokens already minted
included. `resume` undoes it. A user's delete, or `cell-admin offboard`,
revokes the project's credentials at once; the issuer's offboarding job
then proves the cell holds the revocation, deletes every stream and omits
the project (every 15 s, failed attempts retried after 1, 2, 4 ... 60
minutes; `offboarding_projects` and `offboarding_max_attempts` in the
metrics). A workspace taken off the allow-list (`cell-admin disallow`)
gets nothing new within 5 s and keeps its projects running until you
suspend them; its users keep a reduced sign-in (see, revoke, delete).

**Revoke.** A leaked customer credential: the workspace's users revoke it
on the page, or `cell-admin revoke --project P --credential C`. Within
5 s the issuer exchanges it no more, and once the cell holds the bundle
(about 45 s) its outstanding tokens answer 403 `credential_not_active`:
revocation does not wait for the token lifetime. Rotation (page or
management API) replaces a secret at once with no overlap.

**Audit.** Every change the issuer makes, and every refused management
change of a signed-in user, is one JSON object under
`audit/YYYY/MM/DD/<epoch ms>-<random>.json` in the control bucket
(actor `{user_id, email, workspace_id}` or `issuer:<job>`, action, ids,
versions before and after, outcome, request id; never a secret, token,
key or pepper). Exchanges are counted per credential: a minute's counts
to stdout as `{"kind":"exchanges",...}` lines, an hour's in one audit
object with the `jti` of every token minted (up to 10,000 per credential
an hour), the lookup for a leaked token. The audit is a record, not
tamper evidence: anything holding the control bucket's writer key (the
issuer included) can write or delete under the prefix. Nothing ages it
out yet (13 months is the intended retention; no lifecycle rule or sweep
exists). No source address is recorded until Compute names its trusted
client-IP header (K4).

**Incident: the signing key or the issuer's environment leaked.** Follow
"A compromised key" in `deploy/cell-admin/README.md`, step 0 first:
assume the whole environment leaked unless you know otherwise, so rotate
the control bucket's writer keys (`/v1/buckets/{id}/keys`) and the feeds
bucket's, then `keys add --retire-others` the new key (every outstanding
token answers 401 once the cell holds it, about 45 s; the SDK refreshes
and gets a token under the new key once the issuer is redeployed), and in
the same redeploy rotate `STREAMS_ISSUER_SESSION_KEY` and
`STREAMS_ISSUER_METRICS_TOKEN` (and the watch's copy). A leaked pepper
alone exposes nothing (a 256-bit secret cannot be guessed from its HMAC);
rotate it anyway in the same redeploy. Then `list --json` for kids,
credentials and projects you did not make, revoke and offboard those, and
read the audit prefix from before the leak for what the holder did (it
may have deleted objects there). A leaked OAuth client secret: rotate it
with Prisma Auth (team ask C1) and redeploy; it signs no token.

**Known exposure until K4.** Without the edge's trusted client-IP header
there is no per-address limit: one anonymous client can spend the
instance's 30 sign-in starts a minute (every browser's sign-in answers
429 meanwhile; the exchange is unaffected), and the exchange's
unauthenticated work (one HMAC and at most one state read a second) is
bounded only by the platform's request rate. A spray of unknown
credential ids keeps the per-id failure table at 10,000 keys (adversarial
pass B8).
