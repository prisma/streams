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
| `SLATE_S3_BUCKET` | `streams` | default bucket; `--ops-bucket` / `--shard-bucket` / `--data-bucket` flags override per role |
| `SLATE_S3_REGION` | `us-east-1` | `auto` for Tigris |
| `SLATE_S3_ACCESS_KEY_ID` / `SLATE_S3_SECRET_ACCESS_KEY` | `test` | |
| `PATH_PREFIX` | — | key prefix inside the bucket; independent deployments can share a bucket. **Changing it = a fresh, empty keyspace** |

Provider requirements (strong read-after-write, conditional PUT/If-Match,
durability): [OPERATIONS.md §1](./OPERATIONS.md). Tigris satisfies all of
them and negotiates HTTP/1.1 (relevant to §10's latency story). The client
keeps its connection-pool idle timeout at 4 s deliberately: the platform
silently kills flows idle ≳5 s, and a restored scale-to-zero image must wake
with an empty pool rather than dead sockets.

### 3.2 Engine (shard log)

| env | default | guidance |
|---|---|---|
| `INITIAL_SHARDS` | 1 | power of two; pilot fleet used 16. Set at keyspace creation; topology is stored |
| `FLUSH_INTERVAL_MS` | 25 | WAL flush cadence = the ack floor (flush + one PUT ≈ 40 ms on Tigris at 25 ms). 50 ms halves WAL-object churn for ~10 ms of ack; 5 ms mints WAL SSTs faster than GC reaps them and degrades the watermark to ~0.3–1 s — do not go below 25. With `WAL_GROUP_COMMIT=1` this becomes SlateDB's failsafe only (stretched to ≥1 s) |
| `WAL_GROUP_COMMIT` | 0 | 1 = per-shard pump flushes the WAL the moment the previous flush completes when commits are waiting, instead of on the fixed tick. Sparse/moderate appends stop paying tick alignment (local A/B vs 25 ms s3lite: sequential append p50 55→28 ms, durable_wait 47→27 ms); at saturation both modes self-clock to the PUT RTT (equal). Skips entirely when nothing awaits durability, so idle churn is zero |
| `WAL_FLUSH_GAP_MS` | 0 (= `FLUSH_INTERVAL_MS`) | pump-mode floor on start-to-start flush spacing — bounds the WAL SST mint rate to the same ceiling the old tick had. Only binds when the PUT RTT is faster than the gap |
| `FRAME_COMPRESS` | 0 | 1 = zstd-1 each record payload BEFORE encryption (frame v3; readers accept v2+v3, no migration). Ciphertext never compresses, so this is the only tier where compression can live — it shrinks WAL, L0, compaction, absorber and history bytes together. Sinmax campaign: removed a ~5-6x NIC amplification; enable for any workload with compressible payloads |
| `L0_SST_SIZE_BYTES` | 32 MiB | pilot used 8 MiB on 1-GB instances |
| `MAX_UNFLUSHED_BYTES` | 16 MiB | per-shard byte backpressure. SlateDB's default is 512 MB — a byte flood OOMs a 1-GB box before backpressure fires; keep this small |
| `L0_MAX_SSTS` | 8 | L0 count that triggers write backpressure; pilot used 24 for burst headroom |
| `L0_MAX_SSTS_PER_KEY` | 0 (= follow `L0_MAX_SSTS`) | totally-ordered streams rewrite one meta row per memtable, so every L0 overlaps on that key and THIS cap is the real dispatch gate. The upstream default (8) stalled the flusher |
| `MANIFEST_POLL_MS` | 2000 | also how the flusher learns compaction freed L0 slots; loaded shards want 1000–2000. 60 s polls produced 14 s flush stalls. Each poll is a live Tigris 404 probe (~200-240 ms Tigris-internal) — the default IS the idle-cost posture, deploy scripts must not re-tighten it (docs/TIGRIS-404-COST.md; DST-pinned) |
| `COMPACTOR_POLL_MS` | 2500 | compactions-log probe cadence = the largest idle-404 class (the old 500 ms pin was 8 probes/s/instance forever, pre-limiter era). At 5 MB/s/shard a 2.5 s scheduling gap bounds L0 accumulation to ~3 SSTs vs `L0_MAX_SSTS` 64; drain continuity comes from `COMPACTOR_MAX_CONCURRENT`, not scheduling latency (docs/TIGRIS-404-COST.md; DST-pinned) |
| `WAL_GC_INTERVAL_SECS` / `WAL_GC_MIN_AGE_SECS` | 30 / 60 | tighter than upstream (60/300): a loaded shard mints ~20 WAL SSTs/s and the WAL prefix must stay small — GC lists share the path with ack-critical PUTs. `MIN_AGE` must cover shard-move replay (<1 s; 60 s is generous) |
| `COMPACTIONS_GC_INTERVAL_SECS` / `COMPACTIONS_GC_MIN_AGE_SECS` | 30 / 120 | tighter than upstream (60/300): every compactor state change mints a `.compactions` version and shard OPEN pages through the survivors — at cross-region latency that class fed the eu-central-1 slow-open hang (docs/SOAK-REGIONS.md; upstream slatedb#1970). Only superseded versions below the GC boundary are reaped |
| `TRIM_PER_OP` | 8192 | hot-log records retired per absorb commit; must outpace ingest (at 50k rec/s and one pass per 5 s a pass must retire ~250k) |
| `ABSORB_BYTES` / `ABSORB_AGE_SECS` | 4 MiB / 300 | absorber thresholds into the history tier |
| `ABSORB_PASS_BYTES` | 256 MiB | plaintext held in memory per pass — keep well under instance RAM; pilot used 32 MiB on 1-GB boxes |

### 3.2b Service limits, usage telemetry, billing

| env | default | notes |
|---|---|---|
| `LIMIT_BYTES_PER_SEC` | 5000000 (5 MB/s) | per-stream-shard ingest byte limit (token bucket; 0 disables) |
| `LIMIT_REQS_PER_SEC` | 1000 | per-shard append-request limit |
| `LIMIT_RECS_PER_SEC` | 5000 | per-shard record limit |
| `LIMIT_BURST_SECS` | 2 | bucket capacity = rate x this |
| `USAGE_STREAM_KEY` | — | base64url 32-byte key for the `_usage`/`_ops_*` system ledgers; unset = telemetry pipeline off |
| `BILLING_MODE` | `off` | `required` = refuse to serve without ledger key, real identities, an open read spool (and rollup DB on the rollup owner) |
| `ACCOUNT_ID` / `PROJECT_ID` / `CELL_ID` | `acct_local`/`proj_local`/`local` | the cell's tenant identity (one project per cell); placeholders are refused in required mode |
| `ROLLUP` | — | `1` = this instance runs the usage rollup consumer + month closer |
| `TELEMETRY_DRAIN_SECS` | 2 | drain cadence: sealed reads + dirty snapshots -> `_usage` |
| `OUTBOX_SWEEP_SECS` | 300 | owned-shard outbox sweep + billing tombstone walk cadence |
| `MONTH_CLOSE_GRACE_MS` | 86400000 | wait after a month boundary before closing it |
| `METRICS_INTERVAL_SECS` | 15 | `_ops_metrics` snapshot cadence |
| `ALERT_USAGE_OUTBOX_DIRTY` | 1000 | unacked usage snapshots that open the outbox-lag alert |
| `ABSORB_GLOBAL_BUDGET_BYTES` | 67108864 | PROCESS-WIDE absorber gather budget; every gather reserves (estimate x build multiplier) BEFORE reading frames |
| `ABSORB_GLOBAL_GATHERS` | 2 | concurrent gathers, process-wide |
| `TELEMETRY_CACHE_BYTES` | 16777216 | ONE bounded cache shared by the read-spool and rollup SlateDB DBs (they must never inherit SlateDB's per-DB defaults) |
| `SLATEDB_RT_THREADS` | 2 | worker threads of the dedicated SlateDB runtime (two-runtime split) |

Rejections are 429s with error codes `limit_bytes_per_sec` /
`limit_requests_per_sec` / `limit_records_per_sec`, a human message naming
the limit, and a `Retry-After` header. `/v1/debug/usage` (bearer) exposes
per-stream cumulative requests, records, bytes_in, bytes_out,
plaintext_bytes, frame_bytes, and the derived compression ratio. The
billing emitter appends a JSON array per interval to the billing stream —
one record per active stream with DELTA requests/records/bytes_in/bytes_out
plus cumulative plaintext/frame byte totals (stored-volume-pre-compression
and achieved compression rate are derivable; join hash->name via the
registry's by-name objects). The billing stream's own usage is excluded.

### 3.3 Memory & runtime (1-GB instance discipline)

| env | default | guidance |
|---|---|---|
| `SHARED_CACHE_BYTES` | 192 MiB | ONE block cache shared by all shard DBs. SlateDB's per-DB default is 512 MB — 16 shards × 512 MB on a 1-GB box dies by cache fill in tens of minutes (this *was* our "platform kills instances" mystery) |
| `HISTORY_CACHE_BYTES` | 32 MiB | shared cache for history-tier/absorber DBs |
| `TOKIO_WORKERS` | max(2, cores) | **do not run one worker.** On 1-vCPU instances the old `#[tokio::main]` default was a single worker; inline blocking quanta (SST build/compress) froze every future including commit acks — the O14a saga. The floor of 2 is enforced in code; the pilot runs 3. Measured effect at identical load: ack-excursion windows 30 % → 10 %, median-window WAL-PUT p99 617 → 141 ms |
| `STORE_MAX_CONCURRENT` | 0 (off) | global cap on concurrent object-store ops. Diagnostic knob — capping did NOT help O14a (proved the bottleneck wasn't outbound concurrency); leave off unless experimenting |

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
- `ADMIT_RSS_SHED_MB` 550 — below the kill line with margin, so the shed
  fires while the process can still serve;
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
| `SCALE_LATENCY_MS` / `SCALE_LAT_SUSTAIN_SECS` | 250 / 20 | ack-latency dimension: a congested instance suppresses its own rps signal, so latency scales out even when rps wouldn't |
| `SCALE_EDGE_SLOTS` | 140 | per-instance ingress-concurrency capacity through the platform front door (two-layer model, [PLATFORM-EDGE-REPORT.md](./PLATFORM-EDGE-REPORT.md)). Post-Conduit-fix guidance: recalibrate toward ~250 |
| `SCALE_EDGE_LATENCY_MS` | 1000 | router-observed *client* latency breach: adds an instance AND blocks scale-in (server-side metrics cannot see client pain) |
| `SCALE_IN_SECS` | 60 | hysteresis before any shrink |
| `SCALE_RPS_CAPACITY` | 0 (off) | legacy assumed-capacity dimension. Leave off: capacity constants go stale every time the engine changes speed (we once scaled out at 5 % utilization on a stale constant) |

Desired count = **max over all dimensions** (utilization, in-flight/slots,
hot instance, ack latency, router-observed edge latency), clamped to
`FLEET_MAX`. Every dimension exists because a specific incident demanded it;
the reason string is logged on every change. The model is generalized in
[AUTOSCALING-DESIGN.md](./AUTOSCALING-DESIGN.md).

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
| `SCALE_HOT_PCT` / `SCALE_COLD_PCT` | 75 / 15 | split above / merge below, % of per-segment service limits |
| `SCALE_HOT_EVALS` / `SCALE_COLD_EVALS` | 2 / 180 | consecutive evaluations before acting |
| `SCALE_COOLDOWN_SECS` | 600 | min segment age before it may re-scale |
| `MAX_SEGMENTS_PER_STREAM` | 64 | split guard |
| `REBALANCE_LAG_SECS` | 60 | absorb-lag (s) beyond which the laggard moves a shard to a peer |
| `REBALANCE_MOVE_COOLDOWN_SECS` | 60 | min gap between moves per host |

Shard moves are published to `fleet/overrides.json` (CAS) and win over
the rendezvous ring while the target is live. Splits survive scaler
crashes: the append path re-seals and publishes the missing transition
itself (`resume_split`). `SCALE_FAULT_POINT=after_seal` and
`ABSORB_PAUSE=1` are test-only fault hooks (docker ladder D3/D4).

### 3.6 Admission control (run with these ON in production)

| env | default | pilot value | behavior |
|---|---|---|---|
| `ADMIT_MAX_INFLIGHT` | 0 (off) | 256 | above this many in-flight requests, `/v1/stream` gets `429 + Retry-After: 1` and a 25 ms tarpit. Direct-path instance capacity measured at ~510 concurrent; 256 is the guarded setting for router-fronted 1-CPU boxes |
| `ADMIT_RSS_SHED_MB` | 0 (off) | 800 | writes (non-GET) get `429 + Retry-After: 2` while RSS exceeds this. Without it a 1-GB box OOM-dies at full throughput instead of shedding |

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
    -e ADMIT_MAX_INFLIGHT=256 -e ADMIT_RSS_SHED_MB=800 \
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
| `GET /v1/debug/load` | none | `inflight_now`, `inflight_peak` (swap-on-read), `rss_mb`, `admit_shed` |
| `GET /v1/debug/store?window=60&swap=1` | bearer | per-(op,class) object-store latency cells (`put:wal`, `get:manifest`, …: n/err/p50/p90/p99/max), slow-op ring (≥300 ms with paths), outbound in-flight gauge, **timer sentinels** (`timer_thread`, `timer_tokio` drift) and `steal_pct`. `swap=1` resets the gauge peak — samplers only |
| `GET /v1/debug/sleep?ms=N` | none | calibrated-latency probe (≤5000 ms): separates concurrency caps from rate caps at the edge |

**429 semantics**: body `{"error":{"code":"overloaded"…}}` with
`Retry-After: 1` (in-flight shed) or `2` (RSS shed), after a 25 ms tarpit.
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

- **Heartbeats**: every 2 s each instance PUTs
  `<FLEET_PREFIX>/fleet/<instance>.json`: rps, ack_p50_ms, cpu_pct
  (getrusage), inflight/inflight_peak, rss_mb, wal_put_p50/p99_ms,
  out_inflight/peak, owned_shards. Staleness > 10 s = not live.
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
leave it on after a test; it bills continuously).

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
drop writes that would have succeeded.

### 7.5 Platform failure modes you will meet

| symptom | meaning | action |
|---|---|---|
| domain + preview 404, version `running`, `logs` hangs empty | **crash-loop zombie**: app exits at/near boot repeatedly, platform gave up silently ([repro-no-restart/](./repro-no-restart/)) | fix the boot cause; redeploy (a deploy always heals). With the §7.2 wrapper the cause is in the boot log |
| domain + preview 404, version `running`, but the boot log shows the app **listening** and no request ever reaches it | **the edge never published this service** — seen 2026-07-31 for every new service, across projects and PoPs, including a hello-world app ([repro-edge-404/](./repro-edge-404/)) | not fixable from here. Confirm with `repro-edge-404/repro.sh` (hello-world control) before burning deploys; old services keep working, so do NOT redeploy anything you still depend on |
| single instance dies (OOM/exit/wedge), even under traffic | plaform reprovisions transparently in seconds | nothing — this genuinely works (verified legs 1–5 of the repro) |
| deploy CLI throws `styleText` import error | Node < 20 resolving the CLI | run `bunx --bun @prisma/compute-cli …` |
| first requests after idle are slow | scale-to-zero wake + connection-pool warmup | expected; the 4 s pool idle timeout (§3.1) exists for exactly this |
| a URL that worked before now 503s, service looks healthy | **preview domains are per-version**: a redeploy mints a new one and retires the old | re-resolve after every deploy: `compute versions list --project P --service S \| awk '$2=="running"{print $3}'` |
| domain returns a JSON `binary_exited` body | the wrapper's supervisor caught the child dying | read `exitCode` + `stderrTail` in the body — usually a missing required env var or wrong arch |
| parallel deploys fail with `EEXIST` | concurrent `bunx` invocations race on the shared package cache | fan out regions **sequentially**, or pre-warm with one call |
| `--service` calls all fail after a scripted deploy | the script captured the **version** id (`cpv_…`) that `deploy` prints, not the **service** id (`cps_…`) | take service ids from `services list` only |

Deploy-time footguns (region-code mismatches, version-less service shells,
`bun install` on fresh app dirs) are catalogued in
[deploy/README.md](./deploy/README.md#deploy-footguns).

## 8. Monitoring

**Primary feeds**: fleet heartbeats (object store), LB `/stats` (per-upstream
rps/ackMs/live/cpu + desired), `/v1/debug/*` per instance. OTel export is
spec'd in [OPERATIONS.md §5](./OPERATIONS.md).

**Operator dashboard**: every server serves `/operator` — UNSECURED by
explicit product decision (on-call must see the cell without credentials),
so its payload is restricted to operational metadata: never stream names,
tenant identifiers, tokens, keys, or signed URLs. Panels: this-instance
load/RSS, admission (instance cap, per-stream cap, shed counters, RSS shed
threshold), fleet heartbeat table (freshness judged against the serving
instance's clock, not the browser's), and the per-op object-store latency
table with the O14a sentinels. `/operator/data.json` is the same payload as
JSON; `/operator/runbook` serves this document (compiled into the binary).

**Memory envelope** (AWS-readyness.md §2): the platform kernel kills the
process at ~750 MB RSS on the pilot instance class, and the crash loop that
follows is unrecoverable under load (each replacement replays WAL under full
pressure and dies again — observed on slate-codex 2026-07-21). Therefore:
`ADMIT_RSS_SHED_MB` defaults to 600 and must always sit well below the kill
line; steady-state feature budgets must sum ≤ 450 MB, leaving headroom for
shard-open replay and compaction bursts.

**Healthy baselines** (pilot, 4×1-CPU fleet, 16 shards, conc 128×4 offered):

| signal | healthy | investigate |
|---|---|---|
| ack_p50_ms | 50–65 under load | > 250 sustained 15 min (also the scale-out trigger) |
| rss_mb | 190–300 | > 700 (shed starts at 800) |
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
| flusher stalls though L0 count is low | per-key L0 overlap gate (meta row) | `L0_MAX_SSTS_PER_KEY` (0 = follow `L0_MAX_SSTS`, which we raise to 24) |
| WAL prefix grows unboundedly; watermark lags | flush cadence outrunning WAL GC | keep `FLUSH_INTERVAL_MS ≥ 25` and the 30/60 GC settings |
| deploy applies but service behaves like a different role | project-scope env merge | §7.3 — restate complete env, always |
### Latency knobs (2026-07-27, colleague-review implementation)

| env | default | what it does |
|---|---|---|
| `WAL_POST_ACK_GATHER_MS` | 0 (off) | Pump releases each flush's acks itself (explicit barrier on the durable watch), then waits this long before the next freeze — closed-loop herds join one WAL instead of straddling two. Local A/B (25 ms store): c2 append p50 1.97x -> 1.01x of c1; c32 throughput +70 %, WAL PUT/s down. Soaked at 6. Gathers only when the completed flush left work in flight (drift), so a solo producer pays ~1 ms (herd-settle), not the window. |
| `TAIL_RING_BYTES` | 0 (off) | Per-engine durable-tail ring: dispatch publishes freshly-durable frames to memory BEFORE acks; woken live reads serve from it instead of scanning SlateDB. Canonical scan remains the fallback (restart/eviction/lag/filters). 32 MiB suggested. `/v1/debug/timings` -> `tail_ring{published,hits,misses,evicted}`. |
| `TAIL_MAX_BYTES` | 1 MiB | Budget for reads WOKEN by a long-poll wait (a fresh commit group, not a backlog). Bulk reads keep 8 MiB. |
| `HIST_READER_CAP` | 8 | Cached history DbReaders per process. **Size it ≥ the number of streams concurrently reading history**: LRU with a rotating working set of cap+1 reopens on ~every read (the DST thrash test pins this behavior). Each cached reader costs one manifest poll (5 s) + one checkpoint. |
| `HIST_READER_IDLE_SECS` | 120 | Idle eviction for cached history readers. |
| `WAL_GATHER_SKIP_REQS` / `WAL_GATHER_SKIP_BYTES` | 32 / 1 MiB | Adaptive gather: skip the post-ACK window when the NEXT WAL already holds this much — the window exists for a small next generation; at drift+saturation it is a tax (review #2's CDG throughput question). 0 = never skip. `/v1/debug/timings` pump block: gathers_applied vs gathers_skipped_busy, gathered_reqs, flushed_{reqs,records,bytes} (requests-per-WAL), ack_to_enqueue_{sum_us,count}. |
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
- **GC**: WAL objects reaped per §3.2 after `MIN_AGE`; history SSTs retired
  by compaction; deletion protection, soft-delete windows and GDPR erasure:
  [OPERATIONS.md §2.4](./OPERATIONS.md).
- **Backups / PITR / restore drills**: checkpoint-pin + async copy design in
  [OPERATIONS.md §2](./OPERATIONS.md) (RPO ≤ 5 min target). Not yet wired in
  the pilot — treat the pilot keyspace as re-creatable.
- **Fresh environment**: pick a new `PATH_PREFIX` (and `FLEET_PREFIX`).
  Cheap, instant, and how every pilot run isolated itself.
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

1. `scripts/release-gate.sh` — fmt, clippy no-new-warnings (baseline in
   `scripts/clippy-warning-baseline.txt`), the unit suite, `cargo deny check`
   (advisories, licenses, bans, sources; exceptions live in `deny.toml` and
   `SECURITY.md`).
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
| `spool.quarantined > 0` (alert `read_spool_corruption`) | corrupt spool rows moved to `quarantine/` — those reads are NOT billed | inspect `telemetry/read-spool/<instance>` quarantine rows; recover or write off explicitly; the counter persists across restarts until the quarantine is cleared |
| alert `usage_outbox_lag` | dirty segment snapshots not acknowledged (threshold `ALERT_USAGE_OUTBOX_DIRTY`) | ledger append path down or committer wedged; see `drain.lastOkAgeSecs` |
| `drain.lastOkAgeSecs` large | no successful drain round — ledger unreachable or scans failing CLOSED | financial scans defer on error by design; fix the store fault, drains self-heal |
| `rollup.lastApplyAgeSecs` large with traffic | rollup consumer stalled (cursor not progressing) | check the ROLLUP=1 instance; the ledger retains everything, catch-up is automatic |
| `rollup.oldestUnclosedMonth` far behind | month closes are overdue | closes catch up IN ORDER automatically each tick; investigate close errors in logs if the marker stays put |
| `pendingArtifacts`/`pendingCorrectionArtifacts` stuck > 0 | create-only PUTs failing, or a content mismatch | see next row; transient store faults retry every tick |
| `artifactContentMismatches > 0` | an immutable artifact path holds bytes we did not stage — POTENTIAL TAMPERING | the row stays pending on purpose; diff the object against the rollup row, resolve manually, never overwrite without recording why |
| `tombstoneWalkCloseSubmits` climbing steadily | closures being recovered by the walk rather than the delete path | acceptable under crash churn; investigate if it grows without deletes |
| invoice readiness check | before export: `ready=true`, no open alerts, `oldestUnclosedMonth` = previous month, pending artifact queues empty, mismatches 0 | then the frozen rows + correction artifacts under `telemetry/usage-monthly/` are the invoice inputs |


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
binary refuses it under `STREAMS_RELEASE_POSTURE=1`, and in workload
mode the static token does not exist at runtime even if the
environment still carries it. Feed files (`STREAMS_AUTH_*_FILE`) and
the workload token file MUST be replaced atomically: write a
temporary file, flush, `rename(2)` over the live path — never
truncate-and-rewrite in place (docs/CONTROL-PLANE-INTEGRATION.md
§7.2/§8.2).
