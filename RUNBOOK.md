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
| `s3lite` | local S3 emulator with configurable latency, conditional PUTs, ambiguous failures, stale object/LIST responses, and metadata-preserving body corruption — the dev/CI store |
| `streams-keys` | generates stream encryption keys (32-byte base64) |
| `streams-restore` | validates and restores a complete recovery snapshot into empty, offline object-store targets |
| `streams-shard-admin` | fail-closed offline metadata-only shard split; publishes topology only after both projection clones exist |
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
| `S3_REQUEST_TIMEOUT_MS` | 30000 | overall request deadline (50–300000 ms); timeout errors are retried, but no durable watermark/ACK advances until a request succeeds |

Provider requirements (strong read-after-write, conditional PUT/If-Match,
durability): [OPERATIONS.md §1](./OPERATIONS.md). Tigris satisfies all of
them and negotiates HTTP/1.1 (relevant to §10's latency story). The client
keeps its connection-pool idle timeout at 4 s deliberately: the platform
silently kills flows idle ≳5 s, and a restored scale-to-zero image must wake
with an empty pool rather than dead sockets.

**Recovery snapshot settings:**

| env | default | notes |
|---|---|---|
| `BACKUP_S3_ENDPOINT` / `BACKUP_S3_BUCKET` | — | enables marker-last snapshots. Use a different provider/region and independent credentials; an exact primary endpoint+bucket match is rejected |
| `BACKUP_S3_REGION` | `us-east-1` | backup-provider region |
| `BACKUP_S3_ACCESS_KEY_ID` / `BACKUP_S3_SECRET_ACCESS_KEY` | — | required when backup is enabled; grant create/read and mutable `latest.json`, but no overwrite of `snapshots/` |
| `BACKUP_PATH_PREFIX` | — | recovery namespace inside the backup bucket |
| `BACKUP_INTERVAL_SECS` | 300 | full-snapshot cadence; minimum 60 s |
| `REQUIRE_BACKUP` | false | fail startup when backup is absent; readiness remains false until the first complete snapshot succeeds |

The current actor streams exact ETag-pinned objects to immutable snapshot
prefixes, writes a SHA-256 inventory per object, publishes `_complete.json`
last, and updates `latest.json`. Shared physical role buckets are copied once.
This is the exercised recovery baseline; it is not incremental PITR and does
not yet prune snapshots. Configure a lifecycle policy over `snapshots/` and
budget for a full ciphertext copy per interval until the incremental copy
actor described in [OPERATIONS.md §2](./OPERATIONS.md) replaces it.

### 3.2 Engine (shard log)

| env | default | guidance |
|---|---|---|
| `INITIAL_SHARDS` | 1 | power of two; pilot fleet used 16. Set at keyspace creation; topology is stored |
| `SINGLE_SHARD_WRITE_CEILING_BYTES_PER_SEC` | 0 (automatic split off) | measured sustained payload-byte ceiling for one shard on this deployment. Non-zero enables online split at 60%; do not copy a value across instance/store classes without recalibration |
| `AUTO_SPLIT_SUSTAIN_SECS` | 60 | time above the 60% threshold before automatic split; minimum 1 s |
| `AUTO_MERGE_COLD_FRACTION_PCT` | 10 | combined sibling write rate at/below this percentage of the calibrated ceiling is cold; 0 disables, startup rejects >20 to preserve hysteresis |
| `AUTO_MERGE_SUSTAIN_SECS` | 600 | time both current-owner reports must remain cold before automatic merge; minimum 1 s |
| `FLUSH_INTERVAL_MS` | 25 | WAL flush cadence = the ack floor (flush + one PUT ≈ 40 ms on Tigris at 25 ms). 50 ms halves WAL-object churn for ~10 ms of ack; 5 ms mints WAL SSTs faster than GC reaps them and degrades the watermark to ~0.3–1 s — do not go below 25 |
| `L0_SST_SIZE_BYTES` | 32 MiB | pilot used 8 MiB on 1-GB instances |
| `MAX_UNFLUSHED_BYTES` | 16 MiB | per-shard byte backpressure. SlateDB's default is 512 MB — a byte flood OOMs a 1-GB box before backpressure fires; keep this small |
| `L0_MAX_SSTS` | 8 | L0 count that triggers write backpressure; pilot used 24 for burst headroom |
| `L0_MAX_SSTS_PER_KEY` | 0 (= follow `L0_MAX_SSTS`) | totally-ordered streams rewrite one meta row per memtable, so every L0 overlaps on that key and THIS cap is the real dispatch gate. The upstream default (8) stalled the flusher |
| `MANIFEST_POLL_MS` | 2000 | also how the flusher learns compaction freed L0 slots; loaded shards want 1000–2000. 60 s polls produced 14 s flush stalls |
| `WAL_GC_INTERVAL_SECS` / `WAL_GC_MIN_AGE_SECS` | 30 / 60 | tighter than upstream (60/300): a loaded shard mints ~20 WAL SSTs/s and the WAL prefix must stay small — GC lists share the path with ack-critical PUTs. `MIN_AGE` must cover shard-move replay (<1 s; 60 s is generous) |
| `TRIM_PER_OP` | 8192 | hot-log records retired per absorb commit; must outpace ingest (at 50k rec/s and one pass per 5 s a pass must retire ~250k) |
| `ABSORB_BYTES` / `ABSORB_AGE_SECS` | 4 MiB / 300 | absorber thresholds into the history tier |
| `ABSORB_PASS_BYTES` | 256 MiB | plaintext held in memory per pass — keep well under instance RAM; pilot used 32 MiB on 1-GB boxes |

### 3.3 Memory & runtime (1-GB instance discipline)

| env | default | guidance |
|---|---|---|
| `SHARED_CACHE_BYTES` | 192 MiB | ONE block cache shared by all shard DBs. SlateDB's per-DB default is 512 MB — 16 shards × 512 MB on a 1-GB box dies by cache fill in tens of minutes (this *was* our "platform kills instances" mystery) |
| `HISTORY_CACHE_BYTES` | 32 MiB | shared cache for history-tier/absorber DBs |
| `TOKIO_WORKERS` | max(2, cores) | **do not run one worker.** On 1-vCPU instances the old `#[tokio::main]` default was a single worker; inline blocking quanta (SST build/compress) froze every future including commit acks — the O14a saga. The floor of 2 is enforced in code; the pilot runs 3. Measured effect at identical load: ack-excursion windows 30 % → 10 %, median-window WAL-PUT p99 617 → 141 ms |
| `STORE_MAX_CONCURRENT` | 0 (off) | global cap on concurrent object-store ops. Diagnostic knob — capping did NOT help O14a (proved the bottleneck wasn't outbound concurrency); leave off unless experimenting |

Memory budget that survives on 1 GB under load (pilot-validated): shared
cache 192 + history cache 32 + per-shard unflushed 16×16 + absorber pass 32
+ HTTP buffers ≈ 700 MB envelope, RSS shed at 800, observed steady RSS
190–300 MB with mimalloc.

### 3.4 Auth, crypto, metrics

| env | default | notes |
|---|---|---|
| `AUTH_TOKEN` | — | pilot-only single-tenant bearer and operator token. Production uses the JWKS settings below |
| `AUTH_JWKS_URL` / `AUTH_ISSUER` / `AUTH_AUDIENCE` | — | required together in production; locally verifies RS256/EdDSA JWTs with `sub` customer identity, `jti`, verbs, and stream-name prefixes |
| `AUTH_REVOCATION_URL` | — | required with JWKS; monotonic JSON document `{"version":N,"revoked_token_ids":[...]}` polled off the request path |
| `AUTH_JWKS_REFRESH_SECS` / `AUTH_JWKS_MAX_STALE_SECS` | 600 / 3600 | refresh/fail-closed bounds for verification keys |
| `AUTH_REVOCATION_REFRESH_SECS` / `AUTH_REVOCATION_MAX_STALE_SECS` | 60 / 120 | refresh/fail-closed bounds for token revocation |
| `ALLOW_INSECURE_NO_AUTH` | false | explicit local-development escape hatch; production boot otherwise fails without auth |
| `METRICS_KEY` | — | enables the internal `__metrics__` stream (billing/usage records), encrypted with this key |
| `METRICS_LB_URL` | — | metrics appends are routed like tenant writes (through the LB) so the shard's ring owner serves them |
| `METRICS_AUTH_TOKEN` | — | scoped service JWT for `__metrics__`; required with JWKS mode when metrics are enabled |
| `INSTANCE_NAME` | `streams` | instance tag in metrics + fleet heartbeats (`streams-1`…) |

Per-customer production limits are durable ops-role objects at
`customers/<first-128-bits-of-SHA256(customer-id)>/limits.json`:

```json
{
  "version": 1,
  "max_inflight": 64,
  "write_bytes_per_second": 67108864,
  "write_burst_bytes": 134217728,
  "streams_count": 10000
}
```

Fields are optional overrides of the deployment defaults; an explicit zero
disables that rate/concurrency limit, while `streams_count: 0` forbids new
names. Documents are strictly validated and cached for 60 seconds. Corrupt or
unavailable documents return 503 rather than falling back. Count enforcement
uses a 30-second per-customer CAS lease and recounts durable descriptors, so
two instances cannot concurrently exceed the limit; delete/recreate capacity
follows durable tombstones.

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

### 3.6 Admission control (run with these ON in production)

| env | default | pilot value | behavior |
|---|---|---|---|
| `ADMIT_MAX_INFLIGHT` | 0 (off) | 256 | above this many in-flight requests, `/v1/stream` gets `429 + Retry-After: 1` and a 25 ms tarpit. Direct-path instance capacity measured at ~510 concurrent; 256 is the guarded setting for router-fronted 1-CPU boxes |
| `ADMIT_RSS_SHED_MB` | 0 (off) | 800 | writes (non-GET) get `429 + Retry-After: 2` while RSS exceeds this. Without it a 1-GB box OOM-dies at full throughput instead of shedding |
| `ADMIT_MAX_INFLIGHT_PER_CUSTOMER` | 64 | 64 | hard per-customer share including long polls; prevents one valid tenant from occupying all ingress slots |
| `ADMIT_WRITE_BYTES_PER_SEC_PER_CUSTOMER` | 64 MiB/s | tune by plan | streaming token-bucket rate; body chunks are charged before buffering/WAL admission |
| `ADMIT_WRITE_BURST_BYTES_PER_CUSTOMER` | 128 MiB | tune by plan | per-customer write burst capacity |

The A/B is stark (run 11): identical overload, guards off = all four
instances dead in ~2 minutes; guards on = zero deaths, zero stalls, client
p90 5.4× better at 2× the offered load.

`scripts/ci-noisy-neighbor.sh` is the release-sized isolation check: eight
continuous throttled request streams for one real JWT principal run while
a second principal performs durable producer writes. It requires exact victim
data and bounds victim p99 to 10× its same-run baseline + 250 ms (hard cap
2 s). This catches functional isolation regressions; target-hardware capacity
and tail-latency budgets remain a separate performance/soak gate.

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

**429 semantics**: every body uses
`{"error":{"code":"throttled","scope":…,"dimension":…,"limit":n,
"observed":n,"retry_after_ms":n}}` with standards-compliant `Retry-After: 1`
(in-flight, tenant, or queue shed) or `2` (RSS shed), after a 25 ms tarpit for
instance pressure. SDKs add jitter before retrying. The dimensions identify
their units (`connections`, `streams_count`, `write_burst_bytes`,
`queue_depth`, or `memory_bytes`).
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

## 6. Fleet mode: how it actually works

- **Heartbeats**: every 2 s each instance PUTs
  `<FLEET_PREFIX>/fleet/<instance>.json`: rps, ack_p50_ms, cpu_pct
  (getrusage), inflight/inflight_peak, rss_mb, wal_put_p50/p99_ms,
  out_inflight/peak, owned_shards, and a bounded writer-epoch/cumulative-byte
  tuple for every assigned shard. Staleness > 10 s = not live.
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

### 7.5 Platform failure modes you will meet

| symptom | meaning | action |
|---|---|---|
| domain + preview 404, version `running`, `logs` hangs empty | **crash-loop zombie**: app exits at/near boot repeatedly, platform gave up silently ([repro-no-restart/](./repro-no-restart/)) | fix the boot cause; redeploy (a deploy always heals). With the §7.2 wrapper the cause is in the boot log |
| single instance dies (OOM/exit/wedge), even under traffic | plaform reprovisions transparently in seconds | nothing — this genuinely works (verified legs 1–5 of the repro) |
| deploy CLI throws `styleText` import error | Node < 20 resolving the CLI | run `bunx --bun @prisma/compute-cli …` |
| first requests after idle are slow | scale-to-zero wake + connection-pool warmup | expected; the 4 s pool idle timeout (§3.1) exists for exactly this |

## 8. Monitoring

**Primary feeds**: fleet heartbeats (object store), LB `/stats` (per-upstream
rps/ackMs/live/cpu + desired), `/v1/debug/*` per instance. OTel export is
spec'd in [OPERATIONS.md §5](./OPERATIONS.md).

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
| brief 503s on a shard after a fleet change | 3 s anti-flap hold-off after fencing | normal; clients retry |
| `--env KEY=` rejected / RUST_LOG splits into bogus keys | CLI env parsing | `--unset-env` for removals; quote comma values |

## 11. Data operations

- **Storage layout** (all under `PATH_PREFIX/`): `topology.json` in the ops
  role; `split-intents/` and `shards/<id>/…` in the shard role (SlateDB
  per-shard: `wal/`, `manifest/`, `compacted/`),
  online children under `shards/splits/<operation-id>/<prefix>/…`,
  `history/…` (absorbed per-stream SSTs), `registry/…` (by-name),
  `fleet/`+`routers/` under `FLEET_PREFIX`. Everything except
  topology/fleet metadata is tenant-key ciphertext.
- **Storage format:** new topologies carry `storage_format: 2`. Shard keys
  start with the stable 16-byte tenant/name routing hash followed by a
  16-byte incarnation/segment identity. A missing/other format fails startup;
  there is intentionally no silent reinterpretation of the pre-v2 pilot
  layout. Mixed-v1/v2 rolling deploys are unsupported until the migration
  actor lands.
- **GC**: WAL objects reaped per §3.2 after `MIN_AGE`; history SSTs retired
  by compaction; deletion protection, soft-delete windows and GDPR erasure:
  [OPERATIONS.md §2.4](./OPERATIONS.md).
- **Backup / restore**: the full-copy actor is wired and gates readiness when
  `REQUIRE_BACKUP=true`. CI performs a dark restore and reads the original
  encrypted stream. Stop writers and restore into an empty offline target:

  ```bash
  streams-restore \
    --backup-endpoint "$BACKUP_S3_ENDPOINT" --backup-bucket "$BACKUP_S3_BUCKET" \
    --backup-access-key-id "$BACKUP_S3_ACCESS_KEY_ID" \
    --backup-secret-access-key "$BACKUP_S3_SECRET_ACCESS_KEY" \
    --target-endpoint "$RESTORE_S3_ENDPOINT" --target-bucket "$RESTORE_S3_BUCKET" \
    --target-access-key-id "$RESTORE_S3_ACCESS_KEY_ID" \
    --target-secret-access-key "$RESTORE_S3_SECRET_ACCESS_KEY" \
    --confirm-offline-empty-targets
  ```

  `latest` is the default snapshot; pass `--snapshot-id ID` to pin one.
  Use `--backup-prefix` and `--target-prefix` when the service uses prefixes,
  and the per-role target bucket flags when role buckets are split. The tool
  refuses non-empty targets, incomplete markers, changed inventories, ETag
  changes, byte-count changes, or SHA-256 mismatches. Incremental checkpoint
  pinning, PITR, retention, and scrubber drills remain GA work in
  [OPERATIONS.md §2](./OPERATIONS.md).
- **Online shard split:** send an operator-authenticated request to the
  current ring owner (`root` means the empty prefix):

  ```bash
  curl -X POST "$STREAMS_URL/v1/admin/shards/root/split" \
    -H "authorization: Bearer $OPERATOR_TOKEN"
  ```

  The actor creates a conditional intent in the shard store, returns 503 for
  that prefix, drains all earlier groups through remote durability, flushes
  and closes the parent, creates and reopens two exact projection clones, and
  publishes both generation-specific paths in one topology CAS. Producer
  clients must retry 503/408/429 with the same producer sequence. A 12-second
  renewable lease permits bounded takeover; a replacement rotates to fresh
  clone paths, derives progress from durable objects, and cleans the intent
  only after topology publication. Every parent ACK checks the intent after
  WAL durability, so even a stale second owner cannot acknowledge data that
  misses the children.

  `scripts/ci-split-crash-matrix.sh` aborts the release binary after each of
  the seven durable transitions and proves recovery. Its
  `STREAMS_TEST_SPLIT_CRASH_AFTER` environment variable is a CI-only test hook
  and must never be present in a production manifest.

  Takeover atomically records superseded, never-published clone operations in
  the intent and materializes durable GC-candidate markers. Candidates are
  collected every
  `SPLIT_GC_INTERVAL_SECS` (default 300) only after
  `SPLIT_GC_RETENTION_SECS` (default 86400). The scanner protects every
  operation referenced by current topology or any active intent, re-reads
  both immediately before deletion, and fails closed if one run sees more
  than 100,000 split objects. It never infers that a published ancestor is
  disposable because clone manifests can retain its SSTs. Zero retention is
  reserved for CI.

  Set `SINGLE_SHARD_WRITE_CEILING_BYTES_PER_SEC` to the calibrated ceiling to
  enable the same actor automatically after 60% is sustained for
  `AUTO_SPLIT_SUSTAIN_SECS`. Zero is the explicit disable override. CI covers
  concurrent producers, restart and different-identity takeover, automatic
  recursive refinement, and a stale two-owner race with split role buckets.

- **Online shard merge:** send an operator-authenticated request to the
  current coordinator for the parent (`root` means children `0` and `1`):

  ```bash
  curl -X POST "$STREAMS_URL/v1/admin/shards/root/merge" \
    -H "authorization: Bearer $OPERATOR_TOKEN"
  ```

  Both child intent paths become durable ACK fences before either source is
  quiesced. The actor drains WAL then L0, builds and verifies a non-overlapping
  manifest union, and publishes both children→parent in one topology CAS.
  Leases, takeover generations, released CAS tombstones, and abandoned-target
  GC follow the split protocol. The seven-phase merge crash matrix, a stale
  writer drill, repeated split→merge cycles, and takeover-GC drill run in CI.

  With a non-zero calibrated ceiling, the coordinator also merges the deepest
  eligible sibling pair after its combined current-owner-reported rate remains
  below `AUTO_MERGE_COLD_FRACTION_PCT` for `AUTO_MERGE_SUSTAIN_SECS`. It
  requires fresh monotonic reports from both current owners; a stopped owner,
  engine reopen, ring change, malformed vector, or unchanged heartbeat cannot
  advance the clock. CI proves both the hot guard and this remote-owner
  fail-closed behavior. Set the fraction to zero for an operator freeze.

- **Offline shard split fallback:** stop every serving writer for the cell,
  then run:

  ```bash
  streams-shard-admin --parent root --confirm-serving-quiesced \
    --s3-endpoint "$SLATE_S3_ENDPOINT" --bucket "$SLATE_S3_BUCKET" \
    --region "$SLATE_S3_REGION" --path-prefix "$PATH_PREFIX"
  ```

  Use the binary bit prefix instead of `root` for a deeper split and pass
  `--ops-bucket` / `--shard-bucket` when roles differ. It refuses a non-live
  parent and any non-empty child path, creates exact metadata-only projection
  clones, then changes `topology.json` with one expected-version CAS. A CAS
  failure leaves safe orphan children and the old topology live. CI proves
  opposite hash halves remain readable and independently writable after
  restart. This tool remains useful for an offline repair, but the HTTP actor
  is the normal online path.
- **Fresh environment**: pick a new `PATH_PREFIX` (and `FLEET_PREFIX`).
  Cheap, instant, and how every pilot run isolated itself.
- **Decommission**: stop generators, redeploy without `KEEP_AWAKE`, let the
  platform sleep the fleet; delete the prefix when the data is disposable.

## 12. Security notes for operators

Bearer token gates the API; the stream key gates the data (two independent
factors — a leaked token cannot decrypt). Keys never persist server-side;
backups are ciphertext. Metrics stream is encrypted under `METRICS_KEY`.
Full identity/custody/audit design: [OPERATIONS.md §3](./OPERATIONS.md).
Never commit tokens, keys, or presigned URLs; the deploy scripts keep them
in a local scratch directory outside the repo.
