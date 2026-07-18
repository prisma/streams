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
| `streams-registry-restore` | restartably merges one cell point's exact registry closure into an offline global registry target; conflicting bytes fail closed |
| `streams-provider-check` | destructive unique-prefix probe for conditional, consistency, ordered/exclusive cursor listing, range, multipart, copy, and delete semantics required from each recovery/audit provider |
| `streams-at-rest-check` | exact-ETag stable-corpus scan for operator-supplied forbidden payload/key byte patterns; emits aggregate evidence without echoing secrets |
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

**Managed-cell settings:**

| env | default | notes |
|---|---|---|
| `CELL_ID` | — | enables managed-cell mode; requires production JWKS auth and `PATH_PREFIX=cells/<id>` |
| `CELL_DIRECTORY_REFRESH_SECS` | 60 | 5–3600 s; `cells.json` generation rollback or same-generation mutation retains last-known-good routing, fails the cells readiness component after two missed refresh windows, and blocks new placement |
| `REGISTRY_S3_ENDPOINT` / `REGISTRY_S3_BUCKET` / `REGISTRY_S3_REGION` | — | explicit global registry authority in managed mode; it contains `cells.json`, affinities, descriptors, and per-cell recovery indices |
| `REGISTRY_S3_ACCESS_KEY_ID` / `REGISTRY_S3_SECRET_ACCESS_KEY` | — | required and must use an access-key id distinct from the cell data principal |
| `REGISTRY_PATH_PREFIX` | — | required, non-empty, and outside `cells/`; never reuse a cell-local prefix |
| `REGISTRY_S3_ALLOW_HTTP` | false | test-only escape hatch |

The descriptor's immutable `cell` decides all existing operations. A request
at another cell returns 409 with `Streams-Replay-To-Cell` before shard/key work.
New placement uses the last-known-good directory and durable customer affinity;
draining/frozen or zero-weight cells receive no new streams but continue serving
their pinned streams. Backup and primary scrub list only the local immutable
`registry/by-cell/<id>/` projection, then validate each entry against the
global descriptor; they never enumerate another cell's history namespace.

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
| `BACKUP_S3_ACCESS_KEY_ID` / `BACKUP_S3_SECRET_ACCESS_KEY` | — | required when backup is enabled; destination needs read/create plus overwrite for `latest.json`, source indexes, and blob references, and delete for bounded retention |
| `BACKUP_PATH_PREFIX` | — | recovery namespace inside the backup bucket |
| `BACKUP_INTERVAL_SECS` | 300 | incremental recovery-point cadence; minimum 60 s |
| `BACKUP_RPO_BUDGET_SECS` | 300 | maximum age of the newest fully protected point; 60 s–1 day and never lower than the snapshot interval |
| `BACKUP_RETENTION_SECS` | 604800 | complete/partial point and unreferenced-blob retention; at least two intervals, at most one year |
| `BACKUP_CHECKPOINT_LIFETIME_SECS` | 3600 | expiry safety net for per-shard SlateDB pins; at least two intervals, at most one day; successful copies delete eagerly |
| `BACKUP_SCRUB_INTERVAL_SECS` / `BACKUP_SCRUB_OBJECTS_PER_INTERVAL` | 60 / 256 | continuously hash this many referenced recovery blobs; 10 s minimum, 100000 maximum batch |
| `PRIMARY_SCRUB_INTERVAL_SECS` / `PRIMARY_SCRUB_OBJECTS_PER_INTERVAL` | 60 / 16 | bounded live-primary sweep cadence/batch; readiness stays red until a complete sweep after startup or coordinator takeover |
| `PRIMARY_SCRUB_MAX_OBJECT_BYTES` | 268435456 | fail-closed per-manifest/SST/WAL read bound; 1 MiB to 1 GiB |
| `BACKUP_WRITE_FORMAT` | 3 | recovery-corpus writer; use 2 for the read-first/rollback wave and 3 after every backup and restore binary is format-3 capable |
| `REQUIRE_BACKUP` | false | fail startup when backup is absent; readiness requires a complete post-primary-sweep point plus healthy recovery and primary scrubbers |

**Independent audit settings:**

| env | default | notes |
|---|---|---|
| `AUDIT_MIRROR_S3_ENDPOINT` / `AUDIT_MIRROR_S3_BUCKET` | — | enables an immutable second audit copy; production requires a different provider/account. The primary endpoint+ops-bucket pair is rejected |
| `AUDIT_MIRROR_S3_REGION` | `us-east-1` | audit-provider region |
| `AUDIT_MIRROR_S3_ACCESS_KEY_ID` / `AUDIT_MIRROR_S3_SECRET_ACCESS_KEY` | — | required together; the access-key id must differ from the primary identity and the principal needs read/create/delete only in its audit prefix |
| `AUDIT_MIRROR_PATH_PREFIX` | — | independent audit namespace inside the mirror bucket |
| `AUDIT_MIRROR_S3_ALLOW_HTTP` | false | test-only HTTP escape hatch; leave false in production |
| `REQUIRE_AUDIT_MIRROR` | false | production release guard; fail startup unless the independent mirror is configured |
| `AUDIT_SAMPLE_DENOMINATOR` | 100 | sample one in N authenticated data-plane requests; 1–1,000,000 |
| `AUDIT_PRIMARY_RETENTION_SECS` / `AUDIT_MIRROR_RETENTION_SECS` | 30 d / 365 d | provider-clock retention; mirror retention must cover primary retention and may be at most seven years |
| `AUDIT_MAINTENANCE_INTERVAL_SECS` / `AUDIT_MAINTENANCE_OBJECTS_PER_INTERVAL` | 300 / 1000 | durable-cursor reconciliation and pruning cadence/bound |
| `AUDIT_MAINTENANCE_MAX_OBJECT_BYTES` | 8 MiB | fail-closed stable-read bound for one control or NDJSON audit object |

Control operations succeed only after identical immutable writes to primary and
mirror. Sampled batches retain one path and body while retrying either failed
side. On every process start, readiness remains red until durable cursors have
traversed both primary audit prefixes and byte-verified or repaired every
mirror object. Primary deletion occurs only after that exact mirror check;
retention age and “now” both come from each provider's object metadata, not a
host clock. Cursor updates use conditional writes, so overlapping generations
cannot silently skip a range. Provision provider-native encryption, object
lock/lifecycle, and export access on the mirror account as deployment policy;
audit payloads contain identity metadata and are deliberately not encrypted
with customer stream keys.

The actor creates an expiring checkpoint for every initialized live shard and
every initialized active stream-history DB after the shard cut; it records
never-initialized DBs as explicitly absent. The 100,000-history-DB cell bound
is fail-closed and includes per-key segments. It recursively pins external
clone ancestors, exposes only the selected manifest closure, compatible
compactions record, and WAL interval, and rechecks topology plus every pin
before publication. A durable WAL can precede the pinned manifest's remote
`next_wal_sst_id`; the actor captures the complete contiguous ETag set above
the replay watermark and immediately copies this pre-manifest suffix to
immutable recovery content as each DB cut is observed. It does so before
history enumeration and the general object walk, so primary WAL GC cannot
overtake a large-cell recovery point.
In managed mode the same point declares a separate `registry` role. Before
descriptor publication the service already created an immutable per-cell
marker. The backup actor lists only that cell prefix, validates each marker
against the authoritative descriptor and durable affinity, and eagerly copies
those exact ETag-fenced objects plus `cells.json` before pinning their history
DBs. Missing/losing markers are safe orphans; conflicting affinity, identity,
or placement fails the point. This is O(streams in cell), not O(global streams).
Exact source ETags feed durable per-path indexes; unchanged objects reuse
immutable SHA-256 blobs. Each point still gets a complete checksummed inventory,
then `_complete.json` is created last and `latest.json` advances. Retention
deletes expired complete/partial generations and only then reclaims blobs whose
last referencing generation expired. The scrubber walks blob references with a
durable provider-independent cursor, so a missing as well as corrupt recovery
object fails readiness. Shared physical role buckets are copied once,
preferring the shard role so manifest filtering cannot be bypassed.

The same fenced cell actor continuously validates live primary authority with
a durable cursor. It bounds and decodes the newest manifest, every currently
referenced shard SST block/index/statistics record, and every live WAL found
either in the manifest interval or above its replay watermark in object
storage. This latter union includes acknowledged WALs whose next ID has not yet
reached a remote manifest. History blocks use customer-held keys, which the
background actor deliberately does not retain. The absorber therefore
logically decodes each newly written history SST while it has the request key,
then creates an immutable whole-ciphertext SHA-256 baseline under
`integrity/history/`; later sweeps compare primary bytes to that baseline. A
missing baseline fails closed. Any primary failure clears snapshot health, and
repair must finish a primary sweep and publish a fresh recovery point before
readiness returns. `scripts/ci-primary-scrub.sh` corrupts same-length shard and
encrypted-history SSTs and proves this red-to-repaired transition.

Every backup-enabled instance contends for `backup/coordinator-lease.json` in
the cell's ops store. The holder CAS-increments a renewal sequence every two
seconds; there is no wall-clock lease deadline. A contender records the exact
token/epoch/sequence/content digest plus provider version and may take over only
after that identity is unchanged for six seconds on its local monotonic clock.
Only the holder may snapshot, prune, or advance the scrub cursor. Mutable
source indexes, blob references, scrub state, `latest.json`, and
`backup/health.json` carry the lease epoch plus a monotonic sequence and use
conditional writes; a delayed old
holder cannot overwrite takeover state. Followers initially fail closed until
they observe the same holder renew, then accept only health published at or
after that proof. After an unchanged six-second interval they repeat the
handshake. Health freshness uses leader-relative monotonic ages and bounded
renewal gaps; its Unix timestamps are audit-only. Thus all instances expose the
same readiness result without duplicating backup I/O. `INSTANCE_NAME` is the
bounded coordinator identity. Protocol-1 absolute-expiry leases are readable
only for a monotonic takeover upgrade; see `STORAGE-MIGRATIONS.md` before any
binary rollback.
Format-3 blobs and references live under `formats/3/` and include the lease
epoch in their physical path. A takeover checksum-rehomes unchanged content
before publishing its point. Thus a provider DELETE admitted by epoch N can
only name epoch-N content and cannot damage an epoch-(N+1) point. During the
format-2 read-first wave, coordinated legacy-blob GC is disabled; format 3
reclaims it only after live content has been re-homed. Retention durably creates
`gc-intents/<snapshot>/intent.json`, removes the complete marker, deletes
content/metadata, and removes the intent last, so a crash resumes instead of
leaking an unreachable last-reference blob. Its cutoff comes from
`retention/provider-clock.json`: the actor CAS-writes a random probe and compares
that provider timestamp only with provider `Last-Modified` metadata. Host Unix
time is audit-only, and the newest completed recovery point is retained even
after the nominal window.

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
| `HISTORY_BLOCK_WRITE_FORMAT` | 2 | `1` emits legacy raw-key history blocks for the mandatory read-first wave; `2` emits HKDF/AAD incarnation-bound blocks. Readers accept both. Follow `STORAGE-MIGRATIONS.md` before changing an existing cell |
| `ABSORB_PASS_BYTES` | 256 MiB | plaintext held in memory per pass — keep well under instance RAM; pilot used 32 MiB on 1-GB boxes |

Every record-producing commit (including queue DLQ references) atomically
updates a per-stream `a` debt marker with the exact plaintext bytes not yet in
the history tier. Before a reopened shard accepts traffic it seeks once per
storage hash, reconstructs at most 100,000 pending streams, and forces those
passes without waiting for a future append. Because customer keys are never
persisted, a brand-new process keeps recovered work pending until an
authenticated read or write supplies the key; the forced item then retries
without requiring an append. Logs written by an older binary without markers
use the cumulative logical-byte counter conservatively. A
corrupt marker or an over-cap recovery set fails shard open; partial passes
decrement the marker exactly, and only the remotely durable absorbed-frontier
commit clears it. The marker begins with the routing/storage hash, so ordinary
split projection and merge union carry the scheduler state with the records.
The absorber keeps one memory-bounded pass in flight while continuing to drain
its bounded notification receiver.

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
| `METRICS_CUSTOMER_ID` | — | exact `sub` of the scoped metrics principal; only this customer plus `__metrics__` name is excluded from self-metering |
| `METRICS_EXPORT_INTERVAL_SECS` | 15 | bounded per-tenant/per-stream RED interval; sequence advances only after the encrypted append is acknowledged |
| `REQUIRE_METRICS_EXPORT` | false | production release guard; fail startup unless key, LB URL, and customer id are all configured |
| `INSTANCE_NAME` | `streams` | instance tag in standalone mode. Fleet mode requires `streams-N`, with `1 <= N <= FLEET_MAX` |

Admission defaults are deployment-wide fallbacks; every production customer
can override them with the durable object below.

| env | default | notes |
|---|---|---|
| `ADMIT_MAX_INFLIGHT_PER_CUSTOMER` / `ADMIT_MAX_LIVE_CONNECTIONS_PER_CUSTOMER` | 64 / 32 | all response bodies retain the first slot until EOF/disconnect; SSE, long-poll, and queue receive also retain the live slot |
| `ADMIT_WRITE_BYTES_PER_SEC_PER_CUSTOMER` / `ADMIT_WRITE_BURST_BYTES_PER_CUSTOMER` | 64 MiB / 128 MiB | ingress token bucket; rate 0 disables |
| `ADMIT_APPEND_REQUESTS_PER_SEC_PER_CUSTOMER` / `ADMIT_APPEND_REQUEST_BURST_PER_CUSTOMER` | 10000 / 10000 | append request bucket; rate 0 disables |
| `ADMIT_READ_REQUESTS_PER_SEC_PER_CUSTOMER` / `ADMIT_READ_REQUEST_BURST_PER_CUSTOMER` | 10000 / 10000 | GET/HEAD/list request bucket; rate 0 disables |
| `ADMIT_READ_BYTES_PER_SEC_PER_CUSTOMER` / `ADMIT_READ_BURST_BYTES_PER_CUSTOMER` | 128 MiB / 256 MiB | paces finite and SSE response frames; rate 0 disables |
| `ADMIT_QUEUE_RECEIVES_PER_SEC_PER_CUSTOMER` / `ADMIT_QUEUE_RECEIVE_BURST_PER_CUSTOMER` | 5000 / 5000 | receive only; ack and extend do not consume it |
| `ADMIT_APPEND_REQUESTS_PER_SEC_PER_STREAM` / `ADMIT_APPEND_REQUEST_BURST_PER_STREAM` | 5000 / 5000 | descriptor fallback for legacy streams and creates without provisioning headers |
| `ADMIT_WRITE_BYTES_PER_SEC_PER_STREAM` / `ADMIT_WRITE_BURST_BYTES_PER_STREAM` | 50 MiB / 100 MiB | per-incarnation ingress bucket; rate 0 disables |
| `STREAM_COMMIT_WEIGHT` | 1 | descriptor fallback; relative 1..=100 share among one customer's streams, below the equal outer tenant scheduler |

Per-customer production limits are durable ops-role objects at
`customers/<first-128-bits-of-SHA256(customer-id)>/limits.json`:

```json
{
  "version": 1,
  "max_inflight": 64,
  "max_live_connections": 32,
  "write_bytes_per_second": 67108864,
  "write_burst_bytes": 134217728,
  "append_requests_per_second": 10000,
  "append_request_burst": 10000,
  "read_requests_per_second": 10000,
  "read_request_burst": 10000,
  "read_bytes_per_second": 134217728,
  "read_burst_bytes": 268435456,
  "queue_receives_per_second": 5000,
  "queue_receive_burst": 5000,
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

Non-count limits are divided into ceil-shares using fresh active fleet
membership. Consequently, very small limits can overshoot by at most one unit
per additional active instance; 429 `limit` remains the customer value and
`observed` is the cell estimate. Read-byte limits pace an admitted body rather
than aborting it after headers.

Create-time stream provisioning uses
`Stream-Append-Requests-Per-Second`, `Stream-Append-Request-Burst`,
`Stream-Write-Bytes-Per-Second`, `Stream-Write-Burst-Bytes`, and
`Stream-Commit-Weight`. Values are persisted in the descriptor and are part of
idempotent PUT configuration matching; changing one requires a new stream
incarnation. Request and byte excess return a stream-scoped 429 before shard
enqueue. The committer schedules tenants equally, then honors the bounded
weight among that tenant's streams.

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
| `FLEET_PREFIX` | — | shared coordination prefix: heartbeats, aggregate lease, `fleet.json`, `fleet/desired.json`, router reports |
| `FLEET_MAX` | 4 | hard fleet-size cap (1–64); `INSTANCE_NAME` must be an ordinal within it |
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
| `GET /health` / `GET /health/ready` | none | readiness; 503 names a degraded serving state only through the operator metrics below |
| `GET /health/live` | none | process liveness (`ok`); wake/restart probes use this, never readiness |
| `GET /v1/streams` | bearer | list streams |
| `PUT /v1/stream/{name}` | bearer + `Stream-Encryption-Key` | create (400 `missing_key` without the key header) |
| `POST /v1/stream/{name}` | bearer + key | append (`{"events":[…]}`); 204 on durable commit |
| `GET /v1/stream/{name}?…` | bearer + key | read/tail (offsets, long-poll, SSE; profile-specific routes per [PROFILES.md](./PROFILES.md)) |
| `GET /v1/debug/timings` | bearer | per-shard commit-pipeline rings: `queue_wait_us`, `encode_us`, `write_us`, `durable_wait_us` per group — splits our pipeline from store waits |
| `GET /v1/debug/load` | operator bearer | `inflight_now`, `inflight_peak` (swap-on-read), `rss_mb`, `admit_shed` |
| `GET /v1/debug/store?window=60&swap=1` | bearer | per-(op,class) object-store latency cells (`put:wal`, `get:manifest`, …: n/err/p50/p90/p99/max), slow-op ring (≥300 ms with paths), outbound in-flight gauge, **timer sentinels** (`timer_thread`, `timer_tokio` drift) and `steal_pct`. `swap=1` resets the gauge peak — samplers only |
| `GET /v1/debug/metrics` | operator bearer | bounded OpenMetrics RED/component/backup/WAL/memory/per-open-shard scrape; no tenant-controlled labels |
| `GET /v1/debug/sleep?ms=N` | operator bearer | calibrated-latency probe (≤5000 ms): separates concurrency caps from rate caps at the edge |

**429 semantics**: every body uses
`{"error":{"code":"throttled","scope":…,"dimension":…,"limit":n,
"observed":n,"retry_after_ms":n}}` with standards-compliant `Retry-After: 1`
(in-flight, tenant, or queue shed) or `2` (RSS shed), after a 25 ms tarpit for
instance pressure. SDKs add jitter before retrying. The dimensions identify
their units (`connections`, `live_connections`, `append_burst_requests`,
`read_burst_requests`, `queue_receive_burst_requests`, `streams_count`,
`write_burst_bytes`, `queue_depth`, or `memory_bytes`).
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
- **Aggregation**: one process holds `fleet/aggregate-lease.json` through a
  random process token, monotonic epoch, conditional CAS, and six-second
  renewable expiry. Only it reads individual heartbeats/router reports and
  conditionally writes bounded `fleet.json`; all other servers and the pilot
  LB read that single snapshot. Takeover increments the epoch, so a delayed
  former holder cannot regress the view.
- **Desired count**: only the current aggregator CAS-writes
  `fleet/desired.json`. The formula includes the 32-shards-per-instance floor.
- **Failure posture**: stale/corrupt `fleet.json`, lease, heartbeat, router
  report, or `desired.json` makes `/health/ready` fail and clears merge activity
  evidence, while requests retain the last installed ring. Repair the corrupt
  object from a verified prior version; the lease holder then resumes at a
  higher conditional sequence.
- **Placement**: rendezvous hash (FNV-1a over `"<shard> <instance>"`) across
  the first `desired` instances, computed identically by servers and LBs —
  the live set IS the assignment. There is no per-shard lease service; the
  aggregate lease controls telemetry publication, not data correctness.
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
  scale-in. Names must be `router-N`, `1 <= N <= 32`, so the aggregator can
  GET a fixed bounded set without LIST/tombstone accumulation (run 7: the fleet once scaled IN during client congestion,
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

1. Run `cargo deny check`, formatting, targeted warning-as-error clippy, the
   full release suite, and every CI drill against the exact locked graph. The
   three audited RustSec exceptions and removal conditions live in
   `SECURITY.md`; no new exception is a routine dependency update.
2. Build + verify arch (§2). Upload binaries; capture presigned URLs.
3. Roll servers one at a time from the canonical script (full env restated,
   including the binary URL).
4. **Health-gate each instance before the next**: poll `/health` up to
   ~2 min (wake + boot + shard reopen). If it 404s past that, check the
   version's *preview domain* (`cv-….prisma.build/health`) — service-domain
   404 with healthy preview = route propagation; both 404 = boot failure.
5. Roll LBs the same way.
6. Redeploying under live load can zombie an instance (observed ~once per
   ~20 deploys). The heal is simply another deploy. Watch the first minute
   of `/v1/debug/load` after each roll.
7. After load tests: destroy generator versions, redeploy servers/LBs
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
rps/ackMs/live/cpu + desired), and an operator-authenticated scrape of
`/v1/debug/metrics` per instance. The scrape contains fixed operation/status/
component labels plus the bounded set of open binary shard prefixes; customer
and stream names never become monitoring labels. Configure the collector with
trusted `region`, `cell`, and `instance` target labels and convert OpenMetrics
to OTel at the platform boundary if that is the platform-native transport.

Load [ops/prometheus-alerts.json](./ops/prometheus-alerts.json) as a Prometheus-
compatible JSON/YAML rule file. Every alert has a hold time, page/ticket
severity, cell blast radius, and checked-in runbook target. CI validates the
bounded 20-rule schema, including missing/unhealthy audit and billing sinks,
active-tail freshness, absorber backlog, protected-point age, fence rate, L0
compaction debt, and unflushed-WAL recovery debt.
`scripts/ci-operability-game-day.sh` proves the scrape requires operator
authorization, records live RED/shard/tail/absorber signals, leaks no
tenant-controlled label, replays a stale topology, observes readiness 503 and
`component="topology"} 0`, then restores the current topology and requires
recovery. Backup and stale-owner drills separately prove a finite protected-
point age and an observed reconfiguration fence. A release environment must
additionally prove its real collector, rule evaluator, notification route,
inhibition, and ownership labels.

`scripts/ci-audit-mirror.sh` cuts an independent audit provider and proves
control-plane fail-closed behavior, retry-stable sampled batches, and recovery.
`scripts/ci-billing-export.sh` rejects an exporter append at the HTTP boundary,
then proves byte-identical payload and producer id/epoch/sequence on retry,
one encrypted delivered interval, and no recursive self-metering. These are
software failure drills; the production identities, provider/account blast
radius, collector, and notification path still require deployed evidence.

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
| tail freshness p99 | < 500 ms | sustained breach = live delivery path or scheduler delay |
| absorber pending bytes | < 256 MiB | sustained breach = history-store/provider or actor throughput |
| L0 SSTs per shard | ≤ 24 | sustained excess = compaction cannot track ingest |
| unflushed WAL SSTs per shard | ≤ 1024 | sustained excess = recovery/object-count debt |
| protected recovery-point age | ≤ configured RPO budget | breach = snapshot/coordinator/provider failure |
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

### 9.1 Target-hardware release soak

Every release candidate must run the checked soak judge for at least 24 hours
against the real load-balanced Compute cell and scrape every instance directly.
The judge records exact durable stream-offset reconciliation, request rate and
errors, p50/p99/p99.9 ACK latency, readiness/component failures, RSS maximum
and quartile growth, absorber maximum/drain, L0 and unflushed-WAL debt, fencing,
and protected-point RPO. It writes one bounded JSON artifact and fails any
configured regression budget. Credentials are accepted only through the three
secret environment variables and are never serialized.

```bash
export SOAK_STREAM_KEY=...          # fresh customer key for this run
export SOAK_AUTH_TOKEN=...          # scoped workload principal
export SOAK_OPERATOR_TOKEN=...      # operator-only metrics principal

scripts/release-soak.py \
  --url https://cell-router.example \
  --metrics-url https://streams-1.internal \
  --metrics-url https://streams-2.internal \
  --bench-bin target/release/bench \
  --evidence /absolute/release-evidence/soak.json \
  --release-id "$(git rev-parse HEAD)" \
  --target-label compute-production-shape \
  --instance-class 1cpu-1gb \
  --storage-provider tigris-independent-recovery \
  --require-backup \
  --min-req-per-sec 1000
```

Tune throughput and latency budgets only from an approved baseline for the
same instance/store class; record the chosen arguments with the artifact. The
script refuses a run shorter than 24 hours unless `--allow-short` is explicit.
That escape is used only by `scripts/ci-release-soak-harness.sh` to test the
judge, authenticated benchmark, metric parser, durable-offset proof, and
secret-free artifact shape. A short or local run is not release evidence.

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
- **Backup / restore**: checkpoint-pinned incremental recovery points gate
  readiness when `REQUIRE_BACKUP=true`. CI creates two points around a durable
  append, proves the second reuses blobs, restores the older point without the
  append, and restores the newer point with it. Stop target writers and restore
  into an empty offline target:

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
  refuses non-empty targets, incomplete markers, changed inventories,
  byte-count changes, or SHA-256 mismatches. Format-1 full-copy points remain
  restorable for rollback; format 2 resolves shared content-addressed blobs.
  Format 3 binds each point and content path to its coordinator epoch while
  checksum-rehoming unchanged objects on takeover. All content-addressed
  formats allow an operator to repair a blob in place only with the exact
  expected bytes. Follow [STORAGE-MIGRATIONS.md](./STORAGE-MIGRATIONS.md) for
  the read-first format-2 to format-3 flip and one-version rollback.
  Encrypted-history integrity baselines are create-only and must exist from the
  first history write. A pre-existing corpus needs the keyed backfill procedure
  in that migration document; there is intentionally no keyless auto-baseline.
  Recovery is point-exact, not arbitrary restore-to-timestamp PITR. The RPO is
  measured from the newest acknowledged record present in the restored point;
  RTO ends at the first decrypted read from a service using only the recovery
  provider.

  A managed multi-cell disaster is restored as an offline union. For every
  selected cell point, first merge its registry closure into one common target:

  ```bash
  streams-registry-restore --snapshot-id latest \
    --backup-endpoint "$BACKUP_S3_ENDPOINT" --backup-bucket "$BACKUP_S3_BUCKET" \
    --backup-access-key-id "$BACKUP_S3_ACCESS_KEY_ID" \
    --backup-secret-access-key "$BACKUP_S3_SECRET_ACCESS_KEY" \
    --backup-prefix "$CELL_BACKUP_PREFIX" \
    --target-endpoint "$RESTORE_REGISTRY_S3_ENDPOINT" \
    --target-bucket "$RESTORE_REGISTRY_S3_BUCKET" \
    --target-access-key-id "$RESTORE_REGISTRY_S3_ACCESS_KEY_ID" \
    --target-secret-access-key "$RESTORE_REGISTRY_S3_SECRET_ACCESS_KEY" \
    --target-prefix "$RESTORE_REGISTRY_PATH_PREFIX" \
    --confirm-registry-offline
  ```

  Repeating a cell is idempotent. Existing bytes must match size and SHA-256;
  differing descriptors, affinities, indices, or directory bytes stop the
  merge instead of choosing a winner. Restore each cell's data point to its own
  empty target with `streams-restore --skip-registry`, then start no cell until
  all intended closures are merged and an operator has verified `cells.json`.
  `--allow-http` exists only for the local drill. The CI two-cell drill destroys
  serving, restores separate primary/registry targets, and proves the first
  decrypted read using only the recovery point.

  A measured failover against the actual independent provider remains a GA
  release gate in [OPERATIONS.md §2](./OPERATIONS.md). Provision unique empty
  primary, recovery-corpus, and activated-target prefixes; build all release
  binaries; then run:

  ```bash
  export PRIMARY_PROVIDER_ID=... PRIMARY_S3_ENDPOINT=... PRIMARY_S3_BUCKET=...
  export PRIMARY_S3_REGION=... PRIMARY_S3_ACCESS_KEY_ID=... PRIMARY_S3_SECRET_ACCESS_KEY=...
  export PRIMARY_PATH_PREFIX=...
  export RECOVERY_PROVIDER_ID=... RECOVERY_S3_ENDPOINT=... RECOVERY_S3_BUCKET=...
  export RECOVERY_S3_REGION=... RECOVERY_S3_ACCESS_KEY_ID=... RECOVERY_S3_SECRET_ACCESS_KEY=...
  export RECOVERY_PATH_PREFIX=...
  export FAILOVER_S3_ENDPOINT="$RECOVERY_S3_ENDPOINT" FAILOVER_S3_BUCKET=...
  export FAILOVER_S3_REGION=... FAILOVER_S3_ACCESS_KEY_ID=... FAILOVER_S3_SECRET_ACCESS_KEY=...
  export FAILOVER_PATH_PREFIX=...
  export DRILL_PRIMARY_CUTOVER_HOOK=/absolute/path/to/executable-cut-hook
  export DRILL_PRIMARY_RECOVER_HOOK=/absolute/path/to/optional-recover-hook
  export DRILL_RPO_BUDGET_MS=300000 DRILL_RTO_BUDGET_MS=1800000
  export DRILL_EVIDENCE_PATH=/absolute/path/to/provider-failover.json
  scripts/provider-failover-drill.sh
  ```

  Provider IDs, endpoint authorities, and access-key IDs must differ. The cut
  hook must make the primary API unavailable; a process PID is accepted only
  by hermetic tests. The harness first runs `streams-provider-check` against
  both providers, then records conformance timings, exact recovered sequence,
  monotonic RPO/RTO, restore report, and post-activation write proof in the JSON
  artifact. Never point the activated target at a non-empty namespace.

  `scripts/ci-provider-failover.sh` runs this identical path against two
  independent `s3lite` processes and actually kills the primary. On
  2026-07-18 it recovered sequence 1, intentionally lost sequence 2, measured
  8.751 s RPO and 519 ms RTO, and verified a post-failover write. This validates
  the protocol and measurement harness, not independent-provider blast-radius
  isolation.
  CI also runs two backup-enabled instances, kills the actual lease holder,
  requires the survivor to wait through the six-second monotonic unchanged-
  version window, publishes an epoch-incremented point, and rejects
  duplicate completion markers. It then starts a format-2 rollback writer,
  requires another epoch increment and reused objects, and proves the format-2
  and format-3 reference roots coexist.
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
- **First managed-cell cutover:** publish `cells.json` with exactly one active
  cell and keep serving quiesced. Audit, apply, and verify placement with:

  ```bash
  streams-cell-admin --cell-id "$CELL_ID" \
    --s3-endpoint "$REGISTRY_S3_ENDPOINT" \
    --s3-bucket "$REGISTRY_S3_BUCKET" \
    --s3-region "$REGISTRY_S3_REGION" \
    --s3-access-key-id "$REGISTRY_S3_ACCESS_KEY_ID" \
    --s3-secret-access-key "$REGISTRY_S3_SECRET_ACCESS_KEY" \
    --path-prefix "$REGISTRY_PATH_PREFIX" --max-descriptors 100000

  streams-cell-admin --cell-id "$CELL_ID" --apply \
    --confirm-serving-quiesced \
    --s3-endpoint "$REGISTRY_S3_ENDPOINT" \
    --s3-bucket "$REGISTRY_S3_BUCKET" \
    --s3-region "$REGISTRY_S3_REGION" \
    --s3-access-key-id "$REGISTRY_S3_ACCESS_KEY_ID" \
    --s3-secret-access-key "$REGISTRY_S3_SECRET_ACCESS_KEY" \
    --path-prefix "$REGISTRY_PATH_PREFIX" --max-descriptors 100000
  ```

  Add `--s3-allow-http` only in a local drill. The command refuses legacy
  `__legacy__` owners and any directory with more than the target cell. Do not
  add a second cell until the post-audit reports zero pending placements and
  indices and the first cell has produced/dark-restored a recovery point.
- **Decommission**: stop generators, redeploy without `KEEP_AWAKE`, let the
  platform sleep the fleet; delete the prefix when the data is disposable.

## 12. Security notes for operators

Bearer token gates the API; the stream key gates the data (two independent
factors — a leaked token cannot decrypt). Keys never persist server-side;
backups are ciphertext. Metrics stream is encrypted under `METRICS_KEY`.
Full identity/custody/audit design: [OPERATIONS.md §3](./OPERATIONS.md).
Never commit tokens, keys, or presigned URLs; the deploy scripts keep them
in a local scratch directory outside the repo.

For a release inspection, stop writers (or select an immutable recovered
prefix), create a mode-0600 JSON file whose entries are base64-encoded forbidden
payload/key byte patterns, and run `streams-at-rest-check` separately against
every primary role prefix and the recovery prefix. The checker refuses empty,
over-bound, ETag-less, or concurrently changing corpora and prints no forbidden
bytes. Preserve its aggregate JSON with the release evidence, then securely
remove the specification. `scripts/ci-at-rest-inspection.sh` is the hermetic
example and includes a deliberate-leak negative control.

Never flip `HISTORY_BLOCK_WRITE_FORMAT` on an existing cell ad hoc. Deploy the
dual reader everywhere with writer 1, prove mixed-route reads and dark restore,
then canary writer 2 as specified in [STORAGE-MIGRATIONS.md](./STORAGE-MIGRATIONS.md).
