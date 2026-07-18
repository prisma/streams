# Prisma Streams

A multi-tenant **Durable Streams** service in Rust, built on
[SlateDB](https://slatedb.io) — an object-store-native LSM — so that **the
object store is the only stateful tier**. Appends are acknowledged only after
their bytes are durable in object storage; servers are stateless and hold only
caches and in-flight buffers; every stream is encrypted with a per-stream key
the service never persists.

This is the second-generation implementation. It replaces the original
Bun/TypeScript + SQLite version (available in git history before the `slate`
branch landed) with an architecture designed for horizontal scale on
[Prisma Compute](https://prisma.io) with S3-compatible storage (Tigris in the
pilot).

## What it does

- **Durable Streams HTTP protocol**: append-only streams, opaque 26-char
  Crockford base32 offsets, byte/JSON appends, long-poll and SSE tails,
  routing-key reads, and profile semantics (`queue`, `state`) on top
  ([PROFILES.md](./PROFILES.md), [PER-KEY-ORDERING.md](./PER-KEY-ORDERING.md)).
- **Committed-before-ack durability**: concurrent appends are bundled into
  shared WAL PUTs; the ack races nothing — if you got a 2xx, the bytes are in
  object storage.
- **Tenant isolation and key custody**: customer identity is part of registry,
  storage, routing, metrics, and admission identity. Production requests use
  locally verified scoped JWTs with a background-refreshed revocation list.
  Each stream's data is AES-GCM
  encrypted under a stream-specific key attached to requests
  (`Stream-Encryption-Key` header) and never stored by the service. Backups
  are ciphertext; the provider never sees plaintext.
- **Coordination-free horizontal scale**: shard placement is derived from
  fleet heartbeats via rendezvous hashing; correctness never depends on
  routing being right — object-store CAS fencing makes a stale owner's writes
  fail, not corrupt.
- **Self-scaling fleet**: instances publish a load vector (CPU, in-flight,
  ack latency, memory, router-observed client latency) and compute their own
  desired fleet size; scale-to-zero friendly
  ([AUTOSCALING-DESIGN.md](./AUTOSCALING-DESIGN.md) generalizes the model).

## Measured (pilot, Singapore, 1-CPU/1-GB instances on Tigris)

| metric | value |
|---|---|
| fleet of 4 sustained | ~1,250 req/s avg, peaks 2,700+ req/s |
| durable-ack p50 under load | 50–65 ms (25–50 ms WAL flush + Tigris PUT) |
| single instance, direct path | ~1,180 req/s max observed |
| 2 h soak | flat p50 402–410 ms client latency at saturation, zero deaths |
| chaos (kill N−2 under load) | survivors absorb, zero data loss |

Full history: [BENCHMARKS.md](./BENCHMARKS.md) (vs the previous
implementation), [EXPERIMENT-PILOT.md](./EXPERIMENT-PILOT.md) (14 fleet runs,
every failure and fix), [REPORT.md](./REPORT.md) (executive summary).

## Quick start (local, no cloud)

```bash
cargo build --release

# 1. Local S3 emulator with realistic latency
./target/release/s3lite --listen 127.0.0.1:9500 --latency-ms 5 &

# 2. The server
./target/release/streams-slate \
  --listen 127.0.0.1:8090 \
  --s3-endpoint http://127.0.0.1:9500 --bucket streams --region auto \
  --access-key-id test --secret-access-key test \
  --path-prefix dev --initial-shards 4 --auth-token devtoken &

# 3. A stream key (32 bytes, base64 — the service never stores it)
KEY=$(./target/release/streams-keys generate)

# 4. Create, append, read (content-type on create selects the JSON profile)
curl -X PUT  http://127.0.0.1:8090/v1/stream/hello \
  -H "authorization: Bearer devtoken" -H "Stream-Encryption-Key: $KEY" \
  -H 'content-type: application/json' -d '{}'
curl -X POST http://127.0.0.1:8090/v1/stream/hello \
  -H "authorization: Bearer devtoken" -H "Stream-Encryption-Key: $KEY" \
  -H 'content-type: application/json' -d '{"events":[{"data":{"n":1}}]}'
curl http://127.0.0.1:8090/v1/stream/hello \
  -H "authorization: Bearer devtoken" -H "Stream-Encryption-Key: $KEY"
```

A TypeScript client SDK lives in [sdk/](./sdk/).

## Operating it

**[RUNBOOK.md](./RUNBOOK.md)** is the operator manual — building (including
the mandatory x86_64-musl cross-compile for Prisma Compute), the complete
configuration reference, fleet mode and autoscaling, admission control, the
debug endpoints, deployment procedure with every platform trap we hit,
monitoring baselines, capacity numbers, and a symptom→cause→fix
troubleshooting matrix.

[OPERATIONS.md](./OPERATIONS.md) covers the durability/security posture:
what we require of the object-store provider, backup/PITR, tenant identity
and key custody, SLOs.

## Documentation map

| document | what it answers |
|---|---|
| [SPEC.md](./SPEC.md) | the spec of record: architecture, decision log, guarantees |
| [RUNBOOK.md](./RUNBOOK.md) | how to build, run, deploy, scale, monitor, debug |
| [COMPUTE-SPEC.md](./COMPUTE-SPEC.md) | routing, fleet lifecycle, load vector, cells |
| [DESIGN.md](./DESIGN.md) | original single-DB rewrite design + ingest mechanics |
| [OPERATIONS.md](./OPERATIONS.md) | provider requirements, backup/PITR, identity, SLOs |
| [PROFILES.md](./PROFILES.md) | stream profiles (queue, state) semantics |
| [PER-KEY-ORDERING.md](./PER-KEY-ORDERING.md) | ordering model and guarantees |
| [CONFORMANCE.md](./CONFORMANCE.md) | protocol conformance: how to run the suite |
| [VERIFICATION.md](./VERIFICATION.md) | verification items and their status |
| [BENCHMARKS.md](./BENCHMARKS.md) | measured results vs the previous implementation |
| [EXPERIMENT-PILOT.md](./EXPERIMENT-PILOT.md) | the production-pilot lab notebook (runs 1–14) |
| [AUTOSCALING-DESIGN.md](./AUTOSCALING-DESIGN.md) | scaling-groups proposal for Prisma Compute |
| [PLATFORM-EDGE-REPORT.md](./PLATFORM-EDGE-REPORT.md) | the platform edge investigation (with the Compute team) |
| [REPORT.md](./REPORT.md) | executive summary + addenda |
| [repro-no-restart/](./repro-no-restart/) | platform crash-loop repro package |

## Repository layout

```
src/            server crate (streams-slate) + bins
  main.rs       config, store construction, runtime, startup
  shard.rs      shard engine: commit pipeline, group PUTs, watermarks
  history.rs    history tier + absorber
  crypto.rs     stream-key envelope (AES-GCM)
  fleet.rs      heartbeats, load vector, desired-count computation
  http.rs       HTTP surface + admission control + debug endpoints
  store_timing.rs  per-op object-store latency + runtime sentinels
  bin/          pilot (LB/generator/bench), s3lite, streams-keys, bench, …
sdk/            TypeScript client SDK
bench/          benchmark matrix scripts
charts/         chart generators + pilot result data
repro-no-restart/  minimal reproduction for the platform crash-loop issue
```

## License

Apache-2.0 (see [LICENSE](./LICENSE)).
