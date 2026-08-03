# Prisma Streams

A multi-tenant streaming service in Rust, built on
[SlateDB](https://slatedb.io) — an object-store-native LSM — so that **the
object store is the only stateful tier**. Appends are acknowledged only after
their bytes are durable in object storage; servers are stateless and hold only
caches and in-flight buffers; every stream is encrypted with a per-stream key
the service never persists.

The service exposes **two HTTP surfaces**:

- **`/v1/streams/{name}` — the Prisma product API** (the primary product).
  Typed collections with routing-key records, producer sessions, consumer
  groups, watches, forks, and a seal lifecycle. Uses `Prisma-*` headers and
  product cursors. This is what the [`@prisma/streams` SDK](./sdk/) speaks and
  what applications should build against.
- **`/v1/stream/{name}` — the pinned Durable Streams standards surface.**
  The append-only default-key sequence of the [Durable Streams
  protocol](./CONFORMANCE.md), preserved byte-for-byte and verified against
  the upstream conformance suite. It is the raw, standards-compliant view of a
  collection's default routing key — use it for protocol interoperability, not
  as the product API.

> **Pre-launch posture.** This is a clean, single-format cutover: there are
> no legacy decoders, translators, aliases, or dual layouts. Removed
> experimental names (`Stream-Encryption-Key`, `Stream-Key`, stream
> *profiles*) are rejected on the wire, never translated. Descriptors are
> written at one `LAYOUT_VERSION`. Historical designs live in
> [docs/history/](./docs/history/).

## What it does

- **Two coherent surfaces, one engine.** The product route's default routing
  key *is* the raw route's sequence — the same records, the same durability,
  seen two ways.
- **Committed-before-ack durability.** Concurrent appends bundle into shared
  WAL PUTs; the ack races nothing — a 2xx means the bytes are in object
  storage. Every response whose truth depends on state (duplicates,
  idempotent closes, producer/sequence conflicts, seal fences) waits behind
  that same durability barrier.
- **Tenant isolation by cryptography.** Each collection's data is AES-GCM
  encrypted under a caller-supplied key (`Prisma-Encryption-Key`) the service
  never stores. Backups are ciphertext.
- **Product lifecycle.** Typed creation documents, producer sessions with
  exactly-once semantics, consumer groups (pull/settle, leases, per-key FIFO,
  DLQ), watches with signed observation URLs, forks with resumable lineage,
  and a generation-fenced seal state machine (Open → Sealing → Sealed).
- **Coordination-free horizontal scale.** Shard placement derives from fleet
  heartbeats via rendezvous hashing; correctness never depends on routing
  being right — object-store CAS fencing makes a stale owner's writes fail,
  not corrupt. Hot routing keys split into real physical child segments.

## Measured (pilot, Singapore, 1-CPU/1-GB instances on Tigris)

| metric | value |
|---|---|
| fleet of 4 sustained | ~1,250 req/s avg, peaks 2,700+ req/s |
| durable-ack p50 under load | 50–65 ms (25–50 ms WAL flush + Tigris PUT) |
| single instance, direct path | ~1,180 req/s max observed |
| 2 h soak | flat p50 at saturation, zero deaths |
| chaos (kill N−2 under load) | survivors absorb, zero data loss |

Full history: [docs/BENCHMARKS.md](./docs/BENCHMARKS.md),
[EXPERIMENT-PILOT.md](./EXPERIMENT-PILOT.md), [REPORT.md](./REPORT.md).

## Quick start — the SDK (recommended)

The [`@prisma/streams` SDK](./sdk/) is the canonical getting-started path;
its [README](./sdk/README.md) is the tutorial. In brief:

```ts
import { StreamsClient } from "@prisma/streams";

const client = new StreamsClient({ url, token });
const orders = await client.createStream("orders", {
  encryptionKey,                       // 32 bytes, base64url — never stored
  format: { kind: "json" },
  watches: [{ name: "by-customer", fields: ["/customerId"] }],
});
await orders.append({ customerId: "c1", total: 42 }, { routingKey: "c1" });
for await (const record of orders.subscribe()) { /* ... */ }
```

## Quick start — HTTP (local, no cloud)

```bash
cargo build --release
./target/release/s3lite --listen 127.0.0.1:9500 --latency-ms 5 &
./target/release/streams-slate \
  --listen 127.0.0.1:8090 \
  --s3-endpoint http://127.0.0.1:9500 --bucket streams --region auto \
  --access-key-id test --secret-access-key test \
  --initial-shards 4 --auth-token devtoken &
KEY=$(./target/release/streams-keys generate)   # 32 bytes, base64url

# Product route: create a typed collection, append under a routing key, read.
curl -X PUT  http://127.0.0.1:8090/v1/streams/orders \
  -H "authorization: Bearer devtoken" -H "Prisma-Encryption-Key: $KEY" \
  -H 'content-type: application/json' -d '{"format":{"kind":"json"}}'
curl -X POST http://127.0.0.1:8090/v1/streams/orders/records \
  -H "authorization: Bearer devtoken" -H "Prisma-Encryption-Key: $KEY" \
  -H 'Prisma-Routing-Key: c1' -H 'content-type: application/json' -d '{"n":1}'
curl "http://127.0.0.1:8090/v1/streams/orders/records?routingKey=c1" \
  -H "authorization: Bearer devtoken" -H "Prisma-Encryption-Key: $KEY"

# The same record, seen through the pinned Durable Streams standards surface
# (the default routing key):
curl http://127.0.0.1:8090/v1/stream/orders \
  -H "authorization: Bearer devtoken" -H "Prisma-Encryption-Key: $KEY"
```

## Client support

The SDK is dependency-free and derives watch keys via WebCrypto.

- **Node 18 and 22** — gated in CI (built, packed, installed from the tarball,
  smoke-tested end to end).
- **Bun and Deno** — gated in CI.
- **Browsers** — expected to work (WebCrypto + `fetch` only), **not yet
  verified**; a browser integration gate is required before browser support
  is claimed.

## Operating it

**[RUNBOOK.md](./RUNBOOK.md)** is the operator manual — building (including
the mandatory x86_64-musl cross-compile for Prisma Compute), the configuration
reference, fleet mode and autoscaling, admission control, debug endpoints,
deployment, monitoring baselines, and a symptom→cause→fix matrix.

[OPERATIONS.md](./OPERATIONS.md) covers the durability/security posture:
object-store requirements, backup/PITR, tenant identity and key custody, SLOs.

`scripts/release-provenance.sh` binds a release report to the exact artifact
(server commit, SlateDB pin, SDK tarball SHA, layout version, conformance
pin, DST scenario count).

## Documentation map

| document | what it answers |
|---|---|
| [sdk/README.md](./sdk/README.md) | **getting started** with the product API |
| [docs/RELEASE-PRODUCT-SURFACE.md](./docs/RELEASE-PRODUCT-SURFACE.md) | the product surface: gates, audit rounds, contracts |
| [SPEC.md](./SPEC.md) | architecture, decision log, guarantees |
| [RUNBOOK.md](./RUNBOOK.md) | build, run, deploy, scale, monitor, debug |
| [CONFORMANCE.md](./CONFORMANCE.md) | the pinned Durable Streams suite; how to run it |
| [docs/ROUTING-V3.md](./docs/ROUTING-V3.md) | routing keys, physical scaling, postings, cost |
| [OPERATIONS.md](./OPERATIONS.md) | provider requirements, backup/PITR, identity, SLOs |
| [SECURITY.md](./SECURITY.md) | key custody, watch capabilities, tenant isolation |
| [docs/dst/](./docs/dst/) | the deterministic-simulation program + scenario catalogue |
| [docs/history/](./docs/history/) | removed designs (profiles, …) — provenance only |
| [repro-edge-404/](./repro-edge-404/) | the Compute edge-publication platform ticket |

## Repository layout

```
src/            server crate (streams-slate) + bins
  http.rs       both HTTP surfaces + admission + debug endpoints
  product.rs    the /v1/streams product surface (lifecycle, consumers, watches)
  shard.rs      shard engine: commit pipeline, group PUTs, watermarks, fences
  registry.rs   the descriptor control plane (incarnations, seal state)
  scaler3.rs    routing-key distribution sketches + physical split/merge
  history.rs    history tier + absorber
  crypto.rs     stream-key envelope (AES-GCM)
  fleet.rs      heartbeats, load vector, desired-count computation
  dst/          deterministic simulation tests
sdk/            @prisma/streams TypeScript client SDK (canonical entry point)
conformance/    the pinned Durable Streams suite runner
scripts/        field gate, release provenance, analysis
docs/           routing/cost/soak campaigns, RELEASE-PRODUCT-SURFACE, dst/, history/
repro-edge-404/ the Compute edge-publication reproduction package
```

## License

Apache-2.0 (see [LICENSE](./LICENSE)).
