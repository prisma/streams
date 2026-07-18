# Prisma Streams on SlateDB — Architecture & Specification

A multi-tenant Durable Streams service built on [SlateDB](https://slatedb.io)
(object-store-native LSM), running on Prisma Compute, storing everything in
Tigris. This document is the spec of record: decisions, architecture,
guarantees and how we uphold them, scalability properties, customer-facing
notes, limitations, and open questions.

Companion documents:
- [DESIGN.md](./DESIGN.md) — the original single-database rewrite design and
  ingest mechanics (superseded where this document says otherwise)
- [BENCHMARKS.md](./BENCHMARKS.md) — measured results vs the existing Bun/TS
  implementation under 25 ms emulated object-store latency
- [COMPUTE-SPEC.md](./COMPUTE-SPEC.md) — routing, autoscaling, and lifecycle
  spec for Prisma Compute (the storage-tiering section, §7, is **deferred**)

---

## 1. What this is

An implementation of the Durable Streams HTTP protocol (append-only streams,
opaque 26-char Crockford base32 offsets, byte/JSON appends, long-poll/SSE
tails, routing-key reads) re-architected so that:

- **an append is acknowledged only after its bytes are durable in object
  storage** — many concurrent requests are bundled into shared PUTs to make
  this fast;
- **servers are stateless** — all durable state lives in object storage;
  local disk and memory are caches (≤ 500 MB) and in-flight buffers;
- **streams are the tenant isolation boundary** — each stream's data is
  encrypted with a stream-specific key that is attached to requests and never
  persisted by the service;
- **the fleet scales horizontally with no coordination service** — placement
  is derived from heartbeats; safety comes from object-store CAS fencing, not
  from routing correctness.

## 2. Decision log

| # | decision | rationale |
|---|----------|-----------|
| D1 | Rust + SlateDB 0.14 (`slatedb` crate) | SlateDB natively implements the machinery the TS version hand-built (WAL on object storage, group commit, compaction, manifest CAS, block caching, fencing); Rust is its first-class binding |
| D2 | ACK point = object-store durability | product requirement; implemented via SlateDB's durable-seq watermark (writes applied with `await_durable=false`, acked when `DbStatus.durable_seq` passes their seqnum) |
| D3 | **Dynamic shard topology** (extendible-hashing trie): shards are bit-prefixes of the stream hash; the service starts at **one shard** and splits a hot shard into its two children (doubling *that* shard) via SlateDB clone-with-projection; merges via manifest-union | separates the scaling unit from the isolation unit (stream); restores cross-stream WAL bundling; the PUT-cost floor and operational footprint scale *with actual usage* instead of a provisioned shard count; each split is one metadata-only operation on one shard, never a global resharding |
| D4 | One **shard log** SlateDB per shard: ingest, durable+speculative tails, transient tail storage | group commit across all streams on the shard → PUT rate ∝ active shards (bounded), not active streams |
| D5 | **Two-tier storage**: absorber drains shard log into per-stream **WAL-less SlateDBs** (history tier) | this is the no-dictionary path to ~90% compression (block zstd over plaintext inside the per-stream DB), gives exact per-stream physical size (manifest), and real byte deletion (prefix delete) |
| D6 | **Three shared buckets** (ops, shard-logs, data; streams are prefixes), not per-stream buckets | avoids bucket-quota and provisioning friction at billions of streams; isolation moves into cryptography (D7); per-stream physical accounting comes from the per-stream DB manifest. Tigris has a global namespace with no per-prefix throughput limits, so bucket *pools* are unnecessary; splitting into pools remains a later option if provider limits ever appear |
| D7 | Per-stream encryption keys attached to requests, never persisted; **payloads encrypted under per-routing-key subkeys** with deterministic nonces (see §3.7) | shard log stores per-record ciphertext; history tier uses SlateDB's `BlockTransformer` (encryption after compression) with the stream key; tenant deletion = crypto-erasure + async prefix delete |
| D8 | No zstd dictionaries | complexity rejected; the history tier's block compression makes them unnecessary |
| D9 | Tigris for **everything** (WAL + SSTs); no S3 Express | benchmarked ~14–18 ms small-object ops → durable tail latency ~25–45 ms; single provider, zero egress fees on Standard |
| D10 | Archive Instant Retrieval tiering **deferred** | designed (COMPUTE-SPEC §7) but not built now |
| D11 | Optional **speculative tails** (`DurabilityLevel::Memory`) with transient `persisted`/`aborted` events | opt-in low-latency mode for UIs; checkpoints (`Stream-Next-Offset`) only ever advance with the durable watermark |
| D12 | **CDN is the read fan-out tier**: canonical-chunked immutable catch-up reads + collapsed long-polls for durable tails | protocol is built for it (immutable offset-addressed slices, `ETag` + `Cache-Control: immutable`); 50k tails cost the origin ~#POPs requests |
| D13 | Coordination via ops bucket only: heartbeat objects, **derived** weighted-rendezvous ring, CAS'd `overrides.json` and `desired.json` | no controller, no consensus service; every mechanism is allowed to be stale because fencing (D14) is the arbiter |
| D14 | SlateDB **manifest CAS fencing** is the sole ownership arbiter | shard moves need no coordination: the new owner opens the shard log and the old owner is fenced mid-flight; routing errors cost latency, never data |
| D15 | **Inverted autoscaling**: instances jointly compute `desired.json`; the platform converges the fleet to it | the platform never interprets Streams metrics; deterministic formula over shared heartbeats makes CAS races benign |
| D16 | **Replay-header routing** (`409` + `Streams-Replay-To`) | router needs no authoritative map — guess (ring), correction (replay), safety net (fencing) |
| D17 | Separate **compactor service** for shard logs | shard-log values are record-encrypted, so compaction there needs no tenant keys; keeps CPU off 1-core gateways. History-tier compaction is key-gated and runs on shard owners piggybacked on absorption |
| D18 | Stream registry = CAS'd JSON objects in ops bucket (not a control stream) | idempotent provisioning, no bootstrap circularity, any server reads+caches descriptors directly |
| D19 | **The CDN caches ciphertext only**; decryption happens client-side in an SDK using HKDF per-routing-key subkeys | tenant payloads are never at rest decrypted in edge caches; a routing-key subkey grants access to exactly that key's records (e.g. one chat), enabling end-user-granular access |
| D20 | Key lifecycle and authn/authz are **delegated to an external service**; this repo ships a baseline `streams-keys` CLI (generate stream keys, derive routing subkeys, encrypt/decrypt records) used for development and benchmarking | keeps the data plane key-stateless; the CLI pins the envelope format the future service must implement |
| D21 | **Globally unique stream names**; registry sharded by name hash (`registry/by-name/…`) with per-customer listing markers (`registry/by-customer/…`); target scale **billions of streams total, up to 1M per customer** | one lookup path, no tenancy in the stream identity; idle streams cost only their registry object + settled SSTs |
| D22 | `flush_interval ≥ max(25 ms, backend PUT p90)` (amended twice) | 5 ms minted WAL SSTs ~7× faster than SlateDB's WAL GC reaps them; the backlog degraded per-DB durable-watermark latency to 0.3–1 s (EXPERIMENT-PILOT run 3). 25 ms holds the ack floor at ≈ flush + PUT ≈ 40–60 ms and cuts WAL churn 5×. Bench round 2 adds the RTT rider: the WAL flusher PUTs serially, so a flush interval below the backend's PUT latency mints SSTs faster than one pipe can ship them (Tigris p50 ~45 ms → 25 ms flush = durable-wait p90 518 ms; 50 ms flush = p90 82 ms). Local/fast backends keep 25 ms |
| D23 | **No open handle runs default background loops** (response to V4): shard logs poll their manifest at 30–60 s with no embedded compactor/GC (fencing correctness comes from CAS write failures, not polls); history-tier DBs are only ever open in one of three modes — absorber-open (`compactor_options: None`, `garbage_collector_options: None`, no polling), checkpoint-pinned read-open (no loops), or maintenance piggybacked on absorber opens — and absorption fires on bytes-or-age thresholds (~4 MB / ~5 min) so per-open costs amortize | turns idle cost from per-open-database (8.26 ops/s measured at defaults ≈ $10/mo each) into **per-shard baseline (~0.1 ops/s) + per-activity increments**; closed DBs cost zero; idle streams cost zero requests |

## 3. Architecture

> **Cell scoping (COMPUTE-SPEC §10):** every path below roots under the
> owning cell — `cells/<cell-id>/shards/…`, `cells/<cell-id>/streams/…` —
> so per-cell IAM prefixes (§10.1a) and per-cell shard tries compose
> without collision; the registry descriptor pins the stream's cell. The
> single-cell pilot uses the degenerate `pilot/<ns>/` root via the same
> prefix mechanism.

### 3.1 Storage layout

```
ops bucket          control plane: stream registry (registry/by-name/<name>.json,
                    registry/by-customer/<cust>/<name>), topology.json (shard trie),
                    fleet heartbeats, desired.json, overrides.json, audit stream
shard-log bucket    shard logs: shards/<bit-prefix>/ (SlateDB: WAL + SSTs)
data bucket         history tier: streams/<hash>/ (per-stream WAL-less SlateDB:
                    manifest + SSTs)
```

Single bucket per role; Tigris's global namespace has no per-prefix limits,
so pooling is deferred until a provider limit demands it (D6).

### 3.2 Shard topology (dynamic)

A shard is a **bit-prefix of the stream hash**; the live topology is a
complete prefix code (an extendible-hashing trie) stored in a CAS'd
`topology.json`. Service birth: one shard with the empty prefix. Routing:
stream → `hash16` → longest matching prefix in the topology → owner via the
rendezvous ring.

**Split** (trigger: sustained > 60% of a shard's write ceiling): the shard's
owner CASes a split intent, briefly holds that shard's appends in the ingest
queue (bounded by the append timeout), flushes, then clones the shard log
into two children with hash-range projections (`p0…`, `p1…`) — metadata-only,
no data copy — CASes the new topology, and resumes. Children are assigned by
the ring (usually one stays local, one hands off via normal fencing). The
parent database is retired; its SSTs remain referenced by the children until
their compaction ages them out (verification item V8). **Merge** is the
reverse via manifest-union when two sibling shards are cold.

The keyspaces below are **hash-first** precisely so that a hash range is one
contiguous key range — a split is a single `projection_range` per child.

### 3.3 Keyspaces

Shard log (one SlateDB per shard; payload bytes are ciphertext):
```
<hash16> m                      stream meta (registry cache / tombstone)
<hash16> t                      tail: next_offset, last_ts, last Stream-Seq,
                                cumulative logical bytes, absorbed_through
<hash16> r <offset u64 BE>      record: plaintext header + encrypted payload (§3.7)
```

Per-stream history DB (block-transformer encrypted with the stream key,
block-zstd compressed; WAL disabled — the shard log *is* its WAL):
```
r!<offset u64 BE>               record (plaintext inside encrypted blocks)
k!<routing-key>!<offset BE>     routing-key index → record copy (fast key streaming)
```

`hash16` = first 16 bytes of SHA-256 of the stream name. Offsets are a
per-stream u64 sequence; the wire encoding is the canonical 26-char Crockford
base32 of (epoch u32, rawSeq=seq+1 u64, in_block u32), `-1` = start.

### 3.4 The append path

```
client append (+ stream key)
  → owner instance (router: stream → shard → instance)
  → validate, compress-then-encrypt payloads (per-routing-key subkey, §3.7)
  → bounded ingest queue (full ⇒ 429)
  → committer loop: drain queue → one WriteBatch
      records + tail pointers + auto-create meta, batch-locally staged
    → db.write(await_durable=false)   # ordered memtable/WAL-buffer apply
    → push {seqnum, acks, tail snapshots} in-flight
  → SlateDB WAL flusher (flush_interval = 25 ms, D22) bundles everything
    in the window into ONE WAL SST PUT to Tigris
  → acker loop: durable_seq watermark passes seqnum
    → promote tails to readers' durable view, ACK requests,
      wake long-pollers, emit `persisted` events to speculative tails
```

Group commit composes at three levels: HTTP requests → WriteBatch;
WriteBatches → one WAL object per flush interval; commits pipeline while PUTs
are in flight. Measured (25 ms emulated store): p50 ≈ 36 ms at moderate
concurrency, 17k durable appends/s per instance at c=1024, ~490 records per
PUT. On Tigris (~15 ms PUTs) expect ~25–45 ms durable tail latency.

No rollback paths exist by construction: shared stream state is published
only after a successful memtable apply; the acker only advances on the
watermark; a failed batch write touches nothing; an object-store outage
stalls the watermark until backpressure (429) surfaces.

### 3.5 The read paths

- **Durable tail** (default): served by the shard owner from the shard-log
  memtable; long-pollers/SSE woken by the watermark notify. Zero object-store
  ops on this path. Via CDN: all caught-up clients long-poll the same
  canonical URL; request coalescing collapses them to one origin request per
  stream per POP.
- **Speculative tail** (opt-in, `no-store`, direct to owner): records
  streamed at memtable-apply time carrying their offsets in-band, followed by
  transient `persisted` (watermark passed) or `aborted` (flush failed /
  fenced) control events. `Stream-Next-Offset` never advances past the
  durable watermark; on disconnect the client resumes from the durable
  checkpoint and re-receives anything unconfirmed.
- **Catch-up / replay**: canonical fixed chunk boundaries; responses are
  byte-immutable forever (`ETag` + `Cache-Control: immutable`) → CDN cache
  hits. Origin misses read: shard-log tail (recent) merged with the
  per-stream history DB (absorbed prefix), through the shared ≤ 500 MB block
  cache.
- **Routing-key streaming**: `k!` prefix scan in the history DB (physically
  contiguous, compressed) + in-memory filter over the un-absorbed tail.
  Unlimited key cardinality — it's just keyspace.

### 3.6 The absorber

Per shard, on the owner: drains committed shard-log records into per-stream
history DBs in large batches — decrypt (the key arrived with the requests),
write through the block transformer (zstd blocks → ~90% compression), advance
`absorbed_through`, trim the absorbed shard-log prefix. Runs opportunistically
whenever the stream key is in hand; a stream with no keyed traffic keeps its
ciphertext tail in the shard log (harmless: nobody can read it without the
key either). Absorption is idempotent (keys are offsets), so no cross-database
transaction is needed around the cursor.

Cost discipline (D23): absorption fires per stream on a bytes-or-age
threshold (~4 MB accumulated or ~5 min since last absorption), and the
history DB is opened maintenance-free (no compactor, no GC, no polling),
written in bulk with the F2 pattern (non-durable writes + one explicit
`flush()`), optionally compacted while the key is in hand, and closed. A
history DB that isn't being absorbed or read is closed and costs zero
object-store requests; cold reads use a checkpoint-pinned reader with no
background loops.

### 3.7 Encryption envelope & SDK

Decided (D19/D20): the CDN caches **ciphertext**; clients decrypt in an SDK.
The envelope that makes this coherent across both tiers and the wire:

- **Keys:** `streamKey` (customer-held, request-attached, never persisted) →
  `subkey = HKDF(streamKey, routingKey ‖ keyVersion ‖ streamEpoch)`. A reader
  holding only a subkey can decrypt exactly that routing key's records (a
  chat user sees only their chat); a stream-key holder can derive any subkey.
  Two mandates from the crypto review (VERIFICATION.md V9): a stream
  identity change (delete + recreate) always mints a new `streamKey`, and
  `streamEpoch` (minted per creation) is bound into the derivation so a
  violated rule fails closed; key rotation bumps `keyVersion`.
- **Record wire/storage format:** plaintext header (offset, timestamp,
  routing key, key version) + AEAD ciphertext payload (AES-256-GCM under the
  subkey), with the header bound as AAD (tamper-evident).
- **Deterministic nonces:** nonce = record offset (unique per (stream,
  subkey) by construction). Consequences: re-encrypting the same record
  yields byte-identical ciphertext, so catch-up chunk responses are
  byte-immutable regardless of which tier serves them — the CDN caching
  contract holds; and the shard-log stored form can be served on the wire
  with **zero cryptographic work on the tail path**.
- **Tiers:** the shard log stores the wire form as-is. The history tier
  stores plaintext payloads inside block-zstd-compressed, block-transformer-
  encrypted SSTs (stream key) — that's the ~90% compression — and origin
  reads from history decrypt blocks and deterministically re-encrypt records
  to the identical wire form.
- **Visibility boundary (accepted):** routing keys are *not* confidential to
  infrastructure — they appear in read URLs (CDN cache keys) and record
  headers. Payloads are confidential everywhere. Customers must not put
  secrets in routing keys (C10).
- **`streams-keys` CLI (to build, D20):** generate/rotate stream keys, derive
  subkeys, encrypt/decrypt records offline — the baseline the external key
  service implements, and what the benchmark driver uses.

### 3.8 Fleet, routing, scaling

See [COMPUTE-SPEC.md](./COMPUTE-SPEC.md) for the full spec. Summary:
1 CPU / 1 GB instances, symmetric (gateway + shard owner + absorber);
heartbeat objects every 2 s carrying a load vector; shard→instance assignment
is weighted rendezvous hashing over the live set (derived, not stored);
router corrections via `409` + `Streams-Replay-To`; instances jointly CAS a
`desired.json` instance count that the platform autoscaler converges to; a
separate compactor service handles shard-log compaction/GC (keyless by
design); drains hand off shards one at a time and pace SSE closures; shard
splits/merges follow the dynamic-topology procedure in §3.2 (the ring and
router consume `topology.json` alongside the heartbeat set).

## 4. Guarantees, and how we uphold them

**G1 — Durability at ACK.** A `200` append response means the entry is
durably stored in object storage. *Upheld by:* acks issued only when
SlateDB's `durable_seq` watermark (updated on WAL SST PUT completion) passes
the write's seqnum. There is no local-disk-durable intermediate state.

**G2 — Reads serve only durable data** (except opt-in speculative tails).
*Upheld by:* readers observe the promoted durable tail; scans run with
`DurabilityLevel::Remote`; speculative delivery never moves checkpoints.

**G3 — Per-stream total order with contiguous offsets; no holes, no
duplicates.* *Upheld by:* exactly one writer per shard (fencing), one
committer task per shard (offsets assigned under batch-local staging in
submission order), WAL flushes are prefix-consistent, and offset state is
recovered from the tail pointer written atomically with its records.
Verified: 500 concurrent appends → exactly-once, contiguous.

**G4 — Atomic multi-entry appends.** A JSON-array append lands entirely or
not at all. *Upheld by:* all entries + tail pointer in one `WriteBatch`.

**G5 — Read-your-writes for acking producers.** After a `200`, a read
returns the entry. *Upheld by:* G1 + G2 share the same watermark.

**G6 — Crash/move safety with zero coordination.** A stream continues
operating after its shard moves servers without notice. *Upheld by:* SlateDB
manifest-CAS + WAL fencing: the new owner's open fences the old writer
mid-flight; acked data is in WAL SSTs the new owner replays; unacked
in-flight appends fail into the client retry contract. Recovery is bounded
(`max_wal_flushes_before_l0_flush`) to keep moves ~sub-second per shard.
Verified: full restart against the emulator recovered all data from object
storage alone.

**G7 — Append ambiguity is explicit.** On timeout/`408` or a move, an append
may or may not have landed; clients must check `Stream-Next-Offset` (or use
`Stream-Seq`) before retrying. This is inherited from the protocol and is
the honest contract for any at-least-once boundary.

**G8 — Tenant confidentiality.** Stream data is encrypted with the
stream-specific key everywhere at rest in shared infrastructure. *Upheld
by:* the envelope in §3.7 (per-routing-key subkeys, ciphertext on the wire
and at the CDN, client-side decryption); block-transformer encryption
(post-compression) in the history tier; keys held in memory only while
streams are active; crypto-erasure on deletion plus asynchronous physical
prefix deletion. *Boundaries:* routing keys are visible metadata (C10);
index-block coverage is verification item V1 (L6).

**G9 — Bounded resources, explicit overload.** Ingest queues, connection
counts, cache, and memtables are capped; overload surfaces as `429`/`503`,
never unbounded buffering. *Upheld by:* bounded mpsc + `max_unflushed_bytes`
+ connection caps + shared 500 MB cache budget.

**G10 — Accurate size reporting.** Logical size = cumulative payload-byte
counter in the tail record (exact, crash-safe). Physical size = sum of SST
sizes in the stream's history-DB manifest (one GET) + its transient shard-log
share.

## 5. Scalability properties

- **Writes** scale horizontally with shards/instances, and the shard count
  itself scales with the workload (D3): the service starts at one shard and
  splits under pressure, so the object-store PUT floor tracks actual usage —
  a near-idle deployment does a handful of PUTs per second; a loaded one
  bundles heavily per shard. Measured per instance (1 core class): ~17k
  durable appends/s.
- **A single stream never exceeds one server** (by design): its ceiling is
  one shard's capacity — tens of thousands of appends/s. Hot shards split
  (metadata-only, §3.2) but a *single* stream cannot; see C3.
- **Durable-tail fan-out is CDN-bounded, not server-bounded**: collapsed
  long-polls make 50k subscribers cost ~#POPs origin requests.
- **Catch-up/replay is CDN-bounded**: immutable chunks are cache hits.
- **Provisioned streams are ~free when idle**: an idle stream is registry
  JSON + settled SSTs; no open database, no polling, no memory.
- **Billions of provisioned streams total (up to 1M per customer, D21)** is
  a registry-scale property only: lookups are one keyed GET (cached),
  per-customer listing is a prefix LIST; active-stream count is what
  consumes serving resources.
- **Recovery/moves** are bounded by WAL replay caps: shard move ≈ sub-second,
  instance crash → p99 < 15 s to full reassignment.

## 6. What customers need to be aware of

- **C1 Append latency includes durability**: ~25–45 ms typical (Tigris).
  Producers wanting throughput should use JSON-array batched appends and/or
  concurrency — batching is where this architecture excels.
- **C2 Ambiguous appends**: on `408`/disconnect, check `Stream-Next-Offset`
  or use `Stream-Seq` before retrying (G7).
- **C3 Per-stream write ceiling**: one stream = one server's capacity. Model
  high-fan-in workloads as one stream per product with routing keys inside
  it (unlimited), not as one global firehose stream.
- **C4 Keys are the customer's responsibility**: the service never stores
  stream keys. A lost key means the data is cryptographically unrecoverable
  — that is the deletion guarantee working as intended. Key rotation: see O1.
- **C5 Speculative tails are optimistic**: entries may be followed by
  `aborted`; render accordingly (e.g., pending-state styling) and rely on
  the durable checkpoint for resume.
- **C6 Deletion semantics**: crypto-erasure is immediate on key destruction;
  physical byte removal (prefix delete + compaction filter) is asynchronous.
- **C7 Offsets are opaque**: treat `Stream-Next-Offset` as a cursor; never
  arithmetic on it.
- **C8 Reads of freshly-appended data by routing key** may briefly scan the
  un-absorbed tail (slightly higher cost than settled history); semantics
  are identical.
- **C9 Limits (initial, stated AND enforced — enforcement mechanics in
  COMPUTE-SPEC §12):**

  | limit | value | enforcement |
  |---|---|---|
  | append body | 32 MB | router + engine reject |
  | response chunk | 8 MB | engine |
  | per-stream sustained appends | measured (engine, 2026-07-14 round 2): **12.4k req/s, 55k ev/s, 56 MB/s** peak; **~750 req/s / 48k ev/s / ~49 MB/s pinned-sustained** with absorber+trim active — the earlier ~0.4–1.7 MB/s figure was two config defaults (`l0_max_ssts_per_key`, manifest-poll staleness), not a compaction defect. Stated product limit: 5k req/s / 50k ev/s / 50 MB/s per ordered stream, latency floor 2× flush + PUT (EXPERIMENT-PILOT bench round 2) | admission token bucket + committer fair-share |
  | per-stream live tail connections | 10,000 direct (unbounded via CDN/mux tier — OPERATIONS.md §4) | connection counter |
  | per-customer streams | 1,000,000 (D21) | create-time counter leases |
  | per-customer connections/cell | 100,000 | admission |
  | stream size / retention | unbounded bytes; retention configurable per stream, default none | TTL + archive tiering |
  | request rate 429 contract | Retry-After + scoped error body | COMPUTE-SPEC §12.2 |

  Stream names are UTF-8 and **globally unique** (D21); customer-scoped
  naming is under reconsideration (O13) because global uniqueness leaks a
  cross-tenant existence oracle (create → 409).
- **C10 Routing keys are metadata, not secrets**: they appear in read URLs,
  CDN cache keys, and record headers (§3.7). Payloads are encrypted
  everywhere; routing keys are not. Don't encode confidential data in them.
- **C11 Reading requires the SDK (or the envelope spec)**: read responses
  carry ciphertext records; decryption happens client-side with the stream
  key or a per-routing-key subkey (D19). The `streams-keys` CLI documents
  the envelope for non-SDK consumers.

## 7. Limitations

- **L1 Single-writer per stream** (and per shard): no multi-region active
  writes; a region-affine owner serves each shard (multi-region strategy: O7).
- **L2 Protocol subset (v1)**: generic profile only. Not yet ported from the
  TS implementation: evlog/metrics/otel-traces/state-protocol profiles,
  schema registry + lenses, `filter=` expressions, `since=` time seeks,
  search/aggregation companions, live/touch. Roadmap: O8.
- **L3 Absorption is key-gated**: a stream with appends but no subsequent
  keyed traffic keeps its tail un-absorbed (ciphertext in shard log)
  indefinitely — correct but delays best-ratio compression and physical
  accounting for that tail.
- **L4 History-tier compaction is key-gated** (runs on owners when keys are
  in hand): pathological patterns (huge burst, then never touched) leave
  more small SSTs than ideal until the next keyed access.
- **L5 Shard-log compression is weak by design** (per-record ciphertext):
  acceptable because it's a small rolling window; the history tier carries
  the ~90% ratio. Streams that are never absorbed (L3) never reach it.
- **L6 Index-block exposure (to verify)**: if SlateDB's block transformer
  does not cover SST index/filter blocks, routing keys (which appear in `k!`
  index keys) could be visible in shared storage; mitigation is hashing
  routing keys in index entries. Verification item V1.
- **L7 Tigris dependency**: durability, latency, and CAS semantics all ride
  one provider (V2). The design is provider-portable (anything with
  conditional writes), but no second provider is wired.
- **L8 A fenced-but-alive old owner** may serve stale *reads* after a move
  (writes are impossible; history is immutable so responses are never
  wrong, only behind the head). The staleness window is BOUNDED to ≤ 5 s,
  not the manifest-poll interval: an owner only serves tail reads while it
  can prove recent ownership — any durable write in the last 5 s proves it
  (the CAS would have failed if fenced), and an idle owner revalidates via
  a conditional manifest HEAD before serving a tail older than 5 s. G5's
  cross-client read-your-writes therefore degrades to at most 5 s of
  bounded staleness during a move, and only for readers still pinned to
  the old owner by a stale route (R3 corrects them on the next request).
- **L9 Shard splits pause that shard's appends briefly** (queue-held while
  the owner flushes and clones; target well under the append timeout).
  Clients see added latency, not errors. Split frequency is inherently low
  (each split doubles that shard's headroom).

## 8. Open questions & assumptions to confirm

Resolved and promoted to the decision log: dynamic sharding (D3), ciphertext
at the CDN + SDK decryption (D19), external key/auth service with a baseline
CLI (D20), globally unique names at billions-of-streams scale (D21), 5 ms
flush interval and single buckets per role (D22, D6). Conservative starting
numbers retained: 10k conns/instance (revisit upward), 30 s max long-poll,
min 3 / max 64 instances.

**Decisions still needing confirmation**

- **O3 CDN choice & mechanics**: which CDN fronts this (does the chosen one
  support request coalescing for long-polls?); canonical chunk size
  (assumed ~1 MB / 1k records).
- **O4 Speculative-tail API shape**: SSE event names (`persisted`/`aborted`
  assumed), and whether long-poll mode also gets a speculative variant.
- **O7 Multi-region**: Tigris is global; are instances single-region (assumed)
  with latency-based routing later, or do we want region-pinned shards?
- **O8 Protocol roadmap**: which of L2's features are commitments (evlog
  profile? filters?) and in what order.
- **O10 Billing dimensions**: assumed logical-bytes appended + storage
  (logical or physical?) + egress-ish read bytes; G10 supports all three.
- **O11 Key-service interface**: D20 defers key lifecycle/authn to an
  external service; its API contract (issue, rotate, revoke, wrong-key error
  mapping — AEAD failure → `403` assumed) needs defining once the CLI
  baseline exists. Per-customer stream quotas (≤ 1M, D21) are enforced where
  — registry CAS or the external service?
- **O12 Provider commitments**: contract-level per-prefix request-rate and
  volume-pricing commitments from Tigris (OPERATIONS.md §1 parameterizes
  the SLA and unit-economics math on these).
- **O14 (GA gate) SlateDB performance defects**: (a) the
  fence/reopen-correlated per-DB durable-watermark degradation
  (EXPERIMENT-PILOT runs 3–4; 25 ms flush cut incidence ~16×, now
  self-heals, but 773 ms excursions remain and the 250 ms ack-p99 SLO is
  exposed); (b) ~~L0-compaction throughput~~ **RESOLVED (bench round 2)**
  — the "near-zero compaction progress" was misattributed: the flusher was
  gated by `l0_max_ssts_per_key` (default 8; per-key L0 overlap == L0
  count for an ordered stream) plus a stale manifest view (60 s poll).
  With `L0_MAX_SSTS_PER_KEY` raised and `MANIFEST_POLL_MS=1–2 s`, one
  stream sustains 50+ MB/s locally. Remaining upstream ask: flusher
  should learn compaction results via in-process notification, not
  manifest polling, and write-stall must surface as 429s (§12 backstop),
  not hangs.
- **O15 (GA gate) Speculative-tail API shape (O4)**: event names and the
  long-poll variant of D11 are public API; close before shipping D11.
- **O13 Customer-scoped stream naming — GA GATE, not open-ended**: global
  uniqueness (D21) leaks a cross-tenant existence oracle (create → 409)
  and invites squatting; cheap to fix now (prefix registry paths with the
  customer id), expensive after the registry format ossifies. Decision
  required before GA.


**Technical verification items** — executed against live Tigris
(single-region Singapore) on 2026-07-08; full results and methodology in
[VERIFICATION.md](./VERIFICATION.md).

- **V1** Block-transformer coverage — ✅ **pass** (source-verified): data,
  index, filter, and stats blocks are all transformed; routing keys in `k!`
  index entries are encrypted at rest. L6 resolved.
- **V2** Tigris conditional writes — ✅ **pass** (live): Create/Update CAS
  semantics exact, concurrent create race yields exactly one winner. G6's
  foundation verified. Live fencing test also passed (zombie writer rejected
  in 0 ms, nothing leaked).
- **V3** `wal_disable` — ✅ **pass** with a required pattern (finding F2):
  with WAL off there is no timer-driven memtable flush, so `await_durable`
  hangs below the L0 size threshold; the absorber must use non-durable
  writes + an explicit `flush()` before advancing `absorbed_through`.
- **V4** Idle per-open-DB overhead — ⚠️ **real, resolved by D23**: 8.26
  object-store ops/s per idle DB at defaults (~$10/month each at Tigris
  Class B prices) came from the default manifest-poll/compactor/GC loops.
  D23 removes those loops from every open handle; cost becomes per-shard
  baseline + per-activity. **V4b measured**: with the D23 profile, idle
  open DBs drop to **0.03 ops/s** (275× reduction; ~$1/month for a
  24-shard instance). Remaining to measure: ops-per-open/close cycle for
  transient history-DB opens (projected ~3 ops, amortized by absorption
  thresholds).
- **V5** CDN request coalescing — deferred pending CDN choice (O3).
- **V6** Compression — ✅ **target met, no dictionaries**: 64 KiB block zstd
  reaches 90.5% (evlog) / 92.4% (chat), within half a point of the old
  256 KiB segments; the per-record shard-log form is 15–24% as expected.
- **V7** Clone/union — ✅ **pass** (live): projection split is zero-copy
  (children = 2 objects), contents exactly partitioned, union re-merges
  correctly including post-split writes. ~4.7 s per op cross-border ⇒
  ~1 s expected in-region.
- **V8** Retired-parent GC — ◐ **partial**: checkpoint pinning confirmed;
  the long-horizon path (children compact → parent objects become
  GC-eligible) needs a soak test before unattended splits.
- **V9** Crypto envelope — ⚠️ **sound with two mandates**, folded into §3.7:
  a new streamKey on stream re-creation (with `streamEpoch` bound into HKDF
  so violations fail closed) and a keyVersion bump on rotation. Independent
  review still recommended before GA.
- **V10** Split-under-load pause — deferred to the split implementation;
  first timing input from V7 (in-region clone ≈ 1 s, so the quiesce window
  fits the append timeout in-region only — cross-region splits would need
  an async cutover; finding F3).

**New findings folded back into the design**

- **F1**: WAL replay is sequential object-store GETs; a post-bench restart
  with ~500 accumulated WAL SSTs took 25 s. `max_wal_flushes_before_l0_flush
  = 64` is now set explicitly in the server — it is the crash-recovery and
  shard-move time budget, not a tuning nicety.
- **F2/F3**: see V3/V10 above.

---

## Implementation status (2026-07-08; operator-spec addendum 2026-07-14)

**Spec'd in the operator review, not yet implemented** (design of record in
COMPUTE-SPEC §10–12 and OPERATIONS.md; pilot implements earlier subsets):
cells + cells.json global layer; heartbeat aggregation (fleet.json);
deployment waves/canary automation; per-tenant quotas & admission token
buckets; committer fair-share; poison-shard quarantine; backup/PITR copy
actor + scrubber; scoped authn tokens + key-service contract; audit
streams; tail-mux tier (CDN plan B); L8 bounded-staleness read guard.
The pilot has validated: fencing/lazy ownership, heartbeats + desired.json
CAS autoscaling with rps+latency dimensions, live-set ring + R3 replay,
routed metrics/billing stream, scale-from-0 economics, 25 ms flush.

The data plane described above is implemented and verified end-to-end against
both the s3lite emulator (25 ms) and live Tigris:

- **Built**: crypto envelope (§3.7, [crypto.rs](./src/crypto.rs)) with the two
  V9 mandates; `streams-keys` CLI (D20, [keys.rs](./src/bin/keys.rs));
  ops-bucket registry with key fingerprints + wrong-key `403`
  ([registry.rs](./src/registry.rs)); dynamic hash-first shard topology (D3,
  routing implemented; starts at N configurable shards); per-shard
  committer/acker engines ([shard.rs](./src/shard.rs)); absorber + WAL-less
  block-encrypted history tier with the `k!` routing index (§3.6,
  [history.rs](./src/history.rs)); merged two-tier reads with ciphertext
  frames / server-decrypted `format=json` / key filters / long-poll
  ([http.rs](./src/http.rs)).
- **Verified e2e**: create/append/read/filter, wrong-key rejection,
  absorption + byte-identical deterministic re-encryption through the real
  serving path (an absorbed frame decrypts with the offline CLI), restart
  recovery from object storage alone (~3 s), long-poll wake-ups, and
  18,097 req/s durable appends at c=1024 (p99 62 ms, zero errors) with
  encryption + registry + absorber active.
- **F1 amendment**: `max_wal_flushes_before_l0_flush` has a 4096 validation
  floor upstream, so the recovery window is bounded instead by a per-shard
  periodic explicit memtable→L0 flush (5 s when dirty), measured to bring
  restart recovery from 25 s to ~3 s.
- **state-protocol profile built and stress-tested** (PROFILES.md §6 is the
  contract of record): in-memory bucketed touch journal fed post-durability
  by the acker (H2), xxh3 key derivation per live.md
  ([touch_keys.rs](./src/touch_keys.rs)), **collapsible GET-per-key waits**
  (`/touch/key/{key}?cursor=..&sig=..` — journal-global cursors converge a
  cohort onto identical URLs so a CDN coalesces them; HMAC-signed URL
  capabilities; templates pinned in the descriptor), epoch-mismatch →
  `stale` plus fence-close wake-all. Validated by
  [livebench](./src/bin/livebench.rs) (simulated Postgres logical-decoding
  adapter) through [edgesim](./src/bin/edgesim.rs) (CDN-style coalescing):
  cohort-shaped load (40 waiters/key) → **40.0× origin-load reduction**
  (216,280 → 5,852 origin wait requests) at identical latency (p50 ~53ms,
  p99 ~83ms) and **0 missed invalidations**, including kill-restart chaos.
  Findings: adapters must append only after the DB commit is visible; and
  coalescing — not caching — is the mechanism (long-TTL cached wakes caused
  chain-walking; head wakes now cache 2s, catch-ups jump to head, no-store).
- **Upstream protocol conformance: 239/239** (see CONFORMANCE.md) — the
  baseline run drove the full upstream surface into the implementation:
  producer idempotence headers, closed streams, SSE, `offset=now`,
  ETag/304, cursors, content-type config, strict validation, no
  auto-create, security headers; byte-mode reads now return decrypted
  payload bytes and ciphertext frames moved behind `format=frames`.
- **`ordering: per-key` implemented** (PER-KEY-ORDERING.md): opt-in
  Pravega-style segmented streams — per-routing-key total order, segments
  placed across shards/servers (the per-stream write ceiling falls),
  epoch-bearing offsets, keyed live reads, segment-sequential replay.
  Conformance: 239/239 at one segment (degenerate), 194/239 at four with
  only the two specified deviations.
- **`queue` profile implemented** (PROFILES.md §7; Cloudflare-informed):
  pull consumers with leases/visibility, combined ack+retry+extend settles
  with per-message delays, permissive tokens, backlog signals, lazy expiry,
  `$dlq` routing-key dead-letter view; state durable in the shard log via
  the committer. **TypeScript SDK** ([sdk/durable-streams.ts](./sdk/durable-streams.ts)):
  streams (create/append/read/tail) + CF-style queue consumers
  (`receive`/`msg.ack()`/`msg.retry({delayMs})`/`batch.settle()`/`consume`
  loop) — verified by [sdk/example.ts](./sdk/example.ts) end-to-end
  (mixed ack/retry, delayed redelivery, poison→DLQ at maxDeliveries).
- **Not yet built** (fleet phase): split/merge execution (topology *routing*
  is in), speculative tails (D11), CDN canonical chunking, detached compactor
  service, heartbeats/ring/autoscaling (COMPUTE-SPEC), history-DB physical
  size in `_details`, `since=`.

## Appendix: measured baseline

From [BENCHMARKS.md](./BENCHMARKS.md) (25 ms emulated object-store latency,
256 B records): appends at concurrency 1/64/256/1024 → 31/1,581/4,189/17,091
req/s at p50 30/36/61/54 ms, p99 ≤ 68 ms, zero errors; ~490 records per PUT
at c=1024; batched JSON appends 41k entries/s; ack→object-store-durability
gap 0 ms by construction (vs ~800 ms for the tuned TS implementation, whose
ack is local-SQLite only and which plateaued at ~187 req/s); warm reads
240–355 MB/s, cold replay 77 MB/s; full recovery from object storage alone
~3.6 s.
