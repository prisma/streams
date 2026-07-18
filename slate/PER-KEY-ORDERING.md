# Per-key ordering (`Stream-Ordering: per-key`) — specification

Pravega-style elastic streams for the SlateDB architecture: a stream whose
ordering contract is **total order per routing key** instead of total order
across the whole stream, allowing one stream to scale across segments —
and therefore across shards and servers — while preserving exactly the
ordering that keyed workloads (chat-per-key, trace-per-traceId, queue
message groups) rely on.

**Opt-in is absolute.** A stream without `Stream-Ordering: per-key` at
creation behaves exactly as today: one totally ordered sequence, one
segment, byte-for-byte identical API behavior. Nothing in this document
applies to it. The property is immutable for the life of the stream.

## 1. Model

- A per-key stream is composed of **segments**. Each segment is internally a
  full sub-stream (its own hash identity, tail, offsets, shard placement,
  history DB) — segments of one stream are placed by the existing shard
  topology and therefore land on different servers.
- The routing-key space is partitioned by **bit-prefixes of
  SHA-256(routingKey)** (the same extendible-hashing pattern as the shard
  topology, D3). At any moment, exactly one **active** segment owns any
  given routing key. The absent routing key (`""`) hashes like any other
  value: unkeyed appends all land in one segment and remain totally ordered
  among themselves.
- Each segment has an **ordinal** (u32, unique per stream, assigned
  sequentially at creation/split, never reused). Ordinals strictly increase
  along any split chain, which is what makes per-key cursors comparable.
- The segment map lives in the stream descriptor (registry, CAS-updated):
  `[{ordinal, prefix, state: active|sealed}]`.

## 2. Creation

```
PUT /v1/stream/{name}
  Stream-Encryption-Key: ...
  Stream-Ordering: per-key          # opt-in; anything else is an error
  Stream-Segments: 4                # optional initial count; power of two,
                                    # 1..=256; default 2
```

`201`/`200` as today. Constraints: `state-protocol` + `per-key` is rejected
in v1 (`400 unsupported_combination`; per-segment touch journals are
specified in §7 and land with splits).

## 3. Offsets

The canonical 26-char Crockford encoding already reserves a 128-bit tuple
`(epoch u32, seq u64, in_block u32)`. Per-key streams use **epoch = segment
ordinal**. Total-order streams continue to emit `epoch = 0` — encoding,
length, and opacity are unchanged, so clients that treat offsets as opaque
cursors (the contract) are unaffected.

- A keyed cursor `(e, s)` means: position `s` within segment ordinal `e` of
  that key's chain. Since ordinals increase along the chain, `(e, s)` pairs
  order correctly per key.
- `-1` remains start-of-stream / start-of-chain.
- Servers reject offsets whose epoch names a segment that doesn't exist.

## 4. API surface on per-key streams

Responses on per-key streams carry `Stream-Ordering: per-key` so generic
clients can detect the contract.

**Append (`POST`)** — unchanged shape. The routing key (`Stream-Key`, or
`""`) selects the active segment; the ACK's `Stream-Next-Offset` carries
that segment's ordinal in its epoch. `Stream-Seq` is enforced **per
segment** (i.e. per routing-key partition), not across the stream.

**Keyed reads (`?key=` / `/pk/{key}`)** — semantics identical to today from
the client's perspective: strictly-ordered records for that key, resumable
cursor, long-poll supported. Internally the cursor's epoch selects the
segment in the key's chain; when a sealed segment is drained the returned
`Stream-Next-Offset` moves to the successor's ordinal (`seq = -1`), and the
read continues there — predecessors before successors, which is what
preserves per-key order across splits.

**Unkeyed reads (`GET ?offset=`)** — replay is **segment-sequential**: the
response drains segment `e` from `seq`, and when a segment is exhausted the
cursor advances to ordinal `e+1`. Every record is delivered exactly once;
order is per-segment (therefore per-key), with no interleaving guarantee
across keys. This keeps replay, backfill, and analytics working with an
unchanged read loop.

**Accommodations (explicit deviations, only when enabled):**

1. **Unkeyed live reads (`live=true|long-poll` without `key=`) are
   rejected** with `400 unsupported_on_per_key`: "tail of the whole stream"
   has no single durable cursor once writes are concurrent across segments.
   Tail a key, or tail segments individually (`segment={ordinal}` +
   `live=long-poll` is valid).
2. **`HEAD`** returns the tail of the **highest-ordinal active segment**
   plus `Stream-Segment-Count: N`; there is no single end-of-stream offset.
   Consumers needing global progress read per-segment tails (`segment=`).
3. **`Stream-Seq`** is per-routing-key-partition (documented above).
4. A read may return an empty body with an advanced `Stream-Next-Offset`
   (segment-boundary hop). Clients following the returned cursor — the
   protocol contract — are unaffected.

## 5. Split and merge (seal-before-successor)

Specified now; implementation lands with the fleet phase. v1 ships static
segment counts.

- **Split** (trigger: sustained per-segment append rate over threshold,
  hysteresis; same policy shape as shard splits): the segment's owner
  writes a **seal marker** through its committer (durable at the
  watermark), then CASes the descriptor: predecessor → `sealed`, two
  successors with new ordinals and the halved prefixes. A writer hitting a
  sealed segment receives `409 segment_sealed` + re-resolves the map — the
  same fence-then-continue pattern as shard moves. Per-key order holds
  because a key's successor cannot accept its first write until the
  predecessor's seal is durable (Pravega's invariant, enforced by our
  watermark instead of their controller).
- **Merge**: two cold sibling actives seal; one successor owns the joined
  prefix.
- Splits move no data: sealed segments are immutable where they are;
  successors start empty.

## 6. Storage, absorption, history

A segment's sub-stream hash is `SHA-256(streamName ‖ "\0seg\0" ‖ ordinal)
[..16]`. Everything downstream — shard-log keyspace, committer, watermark
acks, absorber signals, per-sub-stream history DBs with `k!` indexes,
crypto envelope (the segment hash is the AAD stream identity) — applies to
segments unmodified, because a segment *is* an internal stream. Keyed reads
touch only the segments of that key's chain. Logical stream size is the sum
over segments (per `_details`, later).

## 7. Interactions (specified, phased)

- **state-protocol**: per-segment touch journals; a watch key hashes with
  its routing key, so fine waiters land on the segment owner that ingests
  their key; coarse waits become one collapsed GET per segment. Blocked in
  v1 (see §2).
- **queue profile** — compatibility analyzed (2026-07-08), compatible by
  construction; the create-time rejection is scaffolding, not a design
  boundary. Findings:
  - Consumer state (`c`/`l`/`x` keys) is namespaced under the sub-stream
    hash, so **per-segment consumer state is free**: each segment's queue
    ops run on its own owner's committer, no cross-server coordination.
  - Sealed segments reject *writes*, not queue-state ops, so consumers
    drain a sealed predecessor to its tail while new messages flow to
    successors — **drain-predecessor-first is also the rule that preserves
    per-group FIFO across splits** (Pravega's invariant, inherited).
  - **DLQ appends must become two-stage** (decided): the settle writes a
    durable local DLQ *marker* in the segment's own keyspace (same
    WriteBatch as the lease removal — atomicity restored, settle latency
    unchanged); an async mover drains markers into the global `$dlq`
    routing-key view via routed appends, deleting markers post-ack.
    Idempotent by `(ordinal, offset)` identity; at-least-once DLQ refs;
    markers queryable during the lag. A synchronous cross-shard append was
    rejected: it slows settles without closing the crash window. (Third
    instance of the local-staging + idempotent-async-drain pattern, after
    the absorber and the trim cursor.)
  - **Consumer groups are the product surface** for multi-segment receive
    (Pravega reader-group-like): named groups, segment assignment via
    ops-bucket CAS + heartbeats (same leaderless patterns as shard
    placement), rebalance on membership change. Per-(group, segment) state
    needs no storage changes.
  - Mechanical items: lease tokens/message ids gain the ordinal
    (`<ord>:<off>:<gen>`); backlog becomes a per-segment sum; strict
    FIFO mode (one in-flight batch per group) is an additive per-segment
    check.
  - Sequencing: after segment splits land — the drain-predecessor receive
    rule and split machinery should be tested together.
- **CDN reads**: canonical chunks are per (segment, range) — cache keys
  already include the cursor's epoch.

## 8. Conformance

The upstream Durable Streams suite assumes total order. Measured
(CONFORMANCE.md): default streams **239/239**; per-key with one segment
**239/239** (the degenerate case is served through the standard path and is
byte-identical); per-key with 4 segments **194/239**, with every failure
attributable to the two documented deviations (unkeyed live reads rejected;
closed state segment-scoped in v1).

## 9. v1 implementation status

Implemented: opt-in creation (`Stream-Ordering` / `Stream-Segments`,
config-compared on idempotent PUT), static segment counts (1..=256, power
of two), per-segment sub-streams placed by the shard topology (segments of
one stream land on different shards/servers), epoch-bearing offset tokens,
keyed reads/long-poll/SSE on the key's segment, segment-sequential unkeyed
replay with cross-segment pagination, `Stream-Ordering` +
`Stream-Segment-Count` response headers, single-segment degeneration.
Deferred to the split/seal phase: dynamic split/merge (§5), stream-wide
close (a seal is a close), per-segment touch journals (§7), `Stream-Seq`
scoping documentation per partition.
