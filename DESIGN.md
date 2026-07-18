# Streams-on-SlateDB rewrite design

A rewrite of the Prisma Streams full server on top of [SlateDB](https://slatedb.io/)
(Rust, `slatedb = 0.14`), replacing the SQLite WAL + segmenter + uploader +
manifest pipeline with a single LSM engine whose WAL and SSTs live directly in
object storage.

## Why SlateDB fits

The current architecture hand-builds a tiered store: SQLite WAL for the tail,
sealed segments on local disk, an uploader that publishes segments + manifest
to R2, WAL GC after publication, and several background index/compaction loops.
SlateDB is exactly that machinery, generalized: an object-store-native LSM with

- a WAL that is flushed to object storage on a configurable interval
  (`flush_interval`), where **all writes that arrive during one interval are
  bundled into a single WAL SST PUT** (group commit against object storage),
- `WriteOptions { await_durable: true }`: the write future resolves only after
  the WAL SST containing it is durably in object storage,
- memtable → L0 SST flush + background compaction (replaces segmenter/uploader),
- manifest CAS in object storage (replaces manifest.json publication),
- block cache + optional local object cache (replaces `DS_ROOT/cache`),
- crash recovery entirely from object storage (stronger than today: no local
  SQLite is needed for correctness; local disk is only a cache).

## The changed durability contract (per request)

**Old:** append is ACKed after the local SQLite group commit (~10ms window);
object-store durability happens minutes later (segment seal + upload + manifest).

**New:** append is ACKed only after the bytes are durable in object storage.
Many concurrent append requests are bundled into one WAL SST write. The ingest
path is:

```
HTTP append -> bounded ingest queue
  -> committer loop: drain queue (group) -> one WriteBatch
     (records + per-stream tail pointers) -> db.write(await_durable=false)
     (ordered memtable/WAL-buffer apply), push {seqnum, acks} in-flight
  -> acker loop: watch db.subscribe() durable_seq; when the watermark passes
     a group's seqnum, promote its tail snapshots to the readers' durable
     view, ACK every request in it, wake long-pollers
```

Three levels of batching compose: the committer groups queued requests into
one `WriteBatch`, SlateDB groups everything in a flush interval (5ms default
here) into one WAL object PUT, and commits pipeline — new groups keep
accumulating while earlier WAL PUTs are in flight, so ack latency stays near
one PUT RTT and throughput scales with concurrency. Because the committer
publishes shared stream state only after a successful memtable apply and the
acker only advances on the durability watermark, there are no rollback paths:
a failed batch write touches nothing, and an object-store outage just stalls
the watermark until backpressure (429) kicks in.

Readers use `durability_filter = Remote`, so **reads never observe data that
is not yet durable in object storage** (the old system could serve
locally-acked data that a total node loss would destroy).

## Key layout (single SlateDB keyspace)

| key                                   | value |
|---------------------------------------|-------|
| `m!<stream name>`                     | meta JSON: created_ms, expires_at_ms?, deleted |
| `t!<hash16>`                          | tail: next_offset u64, last_ts_ms i64, last Stream-Seq |
| `r!<hash16><offset u64 BE>`           | record: ver, ts_ms, routing key, payload |

`hash16` = first 16 bytes of SHA-256 of the stream name (same as the current
segment key hashing). Fixed-length hash prevents prefix collisions and keeps
record keys at 26 bytes. Tail pointers are written in the same `WriteBatch` as
the records, so offset state is atomic with the data, and recovery of
`next_offset` is a point read instead of a reverse scan.

## Protocol surface (v1 subset)

- `PUT /v1/stream/{name}` (create, `Stream-TTL`/`Stream-Expires-At`, 201/200)
- `POST /v1/stream/{name}` byte mode + JSON array mode, `Stream-Key`,
  `Stream-Timestamp` (monotonic clamp), `Stream-Seq` (lexicographic OCC → 409),
  ACK carries `Stream-Next-Offset`; appends auto-create streams (as today)
- `GET /v1/stream/{name}?offset=&format=json&live=long-poll&timeout=&key=`
  with `Stream-Next-Offset` / `Stream-End-Offset`; byte-concat or JSON array
- `HEAD /v1/stream/{name}`, `DELETE /v1/stream/{name}`, `GET /v1/streams`,
  `GET /health`
- Offsets: identical 26-char Crockford base32 of (epoch u32, rawSeq=seq+1 as
  u64, in_block u32), `-1` sentinel accepted; `offset=X` returns entries > X.

Not in v1: schemas/profiles beyond `generic`, `filter=`, `since=`, search,
live/touch, expiry sweeping, multi-epoch offsets.

## Retained properties

- group commit on the ingest path (now to object storage, not SQLite)
- bounded queues + explicit 429 backpressure; bounded memory (memtable caps)
- crash safety: atomic batches; recovery from object store alone (improved)
- read-your-writes for acking producers; long-poll without busy loops (Notify)
- tail reads served from memory (memtable), historical reads via cached SSTs
- routing-key filtered reads (naive scan filter in v1; index later)
- monotonic per-stream timestamps, `Stream-Seq` OCC, TTL metadata
- background compaction with rate limits out of the request path (SlateDB's)

## Benchmark plan

`s3lite`: a local S3-compatible emulator (Rust/axum) with a configurable
injected latency (25ms per operation), implementing PUT/GET(range)/HEAD/DELETE/
ListV2/multipart + `If-Match`/`If-None-Match` conditional PUTs (needed by
SlateDB's manifest CAS) — usable by both servers:

- new server: `object_store::aws::AmazonS3` → s3lite
- old server: `DURABLE_STREAMS_R2_ENDPOINT` → s3lite (its hand-rolled SigV4
  client is endpoint-configurable; s3lite ignores auth)

`bench`: HTTP load driver (Rust/reqwest + hdrhistogram) measuring, for each
server: append ACK latency (p50/p90/p99) and throughput at concurrency
{1, 16, 64, 256}, replay read throughput, and — crucially — **durability lag**:
time from append ACK until the data is actually durable in object storage
(0 by construction for the new server; polled via `/_details` uploadedThrough
for the old one). s3lite also reports op counts so we can compare PUT
amplification.
