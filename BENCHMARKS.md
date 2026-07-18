# Benchmark results — SlateDB rewrite vs existing server

All runs go through **s3lite**, a local in-memory S3 emulator injecting **25ms
latency per object-store operation** (both servers use it as their object
store). Driver: `bench` (reqwest + hdrhistogram), 256-byte payloads, 16
streams, 15s measured after 3s warmup, on one MacBook (server + emulator +
driver share the machine).

- **old** = existing Bun/TypeScript server (`src/server.ts`, SQLite WAL +
  segmenter + uploader), `--object-store r2` pointed at s3lite, tuned for
  fast durability: `DS_SEGMENT_MAX_INTERVAL_MS=1000 DS_SEGMENT_CHECK_MS=100
  DS_UPLOAD_CHECK_MS=100`. Its ACK point is the local SQLite commit.
- **slate** = `streams-slate` (Rust + SlateDB 0.14, 5ms WAL flush interval).
  Its ACK point is durability in object storage.

## Append: one 256B entry per request

| concurrency | old req/s | old p50 / p99 | slate req/s | slate p50 / p99 |
|---:|---:|---:|---:|---:|
| 1    | 96  | 10ms / 14ms      | 31     | 30ms / 37ms |
| 16   | 184 | 85ms / 124ms     | 374    | 37ms / 61ms |
| 64   | 186 | 342ms / 373ms    | 1,581  | 36ms / 62ms |
| 256  | 187 | 1,371ms / 1,389ms| 4,189  | 61ms / 68ms |
| 1024 | —   | —                | 17,091 | 54ms / 64ms |

Zero errors in every run. The old server acks appends after a ~10ms local
group commit, which wins at concurrency 1, but its ingest path saturates near
190 req/s and queueing pushes latency past 1.3s at 256 writers. The SlateDB
ingest path pays one object-store round trip (25ms floor) but bundles every
concurrent request into the same WAL PUT, so throughput scales ~linearly with
offered concurrency at flat latency.

## What an ACK means

`bench --mode durability` appends, then measures how long until the bytes are
actually durable in object storage (polling `/_details uploaded_through` for
the old server):

| | ack latency | ack → object-store durability gap |
|---|---:|---:|
| old   | ~8ms  | **790–1,130ms** (unbounded with default settings*) |
| slate | ~35ms | **0ms** — the ACK *is* object-store durability |

\* with stock settings the old server only seals a segment at 16MiB or 100k
rows — a low-rate stream can stay non-durable (local-disk only) indefinitely.

## Batched JSON appends (10 entries/request, c=256)

slate: 4,097 req/s = **40,967 durable entries/s**, p50 62ms, p99 72ms.

## PUT amplification (from s3lite op counters)

Both servers issue ~30–35 PUTs/s under load. At c=256 the slate server
persists ~120 records per PUT; at c=1024, ~490 records per PUT. Object-store
request cost is essentially flat with load.

## Reads

- warm replay (memtable/block cache): 240–355 MB/s
- cold replay after process restart (every SST block fetched through the 25ms
  emulator): 77 MB/s
- process restart + full recovery from object storage alone: ~3.6s (no local
  state existed)

## Reproducing

```bash
cd slate && cargo build --release
./target/release/s3lite --latency-ms 25 &                # port 9500
./target/release/streams-slate --s3-endpoint http://127.0.0.1:9500 &  # port 8090

# old server against the same emulator
DURABLE_STREAMS_R2_BUCKET=streams-old DURABLE_STREAMS_R2_ACCOUNT_ID=x \
DURABLE_STREAMS_R2_ACCESS_KEY_ID=t DURABLE_STREAMS_R2_SECRET_ACCESS_KEY=t \
DURABLE_STREAMS_R2_ENDPOINT=http://127.0.0.1:9500 DURABLE_STREAMS_R2_REGION=us-east-1 \
DS_ROOT=/tmp/ds-old DS_SEGMENT_MAX_INTERVAL_MS=1000 DS_SEGMENT_CHECK_MS=100 \
DS_UPLOAD_CHECK_MS=100 PORT=8081 bun run src/server.ts --object-store r2 --no-auth &

bench/run_matrix.sh /tmp/results     # full matrix
./target/release/bench --url http://127.0.0.1:8090 --mode append \
  --concurrency 256 --duration-secs 15 --prefix fresh1   # single run
```
