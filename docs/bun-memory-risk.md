# Bun Memory Risk Policy

This document is repository policy for Bun APIs that have shown native memory
retention under sustained Streams workloads. Treat it as authoritative when
changing object-store, fetch-body, file-body, ingest, or background indexing
code.

## Rule

Do not introduce high-volume use of Bun APIs that materialize native-backed
`Blob`, `File`, `S3File`, `ArrayBuffer`, or request/response bodies without a
specific memory investigation.

Prefer explicit streaming APIs and bounded byte budgets. If a code path must
materialize bytes, it must be protected by all of the following:

- a documented size bound
- bounded concurrency
- memory-sampler coverage for RSS, anon RSS, heap, external, and arrayBuffers
- a local 1 GiB Linux-container stress test when the path can run in production

Forced `Bun.gc(true)` is not an acceptable mitigation by itself. The observed
failure mode is RSS, especially anon RSS, staying high while JS heap and
`arrayBuffers` are much lower.

## Risky APIs

Avoid these APIs in long-lived, high-volume production paths:

- `Bun.S3Client`
- `Bun.S3File`
- `Bun.S3File.arrayBuffer()`
- `Response.arrayBuffer()` over repeated remote downloads
- `Response.blob()` over repeated remote downloads
- `Bun.file(path).arrayBuffer()`
- `Bun.file(path).bytes()`
- `Bun.file(path).text()` for repeated large files
- `Bun.file(path)` as a fetch upload body in sustained object-store upload paths

Small tests, CLI utilities, and bounded local-only helpers may still use these
APIs, but production server paths must use extra care. If in doubt, treat the
API as unsafe until a memory sampler run proves otherwise.

## Preferred Patterns

For object-store uploads:

- use signed `fetch()` requests instead of `Bun.S3Client`
- stream file uploads with `node:fs.createReadStream()` converted through
  `node:stream.Readable.toWeb()`
- keep upload concurrency bounded by the memory preset
- avoid hidden follow-up reads or stats unless required for correctness

For object-store reads:

- prefer ranged reads or streaming reads
- use `Response.body.getReader()` rather than `Response.arrayBuffer()`
- if the object-store interface must return `Uint8Array`, collect chunks from
  the stream reader under a known object/range size limit

For local files:

- prefer `node:fs` streams for large or repeated reads
- use `Bun.mmap()` only for immutable cache files whose pinned mapping is
  intentionally tracked as a cache/leak-candidate budget
- avoid repeated `Bun.file().arrayBuffer()`, `bytes()`, or `text()` loops over
  large files in the server

For HTTP request bodies:

- keep append bodies capped by `DS_APPEND_MAX_BODY_BYTES`
- keep ingest concurrency and queue bytes bounded
- on low-memory presets, close append keep-alive connections and keep the
  post-append GC path throttled and observable

## Streams Evidence

The production symptom was repeated Compute OOM kills during external event
ingestion. The generator was not colocated with the Streams server, so the
memory pressure belonged to the Streams server process and its background
segment/upload/index work.

Production failures had this shape:

- process killed around `809 MiB` anon RSS plus about `51 MiB` shmem RSS
- JS heap, external memory, and tracked application counters did not explain
  the RSS high water
- the host clamped a nominal `1024 MB` preset to about `684.9 MiB` of internal
  pressure headroom before the kernel killed `bun`

Local reproduction and fixes:

- MockR2 with `300ms` operation latency alone did not reproduce the OOM shape.
  A 500k-event run peaked around `340 MB` RSS and `290 MB` anon RSS.
- The R2-compatible path using Bun's native S3 implementation against MinIO did
  reproduce production-shaped pressure. A 900k-event, 1 GiB Linux-container run
  reached about `850.9 MB` RSS and `834.4 MB` anon RSS during background
  companion catch-up.
- Replacing `Bun.S3Client` / `S3File` with signed `fetch()` R2 requests dropped
  the same class of run to about `533.8 MB` RSS and `506.1 MB` anon RSS.
- Removing the remaining `Bun.file(path)` upload body and
  `Response.arrayBuffer()` R2 reads reduced the streamed R2 path further. A
  900k-event, 1 GiB Linux-container run peaked at about `474.8 MB` RSS and
  `461.2 MB` anon RSS, with cgroup `memory.peak` about `637.1 MB`, and settled
  near `204.7 MB` RSS and `188.9 MB` anon RSS.

Interpretation: R2 latency can increase overlap between upload and background
work, but the decisive local reproduction came from Bun native S3/body
materialization. Avoiding the Bun S3 API and avoiding remaining Blob/File
`arrayBuffer` paths materially reduced anon RSS.

## Linked Bun Issues

These issues were open or still relevant when this document was written on
2026-04-25. Re-check status before removing any guardrail.

- [oven-sh/bun#29083](https://github.com/oven-sh/bun/issues/29083):
  `Bun.S3File.arrayBuffer()` retains RSS and reaches OOM in a 1 GiB Linux
  container despite forced GC. This is the closest public repro to the Streams
  R2 failure.
- [oven-sh/bun#28741](https://github.com/oven-sh/bun/issues/28741):
  fetch `Blob` / `ArrayBuffer` memory is not reclaimed after references are
  cleared and GC is forced.
- [oven-sh/bun#20487](https://github.com/oven-sh/bun/issues/20487):
  large file downloads through `@google-cloud/storage` and Bun S3 keep RSS high
  after GC; the reporter observed Node returning closer to baseline while Bun
  accumulated RSS.
- [oven-sh/bun#28427](https://github.com/oven-sh/bun/issues/28427):
  simple repeated fetch polling report marked as a Bun memory leak / needs
  triage.
- [oven-sh/bun#15020](https://github.com/oven-sh/bun/issues/15020):
  repeated file reads with `node:fs` and `Bun.File` reported as memory not being
  freed.
- [oven-sh/bun#12941](https://github.com/oven-sh/bun/issues/12941):
  earlier Blob/ArrayBuffer GC-retention report. This one was closed as not
  planned, but it is relevant history because later open reports describe the
  same retention class.

## Review Checklist

Before merging a change that touches body, file, fetch, or object-store code,
check:

- Does the change add `Bun.S3Client`, `Bun.S3File`, `Bun.file()`, `.blob()`, or
  `.arrayBuffer()` to a hot path?
- If bytes are materialized, what is the maximum size and concurrency?
- Is the memory visible in `GET /v1/server/_mem` or
  `DS_MEMORY_SAMPLER_PATH` output?
- Has the path been tested in a memory-limited Linux container when it can run
  on Compute?
- If RSS/anon RSS remains high after work completes, did heap, external,
  `arrayBuffers`, SQLite stats, active jobs, ingest queue bytes, and index or
  companion phases explain it?

If the answer is unclear, use the streaming alternative first.
