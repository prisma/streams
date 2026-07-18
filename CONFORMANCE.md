# Durable Streams conformance results

Upstream suite: `@durable-streams/server-conformance-tests@0.3.6`, pinned
together with `vitest@4.0.17` in CI. The published package currently contains
338 tests. The release run on 2026-07-18 used s3lite with 1 ms injected
latency and a 10 ms remotely durable WAL flush cadence.

## How to run

```bash
cargo build --release --bin s3lite --bin streams-slate --bin streams-keys
bash scripts/ci-conformance.sh
```

The hermetic wrapper creates a fresh S3 namespace, starts and owns both
processes, installs exact npm versions in a scratch directory, and cleans up.
Package 0.3.6's CLI points Vitest 4 at a path under `node_modules`, which
Vitest excludes. The wrapper copies the published bundled runner unchanged to
the scratch root and invokes it directly. `CONFORMANCE_TEST_FILTER` can select
a test name/regex for local diagnosis.

## Accommodations

1. **`--conformance-default-key <b64>`** supplies the customer-held stream
   key because the suite cannot add Prisma's encryption header. It is accepted
   only in explicit development/conformance mode.
2. The wrapper uses a **fresh bucket** because retained stream configuration
   correctly makes incompatible reruns return 409.
3. The local flush interval is 10 ms. Production remains 25 ms; three repeated
   runs of the suite's five-second, 25-run sequential-offset property passed
   at 10 ms while preserving remote-durability-before-ACK.

No test source or expectation is patched.

## Result

| configuration | result |
|---|---|
| total order (release build) | **332 passed, 6 skipped, 0 failed (338 total)** |

The six upstream skips are the package's optional subscription tests. All
executed tests—including fork creation/reading/recursive/lifecycle/TTL,
sliding TTL, CORS, SSE framing, producer idempotence, and fast-check property
tests—pass. CI runs this complete suite, not a curated subset.

## What the baseline run drove into the implementation

Getting from the first run (78/239) to 239/239 implemented, per the upstream
protocol: content-type as create-time stream config (409 on append/PUT
mismatch, case-insensitive, parameters stripped); **producer headers** with
full idempotence semantics (epoch fencing 403, duplicates 204 with highest
seq echoed, gap 409 with `Producer-Expected/Received-Seq`, strict integer
grammar, dedupe-before-everything ordering); **closed streams**
(`Stream-Closed` on PUT/POST, close-only POSTs, 409-after-close with final
offset, idempotent close, producer-close interactions); **SSE**
(`live=sse`: exact `event:`/`data:` framing, control events with
`streamNextOffset`/`streamCursor`/`upToDate`/`streamClosed`, base64
auto-encoding for binary content types + `Stream-SSE-Data-Encoding`,
CRLF-injection-safe line splitting, connection close on closed streams);
`offset=now`; ETag + `If-None-Match` → 304; `Stream-Up-To-Date`,
`Stream-Cursor` with collision jitter; long-poll `offset` requirement and
204-timeout shape; strict TTL grammar; **no auto-create on POST** (404);
JSON batching (top-level array = batch, anything else = one message; empty
array 400 on POST / allowed on PUT); delete→recreate isolation via
per-incarnation storage hashes; `Location` on 201; `X-Content-Type-Options:
nosniff` on all responses; `Cross-Origin-Resource-Policy` on reads;
`Cache-Control` rules. Two engine fixes fell out: zero-work commit groups
are ACKed directly (an empty WriteBatch never crosses the durable
watermark), and long-poll-at-tail on closed streams returns 204 without
waiting.

Protocol-surface changes worth noting: byte-mode reads now return the
decrypted appended bytes (protocol-conformant); the ciphertext wire frames
moved behind an explicit `format=frames`.
