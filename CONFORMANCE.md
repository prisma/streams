# Durable Streams conformance results

Upstream suite: `@durable-streams/server-conformance-tests` (239 tests),
installed from npm. (A vendored copy with vitest wiring lived in the
previous TypeScript implementation — available in git history before the
`slate` branch landed.) Run 2026-07-08 against `streams-slate` with s3lite
(5 ms injected latency).

## How to run

```bash
# emulator + server (fresh bucket per run — the suite assumes a clean namespace)
./target/release/s3lite --listen 127.0.0.1:9500 --latency-ms 5 &
KEY=$(./target/release/streams-keys generate)
./target/release/streams-slate --listen 127.0.0.1:8090 \
  --s3-endpoint http://127.0.0.1:9500 --bucket conf-$RANDOM \
  --conformance-default-key "$KEY" &

# suite: install @durable-streams/server-conformance-tests in a scratch
# directory and run it per its README (it ships an npx CLI; point it at the
# server with CONFORMANCE_TEST_URL=http://127.0.0.1:8090)
```

## Changes required to run the suite (accommodations)

1. **`--conformance-default-key <b64>`** — the suite cannot send custom
   headers, and this server requires `Stream-Encryption-Key` on every
   create/append/read. The flag supplies a key for requests that lack the
   header. Dev/conformance only.
2. **`--conformance-ordering-segments <N>`** (per-key runs only) — the suite
   creates streams with plain PUTs, so this applies
   `Stream-Ordering: per-key` + `Stream-Segments: N` to headerless creates.
3. A **fresh bucket per run**: suite streams accumulate; reruns against a
   used namespace hit config-mismatch 409s by design.

No changes to the suite itself were needed.

## Results

| configuration | result |
|---|---|
| total order (default) | **239 / 239** |
| `ordering: per-key`, 1 segment | **239 / 239** (degenerate case: per-key with one segment is totally ordered and byte-identical to default — served through the standard path) |
| `ordering: per-key`, 4 segments | **194 / 239** — all 45 failures are the two deviations specified in PER-KEY-ORDERING.md §4, below |

### The 45 expected failures at 4 segments

- **Unkeyed live reads are rejected (`400 unsupported_on_per_key`)** —
  accommodation #1: the whole-stream tail has no single durable cursor once
  writes are concurrent across segments. This covers every SSE test, every
  unkeyed long-poll test, and the `offset=now`+live tests (~42 tests). Keyed
  live reads (`key=` + long-poll/SSE) work and are exercised by our own e2e.
- **Closed state is segment-scoped in v1** (~3 tests): a close request
  routes to its routing key's segment (unkeyed close → the empty-key
  segment), so stream-wide `Stream-Closed` reporting on unkeyed reads of a
  multi-segment stream is not implemented. Stream-wide close lands with the
  split/seal machinery (a seal *is* a close).

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
