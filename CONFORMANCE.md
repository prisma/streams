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


## Run 2026-07-31 — pinned suite 0.3.6 (final-gate baseline run)

Suite: `@durable-streams/server-conformance-tests@0.3.6` (pinned in
`src/protocol_pin.rs`), 338 tests (grown from 239 since the 2026-07-08
run). s3lite (2 ms latency), fresh bucket, `--conformance-default-key`,
`--max-unflushed-bytes 67108864`.

Result: **265 passed, 67 failed, 6 skipped.**

Triage of the 67 (all are pinned-baseline features the suite added
since the 239-test run — no regressions from the product-surface work):

| family | count | gap |
|---|---:|---|
| Fork (creation, sub-offsets, reads across boundaries, recursive, live modes, soft-delete/410 lifecycle, GC cascade, TTL inheritance, JSON mode) | 58 | Forks are NOT implemented — `Stream-Forked-From` headers are accepted as plain creates; no source-prefix reads, no reference lifecycle. The largest remaining protocol feature. |
| TTL sliding window | 5 | `Stream-TTL` idle expiry must RESET on origin reads/writes/close; we compute `expires_at` once at create and never slide it. |
| SSE catch-up control pairing | 2 | The 0.3.6 baseline expects a control event after EVERY data event during catch-up; we emit one control per batch. |
| CORS preflight `If-None-Match` | 1 | `OPTIONS` returns 405; the baseline wants a preflight that allows the header. |
| Large payload status | 1 | An oversized body answers 429 (admission) where the baseline wants 413 (or 200/204). |

These are the open items of the final release gate (task #87). The
239 tests of the previous baseline remain green within the 265.

## Run 2026-07-31 (later) — FULL SUITE GREEN

After implementing forks, sliding TTL, per-data SSE control pairing,
the CORS preflight, and 413-for-oversized: **332 passed, 0 failed,
6 skipped (338)** — the complete pinned 0.3.6 suite. Notable finds on
the way: the TTL slide CAS raced the close path's descriptor seal
(single-shot cas_update lost to a 412; fixed with a bounded retry),
and per-request slide spawns herded the registry under rapid op
sequences (fixed with an in-flight-slide set; this also cleared the
property-test timeout).

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
