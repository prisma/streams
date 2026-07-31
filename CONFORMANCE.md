# Durable Streams conformance

The singular route `/v1/stream/{name}` is a Durable Streams server. This
is where that claim is tested, against the pinned upstream suite, run
unmodified.

| pin | value |
|---|---|
| suite | `@durable-streams/server-conformance-tests@0.3.6` |
| recorded in | `src/protocol_pin.rs`, `conformance/package.json` (locked) |
| expected outcome | `conformance/expected.json` |

Latest run — 2026-07-31, after the audit response (auth, create
initialization, seal state machine, fork lifecycle, catalog paging,
route grammar, watch derivation):

**332 passed · 0 failed · 6 skipped (338).**

## How to run

```bash
cargo build --release --bin s3lite --bin streams-slate --bin streams-keys
./target/release/s3lite --listen 127.0.0.1:9500 --latency-ms 2 &
KEY=$(./target/release/streams-keys generate)
./target/release/streams-slate --listen 127.0.0.1:8090 \
  --s3-endpoint http://127.0.0.1:9500 --bucket conf-$RANDOM \
  --max-unflushed-bytes 67108864 \
  --flush-interval-ms 1 --wal-flush-gap-ms 2 \
  --conformance-default-key "$KEY" &
cd conformance && npm ci && CONFORMANCE_TEST_URL=http://127.0.0.1:8090 npm test
```

`npm test` runs the suite and then `check.mjs`, which compares the run
against `expected.json` — the totals AND the family every skip must
belong to. A pass count alone would not catch a suite that ran nothing,
or a test that quietly turned into a skip.

Do not use the package's own `npx durable-streams-server-conformance-tests`
CLI: its include glob does not match the runner it invokes, so it
reports zero tests and exits 0. `conformance/conformance.test.mjs`
imports the entry point directly instead.

## The 6 skipped tests

All six are the OPTIONAL reserved webhook-subscription API
(`__ds/subscriptions/*`). The suite runs them only when the harness
passes `subscriptions: true`; this server does not implement them, so
the harness does not. The product consumer API
(`/v1/streams/{name}/consumers/…`) covers the same need with leases and
settlement, and it is the surface the product SDK exposes.

| # | test |
|---|---|
| 1 | Reserved subscription APIs › creates and idempotently re-confirms a webhook subscription |
| 2 | Reserved subscription APIs › rejects unsafe webhook URLs |
| 3 | Reserved subscription APIs › webhook synchronous done auto-acks the wake snapshot |
| 4 | Reserved subscription APIs › webhook callback acks and fences stale wake generations |
| 5 | Reserved subscription APIs › adds and removes explicit subscription streams |
| 6 | Reserved subscription APIs › pull-wake claim, ack, and release use subscription-scoped leases |

Implementing them re-opens this gate; until then the posture is
"optional feature not implemented", not "passing".

## Rig requirements

1. **`--conformance-default-key <b64>`** — the suite cannot send custom
   headers, and this server requires an encryption key on every
   create/append/read. The flag supplies one for requests that lack the
   header. Dev and conformance only, never a deployment flag.
2. **A fresh bucket per run.** Suite streams accumulate; a reused
   namespace produces config-mismatch 409s by design.
3. **Group commit** (`--flush-interval-ms 1 --wal-flush-gap-ms 2`). The
   suite's fast-check property tests run ~240 sequential appends inside
   vitest's 5 s budget. At the default cadence an append costs ~28 ms
   against s3lite, so those tests are latency-marginal; with group
   commit — what field deployments already run — appends drop to
   ~8.6 ms and they pass consistently. Measured both ways (28.71 vs
   28.54 ms per append, before and after the audit fixes), so the
   marginality is the rig's flush cadence and not a code regression.

The suite itself is never modified.

## History

- **2026-07-08** — first baseline, against an earlier 239-test suite:
  78/239 on the first run, then 239/239. That run drove content-type as
  create-time config, the full producer-idempotence semantics (epoch
  fencing, duplicates, gaps, strict grammar, dedupe-before-everything),
  closed-stream behaviour, SSE framing and control events, `offset=now`,
  ETag/`If-None-Match`, long-poll shapes, TTL grammar, no-auto-create on
  POST, JSON batching rules, delete→recreate isolation, and the response
  header rules. Two engine fixes fell out: zero-work commit groups ack
  directly, and long-poll at the tail of a closed stream returns 204
  without waiting.
- **2026-07-31** — pinned 0.3.6 (338 tests). First run 265/67/6; every
  failure was a feature the newer baseline had added, not a regression:
  forks (58), sliding TTL (5), per-data SSE control pairing (2), CORS
  preflight (1), 413 for oversized bodies (1). Implementing those
  reached 332/0/6. Two finds worth keeping: the TTL slide raced the
  close path's descriptor seal (a single-shot CAS lost the 412; fixed
  with a bounded retry), and per-request slide spawns herded the
  registry under rapid op sequences (fixed with an in-flight-slide set,
  which also cleared a property-test timeout).
- **2026-07-31, after the audit response** — unchanged at 332/0/6,
  confirming that the product-surface work (bearer auth, seal states,
  fork lifecycle, catalog paging, suffix route grammar, SHA-256 watch
  keys) left the protocol surface intact.
