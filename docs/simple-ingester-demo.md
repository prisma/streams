# Simple Ingester Demo

`experiments/demo/simple_ingester.ts` is the lowest-complexity long-soak
workload for Prisma Streams. It avoids external data sources entirely and
ingests synthetic JSON documents into exactly one stream.

It is intended for:

- memory-soak validation without GH Archive sparsity
- routing-key indexing validation with no search families
- isolated throughput and upload experiments

## Stream Shape

Each appended event has this shape:

```json
{
  "randomString": "<100 random base36 chars>",
  "randomNumber": 123456789,
  "id": 42,
  "cardinality100": "<one of 100 fixed 50-char strings>",
  "cardinality1000": "<one of 1000 fixed 50-char strings>",
  "time": "2026-04-07T00:00:00.000Z"
}
```

Behavior:

- `id` is the append-side synthetic primary counter and starts at `0`
- on `--resume-stream`, the ingester resumes `id` from the stream's current
  `next_offset`
- `randomString`, `randomNumber`, and the two cardinality fields are generated
  deterministically from `id`, so resume does not require local generator state

## Installed Stream Config

The demo creates a single `generic` profile stream and applies only:

```json
{
  "routingKey": { "jsonPointer": "/cardinality100", "required": true }
}
```

That means:

- routing-key index is enabled
- routing-key lexicon is enabled
- no search config is installed
- no exact, `.col`, `.fts`, or `.agg` families are installed

This is the supported minimal indexed shape for routing-key memory and
throughput experiments.

## Usage

Fresh start:

```bash
bun run experiments/demo/simple_ingester.ts \
  --url http://127.0.0.1:8787 \
  --stream simple-1 \
  --reset
```

Resume an existing stream:

```bash
bun run experiments/demo/simple_ingester.ts \
  --url http://127.0.0.1:8787 \
  --stream simple-1 \
  --resume-stream
```

Bounded local test run:

```bash
bun run experiments/demo/simple_ingester.ts \
  --url http://127.0.0.1:8787 \
  --stream simple-1 \
  --batch-size 1000 \
  --max-batches 5 \
  --reset
```

## Flags

- `--url <base-url>`
  Streams server base URL. Default: `http://127.0.0.1:8787`
- `--stream <name>`
  Target stream name. Default: `simple-1`
- `--batch-size <n>`
  Events per append request. Default: `5000`
- `--delay-ms <n>`
  Delay between batches. Default: `0`
- `--report-every-ms <n>`
  Progress logging cadence. Default: `5000`
- `--request-timeout-ms <n>`
  Per-append timeout. Default: `30000`
- `--retry-delay-ms <n>`
  Backoff after timeout/408/429/503. Default: `1000`
- `--max-batches <n>`
  Stop after `n` batches. Default: unlimited
- `--resume-stream`
  Reuse the existing stream and continue `id` from current `next_offset`
- `--reset`
  Delete the stream first, then recreate it with the supported routing-only
  config

## Operational Expectations

For this demo:

- retained WAL should stay small when uploads/manifests are current
- search-family memory should remain zero
- any OOM regression is much more likely to be in the core ingest/runtime path
  or routing-key indexing path than in search companions
