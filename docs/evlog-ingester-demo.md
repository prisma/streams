# Evlog Ingester Demo

`experiments/demo/evlog_ingester.ts` creates a synthetic `evlog` stream that
uses the shipped `evlog` profile and its default index surface.

It is useful when you want a repeatable soak that exercises:

- profile-owned request routing via `requestId`
- routing-key and routing-key lexicon backfill
- exact secondary indexes
- bundled `.col` and `.fts` companion builds
- async index telemetry in `async_index_actions`

The generated records are request-log shaped JSON events with:

- `timestamp`
- `service`
- `environment`
- `version`
- `region`
- `requestId`
- `traceId`
- `spanId`
- `method`
- `path`
- `status`
- `duration`
- `message`
- optional `why`, `fix`, and `link`
- `sampling`
- `context.error.message` and other wide contextual fields

The ingester writes ordinary JSON records and lets the built-in `evlog`
profile normalize and route them.

## Usage

Start a fresh stream:

```bash
bun run experiments/demo/evlog_ingester.ts \
  --url http://127.0.0.1:8787 \
  --stream evlog-1 \
  --reset
```

Resume an existing stream:

```bash
bun run experiments/demo/evlog_ingester.ts \
  --url http://127.0.0.1:8787 \
  --stream evlog-1 \
  --resume-stream
```

Useful knobs:

- `--batch-size N`
- `--delay-ms N`
- `--report-every-ms N`
- `--request-timeout-ms N`
- `--retry-delay-ms N`
- `--max-batches N`

## Installed Index Surface

The `evlog` profile auto-installs:

- routing key derived from `requestId` with `traceId` fallback
- exact indexes for keyword and typed evlog fields
- bundled `.col` companions for timestamp and numeric fields
- bundled `.fts` companions for keyword prefix and text fields

That makes this demo the right soak when you want to inspect mixed async-index
backfill timings from the local `async_index_actions` table.

Current performance-focused implementation details:

- exact-secondary L0 backfill splits high-cardinality fields into one-field
  worker actions
- bundled companion builds use compiled field accessors directly on the
  no-rollup path instead of materializing a per-record raw-value map
- `.fts` encoding has a singleton-posting fast path for high-cardinality
  keyword-prefix fields such as `requestId`, `traceId`, and `spanId`

## Telemetry Queries

Recent async index timings for the stream:

```sql
SELECT
  seq,
  action_kind,
  target_kind,
  target_name,
  duration_ms,
  input_count,
  input_size_bytes,
  output_count,
  output_size_bytes,
  detail_json
FROM async_index_actions
WHERE stream = 'evlog-1'
ORDER BY seq DESC
LIMIT 50;
```

Grouped latency summary by build kind:

```sql
SELECT
  action_kind,
  COUNT(*) AS runs,
  ROUND(AVG(duration_ms), 1) AS avg_duration_ms,
  MAX(duration_ms) AS max_duration_ms
FROM async_index_actions
WHERE stream = 'evlog-1'
  AND status = 'succeeded'
GROUP BY action_kind
ORDER BY avg_duration_ms DESC;
```
