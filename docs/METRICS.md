# Metrics Stream: __stream_metrics__

This document describes the **metrics events actually emitted by this Bun+TS server**.
The server appends interval metrics to the `__stream_metrics__` stream as JSON.

## Emission

- Flush interval: `DS_METRICS_FLUSH_MS` (default 10,000 ms; 0 disables)
- Destination: `__stream_metrics__` stream
- Format: JSON entries with `apiVersion = durable.streams/metrics/v1`

## Event format (interval)

Each event includes:

- `apiVersion`: `durable.streams/metrics/v1`
- `kind`: `interval`
- `metric`: metric name
- `unit`: `ns`, `bytes`, or `count`
- `windowStart`, `windowEnd`: Unix millis
- `intervalMs`: window duration in ms
- `instance`: unique instance id (pid + random suffix)
- `stream`: optional stream name
- `tags`: optional dimensions
- `count`, `sum`, `min`, `max`, `avg`, `p50`, `p95`, `p99`
- `buckets`: histogram buckets

## Metrics currently emitted

This implementation emits a **rich set** of interval metrics. All values are aggregated
over the `DS_METRICS_FLUSH_MS` window (default 10s).

### Ingest + backpressure

- `tieredstore.ingest.flush.latency` (ns): end‑to‑end ingest batch flush latency.  
  **Use:** correlate backpressure spikes with slow flushes.
- `tieredstore.ingest.sqlite_busy.wait` (ns): time spent retrying SQLITE_BUSY during flush.  
  **Use:** confirms SQLite contention as a backpressure cause.
- `tieredstore.ingest.queue.bytes` (bytes): queued bytes at sample time.  
  **Use:** rising avg/peaks indicate sustained ingress pressure.
- `tieredstore.ingest.queue.requests` (count): queued requests at sample time.
- `tieredstore.backpressure.over_limit` (count, tag `reason`): count of rejected appends.  
  `reason=memory` (RSS limit), `reason=queue` (queue limit), `reason=backlog` (local backlog gate).  
  **Use:** identifies *why* 429s are happening.

### Process memory

- `process.rss.bytes` (bytes): periodic RSS samples (1s cadence).  
  **Use:** avg vs max shows how spiky RSS is.
- `process.rss.over_limit` (count): number of samples above memory limit.  
  **Use:** indicates how long the process stays above `DS_MEMORY_LIMIT_*`.

### Append / read throughput

- `tieredstore.append.bytes` (bytes): bytes appended per window.
- `tieredstore.append.entries` (count): entries appended per window.
- `tieredstore.read.bytes` (bytes): bytes returned per window.
- `tieredstore.read.entries` (count): entries returned per window.

### Indexing + compaction

- `tieredstore.index.lag.segments` (count, per‑stream): uploaded‑through vs indexed‑through gap.  
  **Use:** high lag indicates indexer falling behind.
- `tieredstore.index.build.queue_len` (count): build queue depth.
- `tieredstore.index.builds_inflight` (count): concurrent index builds.
- `tieredstore.index.build.latency` (ns, tag `level`, per‑stream): time to build a run.
- `tieredstore.index.runs.built` (count, tag `level`, per‑stream): runs built per window.
- `tieredstore.index.compact.latency` (ns, tag `level`, per‑stream): time to compact runs.
- `tieredstore.index.runs.compacted` (count, tag `level`, per‑stream): runs compacted per window.
- `tieredstore.index.bytes.read` (bytes, tag `level`, per‑stream): bytes read to build/compact.
- `tieredstore.index.bytes.written` (bytes, tag `level`, per‑stream): bytes written for runs.
- `tieredstore.index.active_runs` (count, optional tag `level`, per‑stream): number of active runs.

### Index run caches

Tags: `cache=mem` or `cache=disk`.

- `tieredstore.index.run_cache.used_bytes` (bytes): cache size.
- `tieredstore.index.run_cache.entries` (count): cache entries.
- `tieredstore.index.run_cache.hits` / `misses` (count): cache hit/miss counts.
- `tieredstore.index.run_cache.evictions` (count): evictions.
- `tieredstore.index.run_cache.bytes_added` (bytes, disk cache only): bytes added.

---

**Debugging tips**

- Backpressure spikes: check `tieredstore.ingest.flush.latency`, `tieredstore.ingest.sqlite_busy.wait`,
  and `tieredstore.ingest.queue.*` first, then `tieredstore.backpressure.over_limit` to see root cause.
- Memory spikes: compare `process.rss.bytes` avg vs max and the `process.rss.over_limit` count.
- Index slowness: watch `tieredstore.index.lag.segments`, build/compact latencies, and cache hit rate.
