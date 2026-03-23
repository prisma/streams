# Prisma Streams Segment Performance

Status: **informational**. This repo does not ship a dedicated segment perf tool yet.

Current behavior:

- The read path uses the **segment footer index** to avoid full scans.
- For remote segments, the reader performs **range reads** (header + block ranges) instead of downloading entire objects.
- A bounded **disk cache** (`DS_SEGMENT_CACHE_MAX_BYTES`) stores recently used segment objects.

If you need a repeatable perf benchmark, use the synthetic ingest benchmark and
measure read latency at the HTTP layer, or build a small custom driver that:

1) Appends a known number of records
2) Forces a segment seal
3) Issues keyed and unkeyed reads
4) Measures response latency and bytes read
