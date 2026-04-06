# Memory Observability: Ingest Pipeline Buffers

This group exposes live bytes that belong to the current ingest/segment/upload
pipeline rather than long-lived caches.

Observability fields and metrics:

- `runtime_bytes.pipeline_buffers`
- `runtime_totals.pipeline_buffer_bytes`
- `runtime_counts.segmenter_active_builds`
- `runtime_counts.segmenter_active_streams`
- `runtime_counts.segmenter_active_rows`
- `runtime_counts.uploader_inflight_segments`
- `runtime_counts.uploader_manifest_inflight_streams`
- `tieredstore.memory.subsystem.bytes{kind="pipeline_buffers",...}`

Current byte fields:

- `segmenter_active_payload_bytes`
- `segmenter_active_segment_bytes_estimate`
- `uploader_inflight_segment_bytes`

Where implemented:

- `src/segment/segmenter.ts`
- `src/segment/segmenter_worker.ts`
- `src/segment/segmenter_workers.ts`
- `src/uploader.ts`
- `src/app.ts`

Interpretation:

- `segmenter_active_payload_bytes`
  - WAL payload bytes currently being packed into a segment
- `segmenter_active_segment_bytes_estimate`
  - estimated encoded segment bytes for the in-progress build
- `uploader_inflight_segment_bytes`
  - bytes of segments currently being uploaded by the uploader

Worker behavior:

- when the segmenter runs in worker-thread mode, each worker publishes periodic
  memory snapshots back to the main process
- the server endpoint aggregates the most recent worker reports into one current
  view

These counters are intentionally "live buffer" views. They should rise and fall
with work, unlike cache or mmap totals which can remain elevated across many
requests.
