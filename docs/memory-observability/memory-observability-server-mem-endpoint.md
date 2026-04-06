# Memory Observability: Server Memory Endpoint

`GET /v1/server/_mem` is the compact operator-facing snapshot for current
process memory state. It complements `GET /v1/server/_details`:

- `/_details` is the broad configured-and-live node descriptor
- `/_mem` is the denser memory triage payload

Response fields:

- `ts`
- `process`
- `process_breakdown`
- `sqlite`
- `gc`
- `high_water`
- `counters`
- `runtime_counts`
- `runtime_bytes`
- `runtime_totals`
- `top_streams`

Where implemented:

- `src/app_core.ts:613-804` builds the payload and route
- `src/app.ts:205-290` provides runtime byte/count sources

Important distinctions:

- `process` is raw `process.memoryUsage()` output
- `runtime_bytes` is the server's grouped attribution model:
  - `heap_estimates`
  - `mapped_files`
  - `disk_caches`
  - `configured_budgets`
  - `pipeline_buffers`
  - `sqlite_runtime`
- `runtime_counts` contains cardinalities or slot counts rather than bytes
- `runtime_totals` rolls up the grouped byte sections without including `counts`
- `counters` is the leak-candidate series map used for both this endpoint and
  the metrics stream

Operational use:

- start with `process.rss_bytes`
- compare `process_breakdown.unattributed_rss_bytes`
- compare `runtime_totals` and `runtime_bytes`
- inspect `sqlite` for allocator growth
- inspect `top_streams` when the issue may be stream-local rather than process-global

This endpoint intentionally remains read-only and cheap enough for repeated
polling from diagnostics UIs.
