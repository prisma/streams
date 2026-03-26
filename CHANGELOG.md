# Changelog

## Upcoming

- Add stream profiles with built-in `generic` and `state-protocol` support,
  including a simplified `profile`-based `/_profile` API for live/touch setup.
- Rename state-protocol touch processing metrics and runtime state to
  profile-aligned `processor` / `processed_through` terminology.
- Add an `evlog` profile that normalizes JSON writes into canonical wide events
  with pre-append redaction and `requestId`/`traceId` routing-key defaults.
- Auto-install the canonical evlog schema and search registry when the `evlog`
  profile is enabled, so evlog streams are query-ready without a separate
  manual `/_schema` step.
- Replace public schema `indexes[]` with schema-owned `search` fields and add
  object-store-native `.col` and `.fts` companion families alongside the exact
  secondary index accelerator.
- Add `_search` on JSON streams, with fielded exact/prefix/range/text queries,
  search-after pagination, manifest/bootstrap recovery, and local tail
  correctness.
- Add `filter=` support on the main JSON stream read path for schema
  `search.fields`, with exact/column pruning, local tail coverage, and a
  100 MB scan-cap header.
- Add `/_index_status` and `/_details` so stream-management UIs can inspect
  per-stream indexing progress together with current stream, schema, and
  profile state.
- Add schema-owned `search.rollups`, object-store-native `.agg` companions,
  and `POST /v1/stream/{name}/_aggregate` for rollup-backed time-range
  summaries with raw-scan edge correctness.
