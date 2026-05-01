# Changelog

## Upcoming

- Require an explicit full-server auth mode, with `--auth-strategy api-key`
  enforcing bearer-token authentication on every production Streams and Compute
  demo request.
- Add package-based Prisma Compute deployment guidance and publish the
  `@prisma/streams-server/compute` entrypoint for npm consumers.
- Improve Streams Live exact invalidation coverage with membership-only and
  projected-field fine keys for cheap exact query shapes.
- Add settled-cursor barriers and an exact small-key wait lane so small live
  subscriptions can avoid more conservative reruns and key-match false
  positives.
- Add a live SQL invalidation matrix documenting which query families can be
  result-exact versus only template- or table-coarse.
- Align Prisma WAL/state-protocol events with the published spec by using
  `old_value`, validating change/control message shapes on append, and
  accepting control messages without generating touch invalidations.
- Use `bun pm pack` in package smoke tests and release docs so release
  validation works with the repository's Bun package-manager pin.
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
- Add `stream.total_size_bytes` to `/_details` as a constant-time logical size
  lookup, with manifest restore and background repair after bootstrap when
  needed.
- Add schema-owned `search.rollups`, object-store-native `.agg` companions,
  and `POST /v1/stream/{name}/_aggregate` for rollup-backed time-range
  summaries with raw-scan edge correctness.
- Add a built-in `metrics` profile, auto-wire `__stream_metrics__` to it, and
  ship object-store-native `.mblk` companions so metrics queries can use
  rollups first and metrics blocks before raw segment scans.
