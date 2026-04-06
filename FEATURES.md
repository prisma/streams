# Features

## Stream Profiles

- Every stream has a profile. If no profile is declared when the stream is
  created, it is treated as the built-in `generic` profile and profile metadata
  is managed via `/_profile`.
- `state-protocol` is implemented as a real profile, so live/touch streams are
  configured through `/_profile` instead of the schema registry.
- `evlog` is implemented as a real profile, so request-log streams normalize
  JSON writes into canonical wide events with profile-owned redaction and
  routing-key defaults.
- Installing `evlog` also auto-installs the canonical schema version `1` and
  default `search` registry, so `_search` and `filter=` work without a
  separate manual schema setup step.
- `metrics` is implemented as a real profile, so metrics streams normalize
  JSON writes into canonical interval summaries with profile-owned
  schema/search and rollup installation.
- The internal `__stream_metrics__` stream is auto-created with the `metrics`
  profile and is immediately queryable through `_search` and `_aggregate`.
- The public profile API uses a single `profile` field in requests and
  responses.
- State-protocol touch processing uses profile-aligned `processor` and
  `processed_through` naming across runtime metadata, metrics, and packaging.

## Search And Indexing

- Schemas declare searchable fields under top-level `search`, including stable
  field IDs, per-version bindings, aliases, and capabilities such as `exact`,
  `prefix`, `column`, `exists`, and `sortable`.
- Full mode builds five search/indexing layers from that schema:
  - the internal exact-match secondary index family
  - `.col` per-segment companions for typed equality/range
  - `.fts` per-segment companions for keyword exact/prefix and text search
  - `.agg` per-segment companions for schema-owned rollups
  - `.mblk` per-segment companions for metrics-profile aggregate serving
- `POST /v1/stream/{name}/_search` and `GET /v1/stream/{name}/_search?q=...`
  are implemented for fielded exact/prefix/range/text queries with
  search-after pagination.
- `POST /v1/stream/{name}/_aggregate` is implemented for schema-owned
  time-window rollups with aligned-window `.agg` usage, `.mblk` metrics
  fallback, and raw-scan fallback for partial edges and uncovered ranges.
- The main `GET /v1/stream/{name}` path supports `filter=` for schema
  `search.fields` and still covers the local unsealed tail with a bounded
  100 MB scan cap per response.
- Published exact, `.col`, `.fts`, `.agg`, and `.mblk` state survives manifest
  publication and bootstrap-from-R2 recovery.
- `GET /v1/stream/{name}/_index_status` exposes per-stream segment, manifest,
  routing-index, exact-index, and search-family progress for UIs and
  diagnostics.
- `GET /v1/stream/{name}/_details` combines the current stream summary, full
  profile resource, full schema registry, nested index status, and
  `stream.total_size_bytes` in one call.
