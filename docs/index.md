# Prisma Streams Documentation

This directory is the canonical home for project documentation. Documentation
filenames use lowercase kebab-case so links stay predictable across platforms
and tooling.

## Start Here

- [overview.md](./overview.md) — product overview, quick start, package surfaces, and the main
  documentation map
- [live.md](./live.md) — end-to-end guide for the live / touch system and the `/touch/*`
  APIs
- [local-dev.md](./local-dev.md) — local development server behavior and Prisma CLI integration
- [auth.md](./auth.md) — authentication and authorization constraints
- [security.md](./security.md) — security reporting policy and deployment posture

## Runtime And Protocol

- [durable-streams-spec.md](./durable-streams-spec.md) — canonical HTTP protocol reference for this
  implementation
- [stream-profiles.md](./stream-profiles.md) — stream/profile/schema model and profile subresource
- [profile-generic.md](./profile-generic.md) — reference for the baseline `generic` profile
- [profile-metrics.md](./profile-metrics.md) — reference for the built-in `metrics` profile
- [profile-state-protocol.md](./profile-state-protocol.md) — reference for the `state-protocol` profile
- [profile-evlog.md](./profile-evlog.md) — design and reference for the `evlog` profile
- [schemas.md](./schemas.md) — schema registry and lens behavior
- [durable-lens-v1-schema.md](./durable-lens-v1-schema.md) — reference schema for `durable.lens/v1`
- [sqlite-schema.md](./sqlite-schema.md) — SQLite schema, invariants, and migration expectations
- [architecture.md](./architecture.md) — system architecture and data flow
- [tiered-index.md](./tiered-index.md) — tiered routing-key index design
- [indexing-architecture.md](./indexing-architecture.md) — current exact + `.col` + `.fts` search indexing model
- [storage-layout-architecture.md](./storage-layout-architecture.md) — `PSCIX2` bundled companion storage layout and per-family binary section codecs
- [bundled-companion-and-backfill.md](./bundled-companion-and-backfill.md) — bundled `.cix` companions and async backfill for existing streams
- [low-latency-reads-under-ingest.md](./low-latency-reads-under-ingest.md) — future architecture for stable `/_search` and `/_aggregate` latency under heavy ingest
- [ui-search-integration.md](./ui-search-integration.md) — how to build a filtered, chronologically ordered stream UI with `/_search` and `/_details`
- [aggregation-rollups.md](./aggregation-rollups.md) — `.agg` rollup family and aggregation query model
- [alternative-metrics-approach.md](./alternative-metrics-approach.md) — comparison of Axiom MetricsDB with the current Prisma Streams metrics design
- [metrics.md](./metrics.md) — shipped metrics profile, canonical metrics stream shape, and query architecture
- [gharchive-demo.md](./gharchive-demo.md) — self-contained GH Archive demo stream with search fields and Studio-friendly rollups
- [daily-ingest-report-with-more-fts.md](./daily-ingest-report-with-more-fts.md) — completed `gharchive-demo day` ingest report with `title`, `message`, and `body` back on the `.fts` path
- [week-ingest-report.md](./week-ingest-report.md) — completed `gharchive-demo week` ingest report against R2-backed full mode

## Operations

- [operational-notes.md](./operational-notes.md) — tuning knobs and stall diagnosis
- [memory-assumption.md](./memory-assumption.md) — ranked working assumptions for explaining RSS growth from the current memory observability surfaces
- [recovery-integrity-runbook.md](./recovery-integrity-runbook.md) — recovery steps and correctness checks
- [segment-performance.md](./segment-performance.md) — segment read-path performance notes
- [routing-key-performance.md](./routing-key-performance.md) — routing-key performance status and pointers
- [live-load-tests.md](./live-load-tests.md) — black-box load tests for the live / touch system
- [memory-observability-mmap-pinned-caches.md](./memory-observability/memory-observability-mmap-pinned-caches.md) — leak-candidate counters for pinned mmap caches
- [memory-observability-server-mem-endpoint.md](./memory-observability/memory-observability-server-mem-endpoint.md) — `GET /v1/server/_mem` payload, runtime byte groups, and top-stream views
- [memory-observability-process-breakdown.md](./memory-observability/memory-observability-process-breakdown.md) — anon/file/shmem RSS attribution and unattributed memory counters
- [memory-observability-sqlite-runtime.md](./memory-observability/memory-observability-sqlite-runtime.md) — SQLite allocator/runtime counters and current limitations
- [memory-observability-ingest-pipeline-buffers.md](./memory-observability/memory-observability-ingest-pipeline-buffers.md) — live segmenter/uploader buffer attribution
- [memory-observability-gc-high-water.md](./memory-observability/memory-observability-gc-high-water.md) — forced GC effectiveness and high-water marks with timestamps
- [memory-observability-top-stream-contributors.md](./memory-observability/memory-observability-top-stream-contributors.md) — per-stream contributor summaries for local storage, WAL, touch, and notifier state
- [memory-observability-touch-journal-lifecycle.md](./memory-observability/memory-observability-touch-journal-lifecycle.md) — leak-candidate counters for active touch journals
- [memory-observability-touch-journal-default-footprint.md](./memory-observability/memory-observability-touch-journal-default-footprint.md) — leak-candidate counters for touch journal filter footprint
- [memory-observability-state-protocol-journal-creation.md](./memory-observability/memory-observability-state-protocol-journal-creation.md) — leak-candidate counters for journal creation churn
- [memory-observability-touch-manager-stream-maps.md](./memory-observability/memory-observability-touch-manager-stream-maps.md) — leak-candidate counters for touch manager stream maps
- [memory-observability-live-template-registry-maps.md](./memory-observability/memory-observability-live-template-registry-maps.md) — leak-candidate counters for template registry memory maps
- [memory-observability-live-metrics-counters-map.md](./memory-observability/memory-observability-live-metrics-counters-map.md) — leak-candidate counters for live metrics map cardinality
- [memory-observability-stream-notifier-version-maps.md](./memory-observability/memory-observability-stream-notifier-version-maps.md) — leak-candidate counters for notifier version maps
- [memory-observability-metrics-series-cardinality.md](./memory-observability/memory-observability-metrics-series-cardinality.md) — leak-candidate counters for internal metrics series cardinality
- [memory-observability-secondary-index-idle-map.md](./memory-observability/memory-observability-secondary-index-idle-map.md) — leak-candidate counters for secondary index idle map cardinality
- [memory-observability-local-mock-r2-memory.md](./memory-observability/memory-observability-local-mock-r2-memory.md) — leak-candidate counters for local MockR2 in-memory usage

## Development And Release

- [contributing.md](./contributing.md) — contribution workflow and expectations
- [code-of-conduct.md](./code-of-conduct.md) — community participation policy
- [conformance.md](./conformance.md) — upstream conformance suite status and commands
- [releasing.md](./releasing.md) — npm package release process
- [better-result-adoption.md](./better-result-adoption.md) — `better-result` policy and migration history
- [assumptions.md](./assumptions.md) — protocol assumptions that must remain covered by tests
- [pitfalls-and-guardrails.md](./pitfalls-and-guardrails.md) — implementation guardrails for high-risk areas
- [prisma-dev-pglite-live.md](./prisma-dev-pglite-live.md) — Prisma local Postgres embedding guidance

Repository policy note:
- `better-result` is mandatory for fallible development paths in this
  repository. See [better-result-adoption.md](./better-result-adoption.md) for
  scope, exceptions, and rollout phases.
