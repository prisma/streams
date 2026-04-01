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
- [recovery-integrity-runbook.md](./recovery-integrity-runbook.md) — recovery steps and correctness checks
- [segment-performance.md](./segment-performance.md) — segment read-path performance notes
- [routing-key-performance.md](./routing-key-performance.md) — routing-key performance status and pointers
- [live-load-tests.md](./live-load-tests.md) — black-box load tests for the live / touch system

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
