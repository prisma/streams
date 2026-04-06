# Prisma Streams Overview

Prisma Streams is a Bun + TypeScript implementation of the Durable Streams HTTP protocol.

Use [index.md](./index.md) for the full documentation map.

Every stream has a profile.

- If no profile is declared when the stream is created, it is treated as
  `generic`.
- `evlog` is the built-in request-log profile for canonical wide events,
  redaction, `requestId`/`traceId` routing-key defaults, and automatic default
  schema/search/rollup installation.
- `generic` means a plain durable stream with optional user-managed schema
  validation.
- `metrics` is the built-in metrics profile for canonical interval summaries,
  default search/rollups, and object-store-native metrics companions.
- `state-protocol` is the built-in live/touch profile for JSON State Protocol
  streams.
- Profiles define stream semantics; schemas define payload shape.

See [stream-profiles.md](./stream-profiles.md).

This repository currently contains two server modes:

- `full` mode: a self-hosted server with SQLite WAL storage, segmenting, upload, and index maintenance
- `local` mode: an embedded single-SQLite server intended for trusted local development workflows, especially `npx prisma dev`

## Current Durability Model

Full mode today has two different durability points:

- append ACK means the write is durable in local SQLite
- object-store durability happens only after segment upload plus manifest
  publication

`--bootstrap-from-r2` rebuilds published stream history and metadata from
object storage. It does not restore transient local SQLite state
such as the unuploaded WAL tail, producer dedupe state, or runtime live/template
state.

A stream becomes recoverable from object storage after its first manifest is
published.

Not implemented today:

- an object-store-acked mode that would only ACK after persistence to R2
- a cluster quorum mode that would only ACK after a storage quorum accepts the
  write

## Status

- The publishable npm surfaces are intentionally split:
  - `@prisma/streams-local` exports `startLocalDurableStreamsServer` and its server types
  - `@prisma/streams-local/internal/daemon` exists for Prisma CLI integration and is intentionally internal
  - `@prisma/streams-server` is the Bun-only full server package and CLI

See [conformance.md](./conformance.md) for current compatibility status,
verification commands, and known gaps.

## Security

Prisma Streams does **not** currently implement built-in authentication or authorization.

- Do not expose the server directly to the public internet.
- Put the full server behind a trusted reverse proxy, API gateway, VPN boundary, or other authenticated perimeter.
- Treat the local development server as a loopback-only tool for trusted local workflows.

See [security.md](./security.md) and [auth.md](./auth.md).

## Prerequisites

- Bun `>=1.3.11`
- Node.js `>=22` for local embedding and Node-based consumers

## Quick Start

```bash
bun install

# Full server (self-hosted pipeline)
bun run src/server.ts --object-store local

# Local development server
bun run src/local/cli.ts start --name default --port 8080

# Status / stop / reset
bun run src/local/cli.ts status --name default
bun run src/local/cli.ts stop --name default
bun run src/local/cli.ts reset --name default
```

Notes:

- Full server startup requires `--object-store local|r2`.
- Full mode binds to `127.0.0.1` by default. Set `DS_HOST=0.0.0.0` if you intentionally want a non-loopback bind inside a trusted network boundary.
- Local mode is designed for development and Prisma CLI integration, not hostile-network deployment.
- The default local data root remains under `envPaths("prisma-dev").data/durable-streams/` for compatibility with the Prisma development workflow.

## Local Integration API

The supported package import for local development integration is:

```ts
import { startLocalDurableStreamsServer } from "@prisma/streams-local";

const server = await startLocalDurableStreamsServer({
  name: "default",
  hostname: "127.0.0.1",
  port: 0,
});

console.log(server.exports.http.url);
console.log(server.exports.sqlite.path);

await server.close();
```

The published `@prisma/streams-local` surface is built to run on both Bun and
Node. The full self-hosted server remains Bun-only.

The local embedded runtime always applies the built-in `1024 MB` auto-tune
preset. That keeps the local package on the current control-plane and endpoint
surface while giving Prisma CLI a predictable cache and concurrency budget
without extra configuration.

The package smoke tests cover the local Live path under both host runtimes:

- `bun run test:node-local-package`
- `bun run test:bun-local-package`

`@prisma/streams-local/internal/daemon` is exported for Prisma CLI integration, but it is intentionally internal and does not carry the same compatibility guarantee as `@prisma/streams-local`.

More detail is in [local-dev.md](./local-dev.md).

## Full Server

The full server is started via:

```bash
bun run src/server.ts --object-store local
```

Published CLI package:

```bash
bunx --package @prisma/streams-server prisma-streams-server --object-store local
```

Bind control:

```bash
DS_HOST=127.0.0.1 PORT=8080 bun run src/server.ts --object-store local
```

Optional flags:

- `--stats`
- `--hist`
- `--bootstrap-from-r2`
- `--auto-tune[=MB]`

### Object Store Configuration

Local MockR2:

```bash
bun run src/server.ts --object-store local
```

Real R2:

```bash
DURABLE_STREAMS_R2_BUCKET=your-bucket \
DURABLE_STREAMS_R2_ACCOUNT_ID=your-account-id \
DURABLE_STREAMS_R2_ACCESS_KEY_ID=your-access-key \
DURABLE_STREAMS_R2_SECRET_ACCESS_KEY=your-secret \
  bun run src/server.ts --object-store r2
```

## Development Commands

```bash
bun run typecheck
bun run check:result-policy
bun test
bun run test:conformance:local
bun run test:conformance
```

## Documentation

- [index.md](./index.md): full documentation index
- [local-dev.md](./local-dev.md): local server behavior and Prisma CLI integration model
- [releasing.md](./releasing.md): build and release process for `@prisma/streams-local` and `@prisma/streams-server`
- [prisma-dev-pglite-live.md](./prisma-dev-pglite-live.md): integrating local Prisma Postgres (`prisma dev`) with Prisma Streams live queries
- [conformance.md](./conformance.md): test commands and current upstream suite status
- [auth.md](./auth.md): current authentication and authorization constraints
- [architecture.md](./architecture.md): system architecture
- [stream-profiles.md](./stream-profiles.md): stream/profile/schema model
- [profile-generic.md](./profile-generic.md): `generic` profile reference
- [profile-metrics.md](./profile-metrics.md): `metrics` profile reference
- [profile-state-protocol.md](./profile-state-protocol.md): `state-protocol`
  profile reference
- [profile-evlog.md](./profile-evlog.md): `evlog` profile reference
- [indexing-architecture.md](./indexing-architecture.md): current exact +
  `.col` + `.fts` + `.agg` + `.mblk` indexing model
- [aggregation-rollups.md](./aggregation-rollups.md): schema-owned rollup and
  `_aggregate` model
- [sqlite-schema.md](./sqlite-schema.md): SQLite schema and invariants
- [schemas.md](./schemas.md): schema registry and lens behavior
- [live.md](./live.md): end-to-end live / touch integration guide and API semantics
- [metrics.md](./metrics.md): shipped metrics profile and metrics query model
- [gharchive-demo.md](./gharchive-demo.md): self-contained GH Archive demo for ingestion, search, and aggregates
- [recovery-integrity-runbook.md](./recovery-integrity-runbook.md): recovery and operational runbook

## Open Source Baseline

This repository now includes:

- [LICENSE](../LICENSE)
- [security.md](./security.md)
- [contributing.md](./contributing.md)
- [code-of-conduct.md](./code-of-conduct.md)
