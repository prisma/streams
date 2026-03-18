# Prisma Streams

Prisma Streams is a Bun + TypeScript implementation of the Durable Streams HTTP protocol.

This repository currently contains two server modes:

- `full` mode: a self-hosted server with SQLite WAL storage, segmenting, upload, and index maintenance
- `local` mode: an embedded single-SQLite server intended for trusted local development workflows, especially `npx prisma dev`

## Status

- `bun test` currently passes on this repository.
- The upstream black-box conformance suites currently pass for both full and local modes.
- The publishable npm surfaces are intentionally split:
  - `@prisma/streams-local` exports `startLocalDurableStreamsServer` and its server types
  - `@prisma/streams-local/internal/daemon` exists for Prisma CLI integration and is intentionally internal
  - `@prisma/streams-server` is the Bun-only full server package and CLI

See [docs/CONFORMANCE.md](./docs/CONFORMANCE.md) for the current compatibility status and known gaps.

## Security

Prisma Streams does **not** currently implement built-in authentication or authorization.

- Do not expose the server directly to the public internet.
- Put the full server behind a trusted reverse proxy, API gateway, VPN boundary, or other authenticated perimeter.
- Treat the local development server as a loopback-only tool for trusted local workflows.

See [SECURITY.md](./SECURITY.md) and [docs/AUTH.md](./docs/AUTH.md).

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

`@prisma/streams-local/internal/daemon` is exported for Prisma CLI integration, but it is intentionally internal and does not carry the same compatibility guarantee as `@prisma/streams-local`.

More detail is in [docs/local-dev.md](./docs/local-dev.md).

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

- [docs/local-dev.md](./docs/local-dev.md): local server behavior and Prisma CLI integration model
- [docs/releasing.md](./docs/releasing.md): build and release process for `@prisma/streams-local` and `@prisma/streams-server`
- [docs/prisma-dev-pglite-live.md](./docs/prisma-dev-pglite-live.md): integrating local Prisma Postgres (`prisma dev`) with Prisma Streams live queries
- [docs/CONFORMANCE.md](./docs/CONFORMANCE.md): test commands and current upstream suite status
- [docs/AUTH.md](./docs/AUTH.md): current authentication and authorization constraints
- [ARCHITECTURE.md](./ARCHITECTURE.md): system architecture
- [docs/SQLITE_SCHEMA.md](./docs/SQLITE_SCHEMA.md): SQLite schema and invariants
- [docs/SCHEMAS.md](./docs/SCHEMAS.md): schema registry and lens behavior
- [LIVE.md](./LIVE.md): Live Query V2 and touch semantics
- [docs/METRICS.md](./docs/METRICS.md): emitted metrics
- [docs/RECOVERY_INTEGRITY_RUNBOOK.md](./docs/RECOVERY_INTEGRITY_RUNBOOK.md): recovery and operational runbook

## Open Source Baseline

This repository now includes:

- [LICENSE](./LICENSE)
- [SECURITY.md](./SECURITY.md)
- [CONTRIBUTING.md](./CONTRIBUTING.md)
- [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md)
