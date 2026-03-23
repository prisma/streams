# Prisma Streams Local Development Server

The local development server is the package surface intended for `npx prisma dev` and other trusted local workflows.

It is different from the full self-hosted server:

- single SQLite database per named server
- no segmenting
- no object-store uploads
- same HTTP API shape for development use
- optimized for loopback and embedded tooling, not hostile-network deployment

## Supported Package Surface

Supported:

- `@prisma/streams-local` exporting `startLocalDurableStreamsServer` and its server types

Internal:

- `@prisma/streams-local/internal/daemon`

The daemon export exists for Prisma CLI integration and may change more freely than the public `local` entrypoint.

## CLI Commands

```bash
bun run src/local/cli.ts start --name default --port 8080
bun run src/local/cli.ts status --name default
bun run src/local/cli.ts stop --name default
bun run src/local/cli.ts reset --name default
```

## Storage Layout

Default root:

- `<envPaths("prisma-dev").data>/durable-streams/`

Per server name:

- `<root>/<name>/durable-streams.sqlite`
- `<root>/<name>/server.lock`
- `<root>/<name>/server.json`

SQLite may also create `durable-streams.sqlite-wal` and `durable-streams.sqlite-shm` while the database is open.

Override the root when needed:

```bash
DS_LOCAL_DATA_ROOT=/tmp/my-ds-local bun run src/local/cli.ts start --name default
```

## Programmatic API

The published `@prisma/streams-local` package surface is built for both Bun and
Node consumers.

```ts
import { startLocalDurableStreamsServer } from "@prisma/streams-local";

const server = await startLocalDurableStreamsServer({
  name: "default",
  port: 0,
  hostname: "127.0.0.1",
});

console.log(server.exports.http.url);
console.log(server.exports.sqlite.path);

await server.close();
```

## Daemon Integration

The internal daemon entrypoint is:

- `@prisma/streams-local/internal/daemon`

Example:

```ts
import { fork } from "node:child_process";

const child = fork(require.resolve("@prisma/streams-local/internal/daemon"), [
  "--name",
  "default",
  "--port",
  "0",
], {
  stdio: "inherit",
});

child.on("message", (msg) => {
  if (!msg || msg.type !== "ready") return;
  // msg.exports: { name, pid, http: { url, port }, sqlite: { path } }
});
```

This is the intended integration model for `npx prisma dev`, but it should be treated as Prisma CLI plumbing rather than a general public daemon API.

## Operational Model For `npx prisma dev`

Expected behavior:

- reuse a healthy named server when possible
- keep server state under `envPaths("prisma-dev").data`
- record PID and endpoint information in `server.json`
- use `reset` only after the server is stopped

## Validation

Useful commands:

```bash
bun run build:npm-packages
bun test test/local_server.test.ts
bun run test:conformance:local
bun run test:node-local-package
```

See [conformance.md](./conformance.md) for the current upstream suite status.
