# Releasing Prisma Streams

This repository prepares two publishable npm packages under `dist/npm/`:

- `@prisma/streams-local`
- `@prisma/streams-server`

`@prisma/streams-local` is the Node/Bun local runtime intended for `@prisma/dev`
and other trusted local workflows.

`@prisma/streams-server` is the Bun-only self-hosted server package and CLI.

## Release Checklist

1. Run repository verification:

```bash
bun run verify
bun run test:conformance
bun run test:conformance:local
```

2. Run the package-level smoke tests:

```bash
bun run test:node-local-package
bun run test:bun-server-package
```

These tests build the generated package directories, pack them, install them
into temporary consumers, and verify:

- Node end-to-end usage of `@prisma/streams-local`
- Bun CLI startup for `@prisma/streams-server`

3. Build the publishable package directories:

```bash
bun run build:npm-packages
```

This produces:

- `dist/README.md`
- `dist/local/*.js`
- `dist/touch/interpreter_worker.js`
- `dist/types/local/*.d.ts`
- `dist/npm/streams-local/**`
- `dist/npm/streams-server/**`

4. Inspect the package contents:

```bash
npm pack --dry-run ./dist/npm/streams-local
npm pack --dry-run ./dist/npm/streams-server
```

5. Publish the packages:

```bash
npm publish --access public ./dist/npm/streams-local
npm publish --access public ./dist/npm/streams-server
```

Or use the repository release workflow after pushing to `main`:

```bash
gh workflow run release.yml
```

The GitHub workflow builds, validates, and publishes both packages with npm
trusted publishing and provenance enabled.

## Build Notes

The release pipeline is intentionally split:

- `scripts/build-local-node.mjs` generates the Node-compatible local runtime
  artifacts in `dist/`
- `scripts/build-npm-packages.mjs` assembles the publishable package
  directories in `dist/npm/`
- `@prisma/streams-local` publishes only generated local runtime artifacts,
  local API declarations, runtime dependencies, and package docs
- `@prisma/streams-server` publishes a Bun CLI wrapper plus the Bun-oriented
  source runtime needed by the full server

For `@prisma/streams-local`, the build intentionally:

- emits shared chunks under `dist/local/` so `index.js` and `daemon.js` do not
  each embed their own copy of the runtime
- keeps npm dependencies external instead of rebundling them into the local
  package tarball

## Why The Split Exists

`@prisma/dev` should not depend on the full Bun server package when it only
needs the local runtime.

The split gives you:

- `@prisma/streams-local` for Node and Bun local embedding
- `@prisma/streams-server` for `bunx` and Bun-based self-hosting

## Current Packaging Contract

- `@prisma/streams-local` supports Bun and Node
- `@prisma/streams-local/internal/daemon` is intentionally internal
- `@prisma/streams-server` is Bun-only
- the root repository package is still private and is not the publish target
