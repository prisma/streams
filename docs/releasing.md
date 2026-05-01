# Releasing Prisma Streams

This repository prepares two publishable npm packages under `dist/npm/`:

- `@prisma/streams-local`
- `@prisma/streams-server`

`@prisma/streams-local` is the Node/Bun local runtime intended for `@prisma/dev`
and other trusted local workflows.

`@prisma/streams-server` is the Bun-only self-hosted server package and CLI.

## Release Checklist

Release branch policy:

- Always cut and publish releases from `main` only.
- Do not run `release.yml` against feature branches or temporary release
  branches.
- Merge the release changes into `main` first, then dispatch the release
  workflow from `main`.

0. Ensure npm trusted publishing is configured for both packages:

- `@prisma/streams-local`
- `@prisma/streams-server`

For each package on npmjs.com, add a GitHub Actions trusted publisher with:

- Organization or user: `prisma`
- Repository: `streams`
- Workflow filename: `release.yml`

The workflow cannot publish new versions until both packages trust this
repository workflow.

1. Run repository verification:

```bash
bun run verify
bun run test:conformance
bun run test:conformance:local
```

2. Run the package-level smoke tests:

```bash
bun run test:node-local-package
bun run test:bun-local-package
bun run test:bun-server-package
```

These tests build the generated package directories, pack them, install them
into temporary consumers, and verify:

- Node end-to-end usage of `@prisma/streams-local`
- Bun end-to-end usage of `@prisma/streams-local` on Bun `1.2.x` and newer, including the live `/touch/*` path
- stateful local-runtime reopen flows that must read `/_schema` and skip
  duplicate first-schema installs when the registry already matches
- local package exposure of `GET /v1/server/_details` and `GET /v1/stream/{name}/_routing_keys`
- Bun CLI startup for `@prisma/streams-server`
- package exposure of the `@prisma/streams-server/compute` Compute entrypoint

3. Build the publishable package directories:

```bash
bun run build:npm-packages
```

This produces:

- `dist/README.md`
- `dist/local/*.js`
- `dist/touch/processor_worker.js`
- `dist/types/local/*.d.ts`
- `dist/npm/streams-local/**`
- `dist/npm/streams-server/**`

4. Inspect the package contents:

```bash
(cd ./dist/npm/streams-local && bun pm pack --dry-run)
(cd ./dist/npm/streams-server && bun pm pack --dry-run)
```

5. Publish the packages:

```bash
npm publish --access public ./dist/npm/streams-local
npm publish --access public ./dist/npm/streams-server
```

Or use the repository release workflow from `main` only:

```bash
gh workflow run release.yml --ref main
```

The GitHub workflow builds, validates, and publishes both packages with npm
trusted publishing and provenance enabled.

The workflow currently runs the full repository validation on `ubuntu-latest`
and package smoke checks on macOS. This avoids a Bun teardown crash seen on the
macOS GitHub Actions runner while still verifying that the publishable package
shapes work on macOS before release.

## Build Notes

The release pipeline is intentionally split:

- `scripts/build-local-node.mjs` generates the local runtime artifacts in `dist/`
- `scripts/build-npm-packages.mjs` assembles the publishable package
  directories in `dist/npm/`
- `@prisma/streams-local` publishes only generated local runtime artifacts,
  local API declarations, runtime dependencies, and package docs
- `@prisma/streams-server` publishes a Bun CLI wrapper plus the Bun-oriented
  source runtime needed by the full server

For `@prisma/streams-local`, the build intentionally:

- emits shared chunks under `dist/local/` so `index.js` and `daemon.js` do not
  each embed their own copy of the runtime
- keeps the local runtime Bun-compatible even though the generated bundle
  targets the Node module surface
- publishes a local-package Bun engine floor of `>=1.2.0` while keeping the
  full server on the repository Bun floor
- pins the embedded local runtime to the built-in `1024 MB` auto-tune preset so
  Prisma CLI gets a predictable cache and concurrency budget
- keeps npm dependencies external instead of rebundling them into the local
  package tarball
- publishes only the runtime dependency subset the local package actually
  imports, rather than copying the repository-wide dependency list into
  `@prisma/streams-local`

## Why The Split Exists

`@prisma/dev` should not depend on the full Bun server package when it only
needs the local runtime.

The split gives you:

- `@prisma/streams-local` for Node and Bun local embedding
- `@prisma/streams-server` for `bunx` and Bun-based self-hosting

## Current Packaging Contract

- `@prisma/streams-local` supports Bun `>=1.2.0` and Node `>=22`
- `@prisma/streams-local/internal/daemon` is intentionally internal
- `@prisma/streams-server` is Bun-only
- `@prisma/streams-server/compute` starts the Compute server entrypoint from a
  package consumer and injects the Compute object-store and auto-tune defaults
- the root repository package is still private and is not the publish target
