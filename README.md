# prisma-streams

Monorepo for two npm packages:

- `@prisma/streams-local`
- `@prisma/streams-server`

Both packages are intentionally minimal stubs so the names can be published now and implemented later.

## Packages

- `packages/streams-local`: public stub package for the local Prisma Streams runtime.
- `packages/streams-server`: public stub package for the server Prisma Streams runtime.

## Scripts

- `pnpm install`: install workspace dependencies.
- `pnpm build`: build both publishable packages.
- `pnpm check:exports`: validate package export maps after build.
- `pnpm typecheck`: run TypeScript checks for both packages.
- `pnpm release`: build and publish both packages to npm.

## Publishing

Each package declares `publishConfig.access: "public"` and starts at version `0.0.1`.

Before publishing, make sure the active npm account has permission to publish the `@prisma` scoped packages.

## Attribution

This repository was originally scaffolded from the `jkomyno/pnpm-monorepo-template` template.
