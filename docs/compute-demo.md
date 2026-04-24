# Compute Demo Deployment

This document describes the supported Prisma Compute deployment that layers two
browser entrypoints on top of the normal Streams server:

- `/studio` serves the Prisma Studio streams-only UI against the colocated
  Streams server.
- `/generate` serves a write generator with `1k`, `10k`, and `100k` buttons.
  The page includes a stream-name field that defaults to `demo-app`. Each run
  ensures that selected `application/json` stream has the `evlog` profile and
  appends canonical evlog records in server-side chunks with live progress
  polling.

The normal Streams HTTP surface remains available on `/v1/*`.

By default, this entrypoint can still run a colocated Streams app. For the
Studio/generator demo, prefer external-streams mode by setting
`COMPUTE_DEMO_STREAMS_SERVER_URL`. In that mode, the deployment does not create
or use a local Streams store; it proxies Studio and generator writes to the
configured Streams server.

## Entrypoint

Use [`src/compute/demo_entry.ts`](../src/compute/demo_entry.ts) for this
deployment shape. It:

- applies the normal Compute argv defaults from
  [`src/compute/entry.ts`](../src/compute/entry.ts), including `--object-store r2`
  and `--auto-tune` when `DS_MEMORY_LIMIT_MB` is set
- starts the regular Streams app in-process
- fronts it with the `/studio`, `/studio/api/streams/*`, `/generate`, and
  `/api/generate/jobs*` routes
- defaults the generator stream field to `demo-app`

When `COMPUTE_DEMO_STREAMS_SERVER_URL` or `STREAMS_SERVER_URL` is set, the
entrypoint skips the colocated Streams app and proxies all Streams requests to
that external server instead.

## Bundle Build

Studio assets are not part of this repository, so the Compute bundle must be
prebuilt with a local Studio checkout available.

Default Studio root discovery:

- `PRISMA_STUDIO_ROOT`, if set
- otherwise the sibling repository path `../studio`

Build the bundle:

```bash
bun run build:compute-demo-bundle
```

That script:

- prebuilds the Studio demo client and CSS
- injects them into the bundle through a virtual module
- bundles `src/compute/demo_entry.ts`
- bundles the Streams worker entrypoints required on Compute

## Deploy

Example Compute deploy:

```bash
PRISMA_API_TOKEN=... \
  bunx @prisma/compute-cli deploy \
    --service your-service-id \
    --skip-build \
    --path .compute-demo-build/bundle \
    --entrypoint compute/demo_entry.js
```

Required runtime env is the same as the normal R2-backed Compute server:

- `DS_HOST=0.0.0.0`
- `DS_ROOT=/mnt/app/prisma-streams`
- `DS_MEMORY_LIMIT_MB=1024`
- `DURABLE_STREAMS_R2_BUCKET`
- `DURABLE_STREAMS_R2_ACCOUNT_ID`
- `DURABLE_STREAMS_R2_ACCESS_KEY_ID`
- `DURABLE_STREAMS_R2_SECRET_ACCESS_KEY`

On Prisma Compute, keep `DS_ROOT` under `/mnt/app`. Paths such as `/tmp` are
ephemeral and lose the colocated Streams SQLite state on restart.

External-streams demo mode only needs the HTTP bind settings plus the target
Streams server:

```bash
DS_HOST=0.0.0.0 \
COMPUTE_DEMO_STREAMS_SERVER_URL=https://cmoa45nql0u6bzycn7dwdpxe0.cdg.prisma.build \
  bun run src/compute/demo_entry.ts
```

## Studio Integration Shape

The Studio browser app still reads config from `/api/config`, but that config
points its Streams integration at `/studio/api/streams`.

That proxy path forwards to the configured Streams target, so Studio is always
inspecting the same server that `/generate` writes to.
