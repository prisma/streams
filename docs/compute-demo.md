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
  and `--auto-tune` when `DS_MEMORY_LIMIT_MB` is set, before the colocated
  Streams server config is loaded
- requires the same explicit auth mode as the normal full server:
  `--auth-strategy api-key` with `API_KEY`, or `--no-auth`
- starts the regular Streams app in-process
- fronts it with the `/studio`, `/studio/api/streams/*`, `/generate`, and
  `/api/generate/jobs*` routes
- defaults the generator stream field to `demo-app`

When `COMPUTE_DEMO_STREAMS_SERVER_URL` or `STREAMS_SERVER_URL` is set, the
entrypoint skips the colocated Streams app and proxies all Streams requests to
that external server instead.

For the colocated 1 GiB Compute demo, the `1024 MiB` auto-tune preset keeps
segmenting in the main process, seals smaller 8 MiB / 50k-row segments,
disables the segment disk cache, uses one upload lane, and limits
bundled-companion work to one segment / one yield block per pass. During a
generate job, the colocated demo pauses segmenter and indexer loops, appends
directly to the in-process ingest queue instead of serializing batches through
Bun's `Request` body path, then resumes background cutting and indexing for the
generated stream. That avoids overlapping the generator, append path, segment
cutting, uploading, and companion building too aggressively on Compute hosts
whose effective RSS headroom is lower than the nominal memory preset.

When the generator runs on a separate server and writes over HTTP, the Streams
server cannot use the colocated direct-append pause hooks. The low-memory
server preset still keeps append responses non-keep-alive and defers immediate
index/companion enqueue wakeups until the server has had a short
foreground-quiet window, with a bounded deferral cap so a continuous trickle of
writes cannot starve background catch-up. Uploaded segments therefore avoid
starting companion backfill while the external generator is still bursting
writes, but continue to make progress after the burst or during low-rate ingest.

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
- `API_KEY` when using `--auth-strategy api-key`
- `DURABLE_STREAMS_R2_BUCKET`
- `DURABLE_STREAMS_R2_ACCOUNT_ID`
- `DURABLE_STREAMS_R2_ACCESS_KEY_ID`
- `DURABLE_STREAMS_R2_SECRET_ACCESS_KEY`

On Prisma Compute, keep `DS_ROOT` under `/mnt/app`. Paths such as `/tmp` are
ephemeral and lose the colocated Streams SQLite state on restart.

External-streams demo mode only needs the HTTP bind settings plus the target
Streams server and an explicit auth mode:

```bash
DS_HOST=0.0.0.0 \
API_KEY=replace-with-at-least-10-characters \
COMPUTE_DEMO_STREAMS_SERVER_URL=https://cmoa45nql0u6bzycn7dwdpxe0.cdg.prisma.build \
  bun run src/compute/demo_entry.ts --auth-strategy api-key
```

## Studio Integration Shape

The Studio browser app still reads config from `/api/config`, but that config
points its Streams integration at `/studio/api/streams`.

That proxy path forwards to the configured Streams target, so Studio is always
inspecting the same server that `/generate` writes to.
