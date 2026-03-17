# Conformance Tests

This repository uses two layers of protocol verification:

1. Local black-box tests in `test/conformance.test.ts`
2. The upstream suite from `@durable-streams/server-conformance-tests`

## Commands

```bash
bun test test/conformance.test.ts
bun run test:conformance
bun run test:conformance:local
```

Default behavior:

- `test:conformance` starts a temporary full server on `127.0.0.1:8787`
- `test:conformance:local` starts a temporary local-mode server on `127.0.0.1:8787`

Manual target mode:

```bash
DS_HOST=127.0.0.1 PORT=8787 DS_ROOT=/tmp/ds-conformance bun run src/server.ts --object-store local
CONFORMANCE_TEST_URL=http://127.0.0.1:8787 bun run test:conformance
CONFORMANCE_TEST_URL=http://127.0.0.1:8787 bun run test:conformance:local
```

## Current Status

Last verified on `2026-03-14`:

- Local repository suite: `bun test` passed
- Upstream full-server suite: `239/239` passing
- Upstream local-mode suite: `239/239` passing

Prisma Streams currently passes the upstream black-box suite for both server modes.

## Notes

- The upstream suite is black-box and only drives HTTP endpoints.
- The conformance runners remain valuable as regression detectors and should continue to run in CI or release verification.
