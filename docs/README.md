# Prisma Streams Docs

This folder contains the protocol, runtime, and release documentation for
Prisma Streams. For how to run and use the server, see the root `README.md`.

Key files:

- `docs/local-dev.md` — how to run full server vs local dev server
- `docs/releasing.md` — how to build and release `@prisma/streams-local` and `@prisma/streams-server`
- `docs/DURABLE_STREAMS_SPEC.md` — protocol specification (canonical)
- `docs/SQLITE_SCHEMA.md` — SQLite schema requirements
- `docs/DURABLE_LENS_V1_SCHEMA.md` — lens schema reference
- `docs/CONFORMANCE.md` — how to run the upstream conformance suite
- `docs/SCHEMAS.md` — schema + lens behavior in this implementation
- `docs/RECOVERY_INTEGRITY_RUNBOOK.md` — recovery procedures and correctness guarantees
- `docs/BETTER_RESULT_ADOPTION.md` — mandatory `better-result` development standard and migration plan

Development policy note:
- `better-result` is mandatory for fallible development paths in this repository. See `docs/BETTER_RESULT_ADOPTION.md` for scope, exceptions, and rollout phases.
