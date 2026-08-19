# Streams platform emulator

The executable half of [docs/CONTROL-PLANE-INTEGRATION.md](../docs/CONTROL-PLANE-INTEGRATION.md):
a dependency-light Node implementation of the platform side of the
Streams contract — Management API, one-time credential secrets,
Prisma-Auth token exchange (`POST /v1/token/streams`), RS256 signing,
full-snapshot JWKS/policy/grant feeds with **atomic file projection**,
workload-JWT rotation, and a minimal console page.

It is an **independent producer**: it shares only the JSON schemas in
[contracts/streams-platform/v1](../contracts/streams-platform/v1) with
the Rust cell, never serialization code, so producer and consumer
cannot agree on the same bug. The cell consumes its files and tokens
unmodified.

```bash
node platform-demo/src/emulator.mjs \
  --port 9700 --cell local-cell \
  --feed-dir /tmp/feeds --workload-file /tmp/feeds/workload.jwt \
  --fixture proj-demo:ws-demo
# console: http://127.0.0.1:9700/?project=proj-demo
```

The end-to-end battery (`node scripts/platform-e2e.mjs`) starts this
emulator plus a REAL `streams-slate` under the full release posture
(`STREAMS_AUTH_MODE=enforce`, `FLEET_AUTH_MODE=workload`,
`STREAMS_RELEASE_POSTURE=1`, no static fleet token) and proves the
Phase-A scenarios: secret-shown-once, wrong-secret refusal, exchange,
SDK create/append/read through `tokenProvider`, rotation invalidating
the old grant after feed publication, revocation refusing exchange,
suspension cutting off a live token, and atomic workload rotation.

V1 notes (deliberate scope):
- plain ESM JavaScript for a zero-build first version; the TypeScript
  split in the integration doc §14.1 remains the target shape;
- state is in-memory (hashed secrets only) — restarts reset the world;
- schema validation of published snapshots is manual for now (golden
  vectors under `contracts/streams-platform/v1/golden/` are captured
  from real emulator output); a runtime validator lands with the
  two-cell phase.
