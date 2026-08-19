# Streams platform emulator + reference gateway

The executable half of [docs/CONTROL-PLANE-INTEGRATION.md](../docs/CONTROL-PLANE-INTEGRATION.md):
a dependency-light Node implementation of the platform side of the
Streams contract — Management API, one-time credential secrets,
Prisma-Auth token exchange (`POST /v1/token/streams`), RS256 signing
with a full JWKS kid lifecycle, per-cell full-snapshot feeds with
**atomic file projection**, project placement across cells, ownership
transfer + deletion sagas, workload-JWT rotation, a verifying reverse
proxy (the reference gateway), and a minimal console page.

Both components are **independent producers**: they share only the
JSON schemas and OpenAPI starter in
[contracts/streams-platform/v1](../contracts/streams-platform/v1) with
the Rust cell, never serialization code, so producer and consumer
cannot agree on the same bug. The cells consume the emulator's files
and tokens unmodified.

```bash
# platform (two cells + fixtures)
node platform-demo/src/emulator.mjs \
  --port 9700 \
  --cells cell-a=/tmp/feeds/a,cell-b=/tmp/feeds/b \
  --fixture proj-a:ws-a:cell-a --fixture proj-b:ws-b:cell-b

# gateway (verify BEFORE routing; placement from /admin/placement)
node platform-demo/src/gateway.mjs --port 9710 \
  --emulator http://127.0.0.1:9700 \
  --cell cell-a=http://127.0.0.1:9702 --cell cell-b=http://127.0.0.1:9704

# console: http://127.0.0.1:9700/?project=proj-a
```

The end-to-end battery (`node scripts/platform-e2e.mjs`) starts the
emulator, the gateway, and THREE REAL `streams-slate` cells under the
full release posture (`STREAMS_AUTH_MODE=enforce`,
`FLEET_AUTH_MODE=workload`, `STREAMS_RELEASE_POSTURE=1`, no static
fleet token) and proves the §14.5 scenario families — 43 checks:

- **Credential lifecycle** (through the gateway): secret-shown-once,
  wrong-secret refusal, exchange, SDK create/append/read via
  `tokenProvider`, rotation invalidating the old grant after feed
  publication, revocation refusing exchange, suspension cutting off a
  live token.
- **Placement + gateway**: JWT verified before any routing; a forged
  path project cannot select a cell; direct calls to the wrong cell
  answer `421 wrong_cell` (preserved, never converted to 401); the
  same stream name on two projects/cells stays isolated.
- **JWKS lifecycle**: an unknown kid triggers an immediate feed
  refresh (proven against a 100s cadence — the cell's own ceiling,
  refresh ≤ a third of the 300s staleness window); retirement is
  permanent; retired-kid tokens die everywhere.
- **Transfer saga (§10)**: no new token during `transfer_pending`;
  old-ownership tokens fail after publication; the new workspace's
  replacement credential reads the SAME stream (storage identity
  never changes).
- **Deletion saga (§11)**: authorization cutoff first, then the
  project leaves every feed (omission tombstone).
- **Feed faults** (`--enable-fault-api`, deliberate §7.2 violations):
  torn live-path writes never become visible; generation regression,
  same-generation drift, and retired-kid resurrection are all refused
  while the previously accepted snapshot keeps serving.
- **Fleet identity**: operation-scoped workload JWTs (empty operations
  grant nothing), customer tokens refused on the internal surface,
  per-cell atomic workload rotation.

V1 notes (deliberate scope):
- plain ESM JavaScript for a zero-build first version; the TypeScript
  split in the integration doc §14.1 remains the target shape;
- state is in-memory (hashed secrets only) — restarts reset the world;
- the `freeze` fault exists for manual staleness experiments; the fast
  battery relies on the cell's own DST coverage for the 300s
  fail-closed staleness gate rather than waiting it out;
- schema validation of published snapshots is manual for now (golden
  vectors under `contracts/streams-platform/v1/golden/` are captured
  from real emulator output); a runtime validator is a follow-up.
