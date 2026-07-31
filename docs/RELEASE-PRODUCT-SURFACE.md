# Prisma Streams product surface — release gate report

Date: 2026-07-31 · Branch: `slate` · Spec:
`handover/prisma_streams_surface_spec_prelaunch_hard_cutover/`
(appendix §13 defines this gate).

## Verdict

Every gate the appendix defines for the pre-launch hard cutover is
**pass** (or an explicit documented posture where the spec item does
not apply to this deliverable). The eight implementation workstreams,
the conformance appendix, and the final battery are complete.

## Pinned baseline

| pin | value |
|---|---|
| `DURABLE_STREAMS_PROTOCOL_COMMIT` | `npm:@durable-streams/server-conformance-tests@0.3.6` (the executable baseline; the package publishes no gitHead) |
| `DURABLE_STREAMS_SERVER_CONFORMANCE_VERSION` | `0.3.6` |
| `DURABLE_STREAMS_CLIENT_CONFORMANCE_VERSION` | `0.2.12` |

Recorded in `src/protocol_pin.rs`; `pinned()` passes.

## Gate results

| gate | result | evidence |
|---|---|---|
| Pinned DS server suite (338 tests) | **332 pass / 0 fail / 6 suite-skips** | `CONFORMANCE.md` 2026-07-31 runs; s3lite + fresh bucket + `--conformance-default-key` |
| Pinned DS client conformance | **posture: N/A** | This release ships a server plus the Prisma product SDK. No raw Durable Streams protocol *client library* is part of the deliverable, which is what the client suite exercises. If a raw client ships later, this gate re-opens. |
| Prisma product conformance | **pass** | 178-test rust suite (product create/append/read/subscribe/scan/producers/consumers/watches/seal/catalog + error schema + token classes) |
| Dual-surface equivalence | **pass** | the 12-case corpus (`dual_surface_equivalence_corpus`), both orders; case 6 found and fixed a real gap (raw close now seals the descriptor) |
| Removed surfaces absent | **pass** | greps clean: no profile fields/branches, no `/queue/*`, `/touch/*`, no covering-index writers, no legacy per-key layouts; removed product inputs are 400 `unknown_field` |
| Fresh-namespace destructive cutover | **pass** | `decode_desc` is the single chokepoint and refuses any `layout_version != 3` (unit test `layout_gate_refuses_foreign_namespaces`); a live server over a namespace with a planted pre-cutover descriptor never surfaces it |
| DST safety + liveness | **pass** | the 178-suite's DST corpus (I1 across tiers, boundary races, seal gaps, split/merge, consumer/DLQ crash matrix, producer retries) |
| Postings COGS gates | **posture: carried** | the storage write path (absorber/postings/planner) is unchanged since the closed cost campaign — gates and measured results in `docs/ROUTING-V3.md` §11 (stored bytes ≤55 %, postings overhead 4.09 %/0.41 %, Class A parity, ≥1.8× split capacity) |
| Consumer/watch cost invariants | **pass** | consumer/watch state rides the shard commit path; zero new DBs, manifests, namespaces, or LIST loops (per-consumer state = rows under the stream identity) |
| Wide-cardinality memory | **posture: carried** | handle/counter/sketch caps unchanged since the w100k campaigns; the usage map's past-cap fail-open re-verified by the (now isolated) cap test |
| Real Compute + Tigris field campaign | **pass, teardown verified** | see below |

## Field campaign (2026-07-31)

Fresh project (`streams-prodsurf-gate`), fresh Tigris buckets
(artifacts + data — a genuinely fresh namespace, matching the cutover
posture), x86_64-musl build (`cargo zigbuild`, ELF machine verified),
deployed to Prisma Compute us-east-1 with the soak7-tuned environment.

- `sdk/smoke.mjs` against the deployed URL: **14/14** — create
  idempotence, producer append/batch/duplicate/reuse-conflict, keyed
  read, subscribe catch-up + live wake over the WAN, snapshot scan,
  consumer pull (per-key FIFO) + settle, watch definitions,
  seal-with-final, catalog paging, sealed refusal.
- Raw-surface spot check: create 201 with initial body,
  read-your-write, fork with a binary sub-offset (201, inherited
  partial record reads back through real Tigris).
- One anomaly noted, not reproducible on fresh names: the very first
  spot-check PUT answered 200 with an empty stream — consistent with
  an edge retry/replay of the create; every fresh-name sequence
  afterwards behaved exactly to contract.
- Teardown: service destroyed (`services list` empty), both buckets
  deleted (204), project deleted (204); URL answers 404, zero
  campaign buckets remain, project GET 404.

## The final surface (summary)

Raw standards route `/v1/stream/{name}` (the pinned protocol,
including forks, TTL sliding, producer coordination, SSE/long-poll,
lifecycle) and the Prisma product route `/v1/streams/{name}`
(routing-key appends/reads with signed cursors, snapshot scans,
consumer groups, watches, typed creation, seal/delete/catalog) over
ONE descriptor and ONE storage engine. The TypeScript SDK
(`sdk/src/index.ts`) is the product client. "Durable Streams
compliant" is claimed for the singular route only, as tested by the
pinned suite; the plural route is a separate product API.

## Notes for the operator

- The standalone server's default `--max-unflushed-bytes` (16 MiB)
  is rejected against the default `--l0-sst-size-bytes` (32 MiB) by
  slatedb's validation; deployments already override both. Local runs:
  `--max-unflushed-bytes 67108864`.
- The conformance runner's npx CLI mis-globs its own test file; run it
  through a one-line vitest wrapper (see `CONFORMANCE.md`).
