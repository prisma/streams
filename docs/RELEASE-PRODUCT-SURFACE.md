# Prisma Streams product surface — release gate report

Date: 2026-07-31 · Branch: `slate` · Spec:
`handover/prisma_streams_surface_spec_prelaunch_hard_cutover/`
(appendix §13 defines this gate).

## Verdict

**One gate is outstanding.** Everything the appendix defines passes,
carries an explicit posture, or — for the post-audit cloud re-run — is
blocked on a platform condition that is not ours to fix and has not
been re-verified. Details in "Cloud gate" below. Until that run lands,
this release is not fully gated, and the report says so rather than
rounding up.

Three words are used deliberately and never interchangeably:

- **pass** — the gate ran and met its criteria, at a stated time.
- **posture** — the spec item does not apply to this deliverable, with
  the reason recorded and the condition that would re-open it.
- **outstanding** — the gate has not been met yet. Not a pass.

## Pinned baseline

| pin | value |
|---|---|
| `DURABLE_STREAMS_PROTOCOL_COMMIT` | `npm:@durable-streams/server-conformance-tests@0.3.6` (the executable baseline; the package publishes no gitHead) |
| `DURABLE_STREAMS_SERVER_CONFORMANCE_VERSION` | `0.3.6` |
| `DURABLE_STREAMS_CLIENT_CONFORMANCE_VERSION` | `0.2.12` |

Recorded in `src/protocol_pin.rs` and `conformance/package.json`
(locked); `pinned()` passes.

## Gate results

| gate | result | evidence |
|---|---|---|
| Pinned DS server suite (338 tests) | **pass** — 332 passed / 0 failed / 6 skipped, re-run after the audit response | `conformance/` runner + `expected.json` gate; `CONFORMANCE.md` |
| Pinned DS client conformance | **posture: N/A** | this release ships a server plus the Prisma product SDK; no raw Durable Streams protocol *client library* is part of the deliverable, which is what the client suite exercises. Re-opens if a raw client ships. |
| Reserved webhook-subscription API | **posture: not implemented** | the 6 suite skips, named in `CONFORMANCE.md`. The product consumer API covers the need. Re-opens if we implement `__ds/subscriptions/*`. |
| Prisma product conformance | **pass** | 188-test rust suite (create/append/read/subscribe/scan/producers/consumers/watches/seal/catalog/lifecycle + error schema + token classes + the audit regressions) |
| Dual-surface equivalence | **pass** | the 12-case corpus, both orders; case 6 found and fixed a real gap (raw close now seals the descriptor) |
| Removed surfaces absent | **pass** | greps clean: no profile fields/branches, no `/queue/*`, `/touch/*`, no covering-index writers, no legacy per-key layouts; removed inputs answer 400 `unknown_field` |
| Fresh-namespace destructive cutover | **pass** | `decode_desc` is the single chokepoint and refuses any `layout_version != 3` (`layout_gate_refuses_foreign_namespaces`) |
| DST safety + liveness | **pass** | the suite's DST corpus (I1 across tiers, boundary races, seal gaps, split/merge, consumer/DLQ crash matrix, producer retries) |
| Postings COGS gates | **posture: carried** | the storage write path is unchanged since the closed cost campaign — gates and measured results in `docs/ROUTING-V3.md` §11 |
| Consumer/watch cost invariants | **pass** | consumer/watch state rides the shard commit path; zero new DBs, manifests, namespaces or LIST loops |
| Wide-cardinality memory | **posture: carried** | handle/counter/sketch caps unchanged since the w100k campaigns; the usage map's past-cap fail-open re-verified by the isolated cap test |
| Real Compute + Tigris field campaign (pre-audit build) | **pass, teardown verified** | 2026-07-31, `51969ad` — see below |
| Cloud gate re-run on the post-audit build | **OUTSTANDING — blocked** | four deployments, two projects, two PoPs: all boot and listen, none routable. See below. |

## Cloud gate

### What ran, and passed, before the audit response

Fresh project, fresh Tigris buckets, x86_64-musl build, deployed to
Prisma Compute: `sdk/smoke.mjs` 14/14 over the WAN (create idempotence,
producer append/batch/duplicate/reuse-conflict, keyed read, subscribe
catch-up and live wake, snapshot scan, consumer pull and settle, watch
definitions, seal-with-final, catalog paging, sealed refusal), plus a
raw-surface spot check including a fork with a binary sub-offset
reading back through real Tigris. Teardown verified: service destroyed,
both buckets deleted, project deleted, URL 404.

One anomaly was noted in that run and **is now understood**: the first
spot-check PUT answered 200 over an empty stream. That was a real
non-atomic create, it is fixed (durable create initialization), and it
is pinned by `create_replay_never_loses_the_initial_body`.

### What the audit asked for next, and why it has not run

The audit requires re-running the cloud gate on the post-audit build
with negative auth, create replay, seal crash, fork crash, and split
coexistence. That harness now exists — `scripts/field-gate.mjs`, 20
checks — and it passes end to end against a local deployment,
**including driving a real scaler split** and verifying that every
routing key still reads back its own sequence while the raw route shows
the default key's records and nothing else.

It has not run against Compute. Four deployments were made today
(2026-07-31, 11:51–13:15 UTC) and none became reachable:

| # | project | region/PoP | version status | domain |
|---|---|---|---|---|
| 1 | API-created | us-east-1 / ewr | running | 404 |
| 2 | API-created (fresh service) | us-east-1 / ewr | running | 404 |
| 3 | API-created (fresh service) | ap-southeast-1 / sin | running | 404 |
| 4 | CLI-created project | us-east-1 / ewr | running | 404 |

The binary is not the cause, and neither is the wrapper. Deployment
logs show the full healthy boot every time:

```
binary streams-slate-515e350 size 23261744
assembled 23261744 bytes e_machine=62      # 0x3e = x86_64, as required
starting streams-slate on :8080
streams-slate listening on 0.0.0.0:8080
```

Both the service domain and the per-version preview domain answer the
platform's own "There is no service on this URL" page, in two PoPs and
two projects, one of which was created through the Compute CLI rather
than the management API. A service deployed on 2026-07-24 still answers
`/health` with 200 through the same edge. So the platform is up, and
newly created services are not being registered at it.

That is a platform-side condition. Everything provisioned for the
attempt has been destroyed and verified gone (4 services, 2 buckets,
2 projects; project GETs 404, zero `audit-gate` buckets remain).

**To close this gate** when new services route again:

```bash
node scripts/field-gate.mjs "$URL"   # STREAMS_TOKEN + STREAMS_KEY set
node sdk/smoke.mjs "$URL"
```

Deploy with `SCALE_EVAL_SECS=5 SCALE_RATE_WINDOW_SECS=10
SCALE_HOT_PCT=1 SCALE_HOT_EVALS=1 SCALE_COOLDOWN_SECS=5` so the split
case is deterministic inside a timebox.

## What the audit response changed

| audit finding | outcome |
|---|---|
| product API did not enforce the bearer token | enforced, with a signed-watch-URL exception; negative-auth tests local and in the field gate |
| create was not atomic (the field anomaly) | durable create initialization; the anomaly is now a regression test |
| fork create/delete not crash-safe | resumable fork lifecycle, identified references, source epochs enforced, splits refused on fork chains |
| sealing not atomic | Open→Sealing→Sealed state machine; every append consults it; resumable across a crash |
| singular route not the default-key stream | raw route is the default-key view before and after splits; `Stream-Key` removed from it |
| catalog capped at 10,000 with per-page GET storms | order-preserving descriptor paths, page-local GETs, opaque signed cursor |
| SDK package not consumable | built, packed, installed from the tarball, smoke-tested on Node 18 and current Node in CI |
| watches and cross-stream DLQ | client-side watch derivation, verifier persisted in the descriptor, DLQ link validated at config time and permanent refusals surfaced |
| route parsing and SDK lifecycle | subresources matched as suffixes; verbs restricted; abort signal plumbed; producer chains bounded; `ephemeral` given meaning |
| CI ran a runner known to be broken | runner committed with a locked pin and an exact expected outcome |

Packing the SDK found a real server bug on the way (seal-with-final
built a NUL-delimited producer id and passed it as a header, where
control bytes are illegal — the insert failed silently and the seal
refused its own final record). Running the field gate locally found
another (a `routingKey` query parameter on an append was ignored, so
records went to the default key and looked like success). Both are
fixed and pinned by tests.

## The final surface (summary)

Raw standards route `/v1/stream/{name}` (the pinned protocol, including
forks, TTL sliding, producer coordination, SSE/long-poll, lifecycle)
and the Prisma product route `/v1/streams/{name}` (routing-key
appends/reads with signed cursors, snapshot scans, consumer groups,
watches, typed creation, seal/delete/catalog) over ONE descriptor and
ONE storage engine. The TypeScript SDK (`sdk/`) is the product client.
"Durable Streams compliant" is claimed for the singular route only, as
tested by the pinned suite; the plural route is a separate product API.

## Notes for the operator

- The standalone server's default `--max-unflushed-bytes` (16 MiB) is
  rejected against the default `--l0-sst-size-bytes` (32 MiB) by
  slatedb's validation; deployments already override both. Local runs:
  `--max-unflushed-bytes 67108864`.
- Conformance runs through `conformance/`, never the package's npx CLI
  (its include glob misses its own runner and exits 0 having run
  nothing).
