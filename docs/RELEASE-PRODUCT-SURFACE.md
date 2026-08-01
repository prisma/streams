# Prisma Streams product surface — release gate report

Date: 2026-07-31 · Branch: `slate` · Spec:
`handover/prisma_streams_surface_spec_prelaunch_hard_cutover/`
(appendix §13 defines this gate).

## Verdict

**One gate is outstanding.** Everything the appendix defines passes,
carries an explicit posture, or — for the post-audit cloud re-run — is
blocked on a platform condition that is not ours to fix and has not
been re-verified. Six audit rounds have been answered in full; each is summarized
below, and each is a reason to read an earlier round's "pass" claims as
of that date rather than as a standing verdict. Details in "Cloud gate" below. Until that run lands,
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

This was then isolated with a control: a **fifteen-line hello-world bun
app** — no Rust binary, no downloader, no environment — deployed the
same way. It boots (`hello app listening on 0.0.0.0:8080`) and its
domain 404s exactly like ours, in a brand-new project AND in an old
project whose own services still answer 200. So the cause is not this
repo, not the wrapper, not the project, and not the PoP.

Three further observations narrow it to edge publication:

- **DNS is fine.** The failing and working hostnames resolve to the
  same edge IPs. The failing one gets a canned 404 HTML page *from the
  edge*, with an `etag` — the edge is answering, it just has no backend
  for that hostname.
- **It is not scale-to-zero.** Streaming a failing service's logs live
  while pinging it produced zero new lines across five requests. A
  sleeping instance would boot and log; these requests never reach the
  machine at all.
- **The platform sets no `PORT`.** The environment inside the container
  is `HOME,PATH,TERM`; 8080 is convention, and both the hello app and
  streams-slate bind it. Not a port mismatch.

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

**Diagnosis: newly created services are not being published to the
edge router.** The control plane accepts the deploy and reports
`running`, the machine boots and listens, and the edge never learns the
hostname. Every service deployed before this started still routes, and
a deploy from earlier the same day (the pre-audit field campaign) ran a
full 14/14 smoke over the WAN — so this began during 2026-07-31, before
10:51 UTC.

That is a platform-side condition. Everything provisioned for the
attempt and for the isolation has been destroyed and verified gone
(6 services, 2 buckets, 3 projects; project GETs 404, zero `audit-gate`
or `hello-probe` resources remain, and the pre-existing services we
probed are untouched and still answer 200).

Re-tested after a platform incident was reported resolved
(2026-07-31 ~13:30–13:55Z): still failing. Hello-world controls 404 in
both PoPs, and a fresh streams-slate deployment stayed 404 through a
deliberately patient 16-minute poll while its boot log showed the
normal `listening on 0.0.0.0:8080`. All retry resources destroyed and
verified gone. Full evidence: [`repro-edge-404/`](../repro-edge-404/).

**To close this gate** when new services route again:

```bash
node scripts/field-gate.mjs "$URL"   # STREAMS_TOKEN + STREAMS_KEY set
node sdk/smoke.mjs "$URL"
```

Deploy with `SCALE_EVAL_SECS=5 SCALE_RATE_WINDOW_SECS=10
SCALE_HOT_PCT=1 SCALE_HOT_EVALS=1 SCALE_COOLDOWN_SECS=5` so the split
case is deterministic inside a timebox.

## Second audit round (2026-07-31)

A follow-up audit of the first response found that "all ten items are
addressed" was not supported by the code: an account-authentication
bypass, creation that could still expose or corrupt incomplete
resources, seal-with-final that could lose its record, producer writes
passing through sealing, fork references that could leak durably, and
raw fork reads that violated default-key isolation. It was right on
every count. What changed:

| finding | fix |
|---|---|
| Signed-watch exception selected by substring, before route parsing — `acme/watches/x/keys/y/extra` and its `/records` skipped the bearer token | requests are classified into an exact `ProductRoute` first; only `WatchWait` with a signature self-authorizes |
| Up to 32 MiB buffered before authentication | the gate runs on path/method/query/headers, before the body is read |
| CORS on preflights only — the browser passed OPTIONS then blocked the answer | every plural-route response carries CORS and exposes the product's own headers |
| `/v1/segments` unauthenticated | takes the account token |
| `initializing()` expired with the claim, so an abandoned create served as complete after 15 s | readiness is `init.is_none()`; claim staleness only decides who may REDO the work |
| A replay under a different key could resume an initialization and write content the descriptor's fingerprint cannot decrypt | both resume paths check the key; `InitState` records the fingerprint it was claimed under |
| Half-built collections appeared in metadata and the catalog | excluded from both |
| `SealState` did not record what the seal owed, so a plain `:seal` could finish a final-bearing one and drop the record | `SealIntent::Final` with a `final_committed` flag set durably before any segment closes; only the owner may complete it |
| New producer sequences accepted during Sealing/Sealed | the refusal rides on the request and the committer applies it after duplicate detection |
| Raw close sealed the collection after closing its segment, and swallowed the failure | the intent is published first; a failed seal answers 503 and stays resumable |
| Splits/merges not fenced by sealing | both CAS closures refuse sealed or sealing descriptors |
| Raw fork reads returned every routing key | stitched reads and sub-offset materialization filter to the empty key |
| Fork reference installed without checking the source's epoch/lifecycle/topology, and a refused install was ignored | the CAS validates all of it and a refusal answers 409 `fork_source_changed` |
| Fork deletion not resumable — a crash after the tombstone leaked the parent's reference forever | the debt is recorded before the tombstone and any later delete settles it |
| Catalog: an underfull page read as end-of-catalog; expired/initializing not filtered; transient errors skipped silently | pagination continues while the PROVIDER has more; those states are filtered; a vanished descriptor is skipped and any other failure fails the page |
| `Stream.subscribe()` never passed its AbortSignal to fetch | it does, and the retry sleeps are abortable |
| Watch derivation used bare `atob`, breaking URL-safe keys | keys are normalized before decoding |
| SDK claimed Bun/Deno/browsers with only Node gated | Bun and Deno now run the same installed-package smoke in CI; browsers are documented as expected, not verified |
| DLQ target identified by name only | the configured incarnation is pinned and checked at delivery |

The catalog cursor is opaque (base64url of the continuation name), not
signed — an earlier version of this report said signed, which was
wrong. It carries no authority: the catalog is bearer-authenticated on
every request.

## Third audit round (2026-08-01)

A third audit found that round 2's lifecycle records were durable but
some multi-step transitions still were not linearizable or resumable
across every durable boundary. Correct again. What changed:

| finding | fix |
|---|---|
| Splits/merges fenced only at phase A; phase-B publication could add live children under a Sealed collection | both phase-B CAS closures refuse sealed/sealing, and a seal resolves any pending transition before snapshotting live segments and refuses to publish Sealed while one exists |
| Raw close-with-content published `SealIntent::Empty`, so a crash could seal without the promised records | it publishes a Final intent keyed by the request's own content hash and marks the record durable before closing |
| Lifecycle intents published before validation — a 400 left the collection sealing forever | raw close and product seal both validate every deterministic error first |
| `begin_sealing_for_close` ignored its CAS result, so a losing racer continued on a stale descriptor | the CAS outcome is classified: ours, joinable, conflicting, or retry |
| `run_seal` could close segments for a matching op id while the final was still uncommitted | closing is refused while a final is owed, by anyone |
| `{"final": null}` silently sealed with no record | presence-aware parsing (`Option<Option<Value>>`) |
| `seal_op_id` concatenated record and routing key — `{1,"23"}` and `{12,"3"}` collided | versioned, length-delimited envelope |
| Fork initialization identity omitted the source epoch, so a retry against a recreated source resumed the old claim | the epoch is in the hash, resumes re-check parentage, and the empty-epoch wildcard is gone |
| The recursive delete cascade recorded no debt for intermediate generations | the debt is written in the same update that tombstones them |
| Catalog continuation advanced only on a successful GET, so an all-vanished page read as end-of-catalog | the continuation follows the provider's key |
| Producer-backed final seal ran outside the producer chain | it rides the same per-routing-key queue as appends |
| `Access-Control-Allow-Headers: *` does not authorize Authorization | all three preflight handlers list the header names |
| Retry backoff in the shared request helper was not abortable | it uses the abort-aware sleep |

Two scope corrections the same audit asked for, both now stated where
they belong:

- **The field gate is a replay-and-coexistence harness, not a crash
  test.** Its "interrupted" cases issue duplicate requests over a real
  network; nothing kills the server. Crash recovery at durable
  boundaries is covered by failpoint tests in the rust suite. The
  script says so at the top.
- **The post-split capacity gate is a mechanism check.** Best-of-three
  windows suit a shared, noisy host and do not constitute performance
  evidence; a fleet capacity claim needs isolated instances, paired
  steady-state windows, medians and a lower confidence bound.

## Fourth audit round (2026-08-01)

A fourth audit found that round 3's transitions were durable
individually but still not linearizable across their full lifecycle.
Correct again.

| finding | fix |
|---|---|
| A seal intent could install OVER a pending split — phase B then refuses to publish, and the collection is stuck sealing with work that never clears | the seal-intent CAS is the serialization point: it installs only over an open, unclaimed, topologically quiet descriptor, resolving the transition first |
| `run_seal` ignored its CAS result and reported `{"sealed":true}` over a still-Sealing descriptor | success is proven by a fresh read (sealed set, sealing clear, no pending) or it is an error |
| `enter_sealing`/`mark_final_committed` ignored declined CAS | every outcome is classified (`EnterSeal`) |
| `x-seal-final` was an external header that authorized a write into a sealing collection, and no ordinary client sends it — so a crashed raw close could not be resumed by a retry | the owed final is identified by the request's own content hash and routing key, or a trusted internal parameter; the header is refused from the wire |
| Seal intents published before routing-key/producer/capacity validation, so an impossible final left the collection sealing forever | every deterministic refusal is decided first, on both surfaces |
| Fork deletion decided soft-versus-hard from a stale read, so a concurrent first fork could be orphaned | removal, inspection, tombstone and debt happen in ONE CAS |

Three round-3 tests were also weaker than their comments claimed, which
the audit was right to call out: the fork-incarnation test planted an
arbitrary request hash (it would have conflicted whether or not the
epoch mattered), the cascade test hand-built the post-crash state, and
the raw-close test never paused after the intent. They now use the real
hash, a real failpoint inside the production cascade, and a resume
through the public route.

## Fifth audit round (2026-08-01)

| finding | fix |
|---|---|
| A raw final close could not be resumed once its records were durable: no producer identity meant the retry hit the committer's closed-stream check and was refused forever | such a close carries a synthetic identity derived from its own operation id, so the retry is recognised as a duplicate first. The identity is internal — the wire response is unchanged (the first attempt leaked it and broke close-with-final-append in the DS suite) |
| A definitively refused final left its intent behind, bricking the collection | a 4xx that is not 429/408 is the committer's verdict on that request, so the seal abandons its own uncommitted intent; transient outcomes keep it |
| Capacity pre-checks were inexact: value length rather than wire bytes, no record count, and a disabled limit read as zero capacity | one shared predicate — exact wire bytes, record count, and zero means disabled |
| A crashed cascade could only be repaired by deleting a hidden intermediate name | a delete against a tombstone walks the ancestor chain and settles every unpaid debt, so retrying the ORIGINAL delete works |
| A fork initialization was refused against a source retained FOR IT, leaving the child permanently Initializing | the already-installed reference is checked before the liveness guard; the child's own fork-id CAS is classified |
| The fork/delete race test was timing-nudged | deterministic: the delete is parked before its decision, the fork install completes, then the delete is released — verified red against the pre-read decision |

## Sixth audit round (2026-08-01)

| finding | fix |
|---|---|
| A definitive committer verdict on a RAW close left its intent behind (the product route already cleaned up; the raw one did not) | the raw path abandons its own uncommitted intent on any definitive `AppendErr`; 429/timeout/internal keep it |
| The seal operation id covered only the record and routing key, so two attempts differing in producer coordination shared one identity — a refused attempt could tear down the intent a valid one was committing under | the identity is versioned v2 and covers the whole attempt: routing key, record bytes, and producer id/epoch/seq |
| Synthetic producer ids lived in the PUBLIC producer namespace, so a caller could pre-create `prisma.seal.<op>` and turn a later final append into a false duplicate — sealing without the record | internal identities use a reserved prefix the wire parser refuses on both routes |
| A successful final append was accepted without proving it closed the segment | the internal ack now carries whether THIS write closed; a non-closing duplicate is refused and the intent released |
| Readiness publication ignored `Ok(false)`, which `cas_update` returns for a deleted descriptor — creation answered 201 for a stream that no longer existed and left the fork source pinned | the outcome is classified, success requires a live ready descriptor at the same incarnation, and an unpublishable initialization gives its source reference back |
| `permanently_unadmittable` ignored the request bucket (0.1 req/s × 2 s < 1 token) | request capacity is checked too |
| A READY fork could not be re-PUT idempotently once its source was retained | the retained-source lookup accepts a matching child whether it is initializing or ready |
| The fork/delete race test was timing-assisted | superseded by a parked-delete handshake; the readiness race uses the same pattern |

## Seventh audit round (2026-08-01)

| finding | fix |
|---|---|
| A raw close derived its operation identity two ways — the whole semantic request when checking, content + routing key when publishing the intent — so an exact retry could not recognise its own intent and the collection answered `seal_incomplete` to the only request that could finish it | the intent's `request_hash` **is** the operation id, computed once by the request that owns it; the second derivation is deleted |
| A producer gap or stale epoch was treated as definitive and tore the intent down, losing the promised record whenever the missing predecessor was merely late — it may already be admitted inside the server | ordering verdicts keep the intent for the exact retry; only verdicts about the request itself (content-type mismatch, malformed body, sequence reuse) take it down |
| Retaining an intent lets an operation that is simply gone hold a collection Sealing forever | a claim older than `SEAL_CLAIM_MS` (15 s) is abandoned: another seal takes it over and REPLACES the intent. Recovery is a timeout, never a guess about whether a verdict was terminal |
| In-flight lifecycle decisions checked liveness, which cannot distinguish "still mine" from "deleted and recreated while I was parked" | every one is fenced by the incarnation it was issued against — create readiness, fork-id stamp, delete, seal claim, final-committed (`cas_update_incarnation` / `IncarnationCas`) |
| The crash-resume test planted a hand-built `SealState` instead of driving the code that writes one | a failpoint stops the close between its intent and its records; the ABA cases park real operations in real windows |

### The observable contract this settles

A close that carries content and is refused for **ordering** leaves
the collection `Sealing`, and ordinary appends are refused with 409
while that claim stands. This is deliberate: the collection is sealing,
and the exact retry must still be able to deliver the promised record.
It is bounded — after 15 s the claim is abandoned and any subsequent
seal takes it over — so no single bad request can hold a collection
open-but-unwritable indefinitely. A close refused for a reason that
retrying cannot change releases the intent immediately.

## What the first audit response changed

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
- A collection stuck in `Sealing` recovers by itself: retry the close
  that owns it (the exact same request finishes the transition), or
  wait 15 s and seal it — the abandoned claim is taken over. There is
  no operator intervention and no repair tool, by design.
