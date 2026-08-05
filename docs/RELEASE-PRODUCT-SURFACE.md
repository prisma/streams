# Prisma Streams product surface — release gate report

Date: 2026-07-31 · Branch: `slate` · Spec:
`handover/prisma_streams_surface_spec_prelaunch_hard_cutover/`
(appendix §13 defines this gate).

## Verdict

**PASS — the appendix-§13 battery is closed.** Every local gate passes
at the final head; the cloud field campaign passed against two real
Compute edges on build `d445a06`; the destructive-cutover refusals were
verified on the wire. Fifteen audit rounds have been answered in full;
each round's summary below is a statement about its own date, and this
verdict is the standing one.

The final state, in the reviewer's shape:

- **Local gates:** pass (suite, pinned DS conformance, field-gate
  corpus paced AND unpaced, SDK package gate), with the carried
  postures listed in the gate table.
- **Cloud campaign:** passed 2026-08-04 on build `d445a06` (fra +
  ewr; 20/20 field gate each; SDK WAN smoke; wire cutover refusals).
- **Final repository head:** the commit this file ships in (see the
  provenance block below — `server_commit` binds it exactly).
- **Post-campaign changes** (the campaign was NOT re-run on the WAN
  after these; each was re-gated by the full local battery, including
  the paced WAN-shape field gate):
  1. SlateDB dependency moved from our fork to pinned upstream main
     (`0717cc1e…`) — a real server change; suite, conformance, both
     field-gate modes, SDK smoke, and a GC LIST A/B were re-run green
     (see `docs/COST-CAMPAIGN-2.md` addendum).
  2. Round-15 fixes: transactional consumer deletion (fallible scans,
     same-group staged-row burial, in-group deleted-consumer refusal)
     and single-snapshot incarnation pinning through the fork-release
     path with fenced debt clearing — six new red-verified DST tests.
  3. Harness/report-only changes (field-gate pagination + pacing +
     setup-append checking, this report).
- **Services intentionally retained:** see "Retained field
  deployment" below (owner, purpose, expiry, credentials).

Three words are used deliberately and never interchangeably:

- **pass** — the gate ran and met its criteria, at a stated time.
- **posture** — the spec item does not apply to this deliverable, with
  the reason recorded and the condition that would re-open it.
- **outstanding** — the gate has not been met yet. Not a pass.

## Provenance

`scripts/release-provenance.sh` output for the exact artifact this
report describes is pasted at the end of this file and regenerated on
every report edit; if the pasted `server_commit` and `git rev-parse
HEAD` disagree, the report is stale and the script is right.

## Cloud field campaign (2026-08-04) — the last gate, closed

Prisma Compute resumed publishing new services on 2026-08-03 after the
five-day edge outage (`repro-edge-404/` retry log). The campaign then
ran end to end on build `d445a06` (x86_64-musl, provenance via
`scripts/release-provenance.sh`):

| step | result |
|---|---|
| two-PoP deploy (fra `eu-central-1`, ewr `us-east-1`), fresh `camp75a` namespace, split knobs on | both edges healthy |
| 20-check field gate against each REAL edge (negative auth, create replay, seal/fork replay-resume and coexistence, a real scaler split under WAN pacing, raw default-key isolation) | **20/20 both regions** — the gate exercises replay/resume against a LIVE server; process-crash recovery is DST's job, not this script's |
| SDK WAN smoke (`@prisma/streams` against each edge) | PASS both regions |
| destructive-cutover wire checks on a real edge (`Stream-Encryption-Key` on the product route, `profile` in the creation document, `Stream-Key` on the raw route) | all 400, named-field refusals, never translated |

The campaign found and fixed two GATE defects (not server defects) that
only a real WAN could expose — recorded here because they were
initially indistinguishable from data loss:

1. **The gate never checked its own setup appends.** On WAN, the
   split fires MID-SETUP (the setup rounds span multiple scaler eval
   windows), and an early unchecked failure surfaced later as a lying
   "raw route shows []" read failure. Setup appends now retry
   transient refusals and fail loudly at the append step.
2. **The gate read single-shot and asserted full history.** Both
   surfaces page across lineage hops (one segment per response with a
   successor cursor) — the documented contract, which the SDK follows
   (`ReadPage.cursor`/`upToDate`). A fast local client kept every
   record in one segment, so the multi-hop shape had never been
   exercised. The gate now follows `Prisma-Next-Cursor` /
   `Stream-Next-Offset`, and `FIELD_PACE_MS` simulates WAN pacing so
   the paced (multi-hop) shape runs locally and in CI.

Deploy tooling hardened by the same campaign: the artifact
bucket/endpoint defaults in `bench/soak/deploy-region.sh` now come
from `$SOAK_HOME` (a stale constant cost an hour chasing phantom
egress failures), the binary downloader probes with a ranged GET so
real S3 error codes surface (HeadObject's empty error body maps
everything to UnknownError), and the object-store endpoint moved to
`fly.storage.tigris.dev` (Tigris's `t3.storage.dev` apex now serves
their website from some vantage points).

**Deferred to post-freeze arcs** (none of it a known-incorrect
behavior, all tracked, per the round-15 review's explicit deferral
list): the deterministic whole-system simulator and its remaining
catalogue scenarios (SEL-022's true remote-watermark pause, the
CRT-007 two-joiner variant), the fork-install cancellation generation
model for a creator that died before EVER installing a reference (the
crashed-AFTER-install case converges today via the DELETE retry), the
Compute-independent multi-instance VM campaign, warm-history block
refetch, and browser-support verification. The typed
incarnation-mutation registry API is DONE and every fork-lifecycle
mutation now rides it.

## Multi-instance fleet posture

The cloud campaign validated one service instance per region against
two real edges. It did NOT validate a coordinated multi-instance fleet
with live ownership movement on Compute (that ran earlier on the
pre-cutover build in the soak campaigns, not on the product surface).
A controlled preview with one instance per region is what this report
gates; nothing in the README or SLO may claim field-validated
multi-instance fleet operation until the fleet campaign (#113) runs.

## The close-group liveness observation (#115) — classification

A test-only wedge was observed twice in ~8 full-suite runs on
2026-08-03: `a_failed_group_fails_the_close_and_its_retry_together`
hung with the commit pipeline asleep and both close acks outstanding.
It reproduces only under the cfg(test)-ONLY composition machinery
(`commit_gate` hold + one-shot injected group failure — neither exists
in a release binary), passes solo in 0.02 s, and has not recurred in
any full run since; the test now carries a 90 s watchdog that turns
any recurrence into a red panic with counters.

Classification against the launch criteria, if the underlying
mechanism were ever reachable in production (not demonstrated):

- **No promised record can be lost.** Acks ride the durability
  barrier; a group that never dispatches never acks — clients see
  timeouts, and the ambiguity contract (§soak7) already governs
  timed-out appends.
- **Bounded, retryable errors — not silence.** `wedge_ms()`
  (commit-blocked OR oldest-in-flight age) feeds admission shed and
  the fleet load vector; a wedged shard sheds 429/Retry-After within
  the field-validated threshold (wedge-liveness gate, soaks 4/7).
- **No indefinite Sealing.** A seal claim is a 15 s fence lease; any
  retry takes over through the committer fence. No operator repair
  path exists because none is needed — descriptor and segment state
  cannot diverge across the barrier.
- **Recovery is platform-automatic, not operator-manual.** A
  permanently wedged committer in a live process is recovered by
  instance replacement (health-based recycle), through the same
  OpenGate/deadline/reaper reopen machinery the reopen-storm fix
  field-validated. In-process committer self-restart is deliberately
  NOT implemented.

Verdict: **acceptable for preview, tracked as #115** — the hunt
continues, but by the stated criteria it is not a launch blocker.

## Retained field deployment

The two campaign services are intentionally retained:

| | |
|---|---|
| services | `soak-server-eu-central-1` (fra), `soak-server-us-east-1` (ewr), projects `streams-camp75-eu` / `streams-camp75-use` |
| owner | sorenbs |
| purpose | post-campaign probes and preview follow-up on the exact gated build (`d445a06`) |
| cost envelope | 2 × 1-CPU/1-GB always-on (KEEP_AWAKE) + 3 Tigris buckets with camp75a test data |
| expiry | **2026-08-19** — tear down via `bench/soak/teardown.sh` (campaign stamp `camp75a-08032240`) or redeploy the frozen build, whichever comes first |
| credentials | per-project Tigris bucket keys minted 2026-08-03 (`$SOAK_HOME/bkey-*.json`); they die with the buckets at teardown; the workspace management token is not stored in the repo |

## Provenance output (regenerated with this report)

See the fenced block at the end of this file — `server_commit` there
must equal the commit this file ships in.

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
| Cloud gate re-run on the post-audit build | **pass** — 2026-08-04, build `d445a06`, fra + ewr, 20/20 each + SDK WAN smoke + wire cutover refusals | the campaign section above; the 2026-07-31 blockage documented below is HISTORY of the platform outage, kept for the record |

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

### History: the 2026-07-31 platform outage that delayed the re-run

(The re-run has since PASSED — 2026-08-04, above. This section is the
contemporaneous record of the five-day Compute edge-publication outage
and is retained as history.)

The audit requires re-running the cloud gate on the post-audit build
with negative auth, create replay, seal crash, fork crash, and split
coexistence. That harness now exists — `scripts/field-gate.mjs`, 20
checks — and it passes end to end against a local deployment,
**including driving a real scaler split** and verifying that every
routing key still reads back its own sequence while the raw route shows
the default key's records and nothing else.

At the time of writing (2026-07-31) it had not run against Compute.
Four deployments were made that day (11:51–13:15 UTC) and none became
reachable:

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

## Fourteenth audit round (2026-08-03)

The review's verdict — *freeze the surface; make queue operations and
registry mutations follow one model each* — is the right one, and it
named two concentrated correctness risks that were both real.

| finding | fix |
|---|---|
| Queue **config** ops escaped the round-12 staging discipline: `ConfigDelete` mutated the shared handle (a failed write left the durable consumer while dropping it from memory; a same-group `Receive` could copy it back), and `ConfigPut`/`ConfigGet` read the DB directly, so two puts in one group both saw "missing" and both minted a creation | one batch-local overlay (`Local.queue_configs`) that every config op consults and stages into; `ConfigDelete` mutates the group-local queue, never the handle; published only on write success. A truncated lazy-load is now a FAILED load (no partial state installed as `loaded=true`) |
| `release_fork_ref` set its `tombstoned`/`removed_ref` flags inside a `cas_update_retry` closure without resetting them per attempt, so a decision from a LOST attempt (tombstone) could survive into a winning one that retained the source for a live child — breaking the ancestry chain | the flags reset at the top of every attempt; only the successful attempt's decision is acted on |
| Fork-reference release was not incarnation-fenced: a delayed cleanup could evaluate expiry/soft-delete against a RECREATED source | `release_fork_ref` takes the `ForkRef`'s `source_epoch`; a mismatch is conclusive (the original source is gone) — which also lets recreated-source debt converge instead of lingering |
| `final_code_disposition` treated an unreadable/absent error body as definitive (abandoning the intent), contradicting `take_error_code`'s stated "unknown keeps the intent" | `None` code is now `AmbiguousOrTransient`; only a NAMED request verdict is definitive |
| The local field gate never ran in CI | a `product-field-gate` job runs the 20-check corpus with a deterministic split |
| The report named one commit while the reviewed archive was another | `scripts/release-provenance.sh` emits server commit, SlateDB pin, SDK tarball SHA, layout version, conformance pin, and DST scenario count |
| The top-level README still described the removed product (profiles, `Stream-Encryption-Key`, the singular route as primary) | rewritten around the plural product API, with the raw route labeled the Durable Streams standards surface; `PROFILES.md` moved to `docs/history/`; a client-support statement added (Node 18/22 + Bun + Deno gated, browsers expected-not-verified) |

Deferred to their own arcs, tracked, none a known-incorrect behavior:
the typed incarnation-mutation registry API (the deeper form of the
closure-hygiene and epoch-fence fixes above), the fork-install
generation model (so a crashed-before-install creator's tombstone debt
converges without a DELETE retry), and the whole-system simulator with
SEL-022's remote-watermark pause and the CRT-007 two-joiner variant.

## Twelfth audit round (2026-08-03)

| finding | fix |
|---|---|
| The seal-intent cleanup policy predated the generation fencing, and the two surfaces kept separate stringly-typed lists that had DRIFTED: the product list named codes its own translator never emits (`producer_stale_epoch` vs the real `stale_producer_epoch`), so stale-epoch was "retained" in the comment and definitive in fact — while the raw path retained it for real, letting a permanently-stale request hold a collection Sealing and renew its claim indefinitely | ONE typed policy (`FinalDisposition`), shared by both surfaces: gap and epoch-must-start-at-zero stay ambiguous-ordering (the predecessor may be inside the server; the epoch can advance); stale epoch, sequence reuse, Stream-Seq conflict, content/body errors, closed-by-another and superseded release the generation's uncommitted intent NOW. Post-round-11 every verdict is durability-grounded, and post-round-8 the release is generation-fenced — it can never destroy a concurrent exact retry, which owns a newer generation |
| Queue consumer state (cursors, leases, acks) was mutated in the SHARED handle while its WriteBatch was still being assembled — a failed group write left phantom in-memory leases and cursor movement inconsistent with durable state | queue state joins the applied/durable discipline: staged into a batch-local clone, published to the handle only after the write succeeds — exactly the treatment producer rows and tail state already had |
| The engine fence map was correct but invisible | `seal_fence_entries` and `seal_fence_max_generation` in the metrics snapshot — the map is deliberately unbounded (no wall-clock expiry can be proven safe), so its cardinality is observable before it could ever matter |

The group-write-failure gates the review asked for landed the previous
day in the DST expansion (`dca9921`): `fail_next_group_for` +
DUR-002/008, SEL-021, CRT-007.

## Eleventh audit round (2026-08-02)

| finding | fix |
|---|---|
| The round-10 six-hour fence expiry was unsafe: an AppendReq has no maximum queue residence (a timed-out handler drops only its receiver; backpressure can hold the queue arbitrarily long), so a wall-clock timer proves nothing about the request a fence exists to stop | pruning REMOVED. Fences live for the engine's lifetime — one u64 per ever-fenced segment, dying with the exact queue they protect. A bounded cleanup would need queue-progress proof, not elapsed time |
| Definitive conflicts (sequence reuse, producer gap/stale/epoch, Stream-Seq conflict, closed) were still answered from batch-local or applied state — a crash before the group write left a client holding a permanent verdict about state that never existed | the barrier rule now covers RESULTS, not just successes: `pending` carries `Result<AppendAck, AppendErr>`; state-dependent refusals ride the same durability barrier, and a failed group write returns the group's failure instead of a conflict judged against unwritten state. Request-intrinsic errors (malformed body, content-type, syntax) stay immediate |
| Concurrent plain closes could 503 each other: each join RENEWS the shared Empty claim, and a close that pinned its admission-time generation failed publication against a sibling's renewal | a plain close-only passes no generation into run_seal and ADOPTS the standing one; only a final-bearing owner pins its own |

### Deterministic gates added

* `state_dependent_conflicts_wait_for_durability` — a sequence-reuse
  409 must stay pending while the row it judges is applied but not
  durable (red: "a definitive conflict answered before the state it
  judges was durable").
* `idempotent_successes_wait_for_durability` now drives the close-only
  branch through the same held-dispatch window (red proven for both
  halves).
* `a_fence_outlives_the_maintenance_sweep` — pins the no-expiry
  property against any future "cleanup" (red: sweep clearing fences).

## Tenth audit round (2026-08-02)

The fence passed; the review moved one layer down and found the same
disease in older tissue: two committer fast paths still answered
before durability, and the product seal's incarnation binding stopped
at the claim.

| finding | fix |
|---|---|
| Producer-duplicate and idempotent close-only answers were sent immediately from batch-local or applied state — a retry could observe "durably committed" for a record whose group write later failed, and a concurrent exact seal retry could mark, close and publish Sealed off a duplicate whose original was never durable | no successful answer whose truth depends on non-durable state leaves before its barrier. Duplicate and close-only acks ride the group's `pending` (failed with the group if its write fails); any ack in a write-free group attaches to the NEWEST in-flight group, exactly like a fence; only with nothing in flight is the observed state already durable and the answer immediate |
| The product seal carried its validated epoch into the claim but dropped it before the final append: `product_append_inner` refetched the descriptor by name, so a seal claimed on incarnation A could write its final into a same-name same-key replacement and physically close the replacement's segment (only the mark failed, after the damage) | the final append carries a typed execution token (`SealAuthz { op_id, generation, epoch }`) and refuses to run unless the CURRENT descriptor matches all three; a trusted final also never enters the raw-close claim path — without the token check, the red run showed the replacement fully SEALED by the stranger's final |
| The fence map had no bound | entries are timestamped and pruned after 6 h — far beyond any queued request's possible residence — on the same sweep that drives handle eviction; the map stays engine-level (the safety scope) |

## Ninth audit round (2026-08-02)

Round 8 introduced the right concepts; round 9 makes the fence what
the design claimed it was — durable, non-evictable, and uniquely
installed — and binds the last unfenced steps to their incarnation.

| finding | fix |
|---|---|
| The fence answered from STAGED state (`local.fields` / `applied`), so a takeover could read `closed=true` off a WriteBatch that had not been written — or a WAL that was not remotely durable — mark the old final committed, publish Sealed, and then watch the write fail | the fence RAISE is immediate, but its RESPONSE is durability-barriered: it rides the group ack pipeline (`in_flight` → `dispatch_durable`), so it reaches the takeover only after the durable watermark covers every write decided before it. A fence in a group with writes is answered with that group; a fence-only group attaches to the NEWEST in-flight group; with nothing in flight and nothing staged, the observed state is already durable. A failed group write fails the fence with it |
| Two takeovers could reserve against the same lapsed claim; the LOWER reservation could install beneath the higher fence, wedging the collection under its own recovery protocol | the installation CAS requires `seal_gen_counter == reserved` — only the newest reservation installs; older ones restart. Joins (a plain close joining a non-owing sealing) now RENEW instead of sharing a possibly-fenced standing generation |
| The fence lived in the evictable `StreamHandle`; idle eviction or the resident cap re-created it at zero while a stale claim-authorized append was still queued (queued requests carry only the stream hash) | fences live on the ENGINE (`seal_fences` map), which dies exactly with the queue it protects |
| The product seal validated the key against one descriptor and then fetched a FRESH epoch for the claim — a delete+recreate under the same key inside that gap had the request seal a replacement | one descriptor read per seal request; its epoch threads through claim, append, mark and publication; `enter_sealing` no longer re-fetches |
| A close-with-content whose producer tuple was spent by an earlier NON-closing append received the duplicate answer and left its final intent behind — a collection Sealing behind an undeliverable promise | a duplicate that did not close releases this request's own epoch+generation-fenced intent and answers as the duplicate it is; the collection stays open |
| The autonomous scaler's heat survived recreation (sketches keyed by name), and phase-B publication was name-scoped | sketches carry the incarnation that fed them (a feed from a new epoch RESETS the sketch); decisions carry that epoch to `execute_split_fenced`/`execute_merge_fenced`; segments sketched under different incarnations never merge; phase B CASes under the epoch the pending transition was read from |
| `run_seal` could report success against a replacement (terminal read unfenced) or bless a DIFFERENT operation's terminal state | the terminal proof requires the expected epoch, and a caller driving a named operation errs unless `seal_op` is its own |

## Eighth audit round (2026-08-02)

The round-7 claim timeout was a race timer, not a lease: nothing
fenced the old operation's already-queued append out of the committer,
and the incarnation fence stopped at claim installation. Both
architectural gaps are closed by ONE mechanism — every seal claim now
carries an execution token (incarnation + a generation from a
monotonic per-descriptor allocator), and every step of the execution
is checked against it.

| finding | fix |
|---|---|
| A takeover could steal a claim whose owner's final append was still queued (HTTP timed out at 10 s; the committer kept the write). Whichever write lost the race then saw `stream_closed`, classified it definitive, and removed its own intent — a physically closed segment behind an open descriptor | takeover is a fenced protocol: reserve a generation → push a FENCE message through the target segment's committer queue → consult its closed-report. The fence answers only after every earlier append was decided, so `closed=false` proves the old write can never land (the committer refuses claim-authorized writes below the fence BEFORE staging records or applying a close), and `closed=true` means the old operation won — the takeover completes the OLD transition instead of stealing it |
| An exact retry did not renew the claim, so an active owner could be taken over mid-flight — and could be fenced out permanently by an aborted takeover's reservation | a same-operation re-entry re-allocates the generation and refreshes the lease; every allocation is above every earlier fence, so an actively retrying owner always clears them |
| `mark_final_committed` read the CURRENT epoch (proving nothing about the operation's incarnation); `abandon_seal_intent` and `run_seal`'s publication were name-scoped; the raw path continued into `run_seal` after a failed mark — so a close from a deleted incarnation could seal the replacement | mark, abandon, segment closes and publication all require the operation's original epoch AND its claim generation; publication succeeds only while the exact claim this call drove still stands; a failed mark answers 503 `seal_incomplete` and never proceeds |
| The raw identity hashed the DESCRIPTOR's content type, so a close with the wrong type (whose ct-mismatch verdict is deferred when a producer rides along) shared the valid close's identity, joined its intent, and tore it down with its own definitive verdict; `Stream-Key-Version` likewise absent | the identity covers the request's OWN content-type and key-version headers — every input that affects what the committer persists or how it rules |
| Split/merge phase CASes and TTL slides were name-scoped background mutations | phase A/B run under the deciding descriptor's epoch (`execute_split_fenced`); transition parent-closes carry a generation allocated in phase A; TTL slides are fenced to the incarnation that spawned them |
| A plain `:seal` refused ANY outstanding final claim, contradicting the documented recovery ("wait 15 s and seal") | seal-only goes through the same claim path: a LIVE final claim is a 409, a lapsed one is taken over through the fence protocol — recovery by plain seal is now real |

### The lease, precisely

`SEAL_CLAIM_MS` now decides only when a takeover may START. Whether
the old operation is really gone is decided by the fence: its write
either committed before the fence (and the takeover completes that
transition) or can never commit after it (and the claim is replaced).
No wall-clock reading ever decides the fate of a write.

The fence is in-memory by design: it exists to stop a stale append
that is already in this process's queue, and a restart drops the
queue and the fence together.

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
  that owns it (the exact same request finishes the transition), or —
  after the 15 s lease lapses — seal it with a plain `:seal`; the
  abandoned claim is taken over through a committer fence, so a
  takeover can never race the dead operation's own late write. There
  is no operator intervention and no repair tool, by design.

---

## Provenance (regenerated at report freeze)

```
server_commit:      515a64f0b7076a4c80d7ae730103a3f7f7624722
server_dirty:       no
slatedb_pin:         0717cc1e4e9bad10a4773760f66bac4264ecf05e
layout_version:     3
conformance_pin:    0.3.6
sdk_tarball_sha256: cb578dff7ff2eeeba63759898f44735f7de81b5fa07d8a4f3a6769b853cc2d31
dst_scenario_tests: 166
rust_suite_passed:  273   (RUN_SUITE=1 on the same tree, pre-stamp:
rust_suite_failed:  0      273 passed / 0 failed; 166 DST scenarios)
```

The `server_commit` above is the round-15 code commit; this stamp
commit adds only this block. Local gates at that commit: suite 273/0,
pinned DS conformance 332/0/6, field gate PASS (unpaced and
FIELD_PACE_MS=1200 WAN shape), installed-package SDK smoke PASS.
