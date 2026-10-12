# Streams operations & release program (#114)

Standing policies for running and releasing Streams. Facts below are
sourced from the campaign record (RELEASE-PRODUCT-SURFACE.md,
SOAK-REGIONS, COST-CAMPAIGN-2, BUCKETS-SINGLE-REGION) — where a number
has a date, that is its provenance, and re-measurement supersedes it.

## 1. Dependency (fork) ledger

| item | state | exit condition |
|---|---|---|
| SlateDB pin | upstream main `0717cc1e` (git-rev pinned; fork RETIRED 2026-08-05, #116) | move to a crates.io release when one ships past v0.15.0 |
| our upstream PRs | #1964 (WAL yield) MERGED; #1991 (GC cadence) superseded by upstream #1993; probe/concurrent-deletes patches dropped (upstream #1968/#1894) | — |
| open upstream items | PR #2010 (fencing conflict cap, typed error after 8 consecutive CAS losses); issue #1970 answered with 0.15 field data | drop local expectations when #2010 lands |
| foyer-memory 0.22.3 (through the SlateDB pin) | self-deadlock, found 2026-09-24, upstream issue not yet filed: a cache miss polled while its Tokio runtime is closing spawns the fetch into the closed task list, Tokio drops the fetch on the same thread, and the fetch's destructor takes the in-flight lock `get_or_fetch` still holds. Absorbers and every other reader using the block cache can hit it when a runtime shuts down. Contained by the bounded service-runtime teardown (`bootstrap::serve`, 5 s), so process exit cannot hang; tests whose `#[tokio::test]` runtime drops live readers remain exposed | when a SlateDB pin carries a fixed foyer-memory, the canary `held_scan_teardown_helper` fails; then retire this row and the foyer rationale on `TEARDOWN_BOUND` |
| carried behavior deltas | none in code; GC listing economics handled by static long sweep intervals (600 s, constants of the binary since 2026-09-29: `EngineConfig::GC_QUIET_INTERVAL` and the history GC interval) | revisit if upstream adaptive cadence ships |
| conformance suite | `@durable-streams/server-conformance-tests` EXACT-pinned `0.3.6`, must equal `src/protocol_pin.rs` | bump only with a protocol-pin change and a full dual-surface rerun |

Rule: every carried patch exists as an upstream PR or a dated reason
in this table. A patch with neither is a defect in the ledger.

## 2. Security review (surface inventory + posture)

Reviewed surfaces and their enforcement points (audited across rounds
1–17; anchors in parentheses):

- **AuthN — TWO credentials, two boundaries (round 19):**
  - the CUSTOMER account bearer authorizes every public `/v1/*` route
    (product, raw, `/v1/segments`, and — since round 19 — every
    `/v1/debug/*` route, which previously answered unauthenticated
    while this document claimed otherwise; `absorb-pause` and `sleep`
    MUTATE production state, `load` resets peak gauges, `usage`
    exposes per-stream data). Route parsing is exact and happens
    BEFORE auth, auth BEFORE body buffering (#99). Negative-auth
    matrix in the suite covers every product route.
  - `FLEET_INTERNAL_TOKEN` authorizes ONLY `/v1/internal/*` (peer
    fan-out RPCs). It is mandatory in fleet mode — startup refuses a
    missing token, one under 16 characters, or one equal to
    `AUTH_TOKEN` — and the two never cross: a customer bearer cannot
    reach an internal route, and the internal token cannot perform a
    product operation. Internal routes fail CLOSED when unset.
    Rationale: these RPCs fence consumer generations and read segment
    state without a stream key, so a customer token reaching them
    would be a cross-tenant corruption primitive.
  - The ONLY unauthenticated surface is `/operator` (explicit product
    decision; operational metadata only — never names, keys or tokens).
- **Peer trust (round 19):** peer base URLs (heartbeat `url` and
  `fleet/urls.json`, both bucket-writable) are validated as bare
  http(s) origins — no userinfo, path, query, fragment, whitespace or
  non-numeric port — with TLS mandatory unless
  `FLEET_ALLOW_HTTP_PEERS=1` and an optional `FLEET_PEER_DOMAINS`
  allowlist. Relays carry the internal token, and the customer key
  only where decryption is actually required.
- **Incarnation binding (round 19):** every internal RPC carries the
  sender's stream epoch, segment id, and derived segment identity; the
  receiver re-derives all three against the current descriptor before
  reading or mutating anything and answers `409 stale_target` on any
  mismatch. A name is not an identity, across instances too.
- **Response origin (round 19):** every response, errors included,
  carries `Prisma-Streams-Origin`. Routers treat an unmarked response
  as "never reached a server" and convert it to a retryable 503 —
  platform 404s can no longer masquerade as "stream does not exist".
- **Encryption:** stream keys are client-supplied per request and
  never stored; the server holds only fingerprints for mismatch
  refusal (403 `wrong_key`). The raw surface may run with
  `--conformance-default-key` (flag-only, no env); production posture
  is per-caller keys.
- **Signed artifacts:** product cursors are HMAC-signed against the
  stream key (`product_cursor`); watch observation URLs are
  key-derived and credential-free with server-side sig verification
  (#95). Consumer version tokens are deliberately UNSIGNED — they pin
  a target, they are not a capability; DELETE authorization is the
  bearer + key (#122, round 17).
- **Isolation:** the raw route is a true default-key view — keyed
  product records are never served through it (#92).
- **Telemetry planes (round 20, docs/OBSERVABILITY-BILLING.md):** the
  whole leading-`_` stream namespace is SYSTEM-RESERVED — refused with
  403 on both public surfaces after auth, invisible to the catalog,
  excluded from customer usage and limits. `_usage`, `_ops_metrics` and
  `_ops_events` are encrypted with the deployment's USAGE_STREAM_KEY
  (minted into `$SOAK_HOME/usage-stream-key.txt`, never the customer
  key) and written only through the in-process system path.
  BILLING_MODE=required refuses startup without the ledger key. Ops
  events and metrics are low-cardinality by construction: stream names
  appear only in `_ops_events` (operator surface), never in
  `_ops_metrics` dimensions; record content, routing keys, producer
  identities and tokens are prohibited telemetry fields everywhere.
- **Secrets handling:** all deployment secrets in `$SOAK_HOME`
  (outside the repo); nothing secret in the tree — enforced by
  convention and review, checked at release time.
- **Known gaps (tracked, not launch-gating):** per-token rate limits
  (admission is currently global per instance); token rotation
  runbook; dependency CVE watch is `cargo deny` at release only (no
  continuous scanning).

## 3. SLO policy

Ack SLO: **append p50 ≤ 250 ms, in-region caller → durable ack.**
Measured envelope (soak5, 2026-07-27, gather=6ms): nrt 54 / fra 59 /
cdg 68 / sjc 87 — all regions PASS except **us-east-1 (iad1, 341 ms):
excluded from the SLO region set** until the PoP improves (its
mutating-op store costs are 5–12× peers; documented in
SOAK-REGIONS).
That envelope predates the 100 ms write tier (edge change #97, ratified
2026-10-11 under the owner's delegation, S2): at the default
`WAL_FLUSH_GAP_MS=100` an append on a busy shard waits up to the rest of
the gap before its WAL write, about half a gap more on average. The SLO
has not been re-measured at 100 ms; the
interpolated fra p50 is about 0.09-0.14 s (NEXT-WORK §14.8 holds the
local model check still to run).

- Live-read wake: rt ≈ append + 2 ms with ring+gather posture
  (field-validated, soak7). `deliver=applied` subscribers see events
  before durability, up to the rest of the flush gap plus one WAL write
  earlier (opt-in, crash-window documented).
- Error semantics inside SLO: 429/`Retry-After` shed under overload
  and 408 ambiguity on timed-out appends are IN-contract behaviors
  (bounded, retryable), not availability failures; 5xx without
  `retryable:true` is the error budget.
- Availability claim: **one instance per region** until the
  multi-instance campaign (#113) closes; recovery is
  platform-automatic instance replacement through the
  OpenGate/deadline/reaper reopen path (field-validated).
- **Bucket placement rule (2026-08-05, rev 2):** buckets inherit
  their PROJECT's region — set `"region"` at project CREATE or the
  project (and its buckets) is US-homed; projects cannot be re-homed.
  Co-located single-region buckets beat global on every op from fra
  (`put:wal` 16 ms vs 27, GET-404 8 ms vs 268 — the fixed miss
  penalty is gone from GET; HEAD-miss still ~121 ms, Tigris bug,
  tracked). Migration therefore means a NEW region-set project per
  cell — BUCKETS-SINGLE-REGION.md has the numbers and the rehearsal
  plan.

## 4. Drills

Each drill names the command and the observable that proves it.

1. **Instance replacement (monthly, or after any reopen-path change):**
   kill the instance under light load
   (`bench/soak/deploy-region.sh <region> server` redeploy, or
   platform recycle). PASS = health 200 within ~2 min, zero
   acknowledged-record loss (verify bin), fencing takeover visible in
   logs ("fences prior owner"), no reopen storm (OpenGate counters).
2. **Fence takeover under load:** run the field gate while
   redeploying. PASS = in-flight appends resolve as acks or in-
   contract 408/429; no post-fence acks from the old instance.
3. **Wrong-arch zombie check (every deploy):**
   `xxd -s 18 -l 2 -p <binary>` MUST print `3e00` before upload; a
   deploy that health-checks green but serves nothing is the ENOEXEC
   crash-loop signature.
4. **Store-outage posture (tabletop + s3lite fault run):** SlateDB
   retries store faults internally — appends never fail from store
   errors, they time out into the ambiguity contract; shed activates
   on `wedge_ms()`. Verify with the FaultStore soak profile
   (`acked_records_survive_store_faults` family locally).
5. **Restore-from-restart (quarterly):** stop an instance mid-load,
   restart on the same bucket namespace. PASS = durable frontier
   resumes exactly (no acked loss, no double-ack), dirty-stream
   rediscovery re-arms absorption (#41/#49).

Drill results append to this file with dates; a failed drill is a
release blocker until root-caused.

## 5. Supply chain

- **Rust:** `cargo deny check` in `scripts/release-gate.sh`
  (licenses/advisories/bans); slatedb is git-REV pinned (immutable
  hash, not a branch); `Cargo.lock` committed.
- **SDK/npm:** `@prisma/streams` ships with ZERO runtime
  dependencies; version aligned to the release tag; tarball SHA-256
  recorded in the release report and verified by the reviewer
  independently (preview.2: `972482c4…`, preview.3: `c7ec1f4c…`).
- **Conformance:** exact-pinned version, run through the local vitest
  wrapper (never the package's own CLI), result checked by
  `conformance/check.mjs` against `expected.json`.
- **Artifacts:** every deployed binary is uploaded as
  `bin/streams-<tag>-x64` with a `.rev` sidecar naming the exact
  commit; `scripts/release-provenance.sh` binds report ↔ commit ↔
  artifact (three-commit form since preview.2).
- **Build:** `cargo zigbuild --release --target
  x86_64-unknown-linux-musl`; e_machine gate (drill 3) before any
  upload.

## 6. Mixed-version rollout rules (formal-verification closure)

Several correctness fixes on this branch change behaviour that older binaries
do not share. A rolling deployment runs both for a while, and a rollback runs
the older one against state the newer one wrote. None of the rules below has
been qualified with an actual old/new binary combination; until one is, use
the coordinated form each rule names. The storage layout number is unchanged,
but that does not make rollback safe.

| Change | What an older binary does | Rollout rule | Rollback rule |
|---|---|---|---|
| Durable seal fence rows (`<hash16> 'G'`, "A seal takeover's fence outlives the engine that recorded it") and barriered `SealSuperseded` ("A SealSuperseded refusal waits until the fence behind it is durable") | Neither writes nor reads the fence row, and answers `SealSuperseded` from its staged cache at once. An older owner therefore reopens TLA-002-F1/F2 for any segment it serves. | Coordinated: no seal takeover may start while an older binary can own a shard. Replace every instance before seal traffic resumes, or accept the F1/F2 window and release-note it. | Allowed only when no descriptor carries a seal claim (`sealing` is empty registry-wide); otherwise the older owner ignores fences the newer one relied on. |
| Seal renewal and release ("A seal retry refused by its own instance's limits neither renews nor releases the claim") and close gating ("Only a close can resume an owed final") | Renews an owed claim before validating the retry, releases on any definitive refusal, and lets a plain append with a final's bytes renew the claim. | During the window TLA-003-F4/F5 and the plain-append hazard stay reachable on older instances. Prefer a coordinated replacement; do not change `LIMIT_*` or `MAX_RECORD_PAYLOAD_BYTES` during it. | As rollout. |
| Default record ceiling ("A server that does not set MAX_RECORD_PAYLOAD_BYTES refuses a record over 131,072 bytes", edge #71) | Accepts any record on a deployment that never set the name. | On such a deployment the upgrade is a limit reduction: set `MAX_RECORD_PAYLOAD_BYTES` explicitly on both versions first (131072, or 0 to keep no ceiling outside the release posture), then roll. Deployments that source the profile are unaffected. | As rollout. |
| JSON storage contract ("A JSON collection stores each record as the client's validated text"), including seal operation ids under `prisma-seal-v3` | Stores a serde re-encoding and derives seal ids from it. | Roll out when no descriptor owes a final (`owes_final()` in the registry). Otherwise an in-flight seal with a final gets 409 for up to `SEAL_CLAIM_MS`, then a takeover, and a retry of a seal the old build completed answers `AlreadySealed`; release-note it. The dead-letter "own sequence means delivered" rule ships in the same release. | Records stored as client text stay as stored; an older binary re-derives old-style seal ids (the same 409 window in reverse). |
| Read position `u64::MAX` ("A read position of u64::MAX is a position, and now has its own representation") | Treats a forwarded position `2^64 - 1` as its tail. | None; the gap closes when every owner runs the fix. | None. |
| Provisional cursors `KIND_KEY_V3` ("A provisional read cursor proves the history it continues, or answers an explicit resync") | Rejects a V3 cursor with `400 invalid_cursor`; today's SDK throws on it. | Coordinated (all instances at once), or accept that a client holding a V3 cursor gets 400 from an older instance during the window and must resume from its durable cursor. The SDK should learn to rewind on that answer in applied mode (open). | Clients holding V3 cursors get 400 and resume from their durable cursor. |
| Fork-debt markers and backfill ("A background reconciler releases fork references that deleted children still owe", "Fork-reference debt from before the index is backfilled…") | Creates debt without a marker. | After the rollout completes, delete `registry/v4/fork-debt-backfill.json` so the backfill walks again (RUNBOOK.md §8). | Older binaries ignore markers; they stay until a newer binary pays them. |
| Conditional writes without client retries ("Registry conditional writes never mistake their own committed write for a refusal") | Can misreport its own committed conditional write as a refusal. | None beyond the defect staying reachable on older instances. | None. |

Performance acceptance is separate from these rules: the recount read-ahead
("The absorbed-byte recount reads ahead again…") and the postings-cache
bridging change carry measured costs (docs/PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md
§0) that still need workload acceptance.

## 7. Release-candidate certification: the fleet and single-cell shapes

`scripts/promote-rc.sh <sha> <rc-tag> <arguments>` mints a release-candidate
tag. It requires every `REQUIRED_CHECKS` check run green at the SHA, then
runs `scripts/rc-certify.sh <arguments>` in a clean detached worktree at
that SHA. rc-certify has two shapes, and each refuses the other's
arguments before the gate runs. Both start the same way: the local
release gate (`scripts/release-gate.sh`, with the capacity mechanism test
run alone, so on an idle host), then the RC identity from the
`binaries.json` that `bench/soak/build-upload.sh` wrote. Its server entry's
`gitCommit` must be the tagged commit, and its `sha256` is the RC identity
that every later manifest must name. Every check runs as `python3 -E`, so
the caller's `PYTHONOPTIMIZE` cannot strip its asserts.

**Fleet** (`--shape fleet`, the default): the capacity campaign (its
`binaries.json` is the identity), handoff, LiveFeed fleet and LiveFeed
canary manifests. The script's header lists each check.

**Single-cell** (`--shape single-cell`): a single-server, fleet-off RC,
which is the public beta's shape (the owner's answers (1) and (5) of
2026-10-11). The leg set is the lead's call on question 7, made under the
owner's "you decide" of 2026-10-11, not an owner decision (hardening README,
"Public beta (decided 2026-10-11)"):

| argument | evidence | what rc-certify checks |
|---|---|---|
| `--artifact-run <id>` | `$SOAK_HOME/results/<id>/binaries.json` from `build-upload.sh` | server `gitCommit` equals HEAD; its `sha256` is the RC identity |
| `--provider-contract-log <path>` | the real-provider contract log against Tigris, as `scripts/test-leg.sh` kept it ([PROVIDER-CONTRACT.md](PROVIDER-CONTRACT.md)) | `tests_ran.py --exact ...::real_provider_meets_the_provider_contract` passes; exactly one `qualifying endpoint` line, whose endpoint is https on a `storage.dev` or `tigris.dev` host; the `real-provider: provider contract passed` line; no `SKIPPED:` line |
| `--layer-b-run <m>` | Layer B on the single-server shape | legs `H9`, `H9b`, `H11t`; `projects` at least 1,000 |
| `--envelope-run <m>` | the one-shard envelope arm (`WC_PMP`; it sets `CELL_ENVELOPE_*`) | the common contract below |
| `--sc0-1s-run <m>` | SC0-1s, the 72 h idle floor with its RSS-slope bar | the common contract; a run window of at least 259,200 s (72 h) |
| `--sc1-run <m>` | SC1, about 30 h of wakes | the common contract; a run window of at least 108,000 s (30 h, the length of its probe schedule in the Q5 design: the lead's reading of call 7's "about 30 h" as a floor, recorded for the owner to correct) |
| `--stream-count-run <m>` | the stream-count arm at the claimed scale | `streams`, a positive integer: the scale the tag states |
| `--battery-run <m>` | the shared-cell battery reshaped for one server | legs `hostile-tenants`, `cost-hostile-tenants`, `parked-consumers` (SC0b, 1,000 parked consumers), `ch-probe`, `suspension-under-load`, `revocation-under-load`, `billing-reconcile` (against the ledger); `projects` at least 1,000 |

Each `<m>` is a manifest path, or a run id whose
`$SOAK_HOME/results/<id>/` holds `<arm>-manifest.json` (`layer-b`,
`envelope`, `sc0-1s`, `sc1`, `stream-count`, `battery`). The common
contract every arm manifest meets:

```json
{"arm": "battery", "server_sha256": "sha256:<RC identity>", "verdict": "PASS",
 "legs": {"hostile-tenants": "PASS", "...": "PASS"},
 "started_unix": 1791000000, "finished_unix": 1791043200, "projects": 1000}
```

`arm` is the arm's own name. `server_sha256` (with or without the
`sha256:` prefix) is the RC identity: every arm runs the exact
x86_64-musl RC binary, so an arm run on a host build cannot certify.
`verdict` is `PASS`, and `legs` is non-empty with every value `PASS`.
`started_unix` and `finished_unix` are the run's window in unix seconds
(integers); `finished_unix` is later than `started_unix`, and for SC0-1s
and SC1 later by at least the arm's floor in the table, so a short run
that writes `PASS` does not certify either arm. Like the verdict, the
window is the harness's own statement: rc-certify checks it but cannot
re-derive it from raw results. The Q5 harness and the reshaped battery
that will write these manifests do not exist yet (NEXT-WORK, public beta
items 9 and 11). This table is the contract they write to.

Waived for a single-cell RC: the script prints each waiver, and the
certification manifest and the tag record them. They are the capacity
campaign as product evidence (post-split scaling needs a fleet; the
release gate still runs the capacity mechanism test alone), handoff (one
server owns every shard), the LiveFeed fleet certification (`promote-rc.sh`
still requires CI's `livefeed-fleet-cert` check at the SHA) and the
LiveFeed canary (its battery runs a three-instance fleet). The tag says
that it certifies no multi-server operation.

rc-certify writes `target/rc-certify-manifest.json`: `shape`,
`server_sha256`, the provider log's sha256 and endpoint, each arm's
manifest sha256, legs, scale and run window, the waivers, and
`tag_lines` (naming the SC0-1s and SC1 hours), which `promote-rc.sh`
prints in the tag. The provider-contract log names no commit.
rc-certify re-checks its content and records its sha256, but it cannot
prove which tree ran it, so run it from the RC worktree.

```sh
SOAK_HOME=~/.streams-soak scripts/promote-rc.sh <sha> <rc-tag> --shape single-cell \
  --artifact-run <build-run> --provider-contract-log <path> \
  --layer-b-run <id> --envelope-run <id> --sc0-1s-run <id> --sc1-run <id> \
  --stream-count-run <id> --battery-run <id>
```

`scripts/quality/test_rc_certify.py` runs the real script against
disposable evidence for both shapes. It covers each refused mutation,
each shape's refusal of the other's arguments, and the waivers.
`quality.sh` collects it through `python3 -m unittest discover -s scripts/quality`,
so `gate.sh` and the rust-quality workflow run it too. rc-certify does
not hold the public beta's other holds: the scoped mutation campaign on
the RC artifact (the lead's call 10) and the deployment gates (hardening
README, "Deployment gates").

**The SDK tarball.** promote-rc builds and packs the SDK at the RC's
commit, in its worktree, and keeps the tarball in `target/rc/<rc-tag>/`
of the checkout it runs from. It refuses to start when the tag exists or
that directory exists, so a kept tarball is never overwritten, and a run
that mints no tag removes the directory it made, so the next run is not
refused. The tag records the tarball's name and sha256, and npm's
integrity (`sha512-...`), the digest the registry reports for a published
version. The tarball lives under `target/`: `cargo clean` (or deleting
`target/rc/`) loses it, and `scripts/publish-sdk.sh` then refuses the tag
until the RC is promoted again under a new tag. No gate runs this
tarball: `scripts/release-gate.sh` and `scripts/rc-certify.sh` never touch
`sdk/`, and CI's required `sdk-package` check smoked a pack it made itself
from the same commit, with Node 22's npm. So the tag pins the bytes of a
pack of the certified commit, and CI smoked an equivalent pack; it does
not certify these bytes.
`scripts/quality/test_promote_rc.py` runs the real script with stubbed
gates, `gh` and `npm`.

**Publishing the SDK** (the owner's, a release hold): `scripts/publish-sdk.sh
<rc-tag>`, in the checkout that minted the tag, publishes that kept
tarball and nothing else. It refuses a lightweight tag, a commit without
`CI_GREEN` from `scripts/dev/ci-status.sh`, a tarball whose sha256 or
integrity differs from the tag's, another package, a version the tag is
not a release candidate of (`v<version>-rc.<n>`: the SDK moves in lockstep
with the server, the owner's N2 of 2026-10-12), a README that still holds
a placeholder of the beta (a line with a `<...>` and "published with the
beta" or "before publishing"), a CHANGELOG whose first release heading is
not `## <version>` (an `(unreleased)` one included), and a version already
on npm. npm versions are immutable, so each of these is checked before
anything reaches the registry, by `--dry-run` too. It then refuses to publish while `NPM_TOKEN`, `NODE_AUTH_TOKEN`
or an `npm_config_*_auth*` variable is set, runs `npm publish <tgz> --tag
beta --access public --registry https://registry.npmjs.org/` (every npm
call names that registry, whatever an npmrc's `@prisma` scope says; npm
asks the owner's account for its 2FA code),
reads the registry's integrity back against the tarball's, prints `npm
dist-tag ls @prisma/streams` and a `RELEASE_NOTES:` line, and ends
`PUBLISH_SDK_OK`. `--dry-run` runs the checks and stops before the publish
(`PUBLISH_SDK_DRY_RUN_OK`). `scripts/quality/test_publish_sdk.py` covers
each refusal with a stubbed `npm`.

## Release checklist (the short form)

1. `scripts/release-gate.sh` (everything `scripts/quality.sh` checks, every
   formal receipt `check --fresh`, the debug-profile suite over every
   target, the capacity gate alone). For a release candidate,
   `scripts/promote-rc.sh` runs it through `scripts/rc-certify.sh` in the
   fleet shape, or in the single-cell shape for a single-server, fleet-off
   RC (§7).
1a. Mixed-version rules (§6) decided for this release; the provider
   contract suite (docs/PROVIDER-CONTRACT.md) run against the production
   provider, endpoint and configuration, with its log retained.
2. Full release suite + pinned DS conformance + field gate (unpaced
   AND paced) + installed-tarball SDK smoke.
3. Build musl, e_machine gate, upload with `.rev`.
4. WAN smoke on the retained edge (field gate or the scenario smoke
   relevant to the change — e.g. `scripts/consumer-saga-smoke.sh`).
5. `scripts/release-provenance.sh` → paste into
   RELEASE-PRODUCT-SURFACE.md → docs commit → tag.
6. The SDK, after the RC is certified and by the owner (§7, "Publishing
   the SDK"): `scripts/publish-sdk.sh --dry-run <rc-tag>`, then
   `scripts/publish-sdk.sh <rc-tag>`.
