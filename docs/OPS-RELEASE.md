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
| carried behavior deltas | none in code; GC listing economics handled by static long sweep intervals (`GC_QUIET_INTERVAL_SECS` / `HISTORY_GC_INTERVAL_SECS`, default 600) | revisit if upstream adaptive cadence ships |
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

- Live-read wake: rt ≈ append + 2 ms with ring+gather posture
  (field-validated, soak7). `deliver=applied` subscribers see events
  one WAL interval earlier still (opt-in, crash-window documented).
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
| JSON storage contract ("A JSON collection stores each record as the client's validated text"), including seal operation ids under `prisma-seal-v3` | Stores a serde re-encoding and derives seal ids from it. | Roll out when no descriptor owes a final (`owes_final()` in the registry). Otherwise an in-flight seal with a final gets 409 for up to `SEAL_CLAIM_MS`, then a takeover, and a retry of a seal the old build completed answers `AlreadySealed`; release-note it. The dead-letter "own sequence means delivered" rule ships in the same release. | Records stored as client text stay as stored; an older binary re-derives old-style seal ids (the same 409 window in reverse). |
| Read position `u64::MAX` ("A read position of u64::MAX is a position, and now has its own representation") | Treats a forwarded position `2^64 - 1` as its tail. | None; the gap closes when every owner runs the fix. | None. |
| Provisional cursors `KIND_KEY_V3` ("A provisional read cursor proves the history it continues, or answers an explicit resync") | Rejects a V3 cursor with `400 invalid_cursor`; today's SDK throws on it. | Coordinated (all instances at once), or accept that a client holding a V3 cursor gets 400 from an older instance during the window and must resume from its durable cursor. The SDK should learn to rewind on that answer in applied mode (open). | Clients holding V3 cursors get 400 and resume from their durable cursor. |
| Fork-debt markers and backfill ("A background reconciler releases fork references that deleted children still owe", "Fork-reference debt from before the index is backfilled…") | Creates debt without a marker. | After the rollout completes, delete `registry/v4/fork-debt-backfill.json` so the backfill walks again (RUNBOOK.md §8). | Older binaries ignore markers; they stay until a newer binary pays them. |
| Conditional writes without client retries ("Registry conditional writes never mistake their own committed write for a refusal") | Can misreport its own committed conditional write as a refusal. | None beyond the defect staying reachable on older instances. | None. |

Performance acceptance is separate from these rules: the recount read-ahead
("The absorbed-byte recount reads ahead again…") and the postings-cache
bridging change carry measured costs (docs/PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md
§0) that still need workload acceptance.

## Release checklist (the short form)

1. `scripts/release-gate.sh` (fmt, clippy-vs-baseline, formal receipts
   `check --fresh`, suite, deny).
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
