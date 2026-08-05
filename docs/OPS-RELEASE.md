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
| carried behavior deltas | none in code; GC listing economics handled by static long sweep intervals (`GC_QUIET_INTERVAL_SECS` / `HISTORY_GC_INTERVAL_SECS`, default 600) | revisit if upstream adaptive cadence ships |
| conformance suite | `@durable-streams/server-conformance-tests` EXACT-pinned `0.3.6`, must equal `src/protocol_pin.rs` | bump only with a protocol-pin change and a full dual-surface rerun |

Rule: every carried patch exists as an upstream PR or a dated reason
in this table. A patch with neither is a defect in the ledger.

## 2. Security review (surface inventory + posture)

Reviewed surfaces and their enforcement points (audited across rounds
1–17; anchors in parentheses):

- **AuthN:** bearer token on every `/v1/*` route; route parsing is
  exact and happens BEFORE auth, auth BEFORE body buffering (#99).
  Negative-auth matrix in the suite covers every product route.
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
- **Bucket placement gate (2026-08-05):** single-region Prisma
  Buckets currently pin far from fra regardless of project or region
  hint; `put:wal` p50 27 ms (global) vs ~215 ms (single-region,
  mis-placed) — BUCKETS-SINGLE-REGION.md. No cell migrates to
  single-region buckets until placement follows the service region;
  re-measure then.

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

## Release checklist (the short form)

1. `scripts/release-gate.sh` (fmt, clippy-vs-baseline, suite, deny).
2. Full release suite + pinned DS conformance + field gate (unpaced
   AND paced) + installed-tarball SDK smoke.
3. Build musl, e_machine gate, upload with `.rev`.
4. WAN smoke on the retained edge (field gate or the scenario smoke
   relevant to the change — e.g. `scripts/consumer-saga-smoke.sh`).
5. `scripts/release-provenance.sh` → paste into
   RELEASE-PRODUCT-SURFACE.md → docs commit → tag.
