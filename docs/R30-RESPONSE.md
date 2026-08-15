# R30 Response — Round Report

**Scope.** Response to the R30 review of the consumer-saga/custody line
("strong RC for continued private preview"), which asked for three code
fixes, a documentation pass, and — as Phase 2 — an exact-binary
certification of `v0.2.0-rc.1`: build the release candidate once and run
the capacity and fleet-handoff field gates against that identical
artifact. The tenancy direction (many projects per cell) was to be
recorded as decided, **not** implemented; the implementation plan is
being prepared separately by Søren.

**Outcome.** **CERTIFIED.** Both field gates passed against the one
identical artifact
`b91b3cdab4bfdb10fdb982a38a78b5dd7b9e30d37c9a6f93ececaa3ff297452c`
built at `bee7cc82`, `rc-certify.sh` reported `RC_CERTIFY_OK`, and
**`v0.2.0-rc.1` is tagged at `bee7cc82`** (2026-08-15). The harness
hardening that carried the campaign landed as post-tag commits per the
preview.9 three-commit-provenance pattern; details in
`docs/RELEASE-rc.1.md`.

Baseline: `9af3f23e` (preview.9 tag) → `bee7cc82` (tag), 12 commits.

---

## 1. Review findings → disposition

| # | Finding | Disposition |
|---|---------|-------------|
| R30-1 | Tombstone-walk fairness: budget-deferred walk restarted from the top each sweep; shards behind a persistently occupied budget never got visited | **Fixed** (`6b80b1d0`). `SweepSched.walk_cursor` continuation: the walk stops on budget deferral, resumes AT that page next sweep, wraps to the top only on exhaustion. Quantum eviction rotates budget occupants out so the cursor actually advances. |
| R30-2 | Scheduler close empty-slot race: `close_scheduler_engine` removed the map entry before the CAS decision, leaving a window where a concurrent open minted a second engine for the same shard | **Fixed** (`6b80b1d0`). Close is now a single write-guard critical section: `remove → CAS on the installer's custody value → reinsert-on-CAS-failure`. No observer ever sees an empty slot for a live shard. |
| R30-3 | Release-gate clippy false-green: `cargo clippy \| tee` pipeline made the gate report the tail's exit status; warning-count baseline was a warm-cache artifact | **Fixed** (`6b80b1d0`). Direct invocation (no pipeline), and count-gating replaced with fingerprint gating: `scripts/clippy-fingerprints.py` (message::file, deduped) diffed against a reviewed 153-entry baseline via `comm -13`. New fingerprints fail; count noise cannot. |
| R30-4 | One certification driver that proves what it ingests | **Done** (`6b80b1d0`, `cf59f748`). `scripts/rc-certify.sh` derives RC identity from the capacity campaign's own manifest (sha + `gitCommit == git rev-parse HEAD`), then requires capacity verdict PASS + reconcile OK + handoff `binary.sha` match + restore ≥ 1.0 + handoff reconcile OK. `SOURCE_DATE_EPOCH` threads one build timestamp through `build.rs` and the upload manifest so "same binary" is byte-checkable (`cf59f748` fixed the first verify failure, a compile-clock vs upload-clock mismatch). |
| R30-5 | Docs: readiness matrix, tenancy decision | **Done** (`6b80b1d0`, pre-freeze). `docs/READINESS.md` records launch-scope verdicts, the tenancy DECISION (many projects per cell; per-request principals + tenant-scoped quotas are the shared-cell GA blockers), and the platform-side blocker list. No tenancy implementation was started, per instruction. |

DST gates added for R30-1/2 (suite at 360+, green):

- `tombstone_walk_fairness_under_occupied_budget` — pins maintenance
  debt on 2 shards via the absorb-pause flag, TTL-expires others, and
  requires the walk to advance `WALK_CLOSE_SUBMITS` by 2 within ≤14
  sweeps despite the pinned budget.
- `revoked_close_keeps_the_identical_engine_with_no_new_open` —
  `ptr_eq` on the engine across a revoked close is the no-second-open
  proof, scoped to custody == 0.

Version: `3f043c8f` bumps Cargo + SDK to `0.2.0-rc.1`.

---

## 2. The technical finding of the round: the R29 trade, measured

The first full-length certification run (`cap-20260814T101514Z-73709`)
**failed the gate honestly** and, in doing so, surfaced the one real
product-behavior change in this round.

What the verdict showed: peak ledger 390MB = 72.7% of cap (under the
75% stress line), `maintenance_shed_total = 0`, `rate_limited_total =
0`, `catchup_retire = 0` — yet awsbench had counted **74,344 server
throttles** and recovery cleared in 0.3s. Interrogating the preserved
server's cumulative counters resolved the contradiction:
`admit_shed = 74,344` exactly. Every refusal was the **RSS/phys-footprint
admission gate** — the OOM-review survival mechanism — not the
maintenance bound and not the rate limiter.

Why it differs from R27-4 (which peaked at 100.6% with the same 300s
pause): R29 closed the release blocker where history DBs ran the
upstream compactor defaults (4 workers, 4×2MiB read-ahead, 256MiB
rolls). The constrained all-DB profile retires history more slowly, so
RSS sits closer to the admission line under incompressible overload,
admit_shed fires earlier, and **accepted ingest drops ~10%**
(1.33 vs 1.47MB/s steady). The old gate shape could no longer push the
durable ledger to its cap before the pause ended.

This is the intended trade — bounded memory over throughput headroom —
now measured: **~10% steady ingest on a 1GiB cell under incompressible
overload**, with the process surviving (no reset, flat read p50s)
where the pre-R29 binary OOM-killed. It belongs in the rc.1 release
notes as an envelope change.

A second, independent measurement artifact: the post-pause drain now
clears 300+MB in ~2 minutes (64MB absorb passes), faster than the 30s
poll cadence — criterion A computed `catch_rate = 0` because fewer than
3 qualifying samples landed during the drain. The drain outran the
sampler, not the other way around.

### Gate reshaping (criteria unchanged, shape harsher, reporting more honest)

- **Pause 300s → 600s** (`SOAK_CAP_PAUSE_SECS=600`): at the measured
  ledger growth (~1.05MB/s during pause) the bound is reached at
  ~450s and then *held* — restoring the shed-pinned phase criterion B
  requires.
- **Dense catch-up sampling** (`b9705720`): 10s polls for 600s after
  pause-end, so criterion A can see a fast drain.
- **`admit_shed_total` in the verdict** (`b9705720`): report-only; the
  survival stack's pushback is now visible even when it is not the
  criterion under test. The patched evaluator was replayed against the
  failed run's data as validation (it reports the 74,344 and still
  fails that run).

---

## 3. Capacity certification: PASS

Run `cap-20260814T140228Z-23776`, Singapore, conc=64, incompressible
payloads, 90-min window, 600s absorber pause at +3600s, no restart leg,
20-min recovery. Binary `streams-cap-20260814T140228Z-23776-x64`,
manifest `gitCommit = bee7cc82`.

| Metric | R27-4 (preview.9) | rc.1 run 23776 | Gate |
|---|---|---|---|
| Steady accepted ingest | 1.47 MB/s | 1.62 MB/s | — |
| Catch-up retirement | 2.27 MB/s (1.545×) | 3.50 MB/s (**2.16×**) | ≥1.25× (A) ✅ |
| Peak ledger vs 536MB cap | 100.6% | **101.5%** (≤1.05 allowance) | stressed + held (B) ✅ |
| Maintenance shed (typed) | fired | **67,726** | >0, rising in band ✅ |
| Contiguous stress band | — | **350s** | ≥300s ✅ |
| admit_shed (RSS admission) | not reported | 99,974 | report-only |
| Rate limiter | silent | silent (all zeros) | must be 0 ✅ |
| Process resets | 0 | 0 | must be 0 ✅ |
| Probe failures (reads/catalog during pause) | 0 | 0 | must be 0 ✅ |
| Recovery after load stop | 0.8s | **0.7s** | in window ✅ |
| Op-ledger reconcile | OK | OK (exact) | OK ✅ |
| Pause wall | 309s | 604s | declared |

**Both criteria passed independently.** The run also demonstrates the
two shed mechanisms operating in their intended order: admit_shed
carves bursts at the RSS line during ramp and catch-up, while the
durable maintenance bound takes over at the ledger cap during the
pause — caps held throughout, reads stayed available, and the exact
frame-byte reconciliation is clean end to end.

---

## 4. Campaign operations: nine launches to one verdict

2026-08-14 delivered a control-plane outage (~09:00–15:56Z,
data plane unaffected), an afternoon of minutes-long network waves, and
finally a platform-token expiry. Each failed launch exposed a harness
fragility that is now fixed and committed. Abridged ledger:

| Run / event | Failed at | Root cause → fix (commit) |
|---|---|---|
| `…075027Z-49396` | pause step | control curl died under `set -e` during the outage → retry-harden control curls (`3f0bbd50`); run unrecoverable, torn down |
| api.prisma.io outage | — | recovery watcher armed; LB redeploy deferred |
| `…085647Z-57240` | provision | stale receipt: teardown had died mid-loop before receipt retirement → (see `82c17815`) |
| `…101514Z-73709` | **evaluate (honest FAIL)** | the R29 trade + sampling gaps (section 2) → `b9705720` + 600s pause |
| `…122813Z-11739` | pause-on | transport blip, curl exit 7 under `set -e` → `ctl()` bounded until-200 loops for pause controls, probes record `000` instead of dying, retarget same (`0f38edf2`) |
| `…133335Z-17526` | provision | 11739's receipt survived another teardown death → teardown survives per-region failures; receipts retired **only** on confirmed 2xx/404 delete (`82c17815`) |
| `…133644Z-18220` | server deploy | transient CLI failure died silently under pipefail → deploys retry 6×30s and print CLI output (`9089364c`) |
| wrapper #1: `18915/19441/19800/20156` | server / provision | network wave burned all tries; a partial provision's receipt wedged the pipeline → `capacity-retry.sh` verifies receipts clear between tries (`b42d29e5`) |
| wrapper #2: `21522/22142/22695` | server / provision | wave again; 22695 provisioned but never deployed → undeletable by its own run id (stamp only written at deploy) → provision stamps projects **at creation** (`bee7cc82`); wrapper gained a network-stability gate (two API 200s, 20s apart, before each try) |
| wrapper #3: `23370`, then **`23776`** | provision (1 try), then **PASS** | ran to verdict unattended |

Two traps recorded for posterity: the Compute CLI reports an expired
token as *"Unable to connect. Is the computer able to access the
url?"* — check `services list` under the token before blaming the
network; and zsh equals-expansion kills any command with a bare `===`
argument (`(eval): == not found`).

Possible residue: one orphaned empty project from `…135116Z-19441`
(its receipt was clobbered by a later region's teardown before the
stamp fix). A `streams-cap-*` project sweep against receipts will
catch it; non-gating.

---

## 5. Handoff leg: PASSED (post-token, post-flap)

> Written mid-campaign while blocked; kept for the record. The token
> was refreshed, and after a second campaign of local network-flap
> waves (a flapping path to Cloudflare-fronted hosts: platform API and
> Tigris refused connects in ~10ms at the local gateway for minutes at
> a time), run `handoff-fh033626` passed on the certified binary:
> target owner aborted at **342MB** durable backlog, replacement
> restored **8/8 shards to exactly 100%** of frozen pre-kill gauges
> (ratio 1.0), caught up under continued load, drained to 5MB after
> load stop, LB-routed reconcile walked 38,070 records with zero
> problems. The flap waves exposed and fixed five more harness
> fragilities (empty-output cache clobbers in record_svc/resolve_url,
> empty SELF_URL env, single-probe corpse preflight, an unguarded
> json.loads on the binary-identity fetch, unguarded pause/abort
> curls) — committed post-tag. One pre-existing DST test
> (`sweep_residency_bound_rotates_over_many_indebted_shards`) flaked
> once under parallel suite load during certification and passed
> 3/3 isolated + 360/360 on the full rerun; a deflake task is filed.

### (historical) blocked on operator input

The R27-5 handoff gate (fleet-health preflight, all-absorber pause,
exact ≥100% gauge restore, two-phase drain, binary.sha stamp,
reconcile) is ready to run against the certified artifact. Three
earlier attempts on the superseded binary validated the gate machinery
itself: attempt 2 passed every field criterion (265MB durable backlog
handed off, restore 1.0) and failed only on local ephemeral-port
exhaustion during reconcile — since fixed (`69e61688`), with corpse
preflight added (`a44e5d3d`).

**Blocker:** `$SOAK_HOME/platform-token.txt` expired ~16:20Z. The
CLI's stored login is a different team (the fleet project is not
visible in it), so no platform operation — fleet redeploy, teardown,
provisioning — can proceed. Fleet state: services intact (s4 answers
200 on the old sha; s1–s3 URL files clobbered by the failed deploy
attempts; LB down since the outage).

A watcher polls the token file every 5 minutes. On refresh, the queued
sequence runs unattended:

1. Retry-gated fleet redeploy — `BIN_TAG=cap-20260814T140228Z-23776`,
   `PILOT_TAG=rc1h`, fresh `FLEET_DATA_PREFIX=fleetd7` (servers + LB).
2. `bench/fleet/handoff-gate.sh` (250MB peak target).
3. `scripts/rc-certify.sh --capacity-run cap-20260814T140228Z-23776
   --handoff-run <id>` — verifies one identical artifact across both
   legs and `manifest.gitCommit == HEAD == bee7cc82`.
4. On `RC_CERTIFY_OK`: tag `v0.2.0-rc.1` at `bee7cc82`, push, then
   land the release report + this document as post-tag commits.

**Freeze discipline:** no commits until step 4 — rc-certify's identity
check binds the artifact to HEAD, so any commit invalidates the
passing capacity leg. (One consequence: the fleet-deploy retry
hardening currently lives inline in the launcher; it gets committed
post-tag.)

---

## 6. Explicitly out of scope this round

- **Many projects per cell** — decision recorded in
  `docs/READINESS.md`; implementation awaits Søren's plan. Nothing was
  built.
- **R23-9** (hostile + SIGKILL rerun on the final binary) — partially
  folded into the release battery; full rerun remains open,
  non-gating.
- **#197** (fork ReadIoMetrics through scan/fetch) and **#108**
  (simulator substrate) — deferred, unchanged.
- Platform-side GA blockers (per-request principals, tenant quotas,
  control-plane reliability) — tracked in READINESS, not ours to fix
  from this repo.
