# v0.2.0-rc.1 — Exact-Binary Certified Release Candidate

**Tag:** `v0.2.0-rc.1` at commit `bee7cc82` (2026-08-15).
**Binary:** `b91b3cdab4bfdb10fdb982a38a78b5dd7b9e30d37c9a6f93ececaa3ff297452c`
(x86_64-musl, campaign artifact `streams-cap-20260814T140228Z-23776-x64`,
manifest `gitCommit == bee7cc82`, `SOURCE_DATE_EPOCH`-pinned build time).
**Certification:** `scripts/rc-certify.sh --capacity-run
cap-20260814T140228Z-23776 --handoff-run handoff-fh033626` →
`RC_CERTIFY_OK`. Both field gates ran against this identical artifact —
the R30 Phase-2 requirement.

## Certification record

### 1. Local release gate
`RELEASE_GATE_LOCAL_OK`: cargo fmt clean; clippy fingerprint gate
(zero fingerprints beyond the reviewed 153-entry baseline; no
pipelines, no count heuristics); test suite 360/360; cargo deny
(advisories/bans/licenses/sources) ok. One pre-existing DST test
(`sweep_residency_bound_rotates_over_many_indebted_shards`) flaked once
under parallel suite load during the first certify invocation and
passed 3/3 isolated plus 360/360 on the full rerun — a deflake task is
filed; the timing-source pattern matches the known absorber-timing
trap, not a binary regression (no src change since the last green
suite).

### 2. Capacity gate (SIN, run `cap-20260814T140228Z-23776`)
Incompressible payloads, conc=64, 90-min window, **600s** absorber
pause at +3600s, no restart leg, 20-min recovery, on the production
`compute-1g` memory profile.

| Verdict field | Value | Gate |
|---|---|---|
| Steady accepted ingest | 1.62 MB/s | — |
| Catch-up retirement | 3.50 MB/s = **2.16×** steady | ≥1.25× (criterion A) ✅ |
| Peak ledger | 544.7MB = **101.5%** of 536MB cap | stressed ≥75%, held ≤105% (criterion B) ✅ |
| Maintenance shed (typed) | 67,726, rising inside the band | >0 in band ✅ |
| Contiguous stress band | **350s** | ≥300s ✅ |
| admit_shed (RSS admission) | 99,974 | report-only |
| Rate limiter | all zeros | must be 0 ✅ |
| Process resets / probe failures | 0 / 0 | must be 0 ✅ |
| Recovery after load stop | 0.7s | in window ✅ |
| Op-ledger reconcile | OK (exact) | OK ✅ |

Both criteria passed independently. The two shed mechanisms operated
in their intended order: RSS admission carved bursts at the phys line
during ramp/catch-up; the durable maintenance bound took over at the
ledger cap during the pause; reads and catalog stayed available
throughout.

### 3. Fleet handoff gate (fra, run `handoff-fh033626`)
Four instances + pilot LB on the identical binary (`/readyz` git
`bee7cc82` verified on every instance pre-gate), fresh `fleetd7` data
prefix, all absorbers paused (frozen gauges), target owner aborted via
`/v1/debug/abort` holding **342MB** durable backlog.

- **Gauge restore: OK — 8/8 shards at exactly 100% of pre-kill**
  (ratio 1.0; monotone comparison against frozen gauges).
- Catch-up under continued load back to the steady band; absolute
  drain to 5MB within ~60s of load stop.
- LB-routed exact reconcile: 3,807 acked ops, 38,070 records walked,
  0 problems (2,594 ambiguous ops, 0 landed-ambiguous).
- `binary.sha` stamp == RC sha (enforced by rc-certify stage 4).

## Envelope change to note (from the R29 memory fix)

The memory-bounded all-DB compactor profile (the R29 release blocker
fix: history DBs no longer run upstream compactor defaults) costs
**~10% steady accepted ingest on a 1GiB cell under incompressible
overload** — RSS sits closer to the admission line, so `admit_shed`
fires earlier (99,974 refusals in the certifying run, now a verdict
field). This is the intended trade: the pre-R29 binary OOM-killed
(exit 137) under this exact load; this one survives with flat read
p50s and zero resets. The capacity gate's pause was extended
300s→600s to keep driving the maintenance bound to its line under the
lower accepted ingest — criteria unchanged, shape harsher.

## Provenance pattern

Three-commit provenance, as with preview.9: the tag sits on
`bee7cc82` (the exact commit the certified artifact embeds and the
manifest records); the harness hardening that carried the campaign
(`2265fb12`) and this report land as post-tag commits. The rc-certify
stage-3 `srv` NameError fix is in the post-tag harness commit — the
certifying checks themselves (verdict PASS, reconcile OK, sha match,
commit provenance) were unaffected; only a print statement was.

## Campaign operations (abridged)

Certification consumed ~14 launches over 2026-08-14/15 against a
control-plane outage, an expired platform token (the CLI masks the
401 as "Unable to connect"), and recurring waves of **local
ephemeral-port exhaustion**: a concurrent `phase2-cost-gate --profile
full` benchmark (separate repo) held ~16.5k connections to its
loopback S3 daemon — 13,206 of them SYN_SENT — against macOS's
16,384-port ephemeral pool, so every local `connect()` failed with
EADDRNOTAVAIL for minutes at a time, indistinguishable in curl exit
codes from the remote hosts being down. The waves were initially
misread as a flapping network path (the tag-time commit messages say
so); corrected 2026-08-15 after Søren's diagnosis — even connects to
the LAN gateway failed with "Can't assign requested address", which
no router issue produces. The reconcile Errno 49 backoff (`69e61688`)
had already treated the same phenomenon at smaller scale, self-inflicted
by our own walker. Diagnostic lesson: before blaming the network,
count the port pool (`netstat -an -p tcp | grep -c SYN_SENT`).
Every failure hardened the harness:
kill-proof capacity pause controls, confirm-before-retire receipt
teardown, stamped-at-creation projects, retrying deploys that print
CLI output, no-clobber service/URL caches, corpse-vs-flap preflight,
JSON-validated instance polls, until-200 pause controls, and the
`capacity-retry.sh` / step-retry wrappers. Full ledger in
`docs/R30-RESPONSE.md` §4–5.

## Open, non-gating

- R23-9 hostile/SIGKILL rerun on the final binary (partially in the
  battery), #197 fork ReadIoMetrics, #108 simulator substrate.
- DST deflake task for `sweep_residency_bound_rotates_over_many_indebted_shards`.
- One possible orphaned empty campaign project from
  `cap-20260814T135116Z-19441` (sweep `streams-cap-*` vs receipts).
- Platform-side GA blockers unchanged (per-request principals,
  tenant quotas); tenancy implementation awaits the separate plan.
