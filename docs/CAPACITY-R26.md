# Capacity gate — pause/catch-up/restart with exact reconciliation (R26-11)

Run `cap-20260811T181546Z-98337`, ap-southeast-1 (Singapore), 2026-08-11/12.
Build: wrapper-verified sha `5a1c3b24…` (streams) / `8f184342…`
(awsbench) against the campaign manifest. Workload: 32 streams,
concurrency 64, 10 × 1 KiB records/request, FRAME_COMPRESS=1,
`LIMIT_RECS_PER_SEC=100000` (campaign posture — the ordinary limiter
deliberately out of the way), compute-1g memory profile,
INITIAL_SHARDS=4. Driver: `bench/soak/capacity-gate.sh`.
Artifacts: `$SOAK_HOME/results/cap-20260811T181546Z-98337/`.

## Verdict: PASS (criterion B), with exact zero-loss reconciliation

| Check | Result |
|---|---|
| Exact op-ledger reconciliation | **OK — acked 11,248,940 == durable 11,248,940 == walked 11,248,940**, 32 streams, 1,124,894 acked ops each exactly-once with all batch positions, 0 ambiguous, 0 problems |
| B: backlog bounded | **peak 34.4 MB instance / 11.6 MB max-shard vs 512 MiB / 256 MiB caps** — never approached; latch never engaged; server shed = 0; rate-limit refusals = 0 (typed counters, not inference) |
| A: catch-up ≥ 1.25× ingest | formally not established by the windowed counter measurement (1.16 across a counter-resetting restart); the ledger series shows 34.4 MB retired in ≤ 100 s against ~188 KB/s steady ingest — an implied ratio > 2 — see "measurement honesty" |
| Restart at max backlog | process replaced at ledger peak; **serving ≤ 1 s after the 51 s redeploy completed** (bound 300 s) — durable maintenance load + WAL replay inside the deploy window |
| Recovery window | ledger 0, no latches, **0.8 s** after ramp end (fixed-window check) |
| Errors | **0** across 1.6M+ requests |

## What the run measured

- **Steady state (90 min):** ~278 req/s ≈ 2,780 rec/s accepted;
  **exact frame-byte ingest 187.6 KB/s** (the all-'x' payloads compress
  ~15:1 — the same unit correction that retired the 9.4% artifact,
  visible live). Ledger oscillated 2–13 MB: absorption kept pace
  continuously, progress clock never aged.
- **Pause (183 s):** ledger grew linearly 8.9 → 34.4 MB, exactly
  ingest × time. No shed (nowhere near caps at this frame intensity).
- **Resume + restart:** the new process loaded the durable ledger and
  the absorber retired the full backlog within ~100 s.
- **Integrity through a fencing storm:** the restart leg created a
  version overlap — the generator kept the retired version's domain,
  which the platform answered with 503s (484,298 of them) once the old
  process was fenced/stopped. Every one was filed as a definitive
  rejection in the op ledger, and the exact walk still balanced to
  zero: no acked record missing, none duplicated, through fencing,
  restart, and edge 503s. This is the strongest integrity evidence the
  project has produced.

## Measurement honesty (read before quoting)

1. **PASS came via criterion B**, and criterion B was not stressed: at
   187 KB/s compressed-frame intensity, a 3-minute pause builds 34 MB —
   6.6% of the shard cap. The bound's shed behavior is proven
   deterministically (DST + the R25-H part-1 gate at 8/8), not by this
   run. The stressing field case is an INCOMPRESSIBLE workload: the
   same record rate would ingest ~2.9 MB/s of frame bytes, a 3-minute
   pause would build ~520 MB, and the caps + typed shed would be the
   thing holding the line. That run is the natural next experiment now
   that the harness can attribute every refusal.
2. **Criterion A's number (1.16) is a measurement artifact**, not a
   capability statement: the restart resets the process-local
   cumulative counters mid-catch-up, and the evaluator drops the
   negative delta, so the window mostly covers the paused (flat)
   segment. The durable ledger series — 34.4 MB to zero in ≤ 100 s
   against 188 KB/s live ingest — implies a catch-up ratio well above
   2×. Durable counters (or restart-surviving totals) would make A
   formally measurable across restarts.
3. **The post-restart load collapsed** (~25 req/s) because the
   generator targets the version-scoped preview domain captured at
   deploy time; the restart mints a new version. So the catch-up ran
   against reduced live load. Harness fix for next time: point
   generators at the service-stable domain, or redeploy/repoint the
   generator after the restart leg.

## Disposition

- CHAOS-5 is closed as originally stated: the 9.4% figure was the
  payload-vs-frame unit artifact; with exact units, absorption keeps
  pace at this profile and the durable bound + typed shed guard the
  regimes where it cannot.
- The supported-capacity number for INCOMPRESSIBLE frame intensity
  remains to be measured (item 1 above) — the harness is now capable
  of measuring it honestly.
- Teardown verified: services, bucket, project deleted; receipt
  retired.
