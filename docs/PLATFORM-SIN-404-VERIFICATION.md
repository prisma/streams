# Platform verification: `ap-southeast-1` preview-domain 404s — fix confirmed, no shape reproduces

**From:** Prisma Streams team
**Date:** 2026-07-30, 11:31–13:15 UTC
**Re:** docs/PLATFORM-SIN-404-REPORT.md (2026-07-26/27: seven incidents,
four shapes, SIN-only). The Compute team deployed a fix and asked us to
attempt reproduction. **We could not reproduce any of the four shapes.**
Workspace: the service-token workspace provided for this exercise;
campaign `sinrepro-b26644c9f5b5`, all ids below for platform-side log
correlation.

## Protocol

Same wrapper apps, same deploy path (`bunx @prisma/compute-cli deploy
--http-port 8080`, `KEEP_AWAKE=1`), same region pairing as the original
campaigns; `ap-northeast-1` deployed identically as the same-day
control.

1. **Boot cycles (Shapes A + B):** one fresh-service deploy plus five
   consecutive redeploys into the *same* SIN service — the exact
   sequence that historically zombied back-to-back (original incidents
   3→4). Per cycle we measured deploy→`running` and deploy→first byte
   *from our app* off the preview domain, polling every 5 s, 600 s cap,
   classifying platform-HTML 404s separately from app responses.
2. **Load ramps (Shapes C + D):** two full 30-min closed-loop ramps
   (10 tiers, conc 1→64) through the SIN server's preview domain, with
   the generator's own inbound domain polled for platform 404s, version
   identity re-checked every ~2 min, and process continuity established
   from the generators' retained in-memory sample history (a replica
   swap or restart empties/forks it — the signature that caught the
   original Shape D).
3. **Focused inbound probe:** 60 consecutive GETs per gen domain at 3 s
   cadence post-ramp, status-code logged.

## Results

**Shape A (zombie boot, `running` + 404 forever): not reproduced.**
6/6 SIN boots served an app byte in 48–53 s with **zero** platform 404s
observed at any point between deploy and first byte:

| SIN cycle | version | domain | first app byte |
|---|---|---|---|
| 0 (fresh service) | `cpv_qfhlrsikbrj88smsp4u44kgp` | `cv-d46bb4432771.sin` | **49 s** |
| 1 (redeploy) | `cpv_ky3v7l35463qtqz0gxpo6sfn` | `cv-32ac59784bb4.sin` | 49 s |
| 2 (redeploy) | `cpv_cwmmvkxl1xcbhgmguquzplc2` | `cv-8dc92cfbd2e6.sin` | 53 s |
| 3 (redeploy) | `cpv_kfa83697cosevdj3gl11fvgl` | `cv-609a46b17de8.sin` | 48 s |
| 4 (redeploy) | `cpv_cagvbs8ashuxx08510uwsz2x` | `cv-d85d0a7d6432.sin` | 49 s |
| 5 (redeploy) | `cpv_o9j96ydldro8hp3bzu7vs3f3` | `cv-a68ae1a5d690.sin` | 51 s |

**Shape B (5–8 min SIN boots): not reproduced.** SIN first-byte
48–53 s vs control NRT 45–46 s (3/3 boots: `cpv_kpyg297ds0y147gz18uuuntq`,
`cpv_kelm8ggl70vxoau4qyxyc64y`, `cpv_nz59hv96bl7lz3yftrndhsys`). The
original 5–10× SIN boot penalty is gone; SIN is now within seconds of
the healthy-region baseline.

**Shape C (inbound 404 while the app runs): not reproduced.** The
closed loop itself is a continuous in-region probe of the server's
preview domain: **379,185 requests** (181,599 + 197,586 across the two
ramps) flowed through `cv-a68ae1a5d690.sin.prisma.build` with
**errs = 0**. The generator's own inbound domain answered every polled
request during ramp 2 with no "Service not found" body, and the focused
probe scored **120/120 HTTP 200** (60 per region) with zero platform
404s.

**Shape D (mid-run replica flap under `KEEP_AWAKE=1`): not
reproduced.** Across both ramps and both regions: running version ids
never changed mid-run (re-checked every ~2 min), and every generator
finished with its complete uninterrupted 90-sample history, cumulative
counters strictly monotonic — one process, one replica, end to end.
Client-acked vs server-durable records matched to the usual final-window
offset (+640 records ≈ 64 in-flight requests) in both regions.

## Method notes, for honesty

- Ramp 1's live watcher crashed at startup (our bash-3.2 portability
  bug), so its Shape-C/D evidence is the closed-loop request stream
  (server inbound), the retained sample history, and version-identity
  checks before/after — not live 20 s polling. Ramp 2 ran with the
  fixed watcher end-to-end. A sub-20-second *transient* gen-inbound 404
  during ramp 1 is the one thing this protocol cannot strictly exclude;
  every persistent form (the original incidents held for 10+ minutes to
  hours) is excluded many times over.
- Boot classification counts only platform-HTML 404s ("Service not
  found") as routing failures; our supervisor's crash diagnostics would
  have been visible as app bytes and never appeared — the binaries ran
  clean.

## Ids for your logs

- SIN: project `proj_daf0nkohw2wt5w772dc9t2wx`, server service
  `cps_a0dgcaopumnu26ijyth1u9dt` (6 versions above), gen service
  `cps_lxlafqx5romfkgp3kx82ivow` (versions
  `cpv_j3mfuwto3rpgjeh79ivpjrdx`, `cpv_miaees60lfuydvs7j0umw1jx`).
- NRT control: project `proj_sgd3s8xwgo6vrt7b1d2vfirc`, server service
  `cps_iw4h9gkskdp3c2seq9hh2z0h`, gen service
  `cps_j90jr4n1zxx4tt1fm348k7v4` (versions
  `cpv_io9caihf8hfqd8l6cpul7vpv`, `cpv_vi9zphqgb8u9re25wf8t58dd`).
- Windows (UTC, 2026-07-30): boot cycles 11:31–11:39; ramp 1
  11:39–12:10 (unwatched live, reconstructed); ramp 2 12:30–13:06
  (watched); focused probe 13:09–13:15.

## Verdict and state

**11 SIN version boots, two 30-min load runs, and ~380k in-region
requests produced zero occurrences of any reported shape.** From our
side the fix holds under exactly the workload that surfaced the
original incidents at a ~70 % per-deploy hit rate. No specimen to
preserve; both repro projects were torn down after this verification
(campaign RUN_ID guard, teardown verified via the management API).
The §7 asks in the original report — a "route programmed" readiness
signal distinct from `running` — remain interesting to us regardless,
since polling for HTML 404s is still our only way to catch a
recurrence.
