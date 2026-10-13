# The Compute lifecycle checklist

What the platform does to a shared cell when it stops, restarts, rolls back,
kills or keeps it awake, measured on the platform rather than assumed
(public-beta deploy-q5 design §3.4, triage item B6). `lifecycle.py` runs one
leg at a time against a deployed cell, keeps every piece of evidence, and
prints `PASS`, `FAIL`, `FACT` and `FINDING` lines ending in `LEG_PASS` or
`LEG_FAIL` (exit 0 only for `LEG_PASS`).

```bash
python3 bench/lifecycle/lifecycle.py check <leg> --plan PLAN.json   # offline: keys, placeholders, out dir
python3 bench/lifecycle/lifecycle.py <leg> --plan PLAN.json         # run it
python3 bench/lifecycle/lifecycle.py judge RUN/evidence.json        # re-judge a saved run, offline
python3 -m unittest discover -s bench/lifecycle -v                  # the tests (no platform)
```

Runs need a cell: the scratch project of owner decision D1 (deploy-q5 Q1,
decided (a) on 2026-10-12: Composer creates `streams-beta-scratch` in the
Personal workspace, eu-central-1) for the layout-5 rehearsal, then the RC
and the target for certification. A `FACT` is reported as measured; a
`FINDING` is a fact that changes the plan.

| Leg | What it does | Passes when |
|---|---|---|
| `d10-probe` (1) | Deploys [`sigterm-probe/`](sigterm-probe/README.md) into its own stage of the scratch app, redeploys it, then `compute versions stop`s the new version; reads both stops | the probe held the instance awake (`keep_awake=held`, as the kept-awake cell does); a SIGTERM arrives and the process lives at least 30 s after it (the binary's ordered stop bound); the redeploy runs a new artifact. Facts: PID 1 or not, process or group, seconds to the kill, old/new overlap |
| `d10-cell` (2) | Redeploys the cell under light k2gen load, kept running until `d10.tail_s` (60) after the deploy command returns however long it blocks; reads the old version's stop and the new one's boot; `/health` every 0.5 s | the wrapper's `wrapper received SIGTERM` line, the binary's ordered stop within 30 s, exit 0 with no kill, appends acked after the switch (windows that began after the new build's first `/health` 200 and the old one's SIGTERM) and `k2gen audit` clean (0 acked loss). An old/new overlap (instance clocks, the new process's shard open, clients seeing both boot ids) is a FINDING: F2's fenced-write 500 becomes reachable on one server |
| `restart` (3) | The SC1-awake redeploys (`restart.count`, default 3), timed by a raw product client: every 0.25 s a read of `restart.probe_path`, an append to `restart.append_stream` (created first; so a fenced write's 500 shows) and `/health`, whose boot id says which build answered the tick | every tick from a restart's trigger to the next one (or the cooldown's end) answers 2xx or `503` with `Retry-After`, not only those before the first 2xx: Composer's deploys are create-before-delete, so the old build answers after the new one's `listening` line and the switch comes later; clients are without service at most 10 s across the switch (the first failed tick to the next all-2xx tick; owner decision D4 (a) of 2026-10-12, this measure confirmed on 2026-10-13); the new build answers. Facts: when the new build first answered and the old one last did. The windows are a raw client's, so they understate D4's window to the first SDK success by up to one SDK retry backoff |
| `rollback` (4) | older -> newer -> older -> newer under load (each step's load kept until `rollback.tail_s` after its deploy returns), `/health` throughout, `cell-check` and an audit of every ledger so far after each step; then a copy of the last release whose `release.json` names another layout | every step deploys, checks and audits clean and its own build acks appends (windows that began after its boot id first answered `/health`), including the newer build's data read back by the older one (only once the newer build acked some); the cross-layout copy is refused, naming the layout, with the running boot untouched |
| `oom` (5) | **Scratch only, non-shipping config** (CHAOS-5's recipe: `ADMIT_RSS_SHED_MB=2000`, `ABSORB_GLOBAL_GATHERS=4`, incompressible produce); stops the load at the kill; always redeploys the shipping release after | the wrapper's code-137 exit, a new boot, 0 acked loss, one boot over the stable window (no crash loop). Fact: seconds to the first 2xx. Refuses the production stage and any plan without `oom.scratch: true` |
| `feeds` (6) | A quiet window, then an admit, a revoke, a malformed bundle and a good republish (operator publish commands) | no failed poll and no rewrite while nothing is published (each poll a 304: the presigned conditional GET works against the store); admit and revoke effects within 45 s (a revoke takes effect only at a 401 or 403 carrying the cell's revocation code, `credential_not_active` or `project_not_active` by default, `feeds.revoked_codes`: a transport failure or a 5xx is no answer); `feeds deleted` after 120-150 s without a good poll; `503 policy_stale` about 300 s after the deletion; service back within 45 s of the good republish |
| `boot-failures` (7) | Each case deploys a broken cell (a missing secret, a wrong-arch binary), then the good release. The new version is found by the versions before and after the deploy, whatever the deploy command's exit code (deploy-release.sh exits non-zero when its cell-check fails, which is these cases), and its log read; the exit code is a FACT. A missing secret must get past deploy-release.sh's preflight, which refuses it (triage B5): B5 has to provide that way past it, and until then the example's flag is a placeholder | a new version exists; the generic `binary_exited` 500 with nothing else in the body, the detail in the log (`expect_log`), and `/health` 200 again after the restore. Refuses the production stage |
| `keep-awake` (8) | `/health` every minute for SC0-1s-awake's 72 h (partial evidence saved hourly); counts `instance shape:` lines | the whole window observed with one boot id. A recycle without a recorded notice is a FINDING for Compute |

Leg 9, the target's platform export against the two profiles and `cell.env`,
runs with the effective-config family.

## The plan

`plan.example.json` names every key a leg needs; the optional timings,
probe paths and `compute_cli` take the defaults in `lifecycle.py` and
`harness.py`. Copy it outside the repository; `check`
refuses an `out` inside it (except under `target/`). Commands are argv lists
run without a shell, with `{stage}`, `{release}`, `{url}`, `{out}`, `{home}`,
`{ledger}`, `{ledgers}`, `{seed}`, `{duration}` and `{windows}` (where
`load.produce` writes k2gen's `--out` window lines) filled in per step. The
deploy commands are the deploy path's tools, `deploy/composer/deploy-release.sh`
and `deploy/composer/cell-check` (triage B5, not in this tree yet); the
example plan uses B5's paths and arguments. A same-release Composer deploy
is a no-op (triage D18), so `commands.restart` must produce a new version
of the same release, and the missing-secret boot failure must get past
deploy-release.sh's preflight: B5 as specified does neither, so the
example's `--restart` and `--without-secret` are placeholders for the
modes B5 must add (for example a deploy-nonce `envParam` that forces a new
deployment). Check every flag against B5 once it lands.

- **Secrets** come only from files the plan names (`platform_token_file`,
  `env_files`, header files): mode 0600, never on a command line, never in
  the evidence. Subprocesses get the operator's environment without any
  `PRISMA_*` name, plus `env_files`.
- **Evidence** (`<out>/<leg>-<utc>/evidence.json`, one log per command) holds
  the platform log lines the leg read. It stays on this machine and is never
  uploaded. A leg that fails (a platform step, a CLI timeout, anything) keeps
  what it collected so far beside its `error`, and its load jobs, log streams
  and probers are stopped before the file is written.
- **Clocks.** Binary lines carry the instance's time, wrapper and probe
  samples the collector's; windows that compare the two (restart, feeds)
  assume NTP-synchronised clocks.
- The compute CLI runs once alone (`--help`) before a leg's parallel log
  reads: parallel `bunx` calls race its package cache.
- **Log streams** outlive the CLI's 10-minute segments: a segment end
  (`segment_time_limit`), a retryable terminal record or a stream cut
  without one is resumed from its cursor (`compute logs <v> --cursor`).
  Only the version's own end (`vm_stopped`) is read as a kill; any other
  end of a probe's log is "not observed to the end".

## Files

`lifecycle.py` (the legs and the CLI), `verdicts.py` (pure verdicts; every
bar is a named constant with its source), `harness.py` (plan, commands,
compute CLI, HTTP probes), `test_lifecycle.py` (the verdicts and the
plumbing), `test_lifecycle_legs.py` (the legs against fakes), `plan.example.json`,
`sigterm-probe/`.
