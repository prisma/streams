# sigterm-probe: what a Compute stop does to the instance's process

Leg 1 of the Compute lifecycle checklist (the public-beta deploy-q5 design
§3.4, triage item B6: the D10 platform-semantics questions). One Bun file,
`app/probe.ts`, importing only `node:*`:

- serves `200 sigterm-probe` on `$PORT` (default 3000) on `0.0.0.0`;
- starts one child process in its own process group, and never forwards a
  signal to it;
- on SIGTERM logs one line and then one line a second until it is killed;
  it never exits by itself. The child does the same on a SIGTERM of its own;
- holds the instance awake for its whole life, as the cell's entry does
  with `@prisma/compute`'s `KeepAwakeGuard` (`+` written to Compute's
  scale-to-zero control, `/uk/libukp/scale_to_zero_disable`, with the
  descriptor kept open): the cell is kept awake (edge #140), so a probe the
  platform could suspend while idle would answer for a sleeping service.

Every line is `sigterm-probe <main|child> <boot|SIGTERM|alive> t=<ISO-8601> k=v...`:

```
sigterm-probe main boot t=... pid=7 ppid=1 pgid=7 runtime=bun-1.3.13 port=3000 nonce="..." keep_awake=held pid1="/sbin/init"
sigterm-probe child boot t=... pid=9 ppid=7 pgid=7
sigterm-probe main SIGTERM t=... pid=7
sigterm-probe main alive t=... after=1s
```

The log answers D10's three questions: whether anything arrives at all (a
`SIGTERM` line, or none before the log ends); whether it reaches the process
or its whole group (the child logs a SIGTERM only when the platform signals
the group: the main process never forwards one); and how long the platform
waits before the kill (the last `alive` line). `pid1` names the instance's
PID 1, so a probe that is not PID 1 says what is. `keep_awake` is `held`
(the control was written), `unavailable` (no control file: not on Compute)
or `failed`; `lifecycle.py` fails the leg unless it is `held`. `nonce` is
the content of `app/nonce.txt`, which `bench/lifecycle/lifecycle.py` writes before each
deploy so that every redeploy is a new artifact (Composer does not redeploy an
unchanged service) and the versions can be told apart in their logs.

## Run it

Locally (the tests run it as a process-group leader and signal it the two
ways a platform could): `bun test ./bench/lifecycle/sigterm-probe`.

Through Composer: this directory is a Composer 0.29.1 app (`app.ts`,
`service.ts`, `prisma.config.ts`, exact pins in `package.json`). Its service is
a directory build of `app/`. `bench/lifecycle/lifecycle.py d10-probe`
deploys it into its own stage of the scratch app, redeploys it, stops it with
`compute versions stop`, and reads the logs; it needs the scratch project
(owner question D1) and installs nothing itself (`bun install` here first).
