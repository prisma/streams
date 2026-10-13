# Compute wrapper apps

Prisma Compute runs a Bun app; these download the Rust binary from the
artifacts bucket and exec it. They live in the repo because a deploy must
be reproducible from a clean checkout — these files previously existed
only in scratch directories, and one was deleted by a cleanup mid-campaign
(docs/STAGING.md B1).

| dir | binary | role |
|---|---|---|
| `app-server/` | `streams-slate` (`SERVER_BINARY_S3_KEY`) | stream server / fleet member |
| `app-lb/` | `pilot` (`LB_BINARY_S3_KEY`), `MODE=lb` | rendezvous-hash router; the only client entry point |
| `app-gen/` | `awsbench` (`AWSBENCH_S3_KEY`) | in-region load generator for soaks and benchmarks |

Six more directories:
- `profiles/` holds the environment files deploy scripts pass as `--env`
  lines: `compute-1g.env` for every server on the 1 GiB class, and
  `shared-cell.env` on top of it for a single-server shared cell.
- `cell-admin/` is the shared-cell operator tool: it admits, lists,
  suspends and offboards projects, mints design partners' tokens, changes
  the cell's keys and writes the cell's feed bundle, from a state in a
  local directory or in buckets (`--state s3://...`), which the interim
  token issuer shares. Its guide is [cell-admin/README.md](cell-admin/README.md).
  Tests: `bun test ./deploy/cell-admin`.
- `issuer/` is the public beta's interim token issuer (`main.ts`, a Bun
  service of its own, not a wrapper), working from the state cell-admin
  keeps in the control bucket: it exchanges a customer credential's
  secret for a cell token at `POST /v1/token/streams`; serves the beta's
  page at `GET /`; signs a browser in with Prisma at `/auth/login`,
  `/auth/callback` and `/auth/session`; serves the management API, a
  signed-in workspace's projects under `/v1/workspaces/{w}/streams/projects`
  and their credentials under `/v1/projects/{p}/streams/credentials`; runs
  the audit, the pepper re-hash and the offboarding job; and answers
  `GET /healthz` and `GET /v1/admin/metrics` for the watch. `server.ts`
  lists the routes and WIRE-MATRIX section 5 states each; tests and
  `prisma dev` sign in against the mock identity provider, `mock-idp.ts`.
  Its inputs are `config.ts`'s `INPUTS`, read from its environment and
  nothing else. It runs as one bundled file with `@prisma/compute` pinned
  by `package.json` and `bun.lock` (`bun install --frozen-lockfile`),
  started with `bun --no-install` (`main.ts`'s header).
  Tests: `bun test ./deploy/issuer`.
- `composer-issuer/` is the issuer's Composer app (`app.ts`, module
  `streams-issuer`, so a Compute project of its own; the `issuer` service
  in `issuer/service.ts`, no Composer port). It binds each of the
  issuer's inputs under its own name: the secrets and the two bucket key
  pairs as `envSecret`, the rest as `envParam`, never a `generatedParam`.
  Its entry (`issuer/entry.ts`) hands the issuer the service's input and
  `PORT`, and carries the keep-awake guard. A release stages the app in a
  directory of its own: this directory's `package.json`, `bun.lock`,
  `prisma.config.ts`, `app.ts` and `issuer/service.ts`, `bun install
  --frozen-lockfile` there, and `bun build
  deploy/composer-issuer/issuer/entry.ts --target=bun --format=esm
  --outfile <release>/issuer/entry.js` from the repository (with this
  directory's packages installed); `prisma deploy app.ts` (or `prisma dev
  app.ts`, with the mock identity provider's origin in
  `STREAMS_ISSUER_OAUTH_AUTHORITY`) runs there, the settings in its
  environment and the secrets set on the platform. Tests:
  `deploy/issuer/composer.test.ts`; the bundled entry's are opt-in: `bun
  install --frozen-lockfile` in `deploy/composer-issuer`, then
  `ISSUER_COMPOSER_E2E=1 bun test ./deploy/issuer/composer.test.ts`.
- `composer/` is the shared cell's Composer app (`app.ts`, the
  `streamscell` service in `streams/service.ts`, no Composer port). Its
  entry (`streams/entry.ts`, bundled into `entry.js` when a release is
  cut) verifies the release's own `streams-slate` against `release.json`,
  builds the binary's whole environment with `streams/env.ts` from the two
  profiles, the stage's `cells/<stage>.env`, the seven secrets by their
  platform names, the binary's sha256 and `PATH`, `HOME`,
  `SSL_CERT_FILE`, `SSL_CERT_DIR`, holds the keep-awake guard, keeps the
  feed bundle current and supervises the binary with `supervise.ts` below;
  any refusal answers `$PORT` with the generic 500 and logs the reason.
  `prisma deploy` runs from a release directory's root, never from here
  (public-beta deploy-q5 triage B2 and B5). Tests: `bun test
  ./deploy/composer`; the end-to-end test is opt-in: `bun install
  --frozen-lockfile` in `deploy/composer`, then
  `CELL_ADMIN_E2E_BIN_DIR=target/release bun test ./deploy/composer`
  (its good boot runs on an x86_64 Linux host only).
- `cell-watch/` is the shared cell's alerting watch (the public-beta
  alerting design). So far it holds `rules.ts`, a pure step from the
  previous state and one poll of the cell's `/health`, `/operator/data.json`
  and `/operator/billing.json` and the issuer's `/healthz` and metrics to
  the next state and the trigger and resolve events, one rule per row of
  the design's table, and its tests against answers captured from a local
  cell (`fixtures/`). Payloads carry named numbers and fixed enums only.
  RUNBOOK's shared-cell "What to alert on" table lists the same rows with
  their rule, class and destination, and `runbook.test.ts` holds the two
  together.
  `poll.ts` fills that poll each 15 s cycle with GETs of those five paths
  and no other: `/health` on a fresh connection, the two operator
  documents with the deployment bearer on one kept-alive connection
  (billing every fourth cycle), the issuer's when `ISSUER_ORIGIN` is set;
  5 s timeouts, no redirects followed; and, beside them every fourth
  cycle, the canary's round (`canary.ts`) when the watch holds its
  credential. `sinks.ts` delivers the events, in
  order and retried until delivered, to the one configured sink as
  PagerDuty Events v2 or a generic JSON webhook, on alert keys that carry
  the class (a NOTIFY rising to PAGE pages), and sends the heartbeat GET,
  which it withholds after three cycles in a row with a trigger
  undelivered. `watch.ts` runs the cycle (poll, rules, delivery, heartbeat)
  every 15 s with its state in memory only, and `serveOk` answers `ok` to
  `GET /` on `$PORT` and nothing else. `canary.ts` is rows 19 and 22's
  probe, made as a customer makes it: it exchanges the canary project's
  hidden credential at the issuer's public `POST /v1/token/streams`, keeps
  the token until two minutes before it expires (exchanging every round
  after an exchange that failed or answered another `endpoint`, until four
  in a row are good), appends one record to the project's one stream and
  reads it back durable, and reports each step's status, `error.code` and
  origin header only. Their tests run against local stand-ins
  (`fake-cell.ts`); `canary.e2e.test.ts` runs the canary against the real
  binary over s3lite and the issuer's own exchange
  (`CELL_WATCH_E2E_BIN_DIR=target/debug`, opt-in). The directory is also
  the watch's own Composer app (owner answer I1 (a)), deployed into a
  Compute project of its own so that it holds no secret of the cell's or
  the issuer's: `app.ts` mounts `module.ts`, which binds the one service
  (`service.ts`) to the watch's eleven inputs by their platform names
  (`config.ts`: five envSecrets, six envParams; an optional one is `none`
  when absent); `entry.ts`, bundled into `entry.js` before any `prisma
  dev` or `prisma deploy` (`service.ts` gives the command), hands them
  over, holds Compute's keep-awake guard and runs the watch (`run.ts`).
  Composer 0.29.1, the CLI and `@prisma/compute` are pinned exactly, and
  `node_modules` comes from `bun install --frozen-lockfile` here. Tests:
  `bun test ./deploy/cell-watch`; the bundled entry run out of the tree
  (`CELL_WATCH_COMPOSER_E2E=1`, packages installed) and the app in
  `prisma dev` against a local cell (`CELL_WATCH_E2E_BIN_DIR=target/debug`,
  `dev.e2e.test.ts`) are opt-in.

All verify the download is an **x86_64 ELF** before exec. That check is
not politeness: an aarch64 binary deploys "successfully" and crash-loops
into a silent platform zombie (service domain 404, `versions list` says
running, no logs retrievable). All also re-download every boot, because a
warm instance keeps `/tmp` across versions and would otherwise pin the
previous release.

`supervise.ts` closes the same gap from the other side. If the binary
exits at boot — before it ever accepted on `$PORT`, or within its first
minute (bad arch, missing required env var, a store it cannot open,
workload finished) — the wrapper binds `$PORT` itself and answers every
request with a generic 500 `binary_exited`. Without it the only symptom
is the domain answering 404/503, which is indistinguishable from a cold
start; with it a dead service reads as dead. The port is customer-reachable
and the wrapper, not the binary, answers it, so the body carries nothing
of the binary (second external review); the details are in the platform
log: the child's own stderr, which the wrapper tees there as it runs, then
`binary exited with code N; holding :PORT unhealthy; details: {...}` with
the binary, its arguments, exit code, uptime, whether it ever accepted,
and a hint:

```bash
curl -s https://<domain>/
# {"error":"binary_exited","message":"the service is not running; its exit is recorded in the platform log"}
```

A SIGTERM or SIGINT to the wrapper is forwarded to the binary, and the
wrapper exits with the binary's code whatever its policy (the platform is
stopping the instance). A binary still running 180 s after the first
forwarded signal (streams-slate bounds its own stop at 175 s: a fleet
instance's planned drain, at most 145 s, then its 30 s ordered stop, plus
up to 250 ms for its last stderr line) is killed, and the wrapper exits 137.
Whether Compute itself waits that long between its stop signal and a kill
is not verified (D10): a platform kill ends the process at once, with no
ordered stop (as any kill does), and whatever the drain had not yet handed
off is taken over by fencing, as before item 40.

A binary that dies after it was ready — it accepted on `$PORT` and had
been up for 60 s, counted on the monotonic clock from the spawn (owner
decision D4(c)) — died at runtime (an OOM kill; streams-slate's exit 1
after a critical loop's exit, item 38). `app-server`, and `app-lb` as the
router (`PILOT_MODE` unset or `lb`), then exit with the child's own code
(a graceful 0 stays 0; a signal is 128 + its number, e.g. 137 for an OOM
kill) and Compute replaces the instance (item 39). The wrapper's log line
`binary exited with code N after serving on :PORT ...` is then the only
record of that death. `app-gen`, and `app-lb` running the pilot's
generator or benchmark (`PILOT_MODE=gen` or `bench`), hold every death,
because a workload that runs to completion must not be restarted
mid-campaign. The choice is `policyFor(app, env)` in `supervise.ts`; every
`index.ts` passes `policyFor("<its app>", process.env)`, and
`superviseBinary` has no default policy: called without one it refuses to
start, rather than silently holding every death. `bun test
./deploy/supervise.test.ts` pins every app's policy, the wiring in each
`index.ts`, both death paths, the 60 s boot window, the preserved exit code
(0, 1 and 137), the wrapper's log line, and that the three copies stay
byte-identical.

**Deploy only the repo's wrapper (plan decision D11).** A streams-slate
with item 38 behind an older wrapper turns a critical exit into a 500 that
is never replaced, where the old binary at least kept serving. The
campaign scripts deploy from copies of these directories under
`$SOAK_HOME` (they carry `node_modules`), and a copy staged before item 39
still holds every death. `bench/stage-app.sh <app> <dir>` refuses a copy
holding any entry this directory does not (hidden ones such as `.env`
included, which Bun would load on its own; other directories; symlinks),
before installing anything, then builds a fresh directory with exactly
these files, moves the old `node_modules` in only if an install succeeded
for exactly this manifest (else runs `bun install`, output shown, and
refuses on failure), verifies it byte for byte, and only then swaps it in
(`deploy/stage-app.test.ts`);
`bench/soak/deploy-region.sh`, `mt-tenants.sh`, `wc-ladder.sh`,
`bench/fleet/setup-fleet.sh` and `deploy-fleet.sh` run it before every
deploy. Run it yourself before deploying any other copy.

The wrapper forwards SIGTERM and SIGINT to the binary (see above), so
streams-slate's graceful stop and its 30 s stop bound run when the
platform signals the wrapper. How Compute delivers its stop signal (to
PID 1, to the process group, or by tearing the VM down) is not yet
verified: the wrapper -> binary -> platform lifecycle on Compute is a
deployment gate (D10).

Distinct env names per role (`SERVER_BINARY_S3_KEY` vs `LB_BINARY_S3_KEY`,
`BIN_S3_*` vs `SLATE_S3_*`) — Compute env vars are project-scoped and
merged, and shared names have twice caused a service to run the wrong
binary (RUNBOOK section 7.3). Likewise one name per bucket: app-server
reads the auth-feed bundle (`FEEDS_S3_KEY`) with
`FEEDS_S3_{ENDPOINT,BUCKET,REGION,ACCESS_KEY_ID,SECRET_ACCESS_KEY}` only
(region default `auto`), never with the binary's `BIN_S3_*`; with one of
the other four unset it serves its download-failure diagnostic instead of
starting the binary.

## Deploy footguns

Every entry below cost real time on a real campaign. They are platform
behaviours, not bugs in this repo, so the only defence is knowing them.

**Compute region codes are not Tigris PoP codes.** Compute's `--region
us-east-1` lands in `ewr`; the Tigris PoP serving that region is `iad1`.
They are different namespaces and the codes do not line up. Never infer
one from the other — read the preview domain (it carries the Compute PoP)
and read `/v1/debug/store` (it carries the storage side).

**`deploy` reports a version id (`cpv_…`), not a service id (`cps_…`).**
Scripts that scrape the deploy output for "the id" capture the wrong one,
and every later `--service` call fails. Get service ids from `services
list`. This has now bitten twice.

**Preview domains belong to a *version*, not to a service.** Redeploying
mints a new domain and the old one starts answering 503 while still
looking like a plausible URL. Any script that caches a URL must re-resolve
it from the running version after each deploy:

```bash
compute versions list --project "$P" --service "$SV" | awk '$2=="running"{print $3; exit}'
```

**A failed deploy leaves a version-less service shell.** It appears in
`services list` and looks deployable, but has no running version. Delete
it rather than redeploying into it.

**Fresh app directories need `bun install` before `deploy`.** A copied
wrapper app without `node_modules` deploys and then fails at import.
`bench/stage-app.sh` installs when it stages one.

**Do not run parallel `bunx @prisma/compute-cli` invocations.** They race
on the shared package cache and fail with `EEXIST`. Fan out across regions
*sequentially*, or pre-warm the cache with one call first.

**A copied CLI OAuth token expires in about an hour.** Extracting the
access token from the CLI's auth store and exporting it as
`PRISMA_API_TOKEN` works — until it silently doesn't: mid-campaign every
deploy started "failing" in under a second, which was a 401 in disguise.
Let the CLI use its own stored login (it refreshes itself); only pass a
token for the raw management API, minted fresh at the moment of use.

**Deploying by `--service-name` into an existing service fails** with
"already exists". Resolve the id from `services list` first and pass
`--service`; `deploy`'s own output never contains a service id.

**A version can stay "running" while its domain serves the platform 404
forever.** One generator service did this for three consecutive deploys.
When redeploys keep zombie-ing, `services destroy` + recreate gets a
fresh placement and has fixed it every time.

**…except when NO new service routes at all.** On 2026-07-31 four
deployments — two projects (one created through the management API, one
through `compute projects create`), two PoPs (ewr and sin) — all
reported `running` and all answered the platform's "There is no service
on this URL" on both the service domain and the per-version preview
domain, while a service deployed a week earlier still answered 200
through the same edge. The logs showed the full healthy boot each time
(`assembled … e_machine=62`, `listening on 0.0.0.0:8080`), so the app
was never the problem. Distinguish this from the single-service zombie
before you burn deploys on it, with three cheap checks:

1. **Probe an OLD running service.** If it answers 200 and yours does
   not, redeploying yours will not help.
2. **Deploy a hello-world bun app** (fifteen lines, `Bun.serve` on
   `process.env.PORT ?? 8080`). If that 404s too, nothing about your
   app is involved. Do this before debugging your own boot path.
3. **Stream the failing service's logs while pinging it.** A
   scaled-to-zero instance boots and logs when woken — the platform
   answers 404 during that window, which looks identical from outside.
   Zero new log lines under load means the request never reaches the
   machine, i.e. the edge has no mapping, not a slow wake.

DNS is not the discriminator: failing and working hostnames resolve to
the same edge IPs, and the failure is a canned 404 page served BY the
edge. Note also that the platform sets no `PORT` (the container env is
`HOME,PATH,TERM`), so 8080 is convention, not configuration.

Full write-up, evidence and a scripted re-test:
[repro-edge-404/](../repro-edge-404/README.md). Run its `repro.sh` to
find out whether the platform is publishing new services again before
planning any deploy-dependent work.

**`services destroy` takes the id as a POSITIONAL argument.** `--service
cps_…` is silently wrong: the command fails with `appId: must be an
optionally cps prefixed cuid or cuid2` and the service survives. It is
`services destroy cps_… --project proj_…`. A teardown script that uses
the flag form leaves everything running while reporting nothing
unusual.

**A missing required env var can look exactly like a boot failure.** A
clap arg with no default makes the binary exit immediately at startup;
Compute still reports the version `running` while its domain 404s/503s.
Give every benchmark/tooling arg a `default_value` so a forgotten `--env`
degrades to a wrong-but-running config instead of a silent zombie
(`bench/awsbench/src/main.rs`, `BENCH_SHAPE`).
