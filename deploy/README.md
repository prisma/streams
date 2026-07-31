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

All verify the download is an **x86_64 ELF** before exec. That check is
not politeness: an aarch64 binary deploys "successfully" and crash-loops
into a silent platform zombie (service domain 404, `versions list` says
running, no logs retrievable). All also re-download every boot, because a
warm instance keeps `/tmp` across versions and would otherwise pin the
previous release.

`supervise.ts` closes the same gap from the other side. If the binary
exits — bad arch, missing required env var, workload finished — the
wrapper binds `$PORT` itself and serves a 500 carrying the exit code and
the tail of stderr. Without it the only symptom is the domain answering
404/503, which is indistinguishable from a cold start; with it a dead
service explains itself:

```bash
curl -s https://<domain>/ | head -20
# {"error":"binary_exited","exitCode":2,"hint":"non-zero exit: check
#  required env vars ...","stderrTail":"error: the following required
#  arguments were not provided: --shape <SHAPE>"}
```

Distinct env names per role (`SERVER_BINARY_S3_KEY` vs `LB_BINARY_S3_KEY`,
`BIN_S3_*` vs `SLATE_S3_*`) — Compute env vars are project-scoped and
merged, and shared names have twice caused a service to run the wrong
binary (RUNBOOK section 7.3).

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
before you burn deploys on it: probe an OLD running service first. If
it answers and yours does not, the edge is not registering new
services, and no amount of redeploying will help.

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
