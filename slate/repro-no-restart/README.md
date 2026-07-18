# Minimal reproduction: crash-looping apps become permanent, silent zombies

**For:** Prisma Compute platform team
**Date:** 2026-07-15 · region ap-southeast-1 · repro service `cps_lnj6dr37yqzehqfsdaxzc3at`
**Contents:** `index.ts` (~50-line dependency-free app), `repro.sh` (scripted run), this README (with live transcript excerpts).

## Summary

We chased six production incidents where an instance went permanently dark:
requests return the platform's HTML 404 ("There is no service on this URL"),
`versions list` still reports **running**, no logs are retrievable, nothing
recovers it except a fresh `deploy`.

Building this minimal repro produced a **better result than we expected**: the
platform's recovery machinery is *good* for every single-death mode we could
trigger — and the permanent-zombie state reproduces deterministically from
exactly one thing: **an app that cannot stay up (a crash loop)**.

## The evidence matrix (all runs live on 2026-07-15, transcript below)

| leg | trigger | observed outcome |
|---|---|---|
| 1 | `process.exit(1)` while idle | ✅ recovered — next request boots a fresh instance |
| 2 | fast OOM (allocate to death) while idle | ✅ recovered |
| 3 | stop listening, process stays alive ("wedge") | ✅ recovered |
| 4 | fast OOM with the keep-awake guard active | ✅ recovered |
| 5 | fast OOM **under continuous 10 req/s traffic** | ✅ recovered seamlessly (fresh instance within seconds, traffic barely noticed) |
| 6 | **crash loop** (`CRASHLOOP=1`: exit(1) at boot, every boot) | ❌ **permanent zombie**: HTML 404 on every request, `versions list` says `running`, no retrievable logs, no recovery until redeploy |

Legs 1–5 deserve explicit credit: single hard deaths — even OOM kills, even
under live traffic — are reprovisioned transparently. That is exactly the
behavior we want.

## The bug, precisely

When the app exits repeatedly at/near boot, the platform ends up in a state
where:

1. **The service domain serves the platform 404** ("There is no service on
   this URL") — indistinguishable from a service that never existed.
2. **`versions list` reports the version as `running`** the entire time. There
   is no `crashlooping` / `failed` / `unhealthy` state visible anywhere we can
   find (CLI or response headers).
3. **`compute logs <version>` returns nothing** for the affected version, so
   the operator cannot see the crash output that would explain the loop.
4. **It never exits this state on its own.** We have observed production
   instances stuck like this for 30+ minutes (we redeployed; it plausibly
   lasts indefinitely).

### How our production incidents map to this

Our instances died of OOM **under fleet load**. Per leg 5, the platform
restarted them — correctly! — but a restarted instance immediately rejoins a
hot fleet (shard reopen + full traffic + cold caches) and can re-OOM within
seconds, or its bootstrap (binary download) can fail transiently. A few
iterations of that is a crash loop → the leg-6 state. So the user-visible
"platform never restarts crashed instances" we originally reported is more
precisely: *the platform restarts them, but gives up (or wedges) silently on
repeated failure, and the resulting state is invisible*.

## Expected behavior (what we'd ask for)

1. **Truthful status**: a version whose replicas are crash-looping should
   surface as `crashlooping` / `degraded` — anything but `running`. This is
   the single highest-value fix; everything else is diagnosable once status
   tells the truth.
2. **Crash-loop backoff, not abandonment**: keep retrying with exponential
   backoff (the standard supervisor contract), so a transient cause (cold-start
   stampede, dependency blip) self-heals when it passes. Today's behavior
   after the loop threshold appears to be "stop forever."
3. **Boot logs must survive**: the crashing process's stdout/stderr from the
   failed boots is precisely what an operator needs; today `compute logs`
   yields nothing for these versions.
4. **A distinguishable error page**: the generic "There is no service on this
   URL" is the same body a typo'd subdomain gets. A "service exists but is
   failing to start" page (or at minimum a distinct header) would have saved
   us a day of debugging.
5. (Nice-to-have) an API/webhook signal for replica state transitions, so a
   fleet controller can react to crash loops without polling.

## Probable one-flag fix

Prisma Compute runs on the Unikraft platform, whose public docs
(unikraft.com/docs/platform/instances) define
`restart_policy: never | always | on-failure` — with exponential backoff
(immediate, 5s, 10s, 20s, 40s, 5m; a 10-second stable run resets the
sequence) and **`never` as the default**. If Prisma Compute instances run
with the default, that is this entire issue: switching managed instances
to `on-failure` gives exactly the supervisor contract described above,
including the backoff. Worth checking before any deeper work.

## Reproduce it yourself

```bash
cd repro-no-restart
PRISMA_API_TOKEN=... ./repro.sh <project-id>       # legs 1–5: all recover
# then the zombie:
bunx @prisma/compute-cli deploy --project <p> --service <svc> \
  --region ap-southeast-1 --path . --http-port 8080 --env CRASHLOOP=1
curl https://<svc-domain>/health                    # HTML 404, forever
bunx @prisma/compute-cli versions list ...          # says: running
# heal:
bunx @prisma/compute-cli deploy ... --unset-env CRASHLOOP
```

## Live transcript excerpts (2026-07-15, ap-southeast-1)

Leg 5 — OOM under continuous traffic, seamless recovery:

```
== 9. death under CONTINUOUS traffic
-- trigger OOM while hot:
allocating until OOM kill
t+30s  HTTP 200  ok pid=397 uptime_s=28.7     <- fresh instance already up
t+60s  HTTP 200  ok pid=397 uptime_s=58.8
```

Leg 6 — crash loop, the zombie:

```
== 11. deterministic crash-loop leg (CRASHLOOP=1)
-- version status vs reality:
cpv_uhxhgmr9q2j1gdjd8bp1oe6y  running  cv-e7135f46a4eb.sin.prisma.build
t+20s  HTTP 404  <!doctype html>... "There is no service on this URL"
t+40s  HTTP 404  ...
t+60s  HTTP 404  ...
== 12. heal by redeploying without CRASHLOOP
post-heal: ok pid=396 uptime_s=15.0
```

Full transcript: `repro-transcript.txt` (kept alongside our test logs; happy
to share). The repro service is left healthy.
