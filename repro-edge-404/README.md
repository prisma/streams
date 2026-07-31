# New services are never published to the edge — every deploy 404s while the app runs

**For:** Prisma Compute platform team
**Date:** 2026-07-31 · workspace `wksp_cmrj21kxd3scrwfdvkx9wgi54`
(prisma-streams-slatedb) · CLI `@prisma/compute-cli@0.38.0`
**Contents:** `index.ts` (15-line dependency-free app), `package.json`,
this README.

## Summary

Every service we created on 2026-07-31 deployed "successfully", booted,
and bound its port — and **none of them was ever reachable**. Both the
service domain and the per-version preview domain answer the platform's
own HTML 404 ("There is no service on this URL"), indefinitely.

Six deployments, three projects, two PoPs, including a **fifteen-line
hello-world bun app**. Services created before that day still answer
200 through the same edge. A deploy earlier the *same day* worked and
served a full test suite over the WAN, so this started mid-day.

Our reading: the control plane accepts the deploy and reports
`running`, the machine boots and listens, and **the edge never learns
the hostname**. It is not the app, the project, the region, DNS,
scale-to-zero, or the port.

> **Not the same bug as `repro-no-restart/` (2026-07-15), despite an
> identical symptom.** That one reproduced from an app that could not
> stay up. Here the app is demonstrably alive and idle — it logs its
> listen line and never exits. Same HTML 404, same `running` status,
> different cause. Whatever surfaces this symptom should probably
> distinguish the two, because from outside they are indistinguishable
> and the remedies are opposite (fix your app vs. wait for the edge).

## Impact

We could not run the release gate for a product launch. Redeploying,
recreating services, switching regions and recreating projects all
failed identically, and each attempt costs a full deploy cycle before
the symptom appears. There is also no signal anywhere in the CLI or API
that anything is wrong: `versions list` says `running`.

## Minimal reproduction

`index.ts` in this directory, no dependencies:

```ts
const port = Number(process.env.PORT ?? 8080);
console.log("hello app booting; PORT env =", JSON.stringify(process.env.PORT));
console.log("env keys:", Object.keys(process.env).sort().join(","));
Bun.serve({ port, hostname: "0.0.0.0", fetch: () => new Response("ok") });
console.log(`hello app listening on 0.0.0.0:${port}`);
```

```bash
export PRISMA_API_TOKEN=…
bunx --bun @prisma/compute-cli@latest projects create --name hello-probe
bunx --bun @prisma/compute-cli@latest deploy \
  --project proj_… --service-name hello --region us-east-1
curl -s -o /dev/null -w '%{http_code}\n' https://<service-url>/health   # 404, forever
```

Deployment logs for that exact version:

```
spark::app_source  source archive unpacked to /mnt/app/app.tmp
spark::vars        loaded 0 environment variable(s) from vars file
spark              starting bun with entrypoint: bundle/index.js
hello app booting; PORT env = undefined
env keys: HOME,PATH,TERM
hello app listening on 0.0.0.0:8080
```

The app is up. The domain 404s.

## Evidence matrix — every deployment on 2026-07-31

| # | app | project | project created via | region / PoP | version | status | domain |
|---|---|---|---|---|---|---|---|
| 1 | streams-slate | `proj_ta94zxtq10vfy81d2wrvvrh4` | management API | us-east-1 / ewr | `cpv_wq0pk7up1tci17cfuo6p7hvk` | running | **404** |
| 2 | streams-slate | same, new service | management API | us-east-1 / ewr | `cpv_rj9hu9uujwuh15cicn14w34t` | running | **404** |
| 3 | streams-slate | same, new service | management API | ap-southeast-1 / sin | `cpv_wbried7h1vhtgoyn2c0mm63s` | running | **404** |
| 4 | streams-slate | `proj_n02pwdacnqf5aq8f5k4zc0sl` | `compute projects create` | us-east-1 / ewr | `cpv_vca3wogxu0uzezohqsgynuss` | running | **404** |
| 5 | **hello world** | `proj_zujcwlmcimuyyg25az0rykxi` | `compute projects create` | us-east-1 / ewr | `cpv_f2d6vahy4hd5q36cnvjv3x0q` | running | **404** |
| 6 | **hello world** | `proj_qxufde4fkcjzl6t9dsd4ajct` (pre-existing, has services that DO route) | — | ap-southeast-1 / sin | `cpv_s184rz7z422britxxftpq5jg` | running | **404** |

Rows 5 and 6 are the ones that matter: a hello-world app fails, and it
fails inside a project whose older services answer 200 right now.

### Controls — pre-existing services, same edge, same moment

| preview domain | version created | result |
|---|---|---|
| `cv-97bbe99f6d7f.sin.prisma.build` | 2026-07-19 | **200** |
| `cv-e5430a308d96.sin.prisma.build` | 2026-07-24T09:13:11Z | **200** |
| `cv-5299b4123061.sin.prisma.build` | 2026-07-24T10:14:29Z | **200** |
| `cv-bc2b47805303.fra.prisma.build` | 2026-07-22 | **200** |
| `cv-ef9deae56e1e.sin.prisma.build` | 2026-07-20 | 404 (did not wake in 8 pings; likely long-dead from an old campaign) |
| `cv-8fb1ed0af027.fra.prisma.build` | 2026-07-22 | 404 (same) |

## What we ruled out, and how

**Not the app.** A 15-line hello-world bun app with zero dependencies
reproduces it. Our own service downloads an x86_64 binary and the log
confirms the arch (`assembled 23261744 bytes e_machine=62`) and the
listen (`streams-slate listening on 0.0.0.0:8080`).

**Not the project, and not how the project was created.** Fails in a
project made through the management API, in one made through
`compute projects create`, and in a months-old project whose own
services are serving 200 right now.

**Not the region.** ewr and sin both.

**Not DNS.** Failing and working hostnames resolve to the same edge
addresses:

```
fqfef00ipc2c4wv4cz3j34ha.ewr.prisma.build  →  66.135.30.206, 149.28.231.41, 66.135.7.198
g8ethzzp9g7facx0mhcw0mtk.sin.prisma.build  →  45.77.174.147, 207.148.70.167, 207.148.119.195
cv-e5430a308d96.sin.prisma.build (works)   →  45.77.174.147, 207.148.119.195, 207.148.70.167
```

**The edge is answering; it just has no backend.** The 404 is a static
352-byte HTML page with an `etag`, served over HTTP/2:

```
# failing                          # working (same moment)
HTTP/2 404                         HTTP/2 200
content-type: text/html            content-type: text/plain; charset=utf-8
etag: "69d8c7b3-160"               x-content-type-options: nosniff
x-request-id: 01KYVZE8XCAJXXF2V4G2JQ8RZK    x-request-id: 01KYVZEEAJ08ZAHJKH507DTH6K
content-length: 352                content-length: 2
```

**Not scale-to-zero.** This was the one alternative worth eliminating,
since the platform answers 404 while a sleeping instance wakes and that
is indistinguishable from outside. We streamed a failing service's logs
(`wss://api.prisma.io/v1/deployments/<cpv>/logs`) while sending five
`/health` requests over 45 s: **zero new log lines**. A waking instance
boots and logs. These requests never reach the machine.

**Not a port mismatch.** The platform sets no `PORT` — the container
environment is exactly `HOME,PATH,TERM`. Both apps bind the 8080
convention, and the working services do the same.

## Timeline

A deploy earlier on 2026-07-31 worked: the service came up and served a
14-test SDK suite over the WAN, then was torn down normally. The first
failing deploy was at **2026-07-31T10:51:06Z**. So the change landed
between those two points, and everything deployed before it keeps
working.

### Re-tested after the incident was reported resolved

Still failing, at **2026-07-31 ~13:30–13:55Z**:

| test | region | result |
|---|---|---|
| hello-world control (`repro.sh`) | us-east-1 / ewr | 404 through 15 attempts |
| hello-world control (`repro.sh`) | ap-southeast-1 / sin | 404 through 15 attempts |
| streams-slate, version `cpv_u43ji6483c1cgszdgo4rkxxh` | us-east-1 / ewr | **404 for 16 minutes** (~60 polls) |
| control `cv-e5430a308d96.sin.prisma.build` (pre-existing) | sin | 200, unchanged |

The 16-minute poll was deliberately patient in case publication is
merely slow after a fix. That version's boot log is normal and ends
with `streams-slate listening on 0.0.0.0:8080`, so the behaviour is
exactly as described above: app up, edge unaware. Whatever the incident
was, this symptom outlived its resolution — worth checking whether they
are the same issue at all.

## What would help

1. Is there a control-plane → edge publication step that can silently
   fail? If so, `versions list` reporting `running` is misleading, and
   the deploy command exiting 0 doubly so.
2. Can the deploy path verify its own domain before reporting success?
   Every one of these deploys printed a Service URL that never worked.
3. Please distinguish this from the crash-loop zombie in
   `repro-no-restart/` at whatever layer reports service state — the
   two look identical from outside and want opposite responses.

## Cleanup

Every resource created for this investigation has been destroyed and
verified gone: 6 services, 2 buckets, 3 projects (project GETs return
404). The pre-existing services used as controls were only read from —
never redeployed, since redeploying one would likely have destroyed a
working route with no way to restore it.

## Two CLI bugs found on the way

Minor, but they cost time during the above:

- **`services destroy --service cps_…` fails and leaves the service
  running.** It errors with `appId: must be an optionally cps prefixed
  cuid or cuid2`, which reads like the id is malformed. The id is fine;
  the command wants it positionally (`services destroy cps_… --project
  proj_…`). A teardown script using the flag form reports nothing
  unusual while deleting nothing.
- **`compute logs --project P --service S` cannot fetch logs.** It
  fails with `WebSocket connection to
  'wss://api.prisma.io/v1/deployments/<PROJECT-id>/logs' failed:
  Expected 101 status code` — it puts a `proj_…` id where the API wants
  a `cpv_…` deployment id. Connecting to
  `wss://api.prisma.io/v1/deployments/<cpv_…>/logs` directly works and
  is how we got the boot logs above. Without that workaround a deploy
  in this state is completely opaque.
