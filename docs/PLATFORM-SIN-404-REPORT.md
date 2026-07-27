# Platform report: `ap-southeast-1` preview domains serving 404 for live, running versions

**From:** Prisma Streams team (soak/benchmark campaigns, 2026-07-26 → 2026-07-27)
**Severity:** blocks reliable operation of any HTTP-serving app in `ap-southeast-1`; zero occurrences in the same period across five other regions
**Live specimen at time of writing (08:23 UTC, 2026-07-27):** `cv-15c922e43b5d.sin.prisma.build` — see §2, reproducible with one curl

## 1. Summary

Across two days of multi-region benchmark campaigns (~60 deploys total,
six regions in parallel, identical Bun wrapper apps everywhere), we hit
**eight distinct incidents where a Compute version in `ap-southeast-1`
reported `running` while its preview domain served the platform's
"Service not found" 404 page** — in several cases while the app process
was *demonstrably executing* (its outbound workload visible on our
servers). Every other region ran the same apps, deployed the same way,
with zero occurrences.

Four distinct shapes, detailed in §4:

- **A. Zombie boot** — version `running`, domain 404 from the first
  minute, never recovers (observed ≥10–13 min before we gave up).
  Redeploying *into the same service* reproduces it back-to-back;
  destroying the service and creating a fresh one fixes it.
- **B. Slow boot** — fresh SIN services take **5–8 minutes** from
  `running` to first byte; the same app is serving in <60 s in the other
  five regions.
- **C. Inbound-only breakage** — the app runs (outbound traffic
  flowing), the sole `running` version's own preview domain 404s. Live
  right now.
- **D. Mid-run replica flap** — a `KEEP_AWAKE=1` instance is replaced
  transparently mid-run and, for a period, **two replicas alternate
  behind one preview domain** (one with the process's accumulated state,
  one fresh), then in-memory state is lost.

## 2. The live specimen (Shape C) — reproducible right now

Project `proj_aetb1z2e6k5lhm4bg3v21zva`, service
`cps_el0kcfd98b7n4kfhvtsf5hu9` ("soak-gen-ap-southeast-1"), version
`cpv_v025b86odpc2g3ojb5mkk5an` created 2026-07-27T08:08:38Z. It is the
**only** version of the service and `versions list` reports it
`running` with preview domain `cv-15c922e43b5d.sin.prisma.build`.

```
$ curl -si https://cv-15c922e43b5d.sin.prisma.build/ | head -3
HTTP/1.1 404 Not Found            # in 0.17 s — edge-fast, not a timeout
...
<title>Service not found</title>  # the platform's page, not our app's
```

The app behind it is a load generator, and it is **provably alive and
working**: the Streams server it targets (same region, separate project)
attributed 46,166 object-storage operations to this generator's workload
at 08:23:08Z and 47,586 twenty seconds later — ~70 ops/s of sustained
outbound work from a process whose inbound domain says it doesn't exist.
The URL was re-resolved from `versions list` immediately before the curl
(we are aware preview domains are per-version; this is the current one).

## 3. Incident inventory (all `ap-southeast-1`, all `KEEP_AWAKE=1`)

| # | UTC (2026-07-27 unless noted) | project / service / version | shape | outcome |
|---|---|---|---|---|
| 0 | 07-26 ~06:5x | soak-server-ap-southeast-1 (first campaign) | A | healed by one redeploy |
| 1 | 02:46:51 | `proj_zzjpji8f9a0ly0xmfawwtpcd` / `cps_mmgrzbbgww3wzt4fg1uhuq2p` / `cpv_z0hmya0onhn2qngj6danccp7` (`cv-8eff7a6db88d`) | A | 404 ≥10 min; superseded |
| 2 | 02:50:19 | same service / `cpv_wouo6o921a9eh9xz3tecaw02` (`cv-8a18db4fdfa8`) | B | 404 ~8 min, then served |
| 3 | ~03:31 | same service, run-2 redeploy (`cv-8bf8a3337e99`) | A | 404 ≥10 min |
| 4 | ~03:45 | same service / `cpv_pr9t0r33oeqy970ih38afojt` (`cv-4b63c1099213`) | A | 404 ≥12 min → we **destroyed the service**; the fresh service (`cps_thfnto0lslczre0x7u1tkcfp`, 04:34) booted in ~7 min and ran two clean 30-min soaks |
| 5 | 06:47 | dnsprobe project / `cps_alelhc1rhjomkjx819ohh5g4` / `cpv_z4j6bx3z8w1as3g9mehzczxk` (`cv-c6e43b080426`) | A | 404 ≥13 min → destroyed; fresh service (`cpv_cnlj6smh45kx4wnqh2tv5dux`, `cv-237a51dc8957`) booted in ~7 min (B), then fine |
| 6 | ~07:36 | `proj_dr16bcgqxrutjbdiknw9eytf` / `cps_eksjhkslul5jkuoqdm18togo` | D | serving fine (ok=1,972 at 07:08), then a fresh replica with empty state alternated with the old one behind one domain; the process's accumulated results were lost at harvest |
| 7 | 08:08 → ongoing | `proj_aetb1z2e6k5lhm4bg3v21zva` / `cps_el0kcfd98b7n4kfhvtsf5hu9` / `cpv_v025b86odpc2g3ojb5mkk5an` (`cv-15c922e43b5d`) | C | §2 — live |
| 8 | 11:07 create → 11:5x observed | soak5 campaign, service `soak-gen-ap-southeast-1` (created FRESH at 11:07 per the destroy+recreate playbook), version `cpv_wt8980t7qowow4v8eg47qoii` (`cv-1d961a7e9f5b`) | C | gen workload flowing (server-side ops advancing); inbound preview 404 in 0.16 s; client-side benchmark metrics unrecoverable → SIN leg rerun on another fresh service |

For contrast, the same wrapper app deployed the same way is answering
HTTP 200 right now in `us-east-1` (`cv-…ewr`), `eu-central-1`
(`cv-…fra`), and `ap-northeast-1` (`cv-…nrt`), among ~50 clean deploys
across five regions in the same window.

## 4. Why we're confident this is not the app

1. **Our wrapper cannot 404 silently.** Every app carries a supervisor
   (`deploy/supervise.ts`): if the child binary exits for any reason,
   the wrapper itself binds `$PORT` and serves a JSON diagnostic with
   the exit code and stderr tail. A platform HTML "Service not found"
   means **no connection ever reached the app**, not that the app died.
2. **The identical app works everywhere else, simultaneously** — same
   directory, same `bun install`, same deploy flags (`--http-port 8080`),
   deployed in the same loop minutes apart.
3. **Shape C is dispositive:** the process is executing its workload
   (outbound HTTPS at ~70 ops/s, measured on the receiving side) while
   its inbound domain 404s. The route also **flaps**: at 08:39Z one of
   our automated polls received the app's full JSON state through the
   same domain that returned the platform 404 to manual curls at 08:23Z
   and 08:41Z — some edge paths reach the instance, most don't.
4. Scale-to-zero is excluded: `KEEP_AWAKE=1` on every affected service,
   and the affected versions never served a first byte to wake-worthy
   traffic anyway — we polled continuously from deploy time.

## 5. What we observe about the mechanism

- The 404s return in ~0.1–0.2 s from the edge — routing, not timeouts.
- **Per-service stickiness:** consecutive redeploys into an affected
  service stayed broken (incidents 3→4 back-to-back); `services
  destroy` + recreate has fixed it in both cases we tried (incidents
  4→run-3-service, 5→`dnsprobe-sin2`). This smells like the service's
  edge-route or placement record, not the version artifact.
- **SIN-only**, across two days, three unrelated projects, and two
  different app images (a Rust-binary launcher and a pure-Bun probe).
- The 5–8-minute Shape-B boots (vs <60 s elsewhere) suggest the same
  provisioning path is slow even when it eventually succeeds.

## 6. Impact on us (so far)

- One region lost from a six-region benchmark run (30 min of intended
  load never generated); a second run's region delayed ~25 min.
- One run's client-side results lost to the Shape-D replica swap.
- Several person-hours of triage before we recognized the pattern, plus
  permanent defensive complexity (per-run URL re-resolution, "confirm
  load actually flows" checks, destroy-and-recreate playbook).

## 7. Questions / asks

1. What does `running` assert, exactly? These versions report `running`
   while the edge has no route to them — if `running` means "process
   started", is there a separate signal for "edge route programmed"?
   We'd gladly poll a readiness state instead of curling for HTML 404s.
2. Can you inspect the SIN edge/placement records for the version IDs
   above (esp. the live `cpv_v025b86odpc2g3ojb5mkk5an`) and say where
   the route was lost?
3. Why does destroy+recreate fix what redeploy does not? If a service
   accumulates a poisoned placement/route in SIN, is there a lighter
   remediation than destroying it (which changes the service ID and
   breaks our references)?
4. Shape D: is an instance replacement under `KEEP_AWAKE=1` expected to
   run two replicas behind one preview domain concurrently? If yes, for
   how long, and is the old replica's traffic drained or raced?
5. Is the SIN pool known to be special right now (capacity, a bad node,
   a slow image cache)? The 5–8-minute boots are exclusive to it.

## 8. Current state

Incident 7 is **preserved for inspection**: when the current campaign's
other five regions are torn down, project
`proj_aetb1z2e6k5lhm4bg3v21zva` (both services and its bucket) stays up
indefinitely — the 404ing preview domain, its `running` version, and
the live app behind it, untouched. Ping the Streams team when you're
done with it and we'll remove it.
