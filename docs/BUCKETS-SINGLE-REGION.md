# Single-region Prisma Buckets — measured impact on Streams

**2026-08-05, revision 2.** Revision 1 claimed "placement is broken —
single-region buckets pin far from fra regardless of region hints."
That was WRONG, and the correction reshapes the conclusion entirely:
**buckets inherit their PROJECT's region; a bucket has no region of
its own.** Our campaign projects (`streams-camp75-eu`/`-use`, created
2026-08-03) were created WITHOUT a region — `defaultRegion: null` —
so "single-region" buckets created in them fell back to a US-shaped
default. The old GLOBAL buckets had masked the projects' regionless
state completely, because global buckets serve at the nearest edge no
matter where the project lives.

Mechanics, verified against the management API:

- Project CREATE takes `"region"` (e.g. `"eu-central-1"`); the read
  side reports it as `defaultRegion`. Omit it and the project — and
  every bucket in it — is US-homed. (A `"region"`/`"defaultRegion"`
  field on bucket create or project PATCH is ignored/rejected; the
  project's create-time region is the only lever, exactly as
  designed.)
- New buckets land on `t3.storage.dev` (the old global ones are on
  `fly.storage.tigris.dev`).

## The A/B/C, from the fra service (`freeze3` binary, 300-append burst, p50 ms)

| op | global (old bucket) | single-region, region-LESS project | **single-region, EU-homed project** |
|---|---|---|---|
| put:wal (ack path) | 27 | ~215 | **16** |
| put:manifest | 25 | 131 | **16** |
| get:other (data hits) | 65 | 114 | **8** |
| get:manifest (freshness probes, 404-shaped) | 268 | 111 | **8** |
| head:wal (probe, 404-shaped) | 293 | 292/228 | **121** |

## Conclusions

1. **Co-located single-region buckets are a large win everywhere.**
   Ack-path `put:wal` 27 → 16 ms, data GETs 65 → 8 ms — the user-facing
   append p50 in fra should drop well below the soak5 59 ms once a
   cell runs on a properly-homed bucket.
2. **The GET-404 penalty is gone.** The global bucket charged a fixed
   ~270–300 ms for GETs of missing keys from every vantage; co-located
   single-region GET-404s cost **8 ms** — same as hits. This
   transforms the idle-probe economics that soak10's poll-stretch
   posture was built around; revisit those knobs after the first
   long soak on a single-region cell (defaults may be loosenable).
3. **The residual Tigris bug is HEAD-specific, now cleanly isolated:**
   same co-located bucket, same vantage — GET on a missing key 8 ms,
   HEAD on a missing key **121 ms** (~15×). For Tigris's
   investigation: `t3.storage.dev`, single-region, HEAD-miss vs
   GET-miss. SlateDB's opener/WAL probes use HEAD, so reopen and
   probe paths still pay this until fixed.
4. **Migration guidance:** migrating an existing cell means a NEW
   project created with `"region"` set (existing projects cannot be
   re-homed) — i.e., new bucket, new service, data migration or fresh
   namespace. The retained campaign cells stay on their global
   buckets (they are fast there and they are throwaway); FIRST
   single-region cell should be a fresh deployment, ideally the #113
   campaign.
5. **The ~1% metadata trickle** re-measure is now unblocked: the
   EU-homed test bucket exists (`streams-sr-fra-data`,
   `bkt_s11rnxuca330xneqe8rj2enq`, project `streams-sr-fra2` =
   `proj_psrbg85krdtr01pn0oae51fy`). It needs a long soak window;
   scheduled with the next co-located soak. Short-window hint:
   get:other p99 131 ms vs p50 8 ms (n=199) — a tail exists, size
   unknown.

Housekeeping: the two mis-homed test buckets and the one accidentally
US-homed project from revision 1 are deleted; `streams-sr-fra2` and
its bucket are KEPT for the trickle soak and the first migration
rehearsal. The fra retained service was restored to `freeze3` + its
original global bucket after each canary and re-verified (saga smoke
PASS).
