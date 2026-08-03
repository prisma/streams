# Soak 9 — six regions, 30-minute ramp: the full-fleet rerun (2026-08-03)

**~10:47–11:30 UTC.** First all-region soak since soak5; first SIN client
metrics since soak1. Config identical to soak6 (adaptive gather 6 ms,
debug stage timers, ring off, RESOLV_OVERRIDE on) on the same binaries;
explicit `--http-port 8080` throughout (see
repro-edge-404/CAMPAIGN-2026-08-03.md for why that now matters).

**Zero errors in every region. Zero platform shape events** (gates:
canary reachable in 24 s; all 12 first-byte gates ≤1 s from check start;
post-hoc generator-counter monotonicity clean — no replica flaps).

## Headline (soak1 → 5 → 6 → 9; SIN has only soak1 to compare)

| region | append p50 | roundtrip p50 | ceiling rps | t2/t1 | wake / read (p50) |
|---|---|---|---|---|---|
| us-west-1 | 101→87→109→**63** | 130→113→137→**88** | 402→**490** | 1.01 | 18 / 19 ms |
| eu-central-1 | 100→59→61→**58** | 181→90→91→**82** | 490→490 | 0.99 | 23 / 17 ms |
| eu-west-3 | 119→68→82→**78** | 164→111→122→**112** | 486→**490** | 1.01 | 23 / 26 ms |
| ap-northeast-1 | 78→54→55→86* | 111→86→89→119* | 490→490 | 1.02 | 25 / 22 ms |
| us-east-1 | 456→341→297→**241** | 539→413→385→**278** | 132→**178** | 1.16 | 129 / 31 ms |
| ap-southeast-1 | 124→**542**† | 329→**655**† | 438→101† | 1.62† | 123 / 21 ms |

\* NRT regressed ~30 ms this window — the same cross-run Tigris variance
that hit SJC in soak6 (now reversed: SJC posts its best-ever 63 ms).
The gather shape (t2/t1 ≈ 1.0) holds in both.

† **SIN is store-side, not platform and not us:** even idle-period small
GETs to Tigris SIN cost 473–475 ms p50 (n=1,938) at **100 % local
serving** — ~10× the healthy-region cost. At ~half-second store ops the
closed-loop herd naturally outruns a 6 ms gather window (hence 1.62) and
the ceiling caps at 101 rps. Zero errors; routing correct; the Compute
platform's SIN leg was flawless (canary 24 s, gates instant). Tigris SIN
appears mid-migration (its NS1 answers still list old frontend IPs while
FRA moved to new ranges) — worth a line in the Tigris thread.

us-east posts best-ever numbers across the board (241/278/178 rps) —
plausibly the same Tigris re-IP consolidating EWR onto the ord frontends
that every resolver now returns.

## Platform validation verdict (the original soak8 question)

With explicit ports, the Compute platform is **clean in all six
regions**: publication immediate, boots normal, no 404 windows, no
replica flaps, 30 minutes of ramp to conc-64 without a single error.
The remaining platform items are the flag-less port default (3000 vs
documented 8080) and the indistinguishable-404 rendering — both filed
with exact repros.

## Ops note

The campaign's tick monitor died one minute into the ramp on a macOS
bash-3.2 associative-array bug ("us: unbound variable"); the soak is
generator-driven and completed unaffected; harvest ran directly and the
Shape-D check was reconstructed post-hoc from the generators' retained
windows (monotonic everywhere). Lesson added to bench/soak/README
invariants: campaign scripts must be bash-3.2-clean — no `declare -A`.
