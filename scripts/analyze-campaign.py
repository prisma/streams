#!/usr/bin/env python3
"""AWS comparison campaign: per-system per-shape summary + ceiling curves.

Usage: analyze-campaign.py <results-dir>   (files named <system>-<shape>.json,
each a JSON array of awsbench window samples)."""
import json
import statistics as st
import sys
from pathlib import Path

D = Path(sys.argv[1])
SYSTEMS = ["kinesis", "sqs", "prisma"]


def load(system, shape):
    p = D / f"{system}-{shape}.json"
    if not p.exists():
        return []
    try:
        return json.loads(p.read_text())
    except Exception:
        return []


def med(xs):
    xs = [x for x in xs if x is not None]
    return st.median(xs) if xs else None


def fmt(v, d=1):
    if v is None:
        return "-"
    return f"{v:,.{d}f}" if isinstance(v, float) else f"{v:,}"


print("== shape A: latency floor (conc=1, 1x200B) ==")
print(f"{'system':>8} {'req/s':>7} {'p50ms':>7} {'p99ms':>8}")
for s in SYSTEMS:
    rows = load(s, "a")
    if not rows:
        print(f"{s:>8}  (no data)")
        continue
    print(
        f"{s:>8} {fmt(med([r['achievedPerSec'] for r in rows]),0):>7} "
        f"{fmt(med([r['winP50Ms'] for r in rows])):>7} "
        f"{fmt(med([r['winP99Ms'] for r in rows])):>8}"
    )

print("\n== shape B: record ceiling (batch 16x200B, conc sweep) ==")
print(f"{'conc':>5}", end="")
for s in SYSTEMS:
    print(f" | {s+' rec/s':>13} {'p50':>7} {'thr/20s':>8}", end="")
print()
concs = sorted({r["conc"] for s in SYSTEMS for r in load(s, "b")})
prev_thr = {s: 0 for s in SYSTEMS}
for c in concs:
    print(f"{c:>5}", end="")
    for s in SYSTEMS:
        rows = [r for r in load(s, "b") if r["conc"] == c]
        if not rows:
            print(f" | {'-':>13} {'-':>7} {'-':>8}", end="")
            continue
        thr_delta = rows[-1]["throttled"] - prev_thr[s]
        prev_thr[s] = rows[-1]["throttled"]
        print(
            f" | {fmt(med([r['recordsPerSec'] for r in rows]),0):>13}"
            f" {fmt(med([r['winP50Ms'] for r in rows])):>7}"
            f" {fmt(thr_delta,0):>8}",
            end="",
        )
    print()

print("\n== shape B ceilings (max sustained rec/s across sweep) ==")
for s in SYSTEMS:
    rows = load(s, "b")
    if rows:
        best = max(med([r["recordsPerSec"] for r in rows if r["conc"] == c]) or 0 for c in {x["conc"] for x in rows})
        print(f"{s:>8}: {fmt(best,0)} rec/s")

print("\n== shape C: byte ceiling (conc=8, 1x16KB) ==")
print(f"{'system':>8} {'req/s':>7} {'MB/s':>7} {'p50ms':>7} {'p99ms':>8} {'thr':>7}")
for s in SYSTEMS:
    rows = load(s, "c")
    if not rows:
        print(f"{s:>8}  (no data)")
        continue
    rps = med([r["recordsPerSec"] for r in rows]) or 0
    print(
        f"{s:>8} {fmt(med([r['achievedPerSec'] for r in rows]),0):>7} "
        f"{fmt(rps*16384/1e6,2):>7} {fmt(med([r['winP50Ms'] for r in rows])):>7} "
        f"{fmt(med([r['winP99Ms'] for r in rows])):>8} {fmt(rows[-1]['throttled'],0):>7}"
    )

print("\n== shape D: tail freshness (producer->consumer) ==")
print(f"{'system':>8} {'prod rec/s':>10} {'tail p50 ms':>12} {'tail p99 ms':>12}")
for s in SYSTEMS:
    rows = [r for r in load(s, "d") if r.get("tailP50Ms") is not None]
    if not rows:
        print(f"{s:>8}  (no tail data)")
        continue
    print(
        f"{s:>8} {fmt(med([r['recordsPerSec'] for r in rows]),0):>10} "
        f"{fmt(med([r['tailP50Ms'] for r in rows])):>12} "
        f"{fmt(med([r['tailP99Ms'] for r in rows])):>12}"
    )

print("\n== shape E: overload (2x+ ceiling offered) ==")
print(f"{'system':>8} {'goodput rec/s':>13} {'p50ms':>7} {'throttles':>10} {'errors':>8}")
for s in SYSTEMS:
    rows = load(s, "e")
    if not rows:
        print(f"{s:>8}  (no data)")
        continue
    print(
        f"{s:>8} {fmt(med([r['recordsPerSec'] for r in rows]),0):>13} "
        f"{fmt(med([r['winP50Ms'] for r in rows])):>7} "
        f"{fmt(rows[-1]['throttled'],0):>10} {fmt(rows[-1]['errs'],0):>8}"
    )
