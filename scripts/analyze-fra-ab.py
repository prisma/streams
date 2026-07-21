#!/usr/bin/env python3
"""FRA single-instance A/B: slate vs slate-codex. Steady-state comparison."""
import json, statistics as st, sys

import sys
A_FILE, B_FILE = sys.argv[1], sys.argv[2]  # baseline.jsonl candidate.jsonl
WARMUP = 6  # drop first 6 samples (2 min)

def load(arm):
    rows = [json.loads(l) for l in open(A_FILE if arm == "slate" else B_FILE)]
    return rows[WARMUP:]

def med(xs):
    xs = [x for x in xs if x is not None]
    return st.median(xs) if xs else None

def q(xs, p):
    xs = sorted(x for x in xs if x is not None)
    if not xs: return None
    return xs[min(len(xs) - 1, int(p * len(xs)))]

def summarize(arm):
    rows = load(arm)
    g = [r.get("gen", {}) for r in rows]
    ld = [r.get("load", {}) for r in rows]
    stor = [r.get("store", {}).get("ops", {}) for r in rows]

    def op(name, field):
        return [s[name][field] for s in stor if name in s and s[name].get("n", 0) > 0]

    rps = [x.get("achievedPerSec") for x in g]
    oks = [x.get("ok") for x in g if x.get("ok") is not None]
    errs = [x.get("errs") for x in g if x.get("errs") is not None]
    thr = [x.get("throttled") for x in g if x.get("throttled") is not None]
    out = {
        "samples": len(rows),
        "rps_med": med(rps), "rps_p10": q(rps, 0.10), "rps_p90": q(rps, 0.90),
        "evps_med": (med(rps) or 0) * 16,
        "winP50_med": med([x.get("winP50Ms") for x in g]),
        "winP99_med": med([x.get("winP99Ms") for x in g]),
        "winP99_p90": q([x.get("winP99Ms") for x in g], 0.90),
        "errs_delta": (errs[-1] - errs[0]) if errs else None,
        "throttled_delta": (thr[-1] - thr[0]) if thr else None,
        "ok_delta": (oks[-1] - oks[0]) if oks else None,
        "rss_med": med([x.get("rss_mb") for x in ld]),
        "rss_max": max([x.get("rss_mb", 0) for x in ld] or [0]),
        "inflight_max": max([x.get("inflight_peak", 0) for x in ld] or [0]),
        "shed": max([x.get("admit_shed", 0) for x in ld] or [0]),
        "putwal_p50_med": med(op("put:wal", "p50_ms")),
        "putwal_p99_med": med(op("put:wal", "p99_ms")),
        "putwal_n": sum(op("put:wal", "n")),
        "putsst_p50_med": med(op("put:sst", "p50_ms")),
        "putsst_p99_med": med(op("put:sst", "p99_ms")),
        "getsst_p50_med": med(op("get:sst", "p50_ms")),
        "store_err_total": sum(op("put:wal", "err")) + sum(op("put:sst", "err")) + sum(op("put:manifest", "err")),
    }
    return out

def fmt(v):
    if v is None: return "-"
    if isinstance(v, float): return f"{v:,.1f}"
    return f"{v:,}"

a = summarize("slate")
b = summarize("codex")
keys = [
    ("samples", "steady samples (20s)"),
    ("rps_med", "achieved req/s (median)"),
    ("rps_p10", "  req/s p10"),
    ("rps_p90", "  req/s p90"),
    ("evps_med", "events/s (median)"),
    ("winP50_med", "client p50 ms (median of 20s windows)"),
    ("winP99_med", "client p99 ms (median)"),
    ("winP99_p90", "client p99 ms (p90 of windows)"),
    ("ok_delta", "requests completed in window"),
    ("errs_delta", "client errors in window"),
    ("throttled_delta", "throttled (429) in window"),
    ("rss_med", "server RSS MB (median)"),
    ("rss_max", "server RSS MB (max)"),
    ("inflight_max", "server inflight peak"),
    ("shed", "admission sheds"),
    ("putwal_p50_med", "store put:wal p50 ms (median)"),
    ("putwal_p99_med", "store put:wal p99 ms (median)"),
    ("putwal_n", "store put:wal count"),
    ("putsst_p50_med", "store put:sst p50 ms (median)"),
    ("putsst_p99_med", "store put:sst p99 ms (median)"),
    ("getsst_p50_med", "store get:sst p50 ms (median)"),
    ("store_err_total", "store write errors (put wal+sst+manifest)"),
]
w = max(len(l) for _, l in keys)
print(f"{'metric'.ljust(w)}  {'baseline':>18}  {'candidate':>18}")
print("-" * (w + 40))
for k, label in keys:
    print(f"{label.ljust(w)}  {fmt(a[k]):>18}  {fmt(b[k]):>18}")

# Time-aligned decay: rps and winP50 by gen-elapsed-minute bucket (equal keyspace age).
print("\nrps / client-p50ms by gen-elapsed minutes (equal data age):")
print(f"{'gen-min':>8}  {'base rps':>10} {'p50ms':>8}  {'cand rps':>10} {'p50ms':>8}")
def buckets(arm):
    rows = [json.loads(l) for l in open(A_FILE if arm == "slate" else B_FILE)]
    out = {}
    for r in rows:
        g = r.get("gen", {})
        em = g.get("elapsedMin")
        if em is None: continue
        bkt = int(em // 2) * 2
        out.setdefault(bkt, []).append((g.get("achievedPerSec"), g.get("winP50Ms")))
    return out
ba, bb = buckets("slate"), buckets("codex")
for bkt in sorted(set(ba) | set(bb)):
    ra = med([x[0] for x in ba.get(bkt, [])]); pa = med([x[1] for x in ba.get(bkt, [])])
    rb = med([x[0] for x in bb.get(bkt, [])]); pb = med([x[1] for x in bb.get(bkt, [])])
    print(f"{f'{bkt}-{bkt+2}':>8}  {fmt(ra):>10} {fmt(pa):>8}  {fmt(rb):>10} {fmt(pb):>8}")
