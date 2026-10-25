#!/usr/bin/env python3
"""K2 cost experiment: a run's screening counts, and two runs side by side.

  python3 bench/k2cost/counts.py RUN_DIR
      writes RUN_DIR/counts.json and prints it: per window, the store
      requests by tier/kind/op cell (s3lite stats2, every status), Class A
      and B (k2cells' classes), bytes PUT and GET (s3lite stats), the pump's
      WAL flushes and the tail ring's hits and misses (/v1/debug/timings,
      summed over servers and shards), and each process's CPU seconds.
  python3 bench/k2cost/counts.py --compare REF_RUN CAND_RUN
      [--window NAME ...] [--per-hour] [--top N]
      the windows both runs have, REF and CAND and the difference: the
      headline counts, then the cells that differ most (N, 25).

Counts, not dollars: a screening pair (the same point on two binaries, at
the same time, on two port bases) is read by its counts, which host load
cannot move; price.py's summary.json prices each run on its own.

Windows (from the snapshots run-local.sh takes): load (start -> loadend),
deferred (loadend -> quiesced), to_quiesced (start -> quiesced), tail
(quiesced -> end), total (start -> end), one per phase group
"phases J-I" (the boundary snapshots at its first phase's start and its
foreground phase's end), and one per manifest period "period NAME" (the
boundary snapshots a tenants phase takes at the period's start and end).
A window across a restart has no pump counts (the counters start again
with the boot).
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from k2cells import cell_delta, jsonl, load, price_cells

HEADLINE = ("secs", "class_a", "class_b", "put_bytes", "get_bytes", "pump_flushes", "pump_flushed_reqs",
            "ring_hits", "ring_misses")
PUMP_KEYS = ("flushes", "barrier_acked", "flushed_reqs", "flushed_records", "flushed_bytes")
RING_KEYS = ("hits", "misses", "published", "evicted")


def snapshots(run: str) -> dict:
    """label -> {"t_ms", "s3lite": stats2 (+ stats), "servers", "procs"}."""
    out = {}
    for label in ("start", "loadend", "quiesced", "end"):
        s3 = load(os.path.join(run, f"s3lite-{label}.json"))
        if not s3:
            continue
        store = load(os.path.join(run, f"store-{label}.json"), {}) or {}
        out[label] = {"t_ms": s3.get("t_ms"), "s3lite": s3, "servers": store.get("servers") or {},
                      "procs": store.get("procs") or {}}
    for b in jsonl(os.path.join(run, "boundaries.jsonl")):
        out[b["label"]] = {"t_ms": b.get("t_ms"), "s3lite": b.get("s3lite") or {}, "servers": b.get("servers") or {},
                           "procs": b.get("procs") or {}}
    return out


def phase_windows(marks: list) -> list:
    """(name, start label, end label) per phase group: a run of background
    phases ends with the foreground phase after it."""
    out, first = [], None
    for m in marks:
        if m.get("mark") != "phase_start":
            continue
        i = m.get("i")
        first = i if first is None else first
        if not m.get("bg"):
            out.append((f"phases {first}-{i}" if first != i else f"phase {i}", f"phase-{first}-start", f"phase-{i}-end"))
            first = None
    return out


def period_windows(marks: list) -> list:
    """(name, start label, end label) per manifest period a tenants phase
    marked (run-local.sh: a `period_start` mark beside each boundary
    snapshot `period-NAME-start`, and `period-NAME-end`), in mark order."""
    names = [m.get("name") for m in marks if m.get("mark") == "period_start" and m.get("name")]
    return [(f"period {n}", f"period-{n}-start", f"period-{n}-end") for n in dict.fromkeys(names)]


def counters(servers: dict, section: str, keys: tuple) -> dict:
    """server/shard -> {key: cumulative} of one timings section."""
    out = {}
    for name, srv in (servers or {}).items():
        for shard, t in ((srv or {}).get("timings") or {}).items():
            sec = (t or {}).get(section) or {}
            out[f"{name}/{shard}"] = {k: int(sec.get(k) or 0) for k in keys}
    return out


def counter_delta(a: dict, b: dict, section: str, keys: tuple):
    """Summed and per-shard deltas; None when a server rebooted in between
    or either side has no timings."""
    boots = lambda s: {n: (v or {}).get("boot_id") for n, v in (s or {}).items()}  # noqa: E731
    if boots(a["servers"]) != boots(b["servers"]):
        return None, None
    ca, cb = counters(a["servers"], section, keys), counters(b["servers"], section, keys)
    if not ca or not cb:
        return None, None
    by = {k: {f: cb[k][f] - ca.get(k, {}).get(f, 0) for f in keys} for k in cb}
    return {f: sum(v[f] for v in by.values()) for f in keys}, by


def window(a: dict, b: dict) -> dict:
    cells = cell_delta((a["s3lite"] or {}).get("cells") or {}, (b["s3lite"] or {}).get("cells") or {})
    priced = price_cells(cells)
    sa, sb = (a["s3lite"].get("stats") or {}), (b["s3lite"].get("stats") or {})
    pump, pump_by = counter_delta(a, b, "pump", PUMP_KEYS)
    ring, _ = counter_delta(a, b, "tail_ring", RING_KEYS)
    cpu = {}
    for name, p in (b.get("procs") or {}).items():
        pa = (a.get("procs") or {}).get(name) or {}
        if p.get("cpu_s") is not None and pa.get("cpu_s") is not None and p.get("pid") == pa.get("pid"):
            cpu[name] = round(p["cpu_s"] - pa["cpu_s"], 2)
    classes = {r["cell"]: r["class"] for r in priced["cells"]}
    return {
        "secs": round((b["t_ms"] - a["t_ms"]) / 1000, 1),
        "class_a": priced["totals"]["class_a"], "class_b": priced["totals"]["class_b"],
        "free": priced["totals"]["free"], "usd": priced["totals"]["usd"],
        "put_bytes": int(sb.get("put_bytes") or 0) - int(sa.get("put_bytes") or 0),
        "get_bytes": int(sb.get("get_bytes") or 0) - int(sa.get("get_bytes") or 0),
        "pump_flushes": (pump or {}).get("flushes"), "pump_flushed_reqs": (pump or {}).get("flushed_reqs"),
        "ring_hits": (ring or {}).get("hits"), "ring_misses": (ring or {}).get("misses"),
        "pump": pump, "pump_by_shard": pump_by, "ring": ring, "cpu_s": cpu,
        "cells": {k: {"class": classes.get(k), "n": sum(v.values()), "by_status": v} for k, v in sorted(cells.items())},
    }


def counts(run: str) -> dict:
    snaps = snapshots(run)
    if "start" not in snaps or "end" not in snaps:
        raise SystemExit(f"counts: {run} has no s3lite-start.json or s3lite-end.json")
    q = "quiesced" if "quiesced" in snaps else None
    spans = [("load", "start", "loadend"), ("deferred", "loadend", q), ("to_quiesced", "start", q),
             ("tail", q, "end"), ("total", "start", "end")]
    marks = jsonl(os.path.join(run, "marks.jsonl"))
    spans += phase_windows(marks) + period_windows(marks)
    windows = {name: {"from": a, "to": b, **window(snaps[a], snaps[b])}
               for name, a, b in spans if a in snaps and b in snaps}
    posture = load(os.path.join(run, "posture.json"), {}) or {}
    summary = load(os.path.join(run, "summary.json"), {}) or {}
    sc = posture.get("server_commit") or {}
    return {
        "run": run, "point": posture.get("point"), "posture": posture.get("posture"),
        "port_base": posture.get("port_base"),
        "server": {"commit": sc.get("reported"), "expected": sc.get("expected"), "source": sc.get("source"),
                   "sha256": (posture.get("binaries_sha256") or {}).get("streams-slate"),
                   "path": ((posture.get("binaries") or {}).get("streams-slate") or {}).get("path")},
        "valid": (summary.get("validity") or {}).get("valid"),
        # streams-1's environment as posture.json records it (secrets redacted)
        "server_env": (((posture.get("servers") or {}).get("streams-1") or {}).get("env") or {}),
        "windows": windows,
    }


PER_RUN_ENV = {"SLATE_S3_ENDPOINT", "SLATE_S3_BUCKET", "SELF_URL"}  # differ between any two runs


def env_diff(ref: dict, cand: dict) -> list:
    a, b = ref.get("server_env") or {}, cand.get("server_env") or {}
    return [(k, a.get(k), b.get(k)) for k in sorted(set(a) | set(b)) if k not in PER_RUN_ENV and a.get(k) != b.get(k)]


def fmt(v) -> str:
    if v is None:
        return "-"
    if isinstance(v, float) and not v.is_integer():
        return f"{v:,.1f}"
    return f"{int(v):,}"


def header(c: dict) -> str:
    s = c["server"]
    valid = {True: "VALID", False: "VOID", None: "unpriced"}[c["valid"]]
    return (f"{c['point']} [{c['posture']} posture, ports {c['port_base']}] server {','.join(s['commit'] or ['?'])[:12]}"
            f" ({s['source']}) sha256 {(s['sha256'] or '?')[:12]}: {valid}\n  {c['run']}")


def print_counts(c: dict, top: int) -> None:
    print(f"== {header(c)}")
    width = {h: max(13, len(h) + 2) for h in HEADLINE}
    print(f"{'window':<16}" + "".join(f"{h:>{width[h]}}" for h in HEADLINE))
    for name, w in c["windows"].items():
        print(f"{name:<16}" + "".join(f"{fmt(w.get(h)):>{width[h]}}" for h in HEADLINE))
    w = c["windows"].get("to_quiesced") or c["windows"]["load"]
    print(f"\ncells ({'to_quiesced' if 'to_quiesced' in c['windows'] else 'load'}, top {top} by requests):")
    for k, v in sorted(w["cells"].items(), key=lambda kv: -kv[1]["n"])[:top]:
        print(f"  {k:<36}{v['class'] or '?':>5}{fmt(v['n']):>12}")


def compare(ref: dict, cand: dict, names: list, per_hour: bool, top: int) -> None:
    print(f"== ref  {header(ref)}\n== cand {header(cand)}")
    for k, x, y in env_diff(ref, cand):
        print(f"   env {k}: ref {x if x is not None else '(unset)'} -> cand {y if y is not None else '(unset)'}")
    for name in names or [n for n in ref["windows"] if n in cand["windows"]]:
        a, b = ref["windows"].get(name), cand["windows"].get(name)
        if not a or not b:
            print(f"\n-- {name}: not in both runs")
            continue
        scale = (lambda w: 3600 / w["secs"] if w["secs"] else 0) if per_hour else (lambda w: 1)
        print(f"\n-- {name} ({a['secs']:.0f} s vs {b['secs']:.0f} s{', per hour' if per_hour else ''})")
        print(f"  {'':<36}{'ref':>15}{'cand':>15}{'cand-ref':>15}{'%':>8}")
        rows = [(h, a.get(h), b.get(h)) for h in HEADLINE if h != "secs"]
        keys = set(a["cells"]) | set(b["cells"])
        cells = [(k, (a["cells"].get(k) or {}).get("n", 0), (b["cells"].get(k) or {}).get("n", 0)) for k in keys]
        cells.sort(key=lambda r: -abs(r[2] * scale(b) - r[1] * scale(a)))
        for label, x, y in rows + cells[:top]:
            xs = None if x is None else x * scale(a)
            ys = None if y is None else y * scale(b)
            d = None if xs is None or ys is None else ys - xs
            pct = f"{100 * d / xs:+.0f}%" if d is not None and xs else "-"
            print(f"  {label:<36}{fmt(xs):>15}{fmt(ys):>15}{fmt(d):>15}{pct:>8}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run", nargs="?")
    ap.add_argument("--compare", nargs=2, metavar=("REF_RUN", "CAND_RUN"))
    ap.add_argument("--window", action="append", default=[], help="compare only these windows (repeatable)")
    ap.add_argument("--per-hour", action="store_true", help="compare rates per hour of each window")
    ap.add_argument("--top", type=int, default=25)
    args = ap.parse_args()
    if args.compare:
        ref, cand = (counts(r.rstrip("/")) for r in args.compare)
        compare(ref, cand, args.window, args.per_hour, args.top)
        return 0
    if not args.run:
        ap.error("RUN_DIR or --compare REF_RUN CAND_RUN")
    c = counts(args.run.rstrip("/"))
    with open(os.path.join(args.run, "counts.json"), "w", encoding="utf-8") as f:
        json.dump(c, f, indent=1, sort_keys=True)
    print_counts(c, args.top)
    return 0


if __name__ == "__main__":
    sys.exit(main())
