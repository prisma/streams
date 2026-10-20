#!/usr/bin/env python3
"""The Layer B run's summary for a public log (.github/workflows/layer-b.yml).

    python3 bench/k2cost/layer_b_summary.py --results DIR [--rig-log LOG]

DIR is run-shared.sh's results directory ($K2_HOME/results), holding one
layer-b-<stamp> run. The owner's raw-evidence hold keeps the manifest,
the scrape and the logs on the host that ran the rig; this prints only:
each bar of the manifest, PASS or FAIL with its value (cut at 300
characters) and its bar; one RSS line (the peak, the plateau over the
load's last 30 min, the RSS and all admission sheds during the load, the
container's cgroup peak, the boots; MB are MiB, as the scrape's rss_mb);
the driver-pools fact, one line per
saturated pool; and the verdict line with its known-red legs. Exit 0 for
PASS, 1 for FAIL.

A run with no manifest (the rig stopped before its verdict) prints
`layer-b: NO VERDICT`, the last 40 lines of LOG (run-shared.sh's own
output) and, when its last line names a file under DIR as `(see PATH)`,
that file's last 40 lines: what a failing step needs, nothing more. Exit 2.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import k2valid  # noqa: E402
import shared_verdict as V  # noqa: E402

TAIL_LINES = 40
VALUE_CHARS = 300


def tail(path: str) -> list:
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.read().splitlines()[-TAIL_LINES:]


def no_verdict(why: str, results: str, rig_log: str | None) -> int:
    print(f"layer-b: NO VERDICT ({why})")
    if rig_log and os.path.isfile(rig_log):
        lines = tail(rig_log)
        print(f"--- the last {TAIL_LINES} lines of {rig_log}")
        for line in lines:
            print(line)
        named = re.search(r"\(see (\S+)\)\s*$", lines[-1]) if lines else None
        path = os.path.realpath(named.group(1)) if named else None
        if path and path.startswith(os.path.realpath(results) + os.sep) and os.path.isfile(path):
            print(f"--- the last {TAIL_LINES} lines of {named.group(1)}")
            for line in tail(path):
                print(line)
    return 2


def rss_line(r: V.Run) -> str:
    start, end = r.marks.get("load_start"), r.marks.get("load_end")
    rss = [float(b.get("rss_mb") or 0) for _, b in r.loads]
    peak = max(rss, default=0.0)
    plateau = [float(b.get("rss_mb") or 0) for t, b in r.loads
               if end is not None and end - V.SLOPE_WINDOW_SECS * 1000 <= t <= end]
    pts = r.window_loads(start, end) if start is not None and end is not None else []
    sheds = k2valid.boot_delta(pts, lambda b: b.get("admit_shed_rss")) if len(pts) >= 2 else 0
    all_sheds = k2valid.boot_delta(pts, lambda b: b.get("admit_shed")) if len(pts) >= 2 else 0
    cgroup = max((int(b.get("cgroup_peak_bytes") or 0) for _, b in r.loads), default=0)
    boots = len({b.get("boot_id") for _, b in r.loads if b.get("boot_id")})
    med = f"{statistics.median(plateau):.1f}" if plateau else "-"
    top = f"{max(plateau):.1f}" if plateau else "-"
    return (f"RSS: peak {peak:.1f} MB; plateau (the load's last {V.SLOPE_WINDOW_SECS // 60} min) median {med} MB, max {top} MB; "
            f"RSS sheds during the load {sheds} (all admission sheds {all_sheds}); "
            f"container peak {cgroup / 1048576:.1f} MB (cgroup); boots {boots}")


def pool_lines(fact: dict | None) -> list:
    if not fact:
        return ["FACT driver-pools: none (the driver recorded no pools)"]
    saturated = fact.get("saturated") or {}
    out = [f"FACT driver-pools: {len(saturated)} of {fact.get('pools')} pools saturated ({fact.get('sockets')} sockets each)"]
    for name, p in sorted(saturated.items()):
        out.append(f"  {name}: peak waiting {p.get('peak_waiting')}, wait max {p.get('wait_ms_max')} ms, unsent {p.get('unsent')}")
    return out


def summary(run_dir: str) -> tuple:
    with open(os.path.join(run_dir, "layer-b-manifest.json"), encoding="utf-8") as f:
        m = json.load(f)
    lines = [f"{'PASS' if b['pass'] else 'FAIL'} {name}: {json.dumps(b['value'], sort_keys=True)[:VALUE_CHARS]} (bar {b['bar']})"
             for name, b in m["bars"].items()]
    lines.append(rss_line(V.Run(run_dir)))
    lines += pool_lines((m.get("facts") or {}).get("driver-pools"))
    red = sorted(m.get("known_red") or {})
    verdict = f"{m['arm']}: {m['verdict']} {json.dumps(m['legs'], sort_keys=True)}"
    lines.append(verdict + (f" (known red: {', '.join(red)})" if red else ""))
    return lines, 0 if m["verdict"] == "PASS" else 1


def main(argv: list | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--results", required=True, help="run-shared.sh's results directory ($K2_HOME/results)")
    ap.add_argument("--rig-log", help="run-shared.sh's own output, tailed when the run has no verdict")
    a = ap.parse_args(argv)
    runs = sorted(d for d in glob.glob(os.path.join(a.results, "layer-b-*")) if os.path.isdir(d))
    if not runs:
        return no_verdict(f"no layer-b-* run under {a.results}", a.results, a.rig_log)
    if len(runs) > 1:
        return no_verdict(f"{len(runs)} layer-b-* runs under {a.results}, expected one", a.results, None)
    if not os.path.isfile(os.path.join(runs[0], "layer-b-manifest.json")):
        return no_verdict(f"{runs[0]} has no layer-b-manifest.json", a.results, a.rig_log)
    lines, code = summary(runs[0])
    for line in lines:
        print(line)
    return code


if __name__ == "__main__":
    sys.exit(main())
