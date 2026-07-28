#!/usr/bin/env python3
"""Render one comparison table from N wide-test run dirs (run-wide.sh).

Splits each run's s3lite ledger into setup (t0 -> SETUP_DONE snapshot)
and steady (SETUP_DONE -> final), and normalizes: setup Class A per
stream; steady Class A/B per active-stream-minute and per M records."""
import json, sys
from pathlib import Path
from statistics import median

STATUSES = ["2xx", "304", "404", "412", "4xx", "5xx"]

def load_stats2(p):
    return json.loads(Path(p).read_text())

def cells_delta(a, b):
    """b - a per cell/status; missing cells treated as zero."""
    out = {}
    keys = set(a.get("cells", {})) | set(b.get("cells", {}))
    for k in keys:
        av, bv = a.get("cells", {}).get(k, {}), b.get("cells", {}).get(k, {})
        d = {s: bv.get(s, 0) - av.get(s, 0) for s in STATUSES}
        if any(d.values()):
            out[k] = d
    return out

def billing(cells):
    a = b = free = 0
    for name, statuses in cells.items():
        op = name.rsplit("/", 1)[1]
        for s, v in statuses.items():
            if s == "2xx" and op in ("put", "multipart", "list"):
                a += v
            elif s == "2xx" and op in ("get", "head"):
                b += v
            else:
                free += v
    return a, b, free

def load_run(run):
    run = Path(run)
    out = {"dir": run.name}
    t0 = load_stats2(run / "snaps/t0-stats2.json")
    setup = load_stats2(run / "snaps/setup-stats2.json")
    final = load_stats2(run / "snaps/final-stats2.json")
    out["setup_cells"] = cells_delta(t0, setup)
    out["steady_cells"] = cells_delta(setup, final)
    out["setup_bill"] = billing(out["setup_cells"])
    out["steady_bill"] = billing(out["steady_cells"])
    lines = [json.loads(l) for l in (run / "wide.jsonl").read_text().splitlines() if l.strip()]
    out["setup_line"] = next(l for l in lines if l.get("phase") == "setup")
    steady = [l for l in lines if l.get("phase") == "steady"]
    out["last"] = steady[-1]
    def med(key):
        vals = [l[key] for l in steady if l[key] > 0]
        return median(vals) if vals else 0.0

    out["ap_p50"] = med("apWinP50Ms")
    out["ap_p99"] = med("apWinP99Ms")
    # A regime with zero inactive streams runs no scanner.
    out["sc_p50"] = med("scWinP50Ms")
    out["sc_p99"] = med("scWinP99Ms")
    out["sc_p99_max"] = max(l["scWinP99Ms"] for l in steady)
    rss = [int(l.split()[1]) for l in (run / "rss.log").read_text().splitlines() if len(l.split()) == 2]
    out["rss_max_mb"] = max(rss) / 1024 if rss else 0
    out["rss_final_mb"] = rss[-1] / 1024 if rss else 0
    # absorb backlog at the end: streams still carrying lag.
    lagging = lag_max = 0
    up = run / "snaps/final-usage.json"
    if up.exists():
        try:
            u = json.loads(up.read_text())
            lags = [s.get("absorb_lag_secs", 0) for s in u.get("streams", [])]
            lagging = sum(1 for x in lags if x > 0)
            lag_max = max(lags, default=0)
        except Exception:
            pass
    out["lagging"], out["lag_max"] = lagging, lag_max
    return out

def main(dirs):
    runs = [load_run(d) for d in dirs]
    hdr = "".join(f"{r['dir']:>16s}" for r in runs)
    print(f"{'':34s}{hdr}")
    def row(label, fmt, f):
        print(f"{label:34s}" + "".join(f"{f(r):>16{fmt}}" for r in runs))
    row("streams", ",d", lambda r: r["setup_line"]["streams"])
    row("active", ",d", lambda r: r["setup_line"]["active"])
    print("-- setup --")
    row("create+seed secs", ",.0f", lambda r: (r["setup_line"]["createMs"] + r["setup_line"]["seedMs"]) / 1000)
    row("setup Class A", ",d", lambda r: r["setup_bill"][0])
    row("setup Class B", ",d", lambda r: r["setup_bill"][1])
    row("setup Class A / stream", ",.2f", lambda r: r["setup_bill"][0] / r["setup_line"]["streams"])
    print("-- steady (15 min) --")
    row("records appended", ",d", lambda r: r["last"]["apOk"] * 10)
    row("append err/thr", ",d", lambda r: r["last"]["apErr"] + r["last"]["apThr"])
    row("scans ok / err", "s", lambda r: f"{r['last']['scOk']}/{r['last']['scErr']}")
    row("steady Class A", ",d", lambda r: r["steady_bill"][0])
    row("steady Class B", ",d", lambda r: r["steady_bill"][1])
    row("steady free", ",d", lambda r: r["steady_bill"][2])
    row("Class A / stream-min", ",.2f", lambda r: r["steady_bill"][0] / (r["setup_line"]["active"] * 15))
    row("Class A / M records", ",.0f", lambda r: r["steady_bill"][0] / (r["last"]["apOk"] * 10) * 1e6)
    row("Class B / M records", ",.0f", lambda r: r["steady_bill"][1] / (r["last"]["apOk"] * 10) * 1e6)
    print("-- perf guardrails --")
    row("append p50 ms (med of windows)", ",.1f", lambda r: r["ap_p50"])
    row("append p99 ms (med of windows)", ",.1f", lambda r: r["ap_p99"])
    row("scan p50 ms", ",.1f", lambda r: r["sc_p50"])
    row("scan p99 ms (med / worst win)", "s", lambda r: f"{r['sc_p99']:.0f}/{r['sc_p99_max']:.0f}")
    row("RSS max MB / final MB", "s", lambda r: f"{r['rss_max_mb']:.0f}/{r['rss_final_mb']:.0f}")
    row("streams still absorb-lagging", ",d", lambda r: r["lagging"])
    row("max absorb lag secs", ",d", lambda r: r["lag_max"])
    print()
    print("== steady cells (top movers per run) ==")
    for r in runs:
        print(f"-- {r['dir']}")
        items = sorted(r["steady_cells"].items(), key=lambda kv: -sum(kv[1].values()))
        for name, st in items[:14]:
            tot = sum(st.values())
            det = " ".join(f"{k}={v}" for k, v in st.items() if v)
            print(f"  {name:32s}{tot:>9,}   {det}")

if __name__ == "__main__":
    main(sys.argv[1:])
