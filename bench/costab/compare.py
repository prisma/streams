#!/usr/bin/env python3
"""Compare two run-soak.sh output dirs: physical object-store requests by
billing class (s3lite ground truth), plus latency/integrity guardrails."""
import json, sys, re
from pathlib import Path

def load(run):
    run = Path(run)
    out = {"dir": run}
    out["stats2"] = json.loads((run / "snaps/final-stats2.json").read_text())
    out["stats"] = json.loads((run / "snaps/final-stats.json").read_text())
    tiers = []
    for line in (run / "gen.log").read_text().splitlines():
        line = line.strip()
        if line.startswith("{") and '"label"' in line:
            tiers.append(json.loads(line))
    out["tiers"] = tiers
    rss = []
    p = run / "rss.log"
    if p.exists():
        for line in p.read_text().splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[1].isdigit():
                rss.append(int(parts[1]))
    out["rss_max_mb"] = max(rss) / 1024 if rss else 0
    return out

def records(run):
    # awsbench's ok/recordsDecoded counters are cumulative across tiers.
    return run["tiers"][-1]["ok"] * run["tiers"][-1]["batch"]

def cell(run, name):
    return run["stats2"]["cells"].get(name, {})

def total_of(c):
    return sum(c.values())

def fmt_delta(b, a):
    if b == 0:
        return "—" if a == 0 else f"+{a}"
    return f"{(a - b) / b * 100:+.1f}%"

def main(base_dir, after_dir):
    b, a = load(base_dir), load(after_dir)
    rb, ra = records(b), records(a)
    print(f"records appended: baseline {rb:,} vs after {ra:,}")
    print(f"errors (cumulative): baseline {b['tiers'][-1]['errs']} after {a['tiers'][-1]['errs']}")
    intg = lambda r: (records(r), r["tiers"][-1]["recordsDecoded"])
    print(f"integrity (acked vs decoded): baseline {intg(b)} after {intg(a)}\n")

    print("== billing rollup (s3lite physical requests) ==")
    print(f"{'':22s}{'baseline':>12s}{'after':>12s}{'delta':>9s}")
    for scope in ["total"] + sorted(set(b["stats2"]["by_tier"]) | set(a["stats2"]["by_tier"])):
        bt = b["stats2"]["total"] if scope == "total" else b["stats2"]["by_tier"].get(scope, {})
        at = a["stats2"]["total"] if scope == "total" else a["stats2"]["by_tier"].get(scope, {})
        for cls in ["class_a", "class_b", "free"]:
            bv, av = bt.get(cls, 0), at.get(cls, 0)
            print(f"{scope+'/'+cls:22s}{bv:>12,}{av:>12,}{fmt_delta(bv, av):>9s}")
    print()

    print("== per-million-records (normalized) ==")
    for cls in ["class_a", "class_b"]:
        bv = b["stats2"]["total"][cls] / rb * 1e6
        av = a["stats2"]["total"][cls] / ra * 1e6
        print(f"{cls:>10s}/M records: baseline {bv:,.0f}  after {av:,.0f}  ({(av-bv)/bv*100:+.1f}%)")
    print()

    print("== notable cells (2xx unless suffixed) ==")
    names = sorted(set(b["stats2"]["cells"]) | set(a["stats2"]["cells"]))
    print(f"{'cell':34s}{'baseline':>10s}{'after':>10s}{'delta':>9s}")
    for n in names:
        bv, av = total_of(cell(b, n)), total_of(cell(a, n))
        if max(bv, av) < 20:
            continue
        print(f"{n:34s}{bv:>10,}{av:>10,}{fmt_delta(bv, av):>9s}")
    print()

    print("== bytes at the store ==")
    for k in ["put_bytes", "get_bytes"]:
        bv, av = b["stats"][k], a["stats"][k]
        print(f"{k:>10s}: baseline {bv/1e9:,.2f} GB  after {av/1e9:,.2f} GB  ({fmt_delta(bv, av)})")
    print(f"{'objects':>10s}: baseline {b['stats']['objects']:,}  after {a['stats']['objects']:,}")
    print()

    print("== registry refresh behavior ==")
    for run, tag in [(b, "baseline"), (a, "after")]:
        g = cell(run, "registry/meta/get")
        print(f"  {tag}: 2xx={g.get('2xx',0)} 304={g.get('304',0)} 404={g.get('404',0)}")
    print()

    print("== latency guardrails (per-tier medians of 20s windows) ==")
    print(f"{'tier':12s}{'base p50':>9s}{'aft p50':>9s}{'base p99':>9s}{'aft p99':>9s}{'base rec/s':>11s}{'aft rec/s':>11s}")

    def tier_medians(run):
        from statistics import median
        by = {}
        for t in run["tiers"]:
            by.setdefault(t["label"], []).append(t)
        out = {}
        for label, ws in by.items():
            # Drop the tier's last window (straddles the step-down/settle,
            # harness invariant 4) and any zero-rps startup window.
            ws = [w for w in ws[:-1] if w["recordsPerSec"] > 0] or ws
            out[label] = (
                median(w["winP50Ms"] for w in ws),
                median(w["winP99Ms"] for w in ws),
                median(w["recordsPerSec"] for w in ws),
            )
        return out

    bm, am = tier_medians(b), tier_medians(a)
    for label in sorted(bm):
        if label not in am:
            continue
        bp, ap = bm[label], am[label]
        print(f"{label:12s}{bp[0]:>9.1f}{ap[0]:>9.1f}{bp[1]:>9.1f}{ap[1]:>9.1f}{bp[2]:>11,.0f}{ap[2]:>11,.0f}")
    print(f"\nRSS max: baseline {b['rss_max_mb']:.0f} MB, after {a['rss_max_mb']:.0f} MB")

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
