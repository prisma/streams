#!/usr/bin/env python3
"""Round-12 analyzer: aggregate run manifests into the comparison report.

Usage: analyze.py <results-dir> [--json out.json]

Per shape x arm: median across repeats of the load-bearing metrics;
then the two decision gates (A-vs-B legacy comparison at >10%, B-vs-C
final-RC drift at ~5%) and the memory model fit
    RSS ~= base + conn_cost x subscribers + feed_cost x feeds
from the density sweep's parked-idle RSS points (arm-wise least squares
over shapes).
"""
import json, os, sys, statistics, itertools

def load(results):
    runs = []
    for d in sorted(os.listdir(results)):
        p = os.path.join(results, d, "manifest.json")
        if os.path.exists(p):
            try:
                m = json.load(open(p))
                m["_dir"] = d
                # The full proc series stays on disk beside the
                # manifest; per-phase CPU joins need it.
                pp = os.path.join(results, d, "proc.jsonl")
                m["_proc"] = []
                if os.path.exists(pp):
                    for line in open(pp):
                        line = line.strip()
                        if line:
                            try:
                                m["_proc"].append(json.loads(line))
                            except Exception:
                                pass
                runs.append(m)
            except Exception as e:
                print(f"WARN unreadable manifest {d}: {e}")
    return runs


def cpu_between(proc, start_ms, end_ms):
    """CPU seconds burned in [start_ms, end_ms], preferring the cgroup
    counter (accurate under --cpus quota), falling back to utime+stime.
    Interpolation-free: nearest samples at/before each bound; returns
    None when the series doesn't straddle the window."""
    if not proc or start_ms is None or end_ms is None:
        return None
    def at(ts):
        prev = None
        for s in proc:
            if s.get("t", 0) <= ts:
                prev = s
            else:
                break
        return prev
    a, b = at(start_ms), at(end_ms)
    if not a or not b or a is b:
        return None
    ca, cb = a.get("cg_cpu_usec"), b.get("cg_cpu_usec")
    try:
        if ca and cb:
            return (int(cb) - int(ca)) / 1e6
    except (TypeError, ValueError):
        pass
    return ((b["utime"] + b["stime"]) - (a["utime"] + a["stime"])) / 100.0

def phase(m, name):
    for ph in m.get("phases", []):
        if ph["name"] == name:
            return ph
    return None

def extract(m):
    """The per-run scalar metrics the gates consume."""
    out = {"verdict": m.get("verdict"), "gen_exit": m.get("gen_exit")}
    subs = m.get("shape", {}).get("subscribers", 0)
    feeds = m.get("shape", {}).get("feeds", 0)
    def rss(ph, side="server_after"):
        p = phase(m, ph)
        return (p or {}).get(side, {}) and (p or {}).get(side, {}).get("rss_mb")
    out["rss_boot"] = rss("warmup")
    out["rss_created"] = rss("create")
    out["rss_parked"] = rss("park")
    out["rss_idle"] = rss("idle")
    out["rss_teardown"] = rss("teardown")
    out["peak_rss_mb"] = round(m.get("proc_peak_rss_kb", 0) / 1024, 1)
    if out["rss_parked"] is not None and out["rss_boot"] is not None and subs:
        # NOTE: conflates feed+conn state; the model fit separates them.
        out["kb_per_sub_raw"] = round((out["rss_parked"] - out["rss_created"] if out["rss_created"] is not None else out["rss_boot"]) * 1024 / subs, 1)
    for ph in ("sparse", "fanout", "mixed", "slow"):
        p = phase(m, ph)
        if not p:
            continue
        c = p["client"]
        out[f"{ph}_del_s"] = c["deliveries_per_sec"]
        out[f"{ph}_dl_p50"] = c["delivery_latency_ms"]["p50"]
        out[f"{ph}_dl_p99"] = c["delivery_latency_ms"]["p99"]
        # Subscribed-append latency when the run recorded it (review
        # blocker 1: the combined histogram was 95% bogus-zero bg
        # samples on old runs); fall back for legacy manifests.
        ap = c.get("append_latency_subscribed_ms") or c["append_latency_ms"]
        out[f"{ph}_ap_p50"] = ap["p50"]
        out[f"{ph}_ap_p99"] = ap["p99"]
        out[f"{ph}_errors"] = c["append_errors"]
        out[f"{ph}_reconnects"] = c["reconnects"]
        out[f"{ph}_sched"] = c.get("appends_scheduled")
        out[f"{ph}_conc_drops"] = c.get("append_conc_drops")
        out[f"{ph}_overlaps"] = c.get("resume_overlaps")
        # Per-phase CPU (review blocker 2): the delta inside THIS
        # phase's window, ratioed to THIS phase's unique deliveries /
        # launched appends. Phase-attributed — the phase's own append,
        # heartbeat and bookkeeping cost rides along, and is labeled so.
        cpu = cpu_between(m.get("_proc"), p.get("start_ms"), p.get("end_ms"))
        if cpu is not None:
            out[f"{ph}_cpu_secs"] = round(cpu, 2)
            uniq = c.get("unique_deliveries", c["deliveries"])
            if uniq:
                out[f"{ph}_cpu_us_per_unique_delivery"] = round(cpu * 1e6 / uniq, 1)
            launched = c.get("appends_launched") or c.get("appends")
            if launched:
                out[f"{ph}_cpu_us_per_append"] = round(cpu * 1e6 / launched, 1)
    for ph in ("idle", "park"):
        p = phase(m, ph)
        if not p:
            continue
        cpu = cpu_between(m.get("_proc"), p.get("start_ms"), p.get("end_ms"))
        if cpu is not None and p.get("dur_secs"):
            out[f"{ph}_cpu_pct"] = round(100.0 * cpu / p["dur_secs"], 2)
    # Whole-run CPU: kept as a rough full-run cost indicator ONLY —
    # it spans creation, idle, teardown and every phase, and must not
    # be labeled per-delivery (it was, before the round-12 review).
    first, last = m.get("proc_first"), m.get("proc_last")
    if first and last:
        cpu_secs = ((last["utime"] + last["stime"]) - (first["utime"] + first["stime"])) / 100.0
        out["cpu_secs_full_run"] = round(cpu_secs, 1)
        out["fds_last"] = last.get("fds")
    rec = m.get("reconciliation", {})
    out["recon_missing"] = rec.get("missing")
    return out

def med(vals):
    vals = [v for v in vals if v is not None]
    return round(statistics.median(vals), 2) if vals else None

def aggregate(runs):
    table = {}
    for m in runs:
        shape = f"{m.get('shape', {}).get('feeds')}x{m.get('shape', {}).get('subs_per')}"
        table.setdefault(shape, {}).setdefault(m["arm"], []).append(extract(m))
    agg = {}
    for shape, arms in table.items():
        agg[shape] = {}
        for arm, rows in arms.items():
            keys = set(itertools.chain.from_iterable(r.keys() for r in rows))
            agg[shape][arm] = {k: med([r.get(k) for r in rows]) for k in keys
                               if k not in ("verdict",)}
            agg[shape][arm]["runs"] = len(rows)
            agg[shape][arm]["all_pass"] = all(r.get("verdict") == "PASS" for r in rows)
    return agg

def fit3(pts):
    """pts: (feeds, subs, rss, label). Least squares for
    RSS = base + per_sub*subs + per_feed*feeds, WITH the fit set,
    residuals and R^2 published (review: the density sweep alone holds
    subscribers constant at 1,000, which makes base and per-sub
    inseparable — a valid fit needs geometries that vary BOTH axes,
    and the report must show which runs were fitted)."""
    subs_vals = {s for _, s, _, _ in pts}
    feeds_vals = {f for f, _, _, _ in pts}
    def matmul(A, B):
        return [[sum(A[i][k] * B[k][j] for k in range(len(B))) for j in range(len(B[0]))] for i in range(len(A))]
    A = [[1.0, s, f] for f, s, _, _ in pts]
    b = [[r] for _, _, r, _ in pts]
    At = list(map(list, zip(*A)))
    AtA = matmul(At, A)
    Atb = matmul(At, b)
    # 3x3 solve via Cramer.
    def det3(M):
        return (M[0][0]*(M[1][1]*M[2][2]-M[1][2]*M[2][1])
              - M[0][1]*(M[1][0]*M[2][2]-M[1][2]*M[2][0])
              + M[0][2]*(M[1][0]*M[2][1]-M[1][1]*M[2][0]))
    D = det3(AtA)
    if abs(D) < 1e-9:
        return None
    xs = []
    for c in range(3):
        M = [row[:] for row in AtA]
        for r in range(3):
            M[r][c] = Atb[r][0]
        xs.append(det3(M) / D)
    preds = [xs[0] + xs[1] * s + xs[2] * f for f, s, _, _ in pts]
    resid = [r - p for (_, _, r, _), p in zip(pts, preds)]
    mean = sum(r for _, _, r, _ in pts) / len(pts)
    ss_tot = sum((r - mean) ** 2 for _, _, r, _ in pts) or 1e-9
    ss_res = sum(e ** 2 for e in resid)
    return {
        "base_mb": round(xs[0], 1),
        "kb_per_sub": round(xs[1] * 1024, 2),
        "kb_per_feed": round(xs[2] * 1024, 2),
        "r2": round(1 - ss_res / ss_tot, 4),
        "max_resid_mb": round(max(abs(e) for e in resid), 1),
        "identifiable": len(subs_vals) >= 2 and len(feeds_vals) >= 2,
        "points": [{"label": lbl, "feeds": f, "subs": s, "rss_mb": r,
                    "resid_mb": round(e, 1)}
                   for (f, s, r, lbl), e in zip(pts, resid)],
    }

def gates(agg):
    """A-vs-B (>10%) and B-vs-C (~5%) on the medians, per shape."""
    verdicts = []
    def pctdiff(x, y):
        if x in (None, 0) or y is None:
            return None
        return round((y - x) / x * 100, 1)
    for shape, arms in sorted(agg.items()):
        a, b, c = arms.get("a"), arms.get("b"), arms.get("c")
        row = {"shape": shape}
        if a and b:
            row["ab"] = {
                "append_p99_pct": pctdiff(a.get("mixed_ap_p99"), b.get("mixed_ap_p99")),
                "delivery_p99_pct": pctdiff(a.get("mixed_dl_p99"), b.get("mixed_dl_p99")),
                "cpu_per_delivery_pct": pctdiff(a.get("fanout_cpu_us_per_unique_delivery"),
                                                b.get("fanout_cpu_us_per_unique_delivery")),
                "idle_rss_pct": pctdiff(a.get("rss_idle"), b.get("rss_idle")),
            }
            row["ab_flag"] = any(v is not None and v > 10 for k, v in row["ab"].items()
                                 if k != "idle_rss_pct") or (
                row["ab"]["idle_rss_pct"] is not None and row["ab"]["idle_rss_pct"] > 15)
        if b and c:
            row["bc"] = {
                "append_p99_pct": pctdiff(b.get("mixed_ap_p99"), c.get("mixed_ap_p99")),
                "delivery_p99_pct": pctdiff(b.get("mixed_dl_p99"), c.get("mixed_dl_p99")),
                "cpu_per_delivery_pct": pctdiff(b.get("fanout_cpu_us_per_unique_delivery"),
                                                c.get("fanout_cpu_us_per_unique_delivery")),
                "idle_rss_pct": pctdiff(b.get("rss_idle"), c.get("rss_idle")),
            }
            row["bc_flag"] = any(v is not None and abs(v) > 5 for v in row["bc"].values() if v is not None)
        verdicts.append(row)
    return verdicts

def main():
    results = sys.argv[1]
    runs = load(results)
    agg = aggregate(runs)
    # --fit-extra dir[,dir...]: extra results folded into the memory
    # model ONLY (the capacity-ladder geometries vary subscriber count,
    # which the density sweep cannot — without them base and per-sub
    # are inseparable and the fit is not identifiable).
    fit_agg = dict(agg)
    if "--fit-extra" in sys.argv:
        for extra in sys.argv[sys.argv.index("--fit-extra") + 1].split(","):
            for shape, arms in aggregate(load(extra)).items():
                dst = fit_agg.setdefault(shape, {})
                for arm, row in arms.items():
                    dst.setdefault(arm, row)
    models = {}
    for arm in ("a", "b", "c"):
        pts = []
        for shape, arms in fit_agg.items():
            if arm in arms:
                f, s = shape.split("x")
                rss = arms[arm].get("rss_idle")
                if rss is not None:
                    pts.append((int(f), int(f) * int(s), rss, f"{shape}:{arm}"))
        models[arm] = fit3(pts) if len(pts) >= 3 else None
    g = gates(agg)
    report = {"runs": len(runs), "aggregate": agg, "memory_model": models, "gates": g}
    if "--json" in sys.argv:
        out = sys.argv[sys.argv.index("--json") + 1]
        json.dump(report, open(out, "w"), indent=1)
    # Human tables.
    for shape, arms in sorted(agg.items()):
        print(f"\n== {shape}")
        hdr = ["arm", "runs", "pass", "rss_idle", "peak_rss", "kb/sub", "fan_cpu/del",
               "idle_cpu%", "mix_ap_p99", "mix_dl_p99", "fan_del/s", "recon_miss"]
        print("  " + " | ".join(f"{h:>10}" for h in hdr))
        for arm in ("a", "b", "c"):
            if arm not in arms:
                continue
            r = arms[arm]
            row = [arm, r.get("runs"), "Y" if r.get("all_pass") else "N", r.get("rss_idle"),
                   r.get("peak_rss_mb"), r.get("kb_per_sub_raw"),
                   r.get("fanout_cpu_us_per_unique_delivery"), r.get("idle_cpu_pct"),
                   r.get("mixed_ap_p99"), r.get("mixed_dl_p99"), r.get("fanout_del_s"),
                   r.get("recon_missing")]
            print("  " + " | ".join(f"{str(v):>10}" for v in row))
    print("\n== memory model (RSS ~= base + per_sub*S + per_feed*F, idle-parked)")
    for arm, mdl in models.items():
        if not mdl:
            print(f"  arm {arm}: insufficient points")
            continue
        print(f"  arm {arm}: base {mdl['base_mb']}MB + {mdl['kb_per_sub']}KB/sub + "
              f"{mdl['kb_per_feed']}KB/feed  R2={mdl['r2']} max_resid={mdl['max_resid_mb']}MB "
              f"identifiable={mdl['identifiable']}")
        for p in mdl["points"]:
            print(f"    {p['label']:>12} feeds={p['feeds']:>5} subs={p['subs']:>6} "
                  f"rss={p['rss_mb']:>7} resid={p['resid_mb']}")
    print("\n== gates")
    for row in g:
        print(f"  {row['shape']}: A->B {row.get('ab')} flag={row.get('ab_flag')} | B->C {row.get('bc')} flag={row.get('bc_flag')}")

if __name__ == "__main__":
    main()
