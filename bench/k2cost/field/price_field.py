#!/usr/bin/env python3
"""Price one K2 field cell from what observe.py recorded (design §1, §4.1, §8).

    python3 bench/k2cost/field/price_field.py <run-id> <cell> [--mpu-requests 3]
        [--req-overhead-bytes 700] [--k2cost DIR]

Inputs (never the repo): results/<run>/<cell>/observe.jsonl and
observe-summary.json, gen-g<k>.json and gen-g<k>-windows.jsonl,
runs/<run>/resources.json (deploy and teardown times, KEEP_AWAKE).
Writes results/<run>/<cell>/price-field.json and prints a short table.

- Requests: each server's cumulative /v1/debug/store `totals` from the
  scrape lines, stitched across boots by the local harness's
  `stitched_delta` (bench/k2cost/price.py: a since_ms change or a decrease
  opens a new boot, counted from zero), per server and summed. Loss bound
  per reset (§8): the scrape interval before it x the peak request rate;
  the window is void when the bounds exceed 2% of its requests.
- Tigris classes (§1): put, copy and list Class A; a logical multipart
  upload is --mpu-requests Class A requests (create, parts, complete; L0
  calibrates it); get and head Class B; delete free. Get/head `unbilled`
  held the billed 404s with the free 304s before fcd16d84, whose `not_found`
  counts them exactly (used when present); for an older binary the scrape's ?window ring
  counts NotFound + Failed as `err` and a 304 as success, so the window
  err fraction minus `totals.err` estimates the 404s (`billed_404_est`).
  `err` (Failed: statuses left after retries, transport errors) is billed
  in the headline and free in `low`; without ring data `low` bills ok only
  and `high` every call.
- Egress (D1, $0.005 per 1e9 B): `totals.bytes_put` counts PUT payloads
  only; every request also sends a request line and SigV4 headers, so
  egress to Tigris is a range: bytes_put .. bytes_put + requests x
  --req-overhead-bytes (700 B uncalibrated: calibrate once with s3lite or a
  capture). Client legs from the generator windows: wire_bytes_down once
  (T-single) or twice, plus wire_bytes_up behind a router.
- Compute (D1): memory GiB x hours running x $0.006: a KEEP_AWAKE instance
  runs from its deploy start (ledger) to its teardown (or its expiry, when
  its guard released it); any other from the heartbeat awake integral.
  CPU $0.064 per vCPU-hour: the wrapper's cumulative CPU per boot when
  logged (every role; routers have nothing else), else the heartbeat
  integral (servers), with the first-beat-after-wake extra as a range.
- Per generator invocation (the F3 tiers): the scrapes bracketing its
  startMs..endMs give its request delta and the §8 gate deltas
  (rate_limit_refusals, admit_shed*, stream_shed, wedge_shed, a
  maintenance_backpressure engagement, boot_id changes), with the edge
  slop in seconds; the generator windows give its acked load and non-2xx.
"""
from __future__ import annotations

import argparse
import calendar
import json
import os
import sys
import time
from collections import defaultdict

import fieldlib as F

CLASS_USD = {"A": 5e-6, "B": 5e-7}
OP_CLASS = {"put": "A", "copy": "A", "list": "A", "mpu": "A", "get": "B", "head": "B", "delete": "free"}
MEM_USD_GIB_H, CPU_USD_VCPU_H, EGRESS_USD_PER_GB = 0.006, 0.064, 0.005
GATES = ("admit_shed", "admit_shed_inflight", "admit_shed_rss", "admit_shed_survival", "stream_shed", "wedge_shed")


def stitcher(k2cost: str | None):
    """The local harness's stitched_delta (bench/k2cost/price.py)."""
    here = os.path.abspath(os.path.join(F.HERE, ".."))
    sibling = os.path.abspath(os.path.join(F.ROOT, "..", "wt-k2", "bench", "k2cost"))
    for d in [k2cost] if k2cost else [here, sibling]:
        if d and os.path.exists(os.path.join(d, "price.py")):
            sys.path.insert(0, d)
            from price import stitched_delta  # noqa: PLC0415 - the harness lives beside this tool
            return stitched_delta, d
    F.die("bench/k2cost/price.py not found (pass --k2cost DIR)")
    return None, None


def ms(utc: str | None) -> int | None:
    try:
        return calendar.timegm(time.strptime(utc, "%Y-%m-%dT%H:%M:%SZ")) * 1000 if utc else None
    except ValueError:
        return None


def jsonl(path: str) -> list:
    out = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    out.append(json.loads(line))
                except ValueError:
                    pass
    return out


# ---------------------------------------------------------------- requests

def series(obs: list) -> dict:
    """Per server: [(t, totals, window, load)] in time order."""
    per = defaultdict(list)
    for r in obs:
        tot = ((r.get("store") or {}).get("totals")) if r.get("kind") == "scrape" else None
        if tot:
            per[r["instance"]].append((r.get("store_t") or r["t"], tot, (r.get("store") or {}).get("window"),
                                       r.get("load") or {}, r.get("load_t") or r["t"]))
    return {k: sorted(v, key=lambda p: p[0]) for k, v in per.items()}


def calls(cells: dict) -> int:
    return sum(int(n) for v in cells.values() for n in v.values())


def price_cells(cells: dict, frac: dict | None, mpu: int) -> dict:
    """Class A/B counts and dollars: headline, low and high."""
    out = {"A": 0.0, "B": 0.0, "A_low": 0.0, "B_low": 0.0, "A_high": 0.0, "B_high": 0.0, "billed_404_est": 0, "free": 0}
    for k, v in cells.items():
        op = k.split(":", 1)[0]
        cls = OP_CLASS.get(op, "free")
        ok, unb, err = int(v.get("ok", 0)), int(v.get("unbilled", 0)), int(v.get("err", 0))
        nf = int(v.get("not_found", 0))  # a binary from fcd16d84 on counts the billed 404s
        mult = mpu if op == "mpu" else 1
        if cls == "free":
            out["free"] += ok + nf + unb + err
            continue
        refused = 0
        if "not_found" in v:
            out[cls] += mult * (ok + nf + err)
            out[f"{cls}_low"] += mult * (ok + nf)
            out[f"{cls}_high"] += mult * (ok + nf + unb + err)
            out["billed_404_exact"] = out.get("billed_404_exact", 0) + nf
            out["free"] += unb
            continue
        if cls == "B" and frac is not None and k in frac:
            refused = max(0, min(unb, round(frac[k] * (ok + unb + err)) - err))
        elif cls == "B" and frac is None:
            refused = None
        head = ok + err + (refused or 0)
        out[cls] += mult * head
        out[f"{cls}_low"] += mult * (ok + (refused or 0))
        out[f"{cls}_high"] += mult * (ok + unb + err)
        out["billed_404_est"] += refused or 0
        out["free"] += unb - (refused or 0)
    for sfx in ("", "_low", "_high"):
        out[f"usd{sfx}"] = out[f"A{sfx}"] * CLASS_USD["A"] + out[f"B{sfx}"] * CLASS_USD["B"]
    out["ring_split"] = frac is not None
    return out


def requests(ser: dict, stitched_delta, t0: int, t1: int, mpu: int) -> dict:
    """Stitched requests of every server over [t0, t1], priced, with resets
    and the §8 loss bound."""
    total, bp, bg, resets, bound = defaultdict(lambda: defaultdict(int)), 0, 0, 0, 0.0
    frac_n, frac_err, have_ring = defaultdict(int), defaultdict(int), False
    for name, pts in ser.items():
        pts = [p for p in pts if t0 <= p[0] <= t1]
        if len(pts) < 2:
            continue
        cells, b_put, b_got, r = stitched_delta([(p[0], p[1]) for p in pts])
        for k, v in cells.items():
            for f, n in v.items():
                total[k][f] += n
        bp, bg, resets = bp + b_put, bg + b_got, resets + r
        peak = 0.0
        for a, b in zip(pts, pts[1:]):
            same = a[1].get("since_ms") == b[1].get("since_ms")
            d = calls((b[1].get("ops") or {})) - (calls(a[1].get("ops") or {}) if same else 0)
            peak = max(peak, d / max(1e-3, (b[0] - a[0]) / 1000))
        for a, b in zip(pts, pts[1:]):
            if a[1].get("since_ms") != b[1].get("since_ms"):
                bound += (b[0] - a[0]) / 1000 * peak
        for p in pts[1:]:
            for k, v in ((p[2] or {}).get("ops") or {}).items():
                have_ring = True
                frac_n[k] += int(v.get("n") or 0)
                frac_err[k] += int(v.get("err") or 0)
    frac = {k: frac_err[k] / frac_n[k] for k in frac_n if frac_n[k]} if have_ring else None
    n = calls(total)
    return {"cells": {k: dict(v) for k, v in total.items()}, "requests": n, "bytes_put": bp, "bytes_got": bg,
            "resets": resets, "loss_bound_requests": round(bound), "void_loss": bool(n) and bound > 0.02 * n,
            "priced": price_cells(total, frac, mpu)}


def gates(ser: dict, t0: int, t1: int) -> dict:
    """§8 validity deltas over [t0, t1] from the bracketing load docs."""
    out = {"rate_limit_refusals": 0, "sheds": 0, "backpressure_engaged": False, "boot_changes": 0,
           "absorb_lag_max_secs": 0, "rss_mb_max": 0.0}
    for pts in ser.values():
        pts = [p for p in pts if t0 <= p[0] <= t1 and p[3]]
        for a, b in zip(pts, pts[1:]):
            la, lb = a[3], b[3]
            if la.get("boot_id") != lb.get("boot_id"):
                out["boot_changes"] += 1
                continue
            out["rate_limit_refusals"] += sum(int(v or 0) for v in (lb.get("rate_limit_refusals") or {}).values()) - \
                sum(int(v or 0) for v in (la.get("rate_limit_refusals") or {}).values())
            out["sheds"] += sum(int(lb.get(g) or 0) - int(la.get(g) or 0) for g in GATES)
        for p in pts:
            out["backpressure_engaged"] |= bool((p[3].get("maintenance_backpressure") or {}).get("engaged"))
            out["absorb_lag_max_secs"] = max(out["absorb_lag_max_secs"], int(p[3].get("absorb_lag_max_secs") or 0))
            out["rss_mb_max"] = max(out["rss_mb_max"], float(p[3].get("rss_mb") or 0))
    out["valid"] = not (out["rate_limit_refusals"] or out["sheds"] or out["backpressure_engaged"]
                        or out["boot_changes"] or out["absorb_lag_max_secs"] > 90 or out["rss_mb_max"] > 450)
    return out


def bracket(ser: dict, t0: int, t1: int) -> tuple:
    """The widest scrape times within one interval of [t0, t1]: the last
    scrape at or before t0 and the first at or after t1 (any server)."""
    ts = sorted({p[0] for pts in ser.values() for p in pts})
    lo = max((t for t in ts if t <= t0), default=None)
    hi = min((t for t in ts if t >= t1), default=None)
    return lo, hi


# ---------------------------------------------------------------- compute, egress

def compute(run: str, cell: str, summary: dict, now_ms: int) -> dict:
    svcs = (F.load_resources(run)["cells"].get(cell) or {}).get("services") or {}
    out, tot = {}, {"usd_memory": 0.0, "usd_cpu": 0.0, "usd_cpu_high": 0.0}
    for rec in svcs.values():
        name = rec.get("instance") or rec.get("name")
        s = (summary.get("instances") or {}).get(name) or {}
        gib = rec.get("memory_gib") or s.get("memory_gib") or 1.0
        start = (ms(rec.get("deployed")) or 0) - int(float(rec.get("deploy_secs") or 0) * 1000)
        if rec.get("keep_awake"):
            end = min(ms(rec.get("torn_down")) or now_ms, ms(rec.get("keep_awake_expires")) or now_ms)
            hours, basis = max(0, end - start) / 3.6e6, "KEEP_AWAKE: deploy start to teardown/expiry"
        else:
            hours, basis = float(s.get("awake_s") or 0) / 3600, "heartbeat awake integral"
        boots = (summary.get("wrapper_cpu") or {}).get(name) or {}
        w_cpu = sum(float(b.get("proc_cpu_s") or 0) for b in boots.values()) if boots else None
        hb_cpu = s.get("cpu_s")
        cpu = w_cpu if w_cpu is not None else float(hb_cpu or 0)
        cpu_hi = cpu + (0 if w_cpu is not None else float(s.get("cpu_wake_extra_s") or 0))
        r = {"role": rec.get("role"), "memory_gib": gib, "hours": round(hours, 4), "memory_basis": basis,
             "cpu_s": round(cpu, 2), "cpu_basis": "wrapper log" if w_cpu is not None else ("heartbeat" if hb_cpu is not None else "none"),
             "cpu_s_heartbeat": hb_cpu, "cpu_s_wrapper": None if w_cpu is None else round(w_cpu, 2),
             "wrapper_boots": len(boots), "heartbeat_boots": s.get("boots"),
             "usd_memory": round(hours * gib * MEM_USD_GIB_H, 6), "usd_cpu": round(cpu / 3600 * CPU_USD_VCPU_H, 6),
             "usd_cpu_high": round(cpu_hi / 3600 * CPU_USD_VCPU_H, 6)}
        out[name] = r
        for k in tot:
            tot[k] += r[k]
    return {"instances": out, **{k: round(v, 6) for k, v in tot.items()}}


def client_egress(res_dir: str, routed: bool) -> dict:
    up = down = 0
    for f in sorted(os.listdir(res_dir)):
        if f.startswith("gen-") and f.endswith("-windows.jsonl"):
            for w in jsonl(os.path.join(res_dir, f)):
                up += int(w.get("wire_bytes_up") or 0)
                down += int(w.get("wire_bytes_down") or 0)
    b = down * (2 if routed else 1) + (up if routed else 0)
    return {"wire_bytes_up": up, "wire_bytes_down": down, "routed": routed, "bytes": b,
            "usd": b / 1e9 * EGRESS_USD_PER_GB}


def invocations(res_dir: str, obs: list, ser: dict, stitched_delta, mpu: int) -> list:
    last = {}
    for r in obs:
        if r.get("kind") == "gen" and r.get("invs"):
            last[r["gen"]] = r
    out = []
    for g, rec in sorted(last.items()):
        meta = F.read_json(os.path.join(res_dir, f"gen-{g}.json")) or {}
        targets = {a.get("inv"): a.get("target") for a in meta.get("targets") or []}
        wins = defaultdict(list)
        for w in jsonl(os.path.join(res_dir, f"gen-{g}-windows.jsonl")):
            wins[w.get("inv")].append(w)
        for inv in rec["invs"]:
            t0, t1 = inv.get("startMs"), inv.get("endMs")
            if not t0 or not t1:
                continue
            lo, hi = bracket(ser, t0, t1)
            row = {"gen": g, "inv": inv["inv"], "stage": inv.get("stage"), "rc": inv.get("rc"),
                   "target": targets.get(inv["inv"]), "start": F.utc(t0), "secs": round((t1 - t0) / 1000, 1),
                   "acked_requests": sum(int(w.get("acked_requests") or 0) for w in wins[inv["inv"]]),
                   "payload_bytes": sum(int(w.get("payload_bytes") or 0) for w in wins[inv["inv"]]),
                   "non_2xx": sum(int(n) for w in wins[inv["inv"]] for c, n in (w.get("status_counts") or {}).items()
                                  if not str(c).startswith("2"))}
            if lo is not None and hi is not None:
                req = requests(ser, stitched_delta, lo, hi, mpu)
                wal = sum(int(v.get("ok", 0)) for k, v in req["cells"].items() if k == "put:wal")
                row.update({"edge_slop_s": round(((t0 - lo) + (hi - t1)) / 1000, 1), "requests": req["requests"],
                            "wal_puts": wal, "usd_requests": round(req["priced"]["usd"], 6),
                            "gates": gates(ser, lo, hi)})
            out.append(row)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("run")
    ap.add_argument("cell")
    ap.add_argument("--mpu-requests", type=int, default=3)
    ap.add_argument("--req-overhead-bytes", type=int, default=700)
    ap.add_argument("--k2cost", default=None)
    a = ap.parse_args()
    F.check_names(a.run, a.cell)
    stitched_delta, src = stitcher(a.k2cost)
    res_dir = F.results_dir(a.run, a.cell)
    obs = jsonl(os.path.join(res_dir, "observe.jsonl"))
    summary = F.read_json(os.path.join(res_dir, "observe-summary.json")) or {}
    ser = series(obs)
    now = F.now_ms()
    t_all = (min((p[0] for v in ser.values() for p in v), default=0), max((p[0] for v in ser.values() for p in v), default=0))
    req = requests(ser, stitched_delta, *t_all, a.mpu_requests) if ser else None
    cfg = F.read_cell_file(a.cell)
    routed = F.cell_int(cfg, "ROUTERS", 0) > 0
    eg_lo = (req or {}).get("bytes_put", 0)
    eg_hi = eg_lo + ((req or {}).get("requests", 0) * a.req_overhead_bytes)
    doc = {
        "run": a.run, "cell": a.cell, "utc": F.utc(), "stitcher": src, "void": summary.get("void"),
        "scrape_window": [F.utc(t_all[0]), F.utc(t_all[1])] if ser else None,
        "requests": req,
        "egress_store": None if req is None else {
            "bytes_low": eg_lo, "bytes_high": eg_hi, "req_overhead_bytes": a.req_overhead_bytes,
            "usd_low": eg_lo / 1e9 * EGRESS_USD_PER_GB, "usd_high": eg_hi / 1e9 * EGRESS_USD_PER_GB,
            "note": "bytes_put counts PUT payloads only; the high end adds request headers (uncalibrated)"},
        "egress_client": client_egress(res_dir, routed),
        "compute": compute(a.run, a.cell, summary, now),
        "invocations": invocations(res_dir, obs, ser, stitched_delta, a.mpu_requests),
        "harness": summary.get("harness_requests"),
    }
    if req is None:
        doc["requests_note"] = "no scrapes: this cell was never scraped (SCRAPE=0); request cost needs the cadence model"
    F.write_json(os.path.join(res_dir, "price-field.json"), doc)
    p = (req or {}).get("priced") or {}
    print(f"{a.run}/{a.cell}: {'VOID ' + str(doc['void']) if doc['void'] else 'valid identity'}")
    if req:
        print(f"  requests {req['requests']} over {doc['scrape_window']}, resets {req['resets']}, "
              f"loss bound {req['loss_bound_requests']}{' VOID' if req['void_loss'] else ''}")
        print(f"  Class A {p['A']:.0f} (low {p['A_low']:.0f}, high {p['A_high']:.0f}); Class B {p['B']:.0f} "
              f"(low {p['B_low']:.0f}, high {p['B_high']:.0f}; billed 404 est {p['billed_404_est']}, ring "
              f"{'yes' if p['ring_split'] else 'no'}); ${p['usd']:.6f} (${p['usd_low']:.6f}..${p['usd_high']:.6f})")
        print(f"  egress to Tigris {eg_lo}..{eg_hi} B (${doc['egress_store']['usd_low']:.6f}..${doc['egress_store']['usd_high']:.6f})")
    c = doc["compute"]
    print(f"  compute: memory ${c['usd_memory']:.6f}, CPU ${c['usd_cpu']:.6f} (high ${c['usd_cpu_high']:.6f})")
    for name, r in sorted(c["instances"].items()):
        print(f"    {name:<10} {r['hours']:.3f} h x {r['memory_gib']} GiB ({r['memory_basis']}); CPU {r['cpu_s']} s ({r['cpu_basis']})")
    print(f"  client egress {doc['egress_client']['bytes']} B; invocations {len(doc['invocations'])}")
    for r in doc["invocations"]:
        print(f"    {r['gen']}#{r['inv']} {r['secs']}s -> {r.get('target')}: acked {r['acked_requests']}, non-2xx "
              f"{r['non_2xx']}, requests {r.get('requests')}, WAL {r.get('wal_puts')}, slop {r.get('edge_slop_s')} s, "
              f"gates valid {(r.get('gates') or {}).get('valid')}")


if __name__ == "__main__":
    main()
