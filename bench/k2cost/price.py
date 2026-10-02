#!/usr/bin/env python3
"""K2 cost experiment: price one local point (target/k2-design.md §1, §4, §8).

  python3 bench/k2cost/price.py [RUN_DIR] [--baseline BASELINE_RUN_DIR]
      RUN_DIR defaults to the newest $K2_HOME/results/*. --baseline borrows
      the per-cell idle floor rates (and the idle CPU and PUT-byte rates)
      that another point measured in the same posture with the same DBs
      open (its summary.json `floor_baseline`, e.g. an L6 idle run);
      run-local.sh passes $K2_BASELINE. Without it the point's own
      post-quiescence idle tail, or its idle phases, are the baseline.

Inputs (written by run-local.sh and scrape.py): s3lite-{start,loadend,
quiesced,end}.json (the exact physical ledger by tier/kind/op/status),
store-*.json and scrape.jsonl (the servers' logical `totals`, gauges,
s3lite every 10 s), marks.jsonl (phase windows), boundaries.jsonl,
phase-<i>.ledger.json (k2gen), usage.json (the product meters), posture.json.

Pricing (Tigris Standard, free tier ignored):
  Class A $5e-6: PutObject, CopyObject, CreateMultipartUpload, ListObjects(V2),
    ListParts, ListMultipartUploads; and, not named by Tigris, UploadPart,
    CompleteMultipartUpload and POST ?delete (DeleteObjects), priced Class A
    in the headline and free in the `alt` column.
  Class B $5e-7: GetObject, HeadObject.
  Free: every DELETE and abort; answers 301, 307, 400, 403, 405, 409, 411,
    412, 416, 304, 500, 501. Billed at the op's class: 2xx, 404, 429, 502,
    503, 504.
  Storage $0.02 per 2^30 B-month on the average of daily peaks (live_bytes).
Allocation (§4.3): k2cells.py books every cell to the floor (by kind, or at
the baseline rate), produce, consume or lifecycle by its driver. The floor
is its own line, spread over the dimensions pro rata by revenue.
K2 revenue from the server's meters: $0.04 per GiB produced and consumed,
$0.02 per GiB-month retained (per 2^30 B, conservative), decimal-GB beside it.

Writes RUN_DIR/summary.json and prints a compact table.
"""

from __future__ import annotations

import glob
import json
import os
import sys
from collections import defaultdict

from k2cells import (BUCKETS, CLASS_USD, CONSUME_MODES, DEC, GIB, K2, MONTH_SECS, allocate,
                     baseline_rates, cell_delta, jsonl, load, price_cells, sum_buckets)
from k2store import storage
from k2valid import validity

MEM_GIB = 1.0  # deploy/profiles/compute-1g.env: every instance provisions 1 GiB
D1_MEM_USD_GIB_H = 0.006
D1_CPU_USD_VCPU_H = 0.064
EGRESS_USD_PER_GB = 0.005  # per 1e9 B leaving Compute (D1, 2026-10-02)
EGRESS_SENSITIVITY = (0.0, 0.02)
BASELINE_MIN_SECS = 30
GEN_KEYS = ("acked_requests", "acked_records", "payload_bytes", "wire_bytes_up", "wire_bytes_down",
            "delivered_records", "delivered_payload_bytes", "sent_requests", "pulls", "empty_polls", "settles")


class Series:
    """s3lite samples from scrape.jsonl plus the exact snapshots."""

    def __init__(self, scrape: list, snaps: dict, boundaries: list):
        self.pts = [(r["t"], r["body"]) for r in scrape if r.get("src") == "s3lite" and r.get("body")]
        self.pts += [(s["t_ms"], s) for s in snaps.values() if s]
        self.pts += [(b["t_ms"], b["s3lite"]) for b in boundaries if b.get("s3lite")]
        self.pts.sort(key=lambda p: p[0])

    def at(self, t: int) -> dict:
        if not self.pts:
            return {}
        return min(self.pts, key=lambda p: abs(p[0] - t))[1]

    def gap_ms(self, t: int) -> int:
        return min((abs(p[0] - t) for p in self.pts), default=0)


def dimension(modes: list) -> str:
    ms = set(modes)
    for name in ("produce", "setup", "idle", "restart", "quiesce"):
        if ms == {name}:
            return name
    if ms <= CONSUME_MODES:
        return "consume"
    if ms == {"churn"}:
        return "lifecycle"
    if ms == {"corpus-stats"}:
        return "none"
    return "mixed"


def phase_groups(marks: list) -> list:
    """Groups of phases: background phases join the next foreground line."""
    ends = {m["i"]: m for m in marks if m.get("mark") == "phase_end" and "i" in m}
    groups, cur = [], []
    for s in (m for m in marks if m.get("mark") == "phase_start"):
        cur.append(s)
        if not s.get("bg"):
            groups.append(cur)
            cur = []
    if cur:
        groups.append(cur)
    out = []
    for g in groups:
        modes = sorted({p["mode"] for p in g})
        t0 = min(p["t"] for p in g)
        t1 = max(ends.get(p["i"], {}).get("t", p["t"]) for p in g)
        out.append({"phases": [p["i"] for p in g], "modes": modes, "t0": t0, "t1": t1, "secs": (t1 - t0) / 1000,
                    "dimension": dimension(modes), "args": [p.get("args") for p in g],
                    "rcs": [ends.get(p["i"], {}).get("rc", 0) for p in g]})
    return out


# ---- server totals (logical calls), stitched across boots -------------------------

def totals_series(scrape: list, store_snaps: dict, boundaries: list, t0: int, t1: int) -> dict:
    """Per server: [(t, totals)] over [t0, t1], snapshots and phase boundaries included."""
    per = defaultdict(list)
    for snap in list(store_snaps.values()) + boundaries:
        for name, s in ((snap or {}).get("servers") or {}).items():
            tot = ((s or {}).get("store") or {}).get("totals")
            if tot:
                per[name].append((snap["t_ms"], tot))
    for rec in scrape:
        if rec.get("src") == "store" and rec.get("body") and t0 <= rec["t"] <= t1 and rec["body"].get("totals"):
            per[rec["server"]].append((rec["t"], rec["body"]["totals"]))
    return {n: sorted((p for p in v if t0 <= p[0] <= t1), key=lambda p: p[0]) for n, v in per.items()}


# The server's billed outcomes per (op, class) cell (GET /v1/debug/store
# `totals`, edge #94): a 404 is `not_found` (billed) since fcd16d84; an
# older binary reports it inside `unbilled`.
OUTCOMES = ("ok", "not_found", "unbilled", "err")


def stitched_delta(points: list) -> tuple:
    """Sum of per-interval deltas; a since_ms change or a decrease is a new boot
    (counted from zero). Returns (cells, bytes_put, bytes_got, resets)."""
    cells = defaultdict(lambda: defaultdict(int))
    bp = bg = resets = 0
    for (_, a), (_, b) in zip(points, points[1:]):
        reset = a.get("since_ms") != b.get("since_ms")
        ops_a, ops_b = a.get("ops") or {}, b.get("ops") or {}
        if not reset:
            reset = any(int(cb.get(f, 0)) < int(ops_a.get(k, {}).get(f, 0))
                        for k, cb in ops_b.items() for f in OUTCOMES)
        if reset:
            resets += 1
            ops_a = {}
        for k, cb in ops_b.items():
            for f in OUTCOMES:
                cells[k][f] += int(cb.get(f, 0)) - int(ops_a.get(k, {}).get(f, 0))
        bp += int(b.get("bytes_put", 0)) - (0 if reset else int(a.get("bytes_put", 0)))
        bg += int(b.get("bytes_got", 0)) - (0 if reset else int(a.get("bytes_got", 0)))
    return cells, bp, bg, resets


def logical(scrape, stores, boundaries, t0, t1):
    cells, lbp, lbg, resets = defaultdict(lambda: defaultdict(int)), 0, 0, 0
    for pts in totals_series(scrape, stores, boundaries, t0, t1).values():
        c, bp, bg, r = stitched_delta(pts)
        resets, lbp, lbg = resets + r, lbp + bp, lbg + bg
        for k, v in c.items():
            for f, n in v.items():
                cells[k][f] += n
    return cells, lbp, lbg, resets


def mpu_creates(scrape, stores, boundaries, t0, t1):
    """Logical multipart uploads the servers started in [t0, t1] (each is one
    CreateMultipartUpload); None when the servers report no totals."""
    cells, _, _, _ = logical(scrape, stores, boundaries, t0, t1)
    if not cells:
        return None
    return sum(n for k, v in cells.items() if k.startswith("mpu:") for n in v.values())


LOGICAL_OP = {"put": "put", "copy": "copy", "multipart": "mpu", "upload_part": "mpu",
              "mpu_create": "mpu", "mpu_complete": "mpu", "get": "get", "head": "head",
              "delete": "delete", "delete_objects": "delete", "mpu_abort": "delete", "list": "list"}
# A LIST is classified by its prefix and a DeleteObjects has no key, so the
# two sides cannot agree on their class: compare those ops summed.
UNCLASSED_OPS = ("list", "delete")


def calibration(phys_cells: dict, logical_cells: dict, phys_bytes: tuple, log_bytes: tuple) -> dict:
    def lclass(tier, kind):
        return kind if kind in ("wal", "manifest", "sst") else ("fleet" if tier == "fleet" else "other")
    phys = defaultdict(lambda: {"2xx": 0, "404": 0, "other": 0})
    for key, buckets in phys_cells.items():
        tier, kind, op = key.split("/")
        lop = LOGICAL_OP.get(op, op)
        k = f"{lop}:{'all' if lop in UNCLASSED_OPS else lclass(tier, kind)}"
        for s, n in buckets.items():
            phys[k][s if s in ("2xx", "404") else "other"] += n
    lg = defaultdict(lambda: defaultdict(int))
    for k, v in logical_cells.items():
        op, _, cls = k.partition(":")
        for f, n in v.items():
            lg[f"{op}:{'all' if op in UNCLASSED_OPS else cls}"][f] += int(n)
    rows = []
    for k in sorted(set(phys) | set(lg)):
        ln = sum(lg.get(k, {}).values())
        pn = sum(phys[k].values()) if k in phys else 0
        rows.append({"op_class": k, "logical_ok": int(lg.get(k, {}).get("ok", 0)),
                     "logical_not_found": int(lg.get(k, {}).get("not_found", 0)),
                     "logical_unbilled": int(lg.get(k, {}).get("unbilled", 0)),
                     "logical_err": int(lg.get(k, {}).get("err", 0)),
                     "physical_2xx": phys[k]["2xx"] if k in phys else 0,
                     "physical_404": phys[k]["404"] if k in phys else 0,
                     "physical_non2xx": phys[k]["404"] + phys[k]["other"] if k in phys else 0,
                     "physical_per_logical": round(pn / ln, 3) if ln else None})
    return {"rows": rows, "bytes": {"physical_put": phys_bytes[0], "logical_put": log_bytes[0],
                                    "physical_get": phys_bytes[1], "logical_get": log_bytes[1]}}


# ---- CPU and bytes at window bounds ---------------------------------------------------

def proc_cpu(snap) -> dict:
    return {n: (p.get("pid"), float(p["cpu_s"])) for n, p in ((snap or {}).get("procs") or {}).items()
            if isinstance(p, dict) and p.get("cpu_s") is not None}


def cpu_between(a, b) -> float:
    """CPU-seconds of the rig's processes between two snapshots (same pid)."""
    pa, pb = proc_cpu(a), proc_cpu(b)
    return sum(cb - pa[n][1] if n in pa and pa[n][0] == pid else cb for n, (pid, cb) in pb.items())


def put_bytes(snap) -> int:
    s = (snap or {}).get("stats") or ((snap or {}).get("s3lite") or {}).get("stats") or {}
    return int(s.get("put_bytes") or 0)


def proc_series(stores: dict, boundaries: list) -> dict:
    per = defaultdict(list)
    for snap in [v for v in stores.values() if v] + boundaries:
        for name, (pid, cpu) in proc_cpu(snap).items():
            per[name].append((snap["t_ms"], pid, cpu))
    return {k: sorted(v) for k, v in per.items()}


def compute_d1(stores, boundaries, instances, run_secs, baseline) -> dict:
    """Prisma Compute at its published price (D1): provisioned memory for
    the whole run (floor) plus active CPU, at most 1 vCPU per instance. CPU
    comes from ps at every snapshot, stitched per process (a restart counts
    the new process from 0; the old one's shutdown is lost). Local CPU is an
    M-series core, so it is indicative only. The baseline's idle CPU rate x
    run seconds is floor; the rest is split by moved payload bytes (§4.3)."""
    cpu, capped = {}, 0.0
    for name, pts in proc_series(stores, boundaries).items():
        tot = sum(cb - ca if pa == pb else cb for (_, pa, ca), (_, pb, cb) in zip(pts, pts[1:]))
        cpu[name] = round(tot, 2)
        capped += min(tot, run_secs)
    floor_cpu = min(capped, (baseline or {}).get("cpu_s_per_s", 0.0) * run_secs) if baseline else 0.0
    mem = instances * MEM_GIB * run_secs / 3600 * D1_MEM_USD_GIB_H
    to_usd = D1_CPU_USD_VCPU_H / 3600
    return {"instances": instances, "memory_gib_each": MEM_GIB, "memory_usd": mem, "cpu_s_by_proc": cpu,
            "cpu_s_measured": sum(cpu.values()), "cpu_s_capped": capped, "cpu_usd": capped * to_usd,
            "cpu_s_floor": floor_cpu, "cpu_usd_floor": floor_cpu * to_usd,
            "cpu_usd_above_floor": (capped - floor_cpu) * to_usd, "usd": mem + capped * to_usd,
            "floor_usd": mem + floor_cpu * to_usd}


def egress(ledgers: dict, modes: dict, snaps: dict, routed: bool, run_secs: float, baseline) -> dict:
    """Bytes leaving Compute (D1): every byte the servers PUT to the store
    (produce, less the idle floor's PUT rate), client answers (twice behind
    the router: server->router and router->client) and, behind the router,
    the router->server leg of request bodies."""
    store_put = put_bytes(snaps["end"]) - put_bytes(snaps["start"])
    floor_put = min(store_put, baseline.get("put_bytes_per_s", 0.0) * run_secs) if baseline else 0
    legs = 2 if routed else 1
    by = {"produce": float(store_put - floor_put), "consume": 0.0, "floor": float(floor_put)}
    for i, lg in ledgers.items():
        reads = int(lg.get("delivered_payload_bytes") or 0) > 0 and modes.get(i) != "churn"
        writes = int(lg.get("payload_bytes") or 0) > 0 or modes.get(i) in ("setup", "churn")
        by["consume" if reads else "produce"] += legs * int(lg.get("wire_bytes_down") or 0)
        if routed:
            by["produce" if writes else "consume"] += int(lg.get("wire_bytes_up") or 0)
    return {"bytes": by, "store_put_bytes": store_put, "legs": legs,
            "usd": {k: v / 1e9 * EGRESS_USD_PER_GB for k, v in by.items()},
            "usd_total_sensitivity": {str(p): sum(by.values()) / 1e9 * p for p in EGRESS_SENSITIVITY}}


# ---- the retained meter ----------------------------------------------------------------

def retained_meter(usage: dict, t_end: int, project_byte_s: int) -> dict:
    """storageByteSeconds at the run's end snapshot. The project row holds
    the RECORDED integral, which advances only when a segment's gauge is
    accounted (appends, sweeps, closes): after the last append it stands
    still. /v1/streams/{name}/usage/current adds each open segment's gauge
    up to the read; that read came after `end`, so the gauge x (read - end)
    is backed off. The larger of the two is used."""
    sc = (usage or {}).get("streams_current") or {}
    live, n, owned_sum, warnings = 0.0, 0, 0, []
    for name, r in sorted((sc.get("streams") or {}).items()):
        u = r.get("usage") or {}
        if r.get("status") != 200 or not u:
            warnings.append(f"stream usage for {name}: status {r.get('status')} {r.get('error')}")
            continue
        owned = int(u.get("ownedStoredBytesNow") or 0)
        over_s = max(0, int(r.get("t_ms") or t_end) - t_end) / 1000
        live += max(0.0, int(str(u.get("storageByteSeconds") or "0")) - owned * over_s)
        owned_sum += owned
        n += 1
    if sc.get("capped"):
        warnings.append(f"stream usage read for the first of {sc.get('names')} live streams only")
    use_streams = n > 0 and live > project_byte_s
    return {"byte_s_project_recorded": project_byte_s, "byte_s_streams_at_end": int(live),
            "live_streams": n, "owned_frame_bytes_now": owned_sum,
            "basis": "streams_provisional" if use_streams else "project_recorded",
            "byte_s_used": int(live) if use_streams else project_byte_s, "warnings": warnings[:5]}


# ---- the point ---------------------------------------------------------------------------

def group_generator(run: str, g: dict, ledgers: dict) -> dict:
    led, names = defaultdict(int), set()
    for i in g["phases"]:
        lg = load(os.path.join(run, f"phase-{i}.ledger.json"))
        if not lg:
            continue
        ledgers[i] = lg
        for k in GEN_KEYS:
            led[k] += int(lg.get(k) or 0)
        for k in ("late_sends", "unsent", "cap_hits", "ambiguous_timeouts"):
            led[k] += int((lg.get("producer") or {}).get(k) or 0)
        led["non_2xx"] += sum(int(n) for st, n in (lg.get("status_counts") or {}).items() if not str(st).startswith("2"))
        names.update(k.split("#", 1)[0] for k in (lg.get("per_stream") or {}))
    led["streams"] = len(names)
    return dict(led)


def choose_baseline(run, ext, groups, windows, by_label, snaps, stores):
    """Per-cell floor rates: --baseline, else this point's post-quiescence
    tail, else its idle phases taken after the first appends (DBs open),
    else idle phases before any data (flagged)."""
    if ext:
        return {**ext, "source": f"--baseline {ext.get('run')}"}
    wins, cpu, put, secs, src = [], 0.0, 0, 0.0, None
    tail = windows.get("tail")
    if tail and tail["secs"] >= BASELINE_MIN_SECS:
        wins.append((tail["cells_delta"], tail["secs"]))
        cpu += cpu_between(stores.get("quiesced"), stores.get("end"))
        put += put_bytes(snaps["end"]) - put_bytes(snaps["quiesced"])
        secs, src = tail["secs"], "post-quiescence idle tail"
    else:
        first_data = next((g["t0"] for g in groups if g["generator"].get("acked_requests")), None)
        idle = [g for g in groups if g["dimension"] == "idle" and g["secs"] > 0 and "cells_delta" in g]
        after = [g for g in idle if first_data is not None and g["t0"] >= first_data]
        use = after or idle
        for g in use:
            a, b = by_label.get(f"phase-{g['phases'][0]}-start"), by_label.get(f"phase-{g['phases'][-1]}-end")
            wins.append((g["cells_delta"], g["secs"]))
            cpu += cpu_between(a, b)
            put += put_bytes(b) - put_bytes(a)
            secs += g["secs"]
        if use:
            src = "idle phases after the first appends" if after else \
                "idle phases BEFORE any data: history and stream DBs not yet open, floor understated"
    if not wins or secs <= 0:
        return None
    return {"source": src, "run": run, "secs": secs, "rates": baseline_rates(wins),
            "cpu_s_per_s": cpu / secs, "put_bytes_per_s": put / secs}


def price_run(run: str, ext_baseline: dict | None = None) -> dict:
    marks = jsonl(os.path.join(run, "marks.jsonl"))
    scrape = jsonl(os.path.join(run, "scrape.jsonl"))
    labels = ("start", "loadend", "quiesced", "end")
    snaps = {k: load(os.path.join(run, f"s3lite-{k}.json")) for k in labels}
    stores = {k: load(os.path.join(run, f"store-{k}.json")) for k in labels}
    usage = load(os.path.join(run, "usage.json"), {}) or {}
    posture = load(os.path.join(run, "posture.json"), {}) or {}
    boundaries = jsonl(os.path.join(run, "boundaries.jsonl"))
    notes = []
    if not snaps["start"] or not snaps["end"]:
        raise SystemExit(f"price: {run} lacks s3lite-start.json or s3lite-end.json")
    if not snaps["loadend"]:
        snaps["loadend"] = snaps["end"]
    t = {k: (v or {}).get("t_ms") for k, v in snaps.items()}
    c = {k: (v or {}).get("cells") or {} for k, v in snaps.items()}
    q = "quiesced" if snaps["quiesced"] else "end"
    bounds = {"load": ("start", "loadend"), "deferred": ("loadend", q), "total": ("start", "end")}
    if snaps["quiesced"]:
        bounds["tail"] = ("quiesced", "end")
    windows = {}
    for w, (a, b) in bounds.items():
        cr = mpu_creates(scrape, stores, boundaries, t[a], t[b])
        windows[w] = {**price_cells(cell_delta(c[a], c[b]), cr), "secs": (t[b] - t[a]) / 1000,
                      "cells_delta": cell_delta(c[a], c[b]), "creates": cr}
    if not snaps["quiesced"]:
        notes.append("no quiesced snapshot: the deferred window runs to `end` (QUIESCE=0, a timeout, or a run "
                     "from before the rig took one) and has no idle tail to serve as baseline")

    # Phase groups, priced exactly from the boundary snapshots.
    series = Series(scrape, snaps, boundaries)
    groups, ledgers = phase_groups(marks), {}
    by_label = {b["label"]: b for b in boundaries}
    modes = {m["i"]: m.get("mode") for m in marks if m.get("mark") == "phase_start"}
    for g in groups:
        a, b = by_label.get(f"phase-{g['phases'][0]}-start"), by_label.get(f"phase-{g['phases'][-1]}-end")
        if a and b:
            ca, cb, ta, tb = a["s3lite"].get("cells") or {}, b["s3lite"].get("cells") or {}, a["t_ms"], b["t_ms"]
            g["boundary_error_ms"] = 0
        else:
            sa, sb = series.at(g["t0"]), series.at(g["t1"])
            ca, cb, ta, tb = sa.get("cells") or {}, sb.get("cells") or {}, g["t0"], g["t1"]
            g["boundary_error_ms"] = max(series.gap_ms(g["t0"]), series.gap_ms(g["t1"]))
        g["cells_delta"] = cell_delta(ca, cb)
        g["creates"] = mpu_creates(scrape, stores, boundaries, ta, tb)
        g["price"] = price_cells(g["cells_delta"], g["creates"])["totals"]
        g["generator"] = gen = group_generator(run, g, ledgers)
        if g["dimension"] == "consume" and gen.get("payload_bytes"):
            g["dimension"] = "mixed"  # a consumer with its in-process producer
        g["ctx"] = {"dimension": g["dimension"], "has_readers": bool(set(g["modes"]) & CONSUME_MODES),
                    "produce_commits": 0 if g["dimension"] == "lifecycle" else gen.get("acked_requests", 0),
                    "consume_commits": max(0, gen.get("pulls", 0) - gen.get("empty_polls", 0)) + gen.get("settles", 0)}

    base = choose_baseline(run, ext_baseline, groups, windows, by_label, snaps, stores)
    rates = (base or {}).get("rates") or {}
    if base:
        notes.append(f"floor baseline: {base['source']} ({base['secs']:.0f} s, {len(rates)} rate cells)")
    else:
        notes.append("no idle baseline (no --baseline, no post-quiescence tail, no idle phase): only the floor "
                     "by kind is separated; system-stream cadence, timer L0s and GC LISTs stay in the dimensions")

    # §4.3 allocation, cell by cell.
    allocs = []
    for g in groups:
        g["alloc"] = allocate(g["cells_delta"], g["secs"], g["ctx"], rates, g["creates"])
        allocs.append(g["alloc"])
    deferred = windows["deferred"]
    deferred["alloc"] = allocate(deferred["cells_delta"], deferred["secs"], {"dimension": "deferred"}, rates,
                                 deferred["creates"])
    allocs.append(deferred["alloc"])
    if "tail" in windows:
        tw = windows["tail"]
        tw["alloc"] = allocate(tw["cells_delta"], tw["secs"], {"dimension": "idle"}, rates, tw["creates"])
        allocs.append(tw["alloc"])
    dims = sum_buckets(allocs)
    gaps = windows["load"]["totals"]["usd"] - sum(g["price"]["usd"] for g in groups)

    proj = usage.get("project") or {}
    eff = proj.get("effective") or proj
    ingest, read = int(eff.get("ingestPayloadBytes") or 0), int(eff.get("readPayloadBytes") or 0)
    byte_s = int(str(eff.get("storageByteSeconds") or "0"))
    if not proj:
        notes.append("usage.json has no project usage: revenue and per-GiB figures are absent (the point is void)")
    retained = retained_meter(usage, t["end"], byte_s)
    if retained["basis"] == "streams_provisional":
        byte_s = retained["byte_s_used"]
        notes.append(f"retained revenue: per-stream provisional storage at the end snapshot "
                     f"({retained['live_streams']} live streams) instead of the project row's recorded integral, "
                     "which stops advancing after the last append")
    notes.extend(retained["warnings"])
    run_secs = (t["end"] - t["start"]) / 1000
    churned = any(g["dimension"] == "lifecycle" for g in groups)
    all_deleted = churned and retained["live_streams"] == 0
    stor = storage(scrape, snaps, t["start"], t["end"], usage, retained["owned_frame_bytes_now"],
                   all_deleted, ingest, byte_s, posture.get("customer_project"))

    revenue = {"produce": K2["produce"] * ingest / GIB, "consume": K2["consume"] * read / GIB,
               "retained": K2["retained"] * byte_s / GIB / MONTH_SECS}
    revenue["total"] = rtot = sum(revenue.values())
    revenue["total_decimal_gb"] = rtot * DEC

    servers = len(posture.get("servers") or {}) or 1
    instances = servers + (1 if servers == 2 else 0)  # + the pilot router
    d1 = compute_d1(stores, boundaries, instances, run_secs, base)
    if not d1["cpu_s_by_proc"]:
        notes.append("no process CPU samples (snapshots predate --pids): D1 compute has memory only")
    eg = egress(ledgers, modes, snaps, servers == 2, run_secs, base)

    # Per GiB (floor excluded) and the floor spread pro rata by revenue.
    req = {"produce": dims["produce"]["usd"] + dims["lifecycle"]["usd"], "consume": dims["consume"]["usd"]}
    moved = ingest + read
    cpu_split = {"produce": d1["cpu_usd_above_floor"] * (ingest / moved if moved else 0),
                 "consume": d1["cpu_usd_above_floor"] * (read / moved if moved else 0)}
    floor = {"requests": dims["floor_kind"]["usd"] + dims["floor_rate"]["usd"],
             "storage": stor.get("usd_run_floor_tiers", 0.0), "compute": d1["floor_usd"], "egress": eg["usd"]["floor"]}
    floor["total"] = sum(floor.values())
    share = {d: (revenue[d] / rtot if rtot else 0.0) for d in ("produce", "consume", "retained")}
    gib = {"produce": ingest / GIB, "consume": read / GIB}
    per_gib, margins = {}, {"requests": {}, "total": {}, "total_with_floor": {}}
    for d in ("produce", "consume"):
        if not gib[d]:
            continue
        r_, e_, c_ = req[d] / gib[d], eg["usd"][d] / gib[d], cpu_split[d] / gib[d]
        per_gib.update({f"{d}_requests": r_, f"{d}_egress": e_, f"{d}_compute": c_, f"{d}_total": r_ + e_ + c_,
                        f"{d}_floor_share": floor["total"] * share[d] / gib[d]})
        margins["requests"][d] = 1 - r_ / K2[d]
        margins["total"][d] = 1 - (r_ + e_ + c_) / K2[d]
        margins["total_with_floor"][d] = 1 - (r_ + e_ + c_ + per_gib[f"{d}_floor_share"]) / K2[d]
    if read and not any(g["dimension"] in ("consume", "mixed") for g in groups):
        for k in [k for k in per_gib if k.startswith("consume")]:
            per_gib.pop(k)
        for m in margins.values():
            m.pop("consume", None)
        notes.append("every read happened inside lifecycle (churn) phases, priced with produce: no consume figure")
    if stor.get("retained_usd_per_gib_month") is not None:
        per_gib["retained_usd_per_gib_month"] = rp = stor["retained_usd_per_gib_month"]
        for m in ("requests", "total"):
            margins[m]["retained"] = 1 - rp / K2["retained"]
    cost_all = (windows["total"]["totals"]["usd"] + stor.get("usd_run", 0.0) + d1["usd"]
                + sum(eg["usd"].values()))
    blended = None
    if rtot:
        blended = {"cost_usd": cost_all, "revenue_usd": rtot, "margin": 1 - cost_all / rtot,
                   "margin_decimal_gb": 1 - cost_all / (rtot * DEC), "margin_k2_cut_25": 1 - cost_all / (rtot * 0.75),
                   "margin_ex_floor": 1 - (cost_all - floor["total"]) / rtot}
        for p, v in eg["usd_total_sensitivity"].items():
            blended[f"margin_egress_{p}"] = 1 - (cost_all - sum(eg["usd"].values()) + v) / rtot
    margins["floor_basis"] = ("floor by kind + per-cell idle rate" if rates else
                              "floor by kind only: margins include the rate floor (no idle baseline)")

    # The WAL function W(r) per producing group (L1, L2): shard WAL PUTs above
    # the baseline rate, per active shard, against acked requests per shard.
    shards = int(((list((posture.get("servers") or {}).values()) or [{}])[0].get("env") or {}).get("INITIAL_SHARDS") or 1)
    wal_fn = []
    for g in groups:
        gen, al = g["generator"], g["alloc"]
        if g["dimension"] != "produce" or not g["secs"]:
            continue
        active = max(1, min(shards, gen.get("streams") or shards))
        above = al["wal_puts"] - al["wal_floor"]
        wal_fn.append({"phases": g["phases"], "secs": round(g["secs"], 1), "acked_req_per_s": gen.get("acked_requests", 0) / g["secs"],
                       "active_shards": active, "r_per_shard": gen.get("acked_requests", 0) / g["secs"] / active,
                       "wal_puts": al["wal_puts"], "wal_puts_floor": al["wal_floor"],
                       "W_per_shard": above / g["secs"] / active, "W_usd_per_s": above * CLASS_USD["A"] / g["secs"],
                       "floor_class_a": al["floor_kind"]["class_a"] + al["floor_rate"]["class_a"],
                       "floor_usd": al["floor_kind"]["usd"] + al["floor_rate"]["usd"]})

    # Calibration: logical totals (all servers, stitched) vs the physical ledger.
    lcells, lbp, lbg, resets = logical(scrape, stores, boundaries, t["start"], t["end"])
    s1, s2 = (snaps["start"].get("stats") or {}), (snaps["end"].get("stats") or {})
    pbytes = (int(s2.get("put_bytes", 0)) - int(s1.get("put_bytes", 0)), int(s2.get("get_bytes", 0)) - int(s1.get("get_bytes", 0)))
    calib = None
    if lcells:
        calib = calibration(windows["total"]["cells_delta"], lcells, pbytes, (lbp, lbg))
        calib["boot_resets_stitched"] = resets
        if resets:
            notes.append(f"calibration spans {resets} process restart(s): what a server did between its last "
                         "read and its exit (the shutdown flush) is lost from the logical side")
    else:
        notes.append("server /v1/debug/store has no `totals` member (binary predates f23ffc9d): no calibration "
                     "table, and the alt column prices every multipart request free")

    restarts = any(g["dimension"] == "restart" for g in groups)
    quiesce_point = snaps["quiesced"] is not None or any(
        m.get("mark") == "quiesce_request" or (m.get("mark") == "load_end" and m.get("quiesce") == 1) for m in marks)
    valid = validity(scrape, {"run": (t["start"], t["end"]), "load": (t["start"], t["loadend"])}, restarts, usage,
                     eff, ledgers, modes, sorted((posture.get("servers") or {}).keys()), posture, marks, quiesce_point)
    valid["checks"].append({"check": "every phase exited 0", "ok": all(rc in (0, None) for g in groups for rc in g["rcs"]),
                            "detail": {",".join(map(str, g["phases"])): g["rcs"] for g in groups}})
    valid["valid"] = all(x["ok"] for x in valid["checks"])
    answers, codes = defaultdict(int), defaultdict(int)
    for lg in ledgers.values():
        for st, n in (lg.get("status_counts") or {}).items():
            if not str(st).startswith("2"):
                answers[str(st)] += int(n)
        for k, n in (lg.get("error_codes") or {}).items():
            codes[k] += int(n)
    if answers:
        notes.append(f"generator saw non-2xx answers {dict(answers)}, by code {dict(codes)}; k2gen retried "
                     f"{sum(int(lg.get('retries') or 0) for lg in ledgers.values())} (as the SDK does)")
    for w in windows.values():
        if w.get("unknown_status_buckets"):
            notes.append(f"unknown status buckets priced as billed: {w['unknown_status_buckets']}")

    own_base = choose_baseline(run, None, groups, windows, by_label, snaps, stores)
    for g in groups:
        g.pop("cells_delta", None)
    return {
        "run": run, "point": posture.get("point"), "posture_ok": posture.get("binary_commit_matches_head"),
        "windows": {k: {"secs": v["secs"], **v["totals"], "by_tier": v["by_tier"], "by_op": v["by_op"],
                        "multipart_creates_rebilled": v["multipart_creates_rebilled"],
                        **({"alloc": v["alloc"]} if "alloc" in v else {})} for k, v in windows.items()},
        "total_cells": windows["total"]["cells"],
        "groups": groups, "unattributed_load_gaps_usd": gaps,
        "dimensions_usd": dims, "floor_usd": floor, "floor_spread_by_revenue": share,
        "floor_baseline": own_base, "floor_baseline_used": {k: v for k, v in (base or {}).items() if k != "rates"},
        "meters": {"ingestPayloadBytes": ingest, "readPayloadBytes": read, "storageByteSeconds": byte_s,
                   "ingestRecords": eff.get("ingestRecords"), "readRecords": eff.get("readRecords"),
                   "appendRequests": eff.get("appendRequests"), "settled": usage.get("settled"),
                   "retained": retained},
        "revenue_k2_usd": revenue, "per_gib_usd": per_gib, "margins_vs_k2": margins, "blended": blended,
        "wal_function": wal_fn, "storage": stor, "compute_d1": d1, "egress": eg,
        "calibration": calib, "validity": valid, "generator": ledgers, "notes": notes,
    }


def fmt_usd(x):
    if x is None:
        return "-"
    if 0 < abs(x) < 1e-5:
        return f"${x:.2e}"
    return f"${x:.6f}" if abs(x) < 0.01 else f"${x:.4f}"


def pct(x):
    return "-" if x is None else f"{x:.1%}"


def print_table(s: dict) -> None:
    w = s["windows"]
    print(f"\n== {s['point']}  ({s['run']})")
    print(f"{'window':<10}{'secs':>8}{'class A':>10}{'class B':>10}{'free':>8}{'amb A':>8}{'usd':>13}{'usd alt':>13}")
    for k in ("load", "deferred", "tail", "total"):
        if k in w:
            v = w[k]
            print(f"{k:<10}{v['secs']:>8.0f}{v.get('class_a', 0):>10}{v.get('class_b', 0):>10}{v.get('free', 0):>8}"
                  f"{v.get('ambiguous_a', 0):>8}{fmt_usd(v.get('usd', 0)):>13}{fmt_usd(v.get('usd_alt', 0)):>13}")
    print("\nphase groups (usd booked to produce/consume/lifecycle | floor by kind + rate | excluded):")
    for g in s["groups"]:
        p, al, gen = g["price"], g["alloc"], g["generator"]
        print(f"  {','.join(map(str, g['phases'])):<6}{'+'.join(g['modes']):<14}{g['dimension']:<10}{g['secs']:>6.0f}s"
              f" {fmt_usd(p['usd']):>10} = P {fmt_usd(al['produce']['usd'])} C {fmt_usd(al['consume']['usd'])}"
              f" L {fmt_usd(al['lifecycle']['usd'])} | F {fmt_usd(al['floor_kind']['usd'] + al['floor_rate']['usd'])}"
              f" | X {fmt_usd(al['excluded']['usd'])}  acked={gen.get('acked_requests', 0)}"
              f" deliv={gen.get('delivered_payload_bytes', 0)}B")
    for k in ("deferred", "tail"):
        if k in w and w[k].get("alloc"):
            al = w[k]["alloc"]
            print(f"  {k:<30}{w[k]['secs']:>6.0f}s {fmt_usd(w[k]['usd']):>10} = P {fmt_usd(al['produce']['usd'])}"
                  f" | F {fmt_usd(al['floor_kind']['usd'] + al['floor_rate']['usd'])}")
    d = s["dimensions_usd"]
    print("\ndimensions: " + "  ".join(f"{b}={fmt_usd(d[b]['usd'])}" for b in BUCKETS)
          + f"  gaps={fmt_usd(s['unattributed_load_gaps_usd'])}")
    f = s["floor_usd"]
    print("floor: " + "  ".join(f"{k}={fmt_usd(v)}" for k, v in f.items())
          + f"  (spread by revenue: {', '.join(f'{k} {v:.1%}' for k, v in s['floor_spread_by_revenue'].items())})")
    m = s["meters"]
    rt = m.get("retained") or {}
    print(f"\nmeters: ingest {m['ingestPayloadBytes']} B ({m['ingestRecords']} rec), read {m['readPayloadBytes']} B "
          f"({m['readRecords']} rec), storage {m['storageByteSeconds']} B*s ({rt.get('basis')}), "
          f"owned frame bytes now {rt.get('owned_frame_bytes_now')}, settled={m['settled']}")
    r = s["revenue_k2_usd"]
    print(f"K2 revenue: produce {fmt_usd(r['produce'])} consume {fmt_usd(r['consume'])} retained "
          f"{fmt_usd(r['retained'])} total {fmt_usd(r['total'])} (decimal GB {fmt_usd(r['total_decimal_gb'])})")
    print("per GiB (floor excluded): " + "  ".join(f"{k}={fmt_usd(v)}" for k, v in s["per_gib_usd"].items()))
    mg = s["margins_vs_k2"]
    for k in ("requests", "total", "total_with_floor"):
        print(f"margin vs K2 ({k}): " + "  ".join(f"{d}={pct(v)}" for d, v in mg[k].items()))
    print(f"  [{mg['floor_basis']}]")
    b = s.get("blended")
    if b:
        print("blended 1 - cost/revenue: " + "  ".join(f"{k[7:] or 'headline'}={pct(v)}" for k, v in b.items()
                                                      if k.startswith("margin")))
    if s["wal_function"]:
        print("\nW(r): phases  secs  req/s  shards  r/shard  WAL  floor  W/shard  W $/s  floor A")
        for x in s["wal_function"]:
            print(f"  {','.join(map(str, x['phases'])):<6}{x['secs']:>6.0f}{x['acked_req_per_s']:>7.1f}{x['active_shards']:>6}"
                  f"{x['r_per_shard']:>8.2f}{x['wal_puts']:>6.0f}{x['wal_puts_floor']:>7.1f}{x['W_per_shard']:>8.3f}"
                  f"  {fmt_usd(x['W_usd_per_s'])}{x['floor_class_a']:>7.0f}")
    st = s["storage"]
    if st.get("available"):
        ft = st.get("fit") or {}
        print(f"\nstorage: peak {st['peak_bytes']} B (customer {st['customer_peak_bytes']}), end {st['end_bytes']} B, "
              f"run cost {fmt_usd(st.get('usd_run'))}; k_end={st.get('k_end')} k_peak={st.get('k_peak')} "
              f"[{st.get('gauge_basis')}]; fit a={ft.get('a')} b={ft.get('b_secs')}s c={ft.get('c_bytes')} "
              f"r2={ft.get('r2')} n={ft.get('n')}{'' if ft.get('ok') else ' (' + str(ft.get('reason')) + ')'}")
        if st.get("retained_suppressed"):
            u = st["unreclaimed"]
            print(f"  retained suppressed: {st['retained_suppressed']}; U = {u['bytes']} B "
                  f"({u['bytes_per_gib_deleted']} B per GiB deleted, {fmt_usd(u['usd_per_month_per_gib_ingested'])}"
                  f"/month per GiB ingested)")
        else:
            print(f"  retained $/GiB-month {fmt_usd(st.get('retained_usd_per_gib_month'))} ({st.get('retained_basis')}); "
                  f"ramp-inclusive (old) {fmt_usd(st.get('retained_usd_per_gib_month_ramp_inclusive'))}")
        ser = st.get("series") or []
        pick = ser if len(ser) <= 8 else [ser[round(k * (len(ser) - 1) / 7)] for k in range(8)]
        tiers = sorted({x for row in pick for x in row["by_tier"]})
        print("  live_bytes (s: total | gauge | " + " ".join(tiers) + "):")
        for row in pick:
            print(f"  {row['t_rel_s']:>7.0f}s {row['bytes']:>11} | {row.get('gauge', '-')!s:>9} | "
                  + " ".join(str(row["by_tier"].get(x, 0)) for x in tiers))
    else:
        print(f"storage: {st['note']}")
    d1, eg = s["compute_d1"], s["egress"]
    print(f"compute (D1): memory {fmt_usd(d1['memory_usd'])} + cpu {fmt_usd(d1['cpu_usd'])} ({d1['cpu_s_capped']:.1f} "
          f"vCPU-s, floor {d1['cpu_s_floor']:.1f}) = {fmt_usd(d1['usd'])}; egress " + "  ".join(
              f"{k} {int(v)} B {fmt_usd(eg['usd'][k])}" for k, v in eg["bytes"].items()))
    cal = s["calibration"]
    if cal:
        print("\ncalibration (logical server calls vs physical s3lite requests, total window):")
        for row in cal["rows"]:
            print(f"  {row['op_class']:<16}{row['logical_ok']:>8}{row['logical_not_found']:>8}{row['logical_unbilled']:>8}"
                  f"{row['logical_err']:>8}{row['physical_2xx']:>9}{row['physical_404']:>8}{row['physical_non2xx']:>10}"
                  f"{str(row['physical_per_logical']):>8}")
        b = cal["bytes"]
        print(f"  bytes put: physical {b['physical_put']} logical {b['logical_put']}; "
              f"get: physical {b['physical_get']} logical {b['logical_get']}")
    v = s["validity"]
    bad = [x for x in v["checks"] if not x["ok"]]
    print(f"\nvalidity: {'VALID' if v['valid'] else 'VOID'} ({len(v['checks'])} checks"
          + (f"; failed: {[x['check'] + ' ' + json.dumps(x['detail']) for x in bad]}" if bad else "") + ")")
    for n in s["notes"]:
        print(f"note: {n}")


def main() -> int:
    args = sys.argv[1:]
    ext = None
    if "--baseline" in args:
        i = args.index("--baseline")
        ref = args[i + 1] if i + 1 < len(args) else ""
        del args[i:i + 2]
        prior = load(os.path.join(ref, "summary.json")) or {}
        ext = prior.get("floor_baseline")
        if not ext or not ext.get("rates"):
            print(f"price: --baseline {ref!r}: no summary.json with a floor_baseline (per-cell idle rates); "
                  "price that run first", file=sys.stderr)
            return 2
    if args:
        run = args[0]
    else:
        home = os.environ.get("K2_HOME", os.path.expanduser("~/.streams-k2"))
        runs = sorted(glob.glob(os.path.join(home, "results", "*")), key=os.path.getmtime)
        if not runs:
            print("price: no runs under $K2_HOME/results", file=sys.stderr)
            return 2
        run = runs[-1]
    s = price_run(run.rstrip("/"), ext)
    with open(os.path.join(run, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(s, f, indent=1, sort_keys=True, default=str)
    print_table(s)
    return 0


if __name__ == "__main__":
    sys.exit(main())
