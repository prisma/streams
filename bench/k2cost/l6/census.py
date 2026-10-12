#!/usr/bin/env python3
"""L6 seal spike: the per-point census (SPIKE-PLAN.md §2.3, with
SPIKE-PLAN-REVIEW.md A1, A2, A5, A6 and C1 applied).

  python3 census.py RUN_DIR... [--out census.json] [--md census.md]

Python 3.9 compatible, standard library only. Reads what bench/k2cost/run-local.sh
writes for an L6S.env point: marks.jsonl, boundaries.jsonl (s3lite stats2 and
the servers' debug/store, debug/usage, rss and procs at every phase edge),
scrape.jsonl (every 10 s), phase-*.ledger.json, posture.json, summary.json
(price.py), usage.json.

Windows (L6S.env phases): 3 = load, 4 = audit, 5 = deferred tail, 6 = audit,
7 = floor. The write-path window LT is load + tail with both audits excluded
(review A2); W is load start to tail end, audit 4 included, reported beside it.
Floor: every cell's rate in phase 7 times the window's seconds is subtracted
(the raw figure is kept). A5: the floor window's customer-tier PUT bytes are
reported; when they exceed 1.5 x the idle telemetry floor the subtraction may
remove real deferred work, and the figure is flagged a lower bound.

Stored bytes S: growth of the customer streams' frame bytes (debug/usage
frame_bytes per stream, the G10 gauge) over the load window, read at the
phase-3 boundaries (scrape samples nearest the boundary for a run whose
snapshots lack usage). L: the load phase's acked payload bytes (k2gen
ledger); c = L / S.

Uploads per stored byte: PUT body bytes per (tier, kind) from s3lite's byte
ledger (multipart parts included). WAL = shard/wal; shard L0 = SlateDB's
l0_flush_bytes; shard compaction = shard/sst - L0; shard meta =
shard/manifest + shard/compactions + shard/meta; history = hist/*; copy =
hseg/*. "copies" = everything in the customer tiers but the WAL.

Requests: s3lite's statuses; billed = 2xx + 404 (Class A: put, multipart,
list; Class B: get, head; deletes free). Per stored and per logical GiB.
SlateDB's own freezes = delta immutable_memtable_flushes - delta of the
flushes attributed (by before/after delta) to the 5 s ticker (review A1).
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import struct
import sys
import time

GIB = 2 ** 30
PROJECT = "proj-k2"
BILLED = ("2xx", "404")
OP_CLASS = {"put": "A", "multipart": "A", "list": "A", "get": "B", "head": "B"}
CUSTOMER_TIERS = ("shard", "hist", "hseg")
# gt-write-path.md §2: per-request rows in the WAL, measured 830-880 B, 0.83-0.91 KB in the plan.
ROWS_PER_REQ = (830.0, 910.0)
WAL_TOL = 0.15


# ---------------------------------------------------------------- reading

def jsonl(path):
    out = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except ValueError:
                        pass
    except FileNotFoundError:
        pass
    return out


def jload(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, ValueError):
        return default


def route_hash(project, name):
    """RouteHash::for_stream as debug/usage prints it (k2store.route_hash)."""
    raw = b"".join(struct.pack(">I", len(c)) + c for c in (b"route-v1", project.encode(), name.encode()))
    return hashlib.sha256(raw).hexdigest()[:32]


class Run:
    def __init__(self, d):
        self.dir = os.path.abspath(d)
        self.name = os.path.basename(self.dir)
        self.point = self.name.rsplit("-", 1)[0]
        self.marks = jsonl(os.path.join(d, "marks.jsonl"))
        self.bounds = {b["label"]: b for b in jsonl(os.path.join(d, "boundaries.jsonl"))}
        self.scrape = jsonl(os.path.join(d, "scrape.jsonl"))
        self.posture = jload(os.path.join(d, "posture.json"), {})
        self.summary = jload(os.path.join(d, "summary.json"), {})
        self.usage = jload(os.path.join(d, "usage.json"), {})
        self.snaps = {}
        for p in glob.glob(os.path.join(d, "s3lite-*.json")):
            self.snaps[os.path.basename(p)[7:-5]] = jload(p)
        self.ledgers = {}
        for p in glob.glob(os.path.join(d, "phase-*.ledger.json")):
            i = int(os.path.basename(p).split("-")[1].split(".")[0])
            self.ledgers[i] = jload(p, {})
        self.phases = {}
        ends = {m["i"]: m for m in self.marks if m.get("mark") == "phase_end" and "i" in m}
        for m in self.marks:
            if m.get("mark") == "phase_start":
                e = ends.get(m["i"], {})
                self.phases[m["i"]] = {"mode": m.get("mode"), "t0": m["t"], "t1": e.get("t"), "rc": e.get("rc"),
                                       "args": m.get("args")}
        env = {}
        for s in (self.posture.get("servers") or {}).values():
            env = s.get("env") or {}
            break
        self.env = env

    def b(self, phase, edge):
        return self.bounds.get(f"phase-{phase}-{edge}")

    def t(self, phase, edge):
        rec = self.b(phase, edge)
        return rec["t_ms"] if rec else None


# ---------------------------------------------------------------- deltas

def counts(s2):
    return {k: dict(v) for k, v in ((s2 or {}).get("cells") or {}).items()}


def put_cells(s2):
    pb = (s2 or {}).get("put_bytes")
    return dict(pb.get("cells") or {}) if isinstance(pb, dict) else None


def sub(a, b):
    """b - a for nested {key: {bucket: n}} or {key: n}."""
    out = {}
    nested = any(isinstance(v, dict) for v in list(a.values()) + list(b.values()))
    for k in set(a) | set(b):
        va, vb = a.get(k, {} if nested else 0), b.get(k, {} if nested else 0)
        if nested:
            va = va if isinstance(va, dict) else {}
            vb = vb if isinstance(vb, dict) else {}
            d = {s: vb.get(s, 0) - va.get(s, 0) for s in set(va) | set(vb)}
            d = {s: n for s, n in d.items() if n}
            if d:
                out[k] = d
        else:
            n = (vb or 0) - (va or 0)
            if n:
                out[k] = n
    return out


def add(x, y):
    out = {k: (dict(v) if isinstance(v, dict) else v) for k, v in x.items()}
    for k, v in y.items():
        if isinstance(v, dict):
            cur = out.setdefault(k, {})
            for s, n in v.items():
                cur[s] = cur.get(s, 0) + n
        else:
            out[k] = out.get(k, 0) + v
    return out


def scale(x, f):
    return {k: ({s: n * f for s, n in v.items()} if isinstance(v, dict) else v * f) for k, v in x.items()}


def floor_sub(win, floor_rate, secs):
    """win - rate x secs per cell, clamped at 0; also returns what the floor removed."""
    out, removed = {}, {}
    nested = any(isinstance(v, dict) for v in list(win.values()) + list(floor_rate.values()))
    for k in set(win) | set(floor_rate):
        v, r = win.get(k, {} if nested else 0), floor_rate.get(k, {} if nested else 0)
        if nested:
            v = v if isinstance(v, dict) else {}
            r = r if isinstance(r, dict) else {}
            d, rm = {}, {}
            for s in set(v) | set(r):
                want = r.get(s, 0) * secs
                got = v.get(s, 0)
                d[s] = max(0.0, got - want)
                rm[s] = got - d[s]
            out[k], removed[k] = d, rm
        else:
            want = (r or 0) * secs
            got = v or 0
            out[k] = max(0.0, got - want)
            removed[k] = got - out[k]
    return out, removed


def billed(cells):
    """{tier: {A, B}}, {key: {A, B}} and totals from {tier/kind/op: {bucket: n}}."""
    by_tier, by_cell, tot = {}, {}, {"A": 0.0, "B": 0.0}
    for key, buckets in cells.items():
        parts = key.split("/")
        if len(parts) != 3:
            continue
        cls = OP_CLASS.get(parts[2])
        if not cls:
            continue
        n = sum(buckets.get(s, 0) for s in BILLED)
        if not n:
            continue
        by_cell.setdefault(key, {"A": 0.0, "B": 0.0})[cls] += n
        by_tier.setdefault(parts[0], {"A": 0.0, "B": 0.0})[cls] += n
        tot[cls] += n
    return by_tier, by_cell, tot


def by_tier_kind(put):
    out = {}
    for key, n in put.items():
        tier, kind, _op = key.split("/")
        tk = f"{tier}/{kind}"
        out[tk] = out.get(tk, 0) + n
    return out


# ---------------------------------------------------------------- server side

def slatedb(rec):
    """Sum of the per-shard l6_slatedb members of every server in a boundary record."""
    tot, shards = {}, 0
    for s in ((rec or {}).get("servers") or {}).values():
        l6 = (((s or {}).get("store") or {}).get("l6_slatedb") or {}).get("shards") or {}
        for v in l6.values():
            shards += 1
            for k, x in v.items():
                if k != "all" and isinstance(x, (int, float)):
                    tot[k] = tot.get(k, 0) + x
            for k, x in (v.get("all") or {}).items():
                if isinstance(x, (int, float)):
                    tot["all:" + k] = tot.get("all:" + k, 0) + x
    return tot, shards


def customer_ids(run):
    names = set()
    for led in run.ledgers.values():
        names.update(k.split("#", 1)[0] for k in (led.get("per_stream") or {}))
    return {route_hash(PROJECT, n) for n in names}


def frame_bytes_at(run, phase, edge, ids):
    """Customer frame bytes at a boundary: its usage member, else the scrape sample nearest it."""
    rec = run.b(phase, edge)
    if not rec:
        return None, None
    tot, found = 0, False
    for s in (rec.get("servers") or {}).values():
        per = ((s or {}).get("usage") or {}).get("frame_bytes_by_stream")
        if isinstance(per, dict):
            found = True
            tot += sum(int(v) for k, v in per.items() if k in ids)
    if found:
        return tot, "boundary"
    t = rec["t_ms"]
    best = {}
    for r in run.scrape:
        if r.get("src") != "usage" or not r.get("body"):
            continue
        per = r["body"].get("frame_bytes_by_stream")
        if not isinstance(per, dict):
            continue
        srv = r.get("server")
        if srv not in best or abs(r["t"] - t) < abs(best[srv][0] - t):
            best[srv] = (r["t"], sum(int(v) for k, v in per.items() if k in ids))
    if not best:
        return None, None
    return sum(v for _, v in best.values()), "scrape(max %.1f s off)" % (max(abs(x - t) for x, _ in best.values()) / 1000)


def rss_series(run):
    pts = []
    for r in run.scrape:
        if r.get("src") == "load" and r.get("body") and r["body"].get("rss_mb") is not None:
            pts.append((r["t"], float(r["body"]["rss_mb"])))
    return sorted(pts)


def store_series(run, key):
    pts = []
    for r in run.scrape:
        if r.get("src") == "store" and r.get("body"):
            l6 = (r["body"].get("l6_slatedb") or {}).get("shards") or {}
            v = sum(x.get(key, 0) for x in l6.values() if isinstance(x.get(key, 0), (int, float)))
            if l6:
                pts.append((r["t"], v))
    return pts


def proc(rec, field):
    tot = 0.0
    for p in ((rec or {}).get("procs") or {}).values():
        v = p.get(field)
        if isinstance(v, (int, float)):
            tot += v
    return tot


# ---------------------------------------------------------------- reconciliation

def reconcile(s2):
    """Recompute stats2's byte-ledger identities from raw numbers: None when the snapshot has no ledger."""
    pb = (s2 or {}).get("put_bytes")
    if not isinstance(pb, dict) or "cells" not in pb:
        return None
    total = int(pb.get("total") or 0)
    cells = sum(int(v) for v in pb["cells"].values())
    out_t, put_t, live_t = {}, {}, {}
    for k, v in pb["cells"].items():
        put_t[k.split("/")[0]] = put_t.get(k.split("/")[0], 0) + int(v)
    for k, v in (pb.get("out") or {}).items():
        out_t[k.split("/")[0]] = out_t.get(k.split("/")[0], 0) + sum(int(x) for x in v.values())
    lb = (s2.get("live_bytes") or {}).get("cells") or {}
    for k, v in lb.items():
        live_t[k.split("/")[0]] = live_t.get(k.split("/")[0], 0) + int(v.get("bytes") or 0)
    tiers = set(put_t) | set(out_t) | set(live_t)
    bad_tiers = [t for t in tiers if put_t.get(t, 0) - out_t.get(t, 0) != live_t.get(t, 0)]
    stats = (s2.get("stats") or {}).get("put_bytes")
    return {"cells_eq_total": cells == total, "tiers_ok": not bad_tiers, "bad_tiers": bad_tiers,
            "live_total_ok": total - sum(out_t.values()) == int(((s2.get("live_bytes") or {}).get("total") or {}).get("bytes") or 0),
            "stats_put_bytes_ge_total": stats is None or int(stats) >= total}


def reconcile_all(run):
    snaps = []
    for label, b in run.bounds.items():
        snaps.append(("boundary " + label, b.get("s3lite")))
    for label, s in run.snaps.items():
        snaps.append(("snapshot " + label, s))
    for r in run.scrape:
        if r.get("src") == "s3lite" and r.get("body"):
            snaps.append(("scrape seq %s" % r.get("seq"), r["body"]))
    n, fails, missing = 0, [], 0
    for label, s2 in snaps:
        rc = reconcile(s2)
        if rc is None:
            missing += 1
            continue
        n += 1
        # /stats is read after stats2 in a snapshot, so it may only be larger.
        if not (rc["cells_eq_total"] and rc["tiers_ok"] and rc["live_total_ok"] and rc["stats_put_bytes_ge_total"]):
            fails.append({"snapshot": label, **rc})
    return {"snapshots_checked": n, "snapshots_without_ledger": missing, "failures": fails[:20], "n_failures": len(fails),
            "ok": n > 0 and not fails}


# ---------------------------------------------------------------- one run

def window_delta(run, intervals):
    """Summed deltas of counts, put cells, slatedb and procs over [(phase_from, phase_to)] boundary intervals."""
    cnt, put, sdb, secs, cpu = {}, {}, {}, 0.0, 0.0
    put_ok = True
    for p0, p1 in intervals:
        a, b = run.b(p0, "start"), run.b(p1, "end")
        if not a or not b:
            return None
        cnt = add(cnt, sub(counts(a["s3lite"]), counts(b["s3lite"])))
        pa, pbb = put_cells(a["s3lite"]), put_cells(b["s3lite"])
        if pa is None or pbb is None:
            put_ok = False
        else:
            put = add(put, sub(pa, pbb))
        sa, _ = slatedb(a)
        sb, _ = slatedb(b)
        sdb = add(sdb, sub(sa, sb))
        secs += (b["t_ms"] - a["t_ms"]) / 1000.0
        cpu += proc(b, "cpu_s") - proc(a, "cpu_s")
    return {"counts": cnt, "put": put if put_ok else None, "slatedb": sdb, "secs": secs, "cpu_s": cpu}


def census_run(d):
    run = Run(d)
    out = {"run": run.dir, "point": run.point, "notes": []}
    env = run.env
    out["posture"] = {k: env.get(k) for k in ("INITIAL_SHARDS", "WAL_FLUSH_GAP_MS", "L6_SPILL", "TAIL_RING_BYTES",
                                              "ADMIT_RSS_SHED_MB", "PROJECT_MEMORY_PRESSURE_BYTES", "PROJECT_SHARE_K",
                                              "MAX_UNABSORBED_BYTES_PER_INSTANCE", "L6_EXTRA")}
    s3p = run.posture.get("s3lite") or {}
    out["posture"]["s3lite_latency_ms"] = s3p.get("latency_ms")
    out["posture"]["s3lite_put_bytes_per_sec"] = s3p.get("put_bytes_per_sec")
    out["posture"]["git_head"] = (run.posture.get("git") or {}).get("head")
    out["posture"]["dirty_paths"] = (run.posture.get("git") or {}).get("dirty_paths")
    out["posture"]["binary_commit_matches_head"] = run.posture.get("binary_commit_matches_head")
    out["posture"]["binaries_sha256"] = run.posture.get("binaries_sha256")
    pe = run.posture.get("point_file") or ""
    out["point_args"] = (run.phases.get(3) or {}).get("args")

    # validity (price.py) and the spike's own void rules
    val = run.summary.get("validity") or {}
    failing = [c["check"] for c in val.get("checks", []) if not c.get("ok")]
    audits = {}
    for i in (4, 6):
        v = (run.ledgers.get(i) or {}).get("verify") or {}
        audits[i] = {k: v.get(k) for k in ("ok", "expected_records", "read_records", "records_missing",
                                           "unexpected_records", "mismatched_records", "offset_errors",
                                           "incomplete_blocks", "order_inversions")}
    rcs = {i: p.get("rc") for i, p in run.phases.items()}
    void = []
    # SPIKE-PLAN §2.3: "rss_mb > 450 alone does not void a run. RSS is one of the
    # quantities being measured." price.py's two RSS checks (level and slope) are
    # reported, never voiding; every other failing check voids.
    rss_checks = [c for c in failing if "rss_mb <= 450" in c or "rss slope" in c]
    voiding = [c for c in failing if c not in rss_checks]
    if not val.get("valid") and voiding:
        void.append("price.py validity: " + "; ".join(voiding))
    if any(rc not in (0, None) for rc in rcs.values()) or any(p.get("t1") is None for p in run.phases.values()):
        void.append("a phase exited non-zero or did not end: %s" % rcs)
    for i, a in audits.items():
        if not a.get("ok") or any(a.get(k) for k in ("records_missing", "unexpected_records", "mismatched_records",
                                                     "offset_errors", "incomplete_blocks")):
            void.append("audit phase %d not clean: %s" % (i, a))
    if not (run.usage.get("settled") or (run.summary.get("meters") or {}).get("settled")):
        void.append("usage meter not settled")
    out["validity"] = {"price_py_valid": val.get("valid"), "failing_checks": failing,
                       "failing_rss_checks_not_voiding": rss_checks, "phase_rc": rcs,
                       "audits": audits, "void_reasons": void, "valid": not void}

    # windows
    for need in (3, 5, 7):
        if not run.b(need, "start") or not run.b(need, "end"):
            out["error"] = "missing phase %d boundaries" % need
            return out
    LT = window_delta(run, [(3, 3), (5, 5)])
    W = window_delta(run, [(3, 5)])
    Lw = window_delta(run, [(3, 3)])
    F = window_delta(run, [(7, 7)])
    A1 = window_delta(run, [(4, 4)])
    A2 = window_delta(run, [(6, 6)])
    fsecs = F["secs"]
    frate_cnt = scale(F["counts"], 1.0 / fsecs) if fsecs > 0 else {}
    frate_put = scale(F["put"], 1.0 / fsecs) if (fsecs > 0 and F["put"] is not None) else None
    out["windows_s"] = {"load": Lw["secs"], "audit1": A1["secs"], "tail": LT["secs"] - Lw["secs"], "audit2": A2["secs"],
                        "floor": fsecs, "LT": LT["secs"], "W": W["secs"]}

    # stored and logical bytes
    ids = customer_ids(run)
    f0, src0 = frame_bytes_at(run, 3, "start", ids)
    f1, src1 = frame_bytes_at(run, 3, "end", ids)
    S = (f1 - f0) if (f0 is not None and f1 is not None) else None
    led = run.ledgers.get(3) or {}
    L = int(led.get("payload_bytes") or 0)
    reqs = int(led.get("acked_requests") or 0)
    recs = int(led.get("acked_records") or 0)
    out["load"] = {"stored_bytes_S": S, "S_source": [src0, src1], "logical_bytes_L": L, "c_L_over_S": (L / S) if S else None,
                   "acked_requests": reqs, "acked_records": recs, "stored_bytes_per_request": (S / reqs) if (S and reqs) else None,
                   "logical_bytes_per_request": (L / reqs) if reqs else None,
                   "req_per_s": reqs / Lw["secs"] if Lw["secs"] else None,
                   "stored_MBps": S / Lw["secs"] / 1e6 if (S and Lw["secs"]) else None,
                   "append_p50_ms": led.get("append_p50_ms"), "append_p99_ms": led.get("append_p99_ms"),
                   "customer_streams": len(ids), "meter_ingest_payload_bytes": (run.summary.get("meters") or {}).get("ingestPayloadBytes")}
    if not S:
        out["error"] = "no stored bytes"
        return out

    # uploads per stored byte
    shards_open = max(1, slatedb(run.b(3, "end"))[1])
    up = {}
    if LT["put"] is not None:
        put_lt, put_rm = floor_sub(LT["put"], frate_put or {}, LT["secs"])
        put_w, _ = floor_sub(W["put"], frate_put or {}, W["secs"])
        for label, put, raw in (("LT", put_lt, LT["put"]), ("W", put_w, W["put"]), ("LT_raw", LT["put"], LT["put"])):
            tk = by_tier_kind(put)
            g = lambda *keys: sum(tk.get(k, 0) for k in keys)  # noqa: E731
            l0 = max(0.0, LT["slatedb"].get("l0_flush_bytes", 0) if label != "W" else W["slatedb"].get("l0_flush_bytes", 0))
            wal = g("shard/wal")
            sst = g("shard/sst")
            meta = g("shard/manifest", "shard/compactions", "shard/meta")
            hist = sum(v for k, v in tk.items() if k.startswith("hist/"))
            hseg = sum(v for k, v in tk.items() if k.startswith("hseg/"))
            other = {t: sum(v for k, v in tk.items() if k.startswith(t + "/")) for t in ("telemetry", "fleet", "registry", "other")}
            shard_l0 = min(l0, sst)
            row = {"wal": wal / S, "shard_l0": shard_l0 / S, "shard_cmp": (sst - shard_l0) / S, "shard_meta": meta / S,
                   "hist": hist / S, "hseg": hseg / S}
            row["shard_tier"] = row["shard_l0"] + row["shard_cmp"] + row["shard_meta"]
            row["copies"] = row["shard_tier"] + row["hist"] + row["hseg"]
            row["total"] = row["wal"] + row["copies"]
            row["shard_plus_hist"] = row["shard_tier"] + row["hist"]
            row["non_customer_tiers"] = {t: v / S for t, v in other.items()}
            row["by_tier_kind"] = {k: v / S for k, v in sorted(tk.items())}
            up[label] = row
        up["floor_removed_bytes_LT"] = {k: v for k, v in by_tier_kind(put_rm).items() if v}
        fput = by_tier_kind(F["put"])
        cust_floor = sum(v for k, v in fput.items() if k.split("/")[0] in CUSTOMER_TIERS)
        removed_cust = sum(v for k, v in by_tier_kind(put_rm).items() if k.split("/")[0] in CUSTOMER_TIERS)
        up["floor_window"] = {"put_bytes_per_s": sum(F["put"].values()) / fsecs, "customer_tier_put_bytes_per_s": cust_floor / fsecs,
                              "by_tier_kind_per_s": {k: v / fsecs for k, v in sorted(fput.items()) if v},
                              "customer_bytes_removed_over_S": removed_cust / S,
                              "hist_or_hseg_put_in_floor": any(v for k, v in fput.items() if k.split("/")[0] in ("hist", "hseg")),
                              # A5 (no M10 idle reference yet): deferred work still running in the floor window
                              # inflates the floor; above 2% of S removed, read LT_raw as the upper and LT as a lower bound.
                              "suspect": removed_cust / S > 0.02 or any(v for k, v in fput.items() if k.startswith("hist/sst"))}
    else:
        out["notes"].append("no s3lite byte ledger (unmodified s3lite): uploads by cell unavailable; global put_bytes only")
        def gput(rec):
            return int((((rec or {}).get("s3lite") or {}).get("stats") or {}).get("put_bytes") or 0)
        tot = sum(gput(run.b(p1, "end")) - gput(run.b(p0, "start")) for p0, p1 in [(3, 3), (5, 5)])
        up["LT_raw"] = {"total_all_tiers": tot / S}
    out["uploads_per_stored_byte"] = up

    # WAL ratio expectation (gt-write-path §2)
    spr = S / reqs if reqs else None
    if up.get("LT") and spr:
        exp_lo, exp_hi = 1 + ROWS_PER_REQ[0] / spr, 1 + ROWS_PER_REQ[1] / spr
        w = up["LT"]["wal"]
        out["wal_ratio_check"] = {"measured": w, "expected_band": [exp_lo, exp_hi],
                                  "tolerance_band": [exp_lo * (1 - WAL_TOL), exp_hi * (1 + WAL_TOL)],
                                  "implied_row_bytes_per_request": (w - 1) * spr,
                                  "pass": exp_lo * (1 - WAL_TOL) <= w <= exp_hi * (1 + WAL_TOL)}
        sph = up["LT"]["shard_plus_hist"]
        out["shard_plus_hist_check"] = {"measured": sph, "with_wal": sph + up["LT"]["wal"], "band": [4.0, 9.0],
                                        "pass": 4.0 <= sph <= 9.0}

    # requests
    cnt_lt, cnt_rm = floor_sub(LT["counts"], frate_cnt, LT["secs"])
    bt, bc, btot = billed(cnt_lt)
    bt_raw, bc_raw, btot_raw = billed(LT["counts"])
    sg, lg = S / GIB, L / GIB
    out["requests"] = {
        "per_stored_gib": {"class_a": btot["A"] / sg, "class_b": btot["B"] / sg},
        "per_logical_gib": {"class_a": btot["A"] / lg, "class_b": btot["B"] / lg},
        "raw_per_stored_gib": {"class_a": btot_raw["A"] / sg, "class_b": btot_raw["B"] / sg},
        "by_tier_per_stored_gib": {t: {"class_a": v["A"] / sg, "class_b": v["B"] / sg} for t, v in sorted(bt.items())},
        "by_cell_per_stored_gib": {k: {"class_a": v["A"] / sg, "class_b": v["B"] / sg} for k, v in sorted(bc.items())},
        "by_cell_counts_LT_raw": {k: v for k, v in sorted(LT["counts"].items())},
        "audit_reads": {"audit1": {"billed": billed(A1["counts"])[2], "secs": A1["secs"]},
                        "audit2": {"billed": billed(A2["counts"])[2], "secs": A2["secs"]},
                        "class_b_per_logical_gib_audited": (billed(A1["counts"])[2]["B"] / (L / GIB)) if L else None},
    }
    prod = (run.summary.get("dimensions_usd") or {}).get("produce") or {}
    out["requests"]["price_py_produce"] = {"class_a": prod.get("class_a"), "class_b": prod.get("class_b"),
                                           "class_a_per_stored_gib": (prod.get("class_a") or 0) / sg,
                                           "class_b_per_stored_gib": (prod.get("class_b") or 0) / sg,
                                           "note": "whole run's produce dimension (warm-up included), price.py allocation"}

    # WAL PUTs
    walputs_load = sum(v.get("2xx", 0) for k, v in Lw["counts"].items() if k == "shard/wal/put")
    walputs_lt = sum(v.get("2xx", 0) for k, v in cnt_lt.items() if k == "shard/wal/put")
    out["wal_puts"] = {"load_window": walputs_load, "per_s_per_shard_load": walputs_load / Lw["secs"] / shards_open,
                       "per_stored_gib_LT": walputs_lt / sg, "bytes_per_put_load": ((LT["put"] or {}).get("shard/wal/put", 0) / walputs_lt) if walputs_lt else None}

    # SlateDB
    sd = LT["slatedb"]
    imm = sd.get("immutable_memtable_flushes", 0)
    att = sd.get("ticker_flush_attributed", 0)
    mem = store_series(run, "total_mem_size_bytes")
    out["slatedb"] = {"shards_open": shards_open, "immutable_memtable_flushes": imm, "ticker_flushes": sd.get("ticker_flushes", 0),
                      "ticker_flush_attributed": att, "own_freezes": imm - att, "l0_flush_bytes": sd.get("l0_flush_bytes", 0),
                      "memtable_write_bytes": sd.get("memtable_write_bytes", 0), "backpressure_count": sd.get("backpressure_count", 0),
                      "ticker_flush_errors": sd.get("ticker_flush_errors", 0),
                      "total_mem_size_bytes_max": max((v for _, v in mem), default=None),
                      "compactor_bytes_compacted": sum(v for k, v in sd.items() if k.startswith("all:slatedb.compactor.bytes_compacted")),
                      "memtable_freeze_count": sd.get("all:slatedb.memtable_flush.memtable_freeze_count"),
                      "l0_flush_count": sd.get("all:slatedb.memtable_flush.l0_flush_count")}

    # polls: billed GETs on manifest/compactions per open shard DB second
    def polls(cells, secs, tier):
        n = sum(sum(v.get(s, 0) for s in BILLED) for k, v in cells.items()
                if k in (f"{tier}/manifest/get", f"{tier}/compactions/get"))
        return n / secs / (shards_open if tier == "shard" else 1) if secs else None
    out["polls"] = {"shard_load_per_db_s": polls(Lw["counts"], Lw["secs"], "shard"),
                    "shard_floor_per_db_s": polls(F["counts"], fsecs, "shard"),
                    "hist_load_per_s": polls(Lw["counts"], Lw["secs"], "hist"),
                    "hist_floor_per_s": polls(F["counts"], fsecs, "hist")}

    # RSS and CPU
    rs = rss_series(run)
    t0, t1 = run.t(3, "start"), run.t(3, "end")
    half = [v for t, v in rs if t0 + (t1 - t0) / 2 <= t <= t1]
    out["rss_mb"] = {"mean_second_half_load": (sum(half) / len(half)) if half else None, "n": len(half),
                     "max_run": max((v for _, v in rs), default=None),
                     "max_load": max((v for t, v in rs if t0 <= t <= t1), default=None),
                     "end_floor": rs[-1][1] if rs else None}
    out["cpu"] = {"server_cpu_s_LT": LT["cpu_s"], "vcpu_s_per_logical_gib": LT["cpu_s"] / lg if lg else None,
                  "server_cpu_s_load": Lw["cpu_s"]}

    # byte-ledger reconciliation in every snapshot
    out["reconciliation"] = reconcile_all(run)
    return out


def table(runs):
    hdr = ["point", "valid", "S MB", "c", "S/req", "WAL", "WAL exp", "L0", "cmp", "meta", "hist", "sh+hist", "total",
           "A/sGiB", "B/sGiB", "WAL PUT/s", "own frz", "rss½", "recon"]
    rows = []
    for name, r in sorted(runs.items()):
        if "error" in r:
            rows.append([name, "ERR " + r["error"]] + [""] * (len(hdr) - 2))
            continue
        up = (r.get("uploads_per_stored_byte") or {}).get("LT") or {}
        wc = r.get("wal_ratio_check") or {}
        f = lambda x, n=2: ("%.*f" % (n, x)) if isinstance(x, (int, float)) else "-"  # noqa: E731
        rows.append([name.replace("L6S-", ""), "yes" if r["validity"]["valid"] else "VOID", f(r["load"]["stored_bytes_S"] / 1e6, 0),
                     f(r["load"]["c_L_over_S"]), f(r["load"]["stored_bytes_per_request"], 0), f(up.get("wal")),
                     ("%.2f-%.2f" % tuple(wc["expected_band"])) if wc else "-", f(up.get("shard_l0")), f(up.get("shard_cmp")),
                     f(up.get("shard_meta"), 3), f(up.get("hist")), f(up.get("shard_plus_hist")), f(up.get("total")),
                     f(r["requests"]["per_stored_gib"]["class_a"], 0), f(r["requests"]["per_stored_gib"]["class_b"], 0),
                     f(r["wal_puts"]["per_s_per_shard_load"]), str(r["slatedb"]["own_freezes"]),
                     f(r["rss_mb"]["mean_second_half_load"], 0),
                     ("ok" if r["reconciliation"]["ok"] else "FAIL") if r["reconciliation"]["snapshots_checked"] else "n/a (no ledger)"])
    out = ["| " + " | ".join(hdr) + " |", "|" + "---|" * len(hdr)]
    out += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs", nargs="+")
    ap.add_argument("--out")
    ap.add_argument("--md")
    a = ap.parse_args()
    runs = {}
    for d in a.runs:
        if not os.path.isdir(d):
            continue
        try:
            runs[os.path.basename(os.path.abspath(d))] = census_run(d)
        except Exception as e:  # noqa: BLE001 - one broken run must not hide the others
            import traceback
            traceback.print_exc()
            runs[os.path.basename(os.path.abspath(d))] = {"error": "%s: %s" % (type(e).__name__, e)}
    doc = {"generated_unix": int(time.time()), "tool": os.path.abspath(__file__), "runs": runs}
    md = table(runs)
    if a.out:
        with open(a.out, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=1, sort_keys=True)
        md_path = a.md or os.path.splitext(a.out)[0] + ".md"
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(md + "\n")
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
