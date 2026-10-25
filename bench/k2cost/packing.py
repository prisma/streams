#!/usr/bin/env python3
"""Packing: N tenants of one profile on one launch cell, priced per tenant
(README "Packing").

  python3 bench/k2cost/packing.py price RUN [--window NAME] [--cpu-factor F]
      one tenants-mode run of run-local.sh (a PACK-T or PACK-S step): the
      instance's month at the owner's prices from its counts in WINDOW
      (default `period steady`), per tenant, against each tenant's K2 bill
      and the 80%-margin ceiling, and how near each of the cell's limits the
      step ran. Writes RUN/packing.json and prints it.
  python3 bench/k2cost/packing.py curve RUN [RUN ...] [--out FILE]
      the steps of one profile side by side (each priced as `price` does):
      the cost per tenant as N grows, a line through the instance's month
      (its fixed part and its cost per added tenant), the N at which each
      limit would bind, and the cost per tenant there.

The instance's month is its whole bill, as the queue benchmark prices a
dedicated instance (docs/QUEUES-PRICING-BENCHMARK.md section 11): every
store request the window counted (Class A and B as k2cells classes them:
404 billed, DELETE free), the bytes it PUT (transfer in), the growth of the
bucket's live bytes held for half a month (the tree keeps everything), the
server's CPU seconds, the provisioned 1 GiB and the bytes the tenants were
delivered (egress), each scaled from the window to 30 days. Nothing is a
floor share: the instance carries only these tenants. Two price cases:
the owner's, and Class A and B at half (the second case the owner named).
CPU and RSS on a Mac are indicative (Fable's report section 4.4 item 4):
--cpu-factor (default 1) scales the CPU seconds for the vCPU limit and
price, 2 being the field-to-local factor F5 measured.

A tenant's K2 bill is K2's three prices on the customer's uncompressed
bytes in the window (the driver's acked payload bytes produced and payload
bytes delivered; decimal GB), retained bytes as Fable's profiles keep
them (T unlimited: half a month's produce on average in month 1; S 7
days), scaled to a month. The meters each project's usage read settled
(tenants/usage.json) are reported beside it.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import counts as C  # noqa: E402
from k2cells import jsonl, load  # noqa: E402

MONTH_S = 30 * 86400
MONTH_H = 720.0
GB = 1e9
GIB = float(1 << 30)
# The owner's prices (streams-cost report 2026-10-24 section 2; the
# queue benchmark's): Tigris less 25% on requests and storage.
PRICES = {"class_a": 3.75e-6, "class_b": 0.375e-6, "storage_gib_month": 0.015, "transfer_gb": 0.01,
          "egress_gb": 0.005, "mem_gb_h": 0.006, "cpu_vcpu_h": 0.064}
CASES = {"owner": 1.0, "ab50": 0.5}  # the multiplier on Class A and B
INSTANCE_GB = 1.0
K2 = {"produce_gb": 0.04, "consume_gb": 0.04, "retain_gb_month": 0.02}
CEILING = 0.20  # our cost at most 20% of the K2 bill: an 80% margin
# Retention days by the manifest class (Fable's profiles, report section 4.1); None keeps everything.
RETENTION_DAYS = {"t": None, "s": 7}
# The cell's limits a step is held to (deploy/profiles/shared-cell.env, compute-1g.env): the server
# environment posture.json records wins; projects is cell-admin's default --max-projects.
LIMIT_DEFAULTS = {"projects": 1000, "SSE_MAX_CONNECTIONS": 1200, "ADMIT_RSS_SHED_MB": 500,
                  "CELL_ENVELOPE_REQUESTS_PER_SEC": 1411, "CELL_ENVELOPE_APPEND_BYTES_PER_SEC": 2060000}
PUMP_CAP_PER_S = 9.9  # WAL PUTs a second of one shard at the cap (report section 2)
DEFAULT_WINDOW = "period steady"


def _num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def period_bounds(run: str, window: str):
    """The window's (t0_ms, t1_ms) from marks.jsonl: a period's marks, else
    the counts window's snapshot times."""
    if window.startswith("period "):
        name = window[len("period "):]
        t = {m.get("mark"): m.get("t") for m in jsonl(os.path.join(run, "marks.jsonl"))
             if m.get("mark") in ("period_start", "period_end") and m.get("name") == name}
        if t.get("period_start") and t.get("period_end"):
            return int(t["period_start"]), int(t["period_end"])
    return None


def driver_window(run: str, window: str) -> dict:
    """The tenants driver's counters summed over the 10 s windows whose
    midpoint falls in the manifest period (windows.jsonl), by class and in
    all; status counts summed into `requests`."""
    manifest = load(os.path.join(run, "tenants-manifest.json"), {}) or {}
    head = load(os.path.join(run, "tenants", "run.json"), {}) or {}
    scale = _num(head.get("time_scale") or 1) or 1.0
    span = None
    if window.startswith("period "):
        p = (manifest.get("periods") or {}).get(window[len("period "):])
        if p:
            span = (p[0] * scale, p[1] * scale)
    out = {"all": {}, "by_class": {}, "windows": 0, "span_s": span}
    for w in jsonl(os.path.join(run, "tenants", "windows.jsonl")):
        mid = _num(w.get("elapsed_s")) - _num(w.get("window_s")) / 2
        if span and not (span[0] <= mid < span[1]):
            continue
        out["windows"] += 1
        status = w.get("status_counts") or {}
        for into in (out["all"], out["by_class"].setdefault(w.get("class") or "?", {})):
            for k, v in w.items():
                if k in ("t", "elapsed_s", "window_s", "phases", "connected", "inflight") or k.endswith("_ms"):
                    continue
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    into[k] = into.get(k, 0) + v
            if "requests" not in w:  # every answer, when the meter counted no `requests`
                into["requests"] = into.get("requests", 0) + sum(int(n) for n in status.values())
            sc = into.setdefault("status_counts", {})
            for k, v in status.items():
                sc[k] = sc.get(k, 0) + int(v)
    return out


def scrape_window(run: str, t0: int | None, t1: int | None) -> dict:
    """The cell's gauges over [t0, t1] from scrape.jsonl: RSS, sheds, the
    parked waits and SSE connections, and the bucket's live bytes."""
    rss, parked, sse, inflight, live = [], [], [], [], []
    shed = []
    for r in jsonl(os.path.join(run, "scrape.jsonl")):
        t, body = r.get("t"), r.get("body")
        if body is None or (t0 is not None and t < t0) or (t1 is not None and t > t1):
            continue
        src = r.get("src")
        if src == "load":
            if body.get("rss_mb") is not None:
                rss.append(_num(body["rss_mb"]))
            shed.append((t, int(_num(body.get("admit_shed"))), int(_num(body.get("admit_shed_rss")))))
            sse.append(int(_num(body.get("sse_connections"))))
            inflight.append(int(_num(body.get("inflight_now"))))
        elif src == "operator":
            loc = body.get("local") or {}
            if loc.get("parked") is not None:
                parked.append(int(_num(loc["parked"])))
        elif src == "s3lite":
            total = ((body.get("live_bytes") or {}).get("total") or {}).get("bytes")
            if total is not None:
                live.append((t, int(total)))
    shed.sort()
    mean = lambda xs: (sum(xs) / len(xs)) if xs else None  # noqa: E731
    return {
        "rss_mb_max": max(rss) if rss else None, "rss_mb_mean": mean(rss), "samples": len(rss),
        "parked_max": max(parked) if parked else None, "parked_mean": mean(parked),
        "sse_max": max(sse) if sse else None, "inflight_max": max(inflight) if inflight else None,
        "admit_shed": (shed[-1][1] - shed[0][1]) if len(shed) > 1 else None,
        "admit_shed_rss": (shed[-1][2] - shed[0][2]) if len(shed) > 1 else None,
        "live_bytes_growth_per_s": ((live[-1][1] - live[0][1]) / ((live[-1][0] - live[0][0]) / 1000))
        if len(live) > 1 and live[-1][0] > live[0][0] else None,
    }


def server_env(run: str) -> dict:
    posture = load(os.path.join(run, "posture.json"), {}) or {}
    return ((posture.get("servers") or {}).get("streams-1") or {}).get("env") or {}


def limits_of(env: dict) -> dict:
    return {k: (_num(env.get(k)) or v) if k != "projects" else v for k, v in LIMIT_DEFAULTS.items()}


def month_terms(w: dict, drv: dict, gauges: dict, secs: float, cpu_factor: float) -> dict:
    """The instance's month by term at the owner's prices (Class A and B at 1x)."""
    scale = MONTH_S / secs
    cpu_s = sum(_num(v) for v in (w.get("cpu_s") or {}).values()) * cpu_factor
    egress = _num(drv["all"].get("wire_bytes_down")) or _num(drv["all"].get("delivered_payload_bytes"))
    growth = max(0.0, gauges.get("live_bytes_growth_per_s") or 0.0)
    return {
        "class_a": _num(w.get("class_a")) * scale * PRICES["class_a"],
        "class_b": _num(w.get("class_b")) * scale * PRICES["class_b"],
        "transfer": _num(w.get("put_bytes")) * scale / GB * PRICES["transfer_gb"],
        # month 1 holds half the month's growth on average: nothing expires
        "storage": growth * MONTH_S / 2 / GIB * PRICES["storage_gib_month"],
        "cpu": cpu_s * scale / 3600 * PRICES["cpu_vcpu_h"],
        "memory": INSTANCE_GB * MONTH_H * PRICES["mem_gb_h"],
        "egress": egress * scale / GB * PRICES["egress_gb"],
    }


def total(terms: dict, ab: float) -> float:
    return sum(v * (ab if k in ("class_a", "class_b") else 1.0) for k, v in terms.items())


def k2_bill(drv: dict, secs: float, tenants: int, retention_days) -> dict:
    scale = MONTH_S / secs
    produced = _num(drv["all"].get("payload_bytes")) * scale / tenants / GB
    consumed = _num(drv["all"].get("delivered_payload_bytes")) * scale / tenants / GB
    retained = produced * (0.5 if retention_days is None else min(retention_days, 30) / 30.0)
    usd = produced * K2["produce_gb"] + consumed * K2["consume_gb"] + retained * K2["retain_gb_month"]
    return {"produced_gb": produced, "consumed_gb": consumed, "retained_gb_month": retained, "usd": usd,
            "ceiling_usd": CEILING * usd, "retention_days": retention_days}


def meters(run: str) -> dict:
    u = (load(os.path.join(run, "tenants", "usage.json"), {}) or {}).get("projects") or {}
    out = {"projects": len(u), "settled": sum(1 for p in u.values() if p.get("settled"))}
    for k in ("ingestPayloadBytes", "readPayloadBytes", "ingestRecords", "readRecords"):
        out[k] = sum(int(_num((p.get("meters") or {}).get(k))) for p in u.values())
    return out


def audit_failures(run: str):
    doc = load(os.path.join(run, "tenants", "audit.json"))
    if not doc:
        return None
    return sum(1 for p in (doc.get("projects") or {}).values()
               if any(not (c or {}).get("ok") for c in (p.get("prefixes") or {}).values()))


def refusals(run: str) -> dict:
    classes = load(os.path.join(run, "tenants", "classes.json"), {}) or {}
    out = {}
    for name, c in classes.items():
        r = c.get("refusals") or {}
        out[name] = {"typed": sum((r.get("typed") or {}).values()), "untyped": sum((r.get("untyped") or {}).values()),
                     "transport": int(_num(r.get("transport")))}
    return out


def price(run: str, window: str = DEFAULT_WINDOW, cpu_factor: float = 1.0) -> dict:
    run = run.rstrip("/")
    c = C.counts(run)
    notes = []
    asked = window
    w = c["windows"].get(window)
    if not w:
        notes.append(f"no counts window {window!r}: priced on `load` (setup included)")
        window, w = "load", c["windows"]["load"]
    secs = _num(w["secs"])
    head = load(os.path.join(run, "tenants", "run.json"), {}) or {}
    manifest = load(os.path.join(run, "tenants-manifest.json"), {}) or {}
    classes = manifest.get("classes") or []
    tenants = int(head.get("projects") or sum(int(cl.get("count") or 0) for cl in classes))
    if tenants < 1:
        raise SystemExit(f"packing: {run} names no tenants (tenants/run.json, tenants-manifest.json)")
    cls = classes[0].get("class") if len(classes) == 1 else None
    bounds = period_bounds(run, window)
    if bounds is None:
        bounds = (None, None)
        if window.startswith("period "):
            notes.append("no period marks: the gauges are the whole run's")
    drv = driver_window(run, window)
    gauges = scrape_window(run, *bounds)
    terms = month_terms(w, drv, gauges, secs, cpu_factor)
    env = server_env(run)
    lim = limits_of(env)
    cpu_vcpu = sum(_num(v) for v in (w.get("cpu_s") or {}).values()) / secs
    flushes = _num(w.get("pump_flushes"))
    use = {
        "projects": {"value": tenants, "limit": lim["projects"]},
        "parked_waits": {"value": gauges["parked_max"], "limit": lim["SSE_MAX_CONNECTIONS"]},
        "vcpu": {"value": cpu_vcpu * cpu_factor, "limit": 1.0, "mac_vcpu": cpu_vcpu},
        "rss_mb": {"value": gauges["rss_mb_max"], "limit": lim["ADMIT_RSS_SHED_MB"]},
        "requests_per_s": {"value": drv["all"].get("requests", 0) / secs, "limit": lim["CELL_ENVELOPE_REQUESTS_PER_SEC"]},
        "append_bytes_per_s": {"value": _num(drv["all"].get("payload_bytes")) / secs,
                               "limit": lim["CELL_ENVELOPE_APPEND_BYTES_PER_SEC"]},
    }
    for v in use.values():
        v["use"] = (v["value"] / v["limit"]) if v["value"] is not None and v["limit"] else None
    bill = k2_bill(drv, secs, tenants, RETENTION_DAYS.get(cls) if cls else None)
    cases = {}
    for name, ab in CASES.items():
        inst = total(terms, ab)
        per = inst / tenants
        cases[name] = {"instance_month": inst, "per_tenant_month": per,
                       "per_tenant_over_ceiling": (per / bill["ceiling_usd"]) if bill["ceiling_usd"] else None}
    refused = refusals(run)
    audit = audit_failures(run)
    m = meters(run)
    checks = [
        (f"the counts have the window {asked!r}", window == asked),
        ("server built from the declared commit", bool(((load(os.path.join(run, "posture.json"), {}) or {})
                                                        .get("server_commit") or {}).get("matches_expected"))),
        ("no refusal of a compliant tenant", all(sum(r.values()) == 0 for r in refused.values())),
        ("no shed in the window", not (gauges["admit_shed"] or 0)),
        ("every producing project's audit passes", audit == 0),
        ("every project's meters settled", m["projects"] == tenants and m["settled"] == tenants),
    ]
    return {
        "run": run, "point": c.get("point"), "window": window, "secs": secs, "tenants": tenants, "class": cls,
        "server": c.get("server"), "cpu_factor": cpu_factor, "notes": notes,
        "counts": {k: w.get(k) for k in ("class_a", "class_b", "put_bytes", "get_bytes", "pump_flushes",
                                          "pump_flushed_reqs", "ring_hits", "ring_misses", "cpu_s")},
        "rates_per_s": {"class_a": _num(w.get("class_a")) / secs, "class_b": _num(w.get("class_b")) / secs,
                        "wal_puts": sum(v["n"] for k, v in (w.get("cells") or {}).items() if k == "shard/wal/put") / secs,
                        "pump_flushes": flushes / secs, "pump_fill": flushes / secs / PUMP_CAP_PER_S,
                        "put_bytes": _num(w.get("put_bytes")) / secs},
        "driver": {"windows": drv["windows"], "span_s": drv["span_s"], **{k: drv["all"].get(k) for k in (
            "requests", "acked_requests", "payload_bytes", "delivered_payload_bytes", "wire_bytes_down", "pulls",
            "settles")}},
        "gauges": gauges, "limits": use, "terms_month": terms, "cases": cases, "k2": bill,
        "refusals": refused, "audit_failed_projects": audit, "meters": m,
        "validity": {"valid": all(ok for _, ok in checks), "checks": [{"check": n, "ok": ok} for n, ok in checks]},
    }


def fmt(v, d=2) -> str:
    if v is None:
        return "-"
    if isinstance(v, float) and abs(v) < 0.01 and v != 0:
        return f"{v:.2e}"
    return f"{v:,.{d}f}" if isinstance(v, float) else f"{v:,}"


def print_step(s: dict) -> None:
    valid = "VALID" if s["validity"]["valid"] else "VOID"
    print(f"== {s['point']} ({s['tenants']} tenants of class {s['class']}), window {s['window']} {s['secs']:.0f} s: {valid}")
    for n in s["notes"]:
        print(f"   note: {n}")
    for ch in s["validity"]["checks"]:
        if not ch["ok"]:
            print(f"   FAILED: {ch['check']}")
    print(f"   {'term (instance month)':<24}{'owner $':>12}")
    for k, v in s["terms_month"].items():
        print(f"   {k:<24}{fmt(v):>12}")
    for name, cs in s["cases"].items():
        print(f"   {name:<8} instance {fmt(cs['instance_month'])}/month, per tenant {fmt(cs['per_tenant_month'], 4)}"
              f", {fmt(cs['per_tenant_over_ceiling'], 1)}x the ceiling")
    k = s["k2"]
    print(f"   K2 bill per tenant {fmt(k['usd'], 4)}/month (ceiling {fmt(k['ceiling_usd'], 5)}): produced "
          f"{fmt(k['produced_gb'], 4)} GB, consumed {fmt(k['consumed_gb'], 4)} GB, retained {fmt(k['retained_gb_month'], 4)} GB-month")
    r = s["rates_per_s"]
    print(f"   WAL PUT/s {fmt(r['wal_puts'])} (pump fill {fmt(r['pump_fill'])}), Class A/s {fmt(r['class_a'])}, "
          f"Class B/s {fmt(r['class_b'])}")
    print("   limits: " + ", ".join(f"{n} {fmt(v['value'])}/{fmt(v['limit'], 0)}" for n, v in s["limits"].items()))


# ---------------------------------------------------------------- curve

def line(xs: list, ys: list):
    """Least squares y = a + b x; (a, b), or None under two distinct xs."""
    if len(set(xs)) < 2:
        return None
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sum((x - mx) ** 2 for x in xs)
    return my - b * mx, b


def at(xs: list, ys: list, x: float) -> float:
    """Piecewise-linear through the steps, extended by the end segments."""
    pts = sorted(zip(xs, ys))
    if len(pts) == 1:
        return pts[0][1]
    i = 0
    while i < len(pts) - 2 and x > pts[i + 1][0]:
        i += 1
    (x0, y0), (x1, y1) = pts[i], pts[i + 1]
    return y0 + (y1 - y0) * (x - x0) / (x1 - x0) if x1 != x0 else y1


def curve(steps: list) -> dict:
    steps = sorted(steps, key=lambda s: s["tenants"])
    ns = [s["tenants"] for s in steps]
    out = {"class": sorted({s["class"] for s in steps}), "steps": [], "fit": {}, "binds": {}}
    for s in steps:
        out["steps"].append({
            "point": s["point"], "tenants": s["tenants"], "valid": s["validity"]["valid"],
            **{f"{c}_instance": s["cases"][c]["instance_month"] for c in CASES},
            **{f"{c}_per_tenant": s["cases"][c]["per_tenant_month"] for c in CASES},
            "k2_bill": s["k2"]["usd"], "ceiling": s["k2"]["ceiling_usd"],
            "over_ceiling": s["cases"]["owner"]["per_tenant_over_ceiling"],
            **{f"use_{n}": v["use"] for n, v in s["limits"].items()},
        })
    for c in CASES:
        f = line(ns, [s["cases"][c]["instance_month"] for s in steps])
        if f:
            out["fit"][c] = {"fixed_month": f[0], "per_added_tenant_month": f[1]}
    # The N at which each limit's use reaches 1, along a line through the steps.
    for name in steps[0]["limits"]:
        pts = [(s["tenants"], s["limits"][name]["use"]) for s in steps if s["limits"][name]["use"] is not None]
        f = line([p[0] for p in pts], [p[1] for p in pts]) if pts else None
        out["binds"][name] = (1 - f[0]) / f[1] if f and f[1] > 0 else None
    binding = [(n, v) for n, v in out["binds"].items() if v is not None]
    if binding:
        name, nmax = min(binding, key=lambda kv: kv[1])
        nmax = max(1.0, nmax)
        out["n_max"] = {"limit": name, "tenants": nmax}
        out["at_n_max"] = {}
        for c in CASES:
            inst = at(ns, [s["cases"][c]["instance_month"] for s in steps], nmax)
            out["at_n_max"][c] = {"instance_month": inst, "per_tenant_month": inst / nmax}
    return out


def print_curve(cv: dict) -> None:
    print(f"== packing curve, class {','.join(cv['class'])}")
    print(f"   {'point':<22}{'N':>6}{'$/inst':>11}{'$/tenant':>11}{'-50% A/B':>11}{'K2 bill':>11}{'x ceil':>9}"
          f"{'parked':>8}{'vCPU':>7}{'RSS':>7}{'valid':>7}")
    for s in cv["steps"]:
        print(f"   {s['point'] or '?':<22}{s['tenants']:>6}{fmt(s['owner_instance']):>11}{fmt(s['owner_per_tenant'], 4):>11}"
              f"{fmt(s['ab50_per_tenant'], 4):>11}{fmt(s['k2_bill'], 4):>11}{fmt(s['over_ceiling'], 0):>9}"
              f"{fmt(s['use_parked_waits']):>8}{fmt(s['use_vcpu']):>7}{fmt(s['use_rss_mb']):>7}{str(s['valid']):>7}")
    for c, f in cv["fit"].items():
        print(f"   fit {c}: instance month = {fmt(f['fixed_month'])} + {fmt(f['per_added_tenant_month'], 4)} x N")
    print("   N where each limit binds (a line through the steps): "
          + ", ".join(f"{n} {fmt(v, 0)}" for n, v in cv["binds"].items()))
    if cv.get("n_max"):
        print(f"   first to bind: {cv['n_max']['limit']} at N = {cv['n_max']['tenants']:,.0f}; per tenant there: "
              + ", ".join(f"{c} {fmt(v['per_tenant_month'], 4)}" for c, v in cv["at_n_max"].items()))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("price")
    p.add_argument("run")
    p.add_argument("--window", default=DEFAULT_WINDOW)
    p.add_argument("--cpu-factor", type=float, default=1.0)
    q = sub.add_parser("curve")
    q.add_argument("runs", nargs="+")
    q.add_argument("--window", default=DEFAULT_WINDOW)
    q.add_argument("--cpu-factor", type=float, default=1.0)
    q.add_argument("--out")
    args = ap.parse_args()
    if args.cmd == "price":
        s = price(args.run, args.window, args.cpu_factor)
        with open(os.path.join(s["run"], "packing.json"), "w", encoding="utf-8") as f:
            json.dump(s, f, indent=1, sort_keys=True)
        print_step(s)
        return 0
    cv = curve([price(r, args.window, args.cpu_factor) for r in args.runs])
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(cv, f, indent=1, sort_keys=True)
    print_curve(cv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
