"""K2 cost experiment: stored bytes and the retained dimension
(target/k2-design.md §4.1 "Storage S_phys(t)", §1 retained revenue).

Physical bytes come from s3lite's live_bytes census (C2), sampled every
10 s. The customer tiers are shard and hist; fleet, registry, telemetry
and other are the floor's storage. The gauge (owned frame bytes, what K2's
retained price applies to) comes from two places:

  - at the end: the sum of ownedStoredBytesNow over the live customer
    streams (GET /v1/streams/{name}/usage/current, read after the end
    snapshot, so exact when nothing is appended after it);
  - over time: /v1/debug/usage's per-stream frame_bytes for the customer
    streams (scrape.py records them as frame_bytes_by_stream, keyed by the
    stream's route hash: SHA-256 prefix of the length-prefixed route-v1,
    project id and name, src/tenant.rs route_hash_input), stitched across
    restarts. It is the cumulative frame bytes since boot, which equals
    the gauge while nothing is trimmed, expired or deleted.

k(t) = physical customer bytes(t) / gauge(t), both at the same moment.
The fit S_phys(t) = c + a*gauge(t) + b*ingest_rate(t) (§4.1) gives the
steady amplification a, the transient bytes per frame byte/s of ingest b
and the fixed per-DB overhead c.
"""

from __future__ import annotations

import hashlib
import struct
from collections import defaultdict

from k2cells import CUSTOMER_TIERS, GIB, MONTH_SECS, STORAGE_USD


def live_total(lb):
    """Total live bytes from s3lite's live_bytes member, tolerant of shape."""
    if isinstance(lb, (int, float)):
        return int(lb)
    if not isinstance(lb, dict) or lb.get("poisoned"):
        return None
    t = lb.get("total")
    if isinstance(t, dict) and isinstance(t.get("bytes"), (int, float)):
        return int(t["bytes"])
    if isinstance(t, (int, float)):
        return int(t)
    return None


def by_tier(lb) -> dict:
    """{tier: bytes} from live_bytes' "tier/kind" cells."""
    out = defaultdict(int)
    cells = (lb.get("cells") or {}) if isinstance(lb, dict) else {}
    for key, v in cells.items():
        if isinstance(v, dict) and isinstance(v.get("bytes"), (int, float)):
            out[key.split("/", 1)[0]] += int(v["bytes"])
    return dict(out)


def customer_bytes(lb):
    t = by_tier(lb)
    return sum(v for k, v in t.items() if k in CUSTOMER_TIERS) if t else None


def samples(scrape: list, snaps: dict, t0: int, t1: int) -> list:
    """[(t, seq, live_bytes)] in [t0, t1]: the scrape series plus the snapshots."""
    pts = []
    for rec in scrape:
        if rec.get("src") == "s3lite" and rec.get("body") and t0 <= rec["t"] <= t1:
            lb = rec["body"].get("live_bytes")
            if live_total(lb) is not None:
                pts.append((rec["t"], rec.get("seq"), lb))
    for s in snaps.values():
        if s and live_total(s.get("live_bytes")) is not None and t0 <= s["t_ms"] <= t1:
            pts.append((s["t_ms"], None, s["live_bytes"]))
    return sorted(pts, key=lambda p: p[0])


def thin(rows: list, max_points: int = 120, key: str = "bytes") -> list:
    """Evenly thinned; the first, last and peak rows are always kept."""
    if len(rows) <= max_points:
        return rows
    peak = max(range(len(rows)), key=lambda i: rows[i][key])
    step = (len(rows) - 1) / (max_points - 1)
    keep = {round(k * step) for k in range(max_points)} | {peak}
    return [rows[i] for i in sorted(keep)]


def route_hash(project: str, name: str) -> str:
    """The usage counters' key (RouteHash::for_stream), as debug/usage prints it."""
    raw = b"".join(struct.pack(">I", len(c)) + c for c in (b"route-v1", project.encode(), name.encode()))
    return hashlib.sha256(raw).hexdigest()[:32]


def gauge_series(scrape: list, ids: set) -> tuple:
    """{seq: (t, customer frame bytes)} from debug/usage, stitched per
    (server, stream) across restarts (a decrease is a new boot counting from
    0). ids empty or no per-stream member: every tracked stream (system
    streams included), flagged in the basis."""
    last, offset, per_seq = {}, defaultdict(int), defaultdict(lambda: [0, 0])
    basis = "customer_streams"
    for rec in scrape:
        if rec.get("src") != "usage" or not rec.get("body"):
            continue
        body, srv = rec["body"], rec.get("server")
        per = body.get("frame_bytes_by_stream")
        if not isinstance(per, dict) or not ids:
            per = {"*": int((body.get("listed_sums") or {}).get("frame_bytes") or 0)}
            basis = "all_tracked_streams"
        total = 0
        for sid, v in per.items():
            if sid != "*" and sid not in ids:
                continue
            k = (srv, sid)
            if k in last and int(v) < last[k]:
                offset[k] += last[k]
            last[k] = int(v)
            total += offset[k] + int(v)
        # A stream not listed this time (not yet tracked) keeps its stitched value.
        listed = {(srv, s) for s in per}
        total += sum(offset[k] + last[k] for k in last if k[0] == srv and k not in listed
                     and (k[1] in ids or k[1] == "*"))
        acc = per_seq[rec.get("seq")]
        acc[0] = max(acc[0], rec["t"])
        acc[1] += total
    return {s: (v[0], v[1]) for s, v in per_seq.items()}, basis


def fit(rows: list) -> dict:
    """OLS of phys = c + a*gauge + b*rate over rows [(phys, gauge, rate)]."""
    n = len(rows)
    if n < 8 or len({round(g) for _, g, _ in rows}) < 3:
        return {"ok": False, "n": n, "reason": "fewer than 8 samples or a flat gauge"}
    xs = [(1.0, float(g), float(r)) for _, g, r in rows]
    ys = [float(p) for p, _, _ in rows]
    m = [[sum(x[i] * x[j] for x in xs) for j in range(3)] + [sum(x[i] * y for x, y in zip(xs, ys))]
         for i in range(3)]
    scale = max(abs(m[i][i]) for i in range(3)) or 1.0
    for col in range(3):  # Gaussian elimination with partial pivoting
        piv = max(range(col, 3), key=lambda r: abs(m[r][col]))
        if abs(m[piv][col]) < 1e-12 * scale:
            return {"ok": False, "n": n, "reason": "singular: gauge and ingest rate not separable"}
        m[col], m[piv] = m[piv], m[col]
        for r in range(3):
            if r != col:
                f = m[r][col] / m[col][col]
                m[r] = [a - f * b for a, b in zip(m[r], m[col])]
    c, a, b = (m[i][3] / m[i][i] for i in range(3))
    mean = sum(ys) / n
    ss_tot = sum((y - mean) ** 2 for y in ys)
    ss_res = sum((y - (c + a * g + b * r)) ** 2 for y, (_, g, r) in zip(ys, xs))
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else None
    return {"ok": a > 0, "n": n, "a": round(a, 4), "b_secs": round(b, 2), "c_bytes": round(c),
            "r2": round(r2, 4) if r2 is not None else None,
            "usd_per_gib_month_steady": round(STORAGE_USD * a, 6) if a > 0 else None,
            "reason": None if a > 0 else "a <= 0"}


def storage(scrape: list, snaps: dict, t0: int, t1: int, usage: dict, gauge_end: int,
            all_deleted: bool, ingest: int, byte_s: int, project: str | None = None) -> dict:
    pts = samples(scrape, snaps, t0, t1)
    if not pts:
        return {"available": False,
                "note": "s3lite stats2 has no live_bytes member (C2 absent in this s3lite build): "
                        "physical stored bytes and storage cost are not computed"}
    days = defaultdict(int)
    for t, _, lb in pts:
        days[int(t // 86_400_000)] = max(days[int(t // 86_400_000)], live_total(lb))
    avg_daily_peak = sum(days.values()) / len(days)
    tot = [(t, live_total(lb)) for t, _, lb in pts]
    area = sum((b[0] - a[0]) / 1000 * (a[1] + b[1]) / 2 for a, b in zip(tot, tot[1:]))
    run_secs = (t1 - t0) / 1000
    cust = [(t, s, customer_bytes(lb)) for t, s, lb in pts]
    cust_peak = max(cust, key=lambda p: p[2] or 0)
    floor_peak = max((live_total(lb) - (customer_bytes(lb) or 0)) for _, _, lb in pts)
    end_lb, start_lb = (snaps.get("end") or {}).get("live_bytes"), (snaps.get("start") or {}).get("live_bytes")
    out = {
        "available": True, "samples": len(pts), "window_secs": run_secs,
        "peak_bytes": max(v for _, v in tot), "avg_daily_peak_bytes": avg_daily_peak,
        "time_avg_bytes": area / run_secs if run_secs > 0 else tot[-1][1],
        "start_bytes": tot[0][1], "end_bytes": tot[-1][1],
        "customer_peak_bytes": cust_peak[2], "customer_end_bytes": customer_bytes(end_lb),
        "customer_start_bytes": customer_bytes(start_lb), "floor_tiers_peak_bytes": floor_peak,
        "end_by_tier_kind": end_lb if isinstance(end_lb, dict) else None,
        # Tigris bills the average of daily peaks; a sub-day run is one day.
        "usd_run": STORAGE_USD * avg_daily_peak / GIB * run_secs / MONTH_SECS,
        "usd_run_floor_tiers": STORAGE_USD * floor_peak / GIB * run_secs / MONTH_SECS,
    }
    # The gauge over time, aligned with the census by scrape round.
    project = project or (usage or {}).get("project_id")
    names = ((usage or {}).get("streams_current") or {}).get("streams", {})
    ids = {route_hash(project, n) for n in names} if project else set()
    gs, basis = gauge_series(scrape, ids)
    rows, prev = [], None
    for t, seq, lb in pts:
        if seq is None or seq not in gs:
            continue
        g = gs[seq][1]
        rate = (g - prev[1]) / ((t - prev[0]) / 1000) if prev and t > prev[0] else 0.0
        prev = (t, g)
        rows.append({"t_ms": t, "t_rel_s": round((t - t0) / 1000, 1), "bytes": live_total(lb),
                     "customer_bytes": customer_bytes(lb), "gauge": g, "ingest_frame_bps": round(max(rate, 0.0)),
                     "by_tier": by_tier(lb)})
    out["series"] = thin(rows) if rows else thin([{"t_ms": t, "t_rel_s": round((t - t0) / 1000, 1),
                                                   "bytes": live_total(lb), "by_tier": by_tier(lb)}
                                                  for t, _, lb in pts])
    out["gauge_basis"] = basis if rows else None
    out["gauge_end_owned_frame_bytes"] = gauge_end
    if rows and gauge_end:
        out["gauge_series_end_over_owned"] = round(rows[-1]["gauge"] / gauge_end, 4)
    # k(t) on one basis: customer physical over the gauge at the same moment.
    if gauge_end and out["customer_end_bytes"] is not None:
        out["k_end"] = round(out["customer_end_bytes"] / gauge_end, 4)
    live = [r for r in rows if r["gauge"] > 0 and r["customer_bytes"]]
    if gauge_end and out["customer_end_bytes"]:
        # The end snapshot, with the exact gauge (ownedStoredBytesNow).
        live.append({"customer_bytes": out["customer_end_bytes"], "gauge": gauge_end, "t_rel_s": round(run_secs, 1)})
    if live:
        pk = max(live, key=lambda r: r["customer_bytes"])
        out["k_peak"] = round(pk["customer_bytes"] / pk["gauge"], 4)
        out["k_peak_at_s"] = pk["t_rel_s"]
    out["fit"] = fit([(r["customer_bytes"], r["gauge"], r["ingest_frame_bps"]) for r in rows if r["customer_bytes"]])
    out["fit"]["model"] = "customer physical = c + a*gauge + b*ingest frame bytes/s (10 s series)"
    if all_deleted:
        # L7: every customer stream was deleted or expired. The retained
        # price has no meter to apply to; U(t) is what stays behind.
        u_bytes = (out["customer_end_bytes"] or 0) - (out["customer_start_bytes"] or 0)
        out["unreclaimed"] = {
            "bytes": u_bytes,
            "bytes_per_gib_deleted": round(u_bytes / (ingest / GIB)) if ingest else None,
            "usd_per_month_per_gib_ingested": STORAGE_USD * u_bytes / ingest if ingest else None,
            "note": "customer-tier bytes (shard+hist) at end minus start, after every customer stream "
                    "was deleted or expired; includes the system streams' own growth over the run",
        }
        out["retained_suppressed"] = "every customer stream was deleted or expired: no retained meter to price against"
        return out
    k = out.get("k_peak") or out.get("k_end")
    if k is not None:
        out["retained_basis"] = "k_peak (customer daily peak / gauge at that moment)" if out.get("k_peak") \
            else "k_end (customer bytes / owned frame bytes at the end)"
        out["retained_usd_per_gib_month"] = STORAGE_USD * k
    if byte_s and run_secs:
        # The old figure: run peak (all tiers) over the meter's run average.
        # The meter average includes the ramp from zero, so it is biased up.
        meter_avg = byte_s / run_secs
        out["meter_avg_stored_bytes"] = meter_avg
        out["retained_usd_per_gib_month_ramp_inclusive"] = out["usd_run"] / (byte_s / GIB / MONTH_SECS)
        out["k_peak_over_meter_avg_ramp_inclusive"] = avg_daily_peak / meter_avg if meter_avg else None
    return out
