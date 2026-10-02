"""K2 cost experiment: the §8 validity checks for one point
(target/k2-design.md §6, §8).

Counter checks run over the whole run [start, end], which contains the
load window and the deferred tail; the absorb-backlog trend runs over the
load window only (the tail drains by design). Counters are summed per boot,
so a restart phase is never read as a decrease. A point that fails any
check is void.
"""

from __future__ import annotations

from collections import defaultdict

RSS_MAX_MB = 450
RSS_SLOPE_MAX = 2.0  # MB/min over the second half
TREND_MIN_SECS = 300
SHED_KEYS = ("admit_shed", "admit_shed_inflight", "admit_shed_rss", "admit_shed_survival",
             "stream_shed", "wedge_shed")


def find_key(obj, key):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key:
                yield v
            yield from find_key(v, key)
    elif isinstance(obj, list):
        for v in obj:
            yield from find_key(v, key)


def boot_delta(pts: list, get) -> int:
    """A process counter's growth over the samples, summed per boot: a new
    boot_id (a restart phase) counts from zero, never as a decrease."""
    total = 0
    for (_, a), (_, b) in zip(pts, pts[1:]):
        vb = int(get(b) or 0)
        total += vb - int(get(a) or 0) if a.get("boot_id") == b.get("boot_id") else vb
    return total


def bracketed(samples: list, t0: int, t1: int) -> list:
    """The samples inside [t0, t1] plus the last one before t0 and the first
    one after t1: a window shorter than the scrape interval still has two
    samples, and counter deltas over the wider span must be 0 all the same."""
    samples = sorted(samples, key=lambda p: p[0])
    before = [p for p in samples if p[0] < t0][-1:]
    after = [p for p in samples if p[0] > t1][:1]
    return before + [p for p in samples if t0 <= p[0] <= t1] + after


def bp_sum(b: dict, key: str) -> int:
    return sum(int(v) for v in find_key(b.get("maintenance_backpressure"), key) if isinstance(v, (int, float)))


def server_checks(check, srv: str, pts: list, restarts: bool) -> None:
    last = pts[-1][1]
    rl = {k: boot_delta(pts, lambda b, k=k: (b.get("rate_limit_refusals") or {}).get(k))
          for k in (last.get("rate_limit_refusals") or {})}
    check(f"{srv}: rate-limit refusals 0", sum(rl.values()) == 0, rl)
    shed = {k: boot_delta(pts, lambda b, k=k: b.get(k)) for k in SHED_KEYS if isinstance(last.get(k), (int, float))}
    shed["appends_shed (maintenance backpressure)"] = boot_delta(pts, lambda b: bp_sum(b, "appends_shed"))
    shed["project_memory_shed_total"] = boot_delta(
        pts, lambda b: (b.get("project_memory") or {}).get("project_memory_shed_total"))
    check(f"{srv}: shed counters 0", sum(shed.values()) == 0, shed)
    engages = boot_delta(pts, lambda b: bp_sum(b, "engage_count"))
    seen = any(v is True for _, b in pts for key in ("engaged", "instance_engaged")
               for v in find_key(b.get("maintenance_backpressure"), key))
    check(f"{srv}: maintenance backpressure never engaged", engages == 0 and not seen,
          {"engage_count_delta": engages, "engaged_at_a_sample": seen})
    splits = boot_delta(pts, lambda b: (b.get("scaler") or {}).get("segment_splits"))
    check(f"{srv}: no scaler splits", splits == 0, splits)
    lag = max(float(b.get("absorb_lag_max_secs") or 0) for _, b in pts)
    check(f"{srv}: absorb_lag_max_secs <= 90", lag <= 90, lag)
    rss = [(t, float(b.get("rss_mb") or 0)) for t, b in pts]
    half = rss[len(rss) // 2:]
    span = (half[-1][0] - half[0][0]) / 1000 if len(half) > 1 else 0
    slope = (half[-1][1] - half[0][1]) / (span / 60) if span > 0 else 0.0
    peak = max(r for _, r in rss)
    detail = {"max": round(peak, 1), "slope_mb_per_min_2nd_half": round(slope, 2), "second_half_secs": round(span)}
    check(f"{srv}: rss_mb <= {RSS_MAX_MB} (macOS RSS, indicative)", peak <= RSS_MAX_MB, detail)
    if span >= TREND_MIN_SECS:
        check(f"{srv}: rss slope < {RSS_SLOPE_MAX} MB/min over the second half", slope < RSS_SLOPE_MAX, detail)
    else:
        check(f"{srv}: rss slope < {RSS_SLOPE_MAX} MB/min over the second half", True,
              {**detail, "judged": False, "reason": f"second half {span:.0f} s < {TREND_MIN_SECS} s"})
    boots = sorted({b.get("boot_id") for _, b in pts})
    check(f"{srv}: boot_id unchanged", len(boots) == 1 or restarts, {"boots": len(boots), "restart_phase": restarts})


def backlog_checks(check, usages: dict) -> None:
    for srv, pts in sorted(usages.items()):
        bl = [int((b.get("absorb_backlog") or {}).get("streams") or 0) for _, b in pts]
        secs = (pts[-1][0] - pts[0][0]) / 1000
        if secs < TREND_MIN_SECS:
            # ABSORB_AGE_SECS=60 holds records back a minute by design: a
            # trend needs several absorption cycles to show.
            check(f"{srv}: absorb backlog not trending up (load window)", True,
                  {"judged": False, "reason": f"load window {secs:.0f} s < {TREND_MIN_SECS} s", "max": max(bl)})
            continue
        third = max(1, len(bl) // 3)
        mid = sum(bl[third:2 * third]) / max(1, len(bl[third:2 * third]))
        last = sum(bl[-third:]) / third
        check(f"{srv}: absorb backlog not trending up (load window)", last <= mid + 1,
              {"middle_third_mean": round(mid, 2), "last_third_mean": round(last, 2), "max": max(bl)})


def generator_checks(check, ledgers: dict, modes: dict) -> None:
    """Offered load reached the server: every sent append was acked, every
    churn lifecycle completed, pacing held."""
    lost = {}
    for i, lg in sorted(ledgers.items()):
        if lg.get("sent_requests") is not None:
            d = int(lg.get("sent_requests") or 0) - int(lg.get("acked_requests") or 0)
            if d:
                lost[f"phase {i} ({modes.get(i)})"] = {"sent": lg.get("sent_requests"), "acked": lg.get("acked_requests"),
                                                       "failed": lg.get("failed_requests", 0)}
        ch = lg.get("churn") or {}
        if ch and (int(ch.get("failed") or 0) or int(ch.get("completed") or 0) != int(ch.get("count") or 0)):
            lost[f"phase {i} (churn)"] = {"count": ch.get("count"), "completed": ch.get("completed"), "failed": ch.get("failed")}
    check("generator: every sent append acked, every lifecycle completed", not lost, lost)
    bad = defaultdict(int)
    for lg in ledgers.values():
        prod = lg.get("producer") or {}
        for k in ("late_sends", "unsent"):
            bad[k] += int(prod.get(k) or 0)
    check("generator: open-loop pacing held (no late > 100 ms or unsent sends)", not any(bad.values()), dict(bad))


def meter_checks(check, usage: dict, eff: dict, ledgers: dict) -> None:
    proj = (usage or {}).get("project")
    check("project usage meter read and settled", bool(proj) and bool(usage.get("settled")),
          {"status": (usage or {}).get("status"), "settled": (usage or {}).get("settled"), "present": bool(proj)})
    acked = sum(int(lg.get("acked_records") or 0) for lg in ledgers.values())
    meter = eff.get("ingestRecords") if proj else None
    check("acked records == meter ingestRecords", meter is not None and acked == int(meter),
          {"acked": acked, "meter": meter})
    d_rec = sum(int(lg.get("delivered_records") or 0) for lg in ledgers.values())
    d_bytes = sum(int(lg.get("delivered_payload_bytes") or 0) for lg in ledgers.values())
    estimated = any(lg.get("delivered_records_estimated") or (lg.get("walk") or {}).get("delivered_records_estimated")
                    or (lg.get("tail") or {}).get("delivered_records_estimated") for lg in ledgers.values())
    m_rec, m_bytes = (eff.get("readRecords"), eff.get("readPayloadBytes")) if proj else (None, None)
    if d_bytes or (m_bytes and int(m_bytes)):
        ok = m_bytes is not None and int(m_bytes) == d_bytes and (estimated or int(m_rec or 0) == d_rec)
        check("delivered records and payload bytes == meter readRecords/readPayloadBytes", ok,
              {"delivered_records": d_rec, "meter_records": m_rec, "delivered_bytes": d_bytes,
               "meter_bytes": m_bytes, "records_estimated": estimated})


def validity(scrape: list, spans: dict, restarts: bool, usage: dict, eff: dict, ledgers: dict,
             modes: dict, servers: list, posture: dict, marks: list, quiesce_point: bool) -> dict:
    """spans: {"run": (t_start, t_end), "load": (t_start, t_loadend)}."""
    loads, usages = defaultdict(list), defaultdict(list)
    for rec in scrape:
        if rec.get("body"):
            if rec.get("src") == "load":
                loads[rec["server"]].append((rec["t"], rec["body"]))
            elif rec.get("src") == "usage":
                usages[rec["server"]].append((rec["t"], rec["body"]))
    checks = []

    def check(name, ok, detail):
        checks.append({"check": name, "ok": bool(ok), "detail": detail})

    t0, t1 = spans["run"]
    run_loads = {k: bracketed(v, t0, t1) for k, v in loads.items()}
    for srv in servers:
        n = len(run_loads.get(srv, []))
        check(f"{srv}: /v1/debug/load sampled around the run", n >= 2, {"samples": n})
    for srv, pts in sorted(run_loads.items()):
        if pts:
            server_checks(check, srv, pts, restarts)
    lt0, lt1 = spans["load"]
    backlog_checks(check, {k: bracketed(v, lt0, lt1) for k, v in usages.items() if v})
    meter_checks(check, usage, eff, ledgers)
    generator_checks(check, ledgers, modes)
    timeouts = [m for m in marks if m.get("mark") == "quiesce_timeout"]
    if quiesce_point or timeouts:
        check("quiescence reached (no quiesce_timeout)", not timeouts,
              [m.get("id", "load_end") for m in timeouts])
    check("server binaries built from the worktree HEAD (posture)", posture.get("binary_commit_matches_head") is True,
          {"head": (posture.get("git") or {}).get("head"),
           "servers": {k: v.get("git_commit") for k, v in (posture.get("servers") or {}).items()}})
    return {"valid": all(c["ok"] for c in checks), "checks": checks, "spans_ms": spans}
