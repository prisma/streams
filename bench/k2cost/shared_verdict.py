#!/usr/bin/env python3
"""The Q5 arms' verdicts and the arm manifest the single-cell RC gate reads
(deploy-q5 design §3.3, §3.7; triage B7; docs/OPS-RELEASE.md §7 on slate).

    python3 bench/k2cost/shared_verdict.py layer-b RUN_DIR [--out FILE]
        [--repo DIR] [--claimed-streams N]

RUN_DIR is a run of bench/k2cost/run-shared.sh: rig.json (identity,
profiles, cell.env, window), marks.jsonl, scrape.jsonl (scrape.py loop:
/v1/debug/load every 10 s) and tenants/ (tenants.ts run, usage, audit and
its legs). Every bar is a named constant below with its source; a value
the bar needs and the run lacks fails the bar, never passes it.

The manifest (default RUN_DIR/<arm>-manifest.json) carries what
scripts/rc-certify.sh's verify_arm reads: arm, server_sha256, verdict,
legs (PASS|FAIL each), started_unix, finished_unix, projects; the layer-b
legs include the three the lead's call 7 names (H9, H9b, H11t). Beside them,
the fence fields verify_arm will check (triage B9): commit, profile {files,
sha256, pins, pins_missing}, cell_env_sha256, harness_trees, region; and
bars {name: {value, bar, pass}}, known_red, evidence {file: sha256}.

Known-red legs are judged like any other and reported FAIL: Q0 until its
fix; known_red only names why. H11t times quiet names against never-held
ones (the owner's round-3 call 9, (a)): see leg_h11t. A
rehearsal run (rig.json rehearsal: true: run-shared.sh --binary, any
binary) writes its manifest with verdict FAIL and leg "release-binary"
FAIL: it can never certify.

Bars of the owner's decision D4 (deploy-q5 Q4), (a), of 2026-10-12 (the
restart window's measure confirmed on 2026-10-13); each is one constant:
SC01S_PROJECTION_DAYS_MIN, DAY90_OPEN_SECS_MAX, IDLE_RSS_MB_MAX and the
restart window, which is
bench/lifecycle/verdicts.py's RESTART_WINDOW_BAR_S (one copy). The field
arms that read them (envelope, sc0-1s, sc1, stream-count, battery) take
their evidence from the field adapters (triage B8); until those exist this
tool refuses those arms rather than write a manifest without evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import k2valid  # noqa: E402  (bench/k2cost: RSS slope bar, shed keys, per-boot counter deltas)


def _lifecycle_verdicts():
    spec = importlib.util.spec_from_file_location("lifecycle_verdicts", os.path.join(HERE, "..", "lifecycle", "verdicts.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---- bars ---------------------------------------------------------------------
# Layer B (design §3.3; shared-cells PLAN §4.1 "Layer B" pass table).
LEAKS_MAX = 0                    # foreign, bad-MAC and unmarked records, audit mismatches
PROBE_PAIRS_MIN = 2000           # cross-project probes, each a first contact (prober, name)
H11T_PAIRS_MIN = 2000            # timed pairs on the capability path, quiet vs never (round-3 call 9, (a))
H11T_KS_P_MIN = 0.01             # quiet and never indistinguishable: KS p > 0.01
VICTIM_P99_FACTOR = 2.0          # victim p99 under noise <= max(2 x solo p99, 25 ms)
VICTIM_P99_FLOOR_MS = 25.0
# The victim's latency is measured in the one process that drives every
# project: a noisy period whose driver event-loop lag p99 (tenants.ts
# LoopLag) is at least this share of the victim's p99 is flagged
# harness_bound, so a FAIL there may be the harness, not the cell.
LOOP_LAG_MATERIAL_SHARE = 0.25
HOSTILE_UNTYPED_MAX_FRAC = 0.005  # noisy untyped error answers < 0.5% (validation.md §4.2 T4)
RSS_PEAK_MAX_MB = 500.0          # Layer B: RSS peak < 500 MB (the shed line)
RSS_SLOPE_MAX = k2valid.RSS_SLOPE_MAX  # 2 MB/min ...
SLOPE_WINDOW_SECS = 1800         # ... over the last 30 min of the load
Q0_DELTA_MB = 20.0               # README Q0: back within baseline + 20 MB ...
Q0_WITHIN_SECS = 600             # ... within 10 minutes of the load's end
# Compliant tenants: 0 refusals, every typed code counted (the design's bar,
# deploy-q5 §3.3, accepted by the owner as D4 (a)). 503
# temporarily_unavailable is the product's generic "could not serve" answer
# (store errors, unavailable appends, failed read resolves), not a boot-only
# holdoff, so no code is excused; an allowance would be the owner's call.
# Owner decision D4 (deploy-q5 Q4), (a), 2026-10-12:
SC01S_PROJECTION_DAYS_MIN = 30.0  # SC0-1s-awake: hours 24-72 RSS slope projects >= 30 days to the line
SC01S_LINE_MB = 500.0
DAY90_OPEN_SECS_MAX = 10.0        # shard open latency extrapolated to day 90
IDLE_RSS_MB_MAX = 400.0           # idle RSS at the claimed stream count
RESTART_WINDOW_SECS_MAX = _lifecycle_verdicts().RESTART_WINDOW_BAR_S  # SC1-awake: 10 s to the first SDK success

# The load Layer B certifies: the commit's own manifest, at full time.
LOAD_MANIFEST = "bench/k2cost/tenants/layer-b.json"
LOAD_TIME_SCALE = 1

# The single-cell profile pins (triage B9's fence) and the harness trees.
PROFILE_PINS = ("INITIAL_SHARDS=1", "KEEP_AWAKE=1", "STREAMS_RELEASE_POSTURE=1", "PROJECT_SHARE_K=8")
HARNESS_TREES = ("bench/k2cost", "deploy/composer", "deploy/app-server")
# What runs from the rig's checkout (run-shared.sh's CHECKOUT_TREES): the
# harness leg requires these to be the server commit's trees, clean.
CHECKOUT_TREES = ("bench/k2cost", "deploy/cell-admin", "bench/docker")
# The legs rc-certify.sh's verify_arm requires (db518611), and those known red.
REQUIRED_LEGS = {"layer-b": ("H9", "H9b", "H11t")}
KNOWN_RED = {"Q0": "Q0 (RSS after a load stays above the baseline; README Q0)"}


# ---- pure bar functions ----------------------------------------------------------

def ks(a: list, b: list) -> tuple:
    """Two-sample Kolmogorov-Smirnov D and asymptotic p (Numerical
    Recipes' probks with Stephens' correction; hostile.ts's twin)."""
    x, y = sorted(a), sorted(b)
    if not x or not y:
        return (float("nan"), 0.0)
    i = j = 0
    d = 0.0
    while i < len(x) and j < len(y):
        v = min(x[i], y[j])
        while i < len(x) and x[i] <= v:
            i += 1
        while j < len(y) and y[j] <= v:
            j += 1
        d = max(d, abs(i / len(x) - j / len(y)))
    en = math.sqrt(len(x) * len(y) / (len(x) + len(y)))
    lam = (en + 0.12 + 0.11 / en) * d
    p, sign, prev = 0.0, 1.0, 0.0
    for k in range(1, 101):
        term = sign * 2 * math.exp(-2 * k * k * lam * lam)
        p += term
        if abs(term) <= 0.001 * prev or abs(term) <= 1e-8 * p:
            return (d, min(1.0, max(0.0, p)))
        sign, prev = -sign, abs(term)
    return (d, 1.0)


def fit(points: list) -> tuple:
    """Least squares (slope, intercept) of [(x, y)]; None with < 2 x values."""
    if len({x for x, _ in points}) < 2:
        return None
    n = len(points)
    mx = sum(x for x, _ in points) / n
    my = sum(y for _, y in points) / n
    sxx = sum((x - mx) ** 2 for x, _ in points)
    slope = sum((x - mx) * (y - my) for x, y in points) / sxx
    return (slope, my - slope * mx)


def days_to_line(rss: list, line_mb: float = SC01S_LINE_MB, from_h: float = 24, to_h: float = 72) -> float | None:
    """SC0-1s-awake: [(hours since start, rss MB)]; the hours from_h-to_h
    slope projected from the fit's value at to_h to the line, in days
    (infinite when RSS does not grow)."""
    pts = [(h, r) for h, r in rss if from_h <= h <= to_h]
    f = fit(pts)
    if f is None:
        return None
    slope, icpt = f
    at = slope * to_h + icpt
    if at >= line_mb:
        return 0.0
    return math.inf if slope <= 0 else (line_mb - at) / slope / 24


def day90_open(opens: list) -> float | None:
    """[(day, open latency s)] fitted and read at day 90."""
    f = fit(opens)
    return None if f is None else f[0] * 90 + f[1]


def bar(bars: dict, name: str, value, limit, ok: bool) -> bool:
    bars[name] = {"value": value, "bar": limit, "pass": bool(ok)}
    return bool(ok)


# ---- evidence ---------------------------------------------------------------

def read_json(path: str, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def read_lines(path: str) -> list:
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
    except OSError:
        pass
    return out


def sha256_file(path: str) -> str | None:
    try:
        with open(path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()
    except OSError:
        return None


class Run:
    def __init__(self, d: str):
        self.dir = d
        self.rig = read_json(os.path.join(d, "rig.json"), {}) or {}
        self.marks = {m.get("mark"): int(m["t"]) for m in read_lines(os.path.join(d, "marks.jsonl")) if "t" in m}
        self.loads = sorted(((int(r["t"]), r["body"]) for r in read_lines(os.path.join(d, "scrape.jsonl"))
                             if r.get("src") == "load" and r.get("body")), key=lambda p: p[0])
        t = os.path.join(d, "tenants")
        self.tenants = t
        self.run = read_json(os.path.join(t, "run.json"), {}) or {}
        self.classes = read_json(os.path.join(t, "classes.json"), {}) or {}
        self.usage = (read_json(os.path.join(t, "usage.json"), {}) or {}).get("projects", {})
        self.audit = (read_json(os.path.join(t, "audit.json"), {}) or {}).get("projects", {})
        self.ledgers = {}
        pdir = os.path.join(t, "projects")
        for p in sorted(os.listdir(pdir)) if os.path.isdir(pdir) else []:
            lg = read_json(os.path.join(pdir, p, "ledger.json"))
            if lg:
                self.ledgers[p] = lg
        self.legs = {}
        ldir = os.path.join(t, "legs")
        for f in sorted(os.listdir(ldir)) if os.path.isdir(ldir) else []:
            lg = read_json(os.path.join(ldir, f))
            if lg:
                self.legs.setdefault(lg.get("leg"), lg)

    def phases(self, mode: str) -> list:
        """(project, phase ledger) of every phase in that mode."""
        out = []
        for p, lg in self.ledgers.items():
            for ph in lg.get("phases", []):
                if ph.get("mode") == mode:
                    full = read_json(os.path.join(self.tenants, "projects", p, f"phase-{ph['i']}.ledger.json"), {}) or {}
                    out.append((p, full))
        return out

    def window_loads(self, t0: int, t1: int) -> list:
        return k2valid.bracketed(self.loads, t0, t1)


# ---- layer-b legs ---------------------------------------------------------------

def leg_leaks(r: Run, bars: dict) -> bool:
    oracle = {k: sum(int(lg["totals"].get(f"oracle_{k}") or 0) for lg in r.ledgers.values()) for k in ("foreign", "bad_mac", "unmarked")}
    audit_bad = sorted(p for p, a in r.audit.items() if any(not c.get("ok") for c in a.get("prefixes", {}).values()))
    audit_oracle = sum(int((c.get("oracle") or {}).get(k) or 0) for a in r.audit.values() for c in a.get("prefixes", {}).values()
                       for k in ("foreign", "bad_mac", "unmarked"))
    producers = sorted(p for p, lg in r.ledgers.items() if any(ph.get("mode") == "produce" for ph in lg.get("phases", [])))
    unaudited = sorted(set(producers) - set(r.audit))
    leaks = sum(oracle.values()) + audit_oracle
    ok = bar(bars, "leaks: foreign, bad-MAC and unmarked records delivered", {"reads": oracle, "audit": audit_oracle}, LEAKS_MAX, leaks <= LEAKS_MAX)
    ok &= bar(bars, "leaks: every producing project's audit passes", {"failed": audit_bad[:10], "unaudited": unaudited[:10],
              "audited": len(r.audit)}, "all", bool(producers) and not audit_bad and not unaudited)
    return ok


def leg_probes(r: Run, bars: dict) -> bool:
    p = r.legs.get("probes") or {}
    diffs = sum(int(v) for v in (p.get("differences") or {}).values()) if p else None
    put = p.get("put") or {}
    # The value is the distinct (prober, name) first contacts, not the pairs
    # run: a prober that met a name before (or created it itself) proves less.
    first = p.get("first_contacts")
    ok = bar(bars, "probes: cross-project first contacts", {"first_contacts": first, "pairs": p.get("pairs")}, PROBE_PAIRS_MIN,
             int(first or 0) >= PROBE_PAIRS_MIN and first == p.get("pairs"))
    ok &= bar(bars, "probes: differences from a name nobody holds", diffs, 0, diffs == 0)
    ok &= bar(bars, "probes: B's name created in A's own namespace", put, "all 201", bool(put) and set(put) == {"201"})
    return ok


def leg_h11t(r: Run, bars: dict) -> bool:
    """H11t, as the owner re-specified it (round-3 call 9, (a), 2026-10-13):
    on the anonymous capability path, a quiet name (one the target's setup
    created and nothing has touched since, so its descriptor slot is past
    the cell's 5 s TTL) against a never-held name, at 8 pairs a second with
    no catch-up in a window of its own of at least 360 s. Both take the
    server's budgeted store fetch, so their times must not tell them apart
    (KS p > 0.01) and their answers must be identical. The quiet names come
    from the setup's record of what it created; a leg that found none fails
    here with its error, never passes by timing never against never. A name
    used in the last 5 s is answered from its fresh slot without the store:
    the residual the owner accepted (P4, 2026-10-12), not judged here. The
    rig's s3lite sleeps the same 20 ms before every op whatever it answers,
    so the bar certifies the server's path, not a real store's 404-versus-200
    latency."""
    h = r.legs.get("h11t") or {}
    quiet, never = h.get("quiet_ms") or [], h.get("never_ms") or []
    d, p = ks(quiet, never)
    n = min(len(quiet), len(never))
    ok = bar(bars, "H11t: timed pairs (quiet vs never)", {"pairs": n, "rate": h.get("rate"), "gap_min_ms": h.get("gap_min_ms"),
             "names": h.get("names"), "error": h.get("error")}, H11T_PAIRS_MIN, n >= H11T_PAIRS_MIN)
    ok &= bar(bars, "H11t: answers identical", h.get("differences"), 0, h.get("differences") == 0)
    ok &= bar(bars, "H11t: KS p (quiet vs never)", {"p": p, "d": d, "quiet_p50_ms": h.get("quiet_p50_ms"),
              "never_p50_ms": h.get("never_p50_ms")}, f"> {H11T_KS_P_MIN}", p > H11T_KS_P_MIN)
    return ok


def _rss_sheds(r: Run, t0: int, t1: int) -> int | None:
    pts = r.window_loads(t0, t1)
    return k2valid.boot_delta(pts, lambda b: b.get("admit_shed_rss")) if len(pts) >= 2 else None


def shipped_sha256(repo: str, commit: str | None, path: str) -> str | None:
    """sha256 of `path` as committed at `commit` (git show), or None."""
    if not commit:
        return None
    res = subprocess.run(["git", "-C", repo, "show", f"{commit}:{path}"], capture_output=True)
    return hashlib.sha256(res.stdout).hexdigest() if res.returncode == 0 else None


def leg_load(r: Run, bars: dict, repo: str) -> bool:
    """The run drove the shipped Layer B load: tenants.ts's manifest_sha256
    is the server commit's tenants/layer-b.json, at time scale 1. An edited
    manifest or a shortened run (run-shared.sh takes --manifest and
    --time-scale for rehearsals) cannot certify."""
    ran, shipped = r.run.get("manifest_sha256"), shipped_sha256(repo, r.rig.get("commit"), LOAD_MANIFEST)
    ok = bar(bars, f"load: the run's manifest is the commit's {LOAD_MANIFEST}", {"ran": ran, "shipped": shipped}, "equal",
             shipped is not None and ran == shipped)
    scale = r.run.get("time_scale")
    ok &= bar(bars, "load: time scale", scale, LOAD_TIME_SCALE, scale == LOAD_TIME_SCALE)
    return ok


def leg_h9(r: Run, bars: dict, mode: str, name: str) -> bool:
    phases = r.phases(mode)
    if not phases:
        return bar(bars, f"{name}: ran", None, "a phase", False)
    _, ph = phases[0]
    t0, t1 = int(ph.get("start_ms") or 0), int(ph.get("end_ms") or 0)
    detail = ph.get(mode) or {}
    ran = len(detail.get("bursts") or []) if mode == "h9" else int(sum((detail.get("statuses") or {}).values()))
    ok = bar(bars, f"{name}: ran", ran, "> 0", ran > 0)
    sheds = _rss_sheds(r, t0, t1)
    ok &= bar(bars, f"{name}: instance RSS sheds (admit_shed_rss)", sheds, 0, sheds == 0)
    return ok


def leg_compliant(r: Run, bars: dict) -> bool:
    bad = {}
    for cls, c in r.classes.items():
        if c.get("role") == "hostile":
            continue
        ref = c.get("refusals") or {}
        typed = ref.get("typed") or {}
        if typed or ref.get("untyped") or ref.get("transport"):
            bad[cls] = {"typed": typed, "untyped": ref.get("untyped"), "transport": ref.get("transport")}
    return bar(bars, "compliant tenants: refusals", {"refused": bad}, 0, bool(r.classes) and not bad)


def leg_hostile(r: Run, bars: dict) -> bool:
    requests = untyped = 0
    hostile = [c for c in r.classes.values() if c.get("role") == "hostile"]
    for c in hostile:
        requests += int((c.get("totals") or {}).get("requests") or 0)
        untyped += sum(int(v) for v in ((c.get("refusals") or {}).get("untyped") or {}).values())
    frac = untyped / requests if requests else None
    return bar(bars, "hostile tenants: untyped error answers / requests", frac, f"< {HOSTILE_UNTYPED_MAX_FRAC}",
               bool(hostile) and frac is not None and frac < HOSTILE_UNTYPED_MAX_FRAC)


def leg_victim(r: Run, bars: dict) -> bool:
    victims = [c for c in r.classes.values() if c.get("role") == "victim"]
    ok = bool(victims)
    lag = ((r.run.get("loop_lag") or {}).get("noisy") or {}).get("lag_p99_ms")
    for v in victims:
        per = v.get("periods") or {}
        solo, noisy = per.get("solo") or {}, per.get("noisy") or {}
        for kind in ("append", "read"):
            s, n = solo.get(f"{kind}_p99_ms"), noisy.get(f"{kind}_p99_ms")
            limit = max(VICTIM_P99_FACTOR * s, VICTIM_P99_FLOOR_MS) if s is not None else None
            bound = lag is not None and n is not None and lag >= LOOP_LAG_MATERIAL_SHARE * n
            ok &= bar(bars, f"victim: {kind} p99 under noise",
                      {"noisy": n, "solo": s, "loop_lag_p99_ms": lag, "harness_bound": bound}, limit,
                      n is not None and limit is not None and n <= limit)
    return ok


def leg_billing(r: Run, bars: dict) -> bool:
    bad = []
    for p, lg in r.ledgers.items():
        u = r.usage.get(p) or {}
        m = u.get("meters") or {}
        t = lg.get("totals") or {}
        want = (int(t.get("acked_records") or 0), int(t.get("delivered_payload_bytes") or 0), int(t.get("delivered_records") or 0))
        got = (m.get("ingestRecords"), m.get("readPayloadBytes"), m.get("readRecords"))
        if not u.get("settled") or tuple(int(x) if x is not None else None for x in got) != want:
            bad.append({"project": p, "settled": u.get("settled"), "ledger": want, "meters": got})
    projects = int(r.run.get("projects") or 0)
    return bar(bars, "billing: usage = ledger for every project", {"projects": len(r.ledgers), "differ": len(bad), "examples": bad[:5]},
               "all", projects > 0 and len(r.ledgers) == projects and not bad)


def leg_memory(r: Run, bars: dict) -> tuple:
    start, end = r.marks.get("load_start"), r.marks.get("load_end")
    rss = [(t, float(b.get("rss_mb") or 0)) for t, b in r.loads]
    peak = max((v for _, v in rss), default=None)
    ok_peak = bar(bars, "memory: RSS peak (MB)", peak, f"< {RSS_PEAK_MAX_MB}", peak is not None and peak < RSS_PEAK_MAX_MB)
    slope = None
    if start and end:
        win = [(t, v) for t, v in rss if end - SLOPE_WINDOW_SECS * 1000 <= t <= end]
        if len(win) >= 2 and win[-1][0] > win[0][0]:
            slope = (win[-1][1] - win[0][1]) / ((win[-1][0] - win[0][0]) / 60000)
    ok_slope = bar(bars, f"memory: RSS slope over the load's last {SLOPE_WINDOW_SECS // 60} min (MB/min)", slope,
                   f"< {RSS_SLOPE_MAX}", slope is not None and slope < RSS_SLOPE_MAX)
    base_win = [v for t, v in rss if r.marks.get("idle_start", -1) <= t <= (start or -1)]
    base = sorted(base_win)[len(base_win) // 2] if base_win else None
    q0 = r.marks.get("q0_start")
    after = [v for t, v in rss if q0 is not None and t >= q0 + Q0_WITHIN_SECS * 1000]
    at = after[0] if after else None
    ok_q0 = bar(bars, f"Q0: RSS {Q0_WITHIN_SECS // 60} min after the load (MB)", {"rss": at, "baseline": base},
                None if base is None else base + Q0_DELTA_MB, at is not None and base is not None and at <= base + Q0_DELTA_MB)
    return ok_peak and ok_slope, ok_q0


# ---- manifest -----------------------------------------------------------------

def quota_posture(run: dict) -> dict:
    """Which quota set each class ran under (run.json's classes): the
    compliant classes at the cell's ceilings certify isolation at the
    ceilings, not the beta's free-tier posture (owner decision I7 (a))."""
    out: dict = {}
    for cls, c in sorted((run.get("classes") or {}).items()):
        out.setdefault(c.get("quotas") or "ceilings", {}).setdefault(c.get("role") or "?", []).append(cls)
    return out


def tree_oids(repo: str, commit: str | None, paths=HARNESS_TREES) -> dict:
    out = {}
    for path in paths:
        oid = None
        if commit:
            res = subprocess.run(["git", "-C", repo, "rev-parse", "--verify", "-q", f"{commit}:{path}"], capture_output=True, text=True)
            oid = res.stdout.strip() or None
        out[path] = oid
    return out


def leg_harness(r: Run, bars: dict, repo: str) -> bool:
    """The harness that ran (run-shared.sh's checkout.json in rig.json) is
    the server commit's: each tree it runs from equals `git rev-parse
    <commit>:<tree>`, and nothing under them was uncommitted."""
    co = r.rig.get("checkout") or {}
    want, ran = tree_oids(repo, r.rig.get("commit"), CHECKOUT_TREES), co.get("trees") or {}
    ok = bar(bars, "harness: the checkout's trees are the server commit's", {"ran": ran, "commit": want}, "equal",
             bool(co) and all(want[p] is not None and ran.get(p) == want[p] for p in CHECKOUT_TREES))
    ok &= bar(bars, "harness: the checkout is clean under those trees", co.get("dirty"), [], co.get("dirty") == [])
    return ok


def profile_fence(rig: dict) -> dict:
    files = rig.get("profiles") or {}
    lines = set()
    for f in files.values():
        try:
            with open(f.get("path", ""), encoding="utf-8") as fh:
                lines |= {line.strip() for line in fh}
        except OSError:
            pass
    return {"files": sorted(files), "sha256": {k: v.get("sha256") for k, v in sorted(files.items())},
            "pins": [p for p in PROFILE_PINS if p in lines], "pins_missing": [p for p in PROFILE_PINS if p not in lines]}


def identity(r: Run, bars: dict) -> tuple:
    """The server sha from the wrapper's `binary sha256` line and from
    /v1/debug/load, which must agree with each other and with the binary
    the rig shipped."""
    rig = r.rig
    shas = {"wrapper_line": rig.get("wrapper_sha256"), "debug_load": rig.get("debug_load_sha256"), "shipped": (rig.get("binary") or {}).get("sha256")}
    agreed = len(set(shas.values())) == 1 and None not in shas.values()
    ok = bar(bars, "identity: server sha256 (wrapper line, /v1/debug/load, shipped file)", shas, "all equal", agreed)
    return (shas["debug_load"] if agreed else None), ok


def layer_b(run_dir: str, repo: str) -> dict:
    r = Run(run_dir)
    bars: dict = {}
    sha, ok_id = identity(r, bars)
    legs = {
        "load": leg_load(r, bars, repo),
        "H9": leg_h9(r, bars, "h9", "H9"),
        "H9b": leg_h9(r, bars, "h9b", "H9b"),
        "H11t": leg_h11t(r, bars),
        "leaks": leg_leaks(r, bars),
        "probes": leg_probes(r, bars),
        "compliant-refusals": leg_compliant(r, bars),
        "hostile-typed": leg_hostile(r, bars),
        "victim-p99": leg_victim(r, bars),
        "billing": leg_billing(r, bars),
    }
    legs["memory"], legs["Q0"] = leg_memory(r, bars)
    legs["identity"] = ok_id
    legs["harness"] = leg_harness(r, bars, repo)
    legs["release-binary"] = bar(bars, "release binary: the release directory's, not a rehearsal's --binary",
                                 not r.rig.get("rehearsal", True), True, r.rig.get("rehearsal") is False)
    leg_words = {k: "PASS" if v else "FAIL" for k, v in legs.items()}
    commit = r.rig.get("commit")
    legs_dir = os.path.join(r.tenants, "legs")
    files = ([os.path.join(run_dir, f) for f in ("rig.json", "marks.jsonl", "scrape.jsonl")] +
             [os.path.join(r.tenants, f) for f in ("run.json", "classes.json", "usage.json", "audit.json")] +
             [os.path.join(legs_dir, f) for f in (sorted(os.listdir(legs_dir)) if os.path.isdir(legs_dir) else [])])
    evidence = {os.path.relpath(p, run_dir): sha256_file(p) for p in files}
    return {
        "arm": "layer-b",
        "server_sha256": f"sha256:{sha}" if sha else None,
        "verdict": "PASS" if all(legs.values()) else "FAIL",
        "legs": leg_words,
        "started_unix": r.rig.get("started_unix"),
        "finished_unix": r.rig.get("finished_unix"),
        "projects": r.run.get("projects"),
        "load": {"manifest": r.run.get("manifest"), "manifest_sha256": r.run.get("manifest_sha256"),
                 "time_scale": r.run.get("time_scale"), "quota_posture": quota_posture(r.run)},
        "commit": commit,
        "profile": profile_fence(r.rig),
        "cell_env_sha256": (r.rig.get("cell_env") or {}).get("sha256"),
        "harness_trees": tree_oids(repo, commit),
        "region": r.rig.get("region"),
        "bars": bars,
        "known_red": {k: v for k, v in KNOWN_RED.items() if leg_words.get(k) == "FAIL"},
        "evidence": evidence,
    }


ARMS = {"layer-b": layer_b}
FIELD_ARMS = ("envelope", "sc0-1s", "sc1", "stream-count", "battery")


def main(argv: list | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("arm", choices=sorted(ARMS) + list(FIELD_ARMS))
    ap.add_argument("run_dir")
    ap.add_argument("--out", help="the manifest (default RUN_DIR/<arm>-manifest.json)")
    ap.add_argument("--repo", default=os.path.abspath(os.path.join(HERE, "..", "..")), help="the checkout for git rev-parse")
    a = ap.parse_args(argv)
    if a.arm not in ARMS:
        print(f"shared_verdict: arm {a.arm} takes its evidence from the field adapters (deploy-q5 triage B8), "
              "which do not exist yet; no manifest written", file=sys.stderr)
        return 2
    m = ARMS[a.arm](a.run_dir, a.repo)
    out = a.out or os.path.join(a.run_dir, f"{a.arm}-manifest.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(m, f, indent=1, sort_keys=True)
        f.write("\n")
    for name, b in m["bars"].items():
        print(f"{'PASS' if b['pass'] else 'FAIL'} {name}: {json.dumps(b['value'], sort_keys=True)[:200]} (bar {b['bar']})")
    print(f"{m['arm']}: {m['verdict']} {json.dumps(m['legs'], sort_keys=True)} -> {out}")
    return 0 if m["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
