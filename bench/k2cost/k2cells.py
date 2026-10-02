"""K2 cost experiment: the physical ledger's pricing and the §4.3 allocation
(target/k2-design.md §1, §4.3), shared by price.py.

Every s3lite cell is "tier/kind/op" with per-status counts. A cell's
requests are priced at Tigris Standard (Class A $5e-6, Class B $5e-7,
free statuses and ops) and then booked to one of these buckets:

  floor_kind  time-driven by its kind, whatever window it lands in: the
              fleet, registry and telemetry tiers (heartbeats, router
              reports, LISTs, the billing plane's rollup DB, read spool and
              monthly artifacts) and every GET/HEAD answered 404 on a
              manifest or compaction-state key (the idle pollers of each
              open DB);
  floor_rate  the idle-baseline rate of the cell (per cell and status, from
              a baseline window with the same DBs open) x the window's
              seconds: system-stream cadence, timer L0s, GC LISTs;
  produce     write-driven: appends' WAL/L0/compaction PUTs, GC deletes and
              history-tier work (absorption, history compaction) wherever
              they land, the deferred window, quiesce phases;
  consume     read-driven: GETs and HEADs in windows with readers, and the
              durable commits of consumer-group pulls and settles;
  lifecycle   everything above the floor in churn and setup (stream
              creation) windows; folded into produce for the K2 comparison;
  excluded    restart windows (shutdown flush and cold open) and windows
              that move no data.

In a window that holds both appends and consumer commits, a shard write
cell is split by commits: produce share = acked append requests / (acked
appends + non-empty pulls + settles). WAL PUTs are group-committed, so
this is a proportional model, not a count of which PUT carried what.
"""

from __future__ import annotations

import json
from collections import defaultdict

GIB = float(1 << 30)
DEC = GIB / 1e9  # revenue multiplier when K2 bills decimal GB
MONTH_SECS = 30.4 * 86400
CLASS_USD = {"A": 5e-6, "B": 5e-7}
STORAGE_USD = 0.02
K2 = {"produce": 0.04, "consume": 0.04, "retained": 0.02}

# Op -> class. "A*" = Class A in the headline, free in the alt column.
OP_CLASS = {
    "put": "A", "copy": "A", "list": "A", "mpu_create": "A",
    "list_parts": "A", "list_multipart_uploads": "A",
    "multipart": "A*",  # s3lite's coarse cell: create + parts + complete
    "upload_part": "A*", "mpu_complete": "A*", "delete_objects": "A*",
    "get": "B", "head": "B",
    "delete": "free", "mpu_abort": "free", "abort": "free", "other": "free",
}
BILLED_STATUS = {"2xx", "404", "429", "502", "503", "504"}
# s3lite's "4xx" bucket holds only 400/405 and "5xx" only 500 (the codes it
# can emit), all free; C1-style exact buckets are listed too.
FREE_STATUS = {"304", "412", "301", "307", "400", "403", "405", "409", "411",
               "416", "500", "501", "4xx", "5xx", "failed"}
CONSUME_MODES = {"walk", "tail", "group", "subs"}
FIELDS = ("class_a", "class_b", "free", "ambiguous_a", "usd", "usd_alt")

FLOOR_TIERS = {"fleet", "registry", "telemetry"}
POLL_KINDS = {"manifest", "compactions"}
READ_OPS = {"get", "head"}
DELETE_OPS = {"delete", "delete_objects", "mpu_abort", "abort"}
MULTIPART_OPS = {"multipart", "upload_part", "mpu_create", "mpu_complete"}
CUSTOMER_TIERS = {"shard", "hist"}
LIFECYCLE_DIMS = {"lifecycle", "setup"}  # churn, and stream creation (setup)
BUCKETS = ("produce", "consume", "lifecycle", "floor_kind", "floor_rate", "excluded")


def load(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, ValueError):
        return default


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


def cell_delta(a: dict, b: dict) -> dict:
    """b - a per tier/kind/op cell and status bucket (both cumulative)."""
    out = {}
    for key in set(a or {}) | set(b or {}):
        ca, cb = (a or {}).get(key, {}), (b or {}).get(key, {})
        d = {s: int(cb.get(s, 0)) - int(ca.get(s, 0)) for s in set(ca) | set(cb)}
        d = {s: v for s, v in d.items() if v}
        if d:
            out[key] = d
    return out


def op_class(tier: str, kind: str, op: str) -> str:
    if op == "delete" and tier == "other" and kind == "meta":
        # s3lite files POST /bucket?delete (DeleteObjects, no key) here; a
        # keyed DELETE of an other/meta object lands here too (priced high).
        return "A*"
    return OP_CLASS.get(op, "A")


def unit(tier: str, kind: str, op: str, status: str, create_frac: float = 0.0) -> dict:
    """The price of ONE request of this cell and status. create_frac: the
    share of a coarse `multipart` cell that is CreateMultipartUpload (Class
    A in both columns; s3lite files create, parts and complete together)."""
    cls = op_class(tier, kind, op)
    billed = status in BILLED_STATUS or status not in FREE_STATUS  # unknown: billed
    u = {k: 0.0 for k in FIELDS}
    if not billed or cls == "free":
        u["free"] = 1.0
        return u
    if cls in ("A", "A*"):
        u["class_a"] = 1.0
        u["usd"] = CLASS_USD["A"]
        u["usd_alt"] = CLASS_USD["A"]
        if cls == "A*":
            u["ambiguous_a"] = 1.0
            u["usd_alt"] = CLASS_USD["A"] * (create_frac if op == "multipart" else 0.0)
    else:
        u["class_b"] = 1.0
        u["usd"] = u["usd_alt"] = CLASS_USD["B"]
    return u


def create_fraction(cells: dict, creates) -> float:
    """creates / billed multipart requests in these cells (alt column)."""
    if not creates:
        return 0.0
    n = sum(v for k, st in cells.items() if k.endswith("/multipart")
            for s, v in st.items() if s in BILLED_STATUS)
    return min(1.0, creates / n) if n > 0 else 0.0


def zero() -> dict:
    return {k: 0.0 for k in FIELDS}


def add(acc: dict, u: dict, n: float) -> None:
    for k in FIELDS:
        acc[k] += u[k] * n


def rounded(t: dict) -> dict:
    return {k: (round(v, 9) if "usd" in k else round(v, 3)) for k, v in t.items()}


def price_cells(cells: dict, creates=None) -> dict:
    """Class A/B counts and dollars, by cell and rolled up by tier/kind/op.
    creates: logical multipart uploads in the window (None: unknown, every
    multipart request is free in the alt column)."""
    frac = create_fraction(cells, creates)
    rows, unknown = [], set()
    tot = zero()
    by = {"tier": defaultdict(zero), "kind": defaultdict(zero), "op": defaultdict(zero)}
    for key in sorted(cells):
        parts = key.split("/")
        if len(parts) != 3:
            continue
        tier, kind, op = parts
        row = zero()
        for status, n in cells[key].items():
            if status not in BILLED_STATUS and status not in FREE_STATUS:
                unknown.add(status)
            add(row, unit(tier, kind, op, status, frac), n)
        rows.append({"cell": key, "class": op_class(tier, kind, op), **rounded(row)})
        for acc in (tot, by["tier"][tier], by["kind"][kind], by["op"][op]):
            for k in FIELDS:
                acc[k] += row[k]
    return {
        "totals": {k: (round(v, 9) if "usd" in k else int(round(v))) for k, v in tot.items()},
        "by_tier": {t: rounded(v) for t, v in by["tier"].items()},
        "by_kind": {t: rounded(v) for t, v in by["kind"].items()},
        "by_op": {t: rounded(v) for t, v in by["op"].items()},
        "cells": rows,
        "unknown_status_buckets": sorted(unknown),
        "multipart_creates_rebilled": creates if frac else 0,
    }


# ---- the §4.3 drivers ---------------------------------------------------------------

def floor_kind(tier: str, kind: str, op: str, status: str) -> bool:
    """Time-driven by kind: booked to the floor in every window."""
    return tier in FLOOR_TIERS or (op in READ_OPS and status == "404" and kind in POLL_KINDS)


def deferred_write(tier: str, op: str) -> bool:
    """Write-driven work that lands wherever its timer fires: GC deletes and
    history-tier work other than GETs (absorption at ABSORB_AGE_SECS,
    history compaction, the history WAL probes). Produce, in any window."""
    return op in DELETE_OPS or (tier == "hist" and op != "get")


def rate_floor_cell(tier: str, kind: str, op: str, status: str) -> bool:
    """A cell whose idle-baseline rate is subtracted: neither floor-by-kind
    nor deferred write work (a baseline must not absorb earlier phases'
    GC deletes or absorption)."""
    return not floor_kind(tier, kind, op, status) and not deferred_write(tier, op)


def baseline_rates(windows: list) -> dict:
    """{"tier/kind/op|status": requests per second} over baseline windows
    [(cells, secs)], rate-floor cells only."""
    counts, secs = defaultdict(float), sum(s for _, s in windows)
    for cells, _ in windows:
        for key, st in cells.items():
            parts = key.split("/")
            if len(parts) != 3:
                continue
            for status, n in st.items():
                if rate_floor_cell(*parts, status):
                    counts[f"{key}|{status}"] += n
    return {k: v / secs for k, v in counts.items() if v > 0} if secs > 0 else {}


def driver(tier: str, op: str, ctx: dict) -> dict:
    """Bucket shares for requests above the floor (not floor-kind, not
    deferred write work)."""
    dim = ctx["dimension"]
    if dim in LIFECYCLE_DIMS:
        return {"lifecycle": 1.0}
    if dim not in ("consume", "mixed"):
        return {"produce": 1.0}  # produce, quiesce, deferred
    if op in READ_OPS:
        return {"consume": 1.0} if ctx.get("has_readers") else {"produce": 1.0}
    pc, cc = float(ctx.get("produce_commits") or 0), float(ctx.get("consume_commits") or 0)
    if pc + cc > 0:
        return {"produce": pc / (pc + cc), "consume": cc / (pc + cc)}
    return {"consume": 1.0} if dim == "consume" else {"produce": 1.0}


def allocate(cells: dict, secs: float, ctx: dict, rates: dict, creates=None) -> dict:
    """Book one window's cells to the buckets. Returns {bucket: totals} plus
    "wal_puts" (billed shard/wal/put) and "wal_floor" (its rate floor)."""
    frac = create_fraction(cells, creates)
    out = {b: zero() for b in BUCKETS}
    dim = ctx["dimension"]
    wal = wal_floor = 0.0
    for key, st in cells.items():
        parts = key.split("/")
        if len(parts) != 3:
            continue
        tier, kind, op = parts
        for status, n in st.items():
            u = unit(tier, kind, op, status, frac)
            if key == "shard/wal/put" and status in BILLED_STATUS:
                wal += n
            if dim in ("restart", "none"):
                add(out["excluded"], u, n)
                continue
            if floor_kind(tier, kind, op, status):
                add(out["floor_kind"], u, n)
                continue
            if deferred_write(tier, op):
                add(out["lifecycle" if dim in LIFECYCLE_DIMS else "produce"], u, n)
                continue
            if dim == "idle":
                add(out["floor_rate"], u, n)
                continue
            f = min(float(n), rates.get(f"{key}|{status}", 0.0) * secs)
            add(out["floor_rate"], u, f)
            if key == "shard/wal/put":
                wal_floor += f
            for b, share in driver(tier, op, ctx).items():
                add(out[b], u, (n - f) * share)
    res = {b: rounded(v) for b, v in out.items()}
    res["wal_puts"] = wal
    res["wal_floor"] = round(wal_floor, 3)
    return res


def sum_buckets(allocs: list) -> dict:
    out = {b: zero() for b in BUCKETS}
    for a in allocs:
        for b in BUCKETS:
            for k in FIELDS:
                out[b][k] += (a.get(b) or {}).get(k, 0.0)
    return {b: rounded(v) for b, v in out.items()}
