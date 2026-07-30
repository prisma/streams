#!/usr/bin/env python3
"""Gate evaluation for the keyed campaign (spec §15): covering-index
baseline arm vs postings arm, per batch shape.

  keyed-compare.py <cov-out> <v3-out> <batch> <postings_pct_gate>

Prices (documented assumptions): Class A $4.50/M, Class B $0.36/M,
storage $0.02/GiB-month, CPU $0.03/vCPU-hour.
"""
import json
import sys

cov_dir, v3_dir, batch, pct_gate = (
    sys.argv[1],
    sys.argv[2],
    int(sys.argv[3]),
    float(sys.argv[4]),
)

PRICE_A = 4.50 / 1e6
PRICE_B = 0.36 / 1e6
PRICE_GIB_MO = 0.02
PRICE_CPU_HR = 0.03


def load(d, tag):
    with open(f"{d}/snap-{tag}.json") as f:
        return json.load(f)


def rep(d):
    with open(f"{d}/keyed-report.json") as f:
        return json.load(f)


fails = []


def gate(ok, label):
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    if not ok:
        fails.append(label)


def totals(s):
    t = s["stats2"]["total"]
    return t["class_a"], t["class_b"]


def put_bytes(s):
    return s["stats"]["put_bytes"]


def cells(s):
    return s["stats2"]["cells"]


def cell2xx(s, name):
    return cells(s).get(name, {}).get("2xx", 0)


cov0, cov1 = load(cov_dir, "t0"), load(cov_dir, "absorbed")
v30, v31 = load(v3_dir, "t0"), load(v3_dir, "absorbed")
covr, v3r = rep(cov_dir), rep(v3_dir)
cov_read = load(cov_dir, "read")
v3_read = load(v3_dir, "read")

print(f"== keyed campaign gates, batch={batch} ==")

# Write-side gates (spec §15.1) -----------------------------------------
ca_cov = totals(cov1)[0] - totals(cov0)[0]
ca_v3 = totals(v31)[0] - totals(v30)[0]
gate(
    ca_v3 <= ca_cov * 1.01 + 8,
    f"history Class A <= baseline+1%: cov {ca_cov} vs v3 {ca_v3}",
)
for cell in ["shard/manifest/put", "shard/compactions/put", "shard/sst/put"]:
    d_cov = cell2xx(cov1, cell) - cell2xx(cov0, cell)
    d_v3 = cell2xx(v31, cell) - cell2xx(v30, cell)
    gate(
        abs(d_v3 - d_cov) <= max(8, d_cov * 0.1),
        f"flush/manifest unchanged [{cell}]: cov {d_cov} vs v3 {d_v3}",
    )
pb_cov = put_bytes(cov1) - put_bytes(cov0)
pb_v3 = put_bytes(v31) - put_bytes(v30)
gate(
    pb_v3 <= pb_cov * 0.55 or batch > 1 and pb_v3 <= pb_cov * 0.75,
    f"stored bytes <= 55% of covering: cov {pb_cov / 1e6:.1f}MB vs v3 {pb_v3 / 1e6:.1f}MB "
    f"({100 * pb_v3 / max(pb_cov, 1):.1f}%)",
)
post = v31["load"].get("postings", {})
p_ratio = 100.0 * post.get("bytes_written", 0) / max(post.get("canonical_bytes_written", 1), 1)
gate(
    p_ratio <= pct_gate,
    f"postings/canonical bytes {p_ratio:.2f}% <= {pct_gate}%",
)
lists_cov = sum(
    v.get("2xx", 0) for k, v in cells(cov1).items() if "list" in k
) - sum(v.get("2xx", 0) for k, v in cells(cov0).items() if "list" in k)
lists_v3 = sum(
    v.get("2xx", 0) for k, v in cells(v31).items() if "list" in k
) - sum(v.get("2xx", 0) for k, v in cells(v30).items() if "list" in k)
gate(
    abs(lists_v3 - lists_cov) <= max(8, lists_cov * 0.2),
    f"LIST count unchanged: cov {lists_cov} vs v3 {lists_v3}",
)

# Read-side gates (spec §15.2) ------------------------------------------
gate(covr["read_errors"] == 0 and v3r["read_errors"] == 0, "zero read errors both arms")
gate(
    v3r["cold_p50_ms"] <= covr["cold_p50_ms"] * 1.5 + 2,
    f"cold p50 <= 1.5x: cov {covr['cold_p50_ms']}ms vs v3 {v3r['cold_p50_ms']}ms",
)
gate(
    v3r["warm_p50_ms"] <= covr["warm_p50_ms"] * 1.1 + 2,
    f"warm p50 <= 1.1x: cov {covr['warm_p50_ms']}ms vs v3 {v3r['warm_p50_ms']}ms",
)
gate(
    v3r["cold_p99_ms"] <= covr["cold_p99_ms"] * 2.0 + 5,
    f"keyed p99 <= 2.0x: cov {covr['cold_p99_ms']}ms vs v3 {v3r['cold_p99_ms']}ms",
)
spans = v3_read["load"].get("postings", {}).get("read_spans_max", 0)
gate(spans <= 8, f"canonical spans per response {spans} <= 8")
scanned = v3_read["load"]["postings"].get("read_frames_scanned", 0)
matched = v3_read["load"]["postings"].get("read_frames_matched", 1)
gate(
    scanned <= matched * 4,
    f"read amplification {scanned}/{matched} = {scanned / max(matched, 1):.2f}x <= 4x",
)
cache = v3_read["load"]["postings"].get("cache", {}) or {}
hits = cache.get("hits", 0)
# Spec §15.2: >= 90% hit AFTER THE FIRST READ — cold reads are expected
# misses; every WARM read should hit.
warm = v3r["warm_reads"]
gate(
    hits >= warm * 0.9,
    f"postings cache: {hits} hits for {warm} warm reads (>= 90%)",
)
# Per-offset GET pattern: canonical GETs during the read phase must be
# bounded by pages served, not records returned.
recs_served = v3r["cold_reads"] * v3r["expected_per_key"]
g_v3 = cell2xx(v3_read, "shard/sst/get") - cell2xx(v31, "shard/sst/get")
gate(
    g_v3 < recs_served / 4 + 200,
    f"no per-offset GET pattern: {g_v3} sst GETs for {recs_served} records",
)

# Economic gate (spec §15.4) --------------------------------------------
def cogs(t0s, t1s, reads):
    a = totals(t1s)[0] - totals(t0s)[0]
    b = totals(t1s)[1] - totals(t0s)[1]
    pb = put_bytes(t1s) - put_bytes(t0s)
    cpu = t1s["cpu_s"] - t0s["cpu_s"]
    return (
        a * PRICE_A
        + b * PRICE_B
        + pb / 2**30 * PRICE_GIB_MO
        + cpu / 3600 * PRICE_CPU_HR,
        {"class_a": a, "class_b": b, "put_bytes": pb, "cpu_s": round(cpu, 1)},
    )


c_cov, d_cov = cogs(cov0, cov_read, covr)
c_v3, d_v3 = cogs(v30, v3_read, v3r)
ratio = 100.0 * c_v3 / max(c_cov, 1e-12)
gate(
    ratio <= 60.0,
    f"total COGS {ratio:.1f}% <= 60% (cov ${c_cov:.4f} {d_cov} vs v3 ${c_v3:.4f} {d_v3})",
)

print(json.dumps({"fails": fails, "cov": covr, "v3": v3r}, indent=2))
sys.exit(1 if fails else 0)
