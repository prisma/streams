#!/usr/bin/env python3
"""Apply the predeclared screening limits without granting external acceptance."""
import argparse
import json
import pathlib

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("folder", type=pathlib.Path)
args = parser.parse_args()
folder = args.folder
rules = json.loads((folder / "screen-guardrails.json").read_text())
blocks = json.loads((folder / "screen-identified-blocks.json").read_text())
comparisons = json.loads((folder / "screen-comparisons.json").read_text())
checks = []


def check(row, metric, limit, strict=False):
    value = row["metrics"][metric]
    identity = {k: row[k] for k in ("allocator", "format", "kind", "name", "candidate")}
    if "ci95" in value:
        lower, upper = value["ci95"]
        passed = upper < limit if strict else upper <= limit
        result = "pass" if passed else "fail" if lower > limit else "inconclusive"
    else:
        selected = [b for b in blocks if all(b[k] == row[k] for k in
                    ("allocator", "format", "kind", "name"))]
        original = {b["pair_id"]: b for b in selected if b["version"] == "control"}
        candidate = [b for b in selected if [b["version"], b["cache_on"]] == row["candidate"]]
        assert len(original) == len(candidate) == rules["primary"]["warm_screen"]["blocks"]
        passed = all(b[metric] <= original[b["pair_id"]][metric] * limit for b in candidate)
        result = "pass" if passed else "unaccepted-zero-baseline-excess"
    checks.append(identity | {"metric": metric, "upper_ci_limit": limit,
                              "strict": strict, "result": result, "measurement": value})


for row in comparisons:
    if row["control"] != ["control", False]:
        continue
    primary = row["kind"] == "product-replay" or (
        row["kind"] == "history" and row["name"] == "hot" and row["format"] == "plain")
    if primary:
        for metric in ("p50_us", "p99_us"):
            check(row, metric, 1.0, strict=True)
    elif row["kind"] in ("append", "history"):
        limits = rules["additional_controls"]["append_and_cold_rotating_compressed_other_tenant_latency"]
        check(row, "p50_us", limits["maximum_upper_95_ci_p50_ratio"])
        if "p99_us" in row["metrics"]:
            check(row, "p99_us", limits["maximum_upper_95_ci_p99_ratio"])
    for metric in ("process_peak_rss_bytes", "object_get_attempts",
                   "successful_object_gets", "object_get_range_bytes"):
        if metric in row["metrics"]:
            check(row, metric, rules["additional_controls"]["rss_object_bytes_object_operations"]["maximum_upper_95_ci_ratio"])

assert all(b.get("errors", 0) == b.get("producer_errors", 0) == 0 for b in blocks)
assert all(b.get("product_bridge_copies", 0) == 0 for b in blocks if b["version"] == "candidate")
assert all(b["product_payload_bridges"] == b["requests"] for b in blocks
           if b["kind"] == "append" and "product" in b["name"])
outcomes = {str(condition): {outcome: sum(c["result"] == outcome and c["candidate"] == ["candidate", condition]
             for c in checks) for outcome in ("pass", "fail", "inconclusive", "unaccepted-zero-baseline-excess")}
            for condition in (False, True)}
print(json.dumps({
    "verdict": "HOLD" if any(c["result"] != "pass" for c in checks) else "screen-cleared-only",
    "larger_acceptance_campaign": "not advanced" if any(c["result"] != "pass" for c in checks) else "requires source readiness",
    "outcome_counts_by_cache_enabled": outcomes,
    "cold_tail_status": "Five process-cold observations per condition; cold p99 remains unaccepted.",
    "historical_ceiling_status": "Historical System references remain unchanged. Expanded history partition and allocator differences preclude a general absolute-ceiling certificate; consult matched primary comparisons and report.",
    "resource_scope": "RSS is whole-process peak shared by cases in that process; repeated rows are not independent memory observations. GET/HEAD counts are local object-store API attempts, not cloud requests.",
    "authority": "Author screening decision only; no independent source, crypto, regression-tolerance, merge or deployment approval.",
    "checks": checks,
}, indent=2))
