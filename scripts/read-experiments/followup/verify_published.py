#!/usr/bin/env python3
"""Audit published block pairing/arithmetic; this does not verify held raw samples."""
import argparse
import json
import math
import pathlib
import random
import statistics


def verify(folder):
    blocks = json.loads((folder / "screen-identified-blocks.json").read_text())
    comparisons = json.loads((folder / "screen-comparisons.json").read_text())
    receipts = json.loads((folder / "screen-build-receipts.json").read_text())
    identities = {
        (r["allocator"], r["base_revision"], r["base_tree"], r["binary_sha256"])
        for r in receipts
    }
    seen = set()
    for row in blocks:
        key = row["pair_id"], row["version"], row["cache_on"]
        assert key not in seen, ("duplicate condition in pair", key)
        seen.add(key)
        assert row["requests"] > 0
        assert "latencies_us" not in row
        assert row.get("errors", 0) == row.get("producer_errors", 0) == 0
        assert (row["allocator"], row["base_revision"], row["base_tree"], row["binary_sha256"]) in identities
        assert len(row["log_sha256"]) == 64
        if row["name"] == "process-cold":
            assert row["requests"] == 1 and "p99_us" not in row
    checked = 0
    for comparison in comparisons:
        selected = [r for r in blocks if all(
            r[k] == comparison[k] for k in ("allocator", "format", "kind", "name")
        )]
        left, right = [
            {r["pair_id"]: r for r in selected
             if [r["version"], r["cache_on"]] == comparison[side]}
            for side in ("control", "candidate")
        ]
        assert left and left.keys() == right.keys(), "unpaired blocks"
        for metric, reported in comparison["metrics"].items():
            if "geometric_mean_ratio" not in reported:
                assert any(left[p][metric] <= 0 or right[p][metric] <= 0 for p in left)
                continue
            pair_ids = reported["pair_ids"]
            assert len(pair_ids) == len(set(pair_ids)) == reported["paired_blocks"] == len(left)
            assert set(pair_ids) == left.keys()
            logs = [math.log(right[p][metric] / left[p][metric]) for p in pair_ids]
            rng = random.Random(20260907)
            resamples = sorted(math.exp(sum(rng.choices(logs, k=len(logs))) / len(logs))
                               for _ in range(20000))
            expected = [math.exp(statistics.mean(logs)), resamples[499], resamples[19499]]
            actual = [reported["geometric_mean_ratio"], *reported["ci95"]]
            assert all(math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-12)
                       for a, b in zip(expected, actual)), (comparison["name"], metric)
            assert reported["control_median_of_blocks"] == statistics.median(left[p][metric] for p in pair_ids)
            assert reported["candidate_median_of_blocks"] == statistics.median(right[p][metric] for p in pair_ids)
            checked += 1
    return {"block_rows": len(blocks), "comparisons": len(comparisons),
            "ratio_and_interval_triplets_reproduced": checked,
            "tolerance": 1e-12,
            "scope": "Author arithmetic audit over published blocks; no independent raw-sample or binary verification."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=pathlib.Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.folder), indent=2))
