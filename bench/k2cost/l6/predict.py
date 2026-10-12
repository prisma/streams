#!/usr/bin/env python3
"""L6 seal spike: pre-registered model predictions per census point
(SPIKE-PLAN.md §2.3 "Predictions", review C3).

  python3 predict.py CENSUS_JSON --out predict.json [--copy-to-runs]

Python 3.9 compatible, standard library only. For every run in the census it
feeds the measured inputs (request rate per shard, logical bytes per record,
records per request, c = logical / stored, writer streams, shards, the load
and tail seconds) into design B's cost model:

  WAL_GAP=100  -> ~/.streams-k2/analysis/reprice-100ms/upload-once/design-b/design_b_model.py, tier '100ms'
  WAL_GAP=1000 -> ~/.streams-k2/analysis/upload-once/design-b/design_b_model.py, tier '1s'

and records both arms: `today` (layout 5 on today's two LSMs, at the run's
40-minute tenure: the base arm's expected figures) and `design_b`.

design_b carries rBn F4's W2 correction: a seal forces TWO WAL flushes (step 1
and the memtable flush's own), so the fixed point uses 2/w where the model
has 1.0/w. The patch is applied to the model's own design_b source at load
time and recorded (sha256 of the file, occurrences replaced).

Variants (the primary is the one M5 measures):
  primary      review B1 triggers: seal at 48 WAL objects / 45 s / 6 MiB
               (deadlines 64 / 60 s / SlateDB's 8 MiB), PUT_BW = the run's
               s3lite cap (25 MB/s; an uncapped run uses 1e15), 2 flushes/seal
  design       the model's own triggers 64 / 60 s / 6 MiB, PUT_BW 100 MB/s, 2 flushes/seal
  design_w1    the model unchanged (1 flush/seal), for reference
Retention for the index-row term = the run's load + tail (40 min).

Never edited after M5 runs (review C3): a corrected model gets a second file.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import inspect
import json
import math
import os
import sys
import time

A = os.path.expanduser("~/.streams-k2/analysis")
MODELS = {
    100: (os.path.join(A, "reprice-100ms/upload-once/design-b/design_b_model.py"), "100ms", 2),
    1000: (os.path.join(A, "upload-once/design-b/design_b_model.py"), "1s", 1),
}
MIB = 2 ** 20


def sha256(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def load_model(gap):
    path, tier, expect = MODELS[gap]
    spec = importlib.util.spec_from_file_location("design_b_model_g%d" % gap, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    src = inspect.getsource(mod.design_b)
    n = src.count("1.0 / w")
    if n != expect:
        raise SystemExit("predict: %s has %d '1.0 / w' terms in design_b, expected %d" % (path, n, expect))
    patched = src.replace("1.0 / w", "FLUSHES_PER_SEAL / w").replace("def design_b(", "def design_b_w2(", 1)
    mod.FLUSHES_PER_SEAL = 2.0
    exec(compile(patched, path + ":design_b_w2", "exec"), mod.__dict__)  # noqa: S102 - the model's own source, one documented substitution
    return mod, {"path": path, "sha256": sha256(path), "tier": tier, "w2_patch": "'1.0 / w' -> 'FLUSHES_PER_SEAL / w' (=2) in design_b, %d occurrence(s)" % n}


def clean(x):
    if isinstance(x, dict):
        return {k: clean(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [clean(v) for v in x]
    if isinstance(x, float):
        return x if math.isfinite(x) else None
    return x


def predict_run(r):
    gap = int(r["posture"].get("WAL_FLUSH_GAP_MS") or 100)
    shards = int(r["posture"].get("INITIAL_SHARDS") or 1)
    mod, meta = load_model(gap)
    ld = r["load"]
    reqs, recs = ld["acked_requests"], ld["acked_records"]
    L, S = ld["logical_bytes_L"], ld["stored_bytes_S"]
    secs = r["windows_s"]["load"]
    n = recs / reqs
    rec = L / recs
    c = L / S
    p = dict(gday=L / secs * 86400 / mod.GIB, rec=rec, n=n, streams=ld.get("customer_streams") or 8, shards=shards,
             distinct=False)
    tenure_days = (r["windows_s"]["load"] + r["windows_s"]["tail"]) / 86400.0
    cap = r["posture"].get("s3lite_put_bytes_per_sec")
    put_bw = float(cap) if cap else 1e15
    out = {"inputs": {"wal_gap_ms": gap, "tier": meta["tier"], "shards": shards, "streams": p["streams"],
                      "records_per_request": n, "logical_bytes_per_record": rec, "c": c,
                      "req_per_s_per_shard": reqs / secs / shards, "stored_bytes_per_request": S / reqs,
                      "stored_Bps_per_shard": S / secs / shards, "tenure_days": tenure_days,
                      "s3lite_put_bytes_per_sec": cap, "profile": p},
           "model": meta}
    saved = {k: getattr(mod, k) for k in ("SEAL_WAL_OBJECTS", "SEAL_AGE_S", "SEAL_BYTES", "PUT_BW")}
    try:
        out["today"] = mod.today(p, c, 5, meta["tier"], tenure_days)
        variants = {}
        for name, objs, age, bw, fn in (("primary", 48, 45.0, put_bw, mod.design_b_w2),
                                         ("design", 64, 60.0, 100e6, mod.design_b_w2),
                                         ("design_w1", 64, 60.0, 100e6, mod.design_b)):
            mod.SEAL_WAL_OBJECTS, mod.SEAL_AGE_S, mod.SEAL_BYTES, mod.PUT_BW = objs, age, 6 * MIB, bw
            res = fn(p, c, meta["tier"], tenure_days)
            res["params"] = {"seal_wal_objects": objs, "seal_age_s": age, "seal_bytes": 6 * MIB, "put_bw": bw,
                             "flushes_per_seal": 1 if fn is mod.design_b else 2}
            variants[name] = res
        out["design_b"] = variants
    finally:
        for k, v in saved.items():
            setattr(mod, k, v)
    return clean(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("census")
    ap.add_argument("--out", required=True)
    ap.add_argument("--copy-to-runs", action="store_true", help="also write <run dir>/predict.json")
    a = ap.parse_args()
    with open(a.census, encoding="utf-8") as f:
        census = json.load(f)
    doc = {"generated_unix": int(time.time()), "tool": os.path.abspath(__file__), "tool_sha256": sha256(os.path.abspath(__file__)),
           "census": os.path.abspath(a.census), "points": {}}
    for name, r in sorted(census["runs"].items()):
        if "error" in r:
            doc["points"][name] = {"error": r["error"]}
            continue
        doc["points"][name] = predict_run(r)
        pr = doc["points"][name]
        if a.copy_to_runs and os.path.isdir(r["run"]):
            with open(os.path.join(r["run"], "predict.json"), "w", encoding="utf-8") as f:
                json.dump({"point": name, **pr}, f, indent=1, sort_keys=True)
        t, b = pr["today"], pr["design_b"]["primary"]
        print("%-70s today: up %.2f (wal %.2f) A %.0f B %.0f | B: up %.3f (wal %.3f) A %.0f B %.0f window %.1f s obj %.2f MB leak %.3f"
              % (name, t["uploads_total"], t["uploads"]["wal"], t["class_a_total"], t["class_b_total"], b["uploads_total"],
                 b["uploads"]["wal"], b["class_a_total"], b["class_b_total"], b["window_s"], b["object_bytes"] / 1e6, b["leak"]))
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=1, sort_keys=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
