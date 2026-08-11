#!/usr/bin/env python3
"""Harvest the 6-region soak: per-tier client metrics + server-side store telemetry.

Writes results.json (raw) and prints markdown tables.

Client metric semantics (bench/awsbench/src/main.rs):
  winP50Ms / winP99Ms  -- append request -> durable ack, 20s window
  tailP50Ms / tailP99Ms -- producer-embedded ts -> consumer observation
                           (FULL producer->consumer roundtrip), 20s window
  ok / errs / throttled -- cumulative counters
"""
import json
import os, sys, subprocess, os, sys, statistics

S = os.environ.get("SOAK_HOME") or os.path.dirname(os.path.abspath(__file__))
REGIONS = (os.environ.get("SOAK_REGIONS") or
           "us-east-1 us-west-1 eu-central-1 eu-west-3 "
           "ap-southeast-1 ap-northeast-1").split()
POP = {"us-east-1": "ewr/iad", "us-west-1": "sjc", "eu-central-1": "fra",
       "eu-west-3": "cdg", "ap-southeast-1": "sin", "ap-northeast-1": "nrt"}


def get(url, timeout=40):
    try:
        r = subprocess.run(["curl", "-s", "--max-time", str(timeout), url],
                           capture_output=True, text=True, timeout=timeout + 15)
        return r.stdout
    except Exception:
        return ""


def url(role, r):
    with open(f"{S}/url-{role}-{r}.txt") as f:
        return f.read().strip()


def pct(vals, q):
    if not vals:
        return None
    v = sorted(vals)
    i = min(len(v) - 1, int(q * len(v)))
    return v[i]


out = {"regions": {}}

for r in REGIONS:
    # A region can be deliberately absent from a campaign (e.g. SIN
    # skipped per platform issue): no url file = not deployed = skip,
    # never crash the whole harvest.
    import os as _os
    if not _os.path.exists(f"{S}/url-gen-{r}.txt"):
        print(f"skipping {r}: no generator url (not deployed this run)", file=sys.stderr)
        continue
    entry = {"pop": POP[r]}

    # ---- client-side (generator) ----
    body = get(url("gen", r))
    samples = []
    try:
        samples = json.loads(body)
    except Exception:
        entry["gen_error"] = body[:200]

    tiers = {}
    for s in samples:
        tiers.setdefault(s["label"], []).append(s)

    tier_rows = []
    for label in sorted(tiers):
        ss = tiers[label]
        # drop the first window of each tier: it straddles the concurrency
        # step-up and mixes the previous tier's in-flight requests. Also
        # drop R26-8 final records from rate/latency medians — they carry
        # exact cumulative counters but no window rates (ss[-1] below
        # still reads them for the cumulative columns, which is exactly
        # their job).
        body_ss = ss[1:] if len(ss) > 2 else ss
        body_ss = [x for x in body_ss if not x.get("final")] or body_ss
        appends_p50 = [x["winP50Ms"] for x in body_ss if x.get("winP50Ms")]
        appends_p99 = [x["winP99Ms"] for x in body_ss if x.get("winP99Ms")]
        rt_p50 = [x["tailP50Ms"] for x in body_ss if x.get("tailP50Ms")]
        rt_p99 = [x["tailP99Ms"] for x in body_ss if x.get("tailP99Ms")]
        # Honest roundtrip (producer -> record decoded) + consumer rearm
        # gap; absent in pre-2026-07-27 generators.
        rtd_p50 = [x["tailDecP50Ms"] for x in body_ss if x.get("tailDecP50Ms")]
        rtd_p99 = [x["tailDecP99Ms"] for x in body_ss if x.get("tailDecP99Ms")]
        rearm_p50 = [x["rearmP50Ms"] for x in body_ss if x.get("rearmP50Ms")]
        dbg_wake = [x["dbgWakeP50Us"] for x in body_ss if x.get("dbgWakeP50Us")]
        dbg_read = [x["dbgReadP50Us"] for x in body_ss if x.get("dbgReadP50Us")]
        rps = [x["achievedPerSec"] for x in body_ss]
        recs = [x["recordsPerSec"] for x in body_ss]
        tier_rows.append({
            "tier": label,
            "conc": ss[0]["conc"],
            "windows": len(ss),
            "appendP50": round(statistics.median(appends_p50), 1) if appends_p50 else None,
            "appendP99": round(max(appends_p99), 1) if appends_p99 else None,
            "rtP50": round(statistics.median(rt_p50), 1) if rt_p50 else None,
            "rtDecP50": round(statistics.median(rtd_p50), 1) if rtd_p50 else None,
            "rtDecP99": round(statistics.median(rtd_p99), 1) if rtd_p99 else None,
            "rearmP50": round(statistics.median(rearm_p50), 2) if rearm_p50 else None,
            "dbgWakeP50Us": round(statistics.median(dbg_wake)) if dbg_wake else None,
            "dbgReadP50Us": round(statistics.median(dbg_read)) if dbg_read else None,
            "rtP99": round(max(rt_p99), 1) if rt_p99 else None,
            "reqPerSec": round(statistics.median(rps)) if rps else 0,
            "recPerSec": round(statistics.median(recs)) if recs else 0,
            "okCum": ss[-1]["ok"],
            "errsCum": ss[-1]["errs"],
            "throttledCum": ss[-1]["throttled"],
            "lastErr": ss[-1].get("lastErr", ""),
        })
    entry["tiers"] = tier_rows
    if samples:
        entry["totals"] = {
            "ok": samples[-1]["ok"],
            "errs": samples[-1]["errs"],
            "throttled": samples[-1]["throttled"],
            "records": samples[-1]["ok"] * samples[-1]["batch"],
        }

    # ---- server-side telemetry ----
    su = url("server", r)
    with open(f"{S}/auth.txt") as f:
        tok = f.read().strip()
    for path, key in (("/v1/debug/store", "store"),
                      ("/v1/debug/usage", "usage"),
                      ("/v1/debug/scaler", "scaler")):
        raw = subprocess.run(["curl", "-s", "--max-time", "40",
                              "-H", f"authorization: Bearer {tok}", su + path],
                             capture_output=True, text=True).stdout
        try:
            entry[key] = json.loads(raw)
        except Exception:
            entry[key + "_error"] = raw[:200]

    out["regions"][r] = entry
    print(f"harvested {r}: {len(tier_rows)} tiers", file=sys.stderr)

run_id = os.environ.get("SOAK_RUN_ID", "adhoc")
os.makedirs(f"{S}/results/{run_id}", exist_ok=True)
with open(f"{S}/results/{run_id}/results.json", "w") as f:
    json.dump(out, f, indent=2)
print(f"wrote {S}/results/{run_id}/results.json", file=sys.stderr)
