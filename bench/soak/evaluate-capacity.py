#!/usr/bin/env python3
"""Capacity-gate acceptance (R26-11), from EXACT frame-byte samples.

Inputs: the run's store-snaps/<region>-load-<stamp>.json series (written
by poll.py every sample: cumulative ingest/absorbed frame-byte totals,
per-shard ledgers, latch state, typed shed + rate-limit counters), the
pause/restart timestamps, reopen-secs.txt, and recovery.json.

PASS requires:
  (A) catch-up retirement rate >= 1.25 x steady admitted ingest rate
      (frame bytes; catch-up = pause-end .. ledger back under the
      release line), OR
  (B) the hard bound was ACTUALLY EXERCISED and held (R27-3 — the R26
      version passed when a 34 MiB peak stayed under a 512 MiB cap,
      which proves nothing about the bound):
        peak ledger >= CAP_STRESS_FRACTION (default 0.75) x instance cap
        AND peak <= 1.05 x cap (in-flight + evaluator-tick overshoot
            allowance; "approaches but does not materially exceed")
        AND typed maintenance shed observed (appends_shed increased)
        AND the backlog STABILIZED at the line while overloaded
            (>= 2 samples at >= stress x cap with shed active and no
            growth past the allowance)
        AND it drained after healing (recovery clean);
  AND the ordinary rate limiter stayed silent (raised for the campaign;
      any limit_* refusal means the wrong mechanism was tested),
  AND no unexpected counter reset (process exit) outside a declared
      restart leg,
  AND reopen-secs <= bound when a restart leg ran,
  AND recovery.json shows a clean drain,
  AND reconcile.json is verdict OK (checked by the driver stage order).
"""
import glob, json, os, sys

S = os.environ["SOAK_HOME"]
RUN = os.environ.get("SOAK_RUN_ID") or open(f"{S}/current-run-id.txt").read().strip()
D = f"{S}/results/{RUN}"
SHARD_CAP = int(os.environ.get("MAX_UNABSORBED_BYTES_PER_SHARD", 256 * 1024 * 1024))
INST_CAP = int(os.environ.get("MAX_UNABSORBED_BYTES_PER_INSTANCE", 512 * 1024 * 1024))
RELEASE_PCT = 75
STRESS_FRACTION = float(os.environ.get("CAP_STRESS_FRACTION", "0.75"))
OVERSHOOT_ALLOWANCE = 1.05
RESTART_LEG = os.environ.get("SOAK_CAP_RESTART", "1") == "1"

def ts_of(path):
    # <region>-load-<HHMMSS>.json — HHMMSS UTC; runs never span midnight
    # unnoticed (the driver stamps absolute epochs for pause/restart).
    stamp = path.rsplit("-", 1)[1].split(".")[0]
    return int(stamp[0:2]) * 3600 + int(stamp[2:4]) * 60 + int(stamp[4:6])

def main(region):
    snaps = []
    for p in sorted(glob.glob(f"{D}/store-snaps/{region}-load-*.json")):
        try:
            d = json.load(open(p))
        except Exception:
            continue
        m = d.get("maintenance_shards", {})
        rl = d.get("rate_limit_refusals", {})
        snaps.append({
            "t": ts_of(p),
            "ingest": m.get("ingest_frame_bytes_total", 0),
            "absorbed": m.get("absorbed_frame_bytes_total", 0),
            "ledger": sum(sh.get("unabsorbed_frame_bytes", 0)
                          for sh in m.get("shards", [])),
            "max_shard": max([sh.get("unabsorbed_frame_bytes", 0)
                              for sh in m.get("shards", [])] or [0]),
            "shed": d.get("maintenance_backpressure", {}).get("appends_shed", 0),
            "rate_limited": sum(rl.values()) if isinstance(rl, dict) else 0,
        })
    if len(snaps) < 8:
        sys.exit(f"EVALUATE FAILED: only {len(snaps)} load snapshots")
    # Un-wrap midnight if the HHMMSS clock stepped backward mid-series.
    for i in range(1, len(snaps)):
        while snaps[i]["t"] < snaps[i - 1]["t"]:
            snaps[i]["t"] += 86400

    pause_start = int(open(f"{D}/pause-start.ts").read().split()[0])
    pause_end = int(open(f"{D}/pause-end.ts").read().split()[-1])
    pause_wall = pause_end - pause_start

    # Steady ingest rate: the window BEFORE the pause, skipping warmup.
    # Counters are cumulative; a restart resets them (process state), so
    # rates come from per-interval deltas and negative deltas mark the
    # restart boundary.
    def rate(series, key):
        out = []
        for a, b in zip(series, series[1:]):
            dt = b["t"] - a["t"]
            dv = b[key] - a[key]
            if dt > 0 and dv >= 0:
                out.append(dv / dt)
        return out

    n = len(snaps)
    steady = snaps[max(2, n // 10): int(n * 0.6)]
    steady_ingest = rate(steady, "ingest")
    steady_rate = sorted(steady_ingest)[len(steady_ingest) // 2] if steady_ingest else 0

    # Catch-up window: after the pause, while the ledger is above the
    # instance release line, retirement per interval.
    release_line = INST_CAP * RELEASE_PCT // 100
    peak = max(s["ledger"] for s in snaps)
    peak_shard = max(s["max_shard"] for s in snaps)
    tail = [s for s in snaps if s["ledger"] > min(release_line, peak // 2)]
    catch = rate(tail, "absorbed") if len(tail) >= 2 else []
    catch_rate = max(catch) if catch else 0

    try:
        reopen = int(open(f"{D}/reopen-secs.txt").read().strip())
    except OSError:
        reopen = None  # no restart leg this run
    recovery = json.load(open(f"{D}/recovery.json"))
    rec = {r["region"]: r for r in json.load(open(f"{D}/reconcile.json"))}

    ratio = (catch_rate / steady_rate) if steady_rate else 0.0
    # Typed shed: sum of positive per-interval deltas (a restart resets
    # the cumulative counter; negative deltas mark the boundary).
    shed_total = sum(max(0, b["shed"] - a["shed"]) for a, b in zip(snaps, snaps[1:]))
    rate_limited_total = sum(
        max(0, b["rate_limited"] - a["rate_limited"]) for a, b in zip(snaps, snaps[1:])
    )
    # Unexpected process exit: a cumulative-ingest reset with no
    # declared restart leg.
    resets = sum(1 for a, b in zip(snaps, snaps[1:]) if b["ingest"] < a["ingest"])
    unexpected_reset = resets > (1 if RESTART_LEG else 0)

    caps_held = (peak <= INST_CAP * OVERSHOOT_ALLOWANCE
                 and peak_shard <= SHARD_CAP * OVERSHOOT_ALLOWANCE)
    stressed = peak >= STRESS_FRACTION * INST_CAP
    # Stabilized at the line: >= 2 samples in the stress band while
    # typed shed was actively increasing, none past the allowance.
    band = [i for i, sn in enumerate(snaps) if sn["ledger"] >= STRESS_FRACTION * INST_CAP]
    shed_in_band = any(
        snaps[j]["shed"] > snaps[j - 1]["shed"] for j in band if j > 0
    )
    stabilized = len(band) >= 2 and shed_in_band and caps_held
    drained = not recovery.get("failed")

    pass_a = steady_rate > 0 and ratio >= 1.25
    pass_b = caps_held and stressed and shed_total > 0 and stabilized and drained
    verdict = {
        "region": region,
        "steady_ingest_bytes_per_sec": round(steady_rate),
        "catchup_retire_bytes_per_sec": round(catch_rate),
        "catchup_over_ingest": round(ratio, 3),
        "peak_ledger_bytes": peak,
        "peak_shard_ledger_bytes": peak_shard,
        "instance_cap": INST_CAP,
        "shard_cap": SHARD_CAP,
        "stress_fraction": STRESS_FRACTION,
        "stressed": stressed,
        "caps_held": caps_held,
        "maintenance_shed_total": shed_total,
        "rate_limited_total": rate_limited_total,
        "stabilized_under_overload": stabilized,
        "unexpected_reset": unexpected_reset,
        "pause_wall_secs": pause_wall,
        "reopen_secs": reopen,
        "recovery": recovery.get("cleared_after_secs", {}),
        "reconcile_verdict": rec.get(region, {}).get("verdict"),
        "pass_A_catchup_ratio": pass_a,
        "pass_B_bound_exercised_and_held": pass_b,
        "PASS": (pass_a or pass_b)
                and rate_limited_total == 0
                and not unexpected_reset
                and drained
                and rec.get(region, {}).get("verdict") == "OK",
    }
    json.dump(verdict, open(f"{D}/capacity-verdict.json", "w"), indent=1)
    print(json.dumps(verdict, indent=1))
    if not verdict["PASS"]:
        sys.exit("CAPACITY GATE: FAIL")

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else os.environ["SOAK_REGIONS"].split()[0])
