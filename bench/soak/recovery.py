#!/usr/bin/env python3
"""Controlled post-ramp recovery window (R26-9).

Two requirements, both hard:

1. every generator FINISHED its ramp (its last stats line is the
   final:true post-join record) — otherwise "recovery" is measured
   while load still flows somewhere;
2. within SOAK_RECOVERY_SECS of entering this phase, every region's
   durable maintenance ledger drains to zero, no per-shard latch stays
   engaged, and the instance latch is released.

The per-region time-to-clear is recorded in recovery.json — that number
is the field drain-rate evidence, measured from a shared boundary
instead of whatever moment each region's deploy happened to stop.
"""
import json, os, sys, time, urllib.request

S = os.environ["SOAK_HOME"]
AUTH = open(f"{S}/auth.txt").read().strip()
WINDOW = int(os.environ.get("SOAK_RECOVERY_SECS", "600"))

def get(url, auth=False):
    req = urllib.request.Request(url, headers=(
        {"Authorization": f"Bearer {AUTH}"} if auth else {}))
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def gen_finished(region):
    gen = open(f"{S}/url-gen-{region}.txt").read().strip()
    lines = get(f"{gen}/")
    return bool(lines) and lines[-1].get("final") is True

def region_clear(region):
    server = open(f"{S}/url-server-{region}.txt").read().strip()
    d = get(f"{server}/v1/debug/load", auth=True)
    m = d.get("maintenance_shards", {})
    shards = m.get("shards", [])
    backlog = sum(sh.get("unabsorbed_frame_bytes", 0) for sh in shards)
    latched = any(sh.get("shard_shed") for sh in shards)
    engaged = d.get("maintenance_backpressure", {}).get("engaged", False)
    return backlog == 0 and not latched and not engaged, backlog

if __name__ == "__main__":
    regions = sys.argv[1:] or (os.environ.get("SOAK_REGIONS", "").split())
    # Phase 1: all ramps ended. Bounded wait — generators end on their
    # own schedule; a gen still ramping means the campaign's sampling
    # window was mis-sized, which is a configuration failure, not a
    # reason to silently keep waiting.
    deadline = time.time() + 300
    unfinished = list(regions)
    while unfinished and time.time() < deadline:
        unfinished = [r for r in unfinished if not gen_finished(r)]
        if unfinished:
            time.sleep(10)
    if unfinished:
        sys.exit(f"RECOVERY FAILED: generators still ramping: {unfinished}")
    t0 = time.time()
    print(f"  all ramps finished; recovery window {WINDOW}s starts now")

    # Phase 2: everything drains inside the window.
    cleared = {}
    pending = list(regions)
    while pending and time.time() - t0 < WINDOW:
        still = []
        for r in pending:
            try:
                ok, backlog = region_clear(r)
            except Exception:
                ok, backlog = False, -1
            if ok:
                cleared[r] = round(time.time() - t0, 1)
                print(f"  {r}: clear at +{cleared[r]}s")
            else:
                still.append((r, backlog))
        pending = [r for r, _ in still]
        if pending:
            time.sleep(15)

    run_id = os.environ.get("SOAK_RUN_ID", "adhoc")
    os.makedirs(f"{S}/results/{run_id}", exist_ok=True)
    json.dump({"window_secs": WINDOW, "cleared_after_secs": cleared,
               "failed": pending},
              open(f"{S}/results/{run_id}/recovery.json", "w"), indent=1)
    if pending:
        sys.exit(f"RECOVERY FAILED: backlog/latches not clear within "
                 f"{WINDOW}s: {pending}")
    print(f"  recovery complete: {cleared}")
