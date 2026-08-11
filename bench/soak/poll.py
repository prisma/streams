#!/usr/bin/env python3
"""Poll every soak generator; print progress and snapshot the servers.

Three jobs, and none is optional:

1. print one progress line per region, so a stall is visible immediately;
2. write a timestamped `/v1/debug/store` snapshot per region;
3. write a timestamped `/v1/debug/load` snapshot per region (R26-7):
   maintenance engage/shed counters and transitions, per-shard latch
   flags and no-progress clocks, the exact cumulative frame-byte totals,
   and the ordinary rate limiter's refusals by code. Without this, a
   campaign cannot attribute a throughput plateau to the right limiter —
   the 2026-08-11 soak credited maintenance backpressure for what the
   5,000 rec/s per-stream limiter fully explains.

`/v1/debug/store` reports a **trailing 60 s window**. A snapshot taken
after the run has drained is empty — the first version of this harness
collected storage telemetry only at harvest time and got a dash in every
`put:wal` cell of every region. The object-store numbers exist only if
something sampled them while load was flowing. Run this on a loop for the
duration of the soak.
"""
import json, subprocess, os, datetime

S = os.environ.get("SOAK_HOME") or os.path.dirname(os.path.abspath(__file__))
REGIONS = (os.environ.get("SOAK_REGIONS") or
           "us-east-1 us-west-1 eu-central-1 eu-west-3 "
           "ap-southeast-1 ap-northeast-1").split()


def get(url, timeout=30, token=None):
    cmd = ["curl", "-s", "--max-time", str(timeout)]
    if token:
        cmd += ["-H", f"authorization: Bearer {token}"]
    try:
        return subprocess.run(cmd + [url], capture_output=True, text=True,
                              timeout=timeout + 10).stdout
    except Exception:
        return ""


def url(role, r):
    with open(f"{S}/url-{role}-{r}.txt") as f:
        return f.read().strip()


def main():
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%H%M%S")
    print(f"== {stamp} UTC ==")
    try:
        with open(f"{S}/auth.txt") as f:
            tok = f.read().strip()
    except OSError:
        tok = None

    run_id = os.environ.get("SOAK_RUN_ID", "adhoc")
    snapdir = f"{S}/results/{run_id}/store-snaps"
    os.makedirs(snapdir, exist_ok=True)

    for r in REGIONS:
        body = get(url("gen", r))
        try:
            d = json.loads(body)
        except Exception:
            print(f"{r:16s} gen unreachable ({body[:40]!r})")
            d = []
        if d:
            t = d[-1]
            print(f"{r:16s} tiers={len(d):3d}  last={t.get('label')} "
                  f"ok={t.get('ok')} appendP50={t.get('winP50Ms')} "
                  f"appendP99={t.get('winP99Ms')} rtP50={t.get('tailP50Ms')} "
                  f"errs={t.get('errs')} throttled={t.get('throttled')}")
        else:
            print(f"{r:16s} no samples yet")

        raw = get(url("server", r) + "/v1/debug/store", token=tok)
        try:
            json.loads(raw)
        except Exception:
            continue
        with open(f"{snapdir}/{r}-{stamp}.json", "w") as f:
            f.write(raw)

        raw = get(url("server", r) + "/v1/debug/load", token=tok)
        try:
            load = json.loads(raw)
        except Exception:
            continue
        with open(f"{snapdir}/{r}-load-{stamp}.json", "w") as f:
            f.write(raw)
        # Surface latch state inline so an engage is visible in the poll
        # log the minute it happens, not at harvest.
        bp = load.get("maintenance_backpressure", {})
        rl = load.get("rate_limit_refusals", {})
        if bp.get("engaged") or bp.get("appends_shed", 0) or any(rl.values()):
            print(f"{'':16s} maint: engaged={bp.get('engaged')} "
                  f"cause={bp.get('cause')} shed={bp.get('appends_shed')} "
                  f"rate_limited={rl}")


if __name__ == "__main__":
    main()
