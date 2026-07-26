#!/usr/bin/env python3
"""Poll every soak generator; print progress and snapshot the servers.

Two jobs, and the second one is not optional:

1. print one progress line per region, so a stall is visible immediately;
2. write a timestamped `/v1/debug/store` snapshot per region.

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

    snapdir = f"{S}/store-snaps"
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


if __name__ == "__main__":
    main()
