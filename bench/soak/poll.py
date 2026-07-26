#!/usr/bin/env python3
"""Poll every soak generator + server; print a compact progress line."""
import json, subprocess, sys, os, datetime

S = os.environ.get("SOAK_HOME") or os.path.dirname(os.path.abspath(__file__))
REGIONS = ["us-east-1", "us-west-1", "eu-central-1", "eu-west-3",
           "ap-southeast-1", "ap-northeast-1"]


def get(url, timeout=30):
    try:
        out = subprocess.run(["curl", "-s", "--max-time", str(timeout), url],
                             capture_output=True, text=True, timeout=timeout + 10)
        return out.stdout
    except Exception:
        return ""


def url(role, r):
    with open(f"{S}/url-{role}-{r}.txt") as f:
        return f.read().strip()


print(datetime.datetime.utcnow().strftime("== %H:%M:%S UTC =="))
for r in REGIONS:
    body = get(url("gen", r))
    try:
        d = json.loads(body)
    except Exception:
        print(f"{r:16s} gen unreachable ({body[:40]!r})")
        continue
    tail = ""
    if d:
        t = d[-1]
        tail = (f"last={t.get('label')} acc={t.get('accepted')} "
                f"appendP50={t.get('winP50Ms')} appendP99={t.get('winP99Ms')} "
                f"rtP50={t.get('tailP50Ms')} errs={t.get('errs')}")
    print(f"{r:16s} tiers={len(d):2d}  {tail}")
