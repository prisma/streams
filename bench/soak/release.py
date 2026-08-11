#!/usr/bin/env python3
"""Synchronized generator release (R26-9).

Deploys are sequential and take minutes per region; generators deployed
with BENCH_START_GATED park before their first tier. After every region
verifies, this releases them all within seconds of each other, so each
region's tier windows share a common t0 and the post-ramp recovery
window is one controlled measurement instead of six staggered ones.
"""
import os, sys, time, urllib.request

S = os.environ["SOAK_HOME"]

def post_start(region):
    gen = open(f"{S}/url-gen-{region}.txt").read().strip()
    req = urllib.request.Request(f"{gen}/start", method="POST", data=b"")
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.status

if __name__ == "__main__":
    regions = sys.argv[1:] or (os.environ.get("SOAK_REGIONS", "").split())
    t0 = time.time()
    failed = []
    for r in regions:
        try:
            post_start(r)
            print(f"  released {r} at +{time.time() - t0:.1f}s")
        except Exception as e:
            failed.append(f"{r}: {e}")
    if failed:
        sys.exit("RELEASE FAILED:\n  " + "\n  ".join(failed))
    print(f"  all {len(regions)} generators released within "
          f"{time.time() - t0:.1f}s")
