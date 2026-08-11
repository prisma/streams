#!/usr/bin/env python3
"""Distinguish a live campaign server from a platform edge 404 or a
stale URL. A bare 200 on /health is NOT sufficient — the 2026-08-11 run
read health=200 from an OLD campaign's server through a stale URL file.
Require the R25 debug marker (maintenance_shards), which only this
build serves, and confirm the generator answers with its stats shape.
"""
import json, os, sys, time, urllib.request

S = os.environ["SOAK_HOME"]
AUTH = open(f"{S}/auth.txt").read().strip()

def get(url, auth=False, timeout=30):
    req = urllib.request.Request(url, headers=(
        {"Authorization": f"Bearer {AUTH}"} if auth else {}))
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()

def verify(region):
    server = open(f"{S}/url-server-{region}.txt").read().strip()
    gen = open(f"{S}/url-gen-{region}.txt").read().strip()
    st, body = get(f"{server}/livez")
    assert st == 200 and body == b"alive", f"{region}: livez {st} {body!r}"
    st, body = get(f"{server}/v1/debug/load", auth=True)
    d = json.loads(body)
    assert "maintenance_shards" in d, (
        f"{region}: no R25 marker — WRONG BUILD or stale URL")
    st, body = get(f"{gen}/")
    g = json.loads(body)
    assert "ok" in g or isinstance(g, list), f"{region}: gen shape {body[:80]!r}"
    print(f"  {region}: server live (R25 build), generator answering")

if __name__ == "__main__":
    failures = []
    for region in sys.argv[1:]:
        for attempt in range(10):
            try:
                verify(region)
                break
            except Exception as e:
                if attempt == 9:
                    failures.append(f"{region}: {e}")
                else:
                    time.sleep(20)
    if failures:
        sys.exit("VERIFY FAILED:\n  " + "\n  ".join(failures))
