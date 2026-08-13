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
RUN_ID = os.environ.get("SOAK_RUN_ID", "")

def manifest():
    """The campaign's upload manifest (sha256 per S3 key), if present."""
    try:
        return json.load(open(f"{S}/results/{RUN_ID}/binaries.json"))
    except Exception:
        return {}

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
    # R26-9: the marker alone admits ANY post-R25 binary. The wrapper
    # hashes what it actually downloaded; require the digest to match
    # THIS campaign's upload manifest exactly.
    man = manifest()
    expected = {v["sha256"] for k, v in man.items() if "streams" in k}
    got = d.get("binary_sha256", "unknown")
    if expected:
        assert got in expected, (
            f"{region}: server binary sha {got[:16]} not in this "
            f"campaign's manifest — a stale or foreign build is serving")
    # R29: compare the FULL identity, not just the digest — git commit
    # and build timestamp from the manifest, /readyz identity headers,
    # and a nonempty boot id.
    commits = {v.get("gitCommit") for v in man.values() if v.get("gitCommit")}
    if commits:
        assert d.get("git_commit") in commits, (
            f"{region}: git commit {d.get('git_commit','')[:12]} not in manifest")
        builds = {str(v.get("buildUnix")) for v in man.values() if v.get("buildUnix")}
        if builds:
            assert str(d.get("build_unix")) in builds, (
                f"{region}: build timestamp mismatch")
        req = urllib.request.Request(f"{server}/readyz")
        with urllib.request.urlopen(req, timeout=30) as r:
            hdr_git = r.headers.get("x-streams-git", "")
            hdr_boot = r.headers.get("x-streams-boot-id", "")
        assert hdr_git in commits, f"{region}: /readyz header commit mismatch"
        assert hdr_boot and hdr_boot == d.get("boot_id"), (
            f"{region}: boot id missing or inconsistent between /readyz and debug/load")
    st, body = get(f"{gen}/")
    g = json.loads(body)
    assert "ok" in g or isinstance(g, list), f"{region}: gen shape {body[:80]!r}"
    gen_expected = {v["sha256"] for k, v in man.items() if "awsbench" in k}
    if gen_expected and isinstance(g, list) and g:
        gsha = g[-1].get("binSha256", "")
        assert gsha in gen_expected, (
            f"{region}: generator binary sha {gsha[:16]} not in manifest")
    print(f"  {region}: server live (build verified), generator answering")

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
