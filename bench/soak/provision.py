#!/usr/bin/env python3
"""Region-pinned provisioning with immutable creation receipts.

A project's region is set ONLY at create time and cannot be read back
(`defaultRegion` is null on every read — BUCKETS-SINGLE-REGION.md), so a
wrongly-homed bucket is undetectable after the fact: it silently
measures cross-region storage. The receipt written here at creation is
therefore the source of truth, and a project without a matching receipt
is never reused.
"""
import json, os, sys, urllib.request

S = os.environ["SOAK_HOME"]
TOKEN = open(f"{S}/platform-token.txt").read().strip()
API = "https://api.prisma.io/v1"

def call(method, path, body=None):
    req = urllib.request.Request(f"{API}{path}", method=method,
        headers={"Authorization": f"Bearer {TOKEN}",
                 "Content-Type": "application/json",
                 "User-Agent": "curl/8.7.1"},
        data=json.dumps(body).encode() if body else None)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)

def provision(run_id, region):
    receipt_path = f"{S}/receipts/{region}.json"
    if os.path.exists(receipt_path):
        rc = json.load(open(receipt_path))
        if rc.get("createdWithRegion") == region:
            print(f"  {region}: receipt exists (project {rc['projectId']}) — reusing")
            open(f"{S}/proj-{region}.txt", "w").write(rc["projectId"])
            return
        sys.exit(f"FATAL: receipt for {region} claims region "
                 f"{rc.get('createdWithRegion')!r} — refusing to reuse")
    proj = call("POST", "/projects",
        {"name": f"streams-{run_id}-{region}", "region": region})["data"]
    bucket = call("POST", "/buckets",
        {"projectId": proj["id"], "name": f"{run_id}-{region}"})["data"]
    bkey = call("POST", f"/buckets/{bucket['id']}/keys",
        {"role": "read_write", "name": run_id})
    receipt = {
        "runId": run_id,
        "region": region,
        "projectId": proj["id"],
        "bucketId": bucket["id"],
        "bucketName": bkey["data"]["bucketName"],
        "createdWithRegion": region,
    }
    os.makedirs(f"{S}/receipts", exist_ok=True)
    json.dump(receipt, open(receipt_path, "w"), indent=1)
    open(f"{S}/proj-{region}.txt", "w").write(proj["id"])
    json.dump(bkey, open(f"{S}/bkey-{region}.json", "w"))
    open(f"{S}/bucket-{region}.id", "w").write(bucket["id"])
    print(f"  {region}: project {proj['id']}  bucket {bucket['id']}  receipt written")

if __name__ == "__main__":
    args = sys.argv[1:]
    assert args[0] == "--run-id", "usage: provision.py --run-id <id> <region>..."
    run_id, regions = args[1], args[2:]
    for r in regions:
        provision(run_id, r)
