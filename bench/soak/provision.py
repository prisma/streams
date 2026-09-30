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
REGIONS = ("us-east-1", "us-west-1", "eu-central-1", "eu-west-3",
           "ap-southeast-1", "ap-northeast-1")


def region_of(cell):
    """A cell is a Compute region, or a region with an arm suffix
    (`eu-central-1-tick`): two arms of one A/B run side by side in one
    region, each in its own project and bucket (README invariant 6)."""
    if cell in REGIONS:
        return cell
    for r in REGIONS:
        if cell.startswith(r + "-"):
            return r
    raise SystemExit(f"FATAL: {cell!r} names no Compute region")

def call(method, path, body=None):
    req = urllib.request.Request(f"{API}{path}", method=method,
        headers={"Authorization": f"Bearer {TOKEN}",
                 "Content-Type": "application/json",
                 "User-Agent": "curl/8.7.1"},
        data=json.dumps(body).encode() if body else None)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)

def provision(run_id, cell):
    region = region_of(cell)
    receipt_path = f"{S}/receipts/{cell}.json"
    if os.path.exists(receipt_path):
        rc = json.load(open(receipt_path))
        # R26-9: a receipt is reusable ONLY within its own run. A failed
        # preserved campaign followed by a new SOAK_RUN_ID must not
        # silently adopt the old project and namespace — that is how a
        # "fresh" run reads a stale specimen's data.
        if rc.get("runId") != run_id:
            sys.exit(f"FATAL: receipt for {cell} belongs to run "
                     f"{rc.get('runId')!r}, not {run_id!r} — tear the old "
                     f"campaign down (or delete the receipt) first")
        if rc.get("createdWithRegion") == region:
            print(f"  {cell}: receipt exists (project {rc['projectId']}) — reusing")
            open(f"{S}/proj-{cell}.txt", "w").write(rc["projectId"])
            open(f"{S}/proj-{cell}.txt.campaign", "w").write(run_id)
            return
        sys.exit(f"FATAL: receipt for {cell} claims region "
                 f"{rc.get('createdWithRegion')!r} — refusing to reuse")
    proj = call("POST", "/projects",
        {"name": f"streams-{run_id}-{cell}", "region": region})["data"]
    # R26-9: record the project THE MOMENT it exists — a partial
    # provisioning failure (bucket/key call dying) must leave enough
    # state for teardown to find and delete the orphan project.
    # The campaign STAMP is written here too, not only at deploy:
    # teardown refuses stamp mismatches, so a project that provisioned
    # but never deployed was undeletable by its own run id (the
    # 20260814 wave orphaned exactly such a project and wedged its
    # region's receipt).
    os.makedirs(f"{S}/receipts", exist_ok=True)
    open(f"{S}/proj-{cell}.txt", "w").write(proj["id"])
    open(f"{S}/proj-{cell}.txt.campaign", "w").write(run_id)
    json.dump({"runId": run_id, "region": cell, "projectId": proj["id"],
               "partial": True, "createdWithRegion": region},
              open(receipt_path, "w"), indent=1)
    bucket = call("POST", "/buckets",
        {"projectId": proj["id"], "name": f"{run_id}-{cell}"})["data"]
    bkey = call("POST", f"/buckets/{bucket['id']}/keys",
        {"role": "read_write", "name": run_id})
    receipt = {
        "runId": run_id,
        "region": cell,
        "projectId": proj["id"],
        "bucketId": bucket["id"],
        "bucketName": bkey["data"]["bucketName"],
        "createdWithRegion": region,
    }
    os.makedirs(f"{S}/receipts", exist_ok=True)
    json.dump(receipt, open(receipt_path, "w"), indent=1)
    open(f"{S}/proj-{cell}.txt", "w").write(proj["id"])
    json.dump(bkey, open(f"{S}/bkey-{cell}.json", "w"))
    open(f"{S}/bucket-{cell}.id", "w").write(bucket["id"])
    print(f"  {cell} ({region}): project {proj['id']}  bucket {bucket['id']}  receipt written")

if __name__ == "__main__":
    args = sys.argv[1:]
    assert args[0] == "--run-id", "usage: provision.py --run-id <id> <region>..."
    run_id, regions = args[1], args[2:]
    for r in regions:
        provision(run_id, r)
