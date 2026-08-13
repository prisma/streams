#!/usr/bin/env python3
"""Provision the ARTIFACT bucket on the platform (one-time).

2026-08-13: the external Tigris-org artifact bucket revoked our key
MID-CAMPAIGN — build-upload's ranged-GET verified at 04:50Z and the
SIN instances' boot downloads were AccessDenied by 04:52Z. Binaries
now live in a platform bucket exactly like the per-run stream-data
buckets, which have never had a credential failure across every
campaign to date.

Writes (all under $SOAK_HOME, never the repo):
  artifact-endpoint.txt  artifact-bucket.txt  binid.txt  binsec.txt
  artifact-platform-receipt.json (project/bucket ids for teardown)
The previous Tigris pointers are kept as *.tigris-bak.
"""
import json, os, urllib.request

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


def main():
    receipt_path = f"{S}/artifact-platform-receipt.json"
    if os.path.exists(receipt_path):
        raise SystemExit(f"receipt already exists: {receipt_path} — "
                         "artifact bucket is already platform-homed")
    proj = call("POST", "/projects",
                {"name": "streams-artifacts-sin", "region": "ap-southeast-1"})["data"]
    bucket = call("POST", "/buckets",
                  {"projectId": proj["id"], "name": "artifacts-sin"})["data"]
    bkey = call("POST", f"/buckets/{bucket['id']}/keys",
                {"role": "read_write", "name": "artifacts"})
    d = bkey["data"]
    json.dump({"projectId": proj["id"], "bucketId": bucket["id"],
               "bucketName": d["bucketName"],
               "purpose": "artifact-binaries", "region": "ap-southeast-1"},
              open(receipt_path, "w"), indent=1)
    for f in ("artifact-endpoint.txt", "artifact-bucket.txt",
              "binid.txt", "binsec.txt"):
        p = f"{S}/{f}"
        if os.path.exists(p) and not os.path.exists(p + ".tigris-bak"):
            os.rename(p, p + ".tigris-bak")
    open(f"{S}/artifact-endpoint.txt", "w").write(d["endpoint"])
    open(f"{S}/artifact-bucket.txt", "w").write(d["bucketName"])
    open(f"{S}/binid.txt", "w").write(d["accessKeyId"])
    open(f"{S}/binsec.txt", "w").write(d["secretAccessKey"])
    os.chmod(f"{S}/binid.txt", 0o600)
    os.chmod(f"{S}/binsec.txt", 0o600)
    print(f"  artifacts: project {proj['id']}  bucket {bucket['id']}  "
          f"name {d['bucketName']}  endpoint {d['endpoint']}")


if __name__ == "__main__":
    main()
