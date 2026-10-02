#!/usr/bin/env python3
"""Provision K2 field cells: per cell, one Compute project in eu-central-1
and one Prisma Bucket with a read_write key (pattern:
bench/soak/provision.py).

    bench/k2cost/field/provision.py <run-id> <cell>...

Names: project and bucket are both `k2c-<run>-<cell>`. Each resource is
written to runs/<run>/resources.json as {name, status: "pending"} BEFORE
its POST and gets its id the moment the platform returns it, so a lost
answer (fieldlib never retries a POST) still leaves a ledger entry that
teardown.py resolves by name (GET /projects, GET /buckets?projectId=). A
re-run resolves a pending entry by name the same way before it creates
anything, so it never creates a second project or bucket. The bucket key (with its secret) goes to
runs/<run>/<cell>/secrets/bkey.json (0600); resources.json holds no
secret. A project's region is set only at creation and cannot be read
back, so the creation record is the evidence: a cell that already has a
project in this run is reused only if that record says eu-central-1.

Platform calls: POST /v1/projects (createDatabase false: the API creates a
Prisma Postgres database by default), POST /v1/buckets,
POST /v1/buckets/{id}/keys.
"""
from __future__ import annotations

import json
import os
import sys

import fieldlib as F


def resolve_pending(run: str, cell: str, what: str, found) -> dict | None:
    """A pending ledger entry (its POST answer was lost, or never came):
    adopt the live resource of exactly its name, or forget the entry."""
    with F.resources(run) as d:
        c = F.cell_res(d, cell)
        if found:
            c[what].update({"id": found["id"], "status": "created", "resolved_by_name": F.utc()})
        else:
            c.pop(what, None)
        return dict(c[what]) if found else None


def provision(run: str, cell: str) -> None:
    F.check_names(run, cell)
    F.read_cell_file(cell)  # refuse a cell that has no definition
    name = F.base_name(run, cell)
    art = F.artifact_receipt()
    doc = F.load_resources(run)
    have = doc["cells"].get(cell, {})
    proj = have.get("project")
    if proj and not proj.get("id"):
        live = [p for p in F.api_list("/projects") if p.get("name") == name]
        proj = resolve_pending(run, cell, "project", live[0] if len(live) == 1 else None)
        if len(live) > 1:
            F.die(f"{cell}: {len(live)} projects are named {name}: resolve by hand")
    if proj and proj.get("createdWithRegion") != F.REGION:
        F.die(f"{cell}: project {proj['id']} was created with region {proj.get('createdWithRegion')!r}")
    have = F.load_resources(run)["cells"].get(cell, {})
    if proj and (have.get("bucket") or {}).get("id") and os.path.exists(os.path.join(F.secrets_dir(run, cell), "bkey.json")):
        F.say(f"  {cell}: already provisioned (project {proj['id']}, bucket {have['bucket']['id']})")
        return
    if not proj:
        with F.resources(run) as d:
            F.cell_res(d, cell)["project"] = {"name": name, "status": "pending", "createdWithRegion": F.REGION,
                                             "created": F.utc()}
        # createDatabase defaults to true: a cell needs no Prisma Postgres.
        _, body = F.api("POST", "/projects", {"name": name, "region": F.REGION, "createDatabase": False})
        p = body["data"]
        with F.resources(run) as d:
            F.cell_res(d, cell)["project"].update({"id": p["id"], "status": "created"})
            if p.get("database"):
                F.cell_res(d, cell)["database"] = {"id": p["database"].get("id"), "name": name}
        proj = {"id": p["id"]}
        F.say(f"  {cell}: project {p['id']} ({name}, {F.REGION})")
    if proj["id"] == art["projectId"]:
        F.die("refusing to use the artifact project")
    bucket = (F.load_resources(run)["cells"][cell]).get("bucket")
    if bucket and not bucket.get("id"):
        live = [b for b in F.api_list(f"/buckets?projectId={proj['id']}")
                if b.get("name") == name and (b.get("project") or {}).get("id") == proj["id"]]
        bucket = resolve_pending(run, cell, "bucket", live[0] if len(live) == 1 else None)
    if not bucket:
        with F.resources(run) as d:
            F.cell_res(d, cell)["bucket"] = {"name": name, "status": "pending", "created": F.utc()}
        _, body = F.api("POST", "/buckets", {"projectId": proj["id"], "name": name})
        b = body["data"]
        with F.resources(run) as d:
            F.cell_res(d, cell)["bucket"].update({"id": b["id"], "status": "created"})
        bucket = {"id": b["id"]}
        F.say(f"  {cell}: bucket {b['id']}")
    if bucket["id"] == art["bucketId"]:
        F.die("refusing to use the artifact bucket")
    # A key whose answer is lost is orphaned inside the bucket and goes
    # with it at teardown; the next run of this tool mints another.
    _, key = F.api("POST", f"/buckets/{bucket['id']}/keys", {"role": "read_write", "name": name})
    F.write_secret(os.path.join(F.secrets_dir(run, cell), "bkey.json"), json.dumps(key))
    data = key["data"]
    with F.resources(run) as d:
        b = F.cell_res(d, cell)["bucket"]
        b.update({"bucketName": data["bucketName"], "endpoint": data["endpoint"],
                  "keyId": data.get("id") or data.get("keyId")})
    F.say(f"  {cell}: key for {data['bucketName']} at {data['endpoint']} -> secrets/bkey.json")


def main() -> None:
    args = sys.argv[1:]
    if len(args) < 2:
        F.die("usage: provision.py <run-id> <cell>...", 2)
    run, cells = args[0], args[1:]
    F.check_names(run)
    for c in cells:
        provision(run, c)


if __name__ == "__main__":
    main()
