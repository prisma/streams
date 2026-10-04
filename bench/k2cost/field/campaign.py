#!/usr/bin/env python3
"""The K2 field campaign project: ONE Compute project in eu-central-1 that
holds every cell of every run and the artifact bucket (campaign-project
mode), for a workspace that holds other things.

    bench/k2cost/field/campaign.py init <name>       # k2c-*, e.g. k2c-cost
    bench/k2cost/field/campaign.py show
    bench/k2cost/field/campaign.py verify
    bench/k2cost/field/campaign.py destroy [--yes]   # a dry run without --yes

init writes $K2_FIELD_HOME/campaign.json with the project as `pending`
BEFORE its POST (createDatabase false; a POST is never retried) and its id
the moment the answer comes; a re-run resolves a pending entry by exact
name (GET /v1/projects), and a project of that name that no pending entry
accounts for is refused, never adopted. The project is trusted only after
GET /v1/projects/{id} shows the recorded name. The artifact bucket
`<name>-artifacts` is created the same way inside the project, with a
read_write key, and the files the tools read are written to the field
home: artifact-platform-receipt.json (first; no secret), artifact-endpoint
.txt, artifact-bucket.txt, binid.txt, binsec.txt (0600). Files that are
not this campaign's are never overwritten. init also writes
`campaign-required`, so the field home refuses per-cell mode from then on.

show prints the record. verify lists ONLY the campaign project's services
and buckets, with the run and cell whose ledger names each one, and the
names of the project's production variables: the last deploy's set,
which no run teardown removes (secret values included; the gates refuse
every env-variable mutation, so only a deploy restating or unsetting them,
or the project's deletion, changes them).

destroy tears the whole campaign down: it refuses while the project holds
any service or bucket whose name does not start k2c- (or any database:
the campaign never creates one); then it destroys every k2c- service and
bucket in it (the artifact bucket included), each first proven live in the
project (fieldlib.campaign_owns), deletes the project only once a fresh
listing shows it empty, and verifies by GET: the project, every bucket and
every service answer 404. The run ledgers mark what it destroyed; the
report goes to results/campaign-destroy-<utc>.json; campaign.json becomes
`destroyed` (no tool runs against it again).

Platform calls: GET /v1/projects (init: exact name only), POST
/v1/projects, GET /v1/projects/{campaign}, GET /v1/buckets?projectId=
<campaign>, GET /v1/environment-variables?projectId=<campaign> (verify;
a row of another project refuses), POST /v1/buckets, GET /v1/buckets/{id}, POST
/v1/buckets/{id}/keys, GET /v1/databases?projectId=<campaign>, GET
/v1/services/{id}, `compute services list --project <campaign>`, `compute
services destroy <verified id>`, DELETE /v1/buckets/{verified id}, DELETE
/v1/projects/{campaign}. fieldlib.gate_api / gate_cli refuse any other.
"""
from __future__ import annotations

import contextlib
import json
import os
import sys
import traceback

import fieldlib as F

ARTIFACT_FILES = ("artifact-platform-receipt.json", "artifact-endpoint.txt", "artifact-bucket.txt",
                  "binid.txt", "binsec.txt")
USAGE = "usage: campaign.py init <k2c-name> | show | verify | destroy [--yes]"


def save(doc: dict) -> None:
    doc["updated"] = F.utc()
    F.write_json(F.campaign_path(), doc)


def marker_path() -> str:
    return os.path.join(F.FIELD, "campaign-required")


def check_files(name: str) -> None:
    present = [f for f in ARTIFACT_FILES if os.path.exists(os.path.join(F.FIELD, f))]
    rc = F.read_json(os.path.join(F.FIELD, "artifact-platform-receipt.json")) or {}
    if present and rc.get("campaign") != name:
        F.die(f"{present} exist in {F.FIELD} and are not campaign {name}'s: refusing to overwrite them")


def record_project(doc: dict, p: dict, how: str) -> None:
    doc["project"].update({"id": p["id"], "status": "created", how: F.utc(), "defaultRegion": p.get("defaultRegion"),
                           "workspace": {k: (p.get("workspace") or {}).get(k) for k in ("id", "name")}})
    if p.get("database"):
        doc["project"]["database"] = {"id": p["database"].get("id")}
        F.say(f"  WARNING: the platform added database {p['database'].get('id')} to {p['id']} (goes with the project)")
    save(doc)


def ensure_project(doc: dict, fresh: bool) -> None:
    name = doc["name"]
    if not fresh:  # a pending entry: its POST answer was lost, or it was never sent
        live = [p for p in F.api_list("/projects") if p.get("name") == name]
        if len(live) > 1:
            F.die(f"{len(live)} projects are named {name}: resolve by hand")
        if live:
            record_project(doc, live[0], "resolved_by_name")
            F.say(f"  project {live[0]['id']} ({name}) resolved by name")
            return
    try:
        _, body = F.api("POST", "/projects", {"name": name, "region": F.REGION, "createDatabase": False})
    except F.OutcomeUnknown as e:
        F.die(f"{e}: campaign.json keeps the pending entry; re-run `campaign.py init {name}` to resolve it by name")
    record_project(doc, body["data"], "created_at")
    F.say(f"  project {doc['project']['id']} ({name}, {F.REGION}, workspace {doc['project']['workspace']})")


def ensure_bucket(doc: dict) -> dict:
    pid, bname = doc["project"]["id"], f"{doc['name']}-artifacts"

    def named() -> list:
        return [b for b in F.api_list(f"/buckets?projectId={pid}")
                if b.get("name") == bname and (b.get("project") or {}).get("id") == pid]
    ab = doc.get("artifact_bucket") or {}
    if ab and not ab.get("id"):
        live = named()
        if len(live) > 1:
            F.die(f"{len(live)} buckets in {pid} are named {bname}: resolve by hand")
        if live:
            ab.update({"id": live[0]["id"], "status": "created", "resolved_by_name": F.utc()})
            save(doc)
        else:
            ab = {}
    if ab.get("id"):
        state, why = F.campaign_owns("bucket", ab["id"], name=bname)
        if state != "live":
            F.die(f"artifact bucket {ab['id']}: {why or 'gone (404)'}")
        return ab
    if named():
        F.die(f"a bucket named {bname} already exists in {pid} and no pending entry says init made it")
    doc["artifact_bucket"] = {"name": bname, "status": "pending", "created": F.utc()}
    save(doc)
    try:
        _, body = F.api("POST", "/buckets", {"projectId": pid, "name": bname})
    except F.OutcomeUnknown as e:
        F.die(f"{e}: campaign.json keeps the pending bucket; re-run `campaign.py init {doc['name']}`")
    b = body["data"]
    doc["artifact_bucket"].update({"id": b["id"], "status": "created"})
    save(doc)
    F.register_created_bucket(b)
    F.say(f"  artifact bucket {b['id']} ({bname})")
    return doc["artifact_bucket"]


def write_artifact_files(doc: dict, ab: dict) -> None:
    # A key whose answer is lost is orphaned in the bucket (it goes with the
    # bucket); a re-run mints another.
    _, key = F.api("POST", f"/buckets/{ab['id']}/keys", {"role": "read_write", "name": ab["name"]})
    d = key["data"]
    path = lambda n: os.path.join(F.FIELD, n)  # noqa: E731
    F.write_json(path("artifact-platform-receipt.json"), {
        "projectId": doc["project"]["id"], "bucketId": ab["id"], "bucketName": d["bucketName"],
        "purpose": "artifact-binaries", "region": F.REGION, "campaign": doc["name"]})
    for name, value in (("artifact-endpoint.txt", d["endpoint"]), ("artifact-bucket.txt", d["bucketName"]),
                        ("binid.txt", d["accessKeyId"]), ("binsec.txt", d["secretAccessKey"])):
        F.write_secret(path(name), value)
    ab.update({"bucketName": d["bucketName"], "endpoint": d["endpoint"], "keyId": d.get("id")})


def init(name: str) -> int:
    if not F.CAMPAIGN_RE.match(name):
        F.die(f"campaign name {name!r} must match {F.CAMPAIGN_RE.pattern} (start with k2c-)")
    if os.path.exists(marker_path()) and open(marker_path(), encoding="utf-8").read().strip() != name:
        F.die(f"{F.FIELD} is reserved for campaign {open(marker_path(), encoding='utf-8').read().strip()!r}")
    doc = F.campaign_doc()
    if doc and doc.get("name") != name:
        F.die(f"campaign.json is campaign {doc.get('name')!r}, not {name!r}")
    if doc and doc.get("status") == "ready":
        F.say(f"campaign {name} is ready (project {doc['project']['id']})")
        return 0
    if doc and doc.get("status") != "pending":
        F.die(f"campaign {name} is {doc.get('status')!r}: move campaign.json aside to start another")
    check_files(name)
    fresh = doc is None
    if fresh:
        taken = [p.get("id") for p in F.api_list("/projects") if p.get("name") == name]
        if taken:
            F.die(f"project(s) {taken} named {name} already exist and no pending entry says init made them")
        doc = {"name": name, "region": F.REGION, "status": "pending", "created": F.utc(),
               "project": {"name": name, "status": "pending", "createdWithRegion": F.REGION, "created": F.utc()}}
        save(doc)  # the pending entry, BEFORE the POST
    if not doc["project"].get("id"):
        ensure_project(doc, fresh)
    live = F.campaign_check(doc)  # trusted only now: live, under the recorded name
    if live.get("defaultRegion") not in (None, F.REGION):
        F.die(f"project {doc['project']['id']} has defaultRegion {live.get('defaultRegion')!r}, not {F.REGION}")
    ab = ensure_bucket(doc)
    write_artifact_files(doc, ab)
    doc["status"] = "ready"
    save(doc)
    if not os.path.exists(marker_path()):
        with open(marker_path(), "w", encoding="utf-8") as f:
            f.write(name + "\n")
    F.say(f"campaign {name} ready: project {doc['project']['id']}, artifact bucket {ab['id']} "
          f"({ab['bucketName']}); artifact files and campaign.json in {F.FIELD}")
    return 0


def show() -> int:
    doc = F.campaign_doc()
    if not doc:
        F.say(f"no campaign.json in {F.FIELD}" + (" (campaign-required)" if os.path.exists(marker_path()) else ""))
        return 1
    F.say(json.dumps(doc, indent=1, sort_keys=True))
    F.say("artifact files: " + ", ".join(f"{f} {'present' if os.path.exists(os.path.join(F.FIELD, f)) else 'MISSING'}"
                                         for f in ARTIFACT_FILES))
    return 0


def inventory(pid: str) -> tuple:
    """(services, buckets, databases) of the campaign project ONLY: each
    listing is scoped to it and every item is filtered by its project."""
    svcs = F.services_in(pid)
    bkts = [b for b in F.api_list(f"/buckets?projectId={pid}") if (b.get("project") or {}).get("id") == pid]
    dbs = [d for d in F.api_list(f"/databases?projectId={pid}") if (d.get("project") or {}).get("id") == pid]
    return svcs, bkts, dbs


def label(doc: dict, owners: dict, rid: str) -> str:
    if rid == (doc.get("artifact_bucket") or {}).get("id"):
        return "artifact bucket"
    return f"run {owners[rid]}" if rid in owners else "UNLEDGERED"


def verify() -> int:
    doc = F.campaign()
    live = F.campaign_check(doc)
    pid = doc["project"]["id"]
    svcs = F.services_in(pid)
    bkts = [b for b in F.api_list(f"/buckets?projectId={pid}") if (b.get("project") or {}).get("id") == pid]
    owners = F.foreign_ids("")  # every run's ledger ids -> run/cell
    F.say(f"campaign {doc['name']}: project {pid} {live.get('name')!r} (workspace "
          f"{(live.get('workspace') or {}).get('name')!r}, defaultRegion {live.get('defaultRegion')})")
    alien = []
    for kind, items in (("service", svcs), ("bucket", bkts)):
        for x in sorted(items, key=lambda x: x.get("name") or ""):
            ok = (x.get("name") or "").startswith("k2c-")
            alien += [] if ok else [f"{kind} {x.get('id')} {x.get('name')!r}"]
            F.say(f"  {kind:7} {x.get('id')}  {x.get('name')}  ({label(doc, owners, x.get('id'))})"
                  f"{'' if ok else '  NOT k2c-'}")
    F.say(f"{len(svcs)} service(s), {len(bkts)} bucket(s) in {pid}" + (f"; NOT k2c-: {alien}" if alien else ""))
    try:  # names only: the listing returns no values
        names = sorted(str(r.get("key")) for r in F.project_env_rows(pid))
    except F.ScopeError as e:
        F.say(f"REFUSING: {e}")
        return 1
    F.say(f"project env: {len(names)} production variable(s) {names}: the last deploy's set, secrets included; "
          f"no teardown removes them (each deploy restates or unsets every one; campaign destroy deletes them "
          f"with the project)")
    return 1 if alien else 0


def mark_ledgers(gone: set) -> None:
    """Every run ledger: what campaign destroy removed is deleted."""
    for run in sorted(os.listdir(os.path.join(F.FIELD, "runs")) if os.path.isdir(os.path.join(F.FIELD, "runs")) else []):
        if not os.path.exists(os.path.join(F.FIELD, "runs", run, "resources.json")):
            continue
        with F.resources(run) as d:
            for c in d["cells"].values():
                for rec in [c.get("bucket") or {}] + list((c.get("services") or {}).values()):
                    if rec.get("id") in gone and rec.get("status") != "deleted":
                        rec.update({"status": "deleted", "torn_down": F.utc(), "by": "campaign destroy"})


def destroy_all(doc: dict, svcs: list, bkts: list, report: dict) -> set:
    """Services, then (only if every service went) buckets: each proven
    live in the campaign project first."""
    gone = set()
    art = (doc.get("artifact_bucket") or {}).get("id")
    for kind, items in (("service", svcs), ("bucket", bkts)):
        if kind == "bucket" and report["failures"]:
            report["failures"].append(f"buckets {[b.get('id') for b in bkts]} held: a service did not go")
            break
        for x in items:
            try:
                state, why = F.campaign_owns(kind, x["id"], name=x.get("name"), prefix="k2c-")
                if state == "refuse":
                    raise RuntimeError(why)
                if state == "live" and kind == "service":
                    p = F.cli(["services", "destroy", x["id"], "--timeout", "300"], timeout=600)
                    if p.returncode != 0:
                        raise RuntimeError(f"rc={p.returncode}: {(p.stderr or p.stdout)[-300:]}")
                elif state == "live":
                    with F.permit("artifact-bucket-delete") if x["id"] == art else contextlib.nullcontext():
                        F.api("DELETE", f"/buckets/{x['id']}", allow=(404,))
                report[kind + "s"][x["id"]] = {"name": x.get("name"), "result": "destroyed" if state == "live" else "gone"}
                gone.add(x["id"])
            except Exception as e:  # noqa: BLE001 - one object's failure never stops the others
                report[kind + "s"][x["id"]] = {"name": x.get("name"), "result": f"FAILED {str(e)[:300]}"}
                report["failures"].append(f"{kind} {x['id']}: {str(e)[:300]}")
            F.say(f"  {kind} {x['id']} {x.get('name')}: {report[kind + 's'][x['id']]['result']}")
    return gone


def destroy(yes: bool) -> int:
    doc = F.campaign(require_ready=False)
    if not doc or not doc["project"].get("id"):
        F.die(f"no campaign project recorded in {F.campaign_path()}")
    pid = doc["project"]["id"]
    st, _ = F.api("GET", f"/projects/{pid}", allow=(404, 403))
    if st == 404:
        F.say(f"campaign project {pid} already answers 404")
        if yes:
            doc.update({"status": "destroyed", "destroyed": F.utc()})
            save(doc)
        return 0
    F.campaign_check(doc)  # refuses unless it is live under its recorded name
    svcs, bkts, dbs = inventory(pid)
    owners = F.foreign_ids("")
    alien = [f"service {s.get('id')} {s.get('name')!r}" for s in svcs if not (s.get("name") or "").startswith("k2c-")]
    alien += [f"bucket {b.get('id')} {b.get('name')!r}" for b in bkts if not (b.get("name") or "").startswith("k2c-")]
    alien += [f"database {d.get('id')} {d.get('name')!r}" for d in dbs]
    F.say(f"== campaign {doc['name']}: project {pid}")
    for kind, items in (("service", svcs), ("bucket", bkts)):
        for x in items:
            F.say(f"  {kind:7} {x.get('id')}  {x.get('name')}  ({label(doc, owners, x.get('id'))})")
    F.say(f"  project {pid}  {doc['name']}  (deleted last, once empty)")
    if alien:
        F.say(f"REFUSING: the campaign project holds what the campaign did not make: {alien}")
        return 1
    if not yes:
        F.say("\nDRY RUN: re-run with --yes to destroy exactly the resources above and the project.")
        return 0
    doc["status"] = "destroying"
    save(doc)
    report = {"campaign": doc["name"], "project": pid, "started": F.utc(), "services": {}, "buckets": {},
              "failures": [], "verified": False}
    ok = False
    try:
        gone = destroy_all(doc, svcs, bkts, report)
        mark_ledgers(gone)
        left_s, left_b, left_d = inventory(pid)
        report["left"] = [x.get("id") for x in left_s + left_b + left_d]
        if report["left"]:
            report["failures"].append(f"project {pid} left standing: it still holds {report['left']}")
        else:
            with F.permit("campaign-project-delete"):
                report["project_delete"] = F.api("DELETE", f"/projects/{pid}", allow=(404, 409))[0]
        report["project_get"] = F.api("GET", f"/projects/{pid}", allow=(404, 403))[0]
        report["object_gets"] = {rid: F.api("GET", f"/{k}/{rid}", allow=(404, 403))[0]
                                 for k in ("services", "buckets") for rid in report[k]}
        ok = (report["project_get"] == 404 and not report["failures"]
              and all(v == 404 for v in report["object_gets"].values()))
    except Exception as e:  # noqa: BLE001 - recorded; the report is still written
        report["failures"].append(f"{type(e).__name__}: {e}")
        report["trace"] = traceback.format_exc()[-1500:]
    finally:
        report.update({"verified": ok, "finished": F.utc()})
        rdir = os.path.join(F.FIELD, "results")
        os.makedirs(rdir, exist_ok=True)
        F.write_json(os.path.join(rdir, f"campaign-destroy-{report['started'].replace(':', '')}.json"), report)
    if ok:
        doc.update({"status": "destroyed", "destroyed": F.utc()})
        save(doc)
    F.say(f"\ncampaign destroy {'VERIFIED' if ok else 'INCOMPLETE'}: project GET {report.get('project_get')}, "
          f"failures {report['failures']}")
    return 0 if ok else 1


def main() -> None:
    F.banner("campaign")
    a = sys.argv[1:]
    if len(a) == 2 and a[0] == "init":
        sys.exit(init(a[1]))
    if a == ["show"]:
        sys.exit(show())
    if a == ["verify"]:
        sys.exit(verify())
    if a[:1] == ["destroy"] and a[1:] in ([], ["--yes"]):
        sys.exit(destroy(a[1:] == ["--yes"]))
    F.die(USAGE, 2)


if __name__ == "__main__":
    main()
