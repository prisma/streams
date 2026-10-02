#!/usr/bin/env python3
"""Guarded teardown of one K2 field run. Entry points:

    bench/k2cost/field/teardown.sh <run-id> [--yes] [cell...]
    bench/k2cost/field/teardown.sh <run-id> --expired [--yes]
    bench/k2cost/field/teardown.sh <run-id> --service <name> [--yes]

Guards, every one checked before any DELETE, against the live platform and
not only the ledger:
- it acts only on what runs/<run>/resources.json lists;
- a project or bucket must be named k2c-<run>-... in the ledger AND live
  (GET /v1/projects/{id}, GET /v1/buckets/{id}), with the live name equal to
  the ledger's; a bucket's live project must be the ledger's project;
- never the artifact project or bucket (artifact-platform-receipt.json
  projectId and bucketId), and never an id that another run's ledger lists;
- a service must be found live, by its ledger name (and ledger id, when
  recorded), in its guarded project. An unlisted service in a run project
  is a refusal, and so is a project whose services cannot be listed: that
  cell's bucket and project are then left standing, loudly.
A pending ledger entry (a POST whose answer was lost, provision.py) is
resolved by exact name first: GET /v1/projects, GET /v1/buckets.

Without --yes it prints the plan and stops. With --yes:
1. `compute services destroy` for every listed service of every cell first
   (stops and deletes its versions, then the service), so no KEEP_AWAKE
   instance waits behind another cell's failure;
2. per cell, only when it has no refusal and all its services are gone:
   DELETE /v1/buckets/{id}, then DELETE /v1/projects/{id};
3. the run's own objects in the artifact bucket (feeds and generator header
   files under k2c/<run>/, each listed in the ledger; binaries under bin/
   stay; the artifact bucket itself is never touched).
Every step is isolated: a failure is recorded and the others continue.
Verification: every listed project and bucket answers 404 (or 403), every
service destroy succeeded, no project, bucket or database named
k2c-<run>-* (or of a run project) is left in the workspace, and every
deleted artifact object answers 404. results/<run>/teardown.json is
written in every case (a crash included); exit 1 on any failure.

--expired destroys only KEEP_AWAKE services whose ledger expiry has passed
(observe.py runs it every minute); --service destroys one ledger-listed
service (gen.py --replace). Both use the same guards, mark the service
deleted in the ledger and append a line to results/<run>/teardown-partial.jsonl.
"""
from __future__ import annotations

import sys
import traceback

import fieldlib as F

USAGE = ("usage: teardown.sh <run-id> [--yes] [cell...] | <run-id> --expired [--yes] | "
         "<run-id> --service <name> [--yes]")


class Guard:
    """Which platform objects this run may delete."""

    def __init__(self, run: str):
        self.run, self.prefix = run, f"k2c-{run}-"
        rc = F.artifact_receipt()
        self.art = {rc["projectId"]: "the artifact project", rc["bucketId"]: "the artifact bucket"}
        self.foreign = F.foreign_ids(run)

    def static(self, kind: str, rid: str | None, name: str | None) -> str | None:
        """A refusal reason from the ledger entry alone, or None."""
        if not (name or "").startswith(self.prefix):
            return f"{kind} {rid}: ledger name {name!r} lacks {self.prefix}"
        if rid in self.art:
            return f"{kind} {rid}: {self.art[rid]}"
        if rid in self.foreign:
            return f"{kind} {rid}: listed by run {self.foreign[rid]}"
        return None

    def live(self, kind: str, entry: dict, project_id: str | None = None) -> tuple:
        """("live" | "gone" | "refuse", reason). A bucket's live project
        must be `project_id`."""
        rid = entry["id"]
        st, body = F.api("GET", f"/{kind}s/{rid}", allow=(404, 403))
        if st == 404:
            return "gone", None
        if st != 200:
            return "refuse", f"{kind} {rid}: GET answered {st}; cannot verify it"
        live = (body or {}).get("data") or {}
        name = live.get("name") or ""
        if live.get("id") != rid or name != entry.get("name") or not name.startswith(self.prefix):
            return "refuse", f"{kind} {rid}: live name {name!r} is not the ledger's {entry.get('name')!r}"
        if kind == "bucket" and (live.get("project") or {}).get("id") != project_id:
            return "refuse", (f"bucket {rid}: live project {(live.get('project') or {}).get('id')} is not the "
                              f"ledger's {project_id}")
        return "live", None


def resolve_pending(run: str, cell: str, what: str, entry: dict, project_id: str | None) -> dict | None:
    """Adopt the one live resource named like a pending entry (or None)."""
    if what == "project":
        live = [p for p in F.api_list("/projects") if p.get("name") == entry.get("name")]
    else:
        live = [b for b in F.api_list("/buckets") if b.get("name") == entry.get("name")
                and (b.get("project") or {}).get("id") == project_id]
    if len(live) > 1:
        raise RuntimeError(f"{len(live)} live {what}s are named {entry.get('name')}")
    if not live:
        return None
    with F.resources(run) as d:
        F.cell_res(d, cell)[what].update({"id": live[0]["id"], "resolved_by_name": F.utc()})
    return {**entry, "id": live[0]["id"]}


def plan_cell(run: str, cell: str, c: dict, g: Guard) -> dict:
    item = {"cell": cell, "refusals": [], "services": [], "notes": [], "project": None, "bucket": None}
    p = c.get("project")
    pid = (p or {}).get("id")
    if p and p.get("status") != "deleted":
        if not pid:
            p = resolve_pending(run, cell, "project", p, None)
            pid = (p or {}).get("id")
            if not p:
                item["notes"].append("project: pending entry, no live project of its name")
        if p:
            why = g.static("project", pid, p.get("name"))
            state, why = ("refuse", why) if why else g.live("project", p)
            if state == "refuse":
                item["refusals"].append(why)
            elif state == "gone":
                item["notes"].append(f"project {pid}: already gone (404)")
                item["project_gone"] = pid
            else:
                item["project"] = p
    b = c.get("bucket")
    if b and b.get("status") != "deleted":
        if not b.get("id"):
            b = resolve_pending(run, cell, "bucket", b, pid)
            if not b:
                item["notes"].append("bucket: pending entry, no live bucket of its name")
        if b:
            why = g.static("bucket", b["id"], b.get("name"))
            state, why = ("refuse", why) if why else g.live("bucket", b, pid)
            if state == "refuse":
                item["refusals"].append(why)
            elif state == "gone":
                item["notes"].append(f"bucket {b['id']}: already gone (404)")
                item["bucket_gone"] = b["id"]
            else:
                item["bucket"] = b
    listed = {}
    for s in (c.get("services") or {}).values():
        if s.get("status") == "deleted":
            continue
        if not s.get("name", "").startswith(g.prefix):
            item["refusals"].append(f"ledger service {s.get('id')} ({s.get('name')}) lacks {g.prefix}")
        else:
            listed[s["name"]] = s
    item["listed"] = sorted(listed)
    if item["project"]:
        try:
            live = F.services_in(item["project"]["id"])
        except Exception as e:  # noqa: BLE001 - an unknown service set is a refusal
            live = []
            item["refusals"].append(f"services of {item['project']['id']} could not be listed: {str(e)[:200]}")
        for s in live:
            rec = listed.get(s.get("name"))
            if not rec:
                item["refusals"].append(f"unlisted service {s.get('id')} ({s.get('name')}) in {item['project']['id']}")
            elif rec.get("id") and rec["id"] != s.get("id"):
                item["refusals"].append(f"service {s.get('name')}: live id {s.get('id')} is not the ledger's {rec['id']}")
            elif s.get("id") in g.foreign or s.get("id") in g.art:
                item["refusals"].append(f"service {s.get('id')} is listed by another run")
            else:
                item["services"].append({"id": s["id"], "name": s["name"]})
    item["objects"] = [k for k in c.get("artifact_objects", []) if k.startswith(f"k2c/{run}/")]
    item["database"] = c.get("database")
    return item


def plan(run: str, cells: list, g: Guard) -> list:
    doc = F.load_resources(run)
    out = []
    for cell in cells or sorted(doc["cells"]):
        c = doc["cells"].get(cell)
        if not c:
            F.say(f"== {cell}: not in resources.json; nothing to do")
            continue
        try:
            out.append(plan_cell(run, cell, c, g))
        except Exception as e:  # noqa: BLE001 - one cell's planning failure refuses that cell only
            out.append({"cell": cell, "refusals": [f"planning failed: {str(e)[:300]}"], "services": [],
                        "notes": [], "project": None, "bucket": None, "objects": [], "database": None})
    return out


def show(run: str, items: list) -> None:
    for it in items:
        F.say(f"== {it['cell']}")
        for s in it["services"]:
            F.say(f"  service  {s['id']}  {s['name']}")
        if it.get("bucket"):
            F.say(f"  bucket   {it['bucket']['id']}  {it['bucket']['name']}")
        if it.get("project"):
            F.say(f"  project  {it['project']['id']}  {it['project']['name']}")
        if it.get("database"):
            F.say(f"  database {it['database'].get('id')}  (goes with its project)")
        for k in it.get("objects", []):
            F.say(f"  artifact object  {k}")
        for n in it.get("notes", []):
            F.say(f"  note: {n}")
        for r in it["refusals"]:
            F.say(f"  REFUSING {r}")
    show_keep_awake(run)


def show_keep_awake(run: str) -> None:
    now = F.utc()
    for cell, c in sorted(F.load_resources(run)["cells"].items()):
        for k in c.get("keep_awake", []):
            svc = (c.get("services") or {}).get(k["service"]) or {}
            state = "deleted" if svc.get("status") == "deleted" else ("EXPIRED" if k["expires"] < now else "live")
            F.say(f"  KEEP_AWAKE {cell}/{k['service']} expires {k['expires']} ({state})")


def mark(run: str, cell: str, what: str, rid: str | None, status: str, name: str | None = None) -> None:
    with F.resources(run) as doc:
        c = F.cell_res(doc, cell)
        if what in ("project", "bucket"):
            if c.get(what):
                c[what]["status"] = status
                c[what]["torn_down"] = F.utc()
            return
        for s in c["services"].values():
            if (rid and s.get("id") == rid) or (name and s.get("name") == name):
                s["status"] = status
                s["torn_down"] = F.utc()
                if rid and not s.get("id"):
                    s["id"] = rid


def step(report: dict, where: str, fn):
    """Run one teardown step; a failure is recorded, never raised."""
    try:
        return fn()
    except Exception as e:  # noqa: BLE001 - one step never stops the others
        report["failures"].append({"step": where, "error": f"{type(e).__name__}: {str(e)[:300]}"})
        F.say(f"  FAILED {where}: {type(e).__name__}: {str(e)[:300]}")
        return None


def destroy_service(run: str, cell: str, s: dict) -> str:
    p = F.cli(["services", "destroy", s["id"], "--timeout", "300"], timeout=600)
    if p.returncode != 0:
        mark(run, cell, "service", s["id"], "destroy_failed", name=s["name"])
        raise RuntimeError(f"rc={p.returncode}: {(p.stderr or p.stdout)[-300:]}")
    mark(run, cell, "service", s["id"], "deleted", name=s["name"])
    return "destroyed"


def delete_platform(run: str, cell: str, what: str, rid: str) -> int:
    st, body = F.api("DELETE", f"/{what}s/{rid}", allow=(404, 409) if what == "project" else (404,))
    ok = st in (200, 202, 204, 404)
    mark(run, cell, what, rid, "deleted" if ok else "delete_failed")
    if not ok:
        raise RuntimeError(f"DELETE {what} {rid} answered {st}: {str(body)[:200]}")
    return st


def destroy(run: str, items: list, report: dict) -> None:
    for it in items:  # 1. every service of every cell first
        r = report["cells"].setdefault(it["cell"], {"services": {}, "objects": {}})
        for s in it["services"]:
            res = step(report, f"{it['cell']}: destroy service {s['name']}", lambda s=s: destroy_service(run, it["cell"], s))
            r["services"][s["name"]] = res or "FAILED"
            F.say(f"  service {s['name']}: {r['services'][s['name']]}")
    for it in items:  # 2. bucket, then project, of a clean cell
        r = report["cells"][it["cell"]]
        held = it["refusals"] or [n for n, v in r["services"].items() if v != "destroyed"]
        if held:
            r["platform"] = f"left standing: {held}"
            F.say(f"  {it['cell']}: bucket and project left standing ({held})")
            continue
        if it.get("bucket"):
            r["bucket_delete"] = step(report, f"{it['cell']}: delete bucket",
                                      lambda it=it: delete_platform(run, it["cell"], "bucket", it["bucket"]["id"]))
            F.say(f"  bucket {it['bucket']['id']}: DELETE {r['bucket_delete']}")
        if it.get("project"):
            r["project_delete"] = step(report, f"{it['cell']}: delete project",
                                       lambda it=it: delete_platform(run, it["cell"], "project", it["project"]["id"]))
            F.say(f"  project {it['project']['id']}: DELETE {r['project_delete']}")
        for what in ("project", "bucket"):
            if it.get(f"{what}_gone"):
                mark(run, it["cell"], what, it[f"{what}_gone"], "deleted")
        if it.get("project_gone"):  # its services went with it
            for name in it.get("listed", []):
                mark(run, it["cell"], "service", None, "deleted", name=name)
    client, bucket = F.artifact_s3()
    for it in items:  # 3. the run's artifact objects
        r = report["cells"][it["cell"]]
        for k in it.get("objects", []):
            ok = step(report, f"{it['cell']}: delete artifact object {k}",
                      lambda k=k: client.delete_object(Bucket=bucket, Key=k) or True)
            r["objects"][k] = "deleted" if ok else "FAILED"


def verify(run: str, items: list, report: dict) -> bool:
    ok = not report["failures"]
    client, bucket = F.artifact_s3()
    prefix = f"k2c-{run}-"
    for it in items:
        v = {}
        ok &= not it["refusals"]
        for what in ("project", "bucket"):
            if it.get(what):
                st = step(report, f"verify {what}", lambda w=what, it=it: F.api("GET", f"/{w}s/{it[w]['id']}", allow=(404, 403))[0])
                v[f"{what}_get"] = st
                ok &= st in (404, 403)
        for name, res in report["cells"].get(it["cell"], {}).get("services", {}).items():
            v[f"service {name}"] = res
            ok &= res == "destroyed"
        for k in it.get("objects", []):
            try:
                client.head_object(Bucket=bucket, Key=k)
                v[k] = "STILL PRESENT"
                ok = False
            except Exception:  # noqa: BLE001 - 404 is the expected answer
                v[k] = "absent"
        report["cells"].setdefault(it["cell"], {})["verify"] = v

    def leftovers() -> None:
        left = [p for p in F.api_list("/projects") if p.get("name", "").startswith(prefix)]
        report["projects_left"] = [{"id": p["id"], "name": p["name"]} for p in left]
        bkts = [b for b in F.api_list("/buckets") if b.get("name", "").startswith(prefix)]
        report["buckets_left"] = [{"id": b["id"], "name": b["name"]} for b in bkts]
        # A project's database (created by default before provision.py said
        # createDatabase=false) goes with its project; prove it.
        pids = {it["project"]["id"] for it in items if it.get("project")}
        dbs = [d for d in F.api_list("/databases")
               if (d.get("project") or {}).get("id") in pids or d.get("name", "").startswith(prefix)]
        report["databases_left"] = [{"id": d["id"], "name": d.get("name")} for d in dbs]
    if step(report, "verify workspace listings", lambda: leftovers() or True) is None:
        ok = False
    ok &= not (report.get("projects_left") or report.get("buckets_left") or report.get("databases_left"))
    report["verified"] = bool(ok)
    return bool(ok)


def partial(run: str, g: Guard, yes: bool, expired: bool, service: str | None) -> bool:
    """--expired / --service: destroy single ledger-listed services."""
    now = F.utc()
    doc = F.load_resources(run)
    rec = {"utc": now, "mode": "expired" if expired else f"service {service}", "actions": [], "failures": []}
    for cell, c in sorted(doc["cells"].items()):
        targets = [s for s in (c.get("services") or {}).values() if s.get("status") != "deleted" and (
            (expired and s.get("keep_awake") and (s.get("keep_awake_expires") or "9") < now)
            or (service and s.get("name") == service))]
        for s in targets:
            act = {"cell": cell, "service": s.get("name"), "expires": s.get("keep_awake_expires")}
            rec["actions"].append(act)
            why = g.static("service", s.get("id"), s.get("name"))
            p = c.get("project") or {}
            why = why or g.static("project", p.get("id"), p.get("name"))
            if not why and not p.get("id"):
                why = "its project has no id (pending): run a full teardown"
            state, why2 = ("refuse", why) if why else g.live("project", p)
            if state == "refuse":
                act["result"] = f"REFUSED: {why2}"
            elif state == "gone":
                act["result"] = "absent (project gone)"
                if yes:
                    mark(run, cell, "service", s.get("id"), "deleted", name=s.get("name"))
            else:
                live = [x for x in F.services_in(p["id"]) if x.get("name") == s.get("name")]
                if not live:
                    act["result"] = "absent (not in its project)"
                    if yes:
                        mark(run, cell, "service", s.get("id"), "deleted", name=s.get("name"))
                elif s.get("id") and live[0].get("id") != s["id"]:
                    act["result"] = f"REFUSED: live id {live[0].get('id')} is not the ledger's {s['id']}"
                elif not yes:
                    act["result"] = f"would destroy {live[0]['id']}"
                else:
                    res = step(rec, f"{cell}: destroy {s.get('name')}",
                               lambda c=cell, x=live[0]: destroy_service(run, c, x))
                    act["result"] = res or "FAILED"
            F.say(f"  {cell}/{act['service']} (expires {act['expires']}): {act['result']}")
    if yes:
        with F.resources(run) as d:
            for cell, c in d["cells"].items():
                for k in c.get("keep_awake", []):
                    if (c.get("services") or {}).get(k["service"], {}).get("status") == "deleted":
                        k["status"] = "deleted"
        F.append_jsonl(f"{F.results_dir(run)}/teardown-partial.jsonl", rec)
    if not rec["actions"]:
        F.say("  nothing to do")
    return not rec["failures"] and not any(str(a.get("result", "")).startswith(("REFUSED", "FAILED"))
                                           for a in rec["actions"])


def main() -> None:
    argv = sys.argv[1:]
    yes, expired = "--yes" in argv, "--expired" in argv
    service = None
    if "--service" in argv:
        i = argv.index("--service")
        if i + 1 >= len(argv):
            F.die(USAGE, 2)
        service = argv[i + 1]
        argv = argv[:i] + argv[i + 2:]
    args = [a for a in argv if a not in ("--yes", "--expired")]
    if not args or any(a.startswith("--") for a in args):
        F.die(USAGE, 2)
    run, cells = args[0], args[1:]
    F.check_names(run)
    g = Guard(run)
    if expired or service:
        if cells:
            F.die(USAGE, 2)
        ok = partial(run, g, yes, expired, service)
        sys.exit(0 if ok else 1)
    items = plan(run, cells, g)
    show(run, items)
    if not yes:
        F.say("\nDRY RUN: re-run with --yes to delete exactly the resources above.")
        return
    report = {"run": run, "started": F.utc(), "cells": {}, "failures": [], "verified": False}
    ok = False
    try:
        destroy(run, items, report)
        ok = verify(run, items, report)
    except Exception as e:  # noqa: BLE001 - recorded, then the report is still written
        report["failures"].append({"step": "teardown", "error": f"{type(e).__name__}: {e}",
                                   "trace": traceback.format_exc()[-1500:]})
        ok = False
    finally:
        report["finished"] = F.utc()
        F.write_json(f"{F.results_dir(run)}/teardown.json", report)
    F.say(f"\nteardown {'VERIFIED' if ok else 'INCOMPLETE'}: failures {len(report['failures'])}, "
          f"refusals {[r for it in items for r in it['refusals']]}, projects left {report.get('projects_left')}, "
          f"buckets left {report.get('buckets_left')}, databases left {report.get('databases_left')}; "
          f"per cell {[(c, v.get('verify')) for c, v in report['cells'].items()]}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
