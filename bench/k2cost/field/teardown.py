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

Campaign mode (campaign.json): the run's ledger-listed services and buckets
ONLY; the campaign project is never deleted (campaign.py destroy does
that). Guards, in addition to the ledger-name, artifact-bucket and
other-run ones above:
- it refuses to run at all unless the campaign project is live under its
  recorded name (GET /v1/projects/{campaign}) and the run's ledger is
  stamped with that project;
- a service or bucket is acted on only when its LIVE object (GET
  /v1/services/{id}, GET /v1/buckets/{id}) is in the campaign project with
  the ledger's name, starting k2c-<run>- (fieldlib.campaign_owns; the
  mutation gate refuses any id that did not pass it);
- services are listed only in the campaign project (`compute services
  list --project <campaign>`): a ledger id that differs from the live one
  of its name, or a 404 for a service the project still lists, is a
  refusal; an unlisted service named like one of the cell's
  (k2c-<run>-<cell>-s|r|gN) is a refusal that keeps the cell's bucket.
Verification lists only the campaign project's services and buckets and
checks that none named k2c-<run>-* (with named cells: none of those
cells' names) remains, every deleted bucket and
destroyed service answers 404, and every deleted artifact object is absent.
It does NOT cover the campaign project's environment variables: after a
VERIFIED teardown the project still holds the last deploy's whole variable
set, secret values included (AUTH_TOKEN, FLEET_INTERNAL_TOKEN, the data
bucket key, the artifact key, KEEP_AWAKE=1, ...), until the next deploy
restates or unsets them or campaign.py destroy deletes the project. The
scope gates refuse every env-variable mutation, so no tool removes them;
`campaign.py verify` lists their names. (In per-cell mode they went with
the cell's project.)
"""
from __future__ import annotations

import re
import sys
import traceback

import fieldlib as F

USAGE = ("usage: teardown.sh <run-id> [--yes] [cell...] | <run-id> --expired [--yes] | "
         "<run-id> --service <name> [--yes]")
SERVICE_SUFFIX = re.compile(r"-[srg][0-9]+")


class Guard:
    """Which platform objects this run may delete."""

    def __init__(self, run: str):
        self.run, self.prefix = run, f"k2c-{run}-"
        self.camp, self.pid = F.campaign(), None
        stamp = F.load_resources(run).get("campaign")
        if self.camp:
            F.campaign_check(self.camp)  # refuses to run at all if the project lost its name
            self.pid = self.camp["project"]["id"]
            if F.load_resources(run)["cells"] and (stamp or {}).get("project_id") != self.pid:
                F.die(f"run {run} is not a run of campaign {self.camp['name']} ({self.pid}): refusing")
        elif stamp:
            F.die(f"run {run} is a campaign run ({stamp}) but {F.FIELD} has no campaign.json: refusing")
        rc = F.artifact_receipt()
        self.art = {rc["projectId"]: "the campaign project" if self.camp else "the artifact project",
                    rc["bucketId"]: "the artifact bucket"}
        if self.camp:
            self.art.update({self.pid: "the campaign project", self.camp["artifact_bucket"]["id"]: "the artifact bucket"})
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


def resolve_pending(run: str, cell: str, what: str, entry: dict, project_id: str | None,
                    scoped: bool = False) -> dict | None:
    """Adopt the one live resource named like a pending entry (or None).
    `scoped`: buckets are listed in `project_id` only (campaign mode)."""
    if what == "project":
        live = [p for p in F.api_list("/projects") if p.get("name") == entry.get("name")]
    else:
        live = [b for b in F.api_list(f"/buckets?projectId={project_id}" if scoped else "/buckets")
                if b.get("name") == entry.get("name") and (b.get("project") or {}).get("id") == project_id]
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


def plan_service_campaign(run: str, cell: str, s: dict, g: Guard, found: list, item: dict) -> str | None:
    """Plan one ledger service of a campaign run: a refusal, or None."""
    name, sid = s.get("name") or "", s.get("id")
    why = g.static("service", sid, name)
    if why:
        return why
    if len(found) > 1:
        return f"service {name}: {len(found)} live services of that name in {g.pid}"
    if not sid:
        if not found:
            item["notes"].append(f"service {name}: pending entry, no live service of its name")
            item["gone_services"].append(name)
            return None
        sid = found[0].get("id")
        with F.resources(run) as d:
            F.cell_res(d, cell)["services"][name].update({"id": sid, "resolved_by_name": F.utc()})
        why = g.static("service", sid, name)
        if why:
            return why
    if found and found[0].get("id") != sid:
        return f"service {name}: live id {found[0].get('id')} is not the ledger's {sid}"
    state, why = F.campaign_owns("service", sid, name=name, prefix=g.prefix)
    if state == "refuse":
        return why
    if state == "gone":
        if found:
            return f"service {sid} ({name}): GET answered 404 but the campaign project lists it"
        item["notes"].append(f"service {sid} ({name}): already gone (404)")
        item["gone_services"].append(name)
        return None
    item["services"].append({"id": sid, "name": name})
    return None


def plan_cell_campaign(run: str, cell: str, c: dict, g: Guard, live: dict, listing_error: str | None) -> dict:
    """A campaign run's cell: its bucket and services, each proven live in
    the campaign project; never a project."""
    item = {"cell": cell, "refusals": [], "services": [], "notes": [], "project": None, "bucket": None,
            "gone_services": []}
    if c.get("project"):
        item["refusals"].append(f"cell has its own project {c['project'].get('id')} in a campaign run")
    b = c.get("bucket")
    if b and b.get("status") != "deleted":
        if not b.get("id"):
            b = resolve_pending(run, cell, "bucket", b, g.pid, scoped=True)
            if not b:
                item["notes"].append("bucket: pending entry, no live bucket of its name in the campaign project")
        if b:
            why = g.static("bucket", b["id"], b.get("name"))
            state, why = ("refuse", why) if why else F.campaign_owns("bucket", b["id"], name=b.get("name"),
                                                                     prefix=g.prefix)
            if state == "refuse":
                item["refusals"].append(why)
            elif state == "gone":
                item["notes"].append(f"bucket {b['id']}: already gone (404)")
                item["bucket_gone"] = b["id"]
            else:
                item["bucket"] = b
    if listing_error:
        item["refusals"].append(f"services of campaign project {g.pid} could not be listed: {listing_error}")
    listed = set()
    for s in (c.get("services") or {}).values():
        if s.get("status") != "deleted":
            listed.add(s.get("name") or "")
            why = plan_service_campaign(run, cell, s, g, live.get(s.get("name")) or [], item)
            if why:
                item["refusals"].append(why)
    base = F.base_name(run, cell)
    for name, xs in sorted(live.items()):
        if name not in listed and name.startswith(base) and SERVICE_SUFFIX.fullmatch(name[len(base):]):
            item["refusals"].append(f"unlisted service {[x.get('id') for x in xs]} ({name}) in {g.pid}")
    item["listed"] = sorted(listed)
    item["objects"] = [k for k in c.get("artifact_objects", []) if k.startswith(f"k2c/{run}/")]
    item["database"] = None
    return item


def campaign_services(g: Guard) -> tuple:
    """({name: [services]} of the campaign project, listing error or None)."""
    try:
        out: dict = {}
        for s in F.services_in(g.pid):
            out.setdefault(s.get("name") or "", []).append(s)
        return out, None
    except Exception as e:  # noqa: BLE001 - an unknown service set refuses every cell
        return {}, str(e)[:200]


def plan(run: str, cells: list, g: Guard) -> list:
    doc = F.load_resources(run)
    out = []
    live, err = campaign_services(g) if g.camp else ({}, None)
    for cell in cells or sorted(doc["cells"]):
        c = doc["cells"].get(cell)
        if not c:
            F.say(f"== {cell}: not in resources.json; nothing to do")
            continue
        try:
            out.append(plan_cell_campaign(run, cell, c, g, live, err) if g.camp else plan_cell(run, cell, c, g))
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
    for it in items:  # 2. bucket, then project (never in campaign mode: items carry none), of a clean cell
        r = report["cells"][it["cell"]]
        for name in it.get("gone_services", []):  # campaign mode: 404 and not listed in the project
            mark(run, it["cell"], "service", None, "deleted", name=name)
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


def leftovers_campaign(g: Guard, report: dict, cells: list | None) -> None:
    """Only the campaign project's services and buckets are listed; what
    counts as left over is any k2c-<run>-* name, or, for a teardown of
    named cells (F1's f1a f1c while f1b goes on), those cells' names."""
    bases = [F.base_name(g.run, c) for c in cells or []]

    def ours(name: str) -> bool:
        return name.startswith(g.prefix) and (not bases or any(
            name == b or (name.startswith(b) and SERVICE_SUFFIX.fullmatch(name[len(b):])) for b in bases))
    left = [s for s in F.services_in(g.pid) if ours(s.get("name") or "")]
    report["services_left"] = [{"id": s.get("id"), "name": s.get("name")} for s in left]
    bkts = [b for b in F.api_list(f"/buckets?projectId={g.pid}")
            if (b.get("project") or {}).get("id") == g.pid and ours(b.get("name") or "")]
    report["buckets_left"] = [{"id": b["id"], "name": b["name"]} for b in bkts]


def verify(run: str, items: list, report: dict, g: Guard | None = None, cells: list | None = None) -> bool:
    ok = not report["failures"]
    client, bucket = F.artifact_s3()
    prefix = f"k2c-{run}-"
    camp = bool(g and g.camp)
    for it in items:
        v = {}
        ok &= not it["refusals"]
        for what in ("project", "bucket"):
            if it.get(what):
                st = step(report, f"verify {what}", lambda w=what, it=it: F.api("GET", f"/{w}s/{it[w]['id']}", allow=(404, 403))[0])
                v[f"{what}_get"] = st
                ok &= st in ((404,) if camp else (404, 403))
        for name, res in report["cells"].get(it["cell"], {}).get("services", {}).items():
            v[f"service {name}"] = res
            ok &= res == "destroyed"
        for s in it["services"] if camp else []:
            st = step(report, "verify service", lambda s=s: F.api("GET", f"/services/{s['id']}", allow=(404, 403))[0])
            v[f"service {s['name']} get"] = st
            ok &= st == 404
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
    if camp:
        if step(report, "verify campaign project listings", lambda: leftovers_campaign(g, report, cells) or True) is None:
            ok = False
    elif step(report, "verify workspace listings", lambda: leftovers() or True) is None:
        ok = False
    ok &= not (report.get("projects_left") or report.get("buckets_left") or report.get("databases_left")
               or report.get("services_left"))
    report["verified"] = bool(ok)
    return bool(ok)


def partial_campaign(run: str, cell: str, s: dict, g: Guard, yes: bool, rec: dict) -> str:
    """One --expired / --service target of a campaign run: proven live in
    the campaign project, then destroyed (or why not)."""
    name, sid = s.get("name") or "", s.get("id")
    why = g.static("service", sid, name)
    if why:
        return f"REFUSED: {why}"
    found = [x for x in F.services_in(g.pid) if x.get("name") == name]
    if len(found) > 1 or (found and sid and found[0].get("id") != sid):
        return f"REFUSED: live {[x.get('id') for x in found]} named {name} is not the ledger's {sid}"
    sid = sid or (found[0].get("id") if found else None)
    state, why = "gone", None
    if sid:
        why = g.static("service", sid, name)
        state, why = ("refuse", why) if why else F.campaign_owns("service", sid, name=name, prefix=g.prefix)
    if state == "refuse":
        return f"REFUSED: {why}"
    if state == "gone":
        if found:
            return f"REFUSED: service {sid} answers 404 but the campaign project lists it"
        if yes:
            mark(run, cell, "service", sid, "deleted", name=name)
        return "absent (not in the campaign project)"
    if not yes:
        return f"would destroy {sid}"
    return step(rec, f"{cell}: destroy {name}", lambda: destroy_service(run, cell, {"id": sid, "name": name})) or "FAILED"


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
            if g.camp:
                act["result"] = partial_campaign(run, cell, s, g, yes, rec)
                F.say(f"  {cell}/{act['service']} (expires {act['expires']}): {act['result']}")
                continue
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
    F.banner("teardown")
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
        ok = verify(run, items, report, g, cells)
    except Exception as e:  # noqa: BLE001 - recorded, then the report is still written
        report["failures"].append({"step": "teardown", "error": f"{type(e).__name__}: {e}",
                                   "trace": traceback.format_exc()[-1500:]})
        ok = False
    finally:
        report["finished"] = F.utc()
        F.write_json(f"{F.results_dir(run)}/teardown.json", report)
    F.say(f"\nteardown {'VERIFIED' if ok else 'INCOMPLETE'}: failures {len(report['failures'])}, "
          f"refusals {[r for it in items for r in it['refusals']]}, projects left {report.get('projects_left')}, "
          f"buckets left {report.get('buckets_left')}, databases left {report.get('databases_left')}, "
          f"services left {report.get('services_left')}{' (campaign project ' + g.pid + ', kept)' if g.camp else ''}; "
          f"per cell {[(c, v.get('verify')) for c, v in report['cells'].items()]}")
    if g.camp:
        F.say("note: the campaign project's env variables (the last deploy's set, secrets included) are not part "
              "of a run teardown; campaign.py verify lists them")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
