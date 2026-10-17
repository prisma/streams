#!/usr/bin/env python3
"""Provision a shared cell's stage on the platform (public-beta deploy-q5
triage B5; patterned on bench/k2cost/field/provision.py): its Compute
project or Branch, its three buckets outside Composer (design §2.4 (b): a
Composer bucket's delete takes every object with it), their keys, and the
files the release tools read.

    PRISMA_TOKEN_FILE=F deploy/composer/provision-cell.py <stage> --app NAME
        --region R --cells DIR --secrets DIR [--cell-id ID] [--management-api URL]
    PRISMA_TOKEN_FILE=F deploy/composer/provision-cell.py <stage> --app NAME
        --cells DIR --teardown [--management-api URL]
    PRISMA_TOKEN_FILE=F deploy/composer/provision-cell.py <stage> --app NAME
        --region R --cells DIR --no-buckets [--management-api URL]

The project: for the default stage (production), the Compute project named
as the Composer app (--app: streams-cell for the cell), created in --region
with no database, or the one of that name adopted when it is there and in
that region; `prisma deploy` adopts it by name (D10). A named stage
(scratch, cert) is a Branch of that project, gitName the stage; the
project must exist in --region.

The production Branch, for every stage: the project's Branch with role
production or isDefault true (Composer 0.29.1 reads the default stage's
Branch by isDefault and never makes one; a named stage's Branch it
creates by gitName when absent). A project without one (a fresh project
can have no Branch at all, and the first Branch of a project becomes its
production Branch) gets it first, gitName main, before any stage's
Branch, so a named stage's Branch, here or from Composer, never becomes
the production one. When the platform answers that first Branch as
anything but production the run is refused with what to ask Platform for.

The buckets, in the project: <app>-<stage>-data (SlateDB's), -feeds (the
cell's feed bundle only) and -control (cell-admin's and the issuer's
state). The keys (Q11 (a)): data read_write for the cell; feeds read for
the cell; feeds read_write and control read_write for cell-admin and the
issuer. Each key's pair is written, mode 0600, as set-secrets reads it:
the cell's under --secrets DIR (STREAMS_DATA_S3_* and STREAMS_FEEDS_S3_*),
the writers' under DIR/issuer (STREAMS_FEEDS_S3_* and STREAMS_CONTROL_S3_*),
and cell-admin's --s3-credentials file DIR/cell-admin-s3.json. The
non-secret values are set by key in DIR (--cells)/<stage>.env, every other
line kept: CELL_STAGE, REGION, SLATE_S3_*, FEEDS_S3_* and
FEEDS_S3_KEY=cells/<cell id>/feeds-bundle.json (the cell id from
--cell-id or the file's CELL_ID; `cell-admin init --cell-env` sets the
cell's identity, PATH_PREFIX is the operator's); the issuer's settings for
the two buckets go to <stage>.issuer.env the same way.

The ledger, --cells DIR/<stage>.provision.json, gets an entry before every
POST and its id the moment the platform answers, so a lost answer (a POST
is never retried) leaves an entry a re-run resolves by name before it
creates anything. A key's secret is answered once: a key whose files are
missing is deleted and minted again. A re-run with everything in place
makes no POST.

--no-buckets makes the project (created when absent, whatever the stage),
its production Branch and a named stage's Branch, and nothing else: the
containers of an app with no bucket of its own (the issuer, streams-issuer;
the cell watch, streams-cell-watch; the paging stand-in,
streams-pager-standin), or the cell's project before its scratch stage's
buckets. Its ledger is --cells DIR/<stage>.<app>.provision.json.

--teardown deletes the stage's keys and buckets, the bucket's objects with
it, and refuses production (S30: Composer's destroy of a named stage
leaves the external buckets). The Branch goes with `deploy-release.sh
--destroy`.

The Management API token is read from the file PRISMA_TOKEN_FILE names
(mode 0600); no value of a key is printed. Exit codes: 0 done, 1 refused or
failed, 2 usage.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import stat
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

API = "https://api.prisma.io/v1"
STAGES = ("scratch", "cert", "production")
# The production Branch's gitName when this tool makes it: no stage's name.
PRODUCTION_BRANCH = "main"
REGIONS = ("us-east-1", "us-west-1", "eu-west-3", "eu-central-1", "ap-northeast-1", "ap-southeast-1")
BUCKETS = ("data", "feeds", "control")
IDEMPOTENT = {"GET", "DELETE"}
# Each key: its bucket, role, the directory under --secrets and the names
# its pair is written as there.
KEYS = {
    "data-cell": ("data", "read_write", "", "STREAMS_DATA_S3"),
    "feeds-cell": ("feeds", "read", "", "STREAMS_FEEDS_S3"),
    "feeds-writer": ("feeds", "read_write", "issuer", "STREAMS_FEEDS_S3"),
    "control-writer": ("control", "read_write", "issuer", "STREAMS_CONTROL_S3"),
}
LINE = re.compile(r"^[^=\s]+=\S*$")


class Refused(Exception):
    def __init__(self, message: str, code: int = 1):
        super().__init__(message)
        self.code = code


class OutcomeUnknown(Refused):
    """A POST whose answer was lost: the platform may have made it."""


def say(msg: str) -> None:
    print(f"provision-cell: {msg}", flush=True)


def owner_only(path: str, what: str) -> str:
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        raise Refused(f"{what} {path} does not exist")
    if not stat.S_ISREG(st.st_mode):
        raise Refused(f"{what} {path} is not a regular file")
    if st.st_mode & 0o077:
        raise Refused(f"{what} {path} has mode 0{st.st_mode & 0o777:o}: only its owner may read it (chmod 600)")
    with open(path, encoding="utf-8") as f:
        text = f.read()
    return text[:-1] if text.endswith("\n") else text


class Platform:
    def __init__(self, base: str, token: str):
        self.base = base.rstrip("/")
        self.token = token

    def call(self, method: str, path: str, body=None, allow=()):
        """(status, JSON or None). GET and DELETE retry transport errors,
        429 and 5xx three times; a POST is never retried, and one whose
        answer is lost raises OutcomeUnknown."""
        data = json.dumps(body).encode() if body is not None else None
        retries = 3 if method in IDEMPOTENT else 0
        for attempt in range(retries + 1):
            req = urllib.request.Request(f"{self.base}{path}", method=method, data=data, headers={
                "Authorization": f"Bearer {self.token}", "Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    raw = r.read()
                    return r.status, (json.loads(raw) if raw else None)
            except urllib.error.HTTPError as e:
                raw = e.read()
                try:
                    doc = json.loads(raw) if raw else None
                except ValueError:
                    doc = None
                if e.code in allow:
                    return e.code, doc
                if (e.code == 429 or e.code >= 500) and attempt < retries:
                    time.sleep(1 + 2 * attempt)
                    continue
                code = ((doc or {}).get("error") or {}).get("code", "no error code") if isinstance(doc, dict) else "no error code"
                if e.code >= 500 and method not in IDEMPOTENT:
                    raise OutcomeUnknown(f"{method} {path} answered {e.code} ({code}): its outcome is unknown; "
                                         "the ledger keeps a pending entry, run again to resolve it by name")
                raise Refused(f"{method} {path} answered {e.code} ({code})")
            except (urllib.error.URLError, socket.timeout, OSError) as e:
                if attempt < retries:
                    time.sleep(1 + 2 * attempt)
                    continue
                if method not in IDEMPOTENT:
                    raise OutcomeUnknown(f"{method} {path}: the answer was lost ({e}); the ledger keeps a pending "
                                         "entry, run again to resolve it by name")
                raise Refused(f"{method} {path}: {e}")
        raise Refused("unreachable")

    def list(self, path: str) -> list:
        out, cursor = [], None
        for _ in range(200):
            sep = "&" if "?" in path else "?"
            _, doc = self.call("GET", path + (f"{sep}cursor={urllib.parse.quote(cursor)}" if cursor else ""))
            out.extend((doc or {}).get("data") or [])
            pg = (doc or {}).get("pagination") or {}
            cursor = pg.get("nextCursor") if pg.get("hasMore") else None
            if not cursor:
                return out
        raise Refused(f"GET {path}: more than 200 pages")


class Ledger:
    """<cells>/<stage>.provision.json, written whole after every change."""

    def __init__(self, path: str, stage: str, app: str):
        self.path = path
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                self.doc = json.load(f)
            if self.doc.get("stage") != stage or self.doc.get("app") != app:
                raise Refused(f"{path} is the ledger of {self.doc.get('app')} {self.doc.get('stage')}, not {app} {stage}")
        else:
            self.doc = {"stage": stage, "app": app, "buckets": {}, "keys": {}}

    def save(self) -> None:
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.doc, f, indent=2, sort_keys=True)
            f.write("\n")
        os.replace(tmp, self.path)


def utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def created(api: Platform, ledger: Ledger, slot: dict, post_path: str, body: dict) -> dict:
    """POST after a pending ledger entry; the entry gets the id."""
    slot.update({"status": "pending", "requested": utc()})
    ledger.save()
    _, doc = api.call("POST", post_path, body)
    data = (doc or {}).get("data") or {}
    slot.update({"id": data["id"], "status": "created"})
    ledger.save()
    return data


def one_named(items: list, name: str, what: str) -> dict | None:
    found = [i for i in items if i.get("name") == name]
    if len(found) > 1:
        raise Refused(f"{len(found)} {what}s are named {name}: resolve by hand")
    return found[0] if found else None


def project(api: Platform, ledger: Ledger, app: str, region: str, stage: str) -> str:
    """The stage's project id, created for the default stage when absent."""
    slot = ledger.doc.setdefault("project", {"name": app})
    if slot.get("id"):
        _, doc = api.call("GET", f"/projects/{urllib.parse.quote(slot['id'])}")
        live = (doc or {}).get("data") or {}
    else:
        live = one_named(api.list("/projects"), app, "project")
        if live is None and stage != "production":
            raise Refused(f"no project is named {app}: provision the production stage first (a named stage is its Branch; "
                          "--no-buckets makes the project and its Branches alone)")
        if live is None:
            data = created(api, ledger, slot, "/projects", {"name": app, "region": region, "createDatabase": False})
            slot["createdWithRegion"] = region
            ledger.save()
            say(f"project {data['id']} ({app}) created in {region}")
            return data["id"]
    if live.get("name") != app or live.get("defaultRegion") != region:
        raise Refused(f"project {live.get('id')} is {live.get('name')} in {live.get('defaultRegion')}, not {app} in {region}")
    if not slot.get("id"):
        slot.update({"id": live["id"], "status": "adopted" if slot.get("status") != "pending" else "resolved_by_name"})
        ledger.save()
    say(f"project {live['id']} ({app}, {region})")
    return live["id"]


def is_production(b: dict) -> bool:
    """The platform's production Branch: role production (the Management
    API's listing) or isDefault (what Composer reads)."""
    return b.get("role") == "production" or b.get("isDefault") is True


def production_branch(api: Platform, ledger: Ledger, project_id: str) -> str:
    """The project's production Branch, made first when it has none."""
    slot = ledger.doc.setdefault("productionBranch", {"gitName": PRODUCTION_BRANCH})
    path = f"/projects/{urllib.parse.quote(project_id)}/branches"
    rows = api.list(path)
    found = [b for b in rows if is_production(b)]
    if found:
        if slot.get("id") != found[0]["id"]:
            pending = slot.get("status") == "pending" and found[0].get("gitName") == PRODUCTION_BRANCH
            slot.update({"id": found[0]["id"], "gitName": found[0].get("gitName"),
                         "status": "resolved_by_name" if pending else "adopted"})
            ledger.save()
        return found[0]["id"]
    if rows:
        names = ", ".join(f"{b.get('gitName')} ({b.get('id')})" for b in rows)
        raise Refused(f"project {project_id} has Branches but no production one ({names}): resolve it by hand with Platform; "
                      "a Branch made now would not become the production Branch")
    data = created(api, ledger, slot, path, {"gitName": PRODUCTION_BRANCH})
    if not is_production(data):
        raise Refused(f"project {project_id} has no production Branch, and the Branch {PRODUCTION_BRANCH} created for it "
                      f"({data['id']}) is answered as role {data.get('role')}, not production: this tool cannot make one. "
                      "Have Platform give the project a production (default) Branch (GET /v1/projects/{id}/branches must "
                      "list one with role production or isDefault true), then run again; do not deploy a named stage "
                      "first (Composer would create its Branch as the project's first)")
    say(f"branch {data['id']} ({PRODUCTION_BRANCH}) created in {project_id}: its production Branch")
    return data["id"]


def branch(api: Platform, ledger: Ledger, project_id: str, stage: str) -> str:
    slot = ledger.doc.setdefault("branch", {"gitName": stage})
    rows = api.list(f"/projects/{urllib.parse.quote(project_id)}/branches")
    mine = [b for b in rows if b.get("gitName") == stage]
    if mine:
        slot.update({"id": mine[0]["id"], "status": slot.get("status") if slot.get("id") == mine[0]["id"] else "adopted"})
        ledger.save()
        say(f"branch {mine[0]['id']} ({stage}) of {project_id}")
        return mine[0]["id"]
    data = created(api, ledger, slot, f"/projects/{urllib.parse.quote(project_id)}/branches", {"gitName": stage})
    say(f"branch {data['id']} ({stage}) created in {project_id}")
    return data["id"]


def buckets(api: Platform, ledger: Ledger, project_id: str, app: str, stage: str) -> dict:
    live = api.list(f"/buckets?projectId={urllib.parse.quote(project_id)}")
    out = {}
    for b in BUCKETS:
        name = f"{app}-{stage}-{b}"
        slot = ledger.doc["buckets"].setdefault(b, {"name": name})
        found = one_named(live, name, "bucket")
        if slot.get("id") and (found is None or found["id"] != slot["id"]):
            raise Refused(f"the ledger's bucket {slot['id']} ({name}) is not in project {project_id}")
        if found is not None:
            if not slot.get("id"):
                slot.update({"id": found["id"], "status": "resolved_by_name" if slot.get("status") == "pending" else "adopted"})
                ledger.save()
        else:
            created(api, ledger, slot, "/buckets", {"projectId": project_id, "name": name})
            say(f"bucket {slot['id']} ({name}) created")
        out[b] = slot["id"]
    return out


def write_secret(path: str, value: str) -> None:
    os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
    tmp = f"{path}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(value + "\n")
    os.replace(tmp, path)


def key_files(secrets: str, where: str, prefix: str) -> tuple:
    d = os.path.join(secrets, where) if where else secrets
    return os.path.join(d, f"{prefix}_ACCESS_KEY_ID"), os.path.join(d, f"{prefix}_SECRET_ACCESS_KEY")


def keys(api: Platform, ledger: Ledger, bucket_ids: dict, app: str, stage: str, secrets: str) -> None:
    for kname, (b, role, where, prefix) in KEYS.items():
        id_file, secret_file = key_files(secrets, where, prefix)
        slot = ledger.doc["keys"].setdefault(kname, {"name": f"{app}-{stage}-{kname}", "bucket": b, "role": role})
        if slot.get("status") == "created" and os.path.exists(id_file) and os.path.exists(secret_file):
            continue
        bid = bucket_ids[b]
        lost = [k for k in api.list(f"/buckets/{urllib.parse.quote(bid)}/keys")
                if k.get("id") == slot.get("id") or k.get("name") == slot["name"]]
        for k in lost:  # its secret was answered once, and is not on this machine
            api.call("DELETE", f"/buckets/{urllib.parse.quote(bid)}/keys/{urllib.parse.quote(k['id'])}", allow=(404,))
            say(f"key {k['id']} ({slot['name']}) deleted: its secret is not in {os.path.dirname(id_file)}")
        data = created(api, ledger, slot, f"/buckets/{urllib.parse.quote(bid)}/keys", {"name": slot["name"], "role": role})
        write_secret(id_file, data["accessKeyId"])
        write_secret(secret_file, data["secretAccessKey"])
        slot.update({"endpoint": data["endpoint"], "bucketName": data["bucketName"]})
        ledger.save()
        say(f"key {data['id']} ({role} on {b}) -> {id_file}, {secret_file}")


def set_by_key(path: str, values: dict) -> None:
    """Set `values` in the KEY=VALUE file at `path` by key, in place, every
    other line kept; a missing file is made, mode 0644 (it holds no
    secret). Refuses a line that is not space-free KEY=VALUE and a key set
    twice, as the cell's entry does."""
    text = ""
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            text = f.read()
    seen, lines = set(), []
    for i, raw in enumerate(text.split("\n")):
        line = raw.strip()
        if line == "" or line.startswith("#"):
            lines.append(raw)
            continue
        if not LINE.match(line):
            raise Refused(f"{path}:{i + 1}: not a space-free KEY=VALUE line")
        key = line.split("=", 1)[0]
        if key in seen:
            raise Refused(f"{path} sets {key} twice")
        seen.add(key)
        lines.append(f"{key}={values[key]}" if key in values else raw)
    if lines and lines[-1] == "":
        lines.pop()
    lines += [f"{k}={v}" for k, v in values.items() if k not in seen]
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    os.chmod(tmp, os.stat(path).st_mode & 0o777 if os.path.exists(path) else 0o644)
    os.replace(tmp, path)


def file_value(path: str, key: str) -> str | None:
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1]
    return None


def target(ledger: Ledger, kname: str, secrets: str) -> dict:
    slot = ledger.doc["keys"][kname]
    _, _, where, prefix = KEYS[kname]
    id_file, secret_file = key_files(secrets, where, prefix)
    return {"endpoint": slot["endpoint"], "bucket": slot["bucketName"], "region": "auto",
            "accessKeyId": owner_only(id_file, "the key file"), "secretAccessKey": owner_only(secret_file, "the key file")}


def files(ledger: Ledger, cells: str, secrets: str, stage: str, region: str, cell_id: str) -> None:
    k = ledger.doc["keys"]
    data, feeds, control = k["data-cell"], k["feeds-cell"], k["control-writer"]
    set_by_key(os.path.join(cells, f"{stage}.env"), {
        "CELL_STAGE": stage, "REGION": region,
        "SLATE_S3_ENDPOINT": data["endpoint"], "SLATE_S3_BUCKET": data["bucketName"], "SLATE_S3_REGION": "auto",
        "FEEDS_S3_ENDPOINT": feeds["endpoint"], "FEEDS_S3_BUCKET": feeds["bucketName"], "FEEDS_S3_REGION": "auto",
        "FEEDS_S3_KEY": f"cells/{cell_id}/feeds-bundle.json",
    })
    set_by_key(os.path.join(cells, f"{stage}.issuer.env"), {
        "STREAMS_CONTROL_S3_ENDPOINT": control["endpoint"], "STREAMS_CONTROL_S3_BUCKET": control["bucketName"],
        "STREAMS_CONTROL_S3_REGION": "auto",
        "STREAMS_FEEDS_S3_ENDPOINT": feeds["endpoint"], "STREAMS_FEEDS_S3_BUCKET": feeds["bucketName"],
        "STREAMS_FEEDS_S3_REGION": "auto",
    })
    creds = {**target(ledger, "control-writer", secrets), "feeds": target(ledger, "feeds-writer", secrets)}
    write_secret(os.path.join(secrets, "cell-admin-s3.json"), json.dumps(creds, indent=2))
    say(f"{stage}.env and {stage}.issuer.env in {cells}; cell-admin's --s3-credentials {secrets}/cell-admin-s3.json")


def cell_id_of(a: argparse.Namespace, ledger: Ledger) -> str:
    """The cell's id: --cell-id, the cells file's CELL_ID or the ledger's,
    which must agree where given."""
    cells_file = os.path.join(a.cells, f"{a.stage}.env")
    named = {"--cell-id": a.cell_id, f"{cells_file}'s CELL_ID": file_value(cells_file, "CELL_ID"),
             f"{ledger.path}": ledger.doc.get("cell_id")}
    ids = {v for v in named.values() if v}
    if not ids:
        raise Refused(f"no cell id: give --cell-id, or run `cell-admin init --cell-env {cells_file}` first", 2)
    if len(ids) > 1:
        raise Refused("the cell id differs: " + ", ".join(f"{k} {v}" for k, v in named.items() if v))
    return ids.pop()


def provision(api: Platform, a: argparse.Namespace) -> None:
    if a.region not in REGIONS:
        raise Refused(f"--region {a.region} is not one of {', '.join(REGIONS)}", 2)
    ledger = Ledger(os.path.join(a.cells, f"{a.stage}.provision.json"), a.stage, a.app)
    cell_id = cell_id_of(a, ledger)
    if ledger.doc.setdefault("region", a.region) != a.region:
        raise Refused(f"{ledger.path} provisioned {a.stage} in {ledger.doc['region']}, not {a.region}")
    ledger.doc["cell_id"] = cell_id
    pid = project(api, ledger, a.app, a.region, a.stage)
    production_branch(api, ledger, pid)
    if a.stage != "production":
        branch(api, ledger, pid, a.stage)
    keys(api, ledger, buckets(api, ledger, pid, a.app, a.stage), a.app, a.stage, a.secrets)
    files(ledger, a.cells, a.secrets, a.stage, a.region, cell_id)
    b = ledger.doc.get("branch", {}).get("id")
    scope = f"--project {pid}" + (f" --branch {b}" if b else "")
    say(f"done; next: set-secrets {a.stage} {scope} --from {a.secrets} (with the three secrets of your own there), "
        f"and the issuer's from {a.secrets}/issuer")


def containers(api: Platform, a: argparse.Namespace) -> None:
    """--no-buckets: the project, its production Branch and the stage's."""
    if a.region not in REGIONS:
        raise Refused(f"--region {a.region} is not one of {', '.join(REGIONS)}", 2)
    ledger = Ledger(os.path.join(a.cells, f"{a.stage}.{a.app}.provision.json"), a.stage, a.app)
    if ledger.doc.setdefault("region", a.region) != a.region:
        raise Refused(f"{ledger.path} provisioned {a.stage} in {ledger.doc['region']}, not {a.region}")
    pid = project(api, ledger, a.app, a.region, "production")
    production_branch(api, ledger, pid)
    b = branch(api, ledger, pid, a.stage) if a.stage != "production" else None
    scope = f"--project {pid}" + (f" --branch {b}" if b else "")
    say(f"done (no buckets); next: set-secrets {a.stage} {scope} --app <its app> --from <its secrets directory>")


def teardown(api: Platform, a: argparse.Namespace) -> None:
    if a.stage == "production":
        raise Refused("--teardown refuses production: its buckets hold every tenant's data")
    path = os.path.join(a.cells, f"{a.stage}.provision.json")
    if not os.path.exists(path):
        raise Refused(f"{path} does not exist: nothing provisioned for {a.stage} is known here")
    ledger = Ledger(path, a.stage, a.app)
    pid = (ledger.doc.get("project") or {}).get("id")
    live = api.list(f"/buckets?projectId={urllib.parse.quote(pid)}") if pid else []
    for b in BUCKETS:
        slot = ledger.doc["buckets"].get(b) or {}
        found = [x for x in live if x.get("id") == slot.get("id") or (not slot.get("id") and x.get("name") == slot.get("name"))]
        for x in found:
            for k in api.list(f"/buckets/{urllib.parse.quote(x['id'])}/keys"):
                api.call("DELETE", f"/buckets/{urllib.parse.quote(x['id'])}/keys/{urllib.parse.quote(k['id'])}", allow=(404,))
            api.call("DELETE", f"/buckets/{urllib.parse.quote(x['id'])}", allow=(404,))
            say(f"bucket {x['id']} ({x.get('name')}) and its keys deleted")
        if slot:
            slot.update({"status": "deleted", "deleted": utc()})
    for slot in ledger.doc["keys"].values():
        slot.update({"status": "deleted"})
    ledger.save()
    say(f"{a.stage}'s buckets and keys are deleted; the key files under --secrets are dead now")


def main(argv: list) -> int:
    p = argparse.ArgumentParser(prog="provision-cell.py", add_help=True)
    p.add_argument("stage", choices=STAGES)
    p.add_argument("--app", required=True)
    p.add_argument("--region")
    p.add_argument("--cells", required=True)
    p.add_argument("--secrets")
    p.add_argument("--cell-id")
    p.add_argument("--management-api", default=API)
    p.add_argument("--teardown", action="store_true")
    p.add_argument("--no-buckets", action="store_true")
    a = p.parse_args(argv)
    try:
        if a.no_buckets and a.teardown:
            raise Refused("--no-buckets provisions no bucket to tear down: delete the app's Branch or project by hand", 2)
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,40}", a.app):
            raise Refused(f"--app {a.app} is not a lowercase project name", 2)
        if not a.teardown and (a.region is None or (a.secrets is None and not a.no_buckets)):
            raise Refused("provisioning needs --region and --secrets (--region alone with --no-buckets)", 2)
        token_file = os.environ.get("PRISMA_TOKEN_FILE")
        if not token_file:
            raise Refused("PRISMA_TOKEN_FILE must name the file holding the Management API token", 2)
        api = Platform(a.management_api, owner_only(token_file, "the token file"))
        (teardown if a.teardown else containers if a.no_buckets else provision)(api, a)
        return 0
    except Refused as e:
        print(f"provision-cell: {e}", file=sys.stderr)
        return e.code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
