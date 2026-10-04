"""Shared plumbing for the K2 cost field runs (target/k2-design.md §9.2).

Every field tool imports this module. It owns:

- the field home, `$K2_FIELD_HOME` (required, no default: each home belongs
  to one workspace, ~/.streams-k2/field-pro to the K2 cost campaign and
  ~/.streams-k2/field to the per-cell runs, and nothing else ties a command
  to its home): secrets, cell files, run state and results all live there,
  never in the repo; `banner` names it on every tool's first line;
- the platform API (https://api.prisma.io/v1, `User-Agent: curl/8.7.1`,
  or Cloudflare answers 1010) and the compute CLI, both authenticated from
  `platform-token.txt` without ever putting the token on a command line;
- `resources.json`, the per-run ledger of everything a run created; every
  write is locked and atomic. `teardown.py` acts only on what it lists, and
  only after the live platform object agrees with it;
- the compute-CLI lock: every `bunx` subprocess of every field tool (and
  every `bun install` of a staged app) holds `$K2_FIELD_HOME/.cli.lock`, so
  a second tool blocks instead of racing bunx's package cache;
- the S3 clients for a cell's data bucket and for the artifact bucket, and
  a put that is verified by ranged GETs (bench/soak/build-upload.sh);
- campaign-project mode (`campaign.json`, written by campaign.py): every
  cell of every run lives in ONE project, and `gate_api` / `gate_cli`
  refuse, before anything is sent, any platform call that could address
  another project or anything in it (see "campaign mode" below).

Secrets are read from files and handed to subprocesses through the
environment or 0600 files only. `redact` is the one place that decides
what a stored or printed environment may show.
"""
from __future__ import annotations

import base64
import contextlib
import fcntl
import glob
import json
import os
import re
import secrets as _secrets
import select
import socket
import string
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def _field_home() -> str:
    """$K2_FIELD_HOME, never a default: a command typed without the export
    must not fall into another workspace's home (its token, artifact bucket
    and ledgers, and per-cell mode, which no gate scopes)."""
    home = (os.environ.get("K2_FIELD_HOME") or "").strip()
    if not home:
        sys.stderr.write("FATAL: K2_FIELD_HOME is not set: export the field home this command is for "
                         "(the K2 cost campaign: ~/.streams-k2/field-pro; the per-cell runs: ~/.streams-k2/field)\n")
        sys.exit(2)
    return os.path.abspath(os.path.expanduser(home))


FIELD = _field_home()
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
REGION = "eu-central-1"
API = "https://api.prisma.io/v1"
UA = "curl/8.7.1"
CLI = ["bunx", "--bun", "@prisma/compute-cli@0.39.0"]
PATH_PREFIX = "k2d"   # data prefix inside each cell's own bucket
FLEET_PREFIX = "k2f"  # fleet coordination prefix inside each cell's own bucket
RUN_RE = re.compile(r"^[a-z0-9]{1,12}$")
CELL_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,15}$")
SECRET_ENV = re.compile(r"(TOKEN|SECRET|PASSWORD)|(^|_)ACCESS_KEY_ID$|^USAGE_STREAM_KEY$|^STREAMS_CURSOR_KEY$")
OBJECT_KEY_ENV = re.compile(r"_S3_KEY$")  # object names, not secrets


def die(msg: str, code: int = 1) -> None:
    sys.stderr.write(f"FATAL: {msg}\n")
    sys.exit(code)


def say(msg: str) -> None:
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def now_ms() -> int:
    return int(time.time() * 1000)


def utc(ms: int | None = None) -> str:
    t = (ms if ms is not None else now_ms()) / 1000
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


def check_names(run: str, cell: str | None = None) -> None:
    if not RUN_RE.match(run):
        die(f"run id {run!r} must match {RUN_RE.pattern}")
    if cell is not None and not CELL_RE.match(cell):
        die(f"cell {cell!r} must match {CELL_RE.pattern}")
    camp = campaign_doc()
    if camp:  # a run's k2c-<run>- prefix must never cover the campaign's own names
        own = [camp.get("name") or "", ((camp.get("artifact_bucket") or {}).get("name") or "")]
        if any((n + "-").startswith(f"k2c-{run}-") for n in own if n):
            die(f"run id {run!r}: k2c-{run}- would name the campaign's own project or artifact bucket {own}")


def base_name(run: str, cell: str) -> str:
    """Every platform resource of a cell is named from this: k2c-<run>-<cell>."""
    return f"k2c-{run}-{cell}"


# ---------------------------------------------------------------- files

def field_file(name: str) -> str:
    path = os.path.join(FIELD, name)
    try:
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        die(f"missing {path}")
    return ""


def artifact_receipt() -> dict:
    """The artifact project and bucket, which no field tool may modify or
    delete: {"projectId", "bucketId", ...}. In campaign mode projectId is
    the campaign project (runs deploy into it; none deletes it) and the
    bucket is only ever deleted by campaign.py destroy."""
    rc = json.loads(field_file("artifact-platform-receipt.json"))
    if not rc.get("projectId") or not rc.get("bucketId"):
        die("artifact-platform-receipt.json lacks projectId or bucketId")
    return rc


def artifact_project_id() -> str:
    return artifact_receipt()["projectId"]


def foreign_ids(run: str) -> dict:
    """Every project, bucket and service id another run's ledger lists, by
    id -> "<run>/<cell>": no tool of this run may touch them."""
    out: dict = {}
    for path in glob.glob(os.path.join(FIELD, "runs", "*", "resources.json")):
        other = os.path.basename(os.path.dirname(path))
        if other == run:
            continue
        doc = read_json(path) or {}
        for cell, c in (doc.get("cells") or {}).items():
            ids = [(c.get("project") or {}).get("id"), (c.get("bucket") or {}).get("id")]
            ids += [s.get("id") for s in (c.get("services") or {}).values()]
            for i in ids:
                if i:
                    out[i] = f"{other}/{cell}"
    return out


def run_dir(run: str) -> str:
    d = os.path.join(FIELD, "runs", run)
    os.makedirs(d, mode=0o700, exist_ok=True)
    return d


def cell_dir(run: str, cell: str) -> str:
    d = os.path.join(run_dir(run), cell)
    os.makedirs(d, mode=0o700, exist_ok=True)
    return d


def secrets_dir(run: str, cell: str) -> str:
    d = os.path.join(cell_dir(run, cell), "secrets")
    os.makedirs(d, mode=0o700, exist_ok=True)
    os.chmod(d, 0o700)
    return d


def results_dir(run: str, cell: str | None = None) -> str:
    d = os.path.join(FIELD, "results", run, *( [cell] if cell else []))
    os.makedirs(d, exist_ok=True)
    return d


def write_secret(path: str, text: str) -> None:
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def read_secret(run: str, cell: str, name: str) -> str:
    with open(os.path.join(secrets_dir(run, cell), name), encoding="utf-8") as f:
        return f.read().strip()


def ensure_secret(run: str, cell: str, name: str, make) -> str:
    path = os.path.join(secrets_dir(run, cell), name)
    if not os.path.exists(path):
        write_secret(path, make())
    return read_secret(run, cell, name)


def write_json(path: str, doc) -> None:
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=1, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


def read_json(path: str, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default


def append_jsonl(path: str, rec: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, separators=(",", ":"), sort_keys=True) + "\n")


def rand_id(prefix: str, n: int = 24) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return prefix + "".join(_secrets.choice(alphabet) for _ in range(n))


def b64key() -> str:
    return base64.b64encode(_secrets.token_bytes(32)).decode()


def redact(env: dict) -> dict:
    out = {}
    for k, v in sorted(env.items()):
        secret = SECRET_ENV.search(k) and not OBJECT_KEY_ENV.search(k)
        out[k] = "<redacted>" if secret else v
    return out


# ---------------------------------------------------------------- cell files

def read_cell_file(cell: str) -> dict:
    """$K2_FIELD_HOME/cells/<cell>.env: KEY=VALUE lines, # comments,
    no shell expansion. Holds no secrets."""
    path = os.path.join(FIELD, "cells", f"{cell}.env")
    if not os.path.exists(path):
        die(f"no cell file {path} (copy one from bench/k2cost/field/cells/)")
    out: dict = {}
    with open(path, encoding="utf-8") as f:
        for n, raw in enumerate(f, 1):
            line = raw.split(" #", 1)[0].strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                die(f"{path}:{n}: expected KEY=VALUE")
            k, v = line.split("=", 1)
            v = v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
                v = v[1:-1]
            out[k.strip()] = v
    return out


def cell_int(cfg: dict, key: str, default: int) -> int:
    try:
        return int(cfg.get(key, default))
    except ValueError:
        die(f"cell value {key}={cfg.get(key)!r} is not an integer")
    return default


# ---------------------------------------------------------------- resources

@contextlib.contextmanager
def resources(run: str):
    """Locked read-modify-write of runs/<run>/resources.json. The body
    mutates the yielded dict; it is written atomically on exit."""
    d = run_dir(run)
    with open(os.path.join(d, ".lock"), "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = os.path.join(d, "resources.json")
        doc = read_json(path) or {"run_id": run, "region": REGION, "created": utc(), "cells": {}}
        yield doc
        doc["updated"] = utc()
        write_json(path, doc)


def load_resources(run: str) -> dict:
    return read_json(os.path.join(run_dir(run), "resources.json")) or {"run_id": run, "cells": {}}


def cell_res(doc: dict, cell: str) -> dict:
    return doc["cells"].setdefault(cell, {"services": {}, "artifact_objects": [], "keep_awake": []})


# ---------------------------------------------------------------- campaign mode
#
# $K2_FIELD_HOME/campaign.json (campaign.py init) names ONE project that holds
# every cell's services and buckets plus the artifact bucket. Without it a
# field home runs per-cell projects (the original mode), unless it holds
# `campaign-required`, which makes it refuse per-cell mode outright.

CAMPAIGN_RE = re.compile(r"^k2c-[a-z0-9][a-z0-9-]{0,40}$")
_VERIFIED: dict = {"bucket": set(), "service": set(), "deployment": set()}
_PERMIT: set = set()
_CHECKED: dict = {}


class ScopeError(RuntimeError):
    """A platform call outside the campaign project's scope, refused before
    it was sent."""


def campaign_path() -> str:
    return os.path.join(FIELD, "campaign.json")


def campaign_doc() -> dict | None:
    """campaign.json as stored (any status), or None."""
    return read_json(campaign_path())


def campaign(require_ready: bool = True) -> dict | None:
    """The campaign record in campaign mode, None in per-cell mode."""
    doc = campaign_doc()
    if doc is None:
        if os.path.exists(os.path.join(FIELD, "campaign-required")):
            die(f"{FIELD} runs only in campaign mode and has no campaign.json: campaign.py init <name> first")
        return None
    if not CAMPAIGN_RE.match(str(doc.get("name") or "")) or (doc.get("project") or {}).get("name") != doc["name"]:
        die(f"campaign.json: name {doc.get('name')!r} must match {CAMPAIGN_RE.pattern} and be the project's")
    if require_ready and (doc.get("status") != "ready" or not doc["project"].get("id")
                          or not (doc.get("artifact_bucket") or {}).get("id")):
        die(f"campaign {doc['name']} is {doc.get('status')!r}, not ready (campaign.py show / init)")
    return doc


def campaign_check(doc: dict) -> dict:
    """Refuse to go on unless the campaign project is live under its
    recorded name (and workspace): one GET /v1/projects/{id}."""
    p = doc["project"]
    st, body = api("GET", f"/projects/{p['id']}", allow=(404, 403))
    live = (body or {}).get("data") or {}
    ws = (p.get("workspace") or {}).get("id")
    if st != 200 or live.get("id") != p["id"] or live.get("name") != p["name"] or (
            ws and (live.get("workspace") or {}).get("id") != ws):
        die(f"campaign project {p['id']}: GET answered {st}, live name {live.get('name')!r} "
            f"(workspace {(live.get('workspace') or {}).get('id')}) is not the recorded {p['name']!r} "
            f"({ws}): refusing to run")
    _CHECKED[p["id"]] = live
    return live


def campaign_owns(kind: str, rid: str, name: str | None = None, prefix: str = "k2c-",
                  service_id: str | None = None) -> tuple:
    """("live" | "gone" | "refuse", reason) for a bucket, service or
    deployment id, from a GET of the LIVE object. "live" only when its live
    project is the campaign project (a deployment: its live service is
    `service_id`, itself verified) and its live name is `name` (if given)
    and starts with `prefix`; only then is the id registered, and only a
    registered id passes the mutation gate."""
    pid = ((campaign_doc() or {}).get("project") or {}).get("id")
    if not pid:
        return "refuse", "no campaign project id"
    st, body = api("GET", f"/{kind}s/{rid}", allow=(404, 403))
    if st == 404:
        return "gone", None
    if st != 200:
        return "refuse", f"{kind} {rid}: GET answered {st}; cannot verify its project"
    live = (body or {}).get("data") or {}
    if live.get("id") != rid:
        return "refuse", f"{kind} {rid}: GET returned id {live.get('id')}"
    if kind == "deployment":
        if not service_id or live.get("serviceId") != service_id or service_id not in _VERIFIED["service"]:
            return "refuse", f"deployment {rid}: live service {live.get('serviceId')} is not the verified {service_id}"
    else:
        lp = live.get("projectId") if kind == "service" else (live.get("project") or {}).get("id")
        if lp != pid:
            return "refuse", f"{kind} {rid}: live project {lp} is not the campaign project {pid}"
        lname = live.get("name") or ""
        if (name is not None and lname != name) or not lname.startswith(prefix):
            return "refuse", f"{kind} {rid}: live name {lname!r} is not {name!r} or lacks {prefix}"
    _VERIFIED[kind].add(rid)
    return "live", None


def register_created_bucket(data: dict) -> None:
    """A bucket this process just created: registered only if the POST's
    answer puts it in the campaign project."""
    pid = ((campaign_doc() or {}).get("project") or {}).get("id")
    if pid and (data.get("project") or {}).get("id") == pid:
        _VERIFIED["bucket"].add(data["id"])
    elif pid:
        die(f"bucket {data.get('id')} was created in {(data.get('project') or {}).get('id')}, not {pid}")


@contextlib.contextmanager
def permit(what: str):
    """campaign.py destroy only: "artifact-bucket-delete", "campaign-project-delete"."""
    _PERMIT.add(what)
    try:
        yield
    finally:
        _PERMIT.discard(what)


def stamp_campaign(doc: dict, camp: dict) -> None:
    """Bind a run ledger to the campaign project (inside resources())."""
    pid = camp["project"]["id"]
    have = (doc.get("campaign") or {}).get("project_id")
    if have and have != pid:
        die(f"run {doc.get('run_id')} belongs to campaign project {have}, not {pid}")
    if any(c.get("project") for c in doc["cells"].values()):
        die(f"run {doc.get('run_id')} has per-cell projects: campaign mode refuses it")
    doc["campaign"] = {"name": camp["name"], "project_id": pid}


def run_project(run: str, cell: str) -> str | None:
    """The project a cell's services are deployed into: the campaign project
    (live-checked once per process) or, in per-cell mode, the cell's own."""
    doc = load_resources(run)
    c = doc["cells"].get(cell) or {}
    camp = campaign()
    if not camp:
        if doc.get("campaign"):
            die(f"run {run} is a campaign run but {FIELD} has no campaign.json")
        return (c.get("project") or {}).get("id")
    pid = camp["project"]["id"]
    if (doc.get("campaign") or {}).get("project_id") != pid or c.get("project"):
        die(f"run {run} was not provisioned in campaign {camp['name']} ({pid}): refusing")
    if pid not in _CHECKED:
        campaign_check(camp)
    return pid


def banner(tool: str) -> None:
    """The first line of every tool's output (stderr, so a JSON stdout stays
    clean): the field home and its campaign, i.e. what the command acts on."""
    doc = campaign_doc()
    if doc:
        mode = f"campaign {doc.get('name')} (project {(doc.get('project') or {}).get('id')}, {doc.get('status')})"
    elif os.path.exists(os.path.join(FIELD, "campaign-required")):
        mode = "campaign-required, no campaign.json yet"
    else:
        mode = "per-cell mode (no campaign.json)"
    sys.stderr.write(f"[{tool}] field home {FIELD}: {mode}\n")
    sys.stderr.flush()


def project_env_rows(project: str) -> list:
    """The production variables of `project`, names and flags only (the API
    returns no values). A row that names another project, or none, raises
    ScopeError: the listing ignored its projectId filter, and the compute
    CLI's own lookups behind every deploy's --env and --unset-env
    (compute-sdk 0.39.0 #applyEnvVars: list {projectId, class, key}, then
    PATCH or DELETE existing[0] by id without checking its project) depend
    on the same filter, outside gate_api's sight."""
    rows = api_list(f"/environment-variables?projectId={project}&class=production&limit=100")
    bad = sorted({(str(r.get("key")), str(r.get("projectId"))) for r in rows if r.get("projectId") != project})
    if bad:
        raise ScopeError(f"GET /environment-variables?projectId={project} returned rows of another project (or "
                         f"none): {bad[:8]}: the filter is not honoured, so no deploy may run")
    return rows


# ---------------------------------------------------------------- platform API

IDEMPOTENT = {"GET", "HEAD", "DELETE", "PUT"}


class OutcomeUnknown(RuntimeError):
    """A non-idempotent call (POST) whose answer was lost: the platform may
    or may not have created the resource."""


def _scope() -> tuple:
    """("percell" | "required" | "campaign", campaign.json or None)."""
    doc = campaign_doc()
    if doc is not None:
        return "campaign", doc
    return ("required" if os.path.exists(os.path.join(FIELD, "campaign-required")) else "percell"), None


def gate_api(method: str, path: str, body=None) -> None:
    """Campaign mode: raise ScopeError for any API call that could address
    a project other than the campaign's, or anything in one. Allowed: GET of
    the campaign project; GET of one bucket, service or deployment by id
    (the ownership check itself); the workspace-wide GET /projects (only to
    find our own k2c- project by exact name); /services, /buckets,
    /databases and /environment-variables listings scoped to the campaign
    project; POST /projects for the pending campaign project only; POST
    /buckets into the campaign project; keys and DELETE only for buckets
    verified in it (campaign_owns); DELETE of the campaign project and the
    artifact bucket only under permit() (campaign.py destroy)."""
    mode, doc = _scope()
    if mode == "percell":
        return
    camp = doc or {}
    pid = (camp.get("project") or {}).get("id")
    base, _, query = path.partition("?")
    q = urllib.parse.parse_qs(query)
    parts = base.strip("/").split("/")

    def no(why: str) -> None:
        raise ScopeError(f"{method} {base}: {why} (campaign {camp.get('name')}, project {pid})")
    if method == "GET":
        if len(parts) == 2 and parts[0] == "projects":
            return None if pid and parts[1] == pid else no("the only project read by id is the campaign's")
        if len(parts) == 2 and parts[0] in ("buckets", "services", "deployments"):
            return None  # read-only, by id: the ownership check before any mutation
        if parts == ["projects"]:
            return None  # read-only; callers keep only the exact k2c- campaign name
        if len(parts) == 1 and parts[0] in ("services", "buckets", "databases", "environment-variables"):
            return None if pid and q.get("projectId") == [pid] else no("a listing must be scoped to the campaign project")
        return no("not a read these tools make")
    if mode == "required":
        return no("this field home requires campaign mode and has no campaign.json")
    if method == "POST" and parts == ["projects"]:
        p = camp.get("project") or {}
        b = body or {}
        if p.get("id") or p.get("status") != "pending":
            return no("the campaign project exists; no tool creates another project")
        if b.get("name") != p.get("name") or b.get("region") != REGION or b.get("createDatabase") is not False:
            return no("only the pending campaign project, in eu-central-1, without a database")
        return None
    if method == "POST" and parts == ["buckets"]:
        b = body or {}
        if not pid or b.get("projectId") != pid or not str(b.get("name") or "").startswith("k2c-"):
            return no("a bucket is created only in the campaign project, named k2c-*")
        return None
    if len(parts) >= 2 and parts[0] == "buckets" and (
            (method == "POST" and parts[2:] == ["keys"]) or (method == "DELETE" and len(parts) == 2)):
        if parts[1] not in _VERIFIED["bucket"]:
            return no(f"bucket {parts[1]} is not verified in the campaign project")
        art = (camp.get("artifact_bucket") or {}).get("id")
        if method == "DELETE" and parts[1] == art and "artifact-bucket-delete" not in _PERMIT:
            return no("the artifact bucket goes only with campaign.py destroy")
        return None
    if method == "DELETE" and len(parts) == 2 and parts[0] == "projects":
        if parts[1] != pid or "campaign-project-delete" not in _PERMIT:
            return no("only campaign.py destroy deletes a project, and only the campaign's")
        return None
    return no("not a call these tools make")


def gate_cli(args: list) -> None:
    """Campaign mode: raise ScopeError for any compute-cli call outside the
    campaign project. Allowed: `logs <version>` and `versions show
    <version>` (read-only); `services list --project <campaign>`; `deploy
    --project <campaign>` (a new k2c- service by name, or `--service` a
    verified one; its --unset-env keys are this project's variables);
    `services destroy` and `versions stop` of verified ids only. Nothing
    at all while the shell exports a variable the CLI would act on
    (CLI_STRAY_ENV; cli_env drops them too)."""
    mode, doc = _scope()
    if mode == "percell":
        return
    camp = doc or {}
    pid = (camp.get("project") or {}).get("id")
    head = list(args[:2])

    def opt(name: str) -> list:
        return [args[i + 1] for i, a in enumerate(args[:-1]) if a == name]

    def no(why: str) -> None:
        raise ScopeError(f"compute {' '.join(head)}: {why} (campaign {camp.get('name')}, project {pid})")
    stray = sorted(k for k in CLI_STRAY_ENV if os.environ.get(k))
    if stray:
        return no(f"this shell exports {stray}: unset them (the CLI would deploy into that service or call that API)")
    if args[:1] == ["logs"] or head == ["versions", "show"]:
        return None
    if mode == "required" or not pid:
        return no("no campaign project")
    if head == ["services", "list"]:
        return None if opt("--project") == [pid] else no("services are listed only in the campaign project")
    if head == ["services", "destroy"]:
        return None if len(args) > 2 and args[2] in _VERIFIED["service"] else no("service not verified in the campaign")
    if head == ["versions", "stop"]:
        return None if len(args) > 2 and args[2] in _VERIFIED["deployment"] else no("version not verified in the campaign")
    if args[:1] == ["deploy"]:
        sid, names = opt("--service"), opt("--service-name")
        if opt("--project") != [pid]:
            return no("deploys go only into the campaign project")
        if sid and (len(sid) != 1 or sid[0] not in _VERIFIED["service"] or names):
            return no(f"redeploy of service {sid} that is not verified in the campaign project")
        if not sid and (len(names) != 1 or not names[0].startswith("k2c-")):
            return no("a new service must be named k2c-*")
        return None
    return no("not a compute call these tools make")


def api(method: str, path: str, body=None, allow=(), timeout: float = 60.0):
    """(status, parsed JSON or None). Raises on a status outside 2xx unless
    listed in `allow`. Idempotent methods retry transport errors and
    429/5xx three times; a POST is never retried (a lost answer may have
    created a resource), and a POST whose answer is lost raises
    OutcomeUnknown: its caller recorded a pending ledger entry first. In
    campaign mode gate_api runs first and may raise ScopeError."""
    gate_api(method, path, body)
    data = json.dumps(body).encode() if body is not None else None
    retries = 3 if method in IDEMPOTENT else 0
    for attempt in range(retries + 1):
        req = urllib.request.Request(
            f"{API}{path}", method=method, data=data,
            headers={"Authorization": f"Bearer {field_file('platform-token.txt')}",
                     "Content-Type": "application/json", "User-Agent": UA})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                return r.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                doc = json.loads(raw) if raw else None
            except ValueError:
                doc = {"raw": raw[:200].decode(errors="replace")}
            if e.code in allow:
                return e.code, doc
            if (e.code == 429 or e.code >= 500) and attempt < retries:
                time.sleep(2 + 3 * attempt)
                continue
            if e.code >= 500 and method not in IDEMPOTENT:
                raise OutcomeUnknown(f"{method} {path}: HTTP {e.code} {json.dumps(doc)[:300]}") from None
            raise RuntimeError(f"{method} {path}: HTTP {e.code} {json.dumps(doc)[:300]}") from None
        # socket.timeout is not TimeoutError before Python 3.10; OSError
        # covers it, ConnectionError and TimeoutError.
        except (urllib.error.URLError, socket.timeout, OSError) as e:
            if attempt < retries:
                time.sleep(2 + 3 * attempt)
                continue
            if method not in IDEMPOTENT:
                raise OutcomeUnknown(f"{method} {path}: answer lost ({e})") from None
            raise RuntimeError(f"{method} {path}: {e}") from None
    raise RuntimeError("unreachable")


def api_list(path: str) -> list:
    """Every item of a paginated list endpoint."""
    out, cursor = [], None
    for _ in range(200):
        sep = "&" if "?" in path else "?"
        _, doc = api("GET", path + (f"{sep}cursor={urllib.parse.quote(cursor)}" if cursor else ""))
        out.extend(doc.get("data") or [])
        pg = doc.get("pagination") or {}
        cursor = pg.get("nextCursor")
        if not pg.get("hasMore") or not cursor:
            break
    return out


# compute-cli 0.39.0 acts on these: PRISMA_COMPUTE_SERVICE_ID is the
# --service of any deploy that names none (helpers.ts resolveServiceId), so a
# `--service-name k2c-*` deploy would redeploy that unverified service, and
# PRISMA_MANAGEMENT_API_URL points the CLI at another API than API.
CLI_STRAY_ENV = ("PRISMA_COMPUTE_SERVICE_ID", "PRISMA_MANAGEMENT_API_URL")


def cli_env() -> dict:
    """The operator's environment without any PRISMA_* variable (the
    service id, API URL and auth file the CLI would otherwise honour, or
    another workspace's token), plus this field home's token."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("PRISMA_")}
    env["PRISMA_API_TOKEN"] = field_file("platform-token.txt")
    env.pop("RUSTUP_TOOLCHAIN", None)
    return env


NOISE = re.compile(r"resolving|resolved|saved lockfile|downloaded and extracted", re.I)
_CLI_DEPTH = [0]


@contextlib.contextmanager
def cli_lock():
    """Exclusive, machine-wide (per field home) lock around every bunx or
    `bun install` subprocess: parallel bunx races its package cache. A
    second field tool blocks here; re-entrant within one process."""
    if _CLI_DEPTH[0]:
        _CLI_DEPTH[0] += 1
        try:
            yield
        finally:
            _CLI_DEPTH[0] -= 1
        return
    os.makedirs(FIELD, exist_ok=True)
    with open(os.path.join(FIELD, ".cli.lock"), "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            sys.stderr.write("  (waiting for another field tool's compute-cli call)\n")
            fcntl.flock(lock, fcntl.LOCK_EX)
        _CLI_DEPTH[0] = 1
        try:
            yield
        finally:
            _CLI_DEPTH[0] = 0
            fcntl.flock(lock, fcntl.LOCK_UN)


def cli(args: list, timeout: float = 600, cwd: str | None = None) -> subprocess.CompletedProcess:
    """One compute-cli call, under cli_lock() (gate_cli first)."""
    gate_cli(args)
    with cli_lock():
        p = subprocess.run(CLI + args, env=cli_env(), cwd=cwd, capture_output=True, text=True, timeout=timeout)
    p.stdout = "\n".join(l for l in p.stdout.splitlines() if not NOISE.search(l))
    p.stderr = "\n".join(l for l in p.stderr.splitlines() if not NOISE.search(l))
    return p


def cli_json(args: list, timeout: float = 600) -> dict:
    p = cli(args + ["--json"], timeout=timeout)
    start = p.stdout.find("{")
    if start < 0:
        raise RuntimeError(f"compute {' '.join(args[:2])}: no JSON (rc {p.returncode}): {p.stderr[-400:]}")
    # The first JSON document; the CLI may print more after it.
    doc, _ = json.JSONDecoder().raw_decode(p.stdout[start:])
    if not doc.get("ok", False):
        raise RuntimeError(f"compute {' '.join(args[:2])}: {json.dumps(doc.get('error'))[:400]}")
    return doc


def services_in(project: str) -> list:
    """`compute services list --project`; an item that names another
    project is dropped, never acted on."""
    rows = cli_json(["services", "list", "--project", project]).get("data") or []
    return [s for s in rows if s.get("projectId") in (None, project)]


def compute_logs(version: str, seconds: float = 45, stop_when=None, tail: int = 2000,
                 from_start: bool = True, quiet_secs: float = 0) -> list:
    """Lines of a version's platform log. `compute logs` streams forever, so
    it runs in the background (under cli_lock) and is killed after
    `seconds`, as soon as `stop_when(lines)` is true, or (quiet_secs > 0)
    once output has been quiet that long after the first line. From the
    buffer's start by default; from_start=False gives the last `tail`
    lines. The raw log is never stored."""
    args = ["logs", version, "--tail", str(tail)] + (["--from-start"] if from_start else [])
    gate_cli(args)
    with cli_lock():
        p = subprocess.Popen(CLI + args, env=cli_env(), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        lines: list = []
        deadline = time.time() + seconds
        last = None
        buf = b""
        try:
            while time.time() < deadline:
                ready, _, _ = select.select([p.stdout], [], [], 0.5)
                if not ready:
                    if quiet_secs and last is not None and time.time() - last >= quiet_secs:
                        break
                    continue
                chunk = os.read(p.stdout.fileno(), 65536)
                if not chunk:
                    break
                last = time.time()
                buf += chunk
                *done, buf = buf.split(b"\n")
                lines.extend(l.decode(errors="replace") for l in done)
                if stop_when and stop_when(lines):
                    break
        finally:
            p.kill()
            p.wait()
    return lines


# ---------------------------------------------------------------- S3

def _boto(endpoint: str, key_id: str, secret: str):
    import boto3
    from botocore.config import Config
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=key_id,
                        aws_secret_access_key=secret, region_name="auto",
                        config=Config(retries={"max_attempts": 4, "mode": "standard"},
                                      connect_timeout=10, read_timeout=60))


def artifact_s3():
    return (_boto(field_file("artifact-endpoint.txt"), field_file("binid.txt"), field_file("binsec.txt")),
            field_file("artifact-bucket.txt"))


def bucket_keys(run: str, cell: str) -> dict:
    with open(os.path.join(secrets_dir(run, cell), "bkey.json"), encoding="utf-8") as f:
        return json.load(f)["data"]


def data_s3(run: str, cell: str):
    k = bucket_keys(run, cell)
    return _boto(k["endpoint"], k["accessKeyId"], k["secretAccessKey"]), k["bucketName"]


def put_verified(client, bucket: str, key: str, data: bytes) -> None:
    """PUT, then prove it with ranged GETs of the first and last 16 bytes:
    HEAD alone has lied before (bench/soak/build-upload.sh)."""
    client.put_object(Bucket=bucket, Key=key, Body=data)
    n = len(data)
    head = client.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{min(15, n - 1)}")["Body"].read()
    tail = client.get_object(Bucket=bucket, Key=key, Range=f"bytes={max(0, n - 16)}-{n - 1}")["Body"].read()
    if head != data[:16] or tail != data[-16:]:
        raise RuntimeError(f"ranged-GET mismatch for {key}")


def record_artifact_object(run: str, cell: str, key: str) -> None:
    if not key.startswith(f"k2c/{run}/"):
        die(f"artifact object {key} is outside k2c/{run}/")
    with resources(run) as doc:
        objs = cell_res(doc, cell)["artifact_objects"]
        if key not in objs:
            objs.append(key)


# ---------------------------------------------------------------- HTTP probes

def http_get(url: str, headers: dict | None = None, timeout: float = 10.0):
    """(status, body bytes, response headers); status 0 on transport failure."""
    req = urllib.request.Request(url, headers={"User-Agent": "k2c-field", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read(), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read(), dict(e.headers or {})
    except Exception as e:  # noqa: BLE001 - a probe never raises
        return 0, str(e).encode(), {}

