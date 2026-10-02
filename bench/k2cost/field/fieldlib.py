"""Shared plumbing for the K2 cost field runs (target/k2-design.md §9.2).

Every field tool imports this module. It owns:

- the field home, `$K2_FIELD_HOME` (default ~/.streams-k2/field): secrets,
  cell files, run state and results all live there, never in the repo;
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
  a put that is verified by ranged GETs (bench/soak/build-upload.sh).

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

FIELD = os.environ.get("K2_FIELD_HOME") or os.path.expanduser("~/.streams-k2/field")
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
    delete: {"projectId", "bucketId", ...}."""
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
    """~/.streams-k2/field/cells/<cell>.env: KEY=VALUE lines, # comments,
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


# ---------------------------------------------------------------- platform API

IDEMPOTENT = {"GET", "HEAD", "DELETE", "PUT"}


class OutcomeUnknown(RuntimeError):
    """A non-idempotent call (POST) whose answer was lost: the platform may
    or may not have created the resource."""


def api(method: str, path: str, body=None, allow=(), timeout: float = 60.0):
    """(status, parsed JSON or None). Raises on a status outside 2xx unless
    listed in `allow`. Idempotent methods retry transport errors and
    429/5xx three times; a POST is never retried (a lost answer may have
    created a resource), and a POST whose answer is lost raises
    OutcomeUnknown: its caller recorded a pending ledger entry first."""
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


def cli_env() -> dict:
    env = dict(os.environ)
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
    """One compute-cli call, under cli_lock()."""
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
    return cli_json(["services", "list", "--project", project]).get("data") or []


def compute_logs(version: str, seconds: float = 45, stop_when=None, tail: int = 2000,
                 from_start: bool = True, quiet_secs: float = 0) -> list:
    """Lines of a version's platform log. `compute logs` streams forever, so
    it runs in the background (under cli_lock) and is killed after
    `seconds`, as soon as `stop_when(lines)` is true, or (quiet_secs > 0)
    once output has been quiet that long after the first line. From the
    buffer's start by default; from_start=False gives the last `tail`
    lines. The raw log is never stored."""
    args = ["logs", version, "--tail", str(tail)] + (["--from-start"] if from_start else [])
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

