"""Plumbing for lifecycle.py: the plan, operator commands, the compute CLI and
HTTP probes against the cell.

Secrets are read from files the plan names (mode 0600 or stricter) and reach
a subprocess through its environment only: nothing secret is printed, put on
a command line or written into evidence. The operator's PRISMA_* variables
never reach a subprocess unless the plan names them (compute-cli takes
PRISMA_COMPUTE_SERVICE_ID as the service of any command that names none,
bench/k2cost/field/README.md), and evidence stays in the plan's output
directory, outside the repository, never uploaded.
"""
from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
#: The compute CLI the field tools pin (bench/k2cost/field/fieldlib.py).
COMPUTE_CLI = ["bunx", "--bun", "@prisma/compute-cli@0.39.0"]
#: Owner question D1 (deploy-q5 Q1), recommended answer (a): Composer creates
#: the scratch app `streams-beta-scratch` in the Personal workspace, in
#: eu-central-1, destroyed after the rehearsal.
SCRATCH_APP = "streams-beta-scratch"
PLACEHOLDER = re.compile(r"\{(\w+)\}")
_REQUIRED = object()


def now_ms() -> int:
    return int(time.time() * 1000)


class PlanError(ValueError):
    """A plan that cannot drive the leg asked for."""


class Plan:
    """The operator's JSON plan (plan.example.json documents every key)."""

    def __init__(self, doc: dict, path: str = "<plan>"):
        self.doc = doc
        self.path = path

    @classmethod
    def load(cls, path: str) -> "Plan":
        with open(path, encoding="utf-8") as f:
            return cls(json.load(f), path)

    def get(self, dotted: str, default=_REQUIRED):
        node = self.doc
        for part in dotted.split("."):
            if not isinstance(node, dict) or part not in node:
                if default is _REQUIRED:
                    raise PlanError(f"{self.path}: `{dotted}` is required")
                return default
            node = node[part]
        return node

    def secret(self, path: str) -> str:
        """The text of a secret file: present, non-empty, not group or world readable."""
        path = os.path.expanduser(path)
        try:
            mode = os.stat(path).st_mode
        except OSError as e:
            raise PlanError(f"secret file {path}: {e.strerror}") from None
        if mode & 0o077:
            raise PlanError(f"secret file {path} is readable by others (chmod 600)")
        with open(path, encoding="utf-8") as f:
            text = f.read().strip()
        if not text:
            raise PlanError(f"secret file {path} is empty")
        return text

    def env(self) -> dict:
        """The environment of every subprocess: the operator's without any
        PRISMA_* name, plus each name the plan's `env_files` maps to a file."""
        env = {k: v for k, v in os.environ.items() if not k.startswith("PRISMA_")}
        env.pop("RUSTUP_TOOLCHAIN", None)
        for name, path in (self.get("env_files", {}) or {}).items():
            env[name] = self.secret(path)
        return env


def fill(argv: list, subs: dict) -> list:
    """`argv` with every {name} replaced; an unknown name is a plan error."""
    def one(m):
        if m.group(1) not in subs:
            raise PlanError(f"command {argv!r}: no value for {{{m.group(1)}}}")
        return str(subs[m.group(1)])
    if not isinstance(argv, list) or not argv or not all(isinstance(a, str) for a in argv):
        raise PlanError(f"a command is a non-empty list of strings, got {argv!r}")
    return [PLACEHOLDER.sub(one, a) for a in argv]


class Job:
    """A background command; its output goes to a log file in the run's out dir."""

    def __init__(self, name: str, proc: subprocess.Popen, log_path: str, t0: int):
        self.name, self.proc, self.log_path, self.t0 = name, proc, log_path, t0

    def wait(self, timeout: float | None = None) -> dict:
        try:
            rc = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.stop()
            rc = self.proc.returncode
        return {"name": self.name, "rc": rc, "t0": self.t0, "t1": now_ms(), "tail": tail(self.log_path)}

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=120)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


def tail(path: str, n: int = 4000) -> str:
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - n))
            return f.read().decode(errors="replace")
    except OSError:
        return ""


class Shell:
    """Operator commands from the plan, run with the plan's environment."""

    def __init__(self, plan: Plan, out: str, say=print):
        self.plan, self.out, self.say = plan, out, say
        self.n = 0

    def start(self, name: str, argv: list, subs: dict, cwd: str | None = None) -> Job:
        cmd = fill(argv, subs)
        self.n += 1
        log_path = os.path.join(self.out, f"cmd-{self.n:02d}-{name}.log")
        self.say(f"  run {name}: {' '.join(cmd)}")
        log = open(log_path, "wb")
        try:
            proc = subprocess.Popen(cmd, cwd=cwd or ROOT, env=self.plan.env(), stdout=log, stderr=subprocess.STDOUT)
        finally:
            log.close()
        return Job(name, proc, log_path, now_ms())

    def run(self, name: str, argv: list, subs: dict, timeout: float | None = None, cwd: str | None = None) -> dict:
        return self.start(name, argv, subs, cwd).wait(timeout)


class LogStream:
    """`compute logs <version> --json`, read in a thread: every log line with
    its arrival time, and why the stream ended (the CLI's terminal code,
    `eof`, or `closed` when this side stopped it).

    The CLI ends a stream after a 10-minute segment (terminal code
    `segment_time_limit`, exit 0) with a resume cursor, and its own hint
    says to resume with `--cursor` (compute-cli 0.39.0 src/commands/logs.ts
    handleStreamEnd). A segment end, a terminal record marked retryable, or a
    stream that stopped without a terminal record after a log record (the
    last record's byteEnd is then the cursor, as the CLI's hint takes it)
    is resumed from its cursor by `resume(cursor)`'s argv, so the lines run
    on and `end` is only ever the version's own end. Resumes that bring no
    line are bounded (MAX_EMPTY_RESUMES)."""

    RESUMABLE = ("segment_time_limit",)
    MAX_EMPTY_RESUMES = 3

    def __init__(self, argv: list, env: dict, resume=None):
        self.lines: list = []
        self.end: str | None = None
        self.resumes = 0
        self._env, self._resume = env, resume
        self._cond = threading.Condition()
        self._proc = subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        threading.Thread(target=self._read, daemon=True).start()

    def _segment(self, proc) -> tuple:
        """Reads one CLI process to its end: (terminal code or None, its
        retryable flag, the cursor to resume from, lines read)."""
        code, retryable, cursor, got = None, False, None, 0
        for raw in proc.stdout:
            try:
                rec = json.loads(raw)
            except ValueError:
                continue
            if rec.get("type") == "log":
                got += 1
                if rec.get("byteEnd") is not None:
                    cursor = str(rec["byteEnd"])
                with self._cond:
                    self.lines.extend([now_ms(), t] for t in str(rec.get("text", "")).split("\n"))
                    self._cond.notify_all()
            elif rec.get("type") == "terminal":
                code = rec.get("code") or rec.get("kind") or "terminal"
                retryable = rec.get("retryable") is True
                cursor = rec.get("cursor") or cursor
        proc.stdout.close()
        proc.wait()
        return code, retryable, cursor, got

    def _read(self) -> None:
        proc, empty = self._proc, 0
        while True:
            code, retryable, cursor, got = self._segment(proc)
            empty = 0 if got else empty + 1
            again = (code in self.RESUMABLE or retryable or code is None) and cursor and self._resume
            with self._cond:
                if not again or self.end is not None or empty > self.MAX_EMPTY_RESUMES:
                    self.end = self.end or code or "eof"
                    self._cond.notify_all()
                    return
                self.resumes += 1
                proc = self._proc = subprocess.Popen(self._resume(cursor), env=self._env, stdout=subprocess.PIPE,
                                                     stderr=subprocess.DEVNULL)

    def wait_for(self, pred, timeout: float) -> bool:
        """Until `pred(lines)` holds or the stream ends; False on timeout."""
        deadline = time.time() + timeout
        with self._cond:
            while not pred(self.lines):
                if self.end is not None:
                    return pred(self.lines)
                left = deadline - time.time()
                if left <= 0:
                    return False
                self._cond.wait(min(left, 1.0))
            return True

    def wait_end(self, timeout: float) -> str:
        if not self.wait_for(lambda _: self.end is not None, timeout):
            self.close("timeout")
        return self.end

    def close(self, why: str = "closed") -> None:
        with self._cond:
            self.end = self.end or why
            proc = self._proc
        if proc.poll() is None:
            proc.kill()
            proc.wait()

    def snapshot(self) -> list:
        with self._cond:
            return list(self.lines)


class Compute:
    """The compute CLI, authenticated from the plan's platform token file."""

    def __init__(self, plan: Plan):
        self.cli = plan.get("compute_cli", COMPUTE_CLI)
        self.env = plan.env()
        self.env["PRISMA_API_TOKEN"] = plan.secret(plan.get("platform_token_file"))

    def json(self, args: list, timeout: float = 120) -> dict:
        p = subprocess.run(self.cli + args + ["--json"], env=self.env, capture_output=True, text=True, timeout=timeout)
        start = p.stdout.find("{")
        if start < 0:
            raise RuntimeError(f"compute {' '.join(args[:2])}: no JSON (rc {p.returncode}): {p.stderr[-300:]}")
        doc, _ = json.JSONDecoder().raw_decode(p.stdout[start:])
        if doc.get("ok") is False:
            raise RuntimeError(f"compute {' '.join(args[:2])}: {json.dumps(doc.get('error'))[:300]}")
        return doc

    def warm(self) -> None:
        """One call first, alone: parallel bunx calls race its package cache."""
        subprocess.run(self.cli + ["--help"], env=self.env, capture_output=True, timeout=300)

    def versions(self, service: str) -> list:
        rows = self.json(["versions", "list", "--service", service]).get("data") or []
        return sorted(rows, key=lambda v: v.get("createdAt") or "")

    def newest(self, service: str, exclude=(), timeout: float = 0, sleep=time.sleep) -> str | None:
        """The newest version of `service` not in `exclude`, waiting up to `timeout` s for one."""
        deadline = time.time() + timeout
        while True:
            fresh = [v["id"] for v in self.versions(service) if v.get("id") not in exclude]
            if fresh or time.time() >= deadline:
                return fresh[-1] if fresh else None
            sleep(5)

    def stop(self, version: str) -> None:
        self.json(["versions", "stop", version])

    def service_named(self, project: str, part: str) -> str:
        """The one service of `project` whose name contains `part`."""
        rows = [s for s in self.json(["services", "list", "--project", project]).get("data") or []
                if part in str(s.get("name"))]
        if len(rows) != 1:
            raise RuntimeError(f"project {project}: {len(rows)} services named *{part}*: {[s.get('name') for s in rows]}")
        return rows[0]["id"]

    def logs(self, version: str, from_start: bool = False, tail_lines: int = 200) -> LogStream:
        args = ["logs", version, "--tail", str(tail_lines), "--json"] + (["--from-start"] if from_start else [])
        return LogStream(self.cli + args, self.env,
                         resume=lambda cursor: self.cli + ["logs", version, "--cursor", cursor, "--json"])


def read_headers(path: str | None) -> dict:
    """A k2gen header file: `Name: value` lines (bench/k2cost/common.ts)."""
    if not path:
        return {}
    out = {}
    with open(os.path.expanduser(path), encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if line and not line.startswith("#") and ":" in line:
                k, v = line.split(":", 1)
                out[k.strip()] = v.strip()
    return out


def request(url: str, method: str = "GET", headers: dict | None = None, timeout: float = 10.0,
            body: bytes | None = None) -> dict:
    """One request: t (ms), status (0 on a transport failure), the error
    code of a JSON error body, Retry-After, the boot id header, the body."""
    req = urllib.request.Request(url, method=method, data=body,
                                 headers={"User-Agent": "streams-lifecycle", **(headers or {})})
    t = now_ms()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            status, hdrs, data = r.status, r.headers, r.read()
    except urllib.error.HTTPError as e:
        status, hdrs = e.code, e.headers
        try:
            data = e.read()
        except Exception:  # noqa: BLE001 - an error body cut short still answers with its status
            data = b""
    except Exception as e:  # noqa: BLE001 - a probe records, never raises
        return {"t": t, "status": 0, "code": type(e).__name__, "retry_after": None, "boot": None, "body": ""}
    code = None
    try:
        err = json.loads(data).get("error")
        code = err.get("code") if isinstance(err, dict) else err
    except (ValueError, AttributeError):
        pass
    return {"t": t, "status": status, "code": code, "retry_after": hdrs.get("retry-after"),
            "boot": hdrs.get("x-streams-boot-id"), "body": data[:2000].decode(errors="replace")}


class Cell:
    """HTTP against the cell: /health (no auth), the product surface with a
    project's header file, /v1/debug/load with the operator bearer file."""

    def __init__(self, plan: Plan):
        self.plan = plan
        self.url = plan.get("cell.url").rstrip("/")

    def health(self) -> dict:
        s = request(self.url + "/health", timeout=5)
        s.pop("body")
        return s

    def product(self, path: str, headers_file: str | None = None, method: str = "GET", body: bytes | None = None) -> dict:
        headers = read_headers(headers_file or self.plan.get("cell.headers_file"))
        if body is not None:
            headers["content-type"] = "application/json"
        s = request(self.url + path, method=method, headers=headers, body=body)
        s.pop("body")
        return s

    def raw(self, path: str) -> dict:
        return request(self.url + path, timeout=10)


class Prober:
    """Calls `fn` every `every_s` seconds in a thread; `samples` keeps each result."""

    def __init__(self, fn, every_s: float):
        self.fn, self.every_s = fn, every_s
        self.samples: list = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            t = now_ms()
            try:
                self.samples.append(self.fn())
            except Exception as e:  # noqa: BLE001 - a failing probe is a sample, never a dead thread
                self.samples.append({"t": t, "status": 0, "code": f"probe {type(e).__name__}: {e}"[:200]})
            self._stop.wait(self.every_s)

    def stop(self) -> list:
        self._stop.set()
        self._thread.join(timeout=30)
        return list(self.samples)
