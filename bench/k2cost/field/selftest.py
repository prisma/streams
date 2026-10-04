#!/usr/bin/env python3
"""Offline checks of the K2 field tooling: no network, no platform, no
secrets. Every platform and CLI call is a stub; the field home is a fresh
temporary directory.

    python3 bench/k2cost/field/selftest.py      # prints PASS/FAIL per check

Covers: teardown's per-step isolation (a failing bucket DELETE leaves the
other cells' KEEP_AWAKE services destroyed and teardown.json written), its
live guards (artifact bucket id, another run's ids, a live-name or live
project mismatch, an unlistable project), pending-entry resolution,
show() never reading argv, --expired and --service; fieldlib.api never
retrying a POST (socket.timeout included); the CLI lock blocking a second
process; the observer's EWMA inversion on the review's worked example; and
gen.py's router spreading.

Campaign mode (test_campaign_*), against a stub workspace that also holds
unrelated projects ("ppg-bench", "streams-demo", "cool-app"), buckets
("abba", "pink f") and a service, whose listings IGNORE their projectId
filter: the scope gates refuse every out-of-scope API and compute call
before it is sent; a run teardown removes only its own ledger-listed
services and buckets, never the campaign project, and refuses a live
project elsewhere, a non-k2c-<run>- name, the artifact bucket, another
run's ids, a live-name mismatch and an unlisted service of the cell; a
renamed campaign project stops it before anything but one GET;
--expired/--service; campaign init (pending before the POST, a lost
answer resolved by name and never re-POSTed, a taken name or foreign
artifact files refused, the files written); verify's scoped listings;
destroy (dry run, refusal of a non-k2c- object, the whole teardown with
404 verification); provision (no project, buckets in the campaign
project); deploy (the --unset-env list is the campaign project's own
variables, computed and every key probed under the CLI lock; an env row
of another project or of none, or a keyed lookup returning a foreign row
or two rows, refuses before any compute deploy; a version snapshot with an
unstated KEEP_AWAKE stops it; a redeploy over, or a --kill of, a service
or version outside the campaign is refused); verify's env names.

Every tool refuses to start without K2_FIELD_HOME and names the home and
campaign on its first line; the CLI never sees the shell's PRISMA_*
variables, and campaign mode refuses while PRISMA_COMPUTE_SERVICE_ID or
PRISMA_MANAGEMENT_API_URL is exported.
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import types
import urllib.error
import urllib.parse

HOME = tempfile.mkdtemp(prefix="k2f-selftest-")
os.environ["K2_FIELD_HOME"] = HOME
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fieldlib as F  # noqa: E402
import teardown as T  # noqa: E402

ART_P, ART_B = "proj_art", "bkt_art"
RESULTS = []
REAL_SERVICES_IN = F.services_in


def check(name: str, ok, detail: str = "") -> None:
    ok = bool(ok)
    RESULTS.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name}{'' if ok else ': ' + detail}")


def setup_home(ledgers: dict) -> None:
    for d in ("runs", "results", "cells"):
        os.makedirs(os.path.join(HOME, d), exist_ok=True)
    for name in ("campaign.json", "campaign-required"):  # per-cell mode unless a test sets a campaign
        if os.path.exists(os.path.join(HOME, name)):
            os.remove(os.path.join(HOME, name))
    for s in F._VERIFIED.values():
        s.clear()
    F._CHECKED.clear()
    with open(os.path.join(HOME, "artifact-platform-receipt.json"), "w") as f:
        json.dump({"projectId": ART_P, "bucketId": ART_B, "bucketName": "art", "purpose": "x", "region": "x"}, f)
    for run, doc in ledgers.items():
        os.makedirs(os.path.join(HOME, "runs", run), exist_ok=True)
        F.write_json(os.path.join(HOME, "runs", run, "resources.json"), doc)


def cell(p: str, b: str, run: str, c: str, services: dict, ka: list | None = None) -> dict:
    base = f"k2c-{run}-{c}"
    return {"project": {"id": p, "name": base}, "bucket": {"id": b, "name": base},
            "services": {f"{base}-{k}": {"id": v, "name": f"{base}-{k}", "status": "deployed",
                                         **({"keep_awake": True, "keep_awake_expires": "2000-01-01T00:00:00Z"} if ka and k in ka else {})}
                         for k, v in services.items()},
            "artifact_objects": [f"k2c/{run}/{c}/feeds.json"], "keep_awake": []}


class Platform:
    """Stub of the platform API and compute CLI over a dict of live objects."""

    def __init__(self, projects: dict, buckets: dict, services: dict, fail: dict | None = None):
        self.projects, self.buckets, self.services = projects, buckets, services
        self.fail, self.calls = fail or {}, []

    def api(self, method, path, body=None, allow=(), timeout=60.0):
        F.gate_api(method, path, body)  # a no-op in per-cell mode
        self.calls.append(("api", method, path))
        if (method, path) in self.fail:
            raise RuntimeError(f"{method} {path}: HTTP {self.fail[(method, path)]}")
        kind, _, rid = path.strip("/").partition("/")
        table = {"projects": self.projects, "buckets": self.buckets}.get(kind)
        if method == "GET" and rid:
            if rid in table:
                return 200, {"data": table[rid]}
            if 404 in allow:
                return 404, None
            raise RuntimeError("404")
        if method == "DELETE":
            table.pop(rid, None)
            return 204, None
        raise RuntimeError(f"unexpected {method} {path}")

    def api_list(self, path):
        self.calls.append(("list", path))
        if path.startswith("/projects"):
            return list(self.projects.values())
        if path.startswith("/buckets"):
            return list(self.buckets.values())
        return []

    def services_in(self, project):
        self.calls.append(("services", project))
        if project in self.fail.get("list", ()):
            raise RuntimeError("compute services list: no JSON")
        return [s for s in self.services.values() if s["project"] == project]

    def cli(self, args, timeout=600, cwd=None):
        F.gate_cli(args)  # a no-op in per-cell mode
        self.calls.append(("cli", tuple(args)))
        if args[:2] == ["services", "destroy"]:
            self.services.pop(args[2], None)
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")


class S3:
    def delete_object(self, **_):
        return {}

    def head_object(self, **_):
        raise RuntimeError("404")


def install(pf: Platform) -> None:
    F.api, F.api_list, F.services_in, F.cli = pf.api, pf.api_list, pf.services_in, pf.cli
    F.artifact_s3 = lambda: (S3(), "art")


def live_world(run: str, cells: dict) -> Platform:
    projects, buckets, services = {}, {}, {}
    for c, (p, b, svcs) in cells.items():
        base = f"k2c-{run}-{c}"
        projects[p] = {"id": p, "name": base}
        buckets[b] = {"id": b, "name": base, "project": {"id": p}}
        for k, v in svcs.items():
            services[v] = {"id": v, "name": f"{base}-{k}", "project": p}
    return Platform(projects, buckets, services)


def run_main(argv: list) -> tuple:
    out, code = io.StringIO(), 0
    sys.argv = ["teardown.py"] + argv
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        try:
            T.main()
        except SystemExit as e:
            code = e.code or 0
    return code, out.getvalue()


def test_isolation() -> None:
    run = "zz1"
    world = {"a": ("proj_A", "bkt_A", {"s1": "cps_A1"}), "b": ("proj_B", "bkt_B", {"s1": "cps_B1"}),
             "c": ("proj_C", "bkt_C", {"g1": "cps_C1"})}
    setup_home({run: {"run_id": run, "cells": {c: cell(p, b, run, c, s, ka=["s1"]) for c, (p, b, s) in world.items()}}})
    pf = live_world(run, world)
    pf.fail = {("DELETE", "/buckets/bkt_A"): 400}
    install(pf)
    code, out = run_main([run, "--yes"])
    destroyed = [c[1][2] for c in pf.calls if c[0] == "cli"]
    rep = F.read_json(os.path.join(HOME, "results", run, "teardown.json")) or {}
    check("isolation: every service destroyed despite cell a's bucket failure",
          sorted(destroyed) == ["cps_A1", "cps_B1", "cps_C1"], str(destroyed))
    check("isolation: services before any bucket", pf.calls.index(("cli", ("services", "destroy", "cps_C1", "--timeout", "300")))
          < pf.calls.index(("api", "DELETE", "/buckets/bkt_A")), "order")
    check("isolation: cells b and c fully deleted", "proj_B" not in pf.projects and "proj_C" not in pf.projects
          and "bkt_B" not in pf.buckets, str(pf.projects.keys()))
    check("isolation: teardown.json written with the failure, exit 1",
          code == 1 and rep.get("failures") and rep.get("verified") is False, f"code {code} {rep.get('failures')}")


def test_guards() -> None:
    run, other = "zz2", "zz9"
    cells = {"a": cell("proj_A", ART_B, run, "a", {}),             # the artifact bucket's id
             "b": cell("proj_X", "bkt_B", run, "b", {}),           # a project another run lists
             "c": cell("proj_C", "bkt_C", run, "c", {}),           # live bucket of another project
             "d": cell("proj_D", "bkt_D", run, "d", {}),           # live project renamed
             "e": cell("proj_E", "bkt_E", run, "e", {"s1": "cps_E1"})}  # services cannot be listed
    setup_home({run: {"run_id": run, "cells": cells},
                other: {"run_id": other, "cells": {"x": cell("proj_X", "bkt_X", other, "x", {})}}})
    pf = live_world(run, {"a": ("proj_A", ART_B, {}), "b": ("proj_X", "bkt_B", {}), "c": ("proj_C", "bkt_C", {}),
                          "d": ("proj_D", "bkt_D", {}), "e": ("proj_E", "bkt_E", {"s1": "cps_E1"})})
    pf.buckets["bkt_C"]["project"] = {"id": "proj_OTHER"}
    pf.projects["proj_D"]["name"] = "streams-artifacts-sin"
    pf.fail = {"list": ("proj_E",)}
    install(pf)
    code, out = run_main([run, "--yes"])
    deletes = [c[2] for c in pf.calls if c[0] == "api" and c[1] == "DELETE"]
    check("guard: artifact bucket id never deleted", f"/buckets/{ART_B}" not in deletes, str(deletes))
    check("guard: another run's project never deleted", "/projects/proj_X" not in deletes, str(deletes))
    check("guard: bucket whose live project differs never deleted", "/buckets/bkt_C" not in deletes, str(deletes))
    check("guard: project whose live name differs never deleted", "/projects/proj_D" not in deletes, str(deletes))
    check("guard: unlistable project keeps its bucket and project",
          "/buckets/bkt_E" not in deletes and "/projects/proj_E" not in deletes, str(deletes))
    check("guard: refusals make the teardown incomplete", code == 1 and "REFUSING" in out, out[-300:])


def test_pending_and_show() -> None:
    run = "zz3"
    c = cell("proj_P", "bkt_P", run, "p", {})
    c["project"] = {"name": f"k2c-{run}-p", "status": "pending"}
    c["bucket"] = {"name": f"k2c-{run}-p", "status": "pending"}
    c["keep_awake"] = [{"service": "k2c-zz3-p-s1", "expires": "2000-01-01T00:00:00Z"}]
    setup_home({run: {"run_id": run, "cells": {"p": c}}})
    pf = live_world(run, {"p": ("proj_P", "bkt_P", {})})
    install(pf)
    code, out = run_main(["--yes", run])
    check("show(): `--yes <run>` prints KEEP_AWAKE lines", "KEEP_AWAKE p/k2c-zz3-p-s1" in out, out[-400:])
    check("show(): no runs/--yes directory", not os.path.exists(os.path.join(HOME, "runs", "--yes")), "created")
    check("pending: project and bucket resolved by name and deleted",
          "proj_P" not in pf.projects and "bkt_P" not in pf.buckets and code == 0, f"{code} {out[-300:]}")


def test_partial() -> None:
    run = "zz4"
    setup_home({run: {"run_id": run, "cells": {"a": cell("proj_Q", "bkt_Q", run, "a", {"s1": "cps_Q1", "g1": "cps_G1"}, ka=["g1"])}}})
    pf = live_world(run, {"a": ("proj_Q", "bkt_Q", {"s1": "cps_Q1", "g1": "cps_G1"})})
    install(pf)
    code, out = run_main([run, "--expired", "--yes"])
    check("--expired destroys only the expired KEEP_AWAKE service",
          "cps_G1" not in pf.services and "cps_Q1" in pf.services and "proj_Q" in pf.projects, out[-300:])
    code, out = run_main([run, "--service", "k2c-zz4-a-s1", "--yes"])
    led = F.load_resources(run)["cells"]["a"]["services"]
    check("--service destroys one listed service and marks it deleted",
          "cps_Q1" not in pf.services and led["k2c-zz4-a-s1"]["status"] == "deleted" and code == 0, out[-300:])
    setup_home({"zz5": {"run_id": "zz5", "cells": {"a": cell("proj_Q", "bkt_Z", "zz5", "a", {"s1": "cps_Q9"})}}})
    code, out = run_main(["zz5", "--service", "k2c-zz5-a-s1", "--yes"])
    check("--service refuses a project another run's ledger lists", code == 1 and "REFUSED" in out, out[-300:])


def test_api_no_post_retry() -> None:
    import importlib
    lib = importlib.reload(F)
    with open(os.path.join(HOME, "platform-token.txt"), "w") as f:
        f.write("x")
    tries = {"n": 0}

    def opener(req, timeout=0):
        tries["n"] += 1
        raise socket.timeout("read timed out")
    lib.urllib.request.urlopen = opener
    lib.time.sleep = lambda s: None
    try:
        lib.api("POST", "/projects", {"name": "x"})
        raised = None
    except lib.OutcomeUnknown as e:
        raised = e
    check("api: a POST is tried once and a lost answer raises OutcomeUnknown", tries["n"] == 1 and raised is not None,
          f"tries {tries['n']}, {raised!r}")
    tries["n"] = 0

    def opener2(req, timeout=0):
        tries["n"] += 1
        raise urllib.error.URLError("refused")
    lib.urllib.request.urlopen = opener2
    try:
        lib.api("GET", "/projects")
    except RuntimeError:
        pass
    check("api: a GET retries transport errors 3 times", tries["n"] == 4, f"tries {tries['n']}")


def test_cli_lock() -> None:
    code = ("import os,sys,time; os.environ['K2_FIELD_HOME']=sys.argv[1]; sys.path.insert(0, sys.argv[2]); "
            "import fieldlib as F\nwith F.cli_lock():\n    print('held', flush=True); time.sleep(2)")
    here = os.path.dirname(os.path.abspath(__file__))
    p = subprocess.Popen([sys.executable, "-c", code, HOME, here], stdout=subprocess.PIPE, text=True)
    p.stdout.readline()
    t0 = time.time()
    with contextlib.redirect_stderr(io.StringIO()):
        with F.cli_lock():
            waited = time.time() - t0
    p.wait()
    check("cli lock: a second process blocks until the first releases", waited >= 1.5, f"waited {waited:.2f}s")


def test_ewma() -> None:
    import observe as O
    inst = O.Instance("streams-1", 1, None, False)
    inst.prev = {"boot": "b", "seq": 10, "ts": 0, "e": 1.0, "wall": 0.0}
    for n, e in enumerate((16.6, 13.96, 9.18), 1):
        inst.beat({"boot_id": "b", "seq": 10 + n, "ts_ms": 2000 * n, "cpu_pct": e}, 2.0 * n)
    check("EWMA inversion: readings 40/10/2 % over three 2 s beats -> 1.04 CPU-s",
          abs(inst.cpu_s - 1.04) < 0.01 and inst.beats_exact == 3, f"{inst.cpu_s:.3f}")
    inst.beat({"boot_id": "b", "seq": 14, "ts_ms": 2000 * 3 + 60_000, "cpu_pct": 5.0}, 70.0)
    check("wake: a beat after a 60 s gap counts one period awake and logs the wake",
          abs(inst.awake_s - 8.0) < 0.01 and inst.sleeps == 1 and inst.wakes, f"{inst.awake_s} {inst.sleeps}")
    first = O.Instance("streams-2", 1, int(time.time() * 1000) - 30_000, False)
    first.beat({"boot_id": "c", "seq": 45, "ts_ms": 1, "cpu_pct": 1.0}, time.time())
    check("first read credits min(seq x period, now - deploy start)", 29 <= first.awake_s <= 31, f"{first.awake_s}")


def test_scrape_gating() -> None:
    """observe.py never wakes a server, except one baseline scrape per server
    at start in a scraped cell without KEEP_AWAKE."""
    import observe as O
    run, c = "zz6", "w"
    setup_home({run: {"run_id": run, "cells": {c: {"services": {"k2c-zz6-w-s1": {"name": "k2c-zz6-w-s1", "instance": "streams-1",
                                                                              "status": "deployed"}}}}}})
    with open(os.path.join(HOME, "cells", "w.env"), "w") as f:
        f.write("SERVERS=1\nROUTERS=0\nSCRAPE=1\n")
    with open(os.path.join(HOME, "cells", "k.env"), "w") as f:
        f.write("SERVERS=1\nROUTERS=0\nSCRAPE=1\nKEEP_AWAKE=1\n")
    F.data_s3 = lambda r, cl: (None, "b")
    F.read_secret = lambda r, cl, n: "x"
    F.write_json(os.path.join(F.cell_dir(run, c), "cell.json"), {"server_urls": {"streams-1": "https://s1"}})
    got = []
    F.http_get = lambda url, headers=None, timeout=10: (got.append(url) or (200, b'{"git_commit": "g", "binary_sha256": "h"}', {}))
    args = types.SimpleNamespace(scrape_cell=[], cpulog_secs=1800, scrape_secs=20)
    cl = O.Cell(run, c, args)
    cl.expect = {}
    cl.scrape_servers()
    first = len(got)
    cl.scrape_servers()  # asleep (never seen live): not scraped again
    second = len(got) - first
    cl.inst["streams-1"].live = True
    cl.scrape_servers()
    third = len(got) - first - second
    check("scrape gating: one baseline scrape of a sleeping server, none after, scraped when live",
          first == 3 and second == 0 and third == 3, f"{first} {second} {third}")
    F.write_json(os.path.join(F.cell_dir(run, "k"), "cell.json"), {"server_urls": {"streams-1": "https://s1"}})
    ck = O.Cell(run, "k", args)
    ck.expect = {}
    got.clear()
    ck.scrape_servers()  # KEEP_AWAKE cell: no ledger expiry here, server not live -> never woken
    check("scrape gating: a KEEP_AWAKE cell's sleeping server (a spare) gets no baseline wake", not got, str(got))


def test_beats_not_blocked() -> None:
    """A slow scrape (3 x 1 s GETs) on the main thread misses no beat: the
    heartbeat reads run on their own thread."""
    import datetime
    import threading
    import observe as O
    run, c = "zz7", "w"
    setup_home({run: {"run_id": run, "cells": {c: {"services": {}}}}})
    with open(os.path.join(HOME, "cells", "w.env"), "w") as f:
        f.write("SERVERS=1\nROUTERS=0\nSCRAPE=1\n")
    t0 = time.time()

    class FakeS3:
        def get_object(self, Bucket, Key):
            seq = int((time.time() - t0) / 2) + 1
            ts = t0 + seq * 2 - 2
            body = json.dumps({"boot_id": "b", "seq": seq, "ts_ms": int(ts * 1000), "cpu_pct": 1.0}).encode()
            now = datetime.datetime.now(datetime.timezone.utc)
            return {"Body": io.BytesIO(body), "LastModified": now, "ContentLength": len(body),
                    "ResponseMetadata": {"HTTPHeaders": {"date": now.strftime("%a, %d %b %Y %H:%M:%S GMT")}}}
    F.data_s3 = lambda r, cl: (FakeS3(), "b")
    F.read_secret = lambda r, cl, n: "x"
    F.write_json(os.path.join(F.cell_dir(run, c), "cell.json"), {"server_urls": {"streams-1": "https://s1"}})
    F.http_get = lambda url, headers=None, timeout=10: (time.sleep(1.0) or (200, b'{}', {}))
    cl = O.Cell(run, c, types.SimpleNamespace(scrape_cell=[], cpulog_secs=1800, scrape_secs=20, hb_secs=2))
    stop = {"now": False}
    th = threading.Thread(target=cl.beat_loop, args=(stop,), daemon=True)
    th.start()
    end = time.time() + 11
    while time.time() < end:
        cl.inst["streams-1"].live = True
        cl.scrape_servers()  # ~3 s of blocking GETs each
    stop["now"] = True
    th.join(5)
    i = cl.inst["streams-1"]
    check("heartbeat thread: no beat interpolated while slow scrapes block the main loop",
          i.beats_interp == 0 and i.beats_exact >= 4, f"exact {i.beats_exact} interp {i.beats_interp}")


def test_spread() -> None:
    import gen as G
    stages = [[["idle", "300"]], [["setup", "--streams", "2"]], [["produce", "--rate", "5"], ["subs", "--count", "5", "--connect-rate", "3"]]]
    out, assigned = G.spread(stages, [("router-1", "https://r1"), ("router-2", "https://r2")])
    subs = [a for a in out[2] if a[0] == "subs"]
    counts = sorted(int(G.flag(a, "--count", "0")) for a in subs)
    check("spread: invocations alternate over routers and subs is split",
          out[1][0][out[1][0].index("--base") + 1] == "https://r1" and out[2][0][out[2][0].index("--base") + 1] == "https://r2"
          and counts == [2, 3] and [a["inv"] for a in assigned] == list(range(5)), f"{counts} {assigned}")


# ---------------------------------------------------------------- campaign mode

CAMP_P, CAMP_B, WS = "proj_camp", "bkt_campart", "wksp_pro"
OTHER = {"proj_ppg": "ppg-bench", "proj_demo": "streams-demo", "proj_cool": "cool-app"}


class CPlatform:
    """A workspace that holds other things: the campaign project beside
    unrelated projects, buckets and a service. Every call passes the real
    gates first; every listing ignores its projectId filter (the callers
    must filter); the compute listing returns every service."""

    def __init__(self, campaign_exists: bool = True):
        self.projects = {p: {"id": p, "name": n, "workspace": {"id": WS}} for p, n in OTHER.items()}
        self.buckets = {"bkt_abba": {"id": "bkt_abba", "name": "abba", "project": {"id": "proj_ppg"}},
                        "bkt_pink": {"id": "bkt_pink", "name": "pink f", "project": {"id": "proj_demo"}}}
        self.services = {"cps_demo": {"id": "cps_demo", "name": "streams-demo-web", "projectId": "proj_demo"}}
        self.deployments: dict = {}
        self.envs: list = []       # env listing rows (projectId filter ignored)
        self.key_leak: list = []   # rows only a `&key=` lookup returns (its projectId filter broken)
        self.inherit: set = set()  # names every new version's snapshot holds beyond its env file
        self.snaps: dict = {}
        self.calls, self.bodies, self.lose, self.n, self.on_post = [], [], set(), 0, None
        self.hidden: set = set()   # ids whose GET answers 404 although they exist (an API/CLI disagreement)
        self.stuck: set = set()    # services whose destroy fails
        if campaign_exists:
            self.projects[CAMP_P] = {"id": CAMP_P, "name": "k2c-cost", "workspace": {"id": WS},
                                     "defaultRegion": "eu-central-1"}
            self.buckets[CAMP_B] = {"id": CAMP_B, "name": "k2c-cost-artifacts", "project": {"id": CAMP_P}}
        self.protected = set(self.projects) | set(self.buckets) | set(self.services)

    def table(self, kind: str) -> dict:
        return {"projects": self.projects, "buckets": self.buckets, "services": self.services,
                "deployments": self.deployments, "databases": {}}[kind]

    def api(self, method, path, body=None, allow=(), timeout=60.0):
        F.gate_api(method, path, body)
        self.calls.append(("api", method, path))
        parts = path.split("?")[0].strip("/").split("/")
        if method == "GET" and len(parts) == 2:
            t = self.table(parts[0])
            if parts[1] in t and parts[1] not in self.hidden:
                return 200, {"data": t[parts[1]]}
            if 404 in allow:
                return 404, None
            raise RuntimeError("404")
        if method == "DELETE" and len(parts) == 2:
            self.table(parts[0]).pop(parts[1], None)
            return 204, None
        if method != "POST":
            raise RuntimeError(f"unexpected {method} {path}")
        self.n += 1
        self.bodies.append((path, dict(body or {})))
        if self.on_post:
            self.on_post(path)
        if parts == ["projects"]:
            out = {"id": f"proj_new{self.n}", "name": body["name"], "workspace": {"id": WS}, "defaultRegion": body["region"]}
            self.projects[out["id"]] = out
        elif parts == ["buckets"]:
            out = {"id": f"bkt_new{self.n}", "name": body["name"], "project": {"id": body["projectId"]}}
            self.buckets[out["id"]] = out
        else:
            out = {"id": f"bkey_{self.n}", "bucketName": "t-" + self.buckets[parts[1]]["name"],
                   "endpoint": "https://t3.example", "accessKeyId": "AKID", "secretAccessKey": "SECRET"}
        if ("POST", path) in self.lose:
            self.lose.discard(("POST", path))
            raise F.OutcomeUnknown(f"POST {path}: answer lost")
        return 201, {"data": out}

    def api_list(self, path):
        F.gate_api("GET", path)
        self.calls.append(("list", path))
        kind, _, query = path.partition("?")
        kind = kind.strip("/")
        if kind != "environment-variables":
            return list(self.table(kind).values())
        key = urllib.parse.parse_qs(query).get("key")
        return [r for r in self.envs + self.key_leak if r.get("key") == key[0]] if key else list(self.envs)

    def cli_json(self, args, timeout=600):
        F.gate_cli(args)
        self.calls.append(("cli", tuple(args)))
        if args[:2] == ["services", "list"]:
            return {"ok": True, "data": list(self.services.values())}
        if args[:2] == ["versions", "show"]:
            return {"ok": True, "data": {"envVars": dict.fromkeys(self.snaps[args[2]], "v")}}
        if args[:1] == ["deploy"]:
            sid = args[args.index("--service") + 1] if "--service" in args else "cps_dep"
            with open(args[args.index("--env") + 1], encoding="utf-8") as f:
                self.snaps["dep_new"] = {l.split("=", 1)[0] for l in f if "=" in l} | self.inherit
            return {"ok": True, "data": {"appId": sid, "deploymentId": "dep_new", "deploymentEndpointDomain": "d.x",
                                         "appEndpointDomain": "a.x"}}
        raise RuntimeError(f"unexpected compute {args}")

    def cli(self, args, timeout=600, cwd=None):
        F.gate_cli(args)
        self.calls.append(("cli", tuple(args)))
        if args[:2] == ["services", "destroy"] and args[2] in self.stuck:
            return types.SimpleNamespace(returncode=1, stdout="", stderr="destroy failed")
        if args[:2] == ["services", "destroy"]:
            self.services.pop(args[2], None)
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    def mutated(self) -> set:
        """Every id a mutating call addressed."""
        out = set()
        for c in self.calls:
            if c[0] == "api" and c[1] in ("DELETE", "POST", "PATCH", "PUT"):
                out.update(c[2].split("?")[0].strip("/").split("/")[1:2])
            if c[0] == "cli" and c[1][:2] in (("services", "destroy"), ("versions", "stop")):
                out.add(c[1][2])
            if c[0] == "cli" and c[1][:1] == ("deploy",):
                out.update(c[1][i + 1] for i, a in enumerate(c[1][:-1]) if a in ("--project", "--service"))
        return out

    def scopes(self) -> set:
        """What every read addressed: listings and project GETs."""
        out = set()
        for c in self.calls:
            if c[0] == "list":
                out.add(c[1].split("&")[0])
            if c[0] == "api" and c[1] == "GET" and c[2].startswith("/projects/"):
                out.add(c[2])
            if c[0] == "cli" and c[1][:2] == ("services", "list"):
                out.add("compute services list " + " ".join(c[1][2:]))
        return out


def campaign_record(status: str = "ready") -> dict:
    return {"name": "k2c-cost", "region": "eu-central-1", "status": status,
            "project": {"id": CAMP_P, "name": "k2c-cost", "status": "created", "workspace": {"id": WS}},
            "artifact_bucket": {"id": CAMP_B, "name": "k2c-cost-artifacts", "status": "created"}}


def install_c(pf: CPlatform) -> None:
    F.api, F.api_list, F.cli, F.cli_json, F.services_in = pf.api, pf.api_list, pf.cli, pf.cli_json, REAL_SERVICES_IN
    F.artifact_s3 = lambda: (S3(), "art")


def setup_campaign(ledgers: dict, live: dict | None = None) -> CPlatform:
    """ledgers {run: {cell: (bucket id, {suffix: service id})}}; live
    {(run, cell): project} places a cell's objects in another project."""
    setup_home({run: {"run_id": run, "campaign": {"name": "k2c-cost", "project_id": CAMP_P}, "cells": {
        c: {"bucket": {"id": b, "name": f"k2c-{run}-{c}", "project": CAMP_P},
            "services": {f"k2c-{run}-{c}-{k}": {"id": v, "name": f"k2c-{run}-{c}-{k}", "status": "deployed"}
                         for k, v in svcs.items()},
            "artifact_objects": [f"k2c/{run}/{c}/feeds.json"], "keep_awake": []}
        for c, (b, svcs) in cells.items()}} for run, cells in ledgers.items()})
    F.write_json(os.path.join(HOME, "campaign.json"), campaign_record())
    F.write_json(os.path.join(HOME, "artifact-platform-receipt.json"),
                 {"projectId": CAMP_P, "bucketId": CAMP_B, "bucketName": "t-k2c-cost-artifacts", "campaign": "k2c-cost"})
    pf = CPlatform()
    for run, cells in ledgers.items():
        for c, (b, svcs) in cells.items():
            proj = (live or {}).get((run, c), CAMP_P)
            pf.buckets[b] = {"id": b, "name": f"k2c-{run}-{c}", "project": {"id": proj}}
            for k, v in svcs.items():
                pf.services[v] = {"id": v, "name": f"k2c-{run}-{c}-{k}", "projectId": proj}
    install_c(pf)
    return pf


def refused(fn, *args) -> bool:
    """True when fn refused: a ScopeError or a die(); its output is dropped."""
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            fn(*args)
    except (F.ScopeError, SystemExit):
        return True
    return False


def test_campaign_gate() -> None:
    pf = setup_campaign({})
    bad_api = [("POST", "/buckets", {"projectId": "proj_ppg", "name": "k2c-x"}), ("POST", "/buckets", {"projectId": CAMP_P, "name": "abba"}),
               ("DELETE", "/buckets/bkt_abba"), ("POST", "/buckets/bkt_abba/keys", {"role": "read"}),
               ("DELETE", "/projects/proj_ppg"), ("DELETE", f"/projects/{CAMP_P}"),
               ("POST", "/projects", {"name": "k2c-cost", "region": "eu-central-1", "createDatabase": False}),
               ("GET", "/projects/proj_ppg"), ("GET", "/services"), ("GET", "/buckets"), ("GET", "/databases"),
               ("GET", "/environment-variables?projectId=proj_ppg"), ("GET", "/services?projectId=proj_demo"),
               ("PATCH", f"/projects/{CAMP_P}", {}), ("DELETE", "/services/cps_demo"), ("POST", "/environment-variables", {}),
               ("DELETE", f"/buckets/{CAMP_B}")]
    bad_cli = [["deploy", "--project", "proj_demo", "--service-name", "k2c-x-s1"], ["deploy", "--project", CAMP_P, "--service-name", "web"],
               ["deploy", "--project", CAMP_P, "--service", "cps_demo", "--destroy-old-version"],
               ["services", "destroy", "cps_demo"], ["services", "list", "--project", "proj_demo"], ["services", "list"],
               ["versions", "stop", "dep_x"], ["projects", "delete", CAMP_P], ["env", "unset", "X"]]
    let_api = [b for b in bad_api if not refused(F.gate_api, *b)]
    let_cli = [b for b in bad_cli if not refused(F.gate_cli, b)]
    check("campaign gate: every out-of-scope API call is refused before it is sent", not let_api, str(let_api))
    check("campaign gate: every out-of-scope compute call is refused before it runs", not let_cli, str(let_cli))
    ok = [("GET", f"/projects/{CAMP_P}"), ("GET", "/projects"), ("GET", f"/buckets?projectId={CAMP_P}"),
          ("GET", f"/environment-variables?projectId={CAMP_P}&class=production&limit=100"), ("GET", "/services/cps_demo"),
          ("POST", "/buckets", {"projectId": CAMP_P, "name": "k2c-r1-a"})]
    check("campaign gate: in-scope reads and a k2c- bucket in the campaign project pass",
          not [b for b in ok if refused(F.gate_api, *b)] and not refused(F.gate_cli, ["services", "list", "--project", CAMP_P])
          and not refused(F.gate_cli, ["logs", "dep_x", "--tail", "5"]), str(ok))
    st_abba, _ = F.campaign_owns("bucket", "bkt_abba")
    st_demo, _ = F.campaign_owns("service", "cps_demo")
    pf.buckets["bkt_k"] = {"id": "bkt_k", "name": "k2c-r1-a", "project": {"id": CAMP_P}}
    st_k, _ = F.campaign_owns("bucket", "bkt_k")
    st_art, _ = F.campaign_owns("bucket", CAMP_B)
    with F.permit("artifact-bucket-delete"):
        art_ok = not refused(F.gate_api, "DELETE", f"/buckets/{CAMP_B}")
    with F.permit("campaign-project-delete"):
        proj = refused(F.gate_api, "DELETE", "/projects/proj_ppg") and not refused(F.gate_api, "DELETE", f"/projects/{CAMP_P}")
    check("campaign gate: only an id proven live in the campaign project unlocks a mutation; the artifact bucket and "
          "the project need destroy's permit",
          st_abba == st_demo == "refuse" and refused(F.gate_api, "DELETE", "/buckets/bkt_abba") and st_k == st_art == "live"
          and not refused(F.gate_api, "DELETE", "/buckets/bkt_k") and refused(F.gate_api, "DELETE", f"/buckets/{CAMP_B}")
          and art_ok and proj, f"{st_abba} {st_demo} {st_k} {st_art} {art_ok} {proj}")
    check("campaign gate: teardown's own delete calls refuse an unverified id",
          refused(T.delete_platform, "zz", "a", "bucket", "bkt_abba") and refused(T.destroy_service, "zz", "a",
                                                                                 {"id": "cps_demo", "name": "x"})
          and "bkt_abba" in pf.buckets and "cps_demo" in pf.services, str(pf.calls[-3:]))
    wrong = [name for name, ok in (("cost", False), ("cos", True), ("k2c", True), ("r1", True))
             if refused(F.check_names, name) == ok]
    check("campaign: a run id whose k2c-<run>- prefix covers the campaign's names (cost) is refused", not wrong, str(wrong))


def test_campaign_teardown() -> None:
    led = {"r1": {"a": ("bkt_r1a", {"s1": "cps_r1a1", "r1": "cps_r1ar1"}), "b": ("bkt_r1b", {"g1": "cps_r1bg1"})},
           "r2": {"a": ("bkt_r2a", {"s1": "cps_r2a1"})}}
    pf = setup_campaign(led)
    code, out = run_main(["r1", "a", "--yes"])
    check("campaign teardown of one cell (F1's f1a f1c while f1b goes on): VERIFIED, the other cell untouched",
          code == 0 and {"bkt_r1b", "cps_r1bg1"} <= set(pf.buckets) | set(pf.services)
          and not {"bkt_r1a", "cps_r1a1", "cps_r1ar1"} & (set(pf.buckets) | set(pf.services)), f"{code} {out[-400:]}")
    pf.buckets["bkt_r1a_x"] = {"id": "bkt_r1a_x", "name": "k2c-r1-a", "project": {"id": CAMP_P}}
    code, out = run_main(["r1", "a", "--yes"])
    del pf.buckets["bkt_r1a_x"]
    check("campaign teardown: a leftover of the cell (an unledgered k2c-r1-a bucket) fails verification, untouched",
          code == 1 and "bkt_r1a_x" in out and "bkt_r1a_x" not in pf.mutated(), f"{code} {out[-300:]}")
    code, out = run_main(["r1", "--yes"])
    rep = F.read_json(os.path.join(HOME, "results", "r1", "teardown.json")) or {}
    mine = {"bkt_r1a", "bkt_r1b", "cps_r1a1", "cps_r1ar1", "cps_r1bg1"}
    check("campaign teardown: the run's services and buckets go, verified, exit 0",
          code == 0 and not mine & (set(pf.buckets) | set(pf.services)) and rep.get("verified") is True
          and rep.get("services_left") == [] and rep.get("buckets_left") == [], f"{code} {out[-500:]}")
    check("campaign teardown: nothing else is touched (other run, artifact bucket, campaign project, other projects)",
          pf.mutated() == mine and pf.protected <= set(pf.projects) | set(pf.buckets) | set(pf.services)
          and {"bkt_r2a", "cps_r2a1"} <= set(pf.buckets) | set(pf.services), str(pf.mutated() - mine))
    check("campaign teardown: it reads only the campaign project (scoped listings, no workspace listing)",
          pf.scopes() == {f"/projects/{CAMP_P}", f"/buckets?projectId={CAMP_P}", f"compute services list --project {CAMP_P}"},
          str(pf.scopes()))
    led1 = F.load_resources("r1")["cells"]
    check("campaign teardown: the ledger marks every service and bucket deleted",
          all(c["bucket"]["status"] == "deleted" and all(s["status"] == "deleted" for s in c["services"].values())
              for c in led1.values()), json.dumps(led1)[:300])


def test_campaign_teardown_guards() -> None:
    led = {"r3": {"a": ("bkt_r3a", {"s1": "cps_alien"}), "b": ("bkt_b3", {}), "d": (CAMP_B, {}),
                  "e": ("bkt_r9", {"s1": "cps_r9"}), "f": ("bkt_r3f", {"s1": "cps_f1"}), "g": ("bkt_r3g", {"s1": "cps_g1"}),
                  "h": ("bkt_r3h", {"s1": "cps_h1"})},
           "r9": {"x": ("bkt_r9", {"s1": "cps_r9"})}}
    pf = setup_campaign(led, live={("r3", "a"): "proj_demo", ("r3", "b"): "proj_ppg"})
    pf.hidden.add("cps_h1")                                       # cell h: listed in the project, GET says 404
    pf.buckets["bkt_r3a"]["project"] = {"id": CAMP_P}             # cell a: only its service lives elsewhere
    pf.services["cps_f1"]["name"] = "k2c-r3-f-s9"                 # cell f: live name differs (and is unlisted)
    pf.services["cps_g2"] = {"id": "cps_g2", "name": "k2c-r3-g-s2", "projectId": CAMP_P}  # cell g: unlisted
    pf.buckets[CAMP_B]["name"] = "k2c-r3-d"                       # the artifact bucket, even named like the run's
    with F.resources("r3") as d:                                  # cell c: ledger names that are not the run's
        d["cells"]["c"] = {"bucket": {"id": "bkt_pink", "name": "pink f"}, "artifact_objects": [], "keep_awake": [],
                           "services": {"streams-demo-web": {"id": "cps_demo", "name": "streams-demo-web", "status": "deployed"}}}
    code, out = run_main(["r3", "--yes"])
    hit = pf.mutated() & {"cps_alien", "bkt_b3", "cps_demo", "bkt_pink", CAMP_B, "cps_r9", "bkt_r9", "cps_f1", "cps_g2",
                          "bkt_r3a", "bkt_r3g", "cps_h1", "bkt_r3h", CAMP_P}
    check("campaign guards: a live project elsewhere, a foreign name, the artifact bucket, another run's ids, a live-name "
          "mismatch, an unlisted service, a listed service whose GET says 404: nothing of them touched, buckets of "
          "those cells kept", not hit and F.load_resources("r3")["cells"]["h"]["services"]["k2c-r3-h-s1"]["status"]
          == "deployed", str(hit))
    check("campaign guards: the clean service still goes; refusals make it INCOMPLETE",
          "cps_g1" not in pf.services and code == 1 and out.count("REFUSING") >= 11, f"{code} {out[-600:]}")
    pf.projects[CAMP_P]["name"] = "k2c-cost-renamed"
    pf.calls.clear()
    code, out = run_main(["r3", "--yes"])
    code2, _ = run_main(["r3", "--expired", "--yes"])
    check("campaign guards: a renamed campaign project stops teardown (and --expired) after one GET",
          code != 0 and code2 != 0 and pf.calls == [("api", "GET", f"/projects/{CAMP_P}")] * 2, f"{code} {pf.calls}")


def test_campaign_partial() -> None:
    pf = setup_campaign({"r4": {"a": ("bkt_r4a", {"s1": "cps_r4a1", "g1": "cps_r4g1"}), "b": ("bkt_r4b", {"s1": "cps_r4b1"})}},
                        live={("r4", "b"): "proj_demo"})
    with F.resources("r4") as d:
        for c, s in (("a", "k2c-r4-a-s1"), ("b", "k2c-r4-b-s1")):
            d["cells"][c]["services"][s].update({"keep_awake": True, "keep_awake_expires": "2000-01-01T00:00:00Z"})
    code, out = run_main(["r4", "--expired", "--yes"])
    check("campaign --expired: the expired service in the campaign goes; one whose live project is elsewhere is REFUSED",
          "cps_r4a1" not in pf.services and "cps_r4b1" in pf.services and "cps_r4g1" in pf.services and code == 1
          and "REFUSED" in out, f"{code} {out[-300:]}")
    code, out = run_main(["r4", "--service", "k2c-r4-a-g1", "--yes"])
    check("campaign --service: one listed service, proven in the campaign, goes",
          "cps_r4g1" not in pf.services and code == 0 and pf.mutated() == {"cps_r4a1", "cps_r4g1"}, f"{code} {out[-300:]}")


def test_campaign_init() -> None:
    import campaign as C
    setup_home({})
    for f in ("artifact-platform-receipt.json",):
        os.remove(os.path.join(HOME, f))
    with open(os.path.join(HOME, "campaign-required"), "w") as f:
        f.write("k2c-cost\n")
    pf = CPlatform(campaign_exists=False)
    install_c(pf)
    check("campaign init: a name without k2c- and another campaign's name are refused, nothing sent",
          refused(C.init, "cost") and refused(C.init, "k2c-other") and not pf.calls, str(pf.calls))
    seen = []
    pf.on_post = lambda path: seen.append((path, (F.campaign_doc() or {}).get("project", {}).get("status")))
    pf.lose.add(("POST", "/projects"))
    first = refused(C.init, "k2c-cost")
    pending = (F.campaign_doc() or {}).get("project", {})
    with contextlib.redirect_stdout(io.StringIO()):
        code = C.init("k2c-cost")
    doc = F.campaign_doc() or {}
    posts = [p for p, _ in pf.bodies]
    pid = doc.get("project", {}).get("id")
    check("campaign init: pending written before the POST; a lost answer is resolved by name, never re-POSTed",
          first and seen[0] == ("/projects", "pending") and not pending.get("id") and code == 0
          and posts.count("/projects") == 1 and doc.get("project", {}).get("resolved_by_name") and pid == "proj_new1",
          f"{seen} {posts} {doc.get('project')}")
    rc = F.read_json(os.path.join(HOME, "artifact-platform-receipt.json")) or {}
    bid = doc.get("artifact_bucket", {}).get("id")
    files = all(os.path.exists(os.path.join(HOME, f)) for f in C.ARTIFACT_FILES)
    mode = oct(os.stat(os.path.join(HOME, "binsec.txt")).st_mode & 0o777) if files else None
    check("campaign init: k2c-cost-artifacts in the project, its key, the five artifact files, status ready",
          doc.get("status") == "ready" and pf.buckets.get(bid, {}).get("project", {}).get("id") == pid
          and pf.buckets[bid]["name"] == "k2c-cost-artifacts" and files and mode == "0o600"
          and rc.get("projectId") == pid and rc.get("bucketId") == bid and rc.get("campaign") == "k2c-cost"
          and ("/projects", {"name": "k2c-cost", "region": "eu-central-1", "createDatabase": False}) in pf.bodies
          and not pf.mutated() & pf.protected, f"{doc} {rc} {mode} {pf.bodies}")
    for f in ("campaign.json",) + C.ARTIFACT_FILES:
        os.remove(os.path.join(HOME, f))
    n = len(pf.bodies)
    taken = refused(C.init, "k2c-cost")
    with open(os.path.join(HOME, "artifact-endpoint.txt"), "w") as f:
        f.write("https://elsewhere")
    foreign = refused(C.init, "k2c-cost")
    check("campaign init: a project already named so (no pending entry) and another setup's artifact files are refused",
          taken and foreign and len(pf.bodies) == n and F.campaign_doc() is None, f"{taken} {foreign} {pf.bodies[n:]}")


def test_campaign_verify_destroy() -> None:
    import campaign as C
    pf = setup_campaign({"r5": {"a": ("bkt_r5a", {"s1": "cps_r5a1"})}})
    pf.envs = [{"key": "AUTH_TOKEN", "projectId": CAMP_P}, {"key": "KEEP_AWAKE", "projectId": CAMP_P}]
    with contextlib.redirect_stdout(io.StringIO()) as out:
        code = C.verify()
    pf.envs.append({"key": "DEMO_SECRET", "projectId": "proj_demo"})
    with contextlib.redirect_stdout(io.StringIO()) as out2:
        code2 = C.verify()
    pf.envs = []
    check("campaign verify: lists only the campaign project's services, buckets and env names (the leftover a run "
          "teardown keeps); a foreign env row fails it",
          code == 0 and pf.scopes() == {f"/projects/{CAMP_P}", f"/buckets?projectId={CAMP_P}",
                                        f"/environment-variables?projectId={CAMP_P}",
                                        f"compute services list --project {CAMP_P}"}
          and "abba" not in out.getvalue() and "k2c-r5-a-s1" in out.getvalue()
          and "['AUTH_TOKEN', 'KEEP_AWAKE']" in out.getvalue() and code2 == 1 and "REFUSING" in out2.getvalue(),
          f"{pf.scopes()} {code2} {out.getvalue()[-300:]}")
    pf.services["cps_web"] = {"id": "cps_web", "name": "web", "projectId": CAMP_P}
    with contextlib.redirect_stdout(io.StringIO()):
        alien, dry_code = C.destroy(True), None
        del pf.services["cps_web"]
        dry_code = C.destroy(False)
    check("campaign destroy: refused while the project holds a non-k2c- service; the dry run changes nothing",
          alien == 1 and dry_code == 0 and not pf.mutated() and CAMP_P in pf.projects, f"{alien} {dry_code} {pf.mutated()}")
    pf.projects[CAMP_P]["name"] = "k2c-cost-renamed"
    renamed = refused(C.destroy, True)
    pf.projects[CAMP_P]["name"] = "k2c-cost"
    pf.stuck.add("cps_r5a1")
    with contextlib.redirect_stdout(io.StringIO()):
        stuck = C.destroy(True)
    check("campaign destroy: while a service will not go, its buckets and the project are left standing",
          stuck == 1 and CAMP_P in pf.projects and {CAMP_B, "bkt_r5a"} <= set(pf.buckets)
          and not [c for c in pf.calls if c[:2] == ("api", "DELETE")]
          and (F.campaign_doc() or {}).get("status") == "destroying", f"{stuck} {pf.calls[-4:]}")
    pf.stuck.clear()
    with contextlib.redirect_stdout(io.StringIO()) as out:
        code = C.destroy(True)
    gone = {CAMP_P, CAMP_B, "bkt_r5a", "cps_r5a1"}
    led = F.load_resources("r5")["cells"]["a"]
    check("campaign destroy: a renamed project is refused; then services, buckets (artifact too) and the project go, "
          "404-verified, nothing else touched",
          renamed and code == 0 and pf.mutated() == gone and not gone & (set(pf.projects) | set(pf.buckets) | set(pf.services))
          and set(OTHER) <= set(pf.projects) and {"bkt_abba", "bkt_pink", "cps_demo"} <= set(pf.buckets) | set(pf.services)
          and (F.campaign_doc() or {}).get("status") == "destroyed" and led["bucket"]["status"] == "deleted"
          and refused(F.campaign), f"{renamed} {code} {pf.mutated()} {out.getvalue()[-400:]}")


def test_campaign_provision_deploy() -> None:
    import deploy as D
    import provision as P
    pf = setup_campaign({})
    with open(os.path.join(HOME, "cells", "a.env"), "w") as f:
        f.write("SERVERS=1\n")
    with contextlib.redirect_stdout(io.StringIO()):
        P.provision("r6", "a")
    doc = F.load_resources("r6")
    bid = doc["cells"]["a"]["bucket"]["id"]
    check("campaign provision: no project; the cell's bucket and key in the campaign project; the run is stamped",
          not [b for p, b in pf.bodies if p == "/projects"] and ("/buckets", {"projectId": CAMP_P, "name": "k2c-r6-a"}) in pf.bodies
          and pf.buckets[bid]["project"]["id"] == CAMP_P and "project" not in doc["cells"]["a"]
          and doc.get("campaign", {}).get("project_id") == CAMP_P
          and os.path.exists(os.path.join(HOME, "runs", "r6", "a", "secrets", "bkey.json")), f"{pf.bodies} {doc}")
    with F.resources("r7") as d:
        d["cells"]["a"] = {"project": {"id": "proj_ppg", "name": "k2c-r7-a"}, "services": {}}
    check("campaign provision: a run with per-cell projects is refused", refused(P.provision, "r7", "a"), "accepted")
    own = [{"key": "OLD_A", "projectId": CAMP_P}, {"key": "KEEP_AWAKE", "projectId": CAMP_P},
           {"key": "SYS", "projectId": CAMP_P, "isManagedBySystem": True}]
    depth = []
    real_list = pf.api_list
    pf.api_list = lambda path: (depth.append(F._CLI_DEPTH[0]) if "environment" in path else None) or real_list(path)
    install_c(pf)
    pf.services["cps_r6s1"] = {"id": "cps_r6s1", "name": "k2c-r6-a-s1", "projectId": CAMP_P}
    with F.resources("r6") as d:
        d["cells"]["a"]["services"] = {"k2c-r6-a-s1": {"id": "cps_r6s1", "name": "k2c-r6-a-s1", "status": "deployed"},
                                       "k2c-r6-a-s2": {"id": "cps_demo", "name": "k2c-r6-a-s2", "status": "deployed"}}

    def deploys() -> list:
        return [c[1] for c in pf.calls if c[0] == "cli" and c[1][:1] == ("deploy",)]
    s1 = ("r6", "a", CAMP_P, "k2c-r6-a-s1", "server", "app-server")
    foreign = []
    for rows, leak, env in (([{"key": "AUTH_TOKEN", "projectId": "proj_demo", "id": "ev_demo"}], [], {"A": "1"}),
                            ([{"key": "LOOSE", "id": "ev_none"}], [], {"A": "1"}),       # a row naming no project
                            ([], [{"key": "AUTH_TOKEN", "projectId": "proj_demo"}], {"AUTH_TOKEN": "x"}),  # keyed lookup only
                            ([], [{"key": "B", "projectId": CAMP_P}] * 2, {"B": "1"})):  # two rows for one key
        pf.envs, pf.key_leak, n = own + rows, leak, len(deploys())
        foreign.append(refused(D.deploy_service, *s1, env) and len(deploys()) == n)
    check("campaign deploy: a foreign or project-less row in the env listing, or a keyed lookup (the CLI's own) that "
          "returns another project's row or two rows, refuses before any compute deploy", all(foreign), str(foreign))
    pf.envs, pf.key_leak, depth[:], m = own, [], [], len(pf.calls)
    with contextlib.redirect_stdout(io.StringIO()):
        D.deploy_service(*s1, {"A": "1", "OLD_A": "2"})
    dep = deploys()[-1:]
    unset = [dep[0][i + 1] for i, a in enumerate(dep[0][:-1]) if a == "--unset-env"] if dep else None
    probed = sorted(c[1].split("key=")[1] for c in pf.calls[m:] if c[0] == "list" and "key=" in c[1])
    check("campaign deploy: --project and --service are the campaign's; --unset-env is exactly its own other variables, "
          "listed and every key probed under the CLI lock",
          len(dep) == 1 and dep[0][dep[0].index("--project") + 1] == CAMP_P and dep[0][dep[0].index("--service") + 1] == "cps_r6s1"
          and unset == ["KEEP_AWAKE"] and depth and all(depth) and probed == ["A", "KEEP_AWAKE", "OLD_A"],
          f"{dep} {unset} {depth} {probed}")
    pf.inherit = {"SYS"}
    with contextlib.redirect_stdout(io.StringIO()):
        D.deploy_service(*s1, {"A": "1"})  # a system-managed name in the snapshot passes
    pf.inherit, n = {"SYS", "KEEP_AWAKE", "KEEP_AWAKE_UNTIL_MS"}, len(deploys())
    stopped = refused(D.deploy_service, *s1, {"A": "1"})
    led = F.load_resources("r6")["cells"]["a"]["services"]["k2c-r6-a-s1"]
    pf.inherit = set()
    check("campaign deploy: a version whose snapshot holds an unstated KEEP_AWAKE stops the deploy (ledger records it)",
          stopped and len(deploys()) == n + 1 and led.get("platform_env_extra") == ["KEEP_AWAKE", "KEEP_AWAKE_UNTIL_MS"],
          f"{stopped} {led}")
    n = len(pf.calls)
    over = refused(D.deploy_service, "r6", "a", CAMP_P, "k2c-r6-a-s2", "server", "app-server", {"A": "1"})
    other = refused(D.deploy_service, "r6", "a", "proj_demo", "k2c-r6-a-s3", "server", "app-server", {"A": "1"})
    check("campaign deploy: a redeploy over a service elsewhere, or into another project, is refused before deploying",
          over and other and not [c for c in pf.calls[n:] if c[0] == "cli" and c[1][:1] == ("deploy",)], str(pf.calls[n:]))
    with F.resources("r6") as d:  # a ledger service destroyed earlier (404, not listed): a new one by name
        d["cells"]["a"]["services"]["k2c-r6-a-s4"] = {"id": "cps_old", "name": "k2c-r6-a-s4", "status": "deleted"}
    with contextlib.redirect_stdout(io.StringIO()):
        D.deploy_service("r6", "a", CAMP_P, "k2c-r6-a-s4", "server", "app-server", {"A": "1"})
    last = [c[1] for c in pf.calls if c[0] == "cli" and c[1][:1] == ("deploy",)][-1]
    check("campaign deploy: a gone ledger service is redeployed as a new k2c- service by name, never by its old id",
          "--service-name" in last and "--service" not in last and last[last.index("--service-name") + 1] == "k2c-r6-a-s4",
          str(last))
    F.write_json(os.path.join(HOME, "bins.json"), {"_latest": {"streams": "s", "pilot": "p", "k2gen": "g"}})
    with F.resources("r6") as d:
        d["cells"]["a"]["services"]["k2c-r6-a-s1"].update({"version": "dep_bad"})
    pf.deployments = {"dep_bad": {"id": "dep_bad", "serviceId": "cps_demo"}, "dep_ok": {"id": "dep_ok", "serviceId": "cps_r6s1"}}
    del pf.services["cps_demo"]  # the compute listing must not show it under the cell's name
    with contextlib.redirect_stdout(io.StringIO()):
        bad = refused(D.kill, "r6", "a", 1)
        with F.resources("r6") as d:
            d["cells"]["a"]["services"]["k2c-r6-a-s1"]["version"] = "dep_ok"
        good = not refused(D.kill, "r6", "a", 1)
    stops = [c[1][2] for c in pf.calls if c[0] == "cli" and c[1][:2] == ("versions", "stop")]
    check("campaign --kill: a version whose live service is not the ledger's is refused; the verified one is stopped",
          bad and good and stops == ["dep_ok"], f"{bad} {good} {stops}")


def test_home_and_cli_env() -> None:
    """No tool runs without K2_FIELD_HOME (no fallback into another
    workspace's home); a tool's first line names the home and campaign; the
    CLI never sees the shell's PRISMA_* variables, and campaign mode refuses
    while the shell exports one the CLI would act on."""
    here = os.path.dirname(os.path.abspath(__file__))
    bare = {k: v for k, v in os.environ.items() if k != "K2_FIELD_HOME"}
    bare["HOME"] = tempfile.mkdtemp(prefix="k2f-nohome-")  # a regressed default could reach no real home
    bare["PYTHONUSERBASE"] = __import__("site").getuserbase()  # boto3 may live in the user site
    ran = {t: subprocess.run([sys.executable, os.path.join(here, t), "zz1", "a"], env=bare, capture_output=True,
                             text=True) for t in ("provision.py", "deploy.py", "gen.py", "observe.py", "teardown.py",
                                                  "campaign.py", "bins.py", "price_field.py")}
    __import__("shutil").rmtree(bare["HOME"], ignore_errors=True)
    bad = {t: (p.returncode, p.stdout[:80], p.stderr[-120:]) for t, p in ran.items()
           if p.returncode != 2 or p.stdout or "K2_FIELD_HOME is not set" not in p.stderr}
    check("field home: every tool refuses to start without K2_FIELD_HOME (nothing run, no default home)", not bad, str(bad))
    setup_campaign({"r1": {"a": ("bkt_r1a", {})}})
    code, out = run_main(["r1"])
    check("field home: a tool's first output line names the field home and the campaign project",
          out.startswith(f"[teardown] field home {HOME}: campaign k2c-cost (project {CAMP_P}, ready)"), out[:200])
    with open(os.path.join(HOME, "platform-token.txt"), "w") as f:
        f.write("tok-field-home")
    stray = {"PRISMA_COMPUTE_SERVICE_ID": "cps_r9a1", "PRISMA_MANAGEMENT_API_URL": "https://elsewhere.example",
             "PRISMA_COMPUTE_AUTH_FILE": "/elsewhere/auth.json", "PRISMA_API_TOKEN": "tok-other-workspace"}
    saved = {k: os.environ.get(k) for k in stray}
    new = ["deploy", "--project", CAMP_P, "--service-name", "k2c-r1-a-s1"]
    os.environ.update(stray)
    try:
        cenv = F.cli_env()
        no = [refused(F.gate_cli, a) for a in (new, ["logs", "dep_x"], ["services", "list", "--project", CAMP_P])]
    finally:
        for k, v in saved.items():
            os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
    leaked = sorted(k for k in cenv if k.startswith("PRISMA_") and k != "PRISMA_API_TOKEN")
    check("cli env: no PRISMA_* of the shell reaches the CLI (token from the field home); campaign mode refuses every "
          "compute call while PRISMA_COMPUTE_SERVICE_ID or PRISMA_MANAGEMENT_API_URL is exported",
          not leaked and cenv.get("PRISMA_API_TOKEN") == "tok-field-home" and all(no) and not refused(F.gate_cli, new),
          f"{leaked} {no}")


def main() -> None:
    for t in (test_isolation, test_guards, test_pending_and_show, test_partial, test_api_no_post_retry,
              test_cli_lock, test_ewma, test_scrape_gating, test_beats_not_blocked, test_spread,
              test_campaign_gate, test_campaign_teardown, test_campaign_teardown_guards, test_campaign_partial,
              test_campaign_init, test_campaign_verify_destroy, test_campaign_provision_deploy, test_home_and_cli_env):
        try:
            t()
        except (Exception, SystemExit) as e:  # noqa: BLE001 - a crashed check (or a die()) is a failed check
            import traceback
            check(t.__name__, False, f"{type(e).__name__}: {e}\n{traceback.format_exc()[-800:]}")
    print(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed (field home {HOME})")
    if all(RESULTS):
        import shutil
        shutil.rmtree(HOME, ignore_errors=True)
    sys.exit(0 if all(RESULTS) else 1)


if __name__ == "__main__":
    main()
