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

HOME = tempfile.mkdtemp(prefix="k2f-selftest-")
os.environ["K2_FIELD_HOME"] = HOME
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fieldlib as F  # noqa: E402
import teardown as T  # noqa: E402

ART_P, ART_B = "proj_art", "bkt_art"
RESULTS = []


def check(name: str, ok, detail: str = "") -> None:
    ok = bool(ok)
    RESULTS.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name}{'' if ok else ': ' + detail}")


def setup_home(ledgers: dict) -> None:
    for d in ("runs", "results", "cells"):
        os.makedirs(os.path.join(HOME, d), exist_ok=True)
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
    with contextlib.redirect_stdout(out):
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


def main() -> None:
    for t in (test_isolation, test_guards, test_pending_and_show, test_partial, test_api_no_post_retry,
              test_cli_lock, test_ewma, test_scrape_gating, test_beats_not_blocked, test_spread):
        try:
            t()
        except Exception as e:  # noqa: BLE001 - a crashed check is a failed check
            import traceback
            check(t.__name__, False, f"{type(e).__name__}: {e}\n{traceback.format_exc()[-800:]}")
    print(f"\n{sum(RESULTS)}/{len(RESULTS)} checks passed (field home {HOME})")
    if all(RESULTS):
        import shutil
        shutil.rmtree(HOME, ignore_errors=True)
    sys.exit(0 if all(RESULTS) else 1)


if __name__ == "__main__":
    main()
