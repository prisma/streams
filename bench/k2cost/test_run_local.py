"""run-local.sh's offline half (`--dry-run`): the ports a point takes from
K2_PORT_BASE, the binaries it runs (K2_SERVER_BIN, K2_S3LITE_BIN,
K2_PILOT_BIN), the commit it holds the server to (the worktree HEAD, or
K2_SERVER_COMMIT), the results directory (K2_RUN_TAG) and each server's
environment in both postures: `k2` (the pinned §6 engine posture, the
default) and `launch` (deploy/profiles/compute-1g.env + shared-cell.env as
deploy/composer/streams/env.ts layers them). Also scrape.py's commit
verdict and timings shaping, and k2valid's posture check. Nothing here
starts a process that listens.

    python3 -m unittest discover -s bench/k2cost -p 'test_*.py'
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
SCRIPT = os.path.join(HERE, "run-local.sh")
sys.path.insert(0, HERE)

# The keys a fleet-off launch cell never carries (shared-cell.env's header,
# deploy/composer/streams/env.ts) and the k2 posture's engine pins.
FLEET_KEYS = ("FLEET_PREFIX", "FLEET_MAX", "FLEET_MIN", "FLEET_AUTH_MODE", "WORKLOAD_TOKEN_FILE", "SELF_URL",
              "FLEET_ALLOW_HTTP_PEERS")
ENGINE_PINS = {"WAL_GROUP_COMMIT": "1", "FLUSH_INTERVAL_MS": "25", "WAL_POST_ACK_GATHER_MS": "6",
               "FRAME_COMPRESS": "1", "ADMIT_MAX_INFLIGHT_PER_STREAM": "256",
               "LIMIT_BYTES_PER_SEC": "5000000", "LIMIT_REQS_PER_SEC": "1000", "LIMIT_RECS_PER_SEC": "5000",
               "INITIAL_SHARDS": "4"}


def profile(name: str) -> dict[str, str]:
    out = {}
    with open(os.path.join(REPO, "deploy", "profiles", name), encoding="utf-8") as f:
        for line in f:
            m = re.match(r"^([A-Z][A-Z0-9_]*)=(.*)$", line.strip())
            if m:
                out[m.group(1)] = m.group(2)
    return out


def head() -> str:
    return subprocess.run(["git", "-C", REPO, "rev-parse", "HEAD"], capture_output=True, text=True,
                          check=True).stdout.strip()


class DryRun(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="run-local-")
        self.home = os.path.join(self.tmp, "k2home")
        self.bins = {}
        for name in ("streams-slate", "s3lite", "pilot"):
            path = os.path.join(self.tmp, "bin", name)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                f.write(f"#!/bin/sh\n# {name}: never started by a dry run\nexit 99\n")
            os.chmod(path, 0o755)
            self.bins[name] = path

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def point(self, body: str) -> str:
        path = os.path.join(self.tmp, "TEST-point.env")
        with open(path, "w") as f:
            f.write(body)
        return path

    def rig(self, point: str, **env: str) -> subprocess.CompletedProcess:
        full = {k: v for k, v in os.environ.items() if not k.startswith("K2_")}
        full.update({"K2_HOME": self.home, "K2_SERVER_BIN": self.bins["streams-slate"],
                     "K2_S3LITE_BIN": self.bins["s3lite"], "K2_PILOT_BIN": self.bins["pilot"]})
        full.update(env)
        return subprocess.run(["/bin/bash", SCRIPT, point, "--dry-run"], capture_output=True, text=True,
                              env=full, cwd=REPO, timeout=60)

    def ok(self, point: str, **env: str) -> str:
        r = self.rig(point, **env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        return r.stdout

    @staticmethod
    def line(out: str, prefix: str) -> str:
        hits = [l for l in out.splitlines() if l.startswith(prefix)]
        assert len(hits) == 1, (prefix, out)
        return hits[0][len(prefix):].strip()

    @staticmethod
    def server_env(out: str, n: int) -> dict[str, str]:
        lines = out.splitlines()
        i = lines.index(f"DRY_RUN server-{n}.env:")
        env = {}
        for l in lines[i + 1:]:
            if not l.startswith("  "):
                break
            k, v = l.strip().split("=", 1)
            env[k] = v
        return env

    def test_default_point_keeps_the_ports_and_the_k2_posture(self):
        out = self.ok(self.point("POINT_NAME=TEST\nPHASES='idle 1'\n"))
        self.assertEqual(self.line(out, "DRY_RUN ports:"),
                         "emulator 9580 s3lite 9581 streams-1 9582 streams-2 9583 lb 9584")
        self.assertEqual(self.line(out, "DRY_RUN posture:"), "k2 (1 server)")
        env = self.server_env(out, 1)
        for k, v in ENGINE_PINS.items():
            self.assertEqual(env.get(k), v, k)
        compute = profile("compute-1g.env")
        for k in ("SHARED_CACHE_BYTES", "L0_SST_SIZE_BYTES", "ADMIT_RSS_SHED_MB"):
            self.assertEqual(env[k], compute[k], k)
        self.assertEqual(env["SLATE_S3_ENDPOINT"], "http://127.0.0.1:9581")
        self.assertEqual(env["FLEET_PREFIX"], "k2fleet")
        self.assertEqual(env["FLEET_MAX"], "1")
        self.assertEqual(env["FLEET_AUTH_MODE"], "workload")
        self.assertEqual(env["WORKLOAD_TOKEN_FILE"], "<secrets>/feeds/workload.jwt")
        self.assertEqual(env["INSTANCE_NAME"], "streams-1")
        self.assertEqual(env["ROLLUP"], "1")
        self.assertEqual(env["AUTH_TOKEN"], "<redacted>")
        self.assertEqual(env["USAGE_STREAM_KEY"], "<redacted>")
        self.assertNotIn("TAIL_RING_BYTES", env)
        self.assertNotIn("KEEP_AWAKE", env)
        # A dry run leaves no results directory and no secrets behind.
        self.assertFalse(os.path.exists(os.path.join(self.home, "results")) and
                         os.listdir(os.path.join(self.home, "results")))
        self.assertEqual(os.listdir(os.path.join(self.home, "secrets")), [])

    def test_port_base_moves_every_port_and_url(self):
        out = self.ok(self.point("POINT_NAME=TEST\nSERVERS=2\nPHASES='idle 1'\n"), K2_PORT_BASE="9670")
        self.assertEqual(self.line(out, "DRY_RUN ports:"),
                         "emulator 9670 s3lite 9671 streams-1 9672 streams-2 9673 lb 9674")
        one, two = self.server_env(out, 1), self.server_env(out, 2)
        self.assertEqual(one["SLATE_S3_ENDPOINT"], "http://127.0.0.1:9671")
        self.assertEqual(one["SELF_URL"], "http://127.0.0.1:9672")
        self.assertEqual(two["SELF_URL"], "http://127.0.0.1:9673")
        self.assertEqual((one["ROLLUP"], two["ROLLUP"]), ("1", "0"))
        self.assertEqual(self.line(out, "DRY_RUN pilot binary:"), self.bins["pilot"])

    def test_port_base_must_be_a_port_range(self):
        for bad in ("95x0", "80", "65533", ""):
            r = self.rig(self.point("PHASES='idle 1'\n"), K2_PORT_BASE=bad)
            if bad == "":
                self.assertEqual(r.returncode, 0, r.stderr)  # empty is the default
                continue
            self.assertEqual(r.returncode, 2, (bad, r.stdout, r.stderr))
            self.assertIn("K2_PORT_BASE", r.stderr)

    def test_launch_posture_is_the_two_profiles_without_fleet_or_engine_pins(self):
        out = self.ok(self.point("POINT_NAME=TEST\nPOSTURE=launch\nPHASES='idle 1'\n"), K2_PORT_BASE="9660")
        self.assertEqual(self.line(out, "DRY_RUN posture:"), "launch (1 server)")
        env = self.server_env(out, 1)
        layered = {**profile("compute-1g.env"), **profile("shared-cell.env")}
        feeds = {"STREAMS_AUTH_KEYS_FILE": "<secrets>/feeds/keys.json",
                 "STREAMS_AUTH_POLICY_FILE": "<secrets>/feeds/policies.json",
                 "STREAMS_AUTH_GRANTS_FILE": "<secrets>/feeds/grants.json"}
        for k, v in layered.items():
            if k == "KEEP_AWAKE":
                self.assertNotIn(k, env)  # the wrapper's, never the binary's (env.ts)
            else:
                self.assertEqual(env.get(k), feeds.get(k, v), k)
        self.assertEqual(env["INITIAL_SHARDS"], "1")
        self.assertEqual(env["SHARED_CACHE_BYTES"], "33554432")
        # The shared-cell profile carries the tail ring (8 MiB, 4f048b12).
        self.assertEqual(env["TAIL_RING_BYTES"], layered["TAIL_RING_BYTES"])
        for k in FLEET_KEYS + ("INSTANCE_NAME",):
            self.assertNotIn(k, env)
        for k in ENGINE_PINS:
            if k not in layered:
                self.assertNotIn(k, env)
        self.assertEqual(env["SLATE_S3_ENDPOINT"], "http://127.0.0.1:9661")
        self.assertEqual(env["REGION"], "local")
        self.assertEqual(env["ROLLUP"], "1")
        self.assertEqual(env["STREAMS_RELEASE_POSTURE"], "1")

    def test_launch_posture_takes_server_env_extra_last(self):
        out = self.ok(self.point("POSTURE=launch\nSERVER_ENV_EXTRA='TAIL_RING_BYTES=8388608 -ROLLUP'\n"
                                 "PHASES='idle 1'\n"))
        env = self.server_env(out, 1)
        self.assertEqual(env["TAIL_RING_BYTES"], "8388608")
        self.assertNotIn("ROLLUP", env)

    def test_launch_posture_is_one_server(self):
        r = self.rig(self.point("POSTURE=launch\nSERVERS=2\nPHASES='idle 1'\n"))
        self.assertEqual(r.returncode, 2)
        self.assertIn("POSTURE=launch is one server", r.stderr)
        r = self.rig(self.point("POSTURE=soak7\nPHASES='idle 1'\n"))
        self.assertEqual(r.returncode, 2)
        self.assertIn("POSTURE must be k2 or launch", r.stderr)

    def test_binaries_and_the_commit_the_server_is_held_to(self):
        out = self.ok(self.point("PHASES='idle 1'\n"))
        sha = hashlib.sha256(open(self.bins["streams-slate"], "rb").read()).hexdigest()
        self.assertEqual(self.line(out, "DRY_RUN server binary:"), f"{self.bins['streams-slate']} sha256 {sha}")
        sha = hashlib.sha256(open(self.bins["s3lite"], "rb").read()).hexdigest()
        self.assertEqual(self.line(out, "DRY_RUN s3lite binary:"), f"{self.bins['s3lite']} sha256 {sha}")
        self.assertEqual(self.line(out, "DRY_RUN pilot binary:"), "unused (1 server)")
        self.assertEqual(self.line(out, "DRY_RUN server commit:"), f"{head()} (worktree HEAD)")
        out = self.ok(self.point("PHASES='idle 1'\n"), K2_SERVER_COMMIT="2e0d1b86")
        self.assertEqual(self.line(out, "DRY_RUN server commit:"), "2e0d1b86 (K2_SERVER_COMMIT)")

    def test_a_missing_binary_is_refused_by_its_variable(self):
        os.remove(self.bins["streams-slate"])
        r = self.rig(self.point("PHASES='idle 1'\n"))
        self.assertEqual(r.returncode, 2)
        self.assertIn("K2_SERVER_BIN", r.stderr)
        # Unset, it is this checkout's release build (CI's job has one).
        r = self.rig(self.point("PHASES='idle 1'\n"), K2_SERVER_BIN="")
        default = os.path.join(REPO, "target", "release", "streams-slate")
        if os.access(default, os.X_OK):
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertTrue(self.line(r.stdout, "DRY_RUN server binary:").startswith(default + " sha256 "))
        else:
            self.assertEqual(r.returncode, 2)
            self.assertIn("target/release/streams-slate missing", r.stderr)

    def test_a_short_server_commit_is_refused(self):
        r = self.rig(self.point("PHASES='idle 1'\n"), K2_SERVER_COMMIT="2e0d")
        self.assertEqual(r.returncode, 2)
        self.assertIn("K2_SERVER_COMMIT", r.stderr)

    def test_run_tag_names_the_results_directory(self):
        out = self.ok(self.point("POINT_NAME=SCR\nPHASES='idle 1'\n"), K2_RUN_TAG="cand")
        run = self.line(out, "DRY_RUN results:")
        self.assertRegex(os.path.basename(run), r"^SCR-cand-\d{8}T\d{6}Z$")
        self.assertEqual(os.path.dirname(run), os.path.join(self.home, "results"))
        r = self.rig(self.point("PHASES='idle 1'\n"), K2_RUN_TAG="a/b")
        self.assertEqual(r.returncode, 2)
        self.assertIn("K2_RUN_TAG", r.stderr)

    def manifest(self, name: str = "unit-pack", count: int = 3) -> str:
        path = os.path.join(self.tmp, f"{name}.json")
        with open(path, "w") as f:
            json.dump({"name": name, "seed": 1, "duration": 100, "periods": {"steady": [20, 80]},
                       "classes": [{"class": "t", "count": count, "role": "compliant", "phases": []}]}, f)
        return path

    def test_a_tenants_point_runs_the_launch_posture_on_cell_admins_ids(self):
        m = self.manifest()
        out = self.ok(self.point(f"POINT_NAME=PACK\nPOSTURE=launch\nTENANTS_MANIFEST={m}\nPHASES='idle 1\ntenants'\n"))
        self.assertEqual(self.line(out, "DRY_RUN tenants:"),
                         f'{m} (manifest unit-pack, 3 projects, periods {{"steady": [20, 80]}}); cell cell-k2, '
                         "deployment project proj-k2cost-deploy, account acct-k2cost")
        env = self.server_env(out, 1)
        # The ids cell-admin's init is given: its policies name this cell.
        self.assertEqual((env["CELL_ID"], env["PROJECT_ID"], env["ACCOUNT_ID"]),
                         ("cell-k2", "proj-k2cost-deploy", "acct-k2cost"))
        self.assertEqual(env["STREAMS_AUTH_POLICY_FILE"], "<secrets>/feeds/policies.json")
        self.assertEqual(env["PROJECT_SHARE_K"], profile("shared-cell.env")["PROJECT_SHARE_K"])
        # A manifest path relative to the checkout is the checkout's.
        rel = os.path.relpath(m, REPO)
        out = self.ok(self.point(f"POSTURE=launch\nTENANTS_MANIFEST={rel}\nPHASES='tenants --time-scale 0.5'\n"))
        self.assertTrue(self.line(out, "DRY_RUN tenants:").startswith(f"{os.path.join(REPO, rel)} (manifest unit-pack"))

    def test_a_tenants_point_takes_the_rigs_own_lines_only(self):
        m = self.manifest()
        cases = {
            f"POSTURE=launch\nTENANTS_MANIFEST={m}\nPHASES='produce --streams 1 --rate 1'\n":
                "a tenants point runs idle, restart, quiesce and tenants lines only",
            f"POSTURE=launch\nTENANTS_MANIFEST={m}\nPHASES='tenants &'\n": "a tenants phase runs in the foreground",
            "POSTURE=launch\nPHASES='tenants'\n": "a tenants phase needs TENANTS_MANIFEST",
            f"TENANTS_MANIFEST={m}\nPHASES='tenants'\n": "TENANTS_MANIFEST runs on POSTURE=launch",
            f"POSTURE=launch\nTENANTS_MANIFEST={m}.missing\nPHASES='tenants'\n": "no tenants manifest",
        }
        for body, err in cases.items():
            r = self.rig(self.point(body))
            self.assertEqual(r.returncode, 2, (body, r.stdout, r.stderr))
            self.assertIn(err, r.stderr, body)

    def test_the_packing_points_dry_run_every_step(self):
        steps = {"PACK-T.env": (1, 100, 400, 1000), "PACK-S.env": (1, 5, 10, 20, 30, 46)}
        for point, ns in steps.items():
            for n in ns:
                out = self.ok(os.path.join(HERE, "points", point), PACK_N=str(n))
                name = f"{point[:-4]}-{n:04d}"
                self.assertEqual(self.line(out, "DRY_RUN point:").split()[0], name)
                manifest = os.path.join(HERE, "tenants", f"pack-{point[5].lower()}-{n:04d}.json")
                self.assertTrue(self.line(out, "DRY_RUN tenants:").startswith(f"{manifest} (manifest pack-"), out)
                self.assertIn(f"{n} projects", self.line(out, "DRY_RUN tenants:"))
                env = self.server_env(out, 1)
                self.assertEqual((env["TOKIO_WORKERS"], env["INITIAL_SHARDS"]), ("2", "1"))
            r = self.rig(os.path.join(HERE, "points", point))
            self.assertNotEqual(r.returncode, 0)
            self.assertIn("set PACK_N to the tenant count", r.stderr)
            r = self.rig(os.path.join(HERE, "points", point), PACK_N="7")
            self.assertEqual(r.returncode, 2)
            self.assertIn("no tenants manifest", r.stderr)
        out = self.ok(os.path.join(HERE, "points", "PACK-S.env"), PACK_N="5", PACK_SMOKE="1")
        self.assertEqual(self.line(out, "DRY_RUN point:").split()[0], "PACK-S-0005-smoke")


class Posture(unittest.TestCase):
    def test_commit_verdict(self):
        import scrape
        full = "2e0d1b86edf374719886a5f7dea68d258d1fa4e3"
        v = scrape.commit_verdict(head="aaaa" * 10, expected=full, source="K2_SERVER_COMMIT", commits=[full])
        self.assertEqual(v, {"expected": full, "source": "K2_SERVER_COMMIT", "reported": [full],
                             "matches_expected": True, "matches_head": False})
        v = scrape.commit_verdict(head=full, expected="2e0d1b86", source="K2_SERVER_COMMIT", commits=[full, full])
        self.assertTrue(v["matches_expected"])
        self.assertTrue(v["matches_head"])
        v = scrape.commit_verdict(head=full, expected=full, source="worktree HEAD", commits=[full, None])
        self.assertFalse(v["matches_expected"])
        v = scrape.commit_verdict(head=full, expected="2e0d1b87", source="K2_SERVER_COMMIT", commits=[full])
        self.assertFalse(v["matches_expected"])

    def test_timings_keep_the_pump_and_ring_counters(self):
        import scrape
        body = {"0000": {"groups": [{"ts_ms": 1}], "pump": {"flushes": 7, "barrier_acked": 9},
                         "tail_ring": {"hits": 3, "misses": 1}}}
        self.assertEqual(scrape.shape_timings(body),
                         {"0000": {"pump": {"flushes": 7, "barrier_acked": 9}, "tail_ring": {"hits": 3, "misses": 1}}})
        self.assertIsNone(scrape.shape_timings(None))

    def test_validity_holds_the_server_to_the_declared_commit(self):
        import k2valid
        posture = {"git": {"head": "bbbb"}, "binary_commit_matches_head": False,
                   "server_commit": {"expected": "aaaa", "source": "K2_SERVER_COMMIT", "reported": ["aaaa"],
                                     "matches_expected": True, "matches_head": False},
                   "servers": {"streams-1": {"git_commit": "aaaa"}}}
        v = k2valid.validity([], {"run": (0, 1), "load": (0, 1)}, False, {}, {}, {}, {}, [], posture, [], False)
        check = [c for c in v["checks"] if c["check"].startswith("server binaries built from")]
        self.assertEqual(len(check), 1)
        self.assertEqual(check[0]["check"], "server binaries built from the declared commit (posture)")
        self.assertTrue(check[0]["ok"])
        self.assertEqual(check[0]["detail"]["expected"], "aaaa")
        posture["server_commit"]["matches_expected"] = False
        v = k2valid.validity([], {"run": (0, 1), "load": (0, 1)}, False, {}, {}, {}, {}, [], posture, [], False)
        self.assertFalse([c for c in v["checks"] if c["check"].startswith("server binaries built from")][0]["ok"])
        # A posture.json from before the declared commit: the worktree HEAD rule.
        old = {"git": {"head": "bbbb"}, "binary_commit_matches_head": True, "servers": {}}
        v = k2valid.validity([], {"run": (0, 1), "load": (0, 1)}, False, {}, {}, {}, {}, [], old, [], False)
        self.assertTrue([c for c in v["checks"] if c["check"].startswith("server binaries built from")][0]["ok"])


if __name__ == "__main__":
    unittest.main()
