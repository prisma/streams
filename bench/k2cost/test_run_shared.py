"""run-shared.sh's offline half (--dry-run) on a fake release directory in
the layout scripts/cut-release.sh writes (deploy/composer/release.ts):
what it refuses, what it stages (the release's deploy/ tree, the build dir
Compute ships), the cell's deploy/cells/scratch.env, the seven secret names
it hands the container through a file, the service's input document
(COMPOSER_INPUT) and the docker invocation; the live half's host-side
addresses with every tool that would start or reach something replaced by
a recorder; and scrape.py loop's --operator scrape against a fake cell.
deploy/composer/streams/entry.e2e.test.ts runs the real bundled entry from
the rig's staging of a release that release.ts staged; a real live run
needs docker images.

    python3 -m unittest discover -s bench/k2cost -p 'test_*.py'
"""

from __future__ import annotations

import base64
import hashlib
import http.server
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
SCRIPT = os.path.join(HERE, "run-shared.sh")
ARM = os.uname().machine in ("arm64", "aarch64")
BASELINE = "0f" * 32
SECRET_NAMES = ["STREAMS_OPERATOR_TOKEN", "STREAMS_USAGE_STREAM_KEY", "STREAMS_CURSOR_KEY", "STREAMS_DATA_S3_ACCESS_KEY_ID",
                "STREAMS_DATA_S3_SECRET_ACCESS_KEY", "STREAMS_FEEDS_S3_ACCESS_KEY_ID", "STREAMS_FEEDS_S3_SECRET_ACCESS_KEY"]
# deploy/composer/app.ts's envSecret binding: each input field's platform
# name, which the service's input document points to with $secret.
SECRET_INPUTS = {"operatorToken": "STREAMS_OPERATOR_TOKEN", "usageStreamKey": "STREAMS_USAGE_STREAM_KEY",
                 "cursorKey": "STREAMS_CURSOR_KEY", "dataS3AccessKeyId": "STREAMS_DATA_S3_ACCESS_KEY_ID",
                 "dataS3SecretAccessKey": "STREAMS_DATA_S3_SECRET_ACCESS_KEY",
                 "feedsS3AccessKeyId": "STREAMS_FEEDS_S3_ACCESS_KEY_ID",
                 "feedsS3SecretAccessKey": "STREAMS_FEEDS_S3_SECRET_ACCESS_KEY"}
ENTRY_JS = "deploy/composer/streams/entry.js"
SERVER = "deploy/composer/streams/streams-slate"


def repo_bytes(path: str) -> bytes:
    with open(os.path.join(REPO, path), "rb") as f:
        return f.read()


def elf(machine: int, tail: bytes) -> bytes:
    head = bytearray(b"\x7fELF" + b"\x02\x01\x01" + bytes(9) + b"\x02\x00")
    head += bytes([machine, 0])
    return bytes(head) + tail


class Rig(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="run-shared-")
        # The Composer project root (package.json, node_modules/, bin/s3lite)
        # around deploy/, the cell's build dir, which holds release.json, the
        # cells files, the profiles, the app, the bundled entry and the server.
        self.rel = os.path.join(self.tmp, "release")
        self.bin = elf(0x3E, b"release binary")
        self.sha = hashlib.sha256(self.bin).hexdigest()
        files = {
            "package.json": b"{}\n",
            "bin/s3lite": elf(0x3E, b"s3lite"),
            "deploy/cells/production.env": b"CELL_STAGE=production\nCELL_ID=cell_production_real\n",
            "deploy/composer/app.ts": repo_bytes("deploy/composer/app.ts"),
            "deploy/composer/streams/entry.ts": b"// the cell's entry, as committed\n",
            ENTRY_JS: b"// entry.ts bundled at the cut (triage B2)\n",
            SERVER: self.bin,
        }
        for p in ("compute-1g.env", "shared-cell.env"):
            files[f"deploy/profiles/{p}"] = repo_bytes(f"deploy/profiles/{p}")
        for path, data in files.items():
            os.makedirs(os.path.dirname(os.path.join(self.rel, path)), exist_ok=True)
            with open(os.path.join(self.rel, path), "wb") as f:
                f.write(data)
        os.makedirs(os.path.join(self.rel, "node_modules", "@prisma", "compute"))
        with open(os.path.join(self.rel, "node_modules", "@prisma", "compute", "index.js"), "w") as f:
            f.write("// the Composer project's install\n")
        self.release = {"tag": "rc-test", "commit": "c0ffee", "server_sha256": self.sha,
                         "files": {p: hashlib.sha256(d).hexdigest() for p, d in sorted(files.items())}}
        self.write_release(self.release)
        self.home = os.path.join(self.tmp, "k2home")
        self.s3lite = os.path.join(self.tmp, "s3lite")
        with open(self.s3lite, "w") as f:
            f.write("#!/bin/sh\n")
        os.chmod(self.s3lite, 0o755)

    def write_release(self, doc: dict) -> None:
        with open(os.path.join(self.rel, "deploy", "release.json"), "w") as f:
            json.dump(doc, f, indent=2)
            f.write("\n")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def rig(self, *args: str, baseline: bool = True) -> subprocess.CompletedProcess:
        argv = ["/bin/bash", SCRIPT, "--release", self.rel, "--s3lite", self.s3lite, *args]
        if baseline:
            argv += ["--bun-baseline-sha256", BASELINE]
        env = {**os.environ, "K2_HOME": self.home}
        return subprocess.run(argv, capture_output=True, text=True, env=env, cwd=REPO, timeout=60)

    def staged(self) -> str:
        runs = os.listdir(os.path.join(self.home, "results"))
        self.assertEqual(len(runs), 1)
        return os.path.join(self.home, "results", runs[0], "rig-context")

    def live(self, s3_bind: str | None) -> tuple[subprocess.CompletedProcess, list]:
        """The live half end to end with every tool that would start, bind,
        reach or build something replaced by a recorder (bun, docker, curl,
        lsof, openssl, s3lite, and python3 for scrape.py and
        shared_verdict.py): what each was asked to do, in order. Nothing
        listens and nothing connects."""
        fake = os.path.join(self.tmp, f"fakebin-{s3_bind}")
        os.makedirs(fake)
        log = os.path.join(fake, "calls.log")
        scripts = {
            "bun": 'case $2 in tokens) while [ $# -gt 0 ]; do [ "$1" = --out ] && echo "{}" > "$2"; shift; done ;; esac',
            # `docker run` keeps a copy of the container's env file, which
            # the rig removes with its secrets directory at exit.
            "docker": f'case $1 in logs) echo "binary sha256 {self.sha}" ;;\n'
                      '  run) while [ $# -gt 0 ]; do [ "$1" = --env-file ] && cp "$2" "$RIG_FAKE_LOG.env"; shift; done ;; esac',
            "curl": 'for a; do url=$a; done; echo "curl $url" >> "$RIG_FAKE_LOG"\n'
                    'case $url in */v1/debug/load) echo \'{"git_commit": "c0ffee", "boot_id": "b1"}\' ;; esac\nexit 0',
            "lsof": "exit 1",
            "openssl": 'while [ $# -gt 0 ]; do [ "$1" = -out ] && : > "$2"; shift; done',
            "python3": f'case $1 in */scrape.py|*/shared_verdict.py) ;; *) exec {sys.executable} "$@" ;; esac',
        }
        for name, body in scripts.items():
            with open(os.path.join(fake, name), "w") as f:
                record = "" if name == "curl" else f'echo "{name} $*" >> "$RIG_FAKE_LOG"\n'
                f.write(f"#!/bin/sh\n{record}{body}\nexit 0\n")
            os.chmod(os.path.join(fake, name), 0o755)
        with open(self.s3lite, "w") as f:
            f.write('#!/bin/sh\necho "s3lite $*" >> "$RIG_FAKE_LOG"\nexec sleep 30\n')
        env = {**os.environ, "K2_HOME": self.home, "RIG_FAKE_LOG": log, "PATH": fake + os.pathsep + os.environ["PATH"],
               "RIG_IDLE_SECS": "0", "RIG_Q0_IDLE_SECS": "0"}
        env.pop("S3_BIND", None)
        if s3_bind is not None:
            env["S3_BIND"] = s3_bind
        r = subprocess.run(["/bin/bash", SCRIPT, "--release", self.rel, "--s3lite", self.s3lite, "--bun-baseline-sha256", BASELINE],
                           capture_output=True, text=True, env=env, cwd=REPO, timeout=120)
        with open(log) as f:
            return r, [line.rstrip("\n") for line in f]

    def test_the_host_side_clients_reach_s3lite_on_the_address_it_listens_on(self):
        # A specific address (Linux: the docker bridge) answers only there;
        # 0.0.0.0 answers on the loopback too. The cell stays on 127.0.0.1.
        for bind, host in ((None, "127.0.0.1"), ("172.17.0.1", "172.17.0.1"), ("0.0.0.0", "127.0.0.1")):
            r, calls = self.live(bind)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            want = f"http://{host}:9591"
            arg = lambda prefix, flag: [c.split()[c.split().index(flag) + 1] for c in calls if c.startswith(prefix)]  # noqa: E731
            self.assertEqual(arg("s3lite ", "--listen"), [f"{bind or '127.0.0.1'}:9591"])
            self.assertEqual({"ready": sorted({c[5:] for c in calls if c.startswith("curl ") and "/_s3lite/" in c}),
                              "publish": arg(f"bun {HERE}/tenants-admit.ts publish ", "--s3-endpoint"),
                              "scrape": arg(f"python3 {HERE}/scrape.py loop ", "--s3lite")},
                             {"ready": [f"{want}/_s3lite/stats"], "publish": [want], "scrape": [want]}, bind)
            self.assertEqual(sorted({c[5:] for c in calls if c.startswith("curl ") and "/_s3lite/" not in c}),
                             ["http://127.0.0.1:9592/health", "http://127.0.0.1:9592/v1/debug/load"])
            self.assertEqual(arg(f"python3 {HERE}/scrape.py loop ", "--servers"), ["streams-1=http://127.0.0.1:9592"])
            shutil.rmtree(self.home)

    def test_the_container_secrets_are_what_the_server_decodes(self):
        # src/config/validation.rs decodes STREAMS_CURSOR_KEY with base64's
        # STANDARD engine (padded, + and /) to exactly 32 bytes; a key the
        # binary refuses stops the cell before it answers /health.
        r, _ = self.live(None)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        with open(os.path.join(self.tmp, "fakebin-None", "calls.log.env")) as f:
            env = dict(line.rstrip("\n").split("=", 1) for line in f if line.strip())
        self.assertEqual(sorted(env), sorted(SECRET_NAMES))
        self.assertEqual(len(base64.b64decode(env["STREAMS_CURSOR_KEY"], validate=True)), 32)
        self.assertTrue(all(env.values()), env.keys())

    def test_a_dry_run_stages_the_release_the_cell_env_and_the_container_invocation(self):
        r = self.rig("--dry-run")
        self.assertEqual(r.returncode, 0, r.stderr)
        ctx = self.staged()
        files = sorted(os.path.relpath(os.path.join(d, f), ctx) for d, _, fs in os.walk(ctx) for f in fs)
        # deploy/ as the release holds it, the rig's cells file beside the
        # release's own; never the Composer project root around it.
        self.assertEqual(files, ["deploy/cells/production.env", "deploy/cells/scratch.env", "deploy/composer/app.ts",
                                 ENTRY_JS, "deploy/composer/streams/entry.ts", SERVER,
                                 "deploy/profiles/compute-1g.env", "deploy/profiles/shared-cell.env", "deploy/release.json"])
        for path in ("deploy/release.json", SERVER, ENTRY_JS, "deploy/cells/production.env"):
            with open(os.path.join(ctx, path), "rb") as f, open(os.path.join(self.rel, path), "rb") as g:
                self.assertEqual(f.read(), g.read(), path)
        with open(os.path.join(ctx, "deploy", "cells", "scratch.env")) as f:
            env = f.read()
        self.assertRegex(env, re.compile(
            r"\ACELL_STAGE=scratch\nCELL_ID=cell_rig\nPROJECT_ID=proj_rig_deploy\nACCOUNT_ID=acct_rig_sink\n"
            r"STREAMS_AUTH_ISSUER=https://auth.prisma.io\nPATH_PREFIX=rig\nREGION=local\n"
            r"SLATE_S3_ENDPOINT=http://host.docker.internal:9591\nSLATE_S3_BUCKET=rig-data-\d{8}t\d{6}z\nSLATE_S3_REGION=us-east-1\n"
            r"FEEDS_S3_ENDPOINT=http://host.docker.internal:9591\nFEEDS_S3_BUCKET=rig-feeds-\d{8}t\d{6}z\nFEEDS_S3_REGION=us-east-1\n"
            r"FEEDS_S3_KEY=cells/cell_rig/feeds-bundle.json\nCPU_LOG_SECS=60\n\Z"))
        out = r.stdout
        self.assertIn(f"rehearsal: false, binary sha256 {self.sha}", out)
        names = re.search(r"^DRY_RUN container env names: (.*)$", out, re.M).group(1).split()
        self.assertEqual(names, SECRET_NAMES)
        run = re.search(r"^DRY_RUN (docker run .*)$", out, re.M).group(1)
        for want in ("--platform linux/amd64 --cpus 1 --memory 1g --memory-swap 1g", "-p 127.0.0.1:9592:8080",
                     "--add-host=host.docker.internal:host-gateway", "--env-file ", "-e PORT=8080 -e COMPOSER_INPUT="):
            self.assertIn(want, run)
        self.assertNotRegex(run, r"STREAMS_[A-Z_]+=")
        self.assertNotIn("CELL_STAGE", run)
        # The input document Composer's runtime reads: the stage and the
        # deploy's nonce (the release's tag, as a first deploy of it) as
        # literals, and a pointer to each platform name the env file holds.
        doc = json.loads(re.search(r" -e COMPOSER_INPUT=(\S+) ", run).group(1))
        self.assertEqual(doc, {**{k: {"$secret": v} for k, v in SECRET_INPUTS.items()},
                               "cellStage": "scratch", "deployNonce": "rc-test"})
        self.assertEqual(sorted(SECRET_INPUTS.values()), sorted(names))
        build = re.search(r"^DRY_RUN (docker build .*)$", out, re.M).group(1)
        dockerfile = os.path.join(REPO, "bench", "docker", "Dockerfile.rig")
        self.assertIn("--platform linux/amd64 -f " + dockerfile, build)
        self.assertIn(f"--build-arg BUN_BASELINE_SHA256={BASELINE} {ctx}", build)
        # The image runs the bundled entry the rig requires, from the staged
        # copy's root.
        with open(dockerfile) as f:
            self.assertEqual(re.findall(r"^(?:WORKDIR|COPY|CMD) .*$", f.read(), re.M),
                             ["WORKDIR /app", "COPY . /app", f'CMD ["bun", "{ENTRY_JS}"]'])
        self.assertIn("--listen 127.0.0.1:9591 --latency-ms 20", out)
        with open(re.search(r"^DRY_RUN checkout: (.*)$", out, re.M).group(1)) as f:
            co = json.load(f)
        git = lambda *a: subprocess.run(["git", "-C", REPO, *a], capture_output=True, text=True).stdout.strip()  # noqa: E731
        self.assertEqual(co, {"head": git("rev-parse", "HEAD"),
                              "trees": {p: git("rev-parse", f"HEAD:{p}") for p in ("bench/k2cost", "deploy/cell-admin", "bench/docker")},
                              "dirty": [l for l in git("status", "--porcelain", "--untracked-files=all", "--", "bench/k2cost",
                                                       "deploy/cell-admin", "bench/docker").splitlines() if l]})
        self.assertEqual(os.listdir(os.path.join(self.home, "secrets")), [], "the secrets directory outlived the run")

    def test_the_rig_runs_as_triage_b2s_scratch_stage_over_the_releases_own_scratch_cell(self):
        # A release cut with the scratch cell's values (the real buckets): the
        # staged copy gets the rig's, the release keeps its own.
        real = b"CELL_STAGE=scratch\nCELL_ID=cell_scratch_real\n"
        with open(os.path.join(self.rel, "deploy", "cells", "scratch.env"), "wb") as f:
            f.write(real)
        r = self.rig("--dry-run")
        self.assertEqual(r.returncode, 0, r.stderr)
        with open(os.path.join(self.staged(), "deploy", "cells", "scratch.env")) as f:
            self.assertEqual(f.read().splitlines()[:2], ["CELL_STAGE=scratch", "CELL_ID=cell_rig"])
        with open(os.path.join(self.rel, "deploy", "cells", "scratch.env"), "rb") as f:
            self.assertEqual(f.read(), real)
        self.assertIn("DRY_RUN deploy/cells/scratch.env:", r.stdout)

    def test_a_rehearsal_stages_its_own_binary_under_a_rewritten_release_json(self):
        other = os.path.join(self.tmp, "layout5-streams-slate")
        data = elf(0x3E, b"a layout-5 build")
        with open(other, "wb") as f:
            f.write(data)
        r = self.rig("--dry-run", "--binary", other)
        self.assertEqual(r.returncode, 0, r.stderr)
        sha = hashlib.sha256(data).hexdigest()
        self.assertIn(f"rehearsal: true, binary sha256 {sha}", r.stdout)
        ctx = self.staged()
        with open(os.path.join(ctx, SERVER), "rb") as f:
            self.assertEqual(f.read(), data)
        with open(os.path.join(ctx, "deploy", "release.json")) as f:
            self.assertEqual(json.load(f), {**self.release, "server_sha256": sha, "rehearsal": True})
        with open(os.path.join(self.rel, "deploy", "release.json")) as f:
            self.assertEqual(json.load(f), self.release)
        with open(os.path.join(self.rel, SERVER), "rb") as f:
            self.assertEqual(f.read(), self.bin)

    def test_it_refuses_what_cannot_be_the_release_or_cannot_run(self):
        def refused(want: str, *args: str, baseline: bool = True):
            r = self.rig(*args, baseline=baseline)
            self.assertEqual((r.returncode, want in r.stderr), (2, True), r.stderr)
            self.assertFalse(os.path.isdir(os.path.join(self.home, "results")), "a refused run staged something")
        server = os.path.join(self.rel, SERVER)
        with open(server, "wb") as f:
            f.write(elf(0x3E, b"not the release"))
        refused("certification runs the release's binary only", "--dry-run")
        with open(server, "wb") as f:
            f.write(elf(0xB7, b"aarch64"))
        refused("is not an x86_64 ELF (e_machine 0xb7)", "--dry-run")
        with open(server, "wb") as f:
            f.write(self.bin)
        refused("must be 64 lowercase hex digits", "--dry-run", "--bun-baseline-sha256", "XYZ", baseline=False)
        if ARM:
            refused("without AVX, which bun's x64 build needs", "--dry-run", baseline=False)
        refused("needs the solo LiveFeed canary (CANARY_SHAPE=solo, triage B9)", "--canary")
        # release.ts's own test of a release.json (its files, a lowercase-hex
        # server_sha256), and a tag for the deploy's nonce.
        for doc in ({**self.release, "server_sha256": f"sha256:{self.sha}"}, {**self.release, "tag": ""},
                    {k: v for k, v in self.release.items() if k != "files"}):
            self.write_release(doc)
            refused(f"{self.rel}/deploy/release.json is not a release.json cut-release wrote", "--dry-run")
        self.write_release(self.release)
        # An app whose binding the rig's input document cannot follow.
        app = os.path.join(self.rel, "deploy", "composer", "app.ts")
        text = repo_bytes("deploy/composer/app.ts").decode()
        for line, want in (('      extraKey: envSecret("STREAMS_EXTRA_KEY"),\n',
                            "binds the secrets STREAMS_OPERATOR_TOKEN STREAMS_USAGE_STREAM_KEY STREAMS_CURSOR_KEY "
                            "STREAMS_DATA_S3_ACCESS_KEY_ID STREAMS_DATA_S3_SECRET_ACCESS_KEY STREAMS_FEEDS_S3_ACCESS_KEY_ID "
                            "STREAMS_FEEDS_S3_SECRET_ACCESS_KEY STREAMS_EXTRA_KEY, not the seven the rig provides"),
                           ('      cellRegion: envParam("CELL_REGION"),\n',
                            "binds the settings CELL_STAGE CELL_DEPLOY_NONCE CELL_REGION, not the rig's CELL_STAGE and CELL_DEPLOY_NONCE")):
            anchor = '      deployNonce: envParam("CELL_DEPLOY_NONCE"),\n'
            self.assertIn(anchor, text)
            with open(app, "w") as f:
                f.write(text.replace(anchor, anchor + line))
            refused(f"{self.rel}/deploy/composer/app.ts {want}", "--dry-run")
        with open(app, "w") as f:
            f.write(text)
        for path in (ENTRY_JS, "deploy/composer/app.ts", "deploy/release.json"):
            os.rename(os.path.join(self.rel, path), os.path.join(self.tmp, "away"))
            refused(f"{self.rel} has no {path}", "--dry-run")
            os.rename(os.path.join(self.tmp, "away"), os.path.join(self.rel, path))
        r = subprocess.run(["/bin/bash", SCRIPT], capture_output=True, text=True, cwd=REPO)
        self.assertEqual((r.returncode, "--release DIR is required" in r.stderr), (2, True))


class Operator(unittest.TestCase):
    """scrape.py loop --operator adds /operator/data.json to each scrape."""

    def test_the_operator_dashboard_data_is_scraped_with_the_bearer(self):
        seen = []

        class H(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                seen.append((self.path, self.headers.get("authorization")))
                body = json.dumps({"path": self.path}).encode()
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        d = tempfile.mkdtemp(prefix="scrape-")
        try:
            auth = os.path.join(d, "debug.hdr")
            with open(auth, "w") as f:
                f.write("authorization: Bearer op\n")
            sys.path.insert(0, HERE)
            import scrape
            url = f"http://127.0.0.1:{srv.server_address[1]}"
            for operator in (False, True):
                args = types.SimpleNamespace(run=d, servers=f"streams-1={url}", auth_file=auth, interval=10.0, absorb_age_secs=60.0,
                                             s3lite=url, operator=operator)
                loop = scrape.Loop(args)
                loop.scrape_server("streams-1", url)
                loop.out.close()
            with open(os.path.join(d, "scrape.jsonl")) as f:
                recs = [json.loads(line) for line in f]
            self.assertEqual([r["src"] for r in recs], ["store", "load", "usage", "store", "load", "usage", "operator"])
            self.assertEqual(recs[-1]["body"], {"path": "/operator/data.json"})
            self.assertIn(("/operator/data.json", "Bearer op"), seen)
        finally:
            srv.shutdown()
            srv.server_close()
            shutil.rmtree(d)


if __name__ == "__main__":
    unittest.main()
