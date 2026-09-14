#!/usr/bin/env python3
"""Real-auth same-name lifecycle, retained fork, and ownership-transfer campaign."""
import argparse
import dataclasses
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request

from independent import CheckError, HttpClient, Journal, Response, Stream, canonical, stream_path
from lifecycle import LifecycleRecorder, load_lifecycle, pack
from local_store import durable_write, sync_directory

ROOT = Path(__file__).resolve().parents[2]
KEY = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc="
CELL = "lifecycle-cell"
DOMAINS = [("namespace-a", "proj_life_a", "ws_life_a"),
           ("namespace-b", "proj_life_b", "ws_life_b")]
SCOPES = ["streams.create", "streams.records.append", "streams.records.read",
          "streams.metadata.read", "streams.lifecycle.manage", "streams.forks.create",
          "streams.catalog.read"]


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class SurfaceClient:
    """Keep customer product authorization separate from the internal raw surface."""
    def __init__(self, product, raw=None):
        self.product, self.raw = product, raw

    def request(self, method, path, body=None, headers=None):
        if path.startswith("/v1/stream/"):
            if self.raw is None or method not in {"GET", "PUT"}:
                raise CheckError("raw fixture access is restricted to the deployment project and fork operations")
            return self.raw.request(method, path, body, headers)
        return self.product.request(method, path, body, headers)


class Fixture:
    """Own one authenticated cell, its emulator, isolated store and all processes."""
    def __init__(self, args):
        self.args, self.children, self.kills, self.tokens, self.secrets = args, [], [], {}, {}
        self.env = {key: os.environ[key] for key in ("HOME", "TMPDIR", "PATH") if key in os.environ}
        self.env.update(RUST_LOG="warn", TOKIO_WORKERS="4")
        self.server_number = 0
        self.binary_hashes = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                              for path in (args.server, args.s3lite, Path(args.node))}

    def verify_binaries(self):
        for path, expected in self.binary_hashes.items():
            if hashlib.sha256(Path(path).read_bytes()).hexdigest() != expected:
                raise CheckError("campaign binary changed during the run: " + Path(path).name)

    def start(self, name, command, ready, extra=None):
        self.verify_binaries()
        with (self.args.out / f"{name}.log").open("xb") as log:
            child = subprocess.Popen(command, cwd=ROOT, env={**self.env, **(extra or {})},
                                     stdout=log, stderr=subprocess.STDOUT)
        self.children.append((name, child))
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if child.poll() is not None:
                raise CheckError(f"{name} exited before readiness; inspect its log")
            try:
                with urllib.request.urlopen(ready, timeout=1) as response:
                    response.read()
                return child
            except (OSError, urllib.error.URLError):
                time.sleep(0.1)
        raise CheckError(f"{name} readiness deadline exceeded")

    def admin(self, method, path, data=None, credential=None):
        headers = {"Content-Type": "application/json"}
        if credential is not None:
            headers["Authorization"] = "StreamsCredential " + credential
        request = urllib.request.Request(self.emulator + path, method=method,
                                         data=None if data is None else canonical(data), headers=headers)
        try:
            response = urllib.request.urlopen(request, timeout=15)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return Response(response.status, dict(response.headers), response.read())

    def issue(self, namespace):
        response = self.admin("POST", f"/v1/projects/{namespace[1]}/streams/credentials",
                              {"displayName": "independent lifecycle fixture", "scopes": SCOPES})
        if response.status != 201:
            raise CheckError("fixture credential creation failed")
        secret = json.loads(response.body)["secret"]
        token = self.admin("POST", "/v1/token/streams", credential=secret)
        if token.status != 200:
            raise CheckError("fixture credential exchange failed")
        self.secrets[namespace] = secret
        self.tokens[namespace] = json.loads(token.body)["accessToken"]

    def boot(self):
        self.emulator = f"http://127.0.0.1:{free_port()}"
        self.feeds = self.args.out / "feeds"
        command = [self.args.node, "platform-demo/src/emulator.mjs", "--port", self.emulator.rsplit(":", 1)[1],
                   "--cells", f"{CELL}={self.feeds}",
                   "--workload-ops", "telemetry-append,segment-read,raw-read,raw-lifecycle"]
        for _, project, workspace in DOMAINS:
            command.extend(["--fixture", f"{project}:{workspace}:{CELL}"])
        self.start("platform-emulator", command, self.emulator + "/")
        for tenant, project, _ in DOMAINS:
            self.issue((tenant, project))
        self.store = f"http://127.0.0.1:{free_port()}"
        self.start("object-store", [str(self.args.s3lite), "--listen", self.store.removeprefix("http://"),
                                    "--latency-ms", "2"], self.store + "/_s3lite/stats")
        self.start_server()

    def start_server(self):
        self.base = f"http://127.0.0.1:{free_port()}"
        env = {"STREAMS_AUTH_MODE": "enforce", "STREAMS_AUTH_ISSUER": "https://auth.prisma.io",
               "STREAMS_AUTH_KEYS_FILE": str(self.feeds / "keys.json"),
               "STREAMS_AUTH_POLICY_FILE": str(self.feeds / "policies.json"),
               "STREAMS_AUTH_GRANTS_FILE": str(self.feeds / "grants.json"),
               "STREAMS_AUTH_REFRESH_SECS": "1", "FLEET_AUTH_MODE": "workload",
               "WORKLOAD_TOKEN_FILE": str(self.feeds / "workload.jwt"),
               "STREAMS_RELEASE_POSTURE": "1", "MAX_RECORD_PAYLOAD_BYTES": "131072",
               "CELL_ID": CELL, "PROJECT_ID": DOMAINS[0][1], "USAGE_STREAM_KEY": KEY}
        command = [str(self.args.server), "--listen", self.base.removeprefix("http://"),
                   "--s3-endpoint", self.store, "--bucket", "lifecycle", "--initial-shards", "1",
                   "--max-unflushed-bytes", "67108864", "--flush-interval-ms", "1",
                   "--wal-flush-gap-ms", "2", "--absorb-bytes", "1", "--absorb-age-secs", "1"]
        self.server = self.start(f"server-{self.server_number}", command, self.base + "/health", env)
        self.server_number += 1

    def clients(self):
        raw = HttpClient(self.base, (self.feeds / "workload.jwt").read_text().strip(), KEY)
        deployment = tuple(DOMAINS[0][:2])
        return {ns: SurfaceClient(HttpClient(self.base, token, KEY), raw if ns == deployment else None)
                for ns, token in self.tokens.items()}

    def restart(self):
        if self.server.poll() is not None:
            raise CheckError("cannot claim SIGKILL on an already exited server")
        self.server.kill()
        if self.server.wait(timeout=15) != -signal.SIGKILL:
            raise CheckError("server did not exit from the injected SIGKILL")
        self.kills.append(f"server-{self.server_number - 1}")
        self.start_server()

    def transfer(self, recorder, stream):
        namespace = stream.namespace
        old_client = recorder.clients[namespace]
        old_secret = self.secrets[namespace]
        pending = self.admin("POST", f"/admin/projects/{namespace[1]}/transfer", {"toWorkspace": "ws_life_a_new"})
        if pending.status != 200 or self.admin("POST", "/v1/token/streams", credential=old_secret).status != 403:
            raise CheckError("pending transfer did not stop credential exchange")
        complete = self.admin("POST", f"/admin/projects/{namespace[1]}/transfer/complete")
        recorder.record("ownership_transfer", {"namespace": list(namespace), "old_workspace": "ws_life_a",
                                                "new_workspace": "ws_life_a_new", "old_version": 1,
                                                "response": pack(complete)})
        self.issue(namespace)
        recorder.clients = self.clients()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            old = old_client.request("GET", stream_path(stream))
            new = recorder.clients[namespace].request("GET", stream_path(stream))
            if old.status in {401, 403} and new.status == 200:
                recorder.record("authorization_probe", {"namespace": list(namespace), "purpose": "retired-owner",
                                                        "response": pack(old)})
                return
            time.sleep(0.1)
        raise CheckError("ownership transfer did not converge to old-denied/new-authorized")

    def close(self):
        for _, child in reversed(self.children):
            if child.poll() is None:
                child.kill()
                child.wait(timeout=15)


def workload(args, fixture):
    rng = random.Random(args.seed)
    generations = {}
    def fresh(namespace, name):
        key = (*namespace, name)
        generations[key] = generations.get(key, 0) + 1
        return Stream(*namespace, name, f"seed-{args.seed}-generation-{generations[key]}")
    payload = bytes(range(128)) + b"identical application bytes in both namespaces"
    namespaces = [(tenant, project) for tenant, project, _ in DOMAINS]
    journal_path = args.out / "lifecycle.jsonl"
    with Journal(journal_path, {"history_kind": "lifecycle-v1", "seed": args.seed,
                                "namespaces": [list(ns) for ns in namespaces], "cycles": args.cycles}) as journal:
        recorder = LifecycleRecorder(journal, fixture.clients())
        shared, sources = {}, {}
        for namespace in namespaces:
            for views, name in ((shared, "same-name"), (sources, "fork-source")):
                stream = fresh(namespace, name)
                views[namespace] = stream
                recorder.operation("create", stream)
            for sequence in range(2):
                for key in ("alpha", "beta"):
                    recorder.append(shared[namespace], key, sequence, payload)
            for sequence in range(4):
                recorder.append(sources[namespace], "", sequence, payload)
        recorder.checkpoint("initial-authenticated-same-name-data")
        raw_path = "/v1/stream/" + sources[namespaces[0]].name
        customer_raw = recorder.clients[namespaces[0]].product.request("GET", raw_path)
        recorder.record("authorization_probe", {"namespace": list(namespaces[0]), "purpose": "customer-raw",
                                                "path": raw_path, "response": pack(customer_raw)})
        for cycle in range(args.cycles):
            order = list(namespaces)
            rng.shuffle(order)
            for namespace in order:
                old = shared[namespace]
                recorder.cursor_probe(old)
                recorder.operation("delete", old)
                recorder.checkpoint(f"deleted-{cycle}-{namespace[0]}")
                current = fresh(namespace, old.name)
                recorder.operation("create", current)
                shared[namespace] = current
                for sequence in range(2):
                    keys = ["alpha", "beta"]
                    rng.shuffle(keys)
                    for key in keys:
                        recorder.append(current, key, sequence, payload)
                recorder.cursor_probe(old, current)
                recorder.checkpoint(f"recreated-{cycle}-{namespace[0]}")
        namespace = namespaces[0]
        source = sources[namespace]
        boundary = recorder.fork_boundary(source)
        child = fresh(namespace, "fork-child")
        fork = {"source": dataclasses.asdict(source), "boundary": boundary}
        recorder.operation("fork", child, **fork)
        recorder.append(source, "", 4, b"parent record after the fork boundary")
        recorder.append(child, "", 0, b"child record; same producer sequence resets")
        recorder.checkpoint("fork-prefix-and-independent-tail")
        recorder.operation("delete", source)
        recorder.checkpoint("retained-fork-after-parent-delete")
        recorder.operation("create", fresh(namespace, source.name))
        recorder.operation("fork", child, **fork)
        recorder.checkpoint("pinned-parent-refusal-and-fork-retry")
        fixture.restart()
        recorder.clients = fixture.clients()
        recorder.checkpoint("cold-fork-after-sigkill")
        recorder.operation("delete", child)
        recorder.checkpoint("last-fork-deleted")
        replacement = fresh(namespace, source.name)
        recorder.operation("create", replacement)
        recorder.append(replacement, "", 0, b"parent name reused after final fork release")
        recorder.checkpoint("parent-recreated-after-release")
        fixture.transfer(recorder, shared[namespace])
        recorder.append(shared[namespace], "alpha", 2, payload)
        recorder.checkpoint("new-owner-progress-other-tenant-unchanged")
        fixture.restart()
        recorder.clients = fixture.clients()
        recorder.checkpoint("final-cold-all-live-and-deleted-identities")
        recorder.model.finish()
    report = load_lifecycle(journal_path)
    required = {"same-name-recreated", "stale-cursor-refused", "fork-created", "fork-nonempty-prefix",
                "parent-deleted-with-live-fork", "pinned-parent-recreation-refused",
                "fork-retry-after-parent-deletion", "ownership-transferred", "retired-owner-refused",
                "customer-raw-refused"}
    if (not required <= set(report["witnesses"]) or len(fixture.kills) != 2
            or report["acknowledged_operations"] != 20 + 8 * args.cycles
            or report["created_incarnations"] != 6 + 2 * args.cycles
            or report["live_streams"] != 4 or report["checkpoints"] != 9 + 4 * args.cycles):
        raise CheckError("campaign missed a required lifecycle mechanism")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--server", type=Path, default=ROOT / "target/release/streams-slate")
    parser.add_argument("--s3lite", type=Path, default=ROOT / "target/release/s3lite")
    parser.add_argument("--node", default="node")
    args = parser.parse_args()
    if not 1 <= args.cycles <= 20:
        parser.error("--cycles must be between 1 and 20")
    args.server, args.s3lite = args.server.resolve(strict=True), args.s3lite.resolve(strict=True)
    args.node = shutil.which(args.node)
    if not args.node:
        parser.error("Node.js is required for the existing platform emulator")
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False, mode=0o700)
    sync_directory(args.out.parent)
    sources = ["bench/reliability/independent.py", "bench/reliability/lifecycle.py",
               "bench/reliability/lifecycle_campaign.py", "bench/reliability/local_store.py",
               "platform-demo/src/emulator.mjs", "platform-demo/src/validate.mjs", "quality-tools.toml"]
    sources += [str(path.relative_to(ROOT)) for path in sorted((ROOT / "contracts/streams-platform/v1").glob("*.schema.json"))]
    hashes = {}
    for name in sources:
        data = (ROOT / name).read_bytes()
        hashes[name] = hashlib.sha256(data).hexdigest()
        destination = args.out / "source" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        durable_write(destination, data)
    fixture = Fixture(args)
    try:
        fixture.boot()
        report = workload(args, fixture)
        if any(hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != digest for name, digest in hashes.items()):
            raise CheckError("fixture source changed during the run")
        fixture.verify_binaries()
        report.update(status="passed", seed=args.seed, cycles=args.cycles,
                      scope="one shared release cell/store; two enforced JWT projects/workspaces; serialized lifecycle; quiescent checkpoints",
                      auth="enforce; customer JWT product surface; operation-scoped workload raw fork surface; release posture; same encryption key across projects",
                      fork_scope="internal raw default-key fork on deployment projectA; product API does not expose fork creation",
                      workload_scopes=["telemetry-append", "segment-read", "raw-read", "raw-lifecycle"],
                      node=subprocess.check_output([args.node, "--version"], text=True).strip(),
                      git_revision=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                      source_sha256=hashes, sigkill_witnesses=fixture.kills,
                      binary_sha256={Path(path).name: digest for path, digest in fixture.binary_hashes.items()})
        durable_write(args.out / "receipt.json", json.dumps(report, indent=2).encode())
        print(json.dumps(report, indent=2))
    finally:
        fixture.close()


if __name__ == "__main__":
    main()
