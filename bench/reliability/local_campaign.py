#!/usr/bin/env python3
"""Release-binary SIGKILL, lost-response retry, and isolated restore drill."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request

import local_store

ROOT = Path(__file__).resolve().parents[2]
KEY = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc="
TOKEN = "local-reliability-fixture-token"


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class Processes:
    def __init__(self, output):
        self.output = output
        self.children = []
        self.kills = []

    def start(self, name, command, ready_path):
        log = (self.output / f"{name}.log").open("xb")
        # Only explicit fixture configuration may reach the release binary.
        env = {k: os.environ[k] for k in ("PATH", "HOME", "TMPDIR") if k in os.environ}
        env.update(RUST_LOG="warn", TOKIO_WORKERS="4")
        try:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=env)
        finally:
            log.close()
        self.children.append((name, process))
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"{name} exited during startup; see its log")
            try:
                with urllib.request.urlopen(ready_path, timeout=1) as response:
                    response.read()
                return process
            except (OSError, urllib.error.URLError):
                time.sleep(0.05)
        raise RuntimeError(f"{name} failed startup deadline")

    def kill(self, process):
        if process.poll() is not None:
            raise RuntimeError("cannot claim crash injection: child already exited")
        process.kill()
        code = process.wait(timeout=15)
        if code != -signal.SIGKILL:
            raise RuntimeError(f"expected SIGKILL exit, received {code}")
        name = next(name for name, child in self.children if child is process)
        self.kills.append(name)

    def close(self):
        for _, child in reversed(self.children):
            if child.poll() is None:
                child.kill()
                child.wait(timeout=15)


def start_store(processes, binary, name):
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    process = processes.start(name, [str(binary), "--listen", f"127.0.0.1:{port}",
                                     "--latency-ms", "2"], base + "/_s3lite/stats")
    return process, base


def start_server(processes, binary, name, store):
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    command = [str(binary), "--listen", f"127.0.0.1:{port}", "--s3-endpoint", store,
               "--bucket", "reliability", "--auth-token", TOKEN,
               "--account-id", "acct_local", "--project-id", "proj_local",
               "--initial-shards", "1", "--absorb-bytes", "1", "--absorb-age-secs", "1"]
    process = processes.start(name, command, base + "/health")
    return process, base


def lose_append_response(base, call):
    """A real TCP disconnect after the upstream accepted the POST body."""
    witnessed = []
    target = urllib.parse.urlsplit(base)

    class DiscardResponse(BaseHTTPRequestHandler):
        def do_POST(self):
            data = self.rfile.read(int(self.headers["Content-Length"]))
            connection = http.client.HTTPConnection(target.hostname, target.port, timeout=15)
            try:
                connection.request("POST", self.path, data, dict(self.headers))
                response = connection.getresponse()
                response.read()
                witnessed.append(response.status)
                # Deliberately send no HTTP response to the actual client.
                self.close_connection = True
            finally:
                connection.close()

        def log_message(self, *_):
            pass

    with HTTPServer(("127.0.0.1", 0), DiscardResponse) as proxy:
        proxy.timeout = 20
        with ThreadPoolExecutor(max_workers=1) as executor:
            worker = executor.submit(proxy.handle_request)
            result = call(f"http://127.0.0.1:{proxy.server_port}")
            worker.result(timeout=25)
    if len(witnessed) != 1 or not 200 <= witnessed[0] < 300:
        raise RuntimeError(f"lost-response mechanism never committed: {witnessed}")
    return result


def campaign(args, processes):
    from independent import CheckError, HttpClient, Journal, Recorder, Stream, check, load_journal

    def client(base):
        return HttpClient(base, TOKEN, KEY)

    def verify(base):
        # The only writer is this thread and is quiescent here. Seal independent
        # copies of both journal files so the reader can hold its own read lock.
        snapshot = args.out / f"check-{len(checks)}.jsonl"
        local_store.durable_write(snapshot, journal_path.read_bytes())
        local_store.durable_write(snapshot.with_name(snapshot.name + ".head"),
                                  journal_path.with_name(journal_path.name + ".head").read_bytes())
        history = load_journal(snapshot)
        return [check(history, {("acct_local", "proj_local"): client(base)},
                      page_bytes=budget) for budget in (1024, 4096, 65536)]

    store_process, store = start_store(processes, args.s3lite, "primary-store")
    server, base = start_server(processes, args.server, "server-0", store)
    journal_path = args.out / "operations.jsonl"
    streams = [Stream("acct_local", "proj_local", f"confidence-{i}", f"incarnation-{i}")
               for i in range(2)]
    checks = []
    with Journal(journal_path, {"campaign": "local-sigkill-restore"}) as journal:
        recorder = Recorder(journal, client(base))
        for stream in streams:
            recorder.create(stream)
        def writer(stream, route, producer):
            for seq in range(args.records):
                payload = bytes(range(256)) + f"{stream.name}:{route}:{seq}".encode()
                _, verdict = recorder.append(stream, route, producer, seq, payload)
                if verdict != "acknowledged":
                    raise RuntimeError(f"baseline append was {verdict}")

        # Concurrent producers on each routing key exercise overlap without
        # requiring response-arrival order to be the serialization order.
        with ThreadPoolExecutor(max_workers=8) as pool:
            writers = [pool.submit(writer, stream, route, f"producer-{route}-{producer}")
                       for stream in streams for route in ("alpha", "beta") for producer in range(2)]
            for future in writers:
                future.result()
        stream = streams[0]
        logical_id = "lost-response-operation"
        payload = b"committed with the actual TCP response discarded"
        _, verdict = lose_append_response(base, lambda proxy: Recorder(journal, client(proxy)).append(
            stream, "alpha", "producer-alpha-0", args.records, payload, op_id=logical_id))
        if verdict != "ambiguous":
            raise RuntimeError(f"disconnected append incorrectly classified {verdict}")
        processes.kill(server)
        server, base = start_server(processes, args.server, "server-1", store)
        after_ambiguous = verify(base)
        # The client must retain an ambiguous outcome, but this rig separately
        # witnessed the upstream success before discarding its response. A lost
        # committed write must fail here, before retry could insert it anew.
        if any(result["ambiguous_observed"] != 1 or result["ambiguous_absent"] != 0
               for result in after_ambiguous):
            raise RuntimeError("known-committed response-loss target disappeared after crash")
        checks.append({"phase": "cold-after-ambiguous", "checks": after_ambiguous})
        recorder = Recorder(journal, client(base))
        _, verdict = recorder.append(stream, "alpha", "producer-alpha-0", args.records,
                                     payload, op_id=logical_id)
        if verdict != "acknowledged":
            raise RuntimeError(f"ambiguous retry failed to resolve: {verdict}")
        _, verdict = recorder.append(stream, "alpha", "producer-alpha-0", args.records + 1,
                                     b"progress after cold recovery")
        if verdict != "acknowledged":
            raise RuntimeError("server failed to resume progress after recovery")
        processes.kill(server)
        server, base = start_server(processes, args.server, "server-2", store)
        checks.append({"phase": "cold-after-retry", "checks": verify(base)})
        processes.kill(server)
    backup = local_store.capture(store, "reliability", args.out / "backup")
    processes.kill(store_process)
    # Primary server and primary object store are both gone before restore.
    _, restored_store = start_store(processes, args.s3lite, "restored-store")
    restored = local_store.restore(restored_store, "reliability", args.out / "backup")
    server, base = start_server(processes, args.server, "restored-server", restored_store)
    checks.append({"phase": "isolated-restore", "checks": verify(base)})
    processes.kill(server)
    # Executable data-loss control: remove the copied registry descriptors,
    # then cold-open. The expected streams come from receipts, so losing their
    # entire namespace must fail instead of shrinking server-side enumeration.
    registry_keys = [key for key in local_store.objects(restored_store, "reliability")
                     if key.startswith("registry/v4/") and "/streams/" in key]
    if not registry_keys:
        raise RuntimeError("registry-loss control found no physical descriptor objects")
    for key in registry_keys:
        url = restored_store + "/reliability/" + urllib.parse.quote(key, safe="/")
        with urllib.request.urlopen(urllib.request.Request(url, method="DELETE"), timeout=15) as response:
            response.read()
    remaining = local_store.objects(restored_store, "reliability")
    if any(key in remaining for key in registry_keys):
        raise RuntimeError("registry-loss control failed to delete descriptor objects")
    server, base = start_server(processes, args.server, "registry-loss-control", restored_store)
    missing = client(base).request("GET", "/v1/streams/confidence-0:scan")
    if missing.status != 404:
        raise RuntimeError("registry-loss control did not produce its intended missing-stream response")
    try:
        verify(base)
    except CheckError as error:
        loss_detection = {"deleted_descriptor_objects": len(registry_keys), "failure": str(error)}
    else:
        raise RuntimeError("external checker accepted a restored store with missing stream identities")
    processes.kill(server)
    pins = tomllib.loads((ROOT / "quality-tools.toml").read_text())
    return {"status": "passed", "scope": "local release binaries; s3lite; quiescent full copy",
            "git_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "binary_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                              for path in (args.server, args.s3lite)},
            "checker_sha256": {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                               for name in ("independent.py", "local_campaign.py", "local_store.py")},
            "rust": pins["rust"], "slatedb": pins["slatedb"], "sigkill_witnesses": processes.kills,
            "lost_response_witnesses": 1, "records_per_producer_key": args.records,
            "producers_per_stream_key": 2,
            "configuration": {"initial_shards": 1, "absorb_bytes": 1, "absorb_age_secs": 1,
                              "storage_latency_ms": 2, "tokio_workers": 4,
                              "page_budgets": [1024, 4096, 65536],
                              "auth_mode": "static fixture token", "process_environment": "explicit fixture only"},
            "backup": backup, "restored": restored, "checks": checks,
            "registry_loss_negative_control": loss_detection}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True, help="new evidence directory")
    parser.add_argument("--server", type=Path, default=ROOT / "target/release/streams-slate")
    parser.add_argument("--s3lite", type=Path, default=ROOT / "target/release/s3lite")
    parser.add_argument("--records", type=int, default=8)
    args = parser.parse_args()
    if args.records < 1:
        parser.error("--records must be positive")
    args.out = args.out.resolve()
    args.server, args.s3lite = args.server.resolve(strict=True), args.s3lite.resolve(strict=True)
    args.out.mkdir(parents=True, exist_ok=False)
    local_store.sync_directory(args.out.parent)
    source_diff = subprocess.check_output(["git", "diff", "--binary", "HEAD", "--"], cwd=ROOT)
    local_store.durable_write(args.out / "source.patch", source_diff)
    processes = Processes(args.out)
    try:
        report = campaign(args, processes)
        report["source_diff_sha256"] = hashlib.sha256(source_diff).hexdigest()
        report["working_tree_dirty"] = bool(source_diff)
        local_store.durable_write(args.out / "receipt.json", json.dumps(report, indent=2).encode())
        print(json.dumps(report, indent=2))
    finally:
        processes.close()


if __name__ == "__main__":
    main()
