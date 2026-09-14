#!/usr/bin/env python3
"""Exercise the provider verifier against a real emulator and broken wire contracts."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import threading
import urllib.parse
import xml.etree.ElementTree as ET

from local_campaign import Processes, start_store
from local_store import durable_write


class FaultProxy:
    """Loopback-only response/conditional-header faults with engagement witnesses."""

    def __init__(self, upstream, fault):
        self.target = urllib.parse.urlsplit(upstream)
        if self.target.hostname != "127.0.0.1":
            raise ValueError("fault proxy requires its owned loopback emulator")
        self.fault = fault
        self.witnesses = 0
        self.lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def exchange(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                headers = dict(self.headers)
                path = urllib.parse.urlsplit(self.path).path
                changed = False
                field = {"ignore-create": "if-none-match", "ignore-update": "if-match"}.get(owner.fault)
                if field and self.command == "PUT":
                    for name in list(headers):
                        if name.lower() == field:
                            del headers[name]
                            changed = True
                bulk_delete = owner.fault == "ignore-delete" and self.command == "POST" and "delete" in urllib.parse.urlsplit(self.path).query
                if bulk_delete:
                    keys = [x.text for x in ET.fromstring(body).iter() if x.tag.rsplit("}", 1)[-1] == "Key"]
                    if len(keys) != 1 or not keys[0].endswith("/visibility"):
                        raise RuntimeError("delete fault must target the verifier's single visibility object")
                    result = ET.Element("DeleteResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
                    ET.SubElement(ET.SubElement(result, "Deleted"), "Key").text = keys[0]
                    status, response_headers, data = 200, [("Content-Type", "application/xml")], ET.tostring(result)
                    changed = True
                elif owner.fault == "ignore-delete" and self.command == "DELETE" and path.endswith("/visibility"):
                    status, response_headers, data = 204, [], b""
                    changed = True
                else:
                    connection = http.client.HTTPConnection(owner.target.hostname, owner.target.port, timeout=20)
                    try:
                        connection.request(self.command, self.path, body, headers)
                        response = connection.getresponse()
                        status, response_headers, data = response.status, response.getheaders(), response.read()
                    finally:
                        connection.close()
                if owner.fault == "corrupt-get" and self.command == "GET" and path.endswith("/create") and status == 200:
                    data = b"X" + data[1:]
                    changed = True
                if owner.fault == "stale-range" and self.command == "GET" and path.endswith("/visibility") and status == 206:
                    data = b"generation-0"
                    changed = True
                if owner.fault == "hide-list" and self.command == "GET" and "list-type=2" in self.path and status == 200:
                    root = ET.fromstring(data)
                    for child in list(root):
                        if child.tag.rsplit("}", 1)[-1] == "Contents":
                            keys = [x.text for x in child if x.tag.rsplit("}", 1)[-1] == "Key"]
                            if any(key.endswith("/visibility") for key in keys if key):
                                root.remove(child)
                                changed = True
                    data = ET.tostring(root)
                if owner.fault == "duplicate-list" and self.command == "GET" and "list-type=2" in self.path and status == 200:
                    root = ET.fromstring(data)
                    for child in list(root):
                        if child.tag.rsplit("}", 1)[-1] == "Contents":
                            keys = [x.text for x in child if x.tag.rsplit("}", 1)[-1] == "Key"]
                            if any(key.endswith("/visibility") for key in keys if key):
                                duplicate = ET.fromstring(ET.tostring(child))
                                for field in duplicate:
                                    if field.tag.rsplit("}", 1)[-1] == "Size":
                                        field.text = str(int(field.text) + 1)
                                root.append(duplicate)
                                changed = True
                    data = ET.tostring(root)
                if owner.fault == "rejected-write-mutates" and self.command == "PUT" and body == b"stale" and status == 412:
                    headers = {k: v for k, v in headers.items() if k.lower() != "if-match"}
                    connection = http.client.HTTPConnection(owner.target.hostname, owner.target.port, timeout=20)
                    try:
                        connection.request("PUT", self.path, body, headers)
                        mutation = connection.getresponse()
                        mutation.read()
                        if mutation.status != 200:
                            raise RuntimeError("rejected-write mutation did not persist its bytes")
                    finally:
                        connection.close()
                    changed = True
                if changed:
                    with owner.lock:
                        owner.witnesses += 1
                self.send_response(status)
                for name, value in response_headers:
                    if name.lower() not in {"content-length", "transfer-encoding", "connection", "server", "date"}:
                        self.send_header(name, value)
                length = next((v for k, v in response_headers if k.lower() == "content-length"), "0") if self.command == "HEAD" else str(len(data))
                self.send_header("Content-Length", length)
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(data)

            do_GET = do_HEAD = do_PUT = do_DELETE = do_POST = exchange

            def log_message(self, *_):
                pass

        class Server(ThreadingHTTPServer):
            # Conditional races deliberately exceed the stdlib server's
            # five-connection backlog. Do not turn rig congestion into a
            # provider outcome, and retain correctly framed pooled connections.
            request_queue_size = 64

        self.server = Server(("127.0.0.1", 0), Handler)

    def __enter__(self):
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.worker = self.executor.submit(self.server.serve_forever, poll_interval=0.02)
        return self

    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()
        self.worker.result(timeout=10)
        self.executor.shutdown()

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.server.server_port}"


def invoke(binary, endpoint, output, test, expected_error=None):
    env = {k: os.environ[k] for k in ("PATH", "HOME", "TMPDIR") if k in os.environ}
    env.update(AWS_ACCESS_KEY_ID="fixture", AWS_SECRET_ACCESS_KEY="fixture", AWS_EC2_METADATA_DISABLED="true")
    command = [str(binary), test, "--s3-endpoint", endpoint, "--bucket", "provider-contract", "--region", "auto", "--prefix", "reliability", "--n", "8"]
    result = subprocess.run(command, env=env, capture_output=True, timeout=120)
    durable_write(output, result.stdout + result.stderr)
    text = (result.stdout + result.stderr).decode(errors="replace")
    if expected_error is None:
        marker = {"contract": "PROVIDER_CONTRACT_OK ", "cas": "CAS_OK ", "fence": "fencing: PASS", "waloff": "WALOFF_OK", "clone": "CLONE_OK"}[test]
        if result.returncode != 0 or marker not in text:
            raise RuntimeError(f"provider verifier baseline failed: {output}")
    elif result.returncode == 0 or expected_error not in text or "PROVIDER_CONTRACT_OK " in text:
        raise RuntimeError(f"provider fault was not rejected by its intended check: {output}")
    return {"command": command, "exit_code": result.returncode, "expected_error": expected_error,
            "log": str(output), "sha256": hashlib.sha256(output.read_bytes()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", type=Path, required=True)
    parser.add_argument("--s3lite", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.verify, args.s3lite = args.verify.resolve(strict=True), args.s3lite.resolve(strict=True)
    binaries = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (args.verify, args.s3lite)}
    sources = {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
               for name in ("provider_campaign.py", "local_campaign.py", "local_store.py")}
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=False)
    processes = Processes(args.out)
    results = []
    try:
        _, store = start_store(processes, args.s3lite, "provider-store")
        with FaultProxy(store, None) as proxy:
            for test in ("contract", "cas", "fence", "waloff", "clone"):
                results.append(invoke(args.verify, proxy.endpoint, args.out / f"{test}-baseline.log", test))
        faults = {"ignore-create": "multiple winners", "ignore-update": "multiple winners",
                  "corrupt-get": "persisted bytes other than its winner", "hide-list": "LIST omitted or duplicated",
                  "ignore-delete": "GET found successfully deleted", "rejected-write-mutates": "rejected stale update changed",
                  "stale-range": "range GET returned different bytes", "duplicate-list": "LIST omitted or duplicated"}
        for fault, error in faults.items():
            with FaultProxy(store, fault) as proxy:
                result = invoke(args.verify, proxy.endpoint, args.out / f"{fault}.log", "contract", error)
                if not proxy.witnesses:
                    raise RuntimeError(f"provider fault never engaged: {fault}")
                results.append(dict(result, fault=fault, witnesses=proxy.witnesses))
        with FaultProxy(store, "ignore-create") as proxy:
            result = invoke(args.verify, proxy.endpoint, args.out / "legacy-cas-false-green.log", "cas", "multiple winners")
            if not proxy.witnesses:
                raise RuntimeError("legacy CAS negative control never engaged")
            results.append(dict(result, fault="legacy-cas", witnesses=proxy.witnesses))
        if binaries != {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (args.verify, args.s3lite)}:
            raise RuntimeError("provider binaries changed during execution")
        for name, digest in sources.items():
            if hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest() != digest:
                raise RuntimeError("provider campaign source changed during execution")
        receipt = {"status": "passed", "scope": "owned local s3lite and wire faults; no live-provider claim", "results": results,
                   "binary_sha256": binaries, "source_sha256": sources}
        durable_write(args.out / "receipt.json", json.dumps(receipt, indent=2).encode())
        print(f"PROVIDER_CHECKER_OK: {args.out / 'receipt.json'}")
    finally:
        processes.close()


if __name__ == "__main__":
    main()
