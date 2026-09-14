#!/usr/bin/env python3
"""Create an independent receipt workload, or check it against recovered servers."""

import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import sys
import uuid

from independent import (CheckError, HttpClient, Journal, Recorder, Stream,
                         check, load_journal, parse_json)


def configuration(path, timeout):
    namespaces = parse_json(Path(path).read_bytes())
    if not isinstance(namespaces, list) or not namespaces:
        raise CheckError("configuration must be a nonempty namespace array")
    clients = {}
    for namespace in namespaces:
        identity = namespace["tenant"], namespace["project"]
        if identity in clients:
            raise CheckError(f"duplicate namespace: {identity}")
        clients[identity] = HttpClient(namespace["base_url"],
                                       os.environ[namespace["token_env"]],
                                       os.environ[namespace["key_env"]], timeout)
    return namespaces, clients


def drive(args, namespaces, clients):
    prefix = "reliability-" + uuid.uuid4().hex
    source_digest = hashlib.sha256(Path(__file__).with_name("independent.py").read_bytes()).hexdigest()
    header = {"namespaces": [{k: ns[k] for k in ("tenant", "project", "base_url")}
                             for ns in namespaces],
              "workload": {key: getattr(args, key) for key in
                           ("streams", "keys", "writers", "records", "payload_bytes", "retries")},
              "checker_sha256": source_digest, "prefix": prefix}
    with Journal(args.journal, header) as journal:
        streams = [Stream(tenant, project, f"{prefix}/{index}", str(uuid.uuid4()))
                   for tenant, project in clients for index in range(args.streams)]
        for stream in streams:
            Recorder(journal, clients[stream.namespace]).create(stream)

        def writer(stream, key, producer):
            recorder = Recorder(journal, clients[stream.namespace])
            for seq in range(args.records):
                payload = hashlib.shake_256(f"record:{seq}".encode()).digest(args.payload_bytes)
                oid = str(uuid.uuid4())
                for _ in range(args.retries + 1):
                    _, verdict = recorder.append(stream, key, producer, seq, payload, op_id=oid)
                    if verdict != "ambiguous":
                        break
                if verdict != "acknowledged":
                    raise CheckError(f"writer stopped at {stream.name}/{key}/{producer}/{seq}: {verdict}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = [pool.submit(writer, stream, f"key-{key}", f"producer-{writer_id}")
                       for stream in streams for key in range(args.keys)
                       for writer_id in range(args.writers)]
            failures = []
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    failures.append(str(exc))
            if failures:
                raise CheckError("workload incomplete: " + "; ".join(failures[:5]))
    return check(load_journal(args.journal), clients, args.page_bytes, args.max_pages)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("drive", "check"))
    parser.add_argument("--config", required=True, help="namespace JSON; credentials referenced via env names")
    parser.add_argument("--journal", required=True, help="new journal for drive, existing journal for check")
    parser.add_argument("--report", help="optional fresh JSON report file")
    parser.add_argument("--timeout", type=float, default=15)
    parser.add_argument("--page-bytes", type=int, default=4096)
    parser.add_argument("--max-pages", type=int, default=10000)
    parser.add_argument("--streams", type=int, default=2)
    parser.add_argument("--keys", type=int, default=2)
    parser.add_argument("--writers", type=int, default=2)
    parser.add_argument("--records", type=int, default=20)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=8)
    args = parser.parse_args()
    if any(getattr(args, name) < 1 for name in ("streams", "keys", "writers", "records",
                                               "payload_bytes", "concurrency", "page_bytes", "max_pages")):
        parser.error("workload and pagination bounds must be positive")
    if args.retries < 0 or args.timeout <= 0:
        parser.error("retries must be nonnegative and timeout must be positive")
    try:
        namespaces, clients = configuration(args.config, args.timeout)
        report = (drive(args, namespaces, clients) if args.mode == "drive" else
                  check(load_journal(args.journal), clients, args.page_bytes, args.max_pages))
    except (CheckError, OSError, KeyError, TypeError) as exc:
        print(json.dumps({"result": "fail", "reason": str(exc)}))
        return 1
    text = json.dumps(report, sort_keys=True, indent=2) + "\n"
    if args.report:
        with Path(args.report).open("x") as output:
            output.write(text)
    print(text, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
