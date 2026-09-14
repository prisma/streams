#!/usr/bin/env python3
"""Actual-binary, writer-paused version transitions on isolated local storage."""
import argparse
import hashlib
import json
from pathlib import Path
import time

from independent import HttpClient, Journal, Recorder, Stream, check, load_journal
from local_campaign import KEY, TOKEN, Processes, free_port, start_store
import local_store
from version_build import read_builds


def compression_witnesses(uncompressed, compressed):
    if (uncompressed["versions"] != compressed["versions"]
            or len(uncompressed["phases"]) != len(compressed["phases"])):
        raise ValueError("compression campaigns do not exercise the same versions")
    witnesses = []
    for number, (plain, packed) in enumerate(zip(uncompressed["phases"], compressed["phases"])):
        if "new_acknowledged_operations" not in plain:
            if "new_acknowledged_operations" in packed:
                raise ValueError("compression campaigns disagree on writing phases")
            continue
        if (plain["label"] != packed["label"]
                or plain["new_acknowledged_operations"] != packed.get("new_acknowledged_operations")
                or plain["new_acknowledged_operations"] != 8):
            raise ValueError("compression campaigns differ in writer identity or workload")
        before = plain["absorption_witness"]["ingest_frame_bytes_total"]
        after = packed["absorption_witness"]["ingest_frame_bytes_total"]
        if not 0 < after < before:
            raise RuntimeError(f"compression did not reduce encoded ingest in phase {number} ({plain['label']})")
        witnesses.append({"phase": number, "label": plain["label"],
                          "uncompressed_frame_bytes": before, "compressed_frame_bytes": after})
    if not witnesses or not any(witness["label"] == "current" for witness in witnesses):
        raise ValueError("compression evidence requires current-writer activity")
    return witnesses


def start_version(processes, binary, name, store, compress):
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    # Processes.start retains its minimal environment; /usr/bin/env adds only
    # the existing production compression setting for this fixture process.
    command = ["/usr/bin/env", f"FRAME_COMPRESS={int(compress)}", str(binary),
               "--listen", f"127.0.0.1:{port}", "--s3-endpoint", store,
               "--bucket", "reliability", "--auth-token", TOKEN,
               "--account-id", "acct_local", "--project-id", "proj_local",
               "--initial-shards", "1", "--absorb-bytes", "1", "--absorb-age-secs", "1"]
    return processes.start(name, command, base + "/health"), base


def verify_snapshot(journal_path, directory, base):
    snapshot = directory / "operations.jsonl"
    directory.mkdir()
    local_store.durable_write(snapshot, journal_path.read_bytes())
    local_store.durable_write(snapshot.with_name(snapshot.name + ".head"),
                              journal_path.with_name(journal_path.name + ".head").read_bytes())
    history = load_journal(snapshot)
    if not history.streams or len(history.operations) < 8:
        raise RuntimeError("version fixture has no meaningful acknowledged data")
    client = HttpClient(base, TOKEN, KEY)
    return [check(history, {("acct_local", "proj_local"): client}, page_bytes=budget)
            for budget in (1024, 4096, 65536)]


def exercise(directory, builds, labels, compress):
    directory.mkdir()
    processes = Processes(directory)
    journal_path = directory / "operations.jsonl"
    phases, latest = [], {}
    streams = [Stream("acct_local", "proj_local", f"transition-{i}", f"incarnation-{i}")
               for i in range(2)]
    try:
        store_binary = builds["current"]["artifacts"]["s3lite"]["path"]
        store_process, store = start_store(processes, Path(store_binary), "store")
        with Journal(journal_path, {"campaign": "writer-paused-version-transition",
                                    "versions": labels, "compression": compress}) as journal:
            for number, label in enumerate(labels):
                binary = Path(builds[label]["artifacts"]["streams-slate"]["path"])
                name = f"server-{number}-{label}"
                server, base = start_version(processes, binary, name, store, compress)
                recorder = Recorder(journal, HttpClient(base, TOKEN, KEY))
                phase = {"label": label, "revision": builds[label]["revision"],
                         "binary_sha256": builds[label]["artifacts"]["streams-slate"]["sha256"]}
                if number == 0:
                    for stream in streams:
                        recorder.create(stream)
                else:
                    # Verify retained records before this process can retry or
                    # append anything. A retry cannot repair an omitted write.
                    phase["cold_checks"] = verify_snapshot(
                        journal_path, directory / f"cold-{number}", base)
                if number < len(labels) - 1:
                    for stream in streams:
                        for key in ("alpha", "beta"):
                            previous = latest.get((stream.name, key))
                            start = 0
                            if previous is not None:
                                seq, payload, operation = previous
                                _, verdict = recorder.append(stream, key, "producer-" + key,
                                                             seq, payload, op_id=operation)
                                if verdict != "acknowledged":
                                    raise RuntimeError(f"retained producer retry failed: {verdict}")
                                phase["retained_retries"] = phase.get("retained_retries", 0) + 1
                                start = seq + 1
                            for seq in range(start, start + 2):
                                payload = (b"version transition payload " * 12
                                           + f"{number}:{stream.name}:{key}:{seq}".encode())
                                operation, verdict = recorder.append(
                                    stream, key, "producer-" + key, seq, payload)
                                if verdict != "acknowledged":
                                    raise RuntimeError(f"version append failed: {verdict}")
                                latest[(stream.name, key)] = (seq, payload, operation)
                    phase["new_acknowledged_operations"] = 8
                    # This existing diagnostic is an intervention witness, not
                    # the correctness oracle. Require actual recorded absorption
                    # of the new writes; an empty history WAL alone proves none.
                    deadline = time.monotonic() + 30
                    while True:
                        response = recorder.client.request("GET", "/v1/debug/load")
                        if response.status != 200:
                            raise RuntimeError("cannot observe actual absorption progress")
                        maintenance = json.loads(response.body)["maintenance_shards"]
                        ingested = maintenance["ingest_frame_bytes_total"]
                        if (ingested > 0 and maintenance["absorbed_frame_bytes_total"] >= ingested
                                and maintenance["shards"]
                                and all(shard["unabsorbed_frame_bytes"] == 0
                                        for shard in maintenance["shards"])):
                            phase["absorption_witness"] = maintenance
                            break
                        if time.monotonic() >= deadline:
                            raise RuntimeError("new writes never reached observed absorption")
                        time.sleep(0.05)
                    objects = local_store.objects(store, "reliability")
                    phase["history_data_objects"] = [
                        key for key, (_, size) in objects.items()
                        if "/history2/" in key and key.endswith(".sst") and size > 0]
                    if not phase["history_data_objects"]:
                        raise RuntimeError("absorption has no physical history object witness")
                processes.kill(server)
                phase["stopped_before_next_version"] = True
                phases.append(phase)
        # Capture exact quiescent names/bytes after all server processes stopped.
        backup = local_store.capture(store, "reliability", directory / "final-objects")
        processes.kill(store_process)
        return {"versions": labels, "compression": compress, "phases": phases,
                "sigkill_witnesses": processes.kills, "final_objects": backup}
    finally:
        processes.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-receipt", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True, help="new evidence directory")
    args = parser.parse_args()
    args.build_receipt, args.out = args.build_receipt.resolve(), args.out.resolve()
    fixture_sources = {name: Path(__file__).with_name(name).read_bytes()
                       for name in ("version_build.py", "version_transition.py", "independent.py",
                                    "local_campaign.py", "local_store.py")}
    source_hashes = {name: hashlib.sha256(data).hexdigest() for name, data in fixture_sources.items()}
    build_receipt = read_builds(args.build_receipt)
    args.out.mkdir(parents=True, exist_ok=False)
    source_directory = args.out / "fixture-source"
    source_directory.mkdir()
    for name, data in fixture_sources.items():
        local_store.durable_write(source_directory / name, data)
    local_store.durable_write(args.out / "build-receipt.json", args.build_receipt.read_bytes())
    plans = {"released-forward": ["release", "release", "current", "current"],
             "same-format-roundtrip": ["prior", "prior", "current", "prior", "current"]}
    results, compression_checks = [], {}
    for name, labels in plans.items():
        for compress in (False, True):
            print(f"Running {name}, FRAME_COMPRESS={int(compress)}", flush=True)
            result = exercise(args.out / f"{name}-compress-{int(compress)}",
                              build_receipt["builds"], labels, compress)
            results.append({"name": name, **result})
        compression_checks[name] = compression_witnesses(*results[-2:])
    # The file hashes are checked again after execution so a replaced executable
    # cannot inherit the initial receipt's provenance.
    if read_builds(args.build_receipt) != build_receipt:
        raise RuntimeError("build provenance changed during the version matrix")
    for name, digest in source_hashes.items():
        if hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest() != digest:
            raise RuntimeError("fixture source changed during the version matrix")
    receipt = {"status": "passed", "scope": "local writer-paused cold-read frame/layout matrix",
               "provider": "s3lite", "plans": results,
               "compression_witnesses": compression_checks,
               "limitations": ["not rolling deployment or mixed-peer certification",
                               "rc.4 downgrade after current writes is prohibited and never attempted",
                               "prior revision retains known warm keyed-cache omission; no rollback certification",
                               "history object presence does not prove complete WAL reclamation",
                               "compression is configured through the actual writer; stored frame versions are source pins"],
               "source_sha256": source_hashes}
    local_store.durable_write(args.out / "receipt.json", json.dumps(receipt, indent=2).encode())
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
