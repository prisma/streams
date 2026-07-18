#!/usr/bin/env python3
"""Judge direct /v1/debug/capabilities snapshots for one deployment phase.

The deploy system captures the operator-only endpoint from every instance in a
cell and passes those JSON files here. This program performs no network access
and never receives a credential, so its evidence artifact is safe to retain.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


MAX_SNAPSHOT_BYTES = 1024 * 1024
MAX_INSTANCES = 64
SAFE_ID = re.compile(r"^[A-Za-z0-9_.+-]{1,128}$")
SAFE_PHASE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
CAPABILITY_FIELDS = (
    "version",
    "release_id",
    "ring_protocol",
    "live_reader_min",
    "live_reader_max",
    "live_writer",
    "history_reader_min",
    "history_reader_max",
    "history_writer",
    "backup_reader_min",
    "backup_reader_max",
    "backup_writer",
    "backup_coordination_protocol",
)


class JudgeError(Exception):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise JudgeError(message)


def bounded_int(
    value: Any, label: str, minimum: int = 1, maximum: int = 1_000_000
) -> int:
    require(type(value) is int, f"{label} must be an integer")
    require(minimum <= value <= maximum, f"{label} is out of range")
    return value


def safe_id(value: Any, label: str) -> str:
    require(
        isinstance(value, str) and SAFE_ID.fullmatch(value) is not None,
        f"{label} is not a bounded safe identifier",
    )
    return value


def parse_capabilities(value: Any, label: str) -> dict[str, Any]:
    require(isinstance(value, dict), f"{label} must be an object")
    require(
        all(field in value for field in CAPABILITY_FIELDS),
        f"{label} is incomplete or legacy",
    )
    version = bounded_int(value["version"], f"{label}.version")
    require(version == 1, f"{label}.version is unsupported")
    parsed: dict[str, Any] = {
        "version": version,
        "release_id": safe_id(value["release_id"], f"{label}.release_id"),
    }
    for field in CAPABILITY_FIELDS[2:]:
        parsed[field] = bounded_int(value[field], f"{label}.{field}")
    for surface in ("live", "history", "backup"):
        minimum = parsed[f"{surface}_reader_min"]
        maximum = parsed[f"{surface}_reader_max"]
        writer = parsed[f"{surface}_writer"]
        require(minimum <= maximum, f"{label}.{surface} reader range is inverted")
        require(
            minimum <= writer <= maximum,
            f"{label}.{surface} cannot read its own writer format",
        )
    return parsed


def read_snapshot(path: Path) -> tuple[dict[str, Any], str]:
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise JudgeError(f"cannot read snapshot {path.name}: {error.strerror}") from error
    require(
        0 < len(raw) <= MAX_SNAPSHOT_BYTES,
        f"snapshot {path.name} is empty or exceeds the size bound",
    )
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise JudgeError(f"snapshot {path.name} is not valid JSON") from error
    require(isinstance(value, dict), f"snapshot {path.name} must be an object")
    return value, hashlib.sha256(raw).hexdigest()


def parse_node(value: Any, label: str, *, local: bool) -> dict[str, Any]:
    require(isinstance(value, dict), f"{label} must be an object")
    node = {
        "instance": safe_id(value.get("instance"), f"{label}.instance"),
        "cell_move_protocol": bounded_int(
            value.get("cell_move_protocol"), f"{label}.cell_move_protocol"
        ),
        "capabilities": parse_capabilities(
            value.get("capabilities"), f"{label}.capabilities"
        ),
    }
    if not local:
        require(value.get("draining") is False, f"{label} is draining")
        node["ts_ms"] = bounded_int(
            value.get("ts_ms"), f"{label}.ts_ms", maximum=10**16
        )
    return node


def judge(args: argparse.Namespace) -> dict[str, Any]:
    require(
        SAFE_PHASE.fullmatch(args.phase) is not None,
        "phase must be a bounded safe identifier",
    )
    safe_id(args.run_id, "run_id")
    expected_instances = set(args.expected_instance)
    expected_releases = set(args.expected_release)
    require(
        1 <= len(expected_instances) <= MAX_INSTANCES,
        "expected instance count is out of range",
    )
    require(
        len(expected_instances) == len(args.expected_instance),
        "expected instances contain duplicates",
    )
    require(
        expected_releases and len(expected_releases) == len(args.expected_release),
        "expected releases are empty or contain duplicates",
    )
    for value in expected_instances:
        safe_id(value, "expected instance")
    for value in expected_releases:
        safe_id(value, "expected release")
    require(
        len(args.snapshot) == len(expected_instances),
        "one direct snapshot per expected instance is required",
    )

    local_nodes: dict[str, dict[str, Any]] = {}
    aggregate_views: list[dict[str, dict[str, Any]]] = []
    digests: dict[str, str] = {}
    for index, path in enumerate(args.snapshot):
        snapshot, digest = read_snapshot(path)
        require(
            snapshot.get("format_version") == 1,
            f"snapshot {path.name} has an unsupported format",
        )
        require(
            snapshot.get("aggregate_ready") is True,
            f"snapshot {path.name} reports an unready aggregate",
        )
        observed_at_ms = bounded_int(
            snapshot.get("observed_at_ms"),
            f"snapshot {path.name}.observed_at_ms",
            maximum=10**16,
        )
        local_node = parse_node(
            snapshot.get("local"), f"snapshot[{index}].local", local=True
        )
        instance = local_node["instance"]
        require(instance not in local_nodes, "direct snapshots repeat a local instance")
        local_nodes[instance] = local_node
        digests[instance] = digest

        fleet = snapshot.get("fleet")
        require(
            isinstance(fleet, list) and 1 <= len(fleet) <= MAX_INSTANCES,
            f"snapshot {path.name} has an invalid fleet",
        )
        view: dict[str, dict[str, Any]] = {}
        for fleet_index, item in enumerate(fleet):
            node = parse_node(
                item, f"snapshot[{index}].fleet[{fleet_index}]", local=False
            )
            require(
                0 <= observed_at_ms - node["ts_ms"] <= 30_000,
                f"snapshot {path.name} contains a stale or future heartbeat",
            )
            require(
                node["instance"] not in view,
                f"snapshot {path.name} repeats a fleet instance",
            )
            view[node["instance"]] = node
        aggregate_views.append(view)

    require(
        set(local_nodes) == expected_instances,
        "direct snapshot instance set does not match the expected cell",
    )
    for view in aggregate_views:
        require(
            set(view) == expected_instances,
            "an aggregate view is incomplete or contains an unexpected instance",
        )
        for instance, local in local_nodes.items():
            observed = view[instance]
            require(
                observed["cell_move_protocol"] == local["cell_move_protocol"]
                and observed["capabilities"] == local["capabilities"],
                "aggregate and direct capability views disagree",
            )

    capabilities = [node["capabilities"] for node in local_nodes.values()]
    releases = {capability["release_id"] for capability in capabilities}
    require(
        releases == expected_releases,
        "observed release set does not exactly match the expected wave",
    )
    require(
        len({node["cell_move_protocol"] for node in local_nodes.values()}) == 1,
        "cell move protocol differs within the cell",
    )
    require(
        len({capability["ring_protocol"] for capability in capabilities}) == 1,
        "ring protocol differs within the cell",
    )
    require(
        len(
            {
                capability["backup_coordination_protocol"]
                for capability in capabilities
            }
        )
        == 1,
        "backup coordination protocol differs within the cell",
    )
    require(
        capabilities[0]["backup_coordination_protocol"] >= 2,
        "backup coordination protocol is not production eligible",
    )

    for surface in ("live", "history", "backup"):
        writers = {capability[f"{surface}_writer"] for capability in capabilities}
        for capability in capabilities:
            require(
                all(
                    capability[f"{surface}_reader_min"]
                    <= writer
                    <= capability[f"{surface}_reader_max"]
                    for writer in writers
                ),
                f"a {surface} reader cannot read every writer in the cell",
            )
    require(
        {capability["history_writer"] for capability in capabilities}
        == {args.expected_history_writer},
        "history writer format does not match the declared phase",
    )
    require(
        {capability["backup_writer"] for capability in capabilities}
        == {args.expected_backup_writer},
        "backup writer format does not match the declared phase",
    )

    return {
        "format_version": 1,
        "kind": "streams-mixed-version-capability-gate",
        "run_id": args.run_id,
        "phase": args.phase,
        "passed": True,
        "judged_at_ms": int(time.time() * 1000),
        "snapshot_sha256": [
            {"instance": instance, "sha256": digests[instance]}
            for instance in sorted(digests)
        ],
        "expected": {
            "instances": sorted(expected_instances),
            "releases": sorted(expected_releases),
            "history_writer": args.expected_history_writer,
            "backup_writer": args.expected_backup_writer,
        },
        "observed": {
            "ring_protocol": capabilities[0]["ring_protocol"],
            "cell_move_protocol": next(iter(local_nodes.values()))["cell_move_protocol"],
            "backup_coordination_protocol": capabilities[0]["backup_coordination_protocol"],
        },
    }


def write_new(path: Path, evidence: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    require(not path.exists(), "evidence output already exists")
    encoded = (json.dumps(evidence, indent=2, sort_keys=True) + "\n").encode()
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Judge converged direct fleet capability snapshots"
    )
    parser.add_argument("--phase", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--snapshot", action="append", type=Path, required=True)
    parser.add_argument("--expected-instance", action="append", required=True)
    parser.add_argument("--expected-release", action="append", required=True)
    parser.add_argument("--expected-history-writer", type=int, choices=(1, 2), required=True)
    parser.add_argument("--expected-backup-writer", type=int, choices=(2, 3), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        evidence = judge(args)
        write_new(args.output, evidence)
    except (JudgeError, OSError) as error:
        print(f"mixed-version capability gate failed: {error}", file=sys.stderr)
        return 1
    print(f"mixed-version capability gate passed: {args.phase}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
