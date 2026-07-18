#!/usr/bin/env python3
"""Hermetic positive and negative controls for judge-aws-release.py."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
JUDGE_PATH = ROOT / "scripts" / "judge-aws-release.py"
RELEASE_ID = "release-under-test"
OLD_RELEASE_ID = "previous-release"


def load_judge() -> Any:
    specification = importlib.util.spec_from_file_location("aws_release_judge", JUDGE_PATH)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


JUDGE = load_judge()


def encode(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def write_json(path: Path, value: dict[str, Any]) -> str:
    raw = encode(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def provider_evidence(provider_id: str) -> dict[str, Any]:
    return {
        "format_version": 1,
        "provider_id": provider_id,
        "run_id": f"{provider_id}-check",
        "create_ms": 1,
        "immediate_read_ms": 2,
        "conditional_update_ms": 3,
        "immediate_list_ms": 4,
        "multipart_ms": 5,
        "copy_ms": 6,
        "delete_ms": 7,
        "total_ms": 10,
        "multipart_bytes": 1024,
        "checks": sorted(JUDGE.PROVIDER_CHECKS),
    }


def at_rest(provider_id: str) -> dict[str, Any]:
    return {
        "format_version": 1,
        "release_id": RELEASE_ID,
        "provider_id": provider_id,
        "objects": 12,
        "bytes": 4096,
        "inventory_sha256": "1" * 64,
        "forbidden_labels": ["payload_sentinel", "printable_root_key", "raw_root_key"],
        "stable_inventory_verified": True,
        "status": "pass",
    }


def iam_evidence() -> dict[str, Any]:
    principals = ["registry", "cell-a", "cell-b"]
    return {
        "format_version": 1,
        "status": "pass",
        "release_id": RELEASE_ID,
        "provider_id": "primary-provider",
        "run_id": "iam-run",
        "checked_at_ms": 1,
        "principals": principals,
        "positive_checks": 21,
        "permission_denials": 42,
        "operations": sorted(JUDGE.IAM_OPERATIONS),
        "boundaries": [
            {"attacker": attacker, "target": target, "permission_denials": 7}
            for attacker in principals
            for target in principals
            if attacker != target
        ],
        "probes_cleaned": True,
    }


def failover_evidence() -> dict[str, Any]:
    return {
        "format_version": 1,
        "status": "pass",
        "release_id": RELEASE_ID,
        "primary_provider_id": "primary-provider",
        "recovery_provider_id": "recovery-provider",
        "failure_monotonic_ms": 1000,
        "recovered_monotonic_ms": 1020,
        "measured_rpo_ms": 10,
        "measured_rto_ms": 20,
        "rpo_budget_ms": 100,
        "rto_budget_ms": 100,
        "last_recovered_sequence": 10,
        "post_failover_write_verified": True,
        "primary_provider_conformance": provider_evidence("primary-provider"),
        "recovery_provider_conformance": provider_evidence("recovery-provider"),
        "restore": {"snapshot_id": "snapshot-1", "restored_objects": 12},
    }


def soak_evidence() -> dict[str, Any]:
    operation_series = {
        name: index for index, name in enumerate(sorted(JUDGE.OBJECT_STORE_SERIES))
    }
    checks = {
        name: {"passed": True, "observed": 1, "budget": 1}
        for name in JUDGE.SOAK_CHECKS
    }
    checks["backup_protection"]["observed"] = {
        "required": True,
        "configured_samples": 100,
        "within_budget": True,
    }
    return {
        "format_version": 2,
        "status": "pass",
        "release_id": RELEASE_ID,
        "target": {
            "label": "production-cell",
            "instance_class": "compute-large",
            "storage_provider": "primary-provider",
            "metrics_targets": 2,
        },
        "workload": {
            "idle_secs": 300,
            "duration_secs": 86_400,
            "short_run": False,
            "token_rotation_required": True,
            "noisy_neighbor_required": True,
        },
        "monitor": {
            "successful_entries": 100_000,
            "idle_rates": [
                {
                    "target_index": 0,
                    "instance_hash": "1" * 32,
                    "identity_stable": True,
                    "counters_monotonic": True,
                    "cpu_core_fraction": 0.01,
                    "object_store_ops_per_sec": 0.1,
                    "data_plane_requests_delta": 0,
                    "object_store_operations_by_class": operation_series,
                },
                {
                    "target_index": 1,
                    "instance_hash": "2" * 32,
                    "identity_stable": True,
                    "counters_monotonic": True,
                    "cpu_core_fraction": 0.01,
                    "object_store_ops_per_sec": 0.1,
                    "data_plane_requests_delta": 0,
                    "object_store_operations_by_class": operation_series,
                },
            ],
            "workload_store_counters": [
                {
                    "target_index": 0,
                    "instance_hash": "1" * 32,
                    "counter_monotonic": True,
                    "identity_continuous": True,
                    "object_store_operations_by_class": operation_series,
                },
                {
                    "target_index": 1,
                    "instance_hash": "2" * 32,
                    "counter_monotonic": True,
                    "identity_continuous": True,
                    "object_store_operations_by_class": operation_series,
                },
            ],
            "object_store_operations_by_class": operation_series,
            "object_store_ops_per_1000_entries": 4.0,
        },
        "checks": checks,
    }


def capability(phase: str, index: int) -> dict[str, Any]:
    releases = [OLD_RELEASE_ID, RELEASE_ID] if phase != "finalize" else [RELEASE_ID]
    return {
        "format_version": 1,
        "kind": "streams-mixed-version-capability-gate",
        "run_id": "production-canary",
        "phase": phase,
        "passed": True,
        "judged_at_ms": index + 1,
        "snapshot_sha256": [
            {"instance": "node-a", "sha256": "a" * 64},
            {"instance": "node-b", "sha256": "b" * 64},
        ],
        "expected": {
            "instances": ["node-a", "node-b"],
            "releases": releases,
            "history_writer": JUDGE.HISTORY_WRITERS[index],
            "backup_writer": JUDGE.BACKUP_WRITER,
        },
        "observed": {
            "ring_protocol": 1,
            "cell_move_protocol": 1,
            "backup_coordination_protocol": 2,
        },
    }


def semantic(
    phase: str, index: int, capability_sha256: str, previous_sha256: str
) -> dict[str, Any]:
    return {
        "format_version": 1,
        "kind": "streams-mixed-version-semantic-phase",
        "run_id": "production-canary",
        "phase": phase,
        "passed": True,
        "completed_at_ms": index + 1,
        "previous_state_sha256": "c" * 64,
        "previous_evidence_sha256": previous_sha256,
        "capability_sha256": capability_sha256,
        "marker_sha256": "d" * 64,
        "history_writer": JUDGE.HISTORY_WRITERS[index],
        "backup_writer": JUDGE.BACKUP_WRITER,
        "event_count": index + 1,
        "route_count": 2,
        "append_first_status": 200,
        "append_retry_status": 200,
        "all_reads_status": [200],
        "complete": phase == "finalize",
    }


def attestation(gate: str) -> dict[str, Any]:
    return {
        "format_version": 1,
        "kind": "prisma-streams-external-attestation",
        "gate": gate,
        "release_id": RELEASE_ID,
        "status": "pass",
        "environment": "production",
        "completed_at_ms": 1,
        "record_id": f"{gate}-record",
        "evidence_system": "protected-release-system",
        "report_sha256": "e" * 64,
        "checks": {name: True for name in JUDGE.ATTESTATION_CHECKS[gate]},
        "approvals": [
            {"subject": f"{gate}-owner", "role": "owner"},
            {"subject": f"{gate}-reviewer", "role": "independent-reviewer"},
        ],
    }


def build_packet(packet: Path) -> Path:
    artifact_dir = packet / "artifacts"
    values: dict[str, dict[str, Any]] = {
        "at_rest_primary": at_rest("primary-provider"),
        "at_rest_recovery": at_rest("recovery-provider"),
        "cell_iam": iam_evidence(),
        "provider_failover": failover_evidence(),
        "release_soak": soak_evidence(),
    }
    for gate in JUDGE.ATTESTATION_CHECKS:
        values[f"attestation_{gate}"] = attestation(gate)

    previous_semantic_digest = JUDGE.ZERO_DIGEST
    for index, phase in enumerate(JUDGE.PHASES):
        suffix = phase.replace("-", "_")
        capability_name = f"mixed_capability_{suffix}"
        semantic_name = f"mixed_semantic_{suffix}"
        capability_path = artifact_dir / f"{capability_name}.json"
        capability_digest = write_json(capability_path, capability(phase, index))
        values[semantic_name] = semantic(
            phase, index, capability_digest, previous_semantic_digest
        )
        semantic_path = artifact_dir / f"{semantic_name}.json"
        previous_semantic_digest = write_json(semantic_path, values[semantic_name])

    for name, value in values.items():
        path = artifact_dir / f"{name}.json"
        if not path.exists():
            write_json(path, value)

    references = {}
    for name in sorted(JUDGE.REQUIRED_ARTIFACTS):
        path = artifact_dir / f"{name}.json"
        references[name] = {
            "path": str(path.relative_to(packet)),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    manifest = packet / "manifest.json"
    write_json(
        manifest,
        {
            "format_version": 1,
            "kind": "prisma-streams-aws-release-manifest",
            "release_id": RELEASE_ID,
            "artifacts": references,
        },
    )
    return manifest


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_bytes())
    assert isinstance(value, dict)
    return value


def refresh_reference(manifest_path: Path, name: str) -> None:
    manifest = read_json(manifest_path)
    reference = manifest["artifacts"][name]
    artifact_path = manifest_path.parent / reference["path"]
    reference["sha256"] = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    write_json(manifest_path, manifest)


def run_gate(manifest: Path, output: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "python3",
            str(JUDGE_PATH),
            "--manifest",
            str(manifest),
            "--output",
            str(output),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def expect_failure(
    manifest: Path, output: Path, message: str, *, output_may_exist: bool = False
) -> None:
    result = run_gate(manifest, output)
    assert result.returncode == 1, result.stdout + result.stderr
    assert message in result.stderr, result.stderr
    if not output_may_exist:
        assert not output.exists(), "failed judgment created an evidence artifact"


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="streams-aws-release-gate-") as temporary:
        base = Path(temporary)

        valid_manifest = build_packet(base / "valid")
        valid_output = valid_manifest.parent / "verdict.json"
        valid = run_gate(valid_manifest, valid_output)
        assert valid.returncode == 0, valid.stdout + valid.stderr
        verdict = read_json(valid_output)
        assert verdict["passed"] is True
        assert verdict["release_id"] == RELEASE_ID
        assert len(verdict["artifacts"]) == len(JUDGE.REQUIRED_ARTIFACTS)
        expect_failure(
            valid_manifest,
            valid_output,
            "output already exists",
            output_may_exist=True,
        )

        missing_manifest = build_packet(base / "missing")
        missing = read_json(missing_manifest)
        del missing["artifacts"]["cell_iam"]
        write_json(missing_manifest, missing)
        expect_failure(missing_manifest, missing_manifest.parent / "verdict.json", "missing artifacts: cell_iam")

        tampered_manifest = build_packet(base / "tampered")
        tampered = read_json(tampered_manifest)
        tampered_path = tampered_manifest.parent / tampered["artifacts"]["at_rest_primary"]["path"]
        tampered_path.write_bytes(tampered_path.read_bytes() + b" ")
        expect_failure(tampered_manifest, tampered_manifest.parent / "verdict.json", "at_rest_primary digest mismatch")

        traversal_manifest = build_packet(base / "traversal")
        traversal = read_json(traversal_manifest)
        traversal["artifacts"]["cell_iam"]["path"] = "../cell-iam.json"
        write_json(traversal_manifest, traversal)
        expect_failure(
            traversal_manifest,
            traversal_manifest.parent / "verdict.json",
            "cell_iam path is not normalized",
        )

        mismatch_manifest = build_packet(base / "release-mismatch")
        mismatch = read_json(mismatch_manifest)
        mismatch_path = mismatch_manifest.parent / mismatch["artifacts"]["cell_iam"]["path"]
        mismatch_value = read_json(mismatch_path)
        mismatch_value["release_id"] = "another-release"
        write_json(mismatch_path, mismatch_value)
        refresh_reference(mismatch_manifest, "cell_iam")
        expect_failure(mismatch_manifest, mismatch_manifest.parent / "verdict.json", "cell_iam release mismatch")

        same_binary_manifest = build_packet(base / "same-binary")
        same_binary = read_json(same_binary_manifest)
        capability_path = (
            same_binary_manifest.parent
            / same_binary["artifacts"]["mixed_capability_read_first"]["path"]
        )
        capability_value = read_json(capability_path)
        capability_value["expected"]["releases"] = [RELEASE_ID]
        write_json(capability_path, capability_value)
        refresh_reference(same_binary_manifest, "mixed_capability_read_first")
        expect_failure(
            same_binary_manifest,
            same_binary_manifest.parent / "verdict.json",
            "does not prove a deployed old/new binary wave",
        )

    print("AWS release gate hermetic acceptance and negative controls passed")


if __name__ == "__main__":
    main()
