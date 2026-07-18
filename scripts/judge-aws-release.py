#!/usr/bin/env python3
"""Fail-closed judge for the complete Prisma Streams AWS-quality release packet.

The manifest and every referenced artifact are credential-free inputs produced
by separate drills. This judge verifies their digests, release association,
cross-artifact chains, and the mandatory production evidence set. Artifact
storage and approver identity remain responsibilities of the access-controlled
release system that invokes this program.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


MAX_MANIFEST_BYTES = 1024 * 1024
MAX_ARTIFACT_BYTES = 8 * 1024 * 1024
SAFE_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
HEX_256 = re.compile(r"^[0-9a-f]{64}$")
ZERO_DIGEST = "0" * 64

PHASES = ("read-first", "canary-flip", "rollback", "finalize")
HISTORY_WRITERS = (1, 2, 1, 2)
BACKUP_WRITER = 3
ATTESTATION_CHECKS = {
    "security_review": {
        "independent_reviewer",
        "envelope_review",
        "open_critical_zero",
        "open_high_zero",
    },
    "mixed_version_deployment": {
        "deployed_old_new_wave",
        "forced_absorption_each_phase",
        "dark_restore_each_phase",
        "at_rest_each_phase",
        "rollback",
    },
    "managed_cell_game_day": {
        "real_provider_iam",
        "target_scale",
        "registry_union_recovery",
        "retention_cleanup",
    },
    "audit_billing_independence": {
        "independent_audit_account",
        "legacy_backfill",
        "lifecycle_object_lock",
        "billing_reconciliation",
    },
    "notification_game_day": {
        "collector",
        "evaluator",
        "notification_route",
        "inhibition",
        "ownership_labels",
    },
}

MACHINE_ARTIFACTS = {
    "at_rest_primary",
    "at_rest_recovery",
    "cell_iam",
    "provider_failover",
    "release_soak",
    *{
        f"mixed_{kind}_{phase.replace('-', '_')}"
        for phase in PHASES
        for kind in ("capability", "semantic")
    },
}
ATTESTATION_ARTIFACTS = {f"attestation_{gate}" for gate in ATTESTATION_CHECKS}
REQUIRED_ARTIFACTS = MACHINE_ARTIFACTS | ATTESTATION_ARTIFACTS

PROVIDER_CHECKS = {
    "conditional_create",
    "strong_read_after_write",
    "range_get",
    "conditional_update_fencing",
    "strong_list_after_write",
    "ordered_exclusive_offset_list",
    "multipart_upload",
    "server_side_copy",
    "strong_delete_visibility",
}
IAM_OPERATIONS = {
    "get",
    "head",
    "list",
    "put",
    "multipart",
    "delete",
    "batch_delete",
}
OBJECT_STORE_SERIES = {
    f"{operation}:{storage_class}"
    for operation in ("put", "mpu", "get", "head", "delete", "list", "copy")
    for storage_class in ("wal", "manifest", "sst", "fleet", "other")
}
SOAK_CHECKS = {
    "idle_duration",
    "idle_monitor_coverage",
    "idle_readiness",
    "idle_component_health",
    "idle_counters",
    "idle_quiescent",
    "idle_cpu_core_fraction",
    "idle_object_store_ops_per_sec",
    "release_duration",
    "fleet_metrics_coverage",
    "bench_exit",
    "durable_offsets",
    "auth_token_rotation",
    "operator_token_rotation",
    "noisy_neighbor_isolation",
    "append_error_rate",
    "throughput",
    "ack_p50_ms",
    "ack_p99_ms",
    "ack_p999_ms",
    "workload_store_counter_continuity",
    "object_store_ops_per_1000_entries",
    "monitor_coverage",
    "readiness",
    "component_health",
    "rss_max_bytes",
    "rss_growth_bytes",
    "absorber_pending_max_bytes",
    "absorber_pending_end_bytes",
    "l0_ssts_max",
    "unflushed_wal_ssts_max",
    "fence_events",
    "backup_protection",
}


class GateError(Exception):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise GateError(message)


def safe_id(value: Any, label: str) -> str:
    require(
        isinstance(value, str) and SAFE_ID.fullmatch(value) is not None,
        f"{label} must be a 1..64 character safe identifier",
    )
    return value


def digest(value: Any, label: str) -> str:
    require(
        isinstance(value, str) and HEX_256.fullmatch(value) is not None,
        f"{label} must be a lowercase SHA-256 digest",
    )
    return value


def integer(value: Any, label: str, minimum: int = 0, maximum: int = 10**16) -> int:
    require(type(value) is int, f"{label} must be an integer")
    require(minimum <= value <= maximum, f"{label} is out of range")
    return value


def finite_number(value: Any, label: str, minimum: float = 0.0) -> float:
    require(type(value) in (int, float), f"{label} must be numeric")
    parsed = float(value)
    require(math.isfinite(parsed) and parsed >= minimum, f"{label} is invalid")
    return parsed


def object_value(value: Any, label: str) -> dict[str, Any]:
    require(isinstance(value, dict), f"{label} must be an object")
    return value


def unique_strings(
    value: Any, label: str, *, minimum: int = 1, maximum: int = 128
) -> list[str]:
    require(
        isinstance(value, list) and minimum <= len(value) <= maximum,
        f"{label} has an invalid length",
    )
    require(all(isinstance(item, str) for item in value), f"{label} must contain strings")
    require(len(set(value)) == len(value), f"{label} contains duplicates")
    return value


def read_regular(path: Path, maximum: int, label: str) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise GateError(f"cannot open {label}: {error.strerror}") from error
    try:
        before = os.fstat(descriptor)
        require(stat.S_ISREG(before.st_mode), f"{label} is not a regular file")
        require(0 < before.st_size <= maximum, f"{label} is empty or oversized")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        require(len(raw) <= maximum, f"{label} exceeds the size bound")
        require(
            (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns),
            f"{label} changed while it was read",
        )
        require(len(raw) == before.st_size, f"{label} was not read completely")
        return raw
    finally:
        os.close(descriptor)


def parse_json(raw: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise GateError(f"{label} is not valid JSON") from error
    return object_value(value, label)


def read_artifact(
    manifest_dir: Path, name: str, reference: Any
) -> tuple[dict[str, Any], str]:
    value = object_value(reference, f"artifact reference {name}")
    require(
        set(value) == {"path", "sha256"},
        f"artifact reference {name} must contain only path and sha256",
    )
    relative = value["path"]
    require(isinstance(relative, str) and relative, f"artifact {name} path is invalid")
    relative_path = Path(relative)
    require(not relative_path.is_absolute(), f"artifact {name} path must be relative")
    require(
        all(part not in ("", ".", "..") for part in relative_path.parts),
        f"artifact {name} path is not normalized",
    )
    base = manifest_dir.resolve()
    path = base / relative_path
    try:
        path.resolve(strict=True).relative_to(base)
    except (OSError, ValueError) as error:
        raise GateError(f"artifact {name} escapes or is absent from the packet") from error
    expected = digest(value["sha256"], f"artifact reference {name}.sha256")
    raw = read_regular(path, MAX_ARTIFACT_BYTES, f"artifact {name}")
    actual = hashlib.sha256(raw).hexdigest()
    require(actual == expected, f"artifact {name} digest mismatch")
    return parse_json(raw, f"artifact {name}"), actual


def validate_at_rest(value: dict[str, Any], label: str, release_id: str) -> str:
    require(value.get("format_version") == 1, f"{label} format is unsupported")
    require(value.get("status") == "pass", f"{label} did not pass")
    require(value.get("release_id") == release_id, f"{label} release mismatch")
    provider_id = safe_id(value.get("provider_id"), f"{label}.provider_id")
    integer(value.get("objects"), f"{label}.objects", minimum=1, maximum=10**9)
    integer(value.get("bytes"), f"{label}.bytes", minimum=1)
    digest(value.get("inventory_sha256"), f"{label}.inventory_sha256")
    labels = set(unique_strings(value.get("forbidden_labels"), f"{label}.forbidden_labels", maximum=64))
    for item in labels:
        safe_id(item, f"{label}.forbidden_labels item")
    require("printable_root_key" in labels, f"{label} did not inspect the printable root key")
    require("raw_root_key" in labels, f"{label} did not inspect the raw root key")
    require(any("payload" in item for item in labels), f"{label} did not inspect a payload sentinel")
    require(value.get("stable_inventory_verified") is True, f"{label} corpus was not stable")
    return provider_id


def validate_iam(value: dict[str, Any], release_id: str) -> None:
    label = "cell_iam"
    require(value.get("format_version") == 1, f"{label} format is unsupported")
    require(value.get("status") == "pass", f"{label} did not pass")
    require(value.get("release_id") == release_id, f"{label} release mismatch")
    safe_id(value.get("provider_id"), f"{label}.provider_id")
    safe_id(value.get("run_id"), f"{label}.run_id")
    integer(value.get("checked_at_ms"), f"{label}.checked_at_ms", minimum=1)
    principals = unique_strings(value.get("principals"), f"{label}.principals")
    require(set(principals) == {"registry", "cell-a", "cell-b"}, f"{label} principal set is incomplete")
    operations = unique_strings(value.get("operations"), f"{label}.operations")
    require(set(operations) == IAM_OPERATIONS, f"{label} operation set is incomplete")
    require(value.get("positive_checks") == 21, f"{label} must prove 21 owner operations")
    require(value.get("permission_denials") == 42, f"{label} must prove 42 denials")
    boundaries = value.get("boundaries")
    require(isinstance(boundaries, list) and len(boundaries) == 6, f"{label} boundaries are incomplete")
    observed: set[tuple[str, str]] = set()
    for index, entry in enumerate(boundaries):
        boundary = object_value(entry, f"{label}.boundaries[{index}]")
        attacker = boundary.get("attacker")
        target = boundary.get("target")
        require(attacker in principals and target in principals and attacker != target,
                f"{label} boundary identity is invalid")
        require(boundary.get("permission_denials") == 7,
                f"{label} boundary did not deny every operation")
        observed.add((attacker, target))
    expected = {(left, right) for left in principals for right in principals if left != right}
    require(observed == expected, f"{label} directed boundaries are incomplete")
    require(value.get("probes_cleaned") is True, f"{label} probes were not cleaned")


def validate_provider_conformance(value: Any, label: str, provider_id: str) -> None:
    evidence = object_value(value, label)
    require(evidence.get("format_version") == 1, f"{label} format is unsupported")
    require(evidence.get("provider_id") == provider_id, f"{label} provider mismatch")
    safe_id(evidence.get("run_id"), f"{label}.run_id")
    checks = unique_strings(evidence.get("checks"), f"{label}.checks")
    require(set(checks) == PROVIDER_CHECKS, f"{label} check set is incomplete")
    integer(evidence.get("multipart_bytes"), f"{label}.multipart_bytes", minimum=1)
    for field in (
        "create_ms",
        "immediate_read_ms",
        "conditional_update_ms",
        "immediate_list_ms",
        "multipart_ms",
        "copy_ms",
        "delete_ms",
        "total_ms",
    ):
        integer(evidence.get(field), f"{label}.{field}")


def validate_failover(
    value: dict[str, Any], release_id: str, primary_id: str, recovery_id: str
) -> None:
    label = "provider_failover"
    require(value.get("format_version") == 1, f"{label} format is unsupported")
    require(value.get("status") == "pass", f"{label} did not pass")
    require(value.get("release_id") == release_id, f"{label} release mismatch")
    require(value.get("primary_provider_id") == primary_id, f"{label} primary does not match at-rest evidence")
    require(value.get("recovery_provider_id") == recovery_id, f"{label} recovery does not match at-rest evidence")
    require(primary_id != recovery_id, f"{label} providers are not independent")
    failure = integer(value.get("failure_monotonic_ms"), f"{label}.failure_monotonic_ms", minimum=1)
    recovered = integer(value.get("recovered_monotonic_ms"), f"{label}.recovered_monotonic_ms", minimum=1)
    rpo = integer(value.get("measured_rpo_ms"), f"{label}.measured_rpo_ms")
    rto = integer(value.get("measured_rto_ms"), f"{label}.measured_rto_ms")
    rpo_budget = integer(value.get("rpo_budget_ms"), f"{label}.rpo_budget_ms", minimum=1)
    rto_budget = integer(value.get("rto_budget_ms"), f"{label}.rto_budget_ms", minimum=1)
    require(recovered >= failure and recovered - failure == rto, f"{label} RTO timestamps are inconsistent")
    require(rpo <= rpo_budget and rto <= rto_budget, f"{label} exceeded RPO/RTO budget")
    integer(value.get("last_recovered_sequence"), f"{label}.last_recovered_sequence", minimum=1)
    require(value.get("post_failover_write_verified") is True, f"{label} did not prove activated writes")
    validate_provider_conformance(value.get("primary_provider_conformance"), f"{label}.primary_conformance", primary_id)
    validate_provider_conformance(value.get("recovery_provider_conformance"), f"{label}.recovery_conformance", recovery_id)
    restore = object_value(value.get("restore"), f"{label}.restore")
    snapshot_id = restore.get("snapshot_id")
    require(isinstance(snapshot_id, str) and 1 <= len(snapshot_id) <= 256,
            f"{label} restore snapshot is invalid")
    integer(restore.get("restored_objects"), f"{label}.restore.restored_objects", minimum=1)


def validate_soak(value: dict[str, Any], release_id: str) -> None:
    label = "release_soak"
    require(value.get("format_version") == 2, f"{label} format is unsupported")
    require(value.get("status") == "pass", f"{label} did not pass")
    require(value.get("release_id") == release_id, f"{label} release mismatch")
    target = object_value(value.get("target"), f"{label}.target")
    metrics_targets = integer(target.get("metrics_targets"), f"{label}.target.metrics_targets", minimum=1, maximum=64)
    for field in ("label", "instance_class", "storage_provider"):
        safe_id(target.get(field), f"{label}.target.{field}")
    workload = object_value(value.get("workload"), f"{label}.workload")
    integer(workload.get("idle_secs"), f"{label}.workload.idle_secs", minimum=300)
    integer(workload.get("duration_secs"), f"{label}.workload.duration_secs", minimum=86_400)
    require(workload.get("short_run") is False, f"{label} is a short test run")
    require(workload.get("token_rotation_required") is True, f"{label} did not require token rotation")
    require(workload.get("noisy_neighbor_required") is True, f"{label} did not require noisy-neighbor isolation")
    checks = object_value(value.get("checks"), f"{label}.checks")
    require(SOAK_CHECKS <= set(checks), f"{label} check set is incomplete")
    require(len(checks) <= 128, f"{label} check set is oversized")
    for name, result in checks.items():
        safe_id(name, f"{label}.checks name")
        require(isinstance(result, dict) and result.get("passed") is True,
                f"{label} check {name} did not pass")
    backup = object_value(checks["backup_protection"].get("observed"), f"{label}.backup_protection.observed")
    require(backup.get("required") is True and backup.get("within_budget") is True,
            f"{label} did not require healthy recovery protection")
    integer(backup.get("configured_samples"), f"{label}.backup_protection.configured_samples", minimum=1)
    monitor = object_value(value.get("monitor"), f"{label}.monitor")
    integer(monitor.get("successful_entries"), f"{label}.monitor.successful_entries", minimum=1)
    idle_rates = monitor.get("idle_rates")
    require(isinstance(idle_rates, list) and len(idle_rates) == metrics_targets,
            f"{label} idle counters do not cover the fleet")
    idle_identities: set[str] = set()
    idle_indices: set[int] = set()
    for index, rate_value in enumerate(idle_rates):
        rate = object_value(rate_value, f"{label}.monitor.idle_rates[{index}]")
        target_index = integer(
            rate.get("target_index"),
            f"{label}.monitor.idle_rates[{index}].target_index",
            maximum=metrics_targets - 1,
        )
        idle_indices.add(target_index)
        instance_hash = rate.get("instance_hash")
        require(isinstance(instance_hash, str) and re.fullmatch(r"[0-9a-f]{32}", instance_hash),
                f"{label} idle instance identity is invalid")
        idle_identities.add(instance_hash)
        require(rate.get("identity_stable") is True and rate.get("counters_monotonic") is True,
                f"{label} idle counter continuity is invalid")
        finite_number(rate.get("cpu_core_fraction"), f"{label} idle CPU rate")
        finite_number(rate.get("object_store_ops_per_sec"), f"{label} idle store rate")
        require(rate.get("data_plane_requests_delta") == 0,
                f"{label} idle baseline contains data-plane traffic")
        idle_series = object_value(
            rate.get("object_store_operations_by_class"),
            f"{label}.monitor.idle_rates[{index}].series",
        )
        require(set(idle_series) == OBJECT_STORE_SERIES,
                f"{label} idle object-store operation matrix is incomplete")
        for series_value in idle_series.values():
            finite_number(series_value, f"{label} idle object-store counter delta")
    require(idle_indices == set(range(metrics_targets)), f"{label} idle target indices are incomplete")
    require(len(idle_identities) == metrics_targets, f"{label} idle targets are not distinct instances")
    counters = monitor.get("workload_store_counters")
    require(isinstance(counters, list) and len(counters) == metrics_targets,
            f"{label} store counters do not cover the fleet")
    identities: set[str] = set()
    target_indices: set[int] = set()
    for index, counter_value in enumerate(counters):
        counter = object_value(counter_value, f"{label}.monitor.workload_store_counters[{index}]")
        target_index = integer(
            counter.get("target_index"),
            f"{label}.monitor.workload_store_counters[{index}].target_index",
            maximum=metrics_targets - 1,
        )
        target_indices.add(target_index)
        instance_hash = counter.get("instance_hash")
        require(isinstance(instance_hash, str) and re.fullmatch(r"[0-9a-f]{32}", instance_hash),
                f"{label} instance identity is invalid")
        identities.add(instance_hash)
        require(counter.get("counter_monotonic") is True and counter.get("identity_continuous") is True,
                f"{label} store counter continuity is invalid")
        series = object_value(counter.get("object_store_operations_by_class"),
                              f"{label}.monitor.workload_store_counters[{index}].series")
        require(set(series) == OBJECT_STORE_SERIES,
                f"{label} object-store operation matrix is incomplete")
        for series_value in series.values():
            finite_number(series_value, f"{label} object-store counter delta")
    require(target_indices == set(range(metrics_targets)), f"{label} workload target indices are incomplete")
    require(len(identities) == metrics_targets, f"{label} metrics targets are not distinct instances")
    require(identities == idle_identities, f"{label} instance set changed between idle and load")
    top_series = object_value(monitor.get("object_store_operations_by_class"),
                              f"{label}.monitor.object_store_operations_by_class")
    require(set(top_series) == OBJECT_STORE_SERIES,
            f"{label} aggregate object-store matrix is incomplete")
    finite_number(monitor.get("object_store_ops_per_1000_entries"),
                  f"{label}.monitor.object_store_ops_per_1000_entries")


def validate_capability(
    value: dict[str, Any], release_id: str, run_id: str | None, phase: str, index: int
) -> tuple[str, set[str], list[str]]:
    label = f"mixed capability {phase}"
    require(value.get("format_version") == 1, f"{label} format is unsupported")
    require(value.get("kind") == "streams-mixed-version-capability-gate", f"{label} kind is invalid")
    require(value.get("passed") is True, f"{label} did not pass")
    integer(value.get("judged_at_ms"), f"{label}.judged_at_ms", minimum=1)
    observed_run_id = safe_id(value.get("run_id"), f"{label}.run_id")
    require(run_id is None or observed_run_id == run_id, f"{label} run mismatch")
    require(value.get("phase") == phase, f"{label} phase mismatch")
    expected = object_value(value.get("expected"), f"{label}.expected")
    instances = unique_strings(expected.get("instances"), f"{label}.expected.instances", minimum=2, maximum=64)
    for instance in instances:
        safe_id(instance, f"{label}.expected.instance")
    releases = set(unique_strings(expected.get("releases"), f"{label}.expected.releases", maximum=64))
    for release in releases:
        safe_id(release, f"{label}.expected.release")
    if index < len(PHASES) - 1:
        require(release_id in releases and len(releases) >= 2,
                f"{label} does not prove a deployed old/new binary wave")
    else:
        require(releases == {release_id}, f"{label} did not finalize onto the judged release")
    require(expected.get("history_writer") == HISTORY_WRITERS[index], f"{label} history writer is invalid")
    require(expected.get("backup_writer") == BACKUP_WRITER, f"{label} backup writer is invalid")
    snapshots = value.get("snapshot_sha256")
    require(isinstance(snapshots, list) and len(snapshots) == len(instances),
            f"{label} direct snapshot set is incomplete")
    snapshot_instances: set[str] = set()
    for snapshot_value in snapshots:
        snapshot = object_value(snapshot_value, f"{label}.snapshot_sha256 item")
        snapshot_instances.add(safe_id(snapshot.get("instance"), f"{label}.snapshot instance"))
        digest(snapshot.get("sha256"), f"{label}.snapshot sha256")
    require(snapshot_instances == set(instances), f"{label} snapshot instances do not match the fleet")
    observed = object_value(value.get("observed"), f"{label}.observed")
    for field in ("ring_protocol", "cell_move_protocol", "backup_coordination_protocol"):
        integer(observed.get(field), f"{label}.observed.{field}", minimum=1)
    require(observed.get("backup_coordination_protocol") >= 2,
            f"{label} backup coordination protocol is not production eligible")
    return observed_run_id, releases, instances


def validate_semantic(
    value: dict[str, Any], run_id: str, phase: str, index: int,
    capability_digest: str, previous_digest: str, instance_count: int,
) -> None:
    label = f"mixed semantic {phase}"
    require(value.get("format_version") == 1, f"{label} format is unsupported")
    require(value.get("kind") == "streams-mixed-version-semantic-phase", f"{label} kind is invalid")
    require(value.get("passed") is True, f"{label} did not pass")
    require(value.get("run_id") == run_id and value.get("phase") == phase,
            f"{label} belongs to another run or phase")
    require(value.get("capability_sha256") == capability_digest,
            f"{label} does not bind the capability verdict")
    require(value.get("previous_evidence_sha256") == previous_digest,
            f"{label} evidence chain is broken")
    digest(value.get("previous_state_sha256"), f"{label}.previous_state_sha256")
    digest(value.get("marker_sha256"), f"{label}.marker_sha256")
    require(value.get("history_writer") == HISTORY_WRITERS[index], f"{label} history writer is invalid")
    require(value.get("backup_writer") == BACKUP_WRITER, f"{label} backup writer is invalid")
    require(value.get("event_count") == index + 1, f"{label} event sequence is incomplete")
    integer(value.get("route_count"), f"{label}.route_count", minimum=instance_count, maximum=65)
    require(value.get("all_reads_status") == [200], f"{label} did not prove successful reads")
    require(value.get("complete") is (index == len(PHASES) - 1), f"{label} completion flag is invalid")
    integer(value.get("completed_at_ms"), f"{label}.completed_at_ms", minimum=1)
    for field in ("append_first_status", "append_retry_status"):
        status_value = value.get(field)
        require(status_value == "resumed" or
                (type(status_value) is int and 200 <= status_value < 300),
                f"{label}.{field} is invalid")


def validate_attestation(value: dict[str, Any], gate: str, release_id: str) -> None:
    label = f"attestation {gate}"
    require(value.get("format_version") == 1, f"{label} format is unsupported")
    require(value.get("kind") == "prisma-streams-external-attestation", f"{label} kind is invalid")
    require(value.get("gate") == gate, f"{label} gate mismatch")
    require(value.get("release_id") == release_id, f"{label} release mismatch")
    require(value.get("status") == "pass", f"{label} did not pass")
    require(value.get("environment") == "production", f"{label} is not production evidence")
    integer(value.get("completed_at_ms"), f"{label}.completed_at_ms", minimum=1)
    safe_id(value.get("record_id"), f"{label}.record_id")
    safe_id(value.get("evidence_system"), f"{label}.evidence_system")
    digest(value.get("report_sha256"), f"{label}.report_sha256")
    checks = object_value(value.get("checks"), f"{label}.checks")
    require(ATTESTATION_CHECKS[gate] <= set(checks), f"{label} check set is incomplete")
    require(len(checks) <= 64, f"{label} check set is oversized")
    for name, passed in checks.items():
        safe_id(name, f"{label}.checks name")
        require(passed is True, f"{label} check {name} did not pass")
    approvals = value.get("approvals")
    require(isinstance(approvals, list) and 2 <= len(approvals) <= 8,
            f"{label} requires bounded owner and independent-reviewer approvals")
    subjects: set[str] = set()
    roles: set[str] = set()
    for index, approval_value in enumerate(approvals):
        approval = object_value(approval_value, f"{label}.approvals[{index}]")
        require(set(approval) == {"subject", "role"}, f"{label} approval shape is invalid")
        subjects.add(safe_id(approval.get("subject"), f"{label}.approval subject"))
        roles.add(safe_id(approval.get("role"), f"{label}.approval role"))
    require(len(subjects) == len(approvals), f"{label} repeats an approver")
    require({"owner", "independent-reviewer"} <= roles,
            f"{label} lacks owner or independent-reviewer approval")


def judge(manifest_path: Path) -> tuple[dict[str, Any], bytes]:
    manifest_raw = read_regular(manifest_path, MAX_MANIFEST_BYTES, "release manifest")
    manifest = parse_json(manifest_raw, "release manifest")
    require(
        set(manifest) == {"format_version", "kind", "release_id", "artifacts"},
        "release manifest has unknown or missing top-level fields",
    )
    require(manifest.get("format_version") == 1, "release manifest format is unsupported")
    require(manifest.get("kind") == "prisma-streams-aws-release-manifest", "release manifest kind is invalid")
    release_id = safe_id(manifest.get("release_id"), "release manifest release_id")
    references = object_value(manifest.get("artifacts"), "release manifest artifacts")
    missing = sorted(REQUIRED_ARTIFACTS - set(references))
    unknown = sorted(set(references) - REQUIRED_ARTIFACTS)
    require(not missing, f"release manifest is missing artifacts: {', '.join(missing)}")
    require(not unknown, f"release manifest has unknown artifacts: {', '.join(unknown)}")

    artifacts: dict[str, dict[str, Any]] = {}
    digests: dict[str, str] = {}
    for name in sorted(REQUIRED_ARTIFACTS):
        artifacts[name], digests[name] = read_artifact(manifest_path.parent, name, references[name])

    primary_id = validate_at_rest(artifacts["at_rest_primary"], "at_rest_primary", release_id)
    recovery_id = validate_at_rest(artifacts["at_rest_recovery"], "at_rest_recovery", release_id)
    require(primary_id != recovery_id, "at-rest evidence does not cover independent providers")
    validate_iam(artifacts["cell_iam"], release_id)
    validate_failover(artifacts["provider_failover"], release_id, primary_id, recovery_id)
    validate_soak(artifacts["release_soak"], release_id)

    run_id: str | None = None
    previous_semantic_digest = ZERO_DIGEST
    old_releases: set[str] | None = None
    instances: list[str] | None = None
    for index, phase in enumerate(PHASES):
        suffix = phase.replace("-", "_")
        capability_name = f"mixed_capability_{suffix}"
        semantic_name = f"mixed_semantic_{suffix}"
        observed_run_id, releases, phase_instances = validate_capability(
            artifacts[capability_name], release_id, run_id, phase, index
        )
        if run_id is None:
            run_id = observed_run_id
            old_releases = releases - {release_id}
            instances = phase_instances
        else:
            require(phase_instances == instances, f"mixed capability {phase} changed the declared fleet")
            if index < len(PHASES) - 1:
                require(releases - {release_id} == old_releases,
                        f"mixed capability {phase} changed the old release wave")
        validate_semantic(
            artifacts[semantic_name], observed_run_id, phase, index,
            digests[capability_name], previous_semantic_digest, len(phase_instances),
        )
        previous_semantic_digest = digests[semantic_name]

    for gate in sorted(ATTESTATION_CHECKS):
        validate_attestation(artifacts[f"attestation_{gate}"], gate, release_id)

    evidence = {
        "format_version": 1,
        "kind": "prisma-streams-aws-release-gate",
        "release_id": release_id,
        "passed": True,
        "judged_at_ms": int(time.time() * 1000),
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "artifacts": [
            {"name": name, "sha256": digests[name]} for name in sorted(digests)
        ],
    }
    return evidence, manifest_raw


def write_new(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    require(not path.exists(), "release-gate output already exists")
    encoded = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        evidence, _ = judge(args.manifest)
        write_new(args.output, evidence)
    except (GateError, OSError) as error:
        print(f"AWS release gate failed: {error}", file=sys.stderr)
        return 1
    print(f"AWS release gate passed: {evidence['release_id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
