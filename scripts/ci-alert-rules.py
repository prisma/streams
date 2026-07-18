#!/usr/bin/env python3
"""Fail-closed structural validation for the checked-in Prometheus rules."""

import json
import pathlib
import re
import sys


path = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "ops/prometheus-alerts.json")
document = json.loads(path.read_text())
groups = document.get("groups")
if not isinstance(groups, list) or not groups:
    raise SystemExit("alert policy must contain a non-empty groups array")

allowed_metrics = {
    "streams_admission_shed_total",
    "streams_audit_dropped_total",
    "streams_audit_mirror_configured",
    "streams_absorber_pending_bytes",
    "streams_backup_component_ready",
    "streams_backup_configured",
    "streams_backup_recovery_point_age_seconds",
    "streams_backup_rpo_budget_seconds",
    "streams_billing_dropped_series_total",
    "streams_billing_export_configured",
    "streams_billing_export_healthy",
    "streams_component_ready",
    "streams_fence_events_total",
    "streams_http_requests_total",
    "streams_process_resident_memory_bytes",
    "streams_shard_durable_wait_p99_seconds",
    "streams_shard_l0_ssts",
    "streams_shard_unflushed_wal_ssts",
    "streams_tail_freshness_seconds_bucket",
    "streams_wal_put_latency_seconds",
}
names = set()
count = 0
for group in groups:
    if set(group) != {"name", "rules"} or not isinstance(group["rules"], list):
        raise SystemExit("each alert group must contain only name and rules")
    for rule in group["rules"]:
        if set(rule) != {"alert", "expr", "for", "labels", "annotations"}:
            raise SystemExit("each alert rule must contain the exact checked schema")
        name = rule["alert"]
        if not re.fullmatch(r"Streams[A-Za-z0-9]+", name) or name in names:
            raise SystemExit(f"invalid or duplicate alert name: {name!r}")
        names.add(name)
        if not re.fullmatch(r"[0-9]+[smhd]", rule["for"]):
            raise SystemExit(f"invalid hold duration for {name}")
        if rule["labels"].get("severity") not in {"page", "ticket"}:
            raise SystemExit(f"invalid severity for {name}")
        annotations = rule["annotations"]
        if set(annotations) != {"summary", "blast_radius", "runbook"}:
            raise SystemExit(f"incomplete annotations for {name}")
        if not annotations["summary"] or "cell=" not in annotations["blast_radius"]:
            raise SystemExit(f"non-actionable annotation for {name}")
        if not annotations["runbook"].startswith("RUNBOOK.md#"):
            raise SystemExit(f"missing checked-in runbook target for {name}")
        referenced = set(re.findall(r"\bstreams_[a-z0-9_]+\b", rule["expr"]))
        unknown = referenced - allowed_metrics
        if not referenced or unknown:
            raise SystemExit(f"unknown metric(s) in {name}: {sorted(unknown)}")
        count += 1

required = {
    "StreamsTelemetryMissing",
    "StreamsComponentUnready",
    "StreamsBackupUnhealthy",
    "StreamsAuditDropped",
    "StreamsAuditMirrorMissing",
    "StreamsAppendAvailabilityFastBurn",
    "StreamsAbsorberLagHigh",
    "StreamsBillingExportMissing",
    "StreamsBillingExportUnhealthy",
    "StreamsBillingSeriesDropped",
    "StreamsCompactionDebtHigh",
    "StreamsDurableAckLatency",
    "StreamsFenceFlapping",
    "StreamsRecoveryPointTooOld",
    "StreamsTailFreshness",
    "StreamsWalReplayDebtHigh",
}
missing = required - names
if missing:
    raise SystemExit(f"mandatory alert(s) missing: {sorted(missing)}")
if count > 64:
    raise SystemExit("alert policy exceeds the bounded rule count")
print(f"validated {count} bounded actionable alert rules")
