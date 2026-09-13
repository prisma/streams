"""Canonical ownership table for diff-scoped and scheduled mutation checks.

The broad critical-prefix policy lives in :mod:`verification_plan`: it catches
new and moved critical source.  This exact table answers the separate question
of which package and tests own each selected source.  A critical path missing
from this table is an error; it never inherits a nearby owner's tests.
"""
from dataclasses import dataclass
import hashlib
from pathlib import Path


@dataclass(frozen=True)
class MutationOwner:
    name: str
    sources: tuple[str, ...]
    test_filters: tuple[str, ...]
    target: str = 'service-lib'

    @property
    def package(self) -> str:
        return 'streams-quality-invariants' if self.target == 'harness-lib' else 'streams-slate'


def owner(name: str, source: str, filters: str, target: str = 'service-lib') -> MutationOwner:
    return MutationOwner(name, (source,), tuple(filters.split()), target)


# One row owns path selection, cargo package/target selection, and test filters.
# Keep source paths literal and explicit: a new file under a critical prefix
# must receive a deliberate row instead of silently inheriting a broad glob.
OWNERS = (
    owner('postings_codec', 'src/postings.rs', 'postings::', 'harness-lib'),
    owner('postings_codec_tests', 'src/postings/codec_tests.rs', 'postings::', 'harness-lib'),
    owner('postings', 'src/postings/validated.rs', 'postings::', 'harness-lib'),
    owner('batch', 'src/application/read_batch.rs', 'application::read_batch::', 'harness-lib'),
    owner('retained', 'src/retained_bytes.rs', 'retained_bytes::', 'harness-lib'),
    owner('quota', 'src/quota/bucket.rs', 'quota_bucket::', 'harness-lib'),
    owner('cursors', 'src/product_cursor/decode.rs', 'product_cursor::', 'harness-lib'),
    owner('queue', 'src/queue.rs', 'queue::', 'harness-lib'),
    owner('rollup_allocation', 'src/rollup/allocation.rs', 'rollup_allocation::', 'harness-lib'),
    owner('rollup_storage', 'src/rollup/storage.rs', 'rollup_storage::', 'harness-lib'),
    owner('tasks', 'src/tasks.rs', 'tasks::'),
    owner('touch', 'src/touch.rs', 'touch::'),
    owner('read_accumulator', 'src/billing/read_accumulator.rs', 'billing'),
    owner('read_spool', 'src/billing/read_spool.rs', 'billing'),
    owner('shard_directory', 'src/shard_directory.rs', 'shard_directory::'),
    owner('history_partition', 'src/shard/history_partition.rs', 'shard::'),
    owner('ops', 'src/ops.rs', 'ops::'),
    owner('scaler', 'src/scaler3.rs', 'scaler3::'),
    owner('postings_cache', 'src/postings_cache.rs', 'postings_cache::'),
    owner('sharddir', 'src/sharddir.rs', 'sharddir::'),
    owner('crypto', 'src/crypto.rs', 'crypto::'),
    owner('tail_ring', 'src/shard/tail_ring.rs', 'shard:: dst_tests::reads_ring::'),
    owner('tail_ring_tests', 'src/shard/tail_ring_tests.rs', 'shard:: dst_tests::reads_ring::'),
    owner('shard', 'src/shard.rs', 'shard::'),
    owner('bootstrap', 'src/bootstrap.rs', 'bootstrap::'),
    owner('bootstrap_rss', 'src/bootstrap/rss.rs', 'bootstrap::'),
    owner('bootstrap_tests', 'src/bootstrap/tests.rs', 'bootstrap::'),
    owner('read_request', 'src/application/read_request.rs', 'application::read_request::'),
    owner('http_read', 'src/http/read.rs', 'http::read:: dst_tests::reads_raw:: dst_tests::reads_history::'),
    owner('queue_cleanup', 'src/shard/transaction/queue/cleanup.rs', 'shard::'),
    owner('transaction_append', 'src/shard/transaction/append.rs', 'shard::'),
    owner('record', 'src/shard/record.rs', 'shard::'),
    owner('tasks_shutdown', 'src/tasks/shutdown.rs', 'tasks::'),
    owner('runtime', 'src/runtime.rs', 'runtime::'),
    owner('runtime_telemetry', 'src/runtime/telemetry.rs', 'runtime::'),
    owner('product_cursor', 'src/product_cursor.rs', 'product_cursor::'),
    owner('product_cursor_regressions', 'src/product_cursor/regressions.rs', 'product_cursor::', 'harness-lib'),
    owner('quota_registry', 'src/quota.rs', 'quota::'),
    owner('commit_handoff', 'src/shard/commit_handoff.rs', 'shard::'),
    owner('commit_plan', 'src/shard/commit_plan.rs', 'shard::'),
    owner('shard_lifecycle', 'src/shard/lifecycle.rs', 'shard::'),
    owner('transaction_finalize', 'src/shard/transaction/finalize.rs', 'shard::'),
    owner('transaction_maintenance', 'src/shard/transaction/maintenance.rs', 'shard::'),
    owner('transaction_group', 'src/shard/transaction/mod.rs', 'shard::'),
    owner('transaction_overlay', 'src/shard/transaction/overlay.rs', 'shard::'),
    owner('transaction_prepare', 'src/shard/transaction/prepare.rs', 'shard::'),
    owner('transaction_publish', 'src/shard/transaction/publish.rs', 'shard::'),
    owner('queue_config', 'src/shard/transaction/queue/config.rs', 'shard::'),
    owner('queue_load', 'src/shard/transaction/queue/load.rs', 'shard::'),
    owner('queue_dispatch', 'src/shard/transaction/queue/mod.rs', 'shard::'),
    owner('queue_receive', 'src/shard/transaction/queue/receive.rs', 'shard::'),
    owner('queue_settle', 'src/shard/transaction/queue/settle.rs', 'shard::'),
    owner('transaction_tests', 'src/shard/transaction_tests.rs', 'shard::'),
    owner('task_lifecycle_tests', 'src/shard/task_lifecycle_tests.rs', 'shard::'),
    owner('record_scan_tests', 'src/shard/record_scan_tests.rs', 'shard::'),
    owner('read_budget_tests', 'src/shard/read_budget_tests.rs', 'shard::'),
    owner('retirement_tests', 'src/shard/retirement_tests.rs', 'shard::'),
    owner('queue_codec_tests', 'src/shard/queue_codec_tests.rs', 'shard::'),
    owner('durability_frontier_tests', 'src/shard/durability_frontier_tests.rs', 'shard::'),
    owner('read_budget', 'src/application/read_budget.rs', 'application::read'),
    owner('read_decode', 'src/application/read_decode.rs', 'application::read'),
    owner('read_decode_tests', 'src/application/read_decode/tests.rs', 'application::read'),
    owner('read_keys', 'src/application/read_keys.rs', 'application::read'),
    owner('read_remote', 'src/application/read_remote.rs', 'application::read'),
    owner('read_scan', 'src/application/read_scan.rs', 'application::read'),
    owner('read_wire', 'src/application/read_wire.rs', 'application::read'),
    owner('read_wire_tests', 'src/application/read_wire_tests.rs', 'application::read'),
    owner('read_batch_tests', 'src/application/read_batch/tests.rs', 'application::read_batch::'),
    owner('crypto_decrypt', 'src/crypto/decrypt.rs', 'crypto::'),
    owner('crypto_decrypt_tests', 'src/crypto/decrypt/tests.rs', 'crypto::'),
    owner('fleet_outbox', 'src/fleet/outbox.rs', 'fleet::'),
    owner('fleet_repository', 'src/fleet/repository.rs', 'fleet::'),
    owner('fleet_document_tests', 'src/fleet/repository/document_tests.rs', 'fleet::'),
    owner('runtime_handoff', 'src/bootstrap/runtime_handoff.rs', 'bootstrap::'),
    owner('process_executor', 'src/bootstrap/process_executor.rs', 'bootstrap::'),
    owner('sharddir_health', 'src/sharddir/health.rs', 'sharddir::'),
    owner('sse_auth', 'src/sse/auth.rs', 'sse::'),
    owner('sse_registry', 'src/sse/registry.rs', 'sse::'),
    owner('sse_service', 'src/sse/service.rs', 'sse::'),
    owner('sse_session', 'src/sse/session.rs', 'sse:: dst_tests::sse_delivery:: dst_tests::livefeed_swap::'),
    owner('sse_wire', 'src/sse/wire.rs', 'sse::'),
    owner('fleet', 'src/fleet.rs', 'fleet::'),
    owner('http', 'src/http.rs', 'http::'),
    owner('sse_source', 'src/sse/source.rs', 'sse::'),
    owner('sse_source_tests', 'src/sse/source/tests.rs', 'sse::'),
    owner('sse_source_spans', 'src/sse/source/spans.rs', 'sse::'),
    owner('sse_feed', 'src/sse/feed.rs', 'sse::'),
    owner('sse_feed_retention', 'src/sse/feed/retention.rs', 'sse::'),
    owner('sse_feed_test_support', 'src/sse/feed/test_support.rs', 'sse::'),
    owner('postings_validated_tests', 'src/postings/validated/tests.rs', 'postings::'),
    MutationOwner(
        'pilot-benchmark',
        ('src/bin/pilot/benchmark.rs', 'src/bin/pilot/benchmark/config.rs',
         'src/bin/pilot/benchmark/window.rs'),
        ('benchmark::',),
        'pilot-benchmark',
    ),
    MutationOwner(
        'pilot-generator',
        ('src/bin/pilot/generator.rs', 'src/bin/pilot/generator/membership.rs'),
        (),
        'pilot-generator',
    ),
)


def source_map(owners=OWNERS):
    result = {}
    names = set()
    for entry in owners:
        if entry.name in names:
            raise ValueError(f'duplicate mutation owner name: {entry.name}')
        if entry.target not in {'service-lib', 'harness-lib', 'pilot-benchmark', 'pilot-generator'}:
            raise ValueError(f'unknown mutation target for {entry.name}: {entry.target}')
        if not entry.sources or (entry.target != 'pilot-generator' and not entry.test_filters):
            raise ValueError(f'incomplete mutation owner: {entry.name}')
        names.add(entry.name)
        for path in entry.sources:
            if path in result:
                raise ValueError(f'duplicate mutation source: {path}')
            result[path] = entry
    return result


def validate_plan(plan, owners=OWNERS):
    """Return selected owners or fail for every unregistered live source.

    This runs before mutant discovery.  Consequently a registered source that
    happens to select mutants can never mask an unregistered sibling.
    """
    by_source = source_map(owners)
    planned = set(plan.get('mutation_source_files', ()))
    missing = sorted(planned - set(by_source))
    if missing:
        raise ValueError(
            'register every changed critical mutation owner before verification:\n'
            + '\n'.join(missing)
        )
    selected = {by_source[path] for path in planned}
    return tuple(entry for entry in owners if entry in selected)


def scheduled_owners(slot, buckets=7, owners=OWNERS):
    """Stable seven-night rotation; adding a row does not reshuffle old rows."""
    if buckets < 1:
        raise ValueError('schedule bucket count must be positive')
    selected = []
    for entry in owners:
        bucket = int.from_bytes(hashlib.sha256(entry.name.encode()).digest()[:4], 'big') % buckets
        if bucket == slot % buckets:
            selected.append(entry)
    if not selected:
        raise ValueError(f'scheduled mutation bucket {slot % buckets} has no owners')
    return tuple(selected)


def validate_sources(root, owners=OWNERS):
    missing = [path for entry in owners for path in entry.sources if not (Path(root) / path).is_file()]
    if missing:
        raise ValueError('registered mutation source is missing; remove or rename its row explicitly:\n'
                         + '\n'.join(missing))
