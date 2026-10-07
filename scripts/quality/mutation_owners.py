"""Canonical ownership and resolved selection for mutation checks.

The table owns source, package, target, and test-filter identity.  The planner
adds unknown paths selected by its broad critical policy or rename lineage,
then hands one resolved selection to the driver.  No later layer narrows it.
"""
import ast
from dataclasses import dataclass
import json
import math
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


@dataclass(frozen=True)
class MutationSelection:
    """The complete planner-to-driver mutation handoff."""

    changed_sources: tuple[str, ...]
    owners: tuple[MutationOwner, ...]
    unregistered_sources: tuple[str, ...]

    @property
    def discovery_sources(self) -> tuple[str, ...]:
        return tuple(sorted(path for entry in self.owners for path in entry.sources))

    def receipt(self) -> dict:
        return {
            'mutation_source_files': list(self.changed_sources),
            'selected_mutation_owners': [entry.name for entry in self.owners],
            'mutation_discovery_source_files': list(self.discovery_sources),
            'unregistered_mutation_source_files': list(self.unregistered_sources),
        }


def owner(name: str, source: str, filters: str, target: str = 'service-lib') -> MutationOwner:
    return MutationOwner(name, (source,), tuple(filters.split()), target)


# One row owns path selection, cargo package/target selection, and test filters.
# Keep source paths literal and explicit: a new file under a critical prefix
# must receive a deliberate row instead of silently inheriting a broad glob.
OWNERS = (
    owner('postings_codec', 'src/postings.rs', 'postings::', 'harness-lib'),
    owner('postings_codec_tests', 'src/postings/codec_tests.rs', 'postings::', 'harness-lib'),
    owner('postings', 'src/postings/validated.rs', 'postings::', 'harness-lib'),
    owner('postings_properties', 'src/postings/validated/properties.rs', 'postings::', 'harness-lib'),
    owner('batch', 'src/application/read_batch.rs', 'application::read_batch::', 'harness-lib'),
    owner('retained', 'src/retained_bytes.rs', 'retained_bytes::', 'harness-lib'),
    owner('quota', 'src/quota/bucket.rs', 'quota_bucket::', 'harness-lib'),
    owner('cursors', 'src/product_cursor/decode.rs', 'product_cursor::', 'harness-lib'),
    owner('queue', 'src/queue.rs', 'queue::', 'harness-lib'),
    owner('rollup_allocation', 'src/rollup/allocation.rs', 'rollup_allocation::', 'harness-lib'),
    owner('rollup_storage', 'src/rollup/storage.rs', 'rollup_storage::', 'harness-lib'),
    owner('tasks', 'src/tasks.rs', 'tasks::'),
    owner('tasks_refusal', 'src/tasks/refusal.rs', 'tasks::'),
    owner('touch', 'src/touch.rs', 'touch::'),
    owner('read_accumulator', 'src/billing/read_accumulator.rs', 'billing'),
    owner('read_spool', 'src/billing/read_spool.rs', 'billing'),
    owner('sweep_custody', 'src/billing/sweep_custody.rs', 'billing::sweep_custody:: dst_tests::runtime_sweep::'),
    owner('system_append', 'src/billing/system_append.rs', 'security_workload:: security_audit:: runtime_journals:: reserved_streams_append'),
    owner('admission_limits', 'src/config/admission_limits.rs', 'config::admission_limits:: usage::runtime_tests:: validation_rejects_a_limit_posture'),
    # Codec and admission owners outside the critical prefixes: the row is
    # their only mutation selection; each whole file was killed at registration.
    owner('offsets', 'src/offsets.rs', 'offsets::'),
    owner('segmap', 'src/segmap.rs', 'segmap::'),
    owner('telemetry_batch', 'src/telemetry_batch.rs', 'telemetry_batch::'),
    # Isolation owners outside the critical prefixes (owner decision of
    # 2026-10-08, shared cells Q3(d)): a shared cell's per-project ceiling
    # and reserved identities, and each signing key's one audience.
    owner('auth_ceiling', 'src/auth/ceiling.rs', 'auth:: dst_tests::shared_cell_hostile::reserved_ids'),
    owner('auth_signing_key', 'src/auth/signing_key.rs', 'auth:: auth_feed::'),
    owner('shard_directory', 'src/shard_directory.rs', 'shard_directory:: shard::task_lifecycle_tests::'),
    owner('history_partition', 'src/shard/history_partition.rs', 'shard::'),
    owner('ops', 'src/ops.rs', 'ops:: dst_tests::fork_debt:: dst_tests::runtime_journals::'),
    owner('scaler', 'src/scaler3.rs', 'scaler3:: dst_tests::scaler_loop::'),
    owner('postings_cache', 'src/postings_cache.rs', 'postings_cache::'),
    owner('postings_cache_owned_load', 'src/postings_cache/owned_load.rs', 'postings_cache::'),
    owner('sharddir', 'src/sharddir.rs', 'sharddir::'),
    owner('sharddir_unwind', 'src/sharddir/unwind.rs', 'sharddir::'),
    owner('sharddir_holdoff', 'src/sharddir/holdoff.rs', 'sharddir::'),
    owner('crypto', 'src/crypto.rs', 'crypto::'),
    owner('tail_ring', 'src/shard/tail_ring.rs', 'shard:: dst_tests::reads_ring::'),
    owner('tail_ring_tests', 'src/shard/tail_ring_tests.rs', 'shard:: dst_tests::reads_ring::'),
    owner('shard', 'src/shard.rs', 'shard::'),
    owner('shard_billing_ops', 'src/shard/billing_ops.rs', 'shard:: dst_tests::billing_walk_custody::'),
    owner('bootstrap', 'src/bootstrap.rs', 'bootstrap::'),
    owner('bootstrap_rss', 'src/bootstrap/rss.rs', 'bootstrap::'),
    owner('bootstrap_s3_store', 'src/bootstrap/s3_store.rs', 'bootstrap::'),
    owner('bootstrap_service_runtime', 'src/bootstrap/service_runtime.rs', 'bootstrap::service_runtime::'),
    owner('bootstrap_tests', 'src/bootstrap/tests.rs', 'bootstrap::'),
    owner('read_request', 'src/application/read_request.rs', 'dst_tests::read_application:: dst_tests::reads_applied:: dst_tests::reads_applied_history::'),
    owner('http_serve', 'src/http/serve.rs', 'http::serve::'),
    owner('http_read', 'src/http/read.rs', 'dst_tests::reads_raw:: dst_tests::reads_history:: dst_tests::read_application:: dst_tests::read_page_limits:: dst_tests::reads_applied_history:: dst_tests::sse_delivery::'),
    owner('http_telemetry_append', 'src/http/telemetry_append.rs', 'security_workload:: reserved_streams_append'),
    owner('queue_cleanup', 'src/shard/transaction/queue/cleanup.rs', 'shard::'),
    owner('transaction_append', 'src/shard/transaction/append.rs', 'shard::'),
    owner('record', 'src/shard/record.rs', 'shard::'),
    owner('lane_rows', 'src/shard/lane_rows.rs', 'shard::'),
    owner('maintenance_row', 'src/shard/maintenance_row.rs', 'shard::'),
    owner('tasks_shutdown', 'src/tasks/shutdown.rs', 'tasks::'),
    owner('tasks_exits', 'src/tasks/exits.rs', 'tasks::'),
    owner('tasks_drain', 'src/tasks/drain.rs', 'tasks::'),
    owner('runtime', 'src/runtime.rs', 'runtime::'),
    owner('runtime_telemetry', 'src/runtime/telemetry.rs', 'runtime::'),
    owner('product_cursor', 'src/product_cursor.rs', 'product_cursor::'),
    owner('product_cursor_regressions', 'src/product_cursor/regressions.rs', 'product_cursor::', 'harness-lib'),
    owner('quota_registry', 'src/quota.rs', 'quota::'),
    owner('quota_pin', 'src/quota/pin.rs', 'quota::'),
    owner('quota_parked', 'src/quota/parked.rs', 'quota::parked:: dst_tests::parked_waits::a_projects_'),
    owner('quota_pressure', 'src/quota/pressure.rs', 'quota::pressure_tests:: quota::pressure_counting_tests::'),
    owner('registry_cache', 'src/registry/cache.rs', 'registry::'),
    owner('commit_handoff', 'src/shard/commit_handoff.rs', 'shard::'),
    owner('commit_plan', 'src/shard/commit_plan.rs', 'shard::'),
    owner('shard_lifecycle', 'src/shard/lifecycle.rs', 'shard::'),
    owner('transaction_finalize', 'src/shard/transaction/finalize.rs', 'shard::'),
    owner('transaction_maintenance', 'src/shard/transaction/maintenance.rs', 'shard::'),
    owner('transaction_group', 'src/shard/transaction/mod.rs', 'shard::'),
    owner('transaction_billing', 'src/shard/transaction/billing.rs', 'shard::'),
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
    owner('read_remote', 'src/application/read_remote.rs', 'application::read dst_tests::read_application:: dst_tests::read_page_limits::'),
    owner('read_remote_tests', 'src/application/read_remote_tests.rs', 'application::read'),
    owner('read_continuation', 'src/application/read_continuation.rs', 'application::read dst_tests::reads_applied_history::'),
    owner('read_scan', 'src/application/read_scan.rs', 'application::read'),
    owner('read_wire', 'src/application/read_wire.rs', 'application::read'),
    owner('read_wire_tests', 'src/application/read_wire_tests.rs', 'application::read'),
    owner('read_batch_tests', 'src/application/read_batch/tests.rs', 'application::read_batch::'),
    owner('crypto_decrypt', 'src/crypto/decrypt.rs', 'crypto::'),
    owner('crypto_decrypt_tests', 'src/crypto/decrypt/tests.rs', 'crypto::'),
    owner('fleet_outbox', 'src/fleet/outbox.rs', 'fleet::'),
    # The repository hands each runtime its standing; only the rigs read a
    # standing back through a published heartbeat.
    owner('fleet_repository', 'src/fleet/repository.rs', 'fleet:: dst_tests::fleet_controller::'),
    # The drain's handoff is proven by the two-instance rigs.
    owner('fleet_drain', 'src/fleet/drain.rs', 'fleet:: dst_tests::fleet_drain::'),
    owner('fleet_document_tests', 'src/fleet/repository/document_tests.rs', 'fleet::'),
    owner('fleet_planning', 'src/fleet/planning.rs', 'fleet::'),
    owner('fleet_standing', 'src/fleet/standing.rs', 'fleet::'),
    # The beat loop and the tick's pressure reading are proven by the rigs.
    owner('fleet_heartbeat', 'src/fleet/heartbeat.rs',
          'fleet:: dst_tests::fleet_controller:: dst_tests::fleet_drain::'),
    owner('runtime_handoff', 'src/bootstrap/runtime_handoff.rs', 'bootstrap::'),
    owner('process_executor', 'src/bootstrap/process_executor.rs', 'bootstrap::'),
    owner('sharddir_health', 'src/sharddir/health.rs', 'sharddir::'),
    owner('sse_auth', 'src/sse/auth.rs', 'sse::'),
    owner('sse_registry', 'src/sse/registry.rs', 'sse::'),
    owner('sse_service', 'src/sse/service.rs', 'sse::'),
    owner('sse_session', 'src/sse/session.rs', 'sse:: dst_tests::sse_delivery:: dst_tests::livefeed_swap:: livefeed_engine_retired'),
    owner('sse_session_read_retry', 'src/sse/session/read_retry.rs', 'sse::session::tests:: dst_tests::sse_delivery::'),
    owner('sse_session_catch_up', 'src/sse/session/catch_up.rs', 'sse::session::tests:: dst_tests::sse_delivery::'),
    owner('sse_wire', 'src/sse/wire.rs', 'sse::'),
    # The assembly and the tick are proven by DST rigs, not by module tests:
    # `fleet::` alone ran zero tests against a mutated `start_configured`.
    owner('fleet', 'src/fleet.rs',
          'fleet:: dst_tests::fleet_controller:: dst_tests::fleet_drain:: dst_tests::runtime_isolation::'),
    owner('http', 'src/http.rs', 'http:: livefeed_engine_retired security_workload:: security_usage:: debug_store_reports_this_runtimes_shard_opens debug_surface_ dst_tests::billing_readiness:: dst_tests::billing_operation_counts::raw_'),
    owner('http_debug', 'src/http/debug.rs', 'debug_surface_'),
    owner('http_close_identity', 'src/http/close_identity.rs', 'http::close_identity:: dst_tests::seal_fencing:: dst_tests::seal_coordination:: dst_tests::security_seal::'),
    owner('http_internal_routes', 'src/http/internal_routes.rs', 'security_workload:: dst_tests::seal_convergence:: dst_tests::seal_fence_receiver::'),
    # F1-a's relaying sender (82a14a8f); the owner approved the row on 2026-10-02.
    owner('lifecycle_fence_relay', 'src/application/lifecycle/fence_relay.rs', 'dst_tests::seal_fence_relay:: dst_tests::seal_convergence::'),
    owner('sse_source', 'src/sse/source.rs', 'sse:: livefeed_engine_retired'),
    owner('sse_source_tests', 'src/sse/source/tests.rs', 'sse::'),
    owner('sse_source_spans', 'src/sse/source/spans.rs', 'sse::'),
    owner('sse_feed', 'src/sse/feed.rs', 'sse::'),
    owner('sse_feed_drive', 'src/sse/feed/drive.rs', 'sse::'),
    owner('sse_feed_retry_tests', 'src/sse/feed/tests/retry.rs', 'sse::'),
    owner('sse_feed_test_fixture', 'src/sse/feed/tests/fixture.rs', 'sse::'),
    owner('sse_feed_retention', 'src/sse/feed/retention.rs', 'sse::'),
    owner('sse_feed_test_support', 'src/sse/feed/test_support.rs', 'sse::'),
    owner('sse_budget', 'src/sse/budget.rs', 'sse::'),
    owner('postings_validated_tests', 'src/postings/validated/tests.rs', 'postings::'),
    # The release campaign's owners (NEXT-WORK §9 step 2): the product surface,
    # the billing core, the usage rollup, the replaced-incarnation closure
    # debts and the append application had no row, so the per-push plan never
    # selected them. Each filter names the module's own tests and the DST
    # modules that pin the file; a module without tests of its own runs the
    # DSTs alone. `product::` also selects the consumer_product and
    # reads_product DSTs; `billing` is the whole billing surface, as for the
    # read accumulator and spool rows.
    owner('product', 'src/product.rs',
          'product:: dst_tests::product_lifecycle:: dst_tests::product_descriptor_reads:: '
          'dst_tests::security_operations:: dst_tests::security_routes::'),
    owner('product_usage', 'src/product/usage.rs',
          'dst_tests::security_usage:: dst_tests::billing_usage:: dst_tests::billing_attribution:: '
          'dst_tests::product_descriptor_reads::'),
    owner('product_seal_request', 'src/product/seal_request.rs',
          'dst_tests::seal_coordination:: dst_tests::security_seal:: dst_tests::seal_fencing:: '
          'dst_tests::seal_convergence::'),
    owner('product_operation', 'src/product/operation.rs',
          'dst_tests::security_operations:: dst_tests::security_routes::'),
    owner('product_append_body', 'src/product/append_body.rs',
          'product::tests:: dst_tests::append_application:: dst_tests::producer_protocol:: '
          'dst_tests::quota_enforcement::'),
    owner('product_consumer_pull', 'src/product/consumer_pull.rs',
          'dst_tests::consumer_product:: dst_tests::consumer_delete:: dst_tests::consumer_dlq:: '
          'dst_tests::quota_read_volume::'),
    owner('product_internal', 'src/product/internal.rs',
          'product::tests:: dst_tests::runtime_sweep:: dst_tests::read_peer_compatibility:: '
          'dst_tests::product_descriptor_reads::'),
    owner('product_read_cursor', 'src/product/read_cursor.rs',
          'dst_tests::reads_product:: dst_tests::reads_applied:: dst_tests::read_application:: '
          'dst_tests::security_lineage::'),
    owner('product_scan', 'src/product/scan.rs',
          'dst_tests::reads_product:: dst_tests::quota_read_volume:: '
          'dst_tests::product_descriptor_reads:: dst_tests::security_modes::'),
    owner('product_answers', 'src/product/answers.rs',
          'dst_tests::product_lifecycle:: dst_tests::consumer_product::'),
    owner('billing', 'src/billing.rs', 'billing'),
    owner('billing_replaced', 'src/billing/replaced.rs',
          'dst_tests::billing_closure_debts:: dst_tests::billing_closure_owners:: '
          'dst_tests::billing_walk_custody:: dst_tests::billing_controller::'),
    owner('billing_walk', 'src/billing/walk.rs',
          'dst_tests::billing_walk_custody:: dst_tests::billing_closure_owners:: dst_tests::runtime_sweep::'),
    owner('billing_telemetry_loop', 'src/billing/telemetry_loop.rs',
          'billing::sweep_custody:: dst_tests::billing_controller:: dst_tests::billing_readiness:: '
          'dst_tests::runtime_sweep::'),
    owner('rollup', 'src/rollup.rs',
          'rollup:: dst_tests::billing_usage:: dst_tests::billing_late_close:: dst_tests::security_usage::'),
    owner('rollup_page', 'src/rollup/page.rs',
          'rollup:: dst_tests::billing_late_close:: dst_tests::billing_usage::'),
    owner('rollup_close', 'src/rollup/close.rs',
          'rollup:: dst_tests::billing_controller:: dst_tests::billing_closure_debts:: '
          'dst_tests::billing_late_close::'),
    owner('rollup_reconciliation', 'src/rollup/reconciliation.rs',
          'rollup:: dst_tests::billing_attribution:: dst_tests::security_audit::'),
    owner('rollup_totals', 'src/rollup/totals.rs',
          'rollup:: dst_tests::billing_usage:: dst_tests::billing_attribution::'),
    owner('rollup_readiness', 'src/rollup/readiness.rs',
          'rollup:: dst_tests::billing_readiness::'),
    owner('registry_replaced', 'src/registry/replaced.rs',
          'dst_tests::billing_closure_debts:: dst_tests::billing_closure_owners:: '
          'dst_tests::billing_late_close:: dst_tests::fork_debt::'),
    owner('append', 'src/application/append.rs',
          'application::append:: dst_tests::append_application:: dst_tests::producer_protocol:: '
          'dst_tests::producer_handoff::'),
    owner('append_admission', 'src/application/append/admission.rs',
          'application::append:: dst_tests::append_application:: dst_tests::quota_enforcement::'),
    owner('append_close', 'src/application/append/close.rs',
          'application::append:: dst_tests::append_application:: dst_tests::seal_coordination:: '
          'dst_tests::seal_fencing:: dst_tests::seal_convergence::'),
    owner('append_content', 'src/application/append/content.rs',
          'application::append:: dst_tests::append_application:: dst_tests::producer_protocol::'),
    owner('append_contract', 'src/application/append/contract.rs',
          'application::append:: dst_tests::append_application:: dst_tests::producer_protocol:: '
          'dst_tests::producer_handoff::'),
    owner('append_route', 'src/application/append/route.rs',
          'application::append:: dst_tests::append_application:: dst_tests::topology_lifecycle::'),
    owner('append_submit', 'src/application/append/submit.rs',
          'application::append:: dst_tests::append_application:: dst_tests::durability_failures:: '
          'dst_tests::persistence_faults::'),
    # Production files under a critical prefix that the planner listed as
    # unregistered (NEXT-WORK §10): a push that changes one was refused.
    owner('read_range', 'src/application/read_range.rs',
          'application::read dst_tests::read_application:: dst_tests::read_page_limits:: '
          'dst_tests::read_subset_retention::'),
    owner('read_retention_probe', 'src/application/read_retention_probe.rs',
          'application::read_batch:: application::read_decode:: dst_tests::read_subset_retention:: '
          'dst_tests::read_peer_compatibility::'),
    owner('record_checked', 'src/shard/record/checked.rs',
          'shard::record::checked:: application::read_decode:: dst_tests::reads_raw:: '
          'dst_tests::reads_ring:: dst_tests::reads_history::'),
    owner('sse_mod', 'src/sse/mod.rs', 'sse::'),
    owner('tasks_signal', 'src/tasks/signal.rs', 'tasks::'),
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


def declared_source_map(source):
    """Read the literal owner paths from a prior trusted table without running it."""
    tree = ast.parse(source)
    assignments = [
        node.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == 'OWNERS' for target in node.targets)
    ]
    if len(assignments) != 1 or not isinstance(assignments[0], (ast.Tuple, ast.List)):
        raise ValueError('prior mutation owner table has no single literal OWNERS sequence')
    result = {}
    for row in assignments[0].elts:
        if not isinstance(row, ast.Call) or not isinstance(row.func, ast.Name):
            raise ValueError('prior mutation owner table contains an unsupported row')
        if row.func.id == 'owner' and len(row.args) >= 2:
            name = ast.literal_eval(row.args[0])
            sources = (ast.literal_eval(row.args[1]),)
        elif row.func.id == 'MutationOwner' and len(row.args) >= 2:
            name = ast.literal_eval(row.args[0])
            sources = tuple(ast.literal_eval(row.args[1]))
        else:
            raise ValueError('prior mutation owner table contains an unsupported constructor')
        if not isinstance(name, str) or not sources or not all(isinstance(path, str) for path in sources):
            raise ValueError('prior mutation owner table contains a non-literal identity')
        for path in sources:
            if path in result:
                raise ValueError(f'duplicate prior mutation source: {path}')
            result[path] = name
    return result


def resolve_sources(paths, owners=OWNERS):
    """Resolve every already-selected source exactly once."""
    by_source = source_map(owners)
    changed = tuple(sorted(set(paths)))
    unregistered = tuple(path for path in changed if path not in by_source)
    selected = {by_source[path] for path in changed if path in by_source}
    return MutationSelection(
        changed,
        tuple(entry for entry in owners if entry in selected),
        unregistered,
    )


def selection_for_owners(selected, owners=OWNERS):
    requested = set(selected)
    unknown = requested - set(owners)
    if unknown:
        raise ValueError(f'unknown mutation owner selection: {unknown}')
    ordered = tuple(entry for entry in owners if entry in requested)
    sources = tuple(sorted(path for entry in ordered for path in entry.sources))
    return MutationSelection(sources, ordered, ())


def validate_plan(plan, owners=OWNERS):
    """Validate the complete receipt and return its canonical owner objects.

    This runs before mutant discovery.  It both rejects unregistered sources
    and prevents schedule metadata or a stale consumer from narrowing the
    planner's resolved owner/source handoff.
    """
    resolved = resolve_sources(plan.get('mutation_source_files', ()), owners)
    expected = resolved.receipt()
    mismatches = []
    for field, value in expected.items():
        if plan.get(field) != value:
            mismatches.append(f'{field}: recorded={plan.get(field)!r}, resolved={value!r}')

    if plan.get('selection_kind') == 'scheduled-owner-rotation':
        slot = plan.get('schedule_slot')
        if type(slot) is not int:
            mismatches.append(f'schedule_slot: invalid {slot!r}')
        else:
            index, count, shares = scheduled_group(slot, owners=owners)
            recorded = (slot, plan.get('schedule_groups'))
            if recorded != (index, count):
                mismatches.append(
                    f'schedule_slot, schedule_groups: recorded={recorded!r}, '
                    f'expected={(index, count)!r}'
                )
            if plan.get('scheduled_owner_shares') != shares_receipt(shares):
                mismatches.append(
                    'scheduled_owner_shares: recorded='
                    f'{plan.get("scheduled_owner_shares")!r}, expected={shares_receipt(shares)!r}'
                )
            scheduled = selection_for_owners([share.owner for share in shares], owners)
            for field, value in scheduled.receipt().items():
                if plan.get(field) != value:
                    mismatches.append(
                        f'scheduled {field}: recorded={plan.get(field)!r}, expected={value!r}'
                    )
            if plan.get('scheduled_source_files') != list(scheduled.discovery_sources):
                mismatches.append(
                    'scheduled_source_files: recorded='
                    f'{plan.get("scheduled_source_files")!r}, '
                    f'expected={list(scheduled.discovery_sources)!r}'
                )
    if mismatches:
        raise ValueError('mutation selection receipt disagrees with canonical selection:\n'
                         + '\n'.join(mismatches))
    if resolved.unregistered_sources:
        raise ValueError(
            'register every changed critical mutation owner before verification:\n'
            + '\n'.join(resolved.unregistered_sources)
        )
    return resolved.owners


# Every owner's whole-scope mutant count, as the scheduled rotation lists it.
SIZES_PATH = Path(__file__).with_name('mutation-owner-sizes.json')
SIZES_COMMAND = 'python3 scripts/quality/mutation_driver.py --measure-sizes'

# The nightly rotation's cost model, in minutes on the slowest runner of
# rust-quality's `mutants` job. The job deals each owner's n mutants over
# SCHEDULE_JOBS runners round-robin, so runner 0 tests ceil(n / jobs) of them,
# and every runner with a share first builds and tests the owner's unmutated
# baseline. Measured on the scheduled runs of 2026-09-29 to 2026-10-02: a
# service-crate baseline took 293-348 s (430-543 s for the night's first)
# and a mutant 1.4-3.4 min, 2.3 on average, nearly all of it the rebuild;
# the harness crate's baseline under 40 s and its mutants seconds. A group
# may hold 270 minutes, three quarters of the job's 360: the rest is the
# runner's setup, the night's first cold build and slower mutants. Eight
# runners of 360 min since the owner's decision of 2026-10-08 (four of 240
# before), which deals the owners into 12 nights instead of 29.
SCHEDULE_JOBS = 8
SCHEDULE_CAP_MINUTES = 270.0
NIGHT_MINUTES = {'harness-lib': (1.0, 0.25)}  # target: (baseline, each mutant)
DEFAULT_NIGHT_MINUTES = (6.0, 2.5)


@dataclass(frozen=True)
class ScheduledShare:
    """One owner's work in a night: all its mutants, or for an owner too large
    for one night, part `part` of `parts` round-robin shares of them."""

    owner: MutationOwner
    part: int
    parts: int
    minutes: float


def owner_sizes(owners=OWNERS, path=SIZES_PATH):
    """The measured size of every owner; an unmeasured or retired one fails."""
    recorded = json.loads(Path(path).read_text())['owners']
    names = {entry.name for entry in owners}
    missing = sorted(names - set(recorded))
    stale = sorted(set(recorded) - names)
    invalid = sorted(name for name, count in recorded.items()
                     if type(count) is not int or count < 0)
    if missing or stale or invalid:
        raise ValueError(
            f'{Path(path).name} disagrees with the mutation owner table (missing {missing}, '
            f'stale {stale}, invalid {invalid}); re-measure with: {SIZES_COMMAND}'
        )
    return {entry.name: recorded[entry.name] for entry in owners}


def night_minutes(entry, mutants, parts=1, jobs=SCHEDULE_JOBS):
    """Modeled minutes of one share of an owner on the night's slowest runner."""
    if mutants == 0:
        return 0.0  # Only the listing runs; no baseline is built.
    baseline, each = NIGHT_MINUTES.get(entry.target, DEFAULT_NIGHT_MINUTES)
    return baseline + math.ceil(mutants / (parts * jobs)) * each


def owner_shares(entry, mutants, cap):
    """The fewest equal parts whose night fits the cap."""
    for parts in range(1, max(mutants, 1) + 1):
        minutes = night_minutes(entry, mutants, parts)
        if minutes <= cap:
            return tuple(ScheduledShare(entry, part, parts, minutes) for part in range(parts))
    raise ValueError(f'{entry.name}: one mutant alone exceeds the {cap}-minute night')


def schedule_groups(owners=OWNERS, sizes=None, cap=SCHEDULE_CAP_MINUTES):
    """Pack every owner's shares into groups that each fit the cap.

    Largest share first, each into the least-filled group (the lowest index on
    a tie) that holds no other part of the same owner; the group count starts
    at its lower bound and grows until every group fits. Inside a group the
    owners keep the table's order. The packing depends only on the table and
    the measured sizes, so every runner and the driver derive the same one."""
    sizes = owner_sizes(owners) if sizes is None else sizes
    rank = {entry.name: index for index, entry in enumerate(owners)}
    shares = [share for entry in owners for share in owner_shares(entry, sizes[entry.name], cap)]
    ordered = sorted(shares, key=lambda share: (-share.minutes, rank[share.owner.name], share.part))
    count = max(1, math.ceil(sum(share.minutes for share in shares) / cap),
                *(share.parts for share in shares))
    while True:
        groups = [[] for _ in range(count)]
        loads = [0.0] * count
        for share in ordered:
            index = min((index for index, group in enumerate(groups)
                         if all(other.owner != share.owner for other in group)),
                        key=lambda index: (loads[index], index))
            groups[index].append(share)
            loads[index] += share.minutes
        if max(loads) <= cap:
            break
        count += 1
    if not all(groups):
        raise ValueError('a scheduled mutation group has no owners')
    return tuple(tuple(sorted(group, key=lambda share: rank[share.owner.name]))
                 for group in groups)


def scheduled_group(slot, owners=OWNERS, sizes=None):
    """The UTC day's group: (its index, the number of groups, its shares)."""
    groups = schedule_groups(owners, sizes)
    index = slot % len(groups)
    return index, len(groups), groups[index]


def shares_receipt(shares):
    """The split owners of a group, as the plan records them: {name: [part, parts]}."""
    return {share.owner.name: [share.part, share.parts] for share in shares if share.parts > 1}


def validate_sources(root, owners=OWNERS):
    actual = {path: entry.name for path, entry in source_map(owners).items()}
    table = Path(root) / 'scripts/quality/mutation_owners.py'
    if owners is OWNERS and declared_source_map(table.read_text()) != actual:
        raise ValueError('literal mutation owner table disagrees with its runtime mapping')
    missing = [path for entry in owners for path in entry.sources if not (Path(root) / path).is_file()]
    if missing:
        raise ValueError('registered mutation source is missing; remove or rename its row explicitly:\n'
                         + '\n'.join(missing))
