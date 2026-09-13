from pathlib import Path
import tempfile
import unittest

from mutation_driver import mutation_command
from mutation_owners import (
    MutationOwner,
    OWNERS,
    declared_source_map,
    source_map,
    validate_plan,
)


def receipt(*paths, owners=()):
    selected = [entry for entry in OWNERS if entry.name in owners]
    return {
        'mutation_source_files': list(paths),
        'selected_mutation_owners': list(owners),
        'mutation_discovery_source_files': sorted(
            path for entry in selected for path in entry.sources
        ),
        'unregistered_mutation_source_files': [
            path for path in paths if path not in source_map()
        ],
        'selection_kind': 'changed-tree',
    }


class MutationOwnership(unittest.TestCase):
    def test_mixed_registered_and_unregistered_scope_fails_before_counts_exist(self):
        plan = receipt(
            'src/application/read_budget.rs',
            'src/shard/not_registered.rs',
            owners=('read_budget',),
        )
        with self.assertRaisesRegex(ValueError, 'src/shard/not_registered.rs'):
            validate_plan(plan)

    def test_unregistered_only_scope_fails(self):
        with self.assertRaisesRegex(ValueError, 'src/shard/not_registered.rs'):
            validate_plan(receipt('src/shard/not_registered.rs'))

    def test_registered_zero_scope_is_an_explicit_empty_selection(self):
        self.assertEqual(validate_plan(receipt()), ())

    def test_reviewed_moved_owners_are_registered(self):
        owners = source_map()
        self.assertEqual(owners['src/shard/commit_handoff.rs'].name, 'commit_handoff')
        self.assertEqual(owners['src/bootstrap/rss.rs'].name, 'bootstrap_rss')
        self.assertEqual(owners['src/postings/codec_tests.rs'].name, 'postings_codec_tests')
        self.assertEqual(
            owners['src/product_cursor/regressions.rs'].name,
            'product_cursor_regressions',
        )
        self.assertEqual(owners['src/shard/tail_ring_tests.rs'].name, 'tail_ring_tests')
        self.assertEqual(owners['src/sse/source/tests.rs'].name, 'sse_source_tests')

    def test_one_table_row_is_enough_to_add_an_owner(self):
        added = MutationOwner('new_owner', ('src/shard/new_owner.rs',), ('shard::',))
        self.assertEqual(
            validate_plan({
                'mutation_source_files': ['src/shard/new_owner.rs'],
                'selected_mutation_owners': ['new_owner'],
                'mutation_discovery_source_files': ['src/shard/new_owner.rs'],
                'unregistered_mutation_source_files': [],
                'selection_kind': 'changed-tree',
            }, (added,)),
            (added,),
        )

    def test_driver_refuses_a_receipt_that_silently_drops_an_owner(self):
        plan = receipt(
            'src/application/read_budget.rs',
            'src/scaler3.rs',
            owners=('read_budget',),
        )
        with self.assertRaisesRegex(ValueError, 'selection receipt disagrees'):
            validate_plan(plan)

    def test_driver_refuses_a_receipt_that_changes_discovery_sources(self):
        plan = receipt('src/scaler3.rs', owners=('scaler',))
        plan['scheduled_source_files'] = ['src/shard.rs']
        plan['selection_kind'] = 'scheduled-owner-rotation'
        plan['schedule_slot'] = 6
        with self.assertRaisesRegex(ValueError, 'selection receipt disagrees'):
            validate_plan(plan)

    def test_nonzero_owner_command_keeps_baseline_package_and_filters(self):
        entry = source_map()['src/sse/session.rs']
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            command = mutation_command(entry, root, root / 'result', root / 'selected.diff')
        self.assertIn('--baseline', command)
        self.assertIn('run', command)
        self.assertIn('--package', command)
        self.assertIn('streams-slate', command)
        self.assertIn('--cargo-test-arg=sse::', command)
        self.assertIn('--cargo-test-arg=dst_tests::sse_delivery::', command)
        self.assertIn('--cargo-test-arg=dst_tests::livefeed_swap::', command)

    def test_table_names_and_sources_are_unique(self):
        self.assertEqual(len({entry.name for entry in OWNERS}), len(OWNERS))
        self.assertEqual(sum(len(entry.sources) for entry in OWNERS), len(source_map()))

    def test_prior_table_parser_recovers_every_current_owner_without_execution(self):
        source = Path('scripts/quality/mutation_owners.py').read_text()
        self.assertEqual(
            declared_source_map(source),
            {path: owner.name for path, owner in source_map().items()},
        )

    def test_prior_table_parser_rejects_dynamic_owner_construction(self):
        with self.assertRaisesRegex(ValueError, 'literal OWNERS sequence'):
            declared_source_map('OWNERS = make_runtime_table()\n')


if __name__ == '__main__':
    unittest.main()
