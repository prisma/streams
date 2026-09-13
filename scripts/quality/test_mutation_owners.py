from pathlib import Path
import tempfile
import unittest

from mutation_driver import mutation_command
from mutation_owners import MutationOwner, OWNERS, source_map, validate_plan


class MutationOwnership(unittest.TestCase):
    def test_mixed_registered_and_unregistered_scope_fails_before_counts_exist(self):
        plan = {'mutation_source_files': [
            'src/application/read_budget.rs',
            'src/shard/not_registered.rs',
        ]}
        with self.assertRaisesRegex(ValueError, 'src/shard/not_registered.rs'):
            validate_plan(plan)

    def test_unregistered_only_scope_fails(self):
        with self.assertRaisesRegex(ValueError, 'src/shard/not_registered.rs'):
            validate_plan({'mutation_source_files': ['src/shard/not_registered.rs']})

    def test_registered_zero_scope_is_an_explicit_empty_selection(self):
        self.assertEqual(validate_plan({'mutation_source_files': []}), ())

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
            validate_plan({'mutation_source_files': ['src/shard/new_owner.rs']}, (added,)),
            (added,),
        )

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


if __name__ == '__main__':
    unittest.main()
