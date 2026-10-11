import json
import math
import os
from pathlib import Path
import re
import tempfile
import unittest
from unittest import mock

from common import ROOT
from mutation_driver import list_command, mutation_command
import mutation_owners
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

    def test_a_shard_lists_and_tests_its_round_robin_share_and_nothing_else(self):
        entry = source_map()['src/sse/session.rs']
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with mock.patch.dict(os.environ, {'QUALITY_MUTANT_SHARD': '1/4'}):
                run = mutation_command(entry, root, root / 'result', root / 'selected.diff')
                listed = list_command(entry, root / 'selected.diff')
            with mock.patch.dict(os.environ, {'QUALITY_MUTANT_SHARD': ''}):
                whole = mutation_command(entry, root, root / 'result', root / 'selected.diff')
        for command in (run, listed):
            self.assertEqual(command[-4:], ['--shard', '1/4', '--sharding', 'round-robin'])
        self.assertNotIn('--shard', whole)
        for invalid in ('4/4', '1', 'a/b', '2/1'):
            with mock.patch.dict(os.environ, {'QUALITY_MUTANT_SHARD': invalid}):
                with self.assertRaises(ValueError):
                    mutation_command(entry, Path('/tmp'), Path('/tmp/r'), None)

    def test_a_split_owners_parts_and_runners_deal_each_mutant_once(self):
        entry = source_map()['src/sse/session.rs']
        dealt = []
        for part in range(3):
            for job in range(4):
                with mock.patch.dict(os.environ, {'QUALITY_MUTANT_SHARD': f'{job}/4'}):
                    run = mutation_command(entry, Path('/tmp'), Path('/tmp/r'), None, (part, 3))
                    listed = list_command(entry, None, share=(part, 3))
                self.assertEqual(run[-4:], listed[-4:])
                self.assertEqual(run[-2:], ['--sharding', 'round-robin'])
                dealt.append(run[-3])
        # Round-robin gives mutant i to shard i mod 12: every mutant once.
        self.assertCountEqual(dealt, [f'{index}/12' for index in range(12)])
        with mock.patch.dict(os.environ, {'QUALITY_MUTANT_SHARD': ''}):
            self.assertEqual(list_command(entry, None, share=(1, 3))[-4:],
                             ['--shard', '1/3', '--sharding', 'round-robin'])
            self.assertNotIn('--shard', list_command(entry, None, share=(0, 1)))

    def test_every_registered_owner_has_a_measured_size(self):
        recorded = json.loads(mutation_owners.SIZES_PATH.read_text())
        self.assertEqual(sorted(recorded['owners']), sorted(entry.name for entry in OWNERS))
        self.assertTrue(all(type(count) is int and count >= 0
                            for count in recorded['owners'].values()))
        self.assertRegex(recorded['commit'], r'^[0-9a-f]{40}$')
        missing = {name: count for name, count in recorded['owners'].items() if name != 'scaler'}
        with self.assertRaisesRegex(ValueError, r"missing \['scaler'\].*--measure-sizes"):
            with mock.patch.object(Path, 'read_text',
                                   return_value=json.dumps({**recorded, 'owners': missing})):
                mutation_owners.owner_sizes()

    def test_each_night_fits_the_mutants_job_on_its_slowest_runner(self):
        workflow = (ROOT / '.github/workflows/rust-quality.yml').read_text()
        job = workflow[workflow.index('\n  mutants:'):workflow.index('\n  formal:')]
        jobs = mutation_owners.SCHEDULE_JOBS
        self.assertEqual(re.search(r'shard: \[([^\]]*)\]', job).group(1),
                         ', '.join(str(index) for index in range(jobs)))
        self.assertIn(f'QUALITY_MUTANT_SHARD: ${{{{ matrix.shard }}}}/{jobs}', job)
        timeout = int(re.search(r'timeout-minutes: (\d+)', job).group(1))
        self.assertLessEqual(mutation_owners.SCHEDULE_CAP_MINUTES, 0.75 * timeout)
        groups = mutation_owners.schedule_groups()
        self.assertGreater(len(groups), 7)
        for group in groups:
            self.assertTrue(group)
            self.assertLessEqual(sum(share.minutes for share in group),
                                 mutation_owners.SCHEDULE_CAP_MINUTES)

    def test_each_night_fits_the_cap_at_the_slowest_runner_costs_measured_on_2026_10_10(self):
        # The scheduled run of 2026-10-10 (run 38022201458) on eight runners:
        # a service-crate baseline took 435-734 s as the night's first and
        # 293-495 s after it, a mutant 3.1-4.7 min, and the `shard` owner's
        # 354 mutants (44-45 a runner) 144-213 min a runner. Charged those
        # worst costs (harness owners at their model), every group fits.
        # This pins the sizes measured on 2026-10-11, not an invariant of the
        # packer: the night's first baseline costs 4.7 min over the model, each
        # later one 0.75 and each mutant 0.3 under, so a night of many small
        # service owners can pass the cap here while its model fits. If it fails after a re-measure
        # or new owner rows, re-pack: raise DEFAULT_NIGHT_MINUTES (a cost-model
        # call, as N6 was) until the packer deals more nights. Never loosen
        # these measured costs, the cap or a filter to make it pass.
        first, later, each = 734 / 60, 495 / 60, 4.7
        sizes = mutation_owners.owner_sizes()
        jobs = mutation_owners.SCHEDULE_JOBS
        for group in mutation_owners.schedule_groups():
            minutes, built = 0.0, False
            for share in group:
                mutants = math.ceil(sizes[share.owner.name] / (share.parts * jobs))
                if share.owner.target != 'service-lib' or not mutants:
                    minutes += share.minutes
                    continue
                minutes += (later if built else first) + mutants * each
                built = True
            self.assertLessEqual(minutes, mutation_owners.SCHEDULE_CAP_MINUTES,
                                 f'{[share.owner.name for share in group]}: over the cap at the '
                                 'measured costs; re-pack by raising DEFAULT_NIGHT_MINUTES, '
                                 'never by loosening these costs, the cap or a filter')
        shard = next(entry for entry in OWNERS if entry.name == 'shard')
        self.assertGreaterEqual(mutation_owners.night_minutes(shard, 354), 495 / 60 + 213)

    def test_the_night_model_charges_each_runner_its_baseline_and_largest_share(self):
        service = MutationOwner('service', ('src/service.rs',), ('service::',))
        harness = MutationOwner('harness', ('src/harness.rs',), ('harness::',), 'harness-lib')
        minutes = mutation_owners.night_minutes
        self.assertEqual(minutes(service, 0), 0)
        self.assertEqual(minutes(service, 1), 7.5 + 5)
        self.assertEqual(minutes(service, 9, jobs=4), 7.5 + 3 * 5)
        self.assertEqual(minutes(service, 9, parts=2, jobs=4), 7.5 + 2 * 5)
        self.assertEqual(minutes(harness, 9, jobs=4), 1 + 3 * 0.25)
        # The job's default width, eight runners: 17 mutants are 3 on runner 0.
        self.assertEqual(mutation_owners.SCHEDULE_JOBS, 8)
        self.assertEqual(minutes(service, 17), 7.5 + 3 * 5)
        self.assertEqual(minutes(service, 17, parts=2), 7.5 + 2 * 5)

    def test_an_owner_too_large_for_one_night_is_dealt_over_several_nights(self):
        big = MutationOwner('big', ('src/big.rs',), ('big::',))
        small = tuple(MutationOwner(f's{index}', (f'src/s{index}.rs',), ('s::',))
                      for index in range(6))
        idle = MutationOwner('idle', ('src/idle.rs',), ('idle::',))
        owners = (big, *small, idle)
        sizes = {'big': 1000, 'idle': 0, **{entry.name: 100 for entry in small}}
        groups = mutation_owners.schedule_groups(owners, sizes)
        self.assertEqual(groups, mutation_owners.schedule_groups(owners, dict(reversed(sizes.items()))))
        placed = [(index, share.owner.name, share.part, share.parts)
                  for index, group in enumerate(groups) for share in group]
        big_parts = [row for row in placed if row[1] == 'big']
        # 1,000 mutants are 125 per runner of eight, 7.5 + 125 x 5 = 632.5 minutes,
        # over the 270-minute night, and two parts of 7.5 + 63 x 5 = 322.5 are
        # too: three parts of 7.5 + 42 x 5 = 217.5.
        self.assertEqual(sorted(row[2:] for row in big_parts), [(0, 3), (1, 3), (2, 3)])
        self.assertEqual(len({row[0] for row in big_parts}), 3)
        self.assertCountEqual([row[1] for row in placed if row[1] != 'big'],
                              [entry.name for entry in (*small, idle)])
        for group in groups:
            self.assertLessEqual(sum(share.minutes for share in group),
                                 mutation_owners.SCHEDULE_CAP_MINUTES)
            self.assertEqual([share.owner for share in group],
                             [entry for entry in owners if entry in {s.owner for s in group}])
        self.assertEqual(len(groups), 5)  # 3 x 217.5 + 6 x 72.5 = 1,087.5 minutes: at least 5.

    def test_table_names_and_sources_are_unique(self):
        self.assertEqual(len({entry.name for entry in OWNERS}), len(OWNERS))
        self.assertEqual(sum(len(entry.sources) for entry in OWNERS), len(source_map()))

    def test_prior_table_parser_recovers_every_current_owner_without_execution(self):
        source = (Path(__file__).resolve().parent / 'mutation_owners.py').read_text()
        self.assertEqual(
            declared_source_map(source),
            {path: owner.name for path, owner in source_map().items()},
        )

    def test_prior_table_parser_rejects_dynamic_owner_construction(self):
        with self.assertRaisesRegex(ValueError, 'literal OWNERS sequence'):
            declared_source_map('OWNERS = make_runtime_table()\n')


if __name__ == '__main__':
    unittest.main()
