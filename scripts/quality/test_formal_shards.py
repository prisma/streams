"""CI formal shards partition exactly the driver's selection and balance it by LPT."""
import contextlib
import io
import itertools
import json
from pathlib import Path
import random
import tempfile
import unittest
from unittest import mock

import formal
import formal_shards

MANIFEST = formal.load()
ALL_IDS = sorted(o['id'] for o in MANIFEST['obligations'])


def run_main(*argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = formal_shards.main(list(argv))
        except SystemExit as exit_:  # argparse errors
            code = exit_.code
    return code, out.getvalue(), err.getvalue()


class Partition(unittest.TestCase):
    def test_every_real_obligation_lands_in_exactly_one_shard_for_1_to_8_shards(self):
        work = formal_shards.weights(ALL_IDS)
        for shards in range(1, 9):
            with self.subTest(shards=shards):
                partition = formal_shards.lpt(work, shards)
                self.assertEqual(len(partition), shards)
                self.assertEqual(formal_shards.partition_problems(partition, ALL_IDS), [])
                self.assertEqual(sorted(o for shard in partition for o in shard), ALL_IDS)

    def test_the_cli_shards_union_to_the_selection_with_no_duplicates(self):
        for shards in (1, 6, 8):
            printed = []
            for shard in range(shards):
                code, out, _ = run_main('--shards', str(shards), '--shard', str(shard), '--all')
                self.assertEqual(code, 0)
                printed += out.split()
            self.assertEqual(sorted(printed), ALL_IDS)
            self.assertEqual(len(printed), len(set(printed)))

    def test_partition_problems_name_lost_duplicated_and_unselected_ids(self):
        problems = formal_shards.partition_problems([['A', 'B'], ['B', 'X']], ['A', 'B', 'C'])
        self.assertEqual(problems, ['assigned to more than one shard: B',
                                    'selected but in no shard: C',
                                    'in a shard but not selected: X'])

    def test_more_shards_than_obligations_leaves_empty_shards(self):
        partition = formal_shards.lpt({'A': 5.0, 'B': 1.0}, 4)
        self.assertEqual(partition, [['A'], ['B'], [], []])

    def test_zero_shards_is_refused(self):
        with self.assertRaises(ValueError):
            formal_shards.lpt({'A': 1.0}, 0)


class Determinism(unittest.TestCase):
    def test_the_partition_does_not_depend_on_input_order(self):
        work = formal_shards.weights(ALL_IDS)
        expected = formal_shards.lpt(work, 6)
        shuffled = list(work.items())
        for seed in range(5):
            random.Random(seed).shuffle(shuffled)
            self.assertEqual(formal_shards.lpt(dict(shuffled), 6), expected)

    def test_equal_weights_break_ties_by_id_then_shard_number(self):
        partition = formal_shards.lpt({oid: 10.0 for oid in ('D', 'B', 'A', 'C', 'E')}, 2)
        self.assertEqual(partition, [['A', 'C', 'E'], ['B', 'D']])

    def test_heaviest_goes_first_to_the_least_loaded_shard(self):
        partition = formal_shards.lpt({'a': 7.0, 'b': 5.0, 'c': 4.0, 'd': 3.0, 'e': 1.0}, 2)
        self.assertEqual(partition, [['a', 'd'], ['b', 'c', 'e']])
        self.assertEqual(formal_shards.loads(partition, {'a': 7, 'b': 5, 'c': 4, 'd': 3, 'e': 1}),
                         [10, 10])


class Weights(unittest.TestCase):
    def test_a_receipt_weighs_the_sum_of_its_recorded_check_seconds(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, 'KANI-900.json').write_text(json.dumps(
                {'checks': [{'seconds': 12.5}, {'seconds': 30.0}, {'check': 'no-seconds'}]}))
            self.assertEqual(formal_shards.weight('KANI-900', directory), 42.5)

    def test_a_missing_or_unreadable_receipt_weighs_the_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, 'TLA-901.json').write_text('{not json')
            Path(directory, 'TLA-902.json').write_text('[]')
            for oid in ('TLA-900', 'TLA-901', 'TLA-902'):
                self.assertEqual(formal_shards.weight(oid, directory), formal_shards.FALLBACK_SECONDS)

    def test_every_claimed_result_has_a_recorded_weight(self):
        for obligation in MANIFEST['obligations']:
            if obligation['status'] in formal.CLAIMS:  # these must have a receipt
                self.assertIsNotNone(formal_shards.receipt_seconds(obligation['id']), obligation['id'])


class Balance(unittest.TestCase):
    def test_lpt_beats_index_modulo_on_the_real_receipts(self):
        work = formal_shards.weights(ALL_IDS)
        lpt = formal_shards.makespan(formal_shards.lpt(work, 6), work)
        modulo = formal_shards.makespan(formal_shards.modulo(ALL_IDS, 6), work)
        self.assertLess(lpt, modulo)

    def test_lpt_keeps_the_list_scheduling_bound_on_the_real_receipts(self):
        # Graham's bound holds for any list schedule, LPT included, without
        # knowing the optimum: makespan <= sum/m + (1 - 1/m) * longest job.
        work = formal_shards.weights(ALL_IDS)
        total, longest = sum(work.values()), max(work.values())
        for shards in range(1, 9):
            span = formal_shards.makespan(formal_shards.lpt(work, shards), work)
            self.assertLessEqual(span, total / shards + (1 - 1 / shards) * longest + 1e-6)
            self.assertGreaterEqual(span, max(longest, total / shards) - 1e-6)

    def test_lpt_is_within_four_thirds_of_the_optimum_on_small_instances(self):
        # LPT's guarantee is relative to the optimum (4/3 - 1/(3m)); brute
        # force the optimum where that is cheap, including equal fallbacks.
        cases = [{f'O{i}': 3600.0 for i in range(4)}]
        rng = random.Random(7)
        cases += [{f'O{i}': float(rng.randint(1, 100)) for i in range(rng.randint(2, 7))}
                  for _ in range(40)]
        for work in cases:
            ids = sorted(work)
            for shards in range(2, 5):
                best = min(max(sum(work[oid] for oid, s in zip(ids, pick) if s == k)
                               for k in range(shards))
                           for pick in itertools.product(range(shards), repeat=len(ids)))
                span = formal_shards.makespan(formal_shards.lpt(work, shards), work)
                self.assertLessEqual(span, (4 / 3 - 1 / (3 * shards)) * best + 1e-6, (work, shards))

    def test_modulo_reproduces_the_previous_inline_selector(self):
        self.assertEqual(formal_shards.modulo(['c', 'a', 'b', 'd'], 3), [['a', 'd'], ['b'], ['c']])


class Selection(unittest.TestCase):
    def test_base_selection_is_the_drivers_own(self):
        changed = {'src/offsets.rs', 'README.md'}
        with mock.patch.object(formal, 'changed_since', return_value=changed) as diff, \
                mock.patch.object(formal, 'ledger_at', return_value={}):
            selected = formal_shards.selected_ids(MANIFEST, 'base-rev')
            diff.assert_called_once_with('base-rev')
            printed = []
            for shard in range(3):
                code, out, _ = run_main('--shards', '3', '--shard', str(shard), '--base', 'base-rev')
                self.assertEqual(code, 0)
                printed += out.split()
        self.assertEqual(selected, formal.select(MANIFEST, changed, {}))
        self.assertIn('KANI-001', selected)
        self.assertLess(len(selected), len(ALL_IDS))
        self.assertEqual(sorted(printed), selected)

    def test_a_driver_edit_selects_everything(self):
        with mock.patch.object(formal, 'changed_since', return_value={formal.DRIVER}), \
                mock.patch.object(formal, 'ledger_at', return_value={}):
            self.assertEqual(formal_shards.selected_ids(MANIFEST, 'base-rev'), ALL_IDS)

    def test_an_empty_selection_prints_nothing_and_passes(self):
        with mock.patch.object(formal, 'changed_since', return_value={'docs/unrelated.md'}), \
                mock.patch.object(formal, 'ledger_at', return_value={}):
            for shard in range(6):
                self.assertEqual(run_main('--shards', '6', '--shard', str(shard), '--base', 'x'),
                                 (0, '', ''))


class Cli(unittest.TestCase):
    def test_check_prints_every_shard_and_the_comparison(self):
        code, out, _ = run_main('--shards', '6', '--check', '--all')
        self.assertEqual(code, 0)
        self.assertEqual(sum(line.startswith('shard ') for line in out.splitlines()), 6)
        self.assertIn(f'FORMAL_SHARDS_OK: {len(ALL_IDS)} selected obligation(s) in 6 shard(s)', out)

    def test_bad_arguments_are_refused(self):
        for argv in (('--shards', '6', '--shard', '6', '--all'),
                     ('--shards', '0', '--check', '--all'),
                     ('--shards', '6', '--shard', '-1', '--all'),
                     ('--shards', '6', '--shard', '0'),
                     ('--shards', '6', '--all'),
                     ('--shards', '6', '--shard', '0', '--all', '--base', 'x')):
            with self.subTest(argv=argv):
                self.assertEqual(run_main(*argv)[0], 2)

    def test_a_broken_partition_fails_closed(self):
        with mock.patch.object(formal_shards, 'lpt', return_value=[ALL_IDS[1:], []]):
            code, out, err = run_main('--shards', '2', '--shard', '0', '--all')
        self.assertEqual((code, out), (1, ''))
        self.assertIn(f'FORMAL_SHARDS_FAIL: selected but in no shard: {ALL_IDS[0]}', err)


if __name__ == '__main__':
    unittest.main()
