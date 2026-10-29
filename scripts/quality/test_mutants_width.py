"""The mutants job's width: ceil(mutants / 110) jobs, clamped to 8-48, and 8
for the nightly rotation (mutants_width.py); and the workflow's mutants
matrix and shard take the planning job's width."""
from pathlib import Path
import re
import unittest

from mutants_width import MAX_JOBS, MIN_JOBS, PER_JOB, selected_mutants, width

ROOT = Path(__file__).resolve().parents[2]


class MutantsWidth(unittest.TestCase):
    def test_a_push_takes_one_job_per_110_mutants_rounded_up(self):
        self.assertEqual(width(9 * PER_JOB, False), 9)
        self.assertEqual(width(9 * PER_JOB + 1, False), 10)
        self.assertEqual(width(2987, False), 28)

    def test_the_width_is_clamped_at_both_ends(self):
        self.assertEqual(width(0, False), MIN_JOBS)
        self.assertEqual(width(8 * PER_JOB, False), 8)
        self.assertEqual(width(48 * PER_JOB, False), 48)
        self.assertEqual(width(48 * PER_JOB + 1, False), MAX_JOBS)
        self.assertEqual(width(10**6, False), MAX_JOBS)
        self.assertEqual((MIN_JOBS, MAX_JOBS), (8, 48))

    def test_the_nightly_rotation_stays_at_eight(self):
        self.assertEqual(width(0, True), 8)
        self.assertEqual(width(10**6, True), 8)

    def test_the_selection_counts_only_its_owners_and_nothing_without_mutants(self):
        sizes = {'a': 100, 'b': 30, 'c': 7}
        plan = {'mutants': True, 'selected_mutation_owners': ['a', 'c']}
        self.assertEqual(selected_mutants(plan, sizes), 107)
        self.assertEqual(selected_mutants({**plan, 'mutants': False}, sizes), 0)

    def test_the_workflow_deals_the_mutants_over_the_planned_width(self):
        text = (ROOT / '.github/workflows/rust-quality.yml').read_text()
        job = text[text.index('\n  mutants:\n'):]
        job = job[:job.index('\n  formal:\n')]
        self.assertIn('needs: mutants-plan', job)
        self.assertIn('shard: ${{ fromJSON(needs.mutants-plan.outputs.shards) }}', job)
        self.assertIn('QUALITY_MUTANT_SHARD: ${{ matrix.shard }}/${{ needs.mutants-plan.outputs.width }}', job)
        self.assertIsNone(re.search(r'shard: \[0, 1, 2, 3, 4, 5, 6, 7\]', job), 'a fixed matrix')
        plan = text[text.index('\n  mutants-plan:\n'):text.index('\n  mutants:\n')]
        self.assertIn('scripts/quality/mutants_width.py --plan target/quality-plan/plan.json', plan)
        self.assertIn('width: ${{ steps.width.outputs.width }}', plan)
        self.assertIn('shards: ${{ steps.width.outputs.shards }}', plan)


if __name__ == '__main__':
    unittest.main()
