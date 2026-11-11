"""The mutants job's deal (mutants_width.py, gate decision of 2026-11-11, H2):
a push's owners packed into groups by modeled cost, one job each, under the
first cap that takes at most 48 jobs; the nightly stays at eight jobs; and the
workflow deals by the plan job's groups and digest."""
from pathlib import Path
import random
import re
import unittest

import mutants_width
from mutants_width import CAPS, JOB_FIXED_MINUTES, MAX_JOBS, PUSH_MODEL, Refused, deal
from mutation_owners import OWNERS, MutationOwner

ROOT = Path(__file__).resolve().parents[2]
PUSH1 = Path(__file__).with_name('fixtures') / 'push1-in-diff-counts.txt'


def push1_counts():
    """Layout 8's push (dbfed12ca): 384 in-diff mutants of 55 owners."""
    rows = (line.split() for line in PUSH1.read_text().splitlines() if line.strip())
    return {name: int(count) for name, count in rows}


def service(count):
    return tuple(MutationOwner(f'o{index}', (f'src/o{index}.rs',), ('o::',)) for index in range(count))


class PushDeal(unittest.TestCase):
    def assert_every_part_once_and_under_the_cap(self, counts, owners, dealt):
        placed = {}
        for group in dealt.groups:
            self.assertTrue(group)
            self.assertLessEqual(sum(share.minutes for share in group), dealt.cap)
            self.assertEqual(len({share.owner.name for share in group}), len(group),
                             'two parts of one owner in one job')
            for share in group:
                key = (share.owner.name, share.part)
                self.assertNotIn(key, placed)
                placed[key] = share.parts
        for entry in owners:
            parts = {parts for (name, _), parts in placed.items() if name == entry.name}
            if not counts.get(entry.name, 0):
                self.assertEqual(parts, set(), f'{entry.name} has no mutant but a job')
                continue
            self.assertEqual(len(parts), 1, entry.name)
            (count,) = parts
            self.assertEqual(sorted(part for (name, part) in placed if name == entry.name),
                             list(range(count)), entry.name)

    def test_layout_8s_push_lands_every_owner_once_in_far_fewer_jobs(self):
        counts = push1_counts()
        self.assertEqual((sum(counts.values()), sum(1 for c in counts.values() if c)), (384, 55))
        dealt = deal(counts)
        self.assert_every_part_once_and_under_the_cap(counts, OWNERS, dealt)
        self.assertEqual(dealt.cap, CAPS[0])
        self.assertEqual(dealt.width, len(dealt.groups))
        self.assertLessEqual(dealt.width, 25)  # 40 jobs under the old deal, five cancelled
        self.assertTrue(all(dealt.minutes(group) <= JOB_FIXED_MINUTES + CAPS[0]
                            for group in dealt.groups))
        self.assertEqual(sorted(share.owner.name for group in dealt.groups for share in group
                                if share.part == 0),
                         sorted(name for name, count in counts.items() if count))

    def test_generated_selections_land_every_part_once_under_the_cap(self):
        owners = service(70) + (MutationOwner('h', ('src/h.rs',), ('h::',), 'harness-lib'),)
        rng = random.Random(20261111)
        for _ in range(40):
            counts = {entry.name: rng.choice((0, 0, 0, 1, 2, 5, 17, 40, 90)) for entry in owners}
            try:
                dealt = deal(counts, owners)
            except Refused:
                continue
            with self.subTest(counts=counts):
                self.assert_every_part_once_and_under_the_cap(counts, owners, dealt)

    def test_an_owner_larger_than_the_cap_is_split_into_round_robin_parts(self):
        owners = service(2)
        dealt = deal({'o0': 100, 'o1': 1}, owners)
        parts = sorted((share.part, share.parts) for group in dealt.groups for share in group
                       if share.owner.name == 'o0')
        # 9.5 + ceil(100 / parts) x 5.1 <= 120 first holds at five parts.
        self.assertEqual(parts, [(part, 5) for part in range(5)])

    def test_the_cap_climbs_the_ladder_until_the_jobs_fit(self):
        # Sixty owners of n mutants each, (cap, width) per n: the first cap
        # whose packing takes at most 48 jobs (n = 20 packs 60 jobs at 120
        # to 210, 30 at 250).
        owners = service(60)
        for count, expected in ((5, (120.0, 20)), (10, (150.0, 30)), (15, (180.0, 30)),
                                (16, (210.0, 30)), (20, (250.0, 30))):
            with self.subTest(count=count):
                dealt = deal({entry.name: count for entry in owners}, owners)
                self.assertEqual((dealt.cap, dealt.width), expected)

    def test_a_selection_no_cap_fits_in_48_jobs_is_refused(self):
        owners = service(200)
        with self.assertRaisesRegex(Refused, r'more than 48: split the push'):
            deal({entry.name: 60 for entry in owners}, owners)

    def test_49_groups_at_the_last_cap_are_refused_and_48_are_not(self):
        owners = service(49)
        # 9.5 + 47 x 5.1 = 249.2 fits 250 alone and two never share a job.
        self.assertEqual(deal({entry.name: 47 for entry in owners[:48]}, owners).width, 48)
        with self.assertRaises(Refused):
            deal({entry.name: 47 for entry in owners}, owners)

    def test_without_a_mutant_the_deal_is_one_empty_job(self):
        dealt = deal({'append': 0, 'billing': 0})
        self.assertEqual((dealt.width, dealt.groups, dealt.cap), (1, (), None))
        self.assertEqual(deal({}).width, 1)

    def test_the_digest_names_the_counts_and_the_grouping(self):
        counts = push1_counts()
        dealt = deal(counts)
        self.assertEqual(dealt.digest, deal(dict(reversed(counts.items()))).digest)
        self.assertNotEqual(dealt.digest, deal({**counts, 'billing': counts['billing'] + 1}).digest)
        self.assertRegex(dealt.digest, r'^[0-9a-f]{16}$')

    def test_the_push_model_charges_a_baseline_per_owner_per_job(self):
        entry = service(1)[0]
        harness = MutationOwner('h', ('src/h.rs',), ('h::',), 'harness-lib')
        self.assertEqual(PUSH_MODEL.minutes(entry, 0), 0)
        self.assertEqual(PUSH_MODEL.minutes(entry, 3), 9.5 + 3 * 5.1)
        self.assertEqual(PUSH_MODEL.minutes(entry, 3, parts=2), 9.5 + 2 * 5.1)
        self.assertEqual(PUSH_MODEL.minutes(harness, 4), 1.0 + 4 * 0.25)
        self.assertEqual((JOB_FIXED_MINUTES, CAPS, MAX_JOBS),
                         (50.0, (120.0, 150.0, 180.0, 210.0, 250.0), 48))

    def test_an_unknown_owner_in_the_counts_fails(self):
        with self.assertRaisesRegex(ValueError, 'unknown mutation owner'):
            deal({'no-such-owner': 3})

    def test_a_group_spec_is_g_of_g(self):
        self.assertIsNone(mutants_width.parse_group(''))
        self.assertEqual(mutants_width.parse_group('3/7'), (3, 7))
        for invalid in ('7/7', '3', 'a/b', '-1/2'):
            with self.assertRaises(ValueError):
                mutants_width.parse_group(invalid)


class Workflow(unittest.TestCase):
    def test_the_workflow_deals_a_push_by_group_and_a_night_by_shard(self):
        text = (ROOT / '.github/workflows/rust-quality.yml').read_text()
        job = text[text.index('\n  mutants:\n'):]
        job = job[:job.index('\n  formal:\n')]
        self.assertIn('needs: mutants-plan', job)
        self.assertIn('shard: ${{ fromJSON(needs.mutants-plan.outputs.shards) }}', job)
        self.assertIn("QUALITY_MUTANT_SHARD: ${{ github.event_name == 'schedule' && format('{0}/{1}', "
                      "matrix.shard, needs.mutants-plan.outputs.width) || '' }}", job)
        self.assertIn("QUALITY_MUTANT_GROUP: ${{ github.event_name != 'schedule' && format('{0}/{1}', "
                      "matrix.shard, needs.mutants-plan.outputs.width) || '' }}", job)
        self.assertIn('QUALITY_MUTANT_DIGEST: ${{ needs.mutants-plan.outputs.digest }}', job)
        self.assertIsNone(re.search(r'shard: \[0, 1, 2, 3, 4, 5, 6, 7\]', job), 'a fixed matrix')
        plan = text[text.index('\n  mutants-plan:\n'):text.index('\n  mutants:\n')]
        self.assertIn('scripts/quality/mutants_width.py --out target/quality-plan', plan)
        self.assertIn('cargo install cargo-mutants --locked --version "=$version"', plan)
        self.assertIn('timeout-minutes: 45', plan)
        for output in ('width', 'shards', 'digest'):
            self.assertIn(f'{output}: ${{{{ steps.width.outputs.{output} }}}}', plan)

    def test_the_plan_writes_the_width_shards_and_digest(self):
        self.assertEqual(mutants_width.outputs(3, 'abc'),
                         {'width': '3', 'shards': '[0, 1, 2]', 'digest': 'abc'})
        self.assertEqual(mutants_width.NIGHT_JOBS, 8)


if __name__ == '__main__':
    unittest.main()
