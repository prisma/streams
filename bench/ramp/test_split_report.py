"""bench/ramp/split_report.py: the 10,000 leg's rule row from a split's park, open ends and the bench's
seconds (GATE-DECISION-meas10k-rig-limits-2026-11-19).

Run: python3 -m unittest discover -s bench/ramp -p 'test_*.py'
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import split_report  # noqa: E402

PARK = 1000.5  # mid-second, so the bench's 1 s grain and the park disagree
RATE = 167


def seconds(after, pre=RATE):
    """Ten seconds at `pre` served a second before the park's second, then `after` ({second: served})."""
    out = {s: {"served": pre} for s in range(990, 1000)}
    out[1000] = {"served": pre // 2}  # the park's own second: touches before it
    out.update({s: {"served": n} for s, n in after.items()})
    return out


def ends(*offsets):
    return {str(i): PARK + o for i, o in enumerate(offsets)}


class Judge(unittest.TestCase):
    def test_both_children_serving_soon_after_the_park_is_row_one(self):
        v = split_report.judge(PARK, ends(0.8, 0.9), seconds({s: RATE for s in range(1001, 1006)}), 560)
        self.assertTrue(v["children_open_within_2000_ms_of_park"])
        self.assertTrue(v["first_served_within_2000_ms_of_park"])
        self.assertTrue(v["serving_after_the_later_open"])
        self.assertEqual(split_report.rule(True, [v]), 1)

    def test_a_later_child_opening_past_two_seconds_is_row_two(self):
        # The earlier child serves at once and the later opens at 5 s: one child's half of the rate.
        after = {s: RATE // 2 for s in range(1001, 1006)} | {s: RATE for s in range(1006, 1012)}
        v = split_report.judge(PARK, ends(0.8, 5.0), seconds(after), 560)
        self.assertFalse(v["children_open_within_2000_ms_of_park"])
        self.assertEqual(split_report.rule(True, [v]), 2)

    def test_a_first_served_second_starting_within_two_seconds_counts_at_the_grain(self):
        # Park at .5 and the first touch at 1,990 ms lands in second 1002: 1.5 s after the park by its
        # start, 2.5 s by its end. Its start decides (a lower bound), and both children open by 1.45 s.
        v = split_report.judge(PARK, ends(1.40, 1.45), seconds({s: RATE for s in range(1002, 1008)}), 560)
        self.assertTrue(v["first_served_within_2000_ms_of_park"])
        self.assertEqual(split_report.rule(True, [v]), 1)

    def test_a_child_that_opened_and_does_not_serve_is_row_two_and_unexplained(self):
        # Both open by 0.9 s, but three seconds after the later open carry only one child's half.
        v = split_report.judge(PARK, ends(0.8, 0.9), seconds({s: RATE // 2 for s in range(1001, 1010)}), 560)
        self.assertTrue(v["children_open_within_2000_ms_of_park"])
        self.assertFalse(v["serving_after_the_later_open"])
        self.assertTrue(v["unexplained_after_opens"])
        self.assertEqual(split_report.rule(True, [v]), 2)

    def test_three_whole_seconds_after_the_later_opens_second_are_summed(self):
        # The later open ends in second 1001: seconds 1002-1004 decide, not 1001 (a partial second).
        after = {1001: 0, 1002: RATE, 1003: RATE, 1004: int(RATE * 0.71)}
        v = split_report.judge(PARK, ends(0.8, 0.9), seconds(after), 560)
        self.assertEqual(v["served_3s_after_later_open"], 2 * RATE + int(RATE * 0.71))
        self.assertTrue(v["serving_after_the_later_open"])  # 452 >= 0.9 * 3 * 167.0 = 450.9

    def test_a_leg_with_no_published_split_is_not_row_one(self):
        self.assertEqual(split_report.rule(True, []), "no split")
        self.assertEqual(split_report.rule(False, []), "failed")

    def test_a_low_pre_park_rate_or_not_two_children_asks_to_come_back(self):
        v = split_report.judge(PARK, ends(0.8, 0.9), seconds({s: 15 for s in range(1001, 1006)}, pre=15), 560)
        self.assertTrue(v["come_back_low_rate"])
        one = split_report.judge(PARK, ends(0.8), seconds({s: RATE for s in range(1001, 1006)}), 560)
        self.assertTrue(one["come_back_children"])


if __name__ == "__main__":
    unittest.main()
